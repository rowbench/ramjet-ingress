//! What happens when an upstream is not there, is slow, or is not HTTP.
//!
//! The status codes and bodies asserted here are the hyper engine's, verbatim.
//! A client must not be able to tell which engine refused it.

mod common;

use std::time::Duration;

use common::{dead_addr, echo, get, spawn, table_for, Behaviour, Client, Proxy};
use ramjet_router::{Endpoint, LbPolicy, PathType, RouteTableBuilder};

fn table_of(endpoints: &[std::net::SocketAddr]) -> ramjet_router::RouteTable {
    let mut builder = RouteTableBuilder::new();
    builder
        .backend(
            "app",
            LbPolicy::RoundRobin,
            endpoints.iter().copied().map(Endpoint::new).collect(),
        )
        .expect("a valid backend");
    builder
        .route(Some("app.example.com"), "/", PathType::Prefix, "app")
        .expect("a valid route");
    builder.build().expect("a valid table")
}

#[test]
fn a_dead_first_endpoint_fails_over_to_the_next() {
    let alive = echo();
    // Round-robin starts at an arbitrary endpoint, so both orders must work.
    let proxy = Proxy::start(table_of(&[dead_addr(), alive.addr]));

    for _ in 0..4 {
        let response = get(proxy.addr, "/", "app.example.com");
        assert_eq!(response.status, 200, "{}", response.text());
    }
    assert_eq!(alive.seen.requests(), 4);
}

#[test]
fn every_endpoint_dead_is_502() {
    let proxy = Proxy::start(table_of(&[dead_addr(), dead_addr()]));

    let response = get(proxy.addr, "/", "app.example.com");

    assert_eq!(response.status, 502);
    assert!(
        response.text().contains("could not connect"),
        "{}",
        response.text()
    );
}

#[test]
fn an_upstream_that_never_answers_is_504() {
    let black_hole = spawn(Behaviour::BlackHole);
    let proxy = Proxy::with_config(table_of(&[black_hole.addr]), |config| {
        config.response_timeout = Duration::from_millis(200);
        config.tick = Duration::from_millis(10);
    });

    let response = get(proxy.addr, "/", "app.example.com");

    assert_eq!(response.status, 504);
    assert!(
        response.text().contains("no response headers"),
        "{}",
        response.text()
    );
}

#[test]
fn a_timeout_is_not_retried_against_another_endpoint() {
    // A timeout says the upstream is slow, not that it is gone. Trying the next
    // endpoint would double the load on a service that is already struggling.
    let black_hole = spawn(Behaviour::BlackHole);
    let healthy = echo();
    let proxy = Proxy::with_config(table_of(&[black_hole.addr, healthy.addr]), |config| {
        config.response_timeout = Duration::from_millis(200);
        config.tick = Duration::from_millis(10);
    });

    // One of the two endpoints is the black hole; whichever request lands on it
    // must 504 rather than fall through to the healthy one.
    let mut saw_timeout = false;
    for _ in 0..4 {
        if get(proxy.addr, "/", "app.example.com").status == 504 {
            saw_timeout = true;
        }
    }
    assert!(saw_timeout, "the black hole should have produced a 504");
    assert!(
        healthy.seen.requests() <= 2,
        "a timeout must not be failed over: {} requests reached the healthy endpoint",
        healthy.seen.requests()
    );
}

#[test]
fn a_slow_upstream_inside_the_deadline_still_succeeds() {
    let slow = spawn(Behaviour::Slow {
        delay: Duration::from_millis(120),
        body: vec![b'u'; 128],
    });
    let proxy = Proxy::with_config(table_of(&[slow.addr]), |config| {
        config.response_timeout = Duration::from_secs(5);
    });

    assert_eq!(get(proxy.addr, "/", "app.example.com").status, 200);
}

#[test]
fn an_upstream_that_hangs_up_without_answering_is_502() {
    let rude = spawn(Behaviour::HangUp);
    let proxy = Proxy::start(table_of(&[rude.addr]));

    let response = get(proxy.addr, "/", "app.example.com");

    assert_eq!(response.status, 502);
}

#[test]
fn an_upstream_that_is_not_http_is_502() {
    let nonsense = spawn(Behaviour::Raw(b"this is not a response\r\n\r\n".to_vec()));
    let proxy = Proxy::start(table_of(&[nonsense.addr]));

    let response = get(proxy.addr, "/", "app.example.com");

    assert_eq!(response.status, 502);
    assert!(
        response.text().contains("upstream"),
        "{}",
        response.text()
    );
}

#[test]
fn a_request_with_a_body_is_not_failed_over() {
    // The first attempt may already have written some of those bytes upstream,
    // and nothing buffers them for a second try. Buffering every upload to save
    // a rare 502 is the trade this deliberately does not make.
    let alive = echo();
    let proxy = Proxy::with_config(table_of(&[dead_addr(), alive.addr]), |config| {
        config.max_connect_attempts = 3;
    });

    let mut saw_failure = false;
    for _ in 0..6 {
        let mut client = Client::connect(proxy.addr);
        let response = client.send(
            b"POST / HTTP/1.1\r\nHost: app.example.com\r\nContent-Length: 5\r\n\r\nhello",
        );
        if response.status == 502 {
            saw_failure = true;
        }
    }
    assert!(
        saw_failure,
        "a request with a payload must not be replayed on another endpoint"
    );
}

#[test]
fn an_empty_bodied_post_does_fail_over() {
    // The limit is the body, not the method.
    let alive = echo();
    let proxy = Proxy::start(table_of(&[dead_addr(), alive.addr]));

    for _ in 0..4 {
        let mut client = Client::connect(proxy.addr);
        let response = client.send(
            b"POST / HTTP/1.1\r\nHost: app.example.com\r\nContent-Length: 0\r\n\r\n",
        );
        assert_eq!(response.status, 200, "{}", response.text());
    }
    assert_eq!(alive.seen.requests(), 4);
}

#[test]
fn failover_stops_at_the_configured_attempt_limit() {
    let alive = echo();
    let proxy = Proxy::with_config(
        table_of(&[dead_addr(), dead_addr(), dead_addr(), alive.addr]),
        |config| config.max_connect_attempts = 2,
    );

    // Two attempts over four endpoints cannot always reach the live one, and
    // when it cannot the answer is a 502 rather than an unbounded search.
    let mut failures = 0;
    for _ in 0..8 {
        if get(proxy.addr, "/", "app.example.com").status == 502 {
            failures += 1;
        }
    }
    assert!(
        failures > 0,
        "the attempt limit must actually bind; nothing failed"
    );
}

#[test]
fn the_connection_survives_an_error_response() {
    // A 404 or a 502 the proxy invented is not a reason to hang up on a
    // keep-alive client, and a client that has to reconnect after every miss
    // pays a handshake for someone else's typo.
    let upstream = echo();
    let proxy = Proxy::start(table_of(&[upstream.addr]));
    let mut client = Client::connect(proxy.addr);

    let miss = client.send(b"GET / HTTP/1.1\r\nHost: nobody.invalid\r\n\r\n");
    assert_eq!(miss.status, 404);
    assert!(!miss.closing, "a 404 must not close a keep-alive connection");

    let hit = client.send(b"GET /after HTTP/1.1\r\nHost: app.example.com\r\n\r\n");
    assert_eq!(hit.status, 200);
    assert_eq!(hit.header("echo-target"), Some("/after"));
}

#[test]
fn a_client_that_vanishes_mid_request_is_cleaned_up() {
    let upstream = echo();
    let proxy = Proxy::start(table_of(&[upstream.addr]));

    for _ in 0..20 {
        let mut client = Client::connect(proxy.addr);
        // Half a request, then gone.
        client.write(b"GET / HTTP/1.1\r\nHost: app.exa");
        drop(client);
    }

    // The engine must still be serving; a leaked connection or a panicked core
    // would show up as this failing.
    assert_eq!(get(proxy.addr, "/", "app.example.com").status, 200);
}

#[test]
fn an_upstream_that_dies_between_requests_is_retried_transparently() {
    // The pooled-connection race: the origin closes an idle connection at the
    // same moment a request takes it. No pooling proxy can close that window;
    // what it must not do is turn it into a 502.
    let upstream = spawn(Behaviour::Echo {
        body: vec![b'u'; 16],
    });
    let proxy = Proxy::start(table_of(&[upstream.addr]));
    let mut client = Client::connect(proxy.addr);

    for i in 0..30 {
        let response = client.send(b"GET / HTTP/1.1\r\nHost: app.example.com\r\n\r\n");
        assert_eq!(response.status, 200, "request {i}: {}", response.text());
    }
}
