//! Routing, load balancing and keep-alive, over real sockets.
//!
//! These mirror the hyper engine's `routing.rs` assertion for assertion, which
//! is the point: the two data planes are supposed to be indistinguishable from
//! outside, and the way to know that is to ask them the same questions.

mod common;

use std::time::Duration;

use common::{builder_with, echo, get, spread, table_for, Client, Proxy};
use ramjet_router::{PathType, RouteTableBuilder};

#[test]
fn a_request_is_proxied_and_the_answer_comes_back() {
    let upstream = echo();
    let proxy = Proxy::start(table_for("app.example.com", &[upstream.addr]));

    let response = get(proxy.addr, "/", "app.example.com");

    assert_eq!(response.status, 200);
    assert_eq!(response.reason, "OK");
    assert_eq!(response.body, vec![b'u'; 128]);
    assert_eq!(response.header("echo-method"), Some("GET"));
    assert_eq!(upstream.seen.requests(), 1);
}

#[test]
fn dispatches_by_host() {
    let alpha = echo();
    let beta = echo();
    let mut builder = builder_with(&[("alpha", &[alpha.addr]), ("beta", &[beta.addr])]);
    builder
        .route(Some("alpha.example.com"), "/", PathType::Prefix, "alpha")
        .expect("a route");
    builder
        .route(Some("beta.example.com"), "/", PathType::Prefix, "beta")
        .expect("a route");
    let proxy = Proxy::start(builder.build().expect("a table"));

    assert_eq!(get(proxy.addr, "/", "alpha.example.com").status, 200);
    assert_eq!(alpha.seen.requests(), 1);
    assert_eq!(beta.seen.requests(), 0);

    assert_eq!(get(proxy.addr, "/", "beta.example.com").status, 200);
    assert_eq!(beta.seen.requests(), 1);

    // The port is not part of the host for matching purposes.
    assert_eq!(get(proxy.addr, "/", "ALPHA.example.com:8080").status, 200);
    assert_eq!(alpha.seen.requests(), 2, "case and port are normalised");
}

#[test]
fn dispatches_by_path_with_exact_beating_prefix() {
    let root = echo();
    let api = echo();
    let exact = echo();
    let mut builder = builder_with(&[
        ("root", &[root.addr]),
        ("api", &[api.addr]),
        ("exact", &[exact.addr]),
    ]);
    builder
        .route(Some("app.example.com"), "/", PathType::Prefix, "root")
        .expect("a route");
    builder
        .route(Some("app.example.com"), "/api", PathType::Prefix, "api")
        .expect("a route");
    builder
        .route(Some("app.example.com"), "/api/v2", PathType::Exact, "exact")
        .expect("a route");
    let proxy = Proxy::start(builder.build().expect("a table"));

    for (path, expected) in [
        ("/api/v2", "exact"),
        ("/api/v2/more", "api"),
        ("/api", "api"),
        ("/api/", "api"),
        ("/other", "root"),
        // A prefix match stops at a path segment boundary, so this is not /api.
        ("/apiary", "root"),
    ] {
        let before = spread(&[&root, &api, &exact]);
        assert_eq!(get(proxy.addr, path, "app.example.com").status, 200, "{path}");
        let after = spread(&[&root, &api, &exact]);
        let hit = [(&root, "root"), (&api, "api"), (&exact, "exact")]
            .into_iter()
            .find(|(u, _)| after[&u.addr] > before[&u.addr])
            .map(|(_, name)| name);
        assert_eq!(hit, Some(expected), "{path} went to the wrong backend");
    }
}

#[test]
fn an_unmatched_request_is_404() {
    let upstream = echo();
    let proxy = Proxy::start(table_for("app.example.com", &[upstream.addr]));

    let response = get(proxy.addr, "/", "nobody.invalid");

    assert_eq!(response.status, 404);
    assert!(response.text().contains("no ingress rule"), "{}", response.text());
    assert_eq!(
        response.header("content-type"),
        Some("text/plain; charset=utf-8")
    );
    assert_eq!(upstream.seen.requests(), 0, "nothing was forwarded");
}

#[test]
fn a_wildcard_host_serves_one_label() {
    let upstream = echo();
    let mut builder = builder_with(&[("app", &[upstream.addr])]);
    builder
        .route(Some("*.example.com"), "/", PathType::Prefix, "app")
        .expect("a route");
    let proxy = Proxy::start(builder.build().expect("a table"));

    assert_eq!(get(proxy.addr, "/", "shop.example.com").status, 200);
    assert_eq!(get(proxy.addr, "/", "a.b.example.com").status, 404);
}

#[test]
fn the_path_and_query_reach_the_upstream_untouched() {
    let upstream = echo();
    let proxy = Proxy::start(table_for("app.example.com", &[upstream.addr]));

    let response = get(proxy.addr, "/a/b%20c?x=1&y=%2F", "app.example.com");

    assert_eq!(response.status, 200);
    // Not re-encoded, not normalised, not decoded.
    assert_eq!(response.header("echo-target"), Some("/a/b%20c?x=1&y=%2F"));
}

#[test]
fn the_client_host_is_preserved_upstream() {
    let upstream = echo();
    let proxy = Proxy::start(table_for("app.example.com", &[upstream.addr]));

    let response = get(proxy.addr, "/", "app.example.com");

    // Never rewritten to the endpoint's ip:port: the origin uses it to pick a
    // virtual host.
    assert_eq!(response.header("echo-host"), Some("app.example.com"));
}

#[test]
fn round_robin_spreads_across_endpoints() {
    let one = echo();
    let two = echo();
    let proxy = Proxy::start(table_for("app.example.com", &[one.addr, two.addr]));

    for _ in 0..8 {
        assert_eq!(get(proxy.addr, "/", "app.example.com").status, 200);
    }

    assert_eq!(one.seen.requests(), 4);
    assert_eq!(two.seen.requests(), 4);
}

#[test]
fn a_published_table_takes_effect_immediately() {
    let first = echo();
    let second = echo();
    let proxy = Proxy::start(table_for("app.example.com", &[first.addr]));

    assert_eq!(get(proxy.addr, "/", "app.example.com").status, 200);
    assert_eq!(first.seen.requests(), 1);

    proxy.routes.store(table_for("app.example.com", &[second.addr]));

    assert_eq!(get(proxy.addr, "/", "app.example.com").status, 200);
    assert_eq!(second.seen.requests(), 1, "the next request saw the new table");
    assert_eq!(first.seen.requests(), 1);
}

#[test]
fn a_default_backend_answers_instead_of_404() {
    let upstream = echo();
    let mut builder = builder_with(&[("app", &[upstream.addr])]);
    builder.default_backend("app");
    let proxy = Proxy::start(builder.build().expect("a table"));

    assert_eq!(get(proxy.addr, "/anything", "nobody.invalid").status, 200);
}

#[test]
fn a_keep_alive_connection_serves_many_requests() {
    let upstream = echo();
    let proxy = Proxy::start(table_for("app.example.com", &[upstream.addr]));
    let mut client = Client::connect(proxy.addr);

    for i in 0..10 {
        let response = client.send(
            format!("GET /req-{i} HTTP/1.1\r\nHost: app.example.com\r\n\r\n").as_bytes(),
        );
        assert_eq!(response.status, 200, "request {i}");
        assert_eq!(response.header("echo-target"), Some(&*format!("/req-{i}")));
        assert!(!response.closing, "the proxy should keep it open");
    }
    assert_eq!(upstream.seen.requests(), 10);
}

#[test]
fn upstream_connections_are_pooled_across_requests() {
    // The measurement `bench/RESULTS.md` calls "requests per upstream
    // connection". If this regresses, every request pays a TCP handshake.
    let upstream = echo();
    let proxy = Proxy::start(table_for("app.example.com", &[upstream.addr]));
    let mut client = Client::connect(proxy.addr);

    for _ in 0..20 {
        assert_eq!(
            client
                .send(b"GET / HTTP/1.1\r\nHost: app.example.com\r\n\r\n")
                .status,
            200
        );
    }

    assert_eq!(upstream.seen.requests(), 20);
    assert_eq!(
        upstream.seen.connections(),
        1,
        "twenty requests should have reused one upstream connection"
    );
}

#[test]
fn connection_close_is_honoured_in_both_directions() {
    let upstream = echo();
    let proxy = Proxy::start(table_for("app.example.com", &[upstream.addr]));
    let mut client = Client::connect(proxy.addr);

    let response =
        client.send(b"GET / HTTP/1.1\r\nHost: app.example.com\r\nConnection: close\r\n\r\n");

    assert_eq!(response.status, 200);
    assert!(response.closing, "the proxy must say it is closing");
    // And the hop-by-hop header must not have reached the upstream.
    assert_eq!(response.header("echo-connection"), None);
}

#[test]
fn a_pipelined_pair_is_answered_in_order() {
    let upstream = echo();
    let proxy = Proxy::start(table_for("app.example.com", &[upstream.addr]));
    let mut client = Client::connect(proxy.addr);

    // Both requests in one write, before either answer is read.
    client.write(
        b"GET /first HTTP/1.1\r\nHost: app.example.com\r\n\r\n\
          GET /second HTTP/1.1\r\nHost: app.example.com\r\n\r\n",
    );

    let first = client.read_response();
    let second = client.read_response();

    assert_eq!(first.header("echo-target"), Some("/first"));
    assert_eq!(second.header("echo-target"), Some("/second"));
}

#[test]
fn a_request_arriving_one_byte_at_a_time_is_still_answered() {
    // The partial-parse resume path: every prefix of the head is seen, and the
    // scan must not restart from the beginning each time or lose the
    // terminator when it straddles two reads.
    let upstream = echo();
    let proxy = Proxy::start(table_for("app.example.com", &[upstream.addr]));
    let mut client = Client::connect(proxy.addr);

    let response = client.send_dribbled(
        b"GET /slow HTTP/1.1\r\nHost: app.example.com\r\nX-Trailer: yes\r\n\r\n",
    );

    assert_eq!(response.status, 200);
    assert_eq!(response.header("echo-target"), Some("/slow"));
    assert_eq!(response.header("echo-x-trailer"), Some("yes"));
}

#[test]
fn an_empty_backend_is_503() {
    let mut builder = RouteTableBuilder::new();
    builder
        .backend("app", ramjet_router::LbPolicy::RoundRobin, Vec::new())
        .expect("a backend with no endpoints is legal");
    builder
        .route(Some("app.example.com"), "/", PathType::Prefix, "app")
        .expect("a route");
    let proxy = Proxy::start(builder.build().expect("a table"));

    let response = get(proxy.addr, "/", "app.example.com");

    assert_eq!(response.status, 503);
    assert!(
        response.text().contains("no ready endpoints"),
        "{}",
        response.text()
    );
}

#[test]
fn several_cores_all_answer() {
    let upstream = echo();
    let proxy = Proxy::with_config(table_for("app.example.com", &[upstream.addr]), |config| {
        config.workers = Some(4);
    });

    for _ in 0..16 {
        assert_eq!(get(proxy.addr, "/", "app.example.com").status, 200);
    }
    assert_eq!(upstream.seen.requests(), 16);
}

#[test]
fn an_idle_connection_is_eventually_reaped() {
    // Not a fast test to write around, but a proxy that never reaps a client
    // that vanished without a FIN leaks a descriptor per such client.
    let upstream = echo();
    let proxy = Proxy::with_config(table_for("app.example.com", &[upstream.addr]), |config| {
        config.tick = Duration::from_millis(5);
    });
    let mut client = Client::connect(proxy.addr);
    assert_eq!(
        client
            .send(b"GET / HTTP/1.1\r\nHost: app.example.com\r\n\r\n")
            .status,
        200
    );
    // The client idle timeout is minutes, so this only asserts the connection
    // survives a sweep rather than being closed by one.
    std::thread::sleep(Duration::from_millis(50));
    assert_eq!(
        client
            .send(b"GET / HTTP/1.1\r\nHost: app.example.com\r\n\r\n")
            .status,
        200,
        "a live keep-alive connection must survive the deadline sweep"
    );
}
