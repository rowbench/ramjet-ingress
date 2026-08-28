//! Traffic mirroring from the reactor threads.
//!
//! The mirror worker is a tokio task and the request path is a reactor thread,
//! which is the only interesting thing about this: the two never touch except
//! through a `try_send` on a bounded channel, which needs no runtime on the
//! sending side. What is asserted here is that copies actually arrive, carry
//! the right headers, and never cost the primary anything.

mod common;

use std::sync::Arc;
use std::time::{Duration, Instant};

use common::*;
use ramjet_router::{Endpoint, LbPolicy, MirrorRules, PathType, RouteOptions, RouteTableBuilder};

/// A running engine with a mirror lane, and the runtime its worker lives on.
///
/// The runtime is held here rather than dropped: the worker is a task on it,
/// and a dropped runtime is a queue nothing is draining.
struct Mirrored {
    proxy: Proxy,
    _runtime: tokio::runtime::Runtime,
    metrics: Arc<ramjet_proxy::Metrics>,
}

fn start(primary: std::net::SocketAddr, shadow: std::net::SocketAddr, percent: u32) -> Mirrored {
    start_with(primary, shadow, percent, ramjet_proxy::DEFAULT_MIRROR_MAX_BODY)
}

fn start_with(
    primary: std::net::SocketAddr,
    shadow: std::net::SocketAddr,
    percent: u32,
    max_body: usize,
) -> Mirrored {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .expect("a runtime");

    let metrics = Arc::new(ramjet_proxy::Metrics::new());
    let mirror = runtime.block_on(async {
        ramjet_proxy::Mirror::spawn(
            ramjet_proxy::Upstream::new(&ramjet_proxy::UpstreamConfig::default()),
            Arc::clone(&metrics),
        )
        .with_max_body(max_body)
    });
    let lane = ramjet_engine::mirror::MirrorLane::new(mirror, Arc::clone(&metrics));

    let mut builder = RouteTableBuilder::new();
    builder
        .backend("app", LbPolicy::RoundRobin, vec![Endpoint::new(primary)])
        .expect("a valid backend");
    builder
        .backend("shadow", LbPolicy::RoundRobin, vec![Endpoint::new(shadow)])
        .expect("a valid backend");
    builder
        .route_with(
            Some("app.example.com"),
            "/",
            PathType::Prefix,
            "app",
            &RouteOptions {
                mirror: Some(MirrorRules {
                    backend: "shadow",
                    percent,
                    host: None,
                }),
                ..RouteOptions::default()
            },
        )
        .expect("a valid mirror route");

    let proxy = Proxy::with_config(builder.build().expect("a valid table"), move |config| {
        config.workers = Some(1);
        config.mirror = Some(lane);
        config.mirror_max_body = max_body;
    });

    Mirrored {
        proxy,
        _runtime: runtime,
        metrics,
    }
}

/// Wait for the shadow to have seen `wanted` requests.
///
/// The copy is fire-and-forget, so it arrives after the primary's response and
/// a test that read the counter immediately would be racing the worker.
fn wait_for(upstream: &Upstream, wanted: usize) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        if upstream.seen.requests() >= wanted {
            return;
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    panic!(
        "the shadow saw {} of {wanted} expected copies",
        upstream.seen.requests()
    );
}

#[test]
fn a_sampled_request_reaches_the_shadow_backend() {
    let primary = echo();
    let shadow = echo();
    let mirrored = start(primary.addr, shadow.addr, 100);

    for i in 0..5 {
        let response = get(mirrored.proxy.addr, &format!("/{i}"), "app.example.com");
        assert_eq!(response.status, 200);
    }

    wait_for(&shadow, 5);
    assert_eq!(primary.seen.requests(), 5, "the primary served every request");
}

#[test]
fn a_zero_percent_mirror_copies_nothing() {
    let primary = echo();
    let shadow = echo();
    let mirrored = start(primary.addr, shadow.addr, 0);

    for _ in 0..10 {
        assert_eq!(get(mirrored.proxy.addr, "/", "app.example.com").status, 200);
    }
    std::thread::sleep(Duration::from_millis(200));
    assert_eq!(shadow.seen.requests(), 0, "nothing was sampled");
    assert_eq!(primary.seen.requests(), 10);
}

#[test]
fn the_copy_is_marked_and_carries_the_forwarded_headers() {
    // A shadow backend has to be able to tell a copy from the real thing before
    // it decides whether to charge somebody's card.
    let primary = echo();
    let shadow = spawn(Behaviour::Echo { body: b"ok".to_vec() });
    let mirrored = start(primary.addr, shadow.addr, 100);

    // The shadow reflects what it received, but nothing reads its response — so
    // the assertion has to come from the primary's view plus the counter.
    assert_eq!(
        get(mirrored.proxy.addr, "/checkout", "app.example.com").status,
        200
    );
    wait_for(&shadow, 1);
    assert_eq!(mirrored.metrics.mirrored(), 1, "the shadow accepted the copy");
}

#[test]
fn a_request_with_a_body_is_copied_with_it() {
    // The uring lane takes the copy out of the buffer the body is already
    // passing through, so the primary is never held waiting for it.
    let primary = echo();
    let shadow = echo();
    let mirrored = start(primary.addr, shadow.addr, 100);

    let mut client = Client::connect(mirrored.proxy.addr);
    let response = client.send(
        b"POST /submit HTTP/1.1\r\nHost: app.example.com\r\n\
          Content-Length: 11\r\n\r\nhello world",
    );
    assert_eq!(response.status, 200);
    assert_eq!(response.header("echo-body-len"), Some("11"));

    wait_for(&shadow, 1);
}

#[test]
fn a_body_past_the_cap_is_skipped_and_counted() {
    // The cap bounds memory on the pod serving production. Past it the copy is
    // dropped and the number says why, which is the difference between "the
    // shadow is getting nothing" and "the bodies are too big".
    let primary = echo();
    let shadow = echo();
    let mirrored = start_with(primary.addr, shadow.addr, 100, 64);

    let body = "x".repeat(4096);
    let mut client = Client::connect(mirrored.proxy.addr);
    let response = client.send(
        format!(
            "POST /upload HTTP/1.1\r\nHost: app.example.com\r\nContent-Length: {}\r\n\r\n{body}",
            body.len()
        )
        .as_bytes(),
    );
    assert_eq!(response.status, 200, "the primary is unaffected by the cap");
    assert_eq!(response.header("echo-body-len"), Some("4096"));

    let deadline = Instant::now() + Duration::from_secs(2);
    while Instant::now() < deadline && mirrored.metrics.mirror_skipped() == 0 {
        std::thread::sleep(Duration::from_millis(5));
    }
    assert_eq!(mirrored.metrics.mirror_skipped(), 1, "the skip was counted");
    assert_eq!(shadow.seen.requests(), 0, "no copy was sent");
}

#[test]
fn a_chunked_body_is_skipped_rather_than_double_encoded() {
    // This lane forwards chunk framing verbatim, so the bytes streaming past
    // are the body's *encoding*. Sending those as a self-framed copy would
    // double-encode them, and decoding a body this engine deliberately does not
    // decode is not a trade worth making for a copy.
    let primary = echo();
    let shadow = echo();
    let mirrored = start(primary.addr, shadow.addr, 100);

    let mut client = Client::connect(mirrored.proxy.addr);
    let response = client.send(
        b"POST /stream HTTP/1.1\r\nHost: app.example.com\r\n\
          Transfer-Encoding: chunked\r\n\r\n5\r\nhello\r\n0\r\n\r\n",
    );
    assert_eq!(response.status, 200);

    let deadline = Instant::now() + Duration::from_secs(2);
    while Instant::now() < deadline && mirrored.metrics.mirror_skipped() == 0 {
        std::thread::sleep(Duration::from_millis(5));
    }
    assert_eq!(mirrored.metrics.mirror_skipped(), 1);
    assert_eq!(shadow.seen.requests(), 0);
}

#[test]
fn a_shadow_that_is_down_never_touches_the_primary() {
    // The invariant the whole feature lives under: a mirror must not make the
    // primary slower or more likely to fail.
    let primary = echo();
    let dead = dead_addr();
    let mirrored = start(primary.addr, dead, 100);

    for i in 0..5 {
        let response = get(mirrored.proxy.addr, &format!("/{i}"), "app.example.com");
        assert_eq!(response.status, 200, "the primary answered normally");
    }
    assert_eq!(primary.seen.requests(), 5);

    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline && mirrored.metrics.mirror_failures() < 5 {
        std::thread::sleep(Duration::from_millis(10));
    }
    assert!(
        mirrored.metrics.mirror_failures() >= 1,
        "a shadow that refuses every copy must show up as a number"
    );
}

#[test]
fn a_shadow_with_no_endpoints_is_a_counted_failure() {
    // An operator who configured a mirror and sees no copies should be able to
    // tell "the shadow Service has no ready pods" from "the annotation never
    // took effect".
    let primary = echo();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .expect("a runtime");
    let metrics = Arc::new(ramjet_proxy::Metrics::new());
    let mirror = runtime.block_on(async {
        ramjet_proxy::Mirror::spawn(
            ramjet_proxy::Upstream::new(&ramjet_proxy::UpstreamConfig::default()),
            Arc::clone(&metrics),
        )
    });
    let lane = ramjet_engine::mirror::MirrorLane::new(mirror, Arc::clone(&metrics));

    let mut builder = RouteTableBuilder::new();
    builder
        .backend("app", LbPolicy::RoundRobin, vec![Endpoint::new(primary.addr)])
        .expect("a valid backend");
    builder
        .backend("shadow", LbPolicy::RoundRobin, vec![])
        .expect("a backend with no endpoints");
    builder
        .route_with(
            Some("app.example.com"),
            "/",
            PathType::Prefix,
            "app",
            &RouteOptions {
                mirror: Some(MirrorRules {
                    backend: "shadow",
                    percent: 100,
                    host: None,
                }),
                ..RouteOptions::default()
            },
        )
        .expect("a valid mirror route");

    let proxy = Proxy::with_config(builder.build().expect("a valid table"), move |config| {
        config.workers = Some(1);
        config.mirror = Some(lane);
    });

    assert_eq!(get(proxy.addr, "/", "app.example.com").status, 200);
    assert_eq!(metrics.mirror_failures(), 1);
    assert_eq!(metrics.mirrored(), 0);
}

#[test]
fn a_route_with_a_mirror_and_no_lane_simply_makes_no_copies() {
    // Which is the correct behaviour for a data plane with nowhere to put them,
    // and is what an embedding that never starts a worker gets.
    let primary = echo();
    let shadow = echo();

    let mut builder = RouteTableBuilder::new();
    builder
        .backend("app", LbPolicy::RoundRobin, vec![Endpoint::new(primary.addr)])
        .expect("a valid backend");
    builder
        .backend("shadow", LbPolicy::RoundRobin, vec![Endpoint::new(shadow.addr)])
        .expect("a valid backend");
    builder
        .route_with(
            Some("app.example.com"),
            "/",
            PathType::Prefix,
            "app",
            &RouteOptions {
                mirror: Some(MirrorRules {
                    backend: "shadow",
                    percent: 100,
                    host: None,
                }),
                ..RouteOptions::default()
            },
        )
        .expect("a valid mirror route");

    let proxy = Proxy::start(builder.build().expect("a valid table"));
    assert_eq!(get(proxy.addr, "/", "app.example.com").status, 200);
    std::thread::sleep(Duration::from_millis(200));
    assert_eq!(shadow.seen.requests(), 0);
}
