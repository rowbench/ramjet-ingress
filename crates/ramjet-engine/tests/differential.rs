//! The same traffic through both engines, compared answer for answer.
//!
//! # Why this exists
//!
//! There are two data planes now, and the promise made about them is that a
//! client cannot tell which one served it. That promise is not testable by
//! reading either engine: it is a statement about the *difference* between
//! them, and every test that asserts one engine's behaviour against a literal
//! is a test that will keep passing after the other engine's behaviour drifts.
//!
//! So this file starts both, drives them with byte-identical requests against
//! byte-identical route tables, and asserts on three things:
//!
//! 1. **The answer.** Status, and the response body where it is one the proxy
//!    invented rather than relayed.
//! 2. **What the upstream saw.** The full rewritten head, reflected back by the
//!    echo upstream, compared field by field. This is the strictest of the
//!    three: it catches a header written in a different order, a different
//!    case, a missing `X-Forwarded-Host`, an `X-Forwarded-For` that replaced
//!    the trail instead of extending it.
//! 3. **The counter deltas.** Scraped before and after, and subtracted, so the
//!    comparison does not depend on either engine starting from zero.
//!
//! # What is deliberately not compared
//!
//! `X-Request-Id` when the client sent none: it is 32 random hex characters by
//! design, and two engines agreeing on it would mean the randomness was broken.
//! Its *presence* and *length* are compared, and an inbound id is compared
//! exactly, which is what actually matters — a trace has to survive the hop.

mod common;

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use common::*;
use ramjet_router::{CanaryRules, Endpoint, LbPolicy, PathType, RouteTable, RouteTableBuilder};

// ------------------------------------------------------------- the hyper lane

/// The hyper engine, on a runtime of its own, stopped when dropped.
struct HyperProxy {
    addr: SocketAddr,
    admin: SocketAddr,
    routes: Arc<ramjet_router::SharedRouteTable>,
    handle: Option<ramjet_proxy::ShutdownHandle>,
    runtime: Option<tokio::runtime::Runtime>,
    task: Option<std::thread::JoinHandle<()>>,
}

impl Drop for HyperProxy {
    fn drop(&mut self) {
        if let Some(handle) = self.handle.take() {
            handle.shutdown();
        }
        if let Some(task) = self.task.take() {
            let _ = task.join();
        }
        drop(self.runtime.take());
    }
}

impl HyperProxy {
    fn start(table: RouteTable) -> HyperProxy {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .expect("a runtime");

        let routes = Arc::new(ramjet_router::SharedRouteTable::new(table));
        let config = ramjet_proxy::ProxyConfig {
            http: Some(ramjet_proxy::ListenerConfig::new(SocketAddr::from((
                [127, 0, 0, 1],
                0,
            )))),
            https: None,
            admin: Some(ramjet_proxy::ListenerConfig::new(SocketAddr::from((
                [127, 0, 0, 1],
                0,
            )))),
            // One serving runtime, matching the uring side's one core, so the
            // counter comparison is not comparing two shard counts.
            worker_threads: Some(1),
            ..ramjet_proxy::ProxyConfig::default()
        };

        let server = runtime
            .block_on(async {
                ramjet_proxy::Server::bind_with(
                    config,
                    Arc::clone(&routes),
                    Arc::new(ramjet_proxy::CertStore::new()),
                    ramjet_proxy::ReadinessFlag::new(),
                )
            })
            .expect("the hyper engine bound");
        server.readiness().set_ready(true);

        let addr = server.http_addr().expect("an http listener");
        let admin = server.admin_addr().expect("an admin listener");
        let (handle, shutdown) = ramjet_proxy::Shutdown::channel();

        let task = {
            let runtime_handle = runtime.handle().clone();
            std::thread::spawn(move || {
                let _ = runtime_handle.block_on(server.run(shutdown));
            })
        };

        let proxy = HyperProxy {
            addr,
            admin,
            routes,
            handle: Some(handle),
            runtime: Some(runtime),
            task: Some(task),
        };
        proxy.wait_until_listening();
        proxy
    }

    fn wait_until_listening(&self) {
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while std::time::Instant::now() < deadline {
            if std::net::TcpStream::connect(self.addr).is_ok() {
                return;
            }
            std::thread::sleep(Duration::from_millis(5));
        }
        panic!("the hyper engine never started listening on {}", self.addr);
    }

    fn admin(&self, path: &str) -> Response {
        let mut client = Client::connect(self.admin);
        client.send(format!("GET {path} HTTP/1.1\r\nHost: admin\r\n\r\n").as_bytes())
    }
}

// ------------------------------------------------------------- the comparison

/// Both engines, each with its own upstream, over the same table.
struct Pair {
    hyper: HyperProxy,
    uring: Proxy,
    hyper_upstream: Upstream,
    uring_upstream: Upstream,
}

/// Build the same table twice, once per engine.
///
/// Two tables rather than one shared: the per-route counters live *in* the
/// table, so sharing one would sum both engines' traffic into the same block
/// and make the delta comparison meaningless.
fn pair(build: impl Fn(&mut RouteTableBuilder, SocketAddr)) -> Pair {
    let hyper_upstream = echo();
    let uring_upstream = echo();

    let mut hyper_builder = RouteTableBuilder::new();
    build(&mut hyper_builder, hyper_upstream.addr);
    let mut uring_builder = RouteTableBuilder::new();
    build(&mut uring_builder, uring_upstream.addr);

    Pair {
        hyper: HyperProxy::start(hyper_builder.build().expect("a valid table")),
        uring: Proxy::start(uring_builder.build().expect("a valid table")),
        hyper_upstream,
        uring_upstream,
    }
}

/// The headers an echo upstream reflected, as a comparable map.
///
/// `echo-*` prefixes are stripped, so what comes back is exactly the head the
/// upstream received. The reflection is case-folded by the upstream already.
fn upstream_head(response: &Response) -> BTreeMap<String, String> {
    response
        .headers
        .iter()
        .filter_map(|(name, value)| {
            name.strip_prefix("echo-")
                .map(|name| (name.to_ascii_lowercase(), value.clone()))
        })
        .collect()
}

/// Every counter in an exposition, by series name including its labels.
fn counters(exposition: &str) -> BTreeMap<String, f64> {
    exposition
        .lines()
        .filter(|line| !line.starts_with('#'))
        .filter_map(|line| {
            let (name, value) = line.rsplit_once(' ')?;
            Some((name.to_owned(), value.parse().ok()?))
        })
        .collect()
}

/// What changed between two scrapes.
fn delta(before: &BTreeMap<String, f64>, after: &BTreeMap<String, f64>) -> BTreeMap<String, f64> {
    after
        .iter()
        .map(|(name, value)| {
            let was = before.get(name).copied().unwrap_or(0.0);
            (name.clone(), value - was)
        })
        .collect()
}

/// Assert that two rewritten heads agree everywhere it is possible to agree.
fn assert_same_head(hyper: &Response, uring: &Response, context: &str) {
    let mut left = upstream_head(hyper);
    let mut right = upstream_head(uring);

    // The one field that must differ, and would be a bug if it did not: an id
    // this hop generated is 32 random hex characters.
    for map in [&mut left, &mut right] {
        if let Some(id) = map.remove("x-request-id") {
            assert_eq!(
                id.len(),
                32,
                "{context}: a generated request id must be 32 hex characters, got {id:?}"
            );
            assert!(
                id.chars().all(|c| c.is_ascii_hexdigit()),
                "{context}: a generated request id must be hex, got {id:?}"
            );
        }
    }

    assert_eq!(
        left.keys().collect::<Vec<_>>(),
        right.keys().collect::<Vec<_>>(),
        "{context}: the two engines sent different header *names* upstream"
    );
    for (name, hyper_value) in &left {
        assert_eq!(
            Some(hyper_value),
            right.get(name),
            "{context}: the two engines disagree on {name}"
        );
    }
}

/// Send the same request to both engines and compare everything comparable.
fn compare(pair: &Pair, request: &str) -> (Response, Response) {
    let before_hyper = counters(&pair.hyper.admin("/metrics").text());
    let before_uring = counters(&pair.uring.admin("/metrics").text());

    let hyper = Client::connect(pair.hyper.addr).send(request.as_bytes());
    let uring = Client::connect(pair.uring.addr).send(request.as_bytes());

    assert_eq!(
        hyper.status, uring.status,
        "the two engines answered {request:?} with different statuses"
    );
    if hyper.status < 500 || hyper.body == uring.body {
        // A relayed body is the upstream's and must match; an invented one is
        // the proxy's, and `limits.rs` already pins those literal for literal.
        assert_eq!(
            hyper.body, uring.body,
            "the two engines returned different bodies for {request:?}"
        );
    }

    let after_hyper = counters(&pair.hyper.admin("/metrics").text());
    let after_uring = counters(&pair.uring.admin("/metrics").text());
    let hyper_delta = delta(&before_hyper, &after_hyper);
    let uring_delta = delta(&before_uring, &after_uring);

    for (series, hyper_change) in &hyper_delta {
        // The gauges move with connection lifetime and scrape timing rather
        // than with the request, and the latency histogram measures a real
        // upstream. Neither is a routing decision.
        if series.starts_with("ramjet_upstream_latency_seconds")
            || series.starts_with("ramjet_active_connections")
            || series.starts_with("ramjet_route_table_generation")
        {
            continue;
        }
        assert_eq!(
            Some(hyper_change),
            uring_delta.get(series),
            "the two engines moved {series} differently for {request:?}\n\
             hyper: {hyper_delta:#?}\nuring: {uring_delta:#?}"
        );
    }

    (hyper, uring)
}

fn one_route(builder: &mut RouteTableBuilder, upstream: SocketAddr) {
    builder
        .backend("app", LbPolicy::RoundRobin, vec![Endpoint::new(upstream)])
        .expect("a valid backend");
    builder
        .route(Some("app.example.com"), "/", PathType::Prefix, "app")
        .expect("a valid route");
}

// -------------------------------------------------------------------- tests

#[test]
fn an_ordinary_request_is_answered_identically() {
    let pair = pair(one_route);
    let (hyper, uring) = compare(
        &pair,
        "GET /api/v1/users?limit=10 HTTP/1.1\r\nHost: app.example.com\r\n\r\n",
    );
    assert_eq!(hyper.status, 200);
    assert_same_head(&hyper, &uring, "an ordinary GET");
    assert_eq!(pair.hyper_upstream.seen.requests(), 1);
    assert_eq!(pair.uring_upstream.seen.requests(), 1);
}

#[test]
fn the_forwarded_headers_are_written_the_same_way() {
    // The set this hop invents, and the one place two independent
    // implementations most easily drift.
    let pair = pair(one_route);
    let (hyper, uring) = compare(
        &pair,
        "GET / HTTP/1.1\r\nHost: app.example.com\r\nUser-Agent: differential/1\r\n\r\n",
    );

    for (engine, response) in [("hyper", &hyper), ("uring", &uring)] {
        let head = upstream_head(response);
        assert_eq!(
            head.get("x-forwarded-proto").map(String::as_str),
            Some("http"),
            "{engine}"
        );
        assert_eq!(
            head.get("x-forwarded-host").map(String::as_str),
            Some("app.example.com"),
            "{engine}"
        );
        assert_eq!(
            head.get("x-real-ip").map(String::as_str),
            Some("127.0.0.1"),
            "{engine}"
        );
        assert_eq!(
            head.get("user-agent").map(String::as_str),
            Some("differential/1"),
            "{engine}: a header the client sent must cross unchanged"
        );
    }
    assert_same_head(&hyper, &uring, "the forwarded set");
}

#[test]
fn an_inbound_forwarded_trail_is_extended_the_same_way() {
    // Several inbound lines collapse into one, joined by ", ", with this hop
    // appended. A proxy that replaces instead of extending erases the client,
    // and doing it differently on two lanes would show up as a different
    // client IP depending on which engine served.
    let pair = pair(one_route);
    let (hyper, uring) = compare(
        &pair,
        "GET / HTTP/1.1\r\nHost: app.example.com\r\n\
         X-Forwarded-For: 203.0.113.1\r\nX-Forwarded-For: 198.51.100.2\r\n\r\n",
    );
    assert_eq!(
        upstream_head(&hyper).get("x-forwarded-for").map(String::as_str),
        Some("203.0.113.1, 198.51.100.2, 127.0.0.1")
    );
    assert_same_head(&hyper, &uring, "an accumulated trail");
}

#[test]
fn an_inbound_request_id_survives_both_hops_identically() {
    let pair = pair(one_route);
    let (hyper, uring) = compare(
        &pair,
        "GET / HTTP/1.1\r\nHost: app.example.com\r\nX-Request-Id: trace-me-123\r\n\r\n",
    );
    for (engine, response) in [("hyper", &hyper), ("uring", &uring)] {
        assert_eq!(
            upstream_head(response).get("x-request-id").map(String::as_str),
            Some("trace-me-123"),
            "{engine}: an inbound id must be preserved, not regenerated"
        );
    }
}

#[test]
fn hop_by_hop_headers_are_stripped_the_same_way() {
    // `Connection` names its own hop-by-hop headers, and stripping the fixed
    // list while forwarding what `Connection` named is the classic version of
    // this bug.
    let pair = pair(one_route);
    let (hyper, uring) = compare(
        &pair,
        "GET / HTTP/1.1\r\nHost: app.example.com\r\n\
         Connection: keep-alive, x-hop-secret\r\nX-Hop-Secret: do-not-forward\r\n\
         Keep-Alive: timeout=5\r\nKeep: kept\r\n\r\n",
    );
    for (engine, response) in [("hyper", &hyper), ("uring", &uring)] {
        let head = upstream_head(response);
        assert!(
            !head.contains_key("x-hop-secret"),
            "{engine}: a header named by Connection must not cross the hop"
        );
        assert!(!head.contains_key("keep-alive"), "{engine}");
        assert_eq!(
            head.get("keep").map(String::as_str),
            Some("kept"),
            "{engine}: an ordinary header must still cross"
        );
    }
    assert_same_head(&hyper, &uring, "hop-by-hop stripping");
}

#[test]
fn an_unmatched_host_is_a_404_on_both() {
    let pair = pair(one_route);
    let (hyper, _) = compare(
        &pair,
        "GET / HTTP/1.1\r\nHost: nobody.invalid\r\nConnection: close\r\n\r\n",
    );
    assert_eq!(hyper.status, 404);
    assert_eq!(pair.hyper_upstream.seen.requests(), 0);
    assert_eq!(pair.uring_upstream.seen.requests(), 0);
}

#[test]
fn a_backend_with_no_endpoints_is_a_503_on_both() {
    let pair = pair(|builder, _upstream| {
        builder
            .backend("app", LbPolicy::RoundRobin, vec![])
            .expect("a valid backend");
        builder
            .route(Some("app.example.com"), "/", PathType::Prefix, "app")
            .expect("a valid route");
    });
    let (hyper, _) = compare(
        &pair,
        "GET / HTTP/1.1\r\nHost: app.example.com\r\nConnection: close\r\n\r\n",
    );
    assert_eq!(hyper.status, 503);
}

#[test]
fn a_dead_endpoint_is_a_502_on_both() {
    let dead = dead_addr();
    let pair = pair(move |builder, _upstream| {
        builder
            .backend("app", LbPolicy::RoundRobin, vec![Endpoint::new(dead)])
            .expect("a valid backend");
        builder
            .route(Some("app.example.com"), "/", PathType::Prefix, "app")
            .expect("a valid route");
    });
    let (hyper, _) = compare(
        &pair,
        "GET / HTTP/1.1\r\nHost: app.example.com\r\nConnection: close\r\n\r\n",
    );
    assert_eq!(hyper.status, 502);
}

#[test]
fn a_grpc_request_is_refused_the_same_way_on_both() {
    let pair = pair(one_route);
    let (hyper, _) = compare(
        &pair,
        "POST /svc/Method HTTP/1.1\r\nHost: app.example.com\r\n\
         Content-Type: application/grpc+proto\r\nContent-Length: 0\r\n\
         Connection: close\r\n\r\n",
    );
    assert_eq!(hyper.status, 502);
}

#[test]
fn a_request_with_a_body_crosses_both_the_same_way() {
    let pair = pair(one_route);
    let (hyper, uring) = compare(
        &pair,
        "POST /submit HTTP/1.1\r\nHost: app.example.com\r\n\
         Content-Type: application/json\r\nContent-Length: 17\r\n\r\n\
         {\"hello\":\"world\"}",
    );
    assert_eq!(hyper.status, 200);
    assert_same_head(&hyper, &uring, "a POST with a body");
    for (engine, response) in [("hyper", &hyper), ("uring", &uring)] {
        assert_eq!(
            upstream_head(response).get("body-len").map(String::as_str),
            Some("17"),
            "{engine}: the body did not cross intact"
        );
    }
}

#[test]
fn a_canary_diverts_the_same_share_and_counts_it_the_same_way() {
    // The share itself is random, so the assertion is on the *rule*: a header
    // that says `always` diverts on both, one that says `never` diverts on
    // neither, and the counters move in the same blocks either way.
    let pair = pair(|builder, upstream| {
        builder
            .backend("app", LbPolicy::RoundRobin, vec![Endpoint::new(upstream)])
            .expect("a valid backend");
        builder
            .backend("canary", LbPolicy::RoundRobin, vec![Endpoint::new(upstream)])
            .expect("a valid backend");
        builder
            .canary_route(
                Some("app.example.com"),
                "/",
                PathType::Prefix,
                "app",
                &CanaryRules {
                    backend: "canary",
                    header: Some("x-canary"),
                    weight: 0,
                    ..Default::default()
                },
            )
            .expect("a valid canary route");
    });

    for decision in ["always", "never"] {
        let (hyper, uring) = compare(
            &pair,
            &format!(
                "GET / HTTP/1.1\r\nHost: app.example.com\r\nX-Canary: {decision}\r\n\r\n"
            ),
        );
        assert_eq!(hyper.status, 200, "x-canary: {decision}");
        assert_same_head(&hyper, &uring, &format!("a canary saying {decision}"));
    }
}

#[test]
fn per_route_counters_move_in_the_same_blocks() {
    // The counters `/admin/routes` and `ramjet-top` read. They live in the
    // route table rather than in either engine's metrics, so this reads them
    // straight out of the two tables.
    let pair = pair(|builder, upstream| {
        builder
            .backend("app", LbPolicy::RoundRobin, vec![Endpoint::new(upstream)])
            .expect("a valid backend");
        builder
            .backend("canary", LbPolicy::RoundRobin, vec![Endpoint::new(upstream)])
            .expect("a valid backend");
        builder
            .canary_route(
                Some("app.example.com"),
                "/",
                PathType::Prefix,
                "app",
                &CanaryRules {
                    backend: "canary",
                    header: Some("x-canary"),
                    weight: 0,
                    ..Default::default()
                },
            )
            .expect("a valid canary route");
    });

    // Two stable requests and three the canary takes.
    for _ in 0..2 {
        Client::connect(pair.hyper.addr)
            .send(b"GET / HTTP/1.1\r\nHost: app.example.com\r\nX-Canary: never\r\n\r\n");
        Client::connect(pair.uring.addr)
            .send(b"GET / HTTP/1.1\r\nHost: app.example.com\r\nX-Canary: never\r\n\r\n");
    }
    for _ in 0..3 {
        Client::connect(pair.hyper.addr)
            .send(b"GET / HTTP/1.1\r\nHost: app.example.com\r\nX-Canary: always\r\n\r\n");
        Client::connect(pair.uring.addr)
            .send(b"GET / HTTP/1.1\r\nHost: app.example.com\r\nX-Canary: always\r\n\r\n");
    }

    let hyper_table = pair.hyper.routes.load_full();
    let uring_table = pair.uring.routes.load_full();
    let hyper_slot = hyper_table.route_stats().slot(0).expect("a route slot");
    let uring_slot = uring_table.route_stats().slot(0).expect("a route slot");

    let hyper_totals = hyper_slot.totals();
    let uring_totals = uring_slot.totals();
    assert_eq!(
        hyper_totals.requests, 5,
        "the route's own block counts every request that matched the rule"
    );
    assert_eq!(
        uring_totals.requests, hyper_totals.requests,
        "the two engines attributed a different number of requests to the route"
    );

    let hyper_canary = hyper_slot.canary_totals();
    let uring_canary = uring_slot.canary_totals();
    assert_eq!(
        hyper_canary.requests, 3,
        "the canary block counts only what the canary took"
    );
    assert_eq!(
        uring_canary.requests, hyper_canary.requests,
        "the two engines split canary traffic differently"
    );
}

#[test]
fn a_default_backend_is_attributed_to_no_route_on_either() {
    // A request the default backend answered matched no rule, and inventing
    // one to attribute it to would put traffic against a route that is not in
    // the table.
    let pair = pair(|builder, upstream| {
        builder
            .backend("app", LbPolicy::RoundRobin, vec![Endpoint::new(upstream)])
            .expect("a valid backend");
        builder.default_backend("app");
    });

    let (hyper, uring) = compare(
        &pair,
        "GET /anything HTTP/1.1\r\nHost: unmatched.example.com\r\n\r\n",
    );
    assert_eq!(hyper.status, 200);
    assert_same_head(&hyper, &uring, "the default backend");

    for (engine, routes) in [
        ("hyper", &pair.hyper.routes),
        ("uring", &pair.uring.routes),
    ] {
        let table = routes.load_full();
        assert!(
            table.route_stats().is_empty()
                || table
                    .route_stats()
                    .slot(0)
                    .is_none_or(|slot| slot.totals().requests == 0),
            "{engine}: a default-backend request was attributed to a route"
        );
    }
}
