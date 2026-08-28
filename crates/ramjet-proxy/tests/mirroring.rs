//! Traffic mirroring, end to end.
//!
//! The router tests the sampling rule against integers. What is left to prove
//! here is the half that only exists once there are sockets involved: that the
//! copy is actually a copy, that it carries its marker, and — the property the
//! whole design is arranged around — that a mirror backend which is broken,
//! absent, or catatonic costs the request the client is waiting for nothing at
//! all.

mod common;

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use common::*;
use http::{Request, Response};
use http_body_util::BodyExt;
use hyper::body::Incoming;
use ramjet_router::{
    Endpoint, LbPolicy, MirrorRules, PathType, RouteOptions, RouteTable, RouteTableBuilder,
};
use tokio::sync::mpsc;

/// What a mirror backend saw.
#[derive(Debug)]
struct Seen {
    method: String,
    path: String,
    host: Option<String>,
    marker: Option<String>,
    body: String,
}

/// An upstream that reports every request it receives down a channel.
async fn spawn_recorder() -> (SocketAddr, mpsc::UnboundedReceiver<Seen>) {
    let (tx, rx) = mpsc::unbounded_channel();
    let addr = spawn_http(move |request: Request<Incoming>| {
        let tx = tx.clone();
        async move {
            let method = request.method().to_string();
            let path = request
                .uri()
                .path_and_query()
                .map_or_else(|| "/".to_owned(), |p| p.to_string());
            let header = |name: &str| {
                request
                    .headers()
                    .get(name)
                    .and_then(|v| v.to_str().ok())
                    .map(str::to_owned)
            };
            let host = header("host");
            let marker = header("x-mirrored-by");
            let body = request
                .into_body()
                .collect()
                .await
                .map(|c| String::from_utf8_lossy(&c.to_bytes()).into_owned())
                .unwrap_or_default();
            let _ = tx.send(Seen {
                method,
                path,
                host,
                marker,
                body,
            });
            Response::new(full("recorded"))
        }
    })
    .await;
    (addr, rx)
}

/// A table routing `app.example.com` to `primary` with a mirror to `shadow`.
fn mirrored_table(primary: SocketAddr, shadow: SocketAddr, rules: MirrorRules<'_>) -> RouteTable {
    let mut builder = RouteTableBuilder::new();
    builder
        .backend("prod", LbPolicy::RoundRobin, vec![Endpoint::new(primary)])
        .expect("backend");
    builder
        .backend("shadow", LbPolicy::RoundRobin, vec![Endpoint::new(shadow)])
        .expect("backend");
    builder
        .route_with(
            Some("app.example.com"),
            "/",
            PathType::Prefix,
            "prod",
            &RouteOptions {
                mirror: Some(rules),
                ..Default::default()
            },
        )
        .expect("route");
    builder.build().expect("table")
}

fn to_shadow(percent: u32) -> MirrorRules<'static> {
    MirrorRules {
        backend: "shadow",
        percent,
        host: None,
    }
}

/// Waits for the next recorded request, failing rather than hanging.
async fn next_seen(rx: &mut mpsc::UnboundedReceiver<Seen>) -> Seen {
    tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("the mirror backend received a copy")
        .expect("the recorder is still running")
}

#[tokio::test]
async fn the_mirror_backend_receives_an_identical_copy() {
    let primary = spawn_echo("prod").await;
    let (shadow, mut seen) = spawn_recorder().await;
    let proxy = TestProxy::start(mirrored_table(primary, shadow, to_shadow(100))).await;

    let reply = send(
        proxy.http,
        request("app.example.com", "/orders?page=2")
            .method("POST")
            .header("x-trace", "abc123")
            .body(full("the payload"))
            .expect("a request"),
    )
    .await;

    // The primary is untouched: same upstream, same response.
    assert_eq!(reply.status, 200);
    assert_eq!(reply.upstream(), "prod");
    assert_eq!(reply.text(), "POST /orders?page=2");

    let copy = next_seen(&mut seen).await;
    assert_eq!(copy.method, "POST");
    assert_eq!(copy.path, "/orders?page=2");
    assert_eq!(copy.body, "the payload");
    assert_eq!(
        copy.marker.as_deref(),
        Some("ramjet-ingress"),
        "a shadow backend has to be able to tell a copy from the real thing"
    );
    assert_eq!(
        copy.host.as_deref(),
        Some("app.example.com"),
        "without an override the copy keeps the name the client addressed"
    );
}

#[tokio::test]
async fn the_host_override_replaces_the_name_the_client_used() {
    let primary = spawn_echo("prod").await;
    let (shadow, mut seen) = spawn_recorder().await;
    let proxy = TestProxy::start(mirrored_table(
        primary,
        shadow,
        MirrorRules {
            backend: "shadow",
            percent: 100,
            host: Some("shadow.internal"),
        },
    ))
    .await;

    let reply = get(proxy.http, "app.example.com", "/").await;
    assert_eq!(reply.upstream(), "prod");

    let copy = next_seen(&mut seen).await;
    assert_eq!(copy.host.as_deref(), Some("shadow.internal"));
}

#[tokio::test]
async fn a_percent_of_zero_mirrors_nothing() {
    let primary = spawn_echo("prod").await;
    let (shadow, mut seen) = spawn_recorder().await;
    let proxy = TestProxy::start(mirrored_table(primary, shadow, to_shadow(0))).await;

    let replies = send_many(proxy.http, "app.example.com", "/", 50).await;
    assert!(replies.iter().all(|r| r.upstream() == "prod"));

    // Nothing arrives, and the counter agrees. Both halves matter: a mirror
    // that was never attempted and one that was attempted and failed look the
    // same from the shadow backend's side.
    assert!(
        tokio::time::timeout(Duration::from_millis(300), seen.recv())
            .await
            .is_err(),
        "percent 0 must send nothing at all"
    );
    assert_eq!(proxy.metrics.mirrored(), 0);
    assert_eq!(proxy.metrics.mirror_failures(), 0);
    assert_eq!(proxy.metrics.mirror_dropped(), 0);
}

#[tokio::test]
async fn a_sampled_share_is_roughly_the_configured_one() {
    const REQUESTS: usize = 400;

    let primary = spawn_echo("prod").await;
    let (shadow, mut seen) = spawn_recorder().await;
    let proxy = TestProxy::start(mirrored_table(primary, shadow, to_shadow(25))).await;

    let replies = send_many(proxy.http, "app.example.com", "/", REQUESTS).await;
    assert!(replies.iter().all(|r| r.status == 200));

    // Drain what has arrived, giving the worker a moment to catch up. The
    // bounds are wide on purpose: this asserts the percentage is applied at
    // all, not the quality of the generator, which the router already covers.
    tokio::time::sleep(Duration::from_millis(500)).await;
    let mut copies = 0;
    while seen.try_recv().is_ok() {
        copies += 1;
    }
    assert!(
        (50..=170).contains(&copies),
        "{copies} of {REQUESTS} were mirrored, which is not a 25% sample"
    );
}

/// The invariant the whole feature is arranged around, in the form that would
/// actually catch a regression: an upstream that accepts and then never answers.
///
/// A dead address would not prove much — `ECONNREFUSED` on loopback returns
/// instantly, so even a request path that wrongly awaited the copy would look
/// fast. A black hole holds the mirror for the full mirror timeout, so if the
/// primary were waiting on it, each request would take seconds.
#[tokio::test]
async fn a_catatonic_mirror_backend_does_not_slow_the_primary() {
    let primary = spawn_echo("prod").await;
    let shadow = spawn_black_hole().await;
    let proxy = TestProxy::start(mirrored_table(primary, shadow, to_shadow(100))).await;

    let started = Instant::now();
    let replies = send_many(proxy.http, "app.example.com", "/", 40).await;
    let elapsed = started.elapsed();

    assert!(replies.iter().all(|r| r.status == 200 && r.upstream() == "prod"));
    assert!(
        elapsed < Duration::from_secs(2),
        "40 requests took {elapsed:?}; the primary is waiting on the mirror"
    );
}

/// The bound doing its job. One wedged mirror fills a runtime's queue, and the
/// overflow is counted and discarded rather than queued without limit or —
/// far worse — pushed back onto the request path.
#[tokio::test]
async fn a_backed_up_mirror_drops_copies_instead_of_growing() {
    let primary = spawn_echo("prod").await;
    let shadow = spawn_black_hole().await;
    let proxy = TestProxy::start(mirrored_table(primary, shadow, to_shadow(100))).await;

    // Comfortably more than the queue depth, so the drop is reached whatever
    // the worker manages to accept first.
    let replies = send_many(proxy.http, "app.example.com", "/", 600).await;
    assert!(replies.iter().all(|r| r.status == 200));
    assert!(
        proxy.metrics.mirror_dropped() > 0,
        "a wedged mirror must overflow a bounded queue, not an unbounded one"
    );
}

#[tokio::test]
async fn a_body_over_the_cap_skips_the_mirror_and_still_reaches_the_primary() {
    let primary = spawn_echo("prod").await;
    let (shadow, mut seen) = spawn_recorder().await;
    let proxy = TestProxy::start_with(
        mirrored_table(primary, shadow, to_shadow(100)),
        ProxyOptions {
            mirror_max_body: 16,
            ..Default::default()
        },
    )
    .await;

    let payload = "x".repeat(4096);
    let reply = send(
        proxy.http,
        request("app.example.com", "/upload")
            .method("POST")
            .body(full(payload.clone()))
            .expect("a request"),
    )
    .await;

    assert_eq!(reply.status, 200, "the primary must be unaffected");
    assert_eq!(reply.text(), "POST /upload");
    assert_eq!(
        reply.header("echo-content-length"),
        Some(payload.len().to_string()).as_deref(),
        "the whole body must reach the primary, prefix and remainder alike"
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(300), seen.recv())
            .await
            .is_err(),
        "a body over the cap must not be mirrored"
    );
    assert_eq!(proxy.metrics.mirror_skipped(), 1);
    assert_eq!(proxy.metrics.mirrored(), 0);
}

#[tokio::test]
async fn a_body_within_the_cap_reaches_both_sides_whole() {
    let primary = spawn_echo("prod").await;
    let (shadow, mut seen) = spawn_recorder().await;
    let proxy = TestProxy::start_with(
        mirrored_table(primary, shadow, to_shadow(100)),
        ProxyOptions {
            mirror_max_body: 8192,
            ..Default::default()
        },
    )
    .await;

    let payload = "y".repeat(4096);
    let reply = send(
        proxy.http,
        request("app.example.com", "/upload")
            .method("PUT")
            .body(full(payload.clone()))
            .expect("a request"),
    )
    .await;
    assert_eq!(reply.status, 200);
    assert_eq!(
        reply.header("echo-content-length"),
        Some(payload.len().to_string()).as_deref()
    );

    let copy = next_seen(&mut seen).await;
    assert_eq!(copy.body.len(), payload.len());
    assert_eq!(copy.body, payload);
    assert_eq!(proxy.metrics.mirror_skipped(), 0);
}

#[tokio::test]
async fn a_mirror_backend_with_no_endpoints_is_counted_not_ignored() {
    // The distinction an operator needs: "the shadow Service has no ready
    // pods" has to look different from "the annotation never took effect".
    let primary = spawn_echo("prod").await;
    let mut builder = RouteTableBuilder::new();
    builder
        .backend("prod", LbPolicy::RoundRobin, vec![Endpoint::new(primary)])
        .expect("backend");
    builder
        .backend("shadow", LbPolicy::RoundRobin, Vec::new())
        .expect("an empty backend is a normal state");
    builder
        .route_with(
            Some("app.example.com"),
            "/",
            PathType::Prefix,
            "prod",
            &RouteOptions {
                mirror: Some(to_shadow(100)),
                ..Default::default()
            },
        )
        .expect("route");
    let proxy = TestProxy::start(builder.build().expect("table")).await;

    let reply = get(proxy.http, "app.example.com", "/").await;
    assert_eq!(reply.status, 200, "the primary is unaffected");
    assert_eq!(proxy.metrics.mirror_failures(), 1);
    assert_eq!(proxy.metrics.mirrored(), 0);
}

#[tokio::test]
async fn a_route_with_no_mirror_makes_no_copies() {
    let primary = spawn_echo("prod").await;
    let proxy = TestProxy::start(single_route("app.example.com", "/", &[primary])).await;

    let replies = send_many(proxy.http, "app.example.com", "/", 20).await;
    assert!(replies.iter().all(|r| r.status == 200));
    assert_eq!(
        (
            proxy.metrics.mirrored(),
            proxy.metrics.mirror_dropped(),
            proxy.metrics.mirror_skipped(),
            proxy.metrics.mirror_failures()
        ),
        (0, 0, 0, 0)
    );
}

/// Keeps the `Arc` import honest: the harness hands out shared metrics and this
/// file reads them through it.
#[allow(dead_code)]
fn _metrics_are_shared(proxy: &TestProxy) -> Arc<ramjet_proxy::Metrics> {
    Arc::clone(&proxy.metrics)
}

// ---------------------------------------------------------------------------
// A shadow backend on the other protocol
// ---------------------------------------------------------------------------

/// Waits, briefly and boundedly, for `ready` to hold.
async fn settle(ready: impl Fn() -> bool) {
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while !ready() && std::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// The same recorder, behind a cleartext HTTP/2 listener.
async fn spawn_h2c_recorder() -> (SocketAddr, mpsc::UnboundedReceiver<Seen>) {
    let (tx, rx) = mpsc::unbounded_channel();
    let addr = spawn_h2c(move |request: Request<Incoming>| {
        let tx = tx.clone();
        async move {
            let method = request.method().to_string();
            let path = request
                .uri()
                .path_and_query()
                .map_or_else(|| "/".to_owned(), |p| p.to_string());
            let header = |name: &str| {
                request
                    .headers()
                    .get(name)
                    .and_then(|v| v.to_str().ok())
                    .map(str::to_owned)
            };
            // `x-forwarded-host` rather than `host`: an h2 request carries its
            // authority in `:authority`, which names the endpoint, so the client's
            // name arrives in the forwarded header. Recorded into the same field
            // so the assertions below read like the HTTP/1.1 ones.
            let host = header("x-forwarded-host");
            let marker = header("x-mirrored-by");
            let body = request
                .into_body()
                .collect()
                .await
                .map(|c| String::from_utf8_lossy(&c.to_bytes()).into_owned())
                .unwrap_or_default();
            let _ = tx.send(Seen {
                method,
                path,
                host,
                marker,
                body,
            });
            Response::new(full("recorded"))
        }
    })
    .await;
    (addr, rx)
}

/// A table whose primary is HTTP/1.1 and whose shadow is dialled over h2c.
fn crossed_table(primary: SocketAddr, shadow: SocketAddr) -> RouteTable {
    use ramjet_router::{BackendOptions, BackendProtocol};

    let mut builder = RouteTableBuilder::new();
    builder
        .backend("prod", LbPolicy::RoundRobin, vec![Endpoint::new(primary)])
        .expect("backend");
    builder
        .backend_with(
            "shadow",
            vec![Endpoint::new(shadow)],
            &BackendOptions {
                policy: LbPolicy::RoundRobin,
                protocol: BackendProtocol::H2c,
            },
        )
        .expect("backend");
    builder
        .route_with(
            Some("app.example.com"),
            "/",
            PathType::Prefix,
            "prod",
            &RouteOptions {
                mirror: Some(to_shadow(100)),
                ..Default::default()
            },
        )
        .expect("route");
    builder.build().expect("table")
}

#[tokio::test]
async fn a_copy_is_sent_with_the_shadows_own_protocol_not_the_primarys() {
    // A shadow Service is annotated independently of the production one, so the
    // copy has to be re-versioned for its own backend rather than inheriting
    // the primary's. This is the case that breaks if it is not: an HTTP/1.1
    // primary and an h2c shadow, where the copy was built from a request head
    // already rewritten for HTTP/1.1.
    let primary = spawn_echo("prod").await;
    let (shadow, mut seen) = spawn_h2c_recorder().await;
    let proxy = TestProxy::start(crossed_table(primary, shadow)).await;

    let reply = send(
        proxy.http,
        request("app.example.com", "/orders")
            .method("POST")
            .body(full("the payload"))
            .expect("a request"),
    )
    .await;

    assert_eq!(reply.status, 200);
    assert_eq!(reply.upstream(), "prod", "the primary is untouched");

    let copy = next_seen(&mut seen).await;
    assert_eq!(copy.method, "POST");
    assert_eq!(copy.path, "/orders");
    assert_eq!(copy.body, "the payload");
    assert_eq!(copy.marker.as_deref(), Some("ramjet-ingress"));
    assert_eq!(
        copy.host.as_deref(),
        Some("app.example.com"),
        "the client's name reaches an h2c shadow as X-Forwarded-Host"
    );

    // The counter, not just the arrival, because only the counter says the h2
    // *exchange* completed: the recorder reports on receipt, while the worker
    // records after it has drained the response and put the connection back.
    // Bounded wait rather than an immediate read — the two are legitimately a
    // few microseconds apart, and asserting on the gap would be a flake.
    settle(|| proxy.metrics.mirrored() == 1).await;
    assert_eq!(proxy.metrics.mirrored(), 1);
    assert_eq!(proxy.metrics.mirror_failures(), 0);
}

#[tokio::test]
async fn a_mirror_host_override_reaches_an_h2c_shadow_as_forwarded_host() {
    // `mirror-host` writes the override into `Host`, which an h2c backend must
    // not receive — so it has to be moved rather than dropped, or the override
    // silently stops working the moment the shadow is annotated.
    use ramjet_router::{BackendOptions, BackendProtocol};

    let primary = spawn_echo("prod").await;
    let (shadow, mut seen) = spawn_h2c_recorder().await;

    let mut builder = RouteTableBuilder::new();
    builder
        .backend("prod", LbPolicy::RoundRobin, vec![Endpoint::new(primary)])
        .expect("backend");
    builder
        .backend_with(
            "shadow",
            vec![Endpoint::new(shadow)],
            &BackendOptions {
                policy: LbPolicy::RoundRobin,
                protocol: BackendProtocol::H2c,
            },
        )
        .expect("backend");
    builder
        .route_with(
            Some("app.example.com"),
            "/",
            PathType::Prefix,
            "prod",
            &RouteOptions {
                mirror: Some(MirrorRules {
                    backend: "shadow",
                    percent: 100,
                    host: Some("shadow.example.com"),
                }),
                ..Default::default()
            },
        )
        .expect("route");
    let proxy = TestProxy::start(builder.build().expect("table")).await;

    let reply = get(proxy.http, "app.example.com", "/").await;
    assert_eq!(reply.status, 200);

    let copy = next_seen(&mut seen).await;
    assert_eq!(
        copy.host.as_deref(),
        Some("shadow.example.com"),
        "the override survives the crossing to h2c"
    );
}
