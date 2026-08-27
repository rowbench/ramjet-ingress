//! The admin listener: `/metrics`, `/healthz`, `/readyz`.

mod common;

use common::*;
use http::StatusCode;
use ramjet_router::{Endpoint, LbPolicy, PathType, RouteTableBuilder};

/// Pulls a single-value series out of the exposition text.
fn series(text: &str, name: &str) -> Option<f64> {
    text.lines()
        .filter(|line| !line.starts_with('#'))
        .find_map(|line| line.strip_prefix(name)?.trim().parse().ok())
}

#[tokio::test]
async fn healthz_is_unconditional_and_readyz_is_gated() {
    let app = spawn_echo("app").await;
    let proxy = TestProxy::start(single_route("app.example.com", "/", &[app])).await;

    let health = get(proxy.admin, "localhost", "/healthz").await;
    assert_eq!(health.status, StatusCode::OK);
    assert_eq!(health.text(), "ok\n");

    // A replica with no route table yet must stay out of the Service, or a
    // rolling update briefly sends traffic to a pod that can only 404.
    let not_ready = get(proxy.admin, "localhost", "/readyz").await;
    assert_eq!(not_ready.status, StatusCode::SERVICE_UNAVAILABLE);

    proxy.readiness.set_ready(true);
    let ready = get(proxy.admin, "localhost", "/readyz").await;
    assert_eq!(ready.status, StatusCode::OK);
    assert_eq!(ready.text(), "ready\n");
}

#[tokio::test]
async fn an_unknown_admin_path_is_404() {
    let app = spawn_echo("app").await;
    let proxy = TestProxy::start(single_route("app.example.com", "/", &[app])).await;
    assert_eq!(
        get(proxy.admin, "localhost", "/").await.status,
        StatusCode::NOT_FOUND
    );
}

#[tokio::test]
async fn metrics_scrape_in_prometheus_format_and_counters_move() {
    let app = spawn_echo("app").await;
    let proxy = TestProxy::start(single_route("app.example.com", "/", &[app])).await;

    let before = get(proxy.admin, "localhost", "/metrics").await;
    assert_eq!(before.status, StatusCode::OK);
    assert_eq!(
        before.header("content-type"),
        Some("text/plain; version=0.0.4; charset=utf-8")
    );
    let before = before.text().to_owned();
    assert!(before.contains("# TYPE ramjet_requests_total counter"));
    assert!(before.contains("# TYPE ramjet_upstream_latency_seconds histogram"));
    assert!(before.contains("# TYPE ramjet_active_connections gauge"));

    let ok_before = series(&before, "ramjet_requests_total{code=\"2xx\"}").unwrap_or(0.0);
    let observed_before = series(&before, "ramjet_upstream_latency_seconds_count").unwrap_or(0.0);

    for _ in 0..3 {
        assert_eq!(
            get(proxy.http, "app.example.com", "/").await.status,
            StatusCode::OK
        );
    }
    assert_eq!(
        get(proxy.http, "nowhere.example.com", "/").await.status,
        StatusCode::NOT_FOUND
    );

    let after = get(proxy.admin, "localhost", "/metrics").await.text().to_owned();
    assert_eq!(
        series(&after, "ramjet_requests_total{code=\"2xx\"}"),
        Some(ok_before + 3.0)
    );
    assert_eq!(series(&after, "ramjet_requests_total{code=\"4xx\"}"), Some(1.0));
    assert_eq!(series(&after, "ramjet_route_misses_total"), Some(1.0));
    assert_eq!(
        series(&after, "ramjet_upstream_latency_seconds_count"),
        Some(observed_before + 3.0),
        "a 404 never reaches an upstream, so it must not be timed as one"
    );
}

#[tokio::test]
async fn the_generation_gauge_follows_the_published_table() {
    let app = spawn_echo("app").await;

    let mut builder = RouteTableBuilder::new();
    builder
        .backend("app", LbPolicy::RoundRobin, vec![Endpoint::new(app)])
        .expect("backend");
    builder
        .route(Some("app.example.com"), "/", PathType::Prefix, "app")
        .expect("route");
    builder.generation(7);
    let proxy = TestProxy::start(builder.build().expect("table")).await;

    let text = get(proxy.admin, "localhost", "/metrics").await.text().to_owned();
    assert_eq!(series(&text, "ramjet_route_table_generation"), Some(7.0));

    let mut builder = RouteTableBuilder::new();
    builder
        .backend("app", LbPolicy::RoundRobin, vec![Endpoint::new(app)])
        .expect("backend");
    builder
        .route(Some("app.example.com"), "/", PathType::Prefix, "app")
        .expect("route");
    builder.generation(8);
    proxy.routes.store(builder.build().expect("table"));

    let text = get(proxy.admin, "localhost", "/metrics").await.text().to_owned();
    assert_eq!(
        series(&text, "ramjet_route_table_generation"),
        Some(8.0),
        "the gauge is read from the table at scrape time, not mirrored on publish"
    );
}

#[tokio::test]
async fn upstream_failures_are_counted_separately_from_responses() {
    let dead = dead_addr().await;
    let proxy = TestProxy::start(single_route("app.example.com", "/", &[dead])).await;

    assert_eq!(
        get(proxy.http, "app.example.com", "/").await.status,
        StatusCode::BAD_GATEWAY
    );

    let text = get(proxy.admin, "localhost", "/metrics").await.text().to_owned();
    assert_eq!(series(&text, "ramjet_requests_total{code=\"5xx\"}"), Some(1.0));
    assert!(
        series(&text, "ramjet_upstream_connect_failures_total").unwrap_or(0.0) >= 1.0,
        "{text}"
    );
}

#[tokio::test]
async fn active_connections_returns_to_zero_after_a_request() {
    let app = spawn_echo("app").await;
    let proxy = TestProxy::start(single_route("app.example.com", "/", &[app])).await;

    assert_eq!(
        get(proxy.http, "app.example.com", "/").await.status,
        StatusCode::OK
    );

    // The client connection is closed by `get`, but the server task learns
    // about that asynchronously, so allow it a moment to notice.
    for _ in 0..50 {
        if proxy.metrics.active_connections() == 0 {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    panic!(
        "active connections stuck at {}",
        proxy.metrics.active_connections()
    );
}
