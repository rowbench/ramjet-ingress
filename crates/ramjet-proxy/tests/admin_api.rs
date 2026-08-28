//! The generation API on a real admin socket.
//!
//! The unit tests in `history` prove the state machine and the ones in `admin`
//! prove the JSON shape. What is left — and what these cover — is that the two
//! are actually wired to the thing serving traffic: that `POST /admin/rollback`
//! changes where a request lands, that the counters `/admin/routes` reports are
//! the ones the request path increments, and that the endpoints are reachable
//! over HTTP with the methods the contract names.

mod common;

use std::net::SocketAddr;
use std::sync::Arc;

use common::{
    empty_body, full, get, request, send, spawn_echo, ProxyOptions, TestProxy,
};
use http::{Method, StatusCode};
use ramjet_proxy::CertKeys;
use ramjet_router::{Endpoint, LbPolicy, PathType, RouteTable, RouteTableBuilder};
use serde_json::Value;

/// A table at `generation` sending `example.com/` to `upstream`.
fn table(generation: u64, upstream: SocketAddr) -> Arc<RouteTable> {
    let mut builder = RouteTableBuilder::new();
    builder.generation(generation);
    builder
        .backend("prod/api:80", LbPolicy::RoundRobin, vec![Endpoint::new(upstream)])
        .expect("registers");
    builder
        .route(Some("example.com"), "/", PathType::Prefix, "prod/api:80")
        .expect("drafts");
    Arc::new(builder.build().expect("builds"))
}

/// A proxy that starts with nothing published, so every generation the tests
/// serve arrives through the history the same way the daemon's applier sends
/// one.
async fn proxy() -> TestProxy {
    TestProxy::start(RouteTableBuilder::new().build().expect("an empty table")).await
}

async fn proxy_with(options: ProxyOptions) -> TestProxy {
    TestProxy::start_with(
        RouteTableBuilder::new().build().expect("an empty table"),
        options,
    )
    .await
}

/// Records `table` through the history, the way the daemon's applier does.
fn record(proxy: &TestProxy, table: Arc<RouteTable>, digest: u64) -> bool {
    proxy.history.record(
        table.generation(),
        digest,
        Arc::new(serde_json::json!({ "summary": "a change" })),
        table,
        Arc::new(CertKeys::new()),
    )
}

async fn admin_json(addr: SocketAddr, path: &str) -> (StatusCode, Value) {
    let reply = get(addr, "admin", path).await;
    let body = serde_json::from_slice(&reply.body)
        .unwrap_or_else(|e| panic!("{path} did not return JSON: {e}: {:?}", reply.text()));
    (reply.status, body)
}

async fn rollback(addr: SocketAddr, generation: u64) -> (StatusCode, Value) {
    let body = format!("{{\"generation\":{generation}}}");
    let reply = send(
        addr,
        request("admin", "/admin/rollback")
            .method(Method::POST)
            .body(full(body))
            .expect("a request"),
    )
    .await;
    let value = serde_json::from_slice(&reply.body).unwrap_or(Value::Null);
    (reply.status, value)
}

async fn resume(addr: SocketAddr) -> (StatusCode, Value) {
    let reply = send(
        addr,
        request("admin", "/admin/rollback")
            .method(Method::DELETE)
            .body(empty_body())
            .expect("a request"),
    )
    .await;
    let value = serde_json::from_slice(&reply.body).unwrap_or(Value::Null);
    (reply.status, value)
}

/// The headline: a rollback moves live traffic back to where it used to go.
#[tokio::test]
async fn a_rollback_changes_where_requests_land() {
    let old = spawn_echo("old").await;
    let new = spawn_echo("new").await;

    let proxy = proxy().await;
    assert!(record(&proxy, table(1, old), 0xaaa));
    assert_eq!(get(proxy.http, "example.com", "/").await.upstream(), "old");

    // A deploy that moves the route somewhere else.
    assert!(record(&proxy, table(2, new), 0xbbb));
    assert_eq!(
        get(proxy.http, "example.com", "/").await.upstream(),
        "new",
        "a published generation must take effect without a restart"
    );

    // The emergency brake.
    let (status, body) = rollback(proxy.admin, 1).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["pinned"], 1);
    assert_eq!(
        get(proxy.http, "example.com", "/").await.upstream(),
        "old",
        "the pinned generation must actually be serving"
    );

    // Work done while pinned is recorded and held back.
    assert!(!record(&proxy, table(3, new), 0xccc));
    assert_eq!(get(proxy.http, "example.com", "/").await.upstream(), "old");

    // Releasing jumps to the newest, not to the one that was pinned over.
    let (status, body) = resume(proxy.admin).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["pinned"], Value::Null);
    assert_eq!(get(proxy.http, "example.com", "/").await.upstream(), "new");
    assert_eq!(proxy.routes.generation(), 3);

    proxy.shutdown().await.expect("drains");
}

#[tokio::test]
async fn generations_lists_what_was_applied_newest_first() {
    let upstream = spawn_echo("api").await;
    let proxy = proxy().await;
    record(&proxy, table(1, upstream), 0x1111_2222_3333_4444);
    record(&proxy, table(2, upstream), 0x5555);

    let (status, body) = admin_json(proxy.admin, "/admin/generations").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["pinned"], Value::Null);
    assert_eq!(body["serving"], 2);

    let listed = body["generations"].as_array().expect("an array");
    assert_eq!(listed.len(), 2);
    assert_eq!(listed[0]["generation"], 2);
    assert_eq!(listed[1]["generation"], 1);
    assert_eq!(listed[1]["digest"], "1111222233334444");
    assert_eq!(listed[0]["published"], true);
    assert_eq!(listed[0]["routes"], 1);
    assert_eq!(listed[0]["hosts"], 1);
    assert_eq!(listed[0]["certs"], 0);
    assert_eq!(listed[0]["diff"]["summary"], "a change");
    assert!(
        listed[0]["applied_at"]
            .as_str()
            .is_some_and(|t| t.len() == 20 && t.ends_with('Z')),
        "applied_at should be RFC 3339 UTC: {:?}",
        listed[0]["applied_at"]
    );

    // And the pin shows up where a reader would look for it.
    rollback(proxy.admin, 1).await;
    let (_, body) = admin_json(proxy.admin, "/admin/generations").await;
    assert_eq!(body["pinned"], 1);
    assert_eq!(body["serving"], 1);
    assert_eq!(
        body["generations"][0]["generation"], 2,
        "a pin does not reorder the history"
    );

    proxy.shutdown().await.expect("drains");
}

#[tokio::test]
async fn route_counters_move_as_requests_are_served() {
    let upstream = spawn_echo("api").await;
    let proxy = proxy().await;
    record(&proxy, table(1, upstream), 0);

    let (_, before) = admin_json(proxy.admin, "/admin/routes").await;
    assert_eq!(before["generation"], 1);
    assert_eq!(before["routes"][0]["host"], "example.com");
    assert_eq!(before["routes"][0]["path"], "/");
    assert_eq!(before["routes"][0]["path_type"], "Prefix");
    assert_eq!(before["routes"][0]["backend"], "prod/api:80");
    assert_eq!(before["routes"][0]["endpoints"], 1);
    assert_eq!(before["routes"][0]["canary"], Value::Null);
    assert_eq!(before["routes"][0]["requests_total"], 0);

    for _ in 0..3 {
        assert_eq!(get(proxy.http, "example.com", "/").await.status, StatusCode::OK);
    }

    let (_, after) = admin_json(proxy.admin, "/admin/routes").await;
    let route = &after["routes"][0];
    assert_eq!(route["requests_total"], 3, "three requests, three counted");
    assert_eq!(route["errors_5xx_total"], 0);
    assert_eq!(
        route["upstream_latency_count"], 3,
        "every forwarded request observes an upstream latency"
    );
    assert!(
        route["upstream_latency_ms_sum"].as_f64().is_some_and(|ms| ms > 0.0),
        "a real upstream hop takes measurable time: {route}"
    );

    // A request nothing matches is not attributed to a route.
    assert_eq!(
        get(proxy.http, "elsewhere.test", "/").await.status,
        StatusCode::NOT_FOUND
    );
    let (_, after) = admin_json(proxy.admin, "/admin/routes").await;
    assert_eq!(
        after["routes"][0]["requests_total"], 3,
        "a route miss belongs to no route"
    );

    proxy.shutdown().await.expect("drains");
}

/// A 5xx is the route's problem whether the upstream produced it or the proxy
/// did, which is the case an operator is looking at `/admin/routes` for.
#[tokio::test]
async fn a_generated_5xx_is_counted_against_its_route() {
    let mut builder = RouteTableBuilder::new();
    builder.generation(1);
    builder
        .backend("prod/gone:80", LbPolicy::RoundRobin, Vec::new())
        .expect("registers");
    builder
        .route(Some("example.com"), "/", PathType::Prefix, "prod/gone:80")
        .expect("drafts");
    let table = builder.build().expect("builds");

    let proxy = TestProxy::start(table).await;
    assert_eq!(
        get(proxy.http, "example.com", "/").await.status,
        StatusCode::SERVICE_UNAVAILABLE
    );

    let (_, body) = admin_json(proxy.admin, "/admin/routes").await;
    let route = &body["routes"][0];
    assert_eq!(route["requests_total"], 1);
    assert_eq!(
        route["errors_5xx_total"], 1,
        "the backend has no endpoints, and that is the route's 503"
    );
    assert_eq!(
        route["upstream_latency_count"], 0,
        "nothing was dispatched, so nothing was timed"
    );

    proxy.shutdown().await.expect("drains");
}

#[tokio::test]
async fn rollback_reports_the_failures_the_contract_names() {
    let upstream = spawn_echo("api").await;
    let proxy = proxy_with(ProxyOptions {
        history_size: 2,
        ..ProxyOptions::default()
    })
    .await;
    for generation in 1..=3 {
        record(&proxy, table(generation, upstream), 0);
    }

    // Evicted from a ring of two.
    let (status, body) = rollback(proxy.admin, 1).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["generation"], 1);

    let (status, _) = rollback(proxy.admin, 99).await;
    assert_eq!(status, StatusCode::NOT_FOUND, "never existed");

    let (status, _) = rollback(proxy.admin, 2).await;
    assert_eq!(status, StatusCode::OK);

    let (status, body) = rollback(proxy.admin, 3).await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(
        body["pinned"], 2,
        "a conflict must say what is already pinned, or the caller cannot act on it"
    );

    // Releasing is idempotent: the state asked for is the state you get.
    assert_eq!(resume(proxy.admin).await.0, StatusCode::OK);
    assert_eq!(resume(proxy.admin).await.0, StatusCode::OK);
    assert_eq!(proxy.history.pinned(), None);

    proxy.shutdown().await.expect("drains");
}

/// Nothing reachable with a `GET` may change what this replica serves.
#[tokio::test]
async fn the_mutating_endpoint_answers_only_post_and_delete() {
    let upstream = spawn_echo("api").await;
    let proxy = proxy().await;
    record(&proxy, table(1, upstream), 0);

    let reply = get(proxy.admin, "admin", "/admin/rollback").await;
    assert_eq!(reply.status, StatusCode::METHOD_NOT_ALLOWED);
    assert!(
        reply.text().contains("POST") && reply.text().contains("DELETE"),
        "the refusal should say what does work: {}",
        reply.text()
    );
    assert_eq!(proxy.history.pinned(), None);

    // A body that is not what the contract asks for is a 400, not a pin.
    let reply = send(
        proxy.admin,
        request("admin", "/admin/rollback")
            .method(Method::POST)
            .body(full("{\"generation\":\"one\"}"))
            .expect("a request"),
    )
    .await;
    assert_eq!(reply.status, StatusCode::BAD_REQUEST);
    assert_eq!(proxy.history.pinned(), None);

    proxy.shutdown().await.expect("drains");
}

#[tokio::test]
async fn metrics_reports_whether_publication_is_held() {
    let upstream = spawn_echo("api").await;
    let proxy = proxy().await;
    record(&proxy, table(1, upstream), 0);
    record(&proxy, table(2, upstream), 0);

    let scrape = get(proxy.admin, "admin", "/metrics").await;
    assert!(scrape.text().contains("ramjet_pinned 0"), "{}", scrape.text());
    assert!(scrape.text().contains("ramjet_route_table_generation 2"));

    rollback(proxy.admin, 1).await;
    let scrape = get(proxy.admin, "admin", "/metrics").await;
    assert!(scrape.text().contains("ramjet_pinned 1"), "{}", scrape.text());
    assert!(
        scrape.text().contains("ramjet_route_table_generation 1"),
        "the generation gauge follows what is serving, not what was built"
    );

    // Per-route data must never become labelled series, whatever else changes.
    assert!(
        !scrape.text().contains("example.com"),
        "a host name in /metrics is unbounded cardinality: {}",
        scrape.text()
    );

    proxy.shutdown().await.expect("drains");
}
