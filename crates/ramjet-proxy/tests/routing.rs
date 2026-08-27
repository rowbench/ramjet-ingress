//! Host and path dispatch, end to end over real sockets.
//!
//! `ramjet-router` already proves the matching *rules* against string literals.
//! What these tests prove is the wiring: that the value the proxy pulls out of
//! a real request is the value the matcher expects, that the chosen backend is
//! the one the bytes actually reach, and that the request arrives upstream
//! looking like the one the client sent.

mod common;

use common::*;
use ramjet_router::{Endpoint, LbPolicy, PathType, RouteTableBuilder};

#[tokio::test]
async fn dispatches_by_host() {
    let alpha = spawn_echo("alpha").await;
    let beta = spawn_echo("beta").await;

    let mut builder = RouteTableBuilder::new();
    builder
        .backend("alpha", LbPolicy::RoundRobin, vec![Endpoint::new(alpha)])
        .expect("backend");
    builder
        .backend("beta", LbPolicy::RoundRobin, vec![Endpoint::new(beta)])
        .expect("backend");
    builder
        .route(Some("alpha.example.com"), "/", PathType::Prefix, "alpha")
        .expect("route");
    builder
        .route(Some("beta.example.com"), "/", PathType::Prefix, "beta")
        .expect("route");
    let proxy = TestProxy::start(builder.build().expect("table")).await;

    assert_eq!(
        get(proxy.http, "alpha.example.com", "/").await.upstream(),
        "alpha"
    );
    assert_eq!(
        get(proxy.http, "beta.example.com", "/").await.upstream(),
        "beta"
    );
    // A port on the Host header must not change where the request lands.
    assert_eq!(
        get(proxy.http, "ALPHA.example.com:8080", "/")
            .await
            .upstream(),
        "alpha"
    );
}

#[tokio::test]
async fn dispatches_by_path_with_exact_beating_prefix() {
    let api = spawn_echo("api").await;
    let web = spawn_echo("web").await;
    let health = spawn_echo("health").await;

    let mut builder = RouteTableBuilder::new();
    for (name, addr) in [("api", api), ("web", web), ("health", health)] {
        builder
            .backend(name, LbPolicy::RoundRobin, vec![Endpoint::new(addr)])
            .expect("backend");
    }
    builder
        .route(Some("shop.example.com"), "/", PathType::Prefix, "web")
        .expect("route");
    builder
        .route(Some("shop.example.com"), "/api", PathType::Prefix, "api")
        .expect("route");
    builder
        .route(
            Some("shop.example.com"),
            "/api/health",
            PathType::Exact,
            "health",
        )
        .expect("route");
    let proxy = TestProxy::start(builder.build().expect("table")).await;

    assert_eq!(get(proxy.http, "shop.example.com", "/").await.upstream(), "web");
    assert_eq!(
        get(proxy.http, "shop.example.com", "/api/v1").await.upstream(),
        "api"
    );
    assert_eq!(
        get(proxy.http, "shop.example.com", "/api/health")
            .await
            .upstream(),
        "health"
    );
    // The trap the router exists for: `/apiary` is not a path-element prefix
    // of `/api`, so it belongs to the root rule.
    assert_eq!(
        get(proxy.http, "shop.example.com", "/apiary").await.upstream(),
        "web"
    );
}

#[tokio::test]
async fn a_wildcard_host_serves_one_label() {
    let wild = spawn_echo("wild").await;
    let mut builder = RouteTableBuilder::new();
    builder
        .backend("wild", LbPolicy::RoundRobin, vec![Endpoint::new(wild)])
        .expect("backend");
    builder
        .route(Some("*.example.com"), "/", PathType::Prefix, "wild")
        .expect("route");
    let proxy = TestProxy::start(builder.build().expect("table")).await;

    assert_eq!(get(proxy.http, "shop.example.com", "/").await.upstream(), "wild");
    assert_eq!(
        get(proxy.http, "a.b.example.com", "/").await.status,
        http::StatusCode::NOT_FOUND,
        "a wildcard replaces exactly one label"
    );
}

#[tokio::test]
async fn an_unmatched_request_is_404() {
    let app = spawn_echo("app").await;
    let proxy = TestProxy::start(single_route("known.example.com", "/", &[app])).await;

    let reply = get(proxy.http, "unknown.example.com", "/").await;
    assert_eq!(reply.status, http::StatusCode::NOT_FOUND);
    assert!(reply.text().contains("no ingress rule"), "{}", reply.text());
}

#[tokio::test]
async fn a_default_backend_answers_instead_of_404() {
    let app = spawn_echo("app").await;
    let fallback = spawn_echo("fallback").await;

    let mut builder = RouteTableBuilder::new();
    builder
        .backend("app", LbPolicy::RoundRobin, vec![Endpoint::new(app)])
        .expect("backend");
    builder
        .backend(
            "fallback",
            LbPolicy::RoundRobin,
            vec![Endpoint::new(fallback)],
        )
        .expect("backend");
    builder
        .route(Some("known.example.com"), "/", PathType::Prefix, "app")
        .expect("route");
    builder.default_backend("fallback");
    let proxy = TestProxy::start(builder.build().expect("table")).await;

    assert_eq!(
        get(proxy.http, "nobody.example.com", "/").await.upstream(),
        "fallback"
    );
}

#[tokio::test]
async fn the_path_and_query_reach_the_upstream_untouched() {
    let app = spawn_echo("app").await;
    let proxy = TestProxy::start(single_route("app.example.com", "/", &[app])).await;

    let reply = get(proxy.http, "app.example.com", "/a/b%20c?x=1&y=%2F").await;
    assert_eq!(reply.status, http::StatusCode::OK);
    assert_eq!(
        reply.text(),
        "GET /a/b%20c?x=1&y=%2F",
        "the proxy must not normalise or re-encode the target"
    );
}

#[tokio::test]
async fn the_client_host_is_preserved_upstream() {
    // ingress-nginx forwards the client's Host by default and a great many
    // applications route, generate links, or pick a tenant from it. Rewriting
    // it to the endpoint's `ip:port` would break all of them silently.
    let app = spawn_echo("app").await;
    let proxy = TestProxy::start(single_route("tenant.example.com", "/", &[app])).await;

    let reply = get(proxy.http, "tenant.example.com", "/").await;
    assert_eq!(reply.header("echo-host"), Some("tenant.example.com"));
}

#[tokio::test]
async fn round_robin_spreads_across_endpoints() {
    // Two endpoints answer with different bodies, so the split is visible.
    let one = spawn_http(|_| async { http::Response::new(full("one")) }).await;
    let two = spawn_http(|_| async { http::Response::new(full("two")) }).await;
    let proxy = TestProxy::start(single_route("lb.example.com", "/", &[one, two])).await;

    let replies = send_many(proxy.http, "lb.example.com", "/", 8).await;
    let ones = replies.iter().filter(|r| r.text() == "one").count();
    let twos = replies.iter().filter(|r| r.text() == "two").count();
    assert_eq!((ones, twos), (4, 4), "round robin must alternate");
}

#[tokio::test]
async fn a_published_table_takes_effect_immediately() {
    // The whole thesis: publishing is a pointer store, and the very next
    // request sees it. No reload, no drain, no window where both are live.
    let before = spawn_echo("before").await;
    let after = spawn_echo("after").await;
    let proxy = TestProxy::start(single_route("app.example.com", "/", &[before])).await;

    assert_eq!(get(proxy.http, "app.example.com", "/").await.upstream(), "before");

    proxy
        .routes
        .store(single_route("app.example.com", "/", &[after]));

    assert_eq!(get(proxy.http, "app.example.com", "/").await.upstream(), "after");
}
