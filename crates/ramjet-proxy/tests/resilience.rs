//! Failover and the status codes each failure maps to.
//!
//! Every "dead" endpoint here is a real address with nothing listening, so the
//! failure the proxy sees is a real `ECONNREFUSED` rather than an injected one.

mod common;

use std::time::Duration;

use common::*;
use http::StatusCode;
use ramjet_router::{LbPolicy, PathType, RouteTableBuilder};

fn fast_upstream() -> ramjet_proxy::UpstreamConfig {
    ramjet_proxy::UpstreamConfig {
        connect_timeout: Duration::from_millis(200),
        response_timeout: Duration::from_millis(300),
        ..Default::default()
    }
}

#[tokio::test]
async fn a_dead_first_endpoint_fails_over_to_the_next() {
    let dead = dead_addr().await;
    let alive = spawn_echo("alive").await;

    // Round robin starts at endpoint 0, so the first request is guaranteed to
    // hit the dead one and have to fail over.
    let proxy = TestProxy::start(single_route("app.example.com", "/", &[dead, alive])).await;

    let reply = get(proxy.http, "app.example.com", "/").await;
    assert_eq!(reply.status, StatusCode::OK);
    assert_eq!(reply.upstream(), "alive");
    assert!(proxy.metrics.retries() >= 1, "the retry was not counted");
}

#[tokio::test]
async fn every_endpoint_dead_is_502() {
    let first = dead_addr().await;
    let second = dead_addr().await;
    let proxy = TestProxy::start(single_route("app.example.com", "/", &[first, second])).await;

    let reply = get(proxy.http, "app.example.com", "/").await;
    assert_eq!(reply.status, StatusCode::BAD_GATEWAY);
    assert!(reply.text().contains("connect"), "{}", reply.text());
}

#[tokio::test]
async fn a_backend_with_no_endpoints_is_503() {
    // A Service whose pods are all unready is a normal state during a rollout.
    // The table still builds; the request is what fails, and 503 is the code
    // that tells a client to try again.
    let mut builder = RouteTableBuilder::new();
    builder
        .backend("app", LbPolicy::RoundRobin, vec![])
        .expect("backend");
    builder
        .route(Some("app.example.com"), "/", PathType::Prefix, "app")
        .expect("route");
    let proxy = TestProxy::start(builder.build().expect("table")).await;

    let reply = get(proxy.http, "app.example.com", "/").await;
    assert_eq!(reply.status, StatusCode::SERVICE_UNAVAILABLE);
    assert!(reply.text().contains("no ready endpoints"), "{}", reply.text());
}

#[tokio::test]
async fn an_upstream_that_never_answers_is_504() {
    let wedged = spawn_black_hole().await;
    let proxy = TestProxy::start_with(
        single_route("app.example.com", "/", &[wedged]),
        ProxyOptions {
            upstream: fast_upstream(),
            ..Default::default()
        },
    )
    .await;

    let reply = get(proxy.http, "app.example.com", "/").await;
    assert_eq!(reply.status, StatusCode::GATEWAY_TIMEOUT);
    assert!(reply.text().contains("no response headers"), "{}", reply.text());
}

#[tokio::test]
async fn a_slow_upstream_inside_the_deadline_still_succeeds() {
    let slow = spawn_slow(Duration::from_millis(120)).await;
    let proxy = TestProxy::start_with(
        single_route("app.example.com", "/", &[slow]),
        ProxyOptions {
            upstream: ramjet_proxy::UpstreamConfig {
                response_timeout: Duration::from_secs(5),
                ..Default::default()
            },
            ..Default::default()
        },
    )
    .await;

    let reply = get(proxy.http, "app.example.com", "/").await;
    assert_eq!(reply.status, StatusCode::OK, "slow is not the same as broken");
    assert_eq!(reply.text(), "slow");
}

#[tokio::test]
async fn a_timeout_is_not_retried_against_another_endpoint() {
    // A timeout means the request may already have had effects upstream.
    // Replaying it would be a second `POST`, which is worse than a 504.
    let wedged = spawn_black_hole().await;
    let alive = spawn_echo("alive").await;
    let proxy = TestProxy::start_with(
        single_route("app.example.com", "/", &[wedged, alive]),
        ProxyOptions {
            upstream: fast_upstream(),
            ..Default::default()
        },
    )
    .await;

    let reply = get(proxy.http, "app.example.com", "/").await;
    assert_eq!(reply.status, StatusCode::GATEWAY_TIMEOUT);
}

#[tokio::test]
async fn a_request_with_a_body_is_not_failed_over() {
    // Deliberate: re-dispatching a streaming body means having buffered it, and
    // buffering upstream request bodies is exactly the behaviour that makes an
    // ingress pod's memory a function of what its slowest client is uploading.
    // If this test starts failing because somebody added buffering, that is a
    // decision to make on purpose, not a bug to fix quietly.
    let dead = dead_addr().await;
    let alive = spawn_echo("alive").await;
    let proxy = TestProxy::start(single_route("app.example.com", "/", &[dead, alive])).await;

    let reply = send(
        proxy.http,
        request("app.example.com", "/")
            .method("POST")
            .body(full("a payload that cannot be replayed"))
            .expect("a request"),
    )
    .await;

    assert_eq!(reply.status, StatusCode::BAD_GATEWAY);
}

#[tokio::test]
async fn an_empty_bodied_post_does_fail_over() {
    // The limit is the body, not the method: nothing was written, so nothing
    // can have happened twice.
    let dead = dead_addr().await;
    let alive = spawn_echo("alive").await;
    let proxy = TestProxy::start(single_route("app.example.com", "/", &[dead, alive])).await;

    let reply = send(
        proxy.http,
        request("app.example.com", "/")
            .method("POST")
            .body(empty_body())
            .expect("a request"),
    )
    .await;

    assert_eq!(reply.status, StatusCode::OK);
    assert_eq!(reply.upstream(), "alive");
}

#[tokio::test]
async fn failover_stops_at_the_configured_attempt_limit() {
    // Four dead endpoints, a limit of three: the request must not keep walking
    // the endpoint list until it has burned four connect timeouts.
    let dead: Vec<_> = vec![
        dead_addr().await,
        dead_addr().await,
        dead_addr().await,
        dead_addr().await,
    ];
    let proxy = TestProxy::start_with(
        single_route("app.example.com", "/", &dead),
        ProxyOptions {
            upstream: ramjet_proxy::UpstreamConfig {
                max_connect_attempts: 3,
                ..fast_upstream()
            },
            ..Default::default()
        },
    )
    .await;

    let reply = get(proxy.http, "app.example.com", "/").await;
    assert_eq!(reply.status, StatusCode::BAD_GATEWAY);
    assert_eq!(
        proxy.metrics.retries(),
        2,
        "three attempts means two re-dispatches"
    );
}
