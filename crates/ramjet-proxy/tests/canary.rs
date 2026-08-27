//! Canary routing, end to end.
//!
//! The router already tests the decision function exhaustively against literal
//! values. What is left to prove here is that the proxy hands it the right
//! values — the header the annotation names, the cookie the annotation names,
//! and a roll drawn from the right range — because a canary that quietly never
//! fires, or always fires, looks identical to a working one until somebody
//! checks the split.

mod common;

use common::*;
use ramjet_router::{CanaryRules, Endpoint, LbPolicy, PathType, RouteTable, RouteTableBuilder};

fn canary_table(
    production: std::net::SocketAddr,
    canary: std::net::SocketAddr,
    rules: CanaryRules<'_>,
) -> RouteTable {
    let mut builder = RouteTableBuilder::new();
    builder
        .backend("prod", LbPolicy::RoundRobin, vec![Endpoint::new(production)])
        .expect("backend");
    builder
        .backend("canary", LbPolicy::RoundRobin, vec![Endpoint::new(canary)])
        .expect("backend");
    builder
        .canary_route(
            Some("app.example.com"),
            "/",
            PathType::Prefix,
            "prod",
            &rules,
        )
        .expect("route");
    builder.build().expect("table")
}

async fn pair() -> (std::net::SocketAddr, std::net::SocketAddr) {
    (spawn_echo("prod").await, spawn_echo("canary").await)
}

#[tokio::test]
async fn a_weight_splits_traffic_within_statistical_bounds() {
    const REQUESTS: usize = 600;

    let (production, canary) = pair().await;
    let proxy = TestProxy::start(canary_table(
        production,
        canary,
        CanaryRules {
            backend: "canary",
            weight: 25,
            ..Default::default()
        },
    ))
    .await;

    let replies = send_many(proxy.http, "app.example.com", "/", REQUESTS).await;
    let diverted = replies.iter().filter(|r| r.upstream() == "canary").count();

    // Expected 150 of 600, standard deviation about 10.6. These bounds are more
    // than four deviations wide on each side, so the test is about "the weight
    // is being applied at all", not about the quality of the generator.
    assert!(
        (100..=200).contains(&diverted),
        "{diverted} of {REQUESTS} went to the canary, which is not a 25% split"
    );
}

#[tokio::test]
async fn a_zero_weight_never_diverts_and_a_full_weight_always_does() {
    let (production, canary) = pair().await;

    let proxy = TestProxy::start(canary_table(
        production,
        canary,
        CanaryRules {
            backend: "canary",
            weight: 0,
            ..Default::default()
        },
    ))
    .await;
    let replies = send_many(proxy.http, "app.example.com", "/", 50).await;
    assert!(replies.iter().all(|r| r.upstream() == "prod"));
    drop(proxy);

    let proxy = TestProxy::start(canary_table(
        production,
        canary,
        CanaryRules {
            backend: "canary",
            weight: 100,
            ..Default::default()
        },
    ))
    .await;
    let replies = send_many(proxy.http, "app.example.com", "/", 50).await;
    assert!(replies.iter().all(|r| r.upstream() == "canary"));
}

#[tokio::test]
async fn a_matching_header_beats_the_weight() {
    let (production, canary) = pair().await;
    let proxy = TestProxy::start(canary_table(
        production,
        canary,
        CanaryRules {
            backend: "canary",
            header: Some("x-canary"),
            // Zero weight: nothing is diverted unless the header says so.
            weight: 0,
            ..Default::default()
        },
    ))
    .await;

    let diverted = send(
        proxy.http,
        request("app.example.com", "/")
            .header("x-canary", "always")
            .body(empty_body())
            .expect("a request"),
    )
    .await;
    assert_eq!(diverted.upstream(), "canary");

    // A header that is present but says something unrelated is *not* decisive:
    // it falls through to the weight, which here is zero.
    let unrelated = send(
        proxy.http,
        request("app.example.com", "/")
            .header("x-canary", "maybe")
            .body(empty_body())
            .expect("a request"),
    )
    .await;
    assert_eq!(unrelated.upstream(), "prod");
}

#[tokio::test]
async fn never_beats_a_full_weight() {
    let (production, canary) = pair().await;
    let proxy = TestProxy::start(canary_table(
        production,
        canary,
        CanaryRules {
            backend: "canary",
            header: Some("x-canary"),
            weight: 100,
            ..Default::default()
        },
    ))
    .await;

    let reply = send(
        proxy.http,
        request("app.example.com", "/")
            .header("x-canary", "never")
            .body(empty_body())
            .expect("a request"),
    )
    .await;
    assert_eq!(
        reply.upstream(),
        "prod",
        "`never` must win against a 100% weight"
    );
}

#[tokio::test]
async fn a_header_value_match_diverts() {
    let (production, canary) = pair().await;
    let proxy = TestProxy::start(canary_table(
        production,
        canary,
        CanaryRules {
            backend: "canary",
            header: Some("x-track"),
            header_value: Some("beta"),
            weight: 0,
            ..Default::default()
        },
    ))
    .await;

    let matched = send(
        proxy.http,
        request("app.example.com", "/")
            .header("x-track", "beta")
            .body(empty_body())
            .expect("a request"),
    )
    .await;
    assert_eq!(matched.upstream(), "canary");

    assert_eq!(
        get(proxy.http, "app.example.com", "/").await.upstream(),
        "prod",
        "no header at all falls through to the weight"
    );
}

#[tokio::test]
async fn a_cookie_diverts_when_no_header_rule_decides() {
    let (production, canary) = pair().await;
    let proxy = TestProxy::start(canary_table(
        production,
        canary,
        CanaryRules {
            backend: "canary",
            cookie: Some("canary"),
            weight: 0,
            ..Default::default()
        },
    ))
    .await;

    let reply = send(
        proxy.http,
        request("app.example.com", "/")
            .header("cookie", "session=abc; canary=always; theme=dark")
            .body(empty_body())
            .expect("a request"),
    )
    .await;
    assert_eq!(
        reply.upstream(),
        "canary",
        "the cookie must be found among the others in the header"
    );
}
