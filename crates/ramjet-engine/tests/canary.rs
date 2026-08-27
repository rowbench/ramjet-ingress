//! Canary routing, end to end.
//!
//! The arithmetic belongs to `ramjet_router` and is tested there. What these
//! check is that this engine feeds it the right inputs — a header value read
//! from the request, a cookie parsed out of a `Cookie` line — and honours the
//! answer.

mod common;

use common::{canary_table, echo, get, get_with, Proxy};
use ramjet_router::CanaryRules;

#[test]
fn a_matching_header_diverts_and_a_non_matching_one_does_not() {
    let production = echo();
    let canary = echo();
    let proxy = Proxy::start(canary_table(
        "app.example.com",
        &[production.addr],
        &[canary.addr],
        CanaryRules {
            backend: "canary",
            header: Some("x-canary"),
            weight: 0,
            ..Default::default()
        },
    ));

    get_with(proxy.addr, "/", "app.example.com", &[("X-Canary", "always")]);
    assert_eq!(canary.seen.requests(), 1);
    assert_eq!(production.seen.requests(), 0);

    get_with(proxy.addr, "/", "app.example.com", &[("X-Canary", "never")]);
    get_with(proxy.addr, "/", "app.example.com", &[("X-Canary", "maybe")]);
    get(proxy.addr, "/", "app.example.com");
    assert_eq!(canary.seen.requests(), 1, "only `always` diverts at weight 0");
    assert_eq!(production.seen.requests(), 3);
}

#[test]
fn a_cookie_diverts_when_no_header_rule_decides() {
    let production = echo();
    let canary = echo();
    let proxy = Proxy::start(canary_table(
        "app.example.com",
        &[production.addr],
        &[canary.addr],
        CanaryRules {
            backend: "canary",
            cookie: Some("canary"),
            weight: 0,
            ..Default::default()
        },
    ));

    get_with(
        proxy.addr,
        "/",
        "app.example.com",
        &[("Cookie", "session=abc; canary=always; theme=dark")],
    );

    assert_eq!(canary.seen.requests(), 1, "the cookie was not found");
    assert_eq!(production.seen.requests(), 0);
}

#[test]
fn a_weight_splits_traffic_within_statistical_bounds() {
    let production = echo();
    let canary = echo();
    let proxy = Proxy::start(canary_table(
        "app.example.com",
        &[production.addr],
        &[canary.addr],
        CanaryRules {
            backend: "canary",
            weight: 25,
            weight_total: 100,
            ..Default::default()
        },
    ));

    for _ in 0..400 {
        assert_eq!(get(proxy.addr, "/", "app.example.com").status, 200);
    }

    let diverted = canary.seen.requests();
    assert_eq!(diverted + production.seen.requests(), 400);
    // 25% of 400 is 100; the bounds are wide enough that a working splitter
    // never trips them and a broken one always does.
    assert!(
        (60..=150).contains(&diverted),
        "{diverted} of 400 diverted, expected about 100"
    );
}

#[test]
fn a_zero_weight_never_diverts_and_a_full_weight_always_does() {
    for (weight, expect_canary) in [(0u32, false), (100, true)] {
        let production = echo();
        let canary = echo();
        let proxy = Proxy::start(canary_table(
            "app.example.com",
            &[production.addr],
            &[canary.addr],
            CanaryRules {
                backend: "canary",
                weight,
                weight_total: 100,
                ..Default::default()
            },
        ));

        for _ in 0..50 {
            assert_eq!(get(proxy.addr, "/", "app.example.com").status, 200);
        }

        if expect_canary {
            assert_eq!(canary.seen.requests(), 50, "weight 100 must divert all");
            assert_eq!(production.seen.requests(), 0);
        } else {
            assert_eq!(canary.seen.requests(), 0, "weight 0 must divert none");
            assert_eq!(production.seen.requests(), 50);
        }
    }
}

#[test]
fn never_beats_a_full_weight() {
    let production = echo();
    let canary = echo();
    let proxy = Proxy::start(canary_table(
        "app.example.com",
        &[production.addr],
        &[canary.addr],
        CanaryRules {
            backend: "canary",
            header: Some("x-canary"),
            weight: 100,
            weight_total: 100,
            ..Default::default()
        },
    ));

    for _ in 0..10 {
        get_with(proxy.addr, "/", "app.example.com", &[("X-Canary", "never")]);
    }

    assert_eq!(canary.seen.requests(), 0, "an explicit `never` wins");
    assert_eq!(production.seen.requests(), 10);
}

#[test]
fn a_header_value_match_diverts() {
    let production = echo();
    let canary = echo();
    let proxy = Proxy::start(canary_table(
        "app.example.com",
        &[production.addr],
        &[canary.addr],
        CanaryRules {
            backend: "canary",
            header: Some("x-tier"),
            header_value: Some("beta"),
            weight: 0,
            ..Default::default()
        },
    ));

    get_with(proxy.addr, "/", "app.example.com", &[("X-Tier", "beta")]);
    assert_eq!(canary.seen.requests(), 1);

    get_with(proxy.addr, "/", "app.example.com", &[("X-Tier", "stable")]);
    get(proxy.addr, "/", "app.example.com");
    assert_eq!(canary.seen.requests(), 1);
    assert_eq!(production.seen.requests(), 2);
}
