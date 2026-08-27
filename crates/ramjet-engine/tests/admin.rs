//! The admin listener: probes and metrics, served by the data plane itself.
//!
//! The engine answers `/metrics`, `/healthz` and `/readyz` on core 0's reactor
//! rather than on a separate hyper server, so `--engine uring` needs no second
//! runtime. What must not change is the shape of the output: a dashboard that
//! loses a series when an operator changes engine looks like an outage.

mod common;

use std::sync::atomic::Ordering;

use common::{dead_addr, echo, get, table_for, Client, Proxy};
use ramjet_router::{Endpoint, LbPolicy, PathType, RouteTableBuilder};

#[test]
fn healthz_is_unconditional_and_readyz_is_gated() {
    let upstream = echo();
    let proxy = Proxy::start(table_for("app.example.com", &[upstream.addr]));

    let health = proxy.admin("/healthz");
    assert_eq!(health.status, 200);
    assert_eq!(health.text(), "ok\n");

    let not_ready = proxy.admin("/readyz");
    assert_eq!(not_ready.status, 503);
    assert_eq!(not_ready.text(), "not ready\n");

    proxy.readiness.store(true, Ordering::Release);

    let ready = proxy.admin("/readyz");
    assert_eq!(ready.status, 200);
    assert_eq!(ready.text(), "ready\n");
}

#[test]
fn an_unknown_admin_path_is_404() {
    let upstream = echo();
    let proxy = Proxy::start(table_for("app.example.com", &[upstream.addr]));

    let response = proxy.admin("/nope");

    assert_eq!(response.status, 404);
    assert_eq!(response.text(), "not found\n");
}

#[test]
fn a_write_method_on_the_admin_listener_is_405() {
    let upstream = echo();
    let proxy = Proxy::start(table_for("app.example.com", &[upstream.addr]));
    let admin = proxy.admin.expect("an admin listener");

    let mut client = Client::connect(admin);
    let response = client.send(b"POST /metrics HTTP/1.1\r\nHost: admin\r\nContent-Length: 0\r\n\r\n");

    assert_eq!(response.status, 405);
    assert_eq!(response.text(), "method not allowed\n");
}

#[test]
fn metrics_are_served_in_the_prometheus_exposition_format() {
    let upstream = echo();
    let proxy = Proxy::start(table_for("app.example.com", &[upstream.addr]));

    let response = proxy.admin("/metrics");

    assert_eq!(response.status, 200);
    assert_eq!(
        response.header("content-type"),
        Some("text/plain; version=0.0.4; charset=utf-8")
    );
    let text = response.text();
    // Every series the hyper engine exports, present under the same name and
    // with its HELP and TYPE lines.
    for series in [
        "ramjet_requests_total",
        "ramjet_upstream_latency_seconds",
        "ramjet_active_connections",
        "ramjet_route_table_generation",
        "ramjet_tls_handshakes_total",
        "ramjet_tls_handshake_failures_total",
        "ramjet_upstream_connect_failures_total",
        "ramjet_upstream_retries_total",
        "ramjet_upstream_timeouts_total",
        "ramjet_route_misses_total",
    ] {
        assert!(
            text.contains(&format!("# HELP {series} ")),
            "{series} has no HELP line"
        );
        assert!(
            text.contains(&format!("# TYPE {series} ")),
            "{series} has no TYPE line"
        );
    }
    // The bucket bound formatting that is easiest to get wrong.
    assert!(text.contains("le=\"0.0025\""), "{text}");
    assert!(text.contains("le=\"1\""), "not le=\"1.0\":\n{text}");
    assert!(text.contains("le=\"10\""), "not le=\"10.0\":\n{text}");
    assert!(text.contains("le=\"+Inf\""), "{text}");
    assert!(
        text.contains("ramjet_upstream_latency_seconds_sum 0.000000"),
        "the sum is fixed to six decimal places:\n{text}"
    );
}

#[test]
fn the_counters_move_with_the_traffic() {
    let upstream = echo();
    let proxy = Proxy::start(table_for("app.example.com", &[upstream.addr]));

    for _ in 0..3 {
        assert_eq!(get(proxy.addr, "/", "app.example.com").status, 200);
    }
    assert_eq!(get(proxy.addr, "/", "nobody.invalid").status, 404);

    let text = proxy.admin("/metrics").text();

    assert!(
        text.contains("ramjet_requests_total{code=\"2xx\"} 3"),
        "{text}"
    );
    assert!(
        text.contains("ramjet_requests_total{code=\"4xx\"} 1"),
        "{text}"
    );
    assert!(text.contains("ramjet_route_misses_total 1"), "{text}");
    // A 404 is not an upstream, so it must not be timed as one.
    assert!(
        text.contains("ramjet_upstream_latency_seconds_count 3"),
        "{text}"
    );
}

#[test]
fn the_generation_gauge_follows_the_published_table() {
    let upstream = echo();
    let mut builder = RouteTableBuilder::new();
    builder
        .backend(
            "app",
            LbPolicy::RoundRobin,
            vec![Endpoint::new(upstream.addr)],
        )
        .expect("a backend");
    builder.generation(7);
    builder
        .route(Some("app.example.com"), "/", PathType::Prefix, "app")
        .expect("a route");
    let proxy = Proxy::start(builder.build().expect("a table"));

    assert!(proxy
        .admin("/metrics")
        .text()
        .contains("ramjet_route_table_generation 7"));

    let mut next = RouteTableBuilder::new();
    next.backend(
        "app",
        LbPolicy::RoundRobin,
        vec![Endpoint::new(upstream.addr)],
    )
    .expect("a backend");
    next.generation(8);
    next.route(Some("app.example.com"), "/", PathType::Prefix, "app")
        .expect("a route");
    proxy.routes.store(next.build().expect("a table"));

    assert!(proxy
        .admin("/metrics")
        .text()
        .contains("ramjet_route_table_generation 8"));
}

#[test]
fn upstream_failures_are_counted_separately_from_responses() {
    let mut builder = RouteTableBuilder::new();
    builder
        .backend("app", LbPolicy::RoundRobin, vec![Endpoint::new(dead_addr())])
        .expect("a backend");
    builder
        .route(Some("app.example.com"), "/", PathType::Prefix, "app")
        .expect("a route");
    let proxy = Proxy::start(builder.build().expect("a table"));

    assert_eq!(get(proxy.addr, "/", "app.example.com").status, 502);

    let text = proxy.admin("/metrics").text();
    assert!(
        text.contains("ramjet_requests_total{code=\"5xx\"} 1"),
        "{text}"
    );
    let failures = text
        .lines()
        .find(|line| line.starts_with("ramjet_upstream_connect_failures_total "))
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|n| n.parse::<u64>().ok())
        .unwrap_or(0);
    assert!(failures >= 1, "connect failures were not counted:\n{text}");
}

#[test]
fn active_connections_returns_to_zero() {
    let upstream = echo();
    let proxy = Proxy::start(table_for("app.example.com", &[upstream.addr]));

    for _ in 0..5 {
        assert_eq!(get(proxy.addr, "/", "app.example.com").status, 200);
    }

    // The gauge drains as connections close; give the reactor a moment to see
    // the closes, since a client's FIN and our accounting are not simultaneous.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
    loop {
        let text = proxy.admin("/metrics").text();
        let gauge = text
            .lines()
            .find(|line| line.starts_with("ramjet_active_connections "))
            .and_then(|line| line.split_whitespace().nth(1))
            .and_then(|n| n.parse::<i64>().ok())
            .unwrap_or(-1);
        // The scrape's own connection is not counted: admin traffic is not data
        // plane traffic.
        if gauge == 0 {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the gauge settled at {gauge}, not 0:\n{text}"
        );
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
}

#[test]
fn metrics_from_several_cores_are_merged() {
    let upstream = echo();
    let proxy = Proxy::with_config(table_for("app.example.com", &[upstream.addr]), |config| {
        config.workers = Some(4);
    });

    for _ in 0..24 {
        assert_eq!(get(proxy.addr, "/", "app.example.com").status, 200);
    }

    let text = proxy.admin("/metrics").text();
    assert!(
        text.contains("ramjet_requests_total{code=\"2xx\"} 24"),
        "every core's counters must be summed at scrape:\n{text}"
    );
}
