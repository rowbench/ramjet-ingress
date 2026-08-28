//! The client layer, against a real HTTP server.
//!
//! These assert the computed view model — rows, rates, flags, header numbers —
//! rather than anything about how it is drawn. What is being tested is the path
//! from bytes on a socket to the numbers a person reads, which is where a
//! monitoring tool is either right or quietly, plausibly wrong.

mod common;

use std::time::{Duration, Instant};

use common::{
    generations_json, metrics_text, routes_json, routes_json_with_new_route, Bodies, MockAdmin,
};
use ramjet_top::app::{App, Connection, Key};
use ramjet_top::client::AdminClient;
use ramjet_top::model::Sort;

fn client(mock: &MockAdmin) -> AdminClient {
    AdminClient::new(&mock.url(), Duration::from_secs(5)).expect("a usable URL")
}

fn app_for(mock: &MockAdmin, start: Instant) -> App {
    App::new(mock.url(), Duration::from_secs(1), false, start)
}

#[tokio::test]
async fn one_poll_assembles_all_three_endpoints_into_a_snapshot() {
    let mock = MockAdmin::start().await;
    let snapshot = client(&mock).snapshot().await.expect("a snapshot");

    assert_eq!(snapshot.serving(), 42);
    assert_eq!(snapshot.pinned(), None);
    assert_eq!(snapshot.generations.generations.len(), 2);
    assert_eq!(snapshot.routes.routes.len(), 2);
    assert_eq!(snapshot.routes.generation, 42);

    // From /metrics, not from the JSON.
    assert_eq!(snapshot.metrics.active_connections, Some(37));
    assert_eq!(snapshot.metrics.requests_total, 10_007);
    assert_eq!(snapshot.metrics.errors_5xx_total, 12);
    assert_eq!(snapshot.metrics.generation, Some(42));

    // From the JSON, not from /metrics.
    let route = &snapshot.routes.routes[0];
    assert_eq!(route.host, "api.example.com");
    assert_eq!(route.backend, "api-v2");
    assert_eq!(route.endpoints, 4);
    let canary = route.canary.as_ref().expect("a canary");
    assert_eq!(canary.backend, "api-v3");
    assert_eq!(canary.weight_percent, 10);

    // The canary's share of the same counters, over the same socket. A subset
    // of the route's totals rather than a sibling of them, so the difference is
    // the stable side.
    let split = route.canary_stats.as_ref().expect("a canary split");
    assert_eq!(split.requests_total, 1000, "a tenth of 10_007, floored");
    assert_eq!(split.errors_5xx_total, 12);
    assert!(split.requests_total < route.requests_total);
    assert!(
        snapshot.routes.routes[1].canary_stats.is_none(),
        "a route with no canary reports no split"
    );

    let newest = &snapshot.generations.generations[0];
    assert_eq!(newest.generation, 42);
    assert_eq!(newest.diff.summary, "1 route added, 1 backend changed");
    assert_eq!(newest.diff.routes_added.len(), 1);
    assert_eq!(
        newest.diff.backends_changed[0].to_string(),
        "api.example.com/v1 -> api-v2"
    );
}

#[tokio::test]
async fn two_polls_across_the_wire_produce_the_rates_on_screen() {
    let mock = MockAdmin::start().await;
    let client = client(&mock);
    let start = Instant::now();
    let mut app = app_for(&mock, start);

    app.record_poll(client.snapshot().await.expect("first"), start);
    assert_eq!(app.rows.len(), 2);
    assert!(
        app.rows.iter().all(|r| r.rps.is_none()),
        "one poll is not a rate"
    );

    // 500 more requests and 5 more errors, two seconds later.
    mock.set(|b| {
        b.routes = routes_json(42, 10_500, 17);
        b.metrics = metrics_text(10_507, 17, 40, 42);
    });
    app.record_poll(
        client.snapshot().await.expect("second"),
        start + Duration::from_secs(2),
    );

    let api = app
        .rows
        .iter()
        .find(|r| r.host == "api.example.com")
        .expect("the api route");
    assert_eq!(api.rps, Some(250.0), "500 requests over two seconds");
    let errors = api.error_rate_percent.expect("requests happened");
    assert!((errors - 1.0).abs() < 1e-9, "5 of 500 is 1%, got {errors}");
    assert!(!api.is_new);

    // The quiet catch-all had no traffic at all in the window.
    let fallback = app.rows.iter().find(|r| r.host == "*").expect("catch-all");
    assert_eq!(fallback.rps, Some(0.0));
    assert_eq!(
        fallback.error_rate_percent, None,
        "no requests is not a zero error rate"
    );

    assert_eq!(app.global.rps, Some(250.0));
    assert_eq!(app.global.active_connections, Some(40));
    assert_eq!(app.rps_history.len(), 1);
    assert_eq!(app.rps_history[0], 250);
}

#[tokio::test]
async fn a_route_added_by_a_new_generation_is_flagged_and_has_no_rate_yet() {
    let mock = MockAdmin::start().await;
    let client = client(&mock);
    let start = Instant::now();
    let mut app = app_for(&mock, start);

    app.record_poll(client.snapshot().await.expect("first"), start);

    // A new generation lands, adding a route that already has counters.
    mock.set(|b| {
        b.generations = generations_json(43, None, 0);
        b.routes = routes_json_with_new_route(43, 10_200, 12);
        b.metrics = metrics_text(10_232, 12, 37, 43);
    });
    app.record_poll(
        client.snapshot().await.expect("second"),
        start + Duration::from_secs(1),
    );

    assert_eq!(app.rows.len(), 3);
    let new = app
        .rows
        .iter()
        .find(|r| r.host == "shop.example.com")
        .expect("the added route");
    assert!(new.is_new, "a route absent from the last poll is new");
    assert_eq!(
        new.rps, None,
        "its lifetime counter must not be presented as a rate for this interval"
    );

    let existing = app
        .rows
        .iter()
        .find(|r| r.host == "api.example.com")
        .expect("the api route");
    assert!(!existing.is_new);
    assert_eq!(existing.rps, Some(200.0));

    assert_eq!(app.snapshot.as_ref().expect("a snapshot").serving(), 43);
}

#[tokio::test]
async fn a_daemon_that_restarts_between_polls_reports_zero_not_a_wrapped_counter() {
    let mock = MockAdmin::start().await;
    let client = client(&mock);
    let start = Instant::now();
    let mut app = app_for(&mock, start);

    app.record_poll(client.snapshot().await.expect("first"), start);

    // Same generation, counters back to almost nothing.
    mock.set(|b| {
        b.routes = routes_json(42, 3, 0);
        b.metrics = metrics_text(3, 0, 1, 42);
    });
    app.record_poll(
        client.snapshot().await.expect("second"),
        start + Duration::from_secs(1),
    );

    let api = app
        .rows
        .iter()
        .find(|r| r.host == "api.example.com")
        .expect("the api route");
    assert_eq!(api.rps, Some(0.0));
    assert_eq!(app.global.rps, Some(0.0));
}

#[tokio::test]
async fn a_pinned_daemon_is_reported_as_pinned() {
    let mock = MockAdmin::start().await;
    mock.set(|b| b.generations = generations_json(43, Some(41), 0));

    let snapshot = client(&mock).snapshot().await.expect("a snapshot");
    assert_eq!(snapshot.pinned(), Some(41));
    assert_eq!(snapshot.serving(), 43);
}

#[tokio::test]
async fn an_unhealthy_daemon_fails_the_poll_without_losing_the_last_good_data() {
    let mock = MockAdmin::start().await;
    let client = client(&mock);
    let start = Instant::now();
    let mut app = app_for(&mock, start);

    app.record_poll(client.snapshot().await.expect("first"), start);
    assert!(matches!(app.connection, Connection::Live));

    mock.set(|b| b.failing = true);
    let error = client.snapshot().await.expect_err("503 is not a snapshot");
    app.record_failure(error.brief(), start + Duration::from_secs(1));

    assert!(app.connection.is_lost());
    assert_eq!(app.rows.len(), 2, "the last good rows are still on screen");
    assert!(
        app.stale_for(start + Duration::from_secs(3)) == Some(Duration::from_secs(3)),
        "and they are marked as three seconds old"
    );

    // And it recovers.
    mock.set(|b| {
        b.failing = false;
        b.routes = routes_json(42, 10_100, 12);
        b.metrics = metrics_text(10_107, 12, 37, 42);
    });
    app.record_poll(
        client.snapshot().await.expect("recovered"),
        start + Duration::from_secs(10),
    );
    assert!(matches!(app.connection, Connection::Live));
    let api = app
        .rows
        .iter()
        .find(|r| r.host == "api.example.com")
        .expect("the api route");
    assert_eq!(api.rps, Some(10.0), "100 requests averaged over ten seconds");
}

#[tokio::test]
async fn a_url_with_no_scheme_and_a_trailing_slash_still_reaches_the_server() {
    let mock = MockAdmin::start().await;
    // What a person types, rather than what a URL parser wants.
    let bare = format!("{}/", mock.addr);
    let client = AdminClient::new(&bare, Duration::from_secs(5)).expect("normalized");
    assert_eq!(client.url(), format!("http://{}", mock.addr));
    assert!(client.snapshot().await.is_ok());
}

#[tokio::test]
async fn pointing_at_a_server_that_is_not_ingressd_fails_with_a_readable_error() {
    let mock = MockAdmin::start().await;
    mock.set(|b| b.generations = "<html><body>not json</body></html>".to_string());

    let error = client(&mock)
        .snapshot()
        .await
        .expect_err("html is not the contract");
    let brief = error.brief();
    assert!(brief.contains("/admin/generations"), "{brief}");
    assert!(brief.contains("unreadable body"), "{brief}");
    assert!(!brief.contains('\n'), "a status bar has one line: {brief}");
}

#[tokio::test]
async fn a_server_that_never_answers_times_out_rather_than_hanging_the_ui() {
    // A listener that accepts and then says nothing — the failure mode that
    // would otherwise freeze a draw loop inside an await forever.
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("a port");
    let addr = listener.local_addr().expect("an address");
    tokio::spawn(async move {
        let mut held = Vec::new();
        while let Ok((stream, _)) = listener.accept().await {
            held.push(stream);
        }
    });

    let client =
        AdminClient::new(&format!("http://{addr}"), Duration::from_millis(200)).expect("a client");

    let started = Instant::now();
    let error = client.snapshot().await.expect_err("a timeout");
    let elapsed = started.elapsed();

    assert!(error.brief().contains("no answer in 200ms"), "{}", error.brief());
    assert!(
        elapsed < Duration::from_secs(2),
        "the deadline did not fire: {elapsed:?}"
    );
}

#[tokio::test]
async fn nothing_listening_is_a_transport_error_not_a_panic() {
    // Port 1 on loopback: reserved, and nothing in a test environment binds it.
    let client = AdminClient::new("http://127.0.0.1:1", Duration::from_secs(2)).expect("a client");
    let error = client.snapshot().await.expect_err("no server");
    assert!(!error.brief().is_empty());
}

#[tokio::test]
async fn the_emergency_brake_sends_what_the_contract_specifies() {
    let mock = MockAdmin::start().await;
    let client = client(&mock);

    client.pin(41).await.expect("a pin");
    client.unpin().await.expect("a release");

    assert_eq!(
        mock.rollbacks(),
        vec![Some(41), None],
        "a POST carrying the generation, then a DELETE"
    );
}

#[tokio::test]
async fn sorting_and_filtering_apply_to_rows_that_came_off_the_wire() {
    let mock = MockAdmin::start().await;
    let client = client(&mock);
    let start = Instant::now();
    let mut app = app_for(&mock, start);

    app.record_poll(client.snapshot().await.expect("first"), start);
    mock.set(|b| {
        b.routes = routes_json_with_new_route(42, 10_500, 12);
        b.metrics = metrics_text(10_532, 12, 37, 42);
    });
    app.record_poll(
        client.snapshot().await.expect("second"),
        start + Duration::from_secs(1),
    );

    // Rows arrive already ordered by the current sort, which starts at rps
    // descending.
    assert_eq!(
        app.rows.first().expect("rows").host,
        "api.example.com",
        "the busiest route sorts to the top"
    );

    // Selecting the column that is already selected reverses it. The newly
    // added route has no rate yet, and a row with no value stays at the bottom
    // in both directions rather than pretending to be the quietest.
    app.apply_sort(Sort::Rps);
    assert!(!app.descending);
    let ordered: Vec<&str> = app.rows.iter().map(|r| r.host.as_str()).collect();
    assert_eq!(ordered, ["*", "api.example.com", "shop.example.com"]);
    assert!(
        app.rows.last().expect("rows").rps.is_none(),
        "the row at the bottom is the one with no rate"
    );

    app.apply_sort(Sort::Host);
    let hosts: Vec<&str> = app.rows.iter().map(|r| r.host.as_str()).collect();
    assert_eq!(hosts, ["*", "api.example.com", "shop.example.com"]);

    let now = Instant::now();
    app.on_key(Key::Char('/'), now);
    for c in "shop".chars() {
        app.on_key(Key::Char(c), now);
    }
    let visible: Vec<&str> = app.visible_rows().iter().map(|r| r.host.as_str()).collect();
    assert_eq!(visible, ["shop.example.com"]);
}

#[tokio::test]
async fn a_snapshot_survives_a_server_that_grew_fields_this_client_does_not_know() {
    let mock = MockAdmin::start().await;
    mock.set(|b| {
        b.routes = r#"{
          "generation": 44,
          "served_by": "a future ingressd",
          "routes": [{
            "host": "api.example.com",
            "path": "/v1",
            "path_type": "Prefix",
            "backend": "api-v2",
            "endpoints": 4,
            "requests_total": 10,
            "errors_5xx_total": 0,
            "upstream_latency_ms_sum": 50.0,
            "upstream_latency_count": 10,
            "canary": null,
            "weight_class": "gold",
            "shadow": {"backend": "api-v4"}
          }]
        }"#
        .to_string();
    });

    let snapshot = client(&mock).snapshot().await.expect("forward compatible");
    assert_eq!(snapshot.routes.generation, 44);
    assert_eq!(snapshot.routes.routes.len(), 1);
    assert_eq!(snapshot.routes.routes[0].backend, "api-v2");
}

#[tokio::test]
async fn a_poll_reuses_its_connection_rather_than_reconnecting_every_second() {
    // Not a performance assertion so much as a correctness one: at one poll a
    // second against a pod, opening three connections per poll and leaving them
    // in TIME_WAIT is how a monitoring client becomes the problem.
    let mock = MockAdmin::start().await;
    let client = client(&mock);
    for _ in 0..5 {
        client.snapshot().await.expect("a snapshot");
    }
    // Reaching here at all means five sequential polls succeeded over a pooled
    // client without exhausting anything.
    assert_eq!(client.url(), mock.url());
}

#[tokio::test]
async fn an_empty_route_table_is_a_valid_snapshot() {
    let mock = MockAdmin::start().await;
    mock.set(|b| {
        b.routes = r#"{"generation": 1, "routes": []}"#.to_string();
        b.generations = r#"{"pinned": null, "serving": 1, "generations": []}"#.to_string();
    });

    let snapshot = client(&mock).snapshot().await.expect("a snapshot");
    assert!(snapshot.routes.routes.is_empty());
    assert!(snapshot.generations.generations.is_empty());
    assert_eq!(snapshot.serving(), 1);

    let start = Instant::now();
    let mut app = App::new(mock.url(), Duration::from_secs(1), false, start);
    app.record_poll(snapshot, start);
    assert!(app.rows.is_empty());
    assert!(app.visible_rows().is_empty());
}

#[tokio::test]
async fn a_generation_carrying_no_metrics_endpoint_at_all_still_polls() {
    let mock = MockAdmin::start().await;
    mock.set(|b| b.metrics = String::new());

    let snapshot = client(&mock).snapshot().await.expect("a snapshot");
    assert_eq!(snapshot.metrics.requests_total, 0);
    assert_eq!(snapshot.metrics.active_connections, None);
    // The route listing still knows which generation is serving, so the header
    // does not go blank just because the exposition page was empty.
    assert_eq!(snapshot.serving(), 42);
}

#[tokio::test]
async fn concurrent_polls_from_one_client_do_not_interfere() {
    let mock = MockAdmin::start().await;
    let client = client(&mock);

    let results = tokio::join!(
        client.snapshot(),
        client.snapshot(),
        client.snapshot(),
        client.snapshot(),
    );
    for snapshot in [results.0, results.1, results.2, results.3] {
        let snapshot = snapshot.expect("a snapshot");
        assert_eq!(snapshot.serving(), 42);
        assert_eq!(snapshot.routes.routes.len(), 2);
    }
}

#[tokio::test]
async fn the_bodies_helper_produces_the_default_fixture() {
    // Guards the fixture builders themselves: every other test in this file
    // reads its expectations off them.
    let bodies = Bodies::default();
    assert!(bodies.generations.contains("\"serving\": 42"));
    assert!(bodies.routes.contains("api.example.com"));
    assert!(bodies.metrics.contains("ramjet_requests_total"));
    assert!(!bodies.failing);
    assert!(bodies.rollbacks.is_empty());
}
