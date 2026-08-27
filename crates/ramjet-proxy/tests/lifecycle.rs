//! Shutdown: stop accepting, then finish what is already running.
//!
//! The interesting case is the middle one — a request that was in flight when
//! the signal arrived. Dropping it would make every rolling update a burst of
//! client-visible errors, which is the exact failure a graceful shutdown exists
//! to prevent, and it is invisible unless a test holds a request open across
//! the signal on purpose.

mod common;

use std::time::Duration;

use common::*;
use http::StatusCode;

#[tokio::test]
async fn an_in_flight_request_finishes_after_the_shutdown_signal() {
    let slow = spawn_slow(Duration::from_millis(400)).await;
    let proxy = TestProxy::start(single_route("app.example.com", "/", &[slow])).await;
    let addr = proxy.http;

    let request = tokio::spawn(async move { get(addr, "app.example.com", "/").await });

    // Let the request reach the upstream, then ask the server to stop while it
    // is still waiting for the answer.
    tokio::time::sleep(Duration::from_millis(100)).await;
    proxy.signal_shutdown();

    let reply = tokio::time::timeout(Duration::from_secs(5), request)
        .await
        .expect("the request completed")
        .expect("the task did not panic");

    assert_eq!(
        reply.status,
        StatusCode::OK,
        "a request already in flight must not be dropped by shutdown"
    );
    assert_eq!(reply.text(), "slow");

    proxy.wait().await.expect("a clean drain");
}

#[tokio::test]
async fn new_connections_are_refused_once_shutdown_has_started() {
    let app = spawn_echo("app").await;
    let proxy = TestProxy::start(single_route("app.example.com", "/", &[app])).await;
    let addr = proxy.http;

    assert_eq!(get(addr, "app.example.com", "/").await.status, StatusCode::OK);

    proxy.signal_shutdown();
    proxy.wait().await.expect("a clean drain");

    // The listener is closed, so the load balancer finds out immediately rather
    // than by having a connection accepted and then abandoned.
    let refused = tokio::net::TcpStream::connect(addr).await;
    assert!(
        refused.is_err(),
        "the listener should be closed once the drain has finished"
    );
}

#[tokio::test]
async fn shutdown_returns_promptly_when_nothing_is_in_flight() {
    let app = spawn_echo("app").await;
    let proxy = TestProxy::start_with(
        single_route("app.example.com", "/", &[app]),
        ProxyOptions {
            grace: Duration::from_secs(30),
            ..Default::default()
        },
    )
    .await;

    // An idle server must not sit out the whole grace period: the drain ends
    // when the connections do, and the deadline is only a ceiling.
    let started = std::time::Instant::now();
    proxy.shutdown().await.expect("a clean drain");
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "the drain took {:?} with nothing to drain",
        started.elapsed()
    );
}

#[tokio::test]
async fn an_exceeded_grace_period_is_reported() {
    // An upstream slower than the deadline: the drain has to give up, and
    // saying so is the difference between a deadline and a lie.
    let glacial = spawn_slow(Duration::from_secs(30)).await;
    let proxy = TestProxy::start_with(
        single_route("app.example.com", "/", &[glacial]),
        ProxyOptions {
            grace: Duration::from_millis(300),
            ..Default::default()
        },
    )
    .await;
    let addr = proxy.http;

    let inflight = tokio::spawn(async move { get(addr, "app.example.com", "/").await });
    tokio::time::sleep(Duration::from_millis(100)).await;

    proxy.signal_shutdown();
    let outcome = tokio::time::timeout(Duration::from_secs(10), proxy.wait())
        .await
        .expect("the drain gave up rather than hanging");

    assert_eq!(
        outcome.map_err(|error| error.kind()),
        Err(std::io::ErrorKind::TimedOut),
        "a drain that hit its deadline must say so rather than pretend"
    );
    inflight.abort();
}

#[tokio::test]
async fn an_open_tunnel_does_not_hold_the_drain_open() {
    // Documented behaviour, asserted so it stays a decision: once a connection
    // has been upgraded there is no request boundary left to finish at, and a
    // WebSocket can stay open for hours. Waiting for one would mean every
    // rolling update stalls until the deadline. Tunnels end with the process.
    let tunnel_upstream = spawn_raw(|mut stream| async move {
        let _ = read_head(&mut stream).await;
        use tokio::io::AsyncWriteExt;
        let _ = stream
            .write_all(
                b"HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: raw\r\n\r\n",
            )
            .await;
        let _ = stream.flush().await;
        tokio::time::sleep(Duration::from_secs(60)).await;
    })
    .await;

    let proxy = TestProxy::start_with(
        single_route("app.example.com", "/", &[tunnel_upstream]),
        ProxyOptions {
            grace: Duration::from_secs(30),
            ..Default::default()
        },
    )
    .await;

    let (mut sender, connection) = handshake(proxy.http).await;
    let driver = tokio::spawn(connection);
    let response = sender
        .send_request(
            request("app.example.com", "/")
                .header(http::header::CONNECTION, "Upgrade")
                .header(http::header::UPGRADE, "raw")
                .body(empty_body())
                .expect("a request"),
        )
        .await
        .expect("a response");
    assert_eq!(response.status(), StatusCode::SWITCHING_PROTOCOLS);

    let started = std::time::Instant::now();
    proxy.shutdown().await.expect("the drain finished cleanly");
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "the drain waited {:?} for a tunnel that will never close",
        started.elapsed()
    );
    driver.abort();
}
