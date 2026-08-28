//! The audit webhook, against a real socket.
//!
//! The unit tests cover which URLs are accepted. What they cannot cover is the
//! part that matters operationally: that a POST is actually made, that it
//! carries the diff as JSON, and that a collector which is missing, slow, or
//! rude does not slow down or break a publish. So this binds a listener, reads
//! the request off it, and — for the failure cases — asserts on how long the
//! call took rather than on what came back.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use ramjet_controller::{AuditSink, ConfigDiff};
use ramjet_router::{Endpoint, LbPolicy, PathType, RouteTable, RouteTableBuilder};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::mpsc;

/// A one-route table, which is enough to produce a diff with content in it.
fn table(host: &str) -> RouteTable {
    let mut builder = RouteTableBuilder::new();
    builder.generation(42);
    builder
        .backend(
            "prod/api:80",
            LbPolicy::RoundRobin,
            vec![Endpoint::new("10.0.0.1:8080".parse().expect("an address"))],
        )
        .expect("registers");
    builder
        .route(Some(host), "/", PathType::Prefix, "prod/api:80")
        .expect("drafts");
    builder.build().expect("builds")
}

/// A listener that reads one request and reports it, then answers `status`.
async fn collector(status: &'static str) -> (SocketAddr, mpsc::Receiver<String>) {
    let listener = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
        .await
        .expect("binds");
    let addr = listener.local_addr().expect("a port");
    let (tx, rx) = mpsc::channel(4);

    tokio::spawn(async move {
        while let Ok((mut stream, _)) = listener.accept().await {
            let tx = tx.clone();
            tokio::spawn(async move {
                // One read is enough: the whole request is a header block and a
                // small JSON body, written together.
                let mut buffer = vec![0u8; 16 * 1024];
                let Ok(read) = stream.read(&mut buffer).await else {
                    return;
                };
                let request = String::from_utf8_lossy(&buffer[..read]).into_owned();
                let _ = stream
                    .write_all(
                        format!("HTTP/1.1 {status}\r\ncontent-length: 0\r\nconnection: close\r\n\r\n")
                            .as_bytes(),
                    )
                    .await;
                let _ = tx.send(request).await;
            });
        }
    });

    (addr, rx)
}

/// Splits a raw HTTP/1.1 request into its head and its body.
fn split(request: &str) -> (&str, &str) {
    request.split_once("\r\n\r\n").expect("a complete request")
}

#[tokio::test]
async fn the_webhook_receives_the_diff_as_json() {
    let (addr, mut received) = collector("200 OK").await;
    let sink = AuditSink::new(None, "ramjet", Some(&format!("http://{addr}/ingress")))
        .await
        .expect("a plaintext URL is accepted");

    let diff = ConfigDiff::compute(None, &table("example.com"));
    sink.applied(&diff, true);

    let request = tokio::time::timeout(Duration::from_secs(5), received.recv())
        .await
        .expect("the webhook should be called")
        .expect("a request");
    let (head, body) = split(&request);

    assert!(head.starts_with("POST /ingress HTTP/1.1"), "{head}");
    assert!(
        head.to_ascii_lowercase().contains("content-type: application/json"),
        "{head}"
    );

    let value: serde_json::Value = serde_json::from_str(body)
        .unwrap_or_else(|e| panic!("the body should be JSON: {e}: {body}"));
    assert_eq!(value["routes_added"][0], "example.com / -> prod/api:80");
    assert_eq!(value["hosts_added"][0], "example.com");
    assert!(
        value["summary"].as_str().is_some_and(|s| s.contains("gen")),
        "{value}"
    );
    assert!(value["certs_rotated"].is_array());
    assert!(value["backends_changed"].is_array());
    assert!(value["routes_removed"].is_array());
    assert!(value["hosts_removed"].is_array());
}

/// A generation held back by a rollback pin changed nothing about what the
/// cluster serves, so there is nothing to tell a collector about.
#[tokio::test]
async fn a_generation_held_back_by_a_pin_is_not_posted() {
    let (addr, mut received) = collector("200 OK").await;
    let sink = AuditSink::new(None, "ramjet", Some(&format!("http://{addr}/")))
        .await
        .expect("accepted");

    sink.applied(&ConfigDiff::compute(None, &table("example.com")), false);

    let waited = tokio::time::timeout(Duration::from_millis(300), received.recv()).await;
    assert!(
        waited.is_err(),
        "an unpublished generation must not be announced as one that went live"
    );
}

/// The property the whole design rests on: whatever the collector does, the
/// publish that triggered it has already happened and did not wait.
#[tokio::test]
async fn a_collector_that_never_answers_does_not_hold_up_a_publish() {
    // Bound but never accepted, so a connection sits in the backlog and the
    // request never completes.
    let listener = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
        .await
        .expect("binds");
    let addr = listener.local_addr().expect("a port");
    let _held = Arc::new(listener);

    let sink = AuditSink::new(None, "ramjet", Some(&format!("http://{addr}/")))
        .await
        .expect("accepted");

    let started = Instant::now();
    sink.applied(&ConfigDiff::compute(None, &table("example.com")), true);
    assert!(
        started.elapsed() < Duration::from_millis(100),
        "the POST must be fire-and-forget; this one took {:?}",
        started.elapsed()
    );
}

#[tokio::test]
async fn a_collector_that_is_not_there_is_survivable() {
    // Bound, read the port, dropped: nothing is listening on it now.
    let addr = {
        let listener = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
            .await
            .expect("binds");
        listener.local_addr().expect("a port")
    };

    let sink = AuditSink::new(None, "ramjet", Some(&format!("http://{addr}/")))
        .await
        .expect("accepted");
    sink.applied(&ConfigDiff::compute(None, &table("example.com")), true);

    // The failure is logged inside a spawned task; what this asserts is that
    // the sink is still usable afterwards, because a webhook that poisoned the
    // audit trail on the first refused connection would be worse than none.
    tokio::time::sleep(Duration::from_millis(50)).await;
    sink.pinned(41, 42);
    sink.resumed(42);
}

#[tokio::test]
async fn a_rejected_post_is_not_retried() {
    let (addr, mut received) = collector("500 Internal Server Error").await;
    let sink = AuditSink::new(None, "ramjet", Some(&format!("http://{addr}/")))
        .await
        .expect("accepted");

    sink.applied(&ConfigDiff::compute(None, &table("example.com")), true);
    tokio::time::timeout(Duration::from_secs(5), received.recv())
        .await
        .expect("the first attempt happens")
        .expect("a request");

    let again = tokio::time::timeout(Duration::from_millis(300), received.recv()).await;
    assert!(
        again.is_err(),
        "one attempt only: the record is already in the log and the Event"
    );
}

#[tokio::test]
async fn a_sink_with_no_webhook_posts_nothing() {
    let sink = AuditSink::new(None, "ramjet", None)
        .await
        .expect("no webhook is a valid configuration");
    sink.applied(&ConfigDiff::compute(None, &table("example.com")), true);
}
