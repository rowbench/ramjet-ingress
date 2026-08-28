//! What the upstream sees, and what the client gets back.
//!
//! The echo upstream reflects every request header as `echo-<name>`, so these
//! assertions are about bytes that actually crossed two TCP connections rather
//! than about a `HeaderMap` a unit test built by hand.

mod common;

use common::*;
use http::header;
use http::StatusCode;

#[tokio::test]
async fn forwarded_headers_describe_this_hop() {
    let app = spawn_echo("app").await;
    let proxy = TestProxy::start(single_route("app.example.com", "/", &[app])).await;

    let reply = get(proxy.http, "app.example.com", "/").await;
    assert_eq!(reply.status, StatusCode::OK);
    assert_eq!(reply.header("echo-x-forwarded-for"), Some("127.0.0.1"));
    assert_eq!(reply.header("echo-x-real-ip"), Some("127.0.0.1"));
    assert_eq!(reply.header("echo-x-forwarded-proto"), Some("http"));
    assert_eq!(
        reply.header("echo-x-forwarded-host"),
        Some("app.example.com")
    );
}

#[tokio::test]
async fn forwarded_for_is_appended_to_an_existing_trail() {
    // Overwriting instead of appending is how a proxy behind a cloud load
    // balancer loses the real client address.
    let app = spawn_echo("app").await;
    let proxy = TestProxy::start(single_route("app.example.com", "/", &[app])).await;

    let reply = send(
        proxy.http,
        request("app.example.com", "/")
            .header("x-forwarded-for", "203.0.113.9")
            .body(empty_body())
            .expect("a request"),
    )
    .await;

    assert_eq!(
        reply.header("echo-x-forwarded-for"),
        Some("203.0.113.9, 127.0.0.1")
    );
}

#[tokio::test]
async fn a_request_id_is_generated_when_absent_and_reused_when_present() {
    let app = spawn_echo("app").await;
    let proxy = TestProxy::start(single_route("app.example.com", "/", &[app])).await;

    let generated = get(proxy.http, "app.example.com", "/").await;
    let id = generated
        .header("echo-x-request-id")
        .expect("an id was generated");
    assert_eq!(id.len(), 32);
    assert!(id.bytes().all(|b| b.is_ascii_hexdigit()));

    let reused = send(
        proxy.http,
        request("app.example.com", "/")
            .header("x-request-id", "trace-from-the-edge")
            .body(empty_body())
            .expect("a request"),
    )
    .await;
    assert_eq!(
        reused.header("echo-x-request-id"),
        Some("trace-from-the-edge"),
        "an incoming id must survive so a trace stitches together"
    );
}

#[tokio::test]
async fn hop_by_hop_request_headers_do_not_reach_the_upstream() {
    let app = spawn_echo("app").await;
    let proxy = TestProxy::start(single_route("app.example.com", "/", &[app])).await;

    let reply = send(
        proxy.http,
        request("app.example.com", "/")
            .header(header::CONNECTION, "keep-alive, x-hop-secret")
            .header("x-hop-secret", "leaked")
            .header("keep-alive", "timeout=5")
            .header("proxy-connection", "keep-alive")
            .header("x-end-to-end", "kept")
            .body(empty_body())
            .expect("a request"),
    )
    .await;

    assert_eq!(reply.status, StatusCode::OK);
    assert_eq!(
        reply.header("echo-x-hop-secret"),
        None,
        "a Connection-listed header leaked across the hop"
    );
    assert_eq!(reply.header("echo-keep-alive"), None);
    assert_eq!(reply.header("echo-proxy-connection"), None);
    assert_eq!(reply.header("echo-x-end-to-end"), Some("kept"));
}

#[tokio::test]
async fn hop_by_hop_response_headers_do_not_reach_the_client() {
    // The upstream is raw TCP so it can emit headers hyper would otherwise
    // refuse to send, which is exactly the case the strip exists for.
    let upstream = spawn_raw(|mut stream| async move {
        let _ = read_head(&mut stream).await;
        let response = concat!(
            "HTTP/1.1 200 OK\r\n",
            "Content-Length: 2\r\n",
            "Connection: keep-alive, X-Hop-Secret\r\n",
            "X-Hop-Secret: leaked\r\n",
            "Keep-Alive: timeout=5\r\n",
            "X-End-To-End: kept\r\n",
            "\r\n",
            "ok",
        );
        use tokio::io::AsyncWriteExt;
        let _ = stream.write_all(response.as_bytes()).await;
        let _ = stream.flush().await;
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    })
    .await;

    let proxy = TestProxy::start(single_route("app.example.com", "/", &[upstream])).await;
    let reply = get(proxy.http, "app.example.com", "/").await;

    assert_eq!(reply.status, StatusCode::OK);
    assert_eq!(reply.text(), "ok");
    assert_eq!(
        reply.header("x-hop-secret"),
        None,
        "a Connection-listed response header leaked to the client"
    );
    assert_eq!(reply.header("keep-alive"), None);
    assert_eq!(reply.header("x-end-to-end"), Some("kept"));
}

#[tokio::test]
async fn a_grpc_request_is_refused_with_an_explanation() {
    // Until upstreams speak HTTP/2, a gRPC request would be silently downgraded
    // into something the backend cannot parse. Saying so is better.
    let app = spawn_echo("app").await;
    let proxy = TestProxy::start(single_route("grpc.example.com", "/", &[app])).await;

    let reply = send(
        proxy.http,
        request("grpc.example.com", "/pkg.Service/Method")
            .method("POST")
            .header(header::CONTENT_TYPE, "application/grpc")
            .body(empty_body())
            .expect("a request"),
    )
    .await;

    assert_eq!(reply.status, StatusCode::BAD_GATEWAY);
    assert!(reply.text().contains("HTTP/2"), "{}", reply.text());
}

#[tokio::test]
async fn a_header_block_over_the_ceiling_is_refused_rather_than_buffered() {
    // hyper's own ceiling is 408 KiB and the buffer it grows to serve one
    // oversized head is never given back while the connection lives — so
    // without a lower bound, ten thousand connections that each sent one huge
    // request would pin gigabytes for as long as they stayed open. The default
    // is 64 KiB, which is twice what nginx accepts.
    let app = spawn_echo("app").await;
    let proxy = TestProxy::start(single_route("app.example.com", "/", &[app])).await;

    let mut stream = tokio::net::TcpStream::connect(proxy.http)
        .await
        .expect("connect");
    let mut request = String::from("GET / HTTP/1.1\r\nHost: app.example.com\r\n");
    for i in 0..96 {
        request.push_str(&format!("X-Pad-{i}: {}\r\n", "p".repeat(1024)));
    }
    request.push_str("\r\n");

    let response = write_then_read(&mut stream, request.as_bytes(), 4096).await;
    let head = String::from_utf8_lossy(&response);
    assert!(
        head.starts_with("HTTP/1.1 431"),
        "a 98 KiB header block should be refused, got: {}",
        head.lines().next().unwrap_or("<nothing>")
    );
}
