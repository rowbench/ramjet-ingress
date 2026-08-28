//! The PROXY protocol on real sockets: does the client address actually arrive.
//!
//! The unit tests in `src/proxy_protocol.rs` cover the parser's state space.
//! What they cannot cover is the thing that makes the feature work or not:
//! whether the address the header names reaches `X-Forwarded-For`, whether the
//! header is read *before* the TLS handshake rather than after it, and whether
//! the bytes that arrived in the same packet as the header survive the trip.
//! All of that needs a socket, so all of it is here.
//!
//! Every header in this file is written by the test, which is precisely the
//! point of the trust model: a client that can reach the listener can claim any
//! address it likes. These tests are that attack, run deliberately.

mod common;

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use common::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// A v1 header naming `client` as the source.
fn v1(client: &str, port: u16) -> Vec<u8> {
    format!("PROXY TCP4 {client} 10.0.0.1 {port} 443\r\n").into_bytes()
}

/// A v2 header naming an IPv4 source, with `command` in the low nibble.
fn v2(command: u8, client: [u8; 4], port: u16) -> Vec<u8> {
    let mut header = Vec::from([
        0x0D, 0x0A, 0x0D, 0x0A, 0x00, 0x0D, 0x0A, 0x51, 0x55, 0x49, 0x54, 0x0A,
    ]);
    header.push(0x20 | command);
    header.push(0x11); // AF_INET over STREAM
    header.extend_from_slice(&12u16.to_be_bytes());
    header.extend_from_slice(&client);
    header.extend_from_slice(&[10, 0, 0, 1]);
    header.extend_from_slice(&port.to_be_bytes());
    header.extend_from_slice(&443u16.to_be_bytes());
    header
}

/// A minimal request that asks the proxy to close when it has answered, so a
/// read to EOF terminates instead of waiting out the keep-alive.
fn request(host: &str) -> Vec<u8> {
    format!("GET / HTTP/1.1\r\nHost: {host}\r\nConnection: close\r\n\r\n").into_bytes()
}

/// Writes `bytes` to a fresh connection and reads the answer to EOF.
async fn exchange(addr: SocketAddr, bytes: &[u8]) -> String {
    let mut stream = TcpStream::connect(addr).await.expect("connect to the proxy");
    let response = write_then_read(&mut stream, bytes, 64 * 1024).await;
    String::from_utf8_lossy(&response).into_owned()
}

/// A proxy requiring the header, in front of an echoing upstream.
async fn proxied(timeout: Duration) -> (TestProxy, &'static str) {
    let app = spawn_echo("app").await;
    let proxy = TestProxy::start_with(
        single_route("app.example.com", "/", &[app]),
        ProxyOptions {
            proxy_protocol: Some(timeout),
            ..Default::default()
        },
    )
    .await;
    (proxy, "app.example.com")
}

// ---------------------------------------------------------------------------
// The address arrives
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_v1_header_supplies_the_client_address_for_forwarded_for() {
    let (proxy, host) = proxied(Duration::from_secs(5)).await;

    let mut bytes = v1("203.0.113.9", 51234);
    bytes.extend_from_slice(&request(host));
    let response = exchange(proxy.http, &bytes).await;

    assert!(response.starts_with("HTTP/1.1 200"), "{response}");
    // The upstream mirrors every request header back as `echo-<name>`, so this
    // is what the *backend* saw, not what the proxy intended to send.
    assert!(
        response.to_lowercase().contains("echo-x-forwarded-for: 203.0.113.9"),
        "the header's client should reach the backend, got:\n{response}"
    );
    assert!(
        response.to_lowercase().contains("echo-x-real-ip: 203.0.113.9"),
        "X-Real-IP should agree with X-Forwarded-For, got:\n{response}"
    );
    // The proof that it came from the header and not the socket: the connection
    // really was made from loopback.
    assert!(
        !response.to_lowercase().contains("echo-x-real-ip: 127.0.0.1"),
        "the socket peer must not be what the backend sees"
    );
}

#[tokio::test]
async fn a_v2_header_supplies_the_client_address_too() {
    let (proxy, host) = proxied(Duration::from_secs(5)).await;

    let mut bytes = v2(0x1, [198, 51, 100, 22], 40000);
    bytes.extend_from_slice(&request(host));
    let response = exchange(proxy.http, &bytes).await;

    assert!(response.starts_with("HTTP/1.1 200"), "{response}");
    assert!(
        response.to_lowercase().contains("echo-x-forwarded-for: 198.51.100.22"),
        "got:\n{response}"
    );
}

#[tokio::test]
async fn a_v2_local_command_leaves_the_socket_peer_in_place() {
    // What a load balancer's own health check sends. It is a valid header, so
    // the connection is served; it names nobody, so the peer address stands.
    let (proxy, host) = proxied(Duration::from_secs(5)).await;

    let mut bytes = v2(0x0, [198, 51, 100, 22], 40000);
    bytes.extend_from_slice(&request(host));
    let response = exchange(proxy.http, &bytes).await;

    assert!(response.starts_with("HTTP/1.1 200"), "{response}");
    assert!(
        response.to_lowercase().contains("echo-x-forwarded-for: 127.0.0.1"),
        "a LOCAL header must not take the address it happens to carry, got:\n{response}"
    );
}

#[tokio::test]
async fn an_existing_forwarded_for_is_appended_to_rather_than_replaced() {
    // Two proxies in front of this one: the header chain has to keep growing,
    // or the original client is lost at the second hop.
    let (proxy, host) = proxied(Duration::from_secs(5)).await;

    let mut bytes = v1("203.0.113.9", 51234);
    bytes.extend_from_slice(
        format!(
            "GET / HTTP/1.1\r\nHost: {host}\r\nX-Forwarded-For: 192.0.2.1\r\n\
             Connection: close\r\n\r\n"
        )
        .as_bytes(),
    );
    let response = exchange(proxy.http, &bytes).await;

    assert!(
        response
            .to_lowercase()
            .contains("echo-x-forwarded-for: 192.0.2.1, 203.0.113.9"),
        "got:\n{response}"
    );
}

// ---------------------------------------------------------------------------
// Framing
// ---------------------------------------------------------------------------

#[tokio::test]
async fn requests_pipelined_behind_the_header_are_not_lost() {
    // The header and the first requests arrive in one segment, which is the
    // normal case: the parser has to hand back everything it read past the
    // header, byte for byte. Dropping it would lose the first request.
    let (proxy, host) = proxied(Duration::from_secs(5)).await;

    let mut bytes = v1("203.0.113.9", 51234);
    bytes.extend_from_slice(format!("GET /one HTTP/1.1\r\nHost: {host}\r\n\r\n").as_bytes());
    bytes.extend_from_slice(
        format!("GET /two HTTP/1.1\r\nHost: {host}\r\nConnection: close\r\n\r\n").as_bytes(),
    );
    let response = exchange(proxy.http, &bytes).await;

    assert_eq!(
        response.matches("HTTP/1.1 200").count(),
        2,
        "both pipelined requests should be answered, got:\n{response}"
    );
    assert!(response.contains("GET /one"), "got:\n{response}");
    assert!(response.contains("GET /two"), "got:\n{response}");
}

#[tokio::test]
async fn a_header_split_across_packets_is_reassembled() {
    // A load balancer is free to flush mid-header, and the reader has to keep
    // its place across reads rather than starting over or giving up.
    let (proxy, host) = proxied(Duration::from_secs(5)).await;

    let mut stream = TcpStream::connect(proxy.http).await.expect("connect");
    let header = v1("203.0.113.9", 51234);
    let (head, tail) = header.split_at(9);
    stream.write_all(head).await.expect("write");
    stream.flush().await.expect("flush");
    tokio::time::sleep(Duration::from_millis(20)).await;
    stream.write_all(tail).await.expect("write");
    stream.write_all(&request(host)).await.expect("write");

    let mut response = String::new();
    stream
        .read_to_string(&mut response)
        .await
        .expect("a response");
    assert!(response.starts_with("HTTP/1.1 200"), "{response}");
    assert!(
        response.to_lowercase().contains("echo-x-forwarded-for: 203.0.113.9"),
        "got:\n{response}"
    );
}

// ---------------------------------------------------------------------------
// Refusals
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_request_with_no_header_is_dropped() {
    // The whole point of requiring it. A permissive fallback would let a client
    // that can reach the listener choose whether to be spoofed, which is worse
    // than either fixed answer.
    let (proxy, host) = proxied(Duration::from_secs(5)).await;

    let response = exchange(proxy.http, &request(host)).await;
    assert!(
        response.is_empty(),
        "the connection should be closed with no reply, got:\n{response}"
    );
}

#[tokio::test]
async fn a_garbage_header_is_dropped() {
    let (proxy, host) = proxied(Duration::from_secs(5)).await;

    let mut bytes = Vec::from(*b"PROXY TCP4 not-an-address 10.0.0.1 1 2\r\n");
    bytes.extend_from_slice(&request(host));
    let response = exchange(proxy.http, &bytes).await;

    assert!(response.is_empty(), "got:\n{response}");
}

#[tokio::test]
async fn a_stalled_sender_is_dropped_at_the_timeout() {
    // A valid prefix and then silence. Without the deadline this holds a task,
    // a socket, and a slot in the connection gauge for as long as the sender
    // cares to keep the connection open.
    let (proxy, _host) = proxied(Duration::from_millis(200)).await;

    let mut stream = TcpStream::connect(proxy.http).await.expect("connect");
    stream.write_all(b"PROXY TCP4 203.0").await.expect("write");

    let mut response = String::new();
    let closed = tokio::time::timeout(
        Duration::from_secs(5),
        stream.read_to_string(&mut response),
    )
    .await;

    assert!(
        closed.is_ok(),
        "the proxy should have closed the connection at its deadline"
    );
    assert!(response.is_empty(), "got:\n{response}");
}

#[tokio::test]
async fn without_the_flag_a_header_is_not_consumed() {
    // The other direction of the same contract: a proxy that is not expecting
    // the protocol must not quietly accept it, because then anybody could send
    // one. hyper sees `PROXY ...` as a request line and rejects it.
    let app = spawn_echo("app").await;
    let proxy = TestProxy::start(single_route("app.example.com", "/", &[app])).await;

    let mut bytes = v1("203.0.113.9", 51234);
    bytes.extend_from_slice(&request("app.example.com"));
    let response = exchange(proxy.http, &bytes).await;

    assert!(
        !response.contains("203.0.113.9"),
        "an unexpected header must never set the client address, got:\n{response}"
    );
    assert!(
        response.is_empty() || response.starts_with("HTTP/1.1 4"),
        "expected a refusal, got:\n{response}"
    );
}

// ---------------------------------------------------------------------------
// The other listeners
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tls_reads_the_header_before_the_client_hello() {
    // The ordering that makes this work at all: the load balancer speaks the
    // PROXY protocol itself and then relays the client's bytes untouched, so on
    // an HTTPS listener the header arrives *ahead* of the ClientHello. Parsing
    // after the handshake would mean feeding the header to rustls, which would
    // fail every connection.
    let app = spawn_echo("app").await;
    let cert = TestCert::generate(&["app.example.com"]);

    let mut builder = ramjet_router::RouteTableBuilder::new();
    builder
        .backend(
            "app",
            ramjet_router::LbPolicy::RoundRobin,
            vec![ramjet_router::Endpoint::new(app)],
        )
        .expect("backend");
    builder
        .route(
            Some("app.example.com"),
            "/",
            ramjet_router::PathType::Prefix,
            "app",
        )
        .expect("route");
    builder
        .certificate(
            "app.example.com",
            Arc::new(ramjet_router::CertifiedKeyHandle::new(1)),
        )
        .expect("certificate");

    let proxy = TestProxy::start_with(
        builder.build().expect("table"),
        ProxyOptions {
            tls: true,
            certs: cert_store(&[(1, &cert)]),
            proxy_protocol: Some(Duration::from_secs(5)),
            ..Default::default()
        },
    )
    .await;
    let https = proxy.https.expect("a tls port");

    // The header goes on the raw socket, before anything TLS-shaped.
    let mut stream = TcpStream::connect(https).await.expect("connect");
    stream
        .write_all(&v1("203.0.113.9", 51234))
        .await
        .expect("write the header");

    let name = rustls::pki_types::ServerName::try_from("app.example.com".to_owned())
        .expect("a valid name");
    let stream = tokio_rustls::TlsConnector::from(tls_client_config(&[&cert], &[b"http/1.1"]))
        .connect(name, stream)
        .await
        .expect("the handshake should succeed on top of the PROXY header");

    let (mut sender, connection) =
        hyper::client::conn::http1::handshake(hyper_util::rt::TokioIo::new(stream))
            .await
            .expect("client handshake");
    let driver = tokio::spawn(async move {
        let _ = connection.await;
    });
    let response = sender
        .send_request(
            common::request("app.example.com", "/")
                .body(empty_body())
                .expect("a request"),
        )
        .await
        .expect("a response");
    let reply = collect(response).await;
    driver.abort();

    assert_eq!(reply.status, http::StatusCode::OK);
    assert_eq!(
        reply.header("echo-x-forwarded-for"),
        Some("203.0.113.9"),
        "the header's client should survive TLS termination"
    );
    assert_eq!(
        reply.header("echo-x-forwarded-proto"),
        Some("https"),
        "and the scheme should still be the listener's"
    );
}

#[tokio::test]
async fn the_admin_listener_ignores_the_flag() {
    // Prometheus and the kubelet reach the pod directly and speak no PROXY
    // protocol. Requiring it on the admin port would take metrics and both
    // probes offline the moment the flag was set — a readiness probe failing on
    // a healthy pod is a rolling update that never completes.
    let (proxy, _host) = proxied(Duration::from_secs(5)).await;

    for path in ["/healthz", "/readyz", "/metrics"] {
        let response = exchange(
            proxy.admin,
            format!("GET {path} HTTP/1.1\r\nHost: admin\r\nConnection: close\r\n\r\n").as_bytes(),
        )
        .await;
        assert!(
            response.starts_with("HTTP/1.1 200") || response.starts_with("HTTP/1.1 503"),
            "{path} should answer without a PROXY header, got:\n{response}"
        );
    }
}

#[tokio::test]
async fn a_rejected_connection_does_not_disturb_the_next_one() {
    // A dropped connection has to stay one dropped connection. If a rejection
    // left the lane wedged, a single port scanner would take the listener down.
    let (proxy, host) = proxied(Duration::from_secs(5)).await;

    for _ in 0..3 {
        assert!(exchange(proxy.http, &request(host)).await.is_empty());
        assert!(exchange(proxy.http, b"\x16\x03\x01 not tls either").await.is_empty());
    }

    let mut bytes = v1("203.0.113.9", 51234);
    bytes.extend_from_slice(&request(host));
    let response = exchange(proxy.http, &bytes).await;
    assert!(response.starts_with("HTTP/1.1 200"), "{response}");
    assert!(
        response.to_lowercase().contains("echo-x-forwarded-for: 203.0.113.9"),
        "got:\n{response}"
    );
}
