//! The PROXY protocol on the uring lane's accept paths.
//!
//! The parser itself is `ramjet_proxy::proxy_protocol` and is unit-tested
//! there, over every version, family and truncation. What is tested here is the
//! plumbing, which is where this lane can get it wrong independently:
//!
//! - the address in the header becomes the address `X-Forwarded-For` reports;
//! - the bytes *after* the header are handed on exactly, including when they
//!   are a TLS ClientHello rather than an HTTP request;
//! - a connection with no valid header is dropped, not served;
//! - a sender that never finishes the header does not hold a descriptor.

mod common;

use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::Arc;
use std::time::{Duration, Instant};

use common::*;

/// The v2 signature, which is deliberately unprintable.
const V2_SIGNATURE: [u8; 12] = [
    0x0D, 0x0A, 0x0D, 0x0A, 0x00, 0x0D, 0x0A, 0x51, 0x55, 0x49, 0x54, 0x0A,
];

/// A v1 line naming `client` as the source.
fn v1(client: &str) -> Vec<u8> {
    format!("PROXY TCP4 {client} 10.0.0.1 51234 443\r\n").into_bytes()
}

/// A v2 `PROXY`/`AF_INET`/`STREAM` header naming `client` as the source.
fn v2(client: [u8; 4], port: u16) -> Vec<u8> {
    let mut out = V2_SIGNATURE.to_vec();
    // Version 2, command PROXY.
    out.push(0x21);
    // AF_INET over SOCK_STREAM.
    out.push(0x11);
    let mut body = Vec::new();
    body.extend_from_slice(&client);
    body.extend_from_slice(&[10, 0, 0, 1]);
    body.extend_from_slice(&port.to_be_bytes());
    body.extend_from_slice(&443u16.to_be_bytes());
    out.extend_from_slice(&(body.len() as u16).to_be_bytes());
    out.extend_from_slice(&body);
    out
}

/// A v2 `LOCAL` header, which names nobody. What a balancer's own health check
/// sends.
fn v2_local() -> Vec<u8> {
    let mut out = V2_SIGNATURE.to_vec();
    // Version 2, command LOCAL.
    out.push(0x20);
    out.push(0x00);
    out.extend_from_slice(&0u16.to_be_bytes());
    out
}

fn proxied(table: ramjet_router::RouteTable) -> Proxy {
    Proxy::with_config(table, |config| {
        config.workers = Some(1);
        config.proxy_protocol = Some(Duration::from_millis(400));
    })
}

#[test]
fn a_v1_header_becomes_the_forwarded_client() {
    let upstream = echo();
    let proxy = proxied(table_for("app.example.com", &[upstream.addr]));

    let mut client = Client::connect(proxy.addr);
    let mut request = v1("198.51.100.7");
    request.extend_from_slice(b"GET / HTTP/1.1\r\nHost: app.example.com\r\n\r\n");
    let response = client.send(&request);

    assert_eq!(response.status, 200);
    assert_eq!(
        response.header("echo-x-forwarded-for"),
        Some("198.51.100.7"),
        "the header's client, not the socket's"
    );
    assert_eq!(response.header("echo-x-real-ip"), Some("198.51.100.7"));
}

#[test]
fn a_v2_header_becomes_the_forwarded_client() {
    let upstream = echo();
    let proxy = proxied(table_for("app.example.com", &[upstream.addr]));

    let mut client = Client::connect(proxy.addr);
    let mut request = v2([203, 0, 113, 9], 40000);
    request.extend_from_slice(b"GET / HTTP/1.1\r\nHost: app.example.com\r\n\r\n");
    let response = client.send(&request);

    assert_eq!(response.status, 200);
    assert_eq!(response.header("echo-x-forwarded-for"), Some("203.0.113.9"));
}

#[test]
fn a_local_command_leaves_the_sockets_own_address_standing() {
    // A well-formed header that names nobody is a success, not a failure: it is
    // what a load balancer's own health check sends. The header is consumed and
    // the connection's real peer stands.
    let upstream = echo();
    let proxy = proxied(table_for("app.example.com", &[upstream.addr]));

    let mut client = Client::connect(proxy.addr);
    let mut request = v2_local();
    request.extend_from_slice(b"GET / HTTP/1.1\r\nHost: app.example.com\r\n\r\n");
    let response = client.send(&request);

    assert_eq!(response.status, 200);
    assert_eq!(
        response.header("echo-x-forwarded-for"),
        Some("127.0.0.1"),
        "a LOCAL header names nobody, so the socket's own peer is the client"
    );
}

#[test]
fn the_bytes_after_the_header_are_handed_on_exactly() {
    // The load balancer relays the client's first bytes right behind the
    // header, so one read very often carries both. An off-by-one in the handoff
    // eats the `G` of `GET` and the request becomes unparseable.
    let upstream = echo();
    let proxy = proxied(table_for("app.example.com", &[upstream.addr]));

    let mut client = Client::connect(proxy.addr);
    let mut request = v1("198.51.100.7");
    request.extend_from_slice(
        b"POST /submit HTTP/1.1\r\nHost: app.example.com\r\nContent-Length: 11\r\n\r\nhello world",
    );
    let response = client.send(&request);

    assert_eq!(response.status, 200);
    assert_eq!(response.header("echo-method"), Some("POST"));
    assert_eq!(response.header("echo-target"), Some("/submit"));
    assert_eq!(
        response.header("echo-body-len"),
        Some("11"),
        "the body after the header lost bytes"
    );
}

#[test]
fn a_header_split_across_reads_is_reassembled() {
    let upstream = echo();
    let proxy = proxied(table_for("app.example.com", &[upstream.addr]));

    let mut socket = TcpStream::connect(proxy.addr).expect("a connection");
    socket
        .set_read_timeout(Some(Duration::from_secs(5)))
        .expect("a read timeout");

    // One byte at a time through the header, which is the case a parser that
    // assumes one read gets wrong.
    let header = v1("198.51.100.7");
    for byte in &header {
        socket
            .write_all(std::slice::from_ref(byte))
            .expect("a byte was sent");
        socket.flush().expect("flushed");
        std::thread::sleep(Duration::from_micros(200));
    }
    socket
        .write_all(b"GET / HTTP/1.1\r\nHost: app.example.com\r\nConnection: close\r\n\r\n")
        .expect("the request was sent");

    let mut seen = Vec::new();
    socket.read_to_end(&mut seen).expect("a response");
    let text = String::from_utf8_lossy(&seen);
    assert!(text.starts_with("HTTP/1.1 200 "), "{text}");
    assert!(
        text.contains("echo-x-forwarded-for: 198.51.100.7"),
        "{text}"
    );
}

#[test]
fn a_connection_with_no_header_is_dropped_rather_than_served() {
    // The trust model: the header is required when the feature is on. A
    // permissive fallback to the socket address would let a sender choose, per
    // connection, whether to be spoofed — strictly worse than either fixed
    // answer.
    let upstream = echo();
    let proxy = proxied(table_for("app.example.com", &[upstream.addr]));

    let mut socket = TcpStream::connect(proxy.addr).expect("a connection");
    socket
        .set_read_timeout(Some(Duration::from_secs(5)))
        .expect("a read timeout");
    socket
        .write_all(b"GET / HTTP/1.1\r\nHost: app.example.com\r\n\r\n")
        .expect("the request was sent");

    let mut seen = Vec::new();
    let _ = socket.read_to_end(&mut seen);
    assert!(
        seen.is_empty(),
        "a spoofable connection must get no answer at all, got {:?}",
        String::from_utf8_lossy(&seen)
    );
    assert_eq!(upstream.seen.requests(), 0, "nothing was forwarded");
}

#[test]
fn a_header_that_never_finishes_does_not_hold_the_descriptor() {
    // A sender that opens a connection and dribbles is otherwise free to hold a
    // descriptor and a slot in the connection gauge for as long as it likes.
    let upstream = echo();
    let proxy = proxied(table_for("app.example.com", &[upstream.addr]));

    let mut socket = TcpStream::connect(proxy.addr).expect("a connection");
    socket
        .set_read_timeout(Some(Duration::from_secs(10)))
        .expect("a read timeout");
    // A valid prefix that will never become a whole line.
    socket.write_all(b"PROXY TCP4 198.51").expect("bytes sent");
    socket.flush().expect("flushed");

    let started = Instant::now();
    let mut seen = Vec::new();
    let _ = socket.read_to_end(&mut seen);
    assert!(
        seen.is_empty(),
        "an unfinished header must not be answered: {:?}",
        String::from_utf8_lossy(&seen)
    );
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "the connection was held for {:?}",
        started.elapsed()
    );
}

#[test]
fn the_listener_keeps_serving_after_a_refusal() {
    // One spoofed connection must not take the listener down with it.
    let upstream = echo();
    let proxy = proxied(table_for("app.example.com", &[upstream.addr]));

    let mut bad = TcpStream::connect(proxy.addr).expect("a connection");
    bad.write_all(b"not a proxy header at all\r\n\r\n")
        .expect("bytes sent");
    drop(bad);

    let mut client = Client::connect(proxy.addr);
    let mut request = v1("198.51.100.7");
    request.extend_from_slice(b"GET / HTTP/1.1\r\nHost: app.example.com\r\n\r\n");
    assert_eq!(client.send(&request).status, 200);
}

#[test]
fn a_header_in_front_of_a_client_hello_is_handed_to_tls_intact() {
    // The hardest handoff, and the one an HTTP-shaped test would never reach:
    // the bytes after the header are a TLS record, and a single lost or
    // duplicated byte fails the handshake rather than the request.
    let upstream = echo();
    let (table, certs) = tls_table("app.example.com", &[upstream.addr], &["app.example.com"]);
    let routes = Arc::new(ramjet_router::SharedRouteTable::new(table));
    let proxy = Proxy::with_routes(routes, move |config, routes| {
        let resolver = Arc::new(ramjet_proxy::SniResolver::new(Arc::clone(routes), certs));
        config.https = Some(std::net::SocketAddr::from(([127, 0, 0, 1], 0)));
        config.tls = Some(Arc::new(
            ramjet_proxy::tls::h1_server_config(resolver).expect("a server config"),
        ));
        config.proxy_protocol = Some(Duration::from_secs(2));
    });

    // The header goes on the raw socket, and rustls is handed the same socket
    // afterwards — which is exactly the order a load balancer produces.
    let socket = TcpStream::connect(proxy.tls()).expect("a connection");
    socket
        .set_read_timeout(Some(Duration::from_secs(10)))
        .expect("a read timeout");
    let mut socket = socket;
    socket.write_all(&v1("198.51.100.7")).expect("header sent");
    socket.flush().expect("flushed");

    let name = rustls_pki_types::ServerName::try_from("app.example.com".to_owned())
        .expect("a valid name");
    let conn = rustls::ClientConnection::new(tls_client_config(), name).expect("a session");
    let mut client = Client::new(rustls::StreamOwned::new(conn, socket));

    let response = client.send(b"GET / HTTP/1.1\r\nHost: app.example.com\r\n\r\n");
    assert_eq!(response.status, 200);
    assert_eq!(
        response.header("echo-x-forwarded-for"),
        Some("198.51.100.7"),
        "the header's client did not survive the TLS handshake"
    );
    assert_eq!(response.header("echo-x-forwarded-proto"), Some("https"));
}

#[test]
fn a_listener_without_the_flag_reads_no_header() {
    // The flag is per process here, as it is on the hyper engine. Without it, a
    // PROXY line is just the first bytes of a request that is not HTTP.
    let upstream = echo();
    let proxy = Proxy::start(table_for("app.example.com", &[upstream.addr]));

    let mut socket = TcpStream::connect(proxy.addr).expect("a connection");
    socket
        .set_read_timeout(Some(Duration::from_secs(5)))
        .expect("a read timeout");
    let mut request = v1("198.51.100.7");
    request.extend_from_slice(b"GET / HTTP/1.1\r\nHost: app.example.com\r\n\r\n");
    socket.write_all(&request).expect("bytes sent");

    let mut seen = Vec::new();
    let _ = socket.read_to_end(&mut seen);
    let text = String::from_utf8_lossy(&seen);
    assert!(
        text.starts_with("HTTP/1.1 400 ") || text.starts_with("HTTP/1.1 501 "),
        "a PROXY line without the flag is a malformed request, got: {text}"
    );
    assert_eq!(upstream.seen.requests(), 0);
}
