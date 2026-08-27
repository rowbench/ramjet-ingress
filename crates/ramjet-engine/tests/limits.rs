//! What this engine refuses, and how loudly.
//!
//! The v1 gaps — TLS, HTTP/2, upgrades — are refused with a status and an
//! explanation naming the other engine, never by quietly doing something else.
//! A silent gap is worse than a missing feature, because it looks like a bug in
//! whatever is on the other end.

mod common;

use common::{echo, get_with, table_for, Client, Proxy};

#[test]
fn an_upgrade_request_is_refused_with_a_pointer_to_the_other_engine() {
    let upstream = echo();
    let proxy = Proxy::start(table_for("app.example.com", &[upstream.addr]));

    let response = get_with(
        proxy.addr,
        "/ws",
        "app.example.com",
        &[
            ("Connection", "Upgrade"),
            ("Upgrade", "websocket"),
            ("Sec-WebSocket-Version", "13"),
            ("Sec-WebSocket-Key", "dGhlIHNhbXBsZSBub25jZQ=="),
        ],
    );

    assert_eq!(response.status, 502);
    assert!(
        response.text().contains("--engine hyper"),
        "the refusal must say where upgrades do work: {}",
        response.text()
    );
    assert_eq!(upstream.seen.requests(), 0, "nothing was forwarded");
}

#[test]
fn a_bare_upgrade_header_is_not_an_upgrade() {
    // RFC 9110 needs `Connection: upgrade` beside it. A stray `Upgrade` header
    // is a hint a server may ignore, and refusing it would break ordinary
    // traffic.
    let upstream = echo();
    let proxy = Proxy::start(table_for("app.example.com", &[upstream.addr]));

    let response = get_with(
        proxy.addr,
        "/",
        "app.example.com",
        &[("Upgrade", "websocket")],
    );

    assert_eq!(response.status, 200);
    // And it is still hop-by-hop, so it does not cross.
    assert_eq!(response.header("echo-upgrade"), None);
}

#[test]
fn an_http_2_preface_is_refused_by_name() {
    let upstream = echo();
    let proxy = Proxy::start(table_for("app.example.com", &[upstream.addr]));
    let mut client = Client::connect(proxy.addr);

    // h2c with prior knowledge: what an HTTP/2 client sends before anything
    // else. Parsed as HTTP/1.1 it is a malformed request; named, it is a
    // missing feature.
    let response = client.send(b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n");

    assert_eq!(response.status, 502);
    assert!(
        response.text().contains("HTTP/1.1 only"),
        "{}",
        response.text()
    );
}

#[test]
fn an_http_2_version_in_the_request_line_is_a_501() {
    let upstream = echo();
    let proxy = Proxy::start(table_for("app.example.com", &[upstream.addr]));
    let mut client = Client::connect(proxy.addr);

    let response = client.send(b"GET / HTTP/2.0\r\nHost: app.example.com\r\n\r\n");

    assert_eq!(response.status, 501);
    assert!(response.closing);
}

#[test]
fn a_malformed_request_is_a_400_and_the_connection_ends() {
    let upstream = echo();
    let proxy = Proxy::start(table_for("app.example.com", &[upstream.addr]));

    for bad in [
        &b"GET\r\nHost: app.example.com\r\n\r\n"[..],
        b"GET / HTTP/1.1\r\nHost app.example.com\r\n\r\n",
        b"GET / HTTP/1.1\r\nHost: a\r\nHost: b\r\n\r\n",
        // A `Content-Length` and a `Transfer-Encoding` together: the classic
        // request-smuggling pair, and a proxy is the worst place to guess.
        b"POST / HTTP/1.1\r\nHost: a\r\nContent-Length: 5\r\nTransfer-Encoding: chunked\r\n\r\nhello",
    ] {
        let mut client = Client::connect(proxy.addr);
        let response = client.send(bad);
        assert_eq!(
            response.status,
            400,
            "{:?} should be a 400, got {}",
            String::from_utf8_lossy(bad),
            response.status
        );
        assert!(
            response.closing,
            "a stream whose framing was not understood cannot be resynchronised"
        );
    }
    assert_eq!(upstream.seen.requests(), 0);
}

#[test]
fn a_head_past_the_limit_is_refused() {
    let upstream = echo();
    let proxy = Proxy::start(table_for("app.example.com", &[upstream.addr]));
    let mut client = Client::connect(proxy.addr);

    let mut request = b"GET / HTTP/1.1\r\nHost: app.example.com\r\n".to_vec();
    // One enormous header field, past the 16 KiB head limit.
    request.extend_from_slice(b"X-Huge: ");
    request.extend(std::iter::repeat_n(b'v', 20 * 1024));
    request.extend_from_slice(b"\r\n\r\n");

    let response = client.send(&request);

    assert_eq!(response.status, 413);
    assert!(response.closing);
}

#[test]
fn too_many_header_fields_are_refused() {
    let upstream = echo();
    let proxy = Proxy::start(table_for("app.example.com", &[upstream.addr]));
    let mut client = Client::connect(proxy.addr);

    let mut request = b"GET / HTTP/1.1\r\nHost: app.example.com\r\n".to_vec();
    for i in 0..80 {
        request.extend_from_slice(format!("X-{i}: v\r\n").as_bytes());
    }
    request.extend_from_slice(b"\r\n");

    let response = client.send(&request);

    assert_eq!(response.status, 431);
}

#[test]
fn a_request_that_ends_early_is_a_400() {
    let upstream = echo();
    let proxy = Proxy::start(table_for("app.example.com", &[upstream.addr]));
    let mut client = Client::connect(proxy.addr);

    // Promises five bytes, sends two, then goes away.
    client.write(b"POST / HTTP/1.1\r\nHost: app.example.com\r\nContent-Length: 5\r\n\r\nhi");
    client.shutdown_write();

    let response = client.read_response();
    assert_eq!(response.status, 400);
    assert!(response.text().contains("ended early"), "{}", response.text());
}

#[test]
fn the_engine_keeps_serving_after_every_refusal() {
    // A refusal must not be a way to take a core down. Each of these is
    // answered and then the next ordinary request still works.
    let upstream = echo();
    let proxy = Proxy::start(table_for("app.example.com", &[upstream.addr]));

    for bad in [
        &b"GET\r\n\r\n"[..],
        b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n",
        b"GET / HTTP/9.9\r\nHost: a\r\n\r\n",
        b"\x16\x03\x01\x00\xa5\x01\x00\x00\xa1\x03\x03",
    ] {
        let mut client = Client::connect(proxy.addr);
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| client.send(bad)));
    }

    assert_eq!(
        common::get(proxy.addr, "/", "app.example.com").status,
        200,
        "the engine stopped serving after a refusal"
    );
}
