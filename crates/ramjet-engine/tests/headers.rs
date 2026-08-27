//! What the hop does to headers, checked at the far end.
//!
//! Every assertion here has a twin in the hyper engine's `headers.rs`. The two
//! data planes are meant to be indistinguishable to a client and to an origin,
//! and this is the file that would notice if they stopped being.

mod common;

use common::{echo, get, get_with, spawn, table_for, Behaviour, Client, Proxy};

#[test]
fn forwarded_headers_describe_this_hop() {
    let upstream = echo();
    let proxy = Proxy::start(table_for("app.example.com", &[upstream.addr]));

    let response = get(proxy.addr, "/", "app.example.com");

    assert_eq!(response.header("echo-x-forwarded-for"), Some("127.0.0.1"));
    assert_eq!(response.header("echo-x-real-ip"), Some("127.0.0.1"));
    assert_eq!(response.header("echo-x-forwarded-proto"), Some("http"));
    assert_eq!(
        response.header("echo-x-forwarded-host"),
        Some("app.example.com")
    );
}

#[test]
fn forwarded_for_is_appended_to_an_existing_trail() {
    let upstream = echo();
    let proxy = Proxy::start(table_for("app.example.com", &[upstream.addr]));

    let response = get_with(
        proxy.addr,
        "/",
        "app.example.com",
        &[("X-Forwarded-For", "203.0.113.9")],
    );

    assert_eq!(
        response.header("echo-x-forwarded-for"),
        Some("203.0.113.9, 127.0.0.1"),
        "the client must not be erased from the trail"
    );
}

#[test]
fn several_forwarded_for_lines_become_one() {
    let upstream = echo();
    let proxy = Proxy::start(table_for("app.example.com", &[upstream.addr]));

    let response = get_with(
        proxy.addr,
        "/",
        "app.example.com",
        &[
            ("X-Forwarded-For", "203.0.113.7"),
            ("X-Forwarded-For", "10.1.1.1"),
        ],
    );

    assert_eq!(
        response.header("echo-x-forwarded-for"),
        Some("203.0.113.7, 10.1.1.1, 127.0.0.1")
    );
}

#[test]
fn a_request_id_is_generated_when_absent_and_reused_when_present() {
    let upstream = echo();
    let proxy = Proxy::start(table_for("app.example.com", &[upstream.addr]));

    let generated = get(proxy.addr, "/", "app.example.com");
    let id = generated.header("echo-x-request-id").expect("an id");
    assert_eq!(id.len(), 32, "{id}");
    assert!(
        id.bytes()
            .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase()),
        "{id} should be 32 lowercase hex characters"
    );

    let preserved = get_with(
        proxy.addr,
        "/",
        "app.example.com",
        &[("X-Request-Id", "trace-from-the-edge")],
    );
    assert_eq!(
        preserved.header("echo-x-request-id"),
        Some("trace-from-the-edge"),
        "a trace must survive the hop"
    );
}

#[test]
fn two_requests_get_different_ids() {
    let upstream = echo();
    let proxy = Proxy::start(table_for("app.example.com", &[upstream.addr]));

    let first = get(proxy.addr, "/", "app.example.com");
    let second = get(proxy.addr, "/", "app.example.com");

    assert_ne!(
        first.header("echo-x-request-id"),
        second.header("echo-x-request-id")
    );
}

#[test]
fn hop_by_hop_request_headers_do_not_reach_the_upstream() {
    let upstream = echo();
    let proxy = Proxy::start(table_for("app.example.com", &[upstream.addr]));

    let response = get_with(
        proxy.addr,
        "/",
        "app.example.com",
        &[
            ("Connection", "keep-alive, x-hop-secret"),
            ("X-Hop-Secret", "shh"),
            ("Keep-Alive", "timeout=5"),
            ("Proxy-Connection", "keep-alive"),
            ("X-End-To-End", "kept"),
        ],
    );

    for gone in [
        "echo-x-hop-secret",
        "echo-keep-alive",
        "echo-proxy-connection",
        "echo-connection",
    ] {
        assert_eq!(response.header(gone), None, "{gone} crossed the hop");
    }
    assert_eq!(response.header("echo-x-end-to-end"), Some("kept"));
}

#[test]
fn hop_by_hop_response_headers_do_not_reach_the_client() {
    let upstream = spawn(Behaviour::Raw(
        b"HTTP/1.1 200 OK\r\n\
          Connection: keep-alive, x-hop\r\n\
          X-Hop: shh\r\n\
          Keep-Alive: timeout=5\r\n\
          X-Kept: yes\r\n\
          Content-Length: 2\r\n\r\nhi"
            .to_vec(),
    ));
    let proxy = Proxy::start(table_for("app.example.com", &[upstream.addr]));

    let response = get(proxy.addr, "/", "app.example.com");

    assert_eq!(response.status, 200);
    assert_eq!(response.body, b"hi");
    for gone in ["x-hop", "keep-alive"] {
        assert_eq!(response.header(gone), None, "{gone} reached the client");
    }
    assert_eq!(response.header("x-kept"), Some("yes"));
}

#[test]
fn a_grpc_request_is_refused_with_an_explanation() {
    let upstream = echo();
    let proxy = Proxy::start(table_for("app.example.com", &[upstream.addr]));

    let mut client = Client::connect(proxy.addr);
    let response = client.send(
        b"POST /svc/Method HTTP/1.1\r\nHost: app.example.com\r\n\
          Content-Type: application/grpc\r\nContent-Length: 0\r\n\r\n",
    );

    assert_eq!(response.status, 502);
    assert!(response.text().contains("HTTP/2"), "{}", response.text());
}

#[test]
fn an_unrouted_grpc_request_is_a_404_not_a_502() {
    // The order matters: routing first, then the gRPC refusal, so a gRPC
    // request to a host nobody serves is reported as an unknown host.
    let upstream = echo();
    let proxy = Proxy::start(table_for("app.example.com", &[upstream.addr]));

    let mut client = Client::connect(proxy.addr);
    let response = client.send(
        b"POST /svc HTTP/1.1\r\nHost: nobody.invalid\r\n\
          Content-Type: application/grpc\r\nContent-Length: 0\r\n\r\n",
    );

    assert_eq!(response.status, 404);
}

#[test]
fn a_header_value_that_is_not_utf8_is_forwarded_rather_than_rejected() {
    // obs-text is legal, and a proxy that 400s a Latin-1 filename breaks
    // traffic it was only asked to carry.
    let upstream = echo();
    let proxy = Proxy::start(table_for("app.example.com", &[upstream.addr]));
    let mut client = Client::connect(proxy.addr);

    let mut request =
        b"GET / HTTP/1.1\r\nHost: app.example.com\r\nX-Name: caf".to_vec();
    request.push(0xe9);
    request.extend_from_slice(b"\r\n\r\n");
    let response = client.send(&request);

    assert_eq!(response.status, 200, "a Latin-1 value must not be a 400");
}

#[test]
fn the_upstream_request_is_always_http_11() {
    let upstream = echo();
    let proxy = Proxy::start(table_for("app.example.com", &[upstream.addr]));
    let mut client = Client::connect(proxy.addr);

    // An HTTP/1.0 client still reaches a keep-alive upstream pool.
    let response = client.send(b"GET / HTTP/1.0\r\nHost: app.example.com\r\n\r\n");

    assert_eq!(response.status, 200);
    assert_eq!(response.header("echo-version"), Some("HTTP/1.1"));
}

#[test]
fn an_http_10_client_gets_its_connection_closed() {
    let upstream = echo();
    let proxy = Proxy::start(table_for("app.example.com", &[upstream.addr]));
    let mut client = Client::connect(proxy.addr);

    let response = client.send(b"GET / HTTP/1.0\r\nHost: app.example.com\r\n\r\n");

    assert_eq!(response.status, 200);
    assert!(
        response.closing,
        "HTTP/1.0 does not persist unless it says so"
    );
}

#[test]
fn an_http_10_client_asking_to_keep_alive_is_obliged() {
    let upstream = echo();
    let proxy = Proxy::start(table_for("app.example.com", &[upstream.addr]));
    let mut client = Client::connect(proxy.addr);

    let first = client.send(
        b"GET /one HTTP/1.0\r\nHost: app.example.com\r\nConnection: keep-alive\r\n\r\n",
    );
    assert_eq!(first.status, 200);
    assert!(!first.closing);

    let second = client.send(
        b"GET /two HTTP/1.0\r\nHost: app.example.com\r\nConnection: keep-alive\r\n\r\n",
    );
    assert_eq!(second.header("echo-target"), Some("/two"));
}

#[test]
fn a_response_reason_phrase_is_preserved() {
    let upstream = spawn(Behaviour::Raw(
        b"HTTP/1.1 418 I am a teapot\r\nContent-Length: 0\r\n\r\n".to_vec(),
    ));
    let proxy = Proxy::start(table_for("app.example.com", &[upstream.addr]));

    let response = get(proxy.addr, "/", "app.example.com");

    assert_eq!(response.status, 418);
    assert_eq!(response.reason, "I am a teapot");
}

#[test]
fn a_missing_reason_phrase_is_filled_in() {
    let upstream = spawn(Behaviour::Raw(
        b"HTTP/1.1 404\r\nContent-Length: 0\r\n\r\n".to_vec(),
    ));
    let proxy = Proxy::start(table_for("app.example.com", &[upstream.addr]));

    let response = get(proxy.addr, "/", "app.example.com");

    assert_eq!(response.status, 404);
    assert_eq!(response.reason, "Not Found");
}

#[test]
fn a_head_request_gets_headers_and_no_body() {
    let upstream = echo();
    let proxy = Proxy::start(table_for("app.example.com", &[upstream.addr]));
    let mut client = Client::connect(proxy.addr);

    let response = client.send(b"HEAD / HTTP/1.1\r\nHost: app.example.com\r\n\r\n");

    assert_eq!(response.status, 200);
    assert_eq!(response.header("content-length"), Some("128"));
    assert!(
        response.body.is_empty(),
        "a HEAD response carries the length but not the bytes"
    );
    // And the connection is still usable, which is what proves the proxy did
    // not sit waiting for a body that was never coming.
    let next = client.send(b"GET /after HTTP/1.1\r\nHost: app.example.com\r\n\r\n");
    assert_eq!(next.header("echo-target"), Some("/after"));
}

#[test]
fn a_proxy_generated_error_to_head_has_no_body_either() {
    let upstream = echo();
    let proxy = Proxy::start(table_for("app.example.com", &[upstream.addr]));
    let mut client = Client::connect(proxy.addr);

    let response = client.send(b"HEAD / HTTP/1.1\r\nHost: nobody.invalid\r\n\r\n");

    assert_eq!(response.status, 404);
    assert!(response.body.is_empty());
    assert!(
        response
            .header("content-length")
            .is_some_and(|v| v != "0"),
        "the length a GET would have had"
    );
}
