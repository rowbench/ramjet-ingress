//! What this engine refuses, and how loudly.
//!
//! The gap that is left — HTTP/2 in any form — is refused with a status and an
//! explanation naming the other engine, never by quietly doing something else.
//! A silent gap is worse than a missing feature, because it looks like a bug in
//! whatever is on the other end.
//!
//! Upgrades and TLS used to be listed here. They are carried now, and the tests
//! that pinned their refusals were replaced with the ones in `websocket.rs` and
//! `tls.rs` rather than deleted: a closed gap needs a test saying it is closed,
//! or the next person to read `limits.rs` has no way to tell.

mod common;

use common::{echo, get, get_with, h2c_table_for, table_for, Client, Proxy};

#[test]
fn an_upgrade_request_is_forwarded_rather_than_refused() {
    // The gap that closed. An upgrade reaches the backend, and what the backend
    // says about it is what the client gets — here a plain 200, because the
    // echo upstream does not speak WebSocket. `websocket.rs` covers the 101.
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

    assert_eq!(response.status, 200);
    assert_eq!(upstream.seen.requests(), 1, "the upgrade was forwarded");
    // The headers the origin needs in order to answer 101 at all, restated for
    // the hop this proxy opened.
    assert_eq!(response.header("echo-upgrade"), Some("websocket"));
    assert_eq!(response.header("echo-connection"), Some("upgrade"));
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

#[test]
fn an_h2c_backend_is_refused_rather_than_dialled_as_http_1() {
    // The failure this prevents is not a 502, it is a 200. Dialling HTTP/1.1 at
    // a backend annotated `backend-protocol: GRPC` is exactly what that
    // annotation exists to stop, and doing it silently here would put the bug
    // back one engine flag away from the fix.
    let upstream = echo();
    let proxy = Proxy::start(h2c_table_for("app.example.com", &[upstream.addr]));

    let response = get(proxy.addr, "/", "app.example.com");

    assert_eq!(response.status, 502);
    assert!(
        response.text().contains("--engine hyper"),
        "the refusal must name where it does work: {}",
        response.text()
    );
    assert_eq!(
        response.header("x-ramjet-unsupported"),
        Some("h2c-upstream"),
        "a sentence in the body is for a person; a log pipeline and a client \
         library need a token: {:?}",
        response.headers
    );
    assert_eq!(
        upstream.seen.requests(),
        0,
        "nothing may reach an h2c backend from this engine"
    );
}

/// The two refusals are told apart by the header as well as by the prose.
///
/// The distinction is the whole reason there are two bodies: one is fixed by
/// `--engine hyper` and the other by an annotation on the Ingress. Anything
/// reading these mechanically has to be able to tell them apart without parsing
/// English.
#[test]
fn the_two_capability_refusals_name_themselves_differently() {
    let upstream = echo();

    let annotated = Proxy::start(h2c_table_for("app.example.com", &[upstream.addr]));
    let engine_gap = get(annotated.addr, "/", "app.example.com");
    assert_eq!(
        engine_gap.header("x-ramjet-unsupported"),
        Some("h2c-upstream")
    );

    let plain = Proxy::start(table_for("app.example.com", &[upstream.addr]));
    let mut client = Client::connect(plain.addr);
    let misconfigured = client.send(
        b"POST /svc/Method HTTP/1.1\r\nHost: app.example.com\r\n\
          Content-Type: application/grpc\r\nContent-Length: 0\r\n\r\n",
    );
    assert_eq!(
        misconfigured.header("x-ramjet-unsupported"),
        Some("grpc-needs-backend-protocol")
    );
}

/// A 502 that is *not* a capability gap carries no header.
///
/// Without this the token would appear on every gateway error and mean nothing.
#[test]
fn an_ordinary_bad_gateway_names_no_missing_capability() {
    // A backend with an endpoint nothing is listening on: a real 502, and not
    // one an operator fixes by changing an engine or an annotation.
    let dead = std::net::SocketAddr::from(([127, 0, 0, 1], 1));
    let proxy = Proxy::start(table_for("app.example.com", &[dead]));

    let response = get(proxy.addr, "/", "app.example.com");
    assert_eq!(response.status, 502);
    assert_eq!(
        response.header("x-ramjet-unsupported"),
        None,
        "{:?}",
        response.headers
    );
}

#[test]
fn an_h2c_backend_is_refused_whatever_the_request_looks_like() {
    // The backend's protocol decides this, not the content type. A plain GET to
    // an h2c backend is just as undialable as a gRPC POST.
    let upstream = echo();
    let proxy = Proxy::start(h2c_table_for("app.example.com", &[upstream.addr]));
    let mut client = Client::connect(proxy.addr);

    let grpc = client.send(
        b"POST /svc/Method HTTP/1.1\r\nHost: app.example.com\r\n\
          Content-Type: application/grpc\r\nContent-Length: 0\r\n\r\n",
    );

    assert_eq!(grpc.status, 502);
    assert!(
        grpc.text().contains("HTTP/2 upstream"),
        "a gRPC request to a correctly annotated backend is this engine's gap, \
         not a misconfiguration to fix on the Ingress: {}",
        grpc.text()
    );
}

#[test]
fn grpc_to_an_http1_backend_still_points_at_the_annotation() {
    // The other half of the distinction. Here the operator *has not* said the
    // backend speaks HTTP/2, so the useful answer is the annotation to add —
    // and it must be the hyper engine's wording, byte for byte.
    let upstream = echo();
    let proxy = Proxy::start(table_for("app.example.com", &[upstream.addr]));
    let mut client = Client::connect(proxy.addr);

    let response = client.send(
        b"POST /svc/Method HTTP/1.1\r\nHost: app.example.com\r\n\
          Content-Type: application/grpc\r\nContent-Length: 0\r\n\r\n",
    );

    assert_eq!(response.status, 502);
    assert!(
        response.text().contains("backend-protocol: GRPC"),
        "{}",
        response.text()
    );
    assert!(
        !response.text().contains("--engine hyper"),
        "this one is fixable on the Ingress, so it must not send the operator \
         to the other engine: {}",
        response.text()
    );
}
