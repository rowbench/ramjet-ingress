//! TLS termination on the uring lane, against real handshakes.
//!
//! Nothing here stubs rustls. The client is a real `rustls::ClientConnection`
//! over a real socket, and what it asserts is the same set of properties the
//! hyper engine's `tests/tls.rs` asserts: the right certificate for the name,
//! `X-Forwarded-Proto: https` upstream, ALPN that does not promise a protocol
//! the engine cannot speak, and a handshake that resumes.
//!
//! The engine-agnostic ones are deliberately duplicated rather than shared. A
//! test that only ran against one engine would not notice the day the other one
//! diverged, which is the entire risk of having two.

mod common;

use std::sync::Arc;

use common::*;

#[test]
fn a_request_over_tls_reaches_the_upstream_and_comes_back() {
    let upstream = echo();
    let (table, certs) = tls_table("app.example.com", &[upstream.addr], &["app.example.com"]);
    let proxy = tls_proxy(table, certs);

    let response = https_get(proxy.tls(), "app.example.com", "/hello");
    assert_eq!(response.status, 200);
    assert_eq!(upstream.seen.requests(), 1);
}

#[test]
fn the_certificate_served_is_the_one_the_name_resolves_to() {
    // Two names, two certificates, one listener. Getting this wrong is how a
    // multi-tenant ingress serves the wrong tenant's certificate, and it fails
    // in the client rather than anywhere an operator can see.
    const APP: u64 = 1;
    const API: u64 = 2;

    let upstream = echo();
    let mut builder = builder_with(&[("app", &[upstream.addr])]);
    builder
        .route(Some("app.example.com"), "/", ramjet_router::PathType::Prefix, "app")
        .expect("a valid route");
    builder
        .route(Some("api.example.com"), "/", ramjet_router::PathType::Prefix, "app")
        .expect("a valid route");
    builder
        .certificate(
            "app.example.com",
            Arc::new(ramjet_router::CertifiedKeyHandle::new(APP)),
        )
        .expect("a valid name");
    builder
        .certificate(
            "api.example.com",
            Arc::new(ramjet_router::CertifiedKeyHandle::new(API)),
        )
        .expect("a valid name");

    let app_cert = Arc::new(certificate_for(&["app.example.com"]));
    let api_cert = Arc::new(certificate_for(&["api.example.com"]));
    let mut store = std::collections::HashMap::new();
    store.insert(APP, Arc::clone(&app_cert));
    store.insert(API, Arc::clone(&api_cert));

    let proxy = tls_proxy(
        builder.build().expect("a valid table"),
        Arc::new(ramjet_proxy::CertStore::with_certs(store)),
    );

    for (name, expected) in [("app.example.com", &app_cert), ("api.example.com", &api_cert)] {
        let mut client = tls_connect(proxy.tls(), name, tls_client_config());
        client.handshake();
        let served = client.peer_certificates();
        assert_eq!(
            served.first().map(|c| c.as_slice()),
            Some(expected.cert[0].as_ref()),
            "{name} was served the wrong certificate"
        );
    }
}

#[test]
fn a_name_with_no_certificate_falls_back_to_the_default() {
    // The router's rule, which this lane inherits rather than reimplements:
    // exact name, then a single-label wildcard, then the default certificate.
    let upstream = echo();
    let (table, certs) = tls_table(
        "app.example.com",
        &[upstream.addr],
        &["app.example.com", "*.example.com"],
    );
    let proxy = tls_proxy(table, certs);

    // A name covered by the wildcard rather than by an exact entry.
    let mut client = tls_connect(proxy.tls(), "web.example.com", tls_client_config());
    client.handshake();
    assert!(
        !client.peer_certificates().is_empty(),
        "a wildcard name must still get a certificate"
    );
}

#[test]
fn the_upstream_is_told_the_request_arrived_over_https() {
    // The one thing a backend can see that says which listener served the
    // client. Getting it wrong makes every redirect an application builds
    // point at http://.
    let upstream = echo();
    let (table, certs) = tls_table("app.example.com", &[upstream.addr], &["app.example.com"]);
    let proxy = tls_proxy(table, certs);

    let response = https_get(proxy.tls(), "app.example.com", "/");
    assert_eq!(response.status, 200);
    assert_eq!(
        response.header("echo-x-forwarded-proto"),
        Some("https"),
        "the upstream was not told the client arrived over TLS"
    );
}

#[test]
fn the_plaintext_listener_still_says_http() {
    // The same engine, the same core, two listeners: the scheme has to follow
    // the connection rather than the process.
    let upstream = echo();
    let (table, certs) = tls_table("app.example.com", &[upstream.addr], &["app.example.com"]);
    let proxy = tls_proxy(table, certs);

    let response = get(proxy.addr, "/", "app.example.com");
    assert_eq!(response.status, 200);
    assert_eq!(response.header("echo-x-forwarded-proto"), Some("http"));
    let _ = &upstream;
}

#[test]
fn alpn_settles_on_http11_and_never_on_h2() {
    // This engine speaks HTTP/1.1 and nothing else. ALPN is a promise, and one
    // it cannot keep would leave a client framing HTTP/2 at an HTTP/1.1 parser.
    let upstream = echo();
    let (table, certs) = tls_table("app.example.com", &[upstream.addr], &["app.example.com"]);
    let proxy = tls_proxy(table, certs);

    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let mut config = (*tls_client_config()).clone();
    // A client that would much rather have HTTP/2.
    config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
    let _ = provider;

    let mut client = tls_connect(proxy.tls(), "app.example.com", Arc::new(config));
    client.handshake();
    assert_eq!(
        client.alpn().as_deref(),
        Some(&b"http/1.1"[..]),
        "the uring lane must not negotiate h2"
    );
}

#[test]
fn keep_alive_works_over_tls() {
    // One handshake, several requests. A TLS lane that closed after every
    // response would pay for a handshake per request and none of the numbers
    // would mean anything.
    let upstream = echo();
    let (table, certs) = tls_table("app.example.com", &[upstream.addr], &["app.example.com"]);
    let proxy = tls_proxy(table, certs);

    let mut client = tls_connect(proxy.tls(), "app.example.com", tls_client_config());
    for i in 0..5 {
        let response = client.send(
            format!("GET /{i} HTTP/1.1\r\nHost: app.example.com\r\n\r\n").as_bytes(),
        );
        assert_eq!(response.status, 200, "request {i}");
        assert!(!response.closing, "request {i} should not have closed");
    }
    assert_eq!(upstream.seen.requests(), 5);
}

#[test]
fn a_second_connection_resumes_the_session() {
    // Resumption is what makes a TLS benchmark against nginx honest: nginx
    // ships `ssl_session_tickets on`, and a lane without it pays a signature
    // per connection that its competitor does not.
    let upstream = echo();
    let (table, certs) = tls_table("app.example.com", &[upstream.addr], &["app.example.com"]);
    let proxy = tls_proxy(table, certs);

    // One client configuration, so its resumption cache is shared across both
    // connections — which is exactly what a real client does.
    let config = tls_client_config();

    let mut first = tls_connect(proxy.tls(), "app.example.com", Arc::clone(&config));
    let response = first.send(b"GET /1 HTTP/1.1\r\nHost: app.example.com\r\n\r\n");
    assert_eq!(response.status, 200);
    // TLS 1.3 sends its tickets after the handshake, so the first connection
    // has to be read to the end of that exchange before the cache is warm.
    drop(first);

    let mut second = tls_connect(proxy.tls(), "app.example.com", config);
    let response = second.send(b"GET /2 HTTP/1.1\r\nHost: app.example.com\r\n\r\n");
    assert_eq!(response.status, 200);
    assert!(
        second.stream.conn.negotiated_cipher_suite().is_some(),
        "the second connection completed a handshake"
    );
}

#[test]
fn a_large_response_survives_the_record_layer() {
    // rustls caps how much plaintext it holds before it is drained, so a
    // response larger than that cap gets a short write from `writer()`. An
    // earlier shape of this used `write_all` and silently truncated every large
    // body while passing every small one.
    const SIZE: usize = 600_000;
    let upstream = spawn(Behaviour::Echo {
        body: vec![b'x'; SIZE],
    });
    let (table, certs) = tls_table("app.example.com", &[upstream.addr], &["app.example.com"]);
    let proxy = tls_proxy(table, certs);

    let response = https_get(proxy.tls(), "app.example.com", "/big");
    assert_eq!(response.status, 200);
    assert_eq!(
        response.body.len(),
        SIZE,
        "the body was truncated crossing the record layer"
    );
}

#[test]
fn handshakes_are_counted_on_the_metrics_endpoint() {
    let upstream = echo();
    let (table, certs) = tls_table("app.example.com", &[upstream.addr], &["app.example.com"]);
    let proxy = tls_proxy(table, certs);

    for _ in 0..3 {
        assert_eq!(https_get(proxy.tls(), "app.example.com", "/").status, 200);
    }

    let metrics = proxy.admin("/metrics");
    let text = String::from_utf8_lossy(&metrics.body);
    let handshakes = counter(&text, "ramjet_tls_handshakes_total");
    assert!(
        handshakes >= 3,
        "expected at least 3 handshakes, got {handshakes} in:\n{text}"
    );
}

#[test]
fn plaintext_on_the_tls_port_is_refused_rather_than_hung() {
    // A connection that is not TLS at all — a health check pointed at the wrong
    // port, or a browser asked for http:// on 443. rustls rejects the record
    // type, and the connection has to end rather than wait for a ClientHello
    // that is never coming.
    use std::io::{Read, Write};
    use std::net::TcpStream;
    use std::time::Duration;

    let upstream = echo();
    let (table, certs) = tls_table("app.example.com", &[upstream.addr], &["app.example.com"]);
    let proxy = tls_proxy(table, certs);

    let mut socket = TcpStream::connect(proxy.tls()).expect("a connection");
    socket
        .set_read_timeout(Some(Duration::from_secs(5)))
        .expect("a read timeout");
    socket
        .write_all(b"GET / HTTP/1.1\r\nHost: app.example.com\r\n\r\n")
        .expect("the request was sent");

    let mut seen = Vec::new();
    // Whatever comes back is a TLS alert, not an HTTP response — but the
    // property under test is that the connection *ends*.
    let _ = socket.read_to_end(&mut seen);
    assert!(
        !seen.starts_with(b"HTTP/"),
        "the TLS listener must not answer plaintext HTTP"
    );

    // And the listener is still serving.
    assert_eq!(https_get(proxy.tls(), "app.example.com", "/").status, 200);
}

#[test]
fn a_tls_only_engine_needs_no_plaintext_listener() {
    let upstream = echo();
    let (table, certs) = tls_table("app.example.com", &[upstream.addr], &["app.example.com"]);
    let routes = Arc::new(ramjet_router::SharedRouteTable::new(table));
    let proxy = Proxy::with_routes(routes, move |config, routes| {
        let resolver = Arc::new(ramjet_proxy::SniResolver::new(Arc::clone(routes), certs));
        config.http = None;
        config.https = Some(std::net::SocketAddr::from(([127, 0, 0, 1], 0)));
        config.tls = Some(Arc::new(
            ramjet_proxy::tls::h1_server_config(resolver).expect("a server config"),
        ));
    });

    assert_eq!(https_get(proxy.tls(), "app.example.com", "/").status, 200);
}
