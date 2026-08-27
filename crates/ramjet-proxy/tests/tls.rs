//! TLS termination: SNI selection, ALPN, and what the upstream learns about it.
//!
//! Certificates are generated with `rcgen` and trusted directly by the test
//! client, so a handshake here is a real handshake — a real key exchange, a real
//! certificate verification against a real name. The point of that is the SNI
//! test: asserting the *right* certificate was chosen only means something if a
//! wrong one would have been rejected.

mod common;

use std::sync::Arc;

use common::*;
use http::StatusCode;
use ramjet_router::{CertifiedKeyHandle, Endpoint, LbPolicy, PathType, RouteTableBuilder};

/// Two hosts, two certificates, one backend behind both.
fn two_host_table(upstream: std::net::SocketAddr) -> ramjet_router::RouteTable {
    let mut builder = RouteTableBuilder::new();
    builder
        .backend("app", LbPolicy::RoundRobin, vec![Endpoint::new(upstream)])
        .expect("backend");
    for host in ["alpha.example.com", "beta.example.com"] {
        builder
            .route(Some(host), "/", PathType::Prefix, "app")
            .expect("route");
    }
    builder
        .certificate("alpha.example.com", Arc::new(CertifiedKeyHandle::new(1)))
        .expect("certificate");
    builder
        .certificate("beta.example.com", Arc::new(CertifiedKeyHandle::new(2)))
        .expect("certificate");
    builder.build().expect("table")
}

#[tokio::test]
async fn sni_selects_the_certificate_for_the_requested_name() {
    let upstream = spawn_echo("app").await;
    let alpha = TestCert::generate(&["alpha.example.com"]);
    let beta = TestCert::generate(&["beta.example.com"]);

    let proxy = TestProxy::start_with(
        two_host_table(upstream),
        ProxyOptions {
            tls: true,
            certs: cert_store(&[(1, &alpha), (2, &beta)]),
            ..Default::default()
        },
    )
    .await;
    let https = proxy.https.expect("a tls port");

    // Trusting both certificates means a wrong choice would still complete the
    // handshake -- so the assertion is on which chain was actually presented,
    // not merely on the connection succeeding.
    let config = tls_client_config(&[&alpha, &beta], &[b"http/1.1"]);

    let stream = tls_connect(https, "alpha.example.com", Arc::clone(&config)).await;
    let presented = stream
        .get_ref()
        .1
        .peer_certificates()
        .expect("a certificate chain")
        .to_vec();
    assert_eq!(presented, alpha.chain, "SNI alpha got the wrong certificate");
    drop(stream);

    let stream = tls_connect(https, "beta.example.com", config).await;
    let presented = stream
        .get_ref()
        .1
        .peer_certificates()
        .expect("a certificate chain")
        .to_vec();
    assert_eq!(presented, beta.chain, "SNI beta got the wrong certificate");
}

#[tokio::test]
async fn a_request_over_tls_is_proxied_and_marked_https() {
    let upstream = spawn_echo("app").await;
    let alpha = TestCert::generate(&["alpha.example.com"]);
    let beta = TestCert::generate(&["beta.example.com"]);

    let proxy = TestProxy::start_with(
        two_host_table(upstream),
        ProxyOptions {
            tls: true,
            certs: cert_store(&[(1, &alpha), (2, &beta)]),
            ..Default::default()
        },
    )
    .await;
    let https = proxy.https.expect("a tls port");

    let config = tls_client_config(&[&alpha], &[b"http/1.1"]);
    let reply = send_tls(
        https,
        "alpha.example.com",
        config,
        request("alpha.example.com", "/secure")
            .body(empty_body())
            .expect("a request"),
    )
    .await;

    assert_eq!(reply.status, StatusCode::OK);
    assert_eq!(reply.upstream(), "app");
    assert_eq!(
        reply.header("echo-x-forwarded-proto"),
        Some("https"),
        "an application behind TLS termination has no other way to know"
    );
    assert_eq!(reply.header("echo-host"), Some("alpha.example.com"));
    assert!(proxy.metrics.tls_handshakes() >= 1);
}

#[tokio::test]
async fn alpn_negotiates_h2_and_the_request_is_proxied_over_it() {
    let upstream = spawn_echo("app").await;
    let alpha = TestCert::generate(&["alpha.example.com"]);
    let beta = TestCert::generate(&["beta.example.com"]);

    let proxy = TestProxy::start_with(
        two_host_table(upstream),
        ProxyOptions {
            tls: true,
            certs: cert_store(&[(1, &alpha), (2, &beta)]),
            ..Default::default()
        },
    )
    .await;
    let https = proxy.https.expect("a tls port");

    let config = tls_client_config(&[&alpha], &[b"h2"]);
    let (reply, negotiated) = send_tls_h2(
        https,
        "alpha.example.com",
        config,
        http::Request::builder()
            .uri("https://alpha.example.com/over-h2?x=1")
            .body(empty_body())
            .expect("a request"),
    )
    .await;

    assert_eq!(negotiated.as_deref(), Some(b"h2".as_slice()));
    assert_eq!(reply.status, StatusCode::OK);
    assert_eq!(reply.text(), "GET /over-h2?x=1");
    // HTTP/2 carries the host in `:authority` and has no `Host` header, so the
    // downgrade to an HTTP/1.1 upstream has to reconstruct one -- otherwise
    // hyper would fill it in from the endpoint's `ip:port`.
    assert_eq!(reply.header("echo-host"), Some("alpha.example.com"));
    assert_eq!(reply.header("echo-x-forwarded-proto"), Some("https"));
}

#[tokio::test]
async fn a_name_with_no_certificate_fails_the_handshake() {
    let upstream = spawn_echo("app").await;
    let alpha = TestCert::generate(&["alpha.example.com"]);

    // Only handle 1 is loaded, so `beta.example.com` resolves to a handle the
    // store does not hold and the handshake has nothing to offer.
    let proxy = TestProxy::start_with(
        two_host_table(upstream),
        ProxyOptions {
            tls: true,
            certs: cert_store(&[(1, &alpha)]),
            ..Default::default()
        },
    )
    .await;
    let https = proxy.https.expect("a tls port");

    let config = tls_client_config(&[&alpha], &[b"http/1.1"]);
    let name = rustls::pki_types::ServerName::try_from("beta.example.com".to_owned())
        .expect("a valid name");
    let stream = tokio::net::TcpStream::connect(https).await.expect("connect");
    let result = tokio_rustls::TlsConnector::from(config)
        .connect(name, stream)
        .await;

    assert!(
        result.is_err(),
        "a missing certificate must abort the handshake, not serve a wrong one"
    );
}
