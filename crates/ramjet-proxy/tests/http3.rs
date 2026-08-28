//! HTTP/3 over real QUIC, against real UDP sockets.
//!
//! Nothing here is mocked. Every test binds an ephemeral UDP port, does a real
//! QUIC handshake against a real self-signed certificate, and speaks RFC 9114
//! to it with the `h3` client — because the bugs this path can have are in
//! framing, certificate selection, body length, and shutdown ordering, and none
//! of those exist against a fake.
//!
//! The `alt-svc` tests deliberately go the other way round: they connect over
//! TCP, because the advertisement is a property of the *TLS* listener's
//! responses and its whole job is to tell a client that has not used QUIC that
//! it could.

mod common;

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::{Buf, Bytes};
use common::{
    cert_store, dead_addr, empty_body, get, request, send_tls, send_tls_h2, spawn_echo,
    spawn_http, spawn_raw, tls_client_config, ProxyOptions, TestCert, TestProxy,
};
use http::{HeaderMap, Request, Response, StatusCode};
use http_body_util::BodyExt;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// The certificate handle id every test table points its host at.
const CERT_ID: u64 = 1;

// ---------------------------------------------------------------------------
// An HTTP/3 client
// ---------------------------------------------------------------------------

/// A QUIC client configuration trusting `roots` and offering only `h3`.
///
/// TLS 1.3 only, because QUIC carries no earlier version, and quinn refuses to
/// build a configuration that offers one.
fn quic_client_config(roots: &[&TestCert]) -> quinn::ClientConfig {
    let mut store = rustls::RootCertStore::empty();
    for cert in roots {
        for der in &cert.chain {
            store.add(der.clone()).expect("a usable root");
        }
    }
    let mut config = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_protocol_versions(&[&rustls::version::TLS13])
    .expect("tls 1.3")
    .with_root_certificates(store)
    .with_no_client_auth();
    config.alpn_protocols = vec![b"h3".to_vec()];

    quinn::ClientConfig::new(Arc::new(
        quinn::crypto::rustls::QuicClientConfig::try_from(config).expect("a quic client config"),
    ))
}

/// One HTTP/3 connection to the proxy, and the task driving it.
struct H3Client {
    send: h3::client::SendRequest<h3_quinn::OpenStreams, Bytes>,
    endpoint: quinn::Endpoint,
    driver: tokio::task::JoinHandle<()>,
}

impl H3Client {
    /// Connects to `addr`, presenting `server_name` as the SNI.
    async fn connect(addr: SocketAddr, server_name: &str, roots: &[&TestCert]) -> H3Client {
        Self::try_connect(addr, server_name, roots)
            .await
            .expect("the QUIC handshake should succeed")
    }

    async fn try_connect(
        addr: SocketAddr,
        server_name: &str,
        roots: &[&TestCert],
    ) -> Result<H3Client, String> {
        let mut endpoint = quinn::Endpoint::client("127.0.0.1:0".parse().expect("a literal"))
            .map_err(|error| error.to_string())?;
        endpoint.set_default_client_config(quic_client_config(roots));

        let connection = endpoint
            .connect(addr, server_name)
            .map_err(|error| error.to_string())?
            .await
            .map_err(|error| error.to_string())?;

        let (mut driver, send) = h3::client::new(h3_quinn::Connection::new(connection))
            .await
            .map_err(|error| error.to_string())?;
        // The connection makes no progress unless something polls it; a client
        // that only awaits its own streams deadlocks on the control stream.
        let driver = tokio::spawn(async move {
            let _ = driver.wait_idle().await;
        });

        Ok(H3Client {
            send,
            endpoint,
            driver,
        })
    }

    /// Sends a request with no body and collects the whole response.
    async fn get(&mut self, host: &str, path: &str) -> H3Reply {
        let request = Request::builder()
            .method("GET")
            .uri(format!("https://{host}{path}"))
            .body(())
            .expect("a request");
        let mut stream = self.send.send_request(request).await.expect("a stream");
        // A GET has no body, and saying so is what lets the server recognise
        // the request as complete without waiting.
        stream.finish().await.expect("finish");
        collect(&mut stream).await
    }

    /// Sends `chunks` as the request body, `gap` apart, and collects the reply.
    async fn post(&mut self, host: &str, path: &str, chunks: Vec<Bytes>, gap: Duration) -> H3Reply {
        let request = Request::builder()
            .method("POST")
            .uri(format!("https://{host}{path}"))
            .body(())
            .expect("a request");
        let mut stream = self.send.send_request(request).await.expect("a stream");
        for (index, chunk) in chunks.into_iter().enumerate() {
            if index > 0 && !gap.is_zero() {
                tokio::time::sleep(gap).await;
            }
            stream.send_data(chunk).await.expect("send data");
        }
        stream.finish().await.expect("finish");
        collect(&mut stream).await
    }
}

impl Drop for H3Client {
    fn drop(&mut self) {
        self.driver.abort();
        self.endpoint.close(0u32.into(), b"test over");
    }
}

/// A collected HTTP/3 response.
#[derive(Debug)]
struct H3Reply {
    status: StatusCode,
    headers: HeaderMap,
    body: Bytes,
    /// How long after the request the response *head* arrived, which is the
    /// number the streaming tests are about.
    head_after: Duration,
}

impl H3Reply {
    fn text(&self) -> &str {
        std::str::from_utf8(&self.body).unwrap_or("<not utf-8>")
    }

    fn header(&self, name: &str) -> Option<&str> {
        self.headers.get(name).and_then(|value| value.to_str().ok())
    }
}

/// Waits, briefly and boundedly, for `ready` to hold.
///
/// Only ever used for counters that another thread has already written by the
/// time the caller reaches it — what is being waited for is the store becoming
/// visible, not the work happening — so the assertion that follows a call to
/// this stays exact rather than becoming a range.
async fn settle(ready: impl Fn() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while !ready() && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn collect(
    stream: &mut h3::client::RequestStream<h3_quinn::BidiStream<Bytes>, Bytes>,
) -> H3Reply {
    let started = Instant::now();
    let response = stream.recv_response().await.expect("a response");
    let head_after = started.elapsed();

    let mut body = Vec::new();
    while let Some(mut chunk) = stream.recv_data().await.expect("body data") {
        body.extend_from_slice(chunk.copy_to_bytes(chunk.remaining()).as_ref());
    }
    H3Reply {
        status: response.status(),
        headers: response.headers().clone(),
        body: Bytes::from(body),
        head_after,
    }
}

// ---------------------------------------------------------------------------
// The proxy under test
// ---------------------------------------------------------------------------

/// A proxy serving `host` over HTTP/3, with a certificate for `host`.
async fn h3_proxy(host: &str, upstream: SocketAddr) -> (TestProxy, TestCert) {
    let cert = TestCert::generate(&[host]);
    let proxy = TestProxy::start_with(
        table(host, &[upstream], CERT_ID),
        ProxyOptions {
            tls: true,
            http3: true,
            certs: cert_store(&[(CERT_ID, &cert)]),
            ..ProxyOptions::default()
        },
    )
    .await;
    (proxy, cert)
}

/// A one-host table with a certificate attached to that host.
fn table(host: &str, endpoints: &[SocketAddr], cert_id: u64) -> ramjet_router::RouteTable {
    use ramjet_router::{CertifiedKeyHandle, Endpoint, LbPolicy, PathType, RouteTableBuilder};

    let mut builder = RouteTableBuilder::new();
    builder
        .backend(
            "app",
            LbPolicy::RoundRobin,
            endpoints.iter().copied().map(Endpoint::new).collect(),
        )
        .expect("a backend");
    builder
        .route(Some(host), "/", PathType::Prefix, "app")
        .expect("a route");
    builder
        .certificate(host, Arc::new(CertifiedKeyHandle::new(cert_id)))
        .expect("a certificate");
    builder.build().expect("a valid table")
}

// ---------------------------------------------------------------------------
// Requests
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn a_get_over_http3_reaches_the_upstream() {
    let upstream = spawn_echo("origin").await;
    let (proxy, cert) = h3_proxy("h3.example.com", upstream).await;
    let quic = proxy.http3.expect("a QUIC port");

    let mut client = H3Client::connect(quic, "h3.example.com", &[&cert]).await;
    let reply = client.get("h3.example.com", "/hello?q=1").await;

    assert_eq!(reply.status, StatusCode::OK);
    assert_eq!(reply.text(), "GET /hello?q=1");
    assert_eq!(reply.header("x-upstream"), Some("origin"));
}

#[tokio::test(flavor = "multi_thread")]
async fn the_request_arrives_upstream_as_an_ordinary_http_1_1_request() {
    let upstream = spawn_echo("origin").await;
    let (proxy, cert) = h3_proxy("h3.example.com", upstream).await;
    let quic = proxy.http3.expect("a QUIC port");

    let mut client = H3Client::connect(quic, "h3.example.com", &[&cert]).await;
    let reply = client.get("h3.example.com", "/").await;

    // HTTP/3 carries the authority in `:authority` and has no `Host` header at
    // all, exactly like HTTP/2. Losing it on the way to an HTTP/1.1 origin
    // would let hyper fill one in from the endpoint's ip:port, which is the
    // rewrite this proxy promises never to do.
    assert_eq!(reply.header("echo-host"), Some("h3.example.com"));
    // QUIC is TLS 1.3 and there is no plaintext HTTP/3, so an application
    // behind this must be told the request was secure.
    assert_eq!(reply.header("echo-x-forwarded-proto"), Some("https"));
    assert!(reply.header("echo-x-request-id").is_some());
    // The client address is the QUIC peer's, and it is a loopback address here
    // because that is where the test client is.
    let forwarded = reply.header("echo-x-forwarded-for").expect("a client address");
    assert!(
        forwarded.starts_with("127.0.0.1"),
        "{forwarded} is not the QUIC peer"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_bodyless_request_is_not_sent_upstream_as_chunked() {
    // This is what the non-blocking peek in `http3::request_body` buys. A GET
    // over HTTP/3 declares no `content-length` — there is nothing to declare —
    // and treating that as "length unknown" would hand every origin a
    // `Transfer-Encoding: chunked` GET, which is legal, weird, and a visible
    // difference from what the same request looks like over HTTP/2.
    let upstream = spawn_echo("origin").await;
    let (proxy, cert) = h3_proxy("h3.example.com", upstream).await;
    let quic = proxy.http3.expect("a QUIC port");

    let mut client = H3Client::connect(quic, "h3.example.com", &[&cert]).await;
    let reply = client.get("h3.example.com", "/").await;

    assert_eq!(reply.status, StatusCode::OK);
    assert_eq!(
        reply.header("echo-transfer-encoding"),
        None,
        "a request with no body should not be chunked upstream"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_streamed_post_body_arrives_whole() {
    // 512 KiB in sixteen frames, sent with gaps, so the body genuinely crosses
    // the proxy as a stream rather than as one buffer that happened to fit.
    let upstream = spawn_http(|request: Request<hyper::body::Incoming>| async move {
        let body = request.into_body().collect().await.expect("a body").to_bytes();
        let sum: u64 = body.iter().map(|byte| u64::from(*byte)).sum();
        Response::new(common::full(format!("{} {sum}", body.len())))
    })
    .await;

    let (proxy, cert) = h3_proxy("h3.example.com", upstream).await;
    let quic = proxy.http3.expect("a QUIC port");

    let chunk: Vec<u8> = (0..32 * 1024).map(|i| (i % 251) as u8).collect();
    let expected_len = chunk.len() * 16;
    let expected_sum: u64 = chunk.iter().map(|b| u64::from(*b)).sum::<u64>() * 16;
    let chunks = vec![Bytes::from(chunk); 16];

    let mut client = H3Client::connect(quic, "h3.example.com", &[&cert]).await;
    let reply = client
        .post("h3.example.com", "/upload", chunks, Duration::from_millis(2))
        .await;

    assert_eq!(reply.status, StatusCode::OK);
    assert_eq!(reply.text(), format!("{expected_len} {expected_sum}"));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_declared_content_length_is_carried_upstream_verbatim() {
    // A client that declared a length gets that length forwarded, rather than
    // the body being re-framed as chunked. The body has to arrive whole too:
    // the declared length is what hyper writes into the request head, and a
    // size hint that disagreed with the bytes would truncate the upload or
    // hang the origin waiting for more.
    let upstream = spawn_http(|request: Request<hyper::body::Incoming>| async move {
        let declared = request
            .headers()
            .get(http::header::CONTENT_LENGTH)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("<none>")
            .to_owned();
        let chunked = request.headers().contains_key(http::header::TRANSFER_ENCODING);
        let body = request.into_body().collect().await.expect("a body").to_bytes();
        Response::new(common::full(format!(
            "declared={declared} chunked={chunked} got={}",
            body.len()
        )))
    })
    .await;

    let (proxy, cert) = h3_proxy("h3.example.com", upstream).await;
    let quic = proxy.http3.expect("a QUIC port");
    let payload = Bytes::from(vec![b'z'; 4096]);

    let mut client = H3Client::connect(quic, "h3.example.com", &[&cert]).await;
    let request = Request::builder()
        .method("POST")
        .uri("https://h3.example.com/sized")
        .header(http::header::CONTENT_LENGTH, payload.len())
        .body(())
        .expect("a request");
    let mut stream = client.send.send_request(request).await.expect("a stream");
    stream.send_data(payload.clone()).await.expect("send data");
    stream.finish().await.expect("finish");
    let reply = collect(&mut stream).await;

    assert_eq!(reply.status, StatusCode::OK);
    assert_eq!(reply.text(), "declared=4096 chunked=false got=4096");
}

#[tokio::test(flavor = "multi_thread")]
async fn the_request_head_reaches_the_upstream_before_the_body_is_finished() {
    // The proxy must not wait for a complete request body before dialling: an
    // upload that takes a minute would otherwise sit in this process for a
    // minute, which is exactly the buffering the streaming promise rules out.
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let upstream = spawn_raw(move |mut stream| {
        let tx = tx.clone();
        async move {
            // The head is reported the moment it is complete, which is the
            // whole assertion. Reading then continues to the end of the
            // chunked body — answering early would be legal, but it would make
            // the server reset the request stream and the client's remaining
            // sends fail, which is a different test.
            let mut buffer = Vec::new();
            let mut chunk = [0u8; 8192];
            let mut head_seen = false;
            while !buffer.ends_with(b"\r\n0\r\n\r\n") {
                match stream.read(&mut chunk).await {
                    Ok(0) | Err(_) => break,
                    Ok(read) => buffer.extend_from_slice(&chunk[..read]),
                }
                if !head_seen {
                    if let Some(at) = buffer.windows(4).position(|w| w == b"\r\n\r\n") {
                        head_seen = true;
                        let _ = tx.send(String::from_utf8_lossy(&buffer[..at]).into_owned());
                    }
                }
            }
            let _ = stream
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok")
                .await;
        }
    })
    .await;

    let (proxy, cert) = h3_proxy("h3.example.com", upstream).await;
    let quic = proxy.http3.expect("a QUIC port");
    let mut client = H3Client::connect(quic, "h3.example.com", &[&cert]).await;

    let chunks = vec![Bytes::from(vec![b'x'; 32 * 1024]); 2];
    let sending = tokio::spawn(async move {
        client
            .post("h3.example.com", "/slow", chunks, Duration::from_millis(400))
            .await
    });

    // The head has to be upstream well inside the 400ms gap between the two
    // body frames.
    let head = tokio::time::timeout(Duration::from_millis(300), rx.recv())
        .await
        .expect("the request head should reach the upstream before the body ends")
        .expect("a head");
    assert!(head.starts_with("POST /slow "), "{head}");

    let reply = sending.await.expect("the request finishes");
    assert_eq!(reply.status, StatusCode::OK);
    assert_eq!(reply.text(), "ok");
}

#[tokio::test(flavor = "multi_thread")]
async fn the_response_head_arrives_before_its_body() {
    // The other direction: a response is relayed frame by frame, so the client
    // gets the status and headers as soon as the origin has sent them rather
    // than when the last body byte lands.
    let upstream = spawn_raw(|mut stream| async move {
        let _ = common::read_head(&mut stream).await;
        let _ = stream
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 6\r\n\r\n")
            .await;
        let _ = stream.flush().await;
        tokio::time::sleep(Duration::from_millis(400)).await;
        let _ = stream.write_all(b"abcdef").await;
    })
    .await;

    let (proxy, cert) = h3_proxy("h3.example.com", upstream).await;
    let quic = proxy.http3.expect("a QUIC port");

    let mut client = H3Client::connect(quic, "h3.example.com", &[&cert]).await;
    let reply = client.get("h3.example.com", "/slow").await;

    assert_eq!(reply.status, StatusCode::OK);
    assert_eq!(reply.text(), "abcdef");
    assert!(
        reply.head_after < Duration::from_millis(300),
        "the head waited {:?} for a body that was 400ms behind it",
        reply.head_after
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn response_trailers_survive_the_crossing_and_still_end_the_stream() {
    // Trailers are a second HEADERS frame in HTTP/3 and they do not carry the
    // FIN, so the stream still has to be finished afterwards. Getting that
    // wrong resets the stream instead, and the client reports an error on a
    // response it had already received in full.
    let upstream = spawn_raw(|mut stream| async move {
        let _ = common::read_head(&mut stream).await;
        let _ = stream
            .write_all(
                b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nTrailer: x-checksum\r\n\r\n\
                  5\r\nhello\r\n0\r\nx-checksum: 1234\r\n\r\n",
            )
            .await;
    })
    .await;

    let (proxy, cert) = h3_proxy("h3.example.com", upstream).await;
    let quic = proxy.http3.expect("a QUIC port");

    let mut client = H3Client::connect(quic, "h3.example.com", &[&cert]).await;
    let request = Request::builder()
        .method("GET")
        .uri("https://h3.example.com/trailers")
        .body(())
        .expect("a request");
    let mut stream = client.send.send_request(request).await.expect("a stream");
    stream.finish().await.expect("finish");

    let response = stream.recv_response().await.expect("a response");
    assert_eq!(response.status(), StatusCode::OK);

    let mut body = Vec::new();
    while let Some(mut chunk) = stream.recv_data().await.expect("body data") {
        body.extend_from_slice(chunk.copy_to_bytes(chunk.remaining()).as_ref());
    }
    assert_eq!(body, b"hello");

    let trailers = stream
        .recv_trailers()
        .await
        .expect("the stream ended cleanly, not with a reset")
        .expect("a trailer block");
    assert_eq!(
        trailers.get("x-checksum").map(|v| v.as_bytes()),
        Some(&b"1234"[..])
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn an_upstream_that_refuses_the_connection_is_a_502_over_quic() {
    // Error mapping is `forward`'s, not this module's, and that is the point of
    // asserting it here: the QUIC path reaches the same code.
    let (proxy, cert) = h3_proxy("h3.example.com", dead_addr().await).await;
    let quic = proxy.http3.expect("a QUIC port");

    let mut client = H3Client::connect(quic, "h3.example.com", &[&cert]).await;
    let reply = client.get("h3.example.com", "/").await;

    assert_eq!(reply.status, StatusCode::BAD_GATEWAY);
    assert!(reply.text().starts_with("502"), "{}", reply.text());
}

#[tokio::test(flavor = "multi_thread")]
async fn a_request_for_an_unrouted_host_is_a_404_over_quic() {
    let upstream = spawn_echo("origin").await;
    let (proxy, cert) = h3_proxy("h3.example.com", upstream).await;
    let quic = proxy.http3.expect("a QUIC port");

    let mut client = H3Client::connect(quic, "h3.example.com", &[&cert]).await;
    // The SNI still names the host with a certificate; the `:authority` does
    // not, which is what routing reads.
    let reply = client.get("nobody.example.com", "/").await;

    assert_eq!(reply.status, StatusCode::NOT_FOUND);
}

// ---------------------------------------------------------------------------
// Certificates
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn sni_selects_the_certificate_for_the_name_over_quic() {
    // The QUIC listener shares the TLS listener's resolver, so a name has to
    // pick the same certificate over UDP as it does over TCP. Two certificates
    // that trust nothing in common is the sharpest way to assert it: a
    // connection that got the wrong one cannot complete at all.
    use ramjet_router::{CertifiedKeyHandle, Endpoint, LbPolicy, PathType, RouteTableBuilder};

    let upstream = spawn_echo("origin").await;
    let first = TestCert::generate(&["one.example.com"]);
    let second = TestCert::generate(&["two.example.com"]);

    let mut builder = RouteTableBuilder::new();
    builder
        .backend("app", LbPolicy::RoundRobin, vec![Endpoint::new(upstream)])
        .expect("a backend");
    for host in ["one.example.com", "two.example.com"] {
        builder
            .route(Some(host), "/", PathType::Prefix, "app")
            .expect("a route");
    }
    builder
        .certificate("one.example.com", Arc::new(CertifiedKeyHandle::new(1)))
        .expect("a certificate");
    builder
        .certificate("two.example.com", Arc::new(CertifiedKeyHandle::new(2)))
        .expect("a certificate");

    let proxy = TestProxy::start_with(
        builder.build().expect("a valid table"),
        ProxyOptions {
            tls: true,
            http3: true,
            certs: cert_store(&[(1, &first), (2, &second)]),
            ..ProxyOptions::default()
        },
    )
    .await;
    let quic = proxy.http3.expect("a QUIC port");

    // Each name, trusting only its own issuer. Both succeed only if the server
    // answered each handshake with the certificate for the name it was asked
    // about.
    let mut one = H3Client::connect(quic, "one.example.com", &[&first]).await;
    assert_eq!(one.get("one.example.com", "/").await.status, StatusCode::OK);

    let mut two = H3Client::connect(quic, "two.example.com", &[&second]).await;
    assert_eq!(two.get("two.example.com", "/").await.status, StatusCode::OK);

    // And the mismatch really is a mismatch, so the assertions above are not
    // passing because everything is accepted.
    assert!(
        H3Client::try_connect(quic, "two.example.com", &[&first])
            .await
            .is_err(),
        "the wrong issuer should not have verified"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_name_with_no_certificate_fails_the_quic_handshake_and_is_counted() {
    let upstream = spawn_echo("origin").await;
    let (proxy, cert) = h3_proxy("h3.example.com", upstream).await;
    let quic = proxy.http3.expect("a QUIC port");

    let before = proxy.metrics.h3_handshake_failures();
    assert!(
        H3Client::try_connect(quic, "unknown.example.com", &[&cert])
            .await
            .is_err(),
        "a name the table holds no certificate for must not complete"
    );

    // The failure is counted rather than logged per occurrence, exactly as a
    // failed TLS handshake on the TCP listener is.
    settle(|| proxy.metrics.h3_handshake_failures() > before).await;
    assert!(proxy.metrics.h3_handshake_failures() > before);
}

#[tokio::test(flavor = "multi_thread")]
async fn the_h3_counters_move_with_the_traffic() {
    let upstream = spawn_echo("origin").await;
    let (proxy, cert) = h3_proxy("h3.example.com", upstream).await;
    let quic = proxy.http3.expect("a QUIC port");

    let mut client = H3Client::connect(quic, "h3.example.com", &[&cert]).await;
    for _ in 0..3 {
        assert_eq!(
            client.get("h3.example.com", "/").await.status,
            StatusCode::OK
        );
    }

    // Waited for rather than read straight away. The counters are relaxed
    // atomics written on the HTTP/3 runtime's own thread and read here on the
    // test's, so what is being given time is the store becoming visible, not
    // the request finishing — that already has, or `get` would not have
    // returned. The assertions below are still exact.
    settle(|| {
        proxy.metrics.h3_requests() == 3 && proxy.metrics.responses("2xx") == 3
    })
    .await;

    assert_eq!(proxy.metrics.h3_connections(), 1, "one QUIC connection");
    assert_eq!(proxy.metrics.h3_requests(), 3, "three requests on it");
    // The shared series counts them too: an HTTP/3 request is a request.
    assert_eq!(proxy.metrics.responses("2xx"), 3);

    let text = proxy.metrics.render_prometheus(1, false);
    for series in [
        "ramjet_h3_connections_total 1",
        "ramjet_h3_requests_total 3",
        "ramjet_h3_handshake_failures_total 0",
    ] {
        assert!(text.contains(series), "{series} is missing from /metrics");
    }
}

// ---------------------------------------------------------------------------
// alt-svc
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn alt_svc_advertises_the_quic_port_on_tls_responses() {
    let upstream = spawn_echo("origin").await;
    let (proxy, cert) = h3_proxy("h3.example.com", upstream).await;
    let https = proxy.https.expect("a TLS port");
    let quic = proxy.http3.expect("a QUIC port");
    let expected = format!("h3=\":{}\"; ma=86400", quic.port());

    // HTTP/2, which is what a browser that has not yet heard of the QUIC port
    // is most likely speaking.
    let (reply, alpn) = send_tls_h2(
        https,
        "h3.example.com",
        tls_client_config(&[&cert], &[b"h2"]),
        request("h3.example.com", "/").body(empty_body()).expect("a request"),
    )
    .await;
    assert_eq!(alpn.as_deref(), Some(&b"h2"[..]));
    assert_eq!(reply.header("alt-svc"), Some(expected.as_str()));

    // And HTTP/1.1, because a client that never upgrades still deserves to be
    // told there is a faster way in.
    let reply = send_tls(
        https,
        "h3.example.com",
        tls_client_config(&[&cert], &[b"http/1.1"]),
        request("h3.example.com", "/").body(empty_body()).expect("a request"),
    )
    .await;
    assert_eq!(reply.header("alt-svc"), Some(expected.as_str()));
}

#[tokio::test(flavor = "multi_thread")]
async fn alt_svc_is_absent_when_http3_is_off() {
    // The whole point of advertising is that a client believes it. An
    // advertisement with no UDP socket behind it costs every such client a
    // failed QUIC attempt on every connection until it expires.
    let upstream = spawn_echo("origin").await;
    let cert = TestCert::generate(&["h3.example.com"]);
    let proxy = TestProxy::start_with(
        table("h3.example.com", &[upstream], CERT_ID),
        ProxyOptions {
            tls: true,
            http3: false,
            certs: cert_store(&[(CERT_ID, &cert)]),
            ..ProxyOptions::default()
        },
    )
    .await;
    assert!(proxy.http3.is_none(), "no QUIC port should have been bound");

    let (reply, _) = send_tls_h2(
        proxy.https.expect("a TLS port"),
        "h3.example.com",
        tls_client_config(&[&cert], &[b"h2"]),
        request("h3.example.com", "/").body(empty_body()).expect("a request"),
    )
    .await;
    assert_eq!(reply.header("alt-svc"), None);
}

#[tokio::test(flavor = "multi_thread")]
async fn alt_svc_is_never_sent_on_the_plaintext_listener() {
    // `alt-svc` points a client at QUIC for the *same authority*. Sent over
    // `http://`, it would be telling a plaintext client to move to a port that
    // only speaks TLS.
    let upstream = spawn_echo("origin").await;
    let (proxy, _cert) = h3_proxy("h3.example.com", upstream).await;

    let reply = get(proxy.http, "h3.example.com", "/").await;
    assert_eq!(reply.status, StatusCode::OK);
    assert_eq!(reply.header("alt-svc"), None);
}

// ---------------------------------------------------------------------------
// Shutdown
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn an_in_flight_http3_request_finishes_during_a_graceful_shutdown() {
    // The QUIC half of the drain: after SIGTERM the endpoint stops accepting
    // and sends GOAWAY, and the requests already running get their answer.
    let upstream = spawn_raw(|mut stream| async move {
        let _ = common::read_head(&mut stream).await;
        tokio::time::sleep(Duration::from_millis(500)).await;
        let _ = stream
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 9\r\n\r\nfinished!")
            .await;
    })
    .await;

    let upstream_addr = upstream;
    let cert = TestCert::generate(&["h3.example.com"]);
    let proxy = TestProxy::start_with(
        table("h3.example.com", &[upstream_addr], CERT_ID),
        ProxyOptions {
            tls: true,
            http3: true,
            certs: cert_store(&[(CERT_ID, &cert)]),
            grace: Duration::from_secs(10),
            ..ProxyOptions::default()
        },
    )
    .await;
    let quic = proxy.http3.expect("a QUIC port");

    let mut client = H3Client::connect(quic, "h3.example.com", &[&cert]).await;
    let request = Request::builder()
        .method("GET")
        .uri("https://h3.example.com/slow")
        .body(())
        .expect("a request");
    let mut stream = client.send.send_request(request).await.expect("a stream");
    stream.finish().await.expect("finish");

    // The upstream is holding the request; shut the proxy down underneath it.
    tokio::time::sleep(Duration::from_millis(100)).await;
    proxy.signal_shutdown();

    let reply = collect(&mut stream).await;
    assert_eq!(reply.status, StatusCode::OK);
    assert_eq!(reply.text(), "finished!");

    // And the drain reports success rather than the grace period expiring.
    let outcome = tokio::time::timeout(Duration::from_secs(10), proxy.wait())
        .await
        .expect("the server should stop inside the grace period");
    assert!(outcome.is_ok(), "{outcome:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn the_quic_port_stops_answering_once_the_proxy_has_shut_down() {
    let upstream = spawn_echo("origin").await;
    let (proxy, cert) = h3_proxy("h3.example.com", upstream).await;
    let quic = proxy.http3.expect("a QUIC port");

    let mut client = H3Client::connect(quic, "h3.example.com", &[&cert]).await;
    assert_eq!(
        client.get("h3.example.com", "/").await.status,
        StatusCode::OK
    );
    drop(client);

    proxy.shutdown().await.expect("a clean drain");

    // Nothing is listening any more, so the handshake cannot complete. quinn
    // retries into its idle timeout rather than failing fast, so this is
    // bounded rather than awaited.
    let refused = tokio::time::timeout(
        Duration::from_secs(3),
        H3Client::try_connect(quic, "h3.example.com", &[&cert]),
    )
    .await;
    // `Ok(Err(..))` is a refusal and the outer `Err` is the wait timing out;
    // either means nothing is serving. Only a completed handshake is a failure.
    assert!(
        !matches!(refused, Ok(Ok(_))),
        "a shut-down endpoint should not accept a new connection"
    );
}

// ---------------------------------------------------------------------------
// The plain TCP path, unchanged
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn turning_http3_on_does_not_disturb_the_tcp_listeners() {
    let upstream = spawn_echo("origin").await;
    let (proxy, cert) = h3_proxy("h3.example.com", upstream).await;

    // Plaintext.
    let reply = get(proxy.http, "h3.example.com", "/one").await;
    assert_eq!(reply.status, StatusCode::OK);
    assert_eq!(reply.text(), "GET /one");

    // TLS, over the same certificates the QUIC listener resolves through.
    let reply = send_tls(
        proxy.https.expect("a TLS port"),
        "h3.example.com",
        tls_client_config(&[&cert], &[b"http/1.1"]),
        request("h3.example.com", "/two")
            .body(empty_body())
            .expect("a request"),
    )
    .await;
    assert_eq!(reply.status, StatusCode::OK);
    assert_eq!(reply.text(), "GET /two");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_proxy_with_no_tls_listener_can_still_serve_quic() {
    // The library allows it — QUIC terminates its own TLS and needs no TCP
    // listener to do it. `ramjet-ingressd` refuses the combination anyway,
    // because there the UDP port is defined as the TLS listener's, and there
    // would be no response left to carry the `alt-svc` that advertises it.
    let upstream = spawn_echo("origin").await;
    let cert = TestCert::generate(&["h3.example.com"]);
    let proxy = TestProxy::start_with(
        table("h3.example.com", &[upstream], CERT_ID),
        ProxyOptions {
            tls: false,
            http3: true,
            certs: cert_store(&[(CERT_ID, &cert)]),
            ..ProxyOptions::default()
        },
    )
    .await;
    assert!(proxy.https.is_none());

    let quic = proxy.http3.expect("a QUIC port");
    let mut client = H3Client::connect(quic, "h3.example.com", &[&cert]).await;
    assert_eq!(
        client.get("h3.example.com", "/").await.status,
        StatusCode::OK
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_route_table_published_mid_connection_is_picked_up() {
    // One snapshot per request, not per connection — and an HTTP/3 connection
    // is exactly the case that makes the difference visible, because it is
    // long-lived and multiplexed.
    let first = spawn_echo("first").await;
    let second = spawn_echo("second").await;
    let (proxy, cert) = h3_proxy("h3.example.com", first).await;
    let quic = proxy.http3.expect("a QUIC port");

    let mut client = H3Client::connect(quic, "h3.example.com", &[&cert]).await;
    assert_eq!(
        client.get("h3.example.com", "/").await.header("x-upstream"),
        Some("first")
    );

    proxy
        .routes
        .store_shared(Arc::new(table("h3.example.com", &[second], CERT_ID)));

    assert_eq!(
        client.get("h3.example.com", "/").await.header("x-upstream"),
        Some("second"),
        "the same connection must serve the newly published table"
    );
}
