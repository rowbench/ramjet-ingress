//! Cleartext HTTP/2 upstreams, and the gRPC that rides on them.
//!
//! Every upstream here is a real hyper h2 server on a real socket speaking the
//! preface, and every client is a real hyper client. Nothing is mocked, for the
//! usual reason: what could be wrong is framing, version translation, authority
//! handling and trailer relay, and none of those exist in a mock.
//!
//! # What a gRPC test is, without tonic
//!
//! gRPC is HTTP/2 plus a convention. A unary call is a `POST` with
//! `content-type: application/grpc`, a length-prefixed message in the body, and
//! a `grpc-status` **trailer** carrying the outcome. The protobuf is opaque to
//! every hop between the two endpoints, this proxy included, so a test that
//! serialises one would be testing prost rather than this crate. What these
//! tests assert on instead is the part a proxy can break: that the content type
//! does not trip a refusal, that the body arrives byte for byte, and above all
//! that the trailer survives — because a gRPC call whose trailer is dropped
//! looks to the caller like a call that never completed.

mod common;

use std::convert::Infallible;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use bytes::Bytes;
use common::*;
use http::{HeaderMap, HeaderValue, Method, Request, Response, StatusCode, Version};
use http_body::{Body, Frame};
use http_body_util::BodyExt;
use hyper::body::Incoming;
use ramjet_router::{
    BackendOptions, BackendProtocol, Endpoint, LbPolicy, PathType, RouteTable, RouteTableBuilder,
};
use tokio::sync::mpsc;

// ---------------------------------------------------------------------------
// Bodies that do what a gRPC body does
// ---------------------------------------------------------------------------

/// One data chunk followed by a trailing header block.
///
/// The shape of a unary gRPC response: a message, then `grpc-status`.
struct WithTrailers {
    data: Option<Bytes>,
    trailers: Option<HeaderMap>,
}

impl Body for WithTrailers {
    type Data = Bytes;
    type Error = Infallible;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Infallible>>> {
        if let Some(data) = self.data.take() {
            return Poll::Ready(Some(Ok(Frame::data(data))));
        }
        Poll::Ready(self.trailers.take().map(|t| Ok(Frame::trailers(t))))
    }
}

fn with_trailers(data: &'static [u8], trailers: &[(&str, &str)]) -> TestBody {
    let mut map = HeaderMap::new();
    for (name, value) in trailers {
        map.insert(
            http::HeaderName::from_bytes(name.as_bytes()).expect("a header name"),
            HeaderValue::from_str(value).expect("a header value"),
        );
    }
    WithTrailers {
        data: Some(Bytes::from_static(data)),
        trailers: Some(map),
    }
    .boxed()
}

/// A body fed frame by frame from somewhere else, for the streaming tests.
///
/// This is what makes "interleaved" observable: the sender decides when each
/// frame exists, so a test can prove the proxy moved one before the next was
/// written rather than collecting them all and flushing at the end.
struct ChannelBody {
    rx: mpsc::UnboundedReceiver<Frame<Bytes>>,
}

impl Body for ChannelBody {
    type Data = Bytes;
    type Error = Infallible;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Infallible>>> {
        self.rx.poll_recv(cx).map(|frame| frame.map(Ok))
    }
}

fn channel_body() -> (mpsc::UnboundedSender<Frame<Bytes>>, TestBody) {
    let (tx, rx) = mpsc::unbounded_channel();
    (tx, ChannelBody { rx }.boxed())
}

// ---------------------------------------------------------------------------
// Route tables
// ---------------------------------------------------------------------------

/// One host, one prefix rule, one backend reached over the named protocol.
fn table_with(protocol: BackendProtocol, endpoints: &[std::net::SocketAddr]) -> RouteTable {
    let mut builder = RouteTableBuilder::new();
    builder
        .backend_with(
            "app",
            endpoints.iter().copied().map(Endpoint::new).collect(),
            &BackendOptions {
                policy: LbPolicy::RoundRobin,
                protocol,
            },
        )
        .expect("registers a backend");
    builder
        .route(Some("app.example.com"), "/", PathType::Prefix, "app")
        .expect("registers a route");
    builder.build().expect("a valid table")
}

/// A gRPC-shaped request: `POST`, the gRPC content type, and a framed message.
fn grpc_request(path: &str, message: &'static [u8]) -> Request<TestBody> {
    request("app.example.com", path)
        .method(Method::POST)
        .header("content-type", "application/grpc+proto")
        .header("te", "trailers")
        .body(full(message))
        .expect("a request")
}

/// An h2c upstream that answers every request the way a unary gRPC server does.
async fn spawn_grpc_upstream() -> std::net::SocketAddr {
    spawn_h2c(|request: Request<Incoming>| async move {
        let path = request.uri().path().to_owned();
        let authority = request
            .uri()
            .authority()
            .map(|a| a.as_str().to_owned())
            .unwrap_or_default();
        let had_host = request.headers().contains_key(http::header::HOST);
        let body = request.into_body().collect().await.expect("a body").to_bytes();

        let mut response = Response::new(with_trailers(
            b"\x00\x00\x00\x00\x05reply",
            &[("grpc-status", "0"), ("grpc-message", "OK")],
        ));
        let headers = response.headers_mut();
        headers.insert("content-type", HeaderValue::from_static("application/grpc"));
        headers.insert("x-upstream", HeaderValue::from_static("grpc"));
        headers.insert("echo-path", HeaderValue::from_str(&path).expect("a path"));
        headers.insert(
            "echo-authority",
            HeaderValue::from_str(&authority).unwrap_or(HeaderValue::from_static("")),
        );
        headers.insert(
            "echo-had-host",
            HeaderValue::from_static(if had_host { "yes" } else { "no" }),
        );
        headers.insert(
            "echo-body-len",
            HeaderValue::from_str(&body.len().to_string()).expect("a length"),
        );
        response
    })
    .await
}

// ---------------------------------------------------------------------------
// The four version combinations
// ---------------------------------------------------------------------------

#[tokio::test]
async fn an_h2_client_reaches_an_h2c_backend_with_its_trailers_intact() {
    // The combination this whole feature exists for, end to end: h2 in, h2 out,
    // and `grpc-status` arriving after the body on both sides.
    let upstream = spawn_grpc_upstream().await;
    let proxy = TestProxy::start(table_with(BackendProtocol::H2c, &[upstream])).await;

    let reply = send_h2c(proxy.http, grpc_request("/pkg.Svc/Method", b"\x00\x00\x00\x00\x05hello"))
        .await;

    assert_eq!(reply.status, StatusCode::OK);
    assert_eq!(reply.upstream(), "grpc");
    assert_eq!(reply.header("echo-path"), Some("/pkg.Svc/Method"));
    assert_eq!(&reply.body[..], b"\x00\x00\x00\x00\x05reply");
    assert_eq!(
        reply.trailer("grpc-status"),
        Some("0"),
        "the status trailer is the result of the call; losing it loses the call"
    );
    assert_eq!(reply.trailer("grpc-message"), Some("OK"));

    proxy.shutdown().await.expect("clean shutdown");
}

#[tokio::test]
async fn an_http1_client_reaches_an_h2c_backend() {
    // Version translation upward. The client speaks HTTP/1.1 and knows nothing
    // about h2; the backend is dialled with the preface all the same.
    let upstream = spawn_grpc_upstream().await;
    let proxy = TestProxy::start(table_with(BackendProtocol::H2c, &[upstream])).await;

    let reply = send(
        proxy.http,
        request("app.example.com", "/pkg.Svc/Method")
            .method(Method::POST)
            .body(full("a plain body"))
            .expect("a request"),
    )
    .await;

    assert_eq!(reply.status, StatusCode::OK);
    assert_eq!(reply.upstream(), "grpc");
    assert_eq!(reply.header("echo-body-len"), Some("12"));

    proxy.shutdown().await.expect("clean shutdown");
}

#[tokio::test]
async fn an_h2c_client_reaches_an_http1_backend() {
    // Version translation downward, which is the path that already existed —
    // pinned here because the two directions now share one function and a change
    // to either could break the other.
    let upstream = spawn_echo("h1").await;
    let proxy = TestProxy::start(table_with(BackendProtocol::Http1, &[upstream])).await;

    let reply = send_h2c(
        proxy.http,
        request("app.example.com", "/plain").body(empty_body()).expect("a request"),
    )
    .await;

    assert_eq!(reply.status, StatusCode::OK);
    assert_eq!(reply.upstream(), "h1");
    // The HTTP/2 request had no `Host` header at all; the proxy has to
    // reconstruct one from `:authority` or the backend sees the endpoint's
    // `ip:port` as its own name.
    assert_eq!(reply.header("echo-host"), Some("app.example.com"));

    proxy.shutdown().await.expect("clean shutdown");
}

#[tokio::test]
async fn an_http1_client_still_reaches_an_http1_backend() {
    let upstream = spawn_echo("h1").await;
    let proxy = TestProxy::start(table_with(BackendProtocol::Http1, &[upstream])).await;

    let reply = get(proxy.http, "app.example.com", "/plain").await;
    assert_eq!(reply.status, StatusCode::OK);
    assert_eq!(reply.upstream(), "h1");

    proxy.shutdown().await.expect("clean shutdown");
}

// ---------------------------------------------------------------------------
// The authority rewrite
// ---------------------------------------------------------------------------

#[tokio::test]
async fn an_h2c_backend_sees_one_authority_and_no_host_header() {
    // RFC 9113 §8.3.1 lets a server treat a request as malformed when `Host`
    // disagrees with `:authority`, and `:authority` has to be the endpoint
    // because that is what keys the connection pool. So `Host` goes, and
    // `X-Forwarded-Host` is where the client's name survives.
    let upstream = spawn_grpc_upstream().await;
    let proxy = TestProxy::start(table_with(BackendProtocol::H2c, &[upstream])).await;

    let reply = send(
        proxy.http,
        request("app.example.com", "/rpc").body(empty_body()).expect("a request"),
    )
    .await;

    assert_eq!(reply.header("echo-had-host"), Some("no"));
    assert_eq!(
        reply.header("echo-authority"),
        Some(upstream.to_string().as_str()),
        "`:authority` names the endpoint the request was pooled to"
    );

    proxy.shutdown().await.expect("clean shutdown");
}

#[tokio::test]
async fn the_client_host_survives_as_x_forwarded_host_on_an_h2c_backend() {
    let upstream = spawn_h2c(|request: Request<Incoming>| async move {
        let forwarded = request
            .headers()
            .get("x-forwarded-host")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("<none>")
            .to_owned();
        let mut response = Response::new(full(forwarded));
        response
            .headers_mut()
            .insert("x-upstream", HeaderValue::from_static("h2c"));
        response
    })
    .await;
    let proxy = TestProxy::start(table_with(BackendProtocol::H2c, &[upstream])).await;

    let reply = get(proxy.http, "app.example.com", "/rpc").await;
    assert_eq!(reply.text(), "app.example.com");

    proxy.shutdown().await.expect("clean shutdown");
}

// ---------------------------------------------------------------------------
// gRPC at an HTTP/1.1 backend is still refused
// ---------------------------------------------------------------------------

#[tokio::test]
async fn grpc_at_an_http1_backend_is_refused_and_the_body_names_the_annotation() {
    // The refusal that used to cover every gRPC request now covers exactly one
    // case, and it has to say how to leave it.
    let upstream = spawn_echo("h1").await;
    let proxy = TestProxy::start(table_with(BackendProtocol::Http1, &[upstream])).await;

    let reply = send(proxy.http, grpc_request("/pkg.Svc/Method", b"\x00")).await;

    assert_eq!(reply.status, StatusCode::BAD_GATEWAY);
    assert!(
        reply.text().contains("backend-protocol: GRPC"),
        "the 502 must name the fix, got: {}",
        reply.text()
    );
    assert_eq!(
        reply.header("x-upstream"),
        None,
        "the request must not have reached the HTTP/1.1 backend at all"
    );

    proxy.shutdown().await.expect("clean shutdown");
}

#[tokio::test]
async fn a_non_grpc_request_to_an_http1_backend_is_untouched_by_the_refusal() {
    let upstream = spawn_echo("h1").await;
    let proxy = TestProxy::start(table_with(BackendProtocol::Http1, &[upstream])).await;

    let reply = send(
        proxy.http,
        request("app.example.com", "/api")
            .method(Method::POST)
            .header("content-type", "application/json")
            .body(full("{}"))
            .expect("a request"),
    )
    .await;

    assert_eq!(reply.status, StatusCode::OK);
    proxy.shutdown().await.expect("clean shutdown");
}

// ---------------------------------------------------------------------------
// Streaming
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_bidirectional_stream_interleaves_in_both_directions() {
    // The property that separates a streaming proxy from a buffering one: the
    // upstream answers each request frame as it arrives, so a reply for frame
    // *n* reaches the client while the client has not yet sent frame *n+1*. A
    // proxy that collected either side would deadlock here rather than fail an
    // assertion, which is why the test drives the two halves in lockstep.
    let upstream = spawn_h2c(|request: Request<Incoming>| async move {
        let (tx, body) = channel_body();
        tokio::spawn(async move {
            let mut incoming = request.into_body();
            while let Some(Ok(frame)) = incoming.frame().await {
                if let Ok(data) = frame.into_data() {
                    let mut echoed = Vec::with_capacity(data.len() + 4);
                    echoed.extend_from_slice(b"re:");
                    echoed.extend_from_slice(&data);
                    if tx.send(Frame::data(Bytes::from(echoed))).is_err() {
                        return;
                    }
                }
            }
            let mut trailers = HeaderMap::new();
            trailers.insert("grpc-status", HeaderValue::from_static("0"));
            let _ = tx.send(Frame::trailers(trailers));
        });

        let mut response = Response::new(body);
        response
            .headers_mut()
            .insert("x-upstream", HeaderValue::from_static("stream"));
        response
    })
    .await;
    let proxy = TestProxy::start(table_with(BackendProtocol::H2c, &[upstream])).await;

    let (mut sender, connection) = handshake_h2c(proxy.http).await;
    let driver = tokio::spawn(connection);

    let (requests, request_body) = channel_body();
    let response = sender
        .send_request(
            request("app.example.com", "/pkg.Svc/Chat")
                .method(Method::POST)
                .header("content-type", "application/grpc")
                .body(request_body)
                .expect("a request"),
        )
        .await
        .expect("response headers before the request body ends");

    assert_eq!(response.status(), StatusCode::OK);
    let mut incoming = response.into_body();

    // Lockstep: send one, read one. If anything on the path buffered a whole
    // side, the first read would never resolve.
    let mut received = Vec::new();
    for i in 0..5u8 {
        let message = Bytes::from(format!("msg{i}"));
        requests.send(Frame::data(message)).expect("the stream is open");
        let frame = tokio::time::timeout(Duration::from_secs(5), incoming.frame())
            .await
            .expect("a reply arrived before the request stream ended")
            .expect("a frame")
            .expect("a good frame");
        let data = frame.into_data().expect("a data frame");
        received.push(String::from_utf8_lossy(&data).into_owned());
    }
    drop(requests);

    assert_eq!(
        received,
        vec!["re:msg0", "re:msg1", "re:msg2", "re:msg3", "re:msg4"],
        "every message came back, in order, one at a time"
    );

    // Whatever is left is the trailer block that ends the call.
    let mut status = None;
    while let Some(Ok(frame)) = incoming.frame().await {
        if let Ok(trailers) = frame.into_trailers() {
            status = trailers.get("grpc-status").and_then(|v| v.to_str().ok()).map(str::to_owned);
        }
    }
    assert_eq!(status.as_deref(), Some("0"), "the call was completed, not abandoned");

    driver.abort();
    proxy.shutdown().await.expect("clean shutdown");
}

#[tokio::test]
async fn request_trailers_reach_an_h2c_backend() {
    // The direction the response tests do not cover. A gRPC client streaming a
    // request ends it with a trailer block too, and dropping it truncates the
    // call from the server's point of view.
    let upstream = spawn_h2c(|request: Request<Incoming>| async move {
        let collected = request.into_body().collect().await.expect("a body");
        let seen = collected
            .trailers()
            .and_then(|t| t.get("x-request-trailer"))
            .and_then(|v| v.to_str().ok())
            .unwrap_or("<none>")
            .to_owned();
        Response::new(full(seen))
    })
    .await;
    let proxy = TestProxy::start(table_with(BackendProtocol::H2c, &[upstream])).await;

    let mut trailers = HeaderMap::new();
    trailers.insert("x-request-trailer", HeaderValue::from_static("sent"));
    let body = WithTrailers {
        data: Some(Bytes::from_static(b"payload")),
        trailers: Some(trailers),
    }
    .boxed();

    let reply = send_h2c(
        proxy.http,
        request("app.example.com", "/rpc")
            .method(Method::POST)
            .body(body)
            .expect("a request"),
    )
    .await;

    assert_eq!(reply.text(), "sent");
    proxy.shutdown().await.expect("clean shutdown");
}

#[tokio::test]
async fn a_large_body_moves_through_the_h2_upstream_without_being_collected() {
    // Four megabytes, well past hyper's default connection and stream windows,
    // so the transfer only completes if flow control is being propagated rather
    // than the whole body being held somewhere.
    const CHUNK: usize = 64 * 1024;
    const CHUNKS: usize = 64;

    let upstream = spawn_h2c(|request: Request<Incoming>| async move {
        let mut total = 0usize;
        let mut body = request.into_body();
        while let Some(Ok(frame)) = body.frame().await {
            if let Ok(data) = frame.into_data() {
                total += data.len();
            }
        }
        Response::new(full(total.to_string()))
    })
    .await;
    let proxy = TestProxy::start(table_with(BackendProtocol::H2c, &[upstream])).await;

    let (tx, body) = channel_body();
    tokio::spawn(async move {
        for _ in 0..CHUNKS {
            if tx.send(Frame::data(Bytes::from(vec![b'x'; CHUNK]))).is_err() {
                return;
            }
        }
    });

    let reply = send_h2c(
        proxy.http,
        request("app.example.com", "/upload")
            .method(Method::POST)
            .body(body)
            .expect("a request"),
    )
    .await;

    assert_eq!(reply.text(), (CHUNK * CHUNKS).to_string());
    proxy.shutdown().await.expect("clean shutdown");
}

#[tokio::test]
async fn a_large_response_streams_back_from_an_h2c_backend() {
    const CHUNK: usize = 64 * 1024;
    const CHUNKS: usize = 64;

    let upstream = spawn_h2c(|_request: Request<Incoming>| async move {
        let (tx, body) = channel_body();
        tokio::spawn(async move {
            for _ in 0..CHUNKS {
                if tx.send(Frame::data(Bytes::from(vec![b'y'; CHUNK]))).is_err() {
                    return;
                }
            }
        });
        Response::new(body)
    })
    .await;
    let proxy = TestProxy::start(table_with(BackendProtocol::H2c, &[upstream])).await;

    let reply = send_h2c(
        proxy.http,
        request("app.example.com", "/download").body(empty_body()).expect("a request"),
    )
    .await;

    assert_eq!(reply.body.len(), CHUNK * CHUNKS);
    proxy.shutdown().await.expect("clean shutdown");
}

// ---------------------------------------------------------------------------
// Failover and errors
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_dead_h2c_endpoint_fails_over_to_a_live_one() {
    // Connect-level failover is protocol-independent by construction — nothing
    // was written either way — but "by construction" is worth one test, because
    // the h2 client reports its errors through a different path.
    let dead = dead_addr().await;
    let live = spawn_grpc_upstream().await;
    let proxy = TestProxy::start(table_with(BackendProtocol::H2c, &[dead, live])).await;

    let reply = get(proxy.http, "app.example.com", "/rpc").await;

    assert_eq!(reply.status, StatusCode::OK);
    assert_eq!(reply.upstream(), "grpc");
    assert_eq!(proxy.metrics.totals().connect_failures, 1);
    assert_eq!(proxy.metrics.retries(), 1);

    proxy.shutdown().await.expect("clean shutdown");
}

#[tokio::test]
async fn every_dead_h2c_endpoint_ends_in_a_502() {
    let dead = dead_addr().await;
    let proxy = TestProxy::start(table_with(BackendProtocol::H2c, &[dead])).await;

    let reply = get(proxy.http, "app.example.com", "/rpc").await;
    assert_eq!(reply.status, StatusCode::BAD_GATEWAY);
    assert!(reply.text().starts_with("502"), "{}", reply.text());

    proxy.shutdown().await.expect("clean shutdown");
}

#[tokio::test]
async fn an_h2c_backend_with_no_endpoints_answers_503() {
    let proxy = TestProxy::start(table_with(BackendProtocol::H2c, &[])).await;
    let reply = get(proxy.http, "app.example.com", "/rpc").await;
    assert_eq!(reply.status, StatusCode::SERVICE_UNAVAILABLE);
    proxy.shutdown().await.expect("clean shutdown");
}

#[tokio::test]
async fn a_request_with_a_body_gets_one_attempt_at_an_h2c_backend() {
    // The same rule the HTTP/1.1 path follows, and for the same reason: nothing
    // buffers the body, so it cannot be sent twice. The dead endpoint is first,
    // so a failover would have produced a 200.
    let dead = dead_addr().await;
    let live = spawn_grpc_upstream().await;
    let proxy = TestProxy::start(table_with(BackendProtocol::H2c, &[dead, live])).await;

    let reply = send(
        proxy.http,
        request("app.example.com", "/rpc")
            .method(Method::POST)
            .body(full("a body that cannot be replayed"))
            .expect("a request"),
    )
    .await;

    assert_eq!(reply.status, StatusCode::BAD_GATEWAY);
    assert_eq!(proxy.metrics.retries(), 0, "a body-carrying request is not re-dispatched");

    proxy.shutdown().await.expect("clean shutdown");
}

// ---------------------------------------------------------------------------
// Pooling
// ---------------------------------------------------------------------------

#[tokio::test]
async fn concurrent_requests_share_one_h2c_connection_to_an_endpoint() {
    // The reason h2 upstreams are worth having: streams multiplex, so twenty
    // concurrent requests are twenty streams on one socket rather than twenty
    // sockets. Counted at the upstream's accept loop, which is the only place
    // that can tell the difference.
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    let accepted = Arc::new(AtomicUsize::new(0));
    let listener = tokio::net::TcpListener::bind(std::net::SocketAddr::from(([127, 0, 0, 1], 0)))
        .await
        .expect("bind");
    let upstream = listener.local_addr().expect("addr");
    {
        let accepted = Arc::clone(&accepted);
        tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    return;
                };
                accepted.fetch_add(1, Ordering::SeqCst);
                tokio::spawn(async move {
                    let service = hyper::service::service_fn(|_req| async {
                        // A little work, so the requests genuinely overlap
                        // rather than being served one at a time by accident.
                        tokio::time::sleep(Duration::from_millis(50)).await;
                        Ok::<_, Infallible>(Response::new(full("ok")))
                    });
                    let _ = hyper::server::conn::http2::Builder::new(
                        hyper_util::rt::TokioExecutor::new(),
                    )
                    .serve_connection(hyper_util::rt::TokioIo::new(stream), service)
                    .await;
                });
            }
        });
    }

    let proxy = TestProxy::start(table_with(BackendProtocol::H2c, &[upstream])).await;

    let (mut sender, connection) = handshake_h2c(proxy.http).await;
    let driver = tokio::spawn(connection);
    let mut pending = Vec::new();
    for _ in 0..20 {
        let request = request("app.example.com", "/rpc").body(empty_body()).expect("a request");
        pending.push(sender.send_request(request));
    }
    for future in pending {
        let response = future.await.expect("a response");
        assert_eq!(response.status(), StatusCode::OK);
    }
    driver.abort();

    assert_eq!(
        accepted.load(Ordering::SeqCst),
        1,
        "twenty concurrent requests should be twenty streams on one connection"
    );

    proxy.shutdown().await.expect("clean shutdown");
}

// ---------------------------------------------------------------------------
// Version on the wire
// ---------------------------------------------------------------------------

#[tokio::test]
async fn the_upstream_sees_http2_whatever_the_client_spoke() {
    let upstream = spawn_h2c(|request: Request<Incoming>| async move {
        let version = format!("{:?}", request.version());
        Response::new(full(version))
    })
    .await;
    let proxy = TestProxy::start(table_with(BackendProtocol::H2c, &[upstream])).await;

    let from_h1 = get(proxy.http, "app.example.com", "/rpc").await;
    assert_eq!(from_h1.text(), format!("{:?}", Version::HTTP_2));

    let from_h2 = send_h2c(
        proxy.http,
        request("app.example.com", "/rpc").body(empty_body()).expect("a request"),
    )
    .await;
    assert_eq!(from_h2.text(), format!("{:?}", Version::HTTP_2));

    proxy.shutdown().await.expect("clean shutdown");
}

// ---------------------------------------------------------------------------
// Visibility
// ---------------------------------------------------------------------------

#[tokio::test]
async fn admin_routes_report_which_protocol_a_backend_is_dialled_with() {
    // The operator-facing half of the feature: after annotating an Ingress,
    // this is how you confirm the running pod agrees with you.
    let upstream = spawn_grpc_upstream().await;
    let proxy = TestProxy::start(table_with(BackendProtocol::H2c, &[upstream])).await;

    let reply = get(proxy.admin, "admin", "/admin/routes").await;
    assert_eq!(reply.status, StatusCode::OK);
    let body: serde_json::Value = serde_json::from_slice(&reply.body).expect("json");
    let routes = body["routes"].as_array().expect("an array");
    assert_eq!(routes.len(), 1);
    assert_eq!(routes[0]["protocol"], "h2c");

    proxy.shutdown().await.expect("clean shutdown");
}

#[tokio::test]
async fn per_route_counters_move_for_an_h2c_backend_like_any_other() {
    // Nothing about the counters is protocol-aware, which is the point: adding
    // an upstream protocol should not have created a second accounting path.
    let upstream = spawn_grpc_upstream().await;
    let proxy = TestProxy::start(table_with(BackendProtocol::H2c, &[upstream])).await;

    for _ in 0..3 {
        assert_eq!(get(proxy.http, "app.example.com", "/rpc").await.status, StatusCode::OK);
    }

    let reply = get(proxy.admin, "admin", "/admin/routes").await;
    let body: serde_json::Value = serde_json::from_slice(&reply.body).expect("json");
    assert_eq!(body["routes"][0]["requests_total"], 3);
    assert_eq!(body["routes"][0]["errors_5xx_total"], 0);

    proxy.shutdown().await.expect("clean shutdown");
}

#[tokio::test]
async fn the_admin_listener_answers_prior_knowledge_h2c() {
    // Not a feature so much as a fact worth pinning: the admin listener is built
    // on the same protocol-detecting builder the traffic listener is, so it
    // speaks h2c. `deploy/e2e.sh` uses that to get a real HTTP/2 backend inside
    // the cluster without pulling a second image — it routes an h2c-annotated
    // Ingress at the controller's own admin Service. If this stops being true,
    // that assertion turns into a mystery 502 in CI.
    let upstream = spawn_echo("h1").await;
    let proxy = TestProxy::start(table_with(BackendProtocol::Http1, &[upstream])).await;
    proxy.readiness.set_ready(true);

    let reply = send_h2c(
        proxy.admin,
        request("admin", "/readyz").body(empty_body()).expect("a request"),
    )
    .await;
    assert_eq!(reply.status, StatusCode::OK);

    proxy.shutdown().await.expect("clean shutdown");
}

#[tokio::test]
async fn an_h2c_backend_can_be_the_proxys_own_admin_listener() {
    // The exact shape `deploy/e2e.sh` builds, proved here on real sockets so the
    // e2e assertion is checking the cluster rather than debugging this.
    let echo = spawn_echo("h1").await;
    let proxy = TestProxy::start(table_with(BackendProtocol::Http1, &[echo])).await;
    proxy.readiness.set_ready(true);

    // Republish with the admin port as an h2c backend, now that it is bound.
    proxy
        .routes
        .store(table_with(BackendProtocol::H2c, &[proxy.admin]));

    let reply = get(proxy.http, "app.example.com", "/readyz").await;
    assert_eq!(reply.status, StatusCode::OK);
    assert_eq!(
        reply.text().trim(),
        "ready",
        "the h2c upstream answered the admin listener's readiness body"
    );

    proxy.shutdown().await.expect("clean shutdown");
}
