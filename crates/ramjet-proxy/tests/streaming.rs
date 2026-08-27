//! Bodies pass through frame by frame, not response by response.
//!
//! Buffering is invisible until it isn't: a proxy that collects the whole
//! response before forwarding it looks correct on every small request and then
//! turns a 4GB download into 4GB of pod memory. The way to catch it is to make
//! the upstream stop halfway and assert the client already has the first half.

mod common;

use std::time::{Duration, Instant};

use common::*;
use http::StatusCode;
use http_body_util::BodyExt;
use tokio::io::AsyncWriteExt;

/// How long the upstream stalls between the first chunk and the rest.
const STALL: Duration = Duration::from_millis(400);

#[tokio::test]
async fn the_first_chunk_arrives_before_the_upstream_has_finished() {
    let upstream = spawn_raw(|mut stream| async move {
        let _ = read_head(&mut stream).await;
        let _ = stream
            .write_all(
                b"HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nTransfer-Encoding: chunked\r\n\r\n",
            )
            .await;
        let _ = stream.write_all(b"5\r\nfirst\r\n").await;
        let _ = stream.flush().await;
        // The upstream deliberately goes quiet here. A proxy that buffers
        // cannot produce a byte downstream until this sleep is over.
        tokio::time::sleep(STALL).await;
        let _ = stream.write_all(b"4\r\nlast\r\n0\r\n\r\n").await;
        let _ = stream.flush().await;
        tokio::time::sleep(Duration::from_millis(200)).await;
    })
    .await;

    let proxy = TestProxy::start(single_route("stream.example.com", "/", &[upstream])).await;

    let started = Instant::now();
    let response = send_streaming(
        proxy.http,
        request("stream.example.com", "/")
            .body(empty_body())
            .expect("a request"),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);

    let mut body = response.into_body();
    let first = body
        .frame()
        .await
        .expect("a first frame")
        .expect("a readable frame");
    let time_to_first = started.elapsed();
    let first_bytes = first.into_data().expect("a data frame");

    let rest = body.collect().await.expect("the rest").to_bytes();
    let time_to_last = started.elapsed();

    assert_eq!(&first_bytes[..], b"first");
    assert_eq!(&rest[..], b"last");
    assert!(
        time_to_first < STALL / 2,
        "the first chunk took {time_to_first:?}, which means it was buffered"
    );
    assert!(
        time_to_last >= STALL,
        "the whole body arrived in {time_to_last:?}, before the upstream could have sent it"
    );
}

#[tokio::test]
async fn a_large_chunked_body_arrives_intact() {
    const CHUNK: usize = 16 * 1024;
    const CHUNKS: usize = 512; // 8 MiB

    let upstream = spawn_raw(|mut stream| async move {
        let _ = read_head(&mut stream).await;
        let _ = stream
            .write_all(b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n")
            .await;
        let header = format!("{CHUNK:x}\r\n");
        let payload = vec![b'x'; CHUNK];
        for _ in 0..CHUNKS {
            if stream.write_all(header.as_bytes()).await.is_err() {
                return;
            }
            if stream.write_all(&payload).await.is_err() {
                return;
            }
            if stream.write_all(b"\r\n").await.is_err() {
                return;
            }
        }
        let _ = stream.write_all(b"0\r\n\r\n").await;
        let _ = stream.flush().await;
        tokio::time::sleep(Duration::from_millis(200)).await;
    })
    .await;

    let proxy = TestProxy::start(single_route("big.example.com", "/", &[upstream])).await;
    let reply = get(proxy.http, "big.example.com", "/").await;

    assert_eq!(reply.status, StatusCode::OK);
    assert_eq!(reply.body.len(), CHUNK * CHUNKS);
    assert!(reply.body.iter().all(|byte| *byte == b'x'));
    // The upstream's `Transfer-Encoding` is stripped as hop-by-hop and hyper
    // re-decides the framing for this hop. The body has no known length, so it
    // picks chunked again -- but it is *this* connection's framing decision,
    // not a header forwarded from the last one.
    assert_eq!(reply.header("transfer-encoding"), Some("chunked"));
    assert_eq!(
        reply.header("content-length"),
        None,
        "a streamed body must not acquire a length it never had"
    );
}

#[tokio::test]
async fn a_large_request_body_reaches_the_upstream_intact() {
    const SIZE: usize = 4 * 1024 * 1024;

    let upstream = spawn_http(|request: http::Request<hyper::body::Incoming>| async move {
        let body = request.into_body().collect().await.expect("a body").to_bytes();
        let mismatched = body.iter().filter(|byte| **byte != b'y').count();
        http::Response::new(full(format!("{} {}", body.len(), mismatched)))
    })
    .await;

    let proxy = TestProxy::start(single_route("upload.example.com", "/", &[upstream])).await;

    let reply = send(
        proxy.http,
        request("upload.example.com", "/")
            .method("PUT")
            .body(full(vec![b'y'; SIZE]))
            .expect("a request"),
    )
    .await;

    assert_eq!(reply.status, StatusCode::OK);
    assert_eq!(reply.text(), format!("{SIZE} 0"));
}
