//! Protocol upgrades pass straight through.
//!
//! There is no WebSocket library here on purpose. Once a connection has been
//! upgraded there is no HTTP left to interpret, so what the proxy owes the two
//! peers is an honest byte pipe — and asserting on raw bytes tests exactly that
//! without a framing library in between to hide a mistake.
//!
//! The handshake is the part that can go wrong: `Connection` and `Upgrade` are
//! hop-by-hop headers, so the default behaviour strips them, and an upgrade only
//! works because [`headers::upgrade_protocol`] captures them before the strip and
//! puts them back afterwards.
//!
//! [`headers::upgrade_protocol`]: ramjet_proxy::headers::upgrade_protocol

mod common;

use std::time::Duration;

use common::*;
use http::{header, StatusCode};
use hyper_util::rt::TokioIo;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// An upstream that completes a WebSocket handshake and then echoes bytes.
async fn spawn_upgrade_echo() -> std::net::SocketAddr {
    spawn_raw(|mut stream| async move {
        let head = read_head(&mut stream).await.to_ascii_lowercase();
        if !head.contains("upgrade: websocket") || !head.contains("connection: upgrade") {
            let _ = stream.write_all(b"HTTP/1.1 400 Bad Request\r\ncontent-length: 0\r\n\r\n").await;
            return;
        }
        let accepted = concat!(
            "HTTP/1.1 101 Switching Protocols\r\n",
            "Connection: Upgrade\r\n",
            "Upgrade: websocket\r\n",
            "Sec-WebSocket-Accept: s3pPLMBiTxaQ9kYGzzhZRbK+xOo=\r\n",
            "\r\n",
        );
        if stream.write_all(accepted.as_bytes()).await.is_err() {
            return;
        }
        let _ = stream.flush().await;

        let mut buffer = [0u8; 4096];
        loop {
            match stream.read(&mut buffer).await {
                Ok(0) | Err(_) => return,
                Ok(read) => {
                    if stream.write_all(&buffer[..read]).await.is_err() {
                        return;
                    }
                    let _ = stream.flush().await;
                }
            }
        }
    })
    .await
}

fn upgrade_request(host: &str) -> http::Request<TestBody> {
    request(host, "/socket")
        .header(header::CONNECTION, "Upgrade")
        .header(header::UPGRADE, "websocket")
        .header("sec-websocket-version", "13")
        .header("sec-websocket-key", "dGhlIHNhbXBsZSBub25jZQ==")
        .body(empty_body())
        .expect("a request")
}

#[tokio::test]
async fn an_upgrade_is_negotiated_and_then_echoes_both_ways() {
    let upstream = spawn_upgrade_echo().await;
    let proxy = TestProxy::start(single_route("ws.example.com", "/", &[upstream])).await;

    let (mut sender, connection) = handshake(proxy.http).await;
    tokio::spawn(connection);

    let mut response = sender
        .send_request(upgrade_request("ws.example.com"))
        .await
        .expect("a response");

    assert_eq!(response.status(), StatusCode::SWITCHING_PROTOCOLS);
    assert_eq!(
        response
            .headers()
            .get(header::UPGRADE)
            .and_then(|v| v.to_str().ok()),
        Some("websocket"),
        "the upgrade headers must survive the hop-by-hop strip on a 101"
    );

    let upgraded = hyper::upgrade::on(&mut response)
        .await
        .expect("the client half of the tunnel");
    let mut tunnel = TokioIo::new(upgraded);

    // Two round trips, so a one-shot copy would fail the second.
    for message in [b"ping".as_slice(), b"pong".as_slice()] {
        tunnel.write_all(message).await.expect("write");
        tunnel.flush().await.expect("flush");
        let mut echoed = vec![0u8; message.len()];
        tokio::time::timeout(Duration::from_secs(5), tunnel.read_exact(&mut echoed))
            .await
            .expect("the echo arrived in time")
            .expect("read");
        assert_eq!(echoed, message);
    }
}

#[tokio::test]
async fn a_larger_payload_survives_the_tunnel() {
    let upstream = spawn_upgrade_echo().await;
    let proxy = TestProxy::start(single_route("ws.example.com", "/", &[upstream])).await;

    let (mut sender, connection) = handshake(proxy.http).await;
    tokio::spawn(connection);
    let mut response = sender
        .send_request(upgrade_request("ws.example.com"))
        .await
        .expect("a response");
    assert_eq!(response.status(), StatusCode::SWITCHING_PROTOCOLS);

    let upgraded = hyper::upgrade::on(&mut response).await.expect("the tunnel");
    let tunnel = TokioIo::new(upgraded);

    // Writing and reading concurrently, because 256KiB is larger than the
    // socket buffers: a sequential write would deadlock against the echo the
    // other side is trying to push back.
    let payload = vec![b'z'; 256 * 1024];
    let (mut reader, mut writer) = tokio::io::split(tunnel);
    let sent = payload.clone();
    let writer_task = tokio::spawn(async move {
        writer.write_all(&sent).await.expect("write");
        writer.flush().await.expect("flush");
    });

    let mut received = vec![0u8; payload.len()];
    tokio::time::timeout(Duration::from_secs(10), reader.read_exact(&mut received))
        .await
        .expect("the echo arrived in time")
        .expect("read");
    writer_task.await.expect("the writer finished");

    assert_eq!(received, payload);
}

#[tokio::test]
async fn an_upstream_that_refuses_the_upgrade_is_reported_normally() {
    // A backend that does not speak WebSocket answers with an ordinary status;
    // the proxy must pass that through rather than inventing a tunnel.
    let upstream = spawn_echo("plain").await;
    let proxy = TestProxy::start(single_route("ws.example.com", "/", &[upstream])).await;

    let reply = send(proxy.http, upgrade_request("ws.example.com")).await;
    assert_eq!(reply.status, StatusCode::OK);
    assert_eq!(reply.upstream(), "plain");
    assert_eq!(
        reply.header("echo-upgrade"),
        Some("websocket"),
        "an upgrade request must reach the upstream with its headers intact"
    );
}
