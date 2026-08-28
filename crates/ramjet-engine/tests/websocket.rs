//! Protocol upgrades, carried rather than refused.
//!
//! What is tested is deliberately *not* WebSocket. A passthrough tunnel does no
//! frame parsing — after a 101 the two endpoints have agreed on something this
//! hop is not party to — so the upstream here echoes bytes and the assertions
//! are about bytes. A test that spoke real WebSocket would be testing a codec
//! that is not in the request path.
//!
//! The handshake itself is real: `Connection: Upgrade` and `Upgrade:
//! websocket` have to survive a hop that strips both as hop-by-hop headers, and
//! getting that wrong turns every WebSocket into a plain 200.

mod common;

use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::Duration;

use common::*;

/// The handshake a browser sends, minus the parts no proxy looks at.
fn upgrade_request(host: &str) -> String {
    format!(
        "GET /socket HTTP/1.1\r\nHost: {host}\r\nConnection: Upgrade\r\n\
         Upgrade: websocket\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\
         Sec-WebSocket-Version: 13\r\n\r\n"
    )
}

/// Read until `wanted` bytes have arrived, or give up.
fn read_exactly(stream: &mut impl Read, wanted: usize) -> Vec<u8> {
    let mut seen = Vec::new();
    let mut chunk = [0u8; 4096];
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while seen.len() < wanted && std::time::Instant::now() < deadline {
        match stream.read(&mut chunk) {
            Ok(0) => break,
            Ok(n) => seen.extend_from_slice(&chunk[..n]),
            Err(_) => break,
        }
    }
    seen
}

/// Read one response head, leaving anything after it in the returned tail.
fn read_head(stream: &mut impl Read) -> (String, Vec<u8>) {
    let mut seen = Vec::new();
    let mut chunk = [0u8; 4096];
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while std::time::Instant::now() < deadline {
        if let Some(at) = seen
            .windows(4)
            .position(|w| w == b"\r\n\r\n")
            .map(|at| at + 4)
        {
            let head = String::from_utf8_lossy(&seen[..at]).to_string();
            return (head, seen[at..].to_vec());
        }
        match stream.read(&mut chunk) {
            Ok(0) => break,
            Ok(n) => seen.extend_from_slice(&chunk[..n]),
            Err(_) => break,
        }
    }
    panic!(
        "no response head arrived: {:?}",
        String::from_utf8_lossy(&seen)
    );
}

#[test]
fn an_upgrade_is_forwarded_and_the_101_comes_back() {
    let upstream = spawn(Behaviour::UpgradeEcho);
    let proxy = Proxy::start(table_for("app.example.com", &[upstream.addr]));

    let mut socket = TcpStream::connect(proxy.addr).expect("a connection");
    socket
        .set_read_timeout(Some(Duration::from_secs(5)))
        .expect("a read timeout");
    socket
        .write_all(upgrade_request("app.example.com").as_bytes())
        .expect("the handshake was sent");

    let (head, _) = read_head(&mut socket);
    assert!(
        head.starts_with("HTTP/1.1 101 Switching Protocols\r\n"),
        "{head}"
    );
    // Both halves, or the client does not treat the connection as upgraded.
    assert!(head.to_lowercase().contains("connection: upgrade"), "{head}");
    assert!(head.to_lowercase().contains("upgrade: websocket"), "{head}");
}

#[test]
fn the_upstream_sees_the_upgrade_headers_it_needs() {
    // `Connection` and `Upgrade` are hop-by-hop and are stripped on the way
    // through. An upgrade that does not restate them for the new hop arrives at
    // the origin as an ordinary GET, and the origin answers 200.
    let upstream = spawn(Behaviour::UpgradeEcho);
    let proxy = Proxy::start(table_for("app.example.com", &[upstream.addr]));

    let mut socket = TcpStream::connect(proxy.addr).expect("a connection");
    socket
        .set_read_timeout(Some(Duration::from_secs(5)))
        .expect("a read timeout");
    socket
        .write_all(upgrade_request("app.example.com").as_bytes())
        .expect("the handshake was sent");

    let (head, _) = read_head(&mut socket);
    // The upstream only answers 101 when it saw an `Upgrade` header to echo.
    assert!(head.contains("101"), "{head}");
    assert!(head.to_lowercase().contains("upgrade: websocket"), "{head}");
}

#[test]
fn bytes_flow_both_ways_through_the_tunnel() {
    let upstream = spawn(Behaviour::UpgradeEcho);
    let proxy = Proxy::start(table_for("app.example.com", &[upstream.addr]));

    let mut socket = TcpStream::connect(proxy.addr).expect("a connection");
    socket
        .set_read_timeout(Some(Duration::from_secs(5)))
        .expect("a read timeout");
    socket
        .write_all(upgrade_request("app.example.com").as_bytes())
        .expect("the handshake was sent");
    let (_, tail) = read_head(&mut socket);
    assert!(tail.is_empty(), "nothing follows a bare 101");

    // Bytes that are not HTTP and are not WebSocket either: the proxy has no
    // business understanding them, which is the property being tested.
    for payload in [&b"\x81\x05hello"[..], &b"\x00\xff\xfe binary \x01"[..]] {
        socket.write_all(payload).expect("the payload was sent");
        socket.flush().expect("flushed");
        let seen = read_exactly(&mut socket, payload.len());
        assert_eq!(seen, payload, "the tunnel altered the bytes passing through");
    }
}

#[test]
fn a_large_payload_crosses_the_tunnel_intact() {
    // Bigger than one read buffer, so the relay has to reassemble across
    // several completions rather than moving one buffer through.
    let upstream = spawn(Behaviour::UpgradeEcho);
    let proxy = Proxy::start(table_for("app.example.com", &[upstream.addr]));

    let mut socket = TcpStream::connect(proxy.addr).expect("a connection");
    socket
        .set_read_timeout(Some(Duration::from_secs(10)))
        .expect("a read timeout");
    socket
        .write_all(upgrade_request("app.example.com").as_bytes())
        .expect("the handshake was sent");
    let (_, _) = read_head(&mut socket);

    let payload: Vec<u8> = (0..200_000u32).map(|i| (i % 251) as u8).collect();
    let writer = {
        let mut half = socket.try_clone().expect("a second handle");
        let payload = payload.clone();
        std::thread::spawn(move || {
            half.write_all(&payload).expect("the payload was sent");
            half.flush().expect("flushed");
        })
    };
    let seen = read_exactly(&mut socket, payload.len());
    writer.join().expect("the writer finished");
    assert_eq!(seen.len(), payload.len(), "the tunnel truncated the payload");
    assert_eq!(seen, payload, "the tunnel reordered or altered the payload");
}

#[test]
fn an_upstream_that_speaks_first_is_not_dropped() {
    // A server may send before the client does, and its first bytes arrive in
    // the same read as the 101. A relay that starts listening only after the
    // head has been forwarded loses them.
    let greeting = b"\x81\x04ping".to_vec();
    let upstream = spawn(Behaviour::UpgradeThenSpeak(greeting.clone()));
    let proxy = Proxy::start(table_for("app.example.com", &[upstream.addr]));

    let mut socket = TcpStream::connect(proxy.addr).expect("a connection");
    socket
        .set_read_timeout(Some(Duration::from_secs(5)))
        .expect("a read timeout");
    socket
        .write_all(upgrade_request("app.example.com").as_bytes())
        .expect("the handshake was sent");

    let (head, tail) = read_head(&mut socket);
    assert!(head.contains("101"), "{head}");
    let mut seen = tail;
    if seen.len() < greeting.len() {
        seen.extend_from_slice(&read_exactly(&mut socket, greeting.len() - seen.len()));
    }
    assert_eq!(seen, greeting, "the upstream's first bytes were dropped");
}

#[test]
fn a_backend_that_refuses_the_upgrade_answers_the_client_itself() {
    // A 200 to a WebSocket handshake means the backend does not speak it. That
    // answer is the backend's to give; a proxy that turned it into a 502 would
    // be hiding the real one.
    let upstream = spawn(Behaviour::RefuseUpgrade);
    let proxy = Proxy::start(table_for("app.example.com", &[upstream.addr]));

    let mut client = Client::connect(proxy.addr);
    let response = client.send(upgrade_request("app.example.com").as_bytes());
    assert_eq!(response.status, 200);
    assert_eq!(response.body, b"not switching");
}

#[test]
fn a_101_nobody_asked_for_is_refused() {
    // An upstream switching protocols on an ordinary GET leaves this hop with
    // no idea how the rest of the connection is framed. The client has had no
    // bytes yet, so it can still be told.
    let upstream = spawn(Behaviour::Raw(
        b"HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\r\n"
            .to_vec(),
    ));
    let proxy = Proxy::start(table_for("app.example.com", &[upstream.addr]));

    let response = get(proxy.addr, "/", "app.example.com");
    assert_eq!(response.status, 502);
    assert_eq!(
        response.body,
        b"502 Bad Gateway: the upstream refused to complete the upgrade\n"
    );
}

#[test]
fn a_client_half_close_reaches_the_upstream() {
    // A WebSocket that ends cleanly does so by one side hanging up and the
    // other answering. Escalating a half-close to a full close would throw
    // away the other side's last bytes.
    let upstream = spawn(Behaviour::UpgradeEcho);
    let proxy = Proxy::start(table_for("app.example.com", &[upstream.addr]));

    let mut socket = TcpStream::connect(proxy.addr).expect("a connection");
    socket
        .set_read_timeout(Some(Duration::from_secs(5)))
        .expect("a read timeout");
    socket
        .write_all(upgrade_request("app.example.com").as_bytes())
        .expect("the handshake was sent");
    let (_, _) = read_head(&mut socket);

    socket.write_all(b"last words").expect("the payload was sent");
    socket.flush().expect("flushed");
    socket
        .shutdown(std::net::Shutdown::Write)
        .expect("the write half closed");

    // The echo comes back even though the client has already stopped talking,
    // and then the upstream's own close arrives as an end of stream.
    let mut seen = Vec::new();
    let mut chunk = [0u8; 1024];
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while std::time::Instant::now() < deadline {
        match socket.read(&mut chunk) {
            Ok(0) => break,
            Ok(n) => seen.extend_from_slice(&chunk[..n]),
            Err(_) => break,
        }
    }
    assert_eq!(
        seen, b"last words",
        "a half-closed client must still receive the answer"
    );
}

#[test]
fn upgrades_work_over_tls_too() {
    // The tunnel sits under the record layer, not beside it: the client half
    // goes through rustls and the upstream half does not, and neither half
    // knows the other exists.
    let upstream = spawn(Behaviour::UpgradeEcho);
    let (table, certs) = tls_table("app.example.com", &[upstream.addr], &["app.example.com"]);
    let proxy = tls_proxy(table, certs);

    let mut client = tls_connect(proxy.tls(), "app.example.com", tls_client_config());
    client
        .stream
        .write_all(upgrade_request("app.example.com").as_bytes())
        .expect("the handshake was sent");
    client.stream.flush().expect("flushed");

    let (head, tail) = read_head(&mut client.stream);
    assert!(head.contains("101 Switching Protocols"), "{head}");
    assert!(tail.is_empty(), "nothing follows a bare 101");

    let payload = b"\x81\x0bhello secure";
    client.stream.write_all(payload).expect("the payload was sent");
    client.stream.flush().expect("flushed");
    let seen = read_exactly(&mut client.stream, payload.len());
    assert_eq!(seen, payload, "the encrypted tunnel altered its bytes");
}

#[test]
fn a_tunnel_does_not_hold_shutdown_open() {
    // A tunnel is excluded from the drain, explicitly, and closed when it
    // starts: after a 101 there is no request boundary left to finish at, and
    // waiting for one would stall every rolling update until the deadline. The
    // grace period here is the default thirty seconds, so a tunnel that was
    // counted would show up as this test taking that long — which is what the
    // clock below is for. `lifecycle.rs` asserts the same thing through an
    // explicit signal; this one comes through the drop path.
    let upstream = spawn(Behaviour::UpgradeEcho);
    let proxy = Proxy::start(table_for("app.example.com", &[upstream.addr]));

    let mut socket = TcpStream::connect(proxy.addr).expect("a connection");
    socket
        .set_read_timeout(Some(Duration::from_secs(5)))
        .expect("a read timeout");
    socket
        .write_all(upgrade_request("app.example.com").as_bytes())
        .expect("the handshake was sent");
    let (head, _) = read_head(&mut socket);
    assert!(head.contains("101"), "{head}");

    // Dropping the proxy stops the engine and joins its threads. If an open
    // tunnel held that up, this test would hang rather than fail.
    let started = std::time::Instant::now();
    drop(proxy);
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "an open tunnel delayed shutdown by {:?}",
        started.elapsed()
    );
    let _ = &upstream;
}
