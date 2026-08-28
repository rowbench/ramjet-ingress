//! Shutdown on the reactor: stop accepting, then finish what is already
//! running.
//!
//! The hyper engine's `lifecycle.rs` is the same file for the other lane, and
//! the assertions here are deliberately its assertions: a client cannot tell
//! which engine served it, and that has to include the last request before a
//! rolling update replaces the pod.
//!
//! What makes this worth testing rather than reading is that the drain is not a
//! step the engine takes — it is a state the completion loop enters, and every
//! connection reaches the end by the same path it always did. A test that
//! signalled and waited in one call would never see the middle of that, so
//! every case here holds something open across the signal on purpose.

mod common;

use std::io::{Read, Write};
use std::net::TcpStream;
use std::thread;
use std::time::{Duration, Instant};

use common::*;
use ramjet_router::{PathType, RouteTable};

/// A table with a fast route and a slow one, so a test can have a connection
/// idle and another in flight at the same instant.
fn two_speeds(fast: std::net::SocketAddr, slow: std::net::SocketAddr) -> RouteTable {
    let mut builder = builder_with(&[("fast", &[fast]), ("slow", &[slow])]);
    builder
        .route(Some("app.example.com"), "/fast", PathType::Prefix, "fast")
        .expect("a valid route");
    builder
        .route(Some("app.example.com"), "/slow", PathType::Prefix, "slow")
        .expect("a valid route");
    builder.build().expect("a valid table")
}

#[test]
fn an_in_flight_request_finishes_after_the_shutdown_signal() {
    let slow = spawn(Behaviour::Slow {
        delay: Duration::from_millis(400),
        body: b"slow".to_vec(),
    });
    let mut proxy = Proxy::start(table_for("app.example.com", &[slow.addr]));
    let addr = proxy.addr;

    let request = thread::spawn(move || get(addr, "/", "app.example.com"));

    // Let the request reach the upstream, then ask the engine to stop while it
    // is still waiting for the answer.
    thread::sleep(Duration::from_millis(100));
    proxy.signal_shutdown();

    let reply = request.join().expect("the request thread did not panic");
    assert_eq!(
        reply.status, 200,
        "a request already in flight must not be dropped by shutdown"
    );
    assert_eq!(reply.text(), "slow");
    assert!(
        reply.closing,
        "a drained response has to tell the client the connection is ending, \
         or the client keeps it and sends the next request into a closed socket"
    );

    proxy.wait().expect("a clean drain");
}

#[test]
fn new_connections_are_refused_once_shutdown_has_started() {
    let upstream = echo();
    let mut proxy = Proxy::start(table_for("app.example.com", &[upstream.addr]));
    let addr = proxy.addr;

    assert_eq!(get(addr, "/", "app.example.com").status, 200);

    proxy.shutdown().expect("a clean drain");

    // The listeners are closed, so the load balancer finds out immediately
    // rather than by having a connection accepted and then abandoned.
    assert!(
        TcpStream::connect(addr).is_err(),
        "the listener should be closed once the drain has finished"
    );
}

#[test]
fn an_idle_keep_alive_connection_is_closed_at_drain_start() {
    // Two connections and one signal. The idle one holds nothing, so it goes
    // immediately; the other is carrying a request, so it stays. Asserting them
    // together is what makes this about the *start* of the drain rather than
    // about the end of the process — with only the idle connection open the
    // drain would finish at once and the two would be indistinguishable.
    let fast = echo();
    let slow = spawn(Behaviour::Slow {
        delay: Duration::from_millis(1500),
        body: b"slow".to_vec(),
    });
    let mut proxy = Proxy::start(two_speeds(fast.addr, slow.addr));
    let addr = proxy.addr;

    let mut idle = Client::connect(addr);
    let first = idle.send(b"GET /fast HTTP/1.1\r\nHost: app.example.com\r\n\r\n");
    assert_eq!(first.status, 200);
    assert!(!first.closing, "the connection is meant to be reusable here");

    let in_flight = thread::spawn(move || get(addr, "/slow", "app.example.com"));
    thread::sleep(Duration::from_millis(100));

    let signalled = Instant::now();
    proxy.signal_shutdown();

    idle.stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .expect("a read timeout");
    let mut byte = [0u8; 1];
    let read = idle.stream.read(&mut byte).expect("a readable socket");
    let waited = signalled.elapsed();
    assert_eq!(read, 0, "an idle keep-alive connection must be closed, not fed");
    assert!(
        waited < Duration::from_millis(750),
        "the idle connection waited {waited:?} to be closed, which is the \
         in-flight request's grace rather than its own"
    );

    assert_eq!(
        in_flight.join().expect("the request thread did not panic").status,
        200,
        "the request that was in flight had to finish"
    );
    proxy.wait().expect("a clean drain");
}

#[test]
fn a_slow_streaming_response_completes_within_the_grace() {
    // The body is still arriving when the signal does. Nothing about the
    // response is retryable at that point — the head is long gone — so cutting
    // it short would hand the client a truncated body with a 200 in front of
    // it, which is worse than the connection error it replaces.
    let streaming = spawn(Behaviour::SlowChunked {
        pieces: vec![b"one ".to_vec(), b"two ".to_vec(), b"three".to_vec()],
        gap: Duration::from_millis(150),
    });
    let mut proxy = Proxy::start(table_for("app.example.com", &[streaming.addr]));
    let addr = proxy.addr;

    let request = thread::spawn(move || get(addr, "/", "app.example.com"));
    // Long enough for the head and the first piece, short enough that the rest
    // is still to come.
    thread::sleep(Duration::from_millis(200));
    proxy.signal_shutdown();

    let reply = request.join().expect("the request thread did not panic");
    assert_eq!(reply.status, 200);
    assert_eq!(
        reply.text(),
        "one two three",
        "a response still streaming when the signal arrived was truncated"
    );
    proxy.wait().expect("a clean drain");
}

#[test]
fn a_request_whose_body_is_still_arriving_is_carried_to_the_end() {
    // In flight means both directions. A request whose body is half sent has
    // been routed, has an upstream connection open, and has bytes the backend
    // is waiting for — dropping it there is the same lost request as dropping a
    // response, and it is the direction a response-shaped test never reaches.
    let upstream = echo();
    let mut proxy = Proxy::start(table_for("app.example.com", &[upstream.addr]));

    let mut client = Client::connect(proxy.addr);
    client.write(
        b"POST / HTTP/1.1\r\nHost: app.example.com\r\nContent-Length: 10\r\n\r\nabcd",
    );
    thread::sleep(Duration::from_millis(100));

    proxy.signal_shutdown();
    thread::sleep(Duration::from_millis(50));
    client.stream.write_all(b"efghij").expect("the rest of the body");
    client.stream.flush().expect("flushed");

    let reply = client.read_response();
    assert_eq!(reply.status, 200);
    assert_eq!(
        reply.header("echo-body-len"),
        Some("10"),
        "the whole body had to reach the backend, not the part that arrived \
         before the signal"
    );
    proxy.wait().expect("a clean drain");
}

#[test]
fn an_open_tunnel_does_not_hold_the_drain_open() {
    // Documented behaviour, asserted so it stays a decision: once a connection
    // has been upgraded there is no request boundary left to finish at, and a
    // WebSocket can stay open for hours. Waiting for one would mean every
    // rolling update stalls until the deadline. Tunnels end with the process.
    let upstream = spawn(Behaviour::UpgradeEcho);
    let mut proxy = Proxy::with_config(table_for("app.example.com", &[upstream.addr]), |config| {
        config.workers = Some(1);
        config.shutdown_grace = Duration::from_secs(30);
    });

    let mut socket = TcpStream::connect(proxy.addr).expect("a connection");
    socket
        .set_read_timeout(Some(Duration::from_secs(5)))
        .expect("a read timeout");
    socket
        .write_all(
            b"GET /socket HTTP/1.1\r\nHost: app.example.com\r\nConnection: Upgrade\r\n\
              Upgrade: websocket\r\n\r\n",
        )
        .expect("the handshake was sent");
    socket.flush().expect("flushed");

    let mut head = Vec::new();
    let mut chunk = [0u8; 1024];
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline && !head.windows(4).any(|w| w == b"\r\n\r\n") {
        match socket.read(&mut chunk) {
            Ok(0) => break,
            Ok(n) => head.extend_from_slice(&chunk[..n]),
            Err(_) => break,
        }
    }
    assert!(
        String::from_utf8_lossy(&head).starts_with("HTTP/1.1 101"),
        "the upgrade was not carried: {:?}",
        String::from_utf8_lossy(&head)
    );

    let started = Instant::now();
    proxy.shutdown().expect("the drain finished cleanly");
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "the drain waited {:?} for a tunnel that will never close",
        started.elapsed()
    );

    let read = socket.read(&mut chunk).unwrap_or(0);
    assert_eq!(read, 0, "the tunnel should be closed once the process drains");
}

#[test]
fn an_exceeded_grace_period_is_reported() {
    // An upstream that never answers: the drain has to give up, and saying so
    // is the difference between a deadline and a lie.
    let black_hole = spawn(Behaviour::BlackHole);
    let mut proxy = Proxy::with_config(table_for("app.example.com", &[black_hole.addr]), |config| {
        config.workers = Some(1);
        config.shutdown_grace = Duration::from_millis(300);
    });

    let mut socket = TcpStream::connect(proxy.addr).expect("a connection");
    socket
        .write_all(b"GET / HTTP/1.1\r\nHost: app.example.com\r\n\r\n")
        .expect("the request was sent");
    socket.flush().expect("flushed");
    // Long enough for the request to be routed and dispatched, so the drain
    // finds a connection that is genuinely in flight.
    thread::sleep(Duration::from_millis(150));

    let started = Instant::now();
    proxy.signal_shutdown();
    let outcome = proxy.wait();
    assert_eq!(
        outcome.map_err(|error| error.kind()),
        Err(std::io::ErrorKind::TimedOut),
        "a drain that hit its deadline must say so rather than pretend"
    );
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "the drain gave up after {:?} rather than at its deadline",
        started.elapsed()
    );
}

#[test]
fn shutdown_returns_promptly_when_nothing_is_in_flight() {
    let upstream = echo();
    let mut proxy = Proxy::with_config(table_for("app.example.com", &[upstream.addr]), |config| {
        config.workers = Some(1);
        config.shutdown_grace = Duration::from_secs(30);
    });
    assert_eq!(get(proxy.addr, "/", "app.example.com").status, 200);

    // An idle engine must not sit out the whole grace period: the drain ends
    // when the connections do, and the deadline is only a ceiling.
    let started = Instant::now();
    proxy.shutdown().expect("a clean drain");
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "the drain took {:?} with nothing to drain",
        started.elapsed()
    );
}

#[test]
fn every_core_drains_its_own_connections() {
    // One reactor per core, each with its own connections, its own pool and its
    // own countdown. The drain is per core and the engine is done when the last
    // of them is, so this exists to prove the whole fan-out ends rather than
    // whichever core happened to be asked first.
    let slow = spawn(Behaviour::Slow {
        delay: Duration::from_millis(300),
        body: b"slow".to_vec(),
    });
    let mut proxy = Proxy::with_config(table_for("app.example.com", &[slow.addr]), |config| {
        config.workers = Some(4);
        config.shutdown_grace = Duration::from_secs(30);
    });
    let addr = proxy.addr;

    // More connections than cores, so every core is holding several.
    let requests: Vec<_> = (0..16)
        .map(|_| thread::spawn(move || get(addr, "/", "app.example.com")))
        .collect();
    thread::sleep(Duration::from_millis(100));
    proxy.signal_shutdown();

    for request in requests {
        let reply = request.join().expect("the request thread did not panic");
        assert_eq!(reply.status, 200, "a core dropped a request it was serving");
        assert_eq!(reply.text(), "slow");
    }
    proxy.wait().expect("every core drained cleanly");
}
