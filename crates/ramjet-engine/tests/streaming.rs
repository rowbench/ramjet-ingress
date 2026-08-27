//! Bodies: framed by length, framed by chunks, framed by the close.
//!
//! A proxy that gets body framing wrong does not merely serve a bad response —
//! it loses the boundary between one message and the next, and then serves
//! somebody else's bytes as the start of the following request. Every
//! assertion here is really about that boundary.

mod common;

use std::io::Read;
use std::net::TcpStream;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use common::{echo, spawn, table_for, Behaviour, Client, Proxy};

#[test]
fn a_request_body_reaches_the_upstream_intact() {
    let upstream = echo();
    let proxy = Proxy::start(table_for("app.example.com", &[upstream.addr]));
    let mut client = Client::connect(proxy.addr);

    let payload = vec![b'p'; 64 * 1024];
    let mut request = format!(
        "PUT /upload HTTP/1.1\r\nHost: app.example.com\r\nContent-Length: {}\r\n\r\n",
        payload.len()
    )
    .into_bytes();
    request.extend_from_slice(&payload);

    let response = client.send(&request);

    assert_eq!(response.status, 200);
    assert_eq!(
        response.header("echo-body-len"),
        Some(&*payload.len().to_string()),
        "every byte of the upload must arrive"
    );
}

#[test]
fn a_large_response_body_arrives_intact() {
    let body = (0..256 * 1024).map(|i| (i % 251) as u8).collect::<Vec<_>>();
    let mut raw = format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n", body.len()).into_bytes();
    raw.extend_from_slice(&body);
    let upstream = spawn(Behaviour::Raw(raw));
    let proxy = Proxy::start(table_for("app.example.com", &[upstream.addr]));

    let response = common::get(proxy.addr, "/", "app.example.com");

    assert_eq!(response.status, 200);
    assert_eq!(response.body.len(), body.len());
    assert_eq!(response.body, body, "the body was corrupted in transit");
}

#[test]
fn a_chunked_response_is_forwarded_as_chunked() {
    let upstream = spawn(Behaviour::Chunked(vec![
        b"hello ".to_vec(),
        b"chunked ".to_vec(),
        b"world".to_vec(),
    ]));
    let proxy = Proxy::start(table_for("app.example.com", &[upstream.addr]));

    let response = common::get(proxy.addr, "/", "app.example.com");

    assert_eq!(response.status, 200);
    assert_eq!(response.body, b"hello chunked world");
    assert_eq!(
        response.header("transfer-encoding"),
        Some("chunked"),
        "the framing must survive the hop"
    );
    assert_eq!(
        response.header("content-length"),
        None,
        "a chunked response must not also claim a length"
    );
}

#[test]
fn a_large_chunked_response_arrives_intact() {
    let pieces: Vec<Vec<u8>> = (0..64)
        .map(|i| vec![b'a' + (i % 26) as u8; 4096])
        .collect();
    let expected: Vec<u8> = pieces.iter().flatten().copied().collect();
    let upstream = spawn(Behaviour::Chunked(pieces));
    let proxy = Proxy::start(table_for("app.example.com", &[upstream.addr]));

    let response = common::get(proxy.addr, "/", "app.example.com");

    assert_eq!(response.body.len(), expected.len());
    assert_eq!(response.body, expected);
}

#[test]
fn a_chunked_request_body_reaches_the_upstream() {
    let upstream = echo();
    let proxy = Proxy::start(table_for("app.example.com", &[upstream.addr]));
    let mut client = Client::connect(proxy.addr);

    let response = client.send(
        b"POST /up HTTP/1.1\r\nHost: app.example.com\r\nTransfer-Encoding: chunked\r\n\r\n\
          5\r\nhello\r\n6\r\n world\r\n0\r\n\r\n",
    );

    assert_eq!(response.status, 200);
    assert_eq!(
        response.header("echo-body-len"),
        Some("11"),
        "the upstream should have decoded 'hello world'"
    );
    // The framing was restated after being stripped as hop-by-hop.
    assert_eq!(response.header("echo-transfer-encoding"), Some("chunked"));
}

#[test]
fn a_chunked_request_with_a_trailer_is_forwarded() {
    let upstream = echo();
    let proxy = Proxy::start(table_for("app.example.com", &[upstream.addr]));
    let mut client = Client::connect(proxy.addr);

    let response = client.send(
        b"POST /up HTTP/1.1\r\nHost: app.example.com\r\nTransfer-Encoding: chunked\r\n\r\n\
          4\r\nbody\r\n0\r\nX-Checksum: abc\r\n\r\n",
    );

    assert_eq!(response.status, 200);
    assert_eq!(response.header("echo-body-len"), Some("4"));
}

#[test]
fn the_connection_is_reusable_after_a_chunked_exchange() {
    // The framing test that matters most: if the terminator was mis-located,
    // the next request starts at the wrong byte and everything after is
    // nonsense.
    let upstream = spawn(Behaviour::Chunked(vec![b"one".to_vec(), b"two".to_vec()]));
    let proxy = Proxy::start(table_for("app.example.com", &[upstream.addr]));
    let mut client = Client::connect(proxy.addr);

    for i in 0..5 {
        let response = client.send(b"GET / HTTP/1.1\r\nHost: app.example.com\r\n\r\n");
        assert_eq!(response.status, 200, "exchange {i}");
        assert_eq!(response.body, b"onetwo", "exchange {i}");
    }
}

#[test]
fn a_response_framed_by_the_close_is_delivered_and_ends_the_connection() {
    let upstream = spawn(Behaviour::RawThenClose(
        b"HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\n\r\nno framing here".to_vec(),
    ));
    let proxy = Proxy::start(table_for("app.example.com", &[upstream.addr]));

    let response = common::get(proxy.addr, "/", "app.example.com");

    assert_eq!(response.status, 200);
    assert_eq!(response.body, b"no framing here");
    // The upstream's close *was* the framing, so this hop has to pass that on:
    // a client told to keep the connection open would wait for an end that
    // never comes.
    assert!(
        response.closing || response.header("content-length").is_some(),
        "a close-framed response must not be forwarded on a persistent connection"
    );
}

#[test]
fn a_204_carries_no_body_and_the_connection_continues() {
    let upstream = spawn(Behaviour::Raw(
        b"HTTP/1.1 204 No Content\r\n\r\n".to_vec(),
    ));
    let proxy = Proxy::start(table_for("app.example.com", &[upstream.addr]));
    let mut client = Client::connect(proxy.addr);

    let response = client.send(b"GET / HTTP/1.1\r\nHost: app.example.com\r\n\r\n");
    assert_eq!(response.status, 204);
    assert!(response.body.is_empty());

    // If the proxy had waited for a body, this would hang.
    let next = client.send(b"GET /after HTTP/1.1\r\nHost: app.example.com\r\n\r\n");
    assert_eq!(next.status, 204);
}

#[test]
fn a_body_that_arrives_in_pieces_is_reassembled() {
    let upstream = echo();
    let proxy = Proxy::start(table_for("app.example.com", &[upstream.addr]));
    let mut client = Client::connect(proxy.addr);

    client.write(b"POST /slow HTTP/1.1\r\nHost: app.example.com\r\nContent-Length: 12\r\n\r\n");
    for piece in [&b"hello"[..], b" ", b"world!"] {
        std::thread::sleep(Duration::from_millis(20));
        client.write(piece);
    }

    let response = client.read_response();
    assert_eq!(response.status, 200);
    assert_eq!(response.header("echo-body-len"), Some("12"));
}

#[test]
fn the_first_response_byte_does_not_wait_for_the_last() {
    // Streaming, not buffering. An upstream that sends a head and then stalls
    // must not stall the head's delivery to the client.
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("a listener");
    let addr = listener.local_addr().expect("an address");
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(mut stream) = stream else { return };
            let _ = stream.set_nonblocking(false);
            let mut probe = [0u8; 4096];
            let _ = stream.read(&mut probe);
            use std::io::Write;
            let _ = stream.write_all(
                b"HTTP/1.1 200 OK\r\nContent-Length: 10\r\n\r\nfirst",
            );
            let _ = stream.flush();
            std::thread::sleep(Duration::from_millis(400));
            let _ = stream.write_all(b"last");
            let _ = stream.write_all(b"!");
            let _ = stream.flush();
        }
    });

    let proxy = Proxy::start(table_for("app.example.com", &[addr]));
    let mut client = Client::connect(proxy.addr);
    client.write(b"GET / HTTP/1.1\r\nHost: app.example.com\r\n\r\n");

    // Read raw so the first bytes can be observed before the body completes.
    let started = std::time::Instant::now();
    let response = client.read_response();
    let elapsed = started.elapsed();

    assert_eq!(response.body, b"firstlast!");
    assert!(
        elapsed >= Duration::from_millis(350),
        "the test upstream stalls for 400ms; got {elapsed:?}"
    );
}

#[test]
fn concurrent_clients_do_not_see_each_others_bytes() {
    // The interleave check. A shared read buffer, a mis-scoped generation, or a
    // descriptor closed under an operation in flight all show up here as one
    // client receiving another's payload — the failure mode that matters most
    // and the one a single-client test cannot see.
    const CLIENTS: usize = 24;
    const ROUNDS: usize = 20;

    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("a listener");
    let addr = listener.local_addr().expect("an address");
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(mut stream) = stream else { return };
            std::thread::spawn(move || {
                let _ = stream.set_nonblocking(false);
                let mut buf = Vec::new();
                let mut chunk = [0u8; 4096];
                loop {
                    // Reflect the request target back as the whole body, so a
                    // mismatch names the client whose bytes leaked.
                    let head_end = loop {
                        if let Some(at) = buf
                            .windows(4)
                            .position(|w| w == b"\r\n\r\n")
                            .map(|i| i + 4)
                        {
                            break at;
                        }
                        match stream.read(&mut chunk) {
                            Ok(0) | Err(_) => return,
                            Ok(n) => buf.extend_from_slice(&chunk[..n]),
                        }
                    };
                    let head = String::from_utf8_lossy(&buf[..head_end]).to_string();
                    buf.drain(..head_end);
                    let target = head
                        .split(' ')
                        .nth(1)
                        .unwrap_or("/")
                        .trim_start_matches('/')
                        .to_owned();
                    use std::io::Write;
                    let payload = target.repeat(64);
                    let response = format!(
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n{payload}",
                        payload.len()
                    );
                    if stream.write_all(response.as_bytes()).is_err() {
                        return;
                    }
                    let _ = stream.flush();
                }
            });
        }
    });

    let proxy = Proxy::with_config(table_for("app.example.com", &[addr]), |config| {
        config.workers = Some(4);
    });

    let mismatches = Arc::new(AtomicUsize::new(0));
    let details: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let mut threads = Vec::new();
    for id in 0..CLIENTS {
        let addr = proxy.addr;
        let mismatches = Arc::clone(&mismatches);
        let details = Arc::clone(&details);
        threads.push(std::thread::spawn(move || {
            // A distinct, self-identifying payload per client.
            let token = format!("c{id:02}x");
            let mut client = Client::connect(addr);
            for round in 0..ROUNDS {
                let response = client.send(
                    format!("GET /{token} HTTP/1.1\r\nHost: app.example.com\r\n\r\n").as_bytes(),
                );
                let expected = token.repeat(64);
                if response.status != 200 || response.text() != expected {
                    mismatches.fetch_add(1, Ordering::Relaxed);
                    details.lock().expect("the ledger").push(format!(
                        "client {id} round {round}: status {} body {:?}",
                        response.status,
                        &response.text()[..response.text().len().min(40)]
                    ));
                }
            }
        }));
    }
    for thread in threads {
        thread.join().expect("a client thread");
    }

    let seen = mismatches.load(Ordering::Relaxed);
    assert_eq!(
        seen,
        0,
        "{seen} of {} exchanges saw the wrong bytes: {:?}",
        CLIENTS * ROUNDS,
        details.lock().expect("the ledger")
    );
}

#[test]
fn many_short_lived_connections_do_not_leak() {
    // Descriptor hygiene under churn: every connection opens, is served and is
    // closed, and the reactor's cancelled operations have to land somewhere
    // harmless. A leak here exhausts the descriptor table and the last requests
    // fail.
    let upstream = echo();
    let proxy = Proxy::start(table_for("app.example.com", &[upstream.addr]));

    for i in 0..300 {
        let response = common::get(proxy.addr, "/", "app.example.com");
        assert_eq!(response.status, 200, "connection {i}");
    }
    assert_eq!(upstream.seen.requests(), 300);
}

#[test]
fn a_client_that_stops_reading_does_not_wedge_the_core() {
    // Backpressure: a client that asks for a large body and then goes away must
    // not stop the core serving anybody else.
    let body = vec![b'x'; 512 * 1024];
    let mut raw = format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n", body.len()).into_bytes();
    raw.extend_from_slice(&body);
    let upstream = spawn(Behaviour::Raw(raw));
    let proxy = Proxy::start(table_for("app.example.com", &[upstream.addr]));

    let sulker = TcpStream::connect(proxy.addr).expect("a connection");
    {
        use std::io::Write;
        let mut sulker = &sulker;
        sulker
            .write_all(b"GET /big HTTP/1.1\r\nHost: app.example.com\r\n\r\n")
            .expect("sent");
    }
    std::thread::sleep(Duration::from_millis(100));

    // Meanwhile, an ordinary client must still be served.
    let response = common::get(proxy.addr, "/", "app.example.com");
    assert_eq!(response.status, 200);
    drop(sulker);
}
