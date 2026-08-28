//! One port, two engines.
//!
//! # The shape
//!
//! The uring engine owns the listener. A `rustls::server::Acceptor` yields the
//! ClientHello *before* a `ServerConfig` is chosen, so the ALPN list the client
//! offered is readable at a point where nothing has been committed to the
//! connection — no configuration picked, no byte written back. A client that
//! offered `h2` gets its socket handed to the hyper engine, along with every
//! byte read from it so far, and the hyper engine replays those and does the
//! handshake itself.
//!
//! From the client there is no reset, no second handshake and no retry. It sees
//! one connection, which negotiated HTTP/2.
//!
//! The plaintext listener does the same for the HTTP/2 prior-knowledge preface,
//! which is the only way h2 arrives without TLS.
//!
//! # What is asserted
//!
//! That both clients work, and that they took different paths. The second half
//! matters as much as the first: an h2 client that worked because the uring
//! engine quietly answered it in HTTP/1.1 would pass a functional test and be
//! completely wrong, so every case here reads `ramjet_dispatch_uring_total` and
//! `ramjet_dispatch_hyper_total` and asserts which one moved.

mod common;

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::sync::Arc;
use std::time::{Duration, Instant};

use common::*;
use ramjet_router::{RouteTable, SharedRouteTable};

/// Both engines, sharing one pair of listeners.
struct Dispatched {
    uring: Proxy,
    /// Kept alive: the hyper lane's runtime and the task running its server.
    _runtime: tokio::runtime::Runtime,
    hyper: ramjet_proxy::ShutdownHandle,
    hyper_task: Option<std::thread::JoinHandle<()>>,
    hyper_metrics: Arc<ramjet_proxy::Metrics>,
}

impl Drop for Dispatched {
    fn drop(&mut self) {
        self.hyper.shutdown();
        if let Some(task) = self.hyper_task.take() {
            let _ = task.join();
        }
    }
}

impl Dispatched {
    /// The uring engine's TLS listener, which is the only one there is.
    fn tls(&self) -> SocketAddr {
        self.uring.tls()
    }

    fn http(&self) -> SocketAddr {
        self.uring.addr
    }

    fn metrics(&self) -> String {
        self.uring.admin("/metrics").text()
    }

    /// Stop both lanes on one signal and wait for both to drain.
    ///
    /// The order is `ramjet-ingressd`'s: every lane is told at the same instant
    /// and waited for afterwards. Telling them one at a time would add their
    /// grace periods together, and the second one would still be draining long
    /// after the deadline an operator set.
    fn shutdown(&mut self) -> std::io::Result<()> {
        self.uring.signal_shutdown();
        self.hyper.shutdown();
        let drained = self.uring.wait();
        if let Some(task) = self.hyper_task.take() {
            let _ = task.join();
        }
        drained
    }
}

/// Start both engines over one table, with the uring lane owning the sockets.
fn dispatched(table: RouteTable, certs: Arc<ramjet_proxy::CertStore>) -> Dispatched {
    let routes = Arc::new(SharedRouteTable::new(table));

    // The hyper lane binds nothing at all. It exists to be handed connections,
    // and `handoff: true` is what makes it build the TLS acceptor it would
    // otherwise only build for a listener of its own.
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("a runtime");
    let config = ramjet_proxy::ProxyConfig {
        http: None,
        https: None,
        admin: None,
        worker_threads: Some(1),
        handoff: true,
        ..ramjet_proxy::ProxyConfig::default()
    };
    let server = runtime
        .block_on(async {
            ramjet_proxy::Server::bind_with(
                config,
                Arc::clone(&routes),
                Arc::clone(&certs),
                ramjet_proxy::ReadinessFlag::new(),
            )
        })
        .expect("the hyper lane started");
    server.readiness().set_ready(true);
    let hyper_metrics = Arc::clone(server.metrics());
    let dispatch = server.handoffs();
    let (handle, shutdown) = ramjet_proxy::Shutdown::channel();
    let hyper_task = {
        let runtime_handle = runtime.handle().clone();
        std::thread::spawn(move || {
            let _ = runtime_handle.block_on(server.run(shutdown));
        })
    };

    let uring = {
        let certs = Arc::clone(&certs);
        Proxy::with_routes(Arc::clone(&routes), move |config, routes| {
            let resolver = Arc::new(ramjet_proxy::SniResolver::new(Arc::clone(routes), certs));
            config.https = Some(SocketAddr::from(([127, 0, 0, 1], 0)));
            // The full ALPN set, `h2` included. Offering it is what makes the
            // dispatch reachable at all: a client cannot ask for a protocol the
            // server never advertised, and with `h1_server_config` the peek
            // would have nothing to decide.
            config.tls = Some(Arc::new(
                ramjet_proxy::tls::server_config(resolver).expect("a server config"),
            ));
            config.dispatch = Some(dispatch);
        })
    };

    Dispatched {
        uring,
        _runtime: runtime,
        hyper: handle,
        hyper_task: Some(hyper_task),
        hyper_metrics,
    }
}

/// A TLS client offering `h2` first, then `http/1.1`.
fn h2_client_config() -> Arc<rustls::ClientConfig> {
    let mut config = (*tls_client_config()).clone();
    config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
    Arc::new(config)
}

/// Wait for a counter to reach `wanted`, because a handoff crosses a channel
/// and a thread before the other engine touches anything.
fn wait_for_counter(proxy: &Dispatched, name: &str, wanted: u64) -> u64 {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let seen = counter(&proxy.metrics(), name);
        if seen >= wanted || Instant::now() >= deadline {
            return seen;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}

#[test]
fn an_http1_client_is_served_by_the_uring_engine() {
    let upstream = echo();
    let (table, certs) = tls_table("app.example.com", &[upstream.addr], &["app.example.com"]);
    let proxy = dispatched(table, certs);

    let mut client = tls_connect(proxy.tls(), "app.example.com", tls_client_config());
    let response = client.send(b"GET /h1 HTTP/1.1\r\nHost: app.example.com\r\n\r\n");
    assert_eq!(response.status, 200);
    assert_eq!(client.alpn().as_deref(), Some(&b"http/1.1"[..]));

    let text = proxy.metrics();
    assert_eq!(
        counter(&text, "ramjet_dispatch_uring_total"),
        1,
        "an HTTP/1.1 client belongs on the uring lane:\n{text}"
    );
    assert_eq!(
        counter(&text, "ramjet_dispatch_hyper_total"),
        0,
        "nothing should have been handed over:\n{text}"
    );
}

#[test]
fn an_h2_client_is_handed_to_the_hyper_engine() {
    let upstream = echo();
    let (table, certs) = tls_table("app.example.com", &[upstream.addr], &["app.example.com"]);
    let proxy = dispatched(table, certs);

    // The handshake alone is the test's subject: what is asserted is that the
    // connection ended up on the other engine, having negotiated h2.
    let mut client = tls_connect(proxy.tls(), "app.example.com", h2_client_config());
    client.handshake();
    assert_eq!(
        client.alpn().as_deref(),
        Some(&b"h2"[..]),
        "the client asked for h2 and the connection that answered it must speak h2"
    );

    let handed = wait_for_counter(&proxy, "ramjet_dispatch_hyper_total", 1);
    assert_eq!(handed, 1, "the connection was not handed over");
    assert_eq!(
        counter(&proxy.metrics(), "ramjet_dispatch_uring_total"),
        0,
        "an h2 client must not be kept by an engine that cannot speak it"
    );
}

#[test]
fn an_h2_client_gets_a_real_response_end_to_end() {
    // The handoff is not just a socket that moved: the request has to be routed
    // and answered by the engine it landed on. HTTP/2 framing is hand-written
    // here rather than pulling in a client — a request on one stream with no
    // body is a fixed sequence of frames, and what matters is that a response
    // comes back at all.
    let upstream = echo();
    let (table, certs) = tls_table("app.example.com", &[upstream.addr], &["app.example.com"]);
    let proxy = dispatched(table, certs);

    let name = rustls_pki_types::ServerName::try_from("app.example.com".to_owned())
        .expect("a valid name");
    let conn = rustls::ClientConnection::new(h2_client_config(), name).expect("a session");
    let socket = TcpStream::connect(proxy.tls()).expect("a connection");
    socket
        .set_read_timeout(Some(Duration::from_secs(10)))
        .expect("a read timeout");
    let mut tls = rustls::StreamOwned::new(conn, socket);

    tls.write_all(&h2_request("app.example.com", "/h2"))
        .expect("the request was sent");
    tls.flush().expect("flushed");

    // Read until the upstream has seen the request. The response frames are
    // HPACK-encoded and this test does not decode them; what it asserts is that
    // the request crossed the handoff, was routed, and reached a backend — and
    // that the connection carried bytes back.
    let mut seen = Vec::new();
    let mut chunk = [0u8; 4096];
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline {
        match tls.read(&mut chunk) {
            Ok(0) => break,
            Ok(n) => seen.extend_from_slice(&chunk[..n]),
            Err(_) => break,
        }
        // A response frame on stream 1 means the exchange is over. Everything
        // before it is the server's own SETTINGS and window updates.
        if upstream.seen.requests() > 0 && seen.len() > 32 {
            break;
        }
    }

    assert_eq!(
        upstream.seen.requests(),
        1,
        "the handed-over connection's request never reached the upstream; \
         the connection carried {} bytes back",
        seen.len()
    );
    assert!(
        !seen.is_empty(),
        "the handed-over connection produced no bytes at all"
    );
    assert_eq!(wait_for_counter(&proxy, "ramjet_dispatch_hyper_total", 1), 1);
}

/// An HTTP/2 client's opening bytes: preface, SETTINGS, and a HEADERS frame
/// carrying a `GET` with no body.
///
/// Hand-built because the alternative is an h2 client crate in the dev
/// dependencies for one test, and the bytes are not complicated: the header
/// block uses HPACK's static table for `:method: GET` and `:scheme: https`, and
/// literal-without-indexing entries for the two that vary.
fn h2_request(host: &str, path: &str) -> Vec<u8> {
    let mut out = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n".to_vec();

    // An empty SETTINGS frame: length 0, type 0x4, no flags, stream 0.
    out.extend_from_slice(&[0, 0, 0, 0x04, 0x00, 0, 0, 0, 0]);

    // Indexed header fields from HPACK's static table: 2 is `:method: GET`, 7
    // is `:scheme: https`. Then literal-without-indexing entries for the two
    // that vary — index 4 is `:path`, 1 is `:authority` — each followed by its
    // value as a length-prefixed, un-Huffmanned string.
    let mut block = vec![
        0x82,
        0x87,
        0x04,
        u8::try_from(path.len()).expect("a short path"),
    ];
    block.extend_from_slice(path.as_bytes());
    block.push(0x01);
    block.push(u8::try_from(host.len()).expect("a short host"));
    block.extend_from_slice(host.as_bytes());

    let length = u32::try_from(block.len()).expect("a small header block");
    out.extend_from_slice(&length.to_be_bytes()[1..]);
    // HEADERS, END_STREAM | END_HEADERS, on stream 1.
    out.extend_from_slice(&[0x01, 0x05, 0, 0, 0, 1]);
    out.extend_from_slice(&block);
    out
}

#[test]
fn the_plaintext_preface_is_handed_over_too() {
    // The only way h2 arrives without TLS. There is no ClientHello to peek at,
    // so the decision is the preface itself — which RFC 9113 chose precisely
    // because no HTTP/1.1 request can begin with it.
    let upstream = echo();
    let (table, certs) = tls_table("app.example.com", &[upstream.addr], &["app.example.com"]);
    let proxy = dispatched(table, certs);

    let mut socket = TcpStream::connect(proxy.http()).expect("a connection");
    socket
        .set_read_timeout(Some(Duration::from_secs(5)))
        .expect("a read timeout");
    socket
        .write_all(&h2_request("app.example.com", "/h2c"))
        .expect("the preface was sent");
    socket.flush().expect("flushed");

    assert_eq!(wait_for_counter(&proxy, "ramjet_dispatch_hyper_total", 1), 1);

    let mut seen = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut chunk = [0u8; 4096];
    while Instant::now() < deadline && upstream.seen.requests() == 0 {
        match socket.read(&mut chunk) {
            Ok(0) => break,
            Ok(n) => seen.extend_from_slice(&chunk[..n]),
            Err(_) => break,
        }
    }
    assert!(
        !seen.starts_with(b"HTTP/1.1 502"),
        "the preface was refused rather than handed over: {:?}",
        String::from_utf8_lossy(&seen)
    );
}

#[test]
fn a_plain_http1_request_on_the_plaintext_port_stays_on_the_uring_lane() {
    let upstream = echo();
    let (table, certs) = tls_table("app.example.com", &[upstream.addr], &["app.example.com"]);
    let proxy = dispatched(table, certs);

    assert_eq!(get(proxy.http(), "/", "app.example.com").status, 200);
    assert_eq!(upstream.seen.requests(), 1);
    assert_eq!(
        counter(&proxy.metrics(), "ramjet_dispatch_hyper_total"),
        0,
        "an HTTP/1.1 request is not an h2 preface"
    );
}

#[test]
fn the_two_engines_share_one_certificate_store() {
    // The handed-over connection's handshake happens on the *other* engine, so
    // it resolves its certificate through that engine's resolver. Both are
    // built over the same route table and the same store, and this is what says
    // so: the h2 client gets the same certificate the h1 client does.
    let upstream = echo();
    let (table, certs) = tls_table("app.example.com", &[upstream.addr], &["app.example.com"]);
    let proxy = dispatched(table, certs);

    let mut h1 = tls_connect(proxy.tls(), "app.example.com", tls_client_config());
    h1.handshake();
    let mut h2 = tls_connect(proxy.tls(), "app.example.com", h2_client_config());
    h2.handshake();

    assert_eq!(h1.alpn().as_deref(), Some(&b"http/1.1"[..]));
    assert_eq!(h2.alpn().as_deref(), Some(&b"h2"[..]));
    assert_eq!(
        h1.peer_certificates(),
        h2.peer_certificates(),
        "the two lanes served different certificates for the same name"
    );
}

#[test]
fn one_signal_drains_both_lanes() {
    // With dispatch on, a request in flight and a connection that was handed
    // away are being held by two different engines, on two different threading
    // models, and the process has one signal and one deadline for both. What is
    // asserted is that the signal reaches both — the HTTP/1.1 request finishes
    // on the reactor, the handed-over h2 connection is let go by hyper, and the
    // listeners the uring lane owns for both of them are shut.
    let upstream = spawn(Behaviour::Slow {
        delay: Duration::from_millis(400),
        body: b"slow".to_vec(),
    });
    let (table, certs) = tls_table("app.example.com", &[upstream.addr], &["app.example.com"]);
    let mut proxy = dispatched(table, certs);
    let http = proxy.http();
    let tls = proxy.tls();

    // One connection on each lane: an HTTP/1.1 request the reactor is serving,
    // and an h2 connection hyper took over.
    let mut h2 = tls_connect(tls, "app.example.com", h2_client_config());
    h2.handshake();
    assert_eq!(wait_for_counter(&proxy, "ramjet_dispatch_hyper_total", 1), 1);
    assert_eq!(
        proxy.hyper_metrics.active_connections(),
        1,
        "the hyper lane should be holding the handed-over connection"
    );

    let request = std::thread::spawn(move || get(http, "/", "app.example.com"));
    std::thread::sleep(Duration::from_millis(100));

    let started = Instant::now();
    proxy.shutdown().expect("both lanes drained cleanly");
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "the two lanes drained in {:?}, which is one after the other rather \
         than inside one grace period",
        started.elapsed()
    );

    let reply = request.join().expect("the request thread did not panic");
    assert_eq!(
        reply.status, 200,
        "the reactor dropped a request it was serving when the other lane was \
         told to stop"
    );
    assert_eq!(reply.text(), "slow");

    // Both listeners belong to the uring lane, and both are shut — including
    // the TLS one every handoff arrives through.
    assert!(TcpStream::connect(http).is_err(), "the plaintext listener is still open");
    assert!(TcpStream::connect(tls).is_err(), "the TLS listener is still open");
}

#[test]
fn a_handed_over_connection_is_counted_once() {
    // The connection gauge is summed across both engines when dispatch is on,
    // so a handover that left the connection counted on the uring side would
    // report every h2 client twice.
    let upstream = echo();
    let (table, certs) = tls_table("app.example.com", &[upstream.addr], &["app.example.com"]);
    let proxy = dispatched(table, certs);

    let mut client = tls_connect(proxy.tls(), "app.example.com", h2_client_config());
    client.handshake();
    assert_eq!(wait_for_counter(&proxy, "ramjet_dispatch_hyper_total", 1), 1);

    // The uring engine has let go of it.
    let text = proxy.metrics();
    assert_eq!(
        counter(&text, "ramjet_active_connections"),
        0,
        "the uring engine is still counting a connection it handed away:\n{text}"
    );
    // And the hyper engine has taken it up.
    assert_eq!(
        proxy.hyper_metrics.active_connections(),
        1,
        "the hyper engine is not counting the connection it was handed"
    );
}
