//! Real sockets, real upstreams, a real engine.
//!
//! Nothing here is mocked. The upstreams are ordinary TCP servers on loopback,
//! the client is a hand-rolled HTTP/1.1 reader, and the engine under test is
//! bound and run exactly as `ramjet-ingressd` runs it. That matters more than
//! usual for this crate: the thing being tested is a state machine over a
//! completion-based reactor, and the bugs it can have — a descriptor closed
//! while an operation is in flight, a body boundary lost, two connections
//! sharing a buffer — are all invisible to a test that stubs out the I/O.
//!
//! The client deliberately does **not** use this crate's own codec. A framing
//! bug that the parser and the writer share would be invisible to a test that
//! read the response back through the parser that produced it.

#![allow(dead_code)]

use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::{Shutdown, SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use ramjet_engine::engine::{Config, Engine};
use ramjet_router::{
    CanaryRules, Endpoint, LbPolicy, PathType, RouteTable, RouteTableBuilder, SharedRouteTable,
};

// ---------------------------------------------------------------- upstreams

/// What a test upstream recorded about the traffic it received.
#[derive(Debug, Default)]
pub struct Seen {
    /// Requests answered.
    pub requests: AtomicUsize,
    /// Connections accepted. The ratio of the two is what proves pooling.
    pub connections: AtomicUsize,
}

impl Seen {
    pub fn requests(&self) -> usize {
        self.requests.load(Ordering::Relaxed)
    }

    pub fn connections(&self) -> usize {
        self.connections.load(Ordering::Relaxed)
    }
}

/// A TCP server on loopback, stopped when dropped.
pub struct Upstream {
    pub addr: SocketAddr,
    pub seen: Arc<Seen>,
    stop: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
}

impl Drop for Upstream {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        // Unblock the accept loop.
        let _ = TcpStream::connect(self.addr);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

/// How a test upstream answers.
#[derive(Clone)]
pub enum Behaviour {
    /// Reflect every request header back as `echo-<name>`, plus `echo-method`,
    /// `echo-target` and `echo-body-len`, with a fixed body.
    Echo { body: Vec<u8> },
    /// Answer with these exact bytes, once per request, framing and all.
    Raw(Vec<u8>),
    /// Answer with these exact bytes and then close, which is the only way to
    /// delimit a response that carries no framing header.
    RawThenClose(Vec<u8>),
    /// Wait, then answer as `Echo` would.
    Slow { delay: Duration, body: Vec<u8> },
    /// Accept the connection and never say anything.
    BlackHole,
    /// Read the request, then close without answering.
    HangUp,
    /// Answer the first request, then close on the next one without answering.
    ///
    /// This is the pooled-connection race made deterministic: the connection is
    /// alive when it goes into the pool and dead by the time the next request
    /// lands on it.
    EchoThenDieOnNext { body: Vec<u8> },
    /// Answer with a chunked body made of these pieces.
    Chunked(Vec<Vec<u8>>),
}

/// Start an upstream with the given behaviour.
pub fn spawn(behaviour: Behaviour) -> Upstream {
    let listener = TcpListener::bind("127.0.0.1:0").expect("an upstream listener");
    let addr = listener.local_addr().expect("an address");
    listener
        .set_nonblocking(true)
        .expect("a non-blocking listener");
    let seen = Arc::new(Seen::default());
    let stop = Arc::new(AtomicBool::new(false));

    let handle = {
        let seen = Arc::clone(&seen);
        let stop = Arc::clone(&stop);
        thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                match listener.accept() {
                    Ok((stream, _)) => {
                        seen.connections.fetch_add(1, Ordering::Relaxed);
                        let seen = Arc::clone(&seen);
                        let behaviour = behaviour.clone();
                        let stop = Arc::clone(&stop);
                        thread::spawn(move || serve(stream, behaviour, seen, stop));
                    }
                    Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(2));
                    }
                    Err(_) => break,
                }
            }
        })
    };

    Upstream {
        addr,
        seen,
        stop,
        handle: Some(handle),
    }
}

/// An echoing upstream serving a 128-byte body, matching the benchmark's.
pub fn echo() -> Upstream {
    spawn(Behaviour::Echo {
        body: vec![b'u'; 128],
    })
}

/// One counter's value out of a Prometheus exposition.
///
/// Zero when the series is absent, which reads the same way in an assertion as
/// a series that is present and zero — the distinction never matters here,
/// because every series this engine emits is emitted unconditionally.
pub fn counter(exposition: &str, name: &str) -> u64 {
    exposition
        .lines()
        .find(|line| {
            line.split_whitespace().next() == Some(name)
        })
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|value| value.parse().ok())
        .unwrap_or(0)
}

/// An address nothing is listening on.
pub fn dead_addr() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").expect("a listener");
    let addr = listener.local_addr().expect("an address");
    drop(listener);
    addr
}

fn serve(mut stream: TcpStream, behaviour: Behaviour, seen: Arc<Seen>, stop: Arc<AtomicBool>) {
    // On BSD and macOS an accepted socket inherits O_NONBLOCK from its
    // listener, which Linux does not do. Left alone, every read here would
    // return `WouldBlock` the moment the client paused, this loop would treat
    // that as end of stream, and the upstream would hang up after one request
    // — which looks exactly like a proxy that cannot pool connections.
    let _ = stream.set_nonblocking(false);
    let _ = stream.set_read_timeout(Some(Duration::from_secs(10)));
    let mut buf = Vec::new();
    let mut chunk = [0u8; 8192];
    let mut served = 0usize;

    loop {
        if stop.load(Ordering::Relaxed) {
            return;
        }
        // Read one whole request: head, then whatever frames its body.
        let head_end = loop {
            if let Some(at) = find(&buf, b"\r\n\r\n") {
                break at + 4;
            }
            match stream.read(&mut chunk) {
                Ok(0) | Err(_) => return,
                Ok(n) => buf.extend_from_slice(&chunk[..n]),
            }
        };
        let head = String::from_utf8_lossy(&buf[..head_end]).to_string();
        let body_len = content_length(&head);
        let chunked = header_of(&head, "transfer-encoding")
            .is_some_and(|v| v.eq_ignore_ascii_case("chunked"));

        let body;
        if chunked {
            // Read until the terminating zero-length chunk.
            let mut consumed = head_end;
            loop {
                match dechunk(&buf[consumed..]) {
                    Some((decoded, used)) => {
                        body = decoded;
                        consumed += used;
                        break;
                    }
                    None => match stream.read(&mut chunk) {
                        Ok(0) | Err(_) => return,
                        Ok(n) => buf.extend_from_slice(&chunk[..n]),
                    },
                }
            }
            buf.drain(..consumed);
        } else {
            while buf.len() < head_end + body_len {
                match stream.read(&mut chunk) {
                    Ok(0) | Err(_) => return,
                    Ok(n) => buf.extend_from_slice(&chunk[..n]),
                }
            }
            body = buf[head_end..head_end + body_len].to_vec();
            buf.drain(..head_end + body_len);
        }

        seen.requests.fetch_add(1, Ordering::Relaxed);

        let response = match &behaviour {
            Behaviour::BlackHole => {
                // Hold the connection open, saying nothing, until the test ends.
                while !stop.load(Ordering::Relaxed) {
                    thread::sleep(Duration::from_millis(10));
                }
                return;
            }
            Behaviour::HangUp => {
                let _ = stream.shutdown(Shutdown::Both);
                return;
            }
            Behaviour::Raw(bytes) => bytes.clone(),
            Behaviour::RawThenClose(bytes) => {
                let _ = stream.write_all(bytes);
                let _ = stream.flush();
                let _ = stream.shutdown(Shutdown::Write);
                return;
            }
            Behaviour::Slow { delay, body: reply } => {
                thread::sleep(*delay);
                echo_response(&head, &body, reply)
            }
            Behaviour::Echo { body: reply } => echo_response(&head, &body, reply),
            Behaviour::EchoThenDieOnNext { body: reply } => {
                if served == 0 {
                    echo_response(&head, &body, reply)
                } else {
                    let _ = stream.shutdown(Shutdown::Both);
                    return;
                }
            }
            Behaviour::Chunked(pieces) => {
                let mut out = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n".to_vec();
                for piece in pieces {
                    out.extend_from_slice(format!("{:x}\r\n", piece.len()).as_bytes());
                    out.extend_from_slice(piece);
                    out.extend_from_slice(b"\r\n");
                }
                out.extend_from_slice(b"0\r\n\r\n");
                out
            }
        };

        if stream.write_all(&response).is_err() {
            return;
        }
        let _ = stream.flush();
        served += 1;
        if header_of(&head, "connection").is_some_and(|v| v.eq_ignore_ascii_case("close")) {
            return;
        }
    }
}

/// Reflect the request back so a test can assert on what actually crossed the
/// hop.
fn echo_response(head: &str, body: &[u8], reply: &[u8]) -> Vec<u8> {
    let mut out = String::from("HTTP/1.1 200 OK\r\n");
    let mut lines = head.split("\r\n");
    let request_line = lines.next().unwrap_or("");
    let mut parts = request_line.split(' ');
    out.push_str(&format!("echo-method: {}\r\n", parts.next().unwrap_or("")));
    out.push_str(&format!("echo-target: {}\r\n", parts.next().unwrap_or("")));
    out.push_str(&format!("echo-version: {}\r\n", parts.next().unwrap_or("")));
    out.push_str(&format!("echo-body-len: {}\r\n", body.len()));
    for line in lines {
        if let Some((name, value)) = line.split_once(':') {
            // Header values on the wire may be anything; the test only ever
            // asserts on ones it sent, so a lossy reflection is fine.
            out.push_str(&format!("echo-{}: {}\r\n", name.trim().to_lowercase(), value.trim()));
        }
    }
    out.push_str("Content-Type: text/plain\r\n");
    out.push_str(&format!("Content-Length: {}\r\n\r\n", reply.len()));
    let mut bytes = out.into_bytes();
    bytes.extend_from_slice(reply);
    bytes
}

fn find(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

fn header_of(head: &str, name: &str) -> Option<String> {
    head.split("\r\n")
        .skip(1)
        .filter_map(|line| line.split_once(':'))
        .find(|(n, _)| n.eq_ignore_ascii_case(name))
        .map(|(_, v)| v.trim().to_owned())
}

fn content_length(head: &str) -> usize {
    header_of(head, "content-length")
        .and_then(|v| v.parse().ok())
        .unwrap_or(0)
}

/// Decode a chunked body, or `None` if it is not all there yet.
///
/// Written independently of the crate under test, so a shared misunderstanding
/// of the grammar cannot make a test pass.
fn dechunk(input: &[u8]) -> Option<(Vec<u8>, usize)> {
    let mut out = Vec::new();
    let mut at = 0;
    loop {
        let line_end = find(&input[at..], b"\r\n")? + at;
        let size_text = std::str::from_utf8(&input[at..line_end]).ok()?;
        let size_text = size_text.split(';').next().unwrap_or("");
        let size = usize::from_str_radix(size_text.trim(), 16).ok()?;
        at = line_end + 2;
        if size == 0 {
            // Trailer section, then a blank line.
            loop {
                let end = find(&input[at..], b"\r\n")? + at;
                let line = &input[at..end];
                at = end + 2;
                if line.is_empty() {
                    return Some((out, at));
                }
            }
        }
        if input.len() < at + size + 2 {
            return None;
        }
        out.extend_from_slice(&input[at..at + size]);
        at += size + 2;
    }
}

// ------------------------------------------------------------------- client

/// A response, read back off the wire.
#[derive(Debug, Clone)]
pub struct Response {
    pub status: u16,
    pub reason: String,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
    /// Whether the response said the connection is ending.
    pub closing: bool,
}

impl Response {
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(n, _)| n.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }

    pub fn headers_named(&self, name: &str) -> Vec<&str> {
        self.headers
            .iter()
            .filter(|(n, _)| n.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
            .collect()
    }

    pub fn text(&self) -> String {
        String::from_utf8_lossy(&self.body).to_string()
    }
}

/// A client connection that can be reused, so keep-alive is testable.
///
/// Generic over the stream so the HTTP reading below is written once and used
/// for both listeners. That is not tidiness: the response parser is the thing
/// that decides whether a test passes, and a second copy of it for TLS would be
/// a second place for a framing assumption to be wrong.
pub struct Client<S = TcpStream> {
    pub stream: S,
    buf: Vec<u8>,
    /// Responses still owed, and whether each answers a `HEAD`.
    ///
    /// A response to `HEAD` carries the `Content-Length` a `GET` would have had
    /// and none of those bytes, so a reader that does not know the method waits
    /// for a body that is never coming. Real clients track this the same way.
    pending: std::collections::VecDeque<bool>,
}

impl Client<TcpStream> {
    pub fn connect(addr: SocketAddr) -> Client<TcpStream> {
        let stream = TcpStream::connect(addr).expect("a connection to the proxy");
        stream
            .set_read_timeout(Some(Duration::from_secs(10)))
            .expect("a read timeout");
        stream.set_nodelay(true).expect("nodelay");
        Client::new(stream)
    }

    /// Half-close, so the proxy sees end of stream while still able to answer.
    pub fn shutdown_write(&mut self) {
        let _ = self.stream.shutdown(Shutdown::Write);
    }
}

impl<S: Read + Write> Client<S> {
    /// A client over an already-connected stream.
    pub fn new(stream: S) -> Client<S> {
        Client {
            stream,
            buf: Vec::new(),
            pending: std::collections::VecDeque::new(),
        }
    }

    /// Send raw bytes and read one response back.
    pub fn send(&mut self, request: &[u8]) -> Response {
        self.note_methods(request);
        self.stream.write_all(request).expect("the request was sent");
        self.stream.flush().expect("flushed");
        self.read_response()
    }

    /// Record, for each request in `bytes`, whether it is a `HEAD`.
    fn note_methods(&mut self, bytes: &[u8]) {
        let mut at = 0;
        // Requests are separated by a blank line; a test never sends a body
        // together with a pipelined follow-up, so this simple scan is enough.
        while at < bytes.len() {
            let end = find(&bytes[at..], b"\r\n\r\n").map_or(bytes.len(), |i| at + i + 4);
            self.pending
                .push_back(bytes[at..].starts_with(b"HEAD "));
            at = end;
        }
    }

    /// Send raw bytes one at a time, which exercises partial-parse resume.
    pub fn send_dribbled(&mut self, request: &[u8]) -> Response {
        self.note_methods(request);
        for byte in request {
            self.stream
                .write_all(std::slice::from_ref(byte))
                .expect("a byte was sent");
            self.stream.flush().expect("flushed");
            thread::sleep(Duration::from_micros(200));
        }
        self.read_response()
    }

    /// Write bytes without reading anything back.
    pub fn write(&mut self, bytes: &[u8]) {
        self.note_methods(bytes);
        self.stream.write_all(bytes).expect("bytes were sent");
        self.stream.flush().expect("flushed");
    }

    pub fn read_response(&mut self) -> Response {
        let deadline = Instant::now() + Duration::from_secs(10);
        let mut chunk = [0u8; 8192];
        loop {
            if let Some(response) = self.try_parse() {
                return response;
            }
            if Instant::now() > deadline {
                panic!(
                    "no complete response within 10s; have {} bytes: {:?}",
                    self.buf.len(),
                    String::from_utf8_lossy(&self.buf[..self.buf.len().min(400)])
                );
            }
            match self.stream.read(&mut chunk) {
                Ok(0) => {
                    // The peer closed. A body framed by the close is complete
                    // now; anything else is a truncated response.
                    if let Some(response) = self.try_parse_at_eof() {
                        return response;
                    }
                    panic!(
                        "the proxy closed with an incomplete response: {:?}",
                        String::from_utf8_lossy(&self.buf[..self.buf.len().min(400)])
                    );
                }
                Ok(n) => self.buf.extend_from_slice(&chunk[..n]),
                Err(e) => panic!("reading the response failed: {e}"),
            }
        }
    }

    fn try_parse(&mut self) -> Option<Response> {
        let answers_head = self.pending.front().copied().unwrap_or(false);
        let head_end = find(&self.buf, b"\r\n\r\n")? + 4;
        let head = String::from_utf8_lossy(&self.buf[..head_end]).to_string();
        let (status, reason, headers) = parse_head(&head);
        let closing = headers
            .iter()
            .any(|(n, v)| n.eq_ignore_ascii_case("connection") && v.eq_ignore_ascii_case("close"));

        let chunked = headers.iter().any(|(n, v)| {
            n.eq_ignore_ascii_case("transfer-encoding") && v.eq_ignore_ascii_case("chunked")
        });
        let length: Option<usize> = headers
            .iter()
            .find(|(n, _)| n.eq_ignore_ascii_case("content-length"))
            .and_then(|(_, v)| v.trim().parse().ok());

        let (body, total) = if answers_head {
            // Whatever the head promised, no body follows.
            (Vec::new(), head_end)
        } else if chunked {
            let (body, used) = dechunk(&self.buf[head_end..])?;
            (body, head_end + used)
        } else if let Some(length) = length {
            if self.buf.len() < head_end + length {
                return None;
            }
            (
                self.buf[head_end..head_end + length].to_vec(),
                head_end + length,
            )
        } else if (100..200).contains(&status) || status == 204 || status == 304 {
            (Vec::new(), head_end)
        } else {
            // Framed by the close; only complete at EOF.
            return None;
        };

        self.buf.drain(..total);
        self.pending.pop_front();
        Some(Response {
            status,
            reason,
            headers,
            body,
            closing,
        })
    }

    fn try_parse_at_eof(&mut self) -> Option<Response> {
        let head_end = find(&self.buf, b"\r\n\r\n")? + 4;
        let head = String::from_utf8_lossy(&self.buf[..head_end]).to_string();
        let (status, reason, headers) = parse_head(&head);
        let has_framing = headers.iter().any(|(n, _)| {
            n.eq_ignore_ascii_case("content-length") || n.eq_ignore_ascii_case("transfer-encoding")
        });
        if has_framing {
            return self.try_parse();
        }
        let body = self.buf[head_end..].to_vec();
        self.buf.clear();
        Some(Response {
            status,
            reason,
            headers,
            body,
            closing: true,
        })
    }
}

fn parse_head(head: &str) -> (u16, String, Vec<(String, String)>) {
    let mut lines = head.split("\r\n");
    let status_line = lines.next().unwrap_or("");
    let mut parts = status_line.splitn(3, ' ');
    let _version = parts.next().unwrap_or("");
    let status: u16 = parts.next().unwrap_or("0").parse().unwrap_or(0);
    let reason = parts.next().unwrap_or("").to_owned();
    let headers = lines
        .filter_map(|line| line.split_once(':'))
        .map(|(n, v)| (n.trim().to_owned(), v.trim().to_owned()))
        .collect();
    (status, reason, headers)
}

/// One request over a fresh connection.
pub fn get(addr: SocketAddr, path: &str, host: &str) -> Response {
    let mut client = Client::connect(addr);
    client.send(format!("GET {path} HTTP/1.1\r\nHost: {host}\r\n\r\n").as_bytes())
}

/// One request over a fresh connection, with extra header lines.
pub fn get_with(addr: SocketAddr, path: &str, host: &str, extra: &[(&str, &str)]) -> Response {
    let mut request = format!("GET {path} HTTP/1.1\r\nHost: {host}\r\n");
    for (name, value) in extra {
        request.push_str(&format!("{name}: {value}\r\n"));
    }
    request.push_str("\r\n");
    Client::connect(addr).send(request.as_bytes())
}

// -------------------------------------------------------------------- proxy

/// A running engine, stopped when dropped.
pub struct Proxy {
    pub addr: SocketAddr,
    /// The TLS listener's address, for a proxy started with one.
    pub tls_addr: Option<SocketAddr>,
    pub admin: Option<SocketAddr>,
    pub routes: Arc<SharedRouteTable>,
    pub readiness: Arc<AtomicBool>,
    shutdown: ramjet_engine::engine::Shutdown,
    handle: Option<JoinHandle<std::io::Result<()>>>,
}

impl Drop for Proxy {
    fn drop(&mut self) {
        self.shutdown.stop();
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

impl Proxy {
    /// Start an engine serving `table`, on ephemeral ports.
    pub fn start(table: RouteTable) -> Proxy {
        Proxy::with_config(table, |config| {
            config.workers = Some(1);
        })
    }

    /// Start an engine, adjusting the configuration first.
    pub fn with_config(table: RouteTable, adjust: impl FnOnce(&mut Config)) -> Proxy {
        let mut config = Config {
            http: Some(SocketAddr::from(([127, 0, 0, 1], 0))),
            admin: Some(SocketAddr::from(([127, 0, 0, 1], 0))),
            workers: Some(1),
            // A fast tick so a test does not wait a tenth of a second for every
            // timeout it wants to observe.
            tick: Duration::from_millis(10),
            ..Config::default()
        };
        adjust(&mut config);
        Proxy::launch(Arc::new(SharedRouteTable::new(table)), config)
    }

    /// Start an engine whose configuration needs the route table it will serve.
    ///
    /// TLS is the case that needs this: the SNI resolver reads certificate
    /// names out of the very table the engine is about to be handed, so the
    /// table has to exist before the configuration does. Passing the `Arc`
    /// rather than a clone of the table is the point — it is the same sharing
    /// `ramjet-ingressd` does, and a test that built two would not be testing
    /// it.
    pub fn with_routes(
        routes: Arc<SharedRouteTable>,
        adjust: impl FnOnce(&mut Config, &Arc<SharedRouteTable>),
    ) -> Proxy {
        let mut config = Config {
            http: Some(SocketAddr::from(([127, 0, 0, 1], 0))),
            admin: Some(SocketAddr::from(([127, 0, 0, 1], 0))),
            workers: Some(1),
            tick: Duration::from_millis(10),
            ..Config::default()
        };
        adjust(&mut config, &routes);
        Proxy::launch(routes, config)
    }

    fn launch(routes: Arc<SharedRouteTable>, config: Config) -> Proxy {
        let readiness = Arc::new(AtomicBool::new(false));
        let engine = Engine::bind(config, Arc::clone(&routes), Arc::clone(&readiness))
            .expect("the engine bound");
        // Every listener the engine opened, so a caller that asked for TLS
        // alone still has an address to wait on and to connect to.
        let http = engine.http_addr();
        let tls_addr = engine.https_addr();
        let addr = http
            .or(tls_addr)
            .expect("an engine serves at least one listener");
        let admin = engine.admin_addr();
        let shutdown = engine.shutdown();
        let handle = thread::spawn(move || engine.run());

        let proxy = Proxy {
            addr,
            tls_addr,
            admin,
            routes,
            readiness,
            shutdown,
            handle: Some(handle),
        };
        proxy.wait_until_listening();
        proxy
    }

    fn wait_until_listening(&self) {
        let deadline = Instant::now() + Duration::from_secs(5);
        while Instant::now() < deadline {
            if TcpStream::connect(self.addr).is_ok() {
                return;
            }
            thread::sleep(Duration::from_millis(5));
        }
        panic!("the engine never started listening on {}", self.addr);
    }

    /// The TLS listener's address, for a proxy that was started with one.
    pub fn tls(&self) -> SocketAddr {
        self.tls_addr.expect("a TLS listener")
    }

    /// Scrape the admin listener.
    pub fn admin(&self, path: &str) -> Response {
        let admin = self.admin.expect("an admin listener");
        let mut client = Client::connect(admin);
        client.send(format!("GET {path} HTTP/1.1\r\nHost: admin\r\n\r\n").as_bytes())
    }
}

// ---------------------------------------------------------------------- tls

/// A self-signed certificate for `names`, and the private key beside it.
///
/// Generated per test rather than read from a fixture: a checked-in key is a
/// key, and an expiry date in a fixture is a test that fails on a Tuesday in
/// two years.
pub fn certificate_for(names: &[&str]) -> rustls::sign::CertifiedKey {
    let names: Vec<String> = names.iter().map(|n| (*n).to_owned()).collect();
    let issued = rcgen::generate_simple_self_signed(names).expect("a self-signed certificate");
    let chain = vec![issued.cert.der().clone()];
    let key = rustls_pki_types::PrivateKeyDer::try_from(issued.signing_key.serialize_der())
        .expect("a usable private key");
    ramjet_proxy::tls::certified_key(chain, key).expect("a certified key")
}

/// A route table whose single route is served over TLS, with the certificate
/// store the engine will resolve against.
///
/// The two are returned together because they are published together: the
/// table holds an opaque handle id and the store holds what it points at, and a
/// test that built one without the other would fail every handshake.
pub fn tls_table(
    host: &str,
    endpoints: &[SocketAddr],
    cert_names: &[&str],
) -> (RouteTable, Arc<ramjet_proxy::CertStore>) {
    const HANDLE: u64 = 1;

    let mut builder = RouteTableBuilder::new();
    builder
        .backend(
            "app",
            LbPolicy::RoundRobin,
            endpoints.iter().copied().map(Endpoint::new).collect(),
        )
        .expect("a valid backend");
    builder
        .route(Some(host), "/", PathType::Prefix, "app")
        .expect("a valid route");
    for name in cert_names {
        builder
            .certificate(name, Arc::new(ramjet_router::CertifiedKeyHandle::new(HANDLE)))
            .expect("a valid certificate name");
    }

    let mut certs = HashMap::new();
    certs.insert(HANDLE, Arc::new(certificate_for(cert_names)));
    (
        builder.build().expect("a valid table"),
        Arc::new(ramjet_proxy::CertStore::with_certs(certs)),
    )
}

/// Start an engine with a TLS listener over `table` and `certs`.
pub fn tls_proxy(table: RouteTable, certs: Arc<ramjet_proxy::CertStore>) -> Proxy {
    Proxy::with_routes(Arc::new(SharedRouteTable::new(table)), move |config, routes| {
        let resolver = Arc::new(ramjet_proxy::SniResolver::new(Arc::clone(routes), certs));
        config.https = Some(SocketAddr::from(([127, 0, 0, 1], 0)));
        config.tls = Some(Arc::new(
            ramjet_proxy::tls::h1_server_config(resolver).expect("a server config"),
        ));
    })
}

/// A TLS client that trusts anything, because the certificate under test was
/// generated by the test that is about to verify it was served.
///
/// Verification is not what these tests are about — rustls's own suite covers
/// it — and a real root store would mean a real CA, which is a fixture with an
/// expiry date. What *is* asserted is that the name the client asked for is the
/// name on the certificate it got back.
#[derive(Debug)]
struct TrustAnything(Arc<rustls::crypto::CryptoProvider>);

impl rustls::client::danger::ServerCertVerifier for TrustAnything {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls_pki_types::CertificateDer<'_>,
        _intermediates: &[rustls_pki_types::CertificateDer<'_>],
        _server_name: &rustls_pki_types::ServerName<'_>,
        _ocsp: &[u8],
        _now: rustls_pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &rustls_pki_types::CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(
            message,
            cert,
            dss,
            &self.0.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &rustls_pki_types::CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &self.0.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        self.0.signature_verification_algorithms.supported_schemes()
    }
}

/// A client configuration that resumes sessions across connections.
///
/// The resumption cache is what makes a second connection a resumption rather
/// than a full handshake, so it is shared deliberately and a test that wants
/// two independent handshakes builds two of these.
pub fn tls_client_config() -> Arc<rustls::ClientConfig> {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let mut config = rustls::ClientConfig::builder_with_provider(Arc::clone(&provider))
        .with_safe_default_protocol_versions()
        .expect("default protocol versions")
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(TrustAnything(provider)))
        .with_no_client_auth();
    config.alpn_protocols = vec![b"http/1.1".to_vec()];
    Arc::new(config)
}

/// The stream an HTTPS test client reads and writes.
pub type TlsStream = rustls::StreamOwned<rustls::ClientConnection, TcpStream>;

/// An HTTPS client, with exactly the same HTTP reading as the plaintext one.
pub type TlsClient = Client<TlsStream>;

/// Connect to `addr` and handshake for `server_name`.
pub fn tls_connect(
    addr: SocketAddr,
    server_name: &str,
    config: Arc<rustls::ClientConfig>,
) -> TlsClient {
    let name =
        rustls_pki_types::ServerName::try_from(server_name.to_owned()).expect("a valid server name");
    let conn = rustls::ClientConnection::new(config, name).expect("a client session");
    let socket = TcpStream::connect(addr).expect("a TCP connection");
    socket
        .set_read_timeout(Some(Duration::from_secs(10)))
        .expect("a read timeout");
    socket.set_nodelay(true).expect("nodelay");
    Client::new(rustls::StreamOwned::new(conn, socket))
}

impl Client<TlsStream> {
    /// Drive the handshake to completion without sending a request.
    ///
    /// `StreamOwned` handshakes lazily, on the first read or write, so anything
    /// that wants to inspect the negotiated session has to force it first.
    pub fn handshake(&mut self) {
        // A zero-length write is enough: rustls completes the handshake before
        // it will accept any application data, and writes nothing itself.
        self.stream.write_all(&[]).expect("the handshake completed");
        self.stream.flush().expect("the handshake flushed");
    }

    /// The certificate chain the server presented, as DER.
    pub fn peer_certificates(&self) -> Vec<Vec<u8>> {
        self.stream
            .conn
            .peer_certificates()
            .map(|certs| certs.iter().map(|c| c.to_vec()).collect())
            .unwrap_or_default()
    }

    /// The protocol ALPN settled on.
    pub fn alpn(&self) -> Option<Vec<u8>> {
        self.stream.conn.alpn_protocol().map(|p| p.to_vec())
    }
}

/// One HTTPS request, on a connection of its own.
pub fn https_get(addr: SocketAddr, host: &str, path: &str) -> Response {
    let mut client = tls_connect(addr, host, tls_client_config());
    client
        .send(format!("GET {path} HTTP/1.1\r\nHost: {host}\r\nConnection: close\r\n\r\n").as_bytes())
}

// ------------------------------------------------------------------- tables

/// A table with one host and one path, pointing at these endpoints.
pub fn table_for(host: &str, endpoints: &[SocketAddr]) -> RouteTable {
    let mut builder = RouteTableBuilder::new();
    builder
        .backend(
            "app",
            LbPolicy::RoundRobin,
            endpoints.iter().copied().map(Endpoint::new).collect(),
        )
        .expect("a valid backend");
    builder
        .route(Some(host), "/", PathType::Prefix, "app")
        .expect("a valid route");
    builder.build().expect("a valid table")
}

/// A builder with named backends already registered, for the routing tests.
pub fn builder_with(backends: &[(&str, &[SocketAddr])]) -> RouteTableBuilder {
    let mut builder = RouteTableBuilder::new();
    for (name, endpoints) in backends {
        builder
            .backend(
                name,
                LbPolicy::RoundRobin,
                endpoints.iter().copied().map(Endpoint::new).collect(),
            )
            .expect("a valid backend");
    }
    builder
}

/// A table whose single route carries a canary.
pub fn canary_table(
    host: &str,
    production: &[SocketAddr],
    canary: &[SocketAddr],
    rules: CanaryRules<'_>,
) -> RouteTable {
    let mut builder = builder_with(&[("app", production), ("canary", canary)]);
    builder
        .canary_route(Some(host), "/", PathType::Prefix, "app", &rules)
        .expect("a valid canary route");
    builder.build().expect("a valid table")
}

/// Count how many requests each upstream saw, by address.
pub fn spread(upstreams: &[&Upstream]) -> HashMap<SocketAddr, usize> {
    upstreams
        .iter()
        .map(|u| (u.addr, u.seen.requests()))
        .collect()
}

/// A place to record what several concurrent clients observed.
pub type Ledger = Arc<Mutex<Vec<(String, String)>>>;
