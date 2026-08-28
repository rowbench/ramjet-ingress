//! Shared harness: real upstream servers on real sockets, and a real proxy.
//!
//! Nothing here is mocked. Every test binds ephemeral ports, speaks HTTP over
//! loopback TCP, and asserts on what actually came back — because the bugs this
//! crate can have are in framing, header handling, connection reuse, and
//! upgrades, and none of those exist in a mock.

#![allow(dead_code)] // each test binary uses a different subset

use std::collections::HashMap;
use std::convert::Infallible;
use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use http::{HeaderMap, HeaderName, HeaderValue, Request, Response, StatusCode};
use http_body_util::combinators::BoxBody;
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::service::service_fn;
use hyper_util::rt::{TokioExecutor, TokioIo};
use ramjet_proxy::{
    CertStore, ListenerConfig, Metrics, ProxyConfig, ReadinessFlag, Server, Shutdown,
    ShutdownHandle, UpstreamConfig,
};
use ramjet_router::{
    Endpoint, LbPolicy, PathType, RouteTable, RouteTableBuilder, SharedRouteTable,
};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;

/// The body type the test upstreams produce.
pub type TestBody = BoxBody<Bytes, Infallible>;

pub fn full(data: impl Into<Bytes>) -> TestBody {
    Full::new(data.into()).boxed()
}

pub fn empty_body() -> TestBody {
    Full::new(Bytes::new()).boxed()
}

fn loopback() -> SocketAddr {
    SocketAddr::from(([127, 0, 0, 1], 0))
}

// ---------------------------------------------------------------------------
// Upstream servers
// ---------------------------------------------------------------------------

/// Serves `handler` over HTTP/1.1 on a fresh ephemeral port.
pub async fn spawn_http<F, Fut>(handler: F) -> SocketAddr
where
    F: Fn(Request<Incoming>) -> Fut + Clone + Send + Sync + 'static,
    Fut: Future<Output = Response<TestBody>> + Send + 'static,
{
    let listener = TcpListener::bind(loopback()).await.expect("bind upstream");
    let addr = listener.local_addr().expect("upstream addr");
    tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                return;
            };
            let handler = handler.clone();
            tokio::spawn(async move {
                let service = service_fn(move |request| {
                    let handler = handler.clone();
                    async move { Ok::<_, Infallible>(handler(request).await) }
                });
                let _ = hyper::server::conn::http1::Builder::new()
                    .serve_connection(TokioIo::new(stream), service)
                    .with_upgrades()
                    .await;
            });
        }
    });
    addr
}

/// Runs `handler` against the raw socket, for tests about bytes on the wire.
pub async fn spawn_raw<F, Fut>(handler: F) -> SocketAddr
where
    F: Fn(TcpStream) -> Fut + Clone + Send + Sync + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    let listener = TcpListener::bind(loopback()).await.expect("bind raw");
    let addr = listener.local_addr().expect("raw addr");
    tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                return;
            };
            let handler = handler.clone();
            tokio::spawn(async move { handler(stream).await });
        }
    });
    addr
}

/// An upstream that identifies itself and reflects the request back.
///
/// Every request header comes back as `echo-<name>`, which is how the header
/// tests see exactly what the proxy sent without the response's own hop-by-hop
/// rules interfering.
pub async fn spawn_echo(name: &'static str) -> SocketAddr {
    spawn_http(move |request: Request<Incoming>| async move {
        let summary = format!(
            "{} {}",
            request.method(),
            request
                .uri()
                .path_and_query()
                .map_or("/", |p| p.as_str())
        );
        let mut response = Response::new(full(summary));
        let headers = response.headers_mut();
        headers.insert("x-upstream", HeaderValue::from_static(name));
        for (key, value) in request.headers() {
            if let Ok(echoed) = HeaderName::from_bytes(format!("echo-{key}").as_bytes()) {
                headers.append(echoed, value.clone());
            }
        }
        response
    })
    .await
}

/// An address with nothing listening on it: connecting gets ECONNREFUSED.
pub async fn dead_addr() -> SocketAddr {
    let listener = TcpListener::bind(loopback()).await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    drop(listener);
    addr
}

/// An upstream that accepts the connection and then never answers.
pub async fn spawn_black_hole() -> SocketAddr {
    spawn_raw(|stream| async move {
        // Hold the connection open without replying, which is what a wedged
        // application server looks like from out here.
        let _ = stream.readable().await;
        tokio::time::sleep(Duration::from_secs(3600)).await;
    })
    .await
}

/// An upstream that answers `200` after `delay`.
pub async fn spawn_slow(delay: Duration) -> SocketAddr {
    spawn_http(move |_request| async move {
        tokio::time::sleep(delay).await;
        Response::new(full("slow"))
    })
    .await
}

/// Reads an HTTP request head off `stream`, returning it as a string.
pub async fn read_head(stream: &mut TcpStream) -> String {
    let mut buffer = Vec::new();
    let mut chunk = [0u8; 1024];
    while let Ok(read) = stream.read(&mut chunk).await {
        if read == 0 {
            break;
        }
        buffer.extend_from_slice(&chunk[..read]);
        if buffer.windows(4).any(|window| window == b"\r\n\r\n") {
            break;
        }
    }
    String::from_utf8_lossy(&buffer).into_owned()
}

// ---------------------------------------------------------------------------
// Route tables
// ---------------------------------------------------------------------------

/// A table with one host, one prefix rule, and one backend.
pub fn single_route(host: &str, path: &str, endpoints: &[SocketAddr]) -> RouteTable {
    let mut builder = RouteTableBuilder::new();
    builder
        .backend(
            "app",
            LbPolicy::RoundRobin,
            endpoints.iter().copied().map(Endpoint::new).collect(),
        )
        .expect("registers a backend");
    builder
        .route(Some(host), path, PathType::Prefix, "app")
        .expect("registers a route");
    builder.build().expect("a valid table")
}

// ---------------------------------------------------------------------------
// The proxy under test
// ---------------------------------------------------------------------------

/// Knobs the tests need to vary.
pub struct ProxyOptions {
    pub tls: bool,
    pub certs: Arc<CertStore>,
    pub upstream: UpstreamConfig,
    pub grace: Duration,
    /// Serving runtimes. One by default: a test asserting on behaviour wants
    /// the same answer every run, and the suite starts dozens of proxies at
    /// once, so one-per-core each would be a thread per core per test.
    /// `lifecycle.rs` covers the multi-runtime path explicitly.
    pub workers: Option<usize>,
    /// Require a PROXY protocol header on the traffic listeners.
    pub proxy_protocol: Option<Duration>,
}

impl Default for ProxyOptions {
    fn default() -> Self {
        ProxyOptions {
            tls: false,
            certs: Arc::new(CertStore::new()),
            upstream: UpstreamConfig::default(),
            grace: Duration::from_secs(10),
            workers: Some(1),
            proxy_protocol: None,
        }
    }
}

/// A running proxy on ephemeral ports.
pub struct TestProxy {
    pub http: SocketAddr,
    pub https: Option<SocketAddr>,
    pub admin: SocketAddr,
    pub routes: Arc<SharedRouteTable>,
    pub certs: Arc<CertStore>,
    pub metrics: Arc<Metrics>,
    pub readiness: ReadinessFlag,
    handle: ShutdownHandle,
    task: Option<JoinHandle<std::io::Result<()>>>,
}

impl TestProxy {
    pub async fn start(table: RouteTable) -> TestProxy {
        Self::start_with(table, ProxyOptions::default()).await
    }

    pub async fn start_with(table: RouteTable, options: ProxyOptions) -> TestProxy {
        let routes = Arc::new(SharedRouteTable::new(table));
        let config = ProxyConfig {
            http: Some(ListenerConfig::new(loopback())),
            https: options.tls.then(|| ListenerConfig::new(loopback())),
            admin: Some(ListenerConfig::new(loopback())),
            upstream: options.upstream,
            shutdown_grace: options.grace,
            worker_threads: options.workers,
            proxy_protocol: options.proxy_protocol,
            ..ProxyConfig::default()
        };

        let readiness = ReadinessFlag::new();
        let server = Server::bind_with(
            config,
            Arc::clone(&routes),
            Arc::clone(&options.certs),
            readiness.clone(),
        )
        .expect("the proxy binds");

        let http = server.http_addr().expect("an http port");
        let https = server.https_addr();
        let admin = server.admin_addr().expect("an admin port");
        let metrics = Arc::clone(server.metrics());

        let (handle, shutdown) = Shutdown::channel();
        let task = tokio::spawn(server.run(shutdown));

        TestProxy {
            http,
            https,
            admin,
            routes,
            certs: options.certs,
            metrics,
            readiness,
            handle,
            task: Some(task),
        }
    }

    /// Signals shutdown and waits for the drain to finish.
    pub async fn shutdown(mut self) -> std::io::Result<()> {
        self.handle.shutdown();
        match self.task.take() {
            Some(task) => task.await.expect("the server task did not panic"),
            None => Ok(()),
        }
    }

    /// Signals shutdown without waiting, for tests that assert on what happens
    /// during the drain.
    pub fn signal_shutdown(&self) {
        self.handle.shutdown();
    }

    pub async fn wait(mut self) -> std::io::Result<()> {
        match self.task.take() {
            Some(task) => task.await.expect("the server task did not panic"),
            None => Ok(()),
        }
    }
}

impl Drop for TestProxy {
    fn drop(&mut self) {
        self.handle.shutdown();
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

// ---------------------------------------------------------------------------
// Client
// ---------------------------------------------------------------------------

/// A collected response.
#[derive(Debug)]
pub struct Reply {
    pub status: StatusCode,
    pub headers: HeaderMap,
    pub body: Bytes,
}

impl Reply {
    pub fn text(&self) -> &str {
        std::str::from_utf8(&self.body).unwrap_or("<not utf-8>")
    }

    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers.get(name).and_then(|value| value.to_str().ok())
    }

    pub fn upstream(&self) -> &str {
        self.header("x-upstream").unwrap_or("<none>")
    }
}

/// A request builder addressed at `host` with an origin-form target.
pub fn request(host: &str, path: &str) -> http::request::Builder {
    Request::builder().uri(path).header(http::header::HOST, host)
}

/// Sends one request over a fresh HTTP/1.1 connection.
pub async fn send(addr: SocketAddr, request: Request<TestBody>) -> Reply {
    let (mut sender, connection) = handshake(addr).await;
    let driver = tokio::spawn(connection);
    let response = sender.send_request(request).await.expect("a response");
    let reply = collect(response).await;
    driver.abort();
    reply
}

/// A `GET` for `path` on `host`.
pub async fn get(addr: SocketAddr, host: &str, path: &str) -> Reply {
    send(
        addr,
        request(host, path).body(empty_body()).expect("a request"),
    )
    .await
}

/// Sends one request and returns before the body has been read, so a test can
/// observe when each frame arrives.
pub async fn send_streaming(addr: SocketAddr, request: Request<TestBody>) -> Response<Incoming> {
    let (mut sender, connection) = handshake(addr).await;
    // Detached on purpose: the connection has to keep driving while the test
    // reads the body frame by frame.
    tokio::spawn(connection);
    sender.send_request(request).await.expect("a response")
}

/// Sends `count` requests down one keep-alive connection.
///
/// Reusing the connection is not just faster: it is what a load-balancing or
/// canary test wants, because opening a fresh socket per request would also be
/// exercising the accept path a few thousand times for no reason.
pub async fn send_many(addr: SocketAddr, host: &str, path: &str, count: usize) -> Vec<Reply> {
    let (mut sender, connection) = handshake(addr).await;
    let driver = tokio::spawn(connection);
    let mut replies = Vec::with_capacity(count);
    for _ in 0..count {
        let request = request(host, path).body(empty_body()).expect("a request");
        let response = sender.send_request(request).await.expect("a response");
        replies.push(collect(response).await);
    }
    driver.abort();
    replies
}

/// Opens an HTTP/1.1 connection, returning the request sender and the driver
/// future the caller has to poll (normally by spawning it).
///
/// The driver is `impl Future` because hyper does not export the type
/// `with_upgrades` produces. Upgrades are enabled unconditionally: a connection
/// that cannot be hijacked would make the WebSocket tests fail in a way that
/// looks like a proxy bug.
pub async fn handshake(
    addr: SocketAddr,
) -> (
    hyper::client::conn::http1::SendRequest<TestBody>,
    impl Future<Output = ()> + Send,
) {
    let stream = TcpStream::connect(addr).await.expect("connect to the proxy");
    let (sender, connection) = hyper::client::conn::http1::handshake(TokioIo::new(stream))
        .await
        .expect("client handshake");
    let driver = async move {
        let _ = connection.with_upgrades().await;
    };
    (sender, driver)
}

pub async fn collect(response: Response<Incoming>) -> Reply {
    let status = response.status();
    let headers = response.headers().clone();
    let body = response
        .into_body()
        .collect()
        .await
        .expect("a complete body")
        .to_bytes();
    Reply {
        status,
        headers,
        body,
    }
}

// ---------------------------------------------------------------------------
// TLS
// ---------------------------------------------------------------------------

/// A self-signed certificate for one or more names.
pub struct TestCert {
    pub chain: Vec<CertificateDer<'static>>,
    pub key: PrivateKeyDer<'static>,
}

impl TestCert {
    pub fn generate(names: &[&str]) -> TestCert {
        let owned: Vec<String> = names.iter().map(|name| (*name).to_owned()).collect();
        let issued = rcgen::generate_simple_self_signed(owned).expect("a certificate");
        TestCert {
            chain: vec![issued.cert.der().clone()],
            key: PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(
                issued.signing_key.serialize_der(),
            )),
        }
    }

    pub fn certified(&self) -> Arc<rustls::sign::CertifiedKey> {
        Arc::new(
            ramjet_proxy::tls::certified_key(self.chain.clone(), self.key.clone_key())
                .expect("a usable key"),
        )
    }
}

/// Loads `certs` into a store keyed by handle id.
pub fn cert_store(certs: &[(u64, &TestCert)]) -> Arc<CertStore> {
    let mut map: HashMap<u64, Arc<rustls::sign::CertifiedKey>> = HashMap::new();
    for (id, cert) in certs {
        map.insert(*id, cert.certified());
    }
    Arc::new(CertStore::with_certs(map))
}

pub fn tls_client_config(
    trusted: &[&TestCert],
    alpn: &[&[u8]],
) -> Arc<rustls::ClientConfig> {
    let mut roots = rustls::RootCertStore::empty();
    for cert in trusted {
        for der in &cert.chain {
            roots.add(der.clone()).expect("a usable root");
        }
    }
    let mut config = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .expect("protocol versions")
    .with_root_certificates(roots)
    .with_no_client_auth();
    config.alpn_protocols = alpn.iter().map(|p| p.to_vec()).collect();
    Arc::new(config)
}

/// Connects over TLS with the given SNI name, returning the stream.
pub async fn tls_connect(
    addr: SocketAddr,
    server_name: &str,
    config: Arc<rustls::ClientConfig>,
) -> tokio_rustls::client::TlsStream<TcpStream> {
    let name = rustls::pki_types::ServerName::try_from(server_name.to_owned())
        .expect("a valid server name");
    let stream = TcpStream::connect(addr).await.expect("tcp connect");
    tokio_rustls::TlsConnector::from(config)
        .connect(name, stream)
        .await
        .expect("tls handshake")
}

/// Sends one request over a new TLS connection, negotiating HTTP/1.1.
pub async fn send_tls(
    addr: SocketAddr,
    server_name: &str,
    config: Arc<rustls::ClientConfig>,
    request: Request<TestBody>,
) -> Reply {
    let stream = tls_connect(addr, server_name, config).await;
    let (mut sender, connection) = hyper::client::conn::http1::handshake(TokioIo::new(stream))
        .await
        .expect("client handshake");
    let driver = tokio::spawn(async move {
        let _ = connection.await;
    });
    let response = sender.send_request(request).await.expect("a response");
    let reply = collect(response).await;
    driver.abort();
    reply
}

/// Sends one request over a new TLS connection, negotiating HTTP/2.
pub async fn send_tls_h2(
    addr: SocketAddr,
    server_name: &str,
    config: Arc<rustls::ClientConfig>,
    request: Request<TestBody>,
) -> (Reply, Option<Vec<u8>>) {
    let stream = tls_connect(addr, server_name, config).await;
    let negotiated = stream
        .get_ref()
        .1
        .alpn_protocol()
        .map(<[u8]>::to_vec);
    let (mut sender, connection) =
        hyper::client::conn::http2::handshake(TokioExecutor::new(), TokioIo::new(stream))
            .await
            .expect("h2 handshake");
    let driver = tokio::spawn(async move {
        let _ = connection.await;
    });
    let response = sender.send_request(request).await.expect("a response");
    let reply = collect(response).await;
    driver.abort();
    (reply, negotiated)
}

// ---------------------------------------------------------------------------
// Raw socket helpers
// ---------------------------------------------------------------------------

/// Writes `bytes` and reads until the peer closes or `limit` bytes arrive.
pub async fn write_then_read(stream: &mut TcpStream, bytes: &[u8], limit: usize) -> Vec<u8> {
    stream.write_all(bytes).await.expect("write");
    let mut out = Vec::new();
    let mut chunk = [0u8; 4096];
    while out.len() < limit {
        match stream.read(&mut chunk).await {
            Ok(0) | Err(_) => break,
            Ok(read) => out.extend_from_slice(&chunk[..read]),
        }
    }
    out
}
