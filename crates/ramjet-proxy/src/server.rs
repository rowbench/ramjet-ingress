//! Binding the listeners, accepting connections, and draining on shutdown.
//!
//! # Bind, then run
//!
//! [`Server::bind`] does every fallible thing — binding sockets, building the
//! TLS configuration — and returns a server whose ports are already known.
//! [`Server::run`] then consumes it and never fails for a configuration reason.
//! Splitting the two is what lets a caller ask for port `0`, read back what the
//! kernel assigned, and hand that to a test client; it also means a
//! misconfiguration is reported at startup instead of at the first connection.
//!
//! # Draining
//!
//! Kubernetes terminates a pod by sending `SIGTERM` and, some time later,
//! `SIGKILL`. In between, the pod is still in the Service's endpoint list for a
//! moment, and requests keep arriving. So shutdown is two steps, in this order:
//!
//! 1. **Stop accepting.** The listeners are dropped, which closes them. New
//!    connections are refused immediately and the load balancer moves on,
//!    rather than being accepted and then abandoned.
//! 2. **Finish what is in flight.** Every live connection is told to stop
//!    reading new requests and close once the current one completes, and the
//!    server waits — up to `shutdown_grace`, default 30s — for that to happen.
//!
//! This is the part ingress-nginx cannot do for a *configuration* change,
//! because a reload replaces the workers. Here it happens only for an actual
//! process shutdown, which is the only time it should.
//!
//! Upgraded connections — WebSockets and anything else that went through a
//! `101` — are the one exception, and deliberately so. Once a connection has
//! been hijacked there is no request boundary left to finish at, and a
//! WebSocket can legitimately stay open for hours; waiting for one would mean
//! every rolling update stalls until the deadline and then kills it anyway.
//! Tunnels are left running and end with the process, which is what nginx does
//! with them at worker shutdown too.
//!
//! # One task per connection, one runtime per core
//!
//! The accept loop does nothing but accept: the TLS handshake, the HTTP
//! parsing, and the proxying all happen elsewhere. A handshake takes tens of
//! microseconds of arithmetic, and doing it inline would let one client stall
//! every other connection waiting to be accepted.
//!
//! That "elsewhere" is one **`current_thread` runtime per core**, and the
//! accepted socket is handed to one of them round-robin. A connection stays on
//! the runtime it landed on for its whole life, and so does everything the
//! requests on it touch: the upstream connections they dispatch to, the pool
//! those come out of, and the timers that bound them.
//!
//! The alternative — one multi-threaded runtime, which is what this server used
//! to be — is a connection whose work migrates. A request arrives on worker A,
//! is dispatched to an upstream connection task that worker B owns, and the
//! response wakes A again from B. Each of those crossings is an atomic on a
//! contended cache line and, when the other worker has parked, a wakeup
//! syscall. Measured on this workload it cost **43% more CPU per request** than
//! the same code on one thread: 26.7us against 18.7us, with throughput per core
//! falling from 53.6k rps to 37.4k. `bench/PROFILE.md` has the numbers and the
//! experiment that produced them.
//!
//! Two consequences worth stating, because they are the price:
//!
//! - **Each runtime keeps its own upstream pool.** `pool_max_idle_per_host` is
//!   therefore a per-runtime ceiling, and the process-wide maximum is that
//!   number times the runtime count. Idle connections are opened on demand, so
//!   this costs file descriptors only where the traffic exists to need them.
//! - **A connection is bound to its runtime for life.** A single very busy
//!   connection cannot be spread across cores. For an ingress that is the right
//!   trade — the load is many connections, not one — but it is a real
//!   difference from a work-stealing scheduler.
//!
//! The admin listener is deliberately left on the caller's runtime: a scrape
//! every fifteen seconds has no business taking a slot on a serving core.
//!
//! # What an idle connection costs, and where it goes
//!
//! `bench/thesis/RESULTS.md`'s benchmark 4 is the one ingress-nginx won, so it
//! is worth writing down exactly where the memory goes rather than leaving the
//! next reader to rediscover it.
//!
//! A connection that has been accepted but has not sent a byte costs about
//! 1.7 KiB of RSS: the spawned task, and hyper-util's version sniffer sitting
//! on a 24-byte buffer waiting to tell HTTP/1 from an HTTP/2 preface. The first
//! request is what makes it expensive. Building the HTTP/1 connection allocates
//! two buffers of `INIT_BUFFER_SIZE` — 8 KiB to read into, 8 KiB to write
//! response heads from — and hyper never shrinks or drops either one while the
//! connection lives. Measured by patching that constant down to 1 KiB, those
//! two buffers are 14 KiB of the ~17 KiB an idle keep-alive connection holds.
//!
//! There is no public API that lowers it: `max_buf_size` is a ceiling on how
//! far the read buffer may *grow* and hyper refuses to set it below
//! `INIT_BUFFER_SIZE` — see [`MIN_MAX_BUF_SIZE`]. So 16 KiB per idle
//! keep-alive connection is this engine's floor until hyper's initial
//! allocation follows its configured maximum. nginx is cheaper here for a
//! structural reason and not an accidental one: it hands a connection's request
//! buffers back to its pool when the connection goes idle, and keeps only the
//! connection object.
//!
//! What was in this crate's control has been taken out. The response future no
//! longer sits inside the connection (see `serve_http`), which was 2.9 KiB per
//! connection held whether or not a request was in flight.
//!
//! # The PROXY protocol
//!
//! With `proxy_protocol` set, a connection on a traffic listener must open with
//! a PROXY header naming the real client, and the address it names replaces the
//! socket's peer in [`ConnInfo`] — so `X-Forwarded-For` and `X-Real-IP` describe
//! the client rather than the load balancer. The read happens in the connection
//! task and *before* the TLS handshake, which is the order the wire has: the
//! balancer sends the header itself and then relays the client's ClientHello.
//!
//! The admin listener never reads one. See [`proxy_protocol`] for the trust
//! model, which is the part that matters: enabling this on a socket the
//! internet can reach hands out IP spoofing.

use std::convert::Infallible;
use std::io;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};
use std::sync::Arc;
use std::time::Duration;

use http::header::{self, HeaderValue};
use hyper::service::service_fn;
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto;
use hyper_util::server::graceful::{GracefulShutdown, Watcher};
use ramjet_router::SharedRouteTable;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot, watch};
use tokio_rustls::TlsAcceptor;

use crate::admin::{self, AdminState, ReadinessFlag};
use crate::body::ProxyBody;
use crate::forward::{self, ConnInfo, ProxyState, Scheme};
use crate::history::{GenerationHistory, DEFAULT_HISTORY_SIZE};
use crate::http3;
use crate::listener::{Listener, ListenerConfig};
use crate::metrics::{ConnectionGuard, Metrics};
use crate::mirror::{Mirror, DEFAULT_MIRROR_MAX_BODY};
use crate::proxy_protocol;
use crate::tls::{self, CertStore, SniResolver};
use crate::upstream::{Upstream, UpstreamConfig};

/// Default plaintext port.
pub const DEFAULT_HTTP_PORT: u16 = 8080;
/// Default TLS port.
pub const DEFAULT_HTTPS_PORT: u16 = 8443;
/// Default admin port, matching the ingress-nginx convention.
pub const DEFAULT_ADMIN_PORT: u16 = 10254;

/// Ceiling on the HTTP/1 read and write buffers of one client connection.
///
/// hyper's own default is 408 KiB, and the buffer never shrinks again for the
/// life of the connection: one client that sends a 400 KiB header block pins
/// 400 KiB until it disconnects, and ten thousand of them pin four gigabytes.
/// nginx bounds the same thing at 32 KiB (`large_client_header_buffers 4 8k`),
/// so 64 KiB accepts every request nginx would and still bounds the worst case
/// at a sixth of hyper's.
///
/// This does **not** move the idle-connection footprint. hyper allocates its
/// first buffer at a fixed 8 KiB regardless of this number — see
/// [`MIN_MAX_BUF_SIZE`] — so a connection carrying ordinary requests costs the
/// same either way. What this bounds is the tail.
pub const DEFAULT_MAX_BUF_SIZE: usize = 64 * 1024;

/// The smallest ceiling hyper accepts, and the size it allocates up front.
///
/// hyper panics below this, because it is also `INIT_BUFFER_SIZE`: the first
/// read reserves 8 KiB and the write buffer is built with 8 KiB of capacity
/// before a byte has arrived. Two of those are 16 KiB, and they are what an
/// idle keep-alive connection actually costs — the floor this crate cannot get
/// under without a change to hyper.
pub const MIN_MAX_BUF_SIZE: usize = 8 * 1024;

/// How long to pause after an accept error before trying again.
///
/// Running out of file descriptors makes `accept` fail immediately and forever,
/// which without this turns the accept loop into a spin at 100% of a core —
/// removing the only chance the process had of recovering.
const ACCEPT_BACKOFF: Duration = Duration::from_millis(10);

/// Everything the data plane needs to start listening.
#[derive(Debug, Clone)]
pub struct ProxyConfig {
    /// Plaintext listener, or `None` to serve TLS only.
    pub http: Option<ListenerConfig>,
    /// TLS listener, or `None` to serve plaintext only.
    pub https: Option<ListenerConfig>,
    /// Admin listener, or `None` to disable metrics and probes.
    pub admin: Option<ListenerConfig>,
    /// Upstream timeouts and pooling.
    pub upstream: UpstreamConfig,
    /// How long in-flight requests get to finish after a shutdown signal.
    pub shutdown_grace: Duration,
    /// Serving runtimes to start, one per thread. `None` means one per core.
    ///
    /// See the module docs for what a serving runtime owns. `Some(0)` is
    /// treated as one: a data plane with nowhere to serve is not a
    /// configuration, it is a hang.
    pub worker_threads: Option<usize>,
    /// Ceiling on one connection's HTTP/1 buffers; see [`DEFAULT_MAX_BUF_SIZE`].
    ///
    /// Clamped up to [`MIN_MAX_BUF_SIZE`], which is the smallest hyper accepts.
    pub max_buf_size: usize,
    /// Generations kept for `/admin/generations` and `/admin/rollback`.
    ///
    /// Each one holds its route table and its parsed certificates alive, so
    /// this is the knob that trades memory for how far back an operator can
    /// roll. Clamped up to one; see [`GenerationHistory`].
    pub history_size: usize,
    /// Largest request body copied to a mirror backend.
    ///
    /// A route with `ramjet.dev/mirror-backend` reads up to this many bytes in
    /// order to send the same body twice; a request over the cap is forwarded
    /// normally and not mirrored. See [`mirror`](crate::mirror) for why the
    /// number is small and why exceeding it costs the primary nothing.
    pub mirror_max_body: usize,

    /// Require a PROXY protocol header on the traffic listeners, waiting this
    /// long for it. `None` — the default — disables it.
    ///
    /// This covers the HTTP and HTTPS listeners and deliberately **not** the
    /// admin one: metrics are scraped by Prometheus and the probes are called
    /// by the kubelet, neither of which speaks the protocol, and both of which
    /// reach the pod directly rather than through the load balancer.
    ///
    /// Enabling it changes who the data plane believes its clients are. Read
    /// the trust model in [`crate::proxy_protocol`] before
    /// turning it on: the header *is* the client identity, so a listener that
    /// accepts one must be reachable only by a load balancer that always sends
    /// it.
    pub proxy_protocol: Option<Duration>,

    /// UDP address for the experimental HTTP/3 listener, or `None` — the
    /// default — for no QUIC socket at all.
    ///
    /// The port is normally the TLS listener's own, in UDP: `alt-svc` tells a
    /// client to retry the same authority over QUIC, so a different port would
    /// have to be advertised and separately reachable.
    ///
    /// Off costs nothing. No socket is bound, no thread is started, no `alt-svc`
    /// header is added, and quinn never runs. See [`http3`](crate::http3) for
    /// what is and is not supported when it is on.
    pub http3: Option<SocketAddr>,
}

impl Default for ProxyConfig {
    fn default() -> Self {
        let all = |port: u16| ListenerConfig::new(SocketAddr::from(([0, 0, 0, 0], port)));
        ProxyConfig {
            http: Some(all(DEFAULT_HTTP_PORT)),
            https: Some(all(DEFAULT_HTTPS_PORT)),
            admin: Some(all(DEFAULT_ADMIN_PORT)),
            upstream: UpstreamConfig::default(),
            // Kubernetes' default `terminationGracePeriodSeconds` is 30, so a
            // longer drain would just be interrupted by SIGKILL — the deadline
            // would be a lie told to whoever set it.
            shutdown_grace: Duration::from_secs(30),
            worker_threads: None,
            max_buf_size: DEFAULT_MAX_BUF_SIZE,
            history_size: DEFAULT_HISTORY_SIZE,
            mirror_max_body: DEFAULT_MIRROR_MAX_BODY,
            // Off, and it has to be: a listener that requires the header
            // refuses every connection that does not carry one, so defaulting
            // it on would mean a fresh deployment serves nothing.
            proxy_protocol: None,
            // Experimental, and a UDP port an operator did not ask for is a
            // port a security review did not either.
            http3: None,
        }
    }
}

/// One `auto::Builder`, configured the way every listener in this process
/// wants it.
///
/// Built once per runtime and shared by every connection on it: the builder is
/// read-only after this and holds no per-connection state.
fn connection_builder(max_buf_size: usize) -> Arc<auto::Builder<TokioExecutor>> {
    let mut builder = auto::Builder::new(TokioExecutor::new());
    builder
        .http1()
        .max_buf_size(max_buf_size.max(MIN_MAX_BUF_SIZE));
    Arc::new(builder)
}

/// Bound listeners, ready to serve.
///
/// The serving runtimes are *not* started here. Binding is the fallible part
/// and happens on the caller's thread; the runtimes are threads, and starting
/// threads in a constructor that a caller might then drop is a way to leak
/// them.
#[derive(Debug)]
pub struct Server {
    routes: Arc<SharedRouteTable>,
    upstream: UpstreamConfig,
    worker_threads: usize,
    admin_state: Arc<AdminState>,
    http: Option<Listener>,
    https: Option<Listener>,
    admin: Option<Listener>,
    tls: Option<Arc<rustls::ServerConfig>>,
    metrics: Arc<Metrics>,
    history: Arc<GenerationHistory>,
    readiness: ReadinessFlag,
    grace: Duration,
    max_buf_size: usize,
    mirror_max_body: usize,
    proxy_protocol: Option<Duration>,
    http3: Option<http3::Listener>,
}

impl Server {
    /// Binds every configured listener and prepares the TLS configuration.
    ///
    /// The returned server owns a fresh [`ReadinessFlag`], reachable through
    /// [`readiness`](Self::readiness); use [`bind_with`](Self::bind_with) to
    /// supply one the caller already holds.
    ///
    /// Must be called from inside a tokio runtime — see
    /// [`Listener::bind`](crate::listener::Listener::bind).
    pub fn bind(
        config: ProxyConfig,
        routes: Arc<SharedRouteTable>,
        certs: Arc<CertStore>,
    ) -> io::Result<Self> {
        Self::bind_with(config, routes, certs, ReadinessFlag::new())
    }

    /// Binds every configured listener, gating `/readyz` on `readiness`.
    pub fn bind_with(
        config: ProxyConfig,
        routes: Arc<SharedRouteTable>,
        certs: Arc<CertStore>,
        readiness: ReadinessFlag,
    ) -> io::Result<Self> {
        let http = config.http.as_ref().map(Listener::bind).transpose()?;
        let https = config.https.as_ref().map(Listener::bind).transpose()?;
        let admin = config.admin.as_ref().map(Listener::bind).transpose()?;

        // Built only when there is something to serve it on: a TLS config over
        // an empty cert store fails every handshake, and constructing one for a
        // listener that does not exist would hide that.
        //
        // One resolver for both listeners, and that is the point rather than a
        // saving. TLS and QUIC ask the same question — which certificate serves
        // this name — and sharing the resolver means they cannot answer it
        // differently, now or after a rotation: it is the same `SniMap` in the
        // same route table and the same `CertStore`, published by the same two
        // stores in the same order.
        let resolver = (https.is_some() || config.http3.is_some())
            .then(|| Arc::new(SniResolver::new(Arc::clone(&routes), Arc::clone(&certs))));
        let tls = match (&https, &resolver) {
            (Some(_), Some(resolver)) => {
                let config = tls::server_config(Arc::clone(resolver))
                    .map_err(|error| io::Error::other(error.to_string()))?;
                Some(Arc::new(config))
            }
            _ => None,
        };
        let http3 = match (config.http3, &resolver) {
            (Some(addr), Some(resolver)) => {
                Some(http3::Listener::bind(addr, Arc::clone(resolver))?)
            }
            _ => None,
        };

        let metrics = Arc::new(Metrics::new());
        // Built here rather than handed in, because it publishes into exactly
        // the two stores this server was bound over. A history wired to a
        // different route table than the one being served would roll back to
        // somewhere nobody is listening.
        let history = Arc::new(GenerationHistory::new(
            Arc::clone(&routes),
            Arc::clone(&certs),
            config.history_size,
        ));
        let admin_state = Arc::new(AdminState {
            metrics: Arc::clone(&metrics),
            routes: Arc::clone(&routes),
            readiness: readiness.clone(),
            history: Arc::clone(&history),
        });

        Ok(Server {
            routes,
            upstream: config.upstream,
            worker_threads: worker_threads(config.worker_threads),
            admin_state,
            http,
            https,
            admin,
            tls,
            metrics,
            history,
            readiness,
            grace: config.shutdown_grace,
            max_buf_size: config.max_buf_size,
            mirror_max_body: config.mirror_max_body,
            proxy_protocol: config.proxy_protocol,
            http3,
        })
    }

    /// The plaintext address actually bound.
    pub fn http_addr(&self) -> Option<SocketAddr> {
        self.http.as_ref().and_then(|l| l.local_addr().ok())
    }

    /// The TLS address actually bound.
    pub fn https_addr(&self) -> Option<SocketAddr> {
        self.https.as_ref().and_then(|l| l.local_addr().ok())
    }

    /// The UDP address the HTTP/3 listener bound, if it is enabled.
    pub fn http3_addr(&self) -> Option<SocketAddr> {
        self.http3.as_ref().map(http3::Listener::local_addr)
    }

    /// The admin address actually bound.
    pub fn admin_addr(&self) -> Option<SocketAddr> {
        self.admin.as_ref().and_then(|l| l.local_addr().ok())
    }

    /// The data-plane counters, for a caller that wants to read them directly.
    pub fn metrics(&self) -> &Arc<Metrics> {
        &self.metrics
    }

    /// The generation ring this server publishes through.
    ///
    /// Whoever owns the configuration — the Kubernetes applier, or dev mode's
    /// one-shot load — records each generation here rather than storing into
    /// the route table itself, so that a rollback has something to roll back
    /// to and the publication gate has one place to live.
    pub fn history(&self) -> &Arc<GenerationHistory> {
        &self.history
    }

    /// The flag gating `/readyz`.
    pub fn readiness(&self) -> &ReadinessFlag {
        &self.readiness
    }

    /// Accepts until `shutdown` fires, then drains.
    ///
    /// Returns `TimedOut` if connections were still open when the grace period
    /// expired. That is an outcome, not a crash — the caller decides whether it
    /// is worth complaining about — but it is reported rather than swallowed,
    /// because silently abandoning in-flight requests is how a "graceful"
    /// shutdown quietly stops being one.
    pub async fn run(self, mut shutdown: Shutdown) -> io::Result<()> {
        let Server {
            routes,
            upstream,
            worker_threads,
            admin_state,
            http,
            https,
            admin,
            tls,
            metrics,
            grace,
            max_buf_size,
            mirror_max_body,
            proxy_protocol,
            http3,
            ..
        } = self;

        // Advertised only when there is actually a UDP socket to advertise. An
        // `alt-svc` naming a port nothing is listening on costs every client
        // that believes it a failed QUIC attempt and a fallback, on every
        // connection, until the advertisement expires.
        let alt_svc = http3
            .as_ref()
            .and_then(|listener| http3::alt_svc_value(listener.local_addr().port()));

        let acceptor = tls.map(TlsAcceptor::from);
        let mut workers = Workers::start(
            worker_threads,
            &LaneConfig {
                routes: Arc::clone(&routes),
                metrics: Arc::clone(&metrics),
                upstream,
                acceptor: acceptor.clone(),
                grace,
                max_buf_size,
                mirror_max_body,
                proxy_protocol,
                alt_svc,
            },
        )?;

        // The QUIC listener gets a thread of its own rather than a share of the
        // round-robin: it is not handed accepted sockets, it owns an endpoint.
        // Its shard index continues past the TCP runtimes' so that two of them
        // never write to the same per-route counter block.
        let http3 = http3
            .map(|listener| {
                http3::spawn(
                    listener,
                    http3::ServeConfig {
                        routes: Arc::clone(&routes),
                        metrics: Arc::clone(&metrics),
                        upstream,
                        mirror_max_body,
                        grace,
                        shard: worker_threads,
                    },
                    shutdown.clone(),
                )
            })
            .transpose()?;

        // Admin lives on the caller's runtime; see the module docs.
        let graceful = GracefulShutdown::new();
        let builder = connection_builder(max_buf_size);

        loop {
            tokio::select! {
                // Shutdown wins a tie: once asked to stop, there is no reason
                // to take one more connection just because it arrived first.
                biased;
                () = shutdown.recv() => break,

                result = accept(http.as_ref()) => match result {
                    Ok((stream, remote)) => workers.dispatch(
                        stream,
                        metrics.connection_opened(),
                        ConnInfo { remote, scheme: Scheme::Http },
                    ),
                    Err(_) => tokio::time::sleep(ACCEPT_BACKOFF).await,
                },

                result = accept(https.as_ref()) => match (result, &acceptor) {
                    (Ok((stream, remote)), Some(_)) => workers.dispatch(
                        stream,
                        metrics.connection_opened(),
                        ConnInfo { remote, scheme: Scheme::Https },
                    ),
                    (Ok(_), None) => {}
                    (Err(_), _) => tokio::time::sleep(ACCEPT_BACKOFF).await,
                },

                result = accept(admin.as_ref()) => match result {
                    Ok((stream, _)) => serve_admin(
                        Arc::clone(&builder),
                        graceful.watcher(),
                        Arc::clone(&admin_state),
                        stream,
                    ),
                    Err(_) => tokio::time::sleep(ACCEPT_BACKOFF).await,
                },
            }
        }

        // Step one: stop accepting. Closing the sockets is what makes the load
        // balancer look elsewhere instead of queueing behind a draining pod.
        drop(http);
        drop(https);
        drop(admin);

        // Step two: let what is already running finish. The serving runtimes
        // drain in parallel with the admin listener rather than after it —
        // they are the ones holding client requests, and making them wait for
        // a metrics scrape to finish would be the wrong order. The QUIC
        // endpoint drains alongside them for the same reason, and closes its
        // own socket: it has no listener to drop, because the endpoint is what
        // keeps delivering packets to connections that are still finishing.
        let (drained, admin_drained, h3_drained) = tokio::join!(
            workers.drain(),
            tokio::time::timeout(grace, graceful.shutdown()),
            drain_http3(http3),
        );

        if !drained || !h3_drained || admin_drained.is_err() {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "shutdown grace period expired with connections still open",
            ));
        }
        Ok(())
    }
}

/// Waits for the HTTP/3 runtime to drain, or reports success if there was not
/// one.
///
/// A separate function rather than an `Option` dance inside the `join!`: an
/// absent listener has to contribute `true` to the outcome, and writing that
/// inline is how a disabled feature ends up failing a shutdown.
async fn drain_http3(handle: Option<http3::Handle>) -> bool {
    match handle {
        Some(handle) => handle.drain().await,
        None => true,
    }
}

/// How many serving runtimes to start.
///
/// `available_parallelism` reads the cgroup CPU limit, so a pod with
/// `limits.cpu: 2` gets two runtimes rather than one per host core.
fn worker_threads(configured: Option<usize>) -> usize {
    configured
        .unwrap_or_else(|| std::thread::available_parallelism().map_or(1, |n| n.get()))
        .max(1)
}

/// One accepted connection, on its way to a serving runtime.
///
/// The socket crosses as a `std::net::TcpStream` because a `tokio::net`
/// one is registered with the reactor of the runtime that accepted it;
/// `into_std` deregisters it so the receiving runtime can register it with its
/// own. The guard travels with it so the connection gauge counts from accept,
/// not from whenever the serving runtime got round to it.
struct Job {
    stream: std::net::TcpStream,
    guard: ConnectionGuard,
    conn: ConnInfo,
}

/// Everything one serving runtime is configured with.
///
/// A struct rather than a parameter list: every field is handed to every lane
/// unchanged, and threading six of them positionally through both `start` and
/// `serve_lane` was one transposition away from a bug no compiler would catch.
#[derive(Clone)]
struct LaneConfig {
    routes: Arc<SharedRouteTable>,
    metrics: Arc<Metrics>,
    upstream: UpstreamConfig,
    acceptor: Option<TlsAcceptor>,
    grace: Duration,
    max_buf_size: usize,
    mirror_max_body: usize,
    proxy_protocol: Option<Duration>,
    /// The `alt-svc` value advertising the QUIC listener, when there is one.
    alt_svc: Option<HeaderValue>,
}

/// The serving runtimes and the round-robin over them.
struct Workers {
    lanes: Vec<mpsc::UnboundedSender<Job>>,
    done: Vec<oneshot::Receiver<bool>>,
    next: usize,
}

impl Workers {
    fn start(count: usize, config: &LaneConfig) -> io::Result<Workers> {
        let mut lanes = Vec::with_capacity(count);
        let mut done = Vec::with_capacity(count);

        for index in 0..count {
            let (jobs_tx, jobs_rx) = mpsc::unbounded_channel();
            let (done_tx, done_rx) = oneshot::channel();
            let config = config.clone();

            // Built here rather than on the new thread, and moved into it. A
            // runtime that cannot be created is a startup failure the caller
            // can report; discovering it on the thread instead would leave a
            // lane that accepts its share of the round-robin and serves none
            // of it, which is a fifth of the traffic disappearing into a
            // socket nobody is reading.
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()?;

            std::thread::Builder::new()
                .name(format!("ramjet-serve-{index}"))
                .spawn(move || {
                    let drained = serve_lane(runtime, index, config, jobs_rx);
                    // A receiver that has gone away means `run` already
                    // returned, which is not this thread's problem.
                    let _ = done_tx.send(drained);
                })?;

            lanes.push(jobs_tx);
            done.push(done_rx);
        }

        Ok(Workers {
            lanes,
            done,
            next: 0,
        })
    }

    /// Hands a connection to the next runtime.
    ///
    /// Round-robin rather than least-loaded: picking the shortest queue would
    /// mean reading every lane's depth on the accept path, and connection
    /// counts even out on their own across anything longer than a burst.
    fn dispatch(&mut self, stream: TcpStream, guard: ConnectionGuard, conn: ConnInfo) {
        let Ok(stream) = stream.into_std() else { return };
        if self.lanes.is_empty() {
            return;
        }
        let index = self.next % self.lanes.len();
        self.next = self.next.wrapping_add(1);
        if let Some(lane) = self.lanes.get(index) {
            // A closed lane means that runtime is gone; dropping the socket
            // closes it, which is the honest answer to a client whose
            // connection this process can no longer serve.
            let _ = lane.send(Job {
                stream,
                guard,
                conn,
            });
        }
    }

    /// Closes every lane and waits for the runtimes to finish draining.
    ///
    /// Returns whether every one of them drained inside its grace period.
    async fn drain(&mut self) -> bool {
        self.lanes.clear();
        let mut drained = true;
        for done in &mut self.done {
            // A thread that vanished without reporting cannot be said to have
            // drained cleanly.
            drained &= done.await.unwrap_or(false);
        }
        drained
    }
}

/// Everything one serving runtime hands to each of its connections.
///
/// Shared through a single `Arc` rather than cloning three of them per
/// connection: all four fields are read-only for the life of the lane.
struct Lane {
    builder: Arc<auto::Builder<TokioExecutor>>,
    state: Arc<ProxyState>,
    acceptor: Option<TlsAcceptor>,
    proxy_protocol: Option<Duration>,
    /// Whether a rejected PROXY header has already been reported on this lane.
    ///
    /// The failure this exists for is a load balancer that is not sending the
    /// header at all, and that fails *every* connection — so a line per
    /// occurrence would bury the outage under its own logs. The first rejection
    /// is a warning naming the cause, the rest are `debug`.
    proxy_protocol_warned: AtomicBool,
    /// The `alt-svc` value to append to TLS responses, when HTTP/3 is running.
    ///
    /// This is the entire advertisement mechanism, and it lives here — one
    /// place, on the response path of the one listener it applies to — rather
    /// than in `forward`, which has no business knowing which listener a
    /// process happens to have opened.
    alt_svc: Option<HeaderValue>,
}

/// One serving runtime: accepts handed-off connections until the lane closes,
/// then drains. Returns whether the drain finished inside `grace`.
///
/// `index` is this lane's number, which doubles as its per-route counter shard.
fn serve_lane(
    runtime: tokio::runtime::Runtime,
    index: usize,
    config: LaneConfig,
    mut jobs: mpsc::UnboundedReceiver<Job>,
) -> bool {
    let LaneConfig {
        routes,
        metrics,
        upstream,
        acceptor,
        grace,
        max_buf_size,
        mirror_max_body,
        proxy_protocol,
        alt_svc,
    } = config;
    runtime.block_on(async move {
        // Built inside the runtime, and one per lane: this is the pool the
        // module docs are about.
        let upstream = Upstream::new(&upstream);
        // One mirror worker per lane, started inside this runtime so the copies
        // are sent on the same thread the traffic is served on rather than
        // queueing behind everything else on a shared pool.
        let mirror = Mirror::spawn(upstream.clone(), Arc::clone(&metrics))
            .with_max_body(mirror_max_body);
        let lane = Arc::new(Lane {
            builder: connection_builder(max_buf_size),
            state: Arc::new(ProxyState {
                routes,
                upstream,
                metrics,
                shard: index,
                mirror: Some(mirror),
            }),
            acceptor,
            proxy_protocol,
            proxy_protocol_warned: AtomicBool::new(false),
            alt_svc,
        });
        let graceful = GracefulShutdown::new();

        while let Some(job) = jobs.recv().await {
            let Ok(stream) = TcpStream::from_std(job.stream) else {
                continue;
            };
            spawn_connection(
                Arc::clone(&lane),
                graceful.watcher(),
                stream,
                job.guard,
                job.conn,
            );
        }

        tokio::time::timeout(grace, graceful.shutdown()).await.is_ok()
    })
}

/// Spawns the task that carries one client connection to its close.
///
/// Everything that can block on the peer happens in here rather than in the
/// accept loop, and the PROXY header is the newest reason why: waiting up to
/// `proxy_protocol_timeout` for a stalled sender on the accept path would let
/// one connection hold up every other accept on the process.
fn spawn_connection(
    lane: Arc<Lane>,
    watcher: Watcher,
    stream: TcpStream,
    guard: ConnectionGuard,
    conn: ConnInfo,
) {
    tokio::spawn(async move {
        // The guard lives in the task, not the accept loop, so a connection
        // that ends by panic or abort still decrements the gauge.
        let _guard = guard;

        let Some(timeout) = lane.proxy_protocol else {
            serve_client(&lane, watcher, stream, conn).await;
            return;
        };

        // Before the TLS handshake, not after: the load balancer speaks the
        // PROXY protocol itself and then relays the client's bytes untouched,
        // so on an HTTPS listener the header arrives ahead of the ClientHello.
        match proxy_protocol::accept(stream, timeout).await {
            Ok((stream, client)) => {
                let conn = ConnInfo {
                    // A header that names nobody — LOCAL, UNKNOWN, UNSPEC — is
                    // valid and leaves the socket's own peer standing.
                    remote: client.unwrap_or(conn.remote),
                    scheme: conn.scheme,
                };
                serve_client(&lane, watcher, stream, conn).await;
            }
            Err(error) => {
                // Loud once, quiet after. Silence here would be the worst
                // outcome: with the flag on and a balancer that does not send
                // the header, every connection dies and the symptom looks like
                // a network fault rather than the configuration mistake it is.
                if lane.proxy_protocol_warned.swap(true, AtomicOrdering::Relaxed) {
                    tracing::debug!(
                        %error,
                        peer = %conn.remote,
                        "dropped a connection with no valid PROXY protocol header"
                    );
                } else {
                    tracing::warn!(
                        %error,
                        peer = %conn.remote,
                        "dropped a connection with no valid PROXY protocol header; \
                         this listener requires one, so check that the load balancer \
                         in front of it is configured to send it (further occurrences \
                         are logged at debug)"
                    );
                }
            }
        }
    });
}

/// Serves one connection, terminating TLS first if it arrived on that listener.
///
/// Generic over the stream so the ordinary path keeps a bare `TcpStream`: with
/// `--proxy-protocol` off there is no wrapper and no extra branch per read.
async fn serve_client<S>(lane: &Lane, watcher: Watcher, stream: S, conn: ConnInfo)
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    match (conn.scheme, &lane.acceptor) {
        (Scheme::Https, Some(acceptor)) => {
            // The handshake happens here rather than in the accept loop; it is
            // tens of microseconds of arithmetic and one slow client should not
            // delay everyone else's accept.
            let stream = match acceptor.clone().accept(stream).await {
                Ok(stream) => {
                    lane.state.metrics.record_tls_handshake();
                    stream
                }
                Err(_) => {
                    // A failed handshake is routine at the edge — port
                    // scanners, clients with no matching cipher, an SNI we hold
                    // no certificate for. It is counted, not logged per
                    // occurrence.
                    lane.state.metrics.record_tls_handshake_failure();
                    return;
                }
            };
            serve_http(lane, watcher, stream, conn).await;
        }
        // A TLS connection with no acceptor cannot happen — `bind` builds one
        // whenever the listener exists — but serving it as plaintext would be
        // worse than closing it.
        (Scheme::Https, None) => drop(stream),
        (Scheme::Http, _) => serve_http(lane, watcher, stream, conn).await,
    }
}

/// Runs HTTP over an established stream until the connection ends.
async fn serve_http<S>(lane: &Lane, watcher: Watcher, stream: S, conn: ConnInfo)
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let state = Arc::clone(&lane.state);
    // Resolved once per connection rather than per request, and only for TLS:
    // `alt-svc` points a client at the QUIC port for the *same authority*, and
    // an advertisement sent over plaintext would be telling a client to move
    // from `http://` to a port serving HTTPS.
    let alt_svc = match conn.scheme {
        Scheme::Https => lane.alt_svc.clone(),
        Scheme::Http => None,
    };
    let service = service_fn(move |request| {
        let state = Arc::clone(&state);
        let alt_svc = alt_svc.clone();
        // `map` is the whole conversion: `ProxyBody::Stream` delegates every
        // poll straight to `Incoming`, so naming the crate's own body type on
        // the way in costs this path nothing and is what lets a request that
        // never came from hyper — an HTTP/3 one — reach the same function.
        let request = request.map(ProxyBody::stream);
        // Boxed, and it is worth the allocation. hyper's HTTP/1 dispatcher
        // keeps the in-flight response future in a `Pin<Box<Option<S::Future>>>`
        // that it allocates when the connection is created and holds for the
        // connection's whole life. Unboxed, that is `forward::handle`'s whole
        // state machine — 2.9 KiB — charged to every idle keep-alive connection
        // that is not serving anything. Boxing moves it to one allocation per
        // request, paid by traffic instead of by silence.
        Box::pin(async move {
            let mut response = forward::handle(state, conn, request).await;
            if let Some(value) = alt_svc {
                response.headers_mut().insert(header::ALT_SVC, value);
            }
            Ok::<_, Infallible>(response)
        })
    });
    let connection = lane
        .builder
        .serve_connection_with_upgrades(TokioIo::new(stream), service)
        .into_owned();
    let _ = watcher.watch(connection).await;
}

/// Binds and runs in one call, for a caller that has no reason to look at the
/// bound ports.
///
/// This is the entry point the Kubernetes controller phase calls: it owns the
/// [`SharedRouteTable`] and the [`CertStore`], publishes into them as watches
/// fire, and flips `readiness` once the first table has landed.
pub async fn serve(
    config: ProxyConfig,
    routes: Arc<SharedRouteTable>,
    certs: Arc<CertStore>,
    readiness: ReadinessFlag,
    shutdown: Shutdown,
) -> io::Result<()> {
    Server::bind_with(config, routes, certs, readiness)?
        .run(shutdown)
        .await
}

/// Accepts from `listener`, or waits forever if there is not one.
///
/// `pending` rather than an `Option` dance in the `select!`: a disabled
/// listener is an arm that is never ready, which is exactly what it should be.
async fn accept(listener: Option<&Listener>) -> io::Result<(TcpStream, SocketAddr)> {
    match listener {
        Some(listener) => listener.accept().await,
        None => std::future::pending().await,
    }
}

fn serve_admin(
    builder: Arc<auto::Builder<TokioExecutor>>,
    watcher: Watcher,
    state: Arc<AdminState>,
    stream: TcpStream,
) {
    tokio::spawn(async move {
        let service = service_fn(move |request| {
            let state = Arc::clone(&state);
            async move { Ok::<_, Infallible>(admin::handle(state, request).await) }
        });
        // Admin connections are not counted in `active_connections`: that gauge
        // is meant to describe client load, and a scraper polling every fifteen
        // seconds would otherwise show up as traffic.
        let connection = builder
            .serve_connection(TokioIo::new(stream), service)
            .into_owned();
        let _ = watcher.watch(connection).await;
    });
}

/// The receiving half of a shutdown signal.
///
/// Cloneable, so several servers can share one signal.
#[derive(Debug, Clone)]
pub struct Shutdown {
    rx: watch::Receiver<bool>,
}

/// The sending half of a shutdown signal.
///
/// Dropping it does **not** trigger a shutdown; shutting down is always an
/// explicit call. A dropped handle simply means the signal can never fire,
/// which is what [`Shutdown::never`] is.
#[derive(Debug, Clone)]
pub struct ShutdownHandle {
    tx: Arc<watch::Sender<bool>>,
}

impl ShutdownHandle {
    /// Asks every holder of the paired [`Shutdown`] to stop.
    pub fn shutdown(&self) {
        let _ = self.tx.send(true);
    }
}

impl Shutdown {
    /// A signal and the handle that fires it.
    pub fn channel() -> (ShutdownHandle, Shutdown) {
        let (tx, rx) = watch::channel(false);
        (
            ShutdownHandle { tx: Arc::new(tx) },
            Shutdown { rx },
        )
    }

    /// A signal that never fires, for a server whose lifetime is managed some
    /// other way.
    pub fn never() -> Shutdown {
        let (_, shutdown) = Self::channel();
        shutdown
    }

    /// A signal that fires on `SIGTERM` or `SIGINT`.
    ///
    /// `SIGTERM` is what Kubernetes sends to start a pod termination, and
    /// `SIGINT` is what a developer sends with Ctrl-C. Must be called from
    /// within a tokio runtime; it spawns the listener.
    pub fn on_signal() -> Shutdown {
        let (handle, shutdown) = Self::channel();
        tokio::spawn(async move {
            wait_for_signal().await;
            handle.shutdown();
        });
        shutdown
    }

    /// Resolves once shutdown has been requested.
    pub async fn recv(&mut self) {
        loop {
            if *self.rx.borrow_and_update() {
                return;
            }
            if self.rx.changed().await.is_err() {
                // Every handle is gone, so nothing can ever signal. Waiting
                // forever makes this arm of a `select!` simply stop being
                // selectable, which is what a caller who dropped the handle
                // meant.
                std::future::pending::<()>().await;
            }
        }
    }
}

async fn wait_for_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        match signal(SignalKind::terminate()) {
            Ok(mut term) => {
                tokio::select! {
                    _ = term.recv() => {}
                    _ = tokio::signal::ctrl_c() => {}
                }
            }
            // Registering a handler can fail in an unusual environment; falling
            // back to Ctrl-C is better than never shutting down.
            Err(_) => {
                let _ = tokio::signal::ctrl_c().await;
            }
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_use_the_ingress_nginx_ports() {
        let config = ProxyConfig::default();
        assert_eq!(config.http.map(|l| l.addr.port()), Some(8080));
        assert_eq!(config.https.map(|l| l.addr.port()), Some(8443));
        assert_eq!(config.admin.map(|l| l.addr.port()), Some(10254));
    }

    #[test]
    fn the_buffer_ceiling_defaults_below_hypers_and_above_nginxs() {
        // hyper's own default is 408 KiB and nginx's effective limit is 32 KiB.
        // Sitting between them is the whole point: nothing nginx would serve is
        // refused, and the worst case a single connection can pin is bounded.
        assert_eq!(ProxyConfig::default().max_buf_size, DEFAULT_MAX_BUF_SIZE);
        const { assert!(DEFAULT_MAX_BUF_SIZE > 32 * 1024) };
        const { assert!(DEFAULT_MAX_BUF_SIZE < 408 * 1024) };
    }

    #[test]
    fn a_ceiling_under_hypers_minimum_does_not_abort_the_process() {
        // `http1().max_buf_size` panics below `MIN_MAX_BUF_SIZE`, and this
        // builder is constructed on a serving thread — a panic there is a
        // worker that never accepts anything, not an error anybody sees.
        let _ = connection_builder(0);
        let _ = connection_builder(MIN_MAX_BUF_SIZE - 1);
    }

    #[test]
    fn the_grace_period_fits_inside_the_kubernetes_default() {
        // A drain longer than `terminationGracePeriodSeconds` would be cut
        // short by SIGKILL, making the configured deadline a fiction.
        assert!(ProxyConfig::default().shutdown_grace <= Duration::from_secs(30));
    }

    #[tokio::test]
    async fn a_signal_resolves_recv() {
        let (handle, mut shutdown) = Shutdown::channel();
        handle.shutdown();
        // Already signalled before the first poll: `recv` must see the current
        // value, not wait for the next change.
        shutdown.recv().await;
    }

    #[tokio::test]
    async fn recv_waits_for_a_later_signal() {
        let (handle, mut shutdown) = Shutdown::channel();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(5)).await;
            handle.shutdown();
        });
        tokio::time::timeout(Duration::from_secs(5), shutdown.recv())
            .await
            .expect("the signal should arrive");
    }

    #[tokio::test]
    async fn never_does_not_fire_when_the_handle_is_dropped() {
        let mut shutdown = Shutdown::never();
        let result = tokio::time::timeout(Duration::from_millis(20), shutdown.recv()).await;
        assert!(result.is_err(), "a dropped handle must not mean shutdown");
    }
}
