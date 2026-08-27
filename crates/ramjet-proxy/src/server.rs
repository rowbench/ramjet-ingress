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

use std::convert::Infallible;
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use hyper::service::service_fn;
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto;
use hyper_util::server::graceful::{GracefulShutdown, Watcher};
use ramjet_router::SharedRouteTable;
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot, watch};
use tokio_rustls::TlsAcceptor;

use crate::admin::{self, AdminState, ReadinessFlag};
use crate::forward::{self, ConnInfo, ProxyState, Scheme};
use crate::listener::{Listener, ListenerConfig};
use crate::metrics::{ConnectionGuard, Metrics};
use crate::tls::{self, CertStore, SniResolver};
use crate::upstream::{Upstream, UpstreamConfig};

/// Default plaintext port.
pub const DEFAULT_HTTP_PORT: u16 = 8080;
/// Default TLS port.
pub const DEFAULT_HTTPS_PORT: u16 = 8443;
/// Default admin port, matching the ingress-nginx convention.
pub const DEFAULT_ADMIN_PORT: u16 = 10254;

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
        }
    }
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
    readiness: ReadinessFlag,
    grace: Duration,
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
        let tls = match https {
            Some(_) => {
                let resolver = Arc::new(SniResolver::new(Arc::clone(&routes), certs));
                let config = tls::server_config(resolver)
                    .map_err(|error| io::Error::other(error.to_string()))?;
                Some(Arc::new(config))
            }
            None => None,
        };

        let metrics = Arc::new(Metrics::new());
        let admin_state = Arc::new(AdminState {
            metrics: Arc::clone(&metrics),
            routes: Arc::clone(&routes),
            readiness: readiness.clone(),
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
            readiness,
            grace: config.shutdown_grace,
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

    /// The admin address actually bound.
    pub fn admin_addr(&self) -> Option<SocketAddr> {
        self.admin.as_ref().and_then(|l| l.local_addr().ok())
    }

    /// The data-plane counters, for a caller that wants to read them directly.
    pub fn metrics(&self) -> &Arc<Metrics> {
        &self.metrics
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
            ..
        } = self;

        let acceptor = tls.map(TlsAcceptor::from);
        let mut workers = Workers::start(
            worker_threads,
            &routes,
            &metrics,
            upstream,
            acceptor.clone(),
            grace,
        )?;

        // Admin lives on the caller's runtime; see the module docs.
        let graceful = GracefulShutdown::new();
        let builder = Arc::new(auto::Builder::new(TokioExecutor::new()));

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
        // a metrics scrape to finish would be the wrong order.
        let (drained, admin_drained) = tokio::join!(
            workers.drain(),
            tokio::time::timeout(grace, graceful.shutdown()),
        );

        if !drained || admin_drained.is_err() {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "shutdown grace period expired with connections still open",
            ));
        }
        Ok(())
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

/// The serving runtimes and the round-robin over them.
struct Workers {
    lanes: Vec<mpsc::UnboundedSender<Job>>,
    done: Vec<oneshot::Receiver<bool>>,
    next: usize,
}

impl Workers {
    fn start(
        count: usize,
        routes: &Arc<SharedRouteTable>,
        metrics: &Arc<Metrics>,
        upstream: UpstreamConfig,
        acceptor: Option<TlsAcceptor>,
        grace: Duration,
    ) -> io::Result<Workers> {
        let mut lanes = Vec::with_capacity(count);
        let mut done = Vec::with_capacity(count);

        for index in 0..count {
            let (jobs_tx, jobs_rx) = mpsc::unbounded_channel();
            let (done_tx, done_rx) = oneshot::channel();
            let routes = Arc::clone(routes);
            let metrics = Arc::clone(metrics);
            let acceptor = acceptor.clone();

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
                    let drained =
                        serve_lane(runtime, routes, metrics, upstream, acceptor, grace, jobs_rx);
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

/// One serving runtime: accepts handed-off connections until the lane closes,
/// then drains. Returns whether the drain finished inside `grace`.
fn serve_lane(
    runtime: tokio::runtime::Runtime,
    routes: Arc<SharedRouteTable>,
    metrics: Arc<Metrics>,
    upstream: UpstreamConfig,
    acceptor: Option<TlsAcceptor>,
    grace: Duration,
    mut jobs: mpsc::UnboundedReceiver<Job>,
) -> bool {
    runtime.block_on(async move {
        // Built inside the runtime, and one per lane: this is the pool the
        // module docs are about.
        let state = Arc::new(ProxyState {
            routes,
            upstream: Upstream::new(&upstream),
            metrics,
        });
        let graceful = GracefulShutdown::new();
        let builder = Arc::new(auto::Builder::new(TokioExecutor::new()));

        while let Some(job) = jobs.recv().await {
            let Ok(stream) = TcpStream::from_std(job.stream) else {
                continue;
            };
            match (job.conn.scheme, &acceptor) {
                (Scheme::Https, Some(acceptor)) => serve_tls(
                    Arc::clone(&builder),
                    graceful.watcher(),
                    Arc::clone(&state),
                    acceptor.clone(),
                    stream,
                    job.guard,
                    job.conn.remote,
                ),
                // A TLS connection with no acceptor cannot happen — `bind`
                // builds one whenever the listener exists — but serving it as
                // plaintext would be worse than closing it.
                (Scheme::Https, None) => drop(stream),
                (Scheme::Http, _) => serve_proxy(
                    Arc::clone(&builder),
                    graceful.watcher(),
                    Arc::clone(&state),
                    stream,
                    job.guard,
                    job.conn,
                ),
            }
        }

        tokio::time::timeout(grace, graceful.shutdown()).await.is_ok()
    })
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

fn serve_proxy(
    builder: Arc<auto::Builder<TokioExecutor>>,
    watcher: Watcher,
    state: Arc<ProxyState>,
    stream: TcpStream,
    guard: ConnectionGuard,
    conn: ConnInfo,
) {
    tokio::spawn(async move {
        // The guard lives in the task, not the accept loop, so a connection
        // that ends by panic or abort still decrements the gauge.
        let _guard = guard;
        let service = service_fn(move |request| {
            let state = Arc::clone(&state);
            async move { Ok::<_, Infallible>(forward::handle(state, conn, request).await) }
        });
        let connection = builder
            .serve_connection_with_upgrades(TokioIo::new(stream), service)
            .into_owned();
        let _ = watcher.watch(connection).await;
    });
}

fn serve_tls(
    builder: Arc<auto::Builder<TokioExecutor>>,
    watcher: Watcher,
    state: Arc<ProxyState>,
    acceptor: TlsAcceptor,
    stream: TcpStream,
    guard: ConnectionGuard,
    remote: SocketAddr,
) {
    let metrics = Arc::clone(&state.metrics);
    tokio::spawn(async move {
        let _guard = guard;
        // The handshake happens here rather than in the accept loop; it is tens
        // of microseconds of arithmetic and one slow client should not delay
        // everyone else's accept.
        let stream = match acceptor.accept(stream).await {
            Ok(stream) => {
                metrics.record_tls_handshake();
                stream
            }
            Err(_) => {
                // A failed handshake is routine at the edge — port scanners,
                // clients with no matching cipher, an SNI we hold no
                // certificate for. It is counted, not logged per occurrence.
                metrics.record_tls_handshake_failure();
                return;
            }
        };

        let conn = ConnInfo {
            remote,
            scheme: Scheme::Https,
        };
        let service = service_fn(move |request| {
            let state = Arc::clone(&state);
            async move { Ok::<_, Infallible>(forward::handle(state, conn, request).await) }
        });
        let connection = builder
            .serve_connection_with_upgrades(TokioIo::new(stream), service)
            .into_owned();
        let _ = watcher.watch(connection).await;
    });
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
