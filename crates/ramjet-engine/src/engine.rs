//! Binding the listeners and starting one reactor per core.
//!
//! # Where connections come from, per platform
//!
//! Thread-per-core accept needs the kernel to spread incoming connections
//! across the cores' listening sockets. `SO_REUSEPORT` does that on Linux. It
//! does **not** on macOS, where the option means only "several sockets may bind
//! this address" and the *last* one to bind receives *all* of the traffic — the
//! runtime's own `net` module measures this and pins it in a test.
//!
//! So there are two intakes, and which one is used is a property of the
//! platform rather than a setting:
//!
//! - **Linux**: one `SO_REUSEPORT` listener per core, and the kernel does the
//!   distribution. Nothing crosses a thread boundary.
//! - **macOS and BSD**: one listener, one acceptor thread, and accepted
//!   descriptors dealt out round-robin over a socket pair per core. That costs
//!   a hand-off per connection, which is exactly why it is not used where the
//!   kernel will do the job.
//!
//! Only the Linux path is the one being measured. The macOS path exists so the
//! engine can be developed, tested and profiled on a laptop.

pub(crate) mod pool;
pub(crate) mod worker;

use std::io;
use std::net::SocketAddr;
use std::os::fd::{AsRawFd, IntoRawFd, OwnedFd, RawFd};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::Duration;

use ramjet_router::SharedRouteTable;

use crate::helper::Helper;
use crate::metrics::EngineMetrics;
use crate::sys;

/// Where one core's connections arrive from.
///
/// Two variants because two platforms answer "who accepts" differently; see the
/// module documentation. A core has one of these per listener — a plaintext one
/// and, where TLS is configured, a second — and `tls` is what tells the worker
/// which kind of connection just arrived, because by the time it has a
/// descriptor the socket itself no longer says.
pub(crate) struct Intake {
    pub(crate) source: Source,
    pub(crate) tls: bool,
}

pub(crate) enum Source {
    /// This core's own `SO_REUSEPORT` listener, fed by the kernel.
    Listener(RawFd),
    /// A socket pair the acceptor thread deals descriptor numbers over.
    Channel(RawFd),
}

impl Intake {
    pub(crate) fn fd(&self) -> RawFd {
        match self.source {
            Source::Listener(fd) | Source::Channel(fd) => fd,
        }
    }
}

/// How the engine is set up.
#[derive(Debug, Clone)]
pub struct Config {
    /// Plaintext listener address, or `None` to serve TLS alone.
    pub http: Option<SocketAddr>,
    /// TLS listener address, or `None` to disable it.
    ///
    /// Only meaningful with [`Config::tls`] set; a TLS address with no
    /// configuration would bind a socket that fails every handshake.
    pub https: Option<SocketAddr>,
    /// The rustls configuration the TLS listener terminates with.
    ///
    /// Built by `ramjet_proxy::tls::h1_server_config` from the same
    /// [`SniResolver`](ramjet_proxy::SniResolver) the hyper engine uses, so a
    /// certificate published for one engine is published for both.
    pub tls: Option<Arc<rustls::ServerConfig>>,
    /// How long a connection has to produce a PROXY protocol header, or `None`
    /// where listeners expect no header at all.
    ///
    /// The deadline is the whole point of it being a duration rather than a
    /// flag: without one, a sender that opens a connection and dribbles a byte
    /// a minute holds a descriptor and a slot in the connection gauge
    /// indefinitely, which is a cheap way to exhaust a data plane.
    pub proxy_protocol: Option<Duration>,
    /// Admin listener address, or `None` to disable it.
    ///
    /// Served by core 0's reactor rather than a separate runtime, so
    /// `/metrics` costs the data plane one accept and nothing else.
    pub admin: Option<SocketAddr>,
    /// Serving cores. `None` means one per available core.
    pub workers: Option<usize>,
    /// Bound on establishing an upstream connection.
    pub connect_timeout: Duration,
    /// Bound on receiving upstream response headers.
    pub response_timeout: Duration,
    /// Endpoints tried before giving up on a retryable failure.
    pub max_connect_attempts: usize,
    /// Idle upstream connections kept per endpoint, **per core**.
    pub pool_max_idle_per_host: usize,
    /// How long an idle upstream connection is kept.
    pub pool_idle_timeout: Duration,
    /// Ceiling on one connection's read buffer.
    ///
    /// The same `--max-buf-size` the hyper engine takes, and it means the same
    /// thing on both: the most one read can deliver, and so the memory a
    /// connection can be holding on the read side. It is a *ceiling* on that
    /// engine because hyper grows its buffer from a smaller start; here it is
    /// the buffer, because these are allocated once, pooled, and never
    /// truncated — restoring a shrunk one would cost a `memset` per read.
    ///
    /// Clamped up to [`MIN_BUF_SIZE`], which is the smallest a request head is
    /// reliably read in.
    pub max_buf_size: usize,
    /// Largest request body copied to a mirror backend.
    pub mirror_max_body: usize,
    /// Where a connection this engine cannot serve is sent, or `None` to serve
    /// every connection here.
    ///
    /// # One port, two engines
    ///
    /// With this set, the TLS listener offers `h2` as well as `http/1.1` in its
    /// ALPN, and a client that takes `h2` is handed to the hyper engine instead
    /// of being refused. That is possible because a `rustls::server::Acceptor`
    /// yields the ClientHello *before* a configuration is chosen: the ALPN list
    /// is readable at a point where nothing has been committed to the
    /// connection, and the bytes consumed so far can be replayed on the other
    /// side. From the client there is no reset, no second handshake and no
    /// retry — it sees one connection that negotiated HTTP/2.
    ///
    /// The plaintext listener does the same for the HTTP/2 prior-knowledge
    /// preface, which is the only way h2 arrives without TLS.
    pub dispatch: Option<ramjet_proxy::HandoffSender>,
    /// Where sampled copies go, or `None` where mirroring is not wired up.
    ///
    /// The worker draining this queue is a tokio task, so it has to be started
    /// by the embedder from inside a runtime; the reactor threads only ever
    /// `try_send` into it, which needs no runtime at all. A route with a mirror
    /// annotation and no lane here simply makes no copies, which is the correct
    /// behaviour for a data plane with nowhere to put them.
    pub mirror: Option<crate::mirror::MirrorLane>,
    /// Counters for everything this process does on the tokio side, summed into
    /// this engine's own at scrape time.
    ///
    /// Two things write to it, and both of them serve or record traffic this
    /// engine's per-core blocks never see: the mirror worker, and — with
    /// [`Config::dispatch`] on — the hyper lane that every HTTP/2 connection is
    /// handed to. One `/metrics` has to describe the whole process, so leaving
    /// this unset in dispatch mode would report the HTTP/1.1 half and silently
    /// omit the rest.
    ///
    /// The same `Arc` the mirror lane and the hyper lane were built with;
    /// passing a different one would produce a second set of numbers nobody
    /// reads.
    pub peer_metrics: Option<Arc<ramjet_proxy::Metrics>>,
    /// Listen backlog.
    pub backlog: i32,
    /// How often the helper thread ticks, which sets timeout resolution.
    pub tick: Duration,
    /// How long in-flight requests get to finish after [`Shutdown::stop`].
    ///
    /// The same `--shutdown-grace` the hyper engine takes, meaning the same
    /// thing: the listeners close at once, and what is already being served has
    /// until this deadline to end. Past it the remaining connections are closed
    /// and [`Engine::run`] reports `TimedOut`.
    ///
    /// The drain itself is [`worker`]'s state machine; that module's
    /// documentation is where the per-connection rules live.
    pub shutdown_grace: Duration,
}

/// Environment variable that makes [`probe`] report the reactor as
/// unavailable.
///
/// A test hook, and named as one. The failure it stands in for — `io_uring`
/// blocked by seccomp — needs a container with a specific policy to reproduce,
/// which is not something a `cargo test` can arrange, and a fallback path that
/// is only exercised in production is a fallback path nobody has seen work. It
/// is also the only way an operator can check that *their* deployment falls
/// back the way they expect before it has to.
pub const UNAVAILABLE_ENV: &str = "RAMJET_URING_UNAVAILABLE";

/// Whether this host will let the engine start.
///
/// Creating a reactor is the whole test: on Linux that is an `io_uring_setup`
/// syscall, which is what Docker's default seccomp profile blocks and what an
/// old kernel refuses. It costs one ring's worth of setup and teardown, and
/// running it before any listener binds is what makes falling back to the other
/// engine possible — after a bind, the ports are taken and the fallback would
/// have to unbind them first.
///
/// The error is returned rather than logged so the caller can decide whether it
/// is fatal, and can put the real reason in front of an operator either way.
/// "io_uring is unavailable" without the `errno` is a support ticket.
pub fn probe() -> io::Result<()> {
    if std::env::var_os(UNAVAILABLE_ENV).is_some_and(|value| value == "1") {
        return Err(io::Error::new(
            io::ErrorKind::Unsupported,
            format!("{UNAVAILABLE_ENV}=1 is set"),
        ));
    }
    ramjet::reactor::PlatformDriver::new().map(drop)
}

/// The smallest read buffer this engine will use, whatever it is asked for.
///
/// A buffer below this can still serve any request — the head parser resumes
/// across reads — but a head that needs several reads to arrive costs several
/// completions, and 8 KiB is where that stops happening for ordinary traffic.
/// The hyper engine clamps to the same number for the same reason.
pub const MIN_BUF_SIZE: usize = 8 * 1024;

impl Default for Config {
    fn default() -> Self {
        Config {
            http: Some(SocketAddr::from(([0, 0, 0, 0], 8080))),
            https: None,
            tls: None,
            proxy_protocol: None,
            admin: Some(SocketAddr::from(([0, 0, 0, 0], 10254))),
            workers: None,
            // The same defaults as the hyper engine's `UpstreamConfig`, because
            // an operator changing `--engine` should not also be changing the
            // timeouts they run with.
            connect_timeout: Duration::from_secs(5),
            response_timeout: Duration::from_secs(60),
            max_connect_attempts: 3,
            pool_max_idle_per_host: 128,
            pool_idle_timeout: Duration::from_secs(90),
            max_buf_size: 16 * 1024,
            mirror_max_body: ramjet_proxy::DEFAULT_MIRROR_MAX_BODY,
            dispatch: None,
            mirror: None,
            peer_metrics: None,
            backlog: 1024,
            tick: Duration::from_millis(100),
            // Kubernetes' default `terminationGracePeriodSeconds` is 30, so a
            // longer drain would be interrupted by SIGKILL — and the same
            // number as the hyper engine's, because an operator changing
            // `--engine` should not also be changing how long a rolling update
            // waits.
            shutdown_grace: Duration::from_secs(30),
        }
    }
}

impl Config {
    fn cores(&self) -> usize {
        self.workers.unwrap_or_else(|| {
            thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(1)
        })
        .max(1)
    }
}

/// A running engine's stop button.
///
/// Cloneable and safe to call from anywhere, including a signal handler's
/// thread: it sets a flag that every core reads on its next tick.
///
/// Stopping starts a *drain* rather than a close. Every core shuts its
/// listeners immediately, closes what it is not serving — idle keep-alive
/// connections, pooled upstreams, tunnels — and keeps running until the
/// requests it is already carrying have finished or
/// [`Config::shutdown_grace`] has passed. The rules are
/// [`worker`]'s, and its module documentation is where they are written down.
#[derive(Debug, Clone)]
pub struct Shutdown {
    flag: Arc<AtomicBool>,
}

impl Shutdown {
    /// Ask every core to stop accepting and drain what it is already serving.
    pub fn stop(&self) {
        self.flag.store(true, Ordering::Relaxed);
    }

    /// Whether a stop has been requested.
    pub fn stopping(&self) -> bool {
        self.flag.load(Ordering::Relaxed)
    }
}

/// A bound, not yet running, engine.
///
/// Binding and running are separate so a caller — a test, most of all — can
/// learn the port the kernel chose before any traffic is served.
pub struct Engine {
    config: Arc<Config>,
    routes: Arc<SharedRouteTable>,
    readiness: Arc<AtomicBool>,
    metrics: Arc<EngineMetrics>,
    shutdown: Arc<AtomicBool>,
    cores: usize,
    http_addr: Option<SocketAddr>,
    https_addr: Option<SocketAddr>,
    admin_addr: Option<SocketAddr>,
    listeners: Vec<BoundListener>,
    admin_listener: Option<OwnedFd>,
}

/// One listener's sockets, and what arrives on them.
///
/// `sockets` holds one per core on a platform whose kernel distributes
/// connections, and exactly one otherwise; see the module documentation.
struct BoundListener {
    sockets: Vec<OwnedFd>,
    tls: bool,
}

impl Engine {
    /// Bind the listeners.
    pub fn bind(
        config: Config,
        routes: Arc<SharedRouteTable>,
        readiness: Arc<AtomicBool>,
    ) -> io::Result<Engine> {
        // A write to a peer that has gone must be an error, not a signal that
        // ends the process. The reactor covers its own sockets on Linux and the
        // ones it accepted on macOS; this covers everything else.
        sys::ignore_sigpipe();

        if config.http.is_none() && config.https.is_none() {
            return Err(io::Error::other(
                "the engine was given neither a plaintext nor a TLS listener",
            ));
        }
        if config.https.is_some() && config.tls.is_none() {
            // Refused rather than bound: a TLS socket with no configuration
            // accepts connections and fails every handshake, which from outside
            // looks exactly like a broken certificate.
            return Err(io::Error::other(
                "a TLS listener was configured with no rustls configuration to terminate it",
            ));
        }

        let cores = config.cores();
        let mut listeners = Vec::with_capacity(2);
        let mut http_addr = None;
        let mut https_addr = None;
        for (addr, tls) in [(config.http, false), (config.https, true)] {
            let Some(addr) = addr else { continue };
            let (sockets, bound) = bind_intake(addr, cores, config.backlog)?;
            listeners.push(BoundListener { sockets, tls });
            if tls {
                https_addr = Some(bound);
            } else {
                http_addr = Some(bound);
            }
        }

        let (admin_listener, admin_addr) = match config.admin {
            Some(addr) => {
                let listener = ramjet::net::Listener::builder(addr)
                    .reuseaddr(true)
                    .backlog(64)
                    .build()?;
                let bound = listener.local_addr();
                // SAFETY of the descriptor's lifetime: `OwnedFd` keeps it until
                // it is handed to a core.
                let fd = unsafe { owned(listener.into_raw_fd()) };
                (Some(fd), Some(bound))
            }
            None => (None, None),
        };

        // The tokio side's counters are reported alongside this engine's own, so
        // one scrape describes the whole process however the traffic is split.
        let metrics = match config.peer_metrics.as_ref() {
            Some(peer) => EngineMetrics::with_peer(cores, Arc::clone(peer)),
            None => EngineMetrics::new(cores),
        };

        Ok(Engine {
            metrics: Arc::new(metrics),
            config: Arc::new(config),
            routes,
            readiness,
            shutdown: Arc::new(AtomicBool::new(false)),
            cores,
            http_addr,
            https_addr,
            admin_addr,
            listeners,
            admin_listener,
        })
    }

    /// The plaintext address actually bound, which is how a caller learns the
    /// port when it asked for zero.
    pub fn http_addr(&self) -> Option<SocketAddr> {
        self.http_addr
    }

    /// The TLS address actually bound, if there is one.
    pub fn https_addr(&self) -> Option<SocketAddr> {
        self.https_addr
    }

    /// The admin address actually bound, if there is one.
    pub fn admin_addr(&self) -> Option<SocketAddr> {
        self.admin_addr
    }

    /// How many serving cores this engine will start.
    pub fn cores(&self) -> usize {
        self.cores
    }

    /// A handle that stops the engine.
    pub fn shutdown(&self) -> Shutdown {
        Shutdown {
            flag: Arc::clone(&self.shutdown),
        }
    }

    /// The counters every core writes into.
    pub fn metrics(&self) -> Arc<EngineMetrics> {
        Arc::clone(&self.metrics)
    }

    /// Serve until [`Shutdown::stop`] is called, then drain. Blocks the
    /// calling thread.
    ///
    /// Returns `TimedOut` where a core still had connections open when
    /// [`Config::shutdown_grace`] expired. That is an outcome rather than a
    /// crash — the caller decides whether it is worth complaining about — but
    /// it is reported rather than swallowed, which is the same contract
    /// `ramjet_proxy::Server::run` has and the reason `ramjet-ingressd` can
    /// treat the two engines' shutdowns identically.
    pub fn run(self) -> io::Result<()> {
        let Engine {
            config,
            routes,
            readiness,
            metrics,
            shutdown,
            cores,
            listeners,
            admin_listener,
            ..
        } = self;

        let (helper, notifies) = Helper::start(cores, config.tick)?;
        let helper = Arc::new(helper);

        // On a platform where the kernel does not distribute connections, one
        // thread accepts for every listener and deals descriptors out.
        let mut acceptor: Option<AcceptorThread> = None;
        let mut per_core: Vec<Vec<Intake>> = (0..cores).map(|_| Vec::with_capacity(2)).collect();
        let mut accepting: Vec<(OwnedFd, Vec<OwnedFd>)> = Vec::new();

        for listener in listeners {
            let BoundListener { sockets, tls } = listener;
            if sockets.len() == cores {
                for (core, socket) in sockets.into_iter().enumerate() {
                    per_core[core].push(Intake {
                        source: Source::Listener(socket.into_raw_fd()),
                        tls,
                    });
                }
                continue;
            }
            // One listener, and a socket pair per core to deal its descriptors
            // over. Each listener gets its own set, so a core can still tell a
            // TLS connection from a plaintext one after the hand-off.
            let mut theirs = Vec::with_capacity(cores);
            let mut mine = Vec::with_capacity(cores);
            for _ in 0..cores {
                let (a, b) = sys::socket_pair()?;
                sys::set_nonblocking(b.as_raw_fd())?;
                mine.push(a);
                theirs.push(b);
            }
            for (core, channel) in theirs.into_iter().enumerate() {
                per_core[core].push(Intake {
                    source: Source::Channel(channel.into_raw_fd()),
                    tls,
                });
            }
            let socket = sockets
                .into_iter()
                .next()
                .ok_or_else(|| io::Error::other("no listener was bound"))?;
            accepting.push((socket, mine));
        }
        if !accepting.is_empty() {
            acceptor = Some(AcceptorThread::start(accepting, Arc::clone(&shutdown))?);
        }

        let mut admin_for = admin_listener.map(IntoRawFd::into_raw_fd);
        let mut threads: Vec<JoinHandle<io::Result<()>>> = Vec::with_capacity(cores);
        for (core, (intake, notify)) in per_core.into_iter().zip(notifies).enumerate() {
            let config = Arc::clone(&config);
            let routes = Arc::clone(&routes);
            let metrics = Arc::clone(&metrics);
            let readiness = Arc::clone(&readiness);
            let shutdown = Arc::clone(&shutdown);
            let helper = Arc::clone(&helper);
            // The admin listener lives on core 0. It sees a scrape every few
            // seconds; giving it a core of its own would take one away from the
            // data plane for nothing.
            let admin = if core == 0 { admin_for.take() } else { None };
            let notify_fd = notify.into_raw_fd();
            threads.push(
                thread::Builder::new()
                    .name(format!("ramjet-uring-{core}"))
                    .spawn(move || {
                        let mut worker = worker::Worker::new(
                            core, config, routes, metrics, readiness, shutdown, helper, intake,
                            admin, notify_fd,
                        )?;
                        worker.run()
                    })?,
            );
        }

        let mut first_error: Option<io::Error> = None;
        for thread in threads {
            let outcome = match thread.join() {
                Ok(outcome) => outcome,
                Err(_) => Err(io::Error::other("a serving core panicked")),
            };
            let Err(error) = outcome else { continue };
            // A drain that ran out of time is an outcome the caller turns into
            // a clean exit; a core that failed is not. Where one core did each,
            // the failure is the one worth reporting — held the other way
            // round, a real fault would leave the process exiting zero because
            // some other core happened to be slow.
            let held_is_timeout = first_error
                .as_ref()
                .is_some_and(|held| held.kind() == io::ErrorKind::TimedOut);
            if first_error.is_none() || (held_is_timeout && error.kind() != io::ErrorKind::TimedOut)
            {
                first_error = Some(error);
            }
        }
        if let Some(acceptor) = acceptor {
            acceptor.stop();
        }
        match first_error {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }
}

/// Bind however many listening sockets this platform's accept model needs.
///
/// Returns the sockets and the address actually bound. When the caller asked
/// for port 0, the first socket decides the port and the rest join it —
/// otherwise every core would land on a different one.
fn bind_intake(addr: SocketAddr, cores: usize, backlog: i32) -> io::Result<(Vec<OwnedFd>, SocketAddr)> {
    let per_core = cfg!(target_os = "linux") || cfg!(target_os = "freebsd");
    let wanted = if per_core { cores } else { 1 };

    let mut sockets = Vec::with_capacity(wanted);
    let mut bound = addr;
    for i in 0..wanted {
        let wanted_addr = if i == 0 { addr } else { bound };
        let listener = ramjet::net::Listener::builder(wanted_addr)
            .reuseaddr(true)
            .reuseport(per_core)
            .backlog(backlog)
            .build()
            // Same explanation the hyper engine gives, from the same function:
            // which engine happened to own the listener is not something the
            // operator chose, and the remedy does not depend on it.
            .map_err(|error| ramjet_proxy::explain_bind_failure(wanted_addr, error))?;
        if i == 0 {
            bound = listener.local_addr();
        }
        // SAFETY: the listener just handed over a descriptor it owned.
        sockets.push(unsafe { owned(listener.into_raw_fd()) });
    }
    Ok((sockets, bound))
}

/// Wrap a raw descriptor we have just taken sole ownership of.
///
/// # Safety
///
/// `fd` must be a live descriptor that nothing else will close.
unsafe fn owned(fd: RawFd) -> OwnedFd {
    // SAFETY: the caller promises sole ownership.
    unsafe { <OwnedFd as std::os::fd::FromRawFd>::from_raw_fd(fd) }
}

/// The accept thread used where `SO_REUSEPORT` does not distribute.
struct AcceptorThread {
    handle: JoinHandle<()>,
}

impl AcceptorThread {
    /// One thread for every listener that needs one.
    ///
    /// `listeners` pairs each listening socket with the per-core channels its
    /// accepted descriptors are dealt over. One thread rather than one per
    /// listener: it is two file descriptors to poll and no traffic of its own,
    /// and this path only exists on the platform where nothing is being
    /// measured anyway.
    fn start(
        listeners: Vec<(OwnedFd, Vec<OwnedFd>)>,
        shutdown: Arc<AtomicBool>,
    ) -> io::Result<AcceptorThread> {
        let handle = thread::Builder::new()
            .name("ramjet-uring-accept".to_owned())
            .spawn(move || accept_loop(listeners, shutdown))?;
        Ok(AcceptorThread { handle })
    }

    fn stop(self) {
        let _ = self.handle.join();
    }
}

fn accept_loop(listeners: Vec<(OwnedFd, Vec<OwnedFd>)>, shutdown: Arc<AtomicBool>) {
    // One cursor per listener, so a quiet TLS port does not skew which core the
    // busy plaintext port's next connection lands on.
    let mut next = vec![0usize; listeners.len()];
    while !shutdown.load(Ordering::Relaxed) {
        let mut poll_fds: Vec<libc::pollfd> = listeners
            .iter()
            .map(|(listener, _)| libc::pollfd {
                fd: listener.as_raw_fd(),
                events: libc::POLLIN,
                revents: 0,
            })
            .collect();
        // A short timeout rather than an indefinite wait, so a shutdown is
        // noticed without needing anything to connect first.
        match sys::poll(&mut poll_fds, 100) {
            Ok(0) => continue,
            Ok(_) => {}
            Err(_) => return,
        }
        // Re-read after the wait, not only before it: a stop that arrived while
        // this thread was parked has already shut the cores' ends of the
        // channels, so accepting now would be taking a connection nothing in
        // this process is going to serve.
        if shutdown.load(Ordering::Relaxed) {
            return;
        }
        for (index, (listener, channels)) in listeners.iter().enumerate() {
            if poll_fds[index].revents == 0 || channels.is_empty() {
                continue;
            }
            loop {
                match accept(listener.as_raw_fd()) {
                    Ok(Some(fd)) => {
                        let target = next[index] % channels.len();
                        next[index] = next[index].wrapping_add(1);
                        // Four bytes into a socket pair whose buffer starts in
                        // the kilobytes: this cannot actually block.
                        if sys::write(channels[target].as_raw_fd(), &fd.to_le_bytes()).is_err() {
                            // SAFETY: the core never received this descriptor,
                            // so nobody else owns it.
                            unsafe { sys::close(fd) };
                        }
                    }
                    Ok(None) => break,
                    Err(_) => break,
                }
            }
        }
    }
}

/// `accept(2)`, returning `Ok(None)` when there is nothing waiting.
fn accept(listener: RawFd) -> io::Result<Option<RawFd>> {
    // SAFETY: passing null for the address arguments is how accept is told we
    // do not want the peer address; it then writes nothing.
    let fd = unsafe { libc::accept(listener, std::ptr::null_mut(), std::ptr::null_mut()) };
    if fd >= 0 {
        return Ok(Some(fd));
    }
    let err = io::Error::last_os_error();
    match err.raw_os_error() {
        // Nothing waiting, or a pending connection that died before we took
        // it — neither says anything about the listener.
        Some(libc::EAGAIN) | Some(libc::ECONNABORTED) | Some(libc::EINTR) => Ok(None),
        _ => Err(err),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn binding_port_zero_reports_the_port_the_kernel_chose() {
        let routes = Arc::new(SharedRouteTable::new(
            ramjet_router::RouteTableBuilder::new()
                .build()
                .expect("an empty table"),
        ));
        let config = Config {
            http: Some(SocketAddr::from(([127, 0, 0, 1], 0))),
            admin: Some(SocketAddr::from(([127, 0, 0, 1], 0))),
            workers: Some(2),
            ..Config::default()
        };
        let engine =
            Engine::bind(config, routes, Arc::new(AtomicBool::new(false))).expect("a bound engine");
        assert!(
            engine.http_addr().is_some_and(|a| a.port() != 0),
            "the port must be resolved"
        );
        assert!(engine.admin_addr().is_some_and(|a| a.port() != 0));
        assert_eq!(engine.cores(), 2);
    }

    #[test]
    fn a_tls_listener_without_a_configuration_is_refused() {
        // Binding it anyway would present a working HTTPS endpoint that fails
        // every handshake, which is indistinguishable from a broken
        // certificate and much harder to diagnose.
        let routes = Arc::new(SharedRouteTable::new(
            ramjet_router::RouteTableBuilder::new()
                .build()
                .expect("an empty table"),
        ));
        let config = Config {
            http: Some(SocketAddr::from(([127, 0, 0, 1], 0))),
            https: Some(SocketAddr::from(([127, 0, 0, 1], 0))),
            tls: None,
            admin: None,
            workers: Some(1),
            ..Config::default()
        };
        let error = Engine::bind(config, routes, Arc::new(AtomicBool::new(false)))
            .err()
            .expect("a TLS listener with no configuration is refused");
        assert!(error.to_string().contains("rustls configuration"), "{error}");
    }

    #[test]
    fn an_engine_with_no_listener_at_all_is_refused() {
        let routes = Arc::new(SharedRouteTable::new(
            ramjet_router::RouteTableBuilder::new()
                .build()
                .expect("an empty table"),
        ));
        let config = Config {
            http: None,
            https: None,
            admin: None,
            workers: Some(1),
            ..Config::default()
        };
        assert!(Engine::bind(config, routes, Arc::new(AtomicBool::new(false))).is_err());
    }

    #[test]
    fn every_core_binds_the_same_port() {
        // With one listener per core and port 0, the first socket decides and
        // the rest join it; without that each core would serve a different
        // port and only one of them would get any traffic.
        let (sockets, addr) =
            bind_intake(SocketAddr::from(([127, 0, 0, 1], 0)), 4, 128).expect("bound");
        assert_ne!(addr.port(), 0);
        assert!(!sockets.is_empty());
        if cfg!(target_os = "linux") {
            assert_eq!(sockets.len(), 4, "one listener per core on Linux");
        } else {
            assert_eq!(sockets.len(), 1, "one listener plus an acceptor elsewhere");
        }
    }

    #[test]
    fn a_shutdown_handle_is_shared() {
        let flag = Arc::new(AtomicBool::new(false));
        let shutdown = Shutdown {
            flag: Arc::clone(&flag),
        };
        assert!(!shutdown.stopping());
        shutdown.clone().stop();
        assert!(shutdown.stopping());
        assert!(flag.load(Ordering::Relaxed));
    }
}
