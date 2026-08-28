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
    /// Whether listeners expect a PROXY protocol header before anything else.
    pub proxy_protocol: bool,
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
    /// Listen backlog.
    pub backlog: i32,
    /// How often the helper thread ticks, which sets timeout resolution.
    pub tick: Duration,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            http: Some(SocketAddr::from(([0, 0, 0, 0], 8080))),
            https: None,
            tls: None,
            proxy_protocol: false,
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
            backlog: 1024,
            tick: Duration::from_millis(100),
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
#[derive(Debug, Clone)]
pub struct Shutdown {
    flag: Arc<AtomicBool>,
}

impl Shutdown {
    /// Ask every core to stop after finishing what it is doing.
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

        Ok(Engine {
            metrics: Arc::new(EngineMetrics::new(cores)),
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

    /// Serve until [`Shutdown::stop`] is called. Blocks the calling thread.
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

        let mut first_error = None;
        for thread in threads {
            match thread.join() {
                Ok(Ok(())) => {}
                Ok(Err(e)) => {
                    first_error.get_or_insert(e);
                }
                Err(_) => {
                    first_error.get_or_insert(io::Error::other("a serving core panicked"));
                }
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
        let listener = ramjet::net::Listener::builder(if i == 0 { addr } else { bound })
            .reuseaddr(true)
            .reuseport(per_core)
            .backlog(backlog)
            .build()?;
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
