//! `ramjet-ingressd` is the daemon that runs ramjet-ingress.
//!
//! # Two writers, one data plane
//!
//! The proxy reads a [`SharedRouteTable`] and a [`CertStore`]. It has no
//! opinion about where either came from, which is what lets this binary have
//! two modes that share every line of the serving path:
//!
//! - **Kubernetes mode**, the default. [`ramjet_controller::spawn`] watches the
//!   API server and publishes a compiled configuration per generation;
//!   [`kubernetes::Publisher`] applies each one. `/readyz` stays 503 until the
//!   first real generation lands.
//! - **Dev mode**, selected with `--static-routes`. The table is read from a
//!   file once, before the listeners bind, so the data plane can be run,
//!   curled, profiled, and debugged on a laptop with no API server anywhere.
//!
//! They are mutually exclusive because two writers for one route table would
//! make the winner a race, and the flag is the only difference between them.

mod args;
mod certs;
mod config;
mod kubernetes;
mod promotion;
#[cfg(test)]
mod testing;

use std::net::SocketAddr;
use std::process::ExitCode;
use std::sync::Arc;

use ramjet_proxy::{
    CertStore, ListenerConfig, ProxyConfig, ReadinessFlag, Server, Shutdown, SniResolver,
    UpstreamConfig,
};
use ramjet_router::SharedRouteTable;
use tracing_subscriber::EnvFilter;

use crate::args::{ArgError, Args, Engine, USAGE};

/// The allocator, and why it is not glibc's.
///
/// This is a *retention* fix, not a footprint one, and the distinction is the
/// whole reason it is here. Ten thousand idle keep-alive connections opened and
/// then closed left glibc's malloc holding almost every byte it had taken:
/// `bench/thesis/RESULTS.md`'s benchmark 4 measured 266 MiB at peak falling to
/// only 230 MiB when every connection had gone, and a second cycle rising again
/// from there. Nothing there is leaked — the blocks are free — but glibc's heap
/// is contiguous and cannot hand a page back to the kernel unless everything
/// above it is free too, and after ten thousand interleaved connections nothing
/// ever is. A process meant to run for months grows across every traffic cycle,
/// and a memory limit does not care that the bytes are technically free.
///
/// jemalloc's extents are independent, so freeing a connection's blocks makes
/// whole runs purgeable, and its background thread `madvise`s them away on a
/// timer whether or not the process is still allocating. That last part matters
/// more than it sounds: the pathological case is a pod that has just *lost* its
/// traffic, and an allocator that only reclaims while it is being called would
/// sit on the memory precisely then.
///
/// Measured on benchmark 4's topology, the two-pass cycle that grew
/// monotonically under glibc returns to its idle level after every pass here.
/// The cost is about 300 KiB of binary and jemalloc's `configure`/`make` in the
/// builder stage.
#[global_allocator]
static ALLOCATOR: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

/// jemalloc's configuration, read by the allocator before `main` runs.
///
/// Compiled in rather than set through `MALLOC_CONF` in the image or the chart,
/// so that a binary run outside its container behaves the same way. The symbol
/// carries `tikv-jemalloc-sys`'s `_rjem_` prefix: exported under the unprefixed
/// name, jemalloc never reads it and every option here silently does nothing.
///
/// One second of decay rather than zero: purging is a `madvise` per run, and at
/// zero the background thread does that work continuously under load for no
/// benefit a second's delay does not also give.
///
/// `background_thread` is asked for on Linux only. jemalloc supports it on
/// pthread platforms only and says so on stderr at every startup otherwise,
/// which on a developer's macOS laptop is a warning about a setting that was
/// never going to apply there. The decay settings still do.
#[allow(non_upper_case_globals)]
#[export_name = "_rjem_malloc_conf"]
pub static MALLOC_CONF: &[u8] = if cfg!(target_os = "linux") {
    b"background_thread:true,dirty_decay_ms:1000,muzzy_decay_ms:1000\0"
} else {
    b"dirty_decay_ms:1000,muzzy_decay_ms:1000\0"
};

/// One worker thread, deliberately.
///
/// This runtime does not serve traffic. Requests are served by the data
/// plane's own per-core runtimes (see [`ramjet_proxy::Server`]); what is left
/// here is the accept loop, the admin listener, and — in Kubernetes mode — the
/// controller's watches. Letting tokio start one worker per core for that work
/// would put `cores` mostly-idle threads on the same cores the serving runtimes
/// are trying to saturate, and every one of them is a scheduler that can
/// preempt a request in flight.
#[tokio::main(worker_threads = 1)]
async fn main() -> ExitCode {
    match run().await {
        Ok(code) => code,
        Err(error) => {
            eprintln!("ramjet-ingressd: {error}");
            // Print the chain: "cannot parse routes.yaml: ..." on its own does
            // not tell anybody which line was wrong.
            let mut source = std::error::Error::source(&*error);
            while let Some(cause) = source {
                eprintln!("  caused by: {cause}");
                source = cause.source();
            }
            ExitCode::FAILURE
        }
    }
}

async fn run() -> Result<ExitCode, Box<dyn std::error::Error>> {
    let args = match Args::from_env() {
        Ok(args) => args,
        // A usage error is worth pointing at the usage text, which is the one
        // thing that will actually fix it.
        Err(error @ (ArgError::Unknown(_) | ArgError::Unexpected(_))) => {
            eprintln!("ramjet-ingressd: {error}");
            return Ok(ExitCode::FAILURE);
        }
        Err(error) => return Err(Box::new(error)),
    };

    if args.help {
        print!("{USAGE}");
        return Ok(ExitCode::SUCCESS);
    }
    if args.version {
        println!("{} {}", env!("CARGO_PKG_NAME"), env!("CARGO_PKG_VERSION"));
        return Ok(ExitCode::SUCCESS);
    }

    init_logging();

    let engine = match select_engine(args.engine) {
        Ok(engine) => engine,
        Err(error) => return Err(error),
    };

    match (args.static_routes.clone(), engine) {
        (Some(path), Engine::Uring | Engine::UringStrict) => uring_mode(&args, &path).await,
        (Some(path), Engine::Hyper) => dev_mode(&args, &path).await,
        (None, Engine::Uring | Engine::UringStrict) => kubernetes::run_uring(&args).await,
        (None, Engine::Hyper) => kubernetes::run(&args).await,
    }
}

/// Which engine will actually serve, after asking the host whether it can.
///
/// The probe happens here, before anything binds, and that ordering is the
/// whole reason it is a separate step: falling back after a listener is up
/// would mean unbinding the ports the other engine needs, in the window where a
/// load balancer is already sending traffic at them.
///
/// The reason is always logged, with the `errno` behind it. "io_uring is
/// unavailable" without that is a support ticket rather than an answer, and the
/// two causes an operator actually hits — a kernel too old, and Docker's
/// default seccomp profile blocking `io_uring_setup` — are told apart only by
/// which error comes back.
fn select_engine(requested: Engine) -> Result<Engine, Box<dyn std::error::Error>> {
    if !requested.is_uring() {
        return Ok(requested);
    }
    let Err(error) = ramjet_engine::engine::probe() else {
        return Ok(requested);
    };

    if requested.is_strict() {
        return Err(format!(
            "--engine uring-strict was requested and the ramjet reactor will not start \
             on this host: {error}. On Linux this is usually io_uring_setup blocked by \
             seccomp — Docker's default profile does that — or a kernel older than 5.6. \
             Use --engine uring to fall back to hyper instead, or --engine hyper"
        )
        .into());
    }

    tracing::warn!(
        %error,
        requested = requested.as_str(),
        serving = Engine::Hyper.as_str(),
        "the ramjet reactor will not start on this host; falling back to the hyper engine"
    );
    Ok(Engine::Hyper)
}

/// Sends `tracing` output to stderr, filtered by `RUST_LOG`.
///
/// Without a subscriber the controller's logs — every watch error, every
/// rejected Ingress, every publish — go nowhere, which is not a defensible
/// state for a daemon whose whole job is reacting to a cluster it does not
/// control. `info` is the default because the interesting lines (a published
/// generation, a warning about a broken object) are all at that level and the
/// per-object detail is at `debug`.
fn init_logging() {
    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .init();
}

/// Serves a route table read from a file, with no Kubernetes anywhere.
async fn dev_mode(
    args: &Args,
    path: &std::path::Path,
) -> Result<ExitCode, Box<dyn std::error::Error>> {
    let loaded = config::load(path)?;
    let summary = loaded.summary;
    let has_certificates = !loaded.certs.is_empty();

    let routes = Arc::new(SharedRouteTable::new(loaded.table));
    // Kept alongside the store so the one generation dev mode has can be
    // recorded in the history: the rollback endpoints then work here exactly as
    // they do in Kubernetes mode, with nothing to roll back to.
    let cert_keys = Arc::new(loaded.certs.clone());
    let certs = Arc::new(CertStore::with_certs(loaded.certs));

    // Without an explicit --https or --no-https, a TLS listener over an empty
    // certificate store would fail every handshake. Binding it anyway would
    // look like a working HTTPS endpoint to anyone reading the startup output.
    // Kubernetes mode does not make this trade, because there the store fills
    // in after the socket binds.
    let https = if args.https_explicit || has_certificates {
        args.https
    } else {
        None
    };

    let readiness = ReadinessFlag::new();
    let server = Server::bind_with(
        proxy_config(args, https),
        Arc::clone(&routes),
        certs,
        readiness.clone(),
    )?;

    println!(
        "ramjet-ingressd {} — {} backend(s), {} endpoint(s), {} route(s), {} certificate(s){}",
        env!("CARGO_PKG_VERSION"),
        summary.backends,
        summary.endpoints,
        summary.routes,
        summary.certificates,
        if summary.default_backend {
            ", default backend set"
        } else {
            ""
        }
    );
    println!("  config   {}", path.display());
    for (label, addr) in [
        ("http    ", server.http_addr()),
        ("https   ", server.https_addr()),
        ("http3   ", server.http3_addr()),
        ("admin   ", server.admin_addr()),
    ] {
        match addr {
            Some(addr) => println!("  {label} {addr}"),
            None => println!("  {label} disabled"),
        }
    }
    if let Some(admin) = server.admin_addr() {
        println!("  probes   http://{admin}/healthz  http://{admin}/readyz  http://{admin}/metrics");
        println!("  admin    http://{admin}/admin/generations  http://{admin}/admin/routes");
    }

    // The one generation dev mode has, recorded the same way the Kubernetes
    // applier records every generation. Nothing is special-cased for it: the
    // history holds one entry, `/admin/generations` lists it, and rolling back
    // to it is a no-op that republishes what is already serving.
    //
    // The digest is zero because there is no control plane to have computed
    // one; the field means "which compiled configuration is this", and in dev
    // mode the answer is "the file you passed".
    let audit = ramjet_controller::AuditSink::logging_only();
    let table = routes.load_full();
    let diff = ramjet_controller::ConfigDiff::compute(None, &table);
    let published = server.history().record(
        table.generation(),
        0,
        Arc::new(diff.to_json()),
        table,
        cert_keys,
    );
    audit.applied(&diff, published);
    watch_pins(Arc::clone(server.history()), audit);

    // Dev mode reads the whole table before binding, so the replica is ready
    // the moment it can accept. The Kubernetes path flips this only once the
    // first compiled generation has landed.
    readiness.set_ready(true);

    finish(server.run(Shutdown::on_signal()).await)
}

/// Serves a route table read from a file on the experimental uring engine.
///
/// Kept separate from [`dev_mode`] rather than folded into it with branches:
/// the two engines share their configuration and their route table and nothing
/// else, and pretending otherwise would put `if engine ==` through the middle
/// of a path that is already correct.
async fn uring_mode(
    args: &Args,
    path: &std::path::Path,
) -> Result<ExitCode, Box<dyn std::error::Error>> {
    use std::sync::atomic::{AtomicBool, Ordering};

    let loaded = config::load(path)?;
    let summary = loaded.summary;
    let has_certificates = !loaded.certs.is_empty();

    if args.http.is_none() && args.https.is_none() {
        return Err(
            "--engine uring needs a listener; --no-http and --no-https together leave it \
             nothing to serve"
                .into(),
        );
    }

    let routes = Arc::new(SharedRouteTable::new(loaded.table));
    // Kept alongside the store so the one generation dev mode has can be
    // recorded in the history, exactly as the hyper lane's dev mode does.
    let cert_keys = Arc::new(loaded.certs.clone());
    let certs = Arc::new(CertStore::with_certs(loaded.certs));

    // The same rule dev mode applies: without an explicit --https, a TLS
    // listener over an empty certificate store would fail every handshake, and
    // binding it anyway would look like a working HTTPS endpoint to anyone
    // reading the startup output.
    let https = if args.https_explicit || has_certificates {
        args.https
    } else {
        None
    };
    let tls = match https {
        Some(_) => {
            let resolver = Arc::new(SniResolver::new(Arc::clone(&routes), Arc::clone(&certs)));
            Some(Arc::new(ramjet_proxy::tls::h1_server_config(resolver)?))
        }
        None => None,
    };

    // The mirror worker is a tokio task and has to be started from inside the
    // runtime. The reactor threads only `try_send` into its queue, which needs
    // no runtime at all, so this is the one piece of the uring lane that lives
    // on the other side.
    //
    // One queue for the whole engine rather than one per core: the hyper lane
    // gives each serving runtime its own so that a slow shadow fills one
    // runtime's queue rather than contending for a shared one, and that
    // argument does not carry here — the reactor threads are not the ones
    // draining it, and a `try_send` on a full channel is the same
    // constant-time drop from any of them.
    let mirror_metrics = Arc::new(ramjet_proxy::Metrics::new());
    let mirror = ramjet_engine::mirror::MirrorLane::new(
        ramjet_proxy::Mirror::spawn(
            ramjet_proxy::Upstream::new(&upstream_config(args)),
            Arc::clone(&mirror_metrics),
        )
        .with_max_body(args.mirror_max_body),
        mirror_metrics,
    );

    let readiness = Arc::new(AtomicBool::new(false));
    let config = ramjet_engine::engine::Config {
        http: args.http,
        https,
        tls,
        proxy_protocol: args.proxy_protocol.then_some(args.proxy_protocol_timeout),
        // Left unbound: the tokio listener below answers instead. The engine's
        // own admin serves `/metrics` and the probes and nothing else, and dev
        // mode is where an operator reaches for `/admin/routes` and
        // `ramjet-top`. It stays in the crate for an embedder that wants no
        // tokio at all, and the engine's own tests cover it.
        admin: None,
        workers: args.worker_threads,
        connect_timeout: args.connect_timeout,
        response_timeout: args.response_timeout,
        max_connect_attempts: args.max_connect_attempts,
        pool_max_idle_per_host: args.upstream_pool_idle,
        max_buf_size: args.max_buf_size,
        mirror_max_body: args.mirror_max_body,
        mirror: Some(mirror),
        ..ramjet_engine::engine::Config::default()
    };

    let engine = ramjet_engine::engine::Engine::bind(
        config,
        Arc::clone(&routes),
        Arc::clone(&readiness),
    )?;
    let metrics = engine.metrics();

    // The one generation dev mode has, recorded the way the Kubernetes applier
    // records every generation, so the rollback endpoints work here exactly as
    // they do there — with nothing to roll back to.
    let readiness_flag = ReadinessFlag::new();
    let history = Arc::new(ramjet_proxy::GenerationHistory::new(
        Arc::clone(&routes),
        Arc::clone(&certs),
        args.history_size,
    ));
    let audit = ramjet_controller::AuditSink::logging_only();
    {
        let table = routes.load_full();
        let diff = ramjet_controller::ConfigDiff::compute(None, &table);
        let published = history.record(
            table.generation(),
            0,
            Arc::new(diff.to_json()),
            table,
            cert_keys,
        );
        audit.applied(&diff, published);
    }
    watch_pins(Arc::clone(&history), audit);

    let (admin_handle, admin_shutdown) = Shutdown::channel();
    let admin_addr = match args.admin {
        Some(addr) => {
            let listener = ramjet_proxy::Listener::bind(&ListenerConfig::new(addr))?;
            let bound = listener.local_addr()?;
            let state = Arc::new(ramjet_proxy::AdminState {
                metrics: metrics as Arc<dyn ramjet_proxy::Exposition>,
                routes: Arc::clone(&routes),
                readiness: readiness_flag.clone(),
                history,
            });
            tokio::spawn(ramjet_proxy::serve_admin_only(
                listener,
                state,
                admin_shutdown,
            ));
            Some(bound)
        }
        None => None,
    };

    println!(
        "ramjet-ingressd {} — engine {}, {} backend(s), {} endpoint(s), {} route(s), \
         {} certificate(s){}",
        env!("CARGO_PKG_VERSION"),
        Engine::Uring.as_str(),
        summary.backends,
        summary.endpoints,
        summary.routes,
        summary.certificates,
        if summary.default_backend {
            ", default backend set"
        } else {
            ""
        }
    );
    println!("  config   {}", path.display());
    for (label, addr) in [
        ("http    ", engine.http_addr()),
        ("https   ", engine.https_addr()),
    ] {
        match addr {
            Some(addr) => println!("  {label} {addr}"),
            None => println!("  {label} disabled"),
        }
    }
    match admin_addr {
        Some(addr) => {
            println!("  admin    {addr}");
            println!("  probes   http://{addr}/healthz  http://{addr}/readyz  http://{addr}/metrics");
            println!("  admin    http://{addr}/admin/generations  http://{addr}/admin/routes");
        }
        None => println!("  admin    disabled"),
    }
    println!("  cores    {}", engine.cores());
    // Printed at startup, not buried in a doc comment: an operator who chose
    // this engine should see what they gave up before their first request
    // fails rather than after.
    for line in ramjet_engine::limits::V1_LIMITS.lines() {
        println!("  {line}");
    }

    // The table is read before the listeners bind, so the replica is ready the
    // moment it can accept.
    readiness.store(true, Ordering::Release);
    readiness_flag.set_ready(true);

    let stop = engine.shutdown();
    let mut signal = Shutdown::on_signal();
    tokio::spawn(async move {
        signal.recv().await;
        stop.stop();
        admin_handle.shutdown();
    });

    // The reactor is a blocking loop and is `!Send` per core, so it runs off
    // the async runtime entirely; this thread only waits for it.
    let outcome = tokio::task::spawn_blocking(move || engine.run())
        .await
        .map_err(|e| -> Box<dyn std::error::Error> { Box::new(std::io::Error::other(e)) })?;
    finish(outcome)
}

/// Reports every pin and resume to the audit trail, for as long as the process
/// runs.
///
/// The bridge exists because the two halves cannot see each other by design:
/// the rollback endpoints live on the admin listener in `ramjet-proxy`, which
/// knows nothing about Kubernetes, and the Events go through
/// `ramjet-controller`, which knows nothing about sockets. This binary is the
/// only place that depends on both, so this is where the wire goes.
pub(crate) fn watch_pins(
    history: Arc<ramjet_proxy::GenerationHistory>,
    audit: ramjet_controller::AuditSink,
) {
    use ramjet_proxy::PinChange;

    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    history.notify(tx);
    tokio::spawn(async move {
        while let Some(change) = rx.recv().await {
            match change {
                PinChange::Pinned {
                    generation,
                    replaced,
                } => audit.pinned(generation, replaced),
                PinChange::Resumed { generation } => audit.resumed(generation),
            }
        }
    });
}

/// The upstream settings every lane dials with.
///
/// Shared so that an operator changing `--engine` is not also changing their
/// connect timeout, which is the kind of difference that makes a benchmark
/// between the two meaningless.
pub(crate) fn upstream_config(args: &Args) -> UpstreamConfig {
    UpstreamConfig {
        connect_timeout: args.connect_timeout,
        response_timeout: args.response_timeout,
        max_connect_attempts: args.max_connect_attempts,
        pool_max_idle_per_host: args.upstream_pool_idle,
        ..UpstreamConfig::default()
    }
}

/// The listener and upstream configuration both modes share.
///
/// `https` is passed separately because it is the one setting the two modes
/// disagree about; everything else here is the same question with the same
/// answer regardless of where the route table comes from.
fn proxy_config(args: &Args, https: Option<SocketAddr>) -> ProxyConfig {
    ProxyConfig {
        http: args.http.map(ListenerConfig::new),
        https: https.map(ListenerConfig::new),
        admin: args.admin.map(ListenerConfig::new),
        upstream: upstream_config(args),
        shutdown_grace: args.shutdown_grace,
        worker_threads: args.worker_threads,
        max_buf_size: args.max_buf_size,
        mirror_max_body: args.mirror_max_body,
        history_size: args.history_size,
        proxy_protocol: args
            .proxy_protocol
            .then_some(args.proxy_protocol_timeout),
        // The QUIC listener takes the TLS listener's address, in UDP, and
        // follows it: where a mode decides not to open the TLS listener at all
        // — dev mode with no certificates declared — there is nothing for
        // HTTP/3 to serve either, and the startup banner says so rather than
        // leaving a UDP port that fails every handshake.
        http3: args.http3.then_some(https).flatten(),
    }
}

/// Turns the server's outcome into an exit code.
///
/// A drain that ran out of time is reported and then exits zero: the shutdown
/// was requested and did everything it was allowed to, and a non-zero exit
/// would make a normal rolling update look like a crash.
fn finish(result: std::io::Result<()>) -> Result<ExitCode, Box<dyn std::error::Error>> {
    match result {
        Ok(()) => {
            tracing::info!("drained cleanly");
            Ok(ExitCode::SUCCESS)
        }
        Err(error) if error.kind() == std::io::ErrorKind::TimedOut => {
            tracing::warn!(%error, "shutdown grace period expired");
            Ok(ExitCode::SUCCESS)
        }
        Err(error) => Err(Box::new(error)),
    }
}
