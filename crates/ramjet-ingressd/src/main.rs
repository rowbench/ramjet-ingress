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
#[cfg(test)]
mod testing;

use std::net::SocketAddr;
use std::process::ExitCode;
use std::sync::Arc;

use ramjet_proxy::{
    CertStore, ListenerConfig, ProxyConfig, ReadinessFlag, Server, Shutdown, UpstreamConfig,
};
use ramjet_router::SharedRouteTable;
use tracing_subscriber::EnvFilter;

use crate::args::{ArgError, Args, USAGE};

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

    match args.static_routes.clone() {
        Some(path) => dev_mode(&args, &path).await,
        None => kubernetes::run(&args).await,
    }
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
        routes,
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
        ("admin   ", server.admin_addr()),
    ] {
        match addr {
            Some(addr) => println!("  {label} {addr}"),
            None => println!("  {label} disabled"),
        }
    }
    if let Some(admin) = server.admin_addr() {
        println!("  probes   http://{admin}/healthz  http://{admin}/readyz  http://{admin}/metrics");
    }

    // Dev mode reads the whole table before binding, so the replica is ready
    // the moment it can accept. The Kubernetes path flips this only once the
    // first compiled generation has landed.
    readiness.set_ready(true);

    finish(server.run(Shutdown::on_signal()).await)
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
        upstream: UpstreamConfig {
            connect_timeout: args.connect_timeout,
            response_timeout: args.response_timeout,
            max_connect_attempts: args.max_connect_attempts,
            pool_max_idle_per_host: args.upstream_pool_idle,
            ..UpstreamConfig::default()
        },
        shutdown_grace: args.shutdown_grace,
        worker_threads: args.worker_threads,
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
