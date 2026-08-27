//! `ramjet-ingressd` is the daemon that runs the ramjet-ingress data plane.
//!
//! # What it does today
//!
//! It runs the proxy against a route table read from a file. That is dev mode,
//! selected with `--static-routes`, and it exists so the data plane can be run,
//! curled, profiled, and debugged without an API server anywhere near it.
//!
//! # What the Kubernetes phase changes
//!
//! Almost nothing here. The controller phase owns a
//! [`SharedRouteTable`](ramjet_router::SharedRouteTable) and a
//! [`CertStore`](ramjet_proxy::CertStore), publishes into them as informers
//! fire, and flips the [`ReadinessFlag`](ramjet_proxy::ReadinessFlag) once the
//! first table has landed. Then it calls exactly the same entry point this file
//! calls:
//!
//! ```no_run
//! # use std::sync::Arc;
//! # use ramjet_proxy::{CertStore, ProxyConfig, ReadinessFlag, Shutdown};
//! # use ramjet_router::SharedRouteTable;
//! # async fn wire(routes: Arc<SharedRouteTable>, certs: Arc<CertStore>) -> std::io::Result<()> {
//! let readiness = ReadinessFlag::new();
//! ramjet_proxy::serve(
//!     ProxyConfig::default(),
//!     routes,
//!     certs,
//!     readiness,
//!     Shutdown::on_signal(),
//! )
//! .await
//! # }
//! ```
//!
//! The data plane never learns where its table came from, which is the whole
//! point of publishing one through a pointer: dev mode and Kubernetes are the
//! same program with a different writer.

mod args;
mod config;

use std::process::ExitCode;
use std::sync::Arc;

use ramjet_proxy::{
    CertStore, ListenerConfig, ProxyConfig, ReadinessFlag, Server, Shutdown, UpstreamConfig,
};
use ramjet_router::SharedRouteTable;

use crate::args::{ArgError, Args, USAGE};

#[tokio::main]
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

    let Some(path) = args.static_routes.clone() else {
        eprintln!(
            "ramjet-ingressd: nothing to serve.\n\
             \n\
             The Kubernetes controller has not landed yet, so a route table has to come\n\
             from a file. Pass --static-routes <FILE> (see --help), for example:\n\
             \n\
             \x20   ramjet-ingressd --static-routes examples/dev-routes.yaml"
        );
        return Ok(ExitCode::FAILURE);
    };

    let loaded = config::load(&path)?;
    let summary = loaded.summary;
    let has_certificates = !loaded.certs.is_empty();

    let routes = Arc::new(SharedRouteTable::new(loaded.table));
    let certs = Arc::new(CertStore::with_certs(loaded.certs));

    // Without an explicit --https or --no-https, a TLS listener over an empty
    // certificate store would fail every handshake. Binding it anyway would
    // look like a working HTTPS endpoint to anyone reading the startup output.
    let https = if args.https_explicit || has_certificates {
        args.https
    } else {
        None
    };

    let config = ProxyConfig {
        http: args.http.map(ListenerConfig::new),
        https: https.map(ListenerConfig::new),
        admin: args.admin.map(ListenerConfig::new),
        upstream: UpstreamConfig {
            connect_timeout: args.connect_timeout,
            response_timeout: args.response_timeout,
            max_connect_attempts: args.max_connect_attempts,
            ..UpstreamConfig::default()
        },
        shutdown_grace: args.shutdown_grace,
    };

    let readiness = ReadinessFlag::new();
    let server = Server::bind_with(config, routes, Arc::clone(&certs), readiness.clone())?;

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
    // first informer sync has produced a table.
    readiness.set_ready(true);

    match server.run(Shutdown::on_signal()).await {
        Ok(()) => {
            println!("ramjet-ingressd: drained cleanly");
            Ok(ExitCode::SUCCESS)
        }
        Err(error) if error.kind() == std::io::ErrorKind::TimedOut => {
            // The shutdown was requested and did as much as it was allowed to.
            // That is a fact worth printing, not a failure to exit with.
            eprintln!("ramjet-ingressd: {error}");
            Ok(ExitCode::SUCCESS)
        }
        Err(error) => Err(Box::new(error)),
    }
}
