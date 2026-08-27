//! Kubernetes mode: the control plane's compiled configuration, applied to the
//! running data plane.
//!
//! [`ramjet_controller::spawn`] hands over an
//! [`Arc<CompiledConfig>`](CompiledConfig) per generation; the proxy reads a
//! [`SharedRouteTable`] and a [`CertStore`]. This module is the seam between
//! them, and it is deliberately the only place in the tree that touches both.
//!
//! # Certificates first, then the table
//!
//! The route table and the certificate store are two independent `ArcSwap`s, so
//! a handshake can observe a new table against an older store. Publishing the
//! certificates *first* makes the only possible skew a store holding a key
//! nothing points at yet, which is invisible. The other order would leave an
//! `SniMap` entry whose id is not in the store, and
//! [`SniResolver`](ramjet_proxy::SniResolver) answers a missing id with `None`
//! — which rustls turns into a failed handshake. A rotation would drop
//! connections for as long as the gap lasted.
//!
//! That is also what a rejected certificate costs: its id stays out of the
//! store, so TLS for the names it covers fails until the Secret is fixed. Every
//! other host, and all plaintext traffic, is unaffected. Refusing the whole
//! generation instead would let one malformed Secret in one namespace take the
//! cluster's routing offline.
//!
//! # Parsing only what moved
//!
//! [`CertMaterial::handle_id`](ramjet_controller::CertMaterial::handle_id) is
//! derived from the Secret's namespace, name, and *content*, so an id changes
//! if and only if the bytes did. Keeping the parsed keys by id and carrying
//! forward every id that survives means a cluster with 500 certificates does no
//! X.509 work at all when one unrelated Ingress is edited.

use std::collections::HashMap;
use std::process::ExitCode;
use std::sync::Arc;

use ramjet_controller::{CompiledConfig, ControllerOpts};
use ramjet_proxy::{CertStore, ReadinessFlag, Server, Shutdown, ShutdownHandle};
use ramjet_router::{RouteTableBuilder, SharedRouteTable};
use rustls::sign::CertifiedKey;
use tokio::sync::watch;
use tracing::{error, info, warn};

use crate::args::Args;
use crate::certs;

/// Applies compiled configurations to the data plane, in the one order that is
/// safe, reusing the keys it has already parsed.
pub struct Publisher {
    routes: Arc<SharedRouteTable>,
    certs: Arc<CertStore>,
    /// Every key currently published, by handle id. Content-addressed ids make
    /// this a cache with no invalidation problem: an entry is either still
    /// referenced, in which case it is still correct, or it is dropped.
    parsed: HashMap<u64, Arc<CertifiedKey>>,
}

/// What one application of a [`CompiledConfig`] did.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct Applied {
    /// Generation of the table now published.
    pub generation: u64,
    /// Certificates carried over from the previous generation unparsed.
    pub reused: usize,
    /// Certificates parsed for the first time.
    pub parsed: usize,
    /// Certificates the parse rejected. Their names cannot serve TLS.
    pub rejected: usize,
}

impl Applied {
    /// Certificates the store now holds.
    pub fn certificates(&self) -> usize {
        self.reused + self.parsed
    }
}

impl Publisher {
    /// A publisher writing into `routes` and `certs`.
    pub fn new(routes: Arc<SharedRouteTable>, certs: Arc<CertStore>) -> Self {
        Publisher {
            routes,
            certs,
            parsed: HashMap::new(),
        }
    }

    /// Publishes one generation: certificates, then the table that names them.
    ///
    /// Total by construction — a certificate that will not parse is dropped
    /// with a warning and the rest of the generation goes live.
    pub fn apply(&mut self, config: &CompiledConfig) -> Applied {
        let mut applied = Applied {
            generation: config.table.generation(),
            ..Applied::default()
        };

        let mut keys: HashMap<u64, Arc<CertifiedKey>> = HashMap::with_capacity(config.certs.len());
        for material in &config.certs {
            if let Some(key) = self.parsed.get(&material.handle_id) {
                keys.insert(material.handle_id, Arc::clone(key));
                applied.reused += 1;
                continue;
            }
            match certs::certified_key(&material.cert_chain_pem, &material.key_pem) {
                Ok(key) => {
                    keys.insert(material.handle_id, Arc::new(key));
                    applied.parsed += 1;
                }
                Err(error) => {
                    // The handle id is the only identifier that crosses the
                    // layering boundary; the controller logs the Secret it came
                    // from, and the two are joined by that number.
                    warn!(
                        handle = material.handle_id,
                        %error,
                        "certificate will not parse; TLS for its names will fail until it is fixed"
                    );
                    applied.rejected += 1;
                }
            }
        }

        // Cheap: the values are `Arc`s, so this clones pointers, not keys.
        self.parsed = keys.clone();
        self.certs.publish(keys);
        self.routes.store_shared(Arc::clone(&config.table));

        applied
    }
}

/// Watches Kubernetes and serves what it compiles, until a signal arrives.
pub async fn run(args: &Args) -> Result<ExitCode, Box<dyn std::error::Error>> {
    let client = kube::Client::try_default().await?;
    let opts = ControllerOpts {
        namespace: args.watch_namespace.clone(),
        class_name: args.ingress_class.clone(),
        default_backend: args.default_backend.clone(),
        default_tls_secret: args.default_tls_secret.clone(),
        publish_address: args.publish_address.clone(),
        publish_service: args.publish_service.clone(),
        update_status: args.update_status,
        ..ControllerOpts::default()
    };

    let routes = Arc::new(SharedRouteTable::new(RouteTableBuilder::new().build()?));
    let certs = Arc::new(CertStore::new());
    let readiness = ReadinessFlag::new();

    // Unlike dev mode, the TLS listener binds even though the store is empty:
    // the certificates are arriving over a watch that has not finished its
    // first list yet, and refusing to bind 443 because of that would mean a
    // restart could never recover a cluster's HTTPS. `/readyz` is what keeps
    // traffic away in the meantime.
    let config = crate::proxy_config(args, args.https);
    let server = Server::bind_with(
        config,
        Arc::clone(&routes),
        Arc::clone(&certs),
        readiness.clone(),
    )?;

    info!(
        version = env!("CARGO_PKG_VERSION"),
        ingress_class = %args.ingress_class,
        namespace = args.watch_namespace.as_deref().unwrap_or("<all>"),
        http = ?server.http_addr(),
        https = ?server.https_addr(),
        admin = ?server.admin_addr(),
        "starting in kubernetes mode"
    );

    let (configs, controller) = ramjet_controller::spawn(client, opts)?;

    // The server's own signal handling is replaced by a channel this process
    // can also fire, so a control plane that stops taking us down with it goes
    // through exactly the same drain as a SIGTERM.
    let (handle, shutdown) = Shutdown::channel();
    let mut signal = Shutdown::on_signal();
    let on_signal = handle.clone();
    tokio::spawn(async move {
        signal.recv().await;
        on_signal.shutdown();
    });

    let publisher = Publisher::new(routes, certs);
    let applier = tokio::spawn(apply(configs, publisher, readiness, handle));

    let result = server.run(shutdown).await;

    // Whether the control plane outlived the data plane decides the exit code,
    // so it has to be read before the abort makes it moot.
    let control_plane_stopped = controller.is_finished();

    // Aborting the controller handle stops all five watches: they live inside
    // that one task by construction.
    applier.abort();
    controller.abort();
    let _ = applier.await;
    let _ = controller.await;

    let code = crate::finish(result)?;
    if control_plane_stopped {
        // The proxy drained cleanly, but this replica is no longer an ingress
        // controller — it was serving a frozen table. Exit non-zero so the
        // restart is visible in `kubectl get pods` rather than silent.
        return Ok(ExitCode::FAILURE);
    }
    Ok(code)
}

/// Publishes every generation the controller compiles.
async fn apply(
    mut configs: watch::Receiver<Arc<CompiledConfig>>,
    mut publisher: Publisher,
    readiness: ReadinessFlag,
    shutdown: ShutdownHandle,
) {
    // The initial value is the controller's generation-0 seed — an empty table
    // that means "nothing compiled yet" — and a fresh receiver has already seen
    // it, so this waits for the first real publish.
    while configs.changed().await.is_ok() {
        let config = Arc::clone(&*configs.borrow_and_update());
        let applied = publisher.apply(&config);

        info!(
            generation = applied.generation,
            certificates = applied.certificates(),
            parsed = applied.parsed,
            reused = applied.reused,
            rejected = applied.rejected,
            "published to the data plane"
        );

        // Readiness is one-way: a later generation never takes the replica out
        // of rotation, because a table that is one debounce window stale is
        // still far better than 404ing everything while Kubernetes reroutes.
        if applied.generation > 0 && !readiness.is_ready() {
            readiness.set_ready(true);
            info!(generation = applied.generation, "ready");
        }
    }

    error!("control plane stopped; draining");
    shutdown.shutdown();
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::self_signed;
    use ramjet_controller::CertMaterial;
    use ramjet_router::{CertifiedKeyHandle, Endpoint, LbPolicy, PathType, RouteTable};

    /// A compiled configuration serving `host` over the given certificates.
    fn compiled(host: &str, certs: Vec<CertMaterial>) -> CompiledConfig {
        let mut builder = RouteTableBuilder::new();
        builder
            .backend(
                "app",
                LbPolicy::RoundRobin,
                vec![Endpoint::new("10.0.0.1:8080".parse().expect("an address"))],
            )
            .expect("registers");
        builder
            .route(Some(host), "/", PathType::Prefix, "app")
            .expect("drafts");
        for material in &certs {
            builder
                .certificate(host, Arc::new(CertifiedKeyHandle::new(material.handle_id)))
                .expect("a valid host");
        }
        CompiledConfig {
            table: Arc::new(builder.build().expect("builds")),
            certs,
        }
    }

    fn material(handle_id: u64, name: &str) -> CertMaterial {
        let (cert_chain_pem, key_pem) = self_signed(name);
        CertMaterial {
            handle_id,
            cert_chain_pem,
            key_pem,
        }
    }

    fn publisher() -> (Publisher, Arc<SharedRouteTable>, Arc<CertStore>) {
        let routes = Arc::new(SharedRouteTable::new(
            RouteTableBuilder::new().build().expect("an empty table"),
        ));
        let certs = Arc::new(CertStore::new());
        (
            Publisher::new(Arc::clone(&routes), Arc::clone(&certs)),
            routes,
            certs,
        )
    }

    #[test]
    fn a_generation_publishes_its_table_and_its_certificates() {
        let (mut publisher, routes, certs) = publisher();
        let applied = publisher.apply(&compiled("app.example.com", vec![material(7, "app.example.com")]));

        assert_eq!(applied.parsed, 1);
        assert_eq!(applied.rejected, 0);
        assert_eq!(certs.len(), 1);
        assert!(certs.get(7).is_some(), "the handle id is the store's key");

        let table = routes.load_full();
        assert!(table.match_request("app.example.com", "/anything").is_some());
        assert_eq!(
            table.tls().resolve("app.example.com").map(|h| h.id()),
            Some(7),
            "the published table must point at the id the store holds"
        );
    }

    /// The invariant the whole module exists for: a name the table resolves is
    /// a name the store can answer for. Checked *after* a publish, since that
    /// is the only moment the two snapshots are meant to agree.
    #[test]
    fn every_handle_the_table_names_is_in_the_store() {
        let (mut publisher, routes, certs) = publisher();
        publisher.apply(&compiled("app.example.com", vec![material(11, "app.example.com")]));

        let table = routes.load_full();
        let handle = table
            .tls()
            .resolve("app.example.com")
            .expect("the name resolves");
        assert!(
            certs.get(handle.id()).is_some(),
            "a table published over a store missing its ids fails every handshake"
        );
    }

    #[test]
    fn an_unchanged_certificate_is_not_parsed_twice() {
        let (mut publisher, _routes, certs) = publisher();
        let first = publisher.apply(&compiled("app.example.com", vec![material(7, "app.example.com")]));
        assert_eq!((first.parsed, first.reused), (1, 0));

        // A different generation carrying the same content-addressed id: the
        // Secret did not move, so nothing should be re-parsed.
        let same = CertMaterial {
            handle_id: 7,
            cert_chain_pem: b"garbage that would never parse".to_vec(),
            key_pem: b"nor would this".to_vec(),
        };
        let second = publisher.apply(&compiled("app.example.com", vec![same]));
        assert_eq!(
            (second.parsed, second.reused),
            (0, 1),
            "a surviving handle id must reuse the key already parsed"
        );
        assert!(certs.get(7).is_some());
    }

    #[test]
    fn a_rotated_certificate_replaces_the_old_one() {
        let (mut publisher, _routes, certs) = publisher();
        publisher.apply(&compiled("app.example.com", vec![material(7, "app.example.com")]));
        assert!(certs.get(7).is_some());

        // Rotation means new content, and content-addressed ids mean a new id.
        let rotated = publisher.apply(&compiled("app.example.com", vec![material(8, "app.example.com")]));
        assert_eq!((rotated.parsed, rotated.reused), (1, 0));
        assert_eq!(certs.len(), 1, "the retired id is dropped, not accumulated");
        assert!(certs.get(7).is_none());
        assert!(certs.get(8).is_some());
    }

    #[test]
    fn an_unparseable_certificate_is_skipped_and_the_rest_goes_live() {
        let (mut publisher, routes, certs) = publisher();
        let broken = CertMaterial {
            handle_id: 1,
            cert_chain_pem: b"-----BEGIN CERTIFICATE-----\nnope\n-----END CERTIFICATE-----\n".to_vec(),
            key_pem: b"neither is this a key".to_vec(),
        };
        let applied = publisher.apply(&compiled(
            "app.example.com",
            vec![broken, material(2, "app.example.com")],
        ));

        assert_eq!(applied.rejected, 1);
        assert_eq!(applied.parsed, 1);
        assert!(certs.get(1).is_none(), "a broken certificate is not published");
        assert!(certs.get(2).is_some());
        assert!(
            routes
                .load_full()
                .match_request("app.example.com", "/")
                .is_some(),
            "routing must survive a Secret that does not parse"
        );
    }

    /// An empty configuration at an arbitrary generation.
    fn at_generation(generation: u64) -> Arc<CompiledConfig> {
        let mut builder = RouteTableBuilder::new();
        builder.generation(generation);
        Arc::new(CompiledConfig {
            table: Arc::new(builder.build().expect("builds")),
            certs: Vec::new(),
        })
    }

    /// Waits for `condition`, failing the test rather than hanging if the
    /// applier task never gets there.
    async fn eventually(what: &str, condition: impl Fn() -> bool) {
        let waited = tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while !condition() {
                tokio::task::yield_now().await;
            }
        })
        .await;
        assert!(waited.is_ok(), "{what} never happened");
    }

    #[tokio::test]
    async fn readiness_waits_for_a_compiled_generation() {
        let (publisher, routes, _certs) = publisher();
        let readiness = ReadinessFlag::new();
        let (shutdown_handle, mut shutdown) = Shutdown::channel();

        // The channel starts on the controller's seed, which a fresh receiver
        // has already seen.
        let (tx, rx) = watch::channel(at_generation(0));
        let applier = tokio::spawn(apply(rx, publisher, readiness.clone(), shutdown_handle));
        assert!(!readiness.is_ready(), "nothing has been compiled yet");

        // Generation 0 means "no configuration has been compiled". Publishing
        // it must not put the replica into rotation: an empty table 404s every
        // request, which is worse than being absent from the Service.
        tx.send(at_generation(0)).expect("the applier is listening");
        eventually("the seed was applied", || routes.generation() == 0).await;
        assert!(!readiness.is_ready(), "generation 0 must not mean ready");

        tx.send(at_generation(1)).expect("the applier is listening");
        eventually("the first generation landed", || readiness.is_ready()).await;
        assert_eq!(routes.generation(), 1);

        // A control plane that stops has to take the process down with it,
        // rather than leaving a replica serving a table that can never change.
        drop(tx);
        tokio::time::timeout(std::time::Duration::from_secs(5), shutdown.recv())
            .await
            .expect("a dead control plane must trigger the drain");
        applier.await.expect("the applier ends when the channel closes");
    }

    #[test]
    fn the_generation_reported_is_the_one_published() {
        let (mut publisher, routes, _certs) = publisher();
        let first: RouteTable = RouteTableBuilder::new().build().expect("builds");
        let second = RouteTableBuilder::from_previous(&first)
            .build()
            .expect("builds");
        let generation = second.generation();

        let applied = publisher.apply(&CompiledConfig {
            table: Arc::new(second),
            certs: Vec::new(),
        });
        assert_eq!(applied.generation, generation);
        assert_eq!(routes.generation(), generation);
    }
}
