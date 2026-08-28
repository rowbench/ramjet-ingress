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
//! # Parsing and publishing are two jobs
//!
//! [`Publisher`] does not publish. It turns a generation's `CertMaterial` into
//! parsed keys, reusing what it already has, and hands them back; whether they
//! reach the data plane is
//! [`GenerationHistory`](ramjet_proxy::GenerationHistory)'s decision, because
//! that is where a rollback pin lives and there should be exactly one place
//! that knows what "pinned" means. The parse still happens while a pin is held:
//! the ring records live generations behind the pin so an operator can see what
//! they are holding back, and a recorded generation that could not be put back
//! on the wire would be a rollback target that fails when it is used.
//!
//! # What the history is a history of
//!
//! Generations this replica *applied*, which is not quite every generation the
//! controller compiled. The channel between them carries the latest value
//! rather than a queue, so two publishes closer together than one pass of this
//! loop coalesce and only the second is ever seen. That is the same property
//! the loop has always had and the reason a burst of churn costs what one
//! change costs — a pin does not change it, because the applier goes on
//! draining the channel either way. What it does mean is that
//! `/admin/generations` can show 41 followed by 44, and the gap is generations
//! that were never on the wire in the first place.
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

use ramjet_controller::{AuditSink, CompiledConfig, ConfigDiff, ControllerOpts};
use ramjet_proxy::{
    CertKeys, CertStore, GenerationHistory, ReadinessFlag, Server, Shutdown, ShutdownHandle,
};
use ramjet_router::{RouteTable, RouteTableBuilder, SharedRouteTable};
use rustls::sign::CertifiedKey;
use tokio::sync::watch;
use tracing::{error, info, warn};

use crate::args::Args;
use crate::certs;
use crate::promotion::{KubePatcher, Promoter};

/// Turns a compiled generation's certificate material into parsed keys,
/// reusing everything that did not rotate.
pub struct Publisher {
    /// Every key from the last generation prepared, by handle id.
    /// Content-addressed ids make this a cache with no invalidation problem: an
    /// entry is either still referenced, in which case it is still correct, or
    /// it is dropped.
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
    /// A publisher with an empty key cache.
    pub fn new() -> Self {
        Publisher {
            parsed: HashMap::new(),
        }
    }

    /// Parses one generation's certificates, reusing every id that survived.
    ///
    /// Total by construction — a certificate that will not parse is dropped
    /// with a warning and the rest of the generation is still usable.
    pub fn prepare(&mut self, config: &CompiledConfig) -> (Applied, Arc<CertKeys>) {
        let mut applied = Applied {
            generation: config.table.generation(),
            ..Applied::default()
        };

        let mut keys: CertKeys = HashMap::with_capacity(config.certs.len());
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
        (applied, Arc::new(keys))
    }
}

impl Default for Publisher {
    fn default() -> Self {
        Self::new()
    }
}

/// Watches Kubernetes and serves what it compiles, until a signal arrives.
pub async fn run(args: &Args) -> Result<ExitCode, Box<dyn std::error::Error>> {
    // Checked before the client, so a typo in the URL is a refusal to start
    // rather than a warning an hour later, from a process that has been failing
    // to deliver its audit trail to nowhere the whole time.
    if let Some(url) = &args.audit_webhook {
        AuditSink::check_webhook(url)?;
    }

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

    let audit = AuditSink::new(
        Some(client.clone()),
        &args.ingress_class,
        args.audit_webhook.as_deref(),
    )
    .await?;
    crate::watch_pins(Arc::clone(server.history()), audit.clone());

    let (configs, controller) = ramjet_controller::spawn(client.clone(), opts)?;

    // A second reader of the same channel, rather than a second watch or a
    // periodic `list`: the controller has already read every Ingress and parsed
    // every annotation, so the promotion loop's candidates arrive with the
    // generation that compiled them and cost no API traffic at all. With nobody
    // opted in the list is empty and the loop is a timer that does nothing.
    let promoter = tokio::spawn(
        Promoter::new(
            KubePatcher::new(client),
            audit.clone(),
            Arc::clone(&routes),
            Arc::clone(server.history()),
        )
        .run(configs.clone()),
    );

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

    let applier = tokio::spawn(apply(
        configs,
        Publisher::new(),
        Arc::clone(server.history()),
        audit,
        readiness,
        handle,
    ));

    let result = server.run(shutdown).await;

    // Whether the control plane outlived the data plane decides the exit code,
    // so it has to be read before the abort makes it moot.
    let control_plane_stopped = controller.is_finished();

    // Aborting the controller handle stops all five watches: they live inside
    // that one task by construction.
    applier.abort();
    promoter.abort();
    controller.abort();
    let _ = applier.await;
    let _ = promoter.await;
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

/// Records every generation the controller compiles, and publishes the ones a
/// rollback is not holding back.
///
/// The receiver is drained whether or not anything is being published, which is
/// the property a pin depends on: leaving generations queued would block the
/// controller's rebuild loop, and releasing the pin would then jump to whatever
/// had been stuck in the channel rather than to the state of the cluster.
async fn apply(
    mut configs: watch::Receiver<Arc<CompiledConfig>>,
    mut publisher: Publisher,
    history: Arc<GenerationHistory>,
    audit: AuditSink,
    readiness: ReadinessFlag,
    shutdown: ShutdownHandle,
) {
    // Diffed against, and deliberately the previous *compiled* generation
    // rather than the previous published one: while a pin is held, each
    // generation's diff should describe what the controller did since its own
    // predecessor, not restate everything that has happened since the pin.
    let mut previous: Option<Arc<RouteTable>> = None;

    // The initial value is the controller's generation-0 seed — an empty table
    // that means "nothing compiled yet" — and a fresh receiver has already seen
    // it, so this waits for the first real publish.
    while configs.changed().await.is_ok() {
        let config = Arc::clone(&*configs.borrow_and_update());
        let (applied, keys) = publisher.prepare(&config);
        let diff = ConfigDiff::compute(previous.as_deref(), &config.table);

        let published = history.record(
            applied.generation,
            config.digest,
            Arc::new(diff.to_json()),
            Arc::clone(&config.table),
            keys,
        );
        previous = Some(Arc::clone(&config.table));
        audit.applied(&diff, published);

        info!(
            generation = applied.generation,
            published,
            certificates = applied.certificates(),
            parsed = applied.parsed,
            reused = applied.reused,
            rejected = applied.rejected,
            "{}",
            if published {
                "published to the data plane"
            } else {
                "recorded but held back by a rollback pin"
            }
        );

        // Readiness is one-way: a later generation never takes the replica out
        // of rotation, because a table that is one debounce window stale is
        // still far better than 404ing everything while Kubernetes reroutes.
        if published && applied.generation > 0 && !readiness.is_ready() {
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
    use ramjet_router::{CertifiedKeyHandle, Endpoint, LbPolicy, PathType};

    /// A compiled configuration at `generation`, serving `host` over the given
    /// certificates and routing to an endpoint on `port` — so two generations
    /// can be told apart both by number and by where they send a request.
    fn compiled(
        generation: u64,
        host: &str,
        port: u16,
        certs: Vec<CertMaterial>,
    ) -> CompiledConfig {
        let mut builder = RouteTableBuilder::new();
        builder.generation(generation);
        builder
            .backend(
                "app",
                LbPolicy::RoundRobin,
                vec![Endpoint::new(
                    format!("10.0.0.1:{port}").parse().expect("an address"),
                )],
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
            promotions: Vec::new(),
            digest: u64::from(port),
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

    /// The pipeline one generation goes through: parse, then record, then
    /// publish if nothing is holding the gate.
    struct Pipeline {
        publisher: Publisher,
        history: Arc<GenerationHistory>,
        routes: Arc<SharedRouteTable>,
        certs: Arc<CertStore>,
        previous: Option<Arc<RouteTable>>,
    }

    impl Pipeline {
        fn new() -> Self {
            let routes = Arc::new(SharedRouteTable::new(
                RouteTableBuilder::new().build().expect("an empty table"),
            ));
            let certs = Arc::new(CertStore::new());
            Pipeline {
                publisher: Publisher::new(),
                history: Arc::new(GenerationHistory::new(
                    Arc::clone(&routes),
                    Arc::clone(&certs),
                    10,
                )),
                routes,
                certs,
                previous: None,
            }
        }

        fn apply(&mut self, config: &CompiledConfig) -> (Applied, bool) {
            let (applied, keys) = self.publisher.prepare(config);
            let diff = ConfigDiff::compute(self.previous.as_deref(), &config.table);
            let published = self.history.record(
                applied.generation,
                config.digest,
                Arc::new(diff.to_json()),
                Arc::clone(&config.table),
                keys,
            );
            self.previous = Some(Arc::clone(&config.table));
            (applied, published)
        }
    }

    #[test]
    fn a_generation_publishes_its_table_and_its_certificates() {
        let mut pipeline = Pipeline::new();
        let (applied, published) = pipeline.apply(&compiled(
            1,
            "app.example.com",
            8080,
            vec![material(7, "app.example.com")],
        ));

        assert!(published);
        assert_eq!(applied.parsed, 1);
        assert_eq!(applied.rejected, 0);
        assert_eq!(pipeline.certs.len(), 1);
        assert!(pipeline.certs.get(7).is_some(), "the handle id is the store's key");

        let table = pipeline.routes.load_full();
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
        let mut pipeline = Pipeline::new();
        pipeline.apply(&compiled(
            1,
            "app.example.com",
            8080,
            vec![material(11, "app.example.com")],
        ));

        let table = pipeline.routes.load_full();
        let handle = table
            .tls()
            .resolve("app.example.com")
            .expect("the name resolves");
        assert!(
            pipeline.certs.get(handle.id()).is_some(),
            "a table published over a store missing its ids fails every handshake"
        );
    }

    #[test]
    fn an_unchanged_certificate_is_not_parsed_twice() {
        let mut pipeline = Pipeline::new();
        let (first, _) = pipeline.apply(&compiled(
            1,
            "app.example.com",
            8080,
            vec![material(7, "app.example.com")],
        ));
        assert_eq!((first.parsed, first.reused), (1, 0));

        // A different generation carrying the same content-addressed id: the
        // Secret did not move, so nothing should be re-parsed.
        let same = CertMaterial {
            handle_id: 7,
            cert_chain_pem: b"garbage that would never parse".to_vec(),
            key_pem: b"nor would this".to_vec(),
        };
        let (second, _) = pipeline.apply(&compiled(2, "app.example.com", 8080, vec![same]));
        assert_eq!(
            (second.parsed, second.reused),
            (0, 1),
            "a surviving handle id must reuse the key already parsed"
        );
        assert!(pipeline.certs.get(7).is_some());
    }

    #[test]
    fn a_rotated_certificate_replaces_the_old_one() {
        let mut pipeline = Pipeline::new();
        pipeline.apply(&compiled(
            1,
            "app.example.com",
            8080,
            vec![material(7, "app.example.com")],
        ));
        assert!(pipeline.certs.get(7).is_some());

        // Rotation means new content, and content-addressed ids mean a new id.
        let (rotated, _) = pipeline.apply(&compiled(
            2,
            "app.example.com",
            8080,
            vec![material(8, "app.example.com")],
        ));
        assert_eq!((rotated.parsed, rotated.reused), (1, 0));
        assert_eq!(pipeline.certs.len(), 1, "the retired id is dropped, not accumulated");
        assert!(pipeline.certs.get(7).is_none());
        assert!(pipeline.certs.get(8).is_some());
    }

    #[test]
    fn an_unparseable_certificate_is_skipped_and_the_rest_goes_live() {
        let mut pipeline = Pipeline::new();
        let broken = CertMaterial {
            handle_id: 1,
            cert_chain_pem: b"-----BEGIN CERTIFICATE-----\nnope\n-----END CERTIFICATE-----\n".to_vec(),
            key_pem: b"neither is this a key".to_vec(),
        };
        let (applied, _) = pipeline.apply(&compiled(
            1,
            "app.example.com",
            8080,
            vec![broken, material(2, "app.example.com")],
        ));

        assert_eq!(applied.rejected, 1);
        assert_eq!(applied.parsed, 1);
        assert!(
            pipeline.certs.get(1).is_none(),
            "a broken certificate is not published"
        );
        assert!(pipeline.certs.get(2).is_some());
        assert!(
            pipeline
                .routes
                .load_full()
                .match_request("app.example.com", "/")
                .is_some(),
            "routing must survive a Secret that does not parse"
        );
    }

    /// The rollback, all the way through the pipeline the daemon actually runs:
    /// a pinned generation's certificates go back with its table, later
    /// generations are recorded without reaching the wire, and the pinned
    /// generation's key is still in the store while the pin holds.
    #[test]
    fn a_pin_republishes_a_generations_certificates_with_its_table() {
        let mut pipeline = Pipeline::new();
        pipeline.apply(&compiled(
            1,
            "app.example.com",
            8080,
            vec![material(7, "app.example.com")],
        ));
        pipeline.apply(&compiled(
            2,
            "app.example.com",
            8081,
            vec![material(8, "app.example.com")],
        ));
        assert!(pipeline.certs.get(8).is_some());
        assert!(pipeline.certs.get(7).is_none(), "generation 2 retired it");

        pipeline.history.pin(1).expect("generation 1 is in the ring");
        assert!(
            pipeline.certs.get(7).is_some(),
            "the pinned generation's certificate must come back with its table, \
             or every handshake for its names fails"
        );
        assert!(pipeline.certs.get(8).is_none());
        assert_eq!(pipeline.routes.generation(), 1);

        // The controller has not stopped. Its work is recorded and held.
        let (_, published) = pipeline.apply(&compiled(
            3,
            "app.example.com",
            8082,
            vec![material(9, "app.example.com")],
        ));
        assert!(!published);
        assert_eq!(pipeline.routes.generation(), 1);
        assert!(
            pipeline.certs.get(7).is_some(),
            "a held-back generation must not touch the certificate store either"
        );

        assert_eq!(pipeline.history.unpin(), Some(3));
        assert_eq!(pipeline.routes.generation(), 3);
        assert!(pipeline.certs.get(9).is_some());
    }

    /// Per-route counters are the reason `/admin/routes` is worth reading, and
    /// they are only worth reading if a deploy does not reset them.
    #[test]
    fn route_counters_survive_a_rebuild_through_the_publisher() {
        let mut pipeline = Pipeline::new();
        pipeline.apply(&compiled(1, "app.example.com", 8080, Vec::new()));

        let table = pipeline.routes.load_full();
        let (_, rule) = table.routes().next().expect("one route");
        table
            .route_stats()
            .slot(rule.stats_index())
            .expect("a counter block")
            .shard(0)
            .record_response(200);

        // A second generation of the same route, built the way the controller
        // builds one: from the previous table.
        let mut builder = RouteTableBuilder::from_previous(&table);
        builder
            .backend(
                "app",
                LbPolicy::RoundRobin,
                vec![Endpoint::new("10.0.0.1:8080".parse().expect("an address"))],
            )
            .expect("registers");
        builder
            .route(Some("app.example.com"), "/", PathType::Prefix, "app")
            .expect("drafts");
        pipeline.apply(&CompiledConfig {
            table: Arc::new(builder.build().expect("builds")),
            certs: Vec::new(),
            promotions: Vec::new(),
            digest: 2,
        });

        let table = pipeline.routes.load_full();
        let (_, rule) = table.routes().next().expect("one route");
        assert_eq!(
            table
                .route_stats()
                .slot(rule.stats_index())
                .expect("a counter block")
                .totals()
                .requests,
            1,
            "a route that survived the rebuild must keep its counters"
        );
    }

    /// An empty configuration at an arbitrary generation.
    fn at_generation(generation: u64) -> Arc<CompiledConfig> {
        let mut builder = RouteTableBuilder::new();
        builder.generation(generation);
        Arc::new(CompiledConfig {
            table: Arc::new(builder.build().expect("builds")),
            certs: Vec::new(),
            promotions: Vec::new(),
            digest: generation,
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

    fn history() -> (Arc<GenerationHistory>, Arc<SharedRouteTable>) {
        let routes = Arc::new(SharedRouteTable::new(
            RouteTableBuilder::new().build().expect("an empty table"),
        ));
        let history = Arc::new(GenerationHistory::new(
            Arc::clone(&routes),
            Arc::new(CertStore::new()),
            10,
        ));
        (history, routes)
    }

    #[tokio::test]
    async fn readiness_waits_for_a_compiled_generation() {
        let readiness = ReadinessFlag::new();
        let (shutdown_handle, mut shutdown) = Shutdown::channel();
        let (history, routes) = history();

        // The channel starts on the controller's seed, which a fresh receiver
        // has already seen.
        let (tx, rx) = watch::channel(at_generation(0));
        let applier = tokio::spawn(apply(
            rx,
            Publisher::new(),
            Arc::clone(&history),
            AuditSink::logging_only(),
            readiness.clone(),
            shutdown_handle,
        ));
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

    /// The pin's other half: the applier must keep *draining* the channel while
    /// publication is held, or the controller's rebuild loop stalls behind it
    /// and resuming lands on a stale generation rather than the current one.
    #[tokio::test]
    async fn a_pinned_applier_keeps_recording_what_it_is_holding_back() {
        let (shutdown_handle, _shutdown) = Shutdown::channel();
        let (history, routes) = history();

        let (tx, rx) = watch::channel(at_generation(0));
        tokio::spawn(apply(
            rx,
            Publisher::new(),
            Arc::clone(&history),
            AuditSink::logging_only(),
            ReadinessFlag::new(),
            shutdown_handle,
        ));

        tx.send(at_generation(1)).expect("listening");
        eventually("generation 1 landed", || routes.generation() == 1).await;
        history.pin(1).expect("pins");

        // Each one is waited for before the next is sent. The channel carries
        // the latest value rather than a queue — see the module docs — so
        // firing three at once would legitimately coalesce into one, and this
        // test is about what the pin does, not about the channel.
        for generation in 2..=4 {
            tx.send(at_generation(generation)).expect("listening");
            eventually("the generation was recorded", || {
                history.with_records(|_, ring| {
                    ring.iter().any(|record| record.generation == generation)
                })
            })
            .await;
        }
        assert_eq!(routes.generation(), 1, "nothing reached the wire");

        let held: Vec<(u64, bool)> = history
            .with_records(|_, ring| ring.iter().map(|r| (r.generation, r.published)).collect());
        assert_eq!(held, vec![(1, true), (2, false), (3, false), (4, false)]);

        assert_eq!(history.unpin(), Some(4));
        assert_eq!(routes.generation(), 4, "resuming publishes the newest");
    }

    #[test]
    fn the_generation_reported_is_the_one_published() {
        let mut pipeline = Pipeline::new();
        let first: RouteTable = RouteTableBuilder::new().build().expect("builds");
        let second = RouteTableBuilder::from_previous(&first)
            .build()
            .expect("builds");
        let generation = second.generation();

        let (applied, published) = pipeline.apply(&CompiledConfig {
            table: Arc::new(second),
            certs: Vec::new(),
            promotions: Vec::new(),
            digest: 0,
        });
        assert!(published);
        assert_eq!(applied.generation, generation);
        assert_eq!(pipeline.routes.generation(), generation);
    }
}
