//! The watch, coalesce, compile, publish loop.
//!
//! Five reflectors keep an in-memory mirror of the objects we care about. Every
//! event they see pokes a one-slot channel; one rebuild task drains that
//! channel, waits out a short debounce, and compiles the *current* state of the
//! stores — not the event that woke it.
//!
//! Reacting to state rather than to events is what makes the loop correct under
//! load. A 50-pod rollout produces 50 EndpointSlice events; an event-driven
//! controller compiles 50 tables and throws 49 away. This one compiles at most
//! one table per debounce window, and each is built from everything known at
//! that instant, so a burst of churn costs the same as a single change.
//!
//! Publishing is suppressed when the compiled digest matches what is already
//! out there, which matters more than it sounds: the API server re-sends
//! objects on every watch restart and on periodic resyncs, and without the
//! check each of those would bump the generation and hand the data plane a
//! table identical to the one it already has.

use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Instant;

use futures::{FutureExt, StreamExt};
use k8s_openapi::api::core::v1::{Secret, Service};
use k8s_openapi::api::discovery::v1::EndpointSlice;
use k8s_openapi::api::networking::v1::{Ingress, IngressClass};
use kube::runtime::reflector::{store, Store};
use kube::runtime::{watcher, WatchStreamExt};
use kube::{Api, Client, Resource, ResourceExt};
use ramjet_router::{BuildError, RouteTableBuilder};
use serde::de::DeserializeOwned;
use tokio::sync::{mpsc, watch as watch_channel};
use tokio::task::JoinHandle;
use tracing::{debug, error, info, info_span, warn, Instrument};

use crate::annotations::ANNOTATION_OBSERVED_GENERATION;
use crate::config::{CompiledConfig, ControllerOpts};
use crate::diagnostics::WarningEvents;
use crate::snapshot::ClusterSnapshot;
use crate::status::StatusWriter;
use crate::tls::TLS_SECRET_TYPE;
use crate::translate::{translate, ObjectKey, WarningKind};

/// Starts the controller.
///
/// Returns a receiver that yields every published [`CompiledConfig`] and the
/// handle of the task doing the work. Dropping — or aborting — the handle stops
/// every watch: the reflectors and the rebuild loop all live inside that one
/// task, so there is a single place to cancel.
///
/// The receiver's initial value is an empty configuration, published before the
/// first list completes. Consumers that must not serve an empty table should
/// wait for `table.generation()` to advance past zero.
///
/// # Errors
///
/// Only if the router refuses to build the empty seed table, which no
/// [`BuildError`] variant can describe — every one of them reports a conflict
/// between inputs, and there are none. It is surfaced rather than unwrapped so
/// that this crate contains no panic sites at all.
pub fn spawn(
    client: Client,
    opts: ControllerOpts,
) -> Result<(watch_channel::Receiver<Arc<CompiledConfig>>, JoinHandle<()>), BuildError> {
    let seed = Arc::new(CompiledConfig {
        table: Arc::new(RouteTableBuilder::new().build()?),
        certs: Vec::new(),
        promotions: Vec::new(),
        digest: 0,
    });
    let (tx, rx) = watch_channel::channel(seed);
    let handle = tokio::spawn(run(client, opts, tx));
    Ok((rx, handle))
}

/// Why the rebuild loop woke up.
#[derive(Clone)]
struct Signals {
    /// One slot. A full channel already means "rebuild pending", so a dropped
    /// send loses nothing.
    dirty: mpsc::Sender<()>,
    /// Set when a watch restarted, so the status writer re-asserts itself: its
    /// cache describes the API server as it was before the disconnect.
    resynced: Arc<AtomicBool>,
}

/// The five mirrors the translator reads.
struct Stores {
    ingresses: Store<Ingress>,
    classes: Store<IngressClass>,
    services: Store<Service>,
    slices: Store<EndpointSlice>,
    secrets: Store<Secret>,
}

impl Stores {
    fn snapshot(&self) -> ClusterSnapshot {
        ClusterSnapshot {
            ingresses: self.ingresses.state(),
            ingress_classes: self.classes.state(),
            services: self.services.state(),
            endpoint_slices: self.slices.state(),
            secrets: self.secrets.state(),
        }
    }

    /// Blocks until every reflector has finished its initial list.
    ///
    /// Without this the first rebuild would compile whatever had arrived so
    /// far, which for a large cluster is a table missing most of its routes —
    /// published, served, and then corrected a moment later. A visible outage
    /// in exchange for starting a second sooner.
    async fn ready(&self) -> bool {
        let all = futures::future::join_all(vec![
            self.ingresses.wait_until_ready().boxed(),
            self.classes.wait_until_ready().boxed(),
            self.services.wait_until_ready().boxed(),
            self.slices.wait_until_ready().boxed(),
            self.secrets.wait_until_ready().boxed(),
        ])
        .await;
        all.into_iter().all(|r| r.is_ok())
    }
}

async fn run(
    client: Client,
    opts: ControllerOpts,
    tx: watch_channel::Sender<Arc<CompiledConfig>>,
) {
    let (dirty_tx, dirty_rx) = mpsc::channel::<()>(1);
    let signals = Signals {
        dirty: dirty_tx,
        resynced: Arc::new(AtomicBool::new(false)),
    };

    let (ingresses, ingress_writer) = store::<Ingress>();
    let (classes, class_writer) = store::<IngressClass>();
    let (services, service_writer) = store::<Service>();
    let (slices, slice_writer) = store::<EndpointSlice>();
    let (secrets, secret_writer) = store::<Secret>();

    let stores = Stores {
        ingresses,
        classes,
        services,
        slices,
        secrets,
    };

    let resynced = Arc::clone(&signals.resynced);
    let scoped = watcher::Config::default();
    // Every TLS Secret in the cluster is a lot of bytes to mirror for the sake
    // of the handful an Ingress references. `type` is a supported field
    // selector for Secrets, so the filter runs on the API server.
    let tls_only = watcher::Config::default().fields(&format!("type={TLS_SECRET_TYPE}"));

    let watchers = futures::future::join_all(vec![
        feed(
            namespaced(&client, opts.namespace.as_deref()),
            scoped.clone(),
            ingress_writer,
            signals.clone(),
            "Ingress",
        )
        .boxed(),
        feed(
            Api::<IngressClass>::all(client.clone()),
            scoped.clone(),
            class_writer,
            signals.clone(),
            "IngressClass",
        )
        .boxed(),
        feed(
            namespaced(&client, opts.namespace.as_deref()),
            scoped.clone(),
            service_writer,
            signals.clone(),
            "Service",
        )
        .boxed(),
        feed(
            namespaced(&client, opts.namespace.as_deref()),
            scoped,
            slice_writer,
            signals.clone(),
            "EndpointSlice",
        )
        .boxed(),
        feed(
            namespaced(&client, opts.namespace.as_deref()),
            tls_only,
            secret_writer,
            signals,
            "Secret",
        )
        .boxed(),
    ]);

    let rebuilds = rebuild_loop(client, opts, tx, stores, dirty_rx, resynced);

    // Neither side terminates on its own; whichever stops first takes the other
    // with it, so aborting the returned handle really does stop everything.
    tokio::select! {
        _ = watchers => warn!("all watchers ended; controller stopping"),
        () = rebuilds => warn!("rebuild loop ended; controller stopping"),
    }
}

/// A namespaced or cluster-wide `Api`, depending on the configured scope.
fn namespaced<K>(client: &Client, namespace: Option<&str>) -> Api<K>
where
    K: Resource<DynamicType = (), Scope = k8s_openapi::NamespaceResourceScope>,
{
    match namespace {
        Some(namespace) => Api::namespaced(client.clone(), namespace),
        None => Api::all(client.clone()),
    }
}

/// Drives one watch into its reflector, poking the rebuild loop as it goes.
fn feed<K>(
    api: Api<K>,
    config: watcher::Config,
    writer: store::Writer<K>,
    signals: Signals,
    kind: &'static str,
) -> impl std::future::Future<Output = ()> + Send
where
    K: Resource<DynamicType = ()> + Clone + DeserializeOwned + Debug + Send + Sync + 'static,
{
    watcher(api, config)
        // Resume where the watch left off; fall back to a full relist only when
        // the resource version has aged out of the API server's history.
        .default_backoff()
        // Managed-field bookkeeping is a large fraction of a mirrored object
        // and nothing here reads it.
        .modify(|object| object.managed_fields_mut().clear())
        .reflect(writer)
        .for_each(move |event| {
            match &event {
                Ok(watcher::Event::Init) => {
                    signals.resynced.store(true, Ordering::Relaxed);
                }
                Ok(_) => {}
                Err(err) => warn!(kind, error = %err, "watch error; backing off"),
            }
            if event.is_ok() {
                let _ = signals.dirty.try_send(());
            }
            futures::future::ready(())
        })
        .instrument(info_span!("watch", kind))
}

/// Compiles and publishes, once per debounce window.
async fn rebuild_loop(
    client: Client,
    opts: ControllerOpts,
    tx: watch_channel::Sender<Arc<CompiledConfig>>,
    stores: Stores,
    mut dirty: mpsc::Receiver<()>,
    resynced: Arc<AtomicBool>,
) {
    if !stores.ready().await {
        error!("a reflector stopped before its initial list completed");
        return;
    }
    info!("initial list complete");

    let mut warnings = WarningEvents::new(client.clone());
    let mut status = StatusWriter::new(client, &opts);
    // Seeded with the empty table the channel already holds, so the first real
    // publish lands at generation 1 and "generation 0" keeps its meaning:
    // nothing has been compiled yet.
    let mut current: Option<Arc<CompiledConfig>> = Some(tx.borrow().clone());
    let mut published_digest: Option<u64> = None;

    loop {
        if resynced.swap(false, Ordering::Relaxed) {
            // The watch restarted, so what we believe we wrote to each
            // Ingress's status — and what we believe we have already complained
            // about — may predate the gap. Re-assert both.
            if let Some(status) = &mut status {
                status.invalidate();
            }
            warnings.invalidate();
        }

        rebuild(
            &opts,
            &tx,
            &stores,
            &mut status,
            &mut warnings,
            &mut current,
            &mut published_digest,
        )
        .await;

        // Wait for the next change, then keep draining until the window closes.
        // A fixed deadline rather than a sliding one: sustained churn must not
        // postpone a rebuild indefinitely.
        if dirty.recv().await.is_none() {
            return;
        }
        let deadline = tokio::time::Instant::now() + opts.debounce;
        while let Ok(more) = tokio::time::timeout_at(deadline, dirty.recv()).await {
            if more.is_none() {
                return;
            }
        }
    }
}

async fn rebuild(
    opts: &ControllerOpts,
    tx: &watch_channel::Sender<Arc<CompiledConfig>>,
    stores: &Stores,
    status: &mut Option<StatusWriter>,
    warning_events: &mut WarningEvents,
    current: &mut Option<Arc<CompiledConfig>>,
    published_digest: &mut Option<u64>,
) {
    let started = Instant::now();
    let snapshot = stores.snapshot();
    let counts = snapshot.counts();

    let translation = translate(&snapshot, opts, current.as_ref().map(|c| c.table.as_ref()));
    let translation = match translation {
        Ok(translation) => translation,
        Err(err) => {
            // Unreachable unless the translator emitted something internally
            // inconsistent. Keep serving the last good table.
            error!(error = %err, "compiled configuration was rejected; keeping the previous table");
            return;
        }
    };

    for warning in &translation.warnings {
        // Endpoints go unready on every rolling update. Logging that at `warn`
        // would fire on every healthy deploy, and a warning that fires when
        // nothing is wrong teaches people to ignore the ones that matter.
        if warning.kind == WarningKind::EndpointsSkipped {
            debug!(subject = %warning.subject, "{}", warning.detail);
        } else {
            warn!(
                subject = %warning.subject,
                kind = ?warning.kind,
                "{}", warning.detail
            );
        }
    }

    // And the ones that name a value somebody wrote in an annotation also go on
    // the object, where its author can see them without pod-log access. Only
    // when the set changed; see `diagnostics`.
    warning_events.sync(&translation.warnings, &snapshot);

    if *published_digest == Some(translation.digest) {
        debug!(
            ingresses = counts.ingresses,
            warnings = translation.warnings.len(),
            elapsed_us = started.elapsed().as_micros() as u64,
            "no change"
        );
    } else {
        let config = Arc::new(translation.config);
        info!(
            generation = config.table.generation(),
            hosts = config.table.host_count() + config.table.wildcard_host_count(),
            routes = config.table.route_count(),
            backends = config.table.backends().len(),
            certs = config.certs.len(),
            ingresses = translation.managed.len(),
            watched_ingresses = counts.ingresses,
            services = counts.services,
            endpoint_slices = counts.endpoint_slices,
            secrets = counts.secrets,
            warnings = translation.warnings.len(),
            elapsed_us = started.elapsed().as_micros() as u64,
            "published configuration"
        );

        // A send with no receivers is not an error: the data plane may still be
        // starting, and it will read the latest value when it subscribes.
        let _ = tx.send(Arc::clone(&config));
        *current = Some(config);
        *published_digest = Some(translation.digest);
    }

    if let Some(status) = status {
        // The generation this controller has compiled, which is what the
        // annotation reports. Not what a replica is *serving* — a rollback pin
        // holds that back, and the pin lives in one data plane's memory where no
        // control plane can see it. `/admin/routes` answers the other question.
        let generation = current
            .as_ref()
            .map_or(0, |config| config.table.generation());
        status
            .sync(&translation.managed, generation, &observed_generations(&snapshot))
            .await;
    }
}

/// What each Ingress currently claims in `ramjet.dev/observed-generation`.
///
/// Built from the reflector store, so it costs a pass over the mirror rather
/// than a `GET` per object. Ingresses that carry no such annotation, or one that
/// is not a number, are simply absent — which reads as "needs writing", the safe
/// answer either way.
fn observed_generations(snapshot: &ClusterSnapshot) -> HashMap<ObjectKey, u64> {
    snapshot
        .ingresses
        .iter()
        .filter_map(|ingress| {
            ingress
                .annotations()
                .get(ANNOTATION_OBSERVED_GENERATION)
                .and_then(|value| value.trim().parse::<u64>().ok())
                .map(|generation| (ObjectKey::of(ingress.as_ref()), generation))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::translate::ObjectKey;

    /// The seed value the channel starts with must be a real, empty table
    /// rather than something the data plane could mistake for configuration.
    #[test]
    fn the_seed_table_is_empty_and_at_generation_zero() {
        let table = RouteTableBuilder::new().build().expect("an empty table builds");
        assert_eq!(table.generation(), 0);
        assert_eq!(table.route_count(), 0);
        assert!(table.match_request("example.com", "/").is_none());
    }

    /// `sync` compares against `managed` with `contains`, which is only correct
    /// if the translator hands over a list with no duplicates.
    #[test]
    fn object_keys_compare_by_value() {
        let a = ObjectKey {
            namespace: "default".to_owned(),
            name: "web".to_owned(),
        };
        let b = a.clone();
        assert_eq!(a, b);
        assert!([a].contains(&b));
    }
}
