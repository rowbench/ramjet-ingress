//! `ramjet-controller` is the ramjet-ingress control plane.
//!
//! It watches the Kubernetes API for `Ingress`, `IngressClass`, `Service`,
//! `EndpointSlice`, and TLS `Secret` objects, compiles what it observes into an
//! immutable [`ramjet_router::RouteTable`], and publishes the result to the
//! data plane. That is the whole job.
//!
//! # The anti-reload thesis
//!
//! ingress-nginx reacts to a configuration change by regenerating
//! `nginx.conf` and reloading: new workers start, old ones drain, upstream
//! state resets, and long-lived connections die. The cost scales with how much
//! traffic you are carrying, which is exactly the wrong way round.
//!
//! Here a change compiles a new [`CompiledConfig`] and stores one pointer. The
//! data plane's next `load()` sees it. In-flight requests finish against the
//! snapshot they started on, backends keep their in-flight accounting (see
//! [`RouteTableBuilder::from_previous`](ramjet_router::RouteTableBuilder::from_previous)),
//! and nothing drains.
//!
//! # Single writer, pure translate
//!
//! Exactly one task rebuilds. Watch events from every kind funnel into one
//! coalescing channel; after a short debounce the rebuild task takes a
//! [`ClusterSnapshot`] of the reflector stores and calls [`translate`], which
//! is a **pure function** — no I/O, no clock, no cluster. Every interesting
//! rule in this crate (class filtering, path semantics, endpoint resolution,
//! canary merging, conflict resolution) therefore has a unit test that
//! constructs objects in memory and asserts on the compiled table.
//!
//! Rebuilds are also *total*: one malformed Ingress, one dangling Secret, or
//! one unresolvable Service degrades that route and nothing else. A single
//! tenant cannot blank the cluster's routing table. Everything rejected comes
//! back as a structured [`Warning`].
//!
//! # The layering split
//!
//! This crate holds **no rustls types**. TLS material leaves here as raw PEM in
//! [`CertMaterial`], tagged with a [`handle_id`](CertMaterial::handle_id) that
//! matches the [`CertifiedKeyHandle`](ramjet_router::CertifiedKeyHandle) the
//! table's `SniMap` points at. The binary parses the PEM into a
//! `rustls::sign::CertifiedKey` and indexes it by that id. Handle ids are
//! content-derived, so an id changes if and only if the certificate bytes
//! change — the proxy can keep a parsed key cached across rebuilds and re-parse
//! only what actually moved.
//!
//! (The `kube` client does link a TLS stack in order to talk to the API server.
//! That is a transport detail of the watch connection and never touches the
//! data plane's certificate handling.)
//!
//! # Usage
//!
//! ```no_run
//! # async fn run() -> Result<(), Box<dyn std::error::Error>> {
//! let client = kube::Client::try_default().await?;
//! let opts = ramjet_controller::ControllerOpts {
//!     publish_address: Some("203.0.113.10".to_owned()),
//!     ..Default::default()
//! };
//! let (mut configs, _task) = ramjet_controller::spawn(client, opts)?;
//!
//! while configs.changed().await.is_ok() {
//!     let config = configs.borrow_and_update().clone();
//!     // parse `config.certs` into rustls keys, then publish `config.table`.
//!     println!("generation {}", config.table.generation());
//! }
//! # Ok(())
//! # }
//! ```

#![forbid(unsafe_code)]
#![deny(missing_docs)]

mod annotate;
mod annotations;
mod audit;
mod class;
mod config;
mod diagnostics;
mod diff;
mod digest;
mod endpoints;
mod snapshot;
mod status;
mod tls;
mod translate;
mod watch;

pub use annotate::patch_ingress_annotations;
pub use annotations::{
    CanaryAnnotations, MirrorAnnotations, PromotionAnnotations, ANNOTATION_AUTO_PROMOTE,
    ANNOTATION_AUTO_PROMOTE_INTERVAL, ANNOTATION_AUTO_PROMOTE_MAX_5XX,
    ANNOTATION_AUTO_PROMOTE_MAX_LATENCY, ANNOTATION_AUTO_PROMOTE_MIN_REQUESTS,
    ANNOTATION_AUTO_PROMOTE_STATUS, ANNOTATION_AUTO_PROMOTE_STEPS, ANNOTATION_CANARY,
    ANNOTATION_CANARY_BY_COOKIE, ANNOTATION_CANARY_BY_HEADER,
    ANNOTATION_CANARY_BY_HEADER_PATTERN, ANNOTATION_CANARY_BY_HEADER_VALUE,
    ANNOTATION_CANARY_WEIGHT, ANNOTATION_CANARY_WEIGHT_TOTAL, ANNOTATION_IS_DEFAULT_CLASS,
    ANNOTATION_LEGACY_CLASS, ANNOTATION_MIRROR_BACKEND, ANNOTATION_MIRROR_HOST,
    ANNOTATION_MIRROR_PERCENT, ANNOTATION_OBSERVED_GENERATION, DEFAULT_MIRROR_PERCENT,
    DEFAULT_PROMOTE_INTERVAL,
    DEFAULT_PROMOTE_MAX_5XX_PERCENT, DEFAULT_PROMOTE_MAX_LATENCY_FACTOR,
    DEFAULT_PROMOTE_MIN_REQUESTS, DEFAULT_PROMOTE_STEPS, STATUS_PROMOTED, STATUS_ROLLED_BACK,
};
pub use audit::{AuditReason, AuditSink, CanaryDecision, EventSubject, WebhookError};
pub use class::ClassFilter;
pub use config::{
    BackendPort, CertMaterial, CompiledConfig, ControllerOpts, PromotionRoute, PromotionTarget,
    ServiceRef, ServiceRefError, CONTROLLER_NAME, FIELD_MANAGER,
};
pub use diff::ConfigDiff;
pub use snapshot::ClusterSnapshot;
pub use translate::{translate, ObjectKey, Translation, Warning, WarningKind};
pub use watch::spawn;
