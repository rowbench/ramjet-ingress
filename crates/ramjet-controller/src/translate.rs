//! The pure core: cluster objects in, [`CompiledConfig`] out.
//!
//! No I/O, no clock, no `kube::Client`. That constraint is what makes the
//! interesting behaviour — class filtering, path precedence, endpoint
//! resolution, canary merging, conflict arbitration — testable against structs
//! built in memory, and it is why the test module at the bottom of this file is
//! longer than the implementation above it.
//!
//! # Totality
//!
//! A rebuild never fails because of one bad object. Every rejection degrades a
//! single route, backend, or certificate and is recorded as a [`Warning`]; the
//! rest of the cluster compiles. The alternative — refusing to build a table
//! that contains one broken Ingress — hands any namespace owner a cluster-wide
//! kill switch.
//!
//! # Determinism
//!
//! The same snapshot must always produce the same table, byte for byte, or the
//! digest-based publish suppression in [`watch`](crate::spawn) would be
//! useless and every watch event would republish. So: Ingresses are processed
//! in `(creationTimestamp, namespace, name)` order, backends in sorted order,
//! routes in sorted order, and resolved addresses are sorted before they are
//! registered.

use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::sync::Arc;

use k8s_openapi::api::networking::v1::{Ingress, IngressBackend};
use kube::ResourceExt;
use ramjet_router::{
    BackendOptions, BackendProtocol, BuildError, CanaryRules, CertifiedKeyHandle, MirrorRules,
    PathType, RouteOptions, RouteTable, RouteTableBuilder,
};

use crate::annotations::{
    BackendProtocolAnnotation, CanaryAnnotations, MirrorAnnotations, PromotionAnnotations,
    ANNOTATION_BACKEND_PROTOCOL, ANNOTATION_MIRROR_BACKEND,
};
use crate::class::ClassFilter;
use crate::config::{
    BackendPort, CertMaterial, CompiledConfig, ControllerOpts, PromotionRoute, PromotionTarget,
    ServiceRef,
};
use crate::digest::Digest;
use crate::endpoints::{EndpointIndex, ResolveIssue};
use crate::snapshot::ClusterSnapshot;
use crate::tls::SecretIndex;

/// Namespace and name of a Kubernetes object.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ObjectKey {
    /// Namespace, or the empty string for a cluster-scoped object.
    pub namespace: String,
    /// Object name.
    pub name: String,
}

impl ObjectKey {
    /// Builds a key from any namespaced resource.
    pub fn of<K: kube::Resource>(object: &K) -> Self {
        ObjectKey {
            namespace: object.meta().namespace.clone().unwrap_or_default(),
            name: object.meta().name.clone().unwrap_or_default(),
        }
    }
}

impl fmt::Display for ObjectKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.namespace.is_empty() {
            f.write_str(&self.name)
        } else {
            write!(f, "{}/{}", self.namespace, self.name)
        }
    }
}

/// What kind of thing went wrong.
///
/// These are carried out of the pure translator rather than logged inside it,
/// so the caller decides whether they become log lines, Kubernetes Events, or
/// a metric. Today [`spawn`](crate::spawn) turns them into structured warnings.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum WarningKind {
    /// `spec.ingressClassName` names an `IngressClass` that does not exist.
    UnknownClass,
    /// Another Ingress already claimed this host and path.
    RouteConflict,
    /// The backend is not a Service, or names no port.
    UnsupportedBackend,
    /// The Service exists but could not be resolved to addresses.
    ServiceUnresolved,
    /// `spec.type: ExternalName`, which the data plane cannot follow yet.
    ExternalNameService,
    /// Some addresses were dropped for being unready or terminating.
    EndpointsSkipped,
    /// The router refused the host or path.
    InvalidRoute,
    /// An annotation value could not be parsed and was ignored.
    InvalidAnnotation,
    /// A canary Ingress found no production route to attach to.
    CanaryOrphan,
    /// Two canary Ingresses claimed the same production route.
    CanaryConflict,
    /// A canary is configured such that it can never divert a request.
    CanaryInert,
    /// A mirror could not be used and the route is served without it.
    MirrorRejected,
    /// A referenced TLS Secret is missing or unusable.
    TlsSecret,
    /// Two Ingresses supplied a certificate for the same host.
    TlsConflict,
    /// A `spec.tls` entry listed no hosts, which we cannot resolve without
    /// parsing the certificate. See [`crate::ControllerOpts::default_tls_secret`].
    TlsHostless,
    /// More than one Ingress supplied a cluster-wide default backend.
    DefaultBackendConflict,
    /// Two Ingresses asked for different `backend-protocol` on one Service port.
    BackendProtocolConflict,
}

/// One thing the translator refused, and why.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Warning {
    /// The object the operator should go and look at.
    pub subject: ObjectKey,
    /// What kind of problem it was.
    pub kind: WarningKind,
    /// Human-readable detail.
    pub detail: String,
}

impl Warning {
    fn new(subject: ObjectKey, kind: WarningKind, detail: impl Into<String>) -> Self {
        Warning {
            subject,
            kind,
            detail: detail.into(),
        }
    }
}

impl fmt::Display for Warning {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} [{:?}]: {}", self.subject, self.kind, self.detail)
    }
}

/// The result of one translation pass.
#[derive(Debug)]
pub struct Translation {
    /// What to publish.
    pub config: CompiledConfig,
    /// Content hash of everything in `config` except the generation number.
    ///
    /// Equal digests mean equal configuration, so the rebuild loop can skip a
    /// publish. Computed over the *plan* rather than the built table, because a
    /// `RouteTable` holds compiled regexes and `Arc`s that have no useful
    /// equality.
    pub digest: u64,
    /// Ingresses we are managing, sorted. The status writer needs exactly this.
    pub managed: Vec<ObjectKey>,
    /// Everything that was rejected or degraded.
    pub warnings: Vec<Warning>,
}

/// Identity of a route, normalised so that collisions are detected on exactly
/// the same key the router uses to reject duplicates.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct RouteKey {
    /// Lowercased host, `*.` retained for wildcards. `None` is the catch-all.
    host: Option<String>,
    path: String,
    path_type: PathType,
}

impl RouteKey {
    /// Total order for deterministic registration.
    fn sort_key(&self) -> (&str, bool, &str, u8) {
        (
            self.host.as_deref().unwrap_or(""),
            self.host.is_none(),
            &self.path,
            path_type_rank(self.path_type),
        )
    }
}

fn path_type_rank(t: PathType) -> u8 {
    match t {
        PathType::Exact => 0,
        PathType::Prefix => 1,
        PathType::ImplementationSpecific => 2,
    }
}

/// A route as planned, before the router validates it.
#[derive(Debug, Clone)]
struct RoutePlan {
    key: RouteKey,
    backend: ServiceRef,
    owner: ObjectKey,
    /// `backend-protocol` from the Ingress that declared this route.
    protocol: BackendProtocol,
    canary: Option<CanaryPlan>,
    mirror: Option<MirrorPlan>,
}

/// A mirror as planned.
#[derive(Debug, Clone)]
struct MirrorPlan {
    backend: ServiceRef,
    percent: u32,
    host: Option<String>,
    /// From the production Ingress, which is where the mirror annotations live.
    protocol: BackendProtocol,
}

/// A canary as planned.
#[derive(Debug, Clone)]
struct CanaryPlan {
    backend: ServiceRef,
    owner: ObjectKey,
    rules: CanaryAnnotations,
    /// From the *canary* Ingress. A canary is a separate object with its own
    /// annotations, so a gRPC rollout can put an h2c canary in front of an
    /// HTTP/1.1 production Service, or the reverse.
    protocol: BackendProtocol,
}

/// Compiles a snapshot of the cluster into a publishable configuration.
///
/// `previous` carries the load-balancer counters and the generation number
/// forward; pass the table currently published, or `None` for the first build.
///
/// # Errors
///
/// Returns [`BuildError`] only if the router rejects the assembled table as a
/// whole. Per-object problems never reach here — they come back as
/// [`Translation::warnings`] — so an error from this function means the
/// translator produced something internally inconsistent, which is a bug in
/// this crate rather than in the cluster.
pub fn translate(
    snapshot: &ClusterSnapshot,
    opts: &ControllerOpts,
    previous: Option<&RouteTable>,
) -> Result<Translation, BuildError> {
    let mut warnings = Vec::new();
    let mut digest = Digest::new();

    let managed = select_managed(snapshot, opts, &mut warnings);
    let (production, canaries): (Vec<_>, Vec<_>) = managed
        .iter()
        .map(|ing| (Arc::clone(ing), CanaryAnnotations::parse(ing.annotations())))
        .partition(|(_, canary)| !canary.enabled);

    let mut routes: HashMap<RouteKey, RoutePlan> = HashMap::new();
    let mut fallbacks: Vec<RoutePlan> = Vec::new();
    let mut global_default: Option<(ServiceRef, ObjectKey)> = None;

    for (ingress, _) in &production {
        plan_ingress(
            ingress,
            &mut routes,
            &mut fallbacks,
            &mut global_default,
            &mut warnings,
        );
    }

    // `defaultBackend` fallbacks are applied only after every explicit rule has
    // claimed its key, so a real rule always outranks one -- regardless of
    // which Ingress is older. A fallback that could shadow a declared path
    // would be a very confusing bug to chase.
    for plan in fallbacks {
        routes.entry(plan.key.clone()).or_insert(plan);
    }

    for (ingress, rules) in &canaries {
        attach_canary(ingress, rules, &mut routes, &mut warnings);
    }

    let mut builder = match previous {
        Some(table) => RouteTableBuilder::from_previous(table),
        None => RouteTableBuilder::new(),
    };

    let default_backend = global_default
        .map(|(r, _)| r)
        .or_else(|| opts.default_backend.clone());

    let backends = collect_backends(&routes, default_backend.as_ref(), &mut warnings);
    register_backends(
        snapshot,
        opts,
        &backends,
        &mut builder,
        &mut digest,
        &mut warnings,
    )?;

    register_routes(&routes, &mut builder, &mut digest, &mut warnings);

    if let Some(target) = &default_backend {
        builder.default_backend(&target.backend_name());
    }
    digest.opt_str(default_backend.as_ref().map(|r| r.backend_name()).as_deref());

    let certs = register_certificates(
        snapshot,
        opts,
        &managed,
        &mut builder,
        &mut digest,
        &mut warnings,
    );

    // Compiled last, from the same canary Ingresses already partitioned above,
    // so the promotion loop never has to read the API server itself. Folded
    // into the digest because editing an `auto-promote` annotation must
    // republish: the loop reads its policy off the published generation, and a
    // change nobody publishes is a change nobody applies.
    let promotions = collect_promotions(&canaries, &routes, &mut digest, &mut warnings);

    let content = digest.finish();
    Ok(Translation {
        config: CompiledConfig {
            table: Arc::new(builder.build()?),
            certs,
            promotions,
            digest: content,
        },
        digest: content,
        managed: managed.iter().map(|i| ObjectKey::of(i.as_ref())).collect(),
        warnings,
    })
}

/// Ingresses we manage, oldest first.
///
/// Age decides every conflict below, so the ordering is load-bearing rather
/// than cosmetic. Namespace and name break ties, because two objects created in
/// the same second must still compile to the same table on every replica.
fn select_managed(
    snapshot: &ClusterSnapshot,
    opts: &ControllerOpts,
    warnings: &mut Vec<Warning>,
) -> Vec<Arc<Ingress>> {
    let filter = ClassFilter::new(&snapshot.ingress_classes, &opts.class_name);
    let mut managed: Vec<Arc<Ingress>> = Vec::new();

    for ingress in &snapshot.ingresses {
        use crate::class::Claim;
        match filter.classify(ingress) {
            Claim::Ours(_) => managed.push(Arc::clone(ingress)),
            Claim::UnknownClass => warnings.push(Warning::new(
                ObjectKey::of(ingress.as_ref()),
                WarningKind::UnknownClass,
                format!(
                    "ingressClassName `{}` does not name any IngressClass",
                    ingress
                        .spec
                        .as_ref()
                        .and_then(|s| s.ingress_class_name.as_deref())
                        .unwrap_or_default()
                ),
            )),
            Claim::OtherController | Claim::NoClass => {}
        }
    }

    managed.sort_by(|a, b| {
        a.creation_timestamp()
            .cmp(&b.creation_timestamp())
            .then_with(|| ObjectKey::of(a.as_ref()).cmp(&ObjectKey::of(b.as_ref())))
    });
    managed
}

/// Turns one production Ingress into route plans.
fn plan_ingress(
    ingress: &Arc<Ingress>,
    routes: &mut HashMap<RouteKey, RoutePlan>,
    fallbacks: &mut Vec<RoutePlan>,
    global_default: &mut Option<(ServiceRef, ObjectKey)>,
    warnings: &mut Vec<Warning>,
) {
    let owner = ObjectKey::of(ingress.as_ref());
    let namespace = owner.namespace.clone();
    let Some(spec) = ingress.spec.as_ref() else {
        return;
    };

    // Parsed once per Ingress rather than per rule: the annotations are on the
    // object, so every route it declares shares one protocol and one mirror.
    let protocol = backend_protocol(ingress.annotations(), &owner, warnings);
    let mirror = mirror_plan(ingress.annotations(), &namespace, &owner, protocol, warnings);

    let mut hosts: Vec<Option<String>> = Vec::new();

    for rule in spec.rules.as_deref().unwrap_or_default() {
        let host = normalize_host(rule.host.as_deref());
        if !hosts.contains(&host) {
            hosts.push(host.clone());
        }

        for entry in rule
            .http
            .as_ref()
            .map(|h| h.paths.as_slice())
            .unwrap_or_default()
        {
            let Some(backend) = backend_ref(&namespace, &entry.backend, &owner, warnings) else {
                continue;
            };
            let path_type = parse_path_type(&entry.path_type, &owner, warnings);
            let key = RouteKey {
                host: host.clone(),
                path: entry.path.clone().unwrap_or_else(|| "/".to_owned()),
                path_type,
            };

            match routes.get(&key).map(|plan| plan.owner.to_string()) {
                Some(holder) => warnings.push(Warning::new(
                    owner.clone(),
                    WarningKind::RouteConflict,
                    format!(
                        "{} is already served by the older Ingress {holder}",
                        describe(&key)
                    ),
                )),
                None => {
                    routes.insert(
                        key.clone(),
                        RoutePlan {
                            key,
                            backend,
                            owner: owner.clone(),
                            protocol,
                            canary: None,
                            mirror: mirror.clone(),
                        },
                    );
                }
            }
        }
    }

    let Some(default) = spec.default_backend.as_ref() else {
        return;
    };
    let Some(backend) = backend_ref(&namespace, default, &owner, warnings) else {
        return;
    };

    if hosts.is_empty() {
        // An Ingress with a bare `defaultBackend` and no rules is asking to be
        // the cluster's catch-all, which is a singleton. Oldest wins, same as
        // every other conflict.
        match global_default.as_ref().map(|(_, holder)| holder.to_string()) {
            Some(holder) => warnings.push(Warning::new(
                owner,
                WarningKind::DefaultBackendConflict,
                format!("the default backend is already set by the older Ingress {holder}"),
            )),
            None => *global_default = Some((backend, owner)),
        }
        return;
    }

    for host in hosts {
        fallbacks.push(RoutePlan {
            key: RouteKey {
                host,
                path: "/".to_owned(),
                path_type: PathType::Prefix,
            },
            backend: backend.clone(),
            owner: owner.clone(),
            protocol,
            canary: None,
            mirror: mirror.clone(),
        });
    }
}

/// Merges one canary Ingress onto the production routes it shadows.
fn attach_canary(
    ingress: &Arc<Ingress>,
    rules: &CanaryAnnotations,
    routes: &mut HashMap<RouteKey, RoutePlan>,
    warnings: &mut Vec<Warning>,
) {
    let owner = ObjectKey::of(ingress.as_ref());
    let namespace = owner.namespace.clone();
    let protocol = backend_protocol(ingress.annotations(), &owner, warnings);

    for key in &rules.invalid {
        warnings.push(Warning::new(
            owner.clone(),
            WarningKind::InvalidAnnotation,
            format!("`{key}` is not a number and was ignored"),
        ));
    }
    if rules.is_inert() {
        warnings.push(Warning::new(
            owner.clone(),
            WarningKind::CanaryInert,
            "canary has no weight, header, or cookie, so it can never divert a request",
        ));
    }
    // Silently ignoring it would leave somebody watching a shadow backend that
    // never receives anything, with no way to find out why.
    if MirrorAnnotations::parse(ingress.annotations()).enabled() {
        warnings.push(Warning::new(
            owner.clone(),
            WarningKind::InvalidAnnotation,
            format!(
                "`{ANNOTATION_MIRROR_BACKEND}` on a canary Ingress does nothing; a mirror \
                 belongs to the route, so put it on the production Ingress"
            ),
        ));
    }

    let Some(spec) = ingress.spec.as_ref() else {
        return;
    };

    for rule in spec.rules.as_deref().unwrap_or_default() {
        let host = normalize_host(rule.host.as_deref());
        for entry in rule
            .http
            .as_ref()
            .map(|h| h.paths.as_slice())
            .unwrap_or_default()
        {
            let Some(backend) = backend_ref(&namespace, &entry.backend, &owner, warnings) else {
                continue;
            };
            let key = RouteKey {
                host: host.clone(),
                path: entry.path.clone().unwrap_or_else(|| "/".to_owned()),
                path_type: parse_path_type(&entry.path_type, &owner, warnings),
            };

            match routes.get_mut(&key) {
                None => warnings.push(Warning::new(
                    owner.clone(),
                    WarningKind::CanaryOrphan,
                    format!(
                        "no production Ingress serves {}, so this canary routes nothing",
                        describe(&key)
                    ),
                )),
                Some(plan) if plan.canary.is_some() => warnings.push(Warning::new(
                    owner.clone(),
                    WarningKind::CanaryConflict,
                    format!(
                        "{} already has a canary from the older Ingress {}",
                        describe(&key),
                        plan.canary
                            .as_ref()
                            .map(|c| c.owner.to_string())
                            .unwrap_or_default()
                    ),
                )),
                Some(plan) => {
                    plan.canary = Some(CanaryPlan {
                        backend,
                        owner: owner.clone(),
                        rules: rules.clone(),
                        protocol,
                    });
                }
            }
        }
    }
}

/// Every distinct Service port the table will reference, and how to dial it.
///
/// A backend is one Service port, and one Service port is one entry in the route
/// table however many Ingresses point at it — which means two Ingresses can ask
/// for two different `backend-protocol` values on the same pods. There is no
/// answer that satisfies both, so this resolves it the way every other conflict
/// in this file is resolved: **the first claim wins and the loser is named in a
/// warning**. First is defined by the route sort order, not by hash iteration
/// order, so every replica compiles the same table from the same objects.
///
/// The alternative — one backend per (Service port, protocol) pair — would give
/// each Ingress what it asked for, at the cost of two connection pools and two
/// sets of load-balancer counters for one set of pods, and of a backend name in
/// `/metrics` that no longer matches the Service. Silently splitting a Service's
/// traffic accounting in two because somebody annotated a second Ingress is a
/// worse surprise than a warning saying which annotation did not take.
fn collect_backends(
    routes: &HashMap<RouteKey, RoutePlan>,
    default_backend: Option<&ServiceRef>,
    warnings: &mut Vec<Warning>,
) -> BTreeMap<ServiceRef, BackendProtocol> {
    let mut map: BTreeMap<ServiceRef, BackendProtocol> = BTreeMap::new();

    let mut plans: Vec<&RoutePlan> = routes.values().collect();
    plans.sort_by(|a, b| a.key.sort_key().cmp(&b.key.sort_key()));

    for plan in plans {
        let mut claim = |target: &ServiceRef, protocol: BackendProtocol, owner: &ObjectKey| {
            match map.get(target) {
                None => {
                    map.insert(target.clone(), protocol);
                }
                Some(&held) if held != protocol => warnings.push(Warning::new(
                    owner.clone(),
                    WarningKind::BackendProtocolConflict,
                    format!(
                        "backend {target} is already registered as `{held}` by another Ingress; \
                         this Ingress asked for `{protocol}` and was not honoured"
                    ),
                )),
                Some(_) => {}
            }
        };

        claim(&plan.backend, plan.protocol, &plan.owner);
        if let Some(canary) = &plan.canary {
            claim(&canary.backend, canary.protocol, &canary.owner);
        }
        // A mirror backend is resolved exactly like any other, endpoints and
        // all. Skipping it here would leave the router with a route naming a
        // backend that was never registered, which fails the whole table.
        if let Some(mirror) = &plan.mirror {
            claim(&mirror.backend, mirror.protocol, &plan.owner);
        }
    }

    if let Some(target) = default_backend {
        // The cluster-wide default comes from a flag or from an Ingress with no
        // rules, and in neither case is there a route whose annotation could
        // have spoken for it. It stays HTTP/1.1 unless some route also names it,
        // in which case that route's claim is already in the map.
        map.entry(target.clone()).or_default();
    }
    map
}

/// Resolves and registers every backend.
///
/// A backend that cannot be resolved is registered *empty* rather than skipped.
/// The route stays in the table and the proxy answers 503, which is what
/// ingress-nginx does and the only behaviour that survives a rolling update:
/// dropping the route instead would make requests fall through to some
/// unrelated wildcard for the duration of the rollout.
fn register_backends(
    snapshot: &ClusterSnapshot,
    opts: &ControllerOpts,
    backends: &BTreeMap<ServiceRef, BackendProtocol>,
    builder: &mut RouteTableBuilder,
    digest: &mut Digest,
    warnings: &mut Vec<Warning>,
) -> Result<(), BuildError> {
    let index = EndpointIndex::new(&snapshot.services, &snapshot.endpoint_slices);

    for (target, protocol) in backends {
        let subject = ObjectKey {
            namespace: target.namespace.clone(),
            name: target.name.clone(),
        };
        let endpoints = match index.resolve(target) {
            Ok(resolution) => {
                if resolution.skipped > 0 {
                    warnings.push(Warning::new(
                        subject,
                        WarningKind::EndpointsSkipped,
                        format!(
                            "{} address(es) skipped as unready or terminating",
                            resolution.skipped
                        ),
                    ));
                }
                resolution.endpoints
            }
            Err(issue) => {
                let (kind, detail) = match issue {
                    ResolveIssue::ServiceMissing => (
                        WarningKind::ServiceUnresolved,
                        format!("Service {} does not exist; serving 503", target),
                    ),
                    ResolveIssue::PortMissing => (
                        WarningKind::ServiceUnresolved,
                        format!("Service has no port `{}`; serving 503", target.port),
                    ),
                    ResolveIssue::NoReadyEndpoints { skipped } => (
                        WarningKind::ServiceUnresolved,
                        format!("no ready endpoints ({skipped} skipped); serving 503"),
                    ),
                    ResolveIssue::ExternalName { target: name } => (
                        WarningKind::ExternalNameService,
                        // TODO: needs a resolver with TTL handling in the data
                        // plane before this can be served.
                        format!("ExternalName Services are not supported yet (`{name}`); serving 503"),
                    ),
                };
                warnings.push(Warning::new(subject, kind, detail));
                Vec::new()
            }
        };

        let name = target.backend_name();
        digest.str(&name);
        digest.u8(lb_policy_tag(opts.lb_policy));
        // Folded in so that flipping `backend-protocol` republishes. Without
        // this the digest would be equal across the change and the rebuild loop
        // would suppress the publish, leaving the annotation edited and the data
        // plane still dialling the old protocol.
        digest.u8(protocol_tag(*protocol));
        digest.u64(endpoints.len() as u64);
        for endpoint in &endpoints {
            digest.str(&endpoint.addr.to_string());
            digest.u64(u64::from(endpoint.weight));
        }

        builder.backend_with(
            &name,
            endpoints,
            &BackendOptions {
                policy: opts.lb_policy,
                protocol: *protocol,
            },
        )?;
    }
    Ok(())
}

/// Registers every planned route, dropping the ones the router refuses.
fn register_routes(
    routes: &HashMap<RouteKey, RoutePlan>,
    builder: &mut RouteTableBuilder,
    digest: &mut Digest,
    warnings: &mut Vec<Warning>,
) {
    let mut plans: Vec<&RoutePlan> = routes.values().collect();
    plans.sort_by(|a, b| a.key.sort_key().cmp(&b.key.sort_key()));

    for plan in plans {
        let host = plan.key.host.as_deref();
        let path = plan.key.path.as_str();
        let backend = plan.backend.backend_name();

        digest.opt_str(host);
        digest.str(path);
        digest.u8(path_type_rank(plan.key.path_type));
        digest.str(&backend);

        // The canary's backend name has to outlive the borrow inside
        // `CanaryRules`, so it is bound here rather than inside the branch.
        let canary_backend = plan
            .canary
            .as_ref()
            .map(|canary| canary.backend.backend_name());
        let mirror_backend = plan
            .mirror
            .as_ref()
            .map(|mirror| mirror.backend.backend_name());

        match (&plan.canary, &canary_backend) {
            (Some(canary), Some(name)) => {
                digest.u8(1);
                digest.str(name);
                digest.u64(u64::from(canary.rules.weight));
                digest.u64(u64::from(canary.rules.weight_total));
                digest.opt_str(canary.rules.header.as_deref());
                digest.opt_str(canary.rules.header_value.as_deref());
                digest.opt_str(canary.rules.header_pattern.as_deref());
                digest.opt_str(canary.rules.cookie.as_deref());
            }
            _ => {
                digest.u8(0);
            }
        }
        match (&plan.mirror, &mirror_backend) {
            (Some(mirror), Some(name)) => {
                digest.u8(1);
                digest.str(name);
                digest.u64(u64::from(mirror.percent));
                digest.opt_str(mirror.host.as_deref());
            }
            _ => {
                digest.u8(0);
            }
        }

        let canary_rules = plan.canary.as_ref().zip(canary_backend.as_deref()).map(
            |(canary, backend)| CanaryRules {
                backend,
                header: canary.rules.header.as_deref(),
                header_value: canary.rules.header_value.as_deref(),
                header_pattern: canary.rules.header_pattern.as_deref(),
                cookie: canary.rules.cookie.as_deref(),
                weight: canary.rules.weight,
                weight_total: canary.rules.weight_total,
            },
        );
        let mirror_rules =
            plan.mirror
                .as_ref()
                .zip(mirror_backend.as_deref())
                .map(|(mirror, backend)| MirrorRules {
                    backend,
                    percent: mirror.percent,
                    host: mirror.host.as_deref(),
                });

        let options = RouteOptions {
            canary: canary_rules,
            mirror: mirror_rules,
        };
        let mut outcome = builder.route_with(host, path, plan.key.path_type, &backend, &options);

        // A broken canary or mirror must not take the production route down
        // with it: that would turn a bad annotation on a side object into an
        // outage on the main one. Drop the optional parts, keep the route, and
        // say which one went.
        if let Err(err) = &outcome {
            if let Some(canary) = &plan.canary {
                if options.canary.is_some() {
                    warnings.push(Warning::new(
                        canary.owner.clone(),
                        WarningKind::InvalidRoute,
                        format!(
                            "canary rejected ({err}); serving {} without it",
                            describe(&plan.key)
                        ),
                    ));
                }
            }
            if plan.mirror.is_some() && options.mirror.is_some() {
                warnings.push(Warning::new(
                    plan.owner.clone(),
                    WarningKind::MirrorRejected,
                    format!(
                        "mirror rejected ({err}); serving {} without it",
                        describe(&plan.key)
                    ),
                ));
            }
            outcome = builder.route(host, path, plan.key.path_type, &backend);
        }

        if let Err(err) = outcome {
            warnings.push(Warning::new(
                plan.owner.clone(),
                WarningKind::InvalidRoute,
                format!("{} rejected: {err}", describe(&plan.key)),
            ));
        }
    }
}

/// Wires `spec.tls` entries into the SNI map and collects their material.
fn register_certificates(
    snapshot: &ClusterSnapshot,
    opts: &ControllerOpts,
    managed: &[Arc<Ingress>],
    builder: &mut RouteTableBuilder,
    digest: &mut Digest,
    warnings: &mut Vec<Warning>,
) -> Vec<CertMaterial> {
    let secrets = SecretIndex::new(&snapshot.secrets);
    let mut by_host: BTreeMap<String, (u64, ObjectKey)> = BTreeMap::new();
    let mut materials: BTreeMap<u64, CertMaterial> = BTreeMap::new();

    for ingress in managed {
        let owner = ObjectKey::of(ingress.as_ref());
        let namespace = owner.namespace.clone();
        let Some(spec) = ingress.spec.as_ref() else {
            continue;
        };

        for entry in spec.tls.as_deref().unwrap_or_default() {
            let hosts: Vec<String> = entry
                .hosts
                .as_deref()
                .unwrap_or_default()
                .iter()
                .filter_map(|h| normalize_host(Some(h)))
                .collect();
            if hosts.is_empty() {
                warnings.push(Warning::new(
                    owner.clone(),
                    WarningKind::TlsHostless,
                    "spec.tls entry lists no hosts; set ControllerOpts::default_tls_secret \
                     instead, since the controller does not parse certificate SANs",
                ));
                continue;
            }

            let Some(secret_name) = entry.secret_name.as_deref() else {
                warnings.push(Warning::new(
                    owner.clone(),
                    WarningKind::TlsSecret,
                    "spec.tls entry has no secretName",
                ));
                continue;
            };

            let material = match secrets.material(&namespace, secret_name) {
                Ok(material) => material,
                Err(issue) => {
                    warnings.push(Warning::new(
                        owner.clone(),
                        WarningKind::TlsSecret,
                        format!("{namespace}/{secret_name}: {issue}; serving these hosts without it"),
                    ));
                    continue;
                }
            };

            for host in hosts {
                match by_host.get(&host).map(|(_, holder)| holder.to_string()) {
                    Some(holder) => warnings.push(Warning::new(
                        owner.clone(),
                        WarningKind::TlsConflict,
                        format!("`{host}` already has a certificate from the older Ingress {holder}"),
                    )),
                    None => {
                        by_host.insert(host, (material.handle_id, owner.clone()));
                    }
                }
            }
            materials.insert(material.handle_id, material);
        }
    }

    // One `Arc` per certificate, shared by every host it serves.
    let mut handles: HashMap<u64, Arc<CertifiedKeyHandle>> = HashMap::new();
    for (host, (handle_id, owner)) in &by_host {
        let handle = handles
            .entry(*handle_id)
            .or_insert_with(|| Arc::new(CertifiedKeyHandle::new(*handle_id)))
            .clone();
        digest.str(host);
        digest.u64(*handle_id);
        if let Err(err) = builder.certificate(host, handle) {
            warnings.push(Warning::new(
                owner.clone(),
                WarningKind::InvalidRoute,
                format!("TLS host `{host}` rejected: {err}"),
            ));
        }
    }

    if let Some(reference) = opts.default_tls_secret.as_deref() {
        match reference.split_once('/') {
            Some((namespace, name)) => match secrets.material(namespace, name) {
                Ok(material) => {
                    builder.default_certificate(Arc::new(CertifiedKeyHandle::new(
                        material.handle_id,
                    )));
                    digest.u64(material.handle_id);
                    materials.insert(material.handle_id, material);
                }
                Err(issue) => warnings.push(Warning::new(
                    ObjectKey {
                        namespace: namespace.to_owned(),
                        name: name.to_owned(),
                    },
                    WarningKind::TlsSecret,
                    format!("default TLS secret unusable: {issue}"),
                )),
            },
            None => warnings.push(Warning::new(
                ObjectKey {
                    namespace: String::new(),
                    name: reference.to_owned(),
                },
                WarningKind::TlsSecret,
                "default TLS secret must be `namespace/name`",
            )),
        }
    }

    materials.into_values().collect()
}

/// Normalises a host the same way the router's builder does, so a collision we
/// detect here is exactly a collision it would have rejected.
///
/// Returns `None` for an absent or empty host, which is the catch-all rule.
fn normalize_host(host: Option<&str>) -> Option<String> {
    let host = host?.trim().trim_end_matches('.');
    if host.is_empty() {
        return None;
    }
    Some(host.to_ascii_lowercase())
}

/// Gathers the canary Ingresses that asked to be promoted automatically.
///
/// A canary is a candidate only if it actually attached to a production route:
/// `routes` is consulted so that an orphaned canary — one whose host and path
/// nothing serves — is not handed to a loop that would then look for counters
/// belonging to a route that does not exist.
///
/// Every rejection here is silent by design except the flap guard, which says
/// so once. A canary this controller already rolled back is *supposed* to sit
/// there untouched until a human looks at it, and warning about that every
/// rebuild would turn the audit trail into noise at exactly the wrong moment.
fn collect_promotions(
    canaries: &[(Arc<Ingress>, CanaryAnnotations)],
    routes: &HashMap<RouteKey, RoutePlan>,
    digest: &mut Digest,
    warnings: &mut Vec<Warning>,
) -> Vec<PromotionTarget> {
    let mut targets: Vec<PromotionTarget> = Vec::new();

    for (ingress, canary) in canaries {
        let policy = PromotionAnnotations::parse(ingress.annotations());
        if !policy.enabled {
            continue;
        }
        let owner = ObjectKey::of(ingress.as_ref());
        for key in &policy.invalid {
            warnings.push(Warning::new(
                owner.clone(),
                WarningKind::InvalidAnnotation,
                format!("`{key}` could not be read; automatic promotion is using its default"),
            ));
        }

        // The routes this canary actually landed on, in the spelling the
        // compiled table uses.
        let mut covered: Vec<PromotionRoute> = routes
            .values()
            .filter(|plan| {
                plan.canary
                    .as_ref()
                    .is_some_and(|attached| attached.owner == owner)
            })
            .map(|plan| PromotionRoute {
                host: plan.key.host.clone().unwrap_or_else(|| "*".to_owned()),
                path: plan.key.path.clone(),
                path_type: plan.key.path_type,
            })
            .collect();
        if covered.is_empty() {
            continue;
        }
        // Sorted, because the map above is a `HashMap` and this list reaches
        // the digest.
        covered.sort_by(|a, b| {
            (&a.host, &a.path, path_type_rank(a.path_type))
                .cmp(&(&b.host, &b.path, path_type_rank(b.path_type)))
        });

        digest.str(&owner.to_string());
        digest.u64(u64::from(canary.weight));
        digest.u64(policy.interval.as_secs());
        digest.u64(policy.min_requests);
        digest.str(&format!("{:?}", policy.steps));
        digest.str(&format!("{:.6}", policy.max_5xx_percent));
        digest.str(&format!("{:.6}", policy.max_latency_factor));
        digest.opt_str(policy.status.as_deref());
        for route in &covered {
            digest.str(&route.host);
            digest.str(&route.path);
            digest.u8(path_type_rank(route.path_type));
        }

        targets.push(PromotionTarget {
            ingress: owner,
            routes: covered,
            weight: canary.weight,
            policy,
        });
    }

    // Deterministic order, so two replicas hand their loops the same list and
    // the digest is a function of content rather than of iteration.
    targets.sort_by(|a, b| a.ingress.cmp(&b.ingress));
    targets
}

/// Reads the mirror annotations off one Ingress into a plan.
///
/// A mirror that cannot be understood degrades to no mirror, with a warning.
/// The alternative — refusing the Ingress — would let a typo in a shadow
/// backend name take production traffic down, which is the exact inversion of
/// what a mirror is for.
fn mirror_plan(
    annotations: &BTreeMap<String, String>,
    namespace: &str,
    owner: &ObjectKey,
    protocol: BackendProtocol,
    warnings: &mut Vec<Warning>,
) -> Option<MirrorPlan> {
    let parsed = MirrorAnnotations::parse(annotations);
    for key in &parsed.invalid {
        warnings.push(Warning::new(
            owner.clone(),
            WarningKind::InvalidAnnotation,
            format!("`{key}` is not a percentage in 0-100 and was ignored"),
        ));
    }

    let reference = parsed.backend.as_deref()?;
    // `namespace/service:port`, and a bare `service:port` resolved against the
    // Ingress's own namespace — because a shadow of a service usually lives
    // beside it, and making people write their own namespace out is a papercut.
    let qualified = if reference.contains('/') {
        reference.to_owned()
    } else {
        format!("{namespace}/{reference}")
    };
    match qualified.parse::<ServiceRef>() {
        Ok(backend) => Some(MirrorPlan {
            backend,
            percent: parsed.percent,
            host: parsed.host,
            protocol,
        }),
        Err(error) => {
            warnings.push(Warning::new(
                owner.clone(),
                WarningKind::MirrorRejected,
                format!("`{ANNOTATION_MIRROR_BACKEND}: {reference}` is unusable ({error}); serving without a mirror"),
            ));
            None
        }
    }
}

/// Reads `backend-protocol` off one Ingress, reporting a value we cannot honour.
///
/// The warning is the whole point of not simply defaulting: `GRPCS` and `HTTPS`
/// are requests for TLS to the upstream, and an operator who wrote one and got
/// cleartext deserves to be told rather than left reading connection resets.
fn backend_protocol(
    annotations: &BTreeMap<String, String>,
    owner: &ObjectKey,
    warnings: &mut Vec<Warning>,
) -> BackendProtocol {
    let parsed = BackendProtocolAnnotation::parse(annotations);
    if let Some(value) = &parsed.unsupported {
        warnings.push(Warning::new(
            owner.clone(),
            WarningKind::InvalidAnnotation,
            format!(
                "`{ANNOTATION_BACKEND_PROTOCOL}: {value}` is not supported; only `HTTP` and \
                 `GRPC` are, and this backend stays on HTTP/1.1"
            ),
        ));
    }
    parsed.protocol
}

/// Reads a backend into a `ServiceRef`.
fn backend_ref(
    namespace: &str,
    backend: &IngressBackend,
    owner: &ObjectKey,
    warnings: &mut Vec<Warning>,
) -> Option<ServiceRef> {
    let Some(service) = backend.service.as_ref() else {
        warnings.push(Warning::new(
            owner.clone(),
            WarningKind::UnsupportedBackend,
            "`backend.resource` is not supported; only Service backends are routable",
        ));
        return None;
    };

    // The API forbids setting both, and a number is the unambiguous one.
    let port = match service.port.as_ref() {
        Some(port) if port.number.is_some() => BackendPort::Number(port.number.unwrap_or_default()),
        Some(port) if port.name.is_some() => {
            BackendPort::Name(port.name.clone().unwrap_or_default())
        }
        _ => {
            warnings.push(Warning::new(
                owner.clone(),
                WarningKind::UnsupportedBackend,
                format!("backend Service `{}` names no port", service.name),
            ));
            return None;
        }
    };

    Some(ServiceRef {
        namespace: namespace.to_owned(),
        name: service.name.clone(),
        port,
    })
}

/// Maps the `pathType` string onto the router's enum.
///
/// An unrecognised value is treated as `ImplementationSpecific`, which is what
/// the field name promises when a controller does not know better. API
/// validation should make this unreachable; it is here because "should" is not
/// a guarantee we can route traffic on.
fn parse_path_type(raw: &str, owner: &ObjectKey, warnings: &mut Vec<Warning>) -> PathType {
    match raw {
        "Exact" => PathType::Exact,
        "Prefix" => PathType::Prefix,
        "ImplementationSpecific" => PathType::ImplementationSpecific,
        other => {
            warnings.push(Warning::new(
                owner.clone(),
                WarningKind::InvalidAnnotation,
                format!("unknown pathType `{other}`, treating it as ImplementationSpecific"),
            ));
            PathType::ImplementationSpecific
        }
    }
}

fn protocol_tag(protocol: BackendProtocol) -> u8 {
    match protocol {
        BackendProtocol::Http1 => 0,
        BackendProtocol::H2c => 1,
    }
}

fn lb_policy_tag(policy: ramjet_router::LbPolicy) -> u8 {
    match policy {
        ramjet_router::LbPolicy::RoundRobin => 0,
        ramjet_router::LbPolicy::Random => 1,
        ramjet_router::LbPolicy::LeastConn => 2,
    }
}

/// `host/path (PathType)`, for a diagnostic an operator can act on.
fn describe(key: &RouteKey) -> String {
    format!(
        "{}{} ({:?})",
        key.host.as_deref().unwrap_or("*"),
        key.path,
        key.path_type
    )
}

#[cfg(test)]
pub(crate) mod test_support;

#[cfg(test)]
mod tests;
