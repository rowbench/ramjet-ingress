//! Construction and validation of a [`RouteTable`].
//!
//! Everything expensive happens here, once per configuration change, so that
//! the request path is left with nothing to do but compare bytes: hosts are
//! lowercased, wildcards are rewritten to their parent domain, prefix match
//! lengths are precomputed, regexes are compiled, weighted rotations are
//! expanded, and rules are sorted into their final precedence order.
//!
//! Backends may be registered in any order relative to the routes that
//! reference them; names are resolved to [`BackendId`]s in [`build`](
//! RouteTableBuilder::build).

use std::sync::Arc;

use regex::{Regex, RegexBuilder};

use crate::backend::{Backend, BackendId, Endpoint, LbPolicy};
use crate::canary::{CanarySpec, HeaderSpec};
use crate::host::{FxHashMap, FxHashSet, MAX_HOST_LEN};
use crate::path::{PathRule, PathType};
use crate::stats::{BackendStats, RouteIdentity, RouteStats};
use crate::table::{RouteTable, VirtualHost};
use crate::tls::{CertifiedKeyHandle, SniMap};

/// Largest compiled regex we will accept, in bytes. A pathological
/// `ImplementationSpecific` path should fail validation, not quietly consume a
/// hundred megabytes in every ingress replica.
const REGEX_SIZE_LIMIT: usize = 1 << 20;

/// Default denominator for canary weights, matching ingress-nginx.
const DEFAULT_WEIGHT_TOTAL: u32 = 100;

/// Why a route table could not be built.
#[derive(Debug, thiserror::Error)]
pub enum BuildError {
    /// A host name was empty or longer than 253 bytes.
    #[error("host `{host}` is empty or longer than {MAX_HOST_LEN} bytes")]
    HostLength {
        /// The offending name.
        host: String,
    },

    /// A host contained a port, a scheme, or another character that cannot
    /// appear in an Ingress `host` field.
    #[error("host `{host}` contains `{found}`, which is not valid in an Ingress host")]
    HostSyntax {
        /// The offending name.
        host: String,
        /// The character that rejected it.
        found: char,
    },

    /// A wildcard host was not of the form `*.example.com`.
    #[error("host `{host}` is not a valid wildcard: exactly one leading `*.` label is supported")]
    WildcardShape {
        /// The offending name.
        host: String,
    },

    /// An `Exact` or `Prefix` path did not start with `/`.
    #[error("path `{path}` must begin with `/`")]
    PathNotAbsolute {
        /// The offending path.
        path: String,
    },

    /// An `ImplementationSpecific` path was not a valid regular expression.
    #[error("invalid ImplementationSpecific path `{path}`: {source}")]
    BadPattern {
        /// The offending pattern.
        path: String,
        /// The underlying regex error.
        #[source]
        source: Box<regex::Error>,
    },

    /// A canary header pattern was not a valid regular expression.
    #[error("invalid canary header pattern `{pattern}`: {source}")]
    BadCanaryPattern {
        /// The offending pattern.
        pattern: String,
        /// The underlying regex error.
        #[source]
        source: Box<regex::Error>,
    },

    /// Two rules on one host declared the same path with the same path type.
    #[error("duplicate {path_type:?} path `{path}` on host `{host}`")]
    DuplicatePath {
        /// The host the collision occurred on.
        host: String,
        /// The colliding path.
        path: String,
        /// The path type both rules used.
        path_type: PathType,
    },

    /// A backend name was registered twice.
    #[error("backend `{name}` was registered more than once")]
    DuplicateBackend {
        /// The offending name.
        name: String,
    },

    /// A route, canary, or default backend named a backend that was never
    /// registered.
    #[error("`{referrer}` references backend `{name}`, which was never registered")]
    UnknownBackend {
        /// The backend name that could not be resolved.
        name: String,
        /// What referred to it, for locating the offending Ingress.
        referrer: String,
    },

    /// A canary set both `canary-by-header-value` and
    /// `canary-by-header-pattern`, which ingress-nginx treats as mutually
    /// exclusive.
    #[error("canary on `{host}{path}` sets both a header value and a header pattern")]
    CanaryHeaderConflict {
        /// The host the canary is attached to.
        host: String,
        /// The path the canary is attached to.
        path: String,
    },

    /// A canary weight exceeded its total.
    #[error("canary weight {weight} exceeds total {total}")]
    CanaryWeight {
        /// The configured weight.
        weight: u32,
        /// The configured total.
        total: u32,
    },
}

/// The ingress-nginx canary annotations for one route.
///
/// Field names mirror the annotation suffixes so the controller's translation
/// layer stays a transcription rather than an interpretation.
#[derive(Debug, Clone, Copy, Default)]
pub struct CanaryRules<'a> {
    /// Backend that canaried traffic goes to. Required.
    pub backend: &'a str,
    /// `canary-by-header`.
    pub header: Option<&'a str>,
    /// `canary-by-header-value`. Mutually exclusive with `header_pattern`.
    pub header_value: Option<&'a str>,
    /// `canary-by-header-pattern`. Mutually exclusive with `header_value`.
    pub header_pattern: Option<&'a str>,
    /// `canary-by-cookie`.
    pub cookie: Option<&'a str>,
    /// `canary-weight`.
    pub weight: u32,
    /// `canary-weight-total`; `0` means the default of 100.
    pub weight_total: u32,
}

/// Which bucket a rule belongs to.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum HostSlot {
    Exact(Box<str>),
    /// Stored as the parent domain, without the `*.`.
    Wildcard(Box<str>),
    /// An Ingress rule with no `host` field.
    CatchAll,
}

impl HostSlot {
    fn display(&self) -> &str {
        match self {
            HostSlot::Exact(h) => h,
            HostSlot::Wildcard(h) => h,
            HostSlot::CatchAll => "*",
        }
    }
}

struct CanaryDraft {
    backend: Box<str>,
    header: Option<HeaderSpec>,
    cookie: Option<Box<str>>,
    weight: u32,
    weight_total: u32,
}

struct RouteDraft {
    slot: HostSlot,
    path: Box<str>,
    path_type: PathType,
    regex: Option<Box<Regex>>,
    backend: Box<str>,
    canary: Option<CanaryDraft>,
}

/// Builds a [`RouteTable`].
#[derive(Default)]
pub struct RouteTableBuilder {
    generation: u64,
    previous: Option<Arc<BackendStats>>,
    previous_routes: Option<Arc<RouteStats>>,
    backends: Vec<(Box<str>, LbPolicy, Vec<Endpoint>)>,
    backend_ids: FxHashMap<Box<str>, BackendId>,
    routes: Vec<RouteDraft>,
    default_backend: Option<Box<str>>,
    tls_exact: FxHashMap<Box<str>, Arc<CertifiedKeyHandle>>,
    tls_wildcard: FxHashMap<Box<str>, Arc<CertifiedKeyHandle>>,
    tls_default: Option<Arc<CertifiedKeyHandle>>,
}

impl RouteTableBuilder {
    /// A builder for the first table, at generation 0.
    pub fn new() -> Self {
        Self::default()
    }

    /// A builder for the table that will succeed `previous`.
    ///
    /// Sets the generation to one past `previous` and carries its load-balancer
    /// counters forward, so backends that survive the rebuild keep their
    /// in-flight accounting and round-robin position. This is the call that
    /// makes a configuration change invisible to traffic already in progress.
    ///
    /// The per-route counters travel the same way, keyed by route identity
    /// rather than by position; see [`RouteStats`].
    pub fn from_previous(previous: &RouteTable) -> Self {
        RouteTableBuilder {
            generation: previous.generation().saturating_add(1),
            previous: Some(Arc::clone(previous.stats())),
            previous_routes: Some(Arc::clone(previous.route_stats())),
            ..Self::default()
        }
    }

    /// Overrides the generation number.
    pub fn generation(&mut self, generation: u64) -> &mut Self {
        self.generation = generation;
        self
    }

    /// Registers a backend.
    ///
    /// An empty endpoint list is allowed: a Service whose pods are all
    /// unready is a normal state during a rollout, and rejecting the whole
    /// table for it would turn one bad Deployment into a cluster-wide outage.
    /// Selection simply yields nothing and the proxy answers 503.
    pub fn backend(
        &mut self,
        name: &str,
        policy: LbPolicy,
        endpoints: Vec<Endpoint>,
    ) -> Result<BackendId, BuildError> {
        if self.backend_ids.contains_key(name) {
            return Err(BuildError::DuplicateBackend {
                name: name.to_owned(),
            });
        }
        let id = BackendId(self.backends.len() as u32);
        let name: Box<str> = name.into();
        self.backend_ids.insert(name.clone(), id);
        self.backends.push((name, policy, endpoints));
        Ok(id)
    }

    /// The id assigned to an already-registered backend.
    pub fn backend_id(&self, name: &str) -> Option<BackendId> {
        self.backend_ids.get(name).copied()
    }

    /// Sets the backend serving requests that match no rule.
    pub fn default_backend(&mut self, name: &str) -> &mut Self {
        self.default_backend = Some(name.into());
        self
    }

    /// Adds a route.
    ///
    /// `host` is `None` for an Ingress rule with no `host` field, which serves
    /// every name not claimed by an exact or wildcard entry.
    pub fn route(
        &mut self,
        host: Option<&str>,
        path: &str,
        path_type: PathType,
        backend: &str,
    ) -> Result<(), BuildError> {
        let draft = self.draft(host, path, path_type, backend)?;
        self.routes.push(draft);
        Ok(())
    }

    /// Adds a route with a canary attached.
    ///
    /// In Kubernetes a canary arrives as a second Ingress carrying the same
    /// host and path plus `nginx.ingress.kubernetes.io/canary: "true"`. The
    /// controller merges that pair and calls this once.
    pub fn canary_route(
        &mut self,
        host: Option<&str>,
        path: &str,
        path_type: PathType,
        backend: &str,
        canary: &CanaryRules<'_>,
    ) -> Result<(), BuildError> {
        let mut draft = self.draft(host, path, path_type, backend)?;

        if canary.header_value.is_some() && canary.header_pattern.is_some() {
            return Err(BuildError::CanaryHeaderConflict {
                host: draft.slot.display().to_owned(),
                path: path.to_owned(),
            });
        }

        let weight_total = if canary.weight_total == 0 {
            DEFAULT_WEIGHT_TOTAL
        } else {
            canary.weight_total
        };
        if canary.weight > weight_total {
            return Err(BuildError::CanaryWeight {
                weight: canary.weight,
                total: weight_total,
            });
        }

        let header = match canary.header {
            Some(name) => {
                let pattern = match canary.header_pattern {
                    // Header patterns are full-value matches in ingress-nginx,
                    // so anchor both ends. The non-capturing group keeps a
                    // top-level alternation from escaping the anchors.
                    Some(p) => Some(Box::new(compile(&format!("^(?:{p})$"), false).map_err(
                        |source| BuildError::BadCanaryPattern {
                            pattern: p.to_owned(),
                            source,
                        },
                    )?)),
                    None => None,
                };
                Some(HeaderSpec {
                    name: name.into(),
                    value: canary.header_value.map(Into::into),
                    pattern,
                })
            }
            None => None,
        };

        draft.canary = Some(CanaryDraft {
            backend: canary.backend.into(),
            header,
            cookie: canary.cookie.map(Into::into),
            weight: canary.weight,
            weight_total,
        });
        self.routes.push(draft);
        Ok(())
    }

    /// Registers a certificate for a host, which may be a `*.example.com`
    /// wildcard.
    pub fn certificate(
        &mut self,
        host: &str,
        key: Arc<CertifiedKeyHandle>,
    ) -> Result<(), BuildError> {
        match normalize_config_host(host)? {
            HostSlot::Exact(h) => {
                self.tls_exact.insert(h, key);
            }
            HostSlot::Wildcard(h) => {
                self.tls_wildcard.insert(h, key);
            }
            HostSlot::CatchAll => {
                self.tls_default = Some(key);
            }
        }
        Ok(())
    }

    /// Sets the certificate served when SNI matches nothing.
    pub fn default_certificate(&mut self, key: Arc<CertifiedKeyHandle>) -> &mut Self {
        self.tls_default = Some(key);
        self
    }

    fn draft(
        &self,
        host: Option<&str>,
        path: &str,
        path_type: PathType,
        backend: &str,
    ) -> Result<RouteDraft, BuildError> {
        let slot = match host {
            Some(h) => normalize_config_host(h)?,
            None => HostSlot::CatchAll,
        };

        let regex = match path_type {
            PathType::ImplementationSpecific => {
                // ingress-nginx emits `location ~* "^<path>"`: anchored at the
                // start, case-insensitive, unanchored at the end. The
                // non-capturing group is a deliberate divergence from that
                // literal concatenation -- with a top-level alternation,
                // `^a|b` anchors only the first branch, which routes traffic
                // nobody intended.
                Some(Box::new(compile(&format!("^(?:{path})"), true).map_err(|source| {
                    BuildError::BadPattern {
                        path: path.to_owned(),
                        source,
                    }
                })?))
            }
            _ => {
                if !path.starts_with('/') {
                    return Err(BuildError::PathNotAbsolute {
                        path: path.to_owned(),
                    });
                }
                None
            }
        };

        Ok(RouteDraft {
            slot,
            path: path.into(),
            path_type,
            regex,
            backend: backend.into(),
            canary: None,
        })
    }

    /// Validates and freezes everything into a [`RouteTable`].
    pub fn build(self) -> Result<RouteTable, BuildError> {
        let RouteTableBuilder {
            generation,
            previous,
            previous_routes,
            backends,
            backend_ids,
            routes,
            default_backend,
            tls_exact,
            tls_wildcard,
            tls_default,
        } = self;

        // Counters first: a backend's stats index is its id, so the slab is
        // built from the same ordering the table will use.
        let specs: Vec<(Box<str>, Vec<std::net::SocketAddr>)> = backends
            .iter()
            .map(|(name, _, eps)| (name.clone(), eps.iter().map(|e| e.addr).collect()))
            .collect();
        let stats = Arc::new(BackendStats::rebuild(&specs, previous.as_deref()));

        let built_backends: Vec<Backend> = backends
            .into_iter()
            .enumerate()
            .map(|(i, (name, policy, eps))| Backend::new(name, eps, policy, i as u32))
            .collect();

        // Group rules by host, rejecting collisions as we go.
        let mut buckets: FxHashMap<HostSlot, Vec<PathRule>> = FxHashMap::default();
        let mut seen: FxHashSet<(HostSlot, Box<str>, PathType)> = FxHashSet::default();

        for draft in routes {
            let key = (draft.slot.clone(), draft.path.clone(), draft.path_type);
            if !seen.insert(key) {
                return Err(BuildError::DuplicatePath {
                    host: draft.slot.display().to_owned(),
                    path: String::from(draft.path),
                    path_type: draft.path_type,
                });
            }

            let RouteDraft {
                slot,
                path,
                path_type,
                regex,
                backend,
                canary,
            } = draft;

            let backend_id = resolve_backend(&backend_ids, &backend, &slot, &path)?;
            let canary = match canary {
                Some(c) => {
                    let id = resolve_backend(&backend_ids, &c.backend, &slot, &path)?;
                    Some(Box::new(CanarySpec::new(
                        id,
                        c.header,
                        c.cookie,
                        c.weight,
                        c.weight_total,
                    )))
                }
                None => None,
            };

            buckets
                .entry(slot)
                .or_default()
                .push(PathRule::new(path, path_type, regex, backend_id, canary));
        }

        // Bake the precedence order in, so matching never has to compare
        // candidates. A stable sort keeps regex rules in controller order.
        //
        // Counter indices are handed out here, after the sort, because this is
        // the order the rules are stored in. Which number a route gets does not
        // matter across generations — `RouteStats` carries counters forward by
        // identity — so the arbitrary map order is not a determinism problem.
        let mut hosts = FxHashMap::default();
        let mut wildcard_hosts = FxHashMap::default();
        let mut catch_all = None;
        let mut identities: Vec<RouteIdentity> = Vec::new();
        for (slot, mut rules) in buckets {
            rules.sort_by_key(PathRule::sort_key);
            let host: Box<str> = match &slot {
                HostSlot::Exact(h) => h.clone(),
                HostSlot::Wildcard(h) => format!("*.{h}").into(),
                HostSlot::CatchAll => "*".into(),
            };
            for rule in &mut rules {
                rule.set_stats_index(identities.len() as u32);
                identities.push(RouteIdentity {
                    host: host.clone(),
                    path: rule.path().into(),
                    path_type: rule.path_type(),
                    backend: built_backends
                        .get(rule.backend().0 as usize)
                        .map_or_else(|| Box::from(""), |b| Box::from(b.name())),
                });
            }
            match slot {
                HostSlot::Exact(h) => {
                    hosts.insert(h, VirtualHost::new(rules));
                }
                HostSlot::Wildcard(h) => {
                    wildcard_hosts.insert(h, VirtualHost::new(rules));
                }
                HostSlot::CatchAll => catch_all = Some(VirtualHost::new(rules)),
            }
        }
        let route_stats = Arc::new(RouteStats::rebuild(&identities, previous_routes.as_deref()));

        let default_backend = match default_backend {
            Some(name) => Some(backend_ids.get(name.as_ref()).copied().ok_or_else(|| {
                BuildError::UnknownBackend {
                    name: name.to_string(),
                    referrer: "default backend".to_owned(),
                }
            })?),
            None => None,
        };

        Ok(RouteTable::new(
            hosts,
            wildcard_hosts,
            catch_all,
            default_backend,
            built_backends,
            stats,
            route_stats,
            SniMap::new(tls_exact, tls_wildcard, tls_default),
            generation,
        ))
    }
}

/// Resolves a backend name, building the (allocating) referrer string only if
/// the lookup actually fails. A cluster with 10k routes should not allocate
/// 10k diagnostic strings in order to succeed.
fn resolve_backend(
    ids: &FxHashMap<Box<str>, BackendId>,
    name: &str,
    slot: &HostSlot,
    path: &str,
) -> Result<BackendId, BuildError> {
    ids.get(name)
        .copied()
        .ok_or_else(|| BuildError::UnknownBackend {
            name: name.to_owned(),
            referrer: format!("{}{}", slot.display(), path),
        })
}

fn compile(pattern: &str, case_insensitive: bool) -> Result<Regex, Box<regex::Error>> {
    RegexBuilder::new(pattern)
        .case_insensitive(case_insensitive)
        .size_limit(REGEX_SIZE_LIMIT)
        .build()
        .map_err(Box::new)
}

/// Validates a host from an Ingress object and sorts it into its bucket.
///
/// Allocating here is fine; this runs once per configuration change, not once
/// per request.
fn normalize_config_host(host: &str) -> Result<HostSlot, BuildError> {
    let host = host.trim_end_matches('.');
    if host.is_empty() || host.len() > MAX_HOST_LEN {
        return Err(BuildError::HostLength {
            host: host.to_owned(),
        });
    }

    let (is_wildcard, name) = match host.strip_prefix("*.") {
        Some(rest) => (true, rest),
        None => (false, host),
    };

    if let Some(found) = name.chars().find(|c| matches!(c, ':' | '/' | '*' | ' ')) {
        // A second `*`, a port, or a path means the Ingress was written
        // against a different controller's syntax; refusing is better than
        // guessing.
        return Err(if found == '*' {
            BuildError::WildcardShape {
                host: host.to_owned(),
            }
        } else {
            BuildError::HostSyntax {
                host: host.to_owned(),
                found,
            }
        });
    }

    if name.is_empty() {
        return Err(BuildError::WildcardShape {
            host: host.to_owned(),
        });
    }

    let lowered: Box<str> = name.to_ascii_lowercase().into();
    Ok(if is_wildcard {
        HostSlot::Wildcard(lowered)
    } else {
        HostSlot::Exact(lowered)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wildcard_is_stored_as_parent_domain() {
        assert_eq!(
            normalize_config_host("*.example.com").expect("valid"),
            HostSlot::Wildcard("example.com".into())
        );
    }

    #[test]
    fn config_hosts_are_lowercased_and_root_dot_trimmed() {
        assert_eq!(
            normalize_config_host("API.Example.COM.").expect("valid"),
            HostSlot::Exact("api.example.com".into())
        );
    }

    #[test]
    fn rejects_embedded_and_multiple_wildcards() {
        assert!(matches!(
            normalize_config_host("foo.*.example.com"),
            Err(BuildError::WildcardShape { .. })
        ));
        assert!(matches!(
            normalize_config_host("*.*.example.com"),
            Err(BuildError::WildcardShape { .. })
        ));
        assert!(matches!(
            normalize_config_host("*."),
            Err(BuildError::WildcardShape { .. })
        ));
    }

    #[test]
    fn rejects_ports_and_paths_in_config_hosts() {
        assert!(matches!(
            normalize_config_host("example.com:8080"),
            Err(BuildError::HostSyntax { found: ':', .. })
        ));
        assert!(matches!(
            normalize_config_host("example.com/foo"),
            Err(BuildError::HostSyntax { found: '/', .. })
        ));
    }

    #[test]
    fn rejects_empty_and_overlong_config_hosts() {
        assert!(matches!(
            normalize_config_host(""),
            Err(BuildError::HostLength { .. })
        ));
        assert!(matches!(
            normalize_config_host(&"a".repeat(MAX_HOST_LEN + 1)),
            Err(BuildError::HostLength { .. })
        ));
    }

    #[test]
    fn relative_paths_are_rejected() {
        let mut b = RouteTableBuilder::new();
        b.backend("api", LbPolicy::RoundRobin, vec![])
            .expect("registers");
        assert!(matches!(
            b.route(Some("example.com"), "foo", PathType::Prefix, "api"),
            Err(BuildError::PathNotAbsolute { .. })
        ));
    }

    #[test]
    fn duplicate_backend_names_are_rejected() {
        let mut b = RouteTableBuilder::new();
        b.backend("api", LbPolicy::RoundRobin, vec![])
            .expect("registers");
        assert!(matches!(
            b.backend("api", LbPolicy::RoundRobin, vec![]),
            Err(BuildError::DuplicateBackend { .. })
        ));
    }

    #[test]
    fn unknown_backend_is_reported_with_its_referrer() {
        let mut b = RouteTableBuilder::new();
        b.route(Some("example.com"), "/", PathType::Prefix, "missing")
            .expect("drafts");
        match b.build() {
            Err(BuildError::UnknownBackend { name, referrer }) => {
                assert_eq!(name, "missing");
                assert_eq!(referrer, "example.com/");
            }
            other => panic!("expected UnknownBackend, got {other:?}"),
        }
    }

    #[test]
    fn duplicate_path_on_one_host_is_rejected() {
        let mut b = RouteTableBuilder::new();
        b.backend("api", LbPolicy::RoundRobin, vec![])
            .expect("registers");
        b.route(Some("example.com"), "/a", PathType::Prefix, "api")
            .expect("drafts");
        b.route(Some("example.com"), "/a", PathType::Prefix, "api")
            .expect("drafts");
        assert!(matches!(
            b.build(),
            Err(BuildError::DuplicatePath { .. })
        ));
    }

    #[test]
    fn same_path_with_different_types_is_allowed() {
        let mut b = RouteTableBuilder::new();
        b.backend("api", LbPolicy::RoundRobin, vec![])
            .expect("registers");
        b.route(Some("example.com"), "/a", PathType::Prefix, "api")
            .expect("drafts");
        b.route(Some("example.com"), "/a", PathType::Exact, "api")
            .expect("drafts");
        assert!(b.build().is_ok(), "Exact and Prefix /a coexist");
    }

    #[test]
    fn same_path_on_different_hosts_is_allowed() {
        let mut b = RouteTableBuilder::new();
        b.backend("api", LbPolicy::RoundRobin, vec![])
            .expect("registers");
        b.route(Some("a.example.com"), "/", PathType::Prefix, "api")
            .expect("drafts");
        b.route(Some("b.example.com"), "/", PathType::Prefix, "api")
            .expect("drafts");
        assert!(b.build().is_ok());
    }

    #[test]
    fn bad_regex_is_rejected_at_the_offending_call() {
        let mut b = RouteTableBuilder::new();
        assert!(matches!(
            b.route(
                Some("example.com"),
                "/foo[",
                PathType::ImplementationSpecific,
                "api"
            ),
            Err(BuildError::BadPattern { .. })
        ));
    }

    #[test]
    fn canary_header_value_and_pattern_conflict() {
        let mut b = RouteTableBuilder::new();
        let rules = CanaryRules {
            backend: "canary",
            header: Some("x-canary"),
            header_value: Some("beta"),
            header_pattern: Some("beta.*"),
            ..Default::default()
        };
        assert!(matches!(
            b.canary_route(Some("example.com"), "/", PathType::Prefix, "api", &rules),
            Err(BuildError::CanaryHeaderConflict { .. })
        ));
    }

    #[test]
    fn canary_weight_above_total_is_rejected() {
        let mut b = RouteTableBuilder::new();
        let rules = CanaryRules {
            backend: "canary",
            weight: 150,
            ..Default::default()
        };
        assert!(matches!(
            b.canary_route(Some("example.com"), "/", PathType::Prefix, "api", &rules),
            Err(BuildError::CanaryWeight {
                weight: 150,
                total: 100
            })
        ));
    }

    #[test]
    fn canary_weight_total_defaults_to_100() {
        let mut b = RouteTableBuilder::new();
        b.backend("api", LbPolicy::RoundRobin, vec![])
            .expect("registers");
        b.backend("canary", LbPolicy::RoundRobin, vec![])
            .expect("registers");
        let rules = CanaryRules {
            backend: "canary",
            weight: 20,
            ..Default::default()
        };
        b.canary_route(Some("example.com"), "/", PathType::Prefix, "api", &rules)
            .expect("drafts");
        let table = b.build().expect("builds");
        let spec = table
            .match_request("example.com", "/")
            .and_then(|m| m.canary())
            .map(|c| c.weight_total());
        assert_eq!(spec, Some(100));
    }

    #[test]
    fn generation_advances_from_previous() {
        let mut b = RouteTableBuilder::new();
        b.generation(7);
        let first = b.build().expect("builds");
        let next = RouteTableBuilder::from_previous(&first)
            .build()
            .expect("builds");
        assert_eq!(next.generation(), 8);
    }

    #[test]
    fn empty_backend_is_allowed_not_fatal() {
        let mut b = RouteTableBuilder::new();
        b.backend("api", LbPolicy::RoundRobin, vec![])
            .expect("registers");
        b.route(Some("example.com"), "/", PathType::Prefix, "api")
            .expect("drafts");
        let table = b.build().expect("a rolling Deployment must not fail the table");
        let m = table.match_request("example.com", "/").expect("matches");
        assert!(m.backend().endpoints().is_empty());
    }
}
