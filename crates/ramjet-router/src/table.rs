//! The immutable route table and the request matcher.

use std::sync::Arc;

use arc_swap::{ArcSwap, Guard};

use crate::backend::{Backend, BackendId};
use crate::canary::CanarySpec;
use crate::host::{self, FxHashMap, Scan, MAX_HOST_LEN};
use crate::path::PathRule;
use crate::stats::BackendStats;
use crate::tls::SniMap;

/// The routes served for one host name.
///
/// `routes` is stored in final precedence order — Exact rules first, then
/// Prefix rules from longest to shortest, then `ImplementationSpecific` regexes
/// in the order the controller supplied them. Because the order is baked in at
/// build time, matching is a linear scan that stops at the first hit, with no
/// comparison of candidates and no "best so far" bookkeeping.
///
/// Linear looks wrong until you count: a host in a real cluster carries a
/// handful of paths, and a handful of 40-byte rules is one or two cache lines
/// that arrive together. A tree would trade those two lines for pointer chasing.
#[derive(Debug)]
pub struct VirtualHost {
    routes: Vec<PathRule>,
}

impl VirtualHost {
    pub(crate) fn new(routes: Vec<PathRule>) -> Self {
        VirtualHost { routes }
    }

    /// The first rule matching `path`, which is also the highest-precedence one.
    #[inline]
    fn match_path(&self, path: &str) -> Option<&PathRule> {
        self.routes.iter().find(|rule| rule.matches(path))
    }

    /// Every rule, in precedence order.
    pub fn routes(&self) -> &[PathRule] {
        &self.routes
    }
}

/// Which host entry answered a request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HostMatch {
    /// An exact host name.
    Exact,
    /// A `*.example.com` wildcard.
    Wildcard,
    /// An Ingress rule with no host, serving every unclaimed name.
    CatchAll,
    /// No host entry matched; the table's default backend answered.
    DefaultBackend,
}

/// What [`RouteTable::match_request`] found.
///
/// Every field borrows from the table, so producing one of these allocates
/// nothing. It is valid for as long as the caller holds the snapshot it was
/// matched against.
#[derive(Debug)]
pub struct MatchResult<'t> {
    backend: &'t Backend,
    rule: Option<&'t PathRule>,
    host_match: HostMatch,
}

impl<'t> MatchResult<'t> {
    /// The backend to forward to, before any canary decision.
    pub fn backend(&self) -> &'t Backend {
        self.backend
    }

    /// The rule that matched, or `None` when the default backend answered.
    pub fn rule(&self) -> Option<&'t PathRule> {
        self.rule
    }

    /// The canary attached to the matched rule, if any.
    pub fn canary(&self) -> Option<&'t CanarySpec> {
        self.rule?.canary()
    }

    /// How the host was resolved.
    pub fn host_match(&self) -> HostMatch {
        self.host_match
    }
}

/// An immutable routing snapshot.
///
/// Nothing in here is mutated after [`build`](crate::RouteTableBuilder::build)
/// returns. A configuration change produces a whole new table which replaces
/// this one with a single pointer store; see [`SharedRouteTable`].
#[derive(Debug)]
pub struct RouteTable {
    hosts: FxHashMap<Box<str>, VirtualHost>,
    /// Keyed by parent domain: `*.example.com` is stored as `example.com`, so a
    /// wildcard lookup is one hash of the query minus its first label.
    wildcard_hosts: FxHashMap<Box<str>, VirtualHost>,
    /// Rules from Ingress objects with no `host` field.
    catch_all: Option<VirtualHost>,
    default_backend: Option<BackendId>,
    backends: Vec<Backend>,
    stats: Arc<BackendStats>,
    tls: SniMap,
    generation: u64,
}

impl RouteTable {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        hosts: FxHashMap<Box<str>, VirtualHost>,
        wildcard_hosts: FxHashMap<Box<str>, VirtualHost>,
        catch_all: Option<VirtualHost>,
        default_backend: Option<BackendId>,
        backends: Vec<Backend>,
        stats: Arc<BackendStats>,
        tls: SniMap,
        generation: u64,
    ) -> Self {
        RouteTable {
            hosts,
            wildcard_hosts,
            catch_all,
            default_backend,
            backends,
            stats,
            tls,
            generation,
        }
    }

    /// Routes one request.
    ///
    /// `host` is the raw `Host` header (or `:authority`) and `path` is the
    /// request target with any query string already removed.
    ///
    /// # Precedence
    ///
    /// The host selects a [`VirtualHost`] first — exact name, then single-label
    /// wildcard, then the no-host catch-all — and the path is then matched
    /// *within* that one. A request whose host matches exactly but whose path
    /// matches nothing falls to the table's default backend; it does not
    /// reconsider the wildcard. This is nginx's server-then-location order, and
    /// deviating from it would silently change where traffic lands during a
    /// migration from ingress-nginx.
    ///
    /// # Allocation
    ///
    /// This function performs no heap allocation. The host is normalized in
    /// place where possible and otherwise into a stack buffer; lookups borrow;
    /// the result borrows. `tests/no_alloc.rs` enforces this with a counting
    /// global allocator rather than leaving it as a claim in a comment.
    pub fn match_request(&self, host: &str, path: &str) -> Option<MatchResult<'_>> {
        match host::scan(host) {
            Scan::Clean(name) => self.match_normalized(name, path),
            Scan::Fold(name) => {
                // Cold: the header contained uppercase. The buffer lives in
                // this branch so the common path never touches it.
                let mut buf = [0u8; MAX_HOST_LEN];
                match host::fold_lower(name, &mut buf) {
                    Some(folded) => self.match_normalized(folded, path),
                    None => self.catch_all_or_default(path),
                }
            }
            // A missing or malformed `Host` still gets served by the default
            // server, the same way nginx handles it.
            Scan::Invalid => self.catch_all_or_default(path),
        }
    }

    #[inline]
    fn match_normalized(&self, host: &str, path: &str) -> Option<MatchResult<'_>> {
        let (vhost, host_match) = if let Some(v) = self.hosts.get(host) {
            (v, HostMatch::Exact)
        } else if let Some(v) = host::parent_domain(host).and_then(|p| self.wildcard_hosts.get(p)) {
            (v, HostMatch::Wildcard)
        } else if let Some(v) = self.catch_all.as_ref() {
            (v, HostMatch::CatchAll)
        } else {
            return self.default_result();
        };

        match vhost.match_path(path) {
            Some(rule) => self.hit(rule, host_match),
            // The host claimed the request but no path matched. nginx resolves
            // the server before the location, so this does not fall back to a
            // wildcard entry -- it goes to the default backend, i.e. a 404.
            None => self.default_result(),
        }
    }

    /// Serves a request whose host matched nothing: the hostless catch-all
    /// first, then the default backend.
    #[inline]
    fn catch_all_or_default(&self, path: &str) -> Option<MatchResult<'_>> {
        if let Some(rule) = self.catch_all.as_ref().and_then(|v| v.match_path(path)) {
            return self.hit(rule, HostMatch::CatchAll);
        }
        self.default_result()
    }

    #[inline]
    fn hit<'t>(&'t self, rule: &'t PathRule, host_match: HostMatch) -> Option<MatchResult<'t>> {
        Some(MatchResult {
            backend: self.backends.get(rule.backend().0 as usize)?,
            rule: Some(rule),
            host_match,
        })
    }

    #[inline]
    fn default_result(&self) -> Option<MatchResult<'_>> {
        let id = self.default_backend?;
        Some(MatchResult {
            backend: self.backends.get(id.0 as usize)?,
            rule: None,
            host_match: HostMatch::DefaultBackend,
        })
    }

    /// Looks a backend up by id.
    pub fn backend(&self, id: BackendId) -> Option<&Backend> {
        self.backends.get(id.0 as usize)
    }

    /// Every backend, in id order.
    pub fn backends(&self) -> &[Backend] {
        &self.backends
    }

    /// The load-balancer counters for this table.
    ///
    /// Reached through the table so that one snapshot load yields a backend and
    /// the state to select through consistently. The counters themselves are
    /// shared with neighbouring generations; see [`BackendStats`].
    pub fn stats(&self) -> &Arc<BackendStats> {
        &self.stats
    }

    /// Certificate lookup for TLS handshakes.
    pub fn tls(&self) -> &SniMap {
        &self.tls
    }

    /// The backend serving requests that match no rule.
    pub fn default_backend(&self) -> Option<BackendId> {
        self.default_backend
    }

    /// Monotonic version, bumped once per published table.
    pub fn generation(&self) -> u64 {
        self.generation
    }

    /// The virtual host registered for the exact name `name`.
    ///
    /// `name` must already be normalized (lowercase, no port); use
    /// [`match_request`](Self::match_request) for raw header values.
    pub fn host(&self, name: &str) -> Option<&VirtualHost> {
        self.hosts.get(name)
    }

    /// The virtual host registered for `*.{parent}`.
    pub fn wildcard_host(&self, parent: &str) -> Option<&VirtualHost> {
        self.wildcard_hosts.get(parent)
    }

    /// Number of exact host entries.
    pub fn host_count(&self) -> usize {
        self.hosts.len()
    }

    /// Number of wildcard host entries.
    pub fn wildcard_host_count(&self) -> usize {
        self.wildcard_hosts.len()
    }

    /// Every exact host name, in unspecified order.
    pub fn host_names(&self) -> impl Iterator<Item = &str> {
        self.hosts.keys().map(|k| &**k)
    }

    /// Every wildcard parent domain, in unspecified order.
    pub fn wildcard_parents(&self) -> impl Iterator<Item = &str> {
        self.wildcard_hosts.keys().map(|k| &**k)
    }

    /// The virtual host for Ingress rules with no `host` field.
    pub fn catch_all(&self) -> Option<&VirtualHost> {
        self.catch_all.as_ref()
    }

    /// Total number of path rules across every host.
    pub fn route_count(&self) -> usize {
        self.hosts
            .values()
            .chain(self.wildcard_hosts.values())
            .chain(self.catch_all.as_ref())
            .map(|v| v.routes.len())
            .sum()
    }
}

/// The published snapshot pointer.
///
/// This is the entire concurrency design. Readers do one atomic load per
/// request and then work with an immutable table; there is no lock, no
/// reference counting on the read path, and no reload. The controller publishes
/// a new configuration by building a fresh [`RouteTable`] and storing it here,
/// which neither blocks a reader nor invalidates a snapshot already in use — an
/// in-flight request finishes against the table it started with.
#[derive(Debug)]
pub struct SharedRouteTable {
    inner: ArcSwap<RouteTable>,
}

impl SharedRouteTable {
    /// Publishes an initial table.
    pub fn new(table: RouteTable) -> Self {
        SharedRouteTable {
            inner: ArcSwap::from_pointee(table),
        }
    }

    /// Loads the current snapshot.
    ///
    /// The returned guard is the hot-path read: on the fast path it is a plain
    /// atomic load with no reference count touched at all. Hold it for the
    /// duration of a request and drop it when the request completes.
    #[inline]
    pub fn load(&self) -> Guard<Arc<RouteTable>> {
        self.inner.load()
    }

    /// Loads the current snapshot as a full `Arc`, for holding across await
    /// points or handing to another task.
    #[inline]
    pub fn load_full(&self) -> Arc<RouteTable> {
        self.inner.load_full()
    }

    /// Publishes a new table. Returns the one it replaced.
    pub fn store(&self, table: RouteTable) -> Arc<RouteTable> {
        self.inner.swap(Arc::new(table))
    }

    /// The generation currently published.
    pub fn generation(&self) -> u64 {
        self.inner.load().generation()
    }
}
