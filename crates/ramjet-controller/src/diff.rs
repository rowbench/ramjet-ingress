//! What changed between two generations, in the vocabulary an operator uses.
//!
//! # Why this is not the digest
//!
//! [`Digest`](crate::digest) already answers "did anything change?", and that
//! is all the rebuild loop needs to decide whether to publish. It cannot answer
//! the question somebody asks at 3am, which is *what* changed — a hash that
//! differs tells you nothing about whether a deploy added a route, drained a
//! backend, or rotated a certificate.
//!
//! So this is a second pass over the same material, taken only when a publish
//! is actually happening, comparing the two compiled tables field by field. It
//! is a pure function of two [`RouteTable`]s: no cluster, no clock, no I/O,
//! which is why every category below has a unit test built from tables
//! constructed in memory.
//!
//! # Why the tables and not the Ingress objects
//!
//! Diffing the API objects would report what somebody *typed*, and the useful
//! question is what the data plane is going to *do*. An Ingress edited from
//! `Prefix: /foo` to `Prefix: /foo/` compiles to the same route and should not
//! appear here; a Deployment scaling from three pods to five changes no Ingress
//! at all and should. Comparing compiled tables gets both right for free,
//! because the compiler already normalised everything.

use std::collections::BTreeMap;
use std::fmt::Write as _;

use ramjet_router::RouteTable;
use serde_json::{json, Value};

/// A route's identity, as it appears on both sides of a comparison.
type RouteKey = (String, String, &'static str);

/// What one route sends where.
#[derive(Debug, Clone, PartialEq, Eq)]
struct RouteTarget {
    backend: String,
    endpoints: usize,
}

/// The difference between two compiled generations.
///
/// Every list is sorted, so two replicas that compiled the same change describe
/// it identically and a human comparing two of these is comparing content
/// rather than iteration order.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ConfigDiff {
    /// The generation this was compared against; `None` for the first one.
    pub from: Option<u64>,
    /// The generation being described.
    pub to: u64,
    /// `host path -> backend`, for routes that did not exist before.
    pub routes_added: Vec<String>,
    /// `host path -> backend`, for routes that have gone.
    pub routes_removed: Vec<String>,
    /// Routes that stayed but now send somewhere else, or to a different number
    /// of endpoints.
    pub backends_changed: Vec<String>,
    /// Hosts whose certificate identity changed.
    ///
    /// One list rather than three, because the handle id is content-derived and
    /// so covers every case with the same test: a host that gained a
    /// certificate, lost one, or had one replaced all read as "the material
    /// serving this name is not what it was". `*` is the default certificate.
    /// The finer distinction is in [`summary`](Self::summary).
    pub certs_rotated: Vec<String>,
    /// Hosts the table now serves that it did not before.
    pub hosts_added: Vec<String>,
    /// Hosts the table has stopped serving.
    pub hosts_removed: Vec<String>,
    /// Whether the backend answering unmatched requests changed.
    pub default_backend_changed: bool,
    /// The backend now answering unmatched requests, if there is one.
    pub default_backend: Option<String>,
    /// Certificates the new generation has and the old one did not.
    pub certs_added: usize,
    /// Certificates the old generation had and the new one does not.
    pub certs_removed: usize,
}

impl ConfigDiff {
    /// Compares two compiled tables. `previous` is `None` for the first
    /// generation a process applies, which reports everything as added.
    pub fn compute(previous: Option<&RouteTable>, next: &RouteTable) -> Self {
        let before = routes_of(previous);
        let after = routes_of(Some(next));

        let mut diff = ConfigDiff {
            from: previous.map(RouteTable::generation),
            to: next.generation(),
            ..ConfigDiff::default()
        };

        for (key, target) in &after {
            match before.get(key) {
                None => diff.routes_added.push(describe(key, target)),
                Some(was) if was.backend != target.backend => diff.backends_changed.push(format!(
                    "{} {}: {} -> {}",
                    key.0, key.1, was.backend, target.backend
                )),
                Some(was) if was.endpoints != target.endpoints => {
                    diff.backends_changed.push(format!(
                        "{} {}: {} -> {} endpoints",
                        key.0, key.1, was.endpoints, target.endpoints
                    ));
                }
                Some(_) => {}
            }
        }
        for (key, target) in &before {
            if !after.contains_key(key) {
                diff.routes_removed.push(describe(key, target));
            }
        }

        let hosts_before = hosts_of(previous);
        let hosts_after = hosts_of(Some(next));
        diff.hosts_added = hosts_after.difference(&hosts_before).cloned().collect();
        diff.hosts_removed = hosts_before.difference(&hosts_after).cloned().collect();

        let certs_before = certs_of(previous);
        let certs_after = certs_of(Some(next));
        for (host, id) in &certs_after {
            match certs_before.get(host) {
                None => {
                    diff.certs_added += 1;
                    diff.certs_rotated.push(host.clone());
                }
                Some(was) if was != id => diff.certs_rotated.push(host.clone()),
                Some(_) => {}
            }
        }
        for host in certs_before.keys() {
            if !certs_after.contains_key(host) {
                diff.certs_removed += 1;
                diff.certs_rotated.push(host.clone());
            }
        }

        let default_before = previous.and_then(default_backend);
        diff.default_backend = default_backend(next);
        diff.default_backend_changed = default_before != diff.default_backend;

        diff.routes_added.sort();
        diff.routes_removed.sort();
        diff.backends_changed.sort();
        diff.certs_rotated.sort();
        diff.hosts_added.sort();
        diff.hosts_removed.sort();
        diff
    }

    /// Whether anything at all moved.
    ///
    /// A publish with an empty diff is possible — the digest covers the compiled
    /// plan, which includes things no category here names — and saying "no
    /// change" is more honest than an empty message.
    pub fn is_empty(&self) -> bool {
        self.routes_added.is_empty()
            && self.routes_removed.is_empty()
            && self.backends_changed.is_empty()
            && self.certs_rotated.is_empty()
            && self.hosts_added.is_empty()
            && self.hosts_removed.is_empty()
            && !self.default_backend_changed
    }

    /// The one line that goes in a log, a Kubernetes Event, and a chat message.
    ///
    /// Counts rather than contents: the full lists are one `curl` away on
    /// `/admin/generations`, and an Event note is capped at a kilobyte, so a
    /// message that tried to name every route would be truncated exactly when a
    /// change was big enough to be worth reading about.
    pub fn summary(&self) -> String {
        let mut parts: Vec<String> = Vec::new();
        for (count, singular, plural) in [
            (self.routes_added.len(), "route added", "routes added"),
            (self.routes_removed.len(), "route removed", "routes removed"),
            (
                self.backends_changed.len(),
                "backend changed",
                "backends changed",
            ),
            (self.hosts_added.len(), "host added", "hosts added"),
            (self.hosts_removed.len(), "host removed", "hosts removed"),
        ] {
            if count > 0 {
                parts.push(format!(
                    "{count} {}",
                    if count == 1 { singular } else { plural }
                ));
            }
        }

        // Certificates are one list but three events, and which one it was is
        // the first thing anybody asks.
        let replaced = self
            .certs_rotated
            .len()
            .saturating_sub(self.certs_added + self.certs_removed);
        for (count, singular, plural) in [
            (self.certs_added, "cert added", "certs added"),
            (self.certs_removed, "cert removed", "certs removed"),
            (replaced, "cert rotated", "certs rotated"),
        ] {
            if count > 0 {
                parts.push(format!(
                    "{count} {}",
                    if count == 1 { singular } else { plural }
                ));
            }
        }

        if self.default_backend_changed {
            parts.push(match &self.default_backend {
                Some(backend) => format!("default backend now {backend}"),
                None => "default backend cleared".to_owned(),
            });
        }

        let mut summary = if parts.is_empty() {
            "no visible change".to_owned()
        } else {
            parts.join(", ")
        };
        let _ = write!(
            summary,
            " (gen {}\u{2192}{})",
            self.from.unwrap_or(0),
            self.to
        );
        summary
    }

    /// The diff as the admin API and the audit webhook serve it.
    ///
    /// Written by hand rather than derived. The shape is a published contract
    /// that a terminal UI and whatever an operator points `--audit-webhook` at
    /// both parse, and a derive would let an innocent field rename change the
    /// wire format without anybody deciding to.
    pub fn to_json(&self) -> Value {
        json!({
            "summary": self.summary(),
            "routes_added": self.routes_added,
            "routes_removed": self.routes_removed,
            "backends_changed": self.backends_changed,
            "certs_rotated": self.certs_rotated,
            "hosts_added": self.hosts_added,
            "hosts_removed": self.hosts_removed,
        })
    }
}

fn describe(key: &RouteKey, target: &RouteTarget) -> String {
    format!("{} {} -> {}", key.0, key.1, target.backend)
}

fn routes_of(table: Option<&RouteTable>) -> BTreeMap<RouteKey, RouteTarget> {
    let Some(table) = table else {
        return BTreeMap::new();
    };
    table
        .routes()
        .map(|(host, rule)| {
            let backend = table.backend(rule.backend());
            (
                (
                    host.to_string(),
                    rule.path().to_owned(),
                    rule.path_type().as_str(),
                ),
                RouteTarget {
                    backend: backend.map_or_else(String::new, |b| b.name().to_owned()),
                    endpoints: backend.map_or(0, |b| b.endpoints().len()),
                },
            )
        })
        .collect()
}

fn hosts_of(table: Option<&RouteTable>) -> std::collections::BTreeSet<String> {
    let Some(table) = table else {
        return std::collections::BTreeSet::new();
    };
    table
        .virtual_hosts()
        .map(|(host, _)| host.to_string())
        .collect()
}

fn certs_of(table: Option<&RouteTable>) -> BTreeMap<String, u64> {
    let Some(table) = table else {
        return BTreeMap::new();
    };
    table
        .tls()
        .entries()
        .map(|(host, id)| (host.to_string(), id))
        .collect()
}

fn default_backend(table: &RouteTable) -> Option<String> {
    table
        .default_backend()
        .and_then(|id| table.backend(id))
        .map(|backend| backend.name().to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;
    use ramjet_router::{
        CertifiedKeyHandle, Endpoint, LbPolicy, PathType, RouteTableBuilder,
    };
    use std::sync::Arc;

    /// A backend with `count` endpoints, so an endpoint-count delta can be
    /// produced without inventing addresses by hand at every call site.
    fn endpoints(count: u16) -> Vec<Endpoint> {
        (0..count)
            .map(|i| Endpoint::new(std::net::SocketAddr::from(([10, 0, 0, 1], 8080 + i))))
            .collect()
    }

    /// A one-route table: `host path` to `backend`, which has `count`
    /// endpoints.
    fn table(host: &str, path: &str, backend: &str, count: u16) -> RouteTable {
        let mut builder = RouteTableBuilder::new();
        builder
            .backend(backend, LbPolicy::RoundRobin, endpoints(count))
            .expect("registers");
        builder
            .route(Some(host), path, PathType::Prefix, backend)
            .expect("drafts");
        builder.build().expect("builds")
    }

    #[test]
    fn the_first_generation_reports_everything_as_added() {
        let next = table("example.com", "/", "prod/api:80", 2);
        let diff = ConfigDiff::compute(None, &next);

        assert_eq!(diff.from, None);
        assert_eq!(diff.routes_added, vec!["example.com / -> prod/api:80"]);
        assert_eq!(diff.hosts_added, vec!["example.com"]);
        assert!(diff.routes_removed.is_empty());
        assert!(!diff.is_empty());
    }

    #[test]
    fn an_unchanged_table_has_an_empty_diff() {
        let first = table("example.com", "/", "prod/api:80", 2);
        let second = table("example.com", "/", "prod/api:80", 2);
        let diff = ConfigDiff::compute(Some(&first), &second);

        assert!(diff.is_empty(), "{diff:?}");
        assert!(diff.summary().starts_with("no visible change"));
    }

    #[test]
    fn a_new_route_on_an_existing_host_is_an_addition_not_a_host_change() {
        let first = table("example.com", "/", "prod/api:80", 1);
        let mut builder = RouteTableBuilder::new();
        builder
            .backend("prod/api:80", LbPolicy::RoundRobin, endpoints(1))
            .expect("registers");
        builder
            .route(Some("example.com"), "/", PathType::Prefix, "prod/api:80")
            .expect("drafts");
        builder
            .route(Some("example.com"), "/v2", PathType::Prefix, "prod/api:80")
            .expect("drafts");
        let second = builder.build().expect("builds");

        let diff = ConfigDiff::compute(Some(&first), &second);
        assert_eq!(diff.routes_added, vec!["example.com /v2 -> prod/api:80"]);
        assert!(diff.hosts_added.is_empty(), "the host was already served");
    }

    #[test]
    fn a_removed_route_takes_its_host_with_it() {
        let first = table("example.com", "/", "prod/api:80", 1);
        let second = RouteTableBuilder::new().build().expect("builds");

        let diff = ConfigDiff::compute(Some(&first), &second);
        assert_eq!(diff.routes_removed, vec!["example.com / -> prod/api:80"]);
        assert_eq!(diff.hosts_removed, vec!["example.com"]);
    }

    #[test]
    fn a_route_pointed_at_a_different_backend_is_a_backend_change() {
        let first = table("example.com", "/", "prod/api:80", 1);
        let second = table("example.com", "/", "prod/api-v2:80", 1);

        let diff = ConfigDiff::compute(Some(&first), &second);
        assert_eq!(
            diff.backends_changed,
            vec!["example.com /: prod/api:80 -> prod/api-v2:80"]
        );
        assert!(
            diff.routes_added.is_empty() && diff.routes_removed.is_empty(),
            "the route is the same route; only where it points moved"
        );
    }

    /// The one that matters during a rollout: no Ingress changed, but the
    /// Deployment behind it did.
    #[test]
    fn a_scaled_backend_shows_its_endpoint_delta() {
        let first = table("example.com", "/", "prod/api:80", 3);
        let second = table("example.com", "/", "prod/api:80", 5);

        let diff = ConfigDiff::compute(Some(&first), &second);
        assert_eq!(
            diff.backends_changed,
            vec!["example.com /: 3 -> 5 endpoints"]
        );
        assert_eq!(diff.summary(), "1 backend changed (gen 0\u{2192}0)");
    }

    #[test]
    fn a_wildcard_host_is_reported_with_its_star() {
        // The table stores it under its parent domain, and reporting that name
        // would describe a host the table does not serve.
        let first = RouteTableBuilder::new().build().expect("builds");
        let second = table("*.example.com", "/", "prod/api:80", 1);

        let diff = ConfigDiff::compute(Some(&first), &second);
        assert_eq!(diff.hosts_added, vec!["*.example.com"]);
        assert_eq!(diff.routes_added, vec!["*.example.com / -> prod/api:80"]);
    }

    #[test]
    fn a_path_type_change_is_a_different_route() {
        let first = table("example.com", "/a", "prod/api:80", 1);
        let mut builder = RouteTableBuilder::new();
        builder
            .backend("prod/api:80", LbPolicy::RoundRobin, endpoints(1))
            .expect("registers");
        builder
            .route(Some("example.com"), "/a", PathType::Exact, "prod/api:80")
            .expect("drafts");
        let second = builder.build().expect("builds");

        let diff = ConfigDiff::compute(Some(&first), &second);
        assert_eq!(diff.routes_added.len(), 1);
        assert_eq!(diff.routes_removed.len(), 1);
    }

    /// A certificate with `id` for `host`, so the three certificate cases can
    /// be built without a crypto stack anywhere near this crate.
    fn with_cert(host: &str, id: Option<u64>) -> RouteTable {
        let mut builder = RouteTableBuilder::new();
        builder
            .backend("prod/api:80", LbPolicy::RoundRobin, endpoints(1))
            .expect("registers");
        builder
            .route(Some(host), "/", PathType::Prefix, "prod/api:80")
            .expect("drafts");
        if let Some(id) = id {
            builder
                .certificate(host, Arc::new(CertifiedKeyHandle::new(id)))
                .expect("a valid host");
        }
        builder.build().expect("builds")
    }

    #[test]
    fn a_rotated_certificate_is_the_host_whose_material_moved() {
        let first = with_cert("example.com", Some(1));
        let second = with_cert("example.com", Some(2));

        let diff = ConfigDiff::compute(Some(&first), &second);
        assert_eq!(diff.certs_rotated, vec!["example.com"]);
        assert_eq!((diff.certs_added, diff.certs_removed), (0, 0));
        assert_eq!(diff.summary(), "1 cert rotated (gen 0\u{2192}0)");
    }

    #[test]
    fn gaining_and_losing_a_certificate_are_told_apart_in_the_summary() {
        let none = with_cert("example.com", None);
        let some = with_cert("example.com", Some(1));

        let gained = ConfigDiff::compute(Some(&none), &some);
        assert_eq!(gained.certs_rotated, vec!["example.com"]);
        assert_eq!((gained.certs_added, gained.certs_removed), (1, 0));
        assert_eq!(gained.summary(), "1 cert added (gen 0\u{2192}0)");

        let lost = ConfigDiff::compute(Some(&some), &none);
        assert_eq!(lost.certs_rotated, vec!["example.com"]);
        assert_eq!((lost.certs_added, lost.certs_removed), (0, 1));
        assert_eq!(lost.summary(), "1 cert removed (gen 0\u{2192}0)");
    }

    #[test]
    fn a_changed_default_backend_is_named() {
        let mut builder = RouteTableBuilder::new();
        builder
            .backend("kube-system/notfound:80", LbPolicy::RoundRobin, endpoints(1))
            .expect("registers");
        builder.default_backend("kube-system/notfound:80");
        let first = builder.build().expect("builds");

        let mut builder = RouteTableBuilder::new();
        builder
            .backend("prod/fallback:80", LbPolicy::RoundRobin, endpoints(1))
            .expect("registers");
        builder.default_backend("prod/fallback:80");
        let second = builder.build().expect("builds");

        let diff = ConfigDiff::compute(Some(&first), &second);
        assert!(diff.default_backend_changed);
        assert_eq!(diff.default_backend.as_deref(), Some("prod/fallback:80"));
        assert_eq!(
            diff.summary(),
            "default backend now prod/fallback:80 (gen 0\u{2192}0)"
        );

        let cleared = ConfigDiff::compute(Some(&first), &RouteTableBuilder::new().build().expect("builds"));
        assert!(cleared.default_backend_changed);
        assert!(cleared.summary().contains("default backend cleared"));
    }

    #[test]
    fn the_summary_reads_like_the_sentence_an_operator_would_write() {
        let mut diff = ConfigDiff {
            from: Some(41),
            to: 42,
            ..ConfigDiff::default()
        };
        diff.routes_added = vec!["a".to_owned(), "b".to_owned(), "c".to_owned()];
        diff.certs_rotated = vec!["example.com".to_owned()];
        assert_eq!(
            diff.summary(),
            "3 routes added, 1 cert rotated (gen 41\u{2192}42)"
        );
    }

    #[test]
    fn the_json_carries_exactly_the_published_keys() {
        let diff = ConfigDiff::compute(None, &table("example.com", "/", "prod/api:80", 1));
        let value = diff.to_json();
        let object = value.as_object().expect("an object");

        let mut keys: Vec<&str> = object.keys().map(String::as_str).collect();
        keys.sort_unstable();
        assert_eq!(
            keys,
            vec![
                "backends_changed",
                "certs_rotated",
                "hosts_added",
                "hosts_removed",
                "routes_added",
                "routes_removed",
                "summary",
            ],
            "the diff shape is a published contract; adding a key is a change to it"
        );
        assert_eq!(value["routes_added"][0], "example.com / -> prod/api:80");
        assert!(value["summary"].as_str().is_some_and(|s| s.contains("gen")));
    }

    #[test]
    fn every_list_comes_out_sorted() {
        // Two replicas compiling the same change must describe it identically,
        // and the tables underneath are hash maps.
        let first = RouteTableBuilder::new().build().expect("builds");
        let mut builder = RouteTableBuilder::new();
        builder
            .backend("prod/api:80", LbPolicy::RoundRobin, endpoints(1))
            .expect("registers");
        for host in ["z.example.com", "a.example.com", "m.example.com"] {
            builder
                .route(Some(host), "/", PathType::Prefix, "prod/api:80")
                .expect("drafts");
        }
        let second = builder.build().expect("builds");

        let diff = ConfigDiff::compute(Some(&first), &second);
        assert_eq!(
            diff.hosts_added,
            vec!["a.example.com", "m.example.com", "z.example.com"]
        );
        let mut sorted = diff.routes_added.clone();
        sorted.sort();
        assert_eq!(diff.routes_added, sorted);
    }
}
