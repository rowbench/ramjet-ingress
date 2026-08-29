//! The admin API as this client understands it.
//!
//! These types mirror the server's JSON exactly, and they are written to be
//! forgiving in the one direction that matters: a field this client does not
//! know about is ignored, and a field the server stops sending falls back to a
//! default rather than failing the whole poll. A monitoring tool that refuses
//! to start because the thing it monitors grew a field is a monitoring tool
//! that is off precisely when somebody needed it.
//!
//! The rule is not symmetric. Fields this client *displays as numbers* —
//! counters, generations — are typed, because a counter that silently defaults
//! to zero is a rate that is silently wrong.
//!
//! # The version field
//!
//! Both responses carry a top-level `version`, and it is 1. Absent means 0: a
//! daemon built before the field existed serves the same shape without it, and
//! a client that refused those would be a monitoring tool that stops working
//! against the thing it monitors — which is the failure this whole module is
//! written to avoid.
//!
//! Nothing here branches on it yet, and nothing should until there is a version
//! 2. It is parsed and carried so that the day a shape has to change
//! incompatibly, this client can tell which one it is looking at *before* it
//! parses — a discriminator added at the same time as the break is one release
//! too late to be useful.

use serde::{Deserialize, Serialize};

/// The schema version this client was written against.
///
/// Not enforced: a newer server is not a reason to refuse a poll, and an older
/// one — which sends no `version` at all — is the ordinary case during an
/// upgrade.
pub const KNOWN_VERSION: u64 = 1;

/// `GET /admin/generations`.
///
/// Generations are newest first, which is the order they are displayed in and
/// therefore the order this client relies on rather than re-sorting.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct GenerationsResponse {
    /// Schema version of this response. `0` when the server sent none, which
    /// means a build from before the field existed.
    #[serde(default)]
    pub version: u64,
    /// The generation traffic is pinned to, if the emergency brake is on.
    #[serde(default)]
    pub pinned: Option<u64>,
    /// The generation currently serving traffic.
    #[serde(default)]
    pub serving: u64,
    /// The timeline, newest first.
    #[serde(default)]
    pub generations: Vec<GenerationEntry>,
}

/// One entry in the generation timeline.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct GenerationEntry {
    /// Monotonic generation number.
    #[serde(default)]
    pub generation: u64,
    /// RFC 3339 instant the table was applied.
    #[serde(default)]
    pub applied_at: String,
    /// Whether this generation was published to the data plane.
    #[serde(default)]
    pub published: bool,
    /// Content hash of the compiled table.
    #[serde(default)]
    pub digest: String,
    /// Route count in this generation.
    #[serde(default)]
    pub routes: u64,
    /// Distinct hosts in this generation.
    #[serde(default)]
    pub hosts: u64,
    /// Certificates in this generation.
    #[serde(default)]
    pub certs: u64,
    /// What changed relative to the generation before it.
    #[serde(default)]
    pub diff: GenerationDiff,
}

impl GenerationEntry {
    /// The digest, shortened for a column that has room for a fingerprint and
    /// not for a hash.
    ///
    /// Twelve hex characters, the `git log --oneline` convention: enough to
    /// tell two generations apart by eye, short enough to sit in a row.
    pub fn short_digest(&self) -> &str {
        let end = self
            .digest
            .char_indices()
            .nth(12)
            .map_or(self.digest.len(), |(i, _)| i);
        self.digest.get(..end).unwrap_or(&self.digest)
    }
}

/// What one generation changed.
///
/// Every list is `#[serde(default)]` because an unchanged category is as
/// legitimately absent as it is legitimately empty, and the panel renders both
/// the same way.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct GenerationDiff {
    /// One line, already written by the server.
    #[serde(default)]
    pub summary: String,
    /// Routes that appeared.
    #[serde(default)]
    pub routes_added: Vec<DiffItem>,
    /// Routes that went away.
    #[serde(default)]
    pub routes_removed: Vec<DiffItem>,
    /// Routes whose backend changed.
    #[serde(default)]
    pub backends_changed: Vec<DiffItem>,
    /// Certificates that were replaced.
    #[serde(default)]
    pub certs_rotated: Vec<DiffItem>,
    /// Hosts that appeared.
    #[serde(default)]
    pub hosts_added: Vec<DiffItem>,
    /// Hosts that went away.
    #[serde(default)]
    pub hosts_removed: Vec<DiffItem>,
}

impl GenerationDiff {
    /// Every list, paired with the label the expanded view prints.
    ///
    /// One place that names them, so the panel cannot drift out of sync with
    /// the struct by forgetting a category.
    pub fn categories(&self) -> [(&'static str, &[DiffItem]); 6] {
        [
            ("routes added", &self.routes_added),
            ("routes removed", &self.routes_removed),
            ("backends changed", &self.backends_changed),
            ("certs rotated", &self.certs_rotated),
            ("hosts added", &self.hosts_added),
            ("hosts removed", &self.hosts_removed),
        ]
    }

    /// Whether there is anything to expand.
    pub fn is_empty(&self) -> bool {
        self.categories().iter().all(|(_, items)| items.is_empty())
    }
}

/// One line in a diff list.
///
/// The contract fixes these as lists but not what a list holds, and the honest
/// answer for a client is that it does not need to know: every one of them is
/// rendered as a line of text. Accepting any JSON scalar or structure and
/// rendering it compactly costs a dozen lines here and removes an entire class
/// of "the TUI stopped working when the server started sending objects".
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(transparent)]
pub struct DiffItem(pub serde_json::Value);

impl std::fmt::Display for DiffItem {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.0 {
            // A string renders as itself, without the quotes `to_string` on a
            // `Value` would add. This is the expected shape and the one that
            // has to look right.
            serde_json::Value::String(s) => f.write_str(s),
            serde_json::Value::Null => f.write_str("-"),
            other => write!(f, "{other}"),
        }
    }
}

impl From<&str> for DiffItem {
    fn from(s: &str) -> Self {
        Self(serde_json::Value::String(s.to_string()))
    }
}

/// `GET /admin/routes`.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct RoutesResponse {
    /// Schema version of this response. `0` when the server sent none, which
    /// means a build from before the field existed.
    #[serde(default)]
    pub version: u64,
    /// The generation these rows were read from.
    #[serde(default)]
    pub generation: u64,
    /// One row per route in the table.
    #[serde(default)]
    pub routes: Vec<RouteEntry>,
}

/// One route, with its lifetime counters.
///
/// The counters are cumulative. Everything the display shows as a rate is
/// computed here in the client by differencing two polls; see
/// [`crate::model`](crate::model), which is also where the reasons that
/// differencing is not trivial are written down.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct RouteEntry {
    /// The matched host, or `*` for the catch-all.
    #[serde(default)]
    pub host: String,
    /// The matched path.
    #[serde(default)]
    pub path: String,
    /// How the path is matched.
    #[serde(default)]
    pub path_type: PathType,
    /// The backend service this route sends to.
    #[serde(default)]
    pub backend: String,
    /// Ready endpoints behind that backend.
    #[serde(default)]
    pub endpoints: u64,
    /// Requests served by this route, cumulative.
    #[serde(default)]
    pub requests_total: u64,
    /// 5xx responses from this route, cumulative.
    #[serde(default)]
    pub errors_5xx_total: u64,
    /// Total upstream latency in milliseconds, cumulative.
    #[serde(default)]
    pub upstream_latency_ms_sum: f64,
    /// Observations behind that sum, cumulative.
    #[serde(default)]
    pub upstream_latency_count: u64,
    /// The canary split, if this route has one.
    #[serde(default)]
    pub canary: Option<Canary>,
    /// The canary-diverted share of the counters above, if this route has a
    /// canary.
    ///
    /// A *subset* of them and not a sibling: a canary request is counted in
    /// both, so the stable share is the difference. `None` on a route with no
    /// canary, which is why it is an `Option` rather than a zeroed struct —
    /// zeroes cannot be told apart from a canary nothing has reached yet.
    #[serde(default)]
    pub canary_stats: Option<CanaryStats>,
}

/// The canary-diverted share of one route's counters.
#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize)]
pub struct CanaryStats {
    /// Requests the canary backend served, cumulative.
    #[serde(default)]
    pub requests_total: u64,
    /// 5xx responses from the canary backend, cumulative.
    #[serde(default)]
    pub errors_5xx_total: u64,
    /// Total canary upstream latency in milliseconds, cumulative.
    #[serde(default)]
    pub upstream_latency_ms_sum: f64,
    /// Observations behind that sum, cumulative.
    #[serde(default)]
    pub upstream_latency_count: u64,
}

/// A weighted second backend.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct Canary {
    /// Where the split traffic goes.
    #[serde(default)]
    pub backend: String,
    /// The percentage it receives.
    #[serde(default)]
    pub weight_percent: u64,
}

/// How a route's path is matched.
///
/// `Other` exists because an ingress path type this client has never heard of
/// is a string to print, not a reason to fail a poll.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize)]
#[serde(into = "String")]
pub enum PathType {
    /// The path must match exactly.
    Exact,
    /// The path is a prefix.
    #[default]
    Prefix,
    /// Whatever the ingress controller decides.
    ImplementationSpecific,
    /// Something this client does not know.
    Other(String),
}

impl PathType {
    /// The name the server uses.
    pub fn as_str(&self) -> &str {
        match self {
            Self::Exact => "Exact",
            Self::Prefix => "Prefix",
            Self::ImplementationSpecific => "ImplementationSpecific",
            Self::Other(s) => s,
        }
    }

    /// The name a narrow column has room for.
    pub fn short(&self) -> &str {
        match self {
            Self::ImplementationSpecific => "ImplSpec",
            other => other.as_str(),
        }
    }
}

impl From<PathType> for String {
    fn from(value: PathType) -> Self {
        value.as_str().to_string()
    }
}

impl<'de> Deserialize<'de> for PathType {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        // Through `String` rather than a derived enum: the derive would reject
        // an unknown variant, and `#[serde(other)]` is not available for an
        // externally tagged enum deserialized from a bare string.
        let raw = String::deserialize(deserializer)?;
        Ok(match raw.as_str() {
            "Exact" => Self::Exact,
            "Prefix" => Self::Prefix,
            "ImplementationSpecific" => Self::ImplementationSpecific,
            _ => Self::Other(raw),
        })
    }
}

impl std::fmt::Display for PathType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The exact shape the contract specifies, with every field populated.
    ///
    /// `version` was added deliberately: the contract grows additively, and the
    /// fixture has to move with it or these tests stop describing what the
    /// server sends. The tests below cover the other direction too — a response
    /// with no `version`, which is every build before it was added.
    const GENERATIONS_JSON: &str = r#"{
      "version": 1,
      "pinned": 41,
      "serving": 41,
      "generations": [
        {
          "generation": 42,
          "applied_at": "2026-08-28T09:15:04Z",
          "published": false,
          "digest": "a1b2c3d4e5f60718293a4b5c6d7e8f90",
          "routes": 12,
          "hosts": 3,
          "certs": 2,
          "diff": {
            "summary": "1 route added, 1 backend changed",
            "routes_added": ["shop.example.com/checkout"],
            "routes_removed": [],
            "backends_changed": ["api.example.com/v1 -> api-v2"],
            "certs_rotated": [],
            "hosts_added": [],
            "hosts_removed": []
          }
        },
        {
          "generation": 41,
          "applied_at": "2026-08-28T09:02:00Z",
          "published": true,
          "digest": "ffee",
          "routes": 11,
          "hosts": 3,
          "certs": 2,
          "diff": {
            "summary": "initial table",
            "routes_added": ["api.example.com/v1"],
            "routes_removed": [],
            "backends_changed": [],
            "certs_rotated": [],
            "hosts_added": ["api.example.com"],
            "hosts_removed": []
          }
        }
      ]
    }"#;

    const ROUTES_JSON: &str = r#"{
      "version": 1,
      "generation": 42,
      "routes": [
        {
          "host": "api.example.com",
          "path": "/v1",
          "path_type": "Prefix",
          "backend": "api-v2",
          "endpoints": 4,
          "requests_total": 10000,
          "errors_5xx_total": 12,
          "upstream_latency_ms_sum": 51234.5,
          "upstream_latency_count": 9987,
          "canary": {"backend": "api-v3", "weight_percent": 10},
          "canary_stats": {
            "requests_total": 1000,
            "errors_5xx_total": 9,
            "upstream_latency_ms_sum": 7200.0,
            "upstream_latency_count": 998
          }
        },
        {
          "host": "*",
          "path": "/",
          "path_type": "ImplementationSpecific",
          "backend": "default-http-backend",
          "endpoints": 1,
          "requests_total": 7,
          "errors_5xx_total": 0,
          "upstream_latency_ms_sum": 14.0,
          "upstream_latency_count": 7,
          "canary": null,
          "canary_stats": null
        }
      ]
    }"#;

    #[test]
    fn the_generations_contract_parses() {
        let parsed: GenerationsResponse =
            serde_json::from_str(GENERATIONS_JSON).expect("contract shape");
        assert_eq!(parsed.version, KNOWN_VERSION);
        assert_eq!(parsed.pinned, Some(41));
        assert_eq!(parsed.serving, 41);
        assert_eq!(parsed.generations.len(), 2);

        let newest = parsed.generations.first().expect("two entries");
        assert_eq!(newest.generation, 42);
        assert!(!newest.published);
        assert_eq!(newest.routes, 12);
        assert_eq!(newest.hosts, 3);
        assert_eq!(newest.certs, 2);
        assert_eq!(newest.diff.summary, "1 route added, 1 backend changed");
        assert_eq!(newest.diff.routes_added.len(), 1);
        assert_eq!(
            newest.diff.routes_added[0].to_string(),
            "shop.example.com/checkout"
        );
        assert!(newest.diff.routes_removed.is_empty());
    }

    #[test]
    fn a_null_pin_is_not_a_pin() {
        let json = r#"{"pinned": null, "serving": 3, "generations": []}"#;
        let parsed: GenerationsResponse = serde_json::from_str(json).expect("valid");
        assert_eq!(parsed.pinned, None);
        assert_eq!(parsed.serving, 3);
    }

    #[test]
    fn a_response_with_no_version_is_read_as_the_version_before_it_existed() {
        // Every daemon built before the field was added serves exactly this,
        // and an upgrade is precisely when somebody is watching. Refusing these
        // would mean the monitoring tool stops working against the thing it
        // monitors, on the release that made it version-aware.
        let generations = r#"{"pinned": null, "serving": 3, "generations": []}"#;
        let parsed: GenerationsResponse = serde_json::from_str(generations).expect("legacy shape");
        assert_eq!(parsed.version, 0);
        assert_eq!(parsed.serving, 3);

        let routes = r#"{"generation": 3, "routes": []}"#;
        let parsed: RoutesResponse = serde_json::from_str(routes).expect("legacy shape");
        assert_eq!(parsed.version, 0);
        assert_eq!(parsed.generation, 3);
    }

    #[test]
    fn a_version_from_the_future_is_parsed_rather_than_refused() {
        // A newer daemon is not a reason to stop polling. If a version 2 ever
        // changes a field's meaning, *that* is when this client learns to
        // branch; until then, refusing on the number alone would break every
        // pairing where the daemon was upgraded first.
        let json = r#"{"version": 99, "generation": 7, "routes": []}"#;
        let parsed: RoutesResponse = serde_json::from_str(json).expect("still readable");
        assert_eq!(parsed.version, 99);
        assert_eq!(parsed.generation, 7);
    }

    #[test]
    fn the_routes_contract_parses() {
        let parsed: RoutesResponse = serde_json::from_str(ROUTES_JSON).expect("contract shape");
        assert_eq!(parsed.version, KNOWN_VERSION);
        assert_eq!(parsed.generation, 42);
        assert_eq!(parsed.routes.len(), 2);

        let api = &parsed.routes[0];
        assert_eq!(api.host, "api.example.com");
        assert_eq!(api.path, "/v1");
        assert_eq!(api.path_type, PathType::Prefix);
        assert_eq!(api.backend, "api-v2");
        assert_eq!(api.endpoints, 4);
        assert_eq!(api.requests_total, 10_000);
        assert_eq!(api.errors_5xx_total, 12);
        assert!((api.upstream_latency_ms_sum - 51_234.5).abs() < f64::EPSILON);
        assert_eq!(api.upstream_latency_count, 9987);
        let canary = api.canary.as_ref().expect("a canary");
        assert_eq!(canary.backend, "api-v3");
        assert_eq!(canary.weight_percent, 10);

        let split = api.canary_stats.as_ref().expect("a canary split");
        assert_eq!(split.requests_total, 1000);
        assert_eq!(split.errors_5xx_total, 9);
        assert_eq!(split.upstream_latency_count, 998);
        assert!(
            split.requests_total < api.requests_total,
            "the split is a subset of the route's totals, not a sibling of them"
        );

        let fallback = &parsed.routes[1];
        assert_eq!(fallback.host, "*");
        assert_eq!(fallback.path_type, PathType::ImplementationSpecific);
        assert!(fallback.canary.is_none());
        assert!(
            fallback.canary_stats.is_none(),
            "no canary means no split to report"
        );
    }

    #[test]
    fn an_unknown_path_type_is_carried_through_rather_than_rejected() {
        let json = r#"{"generation":1,"routes":[{"host":"a","path":"/","path_type":"RegexSomeday","backend":"b"}]}"#;
        let parsed: RoutesResponse = serde_json::from_str(json).expect("tolerated");
        assert_eq!(
            parsed.routes[0].path_type,
            PathType::Other("RegexSomeday".to_string())
        );
        assert_eq!(parsed.routes[0].path_type.short(), "RegexSomeday");
    }

    #[test]
    fn a_field_this_client_has_never_heard_of_is_ignored() {
        let json = r#"{"generation":1,"routes":[
            {"host":"a","path":"/","path_type":"Exact","backend":"b","weight_class":"gold"}
        ],"served_by":"someone-else"}"#;
        let parsed: RoutesResponse = serde_json::from_str(json).expect("forward compatible");
        assert_eq!(parsed.routes[0].backend, "b");
    }

    #[test]
    fn missing_counters_default_to_zero_rather_than_failing_the_poll() {
        let json = r#"{"generation":1,"routes":[{"host":"a","path":"/","backend":"b"}]}"#;
        let parsed: RoutesResponse = serde_json::from_str(json).expect("defaults");
        let route = &parsed.routes[0];
        assert_eq!(route.requests_total, 0);
        assert_eq!(route.upstream_latency_count, 0);
        assert_eq!(route.path_type, PathType::Prefix, "the documented default");
    }

    #[test]
    fn diff_items_that_are_not_strings_still_render() {
        let json = r#"{"summary":"x","routes_added":[{"host":"a","path":"/b"}, 7, null]}"#;
        let diff: GenerationDiff = serde_json::from_str(json).expect("any JSON");
        let rendered: Vec<String> = diff.routes_added.iter().map(ToString::to_string).collect();
        assert_eq!(rendered[0], r#"{"host":"a","path":"/b"}"#);
        assert_eq!(rendered[1], "7");
        assert_eq!(rendered[2], "-");
    }

    #[test]
    fn a_string_diff_item_renders_without_quotes() {
        assert_eq!(DiffItem::from("api/v1").to_string(), "api/v1");
    }

    #[test]
    fn digests_are_shortened_to_a_fingerprint() {
        let entry = GenerationEntry {
            digest: "a1b2c3d4e5f60718293a4b5c6d7e8f90".to_string(),
            ..Default::default()
        };
        assert_eq!(entry.short_digest(), "a1b2c3d4e5f6");

        let short = GenerationEntry {
            digest: "ffee".to_string(),
            ..Default::default()
        };
        assert_eq!(short.short_digest(), "ffee", "already shorter than the cap");
    }

    #[test]
    fn every_diff_category_is_reachable_from_categories() {
        let diff = GenerationDiff {
            summary: "everything".to_string(),
            routes_added: vec!["ra".into()],
            routes_removed: vec!["rr".into()],
            backends_changed: vec!["bc".into()],
            certs_rotated: vec!["cr".into()],
            hosts_added: vec!["ha".into()],
            hosts_removed: vec!["hr".into()],
        };
        assert!(!diff.is_empty());
        let labels: Vec<&str> = diff.categories().iter().map(|(label, _)| *label).collect();
        assert_eq!(
            labels,
            [
                "routes added",
                "routes removed",
                "backends changed",
                "certs rotated",
                "hosts added",
                "hosts removed"
            ]
        );
        assert!(diff.categories().iter().all(|(_, items)| items.len() == 1));
        assert!(GenerationDiff::default().is_empty());
    }
}
