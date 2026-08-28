//! The admin listener: `/metrics`, the probes, and the generation API.
//!
//! These live on their own port (`:10254`, the ingress-nginx convention) rather
//! than on a reserved path of the data plane, for two reasons that are really
//! the same reason. A path on the data plane is a path an Ingress can claim, so
//! `/metrics` would either shadow somebody's application route or be shadowed
//! by it depending on precedence — and it would be reachable from the internet,
//! which is a way to tell an attacker your request rate. A separate port is
//! bound to the pod and scraped by things inside the cluster.
//!
//! # Liveness and readiness are not the same question
//!
//! `/healthz` answers "is this process working?" and is unconditionally 200 as
//! long as the server is answering at all — a liveness probe that fails
//! restarts the pod, so anything conditional in it turns a transient dependency
//! problem into a crash loop.
//!
//! `/readyz` answers "should this pod receive traffic?" and is gated on a
//! [`ReadinessFlag`] the owner of the route table sets. That is what keeps a
//! freshly started replica out of the Service until it has an actual route
//! table: without it, a rolling update briefly routes traffic to a pod whose
//! table is empty, and every request in that window is a 404.
//!
//! # The trust model, because two of these endpoints change what is served
//!
//! `POST /admin/rollback` republishes an old generation and `DELETE` releases
//! it. There is no authentication here and there is not going to be: the
//! listener is bound to the pod and exposed through a ClusterIP Service, the
//! chart never puts it behind an Ingress or a LoadBalancer, and anything that
//! can reach it can already reach the API server's Service account token on the
//! same pod. Adding a shared secret to a port that is reachable only from
//! inside the cluster would be a login screen on a door that is already in a
//! locked building.
//!
//! What *is* enforced is the shape: the mutating endpoints answer to `POST` and
//! `DELETE` and nothing else. A `GET` cannot change what this replica serves,
//! so a link, a browser prefetch, a scraper following URLs, or a health checker
//! walking paths cannot roll a cluster back by accident.
//!
//! # Why the per-route data is JSON and not Prometheus
//!
//! `/admin/routes` reports counters per route, and those counters are
//! deliberately *not* exported as labelled series. ingress-nginx does export
//! them, and it is the single most common reason its metrics endpoint becomes
//! the most expensive request the pod serves: a cluster with ten thousand
//! routes turns one scrape into ten thousand series, every fifteen seconds,
//! forever, whether or not anybody looks at them. Here `/metrics` keeps its
//! fixed, small set of series and the per-route numbers are fetched by
//! something that asked for them.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use bytes::Bytes;
use http::{header, HeaderValue, Method, Request, Response, StatusCode};
use http_body_util::{BodyExt, Limited};
use hyper::body::Incoming;
use ramjet_router::{RouteTable, SharedRouteTable};
use serde_json::{json, Value};

use crate::body::ProxyBody;
use crate::history::{self, GenerationHistory, PinError};
use crate::metrics::Metrics;

/// The exposition format version Prometheus expects to negotiate.
const PROMETHEUS_CONTENT_TYPE: &str = "text/plain; version=0.0.4; charset=utf-8";

/// Largest request body the rollback endpoint will read.
///
/// The body is one small object. A cap is here because reading an unbounded
/// body from a socket is how an endpoint with no other resource cost acquires
/// one.
const MAX_BODY: usize = 4 * 1024;

/// Whether this replica should receive traffic.
///
/// Cheap to clone — it is an `Arc<AtomicBool>` — so the controller, the daemon,
/// and the admin listener can each hold one without threading a reference
/// through everything in between.
#[derive(Debug, Clone, Default)]
pub struct ReadinessFlag {
    ready: Arc<AtomicBool>,
}

impl ReadinessFlag {
    /// A flag that starts out not ready.
    pub fn new() -> Self {
        Self::default()
    }

    /// Marks the replica ready, or takes it back out of rotation.
    ///
    /// `Release`/`Acquire` rather than `Relaxed`: setting this publishes
    /// everything the caller did to make the replica ready — most importantly
    /// the first route table — and a probe that observed `true` must observe
    /// that work too.
    pub fn set_ready(&self, ready: bool) {
        self.ready.store(ready, Ordering::Release);
    }

    /// Whether the replica is currently ready.
    pub fn is_ready(&self) -> bool {
        self.ready.load(Ordering::Acquire)
    }
}

/// What the admin endpoints read.
#[derive(Debug)]
pub struct AdminState {
    /// Data-plane counters.
    pub metrics: Arc<Metrics>,
    /// The published table: its generation at scrape time, and its routes for
    /// `/admin/routes`.
    pub routes: Arc<SharedRouteTable>,
    /// Gates `/readyz`.
    pub readiness: ReadinessFlag,
    /// The generations this replica has applied, and the publication gate.
    pub history: Arc<GenerationHistory>,
}

/// Answers one admin request.
pub async fn handle(state: Arc<AdminState>, request: Request<Incoming>) -> Response<ProxyBody> {
    let path = request.uri().path().to_owned();

    // Split by mutating and not, rather than by path first: the property worth
    // enforcing is that nothing reachable with a `GET` changes what is served.
    if path == "/admin/rollback" {
        return match *request.method() {
            Method::POST => rollback(&state, request).await,
            Method::DELETE => resume(&state),
            _ => text(
                StatusCode::METHOD_NOT_ALLOWED,
                "method not allowed: POST to pin a generation, DELETE to release it\n",
            ),
        };
    }

    // A scrape or a probe is a GET; anything else against these paths is a
    // misconfiguration worth naming rather than quietly serving.
    if request.method() != Method::GET && request.method() != Method::HEAD {
        return text(StatusCode::METHOD_NOT_ALLOWED, "method not allowed\n");
    }

    match path.as_str() {
        "/metrics" => {
            let body = state
                .metrics
                .render_prometheus(state.routes.generation(), state.history.pinned().is_some());
            let mut response = Response::new(ProxyBody::once(Bytes::from(body)));
            response.headers_mut().insert(
                header::CONTENT_TYPE,
                HeaderValue::from_static(PROMETHEUS_CONTENT_TYPE),
            );
            response
        }
        "/healthz" => text(StatusCode::OK, "ok\n"),
        "/readyz" => {
            if state.readiness.is_ready() {
                text(StatusCode::OK, "ready\n")
            } else {
                text(StatusCode::SERVICE_UNAVAILABLE, "not ready\n")
            }
        }
        "/admin/generations" => json(StatusCode::OK, generations(&state)),
        "/admin/routes" => json(StatusCode::OK, routes(&state)),
        _ => text(StatusCode::NOT_FOUND, "not found\n"),
    }
}

/// `GET /admin/generations` — what this replica has applied, newest first.
fn generations(state: &AdminState) -> Value {
    state.history.with_records(|pinned, ring| {
        let generations: Vec<Value> = ring
            .iter()
            .rev()
            .map(|record| {
                json!({
                    "generation": record.generation,
                    "applied_at": history::rfc3339(record.applied_at),
                    "published": record.published,
                    // Hex, and fixed width: the digest is an opaque identity to
                    // compare between replicas, not a number to do arithmetic
                    // on, and a decimal u64 invites the reader to treat it as
                    // one.
                    "digest": format!("{:016x}", record.digest),
                    "routes": record.routes(),
                    "hosts": record.hosts(),
                    "certs": record.certs(),
                    "diff": *record.diff,
                })
            })
            .collect();
        json!({
            "pinned": pinned,
            "serving": state.routes.generation(),
            "generations": generations,
        })
    })
}

/// `GET /admin/routes` — every route in the serving table, with its counters.
///
/// Sorted by host and path rather than served in table order: the table's hosts
/// live in a hash map, so "table order" changes from one generation to the
/// next, and anything rendering this repeatedly would show a list that
/// reshuffles itself on every rebuild.
fn routes(state: &AdminState) -> Value {
    let table: Arc<RouteTable> = state.routes.load_full();
    let stats = table.route_stats();

    let mut routes: Vec<(String, &str, Value)> = table
        .routes()
        .map(|(host, rule)| {
            let host = host.to_string();
            let totals = stats.slot(rule.stats_index()).map(|slot| slot.totals());
            let totals = totals.unwrap_or_default();
            let backend = table.backend(rule.backend());

            let canary = rule.canary().map(|canary| {
                json!({
                    "backend": table.backend(canary.backend()).map(|b| b.name()).unwrap_or(""),
                    "weight_percent": canary.weight_percent(),
                })
            });

            let value = json!({
                "host": host,
                "path": rule.path(),
                "path_type": rule.path_type().as_str(),
                "backend": backend.map(|b| b.name()).unwrap_or(""),
                "endpoints": backend.map_or(0, |b| b.endpoints().len()),
                "requests_total": totals.requests,
                "errors_5xx_total": totals.errors_5xx,
                "upstream_latency_ms_sum": totals.upstream_latency_ms(),
                "upstream_latency_count": totals.upstream_latency_count,
                "canary": canary,
            });
            (host, rule.path(), value)
        })
        .collect();
    routes.sort_by(|a, b| (&a.0, a.1).cmp(&(&b.0, b.1)));

    json!({
        "generation": table.generation(),
        "routes": routes.into_iter().map(|(_, _, value)| value).collect::<Vec<_>>(),
    })
}

/// `POST /admin/rollback` — republish a generation and hold publication there.
async fn rollback(state: &AdminState, request: Request<Incoming>) -> Response<ProxyBody> {
    let body = match Limited::new(request.into_body(), MAX_BODY).collect().await {
        Ok(collected) => collected.to_bytes(),
        Err(_) => {
            return json(
                StatusCode::BAD_REQUEST,
                json!({ "error": "could not read the request body" }),
            )
        }
    };

    let generation = match serde_json::from_slice::<Value>(&body)
        .ok()
        .as_ref()
        .and_then(|value| value.get("generation"))
        .and_then(Value::as_u64)
    {
        Some(generation) => generation,
        None => {
            return json(
                StatusCode::BAD_REQUEST,
                json!({ "error": "body must be an object with a numeric `generation`" }),
            )
        }
    };

    match state.history.pin(generation) {
        Ok(()) => json(StatusCode::OK, json!({ "pinned": generation })),
        Err(error @ PinError::Unknown(_)) => json(
            StatusCode::NOT_FOUND,
            json!({ "error": error.to_string(), "generation": generation }),
        ),
        Err(error @ PinError::AlreadyPinned(pinned)) => json(
            StatusCode::CONFLICT,
            json!({ "error": error.to_string(), "pinned": pinned }),
        ),
    }
}

/// `DELETE /admin/rollback` — release the pin and publish the newest
/// generation. Idempotent.
fn resume(state: &AdminState) -> Response<ProxyBody> {
    state.history.unpin();
    json(StatusCode::OK, json!({ "pinned": Value::Null }))
}

fn json(status: StatusCode, value: Value) -> Response<ProxyBody> {
    let body = serde_json::to_vec(&value).unwrap_or_else(|_| b"{}".to_vec());
    let mut response = Response::new(ProxyBody::once(Bytes::from(body)));
    *response.status_mut() = status;
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/json"),
    );
    response
}

fn text(status: StatusCode, body: &'static str) -> Response<ProxyBody> {
    let mut response = Response::new(ProxyBody::once(Bytes::from_static(body.as_bytes())));
    *response.status_mut() = status;
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("text/plain; charset=utf-8"),
    );
    response
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tls::CertStore;
    use ramjet_router::{Endpoint, LbPolicy, PathType, RouteTableBuilder};

    #[test]
    fn readiness_starts_false_and_is_shared_by_clones() {
        let flag = ReadinessFlag::new();
        assert!(!flag.is_ready(), "a fresh replica has no route table yet");

        let copy = flag.clone();
        flag.set_ready(true);
        assert!(copy.is_ready(), "clones must observe the same flag");

        copy.set_ready(false);
        assert!(!flag.is_ready());
    }

    /// A table with one plain route, one canary, and one wildcard host, which
    /// is enough to exercise every field `/admin/routes` reports.
    fn table(generation: u64) -> Arc<RouteTable> {
        let mut builder = RouteTableBuilder::new();
        builder.generation(generation);
        builder
            .backend(
                "prod/api:80",
                LbPolicy::RoundRobin,
                vec![
                    Endpoint::new("10.0.0.1:8080".parse().expect("an address")),
                    Endpoint::new("10.0.0.2:8080".parse().expect("an address")),
                ],
            )
            .expect("registers");
        builder
            .backend(
                "prod/api-canary:80",
                LbPolicy::RoundRobin,
                vec![Endpoint::new("10.0.0.3:8080".parse().expect("an address"))],
            )
            .expect("registers");
        builder
            .route(Some("example.com"), "/", PathType::Prefix, "prod/api:80")
            .expect("drafts");
        builder
            .canary_route(
                Some("*.example.com"),
                "/v2",
                PathType::Exact,
                "prod/api:80",
                &ramjet_router::CanaryRules {
                    backend: "prod/api-canary:80",
                    weight: 25,
                    ..Default::default()
                },
            )
            .expect("drafts");
        Arc::new(builder.build().expect("builds"))
    }

    fn state(table: Arc<RouteTable>) -> Arc<AdminState> {
        let routes = Arc::new(SharedRouteTable::new(
            RouteTableBuilder::new().build().expect("an empty table"),
        ));
        let certs = Arc::new(CertStore::new());
        let history = Arc::new(GenerationHistory::new(
            Arc::clone(&routes),
            certs,
            10,
        ));
        history.record(
            table.generation(),
            0xdead_beef,
            Arc::new(json!({ "summary": "2 routes added" })),
            table,
            Arc::new(crate::history::CertKeys::new()),
        );
        Arc::new(AdminState {
            metrics: Arc::new(Metrics::new()),
            routes,
            readiness: ReadinessFlag::new(),
            history,
        })
    }

    #[test]
    fn generations_reports_the_ring_newest_first() {
        let state = state(table(7));
        state.history.record(
            8,
            0x1234,
            Arc::new(json!({ "summary": "no change" })),
            table(8),
            Arc::new(crate::history::CertKeys::new()),
        );

        let body = generations(&state);
        assert_eq!(body["pinned"], Value::Null);
        assert_eq!(body["serving"], 8);

        let listed = body["generations"].as_array().expect("an array");
        assert_eq!(listed.len(), 2);
        assert_eq!(listed[0]["generation"], 8, "newest first");
        assert_eq!(listed[1]["generation"], 7);
        assert_eq!(listed[1]["digest"], "00000000deadbeef");
        assert_eq!(listed[1]["published"], true);
        assert_eq!(listed[1]["routes"], 2);
        assert_eq!(listed[1]["hosts"], 2);
        assert_eq!(listed[1]["certs"], 0);
        assert_eq!(listed[1]["diff"]["summary"], "2 routes added");
        assert!(
            listed[0]["applied_at"].as_str().is_some_and(|t| t.ends_with('Z')),
            "applied_at must be RFC 3339 UTC"
        );
    }

    #[test]
    fn a_pin_shows_up_in_the_listing() {
        let state = state(table(7));
        state.history.record(
            8,
            0,
            Arc::new(json!({})),
            table(8),
            Arc::new(crate::history::CertKeys::new()),
        );
        state.history.pin(7).expect("pins");

        let body = generations(&state);
        assert_eq!(body["pinned"], 7);
        assert_eq!(body["serving"], 7, "serving is what is on the wire, not what was built");
    }

    #[test]
    fn routes_reports_every_field_of_the_contract() {
        let state = state(table(7));
        let body = routes(&state);
        assert_eq!(body["generation"], 7);

        let listed = body["routes"].as_array().expect("an array");
        assert_eq!(listed.len(), 2);

        // Sorted by host, so the wildcard's `*.example.com` sorts first.
        assert_eq!(listed[0]["host"], "*.example.com");
        assert_eq!(listed[0]["path"], "/v2");
        assert_eq!(listed[0]["path_type"], "Exact");
        assert_eq!(listed[0]["backend"], "prod/api:80");
        assert_eq!(listed[0]["endpoints"], 2);
        assert_eq!(listed[0]["canary"]["backend"], "prod/api-canary:80");
        assert_eq!(listed[0]["canary"]["weight_percent"], 25);

        assert_eq!(listed[1]["host"], "example.com");
        assert_eq!(listed[1]["path_type"], "Prefix");
        assert_eq!(listed[1]["canary"], Value::Null);
        assert_eq!(listed[1]["requests_total"], 0);
        assert_eq!(listed[1]["errors_5xx_total"], 0);
        assert_eq!(listed[1]["upstream_latency_ms_sum"], 0.0);
        assert_eq!(listed[1]["upstream_latency_count"], 0);
    }

    #[test]
    fn route_counters_reach_the_listing() {
        let state = state(table(7));
        let table = state.routes.load_full();
        let (_, rule) = table
            .routes()
            .find(|(_, rule)| rule.path() == "/")
            .expect("the route is in the table");
        let slot = table
            .route_stats()
            .slot(rule.stats_index())
            .expect("a counter block");
        slot.shard(0).record_response(200);
        slot.shard(1).record_response(503);
        slot.shard(1)
            .record_upstream_latency(std::time::Duration::from_micros(1500));

        let body = routes(&state);
        let listed = body["routes"].as_array().expect("an array");
        let route = listed
            .iter()
            .find(|route| route["path"] == "/")
            .expect("the route is listed");
        assert_eq!(route["requests_total"], 2, "shards are summed");
        assert_eq!(route["errors_5xx_total"], 1);
        assert_eq!(route["upstream_latency_ms_sum"], 1.5, "microseconds render as milliseconds");
        assert_eq!(route["upstream_latency_count"], 1);
    }
}
