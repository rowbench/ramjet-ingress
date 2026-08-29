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
//! it. Three things stand between that and an accident or an attacker, and they
//! are deliberately different in kind.
//!
//! **The shape.** The mutating endpoints answer to `POST` and `DELETE` and
//! nothing else. A `GET` cannot change what this replica serves, so a link, a
//! browser prefetch, a scraper following URLs, or a health checker walking paths
//! cannot roll a cluster back by accident. This is unconditional and needs no
//! configuration.
//!
//! **The network.** The listener is bound to the pod and exposed through a
//! ClusterIP Service; the chart never puts it behind an Ingress or a
//! LoadBalancer. That bounds the reachable set to the cluster, and the chart's
//! optional `networkPolicy` narrows it further to the release namespace.
//!
//! **A bearer token**, when [`AdminState::auth`] carries one. The earlier
//! argument here was that a secret on a port reachable only from inside the
//! cluster is a login screen on a door in a locked building, and that argument
//! was wrong in one specific way: "inside the cluster" is every pod in every
//! namespace, including the ones running somebody else's code. A pod that can
//! reach this port cannot read the API server token on *our* pod — it is on our
//! filesystem, not on the network — so the only thing that was stopping any
//! workload in the cluster from rolling back the ingress table was that it had
//! not thought of it. With a token configured, every mutating `/admin/` request
//! must carry `Authorization: Bearer <token>`; without one, they are refused
//! with 401 and nothing changes.
//!
//! What the token deliberately does **not** cover is `GET`: `/metrics` is
//! scraped by Prometheus and `/healthz` and `/readyz` are called by the kubelet,
//! and none of the three can be taught to send a header. Gating them would trade
//! a rollback an attacker has to reach the port to perform for a pod that
//! restarts every time its liveness probe is refused. `/admin/generations` and
//! `/admin/routes` stay open for the same reason they are `GET` at all: they
//! report what this replica is serving, which is not a secret from anything that
//! can already send it traffic.
//!
//! Rotation is a restart. The token is read once at startup rather than on every
//! request — a per-request `read(2)` on the mutating path would be a syscall
//! bought to make a yearly event convenient — so replacing the Secret means
//! rolling the Deployment, which is one command and the thing an operator was
//! going to do anyway.
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
//!
//! The shape of that JSON is a contract `ramjet-top` parses, so it grows
//! additively and never otherwise: `canary_stats` and `mirror` were added
//! after the fact and are `null` on a route that has neither, which is a case
//! every existing reader already handles because `canary` has always been
//! nullable.
//!
//! Both JSON endpoints carry a top-level `"version"`, which is
//! [`API_VERSION`] and is 1. It exists for the day the additive rule has to be
//! broken — a field whose meaning changes rather than a field that appears —
//! because on that day a reader needs to be able to tell the two shapes apart
//! before parsing, and adding the discriminator at the same time as the break
//! is one release too late. Until then it is a constant, and a reader that
//! ignores it is correct.
//!
//! A reader must treat *absent* as version 0: every build before this one
//! served the same shape without the field, and refusing to parse those would
//! make a monitoring tool stop working against the thing it monitors.

use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use bytes::Bytes;
use http::{header, HeaderMap, HeaderValue, Method, Request, Response, StatusCode};
use http_body_util::{BodyExt, Limited};
use hyper::body::Incoming;
use ramjet_router::{RouteSlot, RouteTable, SharedRouteTable};
use serde_json::{json, Value};
use subtle::ConstantTimeEq;

use crate::body::ProxyBody;
use crate::history::{self, GenerationHistory, PinError};
use crate::metrics::Exposition;

/// The exposition format version Prometheus expects to negotiate.
const PROMETHEUS_CONTENT_TYPE: &str = "text/plain; version=0.0.4; charset=utf-8";

/// Schema version of the JSON `/admin/generations` and `/admin/routes` serve.
///
/// Bumped only for a change a reader written against the previous version could
/// not survive — a field whose meaning changed, or one that went away. Adding a
/// field does not bump it, because a reader that ignores unknown fields is
/// unaffected and one that does not was already broken.
pub const API_VERSION: u64 = 1;

/// Largest request body the rollback endpoint will read.
///
/// The body is one small object. A cap is here because reading an unbounded
/// body from a socket is how an endpoint with no other resource cost acquires
/// one.
const MAX_BODY: usize = 4 * 1024;

/// The scheme name in `Authorization`, matched case-insensitively as RFC 7235
/// requires.
const BEARER: &str = "Bearer ";

/// Paths a bearer token is never required on, whatever the method.
///
/// The three whose callers cannot be taught to send a header: `/metrics` is
/// scraped by Prometheus, and `/healthz` and `/readyz` are called by the
/// kubelet. Everything else is gated by method — see [`handle`] for why the rule
/// is an exemption list rather than an `/admin/` prefix.
///
/// A non-`GET` to one of these is a 405 further down regardless, so exempting
/// them costs nothing beyond the exemption itself.
const UNGATED: [&str; 3] = ["/metrics", "/healthz", "/readyz"];

/// Why a token file could not be turned into an [`AdminAuth`].
#[derive(Debug)]
pub enum TokenError {
    /// The file could not be read.
    Unreadable(std::io::Error),
    /// The file held nothing but whitespace.
    ///
    /// Refused rather than accepted as an empty token, which would mean every
    /// mutating request had to send the header `Authorization: Bearer ` and
    /// exactly that — authentication that looks configured, passes a smoke
    /// test, and protects nothing an attacker could not also type.
    Empty,
}

impl std::fmt::Display for TokenError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TokenError::Unreadable(error) => write!(f, "cannot read the admin token file: {error}"),
            TokenError::Empty => f.write_str(
                "the admin token file is empty; a blank token would authenticate \
                 anything that sends an empty bearer header",
            ),
        }
    }
}

impl std::error::Error for TokenError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            TokenError::Unreadable(error) => Some(error),
            TokenError::Empty => None,
        }
    }
}

/// The bearer token the mutating admin endpoints require.
///
/// Cheap to clone. Read once, at startup — see the module docs on rotation.
#[derive(Clone)]
pub struct AdminAuth {
    token: Arc<[u8]>,
}

impl std::fmt::Debug for AdminAuth {
    /// Deliberately says only that there is one.
    ///
    /// `AdminState` derives `Debug` and is logged at startup on two of the three
    /// paths that build one. A derived field here would put the token in the
    /// pod's logs, where it outlives the process and reaches whatever ships them.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("AdminAuth(<redacted>)")
    }
}

impl AdminAuth {
    /// Reads the token from `path`.
    ///
    /// Trailing whitespace is trimmed, because a Secret written by `echo` or by
    /// a person's editor ends in a newline and refusing that would be a
    /// half-hour nobody gets back. Interior bytes are taken as they are.
    ///
    /// # Errors
    ///
    /// The file could not be read, or held nothing but whitespace.
    pub fn from_file(path: &Path) -> Result<Self, TokenError> {
        let raw = std::fs::read(path).map_err(TokenError::Unreadable)?;
        let trimmed: &[u8] = {
            let start = raw.iter().position(|b| !b.is_ascii_whitespace());
            let end = raw.iter().rposition(|b| !b.is_ascii_whitespace());
            match (start, end) {
                (Some(start), Some(end)) => &raw[start..=end],
                _ => &[],
            }
        };
        if trimmed.is_empty() {
            return Err(TokenError::Empty);
        }
        Ok(AdminAuth {
            token: Arc::from(trimmed),
        })
    }

    /// A token supplied directly, for tests and embedders.
    ///
    /// # Errors
    ///
    /// The token was empty.
    pub fn from_token(token: &str) -> Result<Self, TokenError> {
        let trimmed = token.trim();
        if trimmed.is_empty() {
            return Err(TokenError::Empty);
        }
        Ok(AdminAuth {
            token: Arc::from(trimmed.as_bytes()),
        })
    }

    /// Whether these headers carry the right bearer token.
    ///
    /// The comparison is constant-time in the value, so a caller cannot find the
    /// token one byte at a time by timing the refusals. It is *not* constant-time
    /// in the length — `ct_eq` on unequal slices is a length check — which leaks
    /// how long the token is and nothing about what it says.
    fn authorizes(&self, headers: &HeaderMap) -> bool {
        let Some(value) = headers.get(header::AUTHORIZATION) else {
            return false;
        };
        let Ok(value) = value.to_str() else {
            return false;
        };
        // Case-insensitive on the scheme only. RFC 7235 makes the scheme name
        // case-insensitive and the credentials opaque, and a client library
        // that sends `bearer` is not wrong.
        let Some(offered) = value
            .get(..BEARER.len())
            .filter(|prefix| prefix.eq_ignore_ascii_case(BEARER))
            .and_then(|_| value.get(BEARER.len()..))
        else {
            return false;
        };
        offered.as_bytes().ct_eq(&self.token).into()
    }
}

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
    /// Data-plane counters, whichever engine is producing them.
    ///
    /// Boxed as a trait object rather than generic: this is read once per
    /// scrape, a few times a minute, and making every caller carry an engine
    /// parameter to save a virtual call there would be the wrong trade.
    pub metrics: Arc<dyn Exposition>,
    /// The published table: its generation at scrape time, and its routes for
    /// `/admin/routes`.
    pub routes: Arc<SharedRouteTable>,
    /// Gates `/readyz`.
    pub readiness: ReadinessFlag,
    /// The generations this replica has applied, and the publication gate.
    pub history: Arc<GenerationHistory>,
    /// The bearer token mutating `/admin/` requests must carry, or `None` to
    /// accept them from anything that can reach the port.
    ///
    /// See the module docs for what this covers and what it deliberately does
    /// not.
    pub auth: Option<AdminAuth>,
}

/// Answers one admin request.
pub async fn handle(state: Arc<AdminState>, request: Request<Incoming>) -> Response<ProxyBody> {
    let path = request.uri().path().to_owned();

    // Gated by method, with three paths exempted by name rather than gated by a
    // prefix. The difference matters for the endpoint nobody has written yet: a
    // mutating handler added anywhere — under `/admin/` or not — is covered by
    // default, where a prefix test would have covered it only if somebody
    // remembered where to put it. The exemptions are exactly the three callers
    // that cannot send a header, and adding a fourth is a line somebody has to
    // type here.
    //
    // The cost is that `POST /admin/nonsense` is a 401 before it is a 404,
    // which is the right way round: an unauthenticated caller learns nothing
    // about which admin endpoints exist.
    if mutates(request.method()) && !UNGATED.contains(&path.as_str()) {
        if let Some(auth) = &state.auth {
            if !auth.authorizes(request.headers()) {
                return unauthorized();
            }
        }
    }

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
            "version": API_VERSION,
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
            let slot = stats.slot(rule.stats_index());
            let totals = slot.map(RouteSlot::totals).unwrap_or_default();
            let backend = table.backend(rule.backend());

            let canary = rule.canary().map(|canary| {
                json!({
                    "backend": table.backend(canary.backend()).map(|b| b.name()).unwrap_or(""),
                    "weight_percent": canary.weight_percent(),
                })
            });

            // Reported only for a route that has a canary. On a route without
            // one the block is unconditionally zero, and a reader cannot tell
            // "no canary" from "a canary nothing has reached yet" if both come
            // back as the same object full of zeroes.
            //
            // These are a *subset* of the fields above, not a sibling of them:
            // a canary request is counted in both, so stable traffic is the
            // difference. See `RouteSlot` for why it is arranged that way.
            let canary_stats = rule.canary().and(slot).map(|slot| {
                let totals = slot.canary_totals();
                json!({
                    "requests_total": totals.requests,
                    "errors_5xx_total": totals.errors_5xx,
                    "upstream_latency_ms_sum": totals.upstream_latency_ms(),
                    "upstream_latency_count": totals.upstream_latency_count,
                })
            });

            let mirror = rule.mirror().map(|mirror| {
                json!({
                    "backend": table.backend(mirror.backend()).map(|b| b.name()).unwrap_or(""),
                    "percent": mirror.percent(),
                    "host": mirror.host(),
                })
            });

            let value = json!({
                "host": host,
                "path": rule.path(),
                "path_type": rule.path_type().as_str(),
                "backend": backend.map(|b| b.name()).unwrap_or(""),
                "endpoints": backend.map_or(0, |b| b.endpoints().len()),
                // The only way to confirm from outside a running pod that
                // `backend-protocol` took effect. It is not visible in any
                // counter, and a route dialled with the wrong one fails at the
                // far end where nothing here can see it.
                "protocol": backend.map_or("", |b| b.protocol().as_str()),
                "requests_total": totals.requests,
                "errors_5xx_total": totals.errors_5xx,
                "upstream_latency_ms_sum": totals.upstream_latency_ms(),
                "upstream_latency_count": totals.upstream_latency_count,
                "canary": canary,
                "canary_stats": canary_stats,
                "mirror": mirror,
            });
            (host, rule.path(), value)
        })
        .collect();
    routes.sort_by(|a, b| (&a.0, a.1).cmp(&(&b.0, b.1)));

    json!({
        "version": API_VERSION,
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

/// Whether this method can change what the replica serves.
///
/// Everything that is not a read. `GET` and `HEAD` are the two the rest of this
/// module answers, and listing the safe ones rather than the unsafe ones means a
/// method nobody thought about is treated as a write.
fn mutates(method: &Method) -> bool {
    !matches!(*method, Method::GET | Method::HEAD)
}

/// The refusal when a mutating request carries no usable token.
///
/// `WWW-Authenticate` because a 401 without one is not a 401 an HTTP client can
/// act on, and because it is how `curl --oauth2-bearer` and every other tool
/// learns what to send. The body says nothing about whether the token was
/// missing, malformed, or wrong: all three are the same answer to whoever sent
/// it, and distinguishing them is free information for something guessing.
fn unauthorized() -> Response<ProxyBody> {
    let mut response = text(
        StatusCode::UNAUTHORIZED,
        "unauthorized: this endpoint requires Authorization: Bearer <token>\n",
    );
    response.headers_mut().insert(
        header::WWW_AUTHENTICATE,
        HeaderValue::from_static("Bearer"),
    );
    response
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

    fn bearer(value: &str) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(
            header::AUTHORIZATION,
            HeaderValue::from_str(value).expect("a header value"),
        );
        headers
    }

    #[test]
    fn only_the_configured_token_authorizes() {
        let auth = AdminAuth::from_token("s3cret").expect("a token");
        assert!(auth.authorizes(&bearer("Bearer s3cret")));
        assert!(!auth.authorizes(&bearer("Bearer s3cre")), "a prefix is not the token");
        assert!(!auth.authorizes(&bearer("Bearer s3crett")));
        assert!(!auth.authorizes(&bearer("Bearer ")));
        assert!(!auth.authorizes(&HeaderMap::new()), "no header at all");
    }

    #[test]
    fn the_scheme_is_case_insensitive_and_the_token_is_not() {
        // RFC 7235: the scheme name is case-insensitive, the credentials are
        // opaque. A client library that sends `bearer` is not wrong; one that
        // upper-cases the secret is.
        let auth = AdminAuth::from_token("Ab").expect("a token");
        assert!(auth.authorizes(&bearer("bearer Ab")));
        assert!(auth.authorizes(&bearer("BEARER Ab")));
        assert!(!auth.authorizes(&bearer("Bearer aB")));
    }

    #[test]
    fn a_credential_that_is_not_a_bearer_is_refused() {
        let auth = AdminAuth::from_token("s3cret").expect("a token");
        assert!(!auth.authorizes(&bearer("s3cret")), "no scheme");
        assert!(!auth.authorizes(&bearer("Basic s3cret")));
        assert!(!auth.authorizes(&bearer("Bearers3cret")), "no separator");
    }

    #[test]
    fn a_token_file_is_trimmed_but_not_otherwise_edited() {
        let dir = std::env::temp_dir().join(format!("ramjet-admin-token-{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("a temp directory");
        let path = dir.join("token");

        // What `kubectl create secret generic --from-literal` and every editor
        // produce: a trailing newline that is not part of the secret.
        std::fs::write(&path, b"  a token with spaces\n").expect("writes");
        let auth = AdminAuth::from_file(&path).expect("a token");
        assert!(auth.authorizes(&bearer("Bearer a token with spaces")));
        assert!(
            !auth.authorizes(&bearer("Bearer  a token with spaces")),
            "the padding the file carried is not part of the token"
        );

        std::fs::write(&path, b"\n\t \n").expect("writes");
        assert!(
            matches!(AdminAuth::from_file(&path), Err(TokenError::Empty)),
            "whitespace is not a token"
        );

        std::fs::remove_dir_all(&dir).expect("cleans up");
    }

    #[test]
    fn a_missing_token_file_is_named_rather_than_ignored() {
        let error = AdminAuth::from_file(Path::new("/nonexistent/ramjet/token"))
            .expect_err("no such file");
        assert!(matches!(error, TokenError::Unreadable(_)), "{error:?}");
        assert!(error.to_string().contains("admin token file"), "{error}");
    }

    #[test]
    fn the_token_never_reaches_a_log_line() {
        // `AdminState` derives Debug and is built next to a startup log on two
        // paths. A derived field here would put the secret in the pod's logs.
        let auth = AdminAuth::from_token("s3cret").expect("a token");
        let rendered = format!("{auth:?}");
        assert!(!rendered.contains("s3cret"), "{rendered}");
    }

    #[test]
    fn only_the_methods_that_can_change_something_are_gated() {
        assert!(!mutates(&Method::GET), "a scrape must never need a token");
        assert!(!mutates(&Method::HEAD));
        for method in [Method::POST, Method::DELETE, Method::PUT, Method::PATCH] {
            assert!(mutates(&method), "{method}");
        }
    }

    #[test]
    fn a_method_nobody_thought_about_is_treated_as_a_write() {
        // The case the fail-closed polarity exists for, and the one the list
        // above cannot reach. `mutates` names the *safe* methods rather than the
        // unsafe ones, so an extension method — or a lower-cased `post`, which
        // `http` parses as one — is gated rather than waved through.
        for name in ["PURGE", "post", "PROPFIND", "\u{1}"] {
            let Ok(method) = Method::from_bytes(name.as_bytes()) else {
                continue;
            };
            assert!(mutates(&method), "{name} must be treated as a write");
        }
    }

    #[test]
    fn the_exemptions_are_the_three_callers_that_cannot_send_a_header() {
        // Prometheus and the kubelet, and nothing else. A path added to this
        // list is a path that can be reached without a token, so the list is
        // worth asserting on rather than trusting to review.
        assert_eq!(UNGATED, ["/metrics", "/healthz", "/readyz"]);
        for path in ["/admin/rollback", "/admin/generations", "/admin/routes", "/"] {
            assert!(
                !UNGATED.contains(&path),
                "{path} must not be exempt from the token"
            );
        }
    }

    #[test]
    fn the_refusal_says_what_to_send() {
        let response = unauthorized();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(
            response.headers().get(header::WWW_AUTHENTICATE).map(HeaderValue::as_bytes),
            Some(&b"Bearer"[..]),
            "a 401 with no challenge is not one a client can act on"
        );
    }

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
            metrics: Arc::new(crate::metrics::Metrics::new()),
            routes,
            readiness: ReadinessFlag::new(),
            history,
            auth: None,
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
        assert_eq!(body["version"], API_VERSION);
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
        assert_eq!(body["version"], API_VERSION);
        assert_eq!(body["generation"], 7);

        let listed = body["routes"].as_array().expect("an array");
        assert_eq!(listed.len(), 2);

        // Sorted by host, so the wildcard's `*.example.com` sorts first.
        assert_eq!(listed[0]["host"], "*.example.com");
        assert_eq!(listed[0]["path"], "/v2");
        assert_eq!(listed[0]["path_type"], "Exact");
        assert_eq!(listed[0]["backend"], "prod/api:80");
        assert_eq!(listed[0]["endpoints"], 2);
        assert_eq!(
            listed[0]["protocol"], "http",
            "a backend nobody annotated reports the default rather than nothing"
        );
        assert_eq!(listed[0]["canary"]["backend"], "prod/api-canary:80");
        assert_eq!(listed[0]["canary"]["weight_percent"], 25);

        assert_eq!(listed[0]["canary_stats"]["requests_total"], 0);

        assert_eq!(listed[1]["host"], "example.com");
        assert_eq!(listed[1]["path_type"], "Prefix");
        assert_eq!(listed[1]["canary"], Value::Null);
        assert_eq!(listed[1]["requests_total"], 0);
        assert_eq!(listed[1]["errors_5xx_total"], 0);
        assert_eq!(listed[1]["upstream_latency_ms_sum"], 0.0);
        assert_eq!(listed[1]["upstream_latency_count"], 0);
        assert_eq!(
            listed[1]["canary_stats"],
            Value::Null,
            "a route with no canary has no split to report, and an object full \
             of zeroes could not be told apart from a canary nothing has reached"
        );
        assert_eq!(listed[1]["mirror"], Value::Null);
    }

    #[test]
    fn the_canary_split_is_a_subset_of_the_route_totals() {
        // The property the whole arrangement rests on: starting a canary must
        // not make an existing graph of a route's request rate step down.
        let state = state(table(7));
        let table = state.routes.load_full();
        let (_, rule) = table
            .routes()
            .find(|(_, rule)| rule.canary().is_some())
            .expect("the canary route is in the table");
        let slot = table
            .route_stats()
            .slot(rule.stats_index())
            .expect("a counter block");

        // Three stable requests, one of them a 5xx.
        slot.shard(0).record_response(200);
        slot.shard(0).record_response(200);
        slot.shard(0).record_response(500);
        // One canary request, also a 5xx, recorded in both blocks.
        slot.shard(1).record_response(503);
        slot.canary_shard(1).record_response(503);
        slot.shard(1)
            .record_upstream_latency(std::time::Duration::from_micros(4000));
        slot.canary_shard(1)
            .record_upstream_latency(std::time::Duration::from_micros(4000));

        let body = routes(&state);
        let route = body["routes"]
            .as_array()
            .expect("an array")
            .iter()
            .find(|route| route["path"] == "/v2")
            .expect("the canary route is listed");

        assert_eq!(route["requests_total"], 4, "the totals are still the totals");
        assert_eq!(route["errors_5xx_total"], 2);
        assert_eq!(route["canary_stats"]["requests_total"], 1);
        assert_eq!(route["canary_stats"]["errors_5xx_total"], 1);
        assert_eq!(route["canary_stats"]["upstream_latency_ms_sum"], 4.0);
        assert_eq!(route["canary_stats"]["upstream_latency_count"], 1);

        // Which is what makes the interesting number computable: three stable
        // requests, one of them failing, against one canary request that failed.
        let stable_requests = route["requests_total"].as_u64().unwrap_or_default()
            - route["canary_stats"]["requests_total"]
                .as_u64()
                .unwrap_or_default();
        let stable_errors = route["errors_5xx_total"].as_u64().unwrap_or_default()
            - route["canary_stats"]["errors_5xx_total"]
                .as_u64()
                .unwrap_or_default();
        assert_eq!((stable_requests, stable_errors), (3, 1));
    }

    #[test]
    fn a_mirror_is_reported_with_its_target_and_sample() {
        let mut builder = RouteTableBuilder::new();
        builder.generation(3);
        builder
            .backend("prod/api:80", LbPolicy::RoundRobin, vec![])
            .expect("registers");
        builder
            .backend("prod/shadow:80", LbPolicy::RoundRobin, vec![])
            .expect("registers");
        builder
            .route_with(
                Some("example.com"),
                "/",
                PathType::Prefix,
                "prod/api:80",
                &ramjet_router::RouteOptions {
                    mirror: Some(ramjet_router::MirrorRules {
                        backend: "prod/shadow:80",
                        percent: 25,
                        host: Some("shadow.internal"),
                    }),
                    ..Default::default()
                },
            )
            .expect("drafts");
        let state = state(Arc::new(builder.build().expect("builds")));

        let body = routes(&state);
        let route = &body["routes"].as_array().expect("an array")[0];
        assert_eq!(route["mirror"]["backend"], "prod/shadow:80");
        assert_eq!(route["mirror"]["percent"], 25);
        assert_eq!(route["mirror"]["host"], "shadow.internal");
        assert_eq!(
            route["canary_stats"],
            Value::Null,
            "a mirror is not a canary and reports no split"
        );
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
