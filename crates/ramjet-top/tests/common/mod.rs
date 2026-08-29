//! A stand-in for `ingressd`'s admin port.
//!
//! Small enough to read in one sitting and real enough to be worth running
//! against: an actual TCP listener speaking actual HTTP/1.1, so the client
//! under test does connection setup, header parsing and body reading exactly as
//! it will in production. A test that hands the parser a `&str` proves the
//! parser; this proves the client.
//!
//! The canned bodies are mutable, so a test can change what the server says
//! between two polls and watch the client difference them — which is the
//! behaviour most worth an integration test and impossible to reach from a
//! fixture string.

#![allow(dead_code)]

use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use http::{Request, Response, StatusCode};
use http_body_util::Full;
use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use tokio::net::TcpListener;

/// What the server currently answers with.
#[derive(Debug, Clone)]
pub struct Bodies {
    /// `GET /admin/generations`.
    pub generations: String,
    /// `GET /admin/routes`.
    pub routes: String,
    /// `GET /metrics`.
    pub metrics: String,
    /// When set, every endpoint answers 503 instead — an unhealthy daemon.
    pub failing: bool,
    /// Rollback requests the server has received, newest last.
    ///
    /// `Some(generation)` for a pin, `None` for a release.
    pub rollbacks: Vec<Option<u64>>,
    /// Every request seen, as `(path, Authorization)`.
    ///
    /// The header rather than a boolean, because the interesting failure is not
    /// "no token was sent" but "the token was sent on the polls too", and a
    /// count of requests that carried one cannot tell those apart.
    pub authorization: Vec<(String, Option<String>)>,
}

impl Default for Bodies {
    fn default() -> Self {
        Self {
            generations: generations_json(42, None, 10_000),
            routes: routes_json(42, 10_000, 12),
            metrics: metrics_text(10_007, 12, 37, 42),
            failing: false,
            rollbacks: Vec::new(),
            authorization: Vec::new(),
        }
    }
}

/// A running mock admin port.
pub struct MockAdmin {
    /// Where it is listening.
    pub addr: SocketAddr,
    /// What it answers with; lock and mutate to change the fixture mid-test.
    pub state: Arc<Mutex<Bodies>>,
}

impl MockAdmin {
    /// Binds an ephemeral port and starts serving.
    pub async fn start() -> Self {
        Self::start_with(Bodies::default()).await
    }

    /// Binds an ephemeral port and starts serving the given bodies.
    ///
    /// Port zero rather than a fixed one: these tests run in parallel with each
    /// other and with whatever else is on the machine, and a hard-coded port is
    /// a flake waiting for a busy CI box.
    pub async fn start_with(bodies: Bodies) -> Self {
        let listener = TcpListener::bind(("127.0.0.1", 0))
            .await
            .expect("an ephemeral port");
        let addr = listener.local_addr().expect("a bound address");
        let state = Arc::new(Mutex::new(bodies));

        let accept_state = Arc::clone(&state);
        tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    return;
                };
                let state = Arc::clone(&accept_state);
                tokio::spawn(async move {
                    let service =
                        service_fn(move |request| handle(Arc::clone(&state), request));
                    // The result is deliberately dropped: a client that hangs
                    // up mid-response is a case some tests create on purpose.
                    let _ = http1::Builder::new()
                        .serve_connection(TokioIo::new(stream), service)
                        .await;
                });
            }
        });

        Self { addr, state }
    }

    /// The URL a client should be pointed at.
    pub fn url(&self) -> String {
        format!("http://{}", self.addr)
    }

    /// Replaces the canned bodies.
    pub fn set(&self, f: impl FnOnce(&mut Bodies)) {
        let mut state = self.state.lock().expect("the fixture lock");
        f(&mut state);
    }

    /// The rollback requests received so far.
    pub fn rollbacks(&self) -> Vec<Option<u64>> {
        self.state.lock().expect("the fixture lock").rollbacks.clone()
    }

    /// The `Authorization` header seen on each request, in arrival order.
    pub fn authorization(&self) -> Vec<(String, Option<String>)> {
        self.state
            .lock()
            .expect("the fixture lock")
            .authorization
            .clone()
    }
}

/// Answers one request.
async fn handle(
    state: Arc<Mutex<Bodies>>,
    request: Request<Incoming>,
) -> Result<Response<Full<Bytes>>, Infallible> {
    let path = request.uri().path().to_string();
    let method = request.method().clone();
    let authorization = request
        .headers()
        .get(http::header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    state
        .lock()
        .expect("the fixture lock")
        .authorization
        .push((path.clone(), authorization));

    // The rollback endpoints are recorded rather than acted on; what these
    // tests care about is that the client sent the right thing.
    if path == "/admin/rollback" {
        let generation = if method == http::Method::DELETE {
            None
        } else {
            let body = collect(request).await;
            let parsed: serde_json::Value =
                serde_json::from_slice(&body).unwrap_or(serde_json::Value::Null);
            Some(parsed["generation"].as_u64().unwrap_or_default())
        };
        state
            .lock()
            .expect("the fixture lock")
            .rollbacks
            .push(generation);
        return Ok(text(StatusCode::OK, "ok"));
    }

    let bodies = state.lock().expect("the fixture lock").clone();
    if bodies.failing {
        return Ok(text(StatusCode::SERVICE_UNAVAILABLE, "unavailable"));
    }

    Ok(match path.as_str() {
        "/admin/generations" => json(&bodies.generations),
        "/admin/routes" => json(&bodies.routes),
        "/metrics" => text(StatusCode::OK, &bodies.metrics),
        _ => text(StatusCode::NOT_FOUND, "not found"),
    })
}

/// Reads a request body to the end.
async fn collect(request: Request<Incoming>) -> Bytes {
    use http_body_util::BodyExt;
    request
        .into_body()
        .collect()
        .await
        .map(|b| b.to_bytes())
        .unwrap_or_default()
}

fn json(body: &str) -> Response<Full<Bytes>> {
    Response::builder()
        .status(StatusCode::OK)
        .header(http::header::CONTENT_TYPE, "application/json")
        .body(Full::new(Bytes::from(body.to_string())))
        .expect("a valid response")
}

fn text(status: StatusCode, body: &str) -> Response<Full<Bytes>> {
    Response::builder()
        .status(status)
        .header(http::header::CONTENT_TYPE, "text/plain")
        .body(Full::new(Bytes::from(body.to_string())))
        .expect("a valid response")
}

// --- fixture builders ----------------------------------------------------
//
// These produce the exact shapes the frozen contract specifies. They are
// written as string templates rather than by serializing this crate's own
// types on purpose: serializing the types under test would make the round trip
// pass even if both sides were wrong about the contract.

/// A `/admin/generations` body with two entries, the newest being `generation`.
pub fn generations_json(generation: u64, pinned: Option<u64>, _requests: u64) -> String {
    let pinned = pinned.map_or("null".to_string(), |p| p.to_string());
    let serving = generation;
    format!(
        r#"{{
  "pinned": {pinned},
  "serving": {serving},
  "generations": [
    {{
      "generation": {generation},
      "applied_at": "2026-08-28T10:00:00Z",
      "published": true,
      "digest": "a1b2c3d4e5f60718293a4b5c6d7e8f90",
      "routes": 2,
      "hosts": 2,
      "certs": 1,
      "diff": {{
        "summary": "1 route added, 1 backend changed",
        "routes_added": ["shop.example.com/checkout"],
        "routes_removed": [],
        "backends_changed": ["api.example.com/v1 -> api-v2"],
        "certs_rotated": [],
        "hosts_added": ["shop.example.com"],
        "hosts_removed": []
      }}
    }},
    {{
      "generation": {},
      "applied_at": "2026-08-28T09:45:00Z",
      "published": true,
      "digest": "ffee0011",
      "routes": 1,
      "hosts": 1,
      "certs": 1,
      "diff": {{
        "summary": "initial table",
        "routes_added": ["api.example.com/v1"],
        "routes_removed": [],
        "backends_changed": [],
        "certs_rotated": [],
        "hosts_added": ["api.example.com"],
        "hosts_removed": []
      }}
    }}
  ]
}}"#,
        generation.saturating_sub(1)
    )
}

/// A `/admin/routes` body with two routes; the first carries the counters.
pub fn routes_json(generation: u64, requests: u64, errors: u64) -> String {
    let latency_sum = requests as f64 * 5.0;
    // A tenth of the traffic and all of the failures, which is what a canary
    // going wrong looks like: the route's totals barely move while the split
    // says exactly where the errors are coming from.
    let canary_requests = requests / 10;
    let canary_latency_sum = canary_requests as f64 * 9.0;
    format!(
        r#"{{
  "generation": {generation},
  "routes": [
    {{
      "host": "api.example.com",
      "path": "/v1",
      "path_type": "Prefix",
      "backend": "api-v2",
      "endpoints": 4,
      "requests_total": {requests},
      "errors_5xx_total": {errors},
      "upstream_latency_ms_sum": {latency_sum},
      "upstream_latency_count": {requests},
      "canary": {{"backend": "api-v3", "weight_percent": 10}},
      "canary_stats": {{
        "requests_total": {canary_requests},
        "errors_5xx_total": {errors},
        "upstream_latency_ms_sum": {canary_latency_sum},
        "upstream_latency_count": {canary_requests}
      }}
    }},
    {{
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
    }}
  ]
}}"#
    )
}

/// A `/admin/routes` body with a third, newly added route.
pub fn routes_json_with_new_route(generation: u64, requests: u64, errors: u64) -> String {
    let with_two = routes_json(generation, requests, errors);
    let extra = r#"    ,{
      "host": "shop.example.com",
      "path": "/checkout",
      "path_type": "Exact",
      "backend": "checkout-svc",
      "endpoints": 2,
      "requests_total": 25,
      "errors_5xx_total": 0,
      "upstream_latency_ms_sum": 300.0,
      "upstream_latency_count": 25,
      "canary": null
    }
  ]
}"#;
    // Splice the extra route in before the closing bracket of `routes`.
    let cut = with_two.rfind("\n  ]").expect("a routes array to extend");
    format!("{}\n{extra}", &with_two[..cut])
}

/// A Prometheus page in the shape `ramjet-proxy` renders.
pub fn metrics_text(requests: u64, errors: u64, connections: i64, generation: u64) -> String {
    let two_xx = requests.saturating_sub(errors);
    let latency_sum = requests as f64 * 0.005;
    format!(
        "# HELP ramjet_requests_total Responses served, by status class.\n\
         # TYPE ramjet_requests_total counter\n\
         ramjet_requests_total{{code=\"1xx\"}} 0\n\
         ramjet_requests_total{{code=\"2xx\"}} {two_xx}\n\
         ramjet_requests_total{{code=\"3xx\"}} 0\n\
         ramjet_requests_total{{code=\"4xx\"}} 0\n\
         ramjet_requests_total{{code=\"5xx\"}} {errors}\n\
         ramjet_requests_total{{code=\"other\"}} 0\n\
         # HELP ramjet_upstream_latency_seconds Time from upstream dispatch to response headers.\n\
         # TYPE ramjet_upstream_latency_seconds histogram\n\
         ramjet_upstream_latency_seconds_bucket{{le=\"0.001\"}} 10\n\
         ramjet_upstream_latency_seconds_bucket{{le=\"0.025\"}} {requests}\n\
         ramjet_upstream_latency_seconds_bucket{{le=\"+Inf\"}} {requests}\n\
         ramjet_upstream_latency_seconds_sum {latency_sum}\n\
         ramjet_upstream_latency_seconds_count {requests}\n\
         # HELP ramjet_active_connections Downstream connections currently being served.\n\
         # TYPE ramjet_active_connections gauge\n\
         ramjet_active_connections {connections}\n\
         # HELP ramjet_route_table_generation Generation of the currently published route table.\n\
         # TYPE ramjet_route_table_generation gauge\n\
         ramjet_route_table_generation {generation}\n"
    )
}
