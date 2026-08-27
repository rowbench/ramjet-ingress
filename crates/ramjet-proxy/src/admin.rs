//! The admin listener: `/metrics`, `/healthz`, and `/readyz`.
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

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use bytes::Bytes;
use http::{header, HeaderValue, Method, Request, Response, StatusCode};
use hyper::body::Incoming;
use ramjet_router::SharedRouteTable;

use crate::body::ProxyBody;
use crate::metrics::Metrics;

/// The exposition format version Prometheus expects to negotiate.
const PROMETHEUS_CONTENT_TYPE: &str = "text/plain; version=0.0.4; charset=utf-8";

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
    /// Consulted only for its generation number, at scrape time.
    pub routes: Arc<SharedRouteTable>,
    /// Gates `/readyz`.
    pub readiness: ReadinessFlag,
}

/// Answers one admin request.
pub async fn handle(state: Arc<AdminState>, request: Request<Incoming>) -> Response<ProxyBody> {
    // A scrape or a probe is a GET; anything else against these paths is a
    // misconfiguration worth naming rather than quietly serving.
    if request.method() != Method::GET && request.method() != Method::HEAD {
        return text(StatusCode::METHOD_NOT_ALLOWED, "method not allowed\n");
    }

    match request.uri().path() {
        "/metrics" => {
            let body = state
                .metrics
                .render_prometheus(state.routes.generation());
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
        _ => text(StatusCode::NOT_FOUND, "not found\n"),
    }
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
}
