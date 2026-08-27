//! The per-request forwarding path.
//!
//! This is the function the router's sub-200ns matching budget was set for.
//! Everything else in the request path has to fit around that number, not the
//! other way around, so the shape of [`handle`] is deliberate:
//!
//! 1. **One snapshot.** [`SharedRouteTable::load_full`] is called exactly once,
//!    at the top. Host matching, path matching, the canary decision, endpoint
//!    selection, and the in-flight counter guard all read through that one
//!    `Arc`. A configuration published mid-request cannot be half-observed, and
//!    the request finishes against the generation it started with.
//! 2. **No locks.** There is not a `Mutex` or an `RwLock` anywhere below this
//!    line. The only shared mutable state a request touches is the router's
//!    relaxed load-balancer counters.
//! 3. **No buffering.** Bodies stream in both directions; see
//!    [`ProxyBody`](crate::body::ProxyBody).
//!
//! # Retrying, and why it is narrower than you might expect
//!
//! A connect failure means nothing was written to the endpoint, so the request
//! is untouched and can go to a different one. That is the only failure this
//! module retries — a timeout or a mid-exchange error may have already had
//! effects upstream, and replaying a `POST` that already ran is worse than
//! returning 504.
//!
//! There is a second, sharper limit: the request *body*. Re-dispatching a
//! streaming body means having kept a copy of it, and buffering upstream
//! request bodies is precisely the behaviour that makes an ingress-nginx pod's
//! memory a function of what its slowest client is uploading. So a request whose
//! body is known to be empty — every `GET`, `HEAD`, `OPTIONS`, `DELETE`, and
//! anything with `Content-Length: 0`, which is the overwhelming majority of
//! ingress traffic — fails over across endpoints, and a request carrying a body
//! gets one attempt. nginx makes the same trade in the other direction, by
//! buffering every request body by default; this way round costs a little
//! failover coverage on writes and bounds memory absolutely.
//!
//! # Error mapping
//!
//! | Condition | Status |
//! |---|---|
//! | no matching route, and no default backend | 404 |
//! | backend matched but has no endpoints | 503 |
//! | every attempted endpoint refused the connection | 502 |
//! | upstream sent no headers before the deadline | 504 |
//! | gRPC upstream (needs HTTP/2, see below) | 502 |
//!
//! Bodies are tiny `&'static [u8]` constants: an error page is not the place to
//! allocate, and an ingress that gets slower the more it is failing has the
//! failure mode backwards.
//!
//! # TODO: gRPC
//!
//! gRPC requires HTTP/2 end to end — it is defined in terms of h2 streams and
//! trailers, and there is no HTTP/1.1 form of it. Downstream already speaks h2,
//! but [`Upstream`](crate::upstream::Upstream) dials HTTP/1.1, so a gRPC
//! request would be silently downgraded into something the backend cannot
//! parse. Rather than emit a confusing failure, requests with an
//! `application/grpc` content type are answered with an explicit 502 naming the
//! limitation. Lifting it means adding an h2 upstream mode, selected per
//! backend from the `backend-protocol: GRPC` annotation.

use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use http::header::{self, HeaderMap, HeaderValue};
use http::{Request, Response, StatusCode, Version};
use hyper::body::Incoming;
use hyper::upgrade::OnUpgrade;
use hyper_util::rt::TokioIo;
use ramjet_router::{select_endpoint, Backend, RouteTable, SharedRouteTable};

use crate::body::ProxyBody;
use crate::headers;
use crate::metrics::Metrics;
use crate::rng;
use crate::upstream::{endpoint_uri, Upstream, UpstreamError};

const BODY_NO_ROUTE: &[u8] = b"404 Not Found: no ingress rule matches this host and path\n";
const BODY_NO_ENDPOINT: &[u8] = b"503 Service Unavailable: the backend has no ready endpoints\n";
const BODY_CONNECT_FAILED: &[u8] = b"502 Bad Gateway: could not connect to any upstream endpoint\n";
const BODY_UPSTREAM_FAILED: &[u8] = b"502 Bad Gateway: the upstream connection failed\n";
const BODY_BAD_TARGET: &[u8] = b"502 Bad Gateway: the endpoint address is not a valid URI\n";
const BODY_UPGRADE_FAILED: &[u8] = b"502 Bad Gateway: the upstream refused to complete the upgrade\n";
const BODY_TIMEOUT: &[u8] = b"504 Gateway Timeout: the upstream sent no response headers in time\n";
const BODY_GRPC: &[u8] =
    b"502 Bad Gateway: gRPC upstreams require HTTP/2, which ramjet does not yet speak upstream\n";

/// Which listener a request arrived on, which is what `X-Forwarded-Proto` says.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Scheme {
    /// The plaintext listener.
    Http,
    /// The TLS listener.
    Https,
}

impl Scheme {
    /// The value used for `X-Forwarded-Proto`.
    pub fn as_str(self) -> &'static str {
        match self {
            Scheme::Http => "http",
            Scheme::Https => "https",
        }
    }
}

/// What the request path needs that is not the request itself.
///
/// Shared by every connection through one `Arc`; nothing in here is mutated
/// after startup except the atomics inside [`Metrics`].
#[derive(Debug)]
pub struct ProxyState {
    /// The published route table.
    pub routes: Arc<SharedRouteTable>,
    /// The upstream client and its pool.
    pub upstream: Upstream,
    /// Data-plane counters.
    pub metrics: Arc<Metrics>,
}

/// Per-connection facts that every request on it inherits.
#[derive(Debug, Clone, Copy)]
pub struct ConnInfo {
    /// The peer address, used for `X-Forwarded-For` and `X-Real-IP`.
    pub remote: std::net::SocketAddr,
    /// Which listener accepted the connection.
    pub scheme: Scheme,
}

/// Routes and forwards one request.
pub async fn handle(
    state: Arc<ProxyState>,
    conn: ConnInfo,
    request: Request<Incoming>,
) -> Response<ProxyBody> {
    let response = forward(&state, conn, request).await;
    state.metrics.record_response(response.status().as_u16());
    response
}

async fn forward(
    state: &ProxyState,
    conn: ConnInfo,
    request: Request<Incoming>,
) -> Response<ProxyBody> {
    // The one and only snapshot load. Everything below borrows from it.
    let snapshot = state.routes.load_full();

    let Some(backend) = select_backend(&snapshot, &request) else {
        state.metrics.record_route_miss();
        return static_response(StatusCode::NOT_FOUND, BODY_NO_ROUTE);
    };

    if is_grpc(request.headers()) {
        return static_response(StatusCode::BAD_GATEWAY, BODY_GRPC);
    }

    let endpoints = backend.endpoints();
    if endpoints.is_empty() {
        return static_response(StatusCode::SERVICE_UNAVAILABLE, BODY_NO_ENDPOINT);
    }

    let forwarded_host = client_host(&request);
    let (mut parts, body) = request.into_parts();

    // Taking `OnUpgrade` out of the extensions is what `hyper::upgrade::on`
    // does internally; doing it explicitly keeps the upgrade path visible
    // rather than hidden behind a call that looks like a getter.
    let downstream_upgrade = parts.extensions.remove::<OnUpgrade>();
    let upgrade = headers::upgrade_protocol(&parts.headers);

    headers::strip_hop_by_hop(&mut parts.headers);
    headers::apply_forwarded(
        &mut parts.headers,
        conn.remote.ip(),
        conn.scheme.as_str(),
        forwarded_host.clone(),
    );
    headers::ensure_request_id(&mut parts.headers);
    if let Some(protocol) = &upgrade {
        headers::restore_upgrade(&mut parts.headers, protocol);
    }

    // An HTTP/2 request carries its host in `:authority` and has no `Host`
    // header at all. Downgrading it to HTTP/1.1 without restoring `Host` would
    // let hyper's client fill one in from the endpoint's `ip:port`, which is
    // exactly the rewrite this proxy promises not to do.
    if !parts.headers.contains_key(header::HOST) {
        if let Some(host) = forwarded_host {
            parts.headers.insert(header::HOST, host);
        }
    }
    parts.version = Version::HTTP_11;

    let path_and_query = parts.uri.path_and_query().cloned();
    let body = ProxyBody::stream(body);

    // See the module docs: only a body we can reproduce may be re-dispatched.
    let retryable = body.is_known_empty();
    let attempts = if retryable {
        endpoints.len().min(state.upstream.max_connect_attempts())
    } else {
        1
    };

    let stats = snapshot.stats();
    let slot = stats.slot(backend.stats_index());
    let Some((mut index, _)) = select_endpoint(backend, stats, rng::next_u64()) else {
        return static_response(StatusCode::SERVICE_UNAVAILABLE, BODY_NO_ENDPOINT);
    };

    let mut streaming = if retryable { None } else { Some(body) };
    let mut pending = Some(parts);
    let mut failure = None;

    for attempt in 0..attempts {
        let Some(base) = pending.take() else { break };
        let last = attempt + 1 == attempts;
        // The final attempt consumes the header block; earlier ones clone it,
        // because building a `Request` moves it. A single-attempt request --
        // which is every request that succeeds first time -- clones nothing.
        let mut outgoing = if last {
            base
        } else {
            let copy = base.clone();
            pending = Some(base);
            copy
        };

        let Some(endpoint) = endpoints.get(index) else { break };
        let Some(uri) = endpoint_uri(endpoint.addr, path_and_query.as_ref()) else {
            return static_response(StatusCode::BAD_GATEWAY, BODY_BAD_TARGET);
        };
        outgoing.uri = uri;

        let request = Request::from_parts(
            outgoing,
            streaming.take().unwrap_or_else(ProxyBody::empty),
        );

        // In-flight accounting for `LeastConn`. The guard borrows the counter
        // out of `BackendStats`, which successive route tables share, so it
        // stays correct even if a new generation is published mid-request.
        let _inflight = slot.and_then(|slot| slot.acquire(index));

        let started = Instant::now();
        match state.upstream.send(request).await {
            Ok(response) => {
                state.metrics.record_upstream_latency(started.elapsed());
                return relay(response, downstream_upgrade, upgrade.as_ref());
            }
            Err(error) => {
                match &error {
                    UpstreamError::Connect(_) => state.metrics.record_connect_failure(),
                    UpstreamError::Timeout => state.metrics.record_upstream_timeout(),
                    UpstreamError::Transport(_) => {}
                }
                let retry = error.is_retryable() && !last;
                failure = Some(error);
                if !retry {
                    break;
                }
                state.metrics.record_retry();
                index = (index + 1) % endpoints.len();
            }
        }
    }

    match failure {
        Some(UpstreamError::Timeout) => static_response(StatusCode::GATEWAY_TIMEOUT, BODY_TIMEOUT),
        Some(UpstreamError::Connect(_)) | None => {
            static_response(StatusCode::BAD_GATEWAY, BODY_CONNECT_FAILED)
        }
        Some(UpstreamError::Transport(_)) => {
            static_response(StatusCode::BAD_GATEWAY, BODY_UPSTREAM_FAILED)
        }
    }
}

/// Matches the request and applies any canary attached to the matched rule.
///
/// The returned reference borrows the snapshot, not the request, so the caller
/// is free to take the request apart afterwards.
fn select_backend<'t>(table: &'t RouteTable, request: &Request<Incoming>) -> Option<&'t Backend> {
    let matched = table.match_request(request_authority(request), request.uri().path())?;

    let Some(canary) = matched.canary() else {
        return Some(matched.backend());
    };

    // The router refuses to know what a `HeaderMap` is, so the two values it
    // might need are looked up here and passed as borrowed `&str`s.
    let header_value = canary
        .header_name()
        .and_then(|name| request.headers().get(name))
        .and_then(|value| value.to_str().ok());
    let cookie_value = canary
        .cookie_name()
        .and_then(|name| headers::cookie_value(request.headers(), name));
    let roll = rng::below(canary.weight_total());

    if canary.decide(header_value, cookie_value, roll) {
        // A canary naming a backend the table does not hold is a controller
        // bug, not a reason to fail the request: serve production instead.
        table.backend(canary.backend()).or(Some(matched.backend()))
    } else {
        Some(matched.backend())
    }
}

/// The host the client addressed.
///
/// HTTP/2 puts it in `:authority`, which hyper surfaces as the URI's authority;
/// HTTP/1.1 puts it in `Host`, except in the absolute-form request target,
/// where RFC 7230 §5.4 says the target wins. Checking the URI first gets all
/// three right.
fn request_authority(request: &Request<Incoming>) -> &str {
    if let Some(authority) = request.uri().authority() {
        return authority.as_str();
    }
    request
        .headers()
        .get(header::HOST)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("")
}

/// The same value as [`request_authority`], as a header value.
///
/// Kept separate so the HTTP/1.1 path clones a `HeaderValue` — a refcount bump
/// on the underlying `Bytes` — instead of re-parsing a `&str` into one.
fn client_host(request: &Request<Incoming>) -> Option<HeaderValue> {
    match request.uri().authority() {
        Some(authority) => HeaderValue::from_str(authority.as_str()).ok(),
        None => request.headers().get(header::HOST).cloned(),
    }
}

fn is_grpc(headers: &HeaderMap) -> bool {
    headers
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.starts_with("application/grpc"))
}

/// Turns an upstream response into a downstream one, wiring up an upgrade if
/// the upstream accepted it.
fn relay(
    response: Response<Incoming>,
    downstream_upgrade: Option<OnUpgrade>,
    requested_protocol: Option<&HeaderValue>,
) -> Response<ProxyBody> {
    let (mut parts, body) = response.into_parts();
    let upstream_upgrade = parts.extensions.remove::<OnUpgrade>();
    let switching = parts.status == StatusCode::SWITCHING_PROTOCOLS;
    // The upstream normally echoes `Upgrade`; if it did not, the protocol the
    // client asked for is the only sensible answer.
    let protocol = switching
        .then(|| headers::upgrade_protocol(&parts.headers).or_else(|| requested_protocol.cloned()))
        .flatten();

    headers::strip_hop_by_hop(&mut parts.headers);

    if switching {
        // A 101 is only meaningful if both halves can actually be hijacked. If
        // either is missing, the connection would be left in a state where the
        // client thinks it has a tunnel and there is nothing on the other end.
        match (downstream_upgrade, upstream_upgrade, protocol) {
            (Some(downstream), Some(upstream), Some(protocol)) => {
                headers::restore_upgrade(&mut parts.headers, &protocol);
                // The downstream half does not resolve until hyper has written
                // this very response, so the task is spawned before returning
                // it and then waits.
                tokio::spawn(splice(downstream, upstream));
            }
            _ => return static_response(StatusCode::BAD_GATEWAY, BODY_UPGRADE_FAILED),
        }
    }

    // Whatever the upstream client attached describes that connection, not
    // this one.
    parts.extensions.clear();
    Response::from_parts(parts, ProxyBody::stream(body))
}

/// Copies bytes both ways until either side closes.
///
/// Once a connection has been upgraded there is no HTTP left to interpret: a
/// WebSocket frame, or whatever else the two peers agreed on, is opaque. So
/// this is a byte pump and nothing more, which is also why it costs one task
/// and two buffers regardless of how chatty the protocol on top turns out to be.
async fn splice(downstream: OnUpgrade, upstream: OnUpgrade) {
    let Ok((downstream, upstream)) = tokio::try_join!(downstream, upstream) else {
        return;
    };
    let mut downstream = TokioIo::new(downstream);
    let mut upstream = TokioIo::new(upstream);
    // A closed tunnel is the normal end state, not an error worth reporting.
    let _ = tokio::io::copy_bidirectional(&mut downstream, &mut upstream).await;
}

/// An error response with a constant body.
pub fn static_response(status: StatusCode, body: &'static [u8]) -> Response<ProxyBody> {
    let mut response = Response::new(ProxyBody::once(Bytes::from_static(body)));
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
    use http_body_util::BodyExt;

    #[test]
    fn scheme_names_match_the_forwarded_proto_values() {
        assert_eq!(Scheme::Http.as_str(), "http");
        assert_eq!(Scheme::Https.as_str(), "https");
    }

    #[test]
    fn grpc_is_detected_by_content_type_prefix() {
        let mut headers = HeaderMap::new();
        headers.insert(
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/grpc+proto"),
        );
        assert!(is_grpc(&headers));

        headers.insert(
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/json"),
        );
        assert!(!is_grpc(&headers));

        assert!(!is_grpc(&HeaderMap::new()));
    }

    #[tokio::test]
    async fn static_responses_carry_their_body_and_a_content_type() {
        let response = static_response(StatusCode::NOT_FOUND, BODY_NO_ROUTE);
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        assert_eq!(
            response.headers().get(header::CONTENT_TYPE).map(|v| v.as_bytes()),
            Some(&b"text/plain; charset=utf-8"[..])
        );
        let body = response.into_body().collect().await.expect("collects").to_bytes();
        assert_eq!(&body[..], BODY_NO_ROUTE);
    }

    #[test]
    fn every_error_body_starts_with_its_status_code() {
        // The body is what an operator sees in a curl; it should say which of
        // the failure modes in the table above they hit.
        for (code, body) in [
            ("404", BODY_NO_ROUTE),
            ("503", BODY_NO_ENDPOINT),
            ("502", BODY_CONNECT_FAILED),
            ("502", BODY_UPSTREAM_FAILED),
            ("502", BODY_BAD_TARGET),
            ("502", BODY_UPGRADE_FAILED),
            ("504", BODY_TIMEOUT),
            ("502", BODY_GRPC),
        ] {
            let text = std::str::from_utf8(body).expect("utf-8");
            assert!(text.starts_with(code), "{text:?} does not start with {code}");
            assert!(text.ends_with('\n'), "{text:?} is missing its newline");
        }
    }
}
