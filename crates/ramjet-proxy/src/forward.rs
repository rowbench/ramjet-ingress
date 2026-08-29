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
//! # The request body is the proxy's own type, not hyper's
//!
//! [`handle`] takes a `Request<ProxyBody>` rather than the `Request<Incoming>`
//! a hyper service hands it, and the caller does the one-line conversion. That
//! is not tidiness: `hyper::body::Incoming` has no public constructor, so a
//! request that did not arrive on a hyper connection cannot be expressed as
//! one — and HTTP/3 requests do not. Naming the crate's own body here is what
//! lets the `http3` module reach this function instead of forking it, and it
//! costs the HTTP/1.1 and HTTP/2 paths nothing: `ProxyBody::Stream` is a
//! straight delegation to `Incoming`.
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
//! | gRPC arriving at an HTTP/1.1 backend (see below) | 502 |
//!
//! Bodies are tiny `&'static [u8]` constants: an error page is not the place to
//! allocate, and an ingress that gets slower the more it is failing has the
//! failure mode backwards.
//!
//! # Two upstream protocols, and the version translation between them
//!
//! Which protocol an endpoint is dialled with is a property of the backend, not
//! of the request: [`BackendProtocol::H2c`] comes from
//! `backend-protocol: GRPC` on the Ingress and is carried through the route
//! table. So all four combinations occur and all four have to work — an
//! HTTP/1.1 client reaching an h2c backend, an HTTP/2 client reaching an
//! HTTP/1.1 one, and both matching pairs.
//!
//! The translation is three lines and each one matters:
//!
//! - **Version.** The outgoing `Version` is set from the backend's protocol, not
//!   from what the client spoke.
//! - **Authority.** HTTP/1.1 carries the client's name in `Host`; HTTP/2 carries
//!   it in `:authority`, which hyper derives from the request URI — and the URI
//!   has to be the endpoint, because that is what keys the connection pool. A
//!   request going out over h2 therefore drops `Host` rather than sending one
//!   that disagrees with `:authority`, which RFC 9113 §8.3.1 lets a server treat
//!   as malformed. `X-Forwarded-Host` still carries the name the client used, on
//!   both paths.
//! - **Upgrades.** `Connection` and `Upgrade` are connection-specific headers
//!   that HTTP/2 forbids outright, so an upgrade request is not reconstructed
//!   for an h2c backend. It goes upstream as an ordinary request, the upstream
//!   does not answer 101, and the client gets whatever the application said —
//!   rather than an h2 stream error. WebSocket over HTTP/2 (RFC 8441 extended
//!   CONNECT) is a separate protocol and is not implemented.
//!
//! # gRPC, and the one case that is still refused
//!
//! gRPC is defined in terms of h2 streams and trailers and has no HTTP/1.1 form,
//! so a gRPC request sent to an [`Http1`](BackendProtocol::Http1) backend would
//! be downgraded into something the backend cannot parse. That request is still
//! answered with an explicit 502, and the body now names the annotation that
//! fixes it. A gRPC request to an [`H2c`](BackendProtocol::H2c) backend is
//! forwarded like any other: trailers — where `grpc-status` lives — pass through
//! in both directions, because [`ProxyBody`] relays whole frames and a trailer
//! frame is just a frame.

use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use http::header::{self, HeaderMap, HeaderValue};
use http::request::Parts;
use http::{Request, Response, StatusCode, Version};
use hyper::body::Incoming;
use hyper::upgrade::OnUpgrade;
use hyper_util::rt::TokioIo;
use ramjet_router::{
    select_endpoint, Backend, BackendProtocol, MirrorSpec, RouteCounters, RouteSlot, RouteTable,
    SharedRouteTable, MIRROR_PERCENT_TOTAL,
};

use crate::body::ProxyBody;
use crate::headers;
use crate::metrics::Metrics;
use crate::mirror::{self, Buffered, Mirror, MIRRORED_BY, MIRRORED_BY_VALUE};
use crate::rng;
use crate::upstream::{endpoint_uri, Upstream, UpstreamError};

const BODY_NO_ROUTE: &[u8] = b"404 Not Found: no ingress rule matches this host and path\n";
const BODY_NO_ENDPOINT: &[u8] = b"503 Service Unavailable: the backend has no ready endpoints\n";
const BODY_CONNECT_FAILED: &[u8] = b"502 Bad Gateway: could not connect to any upstream endpoint\n";
const BODY_UPSTREAM_FAILED: &[u8] = b"502 Bad Gateway: the upstream connection failed\n";
const BODY_BAD_TARGET: &[u8] = b"502 Bad Gateway: the endpoint address is not a valid URI\n";
const BODY_UPGRADE_FAILED: &[u8] = b"502 Bad Gateway: the upstream refused to complete the upgrade\n";
const BODY_TIMEOUT: &[u8] = b"504 Gateway Timeout: the upstream sent no response headers in time\n";
/// A gRPC request whose backend is dialled over HTTP/1.1.
///
/// The hint is the whole value of this response. The failure it replaces —
/// forwarding gRPC over HTTP/1.1 — surfaces at the client as a parse error from
/// a library that cannot say which hop broke it, and the fix is one annotation
/// on an Ingress the operator already has.
const BODY_GRPC: &[u8] = b"502 Bad Gateway: gRPC requires an HTTP/2 backend; \
set nginx.ingress.kubernetes.io/backend-protocol: GRPC on the Ingress\n";

/// Names the capability a 502 was refused for, in one token.
///
/// The body already says it in a sentence, which is the right thing for the
/// person who ran `curl` and the wrong thing for everything else: a body is not
/// in an access log, not in a client library's error, and not something a script
/// should be matching on with a substring. The vocabulary is closed — this and
/// `h2c-upstream`, which only the uring engine can produce — so a check for it
/// is a check for equality.
///
/// Only on the refusals an operator can act on by changing an annotation or a
/// flag. A 502 from an upstream that hung up is not a capability gap and gets no
/// header, because a header on every gateway error would say nothing.
pub const UNSUPPORTED_HEADER: &str = "x-ramjet-unsupported";

/// A gRPC request reached a backend nobody annotated `backend-protocol: GRPC`.
///
/// The same token the uring engine sends for the same refusal: a client must not
/// be able to tell which engine answered it.
pub const UNSUPPORTED_GRPC: &str = "grpc-needs-backend-protocol";

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
    /// Which per-route counter block this serving runtime writes to.
    ///
    /// One per runtime, so two cores never contend on the same cache line for
    /// the same route; see [`RouteStats`](ramjet_router::RouteStats). The
    /// remainder against the shard count is taken inside the router, so this
    /// is just the runtime's index.
    pub shard: usize,
    /// This runtime's mirror queue, or `None` where mirroring is not wired up.
    ///
    /// One per serving runtime, so a shadow backend that cannot keep up fills
    /// one runtime's bounded queue rather than contending with every other
    /// core for a shared one. `None` in the tests and in any embedding that
    /// never starts a worker; a route with a mirror annotation then simply
    /// makes no copies, which is the correct behaviour for a data plane that
    /// has nowhere to put them.
    pub mirror: Option<Mirror>,
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
    request: Request<ProxyBody>,
) -> Response<ProxyBody> {
    let response = forward(&state, conn, request).await;
    state.metrics.record_response(response.status().as_u16());
    response
}

async fn forward(
    state: &ProxyState,
    conn: ConnInfo,
    request: Request<ProxyBody>,
) -> Response<ProxyBody> {
    // The one and only snapshot load. Everything below borrows from it.
    let snapshot = state.routes.load_full();

    let Some(matched) = select_backend(&snapshot, &request) else {
        state.metrics.record_route_miss();
        return static_response(StatusCode::NOT_FOUND, BODY_NO_ROUTE);
    };

    // Resolved once, here, and passed down as plain references into the
    // snapshot this request is already holding. That is the whole per-route
    // accounting cost on the request path: one indexed load now, and a handful
    // of relaxed adds later. There is no map, no label set, and no `Arc` clone
    // — the counters are reached through the table the request matched
    // against, and they outlive it, so recording after a rebuild still lands in
    // the block the new generation serves.
    //
    // A request answered by the default backend has no `route` and so is
    // counted only in the process-wide series: it matched no rule, and
    // attributing it to one would invent a route that is not in the table.
    let slot = matched
        .route
        .and_then(|index| snapshot.route_stats().slot(index));
    let recorder = Recorder::new(slot, state.shard, matched.canaried);

    let response = dispatch(state, conn, request, &snapshot, &matched, recorder).await;
    recorder.record_response(response.status().as_u16());
    response
}

/// The counter blocks one request writes to.
///
/// Always the route's own block; additionally the route's canary block when the
/// canary took this request. Both, rather than one or the other — see
/// [`RouteSlot`] for why the totals have to stay the totals.
#[derive(Debug, Clone, Copy, Default)]
struct Recorder<'t> {
    route: Option<&'t RouteCounters>,
    canary: Option<&'t RouteCounters>,
}

impl<'t> Recorder<'t> {
    fn new(slot: Option<&'t RouteSlot>, shard: usize, canaried: bool) -> Self {
        Recorder {
            route: slot.map(|slot| slot.shard(shard)),
            canary: canaried.then(|| slot.map(|slot| slot.canary_shard(shard))).flatten(),
        }
    }

    #[inline]
    fn record_response(&self, status: u16) {
        if let Some(counters) = self.route {
            counters.record_response(status);
        }
        if let Some(counters) = self.canary {
            counters.record_response(status);
        }
    }

    #[inline]
    fn record_upstream_latency(&self, elapsed: std::time::Duration) {
        if let Some(counters) = self.route {
            counters.record_upstream_latency(elapsed);
        }
        if let Some(counters) = self.canary {
            counters.record_upstream_latency(elapsed);
        }
    }
}

/// Everything after the route is known: header rewriting, endpoint selection,
/// and the upstream exchange.
async fn dispatch(
    state: &ProxyState,
    conn: ConnInfo,
    request: Request<ProxyBody>,
    snapshot: &RouteTable,
    matched: &Matched<'_>,
    recorder: Recorder<'_>,
) -> Response<ProxyBody> {
    let backend = matched.backend;
    let protocol = backend.protocol();

    // Only for an HTTP/1.1 backend. An h2c backend is exactly what gRPC needs,
    // and refusing there would be refusing the feature.
    if protocol == BackendProtocol::Http1 && is_grpc(request.headers()) {
        state.metrics.record_unsupported_grpc();
        return unsupported_response(StatusCode::BAD_GATEWAY, BODY_GRPC, UNSUPPORTED_GRPC);
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

    match protocol {
        BackendProtocol::Http1 => {
            if let Some(upgraded) = &upgrade {
                headers::restore_upgrade(&mut parts.headers, upgraded);
            }
            // An HTTP/2 request carries its host in `:authority` and has no
            // `Host` header at all. Downgrading it to HTTP/1.1 without restoring
            // `Host` would let hyper's client fill one in from the endpoint's
            // `ip:port`, which is exactly the rewrite this proxy promises not to
            // do.
            if !parts.headers.contains_key(header::HOST) {
                if let Some(host) = forwarded_host {
                    parts.headers.insert(header::HOST, host);
                }
            }
            parts.version = Version::HTTP_11;
        }
        BackendProtocol::H2c => {
            // `:authority` comes from the request URI, which is rewritten to the
            // endpoint below because that is what keys the pool. A `Host` header
            // saying something else is a request a server may treat as
            // malformed, so it goes; `X-Forwarded-Host` already carries the name
            // the client used. `Connection`/`Upgrade` stay stripped for the same
            // class of reason — HTTP/2 forbids them outright.
            parts.headers.remove(header::HOST);
            parts.version = Version::HTTP_2;
        }
    }

    let path_and_query = parts.uri.path_and_query().cloned();

    // Mirroring happens here, after the forwarded headers are on and before the
    // request target is rewritten to an endpoint: the copy should carry exactly
    // the headers the real backend will see, and its own URI.
    //
    // The order relative to the primary's dispatch is not observable. Queueing
    // is a `try_send` into a bounded channel — no await, no allocation past the
    // job itself — so the only part of this that can cost the primary anything
    // is reading a body, which has to happen before the primary is dispatched
    // in any case because the primary needs those same bytes.
    let body = match mirror_target(state, snapshot, matched) {
        None => body,
        Some(target) => {
            mirror_request(state, snapshot, &target, &parts, path_and_query.as_ref(), body).await
        }
    };

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
        match state.upstream.send(protocol, request).await {
            Ok(response) => {
                let elapsed = started.elapsed();
                state.metrics.record_upstream_latency(elapsed);
                recorder.record_upstream_latency(elapsed);
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

/// What the router said, in the pieces the request path needs.
struct Matched<'t> {
    /// Where to forward, after any canary decision.
    backend: &'t Backend,
    /// The matched rule's index into the table's per-route counters, or `None`
    /// when the default backend answered and there is no rule to attribute the
    /// request to.
    route: Option<u32>,
    /// Whether the canary took this request.
    ///
    /// Only the *attribution* depends on this. Which route the request counts
    /// against does not — see [`select_backend`].
    canaried: bool,
    /// The rule's mirror, present only when this request was sampled for it.
    ///
    /// The roll happens during matching, alongside the canary's, so that the
    /// snapshot is consulted exactly once and the request path never has to ask
    /// the table a second question.
    mirror: Option<&'t MirrorSpec>,
}

/// Where a sampled copy is going.
struct MirrorTarget<'t> {
    mirror: &'t Mirror,
    backend: &'t Backend,
    host: Option<&'t str>,
}

/// Matches the request and applies any canary and mirror on the matched rule.
///
/// The returned references borrow the snapshot, not the request, so the caller
/// is free to take the request apart afterwards.
///
/// A canary that diverts a request does **not** change which route it is
/// counted against: the request matched that rule, and moving its numbers to a
/// second route the moment somebody starts a canary would break the graph an
/// operator is watching precisely then. What the canary decision does change is
/// which *blocks* of that one route are written — the route's own always, and
/// the route's canary block as well when the canary took it — so the split is
/// available without the totals ever moving.
fn select_backend<'t>(table: &'t RouteTable, request: &Request<ProxyBody>) -> Option<Matched<'t>> {
    let matched = table.match_request(request_authority(request), request.uri().path())?;
    let route = matched.rule().map(|rule| rule.stats_index());

    // Independent of the canary: a mirror belongs to the rule, so a request the
    // canary diverted is sampled on exactly the same terms as a stable one.
    let mirror = matched
        .mirror()
        .filter(|spec| spec.sample(rng::below(MIRROR_PERCENT_TOTAL)));

    let Some(canary) = matched.canary() else {
        return Some(Matched {
            backend: matched.backend(),
            route,
            canaried: false,
            mirror,
        });
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

    let diverted = canary.decide(header_value, cookie_value, roll);
    // A canary naming a backend the table does not hold is a controller bug,
    // not a reason to fail the request: serve production instead. It is also
    // not canary traffic, because it did not reach the canary.
    let canary_backend = diverted.then(|| table.backend(canary.backend())).flatten();
    Some(Matched {
        backend: canary_backend.unwrap_or(matched.backend()),
        route,
        canaried: canary_backend.is_some(),
        mirror,
    })
}

/// Resolves a sampled request's mirror into the backend it goes to.
///
/// `None` — and so no copy — when the route has no mirror, this request was not
/// sampled, or the runtime has no mirror worker. A mirror whose backend has no
/// ready endpoints is counted as a failure rather than ignored: an operator who
/// configured a mirror and sees no copies arriving should be able to tell "the
/// shadow Service has no ready pods" from "the annotation never took effect".
fn mirror_target<'t>(
    state: &'t ProxyState,
    snapshot: &'t RouteTable,
    matched: &Matched<'t>,
) -> Option<MirrorTarget<'t>> {
    let spec = matched.mirror?;
    let mirror = state.mirror.as_ref()?;
    let backend = snapshot.backend(spec.backend())?;
    if backend.endpoints().is_empty() {
        state.metrics.record_mirror_failure();
        return None;
    }
    Some(MirrorTarget {
        mirror,
        backend,
        host: spec.host(),
    })
}

/// Builds the copy and hands it to the queue, returning the body the primary
/// should now send.
///
/// The return is the whole reason this is not simply a `spawn`: reading a body
/// consumes it, so whatever was read has to come back out and go to the real
/// upstream. A body that fit the cap comes back as the bytes both copies share;
/// one that did not comes back as those bytes followed by the rest of the
/// stream, with no copy made.
async fn mirror_request(
    state: &ProxyState,
    snapshot: &RouteTable,
    target: &MirrorTarget<'_>,
    parts: &Parts,
    path_and_query: Option<&http::uri::PathAndQuery>,
    body: ProxyBody,
) -> ProxyBody {
    // The common case by a wide margin, and the only one that costs nothing: a
    // request with no body needs no buffering, so a mirrored `GET` also keeps
    // the endpoint failover that buffering would have taken away.
    let (primary, copy) = if body.is_known_empty() {
        (body, Some(Bytes::new()))
    } else {
        match mirror::buffer(body, target.mirror.max_body()).await {
            Buffered::Complete(bytes) => (ProxyBody::once(bytes.clone()), Some(bytes)),
            Buffered::TooLarge(prefix, rest) => {
                state.metrics.record_mirror_skipped();
                (ProxyBody::prefixed(prefix, rest), None)
            }
        }
    };

    let Some(copy) = copy else {
        return primary;
    };

    let Some((index, _)) = select_endpoint(target.backend, snapshot.stats(), rng::next_u64())
    else {
        state.metrics.record_mirror_failure();
        return primary;
    };
    let Some(endpoint) = target.backend.endpoints().get(index) else {
        state.metrics.record_mirror_failure();
        return primary;
    };
    let Some(uri) = endpoint_uri(endpoint.addr, path_and_query) else {
        state.metrics.record_mirror_failure();
        return primary;
    };

    let mut copy_parts = parts.clone();
    copy_parts.uri = uri;
    copy_parts.headers.insert(MIRRORED_BY, MIRRORED_BY_VALUE);
    if let Some(host) = target.host.and_then(|h| HeaderValue::from_str(h).ok()) {
        copy_parts.headers.insert(header::HOST, host);
    }
    // The shadow backend carries its own annotation, so the copy is re-versioned
    // for *its* protocol rather than inheriting the primary's. `parts` was
    // rewritten for the primary before this function was called, which is what
    // makes a copy of an HTTP/1.1 request to an h2c shadow — or the reverse —
    // need fixing up here rather than being correct by accident.
    let protocol = target.backend.protocol();
    match protocol {
        BackendProtocol::Http1 => copy_parts.version = Version::HTTP_11,
        BackendProtocol::H2c => {
            // Same reasoning as the primary path: `:authority` comes from the
            // URI, and `mirror-host` was just written into `Host`, so keeping it
            // would send two disagreeing authorities. `mirror-host` is a
            // deliberate override, so it goes into `X-Forwarded-Host` where an
            // h2 backend can still read it.
            if let Some(host) = copy_parts.headers.remove(header::HOST) {
                copy_parts.headers.insert(&headers::X_FORWARDED_HOST, host);
            }
            copy_parts.version = Version::HTTP_2;
        }
    }
    // Whatever is in here describes the downstream connection, and an upgrade
    // handle in particular must not be duplicated into a request nobody reads.
    copy_parts.extensions.clear();

    target
        .mirror
        .enqueue(&state.metrics, copy_parts, copy, protocol);
    primary
}

/// The host the client addressed.
///
/// HTTP/2 puts it in `:authority`, which hyper surfaces as the URI's authority;
/// HTTP/1.1 puts it in `Host`, except in the absolute-form request target,
/// where RFC 7230 §5.4 says the target wins. Checking the URI first gets all
/// three right.
fn request_authority(request: &Request<ProxyBody>) -> &str {
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
fn client_host(request: &Request<ProxyBody>) -> Option<HeaderValue> {
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

/// The same, additionally naming the capability that was missing.
///
/// See [`UNSUPPORTED_HEADER`].
pub fn unsupported_response(
    status: StatusCode,
    body: &'static [u8],
    feature: &'static str,
) -> Response<ProxyBody> {
    let mut response = static_response(status, body);
    if let (Ok(name), Ok(value)) = (
        header::HeaderName::from_bytes(UNSUPPORTED_HEADER.as_bytes()),
        HeaderValue::from_str(feature),
    ) {
        response.headers_mut().insert(name, value);
    }
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
