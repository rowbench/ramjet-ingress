//! Dialling upstream endpoints, and the pool in front of them.
//!
//! # Why hyper's pool and not a bespoke one
//!
//! Connection reuse is the single largest lever on upstream latency: a fresh
//! TCP connection to a pod costs a round trip before a byte of the request
//! moves, and at ingress volumes that round trip is most of the p50. hyper's
//! legacy client already implements a correct pool — checkout, idle timeout,
//! per-authority limits, and the awkward case where an upstream closes an idle
//! connection at the same moment it is checked out — and the failure modes of
//! getting that wrong are subtle enough that reimplementing it would be a bad
//! trade for a data plane whose interesting work is elsewhere.
//!
//! The pool is keyed by URI authority, and every request is dispatched to a
//! literal `ip:port`, so a pool entry is exactly one endpoint. Endpoint
//! selection stays entirely in `ramjet-router`; this module never chooses
//! anything.
//!
//! # Two timeouts, deliberately separate
//!
//! `connect_timeout` bounds the TCP handshake and is short (5s), because a pod
//! that has not accepted in five seconds is not going to. `response_timeout`
//! bounds the wait for *response headers* and is long (60s), because a slow
//! endpoint is still a working endpoint. Neither bounds the body: a large
//! download is not a stalled upstream, and a single "request timeout" that
//! covers both cannot tell them apart without capping how much anyone can
//! download.
//!
//! # Two pools, one per upstream protocol
//!
//! Downstream speaks HTTP/1.1, HTTP/2 and HTTP/3; upstream speaks HTTP/1.1 by
//! default and cleartext HTTP/2 for a backend that asked for it. The choice is a
//! property of the *backend*, not of the request — see
//! [`BackendProtocol`](ramjet_router::BackendProtocol) — so the two directions
//! are fully crossed: an HTTP/1.1 client reaches an h2c backend, and an HTTP/2
//! client reaches an HTTP/1.1 one.
//!
//! The two pools are separate clients rather than one client with negotiation,
//! because there is nothing to negotiate. h2c with prior knowledge means sending
//! the HTTP/2 connection preface at a server already known to speak it; there is
//! no ALPN on a cleartext socket and the upgrade dance in RFC 7540 §3.2 is
//! deprecated and unimplemented by most servers. A backend is one or the other,
//! the controller says which, and a client that speaks the wrong one to a socket
//! produces an error at the first frame rather than a subtly wrong request.
//!
//! ## Why the h2 pool is sized the way it is
//!
//! `pool_max_idle_per_host` is a per-endpoint ceiling on *connections*, and h2
//! multiplexes: hyper's pool marks an HTTP/2 connection shareable and hands the
//! same one to every concurrent request for that endpoint, so the steady state
//! is one connection per endpoint carrying every stream. That is the point of
//! HTTP/2, and it is why the number that keeps the HTTP/1.1 pool from churning
//! (see [`DEFAULT_POOL_MAX_IDLE_PER_HOST`]) is simply not the binding constraint
//! here. What binds instead is the server's `SETTINGS_MAX_CONCURRENT_STREAMS`,
//! which is the upstream's to choose and which hyper already respects by opening
//! a second connection when the first is saturated.

use std::net::SocketAddr;
use std::time::Duration;

use http::uri::{PathAndQuery, Uri};
use http::Request;
use hyper::body::Incoming;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::client::legacy::{Client, Error as LegacyError};
use hyper_util::rt::TokioExecutor;
use ramjet_router::BackendProtocol;

use crate::body::ProxyBody;

/// Idle upstream connections kept per endpoint, by default.
///
/// This is a **ceiling, not a reservation**: hyper opens a connection only when
/// a request needs one, so the number here costs nothing until the traffic
/// exists to fill it. That asymmetry is why it is set generously.
///
/// Sizing it below the requests an endpoint has in flight is not a smaller
/// pool, it is connection churn: every request past the limit returns its
/// connection to a full pool, the connection is closed, and the next request
/// pays a TCP handshake on the request path. The first version of this crate
/// used 32, and `bench/RESULTS.md` measured the consequence — a new upstream
/// connection every ~590 requests at c64 over two endpoints, against nginx's
/// one every ~28,700.
///
/// 128 is above the in-flight count any single endpoint sees in normal ingress
/// traffic, and the cost of being wrong in this direction is file descriptors
/// that are never opened.
pub const DEFAULT_POOL_MAX_IDLE_PER_HOST: usize = 128;

/// Timeouts and pool sizing for upstream connections.
#[derive(Debug, Clone, Copy)]
pub struct UpstreamConfig {
    /// Bound on establishing a TCP connection to an endpoint.
    pub connect_timeout: Duration,
    /// Bound on receiving response *headers* after dispatch.
    pub response_timeout: Duration,
    /// How long an unused pooled connection is kept.
    pub pool_idle_timeout: Duration,
    /// Idle connections kept per endpoint.
    pub pool_max_idle_per_host: usize,
    /// TCP keepalive probe interval for upstream connections.
    pub tcp_keepalive: Option<Duration>,
    /// Endpoints tried before giving up, when the failure is a connect failure
    /// and the request is safe to re-dispatch.
    ///
    /// Capped rather than "every endpoint" because the two connect failures
    /// look nothing alike: `ECONNREFUSED` from a terminating pod returns
    /// instantly, while a pod whose node has dropped off the network burns the
    /// full `connect_timeout`. Walking forty endpoints of the second kind
    /// spends ten minutes on a request the client abandoned in the first ten
    /// seconds. Three attempts covers the case failover exists for — one or two
    /// endpoints draining during a rollout — and refuses to turn a partitioned
    /// node into a request that never ends.
    pub max_connect_attempts: usize,
}

impl Default for UpstreamConfig {
    fn default() -> Self {
        UpstreamConfig {
            connect_timeout: Duration::from_secs(5),
            response_timeout: Duration::from_secs(60),
            // Longer than the 60s idle timeout of most application servers
            // would be a mistake: the pool would hand out connections the
            // upstream has already closed. 90s matches Go's default and sits
            // under Kubernetes' typical conntrack expiry.
            pool_idle_timeout: Duration::from_secs(90),
            pool_max_idle_per_host: DEFAULT_POOL_MAX_IDLE_PER_HOST,
            tcp_keepalive: Some(Duration::from_secs(60)),
            max_connect_attempts: 3,
        }
    }
}

/// Why an upstream exchange failed.
///
/// The distinction that matters is [`Connect`](UpstreamError::Connect): nothing
/// was written to the endpoint, so the request is untouched and may be retried
/// against a different one. Every other variant means bytes may already have
/// been sent, and replaying them would be a duplicate request.
#[derive(Debug)]
pub enum UpstreamError {
    /// The TCP connection could not be established.
    Connect(LegacyError),
    /// No response headers arrived before `response_timeout`.
    Timeout,
    /// The connection was established but the exchange failed.
    Transport(LegacyError),
}

impl UpstreamError {
    /// Whether nothing was sent, and so a different endpoint may be tried.
    pub fn is_retryable(&self) -> bool {
        matches!(self, UpstreamError::Connect(_))
    }
}

impl std::fmt::Display for UpstreamError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            UpstreamError::Connect(e) => write!(f, "upstream connect failed: {e}"),
            UpstreamError::Timeout => f.write_str("upstream response timed out"),
            UpstreamError::Transport(e) => write!(f, "upstream exchange failed: {e}"),
        }
    }
}

impl std::error::Error for UpstreamError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            UpstreamError::Connect(e) | UpstreamError::Transport(e) => Some(e),
            UpstreamError::Timeout => None,
        }
    }
}

/// Pooled clients for upstream endpoints: one HTTP/1.1, one cleartext HTTP/2.
///
/// Both are built at startup whether or not the published table has any h2c
/// backend. A `Client` that has never been asked for a connection holds an empty
/// pool and one `Arc`, so the alternative — building the h2 client lazily, or
/// only when a generation happens to contain an h2c backend — would trade a few
/// hundred bytes for a rebuild that has to reach into the data plane.
#[derive(Debug, Clone)]
pub struct Upstream {
    http1: Client<HttpConnector, ProxyBody>,
    h2c: Client<HttpConnector, ProxyBody>,
    response_timeout: Duration,
    max_connect_attempts: usize,
}

impl Upstream {
    /// Builds the clients from `config`.
    pub fn new(config: &UpstreamConfig) -> Self {
        let http1 = Client::builder(TokioExecutor::new())
            .pool_idle_timeout(config.pool_idle_timeout)
            .pool_max_idle_per_host(config.pool_max_idle_per_host)
            // hyper's own retry of a request that raced an upstream closing a
            // pooled connection. This is not the endpoint failover in
            // `forward`; it is the same endpoint, and hyper only does it when
            // it knows nothing was written.
            .retry_canceled_requests(true)
            .build(connector(config));

        let h2c = Client::builder(TokioExecutor::new())
            // Prior knowledge: send the preface and never try HTTP/1.1. Without
            // this the client would speak HTTP/1.1 to a cleartext socket, which
            // is precisely the silent downgrade this mode exists to remove.
            .http2_only(true)
            .pool_idle_timeout(config.pool_idle_timeout)
            .pool_max_idle_per_host(config.pool_max_idle_per_host)
            .retry_canceled_requests(true)
            .build(connector(config));

        Upstream {
            http1,
            h2c,
            response_timeout: config.response_timeout,
            max_connect_attempts: config.max_connect_attempts.max(1),
        }
    }

    /// How many endpoints a retryable request may be dispatched to.
    pub fn max_connect_attempts(&self) -> usize {
        self.max_connect_attempts
    }

    /// Sends one request over `protocol` and waits for its response headers.
    ///
    /// The returned body is still streaming; the timeout covers headers only.
    /// For an h2 exchange that means the timeout does not bound a long-lived
    /// stream either — a server-streaming RPC that sends its first message after
    /// ten minutes is a working RPC, and the headers arrived long before it.
    pub async fn send(
        &self,
        protocol: BackendProtocol,
        request: Request<ProxyBody>,
    ) -> Result<http::Response<Incoming>, UpstreamError> {
        let client = match protocol {
            BackendProtocol::Http1 => &self.http1,
            BackendProtocol::H2c => &self.h2c,
        };
        match tokio::time::timeout(self.response_timeout, client.request(request)).await {
            Ok(Ok(response)) => Ok(response),
            Ok(Err(error)) if error.is_connect() => Err(UpstreamError::Connect(error)),
            Ok(Err(error)) => Err(UpstreamError::Transport(error)),
            Err(_elapsed) => Err(UpstreamError::Timeout),
        }
    }
}

/// The TCP connector both pools dial through.
///
/// Identical for the two protocols, because everything it configures is below
/// HTTP: the handshake that differs happens after the socket is up.
fn connector(config: &UpstreamConfig) -> HttpConnector {
    let mut connector = HttpConnector::new();
    connector.set_connect_timeout(Some(config.connect_timeout));
    // The same argument as on the accept side: Nagle delays a small write
    // waiting for company, and a request header block has none coming.
    connector.set_nodelay(true);
    connector.set_keepalive(config.tcp_keepalive);
    // Endpoints are always `ip:port` literals from the route table, so the
    // connector's IP fast path applies and no name resolution happens on
    // the request path.
    connector.enforce_http(true);
    connector
}

/// Builds the absolute URI for dispatching to `endpoint`.
///
/// hyper's client needs an absolute-form URI to pick a pool entry; the path and
/// query come across from the downstream request untouched. `SocketAddr`'s
/// `Display` already brackets IPv6 addresses, which is exactly the authority
/// form a URI wants.
pub fn endpoint_uri(endpoint: SocketAddr, path_and_query: Option<&PathAndQuery>) -> Option<Uri> {
    let path = path_and_query.map_or("/", PathAndQuery::as_str);
    let mut buffer = String::with_capacity(7 + 46 + path.len());
    buffer.push_str("http://");
    // `SocketAddr: Display` writes into a `String` infallibly.
    use std::fmt::Write as _;
    let _ = write!(buffer, "{endpoint}");
    buffer.push_str(path);
    buffer.parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pq(s: &'static str) -> PathAndQuery {
        s.parse().expect("valid path")
    }

    #[test]
    fn builds_an_absolute_uri_with_the_path_intact() {
        let uri = endpoint_uri(
            "10.0.0.5:8080".parse().expect("addr"),
            Some(&pq("/api/v1/users?limit=10")),
        )
        .expect("a uri");
        assert_eq!(uri.to_string(), "http://10.0.0.5:8080/api/v1/users?limit=10");
        assert_eq!(uri.authority().map(|a| a.as_str()), Some("10.0.0.5:8080"));
    }

    #[test]
    fn ipv6_endpoints_are_bracketed() {
        let uri = endpoint_uri("[2001:db8::1]:8080".parse().expect("addr"), Some(&pq("/")))
            .expect("a uri");
        assert_eq!(uri.to_string(), "http://[2001:db8::1]:8080/");
    }

    #[test]
    fn a_missing_path_becomes_root() {
        let uri = endpoint_uri("10.0.0.5:80".parse().expect("addr"), None).expect("a uri");
        assert_eq!(uri.path(), "/");
    }

    #[test]
    fn defaults_bound_connect_tightly_and_responses_loosely() {
        // A pod that has not accepted in five seconds is not going to; a pod
        // that takes thirty seconds to answer is still working.
        let config = UpstreamConfig::default();
        assert_eq!(config.connect_timeout, Duration::from_secs(5));
        assert_eq!(config.response_timeout, Duration::from_secs(60));
        assert!(config.connect_timeout < config.response_timeout);
    }

    #[test]
    fn only_connect_failures_are_retryable() {
        assert!(!UpstreamError::Timeout.is_retryable());
    }
}
