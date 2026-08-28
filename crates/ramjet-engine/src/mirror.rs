//! Handing a sampled copy of a request to the mirror worker.
//!
//! # Why this is a bridge and not an implementation
//!
//! `ramjet_proxy::mirror` already owns the hard parts: a bounded queue per
//! serving lane, `try_send` so the request path never waits, responses drained
//! and discarded, failures counted and never propagated. None of that is
//! specific to an engine, and a second copy of it would be a second set of
//! numbers to reconcile.
//!
//! What it needs is a `http::request::Parts`, and this engine has bytes. So
//! this module builds one from the head it has already rewritten — the exact
//! bytes the real backend is about to receive — and hands it over. Nothing here
//! blocks or awaits: [`Mirror::enqueue`](ramjet_proxy::Mirror::enqueue) is a
//! `try_send` on a channel, which is safe to call from a reactor thread even
//! though the worker draining it lives on tokio.
//!
//! # The body, and where this differs from the hyper engine
//!
//! The hyper engine reads the request body up to `--mirror-max-body` *before*
//! dispatching the primary, because it has to: a body is a stream that can only
//! be consumed once, so the copy has to be taken before the original is handed
//! to the upstream client.
//!
//! This engine already has the body in a buffer on its way past — it moves
//! bytes from the client's inbox to the upstream's outbox — so the copy is
//! taken as those bytes stream through and the mirror is queued once the body
//! is complete. The primary is never held up at all, which is strictly better
//! than the hyper path rather than a compromise with it. Past the cap the copy
//! is dropped and counted as skipped, exactly as it is there.

use std::net::SocketAddr;

use bytes::Bytes;
use http::header::{HeaderName, HeaderValue};
use http::request::Parts;
use ramjet_proxy::mirror::{MIRRORED_BY, MIRRORED_BY_VALUE};
use ramjet_proxy::upstream::endpoint_uri;
use ramjet_proxy::{Metrics, Mirror};
use ramjet_router::BackendProtocol;

use crate::codec::{parse_request_head, Head, StartLine};

/// The mirror queue an engine hands sampled copies to, with the counters its
/// worker writes.
///
/// The two travel together because they have to: the worker records what
/// happened to a copy after the request that produced it is long gone, so the
/// counters cannot live on the connection or on a per-core block. They are the
/// hyper engine's `Metrics`, shared, which is also what makes the numbers the
/// same shape on both lanes.
#[derive(Debug, Clone)]
pub struct MirrorLane {
    mirror: Mirror,
    metrics: std::sync::Arc<Metrics>,
}

impl MirrorLane {
    /// A lane over an already-started worker.
    pub fn new(mirror: Mirror, metrics: std::sync::Arc<Metrics>) -> MirrorLane {
        MirrorLane { mirror, metrics }
    }

    /// The counters the worker writes, for the exposition to read.
    pub fn metrics(&self) -> &std::sync::Arc<Metrics> {
        &self.metrics
    }

    /// The body cap in force.
    pub fn max_body(&self) -> usize {
        self.mirror.max_body()
    }

    /// A copy whose body was too large to keep.
    pub fn skipped(&self) {
        self.metrics.record_mirror_skipped();
    }

    /// A mirror that had nowhere to send its copy.
    pub fn failed(&self) {
        self.metrics.record_mirror_failure();
    }

    /// Queue one copy, to be sent with the protocol its own backend declares.
    ///
    /// The protocol is the *mirror* backend's, not the primary's. The worker
    /// draining this queue lives on the tokio side and can dial either, so a
    /// shadow Service annotated `h2c` is reached the way it asked to be even
    /// though the engine that sampled the request speaks HTTP/1.1.
    ///
    /// Never blocks and never fails: a full queue is a counted drop, which is
    /// the whole point of the bound.
    pub fn enqueue(&self, parts: Parts, body: Bytes, protocol: BackendProtocol) {
        self.mirror.enqueue(&self.metrics, parts, body, protocol);
    }
}

/// Turn a rewritten request head into the `Parts` a mirrored copy needs.
///
/// `head_bytes` is this engine's own output — the head the real backend is
/// about to receive, `X-Forwarded-*` and `X-Request-Id` included — so the copy
/// carries exactly the headers the primary does. Re-parsing our own bytes
/// rather than building from the original head is deliberate: it is the only
/// way to be sure the two are the same, and it happens once per *sampled*
/// request rather than once per request.
///
/// Returns `None` when the head cannot be reproduced as an `http` type, which
/// means a header name or value this engine forwards verbatim is one the `http`
/// crate refuses. That is a copy not made, never a request not served.
pub fn parts_for(
    head_bytes: &[u8],
    endpoint: SocketAddr,
    host_override: Option<&str>,
) -> Option<Parts> {
    let mut head = Head::default();
    if !parse_request_head(head_bytes, &mut head).ok()? {
        return None;
    }
    let StartLine::Request { method, target, .. } = head.start else {
        return None;
    };

    let method = http::Method::from_bytes(method.bytes(head_bytes)).ok()?;
    let path_and_query: http::uri::PathAndQuery =
        std::str::from_utf8(target.bytes(head_bytes)).ok()?.parse().ok()?;
    let uri = endpoint_uri(endpoint, Some(&path_and_query))?;

    let mut request = http::Request::builder().method(method).uri(uri);
    for (name, value) in head.iter(head_bytes) {
        let Ok(name) = HeaderName::from_bytes(name) else {
            continue;
        };
        let Ok(value) = HeaderValue::from_bytes(value) else {
            continue;
        };
        // `Host` is replaced rather than appended when the mirror names its
        // own; a copy with two `Host` headers is a copy the shadow rejects.
        if name == http::header::HOST && host_override.is_some() {
            continue;
        }
        request = request.header(name, value);
    }
    if let Some(host) = host_override.and_then(|h| HeaderValue::from_str(h).ok()) {
        request = request.header(http::header::HOST, host);
    }
    // The marker a shadow backend reads before it decides whether to charge
    // somebody's card.
    request = request.header(MIRRORED_BY, MIRRORED_BY_VALUE);

    Some(request.body(()).ok()?.into_parts().0)
}

#[cfg(test)]
mod tests {
    use super::*;

    const HEAD: &[u8] = b"GET /api/v1?limit=10 HTTP/1.1\r\nHost: app.example.com\r\n\
                          X-Forwarded-For: 198.51.100.4\r\nX-Request-Id: abc123\r\n\r\n";

    fn endpoint() -> SocketAddr {
        SocketAddr::from(([10, 0, 0, 5], 8080))
    }

    #[test]
    fn a_copy_carries_the_headers_the_primary_will_see() {
        let parts = parts_for(HEAD, endpoint(), None).expect("parts");
        assert_eq!(parts.method, http::Method::GET);
        assert_eq!(parts.uri.to_string(), "http://10.0.0.5:8080/api/v1?limit=10");
        assert_eq!(
            parts.headers.get("x-forwarded-for").map(|v| v.as_bytes()),
            Some(&b"198.51.100.4"[..]),
            "the copy must carry the trail the primary carries"
        );
        assert_eq!(
            parts.headers.get("x-request-id").map(|v| v.as_bytes()),
            Some(&b"abc123"[..]),
            "one request id, so both hops correlate"
        );
    }

    #[test]
    fn a_copy_is_marked_as_one() {
        let parts = parts_for(HEAD, endpoint(), None).expect("parts");
        assert_eq!(
            parts.headers.get(MIRRORED_BY),
            Some(&MIRRORED_BY_VALUE),
            "a shadow backend has to be able to tell a copy from the real thing"
        );
    }

    #[test]
    fn a_host_override_replaces_rather_than_appends() {
        let parts = parts_for(HEAD, endpoint(), Some("shadow.example.com")).expect("parts");
        let hosts: Vec<_> = parts.headers.get_all("host").iter().collect();
        assert_eq!(hosts.len(), 1, "two Host headers is a copy nobody accepts");
        assert_eq!(hosts[0].as_bytes(), b"shadow.example.com");
    }

    #[test]
    fn the_client_host_stands_when_no_override_is_set() {
        let parts = parts_for(HEAD, endpoint(), None).expect("parts");
        assert_eq!(
            parts.headers.get("host").map(|v| v.as_bytes()),
            Some(&b"app.example.com"[..])
        );
    }

    #[test]
    fn a_head_that_is_not_one_makes_no_copy() {
        assert!(parts_for(b"not a request head at all", endpoint(), None).is_none());
        assert!(parts_for(b"GET /partial HTTP/1.1\r\n", endpoint(), None).is_none());
    }
}
