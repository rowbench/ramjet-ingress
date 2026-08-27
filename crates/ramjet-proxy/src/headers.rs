//! Header rewriting on the way in and on the way out.
//!
//! # Hop-by-hop
//!
//! RFC 7230 §6.1 divides headers into end-to-end and hop-by-hop. Hop-by-hop
//! headers describe *this* TCP connection and are meaningless — sometimes
//! actively harmful — on the next one. Forwarding `Transfer-Encoding` verbatim
//! is how a proxy ends up disagreeing with its upstream about where a message
//! ends, which is the entire mechanism behind request smuggling. Forwarding
//! `Connection: close` makes the client's intent about its own connection
//! terminate the pooled upstream connection instead.
//!
//! The list is not fixed: `Connection` itself *names* additional headers that
//! are hop-by-hop for this message, and those have to be removed too. Skipping
//! that step is the most common way a hand-written proxy leaks a hop-by-hop
//! header, so [`strip_hop_by_hop`] removes `Connection` first and then walks
//! the names it listed.
//!
//! # Upgrades are the exception
//!
//! `Connection: upgrade` and `Upgrade: websocket` are hop-by-hop *and* the only
//! way to ask for an upgrade, so an upgrade request has to carry them across
//! the hop on purpose. [`upgrade_protocol`] captures them before the strip and
//! [`restore_upgrade`] puts them back afterwards, which keeps "strip
//! everything" as the default and makes the exception one visible, auditable
//! call rather than a hole in the deny list.
//!
//! # Forwarding headers
//!
//! `X-Forwarded-For` is *appended* to, never replaced: the header is a trail of
//! every proxy the request passed through, and overwriting it destroys the
//! client address whenever ramjet sits behind a cloud load balancer. The rest
//! (`X-Forwarded-Proto`, `X-Forwarded-Host`, `X-Real-IP`) describe this hop and
//! are set, matching ingress-nginx's default header set so an Ingress migrated
//! from it does not need application changes.
//!
//! `Host` is deliberately **not** rewritten. ingress-nginx forwards the
//! client's `Host` upstream by default, and a great many applications route,
//! generate links, or pick a tenant from it.

use std::fmt::Write as _;
use std::net::IpAddr;

use http::header::{self, HeaderMap, HeaderName, HeaderValue};

use crate::rng;

/// `X-Forwarded-For`.
pub const X_FORWARDED_FOR: HeaderName = HeaderName::from_static("x-forwarded-for");
/// `X-Forwarded-Proto`.
pub const X_FORWARDED_PROTO: HeaderName = HeaderName::from_static("x-forwarded-proto");
/// `X-Forwarded-Host`.
pub const X_FORWARDED_HOST: HeaderName = HeaderName::from_static("x-forwarded-host");
/// `X-Real-IP`, which ingress-nginx also sets.
pub const X_REAL_IP: HeaderName = HeaderName::from_static("x-real-ip");
/// `X-Request-Id`.
pub const X_REQUEST_ID: HeaderName = HeaderName::from_static("x-request-id");

/// Headers that describe one connection and must not cross a hop.
///
/// `Proxy-Connection` is not in RFC 7230 — it never was standardised — but it
/// is still emitted by old clients and behaves like `Connection`, so it is
/// dropped alongside the real list rather than forwarded as an end-to-end
/// header.
const HOP_BY_HOP: [HeaderName; 8] = [
    HeaderName::from_static("keep-alive"),
    HeaderName::from_static("proxy-connection"),
    header::PROXY_AUTHENTICATE,
    header::PROXY_AUTHORIZATION,
    header::TE,
    header::TRAILER,
    header::TRANSFER_ENCODING,
    header::UPGRADE,
];

/// Removes every hop-by-hop header, including the ones `Connection` names.
///
/// `Connection` is removed first so its value can be read while the headers it
/// lists are removed — the borrow checker will not allow iterating the map and
/// mutating it at the same time, and taking the value out is cheaper than
/// cloning it.
pub fn strip_hop_by_hop(headers: &mut HeaderMap) {
    let connection = headers.remove(header::CONNECTION);
    if let Some(value) = &connection {
        for token in value.as_bytes().split(|b| *b == b',') {
            let token = token.trim_ascii();
            if token.is_empty() {
                continue;
            }
            if let Ok(name) = HeaderName::from_bytes(token) {
                headers.remove(&name);
            }
        }
    }
    for name in HOP_BY_HOP {
        headers.remove(name);
    }
}

/// The protocol an upgrade request or `101` response is asking for.
///
/// Returns `None` unless `Connection` actually lists the `upgrade` token: an
/// `Upgrade` header on its own is advisory and must not cause the hop-by-hop
/// headers to be preserved.
pub fn upgrade_protocol(headers: &HeaderMap) -> Option<HeaderValue> {
    let requested = headers.get_all(header::CONNECTION).iter().any(|value| {
        value
            .as_bytes()
            .split(|b| *b == b',')
            .any(|token| token.trim_ascii().eq_ignore_ascii_case(b"upgrade"))
    });
    if !requested {
        return None;
    }
    headers.get(header::UPGRADE).cloned()
}

/// Puts back the two headers an upgrade needs, after a strip removed them.
pub fn restore_upgrade(headers: &mut HeaderMap, protocol: &HeaderValue) {
    headers.insert(header::CONNECTION, HeaderValue::from_static("upgrade"));
    headers.insert(header::UPGRADE, protocol.clone());
}

/// Adds the forwarding headers for this hop.
///
/// `authority` is the `Host` header or `:authority` value the client sent, used
/// for `X-Forwarded-Host`. A header value that is not valid UTF-8, or an
/// authority long enough to exceed a header value's limits, is skipped rather
/// than failing the request: a missing `X-Forwarded-Host` is a smaller problem
/// than a 500.
pub fn apply_forwarded(
    headers: &mut HeaderMap,
    client: IpAddr,
    scheme: &'static str,
    authority: Option<HeaderValue>,
) {
    let mut ip = StackStr::<48>::new();
    let _ = write!(ip, "{client}");

    // Join every existing value, not just the first: a client is free to send
    // the trail as several headers, and keeping only one would drop hops.
    let mut trail: Option<Vec<u8>> = None;
    for existing in headers.get_all(&X_FORWARDED_FOR) {
        let buffer = trail.get_or_insert_with(|| Vec::with_capacity(existing.len() + 64));
        if !buffer.is_empty() {
            buffer.extend_from_slice(b", ");
        }
        buffer.extend_from_slice(existing.as_bytes());
    }
    match &mut trail {
        Some(buffer) => {
            buffer.extend_from_slice(b", ");
            buffer.extend_from_slice(ip.as_bytes());
            if let Ok(value) = HeaderValue::from_bytes(buffer) {
                headers.insert(&X_FORWARDED_FOR, value);
            }
        }
        None => {
            if let Ok(value) = HeaderValue::from_bytes(ip.as_bytes()) {
                headers.insert(&X_FORWARDED_FOR, value);
            }
        }
    }

    if let Ok(value) = HeaderValue::from_bytes(ip.as_bytes()) {
        headers.insert(&X_REAL_IP, value);
    }
    headers.insert(&X_FORWARDED_PROTO, HeaderValue::from_static(scheme));
    if let Some(authority) = authority {
        headers.insert(&X_FORWARDED_HOST, authority);
    }
}

/// Returns the request's `X-Request-Id`, generating and inserting one if the
/// client did not supply it.
///
/// Reusing an incoming id is what makes a trace stitch together across an edge
/// load balancer, a service mesh, and the application's own logs; generating a
/// fresh one per hop would give every component a different name for the same
/// request.
pub fn ensure_request_id(headers: &mut HeaderMap) -> HeaderValue {
    if let Some(existing) = headers.get(&X_REQUEST_ID) {
        if !existing.is_empty() {
            return existing.clone();
        }
    }
    let mut hex = [0u8; 32];
    rng::hex_id(&mut hex);
    // 32 lowercase hex bytes are unconditionally a valid header value, but
    // `from_bytes` still returns a `Result`; falling back to a constant keeps
    // this function total without an `expect`.
    let value = HeaderValue::from_bytes(&hex).unwrap_or(HeaderValue::from_static("0"));
    headers.insert(&X_REQUEST_ID, value.clone());
    value
}

/// The value of the cookie called `name`, if the request carries it.
///
/// Cookie names are case-sensitive, so the comparison is too. The value is
/// borrowed straight out of the header, which is what lets a canary decision
/// run without allocating.
pub fn cookie_value<'h>(headers: &'h HeaderMap, name: &str) -> Option<&'h str> {
    for value in headers.get_all(header::COOKIE) {
        let Ok(raw) = value.to_str() else { continue };
        for pair in raw.split(';') {
            let Some((key, value)) = pair.split_once('=') else {
                continue;
            };
            if key.trim() == name {
                return Some(value.trim());
            }
        }
    }
    None
}

/// A fixed-size `fmt::Write` target, so formatting an address does not allocate.
///
/// Writes past the end are dropped rather than panicking; every caller here
/// sizes the buffer for its worst case (an IPv6 address with a scope id fits in
/// 48 bytes), so a truncation would already be a bug elsewhere.
struct StackStr<const N: usize> {
    buffer: [u8; N],
    len: usize,
}

impl<const N: usize> StackStr<N> {
    fn new() -> Self {
        StackStr {
            buffer: [0u8; N],
            len: 0,
        }
    }

    fn as_bytes(&self) -> &[u8] {
        self.buffer.get(..self.len).unwrap_or_default()
    }
}

impl<const N: usize> std::fmt::Write for StackStr<N> {
    fn write_str(&mut self, s: &str) -> std::fmt::Result {
        let end = self.len + s.len();
        match self.buffer.get_mut(self.len..end) {
            Some(slot) => {
                slot.copy_from_slice(s.as_bytes());
                self.len = end;
                Ok(())
            }
            None => Err(std::fmt::Error),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn map(pairs: &[(&str, &str)]) -> HeaderMap {
        let mut headers = HeaderMap::new();
        for (name, value) in pairs {
            headers.append(
                HeaderName::from_bytes(name.as_bytes()).expect("valid name"),
                HeaderValue::from_str(value).expect("valid value"),
            );
        }
        headers
    }

    #[test]
    fn strips_the_fixed_list() {
        let mut headers = map(&[
            ("connection", "close"),
            ("transfer-encoding", "chunked"),
            ("te", "trailers"),
            ("keep-alive", "timeout=5"),
            ("proxy-connection", "keep-alive"),
            ("content-type", "text/plain"),
        ]);
        strip_hop_by_hop(&mut headers);
        assert!(headers.get(header::CONNECTION).is_none());
        assert!(headers.get(header::TRANSFER_ENCODING).is_none());
        assert!(headers.get(header::TE).is_none());
        assert!(headers.get("keep-alive").is_none());
        assert!(headers.get("proxy-connection").is_none());
        assert_eq!(headers.get(header::CONTENT_TYPE).map(|v| v.as_bytes()), Some(&b"text/plain"[..]));
    }

    /// The subtle half of RFC 7230 §6.1: `Connection` names further headers
    /// that are hop-by-hop for this message only.
    #[test]
    fn strips_headers_that_connection_names() {
        let mut headers = map(&[
            ("connection", "x-internal-token, close"),
            ("x-internal-token", "secret"),
            ("x-keep-me", "yes"),
        ]);
        strip_hop_by_hop(&mut headers);
        assert!(
            headers.get("x-internal-token").is_none(),
            "a Connection-listed header leaked to the upstream"
        );
        assert!(headers.get("x-keep-me").is_some());
    }

    #[test]
    fn upgrade_needs_the_connection_token() {
        let with_token = map(&[("connection", "Upgrade"), ("upgrade", "websocket")]);
        assert_eq!(
            upgrade_protocol(&with_token).map(|v| v.as_bytes().to_vec()),
            Some(b"websocket".to_vec())
        );

        // `Upgrade` alone is advisory; it must not keep the hop-by-hop headers
        // alive, or a plain request would be forwarded with them.
        let advisory = map(&[("upgrade", "websocket")]);
        assert!(upgrade_protocol(&advisory).is_none());
    }

    #[test]
    fn upgrade_token_is_found_among_others() {
        let headers = map(&[("connection", "keep-alive, Upgrade"), ("upgrade", "h2c")]);
        assert!(upgrade_protocol(&headers).is_some());
    }

    #[test]
    fn upgrade_survives_a_strip_and_restore() {
        let mut headers = map(&[("connection", "upgrade"), ("upgrade", "websocket")]);
        let protocol = upgrade_protocol(&headers).expect("an upgrade");
        strip_hop_by_hop(&mut headers);
        assert!(headers.get(header::UPGRADE).is_none());
        restore_upgrade(&mut headers, &protocol);
        assert_eq!(
            headers.get(header::CONNECTION).map(|v| v.as_bytes()),
            Some(&b"upgrade"[..])
        );
        assert_eq!(
            headers.get(header::UPGRADE).map(|v| v.as_bytes()),
            Some(&b"websocket"[..])
        );
    }

    #[test]
    fn forwarded_for_is_appended_not_replaced() {
        let mut headers = map(&[("x-forwarded-for", "203.0.113.7")]);
        apply_forwarded(&mut headers, "198.51.100.4".parse().expect("ip"), "https", None);
        assert_eq!(
            headers.get(&X_FORWARDED_FOR).map(|v| v.as_bytes()),
            Some(&b"203.0.113.7, 198.51.100.4"[..])
        );
    }

    #[test]
    fn several_forwarded_for_headers_are_joined() {
        let mut headers = map(&[("x-forwarded-for", "203.0.113.7"), ("x-forwarded-for", "10.1.1.1")]);
        apply_forwarded(&mut headers, "198.51.100.4".parse().expect("ip"), "http", None);
        assert_eq!(headers.get_all(&X_FORWARDED_FOR).iter().count(), 1);
        assert_eq!(
            headers.get(&X_FORWARDED_FOR).map(|v| v.as_bytes()),
            Some(&b"203.0.113.7, 10.1.1.1, 198.51.100.4"[..])
        );
    }

    #[test]
    fn forwarded_for_is_set_when_absent() {
        let mut headers = HeaderMap::new();
        apply_forwarded(&mut headers, "10.0.0.9".parse().expect("ip"), "http", None);
        assert_eq!(
            headers.get(&X_FORWARDED_FOR).map(|v| v.as_bytes()),
            Some(&b"10.0.0.9"[..])
        );
        assert_eq!(
            headers.get(&X_REAL_IP).map(|v| v.as_bytes()),
            Some(&b"10.0.0.9"[..])
        );
        assert_eq!(
            headers.get(&X_FORWARDED_PROTO).map(|v| v.as_bytes()),
            Some(&b"http"[..])
        );
    }

    #[test]
    fn ipv6_clients_format_without_allocating_past_the_buffer() {
        let mut headers = HeaderMap::new();
        apply_forwarded(
            &mut headers,
            "2001:db8::dead:beef".parse().expect("ip"),
            "https",
            None,
        );
        assert_eq!(
            headers.get(&X_FORWARDED_FOR).map(|v| v.as_bytes()),
            Some(&b"2001:db8::dead:beef"[..])
        );
    }

    #[test]
    fn forwarded_host_comes_from_the_authority() {
        let mut headers = HeaderMap::new();
        apply_forwarded(
            &mut headers,
            "10.0.0.1".parse().expect("ip"),
            "https",
            Some(HeaderValue::from_static("shop.example.com")),
        );
        assert_eq!(
            headers.get(&X_FORWARDED_HOST).map(|v| v.as_bytes()),
            Some(&b"shop.example.com"[..])
        );
    }

    #[test]
    fn request_id_is_reused_when_present() {
        let mut headers = map(&[("x-request-id", "abc-123")]);
        let id = ensure_request_id(&mut headers);
        assert_eq!(id.as_bytes(), b"abc-123");
    }

    #[test]
    fn request_id_is_generated_when_absent() {
        let mut headers = HeaderMap::new();
        let first = ensure_request_id(&mut headers);
        assert_eq!(first.len(), 32);
        assert_eq!(headers.get(&X_REQUEST_ID), Some(&first));

        let mut other = HeaderMap::new();
        assert_ne!(ensure_request_id(&mut other), first);
    }

    #[test]
    fn cookies_are_parsed_by_exact_name() {
        let headers = map(&[("cookie", "session=abc; canary=always; other=1")]);
        assert_eq!(cookie_value(&headers, "canary"), Some("always"));
        assert_eq!(cookie_value(&headers, "session"), Some("abc"));
        assert_eq!(cookie_value(&headers, "Canary"), None, "names are case-sensitive");
        assert_eq!(cookie_value(&headers, "missing"), None);
    }

    #[test]
    fn cookies_are_found_across_several_headers() {
        let headers = map(&[("cookie", "a=1"), ("cookie", "canary=never")]);
        assert_eq!(cookie_value(&headers, "canary"), Some("never"));
    }
}
