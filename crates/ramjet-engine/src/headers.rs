//! What a proxy hop does to the headers passing through it.
//!
//! This is a re-implementation of `ramjet_proxy::headers` against a different
//! representation, and the target is not "similar behaviour" but **the same
//! bytes on the wire**. The hyper engine builds an `http::HeaderMap` and lets
//! hyper serialise it; this one writes the head directly into an output buffer.
//! The integration tests run the same assertions against both engines, so a
//! divergence here shows up as a failing test rather than as a support ticket.
//!
//! Two rules carry most of the weight:
//!
//! - **`Connection` names its own hop-by-hop headers.** Stripping the fixed
//!   list and leaving `Connection: x-hop-secret` alone forwards a header that
//!   was explicitly marked as not for forwarding.
//! - **`X-Forwarded-For` accumulates.** Several inbound `X-Forwarded-For` lines
//!   collapse into one, joined by `", "`, with this hop's address appended. A
//!   proxy that replaces the trail instead of extending it erases the client.

use std::net::{IpAddr, SocketAddr};

use crate::codec::head::Span;
use crate::codec::{has_token, trim_ows, Framing, Head, StartLine, Version};
use crate::rng;

/// Headers that describe one connection rather than the message, and so must
/// not be forwarded.
///
/// `connection` itself is handled separately and first, because its *value*
/// names further headers to remove.
const HOP_BY_HOP: [&[u8]; 8] = [
    b"keep-alive",
    b"proxy-connection",
    b"proxy-authenticate",
    b"proxy-authorization",
    b"te",
    b"trailer",
    b"transfer-encoding",
    b"upgrade",
];

/// Headers this hop writes itself, and so never copies from the request.
///
/// `x-forwarded-for` and `x-request-id` are here because they are *derived*
/// from any inbound value rather than replaced by one; the derivation happens
/// before the copy loop runs.
const SELF_WRITTEN: [&[u8]; 5] = [
    b"x-forwarded-for",
    b"x-real-ip",
    b"x-forwarded-proto",
    b"x-forwarded-host",
    b"x-request-id",
];

/// The identity of the hop, for the `X-Forwarded-*` family.
#[derive(Debug, Clone, Copy)]
pub struct Hop {
    /// The client's address, as seen by this proxy.
    pub client: IpAddr,
    /// The scheme the client used: `"https"` when the connection arrived on
    /// the TLS listener, `"http"` otherwise. It follows the connection rather
    /// than the process, because one engine serves both listeners.
    pub scheme: &'static str,
}

/// Whether a header name must not cross this hop.
fn is_hop_by_hop(name: &[u8]) -> bool {
    HOP_BY_HOP.iter().any(|h| name.eq_ignore_ascii_case(h))
}

fn is_self_written(name: &[u8]) -> bool {
    SELF_WRITTEN.iter().any(|h| name.eq_ignore_ascii_case(h))
}

/// Whether some `Connection` header in this head lists `name`.
///
/// Every `Connection` line is scanned, not just the first: a header listed on
/// the second line is exactly as hop-by-hop as one listed on the first.
fn connection_lists(head: &Head, buf: &[u8], name: &[u8]) -> bool {
    head.headers_named(buf, b"connection")
        .any(|(_, value)| has_token(value, name))
}

/// Whether the client asked for a protocol upgrade.
///
/// The `Upgrade` header alone does not count: RFC 9110 requires
/// `Connection: upgrade` beside it, and a bare `Upgrade` is a hint a server may
/// ignore. The hyper engine applies the same test, so the two agree on which
/// requests are forwarded as upgrade attempts.
pub fn wants_upgrade(head: &Head, buf: &[u8]) -> bool {
    connection_lists(head, buf, b"upgrade") && head.header(buf, b"upgrade").is_some()
}

/// The protocol named by an `Upgrade` header, if this message asked for one.
///
/// Returned as bytes out of the caller's buffer rather than a string: the value
/// is forwarded verbatim, and a protocol token this hop does not recognise is
/// still one the two endpoints may have agreed on.
pub fn upgrade_protocol<'a>(head: &'a Head, buf: &'a [u8]) -> Option<&'a [u8]> {
    if !connection_lists(head, buf, b"upgrade") {
        return None;
    }
    let value = trim_ows(head.header(buf, b"upgrade")?);
    (!value.is_empty()).then_some(value)
}

/// Whether a response head is a `101 Switching Protocols`.
pub fn is_switching_protocols(head: &Head) -> bool {
    matches!(head.start, StartLine::Status { code: 101, .. })
}

/// Whether a header should be copied through unchanged.
fn is_forwardable(head: &Head, buf: &[u8], name: &[u8]) -> bool {
    !name.eq_ignore_ascii_case(b"connection")
        && !is_hop_by_hop(name)
        && !is_self_written(name)
        && !connection_lists(head, buf, name)
}

/// Append `name: value\r\n`.
fn field(out: &mut Vec<u8>, name: &[u8], value: &[u8]) {
    out.extend_from_slice(name);
    out.extend_from_slice(b": ");
    out.extend_from_slice(value);
    out.extend_from_slice(b"\r\n");
}

/// The `X-Forwarded-For` value this hop should send: every inbound trail,
/// joined by `", "`, with `client` appended.
fn forwarded_for(out: &mut Vec<u8>, head: &Head, buf: &[u8], client: IpAddr) {
    out.extend_from_slice(b"X-Forwarded-For: ");
    for (_, value) in head.headers_named(buf, b"x-forwarded-for") {
        let value = trim_ows(value);
        if value.is_empty() {
            continue;
        }
        out.extend_from_slice(value);
        out.extend_from_slice(b", ");
    }
    // `IpAddr`'s Display, matching the hyper engine: no brackets around IPv6,
    // because this is a bare address and not an authority.
    out.extend_from_slice(client.to_string().as_bytes());
    out.extend_from_slice(b"\r\n");
}

/// The request id for this exchange: the inbound one if it is present and
/// non-empty, otherwise 32 fresh hex characters.
///
/// Preserving is what makes a trace survive a hop. Regenerating an *empty* one
/// is deliberate — an empty id correlates nothing and is worse than no id,
/// because it looks like one.
pub fn request_id(head: &Head, buf: &[u8], into: &mut [u8; 32]) -> usize {
    if let Some(existing) = head.header(buf, b"x-request-id") {
        let existing = trim_ows(existing);
        if !existing.is_empty() {
            let n = existing.len().min(32);
            into[..n].copy_from_slice(&existing[..n]);
            // A longer inbound id is passed through whole by the caller; this
            // buffer only has to hold what we generate.
            return existing.len();
        }
    }
    rng::hex_id(into);
    32
}

/// Write the request head to send upstream.
///
/// `endpoint` supplies a `Host` only when the client sent none — the client's
/// own `Host` is otherwise preserved end to end, because it is what the origin
/// uses to pick a virtual host and rewriting it would silently change which
/// site answers.
#[allow(clippy::too_many_arguments)]
pub fn write_upstream_request(
    out: &mut Vec<u8>,
    head: &Head,
    buf: &[u8],
    hop: Hop,
    endpoint: SocketAddr,
    framing: Framing,
    close: bool,
    upgrade: Option<&[u8]>,
) {
    let StartLine::Request { method, target, .. } = head.start else {
        debug_assert!(false, "write_upstream_request needs a request head");
        return;
    };

    out.extend_from_slice(method.bytes(buf));
    out.push(b' ');
    out.extend_from_slice(target.bytes(buf));
    // Always 1.1 upstream, whatever the client spoke. The hyper engine does the
    // same (`parts.version = Version::HTTP_11`), and it is what lets an
    // HTTP/1.0 client reach a keep-alive upstream pool.
    out.extend_from_slice(b" HTTP/1.1\r\n");

    let mut host_seen = false;
    for (name, value) in head.iter(buf) {
        if name.eq_ignore_ascii_case(b"host") {
            host_seen = true;
        }
        if is_forwardable(head, buf, name) {
            field(out, name, value);
        }
    }
    if !host_seen {
        // hyper's client synthesises `Host` from the URI authority when the
        // request carries none, and the URI it is given is `http://ip:port/`.
        // Matching that keeps an origin from seeing two different requests
        // depending on which engine forwarded them.
        field(out, b"Host", endpoint.to_string().as_bytes());
    }

    forwarded_for(out, head, buf, hop.client);
    field(out, b"X-Real-IP", hop.client.to_string().as_bytes());
    field(out, b"X-Forwarded-Proto", hop.scheme.as_bytes());
    if let Some(host) = head.header(buf, b"host") {
        // Verbatim: port kept, case kept. The router normalises its own copy;
        // this header reports what the client actually said.
        field(out, b"X-Forwarded-Host", host);
    }

    let mut id = [0u8; 32];
    let len = request_id(head, buf, &mut id);
    if len > 32 {
        // An inbound id longer than the buffer: forward the original rather
        // than a truncation, which would break correlation while looking fine.
        let existing = head.header(buf, b"x-request-id").unwrap_or(b"");
        field(out, b"X-Request-Id", trim_ows(existing));
    } else {
        field(out, b"X-Request-Id", &id[..len]);
    }

    // `Transfer-Encoding` was stripped as hop-by-hop, so a chunked body needs
    // its framing restated for this hop. The body bytes themselves are
    // forwarded verbatim, chunk boundaries and all.
    if framing == Framing::Chunked {
        field(out, b"Transfer-Encoding", b"chunked");
    }
    // `Connection` and `Upgrade` were both stripped as hop-by-hop, which is
    // correct — they describe *this* hop. An upgrade then has to restate them
    // for the hop being opened, or the origin sees a request that no longer
    // asks to switch protocols and answers it as an ordinary GET. The hyper
    // engine does the same thing through `restore_upgrade`.
    if let Some(protocol) = upgrade {
        field(out, b"Connection", b"upgrade");
        field(out, b"Upgrade", protocol);
    } else if close {
        field(out, b"Connection", b"close");
    }
    out.extend_from_slice(b"\r\n");
}

/// Write the response head to send downstream.
///
/// The status line is rebuilt rather than copied so the version reflects what
/// *this* hop speaks, and an upstream that sent no reason phrase gets the
/// standard one rather than a bare `HTTP/1.1 200`.
pub fn write_downstream_response(
    out: &mut Vec<u8>,
    head: &Head,
    buf: &[u8],
    framing: Framing,
    close: bool,
    upgrade: Option<&[u8]>,
) {
    let StartLine::Status { code, reason, .. } = head.start else {
        debug_assert!(false, "write_downstream_response needs a status head");
        return;
    };

    out.extend_from_slice(b"HTTP/1.1 ");
    out.extend_from_slice(code.to_string().as_bytes());
    out.push(b' ');
    if reason.is_empty() {
        out.extend_from_slice(ramjet_http::encode::reason(code).as_bytes());
    } else {
        out.extend_from_slice(reason.bytes(buf));
    }
    out.extend_from_slice(b"\r\n");

    for (name, value) in head.iter(buf) {
        // The response direction strips the same set, minus the ones only a
        // request carries. `is_self_written` is a no-op here: this hop adds no
        // `X-Forwarded-*` to a response, and neither does the hyper engine.
        if !name.eq_ignore_ascii_case(b"connection")
            && !is_hop_by_hop(name)
            && !connection_lists(head, buf, name)
        {
            field(out, name, value);
        }
    }

    if framing == Framing::Chunked {
        field(out, b"Transfer-Encoding", b"chunked");
    }
    // A 101 that lost its `Connection: upgrade` is a 101 the client will not
    // act on: RFC 9110 requires both, and a browser that sees only the status
    // treats the connection as still speaking HTTP.
    if let Some(protocol) = upgrade {
        field(out, b"Connection", b"upgrade");
        field(out, b"Upgrade", protocol);
    } else if close {
        field(out, b"Connection", b"close");
    }
    out.extend_from_slice(b"\r\n");
}

/// The value of a cookie in a `Cookie` header, for canary routing.
///
/// Case-sensitive on the name, matching the hyper engine and RFC 6265: cookie
/// names are not case-folded, and folding them here would divert traffic a
/// `Set-Cookie` never asked to divert.
pub fn cookie_value<'a>(head: &'a Head, buf: &'a [u8], name: &str) -> Option<&'a str> {
    for (_, value) in head.headers_named(buf, b"cookie") {
        let value = std::str::from_utf8(value).ok()?;
        for pair in value.split(';') {
            let Some((key, val)) = pair.split_once('=') else {
                continue;
            };
            if key.trim() == name {
                return Some(val.trim());
            }
        }
    }
    None
}

/// Whether this request is gRPC, which needs an HTTP/2 upstream.
///
/// A prefix test, so `application/grpc+proto` matches too. The hyper engine
/// makes the same test at the same point — after routing, so an unrouted gRPC
/// request is a 404 rather than a 502.
pub fn is_grpc(head: &Head, buf: &[u8]) -> bool {
    head.header(buf, b"content-type")
        .is_some_and(|v| v.starts_with(b"application/grpc"))
}

/// The `Host` the router should match on, as a string.
///
/// `None` when there is no `Host` or it is not UTF-8; the router treats that as
/// an unmatchable host and falls through to the catch-all or default backend,
/// which is what the hyper engine does with an empty authority.
pub fn routing_host<'a>(head: &'a Head, buf: &'a [u8]) -> Option<&'a str> {
    head.header(buf, b"host")
        .and_then(|v| std::str::from_utf8(v).ok())
}

/// The path the router should match on: the target with any query removed.
pub fn routing_path(target: Span, buf: &[u8]) -> Option<&str> {
    let bytes = target.bytes(buf);
    let end = bytes.iter().position(|&b| b == b'?').unwrap_or(bytes.len());
    std::str::from_utf8(&bytes[..end]).ok()
}

/// Whether the connection should stay open after this message.
pub fn message_keep_alive(head: &Head, buf: &[u8], version: Version) -> bool {
    crate::codec::keep_alive(head, buf, version)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::{parse_request_head, parse_response_head, request_framing, Head};

    const CLIENT: IpAddr = IpAddr::V4(std::net::Ipv4Addr::new(198, 51, 100, 4));

    fn hop() -> Hop {
        Hop {
            client: CLIENT,
            scheme: "http",
        }
    }

    fn endpoint() -> SocketAddr {
        SocketAddr::from(([10, 0, 0, 1], 8080))
    }

    /// Rewrite a request and return the head that goes upstream, as text.
    fn forward(wire: &[u8]) -> String {
        let mut head = Head::default();
        assert!(parse_request_head(wire, &mut head).expect("valid request"));
        let framing = request_framing(&head, wire).expect("valid framing");
        let mut out = Vec::new();
        write_upstream_request(&mut out, &head, wire, hop(), endpoint(), framing, false, None);
        String::from_utf8(out).expect("ascii head")
    }

    /// Rewrite a response and return the head that goes downstream, as text.
    fn relay(wire: &[u8], framing: Framing, close: bool) -> String {
        let mut head = Head::default();
        assert!(parse_response_head(wire, &mut head).expect("valid response"));
        let mut out = Vec::new();
        write_downstream_response(&mut out, &head, wire, framing, close, None);
        String::from_utf8(out).expect("ascii head")
    }

    /// The value of one field in a written head, or `None` if it is absent.
    fn header_line(head: &str, name: &str) -> Option<String> {
        head.split("\r\n")
            .skip(1) // the start line has a colon in it only by accident
            .filter_map(|line| line.split_once(':'))
            .find(|(n, _)| n.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.trim().to_owned())
    }

    #[test]
    fn this_hop_describes_itself() {
        let head = forward(b"GET / HTTP/1.1\r\nHost: alpha.example.com\r\n\r\n");
        assert_eq!(header_line(&head, "x-forwarded-for").as_deref(), Some("198.51.100.4"));
        assert_eq!(header_line(&head, "x-real-ip").as_deref(), Some("198.51.100.4"));
        assert_eq!(header_line(&head, "x-forwarded-proto").as_deref(), Some("http"));
        assert_eq!(
            header_line(&head, "x-forwarded-host").as_deref(),
            Some("alpha.example.com")
        );
    }

    #[test]
    fn the_forwarded_trail_is_extended_not_replaced() {
        let head = forward(b"GET / HTTP/1.1\r\nHost: a\r\nX-Forwarded-For: 203.0.113.7\r\n\r\n");
        assert_eq!(
            header_line(&head, "x-forwarded-for").as_deref(),
            Some("203.0.113.7, 198.51.100.4")
        );
    }

    #[test]
    fn several_forwarded_lines_collapse_into_one() {
        let head = forward(
            b"GET / HTTP/1.1\r\nHost: a\r\nX-Forwarded-For: 203.0.113.7\r\nX-Forwarded-For: 10.1.1.1\r\n\r\n",
        );
        assert_eq!(
            header_line(&head, "x-forwarded-for").as_deref(),
            Some("203.0.113.7, 10.1.1.1, 198.51.100.4")
        );
        assert_eq!(
            head.matches("X-Forwarded-For").count(),
            1,
            "one line, not three"
        );
    }

    #[test]
    fn a_request_id_is_generated_when_absent() {
        let head = forward(b"GET / HTTP/1.1\r\nHost: a\r\n\r\n");
        let id = header_line(&head, "x-request-id").expect("an id");
        assert_eq!(id.len(), 32, "{id}");
        assert!(
            id.bytes().all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase()),
            "{id} should be lowercase hex"
        );
    }

    #[test]
    fn an_inbound_request_id_survives() {
        let head = forward(b"GET / HTTP/1.1\r\nHost: a\r\nX-Request-Id: trace-from-the-edge\r\n\r\n");
        assert_eq!(
            header_line(&head, "x-request-id").as_deref(),
            Some("trace-from-the-edge")
        );
    }

    #[test]
    fn an_empty_request_id_is_replaced() {
        let head = forward(b"GET / HTTP/1.1\r\nHost: a\r\nX-Request-Id: \r\n\r\n");
        assert_eq!(header_line(&head, "x-request-id").map(|v| v.len()), Some(32));
    }

    #[test]
    fn hop_by_hop_headers_do_not_reach_the_upstream() {
        let head = forward(
            b"GET / HTTP/1.1\r\nHost: a\r\nConnection: keep-alive, x-hop-secret\r\n\
              X-Hop-Secret: shh\r\nKeep-Alive: timeout=5\r\nProxy-Connection: keep-alive\r\n\
              X-End-To-End: kept\r\n\r\n",
        );
        for gone in [
            "connection",
            "x-hop-secret",
            "keep-alive",
            "proxy-connection",
        ] {
            assert!(header_line(&head, gone).is_none(), "{gone} should be gone:\n{head}");
        }
        assert_eq!(header_line(&head, "x-end-to-end").as_deref(), Some("kept"));
    }

    #[test]
    fn a_header_named_by_connection_on_a_second_line_is_also_stripped() {
        let head = forward(
            b"GET / HTTP/1.1\r\nHost: a\r\nConnection: keep-alive\r\nConnection: x-second\r\n\
              X-Second: leak\r\n\r\n",
        );
        assert!(header_line(&head, "x-second").is_none(), "{head}");
    }

    #[test]
    fn the_client_host_is_preserved_end_to_end() {
        let head = forward(b"GET / HTTP/1.1\r\nHost: alpha.example.com\r\n\r\n");
        assert_eq!(
            header_line(&head, "host").as_deref(),
            Some("alpha.example.com")
        );
    }

    #[test]
    fn a_request_without_a_host_gets_the_endpoint_as_one() {
        // hyper's client does this from the URI authority; matching it keeps
        // the two engines indistinguishable to an origin.
        let head = forward(b"GET / HTTP/1.0\r\nX: y\r\n\r\n");
        assert_eq!(header_line(&head, "host").as_deref(), Some("10.0.0.1:8080"));
    }

    #[test]
    fn the_request_line_is_rewritten_to_http_11() {
        let head = forward(b"GET /a/b%20c?x=1&y=%2F HTTP/1.0\r\nHost: a\r\n\r\n");
        assert!(
            head.starts_with("GET /a/b%20c?x=1&y=%2F HTTP/1.1\r\n"),
            "{head}"
        );
    }

    #[test]
    fn a_chunked_request_restates_its_framing() {
        let head = forward(
            b"POST / HTTP/1.1\r\nHost: a\r\nTransfer-Encoding: chunked\r\n\r\n",
        );
        // Stripped as hop-by-hop, then restated by the framing rule — so it
        // appears exactly once.
        assert_eq!(head.matches("Transfer-Encoding").count(), 1, "{head}");
        assert_eq!(
            header_line(&head, "transfer-encoding").as_deref(),
            Some("chunked")
        );
    }

    #[test]
    fn a_content_length_passes_through_untouched() {
        let head = forward(b"POST / HTTP/1.1\r\nHost: a\r\nContent-Length: 5\r\n\r\n");
        assert_eq!(header_line(&head, "content-length").as_deref(), Some("5"));
    }

    #[test]
    fn hop_by_hop_response_headers_do_not_reach_the_client() {
        let wire = b"HTTP/1.1 200 OK\r\nConnection: keep-alive, x-hop\r\nX-Hop: shh\r\n\
                     Keep-Alive: timeout=5\r\nContent-Length: 2\r\nX-Kept: yes\r\n\r\n";
        let head = relay(wire, Framing::Length(2), false);
        for gone in ["connection", "x-hop", "keep-alive"] {
            assert!(header_line(&head, gone).is_none(), "{gone}:\n{head}");
        }
        assert_eq!(header_line(&head, "x-kept").as_deref(), Some("yes"));
        assert_eq!(header_line(&head, "content-length").as_deref(), Some("2"));
    }

    #[test]
    fn a_missing_reason_phrase_is_filled_in() {
        let head = relay(b"HTTP/1.1 404\r\nContent-Length: 0\r\n\r\n", Framing::Empty, false);
        assert!(head.starts_with("HTTP/1.1 404 Not Found\r\n"), "{head}");
    }

    #[test]
    fn a_chunked_response_keeps_chunked_framing_and_no_length() {
        let wire = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n";
        let head = relay(wire, Framing::Chunked, false);
        assert_eq!(
            header_line(&head, "transfer-encoding").as_deref(),
            Some("chunked")
        );
        assert!(header_line(&head, "content-length").is_none(), "{head}");
    }

    #[test]
    fn closing_says_so_and_keeping_open_says_nothing() {
        let wire = b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n";
        assert_eq!(
            header_line(&relay(wire, Framing::Empty, true), "connection").as_deref(),
            Some("close")
        );
        // HTTP/1.1 persists by default; saying so again is noise on the wire.
        assert!(header_line(&relay(wire, Framing::Empty, false), "connection").is_none());
    }

    #[test]
    fn an_upgrade_needs_both_headers() {
        let with_both = b"GET / HTTP/1.1\r\nHost: a\r\nConnection: Upgrade\r\nUpgrade: websocket\r\n\r\n";
        let bare = b"GET / HTTP/1.1\r\nHost: a\r\nUpgrade: websocket\r\n\r\n";
        for (wire, expected) in [(&with_both[..], true), (bare, false)] {
            let mut head = Head::default();
            assert!(parse_request_head(wire, &mut head).unwrap());
            assert_eq!(wants_upgrade(&head, wire), expected);
        }
    }

    #[test]
    fn grpc_is_detected_by_content_type_prefix() {
        for (ct, expected) in [
            ("application/grpc", true),
            ("application/grpc+proto", true),
            ("application/json", false),
        ] {
            let wire = format!("POST / HTTP/1.1\r\nHost: a\r\nContent-Type: {ct}\r\n\r\n");
            let mut head = Head::default();
            assert!(parse_request_head(wire.as_bytes(), &mut head).unwrap());
            assert_eq!(is_grpc(&head, wire.as_bytes()), expected, "{ct}");
        }
    }

    #[test]
    fn a_cookie_is_found_among_others() {
        let wire = b"GET / HTTP/1.1\r\nHost: a\r\nCookie: session=abc; canary=always; theme=dark\r\n\r\n";
        let mut head = Head::default();
        assert!(parse_request_head(wire, &mut head).unwrap());
        assert_eq!(cookie_value(&head, wire, "canary"), Some("always"));
        assert_eq!(cookie_value(&head, wire, "session"), Some("abc"));
        assert_eq!(cookie_value(&head, wire, "missing"), None);
        // Names are not case-folded.
        assert_eq!(cookie_value(&head, wire, "Canary"), None);
    }

    #[test]
    fn the_routing_path_drops_the_query() {
        let wire = b"GET /api/v1?x=1&y=2 HTTP/1.1\r\nHost: a\r\n\r\n";
        let mut head = Head::default();
        assert!(parse_request_head(wire, &mut head).unwrap());
        let StartLine::Request { target, .. } = head.start else {
            panic!("a request");
        };
        assert_eq!(routing_path(target, wire), Some("/api/v1"));
        assert_eq!(routing_host(&head, wire), Some("a"));
    }

    #[test]
    fn the_head_written_is_parseable_again() {
        // The strongest single check: whatever we emit, our own parser reads
        // back, so a malformed rewrite cannot escape the process.
        let head = forward(
            b"POST /x?y=1 HTTP/1.1\r\nHost: a\r\nContent-Length: 3\r\nX-Odd: v\r\n\r\n",
        );
        let mut reparsed = Head::default();
        assert!(
            parse_request_head(head.as_bytes(), &mut reparsed).expect("valid rewrite"),
            "{head}"
        );
        assert_eq!(reparsed.len, head.len());
    }
}
