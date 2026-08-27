//! What this engine answers, and everything it refuses.
//!
//! Two kinds of constant live here. The first are the error bodies the hyper
//! engine already sends, copied verbatim so a client cannot tell which engine
//! answered it. The second are this engine's own refusals — the v1 gaps — and
//! they exist as named constants rather than inline strings because a gap that
//! is only visible in a match arm is a gap nobody documents.
//!
//! Every body starts with its status code and ends with a newline. The hyper
//! crate has a unit test pinning that convention; so does this module.

use ramjet_http::encode;

/// No rule matched the host and path, and there is no default backend.
pub const NO_ROUTE: &[u8] = b"404 Not Found: no ingress rule matches this host and path\n";

/// A backend matched but has no endpoints to send the request to.
pub const NO_ENDPOINT: &[u8] = b"503 Service Unavailable: the backend has no ready endpoints\n";

/// Every endpoint tried refused the connection.
pub const CONNECT_FAILED: &[u8] = b"502 Bad Gateway: could not connect to any upstream endpoint\n";

/// A connection was established and then the exchange failed.
pub const UPSTREAM_FAILED: &[u8] = b"502 Bad Gateway: the upstream connection failed\n";

/// No response headers arrived before the deadline.
pub const TIMEOUT: &[u8] = b"504 Gateway Timeout: the upstream sent no response headers in time\n";

/// gRPC needs an HTTP/2 upstream, which ramjet does not speak in either engine.
pub const GRPC: &[u8] =
    b"502 Bad Gateway: gRPC upstreams require HTTP/2, which ramjet does not yet speak upstream\n";

/// A protocol upgrade, which v1 of this engine does not carry.
///
/// The hyper engine tunnels these. Lifting it here means keeping a connection
/// pair spliced after the HTTP exchange ends, which is a second state machine
/// rather than a missing branch — see the crate docs.
pub const NO_UPGRADE: &[u8] =
    b"502 Bad Gateway: the uring engine does not carry protocol upgrades; use --engine hyper\n";

/// An HTTP/2 request, including h2c with prior knowledge.
pub const NO_HTTP2: &[u8] =
    b"502 Bad Gateway: the uring engine speaks HTTP/1.1 only; use --engine hyper\n";

/// The client's request could not be read.
///
/// The status varies with the fault — 400, 413, 431 or 501 — so the body is
/// built rather than constant, but it follows the same shape.
pub fn bad_request_body(status: u16, detail: &str) -> Vec<u8> {
    let reason = encode::reason(status);
    let mut body = format!("{status} {reason}: ");
    body.push_str(detail);
    body.push('\n');
    body.into_bytes()
}

/// Write a complete, self-framed response with a plain-text body.
///
/// `ramjet_http::encode` owns the framing — a caller cannot supply its own
/// `Content-Length` — which is exactly the property wanted for the responses
/// the proxy invents, as opposed to the ones it relays.
pub fn write_static(out: &mut Vec<u8>, status: u16, body: &[u8], close: bool) {
    let headers: &[(&str, &str)] = if close {
        &[
            ("Content-Type", "text/plain; charset=utf-8"),
            ("Connection", "close"),
        ]
    } else {
        &[("Content-Type", "text/plain; charset=utf-8")]
    };
    // The only way this fails is an out-of-range status or an invalid header,
    // and every call site passes literals that are neither. Writing nothing at
    // all would leave the client hanging, so a failure degrades to a bare 500.
    if encode::response(out, status, headers, body).is_err() {
        out.extend_from_slice(b"HTTP/1.1 500 Internal Server Error\r\nContent-Length: 0\r\n\r\n");
    }
}

/// Write a response to a `HEAD` request: the head a `GET` would have had,
/// without the body bytes.
pub fn write_static_head_only(out: &mut Vec<u8>, status: u16, body_len: usize, close: bool) {
    let headers: &[(&str, &str)] = if close {
        &[
            ("Content-Type", "text/plain; charset=utf-8"),
            ("Connection", "close"),
        ]
    } else {
        &[("Content-Type", "text/plain; charset=utf-8")]
    };
    if encode::response_head_only(out, status, headers, body_len).is_err() {
        out.extend_from_slice(b"HTTP/1.1 500 Internal Server Error\r\nContent-Length: 0\r\n\r\n");
    }
}

/// The v1 gaps, in one place, for `--help` and the startup log.
///
/// Printed at startup rather than buried in a doc comment, because an operator
/// choosing `--engine uring` should see what they gave up before their first
/// request fails rather than after.
pub const V1_LIMITS: &str = "\
the uring engine is experimental and serves a subset of the hyper engine:
  - HTTP/1.1 plaintext only; no TLS termination and no HTTP/2 (502)
  - no WebSocket or other protocol upgrades (502)
  - static routes only; --engine uring cannot run in Kubernetes mode
  - gRPC is refused, as it is on the hyper engine (502)";

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_error_body_starts_with_its_status_and_ends_with_a_newline() {
        // The same convention the hyper engine pins, so an operator grepping
        // logs for "502" finds both engines' refusals.
        for body in [
            NO_ROUTE,
            NO_ENDPOINT,
            CONNECT_FAILED,
            UPSTREAM_FAILED,
            TIMEOUT,
            GRPC,
            NO_UPGRADE,
            NO_HTTP2,
        ] {
            let text = std::str::from_utf8(body).expect("ascii body");
            let code: u16 = text[..3].parse().expect("leading status code");
            assert!((100..=599).contains(&code), "{text}");
            assert!(text.ends_with('\n'), "{text}");
        }
    }

    #[test]
    fn the_hyper_engines_bodies_are_reproduced_exactly() {
        // Not a paraphrase: a client that switches engines must not be able to
        // tell by reading the body. These literals are the ones in
        // `ramjet_proxy::forward`.
        assert_eq!(
            NO_ROUTE,
            b"404 Not Found: no ingress rule matches this host and path\n"
        );
        assert_eq!(
            NO_ENDPOINT,
            b"503 Service Unavailable: the backend has no ready endpoints\n"
        );
        assert_eq!(
            CONNECT_FAILED,
            b"502 Bad Gateway: could not connect to any upstream endpoint\n"
        );
        assert_eq!(
            UPSTREAM_FAILED,
            b"502 Bad Gateway: the upstream connection failed\n"
        );
        assert_eq!(
            TIMEOUT,
            b"504 Gateway Timeout: the upstream sent no response headers in time\n"
        );
    }

    #[test]
    fn a_static_response_is_complete_and_framed() {
        let mut out = Vec::new();
        write_static(&mut out, 404, NO_ROUTE, false);
        let text = String::from_utf8(out).expect("ascii");
        assert!(text.starts_with("HTTP/1.1 404 Not Found\r\n"), "{text}");
        assert!(text.contains("Content-Type: text/plain; charset=utf-8\r\n"), "{text}");
        assert!(
            text.contains(&format!("Content-Length: {}\r\n", NO_ROUTE.len())),
            "{text}"
        );
        assert!(text.ends_with(std::str::from_utf8(NO_ROUTE).unwrap()), "{text}");
        assert!(!text.contains("Connection:"), "{text}");
    }

    #[test]
    fn closing_is_announced() {
        let mut out = Vec::new();
        write_static(&mut out, 502, CONNECT_FAILED, true);
        let text = String::from_utf8(out).expect("ascii");
        assert!(text.contains("Connection: close\r\n"), "{text}");
    }

    #[test]
    fn a_head_response_carries_the_length_but_no_body() {
        let mut out = Vec::new();
        write_static_head_only(&mut out, 404, NO_ROUTE.len(), false);
        let text = String::from_utf8(out).expect("ascii");
        assert!(
            text.contains(&format!("Content-Length: {}\r\n", NO_ROUTE.len())),
            "{text}"
        );
        assert!(text.ends_with("\r\n\r\n"), "{text}");
    }

    #[test]
    fn a_bad_request_body_follows_the_same_shape() {
        let body = bad_request_body(400, "header line has no colon");
        let text = String::from_utf8(body).expect("ascii");
        assert_eq!(text, "400 Bad Request: header line has no colon\n");
    }

    #[test]
    fn the_limit_list_names_every_gap() {
        for gap in ["TLS", "HTTP/2", "upgrades", "Kubernetes"] {
            assert!(
                V1_LIMITS.contains(gap),
                "{gap} is missing from the printed limits"
            );
        }
    }
}
