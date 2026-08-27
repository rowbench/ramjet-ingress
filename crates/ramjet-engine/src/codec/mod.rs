//! Sans-io HTTP/1.1 codec for **both** halves of a proxy hop.
//!
//! `ramjet-http` is the server half — bytes in, a complete [`Request`] out —
//! and this crate uses it for the responses the proxy generates itself. It is
//! not enough for a proxy, for two reasons that are properties of its contract
//! rather than gaps to be patched:
//!
//! - It has no client half. A proxy has to *write* a request and *read* a
//!   response, and nothing in that direction exists.
//! - It buffers a whole body before yielding a request, and refuses
//!   `Transfer-Encoding` outright so it can never misread a stream it does not
//!   frame. Correct for a server that answers from memory; fatal for a proxy,
//!   which must start forwarding a 4 MiB upload before its last byte arrives.
//!
//! So this module parses **heads only** and hands framing back to the caller as
//! a [`Framing`] it can drive incrementally. Everything here borrows from the
//! caller's buffer and allocates nothing per request.
//!
//! # Bytes, not `str`
//!
//! Header values are `&[u8]`. RFC 9110 permits `obs-text` in field values, and
//! a proxy that rejects a request because a header was Latin-1 is a proxy that
//! breaks traffic it was only asked to forward. Conversion to `str` happens at
//! exactly the two places that need it — the host and path handed to the
//! router — and is checked there.
//!
//! # Spans, not slices
//!
//! A parsed head is a set of `(offset, length)` pairs into the buffer it came
//! from, not a set of `&str`. That is what lets a connection keep one
//! `Vec<HeaderSpan>` and reuse it for every request it ever serves: a vector of
//! borrowed slices would be pinned to one buffer's lifetime and have to be
//! reallocated each time.

pub mod chunked;
pub mod head;

pub use chunked::ChunkScan;
pub use head::{parse_request_head, parse_response_head, Head, HeaderSpan, Span, StartLine};

/// Longest head this codec will accept, in either direction.
///
/// Matches `ramjet_http::MAX_HEAD`. A head is bounded because the alternative
/// is letting a peer decide how much memory one connection costs, and 16 KiB is
/// well past what a browser plus a chain of proxies produces.
pub const MAX_HEAD: usize = 16 * 1024;

/// Most header fields one message may carry.
///
/// Sixty-four matches `ramjet_http::MAX_HEADERS`. A proxy sees a little more
/// than an origin server does — it adds five of its own — so this is a ceiling
/// on what arrives, not on what is sent.
pub const MAX_HEADERS: usize = 64;

/// Which HTTP version a start line declared.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Version {
    /// HTTP/1.0: connections close by default.
    Http10,
    /// HTTP/1.1: connections persist by default.
    Http11,
}

impl Version {
    /// The wire form, for writing a start line.
    pub fn as_bytes(self) -> &'static [u8] {
        match self {
            Version::Http10 => b"HTTP/1.0",
            Version::Http11 => b"HTTP/1.1",
        }
    }
}

/// Why a message could not be read.
///
/// Every variant is terminal for the connection it was read from. HTTP/1.1 has
/// no way to resynchronise a stream whose framing was not understood, and
/// guessing is how request smuggling works, so the only honest recovery is to
/// answer once and hang up.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CodecError {
    /// The message broke framing: a bad start line, a header with no colon, a
    /// `Content-Length` that is not a number, a chunk size that is not hex.
    Malformed(&'static str),
    /// The head went past [`MAX_HEAD`] or a body past a caller-supplied limit.
    TooLarge,
    /// More than [`MAX_HEADERS`] header fields.
    TooManyHeaders,
    /// Well-formed but asking for something this codec does not do — an HTTP
    /// version past 1.1, a transfer coding other than `chunked`.
    Unsupported(&'static str),
}

impl CodecError {
    /// The status a server should answer a *request* carrying this error with.
    ///
    /// Meaningless for an error read from an upstream *response*: there the
    /// answer is always 502, because the fault is not the client's.
    pub fn status(self) -> u16 {
        match self {
            CodecError::Malformed(_) => 400,
            CodecError::TooLarge => 413,
            CodecError::TooManyHeaders => 431,
            CodecError::Unsupported(_) => 501,
        }
    }

    /// A one-line explanation, for the body of the error response.
    pub fn detail(self) -> &'static str {
        match self {
            CodecError::Malformed(why) => why,
            CodecError::TooLarge => "the head exceeded the size limit",
            CodecError::TooManyHeaders => "too many header fields",
            CodecError::Unsupported(what) => what,
        }
    }
}

impl std::fmt::Display for CodecError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CodecError::Malformed(why) => write!(f, "malformed message: {why}"),
            CodecError::TooLarge => f.write_str("message head too large"),
            CodecError::TooManyHeaders => f.write_str("too many header fields"),
            CodecError::Unsupported(what) => write!(f, "unsupported: {what}"),
        }
    }
}

impl std::error::Error for CodecError {}

/// How the body after a head is delimited.
///
/// This is the whole of RFC 9112 §6.3 as it applies to a proxy, and getting it
/// wrong in either direction desynchronises a connection, so the rules are
/// spelled out at [`response_framing`] and [`request_framing`] rather than
/// inferred at each call site.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Framing {
    /// No body at all, and none is possible.
    Empty,
    /// Exactly this many bytes follow the head.
    Length(u64),
    /// Chunked transfer coding: the body ends at its own terminator.
    Chunked,
    /// The body runs to end of stream. Response-only, and it makes the
    /// connection single-use in both directions.
    UntilClose,
}

impl Framing {
    /// Whether a connection carrying this body can be reused afterwards.
    pub fn allows_reuse(self) -> bool {
        !matches!(self, Framing::UntilClose)
    }
}

/// Read the framing of a request body from its head.
///
/// `Transfer-Encoding` wins over `Content-Length` when both are present, but
/// only after rejecting the combination: RFC 9112 §6.3 says a message with both
/// "ought to be handled as an error", and the reason is that the two framings
/// disagreeing is precisely the request-smuggling primitive. A proxy is the
/// worst possible place to guess, because whatever it guesses it then re-emits
/// to a second parser that may guess differently.
pub fn request_framing(head: &Head, buf: &[u8]) -> Result<Framing, CodecError> {
    let chunked = transfer_encoding_is_chunked(head, buf)?;
    let length = content_length(head, buf)?;
    match (chunked, length) {
        (true, Some(_)) => Err(CodecError::Malformed(
            "both Transfer-Encoding and Content-Length",
        )),
        (true, None) => Ok(Framing::Chunked),
        (false, Some(0)) | (false, None) => Ok(Framing::Empty),
        (false, Some(n)) => Ok(Framing::Length(n)),
    }
}

/// Read the framing of a response body from its head.
///
/// The status and the request method both matter, and neither is in the
/// response: a 204 has no body however it is framed, and a response to `HEAD`
/// carries the `Content-Length` a `GET` would have had while sending none of
/// those bytes. Reading that length as a promise is how a proxy hangs waiting
/// for a body the origin was never going to send.
pub fn response_framing(
    head: &Head,
    buf: &[u8],
    method_was_head: bool,
) -> Result<Framing, CodecError> {
    let status = match head.start {
        StartLine::Status { code, .. } => code,
        StartLine::Request { .. } => {
            return Err(CodecError::Malformed("expected a response, got a request"))
        }
    };
    if method_was_head || status < 200 || status == 204 || status == 304 {
        return Ok(Framing::Empty);
    }
    let chunked = transfer_encoding_is_chunked(head, buf)?;
    let length = content_length(head, buf)?;
    match (chunked, length) {
        (true, Some(_)) => Err(CodecError::Malformed(
            "both Transfer-Encoding and Content-Length",
        )),
        (true, None) => Ok(Framing::Chunked),
        (false, Some(0)) => Ok(Framing::Empty),
        (false, Some(n)) => Ok(Framing::Length(n)),
        // No framing header at all. HTTP/1.0's answer, and still legal in 1.1:
        // the body is whatever arrives before the close.
        (false, None) => Ok(Framing::UntilClose),
    }
}

/// Whether the final transfer coding is `chunked`, rejecting anything else.
///
/// `Transfer-Encoding: gzip, chunked` is legal and would need decoding to
/// forward, which this codec does not do; `Transfer-Encoding: chunked, gzip` is
/// not legal at all. Both are refused rather than approximated.
fn transfer_encoding_is_chunked(head: &Head, buf: &[u8]) -> Result<bool, CodecError> {
    let mut seen = false;
    for (_, value) in head.headers_named(buf, b"transfer-encoding") {
        seen = true;
        let mut tokens = value
            .split(|&b| b == b',')
            .map(trim_ows)
            .filter(|t| !t.is_empty());
        let Some(only) = tokens.next() else {
            return Err(CodecError::Malformed("empty Transfer-Encoding"));
        };
        if tokens.next().is_some() || !only.eq_ignore_ascii_case(b"chunked") {
            return Err(CodecError::Unsupported(
                "a transfer coding other than chunked",
            ));
        }
    }
    Ok(seen)
}

/// The declared body length, rejecting the two ways it can disagree with
/// itself.
///
/// Strict digits only: `usize::from_str` would accept a leading `+`, and a
/// `Content-Length` that two parsers read differently is a smuggling primitive
/// rather than a formatting quirk.
fn content_length(head: &Head, buf: &[u8]) -> Result<Option<u64>, CodecError> {
    let mut found: Option<u64> = None;
    for (_, value) in head.headers_named(buf, b"content-length") {
        // One field may still carry a comma-separated list of identical values,
        // which RFC 9110 §8.6 permits and requires to agree.
        for part in value.split(|&b| b == b',').map(trim_ows) {
            let n = parse_digits(part)?;
            match found {
                Some(prev) if prev != n => {
                    return Err(CodecError::Malformed("conflicting Content-Length values"))
                }
                _ => found = Some(n),
            }
        }
    }
    Ok(found)
}

fn parse_digits(value: &[u8]) -> Result<u64, CodecError> {
    if value.is_empty() || !value.iter().all(u8::is_ascii_digit) {
        return Err(CodecError::Malformed("Content-Length is not a number"));
    }
    let mut n: u64 = 0;
    for &b in value {
        n = n
            .checked_mul(10)
            .and_then(|n| n.checked_add(u64::from(b - b'0')))
            .ok_or(CodecError::Malformed("Content-Length overflows"))?;
    }
    Ok(n)
}

/// Strip optional leading and trailing whitespace, as OWS is defined.
pub fn trim_ows(value: &[u8]) -> &[u8] {
    let mut start = 0;
    let mut end = value.len();
    while start < end && (value[start] == b' ' || value[start] == b'\t') {
        start += 1;
    }
    while end > start && (value[end - 1] == b' ' || value[end - 1] == b'\t') {
        end -= 1;
    }
    &value[start..end]
}

/// Whether a comma-separated field value contains `token`, case-insensitively.
///
/// `Connection: keep-alive, Upgrade` has to match "upgrade", which is why this
/// is a token scan and not an equality test.
pub fn has_token(value: &[u8], token: &[u8]) -> bool {
    value
        .split(|&b| b == b',')
        .any(|part| trim_ows(part).eq_ignore_ascii_case(token))
}

/// Whether the connection should stay open after this message.
///
/// HTTP/1.1 persists unless something says `close`; HTTP/1.0 closes unless
/// something says `keep-alive`.
pub fn keep_alive(head: &Head, buf: &[u8], version: Version) -> bool {
    let mut saw_keep_alive = false;
    for (_, value) in head.headers_named(buf, b"connection") {
        if has_token(value, b"close") {
            return false;
        }
        if has_token(value, b"keep-alive") {
            saw_keep_alive = true;
        }
    }
    match version {
        Version::Http11 => true,
        Version::Http10 => saw_keep_alive,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn head_of(wire: &[u8]) -> (Head, Vec<u8>) {
        let mut head = Head::default();
        let buf = wire.to_vec();
        let complete = parse_request_head(&buf, &mut head).expect("valid request");
        assert!(complete, "test input is a complete head");
        (head, buf)
    }

    fn response_head_of(wire: &[u8]) -> (Head, Vec<u8>) {
        let mut head = Head::default();
        let buf = wire.to_vec();
        let complete = parse_response_head(&buf, &mut head).expect("valid response");
        assert!(complete, "test input is a complete head");
        (head, buf)
    }

    #[test]
    fn a_request_with_no_framing_header_has_no_body() {
        let (head, buf) = head_of(b"GET / HTTP/1.1\r\nHost: a\r\n\r\n");
        assert_eq!(request_framing(&head, &buf), Ok(Framing::Empty));
    }

    #[test]
    fn content_length_frames_a_request_body() {
        let (head, buf) = head_of(b"POST / HTTP/1.1\r\nHost: a\r\nContent-Length: 5\r\n\r\n");
        assert_eq!(request_framing(&head, &buf), Ok(Framing::Length(5)));
    }

    #[test]
    fn a_zero_content_length_is_the_same_as_no_body() {
        let (head, buf) = head_of(b"POST / HTTP/1.1\r\nHost: a\r\nContent-Length: 0\r\n\r\n");
        assert_eq!(request_framing(&head, &buf), Ok(Framing::Empty));
    }

    #[test]
    fn chunked_frames_a_request_body() {
        let (head, buf) =
            head_of(b"POST / HTTP/1.1\r\nHost: a\r\nTransfer-Encoding: chunked\r\n\r\n");
        assert_eq!(request_framing(&head, &buf), Ok(Framing::Chunked));
    }

    #[test]
    fn transfer_encoding_and_content_length_together_are_refused() {
        // The classic smuggling pair. Whatever we picked, the next hop might
        // pick the other one.
        let (head, buf) = head_of(
            b"POST / HTTP/1.1\r\nHost: a\r\nContent-Length: 5\r\nTransfer-Encoding: chunked\r\n\r\n",
        );
        assert!(matches!(
            request_framing(&head, &buf),
            Err(CodecError::Malformed(_))
        ));
    }

    #[test]
    fn conflicting_content_lengths_are_refused() {
        let (head, buf) =
            head_of(b"POST / HTTP/1.1\r\nHost: a\r\nContent-Length: 5\r\nContent-Length: 6\r\n\r\n");
        assert!(matches!(
            request_framing(&head, &buf),
            Err(CodecError::Malformed(_))
        ));
    }

    #[test]
    fn repeated_but_agreeing_content_lengths_are_accepted() {
        let (head, buf) =
            head_of(b"POST / HTTP/1.1\r\nHost: a\r\nContent-Length: 5\r\nContent-Length: 5\r\n\r\n");
        assert_eq!(request_framing(&head, &buf), Ok(Framing::Length(5)));
    }

    #[test]
    fn a_transfer_coding_we_cannot_forward_is_refused() {
        let (head, buf) =
            head_of(b"POST / HTTP/1.1\r\nHost: a\r\nTransfer-Encoding: gzip, chunked\r\n\r\n");
        assert_eq!(
            request_framing(&head, &buf),
            Err(CodecError::Unsupported("a transfer coding other than chunked"))
        );
    }

    #[test]
    fn a_content_length_that_is_not_a_number_is_refused() {
        for bad in [
            &b"POST / HTTP/1.1\r\nHost: a\r\nContent-Length: +5\r\n\r\n"[..],
            b"POST / HTTP/1.1\r\nHost: a\r\nContent-Length: 5x\r\n\r\n",
            b"POST / HTTP/1.1\r\nHost: a\r\nContent-Length: \r\n\r\n",
            b"POST / HTTP/1.1\r\nHost: a\r\nContent-Length: -1\r\n\r\n",
        ] {
            let (head, buf) = head_of(bad);
            assert!(
                matches!(request_framing(&head, &buf), Err(CodecError::Malformed(_))),
                "{:?} should be refused",
                String::from_utf8_lossy(bad)
            );
        }
    }

    #[test]
    fn a_204_has_no_body_however_it_is_framed() {
        let (head, buf) = response_head_of(b"HTTP/1.1 204 No Content\r\nContent-Length: 9\r\n\r\n");
        assert_eq!(response_framing(&head, &buf, false), Ok(Framing::Empty));
    }

    #[test]
    fn a_304_and_a_1xx_have_no_body() {
        for wire in [
            &b"HTTP/1.1 304 Not Modified\r\nContent-Length: 9\r\n\r\n"[..],
            b"HTTP/1.1 100 Continue\r\n\r\n",
        ] {
            let (head, buf) = response_head_of(wire);
            assert_eq!(response_framing(&head, &buf, false), Ok(Framing::Empty));
        }
    }

    #[test]
    fn a_response_to_head_carries_a_length_but_no_bytes() {
        // The length is the one a GET would have had. Waiting for those bytes
        // is how a proxy hangs on a HEAD.
        let (head, buf) = response_head_of(b"HTTP/1.1 200 OK\r\nContent-Length: 128\r\n\r\n");
        assert_eq!(response_framing(&head, &buf, true), Ok(Framing::Empty));
        assert_eq!(response_framing(&head, &buf, false), Ok(Framing::Length(128)));
    }

    #[test]
    fn a_response_with_no_framing_runs_to_close() {
        let (head, buf) = response_head_of(b"HTTP/1.1 200 OK\r\nServer: x\r\n\r\n");
        assert_eq!(response_framing(&head, &buf, false), Ok(Framing::UntilClose));
        assert!(!Framing::UntilClose.allows_reuse());
    }

    #[test]
    fn keep_alive_follows_the_version_and_the_connection_header() {
        let cases: [(&[u8], Version, bool); 5] = [
            (b"GET / HTTP/1.1\r\nHost: a\r\n\r\n", Version::Http11, true),
            (
                b"GET / HTTP/1.1\r\nHost: a\r\nConnection: close\r\n\r\n",
                Version::Http11,
                false,
            ),
            (b"GET / HTTP/1.0\r\nHost: a\r\n\r\n", Version::Http10, false),
            (
                b"GET / HTTP/1.0\r\nHost: a\r\nConnection: keep-alive\r\n\r\n",
                Version::Http10,
                true,
            ),
            // A token scan, not an equality test.
            (
                b"GET / HTTP/1.1\r\nHost: a\r\nConnection: keep-alive, close\r\n\r\n",
                Version::Http11,
                false,
            ),
        ];
        for (wire, version, expected) in cases {
            let (head, buf) = head_of(wire);
            assert_eq!(
                keep_alive(&head, &buf, version),
                expected,
                "{:?}",
                String::from_utf8_lossy(wire)
            );
        }
    }

    #[test]
    fn has_token_matches_one_element_of_a_list() {
        assert!(has_token(b"keep-alive, Upgrade", b"upgrade"));
        assert!(has_token(b"  close  ", b"close"));
        assert!(!has_token(b"keep-alive", b"close"));
        // A substring is not a token.
        assert!(!has_token(b"not-close", b"close"));
    }

    #[test]
    fn trim_ows_removes_spaces_and_tabs_only() {
        assert_eq!(trim_ows(b" \tvalue \t"), b"value");
        assert_eq!(trim_ows(b"value"), b"value");
        assert_eq!(trim_ows(b"   "), b"");
    }
}
