//! Head parsing for requests and responses.
//!
//! Both directions share everything below the start line, so they share the
//! code: one header-block parser, two start-line parsers, one [`Head`] to hold
//! either result.
//!
//! Nothing here allocates per message. A [`Head`] owns one `Vec<HeaderSpan>`
//! that is cleared and refilled, and a connection keeps one `Head` for its
//! whole life, so a warm connection parses every subsequent request with no
//! allocation at all.

use super::{CodecError, Version, MAX_HEAD, MAX_HEADERS};

/// A `(offset, length)` pair into the buffer a head was parsed from.
///
/// `u16` rather than `usize` because a head is capped at [`MAX_HEAD`] = 16 KiB,
/// which halves the size of the header table and keeps a 64-field request's
/// spans inside two cache lines.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Span {
    /// Offset of the first byte.
    pub start: u16,
    /// Number of bytes.
    pub len: u16,
}

impl Span {
    /// A span of nothing, at the start.
    pub const EMPTY: Span = Span { start: 0, len: 0 };

    fn at(start: usize, len: usize) -> Span {
        // Both fit: a head is refused past MAX_HEAD, which is 16 KiB.
        debug_assert!(start + len <= MAX_HEAD);
        Span {
            start: start as u16,
            len: len as u16,
        }
    }

    /// The bytes this span points at.
    ///
    /// # Panics
    ///
    /// If `buf` is not the buffer the span was parsed from, or is shorter than
    /// it was. Spans and their buffer travel together by convention; there is
    /// no lifetime tying them, because a `Head` outlives any one buffer.
    pub fn bytes(self, buf: &[u8]) -> &[u8] {
        let start = self.start as usize;
        &buf[start..start + self.len as usize]
    }

    /// The bytes as UTF-8, or `None` if they are not.
    pub fn str(self, buf: &[u8]) -> Option<&str> {
        std::str::from_utf8(self.bytes(buf)).ok()
    }

    /// Whether this span is empty.
    pub fn is_empty(self) -> bool {
        self.len == 0
    }
}

/// One header field, as two spans into the head.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HeaderSpan {
    /// The field name, in the case it arrived in.
    pub name: Span,
    /// The field value, with optional whitespace already trimmed.
    pub value: Span,
}

/// The first line of a message.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StartLine {
    /// A request line: `GET /path HTTP/1.1`.
    Request {
        /// The method, verbatim.
        method: Span,
        /// The request target, verbatim — path and query, not decoded.
        target: Span,
        /// The declared version.
        version: Version,
    },
    /// A status line: `HTTP/1.1 200 OK`.
    Status {
        /// The three-digit status code.
        code: u16,
        /// The reason phrase, which may be empty.
        reason: Span,
        /// The declared version.
        version: Version,
    },
}

/// A parsed message head, plus the scan state that makes partial reads linear.
#[derive(Debug, Clone)]
pub struct Head {
    /// The start line. Meaningless until a parse has returned `Ok(true)`.
    pub start: StartLine,
    /// Header fields in wire order, names in their original case.
    pub headers: Vec<HeaderSpan>,
    /// Total bytes of the head, including the terminating `\r\n\r\n`. The body,
    /// if any, begins here.
    pub len: usize,
    /// How far the `\r\n\r\n` search has already looked.
    ///
    /// Carried between calls so a head arriving one byte at a time costs one
    /// pass over the buffer in total rather than one per byte.
    scanned: usize,
}

impl Default for Head {
    fn default() -> Self {
        Head {
            start: StartLine::Request {
                method: Span::EMPTY,
                target: Span::EMPTY,
                version: Version::Http11,
            },
            headers: Vec::with_capacity(16),
            len: 0,
            scanned: 0,
        }
    }
}

impl Head {
    /// Ready this head to parse a new message, keeping its allocation.
    pub fn reset(&mut self) {
        self.headers.clear();
        self.len = 0;
        self.scanned = 0;
    }

    /// The declared version, whichever kind of start line this is.
    pub fn version(&self) -> Version {
        match self.start {
            StartLine::Request { version, .. } | StartLine::Status { version, .. } => version,
        }
    }

    /// The status code, or `None` for a request.
    pub fn status(&self) -> Option<u16> {
        match self.start {
            StartLine::Status { code, .. } => Some(code),
            StartLine::Request { .. } => None,
        }
    }

    /// The method, or `None` for a response.
    pub fn method<'a>(&self, buf: &'a [u8]) -> Option<&'a [u8]> {
        match self.start {
            StartLine::Request { method, .. } => Some(method.bytes(buf)),
            StartLine::Status { .. } => None,
        }
    }

    /// The request target, or `None` for a response.
    pub fn target<'a>(&self, buf: &'a [u8]) -> Option<&'a [u8]> {
        match self.start {
            StartLine::Request { target, .. } => Some(target.bytes(buf)),
            StartLine::Status { .. } => None,
        }
    }

    /// Every field, in wire order.
    pub fn iter<'a>(&'a self, buf: &'a [u8]) -> impl Iterator<Item = (&'a [u8], &'a [u8])> + 'a {
        self.headers
            .iter()
            .map(move |h| (h.name.bytes(buf), h.value.bytes(buf)))
    }

    /// Every field named `name`, case-insensitively, in wire order.
    ///
    /// A list rather than a lookup because HTTP allows repetition and the
    /// difference matters: `Connection` may arrive as several lines, and
    /// reading only the first is how a hop-by-hop header survives a proxy.
    pub fn headers_named<'a>(
        &'a self,
        buf: &'a [u8],
        name: &'a [u8],
    ) -> impl Iterator<Item = (&'a [u8], &'a [u8])> + 'a {
        self.iter(buf)
            .filter(move |(n, _)| n.eq_ignore_ascii_case(name))
    }

    /// The value of the first field named `name`, case-insensitively.
    pub fn header<'a>(&'a self, buf: &'a [u8], name: &[u8]) -> Option<&'a [u8]> {
        self.iter(buf)
            .find(|(n, _)| n.eq_ignore_ascii_case(name))
            .map(|(_, v)| v)
    }
}

/// Parse a request head out of the front of `buf`.
///
/// `Ok(false)` means the head is not all there yet; nothing has been consumed
/// and `out` is untouched apart from its scan cursor, so the caller appends
/// more bytes and calls again with the longer buffer. `Ok(true)` means `out`
/// describes a complete head occupying `out.len` bytes.
pub fn parse_request_head(buf: &[u8], out: &mut Head) -> Result<bool, CodecError> {
    let Some(end) = find_head_end(buf, out)? else {
        return Ok(false);
    };
    // `end - 4` drops the terminating CRLFCRLF; what is left is the start line
    // and the fields, separated by CRLF.
    let block = &buf[..end - 4];
    let (line, rest) = split_line(block, 0);
    out.start = parse_request_line(buf, line)?;
    parse_fields(buf, block, rest, out)?;
    reject_duplicate_host(buf, out)?;
    out.len = end;
    out.scanned = 0;
    Ok(true)
}

/// Parse a response head out of the front of `buf`. Same contract as
/// [`parse_request_head`].
pub fn parse_response_head(buf: &[u8], out: &mut Head) -> Result<bool, CodecError> {
    let Some(end) = find_head_end(buf, out)? else {
        return Ok(false);
    };
    let block = &buf[..end - 4];
    let (line, rest) = split_line(block, 0);
    out.start = parse_status_line(buf, line)?;
    parse_fields(buf, block, rest, out)?;
    out.len = end;
    out.scanned = 0;
    Ok(true)
}

/// Offset just past the first `\r\n\r\n`, resuming the search where the last
/// call left off.
fn find_head_end(buf: &[u8], out: &mut Head) -> Result<Option<usize>, CodecError> {
    // Back up three bytes: the terminator may straddle the boundary between
    // what was scanned last time and what has arrived since.
    let from = out.scanned.saturating_sub(3).min(buf.len());
    let found = buf[from..]
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .map(|i| from + i + 4);
    match found {
        Some(end) if end > MAX_HEAD => Err(CodecError::TooLarge),
        Some(end) => Ok(Some(end)),
        None => {
            out.scanned = buf.len();
            // Refuse *before* the terminator arrives. Waiting for a head that
            // is already over the limit is how a peer with a broken client
            // makes a connection cost megabytes.
            if buf.len() > MAX_HEAD {
                return Err(CodecError::TooLarge);
            }
            Ok(None)
        }
    }
}

/// The bytes up to the next CRLF, and the offset just after it.
fn split_line(block: &[u8], from: usize) -> (&[u8], usize) {
    let rest = &block[from..];
    match rest.windows(2).position(|w| w == b"\r\n") {
        Some(i) => (&rest[..i], from + i + 2),
        None => (rest, block.len()),
    }
}

fn parse_request_line(buf: &[u8], line: &[u8]) -> Result<StartLine, CodecError> {
    let base = offset_in(buf, line);
    let mut parts = line.splitn(3, |&b| b == b' ');
    let method = parts
        .next()
        .ok_or(CodecError::Malformed("request line is empty"))?;
    let target = parts
        .next()
        .ok_or(CodecError::Malformed("request line has too few parts"))?;
    let version = parts
        .next()
        .ok_or(CodecError::Malformed("request line has too few parts"))?;

    if method.is_empty() || !method.iter().all(|&b| is_tchar(b)) {
        return Err(CodecError::Malformed("malformed method"));
    }
    if target.is_empty() {
        return Err(CodecError::Malformed("request target missing"));
    }
    // A space inside the target is the oldest request-smuggling trick there is,
    // and `splitn(3)` would have folded it into the version field.
    if target.iter().any(|&b| b <= 0x20 || b == 0x7f) {
        return Err(CodecError::Malformed("malformed request target"));
    }
    let version = parse_version(version)?;
    Ok(StartLine::Request {
        method: Span::at(base, method.len()),
        target: Span::at(base + method.len() + 1, target.len()),
        version,
    })
}

fn parse_status_line(buf: &[u8], line: &[u8]) -> Result<StartLine, CodecError> {
    let base = offset_in(buf, line);
    let mut parts = line.splitn(3, |&b| b == b' ');
    let version = parts
        .next()
        .ok_or(CodecError::Malformed("status line is empty"))?;
    let code = parts
        .next()
        .ok_or(CodecError::Malformed("status line has no code"))?;
    // The reason phrase is decorative, and both `HTTP/1.1 200\r\n` and
    // `HTTP/1.1 200 \r\n` turn up from real servers.
    let reason = parts.next().unwrap_or(b"");

    let version = parse_version(version)?;
    if code.len() != 3 || !code.iter().all(u8::is_ascii_digit) {
        return Err(CodecError::Malformed("status code is not three digits"));
    }
    let code = u16::from(code[0] - b'0') * 100
        + u16::from(code[1] - b'0') * 10
        + u16::from(code[2] - b'0');
    let reason_start = base + version_len(line) + 1 + 3 + 1;
    Ok(StartLine::Status {
        code,
        reason: if reason.is_empty() {
            Span::EMPTY
        } else {
            Span::at(reason_start, reason.len())
        },
        version,
    })
}

/// Length of the version token at the head of a status line.
fn version_len(line: &[u8]) -> usize {
    line.iter().position(|&b| b == b' ').unwrap_or(line.len())
}

fn parse_version(token: &[u8]) -> Result<Version, CodecError> {
    match token {
        b"HTTP/1.1" => Ok(Version::Http11),
        b"HTTP/1.0" => Ok(Version::Http10),
        b"" => Err(CodecError::Malformed("version missing")),
        _ => Err(CodecError::Unsupported("http version")),
    }
}

/// Parse the field block, appending to `out.headers`.
fn parse_fields(
    buf: &[u8],
    block: &[u8],
    mut cursor: usize,
    out: &mut Head,
) -> Result<(), CodecError> {
    out.headers.clear();
    while cursor < block.len() {
        let (line, next) = split_line(block, cursor);
        cursor = next;
        if line.is_empty() {
            continue;
        }
        // Obsolete line folding. RFC 9112 §5.2 lets a proxy reject it or
        // replace it with spaces; rejecting is the safe half, because folding
        // is only ever seen now as a way to smuggle a field past one parser.
        if line[0] == b' ' || line[0] == b'\t' {
            return Err(CodecError::Malformed("obsolete line folding"));
        }
        let base = offset_in(buf, line);
        let colon = line
            .iter()
            .position(|&b| b == b':')
            .ok_or(CodecError::Malformed("header line has no colon"))?;
        let name = &line[..colon];
        // Whitespace before the colon is how request smuggling starts
        // (RFC 9112 §5.1 says reject), so no trimming on the name side.
        if name.is_empty() || !name.iter().all(|&b| is_tchar(b)) {
            return Err(CodecError::Malformed("malformed header name"));
        }
        let raw_value = &line[colon + 1..];
        if raw_value
            .iter()
            .any(|&b| (b < 0x20 && b != b'\t') || b == 0x7f)
        {
            return Err(CodecError::Malformed("header value contains a control byte"));
        }
        let trimmed = super::trim_ows(raw_value);
        let value_start = base + colon + 1 + offset_in(raw_value, trimmed);
        if out.headers.len() == MAX_HEADERS {
            return Err(CodecError::TooManyHeaders);
        }
        out.headers.push(HeaderSpan {
            name: Span::at(base, name.len()),
            value: Span::at(value_start, trimmed.len()),
        });
    }
    Ok(())
}

/// Two `Host` headers are two answers to "which site is this for", and a proxy
/// that picks one hands the other to whatever it forwards to.
///
/// Zero is *not* rejected: an HTTP/1.1 request without a `Host` is malformed by
/// the letter of the spec, but the hyper data plane answers it through the
/// no-host routing path rather than with a 400, and the two engines are
/// supposed to be indistinguishable from outside.
fn reject_duplicate_host(buf: &[u8], out: &Head) -> Result<(), CodecError> {
    if out.headers_named(buf, b"host").count() > 1 {
        return Err(CodecError::Malformed("more than one Host header"));
    }
    Ok(())
}

/// Byte offset of `inner` within `outer`, both being slices of one allocation.
fn offset_in(outer: &[u8], inner: &[u8]) -> usize {
    // Slices of the same buffer; the subtraction is the only way to recover an
    // index once `split`/`trim` have handed back a subslice.
    (inner.as_ptr() as usize) - (outer.as_ptr() as usize)
}

/// The HTTP token character set, RFC 9110 §5.6.2.
pub fn is_tchar(b: u8) -> bool {
    b.is_ascii_alphanumeric()
        || matches!(
            b,
            b'!' | b'#'
                | b'$'
                | b'%'
                | b'&'
                | b'\''
                | b'*'
                | b'+'
                | b'-'
                | b'.'
                | b'^'
                | b'_'
                | b'`'
                | b'|'
                | b'~'
        )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse_req(wire: &[u8]) -> Result<(Head, Vec<u8>), CodecError> {
        let mut head = Head::default();
        let buf = wire.to_vec();
        parse_request_head(&buf, &mut head)?;
        Ok((head, buf))
    }

    #[test]
    fn a_minimal_request_parses() {
        let (head, buf) = parse_req(b"GET /hello?x=1 HTTP/1.1\r\nHost: example\r\n\r\n").unwrap();
        assert_eq!(head.method(&buf), Some(&b"GET"[..]));
        assert_eq!(head.target(&buf), Some(&b"/hello?x=1"[..]));
        assert_eq!(head.version(), Version::Http11);
        assert_eq!(head.header(&buf, b"host"), Some(&b"example"[..]));
        assert_eq!(head.len, buf.len());
    }

    #[test]
    fn header_lookup_is_case_insensitive_and_the_case_is_preserved() {
        let (head, buf) =
            parse_req(b"GET / HTTP/1.1\r\nHost: a\r\nX-Odd-CASE: v\r\n\r\n").unwrap();
        assert_eq!(head.header(&buf, b"x-odd-case"), Some(&b"v"[..]));
        let names: Vec<_> = head.iter(&buf).map(|(n, _)| n.to_vec()).collect();
        assert_eq!(names, vec![b"Host".to_vec(), b"X-Odd-CASE".to_vec()]);
    }

    #[test]
    fn optional_whitespace_around_a_value_is_trimmed() {
        let (head, buf) = parse_req(b"GET / HTTP/1.1\r\nHost:  \t a \t \r\n\r\n").unwrap();
        assert_eq!(head.header(&buf, b"host"), Some(&b"a"[..]));
    }

    #[test]
    fn a_partial_head_needs_more_and_resumes() {
        let wire = b"GET / HTTP/1.1\r\nHost: example\r\n\r\n";
        let mut head = Head::default();
        // Feed it one byte at a time; only the last byte completes it.
        for n in 1..wire.len() {
            let complete = parse_request_head(&wire[..n], &mut head).expect("valid prefix");
            assert!(!complete, "prefix of {n} bytes should not be complete");
        }
        assert!(parse_request_head(wire, &mut head).unwrap());
        assert_eq!(head.header(wire, b"host"), Some(&b"example"[..]));
    }

    #[test]
    fn a_terminator_split_across_reads_is_still_found() {
        // The resume cursor backs up three bytes precisely so this works.
        let wire = b"GET / HTTP/1.1\r\nHost: a\r\n\r\n";
        let mut head = Head::default();
        assert!(!parse_request_head(&wire[..wire.len() - 2], &mut head).unwrap());
        assert!(parse_request_head(wire, &mut head).unwrap());
        assert_eq!(head.len, wire.len());
    }

    #[test]
    fn a_pipelined_second_request_is_left_in_the_buffer() {
        let wire = b"GET /one HTTP/1.1\r\nHost: a\r\n\r\nGET /two HTTP/1.1\r\nHost: a\r\n\r\n";
        let (head, buf) = parse_req(wire).unwrap();
        assert_eq!(head.target(&buf), Some(&b"/one"[..]));
        assert_eq!(&buf[head.len..], b"GET /two HTTP/1.1\r\nHost: a\r\n\r\n");
    }

    #[test]
    fn a_response_parses_with_and_without_a_reason() {
        for (wire, reason) in [
            (&b"HTTP/1.1 200 OK\r\nServer: x\r\n\r\n"[..], &b"OK"[..]),
            (b"HTTP/1.1 200 \r\nServer: x\r\n\r\n", b""),
            (b"HTTP/1.1 200\r\nServer: x\r\n\r\n", b""),
            (
                b"HTTP/1.1 404 Not Found Here\r\nServer: x\r\n\r\n",
                b"Not Found Here",
            ),
        ] {
            let mut head = Head::default();
            let buf = wire.to_vec();
            assert!(parse_response_head(&buf, &mut head).unwrap(), "{wire:?}");
            assert_eq!(head.status(), Some(if wire[9] == b'2' { 200 } else { 404 }));
            let StartLine::Status { reason: span, .. } = head.start else {
                panic!("a status line");
            };
            assert_eq!(span.bytes(&buf), reason, "{wire:?}");
        }
    }

    #[test]
    fn malformed_start_lines_are_refused() {
        for bad in [
            &b"GET\r\nHost: a\r\n\r\n"[..],
            b"GET / \r\nHost: a\r\n\r\n",
            b" / HTTP/1.1\r\nHost: a\r\n\r\n",
            b"GE T / HTTP/1.1\r\nHost: a\r\n\r\n",
            b"GET /a b HTTP/1.1\r\nHost: a\r\n\r\n",
        ] {
            assert!(
                parse_req(bad).is_err(),
                "{:?} should be refused",
                String::from_utf8_lossy(bad)
            );
        }
    }

    #[test]
    fn an_unknown_version_is_unsupported_not_malformed() {
        assert_eq!(
            parse_req(b"GET / HTTP/2.0\r\nHost: a\r\n\r\n").unwrap_err(),
            CodecError::Unsupported("http version")
        );
    }

    #[test]
    fn malformed_header_lines_are_refused() {
        for bad in [
            &b"GET / HTTP/1.1\r\nHost a\r\n\r\n"[..],
            b"GET / HTTP/1.1\r\nHost : a\r\n\r\n",
            b"GET / HTTP/1.1\r\n: a\r\n\r\n",
            b"GET / HTTP/1.1\r\nHo st: a\r\n\r\n",
        ] {
            assert!(
                parse_req(bad).is_err(),
                "{:?} should be refused",
                String::from_utf8_lossy(bad)
            );
        }
    }

    #[test]
    fn obsolete_line_folding_is_refused() {
        let bad = b"GET / HTTP/1.1\r\nHost: a\r\nX-Long: one\r\n  two\r\n\r\n";
        assert_eq!(
            parse_req(bad).unwrap_err(),
            CodecError::Malformed("obsolete line folding")
        );
    }

    #[test]
    fn two_host_headers_are_refused_but_zero_is_allowed() {
        assert_eq!(
            parse_req(b"GET / HTTP/1.1\r\nHost: a\r\nHost: b\r\n\r\n").unwrap_err(),
            CodecError::Malformed("more than one Host header")
        );
        // No Host is routed, not rejected — the hyper engine does the same.
        let (head, buf) = parse_req(b"GET / HTTP/1.1\r\nX: y\r\n\r\n").unwrap();
        assert_eq!(head.header(&buf, b"host"), None);
    }

    #[test]
    fn a_head_past_the_limit_is_refused_before_it_completes() {
        let mut wire = b"GET / HTTP/1.1\r\nHost: a\r\nX: ".to_vec();
        wire.resize(MAX_HEAD + 100, b'v');
        let mut head = Head::default();
        // No terminator anywhere, and already over the cap.
        assert_eq!(
            parse_request_head(&wire, &mut head).unwrap_err(),
            CodecError::TooLarge
        );
    }

    #[test]
    fn more_than_the_field_limit_is_refused() {
        let mut wire = b"GET / HTTP/1.1\r\nHost: a\r\n".to_vec();
        for i in 0..MAX_HEADERS {
            wire.extend_from_slice(format!("X-{i}: v\r\n").as_bytes());
        }
        wire.extend_from_slice(b"\r\n");
        assert_eq!(parse_req(&wire).unwrap_err(), CodecError::TooManyHeaders);
    }

    #[test]
    fn a_header_value_may_hold_bytes_that_are_not_utf8() {
        // obs-text is legal, and a proxy that 400s a Latin-1 filename breaks
        // traffic it was only asked to forward.
        let wire = b"GET / HTTP/1.1\r\nHost: a\r\nX-Name: caf\xe9\r\n\r\n";
        let (head, buf) = parse_req(wire).unwrap();
        assert_eq!(head.header(&buf, b"x-name"), Some(&b"caf\xe9"[..]));
    }

    #[test]
    fn a_control_byte_in_a_value_is_refused() {
        assert!(parse_req(b"GET / HTTP/1.1\r\nHost: a\r\nX: v\x01v\r\n\r\n").is_err());
    }

    #[test]
    fn reset_keeps_the_allocation_and_clears_the_state() {
        let (mut head, _) = parse_req(b"GET / HTTP/1.1\r\nHost: a\r\nX: y\r\n\r\n").unwrap();
        let capacity = head.headers.capacity();
        head.reset();
        assert_eq!(head.headers.len(), 0);
        assert_eq!(head.len, 0);
        assert_eq!(head.headers.capacity(), capacity);
    }
}
