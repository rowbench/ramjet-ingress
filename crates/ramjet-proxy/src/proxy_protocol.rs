//! The PROXY protocol, versions 1 and 2, on the traffic listeners.
//!
//! # What it is for
//!
//! A cloud L4 load balancer — an AWS NLB, a DigitalOcean or Scaleway LB, a GCP
//! passthrough LB — forwards TCP without touching the payload. That is what
//! makes it fast, and it is also why the connection this process accepts comes
//! from the load balancer's address and not the client's. There is no
//! `X-Forwarded-For` to read, because at that layer there are no headers at
//! all: TLS has not been terminated yet, and on a plaintext listener the load
//! balancer never parsed the request.
//!
//! HAProxy's PROXY protocol fixes that by prepending a small block to the
//! stream, before any application bytes, saying who the real client is. This
//! module parses that block and hands back the address, which the server then
//! uses as [`ConnInfo::remote`](crate::forward::ConnInfo::remote) — so
//! `X-Forwarded-For`, `X-Real-IP`, and anything that logs a peer see the client
//! rather than the load balancer.
//!
//! # The trust model, stated plainly
//!
//! **The header *is* the client identity.** Anything that can open a TCP
//! connection to this listener and write `PROXY TCP4 1.2.3.4 ...` becomes
//! `1.2.3.4` as far as every downstream application is concerned — including
//! whatever reads `X-Forwarded-For` to make an allow-list or rate-limiting
//! decision. So enable this **only** on a listener that nothing but the load
//! balancer can reach, and only when that load balancer is configured to always
//! send the header. Turning it on for a socket exposed to the open internet is
//! handing out IP spoofing as a feature.
//!
//! That is also why the header is **required** rather than optional when the
//! feature is on: a connection whose first bytes are not a valid PROXY header is
//! dropped. A permissive mode that falls back to the socket address would let an
//! attacker choose, per connection, whether to be spoofed or not, which is
//! strictly worse than either fixed answer. nginx's `proxy_protocol` listener
//! parameter and HAProxy's `accept-proxy` both behave this way.
//!
//! # Both versions, because both are in the field
//!
//! v1 is a text line (`PROXY TCP4 <src> <dst> <sport> <dport>\r\n`, at most 107
//! bytes) and is what a human sees in a packet capture. v2 is a 16-byte binary
//! header plus an address block and optional TLVs, and is what AWS sends —
//! along with a TLV naming the VPC endpoint, which this parser skips along with
//! every other TLV. A sender picks one; a receiver that wants to work behind
//! more than one cloud has to read both. The first byte tells them apart: v1
//! starts with `P`, v2's signature starts with `\r`.
//!
//! # Sans-io, and incremental
//!
//! [`parse`] is a pure function over a byte slice. It never blocks, never reads,
//! and answers one of three things: the header is complete (with how many bytes
//! it occupied), it is not complete yet, or it is invalid. That is what lets the
//! whole state space below — every version, family, and truncation — be tested
//! without a socket, and it is what makes reading the header across several
//! `read` calls fall out for free.
//!
//! The byte count matters as much as the address. A load balancer relays the
//! client's first bytes immediately after the header, so a single read very
//! often returns the header *and* the start of a TLS ClientHello or an HTTP
//! request. Those bytes belong to the connection and must not be dropped, so
//! [`accept`] returns a [`Prefixed`] stream that replays them before reading the
//! socket again.

use std::io;
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr};
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use bytes::{Buf, Bytes};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, ReadBuf};

/// The v1 header's opening token, space included.
const V1_SIGNATURE: &[u8] = b"PROXY ";

/// The longest a v1 line may be, CRLF included, per the specification.
///
/// It is not an arbitrary bound: it is exactly long enough for the widest legal
/// line, `PROXY TCP6 <39> <39> <5> <5>\r\n`. A sender that has not produced a
/// CRLF by here is not going to.
const V1_MAX_LEN: usize = 107;

/// The v2 header's 12-byte signature.
///
/// Deliberately unprintable and deliberately containing `\r\n\r\n`: a v2 header
/// arriving at something that expected HTTP terminates the request line
/// immediately rather than being parsed as one.
const V2_SIGNATURE: [u8; 12] = [
    0x0D, 0x0A, 0x0D, 0x0A, 0x00, 0x0D, 0x0A, 0x51, 0x55, 0x49, 0x54, 0x0A,
];

/// Signature, version/command, family/protocol, and the 16-bit length.
const V2_PREAMBLE_LEN: usize = 16;

/// Bytes in a v2 `AF_INET` address block: two addresses and two ports.
const V2_INET_LEN: usize = 12;

/// Bytes in a v2 `AF_INET6` address block.
const V2_INET6_LEN: usize = 36;

/// The most a valid header can occupy: the v2 preamble plus its widest length.
///
/// This is what bounds the buffer [`accept`] is willing to fill. A v1 line is
/// capped far lower, at the specification's 107 bytes.
pub const MAX_HEADER_LEN: usize = V2_PREAMBLE_LEN + u16::MAX as usize;

/// How much [`accept`] reads at a time.
///
/// Comfortably more than the widest header anyone sends — a v1 line is at most
/// 107 bytes and AWS's v2 header with its VPC-endpoint TLV is under 100 — so the
/// header normally arrives in one read. Reading past it is not a problem: the
/// surplus is the connection's own first bytes and is handed on intact.
const READ_CHUNK: usize = 512;

/// How far [`parse`] got.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Parsed {
    /// The bytes so far are a valid prefix of a header, but not a whole one.
    Incomplete,
    /// A complete header.
    Done {
        /// Bytes the header occupied. Everything after this belongs to the
        /// connection and must be preserved.
        consumed: usize,
        /// The client the header names, or `None` when it names nobody.
        ///
        /// `None` is a success, not a failure: a v2 `LOCAL` command (which is
        /// what a load balancer's own health check sends), a v1 `UNKNOWN`, and a
        /// v2 `AF_UNSPEC` are all well-formed headers that carry no address. The
        /// header is still consumed; the socket's own peer address stands.
        client: Option<SocketAddr>,
    },
}

/// Why a byte sequence is not a PROXY header.
///
/// Both variants end the connection. They are kept apart because they mean
/// different things operationally: [`Signature`](HeaderError::Signature) is
/// almost always "this listener has `--proxy-protocol` on and something is
/// talking to it directly", while [`Malformed`](HeaderError::Malformed) is a
/// sender that meant to speak the protocol and got it wrong.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HeaderError {
    /// The first bytes match neither the v1 nor the v2 signature.
    Signature,
    /// The signature matched but the rest of the header did not hold up.
    Malformed(&'static str),
}

impl std::fmt::Display for HeaderError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            HeaderError::Signature => f.write_str("no PROXY protocol signature"),
            HeaderError::Malformed(why) => write!(f, "malformed PROXY header: {why}"),
        }
    }
}

impl std::error::Error for HeaderError {}

/// Why a header could not be read off a socket.
#[derive(Debug)]
pub enum AcceptError {
    /// The socket failed, or closed before a complete header arrived.
    Io(io::Error),
    /// The sender did not finish the header in time.
    Timeout,
    /// Bytes arrived, and they were not a PROXY header.
    Header(HeaderError),
}

impl From<HeaderError> for AcceptError {
    fn from(error: HeaderError) -> Self {
        AcceptError::Header(error)
    }
}

impl std::fmt::Display for AcceptError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AcceptError::Io(error) => write!(f, "reading a PROXY header failed: {error}"),
            AcceptError::Timeout => f.write_str("no complete PROXY header before the deadline"),
            AcceptError::Header(error) => error.fmt(f),
        }
    }
}

impl std::error::Error for AcceptError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            AcceptError::Io(error) => Some(error),
            AcceptError::Header(error) => Some(error),
            AcceptError::Timeout => None,
        }
    }
}

/// Parses a PROXY header from the front of `input`.
///
/// Sans-io and incremental: call it with whatever has arrived, and call it again
/// with more when it answers [`Parsed::Incomplete`]. It never looks past the
/// header it is reading, and [`Parsed::Done::consumed`] says exactly where that
/// header ended.
///
/// An error is returned as early as it can be proven — `GET / HTTP/1.1` is
/// rejected on its second byte rather than after waiting for a CRLF that would
/// have made it a valid v1 line.
pub fn parse(input: &[u8]) -> Result<Parsed, HeaderError> {
    // The two versions were designed to be distinguishable from the first byte,
    // which is why this needs no lookahead and no backtracking.
    match input.first() {
        None => Ok(Parsed::Incomplete),
        Some(0x0D) => parse_v2(input),
        Some(b'P') => parse_v1(input),
        Some(_) => Err(HeaderError::Signature),
    }
}

/// Compares `input` against a signature it may only have a prefix of.
///
/// `Ok(true)` means the whole signature is present, `Ok(false)` that what is
/// there so far matches and more is needed.
fn signature_matches(input: &[u8], signature: &[u8]) -> Result<bool, HeaderError> {
    for (index, expected) in signature.iter().enumerate() {
        match input.get(index) {
            None => return Ok(false),
            Some(byte) if byte == expected => {}
            Some(_) => return Err(HeaderError::Signature),
        }
    }
    Ok(true)
}

/// Parses the text form: `PROXY TCP4 <src> <dst> <sport> <dport>\r\n`.
fn parse_v1(input: &[u8]) -> Result<Parsed, HeaderError> {
    if !signature_matches(input, V1_SIGNATURE)? {
        return Ok(Parsed::Incomplete);
    }

    // The CR has to leave room for its LF inside the 107-byte budget, so the
    // last position it may legally occupy is `V1_MAX_LEN - 2`.
    let searchable = input.len().min(V1_MAX_LEN - 1);
    let Some(rest) = input.get(..searchable) else {
        return Ok(Parsed::Incomplete);
    };
    let Some(cr) = rest.iter().position(|byte| *byte == b'\r') else {
        return if searchable == V1_MAX_LEN - 1 {
            Err(HeaderError::Malformed(
                "a v1 line reached its 107-byte limit without a CRLF",
            ))
        } else {
            Ok(Parsed::Incomplete)
        };
    };
    match input.get(cr + 1) {
        None => return Ok(Parsed::Incomplete),
        // A bare CR is not a line ending here. The specification is explicit
        // that CR appears exactly once, immediately before the LF, and a parser
        // that shrugs at the difference is one that can be desynchronised.
        Some(byte) if *byte != b'\n' => {
            return Err(HeaderError::Malformed("a v1 line contains a bare CR"))
        }
        Some(_) => {}
    }

    let consumed = cr + 2;
    let Some(fields) = input.get(V1_SIGNATURE.len()..cr) else {
        return Ok(Parsed::Incomplete);
    };
    let Ok(fields) = std::str::from_utf8(fields) else {
        return Err(HeaderError::Malformed("a v1 line is not ASCII"));
    };

    // `UNKNOWN` is how a sender says it has a connection but cannot describe
    // its ends — a health check from the balancer itself, or a protocol it does
    // not recognise. The specification allows it to be followed by anything at
    // all, which the receiver must ignore, so nothing after the token is parsed.
    if fields == "UNKNOWN" || fields.starts_with("UNKNOWN ") {
        return Ok(Parsed::Done {
            consumed,
            client: None,
        });
    }

    let mut parts = fields.split(' ');
    let (
        Some(family),
        Some(source),
        Some(destination),
        Some(source_port),
        Some(destination_port),
    ) = (
        parts.next(),
        parts.next(),
        parts.next(),
        parts.next(),
        parts.next(),
    )
    else {
        return Err(HeaderError::Malformed("a v1 line is missing fields"));
    };
    if parts.next().is_some() {
        return Err(HeaderError::Malformed("a v1 line has trailing fields"));
    }
    // Splitting on a single space means a doubled one produces an empty field,
    // which fails here rather than being silently absorbed.
    let (source_port, _destination_port) = (port(source_port)?, port(destination_port)?);

    // The destination is parsed and then dropped. It is not wanted — this is an
    // ingress, and the address the connection arrived on is already known — but
    // a header naming an unparseable destination is a malformed header, and
    // accepting it would mean accepting a sender whose framing is suspect.
    let client: SocketAddr = match family {
        "TCP4" => {
            let (Ok(source), Ok(_destination)) = (
                source.parse::<Ipv4Addr>(),
                destination.parse::<Ipv4Addr>(),
            ) else {
                return Err(HeaderError::Malformed("a TCP4 line names a non-IPv4 address"));
            };
            SocketAddr::from((source, source_port))
        }
        "TCP6" => {
            let (Ok(source), Ok(_destination)) = (
                source.parse::<Ipv6Addr>(),
                destination.parse::<Ipv6Addr>(),
            ) else {
                return Err(HeaderError::Malformed("a TCP6 line names a non-IPv6 address"));
            };
            SocketAddr::from((source, source_port))
        }
        _ => {
            return Err(HeaderError::Malformed(
                "a v1 line names a protocol other than TCP4, TCP6 or UNKNOWN",
            ))
        }
    };

    Ok(Parsed::Done {
        consumed,
        client: Some(client),
    })
}

/// Parses a decimal port, rejecting everything `u16::from_str` would wave
/// through that a PROXY header may not contain — a sign, whitespace, emptiness.
fn port(field: &str) -> Result<u16, HeaderError> {
    if field.is_empty() || !field.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(HeaderError::Malformed("a v1 port is not a decimal number"));
    }
    field
        .parse()
        .map_err(|_| HeaderError::Malformed("a v1 port is outside 0-65535"))
}

/// Parses the binary form: a 16-byte preamble, an address block, and TLVs.
fn parse_v2(input: &[u8]) -> Result<Parsed, HeaderError> {
    if !signature_matches(input, &V2_SIGNATURE)? {
        return Ok(Parsed::Incomplete);
    }

    // Checked before the length is even read, so a sender that is not speaking
    // this protocol is refused without first buffering up to 64 KiB for it.
    let Some(&version_command) = input.get(12) else {
        return Ok(Parsed::Incomplete);
    };
    if version_command >> 4 != 0x2 {
        return Err(HeaderError::Malformed("a v2 header declares a version other than 2"));
    }
    let command = version_command & 0x0F;
    if command > 0x1 {
        return Err(HeaderError::Malformed(
            "a v2 header declares a command other than LOCAL or PROXY",
        ));
    }

    let Some(&family_protocol) = input.get(13) else {
        return Ok(Parsed::Incomplete);
    };
    let Some(&[high, low]) = input.get(14..16) else {
        return Ok(Parsed::Incomplete);
    };

    let length = usize::from(u16::from_be_bytes([high, low]));
    let consumed = V2_PREAMBLE_LEN + length;
    let Some(body) = input.get(V2_PREAMBLE_LEN..consumed) else {
        return Ok(Parsed::Incomplete);
    };

    // LOCAL means the connection is the sender's own and not proxied on behalf
    // of anybody — a load balancer's health probe. The header is consumed and
    // the socket's peer address stands, which is exactly right: the peer really
    // is who sent it.
    let client = match command {
        0x1 => proxied_address(family_protocol, body)?,
        _ => None,
    };

    // Anything past the address block is TLVs. Skipping them is not a shortcut:
    // they are optional metadata (AWS's VPC endpoint id, an SSL summary, a
    // namespace) and nothing here routes on any of it. `consumed` steps over
    // them, so a header with TLVs costs exactly what one without them does.
    Ok(Parsed::Done { consumed, client })
}

/// The source address a v2 `PROXY` command's address block names.
fn proxied_address(
    family_protocol: u8,
    body: &[u8],
) -> Result<Option<SocketAddr>, HeaderError> {
    // The low nibble is the transport (stream or datagram) and is deliberately
    // not checked. The address sits at the same offset either way, and refusing
    // a header whose address is right there to be read would be a worse answer
    // than reading it.
    match family_protocol >> 4 {
        // AF_UNSPEC: the sender is proxying something it cannot express as an
        // address. Valid, consumed, no address — same outcome as v1 UNKNOWN.
        0x0 => Ok(None),
        // The destination port is fetched and discarded, and that is what makes
        // the length check exact: it is the last field of the block, so a body
        // that has it is a body long enough to hold all of it.
        0x1 => {
            let (Some(&[a, b, c, d]), Some(&[high, low]), Some(_destination_port)) = (
                body.get(..4),
                body.get(8..10),
                body.get(10..V2_INET_LEN),
            ) else {
                return Err(HeaderError::Malformed(
                    "a v2 INET header is shorter than its 12-byte address block",
                ));
            };
            Ok(Some(SocketAddr::from((
                Ipv4Addr::new(a, b, c, d),
                u16::from_be_bytes([high, low]),
            ))))
        }
        0x2 => {
            let (Some(address), Some(&[high, low]), Some(_destination_port)) = (
                body.get(..16),
                body.get(32..34),
                body.get(34..V2_INET6_LEN),
            ) else {
                return Err(HeaderError::Malformed(
                    "a v2 INET6 header is shorter than its 36-byte address block",
                ));
            };
            let Ok(octets) = <[u8; 16]>::try_from(address) else {
                return Err(HeaderError::Malformed(
                    "a v2 INET6 address is not sixteen bytes",
                ));
            };
            Ok(Some(SocketAddr::from((
                Ipv6Addr::from(octets),
                u16::from_be_bytes([high, low]),
            ))))
        }
        // AF_UNIX. A well-formed header naming a filesystem path, which is not
        // a client IP and cannot be turned into one, so the socket peer stands.
        0x3 => Ok(None),
        _ => Err(HeaderError::Malformed(
            "a v2 header declares an unknown address family",
        )),
    }
}

pin_project_lite::pin_project! {
    /// A stream that replays a buffered prefix before reading the socket.
    ///
    /// [`accept`] cannot know where the header ends until it has read past it,
    /// so the bytes it read too far are put back in front of the stream here
    /// rather than being dropped. Once the prefix is drained the buffer is
    /// released and every read goes straight to the inner stream, which matters
    /// because it is held for the whole life of the connection — a per-connection
    /// buffer that outlived its purpose would be a permanent tax on an idle
    /// keep-alive connection.
    #[derive(Debug)]
    pub struct Prefixed<S> {
        prefix: Bytes,
        #[pin]
        inner: S,
    }
}

impl<S> Prefixed<S> {
    /// Wraps `inner`, yielding `prefix` first.
    pub fn new(inner: S, prefix: Bytes) -> Self {
        Prefixed { prefix, inner }
    }

    /// The bytes still waiting to be replayed.
    pub fn buffered(&self) -> usize {
        self.prefix.len()
    }
}

impl<S: AsyncRead> AsyncRead for Prefixed<S> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.project();
        if !this.prefix.is_empty() {
            let take = this.prefix.len().min(buf.remaining());
            if let Some(head) = this.prefix.get(..take) {
                buf.put_slice(head);
            }
            this.prefix.advance(take);
            if this.prefix.is_empty() {
                // Drops the allocation. `advance` only moves the cursor, so
                // without this the buffer would be held until the connection
                // closed.
                *this.prefix = Bytes::new();
            }
            return Poll::Ready(Ok(()));
        }
        this.inner.poll_read(cx, buf)
    }
}

impl<S: AsyncWrite> AsyncWrite for Prefixed<S> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.project().inner.poll_write(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.project().inner.poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.project().inner.poll_shutdown(cx)
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        self.project().inner.poll_write_vectored(cx, bufs)
    }

    fn is_write_vectored(&self) -> bool {
        self.inner.is_write_vectored()
    }
}

/// Reads one PROXY header off `stream`, giving back the client it names and the
/// stream positioned at the first application byte.
///
/// The header is required: a stream whose first bytes are not one is an error,
/// and the caller is expected to close the connection. See the module docs for
/// why that is the only safe answer.
///
/// `timeout` bounds the whole read, not each `read` call. Without it a sender
/// that opens a connection and dribbles one byte a minute holds a task, a
/// socket, and a slot in the connection gauge indefinitely, which is a cheap
/// way to exhaust a data plane.
pub async fn accept<S>(
    mut stream: S,
    timeout: Duration,
) -> Result<(Prefixed<S>, Option<SocketAddr>), AcceptError>
where
    S: AsyncRead + Unpin,
{
    let outcome = tokio::time::timeout(timeout, read_header(&mut stream)).await;
    let (client, leftover) = match outcome {
        Ok(result) => result?,
        Err(_) => return Err(AcceptError::Timeout),
    };
    Ok((Prefixed::new(stream, leftover), client))
}

/// Reads until [`parse`] is satisfied, returning the address and the surplus.
///
/// The buffer needs no explicit ceiling: `parse` refuses a v1 line past 107
/// bytes and answers `Done` for a v2 header the moment its declared length has
/// arrived, so the loop can only run while fewer than [`MAX_HEADER_LEN`] bytes
/// are held.
async fn read_header<S>(stream: &mut S) -> Result<(Option<SocketAddr>, Bytes), AcceptError>
where
    S: AsyncRead + Unpin,
{
    let mut buffer = Vec::with_capacity(READ_CHUNK);
    loop {
        if let Parsed::Done { consumed, client } = parse(&buffer)? {
            // Copied rather than split off the read buffer: the surplus is
            // normally a fraction of it, and keeping the whole `READ_CHUNK`
            // allocation alive for the life of the connection to avoid one
            // memcpy of a few dozen bytes would be the wrong trade.
            let leftover = match buffer.get(consumed..) {
                Some(rest) if !rest.is_empty() => Bytes::copy_from_slice(rest),
                _ => Bytes::new(),
            };
            return Ok((client, leftover));
        }
        if stream
            .read_buf(&mut buffer)
            .await
            .map_err(AcceptError::Io)?
            == 0
        {
            return Err(AcceptError::Io(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "the connection closed before a complete PROXY header",
            )));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A complete v2 header: preamble, then `body`.
    fn v2(version_command: u8, family_protocol: u8, body: &[u8]) -> Vec<u8> {
        let mut header = Vec::from(V2_SIGNATURE);
        header.push(version_command);
        header.push(family_protocol);
        let length = u16::try_from(body.len()).expect("a test body under 64 KiB");
        header.extend_from_slice(&length.to_be_bytes());
        header.extend_from_slice(body);
        header
    }

    /// The 12-byte `AF_INET` address block for one source and destination.
    fn inet_block(source: [u8; 4], source_port: u16) -> Vec<u8> {
        let mut block = Vec::from(source);
        block.extend_from_slice(&[10, 0, 0, 1]);
        block.extend_from_slice(&source_port.to_be_bytes());
        block.extend_from_slice(&443u16.to_be_bytes());
        block
    }

    /// The 36-byte `AF_INET6` address block.
    fn inet6_block(source: Ipv6Addr, source_port: u16) -> Vec<u8> {
        let mut block = Vec::from(source.octets());
        block.extend_from_slice(&Ipv6Addr::LOCALHOST.octets());
        block.extend_from_slice(&source_port.to_be_bytes());
        block.extend_from_slice(&443u16.to_be_bytes());
        block
    }

    fn done(input: &[u8]) -> (usize, Option<SocketAddr>) {
        match parse(input).expect("a valid header") {
            Parsed::Done { consumed, client } => (consumed, client),
            Parsed::Incomplete => panic!("expected a complete header"),
        }
    }

    fn client_of(input: &[u8]) -> SocketAddr {
        done(input).1.expect("an address")
    }

    // -----------------------------------------------------------------------
    // v1
    // -----------------------------------------------------------------------

    #[test]
    fn v1_tcp4_names_the_source_address() {
        let header = b"PROXY TCP4 192.0.2.10 10.0.0.1 56324 443\r\n";
        let (consumed, client) = done(header);
        assert_eq!(consumed, header.len(), "the header ends at the CRLF");
        assert_eq!(
            client,
            Some("192.0.2.10:56324".parse().expect("literal")),
            "the source, not the destination, is the client"
        );
    }

    #[test]
    fn v1_tcp6_names_the_source_address() {
        let header = b"PROXY TCP6 2001:db8::1 2001:db8::2 56324 443\r\n";
        assert_eq!(
            client_of(header),
            "[2001:db8::1]:56324".parse().expect("literal")
        );
    }

    #[test]
    fn v1_unknown_is_valid_and_carries_no_address() {
        // The header is still consumed: the connection's first application byte
        // is after it, and treating UNKNOWN as an error would break a load
        // balancer's own health checks.
        let header = b"PROXY UNKNOWN\r\n";
        assert_eq!(done(header), (header.len(), None));
    }

    #[test]
    fn v1_unknown_ignores_whatever_follows_the_token() {
        // The specification allows a sender to fill in the addresses it does
        // know after UNKNOWN, and requires the receiver to ignore them.
        let header = b"PROXY UNKNOWN ffff::1 ffff::2 65535 65535\r\n";
        assert_eq!(done(header), (header.len(), None));
    }

    #[test]
    fn v1_stops_at_the_crlf_and_reports_the_rest_as_surplus() {
        let mut input = Vec::from(*b"PROXY TCP4 192.0.2.10 10.0.0.1 56324 443\r\n");
        let header_len = input.len();
        input.extend_from_slice(b"GET / HTTP/1.1\r\nHost: example.com\r\n\r\n");
        assert_eq!(done(&input).0, header_len, "the request must not be eaten");
    }

    #[test]
    fn v1_rejects_a_family_it_does_not_know() {
        assert!(matches!(
            parse(b"PROXY TCP5 192.0.2.10 10.0.0.1 56324 443\r\n"),
            Err(HeaderError::Malformed(_))
        ));
    }

    #[test]
    fn v1_rejects_an_address_of_the_wrong_family() {
        // A TCP4 line naming an IPv6 address is the shape a sloppy sender
        // produces, and taking it would mean guessing what it meant.
        assert!(matches!(
            parse(b"PROXY TCP4 2001:db8::1 10.0.0.1 56324 443\r\n"),
            Err(HeaderError::Malformed(_))
        ));
        assert!(matches!(
            parse(b"PROXY TCP6 192.0.2.10 2001:db8::2 56324 443\r\n"),
            Err(HeaderError::Malformed(_))
        ));
    }

    #[test]
    fn v1_rejects_a_bad_port() {
        for line in [
            &b"PROXY TCP4 192.0.2.10 10.0.0.1 65536 443\r\n"[..],
            &b"PROXY TCP4 192.0.2.10 10.0.0.1 -1 443\r\n"[..],
            &b"PROXY TCP4 192.0.2.10 10.0.0.1  443\r\n"[..],
            &b"PROXY TCP4 192.0.2.10 10.0.0.1 http 443\r\n"[..],
        ] {
            assert!(
                matches!(parse(line), Err(HeaderError::Malformed(_))),
                "{}",
                String::from_utf8_lossy(line)
            );
        }
    }

    #[test]
    fn v1_rejects_missing_and_surplus_fields() {
        assert!(matches!(
            parse(b"PROXY TCP4 192.0.2.10 10.0.0.1 56324\r\n"),
            Err(HeaderError::Malformed(_))
        ));
        assert!(matches!(
            parse(b"PROXY TCP4 192.0.2.10 10.0.0.1 56324 443 extra\r\n"),
            Err(HeaderError::Malformed(_))
        ));
    }

    #[test]
    fn v1_rejects_a_bare_cr() {
        assert!(matches!(
            parse(b"PROXY TCP4 192.0.2.10 10.0.0.1 56324 443\rX"),
            Err(HeaderError::Malformed(_))
        ));
    }

    #[test]
    fn v1_refuses_a_line_that_never_ends() {
        // 107 bytes is the specification's ceiling, and a sender past it is
        // either broken or trying to make the receiver buffer without bound.
        let flood = {
            let mut input = Vec::from(*b"PROXY ");
            input.extend(std::iter::repeat_n(b'A', 200));
            input
        };
        assert!(matches!(
            parse(&flood),
            Err(HeaderError::Malformed(_))
        ));

        // One byte short of the ceiling it is still merely incomplete.
        let mut nearly = Vec::from(*b"PROXY ");
        nearly.extend(std::iter::repeat_n(b'A', V1_MAX_LEN - 2 - nearly.len()));
        assert_eq!(nearly.len(), V1_MAX_LEN - 2);
        assert_eq!(parse(&nearly), Ok(Parsed::Incomplete));
    }

    #[test]
    fn the_longest_legal_v1_line_is_accepted() {
        // 107 is the specification's own worst case, and it is the UNKNOWN form
        // that reaches it: the token is longer than `TCP6`, and a sender is
        // allowed to fill in the addresses it does know after it. A parser that
        // refuses this line has its boundary off by one and would drop
        // connections a conforming sender is entitled to make.
        let header = b"PROXY UNKNOWN ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff 65535 65535\r\n";
        assert_eq!(header.len(), V1_MAX_LEN);
        assert_eq!(done(header), (V1_MAX_LEN, None));

        // The widest line that actually names a client is the TCP6 one, three
        // bytes shorter.
        let addressed = b"PROXY TCP6 ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff 65535 65535\r\n";
        assert_eq!(addressed.len(), V1_MAX_LEN - 3);
        assert_eq!(
            client_of(addressed),
            "[ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff]:65535"
                .parse()
                .expect("literal")
        );
    }

    // -----------------------------------------------------------------------
    // v2
    // -----------------------------------------------------------------------

    #[test]
    fn v2_inet_names_the_source_address() {
        let header = v2(0x21, 0x11, &inet_block([192, 0, 2, 10], 56324));
        let (consumed, client) = done(&header);
        assert_eq!(consumed, header.len());
        assert_eq!(client, Some("192.0.2.10:56324".parse().expect("literal")));
    }

    #[test]
    fn v2_inet6_names_the_source_address() {
        let source: Ipv6Addr = "2001:db8::1".parse().expect("literal");
        let header = v2(0x21, 0x21, &inet6_block(source, 56324));
        assert_eq!(
            client_of(&header),
            "[2001:db8::1]:56324".parse().expect("literal")
        );
    }

    #[test]
    fn v2_skips_tlvs_after_the_address_block() {
        // This is the AWS shape: an INET block followed by a type-0xEA TLV
        // carrying the VPC endpoint id. Nothing routes on it, and a parser that
        // choked on an unknown TLV would fail behind an NLB.
        let mut body = inet_block([198, 51, 100, 7], 40001);
        body.push(0xEA);
        body.extend_from_slice(&7u16.to_be_bytes());
        body.extend_from_slice(b"vpce-01");
        let header = v2(0x21, 0x11, &body);

        let (consumed, client) = done(&header);
        assert_eq!(consumed, header.len(), "the TLV is part of the header");
        assert_eq!(client, Some("198.51.100.7:40001".parse().expect("literal")));
    }

    #[test]
    fn v2_local_keeps_the_socket_peer() {
        // A load balancer health-checking the listener sends LOCAL: the
        // connection really is its own, so there is no client to name.
        let header = v2(0x20, 0x11, &inet_block([192, 0, 2, 10], 56324));
        assert_eq!(done(&header), (header.len(), None));
    }

    #[test]
    fn v2_local_with_an_empty_body_is_valid() {
        let header = v2(0x20, 0x00, &[]);
        assert_eq!(done(&header), (V2_PREAMBLE_LEN, None));
    }

    #[test]
    fn v2_unspec_and_unix_keep_the_socket_peer() {
        for family in [0x01u8, 0x31] {
            let header = v2(0x21, family, &[0u8; 8]);
            assert_eq!(
                done(&header),
                (header.len(), None),
                "family {family:#04x} must be consumed, not rejected"
            );
        }
    }

    #[test]
    fn v2_rejects_a_version_other_than_two() {
        let header = v2(0x31, 0x11, &inet_block([192, 0, 2, 10], 1));
        assert!(matches!(parse(&header), Err(HeaderError::Malformed(_))));
    }

    #[test]
    fn v2_rejects_a_command_that_is_neither_local_nor_proxy() {
        let header = v2(0x27, 0x11, &inet_block([192, 0, 2, 10], 1));
        assert!(matches!(parse(&header), Err(HeaderError::Malformed(_))));
    }

    #[test]
    fn v2_rejects_an_unknown_address_family() {
        let header = v2(0x21, 0x71, &[0u8; 12]);
        assert!(matches!(parse(&header), Err(HeaderError::Malformed(_))));
    }

    #[test]
    fn v2_rejects_an_address_block_shorter_than_its_family() {
        // The length field says the header is complete, and the family says the
        // block should be 12 or 36 bytes. Both cannot be true.
        let short_inet = v2(0x21, 0x11, &[0u8; 11]);
        assert!(matches!(parse(&short_inet), Err(HeaderError::Malformed(_))));

        let short_inet6 = v2(0x21, 0x21, &[0u8; 35]);
        assert!(matches!(parse(&short_inet6), Err(HeaderError::Malformed(_))));
    }

    #[test]
    fn v2_waits_for_a_length_that_exceeds_the_buffer() {
        // A declared length longer than what has arrived is incomplete, never
        // an out-of-bounds read and never an error in its own right.
        let mut header = Vec::from(V2_SIGNATURE);
        header.extend_from_slice(&[0x21, 0x11]);
        header.extend_from_slice(&u16::MAX.to_be_bytes());
        header.extend_from_slice(&[0u8; 12]);
        assert_eq!(parse(&header), Ok(Parsed::Incomplete));
    }

    #[test]
    fn the_widest_v2_header_fits_the_documented_bound() {
        let header = v2(0x21, 0x11, &vec![0u8; usize::from(u16::MAX)]);
        assert_eq!(header.len(), MAX_HEADER_LEN);
        assert_eq!(done(&header).0, MAX_HEADER_LEN);
    }

    // -----------------------------------------------------------------------
    // Framing
    // -----------------------------------------------------------------------

    #[test]
    fn garbage_is_refused_on_the_first_byte_that_proves_it() {
        // The point is that this does not wait for a CRLF that will never come:
        // an HTTP request to a PROXY-protocol listener is refused immediately.
        assert_eq!(parse(b"GET / HTTP/1.1\r\n"), Err(HeaderError::Signature));
        assert_eq!(parse(b"G"), Err(HeaderError::Signature));
        // TLS: a ClientHello starts with the handshake content type, 0x16.
        assert_eq!(parse(&[0x16, 0x03, 0x01, 0x00]), Err(HeaderError::Signature));
        // `POST` shares its first byte with `PROXY` and diverges at the second.
        assert_eq!(parse(b"POST / HTTP/1.1\r\n"), Err(HeaderError::Signature));
    }

    #[test]
    fn nothing_at_all_is_incomplete_rather_than_wrong() {
        assert_eq!(parse(b""), Ok(Parsed::Incomplete));
    }

    #[test]
    fn every_truncation_of_every_header_is_incomplete_and_never_wrong() {
        // The property that makes the reader loop safe: a header arriving one
        // byte at a time must never be mistaken for an error, and must never be
        // reported complete before its last byte. Every prefix of every shape
        // this parser accepts is checked, which is the whole state space that
        // splitting a read can produce.
        let source: Ipv6Addr = "2001:db8::1".parse().expect("literal");
        let mut tlv_body = inet_block([198, 51, 100, 7], 40001);
        tlv_body.push(0xEA);
        tlv_body.extend_from_slice(&7u16.to_be_bytes());
        tlv_body.extend_from_slice(b"vpce-01");

        let headers: Vec<Vec<u8>> = vec![
            Vec::from(*b"PROXY TCP4 192.0.2.10 10.0.0.1 56324 443\r\n"),
            Vec::from(*b"PROXY TCP6 2001:db8::1 2001:db8::2 56324 443\r\n"),
            Vec::from(*b"PROXY UNKNOWN\r\n"),
            v2(0x21, 0x11, &inet_block([192, 0, 2, 10], 56324)),
            v2(0x21, 0x21, &inet6_block(source, 56324)),
            v2(0x21, 0x11, &tlv_body),
            v2(0x20, 0x00, &[]),
        ];

        for header in headers {
            for split in 0..header.len() {
                let partial = header.get(..split).expect("a prefix");
                assert_eq!(
                    parse(partial),
                    Ok(Parsed::Incomplete),
                    "{split} of {} bytes must be incomplete",
                    header.len()
                );
            }
            assert!(
                matches!(parse(&header), Ok(Parsed::Done { .. })),
                "the whole header must parse"
            );
        }
    }

    #[test]
    fn a_header_stays_complete_however_much_follows_it() {
        // Feeding more bytes after a complete header must not change the
        // answer, which is what lets the reader stop at the first `Done`.
        let header = v2(0x21, 0x11, &inet_block([192, 0, 2, 10], 56324));
        let expected = done(&header);
        for extra in [0usize, 1, 64, 4096] {
            let mut input = header.clone();
            input.extend(std::iter::repeat_n(0x16, extra));
            assert_eq!(done(&input), expected, "{extra} trailing bytes");
        }
    }

    // -----------------------------------------------------------------------
    // The reader
    // -----------------------------------------------------------------------

    /// Feeds `chunks` to [`accept`], one per read, with a pause between them.
    async fn accept_chunks(chunks: &[&[u8]]) -> Result<(Option<SocketAddr>, Vec<u8>), AcceptError> {
        let (mut client, server) = tokio::io::duplex(4096);
        let owned: Vec<Vec<u8>> = chunks.iter().map(|chunk| chunk.to_vec()).collect();
        tokio::spawn(async move {
            use tokio::io::AsyncWriteExt as _;
            for chunk in owned {
                if client.write_all(&chunk).await.is_err() {
                    return;
                }
                client.flush().await.ok();
                tokio::task::yield_now().await;
            }
            // Held open so the reader sees a stall rather than an EOF.
            tokio::time::sleep(Duration::from_secs(30)).await;
        });

        let (mut stream, address) = accept(server, Duration::from_secs(5)).await?;
        let mut rest = Vec::new();
        let mut chunk = [0u8; 256];
        // One read is enough: everything the writer sent is already buffered.
        if let Ok(read) = tokio::time::timeout(
            Duration::from_millis(200),
            tokio::io::AsyncReadExt::read(&mut stream, &mut chunk),
        )
        .await
        {
            let read = read.expect("a read");
            rest.extend_from_slice(chunk.get(..read).unwrap_or_default());
        }
        Ok((address, rest))
    }

    #[tokio::test]
    async fn a_header_split_across_reads_is_reassembled() {
        let (client, rest) = accept_chunks(&[
            b"PROXY TC",
            b"P4 192.0.2.10 10.0.0.1 5",
            b"6324 443\r\nGET / HTTP/1.1\r\n",
        ])
        .await
        .expect("a valid header");

        assert_eq!(client, Some("192.0.2.10:56324".parse().expect("literal")));
        assert_eq!(rest, b"GET / HTTP/1.1\r\n", "the request survives intact");
    }

    #[tokio::test]
    async fn a_header_arriving_one_byte_at_a_time_is_reassembled() {
        let header = v2(0x21, 0x11, &inet_block([203, 0, 113, 5], 12345));
        let chunks: Vec<&[u8]> = header
            .iter()
            .map(std::slice::from_ref)
            .collect();
        let (client, _) = accept_chunks(&chunks).await.expect("a valid header");
        assert_eq!(client, Some("203.0.113.5:12345".parse().expect("literal")));
    }

    #[tokio::test]
    async fn bytes_read_past_the_header_are_handed_back() {
        let (_, rest) = accept_chunks(&[
            b"PROXY TCP4 192.0.2.10 10.0.0.1 56324 443\r\nhello, world",
        ])
        .await
        .expect("a valid header");
        assert_eq!(rest, b"hello, world");
    }

    #[tokio::test]
    async fn garbage_is_an_error_rather_than_a_wait() {
        let error = accept_chunks(&[b"GET / HTTP/1.1\r\n"])
            .await
            .expect_err("refused");
        assert!(matches!(error, AcceptError::Header(HeaderError::Signature)));
    }

    #[tokio::test]
    async fn a_closed_connection_is_not_a_valid_header() {
        let (client, server) = tokio::io::duplex(64);
        drop(client);
        let error = accept(server, Duration::from_secs(5))
            .await
            .expect_err("refused");
        assert!(matches!(error, AcceptError::Io(_)), "{error}");
    }

    #[tokio::test(start_paused = true)]
    async fn a_stalled_sender_hits_the_timeout() {
        // The half-open case: a valid prefix and then nothing, which without a
        // deadline holds a task and a socket for as long as the sender likes.
        let (mut client, server) = tokio::io::duplex(64);
        tokio::spawn(async move {
            use tokio::io::AsyncWriteExt as _;
            let _ = client.write_all(b"PROXY TCP4 192.0.2.10").await;
            tokio::time::sleep(Duration::from_secs(600)).await;
        });

        let error = accept(server, Duration::from_secs(5))
            .await
            .expect_err("refused");
        assert!(matches!(error, AcceptError::Timeout), "{error}");
    }

    #[tokio::test]
    async fn the_prefix_is_released_once_it_has_been_read() {
        // The prefix is held for the life of the connection, so it has to stop
        // costing anything the moment it is drained.
        let stream = tokio::io::empty();
        let mut prefixed = Prefixed::new(stream, Bytes::from_static(b"abcd"));
        assert_eq!(prefixed.buffered(), 4);

        let mut chunk = [0u8; 2];
        let read = tokio::io::AsyncReadExt::read(&mut prefixed, &mut chunk)
            .await
            .expect("a read");
        assert_eq!(read, 2);
        assert_eq!(&chunk, b"ab");
        assert_eq!(prefixed.buffered(), 2);

        let mut rest = Vec::new();
        tokio::io::AsyncReadExt::read_to_end(&mut prefixed, &mut rest)
            .await
            .expect("the rest");
        assert_eq!(rest, b"cd");
        assert_eq!(prefixed.buffered(), 0);
    }
}
