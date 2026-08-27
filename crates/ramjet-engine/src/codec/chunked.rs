//! Finding the end of a chunked body without rewriting it.
//!
//! A proxy relaying a chunked body has two options: decode it and re-encode it,
//! or forward the bytes verbatim and merely *find* where the body stops. This
//! is the second. It is both faster — no second copy, no re-framing — and more
//! honest, because the bytes the next hop sees are the bytes the origin sent,
//! chunk boundaries and extensions included.
//!
//! What it cannot do is skip validation. A scanner that lost track of a chunk
//! boundary would forward body bytes as though they were the start of the next
//! message, which is request smuggling with extra steps, so every byte of
//! framing is checked and anything unexpected ends the connection.

use super::CodecError;

/// Longest chunk-size line, extensions included.
///
/// A chunk size is at most 16 hex digits; the rest is extension text nobody
/// sends. Bounding it stops a peer from making one chunk header cost megabytes.
const MAX_CHUNK_LINE: usize = 1024;

/// Longest trailer section.
const MAX_TRAILER: usize = 8 * 1024;

/// Where in the chunked grammar the scanner is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum State {
    /// Reading hex digits of a chunk size.
    Size { value: u64, digits: u32 },
    /// Inside a chunk extension, skipping to the CRLF, carrying the size that
    /// was parsed before the `;`.
    Ext { size: u64 },
    /// Saw the CR of a size line, carrying the size it declared. Zero means
    /// this was the last chunk and the trailer section follows.
    SizeLf { size: u64 },
    /// Inside chunk data, this many bytes still to come.
    Data { left: u64 },
    /// Expecting the CR that closes a chunk's data.
    DataCr,
    /// Expecting the LF that closes a chunk's data.
    DataLf,
    /// At the start of a line in the trailer section.
    TrailerStart,
    /// Inside a trailer field line.
    TrailerLine,
    /// Expecting the LF that closes a trailer field.
    TrailerCr,
    /// Expecting the LF that ends the whole body.
    FinalLf,
    /// The body is complete.
    Done,
}

/// A scanner over one chunked body.
///
/// Feed it whatever bytes arrived; it reports how many of them belong to the
/// body and whether the body ended inside them. Bytes past the end belong to
/// the next message on the connection.
#[derive(Debug, Clone)]
pub struct ChunkScan {
    state: State,
    line: usize,
    trailer: usize,
}

impl Default for ChunkScan {
    fn default() -> Self {
        ChunkScan::new()
    }
}

impl ChunkScan {
    /// A scanner positioned at the first chunk size.
    pub fn new() -> Self {
        ChunkScan {
            state: State::Size {
                value: 0,
                digits: 0,
            },
            line: 0,
            trailer: 0,
        }
    }

    /// Whether the body has ended.
    pub fn is_done(&self) -> bool {
        self.state == State::Done
    }

    /// How many leading bytes of `input` belong to this body, and whether the
    /// body ends within them.
    ///
    /// Once it returns `(_, true)` the scanner is finished and must not be fed
    /// again; the caller slices `input` at the returned length and treats the
    /// remainder as the next message.
    pub fn scan(&mut self, input: &[u8]) -> Result<(usize, bool), CodecError> {
        let mut i = 0;
        while i < input.len() {
            match self.state {
                State::Done => return Ok((i, true)),

                // Bulk path: chunk data is forwarded untouched, so there is
                // nothing to look at byte by byte. This is where a large body
                // spends all of its time.
                State::Data { left } => {
                    let take = left.min((input.len() - i) as u64) as usize;
                    i += take;
                    let left = left - take as u64;
                    self.state = if left == 0 {
                        State::DataCr
                    } else {
                        State::Data { left }
                    };
                }

                _ => {
                    self.step(input[i])?;
                    i += 1;
                }
            }
        }
        Ok((i, self.state == State::Done))
    }

    fn step(&mut self, b: u8) -> Result<(), CodecError> {
        self.state = match self.state {
            State::Size { value, digits } => {
                self.line += 1;
                if self.line > MAX_CHUNK_LINE {
                    return Err(CodecError::Malformed("chunk size line too long"));
                }
                match b {
                    b'\r' if digits > 0 => State::SizeLf { size: value },
                    b';' if digits > 0 => State::Ext { size: value },
                    _ => {
                        let Some(d) = hex_value(b) else {
                            return Err(CodecError::Malformed("chunk size is not hexadecimal"));
                        };
                        // Sixteen digits is the whole of u64; a seventeenth is
                        // either an overflow attempt or a broken client.
                        if digits >= 16 {
                            return Err(CodecError::Malformed("chunk size too large"));
                        }
                        State::Size {
                            value: value * 16 + u64::from(d),
                            digits: digits + 1,
                        }
                    }
                }
            }

            State::Ext { size } => {
                self.line += 1;
                if self.line > MAX_CHUNK_LINE {
                    return Err(CodecError::Malformed("chunk size line too long"));
                }
                match b {
                    b'\r' => State::SizeLf { size },
                    // A bare LF inside a chunk header is exactly the
                    // disagreement between two parsers that smuggling needs.
                    b'\n' => return Err(CodecError::Malformed("bare LF in a chunk extension")),
                    _ => State::Ext { size },
                }
            }

            // The size is carried through the extension rather than re-read,
            // because `5;name=value` puts arbitrary text between the digits and
            // the CRLF that ends the line.
            State::SizeLf { size } => {
                if b != b'\n' {
                    return Err(CodecError::Malformed("chunk size line not closed by CRLF"));
                }
                self.line = 0;
                if size == 0 {
                    State::TrailerStart
                } else {
                    State::Data { left: size }
                }
            }

            State::Data { .. } => unreachable!("bulk-consumed in scan"),

            State::DataCr => {
                if b != b'\r' {
                    return Err(CodecError::Malformed("chunk data not closed by CRLF"));
                }
                State::DataLf
            }

            State::DataLf => {
                if b != b'\n' {
                    return Err(CodecError::Malformed("chunk data not closed by CRLF"));
                }
                State::Size {
                    value: 0,
                    digits: 0,
                }
            }

            State::TrailerStart => {
                self.trailer += 1;
                if self.trailer > MAX_TRAILER {
                    return Err(CodecError::Malformed("trailer section too long"));
                }
                match b {
                    b'\r' => State::FinalLf,
                    b'\n' => return Err(CodecError::Malformed("bare LF in a trailer")),
                    _ => State::TrailerLine,
                }
            }

            State::TrailerLine => {
                self.trailer += 1;
                if self.trailer > MAX_TRAILER {
                    return Err(CodecError::Malformed("trailer section too long"));
                }
                match b {
                    b'\r' => State::TrailerCr,
                    b'\n' => return Err(CodecError::Malformed("bare LF in a trailer")),
                    _ => State::TrailerLine,
                }
            }

            State::TrailerCr => {
                if b != b'\n' {
                    return Err(CodecError::Malformed("trailer line not closed by CRLF"));
                }
                State::TrailerStart
            }

            State::FinalLf => {
                if b != b'\n' {
                    return Err(CodecError::Malformed("chunked body not closed by CRLF"));
                }
                State::Done
            }

            State::Done => State::Done,
        };
        Ok(())
    }
}

/// The value of one hexadecimal digit, or `None`.
fn hex_value(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Feed a whole body in one call.
    fn scan_all(wire: &[u8]) -> Result<(usize, bool), CodecError> {
        ChunkScan::new().scan(wire)
    }

    #[test]
    fn a_simple_chunked_body_is_measured() {
        let wire = b"5\r\nhello\r\n0\r\n\r\n";
        assert_eq!(scan_all(wire), Ok((wire.len(), true)));
    }

    #[test]
    fn several_chunks_are_measured() {
        let wire = b"5\r\nhello\r\n6\r\n world\r\n0\r\n\r\n";
        assert_eq!(scan_all(wire), Ok((wire.len(), true)));
    }

    #[test]
    fn bytes_past_the_terminator_are_not_body() {
        let wire = b"5\r\nhello\r\n0\r\n\r\nGET / HTTP/1.1\r\n\r\n";
        let body = b"5\r\nhello\r\n0\r\n\r\n".len();
        assert_eq!(scan_all(wire), Ok((body, true)));
    }

    #[test]
    fn a_body_split_across_reads_is_reassembled() {
        let wire = b"5\r\nhello\r\n6\r\n world\r\n0\r\n\r\n";
        // Every split point must give the same total and the same verdict.
        for split in 1..wire.len() {
            let mut scan = ChunkScan::new();
            let (a, done_a) = scan.scan(&wire[..split]).expect("valid prefix");
            assert!(!done_a || split == wire.len());
            let (b, done_b) = scan.scan(&wire[a..]).expect("valid remainder");
            assert!(done_b, "split at {split} should complete");
            assert_eq!(a + b, wire.len(), "split at {split}");
        }
    }

    #[test]
    fn one_byte_at_a_time_works() {
        let wire = b"a\r\n0123456789\r\n0\r\n\r\n";
        let mut scan = ChunkScan::new();
        let mut total = 0;
        for i in 0..wire.len() {
            let (n, done) = scan.scan(&wire[i..i + 1]).expect("valid byte");
            total += n;
            assert_eq!(done, i + 1 == wire.len());
        }
        assert_eq!(total, wire.len());
    }

    #[test]
    fn a_chunk_extension_is_skipped_and_forwarded() {
        let wire = b"5;name=value\r\nhello\r\n0\r\n\r\n";
        assert_eq!(scan_all(wire), Ok((wire.len(), true)));
    }

    #[test]
    fn an_extension_on_the_last_chunk_still_ends_the_body() {
        let wire = b"0;done\r\n\r\n";
        assert_eq!(scan_all(wire), Ok((wire.len(), true)));
    }

    #[test]
    fn a_trailer_section_is_part_of_the_body() {
        let wire = b"5\r\nhello\r\n0\r\nX-Checksum: abc\r\nX-More: d\r\n\r\n";
        assert_eq!(scan_all(wire), Ok((wire.len(), true)));
    }

    #[test]
    fn an_empty_body_is_just_the_last_chunk() {
        assert_eq!(scan_all(b"0\r\n\r\n"), Ok((5, true)));
    }

    #[test]
    fn uppercase_hex_sizes_are_accepted() {
        let wire = b"1F\r\n0123456789012345678901234567890\r\n0\r\n\r\n";
        assert_eq!(scan_all(wire), Ok((wire.len(), true)));
    }

    #[test]
    fn a_size_that_is_not_hex_is_refused() {
        assert!(scan_all(b"5x\r\nhello\r\n0\r\n\r\n").is_err());
        assert!(scan_all(b"\r\n").is_err());
    }

    #[test]
    fn a_chunk_not_closed_by_crlf_is_refused() {
        assert!(scan_all(b"5\r\nhelloXX0\r\n\r\n").is_err());
        assert!(scan_all(b"5\rhello\r\n0\r\n\r\n").is_err());
    }

    #[test]
    fn a_bare_lf_is_refused() {
        assert!(scan_all(b"5;e\nhello\r\n0\r\n\r\n").is_err());
        assert!(scan_all(b"0\r\n\n").is_err());
    }

    #[test]
    fn an_absurd_chunk_size_is_refused() {
        assert!(scan_all(b"11111111111111111\r\n").is_err());
    }

    #[test]
    fn an_endless_extension_is_refused() {
        let mut wire = b"5;".to_vec();
        wire.resize(MAX_CHUNK_LINE + 10, b'x');
        assert!(scan_all(&wire).is_err());
    }

    #[test]
    fn an_endless_trailer_is_refused() {
        let mut wire = b"0\r\nX: ".to_vec();
        wire.resize(MAX_TRAILER + 20, b'x');
        assert!(scan_all(&wire).is_err());
    }

    #[test]
    fn a_body_larger_than_one_read_stays_in_the_bulk_path() {
        let payload = vec![b'z'; 100_000];
        let mut wire = format!("{:x}\r\n", payload.len()).into_bytes();
        wire.extend_from_slice(&payload);
        wire.extend_from_slice(b"\r\n0\r\n\r\n");
        let mut scan = ChunkScan::new();
        let mut consumed = 0;
        // 4 KiB at a time, as a socket would deliver it.
        while consumed < wire.len() {
            let end = (consumed + 4096).min(wire.len());
            let (n, done) = scan.scan(&wire[consumed..end]).expect("valid");
            consumed += n;
            if done {
                break;
            }
        }
        assert_eq!(consumed, wire.len());
        assert!(scan.is_done());
    }
}
