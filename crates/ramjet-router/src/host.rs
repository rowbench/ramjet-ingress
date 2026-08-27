//! Host normalization and the hasher behind the host maps.
//!
//! A `Host` header arrives in whatever shape the client felt like sending:
//! `Example.COM`, `example.com:8443`, `example.com.`, `[2001:db8::1]:8080`.
//! DNS names are case-insensitive and the port is not part of the name, so all
//! of those have to collapse onto one lookup key before we touch a map.
//!
//! Doing that with `to_lowercase()` would allocate a `String` on every request,
//! which is exactly what the hot path is not allowed to do. Instead [`scan`]
//! makes one pass over the header and reports whether the value is *already*
//! canonical. The overwhelmingly common case — lowercase, no port, no root dot
//! — yields a borrowed subslice and copies nothing at all. Only a header that
//! actually contains uppercase falls through to [`fold_lower`], which writes
//! into a caller-owned stack buffer. No heap traffic on either path.

use std::collections::{HashMap, HashSet};
use std::hash::{BuildHasherDefault, Hasher};

/// Longest DNS name we will accept, per RFC 1035: 253 bytes of presentation
/// form. Anything longer cannot be a real name, so it cannot be in the table,
/// so rejecting it early saves a hash.
pub(crate) const MAX_HOST_LEN: usize = 253;

/// What one pass over a `Host` header found.
pub(crate) enum Scan<'a> {
    /// Canonical already: usable as a lookup key with no copy.
    Clean(&'a str),
    /// Port and root dot trimmed, but the name contains uppercase and still
    /// needs a case-folded copy before it can be looked up.
    Fold(&'a str),
    /// Empty, over-long, or a bracketed literal that never closes.
    Invalid,
}

/// Trims the port and any trailing root dot, and reports whether case folding
/// is still required.
///
/// The returned slice always borrows from `host`; this function never copies.
pub(crate) fn scan(host: &str) -> Scan<'_> {
    let bytes = host.as_bytes();
    let mut end = bytes.len();
    let mut upper = false;

    if bytes.first() == Some(&b'[') {
        // IPv6 literal. The brackets are part of the key: `[::1]` and `::1`
        // are not spellings we need to unify, because an Ingress `host` field
        // cannot hold either. Keep them and drop whatever follows `]`.
        match bytes.iter().position(|&b| b == b']') {
            Some(close) => end = close + 1,
            None => return Scan::Invalid,
        }
        upper = bytes.iter().take(end).any(u8::is_ascii_uppercase);
    } else {
        // First colon wins: a well-formed non-bracketed authority has at most
        // one, and a malformed one is not going to match the table regardless.
        for (i, &b) in bytes.iter().enumerate() {
            if b == b':' {
                end = i;
                break;
            }
            upper |= b.is_ascii_uppercase();
        }
    }

    // `example.com.` is the fully qualified spelling of `example.com`.
    if end > 1 && bytes.get(end - 1) == Some(&b'.') {
        end -= 1;
    }

    if end == 0 || end > MAX_HOST_LEN {
        return Scan::Invalid;
    }

    // Every cut above lands on an ASCII byte, so this is always a char
    // boundary; `get` rather than indexing keeps the failure non-panicking.
    match host.get(..end) {
        Some(trimmed) if upper => Scan::Fold(trimmed),
        Some(trimmed) => Scan::Clean(trimmed),
        None => Scan::Invalid,
    }
}

/// Writes the ASCII-lowercase form of `host` into `buf` and returns it.
///
/// Cold path: only reached when [`scan`] saw an uppercase byte. `buf` lives on
/// the caller's stack, so this allocates nothing.
pub(crate) fn fold_lower<'b>(host: &str, buf: &'b mut [u8; MAX_HOST_LEN]) -> Option<&'b str> {
    let bytes = host.as_bytes();
    let dst = buf.get_mut(..bytes.len())?;
    for (d, s) in dst.iter_mut().zip(bytes) {
        *d = s.to_ascii_lowercase();
    }
    // `to_ascii_lowercase` leaves non-ASCII bytes untouched, so a valid UTF-8
    // input stays valid; the check is a formality the optimizer handles well.
    let frozen: &'b [u8] = dst;
    std::str::from_utf8(frozen).ok()
}

/// Splits `host` into its first label and the parent domain, for wildcard
/// lookups.
///
/// Kubernetes wildcards replace exactly one left-most label, so
/// `*.example.com` matches `foo.example.com` and neither `example.com` nor
/// `foo.bar.example.com`. Cutting one label off the query and looking the
/// remainder up in a flat map reproduces that rule in a single hash, with no
/// suffix trie and no backtracking.
#[inline]
pub(crate) fn parent_domain(host: &str) -> Option<&str> {
    let (label, parent) = host.split_once('.')?;
    // A wildcard must stand in for a real label, so an empty one (`.foo.com`)
    // matches nothing.
    if label.is_empty() || parent.is_empty() {
        return None;
    }
    Some(parent)
}

/// FxHash: the hash rustc uses for its own interning tables.
///
/// Why not SipHash (the `std` default)? SipHash-1-3 runs around a nanosecond
/// per byte; on a 25-byte host name that is roughly a fifth of the entire
/// per-request matching budget, spent before the first bucket is probed.
///
/// The usual reason to pay that is hash-flooding resistance, and it does not
/// apply here. Flooding requires the attacker to *insert* colliding keys so
/// that some bucket grows a long chain. Every key in these maps comes from an
/// Ingress object that the API server accepted; a client can choose what it
/// looks *up*, but not what is stored. The most a crafted `Host` header can
/// buy is a control-byte collision inside one SwissTable group, costing a
/// single extra 64-bit comparison before the lookup misses. That is not a
/// denial of service, and it is a cheap trade for the per-request nanoseconds.
#[derive(Default, Clone, Copy)]
pub(crate) struct FxHasher {
    hash: u64,
}

/// Fractional golden ratio, 64-bit — the multiplier rustc's `FxHasher` uses.
const SEED: u64 = 0x51_7c_c1_b7_27_22_0a_95;

impl FxHasher {
    #[inline]
    fn add(&mut self, word: u64) {
        self.hash = (self.hash.rotate_left(5) ^ word).wrapping_mul(SEED);
    }
}

impl Hasher for FxHasher {
    #[inline]
    fn write(&mut self, bytes: &[u8]) {
        let mut chunks = bytes.chunks_exact(8);
        for chunk in &mut chunks {
            // `chunks_exact` guarantees the length; the fallible conversion
            // just avoids an `unwrap` in a library path.
            if let Ok(word) = <[u8; 8]>::try_from(chunk) {
                self.add(u64::from_ne_bytes(word));
            }
        }
        let mut tail = 0u64;
        for &b in chunks.remainder() {
            tail = (tail << 8) | u64::from(b);
        }
        self.add(tail);
    }

    #[inline]
    fn write_u8(&mut self, n: u8) {
        self.add(u64::from(n));
    }

    #[inline]
    fn write_usize(&mut self, n: usize) {
        self.add(n as u64);
    }

    #[inline]
    fn finish(&self) -> u64 {
        self.hash
    }
}

/// A `HashMap` keyed with [`FxHasher`].
pub(crate) type FxHashMap<K, V> = HashMap<K, V, BuildHasherDefault<FxHasher>>;

/// A `HashSet` keyed with [`FxHasher`]. Build-time only.
pub(crate) type FxHashSet<T> = HashSet<T, BuildHasherDefault<FxHasher>>;

#[cfg(test)]
mod tests {
    use super::*;

    fn norm(host: &str) -> Option<String> {
        match scan(host) {
            Scan::Clean(h) => Some(h.to_owned()),
            Scan::Fold(h) => {
                let mut buf = [0u8; MAX_HOST_LEN];
                fold_lower(h, &mut buf).map(str::to_owned)
            }
            Scan::Invalid => None,
        }
    }

    #[test]
    fn already_canonical_is_borrowed() {
        assert!(matches!(scan("example.com"), Scan::Clean("example.com")));
        // Trimming a port still borrows -- no copy is needed to drop a suffix.
        assert!(matches!(scan("example.com:8443"), Scan::Clean("example.com")));
    }

    #[test]
    fn uppercase_requires_folding() {
        assert!(matches!(scan("Example.COM"), Scan::Fold("Example.COM")));
        assert_eq!(norm("Example.COM").as_deref(), Some("example.com"));
        assert_eq!(norm("EXAMPLE.com:443").as_deref(), Some("example.com"));
    }

    #[test]
    fn root_dot_is_the_same_name() {
        assert_eq!(norm("example.com.").as_deref(), Some("example.com"));
        assert_eq!(norm("example.com.:80").as_deref(), Some("example.com"));
        // A bare "." is not a name we route on, but it must not underflow.
        assert_eq!(norm(".").as_deref(), Some("."));
    }

    #[test]
    fn ipv6_literal_keeps_brackets_drops_port() {
        assert_eq!(norm("[2001:db8::1]:8080").as_deref(), Some("[2001:db8::1]"));
        assert_eq!(norm("[2001:DB8::1]").as_deref(), Some("[2001:db8::1]"));
        assert_eq!(norm("[2001:db8::1"), None);
    }

    #[test]
    fn rejects_empty_and_overlong() {
        assert_eq!(norm(""), None);
        assert_eq!(norm(":8080"), None);
        assert_eq!(norm(&"a".repeat(MAX_HOST_LEN + 1)), None);
        assert!(norm(&"a".repeat(MAX_HOST_LEN)).is_some());
    }

    #[test]
    fn parent_domain_strips_exactly_one_label() {
        assert_eq!(parent_domain("foo.example.com"), Some("example.com"));
        assert_eq!(parent_domain("foo.bar.example.com"), Some("bar.example.com"));
        assert_eq!(parent_domain("localhost"), None);
        assert_eq!(parent_domain(".example.com"), None);
        assert_eq!(parent_domain("foo."), None);
    }

    #[test]
    fn hasher_distinguishes_similar_names() {
        use std::hash::Hash;

        let h = |s: &str| {
            let mut hasher = FxHasher::default();
            s.hash(&mut hasher);
            hasher.finish()
        };
        assert_ne!(h("api.example.com"), h("app.example.com"));
        assert_ne!(h("example.com"), h("example.co"));
        assert_eq!(h("example.com"), h("example.com"));
    }
}
