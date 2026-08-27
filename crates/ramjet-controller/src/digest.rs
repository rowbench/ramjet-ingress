//! A stable content hash for compiled configuration.
//!
//! The rebuild loop needs to answer "is this table the same as the one already
//! published?" without comparing two `RouteTable`s field by field — they hold
//! compiled regexes and `Arc`s that have no meaningful equality. So the digest
//! is taken over the *plan*: the canonical, sorted description of backends,
//! routes, and certificates, fed in the same order every time.
//!
//! FNV-1a rather than [`std::hash::DefaultHasher`] on purpose. `DefaultHasher`
//! is explicitly not stable across releases, and a digest that silently changes
//! meaning after a toolchain bump would republish the entire cluster's routing
//! table on a rebuild that changed nothing.

/// FNV-1a, 64-bit.
#[derive(Debug, Clone)]
pub(crate) struct Digest(u64);

const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

impl Default for Digest {
    fn default() -> Self {
        Digest(FNV_OFFSET)
    }
}

impl Digest {
    /// A fresh digest.
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Mixes in raw bytes.
    pub(crate) fn bytes(&mut self, bytes: &[u8]) -> &mut Self {
        for b in bytes {
            self.0 ^= u64::from(*b);
            self.0 = self.0.wrapping_mul(FNV_PRIME);
        }
        self
    }

    /// Mixes in a string, length-prefixed.
    ///
    /// Without the separator, `("ab", "c")` and `("a", "bc")` would collide,
    /// which for a route key like (host, path) is not hypothetical.
    pub(crate) fn str(&mut self, s: &str) -> &mut Self {
        self.u64(s.len() as u64);
        self.bytes(s.as_bytes())
    }

    /// Mixes in an integer, little-endian.
    pub(crate) fn u64(&mut self, v: u64) -> &mut Self {
        self.bytes(&v.to_le_bytes())
    }

    /// Mixes in a discriminant or other small tag.
    pub(crate) fn u8(&mut self, v: u8) -> &mut Self {
        self.bytes(&[v])
    }

    /// Mixes in an optional string, distinguishing `None` from `Some("")`.
    pub(crate) fn opt_str(&mut self, s: Option<&str>) -> &mut Self {
        match s {
            Some(s) => {
                self.u8(1);
                self.str(s)
            }
            None => self.u8(0),
        }
    }

    /// The accumulated hash.
    pub(crate) fn finish(&self) -> u64 {
        self.0
    }
}

/// Content-derived id for a certificate.
///
/// Namespace and name are folded in alongside the material so two Secrets that
/// happen to hold identical bytes still get distinct handles and independent
/// lifecycles on the proxy side.
pub(crate) fn cert_handle_id(namespace: &str, name: &str, cert: &[u8], key: &[u8]) -> u64 {
    let mut d = Digest::new();
    d.str(namespace);
    d.str(name);
    d.u64(cert.len() as u64);
    d.bytes(cert);
    d.u64(key.len() as u64);
    d.bytes(key);
    d.finish()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matches_the_fnv1a_reference_vector() {
        // "a" and "foobar" from the FNV reference test suite. If this ever
        // fails, the digest is no longer the algorithm the docs claim.
        let mut d = Digest::new();
        d.bytes(b"a");
        assert_eq!(d.finish(), 0xaf63_dc4c_8601_ec8c);

        let mut d = Digest::new();
        d.bytes(b"foobar");
        assert_eq!(d.finish(), 0x85944171f73967e8);
    }

    #[test]
    fn length_prefix_prevents_boundary_collisions() {
        let mut a = Digest::new();
        a.str("ab").str("c");
        let mut b = Digest::new();
        b.str("a").str("bc");
        assert_ne!(a.finish(), b.finish());
    }

    #[test]
    fn none_and_empty_string_differ() {
        let mut a = Digest::new();
        a.opt_str(None);
        let mut b = Digest::new();
        b.opt_str(Some(""));
        assert_ne!(a.finish(), b.finish());
    }

    #[test]
    fn cert_id_tracks_content_not_identity_alone() {
        let same = cert_handle_id("ns", "tls", b"cert", b"key");
        assert_eq!(same, cert_handle_id("ns", "tls", b"cert", b"key"));
        assert_ne!(same, cert_handle_id("ns", "tls", b"cert2", b"key"));
        assert_ne!(same, cert_handle_id("ns", "tls", b"cert", b"key2"));
        assert_ne!(same, cert_handle_id("other", "tls", b"cert", b"key"));
    }

    /// The cert/key split must be unambiguous, or rotating a key into the cert
    /// field would look like no change at all.
    #[test]
    fn cert_id_separates_the_two_fields() {
        assert_ne!(
            cert_handle_id("ns", "tls", b"ab", b"c"),
            cert_handle_id("ns", "tls", b"a", b"bc")
        );
    }
}
