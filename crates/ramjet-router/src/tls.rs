//! SNI to certificate resolution.
//!
//! The router decides *which* certificate answers a handshake; it does not know
//! what a certificate is. [`CertifiedKeyHandle`] is deliberately opaque — the
//! proxy crate will hold the real `rustls::sign::CertifiedKey` and index it by
//! this handle's id. Keeping rustls out of this crate is what lets the whole
//! matcher be tested without a socket, a key, or a clock.

use std::sync::Arc;

use crate::host::{self, FxHashMap, Scan, MAX_HOST_LEN};

/// An opaque reference to a certificate and its private key.
///
/// Cheap to clone through the `Arc` the table stores it in, and comparable by
/// id so the controller can tell whether a rebuild actually changed anything.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct CertifiedKeyHandle {
    id: u64,
}

impl CertifiedKeyHandle {
    /// Creates a handle for the certificate the proxy knows by `id`.
    pub fn new(id: u64) -> Self {
        CertifiedKeyHandle { id }
    }

    /// The proxy-side identifier.
    pub fn id(self) -> u64 {
        self.id
    }
}

/// Certificate lookup by SNI hostname.
///
/// Resolution mirrors host routing exactly — exact name, then a single-label
/// wildcard, then the default certificate — because a handshake that picked a
/// different certificate than the request will later be routed by would be a
/// confusing way to fail.
#[derive(Debug, Default)]
pub struct SniMap {
    exact: FxHashMap<Box<str>, Arc<CertifiedKeyHandle>>,
    /// Keyed by parent domain: `*.example.com` is stored as `example.com`.
    wildcard: FxHashMap<Box<str>, Arc<CertifiedKeyHandle>>,
    default: Option<Arc<CertifiedKeyHandle>>,
}

impl SniMap {
    pub(crate) fn new(
        exact: FxHashMap<Box<str>, Arc<CertifiedKeyHandle>>,
        wildcard: FxHashMap<Box<str>, Arc<CertifiedKeyHandle>>,
        default: Option<Arc<CertifiedKeyHandle>>,
    ) -> Self {
        SniMap {
            exact,
            wildcard,
            default,
        }
    }

    /// Resolves a certificate for the name the client asked for.
    ///
    /// Allocation-free, like the request matcher: TLS 1.3 requires SNI to be
    /// lowercase already, but clients are clients, so the same normalization
    /// runs here.
    pub fn resolve(&self, server_name: &str) -> Option<&Arc<CertifiedKeyHandle>> {
        match host::scan(server_name) {
            Scan::Clean(name) => self.resolve_normalized(name),
            Scan::Fold(name) => {
                let mut buf = [0u8; MAX_HOST_LEN];
                let folded = host::fold_lower(name, &mut buf)?;
                self.resolve_normalized(folded)
            }
            // A malformed name still deserves a certificate, or the client sees
            // a handshake failure instead of the 421 it has earned.
            Scan::Invalid => self.default.as_ref(),
        }
    }

    fn resolve_normalized(&self, name: &str) -> Option<&Arc<CertifiedKeyHandle>> {
        if let Some(key) = self.exact.get(name) {
            return Some(key);
        }
        if let Some(parent) = host::parent_domain(name) {
            if let Some(key) = self.wildcard.get(parent) {
                return Some(key);
            }
        }
        self.default.as_ref()
    }

    /// Number of exact and wildcard names served.
    pub fn len(&self) -> usize {
        self.exact.len() + self.wildcard.len()
    }

    /// Are there no names at all (the default certificate aside)?
    pub fn is_empty(&self) -> bool {
        self.exact.is_empty() && self.wildcard.is_empty()
    }

    /// The certificate served when no name matches.
    pub fn default_key(&self) -> Option<&Arc<CertifiedKeyHandle>> {
        self.default.as_ref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn map() -> SniMap {
        let mut exact = FxHashMap::default();
        exact.insert(
            "api.example.com".into(),
            Arc::new(CertifiedKeyHandle::new(1)),
        );
        let mut wildcard = FxHashMap::default();
        wildcard.insert("example.com".into(), Arc::new(CertifiedKeyHandle::new(2)));
        SniMap::new(exact, wildcard, Some(Arc::new(CertifiedKeyHandle::new(3))))
    }

    fn id(m: &SniMap, name: &str) -> Option<u64> {
        m.resolve(name).map(|k| k.id())
    }

    #[test]
    fn exact_beats_wildcard() {
        assert_eq!(id(&map(), "api.example.com"), Some(1));
    }

    #[test]
    fn wildcard_covers_one_label() {
        assert_eq!(id(&map(), "web.example.com"), Some(2));
        // Two labels deep is the default certificate, not the wildcard.
        assert_eq!(id(&map(), "a.b.example.com"), Some(3));
        // The apex is not covered by `*.example.com` either.
        assert_eq!(id(&map(), "example.com"), Some(3));
    }

    #[test]
    fn unknown_name_gets_the_default() {
        assert_eq!(id(&map(), "elsewhere.test"), Some(3));
    }

    #[test]
    fn normalizes_case_and_port() {
        assert_eq!(id(&map(), "API.Example.COM"), Some(1));
        assert_eq!(id(&map(), "api.example.com:443"), Some(1));
        assert_eq!(id(&map(), "api.example.com."), Some(1));
    }

    #[test]
    fn malformed_name_still_gets_the_default() {
        assert_eq!(id(&map(), ""), Some(3));
    }

    #[test]
    fn no_default_means_no_certificate() {
        let m = SniMap::new(FxHashMap::default(), FxHashMap::default(), None);
        assert_eq!(id(&m, "anything.test"), None);
        assert!(m.is_empty());
        assert_eq!(m.len(), 0);
    }
}
