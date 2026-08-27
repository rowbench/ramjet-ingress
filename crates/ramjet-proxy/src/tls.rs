//! TLS termination: certificate storage, SNI resolution, and ALPN.
//!
//! # Where the router stops and this starts
//!
//! `ramjet-router` answers "which certificate serves this name?" and refuses to
//! know what a certificate *is*: its [`SniMap`](ramjet_router::SniMap) returns
//! an opaque [`CertifiedKeyHandle`](ramjet_router::CertifiedKeyHandle), which is
//! a `u64`. This module holds the other half — a [`CertStore`] mapping those
//! ids to real `rustls::sign::CertifiedKey`s — and [`SniResolver`] joins them.
//!
//! The split is what keeps the matcher testable without a key, a clock, or a
//! socket, and it has a second payoff: certificates and routes are published
//! independently. A Secret rotating does not rebuild the route table, and a new
//! Ingress does not re-parse every certificate in the cluster.
//!
//! # Two snapshots, resolved in one direction
//!
//! A handshake reads the current route table for the `SniMap` and the current
//! cert store for the key. Those are two separate `ArcSwap`s, so in principle a
//! handshake can see a new table and an old store. That is why the store is
//! published *before* the table that references it: an id that exists in the
//! table always exists in the store, and the only transient state is a store
//! holding a certificate nothing points at yet, which is harmless. Publishing
//! in the other order would produce handshake failures during a rotation.

use std::collections::HashMap;
use std::sync::Arc;

use arc_swap::ArcSwap;
use ramjet_router::SharedRouteTable;
use rustls::server::{ClientHello, ResolvesServerCert};
use rustls::sign::CertifiedKey;
use rustls_pki_types::{CertificateDer, PrivateKeyDer};

/// ALPN protocols offered, most preferred first.
const ALPN: [&[u8]; 2] = [b"h2", b"http/1.1"];

/// Certificates and private keys, keyed by the router's opaque handle id.
///
/// Published as a whole map rather than mutated in place, for the same reason
/// the route table is: a reader takes one atomic load and then holds a
/// consistent set, and a rotation never leaves a handshake looking at a
/// half-updated store.
///
/// The map is the standard library's, with its SipHash hasher. That is a
/// deliberate exception to the FxHash used on the routing path: this lookup
/// happens once per TLS handshake, next to an ECDHE key exchange that costs
/// tens of microseconds. Shaving twenty nanoseconds off it would be measuring
/// the wrong thing.
#[derive(Debug, Default)]
pub struct CertStore {
    inner: ArcSwap<HashMap<u64, Arc<CertifiedKey>>>,
}

impl CertStore {
    /// An empty store. A TLS listener over an empty store fails every
    /// handshake, which is the correct behaviour before any Secret has loaded.
    pub fn new() -> Self {
        Self::default()
    }

    /// A store pre-loaded with `certs`.
    pub fn with_certs(certs: HashMap<u64, Arc<CertifiedKey>>) -> Self {
        CertStore {
            inner: ArcSwap::from_pointee(certs),
        }
    }

    /// Replaces the entire set of certificates.
    ///
    /// Handshakes already in progress keep the set they loaded, exactly like an
    /// in-flight request keeps its route table.
    pub fn publish(&self, certs: HashMap<u64, Arc<CertifiedKey>>) {
        self.inner.store(Arc::new(certs));
    }

    /// The certificate registered for `id`.
    pub fn get(&self, id: u64) -> Option<Arc<CertifiedKey>> {
        self.inner.load().get(&id).map(Arc::clone)
    }

    /// Number of certificates held.
    pub fn len(&self) -> usize {
        self.inner.load().len()
    }

    /// Is the store empty?
    pub fn is_empty(&self) -> bool {
        self.inner.load().is_empty()
    }
}

/// Resolves a handshake's certificate from the current route table.
///
/// Certificate selection deliberately reuses the router's host matching —
/// exact name, then a single-label wildcard, then the default certificate —
/// rather than keeping a second lookup structure. A handshake that picked a
/// different certificate than the request will later be routed by is a
/// spectacularly confusing way to fail, and sharing the code makes it
/// impossible.
#[derive(Debug)]
pub struct SniResolver {
    routes: Arc<SharedRouteTable>,
    certs: Arc<CertStore>,
}

impl SniResolver {
    /// A resolver reading `routes` for names and `certs` for keys.
    pub fn new(routes: Arc<SharedRouteTable>, certs: Arc<CertStore>) -> Self {
        SniResolver { routes, certs }
    }

    /// Resolves a server name without a rustls `ClientHello`, which is what
    /// makes the lookup testable — `ClientHello` cannot be constructed outside
    /// rustls.
    ///
    /// Takes the cheap `load()` guard rather than `load_full()`: this runs
    /// synchronously inside the handshake with no await point, so the guard
    /// never outlives the call.
    pub fn resolve_name(&self, server_name: &str) -> Option<Arc<CertifiedKey>> {
        let table = self.routes.load();
        let handle = table.tls().resolve(server_name)?;
        self.certs.get(handle.id())
    }
}

impl ResolvesServerCert for SniResolver {
    fn resolve(&self, client_hello: ClientHello<'_>) -> Option<Arc<CertifiedKey>> {
        // A client that sent no SNI still deserves an answer. The router treats
        // an unparseable name as "use the default certificate", which is what
        // an empty string resolves to, so the two cases converge without a
        // branch here.
        self.resolve_name(client_hello.server_name().unwrap_or(""))
    }
}

/// Builds a `rustls::ServerConfig` that resolves certificates through
/// `resolver` and offers HTTP/2 then HTTP/1.1 over ALPN.
///
/// The provider is `ring`, not `aws-lc-rs`. Both are sound; `ring` is chosen
/// because every crate in this family has to build with a plain Rust toolchain,
/// and `aws-lc-rs` needs a C compiler and cmake. It is passed explicitly rather
/// than relying on rustls's process-wide default provider, because a
/// process-wide default is global mutable state that a library has no business
/// installing on its embedder's behalf.
pub fn server_config(resolver: Arc<SniResolver>) -> Result<rustls::ServerConfig, rustls::Error> {
    let mut config = rustls::ServerConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()?
    .with_no_client_auth()
    .with_cert_resolver(resolver);

    config.alpn_protocols = ALPN.iter().map(|p| p.to_vec()).collect();
    Ok(config)
}

/// Parses a DER certificate chain and private key into a `CertifiedKey`.
///
/// Offered here so callers loading certificates — the controller, or the dev
/// mode that reads PEM files off disk — do not each have to know which crypto
/// provider this crate settled on.
pub fn certified_key(
    chain: Vec<CertificateDer<'static>>,
    key: PrivateKeyDer<'static>,
) -> Result<CertifiedKey, rustls::Error> {
    CertifiedKey::from_der(chain, key, &rustls::crypto::ring::default_provider())
}

#[cfg(test)]
mod tests {
    use super::*;
    use ramjet_router::{CertifiedKeyHandle, RouteTableBuilder};

    /// A `CertifiedKey` needs a real key pair, and generating one here would
    /// pull `rcgen` into the non-test dependency graph for no benefit. The
    /// store's behaviour is about ids and snapshots, so an empty store plus a
    /// missing-id lookup covers it; end-to-end certificate selection is
    /// asserted against a real handshake in `tests/tls.rs`.
    #[test]
    fn an_empty_store_resolves_nothing() {
        let store = CertStore::new();
        assert!(store.is_empty());
        assert_eq!(store.len(), 0);
        assert!(store.get(1).is_none());
    }

    #[test]
    fn a_name_with_no_certificate_resolves_to_none() {
        let mut builder = RouteTableBuilder::new();
        builder
            .certificate("api.example.com", Arc::new(CertifiedKeyHandle::new(7)))
            .expect("valid host");
        let routes = Arc::new(SharedRouteTable::new(builder.build().expect("builds")));

        // The route table knows the name maps to handle 7, but the store has
        // never been published, so the handshake must fail rather than serve a
        // wrong certificate.
        let resolver = SniResolver::new(routes, Arc::new(CertStore::new()));
        assert!(resolver.resolve_name("api.example.com").is_none());
    }

    #[test]
    fn alpn_offers_h2_before_http11() {
        // Order is preference order, and getting it backwards silently costs
        // every HTTP/2-capable client its multiplexing.
        assert_eq!(ALPN[0], b"h2");
        assert_eq!(ALPN[1], b"http/1.1");
    }
}
