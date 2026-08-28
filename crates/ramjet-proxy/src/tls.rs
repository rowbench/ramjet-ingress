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

/// ALPN offered by a listener that speaks HTTP/1.1 and nothing else.
///
/// The uring engine's TLS lane. Offering `h2` there and then not speaking it
/// would be worse than not offering it: ALPN is a promise, and a client that
/// had it accepted would frame its first request as HTTP/2.
const ALPN_H1: [&[u8]; 1] = [b"http/1.1"];

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
    let mut config = base_config(resolver)?;
    config.alpn_protocols = ALPN.iter().map(|p| p.to_vec()).collect();
    Ok(config)
}

/// The same configuration with `http/1.1` as the only ALPN protocol.
///
/// What the uring engine's TLS listener runs. Identical to [`server_config`] in
/// every other respect — the same resolver, the same provider, the same
/// resumption — because the two lanes have to be indistinguishable to a client
/// that is not asking for HTTP/2, and a benchmark comparing them has to be
/// comparing the engine rather than the TLS settings.
pub fn h1_server_config(resolver: Arc<SniResolver>) -> Result<rustls::ServerConfig, rustls::Error> {
    let mut config = base_config(resolver)?;
    config.alpn_protocols = ALPN_H1.iter().map(|p| p.to_vec()).collect();
    Ok(config)
}

/// Everything both TCP listeners agree on.
///
/// # Resumption
///
/// A ticketer is installed, which is what turns TLS 1.3 resumption from
/// stateful into stateless. rustls resumes without one — its default
/// `session_storage` is an in-memory cache of 256 sessions — but that cache is
/// per process, so a client that lands on a different replica does a full
/// handshake, and 256 sessions is nothing in front of real traffic. Tickets
/// move the state to the client and make a resumption cost one round trip
/// instead of a signature. nginx ships `ssl_session_tickets on` by default for
/// the same reason, and a benchmark against it with resumption disabled on our
/// side would be measuring a configuration nobody deploys.
///
/// Keys are generated at startup and rotated every six hours inside rustls.
/// They are *not* shared between replicas: doing that means distributing a
/// secret whose compromise retroactively breaks forward secrecy for every
/// session it covered, and that is a trade an ingress should not make silently.
fn base_config(resolver: Arc<SniResolver>) -> Result<rustls::ServerConfig, rustls::Error> {
    let mut config = rustls::ServerConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()?
    .with_no_client_auth()
    .with_cert_resolver(resolver);

    // A ticketer that cannot be built means no system randomness, which is not
    // a condition to paper over: every key this process is about to use comes
    // from the same source.
    config.ticketer = rustls::crypto::ring::Ticketer::new()?;
    Ok(config)
}

/// Builds the `rustls::ServerConfig` the QUIC listener hands to quinn.
///
/// It differs from [`server_config`] in exactly three ways, and every one of
/// them is forced:
///
/// - **ALPN is `h3` alone.** An `h3` connection that negotiated `h2` would be
///   two peers framing the same stream differently.
/// - **TLS 1.3 only.** QUIC carries the TLS 1.3 handshake as its own frames and
///   has no encoding for anything earlier; offering TLS 1.2 here would produce
///   a configuration quinn refuses to build.
/// - **No 0-RTT.** `max_early_data_size` is zeroed explicitly rather than left
///   to a default. Early data is replayable by anyone who captured it, and
///   which requests are safe to replay is a judgement an application makes, not
///   one an ingress can make on its behalf.
///
/// What it deliberately does *not* differ in is the certificate resolver: the
/// caller passes the same [`SniResolver`] the TLS listener holds, so a name
/// resolves to the same certificate over QUIC as over TCP, and a rotation
/// reaches both at the same instant because it is the same two `ArcSwap`s.
pub fn quic_server_config(
    resolver: Arc<SniResolver>,
) -> Result<rustls::ServerConfig, rustls::Error> {
    let mut config = rustls::ServerConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_protocol_versions(&[&rustls::version::TLS13])?
    .with_no_client_auth()
    .with_cert_resolver(resolver);

    config.alpn_protocols = vec![crate::http3::ALPN_H3.to_vec()];
    config.max_early_data_size = 0;
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

    #[test]
    fn the_http1_only_listener_does_not_offer_h2() {
        // Offering a protocol the engine behind this listener cannot speak is
        // worse than offering nothing: the client would frame its first request
        // as HTTP/2 and get an HTTP/1.1 parser.
        assert_eq!(ALPN_H1.len(), 1);
        assert_eq!(ALPN_H1[0], b"http/1.1");
    }

    #[test]
    fn both_listeners_resume_sessions() {
        // The two configurations have to differ in ALPN and in nothing else, or
        // an engine-versus-engine benchmark is measuring the TLS settings.
        let routes = Arc::new(SharedRouteTable::new(
            RouteTableBuilder::new().build().expect("an empty table"),
        ));
        let resolver = || Arc::new(SniResolver::new(Arc::clone(&routes), Arc::new(CertStore::new())));

        let full = server_config(resolver()).expect("a config");
        let h1 = h1_server_config(resolver()).expect("a config");

        assert!(full.ticketer.enabled(), "resumption is on for the hyper lane");
        assert!(h1.ticketer.enabled(), "resumption is on for the uring lane");
        assert_eq!(full.alpn_protocols.len(), 2);
        assert_eq!(h1.alpn_protocols, vec![b"http/1.1".to_vec()]);
    }
}
