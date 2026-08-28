//! `ramjet-proxy` is the ramjet-ingress data plane.
//!
//! It owns the listeners, terminates TLS, speaks HTTP/1.1 and HTTP/2 to
//! downstream clients, pools connections to upstream endpoints, and forwards
//! requests. It reads the [`SharedRouteTable`](ramjet_router::SharedRouteTable)
//! that the controller publishes and does exactly one `load_full()` per
//! request — no locks, no reload, no rebuild pause.
//!
//! # The sans-io boundary
//!
//! `ramjet-router` decides *what* a request matches; this crate decides *how*
//! the bytes move. The router never opens a socket, never learns what rustls
//! is, and never sees a `HeaderMap`. Everything it needs is handed to it as
//! borrowed `&str`s and plain integers: the `Host` value, the path, a canary
//! header value, a random word. That is the entire contract, and keeping it
//! narrow is why the matcher can be benchmarked without a network and why this
//! crate can be rewritten without touching routing semantics.
//!
//! Three things cross the boundary in the other direction, and they all live
//! here because they need I/O types the router refuses to depend on:
//!
//! - [`CertStore`] resolves the router's opaque
//!   [`CertifiedKeyHandle`](ramjet_router::CertifiedKeyHandle) ids into real
//!   `rustls::sign::CertifiedKey`s.
//! - [`rng`] supplies the random words the router's load balancer and canary
//!   splitter take as arguments.
//! - [`Upstream`] turns a selected [`Endpoint`](ramjet_router::Endpoint) into
//!   an actual TCP connection.
//!
//! # One snapshot per request
//!
//! [`SharedRouteTable::load_full`](ramjet_router::SharedRouteTable::load_full)
//! is called once at the top of [`forward::handle`] and the resulting `Arc` is
//! held until the response is on the wire. Everything downstream of that —
//! host matching, path matching, canary evaluation, endpoint selection, and
//! the in-flight counter guard — reads through that one snapshot, so a request
//! can never observe a half-applied configuration change.
//!
//! The snapshot is deliberately *not* taken once per connection, even though an
//! HTTP/1.1 keep-alive connection would then pay for a single load instead of
//! one per request. A keep-alive connection from a busy client can live for
//! hours; pinning it to the generation it was accepted under would mean a route
//! deleted at 09:00 keeps serving traffic at 17:00, which is a correctness bug
//! wearing a performance costume. Per-request is one uncontended atomic
//! increment against a cache line that every worker is already reading — a few
//! nanoseconds — and it buys the property that a published table is *actually*
//! in force the moment it is published. HTTP/2 makes the argument moot anyway:
//! one connection carries many concurrent streams, so "per connection" would
//! not even be well defined.
//!
//! # What is deliberately not here
//!
//! The proxy runs on tokio and hyper. An earlier sketch of this crate proposed
//! building it on the experimental `ramjet` reactor runtime shared with the
//! other ramjet projects; that is a later phase and an explicitly separate
//! experiment. Replacing a mature HTTP/1.1 and HTTP/2 implementation is not a
//! prerequisite for an ingress controller, and doing it before the data plane
//! is correct would make every bug a two-suspect problem.
//!
//! # Example
//!
//! ```no_run
//! use std::sync::Arc;
//! use ramjet_proxy::{CertStore, ProxyConfig, Server, Shutdown};
//! use ramjet_router::{Endpoint, LbPolicy, PathType, RouteTableBuilder, SharedRouteTable};
//!
//! # async fn run() -> Result<(), Box<dyn std::error::Error>> {
//! let mut builder = RouteTableBuilder::new();
//! builder.backend("api", LbPolicy::RoundRobin, vec![Endpoint::new("10.0.0.1:8080".parse()?)])?;
//! builder.route(Some("example.com"), "/", PathType::Prefix, "api")?;
//!
//! let routes = Arc::new(SharedRouteTable::new(builder.build()?));
//! let certs = Arc::new(CertStore::new());
//!
//! let server = Server::bind(ProxyConfig::default(), routes, certs)?;
//! server.readiness().set_ready(true);
//! server.run(Shutdown::on_signal()).await?;
//! # Ok(())
//! # }
//! ```

#![forbid(unsafe_code)]
#![deny(missing_docs)]

pub mod admin;
pub mod body;
pub mod forward;
pub mod headers;
pub mod history;
pub mod http3;
pub mod listener;
pub mod metrics;
pub mod mirror;
pub mod proxy_protocol;
pub mod rng;
pub mod server;
pub mod tls;
pub mod upstream;

pub use admin::{AdminState, ReadinessFlag};
pub use body::{BodyError, ProxyBody};
pub use forward::{ConnInfo, ProxyState, Scheme};
pub use history::{
    CertKeys, GenerationHistory, GenerationRecord, PinChange, PinError, DEFAULT_HISTORY_SIZE,
};
pub use listener::{Listener, ListenerConfig};
pub use metrics::{Exposition, Metrics};
pub use mirror::{Mirror, DEFAULT_MIRROR_MAX_BODY, MIRROR_QUEUE_DEPTH};
pub use server::{
    serve, serve_admin_only, ProxyConfig, Server, Shutdown, ShutdownHandle, DEFAULT_ADMIN_PORT,
    DEFAULT_HTTP_PORT, DEFAULT_HTTPS_PORT, DEFAULT_MAX_BUF_SIZE, MIN_MAX_BUF_SIZE,
};
pub use tls::{CertStore, SniResolver};
pub use upstream::{
    Upstream, UpstreamConfig, UpstreamError, DEFAULT_POOL_MAX_IDLE_PER_HOST,
};
