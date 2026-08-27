//! `ramjet-proxy` is the ramjet-ingress data plane.
//!
//! It owns the listeners, terminates TLS (via `rustls`), speaks HTTP/1.1 and
//! HTTP/2 to downstream clients, pools connections to upstream endpoints, and
//! forwards requests. It holds the `arc_swap::ArcSwap<ramjet_router::RouteTable>`
//! published by `ramjet-controller` and performs exactly one `load()` per
//! request — no locks, no reload, no per-request allocation beyond that.
//!
//! # Sans-io boundary
//!
//! `ramjet-router` stays pure: it builds and matches route tables with no
//! knowledge of sockets, TLS, or I/O of any kind. This crate is the other
//! side of that boundary — it is where sockets, `rustls`, and async I/O
//! actually appear. A config change never touches this crate directly; the
//! controller builds a new `RouteTable` and swaps a pointer, and the next
//! `forward` call on any worker sees it.
//!
//! # Planned dependencies
//!
//! `ramjet-router` (the route table and matcher), `rustls` (TLS termination
//! and SNI resolution), and the `ramjet` runtime (the sans-io async runtime
//! shared with the other ramjet projects). None of it is wired up yet — this
//! crate is a stub.

// Stub crate: no implementations yet, only the planned module skeleton and
// doc comments describing the intended API surface. Remove once real types
// land and start triggering genuine dead-code warnings.
#![allow(dead_code)]

pub mod listener {
    //! Accept loop, socket options, and PROXY protocol v2 decoding.
    //!
    //! Planned: a `Listener` type that binds a socket address and configures
    //! it (`SO_REUSEADDR`, `TCP_NODELAY`, and friends), then exposes an
    //! `accept` step that yields raw accepted connections — optionally
    //! unwrapping a leading PROXY protocol v2 header to recover the real
    //! client address when running behind an L4 load balancer.
    //!
    //! Shape: `Listener::bind(addr) -> Listener`, `Listener::accept(&self)
    //! -> Connection`. Socket options and PROXY protocol v2 decoding live
    //! here, upstream of TLS and HTTP.
}

pub mod tls {
    //! TLS termination: `rustls::ServerConfig`, SNI certificate resolution,
    //! and ALPN negotiation.
    //!
    //! Planned: a `rustls::server::ResolvesServerCert` implementation
    //! backed by the router's `SniMap`, so certificate selection is a
    //! lookup into the same immutable snapshot the data plane already
    //! reads for routing. ALPN offers `h2` and `http/1.1`; the negotiated
    //! protocol picks between the [`http1`](crate::http1) and
    //! [`http2`](crate::http2) modules downstream.
    //!
    //! The router's `CertifiedKeyHandle` is a placeholder type on the
    //! sans-io side; it resolves to a real `rustls::sign::CertifiedKey`
    //! here, where `rustls` is actually a dependency.
}

pub mod http1 {
    //! HTTP/1.1 request/response pipeline.
    //!
    //! Planned: request parsing, keep-alive connection reuse, and chunked
    //! transfer encoding for both directions of the pipeline.
}

pub mod http2 {
    //! HTTP/2 server: stream multiplexing, HPACK, and flow control.
    //!
    //! Planned: an HTTP/2 server implementation handling multiple
    //! concurrent streams per connection, header compression via HPACK,
    //! and connection- and stream-level flow control windows.
}

pub mod upstream {
    //! Upstream connection pooling: per-endpoint pools, dialing, health,
    //! and retries/failover.
    //!
    //! Planned: a connection pool keyed per backend endpoint, with
    //! dialing, health tracking, and retry/failover behavior. In-flight
    //! request counts hook into the router's `BackendStats` so
    //! load-balancing strategies like LeastConn stay accurate as the
    //! route table is swapped out from under them — a stat belongs to the
    //! backend, not to any one table snapshot.
}

pub mod forward {
    //! The per-request forwarding path.
    //!
    //! Planned: load the current `RouteTable` from the `ArcSwap`, match
    //! the request, select a backend endpoint, and splice bytes in both
    //! directions between the client and upstream connections. This is
    //! the function the sub-200ns matcher budget in `ramjet-router` was
    //! set for — everything else in the request path has to fit around
    //! that number, not the other way around.
}
