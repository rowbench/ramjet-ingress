//! An experimental completion-based HTTP/1.1 data plane for ramjet-ingress.
//!
//! This is the second engine. The first — [`ramjet_proxy`] — is hyper on tokio
//! and is what `--engine hyper`, the default, runs. This one is selected with
//! `--engine uring` and exists to answer one question that
//! [`bench/PROFILE.md`](https://github.com/sofelia-ai/ramjet-ingress) left open:
//!
//! > 59.4% of a request is the four unavoidable syscalls, and another 9.1% is
//! > finding out a socket is ready. That is the floor for this design […]
//! > Getting under it means fewer syscalls per request, which on Linux means
//! > `io_uring`.
//!
//! A proxy hop is four syscalls — read the request, write it upstream, read the
//! response, write it downstream — and a readiness-based runtime pays for each
//! one separately, plus an `epoll_wait`/`kevent` to learn the socket was ready
//! at all. A completion-based reactor submits all four as ring entries and
//! collects them in one `io_uring_enter`, so the syscall count stops scaling
//! with the request count. That is the whole thesis; everything below is the
//! machinery needed to test it without changing what the proxy *does*.
//!
//! # A note on the dependency
//!
//! The reactor is the `ramjet` crate from a **sibling repository**, taken by
//! path (`../enhance-socket`) rather than from crates.io. A checkout of
//! ramjet-ingress on its own therefore does not build: the sibling has to be
//! beside it. That is a deliberate cost of an experiment tracking an unreleased
//! runtime, and it is why the container builds take the parent directory as
//! their context.
//!
//! # What this engine does and does not do
//!
//! It is honest about what is missing. Everything it refuses, it refuses loudly
//! with a status code and an explanation, never by quietly doing something else.
//!
//! | | |
//! |---|---|
//! | HTTP/1.1, both sides | yes |
//! | Keep-alive, both sides | yes |
//! | Pipelined requests | yes |
//! | `Content-Length` bodies, streamed | yes |
//! | Chunked bodies, forwarded verbatim | yes |
//! | Routing, load balancing, canary | yes, the same [`ramjet_router`] |
//! | `X-Forwarded-*`, `X-Request-Id`, hop-by-hop | yes, same semantics |
//! | Static route file (`--static-routes`) | yes |
//! | TLS termination, SNI, resumption | yes, the same certificate store |
//! | HTTP/2 (including h2c) | **no** — 502 |
//! | HTTP/3 | **no** — the hyper engine's QUIC listener |
//!
//! [`limits`] carries the same list at runtime so `--help` and the logs can
//! print it.
//!
//! # Shape
//!
//! - [`codec`] — sans-io HTTP/1.1 for both directions: head parsing, body
//!   framing, chunked scanning. No I/O, no allocation per request.
//! - [`headers`] — the header rewriting a hop performs, byte-for-byte the same
//!   as the hyper engine's.
//! - [`metrics`] — per-core counters, merged at scrape into the exposition
//!   format [`ramjet_proxy`] emits, so `/metrics` does not change shape when
//!   the engine does.
//! - [`route`] — the glue onto [`ramjet_router`]: one snapshot per request,
//!   the same matcher, the same load balancer.
//! - [`pool`] — per-core upstream connections. Nothing is shared between
//!   cores; that was `PROFILE.md`'s lesson and it is not relearned here.
//! - [`helper`] — one background thread that does the two things the reactor
//!   has no operation for: waiting out a non-blocking `connect`, and ticking a
//!   clock.
//! - [`engine`] — the reactor loop and the connection state machines.

#![forbid(unsafe_op_in_unsafe_fn)]
#![warn(missing_docs)]

pub mod codec;
pub mod engine;
pub mod headers;
pub mod helper;
pub mod limits;
pub mod metrics;
pub mod mirror;
pub mod rng;
pub mod route;
pub mod sys;
pub mod tls;
