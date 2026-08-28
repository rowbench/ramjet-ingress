//! HTTP/3 over QUIC, downstream only, and off unless it is asked for.
//!
//! This is a second way for a request to arrive, not a second proxy. A request
//! that comes in over QUIC is translated into the `http` crate types the rest
//! of this crate already speaks and handed to [`forward::handle`], which is the
//! same function the HTTP/1.1 and HTTP/2 listeners call. Routing, canaries,
//! load balancing, header rewriting, retries, per-route counters and the
//! upstream pool are therefore not reimplemented here and cannot drift: the
//! only thing this module owns is how bytes get on and off the wire.
//!
//! # Why there is a listener here at all rather than in `listener.rs`
//!
//! QUIC is not a stream protocol with a different handshake — it is a
//! datagram protocol that provides its own streams, its own TLS, and its own
//! connection identity. There is no `accept(2)` returning a socket, no
//! `TcpStream` to hand to hyper, and nothing for `TCP_NODELAY` to mean. So the
//! socket is a `UdpSocket`, the "connections" come out of a
//! [`quinn::Endpoint`], and none of `listener.rs` applies.
//!
//! # One endpoint, on one runtime, and why not one per core
//!
//! The TCP data plane runs one `current_thread` runtime per core with
//! `SO_REUSEPORT` spreading accepts across them. The obvious transliteration —
//! N UDP sockets on one port, one quinn endpoint each — is wrong, and quietly
//! so.
//!
//! The kernel picks which `SO_REUSEPORT` socket receives a datagram by hashing
//! its **4-tuple**. A QUIC connection is not identified by its 4-tuple; it is
//! identified by a connection ID, precisely so that it can survive the client's
//! address changing — a phone moving from wifi to cellular, or any NAT
//! rebinding. Under 4-tuple hashing, the moment a client's address changes its
//! packets land on a different socket, whose endpoint has never heard of that
//! connection, and the connection dies. Migration is one of the few things QUIC
//! offers that TCP cannot, and sharding this way trades it for throughput.
//!
//! Doing it properly needs the kernel to steer by connection ID, which on Linux
//! means an eBPF `SO_REUSEPORT` program (or a userspace router in front of the
//! endpoints). That is a real design, and it is not this one.
//!
//! So: **one QUIC endpoint, on one dedicated thread with its own
//! `current_thread` runtime.** Every h3 connection and every request on it is
//! served there, from an upstream pool of its own. The ceiling that sets is
//! stated rather than measured — one core's worth of QUIC crypto, packet
//! handling, and proxying — and it is the honest reason this feature is
//! experimental. HTTP/1.1 and HTTP/2 keep every core they had; nothing about
//! the TCP path changes when this is switched on.
//!
//! # What is not here
//!
//! - **No 0-RTT.** `max_early_data_size` is set to zero explicitly. 0-RTT data
//!   is replayable by anyone who captured it, and deciding which requests are
//!   safe to replay is an application's judgement, not an ingress's.
//! - **No QUIC upstream.** Upstream is HTTP/1.1, exactly as it is for every
//!   other downstream protocol here.
//! - **No PROXY protocol.** It is a TCP-stream preamble and has no UDP form —
//!   there is no byte stream in front of the ClientHello to put it in. A
//!   QUIC packet carries the client's real address in its IP header, so a
//!   load balancer that forwards UDP (an AWS NLB, say) forwards the address
//!   with it or does not forward the connection at all.
//! - **No protocol upgrades.** WebSockets over HTTP/3 are RFC 9220 extended
//!   `CONNECT`, which is a different mechanism from a `101`; an upstream that
//!   answers `101` to a request that arrived over QUIC gets the same 502 that
//!   any half-completable upgrade gets.
//! - **No h3 datagrams, no WebTransport, no server push.**

use std::io;
use std::net::{SocketAddr, UdpSocket};
use std::sync::Arc;
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use bytes::{Buf, Bytes};
use http::{header, HeaderMap, HeaderValue, Request, Response};
use http_body::{Body, Frame, SizeHint};
use ramjet_router::SharedRouteTable;
use tokio::sync::oneshot;
use tokio::task::JoinSet;

use crate::body::{BodyError, ProxyBody};
use crate::forward::{self, ConnInfo, ProxyState, Scheme};
use crate::metrics::Metrics;
use crate::server::Shutdown;
use crate::tls::{self, SniResolver};
use crate::upstream::{Upstream, UpstreamConfig};

/// The ALPN protocol identifier for HTTP/3, from RFC 9114.
pub(crate) const ALPN_H3: &[u8] = b"h3";

/// How long the endpoint is given, after the drain, to put its
/// `CONNECTION_CLOSE` frames on the wire.
///
/// `Endpoint::close` only *marks* the connections closed; the driver still has
/// to send the frames, and `wait_idle` is how one waits for that. It is bounded
/// because a peer that has already gone away will never be reachable, and
/// hanging the process's shutdown on it would turn a tidy goodbye into a
/// `SIGKILL`.
const CLOSE_LINGER: Duration = Duration::from_secs(1);

/// The application error code sent in `CONNECTION_CLOSE` at shutdown.
///
/// `H3_NO_ERROR`: the connection is ending because this process is, which is
/// not a protocol error and should not be logged as one by the client.
const H3_NO_ERROR: u32 = 0x0100;

/// The `alt-svc` value advertising this port to HTTP/1.1 and HTTP/2 clients.
///
/// The port is the TLS listener's own, because that is the only port an
/// `alt-svc` advertisement can usefully name: a client that follows it retries
/// the same authority over QUIC, and a different port would have to be
/// separately reachable through whatever is in front of this process. See
/// `deploy/README.md` for which load balancers can and cannot do that.
///
/// `ma=86400` is a day, matching what nginx and Caddy advertise. It is a cache
/// lifetime for the advertisement, not a promise: a client whose QUIC attempt
/// fails falls back to TCP on its own.
pub(crate) fn alt_svc_value(port: u16) -> Option<HeaderValue> {
    HeaderValue::from_str(&format!("h3=\":{port}\"; ma=86400")).ok()
}

/// A bound UDP socket and the crypto configuration to serve QUIC on it.
///
/// Split from the endpoint for the same reason `Server::bind` is split from
/// `Server::run`: binding is the fallible part and happens on the caller's
/// thread, where a failure is a startup error somebody sees. The
/// [`quinn::Endpoint`] itself is built later, on the runtime that will serve
/// it, because creating one spawns its driver task.
#[derive(Debug)]
pub(crate) struct Listener {
    socket: UdpSocket,
    server: quinn::ServerConfig,
    addr: SocketAddr,
}

impl Listener {
    /// Binds the UDP socket and builds the QUIC crypto configuration.
    ///
    /// `resolver` is the *same* [`SniResolver`] the TLS listener uses, holding
    /// the same route table and the same [`CertStore`](crate::tls::CertStore).
    /// That is not a convenience: a name has to resolve to the same
    /// certificate over QUIC as it does over TCP, and it has to keep doing so
    /// across a rotation. Sharing the resolver makes both true by construction
    /// — a certificate published for the TLS listener is in force on this one
    /// the moment the same `ArcSwap` is stored into.
    pub(crate) fn bind(addr: SocketAddr, resolver: Arc<SniResolver>) -> io::Result<Self> {
        // Deliberately *not* `SO_REUSEPORT` or `SO_REUSEADDR`, which every TCP
        // listener in this process sets. A second socket sharing this UDP port
        // would receive a share of the datagrams belonging to connections the
        // first one owns, and QUIC has no way to recover from that: the packets
        // are simply delivered to an endpoint that cannot decrypt them. A bind
        // that fails loudly with `EADDRINUSE` is the outcome to want here.
        let socket = UdpSocket::bind(addr)?;
        // quinn drives this through its own reactor registration, which needs a
        // non-blocking socket; a blocking one stalls the whole runtime.
        socket.set_nonblocking(true)?;
        let addr = socket.local_addr()?;

        let crypto = tls::quic_server_config(resolver)
            .map_err(|error| io::Error::other(error.to_string()))?;
        let crypto = quinn::crypto::rustls::QuicServerConfig::try_from(crypto)
            .map_err(|error| io::Error::other(error.to_string()))?;

        // The transport parameters are quinn's defaults, on purpose: 100
        // concurrent bidirectional streams and the 30 second idle timeout RFC
        // 9308 recommends. Numbers invented here without a measurement to back
        // them would be worse than the ones whose defence is written down in
        // the specification.
        let server = quinn::ServerConfig::with_crypto(Arc::new(crypto));

        Ok(Listener {
            socket,
            server,
            addr,
        })
    }

    /// The address actually bound, which is how a caller recovers the port it
    /// asked the kernel to choose.
    pub(crate) fn local_addr(&self) -> SocketAddr {
        self.addr
    }
}

/// Everything the HTTP/3 runtime is configured with.
pub(crate) struct ServeConfig {
    /// The published route table, shared with every other serving runtime.
    pub(crate) routes: Arc<SharedRouteTable>,
    /// Data-plane counters, shared with every other serving runtime.
    pub(crate) metrics: Arc<Metrics>,
    /// Upstream timeouts and pooling. The pool built from it is this runtime's
    /// own, exactly as each TCP serving runtime has its own.
    pub(crate) upstream: UpstreamConfig,
    /// How long in-flight requests get after the shutdown signal.
    pub(crate) grace: Duration,
    /// Which per-route counter shard this runtime writes to.
    pub(crate) shard: usize,
}

/// A running HTTP/3 listener, and the handle its owner drains it through.
#[derive(Debug)]
pub(crate) struct Handle {
    done: oneshot::Receiver<bool>,
}

impl Handle {
    /// Waits for the QUIC endpoint to finish draining.
    ///
    /// Returns whether it drained inside the grace period. A thread that
    /// vanished without reporting cannot be said to have drained cleanly.
    pub(crate) async fn drain(self) -> bool {
        self.done.await.unwrap_or(false)
    }
}

/// Starts the HTTP/3 runtime on its own thread.
///
/// The thread ends when `shutdown` fires and the drain finishes; see the module
/// docs for why it is one thread rather than one per core.
pub(crate) fn spawn(
    listener: Listener,
    config: ServeConfig,
    shutdown: Shutdown,
) -> io::Result<Handle> {
    let (done_tx, done_rx) = oneshot::channel();

    // Built here rather than on the new thread, and moved into it: a runtime
    // that cannot be created is a startup failure the caller can report, where
    // discovering it on the thread would leave a bound UDP port that answers
    // nothing.
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;

    std::thread::Builder::new()
        .name("ramjet-h3".to_owned())
        .spawn(move || {
            let drained = runtime.block_on(serve(listener, config, shutdown));
            // A receiver that has gone away means `Server::run` already
            // returned, which is not this thread's problem.
            let _ = done_tx.send(drained);
        })?;

    Ok(Handle { done: done_rx })
}

/// The endpoint's accept loop, and the drain that follows it.
async fn serve(listener: Listener, config: ServeConfig, mut shutdown: Shutdown) -> bool {
    let Listener {
        socket,
        server,
        addr: _,
    } = listener;
    let grace = config.grace;

    let endpoint = match quinn::Endpoint::new(
        quinn::EndpointConfig::default(),
        Some(server),
        socket,
        Arc::new(quinn::TokioRuntime),
    ) {
        Ok(endpoint) => endpoint,
        Err(error) => {
            // Everything that can plausibly fail — binding the port, building
            // the crypto — already happened at startup, so reaching here means
            // quinn declined to drive a socket that is already bound and
            // non-blocking. It is logged at `error` rather than being fatal
            // because the TCP listeners are serving and killing the process
            // would take them down with it; there is nothing to serve on this
            // one and nothing to drain, so the drain succeeds vacuously.
            tracing::error!(%error, "the HTTP/3 endpoint could not be started");
            return true;
        }
    };

    // Built inside the runtime and owned by it: this is the upstream pool the
    // module docs are about.
    let state = Arc::new(ProxyState {
        routes: config.routes,
        upstream: Upstream::new(&config.upstream),
        metrics: config.metrics,
        shard: config.shard,
    });

    let mut connections = JoinSet::new();
    loop {
        tokio::select! {
            // Shutdown wins a tie, exactly as it does on the TCP accept loop:
            // once asked to stop there is no reason to take one more
            // connection just because it arrived first.
            biased;
            () = shutdown.recv() => break,
            incoming = endpoint.accept() => {
                // `None` means the endpoint is closed, which at this point can
                // only be something else having closed it.
                let Some(incoming) = incoming else { break };
                connections.spawn(serve_connection(
                    Arc::clone(&state),
                    incoming,
                    shutdown.clone(),
                ));
            }
        }
    }

    // Step one: stop accepting. Unlike a TCP listener this socket cannot simply
    // be dropped — the endpoint is what delivers packets to the connections
    // that are still draining — so new connection attempts are refused while
    // the established ones keep running.
    // Dropping the server configuration is quinn's way of saying this: it
    // "affects new incoming connections only", so the endpoint stops answering
    // handshakes while every established connection keeps its packets flowing.
    // `Endpoint::close` is the wrong tool here — it tears down the connections
    // that are still draining, which is the opposite of a graceful shutdown.
    endpoint.set_server_config(None);

    // Step two: let what is in flight finish. Each connection task has its own
    // copy of the shutdown signal and has already started sending GOAWAY.
    let drained = tokio::time::timeout(grace, async {
        while connections.join_next().await.is_some() {}
    })
    .await
    .is_ok();

    // Whatever is left gets a CONNECTION_CLOSE rather than silence, so a client
    // retries somewhere else instead of waiting out its idle timeout.
    endpoint.close(H3_NO_ERROR.into(), b"shutting down");
    let _ = tokio::time::timeout(CLOSE_LINGER, endpoint.wait_idle()).await;
    drained
}

/// Serves one QUIC connection until it closes or drains.
async fn serve_connection(state: Arc<ProxyState>, incoming: quinn::Incoming, shutdown: Shutdown) {
    let remote = incoming.remote_address();

    let connection = match incoming.await {
        Ok(connection) => connection,
        Err(_) => {
            // Routine at the edge, exactly as a failed TLS handshake is on the
            // TCP listener: a scanner, a client with no acceptable version, an
            // SNI this replica holds no certificate for. Counted, not logged
            // per occurrence.
            state.metrics.record_h3_handshake_failure();
            return;
        }
    };

    // Counted from here, where the connection is usable, and released when this
    // task ends however it ends.
    let _guard = state.metrics.connection_opened();
    state.metrics.record_h3_connection();

    let mut h3 = match h3::server::builder()
        .build::<_, Bytes>(h3_quinn::Connection::new(connection))
        .await
    {
        Ok(h3) => h3,
        Err(_) => {
            // The QUIC handshake finished but the h3 layer never did — a peer
            // that is not speaking HTTP/3 on a port that only offers it. Same
            // series as a failed handshake, because the operator's question is
            // the same one: how many connections never became usable.
            state.metrics.record_h3_handshake_failure();
            return;
        }
    };

    // Boxed and pinned so the accept loop can poll it repeatedly without
    // re-borrowing `h3`, which the loop below needs exclusively.
    let mut signal = Box::pin(async move {
        let mut shutdown = shutdown;
        shutdown.recv().await;
    });
    let mut draining = false;

    // The requests running on this connection, tracked here rather than
    // detached, because "has this connection finished draining?" is exactly
    // "is this set empty?" and there is nowhere else to ask it.
    //
    // h3's own `accept` cannot answer it. It yields `None` once every request
    // is complete *and* a GOAWAY has been received — the peer's, not ours — so
    // a server that sent GOAWAY and then waited for `None` would be waiting for
    // the client to hang up. After a GOAWAY every client is idle by definition,
    // and an idle client does not hang up, so the whole grace period would
    // elapse on every shutdown that had an open connection.
    let mut requests = JoinSet::new();

    loop {
        // Nothing left to finish, and nothing more coming: this connection is
        // drained.
        if draining && requests.is_empty() {
            break;
        }

        // The borrows the arms take end with this expression, which is what
        // lets the handlers below use `h3` and `requests` again.
        let accepted = tokio::select! {
            biased;
            () = &mut signal, if !draining => Accepted::Drain,
            Some(_) = requests.join_next(), if !requests.is_empty() => Accepted::Finished,
            result = h3.accept() => Accepted::Stream(result),
        };

        match accepted {
            // GOAWAY: this connection takes no further requests, so the client
            // opens its next one somewhere else rather than discovering the
            // refusal by having a request fail. What is already running keeps
            // running.
            Accepted::Drain => {
                draining = true;
                let _ = h3.shutdown(0).await;
            }
            // Re-checked at the top of the loop.
            Accepted::Finished => {}
            Accepted::Stream(Ok(Some(resolver))) => {
                requests.spawn(serve_request(Arc::clone(&state), resolver, remote));
            }
            // `Ok(None)` is the peer's own goodbye with everything finished; an
            // error is a connection that ended under us. Neither is worth a log
            // line at the edge, and neither leaves anything to drain.
            Accepted::Stream(Ok(None) | Err(_)) => break,
        }
    }
}

/// What one pass of the connection loop's `select!` produced.
///
/// An enum rather than nested `Option`s because the three outcomes are not
/// shades of the same thing: one starts a drain, one ends a request, and one
/// carries a stream.
enum Accepted<C, B>
where
    C: h3::quic::Connection<B>,
    B: Buf,
{
    /// Shutdown was signalled; send GOAWAY and start draining.
    Drain,
    /// A request finished, so the in-flight set may now be empty.
    Finished,
    /// The connection produced a request, an end, or an error.
    Stream(Result<Option<h3::server::RequestResolver<C, B>>, h3::error::ConnectionError>),
}

/// The h3 connection type this module serves, spelled out once.
type H3Connection = h3_quinn::Connection;
/// The receiving half of one request's bidirectional stream.
type RecvStream = h3::server::RequestStream<h3_quinn::RecvStream, Bytes>;

/// Translates one HTTP/3 request into the shared forwarding path and writes the
/// answer back out as h3 frames.
async fn serve_request(
    state: Arc<ProxyState>,
    resolver: h3::server::RequestResolver<H3Connection, Bytes>,
    remote: SocketAddr,
) {
    let (request, stream) = match resolver.resolve_request().await {
        Ok(pair) => pair,
        // h3 has already reset the stream with the right code — a malformed
        // request, an oversized header block — and there is nothing left to
        // answer on.
        Err(_) => return,
    };
    state.metrics.record_h3_request();

    // Split so the request body can still be streaming upstream while the
    // response streams back down; they are two directions of one QUIC stream
    // and nothing serialises them.
    let (mut send, recv) = stream.split();

    let (parts, ()) = request.into_parts();
    let body = request_body(recv, &parts.headers);
    let request = Request::from_parts(parts, body);

    // `Https` because that is what it is: QUIC carries TLS 1.3 and there is no
    // plaintext HTTP/3. `X-Forwarded-Proto: https` is the answer an application
    // behind this needs, and the peer address is the QUIC connection's — with
    // no PROXY protocol anywhere, because there is no stream to put one in.
    let conn = ConnInfo {
        remote,
        scheme: Scheme::Https,
    };
    let response = forward::handle(state, conn, request).await;
    let (parts, body) = response.into_parts();

    // `send_response` takes the head alone; the body follows as DATA frames.
    if send.send_response(Response::from_parts(parts, ())).await.is_err() {
        return;
    }

    let mut body = std::pin::pin!(body);
    loop {
        let frame = std::future::poll_fn(|cx| body.as_mut().poll_frame(cx)).await;
        match frame {
            Some(Ok(frame)) => match frame.into_data() {
                Ok(data) => {
                    if send.send_data(data).await.is_err() {
                        return;
                    }
                }
                // Anything that is not data is trailers, which HTTP/3 carries
                // as a second HEADERS frame and which end the response. They
                // do not end the *stream*, though: `finish` below is what puts
                // the FIN on it, and without that quinn resets the stream when
                // the handle drops — turning a complete response into an error
                // the client reports.
                Err(frame) => {
                    if let Ok(trailers) = frame.into_trailers() {
                        let _ = send.send_trailers(trailers).await;
                        break;
                    }
                }
            },
            Some(Err(_)) => {
                // The response head is already on the wire, so there is no
                // status code left to change. Resetting the stream is the only
                // way to tell the client that what it received is incomplete —
                // finishing cleanly would hand it a truncated body it believes
                // is whole.
                send.stop_stream(h3::error::Code::H3_INTERNAL_ERROR);
                return;
            }
            None => break,
        }
    }
    let _ = send.finish().await;
}

/// Builds the body a forwarded request carries, from the request stream.
///
/// HTTP/3, like HTTP/2, has no `Transfer-Encoding` and no framing outside the
/// stream itself: a request has a body if and only if DATA frames arrive before
/// the client finishes the stream. So the length comes from `content-length`
/// when the client sent one, and otherwise has to be discovered.
///
/// The discovery is a single **non-blocking** poll. A client that has already
/// finished the stream — which is every ordinary `GET`, and the state its
/// packets arrive in — is recognised immediately and forwarded with a body that
/// is known to be empty, which is what makes such a request retryable against a
/// second endpoint and keeps it from reaching an upstream as
/// `Transfer-Encoding: chunked`. A client that has not is not waited for: the
/// poll returns `Pending`, the body streams, and the first DATA frame goes
/// upstream whenever it arrives.
fn request_body(recv: RecvStream, headers: &HeaderMap) -> ProxyBody {
    let declared = headers
        .get(header::CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok());

    if declared == Some(0) {
        return ProxyBody::empty();
    }

    let mut body = RequestBody::new(recv, declared);
    // Only when the client did not say: a declared length is the client's own
    // statement about its body, and second-guessing it here would build a
    // request whose header and body disagree.
    if declared.is_none() && body.already_ended() {
        return ProxyBody::empty();
    }
    ProxyBody::http3(body)
}

/// A request body arriving as HTTP/3 DATA frames.
///
/// Not an `http_body::Body` implementation of its own: it is polled through
/// [`ProxyBody`], which is the one body type this crate moves in both
/// directions, and giving it a second `Body` impl would only add a way to
/// bypass that.
///
/// Backpressure is the QUIC stream's own. Nothing is read until the upstream
/// connection polls for it, so a client uploading faster than the origin can
/// take it runs out of flow-control window rather than out of this process's
/// memory.
pub(crate) struct RequestBody {
    stream: RecvStream,
    /// A chunk, or a failure, already taken off the stream by
    /// [`already_ended`](Self::already_ended) and not yet handed on.
    queued: Option<Result<Bytes, h3::error::StreamError>>,
    /// What is left of a `content-length` the client declared, or `None` when
    /// it declared nothing.
    remaining: Option<u64>,
    /// Whether the stream has produced its last frame.
    ended: bool,
}

/// Hand-written because `h3::server::RequestStream` is not `Debug`, and the
/// stream is the one field nothing useful could be printed from anyway. The
/// rest is what a reader actually wants when a body misbehaves: whether a frame
/// is held back, how much the client still owes, and whether it has ended.
impl std::fmt::Debug for RequestBody {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RequestBody")
            .field("queued", &self.queued.is_some())
            .field("remaining", &self.remaining)
            .field("ended", &self.ended)
            .finish_non_exhaustive()
    }
}

impl RequestBody {
    fn new(stream: RecvStream, declared: Option<u64>) -> Self {
        RequestBody {
            stream,
            queued: None,
            remaining: declared,
            ended: false,
        }
    }

    /// Asks the stream whether it has already ended, without waiting for it.
    ///
    /// A `Waker` that does nothing is sound here because the answer is used
    /// immediately and discarded: if the poll returns `Pending`, the wakeup it
    /// registered is simply never delivered, and the next poll — the real one,
    /// with the real waker — registers again. That is the ordinary contract for
    /// polling a future twice.
    ///
    /// Anything the poll *did* produce is kept rather than dropped, which is
    /// what makes this a peek rather than a read.
    fn already_ended(&mut self) -> bool {
        let mut context = Context::from_waker(Waker::noop());
        match self.stream.poll_recv_data(&mut context) {
            Poll::Ready(Ok(None)) => {
                self.ended = true;
                true
            }
            Poll::Ready(Ok(Some(chunk))) => {
                self.queued = Some(Ok(collect(chunk)));
                false
            }
            Poll::Ready(Err(error)) => {
                self.queued = Some(Err(error));
                false
            }
            Poll::Pending => false,
        }
    }

    /// Whether this body is known, without reading it, to carry no data.
    pub(crate) fn is_known_empty(&self) -> bool {
        self.queued.is_none() && (self.ended || self.remaining == Some(0))
    }

    /// Whether the last frame has already been produced.
    pub(crate) fn is_end_stream(&self) -> bool {
        self.ended && self.queued.is_none()
    }

    /// What is left to read, as far as the client's `content-length` says.
    pub(crate) fn size_hint(&self) -> SizeHint {
        match self.remaining {
            // The declared length counts *down* as frames are handed on,
            // because this is the remaining length and hyper reads it to
            // decide the upstream framing: a hint that kept reporting the whole
            // body after half of it had gone would describe a longer request
            // than the one being sent.
            //
            // A chunk read off the stream but not yet handed on is still
            // counted here, because `deliver` — the only thing that decrements
            // — runs when a chunk leaves rather than when it arrives.
            Some(remaining) => SizeHint::with_exact(remaining),
            // Nothing was declared. Once the stream has ended there is nothing
            // left by definition; before that, nothing is known.
            None if self.is_end_stream() => SizeHint::with_exact(0),
            None => SizeHint::default(),
        }
    }

    /// Produces the next frame of the request body.
    pub(crate) fn poll_frame(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, BodyError>>> {
        if let Some(queued) = self.queued.take() {
            return Poll::Ready(Some(self.deliver(queued)));
        }
        if self.ended {
            return Poll::Ready(None);
        }
        match self.stream.poll_recv_data(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Ok(None)) => {
                self.ended = true;
                Poll::Ready(None)
            }
            Poll::Ready(Ok(Some(chunk))) => Poll::Ready(Some(self.deliver(Ok(collect(chunk))))),
            Poll::Ready(Err(error)) => Poll::Ready(Some(self.deliver(Err(error)))),
        }
    }

    /// Hands one chunk on, keeping the declared length in step with it.
    ///
    /// A failure ends the body. It is reported rather than swallowed because
    /// the alternative — returning `None` — would tell the upstream that a
    /// truncated request was complete, and for a request with no declared
    /// length there is nothing else that would ever catch it.
    fn deliver(
        &mut self,
        chunk: Result<Bytes, h3::error::StreamError>,
    ) -> Result<Frame<Bytes>, BodyError> {
        match chunk {
            Ok(data) => {
                if let Some(remaining) = &mut self.remaining {
                    *remaining = remaining.saturating_sub(data.len() as u64);
                }
                Ok(Frame::data(data))
            }
            Err(error) => {
                self.ended = true;
                Err(BodyError::Http3(error))
            }
        }
    }
}

/// Turns whatever `h3` handed back into `Bytes`.
///
/// `poll_recv_data` returns an opaque `impl Buf` rather than a concrete type.
/// `copy_to_bytes` over the whole of it is the conversion the `Buf` contract
/// offers, and for the single-chunk case that this is in practice it hands back
/// the underlying `Bytes` without copying.
fn collect(mut chunk: impl Buf) -> Bytes {
    chunk.copy_to_bytes(chunk.remaining())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn alt_svc_names_the_port_and_a_day_of_cache() {
        let value = alt_svc_value(443).expect("a valid header value");
        assert_eq!(value.to_str().expect("ascii"), "h3=\":443\"; ma=86400");
    }

    #[test]
    fn alt_svc_quotes_the_authority_so_a_client_parses_it() {
        // RFC 7838's alt-authority is a quoted string. An unquoted value is
        // silently ignored by clients, which would look exactly like HTTP/3
        // not working.
        let value = alt_svc_value(8443).expect("a valid header value");
        let text = value.to_str().expect("ascii");
        assert!(text.contains("\":8443\""), "{text} does not quote the port");
    }

    #[test]
    fn the_advertised_protocol_is_the_one_alpn_offers() {
        // Two different spellings of "HTTP/3" — the ALPN identifier and the
        // alt-svc protocol name — that have to stay the same string.
        let value = alt_svc_value(443).expect("a valid header value");
        let alpn = std::str::from_utf8(ALPN_H3).expect("ascii");
        assert!(value.to_str().expect("ascii").starts_with(alpn));
    }

    #[tokio::test]
    async fn a_zero_content_length_needs_no_stream_at_all() {
        // The one case decided from the headers alone, and worth keeping there:
        // it is every form POST that turned out to be empty, and it makes the
        // request retryable without touching the QUIC stream.
        let mut headers = HeaderMap::new();
        headers.insert(header::CONTENT_LENGTH, HeaderValue::from_static("0"));
        let declared = headers
            .get(header::CONTENT_LENGTH)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse::<u64>().ok());
        assert_eq!(declared, Some(0));
    }

    #[test]
    fn a_close_uses_the_h3_no_error_code() {
        // Shutting down is not a protocol violation, and a code that says it is
        // shows up in client logs as one.
        assert_eq!(H3_NO_ERROR, h3::error::Code::H3_NO_ERROR.value() as u32);
    }
}
