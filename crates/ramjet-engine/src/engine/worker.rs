//! One core: one reactor, its own connections, its own pool, nothing shared.
//!
//! # The shape of a request
//!
//! Four operations, which is the same four syscalls nginx makes and the same
//! four the hyper engine makes. The difference is that these are *submissions*
//! rather than syscalls: they queue into a ring and the kernel is entered once
//! for a whole batch of them, so the syscall count stops scaling with the
//! request rate. That is the entire thesis of this engine.
//!
//! ```text
//!   Read(client)  ─▶ parse head ─▶ route ─▶ take an upstream from the pool
//!                                              │
//!                    Write(upstream) ◀─────────┘
//!                    Read(upstream)  ─▶ parse head ─▶ frame the body
//!                                              │
//!                    Write(client) ◀───────────┘
//! ```
//!
//! # Two operations per descriptor
//!
//! The reactor allows one read and one write in flight per descriptor, and
//! refuses a second with `ResourceBusy` — **destroying the buffer that came
//! with it**. So this module never submits speculatively: every submission is
//! guarded by the flag that records whether that slot is occupied.
//!
//! # Stale completions
//!
//! Closing a descriptor cancels the operations on it, but their completions
//! arrive later, and by then the kernel may have handed the same descriptor
//! number to a new connection. Every submission therefore carries a generation
//! in its tag, bumped whenever a descriptor is closed; a completion whose
//! generation does not match the current one is dropped. Without this, a
//! cancelled read from a connection that ended would be delivered as input to
//! whoever inherited its number.

use std::io;
use std::net::{IpAddr, SocketAddr};
use std::os::fd::{IntoRawFd, RawFd};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use ramjet::reactor::{Completion, Driver, Op, PlatformDriver};
use ramjet_router::{RouteTable, SharedRouteTable};

use crate::codec::{
    self, parse_request_head, parse_response_head, ChunkScan, CodecError, Framing, Head, StartLine,
};
use crate::engine::pool::Pool;
use crate::engine::{Config, Intake};
use crate::headers::{self, Hop};
use crate::helper::{Helper, Note, NoteReader};
use crate::limits;
use crate::metrics::EngineMetrics;
use crate::route;
use crate::sys;

/// How much may be queued for one peer before we stop reading from the other.
///
/// This is the whole of the backpressure story: a client that will not read its
/// response stops us reading from the upstream, and the TCP window does the
/// rest. Without it a slow reader is a memory leak with extra steps.
const MAX_PENDING: usize = 256 * 1024;

/// How long a client may hold an idle keep-alive connection.
///
/// Generous, because a keep-alive connection that is being reused is the point;
/// this is only here so a client that vanishes without a FIN does not hold a
/// descriptor until the process ends.
const CLIENT_IDLE: Duration = Duration::from_secs(300);

/// Completion tag kinds.
mod kind {
    pub const ACCEPT: u8 = 1;
    pub const ADMIN_ACCEPT: u8 = 2;
    pub const INTAKE: u8 = 3;
    pub const NOTIFY: u8 = 4;
    pub const DOWN_READ: u8 = 5;
    pub const DOWN_WRITE: u8 = 6;
    pub const UP_READ: u8 = 7;
    pub const UP_WRITE: u8 = 8;
    pub const ADMIN_READ: u8 = 9;
    pub const ADMIN_WRITE: u8 = 10;
    pub const CLOSE: u8 = 11;
    /// An accept on the TLS listener.
    ///
    /// A separate kind rather than a lookup: once a descriptor has been
    /// accepted the socket no longer says which listener produced it, and the
    /// completion tag is the only thing that still knows.
    pub const ACCEPT_TLS: u8 = 12;
    /// A descriptor dealt out by the acceptor thread from the TLS listener.
    pub const INTAKE_TLS: u8 = 13;
}

/// A sampled copy of a request, being assembled while the original streams.
///
/// The head is complete the moment routing finishes; the body is not, so the
/// copy waits here until the request's body ends and is queued then. That costs
/// the primary nothing — the bytes are copied out of a buffer they were passing
/// through anyway — which is why this engine does not need the hyper engine's
/// buffer-then-dispatch step.
struct MirrorCopy {
    parts: http::request::Parts,
    body: Vec<u8>,
    /// The body went past `--mirror-max-body`, so there will be no copy.
    ///
    /// Kept as a flag rather than dropping the whole `MirrorCopy`, because a
    /// skip is a number an operator needs: "the shadow is getting nothing"
    /// and "the shadow is getting nothing *because the bodies are too big*"
    /// are different problems.
    too_large: bool,
}

/// How far the PROXY protocol header in front of a connection has got.
enum Preface {
    /// The header is complete. These are the bytes after it, which belong to
    /// whatever protocol the connection actually speaks.
    Done(Vec<u8>),
    /// A valid prefix of a header, and nothing more can be read until the rest
    /// of it arrives.
    NeedMore,
    /// Not a header. The connection is already closing.
    Rejected,
}

/// What a listener's accepts become.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Role {
    /// A data-plane connection, terminating TLS or not.
    Client { tls: bool },
    /// An admin connection: one request, one answer, closed.
    Admin,
}

impl Role {
    /// The tag kind the listener's next accept is armed with.
    fn accept_kind(self) -> u8 {
        match self {
            Role::Admin => kind::ADMIN_ACCEPT,
            Role::Client { tls: false } => kind::ACCEPT,
            Role::Client { tls: true } => kind::ACCEPT_TLS,
        }
    }
}

/// Pack a kind, a generation and a descriptor into the 64 bits a completion
/// carries back, so an operation routes itself with no lookup table.
fn tag(kind: u8, generation: u32, fd: RawFd) -> u64 {
    (u64::from(kind) << 56) | (u64::from(generation & 0x00FF_FFFF) << 32) | u64::from(fd as u32)
}

fn tag_kind(user: u64) -> u8 {
    (user >> 56) as u8
}

fn tag_generation(user: u64) -> u32 {
    ((user >> 32) & 0x00FF_FFFF) as u32
}

fn tag_fd(user: u64) -> RawFd {
    (user & 0xFFFF_FFFF) as u32 as RawFd
}

/// How much of a body is still to come, whatever frames it.
#[derive(Debug, Clone)]
enum Body {
    /// Nothing follows the head.
    Done,
    /// This many bytes follow.
    Length(u64),
    /// Chunked, forwarded verbatim; the scanner only finds the end.
    Chunked(ChunkScan),
    /// Everything until the connection closes.
    UntilClose,
}

impl Body {
    fn new(framing: Framing) -> Body {
        match framing {
            Framing::Empty => Body::Done,
            Framing::Length(n) => Body::Length(n),
            Framing::Chunked => Body::Chunked(ChunkScan::new()),
            Framing::UntilClose => Body::UntilClose,
        }
    }

    /// How many leading bytes of `input` belong to this body, and whether the
    /// body ends within them.
    fn take(&mut self, input: &[u8]) -> Result<(usize, bool), CodecError> {
        match self {
            Body::Done => Ok((0, true)),
            Body::Length(left) => {
                let take = (*left).min(input.len() as u64) as usize;
                *left -= take as u64;
                Ok((take, *left == 0))
            }
            Body::Chunked(scan) => scan.scan(input),
            // Only the peer closing ends this one, so every byte belongs to it.
            Body::UntilClose => Ok((input.len(), false)),
        }
    }

    fn is_done(&self) -> bool {
        matches!(self, Body::Done)
    }
}

/// Where a client connection is in its exchange.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase {
    /// Reading a request head.
    Head,
    /// An upstream connection is being established.
    Connecting,
    /// The request is going out and the response head has not arrived.
    Exchanging,
    /// The response head is sent; its body is streaming.
    Relaying,
    /// The upstream accepted an upgrade. There is no HTTP left in either
    /// direction, only bytes.
    Tunnel,
    /// Nothing more will be read; flush what is queued and close.
    Closing,
}

/// One client connection and the upstream exchange it is running.
struct Conn {
    peer: IpAddr,
    phase: Phase,

    /// The TLS session terminating this connection, if it arrived on the TLS
    /// listener.
    ///
    /// `None` is the plaintext path, and every branch below that tests it is
    /// the difference between the two: `inbox` and `outbox` are plaintext
    /// either way, and everything from [`Conn::parse_request`] onwards cannot
    /// tell which listener a request came in on except through
    /// [`Conn::scheme`].
    tls: Option<Box<crate::tls::Session>>,
    /// What `X-Forwarded-Proto` says: `"https"` behind TLS, `"http"` otherwise.
    scheme: &'static str,
    /// Bytes collected while a PROXY protocol header is still being read.
    ///
    /// `Some` only until the header is complete, and only when the listener was
    /// configured to expect one. The header comes before everything —
    /// ciphertext included — so this sits in front of the TLS session rather
    /// than behind it.
    preface: Option<Vec<u8>>,

    /// Bytes read from the client and not yet consumed.
    inbox: Vec<u8>,
    /// Bytes queued to write to the client.
    ///
    /// Plaintext, always. Under TLS these are handed to rustls by
    /// [`Worker::seal`] and what reaches the socket is the ciphertext it
    /// produced.
    outbox: Vec<u8>,
    reading: bool,
    writing: bool,
    client_eof: bool,

    head: Head,
    /// Whether the current request's head has been taken out of the inbox.
    ///
    /// A response the proxy invents — a 404, a 503 — still has to consume the
    /// request it is answering. Leaving it there means the next read parses the
    /// same request again and answers it the same way, for ever.
    head_consumed: bool,
    /// The rewritten request head, kept so a retry can send it again.
    request_head: Vec<u8>,
    request_body: Body,
    method_was_head: bool,
    client_keep_alive: bool,

    upstream: Option<RawFd>,
    up_inbox: Vec<u8>,
    up_outbox: Vec<u8>,
    up_reading: bool,
    up_writing: bool,
    /// The upstream has accepted at least one write, so a read may be parked.
    up_ready: bool,
    /// Handed to the helper thread; the descriptor must not be closed until it
    /// answers.
    up_borrowed: bool,
    up_head: Head,
    response_body: Body,
    response_head_seen: bool,
    upstream_keep_alive: bool,
    /// The upstream has closed its half. Only meaningful in a tunnel, where an
    /// end of stream is a half-close rather than the end of the exchange.
    upstream_eof: bool,
    /// The protocol this request asked to upgrade to, if it asked.
    ///
    /// Kept as bytes rather than parsed: this hop forwards the token and does
    /// not need to know what it means. A `None` here on a 101 response means
    /// the upstream switched protocols nobody asked it to.
    upgrade: Option<Vec<u8>>,

    /// The table this request was routed against, held so retries see the same
    /// endpoints a mid-request publish cannot change.
    snapshot: Option<Arc<RouteTable>>,
    /// The matched rule's index into the table's per-route counters.
    ///
    /// `None` when the default backend answered: there is no rule to attribute
    /// the request to, and inventing one would put traffic against a route the
    /// table does not contain.
    route_stats: Option<u32>,
    /// Whether the canary took this request, which decides whether the route's
    /// canary block is written as well as its own.
    canaried: bool,
    /// A copy of this request waiting for its body, when the rule's mirror
    /// sampled it.
    mirror: Option<MirrorCopy>,
    stats_index: u32,
    endpoint_index: usize,
    /// How many endpoints the backend has.
    ///
    /// Not the same as `targets.len()`, which is capped at the attempt limit.
    /// The in-flight counters are indexed by position in the backend, so using
    /// the shorter length would attribute a request to the wrong endpoint
    /// whenever a backend has more endpoints than a request may try.
    endpoint_count: usize,
    inflight_held: bool,
    targets: Vec<SocketAddr>,
    attempt: usize,
    /// Whether this request can be sent again at all.
    ///
    /// True only when it has no body. A request with bytes to send cannot be
    /// replayed: the first attempt may already have written some of them, and
    /// nothing buffers them for a second try. The endpoint-failover path gets
    /// this for free — `attempts()` gives a body-carrying request exactly one
    /// target — but the pooled-connection retry below bypasses that count, so
    /// it has to ask directly.
    replayable: bool,
    /// The current upstream came out of the pool, so losing it before the
    /// response starts is a race rather than an endpoint failure.
    from_pool: bool,
    /// Whether the one free retry that race is owed has been used.
    pool_retry_used: bool,
    /// The client has been told a status, so nothing may be retried.
    ///
    /// A flag rather than a sentinel attempt number: `attempt` is added to an
    /// endpoint index, and a sentinel large enough to be unmistakable is also
    /// large enough to overflow that sum.
    committed: bool,
    dispatched_at: Option<Instant>,

    deadline: Option<Instant>,
    /// Set once the response has been counted, so a connection that fails after
    /// its response is not counted twice.
    counted: bool,
}

impl Conn {
    fn new(peer: IpAddr, tls: Option<Box<crate::tls::Session>>) -> Conn {
        Conn {
            peer,
            phase: Phase::Head,
            scheme: if tls.is_some() { "https" } else { "http" },
            tls,
            preface: None,
            inbox: Vec::new(),
            outbox: Vec::new(),
            reading: false,
            writing: false,
            client_eof: false,
            head: Head::default(),
            head_consumed: false,
            request_head: Vec::new(),
            request_body: Body::Done,
            method_was_head: false,
            client_keep_alive: true,
            upstream: None,
            up_inbox: Vec::new(),
            up_outbox: Vec::new(),
            up_reading: false,
            up_writing: false,
            up_ready: false,
            up_borrowed: false,
            up_head: Head::default(),
            response_body: Body::Done,
            response_head_seen: false,
            upstream_keep_alive: true,
            upstream_eof: false,
            upgrade: None,
            snapshot: None,
            route_stats: None,
            canaried: false,
            mirror: None,
            stats_index: 0,
            endpoint_index: 0,
            endpoint_count: 0,
            inflight_held: false,
            targets: Vec::new(),
            attempt: 0,
            replayable: false,
            from_pool: false,
            pool_retry_used: false,
            committed: false,
            dispatched_at: None,
            deadline: None,
            counted: false,
        }
    }

    /// Ready this connection for the next request on it, keeping any bytes that
    /// already arrived — which is how pipelining works without a special case.
    fn reset(&mut self) {
        self.phase = Phase::Head;
        self.head.reset();
        self.head_consumed = false;
        self.up_head.reset();
        self.request_head.clear();
        self.request_body = Body::Done;
        self.response_body = Body::Done;
        self.response_head_seen = false;
        self.upstream_keep_alive = true;
        self.upstream_eof = false;
        self.upgrade = None;
        self.method_was_head = false;
        self.snapshot = None;
        self.route_stats = None;
        self.canaried = false;
        self.mirror = None;
        self.targets.clear();
        self.attempt = 0;
        self.replayable = false;
        self.from_pool = false;
        self.pool_retry_used = false;
        self.committed = false;
        self.dispatched_at = None;
        self.deadline = None;
        self.counted = false;
        self.up_inbox.clear();
        self.up_outbox.clear();
    }

    /// Whether more bytes from the client would be useful right now.
    fn wants_client_bytes(&self) -> bool {
        if self.client_eof || self.phase == Phase::Closing {
            return false;
        }
        match self.phase {
            // Reading ahead for a pipelined request while a response is still
            // streaming would buy nothing: it could not be started until this
            // one finishes, and it would need somewhere to live meanwhile.
            Phase::Head => true,
            Phase::Connecting | Phase::Exchanging => {
                !self.request_body.is_done() && self.up_outbox.len() < MAX_PENDING
            }
            // A tunnel reads from both ends for as long as both are open; the
            // only thing that stops it is the other side falling behind.
            Phase::Tunnel => self.up_outbox.len() < MAX_PENDING,
            Phase::Relaying | Phase::Closing => false,
        }
    }

    /// Whether more bytes from the upstream would be useful right now.
    fn wants_upstream_bytes(&self) -> bool {
        self.upstream.is_some()
            && self.up_ready
            && !matches!(self.phase, Phase::Head | Phase::Closing)
            && !self.upstream_eof
            && pending_out_len(self) < MAX_PENDING
    }
}

/// What a descriptor is being used for. One vector indexed by descriptor
/// number, so routing a completion is an index rather than a hash.
enum Slot {
    Empty,
    /// A client connection, temporarily moved out while it is being driven.
    Taken,
    Client(Box<Conn>),
    /// An upstream in use by the client connection on this descriptor.
    Upstream(RawFd),
    /// An idle pooled upstream to this endpoint.
    Idle(SocketAddr),
    /// An upstream whose client has gone, kept alive only until the helper
    /// thread hands the descriptor back.
    Abandoned,
    Admin(Box<AdminConn>),
}

/// An admin connection: read one request, answer it, close.
///
/// Deliberately not a keep-alive server. A scrape every fifteen seconds does
/// not need connection reuse, and a second state machine on the same reactor
/// would be a second thing to get wrong for no measurable gain.
struct AdminConn {
    fd: RawFd,
    inbox: Vec<u8>,
    outbox: Vec<u8>,
    head: Head,
    reading: bool,
    writing: bool,
}

/// One serving core.
pub(crate) struct Worker {
    core: usize,
    driver: PlatformDriver,
    config: Arc<Config>,
    routes: Arc<SharedRouteTable>,
    metrics: Arc<EngineMetrics>,
    readiness: Arc<AtomicBool>,
    shutdown: Arc<AtomicBool>,
    helper: Arc<Helper>,

    /// This core's listeners: a plaintext one, a TLS one, or both.
    intakes: Vec<Intake>,
    admin: Option<RawFd>,
    notify: RawFd,
    notes: NoteReader,
    /// Partially received descriptor numbers from the acceptor thread, one
    /// buffer per intake channel.
    intake_partial: Vec<(RawFd, Vec<u8>)>,

    slots: Vec<Slot>,
    generations: Vec<u32>,
    high_water: usize,
    pool: Pool,

    /// `--max-buf-size`, clamped, resolved once rather than read through the
    /// `Arc<Config>` on every read.
    read_buf_size: usize,
    read_bufs: Vec<Vec<u8>>,
    write_bufs: Vec<Vec<u8>>,
    done: Vec<Completion>,
    scratch: Vec<RawFd>,
}

impl Worker {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        core: usize,
        config: Arc<Config>,
        routes: Arc<SharedRouteTable>,
        metrics: Arc<EngineMetrics>,
        readiness: Arc<AtomicBool>,
        shutdown: Arc<AtomicBool>,
        helper: Arc<Helper>,
        intakes: Vec<Intake>,
        admin: Option<RawFd>,
        notify: RawFd,
    ) -> io::Result<Worker> {
        Ok(Worker {
            core,
            driver: PlatformDriver::new()?,
            pool: Pool::new(config.pool_max_idle_per_host, config.pool_idle_timeout),
            read_buf_size: config.max_buf_size.max(crate::engine::MIN_BUF_SIZE),
            config,
            routes,
            metrics,
            readiness,
            shutdown,
            intakes,
            helper,
            admin,
            notify,
            notes: NoteReader::default(),
            intake_partial: Vec::new(),
            slots: Vec::new(),
            generations: Vec::new(),
            high_water: 0,
            read_bufs: Vec::new(),
            write_bufs: Vec::new(),
            done: Vec::new(),
            scratch: Vec::new(),
        })
    }

    /// Drive this core until shutdown.
    pub(crate) fn run(&mut self) -> io::Result<()> {
        self.arm_notify()?;
        for index in 0..self.intakes.len() {
            let (source, tls) = {
                let intake = &self.intakes[index];
                (intake.fd(), intake.tls)
            };
            match self.intakes[index].source {
                crate::engine::Source::Listener(_) => {
                    let k = if tls { kind::ACCEPT_TLS } else { kind::ACCEPT };
                    self.arm_accept(source, k)?;
                }
                crate::engine::Source::Channel(_) => {
                    self.intake_partial.push((source, Vec::new()));
                    self.arm_intake(source, tls)?;
                }
            }
        }
        if let Some(fd) = self.admin {
            self.arm_accept(fd, kind::ADMIN_ACCEPT)?;
        }

        let mut done = std::mem::take(&mut self.done);
        while !self.shutdown.load(Ordering::Relaxed) {
            self.driver.wait(&mut done)?;
            if done.is_empty() {
                // Nothing in flight at all. The notify pipe is always armed, so
                // this only happens if that read failed; waiting again would
                // spin.
                break;
            }
            for completion in done.drain(..) {
                self.dispatch(completion)?;
            }
        }
        self.done = done;
        self.teardown();
        Ok(())
    }

    // ---- submission helpers -------------------------------------------------

    fn generation(&mut self, fd: RawFd) -> u32 {
        let i = fd as usize;
        if i >= self.generations.len() {
            self.generations.resize(i + 1, 0);
        }
        self.generations[i]
    }

    fn bump_generation(&mut self, fd: RawFd) {
        let i = fd as usize;
        if i >= self.generations.len() {
            self.generations.resize(i + 1, 0);
        }
        self.generations[i] = self.generations[i].wrapping_add(1) & 0x00FF_FFFF;
    }

    fn slot(&mut self, fd: RawFd) -> &mut Slot {
        let i = fd as usize;
        if i >= self.slots.len() {
            self.slots.resize_with(i + 1, || Slot::Empty);
        }
        self.high_water = self.high_water.max(i + 1);
        &mut self.slots[i]
    }

    /// A read buffer, and so the most one completion can deliver.
    ///
    /// Its size is `--max-buf-size`, read off the configuration rather than a
    /// constant so the two engines are actually running the same bound when a
    /// benchmark compares them.
    ///
    /// These are allocated once and **never truncated**: a buffer is handed to
    /// the reactor at its full length, comes back at its full length, and the
    /// bytes that arrived are copied out. Keeping them full-length is what
    /// avoids a `memset` per read, which at this request rate would cost more
    /// than the copy it replaced.
    fn read_buf(&mut self) -> Vec<u8> {
        let size = self.read_buf_size;
        self.read_bufs.pop().unwrap_or_else(|| vec![0u8; size])
    }

    /// Return a read buffer, restoring its full length without a `memset`.
    ///
    /// A read buffer is only ever handed out at its full size and comes back
    /// the same length — the reactor returns a caller-supplied buffer at the
    /// caller's length, not trimmed — so this is a no-op in the normal case and
    /// a cheap guard against a future change that is not.
    fn recycle_read(&mut self, mut buf: Vec<u8>) {
        if buf.capacity() < self.read_buf_size || self.read_bufs.len() >= 8 {
            return;
        }
        if buf.len() != self.read_buf_size {
            buf.resize(self.read_buf_size, 0);
        }
        self.read_bufs.push(buf);
    }

    fn write_buf(&mut self) -> Vec<u8> {
        self.write_bufs.pop().unwrap_or_default()
    }

    fn recycle_write(&mut self, mut buf: Vec<u8>) {
        if self.write_bufs.len() >= 8 || buf.capacity() > MAX_PENDING {
            return;
        }
        buf.clear();
        self.write_bufs.push(buf);
    }

    fn arm_notify(&mut self) -> io::Result<()> {
        let generation = self.generation(self.notify);
        let buf = self.read_buf();
        let user = tag(kind::NOTIFY, generation, self.notify);
        self.driver.submit_with(
            Op::Read {
                fd: self.notify,
                buf,
            },
            user,
        )?;
        Ok(())
    }

    fn arm_accept(&mut self, fd: RawFd, k: u8) -> io::Result<()> {
        let generation = self.generation(fd);
        self.driver
            .submit_with(Op::Accept { fd }, tag(k, generation, fd))?;
        Ok(())
    }

    fn arm_intake(&mut self, fd: RawFd, tls: bool) -> io::Result<()> {
        let generation = self.generation(fd);
        let buf = self.read_buf();
        let k = if tls { kind::INTAKE_TLS } else { kind::INTAKE };
        self.driver
            .submit_with(Op::Read { fd, buf }, tag(k, generation, fd))?;
        Ok(())
    }

    /// Close a descriptor through the reactor and forget everything about it.
    fn close(&mut self, fd: RawFd) {
        self.bump_generation(fd);
        *self.slot(fd) = Slot::Empty;
        let generation = self.generation(fd);
        // A failure here means the operation was never queued, and the
        // descriptor would leak — but there is nothing better to do with it
        // than carry on serving.
        let _ = self
            .driver
            .submit_with(Op::Close { fd }, tag(kind::CLOSE, generation, fd));
    }

    // ---- completion dispatch ------------------------------------------------

    fn dispatch(&mut self, c: Completion) -> io::Result<()> {
        let k = tag_kind(c.user);
        let fd = tag_fd(c.user);

        // A completion for a descriptor that has since been closed — the
        // cancellation of an operation that was in flight — must not be
        // delivered to whoever inherited the number.
        if k != kind::CLOSE && tag_generation(c.user) != self.generation(fd) {
            if let Some(buf) = c.buf {
                self.recycle_read(buf);
            }
            return Ok(());
        }

        match k {
            kind::ACCEPT => self.on_accept(fd, c, Role::Client { tls: false }),
            kind::ACCEPT_TLS => self.on_accept(fd, c, Role::Client { tls: true }),
            kind::ADMIN_ACCEPT => self.on_accept(fd, c, Role::Admin),
            kind::INTAKE => self.on_intake(fd, c, false),
            kind::INTAKE_TLS => self.on_intake(fd, c, true),
            kind::NOTIFY => self.on_notify(fd, c),
            kind::DOWN_READ => self.on_client_read(fd, c),
            kind::DOWN_WRITE => self.on_client_write(fd, c),
            kind::UP_READ => self.on_upstream_read(fd, c),
            kind::UP_WRITE => self.on_upstream_write(fd, c),
            kind::ADMIN_READ => self.on_admin_read(fd, c),
            kind::ADMIN_WRITE => self.on_admin_write(fd, c),
            _ => {
                if let Some(buf) = c.buf {
                    self.recycle_read(buf);
                }
                Ok(())
            }
        }
    }

    fn on_accept(&mut self, listener: RawFd, c: Completion, role: Role) -> io::Result<()> {
        let accepted = match c.result {
            Ok(fd) => Some(fd as RawFd),
            Err(ref e) => {
                match e.raw_os_error() {
                    // The pending connection died before we took it, which says
                    // nothing about the listener. macOS reports this as EINVAL.
                    Some(
                        libc::ECONNABORTED | libc::ECONNRESET | libc::EINVAL | libc::EINTR,
                    ) => None,
                    // Out of descriptors. Re-arming immediately would spin; the
                    // next tick will try again once something has closed.
                    Some(libc::EMFILE | libc::ENFILE | libc::ENOMEM | libc::ENOBUFS) => {
                        return Ok(())
                    }
                    _ => return Ok(()),
                }
            }
        };

        self.arm_accept(listener, role.accept_kind())?;

        let Some(fd) = accepted else { return Ok(()) };
        match role {
            Role::Admin => self.start_admin(fd),
            Role::Client { tls } => self.start_client(fd, tls),
        }
    }

    /// Descriptors dealt out by the acceptor thread, four bytes each.
    ///
    /// Used only where `SO_REUSEPORT` does not distribute connections — macOS,
    /// where the last socket to bind receives all of them.
    fn on_intake(&mut self, fd: RawFd, c: Completion, tls: bool) -> io::Result<()> {
        let Some(buf) = c.buf else { return Ok(()) };
        match c.result {
            Ok(n) if n > 0 => {
                let mut ready = Vec::new();
                if let Some((_, partial)) = self
                    .intake_partial
                    .iter_mut()
                    .find(|(channel, _)| *channel == fd)
                {
                    partial.extend_from_slice(&buf[..n as usize]);
                    while partial.len() >= 4 {
                        let mut number = [0u8; 4];
                        number.copy_from_slice(&partial[..4]);
                        partial.drain(..4);
                        ready.push(i32::from_le_bytes(number));
                    }
                }
                self.recycle_read(buf);
                for accepted in ready {
                    self.start_client(accepted, tls)?;
                }
                self.arm_intake(fd, tls)
            }
            // The acceptor is gone, and with it any reason for this core to be.
            _ => {
                self.recycle_read(buf);
                self.shutdown.store(true, Ordering::Relaxed);
                Ok(())
            }
        }
    }

    fn on_notify(&mut self, fd: RawFd, c: Completion) -> io::Result<()> {
        let Some(buf) = c.buf else { return Ok(()) };
        let mut notes = Vec::new();
        match c.result {
            Ok(n) if n > 0 => {
                let mut reader = std::mem::take(&mut self.notes);
                reader.feed(&buf[..n as usize], |note| notes.push(note));
                self.notes = reader;
                self.recycle_read(buf);
            }
            _ => {
                self.recycle_read(buf);
                return Ok(());
            }
        }
        for note in notes {
            match note {
                Note::Tick => self.on_tick()?,
                Note::Connected { fd, err } => self.on_connected(fd, err)?,
            }
        }
        let _ = fd;
        self.arm_notify()
    }

    // ---- client connections -------------------------------------------------

    fn start_client(&mut self, fd: RawFd, tls: bool) -> io::Result<()> {
        // The io_uring backend accepts without O_NONBLOCK and applies only
        // TCP_NODELAY; a blocking descriptor would stall this whole core on its
        // first syscall, so both are set here rather than assumed.
        if sys::set_nonblocking(fd).is_err() {
            // SAFETY: nothing has been submitted for this descriptor yet.
            unsafe { sys::close(fd) };
            return Ok(());
        }
        let _ = sys::set_nodelay(fd);
        let peer = sys::peer_addr(fd)
            .map(|a| a.ip())
            .unwrap_or(IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED));

        self.bump_generation(fd);
        let session = tls.then(|| Box::new(crate::tls::Session::new()));
        let mut conn = Conn::new(peer, session);
        conn.deadline = Some(Instant::now() + CLIENT_IDLE);
        if let Some(timeout) = self.config.proxy_protocol {
            // Before the TLS record layer and before HTTP: the header is the
            // very first thing on the connection, and everything after it —
            // including the ClientHello — belongs to the protocol above.
            conn.preface = Some(Vec::new());
            // A much shorter clock than the idle one. A load balancer sends the
            // header in the same segment as the connection; anything taking
            // seconds over it is not a load balancer.
            conn.deadline = Some(Instant::now() + timeout);
        }
        *self.slot(fd) = Slot::Client(Box::new(conn));
        self.metrics.core(self.core).connection_opened();
        self.drive(fd)
    }

    fn on_client_read(&mut self, fd: RawFd, c: Completion) -> io::Result<()> {
        let Some(buf) = c.buf else { return Ok(()) };
        let Slot::Client(mut conn) = std::mem::replace(self.slot(fd), Slot::Taken) else {
            *self.slot(fd) = Slot::Empty;
            self.recycle_read(buf);
            return Ok(());
        };
        conn.reading = false;
        match c.result {
            Ok(n) if n > 0 => {
                self.ingest(&mut conn, &buf[..n as usize]);
                // The idle clock is only restarted once the connection has got
                // past its PROXY header. Restarting it first would let a sender
                // dribbling one byte at a time hold a descriptor for as long as
                // it liked, which is exactly what the header's own deadline is
                // there to stop.
                if conn.preface.is_none() {
                    conn.deadline = Some(Instant::now() + CLIENT_IDLE);
                }
            }
            Ok(_) => {
                conn.client_eof = true;
                // A TLS peer that vanished without a close_notify has truncated
                // whatever it was sending. There is nothing left to decrypt, so
                // the session is finished either way.
                if let Some(session) = conn.tls.as_mut() {
                    if !session.established() {
                        // A handshake that never completed is a failure worth
                        // counting: an operator watching the handshake rate is
                        // watching for exactly this.
                        self.metrics.core(self.core).tls_handshake_failure();
                        session.kill();
                    }
                }
            }
            Err(_) => {
                conn.client_eof = true;
                // A reset client has nothing to receive, so there is no point
                // flushing anything to it.
                conn.outbox.clear();
                if let Some(session) = conn.tls.as_mut() {
                    session.wire_out().clear();
                    session.kill();
                }
                conn.phase = Phase::Closing;
            }
        }
        self.recycle_read(buf);
        *self.slot(fd) = Slot::Client(conn);
        self.advance(fd)
    }

    /// Bytes off the client's socket, into its plaintext inbox.
    ///
    /// Every layer this connection sits behind is unwound here and nowhere
    /// else: the PROXY header first, then the TLS record layer, and what comes
    /// out the far side is what [`Worker::parse_request`] would have read
    /// straight off a plaintext socket. That is the whole reason the state
    /// machine below needed no second version for TLS.
    fn ingest(&mut self, conn: &mut Conn, bytes: &[u8]) {
        // Owned, because the surplus after a PROXY header lives in a buffer
        // this function is about to drop. Allocated once per connection and
        // only when the header feature is on.
        let leftover;
        let mut rest = bytes;

        if conn.preface.is_some() {
            match self.consume_preface(conn, rest) {
                Preface::Done(surplus) => {
                    leftover = surplus;
                    rest = &leftover;
                }
                // Either the header is incomplete — nothing else can be read
                // until it finishes — or it was rejected and the connection is
                // already closing.
                Preface::NeedMore | Preface::Rejected => return,
            }
        }

        let Some(session) = conn.tls.as_mut() else {
            conn.inbox.extend_from_slice(rest);
            return;
        };

        let mut plain = std::mem::take(&mut conn.inbox);
        let outcome = session.feed(rest, &mut plain);
        conn.inbox = plain;

        match outcome {
            Ok(crate::tls::Step::NeedMore) => {}
            Ok(crate::tls::Step::Hello(_)) => {
                // v1 of the TLS lane serves every ClientHello it is offered:
                // ALPN is `http/1.1` only, so an HTTP/2 client never negotiates
                // a protocol this engine cannot speak. Dispatch is what turns
                // this into a decision.
                let Some(config) = self.config.tls.clone() else {
                    self.metrics.core(self.core).tls_handshake_failure();
                    session.kill();
                    conn.phase = Phase::Closing;
                    return;
                };
                let mut plain = std::mem::take(&mut conn.inbox);
                let accepted = session.accept(&config, &mut plain);
                conn.inbox = plain;
                if accepted.is_err() {
                    self.metrics.core(self.core).tls_handshake_failure();
                    conn.phase = Phase::Closing;
                    return;
                }
            }
            Ok(crate::tls::Step::Live) => {}
            Err(_) => {
                // rustls has queued an alert describing the failure; the write
                // path flushes it before the connection goes.
                self.metrics.core(self.core).tls_handshake_failure();
                session.kill();
                conn.phase = Phase::Closing;
                return;
            }
        }

        if conn.tls.as_mut().is_some_and(|s| s.take_established()) {
            self.metrics.core(self.core).tls_handshake();
        }
    }

    /// Read a PROXY protocol header off the front of the connection.
    ///
    /// The same parser the hyper engine uses, and the same trust model: the
    /// header is **required**, and a connection whose first bytes are not one
    /// is dropped without an answer. A permissive fallback to the socket's own
    /// address would let a sender choose per connection whether to be spoofed,
    /// which is strictly worse than either fixed answer.
    ///
    /// Dropping rather than answering is deliberate too. At this point nothing
    /// is known about what the peer speaks — it may be mid-ClientHello — so an
    /// HTTP error would be bytes into a stream that is not HTTP.
    fn consume_preface(&mut self, conn: &mut Conn, bytes: &[u8]) -> Preface {
        use ramjet_proxy::proxy_protocol::{parse, Parsed};

        let Some(preface) = conn.preface.as_mut() else {
            return Preface::Done(Vec::new());
        };
        preface.extend_from_slice(bytes);

        match parse(preface) {
            Ok(Parsed::Incomplete) => Preface::NeedMore,
            Ok(Parsed::Done { consumed, client }) => {
                // A header that names nobody — a v2 LOCAL command, which is
                // what a load balancer's own health check sends — is a success
                // that leaves the socket's peer address standing.
                if let Some(client) = client {
                    conn.peer = client.ip();
                }
                let surplus = preface.get(consumed..).unwrap_or(&[]).to_vec();
                conn.preface = None;
                // Off the header's short clock and onto the ordinary idle one.
                conn.deadline = Some(Instant::now() + CLIENT_IDLE);
                Preface::Done(surplus)
            }
            Err(error) => {
                tracing::debug!(%error, peer = %conn.peer, "refused a connection with no valid PROXY header");
                conn.outbox.clear();
                conn.phase = Phase::Closing;
                conn.client_eof = true;
                Preface::Rejected
            }
        }
    }

    fn on_client_write(&mut self, fd: RawFd, c: Completion) -> io::Result<()> {
        if let Some(buf) = c.buf {
            self.recycle_write(buf);
        }
        let Slot::Client(mut conn) = std::mem::replace(self.slot(fd), Slot::Taken) else {
            *self.slot(fd) = Slot::Empty;
            return Ok(());
        };
        conn.writing = false;
        if c.result.is_err() {
            conn.outbox.clear();
            conn.phase = Phase::Closing;
        }
        *self.slot(fd) = Slot::Client(conn);
        self.advance(fd)
    }

    fn on_upstream_read(&mut self, up: RawFd, c: Completion) -> io::Result<()> {
        let Some(buf) = c.buf else { return Ok(()) };
        let owner = match self.slot(up) {
            Slot::Upstream(down) => Some(*down),
            Slot::Idle(addr) => {
                // A pooled connection spoke, which for an idle connection means
                // the far end closed it or sent something it had no business
                // sending. Either way it is not reusable.
                let addr = *addr;
                self.recycle_read(buf);
                self.pool.remove(addr, up);
                self.close(up);
                return Ok(());
            }
            Slot::Abandoned => {
                self.recycle_read(buf);
                return Ok(());
            }
            _ => None,
        };
        let Some(down) = owner else {
            self.recycle_read(buf);
            return Ok(());
        };
        let Slot::Client(mut conn) = std::mem::replace(self.slot(down), Slot::Taken) else {
            *self.slot(down) = Slot::Empty;
            self.recycle_read(buf);
            return Ok(());
        };
        conn.up_reading = false;
        let outcome = match c.result {
            Ok(n) if n > 0 => {
                conn.up_inbox.extend_from_slice(&buf[..n as usize]);
                Ok(false)
            }
            Ok(_) => Ok(true),
            Err(e) => Err(e),
        };
        self.recycle_read(buf);
        *self.slot(down) = Slot::Client(conn);

        match outcome {
            Ok(false) => self.advance(down),
            Ok(true) => self.on_upstream_eof(down),
            Err(_) => self.upstream_failed(down),
        }
    }

    fn on_upstream_write(&mut self, up: RawFd, c: Completion) -> io::Result<()> {
        if let Some(buf) = c.buf {
            self.recycle_write(buf);
        }
        let Slot::Upstream(down) = *self.slot(up) else {
            return Ok(());
        };
        let Slot::Client(mut conn) = std::mem::replace(self.slot(down), Slot::Taken) else {
            *self.slot(down) = Slot::Empty;
            return Ok(());
        };
        conn.up_writing = false;
        let failed = c.result.is_err();
        if !failed {
            conn.up_ready = true;
        }
        *self.slot(down) = Slot::Client(conn);
        if failed {
            return self.upstream_failed(down);
        }
        self.advance(down)
    }

    /// The upstream closed. Whether that is an answer or a failure depends on
    /// how the response was framed.
    fn on_upstream_eof(&mut self, down: RawFd) -> io::Result<()> {
        let Slot::Client(mut conn) = std::mem::replace(self.slot(down), Slot::Taken) else {
            *self.slot(down) = Slot::Empty;
            return Ok(());
        };
        // Inside a tunnel an end of stream is one peer hanging up, not a
        // truncated response: there is no response any more.
        if conn.phase == Phase::Tunnel {
            conn.upstream_eof = true;
            conn.up_reading = false;
            *self.slot(down) = Slot::Client(conn);
            return self.advance(down);
        }

        let complete = conn.response_head_seen && matches!(conn.response_body, Body::UntilClose);
        if complete {
            // A response with no framing header ends exactly here, and the
            // remaining bytes are all body.
            let rest = std::mem::take(&mut conn.up_inbox);
            conn.outbox.extend_from_slice(&rest);
            conn.up_inbox = rest;
            conn.up_inbox.clear();
            conn.response_body = Body::Done;
            // Such a connection cannot be pooled: its framing *was* the close.
            conn.upstream_keep_alive = false;
            *self.slot(down) = Slot::Client(conn);
            self.finish_response(down)?;
            // `finish_response` only readies the connection; something still
            // has to submit the write it left queued. On the ordinary path
            // `advance` does that on its next turn round the loop, and this
            // path has no next turn.
            return self.advance(down);
        }
        *self.slot(down) = Slot::Client(conn);
        self.upstream_failed(down)
    }

    // ---- the state machine --------------------------------------------------

    /// Move a connection as far forward as its buffers allow, then submit
    /// whatever operations that leaves pending.
    fn advance(&mut self, fd: RawFd) -> io::Result<()> {
        loop {
            let Slot::Client(conn) = self.slot(fd) else {
                return Ok(());
            };
            let progressed = match conn.phase {
                Phase::Head => self.parse_request(fd)?,
                Phase::Connecting => false,
                Phase::Exchanging | Phase::Relaying => self.pump_bodies(fd)?,
                Phase::Tunnel => self.pump_tunnel(fd)?,
                Phase::Closing => false,
            };
            if !progressed {
                break;
            }
        }
        self.drive(fd)
    }

    /// Try to read one complete request head out of the inbox.
    fn parse_request(&mut self, fd: RawFd) -> io::Result<bool> {
        let Slot::Client(mut conn) = std::mem::replace(self.slot(fd), Slot::Taken) else {
            *self.slot(fd) = Slot::Empty;
            return Ok(false);
        };

        // The HTTP/2 connection preface. Detected before parsing so the refusal
        // names the real reason instead of "unsupported version".
        if conn.inbox.starts_with(b"PRI * HTTP/2.0") {
            *self.slot(fd) = Slot::Client(conn);
            self.fail(fd, 502, limits::NO_HTTP2, true);
            return Ok(false);
        }

        let parsed = {
            let inbox = std::mem::take(&mut conn.inbox);
            let outcome = parse_request_head(&inbox, &mut conn.head);
            conn.inbox = inbox;
            outcome
        };
        match parsed {
            Ok(false) => {
                if conn.client_eof {
                    // A half-sent request with the client gone.
                    conn.phase = Phase::Closing;
                }
                *self.slot(fd) = Slot::Client(conn);
                Ok(false)
            }
            Err(e) => {
                *self.slot(fd) = Slot::Client(conn);
                let body = limits::bad_request_body(e.status(), e.detail());
                self.fail(fd, e.status(), &body, true);
                Ok(false)
            }
            Ok(true) => {
                *self.slot(fd) = Slot::Client(conn);
                self.route_request(fd)?;
                Ok(false)
            }
        }
    }

    /// Route a parsed request and start the upstream exchange.
    fn route_request(&mut self, fd: RawFd) -> io::Result<()> {
        let Slot::Client(mut conn) = std::mem::replace(self.slot(fd), Slot::Taken) else {
            *self.slot(fd) = Slot::Empty;
            return Ok(());
        };

        let framing = match codec::request_framing(&conn.head, &conn.inbox) {
            Ok(framing) => framing,
            Err(e) => {
                *self.slot(fd) = Slot::Client(conn);
                let body = limits::bad_request_body(e.status(), e.detail());
                self.fail(fd, e.status(), &body, true);
                return Ok(());
            }
        };
        // Set before any refusal below, because whether this request has a body
        // decides whether the connection can survive being refused.
        conn.request_body = Body::new(framing);
        conn.client_keep_alive =
            codec::keep_alive(&conn.head, &conn.inbox, conn.head.version()) && !conn.client_eof;
        conn.method_was_head = conn
            .head
            .method(&conn.inbox)
            .is_some_and(|m| m.eq_ignore_ascii_case(b"HEAD"));

        // An upgrade is forwarded, not answered. Whether it becomes a tunnel is
        // the upstream's decision, made when its response head arrives: a
        // backend that answers 200 to a WebSocket handshake is a backend that
        // does not speak WebSocket, and that answer belongs to the client.
        conn.upgrade = headers::upgrade_protocol(&conn.head, &conn.inbox).map(<[u8]>::to_vec);

        // One snapshot, taken once, so nothing below can see two configurations.
        let snapshot = self.routes.load_full();
        let host = headers::routing_host(&conn.head, &conn.inbox).unwrap_or("");
        let StartLine::Request { target, .. } = conn.head.start else {
            *self.slot(fd) = Slot::Client(conn);
            self.fail(fd, 400, limits::NO_ROUTE, true);
            return Ok(());
        };
        let path = headers::routing_path(target, &conn.inbox).unwrap_or("/");

        let Some(matched) = route::match_request(&snapshot, host, path, &conn.head, &conn.inbox)
        else {
            self.metrics.core(self.core).route_miss();
            *self.slot(fd) = Slot::Client(conn);
            self.fail(fd, 404, limits::NO_ROUTE, false);
            return Ok(());
        };
        let backend = matched.backend;
        // Recorded on the connection rather than kept as a borrow: the snapshot
        // is held for the life of the request, but this state machine hands the
        // connection back to a slot between completions and cannot carry a
        // reference across that. The indices are stable within the snapshot, so
        // looking the counters up again at record time reaches the same block.
        //
        // A request answered by the default backend has no rule and so is
        // counted only in the process-wide series: it matched nothing, and
        // attributing it to a route would invent one that is not in the table.
        conn.route_stats = matched.route;
        conn.canaried = matched.canaried;

        // Checked after routing, exactly as the hyper engine does, so an
        // unrouted gRPC request is a 404 rather than a 502.
        if headers::is_grpc(&conn.head, &conn.inbox) {
            *self.slot(fd) = Slot::Client(conn);
            self.fail(fd, 502, limits::GRPC, false);
            return Ok(());
        }

        let Some(first) = route::first_endpoint(&snapshot, backend) else {
            *self.slot(fd) = Slot::Client(conn);
            self.fail(fd, 503, limits::NO_ENDPOINT, false);
            return Ok(());
        };

        let allowed = route::attempts(
            framing,
            backend.endpoints().len(),
            self.config.max_connect_attempts,
        );
        conn.targets.clear();
        for attempt in 0..allowed {
            if let Some(endpoint) = route::endpoint_at(backend, first.index, attempt) {
                conn.targets.push(endpoint.addr);
            }
        }
        if conn.targets.is_empty() {
            *self.slot(fd) = Slot::Client(conn);
            self.fail(fd, 503, limits::NO_ENDPOINT, false);
            return Ok(());
        }
        conn.stats_index = backend.stats_index();
        conn.endpoint_index = first.index;
        conn.endpoint_count = backend.endpoints().len();
        conn.replayable = framing == Framing::Empty;
        conn.attempt = 0;

        // The head is rewritten before the inbox is drained, because the parsed
        // head borrows from it.
        let hop = Hop {
            client: conn.peer,
            scheme: conn.scheme,
        };
        conn.request_head.clear();
        {
            let inbox = std::mem::take(&mut conn.inbox);
            let mut head_out = std::mem::take(&mut conn.request_head);
            let upgrade = conn.upgrade.clone();
            headers::write_upstream_request(
                &mut head_out,
                &conn.head,
                &inbox,
                hop,
                conn.targets[0],
                framing,
                false,
                upgrade.as_deref(),
            );
            conn.request_head = head_out;
            conn.inbox = inbox;
        }
        // Mirrored after the forwarded headers are on and before the request
        // target is rewritten to an endpoint: the copy carries exactly the
        // headers the real backend will see, and its own URI.
        self.plan_mirror(&mut conn, &snapshot, matched.mirror, framing);
        // A request with no body is complete the moment its head is, which is
        // the overwhelming majority of ingress traffic and the case that costs
        // nothing at all.
        if conn.request_body.is_done() {
            self.send_mirror(&mut conn);
        }

        let consumed = conn.head.len;
        conn.inbox.drain(..consumed);
        conn.head_consumed = true;
        conn.snapshot = Some(snapshot);
        conn.phase = Phase::Exchanging;

        conn.up_outbox.clear();
        let head = std::mem::take(&mut conn.request_head);
        conn.up_outbox.extend_from_slice(&head);
        conn.request_head = head;

        *self.slot(fd) = Slot::Client(conn);
        self.open_upstream(fd)
    }

    /// Take or open a connection to the current attempt's endpoint.
    fn open_upstream(&mut self, fd: RawFd) -> io::Result<()> {
        let Slot::Client(mut conn) = std::mem::replace(self.slot(fd), Slot::Taken) else {
            *self.slot(fd) = Slot::Empty;
            return Ok(());
        };
        let Some(&addr) = conn.targets.get(conn.attempt) else {
            *self.slot(fd) = Slot::Client(conn);
            return self.exhausted(fd);
        };

        conn.dispatched_at = Some(Instant::now());
        conn.deadline = Some(Instant::now() + self.config.response_timeout);
        self.hold_inflight(&mut conn);

        if let Some(up) = self.pool.take(addr) {
            // A pooled connection already has a read parked on it — the one
            // that was watching for the far end closing. It becomes the read
            // that collects the response, so the hot path submits one operation
            // fewer than a cold one.
            conn.upstream = Some(up);
            conn.up_reading = true;
            conn.up_ready = true;
            conn.from_pool = true;
            conn.phase = Phase::Exchanging;
            *self.slot(up) = Slot::Upstream(fd);
            *self.slot(fd) = Slot::Client(conn);
            return self.advance(fd);
        }

        let (socket, connected) = match sys::tcp_connect(addr) {
            Ok(pair) => pair,
            Err(_) => {
                self.metrics.core(self.core).connect_failure();
                *self.slot(fd) = Slot::Client(conn);
                return self.retry_or_fail(fd);
            }
        };
        let up = socket.into_raw_fd();
        conn.upstream = Some(up);
        conn.up_reading = false;
        conn.up_ready = connected;
        conn.from_pool = false;
        *self.slot(up) = Slot::Upstream(fd);

        if connected {
            conn.phase = Phase::Exchanging;
            *self.slot(fd) = Slot::Client(conn);
            return self.advance(fd);
        }

        // Not connected yet, and this reactor has no operation that waits for
        // that. The helper thread borrows the descriptor until it can say how
        // the connect ended.
        let deadline = Instant::now() + self.config.connect_timeout;
        match self.helper.watch_connect(up, self.core, deadline) {
            Ok(()) => {
                conn.up_borrowed = true;
                conn.phase = Phase::Connecting;
                *self.slot(fd) = Slot::Client(conn);
                Ok(())
            }
            Err(_) => {
                conn.upstream = None;
                *self.slot(up) = Slot::Empty;
                // SAFETY: nothing was submitted for this descriptor and the
                // helper did not take it.
                unsafe { sys::close(up) };
                self.metrics.core(self.core).connect_failure();
                *self.slot(fd) = Slot::Client(conn);
                self.retry_or_fail(fd)
            }
        }
    }

    /// The helper thread reporting how a connect ended.
    fn on_connected(&mut self, up: RawFd, err: i32) -> io::Result<()> {
        // Ownership of the descriptor has just come back.
        let owner = match self.slot(up) {
            Slot::Upstream(down) => Some(*down),
            _ => None,
        };
        let Some(down) = owner else {
            // The client went away while this was connecting. The descriptor
            // was held open precisely until now.
            *self.slot(up) = Slot::Empty;
            // SAFETY: the helper has released it and nothing was ever
            // submitted for it.
            unsafe { sys::close(up) };
            return Ok(());
        };
        let Slot::Client(mut conn) = std::mem::replace(self.slot(down), Slot::Taken) else {
            *self.slot(down) = Slot::Empty;
            *self.slot(up) = Slot::Empty;
            // SAFETY: as above.
            unsafe { sys::close(up) };
            return Ok(());
        };
        conn.up_borrowed = false;
        if err == 0 {
            conn.up_ready = true;
            conn.phase = Phase::Exchanging;
            *self.slot(down) = Slot::Client(conn);
            return self.advance(down);
        }

        conn.upstream = None;
        *self.slot(up) = Slot::Empty;
        // SAFETY: the helper released it and no operation was ever submitted.
        unsafe { sys::close(up) };
        self.metrics.core(self.core).connect_failure();
        *self.slot(down) = Slot::Client(conn);
        self.retry_or_fail(down)
    }

    /// Move body bytes in both directions as far as the buffers allow.
    fn pump_bodies(&mut self, fd: RawFd) -> io::Result<bool> {
        let Slot::Client(mut conn) = std::mem::replace(self.slot(fd), Slot::Taken) else {
            *self.slot(fd) = Slot::Empty;
            return Ok(false);
        };
        let mut progressed = false;

        // Client → upstream.
        if !conn.request_body.is_done() && !conn.inbox.is_empty() {
            let inbox = std::mem::take(&mut conn.inbox);
            match conn.request_body.take(&inbox) {
                Ok((n, done)) => {
                    conn.up_outbox.extend_from_slice(&inbox[..n]);
                    // The copy is taken here, out of a buffer these bytes were
                    // passing through anyway, so the primary waits for nothing.
                    if conn.mirror.is_some() {
                        let taken = inbox[..n].to_vec();
                        self.mirror_body(&mut conn, &taken);
                    }
                    conn.inbox = inbox;
                    conn.inbox.drain(..n);
                    if done {
                        conn.request_body = Body::Done;
                        self.send_mirror(&mut conn);
                    }
                    progressed = n > 0;
                }
                Err(e) => {
                    conn.inbox = inbox;
                    *self.slot(fd) = Slot::Client(conn);
                    let body = limits::bad_request_body(e.status(), e.detail());
                    self.fail(fd, e.status(), &body, true);
                    return Ok(false);
                }
            }
        }
        if !conn.request_body.is_done() && conn.client_eof && conn.inbox.is_empty() {
            // The client promised more body than it sent.
            *self.slot(fd) = Slot::Client(conn);
            self.fail(fd, 400, b"400 Bad Request: the request body ended early\n", true);
            return Ok(false);
        }

        *self.slot(fd) = Slot::Client(conn);

        // Upstream → client.
        if !self.parse_response(fd)? && !progressed {
            return Ok(false);
        }
        Ok(true)
    }

    /// Read the response head if it has not been seen, then relay body bytes.
    ///
    /// Returns whether anything moved.
    fn parse_response(&mut self, fd: RawFd) -> io::Result<bool> {
        let Slot::Client(mut conn) = std::mem::replace(self.slot(fd), Slot::Taken) else {
            *self.slot(fd) = Slot::Empty;
            return Ok(false);
        };
        if conn.up_inbox.is_empty() {
            *self.slot(fd) = Slot::Client(conn);
            return Ok(false);
        }

        if !conn.response_head_seen {
            let parsed = {
                let up_inbox = std::mem::take(&mut conn.up_inbox);
                let outcome = parse_response_head(&up_inbox, &mut conn.up_head);
                conn.up_inbox = up_inbox;
                outcome
            };
            match parsed {
                Ok(false) => {
                    *self.slot(fd) = Slot::Client(conn);
                    return Ok(false);
                }
                Err(_) => {
                    *self.slot(fd) = Slot::Client(conn);
                    return self.upstream_garbage(fd).map(|()| false);
                }
                Ok(true) => {}
            }

            // A 101 is not a response with a body, it is the end of HTTP on
            // this connection. Deciding that here, before framing is worked
            // out, is what keeps the switch out of the body machinery: there
            // is no framing to compute, because everything after the head
            // belongs to whatever protocol the two ends just agreed on.
            if headers::is_switching_protocols(&conn.up_head) {
                *self.slot(fd) = Slot::Client(conn);
                return self.begin_tunnel(fd).map(|()| true);
            }

            let framing = match codec::response_framing(
                &conn.up_head,
                &conn.up_inbox,
                conn.method_was_head,
            ) {
                Ok(framing) => framing,
                Err(_) => {
                    *self.slot(fd) = Slot::Client(conn);
                    return self.upstream_garbage(fd).map(|()| false);
                }
            };
            if let Some(started) = conn.dispatched_at.take() {
                self.record_upstream_latency(&conn, started.elapsed());
            }
            conn.upstream_keep_alive =
                codec::keep_alive(&conn.up_head, &conn.up_inbox, conn.up_head.version())
                    && framing.allows_reuse();

            let status = conn.up_head.status().unwrap_or(502);
            // A response with no framing header is delimited by the close and
            // by nothing else. Forwarding it down a connection we then keep
            // open would leave the client waiting for an end that never comes,
            // so this hop's connection ends with the upstream's.
            if framing == Framing::UntilClose {
                conn.client_keep_alive = false;
            }
            let close_client = !conn.client_keep_alive;
            {
                let up_inbox = std::mem::take(&mut conn.up_inbox);
                let mut out = std::mem::take(&mut conn.outbox);
                headers::write_downstream_response(
                    &mut out,
                    &conn.up_head,
                    &up_inbox,
                    framing,
                    close_client,
                    None,
                );
                conn.outbox = out;
                conn.up_inbox = up_inbox;
            }
            let consumed = conn.up_head.len;
            conn.up_inbox.drain(..consumed);
            conn.response_body = Body::new(framing);
            conn.response_head_seen = true;
            conn.phase = Phase::Relaying;
            // Past this point the exchange is committed: the client has been
            // told a status, so nothing may be retried.
            conn.committed = true;
            conn.deadline = Some(Instant::now() + self.config.response_timeout);
            self.record_response(&mut conn, status);
        }

        // Relay body bytes verbatim — including chunk framing, which is
        // forwarded rather than decoded and re-encoded.
        let mut moved = false;
        if !conn.response_body.is_done() && !conn.up_inbox.is_empty() {
            let up_inbox = std::mem::take(&mut conn.up_inbox);
            match conn.response_body.take(&up_inbox) {
                Ok((n, done)) => {
                    conn.outbox.extend_from_slice(&up_inbox[..n]);
                    conn.up_inbox = up_inbox;
                    conn.up_inbox.drain(..n);
                    moved = n > 0;
                    if done {
                        conn.response_body = Body::Done;
                    }
                }
                Err(_) => {
                    conn.up_inbox = up_inbox;
                    *self.slot(fd) = Slot::Client(conn);
                    // The head is already sent, so there is no status left to
                    // send: all that can be done is stop.
                    return self.upstream_garbage(fd).map(|()| false);
                }
            }
        }

        let complete = conn.response_head_seen && conn.response_body.is_done();
        *self.slot(fd) = Slot::Client(conn);
        if complete {
            self.finish_response(fd)?;
            return Ok(true);
        }
        Ok(moved)
    }

    /// The upstream accepted an upgrade: relay the 101 and stop speaking HTTP.
    ///
    /// # Why there is no frame parsing here
    ///
    /// Passthrough needs none. Once a 101 has crossed this hop the two
    /// endpoints have agreed on a protocol, and every byte after the head is
    /// theirs — a WebSocket frame, or whatever else they settled on, is opaque
    /// to a proxy that is not inspecting it. So this is a byte pump, and it
    /// costs the same two buffers however chatty the protocol on top turns out
    /// to be. The hyper engine's `splice` is the same decision.
    ///
    /// # Shutdown
    ///
    /// A tunnel does not hold shutdown open, for the same reason nothing else
    /// on this engine does: a stop is a flag every core reads on its next tick,
    /// and `teardown` closes whatever is still open. There is no drain to
    /// exclude a tunnel from, which is the same end state the hyper engine
    /// arrives at by excluding it explicitly.
    fn begin_tunnel(&mut self, fd: RawFd) -> io::Result<()> {
        let Slot::Client(mut conn) = std::mem::replace(self.slot(fd), Slot::Taken) else {
            *self.slot(fd) = Slot::Empty;
            return Ok(());
        };

        // A 101 to a request that never asked to upgrade leaves this hop with
        // no idea how the rest of the connection is framed, and no honest way
        // to guess. The client has had no bytes yet, so it can still be told.
        let Some(protocol) = conn.upgrade.clone() else {
            *self.slot(fd) = Slot::Client(conn);
            self.fail(fd, 502, limits::UPGRADE_FAILED, true);
            return self.drive(fd);
        };

        // The upstream normally echoes `Upgrade`; where it did not, the
        // protocol the client asked for is the only sensible answer, which is
        // the rule the hyper engine's `relay` applies.
        let echoed = headers::upgrade_protocol(&conn.up_head, &conn.up_inbox).map(<[u8]>::to_vec);
        let protocol = echoed.unwrap_or(protocol);

        {
            let up_inbox = std::mem::take(&mut conn.up_inbox);
            let mut out = std::mem::take(&mut conn.outbox);
            headers::write_downstream_response(
                &mut out,
                &conn.up_head,
                &up_inbox,
                Framing::Empty,
                false,
                Some(&protocol),
            );
            conn.outbox = out;
            conn.up_inbox = up_inbox;
        }
        let consumed = conn.up_head.len;
        conn.up_inbox.drain(..consumed);

        // Anything the upstream sent past its 101 in the same read is already
        // tunnel traffic. A server that speaks first — which a WebSocket
        // server may — would otherwise have its first frame dropped.
        let carried = std::mem::take(&mut conn.up_inbox);
        conn.outbox.extend_from_slice(&carried);
        conn.up_inbox = carried;
        conn.up_inbox.clear();

        // The same for the client: bytes pipelined behind the handshake.
        let pipelined = std::mem::take(&mut conn.inbox);
        conn.up_outbox.extend_from_slice(&pipelined);
        conn.inbox = pipelined;
        conn.inbox.clear();

        conn.response_head_seen = true;
        conn.committed = true;
        conn.phase = Phase::Tunnel;
        // A tunnel has no response deadline. The bound it lives under is the
        // client's idle timeout, and an idle WebSocket is a working WebSocket.
        conn.deadline = None;
        // An upgraded connection is never pooled: it is not HTTP any more.
        conn.upstream_keep_alive = false;
        if let Some(started) = conn.dispatched_at.take() {
            self.record_upstream_latency(&conn, started.elapsed());
        }
        self.record_response(&mut conn, 101);

        *self.slot(fd) = Slot::Client(conn);
        Ok(())
    }

    /// Move bytes both ways through an established tunnel.
    ///
    /// Returns whether anything moved. There is no parsing: whatever arrived on
    /// one side is queued for the other exactly as it came.
    fn pump_tunnel(&mut self, fd: RawFd) -> io::Result<bool> {
        let Slot::Client(mut conn) = std::mem::replace(self.slot(fd), Slot::Taken) else {
            *self.slot(fd) = Slot::Empty;
            return Ok(false);
        };
        let mut moved = false;

        if !conn.inbox.is_empty() {
            let inbox = std::mem::take(&mut conn.inbox);
            conn.up_outbox.extend_from_slice(&inbox);
            conn.inbox = inbox;
            conn.inbox.clear();
            moved = true;
        }
        if !conn.up_inbox.is_empty() {
            let up_inbox = std::mem::take(&mut conn.up_inbox);
            conn.outbox.extend_from_slice(&up_inbox);
            conn.up_inbox = up_inbox;
            conn.up_inbox.clear();
            moved = true;
        }

        // A half-close is forwarded rather than escalated to a full close.
        // Closing both directions when one ends would throw away the other
        // side's remaining bytes — for a WebSocket, the close frame that says
        // why the connection is ending.
        if conn.client_eof && conn.up_outbox.is_empty() && !conn.up_writing {
            if let Some(up) = conn.upstream {
                sys::shutdown_write(up);
            }
        }
        if conn.upstream_eof && conn.outbox.is_empty() && !conn.writing {
            match conn.tls.as_mut() {
                // TLS has no half-close: the record layer's end-of-stream is
                // `close_notify`, and a session that has sent one cannot send
                // anything else. So the client is told the stream ended and
                // the connection goes, rather than being held half-open for a
                // direction that can no longer carry anything.
                Some(session) => {
                    session.send_close_notify();
                    conn.phase = Phase::Closing;
                }
                None if conn.client_eof => conn.phase = Phase::Closing,
                None => sys::shutdown_write(fd),
            }
        }

        *self.slot(fd) = Slot::Client(conn);
        Ok(moved)
    }

    /// The response is complete: release the upstream and ready the connection
    /// for whatever comes next.
    fn finish_response(&mut self, fd: RawFd) -> io::Result<()> {
        let Slot::Client(mut conn) = std::mem::replace(self.slot(fd), Slot::Taken) else {
            *self.slot(fd) = Slot::Empty;
            return Ok(());
        };
        self.release_inflight(&mut conn);

        if let Some(up) = conn.upstream.take() {
            // An upstream that sent bytes past the end of its response has lost
            // framing, and pooling it would hand those bytes to another
            // request. Reuse requires a clean boundary.
            let clean = conn.up_inbox.is_empty() && !conn.up_writing;
            // `targets` is in attempt order, so the endpoint this exchange
            // actually used is the one at the current attempt.
            let addr = conn.targets.get(conn.attempt).copied();
            match (conn.upstream_keep_alive && clean, addr) {
                (true, Some(addr)) => {
                    *self.slot(up) = Slot::Idle(addr);
                    match self.pool.put(addr, up) {
                        None => {
                            // Park a read: it watches for the far end closing
                            // while idle, and becomes the response read for
                            // whoever takes this connection next.
                            if !conn.up_reading {
                                let generation = self.generation(up);
                                let buf = self.read_buf();
                                if self
                                    .driver
                                    .submit_with(
                                        Op::Read { fd: up, buf },
                                        tag(kind::UP_READ, generation, up),
                                    )
                                    .is_err()
                                {
                                    self.pool.remove(addr, up);
                                    self.close(up);
                                }
                            }
                        }
                        Some(refused) => self.close(refused),
                    }
                }
                _ => self.close(up),
            }
        }
        conn.up_reading = false;
        conn.up_writing = false;
        conn.up_ready = false;

        let keep = conn.client_keep_alive;
        conn.reset();
        if !keep {
            conn.phase = Phase::Closing;
        }
        *self.slot(fd) = Slot::Client(conn);
        Ok(())
    }

    // ---- failure paths ------------------------------------------------------

    /// Queue a response this proxy invented, and optionally end the connection.
    fn fail(&mut self, fd: RawFd, status: u16, body: &[u8], close: bool) {
        let Slot::Client(mut conn) = std::mem::replace(self.slot(fd), Slot::Taken) else {
            *self.slot(fd) = Slot::Empty;
            return;
        };
        self.release_inflight(&mut conn);
        if let Some(up) = conn.upstream.take() {
            if conn.up_borrowed {
                // The helper still holds it; closing now would be a
                // use-after-free of a descriptor number.
                *self.slot(up) = Slot::Abandoned;
            } else {
                self.close(up);
            }
        }
        conn.up_reading = false;
        conn.up_writing = false;
        conn.up_ready = false;

        // Two reasons a refusal has to end the connection, both about framing
        // rather than about the error. A request whose body has not been fully
        // read leaves bytes in the stream that would be parsed as the next
        // request; and a client that asked to close gets what it asked for.
        // Otherwise the request being answered is consumed here, because
        // nothing else has consumed it on this path.
        let close = close || !conn.client_keep_alive || !conn.request_body.is_done();
        if !close && !conn.head_consumed {
            let consumed = conn.head.len;
            conn.inbox.drain(..consumed);
            conn.head_consumed = true;
        }
        self.record_response(&mut conn, status);
        let mut out = std::mem::take(&mut conn.outbox);
        if conn.method_was_head {
            limits::write_static_head_only(&mut out, status, body.len(), close);
        } else {
            limits::write_static(&mut out, status, body, close);
        }
        conn.outbox = out;

        if close {
            conn.phase = Phase::Closing;
        } else {
            let leftover = std::mem::take(&mut conn.inbox);
            conn.reset();
            conn.inbox = leftover;
        }
        *self.slot(fd) = Slot::Client(conn);
    }

    /// An upstream failed before the client was told anything.
    fn upstream_failed(&mut self, fd: RawFd) -> io::Result<()> {
        let Slot::Client(mut conn) = std::mem::replace(self.slot(fd), Slot::Taken) else {
            *self.slot(fd) = Slot::Empty;
            return Ok(());
        };
        if conn.response_head_seen {
            // The client already has a status and part of a body; there is no
            // honest way to turn that into an error, so the truncated response
            // is ended by closing, which is what tells the client it is
            // incomplete.
            conn.phase = Phase::Closing;
            if let Some(up) = conn.upstream.take() {
                *self.slot(fd) = Slot::Client(conn);
                self.close(up);
            } else {
                *self.slot(fd) = Slot::Client(conn);
            }
            return self.drive(fd);
        }
        if let Some(up) = conn.upstream.take() {
            *self.slot(fd) = Slot::Client(conn);
            self.close(up);
        } else {
            *self.slot(fd) = Slot::Client(conn);
        }
        self.retry_or_fail(fd)
    }

    /// An upstream said something that is not HTTP.
    fn upstream_garbage(&mut self, fd: RawFd) -> io::Result<()> {
        let Slot::Client(conn) = self.slot(fd) else {
            return Ok(());
        };
        let seen = conn.response_head_seen;
        if seen {
            return self.upstream_failed(fd);
        }
        let Slot::Client(mut conn) = std::mem::replace(self.slot(fd), Slot::Taken) else {
            return Ok(());
        };
        // Not retryable: the connection worked, the peer is simply not an HTTP
        // server, and another endpoint of the same backend would say the same.
        conn.committed = true;
        if let Some(up) = conn.upstream.take() {
            *self.slot(fd) = Slot::Client(conn);
            self.close(up);
        } else {
            *self.slot(fd) = Slot::Client(conn);
        }
        self.fail(fd, 502, limits::UPSTREAM_FAILED, true);
        self.drive(fd)
    }

    /// Try the next endpoint, or give up with the status the hyper engine
    /// would have given.
    fn retry_or_fail(&mut self, fd: RawFd) -> io::Result<()> {
        let Slot::Client(mut conn) = std::mem::replace(self.slot(fd), Slot::Taken) else {
            *self.slot(fd) = Slot::Empty;
            return Ok(());
        };
        self.release_inflight(&mut conn);

        // A connection taken from the pool can be closed by the origin between
        // the moment it is taken and the moment the request lands on it. No
        // pooling proxy can close that window; what it can do is not blame the
        // endpoint for it. So the *same* endpoint gets one more try on a fresh
        // connection, and it is not counted as a retry or a connect failure —
        // nothing about the endpoint has been learned. hyper's client calls
        // this `retry_canceled_requests`, and it is on by default there.
        let pooled_race =
            conn.from_pool && !conn.pool_retry_used && !conn.committed && conn.replayable;
        if pooled_race {
            conn.from_pool = false;
            conn.pool_retry_used = true;
        } else {
            let next = conn.attempt + 1;
            if conn.committed || next >= conn.targets.len() {
                *self.slot(fd) = Slot::Client(conn);
                return self.exhausted(fd);
            }
            conn.attempt = next;
            self.metrics.core(self.core).retry();
        }

        // Re-send the head from the copy kept for exactly this.
        conn.up_outbox.clear();
        conn.up_inbox.clear();
        conn.up_reading = false;
        conn.up_writing = false;
        conn.up_ready = false;
        conn.response_head_seen = false;
        conn.up_head.reset();
        let head = std::mem::take(&mut conn.request_head);
        conn.up_outbox.extend_from_slice(&head);
        conn.request_head = head;
        conn.phase = Phase::Exchanging;
        *self.slot(fd) = Slot::Client(conn);
        self.open_upstream(fd)
    }

    /// Every endpoint has been tried.
    fn exhausted(&mut self, fd: RawFd) -> io::Result<()> {
        self.fail(fd, 502, limits::CONNECT_FAILED, false);
        self.drive(fd)
    }

    // ---- mirroring ----------------------------------------------------------

    /// Decide where a sampled request's copy is going, and start assembling it.
    ///
    /// Everything that needs the route table happens here, while the snapshot
    /// is in hand: which backend, which endpoint, which `Host`. What is left
    /// for later is the body, and queueing.
    ///
    /// A mirror whose backend has no ready endpoints is counted as a failure
    /// rather than ignored. An operator who configured a mirror and sees no
    /// copies arriving should be able to tell "the shadow Service has no ready
    /// pods" from "the annotation never took effect".
    fn plan_mirror(
        &self,
        conn: &mut Conn,
        snapshot: &RouteTable,
        spec: Option<&ramjet_router::MirrorSpec>,
        framing: Framing,
    ) {
        conn.mirror = None;
        let Some(spec) = spec else { return };
        let Some(lane) = self.config.mirror.as_ref() else {
            return;
        };
        // A chunked request body is forwarded verbatim on this lane — chunk
        // framing and all — so the bytes streaming past are not the body, they
        // are the body's encoding. The mirror worker sends what it is given as
        // a self-framed request, which would double-encode them. Decoding a
        // body this engine deliberately does not decode, to make a copy of it,
        // is not a trade worth making: it is counted as a skip instead, which
        // is the same number the hyper lane reports for a body it could not
        // reproduce.
        if !matches!(framing, Framing::Empty | Framing::Length(_)) {
            lane.skipped();
            return;
        }
        let Some(backend) = snapshot.backend(spec.backend()) else {
            return;
        };
        if backend.endpoints().is_empty() {
            lane.failed();
            return;
        }
        // No in-flight accounting for a mirrored request: the guard borrows out
        // of the route table and cannot cross the queue, and letting shadow
        // traffic move production's load-balancing decisions would be its own
        // kind of leak.
        let Some((index, _)) =
            ramjet_router::select_endpoint(backend, snapshot.stats(), crate::rng::next_u64())
        else {
            lane.failed();
            return;
        };
        let Some(endpoint) = backend.endpoints().get(index) else {
            lane.failed();
            return;
        };
        let Some(parts) = crate::mirror::parts_for(&conn.request_head, endpoint.addr, spec.host())
        else {
            lane.failed();
            return;
        };

        conn.mirror = Some(MirrorCopy {
            parts,
            body: Vec::new(),
            too_large: false,
        });
    }

    /// Keep a copy of request-body bytes on their way upstream.
    fn mirror_body(&self, conn: &mut Conn, bytes: &[u8]) {
        let Some(copy) = conn.mirror.as_mut() else {
            return;
        };
        if copy.too_large {
            return;
        }
        if copy.body.len() + bytes.len() > self.config.mirror_max_body {
            // Past the cap the bytes already held are released too: keeping
            // them would be memory spent on a copy that is not going to be
            // made.
            copy.body = Vec::new();
            copy.too_large = true;
            return;
        }
        copy.body.extend_from_slice(bytes);
    }

    /// The request body is complete: hand the copy over, or count the skip.
    fn send_mirror(&self, conn: &mut Conn) {
        let Some(copy) = conn.mirror.take() else {
            return;
        };
        let Some(lane) = self.config.mirror.as_ref() else {
            return;
        };
        if copy.too_large {
            lane.skipped();
            return;
        }
        lane.enqueue(copy.parts, bytes::Bytes::from(copy.body));
    }

    // ---- per-route accounting ----------------------------------------------

    /// Count a response against the process-wide series and the route's own.
    ///
    /// Always the matched route's block; additionally the route's canary block
    /// when the canary took the request. Both, rather than one or the other —
    /// the totals have to stay the totals, or every graph an operator has jumps
    /// the moment a canary starts.
    ///
    /// The counter blocks are reached through the snapshot the request is
    /// already holding, and they outlive it, so recording after a rebuild still
    /// lands in the block the new generation serves.
    fn record_response(&self, conn: &mut Conn, status: u16) {
        if conn.counted {
            return;
        }
        conn.counted = true;
        self.metrics.core(self.core).response(status);

        let Some(index) = conn.route_stats else { return };
        let Some(table) = conn.snapshot.as_ref() else {
            return;
        };
        let Some(slot) = table.route_stats().slot(index) else {
            return;
        };
        // One block per serving core, so two cores never contend on the same
        // cache line for the same route; the remainder against the shard count
        // is taken inside the router.
        slot.shard(self.core).record_response(status);
        if conn.canaried {
            slot.canary_shard(self.core).record_response(status);
        }
    }

    /// Observe an upstream's response-header latency, in the same two places.
    fn record_upstream_latency(&self, conn: &Conn, elapsed: Duration) {
        self.metrics.core(self.core).upstream_latency(elapsed);

        let Some(index) = conn.route_stats else { return };
        let Some(table) = conn.snapshot.as_ref() else {
            return;
        };
        let Some(slot) = table.route_stats().slot(index) else {
            return;
        };
        slot.shard(self.core).record_upstream_latency(elapsed);
        if conn.canaried {
            slot.canary_shard(self.core).record_upstream_latency(elapsed);
        }
    }

    // ---- in-flight accounting ----------------------------------------------

    /// Count this request against its endpoint, for `leastConn`.
    fn hold_inflight(&mut self, conn: &mut Conn) {
        if conn.inflight_held {
            return;
        }
        let Some(table) = conn.snapshot.as_ref() else {
            return;
        };
        let index = (conn.endpoint_index + conn.attempt) % conn.endpoint_count.max(1);
        if let Some(slot) = table.stats().slot(conn.stats_index) {
            if let Some(counter) = slot.inflight(index) {
                counter.fetch_add(1, Ordering::Relaxed);
                conn.inflight_held = true;
            }
        }
    }

    fn release_inflight(&mut self, conn: &mut Conn) {
        if !conn.inflight_held {
            return;
        }
        conn.inflight_held = false;
        let Some(table) = conn.snapshot.as_ref() else {
            return;
        };
        let index = (conn.endpoint_index + conn.attempt) % conn.endpoint_count.max(1);
        if let Some(slot) = table.stats().slot(conn.stats_index) {
            if let Some(counter) = slot.inflight(index) {
                counter.fetch_sub(1, Ordering::Relaxed);
            }
        }
    }

    // ---- submission ---------------------------------------------------------

    /// Submit every operation this connection has become ready for.
    ///
    /// Every submission is guarded by the flag that records whether that
    /// descriptor's read or write slot is already occupied: the reactor refuses
    /// a collision with `ResourceBusy` *and drops the buffer that came with it*,
    /// so speculative submission would leak a buffer per collision.
    fn drive(&mut self, fd: RawFd) -> io::Result<()> {
        let Slot::Client(mut conn) = std::mem::replace(self.slot(fd), Slot::Taken) else {
            *self.slot(fd) = Slot::Empty;
            return Ok(());
        };

        // Under TLS the bytes that reach the socket are never the bytes the
        // state machine produced, so the plaintext outbox is encrypted here and
        // the session's ciphertext queue is what gets submitted.
        seal(&mut conn);
        if !conn.writing && has_pending_out(&conn) {
            let mut buf = self.write_buf();
            match conn.tls.as_mut() {
                Some(session) => std::mem::swap(session.wire_out(), &mut buf),
                None => std::mem::swap(&mut conn.outbox, &mut buf),
            }
            let generation = self.generation(fd);
            match self
                .driver
                .submit_with(Op::Write { fd, buf }, tag(kind::DOWN_WRITE, generation, fd))
            {
                Ok(_) => conn.writing = true,
                Err(_) => conn.phase = Phase::Closing,
            }
        }

        if let Some(up) = conn.upstream {
            if conn.up_ready && !conn.up_writing && !conn.up_outbox.is_empty() {
                let mut buf = self.write_buf();
                std::mem::swap(&mut conn.up_outbox, &mut buf);
                let generation = self.generation(up);
                match self.driver.submit_with(
                    Op::Write { fd: up, buf },
                    tag(kind::UP_WRITE, generation, up),
                ) {
                    Ok(_) => conn.up_writing = true,
                    Err(_) => conn.phase = Phase::Closing,
                }
            }
            if !conn.up_reading && conn.wants_upstream_bytes() {
                let generation = self.generation(up);
                let buf = self.read_buf();
                if self
                    .driver
                    .submit_with(
                        Op::Read { fd: up, buf },
                        tag(kind::UP_READ, generation, up),
                    )
                    .is_ok()
                {
                    conn.up_reading = true;
                }
            }
        }

        if !conn.reading && conn.wants_client_bytes() {
            let generation = self.generation(fd);
            let buf = self.read_buf();
            if self
                .driver
                .submit_with(Op::Read { fd, buf }, tag(kind::DOWN_READ, generation, fd))
                .is_ok()
            {
                conn.reading = true;
            }
        }

        let finished = conn.phase == Phase::Closing && !conn.writing && !has_pending_out(&conn);
        let borrowed = conn.up_borrowed;
        let upstream = conn.upstream;
        *self.slot(fd) = Slot::Client(conn);

        if finished {
            if let Some(up) = upstream {
                if borrowed {
                    // Held open until the helper answers; see `on_connected`.
                    *self.slot(up) = Slot::Abandoned;
                } else {
                    self.close(up);
                }
            }
            self.metrics.core(self.core).connection_closed();
            self.close(fd);
        }
        Ok(())
    }

    // ---- the clock ----------------------------------------------------------

    fn on_tick(&mut self) -> io::Result<()> {
        let now = Instant::now();

        let mut expired = std::mem::take(&mut self.scratch);
        expired.clear();
        self.pool.expire(now, &mut expired);
        for fd in expired.drain(..) {
            self.close(fd);
        }
        self.scratch = expired;

        for i in 0..self.high_water {
            let fd = i as RawFd;
            let overdue = match self.slots.get(i) {
                Some(Slot::Client(conn)) => conn.deadline.is_some_and(|d| now >= d),
                _ => false,
            };
            if !overdue {
                continue;
            }
            let Slot::Client(conn) = self.slot(fd) else {
                continue;
            };
            match conn.phase {
                // Waiting for a response that has not started.
                Phase::Exchanging | Phase::Connecting => {
                    self.metrics.core(self.core).timeout();
                    self.fail(fd, 504, limits::TIMEOUT, true);
                    self.drive(fd)?;
                }
                // An idle keep-alive connection nobody is using.
                Phase::Head => {
                    self.metrics.core(self.core).connection_closed();
                    self.close(fd);
                }
                // A response that is still streaming is making progress; the
                // deadline bounds how long the *headers* take, not how long a
                // large download is allowed to be. A tunnel is on no clock at
                // all — an idle WebSocket is a working WebSocket — and reaches
                // here only if something set a deadline it should not have.
                Phase::Relaying | Phase::Closing | Phase::Tunnel => {
                    let extended = now + self.config.response_timeout;
                    if let Slot::Client(conn) = self.slot(fd) {
                        conn.deadline = Some(extended);
                    }
                }
            }
        }
        Ok(())
    }

    // ---- admin --------------------------------------------------------------

    fn start_admin(&mut self, fd: RawFd) -> io::Result<()> {
        if sys::set_nonblocking(fd).is_err() {
            // SAFETY: nothing has been submitted for this descriptor.
            unsafe { sys::close(fd) };
            return Ok(());
        }
        self.bump_generation(fd);
        *self.slot(fd) = Slot::Admin(Box::new(AdminConn {
            fd,
            inbox: Vec::new(),
            outbox: Vec::new(),
            head: Head::default(),
            reading: false,
            writing: false,
        }));
        self.drive_admin(fd)
    }

    fn on_admin_read(&mut self, fd: RawFd, c: Completion) -> io::Result<()> {
        let Some(buf) = c.buf else { return Ok(()) };
        let Slot::Admin(mut conn) = std::mem::replace(self.slot(fd), Slot::Taken) else {
            *self.slot(fd) = Slot::Empty;
            self.recycle_read(buf);
            return Ok(());
        };
        conn.reading = false;
        let bytes = match c.result {
            Ok(n) if n > 0 => n as usize,
            _ => {
                self.recycle_read(buf);
                self.close(fd);
                return Ok(());
            }
        };
        conn.inbox.extend_from_slice(&buf[..bytes]);
        self.recycle_read(buf);

        let parsed = {
            let inbox = std::mem::take(&mut conn.inbox);
            let outcome = parse_request_head(&inbox, &mut conn.head);
            conn.inbox = inbox;
            outcome
        };
        match parsed {
            Ok(false) => {
                *self.slot(fd) = Slot::Admin(conn);
                return self.drive_admin(fd);
            }
            Err(_) => {
                let mut out = std::mem::take(&mut conn.outbox);
                limits::write_static(&mut out, 400, b"bad request\n", true);
                conn.outbox = out;
            }
            Ok(true) => {
                let response = self.admin_response(&conn);
                conn.outbox.extend_from_slice(&response);
            }
        }
        *self.slot(fd) = Slot::Admin(conn);
        self.drive_admin(fd)
    }

    fn admin_response(&self, conn: &AdminConn) -> Vec<u8> {
        let mut out = Vec::with_capacity(4096);
        let method = conn.head.method(&conn.inbox).unwrap_or(b"");
        let target = conn.head.target(&conn.inbox).unwrap_or(b"");
        let path = target
            .split(|&b| b == b'?')
            .next()
            .unwrap_or(b"");
        let head_only = method.eq_ignore_ascii_case(b"HEAD");

        // Method first, as the hyper engine's admin does.
        if !method.eq_ignore_ascii_case(b"GET") && !head_only {
            write_admin(&mut out, 405, b"method not allowed\n", None, head_only);
            return out;
        }

        match path {
            b"/metrics" => {
                let generation = self.routes.generation();
                let body = self.metrics.render_prometheus(generation, false);
                write_admin(
                    &mut out,
                    200,
                    body.as_bytes(),
                    Some("text/plain; version=0.0.4; charset=utf-8"),
                    head_only,
                );
            }
            b"/healthz" => write_admin(&mut out, 200, b"ok\n", None, head_only),
            b"/readyz" => {
                if self.readiness.load(Ordering::Acquire) {
                    write_admin(&mut out, 200, b"ready\n", None, head_only);
                } else {
                    write_admin(&mut out, 503, b"not ready\n", None, head_only);
                }
            }
            _ => write_admin(&mut out, 404, b"not found\n", None, head_only),
        }
        out
    }

    fn on_admin_write(&mut self, fd: RawFd, c: Completion) -> io::Result<()> {
        if let Some(buf) = c.buf {
            self.recycle_write(buf);
        }
        let Slot::Admin(mut conn) = std::mem::replace(self.slot(fd), Slot::Taken) else {
            *self.slot(fd) = Slot::Empty;
            return Ok(());
        };
        conn.writing = false;
        let done = conn.outbox.is_empty() || c.result.is_err();
        *self.slot(fd) = Slot::Admin(conn);
        if done {
            self.close(fd);
            return Ok(());
        }
        self.drive_admin(fd)
    }

    fn drive_admin(&mut self, fd: RawFd) -> io::Result<()> {
        let Slot::Admin(mut conn) = std::mem::replace(self.slot(fd), Slot::Taken) else {
            *self.slot(fd) = Slot::Empty;
            return Ok(());
        };
        if !conn.writing && !conn.outbox.is_empty() {
            let mut buf = self.write_buf();
            std::mem::swap(&mut conn.outbox, &mut buf);
            let generation = self.generation(fd);
            if self
                .driver
                .submit_with(Op::Write { fd, buf }, tag(kind::ADMIN_WRITE, generation, fd))
                .is_ok()
            {
                conn.writing = true;
            }
        } else if !conn.reading && !conn.writing && conn.outbox.is_empty() {
            let generation = self.generation(fd);
            let buf = self.read_buf();
            if self
                .driver
                .submit_with(Op::Read { fd, buf }, tag(kind::ADMIN_READ, generation, fd))
                .is_ok()
            {
                conn.reading = true;
            }
        }
        let _ = conn.fd;
        *self.slot(fd) = Slot::Admin(conn);
        Ok(())
    }

    // ---- shutdown -----------------------------------------------------------

    /// Close everything and drain what that cancelled.
    ///
    /// The order is not incidental. `wait` blocks while *anything* is in
    /// flight, and this core always has at least two operations parked that
    /// nothing external will ever complete: the read on the notify pipe and the
    /// accept on the listener. Closing the connections and then waiting would
    /// therefore block for ever — which it did, until those two were added to
    /// the list.
    fn teardown(&mut self) {
        if !self.pool.is_empty() {
            let mut idle = std::mem::take(&mut self.scratch);
            idle.clear();
            self.pool.drain(&mut idle);
            for fd in idle.drain(..) {
                self.close(fd);
            }
            self.scratch = idle;
        }
        for i in 0..self.high_water {
            if matches!(
                self.slots.get(i),
                Some(Slot::Client(_) | Slot::Admin(_) | Slot::Upstream(_) | Slot::Idle(_))
            ) {
                self.close(i as RawFd);
            }
        }

        // The descriptors that are not connections, and whose parked operations
        // are the ones that would otherwise never complete.
        let intakes: Vec<RawFd> = self.intakes.iter().map(Intake::fd).collect();
        for fd in intakes {
            self.close(fd);
        }
        if let Some(fd) = self.admin.take() {
            self.close(fd);
        }
        self.close(self.notify);

        // Now every operation is cancelled, so this drains rather than blocks.
        // The bound is a backstop: a driver that kept handing back completions
        // for ever would hang shutdown, and exiting a little dirty beats that.
        let mut done = std::mem::take(&mut self.done);
        for _ in 0..1024 {
            done.clear();
            if self.driver.wait(&mut done).is_err() || done.is_empty() {
                break;
            }
            for completion in done.drain(..) {
                if let Some(buf) = completion.buf {
                    self.recycle_read(buf);
                }
            }
        }
        self.done = done;
    }
}

/// Encrypt whatever the state machine has queued for the client.
///
/// A no-op on a plaintext connection, and on a TLS connection whose handshake
/// has not finished — rustls has nowhere to put application data until it has.
/// What rustls accepts is removed from `outbox`; what it refuses stays there and
/// is offered again next time round, which is how a response larger than its
/// internal plaintext cap gets out in pieces rather than being truncated.
fn seal(conn: &mut Conn) {
    let Some(session) = conn.tls.as_mut() else {
        return;
    };
    if conn.outbox.is_empty() {
        return;
    }
    let sent = session.seal(&conn.outbox);
    conn.outbox.drain(..sent);
}

/// Whether anything is waiting to go to the client.
///
/// Under TLS that is the ciphertext queue and not the plaintext one: a
/// handshake with nothing to say at the application layer still has records to
/// write, and a connection whose plaintext has all been encrypted still has
/// bytes on the way out.
fn has_pending_out(conn: &Conn) -> bool {
    match &conn.tls {
        Some(session) => session.has_wire(),
        None => !conn.outbox.is_empty(),
    }
}

/// How much is queued for the client, for the backpressure bound.
///
/// Both buffers under TLS: the plaintext waiting to be encrypted and the
/// ciphertext waiting for the socket are both memory this connection is
/// holding on a slow reader's behalf.
fn pending_out_len(conn: &Conn) -> usize {
    match &conn.tls {
        Some(session) => conn.outbox.len() + session.wire_len(),
        None => conn.outbox.len(),
    }
}

/// Write one admin response.
fn write_admin(out: &mut Vec<u8>, status: u16, body: &[u8], content_type: Option<&str>, head: bool) {
    let content_type = content_type.unwrap_or("text/plain; charset=utf-8");
    let headers = [("Content-Type", content_type), ("Connection", "close")];
    let result = if head {
        ramjet_http::encode::response_head_only(out, status, &headers, body.len())
    } else {
        ramjet_http::encode::response(out, status, &headers, body)
    };
    if result.is_err() {
        out.extend_from_slice(b"HTTP/1.1 500 Internal Server Error\r\nContent-Length: 0\r\n\r\n");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_tag_round_trips_its_three_fields() {
        for (k, generation, fd) in [
            (kind::DOWN_READ, 0u32, 3i32),
            (kind::UP_WRITE, 0x00FF_FFFF, 1024),
            (kind::ACCEPT, 7, 65_535),
        ] {
            let user = tag(k, generation, fd);
            assert_eq!(tag_kind(user), k);
            assert_eq!(tag_generation(user), generation);
            assert_eq!(tag_fd(user), fd);
        }
    }

    #[test]
    fn a_generation_wraps_inside_its_field() {
        // 24 bits, so it must not bleed into the descriptor or the kind.
        let user = tag(kind::UP_READ, 0x00FF_FFFF, 12);
        assert_eq!(tag_kind(user), kind::UP_READ);
        assert_eq!(tag_fd(user), 12);
    }

    #[test]
    fn a_content_length_body_is_counted_down() {
        let mut body = Body::new(Framing::Length(5));
        assert_eq!(body.take(b"abc").expect("valid"), (3, false));
        assert_eq!(body.take(b"de-and-more").expect("valid"), (2, true));
    }

    #[test]
    fn an_empty_body_is_immediately_done() {
        let mut body = Body::new(Framing::Empty);
        assert!(body.is_done());
        assert_eq!(body.take(b"anything").expect("valid"), (0, true));
    }

    #[test]
    fn a_chunked_body_ends_at_its_terminator() {
        let mut body = Body::new(Framing::Chunked);
        let wire = b"5\r\nhello\r\n0\r\n\r\nNEXT";
        let (n, done) = body.take(wire).expect("valid");
        assert!(done);
        assert_eq!(&wire[n..], b"NEXT");
    }

    #[test]
    fn a_body_that_runs_to_close_claims_everything() {
        let mut body = Body::new(Framing::UntilClose);
        assert_eq!(body.take(b"whatever").expect("valid"), (8, false));
        assert!(!body.is_done(), "only the close ends it");
    }
}
