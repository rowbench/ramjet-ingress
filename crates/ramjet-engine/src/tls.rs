//! TLS termination on the reactor, driven sans-io.
//!
//! # Why this needed no reactor change
//!
//! rustls never touches a socket. It moves bytes between two buffers: give it
//! ciphertext, take plaintext out; give it plaintext, take ciphertext out. That
//! is the same shape [`crate::codec`] already has, so terminating TLS here is a
//! byte-plumbing job that fits behind the existing `Op::Read`/`Op::Write`
//! contract untouched. Three layers, none of which knows about the others:
//!
//! ```text
//!   reactor   ciphertext in and out of the socket, and nothing else
//!   rustls    ciphertext <-> plaintext
//!   codec     plaintext <-> HTTP/1.1 messages
//! ```
//!
//! The sibling runtime's `wss_echo` example proved the shape before this
//! existed. What is added here is the two-phase accept, the certificate store
//! shared with the hyper engine, and the byte-exact handoff that
//! [`Session::into_replay`] makes possible.
//!
//! # Two phases, because the ClientHello is a decision point
//!
//! A [`Session`] does not start as a `ServerConnection`. It starts as a
//! [`rustls::server::Acceptor`], which reads the ClientHello and *stops* —
//! before a `ServerConfig` has been chosen, and so before anything has been
//! committed to serving the connection here. That is what makes engine dispatch
//! possible: the ALPN list the client offered is readable at that point, and a
//! connection this engine should not serve can be handed away with its bytes
//! intact rather than terminated and re-proxied.
//!
//! Every ciphertext byte fed during that phase is kept for exactly that reason.
//! A few hundred bytes, freed the moment [`Session::accept`] commits the
//! connection to this engine.
//!
//! # What TLS costs here
//!
//! The plaintext path stops being the wire path. Where the plaintext engine
//! relays a response body out of the very buffer it arrived in, under TLS every
//! byte is copied at least once in each direction — rustls reads plaintext out
//! of its own buffer and writes ciphertext into another, and there is nothing
//! to be clever about. kTLS is what would win that back, by moving the record
//! layer into the kernel; it is a separate piece of work and is not attempted
//! here.

use std::io::{self, Read as _, Write as _};
use std::sync::Arc;

use rustls::server::{Accepted, Acceptor, ServerConnection};
use rustls::ServerConfig;

/// How much plaintext is lifted out of rustls per call.
///
/// A stack buffer on the reactor thread, reused across every drain: a TLS
/// record holds at most 16 KiB of plaintext, so a larger one could never be
/// filled in a single read.
const PLAIN_CHUNK: usize = 16 * 1024;

/// What the client asked to speak, read off the ClientHello.
///
/// Only the two answers this engine acts on. A client offering `h2` is one the
/// uring lane cannot serve — it speaks HTTP/1.1 and nothing else — so the
/// distinction decides which engine the connection belongs to rather than which
/// protocol gets negotiated.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Offer {
    /// The client offered `h2`.
    Http2,
    /// The client offered `http/1.1`, or no ALPN at all.
    ///
    /// No ALPN means HTTP/1.1 by convention: HTTP/2 over TLS is only reachable
    /// through ALPN, so a client that wanted it would have said so.
    Http11,
}

/// How far [`Session::feed`] got.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Step {
    /// More ciphertext is needed before anything can be decided.
    NeedMore,
    /// The ClientHello has arrived and nothing has been committed to it.
    ///
    /// The caller now chooses: [`Session::accept`] to serve the connection
    /// here, or [`Session::into_replay`] to hand it away with every byte it has
    /// consumed.
    Hello(Offer),
    /// The session is live. Whatever it decrypted has been appended.
    Live,
}

/// One connection's TLS state.
pub struct Session {
    state: State,
    /// Every ciphertext byte fed before the ClientHello was resolved.
    ///
    /// Kept only during the hello phase and dropped by [`Session::accept`]: its
    /// one purpose is to let a connection be replayed from its first byte on
    /// another engine.
    replay: Vec<u8>,
    /// Ciphertext that arrived after the ClientHello resolved but before the
    /// caller decided what to do with it.
    ///
    /// The acceptor carries everything *it* read across into the live
    /// connection, but bytes arriving after that point were never offered to
    /// it, so they are held here and fed in by [`Session::accept`]. Normally
    /// empty: a caller decides inside the completion that produced
    /// [`Step::Hello`], before another read can land.
    pending: Vec<u8>,
    /// Ciphertext waiting to go out on the socket.
    ///
    /// One write is in flight per descriptor at a time, so this is where a
    /// burst of records coalesces into a single submission.
    wire_out: Vec<u8>,
    /// What the ClientHello offered, once it has been read.
    offer: Option<Offer>,
    /// Whether the completed handshake has already been counted.
    counted: bool,
}

enum State {
    /// Collecting the ClientHello. Nothing has been committed.
    Hello(Box<Acceptor>),
    /// The ClientHello is in and readable, and no `ServerConfig` has been
    /// chosen. The only state a connection can still be handed away from.
    Ready(Box<Accepted>),
    /// Handshaking or established.
    Live(Box<ServerConnection>),
    /// Fatally failed. Only the alert rustls queued is left to flush.
    Dead,
}

impl std::fmt::Debug for Session {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let phase = match self.state {
            State::Hello(_) => "hello",
            State::Ready(_) => "ready",
            State::Live(_) => "live",
            State::Dead => "dead",
        };
        f.debug_struct("Session")
            .field("phase", &phase)
            .field("wire_out", &self.wire_out.len())
            .finish()
    }
}

impl Default for Session {
    fn default() -> Self {
        Session::new()
    }
}

impl Session {
    /// A session waiting for a ClientHello.
    pub fn new() -> Session {
        Session {
            state: State::Hello(Box::default()),
            replay: Vec::new(),
            pending: Vec::new(),
            wire_out: Vec::new(),
            offer: None,
            counted: false,
        }
    }

    /// Feed ciphertext from the socket, appending any plaintext to `plain`.
    ///
    /// Returns what the session is waiting for now. In the [`Step::Hello`] case
    /// nothing has been consumed irreversibly and the connection may still be
    /// taken away.
    pub fn feed(&mut self, cipher: &[u8], plain: &mut Vec<u8>) -> Result<Step, rustls::Error> {
        match self.state {
            // A dead session has nothing left to learn from the client. Its
            // queued alert is still flushed by the caller's write path.
            State::Dead => Ok(Step::Live),
            State::Hello(_) => self.feed_hello(cipher),
            // Bytes arriving while the caller has not yet decided are held
            // rather than parsed: deciding is the caller's turn, and consuming
            // more here would make the handoff lossy.
            State::Ready(_) => {
                self.replay.extend_from_slice(cipher);
                self.pending.extend_from_slice(cipher);
                Ok(Step::Hello(self.offer.unwrap_or(Offer::Http11)))
            }
            State::Live(_) => {
                self.feed_live(cipher, plain)?;
                Ok(Step::Live)
            }
        }
    }

    fn feed_hello(&mut self, cipher: &[u8]) -> Result<Step, rustls::Error> {
        let State::Hello(acceptor) = &mut self.state else {
            return Ok(Step::Live);
        };
        self.replay.extend_from_slice(cipher);

        let mut cursor = cipher;
        while !cursor.is_empty() {
            let before = cursor.len();
            // Reading from a slice cannot fail; a full internal buffer is
            // reported as a short read rather than an error.
            if acceptor.read_tls(&mut cursor).is_err() {
                break;
            }
            if cursor.len() == before {
                break;
            }
        }

        match acceptor.accept() {
            Ok(None) => Ok(Step::NeedMore),
            Ok(Some(accepted)) => {
                let offer = offer_of(&accepted);
                self.offer = Some(offer);
                self.state = State::Ready(Box::new(accepted));
                Ok(Step::Hello(offer))
            }
            Err((error, mut alert)) => {
                // rustls has an alert describing the failure. Queue it rather
                // than dropping the socket silently: a client whose ClientHello
                // was refused deserves to be told which part of it was refused.
                let _ = alert.write(&mut self.wire_out);
                self.state = State::Dead;
                Err(error)
            }
        }
    }

    fn feed_live(&mut self, cipher: &[u8], plain: &mut Vec<u8>) -> Result<(), rustls::Error> {
        let State::Live(mut conn) = std::mem::replace(&mut self.state, State::Dead) else {
            return Ok(());
        };
        let mut cursor = cipher;
        let mut failure = None;
        while !cursor.is_empty() {
            let before = cursor.len();
            if conn.read_tls(&mut cursor).is_err() {
                break;
            }
            // Processing frees the internal buffer, which is what lets the next
            // turn of this loop take more of a read that did not fit in one.
            if let Err(error) = conn.process_new_packets() {
                failure = Some(error);
                break;
            }
            drain_plaintext(&mut conn, plain);
            if cursor.len() == before {
                break;
            }
        }
        // A handshake step can want to write without any plaintext moving, so
        // this runs whether or not the loop above produced anything — and on
        // the failure path it is what carries rustls's alert to the client.
        pump(&mut conn, &mut self.wire_out);
        match failure {
            None => {
                self.state = State::Live(conn);
                Ok(())
            }
            Some(error) => Err(error),
        }
    }

    /// Commit the connection to this engine, using `config`.
    ///
    /// Only meaningful after [`Step::Hello`]. The replay buffer is dropped
    /// here: past this point the connection cannot be handed anywhere else,
    /// because the session holds handshake state no other engine can be given.
    ///
    /// Any plaintext already in the client's first flight is appended to
    /// `plain` — under TLS 1.3 the client's Finished and its first application
    /// records travel together, so a request can be complete before the
    /// handshake's last byte has been written back.
    pub fn accept(
        &mut self,
        config: &Arc<ServerConfig>,
        plain: &mut Vec<u8>,
    ) -> Result<(), rustls::Error> {
        let State::Ready(accepted) = std::mem::replace(&mut self.state, State::Dead) else {
            return Ok(());
        };
        self.replay = Vec::new();

        let mut conn = match accepted.into_connection(Arc::clone(config)) {
            Ok(conn) => Box::new(conn),
            Err((error, mut alert)) => {
                let _ = alert.write(&mut self.wire_out);
                return Err(error);
            }
        };
        // Records the acceptor read past the ClientHello came across inside the
        // connection and have not been looked at yet.
        if let Err(error) = conn.process_new_packets() {
            pump(&mut conn, &mut self.wire_out);
            return Err(error);
        }
        drain_plaintext(&mut conn, plain);
        pump(&mut conn, &mut self.wire_out);
        self.state = State::Live(conn);

        if !self.pending.is_empty() {
            let pending = std::mem::take(&mut self.pending);
            self.feed_live(&pending, plain)?;
        }
        Ok(())
    }

    /// Take the raw ciphertext this session consumed, for a handoff.
    ///
    /// Every byte the client sent, in order, including the ClientHello. The
    /// receiving engine replays these before reading the socket again, so the
    /// handoff is invisible to the client.
    pub fn into_replay(self) -> Vec<u8> {
        self.replay
    }

    /// Hand plaintext to rustls and collect the ciphertext it produces.
    ///
    /// Returns how many bytes of `plain` were accepted, which can be fewer than
    /// were offered. The interleaving inside is not decoration: rustls caps how
    /// much plaintext it will hold before you drain it, so a reply larger than
    /// that cap gets a short write, and draining first is what makes room for
    /// the rest. Getting this wrong silently truncates every large response
    /// while passing every small one.
    pub fn seal(&mut self, plain: &[u8]) -> usize {
        let State::Live(conn) = &mut self.state else {
            // A dead session has only its alert left to flush, and one still
            // handshaking has no channel to put application data in.
            return 0;
        };
        let mut sent = 0;
        loop {
            while conn.wants_write() {
                // Writing into a Vec cannot fail.
                if conn.write_tls(&mut self.wire_out).is_err() {
                    break;
                }
            }
            if sent == plain.len() {
                break;
            }
            match conn.writer().write(&plain[sent..]) {
                Ok(0) | Err(_) => break,
                Ok(n) => sent += n,
            }
        }
        sent
    }

    /// Ciphertext waiting to go out.
    pub fn wire_out(&mut self) -> &mut Vec<u8> {
        &mut self.wire_out
    }

    /// Whether anything is queued for the socket.
    pub fn has_wire(&self) -> bool {
        !self.wire_out.is_empty()
    }

    /// How much ciphertext is queued, for a caller's memory bound.
    pub fn wire_len(&self) -> usize {
        self.wire_out.len()
    }

    /// Whether the handshake has completed.
    pub fn established(&self) -> bool {
        matches!(&self.state, State::Live(conn) if !conn.is_handshaking())
    }

    /// Whether this session has failed and can only be flushed and closed.
    pub fn is_dead(&self) -> bool {
        matches!(self.state, State::Dead)
    }

    /// Give up on this session, keeping whatever alert is already queued.
    pub fn kill(&mut self) {
        self.state = State::Dead;
    }

    /// Report a completed handshake exactly once.
    ///
    /// Returns true the first time the handshake is seen to have finished, so a
    /// caller can count it without keeping a flag of its own.
    pub fn take_established(&mut self) -> bool {
        if self.counted || !self.established() {
            return false;
        }
        self.counted = true;
        true
    }

    /// Ask rustls for a `close_notify`, so the peer can tell a finished
    /// response from a truncated one.
    pub fn send_close_notify(&mut self) {
        if let State::Live(conn) = &mut self.state {
            conn.send_close_notify();
            pump(conn, &mut self.wire_out);
        }
    }

    /// The protocol ALPN settled on, once the handshake has finished.
    pub fn alpn(&self) -> Option<&[u8]> {
        match &self.state {
            State::Live(conn) => conn.alpn_protocol(),
            _ => None,
        }
    }

    /// The name the client asked for, once the ClientHello has been read.
    pub fn server_name(&self) -> Option<&str> {
        match &self.state {
            State::Live(conn) => conn.server_name(),
            _ => None,
        }
    }
}

/// Everything rustls has decrypted, appended to `out`.
fn drain_plaintext(conn: &mut ServerConnection, out: &mut Vec<u8>) {
    let mut scratch = [0u8; PLAIN_CHUNK];
    loop {
        match conn.reader().read(&mut scratch) {
            // Zero is a clean end of the plaintext stream and `WouldBlock` is
            // "nothing more decrypted yet". Neither is an error and both mean
            // stop.
            Ok(0) => return,
            Ok(n) => out.extend_from_slice(&scratch[..n]),
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => return,
            Err(_) => return,
        }
    }
}

/// Drain whatever rustls wants to write into `wire`.
fn pump(conn: &mut ServerConnection, wire: &mut Vec<u8>) {
    while conn.wants_write() {
        // Writing into a Vec cannot fail.
        if conn.write_tls(wire).is_err() {
            return;
        }
    }
}

/// Whether a ClientHello offered HTTP/2.
fn offer_of(accepted: &Accepted) -> Offer {
    let offered_h2 = accepted
        .client_hello()
        .alpn()
        .is_some_and(|mut protocols| protocols.any(|p| p == b"h2"));
    if offered_h2 {
        Offer::Http2
    } else {
        Offer::Http11
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_new_session_waits_for_a_client_hello() {
        let mut session = Session::new();
        let mut plain = Vec::new();
        // A single byte is a valid prefix of a record and decides nothing.
        assert_eq!(
            session.feed(&[0x16], &mut plain).expect("a valid prefix"),
            Step::NeedMore
        );
        assert!(plain.is_empty());
        assert!(!session.established());
    }

    #[test]
    fn bytes_seen_before_the_hello_resolves_are_kept_for_a_handoff() {
        let mut session = Session::new();
        let mut plain = Vec::new();
        let _ = session.feed(&[0x16, 0x03, 0x01], &mut plain);
        let _ = session.feed(&[0x00, 0x05], &mut plain);
        assert_eq!(
            session.into_replay(),
            vec![0x16, 0x03, 0x01, 0x00, 0x05],
            "a handoff replays every byte, in order"
        );
    }

    #[test]
    fn a_hello_that_is_not_tls_at_all_fails_rather_than_waiting() {
        let mut session = Session::new();
        let mut plain = Vec::new();
        // An HTTP request on the TLS port. rustls rejects the record type
        // rather than blocking for a ClientHello that will never come.
        let outcome = session.feed(b"GET / HTTP/1.1\r\nHost: x\r\n\r\n", &mut plain);
        assert!(outcome.is_err(), "plaintext HTTP is not a ClientHello");
        assert!(session.is_dead());
    }

    #[test]
    fn a_dead_session_accepts_no_plaintext() {
        let mut session = Session::new();
        let mut plain = Vec::new();
        let _ = session.feed(b"not tls at all, by any reading", &mut plain);
        assert_eq!(
            session.seal(b"a response nobody can receive"),
            0,
            "application data must not be queued onto a failed session"
        );
    }

    #[test]
    fn a_session_that_never_started_reports_nothing_negotiated() {
        let session = Session::new();
        assert!(session.alpn().is_none());
        assert!(session.server_name().is_none());
        assert!(!session.is_dead());
    }
}
