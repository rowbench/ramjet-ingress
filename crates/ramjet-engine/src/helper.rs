//! One background thread for the two things the reactor has no operation for.
//!
//! The `ramjet` reactor knows five operations: accept, read, write, close, and
//! the pooled read. It has **no connect and no timer**, and both absences are
//! load-bearing for a proxy:
//!
//! - A proxy dials outward. A non-blocking `connect` returns `EINPROGRESS` and
//!   the only way to learn how it ended is to wait for the socket to become
//!   writable and then read `SO_ERROR`. There is no operation that waits for
//!   writability without also writing.
//! - Without a timer, an upstream that accepts a connection and then says
//!   nothing holds a connection, a descriptor and a buffer for ever.
//!
//! The obvious fix — submit the request bytes as a `Write` and let the reactor
//! park on writability — **does not work portably**, and it is worth writing
//! down why rather than rediscovering it. The kqueue backend performs the write
//! syscall eagerly at submission and parks only on `EWOULDBLOCK`. Measured on
//! macOS 15, `write(2)` to a socket in `SYN_SENT` returns **`ENOTCONN`**, not
//! `EWOULDBLOCK`, so the operation fails immediately and `EVFILT_WRITE` is
//! never registered. On Linux the same submission does work, because io_uring
//! turns the kernel's internal `EAGAIN` into an armed `POLLOUT` and reissues —
//! but relying on that would leave macOS, where this engine is developed,
//! unable to open an upstream connection at all.
//!
//! So: one thread, shared by every core, running `poll(2)` over the sockets
//! that are still connecting, with the poll timeout doubling as the clock. It
//! tells a core what happened by writing 12 bytes into a pipe that core has an
//! ordinary [`Op::Read`](ramjet::reactor::Op::Read) parked on, which is how a
//! completion-only reactor gets to hear about something it cannot submit.
//!
//! # Who owns a descriptor
//!
//! The contract that keeps this free of use-after-free: **the helper borrows a
//! connecting descriptor until it sends exactly one note about it.** A core
//! must not close a socket it has handed over, even if the client it was for
//! has gone; it marks it abandoned and closes when the note arrives. In return
//! the helper guarantees a note for every job, including one for a connect that
//! never finishes — which is why the deadline is enforced here and not by the
//! caller.

use std::collections::VecDeque;
use std::io;
use std::os::fd::{AsRawFd, IntoRawFd, OwnedFd, RawFd};
use std::sync::mpsc::{self, Receiver, Sender, TryRecvError};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use crate::sys;

/// Bytes in one note. Small and fixed, so a partial read is easy to resume.
pub const NOTE_LEN: usize = 12;

/// A tick note, carrying no descriptor.
const KIND_TICK: u32 = 0;
/// A connect-finished note.
const KIND_CONNECT: u32 = 1;

/// Something the helper thread is telling a core.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Note {
    /// The clock advanced; sweep deadlines.
    Tick,
    /// A connect finished. `err` is 0 for success, otherwise an errno.
    ///
    /// Receiving this returns ownership of `fd` to the core.
    Connected {
        /// The socket that was connecting.
        fd: RawFd,
        /// `0`, or the errno the connection failed with.
        err: i32,
    },
}

impl Note {
    fn encode(self) -> [u8; NOTE_LEN] {
        let (fd, kind, err) = match self {
            Note::Tick => (0, KIND_TICK, 0),
            Note::Connected { fd, err } => (fd as u32, KIND_CONNECT, err),
        };
        let mut out = [0u8; NOTE_LEN];
        out[0..4].copy_from_slice(&fd.to_le_bytes());
        out[4..8].copy_from_slice(&kind.to_le_bytes());
        out[8..12].copy_from_slice(&err.to_le_bytes());
        out
    }

    fn decode(bytes: &[u8]) -> Option<Note> {
        if bytes.len() < NOTE_LEN {
            return None;
        }
        let fd = u32::from_le_bytes(bytes[0..4].try_into().ok()?) as i32;
        let kind = u32::from_le_bytes(bytes[4..8].try_into().ok()?);
        let err = i32::from_le_bytes(bytes[8..12].try_into().ok()?);
        match kind {
            KIND_CONNECT => Some(Note::Connected { fd, err }),
            _ => Some(Note::Tick),
        }
    }
}

/// Reassembles notes from a byte stream that may split them.
///
/// A pipe is a stream, so a read can land mid-note. Twelve bytes is small
/// enough that this almost never happens and cheap enough that handling it
/// costs nothing.
#[derive(Debug, Default)]
pub struct NoteReader {
    partial: Vec<u8>,
}

impl NoteReader {
    /// Feed bytes read from the notify pipe; call `f` for each complete note.
    pub fn feed(&mut self, bytes: &[u8], mut f: impl FnMut(Note)) {
        if !self.partial.is_empty() {
            self.partial.extend_from_slice(bytes);
            while self.partial.len() >= NOTE_LEN {
                if let Some(note) = Note::decode(&self.partial[..NOTE_LEN]) {
                    f(note);
                }
                self.partial.drain(..NOTE_LEN);
            }
            return;
        }
        let input = bytes;
        let whole = input.len() / NOTE_LEN * NOTE_LEN;
        for chunk in input[..whole].chunks_exact(NOTE_LEN) {
            if let Some(note) = Note::decode(chunk) {
                f(note);
            }
        }
        self.partial.extend_from_slice(&input[whole..]);
    }
}

/// A connect the helper is watching.
struct Job {
    fd: RawFd,
    core: usize,
    deadline: Instant,
}

/// The handle a core uses to hand connects over.
///
/// Dropping it stops the thread: the wake pipe closes, `poll` reports the
/// hangup, and the loop returns. Any connect still in flight gets its note
/// first, so no descriptor is stranded.
pub struct Helper {
    /// `None` only while [`Helper::drop`] is tearing the thread down. Dropping
    /// the sender is what tells the loop to stop, so it has to happen *before*
    /// the join rather than when the struct's fields are dropped afterwards.
    jobs: Option<Sender<Job>>,
    wake: OwnedFd,
    thread: Option<JoinHandle<()>>,
}

impl Helper {
    /// Start the thread, returning it and one notify-pipe read end per core.
    ///
    /// Each core must park a read on its own pipe and hold it open for as long
    /// as it uses the helper.
    pub fn start(cores: usize, tick: Duration) -> io::Result<(Helper, Vec<OwnedFd>)> {
        let cores = cores.max(1);
        let (wake_r, wake_w) = sys::pipe_pair()?;
        let (tx, rx) = mpsc::channel();

        let mut readers = Vec::with_capacity(cores);
        let mut writers = Vec::with_capacity(cores);
        for _ in 0..cores {
            let (r, w) = sys::pipe_pair()?;
            readers.push(r);
            writers.push(w);
        }

        let thread = thread::Builder::new()
            .name("ramjet-helper".to_owned())
            .spawn(move || run(wake_r, rx, writers, tick))?;

        Ok((
            Helper {
                jobs: Some(tx),
                wake: wake_w,
                thread: Some(thread),
            },
            readers,
        ))
    }

    /// Watch `fd` until its connect finishes or `deadline` passes.
    ///
    /// Ownership of `fd` passes to the helper until it answers with exactly one
    /// [`Note::Connected`]. The caller must not close it before then.
    pub fn watch_connect(&self, fd: RawFd, core: usize, deadline: Instant) -> io::Result<()> {
        self.jobs
            .as_ref()
            .ok_or_else(|| io::Error::other("the helper thread is shutting down"))?
            .send(Job { fd, core, deadline })
            .map_err(|_| io::Error::other("the helper thread has stopped"))?;
        // A byte to break `poll`. A full pipe means a wake is already pending,
        // which does the same job.
        match sys::write(self.wake.as_raw_fd(), b"j") {
            Ok(_) => Ok(()),
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => Ok(()),
            Err(e) => Err(e),
        }
    }
}

impl Drop for Helper {
    fn drop(&mut self) {
        // Order matters, and getting it wrong deadlocks: the sender has to go
        // first, because a disconnected channel is what the loop treats as
        // "stop". The wake byte only gets it out of `poll` to notice. Closing
        // the pipe would do as well, but that happens when this struct's fields
        // drop — which is *after* the join below.
        self.jobs = None;
        let _ = sys::write(self.wake.as_raw_fd(), b"q");
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

/// The helper thread itself.
fn run(wake: OwnedFd, jobs: Receiver<Job>, writers: Vec<OwnedFd>, tick: Duration) {
    let mut pending: Vec<Job> = Vec::new();
    // Notes a core was not ready to receive. Bounded implicitly: a core has at
    // most one connect in flight per connection, and a tick is dropped rather
    // than queued.
    let mut backlog: Vec<VecDeque<[u8; NOTE_LEN]>> =
        writers.iter().map(|_| VecDeque::new()).collect();
    let mut next_tick = Instant::now() + tick;
    let mut poll_fds: Vec<libc::pollfd> = Vec::new();
    let mut scratch = [0u8; 256];

    loop {
        poll_fds.clear();
        poll_fds.push(libc::pollfd {
            fd: wake.as_raw_fd(),
            events: libc::POLLIN,
            revents: 0,
        });
        for job in &pending {
            poll_fds.push(libc::pollfd {
                fd: job.fd,
                events: libc::POLLOUT,
                revents: 0,
            });
        }
        // Only ask about writability on a pipe that owes something.
        let backlog_start = poll_fds.len();
        for (core, queue) in backlog.iter().enumerate() {
            if !queue.is_empty() {
                poll_fds.push(libc::pollfd {
                    fd: writers[core].as_raw_fd(),
                    events: libc::POLLOUT,
                    revents: 0,
                });
            }
        }

        let now = Instant::now();
        let mut wait = next_tick.saturating_duration_since(now);
        for job in &pending {
            wait = wait.min(job.deadline.saturating_duration_since(now));
        }
        let timeout = wait.as_millis().min(1000) as i32;

        if sys::poll(&mut poll_fds, timeout).is_err() {
            return;
        }

        let wake_events = poll_fds[0].revents;
        if wake_events & libc::POLLIN != 0 {
            while sys::read(wake.as_raw_fd(), &mut scratch).is_ok_and(|n| n > 0) {}
        }

        // A disconnected channel, or a closed wake pipe, is `Helper::drop`
        // saying stop.
        let mut stopping = wake_events & (libc::POLLHUP | libc::POLLERR | libc::POLLNVAL) != 0;
        loop {
            match jobs.try_recv() {
                Ok(job) => pending.push(job),
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => {
                    stopping = true;
                    break;
                }
            }
        }
        if stopping {
            // Answer everything still outstanding before leaving, so no core
            // waits for a note that will never come and no descriptor is
            // stranded in the borrowed state.
            for job in pending.drain(..) {
                push(
                    &mut backlog[job.core],
                    Note::Connected {
                        fd: job.fd,
                        err: libc::ECANCELED,
                    },
                );
            }
            flush_all(&writers, &mut backlog, true);
            return;
        }

        // Retire connects that finished or ran out of time. Walking backwards
        // keeps `swap_remove` from skipping an entry.
        let now = Instant::now();
        for i in (0..pending.len()).rev() {
            // `poll_fds[0]` is the wake pipe, so job `i` is at `i + 1`.
            let revents = poll_fds.get(i + 1).map_or(0, |p| p.revents);
            let timed_out = now >= pending[i].deadline;
            if revents == 0 && !timed_out {
                continue;
            }
            let job = pending.swap_remove(i);
            let err = if revents & libc::POLLNVAL != 0 {
                libc::EBADF
            } else if timed_out && revents == 0 {
                libc::ETIMEDOUT
            } else {
                // Writability says the connect is *over*, not that it worked.
                match sys::socket_error(job.fd) {
                    Ok(0) if revents & (libc::POLLERR | libc::POLLHUP) != 0 => libc::ECONNRESET,
                    Ok(code) => code,
                    Err(e) => e.raw_os_error().unwrap_or(libc::EIO),
                }
            };
            push(&mut backlog[job.core], Note::Connected { fd: job.fd, err });
        }

        if Instant::now() >= next_tick {
            next_tick = Instant::now() + tick;
            for queue in backlog.iter_mut() {
                // A tick is a hint to sweep deadlines; a core that is behind
                // will get the next one, so queueing them would only build a
                // backlog of stale hints.
                if queue.is_empty() {
                    queue.push_back(Note::Tick.encode());
                }
            }
        }

        let _ = backlog_start;
        flush_all(&writers, &mut backlog, false);
    }
}

fn push(queue: &mut VecDeque<[u8; NOTE_LEN]>, note: Note) {
    queue.push_back(note.encode());
}

/// Write what each core is owed, keeping whatever does not fit.
///
/// `blocking` is used only on the way out, where a short spin is preferable to
/// abandoning a descriptor a core is waiting for.
fn flush_all(writers: &[OwnedFd], backlog: &mut [VecDeque<[u8; NOTE_LEN]>], blocking: bool) {
    for (core, queue) in backlog.iter_mut().enumerate() {
        let mut attempts = 0;
        while let Some(note) = queue.front() {
            match sys::write(writers[core].as_raw_fd(), note) {
                Ok(n) if n == NOTE_LEN => {
                    queue.pop_front();
                }
                // A partial note would desynchronise the stream. A pipe writes
                // up to PIPE_BUF atomically and a note is twelve bytes, so this
                // cannot happen; dropping the connection's note would strand a
                // descriptor, so treat it as fatal for this queue rather than
                // silently corrupting the next one.
                Ok(_) => {
                    queue.clear();
                    break;
                }
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                    attempts += 1;
                    if !blocking || attempts > 100 {
                        break;
                    }
                    thread::sleep(Duration::from_millis(1));
                }
                Err(_) => {
                    // The core is gone; nothing to tell it.
                    queue.clear();
                    break;
                }
            }
        }
    }
}

/// Hand a descriptor to the reactor's numbering without owning it any more.
///
/// A convenience for the one place a socket crosses from `OwnedFd` into the
/// reactor's care, which is where leaks would otherwise be easy.
pub fn release(fd: OwnedFd) -> RawFd {
    fd.into_raw_fd()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::TcpListener;

    /// Read notes from a core's pipe until `want` of them arrive or time runs
    /// out.
    fn collect(pipe: &OwnedFd, want: usize, within: Duration) -> Vec<Note> {
        let mut reader = NoteReader::default();
        let mut notes = Vec::new();
        let deadline = Instant::now() + within;
        let mut buf = [0u8; 256];
        while notes.len() < want && Instant::now() < deadline {
            let mut poll_fds = [libc::pollfd {
                fd: pipe.as_raw_fd(),
                events: libc::POLLIN,
                revents: 0,
            }];
            if sys::poll(&mut poll_fds, 50).unwrap_or(0) == 0 {
                continue;
            }
            match sys::read(pipe.as_raw_fd(), &mut buf) {
                Ok(n) if n > 0 => reader.feed(&buf[..n], |note| notes.push(note)),
                _ => break,
            }
        }
        notes
    }

    #[test]
    fn a_note_survives_a_round_trip() {
        for note in [
            Note::Tick,
            Note::Connected { fd: 7, err: 0 },
            Note::Connected {
                fd: 1234,
                err: libc::ECONNREFUSED,
            },
        ] {
            assert_eq!(Note::decode(&note.encode()), Some(note));
        }
    }

    #[test]
    fn notes_reassemble_from_a_split_stream() {
        let wire: Vec<u8> = [
            Note::Connected { fd: 3, err: 0 },
            Note::Tick,
            Note::Connected { fd: 9, err: 61 },
        ]
        .iter()
        .flat_map(|n| n.encode())
        .collect();

        // Every split point must yield the same three notes.
        for split in 0..wire.len() {
            let mut reader = NoteReader::default();
            let mut seen = Vec::new();
            reader.feed(&wire[..split], |n| seen.push(n));
            reader.feed(&wire[split..], |n| seen.push(n));
            assert_eq!(
                seen,
                vec![
                    Note::Connected { fd: 3, err: 0 },
                    Note::Tick,
                    Note::Connected { fd: 9, err: 61 },
                ],
                "split at {split}"
            );
        }
    }

    #[test]
    fn a_successful_connect_is_reported() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("a listener");
        let addr = listener.local_addr().expect("an address");
        let (helper, pipes) = Helper::start(1, Duration::from_secs(60)).expect("a helper");

        let (sock, _) = sys::tcp_connect(addr).expect("a connect");
        let fd = sock.into_raw_fd();
        helper
            .watch_connect(fd, 0, Instant::now() + Duration::from_secs(5))
            .expect("watched");

        let notes = collect(&pipes[0], 1, Duration::from_secs(5));
        assert_eq!(notes, vec![Note::Connected { fd, err: 0 }]);
        // SAFETY: the note returned ownership, and the helper no longer refers
        // to this descriptor.
        unsafe { sys::close(fd) };
    }

    #[test]
    fn a_refused_connect_is_reported_with_its_errno() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("a listener");
        let addr = listener.local_addr().expect("an address");
        drop(listener);
        let (helper, pipes) = Helper::start(1, Duration::from_secs(60)).expect("a helper");

        let Ok((sock, _)) = sys::tcp_connect(addr) else {
            return; // refused synchronously; the helper is not involved
        };
        let fd = sock.into_raw_fd();
        helper
            .watch_connect(fd, 0, Instant::now() + Duration::from_secs(5))
            .expect("watched");

        let notes = collect(&pipes[0], 1, Duration::from_secs(5));
        assert_eq!(
            notes,
            vec![Note::Connected {
                fd,
                err: libc::ECONNREFUSED
            }]
        );
        // SAFETY: ownership came back with the note.
        unsafe { sys::close(fd) };
    }

    #[test]
    fn a_connect_that_never_finishes_times_out() {
        // 10.255.255.1 is RFC 1918 space with nothing behind it, so the SYN
        // goes unanswered and the socket sits in SYN_SENT. Without the
        // deadline enforced here, the descriptor would be held for ever.
        let addr: std::net::SocketAddr = "10.255.255.1:80".parse().expect("an address");
        let (helper, pipes) = Helper::start(1, Duration::from_secs(60)).expect("a helper");

        let Ok((sock, connected)) = sys::tcp_connect(addr) else {
            return; // some networks refuse it outright
        };
        if connected {
            return; // and some answer, which is not this test
        }
        let fd = sock.into_raw_fd();
        helper
            .watch_connect(fd, 0, Instant::now() + Duration::from_millis(200))
            .expect("watched");

        let notes = collect(&pipes[0], 1, Duration::from_secs(5));
        assert_eq!(notes.len(), 1, "exactly one note per job");
        let Note::Connected { fd: got, err } = notes[0] else {
            panic!("expected a connect note, got {:?}", notes[0]);
        };
        assert_eq!(got, fd);
        assert!(
            err == libc::ETIMEDOUT || err == libc::EHOSTUNREACH || err == libc::ENETUNREACH,
            "unexpected errno {err}"
        );
        // SAFETY: ownership came back with the note.
        unsafe { sys::close(fd) };
    }

    #[test]
    fn the_clock_ticks() {
        let (_helper, pipes) = Helper::start(2, Duration::from_millis(20)).expect("a helper");
        for pipe in &pipes {
            let notes = collect(pipe, 2, Duration::from_secs(3));
            assert!(notes.len() >= 2, "every core is ticked, got {notes:?}");
            assert!(notes.iter().all(|n| *n == Note::Tick), "{notes:?}");
        }
    }

    #[test]
    fn dropping_the_helper_stops_the_thread() {
        let (helper, pipes) = Helper::start(1, Duration::from_millis(20)).expect("a helper");
        drop(helper);
        // The thread is joined by `drop`, so by here it has exited. Reading the
        // pipe must not block for ever: its write end went with the thread.
        let notes = collect(&pipes[0], 100, Duration::from_millis(200));
        assert!(notes.len() < 100, "the ticker kept running after the drop");
    }
}
