//! Idle upstream connections, per core, shared with nobody.
//!
//! `bench/PROFILE.md` measured the cost of a shared pool and then measured the
//! cost of removing it: one `current_thread` runtime per core with its own pool
//! was worth +7.7% on the iteration harness and +39.5% on the committed
//! benchmark, and requests per upstream connection went from ~590 to 8,179.
//! The price was named at the time and is the same here — *"a connection
//! returned to a full pool on one runtime cannot be reused by the other"* — and
//! it is a price worth paying twice.
//!
//! # Liveness, without a syscall
//!
//! A pooled connection has to be checked before it is handed out, or a proxy
//! hands a request to a socket the origin closed thirty seconds ago. Most
//! proxies either pay a `read` to find out or accept the race.
//!
//! A completion-based reactor gets it free. An idle connection sits with an
//! ordinary read submitted: if the upstream closes or sends anything, that read
//! completes and the connection is thrown away before anyone can be given it.
//! And when a request *does* take the connection, the read that was watching
//! for a close is already in flight and simply becomes the read that collects
//! the response — one fewer submission on the hot path, not one more.
//!
//! The race is not fully closed, and cannot be by anyone: an origin may close
//! between the moment a connection is taken and the moment the request lands on
//! it. That is what the retry path is for.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::os::fd::RawFd;
use std::time::{Duration, Instant};

/// An idle connection and when it became idle.
#[derive(Debug, Clone, Copy)]
struct Idle {
    fd: RawFd,
    since: Instant,
}

/// One core's idle upstream connections.
#[derive(Debug)]
pub struct Pool {
    by_endpoint: HashMap<SocketAddr, Vec<Idle>>,
    max_idle_per_host: usize,
    idle_timeout: Duration,
}

impl Pool {
    /// A pool holding at most `max_idle_per_host` connections per endpoint.
    pub fn new(max_idle_per_host: usize, idle_timeout: Duration) -> Self {
        Pool {
            by_endpoint: HashMap::new(),
            max_idle_per_host,
            idle_timeout,
        }
    }

    /// Take an idle connection to `addr`, if there is one.
    ///
    /// Most recently returned first: a connection that was in use a moment ago
    /// is the one least likely to have been closed at the far end, and reusing
    /// it keeps the colder ones available to expire.
    pub fn take(&mut self, addr: SocketAddr) -> Option<RawFd> {
        let idle = self.by_endpoint.get_mut(&addr)?;
        let taken = idle.pop()?;
        if idle.is_empty() {
            self.by_endpoint.remove(&addr);
        }
        Some(taken.fd)
    }

    /// Offer a connection back.
    ///
    /// Returns the descriptor again if the pool declined it, in which case the
    /// caller closes it. The pool never closes anything itself: descriptors
    /// belong to the reactor, and closing one behind its back is how an
    /// operation in flight lands on a recycled number.
    #[must_use = "a refused connection has to be closed by the caller"]
    pub fn put(&mut self, addr: SocketAddr, fd: RawFd) -> Option<RawFd> {
        let idle = self.by_endpoint.entry(addr).or_default();
        if idle.len() >= self.max_idle_per_host {
            return Some(fd);
        }
        idle.push(Idle {
            fd,
            since: Instant::now(),
        });
        None
    }

    /// Forget a connection the caller is taking back — because its parked read
    /// completed, which means the upstream closed it or spoke out of turn.
    ///
    /// Returns whether it was actually in the pool. `false` means the
    /// connection had already been handed to a request, and its read completing
    /// is that request's response rather than a death notice.
    pub fn remove(&mut self, addr: SocketAddr, fd: RawFd) -> bool {
        let Some(idle) = self.by_endpoint.get_mut(&addr) else {
            return false;
        };
        let Some(at) = idle.iter().position(|i| i.fd == fd) else {
            return false;
        };
        idle.remove(at);
        if idle.is_empty() {
            self.by_endpoint.remove(&addr);
        }
        true
    }

    /// Descriptors idle for longer than the timeout, removed from the pool.
    ///
    /// The caller closes them. An upstream with its own idle timeout will
    /// usually close first and the parked read will notice; this is for the
    /// ones that do not, so a quiet endpoint does not hold descriptors for the
    /// life of the process.
    pub fn expire(&mut self, now: Instant, out: &mut Vec<RawFd>) {
        self.by_endpoint.retain(|_, idle| {
            idle.retain(|entry| {
                let expired = now.duration_since(entry.since) >= self.idle_timeout;
                if expired {
                    out.push(entry.fd);
                }
                !expired
            });
            !idle.is_empty()
        });
    }

    /// Every descriptor the pool holds, removed from it. For shutdown.
    pub fn drain(&mut self, out: &mut Vec<RawFd>) {
        for (_, idle) in self.by_endpoint.drain() {
            out.extend(idle.into_iter().map(|i| i.fd));
        }
    }

    /// Whether the pool holds nothing.
    pub fn is_empty(&self) -> bool {
        self.by_endpoint.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn addr(port: u16) -> SocketAddr {
        SocketAddr::from(([127, 0, 0, 1], port))
    }

    fn pool() -> Pool {
        Pool::new(2, Duration::from_secs(90))
    }

    #[test]
    fn a_connection_comes_back_out() {
        let mut pool = pool();
        assert_eq!(pool.put(addr(1), 7), None);
        assert_eq!(pool.take(addr(1)), Some(7));
        assert_eq!(pool.take(addr(1)), None);
        assert!(pool.is_empty());
    }

    #[test]
    fn connections_are_kept_per_endpoint() {
        let mut pool = pool();
        assert_eq!(pool.put(addr(1), 7), None);
        assert_eq!(pool.put(addr(2), 8), None);
        assert_eq!(pool.take(addr(2)), Some(8));
        assert_eq!(pool.take(addr(1)), Some(7));
    }

    #[test]
    fn the_warmest_connection_is_reused_first() {
        let mut pool = pool();
        assert_eq!(pool.put(addr(1), 7), None);
        assert_eq!(pool.put(addr(1), 8), None);
        // 8 went back most recently, so it is the least likely to have been
        // closed at the far end.
        assert_eq!(pool.take(addr(1)), Some(8));
        assert_eq!(pool.take(addr(1)), Some(7));
    }

    #[test]
    fn a_full_pool_hands_the_connection_back_to_be_closed() {
        let mut pool = pool();
        assert_eq!(pool.put(addr(1), 7), None);
        assert_eq!(pool.put(addr(1), 8), None);
        // The third does not fit, and the pool says so rather than leaking it.
        assert_eq!(pool.put(addr(1), 9), Some(9));
        assert_eq!(pool.take(addr(1)), Some(8));
        assert_eq!(pool.take(addr(1)), Some(7));
        assert_eq!(pool.take(addr(1)), None, "9 was never kept");
    }

    #[test]
    fn removing_reports_whether_it_was_still_idle() {
        let mut pool = pool();
        assert_eq!(pool.put(addr(1), 7), None);
        assert!(pool.remove(addr(1), 7), "it was idle");
        assert!(!pool.remove(addr(1), 7), "and now it is not");
        // This is the distinction that keeps a response from being mistaken for
        // a death notice: a connection handed to a request is no longer here.
        assert!(!pool.remove(addr(2), 7), "never was, for this endpoint");
    }

    #[test]
    fn idle_connections_expire_and_are_handed_back() {
        let mut pool = Pool::new(4, Duration::from_millis(10));
        assert_eq!(pool.put(addr(1), 7), None);
        let mut expired = Vec::new();

        pool.expire(Instant::now(), &mut expired);
        assert!(expired.is_empty(), "not yet");

        pool.expire(Instant::now() + Duration::from_millis(20), &mut expired);
        assert_eq!(expired, vec![7]);
        assert!(pool.is_empty(), "an expired entry leaves the pool");
    }

    #[test]
    fn expiry_keeps_the_young_and_drops_the_old() {
        let mut pool = Pool::new(4, Duration::from_millis(50));
        assert_eq!(pool.put(addr(1), 7), None);
        std::thread::sleep(Duration::from_millis(60));
        assert_eq!(pool.put(addr(1), 8), None);

        let mut expired = Vec::new();
        pool.expire(Instant::now(), &mut expired);
        assert_eq!(expired, vec![7]);
        assert_eq!(pool.take(addr(1)), Some(8));
    }

    #[test]
    fn draining_yields_everything_once() {
        let mut pool = Pool::new(8, Duration::from_secs(90));
        for fd in [7, 8, 9] {
            assert_eq!(pool.put(addr(1), fd), None);
        }
        assert_eq!(pool.put(addr(2), 10), None);

        let mut all = Vec::new();
        pool.drain(&mut all);
        all.sort_unstable();
        assert_eq!(all, vec![7, 8, 9, 10]);
        assert!(pool.is_empty());

        let mut again = Vec::new();
        pool.drain(&mut again);
        assert!(again.is_empty(), "draining twice must not double-close");
    }
}
