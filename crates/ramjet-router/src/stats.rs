//! Load-balancer state that outlives the table it is selected through.
//!
//! Round-robin cursors and in-flight request counts are the only mutable state
//! in the router, and they must not be reset when the configuration changes.
//! That is the whole argument against the nginx reload model: adding one
//! Ingress should not make every backend forget how many requests it is
//! currently serving.
//!
//! So the counters do not live in [`RouteTable`](crate::RouteTable). They live
//! behind `Arc`s that successive tables *share*. When the controller rebuilds,
//! [`BackendStats::rebuild`] carries every surviving counter forward by
//! identity — backend name for cursors, socket address for in-flight counts —
//! so a request that started under generation 7 and finishes under generation 8
//! decrements the same `AtomicU32` it incremented. Requests in flight across a
//! swap stay accounted for; only genuinely new endpoints start at zero.
//!
//! Everything here is `Relaxed`. These are load-balancing hints, not
//! synchronization: nothing else is published through them, and paying for
//! acquire/release ordering to distribute requests slightly more evenly would
//! be a bad trade.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use crate::host::FxHashMap;

/// Mutable selection state for one backend.
#[derive(Debug)]
pub struct BackendSlot {
    /// Round-robin position. Wraps freely; only its remainder is used.
    cursor: AtomicU32,
    /// In-flight request count per endpoint, parallel to
    /// [`Backend::endpoints`](crate::Backend::endpoints).
    inflight: Vec<Arc<AtomicU32>>,
    /// Endpoint identity, used only when carrying counters across a rebuild.
    addrs: Vec<SocketAddr>,
}

impl BackendSlot {
    /// In-flight count for the endpoint at `index`.
    #[inline]
    pub fn inflight(&self, index: usize) -> Option<&AtomicU32> {
        self.inflight.get(index).map(Arc::as_ref)
    }

    /// Current in-flight count, or `0` if `index` is out of range.
    #[inline]
    pub fn inflight_count(&self, index: usize) -> u32 {
        self.inflight
            .get(index)
            .map_or(0, |c| c.load(Ordering::Relaxed))
    }

    /// Registers a request against the endpoint at `index`, returning a guard
    /// that releases it on drop.
    ///
    /// The proxy holds this for the lifetime of the upstream request. Because
    /// the guard borrows the shared counter rather than the table, it stays
    /// correct if the route table is swapped underneath it.
    pub fn acquire(&self, index: usize) -> Option<InflightGuard<'_>> {
        let counter = self.inflight.get(index).map(Arc::as_ref)?;
        counter.fetch_add(1, Ordering::Relaxed);
        Some(InflightGuard { counter })
    }

    /// Next round-robin position.
    #[inline]
    pub(crate) fn next_cursor(&self) -> u32 {
        self.cursor.fetch_add(1, Ordering::Relaxed)
    }
}

/// Decrements an endpoint's in-flight count when dropped.
#[derive(Debug)]
pub struct InflightGuard<'a> {
    counter: &'a AtomicU32,
}

impl Drop for InflightGuard<'_> {
    fn drop(&mut self) {
        self.counter.fetch_sub(1, Ordering::Relaxed);
    }
}

/// The slab of per-backend selection state.
///
/// Indexed by [`Backend::stats_index`](crate::Backend::stats_index), which the
/// builder assigns. The slab itself is immutable once built; only the atomics
/// inside it change.
#[derive(Debug, Default)]
pub struct BackendStats {
    slots: Vec<Arc<BackendSlot>>,
    /// Build-time index for carrying state forward. Never touched on the hot
    /// path.
    by_name: FxHashMap<Box<str>, Arc<BackendSlot>>,
}

impl BackendStats {
    /// Selection state for `index`.
    #[inline]
    pub fn slot(&self, index: u32) -> Option<&BackendSlot> {
        self.slots.get(index as usize).map(Arc::as_ref)
    }

    /// Number of backends tracked.
    pub fn len(&self) -> usize {
        self.slots.len()
    }

    /// Is the slab empty?
    pub fn is_empty(&self) -> bool {
        self.slots.is_empty()
    }

    /// Builds the slab for a new generation, reusing counters from `previous`
    /// wherever a backend or endpoint survived.
    ///
    /// `specs` is in `stats_index` order: entry `i` describes the backend that
    /// will carry index `i`.
    pub(crate) fn rebuild(specs: &[(Box<str>, Vec<SocketAddr>)], previous: Option<&Self>) -> Self {
        let mut slots = Vec::with_capacity(specs.len());
        let mut by_name = FxHashMap::with_capacity_and_hasher(specs.len(), Default::default());

        for (name, addrs) in specs {
            let old = previous.and_then(|p| p.by_name.get(name.as_ref()));
            let slot = match old {
                // Unchanged endpoint list: keep the entire slot, so the cursor
                // and every counter are literally the same objects.
                Some(prev) if prev.addrs == *addrs => Arc::clone(prev),
                // The endpoint list moved. Rebuild the slot, but reuse the
                // counter for each address that survived so requests already
                // in flight to it stay counted.
                _ => {
                    let inflight = addrs
                        .iter()
                        .map(|addr| {
                            old.and_then(|prev| {
                                let i = prev.addrs.iter().position(|a| a == addr)?;
                                prev.inflight.get(i).map(Arc::clone)
                            })
                            .unwrap_or_default()
                        })
                        .collect();
                    // The cursor is a rotation position, not an invariant;
                    // copying its value keeps the rotation roughly where it
                    // was without pinning the old allocation.
                    let cursor = old.map_or(0, |prev| prev.cursor.load(Ordering::Relaxed));
                    Arc::new(BackendSlot {
                        cursor: AtomicU32::new(cursor),
                        inflight,
                        addrs: addrs.clone(),
                    })
                }
            };
            by_name.insert(name.clone(), Arc::clone(&slot));
            slots.push(slot);
        }

        BackendStats { slots, by_name }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn addr(port: u16) -> SocketAddr {
        SocketAddr::from(([10, 0, 0, 1], port))
    }

    fn spec(name: &str, ports: &[u16]) -> (Box<str>, Vec<SocketAddr>) {
        (name.into(), ports.iter().copied().map(addr).collect())
    }

    #[test]
    fn unchanged_backend_keeps_the_same_slot() {
        let specs = vec![spec("api", &[8080, 8081])];
        let first = BackendStats::rebuild(&specs, None);
        let second = BackendStats::rebuild(&specs, Some(&first));

        let a = first.slot(0).expect("slot");
        let b = second.slot(0).expect("slot");
        assert!(std::ptr::eq(a, b), "identical specs must reuse the slot");
    }

    #[test]
    fn inflight_survives_a_rebuild_that_changes_endpoints() {
        let first = BackendStats::rebuild(&[spec("api", &[8080, 8081])], None);
        let guard = first.slot(0).expect("slot").acquire(0).expect("endpoint");
        assert_eq!(first.slot(0).expect("slot").inflight_count(0), 1);

        // Endpoint 8081 goes away and 8082 appears; 8080 keeps serving.
        let second = BackendStats::rebuild(&[spec("api", &[8080, 8082])], Some(&first));
        assert_eq!(
            second.slot(0).expect("slot").inflight_count(0),
            1,
            "the surviving endpoint must keep its in-flight count"
        );
        assert_eq!(second.slot(0).expect("slot").inflight_count(1), 0);

        // The request finishes under the *new* generation and still balances.
        drop(guard);
        assert_eq!(second.slot(0).expect("slot").inflight_count(0), 0);
    }

    #[test]
    fn cursor_position_carries_forward() {
        let first = BackendStats::rebuild(&[spec("api", &[8080, 8081])], None);
        for _ in 0..5 {
            first.slot(0).expect("slot").next_cursor();
        }
        let second = BackendStats::rebuild(&[spec("api", &[8080, 8082])], Some(&first));
        assert_eq!(second.slot(0).expect("slot").next_cursor(), 5);
    }

    #[test]
    fn new_backend_starts_at_zero() {
        let first = BackendStats::rebuild(&[spec("api", &[8080])], None);
        first.slot(0).expect("slot").next_cursor();
        let second = BackendStats::rebuild(&[spec("api", &[8080]), spec("web", &[9090])], Some(&first));
        assert_eq!(second.slot(1).expect("slot").next_cursor(), 0);
        assert_eq!(second.len(), 2);
    }

    #[test]
    fn guard_releases_on_drop() {
        let stats = BackendStats::rebuild(&[spec("api", &[8080])], None);
        let slot = stats.slot(0).expect("slot");
        {
            let _a = slot.acquire(0).expect("endpoint");
            let _b = slot.acquire(0).expect("endpoint");
            assert_eq!(slot.inflight_count(0), 2);
        }
        assert_eq!(slot.inflight_count(0), 0);
    }

    #[test]
    fn acquire_rejects_unknown_endpoint() {
        let stats = BackendStats::rebuild(&[spec("api", &[8080])], None);
        assert!(stats.slot(0).expect("slot").acquire(7).is_none());
        assert!(stats.slot(9).is_none());
    }
}
