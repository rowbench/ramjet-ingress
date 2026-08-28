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
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use crate::host::FxHashMap;
use crate::path::PathType;

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

/// How many counter blocks each route carries.
///
/// Fixed rather than "one per core", because the memory is `routes × shards ×
/// 128` bytes and the coherence win flattens fast: four blocks already split a
/// hot route's traffic four ways, and a table of ten thousand routes costs 5 MB
/// at this number whether the pod has two cores or ninety-six. A serving
/// runtime picks its block by index modulo this, so on a pod with four cores or
/// fewer no two cores ever write the same line.
pub const ROUTE_STAT_SHARDS: usize = 4;

/// One route's counters, for one shard.
///
/// 128-byte alignment rather than 64, for the same reason
/// `ramjet_engine::metrics` gives: Apple Silicon's L2 works in 128-byte pairs
/// and x86's adjacent-line prefetcher pulls the neighbour anyway, so 64 would
/// still let two cores share a fetch.
///
/// Everything is `Relaxed`. These are observations, not synchronization.
#[derive(Debug, Default)]
#[repr(align(128))]
pub struct RouteCounters {
    requests: AtomicU64,
    errors_5xx: AtomicU64,
    /// Microseconds rather than milliseconds: there is no lock-free atomic add
    /// for `f64`, and a sub-millisecond upstream — which is most of them —
    /// would round to zero in a millisecond counter. The admin API divides.
    upstream_latency_micros: AtomicU64,
    upstream_latency_count: AtomicU64,
}

impl RouteCounters {
    /// Records one response served for this route.
    ///
    /// A 5xx is counted whether it came from the upstream or was generated
    /// here: from the route's point of view the request failed either way, and
    /// splitting the two would hide exactly the case where the backend is gone.
    #[inline]
    pub fn record_response(&self, status: u16) {
        self.requests.fetch_add(1, Ordering::Relaxed);
        if (500..600).contains(&status) {
            self.errors_5xx.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Records the time from upstream dispatch to response headers.
    #[inline]
    pub fn record_upstream_latency(&self, elapsed: Duration) {
        self.upstream_latency_micros.fetch_add(
            u64::try_from(elapsed.as_micros()).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
        self.upstream_latency_count.fetch_add(1, Ordering::Relaxed);
    }
}

/// One route's counters, merged across every shard.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct RouteTotals {
    /// Responses served for this route.
    pub requests: u64,
    /// Responses in the 5xx class, upstream or generated.
    pub errors_5xx: u64,
    /// Summed upstream latency, in microseconds.
    pub upstream_latency_micros: u64,
    /// Number of upstream latency observations.
    pub upstream_latency_count: u64,
}

impl RouteTotals {
    /// Summed upstream latency in milliseconds, which is what the admin API
    /// reports.
    pub fn upstream_latency_ms(&self) -> f64 {
        self.upstream_latency_micros as f64 / 1000.0
    }

    /// Field-wise `self - other`, saturating at zero.
    ///
    /// Two callers, one shape. Subtracting an earlier reading gives the traffic
    /// in a window; subtracting the canary block from the totals gives the
    /// stable share, because the canary block is a subset — see [`RouteSlot`].
    ///
    /// Saturating rather than wrapping because neither subtraction is
    /// guaranteed monotonic in practice: the two blocks are read at slightly
    /// different instants and a rebuild can swap the slab in between, so a
    /// canary count one ahead of the total it was taken from is a normal race
    /// and must read as zero rather than as eighteen quintillion.
    pub fn saturating_sub(&self, other: &RouteTotals) -> RouteTotals {
        RouteTotals {
            requests: self.requests.saturating_sub(other.requests),
            errors_5xx: self.errors_5xx.saturating_sub(other.errors_5xx),
            upstream_latency_micros: self
                .upstream_latency_micros
                .saturating_sub(other.upstream_latency_micros),
            upstream_latency_count: self
                .upstream_latency_count
                .saturating_sub(other.upstream_latency_count),
        }
    }

    /// 5xx as a percentage of requests, or `None` with no requests to divide by.
    pub fn error_percent(&self) -> Option<f64> {
        (self.requests > 0).then(|| self.errors_5xx as f64 * 100.0 / self.requests as f64)
    }

    /// Mean upstream latency in milliseconds, or `None` with no observations.
    ///
    /// The count can be zero while `requests` is not: a request answered with a
    /// locally generated 503 never reached an upstream and so has no latency to
    /// average.
    pub fn avg_latency_ms(&self) -> Option<f64> {
        (self.upstream_latency_count > 0)
            .then(|| self.upstream_latency_ms() / self.upstream_latency_count as f64)
    }
}

/// Every shard of one route's counters, twice.
///
/// # Why the canary block is a subset and not a sibling
///
/// A canary-diverted request is recorded in **both** blocks: the route's
/// totals stay the route's totals, and the canary block says how much of that
/// was the new backend. The other arrangement — stable in one, canary in the
/// other — would make every existing graph of a route's request rate step down
/// the moment somebody started a canary, which is precisely the graph an
/// operator is watching at that moment. Stable traffic is `totals - canary`,
/// and the one subtraction is a much better price than a discontinuity in a
/// dashboard.
///
/// The cost is on the diverted request only: four more relaxed adds, to a
/// separate 128-byte block, and nothing at all for a route with no canary.
#[derive(Debug, Default)]
pub struct RouteSlot {
    shards: [RouteCounters; ROUTE_STAT_SHARDS],
    canary_shards: [RouteCounters; ROUTE_STAT_SHARDS],
}

impl RouteSlot {
    /// The block a serving runtime writes to.
    ///
    /// `shard` is the runtime's own index; the remainder is taken here so no
    /// caller has to know how many blocks there are.
    #[inline]
    pub fn shard(&self, shard: usize) -> &RouteCounters {
        // The array is `ROUTE_STAT_SHARDS` long, so the remainder is always in
        // range; the fallback exists only to keep this panic-free.
        let index = shard % ROUTE_STAT_SHARDS;
        self.shards.get(index).unwrap_or(&self.shards[0])
    }

    /// The block a serving runtime writes to *in addition* when the canary took
    /// the request.
    #[inline]
    pub fn canary_shard(&self, shard: usize) -> &RouteCounters {
        let index = shard % ROUTE_STAT_SHARDS;
        self.canary_shards.get(index).unwrap_or(&self.canary_shards[0])
    }

    /// Every shard summed, for a scrape.
    pub fn totals(&self) -> RouteTotals {
        sum(&self.shards)
    }

    /// The canary-diverted share of [`totals`](Self::totals).
    pub fn canary_totals(&self) -> RouteTotals {
        sum(&self.canary_shards)
    }
}

fn sum(shards: &[RouteCounters; ROUTE_STAT_SHARDS]) -> RouteTotals {
    let mut totals = RouteTotals::default();
    for counters in shards {
        totals.requests += counters.requests.load(Ordering::Relaxed);
        totals.errors_5xx += counters.errors_5xx.load(Ordering::Relaxed);
        totals.upstream_latency_micros += counters.upstream_latency_micros.load(Ordering::Relaxed);
        totals.upstream_latency_count += counters.upstream_latency_count.load(Ordering::Relaxed);
    }
    totals
}

/// What makes a route the same route across a rebuild.
///
/// Deliberately not the position in the table: adding one Ingress renumbers
/// everything after it, and a counter that resets whenever a neighbour changes
/// is worse than no counter at all. The fields are the ones an operator would
/// use to say "that route" — where it is matched, how, and where it sends
/// traffic. A route whose backend changes is a different route for accounting
/// purposes, because its latency is no longer comparable.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RouteIdentity {
    /// Host as displayed: `example.com`, `*.example.com`, or `*`.
    pub host: Box<str>,
    /// The rule's path, as configured.
    pub path: Box<str>,
    /// How the path is compared.
    pub path_type: PathType,
    /// The backend the rule routes to.
    pub backend: Box<str>,
}

/// Per-route counters, carried across rebuilds by identity.
///
/// The same argument as [`BackendStats`], applied one level down: a request
/// counter that restarts every time an unrelated Ingress is edited cannot
/// answer "is this route getting traffic?", which is the only question it
/// exists for. So the counters live behind `Arc`s that successive tables share,
/// and a request that increments the old table's block after a rebuild
/// increments the same object the new table serves.
#[derive(Debug, Default)]
pub struct RouteStats {
    slots: Vec<Arc<RouteSlot>>,
    /// Build-time index for carrying state forward. Never touched on the hot
    /// path.
    by_identity: FxHashMap<RouteIdentity, Arc<RouteSlot>>,
}

impl RouteStats {
    /// Counters for the route at `index`.
    #[inline]
    pub fn slot(&self, index: u32) -> Option<&RouteSlot> {
        self.slots.get(index as usize).map(Arc::as_ref)
    }

    /// Number of routes tracked.
    pub fn len(&self) -> usize {
        self.slots.len()
    }

    /// Are there no routes at all?
    pub fn is_empty(&self) -> bool {
        self.slots.is_empty()
    }

    /// Builds the counters for a new generation, reusing every block whose
    /// route survived.
    ///
    /// `identities` is in stats-index order: entry `i` describes the route that
    /// will carry index `i`.
    pub(crate) fn rebuild(identities: &[RouteIdentity], previous: Option<&Self>) -> Self {
        let mut slots = Vec::with_capacity(identities.len());
        let mut by_identity =
            FxHashMap::with_capacity_and_hasher(identities.len(), Default::default());

        for identity in identities {
            let slot = previous
                .and_then(|p| p.by_identity.get(identity))
                .map_or_else(|| Arc::new(RouteSlot::default()), Arc::clone);
            by_identity.insert(identity.clone(), Arc::clone(&slot));
            slots.push(slot);
        }

        RouteStats {
            slots,
            by_identity,
        }
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

    fn route(host: &str, path: &str, backend: &str) -> RouteIdentity {
        RouteIdentity {
            host: host.into(),
            path: path.into(),
            path_type: PathType::Prefix,
            backend: backend.into(),
        }
    }

    #[test]
    fn route_counters_split_requests_from_failures() {
        let stats = RouteStats::rebuild(&[route("example.com", "/", "api")], None);
        let slot = stats.slot(0).expect("slot");
        slot.shard(0).record_response(200);
        slot.shard(0).record_response(503);
        slot.shard(0).record_response(404);

        let totals = slot.totals();
        assert_eq!(totals.requests, 3);
        assert_eq!(
            totals.errors_5xx, 1,
            "a 404 is the client's problem, not the route's"
        );
    }

    #[test]
    fn every_shard_is_summed_at_scrape() {
        let stats = RouteStats::rebuild(&[route("example.com", "/", "api")], None);
        let slot = stats.slot(0).expect("slot");
        for shard in 0..ROUTE_STAT_SHARDS {
            slot.shard(shard).record_response(200);
            slot.shard(shard).record_upstream_latency(Duration::from_millis(2));
        }

        let totals = slot.totals();
        assert_eq!(totals.requests, ROUTE_STAT_SHARDS as u64);
        assert_eq!(totals.upstream_latency_count, ROUTE_STAT_SHARDS as u64);
        assert_eq!(totals.upstream_latency_ms(), 2.0 * ROUTE_STAT_SHARDS as f64);
    }

    #[test]
    fn a_runtime_index_past_the_shard_count_wraps() {
        // Runtimes are numbered by core, and there are usually more cores than
        // shards; the wrap is what keeps every one of them writing somewhere.
        let stats = RouteStats::rebuild(&[route("example.com", "/", "api")], None);
        let slot = stats.slot(0).expect("slot");
        slot.shard(0).record_response(200);
        slot.shard(ROUTE_STAT_SHARDS).record_response(200);
        assert!(std::ptr::eq(slot.shard(0), slot.shard(ROUTE_STAT_SHARDS)));
        assert_eq!(slot.totals().requests, 2);
    }

    #[test]
    fn a_canary_request_lands_in_both_blocks() {
        // The property the split is built on: the route's totals stay the
        // route's totals, and the canary block says how much of that was the
        // new backend.
        let stats = RouteStats::rebuild(&[route("example.com", "/", "api")], None);
        let slot = stats.slot(0).expect("slot");

        slot.shard(0).record_response(200);

        slot.shard(0).record_response(500);
        slot.canary_shard(0).record_response(500);

        assert_eq!(slot.totals().requests, 2);
        assert_eq!(slot.totals().errors_5xx, 1);
        assert_eq!(slot.canary_totals().requests, 1);
        assert_eq!(slot.canary_totals().errors_5xx, 1);

        let stable = slot.totals().saturating_sub(&slot.canary_totals());
        assert_eq!(stable.requests, 1);
        assert_eq!(stable.errors_5xx, 0, "the 5xx was the canary's");
    }

    #[test]
    fn a_route_with_no_canary_has_an_empty_canary_block() {
        let stats = RouteStats::rebuild(&[route("example.com", "/", "api")], None);
        let slot = stats.slot(0).expect("slot");
        for shard in 0..ROUTE_STAT_SHARDS {
            slot.shard(shard).record_response(200);
        }
        assert_eq!(slot.totals().requests, ROUTE_STAT_SHARDS as u64);
        assert_eq!(slot.canary_totals(), RouteTotals::default());
    }

    #[test]
    fn canary_shards_are_summed_and_wrap_like_the_others() {
        let stats = RouteStats::rebuild(&[route("example.com", "/", "api")], None);
        let slot = stats.slot(0).expect("slot");
        for shard in 0..ROUTE_STAT_SHARDS {
            slot.canary_shard(shard).record_response(200);
            slot.canary_shard(shard)
                .record_upstream_latency(Duration::from_millis(4));
        }
        assert!(std::ptr::eq(
            slot.canary_shard(0),
            slot.canary_shard(ROUTE_STAT_SHARDS)
        ));

        let totals = slot.canary_totals();
        assert_eq!(totals.requests, ROUTE_STAT_SHARDS as u64);
        assert_eq!(totals.upstream_latency_count, ROUTE_STAT_SHARDS as u64);
        assert_eq!(totals.avg_latency_ms(), Some(4.0));
    }

    #[test]
    fn the_canary_block_survives_a_rebuild_with_its_route() {
        let identities = [route("example.com", "/", "api")];
        let first = RouteStats::rebuild(&identities, None);
        first.slot(0).expect("slot").canary_shard(0).record_response(503);

        let second = RouteStats::rebuild(&identities, Some(&first));
        assert_eq!(second.slot(0).expect("slot").canary_totals().errors_5xx, 1);
    }

    #[test]
    fn a_subtraction_that_would_go_negative_reads_as_zero() {
        // The two blocks are read at different instants, so a canary count one
        // ahead of the total it came from is a normal race.
        let totals = RouteTotals {
            requests: 1,
            ..RouteTotals::default()
        };
        let canary = RouteTotals {
            requests: 3,
            errors_5xx: 2,
            ..RouteTotals::default()
        };
        assert_eq!(totals.saturating_sub(&canary), RouteTotals::default());
    }

    #[test]
    fn rates_are_none_rather_than_a_division_by_zero() {
        let empty = RouteTotals::default();
        assert_eq!(empty.error_percent(), None);
        assert_eq!(empty.avg_latency_ms(), None);

        let served = RouteTotals {
            requests: 200,
            errors_5xx: 3,
            upstream_latency_micros: 0,
            // Every request answered locally: there is a rate but no latency.
            upstream_latency_count: 0,
        };
        assert_eq!(served.error_percent(), Some(1.5));
        assert_eq!(served.avg_latency_ms(), None);
    }

    #[test]
    fn each_shard_has_its_own_cache_line() {
        // The whole reason the blocks are aligned. If they ever share a line,
        // the hot path is silently paying for coherence traffic again.
        let slot = RouteSlot::default();
        for pair in [
            (slot.shard(0), slot.shard(1)),
            (slot.canary_shard(0), slot.canary_shard(1)),
            // The two blocks of one shard are written by the same core on the
            // same request, but they are still separate lines: sharing one
            // would put the canary counters back into the line the stable
            // counters are already contending for across shards.
            (slot.shard(0), slot.canary_shard(0)),
        ] {
            let first = std::ptr::from_ref(pair.0) as usize;
            let second = std::ptr::from_ref(pair.1) as usize;
            assert!(
                second.abs_diff(first) >= 128,
                "blocks are {} bytes apart",
                second.abs_diff(first)
            );
        }
    }

    #[test]
    fn a_surviving_route_keeps_its_counters() {
        let first = RouteStats::rebuild(
            &[route("example.com", "/", "api"), route("example.com", "/v2", "api")],
            None,
        );
        first.slot(0).expect("slot").shard(0).record_response(200);
        first.slot(1).expect("slot").shard(0).record_response(500);

        // `/v2` goes away, `/v3` appears, and `/` is renumbered behind it.
        let second = RouteStats::rebuild(
            &[route("example.com", "/v3", "api"), route("example.com", "/", "api")],
            Some(&first),
        );
        assert_eq!(
            second.slot(1).expect("slot").totals().requests,
            1,
            "a route that survived a rebuild must keep its counters, whatever its new index"
        );
        assert_eq!(second.slot(0).expect("slot").totals().requests, 0);
        assert_eq!(second.len(), 2);
    }

    /// The property that makes carry-forward more than a value copy: a request
    /// that started under the old generation and finishes under the new one
    /// must land in the block the new table serves.
    #[test]
    fn a_late_increment_on_the_old_table_is_visible_through_the_new_one() {
        let identities = [route("example.com", "/", "api")];
        let first = RouteStats::rebuild(&identities, None);
        let second = RouteStats::rebuild(&identities, Some(&first));

        first.slot(0).expect("slot").shard(0).record_response(200);
        assert_eq!(second.slot(0).expect("slot").totals().requests, 1);
    }

    #[test]
    fn a_route_whose_backend_moved_starts_over() {
        // Its latency is no longer comparable to what came before, so carrying
        // the numbers forward would describe two different things as one.
        let first = RouteStats::rebuild(&[route("example.com", "/", "api")], None);
        first.slot(0).expect("slot").shard(0).record_response(200);

        let second = RouteStats::rebuild(&[route("example.com", "/", "api-v2")], Some(&first));
        assert_eq!(second.slot(0).expect("slot").totals().requests, 0);
    }

    #[test]
    fn path_type_is_part_of_a_route_identity() {
        let exact = RouteIdentity {
            path_type: PathType::Exact,
            ..route("example.com", "/a", "api")
        };
        let first = RouteStats::rebuild(&[route("example.com", "/a", "api")], None);
        first.slot(0).expect("slot").shard(0).record_response(200);

        let second = RouteStats::rebuild(&[exact], Some(&first));
        assert_eq!(second.slot(0).expect("slot").totals().requests, 0);
    }
}
