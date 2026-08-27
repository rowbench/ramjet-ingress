//! Backends, endpoints, and endpoint selection.

use std::net::SocketAddr;

use crate::stats::BackendStats;

/// Index of a [`Backend`] within a [`RouteTable`](crate::RouteTable).
///
/// Rules store this rather than a name so that a rule stays 40 bytes and a
/// backend's endpoint list is stored once no matter how many paths point at it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct BackendId(pub u32);

/// One upstream address.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Endpoint {
    /// Where to connect.
    pub addr: SocketAddr,
    /// Relative share of traffic. `0` drains the endpoint without removing it.
    pub weight: u32,
}

impl Endpoint {
    /// An endpoint with the default weight of 1.
    pub fn new(addr: SocketAddr) -> Self {
        Endpoint { addr, weight: 1 }
    }

    /// An endpoint with an explicit weight.
    pub fn weighted(addr: SocketAddr, weight: u32) -> Self {
        Endpoint { addr, weight }
    }
}

/// How requests are spread across a backend's endpoints.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum LbPolicy {
    /// Rotate through endpoints in order. The default.
    #[default]
    RoundRobin,
    /// Pick uniformly at random, honouring weights.
    Random,
    /// Pick the endpoint with the fewest in-flight requests, honouring weights.
    LeastConn,
}

/// Largest weighted rotation we will precompute. A ring is one allocation per
/// backend built once per generation, so the cap is about bounding memory for
/// a pathological weight spread, not about the hot path.
pub(crate) const MAX_RING: u32 = 4096;

/// A named group of endpoints and the policy for choosing between them.
///
/// Immutable. Everything that changes during request handling lives in
/// [`BackendStats`], reached through [`Backend::stats_index`].
#[derive(Debug)]
pub struct Backend {
    name: Box<str>,
    endpoints: Vec<Endpoint>,
    policy: LbPolicy,
    stats_index: u32,
    /// Precomputed weighted rotation: `ring[i]` is an index into `endpoints`.
    /// `None` when every weight is equal, because a plain remainder is cheaper
    /// than an indirection and that is the overwhelmingly common case.
    ring: Option<Box<[u32]>>,
}

impl Backend {
    pub(crate) fn new(
        name: Box<str>,
        endpoints: Vec<Endpoint>,
        policy: LbPolicy,
        stats_index: u32,
    ) -> Self {
        let ring = build_ring(&endpoints);
        Backend {
            name,
            endpoints,
            policy,
            stats_index,
            ring,
        }
    }

    /// The backend's name, as the controller supplied it.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The endpoints, in the order their in-flight counters are indexed.
    pub fn endpoints(&self) -> &[Endpoint] {
        &self.endpoints
    }

    /// The load-balancing policy.
    pub fn policy(&self) -> LbPolicy {
        self.policy
    }

    /// This backend's index into [`BackendStats`].
    pub fn stats_index(&self) -> u32 {
        self.stats_index
    }
}

/// Builds the weighted rotation for a set of endpoints.
///
/// Returns `None` when weights are uniform (use a plain remainder instead) or
/// when every weight is zero, which would otherwise produce a backend that can
/// never be selected. Draining *all* endpoints is a configuration mistake, and
/// falling back to uniform selection fails less badly than a black hole.
fn build_ring(endpoints: &[Endpoint]) -> Option<Box<[u32]>> {
    let first = endpoints.first()?.weight;
    if endpoints.iter().all(|e| e.weight == first) {
        return None;
    }

    let total: u64 = endpoints.iter().map(|e| u64::from(e.weight)).sum();
    if total == 0 {
        return None;
    }

    // Scale down if the weights are extravagant, but never round a live
    // endpoint down to zero slots.
    let scale = |w: u32| -> u32 {
        if w == 0 {
            return 0;
        }
        if total <= u64::from(MAX_RING) {
            return w;
        }
        ((u64::from(w) * u64::from(MAX_RING) / total).max(1)) as u32
    };

    let mut remaining: Vec<u32> = endpoints.iter().map(|e| scale(e.weight)).collect();
    let slots: usize = remaining.iter().map(|&n| n as usize).sum();
    if slots == 0 {
        return None;
    }

    // Interleave rather than emitting each endpoint's slots contiguously.
    // A contiguous ring would send `weight` consecutive requests to the same
    // endpoint, which is a burst, not a rotation.
    let mut ring = Vec::with_capacity(slots);
    while ring.len() < slots {
        let mut emitted = false;
        for (i, left) in remaining.iter_mut().enumerate() {
            if *left > 0 {
                ring.push(i as u32);
                *left -= 1;
                emitted = true;
            }
        }
        if !emitted {
            break;
        }
    }

    Some(ring.into_boxed_slice())
}

/// Chooses an endpoint from `backend`.
///
/// `rng` is a caller-supplied random word, used only by [`LbPolicy::Random`].
/// Taking it as an argument rather than drawing it here keeps this crate
/// deterministic and free of a random-number dependency; the proxy holds a
/// per-core generator and passes a word from it.
///
/// Returns the endpoint and its index, which is what
/// [`BackendSlot::acquire`](crate::BackendSlot::acquire) needs to track the
/// request. `None` only when the backend has no endpoints.
pub fn select_endpoint<'b>(
    backend: &'b Backend,
    stats: &BackendStats,
    rng: u64,
) -> Option<(usize, &'b Endpoint)> {
    let len = backend.endpoints.len();
    if len == 0 {
        return None;
    }

    let index = match backend.policy {
        LbPolicy::RoundRobin => {
            let cursor = stats.slot(backend.stats_index)?.next_cursor() as usize;
            match &backend.ring {
                Some(ring) => *ring.get(cursor % ring.len())? as usize,
                None => cursor % len,
            }
        }
        LbPolicy::Random => match &backend.ring {
            Some(ring) => *ring.get((rng % ring.len() as u64) as usize)? as usize,
            None => (rng % len as u64) as usize,
        },
        LbPolicy::LeastConn => {
            let slot = stats.slot(backend.stats_index)?;
            let mut best = 0usize;
            // Compare load ratios without dividing: n_a/w_a < n_b/w_b becomes
            // n_a*w_b < n_b*w_a. Both sides fit in u64 comfortably.
            let mut best_n = u64::from(slot.inflight_count(0));
            let mut best_w = u64::from(backend.endpoints.first()?.weight.max(1));
            for (i, ep) in backend.endpoints.iter().enumerate().skip(1) {
                let n = u64::from(slot.inflight_count(i));
                let w = u64::from(ep.weight.max(1));
                if n * best_w < best_n * w {
                    best = i;
                    best_n = n;
                    best_w = w;
                }
            }
            best
        }
    };

    backend.endpoints.get(index).map(|ep| (index, ep))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn addr(port: u16) -> SocketAddr {
        SocketAddr::from(([10, 0, 0, 1], port))
    }

    fn backend(policy: LbPolicy, weights: &[u32]) -> (Backend, BackendStats) {
        let endpoints: Vec<Endpoint> = weights
            .iter()
            .enumerate()
            .map(|(i, &w)| Endpoint::weighted(addr(8080 + i as u16), w))
            .collect();
        let addrs: Vec<SocketAddr> = endpoints.iter().map(|e| e.addr).collect();
        let stats = BackendStats::rebuild(&[("b".into(), addrs)], None);
        (Backend::new("b".into(), endpoints, policy, 0), stats)
    }

    #[test]
    fn round_robin_rotates() {
        let (b, s) = backend(LbPolicy::RoundRobin, &[1, 1, 1]);
        let picks: Vec<usize> = (0..6)
            .filter_map(|_| select_endpoint(&b, &s, 0).map(|(i, _)| i))
            .collect();
        assert_eq!(picks, vec![0, 1, 2, 0, 1, 2]);
    }

    #[test]
    fn uniform_weights_skip_the_ring() {
        let (b, _) = backend(LbPolicy::RoundRobin, &[3, 3, 3]);
        assert!(b.ring.is_none(), "equal weights need no ring");
    }

    #[test]
    fn weighted_round_robin_honours_shares() {
        let (b, s) = backend(LbPolicy::RoundRobin, &[3, 1]);
        let ring = b.ring.as_ref().expect("non-uniform weights build a ring");
        assert_eq!(ring.len(), 4);

        let mut counts = [0usize; 2];
        for _ in 0..400 {
            if let Some((i, _)) = select_endpoint(&b, &s, 0) {
                counts[i] += 1;
            }
        }
        assert_eq!(counts, [300, 100]);
    }

    #[test]
    fn weighted_ring_is_interleaved_not_bursty() {
        let (b, _) = backend(LbPolicy::RoundRobin, &[3, 1]);
        let ring = b.ring.as_ref().expect("ring");
        // Interleaved: 0,1,0,0 -- not 0,0,0,1.
        assert_eq!(&ring[..2], &[0, 1]);
    }

    #[test]
    fn zero_weight_endpoint_is_drained() {
        let (b, s) = backend(LbPolicy::RoundRobin, &[1, 0]);
        for _ in 0..10 {
            let (i, _) = select_endpoint(&b, &s, 0).expect("an endpoint");
            assert_eq!(i, 0, "a zero-weight endpoint must never be selected");
        }
    }

    #[test]
    fn all_zero_weights_fall_back_to_uniform() {
        let (b, s) = backend(LbPolicy::RoundRobin, &[0, 0]);
        assert!(b.ring.is_none());
        assert!(select_endpoint(&b, &s, 0).is_some(), "must not black-hole");
    }

    #[test]
    fn random_uses_the_supplied_word() {
        let (b, s) = backend(LbPolicy::Random, &[1, 1, 1]);
        assert_eq!(select_endpoint(&b, &s, 0).map(|(i, _)| i), Some(0));
        assert_eq!(select_endpoint(&b, &s, 1).map(|(i, _)| i), Some(1));
        assert_eq!(select_endpoint(&b, &s, 5).map(|(i, _)| i), Some(2));
    }

    #[test]
    fn least_conn_picks_the_idlest() {
        let (b, s) = backend(LbPolicy::LeastConn, &[1, 1, 1]);
        let slot = s.slot(0).expect("slot");
        let _a = slot.acquire(0).expect("endpoint");
        let _b = slot.acquire(1).expect("endpoint");
        assert_eq!(select_endpoint(&b, &s, 0).map(|(i, _)| i), Some(2));
    }

    #[test]
    fn least_conn_scales_by_weight() {
        // Endpoint 1 is three times the size, so 2 in flight there is a
        // lighter load than 1 in flight on endpoint 0.
        let (b, s) = backend(LbPolicy::LeastConn, &[1, 3]);
        let slot = s.slot(0).expect("slot");
        let _a = slot.acquire(0).expect("endpoint");
        let _b = slot.acquire(1).expect("endpoint");
        let _c = slot.acquire(1).expect("endpoint");
        assert_eq!(select_endpoint(&b, &s, 0).map(|(i, _)| i), Some(1));
    }

    #[test]
    fn empty_backend_selects_nothing() {
        let (b, s) = backend(LbPolicy::RoundRobin, &[]);
        assert!(select_endpoint(&b, &s, 0).is_none());
    }

    #[test]
    fn extravagant_weights_are_capped() {
        let (b, _) = backend(LbPolicy::RoundRobin, &[1_000_000, 1]);
        let ring = b.ring.as_ref().expect("ring");
        assert!(ring.len() <= MAX_RING as usize, "ring was {}", ring.len());
        // The tiny endpoint still gets a slot rather than being rounded away.
        assert!(ring.contains(&1));
    }
}
