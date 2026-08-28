//! Traffic mirroring: a second copy of a request that nobody waits for.
//!
//! A mirror sends the same method, path, headers and body to a second backend
//! and throws the answer away. It is how a rewrite gets production traffic
//! before it gets production responsibility, and how a load test stops being a
//! synthetic load test.
//!
//! # What this module decides, and what it does not
//!
//! Everything here is one struct and one comparison. The router's whole job for
//! a mirror is to say *whether* this request is sampled and *where* the copy
//! goes; the proxy owns the far harder half — buffering a body, bounding a
//! queue, and making sure none of it can touch the request the client is
//! actually waiting for. Keeping the split here means the sampling rule is
//! testable against integers, exactly as [`CanarySpec`](crate::CanarySpec) is.
//!
//! # Sampling is not the canary weight
//!
//! A canary weight *diverts*: the request goes to one backend or the other, and
//! the denominator is configurable because ingress-nginx made it so. A mirror
//! *duplicates*, so there is no traffic to split and no reason for a
//! denominator — the percentage is out of 100 and nothing else. They are also
//! independent: a route may carry both, and a canary-diverted request is
//! mirrored on the same terms as a stable one, because the mirror is a property
//! of the rule rather than of where the rule sent this particular request.

use crate::backend::BackendId;

/// The denominator for [`MirrorSpec::percent`]. Fixed, unlike a canary's.
///
/// Public because it is half the contract of [`MirrorSpec::sample`]: the caller
/// draws the roll, so it has to know the range to draw it from.
pub const MIRROR_PERCENT_TOTAL: u32 = 100;

/// A mirror attached to a route.
#[derive(Debug)]
pub struct MirrorSpec {
    backend: BackendId,
    percent: u32,
    host: Option<Box<str>>,
}

impl MirrorSpec {
    pub(crate) fn new(backend: BackendId, percent: u32, host: Option<Box<str>>) -> Self {
        MirrorSpec {
            backend,
            percent: percent.min(MIRROR_PERCENT_TOTAL),
            host,
        }
    }

    /// The backend the copy is sent to.
    pub fn backend(&self) -> BackendId {
        self.backend
    }

    /// Share of matching requests that are copied, out of 100.
    pub fn percent(&self) -> u32 {
        self.percent
    }

    /// `Host` header to send instead of the client's, if one was configured.
    ///
    /// A shadow deployment usually answers to a different name than production
    /// does, and a mirrored request carrying the production `Host` would be
    /// routed by whatever is in front of it — possibly straight back to
    /// production, which is the one outcome a mirror must never produce.
    pub fn host(&self) -> Option<&str> {
        self.host.as_deref()
    }

    /// Whether this request is sampled.
    ///
    /// `roll` is a caller-supplied number in `0..100`. `percent: 0` never
    /// samples and `percent: 100` always does, with no rounding in between:
    /// `roll < percent` over a uniform roll is exactly `percent` percent.
    #[inline]
    pub fn sample(&self, roll: u32) -> bool {
        self.percent > 0 && roll < self.percent
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec(percent: u32) -> MirrorSpec {
        MirrorSpec::new(BackendId(1), percent, None)
    }

    #[test]
    fn a_hundred_percent_mirrors_every_roll() {
        let m = spec(100);
        for roll in 0..100 {
            assert!(m.sample(roll));
        }
    }

    #[test]
    fn zero_percent_mirrors_nothing() {
        // The annotation exists so a mirror can be turned off without deleting
        // it, and "off" has to mean off for every roll rather than for most.
        let m = spec(0);
        for roll in 0..100 {
            assert!(!m.sample(roll));
        }
    }

    #[test]
    fn the_split_is_exact_at_the_boundary() {
        let m = spec(30);
        assert!(m.sample(0));
        assert!(m.sample(29));
        assert!(!m.sample(30));
        assert!(!m.sample(99));
    }

    #[test]
    fn a_percent_above_the_total_is_clamped_not_wrapped() {
        // The controller rejects these, but a clamp here means a number that
        // slipped through mirrors everything rather than nothing.
        assert_eq!(spec(1000).percent(), 100);
        assert!(spec(1000).sample(99));
    }

    #[test]
    fn the_host_override_is_optional_and_borrowed() {
        assert_eq!(spec(1).host(), None);
        let m = MirrorSpec::new(BackendId(2), 50, Some("shadow.example.com".into()));
        assert_eq!(m.host(), Some("shadow.example.com"));
        assert_eq!(m.backend(), BackendId(2));
    }
}
