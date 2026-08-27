//! A per-thread source of random words.
//!
//! `ramjet-router` takes randomness as a `u64` argument rather than drawing it
//! itself — that is what keeps the matcher deterministic under test and free of
//! a random-number dependency. Somebody still has to supply the word, and that
//! somebody is here.
//!
//! This is xorshift64, seeded once per thread. It is not cryptographic and
//! nothing here should ever be used where that matters: the two callers are
//! weighted load-balancer selection and the canary weight roll, both of which
//! need "spread out", not "unpredictable". A `getrandom` call per request, or
//! even a `ChaCha` stream, would cost more than the routing decision it feeds.
//!
//! The generator is a thread-local `Cell`, so drawing a word is a load, three
//! shifts, three xors, and a store, with no atomics and no contention between
//! workers.

use std::cell::Cell;
use std::sync::atomic::{AtomicU64, Ordering};

/// Distinguishes threads seeded within the same nanosecond.
static THREADS: AtomicU64 = AtomicU64::new(0);

thread_local! {
    static STATE: Cell<u64> = Cell::new(seed());
}

/// SplitMix64's finalizer, used to smear the seed material.
fn mix(mut z: u64) -> u64 {
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

fn seed() -> u64 {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0x9E37_79B9_7F4A_7C15, |d| d.as_nanos() as u64);
    let thread = THREADS.fetch_add(1, Ordering::Relaxed);
    // xorshift64 is undefined for a zero state, so force a bit on. Doing it
    // after the mix rather than before keeps the low bits well distributed.
    mix(nanos ^ thread.wrapping_mul(0x9E37_79B9_7F4A_7C15)) | 1
}

/// Draws the next word.
#[inline]
pub fn next_u64() -> u64 {
    STATE.with(|state| {
        let mut x = state.get();
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        state.set(x);
        x
    })
}

/// Draws a word in `0..bound`, or `0` when `bound` is zero.
///
/// Uses Lemire's multiply-shift instead of a remainder: one 64-bit multiply
/// replaces a division, and the modulo bias it leaves behind is smaller than
/// the rounding error in any canary weight a human would write.
#[inline]
pub fn below(bound: u32) -> u32 {
    if bound == 0 {
        return 0;
    }
    (((next_u64() as u128) * (bound as u128)) >> 64) as u32
}

/// Writes 32 lowercase hex characters of randomness into `out`.
///
/// Used for `X-Request-Id` when the client did not supply one. 128 bits is the
/// same width as a UUID, which is what operators expect to paste into a log
/// query, without pulling in a UUID crate to format it.
pub fn hex_id(out: &mut [u8; 32]) {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let (hi, lo) = (next_u64(), next_u64());
    for (i, word) in [hi, lo].into_iter().enumerate() {
        for nibble in 0..16 {
            let shift = 60 - nibble * 4;
            out[i * 16 + nibble] = HEX[((word >> shift) & 0xF) as usize];
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn never_gets_stuck_at_zero() {
        // A zero state is xorshift's one fixed point; the seed guards against
        // it, and a stuck generator would silently pin every request to
        // endpoint 0.
        let mut seen_nonzero = false;
        for _ in 0..1000 {
            if next_u64() != 0 {
                seen_nonzero = true;
            }
        }
        assert!(seen_nonzero);
    }

    #[test]
    fn below_stays_in_range_and_covers_it() {
        let mut seen = HashSet::new();
        for _ in 0..10_000 {
            let v = below(7);
            assert!(v < 7, "{v} escaped the bound");
            seen.insert(v);
        }
        assert_eq!(seen.len(), 7, "some values in 0..7 were never drawn");
    }

    #[test]
    fn below_zero_is_zero() {
        assert_eq!(below(0), 0);
    }

    #[test]
    fn hex_ids_are_hex_and_distinct() {
        let mut a = [0u8; 32];
        let mut b = [0u8; 32];
        hex_id(&mut a);
        hex_id(&mut b);
        assert_ne!(a, b);
        assert!(a.iter().all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase()));
    }

    #[test]
    fn threads_do_not_share_a_stream() {
        let local: Vec<u64> = (0..8).map(|_| next_u64()).collect();
        let other = std::thread::spawn(|| (0..8).map(|_| next_u64()).collect::<Vec<u64>>())
            .join()
            .expect("thread");
        assert_ne!(local, other, "two threads drew the same sequence");
    }
}
