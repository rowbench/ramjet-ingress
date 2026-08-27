//! A fast non-cryptographic random source, per thread.
//!
//! Two things on the request path need randomness, and neither needs a secure
//! generator: the 32 hex characters of an `X-Request-Id`, and the roll that
//! decides whether a request goes to a canary. Both are observability and
//! traffic-splitting rather than security, so this is xorshift64 in a
//! `Cell`, and it costs a few nanoseconds instead of a syscall.
//!
//! This mirrors `ramjet_proxy::rng` — the format of an id and the distribution
//! of a roll are what the two engines have to agree on, not the bit pattern.
//!
//! **Not for anything that must be unguessable.** A request id from this is a
//! correlation handle, not a capability; treat it as public.

use std::cell::Cell;
use std::time::{SystemTime, UNIX_EPOCH};

const HEX: &[u8; 16] = b"0123456789abcdef";

thread_local! {
    static STATE: Cell<u64> = Cell::new(seed());
}

/// A per-thread starting point that differs between threads and between runs.
///
/// The clock alone is not enough: several serving threads can start inside the
/// same nanosecond, and identical seeds would give every core the same canary
/// rolls in the same order.
fn seed() -> u64 {
    thread_local! {
        static COUNTER: Cell<u64> = const { Cell::new(0) };
    }
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0x9e37_79b9_7f4a_7c15);
    let n = COUNTER.with(|c| {
        let n = c.get().wrapping_add(1);
        c.set(n);
        n
    });
    // SplitMix64's finalizer, to spread two nearly-identical seeds apart.
    let mut z = nanos ^ n.wrapping_mul(0x9e37_79b9_7f4a_7c15);
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    // xorshift64 is stuck at zero forever if it ever reaches it.
    (z ^ (z >> 31)) | 1
}

/// The next 64 random bits.
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

/// A number in `0..bound`, or 0 when `bound` is 0.
///
/// Lemire's multiply-shift rather than `% bound`: no division on the request
/// path, and the modulo bias is confined to a range no canary weight can reach.
pub fn below(bound: u32) -> u32 {
    if bound == 0 {
        return 0;
    }
    ((u128::from(next_u64()) * u128::from(bound)) >> 64) as u32
}

/// Fill 32 bytes with lowercase hexadecimal — 128 bits of request id.
pub fn hex_id(out: &mut [u8; 32]) {
    for (word, half) in [next_u64(), next_u64()].into_iter().zip(out.chunks_mut(16)) {
        for (nibble, slot) in half.iter_mut().enumerate() {
            *slot = HEX[((word >> (60 - nibble * 4)) & 0xF) as usize];
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn an_id_is_32_lowercase_hex_characters() {
        let mut id = [0u8; 32];
        hex_id(&mut id);
        assert_eq!(id.len(), 32);
        assert!(
            id.iter()
                .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase()),
            "{}",
            String::from_utf8_lossy(&id)
        );
    }

    #[test]
    fn ids_do_not_repeat() {
        let mut seen = HashSet::new();
        for _ in 0..10_000 {
            let mut id = [0u8; 32];
            hex_id(&mut id);
            assert!(seen.insert(id), "a repeated id in 10k draws");
        }
    }

    #[test]
    fn below_stays_inside_its_bound() {
        for bound in [1u32, 2, 3, 100, 1000, u32::MAX] {
            for _ in 0..1000 {
                assert!(below(bound) < bound, "bound {bound}");
            }
        }
        assert_eq!(below(0), 0);
    }

    #[test]
    fn below_is_roughly_uniform() {
        // A canary weight is only as honest as this distribution. 100k draws
        // into 10 buckets: each should land near 10,000, and a broken
        // generator (a stuck bit, a bad shift) misses by far more than this.
        let mut buckets = [0u32; 10];
        for _ in 0..100_000 {
            buckets[below(10) as usize] += 1;
        }
        for (i, &count) in buckets.iter().enumerate() {
            assert!(
                (9_000..=11_000).contains(&count),
                "bucket {i} got {count}, expected about 10,000"
            );
        }
    }

    #[test]
    fn two_threads_do_not_share_a_sequence() {
        let mine: Vec<u64> = (0..8).map(|_| next_u64()).collect();
        let theirs = std::thread::spawn(|| (0..8).map(|_| next_u64()).collect::<Vec<_>>())
            .join()
            .expect("thread");
        assert_ne!(mine, theirs, "per-thread seeds collided");
    }
}
