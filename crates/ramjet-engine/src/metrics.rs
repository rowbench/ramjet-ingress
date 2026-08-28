//! Per-core counters, merged at scrape into the exposition the hyper engine
//! emits.
//!
//! # Why not just share the hyper engine's `Metrics`
//!
//! Because sharing is the thing this engine exists to stop doing.
//! `ramjet_proxy::Metrics` is one `Arc` of atomics that every core increments,
//! and `bench/PROFILE.md` records that as *measured* to be free on the hyper
//! path — a diagnostic build that removed metrics recording entirely came out
//! at -0.9%, inside the noise. That result is not transferable: it was measured
//! against a request that cost 24 microseconds, and the whole point here is to
//! make the request cost less. A cost that is 0.4% of 24us is 1.2% of 8us.
//!
//! So each core owns a cache-line-aligned block it alone writes, and a scrape
//! sums them. The hot path is one relaxed add to a line no other core touches;
//! the merge happens a few times a minute on the admin listener.
//!
//! # Why the rendering is duplicated rather than reused
//!
//! `render_prometheus` is a method on the hyper engine's `Metrics`, and there
//! is no way to hand it a set of numbers from somewhere else without changing
//! that crate. Duplicating a formatter is a real risk — the two drift, and
//! `/metrics` quietly changes shape when someone passes `--engine uring` — so
//! the duplication is pinned by a differential test that drives both engines'
//! counters through the same events and asserts the two strings are *equal*.
//! If anyone edits either formatter, that test fails.

use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::time::Duration;

/// Upper bounds, in seconds, of the upstream latency histogram. Identical to
/// the hyper engine's; the differential test would fail otherwise.
const BUCKETS: [f64; 12] = [
    0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 5.0, 10.0,
];

/// Status classes tracked by `requests_total`.
const CLASSES: [&str; 6] = ["1xx", "2xx", "3xx", "4xx", "5xx", "other"];

/// One core's counters, on their own cache lines.
///
/// 128-byte alignment rather than 64: Apple Silicon's L2 works in 128-byte
/// pairs, and the x86 adjacent-line prefetcher pulls the neighbour anyway, so
/// 64 would still let two cores share a fetch.
#[derive(Debug, Default)]
#[repr(align(128))]
pub struct CoreMetrics {
    requests: [AtomicU64; CLASSES.len()],
    buckets: [AtomicU64; BUCKETS.len()],
    sum_nanos: AtomicU64,
    latency_count: AtomicU64,
    active: AtomicI64,
    connect_failures: AtomicU64,
    retries: AtomicU64,
    timeouts: AtomicU64,
    route_misses: AtomicU64,
    tls_handshakes: AtomicU64,
    tls_handshake_failures: AtomicU64,
}

impl CoreMetrics {
    /// Count a response by its status class.
    ///
    /// Anything outside 100..=599 lands in `other`, which is where a bug shows
    /// up rather than where it hides.
    pub fn response(&self, status: u16) {
        let index = match status / 100 {
            1 => 0,
            2 => 1,
            3 => 2,
            4 => 3,
            5 => 4,
            _ => 5,
        };
        self.requests[index].fetch_add(1, Ordering::Relaxed);
    }

    /// Observe the time from upstream dispatch to response headers.
    pub fn upstream_latency(&self, elapsed: Duration) {
        let seconds = elapsed.as_secs_f64();
        // Inclusive upper bound, first match — and an observation past the last
        // bound increments no bucket, surviving only in `+Inf` and `_count`.
        if let Some(i) = BUCKETS.iter().position(|&bound| seconds <= bound) {
            self.buckets[i].fetch_add(1, Ordering::Relaxed);
        }
        self.sum_nanos.fetch_add(
            u64::try_from(elapsed.as_nanos()).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
        self.latency_count.fetch_add(1, Ordering::Relaxed);
    }

    /// A downstream connection was accepted.
    pub fn connection_opened(&self) {
        self.active.fetch_add(1, Ordering::Relaxed);
    }

    /// A downstream connection closed, however it closed.
    pub fn connection_closed(&self) {
        self.active.fetch_sub(1, Ordering::Relaxed);
    }

    /// An attempt to reach an endpoint failed at connect time.
    pub fn connect_failure(&self) {
        self.connect_failures.fetch_add(1, Ordering::Relaxed);
    }

    /// A request was re-dispatched to a different endpoint.
    pub fn retry(&self) {
        self.retries.fetch_add(1, Ordering::Relaxed);
    }

    /// An upstream sent no response headers before the deadline.
    pub fn timeout(&self) {
        self.timeouts.fetch_add(1, Ordering::Relaxed);
    }

    /// A request matched no route and no default backend.
    pub fn route_miss(&self) {
        self.route_misses.fetch_add(1, Ordering::Relaxed);
    }

    /// A TLS handshake completed.
    pub fn tls_handshake(&self) {
        self.tls_handshakes.fetch_add(1, Ordering::Relaxed);
    }

    /// A TLS handshake failed, however it failed.
    pub fn tls_handshake_failure(&self) {
        self.tls_handshake_failures.fetch_add(1, Ordering::Relaxed);
    }
}

/// Every core's counters, and the merge that turns them into an exposition.
#[derive(Debug)]
pub struct EngineMetrics {
    cores: Box<[CoreMetrics]>,
    /// The mirror worker's own counters, when mirroring is wired up.
    ///
    /// Not per-core, and deliberately not: three of the four are written by a
    /// tokio task that outlives the request that queued the copy, so there is
    /// no core to attribute them to. They are the hyper engine's `Metrics`,
    /// shared with the worker, which is also what keeps the numbers on the two
    /// lanes the same shape.
    mirror: Option<std::sync::Arc<ramjet_proxy::Metrics>>,
}

impl EngineMetrics {
    /// Counters for `cores` serving threads.
    pub fn new(cores: usize) -> Self {
        EngineMetrics {
            cores: (0..cores.max(1)).map(|_| CoreMetrics::default()).collect(),
            mirror: None,
        }
    }

    /// Counters for `cores` serving threads, reporting a mirror worker's
    /// numbers alongside their own.
    pub fn with_mirror(cores: usize, mirror: std::sync::Arc<ramjet_proxy::Metrics>) -> Self {
        EngineMetrics {
            mirror: Some(mirror),
            ..EngineMetrics::new(cores)
        }
    }

    /// One of the mirror series, or zero where mirroring is not wired up.
    ///
    /// Zero rather than an absent series: a dashboard that loses a line when an
    /// operator turns a feature off looks like an outage.
    fn mirror(&self, pick: impl Fn(&ramjet_proxy::Metrics) -> u64) -> u64 {
        self.mirror.as_deref().map_or(0, pick)
    }

    /// The block belonging to one core.
    ///
    /// # Panics
    ///
    /// If `index` is past the core count this was built with.
    pub fn core(&self, index: usize) -> &CoreMetrics {
        &self.cores[index]
    }

    /// Downstream connections currently being served, across every core.
    pub fn active_connections(&self) -> i64 {
        self.sum_i64(|c| &c.active)
    }

    /// Responses counted in one status class, across every core.
    pub fn responses(&self, class: &str) -> u64 {
        CLASSES
            .iter()
            .position(|c| *c == class)
            .map_or(0, |i| self.sum(|c| &c.requests[i]))
    }

    fn sum(&self, pick: impl Fn(&CoreMetrics) -> &AtomicU64) -> u64 {
        self.cores
            .iter()
            .map(|c| pick(c).load(Ordering::Relaxed))
            .sum()
    }

    fn sum_i64(&self, pick: impl Fn(&CoreMetrics) -> &AtomicI64) -> i64 {
        self.cores
            .iter()
            .map(|c| pick(c).load(Ordering::Relaxed))
            .sum()
    }

    /// Render every series in the Prometheus text exposition format.
    ///
    /// Byte-identical to `ramjet_proxy::Metrics::render_prometheus` for the
    /// same counter values, including the details that are easy to get wrong:
    /// `le="1"` rather than `le="1.0"` (Rust's `f64` Display), `_sum` at
    /// exactly six decimal places, and the series in this order.
    ///
    /// `pinned` is always `false` on this engine — it serves a table read once
    /// at startup, so there is no generation history to roll back to. The
    /// series is emitted anyway, for the same reason the TLS handshake counters
    /// are: a dashboard that loses a series when an operator changes engine
    /// looks like an outage.
    pub fn render_prometheus(&self, generation: u64, pinned: bool) -> String {
        use std::fmt::Write as _;
        let mut out = String::with_capacity(2048);

        out.push_str("# HELP ramjet_requests_total Responses served, by status class.\n");
        out.push_str("# TYPE ramjet_requests_total counter\n");
        for (i, class) in CLASSES.iter().enumerate() {
            let value = self.sum(|c| &c.requests[i]);
            let _ = writeln!(out, "ramjet_requests_total{{code=\"{class}\"}} {value}");
        }

        out.push_str(
            "# HELP ramjet_upstream_latency_seconds Time from upstream dispatch to response headers.\n",
        );
        out.push_str("# TYPE ramjet_upstream_latency_seconds histogram\n");
        let name = "ramjet_upstream_latency_seconds";
        // Buckets are stored non-cumulative and made cumulative here, which is
        // what keeps the hot path a single add.
        let mut cumulative = 0u64;
        for (i, bound) in BUCKETS.iter().enumerate() {
            cumulative += self.sum(|c| &c.buckets[i]);
            let _ = writeln!(out, "{name}_bucket{{le=\"{bound}\"}} {cumulative}");
        }
        let count = self.sum(|c| &c.latency_count);
        let _ = writeln!(out, "{name}_bucket{{le=\"+Inf\"}} {count}");
        let sum = self.sum(|c| &c.sum_nanos) as f64 / 1e9;
        let _ = writeln!(out, "{name}_sum {sum:.6}");
        let _ = writeln!(out, "{name}_count {count}");

        gauge(
            &mut out,
            "ramjet_active_connections",
            "Downstream connections currently being served.",
            self.active_connections(),
        );
        gauge(
            &mut out,
            "ramjet_route_table_generation",
            "Generation of the currently published route table.",
            generation as i64,
        );
        gauge(
            &mut out,
            "ramjet_pinned",
            "1 when a rollback is holding publication at a chosen generation.",
            i64::from(pinned),
        );

        for (name, help, value) in [
            (
                "ramjet_tls_handshakes_total",
                "TLS handshakes completed.",
                self.sum(|c| &c.tls_handshakes),
            ),
            (
                "ramjet_tls_handshake_failures_total",
                "TLS handshakes that failed.",
                self.sum(|c| &c.tls_handshake_failures),
            ),
            (
                "ramjet_upstream_connect_failures_total",
                "Failures to connect to an upstream endpoint.",
                self.sum(|c| &c.connect_failures),
            ),
            (
                "ramjet_upstream_retries_total",
                "Requests re-dispatched to a different endpoint.",
                self.sum(|c| &c.retries),
            ),
            (
                "ramjet_upstream_timeouts_total",
                "Upstreams that did not send headers before the deadline.",
                self.sum(|c| &c.timeouts),
            ),
            (
                "ramjet_route_misses_total",
                "Requests that matched no route and no default backend.",
                self.sum(|c| &c.route_misses),
            ),
            // Zero for the same reason the TLS handshake counters are: this
            // engine has no QUIC listener, and a dashboard that loses a series
            // when an operator changes engine looks like an outage.
            (
                "ramjet_h3_connections_total",
                "HTTP/3 connections established on the QUIC listener.",
                0,
            ),
            (
                "ramjet_h3_requests_total",
                "Requests that arrived over HTTP/3.",
                0,
            ),
            (
                "ramjet_h3_handshake_failures_total",
                "QUIC connections that never became usable HTTP/3 connections.",
                0,
            ),
            (
                "ramjet_mirrored_total",
                "Requests copied to a mirror backend, which accepted the copy.",
                self.mirror(ramjet_proxy::Metrics::mirrored),
            ),
            (
                "ramjet_mirror_dropped_total",
                "Copies discarded because a serving runtime's mirror queue was full.",
                self.mirror(ramjet_proxy::Metrics::mirror_dropped),
            ),
            (
                "ramjet_mirror_skipped_total",
                "Copies not attempted because the request body exceeded --mirror-max-body.",
                self.mirror(ramjet_proxy::Metrics::mirror_skipped),
            ),
            (
                "ramjet_mirror_failures_total",
                "Copies a mirror backend refused, failed, or did not answer in time.",
                self.mirror(ramjet_proxy::Metrics::mirror_failures),
            ),
        ] {
            let _ = writeln!(out, "# HELP {name} {help}");
            let _ = writeln!(out, "# TYPE {name} counter");
            let _ = writeln!(out, "{name} {value}");
        }

        out
    }
}

fn gauge(out: &mut String, name: &str, help: &str, value: i64) {
    use std::fmt::Write as _;
    let _ = writeln!(out, "# HELP {name} {help}");
    let _ = writeln!(out, "# TYPE {name} gauge");
    let _ = writeln!(out, "{name} {value}");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn responses_land_in_the_right_class() {
        let m = EngineMetrics::new(2);
        for (status, class) in [
            (100u16, "1xx"),
            (200, "2xx"),
            (301, "3xx"),
            (404, "4xx"),
            (502, "5xx"),
            (999, "other"),
        ] {
            m.core(0).response(status);
            assert_eq!(m.responses(class), 1, "{status}");
        }
    }

    #[test]
    fn counters_from_every_core_are_summed() {
        let m = EngineMetrics::new(4);
        for core in 0..4 {
            m.core(core).response(200);
            m.core(core).connection_opened();
        }
        assert_eq!(m.responses("2xx"), 4);
        assert_eq!(m.active_connections(), 4);
        m.core(3).connection_closed();
        assert_eq!(m.active_connections(), 3);
    }

    #[test]
    fn each_core_has_its_own_cache_line() {
        // The whole reason this type exists. If the blocks ever share a line,
        // the hot path is silently paying for coherence traffic again.
        let m = EngineMetrics::new(4);
        let first = std::ptr::from_ref(m.core(0)) as usize;
        let second = std::ptr::from_ref(m.core(1)) as usize;
        assert!(
            second - first >= 128,
            "cores {first:#x} and {second:#x} are {} bytes apart",
            second - first
        );
        assert_eq!(first % 128, 0, "the first block is not line-aligned");
    }

    #[test]
    fn histogram_buckets_are_cumulative_and_inclusive() {
        let m = EngineMetrics::new(1);
        m.core(0).upstream_latency(Duration::from_micros(500)); // <= 0.001
        m.core(0).upstream_latency(Duration::from_millis(20)); // <= 0.025
        m.core(0).upstream_latency(Duration::from_secs(30)); // past the last
        let text = m.render_prometheus(0, false);
        assert!(text.contains("ramjet_upstream_latency_seconds_bucket{le=\"0.001\"} 1"), "{text}");
        assert!(text.contains("ramjet_upstream_latency_seconds_bucket{le=\"0.025\"} 2"), "{text}");
        assert!(text.contains("ramjet_upstream_latency_seconds_bucket{le=\"10\"} 2"), "{text}");
        assert!(text.contains("ramjet_upstream_latency_seconds_bucket{le=\"+Inf\"} 3"), "{text}");
        assert!(text.contains("ramjet_upstream_latency_seconds_count 3"), "{text}");
    }

    #[test]
    fn the_generation_comes_from_the_argument() {
        let m = EngineMetrics::new(1);
        assert!(m
            .render_prometheus(42, false)
            .contains("ramjet_route_table_generation 42"));
    }

    /// The test that makes duplicating the formatter safe.
    ///
    /// Both engines' counters are driven through the same events and the two
    /// renderings are compared as whole strings. Anything at all — a reordered
    /// series, a changed HELP line, `le="1.0"` instead of `le="1"` — fails
    /// here rather than in a dashboard.
    #[test]
    fn the_rendering_matches_the_hyper_engine_byte_for_byte() {
        let hyper = std::sync::Arc::new(ramjet_proxy::Metrics::new());
        let uring = EngineMetrics::new(3);

        // Spread the engine's events over its cores, so the merge is exercised
        // rather than one core standing in for the whole process.
        let events: [(u16, u64); 7] = [
            (200, 5),
            (404, 3),
            (502, 2),
            (301, 1),
            (100, 1),
            (999, 4),
            (204, 6),
        ];
        for (core, (status, times)) in events.into_iter().enumerate() {
            for _ in 0..times {
                hyper.record_response(status);
                uring.core(core % 3).response(status);
            }
        }

        for (core, micros) in [(0usize, 900u64), (1, 4_000), (2, 60_000), (0, 12_000_000)] {
            let elapsed = Duration::from_micros(micros);
            hyper.record_upstream_latency(elapsed);
            uring.core(core).upstream_latency(elapsed);
        }

        let mut guards = Vec::new();
        for core in 0..3 {
            guards.push(hyper.connection_opened());
            uring.core(core).connection_opened();
        }
        drop(guards.pop());
        uring.core(2).connection_closed();

        for _ in 0..4 {
            hyper.record_connect_failure();
            uring.core(1).connect_failure();
        }
        for _ in 0..2 {
            hyper.record_retry();
            uring.core(0).retry();
        }
        hyper.record_upstream_timeout();
        uring.core(2).timeout();
        for _ in 0..3 {
            hyper.record_route_miss();
            uring.core(1).route_miss();
        }

        assert_eq!(
            uring.render_prometheus(7, false),
            hyper.render_prometheus(7, false),
            "the two engines' /metrics output diverged"
        );

        // Guard against the assertion passing because both are empty. 11 is
        // the 200s and the 204s together, which is the point of a class.
        let text = uring.render_prometheus(7, false);
        assert!(text.contains("ramjet_requests_total{code=\"2xx\"} 11"), "{text}");
        assert!(text.contains("ramjet_upstream_retries_total 2"), "{text}");
        assert!(text.contains("ramjet_active_connections 2"), "{text}");
    }
}
