//! Counters, gauges, and a fixed-bucket histogram, rendered as Prometheus text.
//!
//! # Why this is hand-rolled
//!
//! The `prometheus` and `metrics` crates both key observations by a label set,
//! which means a hash of a `&[(&str, &str)]` on every request. That is a real
//! cost next to a route match that is supposed to finish in a couple of hundred
//! nanoseconds, and it buys flexibility this crate does not want: an ingress
//! data plane has a fixed, small set of series, known at compile time.
//!
//! So every series here is a field, every observation is one relaxed atomic
//! add, and label cardinality is bounded by the type system rather than by
//! hoping nobody puts a request path in a label. The whole module is under 200
//! lines and has no dependencies.
//!
//! Per-route and per-backend series are deliberately absent. ingress-nginx
//! emits them and they are the single most common reason its metrics endpoint
//! becomes the most expensive request the pod serves; a cluster with 5k
//! Ingresses produces enough series to knock over the scraper.
//!
//! The per-route *numbers* do exist — the router counts them, and
//! `/admin/routes` serves them as JSON. What is refused is turning them into
//! labelled series that every scrape pays for whether or not anybody is
//! looking. Cardinality here stays bounded by the type system rather than by
//! the size of somebody's cluster.
//!
//! # Ordering
//!
//! Everything is `Relaxed`. Metrics do not publish anything: no reader draws a
//! conclusion about another memory location from a counter's value, so
//! acquire/release ordering would buy nothing but a fence per request.

use std::fmt::Write as _;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;

/// Anything that can render a `/metrics` body.
///
/// The admin listener serves whichever data plane is running, and there are two
/// of them: this crate's counters and `ramjet_engine`'s per-core blocks. They
/// produce byte-identical output for the same events — a differential test
/// pins that — but they are different types with different insides, and the
/// admin listener has no business knowing which one it is talking to.
///
/// One method, because that is genuinely all the admin listener needs from
/// either.
pub trait Exposition: std::fmt::Debug + Send + Sync {
    /// The Prometheus text exposition for the current counter values.
    ///
    /// `generation` and `pinned` are read at scrape time from the route table
    /// and the generation history, rather than mirrored into the counters on
    /// every publish.
    fn render_prometheus(&self, generation: u64, pinned: bool) -> String;
}

impl Exposition for Metrics {
    fn render_prometheus(&self, generation: u64, pinned: bool) -> String {
        Metrics::render_prometheus(self, generation, pinned)
    }
}

/// Upper bounds, in seconds, of the upstream latency histogram.
///
/// Chosen around the shape of ingress traffic rather than as round numbers: the
/// interesting resolution is between 1ms and 100ms, where a healthy service
/// lives, and everything past a second only needs to be distinguishable from
/// "timed out".
const BUCKETS: [f64; 12] = [
    0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 5.0, 10.0,
];

/// Status classes tracked by `requests_total`.
const CLASSES: [&str; 6] = ["1xx", "2xx", "3xx", "4xx", "5xx", "other"];

/// A fixed-bucket cumulative histogram of durations.
#[derive(Debug, Default)]
struct Histogram {
    buckets: [AtomicU64; BUCKETS.len()],
    /// Total observed time in nanoseconds. Nanoseconds rather than a float
    /// because there is no lock-free atomic add for `f64`, and a `u64` of
    /// nanoseconds does not overflow for 584 years.
    sum_nanos: AtomicU64,
    count: AtomicU64,
}

impl Histogram {
    fn observe(&self, elapsed: std::time::Duration) {
        let seconds = elapsed.as_secs_f64();
        // Linear scan over twelve `f64`s: one cache line, no branch predictor
        // surprises, and faster than a binary search at this size.
        let index = BUCKETS.iter().position(|&b| seconds <= b);
        if let Some(i) = index {
            if let Some(bucket) = self.buckets.get(i) {
                bucket.fetch_add(1, Ordering::Relaxed);
            }
        }
        self.sum_nanos
            .fetch_add(elapsed.as_nanos().min(u64::MAX as u128) as u64, Ordering::Relaxed);
        self.count.fetch_add(1, Ordering::Relaxed);
    }

    fn render(&self, out: &mut String, name: &str) {
        // Prometheus histogram buckets are cumulative, so carry the running
        // total rather than storing it that way and paying for it per request.
        let mut cumulative = 0u64;
        for (i, bound) in BUCKETS.iter().enumerate() {
            cumulative += self.buckets.get(i).map_or(0, |b| b.load(Ordering::Relaxed));
            let _ = writeln!(out, "{name}_bucket{{le=\"{bound}\"}} {cumulative}");
        }
        let count = self.count.load(Ordering::Relaxed);
        let _ = writeln!(out, "{name}_bucket{{le=\"+Inf\"}} {count}");
        let sum = self.sum_nanos.load(Ordering::Relaxed) as f64 / 1e9;
        let _ = writeln!(out, "{name}_sum {sum:.6}");
        let _ = writeln!(out, "{name}_count {count}");
    }
}

/// Every series the data plane exports.
///
/// Cloned around as an `Arc` and shared by every worker; nothing in here is
/// per-connection state.
#[derive(Debug, Default)]
pub struct Metrics {
    requests: [AtomicU64; CLASSES.len()],
    upstream_latency: Histogram,
    active_connections: AtomicI64,
    tls_handshakes: AtomicU64,
    tls_handshake_failures: AtomicU64,
    upstream_connect_failures: AtomicU64,
    upstream_retries: AtomicU64,
    upstream_timeouts: AtomicU64,
    route_misses: AtomicU64,
    mirrored: AtomicU64,
    mirror_dropped: AtomicU64,
    mirror_skipped: AtomicU64,
    mirror_failures: AtomicU64,
    h3_connections: AtomicU64,
    h3_requests: AtomicU64,
    h3_handshake_failures: AtomicU64,
}

impl Metrics {
    /// A fresh, zeroed set of series.
    pub fn new() -> Self {
        Self::default()
    }

    /// Records a response by status class.
    #[inline]
    pub fn record_response(&self, status: u16) {
        let index = match status / 100 {
            1 => 0,
            2 => 1,
            3 => 2,
            4 => 3,
            5 => 4,
            _ => 5,
        };
        if let Some(counter) = self.requests.get(index) {
            counter.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Records the time from dispatching upstream to receiving its headers.
    ///
    /// Headers, not the last body byte: a 4GB download is not a slow upstream,
    /// and mixing the two makes the histogram useless for detecting one.
    #[inline]
    pub fn record_upstream_latency(&self, elapsed: std::time::Duration) {
        self.upstream_latency.observe(elapsed);
    }

    /// Registers an accepted connection, returning a guard that deregisters it.
    pub fn connection_opened(self: &Arc<Self>) -> ConnectionGuard {
        self.active_connections.fetch_add(1, Ordering::Relaxed);
        ConnectionGuard {
            metrics: Arc::clone(self),
        }
    }

    /// Records a completed TLS handshake.
    #[inline]
    pub fn record_tls_handshake(&self) {
        self.tls_handshakes.fetch_add(1, Ordering::Relaxed);
    }

    /// Records a TLS handshake that failed before producing a connection.
    #[inline]
    pub fn record_tls_handshake_failure(&self) {
        self.tls_handshake_failures.fetch_add(1, Ordering::Relaxed);
    }

    /// Records a failure to establish a connection to an upstream endpoint.
    #[inline]
    pub fn record_connect_failure(&self) {
        self.upstream_connect_failures.fetch_add(1, Ordering::Relaxed);
    }

    /// Records a request re-dispatched to a different endpoint.
    #[inline]
    pub fn record_retry(&self) {
        self.upstream_retries.fetch_add(1, Ordering::Relaxed);
    }

    /// Records an upstream that did not send headers within the deadline.
    #[inline]
    pub fn record_upstream_timeout(&self) {
        self.upstream_timeouts.fetch_add(1, Ordering::Relaxed);
    }

    /// Records a request that matched no route and no default backend.
    #[inline]
    pub fn record_route_miss(&self) {
        self.route_misses.fetch_add(1, Ordering::Relaxed);
    }

    /// Records a QUIC connection that became a usable HTTP/3 connection.
    #[inline]
    pub fn record_h3_connection(&self) {
        self.h3_connections.fetch_add(1, Ordering::Relaxed);
    }

    /// Records a request that arrived over HTTP/3.
    ///
    /// Counted in addition to `ramjet_requests_total`, not instead of it: the
    /// response goes through the same forwarding path and is classed there like
    /// any other. This series answers a different question — how much of the
    /// traffic actually took the QUIC path — which is the one an operator has
    /// while deciding whether the advertisement is working.
    #[inline]
    pub fn record_h3_request(&self) {
        self.h3_requests.fetch_add(1, Ordering::Relaxed);
    }

    /// Records a QUIC connection that never became a usable HTTP/3 one.
    ///
    /// A failed QUIC handshake — no acceptable version, no certificate for the
    /// SNI — and an h3 setup that did not complete both land here, because the
    /// question they answer is the same: how many peers reached the UDP port
    /// and got nothing back.
    #[inline]
    pub fn record_h3_handshake_failure(&self) {
        self.h3_handshake_failures.fetch_add(1, Ordering::Relaxed);
    }

    /// Records a mirrored copy the mirror backend accepted.
    #[inline]
    pub fn record_mirrored(&self) {
        self.mirrored.fetch_add(1, Ordering::Relaxed);
    }

    /// Records a copy discarded because the runtime's mirror queue was full.
    #[inline]
    pub fn record_mirror_dropped(&self) {
        self.mirror_dropped.fetch_add(1, Ordering::Relaxed);
    }

    /// Records a copy not attempted because the request body was over the cap.
    #[inline]
    pub fn record_mirror_skipped(&self) {
        self.mirror_skipped.fetch_add(1, Ordering::Relaxed);
    }

    /// Records a copy the mirror backend refused, failed, or did not answer.
    #[inline]
    pub fn record_mirror_failure(&self) {
        self.mirror_failures.fetch_add(1, Ordering::Relaxed);
    }

    /// Copies the mirror backend accepted.
    pub fn mirrored(&self) -> u64 {
        self.mirrored.load(Ordering::Relaxed)
    }

    /// Copies discarded because a mirror queue was full.
    pub fn mirror_dropped(&self) -> u64 {
        self.mirror_dropped.load(Ordering::Relaxed)
    }

    /// Copies not attempted because the request body was over the cap.
    pub fn mirror_skipped(&self) -> u64 {
        self.mirror_skipped.load(Ordering::Relaxed)
    }

    /// Copies the mirror backend refused, failed, or did not answer.
    pub fn mirror_failures(&self) -> u64 {
        self.mirror_failures.load(Ordering::Relaxed)
    }

    /// Connections currently being served.
    pub fn active_connections(&self) -> i64 {
        self.active_connections.load(Ordering::Relaxed)
    }

    /// Responses served in the given status class, e.g. `"2xx"`.
    pub fn responses(&self, class: &str) -> u64 {
        CLASSES
            .iter()
            .position(|c| *c == class)
            .and_then(|i| self.requests.get(i))
            .map_or(0, |c| c.load(Ordering::Relaxed))
    }

    /// Number of upstream latency observations.
    pub fn upstream_observations(&self) -> u64 {
        self.upstream_latency.count.load(Ordering::Relaxed)
    }

    /// Completed TLS handshakes.
    pub fn tls_handshakes(&self) -> u64 {
        self.tls_handshakes.load(Ordering::Relaxed)
    }

    /// Requests re-dispatched to a different endpoint.
    pub fn retries(&self) -> u64 {
        self.upstream_retries.load(Ordering::Relaxed)
    }

    /// HTTP/3 connections established.
    pub fn h3_connections(&self) -> u64 {
        self.h3_connections.load(Ordering::Relaxed)
    }

    /// Requests that arrived over HTTP/3.
    pub fn h3_requests(&self) -> u64 {
        self.h3_requests.load(Ordering::Relaxed)
    }

    /// QUIC connections that never became usable HTTP/3 ones.
    pub fn h3_handshake_failures(&self) -> u64 {
        self.h3_handshake_failures.load(Ordering::Relaxed)
    }

    /// Renders the Prometheus text exposition format.
    ///
    /// `generation` and `pinned` are read from the route table and the
    /// generation history at scrape time rather than mirrored into atomics on
    /// every publish; a scrape happens once every fifteen seconds and a publish
    /// is rarer still, so there is no reason for either to maintain state for
    /// the other.
    ///
    /// The two answer different questions and both are needed:
    /// `ramjet_route_table_generation` says what is *serving*, and
    /// `ramjet_pinned` says whether that is the case because somebody pulled
    /// the emergency brake. Without the second, a replica frozen on an old
    /// generation is indistinguishable from a replica whose control plane has
    /// stopped — and those want very different pages.
    pub fn render_prometheus(&self, generation: u64, pinned: bool) -> String {
        let mut out = String::with_capacity(2048);

        out.push_str("# HELP ramjet_requests_total Responses served, by status class.\n");
        out.push_str("# TYPE ramjet_requests_total counter\n");
        for (i, class) in CLASSES.iter().enumerate() {
            let value = self.requests.get(i).map_or(0, |c| c.load(Ordering::Relaxed));
            let _ = writeln!(out, "ramjet_requests_total{{code=\"{class}\"}} {value}");
        }

        out.push_str(
            "# HELP ramjet_upstream_latency_seconds Time from upstream dispatch to response headers.\n",
        );
        out.push_str("# TYPE ramjet_upstream_latency_seconds histogram\n");
        self.upstream_latency
            .render(&mut out, "ramjet_upstream_latency_seconds");

        gauge(
            &mut out,
            "ramjet_active_connections",
            "Downstream connections currently being served.",
            self.active_connections.load(Ordering::Relaxed),
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
                &self.tls_handshakes,
            ),
            (
                "ramjet_tls_handshake_failures_total",
                "TLS handshakes that failed.",
                &self.tls_handshake_failures,
            ),
            (
                "ramjet_upstream_connect_failures_total",
                "Failures to connect to an upstream endpoint.",
                &self.upstream_connect_failures,
            ),
            (
                "ramjet_upstream_retries_total",
                "Requests re-dispatched to a different endpoint.",
                &self.upstream_retries,
            ),
            (
                "ramjet_upstream_timeouts_total",
                "Upstreams that did not send headers before the deadline.",
                &self.upstream_timeouts,
            ),
            (
                "ramjet_route_misses_total",
                "Requests that matched no route and no default backend.",
                &self.route_misses,
            ),
            (
                "ramjet_h3_connections_total",
                "HTTP/3 connections established on the QUIC listener.",
                &self.h3_connections,
            ),
            (
                "ramjet_h3_requests_total",
                "Requests that arrived over HTTP/3.",
                &self.h3_requests,
            ),
            (
                "ramjet_h3_handshake_failures_total",
                "QUIC connections that never became usable HTTP/3 connections.",
                &self.h3_handshake_failures,
            ),
            (
                "ramjet_mirrored_total",
                "Requests copied to a mirror backend, which accepted the copy.",
                &self.mirrored,
            ),
            (
                "ramjet_mirror_dropped_total",
                "Copies discarded because a serving runtime's mirror queue was full.",
                &self.mirror_dropped,
            ),
            (
                "ramjet_mirror_skipped_total",
                "Copies not attempted because the request body exceeded --mirror-max-body.",
                &self.mirror_skipped,
            ),
            (
                "ramjet_mirror_failures_total",
                "Copies a mirror backend refused, failed, or did not answer in time.",
                &self.mirror_failures,
            ),
        ] {
            let _ = writeln!(out, "# HELP {name} {help}");
            let _ = writeln!(out, "# TYPE {name} counter");
            let _ = writeln!(out, "{name} {}", value.load(Ordering::Relaxed));
        }

        out
    }
}

fn gauge(out: &mut String, name: &str, help: &str, value: i64) {
    let _ = writeln!(out, "# HELP {name} {help}");
    let _ = writeln!(out, "# TYPE {name} gauge");
    let _ = writeln!(out, "{name} {value}");
}

/// Decrements the active-connection gauge when dropped.
///
/// Held by the connection task, so a panicking or aborted connection still
/// leaves the gauge correct — the alternative, decrementing at the end of the
/// accept loop body, silently leaks on every abnormal close.
#[derive(Debug)]
pub struct ConnectionGuard {
    metrics: Arc<Metrics>,
}

impl Drop for ConnectionGuard {
    fn drop(&mut self) {
        self.metrics
            .active_connections
            .fetch_sub(1, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn status_classes_land_in_the_right_bucket() {
        let m = Metrics::new();
        m.record_response(200);
        m.record_response(204);
        m.record_response(404);
        m.record_response(503);
        m.record_response(999);
        assert_eq!(m.responses("2xx"), 2);
        assert_eq!(m.responses("4xx"), 1);
        assert_eq!(m.responses("5xx"), 1);
        assert_eq!(m.responses("other"), 1);
        assert_eq!(m.responses("3xx"), 0);
    }

    #[test]
    fn histogram_buckets_are_cumulative_in_the_output() {
        let m = Metrics::new();
        m.record_upstream_latency(Duration::from_micros(500)); // <= 0.001
        m.record_upstream_latency(Duration::from_millis(20)); // <= 0.025
        m.record_upstream_latency(Duration::from_secs(30)); // over the last bound

        let text = m.render_prometheus(0, false);
        assert!(text.contains("ramjet_upstream_latency_seconds_bucket{le=\"0.001\"} 1"));
        assert!(text.contains("ramjet_upstream_latency_seconds_bucket{le=\"0.025\"} 2"));
        assert!(text.contains("ramjet_upstream_latency_seconds_bucket{le=\"10\"} 2"));
        // The +Inf bucket is the total count, which is how an observation past
        // the last bound stays visible.
        assert!(text.contains("ramjet_upstream_latency_seconds_bucket{le=\"+Inf\"} 3"));
        assert!(text.contains("ramjet_upstream_latency_seconds_count 3"));
    }

    #[test]
    fn active_connections_returns_to_zero() {
        let m = Arc::new(Metrics::new());
        {
            let _a = m.connection_opened();
            let _b = m.connection_opened();
            assert_eq!(m.active_connections(), 2);
        }
        assert_eq!(m.active_connections(), 0);
    }

    #[test]
    fn generation_is_reported_from_the_argument() {
        let m = Metrics::new();
        assert!(m
            .render_prometheus(42, false)
            .contains("ramjet_route_table_generation 42"));
    }

    #[test]
    fn the_pin_gauge_says_whether_publication_is_held() {
        let m = Metrics::new();
        assert!(m.render_prometheus(42, false).contains("ramjet_pinned 0"));
        assert!(m.render_prometheus(42, true).contains("ramjet_pinned 1"));
    }

    #[test]
    fn the_four_mirror_outcomes_are_told_apart() {
        // They have four different fixes — a slow shadow, a shallow queue, a
        // body cap set too low, and a backend that is simply down — so folding
        // them into one counter would make the metric unactionable.
        let m = Metrics::new();
        m.record_mirrored();
        m.record_mirror_dropped();
        m.record_mirror_dropped();
        m.record_mirror_skipped();
        m.record_mirror_failure();

        assert_eq!(
            (
                m.mirrored(),
                m.mirror_dropped(),
                m.mirror_skipped(),
                m.mirror_failures()
            ),
            (1, 2, 1, 1)
        );

        let text = m.render_prometheus(0, false);
        assert!(text.contains("ramjet_mirrored_total 1"));
        assert!(text.contains("ramjet_mirror_dropped_total 2"));
        assert!(text.contains("ramjet_mirror_skipped_total 1"));
        assert!(text.contains("ramjet_mirror_failures_total 1"));
    }

    #[test]
    fn exposition_has_help_and_type_for_every_series() {
        let text = Metrics::new().render_prometheus(1, false);
        let series: Vec<&str> = text
            .lines()
            .filter(|l| l.starts_with("# TYPE "))
            .map(|l| l.trim_start_matches("# TYPE "))
            .collect();
        assert!(!series.is_empty());
        for line in &series {
            let name = line.split(' ').next().unwrap_or_default();
            assert!(
                text.contains(&format!("# HELP {name} ")),
                "{name} has a TYPE but no HELP"
            );
        }
    }
}
