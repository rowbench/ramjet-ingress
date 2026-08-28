//! The five numbers this client wants out of `/metrics`.
//!
//! This is not a Prometheus parser. It is a scanner that knows the exposition
//! format well enough to find named series in it and ignores everything else,
//! which is the right shape for a client that wants a global request rate and a
//! connection count from a page that also carries a twelve-bucket histogram.
//!
//! Ignoring everything else is a feature, not a shortcut: the data plane grows
//! series, and a scrape that this client cannot fully parse is still a scrape
//! it can read the request rate out of.
//!
//! # What the series mean
//!
//! `ramjet_requests_total` is labelled by status *class* — `2xx`, `5xx`,
//! `other` — not by status code, so the totals below are sums over classes and
//! the error count is one specific class. Getting that backwards would produce
//! an error rate that is always zero, because no label is ever literally `500`.

/// Everything read out of one scrape.
///
/// Absent series are `None` rather than zero. The difference matters: a
/// `ramjet_pinned` that is missing means the server is older than the pin
/// feature, and a `ramjet_pinned` of `0` means nothing is pinned. Rendering
/// those the same way is how a status line lies.
#[derive(Debug, Clone, Default, PartialEq, serde::Serialize)]
pub struct MetricsSnapshot {
    /// Responses served, summed over every status class.
    pub requests_total: u64,
    /// Responses in the `5xx` class.
    pub errors_5xx_total: u64,
    /// Downstream connections currently open.
    pub active_connections: Option<i64>,
    /// The generation the data plane says it is serving.
    pub generation: Option<u64>,
    /// Total upstream latency, in seconds, across every observation.
    pub latency_sum_seconds: f64,
    /// Observations behind that sum.
    pub latency_count: u64,
    /// The pinned generation, if the server exports one and it is non-zero.
    pub pinned: Option<u64>,
}

impl MetricsSnapshot {
    /// The lifetime mean upstream latency, in milliseconds.
    ///
    /// Present only for the `--once` summary. The live view never shows this —
    /// it shows the mean *since the last poll*, which is a different number and
    /// the one that reacts when an upstream degrades. A lifetime mean on a
    /// process that has been up for a week is a number that cannot move.
    pub fn lifetime_mean_latency_ms(&self) -> Option<f64> {
        (self.latency_count > 0)
            .then(|| self.latency_sum_seconds * 1000.0 / self.latency_count as f64)
    }
}

/// One exposition line, split into the parts this scanner cares about.
struct Sample<'a> {
    name: &'a str,
    labels: &'a str,
    value: f64,
}

/// Splits `name{labels} value` or `name value`.
///
/// Returns `None` for anything that is not a sample — comments, blank lines,
/// and lines whose value is not a number, which in this format includes `NaN`
/// and `+Inf` and which this client has no use for.
fn split_sample(line: &str) -> Option<Sample<'_>> {
    let line = line.trim();
    if line.is_empty() || line.starts_with('#') {
        return None;
    }

    let (name, labels, rest) = match line.split_once('{') {
        Some((name, after)) => {
            let (labels, rest) = after.split_once('}')?;
            (name, labels, rest)
        }
        None => {
            let (name, rest) = line.split_once(' ')?;
            (name, "", rest)
        }
    };

    // The value is the first whitespace-separated token after the name; a
    // scrape may carry a trailing timestamp, which is not this client's
    // business.
    let value = rest.split_whitespace().next()?.parse::<f64>().ok()?;
    Some(Sample {
        name: name.trim(),
        labels,
        value,
    })
}

/// Reads the value of a label out of a label set.
///
/// Deliberately naive about quoting: the exposition format allows escapes, and
/// the only label this client reads is a status class, which is three
/// characters of `[0-9a-z]`. A label value that needed unescaping would not be
/// one of those.
fn label<'a>(labels: &'a str, key: &str) -> Option<&'a str> {
    labels.split(',').find_map(|pair| {
        let (name, value) = pair.split_once('=')?;
        (name.trim() == key).then(|| value.trim().trim_matches('"'))
    })
}

/// Non-negative counters only. A counter is never negative and a float that
/// came out of a scrape can be anything, so this is the one conversion.
fn as_counter(value: f64) -> u64 {
    if value.is_finite() && value > 0.0 {
        value as u64
    } else {
        0
    }
}

/// Scans an exposition page for the series this client displays.
///
/// Never fails. A page that is empty, truncated, or from an entirely different
/// exporter yields a snapshot of zeroes and `None`s, which the display renders
/// as "no data" — the correct outcome for a monitoring client pointed at the
/// wrong port, and a better one than an error that replaces the whole screen.
pub fn parse(text: &str) -> MetricsSnapshot {
    let mut snapshot = MetricsSnapshot::default();

    for line in text.lines() {
        let Some(sample) = split_sample(line) else {
            continue;
        };
        match sample.name {
            "ramjet_requests_total" => {
                let class = label(sample.labels, "code").unwrap_or_default();
                let count = as_counter(sample.value);
                snapshot.requests_total = snapshot.requests_total.saturating_add(count);
                if class == "5xx" {
                    snapshot.errors_5xx_total = snapshot.errors_5xx_total.saturating_add(count);
                }
            }
            "ramjet_active_connections" if sample.value.is_finite() => {
                snapshot.active_connections = Some(sample.value as i64);
            }
            "ramjet_route_table_generation" => {
                snapshot.generation = Some(as_counter(sample.value));
            }
            "ramjet_upstream_latency_seconds_sum"
                if sample.value.is_finite() && sample.value >= 0.0 =>
            {
                snapshot.latency_sum_seconds = sample.value;
            }
            "ramjet_upstream_latency_seconds_count" => {
                snapshot.latency_count = as_counter(sample.value);
            }
            // Optional: the server may not export it at all. Zero means "not
            // pinned", which is the same as absent for display purposes and is
            // normalised to `None` here so the header has one case to handle.
            "ramjet_pinned" => {
                let generation = as_counter(sample.value);
                snapshot.pinned = (generation > 0).then_some(generation);
            }
            _ => {}
        }
    }

    snapshot
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A scrape in the shape `ramjet-proxy` actually renders, buckets and all.
    const SCRAPE: &str = "\
# HELP ramjet_requests_total Responses served, by status class.
# TYPE ramjet_requests_total counter
ramjet_requests_total{code=\"1xx\"} 0
ramjet_requests_total{code=\"2xx\"} 9000
ramjet_requests_total{code=\"3xx\"} 100
ramjet_requests_total{code=\"4xx\"} 30
ramjet_requests_total{code=\"5xx\"} 12
ramjet_requests_total{code=\"other\"} 1
# HELP ramjet_upstream_latency_seconds Time from upstream dispatch to response headers.
# TYPE ramjet_upstream_latency_seconds histogram
ramjet_upstream_latency_seconds_bucket{le=\"0.001\"} 10
ramjet_upstream_latency_seconds_bucket{le=\"0.025\"} 8000
ramjet_upstream_latency_seconds_bucket{le=\"10\"} 9100
ramjet_upstream_latency_seconds_bucket{le=\"+Inf\"} 9143
ramjet_upstream_latency_seconds_sum 51.2345
ramjet_upstream_latency_seconds_count 9143
# HELP ramjet_active_connections Downstream connections currently being served.
# TYPE ramjet_active_connections gauge
ramjet_active_connections 37
# HELP ramjet_route_table_generation Generation of the currently published route table.
# TYPE ramjet_route_table_generation gauge
ramjet_route_table_generation 42
# HELP ramjet_upstream_retries_total Requests re-dispatched to a different endpoint.
# TYPE ramjet_upstream_retries_total counter
ramjet_upstream_retries_total 3
";

    #[test]
    fn a_real_scrape_yields_every_series_this_client_uses() {
        let m = parse(SCRAPE);
        assert_eq!(m.requests_total, 9000 + 100 + 30 + 12 + 1);
        assert_eq!(m.errors_5xx_total, 12);
        assert_eq!(m.active_connections, Some(37));
        assert_eq!(m.generation, Some(42));
        assert!((m.latency_sum_seconds - 51.2345).abs() < 1e-9);
        assert_eq!(m.latency_count, 9143);
        assert_eq!(m.pinned, None, "this scrape has no ramjet_pinned");
    }

    #[test]
    fn histogram_buckets_are_not_mistaken_for_the_sum_or_the_count() {
        let m = parse(SCRAPE);
        // The `+Inf` bucket is 9143 and so is the count; the sum must not have
        // picked up a bucket's value, and the count must not be a bucket's.
        assert_eq!(m.latency_count, 9143);
        assert!((m.latency_sum_seconds - 51.2345).abs() < 1e-9);
    }

    #[test]
    fn the_error_count_reads_the_class_label_not_a_status_code() {
        // The label is never literally "500". A parser looking for one would
        // report a permanent zero error rate.
        let m = parse("ramjet_requests_total{code=\"5xx\"} 7\n");
        assert_eq!(m.errors_5xx_total, 7);
        assert_eq!(m.requests_total, 7);
    }

    #[test]
    fn comments_and_blank_lines_are_skipped() {
        let m = parse("# TYPE ramjet_requests_total counter\n\n   \n# HELP x y\n");
        assert_eq!(m, MetricsSnapshot::default());
    }

    #[test]
    fn an_empty_or_foreign_page_is_a_snapshot_of_nothing_not_an_error() {
        assert_eq!(parse(""), MetricsSnapshot::default());
        assert_eq!(parse("<html>404</html>"), MetricsSnapshot::default());
        assert_eq!(
            parse("go_goroutines 12\nprocess_cpu_seconds_total 0.4\n"),
            MetricsSnapshot::default()
        );
    }

    #[test]
    fn a_truncated_page_still_yields_the_lines_that_did_arrive() {
        let cut = "ramjet_requests_total{code=\"2xx\"} 500\nramjet_active_conn";
        let m = parse(cut);
        assert_eq!(m.requests_total, 500);
        assert_eq!(m.active_connections, None);
    }

    #[test]
    fn an_absent_pin_and_a_zero_pin_both_read_as_not_pinned() {
        assert_eq!(parse("ramjet_pinned 0\n").pinned, None);
        assert_eq!(parse("").pinned, None);
        assert_eq!(parse("ramjet_pinned 41\n").pinned, Some(41));
    }

    #[test]
    fn non_numeric_values_do_not_poison_the_snapshot() {
        let m = parse(
            "ramjet_requests_total{code=\"2xx\"} NaN\n\
             ramjet_requests_total{code=\"4xx\"} 5\n\
             ramjet_active_connections +Inf\n",
        );
        assert_eq!(m.requests_total, 5, "NaN contributed nothing");
        assert_eq!(m.active_connections, None);
    }

    #[test]
    fn a_trailing_timestamp_is_ignored() {
        let m = parse("ramjet_active_connections 9 1735689600000\n");
        assert_eq!(m.active_connections, Some(9));
    }

    #[test]
    fn spacing_around_labels_and_values_is_tolerated() {
        let m = parse("  ramjet_requests_total{ code = \"5xx\" }   4  \n");
        assert_eq!(m.errors_5xx_total, 4);
        assert_eq!(m.requests_total, 4);
    }

    #[test]
    fn an_unlabelled_requests_total_still_counts_toward_the_total() {
        // Not a shape the server emits, but a sum that silently dropped it
        // would under-report the global rate rather than say so.
        let m = parse("ramjet_requests_total 12\n");
        assert_eq!(m.requests_total, 12);
        assert_eq!(m.errors_5xx_total, 0);
    }

    #[test]
    fn the_lifetime_mean_is_milliseconds_and_absent_without_observations() {
        let m = parse(SCRAPE);
        let mean = m.lifetime_mean_latency_ms().expect("observations exist");
        assert!((mean - 51.2345 * 1000.0 / 9143.0).abs() < 1e-9);
        assert_eq!(MetricsSnapshot::default().lifetime_mean_latency_ms(), None);
    }
}
