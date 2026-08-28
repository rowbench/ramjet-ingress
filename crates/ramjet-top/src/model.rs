//! Turning two polls into the numbers on the screen.
//!
//! Everything the server exports is cumulative, and everything worth watching
//! is a rate. This module is the difference, and it is the part of the program
//! most likely to be quietly wrong, so the reasoning is written down rather
//! than implied.
//!
//! # Three things make differencing counters harder than it looks
//!
//! **Counters restart.** A route that is removed and re-added, a data plane
//! that is restarted under a running `ramjet-top` — either resets a counter to
//! a value below the one held from last poll. Subtracting gives a negative
//! delta, which as an unsigned type is a rate of roughly eighteen quintillion.
//! Every subtraction here saturates at zero instead.
//!
//! **Routes are not rows.** The route table is rebuilt on every generation, so
//! "the same route" has to be decided by something stable. That is
//! [`RouteKey`] — host, path, and path type — and *not* the backend, because a
//! backend swap is the single most interesting moment to keep watching a route
//! through, and re-keying on it would blank the row exactly then.
//!
//! **A route with no history has no rate.** A route seen for the first time has
//! a lifetime counter and no baseline. Dividing that counter by the poll
//! interval would report a rate averaged over the process's entire uptime as if
//! it were happening now, which for a busy route that has been up for an hour
//! is off by orders of magnitude. New routes report no rate for one interval
//! and are flagged; the poll after that, they report a real one.
//!
//! # Why the interval is measured and not assumed
//!
//! Rates divide by the time actually elapsed between the two polls, taken from
//! a monotonic clock, not by `--interval`. A poll that took 900ms to answer
//! because the server was busy would otherwise inflate every rate on the screen
//! by the amount the server was struggling — the worst possible time to be
//! reading numbers that are too high.

use std::collections::HashMap;
use std::time::Duration;

use crate::contract::{Canary, PathType, RouteEntry, RoutesResponse};
use crate::prom::MetricsSnapshot;

/// What makes two rows, in two different polls, the same route.
///
/// The backend is deliberately not part of this; see the module docs.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RouteKey {
    /// The matched host.
    pub host: String,
    /// The matched path.
    pub path: String,
    /// How the path is matched.
    pub path_type: String,
}

impl RouteKey {
    /// The key of a route as the server described it.
    pub fn of(entry: &RouteEntry) -> Self {
        Self {
            host: entry.host.clone(),
            path: entry.path.clone(),
            path_type: entry.path_type.as_str().to_string(),
        }
    }
}

/// The cumulative counters carried from one poll to the next.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct RouteCounters {
    /// Requests served, cumulative.
    pub requests_total: u64,
    /// 5xx responses, cumulative.
    pub errors_5xx_total: u64,
    /// Upstream latency in milliseconds, cumulative.
    pub latency_ms_sum: f64,
    /// Observations behind that sum, cumulative.
    pub latency_count: u64,
    /// The canary-diverted share of the four above, cumulative.
    ///
    /// A subset of them, so the stable share is the difference. Zeroes where
    /// the route has no canary, which is harmless: differencing zero against
    /// zero yields no rate, and the row will not display one either way.
    pub canary: CanaryCounters,
}

/// The canary-diverted share of one route's counters, at one instant.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct CanaryCounters {
    /// Requests the canary served, cumulative.
    pub requests_total: u64,
    /// 5xx responses from the canary, cumulative.
    pub errors_5xx_total: u64,
    /// Canary upstream latency in milliseconds, cumulative.
    pub latency_ms_sum: f64,
    /// Observations behind that sum, cumulative.
    pub latency_count: u64,
}

impl RouteCounters {
    /// The counters on one route row.
    pub fn of(entry: &RouteEntry) -> Self {
        Self {
            requests_total: entry.requests_total,
            errors_5xx_total: entry.errors_5xx_total,
            latency_ms_sum: entry.upstream_latency_ms_sum,
            latency_count: entry.upstream_latency_count,
            canary: entry.canary_stats.map_or_else(
                CanaryCounters::default,
                |split| CanaryCounters {
                    requests_total: split.requests_total,
                    errors_5xx_total: split.errors_5xx_total,
                    latency_ms_sum: split.upstream_latency_ms_sum,
                    latency_count: split.upstream_latency_count,
                },
            ),
        }
    }
}

/// Every route's counters at one instant, keyed for the next poll to difference
/// against.
pub type CounterBaseline = HashMap<RouteKey, RouteCounters>;

/// The baseline a set of route rows leaves behind.
pub fn baseline_of(routes: &RoutesResponse) -> CounterBaseline {
    routes
        .routes
        .iter()
        .map(|entry| (RouteKey::of(entry), RouteCounters::of(entry)))
        .collect()
}

/// One row of the routes table, with rates already computed.
#[derive(Debug, Clone)]
pub struct RouteRow {
    /// The matched host.
    pub host: String,
    /// The matched path.
    pub path: String,
    /// How the path is matched.
    pub path_type: PathType,
    /// The backend this route sends to.
    pub backend: String,
    /// Ready endpoints behind that backend.
    pub endpoints: u64,
    /// Requests served, cumulative — shown in `--once`, not in the live table.
    pub requests_total: u64,
    /// 5xx responses, cumulative.
    pub errors_5xx_total: u64,
    /// Requests per second over the last interval, or `None` with no baseline.
    pub rps: Option<f64>,
    /// 5xx as a percentage of requests over the last interval.
    pub error_rate_percent: Option<f64>,
    /// Mean upstream latency over the last interval, in milliseconds.
    ///
    /// This is a windowed mean — the delta of the sum over the delta of the
    /// count — not the lifetime mean. On a process that has been up for a week
    /// the lifetime mean cannot move, and an upstream that just started taking
    /// two seconds would not show up in it at all.
    pub avg_latency_ms: Option<f64>,
    /// The canary split, if any.
    pub canary: Option<Canary>,
    /// The canary side's 5xx percentage over the last interval.
    ///
    /// `None` where there is no canary, no baseline, or no canary traffic in
    /// the window — the same rule the route-wide rate follows, and for the same
    /// reason: 0% on a canary nothing has reached reads as "healthy" when the
    /// truth is "no evidence either way", and that is the number somebody is
    /// about to promote on.
    pub canary_error_rate_percent: Option<f64>,
    /// The canary side's mean upstream latency over the last interval.
    pub canary_avg_latency_ms: Option<f64>,
    /// Whether this route was absent from the previous poll.
    pub is_new: bool,
}

impl RouteRow {
    /// The canary split, and how the canary is doing on it.
    ///
    /// One column rather than three. The canary's error rate and latency are
    /// meaningless without the share beside them — 2% of what? — and appending
    /// rather than prepending means the half a narrow terminal truncates away
    /// is the half the row can do without. The numbers are windowed, so a
    /// canary that started failing a minute ago says so here even on a process
    /// that has been up for a week.
    pub fn canary_label(&self) -> String {
        use std::fmt::Write as _;

        let Some(canary) = &self.canary else {
            return "-".to_string();
        };
        let mut label = format!("{}%→{}", canary.weight_percent, canary.backend);
        if let Some(rate) = self.canary_error_rate_percent {
            let _ = write!(label, " {rate:.1}%");
        }
        if let Some(ms) = self.canary_avg_latency_ms {
            let _ = write!(label, " {ms:.0}ms");
        }
        label
    }

    /// What the filter box matches against.
    fn haystack(&self) -> String {
        format!(
            "{} {} {} {}",
            self.host,
            self.path,
            self.backend,
            self.path_type.as_str()
        )
    }

    /// Whether this row survives a filter.
    ///
    /// Case-insensitive substring across host, path, backend and path type. Not
    /// a regex: the filter is typed live, one character at a time, and every
    /// prefix of a regex a person is halfway through typing is either a syntax
    /// error or a pattern that means something else.
    pub fn matches(&self, needle: &str) -> bool {
        if needle.is_empty() {
            return true;
        }
        self.haystack().to_lowercase().contains(&needle.to_lowercase())
    }
}

/// Which column the table is ordered by.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Sort {
    /// Requests per second, busiest first.
    #[default]
    Rps,
    /// Error rate, worst first.
    Errors,
    /// Mean upstream latency, slowest first.
    Latency,
    /// Host then path, alphabetically.
    Host,
}

impl Sort {
    /// The column name, for the footer.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Rps => "rps",
            Self::Errors => "5xx",
            Self::Latency => "latency",
            Self::Host => "host",
        }
    }

    /// The key that selects this column.
    pub fn key(self) -> char {
        match self {
            Self::Rps => 'r',
            Self::Errors => 'e',
            Self::Latency => 'l',
            Self::Host => 'h',
        }
    }

    /// The column a key selects.
    pub fn from_key(key: char) -> Option<Self> {
        match key {
            'r' => Some(Self::Rps),
            'e' => Some(Self::Errors),
            'l' => Some(Self::Latency),
            'h' => Some(Self::Host),
            _ => None,
        }
    }

    /// Whether this column starts out descending.
    ///
    /// The interesting end of a rate is the top and the interesting end of a
    /// name is `a`, so the default direction differs by column and the toggle
    /// is relative to it.
    pub fn defaults_to_descending(self) -> bool {
        !matches!(self, Self::Host)
    }
}

/// Orders rows in place.
///
/// Rows with no value for the sort column always sink to the bottom, in both
/// directions. A route that has no rate yet is not "the slowest" and should not
/// be parked at the top of a latency sort by an ascending order that treats
/// missing as zero.
///
/// Ties break on host then path so that rows do not swap places between polls
/// when their rates are equal — a table that reshuffles every second is a table
/// nobody can read.
pub fn sort_rows(rows: &mut [RouteRow], sort: Sort, descending: bool) {
    rows.sort_by(|a, b| {
        let ordering = match sort {
            Sort::Host => a.host.cmp(&b.host).then_with(|| a.path.cmp(&b.path)),
            Sort::Rps => compare_optional(a.rps, b.rps, descending),
            Sort::Errors => compare_optional(a.error_rate_percent, b.error_rate_percent, descending),
            Sort::Latency => compare_optional(a.avg_latency_ms, b.avg_latency_ms, descending),
        };
        let directed = match sort {
            // Host is the only column whose "descending" is a plain reversal;
            // the numeric columns have already applied the direction inside
            // `compare_optional`, because they have to keep `None` at the
            // bottom either way.
            Sort::Host if descending => ordering.reverse(),
            _ => ordering,
        };
        directed
            .then_with(|| a.host.cmp(&b.host))
            .then_with(|| a.path.cmp(&b.path))
    });
}

/// Orders two optional numbers, keeping `None` last whichever way round the
/// present values are ordered.
fn compare_optional(a: Option<f64>, b: Option<f64>, descending: bool) -> std::cmp::Ordering {
    match (a, b) {
        (Some(a), Some(b)) => {
            // `total_cmp` rather than `partial_cmp`: a NaN out of a division
            // this module tries hard not to perform would otherwise make the
            // comparator inconsistent, and `sort_by` with an inconsistent
            // comparator is allowed to panic.
            let ordering = a.total_cmp(&b);
            if descending {
                ordering.reverse()
            } else {
                ordering
            }
        }
        (Some(_), None) => std::cmp::Ordering::Less,
        (None, Some(_)) => std::cmp::Ordering::Greater,
        (None, None) => std::cmp::Ordering::Equal,
    }
}

/// A delta of two `u64` counters that cannot go negative.
fn counter_delta(now: u64, before: u64) -> u64 {
    now.saturating_sub(before)
}

/// A delta of two `f64` sums that cannot go negative and cannot be `NaN`.
fn sum_delta(now: f64, before: f64) -> f64 {
    let delta = now - before;
    if delta.is_finite() && delta > 0.0 {
        delta
    } else {
        0.0
    }
}

/// The interval to divide by, or `None` if it is not usable.
///
/// A zero or negative interval happens when two polls land in the same clock
/// tick — on a manual refresh, mostly. Dividing by it yields an infinity that
/// would render as a rate.
fn usable_seconds(elapsed: Duration) -> Option<f64> {
    let seconds = elapsed.as_secs_f64();
    (seconds.is_finite() && seconds > 0.0).then_some(seconds)
}

/// One interval's rates for one route, route-wide and canary-only.
///
/// A named struct rather than a tuple: five `Option<f64>`s positionally is a
/// transposition waiting to happen, and the two canary fields look exactly like
/// the two route-wide ones.
#[derive(Debug, Clone, Copy)]
struct Rates {
    rps: f64,
    error_rate: Option<f64>,
    avg_latency: Option<f64>,
    canary_error_rate: Option<f64>,
    canary_avg_latency: Option<f64>,
}

/// Computes the display rows for one poll.
///
/// `baseline` is the previous poll's counters, or `None` on the very first
/// poll. The distinction matters: on the first poll nothing is "new", because
/// everything is; on later polls, a route missing from the baseline genuinely
/// appeared and is worth pointing at.
pub fn compute_rows(
    routes: &RoutesResponse,
    baseline: Option<&CounterBaseline>,
    elapsed: Duration,
) -> Vec<RouteRow> {
    let seconds = usable_seconds(elapsed);

    routes
        .routes
        .iter()
        .map(|entry| {
            let key = RouteKey::of(entry);
            let previous = baseline.and_then(|b| b.get(&key));
            let is_new = baseline.is_some() && previous.is_none();

            let counters = RouteCounters::of(entry);
            let rates = previous.zip(seconds).map(|(before, seconds)| {
                let requests = counter_delta(counters.requests_total, before.requests_total);
                let errors = counter_delta(counters.errors_5xx_total, before.errors_5xx_total);
                let latency_sum = sum_delta(counters.latency_ms_sum, before.latency_ms_sum);
                let latency_count = counter_delta(counters.latency_count, before.latency_count);

                let rps = requests as f64 / seconds;
                // An interval with no requests has no error rate — not a zero
                // one. Reporting 0% for an idle route reads as "healthy" when
                // the truth is "no evidence either way".
                let error_rate = (requests > 0)
                    .then(|| errors as f64 * 100.0 / requests as f64);
                let avg_latency =
                    (latency_count > 0).then(|| latency_sum / latency_count as f64);

                // The same arithmetic again on the canary subset. Windowed for
                // the same reason the route-wide numbers are: on a process that
                // has been up for a week, a lifetime error rate cannot move
                // fast enough to show a canary that started failing a minute
                // ago — which is exactly the moment somebody is watching.
                let canary_requests =
                    counter_delta(counters.canary.requests_total, before.canary.requests_total);
                let canary_errors = counter_delta(
                    counters.canary.errors_5xx_total,
                    before.canary.errors_5xx_total,
                );
                let canary_latency_sum =
                    sum_delta(counters.canary.latency_ms_sum, before.canary.latency_ms_sum);
                let canary_latency_count =
                    counter_delta(counters.canary.latency_count, before.canary.latency_count);

                let canary_error_rate = (canary_requests > 0)
                    .then(|| canary_errors as f64 * 100.0 / canary_requests as f64);
                let canary_avg_latency = (canary_latency_count > 0)
                    .then(|| canary_latency_sum / canary_latency_count as f64);

                Rates {
                    rps,
                    error_rate,
                    avg_latency,
                    canary_error_rate,
                    canary_avg_latency,
                }
            });

            RouteRow {
                host: entry.host.clone(),
                path: entry.path.clone(),
                path_type: entry.path_type.clone(),
                backend: entry.backend.clone(),
                endpoints: entry.endpoints,
                requests_total: counters.requests_total,
                errors_5xx_total: counters.errors_5xx_total,
                rps: rates.map(|r| r.rps),
                error_rate_percent: rates.and_then(|r| r.error_rate),
                avg_latency_ms: rates.and_then(|r| r.avg_latency),
                canary: entry.canary.clone(),
                canary_error_rate_percent: rates.and_then(|r| r.canary_error_rate),
                canary_avg_latency_ms: rates.and_then(|r| r.canary_avg_latency),
                is_new,
            }
        })
        .collect()
}

/// The numbers in the header.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct GlobalStats {
    /// Requests per second across every route, over the last interval.
    pub rps: Option<f64>,
    /// 5xx as a percentage of requests, over the last interval.
    pub error_rate_percent: Option<f64>,
    /// Mean upstream latency over the last interval, in milliseconds.
    pub avg_latency_ms: Option<f64>,
    /// Downstream connections currently open.
    pub active_connections: Option<i64>,
}

/// Differences two scrapes into the header numbers.
///
/// Same rules as the per-route version: saturating deltas, no rate without a
/// baseline, and an error rate only when there were requests to have errors in.
pub fn compute_global(
    now: &MetricsSnapshot,
    before: Option<&MetricsSnapshot>,
    elapsed: Duration,
) -> GlobalStats {
    let mut stats = GlobalStats {
        active_connections: now.active_connections,
        ..GlobalStats::default()
    };

    let Some((before, seconds)) = before.zip(usable_seconds(elapsed)) else {
        return stats;
    };

    let requests = counter_delta(now.requests_total, before.requests_total);
    let errors = counter_delta(now.errors_5xx_total, before.errors_5xx_total);
    let latency_sum = sum_delta(now.latency_sum_seconds, before.latency_sum_seconds);
    let latency_count = counter_delta(now.latency_count, before.latency_count);

    stats.rps = Some(requests as f64 / seconds);
    stats.error_rate_percent = (requests > 0).then(|| errors as f64 * 100.0 / requests as f64);
    stats.avg_latency_ms =
        (latency_count > 0).then(|| latency_sum * 1000.0 / latency_count as f64);
    stats
}

#[cfg(test)]
mod tests {
    use super::*;

    fn route(host: &str, path: &str, backend: &str, requests: u64, errors: u64) -> RouteEntry {
        RouteEntry {
            host: host.to_string(),
            path: path.to_string(),
            path_type: PathType::Prefix,
            backend: backend.to_string(),
            endpoints: 2,
            requests_total: requests,
            errors_5xx_total: errors,
            upstream_latency_ms_sum: 0.0,
            upstream_latency_count: 0,
            canary_stats: None,
            canary: None,
        }
    }

    fn response(routes: Vec<RouteEntry>) -> RoutesResponse {
        RoutesResponse {
            generation: 1,
            routes,
        }
    }

    fn one_second() -> Duration {
        Duration::from_secs(1)
    }

    #[test]
    fn the_first_poll_has_no_rates_and_flags_nothing_as_new() {
        let now = response(vec![route("a.example", "/", "svc", 5_000, 3)]);
        let rows = compute_rows(&now, None, one_second());

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].rps, None, "a lifetime counter is not a rate");
        assert_eq!(rows[0].error_rate_percent, None);
        assert!(!rows[0].is_new, "everything is new on the first poll");
        assert_eq!(rows[0].requests_total, 5_000);
    }

    #[test]
    fn the_second_poll_reports_the_difference_over_the_measured_interval() {
        let first = response(vec![route("a.example", "/", "svc", 1_000, 10)]);
        let baseline = baseline_of(&first);
        let second = response(vec![route("a.example", "/", "svc", 1_300, 16)]);

        let rows = compute_rows(&second, Some(&baseline), Duration::from_secs(2));
        let row = &rows[0];
        assert_eq!(row.rps, Some(150.0), "300 requests over two seconds");
        let error_rate = row.error_rate_percent.expect("requests happened");
        assert!((error_rate - 2.0).abs() < 1e-9, "6 of 300 is 2%");
        assert!(!row.is_new);
    }

    #[test]
    fn a_counter_that_went_backwards_clamps_to_zero_instead_of_wrapping() {
        // The data plane restarted between polls: every counter is now lower.
        let first = response(vec![route("a.example", "/", "svc", 1_000_000, 500)]);
        let baseline = baseline_of(&first);
        let second = response(vec![route("a.example", "/", "svc", 12, 0)]);

        let rows = compute_rows(&second, Some(&baseline), one_second());
        assert_eq!(
            rows[0].rps,
            Some(0.0),
            "a reset is zero, not eighteen quintillion"
        );
        assert_eq!(rows[0].error_rate_percent, None, "no requests in the window");
    }

    #[test]
    fn a_route_that_appeared_since_the_last_poll_is_flagged_and_has_no_rate() {
        let first = response(vec![route("a.example", "/", "svc", 100, 0)]);
        let baseline = baseline_of(&first);
        let second = response(vec![
            route("a.example", "/", "svc", 200, 0),
            route("b.example", "/new", "svc2", 9_999, 0),
        ]);

        let rows = compute_rows(&second, Some(&baseline), one_second());
        let old = rows.iter().find(|r| r.host == "a.example").expect("present");
        let new = rows.iter().find(|r| r.host == "b.example").expect("present");

        assert!(!old.is_new);
        assert_eq!(old.rps, Some(100.0));
        assert!(new.is_new, "absent from the baseline");
        assert_eq!(
            new.rps, None,
            "its lifetime counter is not a rate for this interval"
        );
    }

    #[test]
    fn a_route_keeps_its_identity_when_only_the_backend_changes() {
        // The interesting case: a generation swapped the backend under a route
        // that is still receiving traffic. The row must keep counting, not
        // blank out and re-flag as new.
        let first = response(vec![route("a.example", "/v1", "api-v1", 1_000, 0)]);
        let baseline = baseline_of(&first);
        let second = response(vec![route("a.example", "/v1", "api-v2", 1_250, 0)]);

        let rows = compute_rows(&second, Some(&baseline), one_second());
        assert!(!rows[0].is_new, "same host, path and path type");
        assert_eq!(rows[0].rps, Some(250.0));
        assert_eq!(rows[0].backend, "api-v2", "the new backend is displayed");
    }

    #[test]
    fn changing_the_path_type_is_a_different_route() {
        let mut before = route("a.example", "/v1", "api", 1_000, 0);
        before.path_type = PathType::Prefix;
        let baseline = baseline_of(&response(vec![before]));

        let mut after = route("a.example", "/v1", "api", 1_200, 0);
        after.path_type = PathType::Exact;

        let rows = compute_rows(&response(vec![after]), Some(&baseline), one_second());
        assert!(rows[0].is_new, "a different match rule is a different route");
    }

    #[test]
    fn the_windowed_latency_mean_is_not_the_lifetime_mean() {
        // A route with a long, fast history that just got slow. The lifetime
        // mean is ~1ms; the last interval was 500ms. The display must show the
        // second number.
        let mut before = route("a.example", "/", "svc", 10_000, 0);
        before.upstream_latency_ms_sum = 10_000.0;
        before.upstream_latency_count = 10_000;
        let baseline = baseline_of(&response(vec![before]));

        let mut after = route("a.example", "/", "svc", 10_010, 0);
        after.upstream_latency_ms_sum = 10_000.0 + 5_000.0;
        after.upstream_latency_count = 10_010;

        let rows = compute_rows(&response(vec![after]), Some(&baseline), one_second());
        let avg = rows[0].avg_latency_ms.expect("ten observations");
        assert!((avg - 500.0).abs() < 1e-9, "5000ms over 10 requests, got {avg}");
    }

    #[test]
    fn an_interval_with_no_latency_observations_reports_no_latency() {
        let mut before = route("a.example", "/", "svc", 100, 0);
        before.upstream_latency_ms_sum = 250.0;
        before.upstream_latency_count = 100;
        let baseline = baseline_of(&response(vec![before]));

        let mut after = route("a.example", "/", "svc", 100, 0);
        after.upstream_latency_ms_sum = 250.0;
        after.upstream_latency_count = 100;

        let rows = compute_rows(&response(vec![after]), Some(&baseline), one_second());
        assert_eq!(rows[0].avg_latency_ms, None, "no new observations");
    }

    #[test]
    fn a_zero_length_interval_produces_no_rates_rather_than_infinity() {
        let first = response(vec![route("a.example", "/", "svc", 100, 0)]);
        let baseline = baseline_of(&first);
        let second = response(vec![route("a.example", "/", "svc", 200, 0)]);

        let rows = compute_rows(&second, Some(&baseline), Duration::ZERO);
        assert_eq!(rows[0].rps, None);
    }

    #[test]
    fn a_route_that_disappeared_simply_stops_being_a_row() {
        let first = response(vec![
            route("a.example", "/", "svc", 100, 0),
            route("gone.example", "/", "svc", 100, 0),
        ]);
        let baseline = baseline_of(&first);
        let second = response(vec![route("a.example", "/", "svc", 150, 0)]);

        let rows = compute_rows(&second, Some(&baseline), one_second());
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].host, "a.example");
    }

    #[test]
    fn baselines_key_every_route_separately() {
        let routes = response(vec![
            route("a.example", "/one", "svc", 1, 0),
            route("a.example", "/two", "svc", 2, 0),
            route("b.example", "/one", "svc", 3, 0),
        ]);
        let baseline = baseline_of(&routes);
        assert_eq!(baseline.len(), 3);
        let key = RouteKey {
            host: "a.example".to_string(),
            path: "/two".to_string(),
            path_type: "Prefix".to_string(),
        };
        assert_eq!(baseline.get(&key).map(|c| c.requests_total), Some(2));
    }

    // --- sorting ---------------------------------------------------------

    fn rows_for_sorting() -> Vec<RouteRow> {
        let build = |host: &str, rps: Option<f64>, errors: Option<f64>, latency: Option<f64>| {
            RouteRow {
                host: host.to_string(),
                path: "/".to_string(),
                path_type: PathType::Prefix,
                backend: "svc".to_string(),
                endpoints: 1,
                requests_total: 0,
                errors_5xx_total: 0,
                rps,
                error_rate_percent: errors,
                avg_latency_ms: latency,
                canary: None,
                canary_error_rate_percent: None,
                canary_avg_latency_ms: None,
                is_new: false,
            }
        };
        vec![
            build("beta", Some(10.0), Some(1.0), Some(50.0)),
            build("alpha", Some(30.0), Some(0.0), Some(5.0)),
            build("delta", None, None, None),
            build("gamma", Some(20.0), Some(9.0), Some(120.0)),
        ]
    }

    fn hosts(rows: &[RouteRow]) -> Vec<&str> {
        rows.iter().map(|r| r.host.as_str()).collect()
    }

    #[test]
    fn sorting_by_rps_puts_the_busiest_first() {
        let mut rows = rows_for_sorting();
        sort_rows(&mut rows, Sort::Rps, true);
        assert_eq!(hosts(&rows), ["alpha", "gamma", "beta", "delta"]);
    }

    #[test]
    fn sorting_by_errors_puts_the_worst_first() {
        let mut rows = rows_for_sorting();
        sort_rows(&mut rows, Sort::Errors, true);
        assert_eq!(hosts(&rows), ["gamma", "beta", "alpha", "delta"]);
    }

    #[test]
    fn sorting_by_latency_puts_the_slowest_first() {
        let mut rows = rows_for_sorting();
        sort_rows(&mut rows, Sort::Latency, true);
        assert_eq!(hosts(&rows), ["gamma", "beta", "alpha", "delta"]);
    }

    #[test]
    fn sorting_by_host_is_alphabetical_and_reverses() {
        let mut rows = rows_for_sorting();
        sort_rows(&mut rows, Sort::Host, false);
        assert_eq!(hosts(&rows), ["alpha", "beta", "delta", "gamma"]);

        sort_rows(&mut rows, Sort::Host, true);
        assert_eq!(hosts(&rows), ["gamma", "delta", "beta", "alpha"]);
    }

    #[test]
    fn rows_without_a_value_sink_to_the_bottom_in_both_directions() {
        let mut rows = rows_for_sorting();
        sort_rows(&mut rows, Sort::Rps, true);
        assert_eq!(rows.last().expect("rows").host, "delta");

        sort_rows(&mut rows, Sort::Rps, false);
        assert_eq!(
            rows.last().expect("rows").host,
            "delta",
            "a route with no rate is not the quietest route"
        );
        assert_eq!(hosts(&rows), ["beta", "gamma", "alpha", "delta"]);
    }

    #[test]
    fn equal_values_keep_a_stable_order_so_the_table_does_not_flicker() {
        let build = |host: &str, path: &str| RouteRow {
            host: host.to_string(),
            path: path.to_string(),
            path_type: PathType::Prefix,
            backend: "svc".to_string(),
            endpoints: 1,
            requests_total: 0,
            errors_5xx_total: 0,
            rps: Some(7.0),
            error_rate_percent: None,
            avg_latency_ms: None,
            canary: None,
            canary_error_rate_percent: None,
            canary_avg_latency_ms: None,
            is_new: false,
        };
        let mut rows = vec![
            build("b", "/z"),
            build("a", "/b"),
            build("a", "/a"),
            build("b", "/a"),
        ];
        sort_rows(&mut rows, Sort::Rps, true);
        let order: Vec<String> = rows.iter().map(|r| format!("{}{}", r.host, r.path)).collect();
        assert_eq!(order, ["a/a", "a/b", "b/a", "b/z"]);
    }

    #[test]
    fn sort_keys_round_trip() {
        for sort in [Sort::Rps, Sort::Errors, Sort::Latency, Sort::Host] {
            assert_eq!(Sort::from_key(sort.key()), Some(sort));
            assert!(!sort.as_str().is_empty());
        }
        assert_eq!(Sort::from_key('z'), None);
        assert!(Sort::Rps.defaults_to_descending());
        assert!(!Sort::Host.defaults_to_descending());
    }

    // --- filtering -------------------------------------------------------

    #[test]
    fn an_empty_filter_matches_everything() {
        let rows = rows_for_sorting();
        assert!(rows.iter().all(|r| r.matches("")));
    }

    #[test]
    fn the_filter_is_a_case_insensitive_substring_over_the_visible_columns() {
        let mut row = rows_for_sorting().remove(0);
        row.host = "Shop.Example.COM".to_string();
        row.path = "/checkout".to_string();
        row.backend = "checkout-svc".to_string();

        assert!(row.matches("shop"), "host, case-insensitively");
        assert!(row.matches("EXAMPLE"), "needle case does not matter either");
        assert!(row.matches("/check"), "path");
        assert!(row.matches("checkout-svc"), "backend");
        assert!(row.matches("prefix"), "path type");
        assert!(!row.matches("api"), "matches nothing on this row");
    }

    // --- global stats ----------------------------------------------------

    fn scrape(requests: u64, errors: u64, latency_sum: f64, latency_count: u64) -> MetricsSnapshot {
        MetricsSnapshot {
            requests_total: requests,
            errors_5xx_total: errors,
            active_connections: Some(12),
            generation: Some(4),
            latency_sum_seconds: latency_sum,
            latency_count,
            pinned: None,
        }
    }

    #[test]
    fn the_first_scrape_gives_connections_but_no_global_rate() {
        let stats = compute_global(&scrape(1_000, 5, 1.0, 1_000), None, one_second());
        assert_eq!(stats.rps, None);
        assert_eq!(stats.error_rate_percent, None);
        assert_eq!(stats.active_connections, Some(12), "a gauge needs no baseline");
    }

    #[test]
    fn the_global_rate_and_error_percentage_come_from_the_delta() {
        let before = scrape(1_000, 10, 1.0, 1_000);
        let now = scrape(1_400, 30, 1.2, 1_400);
        let stats = compute_global(&now, Some(&before), Duration::from_secs(2));

        assert_eq!(stats.rps, Some(200.0), "400 requests over two seconds");
        let errors = stats.error_rate_percent.expect("requests happened");
        assert!((errors - 5.0).abs() < 1e-9, "20 of 400 is 5%");
        let latency = stats.avg_latency_ms.expect("observations happened");
        // 0.2s over 400 observations is 0.5ms each.
        assert!((latency - 0.5).abs() < 1e-9, "got {latency}");
    }

    #[test]
    fn a_restarted_data_plane_reads_as_zero_globally_too() {
        let before = scrape(5_000_000, 400, 900.0, 5_000_000);
        let now = scrape(3, 0, 0.001, 3);
        let stats = compute_global(&now, Some(&before), one_second());
        assert_eq!(stats.rps, Some(0.0));
        assert_eq!(stats.error_rate_percent, None);
        assert_eq!(stats.avg_latency_ms, None);
    }

    #[test]
    fn an_idle_interval_has_no_error_rate_rather_than_a_healthy_looking_zero() {
        let before = scrape(1_000, 50, 1.0, 1_000);
        let now = scrape(1_000, 50, 1.0, 1_000);
        let stats = compute_global(&now, Some(&before), one_second());
        assert_eq!(stats.rps, Some(0.0));
        assert_eq!(
            stats.error_rate_percent, None,
            "no requests is not the same as no errors"
        );
    }

    /// A route carrying a canary that has served `requests` of which `errors`
    /// were 5xx, each taking `latency_ms`.
    fn canaried(
        requests: u64,
        errors: u64,
        canary_requests: u64,
        canary_errors: u64,
        canary_latency_ms: f64,
    ) -> RouteEntry {
        RouteEntry {
            requests_total: requests,
            errors_5xx_total: errors,
            canary: Some(Canary {
                backend: "api-v3".to_string(),
                weight_percent: 10,
            }),
            canary_stats: Some(crate::contract::CanaryStats {
                requests_total: canary_requests,
                errors_5xx_total: canary_errors,
                upstream_latency_ms_sum: canary_latency_ms * canary_requests as f64,
                upstream_latency_count: canary_requests,
            }),
            ..route("api.example.com", "/", "api-v2", requests, errors)
        }
    }

    #[test]
    fn the_canary_rate_is_windowed_like_every_other_rate() {
        // The property that makes it usable: a canary that started failing one
        // interval ago must show it, on a process whose lifetime numbers are
        // dominated by a week of clean traffic.
        let before = response(vec![canaried(100_000, 0, 10_000, 0, 10.0)]);
        let now = response(vec![canaried(100_200, 20, 10_100, 20, 10.0)]);

        let rows = compute_rows(
            &now,
            Some(&baseline_of(&before)),
            Duration::from_secs(10),
        );
        let row = &rows[0];

        // 20 of the canary's 100 new requests failed.
        assert_eq!(row.canary_error_rate_percent, Some(20.0));
        // Against a route-wide rate of 20 in 200, which is where the stable
        // side's health hides if you only look at one number.
        assert_eq!(row.error_rate_percent, Some(10.0));
    }

    #[test]
    fn a_canary_with_no_traffic_this_window_reports_no_rate() {
        // Not a zero rate. 0% on a canary nothing has reached reads as
        // "healthy", and that is the number somebody is about to promote on.
        let before = response(vec![canaried(1000, 0, 100, 0, 10.0)]);
        let now = response(vec![canaried(1200, 0, 100, 0, 10.0)]);

        let rows = compute_rows(&now, Some(&baseline_of(&before)), Duration::from_secs(10));
        assert_eq!(rows[0].canary_error_rate_percent, None);
        assert_eq!(rows[0].canary_avg_latency_ms, None);
        assert_eq!(rows[0].error_rate_percent, Some(0.0), "the route did serve");
    }

    #[test]
    fn a_route_with_no_canary_has_no_canary_rate() {
        let before = response(vec![route("api.example.com", "/", "api", 100, 0)]);
        let now = response(vec![route("api.example.com", "/", "api", 200, 4)]);

        let rows = compute_rows(&now, Some(&baseline_of(&before)), Duration::from_secs(10));
        assert_eq!(rows[0].canary_error_rate_percent, None);
        assert_eq!(rows[0].canary_avg_latency_ms, None);
    }

    #[test]
    fn the_canary_latency_is_the_windowed_mean() {
        let before = response(vec![canaried(1000, 0, 100, 0, 10.0)]);
        let now = response(vec![canaried(1200, 0, 200, 0, 30.0)]);

        let rows = compute_rows(&now, Some(&baseline_of(&before)), Duration::from_secs(10));
        // 200 requests at 30ms is 6000ms of sum; 100 at 10ms was 1000. The
        // window is 5000ms over 100 observations.
        assert_eq!(rows[0].canary_avg_latency_ms, Some(50.0));
    }

    #[test]
    fn a_server_that_omits_the_split_still_polls() {
        // `canary_stats` was added after the fact, and a mixed-version cluster
        // is a normal state during an upgrade.
        let entry = RouteEntry {
            canary: Some(Canary {
                backend: "api-v3".to_string(),
                weight_percent: 10,
            }),
            canary_stats: None,
            ..route("api.example.com", "/", "api-v2", 100, 0)
        };
        let rows = compute_rows(
            &response(vec![entry.clone()]),
            Some(&baseline_of(&response(vec![entry]))),
            Duration::from_secs(10),
        );
        assert_eq!(rows[0].canary_error_rate_percent, None);
        assert_eq!(rows[0].canary_label(), "10%→api-v3");
    }

    #[test]
    fn canary_health_is_appended_after_the_split() {
        // The share is what the row is about; the health is the part a narrow
        // terminal is allowed to cut.
        let mut row = rows_for_sorting().remove(0);
        row.canary = Some(Canary {
            backend: "api-v3".to_string(),
            weight_percent: 25,
        });
        assert_eq!(row.canary_label(), "25%→api-v3");

        row.canary_error_rate_percent = Some(2.14);
        row.canary_avg_latency_ms = Some(41.6);
        assert_eq!(row.canary_label(), "25%→api-v3 2.1% 42ms");

        // A canary with traffic but no upstream observations still says what
        // it knows rather than nothing.
        row.canary_avg_latency_ms = None;
        assert_eq!(row.canary_label(), "25%→api-v3 2.1%");
    }

    #[test]
    fn canary_labels_render_the_split_and_its_absence() {
        let mut row = rows_for_sorting().remove(0);
        assert_eq!(row.canary_label(), "-");
        row.canary = Some(Canary {
            backend: "api-v3".to_string(),
            weight_percent: 10,
        });
        assert_eq!(row.canary_label(), "10%→api-v3");
    }
}
