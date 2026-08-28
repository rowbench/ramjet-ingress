//! `--once`: one poll, printed, no terminal.
//!
//! This is the mode for scripts, CI logs and pasting into an incident channel.
//! It has three properties the interactive view does not need and this one
//! cannot do without:
//!
//! **It is deterministic.** Rows come out sorted by host and path, never by a
//! rate, so two runs against an unchanged daemon produce identical bytes and a
//! diff between them means something.
//!
//! **It reports counters, not rates.** A rate is a difference between two
//! polls, and this mode performs one. Printing `requests_total / uptime` and
//! labelling it "rps" would be a number that looks live and is not; the columns
//! below are cumulative and say so.
//!
//! **It does not truncate.** The interactive table elides a long host to fit a
//! column. Here a host is data somebody may be about to grep, and cutting it
//! short to make a nicer-looking column would corrupt the output for its actual
//! purpose.

use crate::client::Snapshot;
use crate::rfc3339;

/// Renders one poll as aligned text.
pub fn render(snapshot: &Snapshot, now_unix: i64) -> String {
    let mut out = String::with_capacity(4096);

    out.push_str(&header(snapshot, now_unix));
    out.push('\n');
    out.push_str(&routes_table(snapshot));
    out.push('\n');
    out.push_str(&generations(snapshot, now_unix));

    out
}

/// Renders one poll as JSON.
///
/// The merged snapshot, exactly as the client assembled it: both admin
/// responses verbatim plus the series read out of `/metrics`. Verbatim matters
/// — a consumer piping this into `jq` wants the server's fields, not this
/// program's opinion of them.
pub fn render_json(snapshot: &Snapshot) -> Result<String, serde_json::Error> {
    serde_json::to_string_pretty(snapshot)
}

/// The first few lines: where, which generation, and the global counters.
fn header(snapshot: &Snapshot, now_unix: i64) -> String {
    let mut out = String::new();
    out.push_str(&format!("ramjet-top  {}\n", snapshot.url));

    let serving = snapshot.serving();
    let newest = snapshot.generations.generations.first();
    let applied = newest
        .map(|g| format!(" · applied {} ago", rfc3339::age_of(&g.applied_at, now_unix)))
        .unwrap_or_default();

    out.push_str(&format!(
        "generation {serving} serving · {} routes · {} hosts · {} certs{applied}\n",
        newest.map_or(snapshot.routes.routes.len() as u64, |g| g.routes),
        newest.map_or(0, |g| g.hosts),
        newest.map_or(0, |g| g.certs),
    ));

    let metrics = &snapshot.metrics;
    let latency = metrics
        .lifetime_mean_latency_ms()
        .map_or_else(|| "-".to_string(), |ms| format!("{ms:.2}ms"));
    out.push_str(&format!(
        "requests {} · 5xx {} · connections {} · mean upstream {latency}\n",
        metrics.requests_total,
        metrics.errors_5xx_total,
        metrics
            .active_connections
            .map_or_else(|| "-".to_string(), |c| c.to_string()),
    ));

    if let Some(pinned) = snapshot.pinned() {
        // Loud, and on its own line. A pinned data plane is ignoring new
        // configuration, and somebody reading this output in a hurry has to
        // see that before they conclude their change did not apply.
        out.push_str(&format!(
            "PINNED to generation {pinned} — new generations are NOT being served\n"
        ));
    }

    out
}

/// The routes table.
fn routes_table(snapshot: &Snapshot) -> String {
    let headers = [
        "HOST", "PATH", "TYPE", "BACKEND", "EPS", "REQUESTS", "5XX", "MEAN ms", "CANARY",
    ];

    let mut routes: Vec<_> = snapshot.routes.routes.iter().collect();
    routes.sort_by(|a, b| a.host.cmp(&b.host).then_with(|| a.path.cmp(&b.path)));

    let rows: Vec<Vec<String>> = routes
        .iter()
        .map(|route| {
            let mean = (route.upstream_latency_count > 0)
                .then(|| route.upstream_latency_ms_sum / route.upstream_latency_count as f64)
                .map_or_else(|| "-".to_string(), |ms| format!("{ms:.2}"));
            let canary = route.canary.as_ref().map_or_else(
                || "-".to_string(),
                |c| format!("{}%->{}", c.weight_percent, c.backend),
            );
            vec![
                route.host.clone(),
                route.path.clone(),
                route.path_type.short().to_string(),
                route.backend.clone(),
                route.endpoints.to_string(),
                route.requests_total.to_string(),
                route.errors_5xx_total.to_string(),
                mean,
                canary,
            ]
        })
        .collect();

    if rows.is_empty() {
        return "no routes in the published table\n".to_string();
    }

    // Right-align the numeric columns; a column of counters that is not right
    // aligned cannot be compared by eye, which is the only reason to print it
    // as a table rather than as JSON.
    let right = [false, false, false, false, true, true, true, true, false];
    align(&headers, &rows, &right)
}

/// The generation timeline.
fn generations(snapshot: &Snapshot, now_unix: i64) -> String {
    let mut out = String::from("generations (newest first)\n");

    if snapshot.generations.generations.is_empty() {
        out.push_str("  none reported\n");
        return out;
    }

    let serving = snapshot.serving();
    let headers = ["", "GEN", "AGE", "STATE", "DIGEST", "SUMMARY"];
    let rows: Vec<Vec<String>> = snapshot
        .generations
        .generations
        .iter()
        .map(|entry| {
            // One marker column, because "serving" and "pinned" are different
            // facts and during a rollback they point at different rows.
            let marker = match (
                entry.generation == serving,
                Some(entry.generation) == snapshot.pinned(),
            ) {
                (_, true) => "P",
                (true, _) => "*",
                _ => " ",
            };
            vec![
                marker.to_string(),
                entry.generation.to_string(),
                rfc3339::age_of(&entry.applied_at, now_unix),
                if entry.published {
                    "published".to_string()
                } else {
                    "unpublished".to_string()
                },
                entry.short_digest().to_string(),
                entry.diff.summary.clone(),
            ]
        })
        .collect();

    let right = [false, true, false, false, false, false];
    out.push_str(&align(&headers, &rows, &right));
    out.push_str("\n* serving   P pinned\n");
    out
}

/// Pads cells into columns.
///
/// Widths come from the content, so nothing is ever cut; see the module docs
/// for why that is the right trade here even though it can produce a wide line.
fn align(headers: &[&str], rows: &[Vec<String>], right_align: &[bool]) -> String {
    let columns = headers.len();
    let mut widths: Vec<usize> = headers.iter().map(|h| display_width(h)).collect();
    for row in rows {
        for (i, cell) in row.iter().enumerate().take(columns) {
            let width = display_width(cell);
            if let Some(current) = widths.get_mut(i) {
                *current = (*current).max(width);
            }
        }
    }

    let mut out = String::new();
    let push_row = |cells: &[String], out: &mut String| {
        let mut line = String::new();
        for (i, cell) in cells.iter().enumerate().take(columns) {
            let width = widths.get(i).copied().unwrap_or(0);
            let pad = width.saturating_sub(display_width(cell));
            if right_align.get(i).copied().unwrap_or(false) {
                line.push_str(&" ".repeat(pad));
                line.push_str(cell);
            } else {
                line.push_str(cell);
                line.push_str(&" ".repeat(pad));
            }
            if i + 1 < columns {
                line.push_str("  ");
            }
        }
        // Trailing padding on the last column is invisible and shows up in a
        // diff or a `grep -c ' $'`, so it comes off.
        out.push_str(line.trim_end());
        out.push('\n');
    };

    let header_cells: Vec<String> = headers.iter().map(|h| (*h).to_string()).collect();
    push_row(&header_cells, &mut out);
    for row in rows {
        push_row(row, &mut out);
    }
    out
}

/// How many terminal columns a string occupies.
///
/// Counted in `char`s rather than bytes: a hostname can be an IDN, and using
/// `len()` would pad a multi-byte host by the number of bytes it takes, which
/// pushes the rest of the row out of alignment. This is not full grapheme
/// width — a double-width CJK character still counts as one — but the values in
/// these columns are hostnames, paths and Kubernetes object names, and for
/// those it is exact.
fn display_width(text: &str) -> usize {
    text.chars().count()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::contract::{
        Canary, GenerationDiff, GenerationEntry, GenerationsResponse, PathType, RouteEntry,
        RoutesResponse,
    };
    use crate::prom::MetricsSnapshot;

    /// 2026-08-28T10:02:14Z, so the fixtures below have readable ages.
    const NOW: i64 = 1_787_911_334;

    #[test]
    fn the_test_clock_is_the_instant_it_claims_to_be() {
        // Pins the constant above to the parser, so a hand-computed epoch
        // cannot quietly drift away from what every age assertion below means.
        assert_eq!(rfc3339::to_unix_seconds("2026-08-28T10:02:14Z"), Ok(NOW));
    }

    fn fixture() -> Snapshot {
        Snapshot {
            url: "http://127.0.0.1:10254".to_string(),
            generations: GenerationsResponse {
                pinned: None,
                serving: 42,
                generations: vec![
                    GenerationEntry {
                        generation: 42,
                        applied_at: "2026-08-28T10:00:00Z".to_string(),
                        published: true,
                        digest: "a1b2c3d4e5f60718293a4b5c6d7e8f90".to_string(),
                        routes: 2,
                        hosts: 2,
                        certs: 1,
                        diff: GenerationDiff {
                            summary: "1 route added".to_string(),
                            routes_added: vec!["api.example.com/v1".into()],
                            ..Default::default()
                        },
                    },
                    GenerationEntry {
                        generation: 41,
                        applied_at: "2026-08-28T09:45:00Z".to_string(),
                        published: true,
                        digest: "ffee0011".to_string(),
                        routes: 1,
                        hosts: 1,
                        certs: 1,
                        diff: GenerationDiff {
                            summary: "initial table".to_string(),
                            ..Default::default()
                        },
                    },
                ],
            },
            routes: RoutesResponse {
                generation: 42,
                routes: vec![
                    RouteEntry {
                        host: "api.example.com".to_string(),
                        path: "/v1".to_string(),
                        path_type: PathType::Prefix,
                        backend: "api-v2".to_string(),
                        endpoints: 4,
                        requests_total: 10_000,
                        errors_5xx_total: 12,
                        upstream_latency_ms_sum: 51_234.5,
                        upstream_latency_count: 9_987,
                        canary: Some(Canary {
                            backend: "api-v3".to_string(),
                            weight_percent: 10,
                        }),
                    },
                    RouteEntry {
                        host: "*".to_string(),
                        path: "/".to_string(),
                        path_type: PathType::ImplementationSpecific,
                        backend: "default-http-backend".to_string(),
                        endpoints: 1,
                        requests_total: 7,
                        errors_5xx_total: 0,
                        upstream_latency_ms_sum: 14.0,
                        upstream_latency_count: 7,
                        canary: None,
                    },
                ],
            },
            metrics: MetricsSnapshot {
                requests_total: 10_007,
                errors_5xx_total: 12,
                active_connections: Some(37),
                generation: Some(42),
                latency_sum_seconds: 51.2485,
                latency_count: 9_994,
                pinned: None,
            },
        }
    }

    #[test]
    fn the_output_names_the_target_and_the_serving_generation() {
        let text = render(&fixture(), NOW);
        assert!(text.contains("ramjet-top  http://127.0.0.1:10254"));
        assert!(text.contains("generation 42 serving"));
        assert!(text.contains("2 routes · 2 hosts · 1 certs"));
    }

    #[test]
    fn every_route_appears_with_its_counters() {
        let text = render(&fixture(), NOW);
        assert!(text.contains("api.example.com"));
        assert!(text.contains("api-v2"));
        assert!(text.contains("10000"));
        assert!(text.contains("default-http-backend"));
        assert!(text.contains("ImplSpec"));
    }

    #[test]
    fn the_canary_split_is_shown_in_ascii() {
        let text = render(&fixture(), NOW);
        // ASCII rather than the arrow the TUI uses: this output gets piped
        // into logs whose encoding nobody controls.
        assert!(text.contains("10%->api-v3"), "{text}");
        assert!(!text.contains('→'), "no non-ascii in --once output");
    }

    #[test]
    fn rows_are_sorted_by_host_and_path_so_two_runs_are_comparable() {
        let text = render(&fixture(), NOW);
        let star = text.find("\n*  ").expect("the catch-all row");
        let api = text.find("api.example.com").expect("the api row");
        assert!(star < api, "`*` sorts before `a`:\n{text}");
    }

    #[test]
    fn columns_line_up() {
        let text = render(&fixture(), NOW);
        let table: Vec<&str> = text
            .lines()
            .skip_while(|l| !l.starts_with("HOST"))
            .take(3)
            .collect();
        assert_eq!(table.len(), 3, "a header and two rows");

        let backend_column = table[0].find("BACKEND").expect("a backend header");
        for row in &table[1..] {
            let cell = row.get(backend_column..).unwrap_or("").trim_start();
            assert!(
                cell.starts_with("api-v2") || cell.starts_with("default-http-backend"),
                "row does not line up under BACKEND: {row:?}"
            );
        }
    }

    #[test]
    fn numeric_columns_are_right_aligned() {
        let text = render(&fixture(), NOW);
        let lines: Vec<&str> = text
            .lines()
            .skip_while(|l| !l.starts_with("HOST"))
            .take(3)
            .collect();
        // "REQUESTS" is 8 wide, "10000" is 5 and "7" is 1; right alignment puts
        // both of their last digits in the same column.
        let end = lines[0].find("REQUESTS").expect("header") + "REQUESTS".len();
        for row in &lines[1..] {
            let upto = row.get(..end).expect("row reaches the column");
            assert!(
                upto.ends_with('0') || upto.ends_with('7'),
                "counter is not right-aligned: {row:?}"
            );
        }
    }

    #[test]
    fn the_generation_timeline_marks_the_serving_row() {
        let text = render(&fixture(), NOW);
        assert!(text.contains("generations (newest first)"));
        assert!(text.contains("1 route added"));
        assert!(text.contains("initial table"));
        assert!(text.contains("a1b2c3d4e5f6"), "the shortened digest");
        assert!(text.contains("* serving"), "the legend");

        let serving_row = text
            .lines()
            .find(|l| l.contains("1 route added"))
            .expect("the row for generation 42");
        assert!(serving_row.starts_with('*'), "not marked: {serving_row:?}");
        assert!(serving_row.contains(" 42 "));
        assert!(serving_row.contains("published"));
    }

    #[test]
    fn a_pin_is_announced_loudly_and_marks_its_row() {
        let mut snapshot = fixture();
        snapshot.generations.pinned = Some(41);
        let text = render(&snapshot, NOW);

        assert!(text.contains("PINNED to generation 41"));
        assert!(
            text.contains("NOT being served"),
            "a reader in a hurry has to see the consequence"
        );
        let pinned_row = text
            .lines()
            .find(|l| l.contains("initial table"))
            .expect("the row for generation 41");
        assert!(pinned_row.starts_with('P'), "not marked: {pinned_row:?}");
        assert!(pinned_row.contains(" 41 "));
    }

    #[test]
    fn an_unpinned_daemon_says_nothing_about_pins() {
        let text = render(&fixture(), NOW);
        assert!(!text.contains("PINNED"));
    }

    #[test]
    fn an_empty_route_table_says_so_rather_than_printing_a_bare_header() {
        let mut snapshot = fixture();
        snapshot.routes.routes.clear();
        let text = render(&snapshot, NOW);
        assert!(text.contains("no routes in the published table"));
    }

    #[test]
    fn a_daemon_with_no_generation_history_still_renders() {
        let mut snapshot = fixture();
        snapshot.generations.generations.clear();
        let text = render(&snapshot, NOW);
        assert!(text.contains("none reported"));
        assert!(text.contains("api.example.com"), "the routes still print");
    }

    #[test]
    fn a_route_with_no_latency_observations_prints_a_dash_not_a_nan() {
        let mut snapshot = fixture();
        snapshot.routes.routes[0].upstream_latency_count = 0;
        snapshot.routes.routes[0].upstream_latency_ms_sum = 0.0;
        let text = render(&snapshot, NOW);
        assert!(!text.contains("NaN"), "{text}");
    }

    #[test]
    fn no_line_has_trailing_whitespace() {
        let text = render(&fixture(), NOW);
        for line in text.lines() {
            assert_eq!(line, line.trim_end(), "trailing space on {line:?}");
        }
    }

    #[test]
    fn a_multibyte_host_does_not_push_the_row_out_of_alignment() {
        let mut snapshot = fixture();
        // An IDN host: more bytes than characters.
        snapshot.routes.routes[1].host = "münchen.example".to_string();
        let text = render(&snapshot, NOW);

        let lines: Vec<&str> = text
            .lines()
            .skip_while(|l| !l.starts_with("HOST"))
            .take(3)
            .collect();
        assert_eq!(lines.len(), 3, "a header and two rows");

        // The PATH column begins at one character offset, and every row's path
        // has to start exactly there. Padding computed in bytes would push the
        // IDN row one column left of the rest, because the host is sixteen
        // bytes and fifteen characters.
        let path_column = lines[0]
            .char_indices()
            .position(|(_, c)| c == 'P')
            .expect("a PATH header");
        let cell_at = |line: &str| -> String {
            line.chars().skip(path_column).take(4).collect()
        };
        assert_eq!(cell_at(lines[0]), "PATH");
        for row in &lines[1..] {
            let cell = cell_at(row);
            assert!(
                cell.starts_with('/'),
                "the path column drifted on {row:?}: found {cell:?}"
            );
        }
    }

    #[test]
    fn the_json_form_round_trips_the_server_fields() {
        let json = render_json(&fixture()).expect("serializable");
        let value: serde_json::Value = serde_json::from_str(&json).expect("valid JSON");

        assert_eq!(value["generations"]["serving"], 42);
        assert_eq!(value["routes"]["routes"][0]["host"], "api.example.com");
        assert_eq!(value["routes"]["routes"][0]["path_type"], "Prefix");
        assert_eq!(
            value["routes"]["routes"][0]["canary"]["weight_percent"],
            10
        );
        assert_eq!(value["metrics"]["requests_total"], 10_007);
        assert_eq!(value["url"], "http://127.0.0.1:10254");
    }

    #[test]
    fn ages_are_relative_to_the_supplied_clock() {
        let text = render(&fixture(), NOW);
        assert!(text.contains("2m14s"), "the newest generation's age:\n{text}");
    }
}
