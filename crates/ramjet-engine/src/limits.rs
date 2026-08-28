//! What this engine answers, and everything it refuses.
//!
//! Two kinds of constant live here. The first are the error bodies the hyper
//! engine already sends, copied verbatim so a client cannot tell which engine
//! answered it. The second are this engine's own refusals — the v1 gaps — and
//! they exist as named constants rather than inline strings because a gap that
//! is only visible in a match arm is a gap nobody documents.
//!
//! Every body starts with its status code and ends with a newline. The hyper
//! crate has a unit test pinning that convention; so does this module.

use ramjet_http::encode;

/// No rule matched the host and path, and there is no default backend.
pub const NO_ROUTE: &[u8] = b"404 Not Found: no ingress rule matches this host and path\n";

/// A backend matched but has no endpoints to send the request to.
pub const NO_ENDPOINT: &[u8] = b"503 Service Unavailable: the backend has no ready endpoints\n";

/// Every endpoint tried refused the connection.
pub const CONNECT_FAILED: &[u8] = b"502 Bad Gateway: could not connect to any upstream endpoint\n";

/// A connection was established and then the exchange failed.
pub const UPSTREAM_FAILED: &[u8] = b"502 Bad Gateway: the upstream connection failed\n";

/// No response headers arrived before the deadline.
pub const TIMEOUT: &[u8] = b"504 Gateway Timeout: the upstream sent no response headers in time\n";

/// gRPC needs an HTTP/2 upstream, which ramjet does not speak in either engine.
pub const GRPC: &[u8] = b"502 Bad Gateway: gRPC requires an HTTP/2 backend; \
set nginx.ingress.kubernetes.io/backend-protocol: GRPC on the Ingress\n";

/// A backend annotated `backend-protocol: GRPC`, reached on the uring engine.
///
/// Distinct from [`GRPC`], and the distinction is the whole point. `GRPC` means
/// the operator has not told us the backend speaks HTTP/2 and should add the
/// annotation. This means they have, and it is *this engine* that cannot dial
/// it — a different problem with a different fix, so it gets a different
/// sentence rather than a shared vague one.
pub const NO_H2C_UPSTREAM: &[u8] =
    b"502 Bad Gateway: this backend needs an HTTP/2 upstream, which the uring engine does not dial; use --engine hyper\n";

/// An upstream switched protocols nobody asked it to.
///
/// A 101 answering a request with no `Upgrade` leaves this hop unable to say
/// how the rest of the connection is framed, and unwilling to guess. The same
/// body the hyper engine sends when half of an upgrade is missing.
pub const UPGRADE_FAILED: &[u8] =
    b"502 Bad Gateway: the upstream refused to complete the upgrade\n";

/// An HTTP/2 request, including h2c with prior knowledge.
pub const NO_HTTP2: &[u8] =
    b"502 Bad Gateway: the uring engine speaks HTTP/1.1 only; use --engine hyper\n";

/// The client's request could not be read.
///
/// The status varies with the fault — 400, 413, 431 or 501 — so the body is
/// built rather than constant, but it follows the same shape.
pub fn bad_request_body(status: u16, detail: &str) -> Vec<u8> {
    let reason = encode::reason(status);
    let mut body = format!("{status} {reason}: ");
    body.push_str(detail);
    body.push('\n');
    body.into_bytes()
}

/// Write a complete, self-framed response with a plain-text body.
///
/// `ramjet_http::encode` owns the framing — a caller cannot supply its own
/// `Content-Length` — which is exactly the property wanted for the responses
/// the proxy invents, as opposed to the ones it relays.
pub fn write_static(out: &mut Vec<u8>, status: u16, body: &[u8], close: bool) {
    let headers: &[(&str, &str)] = if close {
        &[
            ("Content-Type", "text/plain; charset=utf-8"),
            ("Connection", "close"),
        ]
    } else {
        &[("Content-Type", "text/plain; charset=utf-8")]
    };
    // The only way this fails is an out-of-range status or an invalid header,
    // and every call site passes literals that are neither. Writing nothing at
    // all would leave the client hanging, so a failure degrades to a bare 500.
    if encode::response(out, status, headers, body).is_err() {
        out.extend_from_slice(b"HTTP/1.1 500 Internal Server Error\r\nContent-Length: 0\r\n\r\n");
    }
}

/// Write a response to a `HEAD` request: the head a `GET` would have had,
/// without the body bytes.
pub fn write_static_head_only(out: &mut Vec<u8>, status: u16, body_len: usize, close: bool) {
    let headers: &[(&str, &str)] = if close {
        &[
            ("Content-Type", "text/plain; charset=utf-8"),
            ("Connection", "close"),
        ]
    } else {
        &[("Content-Type", "text/plain; charset=utf-8")]
    };
    if encode::response_head_only(out, status, headers, body_len).is_err() {
        out.extend_from_slice(b"HTTP/1.1 500 Internal Server Error\r\nContent-Length: 0\r\n\r\n");
    }
}

/// The gaps that are left, in one place, for `--help` and the startup log.
///
/// Printed at startup rather than buried in a doc comment, because an operator
/// choosing `--engine uring` should see what they gave up before their first
/// request fails rather than after. Lines are removed from here as the gaps
/// close, and [`the_limit_list_names_every_gap`](tests) is what stops a gap
/// from being closed in the code and left standing here.
pub const V1_LIMITS: &str = "\
the uring engine serves HTTP/1.1, with TLS, and does not speak:
  - HTTP/2, in any form, including h2c with prior knowledge (502)
  - HTTP/2 upstreams (backend-protocol: GRPC), and so gRPC (502)
  - HTTP/3, which stays on the hyper engine's QUIC listener";

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_error_body_starts_with_its_status_and_ends_with_a_newline() {
        // The same convention the hyper engine pins, so an operator grepping
        // logs for "502" finds both engines' refusals.
        for body in [
            NO_ROUTE,
            NO_ENDPOINT,
            CONNECT_FAILED,
            UPSTREAM_FAILED,
            TIMEOUT,
            GRPC,
            NO_H2C_UPSTREAM,
            UPGRADE_FAILED,
            NO_HTTP2,
        ] {
            let text = std::str::from_utf8(body).expect("ascii body");
            let code: u16 = text[..3].parse().expect("leading status code");
            assert!((100..=599).contains(&code), "{text}");
            assert!(text.ends_with('\n'), "{text}");
        }
    }

    #[test]
    fn the_hyper_engines_bodies_are_reproduced_exactly() {
        // Not a paraphrase: a client that switches engines must not be able to
        // tell by reading the body. These literals are the ones in
        // `ramjet_proxy::forward`.
        assert_eq!(
            NO_ROUTE,
            b"404 Not Found: no ingress rule matches this host and path\n"
        );
        assert_eq!(
            NO_ENDPOINT,
            b"503 Service Unavailable: the backend has no ready endpoints\n"
        );
        assert_eq!(
            CONNECT_FAILED,
            b"502 Bad Gateway: could not connect to any upstream endpoint\n"
        );
        assert_eq!(
            UPSTREAM_FAILED,
            b"502 Bad Gateway: the upstream connection failed\n"
        );
        // GRPC is asserted here because it is the one that *did* drift: the
        // hyper engine's wording changed when backend-protocol landed and this
        // copy did not follow, so for a while the two engines told a client
        // different things about the same misconfiguration. A differential test
        // with a hole in it is how that happens quietly.
        assert_eq!(
            GRPC,
            b"502 Bad Gateway: gRPC requires an HTTP/2 backend; \
set nginx.ingress.kubernetes.io/backend-protocol: GRPC on the Ingress\n"
        );
        assert_eq!(
            TIMEOUT,
            b"504 Gateway Timeout: the upstream sent no response headers in time\n"
        );
    }

    #[test]
    fn a_static_response_is_complete_and_framed() {
        let mut out = Vec::new();
        write_static(&mut out, 404, NO_ROUTE, false);
        let text = String::from_utf8(out).expect("ascii");
        assert!(text.starts_with("HTTP/1.1 404 Not Found\r\n"), "{text}");
        assert!(text.contains("Content-Type: text/plain; charset=utf-8\r\n"), "{text}");
        assert!(
            text.contains(&format!("Content-Length: {}\r\n", NO_ROUTE.len())),
            "{text}"
        );
        assert!(text.ends_with(std::str::from_utf8(NO_ROUTE).unwrap()), "{text}");
        assert!(!text.contains("Connection:"), "{text}");
    }

    #[test]
    fn closing_is_announced() {
        let mut out = Vec::new();
        write_static(&mut out, 502, CONNECT_FAILED, true);
        let text = String::from_utf8(out).expect("ascii");
        assert!(text.contains("Connection: close\r\n"), "{text}");
    }

    #[test]
    fn a_head_response_carries_the_length_but_no_body() {
        let mut out = Vec::new();
        write_static_head_only(&mut out, 404, NO_ROUTE.len(), false);
        let text = String::from_utf8(out).expect("ascii");
        assert!(
            text.contains(&format!("Content-Length: {}\r\n", NO_ROUTE.len())),
            "{text}"
        );
        assert!(text.ends_with("\r\n\r\n"), "{text}");
    }

    #[test]
    fn a_bad_request_body_follows_the_same_shape() {
        let body = bad_request_body(400, "header line has no colon");
        let text = String::from_utf8(body).expect("ascii");
        assert_eq!(text, "400 Bad Request: header line has no colon\n");
    }

    #[test]
    fn the_limit_list_names_every_gap() {
        for gap in ["HTTP/2", "gRPC", "HTTP/3"] {
            assert!(
                V1_LIMITS.contains(gap),
                "{gap} is missing from the printed limits"
            );
        }
    }

    /// The markdown tables that make the same promise [`V1_LIMITS`] does.
    ///
    /// `docs/src/operations/engines.md` has the fuller parity matrix and is
    /// deliberately not here: its first table carries a `Default | yes | no`
    /// row meaning "uring is not the default engine" rather than a missing
    /// feature, and nothing in the cell tells that apart from a gap. These two
    /// are the tables a reader meets before they have run the binary, which is
    /// what makes them the ones worth guarding.
    const PARITY_TABLES: [&str; 2] = ["README.md", "docs/src/introduction.md"];

    /// A file from the workspace this crate lives in.
    fn repo_file(relative: &str) -> String {
        // Two levels up from `crates/ramjet-engine`.
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("..")
            .join("..")
            .join(relative);
        std::fs::read_to_string(&path)
            .unwrap_or_else(|error| panic!("{}: {error}", path.display()))
    }

    /// The words of a cell, normalised for comparison.
    ///
    /// Lowercased, stripped of the punctuation and markdown a sentence wraps
    /// around a word, and short words dropped. Four characters is the whole of
    /// the noise filter and it is enough, because the words that collide
    /// between a feature name and a gap description are connectives — "and" is
    /// three. Punctuation is trimmed from the ends only, so `backend-protocol`
    /// stays one token and a bare "protocol" somewhere else cannot match it.
    fn words(cell: &str) -> impl Iterator<Item = String> + '_ {
        cell.split_whitespace()
            .map(|word| {
                word.trim_matches(|c: char| !c.is_ascii_alphanumeric() && c != '/')
                    .to_ascii_lowercase()
            })
            .filter(|word| word.len() >= 4)
    }

    /// The words [`V1_LIMITS`] uses to name the gaps, and only those.
    ///
    /// The bullets, not the whole string. The preamble says the engine serves
    /// HTTP/1.1 *with TLS*, and reading "TLS" out of that would let a table go
    /// on claiming TLS is missing — which is one of the four rows that actually
    /// drifted.
    fn gap_words() -> std::collections::HashSet<String> {
        V1_LIMITS
            .lines()
            .filter_map(|line| line.trim_start().strip_prefix("- "))
            .flat_map(words)
            .collect()
    }

    /// Whether this cell says the uring engine cannot do the thing.
    fn denies(cell: &str) -> bool {
        let cell = cell.to_ascii_lowercase();
        cell == "no"
            || cell.starts_with("no ")
            || cell.starts_with("no;")
            || cell.starts_with("no,")
            || cell.contains("502")
            || cell.contains("refused")
    }

    /// Every row of the `--engine uring` comparison table: what the row is
    /// about, and what the uring column claims about it.
    fn engine_table_rows(markdown: &str) -> Vec<(String, String)> {
        let mut rows = Vec::new();
        let mut inside = false;
        for line in markdown.lines() {
            let line = line.trim();
            if !line.starts_with('|') {
                // A table ends at the first line that is not one of its rows.
                inside = false;
                continue;
            }
            let cells: Vec<&str> = line.trim_matches('|').split('|').map(str::trim).collect();
            if cells.iter().any(|cell| cell.contains("--engine uring")) {
                inside = true;
                continue;
            }
            if !inside {
                continue;
            }
            // The `|---|---|` rule under the header.
            let rule = cells
                .iter()
                .all(|cell| !cell.is_empty() && cell.chars().all(|c| c == '-' || c == ':'));
            if rule {
                continue;
            }
            if let (Some(feature), Some(claim)) = (cells.first(), cells.last()) {
                rows.push(((*feature).to_owned(), (*claim).to_owned()));
            }
        }
        rows
    }

    #[test]
    fn the_parity_tables_do_not_claim_gaps_that_have_closed() {
        // The guard below watches one string. This one watches the two markdown
        // tables that say the same thing to a reader who has not run the binary
        // yet — and those drifted, four rows at a time, precisely because
        // nothing watched them: TLS, upgrades, the PROXY protocol and
        // Kubernetes mode were all still printed as refusals for releases after
        // the uring engine grew them. A gap list is read by somebody deciding
        // whether they can deploy this, so an entry that is false costs them
        // the feature.
        //
        // The rule is one-directional, deliberately. A row may describe
        // something `V1_LIMITS` never mentions, because `V1_LIMITS` lists gaps
        // rather than features. What it may not do is claim a gap that
        // `V1_LIMITS` does not.
        let gaps = gap_words();
        let mut stale = Vec::new();

        for file in PARITY_TABLES {
            let rows = engine_table_rows(&repo_file(file));
            assert!(
                !rows.is_empty(),
                "{file}: no `--engine uring` comparison table was found. If the \
                 table moved or its header was reworded, this guard stopped \
                 guarding — which is how the rows drifted in the first place."
            );
            for (feature, claim) in rows {
                if denies(&claim) && !words(&feature).any(|word| gaps.contains(&word)) {
                    stale.push(format!("{file}: {feature:?} claims {claim:?}"));
                }
            }
        }

        assert!(
            stale.is_empty(),
            "these rows claim the uring engine cannot do something V1_LIMITS \
             does not list as a gap. Either the gap closed and the row is \
             stale, or the gap is real and the startup banner is what is \
             wrong:\n{}",
            stale.join("\n")
        );
    }

    /// Sentences about this engine that were true when written and are false
    /// now, with what closed each one.
    ///
    /// Literal strings, and that is the point. The table guard above is
    /// structural and catches a *row* that goes stale; every one of these was
    /// prose, which is where the drift actually kept happening — four
    /// paragraphs across four files, each written when it was correct, each
    /// surviving the release that closed it, and each found later by somebody
    /// reading the documentation rather than by a test.
    ///
    /// A phrase belongs here once it is unambiguously false. "no TLS" alone
    /// does not qualify: `no TLS to the upstream` is a real and current
    /// limitation about a different subject, and a guard that fired on it would
    /// be deleted rather than fixed.
    const RETIRED_CLAIMS: &[(&str, &str)] = &[
        ("does not terminate TLS", "447d5a1 terminated TLS on the reactor"),
        ("refuses `--proxy-protocol`", "6a729b1 read the PROXY protocol on it"),
        (
            "PROXY protocol is hyper-engine only",
            "6a729b1 read the PROXY protocol on it",
        ),
        (
            "no TLS, no HTTP/2, no protocol upgrades",
            "447d5a1 for TLS, ed2af3f for upgrades, 68af340 for Kubernetes mode",
        ),
        ("no; static routes only", "68af340 added Kubernetes mode"),
        // "engine does not drain" rather than "does not drain": an upgraded
        // tunnel genuinely does not drain, and that sentence is one somebody
        // should still be able to write.
        ("engine does not drain", "the drain landed in this phase"),
        (
            "engine stops accepting and closes",
            "the drain landed in this phase",
        ),
    ];

    /// Every markdown file this repository documents itself with.
    fn markdown_files() -> Vec<std::path::PathBuf> {
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("..")
            .join("..");
        let mut found = vec![root.join("README.md"), root.join("ARCHITECTURE.md")];
        let mut pending = vec![root.join("docs").join("src")];
        while let Some(dir) = pending.pop() {
            let entries = std::fs::read_dir(&dir)
                .unwrap_or_else(|error| panic!("{}: {error}", dir.display()));
            for entry in entries {
                let path = entry.expect("a readable directory entry").path();
                if path.is_dir() {
                    pending.push(path);
                } else if path.extension().is_some_and(|ext| ext == "md") {
                    found.push(path);
                }
            }
        }
        found
    }

    #[test]
    fn no_document_repeats_a_claim_that_stopped_being_true() {
        // The same spirit as the banner guard below, widened to the files an
        // operator actually reads. Every entry in the list was found by hand,
        // one release too late; the point of writing them down is that the next
        // one is found by `cargo test` instead.
        let mut stale = Vec::new();
        for path in markdown_files() {
            let text = std::fs::read_to_string(&path)
                .unwrap_or_else(|error| panic!("{}: {error}", path.display()));
            for (claim, closed_by) in RETIRED_CLAIMS {
                if text.contains(claim) {
                    let name = path.file_name().unwrap_or(path.as_os_str());
                    stale.push(format!("{:?} says {claim:?} — {closed_by}", name));
                }
            }
        }
        assert!(
            stale.is_empty(),
            "documentation still carries a claim about the uring engine that \
             stopped being true:\n{}",
            stale.join("\n")
        );
    }

    #[test]
    fn the_parity_guard_catches_the_rows_that_actually_went_stale() {
        // A guard that has never failed is not yet a guard. This is the table
        // as it really stood — four rows describing an engine that had grown
        // TLS, upgrades, the PROXY protocol and Kubernetes mode releases
        // earlier — and it pins both halves of the rule at once: those four are
        // caught, and the two rows naming real gaps are left alone.
        //
        // Written as a fixture rather than by editing the repository's own
        // README, so the proof lives with the guard instead of in somebody's
        // memory of having tried it once.
        let drifted = "\
| | `--engine hyper` (default) | `--engine uring` |
|---|---|---|
| Runtime | hyper on tokio | the `ramjet` reactor: io_uring on Linux |
| HTTP/1.1 plaintext | yes | yes |
| TLS termination | yes | no (502) |
| HTTP/2 downstream | h2 downstream | no (502) |
| gRPC and HTTP/2 upstreams | yes, via `backend-protocol: GRPC` | no (502) |
| WebSocket and upgrades | yes | no (502) |
| HTTP/3 over QUIC (`--http3`) | experimental, off by default | no; refused at startup |
| PROXY protocol (`--proxy-protocol`) | v1 and v2 | no; refused at startup |
| Kubernetes mode | yes | no; static routes only |
| Status | measured against nginx | experimental |
";
        let gaps = gap_words();
        let caught: Vec<String> = engine_table_rows(drifted)
            .into_iter()
            .filter(|(feature, claim)| {
                denies(claim) && !words(feature).any(|word| gaps.contains(&word))
            })
            .map(|(feature, _)| feature)
            .collect();

        assert_eq!(
            caught,
            [
                "TLS termination",
                "WebSocket and upgrades",
                "PROXY protocol (`--proxy-protocol`)",
                "Kubernetes mode",
            ],
            "the guard has to catch every row whose gap closed, and leave the \
             rows naming real gaps — HTTP/2, gRPC, HTTP/3 — standing"
        );
    }

    #[test]
    fn the_limit_list_does_not_claim_gaps_that_have_closed() {
        // The failure this guards against is not a wrong list; it is a list
        // that keeps telling an operator to use --engine hyper for something
        // this engine has done for a release.
        for closed in [
            "no TLS",
            "plaintext only",
            "static routes only",
            "Kubernetes mode",
            "upgrades",
        ] {
            assert!(
                !V1_LIMITS.contains(closed),
                "{closed:?} is still listed as a limit after it was implemented"
            );
        }
    }
}
