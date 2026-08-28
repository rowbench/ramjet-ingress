//! The binary itself, run the way CI would run it.
//!
//! Everything else in this crate's tests calls functions. These spawn the
//! compiled executable against a mock admin port and read its stdout, stderr
//! and exit status, which is the only way to catch the failures that live
//! between `main` and the library: an argument that is parsed but never used, a
//! mode that prints to the wrong stream, an exit code that is always zero.

mod common;

use std::process::{Command, Output};

use common::{generations_json, MockAdmin};

/// Runs the binary with the given arguments and waits for it.
///
/// `CARGO_BIN_EXE_<name>` is set by cargo for integration tests and points at
/// the binary this same `cargo test` invocation just built, so there is no
/// chance of testing a stale copy from a previous run.
fn run(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_ramjet-top"))
        .args(args)
        .output()
        .expect("the binary runs")
}

fn stdout(output: &Output) -> String {
    String::from_utf8_lossy(&output.stdout).into_owned()
}

fn stderr(output: &Output) -> String {
    String::from_utf8_lossy(&output.stderr).into_owned()
}

#[tokio::test]
async fn once_prints_the_routes_table_and_the_timeline_and_exits_zero() {
    let mock = MockAdmin::start().await;
    let url = mock.url();

    let output = tokio::task::spawn_blocking(move || run(&["--once", &url]))
        .await
        .expect("the child ran");

    assert!(
        output.status.success(),
        "exit {:?}, stderr: {}",
        output.status.code(),
        stderr(&output)
    );

    let text = stdout(&output);

    // The header.
    assert!(text.contains("ramjet-top  http://127.0.0.1:"), "{text}");
    assert!(text.contains("generation 42 serving"), "{text}");
    assert!(text.contains("2 routes · 2 hosts · 1 certs"), "{text}");
    assert!(text.contains("connections 37"), "{text}");

    // The routes table.
    assert!(text.contains("HOST"), "{text}");
    assert!(text.contains("BACKEND"), "{text}");
    assert!(text.contains("api.example.com"), "{text}");
    assert!(text.contains("api-v2"), "{text}");
    assert!(text.contains("10000"), "{text}");
    assert!(text.contains("default-http-backend"), "{text}");
    assert!(text.contains("10%->api-v3"), "the canary split: {text}");

    // The timeline.
    assert!(text.contains("generations (newest first)"), "{text}");
    assert!(text.contains("1 route added, 1 backend changed"), "{text}");
    assert!(text.contains("initial table"), "{text}");
    assert!(text.contains("* serving"), "{text}");

    assert!(stderr(&output).is_empty(), "nothing on stderr on success");
}

#[tokio::test]
async fn once_output_is_byte_for_byte_repeatable() {
    // The `--once` contract is that it can be diffed. Ages are the one thing
    // that move, and at this resolution two runs a moment apart agree.
    let mock = MockAdmin::start().await;
    let url = mock.url();

    let first = tokio::task::spawn_blocking({
        let url = url.clone();
        move || run(&["--once", &url])
    })
    .await
    .expect("the child ran");
    let second = tokio::task::spawn_blocking(move || run(&["--once", &url]))
        .await
        .expect("the child ran");

    assert_eq!(stdout(&first), stdout(&second));
}

#[tokio::test]
async fn a_pinned_daemon_says_so_loudly_in_once_mode() {
    let mock = MockAdmin::start().await;
    mock.set(|b| b.generations = generations_json(43, Some(41), 0));
    let url = mock.url();

    let output = tokio::task::spawn_blocking(move || run(&["--once", &url]))
        .await
        .expect("the child ran");

    let text = stdout(&output);
    assert!(output.status.success());
    assert!(text.contains("PINNED to generation 41"), "{text}");
    assert!(text.contains("NOT being served"), "{text}");
}

#[tokio::test]
async fn json_dumps_the_merged_snapshot_and_implies_once() {
    let mock = MockAdmin::start().await;
    let url = mock.url();

    // No `--once`: `--json` has to imply it, or this would open a TUI and hang.
    let output = tokio::task::spawn_blocking(move || run(&["--json", &url]))
        .await
        .expect("the child ran");

    assert!(output.status.success(), "stderr: {}", stderr(&output));

    let value: serde_json::Value =
        serde_json::from_str(&stdout(&output)).expect("valid JSON on stdout");

    assert_eq!(value["generations"]["serving"], 42);
    assert_eq!(value["generations"]["pinned"], serde_json::Value::Null);
    assert_eq!(value["routes"]["generation"], 42);
    assert_eq!(value["routes"]["routes"][0]["host"], "api.example.com");
    assert_eq!(value["routes"]["routes"][0]["path_type"], "Prefix");
    assert_eq!(value["routes"]["routes"][0]["endpoints"], 4);
    assert_eq!(
        value["routes"]["routes"][0]["canary"]["backend"],
        "api-v3"
    );
    assert_eq!(value["metrics"]["requests_total"], 10_007);
    assert_eq!(value["metrics"]["active_connections"], 37);
    assert_eq!(
        value["generations"]["generations"][0]["diff"]["backends_changed"][0],
        "api.example.com/v1 -> api-v2"
    );
}

#[tokio::test]
async fn the_url_can_be_a_flag_as_well_as_positional() {
    let mock = MockAdmin::start().await;
    let url = mock.url();

    let output = tokio::task::spawn_blocking(move || run(&["--once", "--url", &url]))
        .await
        .expect("the child ran");
    assert!(output.status.success(), "stderr: {}", stderr(&output));
    assert!(stdout(&output).contains("api.example.com"));
}

#[tokio::test]
async fn a_bare_host_and_port_is_accepted() {
    let mock = MockAdmin::start().await;
    let bare = mock.addr.to_string();

    let output = tokio::task::spawn_blocking(move || run(&["--once", &bare]))
        .await
        .expect("the child ran");
    assert!(output.status.success(), "stderr: {}", stderr(&output));
    assert!(stdout(&output).contains("api.example.com"));
}

#[tokio::test]
async fn an_unreachable_daemon_exits_nonzero_and_explains_itself_on_stderr() {
    let output = tokio::task::spawn_blocking(|| {
        run(&["--once", "--timeout", "500ms", "http://127.0.0.1:1"])
    })
    .await
    .expect("the child ran");

    assert!(!output.status.success(), "an unreachable daemon is a failure");
    assert_eq!(output.status.code(), Some(1), "runtime failure, not usage");
    assert!(
        stdout(&output).trim().is_empty(),
        "nothing on stdout, so a pipeline does not consume half a table"
    );
    let err = stderr(&output);
    assert!(err.starts_with("ramjet-top:"), "{err}");
    assert!(err.contains("cannot reach") || err.contains("did not answer"), "{err}");
}

#[tokio::test]
async fn an_unhealthy_daemon_reports_the_status_it_returned() {
    let mock = MockAdmin::start().await;
    mock.set(|b| b.failing = true);
    let url = mock.url();

    let output = tokio::task::spawn_blocking(move || run(&["--once", &url]))
        .await
        .expect("the child ran");

    assert_eq!(output.status.code(), Some(1));
    assert!(stderr(&output).contains("503"), "{}", stderr(&output));
}

#[test]
fn help_lists_the_options_and_exits_zero() {
    for flag in ["-h", "--help"] {
        let output = run(&[flag]);
        assert!(output.status.success(), "{flag}");
        let text = stdout(&output);
        assert!(text.contains("USAGE:"), "{text}");
        assert!(text.contains("--once"), "{text}");
        assert!(text.contains("--read-only"), "{text}");
        assert!(text.contains("KEYS"), "{text}");
    }
}

#[test]
fn version_prints_the_crate_version() {
    let output = run(&["--version"]);
    assert!(output.status.success());
    assert!(
        stdout(&output).starts_with("ramjet-top "),
        "{}",
        stdout(&output)
    );
}

#[test]
fn a_bad_option_exits_two_and_says_what_it_was() {
    let output = run(&["--nonsense"]);
    assert_eq!(
        output.status.code(),
        Some(2),
        "usage errors are distinguishable from runtime failures"
    );
    let err = stderr(&output);
    assert!(err.contains("--nonsense"), "{err}");
    assert!(err.contains("--help"), "{err}");
}

#[test]
fn a_bad_interval_exits_two() {
    let output = run(&["--once", "--interval", "1ms"]);
    assert_eq!(output.status.code(), Some(2));
    assert!(stderr(&output).contains("100ms"), "{}", stderr(&output));
}

#[test]
fn an_https_url_is_refused_before_any_connection_is_attempted() {
    let output = run(&["--once", "https://example.com:10254"]);
    assert_eq!(output.status.code(), Some(2));
    let err = stderr(&output);
    assert!(err.contains("http"), "{err}");
}
