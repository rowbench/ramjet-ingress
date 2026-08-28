//! The two engines' `/metrics` output, compared byte for byte.
//!
//! `EngineMetrics::render_prometheus` is a second copy of a formatter that
//! already exists on `ramjet_proxy::Metrics`, and the module docs claim it is
//! byte-identical. Duplicating a formatter is a real risk — the two drift, and
//! `/metrics` quietly changes shape when somebody passes `--engine uring`, at
//! which point every dashboard and alert built against one of them is wrong
//! about the other.
//!
//! So this drives both counter sets through the same events and asserts the two
//! strings are *equal*. Not "both contain the series", not "both parse":
//! equal. If anyone edits either formatter, this fails.
//!
//! It is a unit test wearing an integration test's clothes — no sockets, no
//! engines, just counters — because the thing under test is a `String`.

use std::sync::Arc;
use std::time::Duration;

use ramjet_engine::metrics::EngineMetrics;
use ramjet_proxy::metrics::ConnectionGuard;
use ramjet_proxy::Metrics;

/// Every event both counter sets know how to record, applied to both.
///
/// Deliberately not a round number of anything: a formatter bug that only shows
/// up at a bucket boundary, or when a counter is zero, or when one is much
/// larger than another, should have somewhere to show up.
#[must_use = "the connection guards have to outlive the render, or the gauge moves"]
fn drive(engine: &EngineMetrics, hyper: &Arc<Metrics>) -> Vec<ConnectionGuard> {
    for status in [200u16, 201, 301, 404, 418, 500, 502, 503, 100, 999] {
        engine.core(0).response(status);
        hyper.record_response(status);
    }
    // A second pass on one core, so the merge across cores is exercised rather
    // than a single block being read back.
    for status in [200u16, 200, 404] {
        engine.core(1).response(status);
        hyper.record_response(status);
    }

    // Latencies either side of several bucket bounds, plus one past the last —
    // which increments no bucket and survives only in `+Inf` and `_count`.
    for micros in [
        500u64, 1_000, 1_001, 2_500, 4_999, 10_000, 99_999, 250_000, 999_999, 5_000_000,
        30_000_000,
    ] {
        engine
            .core(0)
            .upstream_latency(Duration::from_micros(micros));
        hyper.record_upstream_latency(Duration::from_micros(micros));
    }

    // Seven connections opened, three of them closed. The hyper side counts a
    // close by dropping the guard `connection_opened` hands back, so the two
    // are driven with the same arithmetic through different shapes.
    let mut held = Vec::new();
    for _ in 0..7 {
        engine.core(0).connection_opened();
        held.push(hyper.connection_opened());
    }
    for _ in 0..3 {
        engine.core(0).connection_closed();
        held.pop();
    }


    for _ in 0..4 {
        engine.core(0).connect_failure();
        hyper.record_connect_failure();
    }
    for _ in 0..11 {
        engine.core(1).retry();
        hyper.record_retry();
    }
    for _ in 0..2 {
        engine.core(0).timeout();
        hyper.record_upstream_timeout();
    }
    for _ in 0..23 {
        engine.core(1).route_miss();
        hyper.record_route_miss();
    }
    for _ in 0..5 {
        engine.core(0).tls_handshake();
        hyper.record_tls_handshake();
    }
    engine.core(0).tls_handshake_failure();
    hyper.record_tls_handshake_failure();

    held
}

#[test]
fn the_two_engines_render_the_same_exposition() {
    let engine = EngineMetrics::new(2);
    let hyper = Arc::new(Metrics::new());
    // Held until both renders are done: dropping a guard decrements the gauge,
    // and a gauge that moved between the two renders would fail this test for
    // the wrong reason.
    let _open = drive(&engine, &hyper);

    let from_engine = engine.render_prometheus(42, false);
    let from_hyper = hyper.render_prometheus(42, false);

    // Line by line first: a whole-string diff on two 60-line expositions is
    // unreadable, and the first differing line is the answer.
    for (index, (left, right)) in from_engine.lines().zip(from_hyper.lines()).enumerate() {
        assert_eq!(
            left,
            right,
            "line {} of the exposition differs\n  uring: {left}\n  hyper: {right}",
            index + 1
        );
    }
    assert_eq!(
        from_engine.lines().count(),
        from_hyper.lines().count(),
        "one engine emits more series than the other"
    );
    assert_eq!(from_engine, from_hyper);
}

#[test]
fn the_generation_and_pin_arguments_are_rendered_the_same_way() {
    let engine = EngineMetrics::new(1);
    let hyper = Metrics::new();

    for (generation, pinned) in [(0u64, false), (1, true), (u64::MAX, false), (7, true)] {
        assert_eq!(
            engine.render_prometheus(generation, pinned),
            hyper.render_prometheus(generation, pinned),
            "generation {generation}, pinned {pinned}"
        );
    }
}

#[test]
fn an_untouched_pair_renders_the_same_zeros() {
    // The state a replica is in for its first fifteen seconds, and the one a
    // dashboard is built against before any traffic arrives.
    assert_eq!(
        EngineMetrics::new(4).render_prometheus(0, false),
        Metrics::new().render_prometheus(0, false)
    );
}

#[test]
fn which_engine_rendered_it_is_not_readable_from_the_series_set() {
    // Two series *name* the engines — `ramjet_dispatch_uring_total` and
    // `ramjet_dispatch_hyper_total`, which describe the split between them and
    // could not be named anything else. What must not happen is a series
    // appearing on one engine and not the other, because that is what makes a
    // dashboard built against one wrong about the other.
    let names = |text: &str| -> Vec<String> {
        text.lines()
            .filter(|line| !line.starts_with('#'))
            .filter_map(|line| line.split_whitespace().next().map(str::to_owned))
            .collect()
    };

    let from_engine = EngineMetrics::new(1).render_prometheus(1, false);
    let from_hyper = Metrics::new().render_prometheus(1, false);
    assert_eq!(names(&from_engine), names(&from_hyper));

    // And nothing in the *help* text gives it away either, which is where an
    // engine-specific explanation would most easily creep in.
    for text in [&from_engine, &from_hyper] {
        assert!(
            !text.contains("io_uring") && !text.contains("reactor") && !text.contains("tokio"),
            "the exposition explains itself in terms of one engine:\n{text}"
        );
    }
}
