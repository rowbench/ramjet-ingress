//! Match latency on a table the size of a large cluster: 1,000 hosts and
//! 10,000 routes.
//!
//! The number that matters is `deep_prefix_hit`: an exact host, then a linear
//! scan past the host's Exact rules and down its Prefix rules to a four-segment
//! path. That is what a normal request costs. The other cases exist to show
//! where the remaining time goes — the wildcard case pays a second hash, and
//! the mixed-case host pays for the stack-buffer fold.

use std::hint::black_box;

use criterion::{criterion_group, criterion_main, Criterion};
use ramjet_router::{Endpoint, LbPolicy, PathType, RouteTable, RouteTableBuilder};

const HOSTS: usize = 1_000;

/// Ten routes per host, mirroring the shape a real Ingress produces: a couple
/// of exact paths, a spread of prefixes at varying depth, and one regex.
fn build_table() -> RouteTable {
    let mut b = RouteTableBuilder::new();

    b.backend(
        "default",
        LbPolicy::RoundRobin,
        vec![Endpoint::new(
            "10.0.0.1:8080".parse().expect("valid address"),
        )],
    )
    .expect("registers");
    b.default_backend("default");

    for i in 0..HOSTS {
        let host = format!("host{i}.example.com");
        let backend = format!("svc{i}");
        b.backend(
            &backend,
            LbPolicy::RoundRobin,
            vec![
                Endpoint::new("10.0.1.1:8080".parse().expect("valid address")),
                Endpoint::new("10.0.1.2:8080".parse().expect("valid address")),
            ],
        )
        .expect("registers");

        for path in [
            "/exact/login",
            "/exact/logout",
            "/",
            "/api",
            "/api/v1",
            "/api/v1/users",
            "/static",
            "/static/img",
            "/admin",
        ] {
            let kind = if path.starts_with("/exact/") {
                PathType::Exact
            } else {
                PathType::Prefix
            };
            b.route(Some(&host), path, kind, &backend).expect("drafts");
        }
        b.route(
            Some(&host),
            "/media/.*[.]mp4",
            PathType::ImplementationSpecific,
            &backend,
        )
        .expect("drafts");
    }

    // One wildcard host, to time the extra hash a wildcard costs.
    b.backend(
        "wild",
        LbPolicy::RoundRobin,
        vec![Endpoint::new(
            "10.0.2.1:8080".parse().expect("valid address"),
        )],
    )
    .expect("registers");
    b.route(Some("*.wild.example.com"), "/", PathType::Prefix, "wild")
        .expect("drafts");

    b.build().expect("builds")
}

fn bench_match(c: &mut Criterion) {
    let table = build_table();
    assert_eq!(table.route_count(), HOSTS * 10 + 1);
    assert_eq!(table.host_count(), HOSTS);

    let mut group = c.benchmark_group("match_request");

    // The headline: a normal request against a deep prefix.
    group.bench_function("deep_prefix_hit", |bench| {
        bench.iter(|| {
            black_box(
                table
                    .match_request(black_box("host500.example.com"), black_box("/api/v1/users/42")),
            )
        })
    });

    // Exact rules are scanned first, so this is the cheapest hit.
    group.bench_function("exact_hit", |bench| {
        bench.iter(|| {
            black_box(table.match_request(black_box("host500.example.com"), black_box("/exact/login")))
        })
    });

    // Root prefix: the last Prefix rule in the scan, so this walks the whole
    // prefix list before matching.
    group.bench_function("root_prefix_hit", |bench| {
        bench.iter(|| {
            black_box(table.match_request(black_box("host500.example.com"), black_box("/nothing/here")))
        })
    });

    // Unknown host: one failed hash, one failed wildcard hash, default backend.
    group.bench_function("host_miss_default_backend", |bench| {
        bench.iter(|| {
            black_box(table.match_request(black_box("nobody.example.net"), black_box("/api/v1")))
        })
    });

    // Wildcard: a failed exact hash plus a successful parent-domain hash.
    group.bench_function("wildcard_hit", |bench| {
        bench.iter(|| {
            black_box(table.match_request(black_box("anything.wild.example.com"), black_box("/x")))
        })
    });

    // Mixed case and a port: the only path that copies, into a stack buffer.
    group.bench_function("uppercase_host_fold", |bench| {
        bench.iter(|| {
            black_box(
                table.match_request(black_box("Host500.Example.COM:8443"), black_box("/api/v1/users/42")),
            )
        })
    });

    // The regex rules sort last, so this pays the full scan plus a regex.
    group.bench_function("regex_hit", |bench| {
        bench.iter(|| {
            black_box(
                table.match_request(black_box("host500.example.com"), black_box("/media/clip.mp4")),
            )
        })
    });

    group.finish();
}

/// What a request pays to be counted against its route.
///
/// The whole per-request cost of the per-route statistics, on the same table
/// the matcher is benchmarked against: resolve the matched rule's counter block
/// out of the slab, pick this runtime's shard, and record a response and an
/// upstream latency. Everything is a relaxed add to one 128-byte-aligned block,
/// which is why this belongs next to a 25 ns matcher rather than in a footnote.
fn bench_route_stats(c: &mut Criterion) {
    let table = build_table();
    let (_, rule) = table
        .routes()
        .find(|(_, rule)| rule.path() == "/api/v1/users")
        .expect("the table has that route");
    let index = rule.stats_index();

    let mut group = c.benchmark_group("route_stats");

    group.bench_function("record_request", |bench| {
        bench.iter(|| {
            if let Some(slot) = table.route_stats().slot(black_box(index)) {
                let counters = slot.shard(black_box(0));
                counters.record_response(black_box(200));
                counters.record_upstream_latency(black_box(std::time::Duration::from_millis(3)));
            }
        })
    });

    // The comparison that makes the number above mean something: matching the
    // request is the work, counting it is the rounding error.
    group.bench_function("match_then_record", |bench| {
        bench.iter(|| {
            let matched = black_box(
                table.match_request(black_box("host500.example.com"), black_box("/api/v1/users/42")),
            );
            if let Some(rule) = matched.as_ref().and_then(|m| m.rule()) {
                if let Some(slot) = table.route_stats().slot(rule.stats_index()) {
                    slot.shard(0).record_response(200);
                }
            }
        })
    });

    group.finish();
}

criterion_group!(benches, bench_match, bench_route_stats);
criterion_main!(benches);
