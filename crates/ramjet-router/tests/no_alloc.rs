//! Proves the claim that `match_request` does not allocate.
//!
//! A comment saying "zero allocation" rots the moment someone adds a
//! `to_lowercase()` to fix a bug. This installs a counting global allocator and
//! fails the build if a match touches the heap even once, across every shape of
//! input the matcher handles: exact hosts, wildcards, misses, regex rules, and
//! the mixed-case fold path that is the most likely place for a `String` to
//! sneak back in.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

use ramjet_router::{
    CanaryRules, CertifiedKeyHandle, Endpoint, LbPolicy, PathType, RouteTable, RouteTableBuilder,
};
use std::sync::Arc;

// The counters are thread-local, not global. `cargo test` runs the tests in
// this binary concurrently, and a shared counter would attribute one test's
// allocations to whichever other test happened to be measuring at the time --
// which is exactly the false positive this file would otherwise produce.
// `const`-initialised `Cell`s so that touching the TLS cannot itself allocate
// and re-enter the allocator.
thread_local! {
    static COUNTING: Cell<bool> = const { Cell::new(false) };
    static ALLOCATIONS: Cell<usize> = const { Cell::new(0) };
}

fn record() {
    // `try_with` because TLS is unavailable during thread teardown, and an
    // allocation there must not panic inside the allocator.
    let counting = COUNTING.try_with(Cell::get).unwrap_or(false);
    if counting {
        let _ = ALLOCATIONS.try_with(|n| n.set(n.get() + 1));
    }
}

struct Counter;

// SAFETY: every method forwards to `System`, which is a valid allocator, and
// returns its pointer unchanged. `record` only touches `Cell`s in const-init
// TLS, so it cannot allocate and cannot re-enter.
unsafe impl GlobalAlloc for Counter {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        record();
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        record();
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static ALLOC: Counter = Counter;

/// Runs `f` with allocation counting on and returns how many allocations it
/// made on this thread.
fn count_allocations(f: impl FnOnce()) -> usize {
    ALLOCATIONS.with(|n| n.set(0));
    COUNTING.with(|c| c.set(true));
    f();
    COUNTING.with(|c| c.set(false));
    ALLOCATIONS.with(Cell::get)
}

fn table() -> RouteTable {
    let mut b = RouteTableBuilder::new();
    for name in ["api", "web", "canary", "fallback"] {
        b.backend(
            name,
            LbPolicy::RoundRobin,
            vec![Endpoint::new("10.0.0.1:8080".parse().expect("valid"))],
        )
        .expect("registers");
    }
    b.default_backend("fallback");

    b.route(Some("example.com"), "/exact", PathType::Exact, "api")
        .expect("drafts");
    b.route(Some("example.com"), "/api/v1", PathType::Prefix, "api")
        .expect("drafts");
    b.route(Some("example.com"), "/", PathType::Prefix, "web")
        .expect("drafts");
    b.route(
        Some("example.com"),
        "/media/.*[.]mp4",
        PathType::ImplementationSpecific,
        "web",
    )
    .expect("drafts");
    b.route(Some("*.wild.example.com"), "/", PathType::Prefix, "web")
        .expect("drafts");
    b.route(None, "/", PathType::Prefix, "web").expect("drafts");

    b.canary_route(
        Some("canary.example.com"),
        "/",
        PathType::Prefix,
        "api",
        &CanaryRules {
            backend: "canary",
            header: Some("x-canary"),
            header_pattern: Some("beta.*"),
            weight: 25,
            ..Default::default()
        },
    )
    .expect("drafts");

    b.certificate(
        "example.com",
        Arc::new(CertifiedKeyHandle::new(1)),
    )
    .expect("valid host");
    b.certificate(
        "*.wild.example.com",
        Arc::new(CertifiedKeyHandle::new(2)),
    )
    .expect("valid host");

    b.build().expect("builds")
}

#[test]
fn match_request_never_allocates() {
    let table = table();

    // Warm up outside the counted region so any lazy regex initialisation the
    // `regex` crate does on first use is not attributed to the matcher.
    for (host, path) in CASES {
        let _ = table.match_request(host, path);
    }

    for (host, path) in CASES {
        let n = count_allocations(|| {
            let hit = table.match_request(host, path);
            std::hint::black_box(&hit);
        });
        assert_eq!(n, 0, "match_request({host:?}, {path:?}) allocated {n} times");
    }
}

/// Every distinct code path through the matcher.
const CASES: &[(&str, &str)] = &[
    // Exact host, exact rule.
    ("example.com", "/exact"),
    // Exact host, prefix rule.
    ("example.com", "/api/v1/users"),
    // Exact host, root prefix (full scan of the prefix list).
    ("example.com", "/whatever"),
    // Exact host, regex rule (scans past everything else).
    ("example.com", "/media/clip.mp4"),
    // The fold path: uppercase forces the stack-buffer copy.
    ("EXAMPLE.COM", "/api/v1"),
    // Port and root dot trimming.
    ("example.com.:8443", "/api/v1"),
    // Wildcard host.
    ("sub.wild.example.com", "/anything"),
    // Catch-all (no matching host entry, but a hostless rule exists).
    ("unclaimed.example.net", "/anything"),
    // Malformed host, straight to the default backend.
    ("", "/"),
    (":8080", "/"),
    // Canary route, so the canary pointer is followed.
    ("canary.example.com", "/checkout"),
];

#[test]
fn canary_decide_never_allocates() {
    let table = table();
    let hit = table
        .match_request("canary.example.com", "/checkout")
        .expect("matches");
    let canary = hit.canary().expect("has a canary");

    // Warm the regex.
    let _ = canary.decide(Some("beta-1"), None, 0);

    let n = count_allocations(|| {
        std::hint::black_box(canary.decide(Some("beta-1"), None, 0));
        std::hint::black_box(canary.decide(Some("other"), None, 10));
        std::hint::black_box(canary.decide(None, Some("always"), 99));
    });
    assert_eq!(n, 0, "canary decision allocated {n} times");
}

#[test]
fn sni_resolution_never_allocates() {
    let table = table();
    let tls = table.tls();
    let _ = tls.resolve("example.com");

    for name in [
        "example.com",
        "EXAMPLE.COM",
        "example.com:443",
        "sub.wild.example.com",
        "unknown.test",
    ] {
        let n = count_allocations(|| {
            std::hint::black_box(tls.resolve(name));
        });
        assert_eq!(n, 0, "resolve({name:?}) allocated {n} times");
    }
}

#[test]
fn endpoint_selection_never_allocates() {
    let table = table();
    let hit = table.match_request("example.com", "/exact").expect("matches");
    let stats = table.stats();
    let _ = ramjet_router::select_endpoint(hit.backend(), stats, 0);

    let n = count_allocations(|| {
        let picked = ramjet_router::select_endpoint(hit.backend(), stats, 7);
        std::hint::black_box(&picked);
    });
    assert_eq!(n, 0, "select_endpoint allocated {n} times");
}
