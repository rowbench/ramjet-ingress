//! End-to-end Ingress semantics: host selection, path precedence, and canary
//! resolution through the public API.
//!
//! The unit tests in `src/path.rs` cover the matching primitives in isolation.
//! These check that a table built through [`RouteTableBuilder`] actually routes
//! the way the Kubernetes and ingress-nginx documentation says it should.

use std::sync::Arc;

use ramjet_router::{
    CanaryRules, CertifiedKeyHandle, Endpoint, HostMatch, LbPolicy, PathType, RouteTable,
    RouteTableBuilder, SharedRouteTable,
};

fn endpoint(port: u16) -> Endpoint {
    Endpoint::new(std::net::SocketAddr::from(([10, 0, 0, 1], port)))
}

/// Builds a table where each route points at a backend named after itself, so
/// a test can assert exactly which rule won.
fn table(routes: &[(Option<&str>, &str, PathType, &str)], default: Option<&str>) -> RouteTable {
    let mut b = RouteTableBuilder::new();
    let mut registered = std::collections::HashSet::new();

    for (_, _, _, backend) in routes {
        if registered.insert(*backend) {
            b.backend(backend, LbPolicy::RoundRobin, vec![endpoint(8080)])
                .expect("registers");
        }
    }
    if let Some(d) = default {
        if registered.insert(d) {
            b.backend(d, LbPolicy::RoundRobin, vec![endpoint(8080)])
                .expect("registers");
        }
        b.default_backend(d);
    }
    for (host, path, kind, backend) in routes {
        b.route(*host, path, *kind, backend).expect("drafts");
    }
    b.build().expect("builds")
}

fn hit(t: &RouteTable, host: &str, path: &str) -> Option<String> {
    t.match_request(host, path)
        .map(|m| m.backend().name().to_owned())
}

// ---------------------------------------------------------------- path types

#[test]
fn exact_beats_prefix_on_the_same_path() {
    let t = table(
        &[
            (Some("example.com"), "/foo", PathType::Prefix, "prefix"),
            (Some("example.com"), "/foo", PathType::Exact, "exact"),
        ],
        None,
    );
    assert_eq!(hit(&t, "example.com", "/foo").as_deref(), Some("exact"));
    // The Exact rule does not cover subpaths, so those fall to the Prefix rule.
    assert_eq!(
        hit(&t, "example.com", "/foo/bar").as_deref(),
        Some("prefix")
    );
    // ...nor a trailing slash.
    assert_eq!(hit(&t, "example.com", "/foo/").as_deref(), Some("prefix"));
}

#[test]
fn longest_prefix_wins_regardless_of_declaration_order() {
    // Declared shortest-first, so a naive first-match scan would pick "/".
    let t = table(
        &[
            (Some("example.com"), "/", PathType::Prefix, "root"),
            (Some("example.com"), "/a", PathType::Prefix, "a"),
            (Some("example.com"), "/a/b/c", PathType::Prefix, "abc"),
            (Some("example.com"), "/a/b", PathType::Prefix, "ab"),
        ],
        None,
    );
    assert_eq!(hit(&t, "example.com", "/a/b/c/d").as_deref(), Some("abc"));
    assert_eq!(hit(&t, "example.com", "/a/b/x").as_deref(), Some("ab"));
    assert_eq!(hit(&t, "example.com", "/a/x").as_deref(), Some("a"));
    assert_eq!(hit(&t, "example.com", "/x").as_deref(), Some("root"));
}

#[test]
fn the_foobar_trap() {
    let t = table(
        &[(Some("example.com"), "/foo", PathType::Prefix, "foo")],
        Some("fallback"),
    );
    assert_eq!(hit(&t, "example.com", "/foo").as_deref(), Some("foo"));
    assert_eq!(hit(&t, "example.com", "/foo/").as_deref(), Some("foo"));
    assert_eq!(hit(&t, "example.com", "/foo/bar").as_deref(), Some("foo"));
    assert_eq!(
        hit(&t, "example.com", "/foobar").as_deref(),
        Some("fallback"),
        "a string prefix that is not an element prefix must not match"
    );
}

#[test]
fn regex_sorts_after_prefix_but_still_matches() {
    let t = table(
        &[
            (Some("example.com"), "/", PathType::Prefix, "root"),
            (
                Some("example.com"),
                "/media/.*[.]mp4",
                PathType::ImplementationSpecific,
                "video",
            ),
        ],
        None,
    );
    // The root prefix sorts ahead of the regex, so it wins even here. This is
    // ingress-nginx's ordering: regexes are the last resort, not the first.
    assert_eq!(
        hit(&t, "example.com", "/media/clip.mp4").as_deref(),
        Some("root")
    );

    // With no competing prefix, the regex does the routing.
    let t = table(
        &[(
            Some("example.com"),
            "/media/.*[.]mp4",
            PathType::ImplementationSpecific,
            "video",
        )],
        Some("fallback"),
    );
    assert_eq!(
        hit(&t, "example.com", "/media/clip.mp4").as_deref(),
        Some("video")
    );
    assert_eq!(
        hit(&t, "example.com", "/media/clip.mov").as_deref(),
        Some("fallback")
    );
}

#[test]
fn regex_is_anchored_at_the_start() {
    let t = table(
        &[(
            Some("example.com"),
            "/media/.*",
            PathType::ImplementationSpecific,
            "video",
        )],
        Some("fallback"),
    );
    assert_eq!(
        hit(&t, "example.com", "/other/media/x").as_deref(),
        Some("fallback"),
        "an unanchored search would match this"
    );
}

#[test]
fn regex_is_case_insensitive_like_nginx() {
    let t = table(
        &[(
            Some("example.com"),
            "/Media/.*",
            PathType::ImplementationSpecific,
            "video",
        )],
        Some("fallback"),
    );
    // ingress-nginx emits `location ~*`, so case does not matter.
    assert_eq!(hit(&t, "example.com", "/media/x").as_deref(), Some("video"));
}

#[test]
fn regexes_keep_controller_order() {
    let t = table(
        &[
            (
                Some("example.com"),
                "/a.*",
                PathType::ImplementationSpecific,
                "first",
            ),
            (
                Some("example.com"),
                "/ab.*",
                PathType::ImplementationSpecific,
                "second",
            ),
        ],
        None,
    );
    assert_eq!(
        hit(&t, "example.com", "/abc").as_deref(),
        Some("first"),
        "both patterns match; the earlier rule wins"
    );
}

// ---------------------------------------------------------------- host rules

#[test]
fn exact_host_beats_wildcard() {
    let t = table(
        &[
            (Some("api.example.com"), "/", PathType::Prefix, "exact"),
            (Some("*.example.com"), "/", PathType::Prefix, "wild"),
        ],
        None,
    );
    assert_eq!(hit(&t, "api.example.com", "/").as_deref(), Some("exact"));
    assert_eq!(hit(&t, "web.example.com", "/").as_deref(), Some("wild"));
}

#[test]
fn wildcard_covers_exactly_one_label() {
    let t = table(
        &[(Some("*.example.com"), "/", PathType::Prefix, "wild")],
        Some("fallback"),
    );
    assert_eq!(hit(&t, "foo.example.com", "/").as_deref(), Some("wild"));
    assert_eq!(
        hit(&t, "foo.bar.example.com", "/").as_deref(),
        Some("fallback"),
        "*.example.com must not match two labels deep"
    );
    assert_eq!(
        hit(&t, "example.com", "/").as_deref(),
        Some("fallback"),
        "*.example.com must not match the apex"
    );
}

#[test]
fn host_selection_happens_before_path_matching() {
    // The exact host claims the request even though only the wildcard has a
    // rule for this path. nginx resolves the server first, then the location.
    let t = table(
        &[
            (Some("api.example.com"), "/only-here", PathType::Exact, "exact"),
            (Some("*.example.com"), "/", PathType::Prefix, "wild"),
        ],
        Some("fallback"),
    );
    assert_eq!(
        hit(&t, "api.example.com", "/elsewhere").as_deref(),
        Some("fallback"),
        "a claimed host must not fall back to the wildcard server"
    );
}

#[test]
fn host_is_normalized() {
    let t = table(
        &[(Some("Example.COM"), "/", PathType::Prefix, "api")],
        None,
    );
    for host in [
        "example.com",
        "EXAMPLE.COM",
        "Example.Com",
        "example.com:8443",
        "example.com.",
        "example.com.:8443",
    ] {
        assert_eq!(hit(&t, host, "/").as_deref(), Some("api"), "host {host:?}");
    }
}

#[test]
fn hostless_rule_serves_unclaimed_names() {
    let t = table(
        &[
            (Some("api.example.com"), "/", PathType::Prefix, "api"),
            (None, "/", PathType::Prefix, "catchall"),
        ],
        None,
    );
    assert_eq!(hit(&t, "api.example.com", "/").as_deref(), Some("api"));
    assert_eq!(hit(&t, "anything.test", "/").as_deref(), Some("catchall"));

    let m = t.match_request("anything.test", "/").expect("matches");
    assert_eq!(m.host_match(), HostMatch::CatchAll);
}

#[test]
fn malformed_host_is_still_served() {
    let t = table(&[(None, "/", PathType::Prefix, "catchall")], None);
    assert_eq!(hit(&t, "", "/").as_deref(), Some("catchall"));
    assert_eq!(hit(&t, ":8080", "/").as_deref(), Some("catchall"));
}

#[test]
fn default_backend_answers_when_nothing_matches() {
    let t = table(
        &[(Some("api.example.com"), "/api", PathType::Prefix, "api")],
        Some("fallback"),
    );
    let m = t.match_request("nobody.test", "/").expect("matches");
    assert_eq!(m.backend().name(), "fallback");
    assert_eq!(m.host_match(), HostMatch::DefaultBackend);
    assert!(m.rule().is_none(), "the default backend has no rule");
}

#[test]
fn no_default_backend_means_no_match() {
    let t = table(
        &[(Some("api.example.com"), "/api", PathType::Prefix, "api")],
        None,
    );
    assert!(t.match_request("nobody.test", "/").is_none());
    assert!(t.match_request("api.example.com", "/other").is_none());
}

#[test]
fn host_match_kind_is_reported() {
    let t = table(
        &[
            (Some("api.example.com"), "/", PathType::Prefix, "exact"),
            (Some("*.example.com"), "/", PathType::Prefix, "wild"),
        ],
        None,
    );
    let kind = |h: &str| t.match_request(h, "/").map(|m| m.host_match());
    assert_eq!(kind("api.example.com"), Some(HostMatch::Exact));
    assert_eq!(kind("web.example.com"), Some(HostMatch::Wildcard));
}

// -------------------------------------------------------------------- canary

fn canary_table(rules: CanaryRules<'_>) -> RouteTable {
    let mut b = RouteTableBuilder::new();
    b.backend("stable", LbPolicy::RoundRobin, vec![endpoint(8080)])
        .expect("registers");
    b.backend("canary", LbPolicy::RoundRobin, vec![endpoint(8081)])
        .expect("registers");
    b.canary_route(
        Some("example.com"),
        "/",
        PathType::Prefix,
        "stable",
        &rules,
    )
    .expect("drafts");
    b.build().expect("builds")
}

/// Resolves a request the way the proxy will: match, then ask the canary.
fn route_with_canary(
    t: &RouteTable,
    header: Option<&str>,
    cookie: Option<&str>,
    roll: u32,
) -> String {
    let m = t.match_request("example.com", "/checkout").expect("matches");
    match m.canary() {
        Some(c) if c.decide(header, cookie, roll) => t
            .backend(c.backend())
            .expect("canary backend exists")
            .name()
            .to_owned(),
        _ => m.backend().name().to_owned(),
    }
}

#[test]
fn canary_header_beats_weight() {
    let t = canary_table(CanaryRules {
        backend: "canary",
        header: Some("x-canary"),
        weight: 0,
        ..Default::default()
    });
    // Weight says never; the header says always. The header wins.
    assert_eq!(route_with_canary(&t, Some("always"), None, 99), "canary");

    let t = canary_table(CanaryRules {
        backend: "canary",
        header: Some("x-canary"),
        weight: 100,
        ..Default::default()
    });
    // Weight says always; the header says never. The header still wins.
    assert_eq!(route_with_canary(&t, Some("never"), None, 0), "stable");
}

#[test]
fn canary_header_value_beats_weight() {
    let t = canary_table(CanaryRules {
        backend: "canary",
        header: Some("x-canary"),
        header_value: Some("beta"),
        weight: 0,
        ..Default::default()
    });
    assert_eq!(route_with_canary(&t, Some("beta"), None, 99), "canary");
    // A non-matching value is not decisive: it falls through to the weight,
    // which here is 0.
    assert_eq!(route_with_canary(&t, Some("gamma"), None, 0), "stable");
}

#[test]
fn canary_weight_applies_when_no_rule_is_decisive() {
    let t = canary_table(CanaryRules {
        backend: "canary",
        weight: 30,
        ..Default::default()
    });
    assert_eq!(route_with_canary(&t, None, None, 0), "canary");
    assert_eq!(route_with_canary(&t, None, None, 29), "canary");
    assert_eq!(route_with_canary(&t, None, None, 30), "stable");
}

#[test]
fn canary_cookie_sits_between_header_and_weight() {
    let t = canary_table(CanaryRules {
        backend: "canary",
        header: Some("x-canary"),
        cookie: Some("canary"),
        weight: 0,
        ..Default::default()
    });
    // Header decisive, cookie disagrees -> header wins.
    assert_eq!(
        route_with_canary(&t, Some("never"), Some("always"), 0),
        "stable"
    );
    // Header absent, cookie decisive -> cookie wins over the weight.
    assert_eq!(route_with_canary(&t, None, Some("always"), 99), "canary");
}

#[test]
fn canary_names_the_values_the_caller_must_fetch() {
    let t = canary_table(CanaryRules {
        backend: "canary",
        header: Some("X-Canary"),
        cookie: Some("canary_cookie"),
        ..Default::default()
    });
    let m = t.match_request("example.com", "/").expect("matches");
    let c = m.canary().expect("has a canary");
    assert_eq!(c.header_name(), Some("X-Canary"));
    assert_eq!(c.cookie_name(), Some("canary_cookie"));
}

// ------------------------------------------------------------------ swapping

#[test]
fn publishing_a_new_table_does_not_disturb_a_held_snapshot() {
    let first = table(
        &[(Some("example.com"), "/", PathType::Prefix, "v1")],
        None,
    );
    let shared = SharedRouteTable::new(first);

    // A request in flight holds its snapshot.
    let held = shared.load_full();
    assert_eq!(hit(&held, "example.com", "/").as_deref(), Some("v1"));

    let second = table(
        &[(Some("example.com"), "/", PathType::Prefix, "v2")],
        None,
    );
    shared.store(second);

    assert_eq!(
        hit(&held, "example.com", "/").as_deref(),
        Some("v1"),
        "the in-flight request must finish against the table it started with"
    );
    assert_eq!(
        hit(&shared.load(), "example.com", "/").as_deref(),
        Some("v2"),
        "new requests see the new table"
    );
}

#[test]
fn generation_tracks_publications() {
    let shared = SharedRouteTable::new(table(&[], Some("fallback")));
    assert_eq!(shared.generation(), 0);

    let next = RouteTableBuilder::from_previous(&shared.load())
        .build()
        .expect("builds");
    shared.store(next);
    assert_eq!(shared.generation(), 1);
}

#[test]
fn load_balancer_state_survives_a_rebuild() {
    let mut b = RouteTableBuilder::new();
    b.backend(
        "api",
        LbPolicy::LeastConn,
        vec![endpoint(8080), endpoint(8081)],
    )
    .expect("registers");
    b.route(Some("example.com"), "/", PathType::Prefix, "api")
        .expect("drafts");
    let first = b.build().expect("builds");

    // Two requests in flight to endpoint 0.
    let backend = first.backend(ramjet_router::BackendId(0)).expect("backend");
    let slot = first.stats().slot(backend.stats_index()).expect("slot");
    let _a = slot.acquire(0).expect("endpoint");
    let _b = slot.acquire(0).expect("endpoint");

    // The controller publishes a new table.
    let mut nb = RouteTableBuilder::from_previous(&first);
    nb.backend(
        "api",
        LbPolicy::LeastConn,
        vec![endpoint(8080), endpoint(8081)],
    )
    .expect("registers");
    nb.route(Some("example.com"), "/", PathType::Prefix, "api")
        .expect("drafts");
    let second = nb.build().expect("builds");

    let nb_backend = second.backend(ramjet_router::BackendId(0)).expect("backend");
    let picked = ramjet_router::select_endpoint(nb_backend, second.stats(), 0);
    assert_eq!(
        picked.map(|(i, _)| i),
        Some(1),
        "the new table must still see the two requests in flight on endpoint 0"
    );
}

// ----------------------------------------------------------------------- tls

#[test]
fn certificates_resolve_like_hosts() {
    let mut b = RouteTableBuilder::new();
    b.certificate("api.example.com", Arc::new(CertifiedKeyHandle::new(1)))
        .expect("valid host");
    b.certificate("*.example.com", Arc::new(CertifiedKeyHandle::new(2)))
        .expect("valid host");
    b.default_certificate(Arc::new(CertifiedKeyHandle::new(3)));
    let t = b.build().expect("builds");

    let id = |name: &str| t.tls().resolve(name).map(|k| k.id());
    assert_eq!(id("api.example.com"), Some(1));
    assert_eq!(id("web.example.com"), Some(2));
    assert_eq!(id("deep.web.example.com"), Some(3));
    assert_eq!(id("elsewhere.test"), Some(3));
}
