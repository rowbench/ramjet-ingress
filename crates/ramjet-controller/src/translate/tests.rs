//! Behavioural tests for the translator.
//!
//! Every one of these builds a cluster in memory and asserts on the compiled
//! [`RouteTable`], not on the intermediate plan. Asserting on the plan would
//! let the translation and the router drift apart silently, which is exactly
//! the class of bug that produces "the config looks right but traffic goes
//! somewhere else".

use super::test_support::*;
use super::*;
use crate::annotations::{
    ANNOTATION_AUTO_PROMOTE, ANNOTATION_AUTO_PROMOTE_INTERVAL, ANNOTATION_AUTO_PROMOTE_MAX_5XX,
    ANNOTATION_AUTO_PROMOTE_MAX_LATENCY, ANNOTATION_AUTO_PROMOTE_MIN_REQUESTS,
    ANNOTATION_AUTO_PROMOTE_STATUS, ANNOTATION_AUTO_PROMOTE_STEPS, ANNOTATION_BACKEND_PROTOCOL,
    ANNOTATION_CANARY,
    ANNOTATION_CANARY_WEIGHT, ANNOTATION_MIRROR_HOST, ANNOTATION_MIRROR_PERCENT,
    DEFAULT_PROMOTE_INTERVAL, DEFAULT_PROMOTE_STEPS,
};
use crate::config::CONTROLLER_NAME;

fn opts() -> ControllerOpts {
    ControllerOpts::default()
}

/// A cluster that has our IngressClass but has not made it the default.
fn base() -> ClusterSnapshot {
    ClusterSnapshot::new().with_ingress_class(ingress_class("ramjet", CONTROLLER_NAME, false))
}

/// Marks a fixture Ingress as ours the ordinary way.
fn ours(ingress: Ingress) -> Ingress {
    in_class(ingress, "ramjet")
}

/// Adds a Service and a matching EndpointSlice.
fn backed(
    snapshot: ClusterSnapshot,
    namespace: &str,
    name: &str,
    port: i32,
    target: i32,
    addresses: &[&str],
) -> ClusterSnapshot {
    let endpoints: Vec<EndpointFixture> =
        addresses.iter().map(|a| EndpointFixture::ready(a)).collect();
    snapshot
        .with_service(service(namespace, name, &[(None, port)]))
        .with_endpoint_slice(endpoint_slice(namespace, name, &[(None, target)], &endpoints))
}

fn compile(snapshot: &ClusterSnapshot) -> Translation {
    translate(snapshot, &opts(), None).expect("a snapshot always compiles")
}

fn compile_with(snapshot: &ClusterSnapshot, opts: &ControllerOpts) -> Translation {
    translate(snapshot, opts, None).expect("a snapshot always compiles")
}

/// Backend name a request lands on, or `None` if nothing matched.
fn hit(table: &RouteTable, host: &str, path: &str) -> Option<String> {
    table
        .match_request(host, path)
        .map(|m| m.backend().name().to_owned())
}

fn warnings(translation: &Translation, kind: WarningKind) -> Vec<&Warning> {
    translation
        .warnings
        .iter()
        .filter(|w| w.kind == kind)
        .collect()
}

fn addresses(table: &RouteTable, backend: &str) -> Vec<String> {
    table
        .backends()
        .iter()
        .find(|b| b.name() == backend)
        .map(|b| b.endpoints().iter().map(|e| e.addr.to_string()).collect())
        .unwrap_or_default()
}

// ---------------------------------------------------------------- class ----

#[test]
fn an_ingress_for_another_controller_contributes_nothing() {
    let snapshot = backed(base(), "default", "web", 80, 8080, &["10.0.0.1"])
        .with_ingress_class(ingress_class("nginx", "k8s.io/ingress-nginx", false))
        .with_ingress(in_class(
            ingress(
                "default",
                "web",
                &[rule(Some("example.com"), &[path("/", "Prefix", "web", 80)])],
            ),
            "nginx",
        ));

    let t = compile(&snapshot);
    assert!(t.managed.is_empty());
    assert_eq!(t.config.table.route_count(), 0);
    assert!(t.warnings.is_empty(), "{:?}", t.warnings);
}

#[test]
fn a_classless_ingress_is_claimed_only_when_our_class_is_the_default() {
    let ing = ingress(
        "default",
        "web",
        &[rule(Some("example.com"), &[path("/", "Prefix", "web", 80)])],
    );

    let ignored = compile(&backed(
        base().with_ingress(ing.clone()),
        "default",
        "web",
        80,
        8080,
        &["10.0.0.1"],
    ));
    assert_eq!(ignored.config.table.route_count(), 0);

    let claimed = compile(&backed(
        ClusterSnapshot::new()
            .with_ingress_class(ingress_class("ramjet", CONTROLLER_NAME, true))
            .with_ingress(ing),
        "default",
        "web",
        80,
        8080,
        &["10.0.0.1"],
    ));
    assert_eq!(
        hit(&claimed.config.table, "example.com", "/"),
        Some("default/web:80".to_owned())
    );
}

#[test]
fn the_legacy_class_annotation_still_claims_an_ingress() {
    let snapshot = backed(
        ClusterSnapshot::new().with_ingress(legacy_class(
            ingress(
                "default",
                "web",
                &[rule(Some("example.com"), &[path("/", "Prefix", "web", 80)])],
            ),
            "ramjet",
        )),
        "default",
        "web",
        80,
        8080,
        &["10.0.0.1"],
    );

    let t = compile(&snapshot);
    assert_eq!(t.managed.len(), 1);
    assert_eq!(
        hit(&t.config.table, "example.com", "/"),
        Some("default/web:80".to_owned())
    );
}

#[test]
fn a_dangling_ingress_class_name_is_reported() {
    let snapshot = base().with_ingress(in_class(
        ingress(
            "default",
            "web",
            &[rule(Some("example.com"), &[path("/", "Prefix", "web", 80)])],
        ),
        "does-not-exist",
    ));
    let t = compile(&snapshot);
    assert_eq!(warnings(&t, WarningKind::UnknownClass).len(), 1);
    assert!(t.managed.is_empty());
}

// ----------------------------------------------------------------- paths ----

#[test]
fn path_types_are_passed_through_to_the_router() {
    let snapshot = backed(
        base().with_ingress(ours(ingress(
            "default",
            "web",
            &[rule(
                Some("example.com"),
                &[
                    path("/exact", "Exact", "web", 80),
                    path("/prefix", "Prefix", "web", 80),
                    path("/re[0-9]+", "ImplementationSpecific", "web", 80),
                ],
            )],
        ))),
        "default",
        "web",
        80,
        8080,
        &["10.0.0.1"],
    );
    let table = compile(&snapshot).config.table;

    // Exact really is exact.
    assert!(hit(&table, "example.com", "/exact").is_some());
    assert!(hit(&table, "example.com", "/exact/more").is_none());
    // Prefix is element-wise.
    assert!(hit(&table, "example.com", "/prefix/deeper").is_some());
    assert!(hit(&table, "example.com", "/prefixed").is_none());
    // ImplementationSpecific is a regex, ingress-nginx style.
    assert!(hit(&table, "example.com", "/re42/anything").is_some());
    assert!(hit(&table, "example.com", "/rex").is_none());
}

#[test]
fn an_absent_path_defaults_to_root() {
    let snapshot = backed(
        base().with_ingress(ours(ingress(
            "default",
            "web",
            &[rule(Some("example.com"), &[pathless("Prefix", "web", 80)])],
        ))),
        "default",
        "web",
        80,
        8080,
        &["10.0.0.1"],
    );
    assert!(hit(&compile(&snapshot).config.table, "example.com", "/anything").is_some());
}

#[test]
fn wildcard_hosts_cover_exactly_one_label() {
    let snapshot = backed(
        base().with_ingress(ours(ingress(
            "default",
            "web",
            &[rule(
                Some("*.example.com"),
                &[path("/", "Prefix", "web", 80)],
            )],
        ))),
        "default",
        "web",
        80,
        8080,
        &["10.0.0.1"],
    );
    let table = compile(&snapshot).config.table;

    assert!(hit(&table, "api.example.com", "/").is_some());
    assert!(hit(&table, "a.b.example.com", "/").is_none());
    assert!(hit(&table, "example.com", "/").is_none());
}

#[test]
fn a_hostless_rule_serves_every_unclaimed_name() {
    let snapshot = backed(
        base().with_ingress(ours(ingress(
            "default",
            "web",
            &[rule(None, &[path("/", "Prefix", "web", 80)])],
        ))),
        "default",
        "web",
        80,
        8080,
        &["10.0.0.1"],
    );
    let table = compile(&snapshot).config.table;
    assert!(hit(&table, "anything.test", "/").is_some());
}

#[test]
fn host_case_and_a_trailing_dot_are_normalised() {
    let snapshot = backed(
        base().with_ingress(ours(ingress(
            "default",
            "web",
            &[rule(
                Some("Example.COM."),
                &[path("/", "Prefix", "web", 80)],
            )],
        ))),
        "default",
        "web",
        80,
        8080,
        &["10.0.0.1"],
    );
    assert!(hit(&compile(&snapshot).config.table, "example.com", "/").is_some());
}

#[test]
fn an_unknown_path_type_degrades_to_a_regex_with_a_warning() {
    let snapshot = backed(
        base().with_ingress(ours(ingress(
            "default",
            "web",
            &[rule(Some("example.com"), &[path("/x", "Sideways", "web", 80)])],
        ))),
        "default",
        "web",
        80,
        8080,
        &["10.0.0.1"],
    );
    let t = compile(&snapshot);
    assert_eq!(warnings(&t, WarningKind::InvalidAnnotation).len(), 1);
    assert!(hit(&t.config.table, "example.com", "/xyz").is_some());
}

#[test]
fn a_route_the_router_refuses_does_not_take_the_table_with_it() {
    let snapshot = backed(
        base().with_ingress(ours(ingress(
            "default",
            "web",
            &[
                rule(
                    Some("good.example.com"),
                    &[path("/ok", "Prefix", "web", 80)],
                ),
                // An unbalanced group is not a regex, and the builder says so.
                rule(
                    Some("bad.example.com"),
                    &[path("/[oops", "ImplementationSpecific", "web", 80)],
                ),
            ],
        ))),
        "default",
        "web",
        80,
        8080,
        &["10.0.0.1"],
    );

    let t = compile(&snapshot);
    assert_eq!(warnings(&t, WarningKind::InvalidRoute).len(), 1);
    assert!(hit(&t.config.table, "good.example.com", "/ok").is_some());
    assert!(hit(&t.config.table, "bad.example.com", "/anything").is_none());
}

#[test]
fn a_resource_backend_is_rejected_without_dropping_its_neighbours() {
    let snapshot = backed(
        base().with_ingress(ours(ingress(
            "default",
            "web",
            &[rule(
                Some("example.com"),
                &[resource_path("/assets"), path("/", "Prefix", "web", 80)],
            )],
        ))),
        "default",
        "web",
        80,
        8080,
        &["10.0.0.1"],
    );
    let t = compile(&snapshot);
    assert_eq!(warnings(&t, WarningKind::UnsupportedBackend).len(), 1);
    assert!(hit(&t.config.table, "example.com", "/").is_some());
}

// ------------------------------------------------------------- backends ----

#[test]
fn a_named_port_resolves_through_the_service_to_the_target_port() {
    let snapshot = base()
        .with_ingress(ours(ingress(
            "default",
            "web",
            &[rule(
                Some("example.com"),
                &[named_path("/", "Prefix", "web", "http")],
            )],
        )))
        .with_service(service("default", "web", &[(Some("http"), 80)]))
        .with_endpoint_slice(endpoint_slice(
            "default",
            "web",
            &[(Some("http"), 8080)],
            &[EndpointFixture::ready("10.0.0.1")],
        ));

    let table = compile(&snapshot).config.table;
    assert_eq!(
        hit(&table, "example.com", "/"),
        Some("default/web:http".to_owned())
    );
    assert_eq!(addresses(&table, "default/web:http"), ["10.0.0.1:8080"]);
}

#[test]
fn unready_endpoints_never_reach_the_table() {
    let snapshot = base()
        .with_ingress(ours(ingress(
            "default",
            "web",
            &[rule(Some("example.com"), &[path("/", "Prefix", "web", 80)])],
        )))
        .with_service(service("default", "web", &[(None, 80)]))
        .with_endpoint_slice(endpoint_slice(
            "default",
            "web",
            &[(None, 8080)],
            &[
                EndpointFixture::ready("10.0.0.1"),
                EndpointFixture::unready("10.0.0.2"),
                EndpointFixture::terminating("10.0.0.3"),
            ],
        ));

    let t = compile(&snapshot);
    assert_eq!(addresses(&t.config.table, "default/web:80"), ["10.0.0.1:8080"]);
    assert_eq!(warnings(&t, WarningKind::EndpointsSkipped).len(), 1);
}

#[test]
fn a_missing_service_keeps_the_route_and_serves_503() {
    // The route must survive, or requests fall through to some unrelated
    // wildcard for the duration of a rollout.
    let snapshot = base().with_ingress(ours(ingress(
        "default",
        "web",
        &[rule(Some("example.com"), &[path("/", "Prefix", "web", 80)])],
    )));

    let t = compile(&snapshot);
    assert_eq!(
        hit(&t.config.table, "example.com", "/"),
        Some("default/web:80".to_owned())
    );
    assert!(addresses(&t.config.table, "default/web:80").is_empty());
    assert_eq!(warnings(&t, WarningKind::ServiceUnresolved).len(), 1);
}

#[test]
fn an_external_name_service_is_refused_with_a_named_warning() {
    let mut svc = service("default", "web", &[(None, 80)]);
    let spec = svc.spec.get_or_insert_default();
    spec.type_ = Some("ExternalName".to_owned());
    spec.external_name = Some("elsewhere.example.com".to_owned());

    let snapshot = base()
        .with_ingress(ours(ingress(
            "default",
            "web",
            &[rule(Some("example.com"), &[path("/", "Prefix", "web", 80)])],
        )))
        .with_service(svc);

    let t = compile(&snapshot);
    let found = warnings(&t, WarningKind::ExternalNameService);
    assert_eq!(found.len(), 1);
    assert!(found[0].detail.contains("elsewhere.example.com"), "{:?}", found[0]);
    assert!(addresses(&t.config.table, "default/web:80").is_empty());
}

#[test]
fn one_backend_is_registered_per_distinct_service_port() {
    let snapshot = backed(
        base().with_ingress(ours(ingress(
            "default",
            "web",
            &[rule(
                Some("example.com"),
                &[
                    path("/a", "Prefix", "web", 80),
                    path("/b", "Prefix", "web", 80),
                ],
            )],
        ))),
        "default",
        "web",
        80,
        8080,
        &["10.0.0.1"],
    );
    assert_eq!(compile(&snapshot).config.table.backends().len(), 1);
}

// ------------------------------------------------------------ conflicts ----

#[test]
fn the_oldest_ingress_wins_a_host_and_path_conflict() {
    let older = created_at(
        ours(ingress(
            "team-a",
            "web",
            &[rule(Some("example.com"), &[path("/", "Prefix", "a", 80)])],
        )),
        1_000,
    );
    let newer = created_at(
        ours(ingress(
            "team-b",
            "web",
            &[rule(Some("example.com"), &[path("/", "Prefix", "b", 80)])],
        )),
        2_000,
    );

    // Present the newer one first: age must decide, not iteration order.
    let snapshot = backed(
        backed(
            base().with_ingress(newer).with_ingress(older),
            "team-a",
            "a",
            80,
            8080,
            &["10.0.0.1"],
        ),
        "team-b",
        "b",
        80,
        8080,
        &["10.0.0.2"],
    );

    let t = compile(&snapshot);
    assert_eq!(
        hit(&t.config.table, "example.com", "/"),
        Some("team-a/a:80".to_owned())
    );
    let conflicts = warnings(&t, WarningKind::RouteConflict);
    assert_eq!(conflicts.len(), 1);
    assert_eq!(conflicts[0].subject.namespace, "team-b");
}

#[test]
fn a_creation_time_tie_is_broken_deterministically_by_name() {
    let a = created_at(
        ours(ingress(
            "default",
            "aaa",
            &[rule(Some("example.com"), &[path("/", "Prefix", "a", 80)])],
        )),
        1_000,
    );
    let b = created_at(
        ours(ingress(
            "default",
            "zzz",
            &[rule(Some("example.com"), &[path("/", "Prefix", "b", 80)])],
        )),
        1_000,
    );

    let snapshot = base().with_ingress(b).with_ingress(a);
    assert_eq!(
        hit(&compile(&snapshot).config.table, "example.com", "/"),
        Some("default/a:80".to_owned())
    );
}

#[test]
fn the_same_path_with_different_path_types_is_not_a_conflict() {
    let snapshot = base().with_ingress(ours(ingress(
        "default",
        "web",
        &[rule(
            Some("example.com"),
            &[
                path("/x", "Exact", "a", 80),
                path("/x", "Prefix", "b", 80),
            ],
        )],
    )));

    let t = compile(&snapshot);
    assert!(warnings(&t, WarningKind::RouteConflict).is_empty());
    assert_eq!(
        hit(&t.config.table, "example.com", "/x"),
        Some("default/a:80".to_owned()),
        "Exact must outrank Prefix"
    );
    assert_eq!(
        hit(&t.config.table, "example.com", "/x/y"),
        Some("default/b:80".to_owned())
    );
}

#[test]
fn the_same_path_on_different_hosts_is_not_a_conflict() {
    let snapshot = base().with_ingress(ours(ingress(
        "default",
        "web",
        &[
            rule(Some("a.example.com"), &[path("/", "Prefix", "a", 80)]),
            rule(Some("b.example.com"), &[path("/", "Prefix", "b", 80)]),
        ],
    )));
    let t = compile(&snapshot);
    assert!(warnings(&t, WarningKind::RouteConflict).is_empty());
    assert_eq!(t.config.table.route_count(), 2);
}

// ------------------------------------------------------ default backend ----

#[test]
fn an_ingress_default_backend_becomes_a_root_fallback_on_its_hosts() {
    let snapshot = base().with_ingress(with_default_backend(
        ours(ingress(
            "default",
            "web",
            &[rule(
                Some("example.com"),
                &[path("/api", "Prefix", "api", 80)],
            )],
        )),
        "fallback",
        80,
    ));

    let table = compile(&snapshot).config.table;
    assert_eq!(
        hit(&table, "example.com", "/api/v1"),
        Some("default/api:80".to_owned())
    );
    assert_eq!(
        hit(&table, "example.com", "/elsewhere"),
        Some("default/fallback:80".to_owned())
    );
}

#[test]
fn an_explicit_root_route_beats_a_synthesised_fallback() {
    let snapshot = base().with_ingress(with_default_backend(
        ours(ingress(
            "default",
            "web",
            &[rule(Some("example.com"), &[path("/", "Prefix", "real", 80)])],
        )),
        "fallback",
        80,
    ));

    let t = compile(&snapshot);
    assert_eq!(
        hit(&t.config.table, "example.com", "/"),
        Some("default/real:80".to_owned())
    );
    assert!(
        warnings(&t, WarningKind::RouteConflict).is_empty(),
        "a fallback losing to a real rule is normal, not a conflict"
    );
}

#[test]
fn a_bare_default_backend_ingress_becomes_the_cluster_default() {
    let mut bare = ours(ingress("default", "catch-all", &[]));
    bare.spec.get_or_insert_default().rules = None;
    let snapshot = base().with_ingress(with_default_backend(bare, "fallback", 80));

    let t = compile(&snapshot);
    assert!(t.config.table.default_backend().is_some());
    assert_eq!(
        t.config
            .table
            .default_backend()
            .and_then(|id| t.config.table.backend(id))
            .map(|b| b.name().to_owned()),
        Some("default/fallback:80".to_owned())
    );
}

#[test]
fn a_second_bare_default_backend_is_reported_and_ignored() {
    let bare = |name: &str, service: &str, age: i64| {
        let mut ing = ours(ingress("default", name, &[]));
        ing.spec.get_or_insert_default().rules = None;
        created_at(with_default_backend(ing, service, 80), age)
    };
    let snapshot = base()
        .with_ingress(bare("newer", "loser", 2_000))
        .with_ingress(bare("older", "winner", 1_000));

    let t = compile(&snapshot);
    assert_eq!(warnings(&t, WarningKind::DefaultBackendConflict).len(), 1);
    assert_eq!(
        t.config
            .table
            .default_backend()
            .and_then(|id| t.config.table.backend(id))
            .map(|b| b.name().to_owned()),
        Some("default/winner:80".to_owned())
    );
}

#[test]
fn the_configured_default_backend_is_used_when_no_ingress_supplies_one() {
    let opts = ControllerOpts {
        default_backend: Some("kube-system/default-http-backend:8080".parse().expect("valid")),
        ..Default::default()
    };
    let snapshot = base().with_ingress(ours(ingress(
        "default",
        "web",
        &[rule(Some("example.com"), &[path("/", "Prefix", "web", 80)])],
    )));

    let t = compile_with(&snapshot, &opts);
    assert_eq!(
        t.config
            .table
            .default_backend()
            .and_then(|id| t.config.table.backend(id))
            .map(|b| b.name().to_owned()),
        Some("kube-system/default-http-backend:8080".to_owned())
    );
}

// ------------------------------------------------------------------ TLS ----

#[test]
fn a_tls_secret_becomes_an_sni_entry_and_cert_material() {
    let snapshot = base()
        .with_ingress(with_tls(
            ours(ingress(
                "default",
                "web",
                &[rule(Some("example.com"), &[path("/", "Prefix", "web", 80)])],
            )),
            &["example.com"],
            "web-tls",
        ))
        .with_secret(secret("default", "web-tls", PEM_CERT, PEM_KEY));

    let t = compile(&snapshot);
    assert_eq!(t.config.certs.len(), 1);
    let material = &t.config.certs[0];
    assert_eq!(material.cert_chain_pem, PEM_CERT);
    assert_eq!(material.key_pem, PEM_KEY);

    let resolved = t.config.table.tls().resolve("example.com").map(|k| k.id());
    assert_eq!(
        resolved,
        Some(material.handle_id),
        "the SniMap handle must match the material the proxy will parse"
    );
}

#[test]
fn one_secret_serving_several_hosts_yields_one_cert_material() {
    let snapshot = base()
        .with_ingress(with_tls(
            ours(ingress(
                "default",
                "web",
                &[rule(Some("a.example.com"), &[path("/", "Prefix", "web", 80)])],
            )),
            &["a.example.com", "b.example.com", "*.wild.example.com"],
            "web-tls",
        ))
        .with_secret(secret("default", "web-tls", PEM_CERT, PEM_KEY));

    let t = compile(&snapshot);
    assert_eq!(t.config.certs.len(), 1);
    assert_eq!(t.config.table.tls().len(), 3);
    let id = t.config.certs[0].handle_id;
    for host in ["a.example.com", "b.example.com", "one.wild.example.com"] {
        assert_eq!(
            t.config.table.tls().resolve(host).map(|k| k.id()),
            Some(id),
            "{host}"
        );
    }
}

#[test]
fn a_malformed_tls_secret_does_not_poison_the_rebuild() {
    let snapshot = base()
        .with_ingress(with_tls(
            ours(ingress(
                "default",
                "web",
                &[rule(Some("example.com"), &[path("/", "Prefix", "web", 80)])],
            )),
            &["example.com"],
            "broken",
        ))
        .with_secret(secret("default", "broken", b"not pem", PEM_KEY));

    let t = compile(&snapshot);
    assert_eq!(warnings(&t, WarningKind::TlsSecret).len(), 1);
    assert!(t.config.certs.is_empty());
    assert!(t.config.table.tls().is_empty());
    // The plaintext route is untouched.
    assert!(hit(&t.config.table, "example.com", "/").is_some());
}

#[test]
fn a_missing_tls_secret_is_reported_not_fatal() {
    let snapshot = base().with_ingress(with_tls(
        ours(ingress(
            "default",
            "web",
            &[rule(Some("example.com"), &[path("/", "Prefix", "web", 80)])],
        )),
        &["example.com"],
        "absent",
    ));

    let t = compile(&snapshot);
    assert_eq!(warnings(&t, WarningKind::TlsSecret).len(), 1);
    assert!(hit(&t.config.table, "example.com", "/").is_some());
}

#[test]
fn a_tls_entry_with_no_hosts_is_reported() {
    let snapshot = base()
        .with_ingress(with_tls(
            ours(ingress(
                "default",
                "web",
                &[rule(Some("example.com"), &[path("/", "Prefix", "web", 80)])],
            )),
            &[],
            "web-tls",
        ))
        .with_secret(secret("default", "web-tls", PEM_CERT, PEM_KEY));

    let t = compile(&snapshot);
    assert_eq!(warnings(&t, WarningKind::TlsHostless).len(), 1);
    assert!(t.config.certs.is_empty());
}

#[test]
fn the_oldest_ingress_wins_a_certificate_conflict() {
    let older = created_at(
        with_tls(
            ours(ingress(
                "team-a",
                "web",
                &[rule(Some("example.com"), &[path("/", "Prefix", "a", 80)])],
            )),
            &["example.com"],
            "a-tls",
        ),
        1_000,
    );
    let newer = created_at(
        with_tls(
            ours(ingress(
                "team-b",
                "web",
                &[rule(Some("other.example.com"), &[path("/", "Prefix", "b", 80)])],
            )),
            &["example.com"],
            "b-tls",
        ),
        2_000,
    );

    let snapshot = base()
        .with_ingress(newer)
        .with_ingress(older)
        .with_secret(secret("team-a", "a-tls", PEM_CERT, PEM_KEY))
        .with_secret(secret("team-b", "b-tls", b"-----BEGIN CERTIFICATE-----b", PEM_KEY));

    let t = compile(&snapshot);
    assert_eq!(warnings(&t, WarningKind::TlsConflict).len(), 1);

    let winner = t
        .config
        .certs
        .iter()
        .find(|c| c.cert_chain_pem == PEM_CERT)
        .expect("team-a's certificate is compiled");
    assert_eq!(
        t.config.table.tls().resolve("example.com").map(|k| k.id()),
        Some(winner.handle_id)
    );
}

#[test]
fn the_default_tls_secret_answers_an_unmatched_sni() {
    let opts = ControllerOpts {
        default_tls_secret: Some("kube-system/wildcard".to_owned()),
        ..Default::default()
    };
    let snapshot = base()
        .with_ingress(ours(ingress(
            "default",
            "web",
            &[rule(Some("example.com"), &[path("/", "Prefix", "web", 80)])],
        )))
        .with_secret(secret("kube-system", "wildcard", PEM_CERT, PEM_KEY));

    let t = compile_with(&snapshot, &opts);
    assert_eq!(t.config.certs.len(), 1);
    assert_eq!(
        t.config.table.tls().resolve("never.seen.before").map(|k| k.id()),
        Some(t.config.certs[0].handle_id)
    );
}

// --------------------------------------------------------------- canary ----

/// Builds a production Ingress plus a canary Ingress carrying `annotations`.
fn canary_pair(annotations: &[(&str, &str)]) -> ClusterSnapshot {
    let production = created_at(
        ours(ingress(
            "default",
            "web",
            &[rule(
                Some("example.com"),
                &[path("/", "Prefix", "stable", 80)],
            )],
        )),
        1_000,
    );
    let mut canary = created_at(
        ours(ingress(
            "default",
            "web-canary",
            &[rule(
                Some("example.com"),
                &[path("/", "Prefix", "canary", 80)],
            )],
        )),
        2_000,
    );
    canary = annotate(canary, crate::ANNOTATION_CANARY, "true");
    for (key, value) in annotations {
        canary = annotate(canary, key, value);
    }

    base().with_ingress(production).with_ingress(canary)
}

/// The canary decision for one request against the compiled table.
fn diverts(
    table: &RouteTable,
    header: Option<&str>,
    cookie: Option<&str>,
    roll: u32,
) -> Option<String> {
    let matched = table.match_request("example.com", "/")?;
    let spec = matched.canary()?;
    let id = if spec.decide(header, cookie, roll) {
        spec.backend()
    } else {
        return Some(matched.backend().name().to_owned());
    };
    table.backend(id).map(|b| b.name().to_owned())
}

#[test]
fn canary_by_weight_splits_traffic() {
    let t = compile(&canary_pair(&[(crate::ANNOTATION_CANARY_WEIGHT, "30")]));
    let table = &t.config.table;

    let spec = table
        .match_request("example.com", "/")
        .and_then(|m| m.canary())
        .expect("the canary attached to the production route");
    assert_eq!(spec.weight(), 30);
    assert_eq!(spec.weight_total(), 100);

    assert_eq!(diverts(table, None, None, 0), Some("default/canary:80".to_owned()));
    assert_eq!(diverts(table, None, None, 29), Some("default/canary:80".to_owned()));
    assert_eq!(diverts(table, None, None, 30), Some("default/stable:80".to_owned()));
}

#[test]
fn canary_weight_total_overrides_the_denominator() {
    let t = compile(&canary_pair(&[
        (crate::ANNOTATION_CANARY_WEIGHT, "1"),
        (crate::ANNOTATION_CANARY_WEIGHT_TOTAL, "1000"),
    ]));
    let spec = t
        .config
        .table
        .match_request("example.com", "/")
        .and_then(|m| m.canary())
        .expect("canary");
    assert_eq!(spec.weight(), 1);
    assert_eq!(spec.weight_total(), 1000);
}

#[test]
fn canary_by_header_honours_always_and_never() {
    let t = compile(&canary_pair(&[(
        crate::ANNOTATION_CANARY_BY_HEADER,
        "x-canary",
    )]));
    let table = &t.config.table;

    let spec = table
        .match_request("example.com", "/")
        .and_then(|m| m.canary())
        .expect("canary");
    assert_eq!(spec.header_name(), Some("x-canary"));

    assert_eq!(
        diverts(table, Some("always"), None, 99),
        Some("default/canary:80".to_owned())
    );
    assert_eq!(
        diverts(table, Some("never"), None, 0),
        Some("default/stable:80".to_owned())
    );
}

#[test]
fn canary_by_header_value_matches_exactly() {
    let t = compile(&canary_pair(&[
        (crate::ANNOTATION_CANARY_BY_HEADER, "x-canary"),
        (crate::ANNOTATION_CANARY_BY_HEADER_VALUE, "beta"),
    ]));
    let table = &t.config.table;
    assert_eq!(
        diverts(table, Some("beta"), None, 99),
        Some("default/canary:80".to_owned())
    );
    assert_eq!(
        diverts(table, Some("beta-2"), None, 99),
        Some("default/stable:80".to_owned())
    );
}

#[test]
fn canary_by_header_pattern_is_anchored() {
    let t = compile(&canary_pair(&[
        (crate::ANNOTATION_CANARY_BY_HEADER, "x-canary"),
        (crate::ANNOTATION_CANARY_BY_HEADER_PATTERN, "beta.*"),
    ]));
    let table = &t.config.table;
    assert_eq!(
        diverts(table, Some("beta-2"), None, 99),
        Some("default/canary:80".to_owned())
    );
    assert_eq!(
        diverts(table, Some("not-beta"), None, 99),
        Some("default/stable:80".to_owned()),
        "an unanchored pattern would divert this"
    );
}

#[test]
fn canary_by_cookie_honours_always_and_never() {
    let t = compile(&canary_pair(&[(
        crate::ANNOTATION_CANARY_BY_COOKIE,
        "canary-cookie",
    )]));
    let table = &t.config.table;

    let spec = table
        .match_request("example.com", "/")
        .and_then(|m| m.canary())
        .expect("canary");
    assert_eq!(spec.cookie_name(), Some("canary-cookie"));

    assert_eq!(
        diverts(table, None, Some("always"), 99),
        Some("default/canary:80".to_owned())
    );
    assert_eq!(
        diverts(table, None, Some("never"), 0),
        Some("default/stable:80".to_owned())
    );
}

#[test]
fn a_canary_setting_both_a_value_and_a_pattern_keeps_the_production_route() {
    let t = compile(&canary_pair(&[
        (crate::ANNOTATION_CANARY_BY_HEADER, "x-canary"),
        (crate::ANNOTATION_CANARY_BY_HEADER_VALUE, "beta"),
        (crate::ANNOTATION_CANARY_BY_HEADER_PATTERN, "beta.*"),
    ]));

    assert_eq!(warnings(&t, WarningKind::InvalidRoute).len(), 1);
    let matched = t
        .config
        .table
        .match_request("example.com", "/")
        .expect("the production route survives a broken canary");
    assert_eq!(matched.backend().name(), "default/stable:80");
    assert!(matched.canary().is_none());
}

#[test]
fn a_canary_attaches_only_to_the_route_it_shadows() {
    let production = created_at(
        ours(ingress(
            "default",
            "web",
            &[rule(
                Some("example.com"),
                &[
                    path("/", "Prefix", "stable", 80),
                    path("/admin", "Prefix", "admin", 80),
                ],
            )],
        )),
        1_000,
    );
    let canary = annotate(
        created_at(
            ours(ingress(
                "default",
                "web-canary",
                &[rule(
                    Some("example.com"),
                    &[path("/", "Prefix", "canary", 80)],
                )],
            )),
            2_000,
        ),
        crate::ANNOTATION_CANARY,
        "true",
    );
    let canary = annotate(canary, crate::ANNOTATION_CANARY_WEIGHT, "50");

    let snapshot = base().with_ingress(production).with_ingress(canary);
    let table = compile(&snapshot).config.table;

    assert!(table
        .match_request("example.com", "/")
        .and_then(|m| m.canary())
        .is_some());
    assert!(
        table
            .match_request("example.com", "/admin")
            .and_then(|m| m.canary())
            .is_none(),
        "/admin was never shadowed"
    );
}

#[test]
fn a_canary_with_no_production_route_is_reported() {
    let canary = annotate(
        ours(ingress(
            "default",
            "web-canary",
            &[rule(
                Some("orphan.example.com"),
                &[path("/", "Prefix", "canary", 80)],
            )],
        )),
        crate::ANNOTATION_CANARY,
        "true",
    );
    let canary = annotate(canary, crate::ANNOTATION_CANARY_WEIGHT, "50");

    let t = compile(&base().with_ingress(canary));
    assert_eq!(warnings(&t, WarningKind::CanaryOrphan).len(), 1);
    assert_eq!(t.config.table.route_count(), 0);
}

#[test]
fn a_second_canary_on_one_route_is_reported_and_ignored() {
    let mut snapshot = canary_pair(&[(crate::ANNOTATION_CANARY_WEIGHT, "10")]);
    let second = annotate(
        annotate(
            created_at(
                ours(ingress(
                    "default",
                    "web-canary-2",
                    &[rule(
                        Some("example.com"),
                        &[path("/", "Prefix", "other", 80)],
                    )],
                )),
                3_000,
            ),
            crate::ANNOTATION_CANARY,
            "true",
        ),
        crate::ANNOTATION_CANARY_WEIGHT,
        "90",
    );
    snapshot = snapshot.with_ingress(second);

    let t = compile(&snapshot);
    assert_eq!(warnings(&t, WarningKind::CanaryConflict).len(), 1);
    let spec = t
        .config
        .table
        .match_request("example.com", "/")
        .and_then(|m| m.canary())
        .expect("canary");
    assert_eq!(spec.weight(), 10, "the older canary keeps the route");
}

#[test]
fn a_canary_that_can_never_divert_is_reported() {
    let t = compile(&canary_pair(&[]));
    assert_eq!(warnings(&t, WarningKind::CanaryInert).len(), 1);
}

#[test]
fn a_canary_ingress_is_not_itself_a_production_route() {
    let t = compile(&canary_pair(&[(crate::ANNOTATION_CANARY_WEIGHT, "10")]));
    assert_eq!(
        t.config.table.route_count(),
        1,
        "the canary merges into the production route rather than adding one"
    );
}

// --------------------------------------------------------------- digest ----

#[test]
fn the_same_snapshot_compiles_to_the_same_digest() {
    let snapshot = backed(
        base().with_ingress(ours(ingress(
            "default",
            "web",
            &[rule(Some("example.com"), &[path("/", "Prefix", "web", 80)])],
        ))),
        "default",
        "web",
        80,
        8080,
        &["10.0.0.1", "10.0.0.2"],
    );
    assert_eq!(compile(&snapshot).digest, compile(&snapshot).digest);
}

#[test]
fn object_ordering_does_not_change_the_digest() {
    let a = created_at(
        ours(ingress(
            "default",
            "a",
            &[rule(Some("a.example.com"), &[path("/", "Prefix", "a", 80)])],
        )),
        1_000,
    );
    let b = created_at(
        ours(ingress(
            "default",
            "b",
            &[rule(Some("b.example.com"), &[path("/", "Prefix", "b", 80)])],
        )),
        2_000,
    );

    let forwards = base().with_ingress(a.clone()).with_ingress(b.clone());
    let backwards = base().with_ingress(b).with_ingress(a);
    assert_eq!(compile(&forwards).digest, compile(&backwards).digest);
}

#[test]
fn endpoint_ordering_does_not_change_the_digest_but_membership_does() {
    let route = ours(ingress(
        "default",
        "web",
        &[rule(Some("example.com"), &[path("/", "Prefix", "web", 80)])],
    ));

    let one = backed(
        base().with_ingress(route.clone()),
        "default",
        "web",
        80,
        8080,
        &["10.0.0.1", "10.0.0.2"],
    );
    let reordered = backed(
        base().with_ingress(route.clone()),
        "default",
        "web",
        80,
        8080,
        &["10.0.0.2", "10.0.0.1"],
    );
    let scaled = backed(
        base().with_ingress(route),
        "default",
        "web",
        80,
        8080,
        &["10.0.0.1", "10.0.0.2", "10.0.0.3"],
    );

    assert_eq!(compile(&one).digest, compile(&reordered).digest);
    assert_ne!(compile(&one).digest, compile(&scaled).digest);
}

#[test]
fn a_rotated_certificate_changes_the_digest() {
    let ing = with_tls(
        ours(ingress(
            "default",
            "web",
            &[rule(Some("example.com"), &[path("/", "Prefix", "web", 80)])],
        )),
        &["example.com"],
        "web-tls",
    );
    let before = base()
        .with_ingress(ing.clone())
        .with_secret(secret("default", "web-tls", PEM_CERT, PEM_KEY));
    let after = base().with_ingress(ing).with_secret(secret(
        "default",
        "web-tls",
        b"-----BEGIN CERTIFICATE-----rotated",
        PEM_KEY,
    ));

    assert_ne!(compile(&before).digest, compile(&after).digest);
}

#[test]
fn the_generation_is_not_part_of_the_digest() {
    let snapshot = base().with_ingress(ours(ingress(
        "default",
        "web",
        &[rule(Some("example.com"), &[path("/", "Prefix", "web", 80)])],
    )));

    let first = compile(&snapshot);
    let second = translate(&snapshot, &opts(), Some(&first.config.table)).expect("translates");
    assert_eq!(first.digest, second.digest);
    assert_eq!(
        second.config.table.generation(),
        first.config.table.generation() + 1
    );
}

#[test]
fn the_managed_list_is_what_the_status_writer_needs() {
    let snapshot = base()
        .with_ingress(ours(ingress("default", "b", &[])))
        .with_ingress(ours(ingress("default", "a", &[])))
        .with_ingress(in_class(ingress("default", "theirs", &[]), "nginx"));

    let t = compile(&snapshot);
    let names: Vec<String> = t.managed.iter().map(ToString::to_string).collect();
    assert_eq!(names, ["default/a", "default/b"]);
}

// ---------------------------------------------------------------------------
// Traffic mirroring
// ---------------------------------------------------------------------------

/// A cluster with `prod/api` serving `app.example.com/` and a `prod/shadow`
/// Service standing by, which is the starting point for every mirror test.
fn mirrorable() -> ClusterSnapshot {
    let snapshot = backed(base(), "prod", "api", 80, 8080, &["10.0.0.1"]);
    backed(snapshot, "prod", "shadow", 80, 8080, &["10.0.9.1"])
}

fn with_mirror(annotations: &[(&str, &str)]) -> ClusterSnapshot {
    let mut object = ours(ingress(
        "prod",
        "web",
        &[rule(Some("app.example.com"), &[path("/", "Prefix", "api", 80)])],
    ));
    for (key, value) in annotations {
        object = annotate(object, key, value);
    }
    mirrorable().with_ingress(object)
}

/// The mirror on `app.example.com/`, as the compiled table holds it.
fn compiled_mirror(table: &RouteTable) -> Option<(String, u32, Option<String>)> {
    let matched = table.match_request("app.example.com", "/")?;
    let mirror = matched.mirror()?;
    Some((
        table.backend(mirror.backend())?.name().to_owned(),
        mirror.percent(),
        mirror.host().map(str::to_owned),
    ))
}

#[test]
fn a_mirror_backend_annotation_attaches_a_mirror() {
    let snapshot = with_mirror(&[(ANNOTATION_MIRROR_BACKEND, "prod/shadow:80")]);
    let compiled = compile(&snapshot);

    assert_eq!(
        compiled_mirror(&compiled.config.table),
        Some(("prod/shadow:80".to_owned(), 100, None)),
        "an unset percent mirrors everything"
    );
    assert!(compiled.warnings.is_empty(), "{:?}", compiled.warnings);
}

#[test]
fn the_percent_and_host_annotations_are_carried_through() {
    let snapshot = with_mirror(&[
        (ANNOTATION_MIRROR_BACKEND, "prod/shadow:80"),
        (ANNOTATION_MIRROR_PERCENT, "10"),
        (ANNOTATION_MIRROR_HOST, "shadow.internal"),
    ]);
    let compiled = compile(&snapshot);
    assert_eq!(
        compiled_mirror(&compiled.config.table),
        Some((
            "prod/shadow:80".to_owned(),
            10,
            Some("shadow.internal".to_owned())
        ))
    );
}

#[test]
fn a_mirror_backend_may_omit_its_namespace() {
    // A shadow of a service usually lives beside it, and making people write
    // their own namespace out is a papercut with no upside.
    let snapshot = with_mirror(&[(ANNOTATION_MIRROR_BACKEND, "shadow:80")]);
    let compiled = compile(&snapshot);
    assert_eq!(
        compiled_mirror(&compiled.config.table).map(|(backend, _, _)| backend),
        Some("prod/shadow:80".to_owned())
    );
}

#[test]
fn a_mirror_backend_resolves_its_endpoints_like_any_other() {
    // The whole point of routing it through the normal backend machinery: the
    // copy goes to a pod, not to a Service VIP.
    let snapshot = with_mirror(&[(ANNOTATION_MIRROR_BACKEND, "prod/shadow:80")]);
    let table = compile(&snapshot).config.table;
    let mirror = table
        .match_request("app.example.com", "/")
        .and_then(|m| m.mirror())
        .expect("a mirror");
    let backend = table.backend(mirror.backend()).expect("a backend");
    assert_eq!(
        backend
            .endpoints()
            .iter()
            .map(|e| e.addr.to_string())
            .collect::<Vec<_>>(),
        vec!["10.0.9.1:8080"]
    );
}

#[test]
fn a_percent_of_zero_compiles_to_a_mirror_that_never_fires() {
    let snapshot = with_mirror(&[
        (ANNOTATION_MIRROR_BACKEND, "prod/shadow:80"),
        (ANNOTATION_MIRROR_PERCENT, "0"),
    ]);
    let table = compile(&snapshot).config.table;
    let mirror = table
        .match_request("app.example.com", "/")
        .and_then(|m| m.mirror())
        .expect("the mirror is still attached");
    assert_eq!(mirror.percent(), 0);
    assert!((0..100).all(|roll| !mirror.sample(roll)));
}

#[test]
fn an_out_of_range_percent_warns_and_mirrors_everything() {
    let snapshot = with_mirror(&[
        (ANNOTATION_MIRROR_BACKEND, "prod/shadow:80"),
        (ANNOTATION_MIRROR_PERCENT, "150"),
    ]);
    let compiled = compile(&snapshot);
    assert_eq!(
        compiled_mirror(&compiled.config.table).map(|(_, percent, _)| percent),
        Some(100)
    );
    assert_eq!(warnings(&compiled, WarningKind::InvalidAnnotation).len(), 1);
}

#[test]
fn a_malformed_mirror_backend_degrades_the_mirror_and_not_the_route() {
    // A typo in a shadow backend name must never take production traffic down.
    // That inversion is the single worst thing this feature could do.
    let snapshot = with_mirror(&[(ANNOTATION_MIRROR_BACKEND, "not-a-reference")]);
    let compiled = compile(&snapshot);

    assert_eq!(
        hit(&compiled.config.table, "app.example.com", "/"),
        Some("prod/api:80".to_owned()),
        "the route still serves"
    );
    assert!(compiled_mirror(&compiled.config.table).is_none());
    let rejected = warnings(&compiled, WarningKind::MirrorRejected);
    assert_eq!(rejected.len(), 1);
    assert!(rejected[0].detail.contains("not-a-reference"));
}

#[test]
fn a_mirror_naming_a_missing_service_serves_the_route_and_mirrors_nowhere() {
    // The Service does not exist, so the backend resolves empty. That is a
    // normal state during a rollout and must not degrade the route; the data
    // plane counts the copies it cannot make.
    let snapshot = with_mirror(&[(ANNOTATION_MIRROR_BACKEND, "prod/ghost:80")]);
    let compiled = compile(&snapshot);

    assert_eq!(
        hit(&compiled.config.table, "app.example.com", "/"),
        Some("prod/api:80".to_owned())
    );
    let table = &compiled.config.table;
    let mirror = table
        .match_request("app.example.com", "/")
        .and_then(|m| m.mirror())
        .expect("the mirror is attached even with no endpoints");
    assert!(table
        .backend(mirror.backend())
        .is_some_and(|b| b.endpoints().is_empty()));
    assert_eq!(warnings(&compiled, WarningKind::ServiceUnresolved).len(), 1);
}

#[test]
fn a_mirror_annotation_on_a_canary_ingress_is_refused_out_loud() {
    // Silently ignoring it leaves somebody watching a shadow backend that never
    // receives anything, with no way to find out why.
    let production = ours(ingress(
        "prod",
        "web",
        &[rule(Some("app.example.com"), &[path("/", "Prefix", "api", 80)])],
    ));
    let canary = annotate(
        annotate(
            annotate(
                ours(ingress(
                    "prod",
                    "web-canary",
                    &[rule(Some("app.example.com"), &[path("/", "Prefix", "api", 80)])],
                )),
                ANNOTATION_CANARY,
                "true",
            ),
            ANNOTATION_CANARY_WEIGHT,
            "10",
        ),
        ANNOTATION_MIRROR_BACKEND,
        "prod/shadow:80",
    );
    let snapshot = mirrorable()
        .with_ingress(created_at(production, 1))
        .with_ingress(created_at(canary, 2));

    let compiled = compile(&snapshot);
    assert!(
        compiled_mirror(&compiled.config.table).is_none(),
        "the canary's mirror annotation must not take effect"
    );
    let complaints = warnings(&compiled, WarningKind::InvalidAnnotation);
    assert!(
        complaints
            .iter()
            .any(|w| w.detail.contains("production Ingress")),
        "{complaints:?}"
    );
}

#[test]
fn a_mirror_applies_to_every_rule_of_its_ingress() {
    // The annotations are on the object, so an Ingress with three paths gets
    // three mirrors rather than one on whichever rule happened to be first.
    let object = annotate(
        ours(ingress(
            "prod",
            "web",
            &[rule(
                Some("app.example.com"),
                &[
                    path("/", "Prefix", "api", 80),
                    path("/v2", "Prefix", "api", 80),
                ],
            )],
        )),
        ANNOTATION_MIRROR_BACKEND,
        "prod/shadow:80",
    );
    let table = compile(&mirrorable().with_ingress(object)).config.table;

    for path in ["/", "/v2"] {
        assert!(
            table
                .match_request("app.example.com", path)
                .and_then(|m| m.mirror())
                .is_some(),
            "{path} has no mirror"
        );
    }
}

#[test]
fn a_mirror_changes_the_digest_so_editing_one_republishes() {
    let without = compile(&with_mirror(&[])).digest;
    let with = compile(&with_mirror(&[(ANNOTATION_MIRROR_BACKEND, "prod/shadow:80")])).digest;
    let resampled = compile(&with_mirror(&[
        (ANNOTATION_MIRROR_BACKEND, "prod/shadow:80"),
        (ANNOTATION_MIRROR_PERCENT, "50"),
    ]))
    .digest;

    assert_ne!(without, with, "adding a mirror is a configuration change");
    assert_ne!(with, resampled, "resampling one is too");
}

// ---------------------------------------------------------------------------
// Automatic promotion candidates
// ---------------------------------------------------------------------------

/// A production Ingress plus a canary carrying `annotations`.
fn canary_cluster(annotations: &[(&str, &str)]) -> ClusterSnapshot {
    let snapshot = backed(base(), "prod", "api", 80, 8080, &["10.0.0.1"]);
    let snapshot = backed(snapshot, "prod", "api-next", 80, 8080, &["10.0.1.1"]);

    let production = created_at(
        ours(ingress(
            "prod",
            "web",
            &[rule(Some("app.example.com"), &[path("/", "Prefix", "api", 80)])],
        )),
        1,
    );
    let mut canary = ours(ingress(
        "prod",
        "web-canary",
        &[rule(
            Some("app.example.com"),
            &[path("/", "Prefix", "api-next", 80)],
        )],
    ));
    canary = annotate(canary, ANNOTATION_CANARY, "true");
    for (key, value) in annotations {
        canary = annotate(canary, key, value);
    }
    snapshot
        .with_ingress(production)
        .with_ingress(created_at(canary, 2))
}

#[test]
fn a_canary_without_the_opt_in_is_not_a_promotion_candidate() {
    let compiled = compile(&canary_cluster(&[(ANNOTATION_CANARY_WEIGHT, "10")]));
    assert!(
        compiled.config.promotions.is_empty(),
        "promotion is opt-in and must stay that way"
    );
}

#[test]
fn an_opted_in_canary_is_compiled_into_a_promotion_target() {
    let compiled = compile(&canary_cluster(&[
        (ANNOTATION_CANARY_WEIGHT, "5"),
        (ANNOTATION_AUTO_PROMOTE, "true"),
    ]));

    assert_eq!(compiled.config.promotions.len(), 1);
    let target = &compiled.config.promotions[0];
    assert_eq!(target.ingress.to_string(), "prod/web-canary");
    assert_eq!(target.weight, 5);
    assert_eq!(target.policy.steps, DEFAULT_PROMOTE_STEPS);
    assert_eq!(target.policy.interval, DEFAULT_PROMOTE_INTERVAL);
    assert_eq!(
        target.routes,
        vec![PromotionRoute {
            host: "app.example.com".to_owned(),
            path: "/".to_owned(),
            path_type: PathType::Prefix,
        }],
        "the target names the production route, which is where the counters are"
    );
}

#[test]
fn a_promotion_target_carries_every_configured_threshold() {
    let compiled = compile(&canary_cluster(&[
        (ANNOTATION_CANARY_WEIGHT, "25"),
        (ANNOTATION_AUTO_PROMOTE, "true"),
        (ANNOTATION_AUTO_PROMOTE_INTERVAL, "30s"),
        (ANNOTATION_AUTO_PROMOTE_STEPS, "25,50,100"),
        (ANNOTATION_AUTO_PROMOTE_MAX_5XX, "2.5"),
        (ANNOTATION_AUTO_PROMOTE_MAX_LATENCY, "3"),
        (ANNOTATION_AUTO_PROMOTE_MIN_REQUESTS, "200"),
    ]));

    let target = &compiled.config.promotions[0];
    assert_eq!(target.weight, 25);
    assert_eq!(target.policy.interval, std::time::Duration::from_secs(30));
    assert_eq!(target.policy.steps, vec![25, 50, 100]);
    assert_eq!(target.policy.max_5xx_percent, 2.5);
    assert_eq!(target.policy.max_latency_factor, 3.0);
    assert_eq!(target.policy.min_requests, 200);
}

#[test]
fn an_orphaned_canary_is_not_a_promotion_candidate() {
    // It attached to no production route, so there are no counters to judge it
    // by. Handing it to the loop would mean looking for a route that is not in
    // the table every interval, forever.
    let snapshot = backed(base(), "prod", "api-next", 80, 8080, &["10.0.1.1"]);
    let canary = annotate(
        annotate(
            ours(ingress(
                "prod",
                "web-canary",
                &[rule(
                    Some("nobody.example.com"),
                    &[path("/", "Prefix", "api-next", 80)],
                )],
            )),
            ANNOTATION_CANARY,
            "true",
        ),
        ANNOTATION_AUTO_PROMOTE,
        "true",
    );
    let compiled = compile(&snapshot.with_ingress(canary));

    assert!(compiled.config.promotions.is_empty());
    assert_eq!(warnings(&compiled, WarningKind::CanaryOrphan).len(), 1);
}

#[test]
fn a_rolled_back_canary_is_still_reported_so_the_loop_can_refuse_it() {
    // The flap guard lives in the loop, not here: the target has to reach it
    // carrying the status, or a controller restart would re-arm a canary that
    // already failed once.
    let compiled = compile(&canary_cluster(&[
        (ANNOTATION_AUTO_PROMOTE, "true"),
        (
            ANNOTATION_AUTO_PROMOTE_STATUS,
            "rolled-back: 5xx 9.1% over 1%",
        ),
    ]));

    let target = &compiled.config.promotions[0];
    assert!(target.policy.rolled_back());
}

#[test]
fn promotion_annotations_change_the_digest() {
    // The loop reads its policy off the published generation, so a change
    // nobody publishes is a change nobody applies.
    let armed = compile(&canary_cluster(&[
        (ANNOTATION_CANARY_WEIGHT, "5"),
        (ANNOTATION_AUTO_PROMOTE, "true"),
    ]))
    .digest;
    let disarmed = compile(&canary_cluster(&[(ANNOTATION_CANARY_WEIGHT, "5")])).digest;
    let retuned = compile(&canary_cluster(&[
        (ANNOTATION_CANARY_WEIGHT, "5"),
        (ANNOTATION_AUTO_PROMOTE, "true"),
        (ANNOTATION_AUTO_PROMOTE_MAX_5XX, "5"),
    ]))
    .digest;

    assert_ne!(armed, disarmed);
    assert_ne!(armed, retuned);
}

#[test]
fn promotion_targets_come_out_in_a_deterministic_order() {
    // Two replicas must hand their loops the same list, and the plan they are
    // gathered from is a hash map.
    let snapshot = backed(base(), "prod", "api", 80, 8080, &["10.0.0.1"]);
    let snapshot = backed(snapshot, "prod", "api-next", 80, 8080, &["10.0.1.1"]);
    let mut snapshot = snapshot;

    for (index, name) in ["z-canary", "a-canary", "m-canary"].iter().enumerate() {
        let host = format!("{name}.example.com");
        snapshot = snapshot.with_ingress(created_at(
            ours(ingress(
                "prod",
                &format!("web-{name}"),
                &[rule(Some(&host), &[path("/", "Prefix", "api", 80)])],
            )),
            index as i64 * 2,
        ));
        let canary = annotate(
            annotate(
                ours(ingress(
                    "prod",
                    name,
                    &[rule(Some(&host), &[path("/", "Prefix", "api-next", 80)])],
                )),
                ANNOTATION_CANARY,
                "true",
            ),
            ANNOTATION_AUTO_PROMOTE,
            "true",
        );
        snapshot = snapshot.with_ingress(created_at(canary, index as i64 * 2 + 1));
    }

    let names: Vec<String> = compile(&snapshot)
        .config
        .promotions
        .iter()
        .map(|t| t.ingress.to_string())
        .collect();
    assert_eq!(
        names,
        vec!["prod/a-canary", "prod/m-canary", "prod/z-canary"]
    );
}

// ---------------------------------------------------------------------------
// backend-protocol
// ---------------------------------------------------------------------------

/// The protocol the compiled table will dial `host path` with.
fn protocol_of(table: &RouteTable, host: &str, path: &str) -> Option<BackendProtocol> {
    table.match_request(host, path).map(|m| m.backend().protocol())
}

/// One Ingress serving `example.com /`, carrying `backend-protocol: <value>`.
fn with_protocol(value: &str) -> ClusterSnapshot {
    let ingress = annotate(
        ours(ingress(
            "default",
            "web",
            &[rule(Some("example.com"), &[path("/", "Prefix", "web", 80)])],
        )),
        ANNOTATION_BACKEND_PROTOCOL,
        value,
    );
    backed(
        base().with_ingress(ingress),
        "default",
        "web",
        80,
        8080,
        &["10.0.0.1"],
    )
}

#[test]
fn a_backend_is_http1_without_the_annotation() {
    let snapshot = backed(
        base().with_ingress(ours(ingress(
            "default",
            "web",
            &[rule(Some("example.com"), &[path("/", "Prefix", "web", 80)])],
        ))),
        "default",
        "web",
        80,
        8080,
        &["10.0.0.1"],
    );
    let t = compile(&snapshot);
    assert_eq!(
        protocol_of(&t.config.table, "example.com", "/"),
        Some(BackendProtocol::Http1)
    );
    assert!(t.warnings.is_empty());
}

#[test]
fn grpc_compiles_to_an_h2c_backend() {
    let t = compile(&with_protocol("GRPC"));
    assert_eq!(
        protocol_of(&t.config.table, "example.com", "/"),
        Some(BackendProtocol::H2c)
    );
    assert!(
        t.warnings.is_empty(),
        "a supported value produces no warning: {:?}",
        t.warnings
    );
}

#[test]
fn the_protocol_is_read_case_insensitively_as_ingress_nginx_reads_it() {
    for spelling in ["grpc", "Grpc", " GRPC "] {
        let t = compile(&with_protocol(spelling));
        assert_eq!(
            protocol_of(&t.config.table, "example.com", "/"),
            Some(BackendProtocol::H2c),
            "{spelling:?}"
        );
    }
}

#[test]
fn an_unsupported_protocol_warns_and_leaves_the_backend_on_http1() {
    // Not silently HTTP: an operator who asked for GRPCS gets a line naming the
    // value, and the route still serves rather than the table failing to build.
    for value in ["GRPCS", "HTTPS", "AUTO_HTTP", "FCGI"] {
        let t = compile(&with_protocol(value));
        assert_eq!(
            protocol_of(&t.config.table, "example.com", "/"),
            Some(BackendProtocol::Http1),
            "{value} must not change the protocol"
        );
        let found = warnings(&t, WarningKind::InvalidAnnotation);
        assert_eq!(found.len(), 1, "{value} should produce exactly one warning");
        assert!(
            found[0].detail.contains(value),
            "the warning must name the value, got: {}",
            found[0].detail
        );
    }
}

#[test]
fn the_protocol_moves_the_digest_so_the_change_is_published() {
    // The rebuild loop suppresses a publish when the digest is unchanged, so an
    // annotation that does not reach the digest is an annotation that takes
    // effect only after some unrelated edit.
    let plain = compile(&with_protocol("HTTP")).digest;
    let grpc = compile(&with_protocol("GRPC")).digest;
    assert_ne!(plain, grpc, "flipping backend-protocol must republish");
}

#[test]
fn each_ingress_annotates_its_own_backends() {
    let rpc = annotate(
        ours(ingress(
            "default",
            "rpc",
            &[rule(Some("example.com"), &[path("/rpc", "Prefix", "rpc", 80)])],
        )),
        ANNOTATION_BACKEND_PROTOCOL,
        "GRPC",
    );
    let web = ours(ingress(
        "default",
        "web",
        &[rule(Some("example.com"), &[path("/", "Prefix", "web", 80)])],
    ));
    let snapshot = backed(
        backed(
            base().with_ingress(rpc).with_ingress(web),
            "default",
            "rpc",
            80,
            8080,
            &["10.0.0.1"],
        ),
        "default",
        "web",
        80,
        8080,
        &["10.0.0.2"],
    );

    let t = compile(&snapshot);
    assert_eq!(
        protocol_of(&t.config.table, "example.com", "/rpc"),
        Some(BackendProtocol::H2c)
    );
    assert_eq!(
        protocol_of(&t.config.table, "example.com", "/"),
        Some(BackendProtocol::Http1)
    );
}

#[test]
fn two_ingresses_disagreeing_about_one_service_get_the_first_claim_and_a_warning() {
    // A backend is one Service port however many Ingresses point at it, so the
    // two cannot both be satisfied. First by route order wins — `/a` before
    // `/b` — and the loser is named rather than left wondering.
    let grpc = annotate(
        ours(ingress(
            "default",
            "a",
            &[rule(Some("example.com"), &[path("/a", "Prefix", "web", 80)])],
        )),
        ANNOTATION_BACKEND_PROTOCOL,
        "GRPC",
    );
    let plain = ours(ingress(
        "default",
        "b",
        &[rule(Some("example.com"), &[path("/b", "Prefix", "web", 80)])],
    ));
    let snapshot = backed(
        base().with_ingress(grpc).with_ingress(plain),
        "default",
        "web",
        80,
        8080,
        &["10.0.0.1"],
    );

    let t = compile(&snapshot);
    let conflicts = warnings(&t, WarningKind::BackendProtocolConflict);
    assert_eq!(conflicts.len(), 1, "{:?}", t.warnings);
    assert_eq!(conflicts[0].subject.name, "b", "the later claim is the one reported");
    // One backend, and it is the one the first route asked for.
    assert_eq!(
        protocol_of(&t.config.table, "example.com", "/a"),
        Some(BackendProtocol::H2c)
    );
    assert_eq!(
        protocol_of(&t.config.table, "example.com", "/b"),
        Some(BackendProtocol::H2c),
        "the same Service port is the same backend, so it has one protocol"
    );
}

#[test]
fn two_ingresses_agreeing_about_one_service_produce_no_warning() {
    let one = annotate(
        ours(ingress(
            "default",
            "a",
            &[rule(Some("example.com"), &[path("/a", "Prefix", "web", 80)])],
        )),
        ANNOTATION_BACKEND_PROTOCOL,
        "GRPC",
    );
    let two = annotate(
        ours(ingress(
            "default",
            "b",
            &[rule(Some("example.com"), &[path("/b", "Prefix", "web", 80)])],
        )),
        ANNOTATION_BACKEND_PROTOCOL,
        "grpc",
    );
    let snapshot = backed(
        base().with_ingress(one).with_ingress(two),
        "default",
        "web",
        80,
        8080,
        &["10.0.0.1"],
    );

    let t = compile(&snapshot);
    assert!(warnings(&t, WarningKind::BackendProtocolConflict).is_empty());
}

#[test]
fn a_canary_carries_its_own_protocol() {
    // The canary is a separate object with its own annotations, which is what
    // makes a gRPC rollout in front of an HTTP/1.1 production Service possible.
    let mut snapshot = canary_pair(&[(ANNOTATION_BACKEND_PROTOCOL, "GRPC")]);
    snapshot = backed(snapshot, "default", "stable", 80, 8080, &["10.0.0.1"]);
    snapshot = backed(snapshot, "default", "canary", 80, 8080, &["10.0.0.2"]);

    let t = compile(&snapshot);
    let table = &t.config.table;
    let matched = table.match_request("example.com", "/").expect("a route");
    assert_eq!(matched.backend().protocol(), BackendProtocol::Http1);
    let canary = matched.canary().expect("a canary");
    assert_eq!(
        table.backend(canary.backend()).expect("a backend").protocol(),
        BackendProtocol::H2c
    );
}

#[test]
fn the_cluster_default_backend_stays_http1_when_no_route_names_it() {
    let opts = ControllerOpts {
        default_backend: Some("default/fallback:80".parse().expect("a service ref")),
        ..ControllerOpts::default()
    };
    let snapshot = backed(
        with_protocol("GRPC"),
        "default",
        "fallback",
        80,
        8080,
        &["10.0.0.9"],
    );

    let t = compile_with(&snapshot, &opts);
    let table = &t.config.table;
    let id = table.default_backend().expect("a default backend");
    assert_eq!(
        table.backend(id).expect("a backend").protocol(),
        BackendProtocol::Http1
    );
}
