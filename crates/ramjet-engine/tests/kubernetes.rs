//! Live reconfiguration: what Kubernetes mode actually asks of this engine.
//!
//! # Why there is no Kubernetes here
//!
//! Because there is none in the engine either. The controller compiles a
//! generation, `ramjet-ingressd` parses its certificates, and
//! [`GenerationHistory`](ramjet_proxy::GenerationHistory) publishes the result
//! into a [`SharedRouteTable`](ramjet_router::SharedRouteTable) and a
//! [`CertStore`](ramjet_proxy::CertStore). Everything below that line is the
//! same two `ArcSwap`s whether the bytes came from an API server or a file.
//!
//! So what is tested is the seam: a generation published while traffic is
//! flowing, a certificate that arrives after the listener bound, a rollback pin
//! that puts an old table back on the wire, and the ordering rule that keeps a
//! rotation from dropping handshakes. Those are the four things that would
//! break if this engine had cached anything it should not have — and the reason
//! it used to refuse Kubernetes mode at startup was precisely the fear that it
//! had.

mod common;

use std::collections::HashMap;
use std::sync::Arc;

use common::*;
use ramjet_proxy::{CertStore, GenerationHistory};
use ramjet_router::{
    CertifiedKeyHandle, Endpoint, LbPolicy, PathType, RouteTable, RouteTableBuilder,
    SharedRouteTable,
};

/// A table at `generation` routing `host` to `upstream`.
fn table_at(generation: u64, host: &str, upstream: std::net::SocketAddr) -> Arc<RouteTable> {
    let mut builder = RouteTableBuilder::new();
    builder.generation(generation);
    builder
        .backend("app", LbPolicy::RoundRobin, vec![Endpoint::new(upstream)])
        .expect("a valid backend");
    builder
        .route(Some(host), "/", PathType::Prefix, "app")
        .expect("a valid route");
    Arc::new(builder.build().expect("a valid table"))
}

/// The same, with a certificate registered for `host`.
fn tls_table_at(
    generation: u64,
    host: &str,
    upstream: std::net::SocketAddr,
    handle: u64,
) -> Arc<RouteTable> {
    let mut builder = RouteTableBuilder::new();
    builder.generation(generation);
    builder
        .backend("app", LbPolicy::RoundRobin, vec![Endpoint::new(upstream)])
        .expect("a valid backend");
    builder
        .route(Some(host), "/", PathType::Prefix, "app")
        .expect("a valid route");
    builder
        .certificate(host, Arc::new(CertifiedKeyHandle::new(handle)))
        .expect("a valid certificate name");
    Arc::new(builder.build().expect("a valid table"))
}

/// The diff a record carries, which nothing here reads.
///
/// `GenerationHistory` takes it as opaque JSON to hand back on
/// `/admin/generations`; the publication decision does not look at it.
fn empty_diff() -> Arc<serde_json::Value> {
    Arc::new(serde_json::Value::Object(serde_json::Map::new()))
}

#[test]
fn a_generation_published_mid_flight_takes_effect_on_the_next_request() {
    // The whole reason this engine used to refuse Kubernetes mode. A table read
    // once at startup would keep serving the old endpoints, and it would look
    // like it worked right up until the first deployment.
    let first = echo();
    let second = echo();

    let routes = Arc::new(SharedRouteTable::new(
        Arc::try_unwrap(table_at(1, "app.example.com", first.addr))
            .expect("a sole owner"),
    ));
    let proxy = Proxy::with_routes(Arc::clone(&routes), |_config, _routes| {});

    assert_eq!(get(proxy.addr, "/", "app.example.com").status, 200);
    assert_eq!(first.seen.requests(), 1);
    assert_eq!(second.seen.requests(), 0);

    // What the applier does, minus the API server.
    routes.store_shared(table_at(2, "app.example.com", second.addr));

    assert_eq!(get(proxy.addr, "/", "app.example.com").status, 200);
    assert_eq!(
        second.seen.requests(),
        1,
        "the new generation did not reach the data plane"
    );
    assert_eq!(first.seen.requests(), 1, "the old endpoint kept no traffic");
}

#[test]
fn a_keep_alive_connection_follows_the_new_generation_too() {
    // Per-request rather than per-connection, deliberately. A keep-alive
    // connection from a busy client can live for hours, and pinning it to the
    // generation it was accepted under means a route deleted at 09:00 is still
    // serving at 17:00 — a correctness bug wearing a performance costume.
    let first = echo();
    let second = echo();

    let routes = Arc::new(SharedRouteTable::new(
        Arc::try_unwrap(table_at(1, "app.example.com", first.addr))
            .expect("a sole owner"),
    ));
    let proxy = Proxy::with_routes(Arc::clone(&routes), |_config, _routes| {});

    let mut client = Client::connect(proxy.addr);
    assert_eq!(
        client
            .send(b"GET /1 HTTP/1.1\r\nHost: app.example.com\r\n\r\n")
            .status,
        200
    );

    routes.store_shared(table_at(2, "app.example.com", second.addr));

    assert_eq!(
        client
            .send(b"GET /2 HTTP/1.1\r\nHost: app.example.com\r\n\r\n")
            .status,
        200
    );
    assert_eq!(
        second.seen.requests(),
        1,
        "the second request on an open connection used the old table"
    );
}

#[test]
fn the_generation_gauge_follows_what_is_serving() {
    let upstream = echo();
    let routes = Arc::new(SharedRouteTable::new(
        Arc::try_unwrap(table_at(7, "app.example.com", upstream.addr))
            .expect("a sole owner"),
    ));
    let proxy = Proxy::with_routes(Arc::clone(&routes), |_config, _routes| {});

    let text = proxy.admin("/metrics").text();
    assert_eq!(counter(&text, "ramjet_route_table_generation"), 7, "{text}");

    routes.store_shared(table_at(9, "app.example.com", upstream.addr));
    let text = proxy.admin("/metrics").text();
    assert_eq!(counter(&text, "ramjet_route_table_generation"), 9, "{text}");
}

#[test]
fn a_certificate_arriving_after_the_listener_bound_starts_serving_tls() {
    // Kubernetes mode binds 443 over an empty store on purpose: the
    // certificates are arriving over a watch that has not finished its first
    // list, and refusing to bind would mean a restart could never recover a
    // cluster's HTTPS.
    const HANDLE: u64 = 42;
    let upstream = echo();

    let routes = Arc::new(SharedRouteTable::new(
        Arc::try_unwrap(table_at(1, "app.example.com", upstream.addr))
            .expect("a sole owner"),
    ));
    let certs = Arc::new(CertStore::new());
    let proxy = {
        let certs = Arc::clone(&certs);
        Proxy::with_routes(Arc::clone(&routes), move |config, routes| {
            let resolver = Arc::new(ramjet_proxy::SniResolver::new(Arc::clone(routes), certs));
            config.https = Some(std::net::SocketAddr::from(([127, 0, 0, 1], 0)));
            config.tls = Some(Arc::new(
                ramjet_proxy::tls::h1_server_config(resolver).expect("a server config"),
            ));
        })
    };

    // Nothing in the store yet: the handshake has no certificate to offer.
    let socket = std::net::TcpStream::connect(proxy.tls()).expect("the listener accepts");
    drop(socket);

    // The applier's order, and it matters: certificates first, then the table
    // that references them. The other way round leaves an SniMap entry whose id
    // is not in the store, which rustls turns into a failed handshake for as
    // long as the gap lasts.
    let mut store = HashMap::new();
    store.insert(HANDLE, Arc::new(certificate_for(&["app.example.com"])));
    certs.publish(store);
    routes.store_shared(tls_table_at(2, "app.example.com", upstream.addr, HANDLE));

    let response = https_get(proxy.tls(), "app.example.com", "/");
    assert_eq!(response.status, 200, "TLS did not start after the rotation");
    assert_eq!(response.header("echo-x-forwarded-proto"), Some("https"));
}

#[test]
fn a_rotated_certificate_is_served_without_dropping_the_listener() {
    const FIRST: u64 = 1;
    const SECOND: u64 = 2;
    let upstream = echo();

    let original = Arc::new(certificate_for(&["app.example.com"]));
    let mut store = HashMap::new();
    store.insert(FIRST, Arc::clone(&original));

    let routes = Arc::new(SharedRouteTable::new(
        Arc::try_unwrap(tls_table_at(1, "app.example.com", upstream.addr, FIRST))
            .expect("a sole owner"),
    ));
    let certs = Arc::new(CertStore::with_certs(store));
    let proxy = {
        let certs = Arc::clone(&certs);
        Proxy::with_routes(Arc::clone(&routes), move |config, routes| {
            let resolver = Arc::new(ramjet_proxy::SniResolver::new(Arc::clone(routes), certs));
            config.https = Some(std::net::SocketAddr::from(([127, 0, 0, 1], 0)));
            config.tls = Some(Arc::new(
                ramjet_proxy::tls::h1_server_config(resolver).expect("a server config"),
            ));
        })
    };

    let mut client = tls_connect(proxy.tls(), "app.example.com", tls_client_config());
    client.handshake();
    assert_eq!(
        client.peer_certificates().first().map(|c| c.as_slice()),
        Some(original.cert[0].as_ref())
    );

    // A new Secret: a new content-addressed id, a new key, and a table that
    // points at it.
    let rotated = Arc::new(certificate_for(&["app.example.com"]));
    let mut store = HashMap::new();
    store.insert(SECOND, Arc::clone(&rotated));
    certs.publish(store);
    routes.store_shared(tls_table_at(2, "app.example.com", upstream.addr, SECOND));

    let mut client = tls_connect(proxy.tls(), "app.example.com", tls_client_config());
    client.handshake();
    assert_eq!(
        client.peer_certificates().first().map(|c| c.as_slice()),
        Some(rotated.cert[0].as_ref()),
        "the rotated certificate was not picked up"
    );
    assert_eq!(https_get(proxy.tls(), "app.example.com", "/").status, 200);
}

#[test]
fn a_rollback_pin_puts_the_old_table_back_on_the_wire() {
    // The generation history is the publication gate on both engines, because
    // it is where "pinned" is defined. This engine reads whatever ends up in
    // the `ArcSwap`, which is the only thing it has to get right.
    let first = echo();
    let second = echo();

    let routes = Arc::new(SharedRouteTable::new(
        Arc::try_unwrap(table_at(1, "app.example.com", first.addr))
            .expect("a sole owner"),
    ));
    let certs = Arc::new(CertStore::new());
    let history = Arc::new(GenerationHistory::new(
        Arc::clone(&routes),
        Arc::clone(&certs),
        8,
    ));
    let proxy = Proxy::with_routes(Arc::clone(&routes), |_config, _routes| {});

    // Two generations, both applied.
    for (generation, upstream) in [(1u64, first.addr), (2, second.addr)] {
        assert!(history.record(
            generation,
            generation,
            empty_diff(),
            table_at(generation, "app.example.com", upstream),
            Arc::new(HashMap::new()),
        ));
    }
    assert_eq!(get(proxy.addr, "/", "app.example.com").status, 200);
    assert_eq!(second.seen.requests(), 1, "generation 2 is serving");

    // Roll back to the first.
    history.pin(1).expect("generation 1 is in the ring");
    assert_eq!(history.serving(), 1);
    assert_eq!(get(proxy.addr, "/", "app.example.com").status, 200);
    assert_eq!(
        first.seen.requests(),
        1,
        "the pinned generation is not serving traffic"
    );

    // A generation compiled while the pin is held is recorded, not published.
    let third = echo();
    assert!(!history.record(
        3,
        3,
        empty_diff(),
        table_at(3, "app.example.com", third.addr),
        Arc::new(HashMap::new()),
    ));
    assert_eq!(get(proxy.addr, "/", "app.example.com").status, 200);
    assert_eq!(third.seen.requests(), 0, "a pin held back generation 3");
    assert_eq!(first.seen.requests(), 2);

    // Releasing it jumps to the state of the cluster, not to what was queued.
    history.unpin().expect("the pin is released");
    assert_eq!(get(proxy.addr, "/", "app.example.com").status, 200);
    assert_eq!(
        third.seen.requests(),
        1,
        "releasing the pin did not republish"
    );
}

#[test]
fn the_pinned_gauge_says_why_a_replica_is_frozen() {
    // Without it, a replica held on an old generation is indistinguishable from
    // one whose control plane has stopped — and those want very different
    // pages.
    let upstream = echo();
    let routes = Arc::new(SharedRouteTable::new(
        Arc::try_unwrap(table_at(1, "app.example.com", upstream.addr))
            .expect("a sole owner"),
    ));
    let proxy = Proxy::with_routes(Arc::clone(&routes), |_config, _routes| {});

    // The engine's own admin listener has no history to read and reports zero,
    // which is the honest answer for a data plane nothing can roll back.
    let text = proxy.admin("/metrics").text();
    assert_eq!(counter(&text, "ramjet_pinned"), 0, "{text}");
    assert!(
        text.contains("# TYPE ramjet_pinned gauge"),
        "the series must exist whether or not anything can set it:\n{text}"
    );
}

#[test]
fn an_empty_table_serves_404_rather_than_refusing_to_start() {
    // Kubernetes mode starts with an empty table and fills it in from a watch.
    // A data plane that would not run without routes could never come back
    // after a restart.
    let routes = Arc::new(SharedRouteTable::new(
        RouteTableBuilder::new().build().expect("an empty table"),
    ));
    let proxy = Proxy::with_routes(Arc::clone(&routes), |_config, _routes| {});

    assert_eq!(get(proxy.addr, "/", "app.example.com").status, 404);

    let upstream = echo();
    routes.store_shared(table_at(1, "app.example.com", upstream.addr));
    assert_eq!(get(proxy.addr, "/", "app.example.com").status, 200);
}
