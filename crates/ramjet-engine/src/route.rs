//! Choosing a backend and an endpoint, using the same router as the hyper
//! engine.
//!
//! There is deliberately no routing *logic* in this crate. `ramjet_router`
//! owns host normalisation, path precedence, canary arithmetic and load
//! balancing, and both engines call into it the same way with the same
//! arguments. What lives here is the glue — pulling a host and a path out of a
//! parsed head, and reading canary inputs out of headers and cookies — plus the
//! retry policy, which the router has no opinion about.
//!
//! One snapshot is loaded per request and everything below reads through it, so
//! a configuration published mid-request cannot change the answer halfway.

use ramjet_router::{select_endpoint, Backend, Endpoint, RouteTable};

use crate::codec::{Framing, Head};
use crate::headers;
use crate::rng;

/// What a request was routed to.
#[derive(Debug, Clone, Copy)]
pub struct Route<'t> {
    /// The backend that will serve it.
    pub backend: &'t Backend,
    /// The endpoint chosen first. Retries walk forward from here.
    pub index: usize,
    /// That endpoint.
    pub endpoint: &'t Endpoint,
}

/// Pick the backend for a request, honouring any canary on the matched rule.
///
/// Returns `None` when nothing matched and there is no default backend, which
/// is the 404 path.
pub fn select_backend<'t>(
    table: &'t RouteTable,
    host: &str,
    path: &str,
    head: &Head,
    buf: &[u8],
) -> Option<&'t Backend> {
    let matched = table.match_request(host, path)?;
    let Some(canary) = matched.canary() else {
        return Some(matched.backend());
    };

    let header_value = canary
        .header_name()
        .and_then(|name| head.header(buf, name.as_bytes()))
        .and_then(|value| std::str::from_utf8(value).ok());
    let cookie_value = canary
        .cookie_name()
        .and_then(|name| headers::cookie_value(head, buf, name));
    let roll = rng::below(canary.weight_total());

    if canary.decide(header_value, cookie_value, roll) {
        // A canary naming a backend that is no longer in the table is a
        // configuration that changed under a live request, not a reason to
        // fail one: serve production.
        table.backend(canary.backend()).or(Some(matched.backend()))
    } else {
        Some(matched.backend())
    }
}

/// Pick the first endpoint, by the backend's own load-balancing policy.
///
/// Called once per request, before any attempt. Retries do not consult the
/// policy again — they walk forward — which keeps a round-robin cursor
/// advancing once per request rather than once per attempt.
pub fn first_endpoint<'t>(table: &'t RouteTable, backend: &'t Backend) -> Option<Route<'t>> {
    let stats = table.stats();
    let (index, endpoint) = select_endpoint(backend, stats, rng::next_u64())?;
    Some(Route {
        backend,
        index,
        endpoint,
    })
}

/// The endpoint for attempt number `attempt`, counting from the first choice.
pub fn endpoint_at(backend: &Backend, first: usize, attempt: usize) -> Option<&Endpoint> {
    let endpoints = backend.endpoints();
    if endpoints.is_empty() {
        return None;
    }
    endpoints.get((first + attempt) % endpoints.len())
}

/// How many endpoints this request may be tried against.
///
/// The limit is the **body**, not the method. A request with bytes to send
/// cannot be replayed, because the first attempt may already have written some
/// of them upstream and nothing buffers them for a second try — so an
/// empty-bodied `POST` fails over and a `GET` with a body does not. That is the
/// same trade the hyper engine makes, and it is worth naming: buffering request
/// bodies to make everything retryable would put a copy of every upload in
/// memory to save a rare 502.
pub fn attempts(framing: Framing, endpoints: usize, max_attempts: usize) -> usize {
    if framing == Framing::Empty {
        endpoints.min(max_attempts).max(1)
    } else {
        1
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::parse_request_head;
    use ramjet_router::{CanaryRules, Endpoint, LbPolicy, PathType, RouteTableBuilder};
    use std::net::SocketAddr;

    fn addr(last: u8) -> SocketAddr {
        SocketAddr::from(([127, 0, 0, 1], 9000 + u16::from(last)))
    }

    fn head_of(wire: &[u8]) -> (Head, Vec<u8>) {
        let mut head = Head::default();
        let buf = wire.to_vec();
        assert!(parse_request_head(&buf, &mut head).expect("valid"));
        (head, buf)
    }

    fn table_with_canary(rules: CanaryRules<'_>) -> RouteTable {
        let mut builder = RouteTableBuilder::new();
        builder
            .backend("prod", LbPolicy::RoundRobin, vec![Endpoint::new(addr(1))])
            .expect("a valid backend");
        builder
            .backend("canary", LbPolicy::RoundRobin, vec![Endpoint::new(addr(2))])
            .expect("a valid backend");
        builder
            .canary_route(
                Some("app.example.com"),
                "/",
                PathType::Prefix,
                "prod",
                &rules,
            )
            .expect("a valid canary route");
        builder.build().expect("a valid table")
    }

    #[test]
    fn a_plain_route_goes_to_its_backend() {
        let mut builder = RouteTableBuilder::new();
        builder
            .backend("prod", LbPolicy::RoundRobin, vec![Endpoint::new(addr(1))])
            .expect("a valid backend");
        builder
            .route(Some("app.example.com"), "/", PathType::Prefix, "prod")
            .expect("a valid route");
        let table = builder.build().expect("a valid table");
        let (head, buf) = head_of(b"GET / HTTP/1.1\r\nHost: app.example.com\r\n\r\n");
        let backend =
            select_backend(&table, "app.example.com", "/", &head, &buf).expect("a backend");
        assert_eq!(backend.name(), "prod");
    }

    #[test]
    fn an_unmatched_host_has_no_backend() {
        let mut builder = RouteTableBuilder::new();
        builder
            .backend("prod", LbPolicy::RoundRobin, vec![Endpoint::new(addr(1))])
            .expect("a valid backend");
        builder
            .route(Some("app.example.com"), "/", PathType::Prefix, "prod")
            .expect("a valid route");
        let table = builder.build().expect("a valid table");
        let (head, buf) = head_of(b"GET / HTTP/1.1\r\nHost: nope.invalid\r\n\r\n");
        assert!(select_backend(&table, "nope.invalid", "/", &head, &buf).is_none());
    }

    #[test]
    fn a_canary_header_diverts_and_a_non_matching_one_does_not() {
        let table = table_with_canary(CanaryRules {
            backend: "canary",
            header: Some("x-canary"),
            weight: 0,
            ..Default::default()
        });
        for (header, expected) in [("always", "canary"), ("never", "prod"), ("maybe", "prod")] {
            let wire =
                format!("GET / HTTP/1.1\r\nHost: app.example.com\r\nX-Canary: {header}\r\n\r\n");
            let (head, buf) = head_of(wire.as_bytes());
            let backend =
                select_backend(&table, "app.example.com", "/", &head, &buf).expect("a backend");
            assert_eq!(backend.name(), expected, "x-canary: {header}");
        }
    }

    #[test]
    fn a_canary_cookie_decides_when_no_header_rule_does() {
        let table = table_with_canary(CanaryRules {
            backend: "canary",
            cookie: Some("canary"),
            weight: 0,
            ..Default::default()
        });
        let wire = b"GET / HTTP/1.1\r\nHost: app.example.com\r\n\
                     Cookie: session=abc; canary=always; theme=dark\r\n\r\n";
        let (head, buf) = head_of(wire);
        let backend =
            select_backend(&table, "app.example.com", "/", &head, &buf).expect("a backend");
        assert_eq!(backend.name(), "canary");
    }

    #[test]
    fn a_weight_splits_traffic_within_statistical_bounds() {
        let table = table_with_canary(CanaryRules {
            backend: "canary",
            weight: 25,
            weight_total: 100,
            ..Default::default()
        });
        let (head, buf) = head_of(b"GET / HTTP/1.1\r\nHost: app.example.com\r\n\r\n");
        let diverted = (0..600)
            .filter(|_| {
                select_backend(&table, "app.example.com", "/", &head, &buf)
                    .expect("a backend")
                    .name()
                    == "canary"
            })
            .count();
        // 25% of 600 is 150; the same bounds the hyper engine's test uses.
        assert!(
            (100..=200).contains(&diverted),
            "{diverted} of 600 diverted, expected about 150"
        );
    }

    #[test]
    fn a_zero_weight_never_diverts_and_a_full_weight_always_does() {
        for (weight, expected) in [(0u32, "prod"), (100, "canary")] {
            let table = table_with_canary(CanaryRules {
                backend: "canary",
                weight,
                weight_total: 100,
                ..Default::default()
            });
            let (head, buf) = head_of(b"GET / HTTP/1.1\r\nHost: app.example.com\r\n\r\n");
            for _ in 0..200 {
                let backend =
                    select_backend(&table, "app.example.com", "/", &head, &buf).expect("backend");
                assert_eq!(backend.name(), expected, "weight {weight}");
            }
        }
    }

    #[test]
    fn retries_walk_forward_and_wrap() {
        let mut builder = RouteTableBuilder::new();
        builder
            .backend(
                "prod",
                LbPolicy::RoundRobin,
                vec![
                    Endpoint::new(addr(1)),
                    Endpoint::new(addr(2)),
                    Endpoint::new(addr(3)),
                ],
            )
            .expect("a valid backend");
        builder
            .route(Some("a"), "/", PathType::Prefix, "prod")
            .expect("a valid route");
        let table = builder.build().expect("a valid table");
        let backend = &table.backends()[0];

        // Starting at the last endpoint, the next two attempts wrap around
        // rather than running off the end.
        assert_eq!(endpoint_at(backend, 2, 0).map(|e| e.addr), Some(addr(3)));
        assert_eq!(endpoint_at(backend, 2, 1).map(|e| e.addr), Some(addr(1)));
        assert_eq!(endpoint_at(backend, 2, 2).map(|e| e.addr), Some(addr(2)));
    }

    #[test]
    fn only_a_body_free_request_may_be_retried() {
        // Three endpoints, a limit of three: an empty body gets all three.
        assert_eq!(attempts(Framing::Empty, 3, 3), 3);
        // The limit binds.
        assert_eq!(attempts(Framing::Empty, 5, 3), 3);
        // Anything with bytes to send gets exactly one.
        assert_eq!(attempts(Framing::Length(1), 3, 3), 1);
        assert_eq!(attempts(Framing::Chunked, 3, 3), 1);
        assert_eq!(attempts(Framing::UntilClose, 3, 3), 1);
        // Never zero, whatever the configuration says.
        assert_eq!(attempts(Framing::Empty, 0, 0), 1);
    }
}
