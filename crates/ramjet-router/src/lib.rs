//! Kubernetes Ingress route matching, with no I/O and no runtime.
//!
//! The controller compiles configuration into an immutable [`RouteTable`]; the
//! data plane reads it through a [`SharedRouteTable`] and calls
//! [`match_request`](RouteTable::match_request). That is the whole interface.
//!
//! # Why there is no lock
//!
//! ingress-nginx reacts to a configuration change by regenerating `nginx.conf`
//! and reloading. A reload starts new workers, drains the old ones, and along
//! the way resets upstream state and severs connections that were supposed to
//! be long-lived. The cost is proportional to how much traffic you are
//! carrying, which is exactly backwards.
//!
//! Here a configuration change builds a new table and stores one pointer.
//! Readers do a single atomic load per request and then work with an immutable
//! snapshot; there is no `RwLock` to contend on, no reload, and no draining. A
//! request that started under generation 7 finishes against generation 7 even
//! if 8 is published mid-flight, because it holds its snapshot. The mutable
//! load-balancer counters deliberately do *not* live in the table — see
//! [`BackendStats`] — so a rebuild does not make backends forget how many
//! requests they are currently serving.
//!
//! # Sans-io
//!
//! Nothing here opens a socket, spawns a task, or knows what rustls is.
//! Certificates are opaque [`CertifiedKeyHandle`]s, randomness is passed in as
//! a `u64`, and canary decisions take borrowed header values rather than a
//! header collection. The proxy crate supplies all of it. This is what makes
//! the matcher testable against string literals and benchmarkable without a
//! network.
//!
//! # Hot path
//!
//! [`RouteTable::match_request`] performs no heap allocation, and
//! `tests/no_alloc.rs` enforces that with a counting global allocator rather
//! than leaving it as a claim in a comment. Host normalization borrows in the
//! common case and falls back to a stack buffer; lookups and results borrow
//! from the table.
//!
//! ```
//! use ramjet_router::{Endpoint, LbPolicy, PathType, RouteTableBuilder, SharedRouteTable};
//!
//! let mut builder = RouteTableBuilder::new();
//! builder.backend(
//!     "api",
//!     LbPolicy::RoundRobin,
//!     vec![Endpoint::new("10.0.0.1:8080".parse()?)],
//! )?;
//! builder.route(Some("example.com"), "/api", PathType::Prefix, "api")?;
//! let shared = SharedRouteTable::new(builder.build()?);
//!
//! let table = shared.load();
//! let hit = table.match_request("Example.COM:8443", "/api/v1/users").expect("a route");
//! assert_eq!(hit.backend().name(), "api");
//!
//! // `/apiary` is not a path-element prefix of `/api`, so it does not match.
//! assert!(table.match_request("example.com", "/apiary").is_none());
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```

#![forbid(unsafe_code)]
#![deny(missing_docs)]

mod backend;
mod builder;
mod canary;
mod host;
mod mirror;
mod path;
mod stats;
mod table;
mod tls;

pub use backend::{select_endpoint, Backend, BackendId, BackendProtocol, Endpoint, LbPolicy};
pub use builder::{
    BackendOptions, BuildError, CanaryRules, MirrorRules, RouteOptions, RouteTableBuilder,
};
pub use canary::CanarySpec;
pub use mirror::{MirrorSpec, MIRROR_PERCENT_TOTAL};
pub use path::{PathRule, PathType};
pub use stats::{
    BackendSlot, BackendStats, InflightGuard, RouteCounters, RouteIdentity, RouteSlot, RouteStats,
    RouteTotals, ROUTE_STAT_SHARDS,
};
pub use table::{
    HostMatch, MatchResult, RouteHost, RouteTable, SharedRouteTable, VirtualHost,
};
pub use tls::{CertifiedKeyHandle, SniMap};

#[cfg(test)]
mod tests {
    use super::*;

    /// The table is published through an `ArcSwap` and read from every worker,
    /// so this is a load-bearing property, not a formality.
    #[test]
    fn route_table_is_send_and_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<RouteTable>();
        assert_send_sync::<SharedRouteTable>();
        assert_send_sync::<BackendStats>();
    }

    #[test]
    fn a_rule_stays_small() {
        // Rules are scanned linearly, so their size decides how many fit in a
        // cache line. If this grows, check the bench before accepting it.
        assert!(
            std::mem::size_of::<PathRule>() <= 48,
            "PathRule grew to {} bytes",
            std::mem::size_of::<PathRule>()
        );
    }
}
