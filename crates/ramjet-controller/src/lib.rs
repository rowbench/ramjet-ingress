//! `ramjet-controller` is the ramjet-ingress control plane.
//!
//! It watches the Kubernetes API for `Ingress`, `Service`,
//! `Endpoints`/`EndpointSlice`, and `Secret` objects, translates what it
//! observes into an immutable `RouteTable` via
//! `ramjet_router::RouteTableBuilder`, and publishes each new table by
//! swapping the data plane's `ArcSwap`.
//!
//! The controller is the ONLY writer, and it never blocks the data plane: a
//! rebuild is a pure function from the currently watched objects to a new
//! `RouteTable`, and publication is a single atomic pointer store picked up
//! by `ramjet-proxy` on its next `load()`. There is no partial state for a
//! request to observe mid-rebuild.
//!
//! Contrast this with ingress-nginx, which regenerates `nginx.conf` from
//! scratch and reloads the worker process on every change — dropping
//! long-lived connections and resetting upstream connection state each
//! time. Here a config change is a pointer swap, not a process reload.

// Stub crate: no implementations yet, only the planned module skeleton and
// doc comments describing the intended API surface. Remove once real types
// land and start triggering genuine dead-code warnings.
#![allow(dead_code)]

pub mod watch {
    //! Kubernetes informer/watch machinery.
    //!
    //! Planned: informers for the watched object kinds, resync handling,
    //! and resource-version bookkeeping so watches can resume cleanly
    //! after a disconnect without missing events.
}

pub mod translate {
    //! Translation from Ingress objects into `RouteTableBuilder` calls.
    //!
    //! Planned: `Ingress` objects plus ingress-nginx-compatible
    //! annotations become calls against `ramjet_router::RouteTableBuilder`.
    //! Annotation families planned for support: canary
    //! (`canary-by-header`, `canary-by-header-value`,
    //! `canary-by-header-pattern`, `canary-by-cookie`, `canary-weight`),
    //! rewrite, upstream hashing, and TLS.
}

pub mod endpoints {
    //! Endpoint discovery from `EndpointSlice` objects.
    //!
    //! Planned: translation of `EndpointSlice` objects into the `Backend`
    //! endpoint lists consumed by the route table, filtering for
    //! readiness and carrying topology hints through to backend
    //! selection.
}

pub mod secrets {
    //! TLS secret handling.
    //!
    //! Planned: translation of TLS `Secret` objects into certificate
    //! handles consumed by the router's `SniMap`, so a new or rotated
    //! certificate flows into the same route table rebuild as any other
    //! change.
}

pub mod status {
    //! Status reporting and leader election.
    //!
    //! Planned: writing `status.loadBalancer` back onto watched `Ingress`
    //! objects, plus leader election so that in a multi-replica
    //! deployment only one replica writes status at a time.
}
