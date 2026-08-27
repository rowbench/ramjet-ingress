//! Turning a Service reference into a set of upstream addresses.
//!
//! ingress-nginx bypasses `kube-proxy` and load-balances across pod IPs
//! directly; so do we, for the same reason: an ingress controller that hands
//! traffic to a ClusterIP gives up its own load-balancing, its own health
//! signal, and its own session affinity to a `iptables` hash it cannot see.
//!
//! Resolution is a two-step: the Ingress names a Service **port** (by number or
//! name), the Service maps that to a **target** port, and the EndpointSlices
//! carry the resolved target port alongside each ready pod address. Naming
//! matters here — `EndpointPort.name` matches `ServicePort.name`, not the
//! Ingress's port field.

use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;

use k8s_openapi::api::core::v1::{Service, ServicePort};
use k8s_openapi::api::discovery::v1::EndpointSlice;
use kube::ResourceExt;
use ramjet_router::Endpoint;

use crate::config::{BackendPort, ServiceRef};

/// Label every EndpointSlice carries, pointing back at its Service.
pub(crate) const SERVICE_NAME_LABEL: &str = "kubernetes.io/service-name";

/// `spec.type` of a Service that is a DNS alias rather than a set of pods.
const EXTERNAL_NAME: &str = "ExternalName";

/// Why a backend could not be resolved to addresses.
///
/// None of these are fatal to a rebuild. The backend is still registered, with
/// no endpoints, and the proxy answers 503 — the same thing ingress-nginx does,
/// and the only behaviour that lets a Deployment finish rolling out without the
/// route disappearing from under it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ResolveIssue {
    /// No such Service in the snapshot.
    ServiceMissing,
    /// `spec.type: ExternalName`.
    ///
    /// Serving one means resolving a DNS name at request time and re-resolving
    /// it as records change — a resolver, a cache, and a TTL policy the data
    /// plane does not have yet. Skipping loudly beats routing to a stale A
    /// record.
    ExternalName {
        /// The name the Service aliases.
        target: String,
    },
    /// The Service exists but has no port matching the Ingress backend.
    PortMissing,
    /// The port resolved, but no EndpointSlice offered a ready address.
    NoReadyEndpoints {
        /// Addresses skipped because they were unready or terminating. `0`
        /// alongside this variant means the Service simply has no pods yet.
        skipped: usize,
    },
}

/// Resolved addresses plus what was thrown away getting them.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct Resolution {
    /// Ready addresses, sorted so a rebuild is deterministic.
    pub(crate) endpoints: Vec<Endpoint>,
    /// Addresses skipped for being unready or terminating.
    pub(crate) skipped: usize,
}

/// Services indexed by `(namespace, name)`, and EndpointSlices by the Service
/// they belong to.
///
/// Built once per rebuild. A cluster with 5k Services and 20k slices should not
/// pay a linear scan per Ingress path.
pub(crate) struct EndpointIndex<'a> {
    services: HashMap<(&'a str, &'a str), &'a Service>,
    slices: HashMap<(String, String), Vec<&'a EndpointSlice>>,
}

impl<'a> EndpointIndex<'a> {
    /// Indexes a snapshot's Services and EndpointSlices.
    pub(crate) fn new(services: &'a [Arc<Service>], slices: &'a [Arc<EndpointSlice>]) -> Self {
        let mut by_name = HashMap::with_capacity(services.len());
        for svc in services {
            let (Some(ns), Some(name)) = (
                svc.metadata.namespace.as_deref(),
                svc.metadata.name.as_deref(),
            ) else {
                continue;
            };
            by_name.insert((ns, name), svc.as_ref());
        }

        let mut by_service: HashMap<(String, String), Vec<&EndpointSlice>> = HashMap::new();
        for slice in slices {
            let Some(ns) = slice.metadata.namespace.as_deref() else {
                continue;
            };
            let Some(service) = slice.labels().get(SERVICE_NAME_LABEL) else {
                continue;
            };
            by_service
                .entry((ns.to_owned(), service.clone()))
                .or_default()
                .push(slice.as_ref());
        }

        EndpointIndex {
            services: by_name,
            slices: by_service,
        }
    }

    /// Is this Service known at all? Used by the TLS and status paths, which
    /// care about existence but not about ports.
    pub(crate) fn service(&self, namespace: &str, name: &str) -> Option<&'a Service> {
        self.services.get(&(namespace, name)).copied()
    }

    /// Resolves an Ingress backend to ready upstream addresses.
    pub(crate) fn resolve(&self, target: &ServiceRef) -> Result<Resolution, ResolveIssue> {
        let service = self
            .service(&target.namespace, &target.name)
            .ok_or(ResolveIssue::ServiceMissing)?;
        let spec = service.spec.as_ref();

        if spec.and_then(|s| s.type_.as_deref()) == Some(EXTERNAL_NAME) {
            return Err(ResolveIssue::ExternalName {
                target: spec
                    .and_then(|s| s.external_name.clone())
                    .unwrap_or_default(),
            });
        }

        let port = spec
            .and_then(|s| s.ports.as_deref())
            .unwrap_or_default()
            .iter()
            .find(|p| port_matches(p, &target.port))
            .ok_or(ResolveIssue::PortMissing)?;

        let mut addrs: Vec<SocketAddr> = Vec::new();
        let mut skipped = 0usize;

        let slices = self
            .slices
            .get(&(target.namespace.clone(), target.name.clone()))
            .map(Vec::as_slice)
            .unwrap_or_default();

        for slice in slices {
            // FQDN slices would need a resolver, same as ExternalName.
            if !matches!(slice.address_type.as_str(), "IPv4" | "IPv6") {
                continue;
            }
            let Some(target_port) = slice_port(slice, port) else {
                continue;
            };
            let Ok(target_port) = u16::try_from(target_port) else {
                continue;
            };

            for endpoint in slice.endpoints.as_deref().unwrap_or_default() {
                if !is_serving(endpoint) {
                    skipped += endpoint.addresses.len();
                    continue;
                }
                for raw in &endpoint.addresses {
                    match raw.parse::<IpAddr>() {
                        Ok(ip) => addrs.push(SocketAddr::new(ip, target_port)),
                        // An unparseable address in an IPv4/IPv6 slice is the
                        // apiserver contradicting itself; count it as skipped
                        // rather than panicking on the control plane's behalf.
                        Err(_) => skipped += 1,
                    }
                }
            }
        }

        // EndpointSlices arrive in whatever order the watch delivered them, and
        // slice membership churns as pods restart. Sorting makes the compiled
        // table a function of the cluster state rather than of event ordering,
        // which is what lets the digest suppress no-op republishes.
        addrs.sort_unstable();
        addrs.dedup();

        if addrs.is_empty() {
            return Err(ResolveIssue::NoReadyEndpoints { skipped });
        }

        Ok(Resolution {
            endpoints: addrs.into_iter().map(Endpoint::new).collect(),
            skipped,
        })
    }
}

/// Does this ServicePort answer to the name or number the Ingress used?
fn port_matches(port: &ServicePort, wanted: &BackendPort) -> bool {
    match wanted {
        BackendPort::Number(n) => port.port == *n,
        BackendPort::Name(name) => port.name.as_deref() == Some(name.as_str()),
    }
}

/// The target port an EndpointSlice carries for a given ServicePort.
///
/// Matched by *name*: a multi-port Service has one EndpointPort per named
/// ServicePort. A single-port Service is allowed to leave both unnamed, and
/// the API server has historically written that as either absent or empty, so
/// both spellings have to compare equal.
fn slice_port(slice: &EndpointSlice, service_port: &ServicePort) -> Option<i32> {
    let wanted = service_port.name.as_deref().unwrap_or("");
    slice
        .ports
        .as_deref()
        .unwrap_or_default()
        .iter()
        .find(|p| p.name.as_deref().unwrap_or("") == wanted)
        .and_then(|p| p.port)
}

/// Should this endpoint receive traffic?
///
/// `ready` absent means unknown, which the EndpointSlice spec says to read as
/// ready — that is the state a slice is in mid-write, and treating it as
/// unready would blackhole a Service every time it was edited. `terminating`
/// is the opposite: absent means not terminating.
fn is_serving(endpoint: &k8s_openapi::api::discovery::v1::Endpoint) -> bool {
    let Some(conditions) = endpoint.conditions.as_ref() else {
        return true;
    };
    conditions.ready.unwrap_or(true) && !conditions.terminating.unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::translate::test_support::{endpoint_slice, service, EndpointFixture};

    fn index<'a>(
        services: &'a [Arc<Service>],
        slices: &'a [Arc<EndpointSlice>],
    ) -> EndpointIndex<'a> {
        EndpointIndex::new(services, slices)
    }

    fn svc_ref(port: BackendPort) -> ServiceRef {
        ServiceRef {
            namespace: "default".to_owned(),
            name: "web".to_owned(),
            port,
        }
    }

    fn addrs(res: &Resolution) -> Vec<String> {
        res.endpoints.iter().map(|e| e.addr.to_string()).collect()
    }

    #[test]
    fn resolves_a_numeric_port_to_the_slice_target_port() {
        let services = vec![Arc::new(service("default", "web", &[(None, 80)]))];
        let slices = vec![Arc::new(endpoint_slice(
            "default",
            "web",
            &[(None, 8080)],
            &[EndpointFixture::ready("10.0.0.1"), EndpointFixture::ready("10.0.0.2")],
        ))];
        let res = index(&services, &slices)
            .resolve(&svc_ref(BackendPort::Number(80)))
            .expect("resolves");
        assert_eq!(addrs(&res), ["10.0.0.1:8080", "10.0.0.2:8080"]);
        assert!(res.endpoints.iter().all(|e| e.weight == 1));
    }

    #[test]
    fn resolves_a_named_port_to_the_right_target() {
        let services = vec![Arc::new(service(
            "default",
            "web",
            &[(Some("http"), 80), (Some("metrics"), 9090)],
        ))];
        let slices = vec![Arc::new(endpoint_slice(
            "default",
            "web",
            &[(Some("http"), 8080), (Some("metrics"), 9091)],
            &[EndpointFixture::ready("10.0.0.1")],
        ))];
        let idx = index(&services, &slices);

        let res = idx
            .resolve(&svc_ref(BackendPort::Name("metrics".to_owned())))
            .expect("resolves");
        assert_eq!(addrs(&res), ["10.0.0.1:9091"]);

        // Same Service, other port: the target must not be shared.
        let res = idx
            .resolve(&svc_ref(BackendPort::Name("http".to_owned())))
            .expect("resolves");
        assert_eq!(addrs(&res), ["10.0.0.1:8080"]);
    }

    #[test]
    fn a_numeric_reference_to_a_named_port_still_resolves() {
        let services = vec![Arc::new(service("default", "web", &[(Some("http"), 80)]))];
        let slices = vec![Arc::new(endpoint_slice(
            "default",
            "web",
            &[(Some("http"), 8080)],
            &[EndpointFixture::ready("10.0.0.1")],
        ))];
        let res = index(&services, &slices)
            .resolve(&svc_ref(BackendPort::Number(80)))
            .expect("resolves");
        assert_eq!(addrs(&res), ["10.0.0.1:8080"]);
    }

    #[test]
    fn unready_and_terminating_addresses_are_skipped() {
        let services = vec![Arc::new(service("default", "web", &[(None, 80)]))];
        let slices = vec![Arc::new(endpoint_slice(
            "default",
            "web",
            &[(None, 8080)],
            &[
                EndpointFixture::ready("10.0.0.1"),
                EndpointFixture::unready("10.0.0.2"),
                EndpointFixture::terminating("10.0.0.3"),
            ],
        ))];
        let res = index(&services, &slices)
            .resolve(&svc_ref(BackendPort::Number(80)))
            .expect("resolves");
        assert_eq!(addrs(&res), ["10.0.0.1:8080"]);
        assert_eq!(res.skipped, 2);
    }

    #[test]
    fn an_absent_ready_condition_counts_as_ready() {
        let services = vec![Arc::new(service("default", "web", &[(None, 80)]))];
        let slices = vec![Arc::new(endpoint_slice(
            "default",
            "web",
            &[(None, 8080)],
            &[EndpointFixture::unknown("10.0.0.1")],
        ))];
        let res = index(&services, &slices)
            .resolve(&svc_ref(BackendPort::Number(80)))
            .expect("resolves");
        assert_eq!(addrs(&res), ["10.0.0.1:8080"]);
    }

    #[test]
    fn addresses_from_several_slices_merge_and_sort() {
        let services = vec![Arc::new(service("default", "web", &[(None, 80)]))];
        let slices = vec![
            Arc::new(endpoint_slice(
                "default",
                "web",
                &[(None, 8080)],
                &[EndpointFixture::ready("10.0.0.9")],
            )),
            Arc::new(endpoint_slice(
                "default",
                "web",
                &[(None, 8080)],
                &[EndpointFixture::ready("10.0.0.1")],
            )),
        ];
        let res = index(&services, &slices)
            .resolve(&svc_ref(BackendPort::Number(80)))
            .expect("resolves");
        assert_eq!(addrs(&res), ["10.0.0.1:8080", "10.0.0.9:8080"]);
    }

    #[test]
    fn missing_service_port_and_endpoints_are_distinguished() {
        let services = vec![Arc::new(service("default", "web", &[(None, 80)]))];
        let slices: Vec<Arc<EndpointSlice>> = Vec::new();

        assert_eq!(
            index(&services, &slices).resolve(&ServiceRef {
                namespace: "default".to_owned(),
                name: "absent".to_owned(),
                port: BackendPort::Number(80),
            }),
            Err(ResolveIssue::ServiceMissing)
        );
        assert_eq!(
            index(&services, &slices).resolve(&svc_ref(BackendPort::Number(8443))),
            Err(ResolveIssue::PortMissing)
        );
        assert_eq!(
            index(&services, &slices).resolve(&svc_ref(BackendPort::Number(80))),
            Err(ResolveIssue::NoReadyEndpoints { skipped: 0 })
        );
    }

    #[test]
    fn external_name_services_are_refused_with_their_target() {
        let mut svc = service("default", "web", &[(None, 80)]);
        let spec = svc.spec.get_or_insert_default();
        spec.type_ = Some("ExternalName".to_owned());
        spec.external_name = Some("elsewhere.example.com".to_owned());
        let services = vec![Arc::new(svc)];
        let slices: Vec<Arc<EndpointSlice>> = Vec::new();

        assert_eq!(
            index(&services, &slices).resolve(&svc_ref(BackendPort::Number(80))),
            Err(ResolveIssue::ExternalName {
                target: "elsewhere.example.com".to_owned()
            })
        );
    }

    #[test]
    fn fqdn_slices_are_ignored() {
        let services = vec![Arc::new(service("default", "web", &[(None, 80)]))];
        let mut slice = endpoint_slice(
            "default",
            "web",
            &[(None, 8080)],
            &[EndpointFixture::ready("10.0.0.1")],
        );
        slice.address_type = "FQDN".to_owned();
        let slices = vec![Arc::new(slice)];
        assert_eq!(
            index(&services, &slices).resolve(&svc_ref(BackendPort::Number(80))),
            Err(ResolveIssue::NoReadyEndpoints { skipped: 0 })
        );
    }

    #[test]
    fn slices_belonging_to_another_service_are_not_borrowed() {
        let services = vec![Arc::new(service("default", "web", &[(None, 80)]))];
        let slices = vec![Arc::new(endpoint_slice(
            "default",
            "other",
            &[(None, 8080)],
            &[EndpointFixture::ready("10.0.0.1")],
        ))];
        assert_eq!(
            index(&services, &slices).resolve(&svc_ref(BackendPort::Number(80))),
            Err(ResolveIssue::NoReadyEndpoints { skipped: 0 })
        );
    }

    #[test]
    fn ipv6_addresses_survive_the_round_trip() {
        let services = vec![Arc::new(service("default", "web", &[(None, 80)]))];
        let mut slice = endpoint_slice(
            "default",
            "web",
            &[(None, 8080)],
            &[EndpointFixture::ready("2001:db8::1")],
        );
        slice.address_type = "IPv6".to_owned();
        let slices = vec![Arc::new(slice)];
        let res = index(&services, &slices)
            .resolve(&svc_ref(BackendPort::Number(80)))
            .expect("resolves");
        assert_eq!(addrs(&res), ["[2001:db8::1]:8080"]);
    }
}
