//! Fixture builders for the translator's tests.
//!
//! Objects are constructed in Rust rather than parsed from YAML on purpose:
//! `serde_yaml` is unmaintained, a YAML fixture that no longer typechecks fails
//! at runtime instead of at compile time, and the structs below make the
//! *interesting* field of each fixture the only one a test has to mention.

use std::collections::BTreeMap;

use k8s_openapi::api::core::v1::{Secret, Service, ServicePort, ServiceSpec};
use k8s_openapi::api::discovery::v1::{
    Endpoint as SliceEndpoint, EndpointConditions, EndpointPort, EndpointSlice,
};
use k8s_openapi::api::networking::v1::{
    HTTPIngressPath, HTTPIngressRuleValue, Ingress, IngressBackend, IngressClass, IngressClassSpec,
    IngressRule, IngressServiceBackend, IngressSpec, IngressTLS, ServiceBackendPort,
};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{ObjectMeta, Time};
use k8s_openapi::jiff::Timestamp;
use k8s_openapi::ByteString;

use crate::annotations::{ANNOTATION_IS_DEFAULT_CLASS, ANNOTATION_LEGACY_CLASS};
use crate::endpoints::SERVICE_NAME_LABEL;
use crate::tls::TLS_SECRET_TYPE;

/// A certificate-shaped blob. Never parsed by this crate, so the bytes only
/// have to look like PEM.
pub(crate) const PEM_CERT: &[u8] =
    b"-----BEGIN CERTIFICATE-----\nZmFrZSBjZXJ0aWZpY2F0ZQ==\n-----END CERTIFICATE-----\n";
/// A key-shaped blob.
pub(crate) const PEM_KEY: &[u8] =
    b"-----BEGIN PRIVATE KEY-----\nZmFrZSBrZXk=\n-----END PRIVATE KEY-----\n";

/// One `spec.rules[].http.paths[]` entry.
#[derive(Debug, Clone)]
pub(crate) struct PathFixture {
    path: Option<String>,
    path_type: String,
    service: String,
    port: ServiceBackendPort,
}

/// One `spec.rules[]` entry.
#[derive(Debug, Clone)]
pub(crate) struct RuleFixture {
    host: Option<String>,
    paths: Vec<PathFixture>,
}

/// A path backed by a numeric Service port.
pub(crate) fn path(path: &str, path_type: &str, service: &str, port: i32) -> PathFixture {
    PathFixture {
        path: Some(path.to_owned()),
        path_type: path_type.to_owned(),
        service: service.to_owned(),
        port: ServiceBackendPort {
            number: Some(port),
            name: None,
        },
    }
}

/// A path backed by a *named* Service port.
pub(crate) fn named_path(
    path: &str,
    path_type: &str,
    service: &str,
    port_name: &str,
) -> PathFixture {
    PathFixture {
        path: Some(path.to_owned()),
        path_type: path_type.to_owned(),
        service: service.to_owned(),
        port: ServiceBackendPort {
            number: None,
            name: Some(port_name.to_owned()),
        },
    }
}

/// A path whose `path` field is absent, which the spec allows.
pub(crate) fn pathless(path_type: &str, service: &str, port: i32) -> PathFixture {
    PathFixture {
        path: None,
        ..path("/", path_type, service, port)
    }
}

/// A path whose backend is a `resource` reference rather than a Service.
pub(crate) fn resource_path(path_str: &str) -> PathFixture {
    PathFixture {
        service: String::new(),
        ..path(path_str, "Prefix", "", 0)
    }
}

/// A rule for one host, or the catch-all when `host` is `None`.
pub(crate) fn rule(host: Option<&str>, paths: &[PathFixture]) -> RuleFixture {
    RuleFixture {
        host: host.map(str::to_owned),
        paths: paths.to_vec(),
    }
}

/// An Ingress with the given rules.
pub(crate) fn ingress(namespace: &str, name: &str, rules: &[RuleFixture]) -> Ingress {
    Ingress {
        metadata: ObjectMeta {
            namespace: Some(namespace.to_owned()),
            name: Some(name.to_owned()),
            ..Default::default()
        },
        spec: Some(IngressSpec {
            rules: Some(rules.iter().map(build_rule).collect()),
            ..Default::default()
        }),
        status: None,
    }
}

fn build_rule(fixture: &RuleFixture) -> IngressRule {
    IngressRule {
        host: fixture.host.clone(),
        http: Some(HTTPIngressRuleValue {
            paths: fixture
                .paths
                .iter()
                .map(|p| HTTPIngressPath {
                    path: p.path.clone(),
                    path_type: p.path_type.clone(),
                    backend: backend(&p.service, p.port.clone()),
                })
                .collect(),
        }),
    }
}

fn backend(service: &str, port: ServiceBackendPort) -> IngressBackend {
    if service.is_empty() {
        // A `resource` backend. We never route these, so the reference itself
        // does not need to be meaningful.
        return IngressBackend {
            service: None,
            resource: Some(k8s_openapi::api::core::v1::TypedLocalObjectReference {
                api_group: Some("k8s.example.com".to_owned()),
                kind: "StorageBucket".to_owned(),
                name: "assets".to_owned(),
            }),
        };
    }
    IngressBackend {
        service: Some(IngressServiceBackend {
            name: service.to_owned(),
            port: Some(port),
        }),
        resource: None,
    }
}

/// Stamps a creation timestamp, in seconds since the epoch. Conflict
/// arbitration is by age, so tests that care must say how old things are.
pub(crate) fn created_at(mut ingress: Ingress, epoch_seconds: i64) -> Ingress {
    ingress.metadata.creation_timestamp = Some(Time(
        Timestamp::from_second(epoch_seconds).expect("fixture timestamp is in range"),
    ));
    ingress
}

/// Adds an annotation.
pub(crate) fn annotate(mut ingress: Ingress, key: &str, value: &str) -> Ingress {
    ingress
        .metadata
        .annotations
        .get_or_insert_default()
        .insert(key.to_owned(), value.to_owned());
    ingress
}

/// Sets `spec.ingressClassName`.
pub(crate) fn in_class(mut ingress: Ingress, class: &str) -> Ingress {
    ingress.spec.get_or_insert_default().ingress_class_name = Some(class.to_owned());
    ingress
}

/// Sets the legacy `kubernetes.io/ingress.class` annotation.
pub(crate) fn legacy_class(ingress: Ingress, value: &str) -> Ingress {
    annotate(ingress, ANNOTATION_LEGACY_CLASS, value)
}

/// Adds a `spec.tls` entry.
pub(crate) fn with_tls(mut ingress: Ingress, hosts: &[&str], secret: &str) -> Ingress {
    ingress
        .spec
        .get_or_insert_default()
        .tls
        .get_or_insert_default()
        .push(IngressTLS {
            hosts: if hosts.is_empty() {
                None
            } else {
                Some(hosts.iter().map(|h| (*h).to_owned()).collect())
            },
            secret_name: Some(secret.to_owned()),
        });
    ingress
}

/// Sets `spec.defaultBackend`.
pub(crate) fn with_default_backend(mut ingress: Ingress, service: &str, port: i32) -> Ingress {
    ingress.spec.get_or_insert_default().default_backend = Some(backend(
        service,
        ServiceBackendPort {
            number: Some(port),
            name: None,
        },
    ));
    ingress
}

/// An IngressClass, optionally marked as the cluster default.
pub(crate) fn ingress_class(name: &str, controller: &str, is_default: bool) -> IngressClass {
    let mut annotations = BTreeMap::new();
    if is_default {
        annotations.insert(ANNOTATION_IS_DEFAULT_CLASS.to_owned(), "true".to_owned());
    }
    IngressClass {
        metadata: ObjectMeta {
            name: Some(name.to_owned()),
            annotations: Some(annotations),
            ..Default::default()
        },
        spec: Some(IngressClassSpec {
            controller: Some(controller.to_owned()),
            parameters: None,
        }),
    }
}

/// A ClusterIP Service with the given `(name, port)` pairs.
pub(crate) fn service(namespace: &str, name: &str, ports: &[(Option<&str>, i32)]) -> Service {
    Service {
        metadata: ObjectMeta {
            namespace: Some(namespace.to_owned()),
            name: Some(name.to_owned()),
            ..Default::default()
        },
        spec: Some(ServiceSpec {
            type_: Some("ClusterIP".to_owned()),
            ports: Some(
                ports
                    .iter()
                    .map(|(port_name, port)| ServicePort {
                        name: port_name.map(str::to_owned),
                        port: *port,
                        ..Default::default()
                    })
                    .collect(),
            ),
            ..Default::default()
        }),
        status: None,
    }
}

/// One address in an EndpointSlice, with its readiness conditions.
#[derive(Debug, Clone)]
pub(crate) struct EndpointFixture {
    address: String,
    ready: Option<bool>,
    terminating: Option<bool>,
}

impl EndpointFixture {
    /// Ready and not terminating.
    pub(crate) fn ready(address: &str) -> Self {
        EndpointFixture {
            address: address.to_owned(),
            ready: Some(true),
            terminating: Some(false),
        }
    }

    /// Explicitly not ready.
    pub(crate) fn unready(address: &str) -> Self {
        EndpointFixture {
            ready: Some(false),
            ..Self::ready(address)
        }
    }

    /// Ready, but shutting down.
    pub(crate) fn terminating(address: &str) -> Self {
        EndpointFixture {
            terminating: Some(true),
            ..Self::ready(address)
        }
    }

    /// No conditions at all, which the spec says to read as ready.
    pub(crate) fn unknown(address: &str) -> Self {
        EndpointFixture {
            ready: None,
            terminating: None,
            ..Self::ready(address)
        }
    }
}

/// An EndpointSlice belonging to `service`, carrying resolved *target* ports.
pub(crate) fn endpoint_slice(
    namespace: &str,
    service: &str,
    ports: &[(Option<&str>, i32)],
    endpoints: &[EndpointFixture],
) -> EndpointSlice {
    let mut labels = BTreeMap::new();
    labels.insert(SERVICE_NAME_LABEL.to_owned(), service.to_owned());

    EndpointSlice {
        metadata: ObjectMeta {
            namespace: Some(namespace.to_owned()),
            name: Some(format!("{service}-slice")),
            labels: Some(labels),
            ..Default::default()
        },
        address_type: "IPv4".to_owned(),
        ports: Some(
            ports
                .iter()
                .map(|(port_name, port)| EndpointPort {
                    name: port_name.map(str::to_owned),
                    port: Some(*port),
                    ..Default::default()
                })
                .collect(),
        ),
        endpoints: Some(
            endpoints
                .iter()
                .map(|e| SliceEndpoint {
                    addresses: vec![e.address.clone()],
                    conditions: Some(EndpointConditions {
                        ready: e.ready,
                        serving: e.ready,
                        terminating: e.terminating,
                    }),
                    ..Default::default()
                })
                .collect(),
        ),
    }
}

/// A `kubernetes.io/tls` Secret.
pub(crate) fn secret(namespace: &str, name: &str, cert: &[u8], key: &[u8]) -> Secret {
    let mut data = BTreeMap::new();
    data.insert("tls.crt".to_owned(), ByteString(cert.to_vec()));
    data.insert("tls.key".to_owned(), ByteString(key.to_vec()));
    Secret {
        metadata: ObjectMeta {
            namespace: Some(namespace.to_owned()),
            name: Some(name.to_owned()),
            ..Default::default()
        },
        data: Some(data),
        type_: Some(TLS_SECRET_TYPE.to_owned()),
        ..Default::default()
    }
}
