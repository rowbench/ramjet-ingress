//! Exercises the crate the way the binary will: from outside, through the
//! public API only.
//!
//! The unit tests inside the crate can reach private fixtures and private
//! helpers, so they cannot tell you whether the exported surface is actually
//! sufficient to build a controller. This file can, and it is the contract the
//! integration phase depends on: construct Kubernetes objects, hand them to
//! [`translate`], and get a table plus certificate material back.

use std::collections::BTreeMap;

use k8s_openapi::api::core::v1::{Secret, Service, ServicePort, ServiceSpec};
use k8s_openapi::api::discovery::v1::{
    Endpoint, EndpointConditions, EndpointPort, EndpointSlice,
};
use k8s_openapi::api::networking::v1::{
    HTTPIngressPath, HTTPIngressRuleValue, Ingress, IngressBackend, IngressClass, IngressClassSpec,
    IngressRule, IngressServiceBackend, IngressSpec, IngressTLS, ServiceBackendPort,
};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use k8s_openapi::ByteString;

use ramjet_controller::{
    translate, ClusterSnapshot, ControllerOpts, WarningKind, ANNOTATION_CANARY,
    ANNOTATION_CANARY_WEIGHT, CONTROLLER_NAME,
};

const CERT: &[u8] = b"-----BEGIN CERTIFICATE-----\nZmFrZQ==\n-----END CERTIFICATE-----\n";
const KEY: &[u8] = b"-----BEGIN PRIVATE KEY-----\nZmFrZQ==\n-----END PRIVATE KEY-----\n";

fn our_class() -> IngressClass {
    IngressClass {
        metadata: ObjectMeta {
            name: Some("ramjet".to_owned()),
            ..Default::default()
        },
        spec: Some(IngressClassSpec {
            controller: Some(CONTROLLER_NAME.to_owned()),
            parameters: None,
        }),
    }
}

fn ingress(name: &str, host: &str, path: &str, service: &str) -> Ingress {
    Ingress {
        metadata: ObjectMeta {
            namespace: Some("shop".to_owned()),
            name: Some(name.to_owned()),
            ..Default::default()
        },
        spec: Some(IngressSpec {
            ingress_class_name: Some("ramjet".to_owned()),
            rules: Some(vec![IngressRule {
                host: Some(host.to_owned()),
                http: Some(HTTPIngressRuleValue {
                    paths: vec![HTTPIngressPath {
                        path: Some(path.to_owned()),
                        path_type: "Prefix".to_owned(),
                        backend: IngressBackend {
                            service: Some(IngressServiceBackend {
                                name: service.to_owned(),
                                port: Some(ServiceBackendPort {
                                    number: Some(80),
                                    name: None,
                                }),
                            }),
                            resource: None,
                        },
                    }],
                }),
            }]),
            ..Default::default()
        }),
        status: None,
    }
}

fn service(name: &str) -> Service {
    Service {
        metadata: ObjectMeta {
            namespace: Some("shop".to_owned()),
            name: Some(name.to_owned()),
            ..Default::default()
        },
        spec: Some(ServiceSpec {
            type_: Some("ClusterIP".to_owned()),
            ports: Some(vec![ServicePort {
                port: 80,
                ..Default::default()
            }]),
            ..Default::default()
        }),
        status: None,
    }
}

fn slice(service: &str, address: &str, ready: bool) -> EndpointSlice {
    let mut labels = BTreeMap::new();
    labels.insert(
        "kubernetes.io/service-name".to_owned(),
        service.to_owned(),
    );
    EndpointSlice {
        metadata: ObjectMeta {
            namespace: Some("shop".to_owned()),
            name: Some(format!("{service}-abcde")),
            labels: Some(labels),
            ..Default::default()
        },
        address_type: "IPv4".to_owned(),
        ports: Some(vec![EndpointPort {
            port: Some(8080),
            ..Default::default()
        }]),
        endpoints: Some(vec![Endpoint {
            addresses: vec![address.to_owned()],
            conditions: Some(EndpointConditions {
                ready: Some(ready),
                serving: Some(ready),
                terminating: Some(false),
            }),
            ..Default::default()
        }]),
    }
}

fn tls_secret(name: &str) -> Secret {
    let mut data = BTreeMap::new();
    data.insert("tls.crt".to_owned(), ByteString(CERT.to_vec()));
    data.insert("tls.key".to_owned(), ByteString(KEY.to_vec()));
    Secret {
        metadata: ObjectMeta {
            namespace: Some("shop".to_owned()),
            name: Some(name.to_owned()),
            ..Default::default()
        },
        data: Some(data),
        type_: Some("kubernetes.io/tls".to_owned()),
        ..Default::default()
    }
}

/// One realistic cluster, compiled end to end: two routes, a canary, TLS, and
/// an unready pod that must not be routed to.
#[test]
fn a_realistic_cluster_compiles_end_to_end() {
    let mut storefront = ingress("storefront", "shop.example.com", "/", "web");
    storefront
        .spec
        .get_or_insert_default()
        .tls
        .get_or_insert_default()
        .push(IngressTLS {
            hosts: Some(vec!["shop.example.com".to_owned()]),
            secret_name: Some("shop-tls".to_owned()),
        });

    let mut canary = ingress("storefront-canary", "shop.example.com", "/", "web-next");
    let annotations = canary.metadata.annotations.get_or_insert_default();
    annotations.insert(ANNOTATION_CANARY.to_owned(), "true".to_owned());
    annotations.insert(ANNOTATION_CANARY_WEIGHT.to_owned(), "25".to_owned());

    let snapshot = ClusterSnapshot::new()
        .with_ingress_class(our_class())
        .with_ingress(storefront)
        .with_ingress(canary)
        .with_ingress(ingress("api", "api.example.com", "/v1", "api"))
        .with_service(service("web"))
        .with_service(service("web-next"))
        .with_service(service("api"))
        .with_endpoint_slice(slice("web", "10.1.0.1", true))
        .with_endpoint_slice(slice("web-next", "10.1.0.2", true))
        .with_endpoint_slice(slice("api", "10.1.0.3", true))
        .with_endpoint_slice(slice("api", "10.1.0.4", false))
        .with_secret(tls_secret("shop-tls"));

    let translation =
        translate(&snapshot, &ControllerOpts::default(), None).expect("the cluster compiles");
    let table = &translation.config.table;

    // Routing.
    let storefront_hit = table
        .match_request("shop.example.com", "/checkout")
        .expect("the storefront route matches");
    assert_eq!(storefront_hit.backend().name(), "shop/web:80");

    let api_hit = table
        .match_request("api.example.com", "/v1/orders")
        .expect("the api route matches");
    assert_eq!(api_hit.backend().name(), "shop/api:80");
    assert!(table.match_request("api.example.com", "/v2").is_none());

    // The unready address never made it into the table.
    let api_backend = table
        .backends()
        .iter()
        .find(|b| b.name() == "shop/api:80")
        .expect("the api backend is registered");
    let addresses: Vec<String> = api_backend
        .endpoints()
        .iter()
        .map(|e| e.addr.to_string())
        .collect();
    assert_eq!(addresses, ["10.1.0.3:8080"]);

    // The canary merged into the production route rather than adding one.
    let canary_spec = storefront_hit.canary().expect("the canary attached");
    assert_eq!(canary_spec.weight(), 25);
    assert!(canary_spec.decide(None, None, 10), "under weight, diverts");
    assert!(!canary_spec.decide(None, None, 25), "at weight, does not");
    assert_eq!(
        table
            .backend(canary_spec.backend())
            .map(|b| b.name().to_owned()),
        Some("shop/web-next:80".to_owned())
    );

    // TLS: the SniMap handle and the emitted material agree, which is the whole
    // contract between this crate and the rustls side.
    assert_eq!(translation.config.certs.len(), 1);
    let material = &translation.config.certs[0];
    assert_eq!(material.cert_chain_pem, CERT);
    assert_eq!(material.key_pem, KEY);
    assert_eq!(
        table.tls().resolve("shop.example.com").map(|k| k.id()),
        Some(material.handle_id)
    );

    assert_eq!(
        translation
            .managed
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>(),
        ["shop/api", "shop/storefront", "shop/storefront-canary"]
    );
    // The only thing worth reporting is the pod that was not ready, and it is
    // reported against the Service rather than against an Ingress.
    let reported: Vec<(WarningKind, String)> = translation
        .warnings
        .iter()
        .map(|w| (w.kind, w.subject.to_string()))
        .collect();
    assert_eq!(
        reported,
        [(WarningKind::EndpointsSkipped, "shop/api".to_owned())]
    );
}

/// A rebuild that changes nothing must be recognisable as such without
/// comparing two `RouteTable`s, and a rebuild that changes something must not
/// be mistaken for a no-op.
#[test]
fn the_digest_tracks_configuration_and_not_the_generation() {
    let snapshot = ClusterSnapshot::new()
        .with_ingress_class(our_class())
        .with_ingress(ingress("storefront", "shop.example.com", "/", "web"))
        .with_service(service("web"))
        .with_endpoint_slice(slice("web", "10.1.0.1", true));

    let opts = ControllerOpts::default();
    let first = translate(&snapshot, &opts, None).expect("compiles");
    let second =
        translate(&snapshot, &opts, Some(&first.config.table)).expect("compiles");

    assert_eq!(first.digest, second.digest);
    assert_eq!(
        second.config.table.generation(),
        first.config.table.generation() + 1
    );

    let scaled = snapshot.clone().with_endpoint_slice(slice("web", "10.1.0.9", true));
    let third = translate(&scaled, &opts, Some(&second.config.table)).expect("compiles");
    assert_ne!(first.digest, third.digest);
}

/// A broken object degrades itself and nothing else. This is the property that
/// stops one namespace owner from taking down the cluster's routing.
#[test]
fn one_broken_ingress_does_not_take_the_table_with_it() {
    let mut broken = ingress("broken", "broken.example.com", "/[unclosed", "web");
    broken.spec.as_mut().expect("spec")
        .rules.as_mut().expect("rules")[0]
        .http.as_mut().expect("http")
        .paths[0]
        .path_type = "ImplementationSpecific".to_owned();

    let snapshot = ClusterSnapshot::new()
        .with_ingress_class(our_class())
        .with_ingress(ingress("good", "shop.example.com", "/", "web"))
        .with_ingress(broken)
        .with_service(service("web"))
        .with_endpoint_slice(slice("web", "10.1.0.1", true));

    let translation =
        translate(&snapshot, &ControllerOpts::default(), None).expect("the rest compiles");

    assert!(translation
        .config
        .table
        .match_request("shop.example.com", "/")
        .is_some());
    assert!(translation
        .config
        .table
        .match_request("broken.example.com", "/anything")
        .is_none());
    assert_eq!(
        translation
            .warnings
            .iter()
            .filter(|w| w.kind == WarningKind::InvalidRoute)
            .count(),
        1
    );
}
