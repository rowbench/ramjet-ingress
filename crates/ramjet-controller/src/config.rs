//! The compiled artifact and the knobs that shape it.

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use ramjet_router::{LbPolicy, RouteTable};

/// The `spec.controller` value of the `IngressClass` objects we answer to.
///
/// An `IngressClass` naming anything else belongs to a different controller and
/// its Ingresses are invisible to us, which is what makes running ramjet
/// alongside ingress-nginx during a migration safe.
pub const CONTROLLER_NAME: &str = "ramjet.dev/ingress";

/// Field manager used for every server-side apply this controller performs.
pub const FIELD_MANAGER: &str = "ramjet-ingress";

/// One certificate and its private key, exactly as they sat in the Secret.
///
/// The controller deliberately does not parse these. Parsing means rustls,
/// rustls means a crypto provider, and a crypto provider in the control plane
/// means the translation layer can no longer be unit-tested against string
/// literals. The binary owns the parse; see the crate docs.
#[derive(Clone, PartialEq, Eq)]
pub struct CertMaterial {
    /// Matches the [`CertifiedKeyHandle`](ramjet_router::CertifiedKeyHandle)
    /// id used by the table's `SniMap`.
    ///
    /// Derived from the Secret's namespace, name, and *content*. It therefore
    /// changes if and only if the material changes, so a consumer can cache the
    /// parsed key by id and re-parse only what actually rotated.
    pub handle_id: u64,
    /// PEM chain, from the Secret's `tls.crt`.
    pub cert_chain_pem: Vec<u8>,
    /// PEM private key, from the Secret's `tls.key`.
    pub key_pem: Vec<u8>,
}

impl fmt::Debug for CertMaterial {
    /// Never renders the key bytes. A control plane that logs private keys on a
    /// `{:?}` in an error path has a worse problem than whatever it was
    /// debugging.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CertMaterial")
            .field("handle_id", &self.handle_id)
            .field("cert_chain_pem", &format_args!("{} bytes", self.cert_chain_pem.len()))
            .field("key_pem", &format_args!("<{} bytes redacted>", self.key_pem.len()))
            .finish()
    }
}

/// Everything the data plane needs to serve one generation of configuration.
///
/// Published as a whole so table and certificates can never be observed out of
/// step: an `SniMap` entry always has its `CertMaterial` in the same value.
#[derive(Debug)]
pub struct CompiledConfig {
    /// The routing snapshot.
    ///
    /// Behind an `Arc` because publishing it is the point: the consumer moves
    /// this pointer straight into the data plane's
    /// [`SharedRouteTable`](ramjet_router::SharedRouteTable) with
    /// [`store_shared`](ramjet_router::SharedRouteTable::store_shared). A bare
    /// `RouteTable` could not be moved out of the `watch` channel that also
    /// holds this value, so every publish would have to deep-copy a table it
    /// already had a pointer to.
    pub table: Arc<RouteTable>,
    /// Certificate material referenced by `table.tls()`, deduplicated by
    /// [`handle_id`](CertMaterial::handle_id).
    pub certs: Vec<CertMaterial>,
    /// Content hash of everything above, excluding the generation number.
    ///
    /// The rebuild loop uses it to suppress a publish that would change
    /// nothing. It travels with the configuration rather than staying inside
    /// the loop because the data plane reports it too: two replicas serving the
    /// same digest are serving the same configuration, whatever generation
    /// numbers they happen to have reached, and that is the only way to tell
    /// them apart from two replicas that have diverged.
    pub digest: u64,
}

/// A `Service` port as an Ingress backend named it: by number or by name.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum BackendPort {
    /// `spec.rules[].http.paths[].backend.service.port.number`.
    Number(i32),
    /// `spec.rules[].http.paths[].backend.service.port.name`, resolved against
    /// the Service's own port list at translation time.
    Name(String),
}

impl fmt::Display for BackendPort {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BackendPort::Number(n) => write!(f, "{n}"),
            BackendPort::Name(n) => f.write_str(n),
        }
    }
}

/// A namespaced Service and one of its ports.
///
/// Doubles as the backend's name inside the route table, rendered as
/// `namespace/service:port`, which is what shows up in logs and metrics.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ServiceRef {
    /// Namespace of the Service.
    pub namespace: String,
    /// Name of the Service.
    pub name: String,
    /// Port, by number or by name.
    pub port: BackendPort,
}

impl ServiceRef {
    /// The backend name registered with the router.
    pub fn backend_name(&self) -> String {
        format!("{}/{}:{}", self.namespace, self.name, self.port)
    }
}

impl fmt::Display for ServiceRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}:{}", self.namespace, self.name, self.port)
    }
}

/// Why a `namespace/service:port` string could not be parsed.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ServiceRefError {
    /// The string was not of the form `namespace/name:port`.
    #[error("`{0}` is not of the form `namespace/name:port`")]
    Shape(String),
    /// The port was neither a valid number nor a valid name.
    #[error("`{port}` in `{input}` is not a valid port number or name")]
    Port {
        /// The whole input, for context.
        input: String,
        /// The offending port field.
        port: String,
    },
}

impl std::str::FromStr for ServiceRef {
    type Err = ServiceRefError;

    /// Parses `namespace/name:port`, where `port` is a number or a port name.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (namespace, rest) = s
            .split_once('/')
            .ok_or_else(|| ServiceRefError::Shape(s.to_owned()))?;
        let (name, port) = rest
            .rsplit_once(':')
            .ok_or_else(|| ServiceRefError::Shape(s.to_owned()))?;
        if namespace.is_empty() || name.is_empty() || port.is_empty() {
            return Err(ServiceRefError::Shape(s.to_owned()));
        }

        // A leading digit means it was meant as a number; a *failed* numeric
        // parse there is a typo, not a port name, and silently treating
        // `80x` as a name would route traffic nowhere with no diagnostic.
        let port = if port.starts_with(|c: char| c.is_ascii_digit()) {
            BackendPort::Number(port.parse().map_err(|_| ServiceRefError::Port {
                input: s.to_owned(),
                port: port.to_owned(),
            })?)
        } else {
            BackendPort::Name(port.to_owned())
        };

        Ok(ServiceRef {
            namespace: namespace.to_owned(),
            name: name.to_owned(),
            port,
        })
    }
}

/// Controller configuration.
#[derive(Debug, Clone)]
pub struct ControllerOpts {
    /// Namespace to watch, or `None` for every namespace.
    pub namespace: Option<String>,

    /// Value the legacy `kubernetes.io/ingress.class` annotation must carry for
    /// us to manage an Ingress. Matches ingress-nginx's `--ingress-class`.
    pub class_name: String,

    /// Backend serving requests that match no rule, as `namespace/name:port`.
    ///
    /// An Ingress with a bare `spec.defaultBackend` and no rules overrides
    /// this; see [`translate`](crate::translate).
    pub default_backend: Option<ServiceRef>,

    /// Secret (`namespace/name`) whose certificate answers a handshake whose
    /// SNI matches nothing.
    ///
    /// Needed because this crate cannot read a certificate's SANs to work out
    /// which names it covers — that would mean parsing X.509 here. An
    /// `IngressTLS` entry with no `hosts` is therefore skipped with a warning,
    /// and this option is how you serve a fallback certificate instead.
    pub default_tls_secret: Option<String>,

    /// Address written into managed Ingresses' `.status.loadBalancer`. Parsed
    /// as an IP if it looks like one, otherwise treated as a hostname.
    pub publish_address: Option<String>,

    /// Service (`namespace/name`) whose own `.status.loadBalancer` supplies the
    /// published address. Takes precedence over
    /// [`publish_address`](Self::publish_address).
    pub publish_service: Option<String>,

    /// Whether to write Ingress status at all.
    pub update_status: bool,

    /// How long to coalesce watch events before rebuilding.
    ///
    /// A rollout produces one EndpointSlice event per pod; without coalescing,
    /// a 50-pod Deployment would compile 50 route tables in a second, and 49 of
    /// them would be obsolete before anything read them.
    pub debounce: Duration,

    /// Load-balancing policy applied to every compiled backend.
    pub lb_policy: LbPolicy,
}

impl Default for ControllerOpts {
    fn default() -> Self {
        ControllerOpts {
            namespace: None,
            class_name: "ramjet".to_owned(),
            default_backend: None,
            default_tls_secret: None,
            publish_address: None,
            publish_service: None,
            update_status: true,
            debounce: Duration::from_millis(200),
            lb_policy: LbPolicy::RoundRobin,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    #[test]
    fn parses_numeric_and_named_ports() {
        assert_eq!(
            ServiceRef::from_str("default/web:8080").expect("valid"),
            ServiceRef {
                namespace: "default".to_owned(),
                name: "web".to_owned(),
                port: BackendPort::Number(8080),
            }
        );
        assert_eq!(
            ServiceRef::from_str("kube-system/dns:metrics")
                .expect("valid")
                .port,
            BackendPort::Name("metrics".to_owned())
        );
    }

    #[test]
    fn rejects_malformed_service_refs() {
        for bad in ["web:80", "default/web", "/web:80", "default/:80", "default/web:"] {
            assert!(
                ServiceRef::from_str(bad).is_err(),
                "`{bad}` should not parse"
            );
        }
        assert!(matches!(
            ServiceRef::from_str("default/web:80x"),
            Err(ServiceRefError::Port { .. })
        ));
    }

    #[test]
    fn backend_name_round_trips_through_display() {
        let r = ServiceRef::from_str("prod/api:http").expect("valid");
        assert_eq!(r.backend_name(), "prod/api:http");
        assert_eq!(r.to_string(), r.backend_name());
    }

    #[test]
    fn cert_debug_redacts_the_private_key() {
        let material = CertMaterial {
            handle_id: 7,
            cert_chain_pem: b"cert".to_vec(),
            key_pem: b"SUPER-SECRET-KEY".to_vec(),
        };
        let rendered = format!("{material:?}");
        assert!(!rendered.contains("SUPER-SECRET-KEY"), "{rendered}");
        assert!(rendered.contains("redacted"), "{rendered}");
    }
}
