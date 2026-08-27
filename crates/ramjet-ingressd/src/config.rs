//! The `--static-routes` file: a route table without Kubernetes.
//!
//! # Why this exists
//!
//! An ingress data plane that can only be exercised inside a cluster is an
//! ingress data plane nobody exercises. This file format is the smallest thing
//! that can describe what the controller will eventually publish — hosts,
//! paths, backends, canaries, certificates — so the proxy can be run, curled,
//! profiled, and debugged on a laptop with no API server anywhere.
//!
//! It is deliberately *not* a configuration format for production. The
//! Kubernetes path builds a [`RouteTable`] from API objects directly; it does
//! not render YAML and read it back, which is precisely the round trip that
//! makes ingress-nginx's behaviour hard to predict from its inputs. Nothing
//! else in the tree parses YAML.
//!
//! # Shape
//!
//! ```yaml
//! defaultBackend: fallback          # optional, answers unmatched requests
//!
//! backends:
//!   - name: api
//!     policy: roundRobin            # roundRobin | random | leastConn
//!     endpoints:
//!       - 127.0.0.1:9001            # shorthand for weight 1
//!       - address: 127.0.0.1:9002
//!         weight: 3
//!
//! routes:
//!   - host: shop.example.com        # omit for a rule with no host
//!     path: /api
//!     pathType: Prefix              # Prefix | Exact | ImplementationSpecific
//!     backend: api
//!     canary:                       # optional
//!       backend: api-next
//!       weight: 20
//!       header: x-canary
//!
//! tls:
//!   - host: shop.example.com        # omit or "*" for the default certificate
//!     cert: certs/shop.pem
//!     key: certs/shop-key.pem
//! ```
//!
//! Relative `cert` and `key` paths resolve against the directory holding the
//! configuration file, so a checked-in example works from any working
//! directory.

use std::collections::HashMap;
use std::fs::File;
use std::io::BufReader;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use ramjet_router::{
    BuildError, CanaryRules, CertifiedKeyHandle, Endpoint, LbPolicy, PathType, RouteTable,
    RouteTableBuilder,
};
use rustls::sign::CertifiedKey;
use rustls_pki_types::{CertificateDer, PrivateKeyDer};
use serde::Deserialize;

/// Why a configuration file could not be turned into a route table.
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    /// The file could not be read.
    #[error("cannot read {path}: {source}")]
    Read {
        /// The file in question.
        path: PathBuf,
        /// The underlying I/O error.
        #[source]
        source: std::io::Error,
    },
    /// The file was not valid YAML, or did not match the schema.
    #[error("cannot parse {path}: {source}")]
    Parse {
        /// The file in question.
        path: PathBuf,
        /// The underlying parse error, which carries a line and column.
        #[source]
        source: serde_yaml::Error,
    },
    /// An endpoint was not a `host:port` socket address.
    #[error("backend `{backend}` has endpoint `{address}`, which is not an ip:port address")]
    BadEndpoint {
        /// The backend it belongs to.
        backend: String,
        /// What was written.
        address: String,
    },
    /// The route table itself was invalid.
    #[error("invalid route table: {0}")]
    Table(#[from] BuildError),
    /// A certificate or key file could not be read or parsed.
    #[error("cannot load certificate for `{host}` from {path}: {reason}")]
    Certificate {
        /// The host the certificate was for.
        host: String,
        /// The file in question.
        path: PathBuf,
        /// What went wrong.
        reason: String,
    },
}

/// A parsed and validated configuration.
#[derive(Debug)]
pub struct Loaded {
    /// The route table to publish.
    pub table: RouteTable,
    /// The certificates the table's handles refer to.
    pub certs: HashMap<u64, Arc<CertifiedKey>>,
    /// Counts for the startup banner.
    pub summary: Summary,
}

/// What was loaded, for printing at startup.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct Summary {
    /// Number of backends registered.
    pub backends: usize,
    /// Number of endpoints across every backend.
    pub endpoints: usize,
    /// Number of path rules.
    pub routes: usize,
    /// Number of certificates loaded.
    pub certificates: usize,
    /// Whether a default backend was configured.
    pub default_backend: bool,
}

/// Reads and builds the configuration at `path`.
pub fn load(path: &Path) -> Result<Loaded, ConfigError> {
    let text = std::fs::read_to_string(path).map_err(|source| ConfigError::Read {
        path: path.to_owned(),
        source,
    })?;
    let document: Document =
        serde_yaml::from_str(&text).map_err(|source| ConfigError::Parse {
            path: path.to_owned(),
            source,
        })?;
    let base = path.parent().unwrap_or_else(|| Path::new("."));
    build(document, base)
}

/// Builds a table from an already-parsed document, resolving relative
/// certificate paths against `base`.
pub fn build(document: Document, base: &Path) -> Result<Loaded, ConfigError> {
    let mut builder = RouteTableBuilder::new();
    let mut summary = Summary::default();

    for backend in &document.backends {
        let mut endpoints = Vec::with_capacity(backend.endpoints.len());
        for endpoint in &backend.endpoints {
            let (address, weight) = endpoint.parts();
            let parsed: SocketAddr = address.parse().map_err(|_| ConfigError::BadEndpoint {
                backend: backend.name.clone(),
                address: address.to_owned(),
            })?;
            endpoints.push(Endpoint::weighted(parsed, weight));
        }
        summary.endpoints += endpoints.len();
        summary.backends += 1;
        builder.backend(&backend.name, backend.policy.into(), endpoints)?;
    }

    for route in &document.routes {
        let host = route.host.as_deref();
        match &route.canary {
            None => builder.route(host, &route.path, route.path_type.into(), &route.backend)?,
            Some(canary) => builder.canary_route(
                host,
                &route.path,
                route.path_type.into(),
                &route.backend,
                &CanaryRules {
                    backend: &canary.backend,
                    header: canary.header.as_deref(),
                    header_value: canary.header_value.as_deref(),
                    header_pattern: canary.header_pattern.as_deref(),
                    cookie: canary.cookie.as_deref(),
                    weight: canary.weight,
                    weight_total: canary.weight_total,
                },
            )?,
        }
        summary.routes += 1;
    }

    let mut certs = HashMap::with_capacity(document.tls.len());
    for (index, entry) in document.tls.iter().enumerate() {
        // Ids are positional and start at 1, so a `0` in a log is obviously a
        // bug rather than plausibly the first certificate.
        let id = index as u64 + 1;
        let host = entry.host.as_deref().unwrap_or("*");
        let chain = read_chain(&resolve(base, &entry.cert), host)?;
        let key = read_key(&resolve(base, &entry.key), host)?;
        let certified = ramjet_proxy::tls::certified_key(chain, key).map_err(|error| {
            ConfigError::Certificate {
                host: host.to_owned(),
                path: entry.cert.clone(),
                reason: error.to_string(),
            }
        })?;
        certs.insert(id, Arc::new(certified));

        let handle = Arc::new(CertifiedKeyHandle::new(id));
        // `*` is the default certificate rather than a wildcard host: a bare
        // asterisk is not a name the router will accept, and "serve this when
        // SNI matches nothing" is what a single-certificate dev setup wants.
        if host == "*" {
            builder.default_certificate(handle);
        } else {
            builder.certificate(host, handle)?;
        }
        summary.certificates += 1;
    }

    if let Some(name) = &document.default_backend {
        builder.default_backend(name);
        summary.default_backend = true;
    }

    Ok(Loaded {
        table: builder.build()?,
        certs,
        summary,
    })
}

fn resolve(base: &Path, path: &Path) -> PathBuf {
    if path.is_absolute() {
        path.to_owned()
    } else {
        base.join(path)
    }
}

fn read_chain(path: &Path, host: &str) -> Result<Vec<CertificateDer<'static>>, ConfigError> {
    let fail = failure(path, host);
    let file = File::open(path).map_err(|error| fail(error.to_string()))?;
    crate::certs::chain(&mut BufReader::new(file)).map_err(|error| fail(error.to_string()))
}

fn read_key(path: &Path, host: &str) -> Result<PrivateKeyDer<'static>, ConfigError> {
    let fail = failure(path, host);
    let file = File::open(path).map_err(|error| fail(error.to_string()))?;
    crate::certs::private_key(&mut BufReader::new(file)).map_err(|error| fail(error.to_string()))
}

fn failure(path: &Path, host: &str) -> impl Fn(String) -> ConfigError {
    let path = path.to_owned();
    let host = host.to_owned();
    move |reason| ConfigError::Certificate {
        host: host.clone(),
        path: path.clone(),
        reason,
    }
}

// ---------------------------------------------------------------------------
// Schema
// ---------------------------------------------------------------------------

/// The top level of a `--static-routes` file.
#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Document {
    /// Backend serving requests that match no rule.
    #[serde(default)]
    pub default_backend: Option<String>,
    /// Named groups of endpoints.
    #[serde(default)]
    pub backends: Vec<BackendSpec>,
    /// Host and path rules.
    #[serde(default)]
    pub routes: Vec<RouteSpec>,
    /// Certificates, by SNI name.
    #[serde(default)]
    pub tls: Vec<TlsSpec>,
}

/// One backend.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct BackendSpec {
    /// The name routes refer to it by.
    pub name: String,
    /// How requests are spread across its endpoints.
    #[serde(default)]
    pub policy: PolicySpec,
    /// Where to send them. May be empty, exactly as a Service with no ready
    /// pods may be.
    #[serde(default)]
    pub endpoints: Vec<EndpointSpec>,
}

/// An endpoint, either as a bare address or with a weight.
#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub enum EndpointSpec {
    /// `- 127.0.0.1:9001`
    Address(String),
    /// `- {address: 127.0.0.1:9001, weight: 3}`
    Weighted {
        /// `ip:port`.
        address: String,
        /// Relative share of traffic; `0` drains without removing.
        #[serde(default = "default_weight")]
        weight: u32,
    },
}

impl EndpointSpec {
    fn parts(&self) -> (&str, u32) {
        match self {
            EndpointSpec::Address(address) => (address, 1),
            EndpointSpec::Weighted { address, weight } => (address, *weight),
        }
    }
}

fn default_weight() -> u32 {
    1
}

/// Load-balancing policy, spelled as it is in the annotation.
#[derive(Debug, Default, Clone, Copy, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum PolicySpec {
    /// Rotate through endpoints in order.
    #[default]
    RoundRobin,
    /// Pick uniformly at random, honouring weights.
    Random,
    /// Pick the endpoint with the fewest in-flight requests.
    LeastConn,
}

impl From<PolicySpec> for LbPolicy {
    fn from(policy: PolicySpec) -> Self {
        match policy {
            PolicySpec::RoundRobin => LbPolicy::RoundRobin,
            PolicySpec::Random => LbPolicy::Random,
            PolicySpec::LeastConn => LbPolicy::LeastConn,
        }
    }
}

/// One host-and-path rule.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RouteSpec {
    /// The host to serve, or absent for a rule with no host.
    #[serde(default)]
    pub host: Option<String>,
    /// The path, or a regex for `ImplementationSpecific`.
    pub path: String,
    /// How the path is compared.
    #[serde(default)]
    pub path_type: PathTypeSpec,
    /// The backend to forward to.
    pub backend: String,
    /// An optional canary split.
    #[serde(default)]
    pub canary: Option<CanarySpec>,
}

/// Path matching, spelled as Kubernetes spells it.
#[derive(Debug, Default, Clone, Copy, Deserialize)]
pub enum PathTypeSpec {
    /// Byte-for-byte equality.
    Exact,
    /// Element-wise path-segment prefix. The Kubernetes default.
    #[default]
    Prefix,
    /// A regular expression, anchored at the start.
    ImplementationSpecific,
}

impl From<PathTypeSpec> for PathType {
    fn from(path_type: PathTypeSpec) -> Self {
        match path_type {
            PathTypeSpec::Exact => PathType::Exact,
            PathTypeSpec::Prefix => PathType::Prefix,
            PathTypeSpec::ImplementationSpecific => PathType::ImplementationSpecific,
        }
    }
}

/// A canary attached to a route, mirroring the ingress-nginx annotations.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CanarySpec {
    /// Where canaried traffic goes.
    pub backend: String,
    /// `canary-weight`.
    #[serde(default)]
    pub weight: u32,
    /// `canary-weight-total`; `0` means the default of 100.
    #[serde(default)]
    pub weight_total: u32,
    /// `canary-by-header`.
    #[serde(default)]
    pub header: Option<String>,
    /// `canary-by-header-value`.
    #[serde(default)]
    pub header_value: Option<String>,
    /// `canary-by-header-pattern`.
    #[serde(default)]
    pub header_pattern: Option<String>,
    /// `canary-by-cookie`.
    #[serde(default)]
    pub cookie: Option<String>,
}

/// One certificate.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TlsSpec {
    /// The SNI name, a `*.example.com` wildcard, or absent for the default.
    #[serde(default)]
    pub host: Option<String>,
    /// PEM file holding the certificate chain, leaf first.
    pub cert: PathBuf,
    /// PEM file holding the private key.
    pub key: PathBuf,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(yaml: &str) -> Result<Loaded, ConfigError> {
        let document: Document = serde_yaml::from_str(yaml).map_err(|source| ConfigError::Parse {
            path: PathBuf::from("<test>"),
            source,
        })?;
        build(document, Path::new("."))
    }

    #[test]
    fn a_minimal_document_builds() {
        let loaded = parse(
            "
backends:
  - name: app
    endpoints:
      - 127.0.0.1:9001
routes:
  - host: app.example.com
    path: /
    backend: app
",
        )
        .expect("a valid document");

        assert_eq!(loaded.summary.backends, 1);
        assert_eq!(loaded.summary.endpoints, 1);
        assert_eq!(loaded.summary.routes, 1);
        assert!(loaded
            .table
            .match_request("app.example.com", "/anything")
            .is_some());
    }

    #[test]
    fn path_type_defaults_to_prefix_as_kubernetes_does() {
        let loaded = parse(
            "
backends: [{name: app, endpoints: [127.0.0.1:1]}]
routes: [{host: app.example.com, path: /api, backend: app}]
",
        )
        .expect("valid");
        assert!(loaded.table.match_request("app.example.com", "/api/v1").is_some());
        // A prefix rule, not an exact one -- and still not a string prefix.
        assert!(loaded.table.match_request("app.example.com", "/apiary").is_none());
    }

    #[test]
    fn endpoints_accept_both_the_short_and_the_weighted_form() {
        let loaded = parse(
            "
backends:
  - name: app
    policy: leastConn
    endpoints:
      - 127.0.0.1:9001
      - address: 127.0.0.1:9002
        weight: 3
routes: [{host: app.example.com, path: /, backend: app}]
",
        )
        .expect("valid");

        let backend = loaded
            .table
            .match_request("app.example.com", "/")
            .expect("a match");
        let endpoints = backend.backend().endpoints();
        assert_eq!(endpoints.len(), 2);
        assert_eq!(endpoints[0].weight, 1, "the short form means weight 1");
        assert_eq!(endpoints[1].weight, 3);
        assert_eq!(backend.backend().policy(), LbPolicy::LeastConn);
    }

    #[test]
    fn a_hostless_route_serves_every_unclaimed_name() {
        let loaded = parse(
            "
backends: [{name: app, endpoints: [127.0.0.1:1]}]
routes: [{path: /, backend: app}]
",
        )
        .expect("valid");
        assert!(loaded.table.match_request("anything.test", "/").is_some());
    }

    #[test]
    fn a_canary_is_attached_to_its_route() {
        let loaded = parse(
            "
backends:
  - {name: prod, endpoints: [127.0.0.1:1]}
  - {name: next, endpoints: [127.0.0.1:2]}
routes:
  - host: app.example.com
    path: /
    backend: prod
    canary:
      backend: next
      weight: 20
      header: x-canary
      headerValue: beta
",
        )
        .expect("valid");

        let canary = loaded
            .table
            .match_request("app.example.com", "/")
            .and_then(|m| m.canary())
            .expect("a canary");
        assert_eq!(canary.weight(), 20);
        assert_eq!(canary.weight_total(), 100, "an unset total defaults to 100");
        assert_eq!(canary.header_name(), Some("x-canary"));
    }

    #[test]
    fn a_default_backend_is_recorded() {
        let loaded = parse(
            "
defaultBackend: fallback
backends:
  - {name: app, endpoints: [127.0.0.1:1]}
  - {name: fallback, endpoints: [127.0.0.1:2]}
routes: [{host: app.example.com, path: /, backend: app}]
",
        )
        .expect("valid");
        assert!(loaded.summary.default_backend);
        assert!(
            loaded.table.match_request("nowhere.test", "/").is_some(),
            "the default backend must answer an unmatched host"
        );
    }

    #[test]
    fn an_unparseable_endpoint_names_its_backend() {
        let error = parse(
            "
backends: [{name: app, endpoints: [\"not-an-address\"]}]
routes: [{host: app.example.com, path: /, backend: app}]
",
        )
        .expect_err("an invalid address");
        assert!(matches!(
            error,
            ConfigError::BadEndpoint { ref backend, .. } if backend == "app"
        ));
        assert!(error.to_string().contains("not-an-address"));
    }

    #[test]
    fn an_unknown_field_is_rejected_rather_than_ignored() {
        // A typo in a configuration file that silently does nothing is the
        // worst possible outcome: the operator believes they configured
        // something and traffic disagrees.
        let error = parse(
            "
backends: [{name: app, endpints: [127.0.0.1:1]}]
",
        )
        .expect_err("a typo");
        assert!(matches!(error, ConfigError::Parse { .. }), "{error}");
    }

    #[test]
    fn a_route_naming_a_missing_backend_is_rejected() {
        let error = parse("routes: [{host: app.example.com, path: /, backend: ghost}]")
            .expect_err("an unknown backend");
        assert!(error.to_string().contains("ghost"), "{error}");
    }

    #[test]
    fn relative_certificate_paths_resolve_against_the_config_directory() {
        assert_eq!(
            resolve(Path::new("/etc/ramjet"), Path::new("certs/tls.pem")),
            PathBuf::from("/etc/ramjet/certs/tls.pem")
        );
        assert_eq!(
            resolve(Path::new("/etc/ramjet"), Path::new("/abs/tls.pem")),
            PathBuf::from("/abs/tls.pem")
        );
    }

    #[test]
    fn a_missing_certificate_file_says_which_host_and_file() {
        let error = parse(
            "
tls: [{host: app.example.com, cert: nope.pem, key: nope-key.pem}]
",
        )
        .expect_err("a missing file");
        let message = error.to_string();
        assert!(message.contains("app.example.com"), "{message}");
        assert!(message.contains("nope.pem"), "{message}");
    }

    /// The example is the first thing anybody runs, and a broken one is a
    /// worse first impression than no example at all.
    #[test]
    fn the_shipped_example_stays_valid() {
        let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("examples/dev-routes.yaml");
        let loaded = load(&path).expect("examples/dev-routes.yaml must load");

        assert!(loaded.summary.backends >= 3);
        assert!(loaded.summary.routes >= 5);
        assert!(loaded.summary.default_backend);

        // Every curl command in the example's header comment must reach
        // something, or the example teaches the wrong thing.
        for (host, path) in [
            ("shop.example.com", "/"),
            ("shop.example.com", "/api/things"),
            ("shop.example.com", "/healthz"),
            ("anything.else", "/"),
            ("sub.example.com", "/"),
        ] {
            assert!(
                loaded.table.match_request(host, path).is_some(),
                "the example does not route {host}{path}"
            );
        }

        let canary = loaded
            .table
            .match_request("shop.example.com", "/api/things")
            .and_then(|m| m.canary())
            .expect("the example's canary");
        assert_eq!(canary.header_name(), Some("x-canary"));
    }

    #[test]
    fn an_empty_document_builds_an_empty_table() {
        // A cluster with no Ingresses is a normal state, not an error.
        let loaded = parse("{}").expect("valid");
        assert_eq!(loaded.summary, Summary::default());
        assert_eq!(loaded.table.route_count(), 0);
    }
}
