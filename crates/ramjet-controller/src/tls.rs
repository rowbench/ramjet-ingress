//! Extracting certificate material from `kubernetes.io/tls` Secrets.
//!
//! Deliberately shallow. We check that the Secret is the right type, that both
//! keys are present and non-empty, and that the certificate at least *claims*
//! to be PEM. We do not parse it — that is the binary's job, and doing it here
//! would drag rustls and a crypto provider into the control plane for the sake
//! of a validation the TLS stack repeats anyway.
//!
//! The consequence worth naming: because we cannot read a certificate's SANs,
//! an `IngressTLS` entry with no `hosts` cannot be wired to anything. Those are
//! reported and skipped; [`ControllerOpts::default_tls_secret`](crate::ControllerOpts::default_tls_secret)
//! is the supported way to serve a fallback certificate.

use std::collections::HashMap;
use std::sync::Arc;

use k8s_openapi::api::core::v1::Secret;

use crate::config::CertMaterial;
use crate::digest::cert_handle_id;

/// The only Secret type an Ingress may reference for TLS.
pub(crate) const TLS_SECRET_TYPE: &str = "kubernetes.io/tls";

const TLS_CRT: &str = "tls.crt";
const TLS_KEY: &str = "tls.key";
const PEM_MARKER: &[u8] = b"-----BEGIN";

/// Why a Secret could not supply a certificate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum CertIssue {
    /// No such Secret in the snapshot.
    Missing,
    /// `type` was not `kubernetes.io/tls`.
    WrongType {
        /// What the Secret actually said.
        found: String,
    },
    /// `tls.crt` or `tls.key` was absent or empty.
    IncompleteData {
        /// Which key was the problem.
        field: &'static str,
    },
    /// `tls.crt` did not look like PEM.
    NotPem,
}

impl std::fmt::Display for CertIssue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CertIssue::Missing => f.write_str("secret does not exist"),
            CertIssue::WrongType { found } => {
                write!(f, "secret type is `{found}`, not `{TLS_SECRET_TYPE}`")
            }
            CertIssue::IncompleteData { field } => write!(f, "secret has no usable `{field}`"),
            CertIssue::NotPem => f.write_str("`tls.crt` is not PEM"),
        }
    }
}

/// TLS Secrets indexed by `(namespace, name)`.
pub(crate) struct SecretIndex<'a> {
    by_name: HashMap<(&'a str, &'a str), &'a Secret>,
}

impl<'a> SecretIndex<'a> {
    /// Indexes a snapshot's Secrets.
    pub(crate) fn new(secrets: &'a [Arc<Secret>]) -> Self {
        let mut by_name = HashMap::with_capacity(secrets.len());
        for secret in secrets {
            let (Some(ns), Some(name)) = (
                secret.metadata.namespace.as_deref(),
                secret.metadata.name.as_deref(),
            ) else {
                continue;
            };
            by_name.insert((ns, name), secret.as_ref());
        }
        SecretIndex { by_name }
    }

    /// Reads one Secret into certificate material.
    ///
    /// The returned [`CertMaterial::handle_id`] is a hash of the namespace,
    /// name, and bytes, so an unchanged certificate keeps its id across
    /// rebuilds and a rotated one does not.
    pub(crate) fn material(
        &self,
        namespace: &str,
        name: &str,
    ) -> Result<CertMaterial, CertIssue> {
        let secret = self
            .by_name
            .get(&(namespace, name))
            .copied()
            .ok_or(CertIssue::Missing)?;

        match secret.type_.as_deref() {
            Some(TLS_SECRET_TYPE) => {}
            other => {
                return Err(CertIssue::WrongType {
                    found: other.unwrap_or("<none>").to_owned(),
                })
            }
        }

        let field = |key: &'static str| -> Result<Vec<u8>, CertIssue> {
            let bytes = secret
                .data
                .as_ref()
                .and_then(|d| d.get(key))
                .map(|b| b.0.clone())
                .ok_or(CertIssue::IncompleteData { field: key })?;
            if bytes.is_empty() {
                return Err(CertIssue::IncompleteData { field: key });
            }
            Ok(bytes)
        };

        let cert_chain_pem = field(TLS_CRT)?;
        let key_pem = field(TLS_KEY)?;

        // A cheap shape check, not a parse. It catches the common failure --
        // someone stored a DER blob or a base64-of-base64 -- at the point where
        // we can name the Secret, rather than in the proxy's handshake path
        // where the only symptom is a dropped connection.
        if !contains(&cert_chain_pem, PEM_MARKER) {
            return Err(CertIssue::NotPem);
        }

        Ok(CertMaterial {
            handle_id: cert_handle_id(namespace, name, &cert_chain_pem, &key_pem),
            cert_chain_pem,
            key_pem,
        })
    }
}

fn contains(haystack: &[u8], needle: &[u8]) -> bool {
    haystack.windows(needle.len()).any(|w| w == needle)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::translate::test_support::{secret, PEM_CERT, PEM_KEY};

    fn index(secrets: &[Arc<Secret>]) -> SecretIndex<'_> {
        SecretIndex::new(secrets)
    }

    #[test]
    fn reads_a_well_formed_tls_secret() {
        let secrets = vec![Arc::new(secret("default", "web-tls", PEM_CERT, PEM_KEY))];
        let material = index(&secrets)
            .material("default", "web-tls")
            .expect("reads");
        assert_eq!(material.cert_chain_pem, PEM_CERT);
        assert_eq!(material.key_pem, PEM_KEY);
        assert_ne!(material.handle_id, 0);
    }

    #[test]
    fn handle_id_is_stable_for_unchanged_material() {
        let secrets = vec![Arc::new(secret("default", "web-tls", PEM_CERT, PEM_KEY))];
        let a = index(&secrets).material("default", "web-tls").expect("reads");
        let b = index(&secrets).material("default", "web-tls").expect("reads");
        assert_eq!(a.handle_id, b.handle_id);
    }

    #[test]
    fn handle_id_changes_when_the_certificate_rotates() {
        let before = vec![Arc::new(secret("default", "web-tls", PEM_CERT, PEM_KEY))];
        let rotated = format!("{}\nrotated", String::from_utf8_lossy(PEM_CERT));
        let after = vec![Arc::new(secret(
            "default",
            "web-tls",
            rotated.as_bytes(),
            PEM_KEY,
        ))];
        assert_ne!(
            index(&before).material("default", "web-tls").expect("reads").handle_id,
            index(&after).material("default", "web-tls").expect("reads").handle_id,
        );
    }

    #[test]
    fn missing_secret_is_reported() {
        let secrets: Vec<Arc<Secret>> = Vec::new();
        assert_eq!(
            index(&secrets).material("default", "web-tls"),
            Err(CertIssue::Missing)
        );
    }

    #[test]
    fn a_non_tls_secret_is_refused() {
        let mut s = secret("default", "web-tls", PEM_CERT, PEM_KEY);
        s.type_ = Some("Opaque".to_owned());
        let secrets = vec![Arc::new(s)];
        assert_eq!(
            index(&secrets).material("default", "web-tls"),
            Err(CertIssue::WrongType {
                found: "Opaque".to_owned()
            })
        );
    }

    #[test]
    fn empty_or_absent_fields_are_refused() {
        let mut s = secret("default", "web-tls", PEM_CERT, PEM_KEY);
        s.data.as_mut().expect("has data").remove("tls.key");
        let secrets = vec![Arc::new(s)];
        assert_eq!(
            index(&secrets).material("default", "web-tls"),
            Err(CertIssue::IncompleteData { field: "tls.key" })
        );

        let secrets = vec![Arc::new(secret("default", "web-tls", PEM_CERT, b""))];
        assert_eq!(
            index(&secrets).material("default", "web-tls"),
            Err(CertIssue::IncompleteData { field: "tls.key" })
        );
    }

    #[test]
    fn a_certificate_that_is_not_pem_is_refused() {
        let secrets = vec![Arc::new(secret(
            "default",
            "web-tls",
            b"\x30\x82\x03 not pem at all",
            PEM_KEY,
        ))];
        assert_eq!(
            index(&secrets).material("default", "web-tls"),
            Err(CertIssue::NotPem)
        );
    }
}
