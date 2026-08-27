//! PEM bytes into a rustls `CertifiedKey`.
//!
//! Both modes need exactly this and neither should own it. Dev mode reads the
//! chain and the key from two files on disk; Kubernetes mode gets the same two
//! blobs out of a Secret's `tls.crt` and `tls.key`, relayed by the controller as
//! [`CertMaterial`](ramjet_controller::CertMaterial). The bytes are identical
//! and so is the parse.
//!
//! The final step calls [`ramjet_proxy::tls::certified_key`] rather than rustls
//! directly, because which crypto provider signs a handshake is the data
//! plane's decision, not this binary's — and a `CertifiedKey` built against a
//! different provider than the `ServerConfig` fails at handshake time rather
//! than here.

use std::io::BufRead;

use rustls::sign::CertifiedKey;
use rustls_pki_types::{CertificateDer, PrivateKeyDer};

/// Why some PEM could not be turned into a usable key pair.
#[derive(Debug, thiserror::Error)]
pub enum CertError {
    /// The chain held no `CERTIFICATE` block.
    #[error("no CERTIFICATE block")]
    NoCertificates,
    /// The key file held no private key block.
    #[error("no PRIVATE KEY block")]
    NoPrivateKey,
    /// The PEM was malformed.
    #[error("malformed PEM: {0}")]
    Pem(#[source] std::io::Error),
    /// rustls refused the pair — most often a key that does not match the leaf
    /// certificate, or an algorithm the provider does not implement.
    #[error("{0}")]
    Rustls(#[source] rustls::Error),
}

/// Reads a certificate chain, leaf first.
pub fn chain(pem: &mut dyn BufRead) -> Result<Vec<CertificateDer<'static>>, CertError> {
    let chain: Vec<CertificateDer<'static>> = rustls_pemfile::certs(pem)
        .collect::<Result<_, _>>()
        .map_err(CertError::Pem)?;
    if chain.is_empty() {
        return Err(CertError::NoCertificates);
    }
    Ok(chain)
}

/// Reads the first private key, in any of the PEM spellings rustls accepts.
pub fn private_key(pem: &mut dyn BufRead) -> Result<PrivateKeyDer<'static>, CertError> {
    rustls_pemfile::private_key(pem)
        .map_err(CertError::Pem)?
        .ok_or(CertError::NoPrivateKey)
}

/// Parses a chain and its key straight from memory.
pub fn certified_key(
    cert_chain_pem: &[u8],
    key_pem: &[u8],
) -> Result<CertifiedKey, CertError> {
    let chain = chain(&mut &cert_chain_pem[..])?;
    let key = private_key(&mut &key_pem[..])?;
    ramjet_proxy::tls::certified_key(chain, key).map_err(CertError::Rustls)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::self_signed;

    #[test]
    fn empty_input_names_what_is_missing() {
        assert!(matches!(
            certified_key(b"", b""),
            Err(CertError::NoCertificates)
        ));
    }

    #[test]
    fn a_chain_with_no_key_beside_it_is_not_a_certified_key() {
        let (chain, _) = self_signed("example.com");
        assert!(matches!(
            certified_key(&chain, b"not pem at all\n"),
            Err(CertError::NoPrivateKey)
        ));
    }

    #[test]
    fn a_matching_pair_parses() {
        let (chain, key) = self_signed("example.com");
        assert!(certified_key(&chain, &key).is_ok());
    }
}
