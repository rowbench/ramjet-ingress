//! Certificate material for the unit tests.
//!
//! Real PEM rather than a fixture string, because the thing under test is a
//! parse: a hand-written blob that happens to satisfy the parser proves nothing
//! about a certificate a Secret would actually carry.

use rcgen::generate_simple_self_signed;

/// A self-signed certificate for `name`, as `(chain PEM, key PEM)` — the same
/// pair of blobs a `kubernetes.io/tls` Secret holds.
pub fn self_signed(name: &str) -> (Vec<u8>, Vec<u8>) {
    let issued = generate_simple_self_signed([name.to_owned()]).expect("a certificate");
    (
        issued.cert.pem().into_bytes(),
        issued.signing_key.serialize_pem().into_bytes(),
    )
}
