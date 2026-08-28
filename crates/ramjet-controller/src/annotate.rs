//! Writing annotations back onto an Ingress.
//!
//! The only place this controller edits an object's *spec-level* metadata
//! rather than its status, and it exists for exactly one caller: canary
//! auto-promotion, which steps `canary-weight` and records what it did.
//!
//! # Why it is here and not in the daemon
//!
//! The daemon owns the promotion state machine, because that needs the
//! in-process request counters. It does not own this, because
//! `ramjet-ingressd` deliberately handles no Kubernetes API object at all —
//! it takes a `kube::Client` and hands it straight to this crate. Keeping the
//! one `Api<Ingress>` in the crate that already has every other one means the
//! layering statement in the daemon's manifest stays true, and it means the
//! `k8s-openapi` dependency does not spread.
//!
//! # Server-side apply, forced
//!
//! Under the same [`FIELD_MANAGER`] as the status writer, so `managedFields`
//! records which keys this controller owns and `kubectl get -o yaml
//! --show-managed-fields` answers "who set this weight" without anybody
//! guessing.
//!
//! Forced because the annotations being written are normally owned by whoever
//! created the Ingress — a person, a Helm release, a GitOps reconciler — and an
//! unforced apply would be refused as a field-manager conflict on the very
//! first patch. That is not a conflict anybody can resolve, because taking
//! ownership of `canary-weight` is precisely what opting into automatic
//! promotion means. What it does *not* do is settle the argument permanently: a
//! reconciler that also claims the field will take it back on its own schedule.
//! See the promotion module's notes on GitOps.

use std::collections::BTreeMap;

use k8s_openapi::api::networking::v1::Ingress;
use kube::api::{Api, Patch, PatchParams};
use kube::Client;

use crate::config::FIELD_MANAGER;
use crate::translate::ObjectKey;

/// Applies `annotations` to `ingress`, leaving every other field — and every
/// annotation not named here — alone.
///
/// # Errors
///
/// The API server's error, rendered. The caller decides what to do about it;
/// there is deliberately no retry in here, because the only caller recomputes
/// its decision from scratch on its next pass and a retry that raced that would
/// apply a verdict taken from stale numbers.
pub async fn patch_ingress_annotations(
    client: &Client,
    ingress: &ObjectKey,
    annotations: &[(String, String)],
) -> Result<(), String> {
    let map: BTreeMap<&str, &str> = annotations
        .iter()
        .map(|(key, value)| (key.as_str(), value.as_str()))
        .collect();
    let patch = serde_json::json!({
        "apiVersion": "networking.k8s.io/v1",
        "kind": "Ingress",
        "metadata": {
            "name": ingress.name,
            "annotations": map,
        },
    });

    let api: Api<Ingress> = Api::namespaced(client.clone(), &ingress.namespace);
    api.patch(
        &ingress.name,
        &PatchParams::apply(FIELD_MANAGER).force(),
        &Patch::Apply(&patch),
    )
    .await
    .map(|_| ())
    .map_err(|error| error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The patch body, without an API server to send it to.
    ///
    /// Worth asserting on: a server-side apply that omitted `apiVersion` or
    /// `kind` is rejected with a message about the *object* rather than about
    /// the patch, which is a confusing hour to spend.
    #[test]
    fn the_patch_is_a_well_formed_partial_ingress() {
        let annotations = [
            ("ramjet.dev/auto-promote".to_owned(), "false".to_owned()),
            (
                "nginx.ingress.kubernetes.io/canary-weight".to_owned(),
                "0".to_owned(),
            ),
        ];
        let map: BTreeMap<&str, &str> = annotations
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();
        let patch = serde_json::json!({
            "apiVersion": "networking.k8s.io/v1",
            "kind": "Ingress",
            "metadata": { "name": "web-canary", "annotations": map },
        });

        assert_eq!(patch["apiVersion"], "networking.k8s.io/v1");
        assert_eq!(patch["kind"], "Ingress");
        assert_eq!(patch["metadata"]["name"], "web-canary");
        assert_eq!(patch["metadata"]["annotations"]["ramjet.dev/auto-promote"], "false");
        assert_eq!(
            patch["metadata"]["annotations"]["nginx.ingress.kubernetes.io/canary-weight"],
            "0"
        );
        assert!(
            patch["spec"].is_null(),
            "an apply carrying a spec would take ownership of the whole routing rule"
        );
    }
}
