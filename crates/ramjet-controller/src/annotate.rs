//! Writing annotations back onto an Ingress.
//!
//! The only place this controller edits an object's *spec-level* metadata
//! rather than its status. Two callers share it: canary auto-promotion, which
//! steps `canary-weight` and records what it did, and the status writer, which
//! stamps `ramjet.dev/observed-generation`.
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
//! # Why this is a merge patch and *not* a server-side apply
//!
//! It used to be a forced apply under [`FIELD_MANAGER`], and that was a bug —
//! a quiet, destructive one that took two release phases to find, so it is
//! worth stating precisely.
//!
//! **An apply is a statement of everything the manager owns, not of what the
//! request changes.** The API server diffs the applied object against the
//! manager's existing entry in `managedFields` and *deletes* every field that
//! entry claims and the new body omits. Two callers under one manager therefore
//! erase each other: promotion applies `{canary-weight: "60"}`, which drops
//! `observed-generation`; the resulting watch event rebuilds, and the status
//! writer applies `{observed-generation: "11"}`, which drops `canary-weight`
//! entirely. The canary then has no weight — inert, 0% of the traffic, no
//! promotion target left to step — seconds after the controller announced it
//! had stepped it. The same mechanism inside promotion alone would have zeroed
//! a canary at the finish line, because `Promote` writes only
//! `auto-promote-status`.
//!
//! Splitting the field manager in two would fix the first half and leave the
//! second. Nothing here ever needs to *remove* an annotation, which is the only
//! thing an apply buys, so the honest operation is the one whose semantics are
//! already what both callers want: set these keys, touch nothing else. That is
//! a JSON merge patch, and it makes the contract below true rather than
//! aspirational.
//!
//! Ownership is still recorded — a merge patch under a `fieldManager` writes a
//! `managedFields` entry the same way, as an `Update` rather than an `Apply` —
//! so `kubectl get -o yaml --show-managed-fields` still answers "who set this
//! weight". And the reason the apply was *forced* goes away with it: a merge
//! patch is never refused as a field-manager conflict, so taking `canary-weight`
//! from whoever created the Ingress needs no override. What it still does not do
//! is settle the argument permanently — a GitOps reconciler that also claims the
//! field will take it back on its own schedule. See the promotion module's notes.

use std::collections::BTreeMap;

use k8s_openapi::api::networking::v1::Ingress;
use kube::api::{Api, Patch, PatchParams};
use kube::Client;

use crate::config::FIELD_MANAGER;
use crate::translate::ObjectKey;

/// The patch body: the named annotations and nothing else.
///
/// No `apiVersion`, `kind` or `name` — those are an apply's requirement, and a
/// merge patch that carried them would be asserting values for fields it has no
/// business touching.
fn annotation_patch(annotations: &[(String, String)]) -> serde_json::Value {
    let map: BTreeMap<&str, &str> = annotations
        .iter()
        .map(|(key, value)| (key.as_str(), value.as_str()))
        .collect();
    serde_json::json!({ "metadata": { "annotations": map } })
}

/// Names this controller in `managedFields` without claiming an apply's
/// delete-what-I-omit semantics.
///
/// `force` stays false, and must: kube refuses it on anything but an apply, and
/// the conflict it exists to override cannot arise here.
fn annotation_params() -> PatchParams {
    PatchParams {
        field_manager: Some(FIELD_MANAGER.to_owned()),
        ..PatchParams::default()
    }
}

/// Sets `annotations` on `ingress`, leaving every other field — and every
/// annotation not named here — alone.
///
/// # Errors
///
/// The API server's error, rendered. The caller decides what to do about it;
/// there is deliberately no retry in here, because the promotion caller
/// recomputes its decision from scratch on its next pass and a retry that raced
/// that would apply a verdict taken from stale numbers.
pub async fn patch_ingress_annotations(
    client: &Client,
    ingress: &ObjectKey,
    annotations: &[(String, String)],
) -> Result<(), String> {
    let api: Api<Ingress> = Api::namespaced(client.clone(), &ingress.namespace);
    api.patch(
        &ingress.name,
        &annotation_params(),
        &Patch::Merge(annotation_patch(annotations)),
    )
    .await
    .map(|_| ())
    .map_err(|error| error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    const WEIGHT: &str = "nginx.ingress.kubernetes.io/canary-weight";
    const OBSERVED: &str = "ramjet.dev/observed-generation";

    fn pairs(entries: &[(&str, &str)]) -> Vec<(String, String)> {
        entries
            .iter()
            .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
            .collect()
    }

    #[test]
    fn the_patch_carries_the_named_annotations_and_nothing_else() {
        let patch = annotation_patch(&pairs(&[
            ("ramjet.dev/auto-promote", "false"),
            (WEIGHT, "0"),
        ]));

        assert_eq!(patch["metadata"]["annotations"]["ramjet.dev/auto-promote"], "false");
        assert_eq!(patch["metadata"]["annotations"][WEIGHT], "0");
        assert!(
            patch["spec"].is_null(),
            "a patch carrying a spec would rewrite the routing rule"
        );
        assert!(
            patch["status"].is_null(),
            "status has its own subresource and its own writer"
        );
        // `apiVersion`/`kind`/`name` are an apply's requirement. A merge patch
        // asserting them would be claiming fields it has no business touching.
        let metadata = patch["metadata"].as_object().expect("an object");
        assert_eq!(
            metadata.keys().collect::<Vec<_>>(),
            vec!["annotations"],
            "the patch must reach for nothing but annotations"
        );
    }

    /// The regression this module exists to prevent, stated at the only place a
    /// unit test can see it: the *kind* of patch.
    ///
    /// A server-side apply is a statement of everything the field manager owns,
    /// so the API server deletes whatever the manager's `managedFields` entry
    /// claims and the body omits. This function has two callers writing two
    /// disjoint annotation sets under one manager, so as an apply each call
    /// erased the other's work: promotion's `{canary-weight}` dropped
    /// `observed-generation`, and the status writer's `{observed-generation}`
    /// dropped `canary-weight` — leaving a canary with no weight at all,
    /// serving none of the traffic the controller had just announced it was
    /// stepping up. A merge patch sets keys and removes nothing.
    #[test]
    fn the_write_is_a_merge_and_never_an_apply() {
        let patch = Patch::Merge(annotation_patch(&pairs(&[(WEIGHT, "60")])));
        assert!(
            matches!(patch, Patch::Merge(_)),
            "an apply here deletes every annotation this manager owns and the body omits"
        );

        let params = annotation_params();
        assert_eq!(
            params.field_manager.as_deref(),
            Some(FIELD_MANAGER),
            "ownership is still recorded, as an Update entry rather than an Apply one"
        );
        assert!(
            !params.force,
            "force is only valid on an apply, and the conflict it overrode cannot arise here"
        );
    }

    /// Both callers, in the order that used to lose the weight.
    ///
    /// Neither body mentions the other's key, and that is now the point: with a
    /// merge patch the omission means "leave it alone" rather than "delete it".
    #[test]
    fn neither_caller_names_the_other_key() {
        let promotion = annotation_patch(&pairs(&[(WEIGHT, "60")]));
        let status = annotation_patch(&pairs(&[(OBSERVED, "11")]));

        assert!(promotion["metadata"]["annotations"][OBSERVED].is_null());
        assert!(status["metadata"]["annotations"][WEIGHT].is_null());
    }
}
