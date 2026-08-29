//! Telling an Ingress's author that a value on it was refused.
//!
//! Every rebuild produces [`Warning`]s, and they have always gone to the log.
//! That is the right place for the ones about the cluster — a Service with no
//! endpoints, a Secret that has not been created yet — because the person who
//! can fix those is the one with pod-log access.
//!
//! It is the wrong place for a refused *annotation value*. The author of
//! `nginx.ingress.kubernetes.io/canary-weight: "twelve percent"` has a
//! namespace and an Ingress and no reason to have the controller's logs, and
//! the annotation goes on sitting there looking applied. So those warnings
//! additionally become a Warning Event on the object itself, where `kubectl
//! describe ingress` shows them under the resource that carries the mistake.
//!
//! # Why this is deduplicated by content rather than rate-limited
//!
//! A rebuild happens on every watch event in the cluster, and re-emitting the
//! same complaint each time would produce an Event per Ingress per deploy
//! forever. The obvious fix is a cooldown, and it is the wrong one: a cooldown
//! makes the Event stream a function of *time*, so the interesting case — the
//! operator fixed one annotation and broke another in the same edit — is
//! silently swallowed if it lands inside the window.
//!
//! Instead each object's warning set is hashed, and Events are written when that
//! hash changes. A steady broken state is silent after the first rebuild; an
//! object that gets a new problem says so immediately; and an object whose
//! problems all go away is forgotten, so the same complaint returning later is
//! reported again rather than suppressed by a stale entry.
//!
//! This is the same shape as the publish suppression in [`watch`](crate::watch)
//! — compare a digest, act on a change — for the same reason: the API server
//! re-sends objects on every watch restart and every resync, and anything that
//! reacted to events rather than to state would multiply that by the cluster.
//!
//! # What it costs when nothing is wrong
//!
//! A hash map lookup per Ingress that produced a refusal, which on a healthy
//! cluster is none at all. The map is only built when something changed.

use std::collections::{BTreeMap, HashMap};

use k8s_openapi::api::core::v1::ObjectReference;
use kube::{Client, ResourceExt};

use crate::audit::write_raw_event;
use crate::digest::Digest;
use crate::snapshot::ClusterSnapshot;
use crate::translate::{ObjectKey, Warning};

/// The `action` field on these Events: what was done, as opposed to what
/// happened.
///
/// Every one of them is the same verb, because every one of them is the same
/// act — a value was read, refused, and the object served without it.
const ACTION: &str = "Refuse";

/// `kubectl describe` shows this in yellow, which is the whole point.
const SEVERITY: &str = "Warning";

/// Warning Events on the Ingresses whose annotation values were refused.
pub(crate) struct WarningEvents {
    /// `None` in the tests, which exercise the deduplication and write nothing.
    client: Option<Client>,
    /// The hash of the warning set last written for each object.
    ///
    /// An object with nothing to complain about is absent rather than present
    /// with an empty entry, so the map is the size of the broken part of the
    /// cluster rather than of the cluster.
    emitted: HashMap<ObjectKey, u64>,
}

impl WarningEvents {
    /// An emitter that has said nothing yet.
    pub(crate) fn new(client: Client) -> Self {
        WarningEvents {
            client: Some(client),
            emitted: HashMap::new(),
        }
    }

    /// The same, with nothing to write to.
    #[cfg(test)]
    fn detached() -> Self {
        WarningEvents {
            client: None,
            emitted: HashMap::new(),
        }
    }

    /// Forgets what has been said, so the next pass says all of it again.
    ///
    /// Called when a watch restarts, for the same reason the status writer is
    /// invalidated there: the cache describes a cluster as it was before the
    /// gap, and an Event that was never written because we believed we had
    /// already written it is one nobody will ever see.
    pub(crate) fn invalidate(&mut self) {
        self.emitted.clear();
    }

    /// Writes an Event for every object whose refusals changed this rebuild.
    ///
    /// Returns what it wrote, as `(object, reason)` pairs. The return value
    /// exists for the tests — the property worth pinning is *which* Events came
    /// out of a sequence of rebuilds, and asserting on a private hash map would
    /// pin the implementation instead. It costs nothing on the path that
    /// matters: an unchanged cluster returns an empty `Vec`, which does not
    /// allocate.
    pub(crate) fn sync(
        &mut self,
        warnings: &[Warning],
        snapshot: &ClusterSnapshot,
    ) -> Vec<(ObjectKey, &'static str)> {
        let grouped = group(warnings);

        // Drop objects that no longer have anything wrong with them, so the
        // same complaint returning later is reported rather than suppressed by
        // an entry nothing cleared.
        self.emitted.retain(|key, _| grouped.contains_key(key));

        let changed: Vec<(&ObjectKey, &Vec<&Warning>)> = grouped
            .iter()
            .filter(|(key, group)| self.emitted.get(*key) != Some(&digest_of(group)))
            .collect();
        if changed.is_empty() {
            return Vec::new();
        }

        // Built once, and only when there is something to write: an Event needs
        // its object's uid or `kubectl describe` will not find it, and a linear
        // scan of the Ingress store per broken object would be quadratic on the
        // clusters most likely to have several.
        let uids = uid_index(snapshot);
        let mut written = Vec::new();

        for (key, group) in changed {
            let Some(uid) = uids.get(key) else {
                // The object left between the translation and here, or was
                // never a real one. Either way there is nothing to attach to,
                // and nothing is recorded — so an Ingress that comes back is
                // complained about rather than assumed to have been told.
                continue;
            };
            for warning in group {
                if let Some(client) = &self.client {
                    write_raw_event(
                        client.clone(),
                        reference(key, uid),
                        warning.kind.as_str().to_owned(),
                        ACTION.to_owned(),
                        SEVERITY.to_owned(),
                        warning.detail.clone(),
                    );
                }
                written.push((key.clone(), warning.kind.as_str()));
            }
            self.emitted.insert(key.clone(), digest_of(group));
        }
        written
    }
}

/// The refusals worth an Event, by the object that carries them.
///
/// A `BTreeMap` and a sort, because the translator walks a `HashMap` in places
/// and two rebuilds of an unchanged cluster must produce the same hash. Without
/// the ordering, iteration order alone would look like a change and re-emit
/// every Event on every rebuild — which is the exact failure this module exists
/// to avoid.
fn group(warnings: &[Warning]) -> BTreeMap<ObjectKey, Vec<&Warning>> {
    let mut grouped: BTreeMap<ObjectKey, Vec<&Warning>> = BTreeMap::new();
    for warning in warnings {
        if !warning.kind.is_annotation_refusal() {
            continue;
        }
        grouped
            .entry(warning.subject.clone())
            .or_default()
            .push(warning);
    }
    for group in grouped.values_mut() {
        group.sort_by_key(|warning| (warning.kind.as_str(), warning.detail.as_str()));
    }
    grouped
}

/// A content hash of one object's refusals.
fn digest_of(group: &[&Warning]) -> u64 {
    let mut digest = Digest::new();
    for warning in group {
        digest.str(warning.kind.as_str());
        digest.str(&warning.detail);
    }
    digest.finish()
}

/// Every Ingress in the snapshot that has a uid, by key.
fn uid_index(snapshot: &ClusterSnapshot) -> HashMap<ObjectKey, String> {
    snapshot
        .ingresses
        .iter()
        .filter_map(|ingress| {
            ingress
                .uid()
                .map(|uid| (ObjectKey::of(ingress.as_ref()), uid))
        })
        .collect()
}

/// The `regarding` reference for a Warning Event about one Ingress.
fn reference(key: &ObjectKey, uid: &str) -> ObjectReference {
    ObjectReference {
        api_version: Some("networking.k8s.io/v1".to_owned()),
        kind: Some("Ingress".to_owned()),
        namespace: Some(key.namespace.clone()),
        name: Some(key.name.clone()),
        uid: Some(uid.to_owned()),
        ..ObjectReference::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::translate::WarningKind;

    fn warning(namespace: &str, name: &str, kind: WarningKind, detail: &str) -> Warning {
        Warning {
            subject: ObjectKey {
                namespace: namespace.to_owned(),
                name: name.to_owned(),
            },
            kind,
            detail: detail.to_owned(),
        }
    }

    fn key(namespace: &str, name: &str) -> ObjectKey {
        ObjectKey {
            namespace: namespace.to_owned(),
            name: name.to_owned(),
        }
    }

    #[test]
    fn only_refused_annotation_values_are_grouped() {
        let warnings = [
            warning("prod", "web", WarningKind::InvalidAnnotation, "bad weight"),
            // On every healthy rolling update. An Event here would train people
            // to ignore the stream.
            warning("prod", "web", WarningKind::EndpointsSkipped, "2 unready"),
            // A fact about the cluster, not about a value on this object.
            warning("prod", "web", WarningKind::ServiceUnresolved, "no such Service"),
            warning("prod", "shop", WarningKind::CanaryInert, "weight 0"),
        ];

        let grouped = group(&warnings);
        assert_eq!(grouped.len(), 2);
        assert_eq!(grouped[&key("prod", "web")].len(), 1);
        assert_eq!(
            grouped[&key("prod", "web")][0].kind,
            WarningKind::InvalidAnnotation
        );
        assert_eq!(grouped[&key("prod", "shop")].len(), 1);
    }

    #[test]
    fn the_hash_is_a_function_of_content_and_not_of_order() {
        // The translator walks a HashMap in places, so the same broken cluster
        // can produce the same warnings in a different order. If that changed
        // the hash, every rebuild would re-emit every Event.
        let a = [
            warning("prod", "web", WarningKind::InvalidAnnotation, "bad weight"),
            warning("prod", "web", WarningKind::MirrorRejected, "no such backend"),
        ];
        let b = [
            warning("prod", "web", WarningKind::MirrorRejected, "no such backend"),
            warning("prod", "web", WarningKind::InvalidAnnotation, "bad weight"),
        ];

        let (a, b) = (group(&a), group(&b));
        assert_eq!(
            digest_of(&a[&key("prod", "web")]),
            digest_of(&b[&key("prod", "web")])
        );
    }

    #[test]
    fn a_changed_detail_is_a_changed_hash() {
        let before = [warning("prod", "web", WarningKind::InvalidAnnotation, "12%")];
        let after = [warning("prod", "web", WarningKind::InvalidAnnotation, "13%")];
        let (before, after) = (group(&before), group(&after));
        assert_ne!(
            digest_of(&before[&key("prod", "web")]),
            digest_of(&after[&key("prod", "web")])
        );
    }

    #[test]
    fn a_second_problem_on_the_same_object_is_a_changed_hash() {
        // The case a time-based cooldown gets wrong: one annotation fixed and
        // another broken in the same edit, inside the quiet window.
        let one = [warning("prod", "web", WarningKind::CanaryInert, "weight 0")];
        let two = [
            warning("prod", "web", WarningKind::CanaryInert, "weight 0"),
            warning("prod", "web", WarningKind::MirrorRejected, "on a canary"),
        ];
        let (one, two) = (group(&one), group(&two));
        assert_ne!(
            digest_of(&one[&key("prod", "web")]),
            digest_of(&two[&key("prod", "web")])
        );
    }

    #[test]
    fn the_reference_carries_everything_kubectl_searches_on() {
        let reference = reference(&key("prod", "web"), "abcd-1234");
        assert_eq!(reference.kind.as_deref(), Some("Ingress"));
        assert_eq!(reference.api_version.as_deref(), Some("networking.k8s.io/v1"));
        assert_eq!(reference.namespace.as_deref(), Some("prod"));
        assert_eq!(reference.name.as_deref(), Some("web"));
        assert_eq!(reference.uid.as_deref(), Some("abcd-1234"));
    }

    /// A snapshot holding just enough Ingress for a uid lookup to succeed.
    fn snapshot_of(objects: &[(&str, &str)]) -> ClusterSnapshot {
        use k8s_openapi::api::networking::v1::Ingress;
        use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;

        ClusterSnapshot {
            ingresses: objects
                .iter()
                .map(|(namespace, name)| {
                    std::sync::Arc::new(Ingress {
                        metadata: ObjectMeta {
                            namespace: Some((*namespace).to_owned()),
                            name: Some((*name).to_owned()),
                            uid: Some(format!("uid-{namespace}-{name}")),
                            ..ObjectMeta::default()
                        },
                        ..Ingress::default()
                    })
                })
                .collect(),
            ingress_classes: Vec::new(),
            services: Vec::new(),
            endpoint_slices: Vec::new(),
            secrets: Vec::new(),
        }
    }

    #[test]
    fn a_steady_broken_state_is_reported_once() {
        // The whole reason this module has state. A rebuild happens on every
        // watch event in the cluster, so re-emitting here would be one Event per
        // Ingress per deploy, forever.
        let cluster = snapshot_of(&[("prod", "web")]);
        let warnings = [warning(
            "prod",
            "web",
            WarningKind::InvalidAnnotation,
            "`canary-weight` is not a number",
        )];

        let mut events = WarningEvents::detached();
        assert_eq!(
            events.sync(&warnings, &cluster),
            vec![(key("prod", "web"), "InvalidAnnotation")]
        );
        for _ in 0..5 {
            assert!(
                events.sync(&warnings, &cluster).is_empty(),
                "nothing changed, so there is nothing new to say"
            );
        }
    }

    #[test]
    fn a_changed_warning_set_is_reported_again() {
        let cluster = snapshot_of(&[("prod", "web")]);
        let mut events = WarningEvents::detached();

        let first = [warning("prod", "web", WarningKind::CanaryInert, "weight is 0")];
        assert_eq!(events.sync(&first, &cluster).len(), 1);

        // The operator fixed the weight and broke the mirror in the same edit.
        // A time-based cooldown would swallow this; a content hash does not.
        let second = [warning(
            "prod",
            "web",
            WarningKind::MirrorRejected,
            "a canary cannot also mirror",
        )];
        assert_eq!(
            events.sync(&second, &cluster),
            vec![(key("prod", "web"), "MirrorRejected")]
        );
        assert!(events.sync(&second, &cluster).is_empty());
    }

    #[test]
    fn a_problem_that_clears_and_returns_is_reported_again() {
        // The entry has to be forgotten when the object goes clean, or the
        // second occurrence is suppressed by a hash nothing invalidated.
        let cluster = snapshot_of(&[("prod", "web")]);
        let warnings = [warning("prod", "web", WarningKind::CanaryInert, "weight is 0")];
        let mut events = WarningEvents::detached();

        assert_eq!(events.sync(&warnings, &cluster).len(), 1);
        assert!(events.sync(&[], &cluster).is_empty(), "fixed: nothing to say");
        assert_eq!(
            events.sync(&warnings, &cluster).len(),
            1,
            "and broken again is news again"
        );
    }

    #[test]
    fn one_objects_problem_does_not_silence_anothers() {
        let cluster = snapshot_of(&[("prod", "web"), ("prod", "shop")]);
        let mut events = WarningEvents::detached();

        let both = [
            warning("prod", "web", WarningKind::CanaryInert, "weight is 0"),
            warning("prod", "shop", WarningKind::MirrorRejected, "no such backend"),
        ];
        assert_eq!(events.sync(&both, &cluster).len(), 2);

        // Only `shop` changes.
        let changed = [
            warning("prod", "web", WarningKind::CanaryInert, "weight is 0"),
            warning("prod", "shop", WarningKind::MirrorRejected, "backend has no port"),
        ];
        assert_eq!(
            events.sync(&changed, &cluster),
            vec![(key("prod", "shop"), "MirrorRejected")]
        );
    }

    #[test]
    fn an_object_with_no_uid_is_skipped_and_not_recorded_as_told() {
        // Nothing to attach an Event to, so nothing is written — and nothing is
        // remembered either, or the Ingress coming back into the store would
        // find its complaint already suppressed.
        let empty = snapshot_of(&[]);
        let warnings = [warning("prod", "web", WarningKind::CanaryInert, "weight is 0")];
        let mut events = WarningEvents::detached();

        assert!(events.sync(&warnings, &empty).is_empty());
        assert_eq!(
            events.sync(&warnings, &snapshot_of(&[("prod", "web")])).len(),
            1
        );
    }

    #[test]
    fn a_watch_restart_makes_everything_sayable_again() {
        let cluster = snapshot_of(&[("prod", "web")]);
        let warnings = [warning("prod", "web", WarningKind::CanaryInert, "weight is 0")];
        let mut events = WarningEvents::detached();

        assert_eq!(events.sync(&warnings, &cluster).len(), 1);
        assert!(events.sync(&warnings, &cluster).is_empty());

        events.invalidate();
        assert_eq!(
            events.sync(&warnings, &cluster).len(),
            1,
            "the cache described a cluster as it was before the gap"
        );
    }

    #[test]
    fn every_refusal_on_one_object_gets_its_own_event() {
        // One Event per warning, not one per object: `kubectl describe` shows a
        // reason per line, and merging three problems into one message would
        // make each of them unfilterable.
        let cluster = snapshot_of(&[("prod", "web")]);
        let warnings = [
            warning("prod", "web", WarningKind::CanaryInert, "weight is 0"),
            warning("prod", "web", WarningKind::InvalidAnnotation, "bad interval"),
            warning("prod", "web", WarningKind::MirrorRejected, "on a canary"),
        ];

        let mut events = WarningEvents::detached();
        let written = events.sync(&warnings, &cluster);
        assert_eq!(written.len(), 3);
        let reasons: Vec<&str> = written.iter().map(|(_, reason)| *reason).collect();
        assert_eq!(
            reasons,
            ["CanaryInert", "InvalidAnnotation", "MirrorRejected"],
            "sorted, so two rebuilds of one broken object read the same"
        );
    }

    #[test]
    fn every_reason_is_a_pascal_case_identifier() {
        // These are strings operators filter on with
        // `--field-selector reason=...`, so they have to be identifiers and
        // they have to be stable.
        for kind in [
            WarningKind::InvalidAnnotation,
            WarningKind::CanaryOrphan,
            WarningKind::CanaryConflict,
            WarningKind::CanaryInert,
            WarningKind::MirrorRejected,
            WarningKind::BackendProtocolConflict,
            WarningKind::EndpointsSkipped,
        ] {
            let reason = kind.as_str();
            assert!(
                reason.starts_with(|c: char| c.is_ascii_uppercase())
                    && reason.chars().all(|c| c.is_ascii_alphanumeric()),
                "{reason} is not a PascalCase identifier"
            );
        }
    }
}
