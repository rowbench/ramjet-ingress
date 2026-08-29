//! Writing what we know back onto the Ingresses we manage.
//!
//! Two things, on one pass, because they touch the same objects on the same
//! schedule and a second pass would double the API traffic to say two halves of
//! one sentence.
//!
//! **The address.** `kubectl get ingress` shows the ADDRESS column from
//! `.status.loadBalancer.ingress`, and cert-manager, external-dns, and every
//! dashboard read the same field. An ingress controller that routes correctly
//! but never writes status looks broken to everything downstream of it.
//!
//! **The generation.** `ramjet.dev/observed-generation` names the compiled
//! generation that last included this Ingress, which answers the question an
//! operator actually has after an edit: *did it land?* Without it the only
//! answers are a log line naming a generation number and no object, or
//! `/admin/routes`, which needs the admin port.
//!
//! Writes go through server-side apply under the field manager
//! [`FIELD_MANAGER`], so we own exactly these fields: another controller's
//! entries are not clobbered, and clearing ours is a real removal rather than
//! an overwrite with an empty list from someone else's perspective.
//!
//! # Why the generation is an annotation and not a condition
//!
//! Because there is nowhere else to put it. A `networking.k8s.io/v1` Ingress's
//! status holds a `LoadBalancerStatus` and nothing more — no `conditions`, no
//! `observedGeneration`, no extension point. Adding a CRD to carry one field
//! would be a new API object per Ingress for a diagnostic.
//!
//! An annotation on an object we watch is a feedback loop waiting to happen, so
//! two things make it safe. The value is compared against **the live object in
//! the store** before anything is sent, so a steady state costs no API calls
//! however often the loop runs; and the key is read by no parser in this crate,
//! so our own write cannot change a compiled digest and cannot cause the
//! republish that would write it again. See
//! [`ANNOTATION_OBSERVED_GENERATION`](crate::ANNOTATION_OBSERVED_GENERATION).
//!
//! # Why a stale annotation is left behind
//!
//! An Ingress that moves to another controller keeps whatever generation we
//! last wrote. Removing it would mean an apply under [`FIELD_MANAGER`] that
//! omits the key — and that field manager also owns the `canary-weight` and
//! `auto-promote-status` annotations automatic promotion writes, so a clear
//! would take those with it and silently zero somebody's canary. A stale
//! diagnostic on an object we no longer manage is much the smaller problem, and
//! the address — the actively misleading part — *is* cleared.

use std::collections::{HashMap, HashSet};

use k8s_openapi::api::core::v1::Service;
use k8s_openapi::api::networking::v1::Ingress;
use kube::api::{Patch, PatchParams};
use kube::{Api, Client};
use serde_json::json;
use tracing::{debug, warn};

use crate::annotate::patch_ingress_annotations;
use crate::annotations::ANNOTATION_OBSERVED_GENERATION;
use crate::config::{ControllerOpts, FIELD_MANAGER};
use crate::translate::ObjectKey;

/// The address published into `.status.loadBalancer.ingress[]`.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Address {
    /// A literal IP.
    Ip(String),
    /// A DNS name, for cloud load balancers that only hand out hostnames.
    Hostname(String),
}

impl Address {
    /// An operator-supplied string: an IP if it parses as one, else a hostname.
    ///
    /// Guessing beats a second config flag here — the two cases are
    /// syntactically disjoint and the API server rejects a hostname in the `ip`
    /// field, so a wrong guess would be immediately visible rather than subtly
    /// wrong.
    fn parse(raw: &str) -> Option<Self> {
        let raw = raw.trim();
        if raw.is_empty() {
            return None;
        }
        Some(if raw.parse::<std::net::IpAddr>().is_ok() {
            Address::Ip(raw.to_owned())
        } else {
            Address::Hostname(raw.to_owned())
        })
    }

    fn as_json(&self) -> serde_json::Value {
        match self {
            Address::Ip(ip) => json!({ "ip": ip }),
            Address::Hostname(hostname) => json!({ "hostname": hostname }),
        }
    }
}

/// Keeps Ingress status and the observed-generation annotation in step with
/// what we have compiled.
pub(crate) struct StatusWriter {
    client: Client,
    configured: Option<Address>,
    /// `namespace/name` of a Service whose own status supplies the address.
    publish_service: Option<(String, String)>,
    /// What we last successfully wrote, so a steady state costs no API calls.
    applied: HashMap<ObjectKey, Address>,
    /// The generation we last successfully annotated each Ingress with.
    ///
    /// A second guard behind the comparison against the live object, and it
    /// earns its place: a patch takes a moment to come back through the watch,
    /// so between writing and observing, the store still holds the old value
    /// and a rebuild in that window would patch again.
    annotated: HashMap<ObjectKey, u64>,
}

impl StatusWriter {
    /// Builds a writer, or `None` if `--no-status-update` was given.
    ///
    /// Built even with no address source configured, which it did not used to
    /// be: the address is one of two things this writes, and the other —
    /// `ramjet.dev/observed-generation` — needs no address at all. The flag
    /// means "do not write to my Ingresses", and it now switches off both halves
    /// of that rather than one and a half.
    pub(crate) fn new(client: Client, opts: &ControllerOpts) -> Option<Self> {
        if !opts.update_status {
            return None;
        }

        let publish_service = opts.publish_service.as_deref().and_then(|reference| {
            match reference.split_once('/') {
                Some((namespace, name)) if !namespace.is_empty() && !name.is_empty() => {
                    Some((namespace.to_owned(), name.to_owned()))
                }
                _ => {
                    warn!(%reference, "publish_service must be `namespace/name`; ignoring it");
                    None
                }
            }
        });
        let configured = opts.publish_address.as_deref().and_then(Address::parse);

        Some(StatusWriter {
            client,
            configured,
            publish_service,
            applied: HashMap::new(),
            annotated: HashMap::new(),
        })
    }

    /// Publishes the address and the compiled generation on every managed
    /// Ingress, and clears the address from the ones we have stopped managing.
    ///
    /// `observed` is what each Ingress *currently* carries in
    /// `ramjet.dev/observed-generation`, read from the reflector store. Checking
    /// it rather than trusting only our own cache is what makes this safe to run
    /// on every rebuild: a replica that restarted, or one whose write was made
    /// by a predecessor, sends nothing rather than re-patching the cluster.
    pub(crate) async fn sync(
        &mut self,
        managed: &[ObjectKey],
        generation: u64,
        observed: &HashMap<ObjectKey, u64>,
    ) {
        let address = self.address().await;

        // Clear first: if an Ingress moved to another controller, the stale
        // address is the actively misleading part.
        let live: HashSet<&ObjectKey> = managed.iter().collect();
        let stale: Vec<ObjectKey> = self
            .applied
            .keys()
            .filter(|key| !live.contains(*key))
            .cloned()
            .collect();
        for key in stale {
            if self.write(&key, None).await {
                self.applied.remove(&key);
            }
        }
        // The annotation is not cleared — see the module docs — but the cache
        // entry is dropped, so re-adopting an Ingress writes it again rather
        // than assuming a value we can no longer see.
        self.annotated.retain(|key, _| live.contains(key));

        for key in managed {
            if let Some(address) = &address {
                if self.applied.get(key) != Some(address) && self.write(key, Some(address)).await {
                    self.applied.insert(key.clone(), address.clone());
                }
            }

            if needs_annotation(key, generation, observed, &self.annotated)
                && self.annotate(key, generation).await
            {
                self.annotated.insert(key.clone(), generation);
            }
        }
    }

    /// Forgets what has been written, so the next sync rewrites everything.
    ///
    /// Called when a watch restarts: the cache describes the API server's state
    /// as of before the disconnect, and something may have changed underneath.
    pub(crate) fn invalidate(&mut self) {
        self.applied.clear();
        self.annotated.clear();
    }

    /// Applies `ramjet.dev/observed-generation` to one Ingress. Returns whether
    /// it stuck.
    async fn annotate(&self, key: &ObjectKey, generation: u64) -> bool {
        let annotations = [(
            ANNOTATION_OBSERVED_GENERATION.to_owned(),
            generation.to_string(),
        )];
        match patch_ingress_annotations(&self.client, key, &annotations).await {
            Ok(()) => {
                debug!(ingress = %key, generation, "wrote the observed generation");
                true
            }
            Err(error) => {
                // Survivable in exactly the way a lost status write is: routing
                // is unaffected, and the next rebuild retries because
                // `annotated` was not updated.
                warn!(ingress = %key, %error, "failed to annotate the observed generation");
                false
            }
        }
    }

    /// The address to publish, preferring a Service's own status.
    async fn address(&self) -> Option<Address> {
        let Some((namespace, name)) = &self.publish_service else {
            return self.configured.clone();
        };

        let api: Api<Service> = Api::namespaced(self.client.clone(), namespace);
        match api.get(name).await {
            Ok(service) => service
                .status
                .and_then(|s| s.load_balancer)
                .and_then(|lb| lb.ingress)
                .and_then(|entries| entries.into_iter().next())
                .and_then(|entry| match (entry.ip, entry.hostname) {
                    (Some(ip), _) => Some(Address::Ip(ip)),
                    (None, Some(hostname)) => Some(Address::Hostname(hostname)),
                    _ => None,
                })
                // A LoadBalancer Service with no address yet is normal for the
                // first minute of a cluster's life, not an error.
                .or_else(|| self.configured.clone()),
            Err(err) => {
                warn!(%namespace, %name, error = %err, "cannot read publish Service status");
                self.configured.clone()
            }
        }
    }

    /// Applies (or clears) one Ingress's status. Returns whether it stuck.
    async fn write(&self, key: &ObjectKey, address: Option<&Address>) -> bool {
        let entries: Vec<serde_json::Value> =
            address.map(|a| vec![a.as_json()]).unwrap_or_default();
        let patch = json!({
            "apiVersion": "networking.k8s.io/v1",
            "kind": "Ingress",
            "metadata": { "name": key.name },
            "status": { "loadBalancer": { "ingress": entries } },
        });

        let api: Api<Ingress> = Api::namespaced(self.client.clone(), &key.namespace);
        let params = PatchParams::apply(FIELD_MANAGER).force();
        match api.patch_status(&key.name, &params, &Patch::Apply(&patch)).await {
            Ok(_) => {
                debug!(ingress = %key, cleared = address.is_none(), "wrote Ingress status");
                true
            }
            Err(err) => {
                // Losing a status write is survivable: routing is unaffected
                // and the next rebuild retries, because `applied` was not
                // updated.
                warn!(ingress = %key, error = %err, "failed to write Ingress status");
                false
            }
        }
    }
}

/// Whether this Ingress needs `ramjet.dev/observed-generation` written.
///
/// Two guards, and both are load-bearing. `observed` is what the reflector store
/// says the object carries, which is authoritative and survives a restart of
/// this process — without it, every replica would re-patch every Ingress it
/// manages the first time it compiled anything. `annotated` is what this writer
/// last sent, which covers the window between a patch landing and the watch
/// delivering it back: for that moment the store still holds the old value, and
/// a rebuild inside it would patch again.
///
/// A free function rather than a method so that the rule has a test that needs
/// no API server; this is the whole of "no no-op patch storms".
fn needs_annotation(
    key: &ObjectKey,
    generation: u64,
    observed: &HashMap<ObjectKey, u64>,
    annotated: &HashMap<ObjectKey, u64>,
) -> bool {
    observed.get(key) != Some(&generation) && annotated.get(key) != Some(&generation)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(name: &str) -> ObjectKey {
        ObjectKey {
            namespace: "prod".to_owned(),
            name: name.to_owned(),
        }
    }

    #[test]
    fn an_ingress_already_carrying_the_generation_is_not_patched() {
        // The property that keeps this off the API server: a steady cluster
        // rebuilds on every watch event and must send nothing.
        let web = key("web");
        let observed = HashMap::from([(web.clone(), 42)]);
        let annotated = HashMap::new();

        assert!(!needs_annotation(&web, 42, &observed, &annotated));
        assert!(
            needs_annotation(&web, 43, &observed, &annotated),
            "a new generation is a new value"
        );
    }

    #[test]
    fn a_write_that_has_not_come_back_through_the_watch_is_not_repeated() {
        // The store lags a patch by however long the watch takes to deliver it.
        // Without the second guard, every rebuild in that window would patch
        // again — which is the storm this exists to prevent, and the one a
        // busy cluster produces most easily.
        let web = key("web");
        let observed = HashMap::from([(web.clone(), 41)]);
        let annotated = HashMap::from([(web.clone(), 42)]);

        assert!(!needs_annotation(&web, 42, &observed, &annotated));
    }

    #[test]
    fn an_ingress_that_has_never_been_annotated_is_patched_once() {
        let web = key("web");
        let empty = HashMap::new();
        assert!(needs_annotation(&web, 1, &empty, &empty));

        // And a replica that restarted reads the store rather than re-patching
        // what a predecessor already wrote.
        let observed = HashMap::from([(web.clone(), 1)]);
        assert!(!needs_annotation(&web, 1, &observed, &empty));
    }

    #[test]
    fn an_ip_is_recognised_as_an_ip() {
        assert_eq!(
            Address::parse("203.0.113.10"),
            Some(Address::Ip("203.0.113.10".to_owned()))
        );
        assert_eq!(
            Address::parse("2001:db8::1"),
            Some(Address::Ip("2001:db8::1".to_owned()))
        );
    }

    #[test]
    fn anything_else_is_a_hostname() {
        assert_eq!(
            Address::parse(" lb.example.com "),
            Some(Address::Hostname("lb.example.com".to_owned()))
        );
        assert_eq!(Address::parse("   "), None);
    }

    #[test]
    fn the_patch_shape_matches_the_ingress_status_subresource() {
        assert_eq!(
            Address::Ip("203.0.113.10".to_owned()).as_json(),
            json!({ "ip": "203.0.113.10" })
        );
        assert_eq!(
            Address::Hostname("lb.example.com".to_owned()).as_json(),
            json!({ "hostname": "lb.example.com" })
        );
    }
}
