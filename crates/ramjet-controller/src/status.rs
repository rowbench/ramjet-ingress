//! Writing our load-balancer address back onto the Ingresses we manage.
//!
//! `kubectl get ingress` shows the ADDRESS column from
//! `.status.loadBalancer.ingress`, and cert-manager, external-dns, and every
//! dashboard read the same field. An ingress controller that routes correctly
//! but never writes status looks broken to everything downstream of it.
//!
//! Writes go through server-side apply under the field manager
//! [`FIELD_MANAGER`], so we own exactly this subtree: another controller's
//! entries are not clobbered, and clearing ours is a real removal rather than
//! an overwrite with an empty list from someone else's perspective.

use std::collections::{HashMap, HashSet};

use k8s_openapi::api::core::v1::Service;
use k8s_openapi::api::networking::v1::Ingress;
use kube::api::{Patch, PatchParams};
use kube::{Api, Client};
use serde_json::json;
use tracing::{debug, warn};

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

/// Keeps Ingress status in step with what we are actually serving.
pub(crate) struct StatusWriter {
    client: Client,
    configured: Option<Address>,
    /// `namespace/name` of a Service whose own status supplies the address.
    publish_service: Option<(String, String)>,
    /// What we last successfully wrote, so a steady state costs no API calls.
    applied: HashMap<ObjectKey, Address>,
}

impl StatusWriter {
    /// Builds a writer, or `None` if status updates are switched off or no
    /// address source is configured.
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

        if publish_service.is_none() && configured.is_none() {
            return None;
        }

        Some(StatusWriter {
            client,
            configured,
            publish_service,
            applied: HashMap::new(),
        })
    }

    /// Publishes the address on every managed Ingress and clears it from the
    /// ones we have stopped managing.
    pub(crate) async fn sync(&mut self, managed: &[ObjectKey]) {
        let Some(address) = self.address().await else {
            return;
        };

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

        for key in managed {
            if self.applied.get(key) == Some(&address) {
                continue;
            }
            if self.write(key, Some(&address)).await {
                self.applied.insert(key.clone(), address.clone());
            }
        }
    }

    /// Forgets what has been written, so the next sync rewrites everything.
    ///
    /// Called when a watch restarts: the cache describes the API server's state
    /// as of before the disconnect, and something may have changed underneath.
    pub(crate) fn invalidate(&mut self) {
        self.applied.clear();
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

#[cfg(test)]
mod tests {
    use super::*;

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
