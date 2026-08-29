//! Where a configuration change gets written down.
//!
//! Three sinks, because three different people are asking:
//!
//! - a **`tracing` event on the `audit` target**, structured rather than
//!   prose, so a log pipeline can filter on `target=audit` and get every
//!   configuration change this replica made and nothing else;
//! - a **Kubernetes Event**, so `kubectl describe` answers "what has this
//!   controller been doing" without anybody needing access to pod logs;
//! - an optional **webhook**, for a cluster that collects this somewhere else.
//!
//! # Which object an Event hangs on
//!
//! Configuration-level events — `ConfigApplied`, `ConfigPinned`,
//! `ConfigResumed` — go on the `IngressClass`. They describe a whole compiled
//! generation, which belongs to no single Ingress, and the class is the one
//! object that stands for "this controller".
//!
//! Canary events — `CanaryStepped`, `CanaryPromoted`, `CanaryRolledBack` — go on
//! **the canary Ingress**, and only there. An Event exists to point at the
//! object to go and look at, and after an automatic rollback that object is the
//! canary, not the class: `kubectl describe ingress web-canary` is the command
//! somebody actually types. Writing a second copy onto the class as well would
//! double the Event rate and make `kubectl get events` show every promotion
//! twice, for a class-level view that the `audit` log target and the webhook
//! already carry in full.
//!
//! A namespaced Event needs its object's `uid` or `kubectl describe` will not
//! find it — the search is a field selector that includes `involvedObject.uid`
//! whenever the object has one. The uid rides along on
//! [`PromotionTarget`](crate::PromotionTarget) from the translation pass that
//! already had the Ingress in its hand, rather than costing a `GET` per
//! decision.
//!
//! # Why the Events are written directly
//!
//! `kube::runtime::events::Recorder` aggregates events with the same reason
//! against the same object into a series for six minutes, keeping the *first*
//! note. That is the right behaviour for a reconciler reporting the same
//! condition repeatedly, and the wrong behaviour here: three deploys inside a
//! minute would become "ConfigApplied ×3" showing only what the first one did,
//! which is precisely the information an audit trail exists to keep. So each
//! publish creates its own Event.
//!
//! The rate that costs is bounded by the thing it is recording: publication is
//! already debounced, and a rebuild that changes nothing is already suppressed
//! by its digest, so the Event rate is the real configuration-change rate.
//! ingress-nginx emits a Sync event per Ingress per change, which is strictly
//! more.
//!
//! `IngressClass` is cluster-scoped and Events are not, so the class's land in
//! `default` — which is where the Kubernetes convention puts events for
//! cluster-scoped objects, and where `kubectl describe` looks for them. An
//! Ingress's land in the Ingress's own namespace, which is where `kubectl
//! describe ingress` looks.
//!
//! # Why the webhook does not retry
//!
//! It is a copy, not the record. The `tracing` event and the Kubernetes Event
//! have already been written by the time the POST is attempted, and the ring in
//! the data plane still holds the diff for `/admin/generations`; a webhook that
//! queued, retried, and backed off would be a delivery system this controller
//! would then have to reason about during exactly the incidents it exists to
//! describe. One attempt, five seconds, failures logged. If the collector was
//! down, the record is still in three other places.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use http::{header, Method, Request, Uri};
use http_body_util::Full;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::client::legacy::Client;
use hyper_util::rt::TokioExecutor;
use k8s_openapi::api::core::v1::ObjectReference;
use k8s_openapi::api::events::v1::Event;
use k8s_openapi::api::networking::v1::IngressClass;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{MicroTime, ObjectMeta};
use kube::api::{Api, PostParams};
use kube::{Client as KubeClient, ResourceExt};
use tracing::{debug, info, warn};

use crate::config::CONTROLLER_NAME;
use crate::diff::ConfigDiff;

/// Namespace Events about a cluster-scoped object are written to.
const EVENT_NAMESPACE: &str = "default";

/// How long the webhook gets, in total.
const WEBHOOK_TIMEOUT: Duration = Duration::from_secs(5);

/// What happened, in the vocabulary `kubectl describe` shows under `Reason`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuditReason {
    /// A new generation went live.
    ConfigApplied,
    /// A rollback pinned publication at an earlier generation.
    ConfigPinned,
    /// A pin was released and the newest generation went live.
    ConfigResumed,
    /// Automatic promotion advanced a canary to its next weight.
    CanaryStepped,
    /// A canary reached its last step and is now taking all of the traffic.
    CanaryPromoted,
    /// Automatic promotion pulled a canary's weight to zero and disarmed it.
    CanaryRolledBack,
}

impl AuditReason {
    /// The `Reason` string, which is a `PascalCase` identifier by convention.
    pub fn as_str(self) -> &'static str {
        match self {
            AuditReason::ConfigApplied => "ConfigApplied",
            AuditReason::ConfigPinned => "ConfigPinned",
            AuditReason::ConfigResumed => "ConfigResumed",
            AuditReason::CanaryStepped => "CanaryStepped",
            AuditReason::CanaryPromoted => "CanaryPromoted",
            AuditReason::CanaryRolledBack => "CanaryRolledBack",
        }
    }

    /// Whether `kubectl` should show this in yellow.
    ///
    /// A rollback is the only one of these an operator needs to find without
    /// knowing to look for it, and `kubectl describe` sorts and colours by this
    /// field. Everything else here is a thing going to plan.
    fn severity(self) -> &'static str {
        match self {
            AuditReason::CanaryRolledBack => "Warning",
            _ => "Normal",
        }
    }

    /// The `action` field: what was done, as opposed to what happened.
    fn action(self) -> &'static str {
        match self {
            AuditReason::ConfigApplied => "Publish",
            AuditReason::ConfigPinned => "Rollback",
            AuditReason::ConfigResumed => "Resume",
            AuditReason::CanaryStepped => "Step",
            AuditReason::CanaryPromoted => "Promote",
            AuditReason::CanaryRolledBack => "RollBack",
        }
    }
}

/// Why an audit webhook URL was refused.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WebhookError {
    /// The URL did not parse.
    Malformed(String),
    /// The URL was not `http://`.
    NotPlaintext(String),
    /// The URL named no host.
    NoHost(String),
}

impl std::fmt::Display for WebhookError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WebhookError::Malformed(url) => write!(f, "`{url}` is not a URL"),
            WebhookError::NotPlaintext(url) => write!(
                f,
                "`{url}` is not http://; the audit webhook does not speak TLS, \
                 deliberately — see the docs — so point it at a collector inside \
                 the cluster"
            ),
            WebhookError::NoHost(url) => write!(f, "`{url}` names no host"),
        }
    }
}

impl std::error::Error for WebhookError {}

/// The audit trail for one replica.
///
/// Cheap to clone; everything inside is either an `Arc` or a handle that is one
/// internally.
#[derive(Clone)]
pub struct AuditSink {
    client: Option<KubeClient>,
    /// The `IngressClass` Events are attached to, resolved once at startup.
    ///
    /// `None` when the class does not exist, in which case Events are skipped:
    /// an Event whose `regarding` names an object that is not there is written
    /// to etcd, shown by nothing, and garbage collected in an hour.
    regarding: Option<Arc<ObjectReference>>,
    webhook: Option<Webhook>,
}

#[derive(Clone)]
struct Webhook {
    uri: Uri,
    client: Client<HttpConnector, Full<Bytes>>,
}

impl std::fmt::Debug for AuditSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AuditSink")
            .field("events", &self.regarding.is_some())
            .field(
                "webhook",
                &self.webhook.as_ref().map(|w| w.uri.to_string()),
            )
            .finish()
    }
}

impl AuditSink {
    /// Validates a webhook URL without building a sink.
    ///
    /// Called at startup so a typo in `--audit-webhook` is a refusal to start
    /// rather than a warning logged once an hour later, when the first
    /// configuration change fails to be delivered somewhere nobody is looking.
    pub fn check_webhook(url: &str) -> Result<(), WebhookError> {
        let uri: Uri = url
            .parse()
            .map_err(|_| WebhookError::Malformed(url.to_owned()))?;
        if uri.host().is_none() {
            return Err(WebhookError::NoHost(url.to_owned()));
        }
        match uri.scheme_str() {
            Some("http") => Ok(()),
            _ => Err(WebhookError::NotPlaintext(url.to_owned())),
        }
    }

    /// A sink that only logs.
    pub fn logging_only() -> Self {
        AuditSink {
            client: None,
            regarding: None,
            webhook: None,
        }
    }

    /// Builds a sink that writes Events against `class_name` and, if one is
    /// configured, posts to `webhook`.
    ///
    /// `client` is `None` where there is no API server — dev mode, and the
    /// tests — which leaves the logs and the webhook and skips the Events.
    ///
    /// The `IngressClass` is looked up once, here, rather than on every
    /// publish: an Event needs the object's uid to attach to it, and a `GET`
    /// per configuration change would put the audit trail's cost on the API
    /// server during exactly the churn it is recording.
    pub async fn new(
        client: Option<KubeClient>,
        class_name: &str,
        webhook: Option<&str>,
    ) -> Result<Self, WebhookError> {
        let regarding = match &client {
            Some(client) => {
                let classes: Api<IngressClass> = Api::all(client.clone());
                match classes.get(class_name).await {
                    Ok(class) => Some(Arc::new(ObjectReference {
                        api_version: Some("networking.k8s.io/v1".to_owned()),
                        kind: Some("IngressClass".to_owned()),
                        name: Some(class.name_any()),
                        uid: class.uid(),
                        ..ObjectReference::default()
                    })),
                    Err(error) => {
                        warn!(
                            class = class_name,
                            %error,
                            "no IngressClass to attach configuration Events to; \
                             the audit trail will be logs only"
                        );
                        None
                    }
                }
            }
            None => None,
        };

        let webhook = match webhook {
            Some(url) => {
                Self::check_webhook(url)?;
                let uri: Uri = url
                    .parse()
                    .map_err(|_| WebhookError::Malformed(url.to_owned()))?;
                let mut connector = HttpConnector::new();
                connector.set_connect_timeout(Some(WEBHOOK_TIMEOUT));
                Some(Webhook {
                    uri,
                    client: Client::builder(TokioExecutor::new()).build(connector),
                })
            }
            None => None,
        };

        Ok(AuditSink {
            client,
            regarding,
            webhook,
        })
    }

    /// Records one applied generation.
    ///
    /// `published` is false when a rollback pin held the generation back. It
    /// still gets a log line — an operator holding a pin wants to see what they
    /// are holding back — but no Event and no webhook, because nothing about
    /// what the cluster is serving changed.
    pub fn applied(&self, diff: &ConfigDiff, published: bool) {
        let summary = diff.summary();
        info!(
            target: "audit",
            event = "config",
            generation = diff.to,
            previous = diff.from,
            published,
            routes_added = diff.routes_added.len(),
            routes_removed = diff.routes_removed.len(),
            backends_changed = diff.backends_changed.len(),
            hosts_added = diff.hosts_added.len(),
            hosts_removed = diff.hosts_removed.len(),
            certs_added = diff.certs_added,
            certs_removed = diff.certs_removed,
            certs_rotated = diff.certs_rotated.len(),
            mirrors_added = diff.mirrors_added.len(),
            mirrors_removed = diff.mirrors_removed.len(),
            default_backend_changed = diff.default_backend_changed,
            "{summary}"
        );

        if !published {
            return;
        }
        self.event(AuditReason::ConfigApplied, summary);
        self.post(diff.to_json());
    }

    /// Records one automatic-promotion decision.
    ///
    /// The numbers go in the structured fields rather than only in the prose,
    /// because the question after a rollback is always "on what evidence" and
    /// the answer has to be greppable a week later. `detail` carries the same
    /// thing in a sentence, for the Kubernetes Event, where there are no
    /// fields.
    pub fn canary(&self, reason: AuditReason, decision: &CanaryDecision<'_>) {
        info!(
            target: "audit",
            event = "canary",
            action = reason.as_str(),
            ingress = decision.ingress,
            from_weight = decision.from_weight,
            to_weight = decision.to_weight,
            canary_requests = decision.canary_requests,
            canary_5xx_percent = decision.canary_5xx_percent,
            canary_latency_ms = decision.canary_latency_ms,
            stable_requests = decision.stable_requests,
            stable_5xx_percent = decision.stable_5xx_percent,
            stable_latency_ms = decision.stable_latency_ms,
            "{}",
            decision.detail
        );

        // On the canary Ingress, and not on the class. The note drops the
        // `namespace/name` prefix the log line carries, because an Event
        // attached to an object does not need to name it.
        if let (Some(client), Some(subject)) = (self.client.clone(), decision.subject) {
            write_event(
                client,
                subject.reference(),
                reason,
                decision.detail.to_owned(),
            );
        }
        self.post(decision.to_json(reason));
    }

    /// Records a rollback taking effect.
    pub fn pinned(&self, generation: u64, serving: u64) {
        let summary =
            format!("publication pinned to generation {generation}, replacing {serving}");
        info!(
            target: "audit",
            event = "pin",
            generation,
            replaced = serving,
            "{summary}"
        );
        self.event(AuditReason::ConfigPinned, summary);
    }

    /// Records a rollback being released.
    pub fn resumed(&self, generation: u64) {
        let summary = format!("publication resumed at generation {generation}");
        info!(target: "audit", event = "resume", generation, "{summary}");
        self.event(AuditReason::ConfigResumed, summary);
    }

    /// Writes one Kubernetes Event on the `IngressClass`, without waiting.
    fn event(&self, reason: AuditReason, note: String) {
        let (Some(client), Some(regarding)) = (self.client.clone(), self.regarding.clone()) else {
            return;
        };
        write_event(client, (*regarding).clone(), reason, note);
    }

    /// Posts the diff, without waiting for it.
    fn post(&self, diff: serde_json::Value) {
        let Some(webhook) = self.webhook.clone() else {
            return;
        };
        let Ok(body) = serde_json::to_vec(&diff) else {
            return;
        };

        tokio::spawn(async move {
            let request = Request::builder()
                .method(Method::POST)
                .uri(webhook.uri.clone())
                .header(header::CONTENT_TYPE, "application/json")
                .body(Full::new(Bytes::from(body)));
            let Ok(request) = request else {
                return;
            };

            match tokio::time::timeout(WEBHOOK_TIMEOUT, webhook.client.request(request)).await {
                Ok(Ok(response)) if response.status().is_success() => {}
                Ok(Ok(response)) => warn!(
                    status = response.status().as_u16(),
                    url = %webhook.uri,
                    "audit webhook rejected the configuration diff"
                ),
                Ok(Err(error)) => {
                    warn!(%error, url = %webhook.uri, "audit webhook failed")
                }
                Err(_) => warn!(
                    url = %webhook.uri,
                    timeout_secs = WEBHOOK_TIMEOUT.as_secs(),
                    "audit webhook timed out"
                ),
            }
        });
    }
}

/// Writes one Kubernetes Event, without waiting for it.
///
/// Spawned rather than awaited: a slow API server must not delay the next
/// generation reaching the data plane, or the next promotion decision. Serving
/// traffic correctly outranks recording that we are.
///
/// The Event lands in `regarding.namespace`, falling back to
/// [`EVENT_NAMESPACE`] for a cluster-scoped object — which is where the
/// Kubernetes convention puts those and where `kubectl describe` looks.
pub(crate) fn write_event(
    client: KubeClient,
    regarding: ObjectReference,
    reason: AuditReason,
    note: String,
) {
    write_raw_event(
        client,
        regarding,
        reason.as_str().to_owned(),
        reason.action().to_owned(),
        reason.severity().to_owned(),
        note,
    );
}

/// The same, for a reason that is not an [`AuditReason`].
///
/// Exists for the translator's per-object warnings, whose reasons come from
/// [`WarningKind`](crate::WarningKind) and whose severity is always `Warning`.
/// Taking strings rather than growing `AuditReason` a variant per warning kind
/// keeps the two vocabularies apart: one names things this controller *did*, the
/// other names things it *refused*.
pub(crate) fn write_raw_event(
    client: KubeClient,
    regarding: ObjectReference,
    reason: String,
    action: String,
    severity: String,
    note: String,
) {
    let namespace = regarding
        .namespace
        .clone()
        .unwrap_or_else(|| EVENT_NAMESPACE.to_owned());

    tokio::spawn(async move {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or_default();
        let event = Event {
            action: Some(action),
            reason: Some(reason.clone()),
            // The Event API caps this at a kilobyte; the summary is one line by
            // construction, but truncating here is cheaper than having the API
            // server reject the whole write.
            note: Some(note.chars().take(1024).collect()),
            event_time: Some(MicroTime(k8s_openapi::jiff::Timestamp::now())),
            reporting_controller: Some(CONTROLLER_NAME.to_owned()),
            reporting_instance: Some(instance()),
            type_: Some(severity),
            metadata: ObjectMeta {
                namespace: Some(namespace.clone()),
                name: Some(format!(
                    "{}.{now:x}",
                    regarding.name.as_deref().unwrap_or("ramjet")
                )),
                ..ObjectMeta::default()
            },
            regarding: Some(regarding),
            ..Event::default()
        };

        let api: Api<Event> = Api::namespaced(client, &namespace);
        if let Err(error) = api.create(&PostParams::default(), &event).await {
            // Losing an Event is survivable and must not be loud: the same
            // record is in the log line that has already been written, and a
            // missing `events` RBAC rule would otherwise produce one warning per
            // configuration change forever.
            debug!(%error, %reason, "could not write an Event");
        }
    });
}

/// A namespaced object an Event can be attached to.
///
/// Borrowed and `Copy`, so the promotion loop can name the Ingress it is about
/// to patch without allocating three `String`s per interval per canary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EventSubject<'a> {
    /// The object's namespace.
    pub namespace: &'a str,
    /// The object's name.
    pub name: &'a str,
    /// The object's `metadata.uid`.
    ///
    /// Not optional, and that is the point: `kubectl describe` searches by a
    /// field selector that includes `involvedObject.uid`, so an Event written
    /// without the right one is written to etcd, shown by nothing, and garbage
    /// collected in an hour. A caller that does not have the uid has nothing
    /// worth writing, and should pass `None` for the whole subject.
    pub uid: &'a str,
}

impl EventSubject<'_> {
    /// The `regarding` reference for an Event about this object.
    fn reference(self) -> ObjectReference {
        ObjectReference {
            api_version: Some("networking.k8s.io/v1".to_owned()),
            kind: Some("Ingress".to_owned()),
            namespace: Some(self.namespace.to_owned()),
            name: Some(self.name.to_owned()),
            uid: Some(self.uid.to_owned()),
            ..ObjectReference::default()
        }
    }
}

/// One automatic-promotion decision, with the window it was taken on.
///
/// Borrowed rather than owned: the caller is holding all of this already, and a
/// struct of `String`s per interval per canary would be allocation for the sake
/// of a log line.
#[derive(Debug, Clone, Copy)]
pub struct CanaryDecision<'a> {
    /// `namespace/name` of the canary Ingress, for the log line and the webhook.
    pub ingress: &'a str,
    /// The same Ingress as an Event can name it, or `None` where its uid is not
    /// known and no Event should be written.
    pub subject: Option<EventSubject<'a>>,
    /// The weight before this decision.
    pub from_weight: u32,
    /// The weight after it.
    pub to_weight: u32,
    /// What happened, in a sentence.
    pub detail: &'a str,
    /// Canary requests in the window.
    pub canary_requests: u64,
    /// Canary 5xx as a percentage of its requests.
    pub canary_5xx_percent: f64,
    /// Canary mean upstream latency in the window, in milliseconds.
    pub canary_latency_ms: f64,
    /// Stable requests in the window.
    pub stable_requests: u64,
    /// Stable 5xx as a percentage of its requests.
    pub stable_5xx_percent: f64,
    /// Stable mean upstream latency in the window, in milliseconds.
    pub stable_latency_ms: f64,
}

impl CanaryDecision<'_> {
    /// The webhook payload.
    ///
    /// A different shape from [`ConfigDiff::to_json`], on the same URL, tagged
    /// with `"event"` so a collector can tell them apart without guessing from
    /// which keys are present. Adding a second shape rather than forcing this
    /// into the diff's: a promotion is not a configuration change this
    /// controller compiled, it is one it *caused*, and describing it as a diff
    /// would misreport where the change came from.
    fn to_json(self, reason: AuditReason) -> serde_json::Value {
        serde_json::json!({
            "event": "canary",
            "action": reason.as_str(),
            "ingress": self.ingress,
            "summary": self.detail,
            "from_weight": self.from_weight,
            "to_weight": self.to_weight,
            "canary": {
                "requests": self.canary_requests,
                "errors_5xx_percent": self.canary_5xx_percent,
                "upstream_latency_ms": self.canary_latency_ms,
            },
            "stable": {
                "requests": self.stable_requests,
                "errors_5xx_percent": self.stable_5xx_percent,
                "upstream_latency_ms": self.stable_latency_ms,
            },
        })
    }
}

/// This replica's name in `reportingInstance`.
///
/// The pod name where the downward API supplied one, which is what makes two
/// replicas' Events distinguishable; the controller name otherwise.
fn instance() -> String {
    std::env::var("POD_NAME").unwrap_or_else(|_| CONTROLLER_NAME.to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reasons_are_the_strings_kubectl_shows() {
        assert_eq!(AuditReason::ConfigApplied.as_str(), "ConfigApplied");
        assert_eq!(AuditReason::ConfigPinned.as_str(), "ConfigPinned");
        assert_eq!(AuditReason::ConfigResumed.as_str(), "ConfigResumed");
        assert_eq!(AuditReason::CanaryStepped.as_str(), "CanaryStepped");
        assert_eq!(AuditReason::CanaryPromoted.as_str(), "CanaryPromoted");
        assert_eq!(AuditReason::CanaryRolledBack.as_str(), "CanaryRolledBack");
    }

    #[test]
    fn a_canary_event_names_the_ingress_the_way_kubectl_searches_for_it() {
        // `kubectl describe ingress` builds a field selector over
        // involvedObject.{kind,namespace,name,uid} — every one of these — and an
        // Event missing any of them is written to etcd, displayed by nothing,
        // and garbage collected in an hour.
        let reference = EventSubject {
            namespace: "prod",
            name: "web-canary",
            uid: "3f2b1c0d-0000-4000-8000-000000000001",
        }
        .reference();

        assert_eq!(reference.kind.as_deref(), Some("Ingress"));
        assert_eq!(reference.api_version.as_deref(), Some("networking.k8s.io/v1"));
        assert_eq!(reference.namespace.as_deref(), Some("prod"));
        assert_eq!(reference.name.as_deref(), Some("web-canary"));
        assert_eq!(
            reference.uid.as_deref(),
            Some("3f2b1c0d-0000-4000-8000-000000000001")
        );
    }

    #[test]
    fn a_rollback_is_the_only_canary_reason_kubectl_shows_in_yellow() {
        // `kubectl describe` sorts and colours by this field. A step going to
        // plan is not something to find without looking for it; a rollback is.
        assert_eq!(AuditReason::CanaryRolledBack.severity(), "Warning");
        assert_eq!(AuditReason::CanaryStepped.severity(), "Normal");
        assert_eq!(AuditReason::CanaryPromoted.severity(), "Normal");
    }

    #[test]
    fn a_plaintext_url_is_accepted() {
        assert_eq!(
            AuditSink::check_webhook("http://audit.observability.svc:8080/ingress"),
            Ok(())
        );
    }

    #[test]
    fn https_is_refused_rather_than_downgraded() {
        // Silently posting a cluster's routing topology in the clear because
        // the operator wrote `https` would be the worst available answer.
        let error = AuditSink::check_webhook("https://audit.example.com/hook")
            .expect_err("refused");
        assert!(matches!(error, WebhookError::NotPlaintext(_)), "{error:?}");
        assert!(error.to_string().contains("does not speak TLS"), "{error}");
    }

    #[test]
    fn a_url_with_no_host_is_refused() {
        assert!(matches!(
            AuditSink::check_webhook("/just/a/path"),
            Err(WebhookError::NoHost(_) | WebhookError::NotPlaintext(_))
        ));
        assert!(AuditSink::check_webhook("not a url at all").is_err());
    }

    #[test]
    fn a_sink_with_no_cluster_still_logs() {
        // Dev mode has no API server, and the audit trail should not be a
        // reason it cannot run.
        let sink = AuditSink::logging_only();
        sink.applied(&ConfigDiff::default(), true);
        sink.pinned(3, 7);
        sink.resumed(7);
    }
}
