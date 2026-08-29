//! Canary auto-promotion: stepping a canary's weight up on evidence, and
//! pulling it to zero on the first sign that the evidence has turned.
//!
//! # Why this lives in the daemon
//!
//! It needs two things that are never in the same place anywhere else. The
//! numbers — per-route request, 5xx, and latency counters, split by whether the
//! canary took the request — are in the data plane's route table, in this
//! process, reachable with an atomic load. The *action* — patching an
//! annotation on an Ingress — needs a Kubernetes client, which the control
//! plane has. `ramjet-proxy` must not know what an Ingress is and
//! `ramjet-controller` must not know what a socket is, so the one binary that
//! depends on both is where the wire goes. It is the same argument that put
//! [`watch_pins`](crate::watch_pins) here.
//!
//! # The state machine, in five lines
//!
//! Every interval, for each opted-in canary: take the **window** — this
//! interval's deltas only, for the canary side and the stable side separately.
//! If either side saw fewer than `min-requests`, **hold**: no traffic is not
//! evidence of health, and it is certainly not evidence of failure. Otherwise
//! check the gates — canary 5xx percentage against `max-5xx-percent`, canary
//! mean latency against stable's times `max-latency-factor`. A breach is a
//! **rollback**: weight to zero, `auto-promote` to `"false"`, and a status
//! saying why. Otherwise **step** to the next weight, or **promote** if there
//! is no next weight.
//!
//! # Windows, not lifetimes
//!
//! The counters are cumulative and the process may have been up for a week, so
//! a lifetime error rate cannot move fast enough to catch anything. Each pass
//! subtracts the previous pass's reading; a canary that starts failing at 14:00
//! shows a bad window at 14:01 no matter how clean the preceding week was.
//!
//! The first pass after a step spans the moment the weight changed, so it mixes
//! two ratios. That is deliberate and it errs the safe way: the older, smaller
//! weight is the one over-represented, so a step is judged partly on the
//! traffic level it has already survived.
//!
//! # Why a rollback is one-way
//!
//! A canary that failed once and was automatically re-armed is a canary that
//! will fail again on the next interval, and again, flapping traffic between a
//! broken backend and a working one for as long as nobody is watching. So a
//! rollback disarms: it writes `auto-promote: "false"` alongside the weight,
//! and this loop refuses any canary whose status says it was rolled back, even
//! if the `auto-promote` annotation is somehow still true. Re-arming is a human
//! decision, taken after somebody has looked at why.
//!
//! # Why the backend swap is *not* automated
//!
//! Reaching 100% means every request is being served by the canary backend
//! while the production Ingress still names the old one. The obvious next step
//! — rewrite the production Ingress's backend and delete the canary — is
//! deliberately left to a human, and it is worth being explicit about why,
//! because it looks like the last mile of the same job.
//!
//! It is not. Everything this loop does is **reversible by writing one number**:
//! every state it can reach is a weight, and every weight has an inverse this
//! loop already knows how to apply. Editing `spec.rules[].backend` is a
//! different kind of change — it is the thing the canary was a rehearsal for,
//! it usually comes with deleting the canary Ingress, and undoing it means
//! reconstructing an object rather than setting a field. A controller that
//! restructures the objects an operator wrote, on a timer, is a controller
//! people turn off. So this loop drives the dial to 100, says so in an Event
//! and in the annotation, and stops. The remaining step is a one-line edit that
//! a human, a pull request, or a GitOps pipeline makes on purpose.
//!
//! # GitOps
//!
//! This loop writes to `canary-weight`, which in a GitOps cluster is a field
//! something else believes it owns. The patches are server-side applies under
//! the `ramjet-ingress` field manager, so ownership is explicit and a
//! reconciler that also claims the field will fight this loop and win on its
//! own schedule. That is a real interaction and there is no clever way around
//! it: either exclude `canary-weight` from the reconciler's managed fields, or
//! do not opt that Ingress in.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use ramjet_controller::{
    patch_ingress_annotations, AuditReason, AuditSink, CanaryDecision, CompiledConfig, EventSubject,
    ObjectKey,
    PromotionAnnotations, PromotionRoute, PromotionTarget, ANNOTATION_AUTO_PROMOTE,
    ANNOTATION_AUTO_PROMOTE_STATUS, ANNOTATION_CANARY_WEIGHT, STATUS_PROMOTED,
    STATUS_ROLLED_BACK,
};
use ramjet_proxy::GenerationHistory;
use ramjet_router::{RouteHost, RouteTotals, SharedRouteTable};
use tokio::sync::watch;
use tracing::{debug, info, warn};

/// How long the loop sleeps while a rollback pin is holding it.
///
/// A pin is released through the admin API rather than through the control
/// plane, so no generation arrives to wake the loop when it goes away and there
/// has to be a timer. Thirty seconds is short enough that resuming feels
/// immediate and long enough to be free.
///
/// It is not the interval used when nobody has opted in: that case waits on the
/// generation channel and costs nothing at all.
const IDLE_POLL: Duration = Duration::from_secs(30);

/// Shortest gap between passes, whatever the annotations ask for.
///
/// The annotation parser already refuses a zero interval; this is the second
/// line, and it also bounds the wakeup rate when several canaries with
/// different intervals are armed at once.
const MIN_TICK: Duration = Duration::from_secs(1);

// ---------------------------------------------------------------------------
// The state machine
// ---------------------------------------------------------------------------

/// One interval's traffic, both sides of the split.
///
/// Deltas, not totals. See the module docs.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Window {
    /// What the canary backend served this interval.
    pub canary: RouteTotals,
    /// What the production backend served this interval.
    pub stable: RouteTotals,
}

/// Why a pass did nothing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Hold {
    /// One or both sides saw too little traffic for the window to mean
    /// anything.
    ///
    /// **Not** a failure, and this distinction is the one most likely to be got
    /// wrong: a canary receiving no requests at 03:00 is a quiet service, not a
    /// broken one, and rolling it back for that would make the feature unusable
    /// on anything but the busiest routes.
    LowTraffic {
        /// Requests the canary saw.
        canary: u64,
        /// Requests the stable side saw.
        stable: u64,
    },
}

/// What one pass decided for one canary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Verdict {
    /// Do nothing, for the stated reason.
    Hold(Hold),
    /// Advance to a higher weight.
    Step {
        /// The weight to write.
        to: u32,
    },
    /// The last step is healthy; the canary is carrying all of the traffic.
    Promote,
    /// A gate was breached. Weight to zero, and disarm.
    RollBack {
        /// What breached, with the numbers, for the status annotation.
        reason: String,
    },
}

/// Decides what to do with one canary, given one window.
///
/// A pure function of its arguments: no clock, no cluster, no counters. That is
/// what lets the whole decision table below be a unit test rather than a
/// cluster.
///
/// # Order
///
/// The traffic gate comes first, and it has to. Checking error rates before
/// checking whether there were any requests would divide by a zero-ish number
/// and promote or roll back on one unlucky request.
pub fn decide(policy: &PromotionAnnotations, weight: u32, window: &Window) -> Verdict {
    if window.canary.requests < policy.min_requests
        || window.stable.requests < policy.min_requests
    {
        return Verdict::Hold(Hold::LowTraffic {
            canary: window.canary.requests,
            stable: window.stable.requests,
        });
    }

    // Both gates are evaluated against the canary's own window. Comparing to
    // stable rather than to an absolute is right for latency — a backend is
    // slow relative to what it replaced — and wrong for errors, where an
    // absolute budget is what anybody actually has.
    if let Some(percent) = window.canary.error_percent() {
        if percent > policy.max_5xx_percent {
            return Verdict::RollBack {
                reason: format!(
                    "5xx {percent:.2}% over {:.2}%",
                    policy.max_5xx_percent
                ),
            };
        }
    }

    // Skipped rather than failed when either side has no latency observations:
    // a window of requests answered entirely from this process — 503s for a
    // backend with no endpoints — has a request count but nothing to compare.
    if let (Some(canary), Some(stable)) = (
        window.canary.avg_latency_ms(),
        window.stable.avg_latency_ms(),
    ) {
        let ceiling = stable * policy.max_latency_factor;
        if canary > ceiling {
            return Verdict::RollBack {
                reason: format!(
                    "latency {canary:.1}ms over {ceiling:.1}ms \
                     ({stable:.1}ms x {:.2})",
                    policy.max_latency_factor
                ),
            };
        }
    }

    match policy.next_step(weight) {
        Some(to) => Verdict::Step { to },
        None => Verdict::Promote,
    }
}

/// Whether this canary is finished, one way or the other.
///
/// Both terminal states are read off the annotation rather than remembered,
/// because the guard has to survive a restart: a canary rolled back an hour ago
/// must still be refused after the pod is rescheduled onto another node.
pub fn is_finished(policy: &PromotionAnnotations) -> bool {
    policy.rolled_back() || policy.status.as_deref() == Some(STATUS_PROMOTED)
}

// ---------------------------------------------------------------------------
// The cluster seam
// ---------------------------------------------------------------------------

/// The one thing this loop does to the cluster.
///
/// A trait with a single method, so every test above the state machine runs
/// without a cluster, a client, or a network. The real implementation is
/// [`KubePatcher`] and it is twelve lines; everything interesting is on this
/// side of the boundary, which is the point.
pub trait IngressPatcher {
    /// Applies `annotations` to one Ingress's metadata, leaving every other
    /// field — and every annotation not named here — alone.
    fn patch(
        &self,
        ingress: &ObjectKey,
        annotations: Vec<(String, String)>,
    ) -> impl std::future::Future<Output = Result<(), String>> + Send;
}

/// Patches through the Kubernetes API, as a server-side apply.
#[derive(Clone)]
pub struct KubePatcher {
    client: kube::Client,
}

impl KubePatcher {
    /// A patcher on `client`.
    pub fn new(client: kube::Client) -> Self {
        KubePatcher { client }
    }
}

impl IngressPatcher for KubePatcher {
    /// One line, deliberately. Every API object this binary would otherwise
    /// have to name stays behind `ramjet-controller`; see
    /// [`patch_ingress_annotations`] for the server-side-apply semantics and
    /// why the apply is forced.
    async fn patch(
        &self,
        ingress: &ObjectKey,
        annotations: Vec<(String, String)>,
    ) -> Result<(), String> {
        patch_ingress_annotations(&self.client, ingress, &annotations).await
    }
}

// ---------------------------------------------------------------------------
// The loop
// ---------------------------------------------------------------------------

/// Whether a compiled host entry is the one a promotion target named.
///
/// Compared variant by variant rather than by rendering the host to a `String`:
/// this runs once per route per target per interval, and a table with ten
/// thousand routes would otherwise allocate ten thousand short-lived strings a
/// minute to find the one or two a canary is attached to.
fn host_matches(host: RouteHost<'_>, wanted: &str) -> bool {
    match host {
        RouteHost::Exact(name) => wanted == name,
        // The table stores a wildcard under its parent domain; the controller
        // named it the way it is displayed.
        RouteHost::Wildcard(parent) => wanted.strip_prefix("*.") == Some(parent),
        RouteHost::CatchAll => wanted == "*",
    }
}

/// Counters as of the end of the previous pass, per canary.
#[derive(Debug, Clone, Copy, Default)]
struct Baseline {
    totals: RouteTotals,
    canary: RouteTotals,
}

impl Baseline {
    /// The stable share, which is what the totals have left once the canary's
    /// subset is taken out.
    fn stable(&self) -> RouteTotals {
        self.totals.saturating_sub(&self.canary)
    }
}

/// Reads the counters, applies [`decide`], and patches.
pub struct Promoter<P> {
    patcher: P,
    audit: AuditSink,
    routes: Arc<SharedRouteTable>,
    history: Arc<GenerationHistory>,
    baselines: HashMap<ObjectKey, Baseline>,
    /// Deadline for each canary's next pass.
    due: HashMap<ObjectKey, Instant>,
    /// Canaries this process has finished with, so a patch that has not yet
    /// come back around through the watch cannot be acted on twice.
    finished: HashSet<ObjectKey>,
    /// Whether the last pass was suppressed by a rollback pin, so the log line
    /// about it is written once rather than every interval.
    pinned_reported: bool,
}

impl<P: IngressPatcher> Promoter<P> {
    /// A promoter reading `routes` and patching through `patcher`.
    pub fn new(
        patcher: P,
        audit: AuditSink,
        routes: Arc<SharedRouteTable>,
        history: Arc<GenerationHistory>,
    ) -> Self {
        Promoter {
            patcher,
            audit,
            routes,
            history,
            baselines: HashMap::new(),
            due: HashMap::new(),
            finished: HashSet::new(),
            pinned_reported: false,
        }
    }

    /// Runs until the control plane's channel closes.
    pub async fn run(mut self, mut configs: watch::Receiver<Arc<CompiledConfig>>) {
        loop {
            let targets = configs.borrow_and_update().promotions.clone();

            // Nobody has opted in. Nothing can become due without a new
            // generation, so wait for one rather than waking on a timer
            // forever — which is what makes this loop genuinely free on the
            // installations that never use it.
            if targets.is_empty() {
                if configs.changed().await.is_err() {
                    debug!("control plane stopped; automatic promotion is done");
                    return;
                }
                continue;
            }

            let sleep = self.pass(&targets, Instant::now()).await;
            tokio::time::sleep(sleep).await;
            if configs.has_changed().is_err() {
                debug!("control plane stopped; automatic promotion is done");
                return;
            }
        }
    }

    /// One pass over every candidate. Returns how long to sleep afterwards.
    async fn pass(&mut self, targets: &[PromotionTarget], now: Instant) -> Duration {
        // The whole interlock, in one place and at the top. A rollback pin
        // means an operator has taken manual control of what this replica
        // serves, and a loop that went on patching Ingresses underneath them
        // would be changing the cluster they are trying to hold still.
        if self.history.pinned().is_some() {
            if !self.pinned_reported {
                info!("a rollback pin is held; automatic promotion is paused");
                self.pinned_reported = true;
            }
            return IDLE_POLL;
        }
        if self.pinned_reported {
            info!("the rollback pin was released; automatic promotion resumes");
            self.pinned_reported = false;
        }

        // Forget the canaries that have gone, so a long-lived process does not
        // accumulate a baseline per Ingress ever deleted.
        let live: HashSet<&ObjectKey> = targets.iter().map(|t| &t.ingress).collect();
        self.baselines.retain(|key, _| live.contains(key));
        self.due.retain(|key, _| live.contains(key));
        self.finished.retain(|key| live.contains(key));

        let mut next = IDLE_POLL;
        for target in targets {
            if is_finished(&target.policy) || self.finished.contains(&target.ingress) {
                continue;
            }

            let deadline = *self.due.entry(target.ingress.clone()).or_insert(now);
            if deadline > now {
                next = next.min(deadline.saturating_duration_since(now));
                continue;
            }
            self.due
                .insert(target.ingress.clone(), now + target.policy.interval);
            next = next.min(target.policy.interval);

            self.evaluate(target).await;
        }
        next.max(MIN_TICK)
    }

    /// Reads one canary's window and acts on it.
    async fn evaluate(&mut self, target: &PromotionTarget) {
        let Some(reading) = self.read(&target.routes) else {
            // Its routes are not in the published table. Normal for the moment
            // between an Ingress being created and its generation landing.
            debug!(ingress = %target.ingress, "no counters for this canary yet");
            return;
        };

        let Some(previous) = self.baselines.insert(target.ingress.clone(), reading) else {
            // First sighting: there is no earlier reading to subtract, so this
            // pass establishes the baseline and decides nothing.
            debug!(ingress = %target.ingress, "baseline taken");
            return;
        };

        let window = Window {
            canary: reading.canary.saturating_sub(&previous.canary),
            stable: reading.stable().saturating_sub(&previous.stable()),
        };
        let verdict = decide(&target.policy, target.weight, &window);
        self.apply(target, &window, verdict).await;
    }

    /// Sums the counters of every route this canary shadows.
    fn read(&self, routes: &[PromotionRoute]) -> Option<Baseline> {
        let table = self.routes.load();
        let mut reading = Baseline::default();
        let mut found = false;

        for (host, rule) in table.routes() {
            let matches = routes.iter().any(|wanted| {
                host_matches(host, &wanted.host)
                    && wanted.path == rule.path()
                    && wanted.path_type == rule.path_type()
            });
            if !matches {
                continue;
            }
            let Some(slot) = table.route_stats().slot(rule.stats_index()) else {
                continue;
            };
            found = true;
            let totals = slot.totals();
            let canary = slot.canary_totals();
            reading.totals.requests += totals.requests;
            reading.totals.errors_5xx += totals.errors_5xx;
            reading.totals.upstream_latency_micros += totals.upstream_latency_micros;
            reading.totals.upstream_latency_count += totals.upstream_latency_count;
            reading.canary.requests += canary.requests;
            reading.canary.errors_5xx += canary.errors_5xx;
            reading.canary.upstream_latency_micros += canary.upstream_latency_micros;
            reading.canary.upstream_latency_count += canary.upstream_latency_count;
        }

        found.then_some(reading)
    }

    /// Writes what the verdict asks for, and says so everywhere.
    async fn apply(&mut self, target: &PromotionTarget, window: &Window, verdict: Verdict) {
        let ingress = target.ingress.to_string();
        let (reason, detail, to_weight, annotations) = match verdict {
            Verdict::Hold(Hold::LowTraffic { canary, stable }) => {
                // Deliberately not an Event. Holding is the normal state of a
                // canary on a quiet route, and an Event per interval per canary
                // would bury the three that matter.
                debug!(
                    %ingress,
                    canary_requests = canary,
                    stable_requests = stable,
                    min_requests = target.policy.min_requests,
                    "holding: too little traffic this window to judge"
                );
                return;
            }
            Verdict::Step { to } => (
                AuditReason::CanaryStepped,
                format!("canary healthy; weight {} -> {to}", target.weight),
                to,
                vec![(ANNOTATION_CANARY_WEIGHT.to_owned(), to.to_string())],
            ),
            Verdict::Promote => (
                AuditReason::CanaryPromoted,
                format!(
                    "canary healthy at {}%, the last step; swap the production \
                     backend when ready",
                    target.weight
                ),
                target.weight,
                // Only the status. `auto-promote` is the operator's annotation
                // and stays as they wrote it; the status is this controller's,
                // and it is what stops the loop coming back — see
                // `is_finished`. Writing `auto-promote: "false"` here would
                // edit an intent nobody withdrew.
                vec![(
                    ANNOTATION_AUTO_PROMOTE_STATUS.to_owned(),
                    STATUS_PROMOTED.to_owned(),
                )],
            ),
            Verdict::RollBack { ref reason } => (
                AuditReason::CanaryRolledBack,
                format!("rolled back from {}%: {reason}", target.weight),
                0,
                vec![
                    (ANNOTATION_CANARY_WEIGHT.to_owned(), "0".to_owned()),
                    // Both, and in one patch. Zeroing the weight stops the
                    // traffic; disarming stops this loop from stepping it
                    // straight back up on the next healthy-looking window.
                    (ANNOTATION_AUTO_PROMOTE.to_owned(), "false".to_owned()),
                    (
                        ANNOTATION_AUTO_PROMOTE_STATUS.to_owned(),
                        format!("{STATUS_ROLLED_BACK}: {reason}"),
                    ),
                ],
            ),
        };

        if let Err(error) = self.patcher.patch(&target.ingress, annotations).await {
            // Not fatal and not retried here: the next pass recomputes from the
            // current state of the cluster, which is the right behaviour if the
            // API server was briefly unavailable and the only safe one if the
            // patch actually landed and the response was lost.
            warn!(
                %ingress,
                %error,
                verdict = ?verdict,
                "could not patch the canary Ingress; will re-decide next interval"
            );
            return;
        }

        // Recorded only after the patch stuck, so a canary that could not be
        // disarmed is retried rather than quietly abandoned.
        if matches!(verdict, Verdict::Promote | Verdict::RollBack { .. }) {
            self.finished.insert(target.ingress.clone());
        }

        self.audit.canary(
            reason,
            &CanaryDecision {
                ingress: &ingress,
                // The Event goes on this Ingress, which needs its uid — see
                // `AuditSink`. A target compiled from an object that carried
                // none gets the log line and the webhook and no Event, rather
                // than an Event nothing will ever display.
                subject: target.uid.as_deref().map(|uid| EventSubject {
                    namespace: &target.ingress.namespace,
                    name: &target.ingress.name,
                    uid,
                }),
                from_weight: target.weight,
                to_weight,
                detail: &detail,
                canary_requests: window.canary.requests,
                canary_5xx_percent: window.canary.error_percent().unwrap_or(0.0),
                canary_latency_ms: window.canary.avg_latency_ms().unwrap_or(0.0),
                stable_requests: window.stable.requests,
                stable_5xx_percent: window.stable.error_percent().unwrap_or(0.0),
                stable_latency_ms: window.stable.avg_latency_ms().unwrap_or(0.0),
            },
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    // -----------------------------------------------------------------------
    // The state machine
    // -----------------------------------------------------------------------

    fn policy() -> PromotionAnnotations {
        PromotionAnnotations {
            enabled: true,
            ..PromotionAnnotations::default()
        }
    }

    /// A side of the window: `requests` served, `errors` of them 5xx, each
    /// taking `latency_ms` upstream.
    fn side(requests: u64, errors: u64, latency_ms: u64) -> RouteTotals {
        RouteTotals {
            requests,
            errors_5xx: errors,
            upstream_latency_micros: latency_ms * 1000 * requests,
            upstream_latency_count: requests,
        }
    }

    /// A window where both sides are healthy and identical.
    fn healthy() -> Window {
        Window {
            canary: side(100, 0, 10),
            stable: side(100, 0, 10),
        }
    }

    #[test]
    fn a_healthy_window_advances_one_step() {
        assert_eq!(
            decide(&policy(), 0, &healthy()),
            Verdict::Step { to: 5 },
            "the first pass goes to the first configured step"
        );
        assert_eq!(decide(&policy(), 5, &healthy()), Verdict::Step { to: 10 });
        assert_eq!(decide(&policy(), 25, &healthy()), Verdict::Step { to: 50 });
    }

    #[test]
    fn a_weight_between_steps_takes_the_next_one_up() {
        // Somebody set `canary-weight: 30` by hand and then armed the loop.
        // Rounding down would be a demotion nobody asked for.
        assert_eq!(decide(&policy(), 30, &healthy()), Verdict::Step { to: 50 });
    }

    #[test]
    fn the_last_step_promotes_rather_than_stepping() {
        assert_eq!(decide(&policy(), 100, &healthy()), Verdict::Promote);
    }

    /// The last step is validated before it is declared done: reaching 100%
    /// and immediately calling it promoted would mean full traffic never got a
    /// single window of scrutiny.
    #[test]
    fn the_final_weight_is_judged_before_it_is_accepted() {
        let breached = Window {
            canary: side(100, 50, 10),
            stable: side(100, 0, 10),
        };
        assert!(matches!(
            decide(&policy(), 100, &breached),
            Verdict::RollBack { .. }
        ));
    }

    #[test]
    fn too_little_traffic_holds_rather_than_advancing_or_rolling_back() {
        // The distinction most likely to be got wrong. A canary receiving
        // nothing at 03:00 is a quiet service, not a broken one.
        let quiet = Window {
            canary: side(3, 0, 10),
            stable: side(4000, 0, 10),
        };
        assert_eq!(
            decide(&policy(), 5, &quiet),
            Verdict::Hold(Hold::LowTraffic {
                canary: 3,
                stable: 4000
            })
        );
    }

    #[test]
    fn a_quiet_production_side_holds_too() {
        // Both sides gate, because a latency comparison against four stable
        // requests is not a comparison.
        let window = Window {
            canary: side(4000, 0, 10),
            stable: side(2, 0, 10),
        };
        assert!(matches!(
            decide(&policy(), 5, &window),
            Verdict::Hold(Hold::LowTraffic { .. })
        ));
    }

    #[test]
    fn an_empty_window_holds_and_never_divides_by_zero() {
        assert!(matches!(
            decide(&policy(), 5, &Window::default()),
            Verdict::Hold(Hold::LowTraffic {
                canary: 0,
                stable: 0
            })
        ));
    }

    #[test]
    fn a_window_exactly_at_the_minimum_is_evidence() {
        let at_minimum = Window {
            canary: side(50, 0, 10),
            stable: side(50, 0, 10),
        };
        assert_eq!(at_minimum.canary.requests, DEFAULT_MIN);
        assert_eq!(decide(&policy(), 5, &at_minimum), Verdict::Step { to: 10 });
    }

    /// `min_requests` as the annotation defaults it, spelled out so the test
    /// above is checking the boundary rather than restating a literal.
    const DEFAULT_MIN: u64 = 50;

    #[test]
    fn a_five_xx_rate_over_the_budget_rolls_back() {
        // Two of a hundred is 2%, against a default budget of 1%.
        let window = Window {
            canary: side(100, 2, 10),
            stable: side(100, 0, 10),
        };
        match decide(&policy(), 25, &window) {
            Verdict::RollBack { reason } => {
                assert!(reason.contains("5xx"), "{reason}");
                assert!(reason.contains("2.00%"), "{reason}");
                assert!(reason.contains("1.00%"), "{reason}");
            }
            other => panic!("expected a rollback, got {other:?}"),
        }
    }

    #[test]
    fn a_five_xx_rate_exactly_at_the_budget_is_allowed() {
        // The threshold is a budget, and spending all of it is not overspending.
        let window = Window {
            canary: side(100, 1, 10),
            stable: side(100, 0, 10),
        };
        assert_eq!(decide(&policy(), 25, &window), Verdict::Step { to: 50 });
    }

    #[test]
    fn errors_are_judged_absolutely_and_not_against_stable() {
        // Production being on fire is not a licence to promote a canary that
        // is also on fire. An error budget is an absolute number.
        let window = Window {
            canary: side(100, 5, 10),
            stable: side(100, 40, 10),
        };
        assert!(matches!(
            decide(&policy(), 25, &window),
            Verdict::RollBack { .. }
        ));
    }

    #[test]
    fn latency_over_the_factor_rolls_back() {
        // 20ms against 10ms stable is 2.0x, over the 1.5x default.
        let window = Window {
            canary: side(100, 0, 20),
            stable: side(100, 0, 10),
        };
        match decide(&policy(), 10, &window) {
            Verdict::RollBack { reason } => {
                assert!(reason.contains("latency"), "{reason}");
                assert!(reason.contains("20.0ms"), "{reason}");
                assert!(reason.contains("15.0ms"), "{reason}");
            }
            other => panic!("expected a rollback, got {other:?}"),
        }
    }

    #[test]
    fn latency_inside_the_factor_advances() {
        let window = Window {
            canary: side(100, 0, 14),
            stable: side(100, 0, 10),
        };
        assert_eq!(decide(&policy(), 10, &window), Verdict::Step { to: 25 });
    }

    #[test]
    fn a_faster_canary_is_never_a_problem() {
        let window = Window {
            canary: side(100, 0, 1),
            stable: side(100, 0, 100),
        };
        assert_eq!(decide(&policy(), 10, &window), Verdict::Step { to: 25 });
    }

    #[test]
    fn latency_is_relative_so_a_slow_service_can_still_promote() {
        // Both sides at two seconds. Absolute thresholds would make this
        // feature unusable on anything that is legitimately slow.
        let window = Window {
            canary: side(100, 0, 2000),
            stable: side(100, 0, 2000),
        };
        assert_eq!(decide(&policy(), 50, &window), Verdict::Step { to: 100 });
    }

    #[test]
    fn the_error_gate_is_checked_before_the_latency_gate() {
        // Both breached. The 5xx reason is the more actionable one and should
        // be what the annotation ends up saying.
        let window = Window {
            canary: side(100, 50, 100),
            stable: side(100, 0, 10),
        };
        match decide(&policy(), 25, &window) {
            Verdict::RollBack { reason } => assert!(reason.starts_with("5xx"), "{reason}"),
            other => panic!("expected a rollback, got {other:?}"),
        }
    }

    #[test]
    fn a_window_with_no_latency_observations_skips_the_latency_gate() {
        // Every request answered from inside this process — a backend with no
        // ready endpoints 503s without ever dialling upstream — has requests
        // but no latency to compare. The 5xx gate still catches it.
        let window = Window {
            canary: RouteTotals {
                requests: 100,
                errors_5xx: 0,
                upstream_latency_micros: 0,
                upstream_latency_count: 0,
            },
            stable: side(100, 0, 10),
        };
        assert_eq!(decide(&policy(), 10, &window), Verdict::Step { to: 25 });
    }

    #[test]
    fn configured_thresholds_are_the_ones_applied() {
        let strict = PromotionAnnotations {
            enabled: true,
            steps: vec![50, 100],
            max_5xx_percent: 0.1,
            max_latency_factor: 1.05,
            min_requests: 10,
            ..PromotionAnnotations::default()
        };

        let window = Window {
            canary: side(1000, 5, 10),
            stable: side(1000, 0, 10),
        };
        assert!(
            matches!(decide(&strict, 0, &window), Verdict::RollBack { .. }),
            "0.5% must breach a 0.1% budget"
        );

        let clean = Window {
            canary: side(20, 0, 10),
            stable: side(20, 0, 10),
        };
        assert_eq!(
            decide(&strict, 0, &clean),
            Verdict::Step { to: 50 },
            "a lowered min-requests must let a small window count"
        );
        assert_eq!(
            decide(&strict, 50, &clean),
            Verdict::Step { to: 100 },
            "a two-step ladder still has to be walked one rung at a time"
        );
        assert_eq!(decide(&strict, 100, &clean), Verdict::Promote);
    }

    #[test]
    fn hosts_are_matched_in_the_spelling_the_controller_used() {
        // The table stores `*.example.com` under `example.com`, so a comparison
        // that forgot the wildcard would silently find no counters and hold
        // forever — which looks exactly like a quiet route.
        assert!(host_matches(RouteHost::Exact("app.example.com"), "app.example.com"));
        assert!(!host_matches(RouteHost::Exact("app.example.com"), "other.example.com"));

        assert!(host_matches(RouteHost::Wildcard("example.com"), "*.example.com"));
        assert!(
            !host_matches(RouteHost::Wildcard("example.com"), "example.com"),
            "the parent domain is not the name the table serves"
        );

        assert!(host_matches(RouteHost::CatchAll, "*"));
        assert!(!host_matches(RouteHost::CatchAll, "example.com"));
    }

    #[test]
    fn the_flap_guard_reads_both_terminal_states() {
        let mut rolled = policy();
        rolled.status = Some("rolled-back: 5xx 9.0% over 1.00%".to_owned());
        assert!(is_finished(&rolled));

        let mut promoted = policy();
        promoted.status = Some(STATUS_PROMOTED.to_owned());
        assert!(is_finished(&promoted));

        assert!(!is_finished(&policy()), "an armed canary is not finished");
    }

    // -----------------------------------------------------------------------
    // The loop
    // -----------------------------------------------------------------------

    /// One patch that was applied: which Ingress, and which annotations.
    type Call = (String, Vec<(String, String)>);

    /// Records what would have been patched, and can be told to fail.
    #[derive(Default)]
    struct FakePatcher {
        applied: Mutex<Vec<Call>>,
        fail: bool,
    }

    impl FakePatcher {
        fn calls(&self) -> Vec<Call> {
            self.applied.lock().map(|v| v.clone()).unwrap_or_default()
        }
    }

    impl IngressPatcher for &FakePatcher {
        async fn patch(
            &self,
            ingress: &ObjectKey,
            annotations: Vec<(String, String)>,
        ) -> Result<(), String> {
            if self.fail {
                return Err("the API server said no".to_owned());
            }
            if let Ok(mut applied) = self.applied.lock() {
                applied.push((ingress.to_string(), annotations));
            }
            Ok(())
        }
    }

    use ramjet_proxy::{CertStore, CertKeys};
    use ramjet_router::{
        CanaryRules, Endpoint, LbPolicy, PathType, RouteTable, RouteTableBuilder,
    };

    /// A table with `app.example.com/` carrying a canary, which is the shape
    /// every promotion target points at.
    fn table() -> RouteTable {
        let mut builder = RouteTableBuilder::new();
        builder.generation(1);
        for name in ["prod/api:80", "prod/api-next:80"] {
            builder
                .backend(
                    name,
                    LbPolicy::RoundRobin,
                    vec![Endpoint::new("10.0.0.1:8080".parse().expect("an address"))],
                )
                .expect("registers");
        }
        builder
            .canary_route(
                Some("app.example.com"),
                "/",
                PathType::Prefix,
                "prod/api:80",
                &CanaryRules {
                    backend: "prod/api-next:80",
                    weight: 5,
                    ..Default::default()
                },
            )
            .expect("drafts");
        builder.build().expect("builds")
    }

    fn target(weight: u32) -> PromotionTarget {
        PromotionTarget {
            ingress: ObjectKey {
                namespace: "prod".to_owned(),
                name: "web-canary".to_owned(),
            },
            uid: Some("3f2b1c0d-0000-4000-8000-000000000001".to_owned()),
            routes: vec![PromotionRoute {
                host: "app.example.com".to_owned(),
                path: "/".to_owned(),
                path_type: PathType::Prefix,
            }],
            weight,
            policy: policy(),
        }
    }

    /// A promoter over a real route table, with a fake cluster.
    fn promoter(
        patcher: &FakePatcher,
    ) -> (Promoter<&FakePatcher>, Arc<SharedRouteTable>, Arc<GenerationHistory>) {
        let routes = Arc::new(SharedRouteTable::new(table()));
        let history = Arc::new(GenerationHistory::new(
            Arc::clone(&routes),
            Arc::new(CertStore::new()),
            10,
        ));
        let promoter = Promoter::new(
            patcher,
            AuditSink::logging_only(),
            Arc::clone(&routes),
            Arc::clone(&history),
        );
        (promoter, routes, history)
    }

    /// Serves `stable` clean requests and `canary` requests of which
    /// `canary_errors` were 5xx, each taking `latency_ms`.
    fn serve(
        routes: &SharedRouteTable,
        stable: u64,
        canary: u64,
        canary_errors: u64,
        latency_ms: u64,
    ) {
        let table = routes.load();
        let (_, rule) = table.routes().next().expect("one route");
        let slot = table
            .route_stats()
            .slot(rule.stats_index())
            .expect("a counter block");

        for _ in 0..stable {
            slot.shard(0).record_response(200);
            slot.shard(0)
                .record_upstream_latency(Duration::from_millis(10));
        }
        for i in 0..canary {
            let status = if i < canary_errors { 503 } else { 200 };
            // Both blocks, which is the invariant the split rests on.
            slot.shard(0).record_response(status);
            slot.canary_shard(0).record_response(status);
            slot.shard(0)
                .record_upstream_latency(Duration::from_millis(latency_ms));
            slot.canary_shard(0)
                .record_upstream_latency(Duration::from_millis(latency_ms));
        }
    }

    #[tokio::test]
    async fn the_first_pass_only_takes_a_baseline() {
        // There is no earlier reading to subtract, so a first pass that decided
        // anything would be deciding on the whole lifetime of the process.
        let patcher = FakePatcher::default();
        let (mut promoter, routes, _history) = promoter(&patcher);
        serve(&routes, 1000, 1000, 900, 10);

        promoter.pass(&[target(5)], Instant::now()).await;
        assert!(
            patcher.calls().is_empty(),
            "a first sighting must not act, however bad the lifetime numbers look"
        );
    }

    #[tokio::test]
    async fn a_healthy_window_steps_the_weight_annotation() {
        let patcher = FakePatcher::default();
        let (mut promoter, routes, _history) = promoter(&patcher);
        let now = Instant::now();

        promoter.pass(&[target(5)], now).await;
        serve(&routes, 200, 200, 0, 10);
        promoter.pass(&[target(5)], now + Duration::from_secs(60)).await;

        assert_eq!(
            patcher.calls(),
            vec![(
                "prod/web-canary".to_owned(),
                vec![(ANNOTATION_CANARY_WEIGHT.to_owned(), "10".to_owned())]
            )]
        );
    }

    #[tokio::test]
    async fn a_breach_zeroes_the_weight_and_disarms_in_one_patch() {
        let patcher = FakePatcher::default();
        let (mut promoter, routes, _history) = promoter(&patcher);
        let now = Instant::now();

        promoter.pass(&[target(25)], now).await;
        // 20% of the canary's requests failed, against a 1% budget.
        serve(&routes, 200, 200, 40, 10);
        promoter.pass(&[target(25)], now + Duration::from_secs(60)).await;

        let calls = patcher.calls();
        assert_eq!(calls.len(), 1);
        let keys: Vec<&str> = calls[0].1.iter().map(|(k, _)| k.as_str()).collect();
        assert_eq!(
            keys,
            vec![
                ANNOTATION_CANARY_WEIGHT,
                ANNOTATION_AUTO_PROMOTE,
                ANNOTATION_AUTO_PROMOTE_STATUS
            ],
            "one patch, or a half-applied rollback leaves traffic on a broken backend"
        );
        assert_eq!(calls[0].1[0].1, "0");
        assert_eq!(calls[0].1[1].1, "false");
        assert!(calls[0].1[2].1.starts_with(STATUS_ROLLED_BACK));
        assert!(calls[0].1[2].1.contains("5xx"), "{:?}", calls[0].1[2].1);
    }

    #[tokio::test]
    async fn a_rolled_back_canary_is_never_touched_again() {
        // The flap guard. Without it the next healthy window steps the weight
        // straight back up and traffic oscillates across a broken backend.
        let patcher = FakePatcher::default();
        let (mut promoter, routes, _history) = promoter(&patcher);
        let now = Instant::now();

        promoter.pass(&[target(25)], now).await;
        serve(&routes, 200, 200, 40, 10);
        promoter.pass(&[target(25)], now + Duration::from_secs(60)).await;
        assert_eq!(patcher.calls().len(), 1);

        // The cluster has not caught up yet, so the target still looks armed.
        serve(&routes, 200, 200, 0, 10);
        promoter.pass(&[target(25)], now + Duration::from_secs(120)).await;
        assert_eq!(
            patcher.calls().len(),
            1,
            "an in-memory guard has to cover the gap before the watch comes round"
        );

        // And once it has, the annotation carries the guard across a restart.
        let mut rolled = target(0);
        rolled.policy.status = Some("rolled-back: 5xx 20.00% over 1.00%".to_owned());
        let mut fresh = Promoter::new(
            &patcher,
            AuditSink::logging_only(),
            Arc::clone(&routes),
            Arc::new(GenerationHistory::new(
                Arc::clone(&routes),
                Arc::new(CertStore::new()),
                10,
            )),
        );
        fresh.pass(&[rolled.clone()], now).await;
        serve(&routes, 200, 200, 0, 10);
        fresh.pass(&[rolled], now + Duration::from_secs(60)).await;
        assert_eq!(patcher.calls().len(), 1, "a restart must not re-arm it");
    }

    #[tokio::test]
    async fn the_last_step_writes_promoted_and_stops() {
        let patcher = FakePatcher::default();
        let (mut promoter, routes, _history) = promoter(&patcher);
        let now = Instant::now();

        promoter.pass(&[target(100)], now).await;
        serve(&routes, 200, 200, 0, 10);
        promoter.pass(&[target(100)], now + Duration::from_secs(60)).await;

        let calls = patcher.calls();
        assert_eq!(
            calls[0].1,
            vec![(
                ANNOTATION_AUTO_PROMOTE_STATUS.to_owned(),
                STATUS_PROMOTED.to_owned()
            )],
            "promotion writes only the status; auto-promote is the operator's annotation"
        );

        serve(&routes, 200, 200, 0, 10);
        promoter.pass(&[target(100)], now + Duration::from_secs(120)).await;
        assert_eq!(calls.len(), patcher.calls().len(), "promotion is terminal");
    }

    #[tokio::test]
    async fn a_rollback_pin_pauses_everything() {
        // An operator holding the emergency brake has taken manual control of
        // what this replica serves; patching Ingresses underneath them would be
        // changing the cluster they are trying to hold still.
        let patcher = FakePatcher::default();
        let (mut promoter, routes, history) = promoter(&patcher);
        let now = Instant::now();

        promoter.pass(&[target(5)], now).await;
        serve(&routes, 200, 200, 0, 10);

        let table = routes.load_full();
        history.record(
            table.generation(),
            0,
            Arc::new(ramjet_controller::ConfigDiff::default().to_json()),
            table,
            Arc::new(CertKeys::new()),
        );
        history.pin(1).expect("generation 1 is in the ring");

        promoter.pass(&[target(5)], now + Duration::from_secs(60)).await;
        assert!(patcher.calls().is_empty(), "a pin must pause promotion");

        history.unpin();
        promoter.pass(&[target(5)], now + Duration::from_secs(120)).await;
        assert_eq!(patcher.calls().len(), 1, "releasing it resumes");
    }

    #[tokio::test]
    async fn a_target_is_not_evaluated_before_its_interval_elapses() {
        let patcher = FakePatcher::default();
        let (mut promoter, routes, _history) = promoter(&patcher);
        let now = Instant::now();

        promoter.pass(&[target(5)], now).await;
        serve(&routes, 200, 200, 0, 10);

        promoter.pass(&[target(5)], now + Duration::from_secs(30)).await;
        assert!(patcher.calls().is_empty(), "the interval is 60s by default");

        promoter.pass(&[target(5)], now + Duration::from_secs(61)).await;
        assert_eq!(patcher.calls().len(), 1);
    }

    #[tokio::test]
    async fn a_failed_patch_is_retried_by_the_next_pass() {
        // Not retried in place: if the patch actually landed and only the
        // response was lost, recomputing from the cluster's current state is
        // the only safe thing to do.
        let patcher = FakePatcher {
            fail: true,
            ..Default::default()
        };
        let (mut promoter, routes, _history) = promoter(&patcher);
        let now = Instant::now();

        promoter.pass(&[target(100)], now).await;
        serve(&routes, 200, 200, 0, 10);
        promoter.pass(&[target(100)], now + Duration::from_secs(60)).await;

        // The verdict was `Promote`, but the patch failed, so the canary must
        // not have been marked finished.
        assert!(!promoter.finished.contains(&target(100).ingress));
    }

    #[tokio::test]
    async fn a_canary_whose_routes_are_not_in_the_table_is_skipped() {
        let patcher = FakePatcher::default();
        let (mut promoter, _routes, _history) = promoter(&patcher);
        let mut elsewhere = target(5);
        elsewhere.routes = vec![PromotionRoute {
            host: "nobody.example.com".to_owned(),
            path: "/".to_owned(),
            path_type: PathType::Prefix,
        }];

        promoter.pass(&[elsewhere.clone()], Instant::now()).await;
        assert!(promoter.baselines.is_empty(), "no counters, no baseline");
        assert!(patcher.calls().is_empty());
    }

    #[tokio::test]
    async fn baselines_are_forgotten_when_a_canary_goes_away() {
        // A process that runs for months must not accumulate one entry per
        // Ingress that ever existed.
        let patcher = FakePatcher::default();
        let (mut promoter, _routes, _history) = promoter(&patcher);
        let now = Instant::now();

        promoter.pass(&[target(5)], now).await;
        assert_eq!(promoter.baselines.len(), 1);

        promoter.pass(&[], now + Duration::from_secs(60)).await;
        assert!(promoter.baselines.is_empty());
        assert!(promoter.due.is_empty());
    }

    #[tokio::test]
    async fn a_window_measures_the_interval_and_not_the_lifetime() {
        // The property the whole design rests on. A process up for a week with
        // a clean history must still catch a canary that started failing a
        // minute ago.
        let patcher = FakePatcher::default();
        let (mut promoter, routes, _history) = promoter(&patcher);
        let now = Instant::now();

        // A week of clean traffic.
        serve(&routes, 100_000, 100_000, 0, 10);
        promoter.pass(&[target(25)], now).await;

        // Then one bad minute.
        serve(&routes, 200, 200, 100, 10);
        promoter.pass(&[target(25)], now + Duration::from_secs(60)).await;

        let calls = patcher.calls();
        assert_eq!(calls.len(), 1);
        assert_eq!(
            calls[0].1[0].1, "0",
            "a lifetime average would have buried this window entirely"
        );
    }
}
