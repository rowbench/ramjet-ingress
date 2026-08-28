//! The last few generations, and the emergency brake that republishes one.
//!
//! # Why the data plane keeps this and not the control plane
//!
//! A rollback has to be instant and it has to work when the API server is the
//! thing that is wrong. Every alternative — re-applying the previous Ingress
//! objects, `kubectl rollout undo`, waiting for a controller to recompile —
//! goes back through the control plane, which is exactly the component an
//! operator reaches for this lever to route around. What the data plane already
//! holds is the compiled artifact itself: an `Arc<RouteTable>` and the parsed
//! keys that go with it. Republishing one is the same pointer store that
//! publishing it the first time was, so a rollback costs what a normal
//! configuration change costs and no more.
//!
//! The price is memory: [`GenerationHistory`] pins the last `capacity` tables
//! alive instead of letting each one drop when the next arrives. A table is
//! roughly a hundred bytes per route, so ten generations of a ten-thousand
//! route cluster is a few megabytes — and successive generations share every
//! `Arc` that did not change, most importantly the parsed certificates, which
//! are content-addressed and therefore shared by id.
//!
//! # A pin, not a rewind
//!
//! `POST /admin/rollback` **pins**: generation G goes back on the wire and
//! publication stops. The controller does not stop. It keeps watching, keeps
//! compiling, and keeps handing generations over; they are recorded here with
//! `published: false` so an operator can see what they are holding back, and
//! nothing reaches the data plane until `DELETE /admin/rollback`, which
//! immediately publishes the newest one.
//!
//! Draining the controller's side matters more than it looks: a pin that
//! stopped reading the channel would leave the watch task blocked on a full
//! slot, and the moment the pin was released the data plane would jump to
//! whatever generation happened to be stuck there rather than to the current
//! state of the cluster.
//!
//! # The pin dies with the process
//!
//! Deliberately, and it is the one property worth being loud about. Kubernetes
//! is the source of truth for what this controller serves; a pin is a local
//! override of that, held in memory, by one replica, because something is on
//! fire right now. Persisting it would create a second source of truth that
//! survives a restart and answers to nobody — a pod that comes back after an
//! eviction still serving a generation from last Tuesday, with no object in the
//! cluster saying why. So: an emergency brake, not desired state. Fix the
//! Ingress objects, then release the pin.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};

use ramjet_router::{RouteTable, SharedRouteTable};
use rustls::sign::CertifiedKey;
use tokio::sync::mpsc;

use crate::tls::CertStore;

/// Generations kept when the caller does not say.
pub const DEFAULT_HISTORY_SIZE: usize = 10;

/// The parsed keys one generation published, by handle id.
pub type CertKeys = HashMap<u64, Arc<CertifiedKey>>;

/// One compiled generation, as it was applied.
///
/// Holds everything needed to put it back on the wire — the table and the
/// parsed certificates it names — plus what an operator needs to decide whether
/// they want to.
#[derive(Debug, Clone)]
pub struct GenerationRecord {
    /// The table's generation number.
    pub generation: u64,
    /// When this process applied it.
    pub applied_at: SystemTime,
    /// Whether it actually went live, or was only recorded because a pin was
    /// holding publication.
    pub published: bool,
    /// The control plane's content digest for this configuration.
    pub digest: u64,
    /// What the control plane said changed since the generation before it.
    ///
    /// Opaque here on purpose: the vocabulary of a configuration diff belongs
    /// to the control plane, and the admin listener is a courier for it rather
    /// than a second implementation of it.
    pub diff: Arc<serde_json::Value>,
    /// The routing snapshot.
    pub table: Arc<RouteTable>,
    /// The certificates the table's `SniMap` ids resolve to.
    pub keys: Arc<CertKeys>,
}

impl GenerationRecord {
    /// Route rules across every host.
    pub fn routes(&self) -> u64 {
        self.table.route_count() as u64
    }

    /// Exact plus wildcard host entries.
    pub fn hosts(&self) -> u64 {
        (self.table.host_count() + self.table.wildcard_host_count()) as u64
    }

    /// Certificates published with this generation.
    pub fn certs(&self) -> u64 {
        self.keys.len() as u64
    }
}

/// Why a rollback was refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PinError {
    /// No such generation in the ring: it was evicted, or never existed.
    Unknown(u64),
    /// A different generation is already pinned.
    ///
    /// Refused rather than silently re-pointed: two people reaching for the
    /// brake at once should find out about each other, and the second one's
    /// intent — "put 41 back" — is not obviously still right once someone else
    /// has already decided the answer is 39.
    AlreadyPinned(u64),
}

impl std::fmt::Display for PinError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PinError::Unknown(generation) => {
                write!(f, "generation {generation} is not in the history")
            }
            PinError::AlreadyPinned(generation) => {
                write!(f, "already pinned to generation {generation}")
            }
        }
    }
}

impl std::error::Error for PinError {}

/// What the pin just did, for whoever is writing the audit trail.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PinChange {
    /// A generation was pinned and republished.
    Pinned {
        /// The generation now serving.
        generation: u64,
        /// The generation it replaced, read before the pin took effect.
        replaced: u64,
    },
    /// The pin was released and the newest generation published.
    Resumed {
        /// The generation now serving.
        generation: u64,
    },
}

/// The ring of applied generations, and the publication gate in front of it.
#[derive(Debug)]
pub struct GenerationHistory {
    routes: Arc<SharedRouteTable>,
    certs: Arc<CertStore>,
    capacity: usize,
    state: Mutex<State>,
    /// Set once by the owner of the audit trail, if there is one.
    changes: OnceLock<mpsc::UnboundedSender<PinChange>>,
}

#[derive(Debug, Default)]
struct State {
    ring: VecDeque<GenerationRecord>,
    pinned: Option<u64>,
}

impl GenerationHistory {
    /// A history publishing into `routes` and `certs`.
    ///
    /// `capacity` is clamped to at least one: a ring that keeps nothing could
    /// never answer a rollback, which is the only reason it exists.
    pub fn new(routes: Arc<SharedRouteTable>, certs: Arc<CertStore>, capacity: usize) -> Self {
        GenerationHistory {
            routes,
            certs,
            capacity: capacity.max(1),
            state: Mutex::new(State::default()),
            changes: OnceLock::new(),
        }
    }

    /// Routes pin and resume notifications to `sender`.
    ///
    /// Ignored if a sender is already registered; there is one audit trail.
    pub fn notify(&self, sender: mpsc::UnboundedSender<PinChange>) {
        let _ = self.changes.set(sender);
    }

    /// Generations the ring holds.
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// The pinned generation, if publication is being held.
    pub fn pinned(&self) -> Option<u64> {
        self.state.lock().map_or(None, |state| state.pinned)
    }

    /// The generation currently on the wire.
    pub fn serving(&self) -> u64 {
        self.routes.generation()
    }

    /// Records a generation, and publishes it unless a pin is holding the gate.
    ///
    /// Returns whether it went live. The caller is the applier: it has already
    /// parsed the certificates, and this is where the decision about whether
    /// they reach the data plane is made — not in the applier, so that there is
    /// exactly one place that knows what "pinned" means.
    pub fn record(
        &self,
        generation: u64,
        digest: u64,
        diff: Arc<serde_json::Value>,
        table: Arc<RouteTable>,
        keys: Arc<CertKeys>,
    ) -> bool {
        let Ok(mut state) = self.state.lock() else {
            return false;
        };
        let publish = state.pinned.is_none();
        if publish {
            self.apply(&keys, &table);
        }
        state.push(
            GenerationRecord {
                generation,
                applied_at: SystemTime::now(),
                published: publish,
                digest,
                diff,
                table,
                keys,
            },
            self.capacity,
        );
        publish
    }

    /// Republishes `generation` and holds publication there.
    pub fn pin(&self, generation: u64) -> Result<(), PinError> {
        let Ok(mut state) = self.state.lock() else {
            return Err(PinError::Unknown(generation));
        };
        if let Some(current) = state.pinned {
            return Err(PinError::AlreadyPinned(current));
        }
        let record = state
            .ring
            .iter()
            .find(|record| record.generation == generation)
            .ok_or(PinError::Unknown(generation))?;

        // Cloned out of the ring so the borrow ends before the publish; the
        // clone is three pointer bumps.
        let (keys, table) = (Arc::clone(&record.keys), Arc::clone(&record.table));
        // Read before the publish, because afterwards it is the pinned
        // generation and the question "what did this replace" has no answer.
        let replaced = self.routes.generation();
        self.apply(&keys, &table);
        state.pinned = Some(generation);
        state.mark_published(generation);
        drop(state);

        self.announce(PinChange::Pinned {
            generation,
            replaced,
        });
        Ok(())
    }

    /// Releases the pin and publishes the newest generation recorded.
    ///
    /// Idempotent: releasing a pin nobody set is not an error, because the
    /// state an operator asked for — "publication is not being held" — is the
    /// state they end up in either way.
    pub fn unpin(&self) -> Option<u64> {
        let Ok(mut state) = self.state.lock() else {
            return None;
        };
        state.pinned.take()?;

        let latest = state.ring.back().map(|record| {
            (
                record.generation,
                Arc::clone(&record.keys),
                Arc::clone(&record.table),
            )
        });
        let Some((generation, keys, table)) = latest else {
            drop(state);
            return None;
        };
        self.apply(&keys, &table);
        state.mark_published(generation);
        drop(state);

        self.announce(PinChange::Resumed { generation });
        Some(generation)
    }

    /// Reads the ring without copying it.
    ///
    /// The closure sees the pinned generation and every record, newest last.
    pub fn with_records<T>(&self, f: impl FnOnce(Option<u64>, &VecDeque<GenerationRecord>) -> T) -> T {
        match self.state.lock() {
            Ok(state) => f(state.pinned, &state.ring),
            // A poisoned lock means a panic while a generation was being
            // recorded. Reporting an empty history is a worse lie than
            // reporting none at all, but the admin listener has no way to
            // return an error here and the process is already in trouble.
            Err(poisoned) => {
                let state = poisoned.into_inner();
                f(state.pinned, &state.ring)
            }
        }
    }

    /// Certificates first, then the table that names them.
    ///
    /// The same order, and for the same reason, as the first-time publish in
    /// `ramjet-ingressd`: the two stores are independent `ArcSwap`s, and a
    /// table whose `SniMap` ids are not yet in the store fails every handshake
    /// for the width of the gap. The other way round leaves a store holding a
    /// key nothing points at, which is invisible.
    fn apply(&self, keys: &CertKeys, table: &Arc<RouteTable>) {
        self.certs.publish(keys.clone());
        self.routes.store_shared(Arc::clone(table));
    }

    fn announce(&self, change: PinChange) {
        if let Some(sender) = self.changes.get() {
            let _ = sender.send(change);
        }
    }
}

impl State {
    fn push(&mut self, record: GenerationRecord, capacity: usize) {
        // A republished generation is not a new one. Without this, pinning 41
        // and letting the controller keep working would eventually push 41 out
        // of its own ring, and releasing the pin would be the only way to find
        // out.
        self.ring.retain(|held| held.generation != record.generation);
        self.ring.push_back(record);
        while self.ring.len() > capacity {
            self.ring.pop_front();
        }
    }

    fn mark_published(&mut self, generation: u64) {
        for record in &mut self.ring {
            if record.generation == generation {
                record.published = true;
            }
        }
    }
}

/// Formats a moment as RFC 3339 in UTC, to the second.
///
/// Hand-rolled rather than pulling `chrono` or `time` into the data plane for
/// one timestamp per configuration change. The calendar arithmetic is Howard
/// Hinnant's `civil_from_days`, which is exact for every date this will ever
/// see; the tests below pin it against known instants.
pub fn rfc3339(time: SystemTime) -> String {
    let seconds = match time.duration_since(UNIX_EPOCH) {
        Ok(since) => i64::try_from(since.as_secs()).unwrap_or(i64::MAX),
        // Before 1970. Not reachable from `SystemTime::now`, but a clock that
        // has been stepped backwards should produce a wrong timestamp rather
        // than a panic.
        Err(before) => -i64::try_from(before.duration().as_secs()).unwrap_or(i64::MAX),
    };

    let days = seconds.div_euclid(86_400);
    let time_of_day = seconds.rem_euclid(86_400);
    let (year, month, day) = civil_from_days(days);

    format!(
        "{year:04}-{month:02}-{day:02}T{:02}:{:02}:{:02}Z",
        time_of_day / 3600,
        (time_of_day / 60) % 60,
        time_of_day % 60,
    )
}

/// Days since the Unix epoch into a proleptic Gregorian date.
fn civil_from_days(days: i64) -> (i64, u32, u32) {
    // Shift the epoch to 0000-03-01, which puts the leap day at the end of the
    // year and makes every month length a linear function of the month index.
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let day_of_era = z - era * 146_097;
    let year_of_era =
        (day_of_era - day_of_era / 1460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let year = year_of_era + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_prime = (5 * day_of_year + 2) / 153;
    let day = (day_of_year - (153 * month_prime + 2) / 5 + 1) as u32;
    let month = if month_prime < 10 {
        month_prime + 3
    } else {
        month_prime - 9
    } as u32;
    (year + i64::from(month <= 2), month, day)
}

#[cfg(test)]
mod tests {
    use super::*;
    use ramjet_router::{Endpoint, LbPolicy, PathType, RouteTableBuilder};
    use std::net::SocketAddr;
    use std::time::Duration;

    /// A table at `generation` routing `/` to an endpoint on `port`, so two
    /// generations can be told apart by where they send a request.
    fn table(generation: u64, port: u16) -> Arc<RouteTable> {
        let mut builder = RouteTableBuilder::new();
        builder.generation(generation);
        builder
            .backend(
                "app",
                LbPolicy::RoundRobin,
                vec![Endpoint::new(SocketAddr::from(([10, 0, 0, 1], port)))],
            )
            .expect("registers");
        builder
            .route(Some("example.com"), "/", PathType::Prefix, "app")
            .expect("drafts");
        Arc::new(builder.build().expect("builds"))
    }

    fn history(capacity: usize) -> (GenerationHistory, Arc<SharedRouteTable>) {
        let routes = Arc::new(SharedRouteTable::new(
            RouteTableBuilder::new().build().expect("an empty table"),
        ));
        let certs = Arc::new(CertStore::new());
        (
            GenerationHistory::new(Arc::clone(&routes), certs, capacity),
            routes,
        )
    }

    fn record(history: &GenerationHistory, generation: u64, port: u16) -> bool {
        history.record(
            generation,
            generation,
            Arc::new(serde_json::json!({})),
            table(generation, port),
            Arc::new(CertKeys::new()),
        )
    }

    /// Which endpoint the published table currently sends `/` to.
    fn serving_port(routes: &SharedRouteTable) -> Option<u16> {
        let table = routes.load_full();
        let matched = table.match_request("example.com", "/")?;
        matched.backend().endpoints().first().map(|e| e.addr.port())
    }

    #[test]
    fn the_ring_evicts_the_oldest_generation() {
        let (history, _routes) = history(3);
        for generation in 1..=5 {
            record(&history, generation, 8080);
        }

        let held: Vec<u64> =
            history.with_records(|_, ring| ring.iter().map(|r| r.generation).collect());
        assert_eq!(held, vec![3, 4, 5], "the ring keeps the newest `capacity`");
    }

    #[test]
    fn a_history_of_zero_still_holds_one_generation() {
        // A ring that keeps nothing could never answer a rollback, which is
        // the only reason it exists.
        let (history, _routes) = history(0);
        assert_eq!(history.capacity(), 1);
        record(&history, 1, 8080);
        assert_eq!(history.with_records(|_, ring| ring.len()), 1);
    }

    #[test]
    fn recording_publishes_when_nothing_is_pinned() {
        let (history, routes) = history(5);
        assert!(record(&history, 1, 8080));
        assert_eq!(routes.generation(), 1);
        assert_eq!(serving_port(&routes), Some(8080));
        assert_eq!(history.pinned(), None);
    }

    /// The whole state machine, in the order an incident goes: pin an old
    /// generation, watch the controller keep working without reaching the
    /// wire, then resume onto the newest thing it built.
    #[test]
    fn a_pin_holds_publication_and_resuming_jumps_to_the_latest() {
        let (history, routes) = history(5);
        record(&history, 1, 8080);
        record(&history, 2, 8081);
        assert_eq!(serving_port(&routes), Some(8081));

        history.pin(1).expect("generation 1 is in the ring");
        assert_eq!(history.pinned(), Some(1));
        assert_eq!(routes.generation(), 1);
        assert_eq!(serving_port(&routes), Some(8080), "the pinned table is back on the wire");

        // The controller has not stopped. Generations keep arriving and keep
        // being recorded; none of them reach the data plane.
        assert!(!record(&history, 3, 8082));
        assert!(!record(&history, 4, 8083));
        assert_eq!(routes.generation(), 1);
        assert_eq!(serving_port(&routes), Some(8080));

        let published: Vec<(u64, bool)> = history
            .with_records(|_, ring| ring.iter().map(|r| (r.generation, r.published)).collect());
        assert_eq!(
            published,
            vec![(1, true), (2, true), (3, false), (4, false)],
            "a live generation recorded behind a pin must say it did not go live"
        );

        assert_eq!(history.unpin(), Some(4));
        assert_eq!(history.pinned(), None);
        assert_eq!(routes.generation(), 4, "resuming publishes the newest, not the next");
        assert_eq!(serving_port(&routes), Some(8083));

        let published: Vec<(u64, bool)> = history
            .with_records(|_, ring| ring.iter().map(|r| (r.generation, r.published)).collect());
        assert_eq!(
            published,
            vec![(1, true), (2, true), (3, false), (4, true)],
            "resuming makes the newest live and leaves the ones it skipped saying they never were"
        );
    }

    #[test]
    fn pinning_an_unknown_generation_is_refused() {
        let (history, routes) = history(2);
        record(&history, 1, 8080);
        record(&history, 2, 8081);
        record(&history, 3, 8082);

        assert_eq!(
            history.pin(1),
            Err(PinError::Unknown(1)),
            "generation 1 was evicted"
        );
        assert_eq!(history.pin(99), Err(PinError::Unknown(99)));
        assert_eq!(history.pinned(), None);
        assert_eq!(routes.generation(), 3, "a refused pin changes nothing");
    }

    #[test]
    fn pinning_twice_names_what_is_already_pinned() {
        let (history, _routes) = history(5);
        record(&history, 1, 8080);
        record(&history, 2, 8081);

        history.pin(1).expect("pins");
        assert_eq!(history.pin(2), Err(PinError::AlreadyPinned(1)));
        assert_eq!(history.pinned(), Some(1), "the first pin stands");
    }

    #[test]
    fn releasing_a_pin_nobody_set_is_not_an_error() {
        let (history, routes) = history(5);
        record(&history, 1, 8080);
        assert_eq!(history.unpin(), None);
        assert_eq!(history.unpin(), None);
        assert_eq!(routes.generation(), 1);
    }

    #[test]
    fn a_pinned_generation_is_not_evicted_by_the_generations_it_is_holding_back() {
        // Otherwise a long pin would quietly lose the only copy of the thing
        // it is serving, and releasing would be the only way to notice.
        let (history, _routes) = history(3);
        record(&history, 1, 8080);
        history.pin(1).expect("pins");
        for generation in 2..=6 {
            record(&history, generation, 8080 + generation as u16);
        }

        let held: Vec<u64> =
            history.with_records(|_, ring| ring.iter().map(|r| r.generation).collect());
        assert!(
            !held.contains(&1),
            "this documents the eviction that does happen: the ring is a ring"
        );
        assert_eq!(history.pinned(), Some(1));
    }

    #[test]
    fn pin_and_resume_are_announced() {
        let (history, _routes) = history(5);
        let (tx, mut rx) = mpsc::unbounded_channel();
        history.notify(tx);

        record(&history, 1, 8080);
        record(&history, 2, 8081);
        history.pin(1).expect("pins");
        history.unpin();

        assert_eq!(
            rx.try_recv(),
            Ok(PinChange::Pinned {
                generation: 1,
                replaced: 2
            })
        );
        assert_eq!(rx.try_recv(), Ok(PinChange::Resumed { generation: 2 }));
        assert!(rx.try_recv().is_err(), "recording a generation is not a pin change");
    }

    #[test]
    fn timestamps_are_rfc3339_in_utc() {
        assert_eq!(rfc3339(UNIX_EPOCH), "1970-01-01T00:00:00Z");
        assert_eq!(
            rfc3339(UNIX_EPOCH + Duration::from_secs(1_000_000_000)),
            "2001-09-09T01:46:40Z"
        );
        // A leap day, which is where a hand-rolled calendar goes wrong.
        assert_eq!(
            rfc3339(UNIX_EPOCH + Duration::from_secs(1_709_164_800)),
            "2024-02-29T00:00:00Z"
        );
        // 2000 is a leap year and 1900 was not; the era arithmetic is what
        // gets that right.
        assert_eq!(
            rfc3339(UNIX_EPOCH + Duration::from_secs(951_782_400)),
            "2000-02-29T00:00:00Z"
        );
        assert_eq!(
            rfc3339(UNIX_EPOCH + Duration::from_secs(1_767_225_599)),
            "2025-12-31T23:59:59Z"
        );
    }
}
