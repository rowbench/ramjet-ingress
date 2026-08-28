//! The state a session accumulates, and what keys do to it.
//!
//! Everything here is synchronous and terminal-free on purpose. Polling is
//! async and drawing needs a terminal, but *deciding* — what a keypress means,
//! which rows are visible, whether the connection counts as lost — is neither,
//! and keeping it in a plain struct is what lets the interesting behaviour be
//! tested by calling a function instead of by driving a pty.
//!
//! # Losing the connection is a state, not an error
//!
//! When a poll fails, the last good data stays on screen, dimmed, and the
//! header says how long ago it was true. This is the whole reason to prefer
//! this over a `watch curl`: the moment the daemon becomes unreachable is
//! exactly the moment its last known state becomes most interesting, and a
//! client that clears the screen to print a connection error has thrown away
//! the evidence at the worst possible time.

use std::collections::VecDeque;
use std::time::{Duration, Instant};

use ratatui::widgets::{ListState, TableState};

use crate::client::Snapshot;
use crate::model::{
    self, CounterBaseline, GlobalStats, RouteRow, Sort,
};
use crate::prom::MetricsSnapshot;

/// How many polls of global request rate the sparkline remembers.
///
/// At the default one-second interval this is a minute of history, which is
/// the window in which "did the thing I just did change anything?" is asked.
pub const HISTORY_LENGTH: usize = 60;

/// Which panel is in front.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum View {
    /// The routes table.
    #[default]
    Routes,
    /// The generation timeline.
    Generations,
}

impl View {
    /// The other one.
    pub fn toggled(self) -> Self {
        match self {
            Self::Routes => Self::Generations,
            Self::Generations => Self::Routes,
        }
    }

    /// The name in the header.
    pub fn title(self) -> &'static str {
        match self {
            Self::Routes => "routes",
            Self::Generations => "generations",
        }
    }
}

/// Whether the admin port is answering.
#[derive(Debug, Clone)]
pub enum Connection {
    /// No poll has completed yet.
    Connecting,
    /// The last poll succeeded.
    Live,
    /// The last poll failed; the data on screen is from `stale_for` ago.
    Lost {
        /// What went wrong, in one line.
        error: String,
        /// How many consecutive polls have failed.
        failures: u32,
        /// When the last successful poll was, if there was one.
        last_success: Option<Instant>,
    },
}

impl Connection {
    /// Whether data on screen should be drawn as stale.
    pub fn is_lost(&self) -> bool {
        matches!(self, Self::Lost { .. })
    }
}

/// Something the event loop has to go and do.
///
/// Key handling returns these rather than performing them, because performing
/// them needs the network and the point of this module is not needing it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Command {
    /// Leave.
    Quit,
    /// Poll now, without waiting for the tick.
    Refresh,
    /// Pin traffic to a generation.
    Pin(u64),
    /// Release the pin.
    Unpin,
}

/// An action waiting on a `y`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Pending {
    /// Pin to this generation.
    Pin(u64),
    /// Release the pin.
    Unpin,
}

impl Pending {
    /// The line shown while it waits.
    pub fn prompt(&self) -> String {
        match self {
            Self::Pin(generation) => format!(
                "PIN traffic to generation {generation}? \
                 This freezes the data plane until released.  [y/N]"
            ),
            Self::Unpin => {
                "RELEASE the pin and resume the newest published generation?  [y/N]".to_string()
            }
        }
    }

    /// The command it becomes when confirmed.
    pub fn command(&self) -> Command {
        match self {
            Self::Pin(generation) => Command::Pin(*generation),
            Self::Unpin => Command::Unpin,
        }
    }
}

/// A one-line note in the footer, with an expiry.
#[derive(Debug, Clone)]
pub struct Notice {
    /// What to say.
    pub text: String,
    /// Whether it reads as a problem.
    pub is_error: bool,
    /// When it stops being shown.
    expires_at: Instant,
}

/// The whole session.
#[derive(Debug)]
pub struct App {
    /// Where this is pointed.
    pub url: String,
    /// Whether the emergency brake is disabled.
    pub read_only: bool,
    /// The poll interval.
    pub interval: Duration,
    /// When the session started, for the uptime in the header.
    pub started_at: Instant,

    /// Which column orders the table.
    pub sort: Sort,
    /// Which way round.
    pub descending: bool,
    /// The substring filter.
    pub filter: String,
    /// Whether keystrokes are going into the filter.
    pub editing_filter: bool,
    /// Which panel is in front.
    pub view: View,
    /// Whether the selected generation's diff is expanded.
    pub expanded: bool,

    /// The last successful poll.
    pub snapshot: Option<Snapshot>,
    /// Its rows, sorted but not filtered.
    pub rows: Vec<RouteRow>,
    /// Its header numbers.
    pub global: GlobalStats,
    /// Global request rate over the last [`HISTORY_LENGTH`] polls.
    pub rps_history: VecDeque<u64>,

    /// Whether the admin port is answering.
    pub connection: Connection,
    /// Selection in the routes table.
    pub routes_state: TableState,
    /// Selection in the generation timeline.
    pub generations_state: ListState,
    /// An action waiting on a confirmation.
    pub pending: Option<Pending>,
    /// A transient footer message.
    pub notice: Option<Notice>,

    /// Whether the loop should stop.
    pub should_quit: bool,

    baseline: Option<CounterBaseline>,
    previous_metrics: Option<MetricsSnapshot>,
    last_poll_at: Option<Instant>,
    last_success_at: Option<Instant>,
}

impl App {
    /// A session that has not polled yet.
    pub fn new(url: String, interval: Duration, read_only: bool, now: Instant) -> Self {
        Self {
            url,
            read_only,
            interval,
            started_at: now,
            sort: Sort::default(),
            descending: Sort::default().defaults_to_descending(),
            filter: String::new(),
            editing_filter: false,
            view: View::default(),
            expanded: false,
            snapshot: None,
            rows: Vec::new(),
            global: GlobalStats::default(),
            rps_history: VecDeque::with_capacity(HISTORY_LENGTH),
            connection: Connection::Connecting,
            routes_state: TableState::default(),
            generations_state: ListState::default(),
            pending: None,
            notice: None,
            should_quit: false,
            baseline: None,
            previous_metrics: None,
            last_poll_at: None,
            last_success_at: None,
        }
    }

    /// Folds a successful poll into the session.
    ///
    /// The interval used for rates is the measured gap between this poll and
    /// the last *successful* one, not `--interval`. After a reconnect that gap
    /// may be minutes, and dividing a counter delta by it gives the average
    /// rate over the outage — which is the honest number, and much better than
    /// dividing a minute of traffic by one second.
    pub fn record_poll(&mut self, snapshot: Snapshot, now: Instant) {
        let elapsed = self
            .last_poll_at
            .map(|then| now.saturating_duration_since(then))
            .unwrap_or_default();

        self.rows = model::compute_rows(&snapshot.routes, self.baseline.as_ref(), elapsed);
        model::sort_rows(&mut self.rows, self.sort, self.descending);

        self.global = model::compute_global(
            &snapshot.metrics,
            self.previous_metrics.as_ref(),
            elapsed,
        );

        if let Some(rps) = self.global.rps {
            if self.rps_history.len() == HISTORY_LENGTH {
                self.rps_history.pop_front();
            }
            // The sparkline takes integers. Rounding rather than truncating so
            // a steady 0.6 rps is a visible bar and not an empty one.
            self.rps_history.push_back(rps.round().max(0.0) as u64);
        }

        self.baseline = Some(model::baseline_of(&snapshot.routes));
        self.previous_metrics = Some(snapshot.metrics.clone());
        self.last_poll_at = Some(now);
        self.last_success_at = Some(now);
        self.snapshot = Some(snapshot);
        self.connection = Connection::Live;

        self.clamp_selection();
    }

    /// Folds a failed poll into the session.
    ///
    /// The baseline is deliberately *not* cleared. If the daemon comes back
    /// with its counters intact — a network blip rather than a restart — the
    /// next successful poll differences against the last good numbers and
    /// reports the true average across the gap. If it restarted, the counters
    /// went backwards and the saturating subtraction in
    /// [`crate::model`](crate::model) reports zero, which is also right.
    pub fn record_failure(&mut self, error: String, now: Instant) {
        let failures = match &self.connection {
            Connection::Lost { failures, .. } => failures.saturating_add(1),
            _ => 1,
        };
        self.connection = Connection::Lost {
            error,
            failures,
            last_success: self.last_success_at,
        };
        let _ = now;
    }

    /// How long the data on screen has been stale, if it is.
    pub fn stale_for(&self, now: Instant) -> Option<Duration> {
        match &self.connection {
            Connection::Lost { last_success, .. } => {
                Some(last_success.map_or(Duration::ZERO, |t| now.saturating_duration_since(t)))
            }
            _ => None,
        }
    }

    /// How long this session has been running.
    pub fn uptime(&self, now: Instant) -> Duration {
        now.saturating_duration_since(self.started_at)
    }

    /// The rows the filter lets through.
    pub fn visible_rows(&self) -> Vec<&RouteRow> {
        self.rows
            .iter()
            .filter(|row| row.matches(&self.filter))
            .collect()
    }

    /// The generation the timeline has selected, if any.
    pub fn selected_generation(&self) -> Option<&crate::contract::GenerationEntry> {
        let snapshot = self.snapshot.as_ref()?;
        let index = self.generations_state.selected()?;
        snapshot.generations.generations.get(index)
    }

    /// Posts a footer message that fades after a few seconds.
    pub fn notify(&mut self, text: impl Into<String>, is_error: bool, now: Instant) {
        self.notice = Some(Notice {
            text: text.into(),
            is_error,
            // Long enough to read, short enough that it does not become
            // furniture in a display that is otherwise all live numbers.
            expires_at: now + Duration::from_secs(5),
        });
    }

    /// Drops the footer message once it has had its time.
    pub fn expire_notice(&mut self, now: Instant) {
        if let Some(notice) = &self.notice {
            if now >= notice.expires_at {
                self.notice = None;
            }
        }
    }

    /// The footer message, if one is current.
    pub fn current_notice(&self) -> Option<&Notice> {
        self.notice.as_ref()
    }

    /// Keeps a selection pointing at a row that exists.
    ///
    /// The route table is rebuilt on every poll and can shrink, so a selection
    /// held across polls can end up past the end. Ratatui renders that as no
    /// selection at all, which looks like the selection was lost.
    fn clamp_selection(&mut self) {
        let visible = self.visible_rows().len();
        match self.routes_state.selected() {
            Some(_) if visible == 0 => self.routes_state.select(None),
            Some(index) if index >= visible => self.routes_state.select(Some(visible - 1)),
            _ => {}
        }

        let generations = self
            .snapshot
            .as_ref()
            .map_or(0, |s| s.generations.generations.len());
        match self.generations_state.selected() {
            Some(_) if generations == 0 => self.generations_state.select(None),
            Some(index) if index >= generations => {
                self.generations_state.select(Some(generations - 1));
            }
            _ => {}
        }
    }

    /// How many rows the panel in front has.
    fn current_len(&self) -> usize {
        match self.view {
            View::Routes => self.visible_rows().len(),
            View::Generations => self
                .snapshot
                .as_ref()
                .map_or(0, |s| s.generations.generations.len()),
        }
    }

    /// The selection state of the panel in front.
    fn select(&mut self, index: Option<usize>) {
        match self.view {
            View::Routes => self.routes_state.select(index),
            View::Generations => self.generations_state.select(index),
        }
    }

    /// The current selection of the panel in front.
    fn selected(&self) -> Option<usize> {
        match self.view {
            View::Routes => self.routes_state.selected(),
            View::Generations => self.generations_state.selected(),
        }
    }

    /// Moves the selection, saturating at both ends rather than wrapping.
    ///
    /// Wrapping is wrong for a list somebody is scanning: holding `j` at the
    /// bottom of a hundred routes should stay at the bottom, not silently
    /// teleport to the top and start again.
    ///
    /// From no selection at all, a downward move lands on the first row and an
    /// upward move on the last. Treating "nothing selected" as "row zero" would
    /// make the very first press of `j` select the *second* row, which is the
    /// kind of off-by-one nobody reports and everybody notices.
    pub fn move_selection(&mut self, delta: isize) {
        let len = self.current_len();
        if len == 0 {
            self.select(None);
            return;
        }
        let last = len as isize - 1;
        let next = match self.selected() {
            None if delta >= 0 => 0,
            None => last,
            Some(current) => (current as isize + delta).clamp(0, last),
        };
        self.select(Some(next as usize));
    }

    /// Selects the first row of the panel in front.
    pub fn select_first(&mut self) {
        let has_rows = self.current_len() > 0;
        self.select(has_rows.then_some(0));
    }

    /// Selects the last row of the panel in front.
    pub fn select_last(&mut self) {
        let len = self.current_len();
        self.select((len > 0).then(|| len - 1));
    }

    /// Applies a sort key, flipping the direction if that column is already
    /// selected.
    pub fn apply_sort(&mut self, sort: Sort) {
        if self.sort == sort {
            self.descending = !self.descending;
        } else {
            self.sort = sort;
            self.descending = sort.defaults_to_descending();
        }
        model::sort_rows(&mut self.rows, self.sort, self.descending);
        self.clamp_selection();
    }
}

/// A keypress, reduced to what this program distinguishes.
///
/// Not a crossterm type: this is the alphabet the state machine below is
/// written against, and translating a terminal event into it happens at the
/// edge, in [`crate::ui`](crate::ui). That is what lets every branch of
/// [`App::on_key`] be exercised without a terminal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Key {
    /// A printable character.
    Char(char),
    /// Backspace.
    Backspace,
    /// Enter.
    Enter,
    /// Escape.
    Escape,
    /// Tab.
    Tab,
    /// Up, or `k`.
    Up,
    /// Down, or `j`.
    Down,
    /// Page up.
    PageUp,
    /// Page down.
    PageDown,
    /// Home.
    Home,
    /// End.
    End,
    /// Ctrl-C, which quits from anywhere including the filter box.
    CtrlC,
}

impl App {
    /// Handles one keypress.
    ///
    /// Returns what the event loop should go and do, if anything.
    pub fn on_key(&mut self, key: Key, now: Instant) -> Option<Command> {
        // Ctrl-C is checked before anything else so that it works while the
        // filter box has the keyboard and while a confirmation is up. A TUI
        // with a mode that swallows Ctrl-C is a TUI people kill from another
        // terminal.
        if key == Key::CtrlC {
            self.should_quit = true;
            return Some(Command::Quit);
        }

        if let Some(pending) = self.pending.clone() {
            return self.on_confirmation(key, pending, now);
        }

        if self.editing_filter {
            self.on_filter_key(key);
            return None;
        }

        self.on_normal_key(key, now)
    }

    /// Keys while a pin or unpin is waiting for a `y`.
    ///
    /// Anything that is not `y` cancels. Not just `n`: the guard exists so that
    /// a keystroke aimed at the previous screen cannot freeze a production data
    /// plane, and that only works if the accidental keystroke is overwhelmingly
    /// likely to be a cancel.
    fn on_confirmation(&mut self, key: Key, pending: Pending, now: Instant) -> Option<Command> {
        self.pending = None;
        match key {
            Key::Char('y') | Key::Char('Y') => Some(pending.command()),
            _ => {
                self.notify("cancelled", false, now);
                None
            }
        }
    }

    /// Keys while the filter box has the keyboard.
    fn on_filter_key(&mut self, key: Key) {
        match key {
            Key::Char(c) => self.filter.push(c),
            Key::Backspace => {
                self.filter.pop();
            }
            // Enter keeps the filter and gives the keyboard back; Escape
            // abandons it. Both leave the box, which is the part people expect.
            Key::Enter => self.editing_filter = false,
            Key::Escape => {
                self.filter.clear();
                self.editing_filter = false;
            }
            _ => {}
        }
        self.clamp_selection();
    }

    /// Keys in the ordinary state.
    fn on_normal_key(&mut self, key: Key, now: Instant) -> Option<Command> {
        // Sort keys first, and only while the routes table is in front. In the
        // timeline they are meaningless, and `r` in particular should not
        // silently reorder a panel that is not on screen.
        if let Key::Char(c) = key {
            if self.view == View::Routes {
                if let Some(sort) = Sort::from_key(c) {
                    self.apply_sort(sort);
                    return None;
                }
            }
        }

        match key {
            Key::Char('q') => {
                self.should_quit = true;
                return Some(Command::Quit);
            }
            Key::Char('/') => {
                self.editing_filter = true;
            }
            Key::Escape => {
                // One key that means "get me back to the plain view",
                // whichever of the several ways of leaving it is in effect.
                if self.expanded {
                    self.expanded = false;
                } else if !self.filter.is_empty() {
                    self.filter.clear();
                    self.clamp_selection();
                } else {
                    self.select(None);
                }
            }
            Key::Tab => {
                self.view = self.view.toggled();
                self.expanded = false;
            }
            Key::Up | Key::Char('k') => self.move_selection(-1),
            Key::Down | Key::Char('j') => self.move_selection(1),
            Key::PageUp => self.move_selection(-10),
            Key::PageDown => self.move_selection(10),
            Key::Home => self.select_first(),
            Key::End => self.select_last(),
            Key::Enter if self.view == View::Generations => {
                // Expanding with nothing selected means the newest generation,
                // which is what somebody pressing Enter on a fresh timeline is
                // asking about.
                if self.generations_state.selected().is_none() {
                    self.select(Some(0));
                }
                self.expanded = !self.expanded;
            }
            Key::Char('g') => return Some(Command::Refresh),
            Key::Char('p') => return self.request_pin(now),
            Key::Char('u') => return self.request_unpin(now),
            _ => {}
        }
        None
    }

    /// Asks for confirmation to pin, if pinning is allowed here.
    fn request_pin(&mut self, now: Instant) -> Option<Command> {
        if self.read_only {
            self.notify("read-only: pin refused", true, now);
            return None;
        }
        if self.view != View::Generations {
            self.notify("pin: switch to the generations panel (Tab)", true, now);
            return None;
        }
        match self.selected_generation() {
            Some(entry) => {
                self.pending = Some(Pending::Pin(entry.generation));
                None
            }
            None => {
                self.notify("pin: select a generation first", true, now);
                None
            }
        }
    }

    /// Asks for confirmation to release the pin, if there is one.
    fn request_unpin(&mut self, now: Instant) -> Option<Command> {
        if self.read_only {
            self.notify("read-only: unpin refused", true, now);
            return None;
        }
        let pinned = self.snapshot.as_ref().and_then(Snapshot::pinned);
        if pinned.is_none() {
            self.notify("nothing is pinned", true, now);
            return None;
        }
        self.pending = Some(Pending::Unpin);
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::contract::{
        GenerationEntry, GenerationsResponse, PathType, RouteEntry, RoutesResponse,
    };
    use crate::prom::MetricsSnapshot;

    fn app() -> App {
        App::new(
            "http://127.0.0.1:10254".to_string(),
            Duration::from_secs(1),
            false,
            Instant::now(),
        )
    }

    fn route(host: &str, requests: u64) -> RouteEntry {
        RouteEntry {
            host: host.to_string(),
            path: "/".to_string(),
            path_type: PathType::Prefix,
            backend: "svc".to_string(),
            endpoints: 1,
            requests_total: requests,
            errors_5xx_total: 0,
            upstream_latency_ms_sum: 0.0,
            upstream_latency_count: 0,
            canary: None,
        }
    }

    fn snapshot(routes: Vec<RouteEntry>, requests_total: u64, pinned: Option<u64>) -> Snapshot {
        Snapshot {
            url: "http://127.0.0.1:10254".to_string(),
            generations: GenerationsResponse {
                pinned,
                serving: 7,
                generations: vec![
                    GenerationEntry {
                        generation: 8,
                        applied_at: "2026-08-28T10:00:00Z".to_string(),
                        published: false,
                        ..Default::default()
                    },
                    GenerationEntry {
                        generation: 7,
                        applied_at: "2026-08-28T09:00:00Z".to_string(),
                        published: true,
                        ..Default::default()
                    },
                ],
            },
            routes: RoutesResponse {
                generation: 7,
                routes,
            },
            metrics: MetricsSnapshot {
                requests_total,
                errors_5xx_total: 0,
                active_connections: Some(3),
                generation: Some(7),
                latency_sum_seconds: 0.0,
                latency_count: 0,
                pinned: None,
            },
        }
    }

    #[test]
    fn a_fresh_session_is_connecting_with_nothing_to_show() {
        let app = app();
        assert!(matches!(app.connection, Connection::Connecting));
        assert!(app.rows.is_empty());
        assert!(app.snapshot.is_none());
        assert!(app.rps_history.is_empty());
        assert_eq!(app.sort, Sort::Rps);
        assert!(app.descending);
    }

    #[test]
    fn the_first_poll_goes_live_but_contributes_no_history() {
        let mut app = app();
        let start = Instant::now();
        app.record_poll(snapshot(vec![route("a", 100)], 100, None), start);

        assert!(matches!(app.connection, Connection::Live));
        assert_eq!(app.rows.len(), 1);
        assert_eq!(app.rows[0].rps, None);
        assert!(
            app.rps_history.is_empty(),
            "no rate means no bar in the sparkline"
        );
    }

    #[test]
    fn the_second_poll_produces_rates_and_a_history_point() {
        let mut app = app();
        let start = Instant::now();
        app.record_poll(snapshot(vec![route("a", 100)], 100, None), start);
        app.record_poll(
            snapshot(vec![route("a", 150)], 160, None),
            start + Duration::from_secs(1),
        );

        assert_eq!(app.rows[0].rps, Some(50.0));
        assert_eq!(app.global.rps, Some(60.0));
        assert_eq!(app.rps_history.len(), 1);
        assert_eq!(app.rps_history[0], 60);
    }

    #[test]
    fn the_history_is_capped_and_drops_the_oldest_point() {
        let mut app = app();
        let start = Instant::now();
        let mut total = 0;
        for poll in 0..(HISTORY_LENGTH + 10) {
            total += poll as u64;
            app.record_poll(
                snapshot(vec![route("a", total)], total, None),
                start + Duration::from_secs(poll as u64),
            );
        }
        assert_eq!(app.rps_history.len(), HISTORY_LENGTH);
        // The last point is the most recent delta, not the first.
        let expected = (HISTORY_LENGTH + 9) as u64;
        assert_eq!(*app.rps_history.back().expect("history"), expected);
    }

    #[test]
    fn a_failed_poll_keeps_the_last_good_data_and_says_it_is_stale() {
        let mut app = app();
        let start = Instant::now();
        app.record_poll(snapshot(vec![route("a", 100)], 100, None), start);
        app.record_failure("connection refused".to_string(), start);

        assert!(app.connection.is_lost());
        assert_eq!(app.rows.len(), 1, "the rows are still there");
        assert!(app.snapshot.is_some());

        let stale = app
            .stale_for(start + Duration::from_secs(5))
            .expect("stale while lost");
        assert_eq!(stale, Duration::from_secs(5));
    }

    #[test]
    fn consecutive_failures_are_counted() {
        let mut app = app();
        let now = Instant::now();
        app.record_failure("refused".to_string(), now);
        app.record_failure("refused".to_string(), now);
        app.record_failure("refused".to_string(), now);

        match &app.connection {
            Connection::Lost { failures, .. } => assert_eq!(*failures, 3),
            other => panic!("expected Lost, got {other:?}"),
        }
    }

    #[test]
    fn reconnecting_after_an_outage_averages_the_rate_over_the_real_gap() {
        let mut app = app();
        let start = Instant::now();
        app.record_poll(snapshot(vec![route("a", 100)], 100, None), start);
        app.record_failure("refused".to_string(), start + Duration::from_secs(1));

        // Sixty seconds later the daemon is back, with 600 more requests. That
        // is 10/s averaged across the outage, not 600/s.
        app.record_poll(
            snapshot(vec![route("a", 700)], 700, None),
            start + Duration::from_secs(60),
        );
        assert_eq!(app.rows[0].rps, Some(10.0));
        assert!(matches!(app.connection, Connection::Live));
        assert!(app.stale_for(start + Duration::from_secs(60)).is_none());
    }

    #[test]
    fn a_restart_during_an_outage_reads_as_zero_not_as_a_wrapped_counter() {
        let mut app = app();
        let start = Instant::now();
        app.record_poll(snapshot(vec![route("a", 1_000_000)], 1_000_000, None), start);
        app.record_failure("refused".to_string(), start + Duration::from_secs(1));
        app.record_poll(
            snapshot(vec![route("a", 5)], 5, None),
            start + Duration::from_secs(30),
        );
        assert_eq!(app.rows[0].rps, Some(0.0));
    }

    // --- keys ------------------------------------------------------------

    #[test]
    fn q_and_ctrl_c_quit() {
        let now = Instant::now();
        for key in [Key::Char('q'), Key::CtrlC] {
            let mut app = app();
            assert_eq!(app.on_key(key, now), Some(Command::Quit), "{key:?}");
            assert!(app.should_quit);
        }
    }

    #[test]
    fn ctrl_c_quits_even_from_inside_the_filter_box() {
        let now = Instant::now();
        let mut app = app();
        app.on_key(Key::Char('/'), now);
        assert!(app.editing_filter);
        assert_eq!(app.on_key(Key::CtrlC, now), Some(Command::Quit));
    }

    #[test]
    fn the_filter_box_takes_characters_including_the_sort_keys() {
        let now = Instant::now();
        let mut app = app();
        app.on_key(Key::Char('/'), now);
        for c in "rel".chars() {
            app.on_key(Key::Char(c), now);
        }
        assert_eq!(app.filter, "rel");
        assert_eq!(app.sort, Sort::Rps, "typing did not reorder the table");

        app.on_key(Key::Backspace, now);
        assert_eq!(app.filter, "re");

        app.on_key(Key::Enter, now);
        assert!(!app.editing_filter);
        assert_eq!(app.filter, "re", "enter keeps what was typed");
    }

    #[test]
    fn escape_in_the_filter_box_abandons_the_filter() {
        let now = Instant::now();
        let mut app = app();
        app.on_key(Key::Char('/'), now);
        app.on_key(Key::Char('x'), now);
        app.on_key(Key::Escape, now);
        assert!(!app.editing_filter);
        assert!(app.filter.is_empty());
    }

    #[test]
    fn sort_keys_select_a_column_and_then_flip_it() {
        let now = Instant::now();
        let mut app = app();
        app.on_key(Key::Char('l'), now);
        assert_eq!(app.sort, Sort::Latency);
        assert!(app.descending, "latency starts with the slowest first");

        app.on_key(Key::Char('l'), now);
        assert_eq!(app.sort, Sort::Latency);
        assert!(!app.descending, "the same key flips the direction");

        app.on_key(Key::Char('h'), now);
        assert_eq!(app.sort, Sort::Host);
        assert!(!app.descending, "host starts alphabetically");
    }

    #[test]
    fn sort_keys_do_nothing_while_the_timeline_is_in_front() {
        let now = Instant::now();
        let mut app = app();
        app.on_key(Key::Tab, now);
        assert_eq!(app.view, View::Generations);
        app.on_key(Key::Char('e'), now);
        assert_eq!(app.sort, Sort::Rps, "unchanged");
    }

    #[test]
    fn tab_switches_panels_and_collapses_an_expanded_diff() {
        let now = Instant::now();
        let mut app = app();
        app.record_poll(snapshot(vec![route("a", 1)], 1, None), Instant::now());
        app.on_key(Key::Tab, now);
        app.on_key(Key::Enter, now);
        assert!(app.expanded);

        app.on_key(Key::Tab, now);
        assert_eq!(app.view, View::Routes);
        assert!(!app.expanded);
    }

    #[test]
    fn selection_moves_and_stops_at_both_ends_rather_than_wrapping() {
        let now = Instant::now();
        let mut app = app();
        app.record_poll(
            snapshot(vec![route("a", 1), route("b", 2), route("c", 3)], 6, None),
            Instant::now(),
        );

        app.on_key(Key::Down, now);
        assert_eq!(app.routes_state.selected(), Some(0));
        app.on_key(Key::Down, now);
        assert_eq!(app.routes_state.selected(), Some(1));

        for _ in 0..10 {
            app.on_key(Key::Char('j'), now);
        }
        assert_eq!(app.routes_state.selected(), Some(2), "stops at the bottom");

        for _ in 0..10 {
            app.on_key(Key::Char('k'), now);
        }
        assert_eq!(app.routes_state.selected(), Some(0), "stops at the top");
    }

    #[test]
    fn home_and_end_reach_the_ends_of_a_long_list() {
        let now = Instant::now();
        let mut app = app();
        let routes: Vec<RouteEntry> = (0..50).map(|i| route(&format!("h{i:02}"), i)).collect();
        app.record_poll(snapshot(routes, 0, None), Instant::now());

        app.on_key(Key::End, now);
        assert_eq!(app.routes_state.selected(), Some(49));
        app.on_key(Key::Home, now);
        assert_eq!(app.routes_state.selected(), Some(0));
        app.on_key(Key::PageDown, now);
        assert_eq!(app.routes_state.selected(), Some(10));
    }

    #[test]
    fn a_selection_past_the_end_of_a_shrunken_table_is_pulled_back() {
        let now = Instant::now();
        let mut app = app();
        app.record_poll(
            snapshot(vec![route("a", 1), route("b", 2), route("c", 3)], 6, None),
            Instant::now(),
        );
        app.on_key(Key::End, now);
        assert_eq!(app.routes_state.selected(), Some(2));

        // A new generation removed two routes.
        app.record_poll(
            snapshot(vec![route("a", 4)], 8, None),
            Instant::now() + Duration::from_secs(1),
        );
        assert_eq!(
            app.routes_state.selected(),
            Some(0),
            "otherwise the selection silently disappears"
        );
    }

    #[test]
    fn filtering_to_nothing_clears_the_selection() {
        let now = Instant::now();
        let mut app = app();
        app.record_poll(
            snapshot(vec![route("alpha", 1), route("beta", 2)], 3, None),
            Instant::now(),
        );
        app.on_key(Key::Down, now);
        assert_eq!(app.routes_state.selected(), Some(0));

        app.on_key(Key::Char('/'), now);
        for c in "zzz".chars() {
            app.on_key(Key::Char(c), now);
        }
        assert!(app.visible_rows().is_empty());
        assert_eq!(app.routes_state.selected(), None);
    }

    #[test]
    fn the_filter_narrows_the_visible_rows() {
        let now = Instant::now();
        let mut app = app();
        app.record_poll(
            snapshot(vec![route("alpha", 1), route("beta", 2)], 3, None),
            Instant::now(),
        );
        app.on_key(Key::Char('/'), now);
        app.on_key(Key::Char('b'), now);
        let visible = app.visible_rows();
        assert_eq!(visible.len(), 1);
        assert_eq!(visible[0].host, "beta");
    }

    #[test]
    fn g_asks_for_an_immediate_poll() {
        let now = Instant::now();
        let mut app = app();
        assert_eq!(app.on_key(Key::Char('g'), now), Some(Command::Refresh));
    }

    // --- the emergency brake ---------------------------------------------

    #[test]
    fn pinning_needs_the_timeline_a_selection_and_a_confirmation() {
        let now = Instant::now();
        let mut app = app();
        app.record_poll(snapshot(vec![route("a", 1)], 1, None), Instant::now());

        // Wrong panel.
        assert_eq!(app.on_key(Key::Char('p'), now), None);
        assert!(app.pending.is_none());
        assert!(app.current_notice().expect("a complaint").is_error);

        app.on_key(Key::Tab, now);
        // No selection yet.
        assert_eq!(app.on_key(Key::Char('p'), now), None);
        assert!(app.pending.is_none());

        app.on_key(Key::Down, now);
        assert_eq!(app.on_key(Key::Char('p'), now), None);
        assert_eq!(
            app.pending,
            Some(Pending::Pin(8)),
            "armed, but not fired"
        );

        assert_eq!(
            app.on_key(Key::Char('y'), now),
            Some(Command::Pin(8)),
            "y fires it"
        );
        assert!(app.pending.is_none());
    }

    #[test]
    fn any_key_that_is_not_y_cancels_a_pin() {
        let now = Instant::now();
        for key in [Key::Char('n'), Key::Escape, Key::Enter, Key::Char('x')] {
            let mut app = app();
            app.record_poll(snapshot(vec![route("a", 1)], 1, None), Instant::now());
            app.on_key(Key::Tab, now);
            app.on_key(Key::Down, now);
            app.on_key(Key::Char('p'), now);
            assert!(app.pending.is_some());

            assert_eq!(app.on_key(key, now), None, "{key:?} must not fire a pin");
            assert!(app.pending.is_none());
        }
    }

    #[test]
    fn read_only_refuses_both_halves_of_the_brake() {
        let now = Instant::now();
        let mut app = App::new(
            "http://127.0.0.1:10254".to_string(),
            Duration::from_secs(1),
            true,
            Instant::now(),
        );
        app.record_poll(snapshot(vec![route("a", 1)], 1, Some(7)), Instant::now());
        app.on_key(Key::Tab, now);
        app.on_key(Key::Down, now);

        assert_eq!(app.on_key(Key::Char('p'), now), None);
        assert!(app.pending.is_none(), "not even armed");
        assert!(app.current_notice().expect("a refusal").is_error);

        assert_eq!(app.on_key(Key::Char('u'), now), None);
        assert!(app.pending.is_none());
    }

    #[test]
    fn unpinning_is_refused_when_nothing_is_pinned() {
        let now = Instant::now();
        let mut app = app();
        app.record_poll(snapshot(vec![route("a", 1)], 1, None), Instant::now());

        assert_eq!(app.on_key(Key::Char('u'), now), None);
        assert!(app.pending.is_none());
        assert_eq!(
            app.current_notice().expect("a complaint").text,
            "nothing is pinned"
        );
    }

    #[test]
    fn unpinning_a_pinned_daemon_arms_and_confirms() {
        let now = Instant::now();
        let mut app = app();
        app.record_poll(snapshot(vec![route("a", 1)], 1, Some(7)), Instant::now());

        assert_eq!(app.on_key(Key::Char('u'), now), None);
        assert_eq!(app.pending, Some(Pending::Unpin));
        assert_eq!(app.on_key(Key::Char('y'), now), Some(Command::Unpin));
    }

    #[test]
    fn a_confirmation_prompt_names_what_it_will_do() {
        assert!(Pending::Pin(42).prompt().contains("42"));
        assert!(Pending::Pin(42).prompt().contains("[y/N]"));
        assert!(Pending::Unpin.prompt().contains("RELEASE"));
    }

    #[test]
    fn notices_expire() {
        let now = Instant::now();
        let mut app = app();
        app.notify("something", false, now);
        assert!(app.current_notice().is_some());

        app.expire_notice(now + Duration::from_secs(1));
        assert!(app.current_notice().is_some(), "not yet");

        app.expire_notice(now + Duration::from_secs(6));
        assert!(app.current_notice().is_none());
    }

    #[test]
    fn enter_expands_and_collapses_a_generation_diff() {
        let now = Instant::now();
        let mut app = app();
        app.record_poll(snapshot(vec![route("a", 1)], 1, None), Instant::now());
        app.on_key(Key::Tab, now);

        app.on_key(Key::Enter, now);
        assert!(app.expanded);
        assert_eq!(
            app.generations_state.selected(),
            Some(0),
            "enter with nothing selected picks the newest"
        );
        assert_eq!(app.selected_generation().map(|g| g.generation), Some(8));

        app.on_key(Key::Enter, now);
        assert!(!app.expanded);
    }

    #[test]
    fn escape_unwinds_the_view_one_layer_at_a_time() {
        let now = Instant::now();
        let mut app = app();
        app.record_poll(
            snapshot(vec![route("alpha", 1), route("beta", 2)], 3, None),
            Instant::now(),
        );

        app.on_key(Key::Char('/'), now);
        app.on_key(Key::Char('a'), now);
        app.on_key(Key::Enter, now);
        app.on_key(Key::Down, now);
        assert_eq!(app.filter, "a");
        assert_eq!(app.routes_state.selected(), Some(0));

        app.on_key(Key::Escape, now);
        assert!(app.filter.is_empty(), "the filter goes first");
        assert_eq!(app.routes_state.selected(), Some(0));

        app.on_key(Key::Escape, now);
        assert_eq!(app.routes_state.selected(), None, "then the selection");
    }

    #[test]
    fn views_have_names_and_toggle() {
        assert_eq!(View::Routes.toggled(), View::Generations);
        assert_eq!(View::Generations.toggled(), View::Routes);
        assert_eq!(View::Routes.title(), "routes");
        assert_eq!(View::Generations.title(), "generations");
    }

    #[test]
    fn uptime_counts_from_the_start_of_the_session() {
        let start = Instant::now();
        let app = App::new("u".to_string(), Duration::from_secs(1), false, start);
        assert_eq!(
            app.uptime(start + Duration::from_secs(90)),
            Duration::from_secs(90)
        );
    }
}
