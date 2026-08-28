//! `ramjet-top` — a live view of a running ramjet-ingress.
//!
//! See the [`ramjet_top`] library for the pieces; this file is the entry point,
//! the event loop, and the terminal's lifetime.
//!
//! # The terminal is restored on every exit path
//!
//! A TUI that leaves a terminal in raw mode with the alternate screen active
//! has broken the shell it was launched from — no echo, no line editing, and a
//! prompt that is not where it appears to be. There are three ways out of this
//! program and all three restore:
//!
//! - a normal quit, which calls [`ratatui::restore`] on the way past;
//! - an error out of the loop, which does the same before reporting it;
//! - a panic, which is caught by the hook [`ratatui::init`] installs.
//!
//! The third is worth being explicit about, because the workspace builds
//! release binaries with `panic = "abort"`. A panic hook still runs before the
//! abort — the hook is what `panic!` calls, and only after it returns does the
//! runtime decide whether to unwind or die — so the terminal is restored even
//! though nothing unwinds.

use std::io::Write;
use std::process::ExitCode;
use std::time::Instant;

use ramjet_top::app::{App, Command};
use ramjet_top::args::{self, Options, Parsed};
use ramjet_top::client::AdminClient;
use ramjet_top::{plain, rfc3339, ui, ClientError, Snapshot};

use ratatui::crossterm::event::{self, Event};
use tokio::sync::mpsc;

/// Usage was wrong. Distinct from a runtime failure so a script can tell
/// "I typed the command wrong" from "the daemon is down".
const EXIT_USAGE: u8 = 2;
/// The daemon could not be reached, or the terminal could not be set up.
const EXIT_FAILURE: u8 = 1;

fn main() -> ExitCode {
    let parsed = match args::parse(std::env::args_os().skip(1)) {
        Ok(parsed) => parsed,
        Err(error) => {
            eprintln!("ramjet-top: {error}");
            return ExitCode::from(EXIT_USAGE);
        }
    };

    let options = match parsed {
        Parsed::Help => {
            print!("{}", args::help());
            return ExitCode::SUCCESS;
        }
        Parsed::Version => {
            println!("ramjet-top {}", env!("CARGO_PKG_VERSION"));
            return ExitCode::SUCCESS;
        }
        Parsed::Run(options) => *options,
    };

    // The runtime is built by hand rather than with `#[tokio::main]` so that a
    // failure to build it is an error message instead of a panic, and so the
    // worker count can be pinned. Two threads: this is a client that issues
    // three small requests a second, and a thread per core on a 128-core host
    // would be 128 stacks to poll one socket.
    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(error) => {
            eprintln!("ramjet-top: cannot start the async runtime: {error}");
            return ExitCode::from(EXIT_FAILURE);
        }
    };

    let client = match AdminClient::new(&options.url, options.timeout) {
        Ok(client) => client,
        Err(error) => {
            eprintln!("ramjet-top: {error}");
            return ExitCode::from(EXIT_USAGE);
        }
    };

    if options.once {
        return runtime.block_on(run_once(&client, &options));
    }
    runtime.block_on(run_tui(client, options))
}

/// `--once`: one poll, printed, no terminal.
async fn run_once(client: &AdminClient, options: &Options) -> ExitCode {
    let snapshot = match client.snapshot().await {
        Ok(snapshot) => snapshot,
        Err(error) => {
            eprintln!("ramjet-top: {error}");
            return ExitCode::from(EXIT_FAILURE);
        }
    };

    let rendered = if options.json {
        match plain::render_json(&snapshot) {
            Ok(json) => json,
            Err(error) => {
                eprintln!("ramjet-top: cannot render the snapshot as JSON: {error}");
                return ExitCode::from(EXIT_FAILURE);
            }
        }
    } else {
        plain::render(&snapshot, rfc3339::now_unix_seconds())
    };

    // Written through a locked handle and flushed explicitly: this output is
    // usually being piped, and a broken pipe should be a quiet exit rather than
    // the panic that `println!` produces when the reader has gone away.
    let stdout = std::io::stdout();
    let mut handle = stdout.lock();
    if writeln!(handle, "{rendered}").is_err() || handle.flush().is_err() {
        return ExitCode::SUCCESS;
    }
    ExitCode::SUCCESS
}

/// What a background task reports back to the loop.
enum Outcome {
    /// A poll finished.
    Polled(Box<Result<Snapshot, ClientError>>),
    /// A pin or unpin finished.
    Acted(Result<String, String>),
}

/// The interactive view.
async fn run_tui(client: AdminClient, options: Options) -> ExitCode {
    let mut terminal = match ratatui::try_init() {
        Ok(terminal) => terminal,
        Err(error) => {
            // The common cause is stdout not being a terminal — somebody piped
            // this into `less`. Naming the alternative is more useful than the
            // errno.
            eprintln!("ramjet-top: cannot set up the terminal: {error}");
            eprintln!("ramjet-top: if this is not an interactive terminal, use --once");
            return ExitCode::from(EXIT_FAILURE);
        }
    };

    let result = event_loop(&mut terminal, client, options).await;

    // Before anything is printed, always.
    ratatui::restore();

    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("ramjet-top: {error}");
            ExitCode::from(EXIT_FAILURE)
        }
    }
}

/// Reads terminal events on a dedicated thread.
///
/// `crossterm::event::read` blocks, and a blocking read inside the runtime
/// would hold a worker hostage between keystrokes. A plain thread is the right
/// tool: it has one job, it never returns, and it dies with the process.
fn spawn_event_reader() -> mpsc::Receiver<Event> {
    let (tx, rx) = mpsc::channel(64);
    std::thread::Builder::new()
        .name("ramjet-top-input".to_string())
        .spawn(move || {
            while let Ok(event) = event::read() {
                // A closed channel means the loop has quit and the process is
                // on its way out.
                if tx.blocking_send(event).is_err() {
                    break;
                }
            }
        })
        // If the thread cannot be spawned the program would run with no input
        // and no way to quit, which is worse than not starting.
        .expect("the input thread must start");
    rx
}

/// The main loop: draw, wait for something, fold it in, repeat.
async fn event_loop(
    terminal: &mut ratatui::DefaultTerminal,
    client: AdminClient,
    options: Options,
) -> std::io::Result<()> {
    let mut app = App::new(
        client.url().to_string(),
        options.interval,
        options.read_only,
        Instant::now(),
    );

    let mut events = spawn_event_reader();
    let (outcomes_tx, mut outcomes) = mpsc::channel::<Outcome>(8);

    let mut ticker = tokio::time::interval(options.interval);
    // The default, `Burst`, would fire back-to-back ticks to "catch up" after a
    // slow poll, turning a hiccup into a flurry of requests at the moment the
    // server is least able to answer them.
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    // One poll in flight at a time. Without this a server slower than the
    // interval accumulates a queue of polls, each differencing against a
    // baseline that a later one has already moved.
    let mut polling = false;
    let start_poll = |polling: &mut bool| {
        if *polling {
            return;
        }
        *polling = true;
        let client = client.clone();
        let tx = outcomes_tx.clone();
        tokio::spawn(async move {
            let result = client.snapshot().await;
            let _ = tx.send(Outcome::Polled(Box::new(result))).await;
        });
    };

    // Poll immediately rather than waiting a full interval to draw anything.
    start_poll(&mut polling);

    loop {
        let now = Instant::now();
        app.expire_notice(now);
        terminal.draw(|frame| ui::draw(frame, &mut app, now))?;

        tokio::select! {
            // Biased so a keypress is never starved by a fast tick: `q` has to
            // work on a busy screen.
            biased;

            event = events.recv() => {
                let Some(event) = event else {
                    // The reader thread is gone; there is no way to quit.
                    return Ok(());
                };
                if let Event::Key(key) = event {
                    if let Some(key) = ui::translate(key) {
                        match app.on_key(key, Instant::now()) {
                            Some(Command::Quit) => return Ok(()),
                            Some(Command::Refresh) => start_poll(&mut polling),
                            Some(Command::Pin(generation)) => {
                                spawn_action(&client, &outcomes_tx, Some(generation));
                            }
                            Some(Command::Unpin) => {
                                spawn_action(&client, &outcomes_tx, None);
                            }
                            None => {}
                        }
                    }
                }
                // Every other event — a resize, a mouse move, a paste — falls
                // through to the redraw at the top of the loop, which is
                // exactly what a resize needs.
            }

            outcome = outcomes.recv() => {
                match outcome {
                    Some(Outcome::Polled(result)) => {
                        polling = false;
                        match *result {
                            Ok(snapshot) => app.record_poll(snapshot, Instant::now()),
                            Err(error) => app.record_failure(error.brief(), Instant::now()),
                        }
                    }
                    Some(Outcome::Acted(result)) => {
                        let now = Instant::now();
                        match result {
                            Ok(message) => {
                                app.notify(message, false, now);
                                // The pin only becomes visible on the next
                                // poll, and waiting a whole interval to see
                                // whether the emergency brake took makes it
                                // feel like it did not.
                                start_poll(&mut polling);
                            }
                            Err(message) => app.notify(message, true, now),
                        }
                    }
                    None => return Ok(()),
                }
            }

            _ = ticker.tick() => start_poll(&mut polling),
        }

        if app.should_quit {
            return Ok(());
        }
    }
}

/// Runs a pin or an unpin in the background.
///
/// `generation` is `Some` to pin and `None` to release.
fn spawn_action(
    client: &AdminClient,
    outcomes: &mpsc::Sender<Outcome>,
    generation: Option<u64>,
) {
    let client = client.clone();
    let tx = outcomes.clone();
    tokio::spawn(async move {
        let result = match generation {
            Some(generation) => client
                .pin(generation)
                .await
                .map(|()| format!("pinned to generation {generation}"))
                .map_err(|e| format!("pin failed — {}", e.brief())),
            None => client
                .unpin()
                .await
                .map(|()| "pin released".to_string())
                .map_err(|e| format!("unpin failed — {}", e.brief())),
        };
        let _ = tx.send(Outcome::Acted(result)).await;
    });
}
