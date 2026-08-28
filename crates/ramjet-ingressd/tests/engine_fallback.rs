//! Choosing an engine, and what happens when the host will not run it.
//!
//! Driven against the real binary rather than the selection function, because
//! the property that matters is the one an operator sees: the process starts,
//! or it does not, and the log says which engine is serving and why.
//!
//! `RAMJET_URING_UNAVAILABLE=1` stands in for the failure this is really about
//! — `io_uring_setup` blocked by seccomp, which is what Docker's default
//! profile does. Reproducing that needs a container with a specific policy,
//! which a `cargo test` cannot arrange; a fallback path that is only ever
//! exercised in production is one nobody has watched work.

use std::io::Read;
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// Distinguishes one run's scratch directory from another's.
///
/// The engine name is not enough: two tests here ask for `uring` and differ
/// only in whether the reactor is available, so naming the directory after the
/// engine gave them the same one — and each deletes it on the way out, so under
/// enough load one test removed the other's routes file mid-run. That failed as
/// "the daemon should be serving", which points at the daemon rather than at
/// the test that broke it.
static RUN: AtomicUsize = AtomicUsize::new(0);

/// A routes file with one host and one endpoint nothing is listening on.
///
/// Nothing here sends a request, so the endpoint never has to answer: what is
/// under test is which engine started.
fn routes_file(dir: &std::path::Path) -> std::path::PathBuf {
    let path = dir.join("routes.yaml");
    std::fs::write(
        &path,
        "backends:\n  - name: app\n    endpoints:\n      - 127.0.0.1:9\n\
         routes:\n  - host: app.example.com\n    path: /\n    backend: app\n",
    )
    .expect("the routes file was written");
    path
}

/// The port the daemon actually bound, read out of its startup banner.
///
/// The banner is printed after the listeners are up, so a port here means there
/// is something to connect to. `https` and `http3` are separate labels on
/// adjacent lines and must not be mistaken for this one, which is why the label
/// is compared whole rather than by prefix.
fn banner_http_port(output: &str) -> Option<u16> {
    for line in output.lines() {
        let mut fields = line.split_whitespace();
        if fields.next() != Some("http") {
            continue;
        }
        let (_, port) = fields.next()?.rsplit_once(':')?;
        if let Ok(port) = port.parse() {
            return Some(port);
        }
    }
    None
}

/// Drain one of the child's pipes into `into` until it closes.
///
/// A thread per pipe rather than reading both after the child exits: this test
/// has to read the banner *while* the daemon runs, and a single blocking read
/// would hang past the deadline on a daemon that neither printed nor exited —
/// which is a failure worth reporting rather than waiting out.
///
/// `from_utf8_lossy` per chunk can only split a multi-byte character, and
/// everything the daemon writes here is ASCII.
fn drain(
    mut pipe: impl Read + Send + 'static,
    into: Arc<Mutex<String>>,
) -> std::thread::JoinHandle<()> {
    std::thread::spawn(move || {
        let mut buf = [0u8; 1024];
        loop {
            match pipe.read(&mut buf) {
                Ok(0) | Err(_) => return,
                Ok(n) => into
                    .lock()
                    .expect("the collected output is not poisoned")
                    .push_str(&String::from_utf8_lossy(&buf[..n])),
            }
        }
    })
}

/// Run the daemon until it is serving, then stop it.
///
/// Returns whether it got that far, and everything it wrote. The startup banner
/// goes to stdout and the engine decision goes to the log on stderr, so both are
/// collected into one string — every assertion here is a `contains`, and which
/// stream a line arrived on has never been the question.
fn run_until_serving(engine: &str, unavailable: bool) -> (bool, String) {
    let dir = std::env::temp_dir().join(format!(
        "ramjet-engine-fallback-{}-{}-{}",
        std::process::id(),
        engine.replace('-', "_"),
        RUN.fetch_add(1, Ordering::Relaxed),
    ));
    std::fs::create_dir_all(&dir).expect("a scratch directory");
    let routes = routes_file(&dir);

    // Port zero, and the daemon reports back what the kernel gave it.
    //
    // Asking for a specific port meant guessing one: bind `:0`, read the
    // number, close the listener, and hand it to a process that binds it a
    // moment later. Under a full parallel `cargo test` the workspace is opening
    // ephemeral sockets constantly, and anything that took the number in that
    // window made the daemon exit with "address already in use" — which this
    // test reported as "the daemon should be serving", pointing at the daemon
    // rather than at the guess.
    //
    // The worse half is that a stolen port does not always fail. Under the same
    // stress `uring_strict_refuses_to_start_instead` failed the other way: its
    // daemon refused to start, exactly as it should, and the connect *still*
    // succeeded — against whichever concurrent daemon had taken the number. The
    // test then reported that uring-strict had served, having never spoken to
    // the process it started. A port read back from this child's own banner
    // cannot reach another one.
    let mut command = Command::new(env!("CARGO_BIN_EXE_ramjet-ingressd"));
    command
        .arg(format!("--static-routes={}", routes.display()))
        .arg("--http=127.0.0.1:0")
        .arg("--no-https")
        .arg("--admin=127.0.0.1:0")
        .arg(format!("--engine={engine}"))
        .env("RUST_LOG", "info")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    if unavailable {
        command.env(ramjet_engine::engine::UNAVAILABLE_ENV, "1");
    } else {
        command.env_remove(ramjet_engine::engine::UNAVAILABLE_ENV);
    }

    let mut child = command.spawn().expect("the daemon started");
    let output = Arc::new(Mutex::new(String::new()));
    let readers = [
        drain(
            child.stdout.take().expect("stdout is piped"),
            Arc::clone(&output),
        ),
        drain(
            child.stderr.take().expect("stderr is piped"),
            Arc::clone(&output),
        ),
    ];

    // Waiting for the listener rather than for a timer: "has not exited yet" is
    // a weaker claim than "is accepting connections", and it is the second one
    // that says an engine started.
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut serving = false;
    loop {
        let port = banner_http_port(&output.lock().expect("the readers have not panicked"));
        if let Some(port) = port {
            if std::net::TcpStream::connect(("127.0.0.1", port)).is_ok() {
                serving = true;
                break;
            }
        }
        if child.try_wait().expect("the child is waitable").is_some() {
            break;
        }
        if Instant::now() >= deadline {
            break;
        }
        std::thread::sleep(Duration::from_millis(20));
    }

    // Unconditional, because a child that has already exited is not harmed by
    // it and one that is serving has to be stopped before its pipes close.
    let _ = child.kill();
    let _ = child.wait();
    for reader in readers {
        let _ = reader.join();
    }

    let collected = output
        .lock()
        .expect("the readers have not panicked")
        .clone();
    let _ = std::fs::remove_dir_all(&dir);
    (serving, collected)
}

#[test]
fn uring_falls_back_to_hyper_when_the_reactor_will_not_start() {
    let (serving, output) = run_until_serving("uring", true);
    assert!(
        serving,
        "the daemon should have fallen back and kept serving; it exited:\n{output}"
    );
    assert!(
        output.contains("falling back to the hyper engine"),
        "the fallback must say it happened:\n{output}"
    );
    assert!(
        output.contains(ramjet_engine::engine::UNAVAILABLE_ENV),
        "the fallback must name the reason, not just the outcome:\n{output}"
    );
}

#[test]
fn uring_strict_refuses_to_start_instead() {
    let (serving, output) = run_until_serving("uring-strict", true);
    assert!(
        !serving,
        "uring-strict must not serve on the other engine:\n{output}"
    );
    assert!(
        output.contains("uring-strict"),
        "the refusal must name the flag that caused it:\n{output}"
    );
    // The two causes an operator actually hits, named, because the errno alone
    // does not tell them which one they are looking at.
    assert!(
        output.contains("seccomp"),
        "the refusal must say what usually causes this:\n{output}"
    );
}

#[test]
fn uring_serves_on_a_host_where_the_reactor_works() {
    // The other half of the same switch: with nothing blocking it, the engine
    // that was asked for is the engine that runs. On macOS the reactor is
    // kqueue and this never had a chance to fail.
    let (serving, output) = run_until_serving("uring", false);
    assert!(serving, "the daemon should be serving:\n{output}");
    assert!(
        !output.contains("falling back"),
        "nothing was blocking the reactor, so nothing should have fallen back:\n{output}"
    );
    assert!(
        output.contains("engine uring"),
        "the banner should say which engine is serving:\n{output}"
    );
}

#[test]
fn uring_strict_serves_where_uring_would() {
    let (serving, output) = run_until_serving("uring-strict", false);
    assert!(serving, "the daemon should be serving:\n{output}");
    assert!(
        !output.contains("falling back"),
        "strict mode never falls back, and had no reason to here:\n{output}"
    );
}

#[test]
fn hyper_never_probes_the_reactor() {
    // The probe is one `io_uring_setup` and its teardown, and a deployment that
    // asked for hyper should not pay for it — nor be affected by an environment
    // variable aimed at the other engine.
    let (serving, output) = run_until_serving("hyper", true);
    assert!(serving, "the daemon should be serving:\n{output}");
    assert!(
        !output.contains("falling back"),
        "hyper was asked for and hyper is what runs:\n{output}"
    );
}
