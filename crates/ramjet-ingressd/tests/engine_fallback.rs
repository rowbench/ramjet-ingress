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
use std::net::TcpListener;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

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

/// A port nothing is using, released before it is handed over.
fn free_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").expect("a listener");
    let port = listener.local_addr().expect("an address").port();
    drop(listener);
    port
}

/// Run the daemon until it says something, then stop it.
///
/// Returns everything it wrote to stderr. The startup banner goes to stdout and
/// the engine decision goes to the log, so both are collected.
fn run_until_serving(engine: &str, unavailable: bool) -> (bool, String) {
    let dir = std::env::temp_dir().join(format!(
        "ramjet-engine-fallback-{}-{}",
        std::process::id(),
        engine.replace('-', "_")
    ));
    std::fs::create_dir_all(&dir).expect("a scratch directory");
    let routes = routes_file(&dir);
    let http = free_port();

    let mut command = Command::new(env!("CARGO_BIN_EXE_ramjet-ingressd"));
    command
        .arg(format!("--static-routes={}", routes.display()))
        .arg(format!("--http=127.0.0.1:{http}"))
        .arg("--no-https")
        .arg(format!("--admin=127.0.0.1:{}", free_port()))
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

    // Waiting for the listener rather than for a timer: "has not exited yet" is
    // a weaker claim than "is accepting connections", and it is the second one
    // that says an engine started.
    let deadline = Instant::now() + Duration::from_secs(10);
    let serving = loop {
        if child.try_wait().expect("the child is waitable").is_some() {
            break false;
        }
        if std::net::TcpStream::connect(("127.0.0.1", http)).is_ok() {
            break true;
        }
        if Instant::now() >= deadline {
            break false;
        }
        std::thread::sleep(Duration::from_millis(20));
    };

    if serving {
        let _ = child.kill();
    }
    let _ = child.wait();
    let alive = serving;

    let mut output = String::new();
    if let Some(mut stderr) = child.stderr.take() {
        let _ = stderr.read_to_string(&mut output);
    }
    if let Some(mut stdout) = child.stdout.take() {
        let mut banner = String::new();
        let _ = stdout.read_to_string(&mut banner);
        output.push_str(&banner);
    }
    let _ = std::fs::remove_dir_all(&dir);
    (alive, output)
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
