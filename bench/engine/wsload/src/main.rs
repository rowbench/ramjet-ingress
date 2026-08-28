//! A WebSocket echo load generator, because oha does not speak WebSocket.
//!
//! # What this measures, and what it deliberately does not
//!
//! One number: how many echo round trips a second a proxy carries, and at what
//! latency. That is the only thing a *passthrough* tunnel can be measured on.
//! Neither engine parses a frame — after a 101 the bytes are opaque to both —
//! so a benchmark that varied frame types, or fragmentation, or masking would
//! be measuring this program against itself with a proxy in the middle.
//!
//! It is not a WebSocket conformance test and makes no attempt to be one. The
//! frames it sends are the simplest legal ones: a masked binary frame from the
//! client, and whatever the echo server sends back. `enhance-socket`'s Autobahn
//! suite is where conformance is checked, on the codec that is not in this
//! path.
//!
//! # Why it is a separate binary rather than a test
//!
//! Because it has to run against a container over a network, pinned to its own
//! cores, alongside two other containers — the same topology every other
//! contender in `bench/engine` is measured in. A `#[test]` on loopback would
//! measure the loopback.
//!
//! Usage:
//!     wsload <addr:port> <connections> <seconds> [payload-bytes] [host-header]
//!
//! `addr:port` is where to connect; `host-header` is what to put in `Host:`,
//! defaulting to the address. The two are separate because the proxy under test
//! routes by `Host` and is reached by IP — passing the IP as the host would
//! match no route, which is a 404 the load generator would report as a failed
//! handshake and nothing more.
//!
//! Prints one line of JSON, so the reporting script does not have to parse
//! prose:
//!
//! ```text
//! {"connections":64,"seconds":10,"payload":128,"echoes":1234567,
//!  "echoes_per_sec":123456.7,"p50_micros":410,"p99_micros":1830,"errors":0}
//! ```

use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// The handshake, with the fixed key from RFC 6455's example.
///
/// A real client randomises it and checks the accept value. Neither matters
/// here: the proxy forwards the handshake without looking at it, and the echo
/// server behind it is one this benchmark started.
fn handshake(host: &str, path: &str) -> String {
    format!(
        "GET {path} HTTP/1.1\r\nHost: {host}\r\nUpgrade: websocket\r\n\
         Connection: Upgrade\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\
         Sec-WebSocket-Version: 13\r\n\r\n"
    )
}

/// A masked binary frame, which is what a client must send.
///
/// The mask is fixed rather than random, for the same reason the key is: the
/// hop under test does not read it. A server that checked would still accept
/// it — masking is required, randomness is a defence against cache poisoning by
/// intermediaries, and there are none here.
fn frame(payload: &[u8]) -> Vec<u8> {
    let mask = [0x12u8, 0x34, 0x56, 0x78];
    let mut out = vec![0x82]; // FIN, binary
    match payload.len() {
        n if n < 126 => out.push(0x80 | n as u8),
        n if n <= u16::MAX as usize => {
            out.push(0x80 | 126);
            out.extend_from_slice(&(n as u16).to_be_bytes());
        }
        n => {
            out.push(0x80 | 127);
            out.extend_from_slice(&(n as u64).to_be_bytes());
        }
    }
    out.extend_from_slice(&mask);
    out.extend(
        payload
            .iter()
            .enumerate()
            .map(|(i, byte)| byte ^ mask[i % 4]),
    );
    out
}

/// How many bytes the echo of `payload` occupies coming back.
///
/// The server does not mask, so its frame is four bytes shorter than the one
/// that went out. Counting it exactly is what lets the reader wait for a whole
/// echo rather than for "some bytes", which would time a partial read.
fn echo_len(payload: usize) -> usize {
    let header = match payload {
        n if n < 126 => 2,
        n if n <= u16::MAX as usize => 4,
        _ => 10,
    };
    header + payload
}

struct Stats {
    echoes: AtomicU64,
    errors: AtomicU64,
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 4 {
        eprintln!(
            "usage: wsload <addr:port> <connections> <seconds> [payload-bytes] [host-header]"
        );
        std::process::exit(2);
    }
    let target = args[1].clone();
    let connections: usize = args[2].parse().expect("connections must be a number");
    let seconds: u64 = args[3].parse().expect("seconds must be a number");
    let payload_len: usize = args
        .get(4)
        .map_or(128, |s| s.parse().expect("payload must be a number"));
    let host_header = args
        .get(5)
        .cloned()
        .unwrap_or_else(|| target.split(':').next().unwrap_or("localhost").to_owned());

    let stop = Arc::new(AtomicBool::new(false));
    let stats = Arc::new(Stats {
        echoes: AtomicU64::new(0),
        errors: AtomicU64::new(0),
    });

    let payload = vec![b'w'; payload_len];
    let expected = echo_len(payload_len);
    let started = Instant::now();

    let workers: Vec<_> = (0..connections)
        .map(|_| {
            let target = target.clone();
            let host_header = host_header.clone();
            let payload = payload.clone();
            let stop = Arc::clone(&stop);
            let stats = Arc::clone(&stats);
            std::thread::spawn(move || {
                // Latencies are kept per connection and merged at the end: a
                // shared vector behind a lock would put this program's own
                // contention into the numbers it is reporting.
                let mut samples: Vec<u64> = Vec::with_capacity(1 << 16);
                match run_connection(
                    &target,
                    &host_header,
                    &payload,
                    expected,
                    &stop,
                    &stats,
                    &mut samples,
                ) {
                    Ok(()) => {}
                    Err(error) => {
                        // Reported once, on stderr, rather than only counted.
                        // A run that produces zero echoes and a bare error
                        // count says nothing about why, and "why" is a refused
                        // upgrade, a route that did not match, or a peer that
                        // hung up — three different problems.
                        if stats.errors.fetch_add(1, Ordering::Relaxed) == 0 {
                            eprintln!("wsload: {error}");
                        }
                    }
                }
                samples
            })
        })
        .collect();

    std::thread::sleep(Duration::from_secs(seconds));
    stop.store(true, Ordering::Relaxed);

    let mut samples: Vec<u64> = Vec::new();
    for worker in workers {
        if let Ok(mut theirs) = worker.join() {
            samples.append(&mut theirs);
        }
    }
    let elapsed = started.elapsed().as_secs_f64();
    samples.sort_unstable();

    let percentile = |p: f64| -> u64 {
        if samples.is_empty() {
            return 0;
        }
        let index = ((samples.len() as f64 - 1.0) * p).round() as usize;
        samples[index.min(samples.len() - 1)]
    };

    let echoes = stats.echoes.load(Ordering::Relaxed);
    println!(
        "{{\"connections\":{connections},\"seconds\":{seconds},\"payload\":{payload_len},\
         \"echoes\":{echoes},\"echoes_per_sec\":{:.1},\"p50_micros\":{},\"p99_micros\":{},\
         \"errors\":{}}}",
        echoes as f64 / elapsed,
        percentile(0.50),
        percentile(0.99),
        stats.errors.load(Ordering::Relaxed),
    );
}

#[allow(clippy::too_many_arguments)]
fn run_connection(
    target: &str,
    host_header: &str,
    payload: &[u8],
    expected: usize,
    stop: &AtomicBool,
    stats: &Stats,
    samples: &mut Vec<u64>,
) -> std::io::Result<()> {
    let mut socket = TcpStream::connect(target)?;
    socket.set_nodelay(true)?;
    socket.set_read_timeout(Some(Duration::from_secs(10)))?;

    socket.write_all(handshake(host_header, "/ws").as_bytes())?;
    socket.flush()?;

    // Read exactly the handshake response, and no further: anything after the
    // blank line is already tunnel traffic.
    let mut head = Vec::with_capacity(256);
    let mut byte = [0u8; 1];
    while !head.ends_with(b"\r\n\r\n") {
        if socket.read(&mut byte)? == 0 {
            return Err(std::io::Error::other("the peer closed during the handshake"));
        }
        head.push(byte[0]);
        if head.len() > 8192 {
            return Err(std::io::Error::other("the handshake response is too large"));
        }
    }
    if !head.starts_with(b"HTTP/1.1 101") {
        return Err(std::io::Error::other(format!(
            "the upgrade was refused: {}",
            String::from_utf8_lossy(&head[..head.len().min(64)])
        )));
    }

    let outgoing = frame(payload);
    let mut incoming = vec![0u8; expected];
    // One echo in flight at a time, deliberately: this measures round-trip
    // latency through the tunnel, and pipelining would measure throughput while
    // reporting a latency that no request actually experienced.
    while !stop.load(Ordering::Relaxed) {
        let sent = Instant::now();
        socket.write_all(&outgoing)?;
        socket.flush()?;

        let mut have = 0;
        while have < expected {
            let n = socket.read(&mut incoming[have..])?;
            if n == 0 {
                return Err(std::io::Error::other("the tunnel closed"));
            }
            have += n;
        }
        samples.push(u64::try_from(sent.elapsed().as_micros()).unwrap_or(u64::MAX));
        stats.echoes.fetch_add(1, Ordering::Relaxed);
    }
    Ok(())
}
