# Building from source

## The sibling repository

`ramjet-engine` depends on the `ramjet` runtime and its sans-io HTTP codec from
a **sibling repository** by path, so the workspace expects that checkout beside
this one:

```text
.../
  ramjet-ingress/     <- this repository
  enhance-socket/     <- the ramjet runtime and ramjet-http
```

Without it, `cargo` refuses to load the workspace at all rather than skipping
the crate. It is also why the container builds take the parent directory as
their build context.

## Build and test

```sh
cargo build --release
cargo test --workspace
```

The release profile is thin LTO, `codegen-units = 1`, and `panic = "abort"`.
That last one is not a size tweak: the data plane has no recovery story for a
panicking worker, so unwinding past a half-written connection is worse than
dying loudly. Cargo ignores the setting for the test and bench profiles, so
`cargo test` and the criterion harness still build normally.

Minimum supported Rust version is **1.85**.

## The crates

```text
crates/
  ramjet-router/      sans-io: route table, matcher, LB selection
  ramjet-proxy/       sockets, rustls, HTTP/1.1 + HTTP/2 + HTTP/3, upstream pools
  ramjet-controller/  Kubernetes informers, annotation translation, status
  ramjet-engine/      the experimental completion-based data plane
  ramjet-ingressd/    the daemon binary
  ramjet-top/         the terminal cockpit
```

Two dependency rules hold the design together, and they are worth understanding
before changing anything.

**`ramjet-router` depends on `arc-swap`, `regex`, and `thiserror`. Not on tokio,
not on hyper, not on rustls.** It never opens a socket, spawns a task, or reads
a clock. Certificates are opaque handles, randomness is passed in as a number,
and canary decisions take borrowed header values rather than a header
collection. That is what makes the matcher testable against string literals and
benchmarkable without a network.

**`ramjet-controller` holds no rustls types either**, for the mirror-image
reason: parsing a certificate means a crypto provider, and a crypto provider in
the control plane would mean the translation layer could no longer be
unit-tested against objects built in memory.

The daemon is the only crate that depends on both sides — which is also why
canary auto-promotion and the rollback-pin bridge live there.

## Testing

```sh
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
cargo bench -p ramjet-router
```

Some things worth knowing about the test suite:

- **`translate` is a pure function**: cluster snapshot in, compiled config out,
  no I/O and no clock. Class filtering, path precedence, endpoint resolution,
  canary merging, and conflict arbitration all have unit tests that construct
  API objects in memory and assert on the compiled table.
- **`tests/no_alloc.rs`** installs a counting global allocator and asserts zero
  allocations across every path through the matcher. The counters are
  thread-local, because `cargo test` runs tests concurrently and a shared
  counter attributes one test's allocations to another.
- **`ramjet-top`'s mock-server tests** run the real client against a real
  `hyper` listener serving canned admin responses, and assert the computed view
  model rather than anything about pixels. Its `--once` tests spawn the compiled
  binary and read its stdout, stderr and exit status.
- **The auto-promotion state machine is a pure function** — `decide(policy,
  weight, window)` — with no clock, no cluster and no counters, so the entire
  decision table is a unit test.

### End to end

```sh
deploy/e2e.sh          # build the image, install the chart, assert routing
deploy/cloud-e2e.sh    # lint every preset, dry-run every manifest, PROXY protocol
```

Every `kubectl` and `helm` call in both scripts carries an explicit
`--context`/`--kube-context`, and they refuse to run against a cluster that does
not look local. A developer kubeconfig usually holds production clusters, and a
mistyped current-context is exactly how a test script deletes one.

## Benchmarks

See [Performance](./performance.md#reproducing-any-of-it) for what each harness
measures and the two rules that keep a re-run honest (do not shorten the warmup;
check the host is quiet first).

## Documentation

This site is [mdBook](https://rust-lang.github.io/mdBook/). Source is in
`docs/src`, and it is built and published to GitHub Pages by
`.github/workflows/docs.yml` on every push to `main` that touches `docs/`.

```sh
cargo install mdbook      # or: brew install mdbook
mdbook serve docs         # live reload at http://localhost:3000
mdbook build docs         # output in docs/book
```

Where the prose and the code disagree, the code wins — the annotation and flag
references are transcribed from `crates/ramjet-controller/src/annotations.rs`
and `crates/ramjet-ingressd/src/args.rs` respectively, and both have tests
asserting the vocabulary is complete.

## License

Dual-licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](https://github.com/rowbench/ramjet-ingress/blob/main/LICENSE-APACHE))
- MIT license ([LICENSE-MIT](https://github.com/rowbench/ramjet-ingress/blob/main/LICENSE-MIT))

at your option.

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in the work by you, as defined in the Apache-2.0 license, shall be
dual licensed as above, without any additional terms or conditions.
