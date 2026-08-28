# ramjet-ingress vs nginx: reverse-proxy head-to-head

**The two are now level at c64 and nginx is 9% ahead at c256.** That is the
result after the optimisation described in [After optimization](#after-optimization-commit-4f58bd7);
the first measurement, which had nginx 45% ahead, is kept below unchanged. Both
are reproducible with `./bench/run.sh`, and the rest of this document is about
how it was measured and what it does and does not mean.

The original reading follows first, because it is what the optimisation work was
aimed at and the "what this does not test" section applies to both.

## Method

- Both proxies forward `GET / Host: bench.test` to the **same pair** of nginx
  upstreams returning an identical 128-byte body, over one docker bridge
  network. No host port NAT is in the measured path.
- Each contender is pinned to the **same two cores** (`--cpuset-cpus=0,1`), the
  upstreams get one core each, and the load generator gets four. `--cpuset` is
  used rather than `--cpus` because a CPU *quota* is invisible to
  `sched_getaffinity`: nginx's `worker_processes auto` would have read the VM's
  8 CPUs while Rust's `available_parallelism()` reads the cgroup and would have
  given tokio 2 threads. Pinning makes both see 2 CPUs and start 2 workers.
  `run.sh` asserts this at startup.
- oha 1.16.0, HTTP/1.1 with keep-alive, `--host bench.test` against a literal
  IP so no DNS lookup enters either contender's path.
- Per contender: a discarded 10s warmup, then 3 x 30s at c64 and 1 x 30s at
  c256. Runs are **interleaved** so drift is shared rather than handed to
  whoever went second. c64 rows are the median-throughput run, not a per-column
  average, so every number in a row comes from one real 30-second measurement.
- **Both measurements below were taken with a fixed within-round order**
  (ramjet, nginx, baseline, repeated each round). `run.sh` has since been
  changed to *rotate* which contender leads each round, and to wait a 15s
  cooldown before each warmup. The reason is that plain interleaving assumes the
  machine is steady within a round, and on a laptop it is not: the package heats
  up as the round proceeds, so a fixed order hands whoever goes first a
  systematically cooler machine in every round — a bias in one direction that
  averaging over rounds cannot remove. Rotation spreads it evenly. Neither table
  below has been re-measured under the rotated protocol, so read the numbers as
  carrying that bias in ramjet's favour at c64, bounded by the size of the
  within-round drift (the baseline's 13.3% spread is the visible upper bound on
  it, and the contenders' 1.7-5.3% spread the likelier scale).
- **Baseline** is a third nginx pinned to the *proxies' own cores* serving the
  body directly, which makes "proxy overhead" a like-for-like subtraction: the
  same 2 cores, the same response, with and without the extra hop.
- Correctness is gated before anything is measured: both proxies must return
  the same 128 bytes, and ramjet must return 404 for an unrouted Host, proving
  host routing is actually exercised rather than passed through.

## Environment

```
date:      2026-08-27T13:51:27Z
host:      Darwin 25.5.0 arm64 (Apple Silicon), 12 host CPUs
docker:    29.7.2 (Docker Desktop, linuxkit VM)
docker VM: 8 CPUs, kernel 7.0.12-linuxkit
nginx:     nginx:1-alpine -> nginx version: nginx/1.31.4
oha:       ghcr.io/hatoo/oha:latest -> oha 1.16.0
rustc:     rustc 1.98.0 (88d9e12ae 2026-08-18) (in the builder image)
ramjet:    ramjet-ingressd 0.1.0, cargo build --release
           (thin LTO, codegen-units=1, panic=abort)
```

The rerun after the optimisation was the same host and toolchain a few hours
later; only the two lines that can differ are worth repeating:

```
date:      2026-08-27T16:21:45Z
ramjet:    ramjet-ingressd 0.1.0 @ 4f58bd7, cargo build --release
```

This is a **macOS Docker Desktop VM**. The numbers are valid *relative to each
other* under identical conditions; they are not Linux bare-metal absolutes and
should not be quoted as such.

## Results (first measurement, commit d1c08c6)

### Concurrency 64 (median of 3 x 30s runs)

| Contender | RPS | p50 | p90 | p99 | p99.9 |
|---|---:|---:|---:|---:|---:|
| ramjet-ingress | 61,568 | 967 us | 1,353 us | 3,107 us | 8,468 us |
| nginx | 89,593 | 664 us | 876 us | 1,822 us | 6,173 us |
| baseline (no proxy) | 247,875 | 216 us | 309 us | 915 us | 3,656 us |

### Concurrency 256 (single 30s run)

| Contender | RPS | p50 | p90 | p99 | p99.9 |
|---|---:|---:|---:|---:|---:|
| ramjet-ingress | 59,644 | 4,027 us | 5,730 us | 11,930 us | 28,298 us |
| nginx | 88,867 | 2,544 us | 3,702 us | 8,233 us | 21,370 us |
| baseline (no proxy) | 262,377 | 897 us | 1,179 us | 2,427 us | 5,504 us |

### Run-to-run spread (c64 RPS, every run)

| Contender | run 1 | run 2 | run 3 | spread |
|---|---:|---:|---:|---:|
| ramjet-ingress | 61,568 | 61,236 | 64,531 | 5.3% |
| nginx | 89,593 | 89,804 | 88,246 | 1.7% |
| baseline (no proxy) | 247,875 | 219,715 | 251,504 | 13.3% |

### Added latency of the proxy hop (c64, vs baseline)

| Contender | added p50 | added p99 | RPS vs baseline |
|---|---:|---:|---:|
| ramjet-ingress | +752 us | +2,192 us | 25% |
| nginx | +449 us | +907 us | 36% |

### Errors

**Zero.** Across all 12 measured runs — 47,560,185 requests — every response
was a 200 and oha recorded no transport errors. Error rate 0.000% for every
contender at both concurrencies. Per-run accounting is in
`results/before/*.json`.

(oha's `-w` flag is used so in-flight requests are awaited at the deadline.
Without it oha counts exactly `-c` abandoned requests as errors on every run,
which is a harness artifact that would mask a real error signal.)

### Was the proxy actually the bottleneck?

Yes, and this is the check that decides whether the table means anything:

| Component | Utilisation during a c64 run |
|---|---|
| proxy under test | 201.6% (ramjet) / 197.9% (nginx) of its 2 cores — **saturated** |
| upstream 1 / 2 | 45.5% / 45.1% of one core each — idle by comparison |
| load generator | 4 cores; independently measured to 248k rps, ~2.8x the fastest contender |

Both contenders were pinned against their CPU ceiling while nothing else in the
topology was close to its own. The measurement is of the proxies.

## Reading the first measurement

Everything in this section describes the commit d1c08c6 numbers above. It is
kept as written — including the parts the optimisation went on to disprove,
because a prediction is only worth anything if you can still see what it was.

**nginx wins this benchmark, clearly and at every percentile.** It forwards 45%
more requests per second at c64 (49% at c256), its median latency is a third
lower, and its p99 is roughly 40% lower. There is no percentile and no
concurrency level in this test where ramjet-ingress is ahead. The gap is an
order of magnitude larger than the run-to-run noise (1.7-5.3%), so it is real
rather than an artifact of sampling.

**The cost is broad, not one pathology.** Divide two cores by throughput and
ramjet spends ~32.5 us of CPU per request against nginx's ~22.3 us — about 46%
more work for the same forwarded byte. The one concrete asymmetry found is
upstream connection churn: in steady state ramjet opened a new upstream
connection every ~590 requests while nginx opened one every ~28,700, which is
`pool_max_idle_per_host: 32` being too small for 64 in-flight requests across
two endpoints (nginx is configured with `keepalive 64` per worker). But that is
~0.17% of requests, worth well under 1% of CPU, so it does **not** explain a 45%
gap. The rest is diffuse per-request cost in the hyper/tokio forwarding path.

> **This paragraph was half right.** Profiling found the churn was worth even
> less than estimated, and the "diffuse per-request cost" was not diffuse: it
> was one thing, the work-stealing runtime moving each request between cores,
> worth 43% of the CPU per request. See [`PROFILE.md`](PROFILE.md).

**Route matching is not where the time goes.** ARCHITECTURE.md benchmarks
`match_request` at 25.2 ns, which is 0.08% of ramjet's 32.5 us per-request cost.
The part of ramjet-ingress that has been optimised and measured is already
free; the throughput difference lives entirely in the machinery around it —
connection handling, header rewriting, body streaming, upstream pooling. Tuning
the router further would buy nothing measurable here.

**Both hold up under pressure.** Quadrupling concurrency from 64 to 256 changed
neither contender's throughput by more than 3% and produced zero errors from
either; latency grew roughly 4x for both, which is what saturation plus Little's
law predicts. ramjet-ingress does not collapse, thrash, or drop requests under
4x its saturation concurrency — it is simply slower per request. Memory is a
wash: ramjet starts leaner (11.2 MiB idle vs 16.3 MiB) but the two converge
under load (19.2 MiB vs 18.6 MiB).

**What this does not test.** This is a forwarding-engine drag race on the
narrowest possible workload: one route, one host, 128-byte responses, plaintext
HTTP/1.1, static configuration. It says nothing about the project's actual
thesis — that a config change is a pointer swap rather than an nginx reload, so
the cost of a deploy does not scale with the traffic you are carrying. Nothing
here exercises TLS termination, HTTP/2, large or streaming bodies, thousands of
routes, or configuration churn under live traffic, and the ingress-nginx control
plane that the reload argument is aimed at was explicitly out of scope. A fair
summary of *this measurement* is: **on raw HTTP/1.1 forwarding throughput,
ramjet-ingress is about two-thirds of nginx, and the speed argument for the
project has to be made on config-change behaviour rather than on requests per
second.**

> That last sentence is the one the next section overturns. The
> "what this does not test" paragraph above it still stands unchanged.

## After optimization (commit 4f58bd7)

Same `./bench/run.sh`, same host, same afternoon, same everything the Method
section describes. What changed is the data plane: the profile behind it is
[`PROFILE.md`](PROFILE.md), and the short version is that ramjet-ingress now
runs one `current_thread` tokio runtime per core with nothing shared between
them, instead of one work-stealing runtime across both, and its upstream idle
pool is sized for the concurrency it actually sees.

### Concurrency 64 (median of 3 x 30s runs)

| Contender | RPS | p50 | p90 | p99 | p99.9 |
|---|---:|---:|---:|---:|---:|
| ramjet-ingress | 85,908 | 666 us | 921 us | 2,528 us | 6,236 us |
| nginx | 86,670 | 671 us | 873 us | 2,314 us | 5,902 us |
| baseline (no proxy) | 229,400 | 223 us | 356 us | 1,219 us | 4,421 us |

### Concurrency 256 (single 30s run)

| Contender | RPS | p50 | p90 | p99 | p99.9 |
|---|---:|---:|---:|---:|---:|
| ramjet-ingress | 82,524 | 2,975 us | 3,617 us | 6,396 us | 14,185 us |
| nginx | 89,636 | 2,652 us | 3,559 us | 7,683 us | 17,180 us |
| baseline (no proxy) | 247,077 | 918 us | 1,233 us | 3,554 us | 9,111 us |

### Run-to-run spread (c64 RPS, every run)

| Contender | run 1 | run 2 | run 3 | spread |
|---|---:|---:|---:|---:|
| ramjet-ingress | 86,551 | 85,121 | 85,908 | 1.7% |
| nginx | 84,124 | 86,670 | 88,048 | 4.5% |
| baseline (no proxy) | 229,400 | 228,659 | 242,940 | 6.1% |

### Added latency of the proxy hop (c64, vs baseline)

| Contender | added p50 | added p99 | RPS vs baseline |
|---|---:|---:|---:|
| ramjet-ingress | +443 us | +1,308 us | 37% |
| nginx | +448 us | +1,095 us | 38% |

### The delta

| Measure | Before | After | Change |
|---|---:|---:|---:|
| c64 throughput | 61,568 | 85,908 | **+39.5%** |
| c256 throughput | 59,644 | 82,524 | **+38.4%** |
| c64 p50 | 967 us | 666 us | -31% |
| c64 p99 | 3,107 us | 2,528 us | -19% |
| CPU per request | 32.5 us | **23.6 us** | -27% |
| vs nginx at c64 | 69% of it | **99% of it** | — |
| vs nginx at c256 | 67% of it | **92% of it** | — |
| requests per upstream connection | ~590 | **8,179** | 14x |
| memory under load | 19.2 MiB | 33.1 MiB | +72% |

### Reading the new numbers

**At c64 the two are level.** 85,908 against 86,670 is a 0.9% difference, and
nginx's own three runs spread 4.5% — the gap is now smaller than the noise in
the measurement, which means this benchmark can no longer tell them apart at
this concurrency. It does not mean ramjet-ingress is faster; it means the honest
statement is "the same". Divide two cores by throughput and both spend 23.6 us
of CPU per request, which is the same claim made a second way.

**At c256 nginx is still 9% ahead**, 89,636 against 82,524, and that gap is
outside the noise. nginx's throughput barely moves between c64 and c256 (+3%)
while ramjet's drops 4%. Latency runs the other way — ramjet's p99 at c256 is
6,396 us against nginx's 7,683 us — so what this looks like is ramjet trading a
little throughput for shorter queues under saturation, not falling over.

**The upstream connection churn is fixed but not eliminated.** ramjet now gets
8,179 requests out of each upstream connection, up from ~590; nginx gets 27,082.
That remaining 3x is a smaller share of CPU than the measurement can resolve
(0.01% of requests open a connection), and it is a consequence of a per-runtime
pool: with two runtimes each holding their own idle connections, a connection
returned to a full pool on one runtime cannot be reused by the other.

**Memory got worse, and that is a real cost.** 33.1 MiB under load against
19.2 MiB before, and against nginx's 12.6 MiB. One runtime per core means one
connection pool, one timer wheel and one set of hyper buffers per core rather
than per process. On a 2-core replica that is 14 MiB; on a 64-core node with no
CPU limit it would be considerably more, which is an argument for setting
`--worker-threads` deliberately rather than letting it follow the host.

**Nothing else moved.** Zero errors across all twelve runs again — 49,114,324
requests, every one a 200, no transport errors from any contender at either
concurrency. And the saturation check still holds, which is what decides whether
the table means anything:

| Component | Utilisation during a c64 run |
|---|---|
| proxy under test | 202.5% (ramjet) / 204.3% (nginx) of its 2 cores — **saturated** |
| upstream 1 / 2 | 58.5% / 56.4% of one core each |
| load generator | 4 cores; the baseline's 229k rps is 2.7x the fastest contender |

Both contenders were against their CPU ceiling and nothing else in the topology
was near its own, exactly as in the first measurement.

### What this changes about the project's argument

The previous version of this page ended by saying the speed argument for
ramjet-ingress "has to be made on config-change behaviour rather than on
requests per second". That is no longer true at c64 and is a weaker claim at
c256: on this workload, on these two cores, raw HTTP/1.1 forwarding throughput
is now a tie at moderate concurrency and a 9% loss at high concurrency, rather
than a 45% loss.

Everything in **What this does not test** below still applies without
modification, and it is the more important paragraph on this page. This is one
route, one host, 128-byte plaintext responses and static configuration. Nothing
here exercises TLS, HTTP/2, large bodies, thousands of routes, or configuration
churn under live traffic.

## Known unfairness and deviations

Things that could not be fully eliminated, stated so a reader can discount them:

- **Shared docker daemon.** Another agent was building images on this daemon
  during the session. One 4-second smoke run showed nginx at half throughput
  with an unchanged p50 and a 271 ms stall — an external stall, not a config
  effect. The reported 30s runs absorb this, and the 1.7-5.3% contender spread
  is the evidence; the baseline's 13.3% spread shows the noise is real but did
  not touch the contenders' ordering.
- **Upstream keepalive pools were not exactly equal in the first measurement.**
  nginx holds 64 idle upstream connections per worker (128 total); ramjet held
  32 per endpoint (64 total), hardcoded with no CLI flag. The edge there was
  nginx's, and it was left alone rather than patched, because changing the
  product to win its own benchmark is not a measurement. It was then changed on
  purpose, with the measurement rerun from scratch: the value is now
  `--upstream-pool-idle`, defaulting to 128 per endpoint *per serving runtime*.
  Both contenders in the second table are therefore configured generously, and
  neither is starved.
- **Warmup is 10s, not the 5s originally specified.** A cold ramjet container
  measured 42k rps on its first run against 58k once warm, so 5s risked
  measuring warmup. The longer warmup is applied identically to both.
- **nginx tuning choices were tested, not assumed.** `reuseport` was measured
  both ways (with: ~88.5k and stable; without: ~86k with a 69k outlier) and kept
  because it is better for nginx. `access_log off` removes nginx's default
  per-request write, which ramjet does not have — verified: ramjet logged 6
  lines total across 640k requests, so neither contender pays a logging tax.
  `proxy_cache` was deliberately **not** enabled: ramjet has no response cache,
  and serving from nginx's memory would compare two different jobs.
- **Both are round-robin** across the same two endpoints, matching nginx's
  default, rather than ramjet's `leastConn`.
- **arm64 Apple Silicon under a linuxkit VM.** Absolute numbers would differ on
  x86-64 bare metal; the relative comparison is what this page claims.

## Reproducing

```sh
./bench/run.sh                                    # ~15 minutes, cleans up after itself
WARMUP=10s DURATION=5s ROUNDS=1 ./bench/run.sh    # quick smoke test
python3 bench/report.py                           # re-render tables from committed JSON
```

Tunables, all environment variables: `WARMUP`, `DURATION`, `COOLDOWN`,
`ROUNDS`, `CONC_MAIN`, `CONC_HIGH`.

**Overriding any of them redirects output to `results/scratch/`** (gitignored),
so only an unmodified committed-protocol run can write where the committed
measurement lives. That guard is there because the alternative failed quietly:
`run_all` begins by deleting `results/*.json`, so a `ROUNDS=1` smoke run removed
the measurement's files and wrote back three, leaving the tables above unable to
re-derive from the JSON beside them. The dangerous part was not the breakage but
its plausibility — the smoke numbers were low enough to read as a regression and
high enough to be believed, which is exactly the kind of wrong number that gets
quoted instead of caught.

**Do not shorten `WARMUP` below 10s, including in the smoke test.** A cold
contender measures its own warmup rather than its throughput, and the effect is
large enough to look like a regression: a 2s warmup measured 39,000 rps at c64
and 72,549 rps at c128 in the run immediately after — throughput rising with
concurrency is the signature of a first run that had not finished warming. The
effect grew with the per-core runtime change, because there is now a connection
pool, timer wheel and buffer set to warm per core rather than one of each.

**Check the host before trusting any number.** The baseline is the right canary
because it has nothing under test — plain nginx, static body, no proxying — so
any shortfall in it is the machine. On a quiet host it measures ~248,000 rps.
Below ~230,000 the host is busy and the run is not worth starting; measurements
taken there have shown 20-40% run-to-run spread against the 1.7-4.5% this
harness produces on a quiet machine. Note that this is host-side contention
(browsers, Kubernetes GUIs, Spotlight indexing a fresh `target/`), and it
reaches the guest through vCPU preemption, so container `--cpuset` pinning does
not protect against it.

`run.sh` is idempotent, namespaces everything it creates as `ramjet-bench-*`,
and removes only its own containers on exit (including on Ctrl-C) — the docker
daemon may be shared. Raw oha JSON for every run is committed, so both sets of
tables can be re-derived without re-running anything:

- `results/` holds the **after** run (commit 4f58bd7), which is what
  `report.py` reads.
- `results/before/` holds the **first measurement** (commit d1c08c6) verbatim,
  including its own `versions.txt` and `diagnostics.txt`. `report.py` globs one
  directory deep and does not pick these up; to re-render them, point it at the
  subdirectory or read `results/before/table.md`, which is the table it
  produced.
