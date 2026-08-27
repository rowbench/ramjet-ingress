# ramjet-ingress vs nginx: reverse-proxy head-to-head

**nginx forwards about 45% more requests per second than ramjet-ingress on this
workload, and is ahead at every latency percentile.** That is the result, it is
reproducible with `./bench/run.sh`, and the rest of this document is about how
it was measured and what it does and does not mean.

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
  c256. Runs are **interleaved** (ramjet, nginx, baseline, repeat) so drift is
  shared rather than handed to whoever went second. c64 rows are the
  median-throughput run, not a per-column average, so every number in a row
  comes from one real 30-second measurement.
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

This is a **macOS Docker Desktop VM**. The numbers are valid *relative to each
other* under identical conditions; they are not Linux bare-metal absolutes and
should not be quoted as such.

## Results

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
contender at both concurrencies. Per-run accounting is in `results/*.json`.

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

## Reading the numbers

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
summary of this page is: **on raw HTTP/1.1 forwarding throughput, ramjet-ingress
is currently about two-thirds of nginx, and the speed argument for the project
has to be made on config-change behaviour rather than on requests per second.**

## Known unfairness and deviations

Things that could not be fully eliminated, stated so a reader can discount them:

- **Shared docker daemon.** Another agent was building images on this daemon
  during the session. One 4-second smoke run showed nginx at half throughput
  with an unchanged p50 and a 271 ms stall — an external stall, not a config
  effect. The reported 30s runs absorb this, and the 1.7-5.3% contender spread
  is the evidence; the baseline's 13.3% spread shows the noise is real but did
  not touch the contenders' ordering.
- **Upstream keepalive pools are not exactly equal.** nginx holds 64 idle
  upstream connections per worker (128 total); ramjet holds 32 per endpoint (64
  total) and that is hardcoded with no CLI flag. The edge here is nginx's. It
  was left alone rather than patched, because changing the product to win its
  own benchmark is not a measurement — but it is the first thing to try if
  somebody wants to close the gap, and it is worth exposing as a flag.
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
./bench/run.sh                                   # ~11 minutes, cleans up after itself
WARMUP=2s DURATION=5s ROUNDS=1 ./bench/run.sh    # quick smoke test
python3 bench/report.py                          # re-render tables from committed JSON
```

`run.sh` is idempotent, namespaces everything it creates as `ramjet-bench-*`,
and removes only its own containers on exit (including on Ctrl-C) — the docker
daemon may be shared. Raw oha JSON for every run is committed under
`results/`, so the tables can be re-derived without re-running anything.
