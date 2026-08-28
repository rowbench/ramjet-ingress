# Performance

Four benchmark documents live in the repository, and this page condenses them.
Every table below is reproduced from a committed measurement; the raw JSON, the
version manifests and the diagnostics are in-tree so any of it can be
re-derived without re-running anything.

| Document | The question it answers |
|---|---|
| [`bench/thesis/RESULTS.md`](https://github.com/rowbench/ramjet-ingress/blob/main/bench/thesis/RESULTS.md) | What does a configuration change cost, against ingress-nginx? **This is the project's actual thesis.** |
| [`bench/RESULTS.md`](https://github.com/rowbench/ramjet-ingress/blob/main/bench/RESULTS.md) | Raw HTTP/1.1 forwarding throughput, against nginx |
| [`bench/engine/RESULTS.md`](https://github.com/rowbench/ramjet-ingress/blob/main/bench/engine/RESULTS.md) | Does the io_uring engine get under the syscall floor? |
| [`bench/PROFILE.md`](https://github.com/rowbench/ramjet-ingress/blob/main/bench/PROFILE.md) | Where does a request actually go? |

## Read this first

**These are macOS Docker Desktop VM numbers**, on Apple Silicon under a linuxkit
guest. They are valid *relative to each other* under identical conditions; they
are not Linux bare-metal absolutes and should not be quoted as such.

**The competitor is not understated on purpose.** ingress-nginx does **not**
reload for every change — endpoint updates go through its Lua balancer without
touching nginx at all — and a report that measured only the changes which force
a reload would be describing a system that does not exist. The endpoint-churn
arm is measured and reported alongside the rest.

**The losses are on this page.** Idle-connection memory, the `kubectl apply`
write path, upstream connection reuse, and an unexplained 9% gap at high
concurrency all belong to the other side.

## The thesis: what a configuration change costs

Both controllers installed into the same Docker Desktop cluster at the same
time, separate namespaces, separate IngressClasses, one replica each. Load is
never sent to both at once. ingress-nginx is the **more generously provisioned**
of the two: it runs with no memory ceiling while ramjet-ingress runs under the
256Mi cap its own chart imposes.

Load reaches the pods through a NodePort on the node's own bridge address —
identically for both. `kubectl port-forward` was tried and rejected, because a
port-forward is one multiplexed stream through a Go proxy on the host and
becomes the bottleneck long before either contender does.

Two kinds of churn are measured, every two seconds for 100 seconds:

- **Ingress-spec churn** adds a differently-named path to a churn Ingress, which
  forces a reload. Verified from ingress-nginx's own log, not assumed.
- **Endpoint-only churn** moves one running pod in and out of a Service's
  selector, which goes through the Lua balancer without reloading. The mutation
  flips a label on a running pod rather than scaling, because scaling would have
  measured the scheduler's latency and reported it as the controller's.

### Throughput and latency under churn (oha, c64, 2 runs per cell)

| Contender | Arm | RPS (median) | vs own baseline | p50 | p99 | p99.9 | HTTP errors |
|---|---|---:|---:|---:|---:|---:|---:|
| ramjet | baseline | 104,844 | — | 0.35 ms | 3.8 ms | 12.5 ms | 0 |
| ramjet | spec | 107,536 | +2.6% | 0.35 ms | 3.6 ms | 10.3 ms | 0 |
| ramjet | endpoint | 103,298 | -1.5% | 0.35 ms | 3.8 ms | 12.7 ms | 0 |
| nginx | baseline | 87,865 | — | 0.43 ms | 4.6 ms | 13.0 ms | 0 |
| nginx | spec | 78,368 | -10.8% | 0.45 ms | 5.4 ms | 18.3 ms | 1722 |
| nginx | endpoint | 64,305 | -26.8% | 0.57 ms | 6.7 ms | 21.1 ms | 0 |

### Idle keep-alive connections that survived the window

| Contender | Arm | Held | Survived | Lost | Config events applied |
|---|---|---:|---:|---:|---:|
| ramjet | baseline | 100 | 100 | 0 | 0 |
| ramjet | spec | 100 | **100** | 0 | 98 |
| ramjet | endpoint | 100 | 100 | 0 | 98 |
| nginx | baseline | 100 | 100 | 0 | 0 |
| nginx | spec | 100 | **0** | 100 | 66 |
| nginx | endpoint | 100 | 100 | 0 | 0 |

> **This is the cleanest result in the whole report, because it does not depend
> on how fast the machine was.** Under spec churn ingress-nginx ended every
> single run with 0 of 50 idle connections surviving — 0/100 across both rounds,
> and 0/50 again in each of the two contended replicate rounds. ramjet-ingress
> kept 100 of 100.

### Controller cost of the churn window

| Contender | Arm | Pod CPU-seconds | CPU per request | vs own baseline | Pod memory at end |
|---|---|---:|---:|---:|---:|
| ramjet | baseline | 300.9 s | 28.7 µs | — | 17.9 MiB |
| ramjet | spec | 310.1 s | 28.8 µs | **+0%** | 18.3 MiB |
| ramjet | endpoint | 302.3 s | 29.3 µs | **+2%** | 16.5 MiB |
| nginx | baseline | 402.8 s | 45.8 µs | — | 128.1 MiB |
| nginx | spec | 395.5 s | 50.5 µs | **+10%** | 115.7 MiB |
| nginx | endpoint | 367.7 s | 57.2 µs | **+25%** | 127.8 MiB |

Recompiling and republishing a route table 49 times in 110 seconds cost the data
plane nothing this benchmark can find. The CPU column is the version of the
claim that survives, because it normalises out how fast the machine was.

**Where ingress-nginx is fine: endpoint churn.** It did not reload once — 0
reloads across every endpoint-churn run, confirmed from its own log — and it
kept all 50 idle connections and served zero errors. Its Lua balancer does what
it claims, and any characterisation of ingress-nginx as "reloads on every
change" is wrong.

**But its non-reloading path is the more expensive one for CPU**: +25% against
its own baseline, where the reloading path cost +10%.

### What benchmark 1 does not establish

> The **-26.8% endpoint-churn throughput figure is not solid**. Arms always ran
> in the order baseline, spec, endpoint within a round, so the endpoint arm was
> always last and always held whatever drift the machine had accumulated.
> Rounds 3 and 4 were run specifically to test that, with the arm order reversed
> — and they were invalidated by the docker daemon: another agent started their
> own six-container proxy benchmark partway through, and throughput for both
> contenders fell to roughly a quarter, varying between 27k and 96k rps *within
> a single round*.

So the ordering confound on the throughput number is **unresolved**. The
CPU-per-request figure is the version that survives; the connection-survival and
reload counts reproduced identically in all four rounds regardless of
contention.

The contended rounds are kept in the repository rather than discarded, with
their own warning label: they report ramjet-ingress losing 62% of its throughput
to spec churn, from a contender whose CPU per request did not move at all under
the same churn on a quiet machine. **That number is the other agent's benchmark,
not this one's.**

## Propagation latency

`kubectl apply` to the first request the data plane answers correctly, polled
every 20 ms, no other load running. Ten trials of each shape per contender,
interleaved with the order flipping every trial. `kubectl` runs *inside the same
container as the poller*, so "applied" and "served" are two readings of one
clock.

| Contender | Change | Trials | Median | p95 | Min | Max | Median `kubectl apply` |
|---|---|---:|---:|---:|---:|---:|---:|
| ramjet | new Ingress | 10 | **363 ms** | 566 | 324 | 566 | 159 |
| ramjet | backend swap | 10 | **354 ms** | 556 | 322 | 556 | 150 |
| nginx | new Ingress | 10 | 1,151 ms | 3,638 | 302 | 3,638 | 138 |
| nginx | backend swap | 10 | 459 ms | 3,657 | 384 | 3,657 | 188 |

~3x faster at the median and ~6x at p95, and the more useful half of that is the
spread: ten new-Ingress trials from 324 to 566 ms, against 302 to 3,638 ms.

ingress-nginx's slow trials cluster just under 3.5 seconds and alternate with
fast ones. That shape is a rate limiter, not a queue, and the controller names
the number itself: `--sync-rate-limit` defaults to 0.3, one sync per 3.33
seconds. **Raising that flag would shorten this tail — it is a default, not a
limit of the design** — but the default is what a cluster gets. ramjet-ingress
has a fixed 200 ms debounce and no rate limit.

### A loss: the write path

> **The admission webhook is not the reason, and this is where ingress-nginx
> ties or wins.** Median `kubectl apply` was 138 ms for ingress-nginx against
> 159 ms for ramjet-ingress — the write path including `nginx -t` validation of
> the whole generated configuration is *faster* than ramjet-ingress's plain
> unvalidated write.

## 500 routes

500 Ingresses with distinct hosts, applied as one batch, one contender at a
time.

| Contender | Created | `kubectl apply` wall time | Apply → last route served | Controller CPU | Controller memory before → after | nginx reloads |
|---|---:|---:|---:|---:|---:|---:|
| ramjet | 500/500 | 10.7 s | **10.9 s** | 1 s | 21.1 → 20.8 MiB | — |
| nginx | 500/500 | 58.5 s | 61.7 s | 67 s | 115.8 → 214.0 MiB | 19 |

**Both reached 500. Neither choked.** That is worth saying first, because the
brief allowed for reporting the number at which one of them fell over.

- **5.7x faster convergence**, of which most of ingress-nginx's time is the
  write path: roughly 117 ms per Ingress against 21 ms. The admission webhook
  that was free at one Ingress is not free at 500.
- **Controller CPU differs by a factor of 100**: 0.66 CPU-seconds against 66.7.
- **Memory is the sharper result.** 21.1 → 20.8 MiB: 500 compiled routes are,
  within measurement noise, free. ingress-nginx grew 98 MiB, roughly 200 KiB per
  route. Under the 256Mi limit ramjet-ingress's own chart ships, ingress-nginx
  would have been within 40 MiB of being OOM-killed at 500 routes; it survives
  because its chart ships no limit at all.

Propagation with the routes loaded:

| Contender | Trials | Median | p95 | Median on an empty cluster |
|---|---:|---:|---:|---:|
| ramjet | 5 | **507 ms** | 723 ms | 363 ms (1.4x) |
| nginx | 5 | 5,006 ms | 5,964 ms | 1,151 ms (4.4x) |

Every ingress-nginx trial at scale was slower than its own worst trial on an
empty cluster.

> **The throughput row of this benchmark is the weakest number in the whole
> document** and is deliberately not reproduced here. 47,535 rps against 16,109
> is a 3x gap, but the two measurements were taken minutes apart at 38% and 25%
> VM CPU idle respectively, on a machine that was doing someone else's work. It
> is one run each under unequal conditions and should not be quoted as a
> throughput result.

**Deleting 500 Ingresses took about 105 seconds for each.** A tie, and API-server
bound rather than controller bound.

## Idle-connection memory: the loss

**ingress-nginx wins this one decisively, and it is the most important negative
result in the report.**

10,000 idle keep-alive connections, no Kubernetes, both proxies on the same
docker bridge with the same upstream and nginx's own tuning. Two passes, order
reversed between them.

### Originally

| Contender | Pass | Idle before | At 10k | After close | Per connection | Retained |
|---|---|---:|---:|---:|---:|---:|
| ramjet | 1 | 1.5 MiB | 266.1 MiB | 229.6 MiB | 27.1 KiB | +228.1 MiB |
| ramjet | 2 | 229.6 MiB | 329.0 MiB | 292.3 MiB | 10.2 KiB | +62.7 MiB |
| nginx | 1 | 16.3 MiB | 58.9 MiB | 16.5 MiB | 4.4 KiB | +0.2 MiB |
| nginx | 2 | 16.5 MiB | 58.8 MiB | 16.5 MiB | 4.3 KiB | +0.0 MiB |

An idle connection cost nginx 4.4 KiB and ramjet-ingress 27.1 KiB — 6x — and
ramjet-ingress **did not give the memory back**, growing monotonically across
connect/disconnect cycles. At 266 MiB peak it would have been OOM-killed by the
256Mi limit its own Helm chart ships, with no traffic flowing.

### After the fix

| Contender | Pass | Idle before | At 10k | After close | Per connection | Retained |
|---|---|---:|---:|---:|---:|---:|
| ramjet | 1 | 2.5 MiB | **200.7 MiB** | **11.5 MiB** | 20.3 KiB | +9.0 MiB |
| ramjet | 2 | 11.5 MiB | 201.5 MiB | 11.5 MiB | 19.5 KiB | +0.0 MiB |
| nginx | 1 | 16.3 MiB | 58.8 MiB | 16.5 MiB | 4.4 KiB | +0.2 MiB |
| nginx | 2 | 16.5 MiB | 58.8 MiB | 16.5 MiB | 4.3 KiB | +0.0 MiB |

**The retention problem is gone**: a second full cycle peaked at 201.5 and
settled at 11.5 again — the same number, not a higher one. ramjet-ingress now
also idles *lower* than nginx does, 11.5 MiB against 16.5.

> **The per-connection cost improved by a quarter and ingress-nginx still wins
> it.** 27.1 KiB to 20.3 is real, and 20.3 against 4.4 is still **4.6x**. The
> gap is structural.

The original table is left in the repository exactly as it was — a benchmark
that overwrites the evidence it was judged against cannot be checked afterwards.

### Where the remaining 20.3 KiB goes

Measured at 2,000 connections:

| What the connection has done | Per connection, cgroup | Per connection, VmRSS |
|---|---:|---:|
| Accepted, never sent a byte | 6.1 KiB | 1.7 KiB |
| One request, answered by the proxy itself | 20.1 KiB | 16.2 KiB |
| One request, forwarded to the upstream | 20.8 KiB | 16.9 KiB |

The ~4.4 KiB gap between the columns on a merely-accepted connection is kernel
socket memory, which cgroup v2 charges to the container. That is very nearly
nginx's *entire* per-connection cost, which is the sharpest way to state the
difference: **nginx's 4.4 KiB is, to a first approximation, the socket and
nothing else.** It hands a connection's request buffers back to its pool when
the connection goes idle and keeps only the connection object. There is no
equivalent in hyper.

Two checks that pin it down: sending 6 KiB of request headers instead of 90
bytes moved the figure by **two bytes** (16,927 against 16,929) — the read
buffer is resident whether or not anything is read into it. And patching hyper's
`INIT_BUFFER_SIZE` from 8192 down to 1024 gave **11.3 KiB cgroup and 7.3 KiB
RSS**; two 8 KiB buffers becoming two 1 KiB ones accounts for 9.6 KiB, and
nothing else moved.

> There is no public API that lowers it: `max_buf_size` caps how far the read
> buffer may *grow*, and hyper refuses to set it below `INIT_BUFFER_SIZE`. So
> **16 KiB per idle keep-alive connection is this engine's floor** until hyper's
> initial allocation follows its configured maximum instead of a constant. The
> patched measurement is what that change would be worth: roughly 2.5x nginx
> instead of 4.6x. It is a one-line change in a dependency, and the right place
> to make it is upstream.

The experimental `uring` engine was measured on the same harness and is **not**
cheaper: 23.2 KiB per connection, because it allocates per-connection buffers of
its own.

### What this means for the chart

`resources.limits.memory: 256Mi` stays, and the values file now carries the
arithmetic instead of leaving it to be rediscovered — about 20 KiB per idle
keep-alive connection, so 256Mi is roughly twelve thousand of them. Raising the
default to make room for a per-connection cost that is still 4.6x nginx's would
have hidden the finding rather than fixed it.

## Raw forwarding throughput vs nginx

A forwarding-engine drag race: one route, one host, 128-byte plaintext
responses, static configuration. Both proxies pinned to the **same two cores**
with `--cpuset-cpus=0,1` (a CPU *quota* is invisible to `sched_getaffinity`, so
pinning is what makes both see 2 CPUs and start 2 workers), oha 1.16.0 at
HTTP/1.1 keep-alive, a discarded 10s warmup, then 3 × 30s at c64 and 1 × 30s at
c256, interleaved. The c64 rows are the **median-throughput run**, not a
per-column average, so every number in a row comes from one real 30-second
measurement.

### Concurrency 64 (median of 3 × 30s runs)

| Contender | RPS | p50 | p90 | p99 | p99.9 |
|---|---:|---:|---:|---:|---:|
| ramjet-ingress | 85,908 | 666 µs | 921 µs | 2,528 µs | 6,236 µs |
| nginx | 86,670 | 671 µs | 873 µs | 2,314 µs | 5,902 µs |
| baseline (no proxy) | 229,400 | 223 µs | 356 µs | 1,219 µs | 4,421 µs |

### Concurrency 256 (single 30s run)

| Contender | RPS | p50 | p90 | p99 | p99.9 |
|---|---:|---:|---:|---:|---:|
| ramjet-ingress | 82,524 | 2,975 µs | 3,617 µs | 6,396 µs | 14,185 µs |
| nginx | 89,636 | 2,652 µs | 3,559 µs | 7,683 µs | 17,180 µs |
| baseline (no proxy) | 247,077 | 918 µs | 1,233 µs | 3,554 µs | 9,111 µs |

**At c64 the two are level.** 85,908 against 86,670 is a 0.9% difference, and
nginx's own three runs spread 4.5% — the gap is smaller than the noise in the
measurement, which means this benchmark can no longer tell them apart at this
concurrency. **It does not mean ramjet-ingress is faster; the honest statement
is "the same".** Divide two cores by throughput and both spend 23.6 µs of CPU
per request.

**At c256 nginx is still 9% ahead**, and that gap is outside the noise. nginx's
throughput barely moves between c64 and c256 (+3%) while ramjet's drops 4%.
Latency runs the other way — ramjet's p99 at c256 is 6,396 µs against nginx's
7,683 µs — so what this looks like is ramjet trading a little throughput for
shorter queues under saturation, not falling over.

### Where it started

The first measurement of the same benchmark had nginx **45% ahead**, and it is
kept in the repository unchanged. What closed it:

| Measure | Before | After | Change |
|---|---:|---:|---:|
| c64 throughput | 61,568 | 85,908 | **+39.5%** |
| c256 throughput | 59,644 | 82,524 | **+38.4%** |
| c64 p50 | 967 µs | 666 µs | -31% |
| c64 p99 | 3,107 µs | 2,528 µs | -19% |
| CPU per request | 32.5 µs | **23.6 µs** | -27% |
| vs nginx at c64 | 69% of it | **99% of it** | — |
| vs nginx at c256 | 67% of it | **92% of it** | — |
| requests per upstream connection | ~590 | **8,179** | 14x |
| memory under load | 19.2 MiB | 33.1 MiB | **+72%** |

That last row is a real cost and is reported as one: one runtime per core means
one connection pool, one timer wheel and one set of hyper buffers per core
rather than per process. On a 2-core replica that is 14 MiB; on a 64-core node
with no CPU limit it would be considerably more, which is an argument for
setting `--worker-threads` deliberately rather than letting it follow the host.
(The later memory work brought this to 24.9 MiB.)

Zero errors across all twelve runs, both before and after: 49,114,324 requests,
every one a 200, no transport errors from any contender at either concurrency.

### Method honesty on this benchmark

> **Both head-to-head tables were taken with a fixed within-round order.**
> `run.sh` has since been changed to *rotate* which contender leads each round,
> and to wait a 15s cooldown before each warmup — because plain interleaving
> assumes the machine is steady within a round, and on a laptop it is not: the
> package heats up as the round proceeds, so a fixed order hands whoever goes
> first a systematically cooler machine in every round. Neither table has been
> re-measured under the rotated protocol, **so read the numbers as carrying that
> bias in ramjet's favour at c64**, bounded by the within-round drift (the
> baseline's 13.3% spread is the visible upper bound; the contenders' 1.7–5.3%
> the likelier scale).

Other stated unfairness:

- **The upstream keepalive pools were not equal in the first measurement.**
  nginx held 128 idle upstream connections, ramjet 64 — the edge was nginx's,
  and it was left alone rather than patched, **because changing the product to
  win its own benchmark is not a measurement.**
- **nginx tuning choices were tested, not assumed.** `reuseport` was measured
  both ways and kept because it is better for nginx. `access_log off` removes
  nginx's default per-request write, which ramjet does not have. `proxy_cache`
  was deliberately **not** enabled: ramjet has no response cache, and serving
  from nginx's memory would compare two different jobs.
- **Both are round-robin**, matching nginx's default, rather than ramjet's
  `leastConn`.
- **A shared docker daemon**, and it bit. The reported 30s runs absorb it, and
  the contender spread is the evidence.

### What this does not test

> This is a forwarding-engine drag race on the narrowest possible workload: one
> route, one host, 128-byte responses, plaintext HTTP/1.1, static configuration.
> It says nothing about the project's actual thesis — that a config change is a
> pointer swap rather than an nginx reload. Nothing here exercises TLS
> termination, HTTP/2, large or streaming bodies, thousands of routes, or
> configuration churn under live traffic.

That paragraph predates the optimization and still stands unchanged. It is the
more important one on the page.

## Where a request actually goes

Profiling asked where the 10 µs gap lived, and **the answer was not in the
forwarding code.** Route matching, header rewriting, URI building and the
metrics counters together account for about **2%** of a request.

| Own-code function | Inclusive CPU |
|---|---:|
| `upstream::endpoint_uri` (builds and parses a URI per request) | 0.80% |
| `headers::apply_forwarded` (`X-Forwarded-*`) | 0.27% |
| `headers::strip_hop_by_hop` | 0.20% |
| `headers::upgrade_protocol` | 0.14% |
| `forward::select_backend` (the router match itself) | 0.13% |

The router's 25 ns match is 0.1% of that. **There was no hot function to find.**

What the profile found instead was the runtime moving each request's work
between cores:

| Workers | Throughput | Proxy CPU | CPU per request |
|---:|---:|---:|---:|
| 1 | 47.1k rps | 88% | **18.7 µs** |
| 2 | 68.4k rps | 183% | **26.7 µs** |

**The same code costs 43% more CPU per request on two threads than on one**,
held at 33–43% across three interleaved rounds. That is the shape of a
work-stealing scheduler under a request that ping-pongs between workers; nginx
does not pay it, because its workers are shared-nothing processes.

So the data plane became one `current_thread` runtime per core with nothing
shared between them. Everything else that was tried was **inside the noise** and
is recorded as such:

| Tried | Result | Kept? |
|---|---|---|
| Removing the per-request header clone | +0.9% | No — the clone buys endpoint failover for less than the noise floor |
| Flattened writes instead of vectored | -0.2% | No — the syscall dominates; iovec handling is free either way |
| Raising tokio's `event_interval` from 61 to 512 | -1.3% | No |
| Per-core sharding / cache-line padding of the metrics counters | -0.9% | No, **and this is the useful negative**: there is nothing to win, so the sharding was never written |

### The floor

After the change the profile reads:

| Cost | Self CPU |
|---|---:|
| `writev` | 31.2% |
| `read` | 28.2% |
| `kevent` | 9.1% |
| `clock_gettime` | 2.2% |
| everything in `ramjet_proxy` | ~1% |

> **59.4% of a request is the four unavoidable syscalls, and another 9.1% is
> finding out a socket is ready.** That is the floor for this design, and it is
> not a hyper problem or a tokio problem — it is the I/O model.

Getting under it means fewer syscalls per request, which on Linux means
`io_uring`.

**The remaining 9% gap at c256 has not been profiled.** Every measurement was
taken at c64, and the native harness cannot hold c256 steady enough to be worth
reading. Whether it is queueing, the per-runtime pool split, or something else
is an open question.

## The uring engine

A second data plane on a completion-based reactor, selected with
`--engine uring`. Docker on **Linux**, `--cpuset-cpus=0,1`, the same pair of
upstreams, oha at c64, three rotated rounds of 30 seconds each. **Both ramjet
rows are the same image with one flag different.**

### Concurrency 64 (median of 3 runs)

| Contender | RPS | % of baseline | p50 | p90 | p99 | p99.9 |
|---|---:|---:|---:|---:|---:|---:|
| ramjet (hyper) | 80,682 | 35.1% | 687 µs | 1,030 µs | 2,905 µs | 7,562 µs |
| **ramjet (uring)** | **116,927** | **50.9%** | **483 µs** | **702 µs** | **1,941 µs** | **5,569 µs** |
| nginx | 80,790 | 35.1% | 696 µs | 1,002 µs | 2,853 µs | 7,687 µs |
| baseline (no proxy) | 229,902 | 100.0% | 227 µs | 384 µs | 1,160 µs | 3,868 µs |

### Concurrency 256 (single run)

| Contender | RPS | % of baseline | p50 | p90 | p99 | p99.9 |
|---|---:|---:|---:|---:|---:|---:|
| ramjet (hyper) | 65,458 | 28.2% | 3,130 µs | 5,217 µs | 15,918 µs | 52,723 µs |
| **ramjet (uring)** | **110,057** | **47.5%** | **1,962 µs** | **3,070 µs** | **8,122 µs** | **28,387 µs** |
| nginx | 84,480 | 36.4% | 2,680 µs | 3,936 µs | 8,465 µs | 22,245 µs |
| baseline (no proxy) | 231,837 | 100.0% | 924 µs | 1,552 µs | 3,866 µs | 10,629 µs |

**+44.7% over nginx at the median**, and the proxy hop costs 255 µs where
nginx's costs 469 µs — it keeps 51% of the no-proxy throughput where the other
two keep 35%.

### The claim that survives the drift

The machine would not sit still: the baseline, which has no moving parts and
nothing under test, spread 15.1% across three rounds. Drift makes a **median**
shaky. It does not touch a **rank-order** claim:

| Comparison | worst uring round | best rival round | verdict |
|---|---:|---:|---|
| uring vs ramjet (hyper) | 111,250 | 85,640 | uring ahead by 30% at worst |
| uring vs nginx | 111,250 | 84,862 | uring ahead by 31% at worst |

Every measured uring round beat every measured round of both rivals; the ranges
do not overlap. **"At least 31% ahead of nginx" is the claim that survives**,
and +44.7% is the median's reading of the same thing. `report.py` makes this
check itself and refuses the run if the ranges ever overlap.

The hyper row is not under-measured **relative to nginx**: here it is 0.13% from
nginx, so +44.9% for uring over hyper is the same result as +44.7% over nginx
rather than an artifact of a cold hyper.

### And a cross-day check that cuts the other way

Comparing this session against the committed head-to-head runs — taking **both
cells from the same run**, which an earlier version of the engine document
failed to do — puts nginx's row here on the low side:

| | engine session | 4f58bd7, after optimization | d1c08c6, first measurement |
|---|---:|---:|---:|
| nginx, absolute | 80,790 | 86,670 | 89,593 |
| baseline, absolute | 229,902 | 229,400 | 247,875 |
| **nginx as % of baseline** | **35.1%** | **37.8%** | **36.1%** |

The ratio travels across days at the **few-percent level** — 2.6 points against
the nearer comparator, 1.0 against the older one — which is enough to trust this
session's ordering, and not enough to swap an absolute row for another day's.

**The slowdown was not uniform**, and that is the part worth carrying: this
session's baseline is within **0.2%** of 4f58bd7's, while its nginx is 6.8% lower
and its hyper engine 6.1% lower. Whatever cost the two TCP proxies those points
did not cost the no-proxy baseline anything.

So **+44.7% is the optimistic end of the margin rather than the middle of it.**
Against the best committed nginx median, uring's worst round is **+28%**; against
this session's own best nginx round, +31%. At least **28% ahead** is the figure
that survives every pairing, and +44.7% is what you get comparing contenders
measured in the same session on the same host.

### Why: the syscall counters

```text
cqes_per_waiting_enter    = 21.7 … 39.5   (typically 28–37)
enter_share_of_thread_cpu = 0.81
```

Between 22 and 40 completions are harvested per trip into the kernel. A request
is four operations, so that is roughly **seven to ten requests per syscall**,
against the hyper engine's four syscalls per request plus a `kevent` to learn a
socket was ready. The 81% is the share of the serving thread's CPU spent
*inside* `io_uring_enter`, which is where the kernel actually does the reads and
writes — **it is not overhead, it is the work.**

### Cost per request

| | requests | CPU | CPU per request | memory | reqs per upstream conn |
|---|---:|---:|---:|---:|---:|
| ramjet (hyper) | 855,179 | 199.1% | 27.9 µs | 23.5 MiB | 6,681 |
| **ramjet (uring)** | **1,273,310** | **198.1%** | **18.7 µs** | **10.8 MiB** | 9,947 |
| nginx | 986,645 | 174.4% | 21.2 µs | 3.9 MiB | 61,665 |

49% more requests for the same CPU as the hyper engine, and less than half its
memory. Two honest readings alongside that:

- **nginx did not saturate its cores** in this pass (174% of an available 200%),
  so its 21.2 µs is a fair figure for what it spent but its throughput here may
  have been limited by something other than CPU.
- **A loss: nginx reuses upstream connections six times better** — 61,665
  requests per connection against 9,947. Per-core pools are the reason, and the
  price was named when they were introduced: a connection returned to a full
  pool on one core cannot be reused by another. It is not costing throughput
  here, but it is a real difference and it is nginx's win. nginx is also, by a
  wide margin, the most memory-frugal of the three.

### The macOS negative result

The same binary with the flag flipped, on the native macOS harness:

```text
  A (hyper)  median 50,738 rps   spread 30.8%
  B (uring)  median 53,024 rps   spread 18.1%
  B vs A: +4.5%   (inside the noise)
```

**+4.5% against an 18–31% spread is not a result, and the harness says so
itself.** That is the prediction, not a disappointment: on macOS the reactor's
backend is kqueue, which performs each syscall eagerly at submission. There is
no ring, no batch, and nothing to collapse. The whole benefit measured on Linux
is io_uring's, so the platform without io_uring measures none of it.

### Caveats on the uring numbers

> **This is a macOS Docker Desktop VM, and that matters more here than for any
> other benchmark in the repository.** The whole result is about the cost of
> entering the kernel, and a syscall in a virtualised linuxkit guest is dearer
> than one on bare metal. io_uring's advantage *is* the cost it avoids, so a
> more expensive syscall flatters it. **Treat the margin as an upper bound** and
> the direction, not the size, as the transferable claim.

- **nginx runs under Docker's stock seccomp profile; both ramjet containers run
  under a pinned one** (moby v24.0.7's default plus the three io_uring
  syscalls). It is a superset of what nginx needs so it cannot disadvantage
  nginx, but it is a difference between contenders.
- **The two engines were not feature-equivalent when this was measured.** The
  uring engine served HTTP/1.1 plaintext and nothing else. None of the missing
  features is exercised by this workload, so the comparison is like for like
  *for this traffic*. It was **not** a claim that the engines are
  interchangeable. Most of that gap has since closed — see [TLS, and a
  tunnel](#tls-and-a-tunnel) below and [Engines](./operations/engines.md).
- **c256 is a single run**, not a median, and is reported as such.
- The correctness gate compares the two engines' response headers field by
  field, so **neither can be fast by doing less.**

### On real Linux

The caveat above says to treat the margin as an upper bound and the direction,
not the size, as what transfers. This is the check on that, and **it comes out
exactly as the caveat predicted.** A `t3.xlarge` EC2 instance running k0s — real
Linux, no VM in the path — with the same two engines and the same flag
difference:

| | ramjet (hyper) | ramjet (uring) | uring vs hyper |
|---|---:|---:|---:|
| RPS, median | 10,610 | **11,221** | **+5.8%** |
| spread across runs | 4.1% | **2.1%** | |
| % of baseline | 47.4% | **50.2%** | |
| p50 | 4.59 ms | **3.76 ms** | **−18.1%** |

Baseline with no proxy in the path was 22,363 rps.

**+5.8%, against +44.9% in the VM.** The direction transferred and the size did
not, which is the whole of what the caveat asked to be believed.

The share-of-baseline column is where the mechanism shows:

| % of baseline | Docker Desktop VM | t3.xlarge, k0s |
|---|---:|---:|
| ramjet (uring) | 50.9% | 50.2% |
| ramjet (hyper) | 35.1% | 47.4% |

**uring held its share almost exactly; hyper gained twelve points.** So the VM
was not flattering the reactor so much as it was punishing the other engine. The
hyper engine pays four syscalls per request plus a `kevent`, a virtualised
syscall is dearer than a native one, and taking the VM away refunds most of that
to the engine making the most calls. The reactor, which was already avoiding
those calls, had little to be refunded.

Per busy CPU the gap is smaller still, roughly **+2.4%**, where busy is
`100 − idle`:

| | busy | RPS | RPS per busy point |
|---|---:|---:|---:|
| ramjet (hyper) | 91 | 10,610 | 116.6 |
| **ramjet (uring)** | **94** | **11,221** | **119.4** |

The `us`/`sy` split underneath is uring 35/47 against hyper 41/44 — the reactor
spending more of the machine in the kernel and less in userspace, which is the
same shape as the `io_uring_enter` share above and means the same thing: for
this engine the kernel time *is* the work. Those two do not sum to busy; the
remainder is `wa`, `st` and rounding, which is why the busy column is taken from
idle rather than by adding them up.

**Every figure in that table counts the load generator and the upstreams as well
as the proxy**, because all three were on the one box. So +2.4% is a
whole-machine efficiency number rather than the engine's own. Getting the
engine's own needs proxy-only CPU seconds, which this run did not capture — the
same reason the run is not the one to quote from at all:

> **The caveat that matters more than any of the numbers: this run was
> CPU-contended, and the contention structurally compresses the gap between the
> engines.** Four shared vCPUs, with the load generator on the same instance as
> the proxy and the upstreams. The proxy was therefore never the sole
> bottleneck, and a benchmark where the thing under test is not the limiting
> factor understates every difference between two versions of it. **A rerun on
> pinned, isolated cores with the load off-box is the number worth quoting, and
> this is not that run.** Treat +5.8% as a floor under contention rather than
> the engine's ceiling.

### The p99 inversion, unexplained

On the same real-Linux run the reactor **wins throughput and the median and
loses the tail**: p99 is about **6.8% worse** than the hyper engine's, and it
was worse at both c64 and c256. Consistent enough not to be noise, and not
currently explained.

It is not the shape the VM measurements had, where uring led at every percentile
including p99.9. Until someone can say why, **no tail-latency claim is made for
the reactor** — see [Limitations](./limitations.md#the-uring-engines-p99-is-an-open-question).

### What is not claimed

- Not that io_uring beats epoll in general. This is one proxy workload, one
  kernel, one VM.
- Not that the uring engine is ready to deploy. **At the time of this
  measurement** it had no TLS, no HTTP/2, no upgrades, no Kubernetes mode and no
  graceful drain. That list is now down to HTTP/2, which is served by dispatch.
- Not that the hyper engine is badly written. Profiling took it to the syscall
  floor, and this is what is underneath that floor. The difference is the I/O
  model, which is what was being tested.

## TLS, and a tunnel

The measurements above are plaintext HTTP/1.1, which is what the uring engine
could serve when they were taken. It terminates TLS now. Same machine, same
topology, same pinning, same rules — one certificate added.

Both sides resume sessions, which is the setting that decides a TLS benchmark:
nginx ships `ssl_session_tickets on` and every deployment turns on
`ssl_session_cache`, so a run against a ramjet with resumption off would be
measuring a configuration nobody deploys. ECDSA P-256, one certificate generated
per run and mounted into all three containers, HTTP/1.1 on all three.

**Keep-alive**, 30s per run, three rounds, median by throughput:

| c=64 | rps | p50 | p99 |
|---|---:|---:|---:|
| ramjet (hyper) | 82,735 | 0.68 ms | 2.48 ms |
| **ramjet (uring)** | **107,920** | **0.51 ms** | 2.38 ms |
| nginx | 76,531 | 0.76 ms | 2.40 ms |

| c=256 | rps | p50 | p99 |
|---|---:|---:|---:|
| ramjet (hyper) | 81,265 | 2.96 ms | 6.24 ms |
| **ramjet (uring)** | **104,188** | **2.14 ms** | 7.02 ms |
| nginx | 68,758 | 3.38 ms | 8.92 ms |

**A new connection per request**, so every request pays for a handshake — which
is what a rolling deployment or a reconnecting CDN does to a replica:

| | conn/s | p50 | p99 |
|---|---:|---:|---:|
| ramjet (hyper) | 12,412 | 5.09 ms | 12.62 ms |
| **ramjet (uring)** | **14,436** | **4.25 ms** | **11.25 ms** |
| nginx | 5,736 | 10.68 ms | 25.86 ms |

The margin over hyper survives TLS almost intact: 1.30x at c=64, 1.28x at c=256,
against 1.30x on the plaintext run. Crypto is added work, but it is added to
both ramjet contenders equally, and what separates them is still how the bytes
reach the record layer rather than what happens inside it.

The handshake row narrows to 1.16x between the engines, and it should — a
handshake is arithmetic, not syscalls. The 2.52x over nginx there is *ring*
against OpenSSL as much as it is one proxy against another, and should be read
as a statement about two TLS stacks.

### WebSocket tunnels are level, and that is the expected result

64 tunnels, 128-byte payloads, one echo in flight per connection:

| | echo/s | p50 | p99 |
|---|---:|---:|---:|
| ramjet (hyper) | 103,422 | 595 µs | 1,509 µs |
| ramjet (uring) | 105,271 | 585 µs | **1,374 µs** |
| nginx | 102,843 | **571 µs** | 1,619 µs |

All three within 2.4%. A reader who saw 1.30x on the TLS table would expect a
gap here, and there is none, for a reason worth stating: after a 101 there is no
request, no routing and no header rewriting — one read and one write per echo on
each side, with nothing to batch and nothing to overlap. The rate is bounded by
round-trip latency rather than by how many times the kernel is entered, and
submission batching is exactly the advantage that has nothing to work on.

What does separate them is steadiness: both ramjet engines hold a 1.2–1.4%
spread across runs against nginx's 7.1%, and the uring engine has the best p99.

Full protocol, raw JSON and the fairness notes are in
[`bench/engine/RESULTS.md`](https://github.com/rowbench/ramjet-ingress/blob/main/bench/engine/RESULTS.md).

## Route matching

Not a system benchmark — a microbenchmark of the matcher against a table of
**1,000 hosts and 10,001 routes**, on an Apple M2 Pro, criterion, 100 samples.

| Case | Time | What it costs |
|---|---:|---|
| `deep_prefix_hit` | **25.2 ns** | exact host, four-segment prefix — the normal request |
| `exact_hit` | 22.5 ns | exact rules sort first, so this is the cheapest hit |
| `host_miss_default_backend` | 20.6 ns | two failed hashes, then the default backend |
| `wildcard_hit` | 29.7 ns | a failed exact hash plus a parent-domain hash |
| `uppercase_host_fold` | 31.8 ns | the only path that copies, into a stack buffer |
| `regex_hit` | 42.8 ns | full scan past every prefix, then a regex |
| `root_prefix_hit` | 47.3 ns | worst case: scans every prefix rule before matching `/` |

For scale, a single uncached main-memory reference is roughly 80 ns — matching a
route costs less than one cache miss.

> These are laptop numbers taken on a machine that was not otherwise idle, so
> treat them as an order of magnitude rather than a regression baseline. What
> the benchmark is really for is the **shape**: matching does not get slower
> with table size, because host selection is a hash and a host carries a handful
> of rules.

`match_request` performs **no heap allocation**, and that is enforced rather
than asserted in a comment: `tests/no_alloc.rs` installs a counting global
allocator and checks every path through the matcher, including the mixed-case
fold, canary resolution, and SNI lookup.

## Where ingress-nginx won or tied

| | Result |
|---|---|
| **Idle-connection memory** | **Won, heavily.** 4.4 KiB/connection against 27.1, and it returns all of it on close while ramjet-ingress retained and grew. Still won after the fix, by less: 4.4 against 20.3, and both now return what they took |
| **`kubectl apply` write path (single Ingress)** | **Won.** 138 ms median against 159, *including* `nginx -t` validation through an admission webhook that ramjet-ingress does not have |
| **Endpoint-only churn: connection safety** | **Tied.** 50/50 idle connections survived, zero errors, zero reloads. Its Lua balancer does exactly what it claims and the reload argument does not apply to endpoint changes |
| **Deleting 500 Ingresses** | **Tied.** ~105 s each; the API server is the bottleneck, not either controller |
| **Reaching 500 routes at all** | **Tied.** Both converged; neither fell over |
| **Stall severity** | **Tied-ish.** Neither contender produced a stall over one second attributable to churn. ingress-nginx's reload is visible in the tail, but it is tens of milliseconds, not seconds |

Plus, from the raw-forwarding and engine benchmarks: **nginx is still 9% ahead
at c256**, **reuses upstream connections 3–6x better**, and is **by a wide
margin the most memory-frugal** of everything measured.

## Reproducing any of it

```sh
./bench/run.sh                                   # ~15 minutes, cleans up after itself
python3 bench/report.py                          # re-render tables from committed JSON
IMAGES="ramjet:before ramjet:after" python3 bench/ab.py

bench/thesis/run-all.sh                          # the whole cluster suite
python3 bench/thesis/report.py
bench/thesis/teardown.sh                         # remove everything, and verify it

./bench/engine/run.sh                            # ~25 minutes
python3 bench/engine/report.py

cargo bench -p ramjet-router                     # the matcher microbenchmark
```

Raw data lives beside each harness: `bench/results/` (current),
`bench/results/4f58bd7/` and `bench/results/before/` (kept verbatim),
`bench/thesis/results/` including `b4/` and `b4-after/`, and
`bench/engine/results/`. Each archived run keeps its own `versions.txt`,
`diagnostics.txt` and `table.md`.

Two harness rules worth knowing before you re-run anything:

- **Do not shorten `WARMUP` below 10s**, including in the smoke test. A 2s
  warmup measured 39,000 rps at c64 and 72,549 rps at c128 in the run
  immediately after — throughput rising with concurrency is the signature of a
  first run that had not finished warming.
- **Check the host before trusting any number.** On a quiet host the baseline
  measures ~248,000 rps; below ~230,000 the host is busy and the run is not
  worth starting. Host-side contention reaches the guest through vCPU
  preemption, so container `--cpuset` pinning does not protect against it.
  Overriding any tunable redirects output to `results/scratch/` so a smoke run
  cannot overwrite a real measurement.
