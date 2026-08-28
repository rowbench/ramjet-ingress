# ramjet-ingress vs ingress-nginx: what a configuration change costs

**The thesis holds, and it is not close.** Under a configuration change every
two seconds, ramjet-ingress forwards traffic at its own unchurned rate, spends
the same CPU per request, drops nothing, and keeps every idle keep-alive
connection. ingress-nginx, on the same cluster with the same mutation stream,
loses **every** idle keep-alive connection each time the change forces a reload
— 0 out of 50, reproducibly, in every run — spends 10-25% more CPU per request
while churn is happening, and takes 3x longer to get a change into the data
plane on an empty cluster, 10x longer with 500 routes loaded.

**And ingress-nginx wins the fourth benchmark outright.** An idle keep-alive
connection costs it 4.4 KiB; it costs ramjet-ingress 27 KiB, and ramjet-ingress
does not give the memory back when the connections close. At 10,000 idle
connections ramjet-ingress measured 266 MiB — above the 256Mi limit its own
Helm chart ships by default.

<!-- Tables are rendered by report.py from results/; the prose is not. -->

## Why this document exists

[`bench/RESULTS.md`](../RESULTS.md) measured raw HTTP/1.1 forwarding between
ramjet-ingress and plain nginx, found them level at c64, and ended by listing
what it did **not** test: "thousands of routes, or configuration churn under
live traffic, and the ingress-nginx control plane that the reload argument is
aimed at was explicitly out of scope."

This document is that scope. The opponent is the real
`kubernetes/ingress-nginx` controller, both controllers are installed in the
same cluster at the same time, and the question is the one the architecture is
an answer to: **what does changing the configuration cost the traffic already
flowing through?**

One of the inherited honesty rules does most of the work here: *understating the
competitor is the one way to make this report worthless.* ingress-nginx does
**not** reload for every change. Endpoint updates go through its Lua balancer
without touching nginx at all, and a report that measured only the changes which
force a reload would be describing a system that does not exist. Both kinds of
churn are measured below, separately, and the difference between them is the
most useful thing on this page.

## Method

### Both contenders, one cluster

Both controllers are installed into the same Docker Desktop cluster at the same
time, in separate namespaces, answering separate IngressClasses. Neither is
scaled down while the other is measured, so each pays the cost of watching the
other's objects and discarding them — symmetrically. Load is never sent to both
at once.

| | ramjet-ingress | ingress-nginx |
|---|---|---|
| Chart | `deploy/chart/ramjet-ingress` (this repo) | `ingress-nginx/ingress-nginx` 4.15.1 |
| IngressClass | `ramjet-thesis-ramjet` | `ramjet-thesis-nginx` |
| Replicas | 1 (the chart hard-codes it) | 1 (chart default) |
| Requests | cpu 100m, memory 64Mi | cpu 100m, memory 90Mi |
| Limits | **memory 256Mi** | none (chart default) |
| Admission webhook | none | enabled (chart default) |

ingress-nginx is the more generously provisioned of the two: it runs with no
memory ceiling while ramjet-ingress runs under the 256Mi cap its own chart
imposes. That difference matters in benchmark 4.

### The load path

A container on the `kind` docker bridge sends traffic to a **NodePort on the
Kubernetes node's own bridge address**: bridge → nodePort → kube-proxy DNAT →
controller pod, identically for both.

`deploy/e2e.sh` uses `kubectl port-forward`, which was tried and rejected — a
port-forward is one SPDY-multiplexed stream through a Go proxy on the host and
becomes the bottleneck long before either contender does, so every number would
have been a measurement of kubectl. Both charts default to a `LoadBalancer`
Service, also rejected: two of them would both ask Docker Desktop for host port
80 and one would sit Pending forever.

### The backends

Three Deployments of two `nginx:1-alpine` replicas each, returning a fixed
128-byte body from memory with `bench/upstream.conf`'s settings. Each carries a
distinct marker string, which is how the backend-swap benchmark knows the *new*
backend has started answering.

The first attempt used `ealen/echo-server`, because it reports which pod
answered. It capped the entire topology at **~400 rps** — a Node.js process
serialising a JSON description of every request is two orders of magnitude
slower than either proxy in front of it, so every number would have been a
measurement of the backend. Those runs are discarded and not reported.

### Load generation and probes

- **oha 1.16.0** in a container pinned to four of the VM's eight cores, c64,
  HTTP/1.1 keep-alive, `-w` so in-flight requests are awaited at the deadline
  rather than counted as errors.
- **A sequential timeline probe** (`probe.py timeline`): one connection, one
  request at a time, every request logged with its offset and latency. A
  percentile smears a 300 ms stall across a 110-second window until it is
  indistinguishable from ordinary tail latency; the raw series does not.
- **An idle keep-alive holder** (`probe.py idle`): 50 connections, each made
  real with one request, then held idle and probed every 10 seconds. Before each
  probe the socket is checked for a server-sent FIN, because that — not a failed
  request — is the event a reload produces. The 10-second cadence keeps them
  inside nginx's 75-second `keep-alive` idle timeout, so a connection lost here
  was lost to a configuration change and not to a timeout expiring.
- **A propagation prober** (`probe.py propagate`): starts the clock, launches
  `kubectl apply` as a subprocess, and polls the data plane every 20 ms until the
  change is visibly served. kubectl runs *inside the same container as the
  poller*, so "applied" and "served" are two readings of one clock rather than a
  comparison of two machines'.

### The two kinds of churn

This is the distinction the whole comparison turns on.

- **Ingress-spec churn** adds a differently-named path to a churn Ingress every
  two seconds. A new location changes the generated `nginx.conf`, so
  ingress-nginx must write it out and reload. Verified from its own log
  (`Backend successfully reloaded`), not assumed.
- **Endpoint-only churn** moves one running pod in and out of a Service's
  selector every two seconds. ingress-nginx pushes endpoint changes to its Lua
  balancer without reloading nginx — this is the case its architecture handles
  well, and measuring it is what lets this report say where the reload cost does
  and does not apply.

The endpoint mutation flips a **label on a running pod** rather than scaling the
Deployment. The Deployment's selector is `app` only while the Service's is
`app` + `member`, so relabelling removes a pod from the EndpointSlice instantly
without the ReplicaSet noticing. Scaling would have measured the scheduler's
latency and reported it as the controller's.

Each contender churns its own backend Service, so a mutation recompiles exactly
one controller's configuration.

### Counting configuration events

Asymmetric on purpose. ramjet-ingress publishes
`ramjet_route_table_generation` on an admin port its chart already exposes, so
reading it costs nothing. ingress-nginx exposes an equivalent counter only when
`controller.metrics.enabled` is turned on — and that same flag switches on
per-request Lua monitoring, so measuring the reload would have slowed the thing
being measured. Its log states every reload explicitly, at no cost, so that is
what is counted.

### Interleaving and warmup

Contenders are interleaved within every arm and the order flips between rounds.
Every measured window is preceded by a discarded warmup load run of the same
shape, because `bench/RESULTS.md` recorded a cold ramjet at 42k rps against 58k
once its upstream pool had filled.

## Benchmark 1 — configuration churn under live traffic

Constant c64 keep-alive load on a route that never changes, for a 120-second
window with a 10-second quiet lead-in and a 100-second measured load run, while
a *different* Ingress is mutated every two seconds. Two rounds, contenders
interleaved, order flipped between rounds.

### Throughput and latency under churn (oha, c64, 2 runs per cell)

| Contender | Arm | RPS (median of runs) | vs own baseline | p50 | p99 | p99.9 | HTTP errors |
|---|---|---:|---:|---:|---:|---:|---:|
| ramjet | baseline | 104,844 | — | 0.35 ms | 3.8 ms | 12.5 ms | 0 |
| ramjet | spec | 107,536 | +2.6% | 0.35 ms | 3.6 ms | 10.3 ms | 0 |
| ramjet | endpoint | 103,298 | -1.5% | 0.35 ms | 3.8 ms | 12.7 ms | 0 |
| nginx | baseline | 87,865 | — | 0.43 ms | 4.6 ms | 13.0 ms | 0 |
| nginx | spec | 78,368 | -10.8% | 0.45 ms | 5.4 ms | 18.3 ms | 1722 |
| nginx | endpoint | 64,305 | -26.8% | 0.57 ms | 6.7 ms | 21.1 ms | 0 |

### Idle keep-alive connections that survived the window

| Contender | Arm | Held | Survived | Lost | Config events the controller applied |
|---|---|---:|---:|---:|---:|
| ramjet | baseline | 100 | 100 | 0 | 0 |
| ramjet | spec | 100 | 100 | 0 | 98 |
| ramjet | endpoint | 100 | 100 | 0 | 98 |
| nginx | baseline | 100 | 100 | 0 | 0 |
| nginx | spec | 100 | 0 | 100 | 66 |
| nginx | endpoint | 100 | 100 | 0 | 0 |

### The single-connection timeline (every request, not a percentile)

| Contender | Arm | Requests | Errors | p50 | p99 | p99.9 | Worst single request |
|---|---|---:|---:|---:|---:|---:|---:|
| ramjet | baseline | 736,328 | 0 | 0.18 ms | 2.3 ms | 7.7 ms | 1,049 ms |
| ramjet | spec | 747,263 | 0 | 0.18 ms | 2.2 ms | 6.9 ms | 149 ms |
| ramjet | endpoint | 753,277 | 0 | 0.17 ms | 2.3 ms | 7.8 ms | 162 ms |
| nginx | baseline | 483,172 | 0 | 0.25 ms | 3.7 ms | 9.7 ms | 347 ms |
| nginx | spec | 470,240 | 28 | 0.22 ms | 3.9 ms | 12.1 ms | 499 ms |
| nginx | endpoint | 370,031 | 0 | 0.31 ms | 5.1 ms | 14.8 ms | 167 ms |

### Visible stalls in the sequential stream

| Contender | Arm | Requests | > 50 ms | > 200 ms | > 1 s | Wall time inside a > 50 ms request | Median gap between stalls |
|---|---|---:|---:|---:|---:|---:|---:|
| ramjet | baseline | 736,328 | 18 | 1 | 1 | 2.3 s of 240 s (1.0%) | 0.92 s |
| ramjet | spec | 747,263 | 13 | 0 | 0 | 1.2 s of 240 s (0.5%) | 9.88 s |
| ramjet | endpoint | 753,277 | 19 | 0 | 0 | 1.5 s of 240 s (0.6%) | 7.63 s |
| nginx | baseline | 483,172 | 11 | 1 | 0 | 1.2 s of 240 s (0.5%) | 1.25 s |
| nginx | spec | 470,212 | 35 | 9 | 0 | 4.8 s of 240 s (2.0%) | 0.51 s |
| nginx | endpoint | 370,031 | 16 | 0 | 0 | 1.2 s of 240 s (0.5%) | 5.53 s |

### Controller cost of the churn window

| Contender | Arm | Pod CPU-seconds | Pod CPU per request | vs own baseline | Pod memory at end |
|---|---|---:|---:|---:|---:|
| ramjet | baseline | 300.9 s | 28.7 us | — | 17.9 MiB |
| ramjet | spec | 310.1 s | 28.8 us | +0% | 18.3 MiB |
| ramjet | endpoint | 302.3 s | 29.3 us | +2% | 16.5 MiB |
| nginx | baseline | 402.8 s | 45.8 us | — | 128.1 MiB |
| nginx | spec | 395.5 s | 50.5 us | +10% | 115.7 MiB |
| nginx | endpoint | 367.7 s | 57.2 us | +25% | 127.8 MiB |

### Reading benchmark 1

**Churn is free for ramjet-ingress, and the CPU column is the proof.** Its
throughput under spec churn (+2.6%) and endpoint churn (-1.5%) both land inside
its own run-to-run spread, which on its own would only mean "the measurement
cannot tell". CPU spent per forwarded request settles it: +0% and +2% against
its own baseline. Recompiling and republishing a route table 49 times in 110
seconds cost the data plane nothing that this benchmark can find.

**The reload destroys idle keep-alive connections, completely and every time.**
Under spec churn ingress-nginx ended every single run with **0 of 50** idle
connections surviving — 0/100 across both rounds, and 0/50 again in each of the
two contended replicate rounds. ramjet-ingress kept 100 of 100. This is the
cleanest result on the page because it does not depend on how fast the machine
was: nginx's retiring workers close their idle keep-alive connections, and there
is no load level or CPU budget at which they do not.

**It also drops requests, though not many.** 1,722 errors across the two spec
rounds (`connection closed before message completed` and `connection error`),
against zero for ramjet-ingress in every arm of every round. As a rate that is
about 0.01% of the ~16 million requests forwarded — small, but it is not zero,
and it is exactly zero on the other side.

**Where ingress-nginx is fine: endpoint churn.** It did not reload once —
0 reloads across every endpoint-churn run, confirmed from its own log — and it
kept all 50 idle connections and served zero errors. Its Lua balancer does what
it claims. Any characterisation of ingress-nginx as "reloads on every change" is
wrong, and this arm is why the claim has to be qualified.

**But its non-reloading path is the more expensive one for CPU.** Endpoint churn
cost ingress-nginx **+25%** CPU per request against its own baseline, where the
reloading path cost +10%. Pushing a new backend list into every worker's Lua
state, twice a second, is steady work on the request path; a reload is a burst
that forks fresh workers. The throughput column says the same thing more loudly
(-26.8% vs -10.8%) but less reliably — see the caveat below.

**The stall shape is visible but modest.** In the sequential stream, spec churn
gave ingress-nginx 35 requests over 50 ms and 9 over 200 ms, against 11 and 1
at its own baseline; ramjet-ingress had 13 and 0 under spec churn against 18 and
1 at baseline. Neither contender stalls for a whole second because of a config
change. The single worst request in the whole benchmark — 1,049 ms — happened to
ramjet-ingress during an *unchurned* baseline, which is a useful reminder of how
much of this machine's tail belongs to the machine.

**Memory is a seven-fold difference.** ramjet-ingress finished these windows at
16-18 MiB; ingress-nginx at 116-128 MiB.

### What benchmark 1 does not establish

The **-26.8% endpoint-churn throughput figure is not solid**. Arms always ran in
the order baseline, spec, endpoint within a round, so the endpoint arm was
always last and always held whatever drift the machine had accumulated. Rounds 3
and 4 were run specifically to test that, with the arm order reversed — and they
were invalidated by the docker daemon: another agent started their own six-
container proxy benchmark partway through, and throughput for both contenders
fell to roughly a quarter, varying between 27k and 96k rps *within a single
round*. Those rounds are kept, separately, below.

So the ordering confound on the throughput number is **unresolved**. The
CPU-per-request figure (+25%) is the version of this claim that survives, because
it normalises out how fast the machine was; and the connection-survival and
reload counts reproduced identically in all four rounds regardless of contention.

## Benchmark 1 (contended replicate, rounds 3 and 4)

Kept apart from the headline table on purpose. These rounds were measured while
another agent's proxy benchmark was running six containers on the same docker
daemon, and throughput varied by 3.5x *within one round*. Folding them into the
same medians would have averaged two different machines. What they are still
good for is everything that does not depend on machine speed: whether a reload
happened, and whether idle connections survived it.

The throughput table below is its own warning label. It reports ramjet-ingress
losing 62% of its throughput to spec churn — a contender whose CPU per request
did not move at all under the same churn on a quiet machine. That number is the
other agent's benchmark, not this one's, and it is the reason these rounds are
down here instead of up there.

What did reproduce, exactly, on a machine running at a quarter speed: 1,772
errors and **0 of 100** surviving idle keep-alive connections for ingress-nginx
under spec churn, against 100 of 100 and zero errors under endpoint churn with
zero reloads, and 100 of 100 with zero errors for ramjet-ingress in every arm.
Those are the findings this document rests on, and they do not care how busy the
machine is.

### Throughput and latency under churn (oha, c64, 2 runs per cell)

| Contender | Arm | RPS (median of runs) | vs own baseline | p50 | p99 | p99.9 | HTTP errors |
|---|---|---:|---:|---:|---:|---:|---:|
| ramjet | baseline | 95,581 | — | 0.37 ms | 4.3 ms | 14.8 ms | 0 |
| ramjet | spec | 36,320 | -62.0% | 0.82 ms | 13.7 ms | 48.7 ms | 0 |
| ramjet | endpoint | 62,219 | -34.9% | 0.71 ms | 10.9 ms | 38.0 ms | 0 |
| nginx | baseline | 71,257 | — | 0.54 ms | 8.4 ms | 27.2 ms | 0 |
| nginx | spec | 48,064 | -32.5% | 0.75 ms | 13.1 ms | 50.3 ms | 1772 |
| nginx | endpoint | 39,013 | -45.3% | 0.82 ms | 14.7 ms | 52.7 ms | 0 |

### Idle keep-alive connections that survived the window

| Contender | Arm | Held | Survived | Lost | Config events the controller applied |
|---|---|---:|---:|---:|---:|
| ramjet | baseline | 100 | 100 | 0 | 0 |
| ramjet | spec | 100 | 100 | 0 | 91 |
| ramjet | endpoint | 100 | 100 | 0 | 96 |
| nginx | baseline | 100 | 100 | 0 | 0 |
| nginx | spec | 100 | 0 | 100 | 67 |
| nginx | endpoint | 100 | 100 | 0 | 0 |

### The single-connection timeline (every request, not a percentile)

| Contender | Arm | Requests | Errors | p50 | p99 | p99.9 | Worst single request |
|---|---|---:|---:|---:|---:|---:|---:|
| ramjet | baseline | 625,463 | 0 | 0.19 ms | 3.0 ms | 8.9 ms | 223 ms |
| ramjet | spec | 252,871 | 0 | 0.28 ms | 9.2 ms | 29.4 ms | 2,681 ms |
| ramjet | endpoint | 458,554 | 0 | 0.22 ms | 5.5 ms | 20.9 ms | 1,057 ms |
| nginx | baseline | 412,024 | 0 | 0.24 ms | 5.9 ms | 20.4 ms | 217 ms |
| nginx | spec | 328,590 | 26 | 0.21 ms | 7.7 ms | 30.0 ms | 793 ms |
| nginx | endpoint | 226,923 | 0 | 0.39 ms | 10.2 ms | 36.1 ms | 505 ms |

### Visible stalls in the sequential stream

| Contender | Arm | Requests | > 50 ms | > 200 ms | > 1 s | Wall time inside a > 50 ms request | Median gap between stalls |
|---|---|---:|---:|---:|---:|---:|---:|
| ramjet | baseline | 625,463 | 16 | 1 | 0 | 1.4 s of 240 s (0.6%) | 2.59 s |
| ramjet | spec | 252,871 | 102 | 8 | 2 | 13.5 s of 240 s (5.6%) | 0.55 s |
| ramjet | endpoint | 458,554 | 81 | 4 | 1 | 8.7 s of 240 s (3.6%) | 0.54 s |
| nginx | baseline | 412,024 | 58 | 1 | 0 | 4.7 s of 240 s (2.0%) | 1.05 s |
| nginx | spec | 328,564 | 129 | 6 | 0 | 12.3 s of 240 s (5.1%) | 0.57 s |
| nginx | endpoint | 226,923 | 134 | 4 | 0 | 11.5 s of 240 s (4.8%) | 0.72 s |

### Controller cost of the churn window

| Contender | Arm | Pod CPU-seconds | Pod CPU per request | vs own baseline | Pod memory at end |
|---|---|---:|---:|---:|---:|
| ramjet | baseline | 290.1 s | 30.3 us | — | 18.8 MiB |
| ramjet | spec | 142.9 s | 39.3 us | +30% | 18.6 MiB |
| ramjet | endpoint | 208.3 s | 33.5 us | +10% | 22.9 MiB |
| nginx | baseline | 315.5 s | 44.3 us | — | 148.2 MiB |
| nginx | spec | 301.6 s | 62.8 us | +42% | 137.2 MiB |
| nginx | endpoint | 254.3 s | 65.2 us | +47% | 150.5 MiB |

## Benchmark 2 — propagation latency

`kubectl apply` to the first request the data plane answers correctly, polled
every 20 ms, no other load running. Ten trials of each shape per contender,
contenders interleaved with the order flipping every trial.

- **new Ingress** — a brand-new host. Success is the first HTTP 200; both
  contenders 404 until the route exists.
- **backend swap** — an existing route's backend Service is changed. The route
  keeps answering 200 from the *old* backend throughout, so success is the first
  response carrying the new backend's marker, not the first 200.

### Apply -> served, no other load (milliseconds)

| Contender | Change | Trials | Median | p95 | Min | Max | Median `kubectl apply` |
|---|---|---:|---:|---:|---:|---:|---:|
| ramjet | new Ingress | 10 | 363 | 566 | 324 | 566 | 159 |
| ramjet | backend swap | 10 | 354 | 556 | 322 | 556 | 150 |
| nginx | new Ingress | 10 | 1,151 | 3,638 | 302 | 3,638 | 138 |
| nginx | backend swap | 10 | 459 | 3,657 | 384 | 3,657 | 188 |

### Reading benchmark 2

**ramjet-ingress is ~3x faster at the median and ~6x faster at p95**, and the
more useful half of that is the spread. Its ten new-Ingress trials ran from 324
to 566 ms; ingress-nginx's ran from 302 to 3,638 ms. A change either lands in
about a third of a second or it does not; predictability is the difference.

**The admission webhook is not the reason, and this is where ingress-nginx ties
or wins.** Median `kubectl apply` was 138 ms for ingress-nginx against 159 ms
for ramjet-ingress — the write path including `nginx -t` validation of the whole
generated configuration is *faster* than ramjet-ingress's plain unvalidated
write. Every millisecond of the difference between the two contenders is after
the apply returned.

**ingress-nginx's slow trials cluster just under 3.5 seconds** (3,638 / 3,562 /
2,893 / 3,579 / 3,657 ms) and alternate with fast ones. That shape is a rate
limiter, not a queue, and the controller names the number itself:
`--sync-rate-limit` defaults to **0.3**, one sync per 3.33 seconds, so a change
arriving just after a sync waits for the next token. The fast trials show it can
be quick; the slow ones show that whether it *is* quick depends on when the
change arrives relative to the last one. Raising that flag would shorten this
tail — it is a default, not a limit of the design — but the default is what a
cluster gets. ramjet-ingress has a fixed 200 ms debounce and no rate limit, so
it has no slow mode to land in.

**The backend swap is the closer race.** ingress-nginx's median was 459 ms
against ramjet-ingress's 354 ms, because an endpoint-shaped change takes its
Lua path rather than a reload. Its p95 is still 3,657 ms.

## Benchmark 3 — 500 routes

500 Ingresses with distinct hosts, applied as one batch, one contender at a
time. Convergence is timed the same way as benchmark 2: the last object applied
is the sentinel, and the clock stops when the data plane serves it.

### Loading the routes

| Contender | Ingresses created | `kubectl apply` wall time | Apply -> last route served | Controller CPU | Controller memory before -> after | nginx reloads |
|---|---:|---:|---:|---:|---:|---:|
| ramjet | 500/500 | 10.7 s | 10.9 s | 1 s | 21.1 -> 20.8 MiB | — |
| nginx | 500/500 | 58.5 s | 61.7 s | 67 s | 115.8 -> 214.0 MiB | 19 |

### Propagation of a new Ingress with the routes already loaded

| Contender | Trials | Median | p95 | Min | Max | Median with an empty cluster (benchmark 2) |
|---|---:|---:|---:|---:|---:|---:|
| ramjet | 5 | 507 ms | 723 ms | 414 ms | 723 ms | 363 ms |
| nginx | 5 | 5,006 ms | 5,964 ms | 3,374 ms | 5,964 ms | 1,151 ms |

### Forwarding on the stable route with the routes loaded (c64, 30s)

| Contender | RPS | p50 | p99 |
|---|---:|---:|---:|
| ramjet | 47,535 | 0.69 ms | 9.2 ms |
| nginx | 16,109 | 1.26 ms | 35.8 ms |

### Reading benchmark 3

**Both reached 500. Neither choked.** That is worth saying first, because the
brief allowed for reporting the number at which one of them fell over, and
neither did.

**ramjet-ingress converged 5.7x faster and cost the API server far less.** 10.9
seconds against 61.7, of which the apply itself was 10.7 against 58.5 — so most
of ingress-nginx's convergence time is the write path, at roughly 117 ms per
Ingress against ramjet-ingress's 21 ms. The admission webhook that was free at
one Ingress in benchmark 2 is not free at 500.

**Controller CPU differs by a factor of 100.** 0.66 CPU-seconds against 66.7 to
load the same 500 routes.

**Memory is the sharper result.** ramjet-ingress went from 21.1 MiB to 20.8
MiB — 500 compiled routes are, within measurement noise, free. ingress-nginx went
from 115.8 MiB to 214.0 MiB, an increase of 98 MiB, or roughly 200 KiB per
route. Under the 256Mi limit ramjet-ingress's own chart ships, ingress-nginx
would have been within 40 MiB of being OOM-killed at 500 routes; it survives
because its chart ships no limit at all.

**Propagation degrades for both, but not equally.** A new Ingress with 500
already loaded took ramjet-ingress a median 507 ms against 363 ms on an empty
cluster (1.4x), and ingress-nginx 5,006 ms against 1,151 ms (4.4x). Every
ingress-nginx trial at scale was slower than its own worst trial on an empty
cluster.

**The throughput row is the weakest number in this document.** 47,535 rps
against 16,109 is a 3x gap, but the two measurements were taken minutes apart at
38% and 25% VM CPU idle respectively, on a machine that was doing someone else's
work. The direction is consistent with everything else here and the gap is far
larger than a 13-point idle difference plausibly explains, but it is one run
each under unequal conditions and should not be quoted as a throughput result.
Benchmark 1's baselines — 104,844 against 87,865 on a quiet machine — are the
throughput comparison worth citing.

**Deleting 500 Ingresses took about 105 seconds for each.** A tie, and API-server
bound rather than controller bound.

## Benchmark 4 — 10,000 idle keep-alive connections

No Kubernetes. ingress-nginx's data plane *is* nginx, so the question "what does
a connection cost the proxy" is answered by putting ramjet-ingressd and plain
nginx on the same docker bridge with the same upstream, exactly as
`bench/run.sh` does — with `bench/nginx.conf`'s tuning, so nginx arrives with
every advantage that benchmark already gave it. Two passes, order reversed
between them.

What is sampled is the container's cgroup memory working set via `docker stats`,
which is what `bench/RESULTS.md` reports as memory for both contenders. It is
not literally VmRSS — it is RSS plus whatever page cache the cgroup is charged
for, minus inactive file pages — but neither proxy touches a file on the request
path, and it is the number a memory limit is enforced against.

### Container memory across 10,000 idle keep-alive connections

| Contender | Pass | Established | Idle before | At 10k | After close | Per connection | Retained |
|---|---|---:|---:|---:|---:|---:|---:|
| ramjet | 1 | 10,000/10,000 | 1.5 MiB | 266.1 MiB | 229.6 MiB | 27.1 KiB | +228.1 MiB |
| ramjet | 2 | 10,000/10,000 | 229.6 MiB | 329.0 MiB | 292.3 MiB | 10.2 KiB | +62.7 MiB |
| nginx | 1 | 10,000/10,000 | 16.3 MiB | 58.9 MiB | 16.5 MiB | 4.4 KiB | +0.2 MiB |
| nginx | 2 | 10,000/10,000 | 16.5 MiB | 58.8 MiB | 16.5 MiB | 4.3 KiB | +0.0 MiB |

### Reading benchmark 4

**ingress-nginx wins this one decisively, and it is the most important negative
result in this report.** An idle keep-alive connection costs nginx 4.4 KiB and
costs ramjet-ingress 27.1 KiB on a cold process — 6x. Both established all
10,000 connections, in comparable time, so this is a like-for-like measurement of
what each keeps per connection.

**ramjet-ingress does not give the memory back.** nginx returned to 16.5 MiB
after all 10,000 connections closed — to the byte, on both passes.
ramjet-ingress went to 266.1 MiB, fell only to 229.6 MiB when they closed, and on
the second pass rose to 329.0 MiB and settled at 292.3 MiB. That is monotonic
growth across connect/disconnect cycles, which for a process meant to run for
months is a worse property than the peak. Some of it is glibc's allocator holding
freed arenas rather than a leak in the usual sense, but a memory limit does not
care about the distinction.

**Under its own chart's defaults this would be an outage.** The chart sets
`resources.limits.memory: 256Mi`. Peak here was 266 MiB at 10,000 idle
connections, with no traffic flowing. A ramjet-ingress replica holding 10,000
idle keep-alive connections would be OOM-killed by its own default manifest.
`bench/RESULTS.md` already flagged the direction of this — "memory got worse, and
that is a real cost", 33.1 MiB against nginx's 12.6 MiB under load — and this
benchmark shows where that trend ends up.

## Where ingress-nginx won or tied

Collected in one place, because a report that only lists the other side's losses
is not a measurement.

| | Result |
|---|---|
| **Idle-connection memory** | **Won, heavily.** 4.4 KiB/connection against 27.1, and it returns all of it on close while ramjet-ingress retains and grows. |
| **`kubectl apply` write path (single Ingress)** | **Won.** 138 ms median against 159, *including* `nginx -t` validation of the whole configuration through an admission webhook that ramjet-ingress does not have. |
| **Endpoint-only churn: connection safety** | **Tied.** 50/50 idle connections survived, zero errors, zero reloads. Its Lua balancer does exactly what it claims and the reload argument does not apply to endpoint changes. |
| **Deleting 500 Ingresses** | **Tied.** ~105 s each; the API server is the bottleneck, not either controller. |
| **Reaching 500 routes at all** | **Tied.** Both converged; neither fell over. |
| **Stall severity** | **Tied-ish.** Neither contender produced a stall over one second attributable to churn. ingress-nginx's reload is visible in the tail, but it is tens of milliseconds, not seconds. |

## Deviations from chart defaults, and why

Four. Three are forced; the fourth is deliberate and favours ingress-nginx.

1. **`service.type: NodePort` on both.** Forced: two LoadBalancer Services would
   collide on host port 80. Identical on both sides.
2. **`controller.progressDeadlineSeconds: 600` on ingress-nginx.** Forced: chart
   4.15.1 ships `progressDeadlineSeconds: 0` alongside `minReadySeconds: 0`, and
   Kubernetes 1.36 rejects a Deployment whose progress deadline is not greater
   than its `minReadySeconds` — the chart does not install here without it. 600
   is Kubernetes' own default for the field and affects rollout reporting only.
3. **Unique release names and IngressClasses.** Forced by running two
   controllers side by side.
4. **`disable-access-log: "true"` on ingress-nginx.** Deliberate, and it makes
   ingress-nginx *faster*. At the chart default nginx writes a log line for every
   request it forwards and ramjet-ingress writes none, so leaving it on would
   have charged one contender for work its opponent never does. `bench/run.sh`
   made exactly this choice for plain nginx and recorded why. It also keeps the
   controller's own log readable, which matters because that log is where reloads
   are counted.

Nothing else in `controller.config` is set: `worker-processes auto`,
`keep-alive 75`, `worker-connections 16384` and
`upstream-keepalive-connections 320` are all the chart's own values. No tuning
is applied to ramjet-ingress that its chart does not ship.

## Known unfairness that could not be eliminated

- **Nothing is CPU-pinned except the load generator.** `bench/run.sh` pins each
  contender to two cores, which is what makes its numbers like-for-like. That is
  not possible here: the pods run inside a Kubernetes node container whose CPU
  allocation belongs to Docker Desktop. Both controllers see all eight of the
  VM's CPUs and both start eight workers; the load generator is capped at four.
  Symmetric, but not isolation — absolute throughput here should not be compared
  with `bench/RESULTS.md`'s.
- **A shared docker daemon, and it bit.** Every measured run waits for the VM to
  report idle CPU with no `docker build` running. That gate was not strict
  enough: it did not notice another agent's *benchmark* containers, and rounds 3
  and 4 of benchmark 1 were measured against them. The gate now also refuses to
  start while any container it does not own is running. Benchmark 3's throughput
  row was taken under the same conditions and is discounted above.
- **The arm-order confound in benchmark 1 is unresolved**, as described there.
- **Benchmark 3 ran one contender's 500 routes at a time.** Both controllers
  still watch and discard the other's objects, but only one holds 500 compiled
  routes at any moment, so the memory figures are attributable while the
  watch-and-discard cost is shared.
- **The VM is small.** 8 CPUs, 3.9 GiB of RAM, swap already in use before the
  benchmark started.
- **macOS, Apple Silicon, linuxkit.** Same caveat `bench/RESULTS.md` carries: the
  relative comparison is what is claimed, not the absolute figures.

## Verdict

On the claim the project is built to make — that a configuration change is a
pointer swap and therefore costs the traffic in flight nothing — the measurement
supports it without qualification. Forty-nine route-table publishes in 110
seconds moved ramjet-ingress's CPU per request by 0-2% and cost it no
connections and no requests, while the equivalent stream of reloads cost
ingress-nginx every idle keep-alive connection it was holding, 10% more CPU per
request, and about 0.01% of its requests. At 500 routes the control-plane gap
widens: 5.7x on convergence, 100x on CPU, and memory that is flat against memory
that grows 200 KiB per route.

The claim needs one qualification and it is ingress-nginx's to make: **endpoint
changes do not force a reload**, and in that arm ingress-nginx kept every
connection and dropped nothing. If a cluster's churn is scaling and rolling
deployments rather than Ingress edits, the reload argument does not describe it —
though that path still cost ingress-nginx 25% more CPU per request while it was
happening, which the reload argument does not predict either.

And the project has a problem this benchmark found rather than confirmed.
ramjet-ingress holds 27 KiB per idle connection against nginx's 4.4 KiB, does not
release it on close, and grows across cycles — enough that 10,000 idle
connections put it over the memory limit its own Helm chart ships. Being free to
reconfigure is worth little if the process is killed for holding connections open.
That is the next thing to fix, and it is not a tuning knob.

## Reproducing

```sh
bench/thesis/run-all.sh              # the whole suite
QUICK=1 bench/thesis/run-all.sh      # shapes only, not results
python3 bench/thesis/report.py       # re-render every table from committed JSON
python3 bench/thesis/compact.py      # shrink the timeline series before committing
bench/thesis/teardown.sh             # remove everything, and verify it
```

Every script names `--context docker-desktop` on every kubectl and helm call and
refuses to run against a cluster that is not a single local Docker Desktop node.
Raw output for every run is in `results/`, so each table can be re-derived
without re-running anything. One artifact is abridged: the timeline probe's
per-request series is 117 MiB across 24 runs, so `compact.py` keeps every
request of 10 ms or more verbatim — which is everything the stall table counts
— plus a per-second count-and-max envelope for the rest. The percentiles in
those files were computed from the complete series before compaction, and
`report.py` renders byte-identical tables either way.

## Versions

```
date:            2026-08-28T00:47:12Z
host:            Darwin 25.5.0 arm64, 12 host CPUs
docker:          29.7.2
docker VM:       8 CPUs, 3.8 GiB, kernel 7.0.12-linuxkit
kubernetes:      v1.36.1
node runtime:    containerd://2.3.1
helm:            v3.18.0

ramjet-ingress:  ramjet-ingressd 0.1.0
  image:         ramjet-thesis:8078948 (sha256:a9d3ac608dac3c4d9452c1d2073577d8adbe3421ae7b537d5542b8a64b3fe5c7)
  built from:    807894819a3efb2c54672ec240fbbbc6aa381845 (fix(deploy): load the image into the node, keep imagePullPolicy Never)
  repo HEAD now: b09aa5d (bench: rotate round order, add a cooldown, document the host gate) — moved during the run, not measured
  chart:         deploy/chart/ramjet-ingress version 0.1.0
  flags:         --http=:8080 --https=:8443 --admin=:10254 --ingress-class=ramjet-thesis-ramjet --publish-service=ramjet-thesis-ramjet/ramjet-thesis-ramjet --publish-address=127.0.0.1 --connect-timeout=5 --response-timeout=60 --max-connect-attempts=3 --shutdown-grace=30 
  resources:     {"limits":{"memory":"256Mi"},"requests":{"cpu":"100m","memory":"64Mi"}}
```
