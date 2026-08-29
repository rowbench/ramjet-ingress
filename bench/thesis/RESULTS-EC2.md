# ramjet-ingress vs ingress-nginx, on real Linux

**The thesis survives the move off Docker Desktop, and one of the previous
report's supporting claims does not.**

On an EC2 `t3.xlarge` running k0s — real Linux, no VM in the path — under an
Ingress-spec change every two seconds, ingress-nginx again lost **every** idle
keep-alive connection it was holding (0 of 100 across two rounds), dropped 1,829
requests, and spent 12% more CPU per request. ramjet-ingress kept **100 of 100**
in every arm of every round, dropped nothing, and moved its CPU per request by
1%. That is the same result [`RESULTS.md`](./RESULTS.md) found in the VM, at the
same magnitudes, on a different kernel and a different cluster.

**The claim that does not reproduce is one that was in ramjet-ingress's
favour.** The VM run reported that ingress-nginx's *non-reloading* endpoint path
cost it **+25%** CPU per request — the finding that its Lua balancer is
expensive even when it avoids a reload. Here that number is **+1%**. On real
Linux the Lua path is close to free, and the earlier figure looks like an
artifact of the arm-ordering confound that run flagged and could not resolve.
This one resolves it, and the correction belongs to ingress-nginx.

**ingress-nginx also draws the `kubectl apply` write path**, which it lost by
15% in the VM: 191 ms against ramjet-ingress's 189 ms, *including* `nginx -t`
validation of the whole generated configuration through an admission webhook
ramjet-ingress does not have. Doing strictly more work in the same time is a win
on points.

<!-- Tables are rendered by ec2/report.py from results-ec2/; the prose is not. -->

## Why this document exists

[`RESULTS.md`](./RESULTS.md) measured this comparison inside a macOS Docker
Desktop linuxkit VM and said so at the top: *"the relative comparison is what is
claimed, not the absolute figures."* [`docs/src/performance.md`](../../docs/src/performance.md)
then checked the uring engine's margin on this same EC2 box and found the VM had
inflated it — +44.9% became +5.8%.

So the obvious question is whether the *thesis* result was inflated too. It was
not. This document is that check, against the real `kubernetes/ingress-nginx`
controller, on hardware that is not pretending.

## Method

Both — three, here — controllers stand in one cluster at once, in separate
namespaces answering separate IngressClasses. None is scaled down while another
is measured, so each pays the cost of watching the others' objects and
discarding them. Load is never sent to more than one at a time.

### What is different from the Docker Desktop run

| | [`RESULTS.md`](./RESULTS.md) | this run |
|---|---|---|
| Host | macOS on Apple Silicon, linuxkit VM | EC2 `t3.xlarge`, Ubuntu 26.04, no VM |
| CPUs | 8 VM CPUs, load generator pinned to 4 | **4 shared burstable vCPUs, nothing pinned** |
| Cluster | Docker Desktop, Kubernetes v1.36.1 | k0s v1.36.3 |
| Load generator | oha in a container on the `kind` bridge | oha **on the node**, to 127.0.0.1 |
| Load path | bridge → NodePort → DNAT → pod | loopback → NodePort → DNAT → pod |
| Backends | 3 × 2 `nginx:1-alpine`, 128-byte body | 3 × 2 `hashicorp/http-echo`, as Phase 16 used |
| ramjet arms | one, hyper | **two: hyper (stock) and uring (opt-in)** |
| Contenders | 2 | 3, plus the backend Service as a no-proxy baseline |

> **The caveat this run inherits, verbatim in spirit from Phase 16.** The load
> generator, the proxy under test and the upstreams are all on the same four
> shared vCPUs. The proxy is therefore never the sole bottleneck, and **a
> benchmark where the thing under test is not the limiting factor understates
> every difference between contenders.** Every ratio between two proxies on this
> page is compressed toward 1. A rerun on pinned, isolated cores with the load
> off-box is the number worth quoting, and this is not that run.

### Three releases, not one upgraded between arms

Phase 16 measured hyper against uring by upgrading a single release between
runs. That is right when only a flag varies. It is wrong here: rotating four
contenders three times would mean twelve pod restarts and twelve cold upstream
pools inside the measurement, and `bench/RESULTS.md` recorded a cold ramjet at
42k rps against 58k once its pool had filled. So all three controllers stand at
once and rotation is a change of port number.

### The two ramjet arms differ in two things at once, deliberately

`engine: uring` needs `podSecurityContext.seccompProfile.type: Unconfined` to
start at all — containerd's default profile blocks `io_uring_setup`, so a stock
pod falls back to hyper and logs that it did. The two arms are therefore **"what
a stock install gets"** (hyper, `RuntimeDefault`) against **"what opting in
gets"** (uring, `Unconfined`), not an isolation of the engine. Phase 16 isolated
the engine with seccomp held constant and got +5.8%; this run is not comparable
to that one and the difference is discussed below rather than explained away.

Every uring window is gated on the startup line reading `engine="uring"`. **A
run that fell back is a second hyper run wearing a label**, and the harness
refuses to measure one. Both arms' lines are in
[`results-ec2/versions.txt`](./results-ec2/versions.txt).

### The two kinds of churn

Unchanged from `RESULTS.md`, because the distinction is what the comparison
turns on.

- **Ingress-spec churn** adds a differently-named path to a churn Ingress every
  two seconds. A new location changes the generated `nginx.conf`, so
  ingress-nginx must write it and reload. Counted from its own log
  (`Backend successfully reloaded`), not assumed.
- **Endpoint-only churn** flips a label on a *running* pod, moving it in and out
  of a Service's EndpointSlice every two seconds. The Deployment's selector is
  `app` while the Service's is `app` + `member`, so the ReplicaSet never notices
  and nothing is scheduled or killed — scaling would have measured the
  scheduler and called it the controller. ingress-nginx pushes this to its Lua
  balancer without reloading.

Each contender churns its own backend Service, so one mutation recompiles
exactly one controller's configuration.

### Controller CPU, and why it is the column to read

Differenced across each window from the pod's own cgroup v2 `cpu.stat`
(`usage_usec`); memory is `memory.current` minus `inactive_file`, which is how
the kubelet computes a working set and what a memory limit is enforced against.
`kubectl top` reports a rate on metrics-server's schedule and cannot be
differenced across a window.

**On a shared burstable instance, throughput says how much of the machine we
were given and CPU per forwarded request says what the contender did with it.**
The second is the one that reproduced across the VM run's contended and
uncontended rounds when the first did not. `vmstat 1` runs alongside every
measured window and mean steal is in every table; it never exceeded 1.1%.

## Benchmark 1 — steady-state forwarding

One stable route nothing mutates. Three 30-second windows per contender at c64,
rotation offset by one each round, every measured window preceded by a discarded
10-second warmup of the same shape.

#### Concurrency 64 (median of 3 × 30 s, interleaved)

| Contender | RPS (median) | spread | % of baseline | p50 | p99 | Controller CPU per request | Controller memory | mean steal |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| backend, no proxy | 22,234 | 3.0% | 100.0% | 2.11 ms | 13.64 ms | — | — | 0.72% |
| **ramjet (uring)** | **12,355** | 1.2% | **55.6%** | **3.81 ms** | 22.05 ms | **69 µs** | 15.2 MiB | 0.34% |
| ramjet (hyper) | 10,858 | 1.8% | 48.8% | 4.95 ms | **21.11 ms** | 100 µs | **10.5 MiB** | 0.41% |
| ingress-nginx | 7,971 | 2.7% | 35.9% | 7.06 ms | 24.90 ms | 187 µs | 67.6 MiB | 0.09% |

#### Concurrency 256 (single 30 s run each)

| Contender | RPS | % of baseline | p50 | p99 | Controller CPU per request |
|---|---:|---:|---:|---:|---:|
| backend, no proxy | 22,722 | 100.0% | 9.51 ms | 42.32 ms | — |
| **ramjet (uring)** | **13,490** | **59.4%** | **14.62 ms** | 77.83 ms | **69 µs** |
| ramjet (hyper) | 11,705 | 51.5% | 18.56 ms | 78.61 ms | 100 µs |
| ingress-nginx | 8,027 | 35.3% | 29.81 ms | 78.47 ms | 188 µs |

Zero non-2xx responses and zero transport errors in every steady-state window,
for every contender.

#### The claim that survives the drift

A median is shaky on a burstable box. A rank order is not: if every round of one
contender beat every round of another, the ranges do not overlap.

| Comparison | worst round of the first | best round of the second | verdict |
|---|---:|---:|---|
| ramjet (hyper) vs ingress-nginx | 10,689 | 8,078 | disjoint — **ahead by 32% at worst** |
| ramjet (uring) vs ingress-nginx | 12,299 | 8,078 | disjoint — **ahead by 52% at worst** |
| ramjet (uring) vs ramjet (hyper) | 12,299 | 10,880 | disjoint — **ahead by 13% at worst** |

### Reading benchmark 1

**The CPU column is the durable one and it is a 1.9x gap.** ingress-nginx spent
187 µs of pod CPU per forwarded request against ramjet-ingress's 100 µs on the
stock engine and 69 µs on the reactor — 1.9x and 2.7x. That figure does not care
how much of a shared instance we were given, and it is stable to within 2 µs
across every window of both concurrencies.

**Throughput agrees, and is worth less.** +36% for stock ramjet over
ingress-nginx at the median, +55% for the reactor; the rank order holds with
disjoint ranges at +32% and +52%. But all of it is compressed by the caveat
above, and none of these three contenders was the machine's limiting factor.

**At c256 the p99 column collapses into a tie** — 77.8, 78.6 and 78.5 ms — while
throughput and p50 keep their ordering. That is the box saturating, not the
proxies converging: at c256 the queue is in front of all three equally.

**Memory is a 6.4x gap**, 10.5 MiB against 67.6, and it is the one figure here
the VM run agrees with almost exactly.

**The reactor's +13% over stock is not Phase 16's +5.8%, and this run cannot say
why.** Phase 16 measured 10,610 against 11,221 with seccomp held Unconfined in
both arms; here the hyper arm lands at 10,858 — within 2.3% of Phase 16's, so it
is the *uring* arm that moved, not hyper that was handicapped by its stock
seccomp profile. The two runs differ in arm construction (one release upgraded
between arms, against two long-lived releases here), and a cold pod warms its
upstream pool inside a 10-second warmup differently than one that has been up
for minutes. That is a candidate, it is untested, and **the honest statement is
that the two numbers are not comparable rather than that the engine improved.**

## Benchmark 2 — configuration churn under live traffic

The thesis test. Constant c64 keep-alive load on a route that never changes,
inside a 120-second window: 10 seconds quiet, then 100 seconds of load while a
*different* Ingress is mutated every two seconds. Two rounds, contender order
flipped between them, **and arm order flipped too** — the confound `RESULTS.md`
raised and could not settle.

Three things are recorded through each window: oha's aggregate, a single
sequential connection logging every request, and 50 idle keep-alive connections
probed every 10 seconds for a server-sent FIN.

#### Throughput and latency under churn (oha, c64, 2 runs per cell)

| Contender | Arm | RPS (median) | vs own baseline | p50 | p99 | p99.9 | HTTP errors | mean steal |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| ramjet (hyper) | baseline | 10,799 | — | 5.00 ms | 21.08 ms | 31.4 ms | **0** | 0.24% |
| ramjet (hyper) | spec | 10,232 | −5.2% | 5.22 ms | 22.73 ms | 35.0 ms | **0** | 0.12% |
| ramjet (hyper) | endpoint | 10,093 | −6.5% | 5.25 ms | 23.46 ms | 37.4 ms | **0** | 0.22% |
| ingress-nginx | baseline | 8,089 | — | 6.82 ms | 25.50 ms | 37.9 ms | 0 | 0.04% |
| ingress-nginx | spec | 7,226 | −10.7% | 7.75 ms | 28.87 ms | 48.6 ms | **1,829** | 0.04% |
| ingress-nginx | endpoint | 7,578 | −6.3% | 7.15 ms | 29.97 ms | 49.8 ms | 0 | 0.03% |

#### Idle keep-alive connections that survived the window

| Contender | Arm | Held | Survived | Lost | Config events the controller applied |
|---|---|---:|---:|---:|---:|
| ramjet (hyper) | baseline | 100 | **100** | 0 | 0 |
| ramjet (hyper) | spec | 100 | **100** | 0 | 89 |
| ramjet (hyper) | endpoint | 100 | **100** | 0 | 92 |
| ingress-nginx | baseline | 100 | 100 | 0 | 0 |
| ingress-nginx | spec | 100 | **0** | **100** | 66 |
| ingress-nginx | endpoint | 100 | 100 | 0 | **0** |

#### Controller cost of the churn window

| Contender | Arm | Pod CPU-seconds | Pod CPU per request | vs own baseline | Pod memory at end |
|---|---|---:|---:|---:|---:|
| ramjet (hyper) | baseline | 116.0 s | 107.5 µs | — | 12.1 MiB |
| ramjet (hyper) | spec | 111.1 s | 108.6 µs | **+1%** | 11.6 MiB |
| ramjet (hyper) | endpoint | 109.9 s | 108.9 µs | **+1%** | 11.5 MiB |
| ingress-nginx | baseline | 156.0 s | 192.9 µs | — | 68.9 MiB |
| ingress-nginx | spec | 156.5 s | 216.8 µs | **+12%** | 65.7 MiB |
| ingress-nginx | endpoint | 147.1 s | 194.1 µs | **+1%** | 69.6 MiB |

#### The single-connection timeline (every request, not a percentile)

| Contender | Arm | Requests | Errors | p50 | p99 | > 50 ms | > 200 ms | > 1 s | Worst single request |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| ramjet (hyper) | baseline | 130,797 | 0 | 0.41 ms | 15.17 ms | 4 | 0 | 0 | 56 ms |
| ramjet (hyper) | spec | 131,986 | **0** | 0.40 ms | 15.94 ms | 6 | 0 | 0 | 86 ms |
| ramjet (hyper) | endpoint | 127,958 | 0 | 0.42 ms | 16.33 ms | 7 | 0 | 0 | 69 ms |
| ingress-nginx | baseline | 111,349 | 0 | 0.45 ms | 18.91 ms | 6 | 0 | 0 | 105 ms |
| ingress-nginx | spec | 110,139 | **31** | 0.45 ms | 19.63 ms | 20 | 0 | 0 | 90 ms |
| ingress-nginx | endpoint | 106,702 | 0 | 0.47 ms | 20.79 ms | 27 | 0 | 0 | 127 ms |

### Reading benchmark 2

**The reload destroys every idle keep-alive connection, on real Linux exactly as
in the VM.** Under spec churn ingress-nginx ended both rounds with **0 of 50**
surviving — 0 of 100 across the two — while ramjet-ingress kept 100 of 100 in
every arm of both rounds. This is the cleanest result on the page because it
does not depend on how fast the machine was: nginx's retiring workers close the
idle connections they were holding, and there is no CPU budget at which they do
not. Two clusters, two kernels, two Kubernetes distributions, same 0.

**It drops requests, and more of them here.** 1,829 errors under spec churn
against zero for ramjet-ingress in every arm — about 0.13% of the requests in
those windows, against the VM run's 0.01%. Every one is on the reloading arm;
the endpoint arm served zero.

**Churn is free for ramjet-ingress, and the CPU column is the proof.** +1% under
spec churn and +1% under endpoint churn against its own baseline, for 89 and 92
route-table publishes across the two windows — about 45 in each 100 seconds,
against ingress-nginx's 33 reloads. The throughput column shows −5.2% and −6.5%,
and **that is the machine, not the controller** — see the next paragraph.

**The throughput dip under churn is contention, and it is measurable as such.**
ramjet-ingress's CPU per request did not move while its throughput fell 5-6%, so
the missing throughput went somewhere other than the controller: the mutation
stream itself. A `kubectl apply` every two seconds is a process spawn plus API
server work on the same four vCPUs the load generator is using. That tax is paid
by both contenders equally, which lets ingress-nginx's spec-churn deficit be
split: about 6 points of its −10.7% is the same contention ramjet-ingress
suffered, and about 5 points is the reload. Its endpoint arm, at −6.3%, is
contention and essentially nothing else.

**The arm-ordering confound from `RESULTS.md` is resolved, and it did matter.**
That run could only ever run arms in the order baseline, spec, endpoint, so the
endpoint arm always held whatever drift had accumulated, and it reported −26.8%
throughput and +25% CPU for ingress-nginx's endpoint arm with the caveat that
neither could be trusted. Here round 1 ran baseline → spec → endpoint and round
2 ran endpoint → spec → baseline, and **the arm ranking is identical in both
while the positions are reversed**:

| | round 1 (baseline, spec, endpoint) | round 2 (endpoint, spec, baseline) |
|---|---|---|
| ramjet (hyper) | 10,453 > 10,055 > 9,915 | 11,144 > 10,409 > 10,269 |
| ingress-nginx | 7,962 > 7,149 (spec) , 7,530 (endpoint) | 8,215 > 7,302 (spec) , 7,625 (endpoint) |

baseline > endpoint > spec for ingress-nginx and baseline > spec > endpoint for
ramjet-ingress, in both orderings. The ranking is a property of the arm.

**And the resolution corrects a claim in ingress-nginx's favour.** With the
confound removed, endpoint churn costs ingress-nginx **+1%** CPU per request,
not +25%, and **−6.3%** throughput, not −26.8% — the same −6% the contention
costs ramjet-ingress. **Its Lua balancer is close to free on this machine.** The
VM run's conclusion that "its non-reloading path is the more expensive one for
CPU" does not survive, and any reading of the thesis that leaned on it should
drop it. What survives is narrower and stronger: the *reloading* path costs
+12% CPU, 100% of the idle connections, and 0.13% of the requests.

**Neither contender stalled for a second.** No request over 200 ms in any arm on
either side, in 719,000 sequential requests. ingress-nginx's reload is visible
in the >50 ms count (20 under spec churn against 6 at its baseline) and in 31
sequential-stream errors, but it is tens of milliseconds, not seconds.

## Benchmark 3 — propagation latency

`kubectl apply` to the first HTTP 200 through the data plane, polled every
20 ms, no other load. Ten trials per contender, rotated every trial. The clock
starts before kubectl is invoked; the instant it returned is recorded
separately, so the admission-webhook share is a number rather than an argument.

#### Apply → served, new Ingress (milliseconds)

| Contender | Trials | Median | p95 | Min | Max | Median `kubectl apply` | Median after apply returned |
|---|---:|---:|---:|---:|---:|---:|---:|
| ramjet (uring) | 10 | **372** | **394** | 370 | **394** | 186 | **185** |
| ramjet (hyper) | 10 | **384** | 426 | 372 | 426 | 189 | **185** |
| ingress-nginx | 10 | 2,762 | 2,813 | 398 | 2,813 | 191 | 2,585 |

All twenty ramjet trials, both engines, in full: 370, 371, 371, 371, 371, 372,
372, 372, 373, 373, 374, 376, 391, 392, 392, 393, 393, 394, 395, 426. All ten
ingress-nginx trials: 398, 918, 918, 996, 2,751, 2,772, 2,783, 2,787, 2,803,
2,813.

### Reading benchmark 3

**The spread is the result, not the median.** Twenty ramjet trials across two
engines spanned 370 to 426 ms — a 56 ms range with no slow mode in it. Ten
ingress-nginx trials spanned 398 to 2,813 ms, a 7x range, in three distinct
clusters. A change either lands in about four tenths of a second or it does not,
and predictability is the difference.

**ramjet-ingress's post-apply time is quantised into two buckets** — 184-186 ms
and 204-206 ms, nothing between or outside. That is the 200 ms debounce being
sampled at a random phase, which is exactly the shape a fixed debounce with no
rate limiter should produce, and it puts a hard ceiling on the wait.

**ingress-nginx's clusters are its `--sync-rate-limit`.** The flag defaults to
0.3, one sync per 3.33 seconds, so a change arriving just after a sync waits for
the next token; the observed maximum, 2,813 ms, sits under that period as it
must. Raising the flag would shorten this tail — **it is a default, not a limit
of the design** — but the default is what a cluster gets.

> **The median here is harsher on ingress-nginx than the VM run's and should not
> be quoted as a machine-to-machine difference.** That run measured a 1,151 ms
> median against 2,762 ms here. Where each trial lands inside a 3.33-second
> window depends on the gap between trials, and this run's three-contender
> rotation happened to put six of ten trials in the slow cluster where the VM
> run's two-contender rotation put four of ten. **The robust claims are the
> shape and the bound**: bimodality caused by a rate limiter, a maximum set by
> its period, and a contender on the other side with neither.

**The write path is a draw, and that is a change.** Median `kubectl apply` was
191 ms for ingress-nginx against 189 ms for ramjet-ingress — a 2 ms difference
inside the noise, where the VM run had ingress-nginx winning it by 15%. It is
still doing more: `nginx -t` validation of the whole generated configuration
through an admission webhook, against ramjet-ingress's plain unvalidated write.
**Equal time for strictly more work is a win on points for ingress-nginx.**

## Where ingress-nginx won or tied on this run

Collected in one place, because a report that only lists the other side's losses
is not a measurement.

| | Result |
|---|---|
| **Endpoint-churn CPU** | **Won a correction.** The VM run's +25% CPU-per-request penalty on its non-reloading path **does not reproduce**: +1% here, once the arm-ordering confound is removed. Its Lua balancer is close to free, and the earlier figure was an artifact. |
| **Endpoint-churn throughput** | **Won a correction.** −6.3%, not −26.8%, and identical to the −6.5% contention tax ramjet-ingress paid in the same arm. |
| **Endpoint-only churn: connection safety** | **Tied.** 100/100 idle connections survived, zero errors, **zero reloads** across every endpoint-churn window. Any claim that ingress-nginx "reloads on every change" is wrong. |
| **`kubectl apply` write path** | **Tied on the clock, ahead on work done.** 191 ms against 189 ms, *including* `nginx -t` validation through an admission webhook ramjet-ingress does not have. |
| **p99 at c256** | **Tied.** 78.5 ms against 78.6 and 77.8. Under saturation the queue is in front of all three equally. |
| **Steady-state errors** | **Tied.** Zero non-2xx and zero transport errors from every contender in every steady-state window at both concurrencies. |
| **Stall severity** | **Tied.** Neither contender produced a single request over 200 ms in 719,000 sequential requests. |

## Deviations from chart defaults, and why

Five. Four are forced or carried over from `RESULTS.md`; the fifth is what makes
the reactor start.

1. **`service.type: NodePort` on all three.** Forced: three LoadBalancer
   Services on a cluster with no load balancer controller would all sit Pending.
   Identical on every side.
2. **`controller.progressDeadlineSeconds: 600` on ingress-nginx.** Forced: chart
   4.15.1 ships `progressDeadlineSeconds: 0` alongside `minReadySeconds: 0`, and
   Kubernetes 1.36 rejects a Deployment whose progress deadline is not greater
   than its `minReadySeconds`. 600 is Kubernetes' own default for the field and
   affects rollout reporting only.
3. **Unique release names and IngressClasses.** Forced by three controllers side
   by side.
4. **`disable-access-log: "true"` on ingress-nginx.** Deliberate, and it makes
   ingress-nginx *faster*. At the chart default nginx writes a log line for every
   request it forwards and ramjet-ingress writes none, so leaving it on would
   charge one contender for work its opponent never does. It also keeps the
   controller's log readable, which matters because that log is where reloads are
   counted.
5. **`podSecurityContext.seccompProfile.type: Unconfined` on the uring release
   only.** Not a tuning choice: it is the switch that lets the reactor start at
   all, and it is half of what separates the two ramjet arms.

Nothing else in `controller.config` is set — `worker-processes auto`,
`keep-alive 75`, `worker-connections 16384` and
`upstream-keepalive-connections 320` are all the chart's own values — and no
tuning is applied to ramjet-ingress that its chart does not ship.

### The provisioning asymmetry, which favours ingress-nginx

Carried over unchanged. ramjet-ingress runs under the
`resources.limits.memory: 256Mi` its own chart imposes; **ingress-nginx runs with
no memory ceiling at all**, because its chart ships none. It also runs an
admission webhook validating every Ingress write, which is a real cost of the
default install and is left in.

## Known unfairness that could not be eliminated

- **Nothing is CPU-pinned and the load generator is on the box.** The Phase 16
  shape, kept so these numbers sit on the same axis as that phase's. It is the
  largest caveat on this page and it compresses every ratio here.
- **A burstable instance.** Steal is reported per window and never exceeded
  1.1%, but CPU credit balance is not observable from inside the guest without
  the AWS API, which this phase deliberately did not use.
- **Three controllers watching, one measured.** Symmetric — each pays to watch
  and discard the others' objects — but it is not an idle cluster.
- **The two ramjet arms differ in seccomp profile as well as engine**, so the
  +13% is "stock versus opted-in", not the engine's own margin.
- **One node.** No cross-node hop, no kube-proxy load balancing across nodes, no
  scheduler behaviour.
- **Two rounds of churn, not four.** Enough to reverse the arm order once, which
  is what the confound needed; not enough to put a confidence interval on a 1%
  CPU difference.
- **No 500-route or idle-memory benchmark.** `RESULTS.md` benchmarks 3 and 4
  were out of scope here and their findings are neither confirmed nor challenged
  by this run.

## Verdict

**The thesis transfers.** Off the VM, on a different kernel and a different
Kubernetes distribution, a configuration change still costs ramjet-ingress 1% of
its CPU per request, zero connections and zero requests, while the equivalent
stream of reloads costs ingress-nginx every idle keep-alive connection it holds,
12% more CPU per request, and 0.13% of its requests. The margin that Phase 16
showed the VM had inflated — the uring engine's — is not this one: **the
connection-survival result is 100 against 0 in both environments.**

**And the correction runs the other way on the supporting claim.** ingress-nginx
does not reload for endpoint changes, keeps every connection when it does not,
and — with the ordering confound finally removed — spends **1%** more CPU on that
path rather than the 25% the VM run reported. If a cluster's churn is scaling and
rolling deployments rather than Ingress edits, **the reload argument does not
describe it and there is no CPU argument left standing in its place either.**

## Reproducing

```sh
# On the k0s node, with helm, oha and a kubectl that reaches the cluster:
bench/thesis/ec2/setup.sh          # three controllers, backends, gates, engine check
bench/thesis/ec2/run-all.sh        # ~50 minutes: steady state, churn, propagation
bench/thesis/ec2/teardown.sh       # remove everything, and verify it
python3 bench/thesis/ec2/report.py # re-render every table from committed JSON
```

Raw output for every window is in [`results-ec2/`](./results-ec2/) — oha's JSON,
`vmstat` for the steal column, the idle-connection event log, the churn
timestamps and the controller cgroup deltas — so every table above can be
re-derived without re-running anything. One artifact is abridged: the sequential
timeline probe's per-request series is compacted to every request of 10 ms or
more verbatim, which is everything the stall columns count, plus a per-second
count-and-max envelope for the rest. Percentiles were computed from the complete
series before compaction.

Versions of everything are in
[`results-ec2/versions.txt`](./results-ec2/versions.txt).
