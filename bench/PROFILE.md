# Where ramjet-ingress spends a request

`bench/RESULTS.md` measured ramjet-ingress forwarding at 61.6k rps against
nginx's 89.6k on the same two cores — about **32.5 us of CPU per request
against nginx's 22.3 us**. This page is the profile that went looking for the
10 us, what it found, what was changed, and what is left.

The short version: **the 10 us was not in the forwarding code.** Route matching,
header rewriting, URI building and the metrics counters together account for
about 2% of a request. Roughly two thirds of a request is the four syscalls a
proxy hop cannot avoid, and the largest recoverable cost was the multi-threaded
tokio runtime moving each request's work between cores — 43% more CPU per
request than the same code on one thread. Fixing that recovered
**+7.7%** on the iteration harness and **+39.5%** on the committed benchmark,
which took ramjet-ingress from 69% of nginx's throughput to 99% of it at c64.
What is left is close to a floor, and the last section says why.

## How this was measured

Two harnesses, for two different jobs.

**`bench/run.sh`** is the committed benchmark and the only thing `RESULTS.md`
quotes: docker, Linux, `--cpuset-cpus=0,1`, nginx as the contender, ~11 minutes.
It is the proof, and it is far too slow to iterate against.

**`bench/native.sh`** is the iteration loop this page was written from. It runs
the release binary natively on macOS/arm64 against two native nginx upstreams
serving the same 128-byte body, one route, round-robin, oha with keep-alive at
c64 — the same shape as `run.sh`, without docker. macOS has no `cpuset`, so two
things stand in for it: the proxy is given exactly two threads, and the
upstreams and load generator get the other ten cores. Every run reports the
proxy's CPU against the upstreams' and warns when the proxy was not the thing
that ran out, because a measurement where the upstream was the bottleneck is not
a measurement of the proxy.

Three caveats that matter for reading any number below:

- **The noise floor is 2-4%.** An A/B of the *same binary against itself* over
  three interleaved rounds came out at +0.6% with 2.2-3.9% spread. So
  `native.sh ab` runs A and B alternately for several rounds and reports
  medians, and anything under about 4% is reported here as "inside the noise"
  rather than as a result.
- **Syscalls are dearer on macOS than on Linux.** Two of the costs below —
  `clock_gettime` at 2.6% and part of the syscall total — are cheaper or free on
  Linux, where `clock_gettime` is a vDSO call. The *ranking* transferred to the
  committed benchmark; the percentages are macOS-native and should not be
  quoted as Linux numbers.
- **The profiler is not free.** samply at 3000 Hz cost about 25% of throughput
  and, at that rate, stopped the proxy being the bottleneck at all. The profiles
  below were taken at 999 Hz, where throughput is within 3% of an unprofiled run.

Profiles are CPU-time profiles, weighted by `threadCPUDelta` rather than by
sample count. That distinction is load-bearing: by sample count the accept
thread is 33% of the profile, and it is parked in `kevent` doing nothing.

## Before: where the 32.5 us went

Release binary with debug info, two tokio worker threads, c64, 67.0k rps at
175% of two cores — 26.1 us of CPU per request on this host. By module:

| Module | Share of CPU |
|---|---:|
| `libsystem_kernel` (syscalls) | 68.5% |
| `ramjet-ingressd` (our code + hyper + tokio, inlined together) | 21.8% |
| `libsystem_malloc` | 5.0% |
| `libsystem_platform` / `libsystem_pthread` | 4.3% |

The top five leaf costs, by self CPU:

| # | Cost | Self CPU | What it is |
|---|---|---:|---|
| 1 | `writev` | **30.6%** | 15.4% writing the request upstream, 15.2% writing the response downstream |
| 2 | `read` | **25.1%** | 12.9% reading the upstream response, 12.2% reading the downstream request |
| 3 | `kevent` | **4.1%** | the tokio io driver parking and polling for readiness |
| 4 | `psynch_cvwait` | **3.3%** | tokio worker threads parking and being woken |
| 5 | `clock_gettime` | **2.6%** | 1.5% our upstream-latency histogram, 0.7% hyper's `Date` header, 0.4% the pool's idle timestamp |

Then `psynch_mutex` at 1.9% — split between hyper-util's connection pool being
checked out (0.7%) and returned (0.9%) — and malloc/free at 5.0%.

**Items 1 and 2 are the floor.** A proxy hop is read the request, write it
upstream, read the response, write it downstream: four syscalls, and nginx makes
the same four. 55.7% of a request is spent in them.

**Our own code is not on this list, and that is the finding.** Adding up every
`ramjet_proxy` frame by inclusive time:

| Function | Inclusive CPU |
|---|---:|
| `upstream::endpoint_uri` (builds and parses a URI per request) | 0.80% |
| `headers::apply_forwarded` (`X-Forwarded-*`) | 0.27% |
| `headers::strip_hop_by_hop` | 0.20% |
| `headers::upgrade_protocol` | 0.14% |
| `forward::select_backend` (the router match itself) | 0.13% |

About 2% of a request, all told, and the router's 25 ns match is 0.1% of it.
`RESULTS.md` guessed this and the profile confirms it: **there was no hot
function to find.** Every candidate in the original brief that lived in this
code — the request-id hex, the `X-Forwarded-For` builder, the per-request URI
parse, the metrics counters — is smaller than the measurement noise, and three
of them were tested directly and are recorded as such below.

## The experiment that found the real cost

If the work is not in the forwarding code and the syscalls are a floor, the
question becomes what the other 30% is. `psynch_cvwait` (3.3%) and
`psynch_mutex` (1.9%) are both *coordination*, not work, which pointed at the
runtime rather than at any function.

So: run the same binary with one tokio worker thread instead of two, and compare
**CPU per request** rather than throughput.

| Workers | Throughput | Proxy CPU | Throughput per core | CPU per request |
|---:|---:|---:|---:|---:|
| 1 | 47.1k rps | 88% | 53.6k rps | **18.7 us** |
| 2 | 68.4k rps | 183% | 37.4k rps | **26.7 us** |

**The same code costs 43% more CPU per request on two threads than on one.**
Repeated across three interleaved rounds, the effect held at 33-43% every time.

That is the shape of a work-stealing scheduler under a request that ping-pongs.
A request arrives on worker A; `Client::request` hands it to the upstream
connection's task, which worker B may own; the response wakes A again from B.
Each crossing is an atomic on a contended cache line and, when the other worker
has parked, a wakeup syscall — which is exactly what `psynch_cvwait` and the
pool mutex were. nginx does not pay this: its workers are shared-nothing
processes, so its per-core cost does not change with worker count.

## What changed

### 1. One `current_thread` runtime per core, shared-nothing (`+7.7%`)

The server used to run one multi-threaded tokio runtime and spawn a task per
connection onto it. It now starts one `current_thread` runtime per core on its
own thread and hands each accepted socket to one of them round-robin. A
connection stays on the runtime it landed on for its whole life, and so does
everything its requests touch: the upstream connections, the pool they come from
and the timers that bound them. The accept loop and the admin listener stay on
the caller's runtime, which is why `ramjet-ingressd`'s own runtime is now
`#[tokio::main(worker_threads = 1)]` — it no longer serves traffic.

Measured, five interleaved rounds against the pre-change binary on the same two
cores: **69,677 -> 75,068 rps, +7.7%**, every round positive (+5.1% to +10.3%)
against a 3-5% spread.

The after-profile shows the mechanism worked: `psynch_mutex` and
`psynch_cvwait` have both dropped out of the top costs entirely, the two serving
threads split the work 50.0%/50.0%, and what is left is 71% syscalls.

Two prices, both documented in `server.rs`: `pool_max_idle_per_host` is now a
per-runtime ceiling rather than a process-wide one, and a single connection can
no longer be spread across cores.

### 2. `--upstream-pool-idle`, default 32 -> 128

`RESULTS.md` measured ramjet opening a new upstream connection every ~590
requests where nginx opened one every ~28,700, and named
`pool_max_idle_per_host: 32` as the cause and the lack of a flag as the
complaint. Both are fixed: the value is now `--upstream-pool-idle` /
`RAMJET_UPSTREAM_POOL_IDLE`, and the default is 128.

The default moved because the two ways of being wrong are not symmetric. The
pool is a **ceiling, not a reservation** — nothing is opened until a request
needs it — so setting it too high costs file descriptors that are never opened,
while setting it below an endpoint's in-flight request count means every request
past the limit closes its connection and the next one pays a TCP handshake on
the request path.

Measured on its own, 32 against 128 over four interleaved rounds at c64:
**-0.5%, inside the noise.** The thread-per-core change had already fixed most
of the churn by splitting one shared pool into one per runtime, which halves
the in-flight count each pool has to cover. The committed benchmark shows what
the pair of changes did together: requests per upstream connection went from
~590 to **8,179** (nginx: 27,082). The remaining 3x is the price of per-runtime
pools — a connection returned to a full pool on one runtime cannot be reused by
the other — and at 0.01% of requests it is below anything this benchmark can
resolve.

### 3. `--worker-threads`

New flag and `RAMJET_WORKER_THREADS` twin, defaulting to one runtime per
available core. `available_parallelism` reads the cgroup limit, so a pod with
`limits.cpu: 2` gets two runtimes rather than one per host core — which is also
what keeps `bench/run.sh`'s `--cpuset-cpus=0,1` honest.

## What the committed benchmark said

`bench/run.sh`, rerun from scratch at commit 4f58bd7, full tables in
[`RESULTS.md`](RESULTS.md):

| | Before | After | nginx |
|---|---:|---:|---:|
| c64 | 61,568 rps | **85,908 rps** (+39.5%) | 86,670 rps |
| c256 | 59,644 rps | **82,524 rps** (+38.4%) | 89,636 rps |
| CPU per request | 32.5 us | **23.6 us** | 23.6 us |
| requests per upstream connection | ~590 | **8,179** | 27,082 |

**The docker benchmark reports +39.5% where the native harness reported +7.7%,
and that discrepancy is worth explaining rather than quietly taking the larger
number.** The two harnesses are not measuring different things so much as
dividing by different denominators. On this macOS host, 68% of a request is
syscall time and syscalls are expensive; on Linux they are cheaper, so the same
absolute saving in runtime coordination is a much larger *share* of a request
there. The native harness was right about the direction and the mechanism, and
understated the size — which is the safer way round for an iteration loop to be
wrong, and the reason the committed benchmark is the one that gets quoted.

The second-order confirmation is in the diagnostics: ramjet and nginx now both
spend 23.6 us of CPU per forwarded request, having been 32.5 and 22.3.

## What was tried and reverted

Every one of these was a plausible lead. All were measured, none survived, and
they are here so nobody spends the afternoon again.

| Tried | Result | Kept? |
|---|---|---|
| **Removing the per-request header clone.** `forward` clones `request::Parts` on every request when more than one endpoint exists, so a connect failure can be retried against another. Measured with `--max-connect-attempts 1`, which skips the clone entirely. | **+0.9%, inside the noise** | No. The clone stays; it buys endpoint failover for less than the noise floor. |
| **Flattened writes instead of vectored** (`http1_writev(false)` on both the server and the client). `writev` is the single largest cost at 30.6%, so the hypothesis was that one `write` of a copied buffer beats a `writev` of two iovecs. | **-0.2%, inside the noise** | No. The syscall dominates; the iovec handling is free either way. |
| **Raising tokio's `event_interval`** from 61 to 512, to poll the io driver less often and cut the 9.1% spent in `kevent`. | **-1.3%, inside the noise** | No. |
| **Per-core sharding / cache-line padding of the metrics counters**, the classic false-sharing fix at 60k+ rps. Tested by a diagnostic build that removed metrics recording *and* the per-request route-table `Arc` refcount — an upper bound on every shared-state fix at once. | **-0.9%, inside the noise** | No, and this is the useful negative: there is nothing to win here, so the sharding was never written. |

The last one is worth dwelling on. It was lead #6 in the brief and it is a real
effect in general — it is simply not a cost *here*, because after the
thread-per-core change the only shared mutable state left on the request path is
a handful of relaxed atomics that two cores touch 75,000 times a second, and
that is not enough traffic on those lines to show up.

## What is left, and the floor

After the changes: 73.0k rps at 176% of two cores on the native harness, 24.1 us
of CPU per request. The profile now reads:

| Cost | Self CPU |
|---|---:|
| `writev` | 31.2% |
| `read` | 28.2% |
| `kevent` | 9.1% |
| `clock_gettime` | 2.2% |
| everything in `ramjet_proxy` | ~1% |

**59.4% of a request is the four unavoidable syscalls, and another 9.1% is
finding out a socket is ready.** That is the floor for this design, and it is
not a hyper problem or a tokio problem — it is the I/O model. Getting under it
means fewer syscalls per request, which on Linux means `io_uring` (batched
submission and completion, no per-readiness `kevent`/`epoll_wait`) and not a
different HTTP library. That is the experiment the project's own reactor was
proposed for, and it is deliberately out of scope here.

Two smaller things remain on the table, both honestly marginal:

- **`endpoint_uri` builds a `String` and parses a `Uri` per request** (0.80%
  inclusive). Caching an `http::uri::Authority` per endpoint and using
  `Uri::from_parts` would remove most of it. It was left alone because 0.8% is
  a fifth of the noise floor on this harness — it cannot be shown to work, and
  a change that cannot be measured is not an optimisation, it is a guess with a
  diff.
- **`clock_gettime` at 2.2%**, two thirds of it our own `Instant::now()` pair
  around the upstream dispatch, feeding `ramjet_upstream_latency_seconds`. This
  is a macOS artifact: the same two calls are vDSO reads on Linux and cost
  roughly nothing there. Removing a documented metric to win back a cost that
  does not exist on the deployment target would be optimising the benchmark
  rather than the program.

The honest ceiling: with the same four syscalls per request as nginx and no
per-request work of consequence left in our own code, what separates the two is
the per-syscall and per-connection overhead of the runtime underneath — and
closing that further is an I/O model change, not a tuning exercise.

On the committed benchmark, that ceiling has already been reached at c64: both
proxies spend 23.6 us of CPU per request and the 0.9% throughput difference is
inside nginx's own 4.5% run-to-run spread. The remaining 9% at c256 is the one
real gap left, and the honest thing to say about it is that it has not been
profiled — every measurement on this page was taken at c64, and the native
harness cannot hold c256 steady enough to be worth reading (four rounds spread
75%). Whether the c256 gap is queueing, the per-runtime pool split, or something
else is an open question, and it is the obvious place for the next pass to
start.

Two smaller costs are now regressions rather than opportunities, and are
recorded in `RESULTS.md` rather than here: memory under load went from 19.2 MiB
to 33.1 MiB (one pool, timer wheel and buffer set per core rather than per
process), and upstream connection reuse, though 14x better, is still 3x short of
nginx's because a connection returned to a full pool on one runtime cannot be
reused by another.
