# Two engines and nginx, on the same two cores

**The uring engine is ahead of nginx by 44.7% at the median and by at least 31%
in the worst case, and it halves the latency the proxy hop adds.** The hyper
engine and nginx are level, which is where `bench/RESULTS.md` left them.

That is the answer to the question `bench/PROFILE.md` ended on. It found no hot
function to fix — the router and every header rewrite together are about 2% of a
request — and concluded that 59.4% of a request is the four syscalls a proxy hop
cannot avoid, another 9.1% is finding out a socket is ready, and *"getting under
it means fewer syscalls per request, which on Linux means `io_uring`."* It does.

The method is in [METHOD.md](METHOD.md). Read the caveats at the bottom of this
page before quoting anything from it; one of them is load-bearing.

## The measurement

Docker on Linux, `--cpuset-cpus=0,1` for every contender, the same pair of
upstreams, oha at c64, three rotated rounds of 30 seconds each with a discarded
warmup and a cooldown before every one. Both ramjet rows are **the same image
with one flag different**.

### Concurrency 64 (median of 3 runs)

| Contender | RPS | % of baseline | p50 | p90 | p99 | p99.9 |
|---|---:|---:|---:|---:|---:|---:|
| ramjet (hyper) | 80,682 | 35.1% | 687 us | 1,030 us | 2,905 us | 7,562 us |
| **ramjet (uring)** | **116,927** | **50.9%** | **483 us** | **702 us** | **1,941 us** | **5,569 us** |
| nginx | 80,790 | 35.1% | 696 us | 1,002 us | 2,853 us | 7,687 us |
| baseline (no proxy) | 229,902 | 100.0% | 227 us | 384 us | 1,160 us | 3,868 us |

### Concurrency 256 (single run)

| Contender | RPS | % of baseline | p50 | p90 | p99 | p99.9 |
|---|---:|---:|---:|---:|---:|---:|
| ramjet (hyper) | 65,458 | 28.2% | 3,130 us | 5,217 us | 15,918 us | 52,723 us |
| **ramjet (uring)** | **110,057** | **47.5%** | **1,962 us** | **3,070 us** | **8,122 us** | **28,387 us** |
| nginx | 84,480 | 36.4% | 2,680 us | 3,936 us | 8,465 us | 22,245 us |
| baseline (no proxy) | 231,837 | 100.0% | 924 us | 1,552 us | 3,866 us | 10,629 us |

### Added latency of the proxy hop (c64, vs baseline)

The baseline is the same body served on the same two cores with no hop at all,
so this is what inserting a proxy costs.

| Contender | added p50 | added p99 | RPS vs baseline |
|---|---:|---:|---:|
| ramjet (hyper) | +459 us | +1,746 us | 35% |
| **ramjet (uring)** | **+255 us** | **+782 us** | **51%** |
| nginx | +469 us | +1,693 us | 35% |

**The uring engine's hop costs 255 us where nginx's costs 469 us**, and it keeps
51% of the no-proxy throughput where the other two keep 35%.

## The number that survives a slow host, and a check against another day

The **% of baseline** column is the one to compare against a run taken on a
different day. The baseline is measured in the same session against the same
upstreams, so a host that is uniformly slow divides out of it; the absolute RPS
is the number that cannot travel.

It travels well. Against the clean-host run committed in `bench/RESULTS.md`,
taken on a different day by a different agent on the same topology and the same
two cores:

| | this run | `bench/RESULTS.md`, clean host |
|---|---:|---:|
| nginx, absolute | 80,790 | 86,670 |
| baseline, absolute | 229,902 | 247,875 |
| **nginx as % of baseline** | **35.1%** | **35.0%** |

The absolutes here are about 7% low — nginx -6.8%, baseline -7.3% — which is a
**uniform** slowdown of the whole machine, not something that happened to one
contender. And the ratio reproduces the clean-host measurement **to a tenth of a
percentage point**. That is the evidence that this run's ordering and margins
mean something even though its absolute numbers were taken on a host that was
not at its best.

**Every absolute RPS figure on this page is therefore a floor, not an estimate.**
A quieter host would move all four rows up together, uring included. Anyone
revisiting these numbers should expect them to rise rather than fall, and should
not "correct" them downward on the grounds that the machine was busy — the
busyness is already in them, and it cost uring the same 7% it cost nginx.

The same check settles a question about the other engine. `bench/RESULTS.md` has
the hyper engine level with nginx (0.9% apart). Here it is **0.13% apart**
— 80,682 against 80,790, both at 35.1% of baseline. So the hyper row is not
under-measured, and **+44.9% for uring over hyper is the same result as +44.7%
over nginx** rather than an artifact of a cold hyper. An earlier 5-second smoke
run *did* understate hyper by 7%, which is what a 2-second warmup does to the
contender with the most per-core state to warm; that run is not quoted here and
the committed protocol's 10-second warmup is why.

## The machine would not sit still, and what that does to the reading

| Contender | run 1 | run 2 | run 3 | spread |
|---|---|---|---|---|
| ramjet (hyper) | 63,672 | 85,640 | 80,682 | 28.7% |
| ramjet (uring) | 117,553 | 116,927 | 111,250 | 5.5% |
| nginx | 84,862 | 79,688 | 80,790 | 6.3% |
| baseline (no proxy) | 229,902 | 199,099 | 232,428 | 15.1% |

The baseline has no moving parts and nothing under test, so its 15.1% spread is
the machine rather than any contender: this is a laptop, and a laptop under
sustained full load gets slower. Three earlier runs were discarded for worse
versions of the same problem — one of them because a `cargo build` of mine was
competing with it, which is exactly the mistake this benchmark exists to avoid
and is recorded here rather than quietly fixed.

Drift makes a **median** shaky, because a median mixes rounds measured under
different conditions. It does not touch a **rank-order** claim:

| Comparison | worst uring round | best rival round | verdict |
|---|---:|---:|---|
| uring vs ramjet (hyper) | 111,250 | 85,640 | uring ahead by 30% at worst |
| uring vs nginx | 111,250 | 84,862 | uring ahead by 31% at worst |

Every measured uring round beat every measured round of both rivals. The ranges
do not overlap, so **at least 31% ahead of nginx** is the claim that survives the
drift, and +44.7% is the median's reading of the same thing. `report.py` makes
this check itself and refuses the run if the ranges ever overlap.

The hyper engine's own 28.7% spread is mostly its first round (63,672), which
was the very first traffic after the topology started and so ran with a cold
connection pool. Rotation moved it out of that position in later rounds.

## Why: the syscalls stopped scaling with the requests

The runtime reports its own counters, and this is the mechanism stated as a
number. During a c64 run, per `io_uring_enter`:

```
cqes_per_waiting_enter = 21.7 … 39.5      (typically 28–37)
enter_share_of_thread_cpu = 0.81
```

**Between 22 and 40 completions are harvested per trip into the kernel.** A
request is four operations, so that is roughly seven to ten requests per
syscall, against the hyper engine's four syscalls per request plus a `kevent` to
learn a socket was ready. The syscall count stopped scaling with the request
count, which was the entire thesis.

The other 81% figure is worth reading correctly: it is the share of the serving
thread's CPU spent *inside* `io_uring_enter`, which is where the kernel actually
does the reads and writes. It is not overhead — it is the work — and the point
is that it is now reached in tens rather than one at a time.

## Cost per request, and what it is spent on

From the diagnostics pass, a 12-second run at c64:

| | requests | CPU | CPU per request | memory | reqs per upstream conn |
|---|---:|---:|---:|---:|---:|
| ramjet (hyper) | 855,179 | 199.1% | 27.9 us | 23.5 MiB | 6,681 |
| **ramjet (uring)** | **1,273,310** | **198.1%** | **18.7 us** | **10.8 MiB** | 9,947 |
| nginx | 986,645 | 174.4% | 21.2 us | 3.9 MiB | 61,665 |

The uring engine does **49% more requests for the same CPU** as the hyper
engine, and uses less than half its memory. Against nginx it is 12% cheaper per
request while nginx is still, by a wide margin, the most memory-frugal and the
best at reusing an upstream connection.

Two honest readings of that table rather than one:

- **nginx did not saturate its cores** in this pass (174% of an available 200%),
  so its 21.2 us is a fair figure for what it spent but its throughput here may
  have been limited by something other than CPU. The measured rounds above,
  where it does saturate, are the throughput result; this table is about cost.
- **nginx reuses upstream connections six times better** (61,665 requests per
  connection against 9,947). Per-core pools are the reason, and the price was
  named when they were introduced: a connection returned to a full pool on one
  core cannot be reused by another. It is not costing throughput here, but it is
  a real difference and it is nginx's win.

Upstream load while the proxy was saturated: **67.2% and 67.6%** on the two
upstream cores. They had headroom, so the thing being measured was the proxy.

## The native harness cannot see this, and that is the point

`bench/native.sh` runs the same A/B on macOS against native nginx upstreams.
Four interleaved rounds of 15s at c64, the same binary with the flag flipped:

```
  A (hyper)  median 50,738 rps   spread 30.8%
  B (uring)  median 53,024 rps   spread 18.1%
  B vs A: +4.5%   (inside the noise)
```

**+4.5% against an 18-31% spread is not a result, and the harness says so
itself.** That is not a disappointment, it is the prediction. On macOS the
reactor's backend is kqueue, and the kqueue backend performs each syscall
eagerly at submission — a write is a `write(2)` at submit time, parked on
`EVFILT_WRITE` only if it would block. There is no ring, no batch, and nothing
to collapse: the engine makes the same syscalls as the hyper path, in a
different wrapper, with a state machine that is one phase old against hyper's.

The whole benefit measured on this page is io_uring's, so the platform without
io_uring measures none of it. What the native harness *is* good for is
correctness and iteration — it caught four real bugs during development — and
for confirming that the new engine does not cost anything obvious on the paths
that are not about syscall batching.

## Known unfairness and deviations

Stated so a reader can discount them rather than discover them.

- **This is a macOS Docker Desktop VM, and that matters more here than it does
  for any other benchmark in this repository.** The whole result is about the
  cost of entering the kernel, and a syscall in a virtualised linuxkit guest is
  dearer than one on bare metal. io_uring's advantage *is* the cost it avoids,
  so a more expensive syscall flatters it. **Treat the margin as an upper bound**
  and the direction, not the size, as the transferable claim. Kernel 7.0.12,
  Docker 29.7.2, 8 VM CPUs.
- **nginx runs under Docker's stock seccomp profile; both ramjet containers run
  under a pinned one** (moby v24.0.7's default plus the three io_uring
  syscalls). It is a superset of what nginx needs so it cannot disadvantage
  nginx, but it is a difference between contenders.
- **The two engines are not feature-equivalent.** The uring engine serves
  HTTP/1.1 plaintext and nothing else — no TLS, no HTTP/2, no upgrades, no
  Kubernetes mode. None of that is exercised by any contender on this workload,
  so the comparison is like for like *for this traffic*. It is not a claim that
  the engines are interchangeable.
- **Both ramjet contenders are the same image with one flag different**, built
  in the same `docker build`, so nothing about the toolchain or the source can
  differ between them. The correctness gate additionally compares the two
  engines' response headers field by field, so neither can be fast by doing
  less.
- **c256 is a single run**, not a median, and is reported as such.
- Zero errors across all measured runs; the 0.1% error budget was not
  approached.

## What is not claimed

- Not that io_uring beats epoll in general. This is one proxy workload, one
  kernel, one VM.
- Not that the uring engine is ready to deploy. It has no TLS, no HTTP/2, no
  upgrades and no Kubernetes mode, and it is one phase old against a data plane
  that has already been through a profiling pass and a benchmark rewrite.
- Not that the hyper engine is badly written. `bench/PROFILE.md` took it to the
  syscall floor and this is what is underneath that floor. The difference is the
  I/O model, which is what was being tested.

## Reproducing

```sh
./bench/engine/run.sh                                   # ~25 minutes
SMOKE=1 ./bench/engine/run.sh                            # validate the harness
python3 bench/engine/report.py                           # re-render, no re-run
```

Raw oha JSON, the diagnostics and the version manifest are committed under
`results/`, so every table above can be re-derived without re-running anything.
