# Two engines and nginx, on the same two cores

**The uring engine is ahead of nginx by 44.7% at the median, by at least 31%
against every round measured beside it, and by at least 28% on the most
conservative pairing this repository can make — and it halves the latency the
proxy hop adds.** The hyper engine and nginx are level, which is where
`bench/RESULTS.md` left them.

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

`bench/RESULTS.md` holds two committed head-to-head runs, and the comparison has
to take **both cells from the same one**. An earlier version of this section did
not: it paired this run's nginx against one table and its baseline against the
other, and the resulting ratio agreed "to a tenth of a percentage point". That
agreement was an artifact of the mixing. Both tables, paired consistently:

| | this run | 4f58bd7, after optimization | d1c08c6, first measurement |
|---|---:|---:|---:|
| nginx, absolute | 80,790 | 86,670 | 89,593 |
| baseline, absolute | 229,902 | 229,400 | 247,875 |
| **nginx as % of baseline** | **35.1%** | **37.8%** | **36.1%** |

The comparator is **4f58bd7**. It is the run `bench/RESULTS.md` itself names as
the better estimate of the gap, and it is already the run this page cites below
for the hyper engine being level with nginx — taking this check from a different
table than that one is exactly the mistake being corrected.

Against it the ratio holds to **2.6 percentage points**, about 7% of itself;
against the older run, to 1.0 points. So the ratio does travel across days, but
at the **few-percent level**, not to a tenth of a point. That is enough to say
this run's ordering and margins mean something. It is not enough to treat an
absolute row here as interchangeable with another day's.

**The slowdown was not uniform, and the corrected pairing is what shows it.**
This session's baseline is within **0.2%** of 4f58bd7's — the machine was not
broadly slower — while its nginx is **6.8%** lower and its hyper engine 6.1%
lower. Whatever cost the two TCP proxies those points did not cost the no-proxy
baseline anything, so it cannot be waved off as a busy host, and the earlier
"uniform 7% slowdown" reading of it was wrong.

**That direction matters for how the headline is read.** nginx's row here is the
low one, so **+44.7% is the optimistic end of the margin rather than the middle
of it.** Comparing uring's *worst* round (111,250) against the best committed
nginx median (86,670) gives **+28%**; against this session's own best nginx round
(84,862) it gives +31%. **At least 28% ahead** is the claim that survives every
one of those pairings, and +44.7% is what the median reads when nginx is measured
in the same session on the same host, which is the comparison this harness is
built to make.

Absolute RPS figures on this page should still be read as **floors** for the
contenders measured together here, and a quieter host would move them up. What
the corrected numbers withdraw is the stronger claim that the whole machine was
uniformly 7% down and that every row therefore scales by one factor.

The same check settles a question about the other engine. `bench/RESULTS.md` has
the hyper engine level with nginx (0.9% apart). Here it is **0.13% apart**
— 80,682 against 80,790, both at 35.1% of baseline. The two moved together: both
sit about 6% below their 4f58bd7 figures, which is the same effect the paragraphs
above describe. So the hyper row is not under-measured **relative to nginx**,
which is what this argument needs, and **+44.9% for uring over hyper is the same
result as +44.7%
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

That 31% is a **within-session** floor: it compares uring's worst round to the
best nginx round measured beside it. The cross-day check above gives a more
conservative one — against nginx's best *committed* median, from a session where
it read about 7% higher, the same worst uring round is **28%** ahead. Both are
floors; 28% is the one that assumes this session's nginx was having a bad day.

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
- **The two engines are not feature-equivalent.** At the time of this run the
  uring engine served HTTP/1.1 plaintext and nothing else — no TLS, no HTTP/2,
  no upgrades, no Kubernetes mode. None of that is exercised by any contender on
  this workload, so the comparison is like for like *for this traffic*. It was
  not a claim that the engines were interchangeable.

  Most of that gap has since closed; see the TLS and tunnel section below and
  `docs/src/operations/engines.md` for what is left. The numbers in *this*
  section were taken against the engine as it was, and are left as they were
  measured.
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
- Not that the uring engine is ready to deploy. **At the time of this run** it
  had no TLS, no HTTP/2, no upgrades and no Kubernetes mode, and was one phase
  old against a data plane that had already been through a profiling pass and a
  benchmark rewrite. That list is shorter now — TLS, upgrades, the PROXY
  protocol, mirroring and Kubernetes mode all landed, and HTTP/2 is served by
  dispatch — and what remains is graceful drain. The section below measures the
  engine that closed them.
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

---

# TLS, and a tunnel: the same three contenders with crypto in the path

Everything above is plaintext HTTP/1.1, which is what the uring engine could
serve when it was written. It terminates TLS now, carries WebSocket upgrades,
and speaks HTTP/2 by handing those connections to the other engine. This section
adds two measurements and changes none of the ones above.

Same machine, same topology, same pinning, same rotation and the same guard
rules. What is new is a certificate and, for the tunnel, a different shape of
work entirely.

## Why TLS is a separate question

The plaintext thesis is about syscalls: four per request, submitted into a ring
instead of made one at a time. Under TLS that thesis is **diluted on purpose**.
The record layer adds arithmetic no amount of batching removes, and the
plaintext engine's zero-copy relay is gone — rustls has to read plaintext out of
one buffer and write ciphertext into another, so there is a mandatory copy in
each direction.

So the honest question is not "does io_uring still win by the same margin". It
is whether the engine is still ahead once crypto is in the path, which is the
shape almost all real ingress traffic has.

## What was made fair, and what that cost

The one setting that decides a TLS benchmark is **session resumption**. nginx
ships `ssl_session_tickets on` and every deployment turns on
`ssl_session_cache`. A run against a ramjet with resumption off would be
measuring a configuration nobody deploys and would flatter nginx by exactly the
cost of a signature per connection.

So both sides resume. `nginx-tls.conf` sets a shared cache and tickets;
ramjet's rustls configuration gained a ticketer for both of its lanes, which is
a change this benchmark forced rather than found.

The rest:

- **One certificate, generated per run**, mounted into all three containers.
  ECDSA P-256 — what a modern deployment uses, and what both sides are fastest
  at. An RSA-2048 key would have made handshake cost dominate and turned this
  into OpenSSL against *ring* rather than the engines around them.
- **HTTP/1.1 on all three.** The uring container runs `--no-h2-dispatch`, so it
  advertises `http/1.1` alone and starts no second engine competing for the same
  two cores. The correctness gate checks with `curl --http1.1` — which is what
  oha does — that every contender serves HTTP/1.1 to an HTTP/1.1 client. An
  earlier version of that gate used curl's default, offered `h2`, and failed on
  the hyper contender. That failure is the reason the flag is there rather than
  the protocol being assumed.
- **`--engine uring-strict`, not `uring`.** With the fallback available, a
  blocked `io_uring_setup` would have made this benchmark compare hyper with
  hyper and report it as an engine result. Strict mode dies instead, so a
  container that is up and answering is a container on io_uring.

## TLS: keep-alive

30s per run, three rounds, median by throughput, ECDSA P-256, resumption on
both sides.

| c=64 | rps | p50 ms | p99 ms | spread | errors |
|---|---:|---:|---:|---:|---:|
| ramjet (hyper) | 82,735 | 0.68 | 2.48 | 2.2% | 0 |
| **ramjet (uring)** | **107,920** | **0.51** | 2.38 | 11.0% | 0 |
| nginx | 76,531 | 0.76 | 2.40 | 1.0% | 0 |

| c=256 | rps | p50 ms | p99 ms | spread | errors |
|---|---:|---:|---:|---:|---:|
| ramjet (hyper) | 81,265 | 2.96 | 6.24 | 19.8% | 0 |
| **ramjet (uring)** | **104,188** | **2.14** | 7.02 | 5.4% | 0 |
| nginx | 68,758 | 3.38 | 8.92 | 13.4% | 0 |

**uring against hyper: 1.30x at c=64, 1.28x at c=256. Against nginx: 1.41x and
1.52x.**

The margin over hyper survives TLS almost intact — the plaintext run measured
the same engines a similar distance apart — which is the result worth having.
Crypto is added work, but it is added to *both* ramjet contenders equally, and
what separates them is still how the bytes reach the record layer rather than
what happens inside it.

Two numbers to discount. The hyper contender's 19.8% spread at c=256 is the
widest in this file and means its three runs disagreed enough that the median is
doing real work; the direction is safe, the third significant figure is not. And
the uring engine's p99 at c=256 is *worse* than hyper's (7.02ms against 6.24ms)
while its median is better — it is finishing more requests faster and a few
requests slower, which is what a batching reactor does under queueing.

## TLS: a new connection per request

`--disable-keepalive`, so every request pays for a handshake. A different
question from the one above and worth asking separately: under keep-alive the
handshake amortises to nothing, and this is what a rolling deployment or a CDN
reconnecting actually does to a replica.

| | conn/s | p50 ms | p99 ms | spread | errors |
|---|---:|---:|---:|---:|---:|
| ramjet (hyper) | 12,412 | 5.09 | 12.62 | 3.5% | 0 |
| **ramjet (uring)** | **14,436** | **4.25** | **11.25** | 6.7% | 0 |
| nginx | 5,736 | 10.68 | 25.86 | 4.2% | 0 |

**uring against hyper: 1.16x. Against nginx: 2.52x.**

The gap between the two engines narrows here, and it should: a handshake is
arithmetic, not syscalls, and there is proportionally less I/O per connection to
batch. The gap against nginx widens for the opposite reason — this is *ring*
against OpenSSL as much as it is one proxy against another, and the 2.52x should
be read as a statement about two TLS stacks, not two I/O models.

## WebSocket tunnels: where the engines are level

A different shape of work from every other measurement here. After a 101 there
is no request, no route lookup, no header rewriting and no upstream pool: one
connection in, one connection out, and bytes moved between them. `wsload/` is a
small client written for this, with no dependencies, one echo in flight per
connection — so the latency below is a round trip a real message would have
experienced rather than a number produced by pipelining.

64 tunnels, 128-byte payloads, 20s per run, three rounds.

| | echo/s | p50 us | p99 us | spread | errors |
|---|---:|---:|---:|---:|---:|
| ramjet (hyper) | 103,422 | 595 | 1509 | 1.4% | 0 |
| ramjet (uring) | 105,271 | 585 | 1374 | 1.2% | 0 |
| nginx | 102,843 | 571 | 1619 | 7.1% | 0 |

**All three are within 2.4% of each other, and that is the finding.**

It is not a disappointing result, it is the expected one, and it is worth
stating plainly because a reader who saw 1.30x on the TLS table would otherwise
expect it here. A tunnel with one message in flight is one read and one write
per echo, on each side, with nothing to batch and nothing to overlap: the
request rate is bounded by round-trip latency rather than by how many times the
kernel is entered. The uring engine's advantage is *submission batching*, and
there is nothing to batch when the client will not send the next message until
it has the answer to this one.

Where a difference would be expected is many messages in flight per connection,
or many more tunnels than cores. Neither is measured here, and neither should be
inferred from this table.

The one column that does separate them is p99: 1374us against nginx's 1619us,
with nginx's spread at 7.1% against ramjet's 1.2-1.4%. Both engines are steadier
under this load than nginx is, which is a smaller claim than a throughput win
and a real one.

## What is not claimed, for these three tables

- Not that the TLS margin transfers off this VM. See the standing note above:
  this is a macOS Docker Desktop guest, syscalls are dearer here than on bare
  metal, and io_uring's advantage is the cost it avoids. **Treat 1.30x as an
  upper bound and the direction as the transferable claim.**
- Not that ramjet's TLS stack beats nginx's by 2.5x in general. The handshake
  table is *ring* against OpenSSL with one key type on one architecture.
- Not that WebSocket performance is identical in general. One message in flight,
  one payload size, 64 tunnels on two cores.
- Not that the two engines are interchangeable. The uring engine still does not
  drain gracefully on `SIGTERM`, and it speaks HTTP/2 only by handing those
  connections to the other engine. `docs/src/operations/engines.md` has the
  parity matrix.

## Reproducing these three

```sh
./bench/engine/tls-run.sh                          # ~35 minutes
./bench/engine/ws-run.sh                           # ~5 minutes
SMOKE=1 ./bench/engine/tls-run.sh                  # validate the harness
python3 bench/engine/tls-report.py                 # re-render, no re-run
```

Raw JSON and version manifests are committed under `results-tls/` and
`results-ws/`, so every table above can be re-derived without re-running
anything. The generated certificate is not committed and is not meant to be:
`tls-run.sh` makes a fresh one, which is why there is no key in this repository
and no expiry date in a fixture.
