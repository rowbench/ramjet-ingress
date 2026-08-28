# How the engine benchmark is run

Split out of `RESULTS.md` so the method can be read without the numbers and the
numbers can be re-read without the method. `RESULTS.md` links here rather than
repeating it.

## The question

`bench/PROFILE.md` ended by naming a ceiling and the one thing that could get
under it:

> 59.4% of a request is the four unavoidable syscalls, and another 9.1% is
> finding out a socket is ready. That is the floor for this design, and it is
> not a hyper problem or a tokio problem — it is the I/O model. Getting under
> it means fewer syscalls per request, which on Linux means `io_uring`.

So: does it? `crates/ramjet-engine` submits those four operations into a ring
and enters the kernel once for a batch of them. This benchmark is the only place
that can answer, because macOS has no io_uring — the native harness can measure
what the new state machine costs but never what the ring buys.

## Topology

`bench/run.sh`'s, with a third contender. Everything that made those numbers
comparable is kept.

```
                      ┌──────────────────────┐
   oha (cores 4-7) ──▶│ contender (cores 0,1)│──▶ up1 (core 2)
                      └──────────────────────┘──▶ up2 (core 3)
```

Four targets, measured one at a time, all on the same pair of upstreams:

| Target | What it is |
|---|---|
| `ramjet-hyper` | `ramjet-ingressd --engine hyper` |
| `ramjet-uring` | `ramjet-ingressd --engine uring` |
| `nginx` | nginx as a reverse proxy |
| `baseline` | an upstream on the proxies' own cores, no hop at all |

Both ramjet containers are the **same image** with one flag different, so
nothing about the build can differ between them. The baseline is pinned to the
proxies' cores rather than its own, so the cost of the hop is a like-for-like
subtraction.

Containers, network and image are namespaced `ramjet-engine-*` on subnet
`172.31.98.0/24`, which cannot collide with `bench/run.sh`'s `ramjet-bench-*` or
the Kubernetes suite's `ramjet-thesis-*`. `run.sh` also refuses to start while
either of those is running: sharing the pinned cores with another benchmark
would produce noise wearing a table's clothes.

## Seccomp

Docker's default seccomp profile decides whether `io_uring_setup` is permitted,
and the answer has changed between Docker versions — moby allowed the three
io_uring syscalls by default up to v24 and removed them afterwards. Rather than
depend on which side of that line the host happens to be on, both ramjet
containers run with `seccomp-uring.json`: moby v24.0.7's default profile with
`io_uring_setup`, `io_uring_enter` and `io_uring_register` hoisted into their own
explicit allow entry, so the difference from a stock profile is one legible
block rather than three names buried in a list of 424.

**Both** ramjet containers get it, not only the uring one. Applying a different
security profile to one contender and not the other would be a topology
asymmetry, and this benchmark's whole claim is that the two differ in one thing.

nginx does not get it, and that is worth stating plainly: nginx runs under
Docker's stock default. The profile is a superset of what nginx needs, so this
cannot disadvantage it, but it is a difference between contenders and it is
recorded here rather than left for a reader to discover.

## Is io_uring actually being used?

There is no silent fallback to hide behind. On Linux the engine's reactor is
io_uring and nothing else — `PlatformDriver` is `UringDriver` — so a blocked
`io_uring_setup` fails `UringDriver::new()`, the serving core returns an error,
and the process exits at startup. **If the uring container answers a single
request, io_uring is working.** `verify_uring` checks the container is still up
and that its startup banner names the engine, and the correctness gate then makes
it serve real requests before anything is measured.

The `syscall_evidence` pass goes further and asks the runtime for its own
counters (`RAMJET_URING_STATS`), which report submissions against ring entries —
the ratio that is the entire thesis. That pass runs after the measured rounds,
never during them.

## Method

- A discarded warmup at the same concurrency before **every** measured run.
- `ROUNDS` (default 3) interleaved rounds of `DURATION` (default 30s) at c64:
  hyper, uring, nginx, baseline, then round two, and so on. Never all of one and
  then all of another, so drift is shared rather than handed to whoever went
  second.
- One round at c256.
- Each c64 row is the **median-throughput run**, not a per-column average.
  Averaging percentiles across runs would invent a latency profile that no
  single 30-second measurement actually produced.
- `--cpuset-cpus` rather than `--cpus`: a quota is invisible to
  `sched_getaffinity`, so a process would start threads for cores it cannot use.
  `verify_topology` asserts all three contenders see the same CPU count.
- Error budget 0.1%. `report.py` folds oha's `statusCodeDistribution` non-200s
  and its `errorDistribution` into one count and exits non-zero if any run
  exceeds it, so a fast run that was dropping requests cannot be quoted.

## Correctness before speed

Nothing is measured until every contender has been shown to be doing the same
job:

- all four return the same 128-byte body;
- ramjet on both engines and nginx return 200 for the routed host;
- **all three return 404 for an unrouted Host**, which proves host routing is
  being exercised rather than passed through;
- the two engines' response headers are compared field by field, `Date`
  excluded, and any difference is reported.

That last one is specific to this benchmark. Two engines that serve at different
speeds because one of them is doing less work is not a result, and comparing the
headers is the cheapest way to notice.

## Warmup, and why the smoke test is not a measurement

Each core warms its own connection pool, timer set and buffers. That is more
state than a single shared runtime has, so a short warmup measures a contender
that is still filling it — and it penalises whichever engine has the most
per-core state to fill, which is precisely the one under test.

The signature is unmistakable once you know it: **throughput going up with
concurrency.** A smoke run measuring 39,000 rps at c64 and 72,549 at c128
immediately after has not discovered that the proxy likes load; it has measured
one cold run and one warm one.

So the committed protocol warms for 10 seconds, `SMOKE=1` warms for 8, and the
script warns before printing anything if the warmup has been overridden to
single digits. A cold first run read as a regression is a mistake this harness
should make impossible rather than merely unlikely.

## Reading a difference

The run-to-run spread table is the reader's evidence about what the measurement
can resolve. A gap smaller than the spread of either side is reported as
"inside the noise", and that phrasing is deliberate: it does not mean the two
are equal, it means **this benchmark cannot tell them apart**, and the honest
statement is "the same".

## Reproducing

```sh
./bench/engine/run.sh                                   # ~15 minutes
SMOKE=1 ./bench/engine/run.sh                            # validate the harness
python3 bench/engine/report.py                           # re-render from raw JSON
```

The raw oha JSON is committed, so every table can be re-derived without
re-running anything.
