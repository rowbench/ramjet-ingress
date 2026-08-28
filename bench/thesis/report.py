#!/usr/bin/env python3
"""Render the tables in RESULTS.md from the raw JSON in results/.

Every table this prints comes from committed raw output, so the document can be
re-derived without re-running anything — and so a reader can check any number in
it against the file it came from.
"""

import glob
import json
import os
import statistics
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
# Overridable so a subset of the raw output can be rendered on its own — which
# is how the contended rounds were separated from the headline ones.
RES = os.environ.get("THESIS_RESULTS", os.path.join(HERE, "results"))


def load(pattern):
    """Every parseable JSON file matching the pattern, keyed by basename.

    Unparseable files are skipped rather than fatal: a run still in flight has
    an empty output file open, and being able to render the tables so far is
    more useful than refusing to render any of them.
    """
    out = {}
    for p in sorted(glob.glob(os.path.join(RES, pattern))):
        try:
            out[os.path.basename(p)] = json.load(open(p))
        except (json.JSONDecodeError, OSError) as exc:
            print(f"<!-- skipped {os.path.basename(p)}: {exc} -->", file=sys.stderr)
    return out


def med(xs):
    return statistics.median(xs) if xs else None


def pct(xs, p):
    if not xs:
        return None
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(round(p / 100 * (len(xs) - 1))))]


def fmt(x, unit="", digits=0):
    if x is None:
        return "—"
    return f"{x:,.{digits}f}{unit}"


# ---------------------------------------------------------------------------
# Benchmark 1
# ---------------------------------------------------------------------------


def b1(dirname="b1"):
    rows = {}
    for name, d in load(f"{dirname}/*-oha.json").items():
        contender, arm, _ = name[:-9].split("-", 2)
        rows.setdefault((contender, arm), []).append(d)
    tl = {}
    for name, d in load(f"{dirname}/*-timeline.json").items():
        contender, arm, _ = name[:-14].split("-", 2)
        tl.setdefault((contender, arm), []).append(d)
    idle = {}
    for name, d in load(f"{dirname}/*-idle.json").items():
        contender, arm, _ = name[:-10].split("-", 2)
        idle.setdefault((contender, arm), []).append(d)
    ctl = {}
    for name, d in load(f"{dirname}/*-controller.json").items():
        contender, arm, _ = name[:-16].split("-", 2)
        ctl.setdefault((contender, arm), []).append(d)

    if not rows:
        return

    print("### Throughput and latency under churn (oha, c64, "
          f"{len(next(iter(rows.values())))} runs per cell)\n")
    print("| Contender | Arm | RPS (median of runs) | vs own baseline | p50 | p99 | p99.9 | HTTP errors |")
    print("|---|---|---:|---:|---:|---:|---:|---:|")
    base = {}
    for (c, a), ds in sorted(rows.items()):
        if a == "baseline":
            base[c] = med([d["summary"]["requestsPerSec"] for d in ds])
    for c in ("ramjet", "nginx"):
        for a in ("baseline", "spec", "endpoint"):
            ds = rows.get((c, a))
            if not ds:
                continue
            rps = med([d["summary"]["requestsPerSec"] for d in ds])
            delta = "—" if a == "baseline" else f"{(rps / base[c] - 1) * 100:+.1f}%"
            errs = sum(
                sum(v for k, v in d["statusCodeDistribution"].items() if not k.startswith("2"))
                + sum(d.get("errorDistribution", {}).values())
                for d in ds
            )
            print(f"| {c} | {a} | {fmt(rps)} | {delta} | "
                  f"{fmt(med([d['latencyPercentiles']['p50'] * 1e3 for d in ds]), ' ms', 2)} | "
                  f"{fmt(med([d['latencyPercentiles']['p99'] * 1e3 for d in ds]), ' ms', 1)} | "
                  f"{fmt(med([d['latencyPercentiles']['p99.9'] * 1e3 for d in ds]), ' ms', 1)} | "
                  f"{errs} |")

    print("\n### Idle keep-alive connections that survived the window\n")
    print("| Contender | Arm | Held | Survived | Lost | Config events the controller applied |")
    print("|---|---|---:|---:|---:|---:|")
    for c in ("ramjet", "nginx"):
        for a in ("baseline", "spec", "endpoint"):
            ds = idle.get((c, a))
            if not ds:
                continue
            held = sum(d["held"] for d in ds)
            surv = sum(d["survived"] for d in ds)
            ev = sum(x["config_events_applied_by_controller"] for x in ctl.get((c, a), []))
            print(f"| {c} | {a} | {held} | {surv} | {held - surv} | {ev} |")

    print("\n### The single-connection timeline (every request, not a percentile)\n")
    print("| Contender | Arm | Requests | Errors | p50 | p99 | p99.9 | Worst single request |")
    print("|---|---|---:|---:|---:|---:|---:|---:|")
    for c in ("ramjet", "nginx"):
        for a in ("baseline", "spec", "endpoint"):
            ds = tl.get((c, a))
            if not ds:
                continue
            print(f"| {c} | {a} | {sum(d['requests'] for d in ds):,} | "
                  f"{sum(d['errors'] for d in ds)} | "
                  f"{fmt(med([d['latency_us']['p50'] / 1000 for d in ds]), ' ms', 2)} | "
                  f"{fmt(med([d['latency_us']['p99'] / 1000 for d in ds]), ' ms', 1)} | "
                  f"{fmt(med([d['latency_us']['p999'] / 1000 for d in ds]), ' ms', 1)} | "
                  f"{fmt(max(d['latency_us']['max'] for d in ds) / 1000, ' ms', 0)} |")

    # The percentile table above answers "how bad on average". This one answers
    # "how often did a request visibly stall", which is the question a reload
    # actually raises — and it is computed from the raw per-request series, so
    # it cannot be smeared by a longer window the way a percentile can.
    print("\n### Visible stalls in the sequential stream\n")
    print("| Contender | Arm | Requests | > 50 ms | > 200 ms | > 1 s | Wall time inside a > 50 ms request | Median gap between stalls |")
    print("|---|---|---:|---:|---:|---:|---:|---:|")
    for c in ("ramjet", "nginx"):
        for a in ("baseline", "spec", "endpoint"):
            ds = tl.get((c, a))
            if not ds:
                continue
            # Counts come from the run's own totals rather than from len(samples),
            # because compact.py drops the fast requests from the committed
            # artifact. Everything this table thresholds on (50 ms and above) is
            # retained verbatim, so the slow counts are exact either way.
            n = sum(d["requests"] for d in ds)
            lat = [s[1] for d in ds for s in d["samples"] if isinstance(s[2], int)]
            slow = [x for x in lat if x > 50_000]
            wall = sum(d["duration_s"] for d in ds)
            # The gap between consecutive stalls is the tell. A mutation lands
            # every 2 seconds, so a contender whose stalls are ~2 s apart is
            # stalling *because of the mutation*; one whose stalls are scattered
            # is stalling for unrelated reasons.
            gaps = []
            for d in ds:
                ts = [s[0] for s in d["samples"] if isinstance(s[2], int) and s[1] > 50_000]
                gaps += [b - a2 for a2, b in zip(ts, ts[1:])]
            print(f"| {c} | {a} | {n:,} | {len(slow):,} | "
                  f"{sum(1 for x in lat if x > 200_000):,} | "
                  f"{sum(1 for x in lat if x > 1_000_000):,} | "
                  f"{sum(slow) / 1e6:.1f} s of {wall:.0f} s ({sum(slow) / 1e4 / wall:.1f}%) | "
                  f"{fmt(med(gaps), ' s', 2) if gaps else '—'} |")

    # CPU per request rather than CPU or throughput alone. Throughput on this
    # machine moves with whatever else is running; CPU spent per forwarded
    # request does not, so the "relative to own baseline" column is the one
    # claim here that a busy afternoon cannot distort.
    #
    # The pod's CPU covers the whole observation window including the discarded
    # warmup, so the absolute microseconds are inflated by a constant. The
    # window has exactly the same shape in every arm, so the ratio is not.
    print("\n### Controller cost of the churn window\n")
    print("| Contender | Arm | Pod CPU-seconds | Pod CPU per request | vs own baseline | Pod memory at end |")
    print("|---|---|---:|---:|---:|---:|")
    cpu_per_req = {}
    for c in ("ramjet", "nginx"):
        for a in ("baseline", "spec", "endpoint"):
            ds, rs = ctl.get((c, a)), rows.get((c, a))
            if not ds or not rs:
                continue
            reqs = med([sum(d["statusCodeDistribution"].values()) for d in rs])
            cpu_per_req[(c, a)] = med([d["controller_cpu_seconds"] for d in ds]) / reqs * 1e6
    for c in ("ramjet", "nginx"):
        for a in ("baseline", "spec", "endpoint"):
            ds = ctl.get((c, a))
            if not ds or (c, a) not in cpu_per_req:
                continue
            rel = cpu_per_req[(c, a)] / cpu_per_req[(c, "baseline")]
            print(f"| {c} | {a} | {med([d['controller_cpu_seconds'] for d in ds]):.1f} s | "
                  f"{cpu_per_req[(c, a)]:.1f} us | "
                  f"{'—' if a == 'baseline' else f'{(rel - 1) * 100:+.0f}%'} | "
                  f"{med([d['controller_mem_bytes_end'] for d in ds]) / 1048576:.1f} MiB |")


# ---------------------------------------------------------------------------
# Benchmark 2
# ---------------------------------------------------------------------------


def b2():
    data = load("b2/*.json")
    if not data:
        return
    groups = {}
    for name, d in data.items():
        contender, shape, _ = name[:-5].split("-")
        groups.setdefault((contender, shape), []).append(d)

    print("### Apply -> served, no other load (milliseconds)\n")
    print("| Contender | Change | Trials | Median | p95 | Min | Max | Median `kubectl apply` |")
    print("|---|---|---:|---:|---:|---:|---:|---:|")
    for c in ("ramjet", "nginx"):
        for shape, label in (("new", "new Ingress"), ("mutate", "backend swap")):
            ds = groups.get((c, shape))
            if not ds:
                continue
            served = [d["serve_ms"] for d in ds if d["serve_ms"] is not None]
            print(f"| {c} | {label} | {len(ds)} | {fmt(med(served), '', 0)} | "
                  f"{fmt(pct(served, 95), '', 0)} | {fmt(min(served), '', 0)} | "
                  f"{fmt(max(served), '', 0)} | {fmt(med([d['apply_ms'] for d in ds]), '', 0)} |")


# ---------------------------------------------------------------------------
# Benchmark 3
# ---------------------------------------------------------------------------


def b3():
    loads = load("b3/*-load.json")
    if not loads:
        return
    print("### Loading the routes\n")
    print("| Contender | Ingresses created | `kubectl apply` wall time | Apply -> last route served | Controller CPU | Controller memory before -> after | nginx reloads |")
    print("|---|---:|---:|---:|---:|---:|---:|")
    for c in ("ramjet", "nginx"):
        d = loads.get(f"{c}-load.json")
        if not d:
            continue
        conv = f"{d['converge_ms'] / 1000:.1f} s" if d["converge_ms"] else "TIMED OUT"
        print(f"| {c} | {d['routes_created']}/{d['routes_requested']} | {d['apply_ms'] / 1000:.1f} s | {conv} | "
              f"{d['controller_cpu_seconds']:.0f} s | "
              f"{d['controller_mem_bytes_before'] / 1048576:.1f} -> {d['controller_mem_bytes_after'] / 1048576:.1f} MiB | "
              f"{d['nginx_reloads_during_load'] if c == 'nginx' else '—'} |")

    at = {}
    for name, d in load("b3/*-atscale-*.json").items():
        if name.endswith("-oha.json"):
            continue
        at.setdefault(name.split("-")[0], []).append(d)
    if at:
        print("\n### Propagation of a new Ingress with the routes already loaded\n")
        print("| Contender | Trials | Median | p95 | Min | Max | Median with an empty cluster (benchmark 2) |")
        print("|---|---:|---:|---:|---:|---:|---:|")
        b2data = load("b2/*-new-*.json")
        for c in ("ramjet", "nginx"):
            ds = at.get(c)
            if not ds:
                continue
            served = [d["serve_ms"] for d in ds if d["serve_ms"] is not None]
            empty = [d["serve_ms"] for n, d in b2data.items() if n.startswith(c) and d["serve_ms"]]
            print(f"| {c} | {len(ds)} | {fmt(med(served))} ms | {fmt(pct(served, 95))} ms | "
                  f"{fmt(min(served))} ms | {fmt(max(served))} ms | {fmt(med(empty))} ms |")

    oha = load("b3/*-atscale-oha.json")
    if oha:
        print("\n### Forwarding on the stable route with the routes loaded (c64, 30s)\n")
        print("| Contender | RPS | p50 | p99 |")
        print("|---|---:|---:|---:|")
        for c in ("ramjet", "nginx"):
            d = oha.get(f"{c}-atscale-oha.json")
            if not d:
                continue
            print(f"| {c} | {d['summary']['requestsPerSec']:,.0f} | "
                  f"{d['latencyPercentiles']['p50'] * 1e3:.2f} ms | "
                  f"{d['latencyPercentiles']['p99'] * 1e3:.1f} ms |")


# ---------------------------------------------------------------------------
# Benchmark 4
# ---------------------------------------------------------------------------


def b4():
    data = load("b4/*.json")
    if not data:
        return
    print("### Container memory across 10,000 idle keep-alive connections\n")
    print("| Contender | Pass | Established | Idle before | At 10k | After close | Per connection | Retained |")
    print("|---|---|---:|---:|---:|---:|---:|---:|")
    for c in ("ramjet", "nginx"):
        for p in ("pass1", "pass2"):
            d = data.get(f"{c}-{p}.json")
            if not d:
                continue
            print(f"| {c} | {p[-1]} | {d['established']:,}/{d['requested']:,} | "
                  f"{d['mem_before_bytes'] / 1048576:.1f} MiB | "
                  f"{d['mem_at_peak_bytes'] / 1048576:.1f} MiB | "
                  f"{d['mem_after_close_bytes'] / 1048576:.1f} MiB | "
                  f"{fmt(d['bytes_per_connection'] / 1024, ' KiB', 1) if d['bytes_per_connection'] else '—'} | "
                  f"{d['retained_after_close_bytes'] / 1048576:+.1f} MiB |")


def b4_after():
    """Benchmark 4 re-run against the image that fixed it.

    Rendered from `results/b4-after/` and printed beside the original rather
    than instead of it: the first measurement is what the fix was judged
    against, and a table that quietly replaced it would leave the claim
    unfalsifiable.
    """
    data = load("b4-after/*.json")
    if not data:
        return
    print("### Container memory across 10,000 idle keep-alive connections, after the fix\n")
    print("| Contender | Pass | Established | Idle before | At 10k | After close | Per connection | Retained |")
    print("|---|---|---:|---:|---:|---:|---:|---:|")
    for c in ("ramjet", "nginx"):
        for p in ("pass1", "pass2"):
            d = data.get(f"{c}-{p}.json")
            if not d:
                continue
            print(f"| {c} | {p[-1]} | {d['established']:,}/{d['requested']:,} | "
                  f"{d['mem_before_bytes'] / 1048576:.1f} MiB | "
                  f"{d['mem_at_peak_bytes'] / 1048576:.1f} MiB | "
                  f"{d['mem_after_close_bytes'] / 1048576:.1f} MiB | "
                  f"{fmt(d['bytes_per_connection'] / 1024, ' KiB', 1) if d['bytes_per_connection'] else '—'} | "
                  f"{d['retained_after_close_bytes'] / 1048576:+.1f} MiB |")


def b1_contended():
    """Rounds 3 and 4, which are kept apart from the headline table on purpose.

    They were measured while another agent's proxy benchmark was running six
    containers on the same docker daemon: every contender's throughput fell to
    roughly a quarter, so folding these runs into the same medians would have
    averaged two different machines. What they are still good for is the part
    that does not depend on how fast the machine is — whether a reload happened,
    and whether idle connections survived it.
    """
    b1("b1-contended")


if __name__ == "__main__":
    which = sys.argv[1] if len(sys.argv) > 1 else "all"
    stages = (
        ("b1", b1),
        ("b1-contended", b1_contended),
        ("b2", b2),
        ("b3", b3),
        ("b4", b4),
        ("b4-after", b4_after),
    )
    for name, fn in stages:
        if which in ("all", name):
            if not glob.glob(os.path.join(RES, name.split("-")[0] if name == "b1" else name, "*")):
                continue
            print(f"\n<!-- {name} -->\n")
            fn()
