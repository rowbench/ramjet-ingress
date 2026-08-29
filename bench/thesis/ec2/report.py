#!/usr/bin/env python3
"""Render every table in RESULTS-EC2.md from the committed JSON in results/.

Nothing here recomputes a measurement; it reads what the benchmarks wrote and
formats it, so a table can be re-derived without re-running anything.
"""

import json
import os
import statistics
import sys

ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "results-ec2")

NAMES = {
    "baseline": "backend, no proxy",
    "hyper": "ramjet (hyper)",
    "uring": "ramjet (uring)",
    "nginx": "ingress-nginx",
}


def load(sub, pattern_end="-meta.json"):
    d = os.path.join(ROOT, sub)
    out = {}
    if not os.path.isdir(d):
        return out
    for f in sorted(os.listdir(d)):
        if f.endswith(pattern_end):
            out[f[: -len(pattern_end)]] = json.load(open(os.path.join(d, f)))
    return out


def med(xs):
    return statistics.median(xs) if xs else None


def spread(xs):
    return (max(xs) - min(xs)) / statistics.median(xs) * 100 if len(xs) > 1 else 0.0


# ---------------------------------------------------------------------------


def steady(tag):
    rows = load("s1")
    order = ["baseline", "hyper", "uring", "nginx"]
    per = {c: [] for c in order}
    for k, m in rows.items():
        if not k.startswith(tag + "-"):
            continue
        c = k[len(tag) + 1 :].rsplit("-r", 1)[0]
        if c in per:
            per[c].append(m)

    base = med([m["rps"] for m in per["baseline"]]) if per["baseline"] else None
    print(f"\n#### Steady-state forwarding, {tag} "
          f"({len(per['hyper'])} x 30 s per contender, interleaved)\n")
    print("| Contender | RPS (median) | spread | % of baseline | p50 | p99 | "
          "Controller CPU per request | Controller memory | mean steal |")
    print("|---|---:|---:|---:|---:|---:|---:|---:|---:|")
    for c in order:
        ms = per[c]
        if not ms:
            continue
        rps = [m["rps"] for m in ms]
        cpus = [m["cpu_us_per_request"] for m in ms if m["cpu_us_per_request"]]
        mems = [m["controller_mem_bytes_end"] for m in ms if m["controller_mem_bytes_end"]]
        steals = []
        for m in ms:
            for part in m["vmstat"].split():
                if part.startswith("steal="):
                    steals.append(float(part.split("=")[1]))
        pct = f"{med(rps) / base * 100:.1f}%" if base else "—"
        print(f"| {NAMES[c]} | {med(rps):,.0f} | {spread(rps):.1f}% | {pct} | "
              f"{med([m['p50_ms'] for m in ms]):.2f} ms | {med([m['p99_ms'] for m in ms]):.2f} ms | "
              f"{f'{med(cpus):.0f} us' if cpus else '—'} | "
              f"{f'{med(mems) / 2**20:.1f} MiB' if mems else '—'} | "
              f"{med(steals):.2f}% |")

    errs = {c: sum(m["non_2xx"] + m["errors"] for m in per[c]) for c in order if per[c]}
    print(f"\nNon-2xx plus transport errors across every window: "
          + ", ".join(f"{NAMES[c]} {n}" for c, n in errs.items()) + ".")

    # A median is shaky on a box that drifts, and this one is burstable. A
    # rank-order claim is not: if every round of one contender beat every round
    # of another, the ranges do not overlap and the ordering is a fact about the
    # measurement rather than about which round got the warmer machine.
    if len(per["hyper"]) > 1:
        print("\n#### The claim that survives the drift\n")
        print("| Comparison | worst round of the first | best round of the second | verdict |")
        print("|---|---:|---:|---|")
        for a, b in (("hyper", "nginx"), ("uring", "nginx"), ("uring", "hyper")):
            if not per[a] or not per[b]:
                continue
            wa, bb = min(m["rps"] for m in per[a]), max(m["rps"] for m in per[b])
            verdict = (f"ranges disjoint, {NAMES[a]} ahead by {(wa - bb) / bb * 100:.0f}% at worst"
                       if wa > bb else "ranges overlap — no rank-order claim")
            print(f"| {NAMES[a]} vs {NAMES[b]} | {wa:,.0f} | {bb:,.0f} | {verdict} |")


def churn():
    rows = load("s2")
    contenders = ["hyper", "nginx"]
    arms = ["baseline", "spec", "endpoint"]
    per = {(c, a): [] for c in contenders for a in arms}
    for k, m in rows.items():
        c, a, _ = k.split("-")
        if (c, a) in per:
            per[(c, a)].append(m)

    print("\n#### Throughput and latency under churn (oha, c64, "
          f"{len(per[('hyper','spec')])} runs per cell)\n")
    print("| Contender | Arm | RPS (median) | vs own baseline | p50 | p99 | p99.9 | "
          "HTTP errors | mean steal |")
    print("|---|---|---:|---:|---:|---:|---:|---:|---:|")
    for c in contenders:
        b = med([m["rps"] for m in per[(c, "baseline")]])
        for a in arms:
            ms = per[(c, a)]
            if not ms:
                continue
            r = med([m["rps"] for m in ms])
            steals = [float(p.split("=")[1]) for m in ms for p in m["vmstat"].split()
                      if p.startswith("steal=")]
            delta = "—" if a == "baseline" else f"{(r - b) / b * 100:+.1f}%"
            p999 = med([m["p999_ms"] for m in ms if m.get("p999_ms")])
            print(f"| {NAMES[c]} | {a} | {r:,.0f} | {delta} | "
                  f"{med([m['p50_ms'] for m in ms]):.2f} ms | {med([m['p99_ms'] for m in ms]):.2f} ms | "
                  f"{p999:.1f} ms | {sum(m['non_2xx'] + m['errors'] for m in ms)} | "
                  f"{med(steals):.2f}% |")

    print("\n#### Idle keep-alive connections that survived the window\n")
    print("| Contender | Arm | Held | Survived | Lost | Config events the controller applied |")
    print("|---|---|---:|---:|---:|---:|")
    for c in contenders:
        for a in arms:
            ms = per[(c, a)]
            if not ms:
                continue
            held = sum(m["idle_held"] for m in ms)
            surv = sum(m["idle_survived"] for m in ms)
            ev = sum(m["config_events_applied_by_controller"] for m in ms)
            print(f"| {NAMES[c]} | {a} | {held} | {surv} | {held - surv} | {ev} |")

    print("\n#### Controller cost of the churn window\n")
    print("| Contender | Arm | Pod CPU-seconds | Pod CPU per request | vs own baseline | "
          "Pod memory at end |")
    print("|---|---|---:|---:|---:|---:|")
    for c in contenders:
        bc = med([m["cpu_us_per_request"] for m in per[(c, "baseline")]])
        for a in arms:
            ms = per[(c, a)]
            if not ms:
                continue
            cu = med([m["cpu_us_per_request"] for m in ms])
            delta = "—" if a == "baseline" else f"{(cu - bc) / bc * 100:+.0f}%"
            print(f"| {NAMES[c]} | {a} | {med([m['controller_cpu_seconds'] for m in ms]):.1f} s | "
                  f"{cu:.1f} us | {delta} | "
                  f"{med([m['controller_mem_bytes_end'] for m in ms]) / 2**20:.1f} MiB |")

    print("\n#### The single-connection timeline (every request, not a percentile)\n")
    print("| Contender | Arm | Requests | Errors | p50 | p99 | > 50 ms | > 200 ms | > 1 s | "
          "Worst single request |")
    print("|---|---|---:|---:|---:|---:|---:|---:|---:|---:|")
    for c in contenders:
        for a in arms:
            ms = per[(c, a)]
            if not ms:
                continue
            print(f"| {NAMES[c]} | {a} | {sum(m['timeline_requests'] for m in ms):,} | "
                  f"{sum(m['timeline_errors'] for m in ms)} | "
                  f"{med([m['timeline_p50_us'] for m in ms]) / 1e3:.2f} ms | "
                  f"{med([m['timeline_p99_us'] for m in ms]) / 1e3:.2f} ms | "
                  f"{sum(m['stalls']['gt_50ms'] for m in ms)} | "
                  f"{sum(m['stalls']['gt_200ms'] for m in ms)} | "
                  f"{sum(m['stalls']['gt_1s'] for m in ms)} | "
                  f"{max(m['timeline_max_us'] for m in ms) / 1e3:,.0f} ms |")


def propagation():
    d = os.path.join(ROOT, "s3")
    if not os.path.isdir(d):
        return
    per = {}
    for f in sorted(os.listdir(d)):
        if not f.endswith(".json"):
            continue
        c = f.split("-")[0]
        per.setdefault(c, []).append(json.load(open(os.path.join(d, f))))

    print("\n#### Apply -> served, new Ingress, no other load (milliseconds)\n")
    print("| Contender | Trials | Median | p95 | Min | Max | Median `kubectl apply` |")
    print("|---|---:|---:|---:|---:|---:|---:|")
    for c in ["hyper", "uring", "nginx"]:
        rs = per.get(c) or []
        if not rs:
            continue
        s = sorted(r["serve_ms"] for r in rs if r["serve_ms"])
        a = [r["apply_ms"] for r in rs]
        p95 = s[min(len(s) - 1, int(len(s) * 0.95))]
        print(f"| {NAMES[c]} | {len(s)} | {med(s):,.0f} | {p95:,.0f} | {min(s):,.0f} | "
              f"{max(s):,.0f} | {med(a):,.0f} |")


if __name__ == "__main__":
    which = sys.argv[1] if len(sys.argv) > 1 else "all"
    if which in ("all", "s1"):
        steady("c64")
        steady("c256")
    if which in ("all", "s2"):
        churn()
    if which in ("all", "s3"):
        propagation()
