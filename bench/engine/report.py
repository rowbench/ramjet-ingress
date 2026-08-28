#!/usr/bin/env python3
"""Turn the raw oha JSON in results/ into the table in RESULTS.md.

bench/report.py with a third contender. The arithmetic is deliberately
unchanged — median selection by throughput, one folded error count, spread
across the repeated runs — because changing how a number is computed at the
same time as adding the thing being measured is how a benchmark stops being
comparable to the one before it.

The one addition is the head-to-head section: with two engines that share
everything except how they move bytes, the interesting column is not "who is
fastest" but "what did changing the engine do", and that is a division this
script can do once rather than a subtraction every reader does by hand.
"""

import json
import pathlib
import statistics
import sys

RESULTS = pathlib.Path(__file__).parent / "results"

# Display name -> filename stem. Order here is the order of the table rows.
CONTENDERS = [
    ("ramjet (hyper)", "ramjet-hyper"),
    ("ramjet (uring)", "ramjet-uring"),
    ("nginx", "nginx"),
    ("baseline (no proxy)", "baseline"),
]

# The rows the proxy-hop section covers: everything that is actually a proxy.
PROXIES = CONTENDERS[:3]

BASELINE = CONTENDERS[-1][0]


def load(stem, conc, run):
    path = RESULTS / f"{stem}-c{conc}-r{run}.json"
    if not path.exists():
        return None
    with path.open() as handle:
        return json.load(handle)


def runs_for(stem, conc):
    """Every run recorded for one contender at one concurrency, in order."""
    found = []
    for run in range(1, 32):
        doc = load(stem, conc, run)
        if doc is None:
            break
        found.append(doc)
    return found


def concurrencies():
    """Concurrency levels present in results/, discovered rather than assumed."""
    levels = set()
    for path in RESULTS.glob("*-c*-r*.json"):
        try:
            levels.add(int(path.stem.rsplit("-c", 1)[1].split("-r")[0]))
        except (IndexError, ValueError):
            continue
    return sorted(levels)


def accounting(doc):
    """Requests, and everything that was not a 200, from one oha run.

    oha splits outcomes across two maps: statusCodeDistribution counts
    responses that arrived, errorDistribution counts exchanges that never
    produced one. Both are folded into a single error count rather than
    reported separately and quietly ignored.
    """
    status = doc.get("statusCodeDistribution", {})
    errors = doc.get("errorDistribution", {})
    ok = status.get("200", 0)
    other_status = sum(v for k, v in status.items() if k != "200")
    transport = sum(errors.values())
    total = ok + other_status + transport
    return total, ok, other_status + transport, errors


def row(doc):
    us = lambda key: doc["latencyPercentiles"][key] * 1_000_000
    return {
        "rps": doc["summary"]["requestsPerSec"],
        "p50": us("p50"),
        "p90": us("p90"),
        "p99": us("p99"),
        "p999": us("p99.9"),
    }


def median_run(docs):
    """The median-RPS run, not a per-column median.

    Averaging percentiles across runs would invent a latency profile that no
    single run actually produced.
    """
    ranked = sorted(docs, key=lambda d: d["summary"]["requestsPerSec"])
    return ranked[len(ranked) // 2]


def fmt(value):
    return f"{value:,.0f}"


def main():
    problems = []
    table = {}

    levels = concurrencies()
    if not levels:
        sys.exit("no results found; run ./run.sh first")

    main_level = max(levels, key=lambda c: len(runs_for(CONTENDERS[0][1], c)))

    for label, stem in CONTENDERS:
        for conc in levels:
            docs = runs_for(stem, conc)
            if not docs:
                continue
            for n, doc in enumerate(docs, 1):
                total, _ok, bad, raw = accounting(doc)
                rate = bad / total if total else 1.0
                if rate > 0.001:
                    problems.append(
                        f"{stem} c{conc} run{n}: {bad}/{total} ({rate:.3%}) {raw}"
                    )
            table[(label, conc)] = row(median_run(docs))
            table[(label, conc, "rps_all")] = [
                d["summary"]["requestsPerSec"] for d in docs
            ]

    lines = []
    for conc in levels:
        count = len(runs_for(CONTENDERS[0][1], conc))
        note = f"median of {count} runs" if count > 1 else "single run"
        lines.append(f"### Concurrency {conc} ({note})\n")
        lines.append("| Contender | RPS | p50 | p90 | p99 | p99.9 |")
        lines.append("|---|---:|---:|---:|---:|---:|")
        for label, _ in CONTENDERS:
            r = table.get((label, conc))
            if not r:
                continue
            lines.append(
                f"| {label} | {fmt(r['rps'])} | {fmt(r['p50'])} us | {fmt(r['p90'])} us "
                f"| {fmt(r['p99'])} us | {fmt(r['p999'])} us |"
            )
        lines.append("")

    n_runs = len(runs_for(CONTENDERS[0][1], main_level))
    lines.append(f"### Run-to-run spread (c{main_level} RPS, every run)\n")
    lines.append(
        "| Contender | " + " | ".join(f"run {i}" for i in range(1, n_runs + 1)) + " | spread |"
    )
    lines.append("|---" * (n_runs + 2) + "|")
    spreads = {}
    for label, _ in CONTENDERS:
        vals = table.get((label, main_level, "rps_all"))
        if not vals:
            continue
        spread = (max(vals) - min(vals)) / statistics.mean(vals)
        spreads[label] = spread
        cells = " | ".join(fmt(v) for v in vals)
        lines.append(f"| {label} | {cells} | {spread:.1%} |")
    lines.append("")

    base = table.get((BASELINE, main_level))
    if base:
        lines.append(f"### Added latency of the proxy hop (c{main_level}, vs baseline)\n")
        lines.append("| Contender | added p50 | added p99 | RPS vs baseline |")
        lines.append("|---|---:|---:|---:|")
        for label, _ in PROXIES:
            r = table.get((label, main_level))
            if not r:
                continue
            lines.append(
                f"| {label} | +{fmt(r['p50'] - base['p50'])} us | "
                f"+{fmt(r['p99'] - base['p99'])} us | "
                f"{r['rps'] / base['rps']:.0%} |"
            )
        lines.append("")

    # The head-to-head. Two engines, one difference, and the honest reading of
    # whether that difference is bigger than the measurement can resolve.
    lines.append("### Engine to engine, and against nginx\n")
    lines.append("| Concurrency | uring vs hyper | uring vs nginx | verdict |")
    lines.append("|---|---:|---:|---|")
    for conc in levels:
        hyper = table.get(("ramjet (hyper)", conc))
        uring = table.get(("ramjet (uring)", conc))
        nginx = table.get(("nginx", conc))
        if not (hyper and uring):
            continue
        vs_hyper = (uring["rps"] / hyper["rps"] - 1) * 100
        vs_nginx = (uring["rps"] / nginx["rps"] - 1) * 100 if nginx else float("nan")
        # A difference smaller than the run-to-run spread of either side is not
        # a result. Saying so here stops a reader having to cross-reference the
        # spread table to find out whether a number means anything.
        noise = max(
            spreads.get("ramjet (hyper)", 0.0),
            spreads.get("ramjet (uring)", 0.0),
        ) * 100
        if conc == main_level and abs(vs_hyper) < noise:
            verdict = f"inside the noise ({noise:.1f}% spread)"
        elif conc != main_level:
            verdict = "single run, not median"
        elif vs_hyper > 0:
            verdict = "uring ahead"
        else:
            verdict = "hyper ahead"
        lines.append(
            f"| c{conc} | {vs_hyper:+.1f}% | {vs_nginx:+.1f}% | {verdict} |"
        )
    lines.append("")

    # Machine-state gate. The baseline is a plain nginx serving a static body
    # with no proxy hop at all: it has no moving parts and nothing under test.
    # If *it* moves between rounds, the machine moved, and every other number
    # here is measuring the machine rather than the contenders. On a laptop the
    # usual cause is the package heating up over a long run.
    base_runs = table.get((BASELINE, main_level, "rps_all"))
    if base_runs and len(base_runs) > 1:
        drift = (max(base_runs) - min(base_runs)) / statistics.mean(base_runs)
        lines.append("### Was the machine steady?\n")
        lines.append(
            f"Baseline (no proxy) across the {len(base_runs)} rounds: "
            + ", ".join(fmt(v) for v in base_runs)
            + f" rps — {drift:.1%} spread.\n"
        )
        if drift <= 0.15:
            lines.append(
                "Under 15%, so the contenders were measured against a stable "
                "machine.\n"
            )
        else:
            lines.append(
                f"**The machine moved by {drift:.1%} between rounds**, which is more "
                "than this harness should have to tolerate. Whether that invalidates "
                "a given comparison depends on whether the comparison is larger than "
                "the drift, so the rank-order check below decides it rather than a "
                "single threshold.\n"
            )

        # Drift makes a *median* untrustworthy, because a median mixes rounds
        # measured under different conditions. It does not touch a rank-order
        # statement: if one contender's worst round still beats another's best,
        # no amount of drift within the measured range can be what put it there.
        # That is the claim worth making when the machine will not sit still.
        uring = table.get(("ramjet (uring)", main_level, "rps_all"))
        if uring:
            lines.append("### The comparison that drift cannot reach\n")
            lines.append("| Comparison | worst uring round | best rival round | verdict |")
            lines.append("|---|---:|---:|---|")
            clean = True
            for rival in ("ramjet (hyper)", "nginx"):
                other = table.get((rival, main_level, "rps_all"))
                if not other:
                    continue
                worst, best = min(uring), max(other)
                if worst > best:
                    verdict = f"uring ahead by {(worst / best - 1):.0%} at worst"
                else:
                    verdict = "**overlaps — not separable**"
                    clean = False
                lines.append(
                    f"| uring vs {rival} | {fmt(worst)} | {fmt(best)} | {verdict} |"
                )
            lines.append("")
            if clean:
                lines.append(
                    "Every measured uring round beat every measured round of both "
                    "rivals. The ranges do not overlap, so the ordering is a fact "
                    "about the contenders and not about the machine — whatever the "
                    "drift did to the exact figures.\n"
                )
            else:
                problems.append(
                    "contender ranges overlap; the ordering is not separable from "
                    f"the {drift:.1%} baseline drift"
                )

    out = "\n".join(lines)
    print(out)

    if problems:
        print("\n<!-- THIS RUN FAILED A TRUST CHECK -->")
        for p in problems:
            print(f"<!-- {p} -->")
        sys.stderr.write("\nRUN NOT TRUSTWORTHY:\n" + "\n".join(problems) + "\n")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
