#!/usr/bin/env python3
"""Turn the raw oha JSON in results/ into the table in RESULTS.md.

Kept separate from run.sh so the numbers can be re-rendered from the committed
raw JSON without re-running a seven-minute benchmark, and so the arithmetic
(median selection, error accounting) is auditable in one place.
"""

import json
import pathlib
import statistics
import sys

# Which directory to render. Defaults to the committed measurement; run.sh
# passes its own results directory, which for anything other than the committed
# protocol is a scratch subdirectory — so a smoke run renders its own numbers
# instead of appearing to restate the committed ones.
RESULTS = pathlib.Path(sys.argv[1]) if len(sys.argv) > 1 else (
    pathlib.Path(__file__).parent / "results"
)

# Display name -> filename stem. Order here is the order of the table rows.
CONTENDERS = [
    ("ramjet-ingress", "ramjet"),
    ("nginx", "nginx"),
    ("baseline (no proxy)", "baseline"),
]


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
    """Concurrency levels present in results/, discovered rather than assumed.

    run.sh takes CONC_MAIN and CONC_HIGH from the environment, so hardcoding
    them here would silently drop a table row whenever somebody overrode one.
    """
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
    produced one. A run is only trustworthy if the second is empty and the
    first contains nothing but 200s, so both are folded into a single
    error count rather than reported separately and quietly ignored.
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
    single run actually produced. Picking the run whose throughput is the
    median keeps every number in the row from one real 30-second measurement.
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

    # The level with repeated runs is the headline one; the rest are single
    # shots. Derived from what is on disk so the two stay in step.
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

    # The spread across the repeated runs is the reader's evidence that the
    # gap between contenders is bigger than the noise floor.
    n_runs = len(runs_for(CONTENDERS[0][1], main_level))
    lines.append(f"### Run-to-run spread (c{main_level} RPS, every run)\n")
    lines.append(
        "| Contender | " + " | ".join(f"run {i}" for i in range(1, n_runs + 1)) + " | spread |"
    )
    lines.append("|---" * (n_runs + 2) + "|")
    for label, _ in CONTENDERS:
        vals = table.get((label, main_level, "rps_all"))
        if not vals:
            continue
        spread = (max(vals) - min(vals)) / statistics.mean(vals)
        cells = " | ".join(fmt(v) for v in vals)
        lines.append(f"| {label} | {cells} | {spread:.1%} |")
    lines.append("")

    # Proxy overhead: the added latency of inserting a hop, against the same
    # two cores serving the same body with no hop at all.
    base = table.get(("baseline (no proxy)", main_level))
    if base:
        lines.append(f"### Added latency of the proxy hop (c{main_level}, vs baseline)\n")
        lines.append("| Contender | added p50 | added p99 | RPS vs baseline |")
        lines.append("|---|---:|---:|---:|")
        for label, _ in CONTENDERS[:2]:
            r = table.get((label, main_level))
            if not r:
                continue
            lines.append(
                f"| {label} | +{fmt(r['p50'] - base['p50'])} us | "
                f"+{fmt(r['p99'] - base['p99'])} us | "
                f"{r['rps'] / base['rps']:.0%} |"
            )
        lines.append("")

    out = "\n".join(lines)
    print(out)

    if problems:
        print("\n<!-- RUNS EXCEEDING THE 0.1% ERROR BUDGET -->")
        for p in problems:
            print(f"<!-- {p} -->")
        sys.stderr.write("\nERROR BUDGET EXCEEDED:\n" + "\n".join(problems) + "\n")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
