#!/usr/bin/env python3
"""Turn the raw oha JSON in results-tls/ into the tables in RESULTS.md.

A separate script from report.py rather than a flag on it, for two reasons that
are both about not being able to quote the wrong number:

  * there is no baseline row. The plaintext benchmark subtracts a no-proxy
    endpoint to get the cost of the hop; over TLS a "no proxy" endpoint would
    itself be terminating TLS, which makes it a fourth contender rather than a
    floor. So the TLS tables report throughput and latency and do not claim to
    have isolated the hop.

  * there is a scenario report.py has no shape for: a new connection per
    request, which measures handshakes rather than requests and is reported in
    conn/s.

The arithmetic is report.py's, unchanged: median run selected by throughput, one
folded error count, spread shown across the repeated runs. Changing how a number
is computed at the same time as adding what is measured is how a benchmark stops
being comparable to the one before it.
"""

import json
import pathlib
import statistics
import sys

RESULTS = (
    pathlib.Path(sys.argv[1])
    if len(sys.argv) > 1
    else pathlib.Path(__file__).parent / "results-tls"
)

CONTENDERS = [
    ("ramjet (hyper)", "ramjet-hyper"),
    ("ramjet (uring)", "ramjet-uring"),
    ("nginx", "nginx"),
]


def load(path):
    if not path.exists() or path.stat().st_size == 0:
        return None
    try:
        return json.loads(path.read_text())
    except json.JSONDecodeError:
        return None


def runs(stem, suffix):
    """Every complete run for one contender in one scenario."""
    out = []
    for run in range(1, 21):
        doc = load(RESULTS / f"{stem}-{suffix}-r{run}.json")
        if doc is not None:
            out.append(doc)
    return out


def concurrencies():
    levels = set()
    for path in RESULTS.glob("*-c*-r*.json"):
        stem = path.stem
        marker = stem.rfind("-c")
        end = stem.rfind("-r")
        if marker != -1 and end > marker:
            try:
                levels.add(int(stem[marker + 2 : end]))
            except ValueError:
                pass
    return sorted(levels)


def errors(doc):
    """Everything that was not a 2xx or 3xx, folded into one number.

    A benchmark where one contender is quietly failing a slice of its requests
    is faster than one that is not, and the difference does not show up in the
    throughput column.
    """
    summary = doc.get("summary", {})
    total = 0
    for code, count in doc.get("statusCodeDistribution", {}).items():
        if not str(code).startswith(("2", "3")):
            total += count
    for count in doc.get("errorDistribution", {}).values():
        total += count
    total += summary.get("errorRate", 0) and 0  # errorRate is a ratio, not a count
    return total


def rps(doc):
    return doc["summary"]["requestsPerSec"]


def median_run(docs):
    """The run whose throughput is the median, not a median of every column.

    Averaging p99 across runs invents a latency distribution nothing produced.
    """
    ordered = sorted(docs, key=rps)
    return ordered[len(ordered) // 2]


def fmt(value):
    return f"{value:,.0f}"


def latency(doc, key):
    """One latency percentile, in milliseconds."""
    value = doc.get("latencyPercentiles", {}).get(key)
    return f"{value * 1000:.2f}" if value is not None else "-"


def table(scenario_suffix, unit, title):
    rows = []
    for label, stem in CONTENDERS:
        docs = runs(stem, scenario_suffix)
        if not docs:
            continue
        best = median_run(docs)
        spread = ""
        if len(docs) > 1:
            values = [rps(d) for d in docs]
            spread = f"{(max(values) - min(values)) / statistics.mean(values) * 100:.1f}%"
        rows.append(
            (
                label,
                fmt(rps(best)),
                latency(best, "p50"),
                latency(best, "p99"),
                spread,
                str(sum(errors(d) for d in docs)),
                len(docs),
            )
        )
    if not rows:
        return

    print(f"\n### {title}\n")
    print(f"| | {unit} | p50 ms | p99 ms | spread | errors |")
    print("|---|---:|---:|---:|---:|---:|")
    for label, throughput, p50, p99, spread, errs, _ in rows:
        print(f"| {label} | {throughput} | {p50} | {p99} | {spread} | {errs} |")

    # The comparison the reader is actually here for: the two engines differ in
    # one thing, so the ratio between them is the engine's effect and nothing
    # else's.
    by_label = {r[0]: r for r in rows}
    if "ramjet (uring)" in by_label and "ramjet (hyper)" in by_label:
        uring = float(by_label["ramjet (uring)"][1].replace(",", ""))
        hyper = float(by_label["ramjet (hyper)"][1].replace(",", ""))
        print(f"\nuring against hyper: **{uring / hyper:.2f}x**", end="")
        if "nginx" in by_label:
            nginx = float(by_label["nginx"][1].replace(",", ""))
            print(f" — uring against nginx: **{uring / nginx:.2f}x**", end="")
        print(f", over {rows[0][6]} runs.")


def main():
    if not RESULTS.exists():
        print(f"no results in {RESULTS}", file=sys.stderr)
        return 1

    versions = RESULTS / "versions.txt"
    if versions.exists():
        print("```text")
        print(versions.read_text().strip())
        print("```")

    for conc in concurrencies():
        table(f"c{conc}", "rps", f"Keep-alive, concurrency {conc}")
    table("handshake", "conn/s", "New connection per request")
    return 0


if __name__ == "__main__":
    sys.exit(main())
