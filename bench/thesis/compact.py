#!/usr/bin/env python3
"""Shrink the timeline artifacts so the raw output can be committed.

The timeline probe records every request it makes, which across 24 runs of two
minutes each is 130 MB of mostly identical fast requests — too much to put in a
repository, and none of it load-bearing. What the tables actually read out of
these files is:

  * the summary percentiles, which are computed by probe.py from the COMPLETE
    series before this script ever runs, and are copied through untouched;
  * the request and error counts, likewise complete;
  * every slow request, for the stall table and the gaps between stalls.

So this keeps every sample at or above the threshold, exactly, and replaces the
rest with a per-second envelope (count and max) that preserves the shape for
anyone who wants to plot it. Nothing any table reports changes; run report.py
before and after and the output is identical.

The threshold is recorded in each file, along with how many samples were
dropped, so the artifact says plainly that it is not the complete series.

    python3 bench/thesis/compact.py            # compact results/ in place
    python3 bench/thesis/compact.py --check    # report sizes, change nothing
"""

import glob
import json
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
RES = os.environ.get("THESIS_RESULTS", os.path.join(HERE, "results"))

# Well below the 50 ms the stall table starts counting at, so every number that
# table reports comes from a retained sample rather than an interpolation.
THRESHOLD_US = 10_000


def compact(path, dry_run=False):
    d = json.load(open(path))
    samples = d.get("samples")
    if samples is None or "samples_threshold_us" in d:
        return 0, 0

    kept = [s for s in samples if not isinstance(s[2], int) or s[1] >= THRESHOLD_US]

    envelope = {}
    for t, us, _ in samples:
        sec = int(t)
        prev = envelope.get(sec)
        if prev is None:
            envelope[sec] = [1, us]
        else:
            prev[0] += 1
            prev[1] = max(prev[1], us)

    before = os.path.getsize(path)
    d["samples"] = kept
    d["samples_threshold_us"] = THRESHOLD_US
    d["samples_dropped"] = len(samples) - len(kept)
    d["samples_note"] = (
        f"Every request at or above {THRESHOLD_US} us is present verbatim, plus every "
        f"error. Faster requests are summarised in per_second as [second, count, max_us]. "
        f"The percentiles in latency_us were computed from the complete series."
    )
    d["per_second"] = [[s, c, m] for s, (c, m) in sorted(envelope.items())]

    if not dry_run:
        json.dump(d, open(path, "w"))
    return before, os.path.getsize(path) if not dry_run else before


def main():
    dry = "--check" in sys.argv
    total_before = total_after = 0
    for path in sorted(glob.glob(os.path.join(RES, "**", "*-timeline.json"), recursive=True)):
        b, a = compact(path, dry_run=dry)
        total_before += b
        total_after += a
    verb = "would shrink" if dry else "shrank"
    print(f"{verb} timelines: {total_before / 1048576:.1f} MiB -> {total_after / 1048576:.1f} MiB")


if __name__ == "__main__":
    main()
