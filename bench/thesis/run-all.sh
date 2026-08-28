#!/usr/bin/env bash
#
# The whole suite, in order, from nothing to a rendered set of tables.
#
#     bench/thesis/run-all.sh              # ~2 hours
#     QUICK=1 bench/thesis/run-all.sh      # ~15 minutes, shapes only, not results
#
# Every stage waits for the machine to go quiet first: the docker daemon here is
# shared with another agent's Rust builds, and a measurement taken through one
# is a measurement of the build.
#
# Teardown is NOT automatic. The cluster state costs several minutes to rebuild
# and is worth keeping between stages; run bench/thesis/teardown.sh when done.

set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if [[ "${QUICK:-0}" == "1" ]]; then
    export ROUNDS=1 WINDOW=40 SETTLE=5 LOAD_SECONDS=25 WARMUP=5 \
           TRIALS=3 ROUTES=50 SCALE_TRIALS=2 CONNS=1000
fi

bash "$HERE/setup.sh"
bash "$HERE/versions.sh"
bash "$HERE/b1-churn.sh"
bash "$HERE/b2-propagation.sh"
bash "$HERE/b3-scale.sh"
bash "$HERE/b4-idle-memory.sh"

# Shrink the per-request timeline series before anything is committed: the
# complete series is 117 MiB and every table renders identically without it.
python3 "$HERE/compact.py"
python3 "$HERE/report.py" | tee "$HERE/results/tables.md"

printf '\nTables in %s/results/tables.md. Raw JSON beside it.\n' "$HERE"
printf 'Cluster objects are still up — run %s/teardown.sh to remove them.\n' "$HERE"
