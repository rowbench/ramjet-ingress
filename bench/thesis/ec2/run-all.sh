#!/usr/bin/env bash
# Everything, in order, one at a time. Nothing overlaps: this box has four
# vCPUs and the load generator is on it.
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"
cd "$BENCH_DIR"

log "steady-state, c64"
CONC=64  DUR=30s ROUNDS=3 TAG=c64  bash s1-steady.sh
log "steady-state, c256"
CONC=256 DUR=30s ROUNDS=1 TAG=c256 bash s1-steady.sh
log "config churn"
ROUNDS=2 bash s2-churn.sh
log "propagation"
TRIALS=10 bash s3-prop.sh
log "ALL DONE"
