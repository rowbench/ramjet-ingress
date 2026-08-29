#!/usr/bin/env bash
#
# Benchmark 1 — steady-state forwarding.
#
# Four contenders on one stable route that nothing mutates: the three
# controllers and the backend Service itself with no proxy in the path. Three
# 30-second windows each at c64, with the rotation offset by one every round so
# no contender systematically holds the warmest or the most drifted machine.
#
# Every measured window is preceded by a discarded warmup of the same shape.
# bench/RESULTS.md recorded a cold ramjet at 42k rps against 58k once its
# upstream pool had filled, and a benchmark that reports the first window is
# reporting the pool filling.
#
# Controller CPU is differenced across the window from the pod's own cgroup, so
# a CPU-per-request figure exists that does not depend on how much of this
# shared instance we were given at the time. On a burstable box that is the
# column that survives; throughput is the one that does not.

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"

OUT="$RESULTS/s1"
CONC="${CONC:-64}"
DUR="${DUR:-30s}"
WARMUP="${WARMUP:-10s}"
ROUNDS="${ROUNDS:-3}"
TAG="${TAG:-c$CONC}"

mkdir -p "$OUT"

run_one() {
    local contender="$1" round="$2"
    local tag="${TAG}-${contender}-r${round}"
    local target; target="$(target_for "$contender")"
    local host; host="$(host_for "$contender")"
    local ns pod cpu0 mem0 cpu1 mem1

    printf '    %-22s ' "$tag"

    oha --no-tui --output-format json -c "$CONC" -z "$WARMUP" -w \
        --host "$host" "http://${target}/" >/dev/null 2>&1 || true

    if [[ "$contender" != "baseline" ]]; then
        ns="$(ns_for "$contender")"; pod="$(pod_for "$contender")"
        read -r cpu0 mem0 <<<"$(pod_stats "$ns" "$pod" | tr '=' ' ' | awk '{print $2, $4}')"
    fi

    oha_run "$target" "$host" "$CONC" "$DUR" "$OUT/${tag}-oha.json" "$OUT/${tag}-vmstat.txt"

    if [[ "$contender" != "baseline" ]]; then
        read -r cpu1 mem1 <<<"$(pod_stats "$ns" "$pod" | tr '=' ' ' | awk '{print $2, $4}')"
    else
        cpu0=0; cpu1=0; mem1=0
    fi

    python3 - "$OUT" "$tag" "$cpu0" "$cpu1" "$mem1" "$(vmstat_summary "$OUT/${tag}-vmstat.txt")" <<'PY'
import json, sys
out, tag, cpu0, cpu1, mem1, vm = sys.argv[1:7]
o = json.load(open(f"{out}/{tag}-oha.json"))
s, p = o["summary"], o["latencyPercentiles"]
n2xx = sum(v for k, v in o.get("statusCodeDistribution", {}).items() if k.startswith("2"))
nother = sum(v for k, v in o.get("statusCodeDistribution", {}).items() if not k.startswith("2"))
nerr = sum(o.get("errorDistribution", {}).values())
cpu_s = (int(cpu1) - int(cpu0)) / 1e6
meta = {"controller_cpu_seconds": round(cpu_s, 3),
        "controller_mem_bytes_end": int(mem1),
        "cpu_us_per_request": round(cpu_s * 1e6 / n2xx, 2) if n2xx else None,
        "vmstat": vm, "rps": s["requestsPerSec"],
        "p50_ms": p["p50"] * 1e3, "p99_ms": p["p99"] * 1e3,
        "ok": n2xx, "non_2xx": nother, "errors": nerr}
json.dump(meta, open(f"{out}/{tag}-meta.json", "w"))
print(f"{s['requestsPerSec']:>9,.0f} rps  p50 {p['p50']*1e3:>6.2f}  p99 {p['p99']*1e3:>7.2f} ms  "
      f"2xx {n2xx:>8,}  other {nother:>4}  err {nerr:>4}  "
      f"cpu/req {meta['cpu_us_per_request'] or 0:>6.1f} us  | {vm}")
PY
}

log "Benchmark 1: steady-state forwarding, c${CONC}, ${DUR} x ${ROUNDS}, interleaved"
sub "contenders rotate every round so ordering bias cancels rather than accumulates"
assert_engine hyper hyper
assert_engine uring uring
sub "engines verified: hyper=$(engine_line hyper) uring=$(engine_line uring)"

[[ "${KEEP_OLD:-0}" == "1" ]] || rm -f "$OUT/${TAG}"-*.json "$OUT/${TAG}"-*.txt

ALL=(baseline hyper nginx uring)
for round in $(seq 1 "$ROUNDS"); do
    order=()
    for i in "${!ALL[@]}"; do
        order+=("${ALL[$(( (i + round - 1) % ${#ALL[@]} ))]}")
    done
    log "round $round/$ROUNDS: ${order[*]}"
    for c in "${order[@]}"; do
        run_one "$c" "$round"
        sleep 5
    done
done

log "Benchmark 1 complete — raw output in $OUT"
