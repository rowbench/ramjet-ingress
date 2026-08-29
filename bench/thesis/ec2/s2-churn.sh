#!/usr/bin/env bash
#
# Benchmark 2 — configuration churn under live traffic. The thesis test.
#
# Constant c64 keep-alive load on a route that never changes, while a
# *different* Ingress is mutated every two seconds. Three arms per contender:
#
#   baseline   load, configuration untouched. Each contender's own reference
#              point; nothing here is compared across contenders except through
#              their own baselines.
#   spec       every 2s the churn Ingress gains a differently-named path. A new
#              location changes the generated nginx.conf, so ingress-nginx must
#              write it out and reload. Verified from its own log, not assumed.
#   endpoint   every 2s one running pod moves in or out of a Service's
#              EndpointSlice. ingress-nginx pushes this to its Lua balancer
#              without reloading — the case its architecture handles well. A
#              report that measured only the reloading kind would be describing
#              a system that does not exist, so both are here.
#
# ramjet runs its engine=hyper release for this: it is what a stock install
# gets, and the claim under test is about the control plane rather than the
# data plane engine.
#
# Three things are recorded through each window:
#   oha        c64 throughput and latency (the aggregate)
#   timeline   one sequential request stream, every request logged, so a stall
#              is a visible shape rather than a moved percentile
#   idle       50 idle keep-alive connections, probed every 10s, counting how
#              many the server closed underneath them

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"

OUT="$RESULTS/s2"
ROUNDS="${ROUNDS:-2}"
WINDOW="${WINDOW:-120}"
SETTLE="${SETTLE:-10}"
LOAD_SECONDS="${LOAD_SECONDS:-100}"
CONC="${CONC:-64}"
CHURN_EVERY="${CHURN_EVERY:-2}"
IDLE_CONNS="${IDLE_CONNS:-50}"

mkdir -p "$OUT" "$WORK"

CHURN_PID=""
PROBE_PIDS=()
cleanup() {
    [[ -n "$CHURN_PID" ]] && kill "$CHURN_PID" 2>/dev/null || true
    for p in "${PROBE_PIDS[@]:-}"; do [[ -n "$p" ]] && kill "$p" 2>/dev/null || true; done
    reset_churn hyper >/dev/null 2>&1 || true
    reset_churn nginx >/dev/null 2>&1 || true
}
trap cleanup EXIT INT TERM

churn_svc_for() { case "$1" in hyper) echo echo-b ;; nginx) echo echo-c ;; esac; }

write_spec_ingress() {
    local contender="$1" i="$2" file="$3"
    cat >"$file" <<YAML
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata: { name: churn-$contender, namespace: $NS_APP }
spec:
  ingressClassName: $(class_for "$contender")
  rules:
    - host: $CHURN_HOST
      http:
        paths:
          - path: /
            pathType: Prefix
            backend: { service: { name: $(churn_svc_for "$contender"), port: { number: 8080 } } }
          - path: /churn-$i
            pathType: Prefix
            backend: { service: { name: $(churn_svc_for "$contender"), port: { number: 8080 } } }
YAML
}

# Leave the churn Ingress in the shape setup.sh created, so every run starts
# from the same configuration rather than from wherever the last mutation left
# off, and put every churn pod back into its Service.
reset_churn() {
    local contender="$1"
    local f="$WORK/reset-$contender.yaml" svc
    svc="$(churn_svc_for "$contender")"
    cat >"$f" <<YAML
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata: { name: churn-$contender, namespace: $NS_APP }
spec:
  ingressClassName: $(class_for "$contender")
  rules:
    - host: $CHURN_HOST
      http:
        paths:
          - path: /
            pathType: Prefix
            backend: { service: { name: $svc, port: { number: 8080 } } }
YAML
    K apply -f "$f" >/dev/null
    for p in $(K -n "$NS_APP" get pods -l "app=$svc" -o name); do
        K -n "$NS_APP" label "$p" member=yes --overwrite >/dev/null
    done
}

churn_loop() {
    local contender="$1" mode="$2" until_ts="$3" logf="$4"
    local i=0 svc pod file
    svc="$(churn_svc_for "$contender")"
    pod="$(K -n "$NS_APP" get pods -l "app=$svc" -o jsonpath='{.items[0].metadata.name}')"
    file="$WORK/churn-$contender.yaml"
    : >"$logf"
    while (( $(date +%s) < until_ts )); do
        i=$((i + 1))
        case "$mode" in
            spec)
                write_spec_ingress "$contender" "$i" "$file"
                K apply -f "$file" >/dev/null 2>&1 || true ;;
            endpoint)
                if (( i % 2 == 1 )); then
                    K -n "$NS_APP" label pod "$pod" member=no  --overwrite >/dev/null 2>&1 || true
                else
                    K -n "$NS_APP" label pod "$pod" member=yes --overwrite >/dev/null 2>&1 || true
                fi ;;
        esac
        printf '%s %s\n' "$i" "$(date +%s.%N)" >>"$logf"
        sleep "$CHURN_EVERY"
    done
}

run_arm() {
    local contender="$1" arm="$2" round="$3"
    local tag="${contender}-${arm}-r${round}"
    local target; target="$(target_for "$contender")"
    local ns; ns="$(ns_for "$contender")"
    local pod; pod="$(pod_for "$contender")"

    printf '    %-24s ' "$tag"

    oha --no-tui --output-format json -c "$CONC" -z 12s -w \
        --host "$STABLE_HOST" "http://${target}/" >/dev/null 2>&1 || true

    local cfg0 cpu0 mem0 cfg1 cpu1 mem1
    cfg0="$(config_counter "$contender")"
    read -r cpu0 mem0 <<<"$(pod_stats "$ns" "$pod" | tr '=' ' ' | awk '{print $2, $4}')"

    # Observers first: they cover the whole window including the quiet lead-in,
    # so the settle period is visible in the timeline rather than cropped out.
    python3 "$BENCH_DIR/probe.py" timeline --target "$target" --host "$STABLE_HOST" \
        --duration "$WINDOW" --out "$OUT/${tag}-timeline.json" >/dev/null 2>&1 &
    PROBE_PIDS+=("$!")
    python3 "$BENCH_DIR/probe.py" idle --target "$target" --host "$STABLE_HOST" \
        --count "$IDLE_CONNS" --duration "$WINDOW" --probe-every 10 \
        --out "$OUT/${tag}-idle.json" >/dev/null 2>&1 &
    PROBE_PIDS+=("$!")

    sleep "$SETTLE"

    if [[ "$arm" != "baseline" ]]; then
        churn_loop "$contender" "$arm" "$(( $(date +%s) + WINDOW - SETTLE - 2 ))" \
            "$OUT/${tag}-churn.log" &
        CHURN_PID="$!"
    fi

    oha_run "$target" "$STABLE_HOST" "$CONC" "${LOAD_SECONDS}s" \
        "$OUT/${tag}-oha.json" "$OUT/${tag}-vmstat.txt"

    [[ -n "$CHURN_PID" ]] && { wait "$CHURN_PID" 2>/dev/null || true; CHURN_PID=""; }
    for p in "${PROBE_PIDS[@]:-}"; do [[ -n "$p" ]] && wait "$p" 2>/dev/null || true; done
    PROBE_PIDS=()

    cfg1="$(config_counter "$contender")"
    read -r cpu1 mem1 <<<"$(pod_stats "$ns" "$pod" | tr '=' ' ' | awk '{print $2, $4}')"

    python3 - "$OUT" "$tag" "$cfg0" "$cfg1" "$cpu0" "$cpu1" "$mem1" \
        "$(vmstat_summary "$OUT/${tag}-vmstat.txt")" <<'PY'
import json, sys
out, tag, cfg0, cfg1, cpu0, cpu1, mem1, vm = sys.argv[1:9]

o = json.load(open(f"{out}/{tag}-oha.json"))
s, p = o["summary"], o["latencyPercentiles"]
n2xx = sum(v for k, v in o.get("statusCodeDistribution", {}).items() if k.startswith("2"))
nother = sum(v for k, v in o.get("statusCodeDistribution", {}).items() if not k.startswith("2"))
nerr = sum(o.get("errorDistribution", {}).values())
cpu_s = (int(cpu1) - int(cpu0)) / 1e6

idle = json.load(open(f"{out}/{tag}-idle.json"))

# The timeline probe's raw series is the artifact, but it is also tens of
# thousands of samples per run and this repo has to hold it. Keep every request
# of 10 ms or more verbatim — that is everything the stall table counts — plus a
# per-second count-and-max envelope for the rest. Percentiles were computed from
# the complete series before this ran.
tl = json.load(open(f"{out}/{tag}-timeline.json"))
samples = tl.pop("samples")
slow = [x for x in samples if isinstance(x[1], int) and x[1] >= 10_000]
env = {}
for t, us, st in samples:
    b = int(t)
    e = env.setdefault(b, [0, 0])
    e[0] += 1
    if isinstance(us, int):
        e[1] = max(e[1], us)
tl["slow_requests_us_ge_10ms"] = slow
tl["per_second_envelope"] = [[b, c, m] for b, (c, m) in sorted(env.items())]
tl["stalls"] = {k: sum(1 for x in samples if isinstance(x[1], int) and x[1] >= v)
                for k, v in (("gt_50ms", 50_000), ("gt_200ms", 200_000), ("gt_1s", 1_000_000))}
json.dump(tl, open(f"{out}/{tag}-timeline.json", "w"))

meta = {
    "config_events_applied_by_controller": int(cfg1) - int(cfg0),
    "controller_cpu_seconds": round(cpu_s, 3),
    "controller_mem_bytes_end": int(mem1),
    "cpu_us_per_request": round(cpu_s * 1e6 / n2xx, 2) if n2xx else None,
    "rps": s["requestsPerSec"], "p50_ms": p["p50"] * 1e3, "p99_ms": p["p99"] * 1e3,
    "p999_ms": p["p99.9"] * 1e3 if "p99.9" in p else None,
    "ok": n2xx, "non_2xx": nother, "errors": nerr,
    "idle_held": idle["held"], "idle_survived": idle["survived"],
    "timeline_requests": tl["requests"], "timeline_errors": tl["errors"],
    "timeline_p50_us": tl["latency_us"]["p50"], "timeline_p99_us": tl["latency_us"]["p99"],
    "timeline_max_us": tl["latency_us"]["max"], "stalls": tl["stalls"],
    "vmstat": vm,
}
json.dump(meta, open(f"{out}/{tag}-meta.json", "w"))
print(f"{s['requestsPerSec']:>8,.0f} rps  p99 {p['p99']*1e3:>6.2f} ms  err {nerr + nother:>4}  "
      f"idle {idle['survived']}/{idle['held']}  cfg {meta['config_events_applied_by_controller']:>3}  "
      f"cpu/req {meta['cpu_us_per_request'] or 0:>6.1f} us  | {vm}")
PY
}

log "Benchmark 2: config churn under load"
sub "window ${WINDOW}s, settle ${SETTLE}s, oha c${CONC} for ${LOAD_SECONDS}s, churn every ${CHURN_EVERY}s"
sub "${IDLE_CONNS} idle keep-alive connections held throughout, probed every 10s"
assert_engine hyper hyper

[[ "${KEEP_OLD:-0}" == "1" ]] || rm -f "$OUT"/*.json "$OUT"/*.log "$OUT"/*.txt

for round in $(seq 1 "$ROUNDS"); do
    if (( round % 2 == 1 )); then order=(hyper nginx); else order=(nginx hyper); fi
    if (( round % 2 == 1 )); then arms=(baseline spec endpoint); else arms=(endpoint spec baseline); fi
    log "round $round/$ROUNDS (contenders: ${order[*]}; arms: ${arms[*]})"
    for arm in "${arms[@]}"; do
        for c in "${order[@]}"; do
            reset_churn "$c"
            sleep 5
            run_arm "$c" "$arm" "$round"
        done
    done
done

log "Benchmark 2 complete — raw output in $OUT"
