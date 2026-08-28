#!/usr/bin/env bash
#
# Benchmark 3 — 500 routes.
#
# Load 500 Ingresses with distinct hosts, time how long each controller takes to
# serve the last one, then repeat benchmark 2's new-Ingress measurement with all
# 500 present so propagation can be compared against the empty-cluster case.
# Controller CPU and memory are sampled on both sides of the load.
#
# One contender at a time, and its 500 are deleted before the other's are
# created. Both controllers watch every Ingress in the cluster and discard the
# ones whose class is not theirs, so each still pays the watch cost of the
# other's objects — but only one is ever holding 500 compiled routes, which
# keeps memory attributable.
#
# If a contender cannot get through the batch, the number it reached is the
# result and the script says so rather than failing.

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"

OUT="$RESULTS_DIR/b3"
ROUTES="${ROUTES:-500}"
SCALE_TRIALS="${SCALE_TRIALS:-5}"

mkdir -p "$OUT" "$PROBE_WORK"

cleanup() {
    for c in ramjet nginx; do
        K -n "$NS_APP" delete ingress -l "thesis/b3=$c" --ignore-not-found --wait=false >/dev/null 2>&1 || true
        K -n "$NS_APP" delete ingress -l "thesis/b2=$c" --ignore-not-found --wait=false >/dev/null 2>&1 || true
    done
    docker ps -aq --filter "name=^${PREFIX}-probe-" | xargs -r docker rm -f >/dev/null 2>&1 || true
}
trap cleanup EXIT INT TERM

# The bulk manifest. r000 is applied first and rNNN last; kubectl applies a
# multi-document file in order, so the last one is the sentinel that says the
# whole batch has landed.
write_bulk() {
    local contender="$1" n="$2" file="$3" i host
    : >"$file"
    for i in $(seq -w 1 "$n"); do
        host="r${i}-${contender}.thesis.test"
        cat >>"$file" <<YAML
---
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata:
  name: b3-${contender}-r${i}
  namespace: $NS_APP
  labels: { thesis/b3: "$contender" }
spec:
  ingressClassName: $(class_for "$contender")
  rules:
    - host: $host
      http:
        paths:
          - path: /
            pathType: Prefix
            backend:
              service: { name: echo-a, port: { number: 8080 } }
YAML
    done
}

reload_count() {
    K -n "$NS_NGINX" logs "deploy/${PREFIX}-nginx-controller" 2>/dev/null \
        | grep -c "Backend successfully reloaded" || true
}

run_contender() {
    local contender="$1"
    local target; target="$(target_for "$contender")"
    local pod; pod="$(pod_for "$contender")"
    local sentinel; sentinel="r$(seq -w 1 "$ROUTES" | tail -1)-${contender}.thesis.test"

    log "$contender: loading $ROUTES Ingresses"
    wait_for_quiet || true

    write_bulk "$contender" "$ROUTES" "$PROBE_WORK/b3-$contender.yaml"

    local cpu0 mem0 cpu1 mem1 rl0 rl1
    read -r cpu0 mem0 <<<"$(pod_stats "$pod" | tr '=' ' ' | awk '{print $2, $4}')"
    rl0="$(reload_count)"

    # The propagate probe already times "apply -> first correct response
    # through the data plane", which is exactly time-to-converge for a batch
    # whose last object is the sentinel. Reusing it keeps this measurement and
    # benchmark 2 on the same clock and the same polling cadence.
    probe propagate \
        --target "$target" --host "$sentinel" \
        --apply-cmd "kubectl apply -f /w/b3-$contender.yaml" \
        --timeout 900 --poll-interval 0.05 \
        --out "/w/b3-$contender-converge.json" >/dev/null || true
    mv -f "$PROBE_WORK/b3-$contender-converge.json" "$OUT/${contender}-converge.json" 2>/dev/null || true

    local created
    created="$(K -n "$NS_APP" get ingress -l "thesis/b3=$contender" --no-headers 2>/dev/null | wc -l | tr -d ' ')"
    read -r cpu1 mem1 <<<"$(pod_stats "$pod" | tr '=' ' ' | awk '{print $2, $4}')"
    rl1="$(reload_count)"

    python3 - "$OUT" "$contender" "$created" "$ROUTES" "$cpu0" "$cpu1" "$mem0" "$mem1" "$rl0" "$rl1" <<'PY'
import json, sys
out, contender, created, want, cpu0, cpu1, mem0, mem1, rl0, rl1 = sys.argv[1:11]
conv = json.load(open(f"{out}/{contender}-converge.json"))
meta = {
    "routes_requested": int(want),
    "routes_created": int(created),
    "apply_ms": conv["apply_ms"],
    "converge_ms": conv["serve_ms"],
    "timed_out": conv["timed_out"],
    "controller_cpu_seconds": round((int(cpu1) - int(cpu0)) / 1e9, 2),
    "controller_mem_bytes_before": int(mem0),
    "controller_mem_bytes_after": int(mem1),
    "nginx_reloads_during_load": int(rl1) - int(rl0),
}
json.dump(meta, open(f"{out}/{contender}-load.json", "w"))
print(f"    {contender}: {meta['routes_created']}/{meta['routes_requested']} created, "
      f"apply {meta['apply_ms']/1000:.1f}s, converge {(meta['converge_ms'] or 0)/1000:.1f}s"
      f"{' (TIMED OUT)' if meta['timed_out'] else ''}, "
      f"cpu {meta['controller_cpu_seconds']}s, "
      f"mem {meta['controller_mem_bytes_before']/1048576:.1f} -> {meta['controller_mem_bytes_after']/1048576:.1f} MiB, "
      f"nginx reloads {meta['nginx_reloads_during_load']}")
PY

    log "$contender: propagation with $created routes present ($SCALE_TRIALS trials)"
    local i host name
    for i in $(seq 1 "$SCALE_TRIALS"); do
        host="atscale-${contender}-${i}.thesis.test"
        name="b3-atscale-${contender}-${i}"
        cat >"$PROBE_WORK/$name.yaml" <<YAML
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata:
  name: $name
  namespace: $NS_APP
  labels: { thesis/b3: "$contender" }
spec:
  ingressClassName: $(class_for "$contender")
  rules:
    - host: $host
      http:
        paths:
          - path: /
            pathType: Prefix
            backend:
              service: { name: echo-a, port: { number: 8080 } }
YAML
        probe propagate --target "$target" --host "$host" \
            --apply-cmd "kubectl apply -f /w/$name.yaml" \
            --timeout 300 --out "/w/$name.json" >/dev/null || true
        mv -f "$PROBE_WORK/$name.json" "$OUT/${contender}-atscale-${i}.json" 2>/dev/null || true
        printf '    trial %-3s %s ms\n' "$i" \
            "$(python3 -c 'import json,sys;print(json.load(open(sys.argv[1]))["serve_ms"])' "$OUT/${contender}-atscale-${i}.json" 2>/dev/null || echo '?')"
        K -n "$NS_APP" delete ingress "$name" --ignore-not-found --wait=false >/dev/null 2>&1
        sleep 2
    done

    log "$contender: throughput on the stable route with $created routes present"
    oha_run "$target" "$STABLE_HOST" 64 15s /dev/null
    oha_run "$target" "$STABLE_HOST" 64 30s "$OUT/${contender}-atscale-oha.json"
    python3 - "$OUT/${contender}-atscale-oha.json" <<'PY'
import json, sys
d = json.load(open(sys.argv[1]))
print(f"    {d['summary']['requestsPerSec']:,.0f} rps, "
      f"p99 {d['latencyPercentiles']['p99'] * 1e3:.1f} ms")
PY

    log "$contender: removing the $ROUTES routes"
    local t0 t1
    t0="$(date +%s)"
    K -n "$NS_APP" delete ingress -l "thesis/b3=$contender" --wait=true >/dev/null 2>&1 || true
    t1="$(date +%s)"
    sub "deleted in $((t1 - t0))s; $(K -n "$NS_APP" get ingress -l "thesis/b3=$contender" --no-headers 2>/dev/null | wc -l | tr -d ' ') remain"
    sleep 15
}

# ---------------------------------------------------------------------------

log "Benchmark 3: $ROUTES routes"
preflight
probe_build

mkdir -p "$OUT"
rm -f "$OUT"/*.json

for contender in ramjet nginx; do
    run_contender "$contender"
done

log "Benchmark 3 complete — raw output in $OUT"
