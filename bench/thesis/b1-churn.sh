#!/usr/bin/env bash
#
# Benchmark 1 — configuration churn under live traffic.
#
# The project's claim is that a config change costs nothing because it is a
# pointer swap rather than a reload. This measures that claim against the
# controller the claim is aimed at, under constant load, three ways:
#
#   baseline   constant load, configuration untouched. Each contender's own
#              reference point; nothing here is compared across contenders
#              except through their own baselines.
#   spec       every 2s, a *different* Ingress gains a new path. This changes
#              the generated nginx.conf, so ingress-nginx must reload.
#   endpoint   every 2s, a pod moves in or out of a Service's EndpointSlice.
#              ingress-nginx handles this through its Lua balancer without
#              reloading, which is the case where its architecture is fine and
#              a report that only measured `spec` would be lying by omission.
#
# Three things are recorded through each window:
#   oha        c64 keep-alive throughput and latency (the aggregate)
#   timeline   one sequential request stream, every request logged, so a stall
#              is visible as a shape and not as a moved percentile
#   idle       50 idle keep-alive connections, probed every 10s, counting how
#              many the server closed underneath them
#
# Contenders are interleaved within every arm, and the order flips between
# rounds, so neither one systematically gets the warmer or the busier machine.

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"

OUT="$RESULTS_DIR/b1"
ROUNDS="${ROUNDS:-2}"
WINDOW="${WINDOW:-120}"       # total observation window per run
SETTLE="${SETTLE:-10}"        # quiet lead-in before churn and load start
LOAD_SECONDS="${LOAD_SECONDS:-100}"
CONC="${CONC:-64}"
CHURN_EVERY="${CHURN_EVERY:-2}"
IDLE_CONNS="${IDLE_CONNS:-50}"

mkdir -p "$OUT" "$PROBE_WORK"

CHURN_PIDS=()
cleanup() {
    for pid in "${CHURN_PIDS[@]:-}"; do [[ -n "$pid" ]] && kill "$pid" 2>/dev/null; done
    docker ps -aq --filter "name=^${PREFIX}-b1-" | xargs -r docker rm -f >/dev/null 2>&1 || true
    # Leave the churn Ingresses in the shape setup.sh created, so a re-run
    # starts from the same configuration rather than from wherever the last
    # mutation left off.
    reset_churn ramjet >/dev/null 2>&1 || true
    reset_churn nginx  >/dev/null 2>&1 || true
}
trap cleanup EXIT INT TERM

churn_svc_for() { case "$1" in ramjet) echo echo-b ;; nginx) echo echo-c ;; esac; }

# ---------------------------------------------------------------------------
# Mutations
# ---------------------------------------------------------------------------

# Spec churn: rewrite the churn Ingress with an extra, differently-named path
# on every iteration. A new location in the generated configuration is the
# thing that forces nginx to reload; endpoint changes are not, and conflating
# the two is exactly the mistake this benchmark is built to avoid.
write_spec_ingress() {
    local contender="$1" i="$2" file="$3"
    cat >"$file" <<YAML
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata:
  name: churn-$contender
  namespace: $NS_APP
spec:
  ingressClassName: $(class_for "$contender")
  rules:
    - host: $CHURN_HOST
      http:
        paths:
          - path: /
            pathType: Prefix
            backend:
              service: { name: $(churn_svc_for "$contender"), port: { number: 8080 } }
          - path: /churn-$i
            pathType: Prefix
            backend:
              service: { name: $(churn_svc_for "$contender"), port: { number: 8080 } }
YAML
}

reset_churn() {
    local contender="$1" f="$PROBE_WORK/reset-$contender.yaml"
    cat >"$f" <<YAML
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata:
  name: churn-$contender
  namespace: $NS_APP
spec:
  ingressClassName: $(class_for "$contender")
  rules:
    - host: $CHURN_HOST
      http:
        paths:
          - path: /
            pathType: Prefix
            backend:
              service: { name: $(churn_svc_for "$contender"), port: { number: 8080 } }
YAML
    K apply -f "$f" >/dev/null
    # Restore every churn pod to Service membership.
    local svc; svc="$(churn_svc_for "$contender")"
    for p in $(K -n "$NS_APP" get pods -l "app=$svc" -o name); do
        K -n "$NS_APP" label "$p" member=yes --overwrite >/dev/null
    done
}

# Endpoint churn: flip one running pod in and out of its Service's selector.
# The Deployment's selector is `app` only, so the ReplicaSet never notices and
# no pod is ever scheduled or terminated — the only thing that changes is the
# EndpointSlice. Scaling the Deployment instead would have measured the
# scheduler's latency and called it the controller's.
churn_loop() {
    local contender="$1" mode="$2" until_ts="$3" log="$4"
    local i=0 svc pod file
    svc="$(churn_svc_for "$contender")"
    pod="$(K -n "$NS_APP" get pods -l "app=$svc" -o jsonpath='{.items[0].metadata.name}')"
    file="$PROBE_WORK/churn-$contender.yaml"
    : >"$log"

    while (( $(date +%s) < until_ts )); do
        i=$((i + 1))
        local t0 t1
        t0="$(python3 -c 'import time;print(f"{time.time():.4f}")')"
        case "$mode" in
            spec)
                write_spec_ingress "$contender" "$i" "$file"
                K apply -f "$file" >/dev/null 2>&1 || true
                ;;
            endpoint)
                if (( i % 2 == 1 )); then
                    K -n "$NS_APP" label pod "$pod" member=no --overwrite >/dev/null 2>&1 || true
                else
                    K -n "$NS_APP" label pod "$pod" member=yes --overwrite >/dev/null 2>&1 || true
                fi
                ;;
        esac
        t1="$(python3 -c 'import time;print(f"{time.time():.4f}")')"
        printf '%s %s %s\n' "$i" "$t0" "$t1" >>"$log"
        sleep "$CHURN_EVERY"
    done
}

# ---------------------------------------------------------------------------
# Controller-side evidence
#
# The whole argument turns on whether ingress-nginx actually reloaded, so that
# is read out of the controller rather than assumed from the mutation type.
#
# The two contenders are counted from different sources, and the asymmetry is
# deliberate. ramjet publishes ramjet_route_table_generation on an admin port
# its chart already exposes, so reading it costs nothing. ingress-nginx exposes
# an equivalent reload counter only when controller.metrics.enabled is turned
# on, and that flag also switches on per-request Lua monitoring — measuring the
# reload would have slowed the thing being measured. Its own log states each
# reload explicitly, which is direct evidence at no cost, so that is what is
# counted.
# ---------------------------------------------------------------------------

config_counter() {
    case "$1" in
        ramjet)
            K get --raw "/api/v1/namespaces/$NS_RAMJET/services/${PREFIX}-ramjet-admin:10254/proxy/metrics" 2>/dev/null \
                | awk '/^ramjet_route_table_generation /{print $2; found=1} END{if(!found) print 0}' ;;
        nginx)
            K -n "$NS_NGINX" logs "deploy/${PREFIX}-nginx-controller" 2>/dev/null \
                | grep -c "Backend successfully reloaded" || true ;;
    esac
}

# ---------------------------------------------------------------------------
# One run
# ---------------------------------------------------------------------------

run_arm() {
    local contender="$1" arm="$2" round="$3"
    local tag="${contender}-${arm}-r${round}"
    local target; target="$(target_for "$contender")"
    local pod; pod="$(pod_for "$contender")"

    printf '    %-24s ' "$tag"
    wait_for_quiet >/dev/null 2>&1 || true

    # A cold contender measures its own warmup: bench/RESULTS.md recorded a
    # freshly started ramjet at 42k rps against 58k once its upstream pool had
    # filled. Every measured window gets a discarded load run first, and both
    # contenders get the same one.
    oha_run "$target" "$STABLE_HOST" "$CONC" "${WARMUP:-15}s" /dev/null

    local cfg0 cpu0 cfg1 cpu1
    cfg0="$(config_counter "$contender")"
    read -r cpu0 mem0 <<<"$(pod_stats "$pod" | tr '=' ' ' | awk '{print $2, $4}')"

    # Observers first: they cover the whole window including the quiet lead-in,
    # so the settle period is visible in the timeline rather than cropped out.
    probe_bg "${PREFIX}-b1-timeline-$$" timeline \
        --target "$target" --host "$STABLE_HOST" \
        --duration "$WINDOW" --out "/w/${tag}-timeline.json"
    probe_bg "${PREFIX}-b1-idle-$$" idle \
        --target "$target" --host "$STABLE_HOST" \
        --count "$IDLE_CONNS" --duration "$WINDOW" --probe-every 10 \
        --out "/w/${tag}-idle.json"

    sleep "$SETTLE"

    if [[ "$arm" != "baseline" ]]; then
        churn_loop "$contender" "$arm" "$(( $(date +%s) + WINDOW - SETTLE - 2 ))" \
            "$OUT/${tag}-churn.log" &
        CHURN_PIDS+=("$!")
    fi

    oha_run "$target" "$STABLE_HOST" "$CONC" "${LOAD_SECONDS}s" "$OUT/${tag}-oha.json"

    for pid in "${CHURN_PIDS[@]:-}"; do [[ -n "$pid" ]] && wait "$pid" 2>/dev/null; done
    CHURN_PIDS=()
    docker wait "${PREFIX}-b1-timeline-$$" "${PREFIX}-b1-idle-$$" >/dev/null 2>&1 || true
    docker rm -f "${PREFIX}-b1-timeline-$$" "${PREFIX}-b1-idle-$$" >/dev/null 2>&1 || true

    cfg1="$(config_counter "$contender")"
    read -r cpu1 mem1 <<<"$(pod_stats "$pod" | tr '=' ' ' | awk '{print $2, $4}')"

    mv -f "$PROBE_WORK/${tag}-timeline.json" "$OUT/" 2>/dev/null || true
    mv -f "$PROBE_WORK/${tag}-idle.json"     "$OUT/" 2>/dev/null || true

    python3 - "$OUT" "$tag" "$cfg0" "$cfg1" "$cpu0" "$cpu1" "$mem1" <<'PY'
import json, sys
out, tag, cfg0, cfg1, cpu0, cpu1, mem1 = sys.argv[1:8]
meta = {
    "config_events_applied_by_controller": int(cfg1) - int(cfg0),
    "controller_cpu_seconds": round((int(cpu1) - int(cpu0)) / 1e9, 2),
    "controller_mem_bytes_end": int(mem1),
}
json.dump(meta, open(f"{out}/{tag}-controller.json", "w"))
oha = json.load(open(f"{out}/{tag}-oha.json"))
s, p = oha["summary"], oha["latencyPercentiles"]
idle = json.load(open(f"{out}/{tag}-idle.json"))
print(f"{s['requestsPerSec']:>9,.0f} rps  p99 {p['p99']*1e3:>7.1f} ms  "
      f"err {sum(v for k, v in oha['statusCodeDistribution'].items() if not k.startswith('2')) + sum(oha.get('errorDistribution', {}).values()):>4}  "
      f"idle {idle['survived']}/{idle['held']}  "
      f"reloads {meta['config_events_applied_by_controller']}")
PY
}

# ---------------------------------------------------------------------------

log "Benchmark 1: config churn under load"
sub "window ${WINDOW}s, settle ${SETTLE}s, oha c${CONC} for ${LOAD_SECONDS}s, churn every ${CHURN_EVERY}s"
sub "${IDLE_CONNS} idle keep-alive connections held throughout, probed every 10s"
preflight
probe_build

# Old output from a previous run with different parameters would otherwise be
# folded into the tables by report.py, which globs rather than reads a manifest.
# KEEP_OLD=1 with ROUND_START=N adds rounds to an existing set instead, which is
# how rounds 3 and 4 were added after rounds 1 and 2 raised a question the first
# two rounds could not answer.
[[ "${KEEP_OLD:-0}" == "1" ]] || rm -f "$OUT"/*.json "$OUT"/*.log

for round in $(seq "${ROUND_START:-1}" "$ROUNDS"); do
    # Flip contender order between rounds so ordering bias cancels instead of
    # accumulating on whichever one always went second. ARM order is flipped
    # too, from round 3 on: the first two rounds ran baseline, spec, endpoint
    # every time, which left the endpoint arm always last and therefore always
    # holding whatever drift the machine had accumulated. Reversing it is how
    # that confound gets tested rather than argued about.
    if (( round % 2 == 1 )); then order=(ramjet nginx); else order=(nginx ramjet); fi
    if (( round >= 3 )); then arms=(endpoint spec baseline); else arms=(baseline spec endpoint); fi
    log "round $round/$ROUNDS (contenders: ${order[*]}; arms: ${arms[*]})"
    for arm in "${arms[@]}"; do
        for contender in "${order[@]}"; do
            reset_churn "$contender"
            sleep 5
            run_arm "$contender" "$arm" "$round"
        done
    done
done

log "Benchmark 1 complete — raw output in $OUT"
