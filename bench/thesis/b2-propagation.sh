#!/usr/bin/env bash
#
# Benchmark 2 — how long a configuration change takes to reach the data plane.
#
# Two shapes of change, because they exercise different machinery:
#
#   new       a brand-new Ingress on a host nothing has ever served. Success is
#             the first HTTP 200; until the route exists both contenders 404.
#   mutate    an existing route's backend Service is swapped. The route keeps
#             answering 200 from the *old* backend throughout, so success is the
#             first response whose body carries the new backend's marker.
#
# The clock starts before kubectl is invoked, because that is when the change
# was asked for. The instant kubectl *returned* is recorded separately, so the
# admission-webhook share of ingress-nginx's wait is a number in the output
# rather than an argument in the prose.
#
# No load is running. This is a control-plane measurement and background load
# would only add the data plane's queueing to both sides.

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"

OUT="$RESULTS_DIR/b2"
TRIALS="${TRIALS:-10}"

mkdir -p "$OUT" "$PROBE_WORK"

cleanup() {
    for c in ramjet nginx; do
        K -n "$NS_APP" delete ingress -l "thesis/b2=$c" --ignore-not-found --wait=false >/dev/null 2>&1 || true
    done
    docker ps -aq --filter "name=^${PREFIX}-probe-" | xargs -r docker rm -f >/dev/null 2>&1 || true
}
trap cleanup EXIT INT TERM

write_new_ingress() {
    local contender="$1" host="$2" name="$3" file="$4"
    cat >"$file" <<YAML
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata:
  name: $name
  namespace: $NS_APP
  labels: { thesis/b2: "$contender" }
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
}

write_mutate_ingress() {
    local contender="$1" host="$2" name="$3" svc="$4" file="$5"
    cat >"$file" <<YAML
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata:
  name: $name
  namespace: $NS_APP
  labels: { thesis/b2: "$contender" }
spec:
  ingressClassName: $(class_for "$contender")
  rules:
    - host: $host
      http:
        paths:
          - path: /
            pathType: Prefix
            backend:
              service: { name: $svc, port: { number: 8080 } }
YAML
}

trial_new() {
    local contender="$1" i="$2" suffix="${3:-}"
    local host="new${suffix}-${contender}-${i}.thesis.test"
    local name="b2-new${suffix}-${contender}-${i}"
    write_new_ingress "$contender" "$host" "$name" "$PROBE_WORK/$name.yaml"
    probe propagate \
        --target "$(target_for "$contender")" --host "$host" \
        --apply-cmd "kubectl apply -f /w/$name.yaml" \
        --timeout 180 --out "/w/$name.json" >/dev/null
    mv -f "$PROBE_WORK/$name.json" "$OUT/${contender}-new${suffix}-${i}.json"
    K -n "$NS_APP" delete ingress "$name" --ignore-not-found --wait=false >/dev/null 2>&1
    python3 -c 'import json,sys;print(json.load(open(sys.argv[1]))["serve_ms"])' \
        "$OUT/${contender}-new${suffix}-${i}.json"
}

trial_mutate() {
    local contender="$1" i="$2"
    local host="mut-${contender}.thesis.test"
    local name="b2-mut-${contender}"
    # Alternate the target backend so every trial is a real change and no
    # unmeasured reset step is needed between them.
    local svc marker
    if (( i % 2 == 1 )); then svc=echo-b; marker=backend-b; else svc=echo-a; marker=backend-a; fi
    write_mutate_ingress "$contender" "$host" "$name" "$svc" "$PROBE_WORK/$name.yaml"
    probe propagate \
        --target "$(target_for "$contender")" --host "$host" \
        --apply-cmd "kubectl apply -f /w/$name.yaml" \
        --expect "$marker" --timeout 180 --out "/w/$name.json" >/dev/null
    mv -f "$PROBE_WORK/$name.json" "$OUT/${contender}-mutate-${i}.json"
    python3 -c 'import json,sys;print(json.load(open(sys.argv[1]))["serve_ms"])' \
        "$OUT/${contender}-mutate-${i}.json"
}

seed_mutate() {
    local contender="$1"
    local host="mut-${contender}.thesis.test" name="b2-mut-${contender}"
    write_mutate_ingress "$contender" "$host" "$name" echo-a "$PROBE_WORK/$name.yaml"
    K apply -f "$PROBE_WORK/$name.yaml" >/dev/null
    wait_for_route "$contender" "$host" 120 || die "$contender never served the mutation seed route"
}

# ---------------------------------------------------------------------------

log "Benchmark 2: propagation latency (apply -> served)"
sub "$TRIALS trials of each shape per contender, contenders interleaved"
preflight
probe_build
wait_for_quiet || true

mkdir -p "$OUT"
rm -f "$OUT"/*.json

for contender in ramjet nginx; do seed_mutate "$contender"; done

log "new Ingress (unique host, poll until first 200)"
for i in $(seq 1 "$TRIALS"); do
    if (( i % 2 == 1 )); then order=(ramjet nginx); else order=(nginx ramjet); fi
    printf '    trial %-3s' "$i"
    for contender in "${order[@]}"; do
        printf '  %s=%sms' "$contender" "$(trial_new "$contender" "$i")"
    done
    printf '\n'
    sleep 2
done

log "backend swap (existing host, poll until the NEW backend answers)"
for i in $(seq 1 "$TRIALS"); do
    if (( i % 2 == 1 )); then order=(ramjet nginx); else order=(nginx ramjet); fi
    printf '    trial %-3s' "$i"
    for contender in "${order[@]}"; do
        printf '  %s=%sms' "$contender" "$(trial_mutate "$contender" "$i")"
    done
    printf '\n'
    sleep 2
done

log "Benchmark 2 complete — raw output in $OUT"
