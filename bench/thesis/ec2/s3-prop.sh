#!/usr/bin/env bash
#
# Benchmark 3 — propagation latency: `kubectl apply` to the first request the
# data plane answers.
#
# A brand-new Ingress on a host nothing has ever served, so success is
# unambiguous: both contenders 404 until the route exists, and the clock stops
# on the first 200. Polling is every 20 ms on its own connection.
#
# The clock starts *before* kubectl is invoked, because that is when a human or
# a CI job asked for the change. The instant kubectl returned is recorded
# separately, so ingress-nginx's admission-webhook share of the wait is a number
# in the output rather than an argument in the prose. The apply and the poll run
# in one process, so "applied" and "served" are two readings of one clock.
#
# No load is running. This is a control-plane measurement; background load would
# only add the data plane's queueing to every contender equally.
#
# Both ramjet releases are measured even though their control planes are
# identical code, because that is the cheap way to show the engine choice does
# not touch this path rather than asserting it.

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"

OUT="$RESULTS/s3"
TRIALS="${TRIALS:-10}"

mkdir -p "$OUT" "$WORK"

cleanup() {
    K -n "$NS_APP" delete ingress -l "ec2/s3=yes" --ignore-not-found --wait=false >/dev/null 2>&1 || true
}
trap cleanup EXIT INT TERM

trial_new() {
    local contender="$1" i="$2"
    local host="new-${contender}-${i}.ec2.test"
    local name="s3-new-${contender}-${i}"
    cat >"$WORK/$name.yaml" <<YAML
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata:
  name: $name
  namespace: $NS_APP
  labels: { ec2/s3: "yes" }
spec:
  ingressClassName: $(class_for "$contender")
  rules:
    - host: $host
      http:
        paths:
          - path: /
            pathType: Prefix
            backend: { service: { name: echo-a, port: { number: 8080 } } }
YAML
    python3 "$BENCH_DIR/probe.py" propagate \
        --target "$(target_for "$contender")" --host "$host" \
        --apply-cmd "kubectl apply -f $WORK/$name.yaml" \
        --timeout 180 --out "$OUT/${contender}-new-${i}.json" >/dev/null
    K -n "$NS_APP" delete ingress "$name" --ignore-not-found --wait=false >/dev/null 2>&1
    jq -r '"\(.serve_ms)/\(.apply_ms)"' "$OUT/${contender}-new-${i}.json"
}

log "Benchmark 3: propagation latency (apply -> first 200)"
sub "$TRIALS trials per contender, contenders rotated every trial"
assert_engine hyper hyper
assert_engine uring uring

[[ "${KEEP_OLD:-0}" == "1" ]] || rm -f "$OUT"/*.json

ALL=(hyper nginx uring)
for i in $(seq 1 "$TRIALS"); do
    order=()
    for k in "${!ALL[@]}"; do
        order+=("${ALL[$(( (k + i - 1) % ${#ALL[@]} ))]}")
    done
    printf '    trial %-3s' "$i"
    for c in "${order[@]}"; do
        printf '  %s=%sms' "$c" "$(trial_new "$c" "$i")"
    done
    printf '\n'
    sleep 3
done

log "Benchmark 3 complete — raw output in $OUT"
