#!/usr/bin/env bash
#
# End-to-end proof of the deployment story against a local Docker Desktop
# Kubernetes cluster: build the image, install the chart, deploy a pair of echo
# backends behind a production Ingress and a canary Ingress, and assert that
# routing, canary splitting, TLS, status writeback, and metrics all do what the
# chart claims.
#
# Everything is torn down at the end. `KEEP=1 deploy/e2e.sh` leaves it standing.
#
# # Why every command names its context
#
# The kubeconfig on a developer's machine routinely holds production clusters,
# and `kubectl config use-context` is process-global state that a script has no
# business changing. Every kubectl and helm invocation below carries an explicit
# --context/--kube-context, and the preflight refuses to run against anything
# that is not the expected local cluster. This is not defensive
# over-engineering: a mistyped current-context is the exact mechanism by which
# test scripts delete production namespaces.

set -euo pipefail

CONTEXT="${CONTEXT:-docker-desktop}"
SYS_NS="${SYS_NS:-ramjet-e2e-system}"
APP_NS="${APP_NS:-ramjet-e2e}"
RELEASE="${RELEASE:-ramjet-e2e}"
IMAGE="${IMAGE:-ramjet-ingress:e2e}"
HOST="${HOST:-demo.ramjet.test}"
KEEP="${KEEP:-0}"

# Chosen high and fixed so a stale forward from a previous run is visible as a
# bind failure rather than as mysteriously wrong assertions.
HTTP_PORT="${HTTP_PORT:-18080}"
HTTPS_PORT="${HTTPS_PORT:-18443}"
ADMIN_PORT="${ADMIN_PORT:-18254}"

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CHART="$REPO_ROOT/deploy/chart/ramjet-ingress"
WORK="$(mktemp -d)"

K() { kubectl --context "$CONTEXT" "$@"; }
H() { helm --kube-context "$CONTEXT" "$@"; }

PASSES=0
FAILURES=0
RESULTS=()

pass() { PASSES=$((PASSES + 1)); RESULTS+=("PASS  $1"); printf '  \033[32mPASS\033[0m  %s\n' "$1"; }
fail() { FAILURES=$((FAILURES + 1)); RESULTS+=("FAIL  $1"); printf '  \033[31mFAIL\033[0m  %s\n' "$1"; }
step() { printf '\n\033[1m==> %s\033[0m\n' "$1"; }
note() { printf '     %s\n' "$1"; }

PF_PIDS=()

cleanup() {
  local status=$?
  for pid in "${PF_PIDS[@]:-}"; do
    if [[ -n "$pid" ]]; then
      kill "$pid" 2>/dev/null || true
      # Reaping each one here is what keeps bash from printing "Terminated" over
      # the summary. A previous version used `disown` for that, which removed
      # the jobs from the table but also meant a forward could outlive a failed
      # run and sit on the port — the next run then bound nothing and asserted
      # against a dead tunnel.
      wait "$pid" 2>/dev/null || true
    fi
  done
  if [[ "$KEEP" == "1" ]]; then
    printf '\nKEEP=1 — leaving %s and %s in place.\n' "$SYS_NS" "$APP_NS"
  else
    step "Teardown"
    H uninstall "$RELEASE" --namespace "$SYS_NS" --wait --timeout 2m >/dev/null 2>&1 || true
    K delete namespace "$APP_NS" --wait=false >/dev/null 2>&1 || true
    K delete namespace "$SYS_NS" --wait=false >/dev/null 2>&1 || true
    # Cluster-scoped, so a namespace delete does not reach it.
    K delete ingressclass ramjet --ignore-not-found >/dev/null 2>&1 || true
    note "namespaces deleted (asynchronously), release uninstalled"
  fi
  rm -rf "$WORK"
  exit "$status"
}
trap cleanup EXIT

# ---------------------------------------------------------------- preflight --

step "Preflight"

for tool in docker kubectl helm openssl curl; do
  command -v "$tool" >/dev/null || { echo "missing required tool: $tool" >&2; exit 1; }
done

K config get-contexts "$CONTEXT" >/dev/null 2>&1 \
  || { echo "context '$CONTEXT' is not in the kubeconfig; refusing to guess" >&2; exit 1; }

# The context name alone is not proof: names are arbitrary. Check that the
# server really is a single-node local cluster before creating anything.
NODES="$(K get nodes -o jsonpath='{.items[*].metadata.name}' 2>/dev/null || true)"
[[ -n "$NODES" ]] || { echo "context '$CONTEXT' is unreachable; stopping rather than falling back" >&2; exit 1; }
case "$NODES" in
  *desktop*|*docker*) : ;;
  *) echo "context '$CONTEXT' has nodes [$NODES], which do not look like a local Docker Desktop cluster; refusing" >&2; exit 1 ;;
esac
note "context $CONTEXT, node(s): $NODES"

# A forward left behind by an earlier run keeps listening on its port while
# pointing at a namespace that no longer exists. Connections to it are refused,
# which downstream looks exactly like a broken proxy — so refuse to start
# rather than produce a confusing failure.
for port in "$HTTP_PORT" "$HTTPS_PORT" "$ADMIN_PORT"; do
  if (exec 3<>"/dev/tcp/127.0.0.1/$port") 2>/dev/null; then
    exec 3>&- 2>/dev/null || true
    echo "local port $port is already in use — a stale 'kubectl port-forward' from a previous run?" >&2
    echo "  find it with: lsof -nP -iTCP:$port -sTCP:LISTEN" >&2
    exit 1
  fi
done

# ------------------------------------------------------------------- build ---

step "Building $IMAGE"
docker build -t "$IMAGE" "$REPO_ROOT"
SIZE="$(docker image inspect "$IMAGE" --format '{{.Size}}' | awk '{printf "%.1f MB", $1/1024/1024}')"
note "image size: $SIZE"

# Docker Desktop's Kubernetes runs a kind-style node whose containerd is a
# *separate* image store from the docker daemon's: a freshly built image is not
# visible to the kubelet, and a pod referencing it fails with
# ErrImageNeverPull. So load it explicitly, which is what `kind load
# docker-image` does under the hood.
#
# The node is addressable as a container named desktop-control-plane even
# though `docker ps` does not list it — Docker Desktop hides it from the
# container listing while still allowing exec. Do not conclude from an empty
# `docker ps` that there is no node to load into.
#
# Importing rather than relying on a pull is what lets imagePullPolicy stay
# Never: the assertions then prove the image built by *this* script ran, with
# no path by which the kubelet could quietly substitute one from a registry.
NODE="${NODE:-desktop-control-plane}"

step "Loading $IMAGE into the cluster node"
if ! docker exec -i "$NODE" true 2>/dev/null; then
  echo "cannot exec into node container '$NODE'." >&2
  echo "  This script loads the image with 'docker save | docker exec $NODE ctr -n k8s.io images import -'," >&2
  echo "  which needs the kind-style node Docker Desktop runs. Override with NODE=<name> if yours differs." >&2
  exit 1
fi
docker save "$IMAGE" | docker exec -i "$NODE" ctr -n k8s.io images import -
note "imported into the node's containerd (namespace k8s.io)"

PULL_POLICY=Never

# ----------------------------------------------------------------- install ---

step "Installing chart into $SYS_NS"

K create namespace "$SYS_NS" --dry-run=client -o yaml | K apply -f - >/dev/null

H upgrade --install "$RELEASE" "$CHART" \
  --namespace "$SYS_NS" \
  --set image.repository="${IMAGE%:*}" \
  --set image.tag="${IMAGE##*:}" \
  --set image.pullPolicy="$PULL_POLICY" \
  --set controller.logLevel="info,kube=warn" \
  --set-string controller.publishAddress="127.0.0.1" \
  --wait --timeout 3m

FULLNAME="$(H get manifest "$RELEASE" --namespace "$SYS_NS" \
  | awk '/^  name: /{print $2; exit}')"
FULLNAME="${FULLNAME:-$RELEASE-ramjet-ingress}"
note "release objects named: $FULLNAME"

# ---------------------------------------------------------------- workload ---

step "Deploying backends and Ingresses into $APP_NS"

K create namespace "$APP_NS" --dry-run=client -o yaml | K apply -f - >/dev/null

# A self-signed certificate for the TLS assertion. Regenerated every run, so a
# half-finished previous run cannot leave an expired one behind.
openssl req -x509 -newkey rsa:2048 -nodes -days 2 \
  -keyout "$WORK/tls.key" -out "$WORK/tls.crt" \
  -subj "/CN=$HOST/O=ramjet-e2e" \
  -addext "subjectAltName=DNS:$HOST" >/dev/null 2>&1

K -n "$APP_NS" create secret tls demo-tls \
  --cert="$WORK/tls.crt" --key="$WORK/tls.key" \
  --dry-run=client -o yaml | K apply -f - >/dev/null

cat >"$WORK/workload.yaml" <<YAML
apiVersion: apps/v1
kind: Deployment
metadata:
  name: echo-stable
  namespace: $APP_NS
spec:
  replicas: 1
  selector:
    matchLabels: { app: echo-stable }
  template:
    metadata:
      labels: { app: echo-stable }
    spec:
      containers:
        - name: echo
          image: ealen/echo-server:latest
          ports: [{ containerPort: 80, name: http }]
          readinessProbe:
            httpGet: { path: /, port: http }
            periodSeconds: 2
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: echo-canary
  namespace: $APP_NS
spec:
  replicas: 1
  selector:
    matchLabels: { app: echo-canary }
  template:
    metadata:
      labels: { app: echo-canary }
    spec:
      containers:
        - name: echo
          image: ealen/echo-server:latest
          ports: [{ containerPort: 80, name: http }]
          readinessProbe:
            httpGet: { path: /, port: http }
            periodSeconds: 2
---
apiVersion: v1
kind: Service
metadata:
  name: echo-stable
  namespace: $APP_NS
spec:
  selector: { app: echo-stable }
  ports: [{ name: http, port: 8080, targetPort: http }]
---
apiVersion: v1
kind: Service
metadata:
  name: echo-canary
  namespace: $APP_NS
spec:
  selector: { app: echo-canary }
  ports: [{ name: http, port: 8080, targetPort: http }]
---
# Production Ingress: host routing, plus the TLS Secret for the HTTPS
# assertion. Both listeners serve the same rule set, which is the point — the
# certificate is looked up by SNI, the route by Host.
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata:
  name: demo
  namespace: $APP_NS
spec:
  ingressClassName: ramjet
  tls:
    - hosts: ["$HOST"]
      secretName: demo-tls
  rules:
    - host: $HOST
      http:
        paths:
          - path: /
            pathType: Prefix
            backend:
              service:
                name: echo-stable
                port: { number: 8080 }
---
# Canary Ingress: same host and path, so it attaches to the production route
# above rather than creating one of its own. The annotation prefix is
# nginx.ingress.kubernetes.io on purpose — compatibility with what clusters
# already have written down.
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata:
  name: demo-canary
  namespace: $APP_NS
  annotations:
    nginx.ingress.kubernetes.io/canary: "true"
    nginx.ingress.kubernetes.io/canary-weight: "30"
spec:
  ingressClassName: ramjet
  rules:
    - host: $HOST
      http:
        paths:
          - path: /
            pathType: Prefix
            backend:
              service:
                name: echo-canary
                port: { number: 8080 }
YAML

K apply -f "$WORK/workload.yaml" >/dev/null

# ------------------------------------------------------------------- ready ---

step "Waiting for readiness"

K -n "$SYS_NS" rollout status "deploy/$FULLNAME" --timeout=3m
K -n "$APP_NS" rollout status deploy/echo-stable --timeout=2m
K -n "$APP_NS" rollout status deploy/echo-canary --timeout=2m

# Port-forwards rather than the LoadBalancer address. The controller Service is
# a LoadBalancer and Docker Desktop may or may not give it an external address,
# but a port-forward is deterministic on any cluster and tests the same Service
# object, so the assertions below do not depend on the local load balancer
# story. (Status writeback is asserted separately, and *does* exercise it.)
# A port-forward whose output goes to /dev/null fails invisibly, and every
# assertion downstream then reports a connection error instead of the real
# problem. Each forward gets a log, and the script refuses to continue until
# the local port is actually accepting connections.
start_forward() {
  local label="$1" target="$2"
  shift 2
  local log="$WORK/pf-$label.log"

  # Deliberately not the K() wrapper. Backgrounding a shell function makes $!
  # the PID of the subshell bash forks to run it, not of kubectl underneath —
  # so cleanup would kill the wrapper and leave kubectl holding the local port.
  # A leaked forward outlives the namespace it points at, and the next run then
  # fails to bind and asserts against a dead tunnel.
  kubectl --context "$CONTEXT" -n "$SYS_NS" port-forward "$target" "$@" >"$log" 2>&1 &
  local pid=$!
  PF_PIDS+=("$pid")

  local first_port="${1%%:*}"
  for _ in $(seq 1 40); do
    if ! kill -0 "$pid" 2>/dev/null; then
      echo "port-forward $label ($target) exited immediately:" >&2
      cat "$log" >&2
      exit 1
    fi
    # bash's own /dev/tcp is enough to answer "is anything listening", and
    # avoids depending on nc/lsof being present.
    if (exec 3<>"/dev/tcp/127.0.0.1/$first_port") 2>/dev/null; then
      exec 3>&- 2>/dev/null || true
      note "port-forward $label -> $target on :$first_port"
      return 0
    fi
    sleep 0.5
  done

  echo "port-forward $label ($target) never started listening on :$first_port:" >&2
  cat "$log" >&2
  exit 1
}

start_forward traffic "svc/$FULLNAME" "$HTTP_PORT:80" "$HTTPS_PORT:443"
start_forward admin "svc/$FULLNAME-admin" "$ADMIN_PORT:10254"

# Poll /readyz rather than sleeping: readiness in Kubernetes mode means a route
# table has been compiled, which is exactly the precondition every assertion
# below depends on.
READY=no
for _ in $(seq 1 60); do
  if curl -fsS -m 2 "http://127.0.0.1:$ADMIN_PORT/readyz" >/dev/null 2>&1; then
    READY=yes
    break
  fi
  sleep 2
done
[[ "$READY" == "yes" ]] || { echo "controller never became ready" >&2; K -n "$SYS_NS" logs "deploy/$FULLNAME" --tail=50 >&2 || true; exit 1; }
note "/readyz answered 200"

# The route table is published on its own debounce schedule; readiness only
# proves generation >= 1 exists, not that this particular Ingress is in it.
for _ in $(seq 1 30); do
  code="$(curl -s -o /dev/null -w '%{http_code}' -m 3 -H "Host: $HOST" "http://127.0.0.1:$HTTP_PORT/" || true)"
  [[ "$code" == "200" ]] && break
  sleep 2
done

# -------------------------------------------------------------- assertions ---

step "Assertions"

# 1. Host routing returns 200 and reaches a real backend pod.
#
#    "Pod identity" has to be read out of the right field. echo-server reports
#    `.host.hostname`, which is the *request's* Host header — it says
#    demo.ramjet.test no matter which pod answered, so asserting on it would
#    pass even if every request went to the same backend. The pod's own name is
#    the HOSTNAME entry of the reported environment, and that is what gets
#    checked against the namespace's actual pod list below.
backend_pod() {
  sed -n 's/.*"HOSTNAME":"\([^"]*\)".*/\1/p' <<<"$1" | head -1
}

REAL_PODS="$(K -n "$APP_NS" get pods -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}')"

BODY="$(curl -s -m 5 -H "Host: $HOST" "http://127.0.0.1:$HTTP_PORT/" || true)"
CODE="$(curl -s -o /dev/null -w '%{http_code}' -m 5 -H "Host: $HOST" "http://127.0.0.1:$HTTP_PORT/" || true)"
IDENTITY="$(backend_pod "$BODY")"
if [[ "$CODE" == "200" ]] && [[ -n "$IDENTITY" ]] && grep -qxF "$IDENTITY" <<<"$REAL_PODS"; then
  pass "host routing: HTTP 200, answered by pod $IDENTITY (a real pod in $APP_NS)"
else
  fail "host routing: expected 200 from a known pod, got $CODE from '${IDENTITY:-<no identity>}'"
fi

# 2. An unknown Host matches no rule and must not fall through to anything.
CODE="$(curl -s -o /dev/null -w '%{http_code}' -m 5 -H "Host: nope.ramjet.test" "http://127.0.0.1:$HTTP_PORT/" || true)"
if [[ "$CODE" == "404" ]]; then
  pass "unknown host: HTTP 404"
else
  fail "unknown host: expected 404, got $CODE"
fi

# 3. Canary weight. The split is a per-request random roll, so this is a
#    statistical assertion: 30% of 60 is 18, and the tolerance below is roughly
#    3 standard deviations of the binomial, which makes a spurious failure far
#    less likely than a real regression.
#    Classification goes through the same HOSTNAME field as assertion 1, not a
#    substring search of the whole body: Kubernetes injects ECHO_CANARY_* and
#    ECHO_STABLE_* service environment variables into *both* pods, so a naive
#    grep for the backend name matches every response regardless of who served
#    it.
STABLE=0
CANARY=0
OTHER=0
for _ in $(seq 1 60); do
  b="$(curl -s -m 5 -H "Host: $HOST" "http://127.0.0.1:$HTTP_PORT/" || true)"
  case "$(backend_pod "$b")" in
    echo-canary-*) CANARY=$((CANARY + 1)) ;;
    echo-stable-*) STABLE=$((STABLE + 1)) ;;
    *)             OTHER=$((OTHER + 1)) ;;
  esac
done
PCT=$((CANARY * 100 / 60))
if (( OTHER == 0 )) && (( PCT >= 10 )) && (( PCT <= 50 )); then
  pass "canary split: ${CANARY}/60 to canary (${PCT}%, target 30% ±20pp), ${STABLE} to stable"
else
  fail "canary split: ${CANARY}/60 canary (${PCT}%), ${STABLE} stable, ${OTHER} unattributable"
fi

# 4. HTTPS with SNI serves the Ingress's own certificate.
TLS_CODE="$(curl -sk -o /dev/null -w '%{http_code}' -m 5 --resolve "$HOST:$HTTPS_PORT:127.0.0.1" "https://$HOST:$HTTPS_PORT/" || true)"
SUBJECT="$(openssl s_client -connect "127.0.0.1:$HTTPS_PORT" -servername "$HOST" </dev/null 2>/dev/null \
  | openssl x509 -noout -subject 2>/dev/null || true)"
if [[ "$TLS_CODE" == "200" ]] && grep -q "CN *= *$HOST" <<<"$SUBJECT"; then
  pass "TLS with SNI: HTTP 200, served $(sed 's/^subject=//' <<<"$SUBJECT" | xargs)"
else
  fail "TLS with SNI: got $TLS_CODE, subject '${SUBJECT:-<none>}'"
fi

# 5. Ingress status writeback. The address comes from the publish Service's own
#    .status.loadBalancer where the cluster provides one, and from
#    --publish-address otherwise; either way the controller must write it.
LB_SVC="$(K -n "$SYS_NS" get svc "$FULLNAME" -o jsonpath='{.status.loadBalancer.ingress[0].ip}{.status.loadBalancer.ingress[0].hostname}' 2>/dev/null || true)"
STATUS=""
for _ in $(seq 1 30); do
  STATUS="$(K -n "$APP_NS" get ingress demo -o jsonpath='{.status.loadBalancer.ingress[0].ip}{.status.loadBalancer.ingress[0].hostname}' 2>/dev/null || true)"
  [[ -n "$STATUS" ]] && break
  sleep 2
done
if [[ -n "$STATUS" ]]; then
  pass "ingress status: .status.loadBalancer populated with '$STATUS' (publish Service address: '${LB_SVC:-<none, fell back to --publish-address>}')"
else
  fail "ingress status: .status.loadBalancer.ingress is empty"
fi

# 6. Metrics: traffic counted, and a compiled route table generation.
METRICS="$(curl -s -m 5 "http://127.0.0.1:$ADMIN_PORT/metrics" || true)"
REQ_TOTAL="$(awk '/^ramjet_requests_total\{/ {s += $2} END {print s+0}' <<<"$METRICS")"
GENERATION="$(awk '/^ramjet_route_table_generation / {print $2}' <<<"$METRICS")"
if (( REQ_TOTAL > 0 )) && [[ -n "$GENERATION" ]] && (( GENERATION > 0 )); then
  pass "metrics: ramjet_requests_total=$REQ_TOTAL, ramjet_route_table_generation=$GENERATION"
else
  fail "metrics: requests_total=$REQ_TOTAL generation=${GENERATION:-<absent>}"
fi

# ----------------------------------------------------------------- summary ---

step "Summary"
printf '%s\n' "${RESULTS[@]}"
printf '\n%d passed, %d failed. Image %s (%s).\n' "$PASSES" "$FAILURES" "$IMAGE" "$SIZE"

(( FAILURES == 0 )) || exit 1
