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

# Which data plane the release runs. `ENGINE=uring deploy/e2e.sh` puts the whole
# suite through the reactor engine instead of hyper.
#
# Worth doing, and not only for coverage: whether io_uring_setup is permitted
# inside a kubelet's containers depends on the node image, the container
# runtime, and the pod's seccomp profile, and none of those is knowable from
# here. With `uring` the replica falls back to hyper if the answer is no, and
# the run below reports which engine actually served rather than assuming. That
# report is the result.
ENGINE="${ENGINE:-hyper}"
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

for tool in docker kubectl helm openssl curl python3; do
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
# The build context is the *parent* directory, not this repository. The
# workspace's ramjet-engine crate depends on the `ramjet` runtime from the
# enhance-socket sibling checkout by path, so the Dockerfile copies both trees
# in and a context rooted here cannot see the second one.
docker build -f "$REPO_ROOT/Dockerfile" -t "$IMAGE" "$REPO_ROOT/.."
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
  --set engine="$ENGINE" \
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
# The mirror target. A third backend that receives a copy of everything the
# production route serves and whose answers are thrown away.
apiVersion: apps/v1
kind: Deployment
metadata:
  name: echo-shadow
  namespace: $APP_NS
spec:
  replicas: 1
  selector:
    matchLabels: { app: echo-shadow }
  template:
    metadata:
      labels: { app: echo-shadow }
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
  name: echo-shadow
  namespace: $APP_NS
spec:
  selector: { app: echo-shadow }
  ports: [{ name: http, port: 8080, targetPort: http }]
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
  annotations:
    # Mirroring is a property of the route, so it goes on the production
    # Ingress. The prefix is ramjet.dev because ingress-nginx has no equivalent
    # annotation to be compatible with.
    ramjet.dev/mirror-backend: $APP_NS/echo-shadow:8080
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
K -n "$APP_NS" rollout status deploy/echo-shadow --timeout=2m

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

# 7. Per-route counters: the same traffic, attributed to the route that served
#    it. Read-only, over the admin tunnel that is already open — this asserts
#    the JSON API is wired to the request path, and deliberately does not
#    exercise /admin/rollback, which mutates what the pod is serving and would
#    make every assertion above it depend on the order they run in.
#
#    Parsed rather than scraped. This used to be a sed expression matching
#    "host":"…" then [^}]* then "requests_total", which worked only because no
#    nested object happened to sort between those two keys — serde_json orders
#    them alphabetically, so adding "mirror" put a `}` in the middle and the
#    pattern silently matched nothing. A regex over JSON was going to break on
#    the first additive field either way.
ROUTES="$(curl -s -m 5 "http://127.0.0.1:$ADMIN_PORT/admin/routes" || true)"
ROUTE_STATS="$(printf '%s' "$ROUTES" | python3 -c '
import json, sys
host = sys.argv[1]
reqs = canary = 0
for r in json.load(sys.stdin).get("routes", []):
    if r.get("host") == host:
        reqs += r.get("requests_total", 0)
        canary += (r.get("canary_stats") or {}).get("requests_total", 0)
print(reqs, canary)
' "$HOST" || true)"
read -r ROUTE_REQS CANARY_REQS <<<"$ROUTE_STATS"

# The canary share is asserted as a *subset*: the route's totals must still
# count every request whoever served it, or every dashboard of that route steps
# down the moment somebody starts a canary.
if (( ${ROUTE_REQS:-0} > 0 )) && (( ${CANARY_REQS:-0} > 0 )) && (( CANARY_REQS < ROUTE_REQS )); then
  pass "per-route stats: $ROUTE_REQS requests for $HOST, of which $CANARY_REQS were the canary's"
else
  fail "per-route stats: total=${ROUTE_REQS:-<none>} canary=${CANARY_REQS:-<none>} for $HOST"
fi

# 8. Traffic mirroring. Everything above already sent well over a hundred
#    requests through the production route, and every one of them should have
#    been copied to echo-shadow. Two halves are asserted, because either alone
#    could be true while the feature is broken: the copies actually arrived at
#    the shadow backend, and none of them cost the client anything.
#
#    Read fresh rather than from the $METRICS snapshot above, and only once the
#    queue has drained. `ramjet_mirrored_total` is incremented *after* the
#    exchange completes — that is what its help text promises, "which accepted
#    the copy" — while the shadow's access log records on receipt. So while
#    copies are still in flight the shadow is legitimately ahead of the counter,
#    and comparing a stale snapshot against a later log read stacks a second
#    skew on top of that. Settle first, then the two agree.
mirror_counters() {
  curl -s -m 5 "http://127.0.0.1:$ADMIN_PORT/metrics" \
    | awk '/^ramjet_mirrored_total /       {m = $2}
           /^ramjet_mirror_failures_total /{f = $2}
           /^ramjet_mirror_dropped_total / {d = $2}
           END {print m+0, f+0, d+0}'
}
read -r MIRRORED MIRROR_FAILED MIRROR_DROPPED <<<"$(mirror_counters)"
for _ in $(seq 1 15); do
  sleep 1
  read -r M2 F2 D2 <<<"$(mirror_counters)"
  [[ "$M2 $F2 $D2" == "$MIRRORED $MIRROR_FAILED $MIRROR_DROPPED" ]] && break
  MIRRORED=$M2; MIRROR_FAILED=$F2; MIRROR_DROPPED=$D2
done

# The shadow's own view, so this is not just the proxy marking its own homework:
# the echo image logs every request it serves.
SHADOW_SAW="$(K -n "$APP_NS" logs deploy/echo-shadow --tail=-1 2>/dev/null | grep -c 'GET' || true)"
# `>=` rather than `==`: the shadow's log is the ground truth for what arrived,
# and nothing here stops another copy landing between the two reads. What would
# be a bug is the counter running *ahead* of the shadow, which would mean
# counting copies that were never accepted.
if (( ${MIRRORED:-0} > 0 )) && (( ${MIRROR_FAILED:-0} == 0 )) \
   && (( SHADOW_SAW >= MIRRORED )); then
  pass "mirroring: ${MIRRORED} copies accepted by echo-shadow, which logged ${SHADOW_SAW}; ${MIRROR_FAILED} failed, ${MIRROR_DROPPED} dropped"
else
  fail "mirroring: accepted=${MIRRORED:-<absent>} shadow_logged=${SHADOW_SAW} failed=${MIRROR_FAILED:-<absent>} dropped=${MIRROR_DROPPED:-<absent>}"
fi

# 9. The mirroring invariant: with the shadow backend gone, the production
#    route must keep answering. A mirror that is awaited, retried, or allowed
#    to fail a request would show up here as 5xx responses or as a wall-clock
#    blowout.
#
#    Measured in *steady state*, not across the churn. Scaling a Deployment to
#    zero removes a pod, empties an EndpointSlice, and makes the controller
#    compile and publish a new generation; sampling while all of that is in
#    flight measures Docker Desktop's networking as much as it measures this
#    proxy, and an earlier version of this assertion was flaky for exactly that
#    reason. So wait for the shadow's addresses to go and for a publish to land,
#    then measure. The tight, deterministic form of this property — a mirror
#    backend that accepts connections and then never answers — is
#    `a_catatonic_mirror_backend_does_not_slow_the_primary` in
#    crates/ramjet-proxy/tests/mirroring.rs.
K -n "$APP_NS" scale deploy/echo-shadow --replicas=0 >/dev/null
for _ in $(seq 1 30); do
  ADDRS="$(K -n "$APP_NS" get endpointslice -l kubernetes.io/service-name=echo-shadow \
    -o jsonpath='{.items[*].endpoints[*].addresses[*]}' 2>/dev/null || true)"
  [[ -z "$ADDRS" ]] && break
  sleep 2
done
# The rebuild is debounced and then published; a few seconds covers both.
sleep 3

BEFORE_5XX="$(curl -s -m 5 "http://127.0.0.1:$ADMIN_PORT/metrics" | awk '/^ramjet_requests_total\{code="5xx"\}/ {print $2}')"
DEAD_START=$SECONDS
DEAD_OK=0
for _ in $(seq 1 40); do
  c="$(curl -s -o /dev/null -w '%{http_code}' -m 5 -H "Host: $HOST" "http://127.0.0.1:$HTTP_PORT/" || true)"
  [[ "$c" == "200" ]] && DEAD_OK=$((DEAD_OK + 1))
done
DEAD_ELAPSED=$((SECONDS - DEAD_START))
AFTER_5XX="$(curl -s -m 5 "http://127.0.0.1:$ADMIN_PORT/metrics" | awk '/^ramjet_requests_total\{code="5xx"\}/ {print $2}')"
# Every request answering 200 is the assertion that matters: `curl -m 5` means a
# proxy that waited on the mirror would produce empty codes rather than slow
# ones. The wall clock is a second, looser net — if the mirror were awaited these
# 40 would cost at least its 5s deadline each, so around 200s; 60s leaves room
# for a port-forward having a bad minute while still catching that.
if (( DEAD_OK == 40 )) && (( DEAD_ELAPSED < 60 )) && (( ${AFTER_5XX:-1} == ${BEFORE_5XX:-0} )); then
  pass "mirroring invariant: 40/40 served in ${DEAD_ELAPSED}s with the shadow backend gone, no new 5xx"
else
  fail "mirroring invariant: ${DEAD_OK}/40 ok in ${DEAD_ELAPSED}s, 5xx ${BEFORE_5XX:-0} -> ${AFTER_5XX:-?}"
fi

K -n "$APP_NS" scale deploy/echo-shadow --replicas=1 >/dev/null

# 10. Canary auto-promotion, the whole loop: arm the canary with a short
#     interval and a low request floor, drive traffic through it, and watch the
#     controller step canary-weight up on its own. Both backends are healthy, so
#     the expected outcome is advancement rather than a rollback.
#
#     The floors are lowered from their defaults because an e2e cannot afford a
#     60-second window and 50 requests per side; the machine being exercised is
#     the same one either way, and its thresholds are annotations precisely so
#     that they can be set to what a given situation can supply.
K -n "$APP_NS" annotate ingress demo-canary --overwrite \
  ramjet.dev/auto-promote=true \
  ramjet.dev/auto-promote-interval=5s \
  ramjet.dev/auto-promote-min-requests=5 \
  ramjet.dev/auto-promote-steps=30,60,100 >/dev/null

PROMOTED=""
START_WEIGHT="$(K -n "$APP_NS" get ingress demo-canary \
  -o jsonpath='{.metadata.annotations.nginx\.ingress\.kubernetes\.io/canary-weight}')"
# Two windows' worth of chances. Each pass needs traffic on both sides within
# one interval, so traffic is driven *inside* the loop rather than up front.
for _ in $(seq 1 12); do
  for _ in $(seq 1 30); do
    curl -s -o /dev/null -m 5 -H "Host: $HOST" "http://127.0.0.1:$HTTP_PORT/" || true
  done
  W="$(K -n "$APP_NS" get ingress demo-canary \
    -o jsonpath='{.metadata.annotations.nginx\.ingress\.kubernetes\.io/canary-weight}')"
  if [[ -n "$W" ]] && [[ "$W" != "$START_WEIGHT" ]]; then
    PROMOTED="$W"
    break
  fi
  sleep 3
done

STATUS_ANN="$(K -n "$APP_NS" get ingress demo-canary \
  -o jsonpath='{.metadata.annotations.ramjet\.dev/auto-promote-status}' 2>/dev/null || true)"
if [[ -n "$PROMOTED" ]] && (( PROMOTED > START_WEIGHT )); then
  pass "auto-promotion: canary-weight advanced ${START_WEIGHT} -> ${PROMOTED} on healthy traffic"
elif [[ "$STATUS_ANN" == promoted ]]; then
  pass "auto-promotion: canary reached its last step (status: promoted)"
else
  fail "auto-promotion: weight stayed at ${START_WEIGHT}, status '${STATUS_ANN:-<none>}'"
fi

# 11. The audit trail for that decision. A promotion that happened but that
#     nobody can find out about afterwards is half a feature.
EVENT="$(K -n default get events \
  --field-selector reason=CanaryStepped -o jsonpath='{.items[0].note}' 2>/dev/null || true)"
if [[ -n "$EVENT" ]]; then
  pass "auto-promotion audit: Event CanaryStepped — ${EVENT}"
else
  note "no CanaryStepped Event found; this is a soft check (Events need RBAC and are best-effort)"
fi

# 12. Which engine actually served all of the above.
#
#     Asked rather than assumed. `--engine uring` falls back to hyper where
#     io_uring_setup is not permitted, and whether it is inside this cluster's
#     containers depends on the node image, the container runtime and the pod's
#     seccomp profile. A suite that passed without saying which data plane ran
#     it would be reporting half a result.
step "Which engine served"

POD="$(K -n "$SYS_NS" get pods -l app.kubernetes.io/name=ramjet-ingress \
  -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)"
LOG="$(K -n "$SYS_NS" logs "$POD" --tail=200 2>/dev/null || true)"

if grep -q "falling back to the hyper engine" <<<"$LOG"; then
  # `|| true` on both halves: under `set -o pipefail` a grep that matches
  # nothing fails the pipeline and takes the script with it — which is how the
  # first version of this block killed the run at exactly the moment it had
  # something to report.
  REASON="$(grep -o 'falling back.*' <<<"$LOG" | head -1 || true)"
  note "requested engine '$ENGINE'; the reactor would not start and it fell back"
  note "  $REASON"
  SERVED="hyper (fell back from $ENGINE)"
elif grep -qE 'engine[= ]+uring' <<<"$LOG"; then
  SERVED="uring"
else
  SERVED="hyper"
fi
note "served by: $SERVED"

# ----------------------------------------------------------------- summary ---

step "Summary"
printf '%s\n' "${RESULTS[@]}"
printf '\n%d passed, %d failed. Image %s (%s). Engine requested %s, served %s.\n' \
  "$PASSES" "$FAILURES" "$IMAGE" "$SIZE" "$ENGINE" "$SERVED"

(( FAILURES == 0 )) || exit 1
