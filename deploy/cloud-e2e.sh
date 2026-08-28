#!/usr/bin/env bash
#
# Validate the per-provider deployment tier, and prove one preset end to end.
#
# Three passes, cheapest first:
#
#   1. helm lint against every preset.
#   2. Every committed static manifest through `kubectl apply --dry-run=server`,
#      which runs the real admission and schema validation without persisting
#      anything.
#   3. A full install-and-route test of the baremetal-nodeport preset, which is
#      the one preset a local cluster can actually satisfy: it needs no cloud
#      load balancer controller, and Docker Desktop publishes NodePorts on
#      localhost.
#
# # Why the cloud presets are only dry-run
#
# They cannot be more than that here. An `aws-load-balancer-type: external`
# annotation means nothing without the AWS Load Balancer Controller watching,
# and the only cluster that would prove it is a real one in a real account. What
# a server-side dry-run does prove is the half that actually breaks in practice:
# that the manifest is schema-valid, that its field names are real, and that the
# API server accepts every object — which is where a typo'd annotation key or a
# malformed port block would show up.
#
# # Why every command names its context
#
# The kubeconfig on a developer's machine routinely holds production clusters.
# Every kubectl and helm call below carries an explicit --context/--kube-context
# and the preflight refuses anything that is not the expected local cluster.
# Nothing in this script is ever applied to a cloud context.

set -euo pipefail

CONTEXT="${CONTEXT:-docker-desktop}"
NS="${NS:-ramjet-cloud-e2e}"
APP_NS="${APP_NS:-ramjet-cloud-e2e-app}"
RELEASE="${RELEASE:-ramjet-cloud}"
IMAGE="${IMAGE:-ramjet-ingress:cloud-e2e}"
HOST="${HOST:-cloud.ramjet.test}"
BUILD="${BUILD:-1}"
KEEP="${KEEP:-0}"

# The fixed NodePorts the baremetal-nodeport preset pins. Docker Desktop
# publishes these on localhost, which is what makes assertion 3 possible.
NODE_HTTP=30080

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CHART="$REPO_ROOT/deploy/chart/ramjet-ingress"
PROVIDER_DIR="$REPO_ROOT/deploy/provider"
WORK="$(mktemp -d)"

K() { kubectl --context "$CONTEXT" "$@"; }
H() { helm --kube-context "$CONTEXT" "$@"; }

PASSES=0
FAILURES=0
RESULTS=()

pass() { PASSES=$((PASSES + 1)); RESULTS+=("PASS  $1"); printf '  \033[32mPASS\033[0m  %s\n' "$1"; }
fail() { FAILURES=$((FAILURES + 1)); RESULTS+=("FAIL  $1"); printf '  \033[31mFAIL\033[0m  %s\n' "$1"; }
skip() { RESULTS+=("SKIP  $1"); printf '  \033[33mSKIP\033[0m  %s\n' "$1"; }
step() { printf '\n\033[1m==> %s\033[0m\n' "$1"; }
note() { printf '     %s\n' "$1"; }

cleanup() {
  local status=$?
  if [[ "$KEEP" == "1" ]]; then
    printf '\nKEEP=1 — leaving %s and %s in place.\n' "$NS" "$APP_NS"
  else
    step "Teardown"
    H uninstall "$RELEASE" --namespace "$NS" --wait --timeout 2m >/dev/null 2>&1 || true
    K delete namespace "$APP_NS" --wait=false >/dev/null 2>&1 || true
    K delete namespace "$NS" --wait=false >/dev/null 2>&1 || true
    # Cluster-scoped, so deleting the namespace does not reach it.
    K delete ingressclass ramjet --ignore-not-found >/dev/null 2>&1 || true
    note "namespaces deleted (asynchronously), release uninstalled"
  fi
  rm -rf "$WORK"
  exit "$status"
}
trap cleanup EXIT

# ---------------------------------------------------------------- preflight --

step "Preflight"

for tool in kubectl helm docker; do
  command -v "$tool" >/dev/null || { echo "missing required tool: $tool" >&2; exit 1; }
done

K config get-contexts "$CONTEXT" >/dev/null 2>&1 \
  || { echo "context '$CONTEXT' is not in the kubeconfig; refusing to guess" >&2; exit 1; }

# The context name alone is not proof — names are arbitrary. Check the server
# really is a single-node local cluster before creating anything.
NODES="$(K get nodes -o jsonpath='{.items[*].metadata.name}' 2>/dev/null || true)"
[[ -n "$NODES" ]] || { echo "context '$CONTEXT' is unreachable; stopping rather than falling back" >&2; exit 1; }
case "$NODES" in
  *desktop*|*docker*) : ;;
  *) echo "context '$CONTEXT' has nodes [$NODES], which do not look like a local Docker Desktop cluster; refusing" >&2; exit 1 ;;
esac
note "context $CONTEXT, node(s): $NODES"

# # Why the traffic assertions run inside the node
#
# Docker Desktop does not publish NodePorts on the host: :30080 answers on the
# node's own address and is refused on the Mac's localhost. Measured, not
# assumed — `curl 127.0.0.1:30080` from the host gets nothing while the same
# request inside the node gets a reply from the proxy.
#
# So the assertions run inside the node container against the node's InternalIP.
# That is the more faithful test anyway: it is the real NodePort path, node
# address through kube-proxy to the pod, which is exactly what this preset
# claims to set up. A port-forward would have been reachable from here and would
# have proven only that the pod serves — bypassing the NodePort that is the
# entire subject of the test.
NODE="${NODE:-desktop-control-plane}"
docker exec -i "$NODE" true 2>/dev/null \
  || { echo "cannot exec into node container '$NODE'; override with NODE=<name>" >&2; exit 1; }
for tool in curl bash; do
  docker exec -i "$NODE" sh -c "command -v $tool >/dev/null" \
    || { echo "node container '$NODE' has no $tool; the traffic assertions need it" >&2; exit 1; }
done

NODE_IP="$(K get node "$NODE" -o jsonpath='{.status.addresses[?(@.type=="InternalIP")].address}')"
[[ -n "$NODE_IP" ]] || { echo "could not read the node's InternalIP" >&2; exit 1; }
note "node address for NodePort traffic: $NODE_IP"

# curl, run inside the node.
NCURL() { docker exec -i "$NODE" curl "$@"; }

PRESETS=()
for dir in "$PROVIDER_DIR"/*/; do
  [[ -f "$dir/values.yaml" ]] && PRESETS+=("$(basename "$dir")")
done
IFS=$'\n' PRESETS=($(sort <<<"${PRESETS[*]}")); unset IFS
note "${#PRESETS[@]} presets: ${PRESETS[*]}"

# ------------------------------------------------------------------- build ---

if [[ "$BUILD" == "1" ]]; then
  step "Building $IMAGE"
  # The build context is the *parent* directory: ramjet-engine depends on the
  # `ramjet` runtime from the enhance-socket sibling checkout by path, and a
  # context rooted at this repository cannot see it.
  docker build -f "$REPO_ROOT/Dockerfile" -t "$IMAGE" "$REPO_ROOT/.."
else
  step "Reusing $IMAGE (BUILD=0)"
  docker image inspect "$IMAGE" >/dev/null 2>&1 \
    || { echo "BUILD=0 but image '$IMAGE' is not present locally" >&2; exit 1; }
fi
SIZE="$(docker image inspect "$IMAGE" --format '{{.Size}}' | awk '{printf "%.1f MB", $1/1024/1024}')"
note "image size: $SIZE"

# Docker Desktop's Kubernetes runs a kind-style node whose containerd is a
# separate image store from the docker daemon's, so a freshly built image is
# invisible to the kubelet and a pod referencing it fails ErrImageNeverPull.
# Importing rather than pulling is what lets imagePullPolicy stay Never: the
# assertions then prove the image this script has ran, with no path by which the
# kubelet could substitute a registry copy.
step "Loading $IMAGE into the cluster node"
docker save "$IMAGE" | docker exec -i "$NODE" ctr -n k8s.io images import - >/dev/null
note "imported into the node's containerd (namespace k8s.io)"

# Does this image know --proxy-protocol? The flag and the chart value that emits
# it are being added on a separate track, so ask the binary rather than assume:
# the PROXY protocol assertion runs the moment the flag exists and is skipped
# (loudly) until then.
if docker run --rm --entrypoint /usr/local/bin/ramjet-ingressd "$IMAGE" --help 2>&1 | grep -q -- '--proxy-protocol'; then
  HAS_PROXY_PROTOCOL=1
else
  HAS_PROXY_PROTOCOL=0
fi

# ------------------------------------------------------------------- lint ----

step "helm lint, every preset"

for preset in "${PRESETS[@]}"; do
  if out="$(H lint "$CHART" --values "$PROVIDER_DIR/$preset/values.yaml" 2>&1)"; then
    pass "lint $preset"
  else
    fail "lint $preset"
    printf '%s\n' "$out" | sed 's/^/       /'
  fi
done

# ------------------------------------------------------- server-side schema ---

step "kubectl apply --dry-run=server, every static manifest"

# A real namespace, because a server-side dry-run of a namespaced object still
# resolves its namespace and fails if there is none. It holds nothing: every
# apply below is a dry-run, so the namespace is deleted empty.
K create namespace "$NS" --dry-run=client -o yaml | K apply -f - >/dev/null
note "scratch namespace $NS created (dry-run targets resolve against it)"

for preset in "${PRESETS[@]}"; do
  # Re-rendered at the scratch namespace rather than applying the committed
  # file as-is. The committed manifests hardcode namespace ramjet-ingress, and
  # creating that namespace on a machine whose kubeconfig holds production
  # clusters is exactly the habit this script exists to avoid. Everything else
  # about the render — every annotation, every field being validated — is
  # identical.
  helm template "$RELEASE" "$CHART" \
    --namespace "$NS" \
    --values "$PROVIDER_DIR/$preset/values.yaml" >"$WORK/$preset.yaml" 2>"$WORK/$preset.err" \
    || { fail "dry-run $preset (template failed)"; sed 's/^/       /' "$WORK/$preset.err"; continue; }

  if out="$(K apply --dry-run=server -f "$WORK/$preset.yaml" 2>&1)"; then
    objects="$(grep -c '^' <<<"$out")"
    pass "dry-run $preset ($objects objects accepted by the API server)"
  else
    fail "dry-run $preset"
    printf '%s\n' "$out" | sed 's/^/       /'
  fi
done

# Every committed manifest must also match what the chart renders today.
step "Committed manifests match the chart"
if out="$("$REPO_ROOT/deploy/render.sh" --check 2>&1)"; then
  pass "render.sh --check: $out"
else
  fail "render.sh --check: committed static manifests are stale"
  printf '%s\n' "$out" | sed 's/^/       /'
fi

# --------------------------------------------------------------- e2e: nodeport --

step "End to end: baremetal-nodeport"

H upgrade --install "$RELEASE" "$CHART" \
  --namespace "$NS" \
  --values "$PROVIDER_DIR/baremetal-nodeport/values.yaml" \
  --set image.repository="${IMAGE%:*}" \
  --set image.tag="${IMAGE##*:}" \
  --set image.pullPolicy=Never \
  --set-string controller.publishAddress="127.0.0.1" \
  --wait --timeout 3m

FULLNAME="$(H get manifest "$RELEASE" --namespace "$NS" | awk '/^  name: /{print $2; exit}')"
FULLNAME="${FULLNAME:-$RELEASE-ramjet-ingress}"
note "release objects named: $FULLNAME"

K create namespace "$APP_NS" --dry-run=client -o yaml | K apply -f - >/dev/null

cat >"$WORK/workload.yaml" <<YAML
apiVersion: apps/v1
kind: Deployment
metadata:
  name: echo
  namespace: $APP_NS
spec:
  replicas: 1
  selector:
    matchLabels: { app: echo }
  template:
    metadata:
      labels: { app: echo }
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
  name: echo
  namespace: $APP_NS
spec:
  selector: { app: echo }
  ports: [{ name: http, port: 8080, targetPort: http }]
---
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata:
  name: echo
  namespace: $APP_NS
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
                name: echo
                port: { number: 8080 }
YAML

K apply -f "$WORK/workload.yaml" >/dev/null
K -n "$NS" rollout status "deploy/$FULLNAME" --timeout=3m
K -n "$APP_NS" rollout status deploy/echo --timeout=2m

# The NodePort is what is under test, so the assertions go through it rather
# than through a port-forward — a forward would prove the pod works and say
# nothing about whether the Service published the port this preset pins.
NODE_PORT="$(K -n "$NS" get svc "$FULLNAME" -o jsonpath='{.spec.ports[?(@.name=="http")].nodePort}')"
if [[ "$NODE_PORT" == "$NODE_HTTP" ]]; then
  pass "nodePort pinned: http is :$NODE_PORT, as the preset asks"
else
  fail "nodePort pinned: expected :$NODE_HTTP, Service published :${NODE_PORT:-<none>}"
fi

SVC_TYPE="$(K -n "$NS" get svc "$FULLNAME" -o jsonpath='{.spec.type}')"
if [[ "$SVC_TYPE" == "NodePort" ]]; then
  pass "service type: NodePort"
else
  fail "service type: expected NodePort, got $SVC_TYPE"
fi

NODE_URL="http://$NODE_IP:$NODE_HTTP/"

# The route table is published on its own debounce schedule; a Ready pod only
# proves generation >= 1 exists, not that this Ingress made it in.
for _ in $(seq 1 30); do
  code="$(NCURL -s -o /dev/null -w '%{http_code}' -m 3 -H "Host: $HOST" "$NODE_URL" || true)"
  [[ "$code" == "200" ]] && break
  sleep 2
done

# "Pod identity" has to be read out of the right field. echo-server reports
# `.host.hostname`, which is the *request's* Host header — it says the test host
# no matter which pod answered. The pod's own name is the HOSTNAME entry of the
# reported environment, and that is what gets checked against the namespace's
# actual pod list.
backend_pod() { sed -n 's/.*"HOSTNAME":"\([^"]*\)".*/\1/p' <<<"$1" | head -1; }
REAL_PODS="$(K -n "$APP_NS" get pods -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}')"

BODY="$(NCURL -s -m 5 -H "Host: $HOST" "$NODE_URL" || true)"
CODE="$(NCURL -s -o /dev/null -w '%{http_code}' -m 5 -H "Host: $HOST" "$NODE_URL" || true)"
IDENTITY="$(backend_pod "$BODY")"
if [[ "$CODE" == "200" ]] && [[ -n "$IDENTITY" ]] && grep -qxF "$IDENTITY" <<<"$REAL_PODS"; then
  pass "routing through $NODE_IP:$NODE_HTTP: HTTP 200, answered by pod $IDENTITY (a real pod in $APP_NS)"
else
  fail "routing through $NODE_IP:$NODE_HTTP: expected 200 from a known pod, got $CODE from '${IDENTITY:-<no identity>}'"
fi

CODE="$(NCURL -s -o /dev/null -w '%{http_code}' -m 5 -H "Host: nope.ramjet.test" "$NODE_URL" || true)"
if [[ "$CODE" == "404" ]]; then
  pass "unknown host through the NodePort: HTTP 404"
else
  fail "unknown host through the NodePort: expected 404, got $CODE"
fi

# ------------------------------------------------------- e2e: proxy protocol --

step "End to end: PROXY protocol"

if (( ! HAS_PROXY_PROTOCOL )); then
  skip "PROXY protocol: this image's --help does not list --proxy-protocol, so the flag has not landed in the image under test"
  note "the chart value and its arg are wired; re-run once the daemon ships the flag"
else
  H upgrade --install "$RELEASE" "$CHART" \
    --namespace "$NS" \
    --values "$PROVIDER_DIR/baremetal-nodeport/values.yaml" \
    --set image.repository="${IMAGE%:*}" \
    --set image.tag="${IMAGE##*:}" \
    --set image.pullPolicy=Never \
    --set proxyProtocol.enabled=true \
    --set-string controller.publishAddress="127.0.0.1" \
    --wait --timeout 3m
  K -n "$NS" rollout status "deploy/$FULLNAME" --timeout=3m

  for _ in $(seq 1 30); do
    K -n "$NS" get pods -l app.kubernetes.io/instance="$RELEASE" \
      -o jsonpath='{.items[0].status.containerStatuses[0].ready}' 2>/dev/null | grep -q true && break
    sleep 2
  done

  # First: a plain GET, with no header, must now be refused. This is the half
  # that is easy to leave untested and is the whole risk of the feature — a
  # listener that accepts both shapes would let any client that can reach it
  # claim any source address it likes.
  BARE="$(NCURL -s -o /dev/null -w '%{http_code}' -m 5 -H "Host: $HOST" "$NODE_URL" || true)"
  if [[ "$BARE" != "200" ]]; then
    pass "PROXY protocol: a request with no header is refused (curl reports '${BARE}')"
  else
    fail "PROXY protocol: a request with no PROXY header still got 200 — the listener is accepting unheadered connections"
  fi

  # Then a hand-built PROXY v1 header in front of an ordinary GET, spoken
  # straight onto the socket with bash's /dev/tcp inside the node. The address
  # claimed is TEST-NET-3, which nothing on this network could legitimately be,
  # so seeing it echoed back proves the header was parsed rather than that some
  # real address coincided.
  SPOOF="203.0.113.7"
  RAW="$(docker exec -i "$NODE" bash -c "
    exec 3<>/dev/tcp/$NODE_IP/$NODE_HTTP || exit 1
    printf 'PROXY TCP4 $SPOOF 10.0.0.1 56324 $NODE_HTTP\r\nGET / HTTP/1.1\r\nHost: $HOST\r\nConnection: close\r\n\r\n' >&3
    timeout 10 cat <&3
  " 2>/dev/null || true)"
  XFF="$(sed -n 's/.*"x-forwarded-for":"\([^"]*\)".*/\1/p' <<<"$RAW" | head -1)"

  if [[ "$XFF" == *"$SPOOF"* ]]; then
    pass "PROXY v1: the address in the header ($SPOOF) reached the backend as X-Forwarded-For: $XFF"
  else
    fail "PROXY v1: expected $SPOOF in X-Forwarded-For, got '${XFF:-<none>}'"
  fi
fi

# ----------------------------------------------------------------- summary ---

step "Summary"
printf '%s\n' "${RESULTS[@]}"
printf '\n%d passed, %d failed. Image %s (%s).\n' "$PASSES" "$FAILURES" "$IMAGE" "$SIZE"

(( FAILURES == 0 )) || exit 1
