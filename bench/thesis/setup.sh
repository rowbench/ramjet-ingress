#!/usr/bin/env bash
#
# Stand both contenders up side by side in the local cluster:
#
#   ramjet-ingress  (deploy/chart, image built from HEAD)  class ramjet-thesis-ramjet
#   ingress-nginx   (official chart, defaults)             class ramjet-thesis-nginx
#
# plus one shared set of echo backends and the Ingresses the benchmarks mutate.
#
# Idempotent: re-running upgrades in place. `bench/thesis/teardown.sh` removes
# everything, including the cluster-scoped objects a namespace delete would miss.

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"

log "Preflight"
preflight

# ---------------------------------------------------------------------------
# The --label is not decoration, and it cost an hour to find.
#
# deploy/e2e.sh has already built and side-loaded an image from this same
# Dockerfile and the same source tree, so a plain rebuild here produces a
# byte-identical config and therefore the *same* image ID. Importing a second
# tag onto an image ID that containerd's CRI plugin already holds leaves the
# new reference resolvable by `ctr` and by `crictl inspecti` but invisible to
# the kubelet, which then reports ErrImageNeverPull for an image that is
# demonstrably present. A label the other image does not carry gives this build
# its own config digest, its own image ID, and a reference the kubelet can find.
# ---------------------------------------------------------------------------

# The build source is `git archive` of a named commit, not the working tree.
# Another agent is developing in this checkout concurrently, and a benchmark
# whose binary depends on when it happened to run is not a measurement of
# anything. The commit is in the image tag, so the tag names exactly what was
# measured.
RAMJET_COMMIT="${RAMJET_IMAGE##*:}"

if docker image inspect "$RAMJET_IMAGE" >/dev/null 2>&1 && [[ "${REBUILD:-0}" != "1" ]]; then
    log "Reusing $RAMJET_IMAGE ($(docker image inspect "$RAMJET_IMAGE" --format '{{.Id}}'))"
    sub "REBUILD=1 to force"
else
    log "Building $RAMJET_IMAGE from git archive $RAMJET_COMMIT"
    src="$(mktemp -d)"
    git -C "$REPO_DIR" archive "$RAMJET_COMMIT" | tar -x -C "$src"
    docker build --label thesis.bench=phase6a -t "$RAMJET_IMAGE" "$src" >/dev/null
    rm -rf "$src"
    sub "$(docker image inspect "$RAMJET_IMAGE" --format '{{.Id}}')"
fi

# The node's containerd is a different image store from the docker daemon's, so
# a freshly built image is not visible to the kubelet until it is imported.
# Same mechanism deploy/e2e.sh uses.

log "Loading $RAMJET_IMAGE into the node's containerd"
docker save "$RAMJET_IMAGE" | docker exec -i "$NODE_CONTAINER" ctr -n k8s.io images import - >/dev/null
sub "imported"

log "Creating namespaces"
for ns in "$NS_RAMJET" "$NS_NGINX" "$NS_APP"; do
    K create namespace "$ns" --dry-run=client -o yaml | K apply -f - >/dev/null
done

# ---------------------------------------------------------------------------
# Contender A: ramjet-ingress, from this repo's chart at its own defaults.
#
# The only values set are the ones two-controllers-in-one-cluster forces:
# a unique class, a unique name, a locally-loaded image, and NodePort. No
# tuning value the chart does not already ship is applied.
# ---------------------------------------------------------------------------

log "Installing ramjet-ingress into $NS_RAMJET"
H upgrade --install "$RAMJET_RELEASE" "$REPO_DIR/deploy/chart/ramjet-ingress" \
    --namespace "$NS_RAMJET" \
    --set fullnameOverride="$PREFIX-ramjet" \
    --set image.repository="${RAMJET_IMAGE%:*}" \
    --set image.tag="${RAMJET_IMAGE##*:}" \
    --set image.pullPolicy=Never \
    --set controller.ingressClass="$RAMJET_CLASS" \
    --set service.type=NodePort \
    --set service.http.nodePort="$RAMJET_NODEPORT_HTTP" \
    --set service.https.nodePort="$RAMJET_NODEPORT_HTTPS" \
    --set-string controller.publishAddress="127.0.0.1" \
    --wait --timeout 3m >/dev/null
sub "release $RAMJET_RELEASE, class $RAMJET_CLASS, nodePort $RAMJET_NODEPORT_HTTP"

# ---------------------------------------------------------------------------
# Contender B: ingress-nginx, official chart, everything else at defaults.
#
# Explicitly NOT set, so they stay at the chart's own values:
#   controller.config          only disable-access-log (see below); everything
#                                       else is the chart's: worker-processes
#                                       auto, keep-alive 75s, worker-connections
#                                       16384, upstream-keepalive-connections 320
#   controller.resources               requests cpu 100m / memory 90Mi, and
#                                       NO limits. ramjet runs under a 256Mi
#                                       memory limit its own chart imposes, so
#                                       this is the more generous side.
#   controller.admissionWebhooks.enabled true  -- every Ingress write is
#                                       validated by `nginx -t` in the
#                                       controller pod before the API server
#                                       accepts it. That is a real cost of the
#                                       default install and it is left in.
#   controller.replicaCount    1        (matches ramjet's hard-coded 1)
#
# disable-access-log is a deliberate deviation, and it is one that helps
# ingress-nginx rather than this project. At the chart default nginx writes a
# log line to stdout for every request it forwards, and ramjet writes nothing
# per request — so leaving it on would have charged ingress-nginx for work its
# opponent never does. bench/run.sh made exactly this choice for plain nginx
# and recorded why; this keeps the two benchmark suites methodologically
# consistent. It also keeps the controller's own log readable, which matters
# because that log is where reloads are counted: several million access-log
# lines would rotate the evidence out of existence.
#
# progressDeadlineSeconds is the other exception, and it is not a tuning choice:
# chart 4.15.1 ships `progressDeadlineSeconds: 0` and `minReadySeconds: 0`, and
# the API server on Kubernetes 1.36 rejects a Deployment whose progress deadline
# is not greater than its minReadySeconds. The chart simply does not install
# here without it. 600 is the Kubernetes default for the field, so this restores
# the upstream default rather than choosing a value; it affects rollout
# reporting only and nothing any benchmark measures.
# ---------------------------------------------------------------------------

log "Installing ingress-nginx $NGINX_CHART_VERSION into $NS_NGINX"
H upgrade --install "$NGINX_RELEASE" ingress-nginx/ingress-nginx \
    --version "$NGINX_CHART_VERSION" \
    --namespace "$NS_NGINX" \
    --set fullnameOverride="$PREFIX-nginx" \
    --set controller.ingressClassResource.name="$NGINX_CLASS" \
    --set controller.ingressClassResource.controllerValue="k8s.io/$NGINX_CLASS" \
    --set controller.ingressClassResource.default=false \
    --set controller.service.type=NodePort \
    --set controller.service.nodePorts.http="$NGINX_NODEPORT_HTTP" \
    --set controller.service.nodePorts.https="$NGINX_NODEPORT_HTTPS" \
    --set controller.progressDeadlineSeconds=600 \
    --set controller.config.disable-access-log="true" \
    --wait --timeout 6m >/dev/null
sub "release $NGINX_RELEASE, class $NGINX_CLASS, nodePort $NGINX_NODEPORT_HTTP"

# ---------------------------------------------------------------------------
# Workload.
#
#   echo-a  the stable backend every load run forwards to. Never mutated.
#   echo-b  ramjet's endpoint-churn target.
#   echo-c  ingress-nginx's endpoint-churn target.
#
# echo-b and echo-c exist separately so that flipping a pod in or out of one
# Service recompiles exactly one controller's configuration. A shared churn
# Service would have made every endpoint mutation land on both controllers at
# once, so each contender would have been measured while its rival was also
# recomputing.
#
# The Deployment selector is `app` only while the Service selector is
# `app` + `member`. That gap is the mechanism: relabelling a running pod
# `member=no` removes it from the Service's EndpointSlice immediately, without
# the ReplicaSet noticing a thing and without waiting for a pod to schedule.
# Scaling the Deployment would have measured the scheduler.
# ---------------------------------------------------------------------------

log "Deploying backends into $NS_APP"

# The backend is nginx returning a fixed 128-byte body from memory, copied from
# bench/upstream.conf for the same reason that file gives: the upstream must
# not be the bottleneck.
#
# The first attempt used ealen/echo-server, because it reports the pod that
# answered and that is convenient. It capped the whole topology at ~400 rps —
# a Node.js process serialising a JSON description of the request is two orders
# of magnitude slower than either proxy in front of it, so every number would
# have been a measurement of the backend. The pod identity it provided is
# replaced by a per-Deployment marker string in the body, which is all the
# backend-swap benchmark actually needs.
emit_backend() {
    local name="$1" replicas="$2" marker="$3"
    cat <<YAML
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: $name-conf
  namespace: $NS_APP
data:
  nginx.conf: |
    worker_processes 2;
    error_log /dev/stderr error;
    pid /tmp/nginx.pid;
    events { worker_connections 16384; multi_accept on; }
    http {
      access_log off;
      keepalive_requests 100000000;
      keepalive_timeout 300s;
      server {
        listen 8080 default_server reuseport backlog=8192;
        location / {
          default_type text/plain;
          return 200 "$marker ramjet-thesis backend payload, fixed size so every contender moves identical bytes 012345678901234\n";
        }
      }
    }
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: $name
  namespace: $NS_APP
spec:
  replicas: $replicas
  selector:
    matchLabels: { app: $name }
  template:
    metadata:
      labels: { app: $name, member: "yes" }
      annotations: { conf/marker: "$marker" }
    spec:
      containers:
        - name: nginx
          image: nginx:1-alpine
          ports: [{ containerPort: 8080, name: http }]
          volumeMounts:
            - { name: conf, mountPath: /etc/nginx/nginx.conf, subPath: nginx.conf }
          resources:
            requests: { cpu: 50m, memory: 24Mi }
          readinessProbe:
            httpGet: { path: /, port: http }
            periodSeconds: 2
      volumes:
        - name: conf
          configMap: { name: $name-conf }
---
apiVersion: v1
kind: Service
metadata:
  name: $name
  namespace: $NS_APP
spec:
  selector: { app: $name, member: "yes" }
  ports: [{ name: http, port: 8080, targetPort: http }]
YAML
}

emit_ingresses() {
    local contender="$1" class churn_svc
    class="$(class_for "$contender")"
    case "$contender" in
        ramjet) churn_svc=echo-b ;;
        nginx)  churn_svc=echo-c ;;
    esac
    cat <<YAML
---
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata:
  name: stable-$contender
  namespace: $NS_APP
spec:
  ingressClassName: $class
  rules:
    - host: $STABLE_HOST
      http:
        paths:
          - path: /
            pathType: Prefix
            backend:
              service: { name: echo-a, port: { number: 8080 } }
---
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata:
  name: churn-$contender
  namespace: $NS_APP
spec:
  ingressClassName: $class
  rules:
    - host: $CHURN_HOST
      http:
        paths:
          - path: /
            pathType: Prefix
            backend:
              service: { name: $churn_svc, port: { number: 8080 } }
YAML
}

{
    emit_backend echo-a 2 backend-a
    emit_backend echo-b 2 backend-b
    emit_backend echo-c 2 backend-c
    emit_ingresses ramjet
    emit_ingresses nginx
} | K apply -f - >/dev/null

for d in echo-a echo-b echo-c; do
    K -n "$NS_APP" rollout status "deploy/$d" --timeout=3m >/dev/null
done
sub "echo-a, echo-b, echo-c ready"

# ---------------------------------------------------------------------------
# Gate: both data planes must actually route before anything is measured.
# ---------------------------------------------------------------------------

log "Verifying both data planes"
for contender in ramjet nginx; do
    wait_for_route "$contender" "$STABLE_HOST" 120 \
        || die "$contender never served $STABLE_HOST through $(target_for "$contender")"
    wait_for_route "$contender" "$CHURN_HOST" 120 \
        || die "$contender never served $CHURN_HOST through $(target_for "$contender")"
    sub "$contender: $(target_for "$contender") serves both hosts"
done

# An unrouted Host must 404 on both, or the propagation benchmark's
# "poll until 200" is polling something that was already answering.
for contender in ramjet nginx; do
    code="$(docker run --rm --network "$KIND_NET" curlimages/curl:latest \
                -s -o /dev/null -w '%{http_code}' -m 5 \
                -H "Host: nothing-here.thesis.test" "http://$(target_for "$contender")/" 2>/dev/null || true)"
    [[ "$code" == "404" ]] || die "$contender answered $code for an unrouted Host, expected 404"
done
sub "both 404 an unrouted Host"

log "Setup complete"
