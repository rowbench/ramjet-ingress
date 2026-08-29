#!/usr/bin/env bash
#
# Stand three controllers and the backends up side by side on the k0s node.
#
#   ramjet-ingress engine=hyper, seccompProfile RuntimeDefault  (chart default)
#   ramjet-ingress engine=uring, seccompProfile Unconfined      (the opt-in)
#   ingress-nginx  4.15.1, chart defaults
#
# The two ramjet releases are separate rather than one release upgraded between
# runs. The engine choice is not the only difference between them — `uring`
# needs podSecurityContext.seccompProfile.type=Unconfined to start at all, which
# is a pod spec change and therefore a restart. Two releases turn contender
# rotation into a change of port number, which is what makes a 3x interleaved
# rotation affordable on a burstable four-vCPU box.
#
# That does mean the two ramjet arms differ in two things at once (engine and
# seccomp profile), and the report says so. It is the comparison asked for:
# "what a stock install does" against "what opting in does", not an isolation of
# the engine, which Phase 16 already measured with seccomp held constant.

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"

log "Namespaces"
for ns in "$NS_HYPER" "$NS_URING" "$NS_NGINX" "$NS_APP"; do
    K create namespace "$ns" --dry-run=client -o yaml | K apply -f - >/dev/null
done

# ---------------------------------------------------------------------------
# Backends. hashicorp/http-echo, two replicas each, as Phase 16 used — so the
# baseline number here is on the same axis as that phase's 22,363 rps.
#
#   echo-a  the stable backend every load run forwards to. Never mutated, and
#           additionally exposed on its own NodePort as the no-proxy baseline.
#   echo-b  the ramjet arm's endpoint-churn target.
#   echo-c  the ingress-nginx arm's endpoint-churn target.
#
# echo-b and echo-c are separate so that flipping a pod in or out of one
# Service recompiles exactly one controller's configuration. A shared churn
# Service would have made every endpoint mutation land on both at once, so each
# contender would have been measured while its rival was also recomputing.
#
# The Deployment selector is `app` only while the Service selector is
# `app` + `member`. That gap is the mechanism: relabelling a running pod
# `member=no` drops it from the EndpointSlice immediately, with the ReplicaSet
# none the wiser and no pod scheduled or killed. Scaling the Deployment would
# have measured the scheduler and called it the controller.
# ---------------------------------------------------------------------------

emit_backend() {
    local name="$1" text="$2"
    cat <<YAML
---
apiVersion: apps/v1
kind: Deployment
metadata: { name: $name, namespace: $NS_APP }
spec:
  replicas: 2
  selector: { matchLabels: { app: $name } }
  template:
    metadata: { labels: { app: $name, member: "yes" } }
    spec:
      containers:
        - name: echo
          image: hashicorp/http-echo:latest
          args: ["-text=$text", "-listen=:5678"]
          ports: [{ containerPort: 5678, name: http }]
          resources:
            requests: { cpu: 50m, memory: 24Mi }
          readinessProbe:
            httpGet: { path: /, port: http }
            periodSeconds: 2
---
apiVersion: v1
kind: Service
metadata: { name: $name, namespace: $NS_APP }
spec:
  selector: { app: $name, member: "yes" }
  ports: [{ name: http, port: 8080, targetPort: http }]
YAML
}

emit_ingresses() {
    local contender="$1" churn_svc="$2" class
    class="$(class_for "$contender")"
    cat <<YAML
---
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata: { name: stable-$contender, namespace: $NS_APP }
spec:
  ingressClassName: $class
  rules:
    - host: $STABLE_HOST
      http:
        paths:
          - path: /
            pathType: Prefix
            backend: { service: { name: echo-a, port: { number: 8080 } } }
YAML
    if [[ -n "$churn_svc" ]]; then
        cat <<YAML
---
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata: { name: churn-$contender, namespace: $NS_APP }
spec:
  ingressClassName: $class
  rules:
    - host: $CHURN_HOST
      http:
        paths:
          - path: /
            pathType: Prefix
            backend: { service: { name: $churn_svc, port: { number: 8080 } } }
YAML
    fi
}

log "Backends into $NS_APP"
{
    emit_backend echo-a "ok-a"
    emit_backend echo-b "ok-b"
    emit_backend echo-c "ok-c"
    cat <<YAML
---
apiVersion: v1
kind: Service
metadata: { name: echo-a-direct, namespace: $NS_APP }
spec:
  type: NodePort
  selector: { app: echo-a, member: "yes" }
  ports: [{ name: http, port: 8080, targetPort: http, nodePort: $NP_BASELINE }]
YAML
} | K apply -f - >/dev/null

for d in echo-a echo-b echo-c; do
    K -n "$NS_APP" rollout status "deploy/$d" --timeout=3m >/dev/null
done
sub "echo-a, echo-b, echo-c ready; baseline NodePort $NP_BASELINE serves echo-a directly"

# ---------------------------------------------------------------------------
# ramjet-ingress, twice. Only the values two-controllers-in-one-cluster forces
# are set, plus the engine and — for the uring arm alone — the seccomp profile
# the reactor needs. No tuning value the chart does not already ship.
#
# `kind`, `hostNetwork` and the two ports are in that first category rather than
# the second. The chart's default is now a hostNetwork DaemonSet on the node's
# :80 and :443, and three contenders on one node cannot all hold those; putting
# both ramjet arms in the pod network behind NodePorts is also the only way they
# are reached the same way ingress-nginx is, which is the comparison.
# ---------------------------------------------------------------------------

install_ramjet() {
    local contender="$1" engine="$2" seccomp="$3" np
    np="$(port_for "$contender")"
    log "Installing ramjet-ingress engine=$engine seccomp=$seccomp into $(ns_for "$contender")"
    H upgrade --install "${PREFIX}-${contender}" "$CHART_DIR" \
        --namespace "$(ns_for "$contender")" \
        --set fullnameOverride="${PREFIX}-${contender}" \
        --set image.repository="$RAMJET_IMAGE_REPO" \
        --set image.tag="$RAMJET_IMAGE_TAG" \
        --set image.pullPolicy=Always \
        --set engine="$engine" \
        --set podSecurityContext.seccompProfile.type="$seccomp" \
        --set controller.ingressClass="$(class_for "$contender")" \
        --set kind=Deployment \
        --set hostNetwork=false \
        --set ports.http=8080 \
        --set ports.https=8443 \
        --set service.type=NodePort \
        --set service.http.nodePort="$np" \
        --set service.https.nodePort="$((np + 300))" \
        --set-string controller.publishAddress="127.0.0.1" \
        --wait --timeout 5m >/dev/null
    sub "release ${PREFIX}-${contender}, class $(class_for "$contender"), nodePort $np"
}

install_ramjet hyper hyper RuntimeDefault
install_ramjet uring uring Unconfined

# ---------------------------------------------------------------------------
# ingress-nginx, official chart, everything else at its own defaults.
#
# Explicitly NOT set, so they stay the chart's:
#   controller.resources        requests cpu 100m / memory 90Mi and NO limits.
#                               ramjet runs under the 256Mi memory limit its own
#                               chart imposes, so this is the more generous side
#                               and it is left that way.
#   admissionWebhooks.enabled   true. Every Ingress write is validated by
#                               `nginx -t` in the controller pod before the API
#                               server accepts it. A real cost of the default
#                               install, left in.
#   controller.config           everything except disable-access-log: worker-
#                               processes auto, keep-alive 75, worker-connections
#                               16384, upstream-keepalive-connections 320.
#   replicaCount 1              matches ramjet's hard-coded 1.
#
# Two deviations, the same two the docker-desktop run made:
#
#   disable-access-log=true  Deliberate, and it makes ingress-nginx faster. At
#     the default nginx writes a log line for every request it forwards and
#     ramjet writes none, so leaving it on would charge one contender for work
#     its opponent never does. It also keeps the controller log readable, which
#     matters because that log is where reloads are counted.
#
#   progressDeadlineSeconds=600  Forced. Chart 4.15.1 ships
#     progressDeadlineSeconds: 0 next to minReadySeconds: 0, and Kubernetes 1.36
#     rejects a Deployment whose progress deadline is not greater than its
#     minReadySeconds. 600 is Kubernetes' own default for the field and affects
#     rollout reporting only.
# ---------------------------------------------------------------------------

log "Installing ingress-nginx $NGINX_CHART_VERSION into $NS_NGINX"
H upgrade --install "$REL_NGINX" ingress-nginx/ingress-nginx \
    --version "$NGINX_CHART_VERSION" \
    --namespace "$NS_NGINX" \
    --set fullnameOverride="${PREFIX}-nginx" \
    --set controller.ingressClassResource.name="$CLASS_NGINX" \
    --set controller.ingressClassResource.controllerValue="k8s.io/$CLASS_NGINX" \
    --set controller.ingressClassResource.default=false \
    --set controller.service.type=NodePort \
    --set controller.service.nodePorts.http="$NP_NGINX" \
    --set controller.service.nodePorts.https="$((NP_NGINX + 300))" \
    --set controller.progressDeadlineSeconds=600 \
    --set controller.config.disable-access-log="true" \
    --wait --timeout 8m >/dev/null
sub "release $REL_NGINX, class $CLASS_NGINX, nodePort $NP_NGINX"

log "Ingresses"
{
    emit_ingresses hyper echo-b
    emit_ingresses uring ""
    emit_ingresses nginx echo-c
} | K apply -f - >/dev/null

# ---------------------------------------------------------------------------
# Gates. Nothing is measured until every data plane routes, every unrouted Host
# 404s (the propagation benchmark's "poll until 200" would otherwise be polling
# something already answering), and — the one that matters most — the uring
# release is actually running the reactor rather than having fallen back.
# ---------------------------------------------------------------------------

log "Verifying data planes"
for c in hyper uring nginx baseline; do
    wait_for_route "$c" "$STABLE_HOST" 150 || die "$c never served $STABLE_HOST on $(target_for "$c")"
    sub "$c: $(target_for "$c") serves $STABLE_HOST"
done
for c in hyper nginx; do
    wait_for_route "$c" "$CHURN_HOST" 120 || die "$c never served $CHURN_HOST"
done

for c in hyper uring nginx; do
    code="$(curl -s -o /dev/null -w '%{http_code}' -m 5 -H "Host: nothing-here.ec2.test" \
            "http://$(target_for "$c")/" 2>/dev/null || true)"
    [[ "$code" == "404" ]] || die "$c answered $code for an unrouted Host, expected 404"
done
sub "all three 404 an unrouted Host"

log "Engine check"
assert_engine hyper hyper
assert_engine uring uring
sub "hyper: $(engine_line hyper)   uring: $(engine_line uring)"

log "Setup complete"
