#!/usr/bin/env bash
#
# Shared plumbing for the thesis benchmarks: cluster guard rails, kubectl/helm
# wrappers that can only ever talk to the local cluster, and the small helpers
# every benchmark needs.
#
# # Why the context is named on every single call
#
# The kubeconfig on this machine holds two production clusters (an AKS cluster
# and an EKS cluster). `kubectl config use-context` is process-global state,
# and a script that changes it — or that relies on whatever it happens to be —
# is one typo away from deleting a production namespace. Every kubectl below
# goes through K(), every helm through H(), and both hard-code
# --context/--kube-context. The preflight then refuses to proceed unless the
# cluster on the other end really is a single-node Docker Desktop node, because
# a context *name* is arbitrary and proves nothing.

set -euo pipefail

CONTEXT="${CONTEXT:-docker-desktop}"
NODE_CONTAINER="${NODE_CONTAINER:-desktop-control-plane}"

# Everything this suite creates is namespaced under this prefix so a teardown
# can be exhaustive without guessing, and so nothing can collide with the
# unrelated namespaces already on this cluster.
PREFIX="ramjet-thesis"
NS_RAMJET="${PREFIX}-ramjet"
NS_NGINX="${PREFIX}-nginx"
NS_APP="${PREFIX}-app"

RAMJET_CLASS="${PREFIX}-ramjet"
NGINX_CLASS="${PREFIX}-nginx"

RAMJET_RELEASE="${PREFIX}-ramjet"
NGINX_RELEASE="${PREFIX}-nginx"

RAMJET_IMAGE="${RAMJET_IMAGE:-ramjet-thesis:8078948}"
NGINX_CHART_VERSION="${NGINX_CHART_VERSION:-4.15.1}"

# The load generator and the idle-connection holders run as containers on the
# `kind` bridge, which is the network the Kubernetes node container sits on.
# That means load reaches the cluster over a NodePort at the node's own bridge
# address, with no `kubectl port-forward` in the path.
#
# port-forward was the other option (deploy/e2e.sh uses it) and was rejected on
# measurement grounds: it is a single SPDY-multiplexed stream through a Go
# proxy on the host, so at c64 it becomes the bottleneck and every number would
# be a measurement of kubectl. The NodePort path is the same iptables DNAT for
# both contenders, so it is fair, and it is fast enough not to matter.
KIND_NET="${KIND_NET:-kind}"

OHA_IMAGE="${OHA_IMAGE:-ghcr.io/hatoo/oha:latest}"

THESIS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "${THESIS_DIR}/../.." && pwd)"
RESULTS_DIR="${THESIS_DIR}/results"

STABLE_HOST="stable.thesis.test"
CHURN_HOST="churn.thesis.test"

K() { kubectl --context "$CONTEXT" "$@"; }
H() { helm --kube-context "$CONTEXT" "$@"; }

log()  { printf '\033[1;34m==>\033[0m %s\n' "$*"; }
sub()  { printf '    %s\n' "$*"; }
warn() { printf '\033[1;33m[!]\033[0m %s\n' "$*" >&2; }
die()  { printf '\033[1;31m[x]\033[0m %s\n' "$*" >&2; exit 1; }

# ---------------------------------------------------------------------------
# Guard rails
# ---------------------------------------------------------------------------

preflight() {
    for tool in docker kubectl helm python3; do
        command -v "$tool" >/dev/null || die "missing required tool: $tool"
    done
    docker info >/dev/null 2>&1 || die "the docker daemon is not responding"

    K config get-contexts "$CONTEXT" >/dev/null 2>&1 \
        || die "context '$CONTEXT' is not in the kubeconfig; refusing to guess"

    local nodes
    nodes="$(K get nodes -o jsonpath='{.items[*].metadata.name}' 2>/dev/null || true)"
    [[ -n "$nodes" ]] || die "context '$CONTEXT' is unreachable; stopping rather than falling back"

    # A single node whose name looks like Docker Desktop's. Anything with more
    # than one node, or with a node name that does not match, is not this
    # cluster and this script has no business writing to it.
    local count
    count="$(wc -w <<<"$nodes" | tr -d ' ')"
    [[ "$count" == "1" ]] \
        || die "context '$CONTEXT' has $count nodes [$nodes]; a local Docker Desktop cluster has one. Refusing."
    case "$nodes" in
        *desktop*|*docker*) : ;;
        *) die "context '$CONTEXT' node is [$nodes], which does not look like Docker Desktop. Refusing." ;;
    esac

    docker exec -i "$NODE_CONTAINER" true 2>/dev/null \
        || die "cannot exec into node container '$NODE_CONTAINER'"

    docker network inspect "$KIND_NET" >/dev/null 2>&1 \
        || die "docker network '$KIND_NET' does not exist; the load path needs it"

    sub "context $CONTEXT, node $nodes, load path: docker network $KIND_NET -> NodePort"
}

# The docker daemon is shared with another agent who builds Rust images on it.
# A `cargo build --release` inside the VM takes every core the Kubernetes node
# also needs, and a run measured through one is a measurement of the build: the
# first smoke run of benchmark 1 landed during one and produced 1.7-second
# responses on an *unchurned* baseline. So every measured run waits for the
# machine to go quiet first, and says so when it has to.
#
# Idle CPU, not load average. /proc/loadavg inside this VM sits between 10 and
# 17 with 23% of the CPU idle — Docker Desktop's virtio and softirq threads
# spend their lives in uninterruptible sleep, which load average counts and
# which costs nothing. Idle time from /proc/stat is the honest signal. Both are
# read through the node container, which shares the VM's kernel.
vm_idle_pct() {
    docker exec "$NODE_CONTAINER" sh -c \
        'read _ a b c d e f g h _ < /proc/stat; sleep 2; read _ A B C D E F G H _ < /proc/stat;
         echo $(( (D - d) * 100 / ((A+B+C+D+E+F+G+H) - (a+b+c+d+e+f+g+h)) ))' 2>/dev/null || echo 0
}

# Foreign containers, not just CPU idle. Rounds 3 and 4 of benchmark 1 were
# measured at 28k rps against rounds 1 and 2's 100k, because the other agent
# started their *own* proxy benchmark on this daemon partway through — six
# containers driving load, which left the VM at 27% idle, just above the old
# threshold. An idle percentage alone cannot tell "nothing is happening" from
# "somebody else's benchmark is happening and leaving me scraps", so the gate
# now also refuses to start while any container it does not own is running.
foreign_containers() {
    docker ps --format '{{.Names}}' 2>/dev/null \
        | grep -vc "^${PREFIX}-" || true
}

wait_for_quiet() {
    local budget="${1:-2400}" min_idle="${2:-60}" streak=0 waited=0 idle builds foreign
    while (( waited < budget )); do
        builds="$(pgrep -f 'docker build' 2>/dev/null | wc -l | tr -d ' ')"
        foreign="$(foreign_containers)"
        idle="$(vm_idle_pct)"
        if [[ "$builds" == "0" ]] && [[ "$foreign" == "0" ]] && (( idle >= min_idle )); then
            streak=$((streak + 1))
            (( streak >= 2 )) && { (( waited > 0 )) && sub "machine quiet (${idle}% idle) after ${waited}s"; return 0; }
        else
            (( streak > 0 || waited == 0 )) && sub "waiting for a quiet machine: ${idle}% VM CPU idle, ${builds} docker build(s), ${foreign} foreign container(s)"
            streak=0
        fi
        sleep 8
        waited=$((waited + 10))
    done
    warn "still busy after ${budget}s (${idle}% idle, ${foreign} foreign containers); proceeding and recording it"
    return 1
}

node_ip() {
    docker inspect "$NODE_CONTAINER" \
        --format "{{(index .NetworkSettings.Networks \"${KIND_NET}\").IPAddress}}"
}

# ---------------------------------------------------------------------------
# Addressing
# ---------------------------------------------------------------------------

# Both controller Services are NodePort on a fixed port, and the measured entry
# point is that port on the node container's bridge address.
#
# Both charts default to type LoadBalancer, and both would then ask Docker
# Desktop for host :80 — they would collide with each other, and one of the two
# would sit Pending. Pinning both to NodePort is therefore a deviation forced by
# running two ingress controllers side by side, and it is applied identically to
# both: the packet path (bridge -> nodePort -> kube-proxy DNAT -> pod) is the
# same object graph for each contender, so nothing is measured that only one of
# them pays for.
RAMJET_NODEPORT_HTTP=31080
RAMJET_NODEPORT_HTTPS=31443
NGINX_NODEPORT_HTTP=31081
NGINX_NODEPORT_HTTPS=31444

nodeport_for() {
    case "$1" in
        ramjet) echo "$RAMJET_NODEPORT_HTTP" ;;
        nginx)  echo "$NGINX_NODEPORT_HTTP" ;;
        *) die "unknown contender $1" ;;
    esac
}

target_for() { printf '%s:%s' "$(node_ip)" "$(nodeport_for "$1")"; }

class_for() {
    case "$1" in
        ramjet) echo "$RAMJET_CLASS" ;;
        nginx)  echo "$NGINX_CLASS" ;;
        *) die "unknown contender $1" ;;
    esac
}

ns_for() {
    case "$1" in
        ramjet) echo "$NS_RAMJET" ;;
        nginx)  echo "$NS_NGINX" ;;
        *) die "unknown contender $1" ;;
    esac
}

# The running controller pod, not merely the first one listed: ingress-nginx's
# chart runs admission-certificate Jobs in the same namespace, and a Completed
# Job pod that sorted first would have been sampled for CPU and memory instead
# of the controller.
pod_for() {
    local ns; ns="$(ns_for "$1")"
    K -n "$ns" get pods --field-selector status.phase=Running \
        -o jsonpath='{.items[0].metadata.name}'
}

# ---------------------------------------------------------------------------
# Resource sampling
#
# metrics-server is not installed on this cluster, so `kubectl top` is not
# available. crictl on the node reads the same cgroup counters the kubelet
# would report, which is close enough and does not require installing anything
# into the cluster under measurement.
# ---------------------------------------------------------------------------

pod_stats() {
    local pod="$1"
    docker exec "$NODE_CONTAINER" crictl stats --output json 2>/dev/null \
        | python3 -c '
import json, sys
pod = sys.argv[1]
try:
    d = json.load(sys.stdin)
except Exception:
    print("cpu_ns=0 mem_bytes=0"); sys.exit()
for s in d.get("stats", []):
    labels = s.get("attributes", {}).get("labels", {})
    if labels.get("io.kubernetes.pod.name") == pod:
        cpu = int(s.get("cpu", {}).get("usageCoreNanoSeconds", {}).get("value", 0) or 0)
        mem = int(s.get("memory", {}).get("workingSetBytes", {}).get("value", 0) or 0)
        print(f"cpu_ns={cpu} mem_bytes={mem}")
        break
else:
    print("cpu_ns=0 mem_bytes=0")
' "$pod"
}

# ---------------------------------------------------------------------------
# Waiting
# ---------------------------------------------------------------------------

# Poll the data plane until a Host answers 200, or give up. Used both as a
# readiness gate and, with a tight budget, as a convergence check.
wait_for_route() {
    local contender="$1" host="$2" budget="${3:-60}"
    local target; target="$(target_for "$contender")"
    local deadline=$((SECONDS + budget)) code
    while (( SECONDS < deadline )); do
        code="$(docker run --rm --network "$KIND_NET" curlimages/curl:latest \
                    -s -o /dev/null -w '%{http_code}' -m 3 \
                    -H "Host: $host" "http://${target}/" 2>/dev/null || true)"
        [[ "$code" == "200" ]] && return 0
        sleep 1
    done
    return 1
}

# ---------------------------------------------------------------------------
# The probe container
#
# probe.py needs to reach the NodePort, which only exists on the `kind` bridge,
# and the propagation benchmark additionally needs kubectl in the same process
# tree as the poller so that "apply -> served" is two readings of one clock.
# So both run inside a container, against a kubeconfig generated here.
#
# That generated kubeconfig is a safety feature, not a convenience: it is the
# --minify of the local context with the server rewritten to the API server's
# in-network address, so it names exactly one cluster and there is no reachable
# path from inside the container to the production clusters that share the
# developer's real kubeconfig.
# ---------------------------------------------------------------------------

PROBE_IMAGE="${PROBE_IMAGE:-ramjet-thesis-probe:latest}"
PROBE_WORK="${PROBE_WORK:-$THESIS_DIR/.work}"

probe_build() {
    docker build -q -f "$THESIS_DIR/Dockerfile.probe" -t "$PROBE_IMAGE" "$THESIS_DIR" >/dev/null
}

probe_kubeconfig() {
    mkdir -p "$PROBE_WORK"
    local out="$PROBE_WORK/kubeconfig"
    [[ -s "$out" ]] && { echo "$out"; return; }
    K config view --minify --raw --context "$CONTEXT" -o yaml \
        | python3 -c '
import re, sys
cfg = sys.stdin.read()
cfg = re.sub(r"server: https://[^\s]+", "server: https://'"$(node_ip)"':6443", cfg)
# The API server certificate is issued for 127.0.0.1 and the cluster service
# IP, not for the node container bridge address the probe has to dial. The CA
# is therefore useless at this address; skipping verification is safe because
# the only thing on the other end is this machine.
cfg = cfg.replace("certificate-authority-data:", "insecure-skip-tls-verify: true\n    x-unused-ca:")
sys.stdout.write(cfg)
' > "$out"
    chmod 600 "$out"
    echo "$out"
}

# Run probe.py in a container on the kind network with $PROBE_WORK at /w.
probe() {
    probe_kubeconfig >/dev/null
    docker run --rm --network "$KIND_NET" \
        --name "${PREFIX}-probe-$$-${RANDOM}" \
        -v "$PROBE_WORK:/w" \
        -e KUBECONFIG=/w/kubeconfig \
        "$PROBE_IMAGE" "$@"
}

# Same, but detached and named so a caller can wait on it.
probe_bg() {
    local name="$1"; shift
    probe_kubeconfig >/dev/null
    docker run -d --network "$KIND_NET" \
        --name "$name" \
        -v "$PROBE_WORK:/w" \
        -e KUBECONFIG=/w/kubeconfig \
        "$PROBE_IMAGE" "$@" >/dev/null
}

# ---------------------------------------------------------------------------
# Load generation
#
# oha in a container on the same bridge, so the load generator and the probes
# take the identical network path. `-w` makes oha await in-flight requests at
# the deadline instead of counting exactly `-c` abandoned requests as errors on
# every run, which is the harness artifact bench/run.sh documents.
# ---------------------------------------------------------------------------

oha_run() {
    local target="$1" host="$2" conc="$3" dur="$4" out="$5"
    # Pinned to half the VM's cores. The Kubernetes node container is not
    # pinned and cannot be without reconfiguring Docker Desktop itself, so
    # capping the load generator is the only lever available for keeping it
    # from starving the thing it is measuring. Identical for both contenders.
    docker run --rm --network "$KIND_NET" --name "${PREFIX}-oha-$$" \
        --cpuset-cpus=4,5,6,7 \
        "$OHA_IMAGE" --no-tui --output-format json \
        -c "$conc" -z "$dur" -w \
        --worker-threads 4 \
        --host "$host" \
        "http://${target}/" 2>/dev/null > "$out"
}

json_get() { python3 -c 'import json,sys;print(eval("d"+sys.argv[2], {"d": json.load(open(sys.argv[1]))}))' "$1" "$2"; }
