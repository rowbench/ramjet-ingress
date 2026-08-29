#!/usr/bin/env bash
#
# Shared plumbing for the EC2 head-to-head. Adapted from bench/thesis/lib.sh.
#
# Differences from the docker-desktop harness, and why:
#
#  - There is no docker on this box and no `kind` bridge. Everything (load
#    generator, probes, kubectl) runs natively on the node itself, and the
#    measured entry point is a NodePort on 127.0.0.1.
#  - Three controller releases stand at once, not two, because the two ramjet
#    engines differ by a pod securityContext that only a redeploy can change.
#    Running them as separate releases makes contender rotation a change of
#    port number rather than a helm upgrade and a pod restart, which is what
#    keeps interleaving honest on a burstable box.
#  - Resource sampling reads cgroup v2 counters directly (k0s uses the
#    cgroupfs driver), because there is no crictl on this box and
#    `kubectl top` reports an instantaneous rate rather than a cumulative
#    total.

set -euo pipefail

PREFIX="rjec2"
NS_HYPER="${PREFIX}-hyper"
NS_URING="${PREFIX}-uring"
NS_NGINX="${PREFIX}-nginx"
NS_APP="${PREFIX}-app"

CLASS_HYPER="${PREFIX}-hyper"
CLASS_URING="${PREFIX}-uring"
CLASS_NGINX="${PREFIX}-nginx"

REL_HYPER="${PREFIX}-hyper"
REL_URING="${PREFIX}-uring"
REL_NGINX="${PREFIX}-nginx"

RAMJET_IMAGE_REPO="${RAMJET_IMAGE_REPO:-sofelia/ramjet-ingress}"
RAMJET_IMAGE_TAG="${RAMJET_IMAGE_TAG:-latest}"
NGINX_CHART_VERSION="${NGINX_CHART_VERSION:-4.15.1}"

BENCH_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CHART_DIR="${BENCH_DIR}/ramjet-ingress"
RESULTS="${BENCH_DIR}/results"
WORK="${BENCH_DIR}/work"

STABLE_HOST="stable.ec2.test"
CHURN_HOST="churn.ec2.test"

# NodePorts. Four measured entry points: three controllers and the backend
# Service itself, which is the no-proxy baseline.
NP_HYPER=31080
NP_URING=31082
NP_NGINX=31081
NP_BASELINE=31090

K() { kubectl "$@"; }
H() { helm "$@"; }

log()  { printf '\033[1;34m==>\033[0m %s\n' "$*"; }
sub()  { printf '    %s\n' "$*"; }
warn() { printf '\033[1;33m[!]\033[0m %s\n' "$*" >&2; }
die()  { printf '\033[1;31m[x]\033[0m %s\n' "$*" >&2; exit 1; }

# ---------------------------------------------------------------------------
# Addressing
# ---------------------------------------------------------------------------

port_for() {
    case "$1" in
        hyper)    echo "$NP_HYPER" ;;
        uring)    echo "$NP_URING" ;;
        nginx)    echo "$NP_NGINX" ;;
        baseline) echo "$NP_BASELINE" ;;
        *) die "unknown contender $1" ;;
    esac
}

target_for() { printf '127.0.0.1:%s' "$(port_for "$1")"; }

class_for() {
    case "$1" in
        hyper) echo "$CLASS_HYPER" ;;
        uring) echo "$CLASS_URING" ;;
        nginx) echo "$CLASS_NGINX" ;;
        *) die "unknown contender $1" ;;
    esac
}

ns_for() {
    case "$1" in
        hyper) echo "$NS_HYPER" ;;
        uring) echo "$NS_URING" ;;
        nginx) echo "$NS_NGINX" ;;
        *) die "unknown contender $1" ;;
    esac
}

# The baseline sends Host: stable.ec2.test straight at the backend Service's
# NodePort, so the header is identical on every path and the only difference is
# whether a proxy is in it. http-echo ignores the Host header entirely.
host_for() { echo "$STABLE_HOST"; }

# The running controller pod, not merely the first listed: ingress-nginx's
# chart leaves Completed admission-certificate Job pods in its namespace and one
# of those sorting first would be sampled instead of the controller.
pod_for() {
    local ns; ns="$(ns_for "$1")"
    K -n "$ns" get pods --field-selector status.phase=Running \
        -l app.kubernetes.io/instance -o jsonpath='{.items[0].metadata.name}' 2>/dev/null \
      || K -n "$ns" get pods --field-selector status.phase=Running \
        -o jsonpath='{.items[0].metadata.name}'
}

# ---------------------------------------------------------------------------
# Resource sampling, straight off cgroup v2
#
# k0s runs containerd with the cgroupfs driver, so a pod's cgroup is
# /sys/fs/cgroup/kubepods/<qos>/pod<uid>/ with the UID's dashes intact. cpu.stat
# carries a cumulative usage_usec, which is what a per-request CPU figure needs;
# `kubectl top` reports a rate sampled on metrics-server's own schedule and
# cannot be differenced across a benchmark window.
#
# Memory is reported the way the kubelet reports working set: memory.current
# minus inactive_file, which is also the number a memory limit is enforced
# against.
# ---------------------------------------------------------------------------

pod_cgroup() {
    local ns="$1" pod="$2" uid
    uid="$(K -n "$ns" get pod "$pod" -o jsonpath='{.metadata.uid}' 2>/dev/null)" || return 1
    [[ -n "$uid" ]] || return 1
    sudo find /sys/fs/cgroup/kubepods -maxdepth 2 -type d -name "pod${uid}" 2>/dev/null | head -1
}

# echoes: cpu_usec=<n> mem_bytes=<n>
pod_stats() {
    local ns="$1" pod="$2" cg cpu mem inact
    cg="$(pod_cgroup "$ns" "$pod" || true)"
    if [[ -z "$cg" ]]; then echo "cpu_usec=0 mem_bytes=0"; return; fi
    cpu="$(sudo awk '/^usage_usec/{print $2}' "$cg/cpu.stat" 2>/dev/null || echo 0)"
    mem="$(sudo cat "$cg/memory.current" 2>/dev/null || echo 0)"
    inact="$(sudo awk '/^inactive_file /{print $2}' "$cg/memory.stat" 2>/dev/null || echo 0)"
    echo "cpu_usec=${cpu:-0} mem_bytes=$(( ${mem:-0} - ${inact:-0} ))"
}

# ---------------------------------------------------------------------------
# Controller-side evidence: did the configuration actually change?
#
# Asymmetric on purpose, exactly as bench/thesis/b1-churn.sh explains: ramjet
# publishes ramjet_route_table_generation on an admin port its chart already
# exposes, so reading it is free. ingress-nginx exposes an equivalent counter
# only when controller.metrics.enabled is on, and that same flag switches on
# per-request Lua monitoring — measuring the reload would have slowed the thing
# being measured. Its log states every reload explicitly at no cost.
# ---------------------------------------------------------------------------

config_counter() {
    case "$1" in
        hyper|uring)
            local ns; ns="$(ns_for "$1")"
            K get --raw "/api/v1/namespaces/${ns}/services/${PREFIX}-$1-admin:10254/proxy/metrics" 2>/dev/null \
                | awk '/^ramjet_route_table_generation /{print $2; found=1} END{if(!found) print 0}' ;;
        nginx)
            K -n "$NS_NGINX" logs "deploy/${PREFIX}-nginx-controller" 2>/dev/null \
                | grep -c "Backend successfully reloaded" || true ;;
    esac
}

# ---------------------------------------------------------------------------
# Engine verification.
#
# `engine: uring` FALLS BACK to hyper wherever the reactor will not start, and
# says so in the startup line rather than failing. A uring run that silently
# fell back is a second hyper run wearing a label, so every measured uring
# window is gated on this.
# ---------------------------------------------------------------------------

# The daemon logs with ANSI styling on, so `engine` and `=` are separated by
# escape sequences in the raw stream and a naive grep for engine="..." finds
# nothing. Stripping the escapes first is the difference between reading the
# engine and silently concluding there isn't one.
engine_line() {
    local ns; ns="$(ns_for "$1")"
    K -n "$ns" logs "deploy/${PREFIX}-$1" 2>/dev/null | head -4 \
        | sed 's/\x1b\[[0-9;]*m//g' \
        | grep -oE 'engine="[a-z-]+"' | head -1 || true
}

assert_engine() {
    local contender="$1" want="$2" got
    got="$(engine_line "$contender")"
    [[ "$got" == "engine=\"$want\"" ]] \
        || die "$contender reports $got, expected engine=\"$want\" — a fallback run is not a measurement"
}

# ---------------------------------------------------------------------------
# Waiting and load
# ---------------------------------------------------------------------------

wait_for_route() {
    local contender="$1" host="$2" budget="${3:-90}"
    local target; target="$(target_for "$contender")"
    local deadline=$((SECONDS + budget)) code
    while (( SECONDS < deadline )); do
        code="$(curl -s -o /dev/null -w '%{http_code}' -m 3 -H "Host: $host" "http://${target}/" 2>/dev/null || true)"
        [[ "$code" == "200" ]] && return 0
        sleep 1
    done
    return 1
}

# Everything runs on this one box: the load generator, the proxy under test and
# the upstreams. That is the Phase 16 shape and it is kept deliberately, so
# these numbers sit on the same axis as that phase's. It is also the reason the
# compression caveat carries over verbatim — see RESULTS-EC2.md.
#
# vmstat runs alongside every measured window, because the one thing a shared
# burstable instance can do that a dedicated box cannot is quietly stop giving
# you the CPU. steal% is sampled rather than assumed.
oha_run() {
    local target="$1" host="$2" conc="$3" dur="$4" out="$5" vmout="${6:-/dev/null}"
    local secs="${dur%s}"
    ( vmstat 1 $((secs + 3)) > "$vmout" 2>&1 ) &
    local vm=$!
    oha --no-tui --output-format json -c "$conc" -z "$dur" -w \
        --host "$host" "http://${target}/" >"$out" 2>/dev/null || true
    kill "$vm" 2>/dev/null || true
    wait "$vm" 2>/dev/null || true
}

# vmstat's trailing columns are us sy id wa st gu on this kernel.
vmstat_summary() {
    awk 'NR>2 && NF>=16 {n++; u+=$(NF-5); s+=$(NF-4); i+=$(NF-3); w+=$(NF-2); st+=$(NF-1)}
         END {if(n) printf "us=%.0f sy=%.0f id=%.0f wa=%.0f steal=%.2f", u/n, s/n, i/n, w/n, st/n;
              else printf "us=? sy=? id=? wa=? steal=?"}' "$1" 2>/dev/null
}

jqr() { jq -r "$2" "$1" 2>/dev/null; }
