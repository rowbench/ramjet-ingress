#!/usr/bin/env bash
#
# WebSocket tunnels through the three contenders: ramjet-ingress on the hyper
# engine, ramjet-ingress on the uring engine, and nginx.
#
# WHAT IS BEING MEASURED
#
# Echo round trips a second, and the latency of one. That is the only thing a
# *passthrough* tunnel can honestly be measured on: after a 101 neither engine
# parses a frame — the bytes are opaque to both — so varying frame types or
# fragmentation would measure the load generator against itself with a proxy in
# the middle.
#
# This is a different shape of work from every other benchmark here. There is no
# request, no route lookup, no header rewriting and no upstream pool: one
# connection in, one connection out, and bytes moved between them for as long as
# both stay open. What it exercises is the relay loop and the buffer discipline
# around it, which is the part of an engine a request benchmark barely touches.
#
# WHY oha IS NOT USED
#
# It does not speak WebSocket. `wsload/` is a small client written for this,
# with no dependencies: one echo in flight per connection, so the latency it
# reports is a round trip a real message would have experienced rather than a
# number produced by pipelining.
#
# FAIRNESS
#
# All three proxy the same upstream, `enhance-socket`'s `ws_echo`, on the same
# two upstream cores. nginx gets the four directives a WebSocket proxy needs;
# without them it strips `Upgrade` and there is no tunnel to measure. No
# contender gets an upstream keepalive pool, because an upgraded connection is
# never returned to one — after a 101 it is not HTTP any more.
#
# Usage:
#     ./bench/engine/ws-run.sh
#     SMOKE=1 ./bench/engine/ws-run.sh      # validate the script, ~2 minutes

set -euo pipefail

BENCH_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "${BENCH_DIR}/../.." && pwd)"

OVERRIDDEN=""
for knob in DURATION ROUNDS CONNS PAYLOAD COOLDOWN; do
    [ -n "${!knob:-}" ] && OVERRIDDEN="${OVERRIDDEN}${knob} "
done
[ "${SMOKE:-0}" = "1" ] && OVERRIDDEN="${OVERRIDDEN}SMOKE "

if [ -n "${OVERRIDDEN}" ]; then
    RESULTS_DIR="${BENCH_DIR}/results-ws/scratch"
else
    RESULTS_DIR="${BENCH_DIR}/results-ws"
fi

PREFIX="ramjet-ws"
NET="${PREFIX}-net"
SUBNET="172.31.98.0/24"
IMAGE="ramjet-engine-ramjetd:latest"
WS_IMAGE="${PREFIX}-echo:latest"
SECCOMP="${BENCH_DIR}/seccomp-uring.json"

NGINX_IMAGE="nginx:1-alpine"

IP_UP1="172.31.98.11"
IP_UP2="172.31.98.12"
IP_HYPER="172.31.98.21"
IP_URING="172.31.98.22"
IP_NGINX="172.31.98.23"

CPUS_PROXY="0,1"
CPUS_UP1="2"
CPUS_UP2="3"
CPUS_LOAD="4,5,6,7"

# The proxies route by Host and are reached by IP, so the two are passed
# separately. Without this the load generator sends the container's address as
# the Host, matches no route, and gets a 404 the handshake reports as a refused
# upgrade — which is what the correctness gate below caught the first time.
HOST_HEADER="bench.test"

if [ "${SMOKE:-0}" = "1" ]; then
    DURATION="${DURATION:-5}"
    ROUNDS="${ROUNDS:-1}"
    COOLDOWN="${COOLDOWN:-3}"
fi
DURATION="${DURATION:-20}"
ROUNDS="${ROUNDS:-3}"
CONNS="${CONNS:-64}"
PAYLOAD="${PAYLOAD:-128}"
COOLDOWN="${COOLDOWN:-10}"

log()  { printf '\033[1;34m==>\033[0m %s\n' "$*"; }
warn() { printf '\033[1;33m[!]\033[0m %s\n' "$*" >&2; }
die()  { printf '\033[1;31m[x]\033[0m %s\n' "$*" >&2; exit 1; }

cleanup() {
    docker rm -f "${PREFIX}-up1" "${PREFIX}-up2" "${PREFIX}-hyper" \
        "${PREFIX}-uring" "${PREFIX}-nginx" >/dev/null 2>&1 || true
    docker network rm "${NET}" >/dev/null 2>&1 || true
}
trap cleanup EXIT

build_images() {
    log "building the ramjet image"
    docker build -q -f "${REPO_DIR}/bench/Dockerfile.ramjet" \
        -t "${IMAGE}" "$(dirname "${REPO_DIR}")" >/dev/null \
        || die "the ramjet image did not build"
    log "building the WebSocket upstream and load generator"
    docker build -q -f "${BENCH_DIR}/Dockerfile.wsecho" \
        -t "${WS_IMAGE}" "$(dirname "${REPO_DIR}")" >/dev/null \
        || die "the ws_echo image did not build"
}

start_stack() {
    log "starting the stack"
    cleanup
    docker network create --driver bridge --subnet "${SUBNET}" "${NET}" >/dev/null

    # The upstreams get the seccomp profile too, and they need it: `ws_echo`
    # runs on the same reactor the uring engine does, and Docker's default
    # profile blocks `io_uring_setup`. Without this the upstream containers die
    # at startup with EPERM and every contender measures a connection refused.
    #
    # Which is, incidentally, the failure mode `--engine uring`'s fallback
    # exists for, observed here on the one process in this topology that has no
    # fallback to take.
    docker run -d --name "${PREFIX}-up1" --network "${NET}" --ip "${IP_UP1}" \
        --cpuset-cpus="${CPUS_UP1}" --security-opt "seccomp=${SECCOMP}" \
        "${WS_IMAGE}" 9001 >/dev/null
    docker run -d --name "${PREFIX}-up2" --network "${NET}" --ip "${IP_UP2}" \
        --cpuset-cpus="${CPUS_UP2}" --security-opt "seccomp=${SECCOMP}" \
        "${WS_IMAGE}" 9001 >/dev/null

    # `uring-strict`: a silent fallback would make this compare hyper with
    # hyper and report it as an engine result.
    docker run -d --name "${PREFIX}-hyper" --network "${NET}" --ip "${IP_HYPER}" \
        --cpuset-cpus="${CPUS_PROXY}" \
        --security-opt "seccomp=${SECCOMP}" \
        -v "${BENCH_DIR}/ramjet-routes-ws.yaml:/etc/ramjet/routes.yaml:ro" \
        "${IMAGE}" --engine hyper --static-routes /etc/ramjet/routes.yaml \
        --no-https >/dev/null
    docker run -d --name "${PREFIX}-uring" --network "${NET}" --ip "${IP_URING}" \
        --cpuset-cpus="${CPUS_PROXY}" \
        --security-opt "seccomp=${SECCOMP}" \
        -v "${BENCH_DIR}/ramjet-routes-ws.yaml:/etc/ramjet/routes.yaml:ro" \
        "${IMAGE}" --engine uring-strict --static-routes /etc/ramjet/routes.yaml \
        --no-https >/dev/null

    docker run -d --name "${PREFIX}-nginx" --network "${NET}" --ip "${IP_NGINX}" \
        --cpuset-cpus="${CPUS_PROXY}" \
        -v "${BENCH_DIR}/nginx-ws.conf:/etc/nginx/nginx.conf:ro" \
        "${NGINX_IMAGE}" >/dev/null

    sleep 4
}

wsload() {
    local target="$1" conns="$2" secs="$3"
    docker run --rm --network "${NET}" --cpuset-cpus="${CPUS_LOAD}" \
        --security-opt "seccomp=${SECCOMP}" \
        --entrypoint /usr/local/bin/wsload "${WS_IMAGE}" \
        "${target}:8080" "${conns}" "${secs}" "${PAYLOAD}" "${HOST_HEADER}" 2>/dev/null
}

target_ip() {
    case "$1" in
        ramjet-hyper) echo "${IP_HYPER}" ;;
        ramjet-uring) echo "${IP_URING}" ;;
        nginx)        echo "${IP_NGINX}" ;;
        *) die "unknown contender $1" ;;
    esac
}

# Nothing is measured until every contender is proven to carry a tunnel.
#
# A proxy that answers 200 to the upgrade is fast and is measuring nothing: the
# load generator would fail its handshake and report zero, which at least fails
# loudly — but a proxy that upgraded and then dropped every frame would report a
# plausible number, and that is what this check exists for.
verify_tunnels() {
    log "checking all three carry a tunnel"
    local who out echoes
    for who in ramjet-hyper ramjet-uring nginx; do
        out="$(wsload "$(target_ip "${who}")" 2 2 2>&1 | tail -1 || true)"
        [ -n "${out}" ] || die "${who} produced no output at all"
        echoes="$(python3 -c "import json,sys;print(json.loads(sys.argv[1])['echoes'])" "${out}")"
        [ "${echoes}" -gt 0 ] \
            || die "${who} carried no echoes; the upgrade did not cross the hop: ${out}"
        printf '    %-14s %s echoes in 2s\n' "${who}" "${echoes}"
    done
}

verify_uring() {
    log "verifying the uring engine actually started on io_uring"
    docker ps --format '{{.Names}}' | grep -q "^${PREFIX}-uring$" \
        || { docker logs "${PREFIX}-uring" 2>&1 | tail -20 >&2
             die "the uring container exited; io_uring is unavailable in this VM"; }
    docker logs "${PREFIX}-uring" 2>&1 | grep -q "falling back" \
        && die "the uring container fell back to hyper; this run would compare hyper with hyper"
    log "  io_uring is live (uring-strict would have died otherwise)"
}

versions() {
    {
        echo "date:      $(date -u +%FT%TZ)"
        echo "host:      $(uname -srm)"
        echo "docker:    $(docker version --format '{{.Server.Version}}')"
        echo "kernel:    $(docker run --rm "${NGINX_IMAGE}" uname -r)"
        echo "ramjet:    $(cd "${REPO_DIR}" && git rev-parse --short HEAD)"
        echo "nginx:     $(docker run --rm "${NGINX_IMAGE}" nginx -v 2>&1 | tail -1)"
        echo "upstream:  enhance-socket ws_echo"
        echo "load:      wsload, ${CONNS} connections, ${PAYLOAD}-byte payload"
        echo "duration:  ${DURATION}s x ${ROUNDS} rounds"
    } | tee "${RESULTS_DIR}/versions.txt"
}

measure() {
    local who="$1" run="$2" out
    sleep "${COOLDOWN}"
    out="${RESULTS_DIR}/${who}-r${run}.json"
    wsload "$(target_ip "${who}")" "${CONNS}" "${DURATION}" > "${out}"
    python3 - "${out}" "${who}" "${run}" <<'PY'
import json, sys
d = json.load(open(sys.argv[1]))
print(f"    {sys.argv[2]:<14} run {sys.argv[3]}  "
      f"{d['echoes_per_sec']:>12,.0f} echo/s  "
      f"p50 {d['p50_micros']:>5}us  p99 {d['p99_micros']:>6}us  "
      f"errors {d['errors']}")
PY
}

CONTENDERS=(ramjet-hyper ramjet-uring nginx)

main() {
    command -v docker >/dev/null || die "docker is not on PATH"
    command -v python3 >/dev/null || die "python3 is not on PATH"

    if [ -n "${OVERRIDDEN}" ]; then
        warn "non-protocol run (${OVERRIDDEN}); writing to results-ws/scratch/"
        warn "these numbers are NOT comparable to the committed ones"
    fi
    mkdir -p "${RESULTS_DIR}"

    build_images
    start_stack
    verify_uring
    verify_tunnels
    versions

    local round position who
    log "echo round trips, ${CONNS} tunnels, ${PAYLOAD}-byte payload"
    for round in $(seq 1 "${ROUNDS}"); do
        position=$(( (round - 1) % ${#CONTENDERS[@]} ))
        for i in "${!CONTENDERS[@]}"; do
            who="${CONTENDERS[$(( (position + i) % ${#CONTENDERS[@]} ))]}"
            measure "${who}" "${round}"
        done
    done

    log "done; results in ${RESULTS_DIR}"
}

main "$@"
