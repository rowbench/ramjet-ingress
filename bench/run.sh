#!/usr/bin/env bash
#
# ramjet-ingress vs nginx, as reverse proxies, on one machine in one session.
#
#     ./bench/run.sh
#
# Idempotent: it tears down anything it left behind before it starts, and again
# on the way out (including on Ctrl-C). Everything it creates is named
# `ramjet-bench-*` so it cannot collide with another container on this daemon.
#
# ---------------------------------------------------------------------------
# Topology (one docker bridge network, no host port NAT in the measured path)
# ---------------------------------------------------------------------------
#
#   oha (cores 4-7)  ->  proxy under test (cores 0,1)  ->  up1 (core 2)
#                                                      \-> up2 (core 3)
#
#   baseline run:    oha (cores 4-7)  ->  base (cores 0,1)
#
# CPU is allocated with --cpuset-cpus rather than --cpus, and that choice is
# load-bearing for fairness. A --cpus quota is invisible to sched_getaffinity,
# so nginx's `worker_processes auto` would have read the VM's 8 CPUs and
# started 8 workers, while Rust's available_parallelism() *does* read the
# cgroup quota and would have given tokio 2 threads. Pinning to an explicit
# cpuset makes both see exactly 2 CPUs, so both start exactly 2 workers on the
# same 2 cores. Verified at startup by verify_topology().
#
# The two upstreams are shared: the same pair serves both contenders, so
# whatever the upstream costs is paid equally and cancels out.
#
# The baseline is a third nginx pinned to the *proxy's* cores, serving the
# body directly. That makes "proxy overhead" a like-for-like subtraction: the
# same 2 cores, the same 128-byte response, with and without the extra hop.

set -euo pipefail

BENCH_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "${BENCH_DIR}/.." && pwd)"
RESULTS_DIR="${BENCH_DIR}/results"

PREFIX="ramjet-bench"
NET="${PREFIX}-net"
SUBNET="172.31.99.0/24"
IMAGE="${PREFIX}-ramjetd:latest"

NGINX_IMAGE="nginx:1-alpine"
OHA_IMAGE="ghcr.io/hatoo/oha:latest"
CURL_IMAGE="curlimages/curl:latest"

# Static addresses so both contenders can be pointed at literal ip:port.
# ramjet-ingress takes ip:port endpoints (the controller feeds it pod IPs in
# production), so naming the upstreams would have put a DNS lookup in nginx's
# path and not ramjet's.
IP_UP1="172.31.99.11"
IP_UP2="172.31.99.12"
IP_RAMJET="172.31.99.21"
IP_NGINX="172.31.99.22"
IP_BASE="172.31.99.23"

CPUS_PROXY="0,1"      # whichever contender is under test
CPUS_UP1="2"
CPUS_UP2="3"
CPUS_LOAD="4,5,6,7"   # the load generator must never be the bottleneck
LOAD_THREADS=4

HOST_HEADER="bench.test"
WARMUP="${WARMUP:-10s}"
DURATION="${DURATION:-30s}"
ROUNDS="${ROUNDS:-3}"
CONC_MAIN="${CONC_MAIN:-64}"
CONC_HIGH="${CONC_HIGH:-256}"

log()  { printf '\033[1;34m==>\033[0m %s\n' "$*"; }
warn() { printf '\033[1;33m[!]\033[0m %s\n' "$*" >&2; }
die()  { printf '\033[1;31m[x]\033[0m %s\n' "$*" >&2; exit 1; }

# ---------------------------------------------------------------------------
# Lifecycle
# ---------------------------------------------------------------------------

# Only ever removes containers this script owns. The docker daemon is shared,
# so a blanket prune would be somebody else's outage.
cleanup() {
    docker rm -f "${PREFIX}-up1" "${PREFIX}-up2" "${PREFIX}-ramjet" \
                 "${PREFIX}-nginx" "${PREFIX}-base" >/dev/null 2>&1 || true
    docker network rm "${NET}" >/dev/null 2>&1 || true
}
trap cleanup EXIT INT TERM

preflight() {
    command -v docker  >/dev/null || die "docker is not on PATH"
    command -v python3 >/dev/null || die "python3 is needed to render the report"
    docker info >/dev/null 2>&1 || die "the docker daemon is not responding"

    local cpus
    cpus="$(docker run --rm "${NGINX_IMAGE}" nproc 2>/dev/null || echo 0)"
    [ "${cpus}" -ge 8 ] || die "this layout pins 8 distinct cores; the docker VM reports ${cpus}"
}

build_images() {
    log "pulling ${NGINX_IMAGE}, ${OHA_IMAGE}, ${CURL_IMAGE}"
    docker pull -q "${NGINX_IMAGE}" >/dev/null
    docker pull -q "${OHA_IMAGE}"   >/dev/null
    docker pull -q "${CURL_IMAGE}"  >/dev/null

    log "building ${IMAGE} (cargo build --release -p ramjet-ingressd)"
    docker build -q -f "${BENCH_DIR}/Dockerfile.ramjet" -t "${IMAGE}" "${REPO_DIR}" >/dev/null
}

start_topology() {
    cleanup
    log "creating network ${NET} (${SUBNET})"
    docker network create --driver bridge --subnet "${SUBNET}" "${NET}" >/dev/null

    # Upstreams first: nginx resolves `upstream` members at config load, so the
    # proxy must not start before the things it points at exist.
    log "starting shared upstreams"
    docker run -d --name "${PREFIX}-up1" --network "${NET}" --ip "${IP_UP1}" \
        --cpuset-cpus="${CPUS_UP1}" \
        -v "${BENCH_DIR}/upstream.conf:/etc/nginx/nginx.conf:ro" \
        "${NGINX_IMAGE}" >/dev/null
    docker run -d --name "${PREFIX}-up2" --network "${NET}" --ip "${IP_UP2}" \
        --cpuset-cpus="${CPUS_UP2}" \
        -v "${BENCH_DIR}/upstream.conf:/etc/nginx/nginx.conf:ro" \
        "${NGINX_IMAGE}" >/dev/null

    log "starting contender A: ramjet-ingressd"
    docker run -d --name "${PREFIX}-ramjet" --network "${NET}" --ip "${IP_RAMJET}" \
        --cpuset-cpus="${CPUS_PROXY}" \
        -v "${BENCH_DIR}/ramjet-routes.yaml:/etc/ramjet/routes.yaml:ro" \
        "${IMAGE}" --static-routes /etc/ramjet/routes.yaml --no-https >/dev/null

    log "starting contender B: nginx reverse proxy"
    docker run -d --name "${PREFIX}-nginx" --network "${NET}" --ip "${IP_NGINX}" \
        --cpuset-cpus="${CPUS_PROXY}" \
        -v "${BENCH_DIR}/nginx.conf:/etc/nginx/nginx.conf:ro" \
        "${NGINX_IMAGE}" >/dev/null

    log "starting baseline: direct-serve on the proxy's own cores"
    docker run -d --name "${PREFIX}-base" --network "${NET}" --ip "${IP_BASE}" \
        --cpuset-cpus="${CPUS_PROXY}" \
        -v "${BENCH_DIR}/upstream.conf:/etc/nginx/nginx.conf:ro" \
        "${NGINX_IMAGE}" >/dev/null

    sleep 4
}

# ---------------------------------------------------------------------------
# Correctness gates
#
# A fast proxy that returns the wrong thing is not a result. Nothing is
# measured until every one of these passes.
# ---------------------------------------------------------------------------

curl_in_net() {
    docker run --rm --network "${NET}" "${CURL_IMAGE}" "$@" 2>/dev/null
}

verify_correctness() {
    log "verifying both contenders serve identical bytes"

    local ramjet_body nginx_body base_body
    ramjet_body="$(curl_in_net -sS -H "Host: ${HOST_HEADER}" "http://${IP_RAMJET}:8080/")"
    nginx_body="$(curl_in_net  -sS -H "Host: ${HOST_HEADER}" "http://${IP_NGINX}:8080/")"
    base_body="$(curl_in_net   -sS -H "Host: ${HOST_HEADER}" "http://${IP_BASE}:8080/")"

    [ "${#ramjet_body}" -eq 128 ] || die "ramjet returned ${#ramjet_body} bytes, expected 128"
    [ "${ramjet_body}" = "${nginx_body}" ] || die "contenders returned different bodies"
    [ "${ramjet_body}" = "${base_body}" ]  || die "baseline returned a different body"

    local code
    for target in "${IP_RAMJET}" "${IP_NGINX}"; do
        code="$(curl_in_net -sS -o /dev/null -w '%{http_code}' \
                    -H "Host: ${HOST_HEADER}" "http://${target}:8080/")"
        [ "${code}" = "200" ] || die "${target} answered ${code}, expected 200"
    done

    # The Host header must actually be routed on, or the benchmark is measuring
    # a passthrough rather than an ingress doing its job.
    code="$(curl_in_net -sS -o /dev/null -w '%{http_code}' \
                -H "Host: wrong.invalid" "http://${IP_RAMJET}:8080/")"
    [ "${code}" = "404" ] || die "ramjet answered ${code} for an unrouted Host, expected 404"

    log "  both serve 128 identical bytes; host routing is live"
}

verify_topology() {
    log "verifying the two contenders got identical CPU"

    local seen_ramjet seen_nginx workers threads
    seen_ramjet="$(docker exec "${PREFIX}-ramjet" nproc)"
    seen_nginx="$(docker exec "${PREFIX}-nginx" nproc)"
    [ "${seen_ramjet}" = "${seen_nginx}" ] \
        || die "contenders see different CPU counts: ${seen_ramjet} vs ${seen_nginx}"

    workers="$(docker exec "${PREFIX}-nginx" sh -c 'ps -o pid,args | grep -c "[w]orker process"' || true)"
    threads="$(docker exec "${PREFIX}-ramjet" sh -c 'awk "/^Threads:/{print \$2}" /proc/1/status' || true)"

    # nginx's worker count comes from `worker_processes auto` reading the
    # cpuset; ramjet's tokio thread count comes from available_parallelism()
    # reading the same cpuset. They are expected to agree, and if they ever
    # stop agreeing the comparison has quietly stopped being fair.
    [ "${workers}" = "${seen_nginx}" ] \
        || warn "nginx started ${workers} workers on ${seen_nginx} CPUs"

    log "  each sees ${seen_ramjet} CPUs; nginx ${workers} workers, ramjet ${threads} threads (main + tokio)"
}

# ---------------------------------------------------------------------------
# Measurement
# ---------------------------------------------------------------------------

# oha notes:
#   -w  wait for in-flight requests when the deadline hits. Without it oha
#       counts exactly `-c` abandoned requests as errors on every run, which
#       is a harness artifact and would bury a real error signal.
#   --host  sets the Host header while the URL keeps the literal IP, so no DNS
#       lookup enters the measured path for either contender.
#   keep-alive is oha's default and is left on.
oha() {
    local target="$1" conc="$2" dur="$3"
    shift 3
    docker run --rm --network "${NET}" --cpuset-cpus="${CPUS_LOAD}" "${OHA_IMAGE}" \
        --no-tui --output-format json \
        -c "${conc}" -z "${dur}" -w \
        --worker-threads "${LOAD_THREADS}" \
        --host "${HOST_HEADER}" \
        "http://${target}:8080/" "$@" 2>/dev/null
}

target_ip() {
    case "$1" in
        ramjet)   echo "${IP_RAMJET}" ;;
        nginx)    echo "${IP_NGINX}"  ;;
        baseline) echo "${IP_BASE}"   ;;
        *) die "unknown contender $1" ;;
    esac
}

# A cold contender measures its own warmup: a freshly started ramjet-ingressd
# reported 42k rps on its first run against 58k once its upstream pool had
# filled. Every measured run is preceded by a discarded one.
measure() {
    local who="$1" conc="$2" run="$3" ip out rps
    ip="$(target_ip "${who}")"

    oha "${ip}" "${conc}" "${WARMUP}" >/dev/null

    out="${RESULTS_DIR}/${who}-c${conc}-r${run}.json"
    oha "${ip}" "${conc}" "${DURATION}" > "${out}"

    rps="$(python3 -c "import json,sys;print(f\"{json.load(open(sys.argv[1]))['summary']['requestsPerSec']:,.0f}\")" "${out}")"
    printf '    %-10s c%-4s run %s  %12s rps\n' "${who}" "${conc}" "${run}" "${rps}"
}

# Interleaved on purpose: A,B,base then A,B,base again. Running all of A then
# all of B would hand any thermal or background drift entirely to whichever
# contender happened to go second.
run_all() {
    mkdir -p "${RESULTS_DIR}"
    rm -f "${RESULTS_DIR}"/*.json

    log "c${CONC_MAIN}: ${ROUNDS} interleaved rounds of ${DURATION} (after ${WARMUP} warmup each)"
    for round in $(seq 1 "${ROUNDS}"); do
        echo "  round ${round}/${ROUNDS}"
        for who in ramjet nginx baseline; do
            measure "${who}" "${CONC_MAIN}" "${round}"
        done
    done

    log "c${CONC_HIGH}: one round"
    for who in ramjet nginx baseline; do
        measure "${who}" "${CONC_HIGH}" 1
    done
}

# ---------------------------------------------------------------------------
# Diagnostics
#
# Run after the measured rounds, never during: reading docker stats mid-run
# would perturb the thing being measured. These answer the two questions a
# reader should ask of any proxy benchmark -- was the proxy actually the
# bottleneck, and what is it spending itself on.
# ---------------------------------------------------------------------------

passive_opens() {
    docker exec "${PREFIX}-$1" sh -c \
        "awk '/^Tcp:/{n++; if(n==2) print \$7}' /proc/net/snmp"
}

diagnostics() {
    log "diagnostics (separate pass; not part of the reported runs)"
    : > "${RESULTS_DIR}/diagnostics.txt"

    for who in ramjet nginx; do
        local ip a0 b0 a1 b1 conns cpu mem json requests
        ip="$(target_ip "${who}")"

        a0="$(passive_opens up1)"; b0="$(passive_opens up2)"
        json="$(oha "${ip}" "${CONC_MAIN}" 12s)"
        a1="$(passive_opens up1)"; b1="$(passive_opens up2)"
        conns=$(( (a1 - a0) + (b1 - b0) ))
        requests="$(printf '%s' "${json}" | python3 -c \
            "import json,sys;print(sum(json.load(sys.stdin)['statusCodeDistribution'].values()))")"

        # A second short run purely to sample CPU and memory while loaded.
        oha "${ip}" "${CONC_MAIN}" 12s >/dev/null &
        local load_pid=$!
        sleep 6
        read -r cpu mem <<<"$(docker stats --no-stream --format '{{.CPUPerc}} {{.MemUsage}}' "${PREFIX}-${who}")"
        wait "${load_pid}"

        printf '%s: cpu=%s mem=%s upstream_conns_opened=%s requests=%s reqs_per_conn=%s\n' \
            "${who}" "${cpu}" "${mem}" "${conns}" "${requests}" \
            "$(python3 -c "print(f'{${requests}/max(${conns},1):.0f}')")" \
            | tee -a "${RESULTS_DIR}/diagnostics.txt"
    done

    # Upstream headroom: if these are near 100% the benchmark measured the
    # upstream, not either proxy.
    oha "${IP_RAMJET}" "${CONC_MAIN}" 12s >/dev/null &
    local load_pid=$!
    sleep 6
    printf 'upstream cpu during load: %s\n' \
        "$(docker stats --no-stream --format '{{.Name}}={{.CPUPerc}}' \
            "${PREFIX}-up1" "${PREFIX}-up2" | tr '\n' ' ')" \
        | tee -a "${RESULTS_DIR}/diagnostics.txt"
    wait "${load_pid}"
}

versions() {
    log "recording versions"
    {
        echo "date:      $(date -u '+%Y-%m-%dT%H:%M:%SZ')"
        echo "host:      $(uname -srm), $(sysctl -n hw.ncpu 2>/dev/null || nproc) host CPUs"
        echo "docker:    $(docker version --format '{{.Server.Version}}')"
        echo "docker VM: $(docker run --rm "${NGINX_IMAGE}" nproc) CPUs, $(docker run --rm "${NGINX_IMAGE}" uname -r)"
        # --entrypoint, or the image's docker-entrypoint.sh prints ten lines of
        # template-expansion chatter around the one line we want.
        echo "nginx:     ${NGINX_IMAGE} -> $(docker run --rm --entrypoint nginx "${NGINX_IMAGE}" -v 2>&1)"
        echo "oha:       ${OHA_IMAGE} -> $(docker run --rm "${OHA_IMAGE}" --version 2>&1)"
        echo "rustc:     $(docker run --rm --entrypoint rustc rust:1-slim-bookworm --version 2>&1)"
        echo "ramjet:    $(docker run --rm "${IMAGE}" --version 2>&1)"
    } | tee "${RESULTS_DIR}/versions.txt"
}

main() {
    preflight
    build_images
    start_topology
    verify_correctness
    verify_topology
    versions
    run_all
    diagnostics

    log "rendering the table"
    python3 "${BENCH_DIR}/report.py" | tee "${RESULTS_DIR}/table.md"

    log "raw JSON in ${RESULTS_DIR}; write the reading into RESULTS.md"
}

main "$@"
