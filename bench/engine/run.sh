#!/usr/bin/env bash
#
# Three contenders on the same two cores: ramjet-ingress on the hyper engine,
# ramjet-ingress on the uring engine, and nginx. Plus the no-proxy baseline, so
# the cost of the hop itself can be subtracted.
#
# This is bench/run.sh's topology, pinning and honesty rules with a third
# contender added. Everything that made those numbers comparable is kept:
# identical upstreams, `--cpuset-cpus` rather than a quota, a discarded warmup
# before every measured run, interleaved rounds so drift is shared rather than
# handed to whoever went second, and a correctness gate before anything is
# measured.
#
# WHAT THIS IS FOR
#
# bench/PROFILE.md ended by naming a ceiling and the one thing that could get
# under it:
#
#   59.4% of a request is the four unavoidable syscalls, and another 9.1% is
#   finding out a socket is ready. That is the floor for this design [...]
#   Getting under it means fewer syscalls per request, which on Linux means
#   io_uring.
#
# The uring engine submits those four operations into a ring and enters the
# kernel once for a batch of them. Whether that is worth anything against a
# tuned readiness-based proxy is the question, and this script is the only
# place it can be answered: macOS has no io_uring, so the native harness can
# only report the cost of the new state machine, never the benefit.
#
# SECCOMP
#
# Docker's default seccomp profile decides whether io_uring_setup is permitted,
# and the answer has changed between Docker versions — moby allowed the three
# io_uring syscalls by default up to v24 and removed them afterwards. Rather
# than depend on which side of that line the host happens to be, both ramjet
# containers run with seccomp-uring.json: moby v24.0.7's default profile with
# the three io_uring syscalls hoisted into their own explicit allow entry.
#
# Both ramjet containers get it, not just the uring one. Applying a different
# security profile to only one contender would be a topology asymmetry, and
# this benchmark's whole claim is that the contenders differ in one thing.
#
# There is no silent fallback to hide behind: on Linux the engine's reactor is
# io_uring and nothing else, so a blocked `io_uring_setup` fails
# `UringDriver::new()` and the process dies at startup. If the uring container
# serves a single request, io_uring is working.
#
# Usage:
#     ./bench/engine/run.sh
#     SMOKE=1 ./bench/engine/run.sh        # validate the script, ~4 minutes
#
# SMOKE mode checks that every stage works. Its throughput numbers are NOT
# comparable to a real run and the script says so before printing any: each
# core warms its own pool, timer set and buffers, so a short warmup measures
# a contender that is still filling them. The tell is throughput going *up*
# with concurrency.

set -euo pipefail

BENCH_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "${BENCH_DIR}/../.." && pwd)"

# Only an unmodified, committed-protocol run may write where the committed
# measurement lives. Anything with a knob turned goes to results/scratch/.
#
# The failure this prevents is not losing files, which git makes recoverable.
# It is a *plausible* wrong number: a short run leaves partial output where the
# real one was, `report.py` renders it, and the result reads as a modest
# regression rather than as obvious corruption. An obviously-broken figure gets
# caught; a believable one gets quoted and sends somebody hunting a code change
# that never happened. Checked before the defaults below are applied, because
# afterwards there is no way to tell an override from a default.
OVERRIDDEN=""
for knob in WARMUP DURATION COOLDOWN ROUNDS CONC_MAIN CONC_HIGH; do
    [ -n "${!knob:-}" ] && OVERRIDDEN="${OVERRIDDEN}${knob} "
done
[ "${SMOKE:-0}" = "1" ] && OVERRIDDEN="${OVERRIDDEN}SMOKE "

if [ -n "${OVERRIDDEN}" ]; then
    RESULTS_DIR="${BENCH_DIR}/results/scratch"
else
    RESULTS_DIR="${BENCH_DIR}/results"
fi

# A prefix of its own, so this can never collide with bench/run.sh's
# `ramjet-bench-*` or the Kubernetes suite's `ramjet-thesis-*` on a shared
# daemon.
PREFIX="ramjet-engine"
NET="${PREFIX}-net"
SUBNET="172.31.98.0/24"
IMAGE="${PREFIX}-ramjetd:latest"
SECCOMP="${BENCH_DIR}/seccomp-uring.json"

NGINX_IMAGE="nginx:1-alpine"
OHA_IMAGE="ghcr.io/hatoo/oha:latest"
CURL_IMAGE="curlimages/curl:latest"

IP_UP1="172.31.98.11"
IP_UP2="172.31.98.12"
IP_HYPER="172.31.98.21"
IP_URING="172.31.98.22"
IP_NGINX="172.31.98.23"
IP_BASE="172.31.98.24"

CPUS_PROXY="0,1"
CPUS_UP1="2"
CPUS_UP2="3"
CPUS_LOAD="4,5,6,7"
LOAD_THREADS=4

HOST_HEADER="bench.test"
if [ "${SMOKE:-0}" = "1" ]; then
    # Long enough to warm the per-core pools, short enough to be a smoke test.
    WARMUP="${WARMUP:-8s}"
    DURATION="${DURATION:-6s}"
    ROUNDS="${ROUNDS:-1}"
    COOLDOWN="${COOLDOWN:-5}"
fi
WARMUP="${WARMUP:-10s}"
DURATION="${DURATION:-30s}"
ROUNDS="${ROUNDS:-3}"
CONC_MAIN="${CONC_MAIN:-64}"
CONC_HIGH="${CONC_HIGH:-256}"
# Idle gap before each measured run. This machine is a laptop, and a laptop
# under sustained full load gets slower: the first measurement of a run is
# taken on a cold package and the last on a hot one. Left alone that is not
# noise, it is a *bias*, and it lands on whichever contender is measured last.
COOLDOWN="${COOLDOWN:-15}"

log()  { printf '\033[1;34m==>\033[0m %s\n' "$*"; }
warn() { printf '\033[1;33m[!]\033[0m %s\n' "$*" >&2; }
die()  { printf '\033[1;31m[x]\033[0m %s\n' "$*" >&2; exit 1; }

cleanup() {
    docker rm -f "${PREFIX}-up1" "${PREFIX}-up2" "${PREFIX}-hyper" \
                 "${PREFIX}-uring" "${PREFIX}-nginx" "${PREFIX}-base" \
                 >/dev/null 2>&1 || true
    docker network rm "${NET}" >/dev/null 2>&1 || true
}
trap cleanup EXIT INT TERM

preflight() {
    command -v docker  >/dev/null || die "docker is not on PATH"
    command -v python3 >/dev/null || die "python3 is needed to render the report"
    docker info >/dev/null 2>&1 || die "the docker daemon is not responding"
    [ -f "${SECCOMP}" ] || die "the seccomp profile ${SECCOMP} is missing"

    # Never measure while somebody else is measuring. The other suites pin the
    # same cores, and a run that shared them would be noise wearing a table's
    # clothes.
    local busy
    busy="$(docker ps --format '{{.Names}}' | grep -E '^(ramjet-bench|ramjet-thesis)-' || true)"
    [ -z "${busy}" ] || die "another benchmark is running (${busy//$'\n'/ }); wait for it"

    local cpus
    cpus="$(docker run --rm "${NGINX_IMAGE}" nproc 2>/dev/null || echo 0)"
    [ "${cpus}" -ge 8 ] || die "this layout pins 8 distinct cores; the docker VM reports ${cpus}"

    # The flag has to exist, or this would benchmark two identical hyper builds
    # and report the difference as noise.
    "${REPO_DIR}/target/release/ramjet-ingressd" --help 2>/dev/null \
        | grep -q -- "--engine" \
        || warn "the local binary has no --engine; the image is built fresh, so this is only a hint"
}

build_images() {
    log "pulling and building images"
    docker pull -q "${NGINX_IMAGE}" >/dev/null
    docker pull -q "${OHA_IMAGE}"   >/dev/null
    docker pull -q "${CURL_IMAGE}"  >/dev/null
    # Context is the parent of the repository: crates/ramjet-engine depends on
    # the sibling ramjet runtime by path, and cargo will not load a workspace
    # whose member has a dependency outside the context.
    docker build -q -f "${REPO_DIR}/bench/Dockerfile.ramjet" -t "${IMAGE}" \
        "${REPO_DIR}/.." >/dev/null
}

start_topology() {
    log "starting the topology"
    cleanup
    docker network create --driver bridge --subnet "${SUBNET}" "${NET}" >/dev/null

    # Upstreams first: nginx resolves upstream members at config load.
    docker run -d --name "${PREFIX}-up1" --network "${NET}" --ip "${IP_UP1}" \
        --cpuset-cpus="${CPUS_UP1}" \
        -v "${BENCH_DIR}/upstream.conf:/etc/nginx/nginx.conf:ro" \
        "${NGINX_IMAGE}" >/dev/null
    docker run -d --name "${PREFIX}-up2" --network "${NET}" --ip "${IP_UP2}" \
        --cpuset-cpus="${CPUS_UP2}" \
        -v "${BENCH_DIR}/upstream.conf:/etc/nginx/nginx.conf:ro" \
        "${NGINX_IMAGE}" >/dev/null

    # Both ramjet containers: same image, same cores, same seccomp profile,
    # one flag apart.
    docker run -d --name "${PREFIX}-hyper" --network "${NET}" --ip "${IP_HYPER}" \
        --cpuset-cpus="${CPUS_PROXY}" \
        --security-opt "seccomp=${SECCOMP}" \
        -v "${BENCH_DIR}/ramjet-routes.yaml:/etc/ramjet/routes.yaml:ro" \
        "${IMAGE}" --engine hyper --static-routes /etc/ramjet/routes.yaml --no-https >/dev/null
    docker run -d --name "${PREFIX}-uring" --network "${NET}" --ip "${IP_URING}" \
        --cpuset-cpus="${CPUS_PROXY}" \
        --security-opt "seccomp=${SECCOMP}" \
        -v "${BENCH_DIR}/ramjet-routes.yaml:/etc/ramjet/routes.yaml:ro" \
        "${IMAGE}" --engine uring --static-routes /etc/ramjet/routes.yaml --no-https >/dev/null

    docker run -d --name "${PREFIX}-nginx" --network "${NET}" --ip "${IP_NGINX}" \
        --cpuset-cpus="${CPUS_PROXY}" \
        -v "${BENCH_DIR}/nginx.conf:/etc/nginx/nginx.conf:ro" \
        "${NGINX_IMAGE}" >/dev/null

    # The baseline is an upstream on the proxies' own cores, so the hop's cost
    # is a like-for-like subtraction.
    docker run -d --name "${PREFIX}-base" --network "${NET}" --ip "${IP_BASE}" \
        --cpuset-cpus="${CPUS_PROXY}" \
        -v "${BENCH_DIR}/upstream.conf:/etc/nginx/nginx.conf:ro" \
        "${NGINX_IMAGE}" >/dev/null

    sleep 4
}

curl_in_net() { docker run --rm --network "${NET}" "${CURL_IMAGE}" "$@" 2>/dev/null; }

# The gate that decides whether io_uring is being measured at all.
verify_uring() {
    log "verifying the uring engine actually started on io_uring"
    if ! docker ps --format '{{.Names}}' | grep -q "^${PREFIX}-uring$"; then
        echo "--- ${PREFIX}-uring log ---" >&2
        docker logs "${PREFIX}-uring" 2>&1 | tail -20 >&2
        die "the uring container exited; io_uring is unavailable in this VM (see the log above)"
    fi
    local banner
    banner="$(docker logs "${PREFIX}-uring" 2>&1 | head -20)"
    echo "${banner}" | grep -q "engine uring" \
        || die "the uring container did not report the uring engine:\n${banner}"
    # On Linux the reactor is io_uring and nothing else: there is no fallback
    # path, so `UringDriver::new()` failing would have killed the process
    # before it could answer anything. A served request is the proof.
    log "  io_uring is live (the engine has no other backend on Linux)"
}

verify_correctness() {
    log "checking every contender answers identically before measuring anything"
    local hyper uring nginx base code
    hyper="$(curl_in_net -sS -H "Host: ${HOST_HEADER}" "http://${IP_HYPER}:8080/")"
    uring="$(curl_in_net -sS -H "Host: ${HOST_HEADER}" "http://${IP_URING}:8080/")"
    nginx="$(curl_in_net -sS -H "Host: ${HOST_HEADER}" "http://${IP_NGINX}:8080/")"
    base="$(curl_in_net  -sS -H "Host: ${HOST_HEADER}" "http://${IP_BASE}:8080/")"

    [ "${#hyper}" -eq 128 ] || die "ramjet(hyper) returned ${#hyper} bytes, expected 128"
    [ "${#uring}" -eq 128 ] || die "ramjet(uring) returned ${#uring} bytes, expected 128"
    [ "${hyper}" = "${base}" ]  || die "ramjet(hyper) and the baseline disagree"
    [ "${uring}" = "${base}" ]  || die "ramjet(uring) and the baseline disagree"
    [ "${nginx}" = "${base}" ]  || die "nginx and the baseline disagree"

    for name in hyper uring nginx; do
        local ip
        case "${name}" in
            hyper) ip="${IP_HYPER}" ;;
            uring) ip="${IP_URING}" ;;
            nginx) ip="${IP_NGINX}" ;;
        esac
        code="$(curl_in_net -sS -o /dev/null -w '%{http_code}' \
            -H "Host: ${HOST_HEADER}" "http://${ip}:8080/")"
        [ "${code}" = "200" ] || die "${name} answered ${code}, expected 200"
    done

    # Host routing must be exercised rather than passed through, and both
    # engines must agree about it. nginx is not asked: its config is a single
    # `default_server`, so it answers every Host by design and a 404 from it
    # would mean the config was wrong rather than the routing right. bench/run.sh
    # draws the line in the same place.
    for name in hyper uring; do
        local ip
        case "${name}" in
            hyper) ip="${IP_HYPER}" ;;
            uring) ip="${IP_URING}" ;;
        esac
        code="$(curl_in_net -sS -o /dev/null -w '%{http_code}' \
            -H "Host: wrong.invalid" "http://${ip}:8080/")"
        [ "${code}" = "404" ] || die "ramjet(${name}) answered ${code} for an unrouted Host, expected 404"
    done

    # The two engines must be indistinguishable on the wire, not merely both
    # plausible. Compare their headers field for field.
    local h_headers u_headers
    h_headers="$(curl_in_net -sS -D - -o /dev/null -H "Host: ${HOST_HEADER}" \
        "http://${IP_HYPER}:8080/" | grep -iv '^date:' | tr -d '\r' | sort)"
    u_headers="$(curl_in_net -sS -D - -o /dev/null -H "Host: ${HOST_HEADER}" \
        "http://${IP_URING}:8080/" | grep -iv '^date:' | tr -d '\r' | sort)"
    if [ "${h_headers}" != "${u_headers}" ]; then
        warn "the two engines' response headers differ:"
        diff <(echo "${h_headers}") <(echo "${u_headers}") >&2 || true
    fi
}

verify_topology() {
    local seen_hyper seen_uring seen_nginx
    seen_hyper="$(docker exec "${PREFIX}-hyper" nproc 2>/dev/null || echo '?')"
    seen_uring="$(docker exec "${PREFIX}-uring" nproc 2>/dev/null || echo '?')"
    seen_nginx="$(docker exec "${PREFIX}-nginx" nproc 2>/dev/null || echo '?')"
    [ "${seen_hyper}" = "${seen_nginx}" ] && [ "${seen_uring}" = "${seen_nginx}" ] \
        || die "contenders see different CPU counts: hyper=${seen_hyper} uring=${seen_uring} nginx=${seen_nginx}"
    log "  every contender sees ${seen_nginx} CPUs"
}

versions() {
    mkdir -p "${RESULTS_DIR}"
    {
        echo "date:      $(date -u '+%Y-%m-%dT%H:%M:%SZ')"
        echo "host:      $(uname -srm), $(sysctl -n hw.ncpu 2>/dev/null || nproc) host CPUs"
        echo "docker:    $(docker version --format '{{.Server.Version}}')"
        echo "docker VM: $(docker run --rm "${NGINX_IMAGE}" nproc) CPUs, $(docker run --rm "${NGINX_IMAGE}" uname -r)"
        echo "seccomp:   $(basename "${SECCOMP}") (moby v24.0.7 default + io_uring_{setup,enter,register})"
        echo "nginx:     ${NGINX_IMAGE} -> $(docker run --rm --entrypoint nginx "${NGINX_IMAGE}" -v 2>&1)"
        echo "oha:       ${OHA_IMAGE} -> $(docker run --rm "${OHA_IMAGE}" --version 2>&1)"
        echo "rustc:     $(docker run --rm --entrypoint rustc rust:1-slim-bookworm --version 2>&1)"
        echo "ramjet:    $(docker run --rm "${IMAGE}" --version 2>&1)"
    } | tee "${RESULTS_DIR}/versions.txt"
}

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
        ramjet-hyper) echo "${IP_HYPER}" ;;
        ramjet-uring) echo "${IP_URING}" ;;
        nginx)        echo "${IP_NGINX}" ;;
        baseline)     echo "${IP_BASE}" ;;
        *) die "unknown contender $1" ;;
    esac
}

measure() {
    local who="$1" conc="$2" run="$3" ip out rps
    ip="$(target_ip "${who}")"
    sleep "${COOLDOWN}"                                   # let the machine settle
    oha "${ip}" "${conc}" "${WARMUP}" >/dev/null          # discarded warmup
    out="${RESULTS_DIR}/${who}-c${conc}-r${run}.json"
    oha "${ip}" "${conc}" "${DURATION}" > "${out}"
    rps="$(python3 -c "import json,sys;print(f\"{json.load(open(sys.argv[1]))['summary']['requestsPerSec']:,.0f}\")" "${out}")"
    printf '    %-14s c%-4s run %s  %12s rps\n' "${who}" "${conc}" "${run}" "${rps}"
}

# Every contender, starting from a different one each round.
#
# Interleaving alone is not enough when the machine drifts within a round.
# Whoever goes first is measured on the coolest package and whoever goes last on
# the hottest, so a fixed order hands one contender a systematic advantage that
# no number of rounds averages away. Rotating the starting position spreads that
# position evenly across contenders instead.
CONTENDERS=(ramjet-hyper ramjet-uring nginx baseline)

rotated() {
    local offset="$1" i n
    n="${#CONTENDERS[@]}"
    for i in $(seq 0 $((n - 1))); do
        printf '%s\n' "${CONTENDERS[$(( (i + offset) % n ))]}"
    done
}

run_all() {
    mkdir -p "${RESULTS_DIR}"
    rm -f "${RESULTS_DIR}"/*.json
    # Said before any number is printed, not after, because the misreading it
    # prevents is somebody quoting a cold run as a regression.
    case "${WARMUP}" in
        [0-9]s|[0-9]) warn "WARMUP=${WARMUP} is too short to fill the per-core pools; these numbers validate the harness, they do not measure anything" ;;
    esac
    log "c${CONC_MAIN}: ${ROUNDS} rotated rounds of ${DURATION} (${WARMUP} warmup, ${COOLDOWN}s cooldown each)"
    for round in $(seq 1 "${ROUNDS}"); do
        echo "  round ${round}/${ROUNDS} (starting with $(rotated $((round - 1)) | head -1))"
        for who in $(rotated $((round - 1))); do
            measure "${who}" "${CONC_MAIN}" "${round}"
        done
    done
    log "c${CONC_HIGH}: one round"
    for who in $(rotated 0); do
        measure "${who}" "${CONC_HIGH}" 1
    done
}

passive_opens() {
    docker exec "${PREFIX}-$1" sh -c \
        "awk '/^Tcp:/{n++; if(n==2) print \$7}' /proc/net/snmp"
}

diagnostics() {
    log "diagnostics (a separate pass; not part of the measured runs)"
    : > "${RESULTS_DIR}/diagnostics.txt"
    for who in ramjet-hyper ramjet-uring nginx; do
        local ip container before1 before2 after1 after2 conns requests cpu mem out
        ip="$(target_ip "${who}")"
        case "${who}" in
            ramjet-hyper) container="${PREFIX}-hyper" ;;
            ramjet-uring) container="${PREFIX}-uring" ;;
            nginx)        container="${PREFIX}-nginx" ;;
        esac

        before1="$(passive_opens up1)"; before2="$(passive_opens up2)"
        out="$(oha "${ip}" "${CONC_MAIN}" 12s)"
        after1="$(passive_opens up1)";  after2="$(passive_opens up2)"
        conns=$(( (after1 - before1) + (after2 - before2) ))
        requests="$(python3 -c "
import json,sys
d = json.loads(sys.argv[1])
print(sum(d['summary'].get('statusCodeDistribution', {}).values()) or int(d['summary']['requestsPerSec']*12))
" "${out}" 2>/dev/null || echo 0)"

        oha "${ip}" "${CONC_MAIN}" 12s >/dev/null &
        local load_pid=$!
        sleep 6
        read -r cpu mem < <(docker stats --no-stream --format '{{.CPUPerc}} {{.MemUsage}}' "${container}" | head -1)
        wait "${load_pid}" 2>/dev/null || true

        local per_conn="all reused"
        [ "${conns}" -gt 0 ] && per_conn=$(( requests / conns ))
        printf '%s: cpu=%s mem=%s upstream_conns_opened=%s requests=%s reqs_per_conn=%s\n' \
            "${who}" "${cpu}" "${mem}" "${conns}" "${requests}" "${per_conn}" \
            | tee -a "${RESULTS_DIR}/diagnostics.txt"
    done

    # Sampled *while a load is running*. Taken after one, as it was at first,
    # this only ever reported 0.00% — which says the upstreams were idle,
    # not that they had headroom, and the whole point of the number is to show
    # the proxy was the bottleneck rather than them.
    oha "$(target_ip ramjet-uring)" "${CONC_MAIN}" 12s >/dev/null &
    local headroom_load=$!
    sleep 6
    printf 'upstream load while the proxy is saturated: %s\n' \
        "$(docker stats --no-stream --format '{{.Name}}={{.CPUPerc}}' "${PREFIX}-up1" "${PREFIX}-up2" | tr '\n' ' ')" \
        | tee -a "${RESULTS_DIR}/diagnostics.txt"
    wait "${headroom_load}" 2>/dev/null || true
}

# The thesis, measured directly: how many times the kernel is entered per
# thousand requests. The runtime prints its own counters when asked.
syscall_evidence() {
    log "counting ring entries per request (the whole point of the engine)"
    docker rm -f "${PREFIX}-uring" >/dev/null 2>&1 || true
    docker run -d --name "${PREFIX}-uring" --network "${NET}" --ip "${IP_URING}" \
        --cpuset-cpus="${CPUS_PROXY}" \
        --security-opt "seccomp=${SECCOMP}" \
        -e RAMJET_URING_STATS=2000 \
        -v "${BENCH_DIR}/ramjet-routes.yaml:/etc/ramjet/routes.yaml:ro" \
        "${IMAGE}" --engine uring --static-routes /etc/ramjet/routes.yaml --no-https >/dev/null
    sleep 3
    oha "${IP_URING}" "${CONC_MAIN}" 10s >/dev/null || true
    {
        echo "--- RAMJET_URING_STATS during a 10s c${CONC_MAIN} run ---"
        docker logs "${PREFIX}-uring" 2>&1 | tail -25
    } | tee -a "${RESULTS_DIR}/diagnostics.txt"
}

main() {
    if [ -n "${OVERRIDDEN}" ]; then
        warn "non-default settings (${OVERRIDDEN}) — writing to results/scratch/, not the committed results"
        warn "these numbers validate the harness; they are not a measurement"
    fi
    preflight
    build_images
    start_topology
    verify_uring
    verify_correctness
    verify_topology
    versions
    run_all
    diagnostics
    syscall_evidence
    log "rendering the table"
    python3 "${BENCH_DIR}/report.py" "${RESULTS_DIR}" | tee "${RESULTS_DIR}/table.md"
    log "raw JSON in ${RESULTS_DIR}; write the reading into RESULTS.md"
}

main "$@"
