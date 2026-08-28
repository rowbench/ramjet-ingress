#!/usr/bin/env bash
#
# The same three contenders as run.sh, over TLS: ramjet-ingress on the hyper
# engine, ramjet-ingress on the uring engine, and nginx. HTTP/1.1 on all three,
# so the comparison is of engines rather than of protocols.
#
# WHY THIS IS A SEPARATE SCRIPT
#
# Because it answers a different question, and mixing them would let a reader
# quote one number for the other. run.sh measures the syscall thesis: four
# operations per request, submitted into a ring instead of made one at a time.
# Under TLS that thesis is diluted on purpose — the record layer adds work that
# no amount of batching removes, and the plaintext engine's zero-copy relay is
# gone, because rustls has to read plaintext out of one buffer and write
# ciphertext into another. What a TLS run measures is whether the engine is
# still ahead once crypto is in the path, which is the shape almost all real
# ingress traffic has.
#
# FAIRNESS
#
# The one setting that decides a TLS benchmark is session resumption. nginx
# ships `ssl_session_tickets on` and every deployment turns on
# `ssl_session_cache`; a run against a ramjet with resumption off would be
# measuring a configuration nobody deploys, and would flatter nginx by exactly
# the cost of a signature per connection. So both sides resume: nginx-tls.conf
# sets a shared cache and tickets, and ramjet installs a rustls ticketer for
# both of its lanes.
#
# The certificate is generated per run — same key, same algorithm, same file,
# mounted into all three containers — so certificate size and key type cannot
# differ between contenders. ECDSA P-256, which is what a modern deployment
# uses and what both sides are fastest at; an RSA-2048 key would make the
# handshake cost dominate and measure OpenSSL against ring rather than the
# engines around them.
#
# WHAT KEEP-ALIVE MEANS HERE
#
# oha reuses connections, so a 30-second run is a handful of handshakes and
# hundreds of thousands of requests over them. That is the right default: it is
# what a browser and a load balancer both do, and it measures the record layer
# rather than the handshake. The handshake has its own scenario below, driven
# with `--disable-keepalive`, because "how many new TLS connections a second"
# is a real capacity question and the answer is a different number entirely.
#
# Usage:
#     ./bench/engine/tls-run.sh
#     SMOKE=1 ./bench/engine/tls-run.sh     # validate the script, ~4 minutes

set -euo pipefail

BENCH_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "${BENCH_DIR}/../.." && pwd)"

# Same guard as run.sh: only an unmodified, committed-protocol run may write
# where the committed measurement lives. A short run leaving partial output
# where the real one was produces a *plausible* wrong number, which is worse
# than an obviously broken one — it gets quoted.
OVERRIDDEN=""
for knob in WARMUP DURATION ROUNDS CONC_MAIN CONC_HIGH COOLDOWN; do
    [ -n "${!knob:-}" ] && OVERRIDDEN="${OVERRIDDEN}${knob} "
done
[ "${SMOKE:-0}" = "1" ] && OVERRIDDEN="${OVERRIDDEN}SMOKE "

if [ -n "${OVERRIDDEN}" ]; then
    RESULTS_DIR="${BENCH_DIR}/results-tls/scratch"
else
    RESULTS_DIR="${BENCH_DIR}/results-tls"
fi

# A prefix of its own, so a TLS run and a plaintext run can never collide on a
# shared daemon — including when somebody runs both at once to save time and
# gets two benchmarks fighting for the same four cores.
PREFIX="ramjet-tls"
NET="${PREFIX}-net"
SUBNET="172.31.98.0/24"
IMAGE="ramjet-engine-ramjetd:latest"
SECCOMP="${BENCH_DIR}/seccomp-uring.json"
CERT_DIR="${BENCH_DIR}/certs"

NGINX_IMAGE="nginx:1-alpine"
OHA_IMAGE="ghcr.io/hatoo/oha:latest"
CURL_IMAGE="curlimages/curl:latest"
# nginx:1-alpine links OpenSSL but ships no `openssl` binary, so the key is
# generated in an image that has one.
OPENSSL_IMAGE="alpine/openssl:latest"

IP_UP1="172.31.98.11"
IP_UP2="172.31.98.12"
IP_HYPER="172.31.98.21"
IP_URING="172.31.98.22"
IP_NGINX="172.31.98.23"

CPUS_PROXY="0,1"
CPUS_UP1="2"
CPUS_UP2="3"
CPUS_LOAD="4,5,6,7"
LOAD_THREADS=4

HOST_HEADER="bench.test"
if [ "${SMOKE:-0}" = "1" ]; then
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
COOLDOWN="${COOLDOWN:-15}"

log()  { printf '\033[1;34m==>\033[0m %s\n' "$*"; }
warn() { printf '\033[1;33m[!]\033[0m %s\n' "$*" >&2; }
die()  { printf '\033[1;31m[x]\033[0m %s\n' "$*" >&2; exit 1; }

cleanup() {
    docker rm -f "${PREFIX}-up1" "${PREFIX}-up2" "${PREFIX}-hyper" \
        "${PREFIX}-uring" "${PREFIX}-nginx" >/dev/null 2>&1 || true
    docker network rm "${NET}" >/dev/null 2>&1 || true
}
trap cleanup EXIT

make_cert() {
    log "generating the certificate all three contenders will serve"
    rm -rf "${CERT_DIR}"
    mkdir -p "${CERT_DIR}"
    # In a container, so the result does not depend on which openssl or
    # libressl the host happens to ship — macOS and Linux disagree, and a
    # different key type would change what is being measured.
    docker run --rm -v "${CERT_DIR}:/out" "${OPENSSL_IMAGE}" \
        req -x509 -nodes -days 1 \
        -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 \
        -keyout /out/key.pem -out /out/cert.pem \
        -subj "/CN=bench.test" -addext "subjectAltName=DNS:bench.test" \
        >/dev/null 2>&1 \
        || die "could not generate a certificate"
    chmod 644 "${CERT_DIR}/key.pem" "${CERT_DIR}/cert.pem"
}

build_image() {
    log "building the ramjet image"
    docker build -q -f "${REPO_DIR}/bench/Dockerfile.ramjet" \
        -t "${IMAGE}" "$(dirname "${REPO_DIR}")" >/dev/null \
        || die "the image did not build"
}

start_stack() {
    log "starting the stack"
    cleanup
    docker network create --driver bridge --subnet "${SUBNET}" "${NET}" >/dev/null

    docker run -d --name "${PREFIX}-up1" --network "${NET}" --ip "${IP_UP1}" \
        --cpuset-cpus="${CPUS_UP1}" \
        -v "${BENCH_DIR}/upstream.conf:/etc/nginx/nginx.conf:ro" \
        "${NGINX_IMAGE}" >/dev/null
    docker run -d --name "${PREFIX}-up2" --network "${NET}" --ip "${IP_UP2}" \
        --cpuset-cpus="${CPUS_UP2}" \
        -v "${BENCH_DIR}/upstream.conf:/etc/nginx/nginx.conf:ro" \
        "${NGINX_IMAGE}" >/dev/null

    # Both ramjet containers: same image, same cores, same seccomp profile,
    # same certificate, one flag apart.
    #
    # `--no-h2-dispatch` on the uring container, deliberately. With dispatch on
    # it would advertise h2 and start a second engine, and this run is about
    # HTTP/1.1 on one engine — a second engine's threads competing for the same
    # two cores would be measuring the wrong thing. The hyper container is
    # HTTP/1.1 for the same reason: oha does not ask for h2, so ALPN settles on
    # http/1.1 there without any flag.
    #
    # `uring-strict`, not `uring`: a silent fallback to hyper would make this
    # benchmark compare hyper against hyper and report it as an engine result.
    docker run -d --name "${PREFIX}-hyper" --network "${NET}" --ip "${IP_HYPER}" \
        --cpuset-cpus="${CPUS_PROXY}" \
        --security-opt "seccomp=${SECCOMP}" \
        -v "${BENCH_DIR}/ramjet-routes-tls.yaml:/etc/ramjet/routes.yaml:ro" \
        -v "${CERT_DIR}:/etc/ramjet/certs:ro" \
        "${IMAGE}" --engine hyper --static-routes /etc/ramjet/routes.yaml \
        --https=:8443 --no-http >/dev/null
    docker run -d --name "${PREFIX}-uring" --network "${NET}" --ip "${IP_URING}" \
        --cpuset-cpus="${CPUS_PROXY}" \
        --security-opt "seccomp=${SECCOMP}" \
        -v "${BENCH_DIR}/ramjet-routes-tls.yaml:/etc/ramjet/routes.yaml:ro" \
        -v "${CERT_DIR}:/etc/ramjet/certs:ro" \
        "${IMAGE}" --engine uring-strict --no-h2-dispatch \
        --static-routes /etc/ramjet/routes.yaml \
        --https=:8443 --no-http >/dev/null

    docker run -d --name "${PREFIX}-nginx" --network "${NET}" --ip "${IP_NGINX}" \
        --cpuset-cpus="${CPUS_PROXY}" \
        -v "${BENCH_DIR}/nginx-tls.conf:/etc/nginx/nginx.conf:ro" \
        -v "${CERT_DIR}:/etc/ramjet/certs:ro" \
        "${NGINX_IMAGE}" >/dev/null

    sleep 4
}

curl_in_net() { docker run --rm --network "${NET}" "${CURL_IMAGE}" "$@" 2>/dev/null; }

# The gate that decides whether io_uring is being measured at all.
#
# `uring-strict` means a blocked `io_uring_setup` kills the process rather than
# falling back, so a container that is up and answering is a container running
# on io_uring. Without the strict flag this check would pass on a fallback to
# hyper and the whole run would silently compare hyper against hyper.
verify_uring() {
    log "verifying the uring engine actually started on io_uring"
    if ! docker ps --format '{{.Names}}' | grep -q "^${PREFIX}-uring$"; then
        echo "--- ${PREFIX}-uring log ---" >&2
        docker logs "${PREFIX}-uring" 2>&1 | tail -20 >&2
        die "the uring container exited; io_uring is unavailable in this VM (see the log above)"
    fi
    local banner
    banner="$(docker logs "${PREFIX}-uring" 2>&1 | head -30)"
    echo "${banner}" | grep -q "engine uring" \
        || die "the uring container did not report the uring engine: ${banner}"
    echo "${banner}" | grep -q "falling back" \
        && die "the uring container fell back to hyper; this run would compare hyper with hyper"
    log "  io_uring is live (uring-strict would have died otherwise)"
}

# Nothing is measured until every contender is proven to be doing the same job.
#
# A benchmark against a proxy that is 502ing is a benchmark of an error path,
# and it is fast.
verify_correct() {
    log "checking all three contenders answer the same way"
    local who ip body
    for who in hyper uring nginx; do
        case "${who}" in
            hyper) ip="${IP_HYPER}" ;;
            uring) ip="${IP_URING}" ;;
            nginx) ip="${IP_NGINX}" ;;
        esac
        body="$(curl_in_net -sk --resolve "bench.test:8443:${ip}" \
            "https://bench.test:8443/" || true)"
        [ -n "${body}" ] || die "${who} returned nothing over TLS"
        # The upstream is the same nginx for all three, so the bodies must match
        # byte for byte.
        if [ -z "${EXPECT:-}" ]; then
            EXPECT="${body}"
        elif [ "${body}" != "${EXPECT}" ]; then
            die "${who} returned a different body than the contender before it"
        fi
    done
    log "  all three serve the same bytes"

    # And the protocol is the one this run claims to measure.
    #
    # `--http1.1` because that is what oha does, and oha is what takes the
    # measurement. Two of the three contenders *offer* h2 — the hyper lane
    # always does, and nginx would with an `http2` directive it does not have —
    # so the question is not what they would accept but what the load generator
    # asks for. A client that does not offer h2 cannot be given it, and this
    # check proves the negotiation lands where the run claims.
    #
    # The earlier version of this check used curl's default, which offers h2,
    # and failed on the hyper contender: a useful failure, and the reason the
    # flag is here rather than the protocol being assumed.
    local version
    for who in hyper uring nginx; do
        case "${who}" in
            hyper) ip="${IP_HYPER}" ;;
            uring) ip="${IP_URING}" ;;
            nginx) ip="${IP_NGINX}" ;;
        esac
        version="$(curl_in_net -sk --http1.1 -o /dev/null -w '%{http_version}' \
            --resolve "bench.test:8443:${ip}" "https://bench.test:8443/" || true)"
        [ "${version}" = "1.1" ] \
            || die "${who} served HTTP/${version} to an HTTP/1.1 client; this run compares one protocol"
    done
    log "  all three serve HTTP/1.1 to an HTTP/1.1 client"
}

versions() {
    {
        echo "date:      $(date -u +%FT%TZ)"
        echo "host:      $(uname -srm)"
        echo "docker:    $(docker version --format '{{.Server.Version}}')"
        echo "kernel:    $(docker run --rm "${NGINX_IMAGE}" uname -r)"
        echo "ramjet:    $(cd "${REPO_DIR}" && git rev-parse --short HEAD)"
        # `tail -1`: the image's entrypoint writes a banner to stderr before
        # running the command, and without this the version line is fourteen
        # lines of docker-entrypoint noise with the answer at the bottom.
        echo "nginx:     $(docker run --rm "${NGINX_IMAGE}" nginx -v 2>&1 | tail -1)"
        echo "oha:       $(docker run --rm "${OHA_IMAGE}" --version 2>&1)"
        echo "openssl:   $(docker run --rm "${OPENSSL_IMAGE}" version 2>&1)"
        echo "protocol:  https, HTTP/1.1, keep-alive"
        echo "duration:  ${DURATION} x ${ROUNDS} rounds, warmup ${WARMUP}"
    } | tee "${RESULTS_DIR}/versions.txt"
}

# `-k` because the certificate is self-signed and generated seconds earlier.
# That is not a shortcut around verification cost: oha verifies once per
# connection, and with keep-alive on there are a handful of connections in a
# 30-second run. Turning it off changes nothing measurable and removes a CA
# fixture with an expiry date from the repository.
oha() {
    local target="$1" conc="$2" dur="$3"
    shift 3
    docker run --rm --network "${NET}" --cpuset-cpus="${CPUS_LOAD}" "${OHA_IMAGE}" \
        --no-tui --output-format json \
        -c "${conc}" -z "${dur}" -w \
        --worker-threads "${LOAD_THREADS}" \
        --insecure \
        --connect-to "bench.test:8443:${target}:8443" \
        "https://bench.test:8443/" "$@" 2>/dev/null
}

target_ip() {
    case "$1" in
        ramjet-hyper) echo "${IP_HYPER}" ;;
        ramjet-uring) echo "${IP_URING}" ;;
        nginx)        echo "${IP_NGINX}" ;;
        *) die "unknown contender $1" ;;
    esac
}

measure() {
    local who="$1" conc="$2" run="$3" ip out rps
    ip="$(target_ip "${who}")"
    sleep "${COOLDOWN}"
    oha "${ip}" "${conc}" "${WARMUP}" >/dev/null
    out="${RESULTS_DIR}/${who}-c${conc}-r${run}.json"
    oha "${ip}" "${conc}" "${DURATION}" > "${out}"
    rps="$(python3 -c "import json,sys;print(f\"{json.load(open(sys.argv[1]))['summary']['requestsPerSec']:,.0f}\")" "${out}")"
    printf '    %-14s c%-4s run %s  %12s rps\n' "${who}" "${conc}" "${run}" "${rps}"
}

# The handshake scenario: a new connection per request.
#
# A different question from the one above, and worth asking separately. Under
# keep-alive the handshake is amortised to nothing and the number describes the
# record layer; here it dominates, and the number describes how many new TLS
# clients a replica can absorb — which is what a deployment rolling, or a CDN
# reconnecting, actually does to it.
measure_handshake() {
    local who="$1" run="$2" ip out rps
    ip="$(target_ip "${who}")"
    sleep "${COOLDOWN}"
    oha "${ip}" "${CONC_MAIN}" "${WARMUP}" --disable-keepalive >/dev/null
    out="${RESULTS_DIR}/${who}-handshake-r${run}.json"
    oha "${ip}" "${CONC_MAIN}" "${DURATION}" --disable-keepalive > "${out}"
    rps="$(python3 -c "import json,sys;print(f\"{json.load(open(sys.argv[1]))['summary']['requestsPerSec']:,.0f}\")" "${out}")"
    printf '    %-14s handshake run %s  %12s conn/s\n' "${who}" "${run}" "${rps}"
}

CONTENDERS=(ramjet-hyper ramjet-uring nginx)

main() {
    command -v docker >/dev/null || die "docker is not on PATH"
    command -v python3 >/dev/null || die "python3 is not on PATH"

    if [ -n "${OVERRIDDEN}" ]; then
        warn "non-protocol run (${OVERRIDDEN}); writing to results-tls/scratch/"
        warn "these numbers are NOT comparable to the committed ones"
    fi
    mkdir -p "${RESULTS_DIR}"

    make_cert
    build_image
    start_stack
    verify_uring
    verify_correct
    versions

    local round position who conc
    for conc in "${CONC_MAIN}" "${CONC_HIGH}"; do
        log "keep-alive, concurrency ${conc}"
        for round in $(seq 1 "${ROUNDS}"); do
            # Rotate the starting contender: interleaving alone still hands
            # whoever goes first the coolest package, every round.
            position=$(( (round - 1) % ${#CONTENDERS[@]} ))
            for i in "${!CONTENDERS[@]}"; do
                who="${CONTENDERS[$(( (position + i) % ${#CONTENDERS[@]} ))]}"
                measure "${who}" "${conc}" "${round}"
            done
        done
    done

    log "new connection per request (handshake cost)"
    for round in $(seq 1 "${ROUNDS}"); do
        position=$(( (round - 1) % ${#CONTENDERS[@]} ))
        for i in "${!CONTENDERS[@]}"; do
            who="${CONTENDERS[$(( (position + i) % ${#CONTENDERS[@]} ))]}"
            measure_handshake "${who}" "${round}"
        done
    done

    log "done; results in ${RESULTS_DIR}"
    log "render with: python3 ${BENCH_DIR}/report.py ${RESULTS_DIR}"
}

main "$@"
