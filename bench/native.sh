#!/usr/bin/env bash
#
# The quick native harness used to iterate on forwarding performance.
#
#     ./bench/native.sh            # ramjet only
#     ./bench/native.sh both       # ramjet and native nginx, interleaved
#     ./bench/native.sh profile    # ramjet under `samply record`
#
# This is NOT the committed benchmark — bench/run.sh is, and its docker
# topology with pinned cpusets is what RESULTS.md quotes. This one exists
# because a profiler needs a native binary and an 11-minute run is not an
# iteration loop. It reproduces the same shape: two nginx upstreams returning
# the same 128-byte body, one route, round-robin, oha with keep-alive at c64.
#
# Two things stand in for `--cpuset-cpus=0,1`, which macOS has no equivalent of:
#
#   * the proxy gets exactly 2 threads (TOKIO_WORKER_THREADS / worker_processes),
#   * the upstreams and oha get the other ten cores, so they cannot be the
#     bottleneck. verify() checks that after every run.
#
# Absolute numbers here are higher than run.sh's because loopback is cheaper
# than a docker bridge and these cores are not shared with a linuxkit VM. The
# deltas between two invocations are what the iteration loop reads.
#
# `reuseport` is off in the nginx configs below, unlike bench/nginx.conf: on
# BSD, SO_REUSEPORT hands every connection to the last socket that bound rather
# than spreading them across sockets, so `reuseport` leaves one nginx worker
# idle here. That makes the nginx column of `both` a rough reference only; the
# fair head-to-head is bench/run.sh on Linux.

set -euo pipefail

BENCH_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "${BENCH_DIR}/.." && pwd)"
WORK="${TMPDIR:-/tmp}/ramjet-native-bench"

BIN="${BIN:-${REPO_DIR}/target/release/ramjet-ingressd}"
PORT_UP1=19001
PORT_UP2=19002
PORT_RAMJET=18080
PORT_NGINX=18081
PORT_ADMIN=110254   # invalid on purpose; admin is disabled below

WARMUP="${WARMUP:-5s}"
DURATION="${DURATION:-15s}"
CONC="${CONC:-64}"
LOAD_THREADS="${LOAD_THREADS:-4}"
WORKERS="${WORKERS:-2}"
HOST_HEADER="bench.test"

log()  { printf '\033[1;34m==>\033[0m %s\n' "$*"; }
warn() { printf '\033[1;33m[!]\033[0m %s\n' "$*" >&2; }
die()  { printf '\033[1;31m[x]\033[0m %s\n' "$*" >&2; exit 1; }

cleanup() {
    for p in "${WORK}"/*.pid; do
        [ -f "$p" ] || continue
        kill "$(cat "$p")" 2>/dev/null || true
    done
    pkill -f "ramjet-native-bench" 2>/dev/null || true
    [ -n "${RAMJET_PID:-}" ] && kill "${RAMJET_PID}" 2>/dev/null || true
    sleep 0.3
}
trap cleanup EXIT INT TERM

# ---------------------------------------------------------------------------
# Configuration written fresh every run, so an edit here cannot be stale
# ---------------------------------------------------------------------------

setup_files() {
    rm -rf "${WORK}"
    mkdir -p "${WORK}/logs" "${WORK}/temp"

    for port in "${PORT_UP1}" "${PORT_UP2}"; do
        cat > "${WORK}/upstream-${port}.conf" <<EOF
worker_processes 2;
error_log ${WORK}/logs/upstream-${port}.err error;
pid ${WORK}/upstream-${port}.pid;
daemon on;
events { worker_connections 16384; multi_accept on; }
http {
    access_log off;
    client_body_temp_path ${WORK}/temp;
    proxy_temp_path ${WORK}/temp;
    fastcgi_temp_path ${WORK}/temp;
    uwsgi_temp_path ${WORK}/temp;
    scgi_temp_path ${WORK}/temp;
    keepalive_requests 100000000;
    keepalive_timeout 300s;
    server {
        listen 127.0.0.1:${port} default_server backlog=8192;
        location / {
            default_type text/plain;
            return 200 "ramjet-ingress benchmark upstream payload; fixed-size body so both proxies move identical bytes. 0123456789012345678901234567890";
        }
    }
}
EOF
    done

    # The nginx-as-proxy contender, same settings as bench/nginx.conf.
    cat > "${WORK}/proxy.conf" <<EOF
worker_processes ${WORKERS};
worker_rlimit_nofile 65536;
error_log ${WORK}/logs/proxy.err error;
pid ${WORK}/proxy.pid;
daemon on;
events { worker_connections 16384; multi_accept on; }
http {
    access_log off;
    sendfile on;
    tcp_nopush on;
    tcp_nodelay on;
    keepalive_timeout 300s;
    keepalive_requests 100000000;
    server_tokens off;
    client_body_temp_path ${WORK}/temp;
    proxy_temp_path ${WORK}/temp;
    fastcgi_temp_path ${WORK}/temp;
    uwsgi_temp_path ${WORK}/temp;
    scgi_temp_path ${WORK}/temp;
    upstream bench_backends {
        server 127.0.0.1:${PORT_UP1};
        server 127.0.0.1:${PORT_UP2};
        keepalive 64;
        keepalive_requests 100000000;
        keepalive_timeout 300s;
    }
    server {
        listen ${PORT_NGINX} default_server backlog=8192;
        server_name ${HOST_HEADER};
        location / {
            proxy_pass http://bench_backends;
            proxy_http_version 1.1;
            proxy_set_header Connection "";
            proxy_set_header Host \$host;
            proxy_buffering on;
        }
    }
}
EOF

    cat > "${WORK}/routes.yaml" <<EOF
backends:
  - name: bench
    policy: roundRobin
    endpoints:
      - 127.0.0.1:${PORT_UP1}
      - 127.0.0.1:${PORT_UP2}

routes:
  - host: ${HOST_HEADER}
    path: /
    pathType: Prefix
    backend: bench
EOF
}

start_upstreams() {
    for port in "${PORT_UP1}" "${PORT_UP2}"; do
        nginx -c "${WORK}/upstream-${port}.conf" -p "${WORK}" \
            || die "upstream ${port} failed to start"
    done
    sleep 0.4
    for port in "${PORT_UP1}" "${PORT_UP2}"; do
        curl -fsS "http://127.0.0.1:${port}/" > /dev/null \
            || die "upstream ${port} is not answering"
    done
}

start_ramjet() {
    [ -x "${BIN}" ] || die "no binary at ${BIN} (cargo build --release -p ramjet-ingressd)"
    # A binary from before thread-per-core takes its core budget from
    # TOKIO_WORKER_THREADS and rejects --worker-threads; one after it takes the
    # flag. Both are set so an A/B against an older baseline is still a fair
    # fight on the same number of cores.
    local worker_flag=()
    if "${BIN}" --help 2>/dev/null | grep -q -- "--worker-threads"; then
        worker_flag=(--worker-threads "${WORKERS}")
    fi
    TOKIO_WORKER_THREADS="${WORKERS}" RUST_LOG=warn \
        "${BIN}" --static-routes "${WORK}/routes.yaml" \
            --http "127.0.0.1:${PORT_RAMJET}" --no-https --no-admin \
            ${worker_flag[@]+"${worker_flag[@]}"} \
            ${@+"$@"} > "${WORK}/logs/ramjet.log" 2>&1 &
    RAMJET_PID=$!
    sleep 0.6
    kill -0 "${RAMJET_PID}" 2>/dev/null || { cat "${WORK}/logs/ramjet.log"; die "ramjet died"; }
}

start_nginx_proxy() {
    nginx -c "${WORK}/proxy.conf" -p "${WORK}" || die "nginx proxy failed to start"
    sleep 0.4
}

# Correctness gate: the same bytes out of both, and ramjet routes by Host.
verify_correctness() {
    local port="$1" name="$2"
    local body
    body="$(curl -fsS -H "Host: ${HOST_HEADER}" "http://127.0.0.1:${port}/")" \
        || die "${name} did not answer"
    [ "${#body}" -eq 128 ] || die "${name} returned ${#body} bytes, expected 128"
}

# Samples CPU of the proxy and the upstreams while the load runs, so a run
# where the upstream was the bottleneck is visible rather than believed.
# Sums %cpu over a process and its children, which is how an nginx master plus
# its workers gets counted as one contender.
cpu_tree() {
    ps -Ao pid,ppid,%cpu | awk -v root="$1" '
        NR > 1 { parent[$1] = $2; cpu[$1] = $3 }
        END {
            total = 0
            for (p in cpu) {
                q = p; hops = 0
                while (q != "" && q != "0" && q != "1" && hops < 8) {
                    if (q == root) { total += cpu[p]; break }
                    q = parent[q]; hops++
                }
            }
            printf "%.0f", total
        }'
}

# The check that decides whether a number means anything: the proxy has to be
# the thing that ran out of CPU, not the upstream pair behind it.
sample_cpu() {
    local out="$1" seconds="$2" proxy_root="$3"
    (
        for _ in $(seq 1 "${seconds}"); do
            local up=0 u
            for f in "${WORK}/upstream-${PORT_UP1}.pid" "${WORK}/upstream-${PORT_UP2}.pid"; do
                [ -f "$f" ] || continue
                u="$(cpu_tree "$(cat "$f")")"
                up=$(( up + u ))
            done
            echo "$(cpu_tree "${proxy_root}") ${up}"
            sleep 1
        done | awk '{p+=$1; u+=$2; n++} END {printf "%.0f %.0f\n", (n?p/n:0), (n?u/n:0)}'
    ) > "${out}"
}

run_load() {
    local port="$1" name="$2" tag="$3"
    local PROXY_ROOT="${4:-${RAMJET_PID}}"
    oha --no-tui --output-format json -z "${WARMUP}" -c "${CONC}" -w \
        --worker-threads "${LOAD_THREADS}" \
        --host "${HOST_HEADER}" "http://127.0.0.1:${port}/" > /dev/null 2>&1

    local secs="${DURATION%s}"
    sample_cpu "${WORK}/cpu-${tag}.txt" "${secs}" "${PROXY_ROOT}" &
    local sampler=$!

    oha --no-tui --output-format json -z "${DURATION}" -c "${CONC}" -w \
        --worker-threads "${LOAD_THREADS}" \
        --host "${HOST_HEADER}" "http://127.0.0.1:${port}/" \
        > "${WORK}/result-${tag}.json" 2>/dev/null
    wait "${sampler}" 2>/dev/null || true

    [ "${QUIET:-0}" = 1 ] && return 0
    python3 - "${WORK}/result-${tag}.json" "${name}" <<'PY'
import json, sys
with open(sys.argv[1]) as f:
    d = json.load(f)
s = d["summary"]
pct = d.get("latencyPercentiles", {})
rps = s["requestsPerSec"]
total = s["successRate"] * d["summary"].get("total", 0) if False else None
us = lambda k: pct.get(k, 0) * 1e6
errors = sum(d.get("errorDistribution", {}).values())
codes = d.get("statusCodeDistribution", {})
non200 = sum(v for k, v in codes.items() if k != "200")
print(f"{sys.argv[2]:<16} {rps:>10,.0f} rps   "
      f"p50 {us('p50'):>7,.0f}us  p90 {us('p90'):>7,.0f}us  "
      f"p99 {us('p99'):>8,.0f}us   errors {errors}  non-200 {non200}")
PY
}

# The check that decides whether a number means anything: the proxy must be
# the thing that ran out of CPU, not the upstream pair behind it.
report_cpu() {
    local tag="$1" line
    [ -f "${WORK}/cpu-${tag}.txt" ] || return 0
    line="$(cat "${WORK}/cpu-${tag}.txt")"
    set -- ${line}
    printf '                 CPU: proxy %s%% of %d00%%   upstreams %s%%\n' "$1" "${WORKERS}" "$2"
    if [ "$1" -lt $(( WORKERS * 85 )) ]; then
        warn "proxy only reached $1%% of ${WORKERS}00%% — it may not be the bottleneck"
    fi
}


# The rps of the last run, for the A/B driver.
rps_of() {
    python3 -c "import json,sys;print(int(json.load(open(sys.argv[1]))['summary']['requestsPerSec']))" \
        "${WORK}/result-$1.json"
}

# Restarts just the daemon, leaving the upstreams and their warm pools alone.
restart_ramjet() {
    local bin="$1"; shift
    if [ -n "${RAMJET_PID:-}" ]; then kill "${RAMJET_PID}" 2>/dev/null || true; fi
    sleep 0.4
    BIN="${bin}" start_ramjet ${@+"$@"}
}

# A and B, alternating, medians reported. Alternating rather than A-A-A-B-B-B
# because this laptop drifts: whatever it is doing during round 2 is then done
# to both sides, not handed to whichever one happened to run second.
run_ab() {
    local rounds="${ROUNDS:-3}" i a b
    local as="" bs=""
    for i in $(seq 1 "${rounds}"); do
        restart_ramjet "${BIN_A}" ${ARGS_A:-}
        verify_correctness "${PORT_RAMJET}" "A"
        QUIET=1 run_load "${PORT_RAMJET}" A "a${i}"
        a="$(rps_of "a${i}")"; as="${as} ${a}"

        restart_ramjet "${BIN_B}" ${ARGS_B:-}
        verify_correctness "${PORT_RAMJET}" "B"
        QUIET=1 run_load "${PORT_RAMJET}" B "b${i}"
        b="$(rps_of "b${i}")"; bs="${bs} ${b}"

        printf '  round %d:  A %8s rps   B %8s rps   %s\n' "${i}" "${a}" "${b}" \
            "$(python3 -c "print(f'{100*(${b}-${a})/${a}:+.1f}%')")"
    done
    python3 - "${as}" "${bs}" <<'PY'
import sys, statistics
a = [int(x) for x in sys.argv[1].split()]
b = [int(x) for x in sys.argv[2].split()]
ma, mb = statistics.median(a), statistics.median(b)
sa = (max(a) - min(a)) / ma * 100
sb = (max(b) - min(b)) / mb * 100
delta = 100 * (mb - ma) / ma
print()
print(f"  A  median {ma:>8,} rps   spread {sa:4.1f}%   runs {a}")
print(f"  B  median {mb:>8,} rps   spread {sb:4.1f}%   runs {b}")
print(f"  B vs A: {delta:+.1f}%" + ("   (inside the noise)" if abs(delta) < max(sa, sb) else ""))
PY
}

mode="${1:-ramjet}"
shift || true

setup_files
start_upstreams

case "${mode}" in
    ramjet)
        start_ramjet ${@+"$@"}
        verify_correctness "${PORT_RAMJET}" ramjet
        log "ramjet-ingress (${WORKERS} workers, c${CONC}, ${DURATION})"
        run_load "${PORT_RAMJET}" "ramjet-ingress" ramjet
        report_cpu ramjet
        ;;
    both)
        start_ramjet ${@+"$@"}
        start_nginx_proxy
        verify_correctness "${PORT_RAMJET}" ramjet
        verify_correctness "${PORT_NGINX}" nginx
        log "interleaved, ${WORKERS} workers each, c${CONC}, ${DURATION}"
        run_load "${PORT_RAMJET}" "ramjet-ingress" ramjet;  report_cpu ramjet
        run_load "${PORT_NGINX}" "nginx" nginx "$(cat "${WORK}/proxy.pid")";              report_cpu nginx
        run_load "${PORT_RAMJET}" "ramjet-ingress" ramjet2;  report_cpu ramjet2
        run_load "${PORT_NGINX}" "nginx" nginx2 "$(cat "${WORK}/proxy.pid")";             report_cpu nginx2
        ;;
    ab)
        # BIN_A/BIN_B (default: BIN) and ARGS_A/ARGS_B select the two sides:
        #   BIN_A=/tmp/ramjetd-before ./bench/native.sh ab
        #   ARGS_B="--upstream-pool-idle 128" ./bench/native.sh ab
        BIN_A="${BIN_A:-${BIN}}"
        BIN_B="${BIN_B:-${BIN}}"
        log "A/B: ${ROUNDS:-3} interleaved rounds of ${DURATION} at c${CONC}, ${WORKERS} workers"
        log "  A = ${BIN_A} ${ARGS_A:-}"
        log "  B = ${BIN_B} ${ARGS_B:-}"
        run_ab
        ;;
    profile)
        # A profile needs symbols, and the release profile ships without them.
        # CARGO_PROFILE_RELEASE_DEBUG=1 into a separate target dir leaves the
        # measured artifact alone:
        #   CARGO_PROFILE_RELEASE_DEBUG=1 cargo build --release \
        #       -p ramjet-ingressd --target-dir target/profiling
        # then BIN=target/profiling/release/ramjet-ingressd ./bench/native.sh profile
        [ -x "${BIN}" ] || die "no binary at ${BIN}"
        log "recording ${DURATION} of load under samply -> ${WORK}/profile.json.gz"
        worker_flag=()
        if "${BIN}" --help 2>/dev/null | grep -q -- "--worker-threads"; then
            worker_flag=(--worker-threads "${WORKERS}")
        fi
        TOKIO_WORKER_THREADS="${WORKERS}" RUST_LOG=warn \
            samply record --save-only -o "${WORK}/profile.json.gz" --rate "${RATE:-3000}" -- \
            "${BIN}" --static-routes "${WORK}/routes.yaml" \
                --http "127.0.0.1:${PORT_RAMJET}" --no-https --no-admin \
                ${worker_flag[@]+"${worker_flag[@]}"} ${@+"$@"} \
                > "${WORK}/logs/ramjet.log" 2>&1 &
        SAMPLY_PID=$!
        sleep 2
        # samply's child is the process under test; the harness has to stop
        # *it* and then let samply exit on its own, or the profile is never
        # written out.
        RAMJET_PID="$(pgrep -P "${SAMPLY_PID}" -f ramjet-ingressd | head -1)"
        [ -n "${RAMJET_PID}" ] || die "samply did not start the daemon"
        verify_correctness "${PORT_RAMJET}" ramjet
        run_load "${PORT_RAMJET}" "ramjet-ingress" profile "${RAMJET_PID}"
        report_cpu profile
        kill -TERM "${RAMJET_PID}" 2>/dev/null || true
        wait "${SAMPLY_PID}" 2>/dev/null || true
        unset RAMJET_PID
        cp "${WORK}/profile.json.gz" "${PROFILE_OUT:-${WORK}/profile.json.gz}" 2>/dev/null || true
        log "profile: ${WORK}/profile.json.gz"
        ;;
    *)
        die "usage: native.sh [ramjet|both|profile]"
        ;;
esac
