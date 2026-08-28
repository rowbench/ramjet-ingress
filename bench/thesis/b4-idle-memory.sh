#!/usr/bin/env bash
#
# Benchmark 4 — what 10,000 idle keep-alive connections cost.
#
# No Kubernetes here. ingress-nginx's data plane *is* nginx, so the question
# "what does a connection cost the proxy" is answered by putting ramjet-ingressd
# and plain nginx side by side on the same docker bridge with the same upstream,
# exactly as bench/run.sh does — and the tuning below is bench/nginx.conf and
# bench/upstream.conf with only the addresses rewritten, so nginx arrives with
# every advantage that benchmark already gave it.
#
# What is measured is the container's cgroup memory working set, sampled with
# `docker stats`, which is what bench/RESULTS.md reports as memory for both
# contenders. It is not literally VmRSS: it is RSS plus whatever page cache the
# cgroup is charged for, minus inactive file pages. Neither proxy touches a file
# on the request path, so the difference is small — but "RSS" would be the wrong
# word for it and the delta across phases is the number that matters anyway.

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"

# Overridable so a re-run against a fixed image can be recorded beside the
# original rather than on top of it. The first measurement is the reason the
# fix exists, and a benchmark that overwrites the evidence it was judged
# against cannot be checked afterwards.
OUT="${B4_OUT:-$RESULTS_DIR/b4}"
NET="${PREFIX}-b4-net"
SUBNET="172.31.97.0/24"
IP_UP="172.31.97.11"
IP_RAMJET="172.31.97.21"
IP_NGINX="172.31.97.22"
CONNS="${CONNS:-10000}"
NGINX_IMAGE="nginx:1-alpine"
WORK="$PROBE_WORK/b4"

mkdir -p "$OUT" "$WORK"

cleanup() {
    docker rm -f "${PREFIX}-b4-up" "${PREFIX}-b4-ramjet" "${PREFIX}-b4-nginx" \
                 "${PREFIX}-b4-hold" >/dev/null 2>&1 || true
    docker network rm "$NET" >/dev/null 2>&1 || true
    rm -f "$WORK/fifo"
}
trap cleanup EXIT INT TERM

mem_of() {
    # MemUsage is "used / limit"; take the used half and normalise to bytes.
    docker stats --no-stream --format '{{.MemUsage}}' "$1" 2>/dev/null \
        | awk '{print $1}' \
        | python3 -c '
import re, sys
v = sys.stdin.read().strip()
m = re.match(r"([\d.]+)\s*([KMGi]*B)", v, re.I)
if not m:
    print(0); raise SystemExit
n, unit = float(m.group(1)), m.group(2).lower()
mult = {"b": 1, "kib": 1024, "mib": 1024**2, "gib": 1024**3,
        "kb": 1000, "mb": 1000**2, "gb": 1000**3}
print(int(n * mult.get(unit, 1)))
'
}

# ---------------------------------------------------------------------------
# Topology. bench/'s configs with the addresses rewritten for this subnet, so
# there is one source of truth for how nginx is tuned.
# ---------------------------------------------------------------------------

log "Benchmark 4: $CONNS idle keep-alive connections (docker only, no Kubernetes)"
preflight
wait_for_quiet || true

cleanup
docker network create --driver bridge --subnet "$SUBNET" "$NET" >/dev/null

sed -e "s#172\.31\.99\.11#$IP_UP#g" -e "s#172\.31\.99\.12#$IP_UP#g" \
    "$THESIS_DIR/../nginx.conf" > "$WORK/nginx.conf"
cp "$THESIS_DIR/../upstream.conf" "$WORK/upstream.conf"

# One upstream rather than bench/'s two: there is no load here, so a second
# upstream would only be a second idle container. Both proxies point at the
# same single address, so it stays like-for-like.
python3 - "$THESIS_DIR/../ramjet-routes.yaml" "$IP_UP" "$WORK/routes.yaml" <<'PY'
import re, sys
src, ip, dst = sys.argv[1:4]
text = open(src).read()
text = re.sub(r"- 172\.31\.99\.\d+:8080\n", "", text)
text = text.replace("endpoints:", f"endpoints:\n      - {ip}:8080")
open(dst, "w").write(text)
PY

log "starting upstream and both proxies"
docker run -d --name "${PREFIX}-b4-up" --network "$NET" --ip "$IP_UP" \
    --cpuset-cpus=2 \
    -v "$WORK/upstream.conf:/etc/nginx/nginx.conf:ro" \
    "$NGINX_IMAGE" >/dev/null

# nofile is raised on both proxies and set to the same value. Without it the
# default 1024 makes the 10,000th connection a measurement of ulimit.
docker run -d --name "${PREFIX}-b4-ramjet" --network "$NET" --ip "$IP_RAMJET" \
    --cpuset-cpus=0,1 --ulimit nofile=65536:65536 \
    -v "$WORK/routes.yaml:/etc/ramjet/routes.yaml:ro" \
    "$RAMJET_IMAGE" --static-routes /etc/ramjet/routes.yaml --no-https >/dev/null

docker run -d --name "${PREFIX}-b4-nginx" --network "$NET" --ip "$IP_NGINX" \
    --cpuset-cpus=0,1 --ulimit nofile=65536:65536 \
    -v "$WORK/nginx.conf:/etc/nginx/nginx.conf:ro" \
    "$NGINX_IMAGE" >/dev/null

sleep 5

for pair in "ramjet $IP_RAMJET" "nginx $IP_NGINX"; do
    set -- $pair
    code="$(docker run --rm --network "$NET" curlimages/curl:latest \
                -s -o /dev/null -w '%{http_code}' -m 5 -H 'Host: bench.test' "http://$2:8080/" 2>/dev/null || true)"
    [[ "$code" == "200" ]] || die "$1 answered $code before the benchmark started"
done
sub "both proxies serve 200 through the shared upstream"

# ---------------------------------------------------------------------------

hold_run() {
    local who="$1" ip="$2" container="${PREFIX}-b4-$1"
    local fifo="$WORK/fifo"
    rm -f "$fifo"; mkfifo "$fifo"

    local before; before="$(mem_of "$container")"

    docker rm -f "${PREFIX}-b4-hold" >/dev/null 2>&1 || true
    ( docker run -i --rm --name "${PREFIX}-b4-hold" --network "$NET" \
        --cpuset-cpus=4,5,6,7 --ulimit nofile=65536:65536 \
        -v "$THESIS_DIR/hold.py:/hold.py:ro" \
        --entrypoint python3 "$PROBE_IMAGE" /hold.py "$ip:8080" "$CONNS" bench.test \
        >"$WORK/$who-hold.log" 2>"$WORK/$who-hold.err" ) <"$fifo" &
    local holder=$!
    exec 9>"$fifo"

    local opened="" failed="" secs="" line
    for _ in $(seq 1 240); do
        line="$(grep -m1 -E '^(OPEN|ABORT) ' "$WORK/$who-hold.log" 2>/dev/null || true)"
        [[ -n "$line" ]] && break
        sleep 1
    done
    [[ -n "$line" ]] || { exec 9>&-; die "$who: holder never reported"; }
    read -r _ opened failed secs <<<"$line"

    sleep 3
    local at_peak; at_peak="$(mem_of "$container")"

    echo "" >&9
    for _ in $(seq 1 60); do
        grep -q '^CLOSED ' "$WORK/$who-hold.log" 2>/dev/null && break
        sleep 1
    done
    sleep 8
    local after; after="$(mem_of "$container")"
    echo "" >&9
    exec 9>&-
    wait "$holder" 2>/dev/null || true

    python3 - "$OUT/$who.json" "$who" "$opened" "${failed:-0}" "${secs:-0}" "$before" "$at_peak" "$after" "$CONNS" <<'PY'
import json, sys
path, who, opened, failed, secs, before, peak, after, want = sys.argv[1:10]
opened, before, peak, after, want = int(opened), int(before), int(peak), int(after), int(want)
d = {
    "contender": who,
    "requested": want,
    "established": opened,
    "failed": int(failed),
    "open_seconds": float(secs),
    "mem_before_bytes": before,
    "mem_at_peak_bytes": peak,
    "mem_after_close_bytes": after,
    "bytes_per_connection": round((peak - before) / opened, 1) if opened else None,
    "retained_after_close_bytes": after - before,
}
json.dump(d, open(path, "w"))
print(f"    {who:<7} {opened:>6}/{want} established in {float(secs):.0f}s  "
      f"{before/1048576:>6.1f} -> {peak/1048576:>6.1f} -> {after/1048576:>6.1f} MiB   "
      f"{d['bytes_per_connection']:>8} B/conn")
PY
}

log "holding connections (interleaved is not possible: the holder is one process)"
hold_run ramjet "$IP_RAMJET"
sleep 10
hold_run nginx "$IP_NGINX"

# A second pass in the reverse order, so whichever proxy went first does not
# permanently own whatever the first run warms up.
log "second pass, reversed order"
mv "$OUT/ramjet.json" "$OUT/ramjet-pass1.json"
mv "$OUT/nginx.json"  "$OUT/nginx-pass1.json"
hold_run nginx "$IP_NGINX"
sleep 10
hold_run ramjet "$IP_RAMJET"
mv "$OUT/ramjet.json" "$OUT/ramjet-pass2.json"
mv "$OUT/nginx.json"  "$OUT/nginx-pass2.json"

{
    echo "ramjet: $(docker run --rm "$RAMJET_IMAGE" --version 2>&1)"
    echo "nginx:  $(docker run --rm --entrypoint nginx "$NGINX_IMAGE" -v 2>&1)"
} > "$OUT/versions.txt"

log "Benchmark 4 complete — raw output in $OUT"
