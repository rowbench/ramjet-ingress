#!/usr/bin/env python3
"""Back-to-back A/B of two ramjet-ingressd images, on run.sh's topology.

run.sh measures one binary against nginx in one session. That is the right
shape for the committed table and the wrong shape for the question "did this
commit cost throughput", because the answer would be a difference between two
sessions and would carry whatever the host was doing in each. This alternates
two images inside one session instead, rotating which goes first, so whatever
the machine is doing lands on both arms.

    docker build -f Dockerfile -t ramjet:before .      # at the old commit
    docker build -f Dockerfile -t ramjet:after  .      # at the new one
    IMAGES="ramjet:before ramjet:after" ROUNDS=3 CONC=64 python3 bench/ab.py

Defaults match run.sh's protocol: c64, a discarded 10s warmup, 30s measured,
15s cooldown, load generator pinned to four cores and the proxy to two.

This writes nothing. run.sh owns the committed numbers; this answers a
narrower question and prints its answer.
"""

import json
import os
import statistics
import subprocess
import sys
import time

WORK = os.environ.get("AB_WORK", "/tmp/ramjet-ab")
NET = "ramjet-ab-net"
SUBNET = "172.33.60.0/24"
IP_UP1, IP_UP2, IP_PROXY = "172.33.60.11", "172.33.60.12", "172.33.60.21"
UP1, UP2, PROXY = "ramjet-ab-up1", "ramjet-ab-up2", "ramjet-ab-proxy"
BENCH = os.path.dirname(os.path.abspath(__file__))


def sh(*args):
    return subprocess.run(args, capture_output=True, text=True)


def cleanup():
    sh("docker", "rm", "-f", UP1, UP2, PROXY)
    sh("docker", "network", "rm", NET)


def start_upstreams():
    sh("docker", "network", "create", "--driver", "bridge", "--subnet", SUBNET, NET)
    for name, ip, cpu in ((UP1, IP_UP1, "2"), (UP2, IP_UP2, "3")):
        sh("docker", "run", "-d", "--name", name, "--network", NET, "--ip", ip,
           f"--cpuset-cpus={cpu}",
           "-v", f"{BENCH}/upstream.conf:/etc/nginx/nginx.conf:ro",
           "nginx:1-alpine")


def start_proxy(image):
    sh("docker", "rm", "-f", PROXY)
    with open(os.path.join(WORK, "ab-routes.yaml"), "w") as fh:
        fh.write("backends:\n  - name: bench\n    policy: roundRobin\n"
                 f"    endpoints:\n      - {IP_UP1}:8080\n      - {IP_UP2}:8080\n"
                 "routes:\n  - host: bench.test\n    path: /\n"
                 "    pathType: Prefix\n    backend: bench\n")
    r = sh("docker", "run", "-d", "--name", PROXY, "--network", NET, "--ip", IP_PROXY,
           "--cpuset-cpus=0,1",
           "-v", f"{WORK}/ab-routes.yaml:/etc/ramjet/routes.yaml:ro",
           image, "--static-routes", "/etc/ramjet/routes.yaml", "--no-https")
    if r.returncode != 0:
        raise SystemExit(r.stderr)
    time.sleep(4)


def oha(conc, dur):
    r = sh("docker", "run", "--rm", "--network", NET, "--cpuset-cpus=4,5,6,7",
           "ghcr.io/hatoo/oha:latest", "--no-tui", "--output-format", "json",
           "-c", str(conc), "-z", dur, "-w", "--worker-threads", "4",
           "--host", "bench.test", f"http://{IP_PROXY}:8080/")
    return json.loads(r.stdout)


def main():
    images = os.environ.get("IMAGES", "").split()
    rounds = int(os.environ.get("ROUNDS", "3"))
    conc = int(os.environ.get("CONC", "64"))
    dur = os.environ.get("DURATION", "30s")
    warmup = os.environ.get("WARMUP", "10s")
    cooldown = int(os.environ.get("COOLDOWN", "15"))
    if len(images) < 2:
        raise SystemExit("set IMAGES to two or more image tags")

    os.makedirs(WORK, exist_ok=True)
    cleanup()
    start_upstreams()
    time.sleep(3)

    results = {i: [] for i in images}
    lat = {i: [] for i in images}
    try:
        for r in range(1, rounds + 1):
            # Rotate which image goes first, so neither owns the warm cache.
            order = images[(r - 1) % len(images):] + images[:(r - 1) % len(images)]
            for image in order:
                start_proxy(image)
                oha(conc, warmup)
                time.sleep(2)
                out = oha(conc, dur)
                rps = out["summary"]["requestsPerSec"]
                p = out["latencyPercentiles"]
                results[image].append(rps)
                lat[image].append((p["p50"], p["p99"], p["p99.9"]))
                print(f"  round {r}  {image:<28} c{conc}  {rps:10,.0f} rps  "
                      f"p50 {p['p50']*1e6:6.0f} us  p99 {p['p99']*1e6:7.0f} us  "
                      f"p99.9 {p['p99.9']*1e6:8.0f} us", flush=True)
                time.sleep(cooldown)
    finally:
        cleanup()

    print()
    for image in images:
        xs = results[image]
        med = statistics.median(xs)
        spread = (max(xs) - min(xs)) / med * 100 if med else 0
        p50 = statistics.median(x[0] for x in lat[image]) * 1e6
        p99 = statistics.median(x[1] for x in lat[image]) * 1e6
        p999 = statistics.median(x[2] for x in lat[image]) * 1e6
        print(f"{image:<28} median {med:10,.0f} rps  spread {spread:4.1f}%  "
              f"p50 {p50:6.0f} us  p99 {p99:7.0f} us  p99.9 {p999:8.0f} us")


if __name__ == "__main__":
    main()
