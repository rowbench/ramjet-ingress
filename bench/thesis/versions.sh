#!/usr/bin/env bash
#
# Everything a reader needs to know what was actually measured. Written to
# results/versions.txt and quoted verbatim in RESULTS.md.

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"

mkdir -p "$RESULTS_DIR"

{
    echo "date:            $(date -u '+%Y-%m-%dT%H:%M:%SZ')"
    echo "host:            $(uname -srm), $(sysctl -n hw.ncpu 2>/dev/null) host CPUs"
    echo "docker:          $(docker version --format '{{.Server.Version}}')"
    echo "docker VM:       $(docker exec "$NODE_CONTAINER" nproc) CPUs, \
$(docker exec "$NODE_CONTAINER" awk '/MemTotal/{printf "%.1f GiB", $2/1048576}' /proc/meminfo), \
kernel $(docker exec "$NODE_CONTAINER" uname -r)"
    echo "kubernetes:      $(K version -o json 2>/dev/null | python3 -c 'import json,sys;d=json.load(sys.stdin);print(d["serverVersion"]["gitVersion"])')"
    echo "node runtime:    $(K get node -o jsonpath='{.items[0].status.nodeInfo.containerRuntimeVersion}')"
    echo "helm:            $(helm version --template '{{.Version}}')"
    echo
    echo "ramjet-ingress:  $(docker run --rm "$RAMJET_IMAGE" --version 2>&1)"
    echo "  image:         $RAMJET_IMAGE ($(docker image inspect "$RAMJET_IMAGE" --format '{{.Id}}'))"
    # The image tag is the commit the image was built from, and that is what has
    # to be reported. The working tree moved underneath this benchmark twice
    # while it ran — another agent is developing in the same checkout — so
    # today's HEAD is not what was measured, and printing it would be a lie in
    # the one place a reader most needs the truth.
    BUILT_FROM="${RAMJET_IMAGE##*:}"
    echo "  built from:    $(git -C "$REPO_DIR" rev-parse "$BUILT_FROM") ($(git -C "$REPO_DIR" log -1 --format=%s "$BUILT_FROM"))"
    echo "  repo HEAD now: $(git -C "$REPO_DIR" rev-parse --short HEAD) ($(git -C "$REPO_DIR" log -1 --format=%s)) — moved during the run, not measured"
    echo "  chart:         deploy/chart/ramjet-ingress $(python3 -c '
import sys, re
t = open(sys.argv[1]).read()
print("version " + re.search(r"^version:\s*(\S+)", t, re.M).group(1))' "$REPO_DIR/deploy/chart/ramjet-ingress/Chart.yaml")"
    echo "  flags:         $(K -n "$NS_RAMJET" get deploy "${PREFIX}-ramjet" -o jsonpath='{range .spec.template.spec.containers[0].args[*]}{@}{" "}{end}')"
    echo "  resources:     $(K -n "$NS_RAMJET" get deploy "${PREFIX}-ramjet" -o jsonpath='{.spec.template.spec.containers[0].resources}')"
    echo
    echo "ingress-nginx:   chart $NGINX_CHART_VERSION"
    echo "  image:         $(K -n "$NS_NGINX" get deploy "${PREFIX}-nginx-controller" -o jsonpath='{.spec.template.spec.containers[0].image}')"
    echo "  version:       $(K -n "$NS_NGINX" exec "deploy/${PREFIX}-nginx-controller" -- /nginx-ingress-controller --version 2>/dev/null | tr -d '\r' | awk '/Release:|nginx version:/{$1=$1; print}' | paste -sd', ' -)"
    echo "  flags:         $(K -n "$NS_NGINX" get deploy "${PREFIX}-nginx-controller" -o jsonpath='{range .spec.template.spec.containers[0].args[*]}{@}{" "}{end}')"
    echo "  resources:     $(K -n "$NS_NGINX" get deploy "${PREFIX}-nginx-controller" -o jsonpath='{.spec.template.spec.containers[0].resources}')"
    echo "  configmap:     $(K -n "$NS_NGINX" get cm "${PREFIX}-nginx-controller" -o jsonpath='{.data}')"
    echo "  webhook:       $(K get validatingwebhookconfiguration -o name 2>/dev/null | grep -c "$PREFIX") admission webhook(s) registered"
    echo
    echo "backends:        3 x nginx:1-alpine, 2 replicas each, 128-byte body from memory"
    echo "  image:         $(docker run --rm --entrypoint nginx nginx:1-alpine -v 2>&1)"
    echo "load generator:  $OHA_IMAGE -> $(docker run --rm "$OHA_IMAGE" --version 2>&1)"
    echo "probe image:     $PROBE_IMAGE (python $(docker run --rm --entrypoint python3 "$PROBE_IMAGE" --version 2>&1 | awk '{print $2}'), kubectl $(docker run --rm --entrypoint kubectl "$PROBE_IMAGE" version --client=true -o json 2>/dev/null | python3 -c 'import json,sys;print(json.load(sys.stdin)["clientVersion"]["gitVersion"])'))"
    echo "load path:       docker network $KIND_NET -> $(node_ip):NodePort (ramjet $RAMJET_NODEPORT_HTTP, nginx $NGINX_NODEPORT_HTTP)"
} | tee "$RESULTS_DIR/versions.txt"
