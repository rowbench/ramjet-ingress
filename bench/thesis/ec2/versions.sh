#!/usr/bin/env bash
# Everything that has to be true for a table on this page to mean anything.
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"

echo "date:            $(date -u +%Y-%m-%dT%H:%M:%SZ)"
echo "instance:        $(curl -s -m 2 -H "X-aws-ec2-metadata-token: $(curl -s -m 2 -X PUT http://169.254.169.254/latest/api/token -H 'X-aws-ec2-metadata-token-ttl-seconds: 60')" http://169.254.169.254/latest/meta-data/instance-type 2>/dev/null || echo '?') $(nproc) vCPU, $(awk '/MemTotal/{printf "%.1f GiB", $2/1048576}' /proc/meminfo)"
echo "cpu:             $(awk -F': ' '/model name/{print $2; exit}' /proc/cpuinfo)"
echo "host:            $(uname -srm), $(. /etc/os-release && echo "$PRETTY_NAME")"
echo "kubernetes:      $(K version -o json 2>/dev/null | jq -r '.serverVersion.gitVersion')"
echo "node runtime:    $(K get node -o jsonpath='{.items[0].status.nodeInfo.containerRuntimeVersion}')"
echo "kube-proxy mode: $(K -n kube-system get cm kube-proxy-config -o jsonpath='{.data}' 2>/dev/null | head -c 120 || echo '(k0s default)')"
echo "helm:            $(H version --short)"
echo "oha:             $(oha --version)"
echo "python:          $(python3 -V)"
echo
echo "ramjet-ingress:  $(K -n "$NS_HYPER" logs deploy/${PREFIX}-hyper 2>/dev/null | sed 's/\x1b\[[0-9;]*m//g' | grep -oE 'version="[^"]+"' | head -1)"
echo "  image:         ${RAMJET_IMAGE_REPO}:${RAMJET_IMAGE_TAG}"
echo "  image id:      $(K -n "$NS_HYPER" get pod "$(pod_for hyper)" -o jsonpath='{.status.containerStatuses[0].imageID}')"
echo "  chart:         ramjet-ingress $(awk '/^version:/{print $2}' "$CHART_DIR/Chart.yaml") (appVersion $(awk '/^appVersion:/{print $2}' "$CHART_DIR/Chart.yaml"))"
echo "  hyper flags:   $(K -n "$NS_HYPER" get deploy ${PREFIX}-hyper -o jsonpath='{.spec.template.spec.containers[0].args}')"
echo "  uring flags:   $(K -n "$NS_URING" get deploy ${PREFIX}-uring -o jsonpath='{.spec.template.spec.containers[0].args}')"
echo "  hyper seccomp: $(K -n "$NS_HYPER" get deploy ${PREFIX}-hyper -o jsonpath='{.spec.template.spec.securityContext.seccompProfile.type}')"
echo "  uring seccomp: $(K -n "$NS_URING" get deploy ${PREFIX}-uring -o jsonpath='{.spec.template.spec.securityContext.seccompProfile.type}')"
echo "  resources:     $(K -n "$NS_HYPER" get deploy ${PREFIX}-hyper -o jsonpath='{.spec.template.spec.containers[0].resources}')"
echo "  engine lines:  hyper $(engine_line hyper) | uring $(engine_line uring)"
echo
echo "ingress-nginx:   chart $(H -n "$NS_NGINX" list -o json | jq -r '.[0].chart'), app $(H -n "$NS_NGINX" list -o json | jq -r '.[0].app_version')"
echo "  image id:      $(K -n "$NS_NGINX" get pod "$(pod_for nginx)" -o jsonpath='{.status.containerStatuses[0].imageID}')"
echo "  resources:     $(K -n "$NS_NGINX" get deploy ${PREFIX}-nginx-controller -o jsonpath='{.spec.template.spec.containers[0].resources}')"
echo "  webhook:       $(K get validatingwebhookconfiguration -o name | grep -c "$PREFIX") admission webhook(s) registered"
echo "  non-default:   disable-access-log=true, progressDeadlineSeconds=600, NodePort service, unique class/name"
echo
echo "backends:        $(K -n "$NS_APP" get deploy echo-a -o jsonpath='{.spec.template.spec.containers[0].image}'), 2 replicas x 3 deployments"
