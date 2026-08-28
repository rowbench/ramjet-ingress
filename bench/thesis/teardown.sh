#!/usr/bin/env bash
#
# Remove everything setup.sh created, and verify it is gone.
#
# Deleting the three namespaces is not sufficient: IngressClass and
# ValidatingWebhookConfiguration are cluster-scoped, and a leftover
# ingress-nginx admission webhook whose backing Service no longer exists makes
# *every* Ingress write in the cluster fail. That is the failure this script
# exists to prevent, so the cluster-scoped sweep runs even if the helm
# uninstalls fail.

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"

preflight

log "Uninstalling releases"
H uninstall "$RAMJET_RELEASE" --namespace "$NS_RAMJET" --wait --timeout 2m >/dev/null 2>&1 || true
H uninstall "$NGINX_RELEASE"  --namespace "$NS_NGINX"  --wait --timeout 3m >/dev/null 2>&1 || true

log "Deleting namespaces"
K delete namespace "$NS_APP" "$NS_RAMJET" "$NS_NGINX" --ignore-not-found --wait=false >/dev/null 2>&1 || true

log "Sweeping cluster-scoped objects"
K delete ingressclass "$RAMJET_CLASS" "$NGINX_CLASS" --ignore-not-found >/dev/null 2>&1 || true
for kind in clusterrole clusterrolebinding validatingwebhookconfiguration; do
    names="$(K get "$kind" -o name 2>/dev/null | grep -- "$PREFIX" || true)"
    [[ -n "$names" ]] && xargs -r <<<"$names" kubectl --context "$CONTEXT" delete --ignore-not-found >/dev/null 2>&1
done

log "Removing benchmark containers"
docker ps -aq --filter "name=^${PREFIX}-" | xargs -r docker rm -f >/dev/null 2>&1 || true
docker network ls -q --filter "name=^${PREFIX}-" | xargs -r docker network rm >/dev/null 2>&1 || true

# ---------------------------------------------------------------------------
# Verify, rather than assume. Namespace deletion is asynchronous and a stuck
# finalizer is silent.
# ---------------------------------------------------------------------------

log "Verifying"
for _ in $(seq 1 150); do
    remaining="$(K get ns -o name 2>/dev/null | grep -c -- "$PREFIX" || true)"
    [[ "$remaining" == "0" ]] && break
    sleep 2
done

fail=0
remaining="$(K get ns -o name 2>/dev/null | grep -- "$PREFIX" || true)"
if [[ -n "$remaining" ]]; then
    # A namespace that is still Terminating is not the same problem as one that
    # is wedged, and the difference is the whole reason to check: Terminating
    # with every condition False means the API server has already removed the
    # content and is finishing up, while a True condition means something —
    # usually an admission webhook whose backing Service is gone — is blocking
    # it and needs a human.
    for ns in $remaining; do
        blocked="$(K get "$ns" -o json 2>/dev/null \
            | python3 -c 'import json,sys;print(",".join(c["type"] for c in json.load(sys.stdin).get("status",{}).get("conditions",[]) if c["status"]=="True") or "none")')"
        if [[ "$blocked" == "none" ]]; then
            warn "$ns is still Terminating with no blocking condition; it will finish on its own"
        else
            warn "$ns is stuck on: $blocked"
            fail=1
        fi
    done
fi

for kind in ingressclass clusterrole clusterrolebinding validatingwebhookconfiguration; do
    leftover="$(K get "$kind" -o name 2>/dev/null | grep -- "$PREFIX" || true)"
    if [[ -n "$leftover" ]]; then warn "$kind left behind: $leftover"; fail=1; fi
done

leftover="$(docker ps -aq --filter "name=^${PREFIX}-" || true)"
if [[ -n "$leftover" ]]; then warn "containers left behind: $leftover"; fail=1; fi

if (( fail == 0 )); then
    sub "no ${PREFIX}-* namespaces, IngressClasses, ClusterRoles, webhooks or containers remain"
    log "Teardown verified"
else
    die "teardown incomplete — see warnings above"
fi
