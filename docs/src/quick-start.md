# Quick start

Two paths. The first needs no cluster and takes about a minute; the second is
the real thing.

## Without a cluster, in 60 seconds

`--static-routes` swaps the API server for a YAML file and changes nothing else
about the serving path, which makes it the fastest way to look at the data plane
on its own.

Start two throwaway upstreams that say which one they are:

```sh
for u in a b; do
    mkdir -p /tmp/ramjet-$u/api /tmp/ramjet-$u/healthz
    echo "upstream-$u /"        > /tmp/ramjet-$u/index.html
    echo "upstream-$u /api"     > /tmp/ramjet-$u/api/index.html
    echo "upstream-$u /healthz" > /tmp/ramjet-$u/healthz/index.html
done
(cd /tmp/ramjet-a && python3 -m http.server 9001) &
(cd /tmp/ramjet-b && python3 -m http.server 9002) &
```

Then run the daemon against the example route table shipped in the repository:

```sh
cargo run -p ramjet-ingressd -- \
    --static-routes crates/ramjet-ingressd/examples/dev-routes.yaml
```

```console
ramjet-ingressd 0.1.0 — 5 backend(s), 6 endpoint(s), 5 route(s), 0 certificate(s), default backend set
  config   crates/ramjet-ingressd/examples/dev-routes.yaml
  http     0.0.0.0:8080
  https    disabled
  http3    disabled
  admin    0.0.0.0:10254
  probes   http://0.0.0.0:10254/healthz  http://0.0.0.0:10254/readyz  http://0.0.0.0:10254/metrics
  admin    http://0.0.0.0:10254/admin/generations  http://0.0.0.0:10254/admin/routes
 INFO audit: 5 routes added, 3 hosts added, 1 mirror added, default backend now fallback (gen 0→0)
```

`https` is disabled because this file declares no certificates and no explicit
`--https` was given. The `audit` line is the same record every publish gets in
Kubernetes mode — dev mode has exactly one generation and nothing is
special-cased for it.

And drive it:

```sh
curl -H 'Host: shop.example.com' http://127.0.0.1:8080/          # upstream-a
curl -H 'Host: shop.example.com' http://127.0.0.1:8080/api/      # leastConn
curl -H 'Host: sub.example.com'  http://127.0.0.1:8080/          # wildcard
curl -H 'Host: anything.else'    http://127.0.0.1:8080/          # default backend

# `always` forces the canary regardless of the weight; `never` keeps it away.
curl -H 'Host: shop.example.com' -H 'x-canary: always' \
     http://127.0.0.1:8080/api/                                  # upstream-b

curl http://127.0.0.1:10254/readyz
curl http://127.0.0.1:10254/metrics
```

`Ctrl-C` (or `SIGTERM`) drains in-flight requests and exits.

Listeners default to `:8080` plaintext, `:8443` TLS, and `:10254` admin. In dev
mode, without an explicit `--https` or `--no-https`, the TLS listener is skipped
when the configuration declares no certificates — a listener that fails every
handshake is not a useful default.

### The route file

`crates/ramjet-ingressd/examples/dev-routes.yaml` is annotated end to end. The
shape:

```yaml
# Answers any request that matches no rule at all. Without this, those are 404s.
defaultBackend: fallback

backends:
  - name: web
    policy: roundRobin           # roundRobin | leastConn | random
    endpoints:
      - 127.0.0.1:9001           # short form means weight 1

  - name: api
    policy: leastConn
    endpoints:
      - 127.0.0.1:9001
      - address: 127.0.0.1:9002  # long form, for a bigger pod
        weight: 2                # or for draining one with `weight: 0`

routes:
  - host: shop.example.com
    path: /healthz
    pathType: Exact              # Exact | Prefix | ImplementationSpecific
    backend: web

  - host: shop.example.com
    path: /api
    pathType: Prefix
    backend: api
    canary:
      backend: api-next
      weight: 20
      header: x-canary

  - host: "*.example.com"        # replaces exactly one label
    path: /
    pathType: Prefix
    backend: web

  - path: /status                # no host: every name not claimed above
    pathType: Prefix
    backend: web

# tls:
#   - host: shop.example.com
#     cert: /tmp/dev-cert.pem
#     key: /tmp/dev-key.pem
```

This is **not a production configuration format**, and nothing else in the tree
parses YAML. The Kubernetes path builds tables from API objects directly; it
does not render configuration and read it back, which is exactly the round trip
that makes ingress-nginx's behaviour hard to predict from its inputs.

The two modes are mutually exclusive by nature — a file and an API server are
two writers for one route table, and letting both write would make the winner a
race.

## On a cluster, with Helm

```sh
helm install ramjet deploy/chart/ramjet-ingress \
  --namespace ramjet-system --create-namespace
```

That is a `hostNetwork` DaemonSet serving :80 and :443 on every node — the shape
that works on a cluster with nothing underneath it. It also installs a
ServiceAccount, a ClusterRole and binding, a ClusterIP Service, a separate
ClusterIP Service for the admin port, and an `IngressClass` named `ramjet` whose
controller is `ramjet.dev/ingress`. On a cloud, add the preset for your provider
(see [Deployment](./deployment.md)), which goes back to a Deployment on
8080/8443 behind a `LoadBalancer` Service.

Point workloads at it with `ingressClassName: ramjet`:

```yaml
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata:
  name: shop
spec:
  ingressClassName: ramjet
  rules:
    - host: shop.example.com
      http:
        paths:
          - path: /api
            pathType: Prefix
            backend:
              service:
                name: api
                port:
                  number: 80
```

Or set `ingressClass.isDefaultClass=true` to catch Ingresses that name no class
at all.

**[Deployment](./deployment.md) has a values preset and a rendered, Helm-free
manifest for each supported provider**, and answers the question that decides
most of that configuration: where the client's IP address comes from.

### Before you scale it

`replicas: 1` is hard-coded in the chart, and there is no values entry to find
at 3am. There is no leader election yet: status writeback reads a Service's
address and server-side-applies it to every managed Ingress, and a second
replica would do the same work against the same objects with the same field
manager on its own schedule. Scale by making the one replica bigger, or use
`--no-status-update` if you must run more. See [Limitations](./limitations.md).

### Readiness, before you debug a slow rollout

`/readyz` returns 503 until a route table has actually been compiled from the
API server. A replica that has finished starting but not finished its first list
is deliberately kept out of the Service, because an empty table would 404
everything sent to it. `/healthz`, which the liveness probe uses, answers as
soon as the process does.

## Watching it live

The admin port reports counters, and the question you usually have is about
rates. `ramjet-top` polls `/admin/routes`, `/admin/generations` and `/metrics`,
differences the counters, and draws them.

```sh
cargo run -p ramjet-top                 # the local admin port
ramjet-top 10.0.0.5:10254               # somewhere else
ramjet-top --once                       # one aligned table, for scripts and CI
```

See [Observability](./operations/observability.md) for the keybindings and what
the numbers mean.
