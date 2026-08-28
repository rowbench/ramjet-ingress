# ramjet-ingress

[![docs](https://img.shields.io/badge/docs-rowbench.github.io-blue)](https://rowbench.github.io/ramjet-ingress/)
[![license](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-green)](#license)

A Kubernetes ingress controller with a native Rust data plane. No nginx, no
config file regeneration, no reload: a configuration change swaps an `Arc`, and
in-flight connections never notice.

**📖 [Documentation](https://rowbench.github.io/ramjet-ingress/)** — quick start,
deployment per cloud, the full annotation and flag references, operations
guides, and the benchmarks with their caveats.

The design and its reasoning are in [ARCHITECTURE.md](ARCHITECTURE.md). This
file is about running it.

| Crate | What it is |
|---|---|
| `ramjet-router` | Route table, host/path matcher, load balancing, canary, mirroring |
| `ramjet-proxy` | Listeners, TLS termination, HTTP/1.1 and HTTP/2, HTTP/3, upstreams |
| `ramjet-controller` | Kubernetes watches, translation, status writeback |
| `ramjet-engine` | An experimental second data plane on a completion-based reactor |
| `ramjet-ingressd` | The daemon that wires them together, and canary auto-promotion |
| `ramjet-top` | A terminal cockpit for a running instance ([README](crates/ramjet-top/README.md)) |

## Build and test

```sh
cargo build --release
cargo test --workspace
```

`ramjet-engine` depends on the `ramjet` runtime from a **sibling repository**
by path, so the workspace expects that checkout beside this one:

```
.../
  ramjet-ingress/     <- this repository
  enhance-socket/     <- the ramjet runtime and ramjet-http
```

Without it, `cargo` refuses to load the workspace at all rather than skipping
the crate. It is also why the container builds take the parent directory as
their build context.

## Two engines

The data plane is selected with `--engine`, and everything above it — routing,
load balancing, canaries, header rewriting, `/metrics` — is the same code
either way.

| | `--engine hyper` (default) | `--engine uring` |
|---|---|---|
| Runtime | hyper on tokio | the `ramjet` reactor: io_uring on Linux, kqueue elsewhere |
| HTTP/1.1 plaintext | yes | yes |
| TLS termination | yes | no (502) |
| HTTP/2, gRPC upstreams | h2 downstream | no (502) |
| WebSocket and upgrades | yes | no (502) |
| HTTP/3 over QUIC (`--http3`) | experimental, off by default | no; refused at startup |
| PROXY protocol (`--proxy-protocol`) | v1 and v2 | no; refused at startup |
| Kubernetes mode | yes | no; static routes only |
| Status | measured against nginx | experimental |

`uring` exists to answer one question. `bench/PROFILE.md` measured where a
request goes and found no hot function to fix: 59.4% of a request is the four
syscalls a proxy hop cannot avoid, another 9.1% is finding out a socket is
ready, and everything this project wrote is about 1%. Getting under that floor
is not a tuning exercise, it is an I/O model change — so there is now a second
data plane that submits those four operations into a ring and enters the kernel
once for a batch of them. The result is in
[bench/engine/RESULTS.md](bench/engine/RESULTS.md).

Everything it refuses, it refuses with a status code and an explanation naming
the other engine, and it prints the same list at startup. A gap that behaves
like a bug in whatever is on the other end is worse than a missing feature.

## HTTP/3

`--http3` serves HTTP/3 over QUIC on the `--https` port number in UDP, and
advertises it on every HTTPS response with `alt-svc: h3=":<port>"; ma=86400`.
It shares the TLS listener's certificates exactly — the same SNI resolution,
the same store, the same rotation — and a request that arrives over QUIC goes
through the same forwarding path as any other, so routing, canaries, retries
and per-route counters are the ones already in use.

It is experimental and off by default, and the two things to know before
turning it on are that the QUIC endpoint runs on **one** dedicated runtime
rather than one per core, and that the `alt-svc` advertisement only works where
the same port number is reachable over UDP — which most cloud load balancers
cannot do. [ARCHITECTURE.md](ARCHITECTURE.md#http3-experimental) has the first,
[deploy/README.md](deploy/README.md#http3-and-which-load-balancers-can-carry-it)
has the second. No 0-RTT, no QUIC upstream, no upgrades.

```console
$ ramjet-ingressd --static-routes routes.yaml --http3
ramjet-ingressd 0.1.0 — 1 backend(s), 1 endpoint(s), 1 route(s), 1 certificate(s)
  config   routes.yaml
  http     0.0.0.0:8080
  https    0.0.0.0:8443
  http3    0.0.0.0:8443
  admin    0.0.0.0:10254
```

## Running it without a cluster

`--static-routes` swaps the API server for a YAML file and changes nothing else
about the serving path, which makes it the fastest way to look at the data
plane on its own:

```sh
cargo run -p ramjet-ingressd -- --static-routes crates/ramjet-ingressd/examples/dev-routes.yaml
```

Listeners default to `:8080` plaintext, `:8443` TLS, and `:10254` admin
(`/metrics`, `/healthz`, `/readyz`, and a small JSON API: `/admin/generations`,
`/admin/routes`, `/admin/rollback`). `--help` lists every option; each one has
an environment twin, and a flag always beats the environment.

The admin API is where the anti-reload thesis pays out twice. Because publishing
a configuration is one pointer store, republishing an old one is the same
pointer store:

```sh
curl :10254/admin/generations                                   # what has been applied, and what changed
curl -XPOST :10254/admin/rollback -d '{"generation": 41}'       # put 41 back on the wire, now
curl -XDELETE :10254/admin/rollback                             # release, and jump to the newest
curl :10254/admin/routes                                        # per-route requests, 5xx, upstream latency
```

A rollback is an emergency brake and not desired state: it lives in one
replica's memory, the controller keeps compiling behind it, and it does not
survive a restart — after which Kubernetes is the source of truth again. See
[ARCHITECTURE.md](ARCHITECTURE.md#time-travel-and-the-audit-trail).

## Traffic mirroring

Send a second, fire-and-forget copy of a route's traffic to a shadow backend and
throw the answer away — a rewrite gets production traffic before it gets
production responsibility. Annotate the **production** Ingress:

```yaml
metadata:
  annotations:
    ramjet.dev/mirror-backend: shadow/api:80   # namespace optional
    ramjet.dev/mirror-percent: "10"            # default 100
    ramjet.dev/mirror-host: shadow.example.com # optional Host override
```

Copies carry `X-Mirrored-By: ramjet-ingress`. The hard promise is that a mirror
**cannot slow down or fail the request the client is waiting for**: nothing is
awaited on the request path, each serving runtime has a bounded queue that drops
on overflow, responses are drained and discarded, and a shadow backend that is
down or wedged produces a counter and nothing else. Bodies are the one real
cost — a request with one is buffered up to `--mirror-max-body` (256 KiB) so
both copies can have it, and a body over the cap is forwarded whole with the
mirror skipped. Watch `ramjet_mirrored_total`, `ramjet_mirror_dropped_total`,
`ramjet_mirror_skipped_total` and `ramjet_mirror_failures_total`.

## Canary auto-promotion

Let a healthy canary promote itself, and pull it back the moment it stops being
healthy. Annotate the **canary** Ingress; everything but the opt-in has a
default:

```yaml
metadata:
  annotations:
    nginx.ingress.kubernetes.io/canary: "true"
    nginx.ingress.kubernetes.io/canary-weight: "5"
    ramjet.dev/auto-promote: "true"
    # ramjet.dev/auto-promote-interval: 60s
    # ramjet.dev/auto-promote-steps: 5,10,25,50,100
    # ramjet.dev/auto-promote-max-5xx-percent: "1"
    # ramjet.dev/auto-promote-max-latency-factor: "1.5"
    # ramjet.dev/auto-promote-min-requests: "50"
```

Every interval the daemon takes that window's requests, 5xx and latency for the
canary and stable sides of the route *separately* — the router counts them
apart — and steps `canary-weight` to the next value. Too little traffic on
either side holds rather than advancing, because no traffic is not failure. A
breach of either threshold writes the weight to `0`, sets `auto-promote:
"false"`, and records `auto-promote-status: "rolled-back: <reason>"`; re-arming
is a human decision. Reaching the last step records `auto-promote-status:
promoted` and stops — swapping the production Ingress's backend stays a
deliberate human edit ([why](ARCHITECTURE.md#why-the-backend-swap-stays-human)).

Decisions land in the logs with their numbers, as Events on the IngressClass
(`CanaryStepped`, `CanaryPromoted`, `CanaryRolledBack`), and on
`--audit-webhook`. Everything pauses while a rollback pin is held. Needs
`networking.k8s.io`/`ingresses`/`patch`, which the chart grants.

## Watching it live

The admin port reports counters, and the question you usually have is about
rates. `ramjet-top` polls `/admin/routes`, `/admin/generations` and `/metrics`,
differences the counters, and draws them:

```sh
cargo run -p ramjet-top                 # the local admin port
ramjet-top 10.0.0.5:10254               # somewhere else
ramjet-top --once                       # one aligned table, for scripts and CI
```

Routes sortable by rate, error rate or latency; the generation timeline with
expandable diffs; a red banner whenever a pin is in effect; and the last good
data kept on screen, dimmed, when the daemon stops answering. Keybindings and
the reasoning behind the numbers are in
[crates/ramjet-top/README.md](crates/ramjet-top/README.md).

## The container image

Multi-stage: a full Rust toolchain compiles, and
`gcr.io/distroless/cc-debian12:nonroot` carries the result. The runtime has no
shell and no package manager, and the process runs as uid 65532. TLS is rustls
over ring, so the image needs no OpenSSL and no CA bundle.

The build context is the parent directory, for the reason given above — the
Dockerfile copies both this tree and the `enhance-socket` sibling, and a context
rooted here cannot see the second one:

```sh
docker build -f Dockerfile -t ramjet-ingress:0.1.0 ..
```

The builder uses BuildKit cache mounts for Cargo's registry and the target
directory rather than the usual "build dummy sources first" trick, which for a
four-crate workspace would mean maintaining four fabricated source files that
mirror the real layout. The tradeoff: the caches live in the builder, not in
the image, so the binary is copied out inside the same `RUN`.

## Deploying with Helm

```sh
helm install ramjet deploy/chart/ramjet-ingress --namespace ramjet-system --create-namespace
```

The chart installs a Deployment, a ServiceAccount, a ClusterRole and binding, a
LoadBalancer Service for traffic, a separate ClusterIP Service for the admin
port, and an `IngressClass` named `ramjet` whose controller is
`ramjet.dev/ingress`. Point workloads at it with `ingressClassName: ramjet`, or
set `ingressClass.isDefaultClass=true` to catch Ingresses that name no class.

**[deploy/README.md](deploy/README.md) is the deployment guide** — a values
preset and a rendered, Helm-free manifest for each of AWS (three shapes), GCP,
Azure, DigitalOcean, Scaleway, Oracle, Exoscale, and two bare-metal shapes:

```sh
helm install ramjet deploy/chart/ramjet-ingress \
  --namespace ramjet-ingress --create-namespace \
  -f deploy/provider/aws/values.yaml

kubectl apply -f deploy/static/provider/aws.yaml      # the same thing, no Helm
```

It also answers the question that decides most of that configuration: where the
client's IP address comes from on each provider, and what it costs to keep it.
Getting that wrong is quiet — `X-Forwarded-For` still gets written, it just
contains the load balancer's address.

Two things in the chart are deliberately not configurable:

**`replicas: 1` is hard-coded.** There is no leader election yet. Status
writeback reads a Service's address and server-side-applies it to every managed
Ingress, and a second replica would do the same work against the same objects
with the same field manager on its own schedule. The fix is leader election in
the controller, not a values entry — so there is no values entry to find at 3am.

**The admin port is on its own ClusterIP Service.** `/metrics` and the probes
are never attached to the internet-facing LoadBalancer, and because the split is
two objects rather than a list of ports, no value can accidentally publish them.

Readiness is worth understanding before you debug a slow rollout: `/readyz`
returns 503 until a route table has actually been compiled from the API server.
A replica that has finished starting but not finished its first list is
deliberately kept out of the Service, because an empty table would 404
everything sent to it. `/healthz`, which the liveness probe uses, answers as
soon as the process does.

`--publish-service` defaults to the chart's own LoadBalancer Service. That is
the mechanism by which an Ingress in an unrelated namespace ends up advertising
the address traffic really arrives on; set `controller.publishAddress` as a
fallback on clusters with no load balancer controller.

RBAC is exactly the six rules the controller's five watches and its status
writer need. It creates nothing, deletes nothing, and holds no write verb
outside the `ingresses/status` subresource.

## End-to-end proof

`deploy/e2e.sh` builds the image, installs the chart on a local Docker Desktop
cluster, deploys a pair of echo backends behind a production Ingress and a
canary Ingress, and asserts host routing, 404 on an unknown host, the canary
weight split, TLS with SNI, Ingress status writeback, and metrics movement.
It tears everything down afterwards; `KEEP=1` leaves it standing.

```sh
deploy/e2e.sh
```

Every `kubectl` and `helm` call in that script carries an explicit
`--context`/`--kube-context`, and it refuses to run against a cluster that does
not look local. A developer kubeconfig usually holds production clusters, and a
mistyped current-context is exactly how a test script deletes one.

One local-cluster detail the script handles in place: Docker Desktop's
Kubernetes runs a kind-style node whose containerd is a separate image store
from the docker daemon's, so a freshly built image is invisible to the kubelet
and a pod referencing it fails with `ErrImageNeverPull`. The script loads it
explicitly, which is what `kind load docker-image` does under the hood:

```sh
docker save ramjet-ingress:e2e | docker exec -i desktop-control-plane ctr -n k8s.io images import -
```

The node is addressable as `desktop-control-plane` even though `docker ps` does
not list it — Docker Desktop hides the container from the listing while still
allowing `exec`, so an empty `docker ps` is not evidence that there is no node
to load into. Importing rather than pulling is what lets `imagePullPolicy` stay
`Never`: the assertions then prove the image *this script built* is the one that
ran, with no path by which the kubelet could quietly substitute a registry copy.

## License

Dual-licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE))
- MIT license ([LICENSE-MIT](LICENSE-MIT))

at your option.

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in the work by you, as defined in the Apache-2.0 license, shall be
dual licensed as above, without any additional terms or conditions.
