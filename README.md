# ramjet-ingress

A Kubernetes ingress controller with a native Rust data plane. No nginx, no
config file regeneration, no reload: a configuration change swaps an `Arc`, and
in-flight connections never notice.

The design and its reasoning are in [ARCHITECTURE.md](ARCHITECTURE.md). This
file is about running it.

| Crate | What it is |
|---|---|
| `ramjet-router` | Route table, host/path matcher, load balancing, canary |
| `ramjet-proxy` | Listeners, TLS termination, HTTP/1.1 and HTTP/2, upstreams |
| `ramjet-controller` | Kubernetes watches, translation, status writeback |
| `ramjet-engine` | An experimental second data plane on a completion-based reactor |
| `ramjet-ingressd` | The daemon that wires them together |

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

## Running it without a cluster

`--static-routes` swaps the API server for a YAML file and changes nothing else
about the serving path, which makes it the fastest way to look at the data
plane on its own:

```sh
cargo run -p ramjet-ingressd -- --static-routes crates/ramjet-ingressd/examples/dev-routes.yaml
```

Listeners default to `:8080` plaintext, `:8443` TLS, and `:10254` admin
(`/metrics`, `/healthz`, `/readyz`). `--help` lists every option; each one has
an environment twin, and a flag always beats the environment.

## The container image

Multi-stage: a full Rust toolchain compiles, and
`gcr.io/distroless/cc-debian12:nonroot` carries the result. The runtime has no
shell and no package manager, and the process runs as uid 65532. TLS is rustls
over ring, so the image needs no OpenSSL and no CA bundle.

```sh
docker build -t ramjet-ingress:0.1.0 .
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
