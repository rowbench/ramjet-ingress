# Deployment

One command per cloud. Every provider below has a Helm values preset and a
rendered manifest that needs no Helm at all.

```sh
# Helm, with the preset for your provider
helm install ramjet deploy/chart/ramjet-ingress \
  --namespace ramjet-ingress --create-namespace \
  -f deploy/provider/aws/values.yaml

# or the rendered equivalent, no Helm required
kubectl apply -f deploy/static/provider/aws.yaml
```

Both install the same thing: a Deployment (or DaemonSet), a ServiceAccount, a
ClusterRole and binding, a Service for traffic, a separate ClusterIP Service for
the admin port, and an `IngressClass` named `ramjet`. Point workloads at it with
`ingressClassName: ramjet`.

The static manifests are generated from the presets. Edit the preset, not the
manifest — see [Regenerating the static manifests](#regenerating-the-static-manifests).

## Generic install

No provider preset, cluster defaults everywhere:

```sh
helm install ramjet deploy/chart/ramjet-ingress \
  --namespace ramjet-ingress --create-namespace
```

This gives you a `LoadBalancer` Service and whatever your cluster makes of it.
On a cluster with no load balancer controller the Service stays `<pending>`
forever; routing still works, and Ingress status falls back to
`controller.publishAddress`. If that is your situation, you want
[bare metal](#bare-metal) rather than this.

## Providers

| Preset | The annotation it hinges on |
|---|---|
| `aws` | `aws-load-balancer-type: external` — NLB via the AWS Load Balancer Controller, IP targets |
| `aws-nlb-proxy` | `aws-load-balancer-target-group-attributes: proxy_protocol_v2.enabled=true` |
| `aws-nlb-tls` | `aws-load-balancer-ssl-cert` + `aws-load-balancer-ssl-ports: https` — ACM terminates |
| `gcp` | none; the built-in GKE controller and `externalTrafficPolicy: Local` |
| `azure` | `azure-load-balancer-health-probe-request-path: /healthz` |
| `digitalocean` | `do-loadbalancer-enable-proxy-protocol: "true"` |
| `scaleway` | `scw-loadbalancer-proxy-protocol-v2: "true"` |
| `oracle` | `oci-load-balancer-shape: flexible` (+ flex min/max) |
| `exoscale` | `exoscale-loadbalancer-service-strategy: source-hash`, as a DaemonSet |
| `baremetal-nodeport` | none; `NodePort` pinned to 30080/30443 |
| `baremetal-hostnetwork` | none; `hostNetwork` DaemonSet on :80/:443 |

Each preset lives at `deploy/provider/<name>/values.yaml` and carries the
reasoning for every line in it. Read the one you are about to use — several of
them turn on behaviour that is unsafe if the other half is missing.

## Where the client's IP address comes from

This is the question that decides most of the configuration above, and getting
it wrong is quiet: `X-Forwarded-For` still gets written, it just contains the
load balancer's address instead of the client's, and nothing anywhere reports an
error.

There are three mechanisms that preserve it, and two shapes that do not need
one.

| Mechanism | How the address survives | Presets |
|---|---|---|
| Passthrough + `externalTrafficPolicy: Local` | The balancer forwards the client's own packet; `Local` removes the node-to-node hop that would SNAT it | `gcp`, `azure` |
| A target-group setting | The balancer is told not to rewrite the source address | `aws`, `aws-nlb-tls` (`preserve_client_ip.enabled=true`) |
| PROXY protocol | The balancer prepends a header naming the client; the listener reads it | `aws-nlb-proxy`, `digitalocean`, `scaleway` |
| None | The address is lost to SNAT; `X-Forwarded-For` carries a node IP | `baremetal-nodeport` (see its note) |
| Nothing needed | The listener is on the node, so the socket already has it | `baremetal-hostnetwork` |

### `externalTrafficPolicy: Local`, and its health check

Every cloud preset here sets `Local`, and it is worth knowing what that trades.

`Cluster` (the Kubernetes default) lets any node accept the packet and forward
it to a pod on another node. That second hop is a SNAT, so the pod sees the
first node's address. `Local` removes the hop: only a node that already has a
ready pod will serve the packet, and a node without one drops it silently.

What makes that safe is the `healthCheckNodePort` Kubernetes allocates whenever
a `LoadBalancer` Service is `Local`. Every node serves `/healthz` on it, and it
answers 200 only where a ready pod actually is. The cloud balancer checks it and
stops sending to the silent nodes. Note that this is *kube-proxy's* `/healthz`
on a port of its own — it is not this data plane's, and not the admin port.

The trap is a shape with no balancer doing that check. On bare metal with DNS
round-robin or a hand-written upstream list, `Local` means traffic keeps going
to nodes that quietly answer nothing. That is why `baremetal-nodeport` leaves it
at `Cluster` and says so.

### PROXY protocol

Where the balancer supports it, PROXY protocol is the most direct answer: the
balancer prepends a header naming the real client, and the listener reads the
address out of it.

It comes as a pair, and both halves must be set together:

```yaml
proxyProtocol:
  enabled: true          # the listeners require the header
service:
  annotations:
    <provider annotation that makes the balancer send it>
```

Neither half is useful alone. Without the annotation, every connection is
rejected for missing a header nothing is sending. Without the value, the daemon
reads `PROXY TCP4 …` as an HTTP request line.

Three things that follow from it, all of which have bitten someone:

**The listener has no mixed mode.** Once it requires the header, a connection
arriving without one is refused. That includes anything inside the cluster
dialing the Service or the pod directly.

**Which is why the hostname workaround exists.** kube-proxy adds the balancer's
external IP to node-local iptables rules, so a pod connecting to that IP is
short-circuited straight to a backend and never traverses the balancer —
arriving with no header at a listener that demands one. In-cluster clients break
while everything from outside works. The fix is to make the Service status
report a hostname rather than an IP, which stops the rule being installed:
`do-loadbalancer-hostname` on DigitalOcean, `scw-loadbalancer-use-hostname` on
Scaleway. Both are documented, commented out, in their presets.

**A reachable proxy-protocol port is a spoofable one.** The header names the
client, so anything that can open a connection to that port can claim any
address it likes. Only turn this on where the balancer is the sole path in.

The admin port is never covered by any of this: `/healthz` and `/readyz` come
from the kubelet, which speaks no PROXY protocol, and requiring the header there
would take the probes offline the moment the flag was set.

Either engine can sit behind one of these presets. The `uring` engine reads the
header with the same parser, in the same place — ahead of the TLS record layer
— and with the same required-not-optional answer, so a preset that sets
`--proxy-protocol` and an `--engine uring` in `controller.extraArgs` work
together.

Cloud health checks need the same care, and the answer differs by provider. AWS
sends the PROXY header on health check connections too once the target group
attribute is set — so `aws-nlb-proxy` deliberately keeps the default TCP check
rather than aiming an HTTP check at the admin port, which would reject it and
take every target unhealthy. DigitalOcean and Scaleway keep their default TCP
checks for the same reason.

## HTTP/3, and which load balancers can carry it

`http3.enabled=true` is experimental and off by default. It adds `--http3`, a
UDP container port and a UDP Service port — both on the **same number** as
`https` — and makes every HTTPS response carry `alt-svc: h3=":<port>"; ma=86400`.

That header is the whole mechanism, and it is also the whole constraint. A
client that reads it retries **the same authority** over QUIC, so the port
number it is already using for TCP has to answer UDP too, through every hop in
front of this Service. Which is a per-provider question with mostly
disappointing answers:

| Shape | UDP on the same address and port? |
|---|---|
| AWS NLB (`aws`, `aws-nlb-proxy`) | **Yes.** One NLB carries TCP 443 and UDP 443 on one address; this is the shape it was built against |
| `aws-nlb-tls` | **No, and not meaningfully.** ACM terminates TLS at the balancer and forwards plaintext, and there is no QUIC to a plaintext port |
| GCP, Azure, Oracle, Exoscale, DigitalOcean, Scaleway | **Per-provider, usually not on the same address.** Where UDP is supported at all it typically needs a second load balancer, and two balancers do not share an address — so the advertisement would name a port the client cannot reach |
| `baremetal-hostnetwork` | **Yes.** There is no balancer to ask: the node's UDP 443 is the node's UDP 443 |
| `baremetal-nodeport` | **Partly.** The chart does not pin a UDP nodePort, so the allocated one will not match 30443; fine behind something that maps ports, not for direct access |

Getting it wrong is slow rather than broken. A client whose QUIC attempt fails
falls back to TCP by itself — the cost is one wasted attempt per connection
until the advertisement expires, which is why `ma` is a day and not a week.

The presets are deliberately unchanged: none of them turns this on, because
whether UDP reaches the pod is a property of an account's networking rather than
of a provider. Turn it on with `--set http3.enabled=true` on top of a preset
once you have checked that it does.

Two more things worth knowing before enabling it in production:

- **The PROXY protocol does not apply.** It is a preamble on a TCP byte stream
  and has no UDP form, so a QUIC connection's client address is whatever the IP
  header says. On a balancer that forwards UDP without rewriting the source that
  is the real client; on one that SNATs it, `X-Forwarded-For` on HTTP/3 requests
  will name the balancer while the TCP path is still correct. There is no
  configuration that fixes the difference.
- **It is one core.** The QUIC endpoint runs on a single dedicated runtime
  rather than one per core, for the reason set out in
  `crates/ramjet-proxy/src/http3.rs`: sharding a UDP port across sockets with
  `SO_REUSEPORT` hashes by 4-tuple, and a QUIC connection is deliberately not
  identified by its 4-tuple. HTTP/1.1 and HTTP/2 keep every core they had.

See [HTTP/3](./operations/http3.md) for the protocol-side detail.

## Bare metal

Two shapes, and the choice is about which ports you need.

**`baremetal-nodeport`** — a NodePort Service pinned to 30080 and 30443. Works
on any cluster, needs no load balancer controller, and is the right thing for
testing. It cannot serve :80, and the client address is lost to SNAT unless
something in front is health-checking the nodes (see `externalTrafficPolicy`
above). The ports are fixed rather than allocated so that firewall rules and
upstream configs naming them do not go stale when the Service is recreated.

**`baremetal-hostnetwork`** — a DaemonSet on the host network, binding :80 and
:443 on every node. No translation, no balancer, and the socket already carries
the client's address. The ports belong to the node, so a second copy on the same
node cannot start — which is why it is a DaemonSet — and the admin port is now
on every node's external interface, with the node firewall as the only thing
keeping it private.

Binding below 1024 as uid 65532 needs `NET_BIND_SERVICE`, which the preset adds
back to an otherwise-empty capability set. If a pod still fails to bind, the
node's `net.ipv4.ip_unprivileged_port_start` is the other lever (it cannot be
set from the pod: Kubernetes rejects network sysctls on host-network pods).

**MetalLB** is the third option and often the best one: put it in front and use
the generic install with `service.type=LoadBalancer`. MetalLB assigns a real
address out of an `IPAddressPool`, the Service behaves like a cloud one, and
Ingress status writeback works because there is finally an address to publish.
<https://metallb.universe.tf/>

Neither bare-metal preset can populate Ingress status on its own — there is no
`LoadBalancer` Service with an address to read. Set
`controller.publishAddress` to whatever clients actually use. Routing is
unaffected either way; the status field is advertising, not configuration.

## What the chart does not let you configure

Two things, deliberately.

**`replicas: 1` is hard-coded.** There is no leader election yet. Status
writeback reads a Service's address and server-side-applies it to every managed
Ingress, and a second replica would do the same work against the same objects
with the same field manager on its own schedule. The fix is leader election in
the controller, not a values entry — so there is no values entry to find at 3am.

**The admin port is on its own ClusterIP Service.** `/metrics` and the probes
are never attached to the internet-facing LoadBalancer, and because the split is
two objects rather than a list of ports, no value can accidentally publish them.

## Chart values

The defaults, as `deploy/chart/ramjet-ingress/values.yaml` ships them. Every
`controller.*` entry maps to a flag on the [flags
reference](./configuration/flags.md).

```yaml
image:
  repository: sofelia/ramjet-ingress
  tag: ""                        # defaults to .Chart.AppVersion
  pullPolicy: IfNotPresent
  pullSecrets: []

kind: Deployment                 # or DaemonSet

controller:
  ingressClass: ramjet
  watchNamespace: ""             # "" is every namespace
  updateStatus: true
  publishService: ""             # defaults to "<release-namespace>/<fullname>"
  publishAddress: ""
  defaultBackend: ""
  defaultTlsSecret: ""
  connectTimeout: 5
  responseTimeout: 60
  maxConnectAttempts: 3
  shutdownGrace: 30
  historySize: 10
  auditWebhook: ""
  extraArgs: []
  logLevel: "info,kube=warn"
  extraEnv: []

proxyProtocol:
  enabled: false
http3:
  enabled: false

ports:
  http: 8080
  https: 8443
  admin: 10254

service:
  type: LoadBalancer
  annotations: {}
  externalTrafficPolicy: ""
  http:  { port: 80,  nodePort: "", targetPort: http }
  https: { port: 443, nodePort: "", targetPort: https }

adminService:
  enabled: true
  port: 10254

ingressClass:
  create: true
  name: ""                       # defaults to controller.ingressClass
  isDefaultClass: false

resources:
  requests: { cpu: 100m, memory: 64Mi }
  limits:   { memory: 256Mi }

terminationGracePeriodSeconds: 45

podSecurityContext:
  runAsNonRoot: true
  runAsUser: 65532
  runAsGroup: 65532
  fsGroup: 65532
  seccompProfile: { type: RuntimeDefault }

securityContext:
  allowPrivilegeEscalation: false
  readOnlyRootFilesystem: true
  capabilities: { drop: [ALL] }

metrics:
  scrapeAnnotations: true
```

### The memory limit is load-bearing

`resources.limits.memory: 256Mi` is not an arbitrary default. An idle keep-alive
connection costs this data plane about 20 KiB, so 256Mi is roughly twelve
thousand of them. That arithmetic is in the values file rather than left to be
rediscovered, and the number comes from a benchmark that originally found the
opposite: at 10,000 idle connections the process peaked at 266 MiB and would
have been OOM-killed by its own default manifest. That is fixed — the peak is
now 200.7 MiB and the memory comes back on close — but the per-connection cost
is still 4.6x nginx's, and raising the default to make room for it would have
hidden the finding rather than fixed it. See
[Performance](./performance.md#idle-connection-memory-the-loss).

### `--publish-service` and Ingress status

`--publish-service` defaults to the chart's own LoadBalancer Service. That is
the mechanism by which an Ingress in an unrelated namespace ends up advertising
the address traffic really arrives on; set `controller.publishAddress` as a
fallback on clusters with no load balancer controller.

### RBAC

Exactly the six rules the controller's five watches and its status writer need.
It creates nothing, deletes nothing, and holds no write verb outside the
`ingresses/status` subresource — with two additions that are opt-in behaviour
rather than routing:

- `events.k8s.io` / `events` / `create`, `patch` — for the audit trail's
  Kubernetes Events. Without it the Events are skipped at debug level and
  everything else still works.
- `networking.k8s.io` / `ingresses` / `patch` — for
  [canary auto-promotion](./operations/canary.md), the only write this
  controller makes to an object an operator authored. Without it, promotion logs
  a permission error every interval and changes nothing.

## Regenerating the static manifests

`deploy/static/provider/*.yaml` are generated. Edit the preset and re-render:

```sh
deploy/render.sh            # regenerate all of them
deploy/render.sh --check    # fail if the committed files are stale
```

`--check` is for CI. Without it, a chart change that nobody re-rendered ships a
manifest that no longer matches the chart it claims to come from.

## Validating a change

```sh
deploy/cloud-e2e.sh
```

Four passes: `helm lint` against every preset, every static manifest through
`kubectl apply --dry-run=server`, a full install-and-route test of
`baremetal-nodeport` against a local Docker Desktop cluster, and a PROXY
protocol test that reinstalls with `proxyProtocol.enabled=true` and speaks a
hand-built v1 header onto the socket. That last one asserts both halves: that
the spoofed address in the header reaches the backend as `X-Forwarded-For`, and
that a request arriving *without* a header is refused — the second being the one
that matters, since a listener accepting both shapes would let any client that
can reach it claim any address it likes.

The cloud presets are only ever dry-run, and cannot be more than that here — an
`aws-load-balancer-type` annotation means nothing without the AWS Load Balancer
Controller watching. What the dry-run does prove is the half that actually
breaks: that every object is schema-valid and the API server accepts it, which
is where a typo'd annotation key or a malformed port block shows up.

Every `kubectl` and `helm` call carries an explicit `--context`, and the
preflight refuses any cluster that does not look local. A developer kubeconfig
usually holds production clusters, and a mistyped current-context is exactly how
a test script deletes one.

One local-cluster detail worth knowing: Docker Desktop does not publish
NodePorts on the host, so the traffic assertions run *inside* the node container
against the node's own address. That is the more faithful test anyway — it is
the real NodePort path, where a port-forward would have proven only that the pod
serves.

## End-to-end proof

`deploy/e2e.sh` builds the image, installs the chart on a local Docker Desktop
cluster, deploys a pair of echo backends behind a production Ingress and a
canary Ingress, and asserts host routing, 404 on an unknown host, the canary
weight split, TLS with SNI, Ingress status writeback, and metrics movement. It
tears everything down afterwards; `KEEP=1` leaves it standing.

```sh
deploy/e2e.sh
```

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

## The container image

`sofelia/ramjet-ingress` on Docker Hub, public, and what the chart installs by
default — the quick starts at the top of this page pull it and need no local
build.

It is a manifest list covering `linux/amd64` and `linux/arm64`, so the pull
resolves to the node's architecture on its own.

| Tag | What it points at |
|---|---|
| `0.1.0`, `0.1` | A `v*` release. The chart's default, by way of an empty `image.tag` falling back to `appVersion` — so chart and image version together. |
| `sha-<short>` | One commit, exactly. Published by every build, and the tag to pin when a specific build is what you mean. |
| `latest` | The most recent build of `main`. It moves, which makes it the wrong thing for a cluster: a pod rescheduled onto a new node can come back as a different build than the pods beside it. |

`.github/workflows/images.yml` publishes them on every push to `main` and every
`v*` tag. Each architecture builds on a runner of that architecture — a Rust
release build with LTO under QEMU runs past the job timeout, so emulation is not
a slower version of the same thing but a broken one — and each pushes by digest
into the registry untagged. A final job creates the manifest list over both
digests, which is the only point at which any tag above starts resolving. The
workflow needs `DOCKERHUB_USERNAME` and `DOCKERHUB_TOKEN` as repository secrets;
see [deploy/README.md](https://github.com/rowbench/ramjet-ingress/blob/main/deploy/README.md#publishing-it).

### How it is built

Multi-stage: a full Rust toolchain compiles, and
`gcr.io/distroless/cc-debian12:nonroot` carries the result. The runtime has no
shell and no package manager, and the process runs as uid 65532. TLS is rustls
over ring, so the image needs no OpenSSL and no CA bundle.

The build context is the **parent directory**, because `ramjet-engine` depends
on the `ramjet` runtime from a sibling repository by path — the Dockerfile
copies both this tree and the `enhance-socket` sibling, and a context rooted
here cannot see the second one:

```sh
docker build -f Dockerfile -t ramjet-ingress:0.1.0 ..
```

The builder uses BuildKit cache mounts for Cargo's registry and the target
directory rather than the usual "build dummy sources first" trick, which for a
four-crate workspace would mean maintaining four fabricated source files that
mirror the real layout. The tradeoff: the caches live in the builder, not in the
image, so the binary is copied out inside the same `RUN`.
