# Deploying ramjet-ingress

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
manifest — see [Regenerating](#regenerating-the-static-manifests).

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
| [`aws`](provider/aws/values.yaml) | `aws-load-balancer-type: external` — NLB via the AWS Load Balancer Controller, IP targets |
| [`aws-nlb-proxy`](provider/aws-nlb-proxy/values.yaml) | `aws-load-balancer-target-group-attributes: proxy_protocol_v2.enabled=true` |
| [`aws-nlb-tls`](provider/aws-nlb-tls/values.yaml) | `aws-load-balancer-ssl-cert` + `aws-load-balancer-ssl-ports: https` — ACM terminates |
| [`gcp`](provider/gcp/values.yaml) | none; the built-in GKE controller and `externalTrafficPolicy: Local` |
| [`azure`](provider/azure/values.yaml) | `azure-load-balancer-health-probe-request-path: /healthz` |
| [`digitalocean`](provider/digitalocean/values.yaml) | `do-loadbalancer-enable-proxy-protocol: "true"` |
| [`scaleway`](provider/scaleway/values.yaml) | `scw-loadbalancer-proxy-protocol-v2: "true"` |
| [`oracle`](provider/oracle/values.yaml) | `oci-load-balancer-shape: flexible` (+ flex min/max) |
| [`exoscale`](provider/exoscale/values.yaml) | `exoscale-loadbalancer-service-strategy: source-hash`, as a DaemonSet |
| [`baremetal-nodeport`](provider/baremetal-nodeport/values.yaml) | none; `NodePort` pinned to 30080/30443 |
| [`baremetal-hostnetwork`](provider/baremetal-hostnetwork/values.yaml) | none; `hostNetwork` DaemonSet on :80/:443 |

Each preset carries the reasoning for every line in it. Read the one you are
about to use — several of them turn on behaviour that is unsafe if the other
half is missing.

## Where the client's IP address comes from

This is the question that decides most of the configuration above, and getting
it wrong is quiet: `X-Forwarded-For` still gets written, it just contains the
load balancer's address instead of the client's, and nothing anywhere reports an
error.

There are three mechanisms that preserve it, and two shapes that do not need one.

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
short-circuited straight to a backend and never traverses the balancer — arriving
with no header at a listener that demands one. In-cluster clients break while
everything from outside works. The fix is to make the Service status report a
hostname rather than an IP, which stops the rule being installed:
`do-loadbalancer-hostname` on DigitalOcean, `scw-loadbalancer-use-hostname` on
Scaleway. Both are documented, commented out, in their presets.

**A reachable proxy-protocol port is a spoofable one.** The header names the
client, so anything that can open a connection to that port can claim any
address it likes. Only turn this on where the balancer is the sole path in.

The admin port is never covered by any of this: `/healthz` and `/readyz` come
from the kubelet, which speaks no PROXY protocol. Nor is the `uring` engine —
it refuses `--proxy-protocol` at startup, so a preset that sets it and an
`--engine uring` in `controller.extraArgs` will not start together.

Cloud health checks need the same care, and the answer differs by provider. AWS
sends the PROXY header on health check connections too once the target group
attribute is set — so `aws-nlb-proxy` deliberately keeps the default TCP check
rather than aiming an HTTP check at the admin port, which would reject it and
take every target unhealthy. DigitalOcean and Scaleway keep their default TCP
checks for the same reason.

## Bare metal

Two shapes, and the choice is about which ports you need.

**[`baremetal-nodeport`](provider/baremetal-nodeport/values.yaml)** — a NodePort
Service pinned to 30080 and 30443. Works on any cluster, needs no load balancer
controller, and is the right thing for testing. It cannot serve :80, and the
client address is lost to SNAT unless something in front is health-checking the
nodes (see `externalTrafficPolicy` above). The ports are fixed rather than
allocated so that firewall rules and upstream configs naming them do not go
stale when the Service is recreated.

**[`baremetal-hostnetwork`](provider/baremetal-hostnetwork/values.yaml)** — a
DaemonSet on the host network, binding :80 and :443 on every node. No
translation, no balancer, and the socket already carries the client's address.
The ports belong to the node, so a second copy on the same node cannot start —
which is why it is a DaemonSet — and the admin port is now on every node's
external interface, with the node firewall as the only thing keeping it private.

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
`LoadBalancer` Service with an address to read. Set `controller.publishAddress`
to whatever clients actually use. Routing is unaffected either way; the status
field is advertising, not configuration.

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
that a request arriving *without* a header is refused — the second being the
one that matters, since a listener accepting both shapes would let any client
that can reach it claim any address it likes.

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
