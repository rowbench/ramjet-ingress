# Limitations

Known gaps, each with the reason it is a gap rather than a bug. Read this before
you deploy.

## Run one replica

**There is no leader election.** The chart hard-codes `replicas: 1`, and there
is no values entry to find at 3am.

Every replica watches the API server independently and writes Ingress status
independently. Routing is unaffected by that — each replica compiles the same
table from the same objects — but **the status writes race**: several
controllers server-side-applying the same subtree under the same field manager
will fight over `.status.loadBalancer` if their `--publish-address` values
differ.

The fix is a `coordination.k8s.io` `Lease` and gating the status writer on
holding it. The writer is already isolated behind one optional value, so it is a
contained change.

Until then: **scale by making the one replica bigger**, and use
`--no-status-update` if you must run more.

## There is no TLS to the upstream

The upstream side speaks two protocols — HTTP/1.1 by default, and cleartext
HTTP/2 for a backend annotated
[`backend-protocol: GRPC`](./configuration/annotations.md#backend-protocol) —
and **both are cleartext**. That is the same default ingress-nginx ships, and
inside a cluster it is usually what you want.

The consequence is which annotation values are honoured. `HTTP` and `GRPC` are;
`GRPCS` and `HTTPS` are **read, reported in a warning, and not honoured**,
because both mean "dial this pod over TLS" and there is no code here that does.
`AUTO_HTTP` would need per-endpoint scheme detection, and `FCGI` is not HTTP.
The backend stays on HTTP/1.1 in all four cases and the warning names the value,
rather than the request being served against a protocol nobody asked for.

Lifting this means a client-side rustls configuration for upstream connections,
with its own trust store and its own answer to what verifies a pod certificate.

## gRPC needs one annotation, and is refused without it

gRPC over an HTTP/1.1 backend cannot work — gRPC is defined in terms of HTTP/2
streams and trailers and has no HTTP/1.1 form — so a request with an
`application/grpc` content type whose backend is HTTP/1.1 is answered with a
`502` that names the annotation to add:

```
502 Bad Gateway: gRPC requires an HTTP/2 backend; set
nginx.ingress.kubernetes.io/backend-protocol: GRPC on the Ingress
```

Add it and the request is forwarded like any other. This is a refusal to guess,
not a missing feature.

## WebSocket does not cross an h2c backend

`Connection` and `Upgrade` are forbidden in HTTP/2, so an upgrade request sent to
a backend annotated `GRPC` reaches the application as an ordinary request rather
than as a handshake. WebSocket over HTTP/2 (RFC 8441 extended CONNECT) is not
implemented in either direction.

This is only a constraint if one Service port serves both WebSocket and gRPC,
which is unusual. Otherwise: WebSocket routes go to an `HTTP` backend, gRPC
routes to a `GRPC` one, and both work.

## `ExternalName` Services serve 503

Following a DNS name from the data plane needs a resolver with TTL handling and
re-resolution; pointing at whatever the name resolved to at compile time would
be a stale-address bug waiting for the first failover.

## The annotation vocabulary is small

Canary, mirroring, auto-promotion, and class. The route table has no rewrite,
header-mutation, rate-limit, session-affinity, or auth rules, so the
corresponding `nginx.ingress.kubernetes.io` annotations are **not read**.

Those attach to a route when the proxy can act on them. Parsing an annotation
the data plane ignores is worse than not parsing it, because it looks
configured.

The full list of what *is* read is the
[annotations reference](./configuration/annotations.md).

## An `IngressTLS` entry with no `hosts` is skipped

The controller cannot read a certificate's SANs to work out which names it
covers — that would mean parsing X.509 in the control plane, which is exactly
the dependency the layering split exists to avoid.

`--default-tls-secret` is the supported way to serve a fallback certificate.

## The `uring` engine does not speak HTTP/2 itself

`--engine uring` reached parity with the hyper engine on TLS, WebSocket
upgrades, the PROXY protocol, mirroring, per-route counters, Kubernetes mode and
graceful drain. What is left is HTTP/2, at both ends of the hop.

**It speaks HTTP/1.1.** HTTP/2 is served by handing those connections to a hyper
engine in the same process — the ClientHello is read before a configuration is
chosen, so a client that offered `h2` is passed over with its bytes intact and
sees one connection that negotiated HTTP/2. That works, and it is on by default,
but it means an HTTP/2-heavy deployment is running both engines and getting the
reactor's benefit on the HTTP/1.1 half only. `--no-h2-dispatch` turns the
dispatch off, at the cost of not offering HTTP/2 at all.

HTTP/3 stays on the hyper engine's QUIC listener, and `--http3` with `--engine
uring` is refused at startup rather than ignored.

**HTTP/2 upstreams are the hyper engine's alone.** The uring engine has its own
HTTP/1.1 upstream pool rather than sharing hyper's, so a route whose backend is
annotated `backend-protocol: GRPC` answers `502` there, naming the engine, and
gRPC to it is refused with it. The h2 dispatch above does not help: it moves the
*downstream* connection, and the backend protocol is a property of the route.
A cluster serving gRPC wants `--engine hyper`.

[Engines](./operations/engines.md) has the full parity matrix, and the
differential test that keeps it honest.

## The `uring` engine's p99 is an open question

On real Linux the reactor **wins throughput and the median and loses the tail.**
On a `t3.xlarge` k0s cluster it served 5.8% more requests per second at an 18.1%
lower p50, and its p99 came out about **6.8% worse** than the hyper engine's — at
c64 and at c256 both, consistently enough not to read as noise.

Nobody has explained it. It is also not a known property of the design being
rediscovered: the earlier Docker measurements had the reactor ahead at every
percentile out to p99.9, so something about this environment or this engine
changed and the cause is not identified.

Until it is, **no tail-latency claim is made for the reactor.** A deployment
whose SLO is written against p99 rather than throughput should stay on
`--engine hyper`, or measure its own traffic before switching. The numbers, and
the CPU-contention caveat that has to be read with them, are in
[Performance](./performance.md#on-real-linux).

## HTTP/3 is experimental and off by default

One QUIC endpoint on one runtime rather than one per core, no 0-RTT, no QUIC
upstream, no upgrades, and no PROXY protocol. Each of those has a reason rather
than a TODO — they are in [HTTP/3](./operations/http3.md).

**The deployment-side constraint is separate and larger**: `alt-svc` advertises
the TCP port number, so that port has to answer UDP through whatever is in front
of the pod, and most cloud load balancers cannot do that.
[Deployment](./deployment.md#http3-and-which-load-balancers-can-carry-it) has
the per-provider answer.

## Idle-connection memory is 4.6x nginx

An idle keep-alive connection costs this data plane about **20.3 KiB** against
nginx's **4.4 KiB**. The retention problem is fixed — the memory comes back on
close, and a second connect/close cycle settles at the same number rather than a
higher one — but the per-connection gap is structural.

About 16 KiB of it is hyper's two 8 KiB buffers, and there is no public API that
lowers it: `max_buf_size` caps how far the read buffer may *grow*, and hyper
refuses to set it below its initial size. Patching that constant in a local
build takes the figure to 11.3 KiB, so the fix is a one-line change in a
dependency and the right place to make it is upstream.

The practical consequence: `resources.limits.memory: 256Mi` is roughly **twelve
thousand idle keep-alive connections**. Budget accordingly, and see
[Performance](./performance.md#idle-connection-memory-the-loss).

## No Gateway API

The target is parity with `kubernetes/ingress-nginx` on the
`networking.k8s.io/v1` Ingress resource.

## Deliberate divergences from ingress-nginx

Three, and each changes behaviour you might be relying on:

- **Regex anchoring.** ingress-nginx emits `location ~* "^<path>"`, a literal
  concatenation. This compiles `^(?:<path>)`. The two differ only for a
  top-level alternation, where `^a|b` anchors just the first branch and routes
  traffic nobody intended. Case-insensitivity is preserved.
- **Compiled regexes are size-limited** to 1 MiB. A pathological path should
  fail validation, not silently consume memory in every replica.
- **Host validation is strict.** A `host` containing a port, a path, or a
  misplaced `*` is rejected at build time rather than normalized into a guess.
