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

## gRPC upstreams answer 502

gRPC is defined in terms of HTTP/2 streams and trailers and has no HTTP/1.1
form. Downstream already speaks h2, but the upstream pool dials HTTP/1.1, so a
gRPC request would be silently downgraded into something the backend cannot
parse.

Requests with an `application/grpc` content type are **rejected explicitly**,
naming the limitation, instead. Lifting it means an h2 upstream mode selected
per backend from `backend-protocol: GRPC`.

## Upstream is HTTP/1.1 only

Which is the same default ingress-nginx ships, and is transparent for everything
except the case above. There is no TLS to the upstream either.

## h2c is untested

Downstream HTTP/2 over TLS is negotiated by ALPN, which offers `h2` ahead of
`http/1.1`, and there is a test that proxies a real request over it.

Cleartext HTTP/2 is a different story. The plaintext listener is built on
hyper-util's protocol-detecting connection builder, so a client sending the
HTTP/2 connection preface **should** be served — but nothing in the tree
exercises that path, and the `h2c` string appears only in a hop-by-hop header
test. Treat prior-knowledge h2c as unverified rather than as supported, and do
not plan a deployment around it without testing it yourself first.

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

## The `uring` engine is static-mode only

`--engine uring` serves HTTP/1.1 plaintext and nothing else:

- **No TLS** (502)
- **No HTTP/2** (502)
- **No protocol upgrades** (502)
- **No Kubernetes mode** — static routes only
- **Refuses `--proxy-protocol` at startup**, rather than ignoring it: silently
  attributing every request to the load balancer is the one outcome an operator
  who set the flag would never detect. The parser is sans-io precisely so it can
  be reused in the reactor's accept path unchanged, which is where the read
  belongs.
- **Refuses `--http3` at startup**, because it has neither TLS nor QUIC.

Each refusal names the other engine, and the same list prints at startup. A gap
that behaves like a bug in whatever is on the other end is worse than a missing
feature.

It is also **one phase behind** the hyper data plane, which has been through a
profiling pass and a benchmark rewrite. See
[Performance](./performance.md#what-is-not-claimed).

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
