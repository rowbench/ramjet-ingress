# Configuration

There are three places configuration comes from, and they answer different
questions.

| Where | What it configures | Reference |
|---|---|---|
| The **Ingress object** | Which hosts and paths route to which Services, and the certificates that serve them | [Ingress basics](./ingress.md), [TLS](./tls.md) |
| **Annotations** on an Ingress | Canaries, traffic mirroring, canary auto-promotion | [Annotations reference](./annotations.md) |
| **Flags** (or their environment twins) on the daemon | Listeners, timeouts, pool sizes, which engine, what the replica watches | [Flags reference](./flags.md) |

The dividing line is deliberate: an annotation is a per-route decision made by
whoever owns the workload, a flag is a per-replica decision made by whoever owns
the ingress controller.

## What is deliberately not configurable

The annotation vocabulary is **canary, mirroring, auto-promotion, and class**.
`RouteTable` has no rewrite, header-mutation, rate-limit, session-affinity, or
auth rules, so the corresponding `nginx.ingress.kubernetes.io` annotations are
not read. Those attach to a route when the proxy can act on them; parsing an
annotation the data plane ignores is worse than not parsing it, because it looks
configured.

If you are migrating from ingress-nginx, [Limitations](../limitations.md) is the
page that tells you what will not come across.
