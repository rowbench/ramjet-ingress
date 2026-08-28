# Operations

What this controller gives you that a reload-based one cannot, and how to use
it.

| Page | The question it answers |
|---|---|
| [Observability](./observability.md) | What is happening right now, and did the config I just pushed make it worse? |
| [Rollback and the audit trail](./rollback.md) | Put the previous configuration back on the wire, now — and afterwards, what changed and when? |
| [Canary auto-promotion](./canary.md) | Let a healthy canary promote itself, and pull it back the moment it stops being healthy |
| [Traffic mirroring](./mirroring.md) | Give a rewrite production traffic before it gets production responsibility |
| [HTTP/3](./http3.md) | QUIC, experimentally, and the cloud constraint that decides whether it works at all |

Two of these — rollback and mirroring — exist because publishing a
configuration is one pointer store. If applying a generation is a pointer store,
republishing an old one is the same pointer store; and if the request path never
waits on a lock, adding a fire-and-forget copy to it is not a latency decision.

## The admin listener

Everything on these pages is reachable on `:10254`, which is its own port rather
than a reserved path on the data plane. A path on the data plane is a path an
Ingress can claim, so `/metrics` would either shadow somebody's application
route or be shadowed by it — and it would be reachable from the internet, which
is a way to tell an attacker your request rate.

The chart puts it behind a **ClusterIP Service**, never the internet-facing
LoadBalancer, and because the split is two objects rather than a list of ports,
no values entry can accidentally publish it.

```sh
kubectl port-forward -n ramjet-ingress svc/ramjet-ingress-admin 10254:10254
```

There is no authentication and there is not going to be: anything that can reach
this port can already reach the pod's ServiceAccount token. What *is* enforced
is the shape — the mutating endpoint answers to `POST` and `DELETE` and nothing
else, so a link, a browser prefetch, a scraper following URLs, or a health
checker walking paths cannot roll a cluster back by accident.
