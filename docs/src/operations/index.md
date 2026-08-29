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

Three things stand between the mutating endpoints and an accident or an
attacker, and they are deliberately different in kind.

**The shape**, unconditionally: the mutating endpoint answers to `POST` and
`DELETE` and nothing else, so a link, a browser prefetch, a scraper following
URLs, or a health checker walking paths cannot roll a cluster back by accident.

**The network**: a ClusterIP Service and nothing in front of it. The chart's
optional `networkPolicy.enabled` narrows that further, to the release namespace.

**A bearer token**, with `--admin-token-file` (chart:
`controller.adminToken.secretName`). Set it and every mutating `/admin/` request
must carry `Authorization: Bearer <token>`:

```sh
kubectl -n ramjet-ingress create secret generic ramjet-admin \
  --from-literal=token="$(openssl rand -hex 32)"
helm upgrade ramjet ... --set controller.adminToken.secretName=ramjet-admin
```

```sh
curl -X POST -H "Authorization: Bearer $TOKEN" \
  -d '{"generation": 41}' localhost:10254/admin/rollback
```

Without it, the daemon logs one warning at startup and accepts a rollback from
anything that can reach the port. This page used to argue that a token was
pointless because anything reaching the port could already read the pod's
ServiceAccount token — which was wrong in one specific way. That token is on
*our* filesystem, not on the network. A pod in some other namespace cannot read
it, and until now the only thing stopping that pod from rolling the ingress
table back was that it had not thought of it.

`GET` is never gated. `/metrics` is scraped by Prometheus and `/healthz` and
`/readyz` are called by the kubelet, and neither can be taught to send a header —
gating them would trade a rollback for a pod that restarts every time its
liveness probe is refused. `/admin/generations` and `/admin/routes` stay open for
the same reason they are `GET` at all: they report what a replica is serving,
which is not a secret from anything that can already send it traffic.

The token is read once, at startup, so rotating it is `kubectl rollout restart`
after replacing the Secret. A `read(2)` per request to make a yearly event
convenient is the wrong trade. `ramjet-top` sends the token with `--token-file`,
and only on its pin and unpin keys.
