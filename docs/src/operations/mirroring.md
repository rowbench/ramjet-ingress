# Traffic mirroring

Send a second, fire-and-forget copy of a route's traffic to a shadow backend and
throw the answer away — a rewrite gets production traffic before it gets
production responsibility.

Annotate the **production** Ingress:

```yaml
metadata:
  annotations:
    ramjet.dev/mirror-backend: shadow/api:80   # namespace optional
    ramjet.dev/mirror-percent: "10"            # default 100
    ramjet.dev/mirror-host: shadow.example.com # optional Host override
```

Copies carry `X-Mirrored-By: ramjet-ingress`, so a shadow can tell a copy from
the real thing before it decides whether to charge somebody's card.

A mirror is a property of the **route**, so it goes on the production Ingress.
The canary Ingress is a second opinion about where a share of that route's
traffic goes — not a second route that could have its own shadow. Setting
`mirror-backend` on a canary Ingress does nothing, and says so in a warning.

## The invariant

**A mirror must never make the primary request slower or more likely to fail.**

That is not a goal, it is the property that decides whether the feature can be
switched on in front of real traffic, and every mechanism below exists for it.

- **Nothing is awaited.** The request path hands the copy to a queue and
  returns. It never waits for a connection, a response, or a timeout.
- **The queue is bounded and drops.** One channel per serving runtime, 256 deep,
  entered with a non-blocking send. Per-runtime so a wedged shadow fills one
  core's queue rather than contending for a shared one; bounded because the
  alternative turns a slow mirror into unbounded memory growth on the pod
  serving production.
- **Responses are drained and discarded.** Drained rather than dropped so the
  upstream connection returns to the pool instead of being closed, which would
  put a TCP handshake on every mirrored request.
- **Failures are counted, never propagated.** A mirror backend that is down,
  refusing, absent, or catatonic produces a number on `/metrics` and nothing
  else. A five-second deadline, much shorter than the primary's, bounds the one
  thing a slow mirror can still affect: the queue behind it.
- **No in-flight accounting.** A copy does not take the `leastConn` guard its
  primary does. Letting shadow traffic move production's load-balancing
  decisions would be its own kind of leak.

## The body, which is the hard part

Everything above is cheap because a request head is small and already in memory.
A body is neither, and this data plane's whole position on request bodies is
that it does not buffer them.

So the cap is real and small: `--mirror-max-body`, **256 KiB** by default.

| Request | What happens |
|---|---|
| Body known empty — every `GET`, `HEAD`, `OPTIONS`, `DELETE`, and so the overwhelming majority of ingress traffic | Mirrored with **no buffering at all**, and it keeps its endpoint failover |
| Body fits under the cap | Read once; both copies get the same bytes |
| Body over the cap | The bytes already read become a **prefix** on the primary's body and the rest keeps streaming, so the upload resumes from wherever the cap stopped it. The mirror is skipped and counted |

The primary is never held waiting for more than the cap, and never fails because
of the attempt.

`--mirror-max-body 0` is legal and is not clamped up: it means "never buffer",
which still mirrors every `GET` and is a reasonable thing to ask for on a route
that carries large uploads.

## Four counters, because they have four different fixes

| Metric | Means | Fix |
|---|---|---|
| `ramjet_mirrored_total` | Copies sent | — |
| `ramjet_mirror_dropped_total` | The per-runtime queue was full | Raise the shadow's capacity |
| `ramjet_mirror_skipped_total` | The body was over the cap | Raise `--mirror-max-body` |
| `ramjet_mirror_failures_total` | The backend refused or did not answer | Fix the shadow |

A route's mirror configuration also appears on `/admin/routes`:

```json
"mirror": { "backend": "shadow", "percent": 100, "host": null }
```

and mirrors added or removed show up in the
[generation diff](./rollback.md#what-changed-in-words).

## `mirror-host` is not cosmetic

A shadow deployment usually answers to a different name, and a copy carrying the
production `Host` can be routed by whatever sits in front of it — possibly
straight back to production, which is the one outcome a mirror must never
produce.

## Sampling

`ramjet.dev/mirror-percent` takes `0`–`100` and defaults to **100**. The
opposite default would make an operator who added `mirror-backend` and saw no
traffic conclude the feature does not work; sampling exists to turn mirroring
*down* on a route that cannot afford the duplicate load.

`0` is kept rather than defaulted — turning a mirror off without deleting the
annotation that says where it points is the whole reason the knob is separate
from the backend.

An out-of-range or unparseable value (`101`, `-5`, `lots`, `50%`) is reported
and falls back to 100. It never disables the mirror.

## Trying it locally

The dev-mode route file supports the same thing, and
`crates/ramjet-ingressd/examples/dev-routes.yaml` ships with one wired up:

```yaml
routes:
  - host: shop.example.com
    path: /
    pathType: Prefix
    backend: web
    mirror:
      backend: shadow
      percent: 100
```

Kill the shadow upstream and watch nothing change about the response you get,
while `ramjet_mirror_failures_total` moves on `:10254/metrics`.
