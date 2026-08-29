# Annotations reference

Every annotation this controller reads, and nothing else. Anything not on this
page is ignored — see [what is not here](#what-is-not-here).

A value that *is* read and cannot be used is
[reported on the object itself](#a-refused-value-says-so-on-the-object), so
finding out does not need pod-log access.

## Two prefixes, and the rule for choosing

Anything ingress-nginx already spells gets the `nginx.ingress.kubernetes.io`
prefix, on purpose. Compatibility is the whole point: an existing cluster should
be able to swap controllers without rewriting every Ingress, so this controller
speaks the annotations people already have — the canary family below is
transcribed from theirs, semantics included.

Anything ingress-nginx has no equivalent for gets `ramjet.dev`. Traffic
mirroring and canary auto-promotion are both in that group: there is no
established spelling to be compatible with, and borrowing their prefix for a key
they do not define would be a claim about portability that is not true. An
operator reading `ramjet.dev/…` on an Ingress knows immediately that moving back
to ingress-nginx loses that behaviour.

## Class

| Annotation | On | Value | Effect |
|---|---|---|---|
| `kubernetes.io/ingress.class` | Ingress | the controller's `--ingress-class`, default `ramjet` | Pre-`IngressClass` way of claiming an Ingress, still ubiquitous. **Decisive when present**, even over `spec.ingressClassName` |
| `ingressclass.kubernetes.io/is-default-class` | IngressClass | `"true"` | Marks this class as the one that claims Ingresses naming no class at all. Case-insensitive, trimmed |

The full claim order is in [Ingress basics](./ingress.md#which-ingresses-this-controller-claims).

## Canary

Transcribed from ingress-nginx, semantics included. Set these on the **canary**
Ingress — a second Ingress with the same host and path as the production one.

| Annotation | Value | Default | Effect |
|---|---|---|---|
| `nginx.ingress.kubernetes.io/canary` | `"true"` | off | Marks this Ingress as the canary half of a pair. Case-insensitive and trimmed; **only `true` enables it** — `1`, `yes` and `on` do not |
| `nginx.ingress.kubernetes.io/canary-weight` | integer | `0` | Share of traffic diverted to the canary, out of `canary-weight-total` |
| `nginx.ingress.kubernetes.io/canary-weight-total` | integer | `100` | Denominator for `canary-weight` |
| `nginx.ingress.kubernetes.io/canary-by-header` | header name | — | `always` → canary, `never` → stable, **anything else falls through** to the next rule |
| `nginx.ingress.kubernetes.io/canary-by-header-value` | string | — | Exact match on that header → canary; no match falls through |
| `nginx.ingress.kubernetes.io/canary-by-header-pattern` | regex | — | Regex on that header, anchored at both ends. **Mutually exclusive** with `canary-by-header-value`; if both are set, the pattern wins |
| `nginx.ingress.kubernetes.io/canary-by-cookie` | cookie name | — | `always`/`never`, with the same fall-through rule |

### Precedence, and what "beats" means

**header > cookie > weight.** The subtlety is that only the literal values
`always` and `never` are decisive. A header that is present but says something
else is *ignored*, and evaluation continues to the next rule. Getting this wrong
makes every request carrying an unrelated header value bypass the weight split.

### Parsing failures are not fatal

An unparseable or negative `canary-weight` is reported and read as `0`. A
fat-fingered weight should not take the Ingress out of service.

A canary with `canary: "true"` and nothing else is **inert** — weight 0, no
header, no cookie — and is reported as such rather than compiled into a rule
that can never fire. That is also true in ingress-nginx; it is just said out
loud here.

```yaml
metadata:
  annotations:
    nginx.ingress.kubernetes.io/canary: "true"
    nginx.ingress.kubernetes.io/canary-weight: "20"
    nginx.ingress.kubernetes.io/canary-by-header: x-canary
```

## Backend protocol

How the data plane talks to the pods behind a Service. Set on the Ingress, and
it applies to every backend that Ingress's rules point at.

| Annotation | On | Value | Default | Effect |
|---|---|---|---|---|
| `nginx.ingress.kubernetes.io/backend-protocol` | Ingress | `HTTP` or `GRPC` | `HTTP` | `GRPC` dials the pods with cleartext HTTP/2 (h2c, prior knowledge). Matched case-insensitively after trimming, as ingress-nginx matches it |

`GRPC` is what makes a gRPC Service work: gRPC is defined in terms of HTTP/2
streams and trailers and has no HTTP/1.1 form, so without this the request would
be downgraded into something the backend cannot parse. With it, the whole
exchange works — unary and streaming, in both directions, with `grpc-status`
arriving in the trailers where the client expects it. The client may speak
HTTP/1.1, HTTP/2, or HTTP/3; the version is translated at this hop.

Nothing about it is gRPC-specific. Any Service that speaks h2c — a plain
HTTP/2 API, a service mesh sidecar — is reached correctly with the same value.

```yaml
metadata:
  annotations:
    nginx.ingress.kubernetes.io/backend-protocol: GRPC
```

### The four values ingress-nginx has that this does not

`GRPCS`, `HTTPS`, `AUTO_HTTP` and `FCGI` are **read, reported, and not
honoured**. The backend stays on HTTP/1.1 and a warning names the value:

```
default/api [InvalidAnnotation]: `nginx.ingress.kubernetes.io/backend-protocol: GRPCS`
is not supported; only `HTTP` and `GRPC` are, and this backend stays on HTTP/1.1
```

`GRPCS` and `HTTPS` need TLS to the upstream, which this data plane does not do
yet; `AUTO_HTTP` needs per-endpoint scheme detection; `FCGI` is not HTTP.
Treating any of them as `HTTP` silently would send cleartext at a port expecting
TLS, with nothing but connection resets to explain it. Refusing to compile the
Ingress would be worse — one namespace owner could take the table out — so the
route serves and the warning is the signal.

### What an h2c backend sees

Two things differ from the HTTP/1.1 path, both forced by HTTP/2 itself:

- **No `Host` header.** HTTP/2 carries the authority in the `:authority`
  pseudo-header, and `:authority` has to name the endpoint because that is what
  keys the upstream connection pool. Sending a `Host` that disagrees with it is
  something [RFC 9113 §8.3.1][rfc9113] lets a server treat as malformed. **The
  client's host name is in `X-Forwarded-Host`**, on this path and the HTTP/1.1
  one alike.
- **No protocol upgrades.** `Connection` and `Upgrade` are forbidden in HTTP/2,
  so a WebSocket handshake is not reconstructed for an h2c backend; it reaches
  the application as an ordinary request. WebSocket over HTTP/2 (RFC 8441
  extended CONNECT) is not implemented. Put WebSocket routes on an `HTTP`
  backend.

[rfc9113]: https://www.rfc-editor.org/rfc/rfc9113#section-8.3.1

### One Service port is one backend

A backend is a Service port, however many Ingresses point at it, so two
Ingresses cannot give the same pods two protocols. If they try, the first claim
in route order wins and the other is reported:

```
default/b [BackendProtocolConflict]: backend default/web:80 is already registered
as `h2c` by another Ingress; this Ingress asked for `http` and was not honoured
```

Split the Service, or annotate both the same way.

### Not on the uring engine

`--engine uring` dials HTTP/1.1 only. A route whose backend is `GRPC` answers
`502` there, naming the engine, rather than being downgraded — see
[Engines](../operations/engines.md).

## Traffic mirroring

`ramjet.dev` prefix: there is no ingress-nginx spelling of this. Set these on
the **production** Ingress. A mirror is a property of the route, and the canary
Ingress is a second opinion about where a share of that route's traffic goes —
not a second route that could have its own shadow.

| Annotation | Value | Default | Effect |
|---|---|---|---|
| `ramjet.dev/mirror-backend` | `namespace/service:port`, or a bare `service:port` in the Ingress's own namespace | — | **Its presence turns mirroring on.** A blank or whitespace-only value reads as absent |
| `ramjet.dev/mirror-percent` | `0`–`100` | `100` | Share of matching requests copied. `0` is kept, not defaulted — turning a mirror off without deleting the annotation that says where it points is the whole reason the knob is separate |
| `ramjet.dev/mirror-host` | hostname | — | `Host` header sent on the copy instead of the client's |

An out-of-range or unparseable `mirror-percent` (`101`, `-5`, `lots`, `50%`) is
reported and falls back to `100`; it never disables the mirror.

**Setting `mirror-backend` on a canary Ingress does nothing**, and says so in a
warning.

`mirror-host` looks cosmetic and is not: a shadow deployment usually answers to
a different name, and a copy carrying the production `Host` can be routed by
whatever sits in front of it — possibly straight back to production, which is
the one outcome a mirror must never produce.

```yaml
metadata:
  annotations:
    ramjet.dev/mirror-backend: shadow/api:80
    ramjet.dev/mirror-percent: "10"
    ramjet.dev/mirror-host: shadow.example.com
```

See [Traffic mirroring](../operations/mirroring.md) for the invariants and the
body cap.

## Canary auto-promotion

`ramjet.dev` prefix. Set these on the **canary** Ingress. Everything but the
opt-in has a default that is safe to run with.

| Annotation | Value | Default | Effect |
|---|---|---|---|
| `ramjet.dev/auto-promote` | `"true"` | `false` | Opts this canary in. Only `true` enables it |
| `ramjet.dev/auto-promote-interval` | `30s`, `5m`, `1h`, or a bare number of seconds | `60s` | One observation window. **Zero is refused**; so is a compound like `1h30m` |
| `ramjet.dev/auto-promote-steps` | comma-separated weights, `1`–`100` | `5,10,25,50,100` | The weights to walk. **Sorted and deduplicated**, so `50,10,100` means step up through 10, 50, 100 rather than promoting to 50 and then demoting to 10. A `0` or a value over `100` anywhere refuses the whole list |
| `ramjet.dev/auto-promote-max-5xx-percent` | float ≥ 0 | `1` | Canary error budget for one window |
| `ramjet.dev/auto-promote-max-latency-factor` | float ≥ `1.0` | `1.5` | Canary mean latency as a multiple of stable's. **Below 1.0 is refused** — it would demand the canary be *faster* than stable to advance, which is a benchmark and not a health check. Exactly `1` is legal |
| `ramjet.dev/auto-promote-min-requests` | integer | `50` | Requests each side needs in a window before the window counts as evidence. **Per window, per side** |
| `ramjet.dev/auto-promote-status` | — | — | **Written by the controller**, not by you: `promoted`, or `rolled-back: <reason>` |

### Every bad value falls back and is reported

A misspelled threshold does not stop the promotion; it uses the default and logs
which key was unusable. The alternative — refusing to promote because one
threshold is misspelled — leaves a canary stuck at its starting weight with no
explanation, which is a worse failure than promoting against a default somebody
can see.

### `auto-promote-status` is a one-way latch

A rollback writes both `auto-promote: "false"` and
`auto-promote-status: "rolled-back: <reason>"`, and the loop refuses any canary
whose status starts with `rolled-back` even if the enable annotation is somehow
still true. Both, because the guard has to survive a restart — the annotation
carries it across a rescheduled pod — and because the two are written in one
patch that could half-fail.

Re-arming is a human decision: clear the status annotation yourself.

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

See [Canary auto-promotion](../operations/canary.md) for the state machine and
the interlocks.

## A refused value says so on the object

Every annotation above falls back rather than failing the Ingress — a
fat-fingered weight must not take a route out of service. The cost of that is
that a refused value goes on sitting there looking applied, so each one also
becomes a **Warning Event on the Ingress that carries it**:

```sh
kubectl describe ingress web-canary
```

```text
Events:
  Type     Reason             Age   From            Message
  ----     ------             ----  ----            -------
  Warning  InvalidAnnotation  2m    ramjet-ingress  `nginx.ingress.kubernetes.io/canary-weight` is not a number; using 0
  Warning  MirrorRejected     2m    ramjet-ingress  a canary Ingress cannot also mirror; the mirror is ignored
```

The `Reason` is the refusal's kind, so it is filterable:

```sh
kubectl get events -A --field-selector reason=CanaryInert
```

| Reason | Means |
|---|---|
| `InvalidAnnotation` | A value could not be parsed and its default was used |
| `CanaryInert` | A canary is configured such that no request can ever reach it |
| `CanaryOrphan` | A canary attached to no production route |
| `CanaryConflict` | Two canaries claimed the same production route |
| `MirrorRejected` | A mirror could not be used and the route is served without it |
| `BackendProtocolConflict` | Two Ingresses asked for different `backend-protocol` on one Service port |

**Only these, and only when they change.** Events are written when an object's
set of refusals differs from the last set written for it, not on every rebuild —
a rebuild happens on every watch event in the cluster, and re-stating an
unchanged complaint would be one Event per Ingress per deploy forever. Fixing
one annotation and breaking another in the same edit is a change, so it is
reported immediately; a cooldown would have swallowed it.

Warnings that are *not* about an annotation value stay in the log, where the
person who can act on them already is: a Service with no endpoints, a TLS Secret
that has not been created, a route another Ingress already claimed. So does
`EndpointsSkipped`, which fires on every healthy rolling update and would train
people to ignore the stream.

RBAC: `events.k8s.io`/`events`/`create`, which the chart's ClusterRole has.
Without it these are skipped at `debug` and the log lines are unaffected.

## What is not here

The vocabulary above is the whole vocabulary. The route table has no rewrite,
header-mutation, rate-limit, session-affinity, or auth rules, so the
corresponding `nginx.ingress.kubernetes.io` annotations are **not read** — they
are not silently accepted either, they are simply absent from the parser.

Those attach to a route when the proxy can act on them. Parsing an annotation
the data plane ignores is worse than not parsing it, because it looks
configured.

If you are migrating, this is the list to diff your Ingresses against.
