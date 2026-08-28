# Ingress basics

The resource is `networking.k8s.io/v1` `Ingress`, and the semantics follow
ingress-nginx wherever there is a choice to be made — including the parts that
look like historical accidents, because deviating from them would silently move
traffic during a migration.

```yaml
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata:
  name: shop
  namespace: prod
spec:
  ingressClassName: ramjet
  rules:
    - host: shop.example.com
      http:
        paths:
          - path: /api
            pathType: Prefix
            backend:
              service:
                name: api
                port:
                  number: 80
```

## Which Ingresses this controller claims

Getting this wrong in either direction is a production incident: claim too much
and you fight another controller for the same hostnames, claim too little and
traffic silently 404s.

The check runs in this order:

1. **`kubernetes.io/ingress.class` on the Ingress.** If present, it is
   **decisive** — even if `spec.ingressClassName` says something else. Ours if
   the value equals `--ingress-class` (default `ramjet`), another controller's
   otherwise. This is not the order the Ingress API documents, but it is what
   ingress-nginx does, and an object that sets both is asking a compatibility
   question rather than a spec-compliance one.
2. **`spec.ingressClassName`** naming an `IngressClass` whose
   `spec.controller` is `ramjet.dev/ingress` → ours.
3. **`spec.ingressClassName`** naming an `IngressClass` that exists but belongs
   to someone else → not ours, silently.
4. **`spec.ingressClassName`** naming a class that does not exist at all → not
   ours, and logged. A dangling name is a typo or a missing manifest; either way
   the Ingress serves nothing and nobody would otherwise be told why.
5. **No class named at all** → ours only if one of *our* `IngressClass` objects
   carries `ingressclass.kubernetes.io/is-default-class: "true"`. The chart sets
   that from `ingressClass.isDefaultClass`, default `false`.

Because an Ingress naming another class is invisible to this replica, running
alongside ingress-nginx during a migration is safe.

## Path types

Kubernetes defines three, and `Prefix` is the one implementations get wrong. It
is not a string prefix; it matches whole path elements.

| `pathType` | Rule |
|---|---|
| `Exact` | Byte equality. `/foo` does not match `/foo/` |
| `Prefix` | Element-wise segment prefix. `/foo` matches `/foo` and `/foo/bar`, **not** `/foobar` |
| `ImplementationSpecific` | A regex, following ingress-nginx |

An unrecognised `pathType` is treated as `ImplementationSpecific` and warned
about, rather than rejected.

Prefix paths are normalized at build time into a match length: trailing slashes
are stripped, and the root prefix `/` becomes a length of zero — which is what
makes `/` match everything without a special case.

### Regex paths

`ImplementationSpecific` compiles the path as a case-insensitive regex, with two
deliberate divergences from ingress-nginx:

- **Anchoring.** ingress-nginx emits `location ~* "^<path>"`, a literal
  concatenation. This compiles `^(?:<path>)`. The two differ only for a
  top-level alternation, where `^a|b` anchors just the first branch and routes
  traffic nobody intended.
- **Size.** Compiled regexes are limited to 1 MiB. A pathological path should
  fail validation, not silently consume memory in every replica.

## Hosts, wildcards, and precedence

A wildcard host replaces exactly **one** left-most label, which is the
Kubernetes rule:

```text
*.example.com  matches  foo.example.com
               not      foo.bar.example.com
               not      example.com
```

Host validation is strict: a `host` containing a port, a path, or a misplaced
`*` is rejected at build time rather than normalized into a guess.

Host selection happens first, then path matching *within* the selected host.
This is nginx's server-then-location order.

```text
1. exact host        ─┐
2. wildcard host      ├─ pick exactly one virtual host
3. hostless catch-all ─┘
4. default backend    ── if none of the above claimed the request

within the chosen host:
   Exact  >  longest Prefix  >  regex in controller order
```

The subtle one: **a request whose host matches exactly but whose path matches
nothing falls to the default backend.** It does not reconsider the wildcard.

A rule with no `host` at all serves every name not claimed by an exact or
wildcard entry.

## The default backend

Requests matching no rule are 404s unless a default backend is set. There are
two ways to set one, and the flag is the replica-wide answer:

```sh
--default-backend kube-system/notfound:8080
```

as `namespace/name:port`. A malformed value is rejected at startup rather than
at the first unmatched request. An Ingress may also carry
`spec.defaultBackend`.

## Backends and load balancing

A backend is a Service, resolved through its `EndpointSlice`s to a list of
addresses. Three policies exist:

| Policy | How it selects |
|---|---|
| `roundRobin` | One atomic increment on a cursor, then a remainder |
| `random` | A remainder over a per-core random number |
| `leastConn` | Scans per-endpoint in-flight counts; weights compared as ratios without dividing |

Non-uniform weights are expanded once per generation into a precomputed
rotation, interleaved so consecutive requests spread across endpoints rather
than bursting `weight` of them at one. Uniform weights skip it entirely. A live
endpoint is never rounded down to zero.

**An empty endpoint list is not a build error.** A Service whose pods are all
unready is normal during a rollout, and failing the whole table for it would
turn one bad Deployment into a cluster-wide outage. Selection yields nothing and
the proxy answers 503.

### Counters survive a rebuild

Round-robin cursors and in-flight counts are not stored in the route table. They
live behind shared references that successive tables carry forward **by
identity** — backend name for cursors, socket address for in-flight counts — so
adding one Ingress does not make every backend forget how many requests it is
currently serving. A request that started under generation 7 and finishes under
generation 8 decrements the same counter it incremented.

That is the same class of bug as an nginx reload, just quieter, and it is why
the mechanism exists.

## When one object is broken

Rebuilds are **total**: the controller compiles the current state of every
watched object, not the event that woke it. One malformed Ingress, one dangling
Secret, or one unresolvable Service degrades that route and nothing else, and
comes back as a structured warning.

The alternative — refusing to build a table containing one broken object — hands
every namespace owner a cluster-wide kill switch.

Warnings worth alerting on: a rejected Ingress, an unresolvable Service, a
Secret that will not parse. They go to stderr through `tracing`, filtered with
`RUST_LOG`.

## How fast a change lands

Five watches (Ingress, IngressClass, Service, EndpointSlice, Secret) funnel into
one rebuild task with a **200 ms debounce**. A fifty-pod rollout produces fifty
EndpointSlice events and at most one rebuild, and each rebuild is built from
everything known at that instant — so a burst of churn costs what a single
change costs.

A publish is suppressed when the compiled digest matches what is already
serving. The API server re-sends every object on each watch restart and periodic
resync, and without that check each of those would bump the generation and hand
the data plane a table it already has.

Measured end to end — `kubectl apply` to the first request the data plane
answers correctly — that is a median of **363 ms** on an empty cluster and
**507 ms** with 500 routes already loaded. See
[Performance](../performance.md#propagation-latency).

## Status writeback

By default the controller writes the ingress address into every managed
Ingress's `.status.loadBalancer`. The address comes from `--publish-service`
(a Service whose own status supplies it, which the chart points at itself) or
`--publish-address` (a literal). `--no-status-update` turns it off entirely.

Status is advertising, not configuration: routing is unaffected either way.

**This is the part that does not tolerate a second replica.** See
[Limitations](../limitations.md).

## Unsupported backend shapes

Two Service shapes are compiled but answer an error rather than routing, each
for a reason rather than a TODO:

- **`ExternalName` Services serve 503.** Following a DNS name from the data
  plane needs a resolver with TTL handling and re-resolution; pointing at
  whatever the name resolved to at compile time would be a stale-address bug
  waiting for the first failover.
- **gRPC upstreams answer 502.** Upstream is HTTP/1.1, and gRPC has no HTTP/1.1
  form. Requests with an `application/grpc` content type are rejected
  explicitly, naming the limitation, rather than being silently downgraded into
  something the backend cannot parse.
