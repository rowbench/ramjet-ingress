# Rollback and the audit trail

The thesis says a configuration change is one pointer store. Two things follow
that a reload-based controller cannot offer, and this page is both of them.

## The emergency brake

If publishing a generation is a pointer store, republishing an old one is the
same pointer store. So the daemon keeps the last N applied generations — default
10, `--history-size` — and putting one back on the wire costs what a normal
configuration change costs.

```sh
# What has been applied, and what changed
curl :10254/admin/generations

# Put 41 back on the wire, now
curl -XPOST :10254/admin/rollback -d '{"generation": 41}'

# Release, and jump to the newest
curl -XDELETE :10254/admin/rollback
```

| Response | Meaning |
|---|---|
| `200` | Pinned |
| `401` | The listener was started with `--admin-token-file` and this request carried no usable token |
| `404` | That generation is not in the history |
| `409` | Something is already pinned — and the body says what |

`DELETE` is idempotent.

Where the daemon was started with `--admin-token-file` — the chart's
`controller.adminToken.secretName` — both verbs need the token, and the `GET`
above does not:

```sh
TOKEN=$(kubectl -n ramjet-ingress get secret ramjet-admin \
  -o jsonpath='{.data.token}' | base64 -d)
curl -XPOST -H "Authorization: Bearer $TOKEN" \
  :10254/admin/rollback -d '{"generation": 41}'
```

This is the request worth authenticating: it is the one thing an arbitrary pod
in the cluster could otherwise send to change what every replica serves. See
[the admin listener](index.md#the-admin-listener).

**It works when the API server is the thing that is wrong.** Every alternative
route to the same outcome — re-applying the previous Ingress objects,
`kubectl rollout undo`, waiting for a controller to recompile — goes back
through the control plane, which is exactly the component an operator reaches
for this lever to route around.

`ramjet-top` drives both verbs from the generation timeline: `p` to pin the
selected generation, `u` to release. Both ask for a `y` first, and
`--read-only` refuses them outright. Against a daemon with a token, give it
`--token-file <PATH>` (or `RAMJET_TOP_TOKEN_FILE`); everything it polls is a
`GET` and needs none.

## A rollback is a pin, not a rewind

The controller does not stop. It keeps watching, keeps compiling, and keeps
handing generations over; they are recorded with `published: false` so you can
see what is being held back, and nothing reaches the data plane until the pin is
released — at which point it publishes **the newest** generation, not the one
that was pinned over.

Draining the controller's side matters more than it looks: a pin that stopped
reading the channel would block the rebuild loop, and releasing it would then
jump to whatever was stuck there rather than to the current state of the
cluster.

A pinned generation's **certificates go back with its table**, in the same
certificates-then-table order as a first publish, because a table whose TLS ids
are not in the store fails every handshake for the width of the gap.

`ramjet_pinned` is `1` the whole time.

## The pin dies with the process, deliberately

Kubernetes is the source of truth for what this controller serves. A pin is a
local override of that, held in memory, by one replica, because something is on
fire right now.

Persisting it would create a second source of truth that survives a restart and
answers to nobody — a pod that comes back after an eviction still serving a
generation from last Tuesday, with no object in the cluster saying why.

**Fix the Ingress objects, then release the pin.**

## What it costs to keep the history

The ring holds each generation's route table and parsed keys alive instead of
letting them drop: roughly a hundred bytes per route per generation. Successive
generations share everything that did not change — most importantly the
certificates, which are content-addressed and therefore shared by id.

Ten generations of a ten-thousand route cluster is a few megabytes.

The history records generations this replica **applied**, which is not quite
every generation the controller compiled: the channel between them carries the
latest value rather than a queue, so publishes closer together than one pass of
the applier coalesce. **A gap in the numbering is generations that were never on
the wire.**

`--static-routes` gets the same endpoints with one generation in the ring.
Nothing is special-cased for it — rolling back to it is a no-op that republishes
what is already serving.

## What changed, in words

A digest tells you *that* configuration changed, which is all the rebuild loop
needs. It cannot answer the question somebody actually asks, which is *what*
changed.

So every publish is diffed against the previous compiled generation: routes
added and removed, routes whose backend or endpoint count moved, hosts gained
and lost, hosts whose certificate material rotated, mirrors added and removed,
and a changed default backend.

The diff is taken over the two **compiled tables**, not over the API objects,
and that is what makes it useful:

- An Ingress edited from `Prefix: /foo` to `Prefix: /foo/` compiles to the same
  route and **does not appear**.
- A Deployment scaling from three pods to five changes no Ingress at all and
  **does**.

## Three ways it is written down

Each publish is recorded for three different readers.

### A structured `tracing` event on the `audit` target

So a log pipeline can filter to configuration changes and nothing else.

```text
INFO audit: 5 routes added, 3 hosts added, 1 mirror added, default backend now fallback (gen 0→0)
  event="config" generation=0 published=true routes_added=5 routes_removed=0
  backends_changed=0 hosts_added=3 hosts_removed=0 certs_added=0 certs_removed=0
  certs_rotated=0 mirrors_added=1 mirrors_removed=0 default_backend_changed=true
```

### A Kubernetes Event on the `IngressClass`

Reason `ConfigApplied`, `ConfigPinned`, or `ConfigResumed`, with a message like
`"3 routes added, 1 cert rotated (gen 41→42)"` — so
`kubectl describe ingressclass` answers "what has this controller been doing"
without pod-log access.

```sh
kubectl describe ingressclass ramjet
```

Events are written directly rather than through kube's `Recorder`, which
aggregates same-reason events for six minutes and keeps the *first* note: three
deploys in a minute would become "ConfigApplied ×3" showing only what the first
one did, which is precisely the information an audit trail exists to keep.

RBAC: `events.k8s.io` / `events`, `create` and `patch`. The chart's ClusterRole
has it. Without it the Events are skipped at `debug` and nothing else changes.

### An optional webhook

```sh
--audit-webhook http://collector.observability.svc:8080/ingress-audit
```

One fire-and-forget POST of the diff as JSON, five second timeout, failures
logged. It does not retry, because it is a copy and not the record — the log
line, the Event, and the ring all already have it, and a delivery system with
queues and backoff would be a thing to debug during exactly the incidents it
exists to describe.

`http://` only. An `https://` URL is **refused at startup** rather than silently
downgraded, because the control plane does not carry a TLS client for this;
point it at a collector inside the cluster.

[Canary auto-promotion](./canary.md) decisions go down the same three channels.

## Interaction with auto-promotion

**A rollback pin pauses automatic promotion entirely.** An operator holding the
emergency brake has taken manual control of what this replica serves; patching
Ingresses underneath them would be changing the cluster they are trying to hold
still.
