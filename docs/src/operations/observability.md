# Observability

Three surfaces on the admin listener (`:10254` by default): a Prometheus page,
two probes, and a small JSON API.

## Endpoints

| Method | Path | What it answers |
|---|---|---|
| `GET` | `/metrics` | Prometheus text exposition |
| `GET` | `/healthz` | Liveness: 200 whenever the process is answering |
| `GET` | `/readyz` | Readiness: 200 once a route table has been published |
| `GET` | `/admin/generations` | The generations this replica has applied, newest first, each with what changed and whether it went live |
| `GET` | `/admin/routes` | Every route in the serving table, with its request, error and upstream-latency counters |
| `POST` | `/admin/rollback` | `{"generation": N}` — republish N and hold publication there |
| `DELETE` | `/admin/rollback` | Release the pin and publish the newest generation. Idempotent |

The two rollback verbs are covered in
[Rollback and the audit trail](./rollback.md), including the
[bearer token](./index.md#the-admin-listener) they need when one is configured.

Both JSON endpoints carry a top-level `"version"`, currently `1`. It exists for
the day a field's *meaning* has to change rather than a field being added —
a discriminator introduced at the same time as the break would be one release
too late to help anyone. Until then, a reader that ignores it is correct, and one
that reads it must treat **absent as version 0**: every build before this one
serves the same shape without the field, and an upgrade is exactly when somebody
is watching. `ramjet-top` parses it and does not branch on it.

## The probes answer different questions

`/healthz` is **unconditional**. A liveness probe that fails restarts the pod,
so anything conditional in it turns a transient dependency problem into a crash
loop.

`/readyz` is gated on the first **compiled** generation, not on the process
being up. The controller seeds its channel with an empty table at generation 0,
meaning "nothing has been compiled yet", and the flag flips only once a
generation greater than zero has been published. Without that gate a rolling
update would route traffic to a replica whose table is empty, and every request
in that window is a 404.

The flag is **one-way**. A later generation never takes a replica back out of
rotation: a table one debounce window stale is far better than 404ing everything
while Kubernetes reroutes.

## Metrics

```text
ramjet_requests_total
ramjet_route_misses_total
ramjet_active_connections
ramjet_upstream_latency_seconds          (_sum, _count)
ramjet_upstream_connect_failures_total
ramjet_upstream_timeouts_total
ramjet_upstream_retries_total
ramjet_tls_handshakes_total
ramjet_tls_handshake_failures_total
ramjet_route_table_generation
ramjet_pinned
ramjet_mirrored_total
ramjet_mirror_dropped_total
ramjet_mirror_skipped_total
ramjet_mirror_failures_total
ramjet_h3_connections_total
ramjet_h3_requests_total
ramjet_h3_handshake_failures_total
```

Two of them deserve naming:

- **`ramjet_route_table_generation`** is how you tell whether a replica is
  actually serving the configuration you think it is.
- **`ramjet_pinned`** is `1` while a rollback is holding publication — so a
  replica frozen on purpose is distinguishable from one whose control plane has
  died.

An HTTP/3 request is counted in `ramjet_requests_total` like any other, because
it is one; the `h3` series are in addition.

### Per-route counters are deliberately not here

`/metrics` gained exactly **one** series for the whole per-route feature, and it
is a gauge with no labels. Per-route data is served as JSON on `/admin/routes`
instead.

ingress-nginx exports per-route series, and it is the single most common reason
its metrics endpoint becomes the most expensive request the pod serves: ten
thousand routes means ten thousand series on every scrape, forever, whether or
not anybody looks.

The cost of counting is not the reason. On the hot path a route's counters are
four relaxed atomic adds to one cache-line-aligned block, reached by an index
the matched rule already carries — no map, no label set, no reference count.
Measured on a 10,001-route table, the whole per-request sequence is **4.9 ns**
against roughly 24 µs for the forwarded request it describes.

## `/admin/routes`

Every route in the serving table with its counters, exactly as served:

```json
{
  "version": 1,
  "generation": 0,
  "routes": [
    {
      "host": "shop.example.com",
      "path": "/api",
      "path_type": "Prefix",
      "backend": "api",
      "endpoints": 2,
      "requests_total": 0,
      "errors_5xx_total": 0,
      "upstream_latency_count": 0,
      "upstream_latency_ms_sum": 0.0,
      "canary": { "backend": "api-next", "weight_percent": 20 },
      "canary_stats": {
        "requests_total": 0,
        "errors_5xx_total": 0,
        "upstream_latency_count": 0,
        "upstream_latency_ms_sum": 0.0
      },
      "mirror": null
    },
    {
      "host": "shop.example.com",
      "path": "/",
      "path_type": "Prefix",
      "backend": "web",
      "endpoints": 1,
      "requests_total": 0,
      "errors_5xx_total": 0,
      "upstream_latency_count": 0,
      "upstream_latency_ms_sum": 0.0,
      "canary": null,
      "canary_stats": null,
      "mirror": { "backend": "shadow", "percent": 100, "host": null }
    }
  ]
}
```

A hostless rule appears with `"host": "*"`.

### Reading `canary_stats`

`canary_stats` is `null` on a route with no canary, and an object of zeroes on a
canary nothing has reached yet. That distinction is deliberate — an object full
of zeroes could not be told apart from the other case, and it is what an
[automatic promotion](./canary.md) is about to act on.

The totals are **totals**. A route's own counters include the requests the
canary answered; `canary_stats` says how much of them was the new backend, and
the stable share is one subtraction.

The other arrangement — stable in one block, canary in the other — would make
every existing graph of a route's request rate step down the moment somebody
started a canary, which is exactly the graph an operator is watching at that
moment.

### Counters survive a rebuild

They are carried forward by identity, so adding one Ingress does not reset every
neighbour's numbers. A route's identity is its **host, path, path type and
backend** — change the backend and it is a different route for accounting
purposes, because its latency is no longer comparable to what came before.

## `/admin/generations`

```json
{
  "version": 1,
  "serving": 0,
  "pinned": null,
  "generations": [
    {
      "generation": 0,
      "applied_at": "2026-08-28T13:31:56Z",
      "published": true,
      "digest": "0000000000000000",
      "routes": 5,
      "hosts": 2,
      "certs": 0,
      "diff": {
        "summary": "5 routes added, 3 hosts added, 1 mirror added, default backend now fallback (gen 0→0)",
        "routes_added": ["shop.example.com /api -> api", "…"],
        "routes_removed": [],
        "backends_changed": [],
        "hosts_added": ["shop.example.com", "…"],
        "hosts_removed": [],
        "certs_rotated": [],
        "mirrors_added": ["shop.example.com / -> shadow (100%)"],
        "mirrors_removed": []
      }
    }
  ]
}
```

`published: false` marks a generation the controller compiled while a rollback
pin was held — it was recorded, and it never reached the wire.

The diff is taken over the two **compiled tables**, not over the API objects,
and that is what makes it useful. An Ingress edited from `Prefix: /foo` to
`Prefix: /foo/` compiles to the same route and does not appear; a Deployment
scaling from three pods to five changes no Ingress at all and does.

## `ramjet-top`

The admin port reports counters, and the question you usually have is about
rates. `ramjet-top` polls all three endpoints, differences the counters, and
draws them.

```text
╭ ramjet-top ─ http://127.0.0.1:10254 ───────────────────────────────────────────────────────╮
│gen 0  routes 5  gens 1  conns 0                                   rps · last 6 polls · peak 420│
│rps 103.9  5xx 0.00%  upstream 0.6  up 5s          █                                        │
│                                                   █▃▄▄▄▃                                   │
╰────────────────────────────────────────────────────────────────────────────────────────────╯
╭ routes 5 ──────────────────────────────────────────────────────────────────────────────────╮
│HOST                   PATH          TYPE     BACKEND        EPS  RPS       5XX     ms   CANARY
│shop.example.com       /             Prefix   web              1      52.0   0.00%   0.6 -
│shop.example.com       /api          Prefix   api              2      52.0   0.00%   0.6 20%→api-next
│*                      /status       Prefix   web              1      0.00       -     - -
│*.example.com          /             Prefix   web              1      0.00       -     - -
╰ sorted by rps desc ────────────────────────────────────────────────────────────────────────╯
 ● live · polling every 1s
 q quit  Tab generations  r rps  e 5xx  l latency  h host  / filter  g refresh
```

```sh
# The default target is the conventional admin port, 127.0.0.1:10254.
cargo run -p ramjet-top

# Anywhere else. A bare host:port is fine; it gets an http:// scheme.
ramjet-top 10.0.0.5:10254
ramjet-top --url http://10.0.0.5:10254

# Against a pod, through a port-forward.
kubectl port-forward -n ingress ds/ramjet-ingress 10254:10254 &
ramjet-top localhost:10254

# Poll faster, or slower.
ramjet-top -i 250ms
ramjet-top --interval 5s

# Somebody else's cluster: watch, but do not touch.
ramjet-top --read-only

# One shot, for a script, a CI log, or an incident channel.
ramjet-top --once
ramjet-top --json | jq '.routes.routes[] | select(.errors_5xx_total > 0)'
```

`--once` prints an aligned text table and exits: no terminal required, sorted by
host and path so two runs are diffable, and reporting **cumulative counters
rather than rates**, because a rate is a difference between two polls and this
mode does one. `--json` dumps the merged snapshot — both admin responses
verbatim plus the series read out of `/metrics` — and implies `--once`.

Exit status is `0` on success, `1` if the daemon could not be reached, and `2`
if the command line was wrong.

### Keys

| Key | Does |
|---|---|
| `q`, `Ctrl-C` | Quit. Restores the terminal, including after a panic |
| `Tab` | Switch between the routes table and the generation timeline |
| `r` `e` `l` `h` | Sort routes by rps, 5xx rate, latency, host. The same key again reverses |
| `/` | Filter routes. Substring, case-insensitive, over host, path, backend and type |
| `Enter` | In the filter: keep it. In the timeline: expand the generation's diff |
| `Esc` | Collapse a diff, then clear the filter, then clear the selection |
| `j` `k`, `↑` `↓` | Move the selection |
| `PgUp` `PgDn`, `Home` `End` | Move further, and to the ends |
| `g` | Poll now, without waiting for the tick |
| `p` | Pin traffic to the selected generation. Asks first |
| `u` | Release the pin. Asks first |

`p` and `u` are the emergency brake — they drive `POST`/`DELETE
/admin/rollback`. Both need a `y` to confirm, anything else cancels, and
`--read-only` refuses them outright and stops advertising them.

### What the numbers mean

Everything the server exports is cumulative and everything on screen is a rate,
so the interesting part is the subtraction. Three things make it harder than it
looks, and all three are handled:

- **Counters restart.** A removed and re-added route, or a restarted data plane,
  drops a counter below the value held from last poll. Every subtraction
  saturates at zero, so a restart reads as `0.00` rather than as eighteen
  quintillion requests per second.
- **Routes are not rows.** The table is rebuilt every generation, so "the same
  route" is keyed on host, path and path type — deliberately *not* on the
  backend, because a backend swap is the most interesting moment to keep
  watching a route through.
- **A new route has no rate.** Dividing a lifetime counter by one poll interval
  reports an hour's traffic as if it happened this second. New routes show `-`
  for one interval, are flagged green, and report a real rate from the next
  poll.

The interval divided by is the **measured** gap between polls, from a monotonic
clock — not `--interval`. A poll that took 900 ms because the server was busy
would otherwise inflate every rate on screen at the worst possible moment.

Latency is a **windowed** mean: the delta of the sum over the delta of the
count. On a process that has been up a week, a lifetime mean cannot move, and an
upstream that just started taking two seconds would not show up in it at all.

### When the daemon goes away

The last good data stays on screen, dimmed and marked `STALE`, with the status
line saying how long ago it was true and why the poll failed. It never clears
the screen to print a connection error: the moment the daemon becomes
unreachable is the moment its last known state is most worth looking at.

When it comes back, the rate reported for the gap is the true average across it
— 600 requests over a 60-second outage is `10/s`, not `600/s`.

## Logs

Through `tracing` to stderr, `info` by default, filtered with `RUST_LOG`. The
lines worth alerting on are the per-generation publish record on the `audit`
target and the warnings from translation: a rejected Ingress, an unresolvable
Service, a Secret that will not parse.

## Events

Not everything worth knowing needs pod-log access, and the things an Ingress's
author can act on should not.

| Where | What lands there |
|---|---|
| `kubectl describe ingressclass ramjet` | `ConfigApplied`, `ConfigPinned`, `ConfigResumed` — one per published generation, rollback, and resume |
| `kubectl describe ingress <canary>` | `CanaryStepped`, `CanaryPromoted`, `CanaryRolledBack` — [automatic promotion](canary.md#where-the-decisions-show-up) |
| `kubectl describe ingress <any>` | A `Warning` per [refused annotation value](../configuration/annotations.md#a-refused-value-says-so-on-the-object) |

The split is by what the Event is about: a compiled generation belongs to no
single Ingress, and a promotion decision or a refused value belongs to exactly
one. Per-object Events are written only when what they say changes, so a steady
broken state costs one Event rather than one per rebuild.

## Which generation reached which Ingress

Every managed Ingress carries
[`ramjet.dev/observed-generation`](../configuration/annotations.md#written-by-the-controller),
the compiled generation that last included it:

```sh
kubectl get ingress -A -o custom-columns=\
NS:.metadata.namespace,NAME:.metadata.name,GEN:'.metadata.annotations.ramjet\.dev/observed-generation'
```

That is what the **controller compiled**. `/admin/routes`'s top-level
`generation` is what a **replica is serving**. They agree in the steady state and
diverge in exactly one place — while a
[rollback pin](rollback.md#a-rollback-is-a-pin-not-a-rewind) is held, the
controller keeps compiling and annotating while the data plane stays where it was
put. So the pair is a useful diagnostic on its own: annotation ahead of
`/admin/routes` means something is holding publication back, and the same
annotation lagging its neighbours means one Ingress stopped being included.
