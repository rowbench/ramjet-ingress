# ramjet-ingress architecture

A Kubernetes ingress controller with a native Rust data plane. Feature target
is parity with `kubernetes/ingress-nginx`; there is no nginx anywhere in the
design.

## Status

All four crates are built. `ramjet-ingressd` watches the Kubernetes API and
serves what it compiles; `--static-routes` swaps the API server for a file and
changes nothing else about the serving path.

| Crate | State |
|---|---|
| `ramjet-router` — route table, matcher, load balancing | working, tested, benchmarked |
| `ramjet-proxy` — listeners, TLS, HTTP/1.1, HTTP/2, upstreams | working, tested against real sockets |
| `ramjet-proxy` — HTTP/3 over QUIC | experimental, off by default, tested against real UDP sockets |
| `ramjet-engine` — the completion-based data plane | working, tested against real sockets; see [Two engines](#two-engines) |
| `ramjet-controller` — Kubernetes watch, translate, status | working, tested against in-memory objects |
| `ramjet-ingressd` — the daemon | Kubernetes mode and dev mode, on either engine |

What is deliberately absent is in [Limitations](#limitations). The line that
matters operationally: there is no leader election yet, so run **one replica**.

## The thesis: swap a pointer, do not reload

ingress-nginx reacts to a configuration change by regenerating `nginx.conf` and
reloading. A reload forks new workers, drains the old ones, and in the process
resets upstream state and severs connections that were meant to be long-lived.
The cost of a config change is proportional to how much traffic you are
carrying, which is backwards: the busier you are, the more a routine deploy
hurts.

Here the control plane compiles configuration into an immutable `RouteTable`
and publishes it by storing one pointer into an `arc_swap::ArcSwap`. The data
plane does a single atomic load per request and then reads an immutable
snapshot. There is no `RwLock`, no reader-writer contention, no reload, and no
draining.

```
Kubernetes API                    ArcSwap<RouteTable>              worker
     |                                    |                          |
  watch Ingress/Service/Secret            |                     load() ── 1 atomic
     |                                    |                          |
  RouteTableBuilder ──► RouteTable ──► store()                  match_request()
     (pure function)      (immutable)   (one pointer)           (borrows, no alloc)
```

Three properties follow, and they are the point of the design:

- **A publish never blocks a reader.** Writers and readers never share a lock,
  so a rebuild cannot add latency to a request.
- **In-flight requests are unaffected.** A request that loaded generation 7
  holds that `Arc` and finishes against generation 7 even if 8 is published
  mid-flight. Nothing is rewritten under it.
- **Load-balancer state survives the swap.** This is the part that is easy to
  get wrong; see [Counter continuity](#counter-continuity).

## Crate layout

```
crates/
  ramjet-router/      sans-io: route table, matcher, LB selection
  ramjet-proxy/       sockets, rustls, HTTP/1.1 + HTTP/2 + HTTP/3, upstream pools
  ramjet-engine/      the second data plane: a completion-based reactor
  ramjet-controller/  Kubernetes informers, annotation translation, status
  ramjet-ingressd/    the daemon binary
```

`ramjet-engine` depends on `ramjet-proxy`, which is the one dependency edge in
this diagram that is not obvious. It is there so the two engines share *types*
rather than behaviour: one `CertStore`, one `SniResolver`, one PROXY protocol
parser, one mirror queue, one `Handoff`. `ramjet-ingressd` hands both engines
the same `Arc`s, so a Secret rotation reaches whichever one is serving and a
name resolves to the same certificate on both. The cost is that `ramjet-engine`
links hyper, which it does not use; the alternative was a second copy of the
certificate plumbing with its own opportunity to drift.

`ramjet-router` depends on `arc-swap`, `regex`, and `thiserror`. Not on tokio,
not on hyper, not on rustls. It never opens a socket, spawns a task, or reads a
clock. Certificates are opaque handles, randomness is passed in as a `u64`, and
canary decisions take borrowed header values rather than a header collection.
That is what makes the matcher testable against string literals and
benchmarkable without a network.

`ramjet-controller` holds no rustls types either, for the mirror-image reason:
parsing a certificate means a crypto provider, and a crypto provider in the
control plane would mean the translation layer could no longer be unit-tested
against objects built in memory. The daemon is the only crate that depends on
both sides.

## From the API server to the socket

```
Ingress, IngressClass, Service, EndpointSlice, Secret
        │
        │  five watches, one debounced rebuild task
        ▼
 reflector stores ──► ClusterSnapshot ──► translate()   no I/O, no clock
                                               │
                                        CompiledConfig
                                               │  watch channel
                                               ▼
                                    ramjet-ingressd applies it:
                          1. CertStore::publish          handle_id → key
                          2. SharedRouteTable::store_shared    the table
                                               │
                           TLS handshake ◄─────┴─────► match_request
```

Five reflectors mirror the objects that can change a route. Every event they see
pokes a one-slot channel; one rebuild task drains it, waits out a 200 ms
debounce, and then compiles the **current state of the stores** — not the event
that woke it. A fifty-pod rollout produces fifty EndpointSlice events and at
most one rebuild, and each rebuild is built from everything known at that
instant, so a burst of churn costs what a single change costs.

`translate` is a pure function: `ClusterSnapshot` in, `CompiledConfig` out, no
I/O and no clock. That is why class filtering, path precedence, endpoint
resolution, canary merging, and conflict arbitration all have unit tests that
construct API objects in memory and assert on the compiled table.

Rebuilds are **total**. One malformed Ingress, one dangling Secret, or one
unresolvable Service degrades that route and nothing else, and comes back as a
structured warning. The alternative — refusing to build a table containing one
broken object — hands every namespace owner a cluster-wide kill switch.

A publish is suppressed when the compiled digest matches what is already out
there. This matters more than it sounds: the API server re-sends every object on
each watch restart and periodic resync, and without the check each of those
would bump the generation and hand the data plane a table it already has.

### The handoff

`CompiledConfig` is the whole contract between the two halves:

```rust
CompiledConfig {
    table: Arc<RouteTable>,
    certs: Vec<CertMaterial { handle_id: u64, cert_chain_pem, key_pem }>,
}
```

Table and certificates travel in one value so they can never be observed out of
step: an `SniMap` entry always has its material in the same message. The table
is behind an `Arc` because publishing it is a pointer store — the daemon moves
that exact allocation into `SharedRouteTable`, and copying a ten-thousand-route
table per generation to achieve the same thing would be silly.

The daemon applies a generation in **two stores, in this order**:

1. `CertStore::publish` — the whole `handle_id → CertifiedKey` map at once.
2. `SharedRouteTable::store_shared` — the table that references those ids.

Those are two independent `ArcSwap`s, so a handshake can observe a new table
against an older store. Publishing certificates first makes the only possible
skew a store holding a key nothing points at yet, which is invisible. The other
order leaves an `SniMap` entry whose id is missing from the store, and
`SniResolver` answers a missing id with `None` — which rustls turns into a
failed handshake. Every rotation would drop connections for the width of that
gap.

### Certificates are content-addressed

`handle_id` is derived from the Secret's namespace, name, and *contents*, so it
changes if and only if the material changes. The daemon keeps its parsed
`CertifiedKey`s in a map keyed by that id and carries forward every id that
survives a rebuild, parsing only what actually rotated. A cluster with 500
certificates does no X.509 work at all when an unrelated Ingress is edited.

The same property is what makes eviction safe: a key that no longer appears in
the new generation is simply not carried over, and dropping it cannot orphan a
name, because a name that still resolves still names its id.

A certificate that will not parse is logged and skipped, never fatal. TLS for
the names it covers fails until the Secret is fixed; every other host, and all
plaintext traffic, is untouched. Refusing the whole generation would let one
malformed Secret in one namespace take the cluster's routing offline.

### Readiness

`/readyz` is gated on the first **compiled** generation, not on the process
being up. The controller seeds its channel with an empty table at generation 0,
meaning "nothing has been compiled yet", and the daemon flips the readiness flag
only once it has published a generation greater than zero. Without that gate a
rolling update would route traffic to a replica whose table is empty, and every
request in that window is a 404.

The flag is one-way. A later generation never takes a replica back out of
rotation: a table one debounce window stale is far better than 404ing everything
while Kubernetes reroutes.

The two probes answer different questions and are wired accordingly. `/healthz`
is unconditional — a liveness probe that fails restarts the pod, so anything
conditional in it turns a transient dependency problem into a crash loop.

### Observability

The admin listener sits on its own port (`:10254`, the ingress-nginx
convention), not on a reserved path of the data plane. A path on the data plane
is a path an Ingress can claim, so `/metrics` would either shadow somebody's
application route or be shadowed by it — and it would be reachable from the
internet, which is a way to tell an attacker your request rate.

`/metrics` exposes `ramjet_requests_total`, `ramjet_route_misses_total`,
`ramjet_active_connections`, `ramjet_upstream_latency_seconds`,
`ramjet_upstream_{connect_failures,timeouts,retries}_total`,
`ramjet_tls_handshakes_total`, `ramjet_tls_handshake_failures_total`,
`ramjet_route_table_generation` — the last of which is how you tell whether a
replica is actually serving the configuration you think it is — and
`ramjet_pinned`, which says whether that is the case because somebody pulled the
emergency brake.

The admin listener also serves a small JSON API: `/admin/generations`,
`/admin/routes`, and `/admin/rollback`. See
[Time travel and the audit trail](#time-travel-and-the-audit-trail).

Logs go through `tracing` to stderr, `info` by default, filtered with
`RUST_LOG`. The lines worth alerting on are the per-generation publish record
and the warnings from translation: a rejected Ingress, an unresolvable Service,
a Secret that will not parse.

### Shutdown

`SIGTERM` stops the accept loop, closes the listeners so the load balancer looks
elsewhere immediately, and then gives in-flight requests up to 30 seconds to
finish. That is both engines, and with HTTP/2 dispatch on it is both of them
inside one deadline rather than one after the other; [Draining, on a
reactor](#draining-on-a-reactor) has how the uring lane does it without a task
to await. Afterwards the controller task is aborted, which stops all five
watches at once — they live inside that one task precisely so there is a single
place to cancel.

The reverse direction is wired too: if the control plane stops on its own, the
daemon drains and exits non-zero rather than continuing to serve a table that
can never change again. A replica that has quietly stopped being an ingress
controller should show up in `kubectl get pods`, not in a support ticket.

## Time travel and the audit trail

The thesis says a configuration change is one pointer store. Two things follow
that ingress-nginx cannot offer, and this section is both of them.

### The emergency brake

If publishing a generation is a pointer store, republishing an old one is the
same pointer store. So the daemon keeps the last N applied generations —
default 10, `--history-size` — and `POST /admin/rollback {"generation": G}`
puts G back on the wire.

It costs what a normal configuration change costs, and it works when the API
server is the thing that is wrong. Every alternative route to the same
outcome — re-applying the previous Ingress objects, `kubectl rollout undo`,
waiting for a controller to recompile — goes back through the control plane,
which is exactly the component an operator reaches for this lever to route
around.

**A rollback is a pin, not a rewind.** The controller does not stop. It keeps
watching, keeps compiling, and keeps handing generations over; they are recorded
with `published: false` so an operator can see what is being held back, and
nothing reaches the data plane until `DELETE /admin/rollback`, which immediately
publishes the newest one — not the one that was pinned over. Draining the
controller's side matters more than it looks: a pin that stopped reading the
channel would block the rebuild loop, and releasing it would then jump to
whatever was stuck there rather than to the current state of the cluster.

A pinned generation's **certificates go back with its table**, in the same
certificates-then-table order as a first publish, because a table whose `SniMap`
ids are not in the store fails every handshake for the width of the gap.

**The pin dies with the process,** deliberately. Kubernetes is the source of
truth for what this controller serves; a pin is a local override of that, held
in memory, by one replica, because something is on fire right now. Persisting it
would create a second source of truth that survives a restart and answers to
nobody — a pod that comes back after an eviction still serving a generation from
last Tuesday, with no object in the cluster saying why. Fix the Ingress objects,
then release the pin. `ramjet_pinned` is 1 the whole time, so a replica frozen on
purpose is distinguishable from one whose control plane has died.

The memory cost is the tables themselves: the ring holds each generation's
`Arc<RouteTable>` and parsed keys alive instead of letting them drop. That is
roughly a hundred bytes per route per generation, and successive generations
share every `Arc` that did not change — most importantly the certificates, which
are content-addressed and therefore shared by id. Ten generations of a
ten-thousand route cluster is a few megabytes.

The history records generations this replica *applied*, which is not quite every
generation the controller compiled: the channel between them carries the latest
value rather than a queue, so publishes closer together than one pass of the
applier coalesce. A gap in the numbering is generations that were never on the
wire.

`--static-routes` gets the same endpoints with one generation in the ring.
Nothing is special-cased for it.

### What changed, in words

A digest tells you *that* configuration changed, which is all the rebuild loop
needs. It cannot answer the question somebody actually asks, which is *what*
changed. So every publish is diffed against the previous compiled generation:
routes added and removed, routes whose backend or endpoint count moved, hosts
gained and lost, hosts whose certificate material rotated, and a changed default
backend.

The diff is taken over the two **compiled tables**, not over the API objects,
and that is what makes it useful. An Ingress edited from `Prefix: /foo` to
`Prefix: /foo/` compiles to the same route and does not appear; a Deployment
scaling from three pods to five changes no Ingress at all and does. It is a pure
function of two `RouteTable`s, so every category has a unit test built from
tables constructed in memory.

Each publish is written down three ways, for three different readers:

- a **structured `tracing` event on the `audit` target**, so a log pipeline can
  filter to configuration changes and nothing else;
- a **Kubernetes Event on the `IngressClass`** — reason `ConfigApplied`,
  `ConfigPinned`, or `ConfigResumed`, message `"3 routes added, 1 cert rotated
  (gen 41→42)"` — so `kubectl describe ingressclass` answers "what has this
  controller been doing" without pod-log access. Events are written directly
  rather than through kube's `Recorder`, which aggregates same-reason events for
  six minutes and keeps the *first* note: three deploys in a minute would become
  "ConfigApplied ×3" showing only what the first one did, which is precisely the
  information an audit trail exists to keep. RBAC: `events.k8s.io`/`events`,
  `create` and `patch`; without it the Events are skipped at `debug` and nothing
  else changes.
- an optional **`--audit-webhook <url>`**, one fire-and-forget POST of the diff
  as JSON, five second timeout, failures logged. It does not retry, because it
  is a copy and not the record — the log line, the Event, and the ring all
  already have it, and a delivery system with queues and backoff would be a
  thing to debug during exactly the incidents it exists to describe. `http://`
  only; an `https://` URL is refused at startup rather than silently downgraded.

### Per-route counters, and where they are *not*

Each route carries request, 5xx, and upstream-latency counters. They survive a
rebuild the same way load-balancer state does — by identity, not by position —
so adding one Ingress does not reset every neighbour's numbers. A route's
identity is its host, path, path type, and backend; change the backend and it is
a different route for accounting purposes, because its latency is no longer
comparable to what came before.

The hot path pays four relaxed atomic adds to one cache-line-aligned block,
reached by an index the matched rule already carries — no map, no label set, and
no `Arc` clone, because the counters are read through the snapshot the request
already loaded. Measured on the 10,001-route benchmark table
(`cargo bench -p ramjet-router -- route_stats`), the whole per-request sequence
— resolve the block, pick the shard, record a response and an upstream latency —
is **4.9 ns**, against ~24 us for the forwarded request it describes. Matching
and then counting is 26.0 ns where matching alone is 25.2 ns.

Each route has `ROUTE_STAT_SHARDS` (4) of those blocks and a serving runtime
writes only to its own, so a single hot route's counters do not become a
contended line across cores. Four rather than one-per-core because the memory is
`routes × shards × 128` bytes and the coherence win flattens quickly: a
ten-thousand route table costs 5 MB at this number whether the pod has two cores
or ninety-six.

**Per-route data is served as JSON on `/admin/routes` and is never a labelled
Prometheus series.** ingress-nginx exports them, and it is the single most
common reason its metrics endpoint becomes the most expensive request the pod
serves: ten thousand routes means ten thousand series on every scrape, forever,
whether or not anybody looks. `/metrics` gained exactly one series here, and it
is a gauge with no labels.

### The admin API

On the admin listener only (`:10254`), which the chart exposes through a
ClusterIP Service and never through an Ingress or a LoadBalancer.

| Endpoint | What it answers |
|---|---|
| `GET /admin/generations` | every generation applied, newest first, with its diff, digest, counts, and whether it went live |
| `GET /admin/routes` | every route in the serving table, with its counters and its canary split |
| `POST /admin/rollback` | pin a generation. `404` if it is not in the ring, `409` if something is already pinned — and the body says what |
| `DELETE /admin/rollback` | release the pin and publish the newest generation. Idempotent |

There is no authentication and there is not going to be: anything that can reach
this port can already reach the pod's ServiceAccount token. What *is* enforced is
the shape — the mutating endpoint answers to `POST` and `DELETE` and nothing
else, so a link, a browser prefetch, a scraper following URLs, or a health
checker walking paths cannot roll a cluster back by accident.

### Dev mode

`--static-routes <FILE>` reads hosts, paths, backends, canaries, and
certificates from YAML, publishes them once, and never contacts an API server.
It exists because an ingress data plane that can only be exercised inside a
cluster is an ingress data plane nobody exercises — this one can be run, curled,
profiled, and debugged on a laptop.

It is not a production configuration format, and nothing else in the tree parses
YAML. The Kubernetes path builds tables from API objects directly; it does not
render configuration and read it back, which is exactly the round trip that
makes ingress-nginx's behaviour hard to predict from its inputs.

The two modes are mutually exclusive by nature — a file and an API server are
two writers for one route table, and letting both write would make the winner a
race.

## Route table

```rust
RouteTable {
    hosts:          FxHashMap<Box<str>, VirtualHost>,  // exact names
    wildcard_hosts: FxHashMap<Box<str>, VirtualHost>,  // keyed by parent domain
    catch_all:      Option<VirtualHost>,               // Ingress rules with no host
    default_backend: Option<BackendId>,
    backends:       Vec<Backend>,
    stats:          Arc<BackendStats>,
    tls:            SniMap,
    generation:     u64,
}
```

Rules hold a `BackendId` (a `u32` index) rather than a name, so a `PathRule`
stays 40 bytes and a backend's endpoint list is stored once regardless of how
many paths point at it.

### Host lookup

Exact names are a hash map. Wildcards are **also** a hash map, keyed by the
parent domain: `*.example.com` is stored as `example.com`, and a query strips
its first label before looking up. Kubernetes wildcards replace exactly one
left-most label, so this reproduces the rule in a single hash — no suffix trie,
no backtracking.

```
foo.example.com     → strip label → example.com    → hit
foo.bar.example.com → strip label → bar.example.com → miss   (correct)
example.com         → strip label → com             → miss   (correct)
```

A `Host` header arrives in whatever shape the client felt like sending, so it is
normalized first: port stripped, trailing root dot dropped, ASCII lowercased.
The normalizer makes one pass and reports whether the value is *already*
canonical. The overwhelmingly common case — lowercase, no port — borrows a
subslice and copies nothing. Only a header containing uppercase falls through to
a fold into a 253-byte stack buffer. Neither path touches the heap.

### Hashing

The host maps use FxHash, not the standard library's SipHash-1-3. SipHash costs
roughly a nanosecond per byte; on a 25-byte host name that is a fifth of the
per-request budget, spent before the first bucket is probed.

The usual reason to pay it is hash-flooding resistance, and it does not apply
here. Flooding requires the attacker to *insert* colliding keys so some bucket
grows a long chain. Every key in these maps comes from an Ingress object the API
server accepted; a client chooses what it looks *up*, never what is stored. The
most a crafted `Host` header buys is a control-byte collision inside one
SwissTable group — one extra 64-bit comparison before the lookup misses.

## Path matching

Kubernetes defines three path types, and `Prefix` is the one implementations get
wrong. It is not a string prefix; it matches whole path elements.

| Path type | Rule |
|---|---|
| `Exact` | byte equality. `/foo` does not match `/foo/` |
| `Prefix` | element-wise segment prefix. `/foo` matches `/foo` and `/foo/bar`, **not** `/foobar` |
| `ImplementationSpecific` | a regex, following ingress-nginx |

Prefix paths are normalized at build time into a match length: trailing slashes
are stripped, and the root prefix `/` becomes a length of **zero**. That zero is
load-bearing. The test is:

```rust
path[..n] == rule[..n] && (path.len() == n || path[n] == b'/')
```

With `n == 0` the separator check lands on the request path's own leading
slash and always succeeds, so `/` matches everything without a special case.
With `n == 4` and rule `/foo`, the request `/foobar` fails on `path[4] == b'b'`
— which is the whole point.

### Precedence

Host selection happens first, then path matching *within* the selected host.
This is nginx's server-then-location order, and deviating from it would silently
move traffic during a migration from ingress-nginx.

```
1. exact host        ─┐
2. wildcard host      ├─ pick exactly one VirtualHost
3. hostless catch-all ─┘
4. default backend    ── if none of the above claimed the request

within the chosen VirtualHost:
   Exact  >  longest Prefix  >  regex in controller order
```

A request whose host matches exactly but whose path matches nothing falls to the
**default backend** — it does not reconsider the wildcard.

Rules are sorted into that exact order at build time, so matching is a linear
scan that returns the first hit: no candidate comparison, no best-so-far
bookkeeping. Linear looks wrong until you count. A host in a real cluster
carries a handful of paths, and a handful of 40-byte rules is one or two cache
lines that arrive together; a tree would trade those two lines for pointer
chasing. The benchmark below covers the pathological case (`root_prefix_hit`,
which scans every prefix rule on the host before matching `/`).

### Allocation

`RouteTable::match_request` performs **no heap allocation**. The host normalizes
in place or into a stack buffer, lookups borrow, and `MatchResult` holds
references into the table.

This is not a claim in a comment. `tests/no_alloc.rs` installs a counting global
allocator and asserts zero allocations across every path through the matcher —
exact, prefix, wildcard, catch-all, regex, the mixed-case fold, malformed hosts,
canary resolution, SNI resolution, and endpoint selection. The counters are
thread-local, because `cargo test` runs tests concurrently and a shared counter
attributes one test's allocations to another.

## Backends and load balancing

```rust
Backend  { name, endpoints: Vec<Endpoint>, policy: LbPolicy, stats_index: u32, ring }
Endpoint { addr: SocketAddr, weight: u32 }
LbPolicy { RoundRobin, Random, LeastConn }
```

Selection state lives outside the immutable table. `Backend` carries a `u32`
index into a `BackendStats` slab, and `select_endpoint(backend, stats, rng)`
reads it. Everything is a relaxed atomic — these are load-balancing hints, not
synchronization, and nothing else is published through them.

- **RoundRobin** — one `fetch_add` on a cursor, then a remainder.
- **Random** — a remainder over a caller-supplied `u64`. The router draws no
  randomness itself, which keeps it deterministic and dependency-free; the proxy
  holds a per-core generator.
- **LeastConn** — scans per-endpoint in-flight counters. Weights are honoured by
  comparing ratios without dividing: `n_a * w_b < n_b * w_a`.

Non-uniform weights are expanded once per generation into a precomputed rotation
(`ring`), interleaved so consecutive requests spread across endpoints rather
than bursting `weight` of them at one. Uniform weights skip the ring entirely,
because a remainder is cheaper than an indirection and uniform is the common
case. The ring is capped at 4096 slots; a live endpoint is never rounded down to
zero.

An empty endpoint list is **not** a build error. A Service whose pods are all
unready is normal during a rollout, and failing the whole table for it would
turn one bad Deployment into a cluster-wide outage. Selection yields nothing and
the proxy answers 503.

### Counter continuity

Round-robin cursors and in-flight counts must not reset when configuration
changes. Adding one Ingress should not make every backend forget how many
requests it is currently serving — that is the same class of bug as an nginx
reload, just quieter.

So the counters are not stored in the table. They live behind `Arc`s that
successive tables *share*. On rebuild, `BackendStats::rebuild` carries every
surviving counter forward by identity — backend name for cursors, socket address
for in-flight counts:

- endpoint list unchanged → the entire slot is reused, same objects
- endpoint list changed → a new slot, reusing the `Arc<AtomicU32>` for each
  surviving address, with the cursor's value copied

A request that started under generation 7 and finishes under generation 8
therefore decrements the same `AtomicU32` it incremented. `InflightGuard`
borrows the shared counter rather than the table, so it stays correct across a
swap.

## Canary

Canary support matches ingress-nginx annotation semantics. In Kubernetes a
canary is a second Ingress with the same host and path plus
`nginx.ingress.kubernetes.io/canary: "true"`; the controller merges the pair
into one `PathRule` carrying a `CanarySpec`.

Precedence is **header > cookie > weight**. The subtlety is what "beats" means:
only the literal values `always` and `never` are decisive. A header that is
present but says something else is *ignored*, and evaluation continues to the
next rule. Getting this wrong makes every request carrying an unrelated header
value bypass the weight split.

| Annotation | Effect |
|---|---|
| `canary-by-header` | `always` → canary, `never` → stable, anything else → fall through |
| `canary-by-header-value` | exact match → canary; no match falls through |
| `canary-by-header-pattern` | regex, anchored at both ends; mutually exclusive with the above |
| `canary-by-cookie` | `always`/`never`, same fall-through rule |
| `canary-weight` / `canary-weight-total` | the split applied when nothing else was decisive |

Because the crate is sans-io it has no opinion about how headers are stored.
`CanarySpec::header_name()` and `cookie_name()` tell the caller which values to
fetch; `decide(header, cookie, roll)` takes them as borrowed `&str`. No header
collection type leaks into the router and nothing is allocated.

### Canary attribution

Per-route counters split by which backend answered. Each route slot carries a
*second* 128-byte-aligned block per shard, written when the canary took the
request **in addition** to the route's own — not instead of it.

That direction is the whole design. The other arrangement, stable in one block
and canary in the other, would make every existing graph of a route's request
rate step down the moment somebody started a canary, which is exactly the graph
an operator is watching at that moment. Here the totals stay the totals, the
canary block says how much of them was the new backend, and the stable share is
one subtraction. `/admin/routes` reports it as `canary_stats`, `null` on a route
with no canary — an object full of zeroes could not be told apart from a canary
nothing has reached yet, and that distinction is what an automatic promotion is
about to act on.

The cost falls entirely on the diverted request: four more relaxed adds to a
separate cache line, and nothing at all for a route with no canary.

## Traffic mirroring

A route may carry a **mirror**: a second backend that receives a copy of each
sampled request and whose response is thrown away. It is how a rewrite gets
production traffic before it gets production responsibility.

| Annotation | Effect |
|---|---|
| `ramjet.dev/mirror-backend` | `namespace/service:port`, or a bare `service:port` in the Ingress's own namespace. Its presence turns mirroring on. |
| `ramjet.dev/mirror-percent` | share of matching requests copied, `0`–`100`. Default 100. |
| `ramjet.dev/mirror-host` | `Host` header sent on the copy instead of the client's. |

The prefix is `ramjet.dev`, not `nginx.ingress.kubernetes.io`, and that is a
deliberate signal: the canary family above is transcribed from ingress-nginx so
a cluster can swap controllers without rewriting its Ingresses, and there is no
ingress-nginx spelling of this to be compatible with. An operator reading
`ramjet.dev/…` knows immediately that moving back loses the behaviour.

`mirror-host` looks cosmetic and is not. A shadow deployment usually answers to
a different name, and a copy carrying the production `Host` can be routed by
whatever sits in front of it — possibly straight back to production, which is
the one outcome a mirror must never produce.

### The invariant

**A mirror must never make the primary request slower or more likely to fail.**
That is not a goal, it is the property that decides whether the feature can be
switched on in front of real traffic, and every mechanism below exists for it:

- **Nothing is awaited.** The request path hands the copy to a queue and
  returns. It never waits for a connection, a response, or a timeout.
- **The queue is bounded and drops.** One channel per serving runtime, 256 deep,
  entered with `try_send`. Per-runtime so a wedged shadow fills one core's queue
  rather than contending for a shared one; bounded because the alternative turns
  a slow mirror into unbounded memory growth on the pod serving production.
- **Responses are drained and discarded.** Drained rather than dropped so the
  upstream connection returns to the pool instead of being closed, which would
  put a TCP handshake on every mirrored request.
- **Failures are counted, never propagated.** A mirror backend that is down,
  refusing, absent, or catatonic produces a number on `/metrics` and nothing
  else. A five-second deadline, much shorter than the primary's, bounds the one
  thing a slow mirror can still affect: the queue behind it.
- **No in-flight accounting.** A copy does not take the `LeastConn` guard its
  primary does. The guard borrows out of the route table and cannot cross the
  queue, and letting shadow traffic move production's load-balancing decisions
  would be its own kind of leak.

### The body, which is the hard part

Everything above is cheap because a request head is small and already in memory.
A body is neither, and this data plane's whole position on request bodies is
that it does not buffer them.

So the cap is real and small — `--mirror-max-body`, 256 KiB. A request whose
body is *known* empty, which is every `GET`, `HEAD`, `OPTIONS` and `DELETE` and
so the overwhelming majority of ingress traffic, is mirrored with no buffering
at all and keeps its endpoint failover. A request with a body is read up to the
cap; if it fits, both copies get the same `Bytes`. If it does not, the bytes
already read become a **prefix** on the primary's body and the rest keeps
streaming (`ProxyBody::prefixed`), so the upload resumes from wherever the cap
stopped it and the mirror is skipped and counted. The primary is never held
waiting for more than the cap and never fails because of the attempt.

Four counters, because they have four different fixes: `ramjet_mirrored_total`,
`ramjet_mirror_dropped_total` (queue full — raise the shadow's capacity),
`ramjet_mirror_skipped_total` (body over the cap — raise `--mirror-max-body`),
and `ramjet_mirror_failures_total` (the backend refused or did not answer).
Copies carry `X-Mirrored-By: ramjet-ingress`, so a shadow can tell a copy from
the real thing before it decides whether to charge somebody's card.

## Canary auto-promotion

A canary Ingress annotated `ramjet.dev/auto-promote: "true"` is stepped up
automatically on evidence, and pulled to zero on the first sign the evidence has
turned. It is off unless asked for.

| Annotation | Default | Effect |
|---|---|---|
| `ramjet.dev/auto-promote` | `false` | opts this canary in |
| `ramjet.dev/auto-promote-interval` | `60s` | one observation window |
| `ramjet.dev/auto-promote-steps` | `5,10,25,50,100` | the weights to walk, sorted |
| `ramjet.dev/auto-promote-max-5xx-percent` | `1` | canary error budget for a window |
| `ramjet.dev/auto-promote-max-latency-factor` | `1.5` | canary mean latency vs stable's |
| `ramjet.dev/auto-promote-min-requests` | `50` | per window, **per side** |
| `ramjet.dev/auto-promote-status` | — | written by the controller: `promoted`, or `rolled-back: <reason>` |

### The state machine

Every interval, per opted-in canary: take the **window** — this interval's
deltas only, canary side and stable side separately. If either side saw fewer
than `min-requests`, **hold**. Otherwise check the gates: canary 5xx percentage
against `max-5xx-percent`, canary mean latency against stable's times
`max-latency-factor`. A breach is a **rollback** — weight to 0, `auto-promote`
to `"false"`, a status saying why. Otherwise **step** to the next weight, or
**promote** if there is no next weight.

Three things in that are easy to get wrong and are worth naming.

**Holding is not failing.** A canary receiving nothing at 03:00 is a quiet
service, not a broken one. Gating on both sides — not just the canary's — also
matters: a latency comparison against four stable requests is not a comparison.
Rolling back on low traffic would make the feature unusable on anything but the
busiest routes.

**Windows, not lifetimes.** The counters are cumulative and the process may have
been up for a week, so a lifetime error rate cannot move fast enough to catch
anything. Each pass subtracts the previous pass's reading. The first pass after
a step spans the moment the weight changed and so mixes two ratios — deliberate,
and it errs safe, because the older and smaller weight is the one
over-represented.

**Errors are absolute, latency is relative.** An error budget is a number
somebody actually has, so production being on fire is not a licence to promote a
canary that is also on fire. Latency has no such absolute: a service that
legitimately takes two seconds would be un-promotable against a fixed
threshold, so the canary is compared to what it is replacing.

### Interlocks

- **A rollback pin pauses everything.** An operator holding the emergency brake
  has taken manual control of what this replica serves; patching Ingresses
  underneath them would be changing the cluster they are trying to hold still.
- **A rollback is one-way.** It writes `auto-promote: "false"` alongside the
  weight, and the loop refuses any canary whose status says it was rolled back
  even if the annotation is somehow still true. Both, because the guard has to
  survive a restart — the annotation carries it across a rescheduled pod — and
  because the two are written in one patch that could half-fail. A canary
  re-armed automatically after failing once will fail again on the next
  interval, flapping traffic across a broken backend for as long as nobody is
  watching.
- **Reaching the last step is validated before it is accepted.** Stepping to
  100% and immediately declaring victory would mean full traffic never gets a
  single window of scrutiny, so promotion happens on the *next* healthy window
  at the final weight.

Every decision is logged on the `audit` target with its numbers, written as a
Kubernetes Event on the IngressClass (`CanaryStepped`, `CanaryPromoted`,
`CanaryRolledBack` — the last as a `Warning`), and POSTed to `--audit-webhook`.
Holds are `debug` only: on a quiet route they are the normal state, and an Event
per interval per canary would bury the three that matter.

### Where it lives, and why

In `ramjet-ingressd`. It needs two things that are nowhere else in the same
place: the per-route split counters, which are in this process behind an atomic
load, and a Kubernetes client, which the control plane has. `ramjet-proxy` must
not know what an Ingress is and `ramjet-controller` must not know what a socket
is, so the one binary depending on both is where the wire goes — the same
argument that put the rollback-pin bridge there.

The candidates are compiled by the controller into `CompiledConfig.promotions`
and arrive on the generation channel, which the loop reads through a second
receiver. It issues no API reads of its own: the controller has already listed
every Ingress and parsed every annotation, and a loop doing its own `list` would
cost a cluster-wide read every minute, forever, on every installation, whether
or not anybody uses the feature. With nobody opted in the list is empty and the
loop is a timer that does nothing.

The state machine itself is a pure function — `decide(policy, weight, window)` —
with no clock, no cluster and no counters, so the entire decision table is a
unit test. The one cluster effect sits behind a one-method trait, and the real
implementation is a dozen lines.

### Why the backend swap stays human

Reaching 100% means every request is served by the canary backend while the
production Ingress still names the old one. The obvious next step — rewrite
`spec.rules[].backend` and delete the canary Ingress — is deliberately left to a
person, and it looks like the last mile of the same job, so it is worth saying
why it is not.

Everything this loop does is **reversible by writing one number**. Every state
it can reach is a weight, and every weight has an inverse the loop already knows
how to apply; a rollback is the same mechanism as a step. Editing the backend is
a different kind of change: it is the thing the canary was a rehearsal *for*, it
normally comes with deleting an object, and undoing it means reconstructing an
object rather than setting a field. A controller that restructures the resources
an operator wrote, on a timer, is a controller people turn off. So the loop
drives the dial to 100, says so in an Event and in the annotation, and stops.

RBAC: this is the only write this controller makes to an object an operator
authored, and it needs `networking.k8s.io`/`ingresses`/`patch` — spec-level,
because an annotation is metadata and `ingresses/status` cannot carry it. The
patches are server-side applies under the `ramjet-ingress` field manager, forced
because `canary-weight` is normally owned by whoever created the Ingress and
taking ownership is precisely what opting in means. In a GitOps cluster a
reconciler that also claims that field will fight this loop and win on its own
schedule; either exclude `canary-weight` from its managed fields, or do not opt
that Ingress in. Without the RBAC rule, promotion logs a permission error every
interval and changes nothing.

## TLS

`SniMap` resolves a server name to an opaque `CertifiedKeyHandle` using exactly
the same precedence as host routing — exact, single-label wildcard, then a
default certificate. A handshake that picked a different certificate than the
request would later be routed by is a confusing way to fail.

The handle is deliberately opaque. Real `rustls::sign::CertifiedKey` values live
in `ramjet-proxy`, indexed by the handle's id. Keeping rustls out of the router
is what lets the matcher be tested without a key, a socket, or a clock.

## HTTP/3 (experimental)

`--http3` adds a QUIC listener on the TLS listener's port number, in UDP, and
advertises it on every HTTPS response with
`alt-svc: h3=":<port>"; ma=86400`. Off — the default — there is no UDP socket,
no thread, and no header; nothing about the TCP path changes either way.

It is a second **way in**, not a second proxy. A request that arrives over QUIC
is turned into the same `http` crate types the TCP listeners produce and handed
to `forward::handle`, so routing, canary arithmetic, load balancing, header
rewriting, retries, per-route counters, mirroring and the upstream pool are the
ones already in use and cannot drift from them. What the `http3` module owns is
how bytes get on and off the wire, and nothing else.

Two consequences of that reuse are worth stating because they are load-bearing:

- **The certificates are the TLS listener's.** The QUIC crypto configuration is
  built over the *same* `SniResolver` — the same `SniMap` in the same route
  table, the same `CertStore` — so a name resolves to the same certificate over
  UDP as over TCP, and a rotation reaches both at the same instant because it is
  the same two `ArcSwap`s in the same order. A handshake that picked differently
  depending on transport would be a spectacular way to fail.
- **The request body could not reach `forward::handle` as hyper's type.**
  `hyper::body::Incoming` has no public constructor, so `handle` takes the
  crate's own `ProxyBody` and the TCP path converts at the call site. That is
  the whole reason the signature is what it is.

### Deciding whether an HTTP/3 request has a body

HTTP/3 has no `Transfer-Encoding` and no framing outside the stream: a request
has a body if and only if DATA frames arrive before the client finishes the
stream. `content-length` answers it when a client sent one. When none did — and
no `GET` does — the alternative to guessing is one **non-blocking** poll of the
request stream. A client that has already finished it, which is every ordinary
`GET` by the time its packets arrive, is recognised immediately: the body is
known-empty, the request is retryable across endpoints, and the origin sees an
ordinary `GET` rather than one carrying `Transfer-Encoding: chunked`. A client
that has not is not waited for — the poll returns `Pending`, the body streams,
and the first DATA frame goes upstream when it arrives.

### One endpoint, on one runtime

The TCP data plane is one runtime per core with `SO_REUSEPORT` spreading
accepts. The obvious transliteration — N UDP sockets on one port, one quinn
endpoint each — is wrong, and quietly.

The kernel chooses which `SO_REUSEPORT` socket receives a datagram by hashing
its **4-tuple**. A QUIC connection is not identified by its 4-tuple; it is
identified by a connection ID, precisely so it can survive the client's address
changing — a phone moving from wifi to cellular, any NAT rebinding. Under
4-tuple hashing, the moment a client's address changes its packets land on a
socket whose endpoint has never heard of that connection, and the connection
dies. Migration is one of the few things QUIC has that TCP does not, and
sharding this way trades it away.

Doing it properly needs the kernel to steer by connection ID — on Linux, an eBPF
`SO_REUSEPORT` program. So for now there is **one endpoint on one dedicated
thread**, with an upstream pool of its own, and the ceiling that sets is one
core's worth of QUIC crypto, packet handling and proxying. That is stated rather
than measured, and it is the honest reason this is experimental.

### Draining

`SIGTERM` stops the endpoint accepting, and each live connection sends GOAWAY
and then finishes the requests already on it, inside the same grace period the
TCP listeners get.

The in-flight requests are counted here rather than left to h3's own
bookkeeping, and that is not redundancy. `h3::server::Connection::accept` yields
`None` once every request is complete *and* a GOAWAY has been received — the
peer's, not ours. A server that sent GOAWAY and then waited for `None` would be
waiting for the client to hang up, and after a GOAWAY every client is idle by
definition. Every shutdown with an open HTTP/3 connection would burn the whole
grace period and then report a timeout.

### What is not supported

- **No 0-RTT.** `max_early_data_size` is zero, explicitly. Early data is
  replayable by anyone who captured it, and which requests are safe to replay is
  an application's judgement, not an ingress's.
- **No QUIC upstream.** Upstream is HTTP/1.1, as it is for every other
  downstream protocol here.
- **No PROXY protocol**, which is a TCP-stream preamble with no UDP form. The
  client address is the QUIC peer's, from the IP header. `deploy/README.md` has
  what that means behind a balancer that SNATs.
- **No protocol upgrades.** WebSockets over HTTP/3 are RFC 9220 extended
  `CONNECT`, a different mechanism from a `101`; an upstream that answers `101`
  to a request that arrived over QUIC gets the same 502 any half-completable
  upgrade gets.
- **No h3 datagrams, no WebTransport, no server push.**
- **`--engine uring` refuses it**, at startup, because that engine has neither
  TLS nor QUIC.

`ramjet_h3_connections_total`, `ramjet_h3_requests_total` and
`ramjet_h3_handshake_failures_total` are the three series; an HTTP/3 request is
also counted in `ramjet_requests_total` like any other, because it is one.

## Behind an L4 load balancer: the PROXY protocol

A cloud L4 load balancer — AWS NLB, DigitalOcean, Scaleway, GCP passthrough —
forwards TCP without touching the payload, so the connection this process
accepts comes from the balancer and not the client. There is no
`X-Forwarded-For` to read, because at that layer there are no headers at all:
TLS has not been terminated, and on a plaintext listener the balancer never
parsed the request.

`--proxy-protocol` makes the `--http` and `--https` listeners require HAProxy's
PROXY header — **v1** (a text line) or **v2** (binary, TLVs skipped), chosen by
the sender and told apart by the first byte. The address it names replaces
`ConnInfo.remote`, so `X-Forwarded-For`, `X-Real-IP`, and anything that logs a
peer describe the client. `--proxy-protocol-timeout` (default 5s) bounds how
long a sender gets to finish the header.

Three properties are worth stating because they are the ones that are easy to
get wrong:

- **The header is read before the TLS handshake.** That is the order the wire
  has: the balancer speaks the protocol itself and *then* relays the client's
  bytes, so on the HTTPS listener the header arrives ahead of the ClientHello.
  Parsing after the handshake would feed the header to rustls and fail every
  connection.
- **Nothing read past the header is thrown away.** The parser is sans-io and
  reports how many bytes the header occupied; a single read very often returns
  the header *and* the start of a request or a ClientHello, and those bytes are
  replayed to the HTTP or TLS layer intact.
- **A header that names nobody is still consumed.** A v2 `LOCAL` command (a
  balancer health-checking the listener), a v1 `UNKNOWN`, and a v2 `AF_UNSPEC`
  are valid headers carrying no address, and the socket's own peer stands.

**It is required, not optional.** A connection whose first bytes are not a valid
header is dropped, which is what nginx's `proxy_protocol` listener parameter and
HAProxy's `accept-proxy` both do. A permissive fallback would let an attacker
choose per connection whether to be spoofed, which is strictly worse than either
fixed answer — because **the header is the client identity**. Anything that can
reach the listener can claim any address, and every application decision made
from `X-Forwarded-For` follows. So the flag belongs on a listener nothing but
the load balancer can reach, and never on one exposed to the internet.

The failure is deliberately loud once and quiet after: a balancer that is not
sending the header fails *every* connection, so the first rejection on each
serving runtime is a `warn` naming the likely cause and the rest are `debug`. A
line per occurrence would bury the outage under its own logs.

The admin listener never reads a header — Prometheus and the kubelet reach the
pod directly and speak no PROXY protocol, and requiring it there would take
`/metrics` and both probes offline the moment the flag was set.

**Both engines read it, with the same parser.**
`ramjet_proxy::proxy_protocol` is sans-io and incremental, so the uring engine
unwinds the header inside its own connection state machine — ahead of the TLS
record layer, in the same order and with the same "required, not optional"
answer — rather than carrying a second implementation of a trust decision this
important. Everything above is therefore the whole story whichever `--engine`
is serving.

## Two engines

There are two data planes in this binary, and `--engine` picks which one serves.

```
                    --engine hyper            --engine uring
                    ──────────────            ──────────────
  runtime           tokio, one per core       the ramjet reactor, one per core
  I/O               readiness: epoll/kqueue   completion: io_uring, or kqueue
  per request       four syscalls, plus one   four submissions into a ring, and
                    to learn a socket is      one io_uring_enter for a batch
                    ready
```

They are not two implementations of the same idea. `ramjet-proxy` is hyper on
tokio and is what every measurement before this phase was taken on;
`ramjet-engine` exists to answer the question `bench/PROFILE.md` ended on —
59.4% of a request is the four unavoidable syscalls and another 9.1% is finding
out a socket is ready, and getting under that means making fewer of them.

### What they share, and why it is types rather than code

Everything above the socket:

- **the route table.** One `SharedRouteTable`, one `ArcSwap`, one `load_full()`
  per request on both. A generation published while traffic is flowing takes
  effect on the next request on either engine, including the next request on an
  already-open keep-alive connection.
- **the certificate store.** One `CertStore` and one `SniResolver`. A name
  resolves to the same certificate whichever engine answers the handshake, and a
  rotation reaches both at the same instant because it is the same two
  `ArcSwap`s published in the same order.
- **the PROXY protocol parser**, unchanged and unmoved. It was written sans-io
  and incremental, which is exactly what a completion-based reactor needs.
- **the mirror queue.** `Mirror::enqueue` is a `try_send` on a bounded channel,
  so a reactor thread hands a copy to a worker living on tokio without either
  knowing about the other.
- **the generation history**, and so rollback pins, and so `/admin/generations`.

What is *not* shared is the HTTP implementation. `ramjet-engine` has its own
sans-io HTTP/1.1 codec and its own header rewriting, and the target is not
"similar behaviour" but the same bytes on the wire. That duplication is a real
risk, so it is policed rather than hoped about: see [The differential
test](#the-differential-test).

### Where TLS sits

rustls never touches a socket — it moves bytes between two buffers — so
terminating TLS on the reactor needed no driver change at all:

```
  reactor    ciphertext in and out of the socket, and nothing else
  rustls     ciphertext <-> plaintext
  codec      plaintext <-> HTTP/1.1 messages
```

The HTTP state machine below cannot tell which listener a request arrived on
except through `X-Forwarded-Proto`, which is the property that let the whole TLS
lane be added without a second version of anything underneath it.

What TLS costs is the plaintext path's zero-copy relay. Where the plaintext
engine forwards a response body out of the buffer it arrived in, under TLS every
byte is copied at least once in each direction, because rustls reads plaintext
out of its own buffer and writes ciphertext into another. kTLS is what would win
that back by moving the record layer into the kernel; it is a separate piece of
work and is not attempted here.

### One port, two engines

The uring engine speaks HTTP/1.1. Rather than not offering HTTP/2, it offers it
and hands those connections to a hyper engine in the same process.

A `rustls::server::Acceptor` reads the ClientHello and **stops** — before a
`ServerConfig` is chosen, before a byte is written back — and the ALPN list is
readable there. So the decision is made while the connection is still nobody's:

```
  accept ─▶ read ClientHello ─▶ offered h2?
                                   │
                        no ────────┴──────── yes
                        │                     │
                  serve here            hand over: the descriptor, plus every
                  (http/1.1)            byte read from it, to the other engine
```

Handing over a live descriptor is only safe because it is not live at that
moment: the read that produced the ClientHello has completed and nothing has
been submitted since, and the reactor holds one-shot registrations released on
completion, so it has no state for that descriptor at all. The generation is
bumped anyway — a completion arriving after the descriptor left would otherwise
be delivered against whatever number the kernel handed out next, which is a bug
that would appear only under load, in production, as one connection reading
another's bytes.

The socket has had bytes read from it and none written, so replaying the prefix
reconstructs the client's stream exactly. There is no reset and no second
handshake: the ClientHello is answered once, by the engine that can speak what
it asked for. The plaintext listener does the same for the HTTP/2
prior-knowledge preface.

Two engines serving one port means one `/metrics` has to describe both, so the
exposition sums the tokio side's counters into the reactor's. Without that,
dispatch mode reports the HTTP/1.1 half of its traffic and silently omits the
rest.

### Draining, on a reactor

Both engines drain on `SIGTERM`: stop accepting, finish what is in flight,
give up at `--shutdown-grace`. The rules a client can observe are the same on
both — an idle keep-alive connection is closed at once, the in-flight response
carries `Connection: close`, a request counts as in flight until its exchange
ends in *either* direction, and an upgraded tunnel is closed rather than waited
for. What differs is that the reactor has no task to await and no future to
cancel, so the drain had to be built out of what a completion loop has.

It is a flag, a count and a deadline. The core reads the stop flag once, at the
top of its loop, and that turn closes the listeners, closes the idle pooled
upstreams, and classifies every connection exactly once: closed now, or kept and
counted. Counted connections then end through the path they always would have —
`client_keep_alive` is set false, so the response head says `close` and
`finish_response` puts the connection into `Closing` instead of back to `Head`
— and the count comes down in `close()`, the single function through which a
descriptor ever leaves a core. Nothing polls, nothing scans, and there is no
second state machine beside the one already there.

```
  Open ── stop flag ──▶ Draining ── count reaches 0 ──▶ teardown, Ok
                            │
                            └───── grace expires ─────▶ teardown, TimedOut
```

The deadline is checked on the helper's tick, because the tick is the only
thing this reactor has that fires without a peer doing something. `TimedOut`
comes back with the same error kind and the same message
`ramjet_proxy::Server::run` uses, which is what lets `ramjet-ingressd` treat
both engines' shutdowns with one function: report it, exit zero, because a
rolling update that ran long is not a crash.

With dispatch on, the two lanes are told at the same instant rather than one
after the other. Started in sequence their grace periods would add up, and the
second would still be draining well past the `terminationGracePeriodSeconds`
the number was chosen to fit inside.

### Falling back

`io_uring_setup` is blocked by Docker's default seccomp profile, and whether a
given cluster allows it depends on the node image, the container runtime, and
the pod's own profile. So `--engine uring` asks the host before anything binds
— one ring's setup and teardown — and serves on hyper if the answer is no, with
the errno in the log. The ordering is the point: falling back after a listener
is up would mean unbinding ports a load balancer is already using.

`--engine uring-strict` refuses to start instead, for a deployment that would
rather crash-loop visibly than serve on an engine it did not choose.

### The differential test

Two engines that are supposed to be indistinguishable cannot be tested by
asserting either one against a literal — that is a test which keeps passing
after the *other* one drifts. So both are started, driven with byte-identical
requests against byte-identical route tables, and compared on the answer, the
whole rewritten head the upstream received field by field, and the counter
deltas. `/metrics` gets the same treatment: both counter sets through the same
events, and the two strings asserted equal.

The full parity matrix is in [docs/src/operations/engines.md](docs/src/operations/engines.md).

## Performance

Match latency against a table of **1,000 hosts and 10,001 routes** (ten per
host: two exact, seven prefix at varying depth, one regex). Apple M2 Pro
(`arm64`, 12 cores), criterion, 100 samples, median of the confidence interval.
Reproduce with `cargo bench -p ramjet-router`.

| Case | Time | What it costs |
|---|---|---|
| `deep_prefix_hit` | **25.2 ns** | exact host, four-segment prefix — the normal request |
| `exact_hit` | 22.5 ns | exact rules sort first, so this is the cheapest hit |
| `host_miss_default_backend` | 20.6 ns | two failed hashes, then the default backend |
| `wildcard_hit` | 29.7 ns | a failed exact hash plus a parent-domain hash |
| `uppercase_host_fold` | 31.8 ns | the only path that copies, into a stack buffer |
| `regex_hit` | 42.8 ns | full scan past every prefix, then a regex |
| `root_prefix_hit` | 47.3 ns | worst case: scans every prefix rule before matching `/` |

The headline is **25.2 ns for a normal request**, against a 200 ns budget; even
the worst case is 4x under it. For scale, a single uncached main-memory
reference is roughly 80 ns — matching a route costs less than one cache miss.

These are laptop numbers taken on a machine that was not otherwise idle, so
treat them as an order of magnitude rather than a regression baseline; runs on a
quiet machine have come in 5–15% faster across the board. What the benchmark is
really for is the *shape*: matching does not get slower with table size, because
host selection is a hash and a host carries a handful of rules.

`match_request` performs **no heap allocation**, and that is enforced rather
than asserted in a comment — `tests/no_alloc.rs` installs a counting global
allocator and checks every path through the matcher, including the mixed-case
fold, canary resolution, and SNI lookup.

### The data plane: one runtime per core

Route matching is 25 ns; a forwarded request is tens of microseconds. The
matcher was never going to be where the time went, and profiling the whole
request confirmed it — the router and every header rewrite together are about
2% of a request. `bench/PROFILE.md` is that profile.

What it found instead: two thirds of a request is the four syscalls a proxy hop
cannot avoid (read the request, write it upstream, read the response, write it
downstream), and the largest recoverable cost was the runtime moving each
request's work between cores. The same code measured **43% more CPU per
request** on two tokio worker threads than on one — 26.7 us against 18.7 us —
because a request that arrives on one worker, dispatches to an upstream
connection owned by another, and is woken back by a third pays an atomic on a
contended cache line at every crossing.

So the data plane is **shared-nothing across cores**: one `current_thread`
runtime per core, each on its own thread, with the accepted socket handed to one
of them round-robin. A connection stays on its runtime for life, and so do the
upstream connections its requests dispatch to, the pool those come from, and the
timers that bound them. Nothing on the request path is shared between cores
except a handful of relaxed metrics counters and the `ArcSwap` the route table
lives in — and a diagnostic build that removed both measured no difference,
which is how we know the sharing that remains is not costing anything.

The accept loop and the admin listener stay on the process's own runtime, which
is why `ramjet-ingressd` runs `#[tokio::main(worker_threads = 1)]`: it does not
serve traffic. `--worker-threads` overrides the runtime count; the default is
`available_parallelism`, which reads the cgroup CPU limit, so a pod with
`limits.cpu: 2` gets two.

The price is stated where it is paid, in `server.rs`: `--upstream-pool-idle` is
a per-runtime ceiling rather than a process-wide one, and a single very busy
connection cannot be spread across cores. For an ingress carrying many
connections rather than one, that is the right way round.

### The floor, and the second engine

Thread-per-core took the hyper data plane to 99% of nginx's throughput at c64
and the same 23.6 us of CPU per request. What is left is not a tuning problem.
After the change the profile reads: 31.2% `writev`, 28.2% `read`, 9.1% `kevent`,
and about 1% for everything this project wrote. **59.4% of a request is the four
unavoidable syscalls and another 9.1% is finding out a socket is ready** — a
floor set by the I/O model rather than by hyper, tokio, or any function.

Fewer syscalls per request is the only thing under it, and on Linux that means
`io_uring`: submissions batch into a ring and the kernel is entered once for
many of them rather than once per operation, with no readiness poll at all.
That is a different data plane, not a patch to this one, so it is
`crates/ramjet-engine`, selected with `--engine uring`, and the default stays
`hyper`.

The two share the route table, the matcher, the load balancer, the canary
arithmetic and the metrics format, and differ only in how bytes move. Three
things about it are worth recording here because they are properties of the
model rather than of the code:

- **A pooled upstream connection is validated without a syscall.** An idle one
  sits with a read submitted; if the origin closes, that read completes and the
  connection is discarded before anyone can be handed it. When a request does
  take it, the read that was watching for the close becomes the read that
  collects the response — the hot path submits one operation *fewer* than a cold
  one, where a readiness-based pool would have to add a probe.
- **The reactor has no connect and no timer.** A proxy dials outward and has to
  bound how long it waits, and neither operation exists. One shared helper
  thread therefore polls the sockets that are still connecting, with its poll
  timeout doubling as the clock, and tells a core over a pipe the core has an
  ordinary read parked on. The contract that keeps that free of use-after-free
  is that the helper *borrows* a connecting descriptor until it sends exactly
  one note about it, including for a connect that never finishes.
- **Every submission carries a generation.** Closing a descriptor cancels the
  operations on it, but their completions arrive later, by which time the kernel
  may have handed the same number to a new connection. Without the generation
  in the completion tag, a cancelled read from a connection that ended would be
  delivered as input to whoever inherited its number.

What it does not do is as important: no TLS, no HTTP/2, no protocol upgrades,
no Kubernetes mode. Each is refused with a status code and an explanation
naming the other engine, and the same list prints at startup, because a gap
that behaves like a bug in whatever is on the other end is worse than a missing
feature. `bench/engine/RESULTS.md` has the measurement.

## Deliberate divergences from ingress-nginx

- **Regex anchoring.** ingress-nginx emits `location ~* "^<path>"`, a literal
  concatenation. We compile `^(?:<path>)`. The two differ only for a top-level
  alternation, where `^a|b` anchors just the first branch and routes traffic
  nobody intended. Case-insensitivity (`~*`) is preserved.
- **Compiled regexes are size-limited** to 1 MiB. A pathological path should
  fail validation, not silently consume memory in every replica.
- **Host validation is strict.** A `host` containing a port, a path, or a
  misplaced `*` is rejected at build time rather than normalized into a guess.

## Limitations

Known gaps, each with the reason it is a gap rather than a bug.

**No leader election — run `replicas: 1`.** Every replica watches the API server
independently and writes Ingress status independently. Routing is unaffected by
that (each replica compiles the same table from the same objects), but the
status writes race: several controllers server-side-applying the same subtree
under the same field manager will fight over `.status.loadBalancer` if their
`--publish-address` values differ. The fix is a coordination.k8s.io `Lease` and
gating the status writer on holding it — the writer is already isolated behind
one `Option<StatusWriter>`, so it is a contained change. Until then, scale by
making the one replica bigger, and use `--no-status-update` if you must run
more.

**gRPC upstreams answer 502.** gRPC is defined in terms of HTTP/2 streams and
trailers and has no HTTP/1.1 form. Downstream already speaks h2, but the
upstream pool dials HTTP/1.1, so a gRPC request would be silently downgraded
into something the backend cannot parse. Requests with an `application/grpc`
content type are rejected explicitly instead, naming the limitation. Lifting it
means an h2 upstream mode selected per backend from `backend-protocol: GRPC`.

**Upstream is HTTP/1.1 only**, which is the same default ingress-nginx ships and
is transparent for everything except the case above.

**`ExternalName` Services serve 503.** Following a DNS name from the data plane
needs a resolver with TTL handling and re-resolution; pointing at whatever the
name resolved to at compile time would be a stale-address bug waiting for the
first failover.

**The annotation vocabulary is canary and class only.** `RouteTable` has no
rewrite, header-mutation, rate-limit, session-affinity, or auth rules, so the
corresponding `nginx.ingress.kubernetes.io` annotations are not read. Those
attach to `PathRule` when the proxy can act on them; parsing an annotation the
data plane ignores is worse than not parsing it, because it looks configured.

**An `IngressTLS` entry with no `hosts` is skipped.** The controller cannot read
a certificate's SANs to work out which names it covers — that would mean parsing
X.509 in the control plane, which is exactly the dependency the layering split
exists to avoid. `--default-tls-secret` is the supported way to serve a fallback
certificate.

**HTTP/3 is experimental and off by default.** One QUIC endpoint on one
runtime rather than one per core, no 0-RTT, no QUIC upstream, no upgrades, and
no PROXY protocol. Each of those has a reason rather than a TODO; they are in
[HTTP/3 (experimental)](#http3-experimental). The deployment-side constraint is
separate and larger: `alt-svc` advertises the TCP port number, so that port has
to answer UDP through whatever is in front of the pod, and most cloud load
balancers cannot do that. `deploy/README.md` has the per-provider answer.

**No Gateway API.** The target is parity with `kubernetes/ingress-nginx` on the
`networking.k8s.io/v1` Ingress resource.
