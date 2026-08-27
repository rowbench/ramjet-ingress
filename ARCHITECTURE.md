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
| `ramjet-controller` — Kubernetes watch, translate, status | working, tested against in-memory objects |
| `ramjet-ingressd` — the daemon | Kubernetes mode and dev mode |

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
  ramjet-proxy/       sockets, rustls, HTTP/1.1 + HTTP/2, upstream pools
  ramjet-controller/  Kubernetes informers, annotation translation, status
  ramjet-ingressd/    the daemon binary
```

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
`ramjet_tls_handshakes_total`, `ramjet_tls_handshake_failures_total`, and
`ramjet_route_table_generation` — the last of which is how you tell whether a
replica is actually serving the configuration you think it is.

Logs go through `tracing` to stderr, `info` by default, filtered with
`RUST_LOG`. The lines worth alerting on are the per-generation publish record
and the warnings from translation: a rejected Ingress, an unresolvable Service,
a Secret that will not parse.

### Shutdown

`SIGTERM` stops the accept loop, closes the listeners so the load balancer looks
elsewhere immediately, and then gives in-flight requests up to 30 seconds to
finish. Afterwards the controller task is aborted, which stops all five watches
at once — they live inside that one task precisely so there is a single place to
cancel.

The reverse direction is wired too: if the control plane stops on its own, the
daemon drains and exits non-zero rather than continuing to serve a table that
can never change again. A replica that has quietly stopped being an ingress
controller should show up in `kubectl get pods`, not in a support ticket.

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

## TLS

`SniMap` resolves a server name to an opaque `CertifiedKeyHandle` using exactly
the same precedence as host routing — exact, single-label wildcard, then a
default certificate. A handshake that picked a different certificate than the
request would later be routed by is a confusing way to fail.

The handle is deliberately opaque. Real `rustls::sign::CertifiedKey` values live
in `ramjet-proxy`, indexed by the handle's id. Keeping rustls out of the router
is what lets the matcher be tested without a key, a socket, or a clock.

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

**No PROXY protocol** on the listeners, so a replica behind a TCP load balancer
that does not preserve the client IP will attribute every request to the load
balancer in `X-Forwarded-For`.

**An `IngressTLS` entry with no `hosts` is skipped.** The controller cannot read
a certificate's SANs to work out which names it covers — that would mean parsing
X.509 in the control plane, which is exactly the dependency the layering split
exists to avoid. `--default-tls-secret` is the supported way to serve a fallback
certificate.

**No Gateway API.** The target is parity with `kubernetes/ingress-nginx` on the
`networking.k8s.io/v1` Ingress resource.
