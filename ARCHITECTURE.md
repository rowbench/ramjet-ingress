# ramjet-ingress architecture

A Kubernetes ingress controller with a native Rust data plane. Feature target
is parity with `kubernetes/ingress-nginx`; there is no nginx anywhere in the
design.

## Status

Phase 1. Only `ramjet-router` is implemented. The other three crates are
compiling skeletons that document their intended API and nothing more.

| Crate | State |
|---|---|
| `ramjet-router` — route table, matcher, load balancing | working, tested, benchmarked |
| `ramjet-proxy` — listeners, TLS, HTTP/1.1, HTTP/2, upstreams | stub |
| `ramjet-controller` — Kubernetes watch, translate, status | stub |
| `ramjet-ingressd` — daemon binary | prints its version |

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

## Measurements

Table of **1,000 hosts and 10,001 routes** (ten per host: two exact, seven
prefix at varying depth, one regex). Apple M2 Pro, `cargo bench`, criterion,
100 samples. Reproduce with `cargo bench -p ramjet-router`.

| Case | Time | What it costs |
|---|---|---|
| `deep_prefix_hit` | **22.2 ns** | exact host, four-segment prefix — the normal request |
| `exact_hit` | 20.8 ns | exact rules sort first, so this is the cheapest hit |
| `host_miss_default_backend` | 19.1 ns | two failed hashes, then the default backend |
| `uppercase_host_fold` | 28.0 ns | the only path that copies, into a stack buffer |
| `wildcard_hit` | 28.9 ns | a failed exact hash plus a parent-domain hash |
| `regex_hit` | 40.9 ns | full scan past every prefix, then a regex |
| `root_prefix_hit` | 44.6 ns | worst case: scans every prefix rule before matching `/` |

The headline number is 22.2 ns, against a 200 ns budget. Even the worst case is
4x under it. For scale, a single uncached main-memory reference is roughly 80 ns
— matching a route costs less than one cache miss.

## Deliberate divergences from ingress-nginx

- **Regex anchoring.** ingress-nginx emits `location ~* "^<path>"`, a literal
  concatenation. We compile `^(?:<path>)`. The two differ only for a top-level
  alternation, where `^a|b` anchors just the first branch and routes traffic
  nobody intended. Case-insensitivity (`~*`) is preserved.
- **Compiled regexes are size-limited** to 1 MiB. A pathological path should
  fail validation, not silently consume memory in every replica.
- **Host validation is strict.** A `host` containing a port, a path, or a
  misplaced `*` is rejected at build time rather than normalized into a guess.

## Not built yet

Everything the proxy and controller stubs describe: listeners, PROXY protocol,
TLS termination, HTTP/1.1 and HTTP/2, upstream pooling and retries, Kubernetes
informers, annotation translation, EndpointSlice handling, status writeback, and
leader election. `RouteTable` also has no rewrite, header-mutation, rate-limit,
or auth rules yet — those attach to `PathRule` when the proxy can act on them.
