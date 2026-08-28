# ramjet-ingress

A Kubernetes ingress controller with a native Rust data plane. No nginx, no
config file regeneration, no reload: a configuration change swaps an `Arc`, and
in-flight connections never notice.

Feature target is parity with `kubernetes/ingress-nginx` on the
`networking.k8s.io/v1` Ingress resource. There is no nginx anywhere in the
design.

```sh
helm install ramjet deploy/chart/ramjet-ingress \
  --namespace ramjet-ingress --create-namespace
```

## The thesis: swap a pointer, do not reload

ingress-nginx reacts to a configuration change by regenerating `nginx.conf` and
reloading. A reload forks new workers, drains the old ones, and in the process
resets upstream state and severs connections that were meant to be long-lived.
The cost of a config change is proportional to how much traffic you are
carrying, which is backwards: the busier you are, the more a routine deploy
hurts.

Here the control plane compiles configuration into an immutable `RouteTable` and
publishes it by storing one pointer into an `arc_swap::ArcSwap`. The data plane
does a single atomic load per request and then reads an immutable snapshot.
There is no `RwLock`, no reader-writer contention, no reload, and no draining.

```text
Kubernetes API                    ArcSwap<RouteTable>              worker
     |                                    |                          |
  watch Ingress/Service/Secret            |                     load() -- 1 atomic
     |                                    |                          |
  RouteTableBuilder --> RouteTable --> store()                  match_request()
     (pure function)      (immutable)   (one pointer)           (borrows, no alloc)
```

Three properties follow, and they are the point of the design:

- **A publish never blocks a reader.** Writers and readers never share a lock,
  so a rebuild cannot add latency to a request.
- **In-flight requests are unaffected.** A request that loaded generation 7
  holds that `Arc` and finishes against generation 7 even if 8 is published
  mid-flight. Nothing is rewritten under it.
- **Load-balancer state survives the swap.** Round-robin cursors and in-flight
  counts are carried forward by identity, not by position, so adding one Ingress
  does not make every backend forget how many requests it is currently serving.

## The numbers, including the ones we lose

This project's benchmarks are written to be checkable, and that means reporting
the losses with the wins. [Performance](./performance.md) has the full method,
the caveats, and the raw-data paths. The headlines:

| Measurement | Result |
|---|---|
| Configuration churn under live traffic | ramjet-ingress kept **100 of 100** idle keep-alive connections; ingress-nginx kept **0 of 50**, reproducibly, every run, under spec churn |
| CPU per request during churn | **+0% and +2%** against its own baseline, against ingress-nginx's +10% (reload path) and +25% (endpoint path) |
| Raw HTTP/1.1 forwarding, hyper engine vs nginx | **level at c64** (85,908 against 86,670, inside the noise); nginx **9% ahead at c256** |
| The `uring` engine vs nginx (Linux, io_uring) | **+44.7% at the median**, and at least +31% on a rank-order claim that survives the machine's drift |
| Propagating a new Ingress | **~3x faster** at the median, ~6x at p95; **10x** with 500 routes already loaded |
| **Idle-connection memory** | **ingress-nginx wins.** 4.4 KiB per idle connection against ramjet-ingress's 20.3 KiB — 4.6x, and the gap is structural |
| **`kubectl apply` write path** | **ingress-nginx wins.** 138 ms median against 159, including `nginx -t` validation ramjet-ingress does not do |
| **Endpoint-only churn** | **Tied.** ingress-nginx does not reload for endpoint changes, kept every connection, and dropped nothing |

The last three rows are not a disclaimer at the bottom of a marketing page.
ingress-nginx does **not** reload for every change — endpoint updates go through
its Lua balancer without touching nginx at all — and a report that measured only
the changes which force a reload would be describing a system that does not
exist.

## Two engines

The data plane is selected with `--engine`, and everything above it — routing,
load balancing, canaries, header rewriting, `/metrics` — is the same code either
way.

| | `--engine hyper` (default) | `--engine uring` |
|---|---|---|
| Runtime | hyper on tokio | the `ramjet` reactor: io_uring on Linux, kqueue elsewhere |
| HTTP/1.1 plaintext | yes | yes |
| TLS termination | yes | no (502) |
| HTTP/2, gRPC upstreams | h2 downstream | no (502) |
| WebSocket and upgrades | yes | no (502) |
| HTTP/3 over QUIC (`--http3`) | experimental, off by default | no; refused at startup |
| PROXY protocol (`--proxy-protocol`) | v1 and v2 | no; refused at startup |
| Kubernetes mode | yes | no; static routes only |
| Status | measured against nginx | experimental |

`uring` exists to answer one question. Profiling measured where a request goes
and found no hot function to fix: 59.4% of a request is the four syscalls a
proxy hop cannot avoid, another 9.1% is finding out a socket is ready, and
everything this project wrote is about 1%. Getting under that floor is not a
tuning exercise, it is an I/O model change — so there is a second data plane
that submits those four operations into a ring and enters the kernel once for a
batch of them.

Everything it refuses, it refuses with a status code and an explanation naming
the other engine, and it prints the same list at startup. A gap that behaves
like a bug in whatever is on the other end is worse than a missing feature.

## Where to go next

- **[Quick start](./quick-start.md)** — the data plane on a laptop in 60
  seconds, then a cluster.
- **[Deployment](./deployment.md)** — one command per cloud, and the question
  that decides most of the configuration: where the client's IP address comes
  from.
- **[Annotations reference](./configuration/annotations.md)** and
  **[Flags reference](./configuration/flags.md)** — every key and every option,
  verified against the source.
- **[Limitations](./limitations.md)** — read this before you deploy. The line
  that matters operationally: there is no leader election yet, so run **one
  replica**.
