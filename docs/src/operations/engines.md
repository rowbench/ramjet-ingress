# Engines

There are two data planes. They read the same route table, resolve certificates
from the same store, write the same `/metrics`, and answer requests the same
way. What differs is how they move bytes.

| | `hyper` | `uring` |
|---|---|---|
| Runtime | tokio, one runtime per core | the `ramjet` reactor, one per core |
| I/O model | readiness (`epoll`/`kqueue`) | completion (`io_uring` on Linux, `kqueue` elsewhere) |
| Default | yes | no |

`--engine uring` selects the second one. `--engine uring-strict` selects it and
refuses to start if the host will not run it, rather than falling back.

## What each one does

Every row here is covered by a test, and the ones marked *same* are covered by a
[differential test](#the-differential-test) that drives both engines with
identical traffic and compares the answers.

| Feature | `hyper` | `uring` |
|---|---|---|
| HTTP/1.1 | yes | yes |
| HTTP/1.1 keep-alive, pipelining | yes | yes |
| HTTP/2 | yes | by dispatch — see [HTTP/2 on the uring engine](#http2-on-the-uring-engine) |
| HTTP/3 over QUIC | behind `--http3` | no |
| gRPC | 502 (needs an HTTP/2 upstream) | 502, same body |
| TLS termination | yes | yes |
| SNI, wildcard and default certificates | yes | yes, the same resolver |
| Session resumption (tickets) | yes | yes, the same configuration |
| Certificate rotation without dropping the listener | yes | yes |
| WebSocket and other upgrades | yes | yes, passthrough |
| PROXY protocol v1 and v2 | yes | yes, the same parser |
| Routing, host and path precedence | same | same |
| Load balancing, `leastConn` in-flight counts | same | same |
| Canary by header, cookie, and weight | same | same |
| Traffic mirroring | yes | yes — see [the body](#mirroring-and-the-request-body) |
| Per-route counters, canary split | same | same |
| `X-Forwarded-*`, `X-Request-Id`, hop-by-hop | same bytes | same bytes |
| Error bodies and status codes | same bytes | same bytes |
| Kubernetes mode, live generations | yes | yes |
| Rollback pins and generation history | yes | yes |
| `/metrics` exposition | same bytes | same bytes |
| `/admin/generations`, `/admin/routes` | yes | yes, served by a tokio listener |
| Graceful drain on `SIGTERM` | up to `--shutdown-grace` | connections end with the process |

Two rows are worth reading twice.

**Graceful drain.** The hyper engine stops accepting, then waits up to
`--shutdown-grace` for in-flight requests to finish. The uring engine stops
accepting and closes. On a rolling update that is the difference between a
request completing and a client retrying, and it is the largest remaining gap
between the two.

**HTTP/3.** It stays on the hyper engine's QUIC listener. `--http3` with
`--engine uring` is refused at startup rather than ignored.

## HTTP/2 on the uring engine

The uring engine speaks HTTP/1.1. Rather than not offering HTTP/2 at all, it
offers it and hands those connections to a hyper engine running in the same
process.

This is possible because of where rustls lets a server stand. A
`rustls::server::Acceptor` reads the ClientHello and **stops** — before a
configuration is chosen, before a byte is written back — and the ALPN list the
client offered is readable at that point. So the decision happens while the
connection is still nobody's:

- the client offered `http/1.1`, or no ALPN at all → served here;
- the client offered `h2` → the socket and every byte read from it go to the
  other engine, which replays them and finishes the handshake itself.

From the client there is no reset, no second handshake and no retry. It sees one
connection, which negotiated HTTP/2. On the plaintext listener the HTTP/2
prior-knowledge preface (`PRI * HTTP/2.0`) is handed over the same way.

Two counters say which way traffic went:

```text
ramjet_dispatch_uring_total   connections kept after reading the ClientHello
ramjet_dispatch_hyper_total   connections handed over because the client asked for h2
```

`/metrics` sums both engines, so the numbers describe the process rather than
one half of it.

`--no-h2-dispatch` turns this off. The TLS listener then advertises `http/1.1`
alone and an HTTP/2 client negotiates HTTP/1.1 with it — which works, and is
what every browser falls back to, but costs multiplexing. It also means the
second engine's threads and upstream pools are never started, which is the
reason to turn it off.

## Falling back

`io_uring_setup` is blocked by Docker's default seccomp profile. Whether a given
cluster allows it depends on the node image, the container runtime, and the
pod's own seccomp profile, which is not something a chart value can know.

So `--engine uring` asks the host before anything binds — one ring's setup and
teardown — and serves on `hyper` if the answer is no:

```text
WARN the ramjet reactor will not start on this host; falling back to the hyper
     engine error=Operation not permitted (os error 1) requested="uring"
     serving="hyper"
```

The reason is always logged with the `errno` behind it. The two causes an
operator actually hits — a kernel older than 5.6, and seccomp — are told apart
only by which error comes back.

`--engine uring-strict` refuses to start instead. That is for a deployment that
would rather crash-loop visibly than serve on an engine it did not choose: a
silent fallback has none of the properties its operator picked it for, and no
obvious sign anything happened.

On macOS and BSD the reactor is kqueue and this never comes up.

`/metrics` deliberately does **not** say which engine is serving. A series
naming the engine would be the easy way to report it, and it would make every
dashboard engine-specific in exchange. The startup log says it once instead.

### Checking which engine a replica chose

```console
$ kubectl logs -l app.kubernetes.io/name=ramjet-ingress --tail=20 | head
ramjet-ingressd 0.1.0 — engine uring, 3 backend(s), 6 endpoint(s), 4 route(s)
```

A replica that fell back says so on the line above that one.

### One cluster where it does fall back

Docker Desktop's Kubernetes, measured rather than assumed. `deploy/e2e.sh` was
run against it with `ENGINE=uring`, and the pod reported:

```text
WARN the ramjet reactor will not start on this host; falling back to the hyper
     engine error=Operation not permitted (os error 1) requested="uring"
     serving="hyper"
```

`io_uring_setup` returns `EPERM` inside that kubelet's containers. The whole
suite then passed on the hyper engine — routing, canary split, TLS with SNI,
per-route stats, mirroring, auto-promotion — which is the outcome the fallback
exists to produce: a replica that serves rather than one that crash-loops
because of a syscall policy nobody set deliberately.

The same syscall is permitted in the *plain Docker* daemon on the same machine
once the seccomp profile allows it, which is how
[`bench/engine/`](https://github.com/rowbench/ramjet-ingress/blob/main/bench/engine/RESULTS.md)
measures the reactor at all. The difference is the pod's seccomp profile, not
the kernel — so on a cluster where you control that profile, `uring` will run.

## Mirroring and the request body

Both engines mirror. They differ in when the copy is taken, and the uring
engine's way is the better one.

The hyper engine reads the request body up to `--mirror-max-body` **before**
dispatching the primary, because it has to: a body is a stream it can consume
only once, so the copy has to be taken before the original is handed to the
upstream client. A mirrored request with a body therefore waits for its body to
be buffered.

The uring engine already moves those bytes through a buffer on their way
upstream, so the copy is taken as they pass and queued when the body ends. The
primary waits for nothing.

One case goes the other way. A chunked request body is forwarded verbatim on the
uring engine — chunk framing and all — so the bytes going past are the body's
*encoding*, not the body. Sending those as a self-framed copy would
double-encode them, and decoding a body this engine deliberately does not decode
is not a trade worth making for a copy. Chunked request bodies are counted in
`ramjet_mirror_skipped_total` and no copy is sent.

## The differential test

Two engines that are supposed to be indistinguishable cannot be tested by
asserting either one against a literal: that is a test which keeps passing after
the *other* one drifts.

So `crates/ramjet-engine/tests/differential.rs` starts both, drives them with
byte-identical requests against byte-identical route tables, and compares:

1. the status and the body;
2. the **whole rewritten head** the upstream received, field by field — which is
   what catches a header written in a different order, a different case, a
   missing `X-Forwarded-Host`, or an `X-Forwarded-For` that replaced the trail
   instead of extending it;
3. the counter deltas, scraped before and after and subtracted.

The one field that must differ is `X-Request-Id` when the client sent none: it
is 32 random hex characters by design, and two engines agreeing on it would mean
the randomness was broken. Its presence and shape are compared; an inbound id is
compared exactly, which is what actually matters.

`crates/ramjet-engine/tests/exposition.rs` does the same for `/metrics`: both
counter sets driven through the same events, and the two strings asserted
**equal**.

## Choosing one

Run `hyper` unless you have a reason not to. It is the default, it is what the
numbers in [Performance](../performance.md) were measured on for every release
before this one, and it drains gracefully.

Run `uring` when the replica is CPU-bound on a Linux host that permits
`io_uring`, and the traffic is HTTP/1.1 or HTTP/2 over TLS. That is where the
completion-based reactor's fewer syscalls per request show up; see
[Performance](../performance.md) for the measurements and
[`bench/engine/RESULTS.md`](https://github.com/rowbench/ramjet-ingress/blob/main/bench/engine/RESULTS.md)
for the protocol behind them.

Run `uring-strict` when a fallback would be worse than a crash loop.
