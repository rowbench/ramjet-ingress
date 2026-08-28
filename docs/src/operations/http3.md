# HTTP/3

> **Experimental, and off by default.** Off costs nothing: no UDP socket is
> bound, no thread is started, and no header is added. Read
> [the deployment constraint](#which-load-balancers-can-carry-it) before turning
> it on — for most cloud shapes it is the thing that decides whether this works
> at all.

`--http3` serves HTTP/3 over QUIC on the `--https` port number **in UDP**, and
advertises it on every HTTPS response with `alt-svc: h3=":<port>"; ma=86400`.

```console
$ ramjet-ingressd --static-routes routes.yaml --http3
ramjet-ingressd 0.1.0 — 1 backend(s), 1 endpoint(s), 1 route(s), 1 certificate(s)
  config   routes.yaml
  http     0.0.0.0:8080
  https    0.0.0.0:8443
  http3    0.0.0.0:8443
  admin    0.0.0.0:10254
```

In the chart:

```sh
helm upgrade ramjet deploy/chart/ramjet-ingress --reuse-values \
  --set http3.enabled=true
```

which adds `--http3`, a UDP container port and a UDP Service port — both on the
same number as `https`.

## A second way in, not a second proxy

A request that arrives over QUIC is turned into the same types the TCP listeners
produce and handed to the same forwarding function, so routing, canary
arithmetic, load balancing, header rewriting, retries, per-route counters,
mirroring and the upstream pool are **the ones already in use** and cannot drift
from them. What the HTTP/3 module owns is how bytes get on and off the wire, and
nothing else.

Two consequences of that reuse are load-bearing:

**The certificates are the TLS listener's.** The QUIC crypto configuration is
built over the *same* SNI resolver — the same map in the same route table, the
same store — so a name resolves to the same certificate over UDP as over TCP,
and a rotation reaches both at the same instant because it is the same two
pointer stores in the same order. A handshake that picked differently depending
on transport would be a spectacular way to fail.

**Bodies.** hyper's incoming-body type has no public constructor, so the
forwarding function takes the crate's own body type and the TCP path converts at
the call site. That is the whole reason the signature is what it is.

### Deciding whether an HTTP/3 request has a body

HTTP/3 has no `Transfer-Encoding` and no framing outside the stream: a request
has a body if and only if DATA frames arrive before the client finishes the
stream. `content-length` answers it when a client sent one.

When none did — and no `GET` does — the alternative to guessing is one
**non-blocking** poll of the request stream. A client that has already finished
it, which is every ordinary `GET` by the time its packets arrive, is recognised
immediately: the body is known-empty, the request is retryable across endpoints,
and the origin sees an ordinary `GET` rather than one carrying
`Transfer-Encoding: chunked`. A client that has not is not waited for — the poll
returns pending, the body streams, and the first DATA frame goes upstream when
it arrives.

## One endpoint, on one runtime

This is the honest reason the feature is experimental.

The TCP data plane is one runtime per core with `SO_REUSEPORT` spreading
accepts. The obvious transliteration — N UDP sockets on one port, one QUIC
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
core's worth of QUIC crypto, packet handling and proxying.

That is stated rather than measured. HTTP/1.1 and HTTP/2 keep every core they
had, so this is not the path to put peak traffic on yet.

## Which load balancers can carry it

The `alt-svc` header is the whole mechanism, and it is also the whole
constraint. A client that reads it retries **the same authority** over QUIC, so
the port number it is already using for TCP has to answer UDP too, through every
hop in front of the pod.

| Shape | UDP on the same address and port? |
|---|---|
| AWS NLB (`aws`, `aws-nlb-proxy`) | **Yes.** One NLB carries TCP 443 and UDP 443 on one address; this is the shape it was built against |
| `aws-nlb-tls` | **No, and not meaningfully.** ACM terminates TLS at the balancer and forwards plaintext, and there is no QUIC to a plaintext port |
| GCP, Azure, Oracle, Exoscale, DigitalOcean, Scaleway | **Per-provider, usually not on the same address.** Where UDP is supported at all it typically needs a second load balancer, and two balancers do not share an address — so the advertisement would name a port the client cannot reach |
| `baremetal-hostnetwork` | **Yes.** There is no balancer to ask: the node's UDP 443 is the node's UDP 443 |
| `baremetal-nodeport` | **Partly.** The chart does not pin a UDP nodePort, so the allocated one will not match 30443 |

**Getting it wrong is slow rather than broken.** A client whose QUIC attempt
fails falls back to TCP by itself — the cost is one wasted attempt per
connection until the advertisement expires, which is why `ma` is a day and not a
week.

No provider preset turns this on, because whether UDP reaches the pod is a
property of an account's networking rather than of a provider.

## The client address, behind a balancer

The PROXY protocol **does not apply**. It is a preamble on a TCP byte stream and
has no UDP form, so a QUIC connection's client address is whatever the IP header
says.

- On a balancer that forwards UDP without rewriting the source, that is the real
  client.
- On one that SNATs it, `X-Forwarded-For` on HTTP/3 requests will name the
  balancer **while the TCP path is still correct**.

There is no configuration that fixes the difference.

## Draining

`SIGTERM` stops the endpoint accepting, and each live connection sends GOAWAY
and then finishes the requests already on it, inside the same grace period the
TCP listeners get.

In-flight requests are counted here rather than left to the HTTP/3 library's own
bookkeeping, and that is not redundancy: its accept loop yields "done" only once
every request is complete *and* a GOAWAY has been received — the peer's, not
ours. A server that sent GOAWAY and then waited for that would be waiting for
the client to hang up, and after a GOAWAY every client is idle by definition.
Every shutdown with an open HTTP/3 connection would burn the whole grace period
and then report a timeout.

## What is not supported

- **No 0-RTT.** Early data is explicitly disabled. It is replayable by anyone
  who captured it, and which requests are safe to replay is an application's
  judgement, not an ingress's.
- **No QUIC upstream.** Upstream is HTTP/1.1, as it is for every other
  downstream protocol here.
- **No PROXY protocol**, which has no UDP form.
- **No protocol upgrades.** WebSockets over HTTP/3 are RFC 9220 extended
  `CONNECT`, a different mechanism from a `101`; an upstream that answers `101`
  to a request that arrived over QUIC gets the same 502 any half-completable
  upgrade gets.
- **No h3 datagrams, no WebTransport, no server push.**
- **`--engine uring` refuses it at startup**, because that engine has neither
  TLS nor QUIC. So does `--no-https`: there would be no port to take and no
  response to advertise on.

## Metrics

```text
ramjet_h3_connections_total
ramjet_h3_requests_total
ramjet_h3_handshake_failures_total
```

An HTTP/3 request is also counted in `ramjet_requests_total` like any other,
because it is one.
