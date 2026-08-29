# Flags reference

Every option `ramjet-ingressd` accepts. `--help` prints the same list.

**Every flag has an environment variable twin**, because that is how a container
is configured. The precedence is the usual one — an explicit flag beats the
environment, which beats the default — and a flag *always* wins, so a
`kubectl edit` of the args cannot be silently overridden by a ConfigMap somebody
forgot about.

Both `--flag value` and `--flag=value` are accepted. Every option is a `--flag`;
the binary takes no positional arguments.

`RUST_LOG` sets the log filter. The chart's `controller.logLevel` defaults to
`info,kube=warn`.

## Mode

With no `--static-routes`, the daemon watches the Kubernetes API and serves what
the controller compiles. With one, it serves that file and never talks to
Kubernetes at all.

| Flag | Environment | Default | What it does |
|---|---|---|---|
| `--static-routes <FILE>` | `RAMJET_STATIC_ROUTES` | — | Dev mode: serve the hosts, paths, backends and certificates in `FILE`. Its presence selects the mode |

The two are mutually exclusive by nature: a file and an API server are two
writers for one route table, and letting both write would make the winner a
race.

## Kubernetes

| Flag | Environment | Default | What it does |
|---|---|---|---|
| `--ingress-class <NAME>` | `RAMJET_INGRESS_CLASS` | `ramjet` | The `IngressClass` this replica answers to |
| `--watch-namespace <NS>` | `RAMJET_WATCH_NAMESPACE` | all of them | Watch one namespace only |
| `--default-backend <REF>` | `RAMJET_DEFAULT_BACKEND` | — | Backend for requests matching no rule, as `namespace/name:port`. A malformed value is refused at startup, not at the first unmatched request |
| `--default-tls-secret <REF>` | `RAMJET_DEFAULT_TLS_SECRET` | — | Secret (`namespace/name`) serving a handshake whose SNI matches nothing |
| `--publish-address <ADDR>` | `RAMJET_PUBLISH_ADDRESS` | — | Written into managed Ingresses' status |
| `--publish-service <REF>` | `RAMJET_PUBLISH_SERVICE` | — | Service (`namespace/name`) whose own status supplies that address. **Beats `--publish-address`** |
| `--no-status-update` | `RAMJET_UPDATE_STATUS` (boolean, default `true`) | status writeback on | Never write Ingress status |

The Kubernetes client is configured the way every Kubernetes tool configures
one: the in-cluster ServiceAccount if there is one, otherwise the current
context of `$KUBECONFIG` or `~/.kube/config`.

Boolean environment values accept `true`/`1`/`yes`/`on` and
`false`/`0`/`no`/`off`, case-insensitive. Anything else is an error at startup.
On the command line a boolean is a flag — `--no-status-update true` would be a
worse way to say it.

## Listeners

| Flag | Environment | Default | What it does |
|---|---|---|---|
| `--http <ADDR>` | `RAMJET_HTTP` | `0.0.0.0:8080` | Plaintext listener |
| `--https <ADDR>` | `RAMJET_HTTPS` | `0.0.0.0:8443` | TLS listener |
| `--admin <ADDR>` | `RAMJET_ADMIN` | `0.0.0.0:10254` | Metrics and probes |
| `--admin-token-file <PATH>` | `RAMJET_ADMIN_TOKEN_FILE` | — | Require `Authorization: Bearer <token>` on mutating `/admin/` requests, where `<token>` is the contents of `PATH` |
| `--no-http` | — | — | Disable the plaintext listener |
| `--no-https` | — | — | Disable the TLS listener |
| `--no-admin` | — | — | Disable the admin listener |

An address may be `host:port`, `:port`, or a bare port — `--http :8080` and
`--http 8080` both bind every interface. IPv6 takes the bracketed form,
`[::1]:8443`.

`--admin-token-file` covers `POST` and `DELETE` on `/admin/` and nothing else. A
request without the header, or with the wrong token, is a `401` carrying
`WWW-Authenticate: Bearer`. Trailing whitespace in the file is trimmed, so a
Secret written by `echo` works; a file holding only whitespace is refused at
startup rather than accepted as an empty token.

`GET` is never gated — Prometheus and the kubelet cannot send a header, and a
`/healthz` that 401s is a crash loop. Without the flag, the mutating endpoints
accept anything that can reach the port and the daemon says so once at startup:

```text
WARN the mutating /admin/ endpoints accept any caller that can reach the admin
     port; set --admin-token-file to require a bearer token
```

The token is read once, at startup. Rotating it means restarting the process; on
the chart, replace the Secret and `kubectl rollout restart`. See
[the admin listener](../operations/index.md#the-admin-listener) for the whole
trust model, and `controller.adminToken` and `networkPolicy` in the chart.

The three `--no-*` flags have no environment twins; setting the corresponding
`RAMJET_*` variable to an address is how you move a listener from the
environment.

In **dev mode**, without an explicit `--https` or `--no-https`, the TLS listener
is skipped when the configuration declares no certificates. In **Kubernetes
mode** it always binds: the certificates arrive over a watch, after the socket.

## Time travel and the audit trail

| Flag | Environment | Default | What it does |
|---|---|---|---|
| `--history-size <N>` | `RAMJET_HISTORY_SIZE` | `10` | Compiled generations kept for `/admin/generations` and rollback. `0` is read as `1` |
| `--audit-webhook <URL>` | `RAMJET_AUDIT_WEBHOOK` | — | POST the semantic diff of every published generation to `URL` |

Each kept generation holds its route table and its parsed certificates alive,
which is roughly a hundred bytes per route per generation; the certificates are
content-addressed and shared between generations that did not rotate them. Ten
generations of a ten-thousand route cluster is a few megabytes.

The webhook is fire-and-forget: one attempt, a 5s timeout, failures logged and
never blocking a publish. It speaks `http://` **only** and refuses an `https://`
URL at startup rather than downgrading, because the control plane does not carry
a TLS client for this; point it at a collector inside the cluster.

See [Rollback and the audit trail](../operations/rollback.md).

## Upstreams

| Flag | Environment | Default | What it does |
|---|---|---|---|
| `--connect-timeout <SECS>` | `RAMJET_CONNECT_TIMEOUT` | `5` | TCP connect bound |
| `--response-timeout <SECS>` | `RAMJET_RESPONSE_TIMEOUT` | `60` | Response header bound |
| `--max-connect-attempts <N>` | `RAMJET_MAX_CONNECT_ATTEMPTS` | `3` | Endpoints tried on a connect failure. `0` is read as `1` |
| `--upstream-pool-idle <N>` | `RAMJET_UPSTREAM_POOL_IDLE` | `128` | Idle upstream connections kept per endpoint, **per serving runtime** |

`--upstream-pool-idle` is a **ceiling, not a reservation**: nothing is opened
until a request needs it. Below the concurrent requests an endpoint receives,
the surplus connections are closed as they go idle and reopened on the next
request, which is a TCP handshake on the request path. Above it, the only cost
is file descriptors.

## Serving

| Flag | Environment | Default | What it does |
|---|---|---|---|
| `--engine <NAME>` | `RAMJET_ENGINE` | `hyper` | Data plane: `hyper` or `uring` |
| `--worker-threads <N>` | `RAMJET_WORKER_THREADS` | one per available core | Serving runtimes, one per thread. `0` is read as `1` |
| `--max-buf-size <BYTES>` | `RAMJET_MAX_BUF_SIZE` | `65536` (min `8192`) | Ceiling on one client connection's HTTP/1 read and write buffers |

An unrecognised engine name is an **error, not a fallback to the default**.
Somebody who typed `--engine io_uring` asked for something specific, and quietly
serving on the other engine is the worst possible answer, because it looks like
it worked.

Each runtime owns its connections, its upstream connection pool, and its timers,
and a connection stays on the one it landed on. `available_parallelism` reads
the cgroup CPU limit, so a pod with `limits.cpu: 2` gets two runtimes rather
than one per host core. Setting this above the cores the process can actually
use makes them compete; setting it to `1` serves everything on one thread.

`--max-buf-size` bounds the **tail**, not the common case. hyper allocates the
first 8 KiB of each buffer whatever this is set to, and never shrinks one again
while the connection lives — so a client that sends a 400 KiB header block would
pin 400 KiB until it disconnects. 64 KiB accepts every request nginx's own 32
KiB limit would and bounds the worst case at a sixth of hyper's default.
Requests over the ceiling are answered **431**. A value below `8192` is raised
to it, because hyper panics on anything smaller.

### What `--engine uring` refuses

It serves HTTP/1.1 on the ramjet reactor — io_uring on Linux, kqueue elsewhere
— and terminates TLS, carries protocol upgrades, reads the PROXY protocol, runs
in Kubernetes mode and drains on `SIGTERM` exactly as the other engine does.

What is left is HTTP/2, at both ends of the hop:

- **Downstream**, it does not speak HTTP/2 itself. A client that asks for it is
  handed to a hyper engine in the same process, with its bytes intact, and sees
  one connection that negotiated HTTP/2. That is on by default;
  `--no-h2-dispatch` turns it off, at the cost of not offering HTTP/2 at all.
- **Upstream**, it does not dial one. A backend annotated `backend-protocol:
  GRPC` is answered **502** naming the other engine, and gRPC to it with it.

`--http3` is refused **at startup** rather than ignored, because a UDP listener
that silently did not exist is worse than one that says so.

Everything about routing, load balancing, canaries, headers and `/metrics` is
the same on both. [Engines](../operations/engines.md) has the full parity
matrix, and the differential test that keeps it honest.

## Behind a load balancer

| Flag | Environment | Default | What it does |
|---|---|---|---|
| `--proxy-protocol` | `RAMJET_PROXY_PROTOCOL` (boolean) | off | Require a PROXY protocol header (v1 or v2) on the `--http` and `--https` listeners, and take the client address from it |
| `--proxy-protocol-timeout <SECS>` | `RAMJET_PROXY_PROTOCOL_TIMEOUT` | `5` | Time a sender gets to deliver a complete header before the connection is dropped |

A cloud L4 load balancer — AWS NLB, DigitalOcean, Scaleway, GCP passthrough —
forwards TCP without touching the payload, so without this every request is
attributed to the balancer. Turn it on where the balancer is configured to send
the header, and set the same option on both sides.

> **Security.** The header *is* the client identity. Anything that can reach the
> listener can claim to be any address, and `X-Forwarded-For`, `X-Real-IP` and
> every application decision made from them follow. Enable it only on a listener
> nothing but the load balancer can reach.

The header is **required, not optional**: a connection without a valid one is
dropped, which is what nginx's `proxy_protocol` listener parameter and HAProxy's
`accept-proxy` both do. A permissive fallback would let an attacker choose per
connection whether to be spoofed, which is strictly worse than either fixed
answer.

The first such drop on each serving runtime is logged at `warn` and the rest at
`debug` — so a balancer that is not sending the header says so rather than
looking like a network fault, and without a line per occurrence burying the
outage under its own logs.

The `--admin` listener **never** reads one, because Prometheus and the kubelet
do not send one.

Three properties worth knowing: the header is read **before** the TLS handshake
(that is the order the wire has); nothing read past the header is thrown away,
so a read that returns the header *and* the start of a ClientHello replays those
bytes intact; and a header that names nobody — a v2 `LOCAL` from a health
checker, a v1 `UNKNOWN`, a v2 `AF_UNSPEC` — is consumed with the socket's own
peer standing.

## HTTP/3 (experimental)

| Flag | Environment | Default | What it does |
|---|---|---|---|
| `--http3` | `RAMJET_HTTP3` (boolean) | off | Also serve HTTP/3 over QUIC, on the `--https` port in UDP, and advertise it with `alt-svc` |

Off costs nothing: no UDP socket is bound, no thread is started, and no header
is added.

Two combinations are **refused at startup** rather than ignored:

- `--http3` with `--engine uring`, which has neither TLS nor QUIC.
- `--http3` with `--no-https`, which leaves no port to take and no response to
  advertise on.

`--help` and `--version` still print over a refused combination — they are
requests for text, and answering one with a usage error about two other flags is
the least useful moment to be strict.

See [HTTP/3](../operations/http3.md).

## Traffic mirroring

| Flag | Environment | Default | What it does |
|---|---|---|---|
| `--mirror-max-body <BYTES>` | `RAMJET_MIRROR_MAX_BODY` | `262144` (256 KiB) | Largest request body copied to a mirror backend |

`0` is legal and is **not** clamped up, unlike the buffer ceiling: it means
"never buffer", which still mirrors every `GET` and is a reasonable thing to ask
for on a route that carries large uploads.

Mirroring itself is annotation-driven — see
[Traffic mirroring](../operations/mirroring.md).

## Canary auto-promotion

**No flags.** It is annotation-driven, per canary Ingress, and off unless
`ramjet.dev/auto-promote: "true"` is set on one. See
[Canary auto-promotion](../operations/canary.md).

## Shutdown

| Flag | Environment | Default | What it does |
|---|---|---|---|
| `--shutdown-grace <SECS>` | `RAMJET_SHUTDOWN_GRACE` | `30` | In-flight requests get this long after `SIGTERM` |

`SIGTERM` stops the accept loop and closes the listeners immediately, so the
load balancer looks elsewhere, and then gives in-flight requests the grace
period to finish. Both engines do this, and with HTTP/2 dispatch on both lanes
are signalled at once and drain inside the one deadline — see
[Engines](../operations/engines.md#what-each-one-does) for what counts as
in-flight and why tunnels do not.

The chart's `terminationGracePeriodSeconds` is `45`, deliberately longer than
this default, so Kubernetes does not `SIGKILL` a pod mid-drain.

## Other

| Flag | What it does |
|---|---|
| `-h`, `--help` | Print the usage text |
| `-V`, `--version` | Print the version |

## Why the parser is hand-rolled

This binary's options are all `--name value`, and `clap` would add roughly 200KB
and a dozen transitive crates to a data-plane image for the privilege of
formatting the help text. The parser is about a hundred lines and every option
it accepts is visible in one place — `crates/ramjet-ingressd/src/args.rs`, which
also has a test asserting that every option it accepts appears in `--help`.
