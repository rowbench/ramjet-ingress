# TLS

TLS is terminated on the `--https` listener (default `0.0.0.0:8443`) by rustls
over `ring`. There is no OpenSSL in the image and no CA bundle.

```yaml
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata:
  name: shop
  namespace: prod
spec:
  ingressClassName: ramjet
  tls:
    - hosts:
        - shop.example.com
      secretName: shop-tls        # a kubernetes.io/tls Secret in `prod`
  rules:
    - host: shop.example.com
      http:
        paths:
          - path: /
            pathType: Prefix
            backend:
              service:
                name: web
                port:
                  number: 80
```

The Secret is looked up in **the Ingress's own namespace**. There is no
cross-namespace reference.

Both engines terminate TLS, and through the same resolver over the same
certificate store — so a name resolves to the same certificate, and a rotation
reaches both, whichever `--engine` is serving. Everything below is therefore
about this proxy rather than about one lane of it.

## SNI resolution

A server name resolves to a certificate using exactly the same precedence as
host routing — exact name, then single-label wildcard, then the default
certificate. A handshake that picked a different certificate than the request
would later be routed by is a confusing way to fail.

```text
1. exact name          shop.example.com
2. wildcard parent     *.example.com
3. --default-tls-secret
```

## The default certificate

A handshake whose SNI matches nothing gets the default certificate, if there is
one:

```sh
--default-tls-secret ingress/wildcard      # namespace/name
```

or `controller.defaultTlsSecret` in the chart. Without it, such a handshake
fails.

This is also **the supported way to serve a certificate that covers names the
Ingress does not list**, because:

> **An `IngressTLS` entry with no `hosts` is skipped**, with a warning. The
> controller cannot read a certificate's SANs to work out which names it covers
> — that would mean parsing X.509 in the control plane, which is exactly the
> dependency the layering split exists to avoid.

An entry with no `secretName` is skipped the same way.

## Two Ingresses claiming one host

The **older** Ingress keeps the host, and the newer one gets a warning naming
the holder. Nothing is silently overwritten and nothing is refused.

## A certificate that will not parse

Logged and skipped, never fatal. TLS for the names it covers fails until the
Secret is fixed; every other host, and all plaintext traffic, is untouched.
Refusing the whole generation would let one malformed Secret in one namespace
take the cluster's routing offline.

The same is true one level up: a Secret that cannot be read at all produces
`prod/shop-tls: <reason>; serving these hosts without it` and the rest of the
table compiles.

## Rotation costs nothing it does not have to

`handle_id` is derived from the Secret's namespace, name, and **contents**, so
it changes if and only if the material changes. The daemon keeps its parsed keys
in a map keyed by that id and carries forward every id that survives a rebuild,
parsing only what actually rotated.

**A cluster with 500 certificates does no X.509 work at all when an unrelated
Ingress is edited.**

The same property makes eviction safe: a key that no longer appears in a new
generation is simply not carried over, and dropping it cannot orphan a name,
because a name that still resolves still names its id.

### Why a rotation never drops a handshake

A generation is applied in two stores, in this order:

1. the certificate store — the whole `handle_id → key` map at once;
2. the route table that references those ids.

Those are two independent atomic pointers, so a handshake can observe a new
table against an older store. Publishing certificates **first** makes the only
possible skew a store holding a key nothing points at yet, which is invisible.
The other order leaves a name whose id is missing from the store, which rustls
turns into a failed handshake — and every rotation would drop connections for
the width of that gap.

The same order is used when a [rollback](../operations/rollback.md) republishes
an old generation, for the same reason.

## Dev mode

The static route file takes certificates as PEM paths:

```yaml
tls:
  - host: shop.example.com
    cert: /tmp/dev-cert.pem
    key: /tmp/dev-key.pem
  # An entry with no host (or `host: "*"`) becomes the default certificate,
  # served when SNI matches nothing.
```

Generate a throwaway pair with:

```sh
openssl req -x509 -newkey rsa:2048 -nodes -days 365 \
    -keyout /tmp/dev-key.pem -out /tmp/dev-cert.pem \
    -subj '/CN=shop.example.com' \
    -addext 'subjectAltName=DNS:shop.example.com'
```

In dev mode, without an explicit `--https` or `--no-https`, the TLS listener is
**skipped** when the file declares no certificates — a listener that fails every
handshake is not a useful default. In Kubernetes mode it always binds: the
certificates arrive over a watch, after the socket.

## What TLS does not do here

- **There is no TLS to the upstream.** The upstream side speaks HTTP/1.1, or
  cleartext HTTP/2 for a backend annotated
  [`backend-protocol: GRPC`](./annotations.md#backend-protocol) — and both are
  cleartext, which is why `GRPCS` and `HTTPS` are reported and not honoured.
- **HTTP/3 shares this listener's certificates exactly** — the same SNI
  resolution, the same store, the same rotation, reaching both transports at the
  same instant because it is the same two pointer stores in the same order. See
  [HTTP/3](../operations/http3.md).

## Metrics

`ramjet_tls_handshakes_total` and `ramjet_tls_handshake_failures_total`. A
failure rate that moves after a deploy is usually a Secret that did not parse or
a name nothing covers.
