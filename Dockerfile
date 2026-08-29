# The data plane, as a container.
#
# Two stages: a full Rust toolchain to compile, and a runtime that carries the
# binary and a C library and nothing else. The runtime has no shell, no package
# manager, and no root user, which is worth stating plainly — a process that
# terminates untrusted connections from the whole internet should not be one
# `sh -c` away from a working toolchain.
#
# Build context is the **parent** of the repository:
#
#     docker build -f ramjet-ingress/Dockerfile -t ramjet-ingressd .
#
# `crates/ramjet-engine` depends on the `ramjet` runtime by path
# (`../enhance-socket`), and cargo will not load a workspace whose member has a
# dependency it cannot find, so the sibling checkout has to be inside the
# context. Ignore rules live in Dockerfile.dockerignore beside this file.

FROM rust:1-bookworm AS builder

# setcap(8), for the file capability the binary carries out of this stage. It is
# installed before anything is copied so that the layer caches on its own and a
# source change does not re-run apt.
RUN apt-get update \
    && apt-get install -y --no-install-recommends libcap2-bin \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src

# Cargo's registry and the target directory are BuildKit cache mounts rather
# than image layers. That is the alternative to the usual "copy the manifests,
# build dummy sources, copy the real sources" trick, which for a four-crate
# workspace means four fabricated lib.rs/main.rs files that have to be kept in
# step with the real ones — a copy of the workspace layout hidden in a
# Dockerfile. Cache mounts get the same incremental rebuild with a single COPY
# and no fiction. The cost is that the caches live in the builder, not in the
# image, so `cp` below has to lift the binary out before the mount goes away.
COPY ramjet-ingress ./ramjet-ingress
COPY enhance-socket ./enhance-socket

WORKDIR /src/ramjet-ingress

RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/src/ramjet-ingress/target,sharing=locked \
    cargo build --release --locked -p ramjet-ingressd \
    && cp /src/ramjet-ingress/target/release/ramjet-ingressd /ramjet-ingressd \
    && setcap cap_net_bind_service=+ep /ramjet-ingressd \
    && getcap /ramjet-ingressd | grep -q cap_net_bind_service

# `cc` rather than `static-debian12`: the binary links glibc. It does not link
# OpenSSL, because TLS is rustls over ring — which is the reason this image
# needs no CA bundle and no libssl.
FROM gcr.io/distroless/cc-debian12:nonroot

# The `security.capability` xattr set above rides along: BuildKit's COPY
# preserves extended attributes, the layer tar carries it as a PAX
# `SCHILY.xattr.security.capability` record, and containerd restores it on
# unpack. Verified rather than assumed — deploy/README.md says how to check it
# on an image you have pulled.
COPY --from=builder /ramjet-ingressd /usr/local/bin/ramjet-ingressd

# Documentation, not enforcement: 80 plaintext, 443 TLS, 10254 admin
# (/metrics, /healthz, /readyz). The chart's default is a hostNetwork DaemonSet
# on the first two, which is why the binary carries the capability above: uid
# 65532 cannot bind a port below 1024, and Kubernetes' `capabilities.add` alone
# does not help, because the kernel raises a capability into a *non-root*
# process's effective set only from a file capability on the binary. That is
# also why the nginx binary in ingress-nginx carries the same one.
#
# The obligation it creates runs the other way, and matters on every port. A
# file capability with the effective bit set makes execve fail with EPERM when
# that capability is outside the container's bounding set — the kubelet reports
# it as "operation not permitted", before a line of this program runs. So any
# pod spec for this image has to keep NET_BIND_SERVICE in
# securityContext.capabilities, on 8080 exactly as much as on 80. The chart does
# that unconditionally and says why in values.yaml; a hand-written manifest that
# drops ALL and adds nothing back will not start.
EXPOSE 80 443 8080 8443 10254

# The nonroot user (65532) comes from the base image tag; naming it here as
# well means a later change to that tag cannot silently promote this to root.
USER 65532:65532

ENTRYPOINT ["/usr/local/bin/ramjet-ingressd"]
