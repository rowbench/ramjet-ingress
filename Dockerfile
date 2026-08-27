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
    && cp /src/ramjet-ingress/target/release/ramjet-ingressd /ramjet-ingressd

# `cc` rather than `static-debian12`: the binary links glibc. It does not link
# OpenSSL, because TLS is rustls over ring — which is the reason this image
# needs no CA bundle and no libssl.
FROM gcr.io/distroless/cc-debian12:nonroot

COPY --from=builder /ramjet-ingressd /usr/local/bin/ramjet-ingressd

# Documentation, not enforcement: 8080 plaintext, 8443 TLS, 10254 admin
# (/metrics, /healthz, /readyz). All three are above 1024, so the unprivileged
# user the base image already selects can bind them without a capability.
EXPOSE 8080 8443 10254

# The nonroot user (65532) comes from the base image tag; naming it here as
# well means a later change to that tag cannot silently promote this to root.
USER 65532:65532

ENTRYPOINT ["/usr/local/bin/ramjet-ingressd"]
