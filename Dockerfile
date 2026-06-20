FROM rust:1.94-bookworm AS chef
RUN cargo install cargo-chef
WORKDIR /build

FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder
COPY --from=planner /build/recipe.json recipe.json
RUN cargo chef cook --release --recipe-path recipe.json
COPY . .
RUN cargo build --release --bin fakecloud

# Fetch the standalone docker CLI binary. Debian's repos have no
# CLI-only package — `docker.io` bundles the daemon (~bloat we don't
# want) and the lean `docker-ce-cli` lives only in Docker's own apt
# repo. The upstream static tarball ships exactly the client binary,
# so we extract just that. `TARGETARCH` is provided by buildx for each
# platform in the build matrix (issue #1539 Bug 4).
FROM debian:bookworm-slim AS docker-cli
ARG TARGETARCH
# Pin a docker CLI built with a current Go toolchain — the static build
# bakes the Go stdlib into the binary, so a stale toolchain trips the
# image's Trivy CRITICAL/HIGH gate (27.5.1 shipped go1.22.11, flagged by
# CVE-2025-68121). 29.5.2 ships go1.26.3, which clears the gate.
ARG DOCKER_CLI_VERSION=29.5.3
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl ca-certificates \
    && case "$TARGETARCH" in \
         amd64) ARCH=x86_64 ;; \
         arm64) ARCH=aarch64 ;; \
         *) echo "unsupported TARGETARCH: $TARGETARCH" && exit 1 ;; \
       esac \
    && curl -fsSL "https://download.docker.com/linux/static/stable/${ARCH}/docker-${DOCKER_CLI_VERSION}.tgz" -o /tmp/docker.tgz \
    && tar -xzf /tmp/docker.tgz -C /tmp \
    && install -m 0755 /tmp/docker/docker /usr/local/bin/docker \
    && rm -rf /tmp/docker /tmp/docker.tgz \
    && /usr/local/bin/docker --version

FROM debian:bookworm-slim@sha256:67b30a61dc87758f0caf819646104f29ecbda97d920aaf5edc834128ac8493d3
RUN apt-get update \
    && apt-get upgrade -y \
    && apt-get install -y --no-install-recommends ca-certificates nftables kmod procps \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*
# `nft` (nftables) backs EC2 security-group / NACL packet enforcement when the
# operator opts in with FAKECLOUD_EC2_SG_ENFORCEMENT=1 and grants CAP_NET_ADMIN
# (cap_add: NET_ADMIN). Without the binary in the image the feature could never
# activate and silently degraded to metadata-only even with the capability —
# the same "documented feature, missing binary" shape as issue #1539 Bug 4.
# `kmod` (modprobe) and `procps` (sysctl) ship alongside it: same-subnet
# instances share one Linux bridge, so their L2-switched traffic only reaches
# the nft `forward` chain once bridge netfilter is loaded + enabled
# (`modprobe br_netfilter` + `sysctl net.bridge.bridge-nf-call-iptables=1`).
# debian:bookworm-slim ships neither binary, so without these the ruleset
# applied but filtered nothing — the missing-binary shape one layer down from
# #1760 (bug-audit 2026-06-20, 0.B1).
# The docker CLI is required for Lambda / RDS / ElastiCache / ECS
# container orchestration when running fakecloud-in-Docker with the host
# socket bind-mounted (the documented setup at
# fakecloud.dev/docs/getting-started). Without it the published image has
# no way to shell out, and every container-backed service returns
# "Docker/Podman is required" (issue #1539 Bug 4).
COPY --from=docker-cli /usr/local/bin/docker /usr/local/bin/docker
# Signal to the fakecloud binary that it's running inside a container.
# Drives the sibling-container networking path: published Lambda /
# RDS / ElastiCache ports live on the host's loopback, not the
# container's, so fakecloud must reach them via `host.docker.internal`.
ENV FAKECLOUD_IN_CONTAINER=1
COPY --from=builder /build/target/release/fakecloud /usr/local/bin/
EXPOSE 4566
ENTRYPOINT ["fakecloud"]
