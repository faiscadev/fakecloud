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
ARG DOCKER_CLI_VERSION=27.5.1
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
    && apt-get install -y --no-install-recommends ca-certificates \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*
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
