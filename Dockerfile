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

FROM debian:bookworm-slim@sha256:67b30a61dc87758f0caf819646104f29ecbda97d920aaf5edc834128ac8493d3
# `docker-cli` is required for Lambda / RDS / ElastiCache / ECS container
# orchestration when running fakecloud-in-Docker with the host socket
# bind-mounted (the documented setup at fakecloud.dev/docs/getting-started).
# Without it the published image silently has no way to shell out, and
# every container-backed service returns "Docker/Podman is required"
# (issue #1539 Bug 4). debian's `docker.io` package pulls in the daemon,
# which we don't want — `docker-cli` ships the CLI alone.
RUN apt-get update \
    && apt-get upgrade -y \
    && apt-get install -y --no-install-recommends ca-certificates docker-cli \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*
# Signal to the fakecloud binary that it's running inside a container.
# Drives the sibling-container networking path: published Lambda /
# RDS / ElastiCache ports live on the host's loopback, not the
# container's, so fakecloud must reach them via `host.docker.internal`.
ENV FAKECLOUD_IN_CONTAINER=1
COPY --from=builder /build/target/release/fakecloud /usr/local/bin/
EXPOSE 4566
ENTRYPOINT ["fakecloud"]
