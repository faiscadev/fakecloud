# Built and pushed on each fakecloud release tag by
# .github/workflows/docker-rds-images.yml as
#   ghcr.io/faiscadev/fakecloud-postgres:<major>-<fakecloud-version>
# (plus a rolling :<major> tag). RdsRuntime::ensure_postgres_image
# tries to pull that tag first and falls back to building from this
# Dockerfile locally when the pull fails (dev / unreleased / airgapped).
# PG_VERSION must sit before the first FROM so its substitution is
# available there. Do NOT re-declare it inside the postgres stage: that
# image already sets `ENV PG_VERSION=<major>.<minor>-<n>.pgdg<rel>` (e.g.
# 18.4-1.pgdg13+1), and the legacy builder resolves the ENV ahead of the
# ARG, so `postgresql-plpython3-${PG_VERSION}` becomes the full Debian
# version string and apt exits 100. BuildKit prefers the ARG and hides
# the clash, but fakecloud shells out to whatever `docker` it finds —
# in-container CLIs usually have no buildx and fall back to the legacy
# builder. Use the base image's own PG_MAJOR after FROM instead.
ARG PG_VERSION=16

# Rebuild `gosu` from source with current Go to eliminate the upstream
# postgres image's bundled `/usr/local/bin/gosu` Go-stdlib CVEs (Trivy
# flags 8 HIGH + 1 CRITICAL on go1.24.6 stdlib at scan time). gosu
# upstream is rarely re-released; pinning the source version + rebuilding
# on every image build means we control the stdlib version baked in.
FROM golang:1.25-bookworm AS gosu-builder
ENV CGO_ENABLED=0
RUN go install -ldflags='-s -w' github.com/tianon/gosu@v0.0.0-20250923190938-6456aaa0f3c8

FROM postgres:${PG_VERSION}

COPY --from=gosu-builder /go/bin/gosu /usr/local/bin/gosu
RUN chmod 0755 /usr/local/bin/gosu

# `apt-get upgrade` pulls in patched OS packages (openssl, glibc, dirmngr…)
# which sometimes lag behind on the upstream `postgres:<major>` tag —
# Trivy flags those as HIGH/CRITICAL even when fixes are available in
# the same Debian release. Running upgrade keeps the published image
# clean against the latest debian13 security DB at build time.
RUN apt-get update \
    && apt-get upgrade -y --no-install-recommends \
    && apt-get install -y --no-install-recommends \
        postgresql-plpython3-${PG_MAJOR} \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY aws_commons.control aws_commons--1.1.sql aws_commons--1.0--1.1.sql \
     aws_lambda.control aws_lambda--1.0.sql \
     aws_s3.control aws_s3--1.0.sql \
     /usr/share/postgresql/${PG_MAJOR}/extension/
