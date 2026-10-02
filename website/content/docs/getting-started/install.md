+++
title = "Install"
description = "Install fakecloud via install script, Homebrew, Cargo, Docker, Docker Compose, or from source."
weight = 1
+++

fakecloud ships as a single ~19 MB binary. Pick whichever install path fits your workflow.

## Install script (recommended)

Works on macOS and Linux, and in CI:

```sh
curl -fsSL https://fakecloud.dev/install.sh | bash
fakecloud
```

The script downloads the latest release for your platform and puts the `fakecloud` binary in `/usr/local/bin` (override with `--install-dir <dir>`; pin a release with `--version vX.Y.Z`).

On Linux it picks the build that matches your C library:

- **glibc** distros get a binary linked against glibc 2.17, so it runs on Amazon Linux 2 and 2023, RHEL/CentOS 7 and later, Debian 10+, Ubuntu 18.04+ and anything newer.
- **musl** distros such as Alpine get a fully static musl binary. Alpine ships without bash, so pipe to `sh` instead: `apk add curl && curl -fsSL https://fakecloud.dev/install.sh | sh`.

Pass `--libc musl` or `--libc gnu` (or set `FAKECLOUD_LIBC`) to override the detection. The static musl build also runs on glibc systems.

Release assets are named `fakecloud-<version>-<os>-<arch>[-musl].tar.gz` (`linux-amd64`, `linux-arm64`, `linux-amd64-musl`, `linux-arm64-musl`, `darwin-amd64`, `darwin-arm64`), each with a `.sha256` next to it, if you prefer to download from [GitHub Releases](https://github.com/faiscadev/fakecloud/releases) by hand.

## Homebrew

```sh
brew install fakecloud
fakecloud
```

`fakecloud` is in [homebrew-core](https://formulae.brew.sh/formula/fakecloud), so no tap is needed. `brew upgrade fakecloud` tracks every release.

## Cargo

```sh
cargo install fakecloud
fakecloud
```

## From source

```sh
git clone https://github.com/faiscadev/fakecloud.git
cd fakecloud
cargo run --release --bin fakecloud
```

## Docker

```sh
docker run --rm -p 4566:4566 ghcr.io/faiscadev/fakecloud
```

To enable Lambda / RDS / ElastiCache / ECS function execution (real code in containers), mount the Docker socket **and** add the host-gateway alias so fakecloud can reach the sibling containers it spawns on the host's daemon:

```sh
docker run --rm -p 4566:4566 \
  -v /var/run/docker.sock:/var/run/docker.sock \
  --add-host host.docker.internal:host-gateway \
  ghcr.io/faiscadev/fakecloud
```

The image ships with the `docker` CLI installed and `FAKECLOUD_IN_CONTAINER=1` set, so fakecloud automatically reaches spawned Lambda containers via `host.docker.internal:<port>` instead of `127.0.0.1:<port>` (which would resolve to fakecloud's own loopback inside its container).

## Docker Compose

```yaml
# docker-compose.yml
services:
  fakecloud:
    image: ghcr.io/faiscadev/fakecloud
    ports:
      - "4566:4566"
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock # required for Lambda Invoke
    extra_hosts:
      - "host.docker.internal:host-gateway" # required for Lambda Invoke
    environment:
      FAKECLOUD_LOG: info
```

```sh
docker compose up
```

## Verify the install

fakecloud listens on port 4566 by default. Once it's running:

```sh
curl http://localhost:4566/_fakecloud/health
```

You should see a JSON response listing every service fakecloud is serving.

## Next

Point your AWS SDK at `http://localhost:4566` and run your first test — see [First test](/docs/getting-started/first-test/).
