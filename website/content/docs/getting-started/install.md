+++
title = "Install"
description = "Install fakecloud via script, Cargo, Docker, Docker Compose, or from source."
weight = 1
+++

fakecloud ships as a single ~19 MB binary. Pick whichever install path fits your workflow.

## Install script (recommended)

```sh
curl -fsSL https://fakecloud.dev/install.sh | bash
fakecloud
```

The script downloads the latest release for your platform and puts the `fakecloud` binary somewhere on your `PATH`.

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

To enable Lambda function execution (real code in containers), mount the Docker socket:

```sh
docker run --rm -p 4566:4566 -v /var/run/docker.sock:/var/run/docker.sock ghcr.io/faiscadev/fakecloud
```

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
