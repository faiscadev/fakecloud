+++
title = "fakecloud vs Testcontainers"
description = "How fakecloud compares to Testcontainers. Testcontainers manages throwaway Docker containers for tests; fakecloud is an AWS emulator that Testcontainers can run."
template = "page.html"
+++

Testcontainers is not a direct competitor — it's a test-container orchestration library for spinning up throwaway Docker containers for integration tests (PostgreSQL, Redis, Kafka, LocalStack, etc.). It provides the plumbing for managing container lifecycles in tests.

fakecloud is one of the AWS emulators Testcontainers can run. They're complementary.

## How to use them together

Testcontainers has modules for local AWS emulation — historically the LocalStack module. fakecloud has a Rust Testcontainers module in progress: [testcontainers-rs-modules-community PR #467](https://github.com/testcontainers/testcontainers-rs-modules-community/pull/467).

Without the dedicated module, any Testcontainers flavor (Java, Python, Go, Rust, Node) can run fakecloud as a generic container:

```java
// Java
GenericContainer<?> fakecloud = new GenericContainer<>("ghcr.io/faiscadev/fakecloud:latest")
    .withExposedPorts(4566)
    .waitingFor(Wait.forHttp("/_fakecloud/health"));
fakecloud.start();
String endpoint = "http://" + fakecloud.getHost() + ":" + fakecloud.getMappedPort(4566);
```

```python
# Python
from testcontainers.core.container import DockerContainer
fakecloud = DockerContainer("ghcr.io/faiscadev/fakecloud:latest") \
    .with_exposed_ports(4566)
fakecloud.start()
endpoint = f"http://{fakecloud.get_container_host_ip()}:{fakecloud.get_exposed_port(4566)}"
```

```go
// Go
req := testcontainers.ContainerRequest{
    Image:        "ghcr.io/faiscadev/fakecloud:latest",
    ExposedPorts: []string{"4566/tcp"},
    WaitingFor:   wait.ForHTTP("/_fakecloud/health"),
}
container, _ := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{ContainerRequest: req, Started: true})
```

## When to use fakecloud standalone (no Testcontainers)

- Simple CI setup — install-and-run is ~300ms vs ~2-3s container boot.
- Local dev where you don't need per-test container throwaway.
- Single-service CI jobs.

## When to use fakecloud via Testcontainers

- Java / JVM codebases where Testcontainers is already the test-infra convention.
- Per-test container isolation is a requirement.
- You already use Testcontainers for Postgres / Redis / Kafka and want fakecloud in the same lifecycle pattern.
- You need the container lifecycle to match the test class / method / session.

## Testcontainers module status

- **Java**: fakecloud runs as `GenericContainer` today; dedicated module is a future item.
- **Python**: same (GenericContainer works now).
- **Rust**: dedicated module [PR #467](https://github.com/testcontainers/testcontainers-rs-modules-community/pull/467) open.
- **Go**: generic container works.
- **Node**: generic container works via [testcontainers-node](https://github.com/testcontainers/testcontainers-node).

## Links

- [fakecloud GitHub](https://github.com/faiscadev/fakecloud)
- [Testcontainers homepage](https://testcontainers.com)
- [testcontainers-rs-modules-community PR #467](https://github.com/testcontainers/testcontainers-rs-modules-community/pull/467)
- [Integration testing AWS in CI](/blog/integration-testing-aws-in-ci/)
