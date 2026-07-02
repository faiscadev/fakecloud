+++
title = "fakecloud vs floci"
description = "How fakecloud compares to floci. Both free, open-source AWS emulators positioned as LocalStack replacements."
template = "page.html"
+++

floci is a free, open-source AWS emulator that appeared after LocalStack replaced its Community Edition with a proprietary image in March 2026. fakecloud is another option in the same space.

This page is honest positioning. I maintain fakecloud. What follows is architectural — what each project *is* — rather than side-by-side benchmarks I haven't personally measured across both.

## Shared fundamentals

Both are free, open-source, local AWS emulators. Real HTTP server speaking the AWS wire protocol. Any AWS SDK in any language works. Both are replacements for LocalStack Community for the services each covers.

## The positioning difference

**floci's approach** (check their site for the current details — the project publishes performance claims like startup time, memory, and SDK-test pass rate): a free LocalStack replacement. Verify their numbers against the version you'd actually run.

**fakecloud's approach:** depth-first, explicit goal. 100% of AWS services, each at 100% behavioral conformance, with 100% of cross-service integrations. A service is added when it passes the full Smithy-model test variants and cross-service wire-ups, not when the API surface looks filled in. 49 services shipped today (including full ECR with OCI v2 `docker push`/`pull`, full ECS, full ELBv2 ALB/NLB/GWLB control plane). Built around real Lambda execution (23 runtimes in Docker), real stateful backends (Postgres/MySQL/MariaDB/Redis/Valkey/Memcached via Docker), real cross-service wiring, and validation on every commit against AWS's own Smithy models — 129,882/129,882 generated test variants pass, true 100% conformance — plus the upstream `hashicorp/terraform-provider-aws` `TestAcc*` suites.

Breadth-first and depth-first are both valid tradeoffs. Pick by whether your tests need real downstream execution and cross-service flows, or surface-level plausibility across more services.

## How to pick

Run your actual test suite against both. Numbers published on landing pages are marketing; numbers your tests produce against your services are signal.

## fakecloud specifics

| Feature | fakecloud |
|---|---|
| Language | Rust |
| Distribution | Single static binary (~19 MB) + Docker image |
| Startup | ~300ms |
| Idle memory | ~10 MiB |
| Services covered today | 49 (3,902 ops) at true 100% conformance (129,882/129,882 variants), incl. ECR + ECS + ELBv2 |
| Lambda execution | Real code in 13 Docker runtime containers |
| RDS | Real PostgreSQL/MySQL/MariaDB via Docker |
| ElastiCache | Real Redis/Valkey/Memcached via Docker |
| Cross-service wiring | S3 -> Lambda, SNS fan-out, EventBridge -> Step Functions, SES inbound -> S3/SNS/Lambda, 15+ more fire end-to-end |
| Conformance methodology | Smithy-validated, 129,882/129,882 test variants pass on every commit |
| Terraform TestAcc CI | Yes (upstream suites run against fakecloud) |
| Test-assertion SDKs | TypeScript, Python, Go, PHP, Java, Rust |
| Multi-account, SCPs, ABAC | Yes |
| License | AGPL-3.0 |

## Install fakecloud

```sh
curl -fsSL https://fakecloud.dev/install.sh | bash
fakecloud
```

- **Repo:** [github.com/faiscadev/fakecloud](https://github.com/faiscadev/fakecloud)
- **LocalStack alternative landing:** [/localstack-alternative/](/localstack-alternative/)
- **Four-way comparison:** [/blog/localstack-alternatives-compared/](/blog/localstack-alternatives-compared/)
