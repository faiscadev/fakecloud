+++
title = "fakecloud vs MiniStack"
description = "How fakecloud compares to MiniStack. Both free, open-source AWS emulators that surfaced after LocalStack's March 2026 proprietary transition."
template = "page.html"
+++

MiniStack is one of the free, open-source AWS emulators that gained momentum after LocalStack replaced its Community Edition with a proprietary image in March 2026. fakecloud is another.

This page is honest positioning. I maintain fakecloud, so the bias is declared. What follows is architectural — what each project *is* — rather than fabricated side-by-side benchmarks (the surest way to get out of date in days).

## Both solve the same general problem

Free, open-source, local AWS emulation. Real HTTP server speaking the AWS wire protocol on port 4566. Any AWS SDK in any language works. Both are drop-in replacements for LocalStack Community for the services each covers.

## The positioning difference

**MiniStack's approach** (check their repo for current details — it's moving fast): positions as a free LocalStack replacement. Evaluate their service coverage and architecture directly against your test suite.

**fakecloud's approach:** depth-first, explicit goal. 100% of AWS services, each at 100% behavioral conformance, with 100% of cross-service integrations. Services land one at a time; a service is added when it passes the full Smithy-model test variants and cross-service wire-ups, not when the API surface looks filled in. 54 services shipped today (including full ECR with OCI v2 `docker push`/`pull`, full ECS, full ELBv2 ALB/NLB/GWLB control plane); more progressively. Built around real Lambda execution, real stateful backends (Postgres/MySQL/MariaDB/Redis/Valkey/Memcached via Docker), and real cross-service wiring, validated on every commit against AWS's own Smithy models — 136,154/136,154 generated test variants pass, true 100% conformance — plus the upstream `hashicorp/terraform-provider-aws` `TestAcc*` suites.

These are philosophies, not rankings. Breadth-first and depth-first are different tradeoffs. A team whose tests lean lightly on many services will prefer breadth. A team whose tests exercise real cross-service flows or need real code execution will prefer depth.

## How to pick

1. **Open your test suite.** Count the AWS services it actually calls.
2. **Check each tool's current supported-services list** against that set.
3. **For the services you use, check depth:** does the tool actually execute Lambda code? Actually run Postgres? Actually fire S3 -> Lambda notifications end-to-end?
4. **Run your actual tests against each option you're considering.** That's the only benchmark that matters for your codebase.

## fakecloud specifics

| Feature | fakecloud |
|---|---|
| Language | Rust |
| Distribution | Single static binary (~19 MB) + Docker image |
| Startup | ~300ms |
| Idle memory | ~10 MiB |
| Services covered today | 47 (3,966 ops) at true 100% conformance (136,154/136,154 variants), incl. ECR + ECS + ELBv2 |
| Lambda execution | Real, 23 runtimes in Docker |
| RDS | Real PostgreSQL/MySQL/MariaDB via Docker |
| ElastiCache | Real Redis/Valkey/Memcached via Docker |
| Conformance methodology | Smithy-validated, 136,154/136,154 test variants pass on every commit |
| Terraform TestAcc CI | Yes (upstream suites run against fakecloud) |
| Test-assertion SDKs | TypeScript, Python, Go, PHP, Java, Rust |
| License | AGPL-3.0 |

## Install fakecloud

```sh
curl -fsSL https://fakecloud.dev/install.sh | bash
fakecloud
```

- **Repo:** [github.com/faiscadev/fakecloud](https://github.com/faiscadev/fakecloud)
- **LocalStack alternative landing:** [/localstack-alternative/](/localstack-alternative/)
- **Four-way comparison:** [/blog/localstack-alternatives-compared/](/blog/localstack-alternatives-compared/)
