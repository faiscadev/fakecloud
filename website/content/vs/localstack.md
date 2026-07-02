+++
title = "fakecloud vs LocalStack"
description = "How fakecloud compares to LocalStack Community (post-March 2026) and LocalStack Pro. Honest positioning, feature table, migration path."
template = "page.html"
aliases = [
    "/compare/localstack/",
    "/comparison/localstack/",
    "/alternatives/localstack/",
    "/blog/benchmarking-local-aws-500ms-startup-vs-cloud-dependent-mocks/",
    "/blog/benchmarking-local-aws-emulators-fakecloud-vs-localstack-and-moto/",
    "/blog/benchmarking-local-aws-emulators-500ms-vs-15s-startup-latency/",
]
+++

Since LocalStack replaced its open-source Community Edition with a proprietary image in March 2026, `localstack:latest` now requires an account + auth token, and several previously-free services (Cognito, SES v2, RDS, ElastiCache, API Gateway v2, ECS/ECR) moved to the paid LocalStack Pro tier.

**fakecloud is a free, open-source replacement.** Single binary, no account, no token, AGPL-3.0.

## At a glance

| | fakecloud | LocalStack Community (post-Mar 2026) | LocalStack Pro |
|---|---|---|---|
| License | AGPL-3.0 | Proprietary | Proprietary, paid |
| Account / auth token | No | **Required** | Required |
| Commercial use | Free | **Not allowed** | Paid plans only |
| Docker required | No (single binary) | Yes | Yes |
| Startup | ~300ms | ~3s | ~3s |
| Idle memory | ~10 MiB | ~150 MiB | ~150 MiB |
| Install size | ~19 MB | ~1 GB Docker image | ~1 GB Docker image |
| Conformance methodology | Smithy-validated, 128,643/128,643 test variants pass on every commit | Not published | Not published |
| Terraform TestAcc CI | Yes (upstream suites run against fakecloud) | Not published | Not published |
| Test-assertion SDKs | TypeScript, Python, Go, PHP, Java, Rust | Python, Java | Python, Java |
| Cognito User Pools | 122 ops, full auth flows | [Paid only](https://docs.localstack.cloud/references/licensing/) | Yes |
| SES v2 | 110 ops, send + templates + DKIM | [Paid only](https://docs.localstack.cloud/references/licensing/) | Yes |
| SES inbound email | Real receipt rule action execution | [Stored but never executed](https://docs.localstack.cloud/user-guide/aws/ses/) | Stored but never executed |
| RDS | 163 ops, real PostgreSQL/MySQL/MariaDB | [Paid only](https://docs.localstack.cloud/references/licensing/) | Yes |
| ElastiCache | 75 ops, real Redis/Valkey/Memcached | [Paid only](https://docs.localstack.cloud/references/licensing/) | Yes |
| API Gateway v1 | 124 ops | [Paid only](https://docs.localstack.cloud/references/licensing/) | Yes |
| API Gateway v2 | 103 ops | [Paid only](https://docs.localstack.cloud/references/licensing/) | Yes |
| ECR | 58 ops + real `docker push`/`pull` via OCI v2 | [Paid only](https://docs.localstack.cloud/references/licensing/) (and push is [flaky](https://github.com/localstack/localstack/issues/8128) when paid) | Yes |
| Bedrock | 214 ops across Bedrock + Bedrock Runtime + Bedrock Agent + Bedrock Agent Runtime | Not available | Not available |
| SCPs / multi-account | Yes (Organizations control plane + ceiling enforcement) | No | Partial |
| Lambda real code execution | Yes (23 runtimes) | Paywall required | Yes |

## Approach difference

**LocalStack's approach** is breadth-first — a very large catalog of AWS services at varying depth. Good for "tests need the call to resolve plausibly."

**fakecloud's approach** is depth-first — fewer services today (40), each at 100% behavioral conformance with 100% of cross-service integrations. Good for "tests need the downstream actually to happen."

Both are valid. Pick by whether your tests need real cross-service wiring, real Lambda execution, and real stateful backends, or whether you need surface-level plausibility across more services.

## Migrating from LocalStack

Most projects migrate in under 10 minutes. Full guide: [Migrating from LocalStack to fakecloud](/blog/migrate-from-localstack/).

One-liner version:

```diff
# docker-compose.yml
 services:
   fakecloud:
-    image: localstack/localstack:latest
+    image: ghcr.io/faiscadev/fakecloud:latest
     ports:
       - "4566:4566"
```

Endpoint URL, dummy credentials, all SDK wiring unchanged.

## Install

```sh
curl -fsSL https://fakecloud.dev/install.sh | bash
fakecloud
```

- **Repo:** [github.com/faiscadev/fakecloud](https://github.com/faiscadev/fakecloud)
- **Docs:** [fakecloud.dev/docs/](/docs/)
- **Migration guide:** [/blog/migrate-from-localstack/](/blog/migrate-from-localstack/)
- **LocalStack alternative landing:** [/localstack-alternative/](/localstack-alternative/)
