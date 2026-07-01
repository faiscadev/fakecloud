<p align="center">
  <strong>fakecloud</strong><br>
  <em>Local AWS cloud emulator. Free forever.</em>
</p>

<p align="center">
  <a href="https://github.com/faiscadev/fakecloud/actions"><img src="https://img.shields.io/github/actions/workflow/status/faiscadev/fakecloud/ci.yml?branch=main&label=CI" alt="CI"></a>
  <a href="https://github.com/faiscadev/fakecloud/blob/main/LICENSE"><img src="https://img.shields.io/badge/license-AGPL--3.0-blue" alt="License"></a>
  <a href="https://github.com/faiscadev/fakecloud/pkgs/container/fakecloud"><img src="https://img.shields.io/badge/ghcr.io-fakecloud-blue?logo=docker" alt="GHCR"></a>
  <a href="https://crates.io/crates/fakecloud"><img src="https://img.shields.io/crates/v/fakecloud" alt="crates.io"></a>
  <a href="https://formulae.brew.sh/formula/fakecloud"><img src="https://img.shields.io/homebrew/v/fakecloud" alt="Homebrew"></a>
  <a href="https://fakecloud.dev"><img src="https://img.shields.io/badge/docs-fakecloud.dev-green" alt="Docs"></a>
</p>

---

fakecloud is a free, open-source local AWS emulator for integration testing and local development. Single binary, no account, no auth token, no paid tier. Point your AWS SDK at `http://localhost:4566` and it works.

In March 2026, LocalStack replaced its open-source Community Edition with a proprietary image that requires an account and auth token. fakecloud exists so teams can keep a fully local AWS testing workflow without one.

## Quick start

```sh
curl -fsSL https://fakecloud.dev/install.sh | bash   # or: brew install fakecloud
fakecloud
```

Then point any AWS SDK or CLI at `http://localhost:4566` with dummy credentials:

```sh
aws --endpoint-url http://localhost:4566 sqs create-queue --queue-name my-queue
```

Works as a drop-in for LocalStack in CI, with Terraform (`endpoints` block), CDK (`cdklocal`), or any AWS SDK in any language. Other install options (Homebrew, Cargo, Docker, Docker Compose, source) and full guides: [fakecloud.dev/docs/getting-started](https://fakecloud.dev/docs/getting-started).

## Why fakecloud

- **Free, forever.** AGPL-3.0, no paid tier, no account, no token.
- **True 100% conformance.** 126,556 Smithy-model-generated test variants pass on every commit, validated against AWS's own Smithy models. CI also runs upstream `terraform-provider-aws` `TestAcc*` suites to catch waiter/field/drift bugs that SDK tests miss.
- **Real cross-service wiring.** EventBridge -> Step Functions, S3 -> Lambda, SES inbound -> S3/SNS/Lambda, and 15+ more integrations execute end-to-end.
- **Real infrastructure for stateful services.** Lambda (23 runtimes), RDS (Postgres/MySQL/MariaDB/Oracle/SQL Server/Db2), ElastiCache (Redis/Valkey/Memcached), ECS, and EC2 run as real containers. Use Docker (default) or native Kubernetes Pods via `FAKECLOUD_CONTAINER_BACKEND=k8s`. See the [Kubernetes backend guide](https://fakecloud.dev/docs/guides/kubernetes-backend/).
- **Single binary.** ~19 MB, ~10 MiB idle, ~300ms startup. No Docker needed to run fakecloud itself.
- **Full Bedrock surface.** 214 ops across 4 APIs with real `InvokeModel`/`Converse` streaming, guardrails, agents, and flows. Configurable responses + fault injection for deterministic tests. See [`/bedrock-emulator/`](https://fakecloud.dev/bedrock-emulator/).
- **First-party test SDKs** for TypeScript, Python, Go, PHP, Java, and Rust. Assert on what your code called without raw HTTP.
- **Opt-in SigV4 verification and IAM enforcement.** Off by default so tests just work; `--verify-sigv4` for real signature checking and `--iam soft|strict` for policy evaluation across IAM/STS/SQS/SNS/S3. See [security docs](https://fakecloud.dev/docs/reference/security/).
- **LocalStack and real-AWS URL compatibility.** Both `*.localhost.localstack.cloud` and `*.amazonaws.com` Host headers route correctly, including every S3 virtual-hosted variant. Persisted URLs and dev scripts from either system replay unchanged.

## Supported services

46 services, 3,783 operations, true 100% conformance across every implemented service. Notes below are one-liners; the [parity matrix](https://fakecloud.dev/docs/parity/) has full control-plane vs data-plane coverage and known limitations per service.

| Service                   | Ops | Notes |
| ------------------------- | --- | ----- |
| S3                        | 107 | Versioning, lifecycle, notifications, multipart, replication, website, real SSE-KMS |
| SQS                       | 23  | FIFO, DLQs, long polling, batch, real KMS encrypt/decrypt |
| SNS                       | 42  | Fan-out to SQS/Lambda/HTTP, filter policies, KMS audit-trail |
| EventBridge               | 57  | Pattern matching, schedules, archives, replay, API destinations |
| EventBridge Scheduler     | 12  | at/rate/cron, SQS targets, DLQ routing, one-shot self-delete |
| Lambda                    | 70  | Real Docker, 23 runtimes, ESM with FilterCriteria + partial-batch failure |
| DynamoDB                  | 57  | Transactions, PartiQL, backups, global tables, streams, SSE-KMS |
| IAM                       | 176 | Users, roles, policies, groups, OIDC/SAML, PassRole trust enforcement |
| STS                       | 11  | AssumeRole, session tokens, federation |
| SSM                       | 146 | Parameters, documents, commands, maintenance, patch baselines, SecureString KMS |
| Secrets Manager           | 23  | Versioning, rotation via Lambda, replication, real KMS encrypt/decrypt |
| CloudWatch Logs           | 113 | Groups, streams, subscription filters, query language |
| CloudWatch                | 46  | Metrics + composite alarms, dashboards, anomaly detectors, insight rules, metric streams |
| KMS                       | 53  | Encryption, aliases, grants, real ECDH, key import, cross-service hook |
| CloudFormation            | 90  | Template parsing, resource provisioning, custom resources |
| Cloud Control API         | 8   | Uniform CRUD-L over CloudFormation resource types, JSON Patch updates, request tracking |
| SES (v2 + v1 inbound)     | 110 | Sending, templates, DKIM, real receipt rule execution |
| Cognito User Pools        | 122 | Pools, clients, MFA, IdPs, full auth flows, 12 Lambda triggers, email->SES SMS->SNS |
| Kinesis                   | 39  | Streams, records, shard iterators, retention |
| RDS                       | 163 | Real Postgres/MySQL/MariaDB/Oracle/SQL Server/Db2, `aws_lambda`/`aws_s3` extensions, Aurora lambda bridge |
| RDS Data API              | 6   | Real SQL on the backing Postgres/MySQL container, typed params/results incl. `bytea`/`BLOB`, transactions, batch |
| Aurora DSQL               | 16  | Serverless distributed Postgres control plane: clusters, resource policies, change streams to Kinesis, multi-region, tagging |
| Resource Groups           | 23  | Groups by tag/CloudFormation-stack query, explicit membership, group configuration, tagging, account settings, tag-sync tasks |
| ElastiCache               | 75  | Real Redis, Valkey, Memcached via Docker |
| Step Functions            | 37  | Full ASL interpreter, Lambda/SQS/SNS/EventBridge/DynamoDB tasks |
| API Gateway v1            | 124 | REST APIs, integrations incl. real Lambda proxy, authorizers (Lambda + Cognito JWT) |
| API Gateway v2            | 103 | HTTP APIs, routes, developer portals, authorizers (Lambda + Cognito JWT) |
| Bedrock                   | 101 | Foundation models, guardrails, custom models, invocation/eval jobs |
| Bedrock Runtime           | 10  | InvokeModel, Converse, streaming, configurable responses, fault inject |
| Bedrock Agent             | 72  | Agents, aliases, action groups, knowledge bases, ingestion, prompt mgmt, flows |
| Bedrock Agent Runtime     | 31  | InvokeAgent, Retrieve, RetrieveAndGenerate, InvokeFlow, streaming |
| ECR                       | 58  | Full API, real OCI v2 push/pull, lifecycle, scanning, pull-through |
| ECS                       | 76  | Full API, real task execution, services + rolling deploys, task sets, ECS Exec |
| Elastic Load Balancing v2 | 51  | ALB/NLB/GWLB control plane, mTLS trust stores, in-process HTTP data plane for ALBs |
| CloudFront                | 147 | Distributions, functions, policies, KVS, FLE, connection groups; full config round-trip |
| Route 53                  | 71  | Full control plane: zones, RRsets, health checks, traffic policies, DNSSEC, VPC assoc |
| WAF v2                    | 55  | Full control plane: WebACLs/RuleGroups/IPSets, CheckCapacity, managed rule catalog |
| Application Auto Scaling  | 14  | Scalable targets + step/target/predictive policies + scheduled actions, 13 namespaces |
| Athena                    | 70  | Workgroups, data catalogs, query executions (synthesized results), notebooks, sessions |
| ACM (Certificate Manager) | 17  | Public-cert lifecycle, DNS/EMAIL validation, import/export, tags |
| Glue                      | 265 | Full control plane; Data Catalog shared with Athena; execution synthesized (no Spark) |
| Firehose                  | 12  | Delivery stream control plane; data plane stops at acknowledgement |
| Organizations             | 63  | Full control plane, real SCP enforcement under `FAKECLOUD_IAM=strict` |
| EC2                       | 767 | Full 767-op control plane; instances run as real Docker/Podman/k8s with per-subnet isolation |

Per-service docs and feature matrices: [fakecloud.dev/docs/services](https://fakecloud.dev/docs/services).

## Compared to LocalStack Community

| Feature                   | fakecloud                                                              | LocalStack Community (post-March 2026) |
| ------------------------- | --------------------------------------------------------------------- | -------------------------------------- |
| License                   | AGPL-3.0                                                               | Proprietary |
| Auth required             | No                                                                     | Yes (account + token) |
| Commercial use            | Free                                                                   | Paid plans only |
| Docker required           | No (standalone binary)                                                 | Yes |
| Startup time              | ~300ms                                                                 | ~3s |
| Idle memory               | ~10 MiB                                                                | ~150 MiB |
| Install size              | ~19 MB binary                                                          | ~1 GB Docker image |
| Test assertion SDKs       | TypeScript, Python, Go, PHP, Java, Rust                               | Python, Java |
| Cognito User Pools        | 122 ops                                                                | [Paid only](https://docs.localstack.cloud/references/licensing/) |
| SES v2 + inbound          | Full send/templates/DKIM + real receipt-rule execution               | Paid only / [stored but never executed](https://docs.localstack.cloud/user-guide/aws/ses/) |
| RDS                       | 163 ops, 6 real engines via Docker, lambda + s3 extensions           | [Paid only](https://docs.localstack.cloud/references/licensing/) |
| RDS Data API              | 6 ops, real SQL + typed results + transactions on the RDS container  | [Paid only](https://docs.localstack.cloud/references/licensing/) |
| Aurora DSQL               | 16 ops, full control plane free (clusters, policies, streams, tagging) | [Paid only](https://docs.localstack.cloud/references/licensing/) |
| Resource Groups           | 23 ops, groups + queries + membership + tag-sync, free | [Paid only](https://docs.localstack.cloud/references/licensing/) |
| ElastiCache               | 75 ops, real Redis/Valkey/Memcached                                  | [Paid only](https://docs.localstack.cloud/references/licensing/) |
| API Gateway v1 / v2       | 124 / 103 ops, real Lambda proxy data plane                          | [Paid only](https://docs.localstack.cloud/references/licensing/) |
| Bedrock                   | 214 ops across 4 APIs                                                 | Not available |
| ECR / ECS                 | 58 / 76 ops, real `docker push`/`pull`, real task execution         | [Paid only](https://docs.localstack.cloud/references/licensing/) |
| Elastic Load Balancing v2 | 51 ops incl. mTLS + in-process HTTP data plane                       | [Paid only](https://docs.localstack.cloud/references/licensing/) |
| CloudFront                | 147 ops, full distribution config round-trip                         | [Paid only](https://docs.localstack.cloud/references/licensing/) |

> Performance numbers measured on Apple M1 via `time fakecloud`, `ps -o rss`, `ls -lh`.

## First-party SDKs

Normal AWS SDKs handle your application code. fakecloud's own SDKs let your tests assert on what happened: sent emails, SNS messages, Lambda invocations, Bedrock calls, and more.

| Language   | Install                                         |
| ---------- | ----------------------------------------------- |
| TypeScript | `npm install fakecloud`                         |
| Python     | `pip install fakecloud`                         |
| Go         | `go get github.com/faiscadev/fakecloud/sdks/go` |
| PHP        | `composer require fakecloud/fakecloud`          |
| Java       | `dev.fakecloud:fakecloud` (Maven Central)       |
| Rust       | `cargo add fakecloud-sdk`                       |

```ts
import { FakeCloud } from "fakecloud";

const fc = new FakeCloud();

// Your app sends through the normal AWS SDK.
// Your test asserts the side effect directly.
const { emails } = await fc.ses.getEmails();
expect(emails).toHaveLength(1);

await fc.reset();
```

Full SDK reference for all six languages: [fakecloud.dev/docs/sdks](https://fakecloud.dev/docs/sdks).

## Use with AI coding tools

Add one of these snippets to your project so AI coding assistants (Claude Code, Cursor, GitHub Copilot) automatically use fakecloud for AWS integration tests.

<details>
<summary><strong>Claude Code</strong> — add to <code>CLAUDE.md</code></summary>

```markdown
## AWS Testing

For integration tests, use fakecloud as a local AWS emulator.

- Install: `curl -fsSL https://fakecloud.dev/install.sh | bash`
- Run: `fakecloud` (listens on http://localhost:4566)
- Configure AWS SDK endpoint to http://localhost:4566 with dummy credentials (access_key=test, secret_key=test)
- Docs: https://fakecloud.dev/llms.txt
```

</details>

<details>
<summary><strong>Cursor</strong> — add to <code>.cursor/rules</code></summary>

```
For AWS integration tests, use fakecloud (local AWS emulator) at http://localhost:4566.
Install: curl -fsSL https://fakecloud.dev/install.sh | bash
Run: fakecloud
Use dummy credentials (access_key=test, secret_key=test).
Docs: https://fakecloud.dev/llms.txt
```

</details>

<details>
<summary><strong>GitHub Copilot</strong> — add to <code>.github/copilot-instructions.md</code></summary>

```markdown
## AWS Testing

Use fakecloud as a local AWS emulator for integration tests.

- Run: `fakecloud` (listens on http://localhost:4566)
- Configure endpoint_url to http://localhost:4566 with dummy credentials
- Docs: https://fakecloud.dev/llms.txt
```

</details>

## Docs and guides

- **[fakecloud.dev](https://fakecloud.dev)** — website
- **[Getting started](https://fakecloud.dev/docs/getting-started)** — install, first test, SDK setup
- **[Guides](https://fakecloud.dev/docs/guides)** — in-depth how-tos (testing Bedrock, cross-service integration, CI setup)
- **[Reference](https://fakecloud.dev/docs/reference)** — configuration, introspection endpoints, persistence, [image signature verification](https://fakecloud.dev/docs/reference/security/#image-supply-chain-cosign--trivy)
- **[Blog](https://fakecloud.dev/blog)** — essays and hot takes on testing, AWS, and AI-assisted development

## Contributing

Contributions welcome. Fork, branch, write tests, open a PR.

- Conventional commits (`feat:`, `fix:`, `chore:`, `test:`, `refactor:`)
- E2E tests for every new action
- `cargo test --workspace && cargo clippy --workspace -- -D warnings && cargo fmt --check`

See [CONTRIBUTING.md](CONTRIBUTING.md) for more.

## License

fakecloud is free and open-source, licensed under [AGPL-3.0-or-later](https://www.gnu.org/licenses/agpl-3.0.html). Using fakecloud as a dev/test dependency has zero AGPL implications for your application — the copyleft clause only applies if you modify and redistribute fakecloud itself as a network service.

---

<p align="center">
  Part of the <a href="https://faisca.dev">faisca</a> project family | <a href="https://fakecloud.dev">fakecloud.dev</a>
</p>
