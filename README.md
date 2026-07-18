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

<p align="center">
  <a href="https://www.npmjs.com/package/fakecloud"><img src="https://img.shields.io/npm/dm/fakecloud?label=npm%20downloads" alt="npm downloads"></a>
  <a href="https://pypi.org/project/fakecloud/"><img src="https://img.shields.io/pypi/dm/fakecloud?label=pypi%20downloads" alt="PyPI downloads"></a>
  <a href="https://crates.io/crates/fakecloud"><img src="https://img.shields.io/crates/d/fakecloud?label=crates.io%20downloads" alt="crates.io downloads"></a>
  <a href="https://formulae.brew.sh/formula/fakecloud"><img src="https://img.shields.io/homebrew/installs/dm/fakecloud?label=brew%20installs" alt="Homebrew installs"></a>
  <a href="https://github.com/faiscadev/fakecloud/stargazers"><img src="https://img.shields.io/github/stars/faiscadev/fakecloud" alt="GitHub stars"></a>
</p>

---

fakecloud is a free, open-source local AWS emulator for integration testing and local development. Single binary, no account, no auth token, no paid tier. Point your AWS SDK at `http://localhost:4566` and it works.

In March 2026, LocalStack replaced its open-source Community Edition with a proprietary image that requires an account and auth token. fakecloud exists so teams can keep a fully local AWS testing workflow without one.

<!-- Seeding banner: remove once ADOPTERS.md has ~5 entries. -->
> [!NOTE]
> **Using fakecloud?** Add your team to [ADOPTERS.md](ADOPTERS.md). Takes 30 seconds, and it helps others see it is real.

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
- **True 100% conformance.** 248,319 Smithy-model-generated test variants pass on every commit, validated against AWS's own Smithy models. CI also runs upstream `terraform-provider-aws` `TestAcc*` suites to catch waiter/field/drift bugs that SDK tests miss.
- **Real cross-service wiring.** EventBridge -> Step Functions, S3 -> Lambda, SES inbound -> S3/SNS/Lambda, and 15+ more integrations execute end-to-end.
- **Real infrastructure for stateful services.** Lambda (23 runtimes), RDS (Postgres/MySQL/MariaDB/Oracle/SQL Server/Db2), ElastiCache (Redis/Valkey/Memcached), ECS, and EC2 run as real containers. Use Docker (default) or native Kubernetes Pods via `FAKECLOUD_CONTAINER_BACKEND=k8s`. See the [Kubernetes backend guide](https://fakecloud.dev/docs/guides/kubernetes-backend/).
- **Single binary.** ~19 MB, ~10 MiB idle, ~300ms startup. No Docker needed to run fakecloud itself.
- **Full Bedrock surface.** 216 ops across 4 APIs with real `InvokeModel`/`Converse` streaming, guardrails, agents, and flows. Configurable responses + fault injection for deterministic tests. See [`/bedrock-emulator/`](https://fakecloud.dev/bedrock-emulator/).
- **First-party test SDKs** for TypeScript, Python, Go, PHP, Java, and Rust. Assert on what your code called without raw HTTP.
- **Opt-in SigV4 verification and IAM enforcement.** Off by default so tests just work; `--verify-sigv4` for real signature checking and `--iam soft|strict` for policy evaluation across IAM/STS/SQS/SNS/S3. See [security docs](https://fakecloud.dev/docs/reference/security/).
- **Run your app unmodified.** An app that expects an instance/task role resolves the AWS SDK default credential chain against fakecloud with no static keys and no code change: point `AWS_CONTAINER_CREDENTIALS_FULL_URI` at `/_fakecloud/credentials`. See [Run an app unmodified](https://fakecloud.dev/docs/guides/instance-credentials/).
- **LocalStack and real-AWS URL compatibility.** Both `*.localhost.localstack.cloud` and `*.amazonaws.com` Host headers route correctly, including every S3 virtual-hosted variant. Persisted URLs and dev scripts from either system replay unchanged.

## Supported services

105 services, 7,379 operations, and true 100% conformance across every implemented service.

Highlights: S3, DynamoDB, SQS, SNS, EventBridge, Lambda, IAM, STS, KMS, Secrets Manager, CloudFormation, SES, Cognito, Kinesis, RDS (6 real engines), ElastiCache, ECS/ECR, EC2, Step Functions, API Gateway v1/v2, Bedrock, and 80+ more.

Full list with per-service op counts, control-plane vs data-plane coverage, and known limitations:

- Parity matrix: [fakecloud.dev/docs/parity](https://fakecloud.dev/docs/parity/)
- Per-service docs: [fakecloud.dev/docs/services](https://fakecloud.dev/docs/services)

## Compared to LocalStack Community

Since March 2026, LocalStack's Community image requires an account and token, and most services are paid-only. fakecloud stays fully free and local.

| | fakecloud | LocalStack Community (post-March 2026) |
| --- | --- | --- |
| License | AGPL-3.0, free for commercial use | Proprietary, paid plans |
| Auth | None | Account + token required |
| Footprint | ~19 MB binary, ~10 MiB idle, ~300ms start, no Docker | ~1 GB image, ~150 MiB idle, ~3s start, Docker required |
| Conformance | true 100% (248,319 Smithy variants) | partial |
| Paywalled services | None (RDS, Cognito, SES, ElastiCache, ECS/ECR, EKS, Redshift, and more are all free) | Many core services paid-only |
| Test-assertion SDKs | TypeScript, Python, Go, PHP, Java, Rust | Python, Java |

Full feature-by-feature comparison: [fakecloud.dev/vs/localstack](https://fakecloud.dev/vs/localstack/).

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

## Who's using fakecloud

See the current list in [ADOPTERS.md](ADOPTERS.md). It is early days, so it is short for now.

Using fakecloud? Add your team: the preferred way is a quick PR adding a row to ADOPTERS.md. If a PR is inconvenient, comment on the ["Who's using fakecloud?"](https://github.com/faiscadev/fakecloud/issues/2324) issue instead. Self-reported and opt-in.

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
