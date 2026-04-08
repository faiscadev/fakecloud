<p align="center">
  <strong>fakecloud</strong><br>
  <em>Local AWS cloud emulator. Free forever.</em>
</p>

<p align="center">
  <a href="https://github.com/faiscadev/fakecloud/actions"><img src="https://img.shields.io/github/actions/workflow/status/faiscadev/fakecloud/ci.yml?branch=main&label=CI" alt="CI"></a>
  <a href="https://github.com/faiscadev/fakecloud/blob/main/LICENSE"><img src="https://img.shields.io/badge/license-AGPL--3.0-blue" alt="License"></a>
  <a href="https://github.com/faiscadev/fakecloud/pkgs/container/fakecloud"><img src="https://img.shields.io/badge/ghcr.io-fakecloud-blue?logo=docker" alt="GHCR"></a>
  <a href="https://crates.io/crates/fakecloud"><img src="https://img.shields.io/crates/v/fakecloud" alt="crates.io"></a>
  <a href="https://fakecloud.dev"><img src="https://img.shields.io/badge/docs-fakecloud.dev-green" alt="Docs"></a>
</p>

---

fakecloud is a free, open-source local AWS cloud emulator. It runs on a single port
(`4566`), requires no account or auth token, and aims to faithfully replicate
AWS service behavior for local development and testing.

Part of the [faisca project family](https://faisca.dev).

## Why fakecloud?

In March 2026, LocalStack replaced its open-source Community Edition with a
proprietary image that requires an account and auth token.

### Comparison

| Feature | fakecloud | LocalStack Community |
|---|---|---|
| License | AGPL-3.0 | Proprietary |
| Auth required | No | Yes (account + token) |
| Commercial use | Free | Paid plans only |
| Docker required | No (standalone binary) | Yes |
| Startup time | ~500ms | ~3s |
| Idle memory | ~10 MiB | ~150 MiB |
| Install size | ~19 MB binary | ~1 GB Docker image |
| AWS services | 15 | 30+ |
| Cognito User Pools | 80 operations | [Paid only](https://docs.localstack.cloud/references/licensing/) |
| SES v2 | 97 operations | [Paid only](https://docs.localstack.cloud/references/licensing/) |
| SES inbound email | Real receipt rule action execution | [Stored but never executed](https://docs.localstack.cloud/user-guide/aws/ses/) |

## Quick Start

### Install script (recommended)

```sh
curl -fsSL https://raw.githubusercontent.com/faiscadev/fakecloud/main/install.sh | bash
fakecloud
```

### Cargo install

```sh
cargo install fakecloud
fakecloud
```

### From source

```sh
git clone https://github.com/faiscadev/fakecloud.git
cd fakecloud
cargo run --release --bin fakecloud
```

### Docker

```sh
docker run --rm -p 4566:4566 ghcr.io/faiscadev/fakecloud
```

To enable Lambda function execution, mount the Docker socket:

```sh
docker run --rm -p 4566:4566 -v /var/run/docker.sock:/var/run/docker.sock ghcr.io/faiscadev/fakecloud
```

### Docker Compose

```yaml
# docker-compose.yml
services:
  fakecloud:
    image: ghcr.io/faiscadev/fakecloud
    ports:
      - "4566:4566"
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock  # required for Lambda Invoke
    environment:
      FAKECLOUD_LOG: info
```

```sh
docker compose up
```

fakecloud is now listening at `http://localhost:4566`.

## Supported Services

15 AWS services, 922 API operations:

| Service | Actions | Highlights |
|---|---|---|
| **S3** | 74 | Objects, multipart uploads, versioning, lifecycle, notifications, encryption, replication, website hosting |
| **SQS** | 20 | FIFO queues, dead-letter queues, long polling, batch operations, MD5 hashing |
| **SNS** | 34 | Fan-out to SQS/Lambda/HTTP, filter policies, platform applications |
| **EventBridge** | 57 | Pattern matching, scheduled rules, archives, replay, connections, API destinations |
| **IAM** | 128 | Users, roles, policies, groups, instance profiles, OIDC/SAML providers |
| **STS** | 8 | AssumeRole, session tokens, federation |
| **SSM** | 146 | Parameters, documents, commands, maintenance windows, associations, patch baselines |
| **DynamoDB** | 57 | Tables, items, transactions, PartiQL, backups, global tables, exports/imports |
| **Lambda** | 10 | Function CRUD, real code execution via Docker, event source mappings |
| **Secrets Manager** | 23 | Versioning, soft delete, rotation with Lambda, replication |
| **CloudWatch Logs** | 113 | Groups, streams, filtering, deliveries, transformers, query language, anomaly detection |
| **KMS** | 53 | Encryption, key management, aliases, grants, real ECDH and key import |
| **CloudFormation** | 8 | Template parsing, resource provisioning, custom resources via Lambda |
| **SES** | 111 | **v2** (97 ops): identities, templates, configuration sets, contact lists, send email, suppression list, event destinations, DKIM/feedback/mail-from attributes, dedicated IP pools, account settings, import/export jobs, event fanout (SNS/EventBridge), mailbox simulator. **v1 inbound** (14 ops): receipt rule sets, receipt rules, receipt filters, inbound email pipeline with S3/SNS/Lambda actions |
| **Cognito User Pools** | 80 | User pools, app clients, users, groups, MFA, identity providers, resource servers, domains, devices, authentication flows, password management |

### Cross-Service Integration

Services talk to each other — this is the kind of behavior that matters in
integration tests:

- **SNS -> SQS/Lambda/HTTP**: Fan-out delivery to all subscription types
- **EventBridge -> SNS/SQS/Lambda/Logs**: Rules deliver to targets on schedule or event match
- **S3 -> SNS/SQS/Lambda**: Bucket notifications on object create/delete
- **SQS -> Lambda**: Event source mapping polls and invokes
- **SecretsManager -> Lambda**: Rotation invokes Lambda for all 4 steps
- **SES -> SNS/EventBridge**: Email event fanout (send, delivery, bounce, complaint) via configured event destinations
- **SES Inbound -> S3/SNS/Lambda**: Receipt rules evaluate inbound email and execute S3, SNS, and Lambda actions
- **CloudFormation -> Lambda**: Custom resources invoke via ServiceToken
- **S3 Lifecycle**: Background expiration and storage class transitions
- **EventBridge Scheduler**: Cron and rate-based rules fire on schedule

## Configuration

fakecloud is configured via CLI flags or environment variables.

| Flag | Env Var | Default | Description |
|---|---|---|---|
| `--addr` | `FAKECLOUD_ADDR` | `0.0.0.0:4566` | Listen address and port |
| `--region` | `FAKECLOUD_REGION` | `us-east-1` | AWS region to advertise |
| `--account-id` | `FAKECLOUD_ACCOUNT_ID` | `123456789012` | AWS account ID |
| `--log-level` | `FAKECLOUD_LOG` | `info` | Log level (trace, debug, info, warn, error) |
| | `FAKECLOUD_CONTAINER_CLI` | auto-detect | Container CLI to use (`docker` or `podman`) |

```sh
# Examples
fakecloud --addr 127.0.0.1:5000 --log-level debug
FAKECLOUD_LOG=trace cargo run --bin fakecloud
```

## Health Check

```sh
curl http://localhost:4566/_fakecloud/health
```

```json
{
  "status": "ok",
  "version": "0.3.0",
  "services": ["cloudformation", "cognito-idp", "dynamodb", "sqs", "sns", "events", "iam", "sts", "ssm", "lambda", "secretsmanager", "logs", "kms", "s3", "ses"]
}
```

## Simulation Endpoints

fakecloud exposes `/_fakecloud/*` endpoints for testing behaviors that AWS runs asynchronously (TTL expiration, scheduled rotation, etc.). Call these to advance time-dependent processes on demand.

| Endpoint | Method | Description |
|---|---|---|
| `/_fakecloud/dynamodb/ttl-processor/tick` | POST | Expire DynamoDB items whose TTL attribute is in the past. Returns `{"expiredItems": N}`. |
| `/_fakecloud/secretsmanager/rotation-scheduler/tick` | POST | Rotate secrets whose rotation schedule is due. Returns `{"rotatedSecrets": ["name", ...]}`. |
| `/_fakecloud/s3/lifecycle-processor/tick` | POST | Run one S3 lifecycle processing tick. Returns `{"processedBuckets": N, "expiredObjects": N, "transitionedObjects": N}`. |
| `/_fakecloud/sqs/expiration-processor/tick` | POST | Expire SQS messages past retention period. Returns `{"expiredMessages": N}`. |
| `/_fakecloud/sqs/{queue_name}/force-dlq` | POST | Force-move messages exceeding maxReceiveCount to DLQ. Returns `{"movedMessages": N}`. |
| `/_fakecloud/events/fire-rule` | POST | Fire an EventBridge rule regardless of state. Body: `{"busName": "...", "ruleName": "..."}`. |
| `/_fakecloud/lambda/invocations` | GET | List all Lambda invocations (introspection). |
| `/_fakecloud/lambda/warm-containers` | GET | List all warm Lambda containers. Returns `{"containers": [...]}`. |
| `/_fakecloud/lambda/{function-name}/evict-container` | POST | Evict a warm Lambda container (forces cold start). Returns `{"evicted": true/false}`. |
| `/_fakecloud/sns/messages` | GET | List all published SNS messages. |
| `/_fakecloud/sns/pending-confirmations` | GET | List SNS subscriptions pending confirmation. Returns `{"pendingConfirmations": [...]}`. |
| `/_fakecloud/sns/confirm-subscription` | POST | Force-confirm an SNS subscription. Body: `{"subscriptionArn": "..."}`. Returns `{"confirmed": true}`. |
| `/_fakecloud/sqs/messages` | GET | List all SQS messages across queues. |
| `/_fakecloud/events/history` | GET | List all EventBridge events and deliveries. |
| `/_fakecloud/s3/notifications` | GET | List all S3 notification events. |
| `/_fakecloud/ses/emails` | GET | List all sent SES emails. |
| `/_fakecloud/ses/inbound` | POST | Simulate receiving an inbound email. Evaluates receipt rules and executes actions. |
| `/_fakecloud/reset` | POST | Reset all state across all services. |
| `/_fakecloud/reset/{service}` | POST | Reset only the specified service's state. Returns `{"reset": "service_name"}`. |

## Architecture

fakecloud is organized as a Cargo workspace:

| Crate | Purpose |
|---|---|
| `fakecloud` | Binary entry point (clap CLI, Axum HTTP server) |
| `fakecloud-core` | `AwsService` trait, service registry, request dispatch, protocol parsing |
| `fakecloud-aws` | Shared AWS types (ARNs, error builders, SigV4 parser) |
| `fakecloud-sqs` | SQS implementation |
| `fakecloud-sns` | SNS implementation with delivery |
| `fakecloud-eventbridge` | EventBridge implementation with scheduler |
| `fakecloud-iam` | IAM and STS implementation |
| `fakecloud-ssm` | SSM Parameter Store implementation |
| `fakecloud-dynamodb` | DynamoDB implementation |
| `fakecloud-lambda` | Lambda implementation with Docker-based execution |
| `fakecloud-secretsmanager` | Secrets Manager implementation |
| `fakecloud-s3` | S3 implementation |
| `fakecloud-logs` | CloudWatch Logs implementation |
| `fakecloud-kms` | KMS implementation |
| `fakecloud-cloudformation` | CloudFormation implementation |
| `fakecloud-ses` | SES implementation (v2 REST + v1 inbound Query) |
| `fakecloud-cognito` | Cognito User Pools implementation |
| `fakecloud-e2e` | End-to-end tests using aws-sdk-rust |

Protocol handling:
- **Query protocol** (SQS, SNS, IAM, STS, CloudFormation, SES v1): form-encoded body, `Action` parameter, XML responses
- **JSON protocol** (SSM, EventBridge, DynamoDB, Secrets Manager, CloudWatch Logs, KMS, Cognito User Pools): JSON body, `X-Amz-Target` header, JSON responses
- **REST protocol** (S3, Lambda, SES v2): HTTP method + path-based routing, XML/JSON responses
- **SES v1 inbound** uses Query protocol for receipt rule/filter operations
- SigV4 signatures are parsed for service routing but never validated

## Testing

```sh
cargo test --workspace              # unit tests
cargo build && cargo test -p fakecloud-e2e  # E2E tests (280+ tests)
cargo clippy --workspace -- -D warnings     # lint
cargo fmt --check                           # format check
```

E2E tests use the official `aws-sdk-rust` crates and spawn a real fakecloud
server per test.

## Roadmap

See [ROADMAP.md](ROADMAP.md) for what's coming next: Kinesis, RDS, ECS, ElastiCache, and more.

## Contributing

Contributions are welcome.

1. Fork the repository
2. Create a feature branch: `git checkout -b feat/my-feature`
3. Follow existing conventions:
   - Conventional commits (`feat:`, `fix:`, `chore:`, `test:`, `refactor:`)
   - Per-action error enums with `thiserror` (no god enums)
   - Add E2E tests for new actions
4. Run the full test suite: `cargo test --workspace`
5. Open a pull request

## What fakecloud Is (and Isn't)

**fakecloud is** a free, open-source local AWS emulator for integration testing and
local development. For every service it implements, the goal is 100% behavioral
parity with real AWS — verified by 34,000+ automated conformance test variants
against official AWS Smithy models across all API operations. 15 services,
100% conformance.

**fakecloud is not** a production-ready cloud replacement. It's not designed to
be scalable or to handle production workloads. It's for testing — making sure
your code works correctly before it hits the real cloud.

## Use with AI Coding Tools

Add one of these snippets to your project so AI coding assistants (Claude Code,
Cursor, GitHub Copilot) automatically use fakecloud for AWS integration tests.

<details>
<summary><strong>Claude Code</strong> — add to <code>CLAUDE.md</code></summary>

```markdown
## AWS Testing
For integration tests, use fakecloud as a local AWS emulator.
- Install: `curl -fsSL https://raw.githubusercontent.com/faiscadev/fakecloud/main/install.sh | bash`
- Run: `fakecloud` (listens on http://localhost:4566)
- Configure AWS SDK endpoint to http://localhost:4566 with dummy credentials (access_key=test, secret_key=test)
- Docs: https://fakecloud.dev/llms.txt
```

</details>

<details>
<summary><strong>Cursor</strong> — add to <code>.cursor/rules</code></summary>

```
For AWS integration tests, use fakecloud (local AWS emulator) at http://localhost:4566.
Install: curl -fsSL https://raw.githubusercontent.com/faiscadev/fakecloud/main/install.sh | bash
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

## License

fakecloud is free and open-source software, licensed under the [GNU Affero General Public License v3.0](https://www.gnu.org/licenses/agpl-3.0.html) (AGPL-3.0-or-later). Free for commercial use.

---

<p align="center">
  Built by <a href="https://faisca.dev">faisca</a> | <a href="https://fakecloud.dev">fakecloud.dev</a>
</p>
