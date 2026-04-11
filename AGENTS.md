# FakeCloud

Local AWS cloud emulator. Part of the faisca project family.

## Product Context

- FakeCloud is a local AWS emulator focused on high-fidelity behavior and AWS-compatible responses.
- Current project state from prior work: 20 services, 1044 operations, version 0.6.1, with SES, Cognito User Pools, Docker-backed Lambda execution, Kinesis, RDS, ElastiCache, Step Functions, and API Gateway v2 already shipped.
- The broader roadmap prioritizes services that LocalStack keeps behind paid tiers, especially ECR, ECS, ELB/ALB, CloudFront, CloudWatch Metrics, Bedrock, and EC2.
- Introspection SDKs (Rust, Python, TypeScript, Go) are already built and maintained for the `/_fakecloud/*` endpoints.

## Build And Run

```sh
cargo build                              # build all crates
cargo run --bin fakecloud                # run the server (port 4566)
cargo test --workspace                   # run unit tests
cargo test -p fakecloud-e2e             # run E2E tests (build first)
cargo clippy --workspace -- -D warnings  # lint
cargo fmt --check                        # format check
```

## Architecture

- `fakecloud` - binary entry point (clap CLI, Axum server)
- `fakecloud-core` - `AwsService` trait, `ServiceRegistry`, request dispatch, protocol parsing
- `fakecloud-aws` - shared AWS types (ARNs, error builders, SigV4 parser)
- `fakecloud-{sqs,sns,eventbridge,iam,ssm,dynamodb,lambda,secretsmanager,s3,logs,kms,cloudformation,ses,cognito,kinesis,rds,elasticache,stepfunctions,apigatewayv2}` - individual service implementations
- `fakecloud-sdk` - Rust SDK for `/_fakecloud/*` introspection endpoints
- `fakecloud-e2e` - E2E tests using `aws-sdk-rust` and AWS CLI

## Conventions

### Error Handling

- Prefer per-action error enums with `thiserror`; do not introduce broad god enums.
- Each error variant should map to an AWS error code and HTTP status.
- Use `AwsServiceError::aws_error()` for AWS-compatible errors.

### Testing

- Always add tests for changes. Prefer the smallest mix of unit and E2E coverage that proves behavior.
- Unit tests live inline in `#[cfg(test)]` modules.
- E2E tests live in `fakecloud-e2e` and require `cargo build` first.
- SDK tests should use official `aws-sdk-rust` crates.
- CLI tests should use `TestServer::aws_cli()`.
- Conformance coverage and E2E coverage serve different purposes; do not mix them.

### Behavior And Fidelity

- Match AWS output and behavior as closely as possible, including payload shape and error format.
- Do not game conformance by adding validation without implementing the underlying behavior.
- Do not ship stub responses; implement the behavior or return an appropriate error.
- Do not leave small follow-up correctness issues unresolved when they are part of the task at hand.

### Source And Naming

- Avoid referencing Moto in Rust source or user-facing implementation details. Existing protocol paths like `/moto-api/reset` are fine where already established.
- Keep implementation details generic unless there is a concrete compatibility reason not to.

### Git

- Use conventional commits: `feat:`, `fix:`, `chore:`, `test:`, `docs:`, `refactor:`.
- Do not add attribution trailers such as `Co-Authored-By` or generated-by messages.
- Prefer worktrees for parallel work to avoid conflicts.
- Do not merge with red CI.
- Wait for required CI and Cubic checks before merging.
- When merging PRs, prefer a normal merge commit via `gh pr merge` with no special flags.

## AWS Protocol Notes

- Query protocol (SQS, SNS, IAM, STS, CloudFormation, SES v1): form-encoded body, `Action` param, XML responses
- JSON protocol (SSM, EventBridge, DynamoDB, Secrets Manager, CloudWatch Logs, KMS, Cognito User Pools): JSON body, `X-Amz-Target` header, JSON responses
- REST protocol (S3, Lambda, SES v2): HTTP method plus path-based routing, XML or JSON responses
- SigV4 signatures are parsed for routing but never validated

## Service Notes

- SES is fully shipped, including v2 operations, v1 inbound operations, cross-service event fanout, mailbox simulator, and inbound email pipeline.
- Cognito User Pools is fully shipped, including auth flows, JWT token issuance, and introspection endpoints.
- Lambda execution runs real code in Docker containers, supports multiple runtimes, and reuses warm containers.
- Step Functions is fully shipped with complete ASL interpreter (all state types), error handling (Retry/Catch), and cross-service task integrations (Lambda, SQS, SNS, EventBridge, DynamoDB).
- API Gateway v2 (HTTP APIs) is fully shipped with Lambda proxy integration v2.0, HTTP proxy, Mock integrations, route matching with path parameters and wildcards, CORS, and JWT/Lambda authorizers.
- When adding new service behavior, prefer complete, realistic implementations over placeholder API coverage.
