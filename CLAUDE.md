# FakeCloud

Local AWS cloud emulator. Part of the faisca project family.

## Build & Run

```sh
cargo build                              # build all crates
cargo run --bin fakecloud                # run the server (port 4566)
cargo test --workspace                   # run unit tests
cargo test -p fakecloud-e2e             # run E2E tests (build first)
cargo clippy --workspace -- -D warnings  # lint
cargo fmt --check                        # format check
```

## Architecture

- `fakecloud` — binary entry point (clap CLI, Axum server)
- `fakecloud-core` — AwsService trait, ServiceRegistry, request dispatch, protocol parsing
- `fakecloud-aws` — shared AWS types (ARNs, error builders, SigV4 parser)
- `fakecloud-{sqs,sns,eventbridge,iam,ssm,dynamodb,lambda,secretsmanager,s3,logs,kms,cloudformation,ses,cognito}` — individual service implementations
- `fakecloud-e2e` — E2E tests using aws-sdk-rust and AWS CLI

## Conventions

### Error Handling
- Per-action error enums with thiserror (no god enums)
- Each error variant maps to an AWS error code and HTTP status
- Use `AwsServiceError::aws_error()` for AWS-compatible errors

### Git
- Conventional commits: `feat:`, `fix:`, `chore:`, `test:`, `docs:`, `refactor:`

### Testing
- Unit tests: inline `#[cfg(test)]` modules in source files
- E2E tests: `fakecloud-e2e` crate, requires `cargo build` first
- SDK tests use official aws-sdk-rust crates
- CLI tests use `aws` CLI binary via TestServer::aws_cli()

### AWS Protocol Notes
- Query protocol (SQS, SNS, IAM, STS, CloudFormation, SES v1): form-encoded body, `Action` param, XML responses
- JSON protocol (SSM, EventBridge, DynamoDB, Secrets Manager, CloudWatch Logs, KMS, Cognito User Pools): JSON body, `X-Amz-Target` header, JSON responses
- REST protocol (S3, Lambda, SES v2): HTTP method + path-based routing, XML/JSON responses
- SigV4 signatures are parsed for routing but never validated
