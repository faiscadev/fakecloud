# FakeCloud

Local AWS cloud emulator. Part of the faisca project family.

## Product Context

- FakeCloud is a local AWS emulator focused on high-fidelity behavior and AWS-compatible responses.
- Current project state: 105 AWS services, 7,379 operations, 248,319/248,319 Smithy conformance variants pass — true 100% across every implemented service, no flake margin. See [the parity matrix](website/content/docs/parity.md) for the full service-by-service breakdown of control-plane vs data-plane coverage and known limitations.
- The broader roadmap prioritizes services that LocalStack keeps behind paid tiers, especially ECS, ELB/ALB, CloudFront, CloudWatch Metrics, and EC2.
- Introspection SDKs (Rust, Python, TypeScript, Go, PHP, Java) are already built and maintained for the `/_fakecloud/*` endpoints.

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
- `fakecloud-<service>` crates - individual service implementations (one crate per AWS service; see the parity matrix and `crates/` directory for the full list)
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

## Website And Docs Conventions

The fakecloud website lives in `website/` (Zola static site). Evergreen claims, URLs, and code samples ship to the public site, so they need to match the real implementation. The `doc-counts` CI job (`scripts/check-doc-counts.sh`) validates service/operation/variant counts and performance metrics; it does not validate URL conventions, package names, or CLI surface, so the rules below catch what the script can't.

### URL Structure

- Comparison pages live at `/vs/<competitor>/` (`website/content/vs/<competitor>.md`). Do not introduce `/compare/`, `/comparison/`, `/alternatives/`, or other parallel prefixes.
- Service emulator and "local service" landing pages live at the root (`/<service>-emulator/`, `/local-<service>/`, `/test-<service>-locally/`, `/fake-<service>/`). Do not introduce `/features/` or similar nested prefixes for the same content.
- Documentation pages live under `/docs/` and follow the existing section layout (`about/`, `getting-started/`, `guides/`, `reference/`, `services/`, `sdks/`).
- Blog posts live under `/blog/` and are point-in-time — do not retroactively update them. Use new posts to reflect new state instead of editing old ones.
- Before adding a new landing page, search `website/content/` for an existing canonical page on the same topic and extend it rather than adding a parallel page. SEO works against fragmented pages on the same intent.

### Real SDK Package Names

Match what's published. Do not invent scoped names or alternative spellings.

- TypeScript: `fakecloud` (npm) — see `sdks/typescript/package.json`
- Python: `fakecloud` (PyPI) — see `sdks/python/pyproject.toml`
- Go: `github.com/faiscadev/fakecloud/sdks/go` (Go module path) — see `sdks/go/go.mod`
- PHP: `fakecloud/fakecloud` (Composer) — see `sdks/php/composer.json`
- Java: group `dev.fakecloud` — see `sdks/java/build.gradle.kts`
- Rust: crate `fakecloud-sdk` — see `crates/fakecloud-sdk/Cargo.toml`

### Real CLI Surface

The `fakecloud` binary has no subcommands. It accepts only flags. Do not document `fakecloud start`, `fakecloud stop`, `fakecloud reset`, or any other invented subcommand.

Real flags (source of truth: `crates/fakecloud-server/src/cli.rs`): `--addr`, `--region`, `--account-id`, `--log-level`, `--storage-mode`, `--data-path`, `--s3-cache-size`, `--verify-sigv4`, `--iam`. Each has a matching `FAKECLOUD_*` env var.

### Content Completeness

Do not merge website pages that contain `[Full list of X...]`, `[...continuing for all N services...]`, `TODO`, or any other placeholder marker. Either ship complete content or keep the PR in draft. Per-service operation lists should be machine-generated from `website/aws-models/*.json` rather than hand-rolled.

### Performance Claims

Startup time, idle memory, and binary size are tracked as constants in `scripts/check-doc-counts.sh` (no in-repo source of truth). When re-measurement establishes a new number, update the constants and audit every page in the script's `FILES` array in the same PR. Do not introduce new performance claims unless they've been measured.

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
- `GET /_fakecloud/credentials` vends AWS container-credentials-format JSON (minted + registered like AssumeRole, so it verifies under `--verify-sigv4`); point an app's `AWS_CONTAINER_CREDENTIALS_FULL_URI` at it to resolve the SDK default credential chain locally with no code change. Role ARN configurable via `--credentials-role-arn`.
- EC2 IMDS is served on `/latest/*` (IMDSv1 + IMDSv2 token, `meta-data/iam/security-credentials/*`, `iam/info`, `instance-id`, `placement/*`, `dynamic/instance-identity/document`); consumed via `AWS_EC2_METADATA_SERVICE_ENDPOINT`. Credentials share the same registered cache as `/_fakecloud/credentials`. Instance ID via `--imds-instance-id`. `--imds-link-local` additionally binds `169.254.169.254:80` (IMDS) + `169.254.170.2:80/creds` (ECS creds) for apps that hardcode those IPs; needs root plus the addresses pre-aliased on loopback (fakecloud binds them but never creates/deletes the alias), with graceful fallback if the bind fails.
- `--dns` runs a real DNS resolver (UDP+TCP, `--dns-addr` default `0.0.0.0:53`, port 53 needs root) answering the wire-encodable types (A/AAAA/CNAME/MX/TXT/NS/PTR/SPF/CAA/SRV/SOA) from the Route 53 records created in fakecloud. Longest-suffix zone match across all accounts; CNAME chase into local zones with the requested type appended, and an external CNAME target forward-resolved upstream so a stub client still gets an address. Names in no local zone forward to `--dns-upstream` (default first non-loopback `/etc/resolv.conf` nameserver, else `8.8.8.8:53`); forward socket is connect()ed + txid-checked. UDP honors EDNS0 size and sets TC to force TCP fallback past it. Pure resolution logic in `fakecloud_route53::resolver`, hand-rolled wire codec in the server `dns` module reusing `dnssec::encode_rdata`/`type_code`. Best-effort detached listener; a bind failure is logged and the main server is unaffected. The same resolution is exposed for tests at `GET /_fakecloud/dns/resolve?name=<n>&type=<A|...|SOA>` (defaults `A`, unsupported type -> 400) returning `{name,type,status(ANSWERED|NODATA|NXDOMAIN|NOT_AUTHORITATIVE),authoritative,records:[{name,type,ttl,value}],external_cname}` (`external_cname` names the target when an A/AAAA CNAME chain exits all local zones, since this path does no upstream I/O), wrapped as `dns_resolve`/`dnsResolve` in all six SDKs -- no socket needed.
- When adding new service behavior, prefer complete, realistic implementations over placeholder API coverage.
