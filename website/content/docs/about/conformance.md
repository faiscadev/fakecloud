+++
title = "Conformance"
description = "How fakecloud verifies behavioral parity with real AWS."
weight = 2
+++

fakecloud aims for 100% behavioral parity with AWS on every operation it implements. That's a big claim — here's how we verify it.

## Four kinds of tests, answering different questions

It's worth being precise about what each layer does, because they look similar on the surface and people conflate them.

**Conformance tests** ask *"does fakecloud match AWS's API contract?"* They're generated from AWS's own Smithy models and exhaustively cover every operation, every field, every constraint, and every validation rule AWS publishes. If fakecloud accepts a field it shouldn't, omits a required one, returns the wrong type, or fails to reject something AWS would reject, conformance catches it.

**E2E tests** ask *"does fakecloud work?"* They exercise fakecloud as a whole using the real AWS SDKs, across cross-service flows (EventBridge -> Step Functions, S3 -> Lambda, SES inbound -> SQS, etc.) and across fakecloud's own surface — introspection endpoints, persistence mode, time-control ticks for TTL / lifecycle / rotation, warm container management. A lot of what E2E covers doesn't exist on real AWS at all — you can't `POST /_fakecloud/sqs/expiration-processor/tick` against real AWS, because real AWS doesn't have a `/_fakecloud/*` surface. That's why the E2E suite runs against fakecloud only, by design.

**Parity tests** ask *"does fakecloud behave the same as real AWS on the things they both do?"* They're a separate suite that runs the *same test body* against both fakecloud and a real AWS sandbox account, and the parity signal comes from comparing pass/fail across the two runs. This is what catches subtle behavioral drift — the cases where fakecloud returns the shape AWS documents but fakes the semantics in a way real AWS wouldn't.

**Terraform acceptance tests (tfacc)** ask *"does fakecloud pass HashiCorp's own Terraform provider acceptance tests?"* These are the upstream `TestAcc*` functions from [`hashicorp/terraform-provider-aws`](https://github.com/hashicorp/terraform-provider-aws) run against fakecloud. Each test does a full `terraform apply` / `plan` / `destroy` cycle and asserts on the returned resource state — covering waiters, drift detection, and field-presence semantics that SDK-only tests miss. Unlike the other layers on this page, the test bodies come from a third party: they were written by the Terraform team against real AWS, and we just run them.

These four layers complement each other. Conformance catches schema drift. E2E catches functional regressions in fakecloud. Parity catches drift between fakecloud and the real thing. tfacc catches drift from what real-world infrastructure-as-code users expect fakecloud to do.

## How conformance works

We commit AWS's own Smithy models to [`aws-models/`](https://github.com/faiscadev/fakecloud/tree/main/aws-models) and generate test inputs with six orthogonal strategies:

1. **Boundary values** from `@length` and `@range` constraints
2. **Exhaustive enum coverage** — every enum value, every operation that uses it
3. **Optional-field permutations** — every combination of optional fields present or absent
4. **Property-based random generation** — seeded random inputs with shrinking
5. **Examples from the model's `@examples` trait** — the canonical inputs AWS documents
6. **Negative tests** for each constraint — values that should fail validation

Every response is validated against the operation's Smithy output shape. Missing required fields, unexpected fields, and wrong types are all failures. The baseline ([`conformance-baseline.json`](https://github.com/faiscadev/fakecloud/blob/main/conformance-baseline.json)) is checked in CI — any regression blocks the merge.

## Current coverage

248,319/248,319 generated test variants across all 105 services (7,391 operations) pass on every commit — true 100% conformance with no flake margin and no skipped services. The exact pass/total per service is checked into [`conformance-baseline.json`](https://github.com/faiscadev/fakecloud/blob/main/conformance-baseline.json).

See the harness and methodology at [`crates/fakecloud-conformance/`](https://github.com/faiscadev/fakecloud/tree/main/crates/fakecloud-conformance).

## Reproducing locally

```sh
cargo run -p fakecloud-conformance -- run --services s3
```

Omit `--services` to run the full suite. Expect a few minutes; some services have thousands of variants.

## Why schema-driven

The critical property is that the conformance tests are generated from AWS's own API definitions, not written by hand. That means:

- They cover every operation AWS has documented, not just the ones we remembered to test.
- They can't drift from AWS — when AWS updates a model, we pull the new model and the tests regenerate.
- They can't be gamed by code that passes them accidentally. The tests check response structure against the authoritative schema, not against what fakecloud happens to return.

This is the difference between "our tests pass" and "we match AWS." The second is what actually matters for an emulator.

## Parity testing against real AWS

Conformance checks shape. Parity checks behavior. They're related but they catch different bugs, so fakecloud runs both.

The parity suite lives in the [`fakecloud-parity`](https://github.com/faiscadev/fakecloud/tree/main/crates/fakecloud-parity) crate. Every test in it reads `FAKECLOUD_PARITY_BACKEND` at runtime and runs the same body against one of two targets:

- **fakecloud backend** — spawns a local fakecloud process for the test
- **AWS backend** — assumes an IAM role in a dedicated sandbox account and makes real SDK calls

The parity signal comes from comparing pass/fail across the two CI jobs, not from diffing responses inside the tests. The rule of thumb:

- fakecloud passes, AWS passes -> good
- AWS passes, fakecloud fails -> fakecloud bug, fix it
- AWS fails, fakecloud passes -> test bug (we assumed something wrong about AWS)
- both fail -> test bug

Tests in the parity suite must never assert on things that can't be true on real AWS: exact ARNs, exact error messages, specific account IDs, anything environmental.

### Current coverage

7 services today: S3, SQS, SNS, DynamoDB, KMS, Secrets Manager, STS.

We're deliberately starting with fast, cheap services. Parity coverage will grow over time as the sandbox budget grows, but the order is driven by cost: spinning up RDS instances, invoking real Lambda cold starts, or burning Bedrock tokens every week adds up, and those aren't the services where fakecloud is most likely to drift from AWS anyway (they're backed by real Docker containers for RDS and ElastiCache, and fakecloud-native for Lambda and Bedrock where real-AWS comparison is different in kind). The fast request/response services come first.

### Cadence and isolation

The fakecloud-backend parity job runs on every push. The real-AWS parity job runs **weekly** (Mondays 07:00 UTC) — that's enough to catch drift without the cost and noise of running it on every commit.

The real-AWS job is locked down hard:

- **No `pull_request` trigger.** AWS credentials are never exposed to contributor PRs. Changes to parity test files, the workflow, or anything it references are gated through `CODEOWNERS` and need explicit owner review to merge.
- **Protected GitHub Environment** (`aws-parity`) with a required reviewer. Even a scheduled run pauses for a human click before touching AWS.
- **OIDC-assumed role** scoped to `fcparity-*` resource prefixes. The IAM policy is the blast-radius limit — the role cannot touch anything outside the naming scheme.
- **No-op until the sandbox exists.** The job reads the role ARN from a repo variable; if the variable is unset, the job skips entirely. No surprises.

### Why E2E isn't parity

The E2E suite is much bigger (280+ tests, 105 services) but it doesn't run against real AWS — ever. Two reasons:

1. **A lot of E2E is testing fakecloud itself.** Introspection endpoints, persistence mode, `/_fakecloud/*/tick` processors, warm Lambda container introspection, forced SQS DLQ moves, auth event logs in Cognito — none of that exists on real AWS. Running those tests against AWS would be meaningless; they'd just fail at the first `/_fakecloud/*` request.

2. **Cost and speed.** Even the parts of E2E that *could* run against real AWS would be prohibitively expensive at every-push cadence — creating and destroying real S3 buckets, Lambda functions, RDS instances, and Bedrock jobs on every commit isn't a reasonable CI loop. That's exactly what the parity suite avoids by being small, curated, and weekly.

## Terraform provider acceptance tests (tfacc)

The [`fakecloud-tfacc`](https://github.com/faiscadev/fakecloud/tree/main/crates/fakecloud-tfacc) crate runs HashiCorp's own Terraform AWS provider acceptance tests against fakecloud. Each `#[tokio::test]` in it spawns a fakecloud process, sets `TF_ACC=1` plus `AWS_ENDPOINT_URL_<SERVICE>` environment variables, and invokes the upstream `TestAcc*` functions from `hashicorp/terraform-provider-aws` via `go test`. The upstream test does its own `terraform apply` / `plan` / `destroy` cycle and asserts on the returned resource state.

This is semantic coverage that SDK-based tests don't give us: **waiters** (does `CreateDBInstance` actually finish before the test moves on?), **field presence** (does the response include the computed attributes Terraform expects?), **drift detection** (does `terraform plan` report zero changes after a clean apply?), and a huge corpus of resource-lifecycle scenarios the HashiCorp team has collected over years of maintaining the provider.

### How tfacc and conformance complement each other

Both conformance and tfacc ground their expectations in external authoritative sources rather than in tests we invented ourselves. Conformance is derived from AWS's own Smithy models — the same schema AWS publishes for its SDK generators. tfacc is derived from HashiCorp's Terraform provider acceptance suite. Neither is something we could accidentally write to match what fakecloud happens to do; neither can drift from what its upstream source says.

They catch different classes of bug. Conformance covers every operation, field, type, and constraint AWS documents — exhaustively. It's the layer that catches wrong response types, missing fields, validation rules fakecloud failed to enforce, and any other way a fakecloud response could differ from the Smithy specification. tfacc runs real `terraform apply` / `plan` / `destroy` cycles, which exercises things a single request/response pair can't surface: waiters that return before the resource is actually ready, resources that look right at creation time but drift on the next plan, and field-presence assumptions the Terraform provider makes that aren't fully captured by the schema alone.

Running both is how fakecloud can claim 100% behavioral parity with a straight face.

### Current coverage

27 services today: `apigatewayv2`, `bedrock`, `cloudformation`, `cloudfront`, `cognito-identity`, `cognitoidp`, `dynamodb`, `ec2`, `ecr`, `ecs`, `elasticache`, `events` (EventBridge), `glue`, `iam`, `kinesis`, `kms`, `lambda`, `logs`, `organizations`, `route53`, `s3`, `secretsmanager`, `sfn` (Step Functions), `sns`, `sqs`, `ssm`, `sts`.

### Allow-list, not deny-list

Prior art for this approach is [`bblommers/localstack-terraform-test`](https://github.com/bblommers/localstack-terraform-test), which runs the upstream tests against LocalStack using a **deny-list** model: run everything, skip the ones that are known broken.

fakecloud inverts this to an **allow-list** model: each tfacc test explicitly names one service to run, plus per-service filters for which upstream test functions to include or skip. The reason is fakecloud's parity-per-implemented-service invariant — "we're 100% conformant on what we ship" is a stronger claim than "we pass most of these tests and skip the hard ones." An allow-list forces every new service to be an explicit, reviewed addition.

### Hard-fail on missing toolchain

Running the tfacc crate requires `go` and `terraform` on the machine. If either is missing, the tests **hard-fail with an actionable error** instead of silently passing. Running this crate is an opt-in signal that the caller wants the upstream Terraform suite exercised, and silently skipping would just hide regressions.

## The four layers together

Fast, broad E2E against fakecloud for functional correctness.
Exhaustive, schema-driven conformance against AWS's own Smithy models.
Narrow, behavioral parity against a real AWS sandbox.
Real-world tfacc against HashiCorp's Terraform provider acceptance suite.

Conformance and tfacc both derive from external authoritative sources — AWS's own published schema and HashiCorp's own acceptance suite — rather than from anything we invented. Together, the four layers are what lets fakecloud make the 100% behavioral parity claim with a straight face.
