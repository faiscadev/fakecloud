# fakecloud-conformance

The conformance harness that validates every implemented service against AWS's own API contract.

This crate is why the main README says "100% behavioral parity." It's a schema-driven test generator that reads official AWS Smithy models, produces tens of thousands of input variants per service, fires them at a running fakecloud instance, and validates every response against the model's output shape. Results are committed to [`conformance-baseline.json`](../../conformance-baseline.json) and gated in CI — any regression blocks the merge.

## Why this exists

Most AWS mocking projects grade their own homework: hand-written tests that assert the mock does what the author thinks AWS does. That's a closed loop. If the author misunderstands the AWS contract, the tests happily confirm the misunderstanding.

We wanted an oracle outside the loop. AWS publishes Smithy models for every service — these are the same machine-readable contracts AWS uses to generate its own SDKs. They specify required fields, length and range constraints, enum values, error shapes, and response structure. If we generate inputs from those models and validate responses against them, the test suite is grounded in AWS's own definition of the API, not ours.

## How it works

### 1. Load official Smithy models

AWS's Smithy JSON models for each service are committed to [`aws-models/`](../../aws-models/) (e.g. `s3.json`, `dynamodb.json`, `lambda.json`). `smithy.rs` parses them into a `ServiceModel` with shapes, operations, traits (`@length`, `@range`, `@required`, `@enum`, `@examples`), and error definitions.

### 2. Generate inputs with nine orthogonal strategies

For every operation, [`generators/`](./src/generators/) produces test variants using nine independent strategies. Each one targets a different class of bug:

| Strategy | What it generates | What it catches |
|---|---|---|
| **Boundary** (`boundary.rs`) | `@length`/`@range` min, max, and mid values for each constrained field | Off-by-one errors, missing validation, silent truncation |
| **Enum exhaustion** (`enum_exhaust.rs`) | One variant per valid enum value for every enum field | Unhandled enum branches, stale enum lists, default-case fallthrough |
| **Optionals permutation** (`optionals.rs`) | Required-only, required+each-optional, all-fields | Mis-flagged required fields, crashes on absent optionals |
| **Property-based** (`proptest_gen.rs`) | 20 randomized inputs per operation, drawn from each field's type and constraints | Panics on adversarial-but-valid input, parser fragility |
| **Model examples** (`examples.rs`) | The `@examples` trait from the Smithy model itself — AWS's own canonical inputs | Divergence from AWS's documented examples |
| **Negative** (`negative.rs`) | Missing required fields, out-of-range values, invalid enums, wrong types | Validation gaps, silent-pass bugs, 500s that should be 400s |
| **Examples diff** (`examples_diff.rs`) | The `@examples` trait's documented *output* — diffs every leaf field/JSON-type against the live response | Optional-in-Smithy fields that AWS's own examples document, but fakecloud doesn't return |
| **Round-trip echo** (`round_trip.rs`) | Walks the Smithy graph to pair every `Create*`/`Put*`/`Update*` with its matching `Get*`/`Describe*`. Sends the writer with one optional input field set, then chases the reader and asserts that field echoed | Silent input drops on Create/Put/Update — the field is accepted at the wire layer but lost before storage (e.g. Lambda `Layers` dropped on `CreateFunction`, [#853](https://github.com/faiscadev/fakecloud/issues/853)) |
| **Identifier-form fanout** (`id_forms.rs`) | For URL-bound `@httpLabel` members whose `@pattern` admits an ARN, sends the operation with each accepted identifier form: bare name, `name:qualifier`, partial ARN, full ARN, URL-encoded full ARN | REST endpoints that 404 on alternate identifier forms (e.g. Lambda `GetFunction` rejecting full ARNs, [#817](https://github.com/faiscadev/fakecloud/issues/817)) |

A single operation typically produces anywhere from a dozen to several hundred variants. Across all 107 services the baseline is **258,530 / 258,530 variants passing (100%)** — see [`conformance-baseline.json`](../../conformance-baseline.json) for the per-service breakdown.

### 3. Send and capture

[`probe.rs`](./src/probe.rs) serializes each variant in the protocol the service uses (JSON for most, XML for S3/SQS, query-string for IAM/STS/RDS, etc.), signs it with SigV4, and sends it to a running fakecloud instance. The full HTTP response — status code, headers, body — is captured for validation.

### 4. Validate the response against the output shape

[`shape_validator.rs`](./src/shape_validator.rs) is the other half of what makes this a real oracle: the harness doesn't just check that responses come back 200. It walks the Smithy output shape and checks the response body for:

- **`MissingField`** — a field the model declares `@required` isn't in the response
- **`WrongType`** — a field is present but the JSON type doesn't match the model (e.g. string where a list is expected)
- **`UnexpectedField`** — a field is in the response that doesn't exist in the model at all (catches typos, stale fields, leaked internal state)
- **`ParseError`** — the response body doesn't even parse in the declared protocol

Both directions are checked: the input must match what AWS accepts, and the output must match what AWS returns.

#### 4xx responses are also validated against the model

Synthetic placeholder inputs frequently provoke real AWS exceptions like `ResourceNotFoundException` or `ValidationException` from implemented handlers — those 4xxs are *expected* and shouldn't fail the variant. But a 4xx that comes from fakecloud's *own* routing layer (a URL form the dispatcher didn't recognise, an unhandled wire format) is a bug that previously slipped through as "well, it's a 4xx, probably fine." That's exactly how #817 stayed green.

The classifier now splits 4xx by what's in the body:
- **AWS error shape present** (`__type` JSON field, `<Code>` XML element) — handler ran. If the operation's Smithy `error_shapes` list is non-empty, the code's short name must match one of the declared exceptions; this catches stray fakecloud error types AWS would never emit.
- **No recognisable AWS error code** — likely a routing miss or unhandled-form response. Fail.

The check is fully Smithy-driven from each operation's already-parsed `error_shapes` — no allowlist of acceptable error strings to maintain.

### 5. Baseline and CI gate

Results are written to [`conformance-baseline.json`](../../conformance-baseline.json) — total and per-service passed/total counts. The `check` subcommand compares the current run against the committed baseline and fails if any service's pass count drops. This runs on every PR. The baseline is a ratchet: it only goes up.

Variants that legitimately can't be exercised by schema-only generation (cross-operation state, resources that must be created first, operations that require specific AWS account context) are listed with the reason in the per-service reports.

## Level 2: handwritten E2E tests (enforced)

The schema-driven harness above is "Level 1." It's great at exercising the wire format but it can't cover real workflows like `PutObject` → `GetObject`, or `CreateUserPool` → `SignUp` → `InitiateAuth`. Those need handwritten tests that drive real AWS SDK clients against a running fakecloud.

Those tests live in [`tests/`](./tests/) — one file per service (`sqs.rs`, `s3.rs`, `cognito.rs`, …). They use the official `aws-sdk-*` crates as clients and each test is tagged with a proc-macro annotation from [`fakecloud-conformance-macros`](../fakecloud-conformance-macros/):

```rust
#[test_action("sqs", "CreateQueue", checksum = "0a1fae82")]
#[tokio::test]
async fn sqs_create_queue() {
    let server = TestServer::start().await;
    let client = server.sqs_client().await;
    let resp = client.create_queue().queue_name("conformance-test").send().await.unwrap();
    assert!(resp.queue_url().is_some());
}
```

Two things get enforced automatically:

**1. Every implemented action must have a test.** Running `cargo run -p fakecloud-conformance -- audit` (CI does this on every PR) scans `supported_actions()` in every service crate and cross-references against `#[test_action(...)]` annotations in `tests/`. If a service declares `CreateQueue` as a supported action but no test is tagged `("sqs", "CreateQueue", ...)`, the audit fails and the build is blocked. You can't ship an action without an E2E test — the project literally won't compile green.

**2. Tests can't go stale when AWS changes the model.** The `checksum` field is a SHA-256 hash of the operation's full canonical shape tree — input shape, output shape, error shapes, all recursively resolved to capture referenced sub-shapes (see [`checksum.rs`](./src/checksum.rs)). The proc macro reads `aws-models/<service>.json` at *compile time*, recomputes the hash for that operation, and if it doesn't match the stored value, it emits:

```
compile_error!("checksum mismatch for sqs::CreateQueue: expected 0a1fae82, got 7b3ec91f. \
                The Smithy model has changed — update the checksum or the conformance test.");
```

So when we run `scripts/update-aws-models.sh` and AWS has added a new field, changed a type, or introduced an error, every affected test breaks the build until a human looks at the test, confirms it still exercises the meaningful parts of the new shape, and updates the checksum. Model drift can't silently erode test coverage.

Together with the Level 1 baseline, this means: schema-driven tests prove the wire format matches AWS, handwritten tests prove real workflows match AWS, the audit proves every action has a test, and the compile-time checksum proves every test is still current with the model it was written against.

## CLI

```bash
# Run the full harness against a fresh fakecloud (spawned automatically)
cargo run -p fakecloud-conformance -- run

# Run a single service (much faster iteration loop)
cargo run -p fakecloud-conformance -- run --services s3

# Run against an already-running fakecloud
cargo run -p fakecloud-conformance -- run --endpoint http://localhost:4566

# CI mode: fail on any failing variant, or on any service passing fewer variants than the committed baseline
cargo run -p fakecloud-conformance -- check

# Update the baseline after intentional improvements
cargo run -p fakecloud-conformance -- update-baseline

# Level 2: audit handwritten service code for supported_actions() coverage
cargo run -p fakecloud-conformance -- audit

# Print all operations discovered in the Smithy models
cargo run -p fakecloud-conformance -- operations
```

### Container runtime

The spawned sweep server runs with containers disabled, so thousands of variants never start real databases, functions or instances. A few operations refuse outright without a runtime (RDS `CreateDBInstance` will not report an instance it cannot back with a real database). Any variant that fails with that runtime-required error is re-probed on its own fresh server with Docker or Podman enabled, which is shut down gracefully afterwards so it removes the containers it started. Without Docker or Podman those variants are reported as failures, so run `check` and `update-baseline` where a runtime is available (CI does). Setting `FAKECLOUD_CONTAINER_CLI` pins the sweep server's runtime and turns the re-probe off.

## Adding a new service

1. Add the service to the `SERVICES` list in [`scripts/update-aws-models.sh`](../../scripts/update-aws-models.sh) (`our_name:repo_dir`, where `repo_dir` matches the folder in [aws/api-models-aws](https://github.com/aws/api-models-aws)), then run the script. It sparse-clones the repo and drops the Smithy JSON into `aws-models/<service>.json`.
2. Add the service entry to `aws-models/service-map.json`.
3. Run `cargo run -p fakecloud-conformance -- run --services <name>` — every operation in the model is picked up automatically.
4. Implement failing variants in the service crate until the service hits 100% or you have a documented reason for each remaining gap.
5. `update-baseline`, commit, ship.

## What this harness doesn't cover

- **Guaranteed end-to-end workflow coverage.** The Level 1 generator creates inputs from schemas in isolation, so it can't know that `GetObject` needs a `PutObject` first. Level 2 mitigates this — the audit forces a handwritten `#[test_action]` test for every implemented action, which pushes you toward real SDK flows — and each service crate also has its own integration tests covering multi-step scenarios. But neither layer can *prove* that every meaningful workflow permutation has been exercised. We treat missing workflow coverage as a bug to fix when found, not a property the harness enforces.
- **Eventual consistency and timing.** fakecloud is synchronous by design for test determinism; we don't model AWS's consistency delays.
- **IAM policy evaluation semantics.** The IAM API surface is conformed, but fakecloud does not enforce policies on unrelated service calls.
- **Services we haven't implemented.** The goal is 100% conformance for every service we ship — not coverage of all of AWS.
