+++
title = "Glossary"
description = "Definitions for the technical vocabulary used across the fakecloud docs, site, and conformance reports."
template = "page.html"
+++

A short reference for the terms that show up across the fakecloud docs, [conformance](/docs/about/conformance/) reports, and [parity matrix](/docs/parity/).

### Behavioral parity
The measure of whether an emulator reproduces real AWS behavior — request and response shapes, field names, error codes, and observable side effects — not just whether a given API call returns a 200. fakecloud aims at 100% behavioral parity on every service it implements. See [What fakecloud is (and isn't)](/docs/about/what-it-is/).

### Conformance
The automated check that fakecloud's responses match AWS's [Smithy](#smithy) models exactly. fakecloud's conformance harness runs **192,048 generated test variants** on every commit, all of which pass. See [How conformance works](/docs/about/conformance/).

### Configurable response
A test-only mechanism for setting the exact response (or error) fakecloud will return for a given operation, without modifying application code. Used for testing failure paths — throttles, timeouts, specific error codes — that are hard to trigger against real AWS.

### Control plane vs. data plane
The **control plane** is the management API (`CreateBucket`, `CreateQueue`, `CreateFunction`, etc.). The **data plane** is the operational API (`PutObject`, `SendMessage`, `Invoke`, etc.). Most fakecloud services implement both at 100%. A handful — noted in the [parity matrix](/docs/parity/) — implement the full control plane and a partial data plane (e.g., Bedrock Runtime returns shape-correct synthetic responses rather than running real inference).

### Cross-service integration
A wire-up between two AWS services that fires automatically: S3 -> Lambda on object upload, SNS -> SQS on publish, EventBridge -> Step Functions on event match, DynamoDB Streams -> Lambda on record change, SES inbound -> S3/SNS/Lambda on receipt rule match, and 25+ more. fakecloud executes these end-to-end rather than stubbing them.

### Emulator
A server that speaks a service's real wire protocol over HTTP. Distinct from a [mock](#mock-vs-emulator) (which intercepts SDK calls in-process and returns predefined values) and from a real implementation (which actually performs the work). fakecloud is an emulator: real HTTP server on port 4566, real AWS JSON/XML/Query wire formats, real-shaped responses that the official AWS SDKs parse.

### Introspection endpoint
A non-AWS HTTP endpoint exposed by fakecloud under `/_fakecloud/...` for test assertions — for example, listing the messages that landed in an SQS queue, the emails SES processed, or the Lambda invocations recorded. Used by the first-party [test-assertion SDKs](/docs/sdks/).

### Mock vs. emulator
A **mock** replaces the AWS SDK's internals with stubs that never hit HTTP. If you assemble the request wrong, a mock doesn't notice. An **emulator** runs as a separate process and validates the actual wire protocol — wrong shapes, wrong field names, and wrong signatures fail the same way real AWS would. fakecloud is an emulator.

### Multi-account
fakecloud can simulate cross-account AWS workflows. Cross-account delivery is enforced on SQS, SNS, Lambda, S3, EventBridge, and Step Functions, with STS trust policies, session tags, `sts:ExternalId`, and SCP enforcement all wired through.

### SCP (Service Control Policy)
An AWS Organizations policy that sets the *ceiling* of permissions for accounts in an OU. fakecloud implements the Organizations control plane and enforces SCPs as a hard ceiling: even if IAM allows an action, an SCP `Deny` blocks it.

### Smithy
AWS's interface definition language for its services. Every official AWS SDK is generated from Smithy models, and fakecloud validates its responses against the same models on every commit. See [conformance-baseline.json](https://github.com/faiscadev/fakecloud/blob/main/conformance-baseline.json) for the exact baseline.

### Startup time
The wall time from launching `fakecloud` to the server accepting requests on port 4566. Currently **~300ms** on Apple M1, with **~10 MiB** idle memory and a **~19 MB** binary. The fast startup is what makes fakecloud usable in per-test setup/teardown rather than as a long-running shared environment.

### Wire protocol
The on-the-wire format an AWS service uses: REST-XML (S3), REST-JSON (Lambda, API Gateway), JSON 1.1 (most others), Query/JSON 1.1 hybrid (older services). fakecloud implements each service's actual wire protocol so the official AWS SDKs work without modification — pointing them at `http://localhost:4566` is the only change needed.
