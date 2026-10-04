+++
title = "SSM"
description = "Parameters, documents, commands, maintenance windows, associations, patch baselines."
weight = 9
+++

fakecloud implements **152 of 152** SSM operations at 100% Smithy conformance.

## Supported features

- **Parameter Store** — String, StringList, SecureString parameters; tiers; labels; versions; history
- **Documents** — CRUD, versions, tags, permissions, sharing
- **Commands** — RunCommand, command history, invocation status, output
- **Maintenance windows** — CRUD, task execution, target registration
- **Associations** — CRUD, execution history, compliance
- **Patch baselines** — CRUD, baseline registration, patch groups
- **Inventory** — entries, schemas, deletion
- **Automation** — executions, step management, signal handling
- **OpsItems** — CRUD, related items, comments, summaries
- **Resource Data Sync** — CRUD with S3 destinations
- **Service settings** — get, reset, update

## Protocol

JSON protocol. `X-Amz-Target` header, JSON body, JSON responses.

## RunCommand lifecycle

`SendCommand` returns `Pending` immediately, then a background task flips the command (and each per-instance invocation) to `InProgress` after ~500ms and `Success` after another ~1.5s. `GetCommandInvocation`, `ListCommandInvocations`, and `ListCommands` all read live state, so polling clients see the same lifecycle they'd hit on real AWS — no canned `Success` shortcut.

To simulate failures from a test, hit the admin endpoint:

```bash
curl -X POST "$ENDPOINT/_fakecloud/ssm/commands/$CMD_ID/fail" \
  -H "content-type: application/json" \
  -d '{"instanceId":"i-...","statusDetails":"Script exited with code 7","standardErrorContent":"boom"}'
```

`instanceId`, `statusDetails`, and `standardErrorContent` are all optional. See [introspection reference](/docs/reference/introspection/#ssm).

## Session Manager

`StartSession` returns `400 TargetNotConnected` and `ResumeSession` returns
`400 DoesNotExistException` by default. fakecloud doesn't run a real SSM
data-plane websocket, and returning a fake stream URL would lull integration
tests into thinking the session was live. Each operation's error code is
the closest semantically honest exception that's already declared on it in
the SSM Smithy model (StartSession declares `TargetNotConnected`,
ResumeSession declares `DoesNotExistException`) so SDK clients deserialize
a known shape and the error round-trips cleanly. The error message points
at the escape hatches:

**Echo mode** — set `FAKECLOUD_SSM_SESSION_ECHO=1` to make `StartSession` /
`ResumeSession` succeed with a sentinel token (`fakecloud-echo-mode-not-real-websocket`).
The session is recorded in state so `DescribeSessions` and `TerminateSession`
round-trip. Use this when your test only exercises the AWS SDK control flow
and doesn't actually attach to the websocket.

**Admin inject** — drop a session record into state directly:

```bash
curl -X POST "$ENDPOINT/_fakecloud/ssm/sessions/inject" \
  -H "content-type: application/json" \
  -d '{"target":"i-001","reason":"smoke"}'
```

Body fields: `target` (required), `accountId`, `status`
(`Connected` | `Terminated`, default `Connected`), `owner` (defaults to
account-root IAM ARN), `reason`, `sessionId` (optional explicit ID).
Returns `{"sessionId": "..."}`. After injection, `DescribeSessions` and
`TerminateSession` work normally.

## SecureString encryption

SecureString parameters are encrypted through the KMS hook on `PutParameter`
and decrypted on `GetParameter` / `GetParameters` / `GetParametersByPath` when
the caller passes `WithDecryption=true`. The default `alias/aws/ssm`
AWS-managed key is auto-provisioned on first use; passing an explicit `KeyId`
routes encryption through that key instead. KMS calls land in
`/_fakecloud/kms/usage` with the `PARAMETER_ARN` encryption context, so tests
can assert that a parameter's plaintext is never persisted.

`PutParameter` hard-fails when the KMS `Encrypt` call is rejected — a denied
key policy, a disabled CMK, or a missing key all surface as
`KMSInternalException` (with the underlying KMS error in the message) and the
SecureString parameter is never stored. This matches real SSM behavior and
prevents a silent fallback to plaintext storage when an explicit `KeyId` is
unreachable; tests can flip the CMK state via the KMS admin endpoints and
assert that the subsequent `PutParameter` errors rather than persisting.

## Regions

Everything SSM keeps is regional, as on AWS: parameters, documents, maintenance windows, patch baselines (and the default baseline registered per operating system with `RegisterDefaultPatchBaseline`), associations, OpsItems, inventory, managed instances, sessions, resource data syncs and service settings. The same parameter name can hold different values in two regions, `GetParametersByPath` / `DescribeParameters` only see the request region, a parameter ARN names its own region and is not found from another region's endpoint, and `/aws/reference/secretsmanager/...` reads the secret of the request region. The AWS-owned public parameters (`/aws/service/...`) exist in every region. ECS and CodeBuild resolve a parameter name in the task's or build's region and a full ARN in the region it names.

## Limitations

- `StartSession` returns the Smithy-declared `TargetNotConnected` (and `ResumeSession` returns `DoesNotExistException`) with a documentation pointer rather than opening a real websocket. The Session Manager data plane is not implemented; tests that depend on live port-forwarding should use the `POST /_fakecloud/ssm/sessions/inject` admin endpoint to simulate a websocket session.

## Source

- [`crates/fakecloud-ssm`](https://github.com/faiscadev/fakecloud/tree/main/crates/fakecloud-ssm)
- [AWS Systems Manager API reference](https://docs.aws.amazon.com/systems-manager/latest/APIReference/Welcome.html)
