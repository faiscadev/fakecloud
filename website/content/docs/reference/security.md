+++
title = "SigV4 verification and IAM enforcement"
description = "Opt-in security features: real SigV4 signature checking and IAM identity-policy evaluation with Condition block support."
weight = 5
+++

By default, fakecloud parses SigV4 headers for routing but doesn't verify signatures, and stores IAM policies without evaluating them. That's the right default for "tests just work" — real signature verification and policy enforcement get in the way of the happy path.

When you explicitly need them, two orthogonal flags flip them on:

```bash
FAKECLOUD_VERIFY_SIGV4=true    # or --verify-sigv4
FAKECLOUD_IAM=off|soft|strict  # or --iam off|soft|strict
```

They're independent: you can turn on SigV4 verification without touching IAM, or the other way around.

## The reserved root identity

The credential pair `test`/`test` (and any access key starting with `test`) is treated as the de-facto root bypass. It skips both SigV4 verification and IAM enforcement, matching the community convention that LocalStack and other emulators use for local development.

When either opt-in feature is enabled, fakecloud emits a one-time WARN at startup noting this bypass so you don't silently get false-positive "my policies work" results from unsigned test clients.

## `--verify-sigv4`

When on, every incoming request is cryptographically verified:

1. **Canonical request** rebuilt per the AWS SigV4 spec (double-encoded path for non-S3, single-encoded for S3; sorted, URL-encoded query string; lowercased + sorted headers; payload hash from `X-Amz-Content-Sha256` when present, otherwise `sha256(body)`).
2. **Signing key** derived via the four-step HMAC chain `AWS4 -> date -> region -> service -> aws4_request`.
3. **Constant-time comparison** against the signature the client sent.
4. **Clock skew window** of ±15 minutes, matching AWS.

Verification failures return protocol-correct AWS errors before business logic runs:

| Failure | AWS error |
| --- | --- |
| Wrong signature | `SignatureDoesNotMatch` |
| Unknown access key | `InvalidClientTokenId` |
| Clock skew > 15 min | `RequestTimeTooSkewed` |
| Malformed auth header | `IncompleteSignature` |

Header-based `Authorization: AWS4-HMAC-SHA256 ...` and query-string (presigned URL) signatures are both supported. STS temporary credentials from `AssumeRole`, `GetSessionToken`, and `GetFederationToken` are persisted per-request and verified against the secret key the client received when it called STS.

## `--iam off|soft|strict`

Three modes, in order of aggressiveness:

- **`off`** (default): policies are stored but never consulted. Zero behavior change from unconfigured fakecloud.
- **`soft`**: policies are evaluated and each deny is logged on the `fakecloud::iam::audit` tracing target, but the request is allowed through. Useful for onboarding: you can see which statements would fire without breaking your test suite.
- **`strict`**: policies are evaluated and denied requests fail with a protocol-correct `AccessDeniedException` before the service handler runs.

Strict mode also **fails closed on an unknown access key**: a signed request whose access key id resolves to no identity is rejected with `InvalidClientTokenId` before the handler runs, even when `--verify-sigv4` is off (with `--verify-sigv4` on the same key is rejected earlier, at signature verification). Only the reserved `test*` root-bypass credentials skip this check. This closes a fail-open gap where an unresolvable key previously fell through to the handler unenforced. `soft` mode leaves such requests untouched.

Filter the audit events with `RUST_LOG=fakecloud::iam::audit=warn`.

### Root is always allowed

The account's IAM root identity (`arn:aws:iam::<account>:root`) and the reserved `test*` bypass AKIDs always pass enforcement, matching AWS's own behavior where root bypasses identity-based policies.

### Enforced services

Opt-in enforcement covers the services most commonly subject to real IAM policies:

| Service | Covered actions | Resource ARN shape |
| --- | --- | --- |
| **IAM** | All 128 supported actions | `arn:aws:iam::<account>:{user,role,group,policy,instance-profile,mfa,server-certificate,saml-provider,oidc-provider}/<name>` |
| **STS** | All 8 supported actions | `*` (or `RoleArn` for `AssumeRole*`) |
| **SQS** | All 20 supported actions | `arn:aws:sqs:<region>:<account>:<queue-name>` |
| **SNS** | All 34 supported actions | Topic / subscription / platform-app / endpoint ARNs |
| **S3** | All 74 supported actions | `arn:aws:s3:::<bucket>[/<key>]` (object actions include the key; bucket actions don't) |
| **KMS** | All 47 supported actions | `arn:aws:kms:<region>:<account>:key/<key-id>` (key-targeted actions) or `*` (account-level actions like CreateKey, ListKeys) |

Other services are not enforced even with `FAKECLOUD_IAM=strict`. The startup log enumerates which services are enforced vs. skipped so you always know the current surface. If a service you need is missing, [open an issue](https://github.com/faiscadev/fakecloud/issues) — the wiring is straightforward per-service.

## Evaluator scope

The policy evaluator implements the essentials of AWS's identity-based policy evaluation. It deliberately stops short of the features listed below so you don't build false mental models from a half-evaluator.

### Implemented

- `Effect: "Allow"` / `Effect: "Deny"` with **Deny precedence** (any matching deny wins).
- `Action` / `NotAction` with `*` and `?` wildcards. Service prefix match is case-insensitive; action names are case-sensitive (matches AWS).
- `Resource` / `NotResource` with `*` and `?` wildcards.
- `Condition` blocks: all 28 operators AWS defines, plus the `...IfExists` suffix and the `ForAllValues:` / `ForAnyValue:` qualifiers. See the next section for details.
- Identity policies attached to:
  - IAM users (inline + managed + via group membership, inline and managed)
  - IAM roles (inline + managed, for assumed-role sessions)
- Managed policies resolve from both customer-managed policies you create and a built-in catalog of common AWS-managed policies (`arn:aws:iam::aws:policy/*`, e.g. `AdministratorAccess`, `AmazonS3FullAccess`, `AWSLambdaBasicExecutionRole`). Attaching an AWS-managed policy grants its permissions just like a customer-managed one.
- Empty effective policy set -> implicit deny.

### Condition block evaluation

A statement with a `Condition` block only applies when every entry in the block evaluates to `true` against the request-time context. Multiple operators inside a single `Condition` are AND-combined; multiple keys inside one operator are AND-combined; multiple values for one key are OR-combined (modulo the `ForAllValues` qualifier).

**Supported operators:**

| Category | Operators |
| --- | --- |
| String | `StringEquals`, `StringNotEquals`, `StringEqualsIgnoreCase`, `StringNotEqualsIgnoreCase`, `StringLike`, `StringNotLike` |
| Numeric | `NumericEquals`, `NumericNotEquals`, `NumericLessThan`, `NumericLessThanEquals`, `NumericGreaterThan`, `NumericGreaterThanEquals` |
| Date | `DateEquals`, `DateNotEquals`, `DateLessThan`, `DateLessThanEquals`, `DateGreaterThan`, `DateGreaterThanEquals` (RFC3339 and epoch seconds both accepted) |
| Boolean | `Bool` |
| Binary | `BinaryEquals` |
| IP address | `IpAddress`, `NotIpAddress` (v4 and v6 CIDR, plus bare addresses) |
| ARN | `ArnEquals`, `ArnNotEquals`, `ArnLike`, `ArnNotLike` |
| Existence | `Null` |

Every operator supports the `...IfExists` suffix (missing key evaluates to `true`) and the `ForAllValues:` / `ForAnyValue:` set-qualifier prefixes.

**Supported global condition keys:**

| Key | Source |
| --- | --- |
| `aws:username` | Last segment of the IAM user ARN; unset for assumed-role / federated principals, matching AWS |
| `aws:userid` | `Principal.user_id` (e.g. `AIDA...`, `AROA...:<session>`) |
| `aws:PrincipalArn` | Full principal ARN |
| `aws:PrincipalAccount` | 12-digit account ID sourced from the credential (#381 alignment), not global config |
| `aws:PrincipalType` | PascalCase label: `User` / `AssumedRole` / `FederatedUser` / `Account` |
| `aws:SourceIp` | Remote address of the incoming HTTP connection |
| `aws:CurrentTime` | Server-side evaluation timestamp (UTC) |
| `aws:EpochTime` | Same moment as `aws:CurrentTime`, in seconds since the Unix epoch |
| `aws:SecureTransport` | `true` iff the request carries `x-forwarded-proto: https` (the fakecloud server itself speaks HTTP; set this header from an upstream TLS terminator to test) |
| `aws:RequestedRegion` | Region extracted from SigV4 / config |

**Supported service-specific condition keys:**

| Key | Populated on | Source |
| --- | --- | --- |
| `s3:prefix` | `s3:ListObjects`, `s3:ListObjectsV2` | `?prefix=` query param |
| `s3:delimiter` | `s3:ListObjects`, `s3:ListObjectsV2` | `?delimiter=` query param |
| `s3:max-keys` | `s3:ListObjects`, `s3:ListObjectsV2` | `?max-keys=` query param |
| `sns:Protocol` | `sns:Subscribe` | `Protocol` request parameter |
| `sns:Endpoint` | `sns:Subscribe` | `Endpoint` request parameter |
| `lambda:FunctionArn` | `lambda:AddPermission` | Target function ARN resolved from the path |
| `lambda:Principal` | `lambda:AddPermission` | `Principal` field from the JSON body |
| `sqs:MessageAttribute.<Name>` | `sqs:SendMessage` | Each named `MessageAttribute`'s `StringValue` (Binary / Number attributes fall back to the data type) |

New services plug in by implementing `iam_condition_keys_for` on their `AwsService` impl; the dispatcher merges the result into the shared context before the evaluator runs.

**Safe-fail semantics.** Any unimplemented operator, unknown key, or parse failure (malformed date, invalid CIDR, non-numeric value where numeric expected) is logged to `fakecloud::iam::audit` at debug level and evaluates to `false` — i.e. the statement *does not apply*. The evaluator will never silently treat an unrecognized condition as a match. If you see unexpected denies, raise the `fakecloud::iam::audit` log level to see which statements were skipped.

### Resource-based policies

S3 bucket policies, SNS topic policies, Lambda function policies, and KMS key policies are fully wired into the evaluator. When enforcement is on and a resource has a policy attached, dispatch fetches it and hands it to the evaluator alongside the caller's identity policies; the evaluator combines the two using AWS's cross-account semantics:

- **Explicit Deny** from either the identity policy or the resource policy wins immediately.
- **Same-account** callers (principal account ID equals the resource's owning account): the request is allowed if the identity policy **or** the resource policy grants it.
- **Cross-account** callers: the request is allowed only if the identity policy **and** the resource policy both grant it.

The resource's owning account is parsed from the ARN; S3 ARNs have an empty account segment, so fakecloud falls back to the server's configured account ID (#381 multi-account alignment — the decision is per-ARN, not a global config knob).

**Where policies come from.**

- **S3 bucket policies** are stored by `PutBucketPolicy` and updated by `DeleteBucketPolicy`. `GetBucketPolicy` returns the raw JSON.
- **SNS topic policies** are stored in the topic's `Policy` attribute by `SetTopicAttributes` (full document) or by `AddPermission` / `RemovePermission` (incremental statements). `GetTopicAttributes` returns them.
- **Lambda function policies** are built incrementally by `AddPermission`: fakecloud composes a canonical `{"Version":"2012-10-17","Statement":[...]}` document from `(StatementId, Action, Principal, SourceArn?, SourceAccount?)` so the existing evaluator reads it without a Lambda-specific fork. `SourceArn` becomes an `ArnLike` `Condition` on `aws:SourceArn`, and `SourceAccount` becomes a `StringEquals` `Condition` on `aws:SourceAccount` — both are already in the operator set. `GetPolicy` returns the composed document; `RemovePermission` strips the matching `Sid` and leaves an empty `Statement` array behind, matching AWS.
- **IAM role trust policies** (`assume_role_policy_document`) are evaluated on every `AssumeRole`, `AssumeRoleWithSAML`, and `AssumeRoleWithWebIdentity` call before STS issues credentials. The trust policy is the *only* authorization source for role assumption — identity policies do not factor in. Caller principal, action (`sts:AssumeRole*`), `Condition` keys (`sts:ExternalId`, `sts:RoleSessionName`, `sts:SourceIdentity`, `aws:MultiFactorAuthPresent`, `aws:SourceAccount`), and federation-specific keys (`saml:aud`, `saml:iss`, `<provider>:aud`, `<provider>:sub`) are all populated. `AssumeRoleWithWebIdentity` additionally requires the JWT's `iss` to match a registered `OpenIDConnectProvider` and `aud` to be in its `client_id_list`; service-linked roles (path `/aws-service-role/<service>/...`) refuse non-service callers regardless of trust-policy contents.

**Principal matching.** Resource policies use `Principal` / `NotPrincipal` keys that identity policies don't. The evaluator supports the shapes resource policies actually use in practice:

| Shape | Meaning |
| --- | --- |
| `"Principal": "*"` | Any authenticated principal |
| `"Principal": {"AWS": "*"}` | Same as above |
| `"Principal": {"AWS": "arn:aws:iam::ACCOUNT:root"}` | Any principal whose account ID is `ACCOUNT` |
| `"Principal": {"AWS": "ACCOUNT"}` | 12-digit account ID shorthand, equivalent to `...:root` |
| `"Principal": {"AWS": "arn:aws:iam::ACCOUNT:user/alice"}` | Exact principal ARN match |
| `"Principal": {"AWS": [...]}` | List form — any entry matches |
| `"Principal": {"Service": "events.amazonaws.com"}` | Matches an assumed-role principal whose ARN contains the service host (covers EventBridge -> SNS and similar service-linked role scenarios) |
| `"Principal": {"Federated": "<provider>"}` | Matches a federated session minted by `AssumeRoleWithSAML` / `AssumeRoleWithWebIdentity` whose provider ARN equals `<provider>`. Used by IAM role trust policies. |

`NotPrincipal` is fully evaluated — a statement with `NotPrincipal` applies to all callers **except** those matching any entry in the list (the exact inverse of `Principal`). The classic AWS pattern `Deny` + `NotPrincipal` ("deny everyone except this user") works correctly. `NotPrincipal` entries with unrecognized principal types (`CanonicalUser`) are dropped from the match list; if all entries are unrecognized the statement is skipped with a `fakecloud::iam::audit` debug log and never silently grants.

`Principal` types other than `AWS` / `Service` / `Federated` (`CanonicalUser`) fall through to "doesn't match" for the same reason.

`Condition` blocks on resource-policy statements are evaluated with the same operator set and global condition keys as identity policies — the condition entry points are shared.

### Not implemented

Ongoing coverage work:

- Service-specific condition keys for services / operations beyond the ones listed in the table above. The hook is `AwsService::iam_condition_keys_for`; extending coverage is additive and requires no signature changes.

### Permission boundaries

A managed policy attached to a user or role that caps the maximum permissions that identity can ever be granted. The effective permissions of a user with a boundary are the **intersection** of identity policies and the boundary: both must allow, and an explicit Deny in either layer wins.

- Attached via `PutUserPermissionsBoundary` / `PutRolePermissionsBoundary`, removed via `DeleteUserPermissionsBoundary` / `DeleteRolePermissionsBoundary`.
- **Dangling boundary ARN** (the managed policy was deleted while still attached): the principal can perform no action until the boundary is removed or re-created — matches AWS behavior. Logged to `fakecloud::iam::audit` at debug level.
- **Bypass rules:** the account root and service-linked roles (role name starts with `AWSServiceRoleFor`) are exempt from boundary evaluation. Within `evaluate_with_resource_policy`, the boundary gates only the identity side — same-account resource-policy grants stand on their own.

### Session policies

Inline policies passed to `AssumeRole`, `AssumeRoleWithWebIdentity`, `AssumeRoleWithSAML`, or `GetFederationToken` via the `Policy` parameter (and `PolicyArns`) that further restrict the resulting temporary credentials below the role's own policies.

- Session policies are persisted on the STS temporary credential and evaluated as a third intersection layer: effective permission = **identity ∩ boundary ∩ session**. Each layer is evaluated independently; an explicit Deny in any layer wins.
- An STS call with no `Policy` / `PolicyArns` produces a credential with no session-policy gate (pass-through), preserving Phase 2 behavior.
- `GetSessionToken` does not accept a `Policy` parameter per AWS docs, so session policies do not apply to credentials minted by that operation.

**Phase 4 — ABAC (tag-based conditions).**

Tag-based access control via four condition key families:

| Condition key | Description | Enforced services |
|---|---|---|
| `aws:ResourceTag/<key>` | Tags on the target resource | S3, SQS, SNS, IAM, KMS |
| `aws:RequestTag/<key>` | Tags sent in the request (e.g. on CreateQueue, PutObject) | S3, SQS, SNS, IAM, KMS |
| `aws:TagKeys` | List of tag keys in the request (for `ForAllValues`/`ForAnyValue`) | S3, SQS, SNS, IAM, KMS |
| `aws:PrincipalTag/<key>` | Tags on the calling IAM user or assumed role | All enforced services |

Key semantics:

- The condition key prefix (`aws:ResourceTag/`) is matched **case-insensitively** per AWS. The tag key part after the slash (`Environment`) is matched **case-sensitively** — `aws:ResourceTag/Environment` and `aws:ResourceTag/environment` reference different tags.
- `aws:PrincipalTag/<key>` is populated from the IAM user's or assumed role's tags at credential resolution time.
- Services that don't implement ABAC yet (Lambda, Step Functions, etc.) gracefully skip tag evaluation with a `fakecloud::iam::audit` debug log. No fake tag values are ever returned.
- Adding ABAC support to a new service requires implementing two trait methods: `resource_tags_for()` and `request_tags_from()`.

## Phase 6 — Service Control Policies (SCPs)

fakecloud enforces SCPs as the top-of-chain allow-list ceiling above permission boundaries, session policies, and identity policies. SCPs only apply when an AWS Organizations organization exists in the process (see the [Organizations reference](/docs/reference/organizations)) — without one, nothing changes.

The evaluation chain, outermost to innermost:

```
SCP ceiling  ->  permission boundary  ->  session policy  ->  identity policy  (union resource policy)
```

Each layer is an intersection gate with AWS's standard semantics: if the layer is present it must evaluate to `Allow` for the request to survive. An explicit `Deny` in any layer wins immediately.

SCP-specific rules:

- **Inherited intersection.** When a member account lives under `root -> OU_A -> OU_B -> account`, every SCP attached along that path must allow the action. AWS intersects across ancestors; fakecloud mirrors it.
- **Management-account exemption.** The account that called `CreateOrganization` is never constrained by SCPs, matching AWS.
- **Service-linked role exemption.** Assumed roles whose name begins with `AWSServiceRoleFor` bypass SCP evaluation, same as permission boundaries.
- **Empty chain denies.** If `FullAWSAccess` is detached from the root and no other SCP grants the action up the path, the principal is denied. This matches AWS's allow-list ceiling semantics — SCPs are not deny lists.
- **Resource-policy branch untouched.** SCPs gate the identity side only. A resource policy in the resource's account has its own authority; the caller's SCPs have no reach there.

Audit logs emit on `fakecloud::iam::audit` when an SCP layer produces an `ExplicitDeny` or caps the intersection to `ImplicitDeny`, so tests can inspect the decision path.

## Practical example

Bootstrap a user with root credentials, attach a resource-scoped policy, then hit the service with their own access key:

```bash
# Start fakecloud with enforcement on.
FAKECLOUD_VERIFY_SIGV4=true FAKECLOUD_IAM=strict ./fakecloud

# Root-bypass bootstrap.
AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test \
  aws --endpoint-url http://localhost:4566 iam create-user --user-name alice
AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test \
  aws --endpoint-url http://localhost:4566 iam create-access-key --user-name alice
# -> emits AKIA..., SECRET...

AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test \
  aws --endpoint-url http://localhost:4566 iam put-user-policy \
    --user-name alice \
    --policy-name ReadSelf \
    --policy-document '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"iam:GetUser","Resource":"arn:aws:iam::123456789012:user/alice"}]}'

# Alice can read herself...
AWS_ACCESS_KEY_ID=<alice-akid> AWS_SECRET_ACCESS_KEY=<alice-secret> \
  aws --endpoint-url http://localhost:4566 iam get-user --user-name alice
# -> success

# ...but not anyone else.
AWS_ACCESS_KEY_ID=<alice-akid> AWS_SECRET_ACCESS_KEY=<alice-secret> \
  aws --endpoint-url http://localhost:4566 iam get-user --user-name root
# -> AccessDeniedException
```

## Image supply-chain (cosign + Trivy)

Every published container image — `ghcr.io/faiscadev/fakecloud` and the prebuilt RDS support images (`fakecloud-postgres`, `fakecloud-mysql`, `fakecloud-mariadb`) — is:

1. **Scanned** by [Trivy](https://github.com/aquasecurity/trivy) for `CRITICAL`/`HIGH` OS and library vulnerabilities (`ignore-unfixed: true`). The release fails closed if any are found.
2. **Signed** with [cosign](https://github.com/sigstore/cosign) keyless mode using the GitHub Actions OIDC token, so attestations are anchored to the workflow that built the image — no key management, no detached secret.

To verify an image before pulling:

```bash
cosign verify ghcr.io/faiscadev/fakecloud-postgres:16-0.13.1 \
  --certificate-identity-regexp '^https://github\.com/faiscadev/fakecloud/' \
  --certificate-oidc-issuer       https://token.actions.githubusercontent.com
```

Same shape works for `fakecloud-mysql`, `fakecloud-mariadb`, and the main `fakecloud` image. A successful verification means the image was built by a workflow run in the `faiscadev/fakecloud` repository — not republished by anyone else.

## See also

- [Limitations](@/docs/reference/limitations.md) — what fakecloud doesn't do at all
- [Configuration](@/docs/reference/configuration.md) — full flag + env var reference
- [IAM service docs](@/docs/services/iam.md) — per-action coverage
