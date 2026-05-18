+++
title = "Known limitations"
description = "What fakecloud doesn't do, and what to watch out for."
weight = 4
+++

fakecloud aims for 100% behavioral parity with AWS on every implemented operation. A few things are genuinely not supported, and it's worth being explicit about them.

## SNS email and SMS delivery

SNS messages published to email or SMS endpoints are recorded for introspection at `/_fakecloud/sns/messages` but are never actually sent. There's no SMTP or SMS gateway attached. Use the introspection endpoint to assert delivery in your tests.

If you want real email sending, use SES (which also records to `/_fakecloud/ses/emails` and doesn't hit the network).

## SSM Session Manager data plane

`StartSession` returns `400 TargetNotConnected` and `ResumeSession` returns
`400 DoesNotExistException` by default. Real Session Manager hands you a
websocket URL backed by an SSM agent on the target instance; fakecloud
doesn't run that agent or the websocket relay, and returning a fake stream
URL would make integration tests think the session was live. The error
codes are both declared on the respective operations in the SSM Smithy
model, so SDK clients deserialize a known shape.

Two opt-in modes keep round-trip flows working:

- `FAKECLOUD_SSM_SESSION_ECHO=1` — `StartSession` / `ResumeSession` succeed
  with a sentinel token (`fakecloud-echo-mode-not-real-websocket`). Pair
  with `DescribeSessions` / `TerminateSession` to round-trip the SDK.
- `POST /_fakecloud/ssm/sessions/inject` — drop a session record into state
  directly, no env var needed. See
  [SSM service docs](@/docs/services/ssm.md#session-manager) for the body
  shape.

## CloudWatch Logs anomaly detection

Anomaly detectors are fully managed via the control plane — create, update, list, delete all work and conform to AWS. But no anomaly analysis actually runs. `ListAnomalies` returns an empty list. If your code depends on detecting anomalies, you'll need to fake the detections another way.

## Docker socket required for Lambda / RDS / ElastiCache

Lambda function execution, RDS DB instances, and ElastiCache clusters all use real Docker containers. This means fakecloud needs access to a Docker socket (`/var/run/docker.sock` on Linux/macOS) to start and stop those containers on demand.

**Security note:** granting access to the Docker socket gives the fakecloud process the ability to manage containers on the host. Only use this in development or CI environments you trust. Don't run fakecloud with Docker socket access on shared infrastructure.

If you don't need Lambda/RDS/ElastiCache, fakecloud runs fine without Docker at all — just don't mount the socket and those services will return errors only when you try to use them.

## SigV4 signatures and IAM policies are off by default

fakecloud parses SigV4 headers for routing and stores IAM policies without evaluating them. Both can be turned on explicitly:

```bash
FAKECLOUD_VERIFY_SIGV4=true    # real cryptographic signature verification
FAKECLOUD_IAM=soft|strict      # identity + condition + resource-policy evaluation
```

Evaluation covers `Allow` / `Deny` with Deny precedence, `Action` / `NotAction` / `Resource` / `NotResource` with wildcards, identity policies attached via user/group/role, `Condition` blocks with all 28 AWS operators against global keys plus service-specific keys for S3/SNS/Lambda/SQS, resource-based policies for S3 bucket policies, SNS topic policies, Lambda function policies, and KMS key policies (with AWS's cross-account combining semantics), full `Principal` / `NotPrincipal` matching on resource-based policies, permission boundaries (`PutUserPermissionsBoundary` / `PutRolePermissionsBoundary`), session policies passed to `AssumeRole` / `AssumeRoleWithWebIdentity` / `AssumeRoleWithSAML` / `GetFederationToken`, ABAC tag conditions (`aws:ResourceTag/<key>`, `aws:RequestTag/<key>`, `aws:TagKeys`, `aws:PrincipalTag/<key>`) on S3, SQS, SNS, and IAM resources, and Organizations SCPs (Service Control Policies) as a permissions ceiling across multi-account setups. See [SigV4 verification and IAM enforcement](@/docs/reference/security.md) for the full scope, enforced-service list, and the reserved `test`/`test` root-bypass convention.

## Everything else is in scope

If you hit a behavior that's documented in the AWS Smithy model and fakecloud doesn't match it, that's a bug. [Open an issue](https://github.com/faiscadev/fakecloud/issues).
