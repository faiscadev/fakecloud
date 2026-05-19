+++
title = "Local ECR with real `docker push`: what LocalStack and Moto miss, and what fakecloud ships"
date = 2026-04-23
description = "LocalStack gates ECR behind Pro and `docker push` is flaky for those who pay. Moto ships a control plane but can't speak the OCI v2 Distribution protocol. fakecloud ships both — full 58-op API plus real `docker push`/`pull`, free."

[extra]
author = "Lucas Vieira"
+++

If your integration tests build or pull a container image and push it to ECR — CDK docker assets, ECS task definitions, Lambda container images, GitHub Actions workflows — you've hit the wall.

**LocalStack** gates ECR behind its paid Pro tier (the March 2026 shift to proprietary builds also means Community users have no ECR at all). Pro users who actually try `docker push` hit [#7186 "EOF for ecr/getCredentials"](https://github.com/localstack/localstack/issues/7186), [#5598 "pushed image, describe says ImageNotFound"](https://github.com/localstack/localstack/issues/5598), [#8128 "connection refused / 403 on docker push"](https://github.com/localstack/localstack/issues/8128), and [#12043 non-us-east-1 ECR timeouts](https://github.com/localstack/localstack/issues/12043). The bugs are still open.

**Moto** ships an ECR control plane — you can call `CreateRepository` and `DescribeImages` from boto3 — but Moto is not a real HTTP server for the OCI protocol. `docker push localhost:…/your-repo` doesn't work because the `/v2/<name>/blobs/uploads/` endpoints don't exist. If your tests rely on CDK building and pushing a docker asset, Moto is a dead end.

Most teams end up running a standalone `registry:2` container alongside their test harness. That works for raw Docker, but `registry:2` doesn't expose `DescribeRepositories`, `BatchGetImage`, `PutLifecyclePolicy`, or any of the other AWS-shaped operations. CDK, Terraform, and the AWS CLI can't talk to it.

## What fakecloud ships

fakecloud implements ECR end-to-end: all 58 operations in AWS's Smithy model **and** the OCI v2 Distribution HTTP protocol that Docker uses on the wire. Both back onto the same content-addressed sha256 blob store, so a layer pushed by `docker push` is visible via `BatchCheckLayerAvailability`, and an image manifest uploaded via `PutImage` can be `docker pull`ed.

```sh
# Start fakecloud.
curl -fsSL https://fakecloud.dev/install.sh | bash
fakecloud &

# Docker login works.
aws --endpoint-url http://localhost:4566 ecr get-login-password \
  | docker login --username AWS --password-stdin localhost:4566

# Push an image.
aws --endpoint-url http://localhost:4566 ecr create-repository --repository-name demo
docker pull busybox:latest
docker tag busybox:latest localhost:4566/demo:latest
docker push localhost:4566/demo:latest

# The AWS SDK sees it.
aws --endpoint-url http://localhost:4566 ecr describe-images --repository-name demo
```

`GetAuthorizationToken` returns `base64("AWS:<token>")` with a 12-hour expiry matching real AWS. The `/v2/` endpoints honor Basic Auth and issue a `WWW-Authenticate: Basic realm="fakecloud-ecr"` challenge on the initial unauthenticated request, so Docker's two-phase login does its handshake cleanly.

## What else lands in the 58-op surface

- **Lifecycle policy evaluation, not storage.** `PutLifecyclePolicy` applies immediately — `imageCountMoreThan` and `sinceImagePushed` rules actually prune images, ordered by `rulePriority` per AWS semantics. `GetLifecyclePolicyPreview` returns the digests that would be expired. No "stored but never evaluated" like LocalStack's SES receipt rules.
- **Image scanning.** `StartImageScan` stores a COMPLETE scan record that `DescribeImageScanFindings` echoes back in the AWS-shaped payload. Real scanner integration is out of scope for a mock; a schema-complete synthetic shape lets you exercise your findings plumbing.
- **Pull-through cache rules.** Full CRUD + Validate.
- **Registry policy, scanning config, replication config, repository creation templates, signing configuration, pull-time exclusions.** Every operation AWS lists.
- **Smithy-exact validation.** `@length` / `@range` / `@enum` / `@required` traits enforced everywhere — garbage inputs return `InvalidParameterException` / `ImageDigestDoesNotMatchException` / `LayerDigestMismatchException`, not silent success.

## Assert on what was pushed

fakecloud exposes an introspection API at `/_fakecloud/ecr/...` so your test code can check pushed images without parsing Docker output:

```ts
import { FakeCloud } from "fakecloud";

const fc = new FakeCloud();
const { repositories } = await fc.ecr.getRepositories();
const { images } = await fc.ecr.getImages("demo");

expect(repositories).toHaveLength(1);
expect(images[0].imageTags).toContain("latest");
expect(images[0].imageDigest).toMatch(/^sha256:/);
```

The same API is available from Go, Python, Java, PHP, and Rust.

## Conformance

58/58 ECR operations, 1,789/1,789 generated variant probes passing (100%) — this runs on every commit against AWS's Smithy model. The Smithy model drives positive probes (does the call succeed with sensible input?) and negative probes (does a missing required field, a too-long string, an out-of-range integer, a non-enum enum, return the right error code?). fakecloud's baseline is now 61,741/61,743 across the whole workspace.

## Where next

ECR is the prerequisite for ECS, which is the next item on the roadmap. ECS builds on the same Docker runtime pattern fakecloud already uses for Lambda — pull a real container image (from the local ECR we just built), run it, expose it via the ECS API. The moment that ships, the CDK-ECS-ECR pipeline works end-to-end in local tests.

[fakecloud is AGPL-3.0](https://github.com/faiscadev/fakecloud). No account needed, no paid tier.
