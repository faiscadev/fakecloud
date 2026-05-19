+++
title = "Local ECR for integration tests"
description = "Run local Amazon ECR for tests with fakecloud. Full 58-operation API, real OCI v2 Distribution so docker push and docker pull actually work. Free, AGPL-3.0."
template = "page.html"
+++

Need local ECR for integration tests? Use [fakecloud](https://github.com/faiscadev/fakecloud).

```sh
curl -fsSL https://fakecloud.dev/install.sh | bash
fakecloud
```

Point your AWS SDK at `http://localhost:4566`. `docker push` / `docker pull` work against the same port because fakecloud implements the OCI v2 Distribution protocol, not just the JSON control plane.

## Why fakecloud for ECR

- **Full API — 58 operations at 100% conformance.** Repositories, images, layers, tags, policies, lifecycle evaluation, image scanning, registry policy, pull-through cache, replication, repository creation templates, signing configuration, pull-time exclusions.
- **Real `docker push` / `docker pull`.** The OCI v2 Distribution HTTP endpoints (`/v2/<name>/blobs/...`, `/v2/<name>/manifests/<ref>`, etc.) are wired to the same content-addressed blob store the AWS JSON ops read and write. A layer pushed by Docker is visible via `BatchCheckLayerAvailability`; an image created via `PutImage` can be `docker pull`ed.
- **`GetAuthorizationToken` that Docker accepts.** Returns `base64("AWS:<token>")` with a 12h expiry matching real AWS, and the `/v2/` endpoints honor the Basic Auth flow. `aws ecr get-login-password | docker login localhost:4566 --username AWS --password-stdin` works.
- **Real lifecycle policy evaluator.** `PutLifecyclePolicy` applies immediately. Selectors supported: `tagStatus` (tagged/untagged/any), `tagPrefixList`, `tagPatternList` (`*` wildcards), `countType` (`imageCountMoreThan` and `sinceImagePushed`) with `countUnit` days/hours. Rules run in ascending `rulePriority` so earlier rules' decisions can't be reversed. `GetLifecyclePolicyPreview` returns the digests that would be expired.
- **Smithy-exact validation.** Every `@length` / `@range` / `@enum` / `@required` trait from the AWS Smithy model is enforced — invalid inputs return `InvalidParameterException` / `ImageDigestDoesNotMatchException` / `LayerDigestMismatchException` with AWS-shaped error payloads.

## Smoke test (5 commands)

```sh
# Start fakecloud.
fakecloud &

# Log Docker into the fake registry.
aws --endpoint-url http://localhost:4566 ecr get-login-password \
  | docker login --username AWS --password-stdin localhost:4566

# Create the repo, push an image.
aws --endpoint-url http://localhost:4566 ecr create-repository --repository-name demo
docker pull busybox:latest
docker tag busybox:latest localhost:4566/demo:latest
docker push localhost:4566/demo:latest

# Verify with the AWS SDK.
aws --endpoint-url http://localhost:4566 ecr describe-images --repository-name demo
```

## Assert on what was pushed (first-party SDKs)

fakecloud exposes an introspection API at `/_fakecloud/ecr/...` so your test code can assert on pushed images without re-parsing Docker output.

```ts
import { FakeCloud } from "fakecloud";

const fc = new FakeCloud();
const { repositories } = await fc.ecr.getRepositories();
const { images } = await fc.ecr.getImages("demo");

expect(repositories).toHaveLength(1);
expect(images[0].imageTags).toContain("latest");
```

Same API in Python, Go, Java, PHP, and Rust. See [the SDKs page](/docs/sdks) for per-language examples.

## Alternatives

- **LocalStack Community** — ECR moved to the paid Pro tier in March 2026. Community users don't have ECR at all, and Pro's `docker push` has been flaky (see [LocalStack #7186](https://github.com/localstack/localstack/issues/7186), [#5598](https://github.com/localstack/localstack/issues/5598), [#8128](https://github.com/localstack/localstack/issues/8128), [#12043](https://github.com/localstack/localstack/issues/12043)).
- **Moto / Moto-ext** — Ships an ECR control plane but does not implement the OCI v2 Distribution protocol, so real `docker push` and `docker pull` do not work.
- **Local registry (`registry:2`)** — Works for raw Docker but doesn't expose the AWS SDK-facing ECR operations (`DescribeRepositories`, `BatchGetImage`, `PutLifecyclePolicy`, etc.), so CDK / Terraform / boto3 integration tests can't use it.

fakecloud does both.

## See also

- [fakecloud on GitHub](https://github.com/faiscadev/fakecloud) — source, issues, discussions.
- [fakecloud vs LocalStack](/vs/localstack) — full comparison table.
- [Docs](https://fakecloud.dev/docs).
