+++
title = "ECR"
description = "Elastic Container Registry — full 62-operation API plus real OCI v2 Distribution so docker push and docker pull work against fakecloud."
weight = 21
+++

fakecloud implements Amazon Elastic Container Registry (ECR) with full API coverage. 62 operations, plus the OCI v2 Distribution HTTP surface that `docker push` / `docker pull` actually speak.

**Status: full API.** Repositories, images, layers, tags, lifecycle policy evaluation, image scanning, registry policy, pull-through cache, replication, repository creation templates, signing configuration, and pull-time exclusions.

## Supported today (full API)

- **Repositories** — `CreateRepository`, `DescribeRepositories`, `DeleteRepository`, `GetRepositoryPolicy`, `SetRepositoryPolicy`, `DeleteRepositoryPolicy`, `PutImageTagMutability`, `PutImageScanningConfiguration`
- **Images** — `BatchGetImage`, `DescribeImages`, `ListImages`, `BatchDeleteImage`, `PutImage`, `BatchCheckLayerAvailability`, `InitiateLayerUpload`, `UploadLayerPart`, `CompleteLayerUpload`, `GetDownloadUrlForLayer`
- **Lifecycle policies** — `PutLifecyclePolicy`, `GetLifecyclePolicy`, `DeleteLifecyclePolicy`, `StartLifecyclePolicyPreview`, `GetLifecyclePolicyPreview`
- **Image scanning** — `StartImageScan`, `DescribeImageScanFindings`, `GetRegistryScanningConfiguration`, `PutRegistryScanningConfiguration`. When the optional [Trivy](https://github.com/aquasecurity/trivy) CLI is on `PATH` (or `FAKECLOUD_TRIVY_BIN` is set), fakecloud reconstructs the pushed layers as a docker-format tar, invokes `trivy image --input <tar> --format json`, and exposes the parsed CVE findings via `DescribeImageScanFindings`. Without Trivy, scans complete with an empty findings list so client plumbing still works.
- **Registry policy** — `GetRegistryPolicy`, `PutRegistryPolicy`, `DeleteRegistryPolicy`
- **Replication** — `DescribeRegistry`, `PutReplicationConfiguration`
- **Pull-through cache** — `CreatePullThroughCacheRule`, `DeletePullThroughCacheRule`, `DescribePullThroughCacheRules`, `UpdatePullThroughCacheRule`, `BatchGetRepositoryScanningConfiguration`, `ValidatePullThroughCacheRule`
- **Repository templates** — `CreateRepositoryCreationTemplate`, `UpdateRepositoryCreationTemplate`, `DeleteRepositoryCreationTemplate`, `DescribeRepositoryCreationTemplates`
- **Signing** — `DescribeImageReplicationStatus`, real `cosign` keyed-mode (ECDSA-P256) signature verification on push when configured
- **Auth + tagging** — `GetAuthorizationToken`, `TagResource`, `UntagResource`, `ListTagsForResource`

## Real `docker push` / `docker pull`

The OCI v2 Distribution endpoints (`/v2/<name>/blobs/<digest>`, `/v2/<name>/manifests/<ref>`, `/v2/<name>/blobs/uploads/`, etc.) are wired to the same content-addressed blob store the AWS JSON ops read and write. A layer pushed by Docker is visible via `BatchCheckLayerAvailability`; an image created via `PutImage` can be `docker pull`ed.

`GetAuthorizationToken` returns `base64("AWS:<token>")` with a 12h expiry matching real AWS, and the `/v2/` endpoints honor the Basic Auth flow:

```sh
aws --endpoint-url http://localhost:4566 ecr get-login-password \
  | docker login --username AWS --password-stdin localhost:4566

aws --endpoint-url http://localhost:4566 ecr create-repository --repository-name demo
docker pull busybox:latest
docker tag busybox:latest localhost:4566/demo:latest
docker push localhost:4566/demo:latest

aws --endpoint-url http://localhost:4566 ecr describe-images --repository-name demo
```

## Real lifecycle policy evaluator

`PutLifecyclePolicy` applies immediately. Selectors supported:

- `tagStatus` (`tagged` / `untagged` / `any`)
- `tagPrefixList`
- `tagPatternList` (`*` wildcards)
- `countType` (`imageCountMoreThan` and `sinceImagePushed`) with `countUnit` days/hours

Rules run in ascending `rulePriority` so earlier rules' decisions can't be reversed. `GetLifecyclePolicyPreview` returns the digests that would be expired.

## Cosign signature verification

When a repository has `cosign` keyed-mode verification configured, `PutImage` requires a matching ECDSA-P256 signature manifest already present in the registry. fakecloud verifies the signature against the configured public key the same way real AWS Signer + ECR Pull-Through behaves. Unsigned or wrong-key images are rejected with `LayerInaccessibleException`.

## Cross-service integration

- **ECS task definitions -> ECR** — Tasks that reference AWS-private-ECR URIs (`<account>.dkr.ecr.<region>.amazonaws.com/<repo>:<tag>`) are resolved against fakecloud's local OCI endpoint at runtime. The ECS runtime pulls from `127.0.0.1:<port>/<repo>:<tag>`, retags to the AWS URI, then runs the container under the user-visible image name.
- **Lambda Image package -> ECR** — Lambda functions deployed with `PackageType=Image` whose `Code.ImageUri` points at a fakecloud ECR URI are pulled and run on invoke through the same resolution path.
- **ECR signing -> KMS** — When cosign keyed-mode is enabled, signature verification reads the configured public key (or KMS-backed key) before allowing the image to be tagged.

## Smithy-exact validation

Every `@length` / `@range` / `@enum` / `@required` trait from the AWS Smithy model is enforced — invalid inputs return `InvalidParameterException` / `ImageDigestDoesNotMatchException` / `LayerDigestMismatchException` with AWS-shaped error payloads.

## Protocol

JSON protocol over `POST /` with `X-Amz-Target: AmazonEC2ContainerRegistry_V20150921.<Action>`, alongside the OCI v2 surface mounted at `/v2/`.

## Introspection

| Endpoint | Method | Purpose |
|---|---|---|
| `/_fakecloud/ecr/repositories` | GET | Dump every repository across all accounts |
| `/_fakecloud/ecr/images` | GET | Dump every image; filter with `?repository=` |
| `/_fakecloud/ecr/blobs` | GET | Inspect the content-addressed blob store |

## SDK usage (testing helper)

```typescript
// TypeScript
const fc = new FakeCloud();
const { repositories } = await fc.ecr.getRepositories();
const { images } = await fc.ecr.getImages("demo");

expect(repositories).toHaveLength(1);
expect(images[0].imageTags).toContain("latest");
```

```python
# Python
async with FakeCloud() as fc:
    repos = await fc.ecr.get_repositories()
    images = await fc.ecr.get_images("demo")
```

```go
// Go
fc := fakecloud.New("http://localhost:4566")
repos, _ := fc.ECR().GetRepositories(ctx)
images, _ := fc.ECR().GetImages(ctx, "demo")
```

```rust
// Rust
let fc = FakeCloud::new("http://localhost:4566");
let repos = fc.ecr().get_repositories().await?;
let images = fc.ecr().get_images("demo").await?;
```

## Source

- [`crates/fakecloud-ecr`](https://github.com/faiscadev/fakecloud/tree/main/crates/fakecloud-ecr)
- [AWS ECR API reference](https://docs.aws.amazon.com/AmazonECR/latest/APIReference/Welcome.html)
