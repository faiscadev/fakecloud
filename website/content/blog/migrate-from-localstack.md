+++
title = "Migrating from LocalStack to fakecloud in 10 minutes"
date = 2026-04-22
description = "Step-by-step migration from LocalStack Community to fakecloud: docker-compose, CI, Terraform, CDK, Serverless Framework. Copy-paste configs for every common setup."

[extra]
author = "Lucas Vieira"
+++

In March 2026, LocalStack replaced its open-source Community Edition with a proprietary image that requires an account and an auth token. If your build broke last month, this guide is for you. If you are still on a pinned older tag and worried about the next pull, this is also for you.

fakecloud is a free, open-source AWS emulator — single binary, no account, no token, no paid tier — that covers the services most teams relied on LocalStack Community for, plus several that moved to LocalStack Pro (RDS, ElastiCache, Cognito User Pools, SES v2, API Gateway v2).

This guide is step-by-step. Copy, paste, done.

## The one-line summary

Change the image or the install command. Keep `http://localhost:4566` and your dummy credentials. Everything else stays the same.

## Step 1: Stop LocalStack

```sh
docker compose down
# or: docker kill $(docker ps -q --filter ancestor=localstack/localstack)
```

## Step 2: Install fakecloud

Pick one:

```sh
# Option A: single binary, no Docker
curl -fsSL https://fakecloud.dev/install.sh | bash
fakecloud

# Option B: Docker
docker run --rm -p 4566:4566 ghcr.io/faiscadev/fakecloud

# Option C: cargo
cargo install fakecloud
```

fakecloud listens on `http://localhost:4566` — same as LocalStack.

## Step 3: Keep your SDK wiring

Your application code does not change. The endpoint URL and dummy credentials stay identical:

```ts
// TypeScript
import { S3Client } from "@aws-sdk/client-s3";
const s3 = new S3Client({
  endpoint: "http://localhost:4566",
  region: "us-east-1",
  credentials: { accessKeyId: "test", secretAccessKey: "test" },
  forcePathStyle: true,
});
```

```python
# Python (boto3)
import boto3
s3 = boto3.client(
    "s3",
    endpoint_url="http://localhost:4566",
    aws_access_key_id="test",
    aws_secret_access_key="test",
    region_name="us-east-1",
)
```

```go
// Go
cfg, _ := config.LoadDefaultConfig(ctx,
    config.WithRegion("us-east-1"),
    config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
    config.WithEndpointResolverWithOptions(aws.EndpointResolverWithOptionsFunc(
        func(service, region string, options ...interface{}) (aws.Endpoint, error) {
            return aws.Endpoint{URL: "http://localhost:4566"}, nil
        },
    )),
)
```

## Step 4: Update docker-compose.yml

**Before:**

```yaml
services:
  localstack:
    image: localstack/localstack:latest
    ports:
      - "4566:4566"
    environment:
      - SERVICES=s3,sqs,sns,dynamodb,lambda
      - DEBUG=1
```

**After:**

```yaml
services:
  fakecloud:
    image: ghcr.io/faiscadev/fakecloud:latest
    ports:
      - "4566:4566"
```

fakecloud starts all services by default (they are lazy and cheap — no `SERVICES` env var needed). If you want to pass flags:

```yaml
services:
  fakecloud:
    image: ghcr.io/faiscadev/fakecloud:latest
    ports:
      - "4566:4566"
    command: ["fakecloud", "--host", "0.0.0.0", "--port", "4566"]
```

For Lambda execution you will need Docker-in-Docker or a mounted Docker socket (same as LocalStack Pro required):

```yaml
services:
  fakecloud:
    image: ghcr.io/faiscadev/fakecloud:latest
    ports:
      - "4566:4566"
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock
```

## Step 5: Update GitHub Actions

**Before:**

```yaml
services:
  localstack:
    image: localstack/localstack
    ports:
      - 4566:4566
    env:
      LOCALSTACK_AUTH_TOKEN: ${{ secrets.LOCALSTACK_TOKEN }}  # now required
```

**After (service container):**

```yaml
services:
  fakecloud:
    image: ghcr.io/faiscadev/fakecloud:latest
    ports:
      - 4566:4566
```

**After (install-and-run, no Docker):**

```yaml
steps:
  - run: curl -fsSL https://fakecloud.dev/install.sh | bash
  - run: fakecloud &
  - run: |
      for i in $(seq 1 30); do
        curl -sf http://localhost:4566/_fakecloud/health && exit 0
        sleep 1
      done
      exit 1
```

The install-and-run pattern is ~500ms vs ~3s for LocalStack container boot. On a cold CI runner the difference is noticeable over hundreds of test runs.

## Step 6: Terraform / OpenTofu

No change needed. The provider block stays the same — only the running emulator changes.

```hcl
provider "aws" {
  access_key                  = "test"
  secret_key                  = "test"
  region                      = "us-east-1"
  skip_credentials_validation = true
  skip_metadata_api_check     = true
  skip_requesting_account_id  = true

  endpoints {
    s3       = "http://localhost:4566"
    sqs      = "http://localhost:4566"
    sns      = "http://localhost:4566"
    dynamodb = "http://localhost:4566"
    lambda   = "http://localhost:4566"
    # ...
  }
}
```

fakecloud's CI runs the upstream `hashicorp/terraform-provider-aws` `TestAcc*` suites against itself, so Terraform flows that worked against LocalStack Community should work against fakecloud. If you hit a mismatch, it is a bug — open an issue.

## Step 7: CDK — cdklocal

CDK users with `cdklocal` change the endpoint override:

```sh
cdklocal bootstrap --endpoint-url http://localhost:4566
cdklocal deploy --endpoint-url http://localhost:4566
```

Or set `AWS_ENDPOINT_URL=http://localhost:4566` in your shell and use plain `cdk`.

## Step 8: Serverless Framework

If you used `serverless-localstack`, the simplest migration is to drop the plugin and set `AWS_ENDPOINT_URL` before running `serverless`:

```sh
export AWS_ENDPOINT_URL=http://localhost:4566
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test
export AWS_REGION=us-east-1
serverless deploy
```

AWS SDK v3 (which Serverless Framework uses internally on recent versions) respects `AWS_ENDPOINT_URL` automatically — no plugin or custom config required. If you're on an older version that doesn't, configure the endpoint through whatever per-service override your current plugin exposes.

## Things that may need attention

**SERVICES env var.** LocalStack used `SERVICES=s3,sqs,...` to scope which services booted. fakecloud starts all services by default and they are cheap. Drop the env var.

**LOCALSTACK_AUTH_TOKEN.** Drop it. fakecloud does not use auth tokens.

**Gateway / edge URL.** LocalStack had historical URLs like `http://localhost:4566` and older per-service ports (4572, 4576, etc). fakecloud uses only `4566` — same as modern LocalStack. If you still have old per-service URLs pinned, consolidate them.

**Regions.** fakecloud respects the region from your SDK call. Default `us-east-1` works everywhere.

**Persisted state.** LocalStack Pro has `PERSISTENCE=1`. fakecloud has `--persist /path/to/dir` for the same effect.

## Services that moved from LocalStack Community to Pro (and what fakecloud does)

| Service | LocalStack Community (pre-Mar 2026) | LocalStack Community (now) | fakecloud |
|---|---|---|---|
| RDS | [Paid only](https://docs.localstack.cloud/references/licensing/) — always was | Paid only | 163 ops, real PostgreSQL/MySQL/MariaDB via Docker |
| ElastiCache | Paid only | Paid only | 75 ops, real Redis/Valkey via Docker |
| Cognito User Pools | Free | [Paid only](https://docs.localstack.cloud/references/licensing/) | 122 ops, full auth flows + MFA |
| SES v2 | Free (limited) | [Paid only](https://docs.localstack.cloud/references/licensing/) | 110 ops, full send + templates + DKIM |
| API Gateway v2 | Free | [Paid only](https://docs.localstack.cloud/references/licensing/) | 28 ops, HTTP APIs + JWT/Lambda authorizers |
| Bedrock | Not available | Not available | 111 ops (control plane + runtime) |

## Assertion helpers (optional)

This is new capability LocalStack never had: fakecloud ships test-assertion SDKs that let you inspect side effects from tests without raw HTTP.

```ts
import { FakeCloud } from "fakecloud";
const fc = new FakeCloud();

// Your app code sends an email through the normal AWS SDK.
// Your test asserts the side effect directly.
const { emails } = await fc.ses.getEmails();
expect(emails).toHaveLength(1);
expect(emails[0].destination.toAddresses).toContain("alice@example.com");

// Reset between tests.
await fc.reset();
```

SDKs for TypeScript, Python, Go, PHP, Java, Rust. See [the SDK docs](https://fakecloud.dev/docs/sdks).

## Verify the migration

One-shot smoke test that exercises a cross-service flow:

```sh
# Create a bucket, upload, confirm, teardown
aws --endpoint-url http://localhost:4566 s3 mb s3://test-bucket
echo hello | aws --endpoint-url http://localhost:4566 s3 cp - s3://test-bucket/hello.txt
aws --endpoint-url http://localhost:4566 s3 ls s3://test-bucket/
aws --endpoint-url http://localhost:4566 s3 rb s3://test-bucket --force

# Lambda round-trip
echo 'exports.handler = async () => ({ ok: true })' > index.js
zip fn.zip index.js
aws --endpoint-url http://localhost:4566 lambda create-function \
  --function-name smoke --runtime nodejs20.x \
  --role arn:aws:iam::000000000000:role/lambda-role \
  --handler index.handler --zip-file fileb://fn.zip
aws --endpoint-url http://localhost:4566 lambda invoke \
  --function-name smoke out.json && cat out.json
```

If both print what you expect, migration is done.

## A note on coverage

fakecloud's goal is every AWS service at 100% conformance, including every cross-service integration. Services land depth-first: a service is supported when it passes the Smithy-model test variants and the cross-service wire-ups that matter for it — not when the API surface looks filled in. The current 23 services (1,680 operations) are what's done today; more are on the roadmap, prioritized by real-project demand. If a service you need isn't on the list, [open an issue](https://github.com/faiscadev/fakecloud/issues) and it'll get weighed into the queue.

For mission-critical pre-prod validation where you need the full production-parity surface, run against real AWS — that's true of any emulator, not a fakecloud gap specifically.

## Links

- Install: `curl -fsSL https://fakecloud.dev/install.sh | bash`
- Repo: [github.com/faiscadev/fakecloud](https://github.com/faiscadev/fakecloud)
- Site: [fakecloud.dev](https://fakecloud.dev)
- Comparison: [fakecloud vs LocalStack, MiniStack, floci, Moto](/blog/localstack-alternatives-compared/)
- Issues: [github.com/faiscadev/fakecloud/issues](https://github.com/faiscadev/fakecloud/issues)
