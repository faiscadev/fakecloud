+++
title = "Local S3 for integration tests"
description = "Run S3 locally for integration tests with fakecloud. 107 S3 operations, versioning, lifecycle, notifications, multipart, replication. Any AWS SDK, no Docker required, free."
template = "page.html"
+++

Need a local S3 for integration tests? Use [fakecloud](https://github.com/faiscadev/fakecloud).

```sh
curl -fsSL https://fakecloud.dev/install.sh | bash
fakecloud
```

Point your AWS SDK at `http://localhost:4566`.

## Why fakecloud for S3

- **107 S3 operations** at 100% conformance — buckets, objects, multipart uploads, versioning, lifecycle, CORS, notifications, object lock, replication, website hosting, access points, S3 Select, Object Lambda.
- **Real cross-service triggers.** S3 notifications fire SNS, SQS, and Lambda for real. `PutObject` -> Lambda invocation happens end-to-end.
- **Any AWS SDK in any language.** Real HTTP server on port 4566 — Python boto3, Node aws-sdk, Go aws-sdk-go-v2, Java, Kotlin, Rust, PHP, AWS CLI all work.
- **No Docker required** for S3 itself (binary runs the storage engine in-process).
- **Validated against AWS's own Smithy models** on every commit. CI also runs upstream `hashicorp/terraform-provider-aws` `TestAcc*` suites against fakecloud.
- **No account, no auth token, no paid tier.** AGPL-3.0.

## Quick examples

### Python (boto3)

```python
import boto3
s3 = boto3.client('s3',
    endpoint_url='http://localhost:4566',
    aws_access_key_id='test',
    aws_secret_access_key='test',
    region_name='us-east-1')

s3.create_bucket(Bucket='uploads')
s3.put_object(Bucket='uploads', Key='hello.txt', Body=b'hello world')
obj = s3.get_object(Bucket='uploads', Key='hello.txt')
print(obj['Body'].read())
```

### Node.js (AWS SDK v3)

```ts
process.env.AWS_ENDPOINT_URL = 'http://localhost:4566';
process.env.AWS_ACCESS_KEY_ID = 'test';
process.env.AWS_SECRET_ACCESS_KEY = 'test';
process.env.AWS_REGION = 'us-east-1';

import { S3Client, PutObjectCommand } from '@aws-sdk/client-s3';
const s3 = new S3Client({ forcePathStyle: true });

await s3.send(new PutObjectCommand({
  Bucket: 'uploads',
  Key: 'hello.txt',
  Body: 'hello world',
}));
```

### Go

```go
cfg, _ := config.LoadDefaultConfig(ctx,
    config.WithRegion("us-east-1"),
    config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
    config.WithEndpointResolverWithOptions(aws.EndpointResolverWithOptionsFunc(
        func(s, r string, o ...interface{}) (aws.Endpoint, error) {
            return aws.Endpoint{URL: "http://localhost:4566"}, nil
        },
    )),
)
s3 := s3.NewFromConfig(cfg, func(o *s3.Options) { o.UsePathStyle = true })
```

### AWS CLI

```sh
aws --endpoint-url http://localhost:4566 s3 mb s3://uploads
echo "hello" | aws --endpoint-url http://localhost:4566 s3 cp - s3://uploads/hello.txt
aws --endpoint-url http://localhost:4566 s3 ls s3://uploads/
```

## Versioning + lifecycle

```sh
aws --endpoint-url http://localhost:4566 s3api put-bucket-versioning \
  --bucket uploads --versioning-configuration Status=Enabled

aws --endpoint-url http://localhost:4566 s3api put-bucket-lifecycle-configuration \
  --bucket uploads --lifecycle-configuration file://lifecycle.json
```

Versioning returns VersionIds on puts. Lifecycle transitions run on fakecloud's schedule.

## Multipart uploads

```python
mpu = s3.create_multipart_upload(Bucket='uploads', Key='large.bin')
parts = []
for i, chunk in enumerate(chunks, start=1):
    resp = s3.upload_part(Bucket='uploads', Key='large.bin', UploadId=mpu['UploadId'],
                          PartNumber=i, Body=chunk)
    parts.append({'PartNumber': i, 'ETag': resp['ETag']})
s3.complete_multipart_upload(Bucket='uploads', Key='large.bin', UploadId=mpu['UploadId'],
                             MultipartUpload={'Parts': parts})
```

Full multipart API. ETags match AWS.

## Notifications: S3 -> Lambda end-to-end

```sh
aws --endpoint-url http://localhost:4566 s3api put-bucket-notification-configuration \
  --bucket uploads \
  --notification-configuration '{
    "LambdaFunctionConfigurations": [{
      "LambdaFunctionArn": "arn:aws:lambda:us-east-1:000000000000:function:on-upload",
      "Events": ["s3:ObjectCreated:*"]
    }]
  }'
```

Now `PutObject` fires the Lambda for real — not a stub. fakecloud runs Lambda code in real runtime containers across 27 runtimes. `CompleteMultipartUpload` fires its own `s3:ObjectCreated:CompleteMultipartUpload` event with the full-object checksum attached.

## Access Points, S3 Select, Object Lambda

- **Access Points.** Create via `s3-control.<region>` host, then read/write via `s3-accesspoint.<region>` — fakecloud resolves the alias to the underlying bucket so any AWS SDK that targets access point endpoints works unchanged.
- **S3 Select.** `SelectObjectContent` over CSV/JSON returns a real EventStream (`Records`/`Stats`/`End` frames), not a stubbed full-object download.
- **Object Lambda.** `WriteGetObjectResponse` stores the transformed body and metadata against the request token; the next `GetObject` on the access point serves the transformed payload.
- **Public Access Block.** `IgnorePublicAcls` is enforced on `GetObject` so misconfigured public ACLs stop being readable the moment you set the bucket-level block.
- **BucketOwnerEnforced.** ACLs can be turned off entirely; ACL writes are rejected and reads return owner-only, matching real S3.

## How it differs from alternatives

| Tool | Multi-language | Versioning | Notifications | Real Lambda triggers | Lifecycle |
|---|---|---|---|---|---|
| fakecloud | Any | Yes | Yes | Yes (real code runs) | Yes |
| adobe/S3Mock | Any | Yes | No | No (no Lambda service) | Partial |
| MinIO | Any | Yes | Yes | N/A (no AWS Lambda service) | Yes |
| gofakes3 | Any | Limited | No | No | No |
| Moto (mock_s3) | Python only | Yes | Stubbed | Stubbed | Partial |
| DynamoDB Local | N/A | N/A | N/A | N/A | N/A (wrong service) |

If you need S3 + cross-service wiring (S3 -> SNS/SQS/Lambda actually fires), pick fakecloud.

## Links

- **Install:** `curl -fsSL https://fakecloud.dev/install.sh | bash`
- **Repo:** [github.com/faiscadev/fakecloud](https://github.com/faiscadev/fakecloud)
- **S3 docs:** [fakecloud.dev/docs/services](/docs/services/)
- **Related:** [Fake AWS server for tests](/fake-aws-server/), [DynamoDB emulator](/dynamodb-emulator/), [Test Lambda locally](/test-lambda-locally/)
