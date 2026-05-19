+++
title = "A Moto equivalent for Go, Java, Kotlin, and Node.js"
date = 2026-04-22
description = "Moto is Python-only. fakecloud is a real HTTP server speaking the AWS wire protocol, so the same thing works for Go, Java, Kotlin, Node.js, Rust, and PHP. Copy-paste examples for each language."

[extra]
author = "Lucas Vieira"
+++

If you write Python, you have Moto. Decorate a test, boto3 gets patched in-process, and your assertions run against an in-memory AWS. It is great, it has been great for years, and it is not available in any other language because Python is the only ecosystem where you can cleanly monkey-patch the SDK from inside a test process.

What if you write Go, Java, Kotlin, Node.js, Rust, or PHP?

The honest cross-language answer is: a real HTTP server speaking the AWS wire protocol. Point your language's AWS SDK at `http://localhost:4566`, and it doesn't matter whether the thing on the other end is AWS, LocalStack, MiniStack, floci, or fakecloud — the wire contract is the same.

This post is specifically about using [fakecloud](https://github.com/faiscadev/fakecloud) — free, open-source, single binary, no account, no paid tier — as a Moto equivalent in the languages Moto cannot reach.

## Why a real HTTP server, not an in-process patch

Moto works because Python lets you reach into a running process and replace functions. In Java and Go and most other typed languages, you can't really do that — the SDK is compiled, the method dispatch is known at build time, and monkey-patching `S3Client.putObject` is not a thing you do in production code.

The equivalent model in those languages is to configure the SDK's endpoint. Every AWS SDK supports pointing at a non-AWS endpoint URL. Point it at `http://localhost:4566`, and the SDK thinks it's talking to AWS.

The server on the other end handles all the ceremony: signing verification (optional), request parsing, response shaping, persistence, cross-service wiring. Your test code is normal — the only thing that's "fake" is the URL.

## Installing fakecloud

```sh
curl -fsSL https://fakecloud.dev/install.sh | bash
fakecloud
```

Listens on `http://localhost:4566`. One binary, ~19 MB, ~500ms startup.

## Go

```go
package store_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
)

func clientForTest(t *testing.T) *s3.Client {
	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
		config.WithEndpointResolverWithOptions(aws.EndpointResolverWithOptionsFunc(
			func(service, region string, opts ...interface{}) (aws.Endpoint, error) {
				return aws.Endpoint{URL: "http://localhost:4566"}, nil
			},
		)),
	)
	if err != nil {
		t.Fatal(err)
	}
	return s3.NewFromConfig(cfg, func(o *s3.Options) { o.UsePathStyle = true })
}

func TestPutAndGet(t *testing.T) {
	ctx := context.Background()
	c := clientForTest(t)

	_, err := c.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String("test")})
	if err != nil {
		t.Fatal(err)
	}
	// ... putObject, getObject, assert
}
```

That's Go integration tests against fakecloud. Same SDK, same API, same assertions — no mock. For SDK v1, use `WithEndpoint("http://localhost:4566")` on the session config.

## Java

```java
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

import java.net.URI;

class S3Test {
    static S3Client client() {
        return S3Client.builder()
            .endpointOverride(URI.create("http://localhost:4566"))
            .region(Region.US_EAST_1)
            .credentialsProvider(StaticCredentialsProvider.create(
                AwsBasicCredentials.create("test", "test")))
            .forcePathStyle(true)
            .build();
    }

    @Test
    void putAndGet() {
        var s3 = client();
        s3.createBucket(b -> b.bucket("test"));
        // ... assertions
    }
}
```

## Kotlin

Same as Java, nicer syntax:

```kotlin
import software.amazon.awssdk.auth.credentials.*
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3Client
import java.net.URI

fun s3(): S3Client = S3Client.builder()
    .endpointOverride(URI.create("http://localhost:4566"))
    .region(Region.US_EAST_1)
    .credentialsProvider(StaticCredentialsProvider.create(
        AwsBasicCredentials.create("test", "test")))
    .forcePathStyle(true)
    .build()
```

## Node.js (AWS SDK v3)

The SDK v3 respects `AWS_ENDPOINT_URL` automatically, so the test setup is trivial:

```ts
// jest.setup.ts or vitest.setup.ts
process.env.AWS_ENDPOINT_URL = "http://localhost:4566";
process.env.AWS_ACCESS_KEY_ID = "test";
process.env.AWS_SECRET_ACCESS_KEY = "test";
process.env.AWS_REGION = "us-east-1";
```

Your application code imports the AWS SDK normally and does not need any test-specific branching. Your tests import your code and call it. Your assertions check the real state on fakecloud afterwards.

For SDK v2 (deprecated but still widely used):

```ts
import AWS from "aws-sdk";
AWS.config.update({
  endpoint: "http://localhost:4566",
  accessKeyId: "test",
  secretAccessKey: "test",
  region: "us-east-1",
  s3ForcePathStyle: true,
});
```

## PHP

```php
use Aws\S3\S3Client;

$s3 = new S3Client([
    'version' => 'latest',
    'region'  => 'us-east-1',
    'endpoint' => 'http://localhost:4566',
    'credentials' => ['key' => 'test', 'secret' => 'test'],
    'use_path_style_endpoint' => true,
]);
```

## Rust

```rust
use aws_config::{BehaviorVersion, Region};
use aws_sdk_s3::config::Credentials;

async fn client() -> aws_sdk_s3::Client {
    let config = aws_config::defaults(BehaviorVersion::latest())
        .region(Region::new("us-east-1"))
        .endpoint_url("http://localhost:4566")
        .credentials_provider(Credentials::from_keys("test", "test", None))
        .load()
        .await;
    aws_sdk_s3::Client::new(&config)
}
```

## The part Moto cannot do, in any language

Moto patches SDK methods inside the test process. It does not run code — specifically, it does not execute Lambda functions. If your Python test creates a Lambda and invokes it, Moto returns a simulated response; your function's actual code does not run.

fakecloud does run the code. It pulls real Lambda runtime containers (13 runtimes: Node, Python, Java, Go, .NET, Ruby, custom) and executes your handler against them. So a test like "my Go service publishes to SNS, which triggers a Python Lambda, which writes to DynamoDB" works end-to-end — real Lambda code running, real SNS fan-out, real DynamoDB state.

```go
// Go test that publishes to SNS...
snsClient.Publish(ctx, &sns.PublishInput{
    TopicArn: aws.String(topicArn),
    Message:  aws.String(`{"event":"order.placed"}`),
})

// ...and your Python Lambda (deployed to fakecloud) actually runs.
// Then you assert on its side effect (a DynamoDB row) from the Go test.
```

This is the main thing a real HTTP emulator gives you that no in-process mocking library can.

## Assertion helpers

fakecloud's test-assertion SDKs give you introspection into what happened on the server, without writing raw HTTP:

```go
// Go
fc := fakecloud.New("http://localhost:4566")
invs, _ := fc.Lambda.GetInvocations(ctx, &fakecloud.GetInvocationsInput{FunctionName: "on-order"})
if len(invs.Invocations) != 1 { t.Fatal("expected 1 invocation") }
```

```ts
// Node
const fc = new FakeCloud();
const { invocations } = await fc.lambda.getInvocations({ functionName: "on-order" });
expect(invocations).toHaveLength(1);
```

```python
# Python (if you also want to use fakecloud from pytest alongside Go tests)
fc = FakeCloud()
invs = fc.lambda_.get_invocations(function_name="on-order")
assert len(invs.invocations) == 1
```

SDKs for TypeScript, Python, Go, PHP, Java, Rust: [fakecloud.dev/docs/sdks](https://fakecloud.dev/docs/sdks).

## Moto vs fakecloud: when to use which

| Situation | Pick |
|---|---|
| Python-only project, pure unit tests that just need boto3 to respond | Moto |
| Python + real cross-service wiring (Lambda actually runs, S3 -> Lambda actually fires) | fakecloud |
| Any non-Python language | fakecloud |
| Multi-language codebase sharing tests | fakecloud |
| Integration tests in CI across multiple services | fakecloud |

Moto and fakecloud are not really competitive — they solve slightly different problems. If you already use Moto and it works, keep it. If you need cross-service integrations, non-Python languages, or real Lambda execution, add fakecloud.

## Links

- Install: `curl -fsSL https://fakecloud.dev/install.sh | bash`
- Repo: [github.com/faiscadev/fakecloud](https://github.com/faiscadev/fakecloud)
- Moto: [github.com/getmoto/moto](https://github.com/getmoto/moto)
- Language SDK docs: [fakecloud.dev/docs/sdks](https://fakecloud.dev/docs/sdks)
- Issues: [github.com/faiscadev/fakecloud/issues](https://github.com/faiscadev/fakecloud/issues)
