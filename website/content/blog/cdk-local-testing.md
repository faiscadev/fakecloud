+++
title = "CDK local testing: full flow with fakecloud and cdklocal"
date = 2026-04-22
description = "Deploy and test AWS CDK apps locally with fakecloud. Full cdklocal flow, plain cdk with AWS_ENDPOINT_URL, CI setup. Free, open-source, no account required."

[extra]
author = "Lucas Vieira"
+++

AWS CDK writes CloudFormation under the hood and ships it through the AWS SDK. That means "CDK local testing" boils down to one question: where does the SDK send its CreateStack / UpdateStack calls?

Pointed at real AWS, you pay per resource, wait minutes per deploy, and have to remember to destroy. Pointed at a local emulator, you iterate fast and don't pay anything. This guide covers both flows (`cdklocal` wrapper and plain `cdk` with endpoint overrides) against [fakecloud](https://github.com/faiscadev/fakecloud), a free, open-source AWS emulator with real CloudFormation support (90 operations, template parsing, resource provisioning, custom resources, drift detection).

## Install fakecloud

```sh
curl -fsSL https://fakecloud.dev/install.sh | bash
fakecloud
```

Listens on `http://localhost:4566`. ~500ms startup.

## Option 1: plain `cdk` with `AWS_ENDPOINT_URL`

Simplest and works with any CDK version that uses AWS SDK v3 (CDK 2.67+).

```sh
export AWS_ENDPOINT_URL=http://localhost:4566
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test
export AWS_REGION=us-east-1
export CDK_DEFAULT_ACCOUNT=000000000000
export CDK_DEFAULT_REGION=us-east-1

cdk bootstrap
cdk deploy
```

That's it. Same `cdk` binary you use for prod, pointed at fakecloud for dev.

## Option 2: `cdklocal` wrapper

If your team is already using `cdklocal` (from the LocalStack ecosystem), it works the same way against fakecloud:

```sh
cdklocal bootstrap --endpoint-url http://localhost:4566
cdklocal deploy --endpoint-url http://localhost:4566
```

## A minimal stack

```ts
// lib/my-stack.ts
import { Stack, StackProps } from 'aws-cdk-lib';
import { Bucket } from 'aws-cdk-lib/aws-s3';
import { Function, Runtime, Code } from 'aws-cdk-lib/aws-lambda';
import { Queue } from 'aws-cdk-lib/aws-sqs';
import { SqsEventSource } from 'aws-cdk-lib/aws-lambda-event-sources';
import { Construct } from 'constructs';

export class MyStack extends Stack {
  constructor(scope: Construct, id: string, props?: StackProps) {
    super(scope, id, props);

    const bucket = new Bucket(this, 'Uploads');
    const queue = new Queue(this, 'Jobs');

    const processor = new Function(this, 'Processor', {
      runtime: Runtime.NODEJS_20_X,
      handler: 'index.handler',
      code: Code.fromInline(
        `exports.handler = async (event) => ({ ok: true, received: event })`
      ),
    });

    processor.addEventSource(new SqsEventSource(queue));
    bucket.grantRead(processor);
  }
}
```

Deploy:

```sh
cdk deploy
```

Verify:

```sh
aws --endpoint-url http://localhost:4566 cloudformation describe-stacks
aws --endpoint-url http://localhost:4566 lambda list-functions
aws --endpoint-url http://localhost:4566 s3 ls
```

Trigger the flow:

```sh
echo "hi" | aws --endpoint-url http://localhost:4566 s3 cp - s3://$(aws --endpoint-url http://localhost:4566 s3api list-buckets --query 'Buckets[0].Name' --output text)/file.txt
aws --endpoint-url http://localhost:4566 logs tail /aws/lambda/MyStack-Processor --since 1m
```

Your Lambda code actually runs (fakecloud pulls the real Node 20 runtime container). The SQS -> Lambda event source mapping actually fires. CloudFormation actually provisioned the resources from the template CDK generated.

## CI integration

```yaml
jobs:
  cdk:
    runs-on: ubuntu-latest
    env:
      AWS_ENDPOINT_URL: http://localhost:4566
      AWS_ACCESS_KEY_ID: test
      AWS_SECRET_ACCESS_KEY: test
      AWS_REGION: us-east-1
      CDK_DEFAULT_ACCOUNT: '000000000000'
      CDK_DEFAULT_REGION: us-east-1
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-node@v4
        with: { node-version: 20 }

      - name: Start fakecloud
        run: |
          curl -fsSL https://fakecloud.dev/install.sh | bash
          fakecloud &
          for i in $(seq 1 30); do curl -sf http://localhost:4566/_fakecloud/health && break; sleep 1; done
          curl -sf http://localhost:4566/_fakecloud/health

      - run: npm ci
      - run: npx cdk bootstrap
      - run: npx cdk deploy --require-approval never
      - run: npm test
```

## Assertions from tests

CDK integration tests usually fall into two shapes:

**1. Synth assertions** — what CloudFormation does your CDK emit. These don't need fakecloud; use CDK's `@aws-cdk/assert` or `aws-cdk-lib/assertions`.

**2. Deployed-stack behavior** — does the stack actually work when deployed. For these, deploy to fakecloud and assert on real side effects:

```ts
import { FakeCloud } from 'fakecloud';
const fc = new FakeCloud();

test('upload triggers lambda', async () => {
  await uploadToBucket('uploads', 'test.txt', 'hello');

  const { invocations } = await fc.lambda.getInvocations({ functionName: 'Processor' });
  expect(invocations).toHaveLength(1);
});

afterEach(() => fc.reset());
```

fakecloud SDKs available in TypeScript, Python, Go, PHP, Java, Rust.

## Destroy between tests

CloudFormation state persists across tests unless you destroy or reset:

```sh
# Full stack teardown
cdk destroy --force

# OR reset fakecloud's entire state (faster)
curl -X POST http://localhost:4566/_fakecloud/reset
```

Reset is ~instant and covers every service. `cdk destroy` only unwinds one stack at a time.

## Why this works

fakecloud targets 100% behavioral conformance per implemented service. CloudFormation (90 operations), Lambda (85 ops, real code execution in 13 runtimes), SQS (23 ops), S3 (107 ops) — all in. Cross-service wiring like S3 -> Lambda event sources and SQS -> Lambda event source mappings actually execute server-side, not as stubs.

The depth-first goal: 100% of AWS services, each at 100% conformance, with 100% cross-service integrations. 23 services shipped today; more land one-at-a-time as they pass the conformance bar.

## Links

- Install: `curl -fsSL https://fakecloud.dev/install.sh | bash`
- Repo: [github.com/faiscadev/fakecloud](https://github.com/faiscadev/fakecloud)
- Terraform local dev guide: [Terraform local development for AWS](/blog/terraform-local-development-aws/)
- LocalStack migration guide: [Migrating from LocalStack to fakecloud](/blog/migrate-from-localstack/)
- Issues: [github.com/faiscadev/fakecloud/issues](https://github.com/faiscadev/fakecloud/issues)
