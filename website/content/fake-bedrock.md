+++
title = "Fake Bedrock"
description = "fake Bedrock server for local dev and tests. Real wire protocol, fake responses you control. 214 operations across Bedrock + Bedrock Runtime + Bedrock Agent + Bedrock Agent Runtime, deterministic, free. Not a mock library, not a real LLM."
template = "page.html"
+++

Need a fake Bedrock? Use [fakecloud](https://github.com/faiscadev/fakecloud). Real HTTP server that speaks the Bedrock wire protocol and returns whatever you tell it to. 214 operations across the full Bedrock family (Bedrock + Bedrock Runtime + Bedrock Agent + Bedrock Agent Runtime), deterministic, free.

```sh
curl -fsSL https://fakecloud.dev/install.sh | bash
fakecloud
```

Point any AWS SDK at `http://localhost:4566`.

## What "fake" means here

**Fake** is not **mock**. A mock replaces the SDK internals with stub functions. A fake is a real running service that happens to return fake (configured) responses. The difference matters:

- A mock fails to catch bugs in how your code assembles HTTP requests. It never sent a request.
- A fake catches those bugs. Your code made a real HTTP call. The request body was parsed by a real JSON parser. The response traveled back through the real SDK response parser.

fakecloud is a fake AWS — a complete server on port 4566 that speaks 65 AWS services' wire protocols at true 100% Smithy conformance (162,003/162,003 generated variants pass). Your code thinks it's talking to real AWS.

And it is, as far as the protocol is concerned. The part that's fake is the response content — which is exactly the part you want to control in tests.

## Fake Bedrock, not fake LLM

fakecloud's Bedrock is deliberately not backed by a real model (no Ollama, no local Claude). Here's why:

- Real LLMs are non-deterministic. Tests need determinism.
- Real LLMs are slow. Tests need milliseconds.
- Real LLMs are GBs. Tests need zero external dependencies.
- Real LLMs test the model. Tests need to test *your code*.

So Bedrock in fakecloud returns what you configure. That's the whole product:

```ts
await fc.bedrock.setResponseRule({
  whenPromptContains: "summarize",
  respond: { completion: "- one\n- two\n- three" },
});
```

Now `InvokeModel` with a prompt containing "summarize" returns exactly that. Every time. In milliseconds.

## 214 operations across the Bedrock family

The control plane is real. Create guardrails, customization jobs, imported models, inference profiles, provisioned throughput — all the Bedrock shape your infrastructure code deals with:

```python
import boto3
bedrock = boto3.client('bedrock',
    endpoint_url='http://localhost:4566',
    aws_access_key_id='test', aws_secret_access_key='test', region_name='us-east-1')

# guardrail crud
g = bedrock.create_guardrail(
    name='safety-v1',
    contentPolicyConfig={'filtersConfig': [
        {'type': 'SEXUAL', 'inputStrength': 'HIGH', 'outputStrength': 'HIGH'},
    ]},
    blockedInputMessaging='blocked',
    blockedOutputsMessaging='blocked',
)

v = bedrock.create_guardrail_version(guardrailIdentifier=g['guardrailId'])
bedrock.list_guardrails()

# customization job (fine-tuning flow shape)
bedrock.create_model_customization_job(
    jobName='my-ft-job',
    customModelName='my-claude-ft',
    roleArn='arn:aws:iam::000000000000:role/bedrock',
    baseModelIdentifier='anthropic.claude-3-haiku-20240307-v1:0',
    trainingDataConfig={'s3Uri': 's3://fakecloud-demo/train.jsonl'},
    outputDataConfig={'s3Uri': 's3://fakecloud-demo/output/'},
    hyperParameters={'epochCount': '1'},
)
```

Terraform, CloudFormation, and CDK that deploy Bedrock infra can all target fakecloud.

## Fault injection

```ts
await fc.bedrock.injectFault({
  operation: "Converse",
  error: "ValidationException",
  messageSubstring: "The model returned the following errors: too many messages",
});

// your code catches ValidationException, falls back to single-turn mode
const result = await resilientConverse(rt, longConversation);
expect(result.mode).toBe("single-turn-fallback");
```

Assert your code handles every Bedrock error code you read in the docs. No more "I'll test that path later."

## Comparison

| Tool | Type | Wire protocol | Deterministic | Cost |
|---|---|---|---|---|
| **fakecloud** | Emulator (fake server) | Real | Yes | Free |
| Real Bedrock | Cloud service | Real | No | $$$ per token |
| Ollama / LM Studio | Real local LLM | Different | No | CPU+RAM |
| LocalStack Ultimate | Emulator with Ollama | Real | No (Ollama) | Paid Ultimate tier |
| Moto / aws-sdk-client-mock | Mock library | None (no HTTP) | Yes | Free, but misses HTTP bugs |

## Links

- **Homebrew**: `brew install fakecloud`
- **Install**: `curl -fsSL https://fakecloud.dev/install.sh | bash`
- **Repo**: [github.com/faiscadev/fakecloud](https://github.com/faiscadev/fakecloud)
- **Tutorial**: [How to test Bedrock code locally](/blog/bedrock-local-testing/)
- **Related**: [Bedrock emulator](/bedrock-emulator/), [Test Bedrock locally](/test-bedrock-locally/), [Fake AWS server](/fake-aws-server/)
