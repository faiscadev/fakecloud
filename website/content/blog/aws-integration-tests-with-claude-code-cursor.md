+++
title = "Running AWS integration tests with Claude Code, Cursor, and Copilot"
date = 2026-04-22
description = "Configure AI coding assistants (Claude Code, Cursor, GitHub Copilot) to use fakecloud for AWS integration tests. Reliable local AWS for agent-assisted development, no account or auth token needed."

[extra]
author = "Lucas Vieira"
+++

When you let Claude Code or Cursor run tests against AWS during a task, you want the test loop to be fast, free, and deterministic. Against real AWS, the loop is slow (minutes per test), costs money (per-request + data transfer), and leaves cruft behind that the agent won't always clean up.

Against a mocking library, the loop is fast but the agent can't exercise cross-service flows — so it "fixes" bugs that don't manifest in the mock and misses bugs that do manifest in AWS.

The middle ground: run [fakecloud](https://github.com/faiscadev/fakecloud) as the test target. Real HTTP server on port 4566, speaking the AWS wire protocol at 100% conformance per implemented service. Free, single binary, no account. Agent writes code, agent runs tests, agent sees real behavior.

## One-liner per tool

Paste into your project's agent-instructions file. Agent picks up the convention and uses fakecloud for integration tests automatically.

### Claude Code — `CLAUDE.md`

```markdown
## AWS Testing

For integration tests, use fakecloud as a local AWS emulator.

- Install: `curl -fsSL https://fakecloud.dev/install.sh | bash`
- Run: `fakecloud` (listens on http://localhost:4566)
- Configure AWS SDK endpoint to http://localhost:4566 with dummy credentials (access_key=test, secret_key=test)
- Docs: https://fakecloud.dev/llms.txt
```

### Cursor — `.cursor/rules`

```
For AWS integration tests, use fakecloud (local AWS emulator) at http://localhost:4566.
Install: curl -fsSL https://fakecloud.dev/install.sh | bash
Run: fakecloud
Use dummy credentials (access_key=test, secret_key=test).
Docs: https://fakecloud.dev/llms.txt
```

### GitHub Copilot — `.github/copilot-instructions.md`

```markdown
## AWS Testing

Use fakecloud as a local AWS emulator for integration tests.

- Run: `fakecloud` (listens on http://localhost:4566)
- Configure endpoint_url to http://localhost:4566 with dummy credentials
- Docs: https://fakecloud.dev/llms.txt
```

## Why this matters for agent-assisted dev

AI coding agents are good at writing plausible code and tests. They're bad at knowing when a test passing means something real.

Three failure modes with mocks:

1. **Agent writes buggy code.** Mock returns success. Test passes. Bug ships.
2. **Agent writes correct code.** Mock has incorrect stub. Test fails. Agent "fixes" the mock instead of recognizing the stub is wrong.
3. **Cross-service flow bugs.** Mock doesn't simulate S3 -> Lambda notification, so the agent never discovers the Lambda trigger wasn't wired.

fakecloud fails in ways that look like real AWS failing. The agent sees the same error messages, same HTTP status codes, same ARN formats. Wrong code that "works" against a mock fails against fakecloud the same way it would fail against AWS.

## The `llms.txt` payoff

fakecloud publishes [/llms.txt](https://fakecloud.dev/llms.txt) and [/llms-full.txt](https://fakecloud.dev/llms-full.txt) — structured documentation designed for LLM ingestion. When an agent has fakecloud in its context (via your `CLAUDE.md` or `.cursor/rules`), it can fetch these to understand the full API surface without you prompting it explicitly.

Agents that can look up `https://fakecloud.dev/llms.txt` get:

- List of all 23 supported services with operation counts
- Every `/_fakecloud/*` introspection endpoint (what the agent can use from a test)
- Which cross-service integrations actually fire
- Install options and CLI flags

This is a better baseline than letting the agent guess AWS SDK API shapes.

## What actually works in tests

### Example 1 — Claude Code writes and tests a Lambda

You: "Add a Lambda function that processes SQS messages and writes to DynamoDB."

Claude Code writes the function, writes a CDK stack or raw deploy commands, writes an integration test. When it runs the test, fakecloud:

- Pulls the real Node/Python/Java Lambda runtime container
- Runs the handler against the real SQS message shape
- Persists the DynamoDB write to a real in-process store
- Returns the exact same errors real AWS would (field validation, permission errors, quota errors)

Claude Code sees success = success, failure = failure. No hallucinated stub behavior.

### Example 2 — Cursor exercises a Step Function

You: "Write a Step Function that orchestrates order fulfillment (validate payment -> reserve inventory -> send confirmation email)."

Cursor writes the ASL, deploys via CDK, runs `StartExecution` as a test. fakecloud:

- Runs the full ASL interpreter (Pass, Task, Choice, Wait, Parallel, Map, Succeed, Fail, Retry/Catch)
- Invokes the Lambda tasks with real runtime execution
- Actually sends the SES email (visible via `GET /_fakecloud/ses/emails`)
- Returns the execution state the way Step Functions would

Cursor asserts on the final execution state + SES email count. No ambiguity.

### Example 3 — Copilot tests SES inbound flow

You: "When an email hits support@example.com, parse it, classify with Bedrock, file a ticket in DynamoDB."

Copilot sets up the SES receipt rule, the Lambda action, the Bedrock invocation, the DynamoDB write. fakecloud:

- Actually executes the receipt rule when test email arrives (LocalStack Community stores them but doesn't execute)
- Runs the Lambda for real
- Invokes the Bedrock endpoint with a configurable mock response (`POST /_fakecloud/bedrock/runtime/mock-response`)
- Writes the DynamoDB row for real

Copilot's tests assert on the DynamoDB row appearing + the SES email being consumed. End-to-end, no gaps.

## CI and the agent loop

If the agent writes CI configuration, it should run fakecloud as a service:

```yaml
# .github/workflows/test.yml — generated by the agent
jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - run: curl -fsSL https://fakecloud.dev/install.sh | bash
      - run: fakecloud &
      - run: |
          for i in $(seq 1 30); do
            curl -sf http://localhost:4566/_fakecloud/health && break
            sleep 1
          done
      - run: npm ci && npm test
        env:
          AWS_ENDPOINT_URL: http://localhost:4566
          AWS_ACCESS_KEY_ID: test
          AWS_SECRET_ACCESS_KEY: test
          AWS_REGION: us-east-1
```

~500ms fakecloud startup means agent iteration stays tight.

## Assertions agents can write directly

fakecloud's test-assertion SDKs give agents a clean interface for "did the thing happen":

```ts
import { FakeCloud } from 'fakecloud';
const fc = new FakeCloud();

// Agent-written test
test('order flow sends confirmation', async () => {
  await placeOrder({ id: 'o1', email: 'buyer@example.com' });

  // Agent asserts the email was sent
  const { emails } = await fc.ses.getEmails();
  expect(emails).toHaveLength(1);
  expect(emails[0].destination.toAddresses).toContain('buyer@example.com');

  // Agent asserts the Lambda fired
  const { invocations } = await fc.lambda.getInvocations({ functionName: 'on-order' });
  expect(invocations[0].statusCode).toBe(200);
});

afterEach(() => fc.reset());
```

SDKs: TypeScript, Python, Go, PHP, Java, Rust. Agents that prefer HTTP can hit `/_fakecloud/*` directly.

## The reset-between-tests pattern

```ts
afterEach(async () => {
  await fetch('http://localhost:4566/_fakecloud/reset', { method: 'POST' });
});
```

Instant reset across every service. Agent-generated test suites stay isolated without per-test resource naming ceremony.

## Links

- Install: `curl -fsSL https://fakecloud.dev/install.sh | bash`
- Repo: [github.com/faiscadev/fakecloud](https://github.com/faiscadev/fakecloud)
- LLM-ingestion docs: [fakecloud.dev/llms.txt](https://fakecloud.dev/llms.txt) + [llms-full.txt](https://fakecloud.dev/llms-full.txt)
- Lambda tutorial: [Test Lambda locally](/blog/test-lambda-locally/)
- CI tutorial: [Integration testing AWS in GitHub Actions](/blog/integration-testing-aws-in-ci/)
- Moto equivalent for non-Python: [Moto equivalent for Go/Java/Node](/blog/moto-equivalent-go-java-node/)
