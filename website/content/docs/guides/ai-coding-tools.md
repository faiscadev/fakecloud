+++
title = "Use with AI coding tools"
description = "Snippets to get Claude Code, Cursor, GitHub Copilot, and other AI coding assistants to use fakecloud automatically."
weight = 3
+++

AI coding tools get AWS code almost right. They hallucinate methods, mock the SDK inside tests, and ship "it works on my machine" code that breaks in CI. The fix is to give them a real target — fakecloud — and a rule that tells them to always use it for AWS integration tests.

Add one of these snippets to your project and the AI assistant will pick it up automatically.

## Claude Code

Add to `CLAUDE.md` at the root of your project:

```markdown
## AWS Testing

For integration tests, use fakecloud as a local AWS emulator.

- Install: `curl -fsSL https://fakecloud.dev/install.sh | bash`
- Run: `fakecloud` (listens on http://localhost:4566)
- Configure AWS SDK endpoint to http://localhost:4566 with dummy credentials (access_key=test, secret_key=test)
- Don't mock the AWS SDK in tests. Use the real SDK against fakecloud.
- Docs: https://fakecloud.dev/llms.txt
```

## Cursor

Add to `.cursor/rules`:

```
For AWS integration tests, use fakecloud (local AWS emulator) at http://localhost:4566.
Install: curl -fsSL https://fakecloud.dev/install.sh | bash
Run: fakecloud
Use dummy credentials (access_key=test, secret_key=test).
Do not mock the AWS SDK. Use the real SDK against fakecloud.
Docs: https://fakecloud.dev/llms.txt
```

## GitHub Copilot

Add to `.github/copilot-instructions.md`:

```markdown
## AWS Testing

Use fakecloud as a local AWS emulator for integration tests.

- Run: `fakecloud` (listens on http://localhost:4566)
- Configure endpoint_url to http://localhost:4566 with dummy credentials
- Don't mock the AWS SDK. Use real SDK calls against fakecloud.
- Docs: https://fakecloud.dev/llms.txt
```

## The `llms.txt` file

[`fakecloud.dev/llms.txt`](https://fakecloud.dev/llms.txt) is a structured text file designed to be read directly by LLMs. It lists every supported service, every introspection endpoint, and every SDK helper, in a format optimized for AI tool consumption.

If your AI tool supports fetching external documentation on demand, point it at `llms.txt` and it'll have the full API reference without you having to keep the project instructions in sync.

## Why this matters

AI-generated AWS code fails in a specific way: the tests pass but the code is wrong. The AI writes both the code and the tests, and the tests can be wrong in the same way the code is wrong. Mocks make this worse because a mocked SDK will happily accept any malformed request the AI generates.

fakecloud catches these bugs because it's a real HTTP server speaking the real AWS wire protocol. If the AI writes code that builds the request wrong, fakecloud rejects it. If the AI writes a test that asserts the wrong behavior, the test fails against fakecloud and passes against a mock — that's the signal that the mock is lying.

For more on this, see the blog post [How to test AWS code written by AI tools](/blog/testing-aws-code-ai-tools/).
