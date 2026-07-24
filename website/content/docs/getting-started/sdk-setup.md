+++
title = "SDK setup"
description = "Install the first-party fakecloud SDK in TypeScript, Python, Go, PHP, Java, Rust, or C#/.NET."
weight = 3
+++

fakecloud ships first-party SDKs for test assertions in seven languages. They wrap the `/_fakecloud/*` introspection and configuration endpoints into ergonomic helpers.

These SDKs are **not** the AWS SDK. Your application code still uses the normal AWS SDK (boto3, aws-sdk-js, etc.) to talk to fakecloud over the standard AWS wire protocol. The fakecloud SDK is what your tests use to assert on what happened and configure simulation behavior.

## TypeScript

```sh
npm install fakecloud
```

```typescript
import { FakeCloud } from "fakecloud";

const fc = new FakeCloud(); // defaults to http://localhost:4566

await fc.reset();
const { emails } = await fc.ses.getEmails();
```

## Python

```sh
pip install fakecloud
```

```python
from fakecloud import FakeCloud

fc = FakeCloud()

fc.reset()
emails = fc.ses.get_emails()
```

Async variant:

```python
from fakecloud import AsyncFakeCloud

async with AsyncFakeCloud() as fc:
    await fc.reset()
    emails = await fc.ses.get_emails()
```

## Go

```sh
go get github.com/faiscadev/fakecloud/sdks/go
```

```go
import "github.com/faiscadev/fakecloud/sdks/go/fakecloud"

fc := fakecloud.New("http://localhost:4566")
fc.Reset()
emails, _ := fc.SES.GetEmails()
```

## PHP

```sh
composer require fakecloud/fakecloud
```

```php
use FakeCloud\FakeCloud;

$fc = new FakeCloud(); // defaults to http://localhost:4566

$fc->reset();
$emails = $fc->ses()->getEmails()->emails;
```

## Java

```kotlin
// build.gradle.kts
testImplementation("dev.fakecloud:fakecloud:0.12.0")
```

```java
import dev.fakecloud.FakeCloud;

FakeCloud fc = new FakeCloud(); // defaults to http://localhost:4566
fc.reset();
var emails = fc.ses().getEmails().emails();
```

## Rust

```sh
cargo add fakecloud-sdk
```

```rust
use fakecloud_sdk::FakeCloudClient;

let fc = FakeCloudClient::new("http://localhost:4566");
fc.reset().await?;
let invocations = fc.bedrock().get_invocations().await?;
```

## C# / .NET

```sh
dotnet add package FakeCloud
```

```csharp
using FakeCloud;

var fc = new FakeCloudClient(); // defaults to http://localhost:4566
await fc.ResetAsync();
var emails = (await fc.Ses.GetEmailsAsync()).Emails;
```

## What each SDK covers

All seven SDKs wrap the same core surface:

- **Reset:** `reset()` / `reset(service)` — clear state between tests
- **Per-service introspection:** getters for recorded messages, emails, invocations, etc.
- **Simulation:** configure Bedrock response rules, inject faults, tick time-based processors (TTL, rotation, lifecycle)
- **Health:** ping fakecloud to verify it's reachable

For a full method list per SDK, see the README in each SDK's directory on [GitHub](https://github.com/faiscadev/fakecloud/tree/main/sdks).
