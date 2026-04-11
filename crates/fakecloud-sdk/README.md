# fakecloud-sdk

Rust client SDK for [fakecloud](https://github.com/faiscadev/fakecloud), a local AWS cloud emulator.

The crate wraps fakecloud's introspection and simulation API (`/_fakecloud/*`) so Rust tests can inspect emulator state, reset services, and trigger time-based processors without going through raw HTTP calls.

## Installation

```bash
cargo add fakecloud-sdk
```

## Quick Start

```rust
use fakecloud_sdk::FakeCloud;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let fc = FakeCloud::new("http://localhost:4566");

    let health = fc.health().await?;
    println!("{}", health.version);

    fc.reset().await?;

    let emails = fc.ses().get_emails().await?;
    println!("sent {} emails", emails.emails.len());

    Ok(())
}
```

## What It Covers

- health and reset endpoints
- SES email inspection and inbound simulation
- SNS and SQS message inspection
- EventBridge history and manual rule firing
- S3 notifications and lifecycle ticks
- DynamoDB TTL and Secrets Manager rotation ticks
- Lambda invocation and warm-container inspection
- Cognito confirmation codes, token inspection, and auth event access
- RDS instance inspection with runtime metadata
- ElastiCache cluster, replication group, and serverless cache inspection
- Step Functions execution history
- API Gateway v2 HTTP API request history

## Repository

- Project: <https://github.com/faiscadev/fakecloud>
- Website: <https://fakecloud.dev>
