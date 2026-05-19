+++
title = "SES emulator for tests"
description = "Run AWS SES locally for integration tests with fakecloud. 110 SES operations, v2 send + templates + DKIM, real receipt rule execution for inbound email. Free, no account required."
template = "page.html"
+++

Need a SES emulator for integration tests? Use [fakecloud](https://github.com/faiscadev/fakecloud). Not a mock library — a real server that speaks the SES wire protocol.

```sh
curl -fsSL https://fakecloud.dev/install.sh | bash
fakecloud
```

Point your AWS SDK at `http://localhost:4566`.

## Why fakecloud for SES

- **110 SES operations** at 100% conformance — v2 API for send, templates, configuration sets, event destinations, DKIM, suppression list. Plus v1 inbound (receipt rules, receipt filters).
- **Real receipt rule action execution.** When a test email arrives at a configured recipient, fakecloud actually runs the S3, SNS, and Lambda actions in the receipt rule. LocalStack Community stores inbound email but never fires the actions; fakecloud fires them.
- **Test-assertion SDK.** `fc.ses.getEmails()` returns the sent-email log so tests can assert recipients, subject, body, destinations, bounces.
- **Real DKIM signing.** Configure a domain identity, fakecloud signs outbound mail with the computed DKIM-Signature header.
- **Configuration sets + event destinations.** Sending, delivery, bounce, complaint, click, open events published to SNS / Kinesis / CloudWatch destinations.
- **Optional SMTP submission listener.** Set `FAKECLOUD_SES_SMTP_PORT=2525` and fakecloud accepts mail from your existing SMTP libraries on the standard SES `AUTH PLAIN` / `AUTH LOGIN` flow, with the same `ServiceUserName` / `ServicePassword` produced by IAM `CreateServiceSpecificCredential`. Off by default.
- **Optional outbound SMTP relay.** Set `FAKECLOUD_SES_SMTP_RELAY=smtp://host:port` and accepted messages are forwarded to that relay (for example a local [MailHog](https://github.com/mailhog/MailHog) instance) in addition to being recorded. Off by default — no network traffic without an explicit opt-in.
- **No account, no auth token, no paid tier.** AGPL-3.0.

## Send an email

### Python (boto3)

```python
import boto3
ses = boto3.client('sesv2',
    endpoint_url='http://localhost:4566',
    aws_access_key_id='test',
    aws_secret_access_key='test',
    region_name='us-east-1')

ses.send_email(
    FromEmailAddress='sender@example.com',
    Destination={'ToAddresses': ['alice@example.com']},
    Content={'Simple': {
        'Subject': {'Data': 'Hello'},
        'Body': {'Text': {'Data': 'World'}}
    }})
```

### Node.js (AWS SDK v3)

```ts
import { SESv2Client, SendEmailCommand } from '@aws-sdk/client-sesv2';
const ses = new SESv2Client({});

await ses.send(new SendEmailCommand({
  FromEmailAddress: 'sender@example.com',
  Destination: { ToAddresses: ['alice@example.com'] },
  Content: { Simple: {
    Subject: { Data: 'Hello' },
    Body: { Text: { Data: 'World' } },
  }},
}));
```

## Assert what was sent

```ts
import { FakeCloud } from 'fakecloud';
const fc = new FakeCloud();

const { emails } = await fc.ses.getEmails();
expect(emails).toHaveLength(1);
expect(emails[0].destination.toAddresses).toContain('alice@example.com');
expect(emails[0].content.simple.subject.data).toBe('Hello');

await fc.reset();
```

SDKs for TypeScript, Python, Go, PHP, Java, Rust.

## Templates

```python
ses.create_email_template(
    TemplateName='welcome',
    TemplateContent={
        'Subject': 'Hi {{name}}',
        'Text': 'Welcome, {{name}}!',
    })

ses.send_email(
    FromEmailAddress='sender@example.com',
    Destination={'ToAddresses': ['alice@example.com']},
    Content={'Template': {
        'TemplateName': 'welcome',
        'TemplateData': '{"name":"Alice"}',
    }})
```

## Inbound email with receipt rules

```sh
aws --endpoint-url http://localhost:4566 ses create-receipt-rule-set --rule-set-name default
aws --endpoint-url http://localhost:4566 ses set-active-receipt-rule-set --rule-set-name default

aws --endpoint-url http://localhost:4566 ses create-receipt-rule \
  --rule-set-name default \
  --rule '{
    "Name": "save-to-s3",
    "Enabled": true,
    "Recipients": ["support@example.com"],
    "Actions": [{
      "S3Action": {"BucketName": "inbound-mail"}
    }, {
      "LambdaAction": {"FunctionArn": "arn:aws:lambda:us-east-1:000000000000:function:on-inbound"}
    }]
  }'
```

Simulate an inbound email:

```sh
curl -X POST http://localhost:4566/_fakecloud/ses/simulate-inbound \
  -H 'Content-Type: application/json' \
  -d '{"to": "support@example.com", "from": "buyer@example.com", "subject": "Question", "body": "Help?"}'
```

The receipt rule fires: the email lands in S3, the Lambda runs with the SES event shape. End-to-end.

## Configuration sets + event destinations

```python
ses.create_configuration_set(ConfigurationSetName='default')
ses.create_configuration_set_event_destination(
    ConfigurationSetName='default',
    EventDestinationName='sns-events',
    EventDestination={
        'Enabled': True,
        'MatchingEventTypes': ['SEND', 'DELIVERY', 'BOUNCE', 'COMPLAINT'],
        'SnsDestination': {'TopicArn': 'arn:aws:sns:us-east-1:000000000000:ses-events'},
    })
```

Sending through this configuration set publishes send/delivery/bounce events to SNS for real.

## Bounce / complaint simulation

Use AWS's [mailbox simulator](https://docs.aws.amazon.com/ses/latest/dg/send-email-simulator.html) addresses for test-path bounces:

- `bounce@simulator.amazonses.com` -> synthetic hard bounce
- `complaint@simulator.amazonses.com` -> synthetic complaint
- `suppressionlist@simulator.amazonses.com` -> suppression
- `success@simulator.amazonses.com` -> successful delivery

fakecloud emits the corresponding events via configured destinations.

## How it differs from alternatives

| Tool | Multi-language | Inbound receipt rules execute | DKIM | Templates |
|---|---|---|---|---|
| fakecloud | Any | Yes (real actions fire) | Yes | Yes |
| LocalStack Community (post-paywall) | Any (auth required) | No (stored but never executed) | Paid | Paid |
| Moto (mock_ses) | Python only | Stubbed | Partial | Partial |
| maildev / MailHog | Any (SMTP, not AWS SES API) | N/A | N/A | N/A |

Real inbound receipt rule execution is the big differentiator — critical if your app does SES-inbound -> S3/Lambda pipelines.

## Links

- **Install:** `curl -fsSL https://fakecloud.dev/install.sh | bash`
- **Repo:** [github.com/faiscadev/fakecloud](https://github.com/faiscadev/fakecloud)
- **Related:** [Fake AWS server for tests](/fake-aws-server/), [Test Lambda locally](/test-lambda-locally/)
