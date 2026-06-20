+++
title = "SNS"
description = "Topics, subscriptions, fan-out delivery, filter policies, platform applications."
weight = 3
+++

fakecloud implements **42 of 42** SNS operations at 100% Smithy conformance.

## Supported features

- **Topics** — CRUD, attributes, tags, policies
- **Subscriptions** — CRUD with confirmation flow, filter policies, raw message delivery
- **Fan-out delivery** — SQS, Lambda, HTTP/HTTPS subscriptions actually deliver
- **Filter policies** — JSON filter matching on message attributes and body
- **Platform applications** — iOS/Android/FCM/APNS endpoints (recorded for introspection, not sent)
- **Message deduplication** — via attribute
- **FIFO topics** — ordering, group IDs
- **Signed message delivery** — HTTP/HTTPS deliveries carry a real RSA-SHA256 `Signature` plus `SigningCertURL` pointing at the in-process cert server (`/_fakecloud/sns/cert.pem`), so subscribers running the standard `Sns::MessageVerifier`-style flow round-trip end-to-end. This covers both `Notification` (`SignatureVersion` 2) and `SubscriptionConfirmation` messages. The signed canonical string follows AWS's documented field order (`Message`, `MessageId`, `Subject`, `Timestamp`, `TopicArn`, `Type`, and `Token`/`SubscribeURL` for subscription confirmations) and `Signature` decodes to a valid signature over that string with the served public key.
- **Email subscriptions via SMTP relay** — when `FAKECLOUD_SNS_SMTP_RELAY=host:port` is set, `email` and `email-json` subscriptions deliver via SMTP to the configured relay (e.g. MailHog, Mailpit) so end-to-end SNS-to-email tests run without a real SES account. Unset, email subscriptions remain recorded-only at `/_fakecloud/sns/messages` to preserve the LocalStack-style hermetic default.

## Protocol

Query protocol. Form-encoded body, `Action` parameter, XML responses.

## Introspection

- `GET /_fakecloud/sns/messages` — list all published messages
- `GET /_fakecloud/sns/pending-confirmations` — list subscriptions pending confirmation
- `POST /_fakecloud/sns/confirm-subscription` — force-confirm an SNS subscription

## Cross-service delivery

- **SNS -> SQS / Lambda / HTTP** — Fan-out delivery to all subscription types

## Gotchas

- **SMS delivery is not real.** Messages to SMS endpoints are recorded for introspection at `/_fakecloud/sns/messages` but never actually sent. There's no SMS gateway.
- **Email delivery is opt-in.** By default, email and email-json subscriptions are recorded-only at `/_fakecloud/sns/messages`. Set `FAKECLOUD_SNS_SMTP_RELAY=host:port` to deliver to a real SMTP relay (MailHog, Mailpit, or production SES); without it nothing leaves the process. If you need full inbound/outbound email testing with parsing semantics, use SES instead.

## Source

- [`crates/fakecloud-sns`](https://github.com/faiscadev/fakecloud/tree/main/crates/fakecloud-sns)
- [AWS SNS API reference](https://docs.aws.amazon.com/sns/latest/api/Welcome.html)
