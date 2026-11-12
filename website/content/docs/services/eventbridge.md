+++
title = "EventBridge"
description = "Event buses, pattern matching, scheduled rules, archives, replay, API destinations."
weight = 4
+++

fakecloud implements **57 of 57** EventBridge operations at 100% Smithy conformance.

## Supported features

- **Event buses** — default, custom, partner event sources
- **Rules** — pattern matching on events, scheduled rules (cron and rate)
- **Targets** — SNS, SQS, Lambda, CloudWatch Logs, Kinesis, Step Functions, HTTP, API Destinations; an SQS target that refuses the event (missing queue, KMS key refusing `events.amazonaws.com`) sends it to the target's `DeadLetterConfig` queue with `ERROR_CODE`, `ERROR_MESSAGE`, `RULE_ARN` and `TARGET_ARN` message attributes
- **Metrics** — each rule reports `AWS/Events` metrics to CloudWatch, dimensioned by `RuleName` (and `EventBusName` on a custom bus): `Invocations` counts every invocation of every target type, successful or failed (replays included), and an SQS target that refuses the event also counts `FailedInvocations` and `InvocationsSentToDlq` / `InvocationsFailedToBeSentToDlq`
- **Archives** — event archiving with retention
- **Replay** — re-send archived events to every target type, each through the rule that matches it and the same delivery PutEvents uses; `Destination.FilterArns` limits the replay to the rules it names (as the sender SQS sees, in `<aws.events.rule-arn>` and a dead-letter message's `RULE_ARN`)
- **Connections** — API connection management for HTTP targets
- **API destinations** — outbound HTTP integrations
- **Pattern matching**: full EventBridge pattern language including prefix, suffix, equals-ignore-case, wildcard, cidr (IPv4 and IPv6), numeric comparisons, exists, anything-but and `$or`, with AWS array semantics (an array of objects matches when any element matches; every matcher except `exists: false` requires the field to be present)

## Protocol

JSON protocol. `X-Amz-Target` header, JSON body, JSON responses.

## Introspection

- `GET /_fakecloud/events/history` — list all events (with `detail` as structured JSON) and the payloads delivered to Lambda and CloudWatch Logs targets
- `POST /_fakecloud/events/fire-rule` — fire a specific rule manually. Body: `{"busName": "...", "ruleName": "..."}`

## Cross-service delivery

- **EventBridge -> SNS / SQS / Lambda / Logs / Kinesis / Step Functions / HTTP** — Rules deliver to targets on schedule or event match
- **EventBridge Scheduler** — Cron and rate-based rules fire on schedule
- **Service events** (S3, ECS, RDS, SES, Lambda destinations, Step Functions, Pipes, Scheduler): each event lands on a bus in the account that owns the emitting resource (or the account of a target bus ARN), carries that resource's `account` and `region`, and is matched by rules and captured by archives exactly like a `PutEvents` entry. A bus in another account takes the event only when its resource policy allows the source's role (or account) to `events:PutEvents`, the same rule `PutEvents` applies to cross-account callers (a bus without a policy accepts only its own account); a refused event is not stored and the source records a delivery failure (Step Functions fails the task with `EventBridge.FailedEntry`, Scheduler and Pipes retry / dead-letter).

## Source

- [`crates/fakecloud-eventbridge`](https://github.com/faiscadev/fakecloud/tree/main/crates/fakecloud-eventbridge)
- [AWS EventBridge API reference](https://docs.aws.amazon.com/eventbridge/latest/APIReference/Welcome.html)
