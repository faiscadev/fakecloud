+++
title = "EventBridge"
description = "Event buses, pattern matching, scheduled rules, archives, replay, API destinations."
weight = 4
+++

fakecloud implements **57 of 57** EventBridge operations at 100% Smithy conformance.

## Supported features

- **Event buses** — default, custom, partner event sources
- **Rules** — pattern matching on events, scheduled rules (cron and rate)
- **Targets** — SNS, SQS, Lambda, CloudWatch Logs, Kinesis, Step Functions, HTTP, API Destinations
- **Archives** — event archiving with retention
- **Replay** — re-send archived events to targets
- **Connections** — API connection management for HTTP targets
- **API destinations** — outbound HTTP integrations
- **Pattern matching**: full EventBridge pattern language including prefix, suffix, equals-ignore-case, wildcard, cidr (IPv4 and IPv6), numeric comparisons, exists, anything-but and `$or`, with AWS array semantics (an array of objects matches when any element matches; every matcher except `exists: false` requires the field to be present)

## Protocol

JSON protocol. `X-Amz-Target` header, JSON body, JSON responses.

## Introspection

- `GET /_fakecloud/events/history` — list all events and deliveries
- `POST /_fakecloud/events/fire-rule` — fire a specific rule manually. Body: `{"busName": "...", "ruleName": "..."}`

## Cross-service delivery

- **EventBridge -> SNS / SQS / Lambda / Logs / Kinesis / Step Functions / HTTP** — Rules deliver to targets on schedule or event match
- **EventBridge Scheduler** — Cron and rate-based rules fire on schedule

## Source

- [`crates/fakecloud-eventbridge`](https://github.com/faiscadev/fakecloud/tree/main/crates/fakecloud-eventbridge)
- [AWS EventBridge API reference](https://docs.aws.amazon.com/eventbridge/latest/APIReference/Welcome.html)
