+++
title = "CloudWatch Logs"
description = "Log groups, streams, filtering, subscriptions, queries, anomaly detection."
weight = 11
+++

fakecloud implements **115 of 115** CloudWatch Logs operations at 100% Smithy conformance.

## Supported features

- **Log groups** — CRUD, retention (enforced — expired events purged on read/query), tags, KMS association, data protection
- **Log streams** — CRUD, log events, sequence token management
- **Log events** — PutLogEvents, GetLogEvents, FilterLogEvents (supports array-pattern syntax, e.g. `?ERROR ?WARN`, quoted phrases, `-exclude` terms)
- **GetLogRecord** — real pointer resolution: pointers minted by `GetQueryResults` / `FilterLogEvents` round-trip back to the original event with parsed `@message` fields
- **Field indexes** — `DescribeFieldIndexes` returns fields parsed from indexed log events, not stubs
- **GetLogGroupFields** — real field discovery from indexed events
- **ListLogGroupsForQuery** — real list filtered by query string
- **Subscription filters** — delivery to Lambda, Kinesis, SQS
- **Query language** — StartQuery, GetQueryResults with full Insights query syntax
- **Metric filters** — CRUD, extraction patterns
- **Resource policies** — CRUD
- **Export tasks** — S3 exports (recorded)
- **Destinations** — cross-account destinations
- **Anomaly detectors** — CRUD, training state, configuration; `ListAnomalies` / `UpdateAnomaly` operate on anomalies seeded via the admin endpoint below
- **Log deliveries** — CRUD plus delivery configuration and standard delivery templates
- **Live Tail** — real `StartLiveTail` streaming with filter-pattern matching against live `PutLogEvents`
- **GetLogObject / GetLogFields** — real responses backed by the indexed log store
- **Transformers** — log transformation configurations

## Protocol

JSON protocol. `X-Amz-Target` header, JSON body, JSON responses.

## Cross-service delivery

- **CloudWatch Logs -> Lambda / Kinesis / SQS** — Subscription filters deliver log events

## Admin / introspection

- `POST /_fakecloud/logs/anomalies/inject` — Seed a synthetic anomaly so tests can exercise `ListAnomalies` / `UpdateAnomaly` deterministically. Body:

  ```json
  {
    "anomalyDetectorArn": "arn:aws:logs:us-east-1:000000000000:anomaly-detector:my-detector",
    "patternString": "ERROR connection refused <*>",
    "logGroupArns": ["arn:aws:logs:us-east-1:000000000000:log-group:/app/web"],
    "priority": "HIGH"
  }
  ```

  Returns `{ "anomalyId": "<uuid>" }`. Available on every fakecloud SDK as `fc.logs().injectAnomaly(...)`.

- `GET /_fakecloud/logs/delivery-config` — Persisted CloudWatch Logs delivery configurations (the joined output of `PutDeliverySource` + `PutDeliveryDestination` + `CreateDelivery`). Each entry contains `id`, `name`, `deliveryDestinationArn`, `deliverySourceName`, the `logType` carried over from the source, plus `recordFields`, `fieldDelimiter`, `s3DeliveryConfiguration`, and `createdAt` (unix-ms). Available on every fakecloud SDK as `fc.logs().getDeliveryConfig()`.

- `GET /_fakecloud/logs/field-indexes/{logGroupName}` — Parsed `Fields` arrays from each `IndexPolicy` on a log group, plus `createdAt` and `lastUsedAt` timestamps. Returns `404` when the log group does not exist. Available on every fakecloud SDK as `fc.logs().getFieldIndexes(logGroupName)`.

## Gotchas

- Anomaly *detection* (pattern mining) does not run. Anomalies appear in `ListAnomalies` only after being seeded through the admin endpoint above.

## Source

- [`crates/fakecloud-logs`](https://github.com/faiscadev/fakecloud/tree/main/crates/fakecloud-logs)
- [AWS CloudWatch Logs API reference](https://docs.aws.amazon.com/AmazonCloudWatchLogs/latest/APIReference/Welcome.html)
