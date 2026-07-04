+++
title = "CloudTrail"
description = "AWS CloudTrail (cloudtrail) control plane on fakecloud: trails, logging status, event and insight selectors, CloudTrail Lake event data stores, channels, imports, queries, dashboards, resource policies, organization delegated admins, and tagging — real account-partitioned, persisted state."
weight = 54
+++

fakecloud implements the **AWS CloudTrail** (`cloudtrail`) control plane. The
full **60-operation awsJson1.1 API** ships now: trails, event selectors and
insight selectors, CloudTrail Lake event data stores, channels, imports,
queries, dashboards, resource policies, organization delegated admins, event
configuration, and resource tagging. Every resource is real, account-partitioned
state that persists across restarts in persistent mode, so what one session
creates the next session still sees.

CloudTrail is a control-plane emulator: there is no real event-recording engine
— a fake needn't record its own API activity, and no conformance or Terraform
acceptance assertion depends on recorded events, the same way LocalStack
Community mocks CloudTrail. Everything up to and including the control plane that
manages trails, event data stores, and Lake resources is real.

## Supported features

- **Trails** (`CreateTrail`, `GetTrail`, `UpdateTrail`, `DeleteTrail`,
  `DescribeTrails`, `ListTrails`). New trails get an
  `arn:aws:cloudtrail:<region>:<acct>:trail/<name>` ARN. `S3BucketName`,
  `S3KeyPrefix`, `SnsTopicName` (resolved to `SnsTopicARN`), `HomeRegion`,
  CloudWatch Logs wiring, KMS key, and the multi-region / organization flags all
  round-trip. Trail names and trail ARNs are accepted interchangeably.
- **Logging status** (`GetTrailStatus`, `StartLogging`, `StopLogging`).
  `CreateTrail` leaves a trail with logging **off** (`IsLogging: false`) until
  `StartLogging`, matching AWS. `StartLogging`/`StopLogging` toggle the per-trail
  flag that `GetTrailStatus` reflects, along with `StartLoggingTime` /
  `StopLoggingTime`.
- **Event selectors** (`GetEventSelectors`, `PutEventSelectors`). Both the
  classic `EventSelectors` and `AdvancedEventSelectors` forms persist and
  round-trip; setting them flips the trail's `HasCustomEventSelectors`.
- **Insight selectors** (`GetInsightSelectors`, `PutInsightSelectors`), keyed on
  either a trail or an event data store.
- **CloudTrail Lake event data stores** (`CreateEventDataStore`,
  `GetEventDataStore`, `UpdateEventDataStore`, `DeleteEventDataStore`,
  `ListEventDataStores`, `RestoreEventDataStore`). Stores settle to `ENABLED`
  synchronously; `DeleteEventDataStore` moves a store to `PENDING_DELETION` and
  `RestoreEventDataStore` brings it back to `ENABLED` within the restore window.
  Termination protection is enforced.
- **Ingestion & federation** (`StartEventDataStoreIngestion`,
  `StopEventDataStoreIngestion`, `EnableFederation`, `DisableFederation`) toggle
  the store's ingestion `Status` and federation state.
- **Channels** (`CreateChannel`, `GetChannel`, `UpdateChannel`,
  `DeleteChannel`, `ListChannels`) — full CRUD with real round-trip of source
  and destinations.
- **Imports** (`StartImport`, `StopImport`, `GetImport`, `ListImports`,
  `ListImportFailures`). A supplied `ImportId` restarts an existing import;
  otherwise a new import is created.
- **Lake queries** (`StartQuery`, `DescribeQuery`, `GetQueryResults`,
  `CancelQuery`, `ListQueries`, `GenerateQuery`, `SearchSampleQueries`). Queries
  settle to `FINISHED` synchronously with empty result rows and zero statistics;
  `CancelQuery` marks a query `CANCELLED`.
- **Dashboards** (`CreateDashboard`, `GetDashboard`, `UpdateDashboard`,
  `DeleteDashboard`, `ListDashboards`, `StartDashboardRefresh`).
- **Resource policies** (`PutResourcePolicy`, `GetResourcePolicy`,
  `DeleteResourcePolicy`), keyed by `ResourceArn`.
- **Organization delegated admins** (`RegisterOrganizationDelegatedAdmin`,
  `DeregisterOrganizationDelegatedAdmin`).
- **Event configuration** (`GetEventConfiguration`, `PutEventConfiguration`).
- **Tagging** (`AddTags`, `RemoveTags`, `ListTags`).
- **Read-only lookups** (`LookupEvents`, `ListPublicKeys`,
  `ListInsightsMetricData`, `ListInsightsData`) return real, empty result sets —
  a fake records no activity of its own.

## Not implemented

There is no event-recording engine: `LookupEvents` and CloudTrail Lake query
results are always empty because fakecloud does not journal its own API calls to
an S3 bucket or an event data store. Digest-file delivery, log-file integrity
validation signing, and real S3/SNS/CloudWatch Logs delivery side effects are
out of scope — the configuration for them round-trips, but no files are written.

## Model-derived validation

Constrained top-level input members (string `@length`, integer `@range`, and
`enum` value sets) are validated against the AWS Smithy model, returning
`InvalidParameterException` on a violation, matching the real service's
client-side validation.

## Persistence

All CloudTrail state is account-partitioned and, in persistent mode, saved to a
snapshot on every mutation and restored on startup.
