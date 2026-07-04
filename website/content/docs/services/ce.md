+++
title = "AWS Cost Explorer"
description = "AWS Cost Explorer (ce) on fakecloud: a complete 47-operation implementation (100% conformance) — anomaly monitors + subscriptions, cost category definitions, cost-allocation tags, and structurally-valid zero-usage cost/reservation/savings analytics. awsJson1.1."
weight = 62
+++

fakecloud implements **AWS Cost Explorer** as an awsJson1.1 service (target
prefix `AWSInsightsIndexService`, sigv4 signing name `ce`). All **47
operations** ship with **100% conformance** against AWS's own Smithy model,
backed by account-partitioned state that persists across restarts in
persistent mode.

## Real, persisted configuration resources

The Cost Explorer resources you create and manage are fully real and
round-trip through create/read/update/delete against stored, account-scoped
state:

- **Anomaly monitors** — `CreateAnomalyMonitor` / `GetAnomalyMonitors` /
  `UpdateAnomalyMonitor` / `DeleteAnomalyMonitor`, with `GetAnomalies` and
  `ProvideAnomalyFeedback` operating on stored anomalies.
- **Anomaly subscriptions** — `CreateAnomalySubscription` /
  `GetAnomalySubscriptions` / `UpdateAnomalySubscription` /
  `DeleteAnomalySubscription`.
- **Cost category definitions** — `CreateCostCategoryDefinition` /
  `DescribeCostCategoryDefinition` / `UpdateCostCategoryDefinition` /
  `DeleteCostCategoryDefinition` / `ListCostCategoryDefinitions`, plus
  `ListCostCategoryResourceAssociations`.
- **Cost-allocation tags** — `ListCostAllocationTags`,
  `UpdateCostAllocationTagsStatus` (persists Active/Inactive),
  `StartCostAllocationTagBackfill` + `ListCostAllocationTagBackfillHistory`.
- **Analyses + recommendation generation** —
  `StartCommitmentPurchaseAnalysis` / `ListCommitmentPurchaseAnalyses` /
  `GetCommitmentPurchaseAnalysis`,
  `StartSavingsPlansPurchaseRecommendationGeneration` /
  `ListSavingsPlansPurchaseRecommendationGeneration` — each records the request
  with a generated id that settles to a terminal status.
- **Tagging** — `TagResource` / `UntagResource` / `ListTagsForResource` on the
  taggable ARNs.

## Cost analytics: honest zero-usage results

Cost Explorer's analytics operations report **billed AWS spend**. An emulator
incurs no real cost, so the faithful state is a fresh account with **zero
recorded usage** — the same way fakecloud doesn't run a billing engine any more
than it records its own CloudTrail events.

`GetCostAndUsage` (and `GetCostAndUsageWithResources` /
`GetCostAndUsageComparisons`), `GetCostForecast` / `GetUsageForecast`,
`GetDimensionValues` / `GetTags` / `GetCostCategories`, the reservation and
savings-plans coverage / utilization / recommendation ops, and
`GetRightsizingRecommendation` all return the model's **exact output shape** —
`ResultsByTime` bucketed per the requested `Granularity` and `TimePeriod`,
every `@required` aggregate (`Total`, utilization structs) present and
correctly typed — with zeroed metrics and empty result arrays. Requests are
validated (`@length` / `@range` / enum) so malformed input gets the same
`ValidationException` AWS returns.

## Persistence

All configuration state is account-partitioned and persisted across restarts
in persistent mode.
