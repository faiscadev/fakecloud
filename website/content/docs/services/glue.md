+++
title = "Glue"
description = "AWS Glue full control plane — Data Catalog, jobs, crawlers, connections, triggers, workflows, schema registry, sessions, ML transforms, data quality. JSON 1.1 protocol."
weight = 31
+++

fakecloud implements **all 267** AWS Glue operations at 100% Smithy conformance. The entire control plane is real and persisted — Data Catalog, jobs, crawlers, classifiers, connections, triggers, workflows, blueprints, dev endpoints, the schema registry, interactive sessions, ML transforms, data quality, user-defined functions, usage profiles, column statistics, and tagging. The Data Catalog is the same store Athena reads through: tables created here surface immediately under `AwsDataCatalog` for Athena's `ListDatabases` / `GetTableMetadata` paths.

**Status: full control-plane parity. fakecloud is not a Spark engine — job, crawler, ML, and data-quality *execution* is synthesized (runs transition through real status lifecycles), but every resource is created, persisted, and echoed back exactly as registered.**

## Supported today

- **Data Catalog — Databases** — `CreateDatabase` / `GetDatabase` / `GetDatabases` / `UpdateDatabase` / `DeleteDatabase`. Per-account namespace, name-uniqueness enforcement, parameter passthrough.
- **Data Catalog — Tables** — `CreateTable` / `GetTable` / `GetTables` / `UpdateTable` / `DeleteTable` / `BatchDeleteTable`, plus `GetTableVersion` / `GetTableVersions` / `DeleteTableVersion` / `BatchDeleteTableVersion` and `GetUnfilteredTableMetadata`. Full `StorageDescriptor` round-trip (columns + types, location, input/output format, SerDe, partition keys, parameters, view text, table type).
- **Data Catalog — Partitions** — `CreatePartition` / `BatchCreatePartition` / `GetPartition` / `GetPartitions` / `BatchGetPartition` / `UpdatePartition` / `BatchUpdatePartition` / `DeletePartition` / `BatchDeletePartition`, partition indexes (`CreatePartitionIndex` / `GetPartitionIndexes` / `DeletePartitionIndex`), and `GetUnfilteredPartitionMetadata` / `GetUnfilteredPartitionsMetadata`.
  - **`GetPartitions` Expression pruning** — the `Expression` filter is parsed and evaluated server-side. Supports `=` / `!=` / `<>` / `<` / `<=` / `>` / `>=` / `IN` / `BETWEEN` / `LIKE` / `IS NULL` / `IS NOT NULL`, with `AND` / `OR` / `NOT` and parentheses; type-aware over `string` / `int` / `bigint` / `date` / `timestamp`. Unparseable expressions return `InvalidInputException`.
- **Data Catalog — Catalogs & settings** — `CreateCatalog` / `GetCatalog` / `GetCatalogs` / `UpdateCatalog` / `DeleteCatalog`, `GetDataCatalogEncryptionSettings` / `PutDataCatalogEncryptionSettings`, `GetResourcePolicy` / `PutResourcePolicy` / `DeleteResourcePolicy` / `GetResourcePolicies`, and `BatchGetCustomEntityTypes` / `CreateCustomEntityType` / `GetCustomEntityType` / `ListCustomEntityTypes` / `DeleteCustomEntityType`.
- **Jobs & job runs** — `CreateJob` / `GetJob` / `GetJobs` / `ListJobs` / `UpdateJob` / `DeleteJob` / `BatchGetJobs`, `StartJobRun` / `GetJobRun` / `GetJobRuns` / `BatchStopJobRun`, job bookmarks (`GetJobBookmark` / `ResetJobBookmark`), and `UpdateSourceControlFromJob` / `UpdateJobFromSourceControl`.
- **Crawlers** — `CreateCrawler` / `GetCrawler` / `GetCrawlers` / `ListCrawlers` / `UpdateCrawler` / `DeleteCrawler` / `BatchGetCrawlers`, `StartCrawler` / `StopCrawler`, crawler schedule (`UpdateCrawlerSchedule` / `StartCrawlerSchedule` / `StopCrawlerSchedule`), and `GetCrawlerMetrics`. Real `READY` ↔ `RUNNING` transitions — a double `StartCrawler` returns `CrawlerRunningException`, an idle `StopCrawler` returns `CrawlerNotRunningException`.
- **Classifiers** — `CreateClassifier` / `GetClassifier` / `GetClassifiers` / `UpdateClassifier` / `DeleteClassifier` (Grok / XML / JSON / CSV).
- **Connections** — `CreateConnection` / `GetConnection` / `GetConnections` / `UpdateConnection` / `DeleteConnection` / `BatchDeleteConnection`, plus `TestConnection`, `GetConnectionType` / `ListConnectionTypes`, and the connection-integration ops.
- **Triggers** — `CreateTrigger` / `GetTrigger` / `GetTriggers` / `ListTriggers` / `UpdateTrigger` / `DeleteTrigger` / `BatchGetTriggers`, `StartTrigger` / `StopTrigger`. Triggers move through `CREATED` / `ACTIVATED` / `DEACTIVATED`.
- **Workflows** — `CreateWorkflow` / `GetWorkflow` / `ListWorkflows` / `UpdateWorkflow` / `DeleteWorkflow` / `BatchGetWorkflows`, runs (`StartWorkflowRun` / `GetWorkflowRun` / `GetWorkflowRuns` / `StopWorkflowRun` / `ResumeWorkflowRun`), and run properties (`GetWorkflowRunProperties` / `PutWorkflowRunProperties`).
- **Blueprints** — `CreateBlueprint` / `GetBlueprint` / `ListBlueprints` / `UpdateBlueprint` / `DeleteBlueprint` / `BatchGetBlueprints`, runs (`StartBlueprintRun` / `GetBlueprintRun` / `GetBlueprintRuns`).
- **Dev endpoints** — `CreateDevEndpoint` / `GetDevEndpoint` / `GetDevEndpoints` / `ListDevEndpoints` / `UpdateDevEndpoint` / `DeleteDevEndpoint`.
- **Schema registry** — registries (`CreateRegistry` / `GetRegistry` / `ListRegistries` / `UpdateRegistry` / `DeleteRegistry`), schemas (`CreateSchema` / `GetSchema` / `ListSchemas` / `UpdateSchema` / `DeleteSchema`), versions (`RegisterSchemaVersion` / `GetSchemaVersion` / `GetSchemaByDefinition` / `ListSchemaVersions` / `DeleteSchemaVersions` / `GetSchemaVersionsDiff` / `CheckSchemaVersionValidity` / `QuerySchemaVersionMetadata`), version metadata (`PutSchemaVersionMetadata` / `RemoveSchemaVersionMetadata`), and `UpdateSchemaVersionMetadata`.
- **Interactive sessions** — `CreateSession` / `GetSession` / `ListSessions` / `DeleteSession` / `StopSession`, statements (`RunStatement` / `GetStatement` / `ListStatements` / `CancelStatement`). Statement ids auto-increment per session. `GetSessionEndpoint` returns a synthesized Spark Connect endpoint (URL + short-lived auth token) for an existing session, and `GetDashboardUrl` returns a synthesized Spark monitoring dashboard URL for a `SESSION` or `JOB`.
- **ML transforms** — `CreateMLTransform` / `GetMLTransform` / `GetMLTransforms` / `ListMLTransforms` / `UpdateMLTransform` / `DeleteMLTransform`, task runs (`StartMLEvaluationTaskRun` / `StartMLLabelingSetGenerationTaskRun` / `GetMLTaskRun` / `GetMLTaskRuns` / `CancelMLTaskRun`), and `GetMLTransform` parameter round-trip / `StartExportLabelsTaskRun` / `StartImportLabelsTaskRun`.
- **Data quality** — rulesets (`CreateDataQualityRuleset` / `GetDataQualityRuleset` / `ListDataQualityRulesets` / `UpdateDataQualityRuleset` / `DeleteDataQualityRuleset`), evaluation runs (`StartDataQualityRulesetEvaluationRun` / `GetDataQualityRulesetEvaluationRun` / `ListDataQualityRulesetEvaluationRuns`), recommendation runs (`StartDataQualityRuleRecommendationRun` / `GetDataQualityRuleRecommendationRun` / `ListDataQualityRuleRecommendationRuns`), and results (`GetDataQualityResult` / `ListDataQualityResults` / `BatchGetDataQualityResult`).
- **User-defined functions** — `CreateUserDefinedFunction` / `GetUserDefinedFunction` / `GetUserDefinedFunctions` / `UpdateUserDefinedFunction` / `DeleteUserDefinedFunction`.
- **Usage profiles** — `CreateUsageProfile` / `GetUsageProfile` / `ListUsageProfiles` / `UpdateUsageProfile` / `DeleteUsageProfile`.
- **Column statistics** — for tables and partitions (`UpdateColumnStatisticsForTable` / `GetColumnStatisticsForTable` / `DeleteColumnStatisticsForTable`, and the `...ForPartition` variants), plus the async stats-task ops (`StartColumnStatisticsTaskRun` / `GetColumnStatisticsTaskRun` / `ListColumnStatisticsTaskRuns` / `StopColumnStatisticsTaskRun` and the task-settings / schedule ops).
- **Table optimizers** — `CreateTableOptimizer` / `GetTableOptimizer` / `ListTableOptimizerRuns` / `UpdateTableOptimizer` / `DeleteTableOptimizer` / `BatchGetTableOptimizer`.
- **Security configurations & misc** — `CreateSecurityConfiguration` / `GetSecurityConfiguration` / `GetSecurityConfigurations` / `DeleteSecurityConfiguration`, integrations, materialized-view refresh task runs, the Glue Identity Center configuration ops, and tagging (`TagResource` / `UntagResource` / `GetTags`).

Server-side validation ports the Smithy `@length` / `@range` / enum constraints into every handler, so out-of-bounds and malformed inputs return the AWS-shaped `InvalidInputException` rather than being silently accepted.

## Athena integration

The same Data Catalog state powers Athena's catalog reads:

- `glue:CreateDatabase` -> visible in `athena:ListDatabases` under `AwsDataCatalog`.
- `glue:CreateTable` -> visible in `athena:GetTableMetadata` / `athena:ListTableMetadata`.
- Column types and partition keys round-trip end-to-end, so Athena's `DESCRIBE` / `SHOW TABLES` return the schema you registered through Glue.

See the [Athena docs](/docs/services/athena/) for the minimal SQL evaluator that runs over this catalog.

## Smoke test

```sh
fakecloud &

aws --endpoint-url http://localhost:4566 glue create-database \
  --database-input Name=analytics

aws --endpoint-url http://localhost:4566 glue create-table \
  --database-name analytics \
  --table-input 'Name=events,PartitionKeys=[{Name=dt,Type=string}],StorageDescriptor={Columns=[{Name=id,Type=string}],Location=s3://my-bucket/events/}'

# Register a crawler and run it through its lifecycle.
aws --endpoint-url http://localhost:4566 glue create-crawler \
  --name events-crawler \
  --role arn:aws:iam::000000000000:role/glue \
  --database-name analytics \
  --targets 'S3Targets=[{Path=s3://my-bucket/events/}]'

aws --endpoint-url http://localhost:4566 glue start-crawler --name events-crawler
aws --endpoint-url http://localhost:4566 glue get-crawler --name events-crawler \
  --query 'Crawler.State'   # RUNNING

# Prune partitions server-side.
aws --endpoint-url http://localhost:4566 glue get-partitions \
  --database-name analytics --table-name events \
  --expression "dt >= '2026-05-10'"
```

## Introspection

Three IAM-bypass admin endpoints expose Glue state so test assertions don't have to round-trip through the AWS SDK:

- `GET /_fakecloud/glue/jobs` — every Glue Job recorded by `CreateJob`, across every account.
- `GET /_fakecloud/glue/job-runs` — every `JobRun` recorded by `StartJobRun`. Accepts `?job_name=foo` to filter to a single job.
- `GET /_fakecloud/glue/crawlers` — every crawler recorded by `CreateCrawler`, across every account. Returns `name`, `role`, `databaseName`, `state`, a `targetSummary` (e.g. `"2 S3, 1 JDBC"`), `schedule`, `creationTime`, `lastUpdated`. Sorted by account, then name.

All first-party SDKs ship a `glue` sub-client wrapping these endpoints (`getJobs()`, `getJobRuns(jobName?)`, `getCrawlers()`). See [`reference/introspection`](/docs/reference/introspection/) for the full endpoint catalog.

## Caveats

Execution engines are not implemented. `StartJobRun` does not fetch your script from S3 or spin up a Spark / Python Shell worker; crawlers don't actually scan S3 or JDBC sources; ML transforms don't train; data-quality runs don't evaluate real rules. Every run lands in its terminal state immediately with shape-correct metadata. Use real Glue for actual ETL / crawling / ML; use fakecloud to test the orchestration, catalog-management, and registry code paths around it.

## Source

- [`crates/fakecloud-glue`](https://github.com/faiscadev/fakecloud/tree/main/crates/fakecloud-glue)
- [AWS Glue API reference](https://docs.aws.amazon.com/glue/latest/webapi/Welcome.html)
