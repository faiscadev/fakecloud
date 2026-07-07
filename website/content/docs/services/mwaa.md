+++
title = "Amazon MWAA"
description = "Amazon MWAA (Managed Workflows for Apache Airflow, mwaa) on fakecloud: a complete 12-operation control plane (100% conformance). restJson1."
weight = 72
+++

fakecloud implements **Amazon MWAA** (Managed Workflows for Apache Airflow) as a
restJson1 service. All **12 operations** ship with **100% conformance** against
AWS's own Smithy model, backed by account-partitioned state that persists across
restarts in persistent mode. MWAA signs SigV4 with the `airflow` scope
(`arn:aws:airflow:...`), which fakecloud routes to the `mwaa` service.

Amazon MWAA on fakecloud is a faithful **control plane**: every
`CreateEnvironment` / `UpdateEnvironment` is reflected by its `GetEnvironment` /
`ListEnvironments`, every `DeleteEnvironment` deletes, and AWS's async
environment lifecycle is modelled by returning the transient state and settling
on the next read (`CREATING` -> `AVAILABLE`, `UPDATING` -> `AVAILABLE` with a
`SUCCESS` `LastUpdate`, `DELETING` -> removed), with any interrupted transition
reconciled on restart.

**Data plane (later batch).** A real Docker-backed Apache Airflow web server and
DAG runtime is a follow-up batch. Until then, `CreateEnvironment` settles to
`AVAILABLE` through the control-plane state machine honestly, and `InvokeRestApi`
returns the modelled `RestApiServerException` (the Airflow REST API is not yet
reachable) rather than a fabricated response.

## Resources

- **Environments** - `CreateEnvironment` (`PUT /environments/{Name}`) validates
  the required `ExecutionRoleArn`, `SourceBucketArn`, `DagS3Path`, and
  `NetworkConfiguration`, enforces the `EnvironmentName` pattern
  (`^[a-zA-Z][0-9a-zA-Z-_]*$`, 1-80 chars) and account+region name uniqueness,
  and returns the environment's
  `arn:aws:airflow:<region>:<account>:environment/<name>` ARN. The environment
  is created `CREATING` and settles to `AVAILABLE` on the next read.
  `GetEnvironment` returns the full `Environment` shape (echoing the create
  inputs plus AWS-synthesized fields: `WebserverUrl`, `ServiceRoleArn`,
  `CeleryExecutorQueue`, a per-module `LoggingConfiguration` with
  `CloudWatchLogGroupArn`s for enabled modules, and — under `CUSTOMER`
  endpoint management — `WebserverVpcEndpointService` /
  `DatabaseVpcEndpointService`). `UpdateEnvironment` (`PATCH`) applies the
  requested changes, moves the environment to `UPDATING`, and records a
  `LastUpdate` that settles `SUCCESS`. `ListEnvironments` returns the
  environment names (paginated via `MaxResults` / `NextToken`), and
  `DeleteEnvironment` moves the environment to `DELETING`.

- **Access tokens** - `CreateCliToken` (`POST /clitoken/{Name}`) and
  `CreateWebLoginToken` (`POST /webtoken/{Name}`) require the environment to
  exist and return a freshly minted, single-use token plus the environment's
  Airflow `WebServerHostname` (`CreateWebLoginToken` also returns the resolved
  `IamIdentity` / `AirflowIdentity`).

- **Airflow REST passthrough** - `InvokeRestApi` (`POST /restapi/{Name}`)
  validates the required `Path` / `Method`; it returns
  `ResourceNotFoundException` for an unknown environment and the modelled
  `RestApiServerException` until the Airflow web-server data plane is attached.

- **Metrics** - `PublishMetrics` (`POST /metrics/environments/{EnvironmentName}`)
  is the internal, deprecated metric sink the Airflow environment itself calls;
  its `MetricData` is documented as ignored, so it validates the input and
  returns an empty body.

- **Tagging** - `TagResource` / `UntagResource` / `ListTagsForResource` address
  the environment by its ARN (`/tags/{ResourceArn}`); tags live on the
  environment object so they round-trip on `GetEnvironment` and survive
  restarts. `UntagResource` reads its repeated multi-value `tagKeys` query
  parameter intact.

## Persistence

All MWAA state is account-partitioned and persisted in persistent mode;
in-flight environment lifecycle transitions (`CREATING` / `UPDATING` /
`DELETING`) reconcile on restart.
