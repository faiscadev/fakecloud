+++
title = "Amazon Managed Service for Apache Flink"
description = "Amazon Managed Service for Apache Flink (kinesisanalyticsv2, formerly Kinesis Data Analytics v2) on fakecloud: full 33-operation control plane for SQL and Flink streaming applications, versions, snapshots, operations, VPC and CloudWatch logging configuration, with persistence."
weight = 33
+++

fakecloud implements **Amazon Managed Service for Apache Flink**
(`kinesisanalyticsv2`, formerly Kinesis Data Analytics v2), the managed
streaming-application service. All **33 operations** from the AWS Smithy model
ship now, backed by account-partitioned state that persists across restarts in
persistent mode. The wire protocol is awsJson1.1 (x-amz-target
`KinesisAnalytics_20180523.<Op>`), signing as `kinesisanalytics`.

## Supported features

- **Applications** (`CreateApplication`, `DescribeApplication`,
  `UpdateApplication`, `DeleteApplication`, `ListApplications`). Both
  application flavors are modeled: **SQL** applications
  (`RuntimeEnvironment` `SQL-1_0`, with inputs/outputs/reference data sources)
  and **Flink** applications (`FLINK-1_15` ... `FLINK-1_20`, with
  `ApplicationCodeConfiguration`, `EnvironmentProperties`, and
  checkpoint/monitoring/parallelism configuration). Whatever configuration is
  created or updated is persisted and echoed back on describe. Applications
  carry `arn:aws:kinesisanalytics:<region>:<account>:application/<name>` ARNs.
- **Lifecycle** (`StartApplication`, `StopApplication`, `RollbackApplication`).
  For a **Flink-flavor** application with a JAR in S3, `StartApplication` spawns
  a REAL Apache Flink job in a Docker container (see [Data plane](#data-plane)
  below) and only reaches `RUNNING` once Flink reports the job `RUNNING`;
  `StopApplication` cancels the real job and tears the container down. In the
  degraded / control-plane-only path (no container runtime, or a SQL app),
  `StartApplication` moves `READY` -> `STARTING` -> `RUNNING` (settling on the
  next describe) and `StopApplication` moves `RUNNING` -> `STOPPING` -> `READY`.
  `Force` is supported; `RollbackApplication` restores the previous version's
  configuration into a new version.
- **Versioning** (`DescribeApplicationVersion`, `ListApplicationVersions`).
  Every configuration-changing operation increments `ApplicationVersionId` and
  records the full version history. `CurrentApplicationVersionId` and
  `ConditionalToken` optimistic-concurrency checks are enforced.
- **Snapshots** (`CreateApplicationSnapshot`, `DescribeApplicationSnapshot`,
  `ListApplicationSnapshots`, `DeleteApplicationSnapshot`) capture the
  application version and settle `CREATING` -> `READY`.
- **Operations** (`DescribeApplicationOperation`, `ListApplicationOperations`).
  Each Start/Stop/Update/Rollback records an async operation that settles
  `SUCCESSFUL`.
- **CloudWatch logging and VPC configuration**
  (`AddApplicationCloudWatchLoggingOption`,
  `DeleteApplicationCloudWatchLoggingOption`,
  `AddApplicationVpcConfiguration`, `DeleteApplicationVpcConfiguration`) mint
  ids, bump the version, and are reflected in `DescribeApplication`.
- **SQL sub-resources** (`AddApplicationInput`,
  `AddApplicationInputProcessingConfiguration`,
  `DeleteApplicationInputProcessingConfiguration`, `AddApplicationOutput`,
  `DeleteApplicationOutput`, `AddApplicationReferenceDataSource`,
  `DeleteApplicationReferenceDataSource`) assign ids and update the version.
- **Schema discovery** (`DiscoverInputSchema`) infers a record format and
  columns from the provided input.
- **Presigned dashboard URL** (`CreateApplicationPresignedUrl`) returns the
  REAL reachable Flink Web Dashboard / REST base URL when a Flink app is running
  against the live container runtime, and a well-formed synthesized authorized
  URL otherwise.
- **Maintenance windows** (`UpdateApplicationMaintenanceConfiguration`).
- **Tagging** (`TagResource`, `UntagResource`, `ListTagsForResource`),
  ARN-keyed; `CreateApplication` honors inline `Tags`.

100% conformance: all 1,232 generated Smithy probe variants pass.

## Data plane

Starting a **Flink-flavor** application (`RuntimeEnvironment` `FLINK-1_x`) whose
`ApplicationCodeConfiguration` points at a JAR in a fakecloud S3 bucket runs a
GENUINE Apache Flink job, the same Docker-backed data-plane bar Amazon MQ, MSK,
RDS, ElastiCache, and Lambda meet:

- `StartApplication` spawns a real **`flink:1.19`** session cluster (a JobManager
  + TaskManager) in a container, publishing the Flink REST port `8081` on a free
  host port.
- fakecloud fetches the application's code JAR from its own S3 service, uploads
  it to the cluster via the Flink REST API (`POST /jars/upload`), and submits it
  (`POST /jars/{id}/run`) with the configured parallelism.
- The application settles to `RUNNING` **only once the real Flink job reaches
  `RUNNING`** (polled via `GET /jobs/{id}`); `DescribeApplication` reflects the
  live job status.
- `CreateApplicationPresignedUrl` returns the cluster's reachable dashboard/REST
  URL, so you can drive the live JobManager directly.
- `StopApplication` cancels the real job (`PATCH /jobs/{id}?mode=cancel`) and
  tears the container down; `DeleteApplication` reaps it. Persisted RUNNING apps
  re-attach their container on restart (no `RUNNING`-with-dead-container).

The backing runtime is used automatically when a container CLI (Docker/Podman)
is available. It is skipped (and the pure control-plane state machine used
instead) when none is present or when `FAKECLOUD_KINESISANALYTICSV2_DISABLE_BACKEND=1`
is set; the image is overridable via `FAKECLOUD_KINESISANALYTICSV2_IMAGE`. The
end-to-end Docker-backed job is proven by the `flink-runtime` CI job, gated on
`FAKECLOUD_E2E_FLINK=1`.

**Honest scope: SQL applications do not run for real.** A **SQL-flavor**
application (`RuntimeEnvironment` `SQL-1_0`) keeps the control-plane state
machine (`STARTING` -> `RUNNING` in memory) — running arbitrary SQL as a live
Flink job needs the Flink SQL gateway, which is out of scope for now. Only
Flink-flavor apps with a JAR get the real runtime.

## Example

```python
import boto3
flink = boto3.client("kinesisanalyticsv2", endpoint_url="http://localhost:4566")

flink.create_application(
    ApplicationName="clickstream",
    RuntimeEnvironment="FLINK-1_20",
    ServiceExecutionRole="arn:aws:iam::000000000000:role/service-role/kinesis-analytics",
    ApplicationConfiguration={
        "FlinkApplicationConfiguration": {
            "ParallelismConfiguration": {"ConfigurationType": "DEFAULT"}
        },
        "ApplicationCodeConfiguration": {
            "CodeContentType": "ZIPFILE",
            "CodeContent": {
                "S3ContentLocation": {
                    "BucketARN": "arn:aws:s3:::my-code",
                    "FileKey": "app.jar",
                }
            },
        },
    },
)

flink.start_application(ApplicationName="clickstream")
app = flink.describe_application(ApplicationName="clickstream")["ApplicationDetail"]
print(app["ApplicationStatus"], app["ApplicationVersionId"])
```
