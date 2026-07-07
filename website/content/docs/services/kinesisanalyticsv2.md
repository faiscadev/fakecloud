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
  `StartApplication` moves `READY` -> `STARTING` -> `RUNNING` (settling on the
  next describe); `StopApplication` moves `RUNNING` -> `STOPPING` -> `READY`
  (with `Force` support); `RollbackApplication` restores the previous version's
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
- **Presigned dashboard URL** (`CreateApplicationPresignedUrl`) returns a
  well-formed authorized URL.
- **Maintenance windows** (`UpdateApplicationMaintenanceConfiguration`).
- **Tagging** (`TagResource`, `UntagResource`, `ListTagsForResource`),
  ARN-keyed; `CreateApplication` honors inline `Tags`.

100% conformance: all 1,232 generated Smithy probe variants pass.

## Control plane vs data plane

Amazon Managed Service for Apache Flink ships as a full control plane today:
every application, version, snapshot, and operation is real, validated,
account-partitioned, and persisted. The real Flink-job **data plane**
(`StartApplication` actually running a Flink job inside a Docker container) is a
roadmap item, mirroring how other fakecloud services shipped their control plane
first.

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
