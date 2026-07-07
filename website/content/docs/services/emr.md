+++
title = "Amazon EMR"
description = "Amazon EMR (Elastic MapReduce) on fakecloud: the full 65-operation control plane -- clusters/job flows, steps, instance groups and fleets, EMR Studio, notebook executions, security configurations, scaling and auto-termination policies, block public access, and tags -- with account-partitioned persistence."
weight = 72
+++

fakecloud implements **Amazon EMR** (`elasticmapreduce`), the managed
Hadoop/Spark big-data platform. All **65 operations** from the AWS Smithy model
ship now, backed by account-partitioned state that persists across restarts in
persistent mode. The wire protocol is awsJson1.1 (x-amz-target
`ElasticMapReduce.<Op>`), signing as `elasticmapreduce`.

This is the **control plane**: `RunJobFlow` provisions a cluster that settles to
`WAITING` via the control-plane state machine, and steps settle to `COMPLETED`.
Real Spark/Hadoop job execution inside containers is a later batch; every
operation here is real, validated, persisted CRUD -- no stubbed success
responses. Requests are validated against the model's `required` / `length` /
`range` / `enum` constraints before any handler runs, and an operation that
dereferences a cluster/step/studio/session that does not exist returns EMR's
`InvalidRequestException`, matching the live service.

## Supported features

- **Clusters / job flows** (`RunJobFlow`, `DescribeCluster`, `ListClusters`,
  `ModifyCluster`, `TerminateJobFlows`, `DescribeJobFlows`,
  `SetTerminationProtection`, `SetKeepJobFlowAliveWhenNoSteps`,
  `SetVisibleToAllUsers`, `SetUnhealthyNodeReplacement`). `RunJobFlow` mints a
  `j-XXXXXXXXXXXXX` cluster id and an
  `arn:aws:elasticmapreduce:<region>:<account>:cluster/<id>` ARN, derives
  instance groups/fleets and EC2 instances from the request, and settles the
  cluster to `WAITING`. `ListClusters` honors the `ClusterStates` filter.
- **Steps** (`AddJobFlowSteps`, `ListSteps`, `DescribeStep`, `CancelSteps`).
  Steps carry `s-XXXXXXXXXXXXX` ids; `ListSteps` honors `StepStates` / `StepIds`
  filters; `CancelSteps` reports per-step `SUBMITTED` / `FAILED` status.
- **Instance groups** (`AddInstanceGroups`, `ListInstanceGroups`,
  `ModifyInstanceGroups`) with `ig-` ids, plus **auto-scaling policies**
  (`PutAutoScalingPolicy`, `RemoveAutoScalingPolicy`).
- **Instance fleets** (`AddInstanceFleet`, `ListInstanceFleets`,
  `ModifyInstanceFleet`) with `if-` ids and provisioned on-demand/spot capacity.
- **Instances and bootstrap actions** (`ListInstances`, `ListBootstrapActions`).
- **Managed scaling and auto-termination policies** (`PutManagedScalingPolicy`,
  `GetManagedScalingPolicy`, `RemoveManagedScalingPolicy`,
  `PutAutoTerminationPolicy`, `GetAutoTerminationPolicy`,
  `RemoveAutoTerminationPolicy`).
- **Security configurations** (`CreateSecurityConfiguration`,
  `DescribeSecurityConfiguration`, `DeleteSecurityConfiguration`,
  `ListSecurityConfigurations`) with duplicate-name rejection.
- **EMR Studio** (`CreateStudio`, `DescribeStudio`, `UpdateStudio`,
  `DeleteStudio`, `ListStudios`) with `es-` ids and Studio ARNs, plus **session
  mappings** (`CreateStudioSessionMapping`, `GetStudioSessionMapping`,
  `UpdateStudioSessionMapping`, `DeleteStudioSessionMapping`,
  `ListStudioSessionMappings`).
- **Notebook executions** (`StartNotebookExecution`,
  `DescribeNotebookExecution`, `ListNotebookExecutions`,
  `StopNotebookExecution`).
- **Persistent app UIs** (`CreatePersistentAppUI`, `DescribePersistentAppUI`,
  `GetPersistentAppUIPresignedURL`, `GetOnClusterAppUIPresignedURL`).
- **Interactive sessions** (`StartSession`, `GetSession`, `GetSessionEndpoint`,
  `TerminateSession`, `ListSessions`, `GetClusterSessionCredentials`).
- **Block public access** (`GetBlockPublicAccessConfiguration`,
  `PutBlockPublicAccessConfiguration`).
- **Release labels and instance types** (`ListReleaseLabels`,
  `DescribeReleaseLabel`, `ListSupportedInstanceTypes`).
- **Tags** (`AddTags`, `RemoveTags`) keyed by cluster or Studio id.

## Persistence

All EMR state is account-partitioned and, in persistent mode
(`--data-dir` / `FAKECLOUD_DATA_DIR`), is snapshotted to
`<data-dir>/emr/snapshot.json` after every mutation and restored on restart.

## Example

```python
import boto3

emr = boto3.client("emr", endpoint_url="http://localhost:8080")

run = emr.run_job_flow(
    Name="analytics",
    ReleaseLabel="emr-7.1.0",
    ServiceRole="EMR_DefaultRole",
    JobFlowRole="EMR_EC2_DefaultRole",
    Instances={
        "InstanceCount": 3,
        "MasterInstanceType": "m5.xlarge",
        "SlaveInstanceType": "m5.xlarge",
        "KeepJobFlowAliveWhenNoSteps": True,
    },
)
cluster_id = run["JobFlowId"]

emr.add_job_flow_steps(
    JobFlowId=cluster_id,
    Steps=[
        {
            "Name": "word-count",
            "HadoopJarStep": {
                "Jar": "command-runner.jar",
                "Args": ["spark-submit", "s3://mybucket/wordcount.py"],
            },
        }
    ],
)

cluster = emr.describe_cluster(ClusterId=cluster_id)["Cluster"]
print(cluster["Status"]["State"])  # WAITING

emr.terminate_job_flows(JobFlowIds=[cluster_id])
```
