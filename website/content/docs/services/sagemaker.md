+++
title = "Amazon SageMaker"
description = "Amazon SageMaker (sagemaker) on fakecloud: the full 409-operation ML control plane — models, endpoints, endpoint configs, training/processing/transform/AutoML/tuning jobs, notebook instances, pipelines, feature groups, domains, model packages — at 100% conformance. awsJson1.1."
weight = 80
+++

fakecloud implements the **Amazon SageMaker control plane** (`sagemaker`, SDK id
`SageMaker`, endpoint prefix `api.sagemaker`) as an **awsJson1.1** service. All
**409 operations** ship with **100% conformance** against AWS's own Smithy
model, backed by account-partitioned state that persists across restarts in
persistent mode. SageMaker signs SigV4 with the `sagemaker` scope; every request
is a `POST /` whose operation is selected by the `X-Amz-Target: SageMaker.<Op>`
header, with all inputs carried in the JSON body (no HTTP path / label / query
bindings).

The operation table, the per-operation model-derived input constraints, the
output member shapes, the list element shapes, and each operation's resource
family + identifier member are all generated directly from the Smithy model
(`scripts/generate-sagemaker-tables.py`), so the control plane tracks the model
exactly rather than by hand.

## Uniform resource engine

SageMaker's ~130 resource families all follow the same shape: `Create<X>` takes
an `<X>Name`, `Describe<X>` / `Delete<X>` / `Update<X>` take the same name, and
`List<X>s` returns an array of `<X>Summary` structs. A single generic engine
serves every family:

- **Create** mints a proper `arn:aws:sagemaker:<region>:<account>:<kind>/<name>`
  ARN, stamps a numeric `CreationTime` / `LastModifiedTime`, persists every
  accepted input field, and returns the family's `<X>Arn`. A duplicate name
  returns the family's declared conflict error (`ResourceInUse` /
  `ConflictException`).
- **Describe** echoes the persisted record — every field the create accepted,
  the minted ARN, and the timestamps.
- **List** projects each record onto its `<X>Summary` (Name / Arn /
  CreationTime and any other summary members), paginating with `NextToken`.
- **Update** merges the new fields and refreshes `LastModifiedTime`; **Delete**
  is idempotent.

A resource whose `Describe` identifier is a service-minted Id or ARN distinct
from the create-time Name (for example `Domain`, `ImageVersion`,
`ModelCardExportJob`) is resolved by scanning the family's minted identifiers,
so a describe by the returned Id / ARN still round-trips.

## What is real

Every modelled named resource mints proper ARNs, persists its attributes, and
round-trips on read / list / update:

- **Models** — `CreateModel` / `DescribeModel` / `ListModels` / `DeleteModel`,
  round-tripping the `PrimaryContainer` / `Containers` / `ExecutionRoleArn` /
  `VpcConfig`.
- **Endpoint configs** and **endpoints** — production variants, data-capture
  config, async-inference config, all persisted and echoed on describe.
- **Jobs** — training, processing, transform, labeling, compilation, AutoML
  (v1 + v2), and hyper-parameter-tuning jobs are created, persisted, described,
  and listed; `Stop*` / `Start*` move the stored status (a Describe reflects
  it). For a resource that does not exist they return the operation's
  declared `ResourceNotFound`, or SageMaker's `ValidationException`
  (`RecordNotFound`) for operations that declare none, such as
  `StopNotebookInstance`.
- **Edge deployment stages**: a plan's stages live on the plan:
  `CreateEdgeDeploymentStage` appends to it (an unknown plan or a duplicate
  stage name is rejected), `DeleteEdgeDeploymentStage` removes one stage, and
  `Start` / `StopEdgeDeploymentStage` move its status, all visible in
  `DescribeEdgeDeploymentPlan`.
- **Model packages** (+ groups), **pipelines**, **feature groups**, **domains**,
  **user profiles**, **spaces**, **apps**, **images** (+ versions),
  **experiments**, **trials** (+ components), **actions**, **artifacts**,
  **contexts**, **clusters**, **inference components**, **monitoring
  schedules**, **notebook instances** (+ lifecycle configs), **code
  repositories**, **workteams**, **workforces**, and the rest of the ~130
  families.
- **HyperPod cluster nodes** — `BatchAddClusterNodes` persists nodes that
  `ListClusterNodes` reflects; `BatchReplaceClusterNodes` /
  `BatchDeleteClusterNodes` mutate them. `AttachClusterNodeNetworkInterface`
  records an `eni-attach-*` attachment on an existing node (re-attaching the
  same ENI to the same node returns the same attachment; an ENI already on
  another node is a `ConflictException`; an unknown node is
  `ResourceNotFound`).
- **HyperPod node volumes**: `AttachClusterNodeVolume` records the volume on
  an existing node with the next free device name (`/dev/sdf`, `/dev/sdg`, ...);
  a volume already attached to any node is a `ConflictException`.
  `DetachClusterNodeVolume` removes it and echoes its device name and attach
  time; an unknown node, or a volume not attached to it, is `ResourceNotFound`.
- **Training plan extensions**: `SearchTrainingPlanOfferings` with a
  `TrainingPlanArn` mints an extension offering that starts where the plan
  ends; `ExtendTrainingPlan` redeems it (growing the plan's `EndTime` /
  `DurationHours`) and records it for `DescribeTrainingPlanExtensionHistory`.
  An unknown offering or plan is `ResourceNotFound`.
- **Search**: evaluates the `SearchExpression` (filters with every
  operator, nested filters, sub-expressions, `And` / `Or`, `Tags.<key>`
  properties) over the stored resources of the requested type, sorts by
  `SortBy` / `SortOrder` (default `LastModifiedTime`, `Descending`), paginates, and projects each hit onto its
  `SearchRecord` member with an exact `TotalHits`.
- **QueryLineage**: walks the stored `AddAssociation` edges from the start
  entities (`Ascendants` / `Descendants` / `Both`, `MaxDepth`, `Filters`),
  returning the reached vertices with their lineage type and entity type plus
  the traversed edges. Any stored SageMaker resource is a valid start entity
  (one with no associations comes back as a lone vertex); an unknown start
  ARN is `ResourceNotFound`.
- **Tags**: ARN-keyed `AddTags` / `ListTags` / `DeleteTags`. Tags passed to
  a `Create*` land in the same store, so `ListTags` returns them right away;
  Describe outputs that carry `Tags` (for example `DescribeLabelingJob`)
  render from that store, and deleting a resource drops its tags.

Input validation is model-derived: `required` members, string `@length`,
numeric `@range`, and `@enum` constraints are enforced, returning SageMaker's
`ValidationException`. A missing resource returns `ResourceNotFound`; a
duplicate create returns `ResourceInUse`. Timestamps are emitted as awsJson1.1
epoch-second JSON numbers. State is account-partitioned and persisted across
restarts.

## Honest emulation choices

This is the SageMaker **control plane** only. There is **no ML execution
plane**: training / processing / transform / AutoML / tuning jobs are created,
persisted, and described, but no container is scheduled, no model is trained,
and no inference endpoint serves traffic. Jobs and endpoints are not advanced
through a live lifecycle by a background scheduler — a described job reflects
what was submitted, not a running computation. Presigned-URL operations
(`CreatePresignedNotebookInstanceUrl`, `CreatePresignedDomainUrl`) are
accepted as control-plane no-ops.
