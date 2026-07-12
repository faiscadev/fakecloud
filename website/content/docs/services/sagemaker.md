+++
title = "Amazon SageMaker"
description = "Amazon SageMaker (sagemaker) on fakecloud: the full 403-operation ML control plane — models, endpoints, endpoint configs, training/processing/transform/AutoML/tuning jobs, notebook instances, pipelines, feature groups, domains, model packages — at 100% conformance. awsJson1.1."
weight = 80
+++

fakecloud implements the **Amazon SageMaker control plane** (`sagemaker`, SDK id
`SageMaker`, endpoint prefix `api.sagemaker`) as an **awsJson1.1** service. All
**403 operations** ship with **100% conformance** against AWS's own Smithy
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
  and listed; `Stop*` is accepted.
- **Model packages** (+ groups), **pipelines**, **feature groups**, **domains**,
  **user profiles**, **spaces**, **apps**, **images** (+ versions),
  **experiments**, **trials** (+ components), **actions**, **artifacts**,
  **contexts**, **clusters**, **inference components**, **monitoring
  schedules**, **notebook instances** (+ lifecycle configs), **code
  repositories**, **workteams**, **workforces**, and the rest of the ~130
  families.
- **Tags** — ARN-keyed `AddTags` / `ListTags` / `DeleteTags`.

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
(`CreatePresignedNotebookInstanceUrl`, `CreatePresignedDomainUrl`) and the
lineage / search query operations are accepted as control-plane no-ops.
