+++
title = "AWS FIS"
description = "AWS Fault Injection Simulator (fis) on fakecloud: a complete 26-operation control plane (100% conformance). restJson1."
weight = 73
+++

fakecloud implements **AWS FIS** (Fault Injection Simulator) as a restJson1
service. All **26 operations** ship with **100% conformance** against AWS's own
Smithy model, backed by account-partitioned state that persists across restarts
in persistent mode. FIS signs SigV4 with the `fis` scope
(`arn:aws:fis:...`), so no signing-name alias is required.

AWS FIS on fakecloud is a faithful **control plane**: every
`CreateExperimentTemplate` / `UpdateExperimentTemplate` is reflected by its
`GetExperimentTemplate` / `ListExperimentTemplates`, every
`DeleteExperimentTemplate` deletes, and every `StartExperiment` is durably
recorded and progresses through a deterministic lifecycle that settles on the
next read.

**Data plane (later batch).** The actual fault injection into other services —
stopping EC2 instances, draining ECS tasks, throttling API calls — is a
follow-up batch. Until then, an experiment progresses through its states
(`initiating` -> `running` -> `completed`) through the control-plane state
machine honestly, without perturbing real resources. This is stated plainly
rather than faked.

## Resources

- **Experiment templates** - `CreateExperimentTemplate`
  (`POST /experimentTemplates`) validates the required `clientToken`,
  `description`, `stopConditions`, `actions`, and `roleArn` (plus model
  `@length` constraints), echoes the `targets` / `actions` / `stopConditions` /
  `logConfiguration` verbatim, defaults `experimentOptions` (single-account
  targeting, fail-on-empty resolution), and returns an `EXT`-shaped id with an
  `arn:aws:fis:<region>:<account>:experiment-template/<id>` ARN.
  `GetExperimentTemplate` echoes exactly what was persisted;
  `UpdateExperimentTemplate` (`PATCH`) applies the requested changes and stamps
  `lastUpdateTime`; `ListExperimentTemplates` returns the
  `ExperimentTemplateSummary` projections (paginated via `maxResults` /
  `nextToken`); `DeleteExperimentTemplate` removes the template.

- **Experiments** - `StartExperiment` (`POST /experiments`) requires the
  template to exist, copies its targets / actions / stopConditions onto a new
  `EXP`-shaped experiment in state `initiating`, and returns it. The lifecycle
  settles deterministically on the next read:
  `initiating` -> `running` -> `completed`, with each action's `state` and the
  experiment's `startTime` / `endTime` tracking the run. `StopExperiment`
  (`DELETE /experiments/{id}`) moves a non-terminal experiment to `stopping`,
  which settles to `stopped`. `GetExperiment` / `ListExperiments` (with the
  `experimentTemplateId` filter) read them back; in-flight transitions reconcile
  on restart. `ListExperimentResolvedTargets` projects each experiment target's
  resolved resources.

- **Actions catalog** - `ListActions` / `GetAction` (`GET /actions` and
  `GET /actions/{id}`) serve the AWS-provided static action library — real ids
  like `aws:ec2:stop-instances`, `aws:ecs:stop-task`, `aws:eks:pod-delete`,
  `aws:rds:failover-db-cluster`, `aws:ssm:send-command`,
  `aws:fis:inject-api-internal-error`, and `aws:fis:wait` — with each action's
  parameters and target roles, and `arn:aws:fis:<region>::action/<id>` ARNs (an
  empty account field, matching AWS's AWS-owned actions).

- **Target resource types** - `ListTargetResourceTypes` /
  `GetTargetResourceType` serve the predefined resource-type catalog
  (`aws:ec2:instance`, `aws:ecs:task`, `aws:eks:pod`, `aws:rds:db-instance`, ...)
  with their descriptions and parameters.

- **Target account configurations** - the per-template
  `CreateTargetAccountConfiguration` /
  `UpdateTargetAccountConfiguration` / `DeleteTargetAccountConfiguration` /
  `GetTargetAccountConfiguration` / `ListTargetAccountConfigurations`
  operations (`.../targetAccountConfigurations/{accountId}`) manage multi-account
  experiment configurations, reflected by the template's
  `targetAccountConfigurationsCount`. At `StartExperiment` they are copied onto
  the experiment and read back via `GetExperimentTargetAccountConfiguration` /
  `ListExperimentTargetAccountConfigurations`.

- **Safety levers** - `GetSafetyLever` / `UpdateSafetyLeverState` model the
  account-level safety lever (`disengaged` by default; `engaged` halts
  experiments); an id that is not the account's lever returns
  `ResourceNotFoundException`.

- **Tagging** - `TagResource` / `UntagResource` / `ListTagsForResource`
  (`.../tags/{resourceArn}`) apply tags to experiment templates and experiments
  by ARN.

## Validation & errors

Model-derived validation rejects missing required members, out-of-range
`@length` / `@range` values, and invalid enums with `ValidationException`;
unknown resources return `ResourceNotFoundException`; a duplicate target-account
configuration returns `ConflictException` — matching each operation's declared
Smithy error set.

## Persistence

All experiment templates, experiments, target-account configurations, and
safety-lever states are account-partitioned and persisted. In persistent mode
they survive restart, and any experiment left mid-lifecycle is reconciled
forward on load.
