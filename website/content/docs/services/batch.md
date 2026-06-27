+++
title = "Batch"
description = "AWS Batch — compute environments, job queues, job definitions, scheduling policies, and the job control plane. restJson1 protocol."
weight = 29
+++

AWS Batch (the `batch` service) runs batch computing workloads: you register a
job definition, point a job queue at one or more compute environments, and
submit jobs that run as containers.

The wedge: against every other free local emulator AWS Batch is a *fake* — its
compute does no real work. MiniStack's Batch jumps a job straight to
`SUCCEEDED` with no container; Moto runs Docker but leaks; LocalStack gates
Batch behind its Ultimate tier. fakecloud already runs ECS tasks as real
containers, and Batch is built to run real jobs on that same engine.

## Supported today

- **Compute environments** — `CreateComputeEnvironment`, `DescribeComputeEnvironments`, `UpdateComputeEnvironment`, `DeleteComputeEnvironment`. Created `VALID` / `ENABLED`.
- **Job queues** — `CreateJobQueue`, `DescribeJobQueues`, `UpdateJobQueue`, `DeleteJobQueue`, with `computeEnvironmentOrder`, `priority`, and an optional `schedulingPolicyArn`.
- **Job definitions** — `RegisterJobDefinition` (monotonic per-name `revision`), `DescribeJobDefinitions` (filter by name / ARN / `status`), `DeregisterJobDefinition` (marks the revision `INACTIVE`).
- **Scheduling policies** — `CreateSchedulingPolicy`, `DescribeSchedulingPolicies`, `ListSchedulingPolicies`, `UpdateSchedulingPolicy`, `DeleteSchedulingPolicy` (fair-share).
- **Jobs — real container execution** — `SubmitJob` launches the job definition's `containerProperties` (image / command / vcpus / memory / environment, with this submit's `containerOverrides` applied) as a **real container** on fakecloud's ECS task engine, and drives the job status off the container's actual lifecycle: `SUBMITTED → STARTING → RUNNING → SUCCEEDED` when the container exits 0, or `FAILED` (carrying the real `container.exitCode`) on a non-zero exit. This is the wedge: every other free emulator fakes Batch compute (MiniStack jumps straight to `SUCCEEDED` with no container). With no container runtime available the job stays `SUBMITTED` honestly — never an auto-success. `DescribeJobs` / `ListJobs` (filter by queue / status) report live status + exit code; `CancelJob` / `TerminateJob` stop a job.
- **Array jobs** — `SubmitJob` with `arrayProperties.size = N` spawns `N` real child containers (`<jobId>:<index>`), each with `AWS_BATCH_JOB_ARRAY_INDEX` set so it can select its slice of work. The parent's status and `arrayProperties.statusSummary` aggregate the children live — `SUCCEEDED` only when every child exits 0.
- **Job dependencies** — `SubmitJob` with `dependsOn` parks the job at `PENDING` and launches it only once every dependency has `SUCCEEDED`; if any dependency `FAILED`, the dependent job fails with "Dependent job failed". The wait never blocks the `SubmitJob` call.
- **Retry + timeout** — `retryStrategy.attempts` re-launches a failed container up to that many times (each prior attempt recorded under `attempts[]`); `timeout.attemptDurationSeconds` caps each attempt and fails the job with "Job attempt duration exceeded timeout" if the container overruns.
- **Tags** — `TagResource`, `UntagResource`, `ListTagsForResource`.

Terraform / CloudFormation can provision a full Batch stack
(`aws_batch_compute_environment`, `aws_batch_job_queue`,
`aws_batch_job_definition`, `aws_batch_scheduling_policy`) and an SDK client can
submit jobs (single, array, dependency-chained, retried, or timed-out) that run
real containers and report their real exit codes.

## Coming next

A CloudFormation provisioner for `AWS::Batch::*` resources + Terraform
acceptance-test coverage.
