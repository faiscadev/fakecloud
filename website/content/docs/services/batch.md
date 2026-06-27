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
- **Jobs (control plane)** — `SubmitJob` accepts a job against a queue + definition and returns its `jobId` / `jobArn`; `DescribeJobs` and `ListJobs` (filter by queue / status) report it; `CancelJob` and `TerminateJob` move a stoppable job to `FAILED`.
- **Tags** — `TagResource`, `UntagResource`, `ListTagsForResource`.

This is enough for Terraform / CloudFormation to provision a full Batch stack
(`aws_batch_compute_environment`, `aws_batch_job_queue`,
`aws_batch_job_definition`, `aws_batch_scheduling_policy`) and for an SDK client
to submit and track jobs.

## Coming next

Real container-backed job execution: `SubmitJob` launches the job definition's
container on the ECS task engine and drives the job through
`SUBMITTED → PENDING → RUNNABLE → STARTING → RUNNING → SUCCEEDED/FAILED` off the
real container exit code, plus array jobs, job dependencies, retry strategies,
and a CloudFormation provisioner.
