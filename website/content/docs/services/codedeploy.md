+++
title = "AWS CodeDeploy"
description = "AWS CodeDeploy (codedeploy) on fakecloud: a complete 47-operation implementation (100% conformance) — applications, application revisions, deployment groups, deployment configurations, deployments and their targets, on-premises instances, and tagging. awsJson1.1."
weight = 65
+++

fakecloud implements **AWS CodeDeploy** as an awsJson1.1 service (sigv4 signing
name `codedeploy`, target prefix `CodeDeploy_20141006`). All **47 operations**
ship with **100% conformance** against AWS's own Smithy model, backed by
account-partitioned state that persists across restarts in persistent mode.

CodeDeploy automates application deployments to EC2/on-premises instances,
Lambda functions, and ECS services. fakecloud models the whole control plane as
real, persisted CRUD; there is no deployment engine, so a `CreateDeployment`
produces a deployment that settles deterministically from `Created` through
`InProgress` to a terminal `Succeeded` state across successive reads rather than
rolling anything out to real instances.

## Applications and revisions

`CreateApplication` / `GetApplication` / `UpdateApplication` /
`DeleteApplication` / `ListApplications` / `BatchGetApplications` manage
applications. Each application mints an `applicationId`, records its
`computePlatform` (`Server` / `Lambda` / `ECS`), and round-trips its
`linkedToGitHub` flag. `RegisterApplicationRevision` / `GetApplicationRevision`
/ `ListApplicationRevisions` / `BatchGetApplicationRevisions` track the revision
locations (`S3` / `GitHub` / `String` / `AppSpecContent`) registered against an
application, with their `genericRevisionInfo` register/first-used/last-used
timestamps.

## Deployment groups

`CreateDeploymentGroup` / `GetDeploymentGroup` / `UpdateDeploymentGroup` /
`DeleteDeploymentGroup` / `ListDeploymentGroups` / `BatchGetDeploymentGroups`
manage deployment groups. The `ec2TagFilters`, `onPremisesInstanceTagFilters`,
`autoScalingGroups`, `triggerConfigurations`, `alarmConfiguration`,
`autoRollbackConfiguration`, `deploymentStyle`, `blueGreenDeploymentConfiguration`,
`loadBalancerInfo`, and tag-set blocks round-trip verbatim. A group inherits its
application's compute platform, defaults its `deploymentConfigName` to
`CodeDeployDefault.OneAtATime`, and mints a `deploymentGroupId`;
`UpdateDeploymentGroup` supports renaming via `newDeploymentGroupName`.

## Deployment configurations

`CreateDeploymentConfig` / `GetDeploymentConfig` / `DeleteDeploymentConfig` /
`ListDeploymentConfigs` manage deployment configurations. The predefined
`CodeDeployDefault.*` configurations — `OneAtATime` / `HalfAtATime` /
`AllAtOnce` for EC2/on-premises, plus the Lambda and ECS canary/linear/all-at-once
traffic-routing configs — are always resolvable and always listed. User-created
configs carry a `minimumHealthyHosts` block (`HOST_COUNT` / `FLEET_PERCENT`) or a
`trafficRoutingConfig` and a `zonalConfig`, all round-tripping verbatim.

## Deployments

`CreateDeployment` resolves the application, deployment group, and deployment
config, mints a deployment id in exact AWS `d-XXXXXXXXX` form, and returns it in
`Created`. Successive `GetDeployment` / `BatchGetDeployments` reads settle it
`Created` -> `InProgress` -> `Succeeded`, stamping `startTime` / `completeTime`
and a fully-succeeded `deploymentOverview` — the same lazy-settle pattern
CodeBuild and EKS use. `ListDeployments` filters by application, deployment
group, and status; `StopDeployment`, `ContinueDeployment`, and
`SkipWaitTimeForInstanceTermination` cover the lifecycle actions. The deployment
target and (deprecated) instance read surfaces — `GetDeploymentTarget` /
`ListDeploymentTargets` / `BatchGetDeploymentTargets` and `GetDeploymentInstance`
/ `ListDeploymentInstances` / `BatchGetDeploymentInstances` — validate the
deployment and return the empty target set (no real instances participate).

## On-premises instances, tokens, and tagging

`RegisterOnPremisesInstance` / `DeregisterOnPremisesInstance` /
`GetOnPremisesInstance` / `ListOnPremisesInstances` /
`BatchGetOnPremisesInstances` manage on-premises instance registrations (an IAM
user or session ARN is required, and only one), and
`AddTagsToOnPremisesInstances` / `RemoveTagsFromOnPremisesInstances` maintain
their tags. `ListGitHubAccountTokenNames` / `DeleteGitHubAccountToken` cover the
GitHub account-token surface, `PutLifecycleEventHookExecutionStatus` records a
lifecycle-hook result, and `TagResource` / `UntagResource` /
`ListTagsForResource` tag applications, deployment groups, and deployment
configs by ARN.

## Not implemented

There is no deployment engine — the actual rollout (running the AppSpec
lifecycle on real instances, Lambda traffic shifting, ECS task-set swaps) is out
of scope, matching how LocalStack Community mocks CodeDeploy. Deployments settle
to a terminal status synchronously, and the deployment-target / instance reads
return well-formed empty result sets because no instances participate.
