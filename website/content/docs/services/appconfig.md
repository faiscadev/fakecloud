+++
title = "AWS AppConfig"
description = "AWS AppConfig (appconfig + appconfigdata) on fakecloud: applications, environments, configuration profiles, hosted configuration versions storing real bytes, deployment strategies, deployments, extensions, experiments, and a real data plane that serves deployed configuration — account-partitioned, persisted state."
weight = 54
+++

fakecloud implements **AWS AppConfig** as a single service that speaks both AWS
model-services behind the shared `appconfig` SigV4 signing name: the
**56-operation `appconfig` control plane** and the **2-operation `appconfigdata`
data plane** (58 operations total, restJson1). Control-plane traffic routes on
paths like `/applications/...`, `/deploymentstrategies/...` and `/extensions/...`;
the data plane routes on `/configurationsessions` and `/configuration`. Every
resource is real, account-partitioned state that persists across restarts in
persistent mode, so what one session creates the next session still sees.

Unlike a control-plane-only emulator, AppConfig on fakecloud serves a **real data
plane**: hosted configuration versions store the exact bytes you upload, and
`GetLatestConfiguration` resolves a session token back to the latest deployed
configuration and returns those bytes verbatim with the right content type.

## Supported features

- **Applications** (`CreateApplication`, `GetApplication`, `UpdateApplication`,
  `DeleteApplication`, `ListApplications`). New applications get a 7-character
  id and an `arn:aws:appconfig:<region>:<acct>:application/<id>` ARN. Deleting an
  application cascades to its environments, configuration profiles (and their
  hosted versions) and experiments.
- **Environments** (`CreateEnvironment`, `GetEnvironment`, `UpdateEnvironment`,
  `DeleteEnvironment`, `ListEnvironments`) nested under an application, with
  `Monitors` round-tripped verbatim.
- **Configuration profiles** (`CreateConfigurationProfile`,
  `GetConfigurationProfile`, `UpdateConfigurationProfile`,
  `DeleteConfigurationProfile`, `ListConfigurationProfiles`,
  `ValidateConfiguration`). Deleting a profile cascades to its hosted
  configuration versions.
- **Hosted configuration versions** (`CreateHostedConfigurationVersion`,
  `GetHostedConfigurationVersion`, `DeleteHostedConfigurationVersion`,
  `ListHostedConfigurationVersions`). The request body is the raw configuration
  payload; fakecloud stores the exact bytes plus the `Content-Type`, and returns
  them verbatim with the `Application-Id`, `Configuration-Profile-Id`,
  `Version-Number` and `Content-Type` response headers. `VersionNumber`
  auto-increments per profile.
- **Deployment strategies** (`CreateDeploymentStrategy`, `GetDeploymentStrategy`,
  `UpdateDeploymentStrategy`, `DeleteDeploymentStrategy`,
  `ListDeploymentStrategies`). The AWS-predefined strategies
  (`AppConfig.AllAtOnce`, `AppConfig.Linear50PercentEvery30Seconds`,
  `AppConfig.Canary10Percent20Minutes`,
  `AppConfig.Linear20PercentEvery6Minutes`) are resolvable by their well-known
  ids.
- **Deployments** (`StartDeployment`, `GetDeployment`, `StopDeployment`,
  `ListDeployments`). A deployment settles straight to `COMPLETE` (100 %
  `PercentageComplete`) with an event log so a waiter — and Terraform — observes
  the terminal state; `DeploymentNumber` auto-increments per environment.
- **Extensions + associations** (`CreateExtension`, `GetExtension`,
  `UpdateExtension`, `DeleteExtension`, `ListExtensions`,
  `CreateExtensionAssociation`, `GetExtensionAssociation`,
  `UpdateExtensionAssociation`, `DeleteExtensionAssociation`,
  `ListExtensionAssociations`) with action-point maps and parameters
  round-tripped verbatim.
- **Experiments** (`CreateExperimentDefinition`, `GetExperimentDefinition`,
  `UpdateExperimentDefinition`, `DeleteExperimentDefinition`,
  `ListExperimentDefinitions`, plus the run lifecycle: `StartExperimentRun`,
  `GetExperimentRun`, `UpdateExperimentRun`, `StopExperimentRun`,
  `ListExperimentRuns`, `ListExperimentRunEvents`).
- **Account settings** (`GetAccountSettings`, `UpdateAccountSettings`) for
  deletion protection and vended metrics.
- **Legacy `GetConfiguration`** returns the latest deployed hosted-config bytes
  for an application/environment/profile, and **tagging** (`TagResource`,
  `UntagResource`, `ListTagsForResource`) is keyed by resource ARN.

## Data plane (`appconfigdata`)

- **`StartConfigurationSession`** binds an application/environment/profile to an
  opaque `InitialConfigurationToken`.
- **`GetLatestConfiguration`** resolves that token to the latest deployed hosted
  configuration version and returns its raw bytes, the `Content-Type`, and a
  `NextPollConfigurationToken`.

All `List` operations honor `MaxResults` / `NextToken` pagination, and
`@length`, `@range`, and enum input constraints are enforced with the model's
declared error codes (`BadRequestException`, `ResourceNotFoundException`,
`ConflictException`).

100% conformance: all 2,324 generated Smithy probe variants pass across both the
`appconfig` (2,257) and `appconfigdata` (67) model-services.
