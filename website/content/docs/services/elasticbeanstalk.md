+++
title = "Elastic Beanstalk"
description = "Full Elastic Beanstalk control plane: applications, application versions, environments with a real Launching->Ready lifecycle, configuration templates and option settings, events, platforms, and CNAMEs."
weight = 67
+++

fakecloud implements **47 of 47** Elastic Beanstalk operations at 100% Smithy conformance. Elastic Beanstalk is an orchestration facade over EC2, Auto Scaling, Elastic Load Balancing, CloudFormation, S3, and CloudWatch; fakecloud models the full control plane with real, persisted state.

## Supported features

- **Applications** - `CreateApplication`, `UpdateApplication`, `UpdateApplicationResourceLifecycle`, `DescribeApplications`, `DeleteApplication`. `arn:aws:elasticbeanstalk:<region>:<account>:application/<name>` ARNs, version-lifecycle config (`MaxCountRule` / `MaxAgeRule`), and tags.
- **Application versions** - `CreateApplicationVersion` (source bundle recorded in S3), `UpdateApplicationVersion`, `DescribeApplicationVersions`, `DeleteApplicationVersion`, with `AutoCreateApplication` and `SourceBuildInformation` support.
- **Environments** - `CreateEnvironment`, `UpdateEnvironment`, `TerminateEnvironment`, `DescribeEnvironments`, `DescribeEnvironmentResources`, plus `RebuildEnvironment`, `RestartAppServer`, `AbortEnvironmentUpdate`, `SwapEnvironmentCNAMEs`, `ComposeEnvironments`, `AssociateEnvironmentOperationsRole` / `DisassociateEnvironmentOperationsRole`, and the `RequestEnvironmentInfo` / `RetrieveEnvironmentInfo` bundle-log pair.
- **Real environment lifecycle** - `CreateEnvironment` returns immediately with `Status=Launching`, then a background task settles the environment to `Ready`, exactly as AWS is asynchronous. Updates transition `Updating -> Ready` and terminations `Terminating -> Terminated`. Every transition emits the matching **Event**. In-flight transitions are reconciled on restart, so no environment is ever left stuck.
- **Health, derived not faked** - `DescribeEnvironmentHealth`, `DescribeInstancesHealth`, and the environment `Health` (`Green/Yellow/Red/Grey`) and `HealthStatus` fields are computed from the real modeled environment state.
- **CNAMEs and endpoints** - environment ids are `e-xxxxxxxxxx`, CNAMEs follow the modern `<prefix>.<hash>.<region>.elasticbeanstalk.com` form, and endpoints use the ELB-style `awseb-<id>-<hash>.<region>.elb.amazonaws.com` shape. `CheckDNSAvailability` reports availability against live environments.
- **Configuration** - `CreateConfigurationTemplate`, `UpdateConfigurationTemplate`, `DeleteConfigurationTemplate`, `DescribeConfigurationSettings`, `DescribeConfigurationOptions` (options across namespaces such as `aws:autoscaling:launchconfiguration`, `aws:autoscaling:asg`, and `aws:elasticbeanstalk:environment`), and `ValidateConfigurationSettings`. Option settings apply and remove idempotently on both templates and environments.
- **Events** - `DescribeEvents` with filtering by application, environment, and severity; events are emitted on every environment state transition.
- **Platforms and solution stacks** - `ListAvailableSolutionStacks`, `ListPlatformVersions`, `ListPlatformBranches`, `DescribePlatformVersion`, `CreatePlatformVersion`, `DeletePlatformVersion`.
- **Managed actions** - `DescribeEnvironmentManagedActions`, `DescribeEnvironmentManagedActionHistory`, `ApplyEnvironmentManagedAction`.
- **Account and storage** - `DescribeAccountAttributes` (resource quotas) and `CreateStorageLocation` (the account's managed `elasticbeanstalk-<region>-<account>` bucket).
- **Tags** - `ListTagsForResource`, `UpdateTagsForResource`, and resource tags supplied at create time.

State is account-partitioned and persists across restarts in persistent mode.

## Not implemented

- **Application data plane** - fakecloud models the Elastic Beanstalk management API (the control plane) faithfully, but does not yet spawn a container that serves the deployed application version at the environment endpoint. Health and status are derived from the modeled lifecycle rather than a live application. This mirrors how ELBv2 shipped control-plane-complete with its data plane deferred.
