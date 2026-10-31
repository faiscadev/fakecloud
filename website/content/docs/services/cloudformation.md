+++
title = "CloudFormation"
description = "Template parsing, resource provisioning, conditions + intrinsics, nested stacks, SAM transform, drift detection, change sets, stack sets, custom resources."
weight = 13
+++

fakecloud implements **90 of 90** CloudFormation operations at 100% Smithy conformance.

**Status: full API.** Stack lifecycle (create/update/delete with real events), nested stacks, SAM transform, change sets, stack sets whose instances provision real stacks across accounts and regions, drift detection, custom resources backed by Lambda, cross-stack exports/imports, and a broad resource-provisioner library that creates real backing state in the other fakecloud services.

## Protocol

Query protocol. Form-encoded body, `Action` parameter, XML responses. Templates accepted as JSON or YAML on `CreateStack`, `UpdateStack`, `CreateChangeSet`, `ValidateTemplate`, etc. YAML templates may write intrinsics in either the long form (`Fn::Sub:`) or CloudFormation's short-form node tags (`!Sub`) — see below.

## Template engine

- **Parameters** — typed, with `AllowedValues`, `AllowedPattern`, `MinLength`/`MaxLength`, `MinValue`/`MaxValue`, `NoEcho`, and default substitution.
- **Mappings** — two-level lookup via `Fn::FindInMap`.
- **Conditions** — top-level `Conditions` block evaluated to booleans, with cross-condition references resolved in dependency order (circular refs return `ValidationError`). Resources, outputs, and properties carrying a `Condition` key are pruned when the condition is false.
- **Intrinsics** — `Ref`, `Fn::GetAtt`, `Fn::Sub`, `Fn::Join`, `Fn::Split`, `Fn::Select`, `Fn::FindInMap`, `Fn::Base64`, `Fn::Cidr`, `Fn::Length`, `Fn::ToJsonString`, `Fn::GetAZs`, `Fn::ImportValue`, and the condition intrinsics `Fn::If`, `Fn::Equals`, `Fn::And`, `Fn::Or`, `Fn::Not`. `Fn::Transform` (the macro call) is parsed in both spellings but not expanded — the `AWS::Serverless-2016-10-31` transform below is handled separately.
- **YAML short-form tags** — every intrinsic above, plus the `Rules`-section functions (`Fn::Contains`, `Fn::EachMemberEquals`, `Fn::EachMemberIn`, `Fn::RefAll`, `Fn::ValueOf`, `Fn::ValueOfAll`), is also accepted in its short form (`!Ref`, `!GetAtt Res.Attr`, `!Sub`, `!Join`, `!Select`, `!Split`, `!Base64`, `!Cidr`, `!FindInMap`, `!GetAZs`, `!ImportValue`, `!Length`, `!ToJsonString`, `!Transform`, `!If`, `!Equals`, `!And`, `!Or`, `!Not`, `!Condition`), nested to any depth. Short and long forms are equivalent and can be mixed in one template.
- **`Fn::If`** — evaluated inline anywhere a value can appear, including inside resource properties, output values, and nested intrinsics. The `AWS::NoValue` pseudo-parameter prunes the surrounding key.
- **`Fn::And` / `Fn::Or`** — accept 1-10 sub-conditions and short-circuit on the first decisive value, matching AWS's documented evaluation order.
- **Outputs** — `Outputs.*.Export.Name` registers entries in the exports registry of the stack's account and region; `Fn::ImportValue` substitutes them at provision time and, as in AWS, only resolves exports of the importing stack's own region. The same export name can be exported independently in two regions. Unknown export names fail the create/update with a `ValidationError` ("No export named X found"), and `DeleteStack` blocks while another live stack still imports an export.
- **`Transform: AWS::Serverless-2016-10-31`** — SAM templates are expanded into native CloudFormation resources before provisioning: `AWS::Serverless::Function` -> `AWS::Lambda::Function` (+ role, event sources), `AWS::Serverless::Api` -> `AWS::ApiGateway::RestApi` + deployment + stage, `AWS::Serverless::HttpApi` -> `AWS::ApiGatewayV2::Api` + stage, `AWS::Serverless::SimpleTable` -> `AWS::DynamoDB::Table`, `AWS::Serverless::LayerVersion` -> `AWS::Lambda::LayerVersion`, `AWS::Serverless::StateMachine` -> `AWS::StepFunctions::StateMachine`. Details:
  - **APIs** - a REST API is built as an OpenAPI definition (its `DefinitionBody`, or one generated with the stack name as title) that the `RestApi` imports; `DefinitionUri` becomes `BodyS3Location`. Function `Api` events land on the API their `RestApiId` names (or the implicit `ServerlessRestApi`, stage `Prod`), with a Lambda proxy integration and invoke permission. `Auth` (Lambda TOKEN/REQUEST and Cognito authorizers, `DefaultAuthorizer`, `ApiKeyRequired`, `AWS_IAM`) and `Cors` (MOCK `OPTIONS` preflight methods) are honored. `HttpApi` events add an integration and route (`$default` when no path is given) on the API their `ApiId` names, with JWT / Lambda authorizers and `CorsConfiguration`. `Ref: MyApi.Stage` / `MyApi.Deployment` resolve to the generated stage / deployment.
  - **Events** - `Schedule`, `ScheduleV2`, `EventBridgeRule`, `CloudWatchEvent`, `SQS`, `SNS`, `Kinesis`, `DynamoDB`, `MSK`, `MQ`, `SelfManagedKafka`, `DocumentDB`, `S3` (a `LambdaConfiguration` on the referenced bucket), `Cognito` (the user pool's `LambdaConfig` trigger), `CloudWatchLogs` (a subscription filter), `IoTRule` (an `AWS::IoT::TopicRule`), `AlexaSkill`, `Api` and `HttpApi`, each with the `Lambda::Permission` its source needs. An event type SAM does not define, or an event referencing something SAM cannot use (an `S3` event's bucket outside the template, say), fails the stack.
  - **Versions and URLs** - `AutoPublishAlias` publishes an `AWS::Lambda::Version` (`<Function>Version<hash>`, retained) and an `<Function>Alias<name>` alias that every event source then targets; `Ref: MyFunction.Alias` / `MyFunction.Version` resolve to them. `ProvisionedConcurrencyConfig` goes on the alias. `DeploymentPreference` adds the CodeDeploy application (`ServerlessDeploymentApplication`) and `<Function>DeploymentGroup`; the alias moves to the new version as part of the stack update, i.e. traffic shifts all at once (canary/linear shifting and Pre/PostTraffic hooks are not simulated). `FunctionUrlConfig` adds `<Function>Url` (with a public invoke permission for `AuthType: NONE`).

## Stack lifecycle

- **Regional** - stacks live in the region the request is sent to, as in AWS. The same stack name can exist independently in two regions of one account; `DescribeStacks`, `ListStacks`, change sets, stack events, resources, templates, policies and `DeleteStack` only see the request region's stacks, and a stack ID (ARN) from another region or account answers `ValidationError` ("Stack with id ... does not exist") on every operation that takes a `StackName` (and `CreateStackSet`'s `StackId`), including `DeleteStack`, `UpdateStack` and `CreateChangeSet`. `ImportStacksToStackSet` is the exception: its `StackIds` name stacks in the accounts and regions they live in. A stack ID in the request region works wherever a stack name does and addresses exactly the stack it identifies: once a stack is deleted and its name reused, its old ID still reaches only the deleted stack (its events, a no-op delete), never the new one. A stack ID never stands in for a name to create a stack. Stack sets, registry types and the other per-account CloudFormation records are regional too, except Organizations trusted access (`ActivateOrganizationsAccess`), which is account-level and shared by every region. State persisted by an older build is migrated on load into the region named in each stack's ARN.
- **`CreateStack` / `UpdateStack` / `DeleteStack`** — drive real provisioning against the other fakecloud services. Resources are created in topological order based on `Ref` / `Fn::GetAtt` / `DependsOn` edges; updates compute a diff and call the per-type updater; deletes walk in reverse order and respect `DeletionPolicy: Retain` / `Snapshot` / `RetainExceptOnCreate` (the physical resource is left in place instead of being destroyed). Creating a resource whose explicit name is already taken (a table, bucket, queue, topic or function a deleted stack retained, say) fails it with "already exists" and rolls the stack back, as in AWS, rather than overwriting the existing resource. A resource replaced by an update honors its `UpdateReplacePolicy` the same way, so the old physical resource is preserved when the policy is `Retain` / `Snapshot`. (`Snapshot` is treated as retain — the resource is preserved rather than snapshot-then-deleted.)
- **Generated names** — a resource whose name property the template leaves out is named the way CloudFormation names it: `{StackName}-{LogicalId}-{SUFFIX}`, with a random 13-character suffix, truncated to the type's name limit and lowercased where the type requires it (S3 buckets, RDS and ElastiCache identifiers, ECR repositories, ...). State machines and secrets are named `{LogicalId}-{SUFFIX}`, and a nested stack `{ParentStack}-{LogicalId}-{SUFFIX}`. Stacks built from the same template therefore never collide. An update that still leaves the name out keeps the resource's name; a replacement gets a new one. An unnamed FIFO queue (`FifoQueue: true`) ends in `.fifo`.
- **Stack events** — each stage transition (`CREATE_IN_PROGRESS`, `CREATE_COMPLETE`, `UPDATE_ROLLBACK_*`, `DELETE_*`, etc.) emits a real `StackEvent` with timestamp, logical/physical IDs, and resource type. A failing transition also carries `ResourceStatusReason`. `DescribeStackEvents` returns them in reverse-chronological order, matching AWS.
- **Failure reporting** — a template that is a CloudFormation document but cannot be parsed (a syntax error, a resource with no `Type`, an unresolvable condition, malformed `Fn::ForEach`) is rejected up front with a `ValidationError` naming the problem, and no stack record is created — so fixing the template and redeploying under the same name just works. The same applies to a `TemplateURL` that is unusable or resolves to nothing. `CREATE_FAILED` is reserved for failures that happen once provisioning has begun, and carries the reason in `StackStatusReason`; `DescribeStacks` and `ListStacks` surface it whenever a stack has one, and `DescribeStackEvents` carries `ResourceStatusReason` on the failing event.
- **`DescribeStacks` / `DescribeStackResource` / `DescribeStackResources` / `ListStackResources`** — read from persisted state, including the resolved physical ID for every provisioned resource.
- **`ContinueUpdateRollback` / `CancelUpdateStack` / `RollbackStack`** — accepted and transition the stack through the rollback states.
- **`GetTemplate` / `GetTemplateSummary`** — `GetTemplate` round-trips the original body. `GetTemplateSummary` parses it and reports the declared parameters (type, default, `NoEcho`, description and `AllowedValues` constraints), the required capabilities with a `CapabilitiesReason` naming the resources that forced them (or the transforms, when a template declares one but has no IAM resource), the resource types, declared transforms, template version and metadata. The template can come from `TemplateBody`, `TemplateURL`, an existing `StackName` (by name or ARN), or a `StackSetName`; an unknown stack set reports `StackSetNotFoundException`.
- **`ValidateTemplate`** — parses YAML/JSON (short forms included) and reports the declared parameters, the template description, the required capabilities and the transform list:

  - `CAPABILITY_IAM` when the template contains an `AWS::IAM::*` resource. A SAM function with no explicit `Role` counts, because the transform expands it into an `AWS::IAM::Role`.
  - `CAPABILITY_NAMED_IAM` when one of those resources carries a *custom* name — a name property AWS documents as optional, which CloudFormation would otherwise have generated. A required name property (`AWS::IAM::Policy.PolicyName`) is not a custom name, so a role plus an inline policy needs only `CAPABILITY_IAM`.
  - `CAPABILITY_AUTO_EXPAND` for a declared `Transform`. A template that cannot be parsed is reported as a `ValidationError` naming the problem, rather than validating clean — a validator that always passes is worse than none, since it is trusted.

The structural check is deliberately no stricter than the deploy path: it never rejects a template `CreateStack` would accept. `Fn::ForEach` entries are expanded first, a resource carrying a `Condition` is exempt from the `Type` check, and an empty `Resources` map passes — because each of those deploys. CDK and `sam deploy` call `GetTemplateSummary` during a deploy, so rejecting more than `CreateStack` does would break the deploy before it starts.

## Change sets

`CreateChangeSet`, `DescribeChangeSet`, `ListChangeSets`, `ExecuteChangeSet`, `DeleteChangeSet`. Change-set creation runs the template diff against the current stack state and records per-resource `Action` (`Add`, `Modify`, `Remove`) and `Replacement` flags. `ExecuteChangeSet` runs the recorded plan and emits the same `StackEvent` stream a normal update would.

## Stack sets

`CreateStackSet`, `UpdateStackSet`, `DeleteStackSet`, `DescribeStackSet`, `ListStackSets`, `CreateStackInstances`, `UpdateStackInstances`, `DeleteStackInstances`, `DescribeStackInstance`, `ListStackInstances`, `ImportStacksToStackSet`, `ListStackSetAutoDeploymentTargets`, and operation tracking via `DescribeStackSetOperation`, `ListStackSetOperations`, `ListStackSetOperationResults`, `StopStackSetOperation`.

Stack instances are **real stacks**. `CreateStackInstances` creates a `StackSet-<name>-<uuid>` stack in every target account and region through the same path `CreateStack` uses, so the template's resources exist in the target account's backing services (a queue shows up in that account's `ListQueues`) and the stack is visible to `DescribeStacks` in that account and region. The stack ID carries the target region and account. The stack set itself lives in the region it was created in, which is the region its stack set APIs are called in.

- **Parameters** - the stack set's `Parameters` apply to every instance; `ParameterOverrides` on `CreateStackInstances` / `UpdateStackInstances` override them per instance. `UsePreviousValue` keeps an override, and leaving a parameter out of the list reverts it to the stack set's value. Overriding a parameter the template does not declare is a `ValidationError`.
- **Updates** - `UpdateStackSet` stores the new template, parameters, capabilities and tags, then updates every instance's stack (or only the instances named by `Accounts`/`DeploymentTargets` + `Regions`, leaving the rest `OUTDATED`). `UpdateStackInstances` redeploys just the named instances.
- **Deletes** - `DeleteStackInstances` deletes each instance's stack and its resources, or with `RetainStacks=true` leaves the stack in place and only removes the instance. A stack that cannot be deleted (termination protection, say) leaves its instance `INOPERABLE`. `DeleteStackSet` refuses a stack set that still has instances (`StackSetNotEmptyException`); a deleted stack set stays listable as `DELETED` and describable by its ID, and its name is free for reuse.
- **Operations** - as in AWS, a mutating call returns its `OperationId` straight away and the deployment runs in the background; poll `DescribeStackSetOperation` until it leaves `RUNNING`. Every operation records a per-target result (`Account`, `Region`, `Status`, `StatusReason`, `AccountGateResult`). Targets deploy in `RegionOrder` then request order. A failing target counts against `FailureToleranceCount` / `FailureTolerancePercentage` per region; once the tolerance is exceeded the remaining targets are `CANCELLED` and the operation ends `FAILED`. Stacks that provision asynchronously (templates with custom resources) leave the operation `RUNNING` until they settle; `StopStackSetOperation` cancels the targets that have not started. A second operation while one is running is `OperationInProgressException`, and a reused `OperationId` is `OperationIdAlreadyExistsException`. An operation cut short by a restart is settled as `FAILED` when state is loaded, so it does not block the stack set.
- **Account gate** - when a target account has a Lambda named `AWSCloudFormationStackSetAccountGate`, it is invoked before deploying and the deployment only proceeds if it returns `{"Status": "SUCCEEDED"}`. Without the function the gate is `SKIPPED`.
- **Service-managed** - `PermissionModel=SERVICE_MANAGED` needs an organization with StackSets trusted access (`ActivateOrganizationsAccess`, or Organizations `EnableAWSServiceAccess` for `member.org.stacksets.cloudformation.amazonaws.com`). `DeploymentTargets.OrganizationalUnitIds` resolve to the accounts in those OUs and every OU nested below them, never the management account; `AccountFilterType` (`INTERSECTION`, `DIFFERENCE`, `UNION`, `NONE`) combines them with `DeploymentTargets.Accounts` or an `AccountsUrl` file in S3. Suspended accounts are recorded as `SKIPPED_SUSPENDED_ACCOUNT`. `CallAs=DELEGATED_ADMIN` works from an account registered as a StackSets delegated administrator and acts on the management account's stack sets.
- **Auto-deployment** - with `AutoDeployment.Enabled=true`, a service-managed stack set follows the organization. An account that joins one of the OUs the stack set is deployed to (directly, by being moved there, created there, or invited into the organization) gains that stack set's instances in that OU's regions, recorded as an ordinary `CREATE` operation. An account that leaves loses them: its instances are deleted with their stacks, or, with `RetainStacksOnAccountRemoval=true`, only the instances are dropped and the stacks stay behind in the account. The OUs and regions are the stack set's own record of where it deploys, set by `CreateStackInstances` and cleared when `DeleteStackInstances` empties an OU of a region, so an OU that momentarily holds no accounts stays a target; `ListStackSetAutoDeploymentTargets` reports them. Instances left out on purpose stay out: an account an `AccountFilterType` excluded when the instances were created, or one whose instance was removed with `DeleteStackInstances`, is not re-added while it stays in the OU that decision was made under (leaving, or moving to another target OU, deploys to it again). The decision is per OU and per region, so removing an instance in one region does not stop the OU's other regions deploying to that account. A stack that creates an account itself (`AWS::Organizations::Account`) triggers the same deployment. Deployment runs before the Organizations call that triggered it returns, so an account is fully provisioned by the time `MoveAccount`, `AcceptHandshake` or `RemoveAccountFromOrganization` answers (`CreateAccount` enrolls the account asynchronously, as in AWS, so its deployment follows the request reaching `SUCCEEDED` rather than preceding it); a stack set that is busy with another operation is never waited on inline — it is re-planned in the background once that operation finishes. A template whose resources take a long time to provision makes the triggering Organizations call wait too, but only up to 20 seconds; past that the deployment carries on in the background, as AWS does from the start.
- **Import** - `ImportStacksToStackSet` adopts existing stacks (by `StackIds` or a `StackIdsUrl` file) as instances without redeploying them. A stack already managed by a stack set is refused, and a stack whose template differs from the stack set's is recorded as `FAILED_IMPORT`. `CreateStackSet` with `StackId` starts a stack set from a stack's template and parameters.
- **Drift** - `DetectStackSetDrift` checks each instance's stack resources against the live backing services, sets each instance's `DriftStatus`, and reports the counts in `StackSetDriftDetectionDetails`; `ListStackInstanceResourceDrifts` lists the per-resource results for an operation.

Execution roles (`AdministrationRoleARN`, `ExecutionRoleName`) are recorded and reported, with the AWS defaults for self-managed stack sets, but not required to exist in the target account.

## Nested stacks

`AWS::CloudFormation::Stack` is a real provisioner: the parent fetches `TemplateURL` from the S3 reference (or accepts an inline body), creates a child stack with its own ID/events/exports, and links `Outputs` so the parent's `Fn::GetAtt NestedStack.Outputs.X` resolves to the child's output value. Deleting the parent cascades to children. A snapshot-backed resource inside a nested stack (e.g. an SQS queue synthesized by CDK into a nested stack) is persisted through its owning service's snapshot hook — the persist pass recurses into nested-stack children — so it survives a restart on both create and delete, not just the top-level stack metadata.

## Termination protection

`EnableTerminationProtection` is honored end to end: set it on `CreateStack` (or toggle it with `UpdateTerminationProtection`) and `DescribeStacks` reports it. `DeleteStack` refuses a protected stack with `Stack [name] cannot be deleted while TerminationProtection is enabled` until protection is disabled, matching real CloudFormation.

## Custom resources

`AWS::CloudFormation::CustomResource` and `Custom::*` types invoke the Lambda function referenced by `ServiceToken` with the CFN custom-resource event payload (`RequestType`, `ResponseURL`, `StackId`, `RequestId`, `LogicalResourceId`, `ResourceProperties`, `OldResourceProperties` on update). The provisioner POSTs to the response URL on the function's behalf if the function doesn't, so simple custom resources work even when the user code forgets to signal.

## Drift detection

`DetectStackDrift`, `DetectStackResourceDrift`, `DescribeStackDriftDetectionStatus`, `DescribeStackResourceDrifts`, and `DetectStackSetDrift` (see stack sets). Detection runs synchronously and checks whether each resource's physical resource still exists in its backing service: a resource deleted outside CloudFormation reports `DELETED` and the stack `DRIFTED`. Existence is checked for SQS queues, SNS topics, S3 buckets, Lambda functions, IAM roles, DynamoDB tables, KMS keys and Secrets Manager secrets; other types are assumed in sync, and property-level differences are not compared.

## Type registry, hooks, publishing

`RegisterType`, `DescribeType`, `DeregisterType`, `ListTypes`, `ListTypeVersions`, `ListTypeRegistrations`, `DescribeTypeRegistration`, `SetTypeDefaultVersion`, `SetTypeConfiguration`, `BatchDescribeTypeConfigurations`, `PublishType`, `TestType`, `ActivateType`, `DeactivateType`, `ActivateOrganizationsAccess`, `DeactivateOrganizationsAccess`, `DescribeOrganizationsAccess`. Registrations are recorded; resource types registered here are not actually invoked during stack provisioning (only the built-in types listed below provision real state).

## Resource provisioners

Resources of these types create real backing state in the corresponding fakecloud service. Any other resource type — including real AWS types fakecloud doesn't model (e.g. `AWS::CloudFormation::WaitConditionHandle`) — is accepted and recorded as provisioned without allocating underlying state, rather than failing the stack; `Ref` on it resolves to its logical ID. Dependent operations that need real backing state may still fail.

For **container-backed** services, a CloudFormation-provisioned resource is backed by the same **real container** the direct API spawns, not phantom metadata. A CFN-created `AWS::RDS::DBInstance` is a genuinely connectable Postgres/MySQL: the record is inserted synchronously (so `Ref`/`GetAtt` resolve during provisioning) and the container boots in the background, so `CreateStack` never blocks on the image pull; the instance flips from `creating` to `available` once the container is up. A CFN-created `AWS::AutoScaling::AutoScalingGroup` likewise reconciles to **real container-backed EC2 instances**: the group record is inserted synchronously (control plane only), then a background task launches its desired capacity through the same `RunInstances` path the direct `CreateAutoScalingGroup` API uses, so the launched instances show up in EC2 `DescribeInstances` instead of being phantom ASG metadata. A CFN-created `AWS::ElastiCache::CacheCluster` or `AWS::ElastiCache::ReplicationGroup` is similarly backed by a **real Redis/Memcached container**: the record is inserted synchronously and the container boots in the background, flipping the resource from `creating` to `available` once it is up, so it is genuinely connectable rather than phantom metadata. A CFN-created `AWS::ECS::Service` likewise launches **real running tasks** to reach its `DesiredCount`: the service record is inserted synchronously, then a background task spawns the container-backed tasks through the same path the direct `CreateService` API uses, so the tasks show up in ECS `ListTasks` / `DescribeTasks` and reach `RUNNING` instead of leaving the service at `running_count` 0. When no container runtime is configured (e.g. CI without Docker/Podman), provisioning degrades to metadata-only, exactly as the direct API does.

`UpdateStack` applies in-place property changes through the same persistence path the direct API uses, so the change actually reaches the owning service and its `Get`/`Describe` reflects the new value. This covers common freely-mutable types including `AWS::SSM::Parameter` (Value), `AWS::Logs::LogGroup` (RetentionInDays), `AWS::Kinesis::Stream` (ShardCount, resharded like `UpdateShardCount` so existing records stay readable in the closed parent shards / RetentionPeriodHours / StreamEncryption / Tags), `AWS::SQS::Queue` (the attribute set is rebuilt from the template, so a dropped property returns to its default and an added `RedrivePolicy` routes to the DLQ), `AWS::SNS::Topic` (attributes, Tags and the inline `Subscription` list), `AWS::Events::Rule` (ScheduleExpression / State / Targets), `AWS::DynamoDB::Table` (BillingMode / throughput / GSI), `AWS::SNS::Subscription` (filter/delivery attributes), `AWS::SecretsManager::Secret` (SecretString / Description), `AWS::Cognito::UserPool`, `AWS::Cognito::UserPoolClient` and `AWS::Cognito::IdentityPool` (for the client and identity pool, a property dropped from the template returns to its default, as their Update APIs reset what a request omits; a dropped `ClientName` or `IdentityPoolName` keeps the current name, and a set `DeveloperProviderName` is kept), and `AWS::RDS::DBInstance` (the `ModifyDBInstance`-mutable subset), alongside the types already updatable (Lambda, IAM, API Gateway, SQS, SNS topics, S3, CloudWatch, ELBv2, and more). Properties that require resource replacement in real CloudFormation are left to a future replacement path rather than partially mutated.

- **API Gateway v1** — `RestApi`, `Resource`, `Method`, `Model`, `RequestValidator`, `Authorizer`, `Deployment`, `Stage`, `ApiKey`, `UsagePlan`, `UsagePlanKey`, `DomainName`, `BasePathMapping`, `GatewayResponse`
- **API Gateway v2** — `Api`, `Stage`, `Route`, `RouteResponse`, `Integration`, `IntegrationResponse`, `Authorizer`, `Deployment`, `Model`, `DomainName`, `ApiMapping`, `VpcLink`
- **Application Auto Scaling** — `ScalableTarget`, `ScalingPolicy`
- **Athena** — `WorkGroup`, `DataCatalog`, `NamedQuery`, `PreparedStatement`
- **Auto Scaling** — `LaunchConfiguration` (including `BlockDeviceMappings` and `MetadataOptions`), `AutoScalingGroup` (the group reconciles its `DesiredCapacity` to real container-backed EC2 instances launched through `RunInstances` from its launch configuration, `LaunchTemplate` or `MixedInstancesPolicy`, with its `Tags` propagated at launch; a missing launch configuration or template fails the resource)
- **ACM** — `Certificate`, `Account`
- **Batch** - `ComputeEnvironment`, `JobQueue`, `JobDefinition`, `SchedulingPolicy`, `ConsumableResource`, `ServiceEnvironment`, `QuotaShare` (the last three through the Batch API handlers; `Ref` is the ARN)
- **CloudFormation** — `Stack` (nested), `CustomResource` / `Custom::*`
- **CloudFront** — `Distribution`, `Function`, `CachePolicy`, `OriginRequestPolicy`, `ResponseHeadersPolicy`, `KeyGroup`, `PublicKey`, `OriginAccessControl`, `CloudFrontOriginAccessIdentity`
- **CloudWatch** — `Alarm`, `Dashboard`
- **Cognito** — `UserPool`, `UserPoolClient`, `UserPoolDomain`, `IdentityPool`, `IdentityPoolRoleAttachment`
- **DynamoDB** — `GlobalTable` (the local replica table plus its replication group; `Ref` is the table name), `Table` (`Ref` is the table name; `BillingMode` defaults to `PROVISIONED`, as in CloudFormation; `TimeToLiveSpecification`, `PointInTimeRecoverySpecification`, `KinesisStreamSpecification` and, on update, `StreamSpecification` are applied, so `DescribeTimeToLive` / `DescribeContinuousBackups` / `DescribeKinesisStreamingDestination` reflect them)
- **EC2** — `VPC`, `Subnet`, `SecurityGroup` (including inline `SecurityGroupIngress` / `SecurityGroupEgress` rules), standalone `SecurityGroupIngress` / `SecurityGroupEgress` (`Ref` is the `sgr-` rule id), `InternetGateway`, `VPCGatewayAttachment`, `RouteTable`, `Route`, `EIP` (`Ref` is the public IP; `Fn::GetAtt` `AllocationId`), `NatGateway`, `Instance`, `LaunchTemplate`. VPC `EnableDnsSupport` / `EnableDnsHostnames` and subnet `MapPublicIpOnLaunch` are applied; `Fn::GetAtt` on a subnet resolves `VpcId` / `CidrBlock`. An `Instance` is launched through the same real `RunInstances` path the direct API uses and carries `MetadataOptions`, `IamInstanceProfile`, `EbsOptimized`, and `Monitoring` through to `DescribeInstances`; with a `LaunchTemplate` property it launches from that template's resolved version with its own properties winning, exactly as `RunInstances` does, and `PropagateTagsToVolumeOnCreation` puts its `Tags` on the launch volumes too. A `LaunchTemplate` is a real EC2 launch template (`Ref` is its id; `Fn::GetAtt` `LaunchTemplateId` / `LatestVersionNumber` / `DefaultVersionNumber`); an update to its `LaunchTemplateData` adds a new version and makes it the default
- **DocDB** — `DBCluster` (routed through the real `CreateDBCluster`; `Fn::GetAtt` resolves `Endpoint`, `ReadEndpoint`, `Port`, `ClusterResourceId`)
- **Neptune** — `DBCluster` (routed through the real `CreateDBCluster`; `Fn::GetAtt` resolves `Endpoint`, `ReadEndpoint`, `Port`, `ClusterResourceId`)
- **ECR** — `Repository`, `RepositoryPolicy`, `LifecyclePolicy`, `PullThroughCacheRule`, `RegistryPolicy`, `RegistryScanningConfiguration`, `ReplicationConfiguration`
- **ECS** — `Cluster`, `Service` (starts its PRIMARY deployment and `DeploymentCircuitBreaker` as `CreateService` does, launches real running tasks to reach `DesiredCount`, and a new `TaskDefinition` on update rolls a new deployment), `TaskDefinition`, `CapacityProvider`
- **EKS** — `Cluster`, `Nodegroup`, `FargateProfile`, `Addon`, `AccessEntry`, `IdentityProviderConfig`, `PodIdentityAssociation`
- **ElastiCache** — `CacheCluster` and `ReplicationGroup` (backed by a real Redis/Memcached container), `ParameterGroup`, `SubnetGroup`, `SecurityGroup`, `User`, `UserGroup`
- **ELBv2** — `LoadBalancer`, `Listener`, `ListenerRule`, `ListenerCertificate`, `TargetGroup`, `TrustStore`
- **EventBridge** — `EventBus`, `Rule` (`Ref` is the rule name, or `<bus>|<rule>` off the default bus), `Archive`, `Connection`, `ApiDestination`, `Endpoint`, `EventBusPolicy`
- **Firehose** — `DeliveryStream`
- **Glue** — `Database`, `Table`, `Partition`
- **IAM** — `Role`, `User`, `Group`, `Policy` (an inline policy embedded in each listed role, user and group), `RolePolicy`, `ManagedPolicy`, `AccessKey`, `InstanceProfile`, `OIDCProvider`, `SAMLProvider`, `ServiceLinkedRole`, `UserToGroupAddition`, `VirtualMFADevice`
- **IoT** - `TopicRule` (read back by `GetTopicRule` / `ListTopicRules`)
- **Kinesis** — `Stream`, `StreamConsumer`
- **Kinesis Analytics v2 (Managed Service for Apache Flink)** — `Application`, `ApplicationOutput`, `ApplicationReferenceDataSource`, `ApplicationCloudWatchLoggingOption`
- **KMS** — `Key`, `Alias`, `ReplicaKey`
- **MSK (Managed Streaming for Apache Kafka)** — `Cluster`, `ServerlessCluster`, `Configuration`, `ClusterPolicy`, `BatchScramSecret`, `VpcConnection`, `Replicator`
- **Lambda** — `Function` (including `ImageConfig` and `ReservedConcurrentExecutions`), `Version`, `Alias`, `LayerVersion`, `Permission`, `EventSourceMapping`, `EventInvokeConfig`, `Url`
- **CloudWatch Logs** — `LogGroup`, `LogStream`, `MetricFilter`, `SubscriptionFilter`, `Destination`, `ResourcePolicy`, `QueryDefinition`, `Delivery`, `DeliverySource`, `DeliveryDestination`
- **Organizations** — `Organization`, `OrganizationalUnit`, `Account`, `Policy`, `ResourcePolicy`
- **RDS** — `DBInstance` (a `DBClusterIdentifier` member joins the cluster's `DBClusterMembers` and takes its engine, credentials and port; `Endpoint.Address` / `Endpoint.Port` resolve live), `DBCluster` (port defaults per engine), `DBParameterGroup`, `DBClusterParameterGroup`, `DBSubnetGroup`, `DBSecurityGroup`, `OptionGroup`, `DBProxy`, `EventSubscription`
- **Redshift** — `Cluster` (routed through the real `CreateCluster`; `Fn::GetAtt` resolves `Endpoint.Address`, `Endpoint.Port`, `Id`)
- **Route 53** — `HostedZone`, `RecordSet`, `HealthCheck`, `DNSSEC`, `KeySigningKey`
- **S3** — `Bucket`, `BucketPolicy`
- **Scheduler** - `Schedule`, `ScheduleGroup`
- **Secrets Manager** — `Secret`, `ResourcePolicy`, `RotationSchedule`, `SecretTargetAttachment`
- **Service Discovery (Cloud Map)** — `HttpNamespace`, `PublicDnsNamespace`, `PrivateDnsNamespace`, `Service`, `Instance`
- **SES v2** — `EmailIdentity`, `ConfigurationSet`, `ConfigurationSetEventDestination`, `ContactList`, `DedicatedIpPool`, `ReceiptFilter`, `ReceiptRule`, `ReceiptRuleSet`, `Template`, `VdmAttributes`
- **SNS** — `Topic`, `TopicPolicy`, `Subscription`
- **SQS** — `Queue`, `QueuePolicy`
- **SSM** — `Parameter`
- **Step Functions** — `StateMachine`, `StateMachineVersion`, `StateMachineAlias`, `Activity`
- **WAFv2** — `WebACL`, `WebACLAssociation`, `IPSet`, `RegexPatternSet`, `RuleGroup`, `LoggingConfiguration`

### Fn::GetAtt coverage

The provisioners populate the AWS-documented attribute set for each type, so `Fn::GetAtt` on common shapes works without templates having to fall back to `Ref` plus string surgery. SES email identities expose `DkimDNSTokenName1/2/3` + `DkimDNSTokenValue1/2/3`; WAFv2 web ACLs expose `Arn`, `Id`, `Capacity`, `LabelNamespace`; ELBv2 load balancers expose `DNSName`, `CanonicalHostedZoneID`, `LoadBalancerFullName`, `SecurityGroups`; EKS clusters expose `Arn`, `Endpoint`, `CertificateAuthorityData`, `ClusterSecurityGroupId`, `OpenIdConnectIssuerUrl`; Cloud Map services expose `Arn`, `Id`, `Name`; Lambda functions expose `Arn`, `FunctionArn`, etc. S3 buckets expose `Arn`, `DomainName`, `RegionalDomainName`, `DualStackDomainName`, and `WebsiteURL`, with hostnames under the stack region's partition DNS suffix (`amazonaws.com.cn` in China, the isolated partitions' own suffixes) and the website endpoint form AWS publishes for the region (`s3-website-<region>` in the legacy regions such as `us-east-1`, `s3-website.<region>` elsewhere).

## Cross-service delivery

- **CloudFormation -> Lambda** — `AWS::CloudFormation::CustomResource` / `Custom::*` invoke via `ServiceToken` and post lifecycle results back on the function's behalf when needed.
- **CloudFormation -> SNS** — stack events notify configured topics via `NotificationARNs` on `CreateStack` / `UpdateStack` / `DeleteStack`.
- **CloudFormation -> S3** — `TemplateURL` is fetched from S3 for both top-level and nested stacks.

## Smoke test

```sh
fakecloud &

cat > template.yaml <<'YAML'
AWSTemplateFormatVersion: '2010-09-09'
Parameters:
  Stage:
    Type: String
    AllowedValues: [dev, prod]
    Default: dev
Conditions:
  IsProd: !Equals [!Ref Stage, prod]
Resources:
  Queue:
    Type: AWS::SQS::Queue
    Properties:
      QueueName: !Sub orders-${Stage}
      VisibilityTimeout: !If [IsProd, 300, 30]
Outputs:
  QueueUrl:
    Value: !Ref Queue
    Export:
      Name: !Sub orders-url-${Stage}
YAML

aws --endpoint-url http://localhost:4566 cloudformation create-stack \
    --stack-name orders --template-body file://template.yaml \
    --parameters ParameterKey=Stage,ParameterValue=prod

aws --endpoint-url http://localhost:4566 cloudformation describe-stack-events \
    --stack-name orders

aws --endpoint-url http://localhost:4566 cloudformation list-exports
```

## Gotchas

- **Not every resource type provisions something.** Types in the provisioner list above create real backing state. Anything else (the remaining `AWS::EC2::*` types such as `TransitGateway` / `VPCEndpoint`, etc.) is recorded but has no underlying resource, so a follow-up call against that service will 404.
- **Drift only detects deleted resources.** A resource removed outside CloudFormation reports `DELETED`; property changes made outside CloudFormation are not compared.
- **Explicit names are per account, not per region.** A resource the template names (`QueueName: orders`) is one resource per account in fakecloud, so deploying that template to two regions of the same account (a stack set, say) collides where AWS would keep the regions apart. Leave the name out, or include `${AWS::Region}` in it.
- **SAM expansion runs at create time.** A re-uploaded template still requires `Capabilities=[CAPABILITY_AUTO_EXPAND]` on operations that touch transforms.

## Source

- [`crates/fakecloud-cloudformation`](https://github.com/faiscadev/fakecloud/tree/main/crates/fakecloud-cloudformation)
- [AWS CloudFormation API reference](https://docs.aws.amazon.com/AWSCloudFormation/latest/APIReference/Welcome.html)
