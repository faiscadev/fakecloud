+++
title = "CloudFormation"
description = "Template parsing, resource provisioning, conditions + intrinsics, nested stacks, SAM transform, drift detection, change sets, stack sets, custom resources."
weight = 13
+++

fakecloud implements **90 of 90** CloudFormation operations at 100% Smithy conformance.

**Status: full API.** Stack lifecycle (create/update/delete with real events), nested stacks, SAM transform, change sets, stack sets + instances, drift detection CRUD, custom resources backed by Lambda, cross-stack exports/imports, and a broad resource-provisioner library that creates real backing state in the other fakecloud services.

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
- **Outputs** — `Outputs.*.Export.Name` registers entries in an account-wide exports registry; `Fn::ImportValue` substitutes them at provision time. Unknown export names fail the create/update with a `ValidationError` ("No export named X found"), and `DeleteStack` blocks while another live stack still imports an export.
- **`Transform: AWS::Serverless-2016-10-31`** — SAM templates are expanded into native CloudFormation resources before provisioning: `AWS::Serverless::Function` -> `AWS::Lambda::Function` (+ role, event sources), `AWS::Serverless::Api` -> `AWS::ApiGateway::RestApi` + deployment, `AWS::Serverless::HttpApi` -> `AWS::ApiGatewayV2::Api`, `AWS::Serverless::SimpleTable` -> `AWS::DynamoDB::Table`, `AWS::Serverless::LayerVersion` -> `AWS::Lambda::LayerVersion`, `AWS::Serverless::StateMachine` -> `AWS::StepFunctions::StateMachine`.

## Stack lifecycle

- **`CreateStack` / `UpdateStack` / `DeleteStack`** — drive real provisioning against the other fakecloud services. Resources are created in topological order based on `Ref` / `Fn::GetAtt` / `DependsOn` edges; updates compute a diff and call the per-type updater; deletes walk in reverse order and respect `DeletionPolicy: Retain` / `Snapshot` / `RetainExceptOnCreate` (the physical resource is left in place instead of being destroyed). A resource replaced by an update honors its `UpdateReplacePolicy` the same way, so the old physical resource is preserved when the policy is `Retain` / `Snapshot`. (`Snapshot` is treated as retain — the resource is preserved rather than snapshot-then-deleted.)
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

Full control plane: `CreateStackSet`, `UpdateStackSet`, `DeleteStackSet`, `DescribeStackSet`, `ListStackSets`, plus instance management (`CreateStackInstances`, `UpdateStackInstances`, `DeleteStackInstances`, `DescribeStackInstance`, `ListStackInstances`) and operation tracking (`DescribeStackSetOperation`, `ListStackSetOperations`, `ListStackSetOperationResults`, `StopStackSetOperation`). Self-managed and service-managed permission models both round-trip.

## Nested stacks

`AWS::CloudFormation::Stack` is a real provisioner: the parent fetches `TemplateURL` from the S3 reference (or accepts an inline body), creates a child stack with its own ID/events/exports, and links `Outputs` so the parent's `Fn::GetAtt NestedStack.Outputs.X` resolves to the child's output value. Deleting the parent cascades to children. A snapshot-backed resource inside a nested stack (e.g. an SQS queue synthesized by CDK into a nested stack) is persisted through its owning service's snapshot hook — the persist pass recurses into nested-stack children — so it survives a restart on both create and delete, not just the top-level stack metadata.

## Termination protection

`EnableTerminationProtection` is honored end to end: set it on `CreateStack` (or toggle it with `UpdateTerminationProtection`) and `DescribeStacks` reports it. `DeleteStack` refuses a protected stack with `Stack [name] cannot be deleted while TerminationProtection is enabled` until protection is disabled, matching real CloudFormation.

## Custom resources

`AWS::CloudFormation::CustomResource` and `Custom::*` types invoke the Lambda function referenced by `ServiceToken` with the CFN custom-resource event payload (`RequestType`, `ResponseURL`, `StackId`, `RequestId`, `LogicalResourceId`, `ResourceProperties`, `OldResourceProperties` on update). The provisioner POSTs to the response URL on the function's behalf if the function doesn't, so simple custom resources work even when the user code forgets to signal.

## Drift detection

`DetectStackDrift`, `DetectStackResourceDrift`, `DescribeStackDriftDetectionStatus`, `DetectStackSetDrift`. Detection runs synchronously and reports `IN_SYNC` for every resource — fakecloud is the source of truth for the backing state, so real drift never occurs. The detection IDs, statuses, and timestamps round-trip through the API for tooling that polls them.

## Type registry, hooks, publishing

`RegisterType`, `DescribeType`, `DeregisterType`, `ListTypes`, `ListTypeVersions`, `ListTypeRegistrations`, `DescribeTypeRegistration`, `SetTypeDefaultVersion`, `SetTypeConfiguration`, `BatchDescribeTypeConfigurations`, `PublishType`, `TestType`, `ActivateType`, `DeactivateType`, `ActivateOrganizationsAccess`, `DeactivateOrganizationsAccess`, `DescribeOrganizationsAccess`. Registrations are recorded; resource types registered here are not actually invoked during stack provisioning (only the built-in types listed below provision real state).

## Resource provisioners

Resources of these types create real backing state in the corresponding fakecloud service. Any other resource type — including real AWS types fakecloud doesn't model (e.g. `AWS::CloudFormation::WaitConditionHandle`) — is accepted and recorded as provisioned without allocating underlying state, rather than failing the stack; `Ref` on it resolves to its logical ID. Dependent operations that need real backing state may still fail.

For **container-backed** services, a CloudFormation-provisioned resource is backed by the same **real container** the direct API spawns, not phantom metadata. A CFN-created `AWS::RDS::DBInstance` is a genuinely connectable Postgres/MySQL: the record is inserted synchronously (so `Ref`/`GetAtt` resolve during provisioning) and the container boots in the background, so `CreateStack` never blocks on the image pull; the instance flips from `creating` to `available` once the container is up. A CFN-created `AWS::AutoScaling::AutoScalingGroup` likewise reconciles to **real container-backed EC2 instances**: the group record is inserted synchronously (control plane only), then a background task launches its desired capacity through the same `RunInstances` path the direct `CreateAutoScalingGroup` API uses, so the launched instances show up in EC2 `DescribeInstances` instead of being phantom ASG metadata. A CFN-created `AWS::ElastiCache::CacheCluster` or `AWS::ElastiCache::ReplicationGroup` is similarly backed by a **real Redis/Memcached container**: the record is inserted synchronously and the container boots in the background, flipping the resource from `creating` to `available` once it is up, so it is genuinely connectable rather than phantom metadata. A CFN-created `AWS::ECS::Service` likewise launches **real running tasks** to reach its `DesiredCount`: the service record is inserted synchronously, then a background task spawns the container-backed tasks through the same path the direct `CreateService` API uses, so the tasks show up in ECS `ListTasks` / `DescribeTasks` and reach `RUNNING` instead of leaving the service at `running_count` 0. When no container runtime is configured (e.g. CI without Docker/Podman), provisioning degrades to metadata-only, exactly as the direct API does.

`UpdateStack` applies in-place property changes through the same persistence path the direct API uses, so the change actually reaches the owning service and its `Get`/`Describe` reflects the new value. This covers common freely-mutable types including `AWS::SSM::Parameter` (Value), `AWS::Logs::LogGroup` (RetentionInDays), `AWS::Kinesis::Stream` (ShardCount / RetentionPeriodHours), `AWS::Events::Rule` (ScheduleExpression / State / Targets), `AWS::DynamoDB::Table` (BillingMode / throughput / GSI), `AWS::SNS::Subscription` (filter/delivery attributes), `AWS::SecretsManager::Secret` (SecretString / Description), `AWS::Cognito::UserPool` and `AWS::Cognito::UserPoolClient`, and `AWS::RDS::DBInstance` (the `ModifyDBInstance`-mutable subset), alongside the types already updatable (Lambda, IAM, API Gateway, SQS, SNS topics, S3, CloudWatch, ELBv2, and more). Properties that require resource replacement in real CloudFormation are left to a future replacement path rather than partially mutated.

- **API Gateway v1** — `RestApi`, `Resource`, `Method`, `Model`, `RequestValidator`, `Authorizer`, `Deployment`, `Stage`, `ApiKey`, `UsagePlan`, `UsagePlanKey`, `DomainName`, `BasePathMapping`, `GatewayResponse`
- **API Gateway v2** — `Api`, `Stage`, `Route`, `RouteResponse`, `Integration`, `IntegrationResponse`, `Authorizer`, `Deployment`, `Model`, `DomainName`, `ApiMapping`, `VpcLink`
- **Application Auto Scaling** — `ScalableTarget`, `ScalingPolicy`
- **Athena** — `WorkGroup`, `DataCatalog`, `NamedQuery`, `PreparedStatement`
- **Auto Scaling** — `LaunchConfiguration`, `AutoScalingGroup` (the group reconciles its `DesiredCapacity` to real container-backed EC2 instances)
- **ACM** — `Certificate`, `Account`
- **CloudFormation** — `Stack` (nested), `CustomResource` / `Custom::*`
- **CloudFront** — `Distribution`, `Function`, `CachePolicy`, `OriginRequestPolicy`, `ResponseHeadersPolicy`, `KeyGroup`, `PublicKey`, `OriginAccessControl`, `CloudFrontOriginAccessIdentity`
- **CloudWatch** — `Alarm`, `Dashboard`
- **Cognito** — `UserPool`, `UserPoolClient`, `UserPoolDomain`, `IdentityPool`, `IdentityPoolRoleAttachment`
- **DynamoDB** — `Table`
- **EC2** — `VPC`, `Subnet`, `SecurityGroup` (including inline `SecurityGroupIngress` / `SecurityGroupEgress` rules), `InternetGateway`, `RouteTable`, `Instance`. VPC `EnableDnsSupport` / `EnableDnsHostnames` and subnet `MapPublicIpOnLaunch` are applied; `Fn::GetAtt` on a subnet resolves `VpcId` / `CidrBlock`. An `Instance` is launched through the same real `RunInstances` path the direct API uses and carries `MetadataOptions`, `IamInstanceProfile`, `EbsOptimized`, and `Monitoring` through to `DescribeInstances`
- **DocDB** — `DBCluster` (routed through the real `CreateDBCluster`; `Fn::GetAtt` resolves `Endpoint`, `ReadEndpoint`, `Port`, `ClusterResourceId`)
- **Neptune** — `DBCluster` (routed through the real `CreateDBCluster`; `Fn::GetAtt` resolves `Endpoint`, `ReadEndpoint`, `Port`, `ClusterResourceId`)
- **ECR** — `Repository`, `RepositoryPolicy`, `LifecyclePolicy`, `PullThroughCacheRule`, `RegistryPolicy`, `RegistryScanningConfiguration`, `ReplicationConfiguration`
- **ECS** — `Cluster`, `Service` (launches real running tasks to reach `DesiredCount`), `TaskDefinition`, `CapacityProvider`
- **EKS** — `Cluster`, `Nodegroup`, `FargateProfile`, `Addon`, `AccessEntry`, `IdentityProviderConfig`, `PodIdentityAssociation`
- **ElastiCache** — `CacheCluster` and `ReplicationGroup` (backed by a real Redis/Memcached container), `ParameterGroup`, `SubnetGroup`, `SecurityGroup`, `User`, `UserGroup`
- **ELBv2** — `LoadBalancer`, `Listener`, `ListenerRule`, `ListenerCertificate`, `TargetGroup`, `TrustStore`
- **EventBridge** — `EventBus`, `Rule`, `Archive`, `Connection`, `ApiDestination`, `Endpoint`, `EventBusPolicy`
- **Firehose** — `DeliveryStream`
- **Glue** — `Database`, `Table`, `Partition`
- **IAM** — `Role`, `User`, `Group`, `Policy`, `ManagedPolicy`, `AccessKey`, `InstanceProfile`, `OIDCProvider`, `SAMLProvider`, `ServiceLinkedRole`, `UserToGroupAddition`, `VirtualMFADevice`
- **Kinesis** — `Stream`, `StreamConsumer`
- **Kinesis Analytics v2 (Managed Service for Apache Flink)** — `Application`, `ApplicationOutput`, `ApplicationReferenceDataSource`, `ApplicationCloudWatchLoggingOption`
- **KMS** — `Key`, `Alias`, `ReplicaKey`
- **MSK (Managed Streaming for Apache Kafka)** — `Cluster`, `ServerlessCluster`, `Configuration`, `ClusterPolicy`, `BatchScramSecret`, `VpcConnection`, `Replicator`
- **Lambda** — `Function`, `Version`, `Alias`, `LayerVersion`, `Permission`, `EventSourceMapping`, `Url`
- **CloudWatch Logs** — `LogGroup`, `LogStream`, `MetricFilter`, `SubscriptionFilter`, `Destination`, `ResourcePolicy`, `QueryDefinition`, `Delivery`, `DeliverySource`, `DeliveryDestination`
- **Organizations** — `Organization`, `OrganizationalUnit`, `Account`, `Policy`, `ResourcePolicy`
- **RDS** — `DBInstance`, `DBCluster`, `DBParameterGroup`, `DBClusterParameterGroup`, `DBSubnetGroup`, `DBSecurityGroup`, `OptionGroup`, `DBProxy`, `EventSubscription`
- **Redshift** — `Cluster` (routed through the real `CreateCluster`; `Fn::GetAtt` resolves `Endpoint.Address`, `Endpoint.Port`, `Id`)
- **Route 53** — `HostedZone`, `RecordSet`, `HealthCheck`, `DNSSEC`, `KeySigningKey`
- **S3** — `Bucket`, `BucketPolicy`
- **Secrets Manager** — `Secret`, `ResourcePolicy`, `RotationSchedule`, `SecretTargetAttachment`
- **Service Discovery (Cloud Map)** — `HttpNamespace`, `PublicDnsNamespace`, `PrivateDnsNamespace`, `Service`, `Instance`
- **SES v2** — `EmailIdentity`, `ConfigurationSet`, `ConfigurationSetEventDestination`, `ContactList`, `DedicatedIpPool`, `ReceiptFilter`, `ReceiptRule`, `ReceiptRuleSet`, `Template`, `VdmAttributes`
- **SNS** — `Topic`, `TopicPolicy`, `Subscription`
- **SQS** — `Queue`, `QueuePolicy`
- **SSM** — `Parameter`
- **Step Functions** — `StateMachine`, `StateMachineVersion`, `StateMachineAlias`, `Activity`
- **WAFv2** — `WebACL`, `WebACLAssociation`, `IPSet`, `RegexPatternSet`, `RuleGroup`, `LoggingConfiguration`

### Fn::GetAtt coverage

The provisioners populate the AWS-documented attribute set for each type, so `Fn::GetAtt` on common shapes works without templates having to fall back to `Ref` plus string surgery. SES email identities expose `DkimDNSTokenName1/2/3` + `DkimDNSTokenValue1/2/3`; WAFv2 web ACLs expose `Arn`, `Id`, `Capacity`, `LabelNamespace`; ELBv2 load balancers expose `DNSName`, `CanonicalHostedZoneID`, `LoadBalancerFullName`, `SecurityGroups`; EKS clusters expose `Arn`, `Endpoint`, `CertificateAuthorityData`, `ClusterSecurityGroupId`, `OpenIdConnectIssuerUrl`; Cloud Map services expose `Arn`, `Id`, `Name`; Lambda functions expose `Arn`, `FunctionArn`, etc.

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

- **Not every resource type provisions something.** Types in the provisioner list above create real backing state. Anything else (the remaining `AWS::EC2::*` types such as `NatGateway` / `Route`, etc.) is recorded but has no underlying resource, so a follow-up call against that service will 404.
- **Drift always reports IN_SYNC.** fakecloud is the source of truth for backing state, so real drift never occurs. The drift API still round-trips IDs and statuses for tooling that polls them.
- **SAM expansion runs at create time.** A re-uploaded template still requires `Capabilities=[CAPABILITY_AUTO_EXPAND]` on operations that touch transforms.

## Source

- [`crates/fakecloud-cloudformation`](https://github.com/faiscadev/fakecloud/tree/main/crates/fakecloud-cloudformation)
- [AWS CloudFormation API reference](https://docs.aws.amazon.com/AWSCloudFormation/latest/APIReference/Welcome.html)
