+++
title = "CloudFormation"
description = "Template parsing, resource provisioning, conditions + intrinsics, nested stacks, SAM transform, drift detection, change sets, stack sets, custom resources."
weight = 13
+++

fakecloud implements **90 of 90** CloudFormation operations at 100% Smithy conformance.

**Status: full API.** Stack lifecycle (create/update/delete with real events), nested stacks, SAM transform, change sets, stack sets + instances, drift detection CRUD, custom resources backed by Lambda, cross-stack exports/imports, and a broad resource-provisioner library that creates real backing state in the other fakecloud services.

## Protocol

Query protocol. Form-encoded body, `Action` parameter, XML responses. Templates accepted as JSON or YAML on `CreateStack`, `UpdateStack`, `CreateChangeSet`, `ValidateTemplate`, etc.

## Template engine

- **Parameters** — typed, with `AllowedValues`, `AllowedPattern`, `MinLength`/`MaxLength`, `MinValue`/`MaxValue`, `NoEcho`, and default substitution.
- **Mappings** — two-level lookup via `Fn::FindInMap`.
- **Conditions** — top-level `Conditions` block evaluated to booleans, with cross-condition references resolved in dependency order (circular refs return `ValidationError`). Resources, outputs, and properties carrying a `Condition` key are pruned when the condition is false.
- **Intrinsics** — `Ref`, `Fn::GetAtt`, `Fn::Sub`, `Fn::Join`, `Fn::Split`, `Fn::Select`, `Fn::FindInMap`, `Fn::Base64`, `Fn::Cidr`, `Fn::Length`, `Fn::ToJsonString`, `Fn::GetAZs`, `Fn::ImportValue`, and the condition intrinsics `Fn::If`, `Fn::Equals`, `Fn::And`, `Fn::Or`, `Fn::Not`.
- **`Fn::If`** — evaluated inline anywhere a value can appear, including inside resource properties, output values, and nested intrinsics. The `AWS::NoValue` pseudo-parameter prunes the surrounding key.
- **`Fn::And` / `Fn::Or`** — accept 1-10 sub-conditions and short-circuit on the first decisive value, matching AWS's documented evaluation order.
- **Outputs** — `Outputs.*.Export.Name` registers entries in an account-wide exports registry; `Fn::ImportValue` substitutes them at provision time. Unknown export names fail the create/update with a `ValidationError` ("No export named X found"), and `DeleteStack` blocks while another live stack still imports an export.
- **`Transform: AWS::Serverless-2016-10-31`** — SAM templates are expanded into native CloudFormation resources before provisioning: `AWS::Serverless::Function` -> `AWS::Lambda::Function` (+ role, event sources), `AWS::Serverless::Api` -> `AWS::ApiGateway::RestApi` + deployment, `AWS::Serverless::HttpApi` -> `AWS::ApiGatewayV2::Api`, `AWS::Serverless::SimpleTable` -> `AWS::DynamoDB::Table`, `AWS::Serverless::LayerVersion` -> `AWS::Lambda::LayerVersion`, `AWS::Serverless::StateMachine` -> `AWS::StepFunctions::StateMachine`.

## Stack lifecycle

- **`CreateStack` / `UpdateStack` / `DeleteStack`** — drive real provisioning against the other fakecloud services. Resources are created in topological order based on `Ref` / `Fn::GetAtt` / `DependsOn` edges; updates compute a diff and call the per-type updater; deletes walk in reverse order and respect `DeletionPolicy: Retain` / `Snapshot`.
- **Stack events** — each stage transition (`CREATE_IN_PROGRESS`, `CREATE_COMPLETE`, `UPDATE_ROLLBACK_*`, `DELETE_*`, etc.) emits a real `StackEvent` with timestamp, logical/physical IDs, resource type, and status reason. `DescribeStackEvents` returns them in reverse-chronological order, matching AWS.
- **`DescribeStacks` / `DescribeStackResource` / `DescribeStackResources` / `ListStackResources`** — read from persisted state, including the resolved physical ID for every provisioned resource.
- **`ContinueUpdateRollback` / `CancelUpdateStack` / `RollbackStack`** — accepted and transition the stack through the rollback states.
- **`GetTemplate` / `GetTemplateSummary`** — round-trip the original body and surface declared parameters, capabilities, resource types, and the resolved transform list.
- **`ValidateTemplate`** — parses YAML/JSON and reports parameter metadata and required capabilities (`CAPABILITY_IAM`, `CAPABILITY_NAMED_IAM`, `CAPABILITY_AUTO_EXPAND`).

## Change sets

`CreateChangeSet`, `DescribeChangeSet`, `ListChangeSets`, `ExecuteChangeSet`, `DeleteChangeSet`. Change-set creation runs the template diff against the current stack state and records per-resource `Action` (`Add`, `Modify`, `Remove`) and `Replacement` flags. `ExecuteChangeSet` runs the recorded plan and emits the same `StackEvent` stream a normal update would.

## Stack sets

Full control plane: `CreateStackSet`, `UpdateStackSet`, `DeleteStackSet`, `DescribeStackSet`, `ListStackSets`, plus instance management (`CreateStackInstances`, `UpdateStackInstances`, `DeleteStackInstances`, `DescribeStackInstance`, `ListStackInstances`) and operation tracking (`DescribeStackSetOperation`, `ListStackSetOperations`, `ListStackSetOperationResults`, `StopStackSetOperation`). Self-managed and service-managed permission models both round-trip.

## Nested stacks

`AWS::CloudFormation::Stack` is a real provisioner: the parent fetches `TemplateURL` from the S3 reference (or accepts an inline body), creates a child stack with its own ID/events/exports, and links `Outputs` so the parent's `Fn::GetAtt NestedStack.Outputs.X` resolves to the child's output value. Deleting the parent cascades to children.

## Custom resources

`AWS::CloudFormation::CustomResource` and `Custom::*` types invoke the Lambda function referenced by `ServiceToken` with the CFN custom-resource event payload (`RequestType`, `ResponseURL`, `StackId`, `RequestId`, `LogicalResourceId`, `ResourceProperties`, `OldResourceProperties` on update). The provisioner POSTs to the response URL on the function's behalf if the function doesn't, so simple custom resources work even when the user code forgets to signal.

## Drift detection

`DetectStackDrift`, `DetectStackResourceDrift`, `DescribeStackDriftDetectionStatus`, `DetectStackSetDrift`. Detection runs synchronously and reports `IN_SYNC` for every resource — fakecloud is the source of truth for the backing state, so real drift never occurs. The detection IDs, statuses, and timestamps round-trip through the API for tooling that polls them.

## Type registry, hooks, publishing

`RegisterType`, `DescribeType`, `DeregisterType`, `ListTypes`, `ListTypeVersions`, `ListTypeRegistrations`, `DescribeTypeRegistration`, `SetTypeDefaultVersion`, `SetTypeConfiguration`, `BatchDescribeTypeConfigurations`, `PublishType`, `TestType`, `ActivateType`, `DeactivateType`, `ActivateOrganizationsAccess`, `DeactivateOrganizationsAccess`, `DescribeOrganizationsAccess`. Registrations are recorded; resource types registered here are not actually invoked during stack provisioning (only the built-in types listed below provision real state).

## Resource provisioners

Resources of these types create real backing state in the corresponding fakecloud service. Any other resource type — including real AWS types fakecloud doesn't model (e.g. `AWS::CloudFormation::WaitConditionHandle`) — is accepted and recorded as provisioned without allocating underlying state, rather than failing the stack; `Ref` on it resolves to its logical ID. Dependent operations that need real backing state may still fail.

- **API Gateway v1** — `RestApi`, `Resource`, `Method`, `Model`, `RequestValidator`, `Authorizer`, `Deployment`, `Stage`, `ApiKey`, `UsagePlan`, `UsagePlanKey`, `DomainName`, `BasePathMapping`, `GatewayResponse`
- **API Gateway v2** — `Api`, `Stage`, `Route`, `RouteResponse`, `Integration`, `IntegrationResponse`, `Authorizer`, `Deployment`, `Model`, `DomainName`, `ApiMapping`, `VpcLink`
- **Application Auto Scaling** — `ScalableTarget`, `ScalingPolicy`
- **Athena** — `WorkGroup`, `DataCatalog`, `NamedQuery`, `PreparedStatement`
- **ACM** — `Certificate`, `Account`
- **CloudFormation** — `Stack` (nested), `CustomResource` / `Custom::*`
- **CloudFront** — `Distribution`, `Function`, `CachePolicy`, `OriginRequestPolicy`, `ResponseHeadersPolicy`, `KeyGroup`, `PublicKey`, `OriginAccessControl`, `CloudFrontOriginAccessIdentity`
- **CloudWatch** — `Alarm`, `Dashboard`
- **Cognito** — `UserPool`, `UserPoolClient`, `UserPoolDomain`, `IdentityPool`, `IdentityPoolRoleAttachment`
- **DynamoDB** — `Table`
- **ECR** — `Repository`, `RepositoryPolicy`, `LifecyclePolicy`, `PullThroughCacheRule`, `RegistryPolicy`, `RegistryScanningConfiguration`, `ReplicationConfiguration`
- **ECS** — `Cluster`, `Service`, `TaskDefinition`, `CapacityProvider`
- **ElastiCache** — `CacheCluster`, `ReplicationGroup`, `ParameterGroup`, `SubnetGroup`, `SecurityGroup`, `User`, `UserGroup`
- **ELBv2** — `LoadBalancer`, `Listener`, `ListenerRule`, `ListenerCertificate`, `TargetGroup`, `TrustStore`
- **EventBridge** — `EventBus`, `Rule`, `Archive`, `Connection`, `ApiDestination`, `Endpoint`, `EventBusPolicy`
- **Firehose** — `DeliveryStream`
- **Glue** — `Database`, `Table`, `Partition`
- **IAM** — `Role`, `User`, `Group`, `Policy`, `ManagedPolicy`, `AccessKey`, `InstanceProfile`, `OIDCProvider`, `SAMLProvider`, `ServiceLinkedRole`, `UserToGroupAddition`, `VirtualMFADevice`
- **Kinesis** — `Stream`, `StreamConsumer`
- **KMS** — `Key`, `Alias`, `ReplicaKey`
- **Lambda** — `Function`, `Version`, `Alias`, `LayerVersion`, `Permission`, `EventSourceMapping`, `Url`
- **CloudWatch Logs** — `LogGroup`, `LogStream`, `MetricFilter`, `SubscriptionFilter`, `Destination`, `ResourcePolicy`, `QueryDefinition`, `Delivery`, `DeliverySource`, `DeliveryDestination`
- **Organizations** — `Organization`, `OrganizationalUnit`, `Account`, `Policy`, `ResourcePolicy`
- **RDS** — `DBInstance`, `DBCluster`, `DBParameterGroup`, `DBClusterParameterGroup`, `DBSubnetGroup`, `DBSecurityGroup`, `OptionGroup`, `DBProxy`, `EventSubscription`
- **Route 53** — `HostedZone`, `RecordSet`, `HealthCheck`, `DNSSEC`, `KeySigningKey`
- **S3** — `Bucket`, `BucketPolicy`
- **Secrets Manager** — `Secret`, `ResourcePolicy`, `RotationSchedule`, `SecretTargetAttachment`
- **SES v2** — `EmailIdentity`, `ConfigurationSet`, `ConfigurationSetEventDestination`, `ContactList`, `DedicatedIpPool`, `ReceiptFilter`, `ReceiptRule`, `ReceiptRuleSet`, `Template`, `VdmAttributes`
- **SNS** — `Topic`, `TopicPolicy`, `Subscription`
- **SQS** — `Queue`, `QueuePolicy`
- **SSM** — `Parameter`
- **Step Functions** — `StateMachine`, `StateMachineVersion`, `StateMachineAlias`, `Activity`
- **WAFv2** — `WebACL`, `WebACLAssociation`, `IPSet`, `RegexPatternSet`, `RuleGroup`, `LoggingConfiguration`

### Fn::GetAtt coverage

The provisioners populate the AWS-documented attribute set for each type, so `Fn::GetAtt` on common shapes works without templates having to fall back to `Ref` plus string surgery. SES email identities expose `DkimDNSTokenName1/2/3` + `DkimDNSTokenValue1/2/3`; WAFv2 web ACLs expose `Arn`, `Id`, `Capacity`, `LabelNamespace`; ELBv2 load balancers expose `DNSName`, `CanonicalHostedZoneID`, `LoadBalancerFullName`, `SecurityGroups`; Lambda functions expose `Arn`, `FunctionArn`, etc.

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

- **Not every resource type provisions something.** Types in the provisioner list above create real backing state. Anything else (most `AWS::EC2::*`, `AWS::AutoScaling::*`, etc.) is recorded but has no underlying resource, so a follow-up call against that service will 404.
- **Drift always reports IN_SYNC.** fakecloud is the source of truth for backing state, so real drift never occurs. The drift API still round-trips IDs and statuses for tooling that polls them.
- **SAM expansion runs at create time.** A re-uploaded template still requires `Capabilities=[CAPABILITY_AUTO_EXPAND]` on operations that touch transforms.

## Source

- [`crates/fakecloud-cloudformation`](https://github.com/faiscadev/fakecloud/tree/main/crates/fakecloud-cloudformation)
- [AWS CloudFormation API reference](https://docs.aws.amazon.com/AWSCloudFormation/latest/APIReference/Welcome.html)
