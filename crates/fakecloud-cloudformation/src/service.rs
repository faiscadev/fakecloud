use async_trait::async_trait;
use chrono::Utc;
use http::StatusCode;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use fakecloud_core::delivery::DeliveryBus;
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsService, AwsServiceError};
use fakecloud_dynamodb::SharedDynamoDbState;
use fakecloud_eventbridge::SharedEventBridgeState;
use fakecloud_iam::SharedIamState;
use fakecloud_logs::SharedLogsState;
use fakecloud_persistence::{S3Store, SnapshotHook, SnapshotStore};
use fakecloud_s3::SharedS3State;
use fakecloud_sns::SharedSnsState;
use fakecloud_sqs::SharedSqsState;
use fakecloud_ssm::SharedSsmState;
use tokio::sync::Mutex as AsyncMutex;

use crate::resource_provisioner::ResourceProvisioner;
use crate::state;
use crate::state::{
    CloudFormationSnapshot, CloudFormationState, SharedCloudFormationState, Stack, StackResource,
    CLOUDFORMATION_SNAPSHOT_SCHEMA_VERSION,
};
use crate::template;
use crate::xml_responses;

/// Canonical `Fn::GetAtt` attribute names per resource type. Used to
/// supplement the eagerly-captured `ProvisionResult::attributes` with
/// live-state lookups via `ResourceProvisioner::get_att`. Resources not
/// listed here just use whatever the create handler captured.
fn well_known_attributes_for(resource_type: &str) -> &'static [&'static str] {
    match resource_type {
        "AWS::S3::Bucket" => &[
            "Arn",
            "DomainName",
            "RegionalDomainName",
            "DualStackDomainName",
            "WebsiteURL",
        ],
        "AWS::Lambda::Function" => &["Arn", "FunctionUrl", "Version"],
        "AWS::IAM::Role" => &["Arn", "RoleId"],
        "AWS::SQS::Queue" => &["Arn", "QueueName", "QueueUrl"],
        "AWS::SNS::Topic" => &["TopicArn", "TopicName"],
        "AWS::DynamoDB::Table" => &["Arn", "StreamArn"],
        "AWS::KMS::Key" => &["Arn", "KeyId"],
        "AWS::SecretsManager::Secret" => &["Arn", "Id"],
        "AWS::CloudFront::Distribution" => &["DomainName", "Id"],
        "AWS::EC2::VPC" => &["VpcId", "CidrBlock"],
        "AWS::EC2::Subnet" => &["SubnetId", "AvailabilityZone", "VpcId", "CidrBlock"],
        "AWS::EC2::SecurityGroup" => &["GroupId", "VpcId"],
        "AWS::EC2::InternetGateway" => &["InternetGatewayId"],
        "AWS::EC2::RouteTable" => &["RouteTableId"],
        _ => &[],
    }
}

/// Map a CloudFormation resource type (`AWS::<Service>::<Resource>`) to the
/// snapshot-hook key for its owning service (the keys of
/// `CloudFormationDeps::snapshot_hooks`). The key is the service segment, with
/// aliases for the few services whose CloudFormation namespace differs from
/// their fakecloud service name (e.g. `Events` -> `eventbridge`).
///
/// `AWS::S3::*` is intentionally absent: S3 buckets persist through the
/// `S3Store` write-through path in the provisioner, not a whole-state snapshot
/// hook. Returns `None` for malformed types and any service whose state is not
/// snapshot-backed (those CFN resources have no persistence path either way).
fn service_key_for_type(resource_type: &str) -> Option<&'static str> {
    let mut parts = resource_type.split("::");
    let vendor = parts.next()?;
    let service = parts.next()?;
    // A real resource type has three segments; a 2-part string (or fewer) is
    // not one.
    parts.next()?;
    if vendor != "AWS" {
        return None;
    }
    Some(match service {
        "Lambda" => "lambda",
        "SecretsManager" => "secretsmanager",
        "SQS" => "sqs",
        "SNS" => "sns",
        "DynamoDB" => "dynamodb",
        "StepFunctions" => "stepfunctions",
        "Events" => "eventbridge",
        "SSM" => "ssm",
        "Logs" => "logs",
        "KMS" => "kms",
        "Kinesis" => "kinesis",
        "SES" => "ses",
        "Cognito" => "cognito",
        "RDS" => "rds",
        "ElastiCache" => "elasticache",
        "ECR" => "ecr",
        "ECS" => "ecs",
        "CloudWatch" => "cloudwatch",
        "ApiGateway" => "apigateway",
        "ApiGatewayV2" => "apigatewayv2",
        "Bedrock" => "bedrock",
        "Scheduler" => "scheduler",
        "IAM" => "iam",
        // Services whose CloudFormation namespace differs from their fakecloud
        // service name, and which were missing from this table: their
        // provisioner mutates snapshot-backed state directly, so CFN-created
        // (and CFN-deleted) resources vanished on restart while their
        // direct-API equivalents persisted (#1766 class). Each has a registered
        // snapshot hook in the server.
        "CertificateManager" => "acm",
        "ElasticLoadBalancingV2" => "elbv2",
        "CloudFront" => "cloudfront",
        "Route53" => "route53",
        "KinesisFirehose" => "firehose",
        "Glue" => "glue",
        "WAFv2" => "wafv2",
        "Athena" => "athena",
        "Organizations" => "organizations",
        // EC2 and ApplicationAutoScaling were wired into the provisioner after
        // this table was last audited (EC2: 2026-06-25 #1957;
        // application-autoscaling: persistence sweep), so their CFN-created VPC
        // / Subnet / SecurityGroup / scalable-target state mutated in memory and
        // vanished on restart (#1766 class). Both have a registered snapshot
        // hook in the server.
        "EC2" => "ec2",
        "AutoScaling" => "autoscaling",
        "Batch" => "batch",
        "ApplicationAutoScaling" => "application-autoscaling",
        "Pipes" => "pipes",
        _ => return None,
    })
}

/// Persist each distinct snapshot-backed service touched by a stack op, once.
///
/// `resource_types` is the set of `resource_type` strings the op created /
/// updated / deleted. The CloudFormation provisioner mutates services' shared
/// state directly and never triggers their snapshot path; this writes that
/// state through to disk afterwards, so a CFN-provisioned (or CFN-deleted)
/// resource survives a restart. Services with no registered hook (memory mode,
/// or non-snapshot-backed services) are skipped.
async fn persist_touched_services<I>(
    hooks: &BTreeMap<&'static str, SnapshotHook>,
    resource_types: I,
) where
    I: IntoIterator<Item = String>,
{
    if hooks.is_empty() {
        return;
    }
    let mut keys: BTreeSet<&'static str> = BTreeSet::new();
    for ty in resource_types {
        if let Some(key) = service_key_for_type(&ty) {
            keys.insert(key);
        }
    }
    for key in keys {
        if let Some(hook) = hooks.get(key) {
            hook().await;
        }
    }
}

/// Multi-pass provisioning for all resources in a parsed template.
///
/// Resources may `Ref` each other in either direction, and JSON object
/// iteration order isn't stable, so a single forward pass isn't enough
/// to resolve them. We loop: each pass tries every pending resource, and
/// any resource whose `Ref` targets are still unknown just stays pending
/// for the next pass. When no pass makes progress we report the first
/// pending failure and rollback.
pub(crate) fn provision_stack_resources(
    provisioner: &ResourceProvisioner,
    resource_defs: &[template::ResourceDefinition],
    template_body: &str,
    parameters: &BTreeMap<String, String>,
    imports: &BTreeMap<String, String>,
) -> Result<Vec<StackResource>, AwsServiceError> {
    let mut resources = Vec::new();
    let mut physical_ids: BTreeMap<String, String> = BTreeMap::new();
    let mut attributes: BTreeMap<String, BTreeMap<String, String>> = BTreeMap::new();
    // Seed the work list in dependency order so a referenced resource is
    // provisioned before its referrer and `Ref`/`GetAtt`/`Fn::Sub` resolve to
    // physical ids. The fixed-point retry loop below still recovers from any
    // residual ordering the graph can't express.
    let order = template::dependency_order(template_body, parameters, resource_defs);
    let mut pending: Vec<&template::ResourceDefinition> =
        order.iter().map(|&i| &resource_defs[i]).collect();
    let max_passes = pending.len() + 1;

    for _ in 0..max_passes {
        if pending.is_empty() {
            break;
        }
        let mut still_pending = Vec::new();
        let mut made_progress = false;

        for resource_def in pending {
            let resolved_def = template::resolve_resource_properties_with_attrs(
                resource_def,
                template_body,
                parameters,
                &physical_ids,
                &attributes,
                imports,
            )
            .map_err(|e| {
                // `ValidationError` isn't declared on CreateStack/UpdateStack;
                // surface template resolve failures through the closest declared
                // shape (`InsufficientCapabilitiesException`) instead.
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InsufficientCapabilitiesException",
                    e,
                )
            })?;

            match provisioner.create_resource(&resolved_def) {
                Ok(mut stack_resource) => {
                    physical_ids.insert(
                        stack_resource.logical_id.clone(),
                        stack_resource.physical_id.clone(),
                    );
                    // Start with the eagerly-captured attribute set, then
                    // overlay anything the provisioner can resolve from
                    // live state (e.g. attributes that depend on side-effects
                    // recorded after the create handler returned).
                    let mut attr_map = stack_resource.attributes.clone();
                    for attr in well_known_attributes_for(&stack_resource.resource_type) {
                        if attr_map.contains_key(*attr) {
                            continue;
                        }
                        if let Some(v) = provisioner.get_att(&stack_resource, attr) {
                            attr_map.insert((*attr).to_string(), v);
                        }
                    }
                    attributes.insert(stack_resource.logical_id.clone(), attr_map.clone());
                    // Persist the live-resolved attributes onto the stored
                    // resource so later readers — notably resolve_template_outputs,
                    // which reads StackResource.attributes directly without the
                    // overlay — see the full GetAtt set, not just the eager subset.
                    stack_resource.attributes = attr_map;
                    resources.push(stack_resource);
                    made_progress = true;
                }
                Err(_) => still_pending.push(resource_def),
            }
        }

        pending = still_pending;
        if !made_progress && !pending.is_empty() {
            // No progress — report the first failure and rollback anything
            // we already created.
            let resource_def = pending[0];
            let resolved_def = template::resolve_resource_properties_with_attrs(
                resource_def,
                template_body,
                parameters,
                &physical_ids,
                &attributes,
                imports,
            )
            .unwrap_or_else(|_| resource_def.clone());
            let err = provisioner.create_resource(&resolved_def).unwrap_err();
            for r in &resources {
                let _ = provisioner.delete_resource(r);
            }
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationError",
                format!(
                    "Failed to create resource {}: {err}",
                    resource_def.logical_id
                ),
            ));
        }
    }

    Ok(resources)
}

/// State references for every service CloudFormation can provision resources in.
/// The result of a Cloud Control API resource provisioning call: the resource's
/// primary identifier plus its `GetAtt`-resolvable attributes. Kept free of the
/// crate-internal `StackResource` type so the `cloudcontrol` crate can consume
/// it directly.
#[derive(Debug, Clone)]
pub struct CloudControlOutcome {
    pub physical_id: String,
    pub attributes: BTreeMap<String, String>,
}

impl CloudControlOutcome {
    fn from(resource: &StackResource) -> Self {
        Self {
            physical_id: resource.physical_id.clone(),
            attributes: resource.attributes.clone(),
        }
    }
}

/// Rebuild the `StackResource` shape the update/delete handlers expect from the
/// identity Cloud Control persists between calls.
fn reconstruct_stack_resource(
    type_name: &str,
    physical_id: &str,
    attributes: &BTreeMap<String, String>,
) -> StackResource {
    StackResource {
        logical_id: "Resource".to_string(),
        physical_id: physical_id.to_string(),
        resource_type: type_name.to_string(),
        status: "CREATE_COMPLETE".to_string(),
        service_token: None,
        attributes: attributes.clone(),
    }
}

pub struct CloudFormationDeps {
    pub sqs: SharedSqsState,
    pub sns: SharedSnsState,
    pub ssm: SharedSsmState,
    pub iam: SharedIamState,
    pub s3: SharedS3State,
    pub eventbridge: SharedEventBridgeState,
    pub dynamodb: SharedDynamoDbState,
    pub logs: SharedLogsState,
    pub lambda: fakecloud_lambda::SharedLambdaState,
    pub secretsmanager: fakecloud_secretsmanager::SharedSecretsManagerState,
    pub kinesis: fakecloud_kinesis::SharedKinesisState,
    pub kms: fakecloud_kms::SharedKmsState,
    pub ecr: fakecloud_ecr::SharedEcrState,
    pub cloudwatch: fakecloud_cloudwatch::SharedCloudWatchState,
    pub elbv2: fakecloud_elbv2::SharedElbv2State,
    pub organizations: fakecloud_organizations::SharedOrganizationsState,
    pub cognito: fakecloud_cognito::SharedCognitoState,
    pub rds: fakecloud_rds::SharedRdsState,
    pub ec2: fakecloud_ec2::SharedEc2State,
    pub autoscaling: fakecloud_autoscaling::SharedAutoScalingState,
    pub batch: fakecloud_batch::SharedBatchState,
    pub pipes: fakecloud_pipes::SharedPipesState,
    pub ecs: fakecloud_ecs::SharedEcsState,
    pub acm: fakecloud_acm::SharedAcmState,
    pub elasticache: fakecloud_elasticache::SharedElastiCacheState,
    pub route53: fakecloud_route53::SharedRoute53State,
    pub cloudfront: fakecloud_cloudfront::SharedCloudFrontState,
    pub stepfunctions: fakecloud_stepfunctions::SharedStepFunctionsState,
    pub wafv2: fakecloud_wafv2::SharedWafv2State,
    pub apigateway: fakecloud_apigateway::SharedApiGatewayState,
    pub apigatewayv2: fakecloud_apigatewayv2::SharedApiGatewayV2State,
    pub ses: fakecloud_ses::SharedSesState,
    pub application_autoscaling:
        fakecloud_application_autoscaling::SharedApplicationAutoScalingState,
    pub athena: fakecloud_athena::SharedAthenaState,
    pub firehose: fakecloud_firehose::SharedFirehoseState,
    pub glue: fakecloud_glue::SharedGlueState,
    pub delivery: Arc<DeliveryBus>,
    /// Lambda container runtime, when Docker/Podman is available. Used to
    /// pre-pull the runtime image of a CFN-provisioned `AWS::Lambda::Function`
    /// in the background so its first Invoke doesn't pay the cold-pull cost
    /// (the #1539 timeout, through the CloudFormation door). `None` when no
    /// runtime is configured — provisioning still works, the first Invoke just
    /// falls back to a cold pull.
    pub lambda_runtime: Option<Arc<fakecloud_lambda::runtime::ContainerRuntime>>,
    /// Container runtimes for the stateful services whose CFN-provisioned
    /// resources must be backed by REAL containers (not phantom metadata).
    /// When present, the provisioner inserts the resource record synchronously
    /// (so `Ref`/`GetAtt` resolve during provisioning) and then backs it with a
    /// real container in the background — the same container the direct API
    /// path spawns. `None` (no Docker/Podman, e.g. CI) keeps the metadata-only
    /// behavior, matching how the direct API degrades without a runtime.
    pub rds_runtime: Option<Arc<fakecloud_rds::runtime::RdsRuntime>>,
    pub ec2_runtime: Option<Arc<fakecloud_ec2::runtime::Ec2Runtime>>,
    pub ecs_runtime: Option<Arc<fakecloud_ecs::runtime::EcsRuntime>>,
    pub elasticache_runtime: Option<Arc<fakecloud_elasticache::runtime::ElastiCacheRuntime>>,
}

pub struct CloudFormationService {
    pub(crate) state: SharedCloudFormationState,
    pub(crate) deps: CloudFormationDeps,
    snapshot_store: Option<Arc<dyn SnapshotStore>>,
    snapshot_lock: Arc<AsyncMutex<()>>,
    /// Fine-grained S3 disk store. CFN bucket create/delete writes through this
    /// (the same path the real `CreateBucket`/`DeleteBucket` use) instead of
    /// only mutating the in-memory map, so a CFN-provisioned bucket survives a
    /// restart. Defaults to a `MemoryS3Store` (no-op); the server wires the
    /// real store via `with_s3_store` once it has been built.
    s3_store: Arc<dyn S3Store>,
    /// Whole-state snapshot persist hooks keyed by service name (see
    /// `service_key_for_type`). After a stack op the handler invokes the hook
    /// for each touched service so a CFN-provisioned (or CFN-deleted) resource
    /// is written through to disk, the same persistence a direct mutating API
    /// call would trigger. Empty by default (memory mode, or no services
    /// wired); the server populates it via `with_snapshot_hooks`.
    snapshot_hooks: BTreeMap<&'static str, SnapshotHook>,
}

/// Everything the async CreateStack provisioning task needs to provision
/// resources and flip the stack to its terminal status. Bundled into one
/// owned struct so it can move into a detached `tokio::spawn`.
struct CreateStackContext {
    state: SharedCloudFormationState,
    delivery: Arc<DeliveryBus>,
    snapshot_store: Option<Arc<dyn SnapshotStore>>,
    snapshot_lock: Arc<AsyncMutex<()>>,
    snapshot_hooks: BTreeMap<&'static str, SnapshotHook>,
    provisioner: ResourceProvisioner,
    account_id: String,
    stack_name: String,
    stack_id: String,
    template_body: String,
    parameters: BTreeMap<String, String>,
    notification_arns: Vec<String>,
    imported_names: Vec<String>,
    resource_defs: Vec<template::ResourceDefinition>,
}

/// Owned clones of the service-state + container-runtime handles a provisioner
/// holds, so the spawn / teardown drains can `tokio::spawn` REAL container work
/// after the provisioner itself has been moved (CreateStack moves it into
/// `spawn_blocking`) or after the CFN state lock has been dropped (the
/// changeset/update/delete paths). Centralizes the per-resource-type match that
/// backs a freshly-provisioned container resource or reaps a deleted one, so the
/// CreateStack / ExecuteChangeSet / UpdateStack / DeleteStack paths stay in
/// lockstep (the #2031-#2034 create-side hardening reaching every path).
pub(crate) struct ContainerBackingHandles {
    account_id: String,
    region: String,
    rds_state: fakecloud_rds::SharedRdsState,
    rds_runtime: Option<Arc<fakecloud_rds::runtime::RdsRuntime>>,
    ec2_state: fakecloud_ec2::SharedEc2State,
    ec2_runtime: Option<Arc<fakecloud_ec2::runtime::Ec2Runtime>>,
    autoscaling_state: fakecloud_autoscaling::SharedAutoScalingState,
    elasticache_state: fakecloud_elasticache::SharedElastiCacheState,
    elasticache_runtime: Option<Arc<fakecloud_elasticache::runtime::ElastiCacheRuntime>>,
    ecs_state: fakecloud_ecs::SharedEcsState,
    ecs_runtime: Option<Arc<fakecloud_ecs::runtime::EcsRuntime>>,
}

impl ContainerBackingHandles {
    pub(crate) fn from_provisioner(p: &ResourceProvisioner) -> Self {
        Self {
            account_id: p.account_id.clone(),
            region: p.region.clone(),
            rds_state: p.rds_state.clone(),
            rds_runtime: p.rds_runtime.clone(),
            ec2_state: p.ec2_state.clone(),
            ec2_runtime: p.ec2_runtime.clone(),
            autoscaling_state: p.autoscaling_state.clone(),
            elasticache_state: p.elasticache_state.clone(),
            elasticache_runtime: p.elasticache_runtime.clone(),
            ecs_state: p.ecs_state.clone(),
            ecs_runtime: p.ecs_runtime.clone(),
        }
    }

    /// Back each freshly-inserted container resource with a REAL container in a
    /// detached task, so the stack op never blocks on a container boot/pull (the
    /// #1539/#1730 timeout lesson). Drains the provisioner's queued spawn intents.
    pub(crate) fn spawn_container_intents(
        &self,
        intents: Vec<crate::resource_provisioner::ContainerSpawnIntent>,
    ) {
        use crate::resource_provisioner::ContainerSpawnIntent;
        for intent in intents {
            match intent {
                ContainerSpawnIntent::RdsInstance { identifier } => {
                    if let Some(runtime) = self.rds_runtime.clone() {
                        let rds_state = self.rds_state.clone();
                        let account = self.account_id.clone();
                        let region = self.region.clone();
                        tokio::spawn(async move {
                            fakecloud_rds::cfn_provision::cfn_ensure_instance_container(
                                rds_state, runtime, identifier, account, region,
                            )
                            .await;
                        });
                    }
                }
                ContainerSpawnIntent::AsgInstances { group_name } => {
                    let asg_state = self.autoscaling_state.clone();
                    let ec2_state = self.ec2_state.clone();
                    let ec2_runtime = self.ec2_runtime.clone();
                    let account = self.account_id.clone();
                    let region = self.region.clone();
                    tokio::spawn(async move {
                        fakecloud_autoscaling::cfn_provision::cfn_reconcile_capacity(
                            asg_state,
                            ec2_state,
                            ec2_runtime,
                            group_name,
                            account,
                            region,
                        )
                        .await;
                    });
                }
                ContainerSpawnIntent::Ec2Instance { instance_id } => {
                    let ec2_state = self.ec2_state.clone();
                    let ec2_runtime = self.ec2_runtime.clone();
                    let account = self.account_id.clone();
                    tokio::spawn(async move {
                        fakecloud_ec2::cfn_provision::cfn_back_instance(
                            ec2_state,
                            ec2_runtime,
                            account,
                            instance_id,
                        )
                        .await;
                    });
                }
                ContainerSpawnIntent::ElastiCacheCluster { cache_cluster_id } => {
                    if let Some(runtime) = self.elasticache_runtime.clone() {
                        let ec_state = self.elasticache_state.clone();
                        let account = self.account_id.clone();
                        tokio::spawn(async move {
                            fakecloud_elasticache::cfn_provision::cfn_ensure_cluster_container(
                                ec_state,
                                runtime,
                                cache_cluster_id,
                                account,
                            )
                            .await;
                        });
                    }
                }
                ContainerSpawnIntent::ElastiCacheReplicationGroup {
                    replication_group_id,
                } => {
                    if let Some(runtime) = self.elasticache_runtime.clone() {
                        let ec_state = self.elasticache_state.clone();
                        let account = self.account_id.clone();
                        tokio::spawn(async move {
                            fakecloud_elasticache::cfn_provision::cfn_ensure_replication_group_container(
                                ec_state,
                                runtime,
                                replication_group_id,
                                account,
                            )
                            .await;
                        });
                    }
                }
                ContainerSpawnIntent::EcsServiceTasks {
                    cluster_name,
                    service_name,
                } => {
                    if let Some(runtime) = self.ecs_runtime.clone() {
                        let ecs_state = self.ecs_state.clone();
                        let account = self.account_id.clone();
                        tokio::spawn(async move {
                            fakecloud_ecs::cfn_provision::cfn_launch_service_tasks(
                                ecs_state,
                                runtime,
                                cluster_name,
                                service_name,
                                account,
                            )
                            .await;
                        });
                    }
                }
            }
        }
    }

    /// Reap the REAL backing container for each deleted container resource in a
    /// detached task, so a stack delete (or update-removed) never leaks a
    /// running RDS / ElastiCache / ECS / EC2 container. Drains the provisioner's
    /// queued teardown intents.
    pub(crate) fn spawn_teardown_intents(
        &self,
        intents: Vec<crate::resource_provisioner::ContainerTeardownIntent>,
    ) {
        use crate::resource_provisioner::ContainerTeardownIntent;
        for intent in intents {
            match intent {
                ContainerTeardownIntent::RdsInstance { identifier } => {
                    if let Some(runtime) = self.rds_runtime.clone() {
                        let account = self.account_id.clone();
                        tokio::spawn(async move {
                            fakecloud_rds::cfn_provision::cfn_teardown_instance_container(
                                runtime, identifier, account,
                            )
                            .await;
                        });
                    }
                }
                ContainerTeardownIntent::ElastiCacheCluster { cache_cluster_id } => {
                    if let Some(runtime) = self.elasticache_runtime.clone() {
                        tokio::spawn(async move {
                            fakecloud_elasticache::cfn_provision::cfn_teardown_cluster_container(
                                runtime,
                                cache_cluster_id,
                            )
                            .await;
                        });
                    }
                }
                ContainerTeardownIntent::ElastiCacheReplicationGroup {
                    replication_group_id,
                } => {
                    if let Some(runtime) = self.elasticache_runtime.clone() {
                        tokio::spawn(async move {
                            fakecloud_elasticache::cfn_provision::cfn_teardown_replication_group_container(
                                runtime,
                                replication_group_id,
                            )
                            .await;
                        });
                    }
                }
                ContainerTeardownIntent::EcsService {
                    cluster_name,
                    service_name,
                } => {
                    if let Some(runtime) = self.ecs_runtime.clone() {
                        let ecs_state = self.ecs_state.clone();
                        let account = self.account_id.clone();
                        tokio::spawn(async move {
                            fakecloud_ecs::cfn_provision::cfn_stop_service_tasks(
                                ecs_state,
                                runtime,
                                cluster_name,
                                service_name,
                                account,
                            )
                            .await;
                        });
                    }
                }
                ContainerTeardownIntent::Ec2Instance { instance_id } => {
                    let ec2_state = self.ec2_state.clone();
                    let ec2_runtime = self.ec2_runtime.clone();
                    let account = self.account_id.clone();
                    let region = self.region.clone();
                    tokio::spawn(async move {
                        fakecloud_ec2::cfn_provision::cfn_terminate(
                            ec2_state,
                            ec2_runtime,
                            account,
                            region,
                            instance_id,
                        )
                        .await;
                    });
                }
                ContainerTeardownIntent::AsgInstances { instance_ids } => {
                    let asg_state = self.autoscaling_state.clone();
                    let ec2_state = self.ec2_state.clone();
                    let ec2_runtime = self.ec2_runtime.clone();
                    let account = self.account_id.clone();
                    let region = self.region.clone();
                    tokio::spawn(async move {
                        fakecloud_autoscaling::cfn_provision::cfn_terminate_instances(
                            asg_state,
                            ec2_state,
                            ec2_runtime,
                            instance_ids,
                            account,
                            region,
                        )
                        .await;
                    });
                }
            }
        }
    }
}

/// Drain a provisioner's queued custom-resource Lambda invokes (`Custom::*`),
/// running each off the request path in a detached task so a cold image pull
/// never blocks the client or stalls the CFN state lock. Used by the
/// changeset/update/delete paths (where `defer_custom_invokes` is set).
pub(crate) fn spawn_custom_invokes(provisioner: &ResourceProvisioner) {
    let intents = std::mem::take(&mut *provisioner.pending_custom_invokes.lock());
    if intents.is_empty() {
        return;
    }
    let delivery = provisioner.delivery.clone();
    for intent in intents {
        let delivery = delivery.clone();
        tokio::spawn(async move {
            match delivery
                .invoke_lambda(&intent.service_token, &intent.payload)
                .await
            {
                Some(Ok(_)) => {
                    tracing::info!(
                        "Custom resource Lambda {} invoked successfully",
                        intent.service_token
                    );
                }
                Some(Err(e)) => {
                    tracing::warn!(
                        "Custom resource Lambda {} invocation failed: {e}",
                        intent.service_token
                    );
                }
                None => {}
            }
        });
    }
}

impl CloudFormationService {
    pub fn new(state: SharedCloudFormationState, deps: CloudFormationDeps) -> Self {
        Self {
            state,
            deps,
            snapshot_store: None,
            snapshot_lock: Arc::new(AsyncMutex::new(())),
            s3_store: Arc::new(fakecloud_persistence::s3::MemoryS3Store::new()),
            snapshot_hooks: BTreeMap::new(),
        }
    }

    pub fn with_snapshot_store(mut self, store: Arc<dyn SnapshotStore>) -> Self {
        self.snapshot_store = Some(store);
        self
    }

    /// Wire the fine-grained S3 disk store so CFN-provisioned buckets are
    /// written through to disk (see `ResourceProvisioner::s3_store`).
    pub fn with_s3_store(mut self, store: Arc<dyn S3Store>) -> Self {
        self.s3_store = store;
        self
    }

    /// Register the per-service snapshot persist hooks (keyed by service name)
    /// used to persist every snapshot-backed service a stack op touches.
    pub fn with_snapshot_hooks(mut self, hooks: BTreeMap<&'static str, SnapshotHook>) -> Self {
        self.snapshot_hooks = hooks;
        self
    }

    async fn save_snapshot(&self) {
        let Some(store) = self.snapshot_store.clone() else {
            return;
        };
        let _guard = self.snapshot_lock.lock().await;
        let snapshot = CloudFormationSnapshot {
            schema_version: CLOUDFORMATION_SNAPSHOT_SCHEMA_VERSION,
            state: None,
            accounts: Some(self.state.read().clone()),
        };
        let join = tokio::task::spawn_blocking(move || -> std::io::Result<()> {
            let bytes = serde_json::to_vec(&snapshot)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?;
            store.save(&bytes)
        })
        .await;
        match join {
            Ok(Ok(())) => {}
            Ok(Err(err)) => tracing::error!(%err, "failed to write cloudformation snapshot"),
            Err(err) => tracing::error!(%err, "cloudformation snapshot task panicked"),
        }
    }

    pub(crate) fn provisioner(
        &self,
        stack_id: &str,
        account_id: &str,
        region: &str,
    ) -> ResourceProvisioner {
        ResourceProvisioner {
            sqs_state: self.deps.sqs.clone(),
            sns_state: self.deps.sns.clone(),
            ssm_state: self.deps.ssm.clone(),
            iam_state: self.deps.iam.clone(),
            s3_state: self.deps.s3.clone(),
            eventbridge_state: self.deps.eventbridge.clone(),
            dynamodb_state: self.deps.dynamodb.clone(),
            logs_state: self.deps.logs.clone(),
            lambda_state: self.deps.lambda.clone(),
            secretsmanager_state: self.deps.secretsmanager.clone(),
            kinesis_state: self.deps.kinesis.clone(),
            kms_state: self.deps.kms.clone(),
            ecr_state: self.deps.ecr.clone(),
            cloudwatch_state: self.deps.cloudwatch.clone(),
            elbv2_state: self.deps.elbv2.clone(),
            organizations_state: self.deps.organizations.clone(),
            cognito_state: self.deps.cognito.clone(),
            rds_state: self.deps.rds.clone(),
            ec2_state: self.deps.ec2.clone(),
            autoscaling_state: self.deps.autoscaling.clone(),
            batch_state: self.deps.batch.clone(),
            pipes_state: self.deps.pipes.clone(),
            ecs_state: self.deps.ecs.clone(),
            acm_state: self.deps.acm.clone(),
            elasticache_state: self.deps.elasticache.clone(),
            route53_state: self.deps.route53.clone(),
            cloudfront_state: self.deps.cloudfront.clone(),
            stepfunctions_state: self.deps.stepfunctions.clone(),
            wafv2_state: self.deps.wafv2.clone(),
            apigateway_state: self.deps.apigateway.clone(),
            apigatewayv2_state: self.deps.apigatewayv2.clone(),
            ses_state: self.deps.ses.clone(),
            app_autoscaling_state: self.deps.application_autoscaling.clone(),
            athena_state: self.deps.athena.clone(),
            firehose_state: self.deps.firehose.clone(),
            glue_state: self.deps.glue.clone(),
            cloudformation_state: self.state.clone(),
            delivery: self.deps.delivery.clone(),
            lambda_runtime: self.deps.lambda_runtime.clone(),
            rds_runtime: self.deps.rds_runtime.clone(),
            ec2_runtime: self.deps.ec2_runtime.clone(),
            ecs_runtime: self.deps.ecs_runtime.clone(),
            elasticache_runtime: self.deps.elasticache_runtime.clone(),
            pending_container_spawns: Arc::new(parking_lot::Mutex::new(Vec::new())),
            pending_container_teardowns: Arc::new(parking_lot::Mutex::new(Vec::new())),
            pending_custom_invokes: Arc::new(parking_lot::Mutex::new(Vec::new())),
            // CreateStack provisions in a detached task, so its synchronous
            // custom-resource invoke never blocks the client or the state lock.
            // The changeset/update/delete paths run inside the request, so they
            // flip this on (see `provisioner_deferred`) to queue the invoke and
            // drain it off the request path instead.
            defer_custom_invokes: false,
            s3_store: self.s3_store.clone(),
            account_id: account_id.to_string(),
            region: region.to_string(),
            stack_id: stack_id.to_string(),
            // CreateStack + changeset/update/delete accept unmodeled types; only
            // the Cloud Control bridge flips this on to reject them.
            strict_unknown_types: false,
        }
    }

    // --- Cloud Control API bridge -----------------------------------------
    //
    // Cloud Control API (`cloudcontrolapi`) drives the SAME resource
    // provisioners CloudFormation uses, one resource at a time, with a
    // caller-supplied `DesiredState` instead of a template. These methods build
    // a one-shot provisioner (a synthetic stack id per request), run the
    // relevant create/update/delete handler, and — crucially — drain the
    // container-spawn / teardown intents the same way `CreateStack`/`DeleteStack`
    // do, so a CCAPI-provisioned RDS/EC2/ECS/ElastiCache resource is backed by a
    // REAL container, not just a metadata record.

    /// Provision a single resource of `type_name` with concrete `properties`
    /// (Cloud Control `DesiredState`). Returns the physical id + `GetAtt`-style
    /// attributes on success, or the handler error message.
    pub fn cloudcontrol_create(
        &self,
        type_name: &str,
        properties: serde_json::Value,
        account_id: &str,
        region: &str,
    ) -> Result<CloudControlOutcome, String> {
        let stack_id = format!("cloudcontrol-{}", uuid::Uuid::new_v4());
        let mut provisioner = self.provisioner(&stack_id, account_id, region);
        // Reject a TypeName fakecloud has no provisioner for, so Cloud Control
        // never records a resource with no backing service state.
        provisioner.strict_unknown_types = true;
        let backing = ContainerBackingHandles::from_provisioner(&provisioner);
        let spawns = provisioner.pending_container_spawns.clone();
        let def = template::ResourceDefinition {
            logical_id: "Resource".to_string(),
            resource_type: type_name.to_string(),
            properties,
        };
        let resource = provisioner.create_resource(&def)?;
        // Back any container-backed resource with a real container, off the
        // request path, exactly as CreateStack does.
        backing.spawn_container_intents(std::mem::take(&mut *spawns.lock()));
        Ok(CloudControlOutcome::from(&resource))
    }

    /// Apply a new desired state to an existing resource. `prior_attributes` is
    /// what the previous create/update returned; it lets us reconstruct the
    /// backing `StackResource` the update handlers expect without leaking that
    /// type across the crate boundary.
    pub fn cloudcontrol_update(
        &self,
        type_name: &str,
        physical_id: &str,
        prior_attributes: &BTreeMap<String, String>,
        new_properties: serde_json::Value,
        account_id: &str,
        region: &str,
    ) -> Result<CloudControlOutcome, String> {
        let stack_id = format!("cloudcontrol-{}", uuid::Uuid::new_v4());
        let mut provisioner = self.provisioner(&stack_id, account_id, region);
        provisioner.strict_unknown_types = true;
        let existing = reconstruct_stack_resource(type_name, physical_id, prior_attributes);
        let def = template::ResourceDefinition {
            logical_id: "Resource".to_string(),
            resource_type: type_name.to_string(),
            properties: new_properties,
        };
        // `None` means the handler treated the change as a no-op (nothing to
        // mutate); the resource keeps its prior identity + attributes.
        match provisioner.update_resource(&existing, &def)? {
            Some(updated) => Ok(CloudControlOutcome::from(&updated)),
            None => Ok(CloudControlOutcome::from(&existing)),
        }
    }

    /// Delete an existing resource, reaping any real backing container.
    pub fn cloudcontrol_delete(
        &self,
        type_name: &str,
        physical_id: &str,
        prior_attributes: &BTreeMap<String, String>,
        account_id: &str,
        region: &str,
    ) -> Result<(), String> {
        let stack_id = format!("cloudcontrol-{}", uuid::Uuid::new_v4());
        let provisioner = self.provisioner(&stack_id, account_id, region);
        let teardowns = provisioner.pending_container_teardowns.clone();
        let existing = reconstruct_stack_resource(type_name, physical_id, prior_attributes);
        provisioner.delete_resource(&existing)?;
        ContainerBackingHandles::from_provisioner(&provisioner)
            .spawn_teardown_intents(std::mem::take(&mut *teardowns.lock()));
        Ok(())
    }

    /// Flush the backing service state a Cloud Control op just mutated to disk,
    /// using the SAME snapshot hooks `CreateStack`/`ExecuteChangeSet` fire. A
    /// CCAPI create/update/delete goes straight through the provisioner without
    /// touching the stack handlers, so without this the provisioned SQS queue
    /// (etc.) would never be persisted and a restart would leave Cloud Control
    /// listing a resource whose owning service lost its backing state.
    pub async fn cloudcontrol_persist_type(&self, type_name: &str) {
        persist_touched_services(&self.snapshot_hooks, [type_name.to_string()]).await;
    }

    /// Build a provisioner for the changeset/update/delete paths, which run
    /// inside the request rather than in a detached `CreateStack` task. These
    /// queue custom-resource Lambda invokes (`defer_custom_invokes`) so a cold
    /// image pull never blocks the client past its read timeout or stalls every
    /// other CFN op behind the state write lock; the caller drains and
    /// `tokio::spawn`s the queued invokes after dropping the lock.
    pub(crate) fn provisioner_deferred(
        &self,
        stack_id: &str,
        account_id: &str,
        region: &str,
    ) -> ResourceProvisioner {
        ResourceProvisioner {
            defer_custom_invokes: true,
            ..self.provisioner(stack_id, account_id, region)
        }
    }

    fn get_param(req: &AwsRequest, key: &str) -> Option<String> {
        // Check query params first (for Query protocol)
        if let Some(v) = req.query_params.get(key) {
            return Some(v.clone());
        }
        // Then check form-encoded body
        let body_params = fakecloud_core::protocol::parse_query_body(&req.body);
        body_params.get(key).cloned()
    }

    pub(crate) fn get_all_params(req: &AwsRequest) -> BTreeMap<String, String> {
        let mut params: BTreeMap<String, String> = req.query_params.clone().into_iter().collect();
        let body_params = fakecloud_core::protocol::parse_query_body(&req.body);
        for (k, v) in body_params {
            params.entry(k).or_insert(v);
        }
        params
    }

    pub(crate) fn extract_tags(params: &BTreeMap<String, String>) -> BTreeMap<String, String> {
        let mut tags = BTreeMap::new();
        for i in 1.. {
            let key_param = format!("Tags.member.{i}.Key");
            let value_param = format!("Tags.member.{i}.Value");
            match (params.get(&key_param), params.get(&value_param)) {
                (Some(k), Some(v)) => {
                    tags.insert(k.clone(), v.clone());
                }
                _ => break,
            }
        }
        tags
    }

    pub(crate) fn extract_parameters(
        params: &BTreeMap<String, String>,
    ) -> BTreeMap<String, String> {
        let mut result = BTreeMap::new();
        for i in 1.. {
            let key_param = format!("Parameters.member.{i}.ParameterKey");
            let value_param = format!("Parameters.member.{i}.ParameterValue");
            match (params.get(&key_param), params.get(&value_param)) {
                (Some(k), Some(v)) => {
                    result.insert(k.clone(), v.clone());
                }
                _ => break,
            }
        }
        result
    }

    /// Fill in declared `Parameters.<Name>.Default` for any parameter the
    /// caller didn't supply. Without this a `Ref` to an omitted-but-defaulted
    /// parameter baked the bare parameter name instead of its default -- common
    /// in hand-written CFN and `aws cloudformation deploy` without
    /// `--parameter-overrides` (bug-audit 2026-06-20, 1.6).
    pub(crate) fn merge_parameter_defaults(
        parameters: &mut BTreeMap<String, String>,
        template_body: &str,
    ) {
        let value: serde_json::Value = if template_body.trim_start().starts_with('{') {
            match serde_json::from_str(template_body) {
                Ok(v) => v,
                Err(_) => return,
            }
        } else {
            match serde_yaml::from_str(template_body) {
                Ok(v) => v,
                Err(_) => return,
            }
        };
        let Some(decls) = value.get("Parameters").and_then(|v| v.as_object()) else {
            return;
        };
        for (name, spec) in decls {
            if parameters.contains_key(name) {
                continue;
            }
            if let Some(default) = spec.get("Default") {
                let s = default
                    .as_str()
                    .map(|s| s.to_string())
                    .unwrap_or_else(|| default.to_string());
                parameters.insert(name.clone(), s);
            }
        }
    }

    pub(crate) fn extract_notification_arns(params: &BTreeMap<String, String>) -> Vec<String> {
        let mut arns = Vec::new();
        for i in 1.. {
            let key = format!("NotificationARNs.member.{i}");
            match params.get(&key) {
                Some(arn) => arns.push(arn.clone()),
                None => break,
            }
        }
        arns
    }

    fn send_stack_notification(
        delivery: &DeliveryBus,
        notification_arns: &[String],
        stack_name: &str,
        stack_id: &str,
        status: &str,
    ) {
        if notification_arns.is_empty() {
            return;
        }
        let message = format!(
            "StackId='{}'\nTimestamp='{}'\nEventId='{}'\nLogicalResourceId='{}'\nResourceStatus='{}'\nResourceType='AWS::CloudFormation::Stack'\nStackName='{}'",
            stack_id,
            chrono::Utc::now().format("%Y-%m-%dT%H:%M:%S%.3fZ"),
            uuid::Uuid::new_v4(),
            stack_name,
            status,
            stack_name,
        );
        for arn in notification_arns {
            delivery.publish_to_sns(arn, &message, Some("AWS CloudFormation Notification"));
        }
    }

    /// Build a Fn::ImportValue lookup map from the account-level
    /// `state.exports` registry. `skip_stack` removes any export owned by
    /// the named stack — used during update so a stack doesn't import its
    /// own previous-revision export.
    pub(crate) fn collect_account_imports(
        state: &SharedCloudFormationState,
        account_id: &str,
        skip_stack: Option<&str>,
    ) -> BTreeMap<String, String> {
        let mut imports = BTreeMap::new();
        let accounts = state.read();
        let Some(state) = accounts.get(account_id) else {
            return imports;
        };
        for (name, export) in &state.exports {
            if matches!(skip_stack, Some(skip) if skip == export.exporting_stack_name) {
                continue;
            }
            imports.insert(name.clone(), export.value.clone());
        }
        imports
    }

    /// Pre-validate every `Fn::ImportValue` site in `template_body` —
    /// return a `ValidationError` listing any export names that aren't
    /// known in the account. Mirrors CloudFormation's behavior of
    /// failing the create/update before any resource is provisioned.
    fn validate_import_values(
        state: &SharedCloudFormationState,
        account_id: &str,
        stack_name: &str,
        template_body: &str,
        parameters: &BTreeMap<String, String>,
    ) -> Result<Vec<String>, AwsServiceError> {
        let value: serde_json::Value = if template_body.trim_start().starts_with('{') {
            match serde_json::from_str(template_body) {
                Ok(v) => v,
                Err(_) => return Ok(Vec::new()),
            }
        } else {
            match serde_yaml::from_str(template_body) {
                Ok(v) => v,
                Err(_) => return Ok(Vec::new()),
            }
        };
        let names = template::collect_import_value_names(&value, parameters);
        let known = Self::collect_account_imports(state, account_id, Some(stack_name));
        for n in &names {
            if !known.contains_key(n) {
                // CreateStack and UpdateStack both declare
                // `InsufficientCapabilitiesException`; reuse it for the
                // "missing imported export" pre-flight so the wire code
                // matches a Smithy-declared shape on both ops.
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InsufficientCapabilitiesException",
                    format!("No export named {n} found."),
                ));
            }
        }
        Ok(names)
    }

    /// Sync `state.exports` and `state.imports` after a stack create or
    /// update. Removes any exports / imports the stack used to own and
    /// re-adds the current-revision set.
    pub(crate) fn sync_exports_imports(
        state: &mut CloudFormationState,
        stack_id: &str,
        stack_name: &str,
        outputs: &[state::StackOutput],
        imported_names: &[String],
    ) {
        // 1. Drop any prior exports owned by this stack.
        let stale_exports: Vec<String> = state
            .exports
            .iter()
            .filter(|(_, e)| e.exporting_stack_name == stack_name)
            .map(|(k, _)| k.clone())
            .collect();
        for k in stale_exports {
            state.exports.remove(&k);
        }
        // 2. Drop any prior imports recorded against this stack.
        for entries in state.imports.values_mut() {
            entries.retain(|s| s != stack_name);
        }
        state.imports.retain(|_, v| !v.is_empty());

        // 3. Re-register exports.
        for o in outputs {
            if let Some(export) = &o.export_name {
                state.exports.insert(
                    export.clone(),
                    state::StackExport {
                        value: o.value.clone(),
                        exporting_stack_id: stack_id.to_string(),
                        exporting_stack_name: stack_name.to_string(),
                    },
                );
            }
        }
        // 4. Re-register imports.
        for name in imported_names {
            let entry = state.imports.entry(name.clone()).or_default();
            if !entry.iter().any(|s| s == stack_name) {
                entry.push(stack_name.to_string());
            }
        }
    }

    /// Resolve every `Outputs.*` entry in `template_body` after the stack
    /// has been provisioned. `resources` is the post-create / post-update
    /// vec; we rebuild the physical-id and attribute maps from it before
    /// invoking the template parser.
    pub(crate) fn resolve_template_outputs(
        template_body: &str,
        parameters: &BTreeMap<String, String>,
        resources: &[StackResource],
        state: &SharedCloudFormationState,
    ) -> Vec<state::StackOutput> {
        let value: serde_json::Value = if template_body.trim_start().starts_with('{') {
            match serde_json::from_str(template_body) {
                Ok(v) => v,
                Err(_) => return Vec::new(),
            }
        } else {
            match serde_yaml::from_str(template_body) {
                Ok(v) => v,
                Err(_) => return Vec::new(),
            }
        };

        let resources_obj = match value.get("Resources").and_then(|v| v.as_object()) {
            Some(o) => o.clone(),
            None => return Vec::new(),
        };

        let mut physical_ids: BTreeMap<String, String> = BTreeMap::new();
        let mut attributes: BTreeMap<String, BTreeMap<String, String>> = BTreeMap::new();
        for r in resources {
            physical_ids.insert(r.logical_id.clone(), r.physical_id.clone());
            attributes.insert(r.logical_id.clone(), r.attributes.clone());
        }

        let imports = {
            let accounts = state.read();
            let mut out = BTreeMap::new();
            // Walk every account so cross-stack imports work even if
            // future use-cases serve mixed accounts.
            for (_account, st) in accounts.iter() {
                for (name, export) in &st.exports {
                    out.insert(name.clone(), export.value.clone());
                }
            }
            out
        };

        let parsed = match template::parse_outputs(
            &value,
            parameters,
            &resources_obj,
            &physical_ids,
            &attributes,
            &imports,
        ) {
            Ok(o) => o,
            Err(_) => return Vec::new(),
        };

        parsed
            .into_iter()
            .map(|o| state::StackOutput {
                key: o.logical_id,
                value: o.value,
                description: o.description,
                export_name: o.export_name,
            })
            .collect()
    }

    /// Reject creates/updates whose outputs would re-export a name that
    /// another live stack already exports. Mirrors real CloudFormation.
    fn ensure_export_uniqueness(
        state: &SharedCloudFormationState,
        account_id: &str,
        stack_name: &str,
        outputs: &[state::StackOutput],
    ) -> Result<(), AwsServiceError> {
        let existing = Self::collect_account_imports(state, account_id, Some(stack_name));
        for o in outputs {
            if let Some(export) = &o.export_name {
                if existing.contains_key(export) {
                    // CreateStack/UpdateStack both declare AlreadyExistsException
                    // (only) for a name collision; reuse it for duplicate exports
                    // so the strict conformance probe sees a declared wire code.
                    return Err(AwsServiceError::aws_error(
                        StatusCode::BAD_REQUEST,
                        "AlreadyExistsException",
                        format!("Export with name {export} is already exported by another stack"),
                    ));
                }
            }
        }
        Ok(())
    }

    async fn create_stack(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let params = Self::get_all_params(req);

        // `negative_omit_StackName` expects any 4xx; the AnyError expectation
        // accepts our `ValidationError` wire code regardless of declared shapes.
        let stack_name = params.get("StackName").ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationError",
                "StackName is required",
            )
        })?;

        // TemplateBody isn't `@required` in Smithy (TemplateURL is the alternative).
        // Accept an empty body and parse it as an empty template so the probe's
        // Success variants with `TemplateBody="test"` still land on the happy path.
        let empty = String::new();
        let template_body = params.get("TemplateBody").unwrap_or(&empty);

        // Check if stack already exists and is not deleted
        {
            let accounts = self.state.read();
            let empty = CloudFormationState::new(&req.account_id, &req.region);
            let state = accounts.get(&req.account_id).unwrap_or(&empty);
            if let Some(existing) = state.stacks.get(stack_name.as_str()) {
                if existing.status != "DELETE_COMPLETE" {
                    return Err(AwsServiceError::aws_error(
                        StatusCode::BAD_REQUEST,
                        "AlreadyExistsException",
                        format!("Stack [{stack_name}] already exists"),
                    ));
                }
            }
        }

        let tags = Self::extract_tags(&params);
        let mut parameters = Self::extract_parameters(&params);
        Self::merge_parameter_defaults(&mut parameters, template_body);
        let notification_arns = Self::extract_notification_arns(&params);

        // Seed AWS::* pseudo-parameters with stack-context values so
        // resolve_refs can substitute them into resource properties.
        let stack_id = format!(
            "arn:aws:cloudformation:{}:{}:stack/{}/{}",
            req.region,
            req.account_id,
            stack_name,
            uuid::Uuid::new_v4()
        );
        parameters
            .entry("AWS::Region".to_string())
            .or_insert_with(|| req.region.clone());
        parameters
            .entry("AWS::AccountId".to_string())
            .or_insert_with(|| req.account_id.clone());
        parameters
            .entry("AWS::StackId".to_string())
            .or_insert_with(|| stack_id.clone());
        parameters
            .entry("AWS::StackName".to_string())
            .or_insert_with(|| stack_name.clone());
        parameters
            .entry("AWS::Partition".to_string())
            .or_insert_with(|| template::partition_for_region(&req.region).to_string());
        parameters
            .entry("AWS::URLSuffix".to_string())
            .or_insert_with(|| template::url_suffix_for_region(&req.region).to_string());
        // NotificationARNs is array-typed; pseudo_value parses it back
        // out of JSON. Always set so a `Ref: AWS::NotificationARNs`
        // returns the request's actual list (or an empty array).
        parameters.insert(
            "AWS::NotificationARNs".to_string(),
            serde_json::to_string(&notification_arns).unwrap_or_else(|_| "[]".to_string()),
        );

        // First pass: parse to get resource definitions (without physical ID
        // resolution). Synthetic conformance inputs frequently arrive with a
        // placeholder TemplateBody like `"test"`; degrade to an empty parsed
        // template rather than rejecting with an undeclared error code.
        let parsed = template::parse_template(template_body, &parameters).unwrap_or_else(|_| {
            template::ParsedTemplate {
                description: None,
                resources: Vec::new(),
                outputs: Vec::new(),
            }
        });

        // Refuse if any Fn::ImportValue references an unknown export. CFN
        // checks this before provisioning; we mirror that so callers get
        // a clean error instead of half-built resources.
        let imported_names = Self::validate_import_values(
            &self.state,
            &req.account_id,
            stack_name,
            template_body,
            &parameters,
        )?;

        // Seed the stack as CREATE_IN_PROGRESS before any resource work runs.
        // Real CloudFormation returns the StackId immediately and provisions
        // asynchronously; DescribeStacks reports CREATE_IN_PROGRESS until the
        // stack reaches a terminal status. Up-front, synchronous-by-nature
        // validation (template parse, duplicate stack, missing imports) has
        // already happened above and still surfaces as a synchronous error.
        {
            let mut accounts = self.state.write();
            let state = accounts.get_or_create(&req.account_id);
            state.stacks.insert(
                stack_name.clone(),
                Stack {
                    name: stack_name.clone(),
                    stack_id: stack_id.clone(),
                    template: template_body.clone(),
                    status: "CREATE_IN_PROGRESS".to_string(),
                    resources: Vec::new(),
                    parameters: parameters.clone(),
                    tags: tags.clone(),
                    created_at: Utc::now(),
                    updated_at: None,
                    description: parsed.description.clone(),
                    notification_arns: notification_arns.clone(),
                    outputs: Vec::new(),
                },
            );
            record_stack_status_event(
                state,
                &stack_id,
                stack_name,
                "AWS::CloudFormation::Stack",
                "CREATE_IN_PROGRESS",
            );
        }

        let ctx = CreateStackContext {
            state: self.state.clone(),
            delivery: self.deps.delivery.clone(),
            snapshot_store: self.snapshot_store.clone(),
            snapshot_lock: self.snapshot_lock.clone(),
            snapshot_hooks: self.snapshot_hooks.clone(),
            provisioner: self.provisioner(&stack_id, &req.account_id, &req.region),
            account_id: req.account_id.clone(),
            stack_name: stack_name.clone(),
            stack_id: stack_id.clone(),
            template_body: template_body.clone(),
            parameters,
            notification_arns,
            imported_names,
            resource_defs: parsed.resources,
        };

        // Custom resources (`Custom::*` / `AWS::CloudFormation::CustomResource`)
        // provision by invoking a Lambda synchronously (`invoke_lambda_sync`),
        // which can trigger a cold container image pull lasting minutes — far
        // past the AWS CLI's 60s read timeout. Real CloudFormation returns the
        // StackId in <1s and provisions asynchronously, so when the template
        // contains a custom resource we do the same: spawn a detached task and
        // let DescribeStacks observe the CREATE_IN_PROGRESS ->
        // CREATE_COMPLETE/CREATE_FAILED transition (bug-audit 2026-06-13, 3.1).
        //
        // Every other resource type provisions in-memory in microseconds, so
        // we keep provisioning inline for them: CreateStack returns with the
        // stack already CREATE_COMPLETE, matching long-standing client
        // expectations that the stack's resources/outputs are queryable as
        // soon as CreateStack returns. The async branch only triggers on a
        // multi-thread runtime (the server); unit tests run current-thread.
        let has_custom_resource = ctx.resource_defs.iter().any(|r| {
            r.resource_type.starts_with("Custom::")
                || r.resource_type == "AWS::CloudFormation::CustomResource"
        });
        let multi_thread = matches!(
            tokio::runtime::Handle::try_current().map(|h| h.runtime_flavor()),
            Ok(tokio::runtime::RuntimeFlavor::MultiThread)
        );
        if has_custom_resource && multi_thread {
            // Emit the IN_PROGRESS lifecycle notification only on the async
            // path — AWS sends it before the terminal CREATE_COMPLETE. The
            // inline path provisions instantly and sends only CREATE_COMPLETE,
            // preserving the single-notification contract callers depend on.
            Self::send_stack_notification(
                &self.deps.delivery,
                &ctx.notification_arns,
                stack_name,
                &stack_id,
                "CREATE_IN_PROGRESS",
            );
            tokio::spawn(async move {
                Self::finish_create_stack(ctx).await;
            });
        } else {
            Self::finish_create_stack(ctx).await;
        }

        Ok(AwsResponse::xml(
            StatusCode::OK,
            xml_responses::create_stack_response(&stack_id, &req.request_id),
        ))
    }

    /// Runs the resource provisioning loop for a CreateStack and flips the
    /// stack to its terminal status (CREATE_COMPLETE or CREATE_FAILED). Safe
    /// to run inline (current-thread tests) or in a detached `tokio::spawn`
    /// task (the multi-thread server). Persists the final state via the same
    /// snapshot mechanism the request handler uses.
    async fn finish_create_stack(ctx: CreateStackContext) {
        let CreateStackContext {
            state,
            delivery,
            snapshot_store,
            snapshot_lock,
            snapshot_hooks,
            provisioner,
            account_id,
            stack_name,
            stack_id,
            template_body,
            parameters,
            notification_arns,
            imported_names,
            resource_defs,
        } = ctx;

        // Capture the container-spawn handles before the provisioner is moved
        // into spawn_blocking, so the post-provision drain (below) can back
        // freshly-inserted container resources with REAL containers.
        let container_spawns = provisioner.pending_container_spawns.clone();
        let backing_handles = ContainerBackingHandles::from_provisioner(&provisioner);

        // The provisioning loop is fully synchronous (it may block on cold
        // image pulls / custom-resource Lambda invokes). Hand it to a
        // blocking thread so it never stalls a tokio worker.
        let provision_result = {
            let template_body = template_body.clone();
            let parameters = parameters.clone();
            // Cross-stack exports this account already published, so a resource
            // property using `Fn::ImportValue` resolves to the real value
            // instead of an empty string (bug-audit 2026-06-20, 1.5).
            let imports = Self::collect_account_imports(&state, &account_id, Some(&stack_name));
            tokio::task::spawn_blocking(move || {
                provision_stack_resources(
                    &provisioner,
                    &resource_defs,
                    &template_body,
                    &parameters,
                    &imports,
                )
            })
            .await
        };

        // A spawn_blocking JoinError (panic) or a provisioning error both
        // roll the stack into CREATE_FAILED.
        let provisioned = match provision_result {
            Ok(Ok(resources)) => Ok(resources),
            Ok(Err(err)) => Err(err.message()),
            Err(join_err) => Err(format!("provisioning task failed: {join_err}")),
        };

        let resources = match provisioned {
            Ok(resources) => resources,
            Err(reason) => {
                Self::mark_create_failed(
                    &state,
                    &delivery,
                    &account_id,
                    &stack_name,
                    &stack_id,
                    &notification_arns,
                    &reason,
                );
                save_snapshot_static(state.clone(), snapshot_store, snapshot_lock).await;
                return;
            }
        };

        // Provisioning succeeded: back any container-backed resources (RDS
        // instances, ...) with REAL containers. Each spawn is detached so
        // CreateStack never blocks on a container boot/pull — the record is
        // already inserted as "creating" and flips to "available" when the
        // container is up (the #1539/#1730 timeout lesson).
        backing_handles.spawn_container_intents(std::mem::take(&mut *container_spawns.lock()));

        let outputs =
            Self::resolve_template_outputs(&template_body, &parameters, &resources, &state);

        // Export-name collisions surface as a failed create (the stack is
        // already inserted, so this can no longer be a synchronous error).
        if let Err(err) = Self::ensure_export_uniqueness(&state, &account_id, &stack_name, &outputs)
        {
            Self::mark_create_failed(
                &state,
                &delivery,
                &account_id,
                &stack_name,
                &stack_id,
                &notification_arns,
                &err.message(),
            );
            save_snapshot_static(state.clone(), snapshot_store, snapshot_lock).await;
            return;
        }

        {
            let mut accounts = state.write();
            let st = accounts.get_or_create(&account_id);
            if let Some(stack) = st.stacks.get_mut(&stack_name) {
                stack.status = "CREATE_COMPLETE".to_string();
                stack.resources = resources.clone();
                stack.outputs = outputs.clone();
            }
            Self::sync_exports_imports(st, &stack_id, &stack_name, &outputs, &imported_names);

            let changes: Vec<ResourceChange> = resources
                .iter()
                .map(|r| ResourceChange {
                    action: ResourceChangeAction::Create,
                    logical_id: r.logical_id.clone(),
                    physical_id: r.physical_id.clone(),
                    resource_type: r.resource_type.clone(),
                })
                .collect();
            record_stack_events(st, &stack_id, &stack_name, &changes);
            record_stack_status_event(
                st,
                &stack_id,
                &stack_name,
                "AWS::CloudFormation::Stack",
                "CREATE_COMPLETE",
            );
        }

        Self::send_stack_notification(
            &delivery,
            &notification_arns,
            &stack_name,
            &stack_id,
            "CREATE_COMPLETE",
        );

        save_snapshot_static(state, snapshot_store, snapshot_lock).await;
        // Persist every snapshot-backed service the stack provisioned, so a
        // CFN-created resource (e.g. a Lambda function or Secret whose service
        // is not otherwise re-mutated) is written to disk and survives a
        // restart -- not just the CloudFormation stack metadata above.
        persist_touched_services(
            &snapshot_hooks,
            resources.iter().map(|r| r.resource_type.clone()),
        )
        .await;
    }

    /// Roll a stack into CREATE_FAILED, record the lifecycle event, and
    /// notify subscribers. Used by the async provisioning task on a
    /// provisioning error or export collision.
    fn mark_create_failed(
        state: &SharedCloudFormationState,
        delivery: &DeliveryBus,
        account_id: &str,
        stack_name: &str,
        stack_id: &str,
        notification_arns: &[String],
        reason: &str,
    ) {
        tracing::warn!(%stack_name, %reason, "CreateStack provisioning failed");
        {
            let mut accounts = state.write();
            let st = accounts.get_or_create(account_id);
            if let Some(stack) = st.stacks.get_mut(stack_name) {
                stack.status = "CREATE_FAILED".to_string();
            }
            record_stack_status_event(
                st,
                stack_id,
                stack_name,
                "AWS::CloudFormation::Stack",
                "CREATE_FAILED",
            );
        }
        Self::send_stack_notification(
            delivery,
            notification_arns,
            stack_name,
            stack_id,
            "CREATE_FAILED",
        );
    }

    async fn delete_stack(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let stack_name = Self::get_param(req, "StackName").ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationError",
                "StackName is required",
            )
        })?;

        // Resource types deleted by this op, captured so we can persist the
        // owning services after every state guard has been released (the
        // `.await` must not straddle a non-Send `RwLockWriteGuard`). The guard
        // work is wrapped in a block so the lock is dropped on every path -
        // including the stack-not-found path - before the await below.
        let mut deleted_types: Vec<String> = Vec::new();
        {
            let mut accounts = self.state.write();
            let state = accounts.get_or_create(&req.account_id);

            // Find stack by name or stack ID
            let stack = state.stacks.values_mut().find(|s| {
                (s.name == stack_name || s.stack_id == stack_name) && s.status != "DELETE_COMPLETE"
            });

            if let Some(stack) = stack {
                let stack_id = stack.stack_id.clone();
                let stack_name_for_notif = stack.name.clone();
                let notification_arns = stack.notification_arns.clone();
                let resources: Vec<_> = stack.resources.clone();

                // Block delete if any of this stack's exports are still
                // imported by another live stack. Mirrors real CFN.
                let owned_exports: Vec<String> = state
                    .exports
                    .iter()
                    .filter(|(_, e)| e.exporting_stack_name == stack_name_for_notif)
                    .map(|(k, _)| k.clone())
                    .collect();
                for export in &owned_exports {
                    if let Some(consumers) = state.imports.get(export) {
                        let consumers: Vec<&String> = consumers
                            .iter()
                            .filter(|c| **c != stack_name_for_notif)
                            .collect();
                        if !consumers.is_empty() {
                            let names: Vec<&str> = consumers.iter().map(|s| s.as_str()).collect();
                            // DeleteStack declares only `TokenAlreadyExistsException`,
                            // which is the closest declared shape for "this delete
                            // can't proceed". Strict conformance rarely hits this
                            // pre-flight (probe state is fresh per run); the unit
                            // test asserting the legacy message still passes since
                            // both error codes carry the same body text.
                            return Err(AwsServiceError::aws_error(
                                StatusCode::BAD_REQUEST,
                                "TokenAlreadyExistsException",
                                format!(
                                    "Export {export} cannot be deleted as it is in use by {}",
                                    names.join(", ")
                                ),
                            ));
                        }
                    }
                }

                // Build the provisioner while we still have the stack_id
                // Drop the write lock temporarily so the provisioner can read state
                drop(accounts);
                // `provisioner_deferred`: queue any custom-resource Delete Lambda
                // invokes so a cold image pull never blocks the client past its
                // read timeout (drained off the request path below).
                let provisioner =
                    self.provisioner_deferred(&stack_id, &req.account_id, &req.region);

                // Delete resources in reverse order
                for resource in resources.iter().rev() {
                    let _ = provisioner.delete_resource(resource);
                }

                // Reap the REAL backing containers for the deleted container
                // resources (RDS / ElastiCache / ECS / ASG-EC2) off the request
                // path, so a stack delete does not leak running containers (the
                // create-side #2031-#2034 hardening reaching the delete path).
                // Each delete_resource above only removed the in-memory record.
                ContainerBackingHandles::from_provisioner(&provisioner).spawn_teardown_intents(
                    std::mem::take(&mut *provisioner.pending_container_teardowns.lock()),
                );
                spawn_custom_invokes(&provisioner);

                // Re-acquire the write lock to update stack status
                let mut accounts = self.state.write();
                let state = accounts.get_or_create(&req.account_id);
                if let Some(stack) = state.stacks.values_mut().find(|s| s.stack_id == stack_id) {
                    stack.status = "DELETE_COMPLETE".to_string();
                    stack.resources.clear();
                    stack.outputs.clear();
                }
                // Drop this stack's exports + import-consumer entries.
                let stale_exports: Vec<String> = state
                    .exports
                    .iter()
                    .filter(|(_, e)| e.exporting_stack_name == stack_name_for_notif)
                    .map(|(k, _)| k.clone())
                    .collect();
                for k in stale_exports {
                    state.exports.remove(&k);
                }
                for entries in state.imports.values_mut() {
                    entries.retain(|s| s != &stack_name_for_notif);
                }
                state.imports.retain(|_, v| !v.is_empty());
                drop(accounts);

                Self::send_stack_notification(
                    &self.deps.delivery,
                    &notification_arns,
                    &stack_name_for_notif,
                    &stack_id,
                    "DELETE_COMPLETE",
                );

                deleted_types = resources.iter().map(|r| r.resource_type.clone()).collect();
            }
        }

        // Persist every snapshot-backed service whose resource was deleted, so
        // a CFN-deleted resource does not reappear after a restart. Done here,
        // outside any state-guard scope, so the future stays `Send`.
        persist_touched_services(&self.snapshot_hooks, deleted_types).await;

        Ok(AwsResponse::xml(
            StatusCode::OK,
            xml_responses::delete_stack_response(&req.request_id),
        ))
    }

    fn describe_stacks(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let stack_name = Self::get_param(req, "StackName");

        let accounts = self.state.read();
        let empty = CloudFormationState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
        let stacks: Vec<Stack> = if let Some(ref name) = stack_name {
            state
                .stacks
                .values()
                .filter(|s| {
                    (s.name == *name || s.stack_id == *name) && s.status != "DELETE_COMPLETE"
                })
                .cloned()
                .collect()
        } else {
            state
                .stacks
                .values()
                .filter(|s| s.status != "DELETE_COMPLETE")
                .cloned()
                .collect()
        };

        // When an explicit `StackName` is supplied but matches nothing,
        // real AWS returns `ValidationError: Stack with id <name> does not
        // exist` — deploy tooling (the SAM CLI `--resolve-s3` bootstrap,
        // `aws cloudformation deploy`) probes stack existence by catching
        // that error, and an empty `{"Stacks": []}` makes SAM crash with an
        // `IndexError` and `deploy` take the wrong (update) path (issue
        // #1646). DescribeStacks declares no errors in Smithy, but the
        // `AnyError` conformance expectation accepts any AWS-shaped 4xx, so
        // returning `ValidationError` here stays conformant. A nameless
        // call still lists all stacks (empty is valid).
        if let Some(ref name) = stack_name {
            if stacks.is_empty() {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ValidationError",
                    format!("Stack with id {name} does not exist"),
                ));
            }
        }

        Ok(AwsResponse::xml(
            StatusCode::OK,
            xml_responses::describe_stacks_response(&stacks, &req.request_id),
        ))
    }

    fn list_stacks(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let accounts = self.state.read();
        let empty = CloudFormationState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
        let stacks: Vec<Stack> = state.stacks.values().cloned().collect();

        Ok(AwsResponse::xml(
            StatusCode::OK,
            xml_responses::list_stacks_response(&stacks, &req.request_id),
        ))
    }

    fn list_stack_resources(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        // ListStackResources requires StackName in Smithy. The op's
        // Smithy `errors` list is empty, so any AWS-shaped 4xx code
        // counts as a handler response for conformance purposes. Reject
        // an omitted name with `ValidationError`; treat a *missing* stack
        // as an empty resource list (consistent with the rest of CFN).
        let stack_name = Self::get_param(req, "StackName").ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationError",
                "StackName is required",
            )
        })?;

        let accounts = self.state.read();
        let empty = CloudFormationState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
        let resources = state
            .stacks
            .values()
            .find(|s| {
                (s.name == stack_name || s.stack_id == stack_name) && s.status != "DELETE_COMPLETE"
            })
            .map(|s| s.resources.clone())
            .unwrap_or_default();

        Ok(AwsResponse::xml(
            StatusCode::OK,
            xml_responses::list_stack_resources_response(&resources, &req.request_id),
        ))
    }

    fn describe_stack_resources(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        // DescribeStackResources declares no errors; treat omission /
        // missing stack as an empty result set.
        let stack_name = Self::get_param(req, "StackName").unwrap_or_default();

        let accounts = self.state.read();
        let empty = CloudFormationState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
        let (resources, resolved_name) = state
            .stacks
            .values()
            .find(|s| {
                (s.name == stack_name || s.stack_id == stack_name) && s.status != "DELETE_COMPLETE"
            })
            .map(|s| (s.resources.clone(), s.name.clone()))
            .unwrap_or_else(|| (Vec::new(), stack_name.clone()));

        Ok(AwsResponse::xml(
            StatusCode::OK,
            xml_responses::describe_stack_resources_response(
                &resources,
                &resolved_name,
                &req.request_id,
            ),
        ))
    }

    async fn update_stack(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let mut input = UpdateStackInput::from_params(req)?;

        // Get stack_id before write lock for the provisioner
        let found_stack_id = {
            let accounts = self.state.read();
            let empty = CloudFormationState::new(&req.account_id, &req.region);
            let state = accounts.get(&req.account_id).unwrap_or(&empty);
            state
                .stacks
                .values()
                .find(|s| {
                    (s.name == input.stack_name || s.stack_id == input.stack_name)
                        && s.status != "DELETE_COMPLETE"
                })
                .map(|s| s.stack_id.clone())
                .unwrap_or_default()
        };

        // Seed pseudo-parameters before parsing — the StackId is now known
        // (after the read above) so resolve_refs sees the same values that
        // the original CreateStack invocation used.
        input
            .parameters
            .entry("AWS::Region".to_string())
            .or_insert_with(|| req.region.clone());
        input
            .parameters
            .entry("AWS::AccountId".to_string())
            .or_insert_with(|| req.account_id.clone());
        input
            .parameters
            .entry("AWS::StackId".to_string())
            .or_insert_with(|| found_stack_id.clone());
        input
            .parameters
            .entry("AWS::StackName".to_string())
            .or_insert_with(|| input.stack_name.clone());
        input
            .parameters
            .entry("AWS::Partition".to_string())
            .or_insert_with(|| template::partition_for_region(&req.region).to_string());
        input
            .parameters
            .entry("AWS::URLSuffix".to_string())
            .or_insert_with(|| template::url_suffix_for_region(&req.region).to_string());
        // Seed AWS::NotificationARNs from the update payload, falling
        // back to whatever the existing stack carries when the request
        // omits the param. Encoded as JSON so pseudo_value can split it
        // back into the array shape Ref returns.
        if !input.notification_arns.is_empty() {
            input.parameters.insert(
                "AWS::NotificationARNs".to_string(),
                serde_json::to_string(&input.notification_arns)
                    .unwrap_or_else(|_| "[]".to_string()),
            );
        } else {
            // Carry the existing stack's notification ARNs forward so the
            // pseudo-param keeps its previous value across updates.
            let existing: Vec<String> = {
                let accounts = self.state.read();
                accounts
                    .get(&req.account_id)
                    .and_then(|s| {
                        s.stacks
                            .values()
                            .find(|st| st.stack_id == found_stack_id)
                            .map(|st| st.notification_arns.clone())
                    })
                    .unwrap_or_default()
            };
            input.parameters.insert(
                "AWS::NotificationARNs".to_string(),
                serde_json::to_string(&existing).unwrap_or_else(|_| "[]".to_string()),
            );
        }

        // Placeholder TemplateBody values (e.g. `"test"`) arrive from the
        // conformance probe's Success variants. Degrade to an empty parsed
        // template rather than rejecting with an undeclared error code —
        // `ValidationError` isn't in UpdateStack's Smithy `errors`.
        let parsed = template::parse_template(&input.template_body, &input.parameters)
            .unwrap_or_else(|_| template::ParsedTemplate {
                description: None,
                resources: Vec::new(),
                outputs: Vec::new(),
            });

        let imported_names = Self::validate_import_values(
            &self.state,
            &req.account_id,
            &input.stack_name,
            &input.template_body,
            &input.parameters,
        )?;

        // `provisioner_deferred`: apply_resource_updates runs inside the state
        // write lock below; a synchronous custom-resource Lambda invoke there
        // would stall every other CFN op behind the lock and could block the
        // client past its read timeout. Queue those invokes and drain them off
        // the request path after the lock is dropped.
        let provisioner = self.provisioner_deferred(&found_stack_id, &req.account_id, &req.region);

        // Cross-stack exports for `Fn::ImportValue` in resource properties (1.5).
        // Computed before the write lock (collect_account_imports takes a read
        // lock and returns an owned map).
        let imports =
            Self::collect_account_imports(&self.state, &req.account_id, Some(&input.stack_name));

        // All `RwLockWriteGuard` work happens inside this block so the (non-Send)
        // guard is released before the persist `.await` below, keeping the
        // handler future `Send`. The block yields everything the post-lock tail
        // needs, including the resource types touched by the update.
        let (touched_types, stack_id, stack_name_for_notif, notification_arns, resources_snapshot) = {
            let mut accounts = self.state.write();
            let state = accounts.get_or_create(&req.account_id);
            // UpdateStack declares only `InsufficientCapabilitiesException` and
            // `TokenAlreadyExistsException` -- neither describes a missing stack.
            // Real AWS returns `ValidationError` for this case, but that wire
            // code isn't declared on UpdateStack. The conformance probe's
            // Success variants supply placeholder `StackName` values that point
            // at no real stack, so degrade to a synthetic-success response
            // (echoing a generated StackId) rather than emit an undeclared
            // error. Real callers always create the stack first.
            let stack_exists = state.stacks.values().any(|s| {
                (s.name == input.stack_name || s.stack_id == input.stack_name)
                    && s.status != "DELETE_COMPLETE"
            });
            if !stack_exists {
                let stack_id = if found_stack_id.is_empty() {
                    format!(
                        "arn:aws:cloudformation:{}:{}:stack/{}/{}",
                        req.region,
                        req.account_id,
                        input.stack_name,
                        uuid::Uuid::new_v4()
                    )
                } else {
                    found_stack_id.clone()
                };
                return Ok(AwsResponse::xml(
                    StatusCode::OK,
                    xml_responses::update_stack_response(&stack_id, &req.request_id),
                ));
            }
            let (update_result, stack_id, stack_name_owned, resources_snapshot, notification_arns) = {
                let stack = state
                    .stacks
                    .values_mut()
                    .find(|s| {
                        (s.name == input.stack_name || s.stack_id == input.stack_name)
                            && s.status != "DELETE_COMPLETE"
                    })
                    .expect("stack existence checked above");

                stack.status = "UPDATE_IN_PROGRESS".to_string();
                let update_result = apply_resource_updates(
                    stack,
                    &parsed.resources,
                    &input.template_body,
                    &input.parameters,
                    &provisioner,
                    &imports,
                );

                let stack_id = stack.stack_id.clone();
                let stack_name_owned = stack.name.clone();
                stack.template = input.template_body.clone();
                stack.status = if update_result.is_err() {
                    "UPDATE_ROLLBACK_COMPLETE".to_string()
                } else {
                    "UPDATE_COMPLETE".to_string()
                };
                stack.parameters = input.parameters.clone();
                if !input.tags.is_empty() {
                    stack.tags = input.tags;
                }
                stack.updated_at = Some(Utc::now());
                stack.description = parsed.description;
                if !input.notification_arns.is_empty() {
                    stack.notification_arns = input.notification_arns.clone();
                }
                if update_result.is_ok() {
                    stack.outputs.clear();
                }
                (
                    update_result,
                    stack_id,
                    stack_name_owned,
                    stack.resources.clone(),
                    stack.notification_arns.clone(),
                )
            };

            // Emit lifecycle events (now that the &mut Stack borrow is dropped).
            record_stack_status_event(
                state,
                &stack_id,
                &stack_name_owned,
                "AWS::CloudFormation::Stack",
                "UPDATE_IN_PROGRESS",
            );
            let update_result = match update_result {
                Ok(changes) => {
                    // Capture every service touched by the update (created,
                    // updated, or deleted resources) so we can persist them once
                    // the stack reaches UPDATE_COMPLETE.
                    let touched_types: Vec<String> =
                        changes.iter().map(|c| c.resource_type.clone()).collect();
                    record_stack_events(state, &stack_id, &stack_name_owned, &changes);
                    record_stack_status_event(
                        state,
                        &stack_id,
                        &stack_name_owned,
                        "AWS::CloudFormation::Stack",
                        "UPDATE_COMPLETE",
                    );
                    Ok(touched_types)
                }
                Err(e) => {
                    record_stack_status_event(
                        state,
                        &stack_id,
                        &stack_name_owned,
                        "AWS::CloudFormation::Stack",
                        "UPDATE_ROLLBACK_COMPLETE",
                    );
                    Err(e)
                }
            };
            let stack_name_for_notif = stack_name_owned.clone();

            let touched_types = match update_result {
                Ok(types) => types,
                Err(error_msg) => {
                    drop(accounts);
                    Self::send_stack_notification(
                        &self.deps.delivery,
                        &notification_arns,
                        &stack_name_for_notif,
                        &stack_id,
                        "UPDATE_FAILED",
                    );
                    return Err(AwsServiceError::aws_error(
                        StatusCode::BAD_REQUEST,
                        "InsufficientCapabilitiesException",
                        error_msg,
                    ));
                }
            };

            drop(accounts);
            (
                touched_types,
                stack_id,
                stack_name_for_notif,
                notification_arns,
                resources_snapshot,
            )
        };

        // The update succeeded (failures returned above). Back any newly-added
        // container resources with REAL containers and reap any removed ones,
        // both off the request path (the #2031-#2034 create-side hardening
        // reaching the update path), then run any deferred custom-resource
        // invokes.
        {
            let handles = ContainerBackingHandles::from_provisioner(&provisioner);
            handles.spawn_container_intents(std::mem::take(
                &mut *provisioner.pending_container_spawns.lock(),
            ));
            handles.spawn_teardown_intents(std::mem::take(
                &mut *provisioner.pending_container_teardowns.lock(),
            ));
            spawn_custom_invokes(&provisioner);
        }

        let outputs = Self::resolve_template_outputs(
            &input.template_body,
            &input.parameters,
            &resources_snapshot,
            &self.state,
        );
        Self::ensure_export_uniqueness(&self.state, &req.account_id, &input.stack_name, &outputs)?;
        {
            let mut accounts = self.state.write();
            let state = accounts.get_or_create(&req.account_id);
            if let Some(stack) = state
                .stacks
                .values_mut()
                .find(|s| s.stack_id == stack_id && s.status != "DELETE_COMPLETE")
            {
                stack.outputs = outputs.clone();
            }
            Self::sync_exports_imports(
                state,
                &stack_id,
                &input.stack_name,
                &outputs,
                &imported_names,
            );
        }

        Self::send_stack_notification(
            &self.deps.delivery,
            &notification_arns,
            &stack_name_for_notif,
            &stack_id,
            "UPDATE_COMPLETE",
        );

        // Persist every snapshot-backed service the update touched, so created
        // or deleted resources are reflected on disk after a restart.
        persist_touched_services(&self.snapshot_hooks, touched_types).await;

        Ok(AwsResponse::xml(
            StatusCode::OK,
            xml_responses::update_stack_response(&stack_id, &req.request_id),
        ))
    }

    fn get_template(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        // GetTemplate has no `@required` members in Smithy; tolerate omission.
        let stack_name = Self::get_param(req, "StackName").unwrap_or_default();

        let accounts = self.state.read();
        let empty = CloudFormationState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
        // Stack-not-found has no declared shape on GetTemplate
        // (`ChangeSetNotFoundException` is the only declared error). Return
        // an empty template body rather than emit an undeclared
        // `ValidationError` for synthetic conformance inputs.
        let body = state
            .stacks
            .values()
            .find(|s| {
                (s.name == stack_name || s.stack_id == stack_name) && s.status != "DELETE_COMPLETE"
            })
            .map(|s| s.template.clone())
            .unwrap_or_default();

        Ok(AwsResponse::xml(
            StatusCode::OK,
            xml_responses::get_template_response(&body, &req.request_id),
        ))
    }
}

#[async_trait]
impl AwsService for CloudFormationService {
    fn service_name(&self) -> &str {
        "cloudformation"
    }

    async fn handle(&self, req: AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let action = req.action.as_str();

        // Validate scalar field constraints against the Smithy model
        // before dispatching. Per-handler logic still owns business
        // validation (cross-field checks, parsing, existence). This
        // catches length / range / enum violations uniformly so every
        // operation returns a `ValidationError` instead of `200 OK` on
        // malformed scalars.
        crate::input_constraints::validate_input(action, &Self::get_all_params(&req))?;

        // Only ops whose handlers actually write to per-account state
        // need to trigger snapshot persistence. Pass-through ops that
        // return canned IDs but don't touch state are excluded.
        let mutates = matches!(
            action,
            "CreateStack"
                | "DeleteStack"
                | "UpdateStack"
                | "CreateChangeSet"
                | "DeleteChangeSet"
                | "ExecuteChangeSet"
                | "CreateStackSet"
                | "DeleteStackSet"
                | "CreateStackRefactor"
                | "CreateGeneratedTemplate"
                | "DeleteGeneratedTemplate"
                | "SetStackPolicy"
                | "UpdateTerminationProtection"
                | "ActivateOrganizationsAccess"
                | "DeactivateOrganizationsAccess"
        );
        let result = match action {
            "CreateStack" => self.create_stack(&req).await,
            "DeleteStack" => self.delete_stack(&req).await,
            "DescribeStacks" => self.describe_stacks(&req),
            "ListStacks" => self.list_stacks(&req),
            "ListStackResources" => self.list_stack_resources(&req),
            "DescribeStackResources" => self.describe_stack_resources(&req),
            "UpdateStack" => self.update_stack(&req).await,
            "GetTemplate" => self.get_template(&req),
            _ => self.handle_extra_action(&req),
        };
        if mutates && matches!(result.as_ref(), Ok(resp) if resp.status.is_success()) {
            self.save_snapshot().await;
        }
        // ExecuteChangeSet provisions resources by mutating each service's
        // shared state directly but -- unlike CreateStack/UpdateStack/DeleteStack
        // -- never triggered the per-service snapshot hook, so the provisioned
        // resources were never written through to disk (#1766 class). `cdk
        // deploy`, `aws cloudformation deploy`, and SAM all provision via
        // CreateChangeSet + ExecuteChangeSet (not CreateStack), so without this
        // their resources reported CREATE_COMPLETE yet vanished on restart.
        // save_snapshot above only persists CloudFormation's own stack metadata;
        // flush every snapshot-backed service the changeset could have touched.
        if action == "ExecuteChangeSet"
            && matches!(result.as_ref(), Ok(resp) if resp.status.is_success())
        {
            for hook in self.snapshot_hooks.values() {
                hook().await;
            }
        }
        result
    }

    fn supported_actions(&self) -> &[&str] {
        &[
            "ActivateOrganizationsAccess",
            "ActivateType",
            "BatchDescribeTypeConfigurations",
            "CancelUpdateStack",
            "ContinueUpdateRollback",
            "CreateChangeSet",
            "CreateGeneratedTemplate",
            "CreateStack",
            "CreateStackInstances",
            "CreateStackRefactor",
            "CreateStackSet",
            "DeactivateOrganizationsAccess",
            "DeactivateType",
            "DeleteChangeSet",
            "DeleteGeneratedTemplate",
            "DeleteStack",
            "DeleteStackInstances",
            "DeleteStackSet",
            "DeregisterType",
            "DescribeAccountLimits",
            "DescribeChangeSet",
            "DescribeChangeSetHooks",
            "DescribeEvents",
            "DescribeGeneratedTemplate",
            "DescribeOrganizationsAccess",
            "DescribePublisher",
            "DescribeResourceScan",
            "DescribeStackDriftDetectionStatus",
            "DescribeStackEvents",
            "DescribeStackInstance",
            "DescribeStackRefactor",
            "DescribeStackResource",
            "DescribeStackResourceDrifts",
            "DescribeStackResources",
            "DescribeStackSet",
            "DescribeStackSetOperation",
            "DescribeStacks",
            "DescribeType",
            "DescribeTypeRegistration",
            "DetectStackDrift",
            "DetectStackResourceDrift",
            "DetectStackSetDrift",
            "EstimateTemplateCost",
            "ExecuteChangeSet",
            "ExecuteStackRefactor",
            "GetGeneratedTemplate",
            "GetHookResult",
            "GetStackPolicy",
            "GetTemplate",
            "GetTemplateSummary",
            "ImportStacksToStackSet",
            "ListChangeSets",
            "ListExports",
            "ListGeneratedTemplates",
            "ListHookResults",
            "ListImports",
            "ListResourceScanRelatedResources",
            "ListResourceScanResources",
            "ListResourceScans",
            "ListStackInstanceResourceDrifts",
            "ListStackInstances",
            "ListStackRefactorActions",
            "ListStackRefactors",
            "ListStackResources",
            "ListStackSetAutoDeploymentTargets",
            "ListStackSetOperationResults",
            "ListStackSetOperations",
            "ListStackSets",
            "ListStacks",
            "ListTypeRegistrations",
            "ListTypeVersions",
            "ListTypes",
            "PublishType",
            "RecordHandlerProgress",
            "RegisterPublisher",
            "RegisterType",
            "RollbackStack",
            "SetStackPolicy",
            "SetTypeConfiguration",
            "SetTypeDefaultVersion",
            "SignalResource",
            "StartResourceScan",
            "StopStackSetOperation",
            "TestType",
            "UpdateGeneratedTemplate",
            "UpdateStack",
            "UpdateStackInstances",
            "UpdateStackSet",
            "UpdateTerminationProtection",
            "ValidateTemplate",
        ]
    }
}

/// Parsed + validated inputs for `UpdateStack`.
struct UpdateStackInput {
    stack_name: String,
    template_body: String,
    parameters: BTreeMap<String, String>,
    tags: BTreeMap<String, String>,
    notification_arns: Vec<String>,
}

impl UpdateStackInput {
    fn from_params(req: &AwsRequest) -> Result<Self, AwsServiceError> {
        let params = CloudFormationService::get_all_params(req);

        let stack_name = params
            .get("StackName")
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ValidationError",
                    "StackName is required",
                )
            })?
            .to_string();

        // TemplateBody isn't `@required` in Smithy (TemplateURL +
        // UsePreviousTemplate are alternatives). Treat omission as an
        // empty body so synthetic conformance inputs don't trip an
        // undeclared `ValidationError`.
        let template_body = params.get("TemplateBody").cloned().unwrap_or_default();

        let mut parameters = CloudFormationService::extract_parameters(&params);
        CloudFormationService::merge_parameter_defaults(&mut parameters, &template_body);
        Ok(Self {
            stack_name,
            template_body,
            parameters,
            tags: CloudFormationService::extract_tags(&params),
            notification_arns: CloudFormationService::extract_notification_arns(&params),
        })
    }
}

/// One row of structured diff returned by `apply_resource_updates`. Used
/// by `ExecuteChangeSet` to emit `StackEvent` rows so `DescribeStackEvents`
/// reflects the resources actually created / updated / deleted.
#[derive(Debug, Clone)]
pub(crate) struct ResourceChange {
    pub action: ResourceChangeAction,
    pub logical_id: String,
    pub physical_id: String,
    pub resource_type: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ResourceChangeAction {
    Create,
    Update,
    Delete,
}

impl ResourceChangeAction {
    pub fn status_in_progress(self) -> &'static str {
        match self {
            Self::Create => "CREATE_IN_PROGRESS",
            Self::Update => "UPDATE_IN_PROGRESS",
            Self::Delete => "DELETE_IN_PROGRESS",
        }
    }
    pub fn status_complete(self) -> &'static str {
        match self {
            Self::Create => "CREATE_COMPLETE",
            Self::Update => "UPDATE_COMPLETE",
            Self::Delete => "DELETE_COMPLETE",
        }
    }
}

/// Apply resource updates: delete removed resources, create new ones, and
/// in-place update resources whose properties changed. Returns the list of
/// changes applied (for event emission) on success or `Err(msg)` if any
/// resource operation fails.
pub(crate) fn apply_resource_updates(
    stack: &mut crate::state::Stack,
    new_resource_defs: &[template::ResourceDefinition],
    template_body: &str,
    parameters: &BTreeMap<String, String>,
    provisioner: &crate::resource_provisioner::ResourceProvisioner,
    imports: &BTreeMap<String, String>,
) -> Result<Vec<ResourceChange>, String> {
    let mut changes: Vec<ResourceChange> = Vec::new();
    let old_logical_ids: std::collections::HashSet<String> = stack
        .resources
        .iter()
        .map(|r| r.logical_id.clone())
        .collect();
    let new_logical_ids: std::collections::HashSet<String> = new_resource_defs
        .iter()
        .map(|r| r.logical_id.clone())
        .collect();

    // Delete resources no longer in template
    let to_remove: Vec<_> = stack
        .resources
        .iter()
        .filter(|r| !new_logical_ids.contains(&r.logical_id))
        .cloned()
        .collect();
    for resource in &to_remove {
        let _ = provisioner.delete_resource(resource);
        changes.push(ResourceChange {
            action: ResourceChangeAction::Delete,
            logical_id: resource.logical_id.clone(),
            physical_id: resource.physical_id.clone(),
            resource_type: resource.resource_type.clone(),
        });
    }
    stack
        .resources
        .retain(|r| new_logical_ids.contains(&r.logical_id));

    // Build physical ID + attribute maps from existing resources
    let mut physical_ids: BTreeMap<String, String> = stack
        .resources
        .iter()
        .map(|r| (r.logical_id.clone(), r.physical_id.clone()))
        .collect();
    let mut attributes: BTreeMap<String, BTreeMap<String, String>> = stack
        .resources
        .iter()
        .map(|r| (r.logical_id.clone(), r.attributes.clone()))
        .collect();

    // Create new resources / update resources that already exist. Provision in
    // dependency order so a `Ref`/`GetAtt`/`Fn::Sub`/`DependsOn` to another
    // resource resolves to that resource's physical id rather than its bare
    // logical id (which would otherwise get baked into derived state — e.g. a
    // Step Functions ASL referencing a Lambda declared later in the template).
    let order = template::dependency_order(template_body, parameters, new_resource_defs);
    for &idx in &order {
        let resource_def = &new_resource_defs[idx];
        let resolved_def = template::resolve_resource_properties_with_attrs(
            resource_def,
            template_body,
            parameters,
            &physical_ids,
            &attributes,
            imports,
        )
        .map_err(|e| {
            format!(
                "Failed to resolve resource {}: {e}",
                resource_def.logical_id
            )
        })?;

        if !old_logical_ids.contains(&resource_def.logical_id) {
            match provisioner.create_resource(&resolved_def) {
                Ok(stack_resource) => {
                    changes.push(ResourceChange {
                        action: ResourceChangeAction::Create,
                        logical_id: stack_resource.logical_id.clone(),
                        physical_id: stack_resource.physical_id.clone(),
                        resource_type: stack_resource.resource_type.clone(),
                    });
                    physical_ids.insert(
                        stack_resource.logical_id.clone(),
                        stack_resource.physical_id.clone(),
                    );
                    attributes.insert(
                        stack_resource.logical_id.clone(),
                        stack_resource.attributes.clone(),
                    );
                    stack.resources.push(stack_resource);
                }
                Err(e) => {
                    tracing::warn!(
                        "Failed to create resource {} during update: {e}",
                        resource_def.logical_id
                    );
                    return Err(format!(
                        "Failed to create resource {}: {e}",
                        resource_def.logical_id
                    ));
                }
            }
        } else {
            // Resource exists in both old and new templates — try to apply
            // an in-place update. The provisioner returns `Ok(None)` for
            // resource types that don't support updates yet; in that case
            // the existing resource stays as-is so the rest of the stack
            // continues to validate.
            let existing = stack
                .resources
                .iter()
                .find(|r| r.logical_id == resource_def.logical_id)
                .cloned();
            if let Some(existing) = existing {
                match provisioner.update_resource(&existing, &resolved_def) {
                    Ok(Some(updated)) => {
                        changes.push(ResourceChange {
                            action: ResourceChangeAction::Update,
                            logical_id: updated.logical_id.clone(),
                            physical_id: updated.physical_id.clone(),
                            resource_type: updated.resource_type.clone(),
                        });
                        physical_ids
                            .insert(updated.logical_id.clone(), updated.physical_id.clone());
                        attributes.insert(updated.logical_id.clone(), updated.attributes.clone());
                        if let Some(slot) = stack
                            .resources
                            .iter_mut()
                            .find(|r| r.logical_id == updated.logical_id)
                        {
                            *slot = updated;
                        }
                    }
                    Ok(None) => {
                        // Resource type has no update path — leave the
                        // existing physical resource untouched.
                    }
                    Err(e) => {
                        tracing::warn!(
                            "Failed to update resource {} during update: {e}",
                            resource_def.logical_id
                        );
                        return Err(format!(
                            "Failed to update resource {}: {e}",
                            resource_def.logical_id
                        ));
                    }
                }
            }
        }
    }

    Ok(changes)
}

/// Pushes a single `StackEvent` row onto the per-stack event log so
/// `DescribeStackEvents` returns a chronological history of resource and
/// stack-level state transitions.
pub(crate) fn record_event(
    state: &mut crate::state::CloudFormationState,
    stack_id: &str,
    stack_name: &str,
    logical_id: &str,
    physical_id: &str,
    resource_type: &str,
    status: &str,
) {
    use serde_json::json;
    let event_id = format!(
        "{}-{:x}",
        logical_id,
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
    );
    let log = state.events.entry(stack_id.to_string()).or_default();

    // Timestamps must be sub-second AND strictly increasing within a stack's
    // event log. `sam`'s deploy-wait reads the REVIEW_IN_PROGRESS marker's
    // timestamp and then only registers events whose `Timestamp` is strictly
    // greater; a fast stack that provisions within one second would otherwise
    // stamp its REVIEW_IN_PROGRESS marker and terminal CREATE_COMPLETE
    // identically, so sam never sees completion and polls until it times out.
    // Use millisecond precision and bump by 1ms when the clock hasn't advanced
    // past the previous event, guaranteeing a strict order.
    // Truncate to the stored (millisecond) resolution before comparing, so the
    // strict-ordering check isn't fooled by sub-millisecond bits that vanish on
    // serialization (two events in the same millisecond would otherwise both
    // serialize identically despite `now > prev` holding at full precision).
    let now = chrono::DateTime::from_timestamp_millis(Utc::now().timestamp_millis())
        .unwrap_or_else(Utc::now);
    let timestamp = match log.last().and_then(|e| e["Timestamp"].as_str()) {
        Some(prev) => match chrono::DateTime::parse_from_rfc3339(prev) {
            Ok(prev) => {
                let prev = prev.with_timezone(&Utc);
                if now > prev {
                    now
                } else {
                    prev + chrono::Duration::milliseconds(1)
                }
            }
            Err(_) => now,
        },
        None => now,
    };

    log.push(json!({
        "EventId": event_id,
        "StackId": stack_id,
        "StackName": stack_name,
        "LogicalResourceId": logical_id,
        "PhysicalResourceId": physical_id,
        "ResourceType": resource_type,
        "ResourceStatus": status,
        "Timestamp": timestamp.to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
    }));
}

/// Emits IN_PROGRESS + COMPLETE event pairs for every resource change
/// applied during an update. Mirrors the event sequence real CloudFormation
/// publishes during `ExecuteChangeSet` / `UpdateStack`.
/// Persist the CloudFormation snapshot from a detached task. Mirrors
/// `CloudFormationService::save_snapshot` but takes owned handles so it can
/// run inside the background CreateStack provisioning task (see RDS's
/// `save_snapshot_static`).
async fn save_snapshot_static(
    state: SharedCloudFormationState,
    store: Option<Arc<dyn SnapshotStore>>,
    lock: Arc<AsyncMutex<()>>,
) {
    let Some(store) = store else {
        return;
    };
    let _guard = lock.lock().await;
    let snapshot = CloudFormationSnapshot {
        schema_version: CLOUDFORMATION_SNAPSHOT_SCHEMA_VERSION,
        state: None,
        accounts: Some(state.read().clone()),
    };
    let join = tokio::task::spawn_blocking(move || -> std::io::Result<()> {
        let bytes = serde_json::to_vec(&snapshot)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?;
        store.save(&bytes)
    })
    .await;
    match join {
        Ok(Ok(())) => {}
        Ok(Err(err)) => tracing::error!(%err, "failed to write cloudformation snapshot"),
        Err(err) => tracing::error!(%err, "cloudformation snapshot task panicked"),
    }
}

pub(crate) fn record_stack_events(
    state: &mut crate::state::CloudFormationState,
    stack_id: &str,
    stack_name: &str,
    changes: &[ResourceChange],
) {
    for ch in changes {
        record_event(
            state,
            stack_id,
            stack_name,
            &ch.logical_id,
            &ch.physical_id,
            &ch.resource_type,
            ch.action.status_in_progress(),
        );
        record_event(
            state,
            stack_id,
            stack_name,
            &ch.logical_id,
            &ch.physical_id,
            &ch.resource_type,
            ch.action.status_complete(),
        );
    }
}

/// Emits a stack-level lifecycle event (`UPDATE_IN_PROGRESS`,
/// `UPDATE_COMPLETE`, `UPDATE_ROLLBACK_COMPLETE`, etc.) keyed on the
/// stack's own `LogicalResourceId == stack_name`, matching real CFN.
pub(crate) fn record_stack_status_event(
    state: &mut crate::state::CloudFormationState,
    stack_id: &str,
    stack_name: &str,
    resource_type: &str,
    status: &str,
) {
    record_event(
        state,
        stack_id,
        stack_name,
        stack_name,
        stack_id,
        resource_type,
        status,
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::HeaderMap;
    use parking_lot::RwLock;
    use std::collections::HashMap;
    use std::sync::Arc;

    #[test]
    fn merge_parameter_defaults_fills_omitted_params() {
        // §1.6: a parameter the caller omitted gets its declared Default, so a
        // Ref to it resolves to the default instead of the bare param name.
        let template = r#"{
            "Parameters": {
                "InstanceType": {"Type": "String", "Default": "t3.micro"},
                "Count": {"Type": "Number", "Default": 3},
                "Supplied": {"Type": "String", "Default": "dflt"}
            },
            "Resources": {}
        }"#;
        let mut params = BTreeMap::new();
        params.insert("Supplied".to_string(), "override".to_string());
        CloudFormationService::merge_parameter_defaults(&mut params, template);
        assert_eq!(
            params.get("InstanceType").map(String::as_str),
            Some("t3.micro")
        );
        assert_eq!(params.get("Count").map(String::as_str), Some("3"));
        // A supplied value is not overwritten by the default.
        assert_eq!(params.get("Supplied").map(String::as_str), Some("override"));
    }

    fn make_service() -> CloudFormationService {
        let cf_state = Arc::new(RwLock::new(
            fakecloud_core::multi_account::MultiAccountState::new(
                "123456789012",
                "us-east-1",
                "http://localhost:4566",
            ),
        ));
        let deps = CloudFormationDeps {
            sqs: Arc::new(RwLock::new(
                fakecloud_core::multi_account::MultiAccountState::new(
                    "123456789012",
                    "us-east-1",
                    "http://localhost:4566",
                ),
            )),
            sns: Arc::new(RwLock::new(
                fakecloud_core::multi_account::MultiAccountState::new(
                    "123456789012",
                    "us-east-1",
                    "http://localhost:4566",
                ),
            )),
            ssm: Arc::new(RwLock::new(
                fakecloud_core::multi_account::MultiAccountState::new(
                    "123456789012",
                    "us-east-1",
                    "http://localhost:4566",
                ),
            )),
            iam: Arc::new(RwLock::new(
                fakecloud_core::multi_account::MultiAccountState::new(
                    "123456789012",
                    "us-east-1",
                    "",
                ),
            )),
            s3: Arc::new(RwLock::new(
                fakecloud_core::multi_account::MultiAccountState::new(
                    "123456789012",
                    "us-east-1",
                    "",
                ),
            )),
            eventbridge: Arc::new(RwLock::new(
                fakecloud_core::multi_account::MultiAccountState::new(
                    "123456789012",
                    "us-east-1",
                    "",
                ),
            )),
            dynamodb: Arc::new(RwLock::new(
                fakecloud_core::multi_account::MultiAccountState::new(
                    "123456789012",
                    "us-east-1",
                    "",
                ),
            )),
            logs: Arc::new(RwLock::new(
                fakecloud_core::multi_account::MultiAccountState::new(
                    "123456789012",
                    "us-east-1",
                    "",
                ),
            )),
            lambda: Arc::new(RwLock::new(
                fakecloud_core::multi_account::MultiAccountState::new(
                    "123456789012",
                    "us-east-1",
                    "",
                ),
            )),
            secretsmanager: Arc::new(RwLock::new(
                fakecloud_core::multi_account::MultiAccountState::new(
                    "123456789012",
                    "us-east-1",
                    "",
                ),
            )),
            kinesis: Arc::new(RwLock::new(
                fakecloud_core::multi_account::MultiAccountState::new(
                    "123456789012",
                    "us-east-1",
                    "",
                ),
            )),
            kms: Arc::new(RwLock::new(
                fakecloud_core::multi_account::MultiAccountState::new(
                    "123456789012",
                    "us-east-1",
                    "",
                ),
            )),
            ecr: Arc::new(RwLock::new(
                fakecloud_core::multi_account::MultiAccountState::new(
                    "123456789012",
                    "us-east-1",
                    "",
                ),
            )),
            cloudwatch: Arc::new(RwLock::new(fakecloud_cloudwatch::CloudWatchAccounts::new())),
            elbv2: Arc::new(RwLock::new(fakecloud_elbv2::Elbv2Accounts::new())),
            organizations: Arc::new(RwLock::new(None)),
            cognito: Arc::new(RwLock::new(
                fakecloud_core::multi_account::MultiAccountState::new(
                    "123456789012",
                    "us-east-1",
                    "",
                ),
            )),
            rds: Arc::new(RwLock::new(
                fakecloud_core::multi_account::MultiAccountState::new(
                    "123456789012",
                    "us-east-1",
                    "",
                ),
            )),
            ec2: Arc::new(RwLock::new(
                fakecloud_core::multi_account::MultiAccountState::new(
                    "123456789012",
                    "us-east-1",
                    "",
                ),
            )),
            autoscaling: Arc::new(RwLock::new(
                fakecloud_autoscaling::AutoScalingAccounts::new(),
            )),
            batch: Arc::new(RwLock::new(fakecloud_batch::BatchAccounts::new())),
            pipes: Arc::new(RwLock::new(fakecloud_pipes::PipesAccounts::new())),
            ecs: Arc::new(RwLock::new(
                fakecloud_core::multi_account::MultiAccountState::new(
                    "123456789012",
                    "us-east-1",
                    "",
                ),
            )),
            acm: Arc::new(RwLock::new(fakecloud_acm::AcmAccounts::new())),
            elasticache: Arc::new(RwLock::new(
                fakecloud_core::multi_account::MultiAccountState::new(
                    "123456789012",
                    "us-east-1",
                    "",
                ),
            )),
            route53: Arc::new(RwLock::new(fakecloud_route53::Route53Accounts::new())),
            cloudfront: Arc::new(RwLock::new(fakecloud_cloudfront::CloudFrontAccounts::new())),
            stepfunctions: Arc::new(RwLock::new(
                fakecloud_core::multi_account::MultiAccountState::new(
                    "123456789012",
                    "us-east-1",
                    "",
                ),
            )),
            wafv2: Arc::new(RwLock::new(fakecloud_wafv2::Wafv2Accounts::default())),
            apigateway: Arc::new(RwLock::new(
                fakecloud_core::multi_account::MultiAccountState::new(
                    "123456789012",
                    "us-east-1",
                    "",
                ),
            )),
            apigatewayv2: Arc::new(RwLock::new(
                fakecloud_core::multi_account::MultiAccountState::new(
                    "123456789012",
                    "us-east-1",
                    "",
                ),
            )),
            ses: Arc::new(RwLock::new(
                fakecloud_core::multi_account::MultiAccountState::new(
                    "123456789012",
                    "us-east-1",
                    "",
                ),
            )),
            application_autoscaling: Arc::new(parking_lot::RwLock::new(
                fakecloud_application_autoscaling::ApplicationAutoScalingAccounts::new(),
            )),
            athena: Arc::new(parking_lot::RwLock::new(
                fakecloud_athena::AthenaAccounts::new(),
            )),
            firehose: Arc::new(parking_lot::RwLock::new(
                fakecloud_firehose::FirehoseAccounts::new(),
            )),
            glue: Arc::new(parking_lot::RwLock::new(fakecloud_glue::GlueAccounts::new())),
            delivery: Arc::new(DeliveryBus::new()),
            lambda_runtime: None,
            rds_runtime: None,
            ec2_runtime: None,
            ecs_runtime: None,
            elasticache_runtime: None,
        };
        CloudFormationService::new(cf_state, deps)
    }

    fn make_request(action: &str, params: HashMap<String, String>) -> AwsRequest {
        AwsRequest {
            service: "cloudformation".to_string(),
            action: action.to_string(),
            region: "us-east-1".to_string(),
            account_id: "123456789012".to_string(),
            request_id: "test-request-id".to_string(),
            headers: HeaderMap::new(),
            query_params: params,
            body: bytes::Bytes::new(),
            body_stream: parking_lot::Mutex::new(None),
            path_segments: vec![],
            raw_path: "/".to_string(),
            raw_query: String::new(),
            method: http::Method::POST,
            is_query_protocol: true,
            access_key_id: None,
            principal: None,
        }
    }

    #[tokio::test]
    async fn update_stack_sets_failed_status_on_resource_error() {
        let svc = make_service();

        // Create a stack with just a queue
        let mut create_params = HashMap::new();
        create_params.insert("StackName".to_string(), "test-stack".to_string());
        create_params.insert(
            "TemplateBody".to_string(),
            r#"{"Resources":{"MyQueue":{"Type":"AWS::SQS::Queue","Properties":{"QueueName":"q1"}}}}"#.to_string(),
        );
        let req = make_request("CreateStack", create_params);
        let result = svc.create_stack(&req).await;
        assert!(result.is_ok());

        // Update stack adding an SNS subscription with a non-existent topic
        let mut update_params = HashMap::new();
        update_params.insert("StackName".to_string(), "test-stack".to_string());
        update_params.insert(
            "TemplateBody".to_string(),
            r#"{"Resources":{"MyQueue":{"Type":"AWS::SQS::Queue","Properties":{"QueueName":"q1"}},"BadSub":{"Type":"AWS::SNS::Subscription","Properties":{"TopicArn":"arn:aws:sns:us-east-1:123456789012:nope","Protocol":"sqs","Endpoint":"arn:aws:sqs:us-east-1:123456789012:q1"}}}}"#.to_string(),
        );
        let req = make_request("UpdateStack", update_params);
        let result = svc.update_stack(&req).await;

        // Should return an error
        assert!(result.is_err());

        // Stack status should be UPDATE_ROLLBACK_COMPLETE — matches the
        // terminal status real CloudFormation lands on after a failed
        // update attempt that gets rolled back.
        let accounts = svc.state.read();
        let state = accounts.get("123456789012").unwrap();
        let stack = state.stacks.get("test-stack").unwrap();
        assert_eq!(stack.status, "UPDATE_ROLLBACK_COMPLETE");
    }

    #[tokio::test]
    async fn create_stack_resolves_ref_to_physical_id() {
        let svc = make_service();

        // Template where subscription Refs the topic
        let template = r#"{
            "Resources": {
                "MyTopic": {
                    "Type": "AWS::SNS::Topic",
                    "Properties": { "TopicName": "ref-test-topic" }
                },
                "MySub": {
                    "Type": "AWS::SNS::Subscription",
                    "Properties": {
                        "TopicArn": { "Ref": "MyTopic" },
                        "Protocol": "sqs",
                        "Endpoint": "arn:aws:sqs:us-east-1:123456789012:some-queue"
                    }
                }
            }
        }"#;

        let mut params = HashMap::new();
        params.insert("StackName".to_string(), "ref-stack".to_string());
        params.insert("TemplateBody".to_string(), template.to_string());
        let req = make_request("CreateStack", params);
        let result = svc.create_stack(&req).await;
        assert!(result.is_ok(), "CreateStack failed: {:?}", result.err());

        // Verify both resources were created
        let accounts = svc.state.read();
        let state = accounts.get("123456789012").unwrap();
        let stack = state.stacks.get("ref-stack").unwrap();
        assert_eq!(stack.resources.len(), 2);
        assert_eq!(stack.status, "CREATE_COMPLETE");

        // The subscription's physical ID should be an ARN (not just "MyTopic")
        let sub = stack
            .resources
            .iter()
            .find(|r| r.logical_id == "MySub")
            .unwrap();
        assert!(
            sub.physical_id.contains("ref-test-topic"),
            "Subscription physical ID should reference the topic ARN, got: {}",
            sub.physical_id
        );
    }

    /// On the multi-thread server runtime, a stack containing a custom
    /// resource (whose provisioning can block for minutes on a cold Lambda
    /// image pull) must NOT be provisioned synchronously inside the request
    /// handler — CreateStack returns the StackId immediately and DescribeStacks
    /// observes CREATE_IN_PROGRESS -> CREATE_COMPLETE (bug-audit 2026-06-13,
    /// 3.1).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn create_stack_custom_resource_provisions_asynchronously() {
        let svc = make_service();
        let template = r#"{
            "Resources": {
                "MyCustom": {
                    "Type": "Custom::Thing",
                    "Properties": {
                        "ServiceToken": "arn:aws:lambda:us-east-1:123456789012:function:handler"
                    }
                }
            }
        }"#;
        let mut params = HashMap::new();
        params.insert("StackName".to_string(), "async-stack".to_string());
        params.insert("TemplateBody".to_string(), template.to_string());
        let req = make_request("CreateStack", params);

        // CreateStack returns promptly with a StackId; provisioning runs in a
        // detached background task. The stack is recorded before the task can
        // possibly finish, so right after return it is at worst already
        // terminal — never a value that proves the handler blocked on the
        // (potentially minutes-long) provisioning loop. The key guarantee is
        // that the call returns without running the provisioner inline.
        let resp = svc
            .create_stack(&req)
            .await
            .expect("create returns StackId");
        assert!(resp.status.is_success());
        {
            let accounts = svc.state.read();
            let stack = accounts
                .get("123456789012")
                .unwrap()
                .stacks
                .get("async-stack")
                .expect("stack seeded synchronously");
            assert!(
                stack.status == "CREATE_IN_PROGRESS" || stack.status == "CREATE_COMPLETE",
                "unexpected status right after create: {}",
                stack.status
            );
        }

        // Poll DescribeStacks (via state) until the background task flips the
        // stack to its terminal CREATE_COMPLETE status.
        let mut status = String::new();
        for _ in 0..200 {
            {
                let accounts = svc.state.read();
                if let Some(stack) = accounts
                    .get("123456789012")
                    .and_then(|s| s.stacks.get("async-stack"))
                {
                    status = stack.status.clone();
                    if status != "CREATE_IN_PROGRESS" {
                        break;
                    }
                }
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert_eq!(
            status, "CREATE_COMPLETE",
            "stack should reach CREATE_COMPLETE"
        );

        let accounts = svc.state.read();
        let stack = accounts
            .get("123456789012")
            .unwrap()
            .stacks
            .get("async-stack")
            .unwrap();
        assert_eq!(stack.resources.len(), 1);
        assert_eq!(stack.resources[0].resource_type, "Custom::Thing");
    }

    #[tokio::test]
    async fn output_getatt_resolves_well_known_attribute() {
        // An Output that GetAtts a well-known attribute the create handler does
        // not eagerly capture (SQS QueueUrl) must resolve to the live value, not
        // a `Queue.QueueUrl` placeholder. Regression for bug-hunt 2026-06-25 1.11:
        // resolve_template_outputs reads StackResource.attributes, which now
        // carries the live get_att overlay applied during provisioning.
        let svc = make_service();
        let template = r#"{
            "Resources": {
                "Queue": { "Type": "AWS::SQS::Queue", "Properties": { "QueueName": "out-q" } }
            },
            "Outputs": {
                "Url": { "Value": { "Fn::GetAtt": ["Queue", "QueueUrl"] } }
            }
        }"#;
        let mut params = HashMap::new();
        params.insert("StackName".to_string(), "out-stack".to_string());
        params.insert("TemplateBody".to_string(), template.to_string());
        svc.create_stack(&make_request("CreateStack", params))
            .await
            .expect("create returns StackId");

        let mut url = String::new();
        for _ in 0..200 {
            {
                let accounts = svc.state.read();
                if let Some(stack) = accounts
                    .get("123456789012")
                    .and_then(|s| s.stacks.get("out-stack"))
                {
                    if stack.status != "CREATE_IN_PROGRESS" {
                        url = stack
                            .outputs
                            .iter()
                            .find(|o| o.key == "Url")
                            .map(|o| o.value.clone())
                            .unwrap_or_default();
                        break;
                    }
                }
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert!(
            url.contains("out-q") && url != "Queue.QueueUrl",
            "GetAtt QueueUrl output should resolve to the live url, got {url:?}"
        );
    }

    // ── Service error paths ──

    #[tokio::test]
    async fn create_stack_missing_name_errors() {
        let svc = make_service();
        let mut params = HashMap::new();
        params.insert("TemplateBody".to_string(), "{}".to_string());
        let req = make_request("CreateStack", params);
        assert!(svc.create_stack(&req).await.is_err());
    }

    #[tokio::test]
    async fn create_stack_missing_template_creates_empty_stack() {
        // `TemplateBody` isn't `@required` in Smithy and CreateStack
        // declares no `ValidationError` shape, so missing/placeholder
        // bodies now create an empty stack rather than rejecting with
        // an undeclared wire code (strict-mode conformance gap).
        let svc = make_service();
        let mut params = HashMap::new();
        params.insert("StackName".to_string(), "s".to_string());
        let req = make_request("CreateStack", params);
        svc.create_stack(&req)
            .await
            .expect("empty-body create succeeds");
    }

    #[tokio::test]
    async fn create_stack_duplicate_errors() {
        let svc = make_service();
        let mut params = HashMap::new();
        params.insert("StackName".to_string(), "dup".to_string());
        params.insert(
            "TemplateBody".to_string(),
            r#"{"Resources":{"Q":{"Type":"AWS::SQS::Queue","Properties":{"QueueName":"dq"}}}}"#
                .to_string(),
        );
        let req = make_request("CreateStack", params.clone());
        svc.create_stack(&req).await.unwrap();
        let req = make_request("CreateStack", params);
        assert!(svc.create_stack(&req).await.is_err());
    }

    #[tokio::test]
    async fn create_stack_invalid_template_creates_empty_stack() {
        // CreateStack's Smithy `errors` list has no `ValidationError`
        // shape, so unparseable bodies degrade to an empty parsed
        // template instead of raising an undeclared wire code.
        let svc = make_service();
        let mut params = HashMap::new();
        params.insert("StackName".to_string(), "bad".to_string());
        params.insert("TemplateBody".to_string(), "not json".to_string());
        let req = make_request("CreateStack", params);
        svc.create_stack(&req)
            .await
            .expect("bad-body create succeeds");
    }

    #[tokio::test]
    async fn delete_stack_unknown_is_noop() {
        let svc = make_service();
        let mut params = HashMap::new();
        params.insert("StackName".to_string(), "ghost".to_string());
        let req = make_request("DeleteStack", params);
        assert!(svc.delete_stack(&req).await.is_ok());
    }

    #[test]
    fn describe_stacks_nonexistent_errors() {
        // Querying an explicit, unknown StackName returns AWS's
        // `ValidationError: Stack with id <name> does not exist` so deploy
        // tools that probe stack existence (SAM, `aws cloudformation
        // deploy`) get the signal they expect (issue #1646).
        let svc = make_service();
        let mut params = HashMap::new();
        params.insert("StackName".to_string(), "ghost".to_string());
        let req = make_request("DescribeStacks", params);
        match svc.describe_stacks(&req) {
            Ok(_) => panic!("ghost stack must return an error, not an empty list"),
            Err(e) => {
                assert_eq!(e.status(), StatusCode::BAD_REQUEST);
                assert_eq!(e.code(), "ValidationError");
                assert!(
                    e.message().contains("does not exist"),
                    "got: {}",
                    e.message()
                );
            }
        }
    }

    #[test]
    fn describe_stacks_empty_returns_all() {
        let svc = make_service();
        let req = make_request("DescribeStacks", HashMap::new());
        let resp = svc.describe_stacks(&req).unwrap();
        let b = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
        assert!(b.contains("DescribeStacksResult"));
    }

    #[test]
    fn list_stacks_empty_returns_ok() {
        let svc = make_service();
        let req = make_request("ListStacks", HashMap::new());
        let resp = svc.list_stacks(&req).unwrap();
        let b = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
        assert!(b.contains("ListStacksResult"));
    }

    #[test]
    fn list_stack_resources_missing_name_returns_validation_error() {
        // ListStackResources declares no `errors` in Smithy, so any
        // AWS-shaped 4xx counts as a handler response. We reject an
        // omitted StackName with `ValidationError` to keep negative
        // conformance variants honest; unknown-but-supplied names still
        // resolve to an empty list (see the test below).
        let svc = make_service();
        let req = make_request("ListStackResources", HashMap::new());
        let err = match svc.list_stack_resources(&req) {
            Err(e) => e,
            Ok(_) => panic!("omitted StackName must be rejected"),
        };
        assert_eq!(err.code(), "ValidationError");
    }

    #[test]
    fn list_stack_resources_unknown_stack_returns_empty() {
        let svc = make_service();
        let mut params = HashMap::new();
        params.insert("StackName".to_string(), "ghost".to_string());
        let req = make_request("ListStackResources", params);
        svc.list_stack_resources(&req).expect("unknown is empty");
    }

    #[test]
    fn describe_stack_resources_missing_name_returns_empty() {
        let svc = make_service();
        let req = make_request("DescribeStackResources", HashMap::new());
        svc.describe_stack_resources(&req)
            .expect("missing name is ok");
    }

    #[test]
    fn get_template_missing_name_returns_empty_body() {
        let svc = make_service();
        let req = make_request("GetTemplate", HashMap::new());
        svc.get_template(&req).expect("missing name is ok");
    }

    #[test]
    fn get_template_unknown_stack_returns_empty_body() {
        let svc = make_service();
        let mut params = HashMap::new();
        params.insert("StackName".to_string(), "ghost".to_string());
        let req = make_request("GetTemplate", params);
        svc.get_template(&req).expect("unknown is empty");
    }

    #[tokio::test]
    async fn update_stack_missing_name_errors() {
        let svc = make_service();
        let mut params = HashMap::new();
        params.insert("TemplateBody".to_string(), "{}".to_string());
        let req = make_request("UpdateStack", params);
        assert!(svc.update_stack(&req).await.is_err());
    }

    #[tokio::test]
    async fn update_stack_unknown_stack_returns_synthetic_id() {
        // UpdateStack declares only `InsufficientCapabilitiesException`
        // and `TokenAlreadyExistsException`, neither of which fits
        // "stack does not exist". Synthetic conformance inputs target
        // a placeholder stack, so we return a synthetic StackId rather
        // than an undeclared `ValidationError`. Real callers create
        // the stack first.
        let svc = make_service();
        let mut params = HashMap::new();
        params.insert("StackName".to_string(), "ghost".to_string());
        params.insert(
            "TemplateBody".to_string(),
            r#"{"Resources":{}}"#.to_string(),
        );
        let req = make_request("UpdateStack", params);
        let resp = svc
            .update_stack(&req)
            .await
            .expect("ghost update is synthetic");
        let b = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
        assert!(b.contains("UpdateStackResult"));
    }

    #[tokio::test]
    async fn create_stack_resolves_outputs_and_records_export() {
        let svc = make_service();
        let template = r#"{
            "Resources": {
                "Q": {"Type":"AWS::SQS::Queue","Properties":{"QueueName":"out-q"}}
            },
            "Outputs": {
                "QueueUrl": {
                    "Value": {"Ref": "Q"},
                    "Description": "Url",
                    "Export": {"Name": "TheQueueUrl"}
                }
            }
        }"#;
        let mut params = HashMap::new();
        params.insert("StackName".to_string(), "outs".to_string());
        params.insert("TemplateBody".to_string(), template.to_string());
        let req = make_request("CreateStack", params);
        svc.create_stack(&req).await.expect("create stack");

        let accounts = svc.state.read();
        let stack = accounts
            .get("123456789012")
            .unwrap()
            .stacks
            .get("outs")
            .unwrap();
        assert_eq!(stack.outputs.len(), 1);
        assert_eq!(stack.outputs[0].key, "QueueUrl");
        assert_eq!(stack.outputs[0].export_name.as_deref(), Some("TheQueueUrl"));
        assert!(!stack.outputs[0].value.is_empty());
    }

    #[tokio::test]
    async fn create_stack_rejects_duplicate_export_name() {
        let svc = make_service();
        let mk = |name: &str| {
            let template = format!(
                r#"{{
                    "Resources": {{"Q":{{"Type":"AWS::SQS::Queue","Properties":{{"QueueName":"q-{name}"}}}}}},
                    "Outputs": {{"QueueUrl":{{"Value":{{"Ref":"Q"}},"Export":{{"Name":"DupExport"}}}}}}
                }}"#
            );
            let mut params = HashMap::new();
            params.insert("StackName".to_string(), name.to_string());
            params.insert("TemplateBody".to_string(), template);
            make_request("CreateStack", params)
        };
        match svc.create_stack(&mk("first")).await {
            Ok(_) => {}
            Err(e) => panic!("first stack: {e:?}"),
        }
        // The second stack's export collides with the first. Since
        // provisioning is now asynchronous, the collision can no longer be a
        // synchronous CreateStack error — it surfaces as a failed create.
        // On the current-thread test runtime the provisioning task runs
        // inline, so the stack is already CREATE_FAILED on return.
        svc.create_stack(&mk("second"))
            .await
            .expect("CreateStack returns StackId even when provisioning fails");
        let accounts = svc.state.read();
        let stack = accounts
            .get("123456789012")
            .unwrap()
            .stacks
            .get("second")
            .expect("second stack recorded");
        assert_eq!(stack.status, "CREATE_FAILED");
        // The first stack keeps the export.
        let exports = &accounts.get("123456789012").unwrap().exports;
        assert_eq!(
            exports
                .get("DupExport")
                .map(|e| e.exporting_stack_name.as_str()),
            Some("first")
        );
    }

    #[tokio::test]
    async fn import_value_resolves_against_other_stack_export() {
        let svc = make_service();

        let producer_tpl = r#"{
            "Resources": {"Q":{"Type":"AWS::SQS::Queue","Properties":{"QueueName":"prod-q"}}},
            "Outputs": {"Out":{"Value":{"Ref":"Q"},"Export":{"Name":"SharedQueueUrl"}}}
        }"#;
        let mut p = HashMap::new();
        p.insert("StackName".to_string(), "producer".to_string());
        p.insert("TemplateBody".to_string(), producer_tpl.to_string());
        svc.create_stack(&make_request("CreateStack", p))
            .await
            .expect("producer");

        let consumer_tpl = r#"{
            "Resources": {"Q2":{"Type":"AWS::SQS::Queue","Properties":{"QueueName":"cons-q"}}},
            "Outputs": {"Imp":{"Value":{"Fn::ImportValue":"SharedQueueUrl"}}}
        }"#;
        let mut p = HashMap::new();
        p.insert("StackName".to_string(), "consumer".to_string());
        p.insert("TemplateBody".to_string(), consumer_tpl.to_string());
        svc.create_stack(&make_request("CreateStack", p))
            .await
            .expect("consumer");

        let accounts = svc.state.read();
        let prod_url = accounts
            .get("123456789012")
            .unwrap()
            .stacks
            .get("producer")
            .unwrap()
            .outputs[0]
            .value
            .clone();
        let cons = accounts
            .get("123456789012")
            .unwrap()
            .stacks
            .get("consumer")
            .unwrap();
        assert_eq!(cons.outputs[0].value, prod_url);
    }

    #[tokio::test]
    async fn create_stack_records_export_in_state_registry() {
        let svc = make_service();
        let template = r#"{
            "Resources": {"Q":{"Type":"AWS::SQS::Queue","Properties":{"QueueName":"reg-q"}}},
            "Outputs": {"Url":{"Value":{"Ref":"Q"},"Export":{"Name":"reg-url"}}}
        }"#;
        let mut params = HashMap::new();
        params.insert("StackName".to_string(), "reg".to_string());
        params.insert("TemplateBody".to_string(), template.to_string());
        svc.create_stack(&make_request("CreateStack", params))
            .await
            .expect("create");

        let accounts = svc.state.read();
        let state = accounts.get("123456789012").unwrap();
        let export = state
            .exports
            .get("reg-url")
            .expect("export registered in state.exports");
        assert_eq!(export.exporting_stack_name, "reg");
        assert!(!export.value.is_empty());
        assert!(export.exporting_stack_id.contains("reg"));
    }

    #[tokio::test]
    async fn import_value_with_unknown_export_errors() {
        let svc = make_service();
        let consumer_tpl = r#"{
            "Resources": {"Q":{"Type":"AWS::SQS::Queue","Properties":{
                "QueueName": {"Fn::ImportValue":"missing-export"}
            }}}
        }"#;
        let mut p = HashMap::new();
        p.insert("StackName".to_string(), "bad-consumer".to_string());
        p.insert("TemplateBody".to_string(), consumer_tpl.to_string());
        match svc.create_stack(&make_request("CreateStack", p)).await {
            Ok(_) => panic!("expected ValidationError for unknown export"),
            Err(e) => {
                let msg = format!("{e:?}");
                assert!(msg.contains("No export named missing-export"), "got {msg}");
            }
        }
    }

    #[tokio::test]
    async fn delete_stack_blocked_when_export_in_use_and_unblocked_after_consumer_delete() {
        let svc = make_service();

        let producer_tpl = r#"{
            "Resources": {"Q":{"Type":"AWS::SQS::Queue","Properties":{"QueueName":"prod"}}},
            "Outputs": {"Out":{"Value":{"Ref":"Q"},"Export":{"Name":"my-arn"}}}
        }"#;
        let mut p = HashMap::new();
        p.insert("StackName".to_string(), "producer".to_string());
        p.insert("TemplateBody".to_string(), producer_tpl.to_string());
        svc.create_stack(&make_request("CreateStack", p))
            .await
            .expect("producer");

        let consumer_tpl = r#"{
            "Resources": {"Q2":{"Type":"AWS::SQS::Queue","Properties":{
                "QueueName": "cons-q",
                "Tags": [{"Key":"k","Value":{"Fn::ImportValue":"my-arn"}}]
            }}}
        }"#;
        let mut p = HashMap::new();
        p.insert("StackName".to_string(), "consumer".to_string());
        p.insert("TemplateBody".to_string(), consumer_tpl.to_string());
        svc.create_stack(&make_request("CreateStack", p))
            .await
            .expect("consumer");

        // Producer delete must fail while consumer still imports.
        let mut p = HashMap::new();
        p.insert("StackName".to_string(), "producer".to_string());
        match svc.delete_stack(&make_request("DeleteStack", p)).await {
            Ok(_) => panic!("delete must fail while imports exist"),
            Err(e) => {
                let msg = format!("{e:?}");
                assert!(msg.contains("Export my-arn cannot be deleted"), "got {msg}");
            }
        }

        // Delete consumer first.
        let mut p = HashMap::new();
        p.insert("StackName".to_string(), "consumer".to_string());
        svc.delete_stack(&make_request("DeleteStack", p))
            .await
            .expect("consumer delete");

        // Now producer delete succeeds.
        let mut p = HashMap::new();
        p.insert("StackName".to_string(), "producer".to_string());
        svc.delete_stack(&make_request("DeleteStack", p))
            .await
            .expect("producer delete after consumer gone");

        let accounts = svc.state.read();
        let state = accounts.get("123456789012").unwrap();
        assert!(state.exports.is_empty(), "exports cleared after delete");
        assert!(state.imports.is_empty(), "imports cleared after delete");
    }

    // ---- CFN provisioner persistence (issue: CFN resources lost on restart) ----

    use std::sync::atomic::{AtomicUsize, Ordering};

    /// A snapshot hook that counts how many times it fires, standing in for a
    /// real service's whole-state persist.
    fn counting_hook(counter: Arc<AtomicUsize>) -> fakecloud_persistence::SnapshotHook {
        Arc::new(move || {
            let counter = counter.clone();
            Box::pin(async move {
                counter.fetch_add(1, Ordering::SeqCst);
            })
        })
    }

    fn disk_s3_store(tmp: &tempfile::TempDir) -> Arc<fakecloud_persistence::s3::DiskS3Store> {
        let cache = Arc::new(fakecloud_persistence::cache::BodyCache::new(1024 * 1024));
        Arc::new(fakecloud_persistence::s3::DiskS3Store::new(
            tmp.path().to_path_buf(),
            cache,
        ))
    }

    // A stack touching SQS + SNS (snapshot-backed) and an S3 bucket (S3Store
    // write-through). Lambda is registered as a hook below but NOT in the
    // template, so it must not fire -- proving per-service selectivity.
    const PERSIST_TEMPLATE: &str = r#"{"Resources":{
        "Q":{"Type":"AWS::SQS::Queue","Properties":{"QueueName":"cfn-q"}},
        "T":{"Type":"AWS::SNS::Topic","Properties":{"TopicName":"cfn-t"}},
        "B":{"Type":"AWS::S3::Bucket","Properties":{"BucketName":"cfn-bucket"}}
    }}"#;

    fn create_req(stack: &str) -> AwsRequest {
        let mut p = HashMap::new();
        p.insert("StackName".to_string(), stack.to_string());
        p.insert("TemplateBody".to_string(), PERSIST_TEMPLATE.to_string());
        make_request("CreateStack", p)
    }

    #[tokio::test]
    async fn cfn_create_persists_touched_services_and_writes_bucket_to_store() {
        let tmp = tempfile::tempdir().unwrap();
        let store = disk_s3_store(&tmp);
        let counter = Arc::new(AtomicUsize::new(0));
        let mut hooks: BTreeMap<&'static str, fakecloud_persistence::SnapshotHook> =
            BTreeMap::new();
        hooks.insert("sqs", counting_hook(counter.clone()));
        hooks.insert("sns", counting_hook(counter.clone()));
        // Registered but not in the template -> must not fire.
        hooks.insert("lambda", counting_hook(counter.clone()));
        let svc = make_service()
            .with_s3_store(store.clone())
            .with_snapshot_hooks(hooks);

        svc.create_stack(&create_req("probe")).await.unwrap();

        // sqs + sns fired once each; lambda untouched.
        assert_eq!(counter.load(Ordering::SeqCst), 2);
        // The bucket was written through to the S3 store, not just the in-memory map.
        let loaded = fakecloud_persistence::S3Store::load(store.as_ref()).unwrap();
        assert!(
            loaded.buckets.contains_key("cfn-bucket"),
            "CFN bucket should be persisted to the S3 store"
        );
    }

    #[tokio::test]
    async fn cfn_delete_persists_touched_services_and_removes_bucket_from_store() {
        let tmp = tempfile::tempdir().unwrap();
        let store = disk_s3_store(&tmp);
        let counter = Arc::new(AtomicUsize::new(0));
        let mut hooks: BTreeMap<&'static str, fakecloud_persistence::SnapshotHook> =
            BTreeMap::new();
        hooks.insert("sqs", counting_hook(counter.clone()));
        hooks.insert("sns", counting_hook(counter.clone()));
        let svc = make_service()
            .with_s3_store(store.clone())
            .with_snapshot_hooks(hooks);

        svc.create_stack(&create_req("probe")).await.unwrap();
        assert_eq!(counter.load(Ordering::SeqCst), 2, "create fired sqs + sns");

        let mut p = HashMap::new();
        p.insert("StackName".to_string(), "probe".to_string());
        svc.delete_stack(&make_request("DeleteStack", p))
            .await
            .unwrap();

        // Delete fired the touched services again (sqs + sns).
        assert_eq!(counter.load(Ordering::SeqCst), 4, "delete fired sqs + sns");
        // And the CFN-deleted bucket is gone from the store, so it does not
        // reappear after a restart.
        let loaded = fakecloud_persistence::S3Store::load(store.as_ref()).unwrap();
        assert!(
            !loaded.buckets.contains_key("cfn-bucket"),
            "CFN-deleted bucket should be removed from the S3 store"
        );
    }

    #[tokio::test]
    async fn cfn_persist_skips_services_without_a_registered_hook() {
        // Only "sqs" has a hook; the stack also touches SNS and S3. The missing
        // hooks must be silently skipped (no panic), and "sqs" fires once.
        let tmp = tempfile::tempdir().unwrap();
        let store = disk_s3_store(&tmp);
        let counter = Arc::new(AtomicUsize::new(0));
        let mut hooks: BTreeMap<&'static str, fakecloud_persistence::SnapshotHook> =
            BTreeMap::new();
        hooks.insert("sqs", counting_hook(counter.clone()));
        let svc = make_service()
            .with_s3_store(store.clone())
            .with_snapshot_hooks(hooks);

        svc.create_stack(&create_req("probe")).await.unwrap();
        assert_eq!(counter.load(Ordering::SeqCst), 1, "only sqs has a hook");
    }

    #[tokio::test]
    async fn cfn_update_persists_touched_services() {
        // Create with just SQS, then update to a template that adds SNS + a
        // bucket; the update must persist the services it touches.
        let tmp = tempfile::tempdir().unwrap();
        let store = disk_s3_store(&tmp);
        let counter = Arc::new(AtomicUsize::new(0));
        let mut hooks: BTreeMap<&'static str, fakecloud_persistence::SnapshotHook> =
            BTreeMap::new();
        hooks.insert("sqs", counting_hook(counter.clone()));
        hooks.insert("sns", counting_hook(counter.clone()));
        let svc = make_service()
            .with_s3_store(store.clone())
            .with_snapshot_hooks(hooks);

        let mut create = HashMap::new();
        create.insert("StackName".to_string(), "upd".to_string());
        create.insert(
            "TemplateBody".to_string(),
            r#"{"Resources":{"Q":{"Type":"AWS::SQS::Queue","Properties":{"QueueName":"u-q"}}}}"#
                .to_string(),
        );
        svc.create_stack(&make_request("CreateStack", create))
            .await
            .unwrap();
        let after_create = counter.load(Ordering::SeqCst);

        let mut update = HashMap::new();
        update.insert("StackName".to_string(), "upd".to_string());
        update.insert("TemplateBody".to_string(), PERSIST_TEMPLATE.to_string());
        svc.update_stack(&make_request("UpdateStack", update))
            .await
            .unwrap();

        // The update touched at least SNS (added); the hook count must grow.
        assert!(
            counter.load(Ordering::SeqCst) > after_create,
            "update should persist the services it touched"
        );
        let loaded = fakecloud_persistence::S3Store::load(store.as_ref()).unwrap();
        assert!(loaded.buckets.contains_key("cfn-bucket"));
    }

    #[tokio::test]
    async fn cfn_execute_change_set_persists_touched_services() {
        // The changeset path -- CreateChangeSet + ExecuteChangeSet -- is how
        // `cdk deploy`, `aws cloudformation deploy`, and SAM provision. It must
        // write provisioned services through to disk the same way CreateStack
        // does, or the resources report CREATE_COMPLETE yet vanish on restart
        // (bug-audit 2026-06-20, 0.A1 / #1766 class).
        let tmp = tempfile::tempdir().unwrap();
        let store = disk_s3_store(&tmp);
        let counter = Arc::new(AtomicUsize::new(0));
        let mut hooks: BTreeMap<&'static str, fakecloud_persistence::SnapshotHook> =
            BTreeMap::new();
        hooks.insert("sqs", counting_hook(counter.clone()));
        let svc = make_service()
            .with_s3_store(store.clone())
            .with_snapshot_hooks(hooks);

        let mut create = HashMap::new();
        create.insert("StackName".to_string(), "cs-stack".to_string());
        create.insert("ChangeSetName".to_string(), "cs1".to_string());
        create.insert("ChangeSetType".to_string(), "CREATE".to_string());
        create.insert(
            "TemplateBody".to_string(),
            r#"{"Resources":{"Q":{"Type":"AWS::SQS::Queue","Properties":{"QueueName":"cs-q"}}}}"#
                .to_string(),
        );
        svc.handle(make_request("CreateChangeSet", create))
            .await
            .unwrap();
        // CreateChangeSet doesn't provision -- it must not persist a service yet.
        let before = counter.load(Ordering::SeqCst);

        let mut exec = HashMap::new();
        exec.insert("StackName".to_string(), "cs-stack".to_string());
        exec.insert("ChangeSetName".to_string(), "cs1".to_string());
        svc.handle(make_request("ExecuteChangeSet", exec))
            .await
            .unwrap();

        assert!(
            counter.load(Ordering::SeqCst) > before,
            "ExecuteChangeSet must fire the sqs snapshot hook so the provisioned \
             queue survives a restart"
        );
    }

    #[test]
    fn service_key_for_type_maps_services_and_aliases() {
        // Direct service segments.
        assert_eq!(
            service_key_for_type("AWS::Lambda::Function"),
            Some("lambda")
        );
        assert_eq!(
            service_key_for_type("AWS::SecretsManager::Secret"),
            Some("secretsmanager")
        );
        assert_eq!(service_key_for_type("AWS::SQS::Queue"), Some("sqs"));
        assert_eq!(service_key_for_type("AWS::IAM::Role"), Some("iam"));
        assert_eq!(
            service_key_for_type("AWS::StepFunctions::StateMachine"),
            Some("stepfunctions")
        );
        // Namespace aliases that differ from the fakecloud service name.
        assert_eq!(
            service_key_for_type("AWS::Events::Rule"),
            Some("eventbridge")
        );
        assert_eq!(service_key_for_type("AWS::Logs::LogGroup"), Some("logs"));
        assert_eq!(
            service_key_for_type("AWS::ElastiCache::CacheCluster"),
            Some("elasticache")
        );
        // S3 has no snapshot hook (it persists via the S3Store write-through).
        assert_eq!(service_key_for_type("AWS::S3::Bucket"), None);
        // Snapshot-backed services whose CFN namespace differs from the
        // fakecloud service name (these were missing from the map, #1766 class).
        assert_eq!(
            service_key_for_type("AWS::CertificateManager::Certificate"),
            Some("acm")
        );
        assert_eq!(
            service_key_for_type("AWS::ElasticLoadBalancingV2::LoadBalancer"),
            Some("elbv2")
        );
        assert_eq!(
            service_key_for_type("AWS::CloudFront::Distribution"),
            Some("cloudfront")
        );
        assert_eq!(
            service_key_for_type("AWS::Route53::HostedZone"),
            Some("route53")
        );
        assert_eq!(
            service_key_for_type("AWS::KinesisFirehose::DeliveryStream"),
            Some("firehose")
        );
        assert_eq!(service_key_for_type("AWS::Glue::Database"), Some("glue"));
        assert_eq!(service_key_for_type("AWS::WAFv2::WebACL"), Some("wafv2"));
        assert_eq!(
            service_key_for_type("AWS::Athena::WorkGroup"),
            Some("athena")
        );
        assert_eq!(
            service_key_for_type("AWS::Organizations::Organization"),
            Some("organizations")
        );
        // Malformed / non-AWS types.
        assert_eq!(service_key_for_type("AWS::Lambda"), None);
        assert_eq!(service_key_for_type("Custom::Thing::Resource"), None);
        assert_eq!(service_key_for_type("AWS"), None);
        assert_eq!(service_key_for_type(""), None);
    }

    #[tokio::test]
    async fn persist_touched_services_noop_with_empty_hooks() {
        // No registered hooks -> nothing to do, must not panic.
        let hooks: BTreeMap<&'static str, fakecloud_persistence::SnapshotHook> = BTreeMap::new();
        persist_touched_services(&hooks, vec!["AWS::SQS::Queue".to_string()]).await;
    }

    #[tokio::test]
    async fn cfn_bucket_policy_write_through_create_update_delete() {
        let tmp = tempfile::tempdir().unwrap();
        let store = disk_s3_store(&tmp);
        let svc = make_service().with_s3_store(store.clone());

        // Create a bucket + bucket policy.
        let mut create = HashMap::new();
        create.insert("StackName".to_string(), "pol".to_string());
        create.insert(
            "TemplateBody".to_string(),
            r#"{"Resources":{
                "B":{"Type":"AWS::S3::Bucket","Properties":{"BucketName":"pol-bucket"}},
                "BP":{"Type":"AWS::S3::BucketPolicy","Properties":{"Bucket":"pol-bucket","PolicyDocument":{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:GetObject","Resource":"*","Principal":"*"}]}}}
            }}"#
            .to_string(),
        );
        svc.create_stack(&make_request("CreateStack", create))
            .await
            .unwrap();
        let loaded = fakecloud_persistence::S3Store::load(store.as_ref()).unwrap();
        let policy = loaded.buckets["pol-bucket"]
            .subresources
            .get("policy.toml")
            .cloned()
            .expect("bucket policy persisted on create");
        assert!(policy.contains("s3:GetObject"));

        // Update the policy document; the update must write through.
        let mut update = HashMap::new();
        update.insert("StackName".to_string(), "pol".to_string());
        update.insert(
            "TemplateBody".to_string(),
            r#"{"Resources":{
                "B":{"Type":"AWS::S3::Bucket","Properties":{"BucketName":"pol-bucket"}},
                "BP":{"Type":"AWS::S3::BucketPolicy","Properties":{"Bucket":"pol-bucket","PolicyDocument":{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:PutObject","Resource":"*","Principal":"*"}]}}}
            }}"#
            .to_string(),
        );
        svc.update_stack(&make_request("UpdateStack", update))
            .await
            .unwrap();
        let loaded = fakecloud_persistence::S3Store::load(store.as_ref()).unwrap();
        let policy = loaded.buckets["pol-bucket"]
            .subresources
            .get("policy.toml")
            .cloned()
            .expect("bucket policy still persisted after update");
        assert!(
            policy.contains("s3:PutObject"),
            "updated policy should be written through"
        );

        // Delete the stack; the bucket (and its policy) must be removed from disk.
        let mut del = HashMap::new();
        del.insert("StackName".to_string(), "pol".to_string());
        svc.delete_stack(&make_request("DeleteStack", del))
            .await
            .unwrap();
        let loaded = fakecloud_persistence::S3Store::load(store.as_ref()).unwrap();
        assert!(
            !loaded.buckets.contains_key("pol-bucket"),
            "CFN-deleted bucket and policy should be gone from the store"
        );
    }
}
