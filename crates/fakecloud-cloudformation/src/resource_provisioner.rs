use chrono::Utc;
use parking_lot::RwLock;
use std::collections::BTreeMap;
use std::sync::Arc;
use uuid::Uuid;

use crate::state::SharedCloudFormationState;
use fakecloud_acm::{
    CertificateOptions as AcmCertificateOptions, DomainValidation as AcmDomainValidation,
    RenewalSummary as AcmRenewalSummary, SharedAcmState, StoredCertificate as AcmStoredCertificate,
};
use fakecloud_apigateway::{
    make_id as apigw_make_id, ApiKey as ApiGwApiKey, Authorizer as ApiGwAuthorizer,
    Deployment as ApiGwDeployment, Integration as ApiGwIntegration, Method as ApiGwMethod,
    Model as ApiGwModel, Resource as ApiGwResource, RestApi as ApiGwRestApi, SharedApiGatewayState,
    Stage as ApiGwStage, UsagePlan as ApiGwUsagePlan,
};
use fakecloud_apigatewayv2::{
    Authorizer as ApiGwV2Authorizer, CorsConfiguration as ApiGwV2CorsConfiguration,
    Deployment as ApiGwV2Deployment, HttpApi as ApiGwV2HttpApi, Integration as ApiGwV2Integration,
    JwtConfiguration as ApiGwV2JwtConfiguration, Route as ApiGwV2Route, SharedApiGatewayV2State,
    Stage as ApiGwV2Stage,
};
use fakecloud_application_autoscaling::{
    ScalableTarget as AppasScalableTarget, ScalingPolicy as AppasScalingPolicy,
    SharedApplicationAutoScalingState as AppasState, SuspendedState as AppasSuspendedState,
};
use fakecloud_athena::{DataCatalog, NamedQuery, PreparedStatement, SharedAthenaState, WorkGroup};
use fakecloud_cloudfront::{
    functions::{
        CloudFrontOriginAccessIdentityConfig, FunctionConfig, KeyGroupConfig, KeyGroupItems,
        PublicKeyConfig, StoredFunction, StoredKeyGroup, StoredOriginAccessIdentity,
        StoredPublicKey,
    },
    model::{
        DefaultCacheBehavior, DistributionConfig, Origin, OriginItems, Origins, ViewerCertificate,
    },
    policies::{
        CachePolicyConfig, OriginAccessControlConfig, OriginRequestPolicyConfig,
        OriginRequestPolicyCookiesConfig, OriginRequestPolicyHeadersConfig,
        OriginRequestPolicyQueryStringsConfig, ResponseHeadersPolicyConfig, StoredCachePolicy,
        StoredOriginAccessControl, StoredOriginRequestPolicy, StoredResponseHeadersPolicy,
    },
    state::StoredDistribution,
    SharedCloudFrontState,
};
use fakecloud_cloudwatch::{AlarmState, Dashboard, MetricAlarm, SharedCloudWatchState};
use fakecloud_cognito::{
    default_schema_attributes, AccountRecoverySetting, AdminCreateUserConfig,
    CognitoIdentityProvider, CustomDomainConfig, EmailConfiguration, IdentityPool,
    IdentityPoolRoleAttachment, PasswordPolicy, PoolPolicies, RecoveryOption, SchemaAttribute,
    SharedCognitoState, SignInPolicy, SmsConfiguration, UserPool, UserPoolClient, UserPoolDomain,
};
use fakecloud_core::delivery::DeliveryBus;
use fakecloud_dynamodb::{
    AttributeDefinition, DynamoTable, KeySchemaElement, OnDemandThroughput, ProvisionedThroughput,
    SharedDynamoDbState,
};
use fakecloud_ecr::{Repository, SharedEcrState};
use fakecloud_ecs::{
    CapacityProvider as EcsCapacityProvider, Cluster as EcsCluster, Service as EcsService,
    SharedEcsState, TagEntry as EcsTagEntry, TaskDefinition as EcsTaskDefinition,
};
use fakecloud_elasticache::{
    CacheCluster as EcCacheCluster, CacheParameterGroup, CacheSecurityGroup, CacheSubnetGroup,
    ElastiCacheUser as EcUser, ElastiCacheUserGroup as EcUserGroup,
    ReplicationGroup as EcReplicationGroup, SharedElastiCacheState,
};
use fakecloud_elbv2::{
    Action as ElbAction, Listener, LoadBalancer, Rule as ElbRule, RuleCondition, SharedElbv2State,
    Tag as ElbTag, TargetGroup, TargetGroupTuple,
};
use fakecloud_eventbridge::{
    ApiDestination, Archive, Connection, Endpoint, EventBus, EventRule, SharedEventBridgeState,
};
use fakecloud_firehose::state::{DeliveryStream, S3Destination};
use fakecloud_iam::{
    IamAccessKey, IamGroup, IamInstanceProfile, IamPolicy, IamRole, IamUser, OidcProvider,
    PolicyVersion, SamlProvider, SharedIamState, Tag, VirtualMfaDevice,
};
use fakecloud_kinesis::{build_stream_shards, KinesisConsumer, KinesisStream, SharedKinesisState};
use fakecloud_kms::provisioner as kms_provisioner;
use fakecloud_kms::SharedKmsState;
use fakecloud_lambda::{
    AttachedLayer, EventSourceMapping, FunctionAlias, FunctionUrlConfig, Layer, LayerVersion,
    SharedLambdaState,
};
use fakecloud_logs::{
    Delivery, DeliveryDestination, DeliverySource, Destination, LogStream, MetricFilter,
    MetricTransformation, QueryDefinition, ResourcePolicy, SharedLogsState, SubscriptionFilter,
};
use fakecloud_organizations::{
    OrganizationState, OrganizationalUnit, Policy as OrgPolicy, SharedOrganizationsState,
    POLICY_TYPE_SCP,
};
use fakecloud_rds::{DbInstance, DbParameterGroup, DbSubnetGroup, RdsTag, SharedRdsState};
use fakecloud_route53::{
    model::{HealthCheckConfig, HostedZoneFeatures, ResourceRecordSet},
    SharedRoute53State, StoredHealthCheck, StoredHostedZone,
};
use fakecloud_s3::{S3Bucket, SharedS3State};
use fakecloud_secretsmanager::{RotationRules, Secret, SecretVersion, SharedSecretsManagerState};
use fakecloud_ses::{
    ConfigurationSet as SesConfigurationSet, ContactList as SesContactList,
    DedicatedIpPool as SesDedicatedIpPool, EmailIdentity as SesEmailIdentity,
    EmailTemplate as SesEmailTemplate, EventDestination as SesEventDestination,
    IpFilter as SesIpFilter, ReceiptAction as SesReceiptAction, ReceiptFilter as SesReceiptFilter,
    ReceiptRule as SesReceiptRule, ReceiptRuleSet as SesReceiptRuleSet, SharedSesState,
};
use fakecloud_sns::{SharedSnsState, SnsSubscription, SnsTopic};
use fakecloud_sqs::{SharedSqsState, SqsQueue};
use fakecloud_ssm::{SharedSsmState, SsmParameter};
use fakecloud_stepfunctions::{
    Activity as SfnActivity, AliasRoute, SharedStepFunctionsState, StateMachine, StateMachineAlias,
    StateMachineStatus, StateMachineType, StateMachineVersion,
};
use fakecloud_wafv2::{IpSet, RegexPatternSet, RuleGroup, SharedWafv2State, WebAcl};

use crate::state::StackResource;
use crate::template::ResourceDefinition;

/// Convert a CFN `Tags` property (`[{Key, Value}, ...]`) into the IAM
/// crate's `Tag` Vec form. Silently skips malformed entries — the same
/// tolerant behaviour the existing IAM service uses for runtime input.
fn parse_iam_tags(value: Option<&serde_json::Value>) -> Vec<Tag> {
    let Some(arr) = value.and_then(|v| v.as_array()) else {
        return Vec::new();
    };
    arr.iter()
        .filter_map(|t| {
            let key = t.get("Key").and_then(|v| v.as_str())?.to_string();
            let value = t.get("Value").and_then(|v| v.as_str())?.to_string();
            Some(Tag { key, value })
        })
        .collect()
}

/// Mirror of `parse_iam_tags` but for the ELBv2 crate's separate `Tag`
/// type. Same `[{Key, Value}, ...]` JSON shape, ignored on malformed entries.
fn parse_elb_tags(value: Option<&serde_json::Value>) -> Vec<ElbTag> {
    let Some(arr) = value.and_then(|v| v.as_array()) else {
        return Vec::new();
    };
    arr.iter()
        .filter_map(|t| {
            let key = t.get("Key").and_then(|v| v.as_str())?.to_string();
            let value = t.get("Value").and_then(|v| v.as_str())?.to_string();
            Some(ElbTag { key, value })
        })
        .collect()
}

/// Translate CFN-shape Listener/ListenerRule actions into ELBv2 internal
/// `Action`s. Only the action-type knobs CFN exposes are wired; anything
/// not recognised becomes a bare action with no target.
fn parse_elb_actions(value: Option<&serde_json::Value>) -> Vec<ElbAction> {
    let Some(arr) = value.and_then(|v| v.as_array()) else {
        return Vec::new();
    };
    arr.iter()
        .map(|a| {
            let action_type = a
                .get("Type")
                .and_then(|v| v.as_str())
                .unwrap_or("forward")
                .to_string();
            let target_group_arn = a
                .get("TargetGroupArn")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            let order = a.get("Order").and_then(|v| v.as_i64()).map(|n| n as i32);
            let redirect = a
                .get("RedirectConfig")
                .map(|r| fakecloud_elbv2::RedirectConfig {
                    protocol: r
                        .get("Protocol")
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string()),
                    port: r
                        .get("Port")
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string()),
                    host: r
                        .get("Host")
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string()),
                    path: r
                        .get("Path")
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string()),
                    query: r
                        .get("Query")
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string()),
                    status_code: r
                        .get("StatusCode")
                        .and_then(|v| v.as_str())
                        .unwrap_or("HTTP_302")
                        .to_string(),
                });
            let fixed_response =
                a.get("FixedResponseConfig")
                    .map(|f| fakecloud_elbv2::FixedResponseConfig {
                        message_body: f
                            .get("MessageBody")
                            .and_then(|v| v.as_str())
                            .map(|s| s.to_string()),
                        status_code: f
                            .get("StatusCode")
                            .and_then(|v| v.as_str())
                            .unwrap_or("200")
                            .to_string(),
                        content_type: f
                            .get("ContentType")
                            .and_then(|v| v.as_str())
                            .map(|s| s.to_string()),
                    });
            let forward = a.get("ForwardConfig").map(|f| {
                let target_groups: Vec<TargetGroupTuple> = f
                    .get("TargetGroups")
                    .and_then(|v| v.as_array())
                    .map(|arr| {
                        arr.iter()
                            .filter_map(|t| {
                                let target_group_arn = t
                                    .get("TargetGroupArn")
                                    .and_then(|v| v.as_str())?
                                    .to_string();
                                let weight =
                                    t.get("Weight").and_then(|v| v.as_i64()).map(|n| n as i32);
                                Some(TargetGroupTuple {
                                    target_group_arn,
                                    weight,
                                })
                            })
                            .collect()
                    })
                    .unwrap_or_default();
                fakecloud_elbv2::ForwardConfig {
                    target_groups,
                    stickiness: None,
                }
            });
            ElbAction {
                action_type,
                target_group_arn,
                order,
                redirect,
                fixed_response,
                forward,
                authenticate_cognito: None,
                authenticate_oidc: None,
            }
        })
        .collect()
}

fn parse_elb_rule_conditions(value: Option<&serde_json::Value>) -> Vec<RuleCondition> {
    let Some(arr) = value.and_then(|v| v.as_array()) else {
        return Vec::new();
    };
    arr.iter()
        .map(|c| {
            let field = c
                .get("Field")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let values: Vec<String> = c
                .get("Values")
                .and_then(|v| v.as_array())
                .map(|arr| {
                    arr.iter()
                        .filter_map(|s| s.as_str().map(|s| s.to_string()))
                        .collect()
                })
                .unwrap_or_default();
            let host_header_values: Vec<String> = c
                .get("HostHeaderConfig")
                .and_then(|v| v.get("Values"))
                .and_then(|v| v.as_array())
                .map(|arr| {
                    arr.iter()
                        .filter_map(|s| s.as_str().map(|s| s.to_string()))
                        .collect()
                })
                .unwrap_or_default();
            RuleCondition {
                field,
                values,
                host_header_values,
                path_pattern_values: Vec::new(),
                http_header_name: None,
                http_header_values: Vec::new(),
                query_string_values: Vec::new(),
                http_request_method_values: Vec::new(),
                source_ip_values: Vec::new(),
            }
        })
        .collect()
}

/// Parse the `KeyPolicy` field of an `AWS::KMS::Key` /
/// `AWS::KMS::ReplicaKey` resource. CFN allows either a JSON string or
/// an inline JSON object; we serialize the object form back to a string
/// to match the [`KmsKey.policy`](fakecloud_kms::KmsKey) shape.
fn parse_key_policy(props: &serde_json::Value) -> Option<String> {
    match props.get("KeyPolicy") {
        Some(v) if v.is_string() => Some(v.as_str().unwrap_or("").to_string()),
        Some(v) => Some(serde_json::to_string(v).unwrap_or_default()),
        None => None,
    }
}

/// Parse the `Tags` field common to KMS CFN resources: an array of
/// `{Key, Value}` pairs into a sorted map.
fn parse_tag_list(props: &serde_json::Value) -> BTreeMap<String, String> {
    let mut tags: BTreeMap<String, String> = BTreeMap::new();
    if let Some(arr) = props.get("Tags").and_then(|v| v.as_array()) {
        for t in arr {
            if let (Some(k), Some(v)) = (
                t.get("Key").and_then(|x| x.as_str()),
                t.get("Value").and_then(|x| x.as_str()),
            ) {
                tags.insert(k.to_string(), v.to_string());
            }
        }
    }
    tags
}

/// Parse the `Properties` of an `AWS::KMS::Key` resource into the
/// shared [`fakecloud_kms::provisioner::KeyCreationInput`]. Defaults
/// match AWS: `ENCRYPT_DECRYPT` / `SYMMETRIC_DEFAULT` / `AWS_KMS` /
/// `Enabled=true`. `RotationPeriodInDays`, `PendingWindowInDays`, and
/// `BypassPolicyLockoutSafetyCheck` are accepted by the parser for
/// CFN compatibility but not persisted on the key — the underlying
/// KMS state doesn't model per-key rotation periods, and the deletion
/// window only matters at scheduled-deletion time which CFN delete
/// short-circuits.
fn parse_kms_key_input(props: &serde_json::Value) -> kms_provisioner::KeyCreationInput {
    kms_provisioner::KeyCreationInput {
        description: props
            .get("Description")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string(),
        key_usage: props
            .get("KeyUsage")
            .and_then(|v| v.as_str())
            .unwrap_or("ENCRYPT_DECRYPT")
            .to_string(),
        key_spec: props
            .get("KeySpec")
            .and_then(|v| v.as_str())
            .unwrap_or("SYMMETRIC_DEFAULT")
            .to_string(),
        origin: props
            .get("Origin")
            .and_then(|v| v.as_str())
            .unwrap_or("AWS_KMS")
            .to_string(),
        enabled: props
            .get("Enabled")
            .and_then(|v| v.as_bool())
            .unwrap_or(true),
        multi_region: props
            .get("MultiRegion")
            .and_then(|v| v.as_bool())
            .unwrap_or(false),
        key_rotation_enabled: props
            .get("EnableKeyRotation")
            .and_then(|v| v.as_bool())
            .unwrap_or(false),
        policy: parse_key_policy(props),
        tags: parse_tag_list(props),
    }
}

/// `LogGroupName` properties on Logs CFN resources may carry either a
/// log-group ARN (when they come from `{Ref: SomeLogGroup}` in the same
/// template) or a plain name. Extract the name in either case.
fn parse_log_group_name(input: &str) -> String {
    if let Some(rest) = input.strip_prefix("arn:aws:logs:") {
        if let Some(after) = rest.split(":log-group:").nth(1) {
            // ARN ends with `:*`; trim it if present.
            return after.trim_end_matches(":*").to_string();
        }
    }
    input.to_string()
}

/// Pull the function name out of either a bare name or a Lambda
/// function ARN. CFN passes `{Ref: SomeFunction}` which resolves to the
/// function name today, but `{Fn::GetAtt: [F, Arn]}` resolves to the
/// full ARN; both shapes need to land at the same map key.
fn parse_lambda_function_name(input: &str) -> String {
    if let Some(rest) = input.strip_prefix("arn:aws:lambda:") {
        if let Some(after) = rest.split(":function:").nth(1) {
            // Trim trailing `:qualifier` (alias / version).
            return after.split(':').next().unwrap_or(after).to_string();
        }
    }
    input.to_string()
}

/// All AWS::Lambda::Function CFN properties parsed and pre-defaulted into
/// their lambda-state shapes. Mirrors the lambda service's
/// `CreateFunctionInput` so create + update share one parse path.
struct LambdaFunctionProps {
    runtime: String,
    role: String,
    handler: String,
    description: String,
    timeout: i64,
    memory_size: i64,
    package_type: String,
    tags: BTreeMap<String, String>,
    environment: BTreeMap<String, String>,
    architectures: Vec<String>,
    /// Decoded `Code.ZipFile` bytes (base64 → raw). `None` when the
    /// caller specified `Code.S3Bucket`/`Code.S3Key` or `Code.ImageUri`
    /// instead — the create/update path resolves S3 separately.
    code_zip: Option<Vec<u8>>,
    s3_bucket: Option<String>,
    s3_key: Option<String>,
    image_uri: Option<String>,
    layers: Vec<String>,
    tracing_mode: Option<String>,
    kms_key_arn: Option<String>,
    ephemeral_storage_size: Option<i64>,
    vpc_config: Option<serde_json::Value>,
    snap_start: Option<serde_json::Value>,
    dead_letter_config_arn: Option<String>,
    file_system_configs: Vec<serde_json::Value>,
    logging_config: Option<serde_json::Value>,
}

/// Parse the `Properties` value of an `AWS::Lambda::Function` resource
/// into a `LambdaFunctionProps`. Defaults match the AWS Lambda
/// CreateFunction API: `python3.12` runtime, `index.handler` handler,
/// `Zip` package type, `x86_64` architecture, 3s timeout, 128MB memory.
fn parse_lambda_function_props(props: &serde_json::Value) -> Result<LambdaFunctionProps, String> {
    let runtime = props
        .get("Runtime")
        .and_then(|v| v.as_str())
        .unwrap_or("python3.12")
        .to_string();
    let role = props
        .get("Role")
        .and_then(|v| v.as_str())
        .unwrap_or_default()
        .to_string();
    let handler = props
        .get("Handler")
        .and_then(|v| v.as_str())
        .unwrap_or("index.handler")
        .to_string();
    let description = props
        .get("Description")
        .and_then(|v| v.as_str())
        .unwrap_or_default()
        .to_string();
    let timeout = props.get("Timeout").and_then(|v| v.as_i64()).unwrap_or(3);
    let memory_size = props
        .get("MemorySize")
        .and_then(|v| v.as_i64())
        .unwrap_or(128);
    let architectures = props
        .get("Architectures")
        .and_then(|v| v.as_array())
        .map(|a| {
            a.iter()
                .filter_map(|v| v.as_str().map(|s| s.to_string()))
                .collect::<Vec<_>>()
        })
        .unwrap_or_else(|| vec!["x86_64".to_string()]);
    let package_type = props
        .get("PackageType")
        .and_then(|v| v.as_str())
        .unwrap_or("Zip")
        .to_string();
    let environment = props
        .get("Environment")
        .and_then(|v| v.get("Variables"))
        .and_then(|v| v.as_object())
        .map(|o| {
            o.iter()
                .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                .collect::<BTreeMap<String, String>>()
        })
        .unwrap_or_default();

    // CFN tags ride as `[{Key, Value}, ...]`; flatten to the map shape
    // the lambda crate stores tags in.
    let tags: BTreeMap<String, String> = props
        .get("Tags")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|t| {
                    let k = t.get("Key").and_then(|v| v.as_str())?.to_string();
                    let v = t.get("Value").and_then(|v| v.as_str())?.to_string();
                    Some((k, v))
                })
                .collect()
        })
        .unwrap_or_default();

    let code = props.get("Code");
    // CFN's `Code.ZipFile` is the raw source code inline (per the
    // CloudFormation user guide), not base64 like the Lambda
    // CreateFunction API. We store it as the raw bytes — fakecloud's
    // Lambda runtime is content-agnostic and just needs *some* deployable
    // payload to execute.
    let code_zip = code
        .and_then(|c| c.get("ZipFile"))
        .and_then(|v| v.as_str())
        .map(|s| s.as_bytes().to_vec());
    let s3_bucket = code
        .and_then(|c| c.get("S3Bucket"))
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());
    let s3_key = code
        .and_then(|c| c.get("S3Key"))
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());
    // ImageUri is only meaningful for `PackageType=Image`. Mirroring the
    // lambda service path, drop it on Zip functions so GetFunction
    // doesn't return ECR metadata for a ZIP-based function.
    let image_uri = if package_type == "Image" {
        code.and_then(|c| c.get("ImageUri"))
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
    } else {
        None
    };
    if package_type == "Image" && image_uri.is_none() {
        return Err("Code.ImageUri is required when PackageType is Image".to_string());
    }

    let layers: Vec<String> = props
        .get("Layers")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(String::from))
                .collect()
        })
        .unwrap_or_default();

    let tracing_mode = props
        .get("TracingConfig")
        .and_then(|v| v.get("Mode"))
        .and_then(|v| v.as_str())
        .map(String::from);
    let kms_key_arn = props
        .get("KmsKeyArn")
        .and_then(|v| v.as_str())
        .map(String::from);
    let ephemeral_storage_size = props
        .get("EphemeralStorage")
        .and_then(|v| v.get("Size"))
        .and_then(|v| v.as_i64());
    let vpc_config = props.get("VpcConfig").filter(|v| v.is_object()).cloned();
    let snap_start = props.get("SnapStart").filter(|v| v.is_object()).cloned();
    let dead_letter_config_arn = props
        .get("DeadLetterConfig")
        .and_then(|v| v.get("TargetArn"))
        .and_then(|v| v.as_str())
        .map(String::from);
    let file_system_configs = props
        .get("FileSystemConfigs")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();
    let logging_config = props
        .get("LoggingConfig")
        .filter(|v| v.is_object())
        .cloned();

    Ok(LambdaFunctionProps {
        runtime,
        role,
        handler,
        description,
        timeout,
        memory_size,
        package_type,
        tags,
        environment,
        architectures,
        code_zip,
        s3_bucket,
        s3_key,
        image_uri,
        layers,
        tracing_mode,
        kms_key_arn,
        ephemeral_storage_size,
        vpc_config,
        snap_start,
        dead_letter_config_arn,
        file_system_configs,
        logging_config,
    })
}

/// Properties for `AWS::Lambda::EventSourceMapping` shared between the
/// create and update provisioners. Mirrors the lambda crate
/// `EventSourceMapping` shape one-for-one — every CFN-mutable field
/// lands here so update can rewrite without re-parsing.
struct LambdaEventSourceMappingProps {
    event_source_arn: String,
    batch_size: i64,
    enabled: bool,
    starting_position: Option<String>,
    starting_position_timestamp: Option<f64>,
    parallelization_factor: Option<i64>,
    maximum_batching_window_in_seconds: Option<i64>,
    function_response_types: Vec<String>,
    filter_patterns: Vec<String>,
    kms_key_arn: Option<String>,
    metrics_config: Option<serde_json::Value>,
    destination_config: Option<serde_json::Value>,
    maximum_retry_attempts: Option<i64>,
    maximum_record_age_in_seconds: Option<i64>,
    bisect_batch_on_function_error: Option<bool>,
    tumbling_window_in_seconds: Option<i64>,
    topics: Vec<String>,
    queues: Vec<String>,
}

/// Parse the `Properties` value of an `AWS::Lambda::EventSourceMapping`
/// resource. `EventSourceArn` is required on create; updates re-parse but
/// the value is ignored since the field is immutable.
fn parse_lambda_event_source_mapping_props(
    props: &serde_json::Value,
) -> Result<LambdaEventSourceMappingProps, String> {
    let event_source_arn = props
        .get("EventSourceArn")
        .and_then(|v| v.as_str())
        .unwrap_or_default()
        .to_string();
    let batch_size = props
        .get("BatchSize")
        .and_then(|v| v.as_i64())
        .unwrap_or(10);
    let enabled = props
        .get("Enabled")
        .and_then(|v| v.as_bool())
        .unwrap_or(true);
    let starting_position = props
        .get("StartingPosition")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());
    let starting_position_timestamp = props
        .get("StartingPositionTimestamp")
        .and_then(|v| v.as_f64());
    let parallelization_factor = props.get("ParallelizationFactor").and_then(|v| v.as_i64());
    let maximum_batching_window_in_seconds = props
        .get("MaximumBatchingWindowInSeconds")
        .and_then(|v| v.as_i64());
    let function_response_types: Vec<String> = props
        .get("FunctionResponseTypes")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(|s| s.to_string()))
                .collect()
        })
        .unwrap_or_default();
    let filter_patterns: Vec<String> = props
        .get("FilterCriteria")
        .and_then(|v| v.get("Filters"))
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|f| {
                    f.get("Pattern")
                        .and_then(|p| p.as_str())
                        .map(|s| s.to_string())
                })
                .collect()
        })
        .unwrap_or_default();
    let kms_key_arn = props
        .get("KmsKeyArn")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());
    let metrics_config = props
        .get("MetricsConfig")
        .filter(|v| v.is_object())
        .cloned();
    let destination_config = props
        .get("DestinationConfig")
        .filter(|v| v.is_object())
        .cloned();
    let maximum_retry_attempts = props.get("MaximumRetryAttempts").and_then(|v| v.as_i64());
    let maximum_record_age_in_seconds = props
        .get("MaximumRecordAgeInSeconds")
        .and_then(|v| v.as_i64());
    let bisect_batch_on_function_error = props
        .get("BisectBatchOnFunctionError")
        .and_then(|v| v.as_bool());
    let tumbling_window_in_seconds = props
        .get("TumblingWindowInSeconds")
        .and_then(|v| v.as_i64());
    let topics: Vec<String> = props
        .get("Topics")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(|s| s.to_string()))
                .collect()
        })
        .unwrap_or_default();
    let queues: Vec<String> = props
        .get("Queues")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(|s| s.to_string()))
                .collect()
        })
        .unwrap_or_default();

    Ok(LambdaEventSourceMappingProps {
        event_source_arn,
        batch_size,
        enabled,
        starting_position,
        starting_position_timestamp,
        parallelization_factor,
        maximum_batching_window_in_seconds,
        function_response_types,
        filter_patterns,
        kms_key_arn,
        metrics_config,
        destination_config,
        maximum_retry_attempts,
        maximum_record_age_in_seconds,
        bisect_batch_on_function_error,
        tumbling_window_in_seconds,
        topics,
        queues,
    })
}

/// Compute base64-encoded SHA-256 of code bytes — matches what the
/// lambda service stores in `LambdaFunction.code_sha256`.
fn sha256_b64(bytes: &[u8]) -> String {
    use sha2::Digest;
    let hash = sha2::Sha256::digest(bytes);
    base64::Engine::encode(&base64::engine::general_purpose::STANDARD, hash)
}

/// Look up the `code_size` for an attached layer-version ARN by walking
/// the in-process lambda state. Falls back to 0 when the ARN is
/// unparseable or the layer/version is unknown — same fallback as the
/// lambda service helper.
fn layer_code_size(
    accounts: &fakecloud_core::multi_account::MultiAccountState<fakecloud_lambda::LambdaState>,
    arn: &str,
) -> i64 {
    // arn:aws:lambda:<region>:<account>:layer:<name>:<version>
    let Some(rest) = arn.strip_prefix("arn:aws:lambda:") else {
        return 0;
    };
    let mut parts = rest.split(':');
    let _region = parts.next();
    let Some(account) = parts.next() else {
        return 0;
    };
    if parts.next() != Some("layer") {
        return 0;
    }
    let Some(name) = parts.next() else {
        return 0;
    };
    let Some(ver_str) = parts.next() else {
        return 0;
    };
    let Ok(ver) = ver_str.parse::<i64>() else {
        return 0;
    };
    accounts
        .get(account)
        .and_then(|s| s.layers.get(name))
        .and_then(|l| l.versions.iter().find(|v| v.version == ver))
        .map(|v| v.code_size)
        .unwrap_or(0)
}

/// What a resource provisioner returns. The physical id is what `Ref` resolves
/// to; `attributes` is what `Fn::GetAtt` resolves to (per-resource-type).
pub struct ProvisionResult {
    pub physical_id: String,
    pub attributes: BTreeMap<String, String>,
}

impl ProvisionResult {
    pub fn new(physical_id: impl Into<String>) -> Self {
        Self {
            physical_id: physical_id.into(),
            attributes: BTreeMap::new(),
        }
    }

    pub fn with(mut self, key: &str, value: impl Into<String>) -> Self {
        self.attributes.insert(key.to_string(), value.into());
        self
    }

    /// Merge another attribute map into this result. Used by update
    /// handlers that delegate to a create handler but need to keep the
    /// existing physical id while inheriting the freshly computed
    /// attributes.
    pub fn merge_attributes(mut self, other: BTreeMap<String, String>) -> Self {
        for (k, v) in other {
            self.attributes.insert(k, v);
        }
        self
    }
}

/// Holds references to all service states so CloudFormation can provision resources.
pub struct ResourceProvisioner {
    pub sqs_state: SharedSqsState,
    pub sns_state: SharedSnsState,
    pub ssm_state: SharedSsmState,
    pub iam_state: SharedIamState,
    pub s3_state: SharedS3State,
    pub eventbridge_state: SharedEventBridgeState,
    pub dynamodb_state: SharedDynamoDbState,
    pub logs_state: SharedLogsState,
    pub lambda_state: SharedLambdaState,
    pub secretsmanager_state: SharedSecretsManagerState,
    pub kinesis_state: SharedKinesisState,
    pub kms_state: SharedKmsState,
    pub ecr_state: SharedEcrState,
    pub cloudwatch_state: SharedCloudWatchState,
    pub elbv2_state: SharedElbv2State,
    pub organizations_state: SharedOrganizationsState,
    pub cognito_state: SharedCognitoState,
    pub rds_state: SharedRdsState,
    pub ecs_state: SharedEcsState,
    pub acm_state: SharedAcmState,
    pub elasticache_state: SharedElastiCacheState,
    pub route53_state: SharedRoute53State,
    pub cloudfront_state: SharedCloudFrontState,
    pub stepfunctions_state: SharedStepFunctionsState,
    pub wafv2_state: SharedWafv2State,
    pub apigateway_state: SharedApiGatewayState,
    pub apigatewayv2_state: SharedApiGatewayV2State,
    pub ses_state: SharedSesState,
    pub app_autoscaling_state: AppasState,
    pub athena_state: SharedAthenaState,
    pub firehose_state: fakecloud_firehose::SharedFirehoseState,
    pub glue_state: fakecloud_glue::SharedGlueState,
    pub cloudformation_state: SharedCloudFormationState,
    pub delivery: Arc<DeliveryBus>,
    pub account_id: String,
    pub region: String,
    pub stack_id: String,
}

impl ResourceProvisioner {
    /// Create a resource and return the StackResource with physical ID.
    pub fn create_resource(&self, resource: &ResourceDefinition) -> Result<StackResource, String> {
        let result = match resource.resource_type.as_str() {
            "AWS::SQS::Queue" => self.create_sqs_queue(resource),
            "AWS::SNS::Topic" => self.create_sns_topic(resource),
            "AWS::SNS::Subscription" => self.create_sns_subscription(resource),
            "AWS::SSM::Parameter" => self.create_ssm_parameter(resource),
            "AWS::IAM::Role" => self.create_iam_role(resource),
            "AWS::IAM::Policy" => self.create_iam_policy(resource),
            "AWS::IAM::User" => self.create_iam_user(resource),
            "AWS::IAM::Group" => self.create_iam_group(resource),
            "AWS::IAM::ManagedPolicy" => self.create_iam_managed_policy(resource),
            "AWS::IAM::UserToGroupAddition" => self.create_iam_user_to_group_addition(resource),
            "AWS::IAM::AccessKey" => self.create_iam_access_key(resource),
            "AWS::IAM::InstanceProfile" => self.create_iam_instance_profile(resource),
            "AWS::IAM::OIDCProvider" => self.create_iam_oidc_provider(resource),
            "AWS::IAM::SAMLProvider" => self.create_iam_saml_provider(resource),
            "AWS::IAM::ServiceLinkedRole" => self.create_iam_service_linked_role(resource),
            "AWS::IAM::VirtualMFADevice" => self.create_iam_virtual_mfa_device(resource),
            "AWS::S3::Bucket" => self.create_s3_bucket(resource),
            "AWS::Events::Rule" => self.create_eventbridge_rule(resource),
            "AWS::Events::Connection" => self.create_eventbridge_connection(resource),
            "AWS::Events::ApiDestination" => self.create_eventbridge_api_destination(resource),
            "AWS::Events::Archive" => self.create_eventbridge_archive(resource),
            "AWS::Events::EventBus" => self.create_eventbridge_event_bus(resource),
            "AWS::Events::EventBusPolicy" => self.create_eventbridge_event_bus_policy(resource),
            "AWS::Events::Endpoint" => self.create_eventbridge_endpoint(resource),
            "AWS::DynamoDB::Table" => self.create_dynamodb_table(resource),
            "AWS::Logs::LogGroup" => self.create_log_group(resource),
            "AWS::Logs::LogStream" => self.create_log_stream(resource),
            "AWS::Logs::MetricFilter" => self.create_metric_filter(resource),
            "AWS::Logs::SubscriptionFilter" => self.create_subscription_filter(resource),
            "AWS::Logs::Destination" => self.create_logs_destination(resource),
            "AWS::Logs::ResourcePolicy" => self.create_logs_resource_policy(resource),
            "AWS::Logs::QueryDefinition" => self.create_logs_query_definition(resource),
            "AWS::Logs::Delivery" => self.create_logs_delivery(resource),
            "AWS::Logs::DeliveryDestination" => self.create_logs_delivery_destination(resource),
            "AWS::Logs::DeliverySource" => self.create_logs_delivery_source(resource),
            "AWS::Lambda::Function" => self.create_lambda_function(resource),
            "AWS::Lambda::Permission" => self.create_lambda_permission(resource),
            "AWS::Lambda::EventSourceMapping" => self.create_lambda_event_source_mapping(resource),
            "AWS::Lambda::LayerVersion" => self.create_lambda_layer_version(resource),
            "AWS::Lambda::Url" => self.create_lambda_url(resource),
            "AWS::Lambda::Alias" => self.create_lambda_alias(resource),
            "AWS::Lambda::Version" => self.create_lambda_version(resource),
            "AWS::SecretsManager::Secret" => self.create_secrets_manager_secret(resource),
            "AWS::Kinesis::Stream" => self.create_kinesis_stream(resource),
            "AWS::Kinesis::StreamConsumer" => self.create_kinesis_stream_consumer(resource),
            "AWS::KMS::Key" => self.create_kms_key(resource),
            "AWS::KMS::Alias" => self.create_kms_alias(resource),
            "AWS::KMS::ReplicaKey" => self.create_kms_replica_key(resource),
            "AWS::ECR::Repository" => self.create_ecr_repository(resource),
            "AWS::ECR::RepositoryPolicy" => self.create_ecr_repository_policy(resource),
            "AWS::ECR::LifecyclePolicy" => self.create_ecr_lifecycle_policy(resource),
            "AWS::ECR::RegistryPolicy" => self.create_ecr_registry_policy(resource),
            "AWS::ECR::ReplicationConfiguration" => {
                self.create_ecr_replication_configuration(resource)
            }
            "AWS::ECR::RegistryScanningConfiguration" => {
                self.create_ecr_registry_scanning_configuration(resource)
            }
            "AWS::ECR::PullThroughCacheRule" => self.create_ecr_pull_through_cache_rule(resource),
            "AWS::CloudWatch::Alarm" => self.create_cloudwatch_alarm(resource),
            "AWS::CloudWatch::Dashboard" => self.create_cloudwatch_dashboard(resource),
            "AWS::ElasticLoadBalancingV2::LoadBalancer" => {
                self.create_elbv2_load_balancer(resource)
            }
            "AWS::ElasticLoadBalancingV2::TargetGroup" => self.create_elbv2_target_group(resource),
            "AWS::ElasticLoadBalancingV2::Listener" => self.create_elbv2_listener(resource),
            "AWS::ElasticLoadBalancingV2::ListenerRule" => {
                self.create_elbv2_listener_rule(resource)
            }
            "AWS::ElasticLoadBalancingV2::ListenerCertificate" => {
                self.create_elbv2_listener_certificate(resource)
            }
            "AWS::ElasticLoadBalancingV2::TrustStore" => self.create_elbv2_trust_store(resource),
            "AWS::Organizations::Organization" => self.create_organization(resource),
            "AWS::Organizations::OrganizationalUnit" => self.create_organization_unit(resource),
            "AWS::Organizations::Account" => self.create_organization_account(resource),
            "AWS::Organizations::Policy" => self.create_organization_policy(resource),
            "AWS::Organizations::ResourcePolicy" => {
                self.create_organization_resource_policy(resource)
            }
            "AWS::Cognito::UserPool" => self.create_cognito_user_pool(resource),
            "AWS::Cognito::UserPoolClient" => self.create_cognito_user_pool_client(resource),
            "AWS::Cognito::UserPoolDomain" => self.create_cognito_user_pool_domain(resource),
            "AWS::Cognito::IdentityPool" => self.create_cognito_identity_pool(resource),
            "AWS::Cognito::IdentityPoolRoleAttachment" => {
                self.create_cognito_identity_pool_role_attachment(resource)
            }
            "AWS::RDS::DBSubnetGroup" => self.create_rds_subnet_group(resource),
            "AWS::RDS::DBParameterGroup" => self.create_rds_parameter_group(resource),
            "AWS::RDS::DBClusterParameterGroup" => {
                self.create_rds_cluster_parameter_group(resource)
            }
            "AWS::RDS::OptionGroup" => self.create_rds_option_group(resource),
            "AWS::RDS::EventSubscription" => self.create_rds_event_subscription(resource),
            "AWS::RDS::DBSecurityGroup" => self.create_rds_security_group(resource),
            "AWS::RDS::DBProxy" => self.create_rds_db_proxy(resource),
            "AWS::RDS::DBInstance" => self.create_rds_db_instance(resource),
            "AWS::RDS::DBCluster" => self.create_rds_db_cluster(resource),
            "AWS::ECS::Cluster" => self.create_ecs_cluster(resource),
            "AWS::ECS::TaskDefinition" => self.create_ecs_task_definition(resource),
            "AWS::ECS::Service" => self.create_ecs_service(resource),
            "AWS::ECS::CapacityProvider" => self.create_ecs_capacity_provider(resource),
            "AWS::CertificateManager::Certificate" => self.create_acm_certificate(resource),
            "AWS::CertificateManager::Account" => self.create_acm_account(resource),
            "AWS::ElastiCache::ParameterGroup" => self.create_ec_parameter_group(resource),
            "AWS::ElastiCache::SubnetGroup" => self.create_ec_subnet_group(resource),
            "AWS::ElastiCache::SecurityGroup" => self.create_ec_security_group(resource),
            "AWS::ElastiCache::User" => self.create_ec_user(resource),
            "AWS::ElastiCache::UserGroup" => self.create_ec_user_group(resource),
            "AWS::ElastiCache::CacheCluster" => self.create_ec_cache_cluster(resource),
            "AWS::ElastiCache::ReplicationGroup" => self.create_ec_replication_group(resource),
            "AWS::Route53::HostedZone" => self.create_route53_hosted_zone(resource),
            "AWS::Route53::RecordSet" => self.create_route53_record_set(resource),
            "AWS::Route53::HealthCheck" => self.create_route53_health_check(resource),
            "AWS::Route53::DNSSEC" => self.create_route53_dnssec(resource),
            "AWS::Route53::KeySigningKey" => self.create_route53_key_signing_key(resource),
            "AWS::CloudFront::CloudFrontOriginAccessIdentity" => {
                self.create_cf_origin_access_identity(resource)
            }
            "AWS::CloudFront::Distribution" => self.create_cf_distribution(resource),
            "AWS::CloudFront::OriginAccessControl" => {
                self.create_cf_origin_access_control(resource)
            }
            "AWS::CloudFront::PublicKey" => self.create_cf_public_key(resource),
            "AWS::CloudFront::KeyGroup" => self.create_cf_key_group(resource),
            "AWS::CloudFront::Function" => self.create_cf_function(resource),
            "AWS::CloudFront::CachePolicy" => self.create_cf_cache_policy(resource),
            "AWS::CloudFront::OriginRequestPolicy" => {
                self.create_cf_origin_request_policy(resource)
            }
            "AWS::CloudFront::ResponseHeadersPolicy" => {
                self.create_cf_response_headers_policy(resource)
            }
            "AWS::StepFunctions::StateMachine" => self.create_sfn_state_machine(resource),
            "AWS::StepFunctions::Activity" => self.create_sfn_activity(resource),
            "AWS::StepFunctions::StateMachineVersion" => self.create_sfn_version(resource),
            "AWS::StepFunctions::StateMachineAlias" => self.create_sfn_alias(resource),
            "AWS::WAFv2::WebACL" => self.create_wafv2_web_acl(resource),
            "AWS::WAFv2::IPSet" => self.create_wafv2_ip_set(resource),
            "AWS::WAFv2::RegexPatternSet" => self.create_wafv2_regex_pattern_set(resource),
            "AWS::WAFv2::RuleGroup" => self.create_wafv2_rule_group(resource),
            "AWS::WAFv2::LoggingConfiguration" => self.create_wafv2_logging_configuration(resource),
            "AWS::WAFv2::WebACLAssociation" => self.create_wafv2_web_acl_association(resource),
            "AWS::ApiGateway::RestApi" => self.create_apigw_rest_api(resource),
            "AWS::ApiGateway::Resource" => self.create_apigw_resource(resource),
            "AWS::ApiGateway::Method" => self.create_apigw_method(resource),
            "AWS::ApiGateway::Deployment" => self.create_apigw_deployment(resource),
            "AWS::ApiGateway::Stage" => self.create_apigw_stage(resource),
            "AWS::ApiGateway::Authorizer" => self.create_apigw_authorizer(resource),
            "AWS::ApiGateway::RequestValidator" => self.create_apigw_request_validator(resource),
            "AWS::ApiGateway::Model" => self.create_apigw_model(resource),
            "AWS::ApiGateway::GatewayResponse" => self.create_apigw_gateway_response(resource),
            "AWS::ApiGateway::UsagePlan" => self.create_apigw_usage_plan(resource),
            "AWS::ApiGateway::ApiKey" => self.create_apigw_api_key(resource),
            "AWS::ApiGateway::UsagePlanKey" => self.create_apigw_usage_plan_key(resource),
            "AWS::ApiGateway::DomainName" => self.create_apigw_domain_name(resource),
            "AWS::ApiGateway::BasePathMapping" => self.create_apigw_base_path_mapping(resource),
            "AWS::ApiGatewayV2::Api" => self.create_apigwv2_api(resource),
            "AWS::ApiGatewayV2::Route" => self.create_apigwv2_route(resource),
            "AWS::ApiGatewayV2::Integration" => self.create_apigwv2_integration(resource),
            "AWS::ApiGatewayV2::IntegrationResponse" => {
                self.create_apigwv2_integration_response(resource)
            }
            "AWS::ApiGatewayV2::RouteResponse" => self.create_apigwv2_route_response(resource),
            "AWS::ApiGatewayV2::Stage" => self.create_apigwv2_stage(resource),
            "AWS::ApiGatewayV2::Deployment" => self.create_apigwv2_deployment(resource),
            "AWS::ApiGatewayV2::Authorizer" => self.create_apigwv2_authorizer(resource),
            "AWS::ApiGatewayV2::DomainName" => self.create_apigwv2_domain_name(resource),
            "AWS::ApiGatewayV2::ApiMapping" => self.create_apigwv2_api_mapping(resource),
            "AWS::ApiGatewayV2::VpcLink" => self.create_apigwv2_vpc_link(resource),
            "AWS::ApiGatewayV2::Model" => self.create_apigwv2_model(resource),
            "AWS::SES::ConfigurationSet" => self.create_ses_configuration_set(resource),
            "AWS::SES::ConfigurationSetEventDestination" => {
                self.create_ses_event_destination(resource)
            }
            "AWS::SES::EmailIdentity" => self.create_ses_email_identity(resource),
            "AWS::SES::Template" => self.create_ses_template(resource),
            "AWS::SES::ContactList" => self.create_ses_contact_list(resource),
            "AWS::SES::DedicatedIpPool" => self.create_ses_dedicated_ip_pool(resource),
            "AWS::SES::ReceiptRule" => self.create_ses_receipt_rule(resource),
            "AWS::SES::ReceiptRuleSet" => self.create_ses_receipt_rule_set(resource),
            "AWS::SES::ReceiptFilter" => self.create_ses_receipt_filter(resource),
            "AWS::SES::VdmAttributes" => self.create_ses_vdm_attributes(resource),
            "AWS::SecretsManager::RotationSchedule" => {
                self.create_secrets_manager_rotation_schedule(resource)
            }
            "AWS::SecretsManager::ResourcePolicy" => {
                self.create_secrets_manager_resource_policy(resource)
            }
            "AWS::SecretsManager::SecretTargetAttachment" => {
                self.create_secrets_manager_target_attachment(resource)
            }
            "AWS::ApplicationAutoScaling::ScalableTarget" => {
                self.create_application_autoscaling_scalable_target(resource)
            }
            "AWS::ApplicationAutoScaling::ScalingPolicy" => {
                self.create_application_autoscaling_scaling_policy(resource)
            }
            "AWS::Athena::DataCatalog" => self.create_athena_data_catalog(resource),
            "AWS::Athena::NamedQuery" => self.create_athena_named_query(resource),
            "AWS::Athena::WorkGroup" => self.create_athena_work_group(resource),
            "AWS::Athena::PreparedStatement" => self.create_athena_prepared_statement(resource),
            "AWS::KinesisFirehose::DeliveryStream" => {
                self.create_firehose_delivery_stream(resource)
            }
            "AWS::Glue::Database" => self.create_glue_database(resource),
            "AWS::CloudFormation::Stack" => self.create_cloudformation_stack(resource),
            "AWS::Glue::Table" => self.create_glue_table(resource),
            "AWS::Glue::Partition" => self.create_glue_partition(resource),
            t if t.starts_with("Custom::") || t == "AWS::CloudFormation::CustomResource" => self
                .create_custom_resource(resource)
                .map(ProvisionResult::new),
            other => Err(format!("Unsupported resource type: {other}")),
        };

        let is_custom = resource.resource_type.starts_with("Custom::")
            || resource.resource_type == "AWS::CloudFormation::CustomResource";
        let service_token = if is_custom {
            resource
                .properties
                .get("ServiceToken")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string())
        } else {
            None
        };

        result.map(|res| StackResource {
            logical_id: resource.logical_id.clone(),
            physical_id: res.physical_id,
            resource_type: resource.resource_type.clone(),
            status: "CREATE_COMPLETE".to_string(),
            service_token,
            attributes: res.attributes,
        })
    }

    /// Apply a property update to an existing stack resource. Returns
    /// `Ok(Some(updated))` when the resource type supports in-place updates
    /// (the caller swaps the resulting `StackResource` for the old one) or
    /// `Ok(None)` when the type has no update path defined (the caller
    /// leaves the existing resource alone). `Err` propagates a
    /// resource-level failure up to the stack-level UPDATE_FAILED status.
    pub fn update_resource(
        &self,
        existing: &StackResource,
        new_def: &ResourceDefinition,
    ) -> Result<Option<StackResource>, String> {
        let result = match new_def.resource_type.as_str() {
            "AWS::Lambda::Function" => Some(self.update_lambda_function(existing, new_def)?),
            "AWS::Lambda::Permission" => Some(self.update_lambda_permission(existing, new_def)?),
            "AWS::Lambda::EventSourceMapping" => {
                Some(self.update_lambda_event_source_mapping(existing, new_def)?)
            }
            "AWS::Lambda::LayerVersion" => {
                Some(self.update_lambda_layer_version(existing, new_def)?)
            }
            "AWS::Lambda::Url" => Some(self.update_lambda_url(existing, new_def)?),
            "AWS::Lambda::Alias" => Some(self.update_lambda_alias(existing, new_def)?),
            "AWS::Lambda::Version" => Some(self.update_lambda_version(existing, new_def)?),
            "AWS::ApiGateway::RestApi" => Some(self.update_apigw_rest_api(existing, new_def)?),
            "AWS::ApiGateway::Resource" => Some(self.update_apigw_resource(existing, new_def)?),
            "AWS::ApiGateway::Method" => Some(self.update_apigw_method(existing, new_def)?),
            "AWS::ApiGateway::Deployment" => Some(self.update_apigw_deployment(existing, new_def)?),
            "AWS::ApiGateway::Stage" => Some(self.update_apigw_stage(existing, new_def)?),
            "AWS::ApiGateway::Authorizer" => Some(self.update_apigw_authorizer(existing, new_def)?),
            "AWS::ApiGateway::RequestValidator" => {
                Some(self.update_apigw_request_validator(existing, new_def)?)
            }
            "AWS::ApiGateway::Model" => Some(self.update_apigw_model(existing, new_def)?),
            "AWS::ApiGateway::GatewayResponse" => {
                Some(self.update_apigw_gateway_response(existing, new_def)?)
            }
            "AWS::ApiGateway::UsagePlan" => Some(self.update_apigw_usage_plan(existing, new_def)?),
            "AWS::ApiGateway::ApiKey" => Some(self.update_apigw_api_key(existing, new_def)?),
            "AWS::ApiGateway::UsagePlanKey" => {
                Some(self.update_apigw_usage_plan_key(existing, new_def)?)
            }
            "AWS::ApiGateway::DomainName" => {
                Some(self.update_apigw_domain_name(existing, new_def)?)
            }
            "AWS::ApiGateway::BasePathMapping" => {
                Some(self.update_apigw_base_path_mapping(existing, new_def)?)
            }
            "AWS::ApiGatewayV2::Api" => Some(self.update_apigwv2_api(existing, new_def)?),
            "AWS::ApiGatewayV2::Route" => Some(self.update_apigwv2_route(existing, new_def)?),
            "AWS::ApiGatewayV2::Integration" => {
                Some(self.update_apigwv2_integration(existing, new_def)?)
            }
            "AWS::ApiGatewayV2::IntegrationResponse" => {
                Some(self.update_apigwv2_integration_response(existing, new_def)?)
            }
            "AWS::ApiGatewayV2::RouteResponse" => {
                Some(self.update_apigwv2_route_response(existing, new_def)?)
            }
            "AWS::ApiGatewayV2::Stage" => Some(self.update_apigwv2_stage(existing, new_def)?),
            "AWS::ApiGatewayV2::Deployment" => {
                Some(self.update_apigwv2_deployment(existing, new_def)?)
            }
            "AWS::ApiGatewayV2::Authorizer" => {
                Some(self.update_apigwv2_authorizer(existing, new_def)?)
            }
            "AWS::ApiGatewayV2::DomainName" => {
                Some(self.update_apigwv2_domain_name(existing, new_def)?)
            }
            "AWS::ApiGatewayV2::ApiMapping" => {
                Some(self.update_apigwv2_api_mapping(existing, new_def)?)
            }
            "AWS::ApiGatewayV2::VpcLink" => Some(self.update_apigwv2_vpc_link(existing, new_def)?),
            "AWS::ApiGatewayV2::Model" => Some(self.update_apigwv2_model(existing, new_def)?),
            "AWS::ECS::Cluster" => Some(self.update_ecs_cluster(existing, new_def)?),
            "AWS::ECS::Service" => Some(self.update_ecs_service(existing, new_def)?),
            "AWS::ECS::TaskDefinition" => Some(self.update_ecs_task_definition(existing, new_def)?),
            "AWS::ECS::CapacityProvider" => {
                Some(self.update_ecs_capacity_provider(existing, new_def)?)
            }
            "AWS::ECR::Repository" => Some(self.update_ecr_repository(existing, new_def)?),
            "AWS::ECR::RepositoryPolicy" => {
                Some(self.update_ecr_repository_policy(existing, new_def)?)
            }
            "AWS::ECR::LifecyclePolicy" => {
                Some(self.update_ecr_lifecycle_policy(existing, new_def)?)
            }
            "AWS::ECR::RegistryPolicy" => Some(self.update_ecr_registry_policy(existing, new_def)?),
            "AWS::ECR::ReplicationConfiguration" => {
                Some(self.update_ecr_replication_configuration(existing, new_def)?)
            }
            "AWS::ECR::RegistryScanningConfiguration" => {
                Some(self.update_ecr_registry_scanning_configuration(existing, new_def)?)
            }
            "AWS::ECR::PullThroughCacheRule" => {
                Some(self.update_ecr_pull_through_cache_rule(existing, new_def)?)
            }
            "AWS::KMS::Key" => Some(self.update_kms_key(existing, new_def)?),
            "AWS::KMS::ReplicaKey" => Some(self.update_kms_replica_key(existing, new_def)?),
            "AWS::KMS::Alias" => Some(self.update_kms_alias(existing, new_def)?),
            "AWS::ElasticLoadBalancingV2::LoadBalancer" => {
                Some(self.update_elbv2_load_balancer(existing, new_def)?)
            }
            "AWS::ElasticLoadBalancingV2::TargetGroup" => {
                Some(self.update_elbv2_target_group(existing, new_def)?)
            }
            "AWS::ElasticLoadBalancingV2::Listener" => {
                Some(self.update_elbv2_listener(existing, new_def)?)
            }
            "AWS::ElasticLoadBalancingV2::ListenerRule" => {
                Some(self.update_elbv2_listener_rule(existing, new_def)?)
            }
            "AWS::ElasticLoadBalancingV2::ListenerCertificate" => {
                Some(self.update_elbv2_listener_certificate(existing, new_def)?)
            }
            "AWS::ElasticLoadBalancingV2::TrustStore" => {
                Some(self.update_elbv2_trust_store(existing, new_def)?)
            }
            "AWS::CloudWatch::Alarm" => Some(self.update_cloudwatch_alarm(existing, new_def)?),
            "AWS::CloudWatch::Dashboard" => {
                Some(self.update_cloudwatch_dashboard(existing, new_def)?)
            }
            _ => None,
        };

        Ok(result.map(|res| StackResource {
            logical_id: existing.logical_id.clone(),
            physical_id: res.physical_id,
            resource_type: existing.resource_type.clone(),
            status: "UPDATE_COMPLETE".to_string(),
            service_token: existing.service_token.clone(),
            attributes: res.attributes,
        }))
    }

    /// Resolve a `Fn::GetAtt` against a previously provisioned resource.
    /// Returns the attribute value as a string, or `None` if the resource
    /// type doesn't expose that attribute (caller falls back to a placeholder
    /// so multi-pass provisioning can retry).
    ///
    /// The lookup first checks attributes captured at create time on the
    /// `StackResource`, then falls back to live service-state queries for
    /// the well-known attribute names of each resource type. This means
    /// attributes that change after creation (e.g. Lambda `FunctionUrl`)
    /// resolve correctly even when the URL was added in a separate pass.
    pub fn get_att(&self, resource: &StackResource, attribute: &str) -> Option<String> {
        // Captured attributes are the source of truth — they were computed
        // at create time and never go stale for the resources we ship today.
        if let Some(v) = resource.attributes.get(attribute) {
            return Some(v.clone());
        }
        // Live-state fallback for attributes that aren't pre-captured. This
        // is the extension point for future provisioners.
        match resource.resource_type.as_str() {
            "AWS::S3::Bucket" => self.get_att_s3_bucket(&resource.physical_id, attribute),
            "AWS::Lambda::Function" => {
                self.get_att_lambda_function(&resource.physical_id, attribute)
            }
            "AWS::IAM::Role" => self.get_att_iam_role(&resource.physical_id, attribute),
            "AWS::SQS::Queue" => self.get_att_sqs_queue(&resource.physical_id, attribute),
            "AWS::SNS::Topic" => self.get_att_sns_topic(&resource.physical_id, attribute),
            "AWS::DynamoDB::Table" => self.get_att_dynamodb_table(&resource.physical_id, attribute),
            "AWS::KMS::Key" => self.get_att_kms_key(&resource.physical_id, attribute),
            "AWS::SecretsManager::Secret" => {
                self.get_att_secrets_manager_secret(&resource.physical_id, attribute)
            }
            "AWS::CloudFront::Distribution" => {
                self.get_att_cf_distribution(&resource.physical_id, attribute)
            }
            "AWS::ECS::Cluster" => self.get_att_ecs_cluster(&resource.physical_id, attribute),
            "AWS::ECS::Service" => self.get_att_ecs_service(&resource.physical_id, attribute),
            "AWS::ECS::CapacityProvider" => {
                self.get_att_ecs_capacity_provider(&resource.physical_id, attribute)
            }
            "AWS::ECR::Repository" => self.get_att_ecr_repository(&resource.physical_id, attribute),
            "AWS::ElasticLoadBalancingV2::LoadBalancer" => {
                self.get_att_elbv2_load_balancer(&resource.physical_id, attribute)
            }
            "AWS::ElasticLoadBalancingV2::TargetGroup" => {
                self.get_att_elbv2_target_group(&resource.physical_id, attribute)
            }
            "AWS::ElasticLoadBalancingV2::Listener" => {
                self.get_att_elbv2_listener(&resource.physical_id, attribute)
            }
            "AWS::ElasticLoadBalancingV2::ListenerRule" => {
                self.get_att_elbv2_listener_rule(&resource.physical_id, attribute)
            }
            "AWS::ElasticLoadBalancingV2::TrustStore" => {
                self.get_att_elbv2_trust_store(&resource.physical_id, attribute)
            }
            "AWS::WAFv2::WebACL" => self.get_att_wafv2_web_acl(&resource.physical_id, attribute),
            "AWS::WAFv2::IPSet" => self.get_att_wafv2_ip_set(&resource.physical_id, attribute),
            "AWS::WAFv2::RegexPatternSet" => {
                self.get_att_wafv2_regex_pattern_set(&resource.physical_id, attribute)
            }
            "AWS::WAFv2::RuleGroup" => {
                self.get_att_wafv2_rule_group(&resource.physical_id, attribute)
            }
            "AWS::SES::ConfigurationSet" => {
                self.get_att_ses_configuration_set(&resource.physical_id, attribute)
            }
            "AWS::SES::EmailIdentity" => {
                self.get_att_ses_email_identity(&resource.physical_id, attribute)
            }
            "AWS::SES::Template" => self.get_att_ses_template(&resource.physical_id, attribute),
            "AWS::SES::ContactList" => {
                self.get_att_ses_contact_list(&resource.physical_id, attribute)
            }
            "AWS::SES::DedicatedIpPool" => {
                self.get_att_ses_dedicated_ip_pool(&resource.physical_id, attribute)
            }
            "AWS::SES::ReceiptRuleSet" => {
                self.get_att_ses_receipt_rule_set(&resource.physical_id, attribute)
            }
            "AWS::Athena::DataCatalog" => {
                self.get_att_athena_data_catalog(&resource.physical_id, attribute)
            }
            "AWS::Athena::NamedQuery" => {
                self.get_att_athena_named_query(&resource.physical_id, attribute)
            }
            "AWS::Athena::WorkGroup" => {
                self.get_att_athena_work_group(&resource.physical_id, attribute)
            }
            "AWS::Athena::PreparedStatement" => {
                self.get_att_athena_prepared_statement(&resource.physical_id, attribute)
            }
            "AWS::CloudFormation::Stack" => {
                self.get_att_cloudformation_stack(&resource.physical_id, attribute)
            }
            _ => None,
        }
    }

    fn get_att_ses_configuration_set(&self, physical_id: &str, attribute: &str) -> Option<String> {
        let mut accounts = self.ses_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let cs = state.configuration_sets.get(physical_id)?;
        match attribute {
            "Name" => Some(cs.name.clone()),
            _ => None,
        }
    }

    fn get_att_ses_email_identity(&self, physical_id: &str, attribute: &str) -> Option<String> {
        let mut accounts = self.ses_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let id = state.identities.get(physical_id)?;
        match attribute {
            "IdentityName" => Some(id.identity_name.clone()),
            _ => None,
        }
    }

    fn get_att_ses_template(&self, physical_id: &str, attribute: &str) -> Option<String> {
        let mut accounts = self.ses_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let tpl = state.templates.get(physical_id)?;
        match attribute {
            "TemplateName" => Some(tpl.template_name.clone()),
            _ => None,
        }
    }

    fn get_att_ses_contact_list(&self, physical_id: &str, attribute: &str) -> Option<String> {
        let mut accounts = self.ses_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let cl = state.contact_lists.get(physical_id)?;
        match attribute {
            "ContactListName" => Some(cl.contact_list_name.clone()),
            _ => None,
        }
    }

    fn get_att_ses_dedicated_ip_pool(&self, physical_id: &str, attribute: &str) -> Option<String> {
        let mut accounts = self.ses_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let pool = state.dedicated_ip_pools.get(physical_id)?;
        match attribute {
            "PoolName" => Some(pool.pool_name.clone()),
            _ => None,
        }
    }

    fn get_att_ses_receipt_rule_set(&self, physical_id: &str, attribute: &str) -> Option<String> {
        let mut accounts = self.ses_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let rs = state.receipt_rule_sets.get(physical_id)?;
        match attribute {
            "RuleSetName" => Some(rs.name.clone()),
            _ => None,
        }
    }

    fn get_att_wafv2_web_acl(&self, physical_id: &str, attribute: &str) -> Option<String> {
        let mut accounts = self.wafv2_state.write();
        let state = accounts
            .accounts
            .entry(self.account_id.clone())
            .or_default();
        let acl = state.web_acls.values().find(|a| a.arn == physical_id)?;
        match attribute {
            "Arn" => Some(acl.arn.clone()),
            "Id" => Some(acl.id.clone()),
            "Name" => Some(acl.name.clone()),
            "LabelNamespace" => Some(acl.label_namespace.clone()),
            "Capacity" => Some(acl.capacity.to_string()),
            _ => None,
        }
    }

    fn get_att_wafv2_ip_set(&self, physical_id: &str, attribute: &str) -> Option<String> {
        let mut accounts = self.wafv2_state.write();
        let state = accounts
            .accounts
            .entry(self.account_id.clone())
            .or_default();
        let ip_set = state.ip_sets.values().find(|i| i.arn == physical_id)?;
        match attribute {
            "Arn" => Some(ip_set.arn.clone()),
            "Id" => Some(ip_set.id.clone()),
            "Name" => Some(ip_set.name.clone()),
            _ => None,
        }
    }

    fn get_att_wafv2_regex_pattern_set(
        &self,
        physical_id: &str,
        attribute: &str,
    ) -> Option<String> {
        let mut accounts = self.wafv2_state.write();
        let state = accounts
            .accounts
            .entry(self.account_id.clone())
            .or_default();
        let set = state
            .regex_pattern_sets
            .values()
            .find(|r| r.arn == physical_id)?;
        match attribute {
            "Arn" => Some(set.arn.clone()),
            "Id" => Some(set.id.clone()),
            "Name" => Some(set.name.clone()),
            _ => None,
        }
    }

    fn get_att_wafv2_rule_group(&self, physical_id: &str, attribute: &str) -> Option<String> {
        let mut accounts = self.wafv2_state.write();
        let state = accounts
            .accounts
            .entry(self.account_id.clone())
            .or_default();
        let rg = state.rule_groups.values().find(|r| r.arn == physical_id)?;
        match attribute {
            "Arn" => Some(rg.arn.clone()),
            "Id" => Some(rg.id.clone()),
            "Name" => Some(rg.name.clone()),
            _ => None,
        }
    }

    fn get_att_s3_bucket(&self, physical_id: &str, attribute: &str) -> Option<String> {
        let mut accounts = self.s3_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let bucket = state.buckets.get(physical_id)?;
        match attribute {
            "Arn" => Some(format!("arn:aws:s3:::{}", bucket.name)),
            "DomainName" => Some(format!("{}.s3.amazonaws.com", bucket.name)),
            "RegionalDomainName" => {
                Some(format!("{}.s3.{}.amazonaws.com", bucket.name, self.region))
            }
            "DualStackDomainName" => Some(format!(
                "{}.s3.dualstack.{}.amazonaws.com",
                bucket.name, self.region
            )),
            "WebsiteURL" => Some(format!(
                "http://{}.s3-website-{}.amazonaws.com",
                bucket.name, self.region
            )),
            _ => None,
        }
    }

    fn get_att_lambda_function(&self, physical_id: &str, attribute: &str) -> Option<String> {
        let function_name = parse_lambda_function_name(physical_id);
        let mut accounts = self.lambda_state.write();
        let state = accounts.get_or_create(&self.account_id);
        match attribute {
            "Arn" => state
                .functions
                .get(&function_name)
                .map(|f| f.function_arn.clone()),
            "FunctionUrl" => state
                .function_url_configs
                .get(&function_name)
                .map(|u| u.function_url.clone()),
            "Version" => state
                .functions
                .get(&function_name)
                .map(|f| f.version.clone()),
            _ => None,
        }
    }

    fn get_att_iam_role(&self, physical_id: &str, attribute: &str) -> Option<String> {
        let mut accounts = self.iam_state.write();
        let state = accounts.get_or_create(&self.account_id);
        // physical_id may be the role ARN or the role name depending on how
        // provisioning resolved Ref. Try both shapes.
        let role = state
            .roles
            .values()
            .find(|r| r.arn == physical_id || r.role_name == physical_id)?;
        match attribute {
            "Arn" => Some(role.arn.clone()),
            "RoleId" => Some(role.role_id.clone()),
            _ => None,
        }
    }

    fn get_att_sqs_queue(&self, physical_id: &str, attribute: &str) -> Option<String> {
        let mut accounts = self.sqs_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let queue = state.queues.get(physical_id)?;
        match attribute {
            "Arn" => Some(queue.arn.clone()),
            "QueueName" => Some(queue.queue_name.clone()),
            "QueueUrl" => Some(queue.queue_url.clone()),
            _ => None,
        }
    }

    fn get_att_sns_topic(&self, physical_id: &str, attribute: &str) -> Option<String> {
        let mut accounts = self.sns_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let topic = state.topics.get(physical_id)?;
        match attribute {
            "TopicArn" => Some(topic.topic_arn.clone()),
            "TopicName" => Some(topic.name.clone()),
            _ => None,
        }
    }

    fn get_att_dynamodb_table(&self, physical_id: &str, attribute: &str) -> Option<String> {
        let mut accounts = self.dynamodb_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let table = state.tables.get(physical_id)?;
        match attribute {
            "Arn" => Some(table.arn.clone()),
            "StreamArn" => table.stream_arn.clone(),
            _ => None,
        }
    }

    fn get_att_kms_key(&self, physical_id: &str, attribute: &str) -> Option<String> {
        let mut accounts = self.kms_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let key = state.keys.get(physical_id)?;
        match attribute {
            "Arn" => Some(key.arn.clone()),
            "KeyId" => Some(key.key_id.clone()),
            _ => None,
        }
    }

    fn get_att_secrets_manager_secret(&self, physical_id: &str, attribute: &str) -> Option<String> {
        let mut accounts = self.secretsmanager_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let secret = state.secrets.get(physical_id)?;
        match attribute {
            // Secrets Manager's CFN doc treats Id and Arn interchangeably —
            // both resolve to the secret ARN.
            "Arn" | "Id" => Some(secret.arn.clone()),
            _ => None,
        }
    }

    fn get_att_cf_distribution(&self, physical_id: &str, attribute: &str) -> Option<String> {
        // CloudFront state is keyed under a fixed "000000000000" account in the
        // fakecloud_cloudfront crate; matching create_cf_distribution above.
        let accounts = self.cloudfront_state.read();
        let state = accounts.get("000000000000")?;
        let dist = state.distributions.get(physical_id)?;
        match attribute {
            "DomainName" => Some(dist.domain_name.clone()),
            "Id" => Some(dist.id.clone()),
            _ => None,
        }
    }

    /// Delete a previously created resource.
    pub fn delete_resource(&self, resource: &StackResource) -> Result<(), String> {
        match resource.resource_type.as_str() {
            "AWS::SQS::Queue" => self.delete_sqs_queue(&resource.physical_id),
            "AWS::SNS::Topic" => self.delete_sns_topic(&resource.physical_id),
            "AWS::SNS::Subscription" => self.delete_sns_subscription(&resource.physical_id),
            "AWS::SSM::Parameter" => self.delete_ssm_parameter(&resource.physical_id),
            "AWS::IAM::Role" => self.delete_iam_role(&resource.physical_id),
            "AWS::IAM::Policy" => self.delete_iam_policy(&resource.physical_id),
            "AWS::IAM::User" => self.delete_iam_user(&resource.physical_id),
            "AWS::IAM::Group" => self.delete_iam_group(&resource.physical_id),
            "AWS::IAM::ManagedPolicy" => self.delete_iam_managed_policy(&resource.physical_id),
            "AWS::IAM::UserToGroupAddition" => {
                self.delete_iam_user_to_group_addition(&resource.physical_id)
            }
            "AWS::IAM::AccessKey" => self.delete_iam_access_key(&resource.physical_id),
            "AWS::IAM::InstanceProfile" => self.delete_iam_instance_profile(&resource.physical_id),
            "AWS::IAM::OIDCProvider" => self.delete_iam_oidc_provider(&resource.physical_id),
            "AWS::IAM::SAMLProvider" => self.delete_iam_saml_provider(&resource.physical_id),
            "AWS::IAM::ServiceLinkedRole" => {
                self.delete_iam_service_linked_role(&resource.physical_id)
            }
            "AWS::IAM::VirtualMFADevice" => {
                self.delete_iam_virtual_mfa_device(&resource.physical_id)
            }
            "AWS::S3::Bucket" => self.delete_s3_bucket(&resource.physical_id),
            "AWS::Events::Rule" => self.delete_eventbridge_rule(&resource.physical_id),
            "AWS::Events::Connection" => self.delete_eventbridge_connection(&resource.physical_id),
            "AWS::Events::EventBus" => self.delete_eventbridge_event_bus(&resource.physical_id),
            "AWS::Events::EventBusPolicy" => {
                self.delete_eventbridge_event_bus_policy(&resource.physical_id)
            }
            "AWS::Events::Endpoint" => self.delete_eventbridge_endpoint(&resource.physical_id),
            "AWS::Events::ApiDestination" => {
                self.delete_eventbridge_api_destination(&resource.physical_id)
            }
            "AWS::Events::Archive" => self.delete_eventbridge_archive(&resource.physical_id),
            "AWS::DynamoDB::Table" => self.delete_dynamodb_table(&resource.physical_id),
            "AWS::Logs::LogGroup" => self.delete_log_group(&resource.physical_id),
            "AWS::Logs::LogStream" => self.delete_log_stream(&resource.physical_id),
            "AWS::Logs::MetricFilter" => self.delete_metric_filter(&resource.physical_id),
            "AWS::Logs::SubscriptionFilter" => {
                self.delete_subscription_filter(&resource.physical_id)
            }
            "AWS::Logs::Destination" => self.delete_logs_destination(&resource.physical_id),
            "AWS::Logs::ResourcePolicy" => self.delete_logs_resource_policy(&resource.physical_id),
            "AWS::Logs::QueryDefinition" => {
                self.delete_logs_query_definition(&resource.physical_id)
            }
            "AWS::Logs::Delivery" => self.delete_logs_delivery(&resource.physical_id),
            "AWS::Logs::DeliveryDestination" => {
                self.delete_logs_delivery_destination(&resource.physical_id)
            }
            "AWS::Logs::DeliverySource" => self.delete_logs_delivery_source(&resource.physical_id),
            "AWS::Lambda::Function" => self.delete_lambda_function(&resource.physical_id),
            "AWS::Lambda::Permission" => self.delete_lambda_permission(&resource.physical_id),
            "AWS::Lambda::EventSourceMapping" => {
                self.delete_lambda_event_source_mapping(&resource.physical_id)
            }
            "AWS::Lambda::LayerVersion" => self.delete_lambda_layer_version(&resource.physical_id),
            "AWS::Lambda::Url" => self.delete_lambda_url(&resource.physical_id),
            "AWS::Lambda::Alias" => self.delete_lambda_alias(&resource.physical_id),
            "AWS::Lambda::Version" => self.delete_lambda_version(&resource.physical_id),
            "AWS::SecretsManager::Secret" => {
                self.delete_secrets_manager_secret(&resource.physical_id)
            }
            "AWS::Kinesis::Stream" => self.delete_kinesis_stream(&resource.physical_id),
            "AWS::Kinesis::StreamConsumer" => {
                self.delete_kinesis_stream_consumer(&resource.physical_id)
            }
            "AWS::KMS::Key" => self.delete_kms_key(&resource.physical_id),
            "AWS::KMS::ReplicaKey" => self.delete_kms_replica_key(&resource.physical_id),
            "AWS::KMS::Alias" => self.delete_kms_alias(&resource.physical_id),
            "AWS::ECR::Repository" => self.delete_ecr_repository(&resource.physical_id),
            "AWS::ECR::RepositoryPolicy" => {
                self.delete_ecr_repository_policy(&resource.physical_id)
            }
            "AWS::ECR::LifecyclePolicy" => self.delete_ecr_lifecycle_policy(&resource.physical_id),
            "AWS::ECR::RegistryPolicy" => self.delete_ecr_registry_policy(),
            "AWS::ECR::ReplicationConfiguration" => self.delete_ecr_replication_configuration(),
            "AWS::ECR::RegistryScanningConfiguration" => {
                self.delete_ecr_registry_scanning_configuration()
            }
            "AWS::ECR::PullThroughCacheRule" => {
                self.delete_ecr_pull_through_cache_rule(&resource.physical_id)
            }
            "AWS::CloudWatch::Alarm" => self.delete_cloudwatch_alarm(&resource.physical_id),
            "AWS::CloudWatch::Dashboard" => self.delete_cloudwatch_dashboard(&resource.physical_id),
            "AWS::ElasticLoadBalancingV2::LoadBalancer" => {
                self.delete_elbv2_load_balancer(&resource.physical_id)
            }
            "AWS::ElasticLoadBalancingV2::TargetGroup" => {
                self.delete_elbv2_target_group(&resource.physical_id)
            }
            "AWS::ElasticLoadBalancingV2::Listener" => {
                self.delete_elbv2_listener(&resource.physical_id)
            }
            "AWS::ElasticLoadBalancingV2::ListenerRule" => {
                self.delete_elbv2_listener_rule(&resource.physical_id)
            }
            "AWS::ElasticLoadBalancingV2::ListenerCertificate" => {
                self.delete_elbv2_listener_certificate(&resource.physical_id)
            }
            "AWS::ElasticLoadBalancingV2::TrustStore" => {
                self.delete_elbv2_trust_store(&resource.physical_id)
            }
            "AWS::Organizations::Organization" => self.delete_organization(&resource.physical_id),
            "AWS::Organizations::OrganizationalUnit" => {
                self.delete_organization_unit(&resource.physical_id)
            }
            "AWS::Organizations::Account" => {
                self.delete_organization_account(&resource.physical_id)
            }
            "AWS::Organizations::Policy" => self.delete_organization_policy(&resource.physical_id),
            "AWS::Organizations::ResourcePolicy" => {
                self.delete_organization_resource_policy(&resource.physical_id)
            }
            "AWS::Cognito::UserPool" => self.delete_cognito_user_pool(&resource.physical_id),
            "AWS::Cognito::UserPoolClient" => {
                self.delete_cognito_user_pool_client(&resource.physical_id)
            }
            "AWS::Cognito::UserPoolDomain" => {
                self.delete_cognito_user_pool_domain(&resource.physical_id)
            }
            "AWS::Cognito::IdentityPool" => {
                self.delete_cognito_identity_pool(&resource.physical_id)
            }
            "AWS::Cognito::IdentityPoolRoleAttachment" => {
                self.delete_cognito_identity_pool_role_attachment(&resource.physical_id)
            }
            "AWS::RDS::DBSubnetGroup" => self.delete_rds_subnet_group(&resource.physical_id),
            "AWS::RDS::DBParameterGroup" => self.delete_rds_parameter_group(&resource.physical_id),
            "AWS::RDS::DBClusterParameterGroup" => {
                self.delete_rds_cluster_parameter_group(&resource.physical_id)
            }
            "AWS::RDS::OptionGroup" => self.delete_rds_option_group(&resource.physical_id),
            "AWS::RDS::EventSubscription" => {
                self.delete_rds_event_subscription(&resource.physical_id)
            }
            "AWS::RDS::DBSecurityGroup" => self.delete_rds_security_group(&resource.physical_id),
            "AWS::RDS::DBProxy" => self.delete_rds_db_proxy(&resource.physical_id),
            "AWS::RDS::DBInstance" => self.delete_rds_db_instance(&resource.physical_id),
            "AWS::RDS::DBCluster" => self.delete_rds_db_cluster(&resource.physical_id),
            "AWS::ECS::Cluster" => self.delete_ecs_cluster(&resource.physical_id),
            "AWS::ECS::TaskDefinition" => self.delete_ecs_task_definition(&resource.physical_id),
            "AWS::ECS::Service" => self.delete_ecs_service(&resource.physical_id),
            "AWS::ECS::CapacityProvider" => {
                self.delete_ecs_capacity_provider(&resource.physical_id)
            }
            "AWS::CertificateManager::Certificate" => {
                self.delete_acm_certificate(&resource.physical_id)
            }
            "AWS::CertificateManager::Account" => self.delete_acm_account(),
            "AWS::ElastiCache::ParameterGroup" => {
                self.delete_ec_parameter_group(&resource.physical_id)
            }
            "AWS::ElastiCache::SubnetGroup" => self.delete_ec_subnet_group(&resource.physical_id),
            "AWS::ElastiCache::SecurityGroup" => {
                self.delete_ec_security_group(&resource.physical_id)
            }
            "AWS::ElastiCache::User" => self.delete_ec_user(&resource.physical_id),
            "AWS::ElastiCache::UserGroup" => self.delete_ec_user_group(&resource.physical_id),
            "AWS::ElastiCache::CacheCluster" => self.delete_ec_cache_cluster(&resource.physical_id),
            "AWS::ElastiCache::ReplicationGroup" => {
                self.delete_ec_replication_group(&resource.physical_id)
            }
            "AWS::Route53::HostedZone" => self.delete_route53_hosted_zone(&resource.physical_id),
            "AWS::Route53::RecordSet" => {
                self.delete_route53_record_set(&resource.physical_id, &resource.attributes)
            }
            "AWS::Route53::HealthCheck" => self.delete_route53_health_check(&resource.physical_id),
            "AWS::Route53::DNSSEC" => self.delete_route53_dnssec(&resource.physical_id),
            "AWS::Route53::KeySigningKey" => {
                self.delete_route53_key_signing_key(&resource.physical_id)
            }
            "AWS::CloudFront::CloudFrontOriginAccessIdentity" => {
                self.delete_cf_origin_access_identity(&resource.physical_id)
            }
            "AWS::CloudFront::Distribution" => self.delete_cf_distribution(&resource.physical_id),
            "AWS::CloudFront::OriginAccessControl" => {
                self.delete_cf_origin_access_control(&resource.physical_id)
            }
            "AWS::CloudFront::PublicKey" => self.delete_cf_public_key(&resource.physical_id),
            "AWS::CloudFront::KeyGroup" => self.delete_cf_key_group(&resource.physical_id),
            "AWS::CloudFront::Function" => self.delete_cf_function(&resource.physical_id),
            "AWS::CloudFront::CachePolicy" => self.delete_cf_cache_policy(&resource.physical_id),
            "AWS::CloudFront::OriginRequestPolicy" => {
                self.delete_cf_origin_request_policy(&resource.physical_id)
            }
            "AWS::CloudFront::ResponseHeadersPolicy" => {
                self.delete_cf_response_headers_policy(&resource.physical_id)
            }
            "AWS::StepFunctions::StateMachine" => {
                self.delete_sfn_state_machine(&resource.physical_id)
            }
            "AWS::StepFunctions::Activity" => self.delete_sfn_activity(&resource.physical_id),
            "AWS::StepFunctions::StateMachineVersion" => {
                self.delete_sfn_version(&resource.physical_id)
            }
            "AWS::StepFunctions::StateMachineAlias" => self.delete_sfn_alias(&resource.physical_id),
            "AWS::WAFv2::WebACL" => self.delete_wafv2_web_acl(&resource.physical_id),
            "AWS::WAFv2::IPSet" => self.delete_wafv2_ip_set(&resource.physical_id),
            "AWS::WAFv2::RegexPatternSet" => {
                self.delete_wafv2_regex_pattern_set(&resource.physical_id)
            }
            "AWS::WAFv2::RuleGroup" => self.delete_wafv2_rule_group(&resource.physical_id),
            "AWS::WAFv2::LoggingConfiguration" => {
                self.delete_wafv2_logging_configuration(&resource.physical_id)
            }
            "AWS::WAFv2::WebACLAssociation" => {
                self.delete_wafv2_web_acl_association(&resource.physical_id)
            }
            "AWS::ApiGateway::RestApi" => self.delete_apigw_rest_api(&resource.physical_id),
            "AWS::ApiGateway::Resource" => {
                self.delete_apigw_resource(&resource.physical_id, &resource.attributes)
            }
            "AWS::ApiGateway::Method" => self.delete_apigw_method(&resource.physical_id),
            "AWS::ApiGateway::Deployment" => {
                self.delete_apigw_deployment(&resource.physical_id, &resource.attributes)
            }
            "AWS::ApiGateway::Stage" => {
                self.delete_apigw_stage(&resource.physical_id, &resource.attributes)
            }
            "AWS::ApiGateway::Authorizer" => {
                self.delete_apigw_authorizer(&resource.physical_id, &resource.attributes)
            }
            "AWS::ApiGateway::RequestValidator" => {
                self.delete_apigw_request_validator(&resource.physical_id, &resource.attributes)
            }
            "AWS::ApiGateway::Model" => {
                self.delete_apigw_model(&resource.physical_id, &resource.attributes)
            }
            "AWS::ApiGateway::GatewayResponse" => {
                self.delete_apigw_gateway_response(&resource.physical_id, &resource.attributes)
            }
            "AWS::ApiGateway::UsagePlan" => self.delete_apigw_usage_plan(&resource.physical_id),
            "AWS::ApiGateway::ApiKey" => self.delete_apigw_api_key(&resource.physical_id),
            "AWS::ApiGateway::UsagePlanKey" => {
                self.delete_apigw_usage_plan_key(&resource.physical_id, &resource.attributes)
            }
            "AWS::ApiGateway::DomainName" => self.delete_apigw_domain_name(&resource.physical_id),
            "AWS::ApiGateway::BasePathMapping" => {
                self.delete_apigw_base_path_mapping(&resource.physical_id, &resource.attributes)
            }
            "AWS::ApiGatewayV2::Api" => self.delete_apigwv2_api(&resource.physical_id),
            "AWS::ApiGatewayV2::Route" => {
                self.delete_apigwv2_route(&resource.physical_id, &resource.attributes)
            }
            "AWS::ApiGatewayV2::Integration" => {
                self.delete_apigwv2_integration(&resource.physical_id, &resource.attributes)
            }
            "AWS::ApiGatewayV2::IntegrationResponse" => self
                .delete_apigwv2_integration_response(&resource.physical_id, &resource.attributes),
            "AWS::ApiGatewayV2::RouteResponse" => {
                self.delete_apigwv2_route_response(&resource.physical_id, &resource.attributes)
            }
            "AWS::ApiGatewayV2::Stage" => {
                self.delete_apigwv2_stage(&resource.physical_id, &resource.attributes)
            }
            "AWS::ApiGatewayV2::Deployment" => {
                self.delete_apigwv2_deployment(&resource.physical_id, &resource.attributes)
            }
            "AWS::ApiGatewayV2::Authorizer" => {
                self.delete_apigwv2_authorizer(&resource.physical_id, &resource.attributes)
            }
            "AWS::ApiGatewayV2::DomainName" => {
                self.delete_apigwv2_domain_name(&resource.physical_id)
            }
            "AWS::ApiGatewayV2::ApiMapping" => {
                self.delete_apigwv2_api_mapping(&resource.physical_id, &resource.attributes)
            }
            "AWS::ApiGatewayV2::VpcLink" => self.delete_apigwv2_vpc_link(&resource.physical_id),
            "AWS::ApiGatewayV2::Model" => {
                self.delete_apigwv2_model(&resource.physical_id, &resource.attributes)
            }
            "AWS::SES::ConfigurationSet" => {
                self.delete_ses_configuration_set(&resource.physical_id)
            }
            "AWS::SES::ConfigurationSetEventDestination" => {
                self.delete_ses_event_destination(&resource.physical_id, &resource.attributes)
            }
            "AWS::SES::EmailIdentity" => self.delete_ses_email_identity(&resource.physical_id),
            "AWS::SES::Template" => self.delete_ses_template(&resource.physical_id),
            "AWS::SES::ContactList" => self.delete_ses_contact_list(&resource.physical_id),
            "AWS::SES::DedicatedIpPool" => self.delete_ses_dedicated_ip_pool(&resource.physical_id),
            "AWS::SES::ReceiptRule" => {
                self.delete_ses_receipt_rule(&resource.physical_id, &resource.attributes)
            }
            "AWS::SES::ReceiptRuleSet" => self.delete_ses_receipt_rule_set(&resource.physical_id),
            "AWS::SES::ReceiptFilter" => self.delete_ses_receipt_filter(&resource.physical_id),
            "AWS::SES::VdmAttributes" => Ok(()),
            "AWS::SecretsManager::RotationSchedule" => {
                self.delete_secrets_manager_rotation_schedule(&resource.physical_id)
            }
            "AWS::SecretsManager::ResourcePolicy" => {
                self.delete_secrets_manager_resource_policy(&resource.physical_id)
            }
            "AWS::SecretsManager::SecretTargetAttachment" => Ok(()),
            "AWS::ApplicationAutoScaling::ScalableTarget" => self
                .delete_application_autoscaling_scalable_target(
                    &resource.physical_id,
                    &resource.attributes,
                ),
            "AWS::ApplicationAutoScaling::ScalingPolicy" => self
                .delete_application_autoscaling_scaling_policy(
                    &resource.physical_id,
                    &resource.attributes,
                ),
            "AWS::Athena::DataCatalog" => self.delete_athena_data_catalog(&resource.physical_id),
            "AWS::Athena::NamedQuery" => self.delete_athena_named_query(&resource.physical_id),
            "AWS::Athena::WorkGroup" => self.delete_athena_work_group(&resource.physical_id),
            "AWS::Athena::PreparedStatement" => {
                self.delete_athena_prepared_statement(&resource.physical_id, &resource.attributes)
            }
            "AWS::KinesisFirehose::DeliveryStream" => {
                self.delete_firehose_delivery_stream(&resource.physical_id)
            }
            "AWS::Glue::Database" => self.delete_glue_database(&resource.physical_id),
            "AWS::CloudFormation::Stack" => self.delete_cloudformation_stack(&resource.physical_id),
            "AWS::Glue::Table" => self.delete_glue_table(&resource.physical_id),
            "AWS::Glue::Partition" => {
                self.delete_glue_partition(&resource.physical_id, &resource.attributes)
            }
            t if t.starts_with("Custom::") || t == "AWS::CloudFormation::CustomResource" => {
                self.delete_custom_resource(resource)
            }
            other => Err(format!("Unsupported resource type: {other}")),
        }
    }

    // --- SQS ---

    fn create_sqs_queue(&self, resource: &ResourceDefinition) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let queue_name = props
            .get("QueueName")
            .and_then(|v| v.as_str())
            .unwrap_or(&resource.logical_id);

        let mut __sqs_mas = self.sqs_state.write();
        let state = __sqs_mas.get_or_create(&self.account_id);
        let queue_url = format!("{}/{}/{}", state.endpoint, state.account_id, queue_name);
        let arn = format!(
            "arn:aws:sqs:{}:{}:{}",
            state.region, state.account_id, queue_name
        );

        let is_fifo = queue_name.ends_with(".fifo");
        let mut attributes = std::collections::BTreeMap::new();
        if let Some(obj) = props.as_object() {
            for (k, v) in obj {
                if k != "QueueName" {
                    if let Some(s) = v.as_str() {
                        attributes.insert(k.clone(), s.to_string());
                    } else if let Some(n) = v.as_i64() {
                        attributes.insert(k.clone(), n.to_string());
                    }
                }
            }
        }

        let queue = SqsQueue {
            queue_name: queue_name.to_string(),
            queue_url: queue_url.clone(),
            arn: arn.clone(),
            created_at: Utc::now(),
            messages: std::collections::VecDeque::new(),
            inflight: Vec::new(),
            attributes,
            is_fifo,
            dedup_cache: std::collections::BTreeMap::new(),
            redrive_policy: None,
            tags: std::collections::BTreeMap::new(),
            next_sequence_number: 0,
            permission_labels: Vec::new(),
            receipt_handle_map: std::collections::BTreeMap::new(),
        };

        state
            .name_to_url
            .insert(queue_name.to_string(), queue_url.clone());
        state.queues.insert(queue_url.clone(), queue);

        Ok(ProvisionResult::new(queue_url.clone())
            .with("Arn", arn)
            .with("QueueName", queue_name)
            .with("QueueUrl", queue_url))
    }

    fn delete_sqs_queue(&self, physical_id: &str) -> Result<(), String> {
        let mut __sqs_mas = self.sqs_state.write();
        let state = __sqs_mas.get_or_create(&self.account_id);
        if let Some(queue) = state.queues.remove(physical_id) {
            state.name_to_url.remove(&queue.queue_name);
        }
        Ok(())
    }

    // --- SNS ---

    fn create_sns_topic(&self, resource: &ResourceDefinition) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let topic_name = props
            .get("TopicName")
            .and_then(|v| v.as_str())
            .unwrap_or(&resource.logical_id);

        let mut __sns_mas = self.sns_state.write();
        let state = __sns_mas.get_or_create(&self.account_id);
        let topic_arn = format!(
            "arn:aws:sns:{}:{}:{}",
            state.region, state.account_id, topic_name
        );

        let topic = SnsTopic {
            topic_arn: topic_arn.clone(),
            name: topic_name.to_string(),
            attributes: BTreeMap::new(),
            tags: Vec::new(),
            is_fifo: topic_name.ends_with(".fifo"),
            created_at: Utc::now(),
            subscriptions_deleted: 0,
        };

        state.topics.insert(topic_arn.clone(), topic);
        Ok(ProvisionResult::new(topic_arn.clone())
            .with("TopicArn", topic_arn)
            .with("TopicName", topic_name))
    }

    fn delete_sns_topic(&self, physical_id: &str) -> Result<(), String> {
        let mut __sns_mas = self.sns_state.write();
        let state = __sns_mas.get_or_create(&self.account_id);
        state.topics.remove(physical_id);
        // Also remove subscriptions for this topic
        state
            .subscriptions
            .retain(|_, sub| sub.topic_arn != physical_id);
        Ok(())
    }

    // --- SNS Subscription ---

    fn create_sns_subscription(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let topic_arn = props
            .get("TopicArn")
            .and_then(|v| v.as_str())
            .ok_or("SNS Subscription requires TopicArn")?;
        let protocol = props
            .get("Protocol")
            .and_then(|v| v.as_str())
            .ok_or("SNS Subscription requires Protocol")?;
        let endpoint = props
            .get("Endpoint")
            .and_then(|v| v.as_str())
            .ok_or("SNS Subscription requires Endpoint")?;

        let mut __sns_mas = self.sns_state.write();
        let state = __sns_mas.get_or_create(&self.account_id);

        // Validate that the topic exists
        if !state.topics.contains_key(topic_arn) {
            return Err(format!("Topic ARN does not exist: {topic_arn}"));
        }

        let sub_arn = format!("{}:{}", topic_arn, Uuid::new_v4());

        let subscription = SnsSubscription {
            subscription_arn: sub_arn.clone(),
            topic_arn: topic_arn.to_string(),
            protocol: protocol.to_string(),
            endpoint: endpoint.to_string(),
            owner: state.account_id.clone(),
            attributes: BTreeMap::new(),
            confirmed: true,
            confirmation_token: None,
        };

        state.subscriptions.insert(sub_arn.clone(), subscription);
        Ok(ProvisionResult::new(sub_arn.clone()).with("Arn", sub_arn))
    }

    fn delete_sns_subscription(&self, physical_id: &str) -> Result<(), String> {
        let mut __sns_mas = self.sns_state.write();
        let state = __sns_mas.get_or_create(&self.account_id);
        state.subscriptions.remove(physical_id);
        Ok(())
    }

    // --- SSM ---

    fn create_ssm_parameter(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let name = props
            .get("Name")
            .and_then(|v| v.as_str())
            .ok_or("SSM Parameter requires Name")?;
        let value = props
            .get("Value")
            .and_then(|v| v.as_str())
            .ok_or("SSM Parameter requires Value")?;
        let param_type = props
            .get("Type")
            .and_then(|v| v.as_str())
            .unwrap_or("String");

        let mut accounts = self.ssm_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let arn = format!(
            "arn:aws:ssm:{}:{}:parameter{}",
            self.region,
            self.account_id,
            if name.starts_with('/') {
                name.to_string()
            } else {
                format!("/{name}")
            }
        );

        let parameter = SsmParameter {
            name: name.to_string(),
            value: value.to_string(),
            param_type: param_type.to_string(),
            version: 1,
            arn: arn.clone(),
            last_modified: Utc::now(),
            history: Vec::new(),
            tags: BTreeMap::new(),
            labels: BTreeMap::new(),
            description: props
                .get("Description")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            allowed_pattern: None,
            key_id: None,
            data_type: "text".to_string(),
            tier: "Standard".to_string(),
            policies: None,
            expiration_notified: false,
            no_change_notified: false,
        };

        state.parameters.insert(name.to_string(), parameter);
        Ok(ProvisionResult::new(name)
            .with("Type", param_type)
            .with("Value", value))
    }

    fn delete_ssm_parameter(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.ssm_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.parameters.remove(physical_id);
        Ok(())
    }

    // --- IAM Role ---

    fn create_iam_role(&self, resource: &ResourceDefinition) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let role_name = props
            .get("RoleName")
            .and_then(|v| v.as_str())
            .unwrap_or(&resource.logical_id);

        let assume_role_policy = props
            .get("AssumeRolePolicyDocument")
            .map(|v| {
                if v.is_string() {
                    v.as_str().unwrap().to_string()
                } else {
                    serde_json::to_string(v).unwrap_or_default()
                }
            })
            .unwrap_or_default();

        let path = props.get("Path").and_then(|v| v.as_str()).unwrap_or("/");

        let mut accounts = self.iam_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let role_id = format!(
            "FKIA{}",
            &Uuid::new_v4().to_string().replace('-', "").to_uppercase()[..16]
        );
        let arn = format!(
            "arn:aws:iam::{}:role{}{}",
            state.account_id,
            if path == "/" { "/" } else { path },
            role_name
        );

        let role = IamRole {
            role_name: role_name.to_string(),
            role_id: role_id.clone(),
            arn: arn.clone(),
            path: path.to_string(),
            assume_role_policy_document: assume_role_policy,
            created_at: Utc::now(),
            description: props
                .get("Description")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            max_session_duration: 3600,
            tags: Vec::new(),
            permissions_boundary: None,
        };

        state.roles.insert(role_name.to_string(), role);
        Ok(ProvisionResult::new(arn.clone())
            .with("Arn", arn)
            .with("RoleId", role_id))
    }

    fn delete_iam_role(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.iam_state.write();
        let state = accounts.get_or_create(&self.account_id);
        // physical_id is the ARN; find the role name
        let role_name = state
            .roles
            .iter()
            .find(|(_, r)| r.arn == physical_id)
            .map(|(name, _)| name.clone());
        if let Some(name) = role_name {
            state.roles.remove(&name);
            state.role_policies.remove(&name);
            state.role_inline_policies.remove(&name);
        }
        Ok(())
    }

    // --- IAM Policy ---

    fn create_iam_policy(&self, resource: &ResourceDefinition) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let policy_name = props
            .get("PolicyName")
            .and_then(|v| v.as_str())
            .unwrap_or(&resource.logical_id);

        let policy_document = props
            .get("PolicyDocument")
            .map(|v| {
                if v.is_string() {
                    v.as_str().unwrap().to_string()
                } else {
                    serde_json::to_string(v).unwrap_or_default()
                }
            })
            .unwrap_or_default();

        let path = props.get("Path").and_then(|v| v.as_str()).unwrap_or("/");

        let mut accounts = self.iam_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let policy_id = format!(
            "FSIA{}",
            &Uuid::new_v4().to_string().replace('-', "").to_uppercase()[..16]
        );
        let arn = format!(
            "arn:aws:iam::{}:policy{}{}",
            state.account_id,
            if path == "/" { "/" } else { path },
            policy_name
        );

        let now = Utc::now();
        let policy = IamPolicy {
            policy_name: policy_name.to_string(),
            policy_id,
            arn: arn.clone(),
            path: path.to_string(),
            description: props
                .get("Description")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string(),
            created_at: now,
            tags: Vec::new(),
            default_version_id: "v1".to_string(),
            versions: vec![PolicyVersion {
                version_id: "v1".to_string(),
                document: policy_document,
                is_default: true,
                created_at: now,
            }],
            next_version_num: 2,
            attachment_count: 0,
        };

        state.policies.insert(arn.clone(), policy);
        Ok(ProvisionResult::new(arn.clone()).with("Arn", arn))
    }

    fn delete_iam_policy(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.iam_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.policies.remove(physical_id);
        Ok(())
    }

    fn create_iam_user(&self, resource: &ResourceDefinition) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let user_name = props
            .get("UserName")
            .and_then(|v| v.as_str())
            .unwrap_or(&resource.logical_id)
            .to_string();
        let path = props
            .get("Path")
            .and_then(|v| v.as_str())
            .unwrap_or("/")
            .to_string();
        let permissions_boundary = props
            .get("PermissionsBoundary")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let tags = parse_iam_tags(props.get("Tags"));

        let mut accounts = self.iam_state.write();
        let state = accounts.get_or_create(&self.account_id);
        if state.users.contains_key(&user_name) {
            return Err(format!("User {user_name} already exists"));
        }
        let arn = format!(
            "arn:aws:iam::{}:user{}{}",
            state.account_id, path, user_name
        );
        let user_id = format!(
            "AIDA{}",
            &Uuid::new_v4().to_string().replace('-', "").to_uppercase()[..16]
        );
        let user = IamUser {
            user_name: user_name.clone(),
            user_id: user_id.clone(),
            arn: arn.clone(),
            path,
            created_at: Utc::now(),
            tags,
            permissions_boundary,
        };
        state.users.insert(user_name.clone(), user);

        // Inline + managed policies declared inline on the user.
        if let Some(policies) = props.get("Policies").and_then(|v| v.as_array()) {
            let inline = state
                .user_inline_policies
                .entry(user_name.clone())
                .or_default();
            for p in policies {
                if let (Some(n), Some(doc)) = (
                    p.get("PolicyName").and_then(|v| v.as_str()),
                    p.get("PolicyDocument"),
                ) {
                    let document = if doc.is_string() {
                        doc.as_str().unwrap_or("").to_string()
                    } else {
                        serde_json::to_string(doc).unwrap_or_default()
                    };
                    inline.insert(n.to_string(), document);
                }
            }
        }
        if let Some(arns) = props.get("ManagedPolicyArns").and_then(|v| v.as_array()) {
            let attached = state.user_policies.entry(user_name.clone()).or_default();
            for a in arns {
                if let Some(s) = a.as_str() {
                    if !attached.contains(&s.to_string()) {
                        attached.push(s.to_string());
                    }
                }
            }
        }
        if let Some(groups) = props.get("Groups").and_then(|v| v.as_array()) {
            for g in groups {
                if let Some(g_name) = g.as_str() {
                    if let Some(group) = state.groups.get_mut(g_name) {
                        if !group.members.iter().any(|m| m == &user_name) {
                            group.members.push(user_name.clone());
                        }
                    }
                }
            }
        }

        Ok(ProvisionResult::new(user_name).with("Arn", arn))
    }

    fn delete_iam_user(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.iam_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.users.remove(physical_id);
        state.user_inline_policies.remove(physical_id);
        state.user_policies.remove(physical_id);
        state.access_keys.remove(physical_id);
        for group in state.groups.values_mut() {
            group.members.retain(|m| m != physical_id);
        }
        Ok(())
    }

    fn create_iam_group(&self, resource: &ResourceDefinition) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let group_name = props
            .get("GroupName")
            .and_then(|v| v.as_str())
            .unwrap_or(&resource.logical_id)
            .to_string();
        let path = props
            .get("Path")
            .and_then(|v| v.as_str())
            .unwrap_or("/")
            .to_string();

        let mut accounts = self.iam_state.write();
        let state = accounts.get_or_create(&self.account_id);
        if state.groups.contains_key(&group_name) {
            return Err(format!("Group {group_name} already exists"));
        }
        let arn = format!(
            "arn:aws:iam::{}:group{}{}",
            state.account_id, path, group_name
        );
        let group_id = format!(
            "AGPA{}",
            &Uuid::new_v4().to_string().replace('-', "").to_uppercase()[..16]
        );
        let mut inline_policies: BTreeMap<String, String> = BTreeMap::new();
        if let Some(policies) = props.get("Policies").and_then(|v| v.as_array()) {
            for p in policies {
                if let (Some(n), Some(doc)) = (
                    p.get("PolicyName").and_then(|v| v.as_str()),
                    p.get("PolicyDocument"),
                ) {
                    let document = if doc.is_string() {
                        doc.as_str().unwrap_or("").to_string()
                    } else {
                        serde_json::to_string(doc).unwrap_or_default()
                    };
                    inline_policies.insert(n.to_string(), document);
                }
            }
        }
        let mut attached_policies: Vec<String> = Vec::new();
        if let Some(arns) = props.get("ManagedPolicyArns").and_then(|v| v.as_array()) {
            for a in arns {
                if let Some(s) = a.as_str() {
                    attached_policies.push(s.to_string());
                }
            }
        }
        state.groups.insert(
            group_name.clone(),
            IamGroup {
                group_name: group_name.clone(),
                group_id,
                arn: arn.clone(),
                path,
                created_at: Utc::now(),
                members: Vec::new(),
                inline_policies,
                attached_policies,
            },
        );

        Ok(ProvisionResult::new(group_name).with("Arn", arn))
    }

    fn delete_iam_group(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.iam_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.groups.remove(physical_id);
        Ok(())
    }

    fn create_iam_managed_policy(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        // Same shape as AWS::IAM::Policy minus the inline-attach knobs;
        // ManagedPolicy is a standalone policy, attached separately.
        let props = &resource.properties;
        let policy_name = props
            .get("ManagedPolicyName")
            .and_then(|v| v.as_str())
            .unwrap_or(&resource.logical_id)
            .to_string();
        let policy_document = props
            .get("PolicyDocument")
            .map(|v| {
                if v.is_string() {
                    v.as_str().unwrap_or("").to_string()
                } else {
                    serde_json::to_string(v).unwrap_or_default()
                }
            })
            .unwrap_or_default();
        let path = props
            .get("Path")
            .and_then(|v| v.as_str())
            .unwrap_or("/")
            .to_string();
        let description = props
            .get("Description")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();

        let mut accounts = self.iam_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let arn = format!(
            "arn:aws:iam::{}:policy{}{}",
            state.account_id,
            if path == "/" { "/" } else { path.as_str() },
            policy_name
        );
        if state.policies.contains_key(&arn) {
            return Err(format!("Managed policy {policy_name} already exists"));
        }
        let policy_id = format!(
            "ANPA{}",
            &Uuid::new_v4().to_string().replace('-', "").to_uppercase()[..16]
        );
        let now = Utc::now();
        state.policies.insert(
            arn.clone(),
            IamPolicy {
                policy_name,
                policy_id,
                arn: arn.clone(),
                path,
                description,
                created_at: now,
                tags: Vec::new(),
                default_version_id: "v1".to_string(),
                versions: vec![PolicyVersion {
                    version_id: "v1".to_string(),
                    document: policy_document,
                    is_default: true,
                    created_at: now,
                }],
                next_version_num: 2,
                attachment_count: 0,
            },
        );

        // Attach to declared users/groups/roles.
        if let Some(users) = props.get("Users").and_then(|v| v.as_array()) {
            for u in users {
                if let Some(name) = u.as_str() {
                    let attached = state.user_policies.entry(name.to_string()).or_default();
                    if !attached.contains(&arn) {
                        attached.push(arn.clone());
                    }
                }
            }
        }
        if let Some(groups) = props.get("Groups").and_then(|v| v.as_array()) {
            for g in groups {
                if let Some(name) = g.as_str() {
                    if let Some(group) = state.groups.get_mut(name) {
                        if !group.attached_policies.contains(&arn) {
                            group.attached_policies.push(arn.clone());
                        }
                    }
                }
            }
        }
        if let Some(roles) = props.get("Roles").and_then(|v| v.as_array()) {
            for r in roles {
                if let Some(name) = r.as_str() {
                    let attached = state.role_policies.entry(name.to_string()).or_default();
                    if !attached.contains(&arn) {
                        attached.push(arn.clone());
                    }
                }
            }
        }

        Ok(ProvisionResult::new(arn.clone()).with("Arn", arn))
    }

    fn delete_iam_managed_policy(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.iam_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.policies.remove(physical_id);
        for arns in state.user_policies.values_mut() {
            arns.retain(|a| a != physical_id);
        }
        for arns in state.role_policies.values_mut() {
            arns.retain(|a| a != physical_id);
        }
        for group in state.groups.values_mut() {
            group.attached_policies.retain(|a| a != physical_id);
        }
        Ok(())
    }

    fn create_iam_user_to_group_addition(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let group_name = props
            .get("GroupName")
            .and_then(|v| v.as_str())
            .ok_or_else(|| "GroupName is required".to_string())?
            .to_string();
        let users: Vec<String> = props
            .get("Users")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|u| u.as_str().map(|s| s.to_string()))
                    .collect()
            })
            .unwrap_or_default();

        let mut accounts = self.iam_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let group = state
            .groups
            .get_mut(&group_name)
            .ok_or_else(|| format!("Group {group_name} does not exist"))?;
        for u in &users {
            if !group.members.iter().any(|m| m == u) {
                group.members.push(u.clone());
            }
        }

        // Encode group + users so delete can revert exactly this addition.
        let physical_id = format!("{group_name}|{}", users.join(","));
        Ok(ProvisionResult::new(physical_id))
    }

    fn delete_iam_user_to_group_addition(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.iam_state.write();
        let state = accounts.get_or_create(&self.account_id);
        if let Some((group_name, users)) = physical_id.split_once('|') {
            if let Some(group) = state.groups.get_mut(group_name) {
                let to_remove: Vec<&str> = users.split(',').filter(|s| !s.is_empty()).collect();
                group.members.retain(|m| !to_remove.iter().any(|u| u == m));
            }
        }
        Ok(())
    }

    fn create_iam_access_key(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let user_name = props
            .get("UserName")
            .and_then(|v| v.as_str())
            .ok_or_else(|| "UserName is required".to_string())?
            .to_string();
        let status = props
            .get("Status")
            .and_then(|v| v.as_str())
            .unwrap_or("Active")
            .to_string();

        let mut accounts = self.iam_state.write();
        let state = accounts.get_or_create(&self.account_id);
        if !state.users.contains_key(&user_name) {
            return Err(format!("User {user_name} does not exist"));
        }
        let access_key_id = format!(
            "AKIA{}",
            &Uuid::new_v4().to_string().replace('-', "").to_uppercase()[..16]
        );
        let secret_access_key: String = Uuid::new_v4()
            .to_string()
            .replace('-', "")
            .chars()
            .take(40)
            .collect();
        state
            .access_keys
            .entry(user_name.clone())
            .or_default()
            .push(IamAccessKey {
                access_key_id: access_key_id.clone(),
                secret_access_key: secret_access_key.clone(),
                user_name: user_name.clone(),
                status,
                created_at: Utc::now(),
            });

        Ok(ProvisionResult::new(access_key_id.clone()).with("SecretAccessKey", secret_access_key))
    }

    fn delete_iam_access_key(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.iam_state.write();
        let state = accounts.get_or_create(&self.account_id);
        for keys in state.access_keys.values_mut() {
            keys.retain(|k| k.access_key_id != physical_id);
        }
        Ok(())
    }

    fn create_iam_instance_profile(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let name = props
            .get("InstanceProfileName")
            .and_then(|v| v.as_str())
            .unwrap_or(&resource.logical_id)
            .to_string();
        let path = props
            .get("Path")
            .and_then(|v| v.as_str())
            .unwrap_or("/")
            .to_string();
        // Roles[] entries can be plain role names or `Ref`-resolved role
        // ARNs (which the IAM Role provisioner emits as physical_id);
        // extract the trailing name segment so DescribeInstanceProfile
        // round-trips a name list.
        let roles: Vec<String> = props
            .get("Roles")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|r| r.as_str())
                    .map(|s| {
                        if let Some(rest) = s.strip_prefix("arn:aws:iam::") {
                            rest.split(":role/")
                                .nth(1)
                                .map(|name| name.to_string())
                                .unwrap_or_else(|| s.to_string())
                        } else {
                            s.to_string()
                        }
                    })
                    .collect()
            })
            .unwrap_or_default();

        let mut accounts = self.iam_state.write();
        let state = accounts.get_or_create(&self.account_id);
        if state.instance_profiles.contains_key(&name) {
            return Err(format!("InstanceProfile {name} already exists"));
        }
        // Force a retry pass when role refs haven't been resolved yet: a
        // logical-id placeholder won't match any real role, and silently
        // storing it would leave DescribeInstanceProfile returning an
        // empty Roles array.
        for role_name in &roles {
            if !state.roles.contains_key(role_name) {
                return Err(format!(
                    "InstanceProfile {name}: referenced role {role_name} not yet provisioned"
                ));
            }
        }
        let arn = format!(
            "arn:aws:iam::{}:instance-profile{}{}",
            state.account_id, path, name
        );
        let id = format!(
            "AIPA{}",
            &Uuid::new_v4().to_string().replace('-', "").to_uppercase()[..16]
        );
        state.instance_profiles.insert(
            name.clone(),
            IamInstanceProfile {
                instance_profile_name: name.clone(),
                instance_profile_id: id,
                arn: arn.clone(),
                path,
                created_at: Utc::now(),
                roles,
                tags: Vec::new(),
            },
        );

        Ok(ProvisionResult::new(name).with("Arn", arn))
    }

    fn delete_iam_instance_profile(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.iam_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.instance_profiles.remove(physical_id);
        Ok(())
    }

    fn create_iam_oidc_provider(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let url = props
            .get("Url")
            .and_then(|v| v.as_str())
            .ok_or("Url is required")?
            .to_string();
        let client_id_list: Vec<String> = props
            .get("ClientIdList")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();
        let thumbprint_list: Vec<String> = props
            .get("ThumbprintList")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();
        // Real AWS strips the scheme to form the resource path component.
        let url_path = url
            .trim_start_matches("https://")
            .trim_start_matches("http://")
            .to_string();
        let arn = format!(
            "arn:aws:iam::{}:oidc-provider/{}",
            self.account_id, url_path
        );
        let provider = OidcProvider {
            arn: arn.clone(),
            url,
            client_id_list,
            thumbprint_list,
            created_at: Utc::now(),
            tags: Vec::new(),
        };
        let mut accounts = self.iam_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.oidc_providers.insert(arn.clone(), provider);
        Ok(ProvisionResult::new(arn.clone()).with("Arn", arn))
    }

    fn delete_iam_oidc_provider(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.iam_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.oidc_providers.remove(physical_id);
        Ok(())
    }

    fn create_iam_saml_provider(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let name = props
            .get("Name")
            .and_then(|v| v.as_str())
            .map(String::from)
            .unwrap_or_else(|| {
                let suffix = Uuid::new_v4().simple().to_string();
                format!("{}-{}", resource.logical_id, &suffix[..8])
            });
        let saml_metadata_document = props
            .get("SamlMetadataDocument")
            .and_then(|v| v.as_str())
            .ok_or("SamlMetadataDocument is required")?
            .to_string();
        let arn = format!("arn:aws:iam::{}:saml-provider/{name}", self.account_id);
        let now = Utc::now();
        let valid_until = now + chrono::Duration::days(365 * 10);
        let provider = SamlProvider {
            arn: arn.clone(),
            name,
            saml_metadata_document,
            created_at: now,
            valid_until,
            tags: Vec::new(),
        };
        let mut accounts = self.iam_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.saml_providers.insert(arn.clone(), provider);
        Ok(ProvisionResult::new(arn.clone()).with("Arn", arn))
    }

    fn delete_iam_saml_provider(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.iam_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.saml_providers.remove(physical_id);
        Ok(())
    }

    fn create_iam_service_linked_role(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let aws_service_name = props
            .get("AWSServiceName")
            .and_then(|v| v.as_str())
            .ok_or("AWSServiceName is required")?
            .to_string();
        let custom_suffix = props
            .get("CustomSuffix")
            .and_then(|v| v.as_str())
            .map(String::from);
        let description = props
            .get("Description")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        // AWS service-linked role naming convention.
        let service_short = aws_service_name.split('.').next().unwrap_or("Service");
        let role_name = match &custom_suffix {
            Some(s) => format!("AWSServiceRoleFor{service_short}_{s}"),
            None => format!("AWSServiceRoleFor{service_short}"),
        };
        let path = format!("/aws-service-role/{aws_service_name}/");
        let arn = format!("arn:aws:iam::{}:role{path}{role_name}", self.account_id);
        // Service-linked roles get a trust policy specific to the service.
        let assume_role_policy_document = serde_json::json!({
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": {"Service": aws_service_name.clone()},
                "Action": "sts:AssumeRole"
            }]
        })
        .to_string();
        let role_id_suffix = Uuid::new_v4().simple().to_string();
        let role = IamRole {
            role_name: role_name.clone(),
            role_id: format!("AROA{}", role_id_suffix[..16].to_uppercase()),
            arn: arn.clone(),
            path,
            assume_role_policy_document,
            created_at: Utc::now(),
            description: Some(description),
            max_session_duration: 3600,
            tags: Vec::new(),
            permissions_boundary: None,
        };
        let mut accounts = self.iam_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.roles.insert(role_name.clone(), role);
        Ok(ProvisionResult::new(role_name)
            .with("Arn", arn)
            .with("RoleId", String::new()))
    }

    fn delete_iam_service_linked_role(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.iam_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.roles.remove(physical_id);
        Ok(())
    }

    fn create_iam_virtual_mfa_device(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let name = props
            .get("VirtualMfaDeviceName")
            .and_then(|v| v.as_str())
            .ok_or("VirtualMfaDeviceName is required")?
            .to_string();
        let path = props
            .get("Path")
            .and_then(|v| v.as_str())
            .unwrap_or("/")
            .to_string();
        let serial_number = format!("arn:aws:iam::{}:mfa{}{name}", self.account_id, path);
        // Real AWS returns a base32 seed + a PNG QR code; we synthesize
        // deterministic placeholders so callers can read them back.
        let seed = format!("BASE32SEED{}", Uuid::new_v4().simple());
        let user = props
            .get("Users")
            .and_then(|v| v.as_array())
            .and_then(|arr| arr.first())
            .and_then(|v| v.as_str())
            .map(String::from);
        let device = VirtualMfaDevice {
            serial_number: serial_number.clone(),
            base32_string_seed: seed,
            qr_code_png: String::new(),
            enable_date: user.as_ref().map(|_| Utc::now()),
            user,
            tags: Vec::new(),
        };
        let mut accounts = self.iam_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state
            .virtual_mfa_devices
            .insert(serial_number.clone(), device);
        Ok(ProvisionResult::new(serial_number.clone()).with("SerialNumber", serial_number))
    }

    fn delete_iam_virtual_mfa_device(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.iam_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.virtual_mfa_devices.remove(physical_id);
        Ok(())
    }

    // --- S3 ---

    fn create_s3_bucket(&self, resource: &ResourceDefinition) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let bucket_name = props
            .get("BucketName")
            .and_then(|v| v.as_str())
            .unwrap_or(&resource.logical_id);

        let mut __s3_mas = self.s3_state.write();
        let state = __s3_mas.get_or_create(&self.account_id);
        let region = state.region.clone();
        let bucket = S3Bucket::new(bucket_name, &state.region, &state.account_id);
        state.buckets.insert(bucket_name.to_string(), bucket);

        let arn = format!("arn:aws:s3:::{bucket_name}");
        let domain_name = format!("{bucket_name}.s3.amazonaws.com");
        let regional_domain_name = format!("{bucket_name}.s3.{region}.amazonaws.com");
        let dual_stack_domain_name = format!("{bucket_name}.s3.dualstack.{region}.amazonaws.com");
        let website_url = format!("http://{bucket_name}.s3-website-{region}.amazonaws.com");
        Ok(ProvisionResult::new(bucket_name)
            .with("Arn", arn)
            .with("DomainName", domain_name)
            .with("RegionalDomainName", regional_domain_name)
            .with("DualStackDomainName", dual_stack_domain_name)
            .with("WebsiteURL", website_url))
    }

    fn delete_s3_bucket(&self, physical_id: &str) -> Result<(), String> {
        let mut __s3_mas = self.s3_state.write();
        let state = __s3_mas.get_or_create(&self.account_id);
        state.buckets.remove(physical_id);
        Ok(())
    }

    // --- EventBridge ---

    fn create_eventbridge_rule(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let rule_name = props
            .get("Name")
            .and_then(|v| v.as_str())
            .unwrap_or(&resource.logical_id);
        let event_bus_name = props
            .get("EventBusName")
            .and_then(|v| v.as_str())
            .unwrap_or("default");

        let mut eb_accounts = self.eventbridge_state.write();
        let state = eb_accounts.get_or_create(&self.account_id);

        // Validate that the event bus exists
        if !state.buses.contains_key(event_bus_name) {
            return Err(format!("Event bus does not exist: {event_bus_name}"));
        }

        let arn = if event_bus_name == "default" {
            format!(
                "arn:aws:events:{}:{}:rule/{}",
                state.region, state.account_id, rule_name
            )
        } else {
            format!(
                "arn:aws:events:{}:{}:rule/{}/{}",
                state.region, state.account_id, event_bus_name, rule_name
            )
        };

        let rule = EventRule {
            name: rule_name.to_string(),
            arn: arn.clone(),
            event_bus_name: event_bus_name.to_string(),
            event_pattern: props.get("EventPattern").map(|v| {
                if v.is_string() {
                    v.as_str().unwrap().to_string()
                } else {
                    serde_json::to_string(v).unwrap_or_default()
                }
            }),
            schedule_expression: props
                .get("ScheduleExpression")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            state: props
                .get("State")
                .and_then(|v| v.as_str())
                .unwrap_or("ENABLED")
                .to_string(),
            description: props
                .get("Description")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            role_arn: props
                .get("RoleArn")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            managed_by: None,
            created_by: None,
            targets: Vec::new(),
            tags: std::collections::BTreeMap::new(),
            last_fired: None,
        };

        state
            .rules
            .insert((event_bus_name.to_string(), rule_name.to_string()), rule);
        Ok(ProvisionResult::new(arn.clone()).with("Arn", arn))
    }

    fn delete_eventbridge_rule(&self, physical_id: &str) -> Result<(), String> {
        let mut eb_accounts = self.eventbridge_state.write();
        let state = eb_accounts.default_mut();
        // physical_id is the ARN; find the rule key
        let key = state
            .rules
            .iter()
            .find(|(_, r)| r.arn == physical_id)
            .map(|(k, _)| k.clone());
        if let Some(k) = key {
            state.rules.remove(&k);
        }
        Ok(())
    }

    fn create_eventbridge_connection(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let name = props
            .get("Name")
            .and_then(|v| v.as_str())
            .unwrap_or(&resource.logical_id)
            .to_string();
        let description = props
            .get("Description")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let authorization_type = props
            .get("AuthorizationType")
            .and_then(|v| v.as_str())
            .unwrap_or("API_KEY")
            .to_string();
        let auth_parameters = props
            .get("AuthParameters")
            .cloned()
            .unwrap_or(serde_json::Value::Object(serde_json::Map::new()));

        let mut eb_accounts = self.eventbridge_state.write();
        let state = eb_accounts.get_or_create(&self.account_id);
        if state.connections.contains_key(&name) {
            return Err(format!("Connection {name} already exists"));
        }
        let now = Utc::now();
        let arn = format!(
            "arn:aws:events:{}:{}:connection/{}/{}",
            state.region,
            state.account_id,
            name,
            Uuid::new_v4().as_simple()
        );
        let secret_arn = format!(
            "arn:aws:secretsmanager:{}:{}:secret:events!connection/{}-{}",
            state.region,
            state.account_id,
            name,
            Uuid::new_v4().as_simple()
        );
        let connection = Connection {
            name: name.clone(),
            arn: arn.clone(),
            description,
            authorization_type,
            auth_parameters,
            connection_state: "AUTHORIZED".to_string(),
            secret_arn: secret_arn.clone(),
            creation_time: now,
            last_modified_time: now,
            last_authorized_time: now,
        };
        state.connections.insert(name.clone(), connection);

        Ok(ProvisionResult::new(name)
            .with("Arn", arn)
            .with("SecretArn", secret_arn))
    }

    fn delete_eventbridge_connection(&self, physical_id: &str) -> Result<(), String> {
        let mut eb_accounts = self.eventbridge_state.write();
        let state = eb_accounts.get_or_create(&self.account_id);
        state.connections.remove(physical_id);
        Ok(())
    }

    fn create_eventbridge_api_destination(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let name = props
            .get("Name")
            .and_then(|v| v.as_str())
            .unwrap_or(&resource.logical_id)
            .to_string();
        let description = props
            .get("Description")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let connection_arn = props
            .get("ConnectionArn")
            .and_then(|v| v.as_str())
            .ok_or_else(|| "ConnectionArn is required".to_string())?
            .to_string();
        let invocation_endpoint = props
            .get("InvocationEndpoint")
            .and_then(|v| v.as_str())
            .ok_or_else(|| "InvocationEndpoint is required".to_string())?
            .to_string();
        let http_method = props
            .get("HttpMethod")
            .and_then(|v| v.as_str())
            .unwrap_or("POST")
            .to_string();
        let invocation_rate_limit_per_second = props
            .get("InvocationRateLimitPerSecond")
            .and_then(|v| v.as_i64());

        let mut eb_accounts = self.eventbridge_state.write();
        let state = eb_accounts.get_or_create(&self.account_id);
        if state.api_destinations.contains_key(&name) {
            return Err(format!("ApiDestination {name} already exists"));
        }
        let now = Utc::now();
        let arn = format!(
            "arn:aws:events:{}:{}:api-destination/{}/{}",
            state.region,
            state.account_id,
            name,
            Uuid::new_v4().as_simple()
        );
        state.api_destinations.insert(
            name.clone(),
            ApiDestination {
                name: name.clone(),
                arn: arn.clone(),
                description,
                connection_arn,
                invocation_endpoint,
                http_method,
                invocation_rate_limit_per_second,
                state: "ACTIVE".to_string(),
                creation_time: now,
                last_modified_time: now,
            },
        );

        Ok(ProvisionResult::new(name).with("Arn", arn))
    }

    fn delete_eventbridge_api_destination(&self, physical_id: &str) -> Result<(), String> {
        let mut eb_accounts = self.eventbridge_state.write();
        let state = eb_accounts.get_or_create(&self.account_id);
        state.api_destinations.remove(physical_id);
        Ok(())
    }

    fn create_eventbridge_archive(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let name = props
            .get("ArchiveName")
            .and_then(|v| v.as_str())
            .unwrap_or(&resource.logical_id)
            .to_string();
        let event_source_arn = props
            .get("SourceArn")
            .and_then(|v| v.as_str())
            .ok_or_else(|| "SourceArn is required".to_string())?
            .to_string();
        let description = props
            .get("Description")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let event_pattern = props.get("EventPattern").map(|v| {
            if v.is_string() {
                v.as_str().unwrap_or("").to_string()
            } else {
                serde_json::to_string(v).unwrap_or_default()
            }
        });
        let retention_days = props
            .get("RetentionDays")
            .and_then(|v| v.as_i64())
            .unwrap_or(0);

        let mut eb_accounts = self.eventbridge_state.write();
        let state = eb_accounts.get_or_create(&self.account_id);
        if state.archives.contains_key(&name) {
            return Err(format!("Archive {name} already exists"));
        }
        let arn = format!(
            "arn:aws:events:{}:{}:archive/{}",
            state.region, state.account_id, name
        );
        state.archives.insert(
            name.clone(),
            Archive {
                name: name.clone(),
                arn: arn.clone(),
                event_source_arn,
                description,
                event_pattern,
                retention_days,
                state: "ENABLED".to_string(),
                creation_time: Utc::now(),
                event_count: 0,
                size_bytes: 0,
                events: Vec::new(),
            },
        );

        Ok(ProvisionResult::new(name).with("Arn", arn))
    }

    fn delete_eventbridge_archive(&self, physical_id: &str) -> Result<(), String> {
        let mut eb_accounts = self.eventbridge_state.write();
        let state = eb_accounts.get_or_create(&self.account_id);
        state.archives.remove(physical_id);
        Ok(())
    }

    fn create_eventbridge_event_bus(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let name = props
            .get("Name")
            .and_then(|v| v.as_str())
            .ok_or("Name is required")?
            .to_string();
        let description = props
            .get("Description")
            .and_then(|v| v.as_str())
            .map(String::from);
        let kms_key_identifier = props
            .get("KmsKeyIdentifier")
            .and_then(|v| v.as_str())
            .map(String::from);
        let dead_letter_config = props.get("DeadLetterConfig").cloned();
        let policy = props.get("Policy").cloned();
        let arn = format!(
            "arn:aws:events:{}:{}:event-bus/{name}",
            self.region, self.account_id
        );
        let now = Utc::now();
        let bus = EventBus {
            name: name.clone(),
            arn: arn.clone(),
            tags: BTreeMap::new(),
            policy,
            description,
            kms_key_identifier,
            dead_letter_config,
            creation_time: now,
            last_modified_time: now,
        };

        let mut eb_accounts = self.eventbridge_state.write();
        let state = eb_accounts.get_or_create(&self.account_id);
        state.buses.insert(name.clone(), bus);

        Ok(ProvisionResult::new(name).with("Arn", arn))
    }

    fn delete_eventbridge_event_bus(&self, physical_id: &str) -> Result<(), String> {
        let mut eb_accounts = self.eventbridge_state.write();
        let state = eb_accounts.get_or_create(&self.account_id);
        // The default bus is reserved; refuse to delete it.
        if physical_id == "default" {
            return Ok(());
        }
        state.buses.remove(physical_id);
        Ok(())
    }

    fn create_eventbridge_event_bus_policy(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let bus_name = props
            .get("EventBusName")
            .and_then(|v| v.as_str())
            .unwrap_or("default")
            .to_string();
        // Statement is the v2 shape; the older v1 shape has Action+Principal+
        // StatementId; both ultimately end up as a single statement on the bus
        // policy. We accept either form.
        let statement = if let Some(s) = props.get("Statement") {
            s.clone()
        } else {
            let sid = props
                .get("Sid")
                .or_else(|| props.get("StatementId"))
                .and_then(|v| v.as_str())
                .map(String::from);
            let action = props
                .get("Action")
                .and_then(|v| v.as_str())
                .map(String::from);
            let principal = props.get("Principal").cloned();
            let condition = props.get("Condition").cloned();
            let mut obj = serde_json::json!({
                "Effect": "Allow",
                "Resource": format!(
                    "arn:aws:events:{}:{}:event-bus/{bus_name}",
                    self.region, self.account_id
                ),
            });
            if let (Some(sid), Some(obj)) = (sid, obj.as_object_mut()) {
                obj.insert("Sid".to_string(), serde_json::Value::String(sid));
            }
            if let (Some(action), Some(obj)) = (action, obj.as_object_mut()) {
                obj.insert("Action".to_string(), serde_json::Value::String(action));
            }
            if let (Some(principal), Some(obj)) = (principal, obj.as_object_mut()) {
                obj.insert("Principal".to_string(), principal);
            }
            if let (Some(condition), Some(obj)) = (condition, obj.as_object_mut()) {
                obj.insert("Condition".to_string(), condition);
            }
            obj
        };

        let mut eb_accounts = self.eventbridge_state.write();
        let state = eb_accounts.get_or_create(&self.account_id);
        let bus = state
            .buses
            .get_mut(&bus_name)
            .ok_or_else(|| format!("EventBus {bus_name} not yet provisioned"))?;
        // Append to the existing policy's Statement array, or create one.
        match bus.policy.as_mut() {
            Some(serde_json::Value::Object(obj)) => {
                if let Some(serde_json::Value::Array(arr)) = obj.get_mut("Statement") {
                    arr.push(statement);
                } else {
                    obj.insert(
                        "Statement".to_string(),
                        serde_json::Value::Array(vec![statement]),
                    );
                }
            }
            _ => {
                bus.policy = Some(serde_json::json!({
                    "Version": "2012-10-17",
                    "Statement": [statement],
                }));
            }
        }

        // Physical id encodes the bus name + a synthetic id so delete locates the
        // entry even when the user never sets Sid/StatementId.
        let pid = format!("{bus_name}|{}", Uuid::new_v4().simple());
        Ok(ProvisionResult::new(pid))
    }

    fn delete_eventbridge_event_bus_policy(&self, physical_id: &str) -> Result<(), String> {
        let bus_name = physical_id
            .split_once('|')
            .map(|(b, _)| b.to_string())
            .unwrap_or_else(|| "default".to_string());
        let mut eb_accounts = self.eventbridge_state.write();
        let state = eb_accounts.get_or_create(&self.account_id);
        if let Some(bus) = state.buses.get_mut(&bus_name) {
            bus.policy = None;
        }
        Ok(())
    }

    fn create_eventbridge_endpoint(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let name = props
            .get("Name")
            .and_then(|v| v.as_str())
            .ok_or("Name is required")?
            .to_string();
        let description = props
            .get("Description")
            .and_then(|v| v.as_str())
            .map(String::from);
        let routing_config = props
            .get("RoutingConfig")
            .cloned()
            .ok_or("RoutingConfig is required")?;
        let replication_config = props.get("ReplicationConfig").cloned();
        let event_buses = props
            .get("EventBuses")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        let role_arn = props
            .get("RoleArn")
            .and_then(|v| v.as_str())
            .map(String::from);

        let endpoint_id = Uuid::new_v4().simple().to_string()[..16].to_string();
        let arn = format!(
            "arn:aws:events:{}:{}:endpoint/{name}",
            self.region, self.account_id
        );
        let endpoint_url = format!("https://{endpoint_id}.endpoint.events.amazonaws.com");
        let now = Utc::now();
        let endpoint = Endpoint {
            name: name.clone(),
            arn: arn.clone(),
            endpoint_id: endpoint_id.clone(),
            endpoint_url: Some(endpoint_url.clone()),
            description,
            routing_config,
            replication_config,
            event_buses,
            role_arn,
            state: "ACTIVE".to_string(),
            creation_time: now,
            last_modified_time: now,
        };

        let mut eb_accounts = self.eventbridge_state.write();
        let state = eb_accounts.get_or_create(&self.account_id);
        state.endpoints.insert(name.clone(), endpoint);

        Ok(ProvisionResult::new(name)
            .with("Arn", arn)
            .with("EndpointId", endpoint_id)
            .with("EndpointUrl", endpoint_url)
            .with("State", "ACTIVE"))
    }

    fn delete_eventbridge_endpoint(&self, physical_id: &str) -> Result<(), String> {
        let mut eb_accounts = self.eventbridge_state.write();
        let state = eb_accounts.get_or_create(&self.account_id);
        state.endpoints.remove(physical_id);
        Ok(())
    }

    // --- DynamoDB ---

    fn create_dynamodb_table(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let table_name = props
            .get("TableName")
            .and_then(|v| v.as_str())
            .unwrap_or(&resource.logical_id);

        let mut key_schema = Vec::new();
        if let Some(ks) = props.get("KeySchema").and_then(|v| v.as_array()) {
            for item in ks {
                let attr_name = item
                    .get("AttributeName")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                let key_type = item
                    .get("KeyType")
                    .and_then(|v| v.as_str())
                    .unwrap_or("HASH")
                    .to_string();
                key_schema.push(KeySchemaElement {
                    attribute_name: attr_name,
                    key_type,
                });
            }
        }

        let mut attribute_definitions = Vec::new();
        if let Some(defs) = props.get("AttributeDefinitions").and_then(|v| v.as_array()) {
            for item in defs {
                let attr_name = item
                    .get("AttributeName")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                let attr_type = item
                    .get("AttributeType")
                    .and_then(|v| v.as_str())
                    .unwrap_or("S")
                    .to_string();
                attribute_definitions.push(AttributeDefinition {
                    attribute_name: attr_name,
                    attribute_type: attr_type,
                });
            }
        }

        let billing_mode = props
            .get("BillingMode")
            .and_then(|v| v.as_str())
            .unwrap_or("PAY_PER_REQUEST")
            .to_string();

        let provisioned_throughput = if billing_mode == "PROVISIONED" {
            if let Some(pt) = props.get("ProvisionedThroughput") {
                ProvisionedThroughput {
                    read_capacity_units: pt
                        .get("ReadCapacityUnits")
                        .and_then(|v| v.as_i64())
                        .unwrap_or(5),
                    write_capacity_units: pt
                        .get("WriteCapacityUnits")
                        .and_then(|v| v.as_i64())
                        .unwrap_or(5),
                }
            } else {
                ProvisionedThroughput {
                    read_capacity_units: 5,
                    write_capacity_units: 5,
                }
            }
        } else {
            ProvisionedThroughput {
                read_capacity_units: 0,
                write_capacity_units: 0,
            }
        };

        // Parse StreamSpecification from CloudFormation properties
        let (stream_enabled, stream_view_type) =
            if let Some(stream_spec) = props.get("StreamSpecification") {
                let view_type = stream_spec
                    .get("StreamViewType")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string());
                let enabled = stream_spec
                    .get("StreamEnabled")
                    .and_then(|v| v.as_bool().or_else(|| v.as_str().map(|s| s == "true")))
                    // If StreamViewType is set, treat streams as enabled even if StreamEnabled is missing
                    .unwrap_or(view_type.is_some());
                (enabled, view_type)
            } else {
                (false, None)
            };

        let deletion_protection_enabled = props
            .get("DeletionProtectionEnabled")
            .and_then(|v| v.as_bool().or_else(|| v.as_str().map(|s| s == "true")))
            .unwrap_or(false);

        let on_demand_throughput = props
            .get("OnDemandThroughput")
            .map(|odt| OnDemandThroughput {
                max_read_request_units: odt
                    .get("MaxReadRequestUnits")
                    .and_then(|v| v.as_i64())
                    .unwrap_or(-1),
                max_write_request_units: odt
                    .get("MaxWriteRequestUnits")
                    .and_then(|v| v.as_i64())
                    .unwrap_or(-1),
            });

        let mut __ddb_mas = self.dynamodb_state.write();
        let state = __ddb_mas.get_or_create(&self.account_id);
        let arn = format!(
            "arn:aws:dynamodb:{}:{}:table/{}",
            state.region, state.account_id, table_name
        );

        let stream_arn = if stream_enabled {
            Some(format!(
                "{}/stream/{}",
                arn,
                Utc::now().format("%Y-%m-%dT%H:%M:%S.%3f")
            ))
        } else {
            None
        };
        let stream_arn_attr = stream_arn.clone();

        let table = DynamoTable {
            name: table_name.to_string(),
            arn: arn.clone(),
            table_id: Uuid::new_v4().to_string().replace('-', ""),
            key_schema,
            attribute_definitions,
            provisioned_throughput,
            items: Vec::new(),
            gsi: Vec::new(),
            lsi: Vec::new(),
            tags: BTreeMap::new(),
            created_at: Utc::now(),
            status: "ACTIVE".to_string(),
            item_count: 0,
            size_bytes: 0,
            billing_mode,
            ttl_attribute: None,
            ttl_enabled: false,
            resource_policy: None,
            pitr_enabled: false,
            kinesis_destinations: Vec::new(),
            contributor_insights_status: "DISABLED".to_string(),
            contributor_insights_counters: BTreeMap::new(),
            stream_enabled,
            stream_view_type,
            stream_arn,
            stream_records: Arc::new(RwLock::new(Vec::new())),
            sse_type: None,
            sse_kms_key_arn: None,
            deletion_protection_enabled,
            on_demand_throughput,
        };

        state.tables.insert(table_name.to_string(), table);
        let mut result = ProvisionResult::new(arn.clone()).with("Arn", arn);
        if let Some(stream_arn_value) = stream_arn_attr {
            result = result.with("StreamArn", stream_arn_value);
        }
        Ok(result)
    }

    fn delete_dynamodb_table(&self, physical_id: &str) -> Result<(), String> {
        let mut __ddb_mas = self.dynamodb_state.write();
        let state = __ddb_mas.get_or_create(&self.account_id);
        // physical_id is the ARN; find the table name
        let table_name = state
            .tables
            .iter()
            .find(|(_, t)| t.arn == physical_id)
            .map(|(name, _)| name.clone());
        if let Some(name) = table_name {
            state.tables.remove(&name);
        }
        Ok(())
    }

    // --- CloudWatch Logs ---

    fn create_log_group(&self, resource: &ResourceDefinition) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let log_group_name = props
            .get("LogGroupName")
            .and_then(|v| v.as_str())
            .unwrap_or(&resource.logical_id);

        let retention_in_days = props
            .get("RetentionInDays")
            .and_then(|v| v.as_i64())
            .map(|v| v as i32);

        let mut logs_accounts = self.logs_state.write();
        let state = logs_accounts.get_or_create(&self.account_id);
        let arn = format!(
            "arn:aws:logs:{}:{}:log-group:{}:*",
            state.region, state.account_id, log_group_name
        );

        let log_group = fakecloud_logs::LogGroup {
            name: log_group_name.to_string(),
            arn: arn.clone(),
            creation_time: Utc::now().timestamp_millis(),
            retention_in_days,
            kms_key_id: None,
            stored_bytes: 0,
            log_streams: std::collections::BTreeMap::new(),
            tags: std::collections::BTreeMap::new(),
            subscription_filters: Vec::new(),
            data_protection_policy: None,
            index_policies: Vec::new(),
            transformer: None,
            deletion_protection: false,
            log_group_class: Some("STANDARD".to_string()),
        };

        state
            .log_groups
            .insert(log_group_name.to_string(), log_group);
        Ok(ProvisionResult::new(arn.clone()).with("Arn", arn))
    }

    fn create_lambda_function(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let function_name = props
            .get("FunctionName")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_else(|| {
                format!(
                    "{}-{}-{}",
                    self.stack_id
                        .rsplit('/')
                        .nth(1)
                        .unwrap_or(&resource.logical_id),
                    resource.logical_id,
                    Uuid::new_v4()
                        .to_string()
                        .split('-')
                        .next()
                        .unwrap_or("rand")
                )
            });

        let cfg = parse_lambda_function_props(props)?;
        let function_arn = format!(
            "arn:aws:lambda:{}:{}:function:{}",
            self.region, self.account_id, function_name
        );

        // Resolve `Code.S3Bucket` + `Code.S3Key` against the in-process S3 state
        // so a stack that uploads code via `AWS::S3::Bucket` + `AWS::S3::Object`
        // (or any S3 PutObject before CreateStack) hydrates `code_zip` from the
        // real bytes. ZipFile (already decoded by parse_lambda_function_props)
        // wins over S3 if both are present, mirroring AWS validation order.
        let code_zip = if cfg.code_zip.is_some() {
            cfg.code_zip.clone()
        } else if let (Some(bucket_name), Some(key)) = (&cfg.s3_bucket, &cfg.s3_key) {
            Some(self.read_s3_object_bytes(bucket_name, key).map_err(|e| {
                format!("Failed to read Code.S3Bucket={bucket_name} Code.S3Key={key}: {e}")
            })?)
        } else {
            None
        };

        // Hash the actual ZIP bytes when available; image-based functions
        // leave the sha empty (matching the lambda CreateFunction code path
        // when neither ZipFile nor S3 source is supplied).
        let (code_sha256, code_size) = match &code_zip {
            Some(bytes) => (sha256_b64(bytes), bytes.len() as i64),
            None => (String::new(), 0),
        };

        // Resolve attached layer ARNs to their current code_size so
        // GetFunctionConfiguration echoes the right size without a
        // second state lookup.
        let layers: Vec<AttachedLayer> = {
            let accounts = self.lambda_state.read();
            cfg.layers
                .iter()
                .map(|arn| AttachedLayer {
                    arn: arn.clone(),
                    code_size: layer_code_size(&accounts, arn),
                })
                .collect()
        };

        let func = fakecloud_lambda::LambdaFunction {
            function_name: function_name.clone(),
            function_arn: function_arn.clone(),
            runtime: cfg.runtime,
            role: cfg.role,
            handler: cfg.handler,
            description: cfg.description,
            timeout: cfg.timeout,
            memory_size: cfg.memory_size,
            code_sha256,
            code_size,
            version: "$LATEST".to_string(),
            last_modified: Utc::now(),
            tags: cfg.tags,
            environment: cfg.environment,
            architectures: cfg.architectures,
            package_type: cfg.package_type,
            code_zip,
            image_uri: cfg.image_uri,
            policy: None,
            layers,
            revision_id: Uuid::new_v4().to_string(),
            tracing_mode: cfg.tracing_mode,
            kms_key_arn: cfg.kms_key_arn,
            ephemeral_storage_size: cfg.ephemeral_storage_size,
            vpc_config: cfg.vpc_config,
            snap_start: cfg.snap_start,
            dead_letter_config_arn: cfg.dead_letter_config_arn,
            file_system_configs: cfg.file_system_configs,
            logging_config: cfg.logging_config,
            image_config: None,
            durable_config: None,
            signing_profile_version_arn: None,
            signing_job_arn: None,
            runtime_version_config: None,
            master_arn: None,
            state_reason: None,
            state_reason_code: None,
            last_update_status_reason: None,
            last_update_status_reason_code: None,
        };

        let mut accounts = self.lambda_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.functions.insert(function_name.clone(), func);

        // Capture every GetAtt-resolvable attribute eagerly. Output
        // resolution happens after `provision_stack_resources` returns
        // and only sees `StackResource.attributes`, so any attribute the
        // template's `Outputs` need must be in this map.
        Ok(ProvisionResult::new(function_name.clone())
            .with("Arn", function_arn)
            .with("FunctionName", function_name)
            .with("Version", "$LATEST"))
    }

    /// Apply a CFN template-driven update to an existing Lambda function.
    /// Mirrors `UpdateFunctionConfiguration` + `UpdateFunctionCode`:
    /// rewrite mutable configuration fields from the new template, re-hash
    /// any new code bytes, bump `revision_id` and `last_modified`. The
    /// function's ARN, name, and `$LATEST` version stay put — those are
    /// the immutable identity of the resource as far as CloudFormation
    /// is concerned.
    fn update_lambda_function(
        &self,
        existing: &StackResource,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let function_name = existing.physical_id.clone();
        let cfg = parse_lambda_function_props(props)?;

        let new_code_zip = if cfg.code_zip.is_some() {
            cfg.code_zip.clone()
        } else if let (Some(bucket_name), Some(key)) = (&cfg.s3_bucket, &cfg.s3_key) {
            Some(self.read_s3_object_bytes(bucket_name, key).map_err(|e| {
                format!("Failed to read Code.S3Bucket={bucket_name} Code.S3Key={key}: {e}")
            })?)
        } else {
            None
        };

        let resolved_layers: Vec<AttachedLayer> = {
            let accounts = self.lambda_state.read();
            cfg.layers
                .iter()
                .map(|arn| AttachedLayer {
                    arn: arn.clone(),
                    code_size: layer_code_size(&accounts, arn),
                })
                .collect()
        };

        let mut accounts = self.lambda_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let func = state.functions.get_mut(&function_name).ok_or_else(|| {
            format!("Cannot update {function_name}: function does not exist in lambda state")
        })?;

        func.runtime = cfg.runtime;
        func.role = cfg.role;
        func.handler = cfg.handler;
        func.description = cfg.description;
        func.timeout = cfg.timeout;
        func.memory_size = cfg.memory_size;
        func.environment = cfg.environment;
        func.architectures = cfg.architectures;
        func.package_type = cfg.package_type;
        func.tracing_mode = cfg.tracing_mode;
        func.kms_key_arn = cfg.kms_key_arn;
        func.ephemeral_storage_size = cfg.ephemeral_storage_size;
        func.vpc_config = cfg.vpc_config;
        func.snap_start = cfg.snap_start;
        func.dead_letter_config_arn = cfg.dead_letter_config_arn;
        func.file_system_configs = cfg.file_system_configs;
        func.logging_config = cfg.logging_config;
        func.layers = resolved_layers;
        if cfg.image_uri.is_some() {
            func.image_uri = cfg.image_uri;
        }
        if !cfg.tags.is_empty() {
            func.tags = cfg.tags;
        }
        if let Some(bytes) = new_code_zip {
            func.code_sha256 = sha256_b64(&bytes);
            func.code_size = bytes.len() as i64;
            func.code_zip = Some(bytes);
        }
        func.last_modified = Utc::now();
        func.revision_id = Uuid::new_v4().to_string();

        let function_arn = func.function_arn.clone();
        Ok(ProvisionResult::new(function_name.clone())
            .with("Arn", function_arn)
            .with("FunctionName", function_name)
            .with("Version", "$LATEST"))
    }

    fn delete_lambda_function(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.lambda_state.write();
        let state = accounts.default_mut();
        state.functions.remove(physical_id);
        Ok(())
    }

    /// Look up an S3 object's bytes from the in-process S3 state. Used by
    /// the Lambda function provisioner to hydrate `Code.S3Bucket` /
    /// `Code.S3Key` references into real ZIP content. Returns an error
    /// string when the bucket or key is missing so the CFN error
    /// surfaces back to the caller.
    fn read_s3_object_bytes(&self, bucket: &str, key: &str) -> Result<Vec<u8>, String> {
        let mut accounts = self.s3_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let body_ref = {
            let b = state
                .buckets
                .get(bucket)
                .ok_or_else(|| format!("S3 bucket {bucket} does not exist"))?;
            let object = b
                .objects
                .get(key)
                .ok_or_else(|| format!("S3 object s3://{bucket}/{key} does not exist"))?;
            object.body.clone()
        };
        // `read_body` consults the body cache (which is owned by the state),
        // so re-borrow `state` after dropping the bucket borrow above.
        state
            .read_body(&body_ref)
            .map(|b| b.to_vec())
            .map_err(|e| format!("S3 read failed: {e}"))
    }

    fn create_lambda_permission(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let function_name = parse_lambda_function_name(
            props
                .get("FunctionName")
                .and_then(|v| v.as_str())
                .ok_or_else(|| "FunctionName is required".to_string())?,
        );
        // CFN does not surface a StatementId knob; synthesize one from
        // the logical id so subsequent updates / deletes can find the
        // statement again.
        let statement_id = format!(
            "cfn-{}-{}",
            resource.logical_id,
            &Uuid::new_v4().simple().to_string()[..8]
        );
        self.append_lambda_permission_statement(&function_name, &statement_id, props)?;

        // Encode `{function}|{sid}` so delete can target a single statement.
        let physical_id = format!("{function_name}|{statement_id}");
        Ok(ProvisionResult::new(physical_id).with("Id", statement_id))
    }

    /// Build a canonical Lambda resource-policy statement from a CFN
    /// `AWS::Lambda::Permission` `Properties` map and append it to the
    /// function's stored policy. Shared by create + update so the same
    /// property→condition mapping applies on both paths. Returns the
    /// function ARN of the target so callers can echo it back if needed.
    fn append_lambda_permission_statement(
        &self,
        function_name: &str,
        statement_id: &str,
        props: &serde_json::Value,
    ) -> Result<String, String> {
        let action = props
            .get("Action")
            .and_then(|v| v.as_str())
            .ok_or_else(|| "Action is required".to_string())?
            .to_string();
        let principal = props
            .get("Principal")
            .and_then(|v| v.as_str())
            .ok_or_else(|| "Principal is required".to_string())?
            .to_string();
        let source_arn = props
            .get("SourceArn")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let source_account = props
            .get("SourceAccount")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let event_source_token = props
            .get("EventSourceToken")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let function_url_auth_type = props
            .get("FunctionUrlAuthType")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let principal_org_id = props
            .get("PrincipalOrgID")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());

        let mut accounts = self.lambda_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let func = state.functions.get_mut(function_name).ok_or_else(|| {
            format!(
                "Function {function_name} does not exist yet — retry once it has been provisioned"
            )
        })?;

        let mut doc: serde_json::Value = func
            .policy
            .as_deref()
            .and_then(|s| serde_json::from_str::<serde_json::Value>(s).ok())
            .filter(|v| v.is_object())
            .unwrap_or_else(|| serde_json::json!({"Version": "2012-10-17", "Statement": []}));
        if !doc.get("Statement").map(|s| s.is_array()).unwrap_or(false) {
            doc["Statement"] = serde_json::json!([]);
        }
        let principal_value =
            if principal.ends_with(".amazonaws.com") || principal.contains(".amazon") {
                serde_json::json!({ "Service": principal })
            } else {
                serde_json::json!({ "AWS": principal })
            };
        let mut arn_like = serde_json::Map::new();
        let mut string_equals = serde_json::Map::new();
        if let Some(src) = source_arn {
            arn_like.insert("AWS:SourceArn".to_string(), serde_json::Value::String(src));
        }
        if let Some(acct) = source_account {
            string_equals.insert(
                "AWS:SourceAccount".to_string(),
                serde_json::Value::String(acct),
            );
        }
        if let Some(token) = event_source_token {
            string_equals.insert(
                "lambda:EventSourceToken".to_string(),
                serde_json::Value::String(token),
            );
        }
        if let Some(auth) = function_url_auth_type {
            string_equals.insert(
                "lambda:FunctionUrlAuthType".to_string(),
                serde_json::Value::String(auth),
            );
        }
        if let Some(org) = principal_org_id {
            string_equals.insert(
                "aws:PrincipalOrgID".to_string(),
                serde_json::Value::String(org),
            );
        }
        let mut conditions = serde_json::Map::new();
        if !arn_like.is_empty() {
            conditions.insert("ArnLike".to_string(), serde_json::Value::Object(arn_like));
        }
        if !string_equals.is_empty() {
            conditions.insert(
                "StringEquals".to_string(),
                serde_json::Value::Object(string_equals),
            );
        }

        let mut statement = serde_json::Map::new();
        statement.insert(
            "Sid".to_string(),
            serde_json::Value::String(statement_id.to_string()),
        );
        statement.insert(
            "Effect".to_string(),
            serde_json::Value::String("Allow".to_string()),
        );
        statement.insert("Principal".to_string(), principal_value);
        statement.insert("Action".to_string(), serde_json::Value::String(action));
        statement.insert(
            "Resource".to_string(),
            serde_json::Value::String(func.function_arn.clone()),
        );
        if !conditions.is_empty() {
            statement.insert(
                "Condition".to_string(),
                serde_json::Value::Object(conditions),
            );
        }
        doc["Statement"]
            .as_array_mut()
            .unwrap()
            .push(serde_json::Value::Object(statement));
        func.policy = Some(doc.to_string());
        Ok(func.function_arn.clone())
    }

    /// Replace the policy statement for an existing CFN-managed
    /// `AWS::Lambda::Permission`. CFN treats most property changes as
    /// in-place updates: the statement id stays put, the body is
    /// rewritten to reflect the new properties.
    fn update_lambda_permission(
        &self,
        existing: &StackResource,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let Some((function_name, statement_id)) = existing.physical_id.split_once('|') else {
            return Err(format!(
                "Permission physical id `{}` is malformed; expected `{{function}}|{{sid}}`",
                existing.physical_id
            ));
        };
        // First strip the prior statement so we don't end up with a duplicate
        // sid in the policy doc.
        {
            let mut accounts = self.lambda_state.write();
            let state = accounts.get_or_create(&self.account_id);
            if let Some(func) = state.functions.get_mut(function_name) {
                if let Some(policy_str) = func.policy.as_deref() {
                    if let Ok(mut doc) = serde_json::from_str::<serde_json::Value>(policy_str) {
                        if let Some(arr) = doc.get_mut("Statement").and_then(|v| v.as_array_mut()) {
                            arr.retain(|s| {
                                s.get("Sid").and_then(|v| v.as_str()) != Some(statement_id)
                            });
                            func.policy = Some(doc.to_string());
                        }
                    }
                }
            }
        }
        self.append_lambda_permission_statement(function_name, statement_id, &resource.properties)?;
        Ok(ProvisionResult::new(existing.physical_id.clone()).with("Id", statement_id.to_string()))
    }

    fn delete_lambda_permission(&self, physical_id: &str) -> Result<(), String> {
        let Some((function_name, sid)) = physical_id.split_once('|') else {
            return Ok(());
        };
        let mut accounts = self.lambda_state.write();
        let state = accounts.get_or_create(&self.account_id);
        if let Some(func) = state.functions.get_mut(function_name) {
            if let Some(policy_str) = func.policy.as_deref() {
                if let Ok(mut doc) = serde_json::from_str::<serde_json::Value>(policy_str) {
                    if let Some(arr) = doc.get_mut("Statement").and_then(|v| v.as_array_mut()) {
                        arr.retain(|s| s.get("Sid").and_then(|v| v.as_str()) != Some(sid));
                        func.policy = Some(doc.to_string());
                    }
                }
            }
        }
        Ok(())
    }

    fn create_lambda_event_source_mapping(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let function_name = parse_lambda_function_name(
            props
                .get("FunctionName")
                .and_then(|v| v.as_str())
                .ok_or_else(|| "FunctionName is required".to_string())?,
        );
        let cfg = parse_lambda_event_source_mapping_props(props)?;

        let mut accounts = self.lambda_state.write();
        let state = accounts.get_or_create(&self.account_id);
        if !state.functions.contains_key(&function_name) {
            return Err(format!(
                "Function {function_name} does not exist yet — retry once it has been provisioned"
            ));
        }
        let function_arn = format!(
            "arn:aws:lambda:{}:{}:function:{}",
            self.region, self.account_id, function_name
        );
        let uuid = Uuid::new_v4().to_string();
        let esm = EventSourceMapping {
            uuid: uuid.clone(),
            function_arn,
            event_source_arn: cfg.event_source_arn,
            batch_size: cfg.batch_size,
            enabled: cfg.enabled,
            state: if cfg.enabled {
                "Enabled".to_string()
            } else {
                "Disabled".to_string()
            },
            last_modified: Utc::now(),
            filter_patterns: cfg.filter_patterns,
            maximum_batching_window_in_seconds: cfg.maximum_batching_window_in_seconds,
            starting_position: cfg.starting_position,
            starting_position_timestamp: cfg.starting_position_timestamp,
            parallelization_factor: cfg.parallelization_factor,
            function_response_types: cfg.function_response_types,
            kms_key_arn: cfg.kms_key_arn,
            metrics_config: cfg.metrics_config,
            destination_config: cfg.destination_config,
            maximum_retry_attempts: cfg.maximum_retry_attempts,
            maximum_record_age_in_seconds: cfg.maximum_record_age_in_seconds,
            bisect_batch_on_function_error: cfg.bisect_batch_on_function_error,
            tumbling_window_in_seconds: cfg.tumbling_window_in_seconds,
            topics: cfg.topics,
            queues: cfg.queues,
        };
        state.event_source_mappings.insert(uuid.clone(), esm);
        Ok(ProvisionResult::new(uuid.clone()).with("Id", uuid))
    }

    /// In-place update of an existing EventSourceMapping. CFN treats
    /// every mutable property as in-place; the EventSourceArn /
    /// FunctionName pair stays put (those are immutable identity).
    fn update_lambda_event_source_mapping(
        &self,
        existing: &StackResource,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let cfg = parse_lambda_event_source_mapping_props(&resource.properties)?;
        let mut accounts = self.lambda_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let esm = state
            .event_source_mappings
            .get_mut(&existing.physical_id)
            .ok_or_else(|| {
                format!(
                    "EventSourceMapping {} does not exist in lambda state",
                    existing.physical_id
                )
            })?;
        esm.batch_size = cfg.batch_size;
        esm.enabled = cfg.enabled;
        esm.state = if cfg.enabled {
            "Enabled".to_string()
        } else {
            "Disabled".to_string()
        };
        esm.last_modified = Utc::now();
        esm.filter_patterns = cfg.filter_patterns;
        esm.maximum_batching_window_in_seconds = cfg.maximum_batching_window_in_seconds;
        esm.parallelization_factor = cfg.parallelization_factor;
        esm.function_response_types = cfg.function_response_types;
        esm.kms_key_arn = cfg.kms_key_arn;
        esm.metrics_config = cfg.metrics_config;
        esm.destination_config = cfg.destination_config;
        esm.maximum_retry_attempts = cfg.maximum_retry_attempts;
        esm.maximum_record_age_in_seconds = cfg.maximum_record_age_in_seconds;
        esm.bisect_batch_on_function_error = cfg.bisect_batch_on_function_error;
        esm.tumbling_window_in_seconds = cfg.tumbling_window_in_seconds;
        Ok(ProvisionResult::new(existing.physical_id.clone())
            .with("Id", existing.physical_id.clone()))
    }

    fn delete_lambda_event_source_mapping(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.lambda_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.event_source_mappings.remove(physical_id);
        Ok(())
    }

    fn create_lambda_layer_version(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let layer_name = props
            .get("LayerName")
            .and_then(|v| v.as_str())
            .unwrap_or(&resource.logical_id)
            .to_string();
        let description = props
            .get("Description")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let license_info = props
            .get("LicenseInfo")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let compatible_runtimes: Vec<String> = props
            .get("CompatibleRuntimes")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                    .collect()
            })
            .unwrap_or_default();
        let compatible_architectures: Vec<String> = props
            .get("CompatibleArchitectures")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                    .collect()
            })
            .unwrap_or_default();
        // CFN's `Content.ZipFile` rides as base64 (per the user guide).
        // `Content.S3Bucket` + `Content.S3Key` hydrate the same way the
        // Lambda function provisioner pulls Code.S3Bucket. Either path
        // is optional — fakecloud accepts a layer with zero-byte content
        // so templates that reference an upstream-managed layer ARN
        // still work.
        let content = props.get("Content");
        let zip_bytes = if let Some(b64) = content
            .and_then(|v| v.get("ZipFile"))
            .and_then(|v| v.as_str())
        {
            use base64::Engine;
            Some(
                base64::engine::general_purpose::STANDARD
                    .decode(b64)
                    .map_err(|e| format!("Content.ZipFile is not valid base64: {e}"))?,
            )
        } else if let (Some(bucket), Some(key)) = (
            content
                .and_then(|c| c.get("S3Bucket"))
                .and_then(|v| v.as_str()),
            content
                .and_then(|c| c.get("S3Key"))
                .and_then(|v| v.as_str()),
        ) {
            Some(self.read_s3_object_bytes(bucket, key).map_err(|e| {
                format!("Failed to read Content.S3Bucket={bucket} Content.S3Key={key}: {e}")
            })?)
        } else {
            None
        };

        let (code_sha256, code_size) = match zip_bytes.as_deref() {
            Some(bytes) => (sha256_b64(bytes), bytes.len() as i64),
            None => (String::new(), 0),
        };

        let mut accounts = self.lambda_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let layer_arn = format!(
            "arn:aws:lambda:{}:{}:layer:{}",
            self.region, self.account_id, layer_name
        );
        let layer = state
            .layers
            .entry(layer_name.clone())
            .or_insert_with(|| Layer {
                layer_name: layer_name.clone(),
                layer_arn: layer_arn.clone(),
                versions: Vec::new(),
            });
        let next_version = (layer.versions.len() as i64) + 1;
        let version_arn = format!("{}:{}", layer.layer_arn, next_version);
        layer.versions.push(LayerVersion {
            version: next_version,
            layer_version_arn: version_arn.clone(),
            description: description.clone(),
            created_date: Utc::now(),
            compatible_runtimes,
            license_info,
            policy: None,
            code_zip: zip_bytes,
            code_sha256,
            code_size,
            compatible_architectures,
        });
        Ok(ProvisionResult::new(version_arn.clone())
            .with("LayerVersionArn", version_arn)
            .with("LayerArn", layer_arn))
    }

    fn delete_lambda_layer_version(&self, physical_id: &str) -> Result<(), String> {
        // physical_id = `{layer_arn}:{version}` — strip trailing version.
        let Some(idx) = physical_id.rfind(':') else {
            return Ok(());
        };
        let (layer_arn, version_part) = physical_id.split_at(idx);
        let version_part = &version_part[1..];
        let Ok(version) = version_part.parse::<i64>() else {
            return Ok(());
        };
        // ARN form: arn:aws:lambda:<region>:<account>:layer:<name>
        let layer_name = layer_arn.rsplit(':').next().unwrap_or("").to_string();
        let mut accounts = self.lambda_state.write();
        let state = accounts.get_or_create(&self.account_id);
        if let Some(layer) = state.layers.get_mut(&layer_name) {
            layer.versions.retain(|v| v.version != version);
            if layer.versions.is_empty() {
                state.layers.remove(&layer_name);
            }
        }
        Ok(())
    }

    fn create_lambda_url(&self, resource: &ResourceDefinition) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let function_name = parse_lambda_function_name(
            props
                .get("TargetFunctionArn")
                .and_then(|v| v.as_str())
                .ok_or_else(|| "TargetFunctionArn is required".to_string())?,
        );
        // Optional qualifier — when set the URL points at an alias
        // (`{function_name}:{qualifier}`) rather than `$LATEST`. AWS
        // refuses to mint a URL for a numeric version, but the CFN
        // schema allows the property so we accept and round-trip it.
        let qualifier = props
            .get("Qualifier")
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string());
        let auth_type = props
            .get("AuthType")
            .and_then(|v| v.as_str())
            .unwrap_or("NONE")
            .to_string();
        let invoke_mode = props
            .get("InvokeMode")
            .and_then(|v| v.as_str())
            .unwrap_or("BUFFERED")
            .to_string();
        let cors = props.get("Cors").cloned();

        let mut accounts = self.lambda_state.write();
        let state = accounts.get_or_create(&self.account_id);
        if !state.functions.contains_key(&function_name) {
            return Err(format!(
                "Function {function_name} does not exist yet — retry once it has been provisioned"
            ));
        }
        let function_arn = match &qualifier {
            Some(q) => format!(
                "arn:aws:lambda:{}:{}:function:{}:{}",
                self.region, self.account_id, function_name, q
            ),
            None => format!(
                "arn:aws:lambda:{}:{}:function:{}",
                self.region, self.account_id, function_name
            ),
        };
        let function_url = format!("https://{function_name}.lambda-url.{}.on.aws/", self.region);
        let now = Utc::now();
        let cfg = FunctionUrlConfig {
            function_arn: function_arn.clone(),
            function_url: function_url.clone(),
            auth_type,
            cors,
            creation_time: now,
            last_modified_time: now,
            invoke_mode,
        };
        // Key by `{function}:{qualifier}` when qualifier is set so
        // delete and update can round-trip the same shape, while leaving
        // the bare-function-name URL alone for $LATEST.
        let key = match &qualifier {
            Some(q) => format!("{function_name}:{q}"),
            None => function_name.clone(),
        };
        state.function_url_configs.insert(key.clone(), cfg);

        Ok(ProvisionResult::new(key)
            .with("FunctionArn", function_arn)
            .with("FunctionUrl", function_url))
    }

    /// Update an existing Lambda Url config in place. AuthType, Cors,
    /// and InvokeMode are mutable; the target function and qualifier
    /// (which collectively form the key) are not — CFN replaces the
    /// resource if those change, so we only touch the body here.
    fn update_lambda_url(
        &self,
        existing: &StackResource,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let auth_type = props
            .get("AuthType")
            .and_then(|v| v.as_str())
            .unwrap_or("NONE")
            .to_string();
        let invoke_mode = props
            .get("InvokeMode")
            .and_then(|v| v.as_str())
            .unwrap_or("BUFFERED")
            .to_string();
        let cors = props.get("Cors").cloned();

        let mut accounts = self.lambda_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let cfg = state
            .function_url_configs
            .get_mut(&existing.physical_id)
            .ok_or_else(|| {
                format!(
                    "FunctionUrlConfig {} does not exist in lambda state",
                    existing.physical_id
                )
            })?;
        cfg.auth_type = auth_type;
        cfg.invoke_mode = invoke_mode;
        cfg.cors = cors;
        cfg.last_modified_time = Utc::now();
        let function_arn = cfg.function_arn.clone();
        let function_url = cfg.function_url.clone();
        Ok(ProvisionResult::new(existing.physical_id.clone())
            .with("FunctionArn", function_arn)
            .with("FunctionUrl", function_url))
    }

    fn delete_lambda_url(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.lambda_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.function_url_configs.remove(physical_id);
        Ok(())
    }

    fn create_lambda_alias(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let function_name = parse_lambda_function_name(
            props
                .get("FunctionName")
                .and_then(|v| v.as_str())
                .ok_or_else(|| "FunctionName is required".to_string())?,
        );
        let alias_name = props
            .get("Name")
            .and_then(|v| v.as_str())
            .ok_or_else(|| "Name is required".to_string())?
            .to_string();
        let function_version = props
            .get("FunctionVersion")
            .and_then(|v| v.as_str())
            .unwrap_or("$LATEST")
            .to_string();
        let description = props
            .get("Description")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let routing_config = props.get("RoutingConfig").cloned();

        let mut accounts = self.lambda_state.write();
        let state = accounts.get_or_create(&self.account_id);
        if !state.functions.contains_key(&function_name) {
            return Err(format!(
                "Function {function_name} does not exist yet — retry once it has been provisioned"
            ));
        }
        let alias_arn = format!(
            "arn:aws:lambda:{}:{}:function:{}:{}",
            self.region, self.account_id, function_name, alias_name
        );
        let key = format!("{function_name}:{alias_name}");
        state.aliases.insert(
            key.clone(),
            FunctionAlias {
                alias_arn: alias_arn.clone(),
                name: alias_name,
                function_version,
                description,
                revision_id: Uuid::new_v4().to_string(),
                routing_config,
            },
        );
        // ProvisionedConcurrencyConfig — CFN allows attaching a target
        // concurrency at the same time as the alias is created. Mirror
        // the runtime PutProvisionedConcurrencyConfig path: stamp a
        // `ProvisionedConcurrencyConfig` entry keyed by `{function}:{alias}`
        // with a `READY` status and the requested count fully allocated.
        if let Some(cnt) = props
            .get("ProvisionedConcurrencyConfig")
            .and_then(|v| v.get("ProvisionedConcurrentExecutions"))
            .and_then(|v| v.as_i64())
        {
            state.provisioned_concurrency.insert(
                key.clone(),
                fakecloud_lambda::ProvisionedConcurrencyConfig {
                    requested: cnt,
                    allocated: cnt,
                    status: "READY".to_string(),
                    last_modified: Utc::now(),
                },
            );
        }
        Ok(ProvisionResult::new(key).with("AliasArn", alias_arn))
    }

    /// Update an existing Lambda Alias in place. FunctionVersion,
    /// Description, RoutingConfig, and ProvisionedConcurrencyConfig are
    /// the mutable fields — Name and FunctionName are immutable, so a
    /// change there is a replacement (the parent CFN engine handles
    /// that path; this fn only sees the in-place case).
    fn update_lambda_alias(
        &self,
        existing: &StackResource,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let function_version = props
            .get("FunctionVersion")
            .and_then(|v| v.as_str())
            .unwrap_or("$LATEST")
            .to_string();
        let description = props
            .get("Description")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let routing_config = props.get("RoutingConfig").cloned();

        let mut accounts = self.lambda_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let alias = state
            .aliases
            .get_mut(&existing.physical_id)
            .ok_or_else(|| {
                format!(
                    "Alias {} does not exist in lambda state",
                    existing.physical_id
                )
            })?;
        alias.function_version = function_version;
        alias.description = description;
        alias.routing_config = routing_config;
        alias.revision_id = Uuid::new_v4().to_string();
        let alias_arn = alias.alias_arn.clone();

        // Re-stamp provisioned concurrency to match the new shape; remove
        // when the property is dropped from the template.
        match props
            .get("ProvisionedConcurrencyConfig")
            .and_then(|v| v.get("ProvisionedConcurrentExecutions"))
            .and_then(|v| v.as_i64())
        {
            Some(cnt) => {
                state.provisioned_concurrency.insert(
                    existing.physical_id.clone(),
                    fakecloud_lambda::ProvisionedConcurrencyConfig {
                        requested: cnt,
                        allocated: cnt,
                        status: "READY".to_string(),
                        last_modified: Utc::now(),
                    },
                );
            }
            None => {
                state.provisioned_concurrency.remove(&existing.physical_id);
            }
        }
        Ok(ProvisionResult::new(existing.physical_id.clone()).with("AliasArn", alias_arn))
    }

    fn delete_lambda_alias(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.lambda_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.aliases.remove(physical_id);
        Ok(())
    }

    fn create_lambda_version(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let function_name = parse_lambda_function_name(
            props
                .get("FunctionName")
                .and_then(|v| v.as_str())
                .ok_or_else(|| "FunctionName is required".to_string())?,
        );
        // CFN's `Description` overrides the parent function's
        // description on this immutable snapshot; AWS does the same.
        // `CodeSha256`, when set, gates publish — AWS rejects publish if
        // the current `$LATEST` sha doesn't match the supplied value.
        let description_override = props
            .get("Description")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let expected_sha = props
            .get("CodeSha256")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());

        let mut accounts = self.lambda_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let func = state
            .functions
            .get(&function_name)
            .ok_or_else(|| format!("Function {function_name} does not exist yet — retry once it has been provisioned"))?
            .clone();
        if let Some(expected) = &expected_sha {
            if !expected.is_empty() && expected != &func.code_sha256 {
                return Err(format!(
                    "PreconditionFailed: CodeSha256 mismatch on {function_name} — expected {expected}, $LATEST is {actual}",
                    actual = func.code_sha256,
                ));
            }
        }
        let versions = state
            .function_versions
            .entry(function_name.clone())
            .or_default();
        let next_version = (versions.len() as i64 + 1).to_string();
        versions.push(next_version.clone());
        // Snapshot current function config under this version with the
        // CFN-supplied description override (if any).
        let mut snapshot = func.clone();
        snapshot.version = next_version.clone();
        if let Some(desc) = description_override {
            snapshot.description = desc;
        }
        state
            .function_version_snapshots
            .entry(function_name.clone())
            .or_default()
            .insert(next_version.clone(), snapshot);
        let version_arn = format!(
            "arn:aws:lambda:{}:{}:function:{}:{}",
            self.region, self.account_id, function_name, next_version
        );
        let physical_id = format!("{function_name}:{next_version}");
        Ok(ProvisionResult::new(physical_id)
            .with("Version", next_version)
            .with("FunctionArn", version_arn))
    }

    /// Update an existing Lambda Version. CFN treats numbered versions
    /// as immutable — any property change forces replacement (a new
    /// version number) at the engine level, so this fn just no-ops and
    /// echoes the existing physical id back. Keeping it wired keeps the
    /// update dispatch table consistent with the other 5 aux types.
    fn update_lambda_version(
        &self,
        existing: &StackResource,
        _resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let mut accounts = self.lambda_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let Some((function_name, version)) = existing.physical_id.split_once(':') else {
            return Err(format!(
                "Version physical id `{}` is malformed; expected `{{function}}:{{version}}`",
                existing.physical_id
            ));
        };
        let exists = state
            .function_version_snapshots
            .get(function_name)
            .map(|m| m.contains_key(version))
            .unwrap_or(false);
        if !exists {
            return Err(format!(
                "Version {version} for function {function_name} no longer exists in lambda state"
            ));
        }
        let version_arn = format!(
            "arn:aws:lambda:{}:{}:function:{}:{}",
            self.region, self.account_id, function_name, version
        );
        Ok(ProvisionResult::new(existing.physical_id.clone())
            .with("Version", version.to_string())
            .with("FunctionArn", version_arn))
    }

    /// Update an existing LayerVersion. CFN treats LayerVersion as
    /// immutable (Content / CompatibleRuntimes / etc are all fixed once
    /// published), so a property change forces a new version. This fn
    /// no-ops to keep the dispatch table symmetric with the other aux
    /// types.
    fn update_lambda_layer_version(
        &self,
        existing: &StackResource,
        _resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let arn = existing.physical_id.clone();
        // ARN form: arn:aws:lambda:<region>:<account>:layer:<name>:<version>
        let layer_arn_only = arn
            .rsplit_once(':')
            .map(|(prefix, _)| prefix.to_string())
            .unwrap_or_else(|| arn.clone());
        Ok(ProvisionResult::new(existing.physical_id.clone())
            .with("LayerVersionArn", arn)
            .with("LayerArn", layer_arn_only))
    }

    fn delete_lambda_version(&self, physical_id: &str) -> Result<(), String> {
        let Some((function_name, version)) = physical_id.split_once(':') else {
            return Ok(());
        };
        let mut accounts = self.lambda_state.write();
        let state = accounts.get_or_create(&self.account_id);
        if let Some(versions) = state.function_versions.get_mut(function_name) {
            versions.retain(|v| v != version);
        }
        if let Some(snapshots) = state.function_version_snapshots.get_mut(function_name) {
            snapshots.remove(version);
        }
        Ok(())
    }

    // --- SecretsManager ---

    fn create_secrets_manager_secret(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let name = props
            .get("Name")
            .and_then(|v| v.as_str())
            .unwrap_or(&resource.logical_id)
            .to_string();
        let description = props
            .get("Description")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let kms_key_id = props
            .get("KmsKeyId")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());

        let mut accounts = self.secretsmanager_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let arn = format!(
            "arn:aws:secretsmanager:{}:{}:secret:{}",
            state.region, state.account_id, name
        );

        if state.secrets.contains_key(&arn) {
            return Err(format!("Secret {name} already exists"));
        }

        let now = Utc::now();
        let mut versions = BTreeMap::new();
        let mut current_version_id: Option<String> = None;
        let initial_string: Option<String> =
            if let Some(secret_string) = props.get("SecretString").and_then(|v| v.as_str()) {
                Some(secret_string.to_string())
            } else if let Some(gen) = props.get("GenerateSecretString") {
                Some(generate_secret_string_payload(gen)?)
            } else {
                None
            };
        if let Some(secret_string) = initial_string {
            let version_id = Uuid::new_v4().to_string();
            versions.insert(
                version_id.clone(),
                SecretVersion {
                    version_id: version_id.clone(),
                    secret_string: Some(secret_string),
                    secret_binary: None,
                    stages: vec!["AWSCURRENT".to_string()],
                    created_at: now,
                },
            );
            current_version_id = Some(version_id);
        }

        let mut tags: Vec<(String, String)> = Vec::new();
        if let Some(arr) = props.get("Tags").and_then(|v| v.as_array()) {
            for t in arr {
                if let (Some(k), Some(v)) = (
                    t.get("Key").and_then(|x| x.as_str()),
                    t.get("Value").and_then(|x| x.as_str()),
                ) {
                    tags.push((k.to_string(), v.to_string()));
                }
            }
        }
        let tags_set = !tags.is_empty();

        let secret = Secret {
            name: name.clone(),
            arn: arn.clone(),
            description,
            kms_key_id,
            versions,
            current_version_id,
            tags,
            tags_ever_set: tags_set,
            deleted: false,
            deletion_date: None,
            created_at: now,
            last_changed_at: now,
            last_accessed_at: None,
            rotation_enabled: None,
            rotation_lambda_arn: None,
            rotation_rules: None,
            last_rotated_at: None,
            resource_policy: None,
        };
        state.secrets.insert(arn.clone(), secret);

        Ok(ProvisionResult::new(arn.clone())
            .with("Id", arn.clone())
            .with("Name", name))
    }

    fn delete_secrets_manager_secret(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.secretsmanager_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.secrets.remove(physical_id);
        Ok(())
    }

    // --- Kinesis ---

    fn create_kinesis_stream(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let stream_name = props
            .get("Name")
            .and_then(|v| v.as_str())
            .unwrap_or(&resource.logical_id)
            .to_string();
        let shard_count = props
            .get("ShardCount")
            .and_then(|v| v.as_i64())
            .unwrap_or(1) as i32;
        if shard_count <= 0 {
            return Err("ShardCount must be greater than zero".to_string());
        }
        let stream_mode = props
            .get("StreamModeDetails")
            .and_then(|v| v.get("StreamMode"))
            .and_then(|v| v.as_str())
            .unwrap_or("PROVISIONED")
            .to_string();
        let retention_period_hours = props
            .get("RetentionPeriodHours")
            .and_then(|v| v.as_i64())
            .unwrap_or(24) as i32;

        let mut accounts = self.kinesis_state.write();
        let state = accounts.get_or_create(&self.account_id);
        if state.streams.contains_key(&stream_name) {
            return Err(format!("Stream {stream_name} already exists"));
        }
        let stream_arn = format!(
            "arn:aws:kinesis:{}:{}:stream/{}",
            state.region, state.account_id, stream_name
        );
        let stream = KinesisStream {
            stream_name: stream_name.clone(),
            stream_arn: stream_arn.clone(),
            stream_status: "ACTIVE".to_string(),
            stream_creation_timestamp: Utc::now(),
            retention_period_hours,
            stream_mode,
            encryption_type: "NONE".to_string(),
            key_id: None,
            shard_count,
            open_shard_count: shard_count,
            tags: BTreeMap::new(),
            shards: build_stream_shards(shard_count),
            next_shard_index: shard_count,
            enhanced_metrics: Vec::new(),
            warm_throughput_mibps: None,
            max_record_size_kib: None,
        };
        state.streams.insert(stream_name.clone(), stream);

        Ok(ProvisionResult::new(stream_name).with("Arn", stream_arn))
    }

    fn delete_kinesis_stream(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.kinesis_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.streams.remove(physical_id);
        Ok(())
    }

    fn create_kinesis_stream_consumer(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let stream_arn = props
            .get("StreamARN")
            .and_then(|v| v.as_str())
            .ok_or_else(|| "StreamARN is required".to_string())?
            .to_string();
        let consumer_name = props
            .get("ConsumerName")
            .and_then(|v| v.as_str())
            .ok_or_else(|| "ConsumerName is required".to_string())?
            .to_string();

        let mut accounts = self.kinesis_state.write();
        let state = accounts.get_or_create(&self.account_id);
        if state
            .consumers
            .values()
            .any(|c| c.stream_arn == stream_arn && c.consumer_name == consumer_name)
        {
            return Err(format!(
                "Consumer {consumer_name} already exists on stream {stream_arn}"
            ));
        }
        let now = Utc::now();
        let consumer_arn = format!(
            "{}/consumer/{}:{}",
            stream_arn,
            consumer_name,
            now.timestamp()
        );
        let consumer = KinesisConsumer {
            consumer_name: consumer_name.clone(),
            consumer_arn: consumer_arn.clone(),
            consumer_status: "ACTIVE".to_string(),
            consumer_creation_timestamp: now,
            stream_arn: stream_arn.clone(),
        };
        state.consumers.insert(consumer_arn.clone(), consumer);

        Ok(ProvisionResult::new(consumer_arn.clone())
            .with("ConsumerARN", consumer_arn)
            .with("ConsumerName", consumer_name)
            .with("ConsumerStatus", "ACTIVE")
            .with("ConsumerCreationTimestamp", now.timestamp().to_string())
            .with("StreamARN", stream_arn))
    }

    fn delete_kinesis_stream_consumer(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.kinesis_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.consumers.remove(physical_id);
        Ok(())
    }

    // --- KMS ---

    fn create_kms_key(&self, resource: &ResourceDefinition) -> Result<ProvisionResult, String> {
        let input = parse_kms_key_input(&resource.properties);
        let (key_id, arn) =
            kms_provisioner::provision_key(&self.kms_state, &self.account_id, &input)?;
        Ok(ProvisionResult::new(key_id.clone())
            .with("Arn", arn)
            .with("KeyId", key_id))
    }

    /// Update an `AWS::KMS::Key` in place. Properties that AWS treats
    /// as immutable (`KeySpec`, `KeyUsage`, `Origin`, `MultiRegion`)
    /// trigger replacement: when the diff carries one of those, this
    /// returns an error and the upstream stack engine recreates the
    /// resource.
    fn update_kms_key(
        &self,
        existing: &StackResource,
        new_def: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let new_input = parse_kms_key_input(&new_def.properties);
        let arn = {
            let mut accounts = self.kms_state.write();
            let state = accounts.get_or_create(&self.account_id);
            let key = state
                .keys
                .get(&existing.physical_id)
                .ok_or_else(|| format!("Key '{}' does not exist", existing.physical_id))?;
            if key.key_spec != new_input.key_spec
                || key.key_usage != new_input.key_usage
                || key.origin != new_input.origin
                || key.multi_region != new_input.multi_region
            {
                return Err(
                    "AWS::KMS::Key updates that change KeySpec, KeyUsage, Origin, or MultiRegion require replacement"
                        .to_string(),
                );
            }
            key.arn.clone()
        };
        kms_provisioner::update_key_properties(
            &self.kms_state,
            &self.account_id,
            &existing.physical_id,
            kms_provisioner::KeyUpdate {
                description: Some(new_input.description.clone()),
                enabled: Some(new_input.enabled),
                key_rotation_enabled: Some(new_input.key_rotation_enabled),
                policy: new_input.policy.clone(),
                tags: Some(new_input.tags.clone()),
            },
        )?;
        Ok(ProvisionResult::new(existing.physical_id.clone())
            .with("Arn", arn)
            .with("KeyId", existing.physical_id.clone()))
    }

    fn delete_kms_key(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.kms_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.keys.remove(physical_id);
        state.aliases.retain(|_, a| a.target_key_id != physical_id);
        Ok(())
    }

    /// Provision an `AWS::KMS::ReplicaKey`. Looks up the primary key by
    /// arn and inserts a region-keyed replica into the same account
    /// state, mirroring the ReplicateKey API contract.
    fn create_kms_replica_key(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let primary_arn = props
            .get("PrimaryKeyArn")
            .and_then(|v| v.as_str())
            .ok_or_else(|| "PrimaryKeyArn is required".to_string())?
            .to_string();
        let description = props
            .get("Description")
            .and_then(|v| v.as_str())
            .map(str::to_string);
        let enabled = props
            .get("Enabled")
            .and_then(|v| v.as_bool())
            .unwrap_or(true);
        let policy = parse_key_policy(props);
        let tags = parse_tag_list(props);

        let (replica_key_id, replica_arn) = kms_provisioner::provision_replica_key(
            &self.kms_state,
            &self.account_id,
            &primary_arn,
            description,
            enabled,
            policy,
            tags,
        )?;
        Ok(ProvisionResult::new(replica_key_id.clone())
            .with("KeyId", replica_key_id)
            .with("Arn", replica_arn))
    }

    /// Update an `AWS::KMS::ReplicaKey` in place. `PrimaryKeyArn`
    /// changes are replacement-only, like the AWS contract — we'd
    /// have to rebuild the replica from a different source. Other
    /// fields (description, enabled, policy, tags) flow through
    /// [`update_key_properties`].
    fn update_kms_replica_key(
        &self,
        existing: &StackResource,
        new_def: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &new_def.properties;
        let new_primary = props
            .get("PrimaryKeyArn")
            .and_then(|v| v.as_str())
            .ok_or_else(|| "PrimaryKeyArn is required".to_string())?;
        // Compare primary region from existing replica's primary_region
        // field. Replacement triggers when the primary key changes.
        {
            let mut accounts = self.kms_state.write();
            let state = accounts.get_or_create(&self.account_id);
            let key = state
                .keys
                .get(&existing.physical_id)
                .ok_or_else(|| format!("ReplicaKey '{}' does not exist", existing.physical_id))?;
            if let Some(existing_region) = key.primary_region.as_deref() {
                let parts: Vec<&str> = new_primary.split(':').collect();
                if parts.len() < 4 || parts[3] != existing_region {
                    return Err(
                        "AWS::KMS::ReplicaKey updates that change PrimaryKeyArn require replacement"
                            .to_string(),
                    );
                }
            }
        }
        let description = props
            .get("Description")
            .and_then(|v| v.as_str())
            .map(str::to_string);
        let enabled = props
            .get("Enabled")
            .and_then(|v| v.as_bool())
            .unwrap_or(true);
        let policy = parse_key_policy(props);
        let tags = parse_tag_list(props);
        kms_provisioner::update_key_properties(
            &self.kms_state,
            &self.account_id,
            &existing.physical_id,
            kms_provisioner::KeyUpdate {
                description,
                enabled: Some(enabled),
                key_rotation_enabled: None,
                policy,
                tags: Some(tags),
            },
        )?;
        let arn = {
            let mut accounts = self.kms_state.write();
            let state = accounts.get_or_create(&self.account_id);
            state
                .keys
                .get(&existing.physical_id)
                .map(|k| k.arn.clone())
                .unwrap_or_default()
        };
        Ok(ProvisionResult::new(existing.physical_id.clone())
            .with("KeyId", existing.physical_id.clone())
            .with("Arn", arn))
    }

    fn delete_kms_replica_key(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.kms_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.keys.remove(physical_id);
        Ok(())
    }

    fn create_kms_alias(&self, resource: &ResourceDefinition) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let alias_name = props
            .get("AliasName")
            .and_then(|v| v.as_str())
            .ok_or_else(|| "AliasName is required".to_string())?
            .to_string();
        let target_input = props
            .get("TargetKeyId")
            .and_then(|v| v.as_str())
            .ok_or_else(|| "TargetKeyId is required".to_string())?
            .to_string();
        let alias = kms_provisioner::provision_alias(
            &self.kms_state,
            &self.account_id,
            &alias_name,
            &target_input,
        )?;
        Ok(ProvisionResult::new(alias))
    }

    /// Update an `AWS::KMS::Alias` in place. Changing `AliasName`
    /// triggers replacement (the alias is the resource's identity);
    /// only `TargetKeyId` is updatable here.
    fn update_kms_alias(
        &self,
        existing: &StackResource,
        new_def: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &new_def.properties;
        let new_alias_name = props
            .get("AliasName")
            .and_then(|v| v.as_str())
            .ok_or_else(|| "AliasName is required".to_string())?;
        if new_alias_name != existing.physical_id {
            return Err(
                "AWS::KMS::Alias updates that change AliasName require replacement".to_string(),
            );
        }
        let target_input = props
            .get("TargetKeyId")
            .and_then(|v| v.as_str())
            .ok_or_else(|| "TargetKeyId is required".to_string())?;
        kms_provisioner::update_alias_target(
            &self.kms_state,
            &self.account_id,
            &existing.physical_id,
            target_input,
        )?;
        Ok(ProvisionResult::new(existing.physical_id.clone()))
    }

    fn delete_kms_alias(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.kms_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.aliases.remove(physical_id);
        Ok(())
    }

    // --- ECR ---

    fn create_ecr_repository(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let repository_name = props
            .get("RepositoryName")
            .and_then(|v| v.as_str())
            .unwrap_or(&resource.logical_id)
            .to_string();
        let image_tag_mutability = props
            .get("ImageTagMutability")
            .and_then(|v| v.as_str())
            .unwrap_or("MUTABLE")
            .to_string();
        let scan_on_push = props
            .get("ImageScanningConfiguration")
            .and_then(|v| v.get("ScanOnPush"))
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let encryption_type = props
            .get("EncryptionConfiguration")
            .and_then(|v| v.get("EncryptionType"))
            .and_then(|v| v.as_str())
            .unwrap_or("AES256")
            .to_string();
        let kms_key = props
            .get("EncryptionConfiguration")
            .and_then(|v| v.get("KmsKey"))
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let policy_text = props
            .get("RepositoryPolicyText")
            .map(|v| {
                if v.is_string() {
                    v.as_str().unwrap_or("").to_string()
                } else {
                    serde_json::to_string(v).unwrap_or_default()
                }
            })
            .filter(|s| !s.is_empty());
        let lifecycle_policy = props
            .get("LifecyclePolicy")
            .and_then(|v| v.get("LifecyclePolicyText"))
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let mut tags: BTreeMap<String, String> = BTreeMap::new();
        if let Some(arr) = props.get("Tags").and_then(|v| v.as_array()) {
            for t in arr {
                if let (Some(k), Some(v)) = (
                    t.get("Key").and_then(|x| x.as_str()),
                    t.get("Value").and_then(|x| x.as_str()),
                ) {
                    tags.insert(k.to_string(), v.to_string());
                }
            }
        }

        let empty_on_delete = props
            .get("EmptyOnDelete")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);

        let mut accounts = self.ecr_state.write();
        let state = accounts.get_or_create(&self.account_id);
        if state.repositories.contains_key(&repository_name) {
            return Err(format!("Repository {repository_name} already exists"));
        }
        let arn = state.repository_arn(&repository_name);
        let registry_id = state.account_id.clone();
        let endpoint = format!(
            "{}.dkr.ecr.{}.amazonaws.com",
            state.account_id, state.region
        );
        let mut repo = Repository::new(&repository_name, arn.clone(), &registry_id, &endpoint);
        repo.image_tag_mutability = image_tag_mutability;
        repo.image_scanning_configuration.scan_on_push = scan_on_push;
        repo.encryption_configuration.encryption_type = encryption_type;
        repo.encryption_configuration.kms_key = kms_key;
        repo.policy = policy_text;
        // Apply lifecycle policy synchronously so the store reflects the
        // initial prune, matching the PutLifecyclePolicy data path.
        if let Some(policy) = lifecycle_policy.as_ref() {
            let prune = fakecloud_ecr::evaluate_lifecycle_policy(&repo, policy);
            for digest in &prune {
                repo.images.remove(digest);
                repo.image_tags.retain(|_, d| d != digest);
            }
            repo.lifecycle_policy_last_evaluated_at = Some(Utc::now());
        }
        repo.lifecycle_policy = lifecycle_policy;
        repo.tags = tags;
        let uri = repo.repository_uri.clone();
        state.repositories.insert(repository_name.clone(), repo);

        Ok(ProvisionResult::new(repository_name)
            .with("Arn", arn)
            .with("RepositoryUri", uri)
            .with("RegistryId", registry_id)
            .with("EmptyOnDelete", empty_on_delete.to_string()))
    }

    fn delete_ecr_repository(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.ecr_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.repositories.remove(physical_id);
        Ok(())
    }

    /// Provision a standalone `AWS::ECR::RepositoryPolicy` against an
    /// existing repository. Physical id encodes `account/repo` so the
    /// delete path can find the right repository.
    fn create_ecr_repository_policy(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let repository_name = props
            .get("RepositoryName")
            .and_then(|v| v.as_str())
            .ok_or_else(|| "RepositoryName is required".to_string())?
            .to_string();
        let policy_text = props
            .get("PolicyText")
            .map(|v| {
                if v.is_string() {
                    v.as_str().unwrap_or("").to_string()
                } else {
                    serde_json::to_string(v).unwrap_or_default()
                }
            })
            .ok_or_else(|| "PolicyText is required".to_string())?;
        let mut accounts = self.ecr_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let repo = state
            .repositories
            .get_mut(&repository_name)
            .ok_or_else(|| format!("Repository {repository_name} does not exist"))?;
        repo.policy = Some(policy_text);
        Ok(ProvisionResult::new(format!(
            "{}/{}",
            self.account_id, repository_name
        )))
    }

    fn delete_ecr_repository_policy(&self, physical_id: &str) -> Result<(), String> {
        let repository_name = physical_id
            .split_once('/')
            .map(|(_, n)| n.to_string())
            .unwrap_or_else(|| physical_id.to_string());
        let mut accounts = self.ecr_state.write();
        let state = accounts.get_or_create(&self.account_id);
        if let Some(repo) = state.repositories.get_mut(&repository_name) {
            repo.policy = None;
        }
        Ok(())
    }

    /// Set the registry-wide policy for the active account. Physical id
    /// is just the account id since the registry is a singleton.
    fn create_ecr_registry_policy(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let policy_text = props
            .get("PolicyText")
            .map(|v| {
                if v.is_string() {
                    v.as_str().unwrap_or("").to_string()
                } else {
                    serde_json::to_string(v).unwrap_or_default()
                }
            })
            .ok_or_else(|| "PolicyText is required".to_string())?;
        let mut accounts = self.ecr_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.registry_policy = Some(policy_text);
        Ok(ProvisionResult::new(self.account_id.clone())
            .with("RegistryId", self.account_id.clone()))
    }

    fn delete_ecr_registry_policy(&self) -> Result<(), String> {
        let mut accounts = self.ecr_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.registry_policy = None;
        Ok(())
    }

    /// Provision the singleton `AWS::ECR::ReplicationConfiguration`.
    fn create_ecr_replication_configuration(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        use fakecloud_ecr::state::{
            ReplicationConfiguration, ReplicationDestination, ReplicationRule, RepositoryFilter,
        };
        let cfg = resource
            .properties
            .get("ReplicationConfiguration")
            .ok_or_else(|| "ReplicationConfiguration is required".to_string())?;
        let rules: Vec<ReplicationRule> = cfg
            .get("Rules")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .map(|r| {
                        let destinations: Vec<ReplicationDestination> = r
                            .get("Destinations")
                            .and_then(|v| v.as_array())
                            .map(|d| {
                                d.iter()
                                    .map(|dest| ReplicationDestination {
                                        region: dest
                                            .get("Region")
                                            .and_then(|v| v.as_str())
                                            .unwrap_or_default()
                                            .to_string(),
                                        registry_id: dest
                                            .get("RegistryId")
                                            .and_then(|v| v.as_str())
                                            .unwrap_or_default()
                                            .to_string(),
                                    })
                                    .collect()
                            })
                            .unwrap_or_default();
                        let repository_filters: Vec<RepositoryFilter> = r
                            .get("RepositoryFilters")
                            .and_then(|v| v.as_array())
                            .map(|f| {
                                f.iter()
                                    .map(|f| RepositoryFilter {
                                        filter: f
                                            .get("Filter")
                                            .and_then(|v| v.as_str())
                                            .unwrap_or_default()
                                            .to_string(),
                                        filter_type: f
                                            .get("FilterType")
                                            .and_then(|v| v.as_str())
                                            .unwrap_or_default()
                                            .to_string(),
                                    })
                                    .collect()
                            })
                            .unwrap_or_default();
                        ReplicationRule {
                            destinations,
                            repository_filters,
                        }
                    })
                    .collect()
            })
            .unwrap_or_default();
        let mut accounts = self.ecr_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.replication_configuration = Some(ReplicationConfiguration { rules });
        Ok(ProvisionResult::new(self.account_id.clone()))
    }

    fn delete_ecr_replication_configuration(&self) -> Result<(), String> {
        let mut accounts = self.ecr_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.replication_configuration = None;
        Ok(())
    }

    fn create_ecr_pull_through_cache_rule(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        use fakecloud_ecr::state::PullThroughCacheRule;
        let props = &resource.properties;
        let prefix = props
            .get("EcrRepositoryPrefix")
            .and_then(|v| v.as_str())
            .ok_or_else(|| "EcrRepositoryPrefix is required".to_string())?
            .to_string();
        let upstream_url = props
            .get("UpstreamRegistryUrl")
            .and_then(|v| v.as_str())
            .ok_or_else(|| "UpstreamRegistryUrl is required".to_string())?
            .to_string();
        let upstream_registry = props
            .get("UpstreamRegistry")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let credential_arn = props
            .get("CredentialArn")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let custom_role_arn = props
            .get("CustomRoleArn")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let now = Utc::now();
        let rule = PullThroughCacheRule {
            ecr_repository_prefix: prefix.clone(),
            upstream_registry_url: upstream_url,
            upstream_registry,
            credential_arn,
            created_at: now,
            updated_at: now,
            custom_role_arn,
        };
        let mut accounts = self.ecr_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.pull_through_cache_rules.insert(prefix.clone(), rule);
        Ok(ProvisionResult::new(prefix))
    }

    fn delete_ecr_pull_through_cache_rule(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.ecr_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.pull_through_cache_rules.remove(physical_id);
        Ok(())
    }

    /// Provision a standalone `AWS::ECR::LifecyclePolicy` against an
    /// existing repository. Mirrors the `PutLifecyclePolicy` data path:
    /// the policy text is stored on the repo and applied synchronously
    /// so the store reflects any prune set immediately. Physical id
    /// encodes `account/repo` so delete can find the right repository.
    fn create_ecr_lifecycle_policy(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let repository_name = props
            .get("RepositoryName")
            .and_then(|v| v.as_str())
            .ok_or_else(|| "RepositoryName is required".to_string())?
            .to_string();
        let policy_text = props
            .get("LifecyclePolicyText")
            .map(|v| {
                if v.is_string() {
                    v.as_str().unwrap_or("").to_string()
                } else {
                    serde_json::to_string(v).unwrap_or_default()
                }
            })
            .ok_or_else(|| "LifecyclePolicyText is required".to_string())?;
        // RegistryId is informational on AWS — accepted but the registry
        // lookup is always the active account in fakecloud.
        let _registry_id = props
            .get("RegistryId")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let mut accounts = self.ecr_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let repo = state
            .repositories
            .get_mut(&repository_name)
            .ok_or_else(|| format!("Repository {repository_name} does not exist"))?;
        // Run the policy once now so callers see the same prune-on-write
        // behaviour they get from the JSON `PutLifecyclePolicy` endpoint.
        let prune = fakecloud_ecr::evaluate_lifecycle_policy(repo, &policy_text);
        for digest in &prune {
            repo.images.remove(digest);
            repo.image_tags.retain(|_, d| d != digest);
        }
        repo.lifecycle_policy = Some(policy_text);
        repo.lifecycle_policy_last_evaluated_at = Some(Utc::now());
        let registry_id = repo.registry_id.clone();
        Ok(
            ProvisionResult::new(format!("{}/{}", self.account_id, repository_name))
                .with("RepositoryName", repository_name)
                .with("RegistryId", registry_id),
        )
    }

    fn delete_ecr_lifecycle_policy(&self, physical_id: &str) -> Result<(), String> {
        let repository_name = physical_id
            .split_once('/')
            .map(|(_, n)| n.to_string())
            .unwrap_or_else(|| physical_id.to_string());
        let mut accounts = self.ecr_state.write();
        let state = accounts.get_or_create(&self.account_id);
        if let Some(repo) = state.repositories.get_mut(&repository_name) {
            repo.lifecycle_policy = None;
            repo.lifecycle_policy_last_evaluated_at = None;
        }
        Ok(())
    }

    /// Provision the singleton `AWS::ECR::RegistryScanningConfiguration`.
    /// One per account; later applies overwrite the prior config —
    /// matching `PutRegistryScanningConfiguration` semantics.
    fn create_ecr_registry_scanning_configuration(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        use fakecloud_ecr::state::{
            RegistryScanningConfiguration, RegistryScanningRule, RepositoryFilter,
        };
        let props = &resource.properties;
        let scan_type = props
            .get("ScanType")
            .and_then(|v| v.as_str())
            .unwrap_or("BASIC")
            .to_string();
        let rules: Vec<RegistryScanningRule> = props
            .get("Rules")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .map(|r| {
                        let scan_frequency = r
                            .get("ScanFrequency")
                            .and_then(|v| v.as_str())
                            .unwrap_or("MANUAL")
                            .to_string();
                        let repository_filters: Vec<RepositoryFilter> = r
                            .get("RepositoryFilters")
                            .and_then(|v| v.as_array())
                            .map(|f| {
                                f.iter()
                                    .map(|f| RepositoryFilter {
                                        filter: f
                                            .get("Filter")
                                            .and_then(|v| v.as_str())
                                            .unwrap_or_default()
                                            .to_string(),
                                        filter_type: f
                                            .get("FilterType")
                                            .and_then(|v| v.as_str())
                                            .unwrap_or_default()
                                            .to_string(),
                                    })
                                    .collect()
                            })
                            .unwrap_or_default();
                        RegistryScanningRule {
                            scan_frequency,
                            repository_filters,
                        }
                    })
                    .collect()
            })
            .unwrap_or_default();
        let mut accounts = self.ecr_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.registry_scanning_configuration = RegistryScanningConfiguration { scan_type, rules };
        Ok(ProvisionResult::new(self.account_id.clone()))
    }

    fn delete_ecr_registry_scanning_configuration(&self) -> Result<(), String> {
        use fakecloud_ecr::state::RegistryScanningConfiguration;
        let mut accounts = self.ecr_state.write();
        let state = accounts.get_or_create(&self.account_id);
        // CFN delete reverts to the AWS default (BASIC, no rules).
        state.registry_scanning_configuration = RegistryScanningConfiguration::default();
        Ok(())
    }

    // ECR update_resource handlers. RepositoryName / EcrRepositoryPrefix
    // are immutable on AWS; CFN replaces the resource if they change.
    // These handlers refresh the mutable fields and keep the physical id
    // stable.

    fn update_ecr_repository(
        &self,
        existing: &StackResource,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let repository_name = existing.physical_id.clone();
        let mut accounts = self.ecr_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let repo = state
            .repositories
            .get_mut(&repository_name)
            .ok_or_else(|| format!("Repository {repository_name} no longer exists"))?;
        if let Some(s) = props.get("ImageTagMutability").and_then(|v| v.as_str()) {
            repo.image_tag_mutability = s.to_string();
        }
        if let Some(b) = props
            .get("ImageScanningConfiguration")
            .and_then(|v| v.get("ScanOnPush"))
            .and_then(|v| v.as_bool())
        {
            repo.image_scanning_configuration.scan_on_push = b;
        }
        if let Some(cfg) = props.get("EncryptionConfiguration") {
            if let Some(s) = cfg.get("EncryptionType").and_then(|v| v.as_str()) {
                repo.encryption_configuration.encryption_type = s.to_string();
            }
            if let Some(s) = cfg.get("KmsKey").and_then(|v| v.as_str()) {
                repo.encryption_configuration.kms_key = Some(s.to_string());
            }
        }
        if let Some(v) = props.get("RepositoryPolicyText") {
            let text = if v.is_string() {
                v.as_str().unwrap_or("").to_string()
            } else {
                serde_json::to_string(v).unwrap_or_default()
            };
            repo.policy = if text.is_empty() { None } else { Some(text) };
        }
        if let Some(text) = props
            .get("LifecyclePolicy")
            .and_then(|v| v.get("LifecyclePolicyText"))
            .and_then(|v| v.as_str())
        {
            let prune = fakecloud_ecr::evaluate_lifecycle_policy(repo, text);
            for digest in &prune {
                repo.images.remove(digest);
                repo.image_tags.retain(|_, d| d != digest);
            }
            repo.lifecycle_policy = Some(text.to_string());
            repo.lifecycle_policy_last_evaluated_at = Some(Utc::now());
        }
        if let Some(arr) = props.get("Tags").and_then(|v| v.as_array()) {
            let mut tags: BTreeMap<String, String> = BTreeMap::new();
            for t in arr {
                if let (Some(k), Some(v)) = (
                    t.get("Key").and_then(|x| x.as_str()),
                    t.get("Value").and_then(|x| x.as_str()),
                ) {
                    tags.insert(k.to_string(), v.to_string());
                }
            }
            repo.tags = tags;
        }
        let arn = repo.repository_arn.clone();
        let uri = repo.repository_uri.clone();
        let registry_id = repo.registry_id.clone();
        Ok(ProvisionResult::new(repository_name)
            .with("Arn", arn)
            .with("RepositoryUri", uri)
            .with("RegistryId", registry_id))
    }

    fn update_ecr_repository_policy(
        &self,
        existing: &StackResource,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let physical_id = existing.physical_id.clone();
        let repository_name = physical_id
            .split_once('/')
            .map(|(_, n)| n.to_string())
            .unwrap_or_else(|| physical_id.clone());
        let policy_text = props
            .get("PolicyText")
            .map(|v| {
                if v.is_string() {
                    v.as_str().unwrap_or("").to_string()
                } else {
                    serde_json::to_string(v).unwrap_or_default()
                }
            })
            .ok_or_else(|| "PolicyText is required".to_string())?;
        let mut accounts = self.ecr_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let repo = state
            .repositories
            .get_mut(&repository_name)
            .ok_or_else(|| format!("Repository {repository_name} does not exist"))?;
        repo.policy = Some(policy_text);
        Ok(ProvisionResult::new(physical_id))
    }

    fn update_ecr_lifecycle_policy(
        &self,
        existing: &StackResource,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let physical_id = existing.physical_id.clone();
        let repository_name = physical_id
            .split_once('/')
            .map(|(_, n)| n.to_string())
            .unwrap_or_else(|| physical_id.clone());
        let policy_text = props
            .get("LifecyclePolicyText")
            .map(|v| {
                if v.is_string() {
                    v.as_str().unwrap_or("").to_string()
                } else {
                    serde_json::to_string(v).unwrap_or_default()
                }
            })
            .ok_or_else(|| "LifecyclePolicyText is required".to_string())?;
        let mut accounts = self.ecr_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let repo = state
            .repositories
            .get_mut(&repository_name)
            .ok_or_else(|| format!("Repository {repository_name} does not exist"))?;
        let prune = fakecloud_ecr::evaluate_lifecycle_policy(repo, &policy_text);
        for digest in &prune {
            repo.images.remove(digest);
            repo.image_tags.retain(|_, d| d != digest);
        }
        repo.lifecycle_policy = Some(policy_text);
        repo.lifecycle_policy_last_evaluated_at = Some(Utc::now());
        let registry_id = repo.registry_id.clone();
        Ok(ProvisionResult::new(physical_id)
            .with("RepositoryName", repository_name)
            .with("RegistryId", registry_id))
    }

    fn update_ecr_registry_policy(
        &self,
        existing: &StackResource,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let policy_text = props
            .get("PolicyText")
            .map(|v| {
                if v.is_string() {
                    v.as_str().unwrap_or("").to_string()
                } else {
                    serde_json::to_string(v).unwrap_or_default()
                }
            })
            .ok_or_else(|| "PolicyText is required".to_string())?;
        let mut accounts = self.ecr_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.registry_policy = Some(policy_text);
        Ok(ProvisionResult::new(existing.physical_id.clone())
            .with("RegistryId", self.account_id.clone()))
    }

    fn update_ecr_replication_configuration(
        &self,
        existing: &StackResource,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        // Updates fully replace the prior config — same as create.
        let result = self.create_ecr_replication_configuration(resource)?;
        Ok(ProvisionResult::new(existing.physical_id.clone()).merge_attributes(result.attributes))
    }

    fn update_ecr_registry_scanning_configuration(
        &self,
        existing: &StackResource,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let result = self.create_ecr_registry_scanning_configuration(resource)?;
        Ok(ProvisionResult::new(existing.physical_id.clone()).merge_attributes(result.attributes))
    }

    fn update_ecr_pull_through_cache_rule(
        &self,
        existing: &StackResource,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let prefix = existing.physical_id.clone();
        let mut accounts = self.ecr_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let rule = state
            .pull_through_cache_rules
            .get_mut(&prefix)
            .ok_or_else(|| format!("PullThroughCacheRule {prefix} no longer exists"))?;
        if let Some(s) = props.get("UpstreamRegistryUrl").and_then(|v| v.as_str()) {
            rule.upstream_registry_url = s.to_string();
        }
        if let Some(s) = props.get("UpstreamRegistry").and_then(|v| v.as_str()) {
            rule.upstream_registry = Some(s.to_string());
        }
        if let Some(s) = props.get("CredentialArn").and_then(|v| v.as_str()) {
            rule.credential_arn = Some(s.to_string());
        }
        if let Some(s) = props.get("CustomRoleArn").and_then(|v| v.as_str()) {
            rule.custom_role_arn = Some(s.to_string());
        }
        rule.updated_at = Utc::now();
        Ok(ProvisionResult::new(prefix))
    }

    fn get_att_ecr_repository(&self, physical_id: &str, attribute: &str) -> Option<String> {
        let mut accounts = self.ecr_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let repo = state.repositories.get(physical_id)?;
        match attribute {
            "Arn" => Some(repo.repository_arn.clone()),
            "RepositoryUri" => Some(repo.repository_uri.clone()),
            "RegistryId" => Some(repo.registry_id.clone()),
            _ => None,
        }
    }

    // --- CloudWatch ---

    fn create_cloudwatch_alarm(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let alarm_name = props
            .get("AlarmName")
            .and_then(|v| v.as_str())
            .unwrap_or(&resource.logical_id)
            .to_string();
        let alarm_description = props
            .get("AlarmDescription")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let actions_enabled = props
            .get("ActionsEnabled")
            .and_then(|v| v.as_bool())
            .unwrap_or(true);
        let str_array = |key: &str| -> Vec<String> {
            props
                .get(key)
                .and_then(|v| v.as_array())
                .map(|arr| {
                    arr.iter()
                        .filter_map(|x| x.as_str().map(|s| s.to_string()))
                        .collect()
                })
                .unwrap_or_default()
        };
        let alarm_actions = str_array("AlarmActions");
        let ok_actions = str_array("OKActions");
        let insufficient_data_actions = str_array("InsufficientDataActions");

        let metric_name = props
            .get("MetricName")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let namespace = props
            .get("Namespace")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let statistic = props
            .get("Statistic")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let extended_statistic = props
            .get("ExtendedStatistic")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let unit = props
            .get("Unit")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let period = props.get("Period").and_then(|v| v.as_i64());
        let evaluation_periods = props
            .get("EvaluationPeriods")
            .and_then(|v| v.as_i64())
            .unwrap_or(1);
        let datapoints_to_alarm = props.get("DatapointsToAlarm").and_then(|v| v.as_i64());
        let threshold = props.get("Threshold").and_then(|v| v.as_f64());
        let comparison_operator = props
            .get("ComparisonOperator")
            .and_then(|v| v.as_str())
            .unwrap_or("GreaterThanThreshold")
            .to_string();
        let treat_missing_data = props
            .get("TreatMissingData")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let evaluate_low_sample_count_percentile = props
            .get("EvaluateLowSampleCountPercentile")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());

        let mut dimensions: BTreeMap<String, String> = BTreeMap::new();
        if let Some(arr) = props.get("Dimensions").and_then(|v| v.as_array()) {
            for d in arr {
                if let (Some(k), Some(v)) = (
                    d.get("Name").and_then(|x| x.as_str()),
                    d.get("Value").and_then(|x| x.as_str()),
                ) {
                    dimensions.insert(k.to_string(), v.to_string());
                }
            }
        }

        let mut accounts = self.cloudwatch_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let alarm_arn = format!(
            "arn:aws:cloudwatch:{}:{}:alarm:{}",
            self.region, self.account_id, alarm_name
        );
        let now = Utc::now();
        let alarm = MetricAlarm {
            alarm_name: alarm_name.clone(),
            alarm_arn: alarm_arn.clone(),
            alarm_description,
            actions_enabled,
            ok_actions,
            alarm_actions,
            insufficient_data_actions,
            state_value: AlarmState::InsufficientData,
            state_reason: "Unchecked: Initial alarm creation".to_string(),
            state_updated_timestamp: now,
            metric_name,
            namespace,
            statistic,
            extended_statistic,
            dimensions,
            period,
            unit,
            evaluation_periods,
            datapoints_to_alarm,
            threshold,
            comparison_operator,
            treat_missing_data,
            evaluate_low_sample_count_percentile,
            configuration_updated_timestamp: now,
            alarm_configuration_updated_timestamp: now,
        };
        let region_alarms = state.alarms_in_mut(&self.region);
        if region_alarms.contains_key(&alarm_name) {
            return Err(format!("Alarm {alarm_name} already exists"));
        }
        region_alarms.insert(alarm_name.clone(), alarm);

        Ok(ProvisionResult::new(alarm_name).with("Arn", alarm_arn))
    }

    fn delete_cloudwatch_alarm(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.cloudwatch_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.alarms_in_mut(&self.region).remove(physical_id);
        Ok(())
    }

    fn create_cloudwatch_dashboard(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let dashboard_name = props
            .get("DashboardName")
            .and_then(|v| v.as_str())
            .map(String::from)
            .unwrap_or_else(|| {
                let suffix = Uuid::new_v4().simple().to_string();
                format!("{}-{}", resource.logical_id, &suffix[..8])
            });
        // CFN passes DashboardBody as a JSON string (Fn::Sub friendly).
        let body = props
            .get("DashboardBody")
            .ok_or("DashboardBody is required")?;
        let body_str = if let Some(s) = body.as_str() {
            s.to_string()
        } else {
            serde_json::to_string(body).map_err(|e| format!("invalid DashboardBody: {e}"))?
        };
        // Validate JSON syntax to mirror real PutDashboard behavior.
        serde_json::from_str::<serde_json::Value>(&body_str)
            .map_err(|e| format!("DashboardBody must be valid JSON: {e}"))?;

        let arn = format!(
            "arn:aws:cloudwatch::{}:dashboard/{dashboard_name}",
            self.account_id
        );
        let dashboard = Dashboard {
            name: dashboard_name.clone(),
            arn: arn.clone(),
            size_bytes: body_str.len() as i64,
            body: body_str,
            last_modified: Utc::now(),
        };

        let mut accounts = self.cloudwatch_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.dashboards.insert(dashboard_name.clone(), dashboard);

        Ok(ProvisionResult::new(dashboard_name).with("Arn", arn))
    }

    fn delete_cloudwatch_dashboard(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.cloudwatch_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.dashboards.remove(physical_id);
        Ok(())
    }

    fn update_cloudwatch_alarm(
        &self,
        existing: &StackResource,
        new_def: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &new_def.properties;
        // Renaming AlarmName forces replacement in real CFN; mirror that.
        let new_alarm_name = props
            .get("AlarmName")
            .and_then(|v| v.as_str())
            .unwrap_or(&new_def.logical_id);
        if new_alarm_name != existing.physical_id {
            return Err(
                "AWS::CloudWatch::Alarm updates that change AlarmName require replacement"
                    .to_string(),
            );
        }

        let str_array = |key: &str| -> Vec<String> {
            props
                .get(key)
                .and_then(|v| v.as_array())
                .map(|arr| {
                    arr.iter()
                        .filter_map(|x| x.as_str().map(|s| s.to_string()))
                        .collect()
                })
                .unwrap_or_default()
        };
        let alarm_description = props
            .get("AlarmDescription")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let actions_enabled = props
            .get("ActionsEnabled")
            .and_then(|v| v.as_bool())
            .unwrap_or(true);
        let alarm_actions = str_array("AlarmActions");
        let ok_actions = str_array("OKActions");
        let insufficient_data_actions = str_array("InsufficientDataActions");
        let metric_name = props
            .get("MetricName")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let namespace = props
            .get("Namespace")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let statistic = props
            .get("Statistic")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let extended_statistic = props
            .get("ExtendedStatistic")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let unit = props
            .get("Unit")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let period = props.get("Period").and_then(|v| v.as_i64());
        let evaluation_periods = props
            .get("EvaluationPeriods")
            .and_then(|v| v.as_i64())
            .unwrap_or(1);
        let datapoints_to_alarm = props.get("DatapointsToAlarm").and_then(|v| v.as_i64());
        let threshold = props.get("Threshold").and_then(|v| v.as_f64());
        let comparison_operator = props
            .get("ComparisonOperator")
            .and_then(|v| v.as_str())
            .unwrap_or("GreaterThanThreshold")
            .to_string();
        let treat_missing_data = props
            .get("TreatMissingData")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let evaluate_low_sample_count_percentile = props
            .get("EvaluateLowSampleCountPercentile")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());

        let mut dimensions: BTreeMap<String, String> = BTreeMap::new();
        if let Some(arr) = props.get("Dimensions").and_then(|v| v.as_array()) {
            for d in arr {
                if let (Some(k), Some(v)) = (
                    d.get("Name").and_then(|x| x.as_str()),
                    d.get("Value").and_then(|x| x.as_str()),
                ) {
                    dimensions.insert(k.to_string(), v.to_string());
                }
            }
        }

        let mut accounts = self.cloudwatch_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let region_alarms = state.alarms_in_mut(&self.region);
        let alarm = region_alarms
            .get_mut(&existing.physical_id)
            .ok_or_else(|| format!("Alarm {} not found", existing.physical_id))?;
        let now = Utc::now();
        alarm.alarm_description = alarm_description;
        alarm.actions_enabled = actions_enabled;
        alarm.ok_actions = ok_actions;
        alarm.alarm_actions = alarm_actions;
        alarm.insufficient_data_actions = insufficient_data_actions;
        alarm.metric_name = metric_name;
        alarm.namespace = namespace;
        alarm.statistic = statistic;
        alarm.extended_statistic = extended_statistic;
        alarm.dimensions = dimensions;
        alarm.period = period;
        alarm.unit = unit;
        alarm.evaluation_periods = evaluation_periods;
        alarm.datapoints_to_alarm = datapoints_to_alarm;
        alarm.threshold = threshold;
        alarm.comparison_operator = comparison_operator;
        alarm.treat_missing_data = treat_missing_data;
        alarm.evaluate_low_sample_count_percentile = evaluate_low_sample_count_percentile;
        alarm.configuration_updated_timestamp = now;
        alarm.alarm_configuration_updated_timestamp = now;

        let alarm_arn = alarm.alarm_arn.clone();
        Ok(ProvisionResult::new(existing.physical_id.clone()).with("Arn", alarm_arn))
    }

    fn update_cloudwatch_dashboard(
        &self,
        existing: &StackResource,
        new_def: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &new_def.properties;
        // Renaming DashboardName forces replacement.
        if let Some(new_name) = props.get("DashboardName").and_then(|v| v.as_str()) {
            if new_name != existing.physical_id {
                return Err(
                    "AWS::CloudWatch::Dashboard updates that change DashboardName require replacement"
                        .to_string(),
                );
            }
        }
        let body = props
            .get("DashboardBody")
            .ok_or("DashboardBody is required")?;
        let body_str = if let Some(s) = body.as_str() {
            s.to_string()
        } else {
            serde_json::to_string(body).map_err(|e| format!("invalid DashboardBody: {e}"))?
        };
        serde_json::from_str::<serde_json::Value>(&body_str)
            .map_err(|e| format!("DashboardBody must be valid JSON: {e}"))?;

        let mut accounts = self.cloudwatch_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let dashboard = state
            .dashboards
            .get_mut(&existing.physical_id)
            .ok_or_else(|| format!("Dashboard {} not found", existing.physical_id))?;
        dashboard.size_bytes = body_str.len() as i64;
        dashboard.body = body_str;
        dashboard.last_modified = Utc::now();
        let arn = dashboard.arn.clone();
        Ok(ProvisionResult::new(existing.physical_id.clone()).with("Arn", arn))
    }

    // --- ELBv2 ---

    fn create_elbv2_load_balancer(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let name = props
            .get("Name")
            .and_then(|v| v.as_str())
            .unwrap_or(&resource.logical_id)
            .to_string();
        let scheme = props
            .get("Scheme")
            .and_then(|v| v.as_str())
            .unwrap_or("internet-facing")
            .to_string();
        let lb_type = props
            .get("Type")
            .and_then(|v| v.as_str())
            .unwrap_or("application")
            .to_string();
        let ip_address_type = props
            .get("IpAddressType")
            .and_then(|v| v.as_str())
            .unwrap_or("ipv4")
            .to_string();
        let security_groups: Vec<String> = props
            .get("SecurityGroups")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|s| s.as_str().map(|s| s.to_string()))
                    .collect()
            })
            .unwrap_or_default();
        let tags = parse_elb_tags(props.get("Tags"));

        let mut accounts = self.elbv2_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let lb_id = Uuid::new_v4().simple().to_string();
        let arn = format!(
            "arn:aws:elasticloadbalancing:{}:{}:loadbalancer/{}/{}/{}",
            self.region,
            self.account_id,
            if lb_type == "network" { "net" } else { "app" },
            name,
            &lb_id[..16]
        );
        let dns_name = format!(
            "{}-{}.{}.elb.{}.amazonaws.com",
            name,
            &lb_id[..16],
            self.region,
            self.region
        );

        let mut availability_zones: Vec<fakecloud_elbv2::AvailabilityZone> = Vec::new();
        if let Some(arr) = props.get("Subnets").and_then(|v| v.as_array()) {
            for s in arr {
                if let Some(subnet_id) = s.as_str() {
                    availability_zones.push(fakecloud_elbv2::AvailabilityZone {
                        zone_name: format!("{}a", self.region),
                        subnet_id: subnet_id.to_string(),
                        outpost_id: None,
                        load_balancer_addresses: Vec::new(),
                        source_nat_ipv6_prefixes: Vec::new(),
                    });
                }
            }
        }

        state.load_balancers.insert(
            arn.clone(),
            LoadBalancer {
                arn: arn.clone(),
                name: name.clone(),
                dns_name: dns_name.clone(),
                canonical_hosted_zone_id: "Z2P70J7EXAMPLE".to_string(),
                created_time: Utc::now(),
                scheme,
                vpc_id: String::new(),
                state_code: "active".to_string(),
                state_reason: None,
                lb_type,
                availability_zones,
                security_groups,
                ip_address_type,
                customer_owned_ipv4_pool: None,
                enforce_security_group_inbound_rules_on_private_link_traffic: None,
                enable_prefix_for_ipv6_source_nat: None,
                ipv4_ipam_pool_id: None,
                tags,
                attributes: BTreeMap::new(),
                minimum_capacity_units: None,
                bound_port: None,
            },
        );

        Ok(ProvisionResult::new(arn.clone())
            .with("LoadBalancerArn", arn)
            .with(
                "LoadBalancerFullName",
                format!("app/{name}/{}", &lb_id[..16]),
            )
            .with("LoadBalancerName", name)
            .with("DNSName", dns_name)
            .with("CanonicalHostedZoneID", "Z2P70J7EXAMPLE"))
    }

    fn delete_elbv2_load_balancer(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.elbv2_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.load_balancers.remove(physical_id);
        // Cascade-delete listeners and rules attached to this LB.
        let listeners: Vec<String> = state
            .listeners
            .iter()
            .filter(|(_, l)| l.load_balancer_arn == physical_id)
            .map(|(arn, _)| arn.clone())
            .collect();
        for arn in &listeners {
            state.listeners.remove(arn);
            let rules: Vec<String> = state
                .rules
                .iter()
                .filter(|(_, r)| r.listener_arn == *arn)
                .map(|(a, _)| a.clone())
                .collect();
            for r in rules {
                state.rules.remove(&r);
            }
        }
        for tg in state.target_groups.values_mut() {
            tg.load_balancer_arns.retain(|a| a != physical_id);
        }
        Ok(())
    }

    fn create_elbv2_target_group(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let name = props
            .get("Name")
            .and_then(|v| v.as_str())
            .unwrap_or(&resource.logical_id)
            .to_string();
        let protocol = props
            .get("Protocol")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let port = props.get("Port").and_then(|v| v.as_i64()).map(|n| n as i32);
        let vpc_id = props
            .get("VpcId")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let target_type = props
            .get("TargetType")
            .and_then(|v| v.as_str())
            .unwrap_or("instance")
            .to_string();
        let ip_address_type = props
            .get("IpAddressType")
            .and_then(|v| v.as_str())
            .unwrap_or("ipv4")
            .to_string();
        let protocol_version = props
            .get("ProtocolVersion")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let tags = parse_elb_tags(props.get("Tags"));

        let mut accounts = self.elbv2_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let id = Uuid::new_v4().simple().to_string();
        let arn = format!(
            "arn:aws:elasticloadbalancing:{}:{}:targetgroup/{}/{}",
            self.region,
            self.account_id,
            name,
            &id[..16]
        );

        state.target_groups.insert(
            arn.clone(),
            TargetGroup {
                arn: arn.clone(),
                name: name.clone(),
                protocol,
                port,
                vpc_id,
                target_type,
                ip_address_type,
                protocol_version,
                health_check_protocol: props
                    .get("HealthCheckProtocol")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string()),
                health_check_port: props
                    .get("HealthCheckPort")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string()),
                health_check_enabled: props
                    .get("HealthCheckEnabled")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(true),
                health_check_path: props
                    .get("HealthCheckPath")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string()),
                health_check_interval_seconds: props
                    .get("HealthCheckIntervalSeconds")
                    .and_then(|v| v.as_i64())
                    .unwrap_or(30) as i32,
                health_check_timeout_seconds: props
                    .get("HealthCheckTimeoutSeconds")
                    .and_then(|v| v.as_i64())
                    .unwrap_or(5) as i32,
                healthy_threshold_count: props
                    .get("HealthyThresholdCount")
                    .and_then(|v| v.as_i64())
                    .unwrap_or(5) as i32,
                unhealthy_threshold_count: props
                    .get("UnhealthyThresholdCount")
                    .and_then(|v| v.as_i64())
                    .unwrap_or(2) as i32,
                matcher_http_code: props
                    .get("Matcher")
                    .and_then(|v| v.get("HttpCode"))
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string()),
                matcher_grpc_code: props
                    .get("Matcher")
                    .and_then(|v| v.get("GrpcCode"))
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string()),
                load_balancer_arns: Vec::new(),
                targets: Vec::new(),
                tags,
                attributes: BTreeMap::new(),
                created_time: Utc::now(),
            },
        );

        Ok(ProvisionResult::new(arn.clone())
            .with("TargetGroupArn", arn)
            .with("TargetGroupName", name)
            .with("TargetGroupFullName", format!("targetgroup/{}", &id[..16])))
    }

    fn delete_elbv2_target_group(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.elbv2_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.target_groups.remove(physical_id);
        Ok(())
    }

    fn create_elbv2_listener(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let load_balancer_arn = props
            .get("LoadBalancerArn")
            .and_then(|v| v.as_str())
            .ok_or_else(|| "LoadBalancerArn is required".to_string())?
            .to_string();
        let port = props.get("Port").and_then(|v| v.as_i64()).map(|n| n as i32);
        let protocol = props
            .get("Protocol")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let default_actions = parse_elb_actions(props.get("DefaultActions"));

        let mut accounts = self.elbv2_state.write();
        let state = accounts.get_or_create(&self.account_id);
        if !state.load_balancers.contains_key(&load_balancer_arn) {
            return Err(format!(
                "LoadBalancer {load_balancer_arn} not yet provisioned"
            ));
        }

        let lb_full = load_balancer_arn
            .rsplit("loadbalancer/")
            .next()
            .unwrap_or("")
            .to_string();
        let listener_id = Uuid::new_v4().simple().to_string();
        let arn = format!(
            "arn:aws:elasticloadbalancing:{}:{}:listener/{}/{}",
            self.region,
            self.account_id,
            lb_full,
            &listener_id[..16]
        );

        // Wire forward target groups -> LB association so dataplane probing
        // and DescribeTargetGroups round-trip the relationship.
        for action in &default_actions {
            if let Some(tg_arn) = &action.target_group_arn {
                if let Some(tg) = state.target_groups.get_mut(tg_arn) {
                    if !tg.load_balancer_arns.contains(&load_balancer_arn) {
                        tg.load_balancer_arns.push(load_balancer_arn.clone());
                    }
                }
            }
            if let Some(forward) = &action.forward {
                for tgt in &forward.target_groups {
                    if let Some(tg) = state.target_groups.get_mut(&tgt.target_group_arn) {
                        if !tg.load_balancer_arns.contains(&load_balancer_arn) {
                            tg.load_balancer_arns.push(load_balancer_arn.clone());
                        }
                    }
                }
            }
        }

        state.listeners.insert(
            arn.clone(),
            Listener {
                arn: arn.clone(),
                load_balancer_arn,
                port,
                protocol,
                certificates: Vec::new(),
                ssl_policy: props
                    .get("SslPolicy")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string()),
                default_actions,
                alpn_policy: Vec::new(),
                mutual_authentication: None,
                tags: parse_elb_tags(props.get("Tags")),
                attributes: BTreeMap::new(),
            },
        );

        Ok(ProvisionResult::new(arn.clone()).with("ListenerArn", arn))
    }

    fn delete_elbv2_listener(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.elbv2_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.listeners.remove(physical_id);
        let rules: Vec<String> = state
            .rules
            .iter()
            .filter(|(_, r)| r.listener_arn == physical_id)
            .map(|(arn, _)| arn.clone())
            .collect();
        for r in rules {
            state.rules.remove(&r);
        }
        Ok(())
    }

    fn create_elbv2_listener_rule(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let listener_arn = props
            .get("ListenerArn")
            .and_then(|v| v.as_str())
            .ok_or_else(|| "ListenerArn is required".to_string())?
            .to_string();
        let priority = props
            .get("Priority")
            .map(|v| {
                if let Some(s) = v.as_str() {
                    s.to_string()
                } else if let Some(n) = v.as_i64() {
                    n.to_string()
                } else {
                    "1".to_string()
                }
            })
            .unwrap_or_else(|| "1".to_string());
        let actions = parse_elb_actions(props.get("Actions"));
        let conditions = parse_elb_rule_conditions(props.get("Conditions"));

        let mut accounts = self.elbv2_state.write();
        let state = accounts.get_or_create(&self.account_id);
        if !state.listeners.contains_key(&listener_arn) {
            return Err(format!("Listener {listener_arn} not yet provisioned"));
        }
        let listener_full = listener_arn
            .rsplit("listener/")
            .next()
            .unwrap_or("")
            .to_string();
        let rule_id = Uuid::new_v4().simple().to_string();
        let arn = format!(
            "arn:aws:elasticloadbalancing:{}:{}:listener-rule/{}/{}",
            self.region,
            self.account_id,
            listener_full,
            &rule_id[..16]
        );

        state.rules.insert(
            arn.clone(),
            ElbRule {
                arn: arn.clone(),
                listener_arn,
                priority,
                conditions,
                actions,
                is_default: false,
                tags: parse_elb_tags(props.get("Tags")),
            },
        );

        Ok(ProvisionResult::new(arn.clone()).with("RuleArn", arn))
    }

    fn delete_elbv2_listener_rule(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.elbv2_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.rules.remove(physical_id);
        Ok(())
    }

    /// Provision an `AWS::ElasticLoadBalancingV2::ListenerCertificate`.
    /// Appends each non-default certificate from `Certificates` to the
    /// target listener (the default listener cert is set on Listener
    /// creation, so this resource only manages SNI extras).
    fn create_elbv2_listener_certificate(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let listener_arn = props
            .get("ListenerArn")
            .and_then(|v| v.as_str())
            .ok_or_else(|| "ListenerArn is required".to_string())?
            .to_string();
        let certs: Vec<String> = props
            .get("Certificates")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|c| c.get("CertificateArn").and_then(|v| v.as_str()))
                    .map(|s| s.to_string())
                    .collect()
            })
            .unwrap_or_default();
        if certs.is_empty() {
            return Err("Certificates must contain at least one CertificateArn".to_string());
        }
        let mut accounts = self.elbv2_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let listener = state
            .listeners
            .get_mut(&listener_arn)
            .ok_or_else(|| format!("Listener {listener_arn} does not exist"))?;
        for arn in &certs {
            listener.certificates.retain(|c| &c.certificate_arn != arn);
            listener.certificates.push(fakecloud_elbv2::Certificate {
                certificate_arn: arn.clone(),
                is_default: false,
            });
        }
        Ok(ProvisionResult::new(format!(
            "{}#{}",
            listener_arn,
            certs.join(",")
        )))
    }

    fn delete_elbv2_listener_certificate(&self, physical_id: &str) -> Result<(), String> {
        let (listener_arn, cert_list) = match physical_id.split_once('#') {
            Some(parts) => parts,
            None => return Ok(()),
        };
        let cert_arns: Vec<&str> = cert_list.split(',').collect();
        let mut accounts = self.elbv2_state.write();
        let state = accounts.get_or_create(&self.account_id);
        if let Some(listener) = state.listeners.get_mut(listener_arn) {
            listener
                .certificates
                .retain(|c| !cert_arns.iter().any(|a| *a == c.certificate_arn));
        }
        Ok(())
    }

    /// Provision an `AWS::ElasticLoadBalancingV2::TrustStore`.
    fn create_elbv2_trust_store(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let name = props
            .get("Name")
            .and_then(|v| v.as_str())
            .unwrap_or(&resource.logical_id)
            .to_string();
        let bucket = props
            .get("CaCertificatesBundleS3Bucket")
            .and_then(|v| v.as_str())
            .ok_or_else(|| "CaCertificatesBundleS3Bucket is required".to_string())?;
        let key = props
            .get("CaCertificatesBundleS3Key")
            .and_then(|v| v.as_str())
            .ok_or_else(|| "CaCertificatesBundleS3Key is required".to_string())?;
        let tags: Vec<fakecloud_elbv2::Tag> = props
            .get("Tags")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|t| {
                        let k = t.get("Key").and_then(|v| v.as_str())?;
                        let val = t.get("Value").and_then(|v| v.as_str()).unwrap_or("");
                        Some(fakecloud_elbv2::Tag {
                            key: k.to_string(),
                            value: val.to_string(),
                        })
                    })
                    .collect()
            })
            .unwrap_or_default();

        let mut accounts = self.elbv2_state.write();
        let state = accounts.get_or_create(&self.account_id);
        if state.trust_stores.values().any(|t| t.name == name) {
            return Err(format!("Trust store {name} already exists"));
        }
        let suffix: String = Uuid::new_v4()
            .simple()
            .to_string()
            .chars()
            .take(16)
            .collect();
        let arn = format!(
            "arn:aws:elasticloadbalancing:{}:{}:truststore/{}/{}",
            self.region, self.account_id, name, suffix
        );
        let ts = fakecloud_elbv2::TrustStore {
            arn: arn.clone(),
            name: name.clone(),
            status: "ACTIVE".to_string(),
            number_of_ca_certificates: 1,
            total_revoked_entries: 0,
            created_time: Utc::now(),
            ca_certificates_bundle: Some(format!("s3://{bucket}/{key}").into_bytes()),
            revocations: BTreeMap::new(),
            next_revocation_id: 1,
            tags,
        };
        state.trust_stores.insert(arn.clone(), ts);
        Ok(ProvisionResult::new(arn.clone())
            .with("TrustStoreArn", arn)
            .with("Name", name)
            .with("Status", "ACTIVE".to_string()))
    }

    fn delete_elbv2_trust_store(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.elbv2_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.trust_stores.remove(physical_id);
        Ok(())
    }

    /// In-place update for AWS::ElasticLoadBalancingV2::LoadBalancer. Name,
    /// scheme, type and subnet topology are immutable in real AWS — CFN
    /// would replace the resource. We only mutate fields the SetSubnets /
    /// SetSecurityGroups / SetIpAddressType APIs would touch.
    fn update_elbv2_load_balancer(
        &self,
        existing: &StackResource,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let arn = existing.physical_id.clone();
        let mut accounts = self.elbv2_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let lb = state
            .load_balancers
            .get_mut(&arn)
            .ok_or_else(|| format!("LoadBalancer {arn} no longer exists"))?;
        if let Some(arr) = props.get("SecurityGroups").and_then(|v| v.as_array()) {
            lb.security_groups = arr
                .iter()
                .filter_map(|s| s.as_str().map(|s| s.to_string()))
                .collect();
        }
        if let Some(s) = props.get("IpAddressType").and_then(|v| v.as_str()) {
            lb.ip_address_type = s.to_string();
        }
        if let Some(arr) = props.get("Subnets").and_then(|v| v.as_array()) {
            let mut zones: Vec<fakecloud_elbv2::AvailabilityZone> = Vec::new();
            for s in arr {
                if let Some(subnet_id) = s.as_str() {
                    zones.push(fakecloud_elbv2::AvailabilityZone {
                        zone_name: format!("{}a", self.region),
                        subnet_id: subnet_id.to_string(),
                        outpost_id: None,
                        load_balancer_addresses: Vec::new(),
                        source_nat_ipv6_prefixes: Vec::new(),
                    });
                }
            }
            lb.availability_zones = zones;
        }
        if props.get("Tags").is_some() {
            lb.tags = parse_elb_tags(props.get("Tags"));
        }
        let name = lb.name.clone();
        let dns_name = lb.dns_name.clone();
        let canonical = lb.canonical_hosted_zone_id.clone();
        let lb_full = arn.rsplit("loadbalancer/").next().unwrap_or("").to_string();
        Ok(ProvisionResult::new(arn.clone())
            .with("LoadBalancerArn", arn)
            .with("LoadBalancerFullName", lb_full)
            .with("LoadBalancerName", name)
            .with("DNSName", dns_name)
            .with("CanonicalHostedZoneID", canonical))
    }

    /// In-place update for AWS::ElasticLoadBalancingV2::TargetGroup. Mirrors
    /// ModifyTargetGroup: only health-check fields and matcher are mutable.
    fn update_elbv2_target_group(
        &self,
        existing: &StackResource,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let arn = existing.physical_id.clone();
        let mut accounts = self.elbv2_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let tg = state
            .target_groups
            .get_mut(&arn)
            .ok_or_else(|| format!("TargetGroup {arn} no longer exists"))?;
        if let Some(s) = props.get("HealthCheckProtocol").and_then(|v| v.as_str()) {
            tg.health_check_protocol = Some(s.to_string());
        }
        if let Some(s) = props.get("HealthCheckPort").and_then(|v| v.as_str()) {
            tg.health_check_port = Some(s.to_string());
        }
        if let Some(b) = props.get("HealthCheckEnabled").and_then(|v| v.as_bool()) {
            tg.health_check_enabled = b;
        }
        if let Some(s) = props.get("HealthCheckPath").and_then(|v| v.as_str()) {
            tg.health_check_path = Some(s.to_string());
        }
        if let Some(n) = props.get("HealthCheckIntervalSeconds").and_then(cfn_as_i64) {
            tg.health_check_interval_seconds = n as i32;
        }
        if let Some(n) = props.get("HealthCheckTimeoutSeconds").and_then(cfn_as_i64) {
            tg.health_check_timeout_seconds = n as i32;
        }
        if let Some(n) = props.get("HealthyThresholdCount").and_then(cfn_as_i64) {
            tg.healthy_threshold_count = n as i32;
        }
        if let Some(n) = props.get("UnhealthyThresholdCount").and_then(cfn_as_i64) {
            tg.unhealthy_threshold_count = n as i32;
        }
        if let Some(matcher) = props.get("Matcher") {
            tg.matcher_http_code = matcher
                .get("HttpCode")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            tg.matcher_grpc_code = matcher
                .get("GrpcCode")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
        }
        if props.get("Tags").is_some() {
            tg.tags = parse_elb_tags(props.get("Tags"));
        }
        let name = tg.name.clone();
        let tg_full = arn
            .rsplit("targetgroup/")
            .next()
            .map(|s| format!("targetgroup/{s}"))
            .unwrap_or_default();
        Ok(ProvisionResult::new(arn.clone())
            .with("TargetGroupArn", arn)
            .with("TargetGroupName", name)
            .with("TargetGroupFullName", tg_full))
    }

    /// In-place update for AWS::ElasticLoadBalancingV2::Listener. Mirrors
    /// ModifyListener: port, protocol, default actions, certs, ssl policy.
    fn update_elbv2_listener(
        &self,
        existing: &StackResource,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let arn = existing.physical_id.clone();
        let new_default_actions = props
            .get("DefaultActions")
            .map(|v| parse_elb_actions(Some(v)));
        let mut accounts = self.elbv2_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let listener = state
            .listeners
            .get_mut(&arn)
            .ok_or_else(|| format!("Listener {arn} no longer exists"))?;
        if let Some(n) = props.get("Port").and_then(cfn_as_i64) {
            listener.port = Some(n as i32);
        }
        if let Some(s) = props.get("Protocol").and_then(|v| v.as_str()) {
            listener.protocol = Some(s.to_string());
        }
        if let Some(s) = props.get("SslPolicy").and_then(|v| v.as_str()) {
            listener.ssl_policy = Some(s.to_string());
        }
        if let Some(actions) = new_default_actions {
            listener.default_actions = actions;
        }
        if props.get("Tags").is_some() {
            listener.tags = parse_elb_tags(props.get("Tags"));
        }
        Ok(ProvisionResult::new(arn.clone()).with("ListenerArn", arn))
    }

    /// In-place update for AWS::ElasticLoadBalancingV2::ListenerRule. Mirrors
    /// ModifyRule + SetRulePriorities.
    fn update_elbv2_listener_rule(
        &self,
        existing: &StackResource,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let arn = existing.physical_id.clone();
        let new_actions = props.get("Actions").map(|v| parse_elb_actions(Some(v)));
        let new_conditions = props
            .get("Conditions")
            .map(|v| parse_elb_rule_conditions(Some(v)));
        let mut accounts = self.elbv2_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let rule = state
            .rules
            .get_mut(&arn)
            .ok_or_else(|| format!("ListenerRule {arn} no longer exists"))?;
        if let Some(v) = props.get("Priority") {
            rule.priority = if let Some(s) = v.as_str() {
                s.to_string()
            } else if let Some(n) = v.as_i64() {
                n.to_string()
            } else {
                rule.priority.clone()
            };
        }
        if let Some(actions) = new_actions {
            rule.actions = actions;
        }
        if let Some(conditions) = new_conditions {
            rule.conditions = conditions;
        }
        if props.get("Tags").is_some() {
            rule.tags = parse_elb_tags(props.get("Tags"));
        }
        Ok(ProvisionResult::new(arn.clone()).with("RuleArn", arn))
    }

    /// In-place update for AWS::ElasticLoadBalancingV2::ListenerCertificate.
    /// CFN treats this as replace-on-cert-list-change in real AWS, but we
    /// can rebuild the SNI cert set against the same physical id without
    /// disrupting the listener.
    fn update_elbv2_listener_certificate(
        &self,
        existing: &StackResource,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let physical_id = existing.physical_id.clone();
        let listener_arn = props
            .get("ListenerArn")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .or_else(|| physical_id.split_once('#').map(|(l, _)| l.to_string()))
            .ok_or_else(|| "ListenerArn is required".to_string())?;
        let new_certs: Vec<String> = props
            .get("Certificates")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|c| c.get("CertificateArn").and_then(|v| v.as_str()))
                    .map(|s| s.to_string())
                    .collect()
            })
            .unwrap_or_default();
        if new_certs.is_empty() {
            return Err("Certificates must contain at least one CertificateArn".to_string());
        }

        // Strip the previously-managed certs, then attach the new set.
        let prev_certs: Vec<String> = physical_id
            .split_once('#')
            .map(|(_, list)| list.split(',').map(|s| s.to_string()).collect())
            .unwrap_or_default();

        let mut accounts = self.elbv2_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let listener = state
            .listeners
            .get_mut(&listener_arn)
            .ok_or_else(|| format!("Listener {listener_arn} does not exist"))?;
        listener
            .certificates
            .retain(|c| !prev_certs.iter().any(|p| p == &c.certificate_arn));
        for arn in &new_certs {
            listener.certificates.retain(|c| &c.certificate_arn != arn);
            listener.certificates.push(fakecloud_elbv2::Certificate {
                certificate_arn: arn.clone(),
                is_default: false,
            });
        }
        Ok(ProvisionResult::new(format!(
            "{}#{}",
            listener_arn,
            new_certs.join(",")
        )))
    }

    /// In-place update for AWS::ElasticLoadBalancingV2::TrustStore. Only the
    /// CA bundle and tags are mutable; name is immutable in real AWS.
    fn update_elbv2_trust_store(
        &self,
        existing: &StackResource,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let arn = existing.physical_id.clone();
        let mut accounts = self.elbv2_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let ts = state
            .trust_stores
            .get_mut(&arn)
            .ok_or_else(|| format!("TrustStore {arn} no longer exists"))?;
        let new_bucket = props
            .get("CaCertificatesBundleS3Bucket")
            .and_then(|v| v.as_str());
        let new_key = props
            .get("CaCertificatesBundleS3Key")
            .and_then(|v| v.as_str());
        if let (Some(b), Some(k)) = (new_bucket, new_key) {
            ts.ca_certificates_bundle = Some(format!("s3://{b}/{k}").into_bytes());
        }
        if let Some(arr) = props.get("Tags").and_then(|v| v.as_array()) {
            ts.tags = arr
                .iter()
                .filter_map(|t| {
                    let k = t.get("Key").and_then(|v| v.as_str())?;
                    let v = t.get("Value").and_then(|v| v.as_str()).unwrap_or("");
                    Some(fakecloud_elbv2::Tag {
                        key: k.to_string(),
                        value: v.to_string(),
                    })
                })
                .collect();
        }
        let name = ts.name.clone();
        let status = ts.status.clone();
        Ok(ProvisionResult::new(arn.clone())
            .with("TrustStoreArn", arn)
            .with("Name", name)
            .with("Status", status))
    }

    /// Live-state GetAtt fallback for AWS::ElasticLoadBalancingV2::LoadBalancer.
    fn get_att_elbv2_load_balancer(&self, physical_id: &str, attribute: &str) -> Option<String> {
        let mut accounts = self.elbv2_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let lb = state.load_balancers.get(physical_id)?;
        let lb_full = lb
            .arn
            .rsplit("loadbalancer/")
            .next()
            .unwrap_or("")
            .to_string();
        match attribute {
            "Arn" | "LoadBalancerArn" => Some(lb.arn.clone()),
            "DNSName" => Some(lb.dns_name.clone()),
            "CanonicalHostedZoneID" => Some(lb.canonical_hosted_zone_id.clone()),
            "LoadBalancerFullName" => Some(lb_full),
            "LoadBalancerName" => Some(lb.name.clone()),
            "SecurityGroups" => Some(lb.security_groups.join(",")),
            _ => None,
        }
    }

    /// Live-state GetAtt fallback for AWS::ElasticLoadBalancingV2::TargetGroup.
    fn get_att_elbv2_target_group(&self, physical_id: &str, attribute: &str) -> Option<String> {
        let mut accounts = self.elbv2_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let tg = state.target_groups.get(physical_id)?;
        let tg_full = tg
            .arn
            .rsplit("targetgroup/")
            .next()
            .map(|s| format!("targetgroup/{s}"))
            .unwrap_or_default();
        match attribute {
            "TargetGroupArn" => Some(tg.arn.clone()),
            "TargetGroupName" => Some(tg.name.clone()),
            "TargetGroupFullName" => Some(tg_full),
            "LoadBalancerArns" => Some(tg.load_balancer_arns.join(",")),
            _ => None,
        }
    }

    /// Live-state GetAtt fallback for AWS::ElasticLoadBalancingV2::Listener.
    fn get_att_elbv2_listener(&self, physical_id: &str, attribute: &str) -> Option<String> {
        let mut accounts = self.elbv2_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let listener = state.listeners.get(physical_id)?;
        match attribute {
            "Arn" | "ListenerArn" => Some(listener.arn.clone()),
            _ => None,
        }
    }

    /// Live-state GetAtt fallback for AWS::ElasticLoadBalancingV2::ListenerRule.
    fn get_att_elbv2_listener_rule(&self, physical_id: &str, attribute: &str) -> Option<String> {
        let mut accounts = self.elbv2_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let rule = state.rules.get(physical_id)?;
        match attribute {
            "RuleArn" => Some(rule.arn.clone()),
            "IsDefault" => Some(rule.is_default.to_string()),
            _ => None,
        }
    }

    /// Live-state GetAtt fallback for AWS::ElasticLoadBalancingV2::TrustStore.
    fn get_att_elbv2_trust_store(&self, physical_id: &str, attribute: &str) -> Option<String> {
        let mut accounts = self.elbv2_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let ts = state.trust_stores.get(physical_id)?;
        match attribute {
            "TrustStoreArn" => Some(ts.arn.clone()),
            "Name" => Some(ts.name.clone()),
            "Status" => Some(ts.status.clone()),
            "NumberOfCaCertificates" => Some(ts.number_of_ca_certificates.to_string()),
            "TotalRevokedEntries" => Some(ts.total_revoked_entries.to_string()),
            _ => None,
        }
    }

    // --- Organizations ---

    fn create_organization(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let feature_set = props
            .get("FeatureSet")
            .and_then(|v| v.as_str())
            .unwrap_or("ALL")
            .to_string();

        let mut org = self.organizations_state.write();
        if org.is_some() {
            return Err("Organization already exists; only one per fakecloud process".to_string());
        }
        let mut state = OrganizationState::bootstrap(&self.account_id);
        state.feature_set = feature_set;
        let org_id = state.org_id.clone();
        let org_arn = state.org_arn.clone();
        let mgmt_arn = state.management_account_arn.clone();
        let root_id = state.root_id.clone();
        *org = Some(state);

        Ok(ProvisionResult::new(org_id.clone())
            .with("Id", org_id)
            .with("Arn", org_arn)
            .with("ManagementAccountArn", mgmt_arn)
            .with("RootId", root_id))
    }

    fn delete_organization(&self, _physical_id: &str) -> Result<(), String> {
        let mut org = self.organizations_state.write();
        *org = None;
        Ok(())
    }

    fn create_organization_unit(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let name = props
            .get("Name")
            .and_then(|v| v.as_str())
            .unwrap_or(&resource.logical_id)
            .to_string();
        let parent_id = props
            .get("ParentId")
            .and_then(|v| v.as_str())
            .ok_or_else(|| "ParentId is required".to_string())?
            .to_string();

        let mut org_lock = self.organizations_state.write();
        let org = org_lock
            .as_mut()
            .ok_or_else(|| "Organization not yet created".to_string())?;
        // Accept root id, OU id, or `Ref`-resolved logical id (we map to root).
        let resolved_parent_id = if parent_id == org.root_id || org.ous.contains_key(&parent_id) {
            parent_id
        } else {
            return Err(format!("Parent {parent_id} does not exist"));
        };
        let id_suffix: String = Uuid::new_v4()
            .simple()
            .to_string()
            .chars()
            .take(8)
            .collect();
        let id = format!("ou-{}-{}", &org.root_id[2..], id_suffix);
        let arn = format!(
            "arn:aws:organizations::{}:ou/{}/{}",
            org.management_account_id, org.org_id, id
        );
        org.ous.insert(
            id.clone(),
            OrganizationalUnit {
                id: id.clone(),
                arn: arn.clone(),
                name: name.clone(),
                parent_id: resolved_parent_id,
            },
        );
        Ok(ProvisionResult::new(id.clone())
            .with("Id", id)
            .with("Arn", arn)
            .with("Name", name))
    }

    fn delete_organization_unit(&self, physical_id: &str) -> Result<(), String> {
        let mut org_lock = self.organizations_state.write();
        if let Some(org) = org_lock.as_mut() {
            org.ous.remove(physical_id);
            org.attachments.remove(physical_id);
        }
        Ok(())
    }

    /// Provision an `AWS::Organizations::Account`. Mints a new member
    /// account synchronously (via Organizations state), optionally moves
    /// it under the first ParentId when supplied, and persists tags.
    fn create_organization_account(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let email = props
            .get("Email")
            .and_then(|v| v.as_str())
            .ok_or_else(|| "Email is required".to_string())?
            .to_string();
        let name = props
            .get("AccountName")
            .and_then(|v| v.as_str())
            .ok_or_else(|| "AccountName is required".to_string())?
            .to_string();
        let parent_ids: Vec<String> = props
            .get("ParentIds")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                    .collect()
            })
            .unwrap_or_default();
        let tags: Vec<(String, String)> = props
            .get("Tags")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|t| {
                        let k = t.get("Key").and_then(|v| v.as_str())?;
                        let val = t.get("Value").and_then(|v| v.as_str()).unwrap_or("");
                        Some((k.to_string(), val.to_string()))
                    })
                    .collect()
            })
            .unwrap_or_default();

        let mut org_lock = self.organizations_state.write();
        let org = org_lock
            .as_mut()
            .ok_or_else(|| "Organization not yet created".to_string())?;
        // CFN provisioning is its own asynchronous flow; we don't need
        // a second layer of poll-for-completion on top. Begin the
        // request and immediately drive it to SUCCEEDED so the rest of
        // this provisioner sees a fully enrolled account.
        let pending = org.begin_create_account(&email, &name, None);
        let status = org.complete_create_account(&pending.id).unwrap_or(pending);
        let account_id = status
            .account_id
            .clone()
            .ok_or_else(|| "create_account did not return an account id".to_string())?;
        let account_arn = org
            .accounts
            .get(&account_id)
            .map(|a| a.arn.clone())
            .unwrap_or_default();
        let joined_method = org
            .accounts
            .get(&account_id)
            .map(|a| a.joined_method.clone())
            .unwrap_or_else(|| "CREATED".to_string());
        let joined_timestamp = org
            .accounts
            .get(&account_id)
            .map(|a| a.joined_timestamp.to_rfc3339())
            .unwrap_or_default();
        let acct_status = org
            .accounts
            .get(&account_id)
            .map(|a| a.status.clone())
            .unwrap_or_else(|| "ACTIVE".to_string());

        if let Some(parent) = parent_ids.first() {
            let source = org
                .accounts
                .get(&account_id)
                .map(|a| a.parent_id.clone())
                .unwrap_or_else(|| org.root_id.clone());
            if parent != &source {
                org.move_account(&account_id, &source, parent)
                    .map_err(|e| format!("Failed to move account to parent {parent}: {e:?}"))?;
            }
        }

        if !tags.is_empty() {
            org.set_resource_tags(&account_id, &tags);
        }

        Ok(ProvisionResult::new(account_id.clone())
            .with("AccountId", account_id)
            .with("AccountName", name)
            .with("Email", email)
            .with("Arn", account_arn)
            .with("JoinedMethod", joined_method)
            .with("JoinedTimestamp", joined_timestamp)
            .with("Status", acct_status))
    }

    /// Close the member account on stack delete. Real AWS leaves the
    /// account in `SUSPENDED` for 90 days; we just flip it via
    /// `close_account` so subsequent reads see it as suspended.
    fn delete_organization_account(&self, physical_id: &str) -> Result<(), String> {
        let mut org_lock = self.organizations_state.write();
        if let Some(org) = org_lock.as_mut() {
            let _ = org.close_account(physical_id);
        }
        Ok(())
    }

    fn create_organization_policy(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let name = props
            .get("Name")
            .and_then(|v| v.as_str())
            .unwrap_or(&resource.logical_id)
            .to_string();
        let description = props
            .get("Description")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let policy_type = props
            .get("Type")
            .and_then(|v| v.as_str())
            .unwrap_or(POLICY_TYPE_SCP)
            .to_string();
        let content = props
            .get("Content")
            .map(|v| {
                if v.is_string() {
                    v.as_str().unwrap_or("").to_string()
                } else {
                    serde_json::to_string(v).unwrap_or_default()
                }
            })
            .unwrap_or_default();
        let target_ids: Vec<String> = props
            .get("TargetIds")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|t| t.as_str().map(|s| s.to_string()))
                    .collect()
            })
            .unwrap_or_default();

        let mut org_lock = self.organizations_state.write();
        let org = org_lock
            .as_mut()
            .ok_or_else(|| "Organization not yet created".to_string())?;
        let id_suffix: String = Uuid::new_v4()
            .simple()
            .to_string()
            .chars()
            .take(8)
            .collect();
        let id = format!("p-{}", id_suffix);
        let arn = format!(
            "arn:aws:organizations::{}:policy/{}/{}/{}",
            org.management_account_id,
            org.org_id,
            policy_type.to_lowercase(),
            id
        );
        org.policies.insert(
            id.clone(),
            OrgPolicy {
                id: id.clone(),
                arn: arn.clone(),
                name: name.clone(),
                description,
                policy_type,
                aws_managed: false,
                content,
            },
        );
        for target in target_ids {
            org.attachments
                .entry(target)
                .or_default()
                .insert(id.clone());
        }
        Ok(ProvisionResult::new(id.clone())
            .with("Id", id)
            .with("Arn", arn)
            .with("Name", name))
    }

    fn delete_organization_policy(&self, physical_id: &str) -> Result<(), String> {
        let mut org_lock = self.organizations_state.write();
        if let Some(org) = org_lock.as_mut() {
            org.policies.remove(physical_id);
            for attachments in org.attachments.values_mut() {
                attachments.remove(physical_id);
            }
        }
        Ok(())
    }

    fn create_organization_resource_policy(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let content = props
            .get("Content")
            .map(|v| {
                if v.is_string() {
                    v.as_str().unwrap_or("").to_string()
                } else {
                    serde_json::to_string(v).unwrap_or_default()
                }
            })
            .ok_or_else(|| "Content is required".to_string())?;

        let mut org_lock = self.organizations_state.write();
        let org = org_lock
            .as_mut()
            .ok_or_else(|| "Organization not yet created".to_string())?;
        org.resource_policy = Some(content);
        let arn = format!(
            "arn:aws:organizations::{}:resourcepolicy/{}/rp",
            org.management_account_id, org.org_id
        );
        Ok(ProvisionResult::new(arn.clone()).with("Arn", arn))
    }

    fn delete_organization_resource_policy(&self, _physical_id: &str) -> Result<(), String> {
        let mut org_lock = self.organizations_state.write();
        if let Some(org) = org_lock.as_mut() {
            org.resource_policy = None;
        }
        Ok(())
    }

    fn delete_log_group(&self, physical_id: &str) -> Result<(), String> {
        let mut logs_accounts = self.logs_state.write();
        let state = logs_accounts.default_mut();
        // physical_id is the ARN; find the log group name
        let name = state
            .log_groups
            .iter()
            .find(|(_, g)| g.arn == physical_id)
            .map(|(name, _)| name.clone());
        if let Some(name) = name {
            state.log_groups.remove(&name);
        }
        Ok(())
    }

    fn create_log_stream(&self, resource: &ResourceDefinition) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let log_group_name = props
            .get("LogGroupName")
            .and_then(|v| v.as_str())
            .map(parse_log_group_name)
            .ok_or_else(|| "LogGroupName is required".to_string())?;
        let log_stream_name = props
            .get("LogStreamName")
            .and_then(|v| v.as_str())
            .unwrap_or(&resource.logical_id)
            .to_string();

        let mut logs_accounts = self.logs_state.write();
        let state = logs_accounts.get_or_create(&self.account_id);
        let group = state
            .log_groups
            .get_mut(&log_group_name)
            .ok_or_else(|| format!("Log group {log_group_name} does not exist"))?;
        let arn = format!(
            "arn:aws:logs:{}:{}:log-group:{}:log-stream:{}",
            self.region, self.account_id, log_group_name, log_stream_name
        );
        if group.log_streams.contains_key(&log_stream_name) {
            return Err(format!(
                "Log stream {log_stream_name} already exists in {log_group_name}"
            ));
        }
        group.log_streams.insert(
            log_stream_name.clone(),
            LogStream {
                name: log_stream_name.clone(),
                arn,
                creation_time: Utc::now().timestamp_millis(),
                first_event_timestamp: None,
                last_event_timestamp: None,
                last_ingestion_time: None,
                upload_sequence_token: String::new(),
                events: Vec::new(),
            },
        );

        // Encode group + stream into the physical id so deletion can target both.
        let physical_id = format!("{log_group_name}|{log_stream_name}");
        Ok(ProvisionResult::new(physical_id))
    }

    fn delete_log_stream(&self, physical_id: &str) -> Result<(), String> {
        let mut logs_accounts = self.logs_state.write();
        let state = logs_accounts.get_or_create(&self.account_id);
        if let Some((group_name, stream_name)) = physical_id.split_once('|') {
            if let Some(group) = state.log_groups.get_mut(group_name) {
                group.log_streams.remove(stream_name);
            }
        }
        Ok(())
    }

    fn create_metric_filter(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let log_group_name = props
            .get("LogGroupName")
            .and_then(|v| v.as_str())
            .map(parse_log_group_name)
            .ok_or_else(|| "LogGroupName is required".to_string())?;
        let filter_name = props
            .get("FilterName")
            .and_then(|v| v.as_str())
            .unwrap_or(&resource.logical_id)
            .to_string();
        let filter_pattern = props
            .get("FilterPattern")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();

        let mut transformations: Vec<MetricTransformation> = Vec::new();
        if let Some(arr) = props
            .get("MetricTransformations")
            .and_then(|v| v.as_array())
        {
            for t in arr {
                let metric_name = t
                    .get("MetricName")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                let metric_namespace = t
                    .get("MetricNamespace")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                let metric_value = t
                    .get("MetricValue")
                    .and_then(|v| v.as_str())
                    .unwrap_or("1")
                    .to_string();
                let default_value = t.get("DefaultValue").and_then(|v| v.as_f64());
                transformations.push(MetricTransformation {
                    metric_name,
                    metric_namespace,
                    metric_value,
                    default_value,
                });
            }
        }

        let mut logs_accounts = self.logs_state.write();
        let state = logs_accounts.get_or_create(&self.account_id);
        if !state.log_groups.contains_key(&log_group_name) {
            return Err(format!("Log group {log_group_name} does not exist"));
        }
        state
            .metric_filters
            .retain(|f| !(f.log_group_name == log_group_name && f.filter_name == filter_name));
        state.metric_filters.push(MetricFilter {
            filter_name: filter_name.clone(),
            filter_pattern,
            log_group_name: log_group_name.clone(),
            metric_transformations: transformations,
            creation_time: Utc::now().timestamp_millis(),
        });

        Ok(ProvisionResult::new(format!(
            "{log_group_name}|{filter_name}"
        )))
    }

    fn delete_metric_filter(&self, physical_id: &str) -> Result<(), String> {
        let mut logs_accounts = self.logs_state.write();
        let state = logs_accounts.get_or_create(&self.account_id);
        if let Some((group_name, filter_name)) = physical_id.split_once('|') {
            state
                .metric_filters
                .retain(|f| !(f.log_group_name == group_name && f.filter_name == filter_name));
        }
        Ok(())
    }

    fn create_subscription_filter(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let log_group_name = props
            .get("LogGroupName")
            .and_then(|v| v.as_str())
            .map(parse_log_group_name)
            .ok_or_else(|| "LogGroupName is required".to_string())?;
        let filter_name = props
            .get("FilterName")
            .and_then(|v| v.as_str())
            .unwrap_or(&resource.logical_id)
            .to_string();
        let filter_pattern = props
            .get("FilterPattern")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let destination_arn = props
            .get("DestinationArn")
            .and_then(|v| v.as_str())
            .ok_or_else(|| "DestinationArn is required".to_string())?
            .to_string();
        let role_arn = props
            .get("RoleArn")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let distribution = props
            .get("Distribution")
            .and_then(|v| v.as_str())
            .unwrap_or("ByLogStream")
            .to_string();

        let mut logs_accounts = self.logs_state.write();
        let state = logs_accounts.get_or_create(&self.account_id);
        let group = state
            .log_groups
            .get_mut(&log_group_name)
            .ok_or_else(|| format!("Log group {log_group_name} does not exist"))?;
        group
            .subscription_filters
            .retain(|f| f.filter_name != filter_name);
        group.subscription_filters.push(SubscriptionFilter {
            filter_name: filter_name.clone(),
            log_group_name: log_group_name.clone(),
            filter_pattern,
            destination_arn,
            role_arn,
            distribution,
            creation_time: Utc::now().timestamp_millis(),
        });

        Ok(ProvisionResult::new(format!(
            "{log_group_name}|{filter_name}"
        )))
    }

    fn delete_subscription_filter(&self, physical_id: &str) -> Result<(), String> {
        let mut logs_accounts = self.logs_state.write();
        let state = logs_accounts.get_or_create(&self.account_id);
        if let Some((group_name, filter_name)) = physical_id.split_once('|') {
            if let Some(group) = state.log_groups.get_mut(group_name) {
                group
                    .subscription_filters
                    .retain(|f| f.filter_name != filter_name);
            }
        }
        Ok(())
    }

    // --- Logs: Destination / ResourcePolicy / QueryDefinition / Delivery* ---

    fn create_logs_destination(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let destination_name = props
            .get("DestinationName")
            .and_then(|v| v.as_str())
            .ok_or("DestinationName is required")?
            .to_string();
        let target_arn = props
            .get("TargetArn")
            .and_then(|v| v.as_str())
            .ok_or("TargetArn is required")?
            .to_string();
        let role_arn = props
            .get("RoleArn")
            .and_then(|v| v.as_str())
            .ok_or("RoleArn is required")?
            .to_string();
        let access_policy = props
            .get("DestinationPolicy")
            .and_then(|v| v.as_str())
            .map(String::from);

        let arn = format!(
            "arn:aws:logs:{}:{}:destination:{destination_name}",
            self.region, self.account_id
        );
        let dest = Destination {
            destination_name: destination_name.clone(),
            target_arn,
            role_arn,
            arn: arn.clone(),
            access_policy,
            creation_time: Utc::now().timestamp_millis(),
            tags: BTreeMap::new(),
        };

        let mut logs_accounts = self.logs_state.write();
        let state = logs_accounts.get_or_create(&self.account_id);
        state.destinations.insert(destination_name.clone(), dest);

        Ok(ProvisionResult::new(destination_name).with("Arn", arn))
    }

    fn delete_logs_destination(&self, physical_id: &str) -> Result<(), String> {
        let mut logs_accounts = self.logs_state.write();
        let state = logs_accounts.get_or_create(&self.account_id);
        state.destinations.remove(physical_id);
        Ok(())
    }

    fn create_logs_resource_policy(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let policy_name = props
            .get("PolicyName")
            .and_then(|v| v.as_str())
            .ok_or("PolicyName is required")?
            .to_string();
        let policy_document = props
            .get("PolicyDocument")
            .map(|v| {
                if let Some(s) = v.as_str() {
                    s.to_string()
                } else {
                    serde_json::to_string(v).unwrap_or_default()
                }
            })
            .ok_or("PolicyDocument is required")?;

        let policy = ResourcePolicy {
            policy_name: policy_name.clone(),
            policy_document,
            last_updated_time: Utc::now().timestamp_millis(),
        };

        let mut logs_accounts = self.logs_state.write();
        let state = logs_accounts.get_or_create(&self.account_id);
        state.resource_policies.insert(policy_name.clone(), policy);

        Ok(ProvisionResult::new(policy_name))
    }

    fn delete_logs_resource_policy(&self, physical_id: &str) -> Result<(), String> {
        let mut logs_accounts = self.logs_state.write();
        let state = logs_accounts.get_or_create(&self.account_id);
        state.resource_policies.remove(physical_id);
        Ok(())
    }

    fn create_logs_query_definition(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let name = props
            .get("Name")
            .and_then(|v| v.as_str())
            .ok_or("Name is required")?
            .to_string();
        let query_string = props
            .get("QueryString")
            .and_then(|v| v.as_str())
            .ok_or("QueryString is required")?
            .to_string();
        let log_group_names: Vec<String> = props
            .get("LogGroupNames")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();

        let id = Uuid::new_v4().to_string();
        let qd = QueryDefinition {
            query_definition_id: id.clone(),
            name,
            query_string,
            log_group_names,
            last_modified: Utc::now().timestamp_millis(),
        };

        let mut logs_accounts = self.logs_state.write();
        let state = logs_accounts.get_or_create(&self.account_id);
        state.query_definitions.insert(id.clone(), qd);

        Ok(ProvisionResult::new(id.clone()).with("QueryDefinitionId", id))
    }

    fn delete_logs_query_definition(&self, physical_id: &str) -> Result<(), String> {
        let mut logs_accounts = self.logs_state.write();
        let state = logs_accounts.get_or_create(&self.account_id);
        state.query_definitions.remove(physical_id);
        Ok(())
    }

    fn create_logs_delivery_destination(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let name = props
            .get("Name")
            .and_then(|v| v.as_str())
            .ok_or("Name is required")?
            .to_string();
        let output_format = props
            .get("OutputFormat")
            .and_then(|v| v.as_str())
            .map(String::from);
        let mut configuration: BTreeMap<String, String> = BTreeMap::new();
        if let Some(arn) = props.get("DestinationResourceArn").and_then(|v| v.as_str()) {
            configuration.insert("destinationResourceArn".to_string(), arn.to_string());
        }
        if let Some(cfg) = props
            .get("DeliveryDestinationConfiguration")
            .and_then(|v| v.as_object())
        {
            for (k, v) in cfg {
                if let Some(s) = v.as_str() {
                    configuration.insert(k.clone(), s.to_string());
                }
            }
        }
        let policy = props.get("DeliveryDestinationPolicy").map(|v| {
            if let Some(s) = v.as_str() {
                s.to_string()
            } else {
                serde_json::to_string(v).unwrap_or_default()
            }
        });

        let arn = format!(
            "arn:aws:logs:{}:{}:delivery-destination:{name}",
            self.region, self.account_id
        );
        let dd = DeliveryDestination {
            name: name.clone(),
            arn: arn.clone(),
            output_format,
            delivery_destination_configuration: configuration,
            tags: BTreeMap::new(),
            delivery_destination_policy: policy,
        };

        let mut logs_accounts = self.logs_state.write();
        let state = logs_accounts.get_or_create(&self.account_id);
        state.delivery_destinations.insert(name.clone(), dd);

        Ok(ProvisionResult::new(name).with("Arn", arn))
    }

    fn delete_logs_delivery_destination(&self, physical_id: &str) -> Result<(), String> {
        let mut logs_accounts = self.logs_state.write();
        let state = logs_accounts.get_or_create(&self.account_id);
        state.delivery_destinations.remove(physical_id);
        Ok(())
    }

    fn create_logs_delivery_source(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let name = props
            .get("Name")
            .and_then(|v| v.as_str())
            .ok_or("Name is required")?
            .to_string();
        let resource_arns: Vec<String> = props
            .get("ResourceArn")
            .and_then(|v| v.as_str())
            .map(|s| vec![s.to_string()])
            .or_else(|| {
                props
                    .get("ResourceArns")
                    .and_then(|v| v.as_array())
                    .map(|arr| {
                        arr.iter()
                            .filter_map(|v| v.as_str().map(String::from))
                            .collect()
                    })
            })
            .unwrap_or_default();
        let log_type = props
            .get("LogType")
            .and_then(|v| v.as_str())
            .ok_or("LogType is required")?
            .to_string();
        let service = props
            .get("Service")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();

        let arn = format!(
            "arn:aws:logs:{}:{}:delivery-source:{name}",
            self.region, self.account_id
        );
        let ds = DeliverySource {
            name: name.clone(),
            arn: arn.clone(),
            resource_arns,
            service,
            log_type,
            tags: BTreeMap::new(),
            created_at: chrono::Utc::now().timestamp_millis(),
        };

        let mut logs_accounts = self.logs_state.write();
        let state = logs_accounts.get_or_create(&self.account_id);
        state.delivery_sources.insert(name.clone(), ds);

        Ok(ProvisionResult::new(name).with("Arn", arn))
    }

    fn delete_logs_delivery_source(&self, physical_id: &str) -> Result<(), String> {
        let mut logs_accounts = self.logs_state.write();
        let state = logs_accounts.get_or_create(&self.account_id);
        state.delivery_sources.remove(physical_id);
        Ok(())
    }

    fn create_logs_delivery(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let delivery_source_name = props
            .get("DeliverySourceName")
            .and_then(|v| v.as_str())
            .ok_or("DeliverySourceName is required")?
            .to_string();
        let delivery_destination_arn = props
            .get("DeliveryDestinationArn")
            .and_then(|v| v.as_str())
            .ok_or("DeliveryDestinationArn is required")?
            .to_string();
        // Infer destination type from the destination ARN service segment.
        let delivery_destination_type = if delivery_destination_arn.contains(":s3:") {
            "S3".to_string()
        } else if delivery_destination_arn.contains(":firehose:") {
            "FH".to_string()
        } else {
            "CWL".to_string()
        };

        let id = Uuid::new_v4().simple().to_string();
        let arn = format!(
            "arn:aws:logs:{}:{}:delivery:{id}",
            self.region, self.account_id
        );
        let delivery = Delivery {
            id: id.clone(),
            delivery_source_name,
            delivery_destination_arn,
            delivery_destination_type,
            arn: arn.clone(),
            tags: BTreeMap::new(),
            field_delimiter: None,
            record_fields: Vec::new(),
            s3_delivery_configuration: None,
            created_at: chrono::Utc::now().timestamp_millis(),
        };

        let mut logs_accounts = self.logs_state.write();
        let state = logs_accounts.get_or_create(&self.account_id);
        state.deliveries.insert(id.clone(), delivery);

        Ok(ProvisionResult::new(id.clone())
            .with("DeliveryId", id)
            .with("Arn", arn))
    }

    fn delete_logs_delivery(&self, physical_id: &str) -> Result<(), String> {
        let mut logs_accounts = self.logs_state.write();
        let state = logs_accounts.get_or_create(&self.account_id);
        state.deliveries.remove(physical_id);
        Ok(())
    }

    // --- Custom Resources ---

    /// Invoke a Lambda function synchronously via the delivery bus.
    fn invoke_lambda_sync(&self, function_arn: &str, payload: &str) -> Result<(), String> {
        let delivery = self.delivery.clone();
        let function_arn = function_arn.to_string();
        let payload = payload.to_string();
        std::thread::scope(|s| {
            s.spawn(|| {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .map_err(|e| format!("Failed to create runtime: {e}"))?;
                rt.block_on(async {
                    match delivery.invoke_lambda(&function_arn, &payload).await {
                        Some(Ok(_)) => {
                            tracing::info!(
                                "Custom resource Lambda {} invoked successfully",
                                function_arn
                            );
                            Ok(())
                        }
                        Some(Err(e)) => {
                            tracing::warn!(
                                "Custom resource Lambda {} invocation failed: {e}",
                                function_arn
                            );
                            Err(format!("Lambda invocation failed: {e}"))
                        }
                        None => {
                            tracing::warn!(
                                "No Lambda delivery configured; skipping custom resource invocation for {}",
                                function_arn
                            );
                            Ok(())
                        }
                    }
                })
            })
            .join()
            .map_err(|_| "Lambda invocation thread panicked".to_string())?
        })
    }

    fn create_custom_resource(&self, resource: &ResourceDefinition) -> Result<String, String> {
        let props = &resource.properties;
        let service_token = props
            .get("ServiceToken")
            .and_then(|v| v.as_str())
            .ok_or("Custom resource requires ServiceToken property")?;

        let request_id = Uuid::new_v4().to_string();

        // Build the CloudFormation custom resource event
        let event = serde_json::json!({
            "RequestType": "Create",
            "ServiceToken": service_token,
            "StackId": self.stack_id,
            "RequestId": request_id,
            "ResourceType": resource.resource_type,
            "LogicalResourceId": resource.logical_id,
            "ResourceProperties": props,
        });

        let payload = serde_json::to_string(&event).map_err(|e| e.to_string())?;
        self.invoke_lambda_sync(service_token, &payload)?;

        // Physical resource ID: use a generated ID (the Lambda could return one,
        // but for simplicity we generate one here).
        let physical_id = format!("{}-{}", resource.logical_id, &request_id[..8]);
        Ok(physical_id)
    }

    fn delete_custom_resource(&self, resource: &StackResource) -> Result<(), String> {
        let service_token = match &resource.service_token {
            Some(token) => token.clone(),
            None => {
                // No ServiceToken stored — nothing to invoke
                return Ok(());
            }
        };

        let request_id = Uuid::new_v4().to_string();

        let event = serde_json::json!({
            "RequestType": "Delete",
            "ServiceToken": service_token,
            "StackId": self.stack_id,
            "RequestId": request_id,
            "ResourceType": resource.resource_type,
            "LogicalResourceId": resource.logical_id,
            "PhysicalResourceId": resource.physical_id,
        });

        let payload = serde_json::to_string(&event).map_err(|e| e.to_string())?;

        // Best-effort: don't fail stack deletion if Lambda invocation fails
        if let Err(e) = self.invoke_lambda_sync(&service_token, &payload) {
            tracing::warn!(
                "Custom resource delete Lambda invocation failed for {}: {e}",
                resource.logical_id
            );
        }
        Ok(())
    }

    // --- Application Auto Scaling ---

    fn create_application_autoscaling_scalable_target(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let service_namespace = props
            .get("ServiceNamespace")
            .and_then(|v| v.as_str())
            .ok_or_else(|| "ServiceNamespace is required".to_string())?
            .to_string();
        let resource_id = props
            .get("ResourceId")
            .and_then(|v| v.as_str())
            .ok_or_else(|| "ResourceId is required".to_string())?
            .to_string();
        let scalable_dimension = props
            .get("ScalableDimension")
            .and_then(|v| v.as_str())
            .ok_or_else(|| "ScalableDimension is required".to_string())?
            .to_string();
        let min_capacity = props
            .get("MinCapacity")
            .and_then(|v| v.as_i64())
            .map(|n| n as i32)
            .ok_or_else(|| "MinCapacity is required".to_string())?;
        let max_capacity = props
            .get("MaxCapacity")
            .and_then(|v| v.as_i64())
            .map(|n| n as i32)
            .ok_or_else(|| "MaxCapacity is required".to_string())?;
        if min_capacity > max_capacity {
            return Err("MinCapacity must be <= MaxCapacity".to_string());
        }
        let role_arn = props
            .get("RoleARN")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let suspended_state = props.get("SuspendedState").map(|v| AppasSuspendedState {
            dynamic_scaling_in_suspended: v
                .get("DynamicScalingInSuspended")
                .and_then(|x| x.as_bool()),
            dynamic_scaling_out_suspended: v
                .get("DynamicScalingOutSuspended")
                .and_then(|x| x.as_bool()),
            scheduled_scaling_suspended: v
                .get("ScheduledScalingSuspended")
                .and_then(|x| x.as_bool()),
        });

        let arn = format!(
            "arn:aws:application-autoscaling:{}:{}:scalable-target/{}",
            self.region,
            self.account_id,
            &Uuid::new_v4().simple().to_string()[..10]
        );
        let role = role_arn.unwrap_or_else(|| {
            let suffix = match service_namespace.as_str() {
                "ecs" => "ECSService",
                "elasticmapreduce" => "EMRContainerService",
                "ec2" => "EC2SpotFleetRequest",
                "appstream" => "ApplicationAutoScaling_AppStreamFleet",
                "dynamodb" => "DynamoDBTable",
                "rds" => "RDSCluster",
                "sagemaker" => "SageMakerEndpoint",
                "lambda" => "LambdaConcurrency",
                "elasticache" => "ElastiCacheRG",
                "cassandra" => "CassandraTable",
                "kafka" => "KafkaCluster",
                _ => "ApplicationAutoScaling_Default",
            };
            format!(
                "arn:aws:iam::{}:role/aws-service-role/applicationautoscaling.amazonaws.com/AWSServiceRoleForApplicationAutoScaling_{}",
                self.account_id, suffix
            )
        });

        let mut state = self.app_autoscaling_state.write();
        let account = state.accounts.entry(self.account_id.clone()).or_default();
        let key = (
            service_namespace.clone(),
            resource_id.clone(),
            scalable_dimension.clone(),
        );
        let target = AppasScalableTarget {
            arn: arn.clone(),
            service_namespace: service_namespace.clone(),
            resource_id: resource_id.clone(),
            scalable_dimension: scalable_dimension.clone(),
            min_capacity,
            max_capacity,
            role_arn: role,
            creation_time: Utc::now(),
            suspended_state,
            predicted_capacity: None,
        };
        account.scalable_targets.insert(key, target);

        Ok(ProvisionResult::new(resource_id.clone())
            .with("ScalableTargetARN", arn)
            .with("ServiceNamespace", service_namespace)
            .with("ScalableDimension", scalable_dimension))
    }

    fn create_application_autoscaling_scaling_policy(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let policy_name = props
            .get("PolicyName")
            .and_then(|v| v.as_str())
            .ok_or_else(|| "PolicyName is required".to_string())?
            .to_string();
        let service_namespace = props
            .get("ServiceNamespace")
            .and_then(|v| v.as_str())
            .ok_or_else(|| "ServiceNamespace is required".to_string())?
            .to_string();
        let resource_id = props
            .get("ResourceId")
            .and_then(|v| v.as_str())
            .ok_or_else(|| "ResourceId is required".to_string())?
            .to_string();
        let scalable_dimension = props
            .get("ScalableDimension")
            .and_then(|v| v.as_str())
            .ok_or_else(|| "ScalableDimension is required".to_string())?
            .to_string();
        let policy_type = props
            .get("PolicyType")
            .and_then(|v| v.as_str())
            .unwrap_or("StepScaling")
            .to_string();
        let step_cfg = props.get("StepScalingPolicyConfiguration").cloned();
        let tt_cfg = props
            .get("TargetTrackingScalingPolicyConfiguration")
            .cloned();
        let pred_cfg = props.get("PredictiveScalingPolicyConfiguration").cloned();

        let target_key = (
            service_namespace.clone(),
            resource_id.clone(),
            scalable_dimension.clone(),
        );
        let policy_key = (
            service_namespace.clone(),
            resource_id.clone(),
            scalable_dimension.clone(),
            policy_name.clone(),
        );

        let mut state = self.app_autoscaling_state.write();
        let account = state.accounts.entry(self.account_id.clone()).or_default();
        if !account.scalable_targets.contains_key(&target_key) {
            return Err(format!(
                "No scalable target registered for ServiceNamespace={} ResourceId={} ScalableDimension={}",
                service_namespace, resource_id, scalable_dimension
            ));
        }
        let arn = format!(
            "arn:aws:autoscaling:{}:{}:scalingPolicy:{}:resource/{}/{}:policyName/{}",
            self.region,
            self.account_id,
            Uuid::new_v4(),
            service_namespace,
            resource_id,
            policy_name
        );
        let policy = AppasScalingPolicy {
            arn: arn.clone(),
            policy_name: policy_name.clone(),
            service_namespace: service_namespace.clone(),
            resource_id: resource_id.clone(),
            scalable_dimension: scalable_dimension.clone(),
            policy_type: policy_type.clone(),
            creation_time: Utc::now(),
            step_scaling_policy_configuration: step_cfg,
            target_tracking_scaling_policy_configuration: tt_cfg,
            predictive_scaling_policy_configuration: pred_cfg,
            alarms: Vec::new(),
            last_applied_at: None,
        };
        account.scaling_policies.insert(policy_key, policy);

        Ok(ProvisionResult::new(arn.clone())
            .with("PolicyName", policy_name)
            .with("ServiceNamespace", service_namespace)
            .with("ResourceId", resource_id)
            .with("ScalableDimension", scalable_dimension))
    }

    fn delete_application_autoscaling_scalable_target(
        &self,
        physical_id: &str,
        attributes: &BTreeMap<String, String>,
    ) -> Result<(), String> {
        let namespace = attributes
            .get("ServiceNamespace")
            .cloned()
            .ok_or_else(|| "ServiceNamespace missing in attributes".to_string())?;
        let resource_id = physical_id.to_string();
        let dimension = attributes
            .get("ScalableDimension")
            .cloned()
            .ok_or_else(|| "ScalableDimension missing in attributes".to_string())?;
        let key = (namespace, resource_id.clone(), dimension);

        let mut state = self.app_autoscaling_state.write();
        let account = state.accounts.entry(self.account_id.clone()).or_default();
        account.scalable_targets.remove(&key);
        account
            .scaling_policies
            .retain(|k, _| !(k.0 == key.0 && k.1 == key.1 && k.2 == key.2));
        account
            .scheduled_actions
            .retain(|k, _| !(k.0 == key.0 && k.1 == key.1 && k.2 == key.2));
        Ok(())
    }

    fn delete_application_autoscaling_scaling_policy(
        &self,
        _physical_id: &str,
        attributes: &BTreeMap<String, String>,
    ) -> Result<(), String> {
        let policy_name = attributes
            .get("PolicyName")
            .cloned()
            .ok_or_else(|| "PolicyName missing in attributes".to_string())?;
        let namespace = attributes
            .get("ServiceNamespace")
            .cloned()
            .ok_or_else(|| "ServiceNamespace missing in attributes".to_string())?;
        let resource_id = attributes
            .get("ResourceId")
            .cloned()
            .ok_or_else(|| "ResourceId missing in attributes".to_string())?;
        let dimension = attributes
            .get("ScalableDimension")
            .cloned()
            .ok_or_else(|| "ScalableDimension missing in attributes".to_string())?;
        let key = (namespace, resource_id, dimension, policy_name);

        let mut state = self.app_autoscaling_state.write();
        let account = state.accounts.entry(self.account_id.clone()).or_default();
        account.scaling_policies.remove(&key);
        Ok(())
    }

    // --- Firehose ---

    fn create_firehose_delivery_stream(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let name = props
            .get("DeliveryStreamName")
            .and_then(|v| v.as_str())
            .unwrap_or(&resource.logical_id)
            .to_string();

        let arn = format!(
            "arn:aws:firehose:{}:{}:deliverystream/{}",
            self.region, self.account_id, name
        );
        let stream_type = props
            .get("DeliveryStreamType")
            .and_then(|v| v.as_str())
            .unwrap_or("DirectPut")
            .to_string();

        let has_s3 = props.get("S3DestinationConfiguration").is_some();
        let has_extended_s3 = props.get("ExtendedS3DestinationConfiguration").is_some();
        if has_s3 && has_extended_s3 {
            return Err("Only one of S3DestinationConfiguration or ExtendedS3DestinationConfiguration may be set".to_string());
        }
        let destination = Some(if let Some(s3) = props.get("S3DestinationConfiguration") {
            parse_firehose_s3_destination(s3)?
        } else if let Some(s3) = props.get("ExtendedS3DestinationConfiguration") {
            parse_firehose_s3_destination(s3)?
        } else {
            return Err("Delivery stream requires a destination configuration".to_string());
        });

        let mut tags = BTreeMap::new();
        if let Some(arr) = props.get("Tags").and_then(|v| v.as_array()) {
            for tag in arr {
                if let (Some(k), Some(v)) = (
                    tag.get("Key").and_then(|v| v.as_str()),
                    tag.get("Value").and_then(|v| v.as_str()),
                ) {
                    tags.insert(k.to_string(), v.to_string());
                }
            }
        }

        let stream = DeliveryStream {
            name: name.clone(),
            arn: arn.clone(),
            status: "ACTIVE".to_string(),
            stream_type: stream_type.clone(),
            created_at: Utc::now(),
            last_update: Utc::now(),
            version_id: "1".to_string(),
            destination,
            tags,
        };

        let mut state = self.firehose_state.write();
        let account = state.get_or_create(&self.account_id, &self.region);
        account
            .streams_mut(&self.region)
            .insert(name.clone(), stream);

        let mut attributes = BTreeMap::new();
        attributes.insert("Arn".to_string(), arn.clone());
        attributes.insert("DeliveryStreamName".to_string(), name.clone());

        Ok(ProvisionResult {
            physical_id: name,
            attributes,
        })
    }

    fn delete_firehose_delivery_stream(&self, physical_id: &str) -> Result<(), String> {
        let mut state = self.firehose_state.write();
        let account = state.get_or_create(&self.account_id, &self.region);
        account.streams_mut(&self.region).remove(physical_id);
        Ok(())
    }

    // --- Cognito ---

    fn create_cognito_user_pool(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let pool_name = props
            .get("PoolName")
            .and_then(|v| v.as_str())
            .unwrap_or(&resource.logical_id)
            .to_string();

        let pool_id = format!(
            "{}_{}",
            self.region,
            Uuid::new_v4()
                .simple()
                .to_string()
                .chars()
                .take(9)
                .collect::<String>()
        );
        let arn = format!(
            "arn:aws:cognito-idp:{}:{}:userpool/{}",
            self.region, self.account_id, pool_id
        );
        let now = Utc::now();

        let password_policy = parse_cognito_password_policy(props.get("Policies"));
        let auto_verified = parse_cognito_string_array(props.get("AutoVerifiedAttributes"));
        let username_attributes = props
            .get("UsernameAttributes")
            .and_then(|v| v.as_array())
            .map(|_| parse_cognito_string_array(props.get("UsernameAttributes")));
        let alias_attributes = props
            .get("AliasAttributes")
            .and_then(|v| v.as_array())
            .map(|_| parse_cognito_string_array(props.get("AliasAttributes")));
        let mut schema_attributes = default_schema_attributes();
        if let Some(arr) = props.get("Schema").and_then(|v| v.as_array()) {
            for attr in arr {
                if let Some(parsed) = parse_cognito_schema_attribute(attr) {
                    if !schema_attributes.iter().any(|a| a.name == parsed.name) {
                        schema_attributes.push(parsed);
                    }
                }
            }
        }
        let mfa_configuration = props
            .get("MfaConfiguration")
            .and_then(|v| v.as_str())
            .unwrap_or("OFF")
            .to_string();
        let user_pool_tier = props
            .get("UserPoolTier")
            .and_then(|v| v.as_str())
            .unwrap_or("ESSENTIALS")
            .to_string();
        let deletion_protection = props
            .get("DeletionProtection")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let user_pool_tags = parse_cognito_tags(props.get("UserPoolTags"));
        let email_configuration =
            parse_cognito_email_configuration(props.get("EmailConfiguration"));
        let sms_configuration = parse_cognito_sms_configuration(props.get("SmsConfiguration"));
        let admin_create_user_config =
            parse_cognito_admin_create_user_config(props.get("AdminCreateUserConfig"));
        let account_recovery_setting =
            parse_cognito_account_recovery(props.get("AccountRecoverySetting"));

        // Generate the RSA-2048 keypair eagerly. The kid is derived
        // from a SHA-256 of the public SPKI DER so it stays stable
        // across snapshots and matches the JWKS document.
        let signing = fakecloud_cognito::jwt::generate_pool_signing_key();
        let signing_key_pem = signing.private_key_pem;
        let signing_kid = signing.kid;
        let pool = UserPool {
            id: pool_id.clone(),
            name: pool_name,
            arn: arn.clone(),
            status: "ACTIVE".to_string(),
            creation_date: now,
            last_modified_date: now,
            policies: PoolPolicies {
                password_policy,
                sign_in_policy: SignInPolicy {
                    allowed_first_auth_factors: vec!["PASSWORD".to_string()],
                },
            },
            auto_verified_attributes: auto_verified,
            username_attributes,
            alias_attributes,
            schema_attributes,
            lambda_config: None,
            mfa_configuration,
            email_configuration,
            sms_configuration,
            admin_create_user_config,
            user_pool_tags,
            account_recovery_setting,
            deletion_protection,
            estimated_number_of_users: 0,
            software_token_mfa_configuration: None,
            sms_mfa_configuration: None,
            user_pool_tier,
            verification_message_template: None,
            signing_key_pem: Some(signing_key_pem),
            signing_kid: Some(signing_kid),
            email_verification_message: None,
            email_verification_subject: None,
            sms_verification_message: None,
            sms_authentication_message: None,
            device_configuration: None,
            user_attribute_update_settings: None,
            user_pool_add_ons: None,
            username_configuration: None,
        };

        let mut accounts = self.cognito_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.user_pools.insert(pool_id.clone(), pool);

        let provider_name = format!("cognito-idp.{}.amazonaws.com/{}", self.region, pool_id);
        let provider_url = format!("https://{provider_name}");

        Ok(ProvisionResult::new(pool_id.clone())
            .with("Arn", arn)
            .with("ProviderName", provider_name)
            .with("ProviderURL", provider_url)
            .with("UserPoolId", pool_id))
    }

    fn delete_cognito_user_pool(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.cognito_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.user_pools.remove(physical_id);
        // Cascade: drop clients tied to this pool, plus per-pool side maps.
        state
            .user_pool_clients
            .retain(|_, c| c.user_pool_id != physical_id);
        state.users.remove(physical_id);
        state.groups.remove(physical_id);
        state.user_groups.remove(physical_id);
        state.identity_providers.remove(physical_id);
        state.resource_servers.remove(physical_id);
        state.import_jobs.remove(physical_id);
        state.domains.retain(|_, d| d.user_pool_id != physical_id);
        Ok(())
    }

    fn create_cognito_user_pool_client(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let pool_id = props
            .get("UserPoolId")
            .and_then(|v| v.as_str())
            .ok_or_else(|| "UserPoolId is required".to_string())?
            .to_string();
        let client_name = props
            .get("ClientName")
            .and_then(|v| v.as_str())
            .unwrap_or(&resource.logical_id)
            .to_string();

        let mut accounts = self.cognito_state.write();
        let state = accounts.get_or_create(&self.account_id);
        if !state.user_pools.contains_key(&pool_id) {
            // Force CFN to retry once UserPool resource provisions.
            return Err(format!(
                "User pool {pool_id} does not exist yet — retry once it has been provisioned"
            ));
        }

        let client_id: String = format!("{}{}", Uuid::new_v4().simple(), Uuid::new_v4().simple())
            .chars()
            .filter(|c| c.is_ascii_alphanumeric())
            .take(26)
            .collect::<String>()
            .to_lowercase();
        let generate_secret = props
            .get("GenerateSecret")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let client_secret = if generate_secret {
            use base64::Engine;
            let mut bytes = Vec::with_capacity(48);
            for _ in 0..3 {
                bytes.extend_from_slice(Uuid::new_v4().as_bytes());
            }
            Some(
                base64::engine::general_purpose::STANDARD
                    .encode(&bytes)
                    .chars()
                    .take(51)
                    .collect(),
            )
        } else {
            None
        };

        let now = Utc::now();
        let client = UserPoolClient {
            client_id: client_id.clone(),
            client_name,
            user_pool_id: pool_id.clone(),
            client_secret: client_secret.clone(),
            explicit_auth_flows: parse_cognito_string_array(props.get("ExplicitAuthFlows")),
            token_validity_units: None,
            access_token_validity: props.get("AccessTokenValidity").and_then(|v| v.as_i64()),
            id_token_validity: props.get("IdTokenValidity").and_then(|v| v.as_i64()),
            refresh_token_validity: props.get("RefreshTokenValidity").and_then(|v| v.as_i64()),
            callback_urls: parse_cognito_string_array(props.get("CallbackURLs")),
            logout_urls: parse_cognito_string_array(props.get("LogoutURLs")),
            supported_identity_providers: parse_cognito_string_array(
                props.get("SupportedIdentityProviders"),
            ),
            allowed_o_auth_flows: parse_cognito_string_array(props.get("AllowedOAuthFlows")),
            allowed_o_auth_scopes: parse_cognito_string_array(props.get("AllowedOAuthScopes")),
            allowed_o_auth_flows_user_pool_client: props
                .get("AllowedOAuthFlowsUserPoolClient")
                .and_then(|v| v.as_bool())
                .unwrap_or(false),
            prevent_user_existence_errors: props
                .get("PreventUserExistenceErrors")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            read_attributes: parse_cognito_string_array(props.get("ReadAttributes")),
            write_attributes: parse_cognito_string_array(props.get("WriteAttributes")),
            creation_date: now,
            last_modified_date: now,
            enable_token_revocation: props
                .get("EnableTokenRevocation")
                .and_then(|v| v.as_bool())
                .unwrap_or(true),
            auth_session_validity: props.get("AuthSessionValidity").and_then(|v| v.as_i64()),
            client_secrets: Vec::new(),
            refresh_token_rotation: None,
        };

        state.user_pool_clients.insert(client_id.clone(), client);

        let mut result = ProvisionResult::new(client_id.clone())
            .with("ClientId", client_id.clone())
            .with("Name", client_id);
        if let Some(secret) = client_secret {
            result = result.with("ClientSecret", secret);
        }
        Ok(result)
    }

    fn delete_cognito_user_pool_client(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.cognito_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.user_pool_clients.remove(physical_id);
        Ok(())
    }

    fn create_cognito_user_pool_domain(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let domain = props
            .get("Domain")
            .and_then(|v| v.as_str())
            .ok_or_else(|| "Domain is required".to_string())?
            .to_string();
        let pool_id = props
            .get("UserPoolId")
            .and_then(|v| v.as_str())
            .ok_or_else(|| "UserPoolId is required".to_string())?
            .to_string();
        let custom_domain_config = props
            .get("CustomDomainConfig")
            .and_then(|v| v.as_object())
            .and_then(|m| {
                m.get("CertificateArn")
                    .and_then(|v| v.as_str())
                    .map(|s| CustomDomainConfig {
                        certificate_arn: s.to_string(),
                    })
            });

        let mut accounts = self.cognito_state.write();
        let state = accounts.get_or_create(&self.account_id);
        if !state.user_pools.contains_key(&pool_id) {
            return Err(format!(
                "User pool {pool_id} does not exist yet — retry once it has been provisioned"
            ));
        }
        if state.domains.contains_key(&domain) {
            return Err(format!("Domain {domain} already exists"));
        }
        state.domains.insert(
            domain.clone(),
            UserPoolDomain {
                user_pool_id: pool_id,
                domain: domain.clone(),
                status: "ACTIVE".to_string(),
                custom_domain_config: custom_domain_config.clone(),
                creation_date: Utc::now(),
            },
        );

        let cloudfront_distribution = if custom_domain_config.is_some() {
            format!("{domain}.cloudfront.net")
        } else {
            format!("{domain}.auth.{}.amazoncognito.com", self.region)
        };

        Ok(ProvisionResult::new(domain.clone())
            .with("Domain", domain)
            .with("CloudFrontDistribution", cloudfront_distribution))
    }

    fn delete_cognito_user_pool_domain(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.cognito_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.domains.remove(physical_id);
        Ok(())
    }

    fn create_cognito_identity_pool(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let identity_pool_name = props
            .get("IdentityPoolName")
            .and_then(|v| v.as_str())
            .unwrap_or(&resource.logical_id)
            .to_string();
        let allow_unauth = props
            .get("AllowUnauthenticatedIdentities")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let allow_classic = props
            .get("AllowClassicFlow")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let developer_provider_name = props
            .get("DeveloperProviderName")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let cognito_identity_providers = props
            .get("CognitoIdentityProviders")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|p| {
                        let obj = p.as_object()?;
                        let provider_name = obj
                            .get("ProviderName")
                            .and_then(|v| v.as_str())?
                            .to_string();
                        let client_id = obj.get("ClientId").and_then(|v| v.as_str())?.to_string();
                        let server_side_token_check = obj
                            .get("ServerSideTokenCheck")
                            .and_then(|v| v.as_bool())
                            .unwrap_or(false);
                        Some(CognitoIdentityProvider {
                            provider_name,
                            client_id,
                            server_side_token_check,
                        })
                    })
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        let open_id_connect_provider_arns =
            parse_cognito_string_array(props.get("OpenIdConnectProviderARNs"));
        let saml_provider_arns = parse_cognito_string_array(props.get("SamlProviderARNs"));
        let supported_login_providers = props
            .get("SupportedLoginProviders")
            .and_then(|v| v.as_object())
            .map(|m| {
                m.iter()
                    .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                    .collect()
            })
            .unwrap_or_default();
        let identity_pool_tags = parse_cognito_tags(props.get("IdentityPoolTags"));
        let cognito_streams = props.get("CognitoStreams").cloned();
        let push_sync = props.get("PushSync").cloned();

        // Real Cognito identity pool ids look like `<region>:<uuid>`. Match
        // that shape so SDKs that parse it don't choke.
        let identity_pool_id = format!("{}:{}", self.region, Uuid::new_v4());

        let pool = IdentityPool {
            identity_pool_id: identity_pool_id.clone(),
            identity_pool_name,
            allow_unauthenticated_identities: allow_unauth,
            allow_classic_flow: allow_classic,
            developer_provider_name,
            cognito_identity_providers,
            open_id_connect_provider_arns,
            saml_provider_arns,
            supported_login_providers,
            cognito_streams,
            push_sync,
            identity_pool_tags,
            creation_date: Utc::now(),
        };

        let mut accounts = self.cognito_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.identity_pools.insert(identity_pool_id.clone(), pool);

        Ok(ProvisionResult::new(identity_pool_id.clone()).with("Name", identity_pool_id))
    }

    fn delete_cognito_identity_pool(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.cognito_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.identity_pools.remove(physical_id);
        // Cascade: drop role attachments tied to this pool.
        state
            .identity_pool_role_attachments
            .retain(|_, a| a.identity_pool_id != physical_id);
        Ok(())
    }

    fn create_cognito_identity_pool_role_attachment(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let identity_pool_id = props
            .get("IdentityPoolId")
            .and_then(|v| v.as_str())
            .ok_or_else(|| "IdentityPoolId is required".to_string())?
            .to_string();

        let mut accounts = self.cognito_state.write();
        let state = accounts.get_or_create(&self.account_id);
        if !state.identity_pools.contains_key(&identity_pool_id) {
            return Err(format!(
                "Identity pool {identity_pool_id} does not exist yet — retry once it has been provisioned"
            ));
        }

        let roles = props
            .get("Roles")
            .and_then(|v| v.as_object())
            .map(|m| {
                m.iter()
                    .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                    .collect()
            })
            .unwrap_or_default();
        let role_mappings = props
            .get("RoleMappings")
            .and_then(|v| v.as_object())
            .map(|m| m.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
            .unwrap_or_default();

        let attachment_id = Uuid::new_v4().simple().to_string();
        let physical_id = format!("{identity_pool_id}:{attachment_id}");
        let attachment = IdentityPoolRoleAttachment {
            identity_pool_id: identity_pool_id.clone(),
            attachment_id,
            roles,
            role_mappings,
        };
        state
            .identity_pool_role_attachments
            .insert(physical_id.clone(), attachment);

        Ok(ProvisionResult::new(physical_id))
    }

    fn delete_cognito_identity_pool_role_attachment(
        &self,
        physical_id: &str,
    ) -> Result<(), String> {
        let mut accounts = self.cognito_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.identity_pool_role_attachments.remove(physical_id);
        Ok(())
    }

    // --- RDS ---

    fn create_rds_subnet_group(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let name = props
            .get("DBSubnetGroupName")
            .and_then(|v| v.as_str())
            .unwrap_or(&resource.logical_id)
            .to_string();
        let description = props
            .get("DBSubnetGroupDescription")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let subnet_ids: Vec<String> = props
            .get("SubnetIds")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                    .collect()
            })
            .unwrap_or_default();
        let tags = parse_rds_tags(props.get("Tags"));
        let mut accounts = self.rds_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let arn = state.db_subnet_group_arn(&name);
        let group = DbSubnetGroup {
            db_subnet_group_name: name.clone(),
            db_subnet_group_arn: arn.clone(),
            db_subnet_group_description: description,
            vpc_id: String::new(),
            subnet_ids,
            subnet_availability_zones: Vec::new(),
            tags,
        };
        state.subnet_groups.insert(name.clone(), group);
        Ok(ProvisionResult::new(name).with("Arn", arn))
    }

    fn delete_rds_subnet_group(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.rds_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.subnet_groups.remove(physical_id);
        Ok(())
    }

    fn create_rds_parameter_group(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let name = props
            .get("DBParameterGroupName")
            .and_then(|v| v.as_str())
            .unwrap_or(&resource.logical_id)
            .to_string();
        let family = props
            .get("Family")
            .and_then(|v| v.as_str())
            .unwrap_or("postgres16")
            .to_string();
        let description = props
            .get("Description")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let parameters: std::collections::BTreeMap<String, String> = props
            .get("Parameters")
            .and_then(|v| v.as_object())
            .map(|m| {
                m.iter()
                    .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                    .collect()
            })
            .unwrap_or_default();
        let tags = parse_rds_tags(props.get("Tags"));

        let mut accounts = self.rds_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let arn = state.db_parameter_group_arn(&name);
        let group = DbParameterGroup {
            db_parameter_group_name: name.clone(),
            db_parameter_group_arn: arn.clone(),
            db_parameter_group_family: family,
            description,
            parameters,
            tags,
        };
        state.parameter_groups.insert(name.clone(), group);
        Ok(ProvisionResult::new(name).with("Arn", arn))
    }

    fn delete_rds_parameter_group(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.rds_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.parameter_groups.remove(physical_id);
        Ok(())
    }

    fn create_rds_cluster_parameter_group(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let name = props
            .get("DBClusterParameterGroupName")
            .or_else(|| props.get("Name"))
            .and_then(|v| v.as_str())
            .unwrap_or(&resource.logical_id)
            .to_string();
        let family = props
            .get("Family")
            .and_then(|v| v.as_str())
            .unwrap_or("aurora-postgresql15")
            .to_string();
        let description = props
            .get("Description")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let arn = format!(
            "arn:aws:rds:{}:{}:cluster-pg:{}",
            self.region, self.account_id, name
        );
        let entry = serde_json::json!({
            "DBClusterParameterGroupName": name,
            "DBClusterParameterGroupArn": arn,
            "DBParameterGroupFamily": family,
            "Description": description,
        });
        let mut accounts = self.rds_state.write();
        let state = accounts.get_or_create(&self.account_id);
        rds_extras_mut(state, "cluster_param_groups").insert(name.clone(), entry);
        Ok(ProvisionResult::new(name).with("Arn", arn))
    }

    fn delete_rds_cluster_parameter_group(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.rds_state.write();
        let state = accounts.get_or_create(&self.account_id);
        if let Some(m) = state.extras.get_mut("cluster_param_groups") {
            m.remove(physical_id);
        }
        Ok(())
    }

    fn create_rds_option_group(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let name = props
            .get("OptionGroupName")
            .and_then(|v| v.as_str())
            .unwrap_or(&resource.logical_id)
            .to_string();
        let engine_name = props
            .get("EngineName")
            .and_then(|v| v.as_str())
            .unwrap_or("mysql")
            .to_string();
        let major_engine_version = props
            .get("MajorEngineVersion")
            .and_then(|v| v.as_str())
            .unwrap_or("8.0")
            .to_string();
        let description = props
            .get("OptionGroupDescription")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let arn = format!(
            "arn:aws:rds:{}:{}:og:{}",
            self.region, self.account_id, name
        );
        let entry = serde_json::json!({
            "OptionGroupName": name,
            "OptionGroupArn": arn,
            "EngineName": engine_name,
            "MajorEngineVersion": major_engine_version,
            "OptionGroupDescription": description,
        });
        let mut accounts = self.rds_state.write();
        let state = accounts.get_or_create(&self.account_id);
        rds_extras_mut(state, "option_groups").insert(name.clone(), entry);
        Ok(ProvisionResult::new(name).with("Arn", arn))
    }

    fn delete_rds_option_group(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.rds_state.write();
        let state = accounts.get_or_create(&self.account_id);
        if let Some(m) = state.extras.get_mut("option_groups") {
            m.remove(physical_id);
        }
        Ok(())
    }

    fn create_rds_event_subscription(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let name = props
            .get("SubscriptionName")
            .and_then(|v| v.as_str())
            .unwrap_or(&resource.logical_id)
            .to_string();
        let sns_topic_arn = props
            .get("SnsTopicArn")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let entry = serde_json::json!({
            "CustSubscriptionId": name,
            "SnsTopicArn": sns_topic_arn,
            "Status": "active",
            "Enabled": props.get("Enabled").and_then(|v| v.as_bool()).unwrap_or(true),
        });
        let mut accounts = self.rds_state.write();
        let state = accounts.get_or_create(&self.account_id);
        rds_extras_mut(state, "event_subscriptions").insert(name.clone(), entry);
        Ok(ProvisionResult::new(name))
    }

    fn delete_rds_event_subscription(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.rds_state.write();
        let state = accounts.get_or_create(&self.account_id);
        if let Some(m) = state.extras.get_mut("event_subscriptions") {
            m.remove(physical_id);
        }
        Ok(())
    }

    fn create_rds_security_group(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let name = props
            .get("DBSecurityGroupName")
            .and_then(|v| v.as_str())
            .unwrap_or(&resource.logical_id)
            .to_string();
        let description = props
            .get("GroupDescription")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let entry = serde_json::json!({
            "DBSecurityGroupName": name,
            "DBSecurityGroupDescription": description,
            "OwnerId": self.account_id,
        });
        let mut accounts = self.rds_state.write();
        let state = accounts.get_or_create(&self.account_id);
        rds_extras_mut(state, "security_groups").insert(name.clone(), entry);
        Ok(ProvisionResult::new(name))
    }

    fn delete_rds_security_group(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.rds_state.write();
        let state = accounts.get_or_create(&self.account_id);
        if let Some(m) = state.extras.get_mut("security_groups") {
            m.remove(physical_id);
        }
        Ok(())
    }

    fn create_rds_db_proxy(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let name = props
            .get("DBProxyName")
            .and_then(|v| v.as_str())
            .unwrap_or(&resource.logical_id)
            .to_string();
        let engine_family = props
            .get("EngineFamily")
            .and_then(|v| v.as_str())
            .unwrap_or("POSTGRESQL")
            .to_string();
        let arn = format!(
            "arn:aws:rds:{}:{}:db-proxy:{}",
            self.region, self.account_id, name
        );
        let endpoint = format!("{name}.proxy-default.{}.rds.amazonaws.com", self.region);
        let entry = serde_json::json!({
            "DBProxyName": name,
            "DBProxyArn": arn,
            "Status": "available",
            "EngineFamily": engine_family,
            "Endpoint": endpoint,
        });
        let mut accounts = self.rds_state.write();
        let state = accounts.get_or_create(&self.account_id);
        rds_extras_mut(state, "proxies").insert(name.clone(), entry);
        Ok(ProvisionResult::new(name)
            .with("DBProxyArn", arn)
            .with("Endpoint", endpoint))
    }

    fn delete_rds_db_proxy(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.rds_state.write();
        let state = accounts.get_or_create(&self.account_id);
        if let Some(m) = state.extras.get_mut("proxies") {
            m.remove(physical_id);
        }
        Ok(())
    }

    fn create_rds_db_instance(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let identifier = props
            .get("DBInstanceIdentifier")
            .and_then(|v| v.as_str())
            .map(String::from)
            .unwrap_or_else(|| {
                format!(
                    "cfn-{}-{}",
                    resource.logical_id.to_lowercase(),
                    Uuid::new_v4().simple().to_string()[..8].to_lowercase()
                )
            });
        let class = props
            .get("DBInstanceClass")
            .and_then(|v| v.as_str())
            .unwrap_or("db.t4g.micro")
            .to_string();
        let engine = props
            .get("Engine")
            .and_then(|v| v.as_str())
            .unwrap_or("postgres")
            .to_string();
        let engine_version = props
            .get("EngineVersion")
            .and_then(|v| v.as_str())
            .unwrap_or("16.0")
            .to_string();
        let master_username = props
            .get("MasterUsername")
            .and_then(|v| v.as_str())
            .unwrap_or("admin")
            .to_string();
        let master_user_password = props
            .get("MasterUserPassword")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let db_name = props
            .get("DBName")
            .and_then(|v| v.as_str())
            .map(String::from);
        let port = props
            .get("Port")
            .and_then(|v| v.as_i64())
            .map(|n| n as i32)
            .unwrap_or(5432);
        let allocated_storage = props
            .get("AllocatedStorage")
            .and_then(|v| {
                v.as_i64()
                    .or_else(|| v.as_str().and_then(|s| s.parse::<i64>().ok()))
            })
            .map(|n| n as i32)
            .unwrap_or(20);
        let publicly_accessible = props
            .get("PubliclyAccessible")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let deletion_protection = props
            .get("DeletionProtection")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let backup_retention_period = props
            .get("BackupRetentionPeriod")
            .and_then(|v| v.as_i64())
            .map(|n| n as i32)
            .unwrap_or(0);
        let multi_az = props
            .get("MultiAZ")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let availability_zone = props
            .get("AvailabilityZone")
            .and_then(|v| v.as_str())
            .map(String::from);
        let storage_type = props
            .get("StorageType")
            .and_then(|v| v.as_str())
            .map(String::from);
        let storage_encrypted = props
            .get("StorageEncrypted")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let kms_key_id = props
            .get("KmsKeyId")
            .and_then(|v| v.as_str())
            .map(String::from);
        let iam_database_authentication_enabled = props
            .get("EnableIAMDatabaseAuthentication")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let db_parameter_group_name = props
            .get("DBParameterGroupName")
            .and_then(|v| v.as_str())
            .map(String::from);
        let option_group_name = props
            .get("OptionGroupName")
            .and_then(|v| v.as_str())
            .map(String::from);
        let vpc_security_group_ids: Vec<String> = props
            .get("VPCSecurityGroups")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();
        let enabled_cloudwatch_logs_exports: Vec<String> = props
            .get("EnableCloudwatchLogsExports")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();
        let tags = parse_rds_tags(props.get("Tags"));

        let mut accounts = self.rds_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let arn = state.db_instance_arn(&identifier);
        let endpoint_address = format!(
            "{identifier}.cluster-fakecloud.{}.rds.amazonaws.com",
            state.region
        );
        let dbi_resource_id = format!("db-{}", Uuid::new_v4().simple());
        let inst = DbInstance {
            db_instance_identifier: identifier.clone(),
            db_instance_arn: arn.clone(),
            db_instance_class: class,
            engine,
            engine_version,
            db_instance_status: "available".to_string(),
            master_username,
            db_name,
            endpoint_address,
            port,
            allocated_storage,
            publicly_accessible,
            deletion_protection,
            created_at: Utc::now(),
            dbi_resource_id,
            master_user_password,
            container_id: String::new(),
            host_port: 0,
            tags,
            read_replica_source_db_instance_identifier: None,
            read_replica_db_instance_identifiers: Vec::new(),
            vpc_security_group_ids,
            db_parameter_group_name,
            backup_retention_period,
            preferred_backup_window: "03:00-04:00".to_string(),
            preferred_maintenance_window: None,
            latest_restorable_time: None,
            option_group_name,
            multi_az,
            pending_modified_values: None,
            availability_zone,
            storage_type,
            storage_encrypted,
            kms_key_id,
            iam_database_authentication_enabled,
            iops: props.get("Iops").and_then(|v| v.as_i64()).map(|n| n as i32),
            monitoring_interval: props
                .get("MonitoringInterval")
                .and_then(|v| v.as_i64())
                .map(|n| n as i32),
            monitoring_role_arn: props
                .get("MonitoringRoleArn")
                .and_then(|v| v.as_str())
                .map(String::from),
            performance_insights_enabled: props
                .get("EnablePerformanceInsights")
                .and_then(|v| v.as_bool())
                .unwrap_or(false),
            performance_insights_kms_key_id: props
                .get("PerformanceInsightsKMSKeyId")
                .and_then(|v| v.as_str())
                .map(String::from),
            performance_insights_retention_period: props
                .get("PerformanceInsightsRetentionPeriod")
                .and_then(|v| v.as_i64())
                .map(|n| n as i32),
            enabled_cloudwatch_logs_exports,
            ca_certificate_identifier: props
                .get("CACertificateIdentifier")
                .and_then(|v| v.as_str())
                .map(String::from),
            network_type: props
                .get("NetworkType")
                .and_then(|v| v.as_str())
                .map(String::from),
            character_set_name: props
                .get("CharacterSetName")
                .and_then(|v| v.as_str())
                .map(String::from),
            auto_minor_version_upgrade: props
                .get("AutoMinorVersionUpgrade")
                .and_then(|v| v.as_bool()),
            copy_tags_to_snapshot: props.get("CopyTagsToSnapshot").and_then(|v| v.as_bool()),
            master_user_secret_arn: None,
            master_user_secret_kms_key_id: props
                .get("MasterUserSecret")
                .and_then(|v| v.get("KmsKeyId"))
                .and_then(|v| v.as_str())
                .map(String::from),
            license_model: props
                .get("LicenseModel")
                .and_then(|v| v.as_str())
                .map(String::from),
            max_allocated_storage: props
                .get("MaxAllocatedStorage")
                .and_then(|v| v.as_i64())
                .map(|n| n as i32),
            multi_tenant: props.get("MultiTenant").and_then(|v| v.as_bool()),
            storage_throughput: props
                .get("StorageThroughput")
                .and_then(|v| v.as_i64())
                .map(|n| n as i32),
            tde_credential_arn: props
                .get("TdeCredentialArn")
                .and_then(|v| v.as_str())
                .map(String::from),
            delete_automated_backups: props
                .get("DeleteAutomatedBackups")
                .and_then(|v| v.as_bool()),
            db_security_groups: props
                .get("DBSecurityGroups")
                .and_then(|v| v.as_array())
                .map(|arr| {
                    arr.iter()
                        .filter_map(|v| v.as_str().map(String::from))
                        .collect()
                })
                .unwrap_or_default(),
            domain: props
                .get("Domain")
                .and_then(|v| v.as_str())
                .map(String::from),
            domain_fqdn: props
                .get("DomainFqdn")
                .and_then(|v| v.as_str())
                .map(String::from),
            domain_ou: props
                .get("DomainOu")
                .and_then(|v| v.as_str())
                .map(String::from),
            domain_iam_role_name: props
                .get("DomainIAMRoleName")
                .and_then(|v| v.as_str())
                .map(String::from),
            domain_auth_secret_arn: props
                .get("DomainAuthSecretArn")
                .and_then(|v| v.as_str())
                .map(String::from),
            domain_dns_ips: props
                .get("DomainDnsIps")
                .and_then(|v| v.as_array())
                .map(|arr| {
                    arr.iter()
                        .filter_map(|v| v.as_str().map(String::from))
                        .collect()
                })
                .unwrap_or_default(),
            db_cluster_identifier: props
                .get("DBClusterIdentifier")
                .and_then(|v| v.as_str())
                .map(String::from),
        };
        let endpoint = inst.endpoint_address.clone();
        let endpoint_port = inst.port;
        state.instances.insert(identifier.clone(), inst);

        Ok(ProvisionResult::new(identifier.clone())
            .with("DBInstanceArn", arn)
            .with("Endpoint.Address", endpoint)
            .with("Endpoint.Port", endpoint_port.to_string())
            .with("DbiResourceId", format!("db-{identifier}")))
    }

    fn delete_rds_db_instance(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.rds_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.instances.remove(physical_id);
        Ok(())
    }

    fn create_rds_db_cluster(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let identifier = props
            .get("DBClusterIdentifier")
            .and_then(|v| v.as_str())
            .map(String::from)
            .unwrap_or_else(|| {
                format!(
                    "cfn-cluster-{}-{}",
                    resource.logical_id.to_lowercase(),
                    Uuid::new_v4().simple().to_string()[..8].to_lowercase()
                )
            });
        let engine = props
            .get("Engine")
            .and_then(|v| v.as_str())
            .unwrap_or("aurora-postgresql")
            .to_string();
        let engine_version = props
            .get("EngineVersion")
            .and_then(|v| v.as_str())
            .map(String::from);
        let master_username = props
            .get("MasterUsername")
            .and_then(|v| v.as_str())
            .map(String::from);
        let port = props.get("Port").and_then(|v| v.as_i64()).unwrap_or(5432);
        let mut accounts = self.rds_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let arn = format!(
            "arn:aws:rds:{}:{}:cluster:{}",
            state.region, state.account_id, identifier
        );
        let cluster_resource_id = format!("cluster-{}", Uuid::new_v4().simple());
        let endpoint = format!(
            "{identifier}.cluster-fakecloud.{}.rds.amazonaws.com",
            state.region
        );
        let reader_endpoint = format!(
            "{identifier}.cluster-ro-fakecloud.{}.rds.amazonaws.com",
            state.region
        );
        let body = serde_json::json!({
            "DBClusterIdentifier": identifier,
            "DBClusterArn": arn,
            "Engine": engine,
            "EngineVersion": engine_version,
            "MasterUsername": master_username,
            "Status": "available",
            "DbClusterResourceId": cluster_resource_id,
            "Endpoint": endpoint,
            "ReaderEndpoint": reader_endpoint,
            "Port": port,
            "AllocatedStorage": props.get("AllocatedStorage").and_then(|v| v.as_i64()).unwrap_or(1),
            "BackupRetentionPeriod": props.get("BackupRetentionPeriod").and_then(|v| v.as_i64()).unwrap_or(1),
            "DatabaseName": props.get("DatabaseName").and_then(|v| v.as_str()),
            "DBSubnetGroup": props.get("DBSubnetGroupName").and_then(|v| v.as_str()),
            "VpcSecurityGroupIds": props.get("VpcSecurityGroupIds").cloned().unwrap_or(serde_json::json!([])),
            "StorageEncrypted": props.get("StorageEncrypted").and_then(|v| v.as_bool()).unwrap_or(false),
            "KmsKeyId": props.get("KmsKeyId").and_then(|v| v.as_str()),
            "DeletionProtection": props.get("DeletionProtection").and_then(|v| v.as_bool()).unwrap_or(false),
            "ClusterCreateTime": Utc::now().to_rfc3339(),
            "EnabledCloudwatchLogsExports": props.get("EnableCloudwatchLogsExports").cloned().unwrap_or(serde_json::json!([])),
            "MultiAZ": false,
            "DBClusterMembers": [],
        });
        state
            .extras
            .entry("clusters".to_string())
            .or_default()
            .insert(identifier.clone(), body);
        Ok(ProvisionResult::new(identifier.clone())
            .with("DBClusterArn", arn)
            .with("Endpoint.Address", endpoint)
            .with("ReadEndpoint.Address", reader_endpoint)
            .with("Endpoint.Port", port.to_string())
            .with("DBClusterResourceId", cluster_resource_id))
    }

    fn delete_rds_db_cluster(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.rds_state.write();
        let state = accounts.get_or_create(&self.account_id);
        if let Some(m) = state.extras.get_mut("clusters") {
            m.remove(physical_id);
        }
        Ok(())
    }

    // --- ECS ---

    fn create_ecs_cluster(&self, resource: &ResourceDefinition) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let cluster_name = props
            .get("ClusterName")
            .and_then(|v| v.as_str())
            .unwrap_or(&resource.logical_id)
            .to_string();
        let cluster_arn = format!(
            "arn:aws:ecs:{}:{}:cluster/{}",
            self.region, self.account_id, cluster_name
        );
        let mut cluster = EcsCluster::new(&cluster_name, cluster_arn.clone());
        cluster.tags = parse_ecs_tags(props.get("Tags"));
        cluster.capacity_providers = props
            .get("CapacityProviders")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                    .collect()
            })
            .unwrap_or_default();
        if let Some(strategy) = props
            .get("DefaultCapacityProviderStrategy")
            .and_then(|v| v.as_array())
        {
            cluster.default_capacity_provider_strategy =
                strategy.iter().cloned().map(lowercase_first_keys).collect();
        }
        if let Some(cfg) = props.get("Configuration") {
            cluster.configuration = Some(lowercase_first_keys(cfg.clone()));
        }
        if let Some(settings) = props.get("ClusterSettings").and_then(|v| v.as_array()) {
            cluster.settings = settings.iter().cloned().map(lowercase_first_keys).collect();
        }
        if let Some(scd) = props.get("ServiceConnectDefaults") {
            cluster.service_connect_defaults = Some(lowercase_first_keys(scd.clone()));
        }
        let mut accounts = self.ecs_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.clusters.insert(cluster_name.clone(), cluster);
        Ok(ProvisionResult::new(cluster_name).with("Arn", cluster_arn))
    }

    fn delete_ecs_cluster(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.ecs_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.clusters.remove(physical_id);
        // Cascade: drop services + tasks tied to this cluster.
        state.services.retain(|_, s| s.cluster_name != physical_id);
        state
            .tasks
            .retain(|_, t| t.cluster_arn.split('/').next_back() != Some(physical_id));
        Ok(())
    }

    fn create_ecs_task_definition(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let family = props
            .get("Family")
            .and_then(|v| v.as_str())
            .unwrap_or(&resource.logical_id)
            .to_string();
        // ECS DescribeTaskDefinition emits camelCase keys; CFN ships PascalCase.
        // Recursively lower the leading char so SDKs deserialize cleanly.
        let container_definitions: Vec<serde_json::Value> = props
            .get("ContainerDefinitions")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .map(lowercase_first_keys)
            .collect();
        let task_role_arn = props
            .get("TaskRoleArn")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let execution_role_arn = props
            .get("ExecutionRoleArn")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let network_mode = props
            .get("NetworkMode")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let requires_compatibilities: Vec<String> = props
            .get("RequiresCompatibilities")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                    .collect()
            })
            .unwrap_or_default();
        let cpu = props
            .get("Cpu")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let memory = props
            .get("Memory")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let volumes: Vec<serde_json::Value> = props
            .get("Volumes")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .map(lowercase_first_keys)
            .collect();
        let placement_constraints: Vec<serde_json::Value> = props
            .get("PlacementConstraints")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .map(lowercase_first_keys)
            .collect();
        let proxy_configuration = props
            .get("ProxyConfiguration")
            .cloned()
            .map(lowercase_first_keys);
        let ephemeral_storage = props
            .get("EphemeralStorage")
            .cloned()
            .map(lowercase_first_keys);
        let runtime_platform = props
            .get("RuntimePlatform")
            .cloned()
            .map(lowercase_first_keys);
        let tags = parse_ecs_tags(props.get("Tags"));

        let mut accounts = self.ecs_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let revision = state
            .next_revision
            .entry(family.clone())
            .and_modify(|n| *n += 1)
            .or_insert(1);
        let revision = *revision;
        let arn = format!(
            "arn:aws:ecs:{}:{}:task-definition/{}:{}",
            self.region, self.account_id, family, revision
        );
        let td = EcsTaskDefinition {
            family: family.clone(),
            revision,
            task_definition_arn: arn.clone(),
            container_definitions,
            status: "ACTIVE".to_string(),
            task_role_arn,
            execution_role_arn,
            network_mode,
            requires_compatibilities: requires_compatibilities.clone(),
            compatibilities: requires_compatibilities,
            cpu,
            memory,
            pid_mode: None,
            ipc_mode: None,
            volumes,
            placement_constraints,
            proxy_configuration,
            inference_accelerators: Vec::new(),
            ephemeral_storage,
            runtime_platform,
            requires_attributes: Vec::new(),
            registered_at: Utc::now(),
            registered_by: None,
            deregistered_at: None,
            tags,
            enable_fault_injection: props.get("EnableFaultInjection").and_then(|v| v.as_bool()),
        };
        state
            .task_definitions
            .entry(family.clone())
            .or_default()
            .insert(revision, td);
        Ok(ProvisionResult::new(arn.clone()).with("TaskDefinitionArn", arn))
    }

    fn delete_ecs_task_definition(&self, physical_id: &str) -> Result<(), String> {
        // physical_id is the full task-definition ARN; family + revision
        // sit at the trailing segment after `/`.
        let Some(suffix) = physical_id.rsplit('/').next() else {
            return Ok(());
        };
        let Some((family, rev)) = suffix.split_once(':') else {
            return Ok(());
        };
        let Ok(revision) = rev.parse::<i32>() else {
            return Ok(());
        };
        let mut accounts = self.ecs_state.write();
        let state = accounts.get_or_create(&self.account_id);
        if let Some(revs) = state.task_definitions.get_mut(family) {
            if let Some(td) = revs.get_mut(&revision) {
                td.status = "INACTIVE".to_string();
                td.deregistered_at = Some(Utc::now());
            }
        }
        Ok(())
    }

    fn create_ecs_service(&self, resource: &ResourceDefinition) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let service_name = props
            .get("ServiceName")
            .and_then(|v| v.as_str())
            .unwrap_or(&resource.logical_id)
            .to_string();
        // Cluster: default to "default" if missing; accept name or ARN.
        let cluster_name = props
            .get("Cluster")
            .and_then(|v| v.as_str())
            .map(parse_ecs_cluster_name)
            .unwrap_or_else(|| "default".to_string());
        let task_definition_arn = props
            .get("TaskDefinition")
            .and_then(|v| v.as_str())
            .ok_or_else(|| "TaskDefinition is required".to_string())?
            .to_string();
        let desired_count = props
            .get("DesiredCount")
            .and_then(cfn_as_i64)
            .map(|n| n as i32)
            .unwrap_or(1);
        let launch_type = props
            .get("LaunchType")
            .and_then(|v| v.as_str())
            .unwrap_or("FARGATE")
            .to_string();
        let scheduling_strategy = props
            .get("SchedulingStrategy")
            .and_then(|v| v.as_str())
            .unwrap_or("REPLICA")
            .to_string();
        let deployment_controller = props
            .get("DeploymentController")
            .and_then(|v| v.get("Type"))
            .and_then(|v| v.as_str())
            .unwrap_or("ECS")
            .to_string();
        let load_balancers: Vec<serde_json::Value> = props
            .get("LoadBalancers")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .map(lowercase_first_keys)
            .collect();
        let service_registries: Vec<serde_json::Value> = props
            .get("ServiceRegistries")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .map(lowercase_first_keys)
            .collect();
        let placement_constraints: Vec<serde_json::Value> = props
            .get("PlacementConstraints")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .map(lowercase_first_keys)
            .collect();
        let placement_strategy: Vec<serde_json::Value> = props
            .get("PlacementStrategies")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .map(lowercase_first_keys)
            .collect();
        let network_configuration = props
            .get("NetworkConfiguration")
            .cloned()
            .map(lowercase_first_keys);
        let role_arn = props
            .get("Role")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let platform_version = props
            .get("PlatformVersion")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let health_check_grace_period_seconds = props
            .get("HealthCheckGracePeriodSeconds")
            .and_then(cfn_as_i64)
            .map(|n| n as i32);
        let enable_ecs_managed_tags = props
            .get("EnableECSManagedTags")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let enable_execute_command = props
            .get("EnableExecuteCommand")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let propagate_tags = props
            .get("PropagateTags")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let capacity_provider_strategy: Vec<serde_json::Value> = props
            .get("CapacityProviderStrategy")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .map(lowercase_first_keys)
            .collect();
        let availability_zone_rebalancing = props
            .get("AvailabilityZoneRebalancing")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let tags = parse_ecs_tags(props.get("Tags"));

        // Family + revision parsed from the task definition ARN tail.
        let (family, revision) = parse_td_arn(&task_definition_arn);

        let mut accounts = self.ecs_state.write();
        let state = accounts.get_or_create(&self.account_id);
        if !state.clusters.contains_key(&cluster_name) {
            return Err(format!(
                "Cluster {cluster_name} does not exist yet — retry once it has been provisioned"
            ));
        }
        let cluster_arn = state.clusters[&cluster_name].cluster_arn.clone();
        let service_arn = format!(
            "arn:aws:ecs:{}:{}:service/{}/{}",
            self.region, self.account_id, cluster_name, service_name
        );
        let key = format!("{cluster_name}/{service_name}");
        let service = EcsService {
            service_name: service_name.clone(),
            service_arn: service_arn.clone(),
            cluster_name: cluster_name.clone(),
            cluster_arn,
            task_definition_arn,
            family,
            revision,
            desired_count,
            running_count: 0,
            pending_count: 0,
            launch_type,
            status: "ACTIVE".to_string(),
            scheduling_strategy,
            deployment_controller,
            minimum_healthy_percent: props
                .get("DeploymentConfiguration")
                .and_then(|v| v.get("MinimumHealthyPercent"))
                .and_then(cfn_as_i64)
                .map(|n| n as i32),
            maximum_percent: props
                .get("DeploymentConfiguration")
                .and_then(|v| v.get("MaximumPercent"))
                .and_then(cfn_as_i64)
                .map(|n| n as i32),
            circuit_breaker: None,
            deployments: Vec::new(),
            load_balancers,
            service_registries,
            placement_constraints,
            placement_strategy,
            network_configuration,
            tags,
            created_at: Utc::now(),
            created_by: None,
            role_arn,
            platform_version,
            health_check_grace_period_seconds,
            enable_execute_command,
            enable_ecs_managed_tags,
            propagate_tags,
            capacity_provider_strategy,
            availability_zone_rebalancing,
            volume_configurations: Vec::new(),
        };
        state.services.insert(key.clone(), service);
        if let Some(c) = state.clusters.get_mut(&cluster_name) {
            c.active_services_count += 1;
        }
        Ok(ProvisionResult::new(service_arn.clone())
            .with("Name", service_name)
            .with("ServiceArn", service_arn))
    }

    fn delete_ecs_service(&self, physical_id: &str) -> Result<(), String> {
        // physical_id is full Service ARN: .../service/<cluster>/<service>
        let Some((cluster, service)) = parse_service_arn(physical_id) else {
            return Ok(());
        };
        let key = format!("{cluster}/{service}");
        let mut accounts = self.ecs_state.write();
        let state = accounts.get_or_create(&self.account_id);
        if state.services.remove(&key).is_some() {
            if let Some(c) = state.clusters.get_mut(&cluster) {
                if c.active_services_count > 0 {
                    c.active_services_count -= 1;
                }
            }
        }
        Ok(())
    }

    fn create_ecs_capacity_provider(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let name = props
            .get("Name")
            .and_then(|v| v.as_str())
            .unwrap_or(&resource.logical_id)
            .to_string();
        let arn = format!(
            "arn:aws:ecs:{}:{}:capacity-provider/{}",
            self.region, self.account_id, name
        );
        let cp = EcsCapacityProvider {
            name: name.clone(),
            arn: arn.clone(),
            status: "ACTIVE".to_string(),
            auto_scaling_group_provider: props.get("AutoScalingGroupProvider").cloned(),
            update_status: None,
            update_status_reason: None,
            created_at: Utc::now(),
            tags: parse_ecs_tags(props.get("Tags")),
        };
        let mut accounts = self.ecs_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.capacity_providers.insert(name.clone(), cp);
        Ok(ProvisionResult::new(name).with("Arn", arn))
    }

    fn delete_ecs_capacity_provider(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.ecs_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.capacity_providers.remove(physical_id);
        Ok(())
    }

    /// In-place update for AWS::ECS::Cluster. ClusterName is immutable —
    /// CFN replaces the resource if it changes — so we only refresh
    /// settings/configuration/capacity-provider/tags here. The physical
    /// id is kept stable.
    fn update_ecs_cluster(
        &self,
        existing: &StackResource,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let cluster_name = existing.physical_id.clone();
        let mut accounts = self.ecs_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let cluster = state
            .clusters
            .get_mut(&cluster_name)
            .ok_or_else(|| format!("Cluster {cluster_name} no longer exists"))?;
        if let Some(settings) = props.get("ClusterSettings").and_then(|v| v.as_array()) {
            cluster.settings = settings.iter().cloned().map(lowercase_first_keys).collect();
        }
        if let Some(cfg) = props.get("Configuration") {
            cluster.configuration = Some(lowercase_first_keys(cfg.clone()));
        }
        if let Some(cps) = props.get("CapacityProviders").and_then(|v| v.as_array()) {
            cluster.capacity_providers = cps
                .iter()
                .filter_map(|v| v.as_str().map(|s| s.to_string()))
                .collect();
        }
        if let Some(strategy) = props
            .get("DefaultCapacityProviderStrategy")
            .and_then(|v| v.as_array())
        {
            cluster.default_capacity_provider_strategy =
                strategy.iter().cloned().map(lowercase_first_keys).collect();
        }
        if let Some(scd) = props.get("ServiceConnectDefaults") {
            cluster.service_connect_defaults = Some(lowercase_first_keys(scd.clone()));
        }
        if props.get("Tags").is_some() {
            cluster.tags = parse_ecs_tags(props.get("Tags"));
        }
        let cluster_arn = cluster.cluster_arn.clone();
        Ok(ProvisionResult::new(cluster_name).with("Arn", cluster_arn))
    }

    /// In-place update for AWS::ECS::Service. Service ARN is keyed by
    /// cluster + service name, both immutable. CFN replaces the resource
    /// if either changes, so we only mutate task definition / desired
    /// count / deployment-related fields here.
    fn update_ecs_service(
        &self,
        existing: &StackResource,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let service_arn = existing.physical_id.clone();
        let Some((cluster_name, service_name)) = parse_service_arn(&service_arn) else {
            return Err(format!("Cannot parse service ARN: {service_arn}"));
        };
        let key = format!("{cluster_name}/{service_name}");
        let mut accounts = self.ecs_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let svc = state
            .services
            .get_mut(&key)
            .ok_or_else(|| format!("Service {service_arn} no longer exists"))?;
        if let Some(td) = props.get("TaskDefinition").and_then(|v| v.as_str()) {
            svc.task_definition_arn = td.to_string();
            let (family, revision) = parse_td_arn(td);
            svc.family = family;
            svc.revision = revision;
        }
        if let Some(n) = props.get("DesiredCount").and_then(cfn_as_i64) {
            svc.desired_count = n as i32;
        }
        if let Some(s) = props.get("LaunchType").and_then(|v| v.as_str()) {
            svc.launch_type = s.to_string();
        }
        if let Some(s) = props.get("PlatformVersion").and_then(|v| v.as_str()) {
            svc.platform_version = Some(s.to_string());
        }
        if let Some(n) = props
            .get("HealthCheckGracePeriodSeconds")
            .and_then(cfn_as_i64)
        {
            svc.health_check_grace_period_seconds = Some(n as i32);
        }
        if let Some(b) = props.get("EnableExecuteCommand").and_then(|v| v.as_bool()) {
            svc.enable_execute_command = b;
        }
        if let Some(b) = props.get("EnableECSManagedTags").and_then(|v| v.as_bool()) {
            svc.enable_ecs_managed_tags = b;
        }
        if let Some(s) = props.get("PropagateTags").and_then(|v| v.as_str()) {
            svc.propagate_tags = Some(s.to_string());
        }
        if let Some(s) = props
            .get("AvailabilityZoneRebalancing")
            .and_then(|v| v.as_str())
        {
            svc.availability_zone_rebalancing = Some(s.to_string());
        }
        if let Some(arr) = props
            .get("CapacityProviderStrategy")
            .and_then(|v| v.as_array())
        {
            svc.capacity_provider_strategy =
                arr.iter().cloned().map(lowercase_first_keys).collect();
        }
        if let Some(dc) = props.get("DeploymentConfiguration") {
            if let Some(n) = dc.get("MinimumHealthyPercent").and_then(cfn_as_i64) {
                svc.minimum_healthy_percent = Some(n as i32);
            }
            if let Some(n) = dc.get("MaximumPercent").and_then(cfn_as_i64) {
                svc.maximum_percent = Some(n as i32);
            }
        }
        if let Some(arr) = props.get("LoadBalancers").and_then(|v| v.as_array()) {
            svc.load_balancers = arr.iter().cloned().map(lowercase_first_keys).collect();
        }
        if let Some(arr) = props.get("ServiceRegistries").and_then(|v| v.as_array()) {
            svc.service_registries = arr.iter().cloned().map(lowercase_first_keys).collect();
        }
        if let Some(arr) = props.get("PlacementConstraints").and_then(|v| v.as_array()) {
            svc.placement_constraints = arr.iter().cloned().map(lowercase_first_keys).collect();
        }
        if let Some(arr) = props.get("PlacementStrategies").and_then(|v| v.as_array()) {
            svc.placement_strategy = arr.iter().cloned().map(lowercase_first_keys).collect();
        }
        if let Some(nc) = props.get("NetworkConfiguration") {
            svc.network_configuration = Some(lowercase_first_keys(nc.clone()));
        }
        if props.get("Tags").is_some() {
            svc.tags = parse_ecs_tags(props.get("Tags"));
        }
        let name = svc.service_name.clone();
        Ok(ProvisionResult::new(service_arn.clone())
            .with("Name", name)
            .with("ServiceArn", service_arn))
    }

    /// In-place update for AWS::ECS::TaskDefinition. ECS treats every
    /// register call as a new revision, so a CFN update produces a fresh
    /// revision and the physical id (the full ARN) shifts. Returning a
    /// new ARN tells CFN's update path that the resource was replaced.
    fn update_ecs_task_definition(
        &self,
        _existing: &StackResource,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        // Delegating to create_ecs_task_definition is safe because each
        // call bumps the revision counter — semantically the right
        // behaviour for ECS.
        self.create_ecs_task_definition(resource)
    }

    /// In-place update for AWS::ECS::CapacityProvider. Name is immutable;
    /// only the underlying ASG provider config and tags can change.
    fn update_ecs_capacity_provider(
        &self,
        existing: &StackResource,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let name = existing.physical_id.clone();
        let mut accounts = self.ecs_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let cp = state
            .capacity_providers
            .get_mut(&name)
            .ok_or_else(|| format!("CapacityProvider {name} no longer exists"))?;
        if props.get("AutoScalingGroupProvider").is_some() {
            cp.auto_scaling_group_provider = props.get("AutoScalingGroupProvider").cloned();
        }
        if props.get("Tags").is_some() {
            cp.tags = parse_ecs_tags(props.get("Tags"));
        }
        let arn = cp.arn.clone();
        Ok(ProvisionResult::new(name).with("Arn", arn))
    }

    fn get_att_ecs_cluster(&self, physical_id: &str, attribute: &str) -> Option<String> {
        let mut accounts = self.ecs_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let cluster = state.clusters.get(physical_id)?;
        match attribute {
            "Arn" => Some(cluster.cluster_arn.clone()),
            _ => None,
        }
    }

    fn get_att_ecs_service(&self, physical_id: &str, attribute: &str) -> Option<String> {
        let (cluster, service) = parse_service_arn(physical_id)?;
        let key = format!("{cluster}/{service}");
        let mut accounts = self.ecs_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let svc = state.services.get(&key)?;
        match attribute {
            "Name" => Some(svc.service_name.clone()),
            "ServiceArn" => Some(svc.service_arn.clone()),
            _ => None,
        }
    }

    fn get_att_ecs_capacity_provider(&self, physical_id: &str, attribute: &str) -> Option<String> {
        let mut accounts = self.ecs_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let cp = state.capacity_providers.get(physical_id)?;
        match attribute {
            "Arn" => Some(cp.arn.clone()),
            _ => None,
        }
    }

    // --- ACM ---

    fn create_acm_certificate(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let domain_name = props
            .get("DomainName")
            .and_then(|v| v.as_str())
            .ok_or_else(|| "DomainName is required".to_string())?
            .to_string();
        let sans: Vec<String> = props
            .get("SubjectAlternativeNames")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                    .collect()
            })
            .unwrap_or_default();
        let key_algorithm = props
            .get("KeyAlgorithm")
            .and_then(|v| v.as_str())
            .unwrap_or("RSA_2048")
            .to_string();
        let validation_method = props
            .get("ValidationMethod")
            .and_then(|v| v.as_str())
            .unwrap_or("DNS")
            .to_string();
        let ca_arn = props
            .get("CertificateAuthorityArn")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let tags = parse_acm_tags(props.get("Tags"));
        let cert_transparency = props
            .get("CertificateTransparencyLoggingPreference")
            .and_then(|v| v.as_str())
            .unwrap_or("ENABLED")
            .to_string();

        // Mint a deterministic-ish ARN — ACM uses a UUID per certificate.
        let arn = format!(
            "arn:aws:acm:{}:{}:certificate/{}",
            self.region,
            self.account_id,
            Uuid::new_v4()
        );
        let now = Utc::now();

        // Build a real self-signed PEM via rcgen for the cert+SANs so
        // GetCertificate / DescribeCertificate round-trip parseable
        // X.509 (matches the runtime RequestCertificate path).
        let mut all_names = vec![domain_name.clone()];
        for s in &sans {
            if !all_names.contains(s) {
                all_names.push(s.clone());
            }
        }
        let (cert_pem, key_pem) = rcgen::generate_simple_self_signed(all_names.clone())
            .map(|c| (c.cert.pem(), c.key_pair.serialize_pem()))
            .map(|(c, k)| (Some(c), Some(k)))
            .unwrap_or((None, None));

        // CFN-provisioned certs land as ISSUED right away — real CFN
        // blocks until validation completes, but fakecloud doesn't run
        // a DNS server, so leaving the cert PENDING_VALIDATION would
        // wedge dependent resources (CloudFront/ELBv2) forever. The
        // runtime RequestCertificate path uses the async auto-issue
        // tick (DNS) or admin `/approve` endpoint (EMAIL) for parity
        // with ACM's async behaviour.
        let domain_validation: Vec<AcmDomainValidation> =
            synth_acm_domain_validation(&domain_name, &sans, &validation_method)
                .into_iter()
                .map(|mut dv| {
                    dv.validation_status = "SUCCESS".to_string();
                    dv
                })
                .collect();
        let renewal_summary = Some(AcmRenewalSummary {
            renewal_status: "PENDING_AUTO_RENEWAL".to_string(),
            domain_validation: domain_validation.clone(),
            renewal_status_reason: None,
            updated_at: now,
        });
        let cert = AcmStoredCertificate {
            arn: arn.clone(),
            domain_name: domain_name.clone(),
            subject_alternative_names: all_names,
            status: "ISSUED".to_string(),
            cert_type: "AMAZON_ISSUED".to_string(),
            certificate_pem: cert_pem,
            certificate_chain_pem: None,
            private_key_pem: key_pem,
            idempotency_token: None,
            serial: format!("{:032x}", Uuid::new_v4().as_u128()),
            subject: format!("CN={domain_name}"),
            issuer: "Amazon".to_string(),
            key_algorithm,
            signature_algorithm: "SHA256WITHRSA".to_string(),
            created_at: now,
            issued_at: Some(now),
            imported_at: None,
            revoked_at: None,
            revocation_reason: None,
            not_before: now,
            // 13 months (matches real ACM issued-cert lifetime).
            not_after: now + chrono::Duration::days(395),
            validation_method: Some(validation_method.clone()),
            domain_validation,
            options: AcmCertificateOptions {
                certificate_transparency_logging_preference: cert_transparency,
                export: "DISABLED".to_string(),
            },
            renewal_eligibility: "ELIGIBLE".to_string(),
            managed_by: None,
            certificate_authority_arn: ca_arn,
            tags,
            in_use_by: Vec::new(),
            describe_read_count: 0,
            failure_reason: None,
            renewal_summary,
        };

        let mut accounts = self.acm_state.write();
        let account = accounts
            .accounts
            .entry(self.account_id.clone())
            .or_default();
        account.certificates.insert(arn.clone(), cert);

        Ok(ProvisionResult::new(arn))
    }

    fn delete_acm_certificate(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.acm_state.write();
        if let Some(account) = accounts.accounts.get_mut(&self.account_id) {
            account.certificates.remove(physical_id);
        }
        Ok(())
    }

    /// Provision the singleton `AWS::CertificateManager::Account` resource —
    /// stores `ExpiryEventsConfiguration.DaysBeforeExpiry` on the account
    /// config so `GetAccountConfiguration` reflects it.
    fn create_acm_account(&self, resource: &ResourceDefinition) -> Result<ProvisionResult, String> {
        let days = resource
            .properties
            .get("ExpiryEventsConfiguration")
            .and_then(|v| v.get("DaysBeforeExpiry"))
            .and_then(|v| v.as_i64())
            .map(|n| n as i32);
        let mut accounts = self.acm_state.write();
        let account = accounts
            .accounts
            .entry(self.account_id.clone())
            .or_default();
        account.account_config.expiry_events_days_before_expiry = days;
        Ok(ProvisionResult::new(format!(
            "acm-account-{}",
            self.account_id
        )))
    }

    /// Reset the account config back to the default (no expiry events).
    /// AWS keeps the account around — the CFN deletion just clears the
    /// per-account override.
    fn delete_acm_account(&self) -> Result<(), String> {
        let mut accounts = self.acm_state.write();
        if let Some(account) = accounts.accounts.get_mut(&self.account_id) {
            account.account_config.expiry_events_days_before_expiry = None;
        }
        Ok(())
    }

    // --- ElastiCache ---

    fn create_ec_parameter_group(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let name = props
            .get("CacheParameterGroupName")
            .and_then(|v| v.as_str())
            .unwrap_or(&resource.logical_id)
            .to_string();
        let family = props
            .get("CacheParameterGroupFamily")
            .and_then(|v| v.as_str())
            .unwrap_or("redis7")
            .to_string();
        let description = props
            .get("Description")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let arn = format!(
            "arn:aws:elasticache:{}:{}:parametergroup:{}",
            self.region, self.account_id, name
        );
        let group = CacheParameterGroup {
            cache_parameter_group_name: name.clone(),
            cache_parameter_group_family: family,
            description,
            is_global: false,
            arn: arn.clone(),
        };
        let mut accounts = self.elasticache_state.write();
        let state = accounts.get_or_create(&self.account_id);
        // ParameterGroups stored as Vec — replace any existing entry.
        state
            .parameter_groups
            .retain(|p| p.cache_parameter_group_name != name);
        state.parameter_groups.push(group);
        Ok(ProvisionResult::new(name).with("Arn", arn))
    }

    fn delete_ec_parameter_group(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.elasticache_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state
            .parameter_groups
            .retain(|p| p.cache_parameter_group_name != physical_id);
        Ok(())
    }

    fn create_ec_subnet_group(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let name = props
            .get("CacheSubnetGroupName")
            .and_then(|v| v.as_str())
            .unwrap_or(&resource.logical_id)
            .to_string();
        let description = props
            .get("Description")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let subnet_ids: Vec<String> = props
            .get("SubnetIds")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                    .collect()
            })
            .unwrap_or_default();
        let arn = format!(
            "arn:aws:elasticache:{}:{}:subnetgroup:{}",
            self.region, self.account_id, name
        );
        let group = CacheSubnetGroup {
            cache_subnet_group_name: name.clone(),
            cache_subnet_group_description: description,
            vpc_id: String::new(),
            subnet_ids,
            arn: arn.clone(),
        };
        let mut accounts = self.elasticache_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.subnet_groups.insert(name.clone(), group);
        Ok(ProvisionResult::new(name).with("Arn", arn))
    }

    fn delete_ec_subnet_group(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.elasticache_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.subnet_groups.remove(physical_id);
        Ok(())
    }

    fn create_ec_security_group(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let name = props
            .get("CacheSecurityGroupName")
            .and_then(|v| v.as_str())
            .unwrap_or(&resource.logical_id)
            .to_string();
        let description = props
            .get("Description")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let arn = format!(
            "arn:aws:elasticache:{}:{}:securitygroup:{}",
            self.region, self.account_id, name
        );
        let group = CacheSecurityGroup {
            cache_security_group_name: name.clone(),
            description,
            owner_id: self.account_id.clone(),
            arn: arn.clone(),
            ec2_security_groups: Vec::new(),
        };
        let mut accounts = self.elasticache_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.security_groups.insert(name.clone(), group);
        Ok(ProvisionResult::new(name).with("Arn", arn))
    }

    fn delete_ec_security_group(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.elasticache_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.security_groups.remove(physical_id);
        Ok(())
    }

    fn create_ec_user(&self, resource: &ResourceDefinition) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let user_id = props
            .get("UserId")
            .and_then(|v| v.as_str())
            .unwrap_or(&resource.logical_id)
            .to_string();
        let user_name = props
            .get("UserName")
            .and_then(|v| v.as_str())
            .unwrap_or(&user_id)
            .to_string();
        let engine = props
            .get("Engine")
            .and_then(|v| v.as_str())
            .unwrap_or("redis")
            .to_string();
        let access_string = props
            .get("AccessString")
            .and_then(|v| v.as_str())
            .unwrap_or("on ~* +@all")
            .to_string();
        let authentication_type = props
            .get("AuthenticationMode")
            .and_then(|v| v.get("Type"))
            .and_then(|v| v.as_str())
            .unwrap_or("no-password-required")
            .to_string();
        let arn = format!(
            "arn:aws:elasticache:{}:{}:user:{}",
            self.region, self.account_id, user_id
        );
        let user = EcUser {
            user_id: user_id.clone(),
            user_name,
            engine,
            access_string,
            status: "active".to_string(),
            authentication_type,
            password_count: 0,
            arn: arn.clone(),
            minimum_engine_version: "6.0".to_string(),
            user_group_ids: Vec::new(),
        };
        let mut accounts = self.elasticache_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.users.insert(user_id.clone(), user);
        Ok(ProvisionResult::new(user_id).with("Arn", arn))
    }

    fn delete_ec_user(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.elasticache_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.users.remove(physical_id);
        Ok(())
    }

    fn create_ec_user_group(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let user_group_id = props
            .get("UserGroupId")
            .and_then(|v| v.as_str())
            .unwrap_or(&resource.logical_id)
            .to_string();
        let engine = props
            .get("Engine")
            .and_then(|v| v.as_str())
            .unwrap_or("redis")
            .to_string();
        let user_ids: Vec<String> = props
            .get("UserIds")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                    .collect()
            })
            .unwrap_or_default();
        let arn = format!(
            "arn:aws:elasticache:{}:{}:usergroup:{}",
            self.region, self.account_id, user_group_id
        );
        let group = EcUserGroup {
            user_group_id: user_group_id.clone(),
            engine,
            status: "active".to_string(),
            user_ids,
            arn: arn.clone(),
            minimum_engine_version: "6.0".to_string(),
            pending_changes: None,
            replication_groups: Vec::new(),
        };
        let mut accounts = self.elasticache_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.user_groups.insert(user_group_id.clone(), group);
        Ok(ProvisionResult::new(user_group_id).with("Arn", arn))
    }

    fn delete_ec_user_group(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.elasticache_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.user_groups.remove(physical_id);
        Ok(())
    }

    fn create_ec_cache_cluster(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let id = props
            .get("ClusterName")
            .and_then(|v| v.as_str())
            .map(String::from)
            .unwrap_or_else(|| format!("cfn-cc-{}", resource.logical_id.to_lowercase()));
        let cache_node_type = props
            .get("CacheNodeType")
            .and_then(|v| v.as_str())
            .unwrap_or("cache.t4g.micro")
            .to_string();
        let engine = props
            .get("Engine")
            .and_then(|v| v.as_str())
            .unwrap_or("redis")
            .to_string();
        let engine_version = props
            .get("EngineVersion")
            .and_then(|v| v.as_str())
            .unwrap_or("7.1")
            .to_string();
        let num_cache_nodes = props
            .get("NumCacheNodes")
            .and_then(|v| v.as_i64())
            .map(|n| n as i32)
            .unwrap_or(1);
        let preferred_az = props
            .get("PreferredAvailabilityZone")
            .and_then(|v| v.as_str())
            .unwrap_or("us-east-1a")
            .to_string();
        let cache_subnet_group_name = props
            .get("CacheSubnetGroupName")
            .and_then(|v| v.as_str())
            .map(String::from);
        let auto_minor_version_upgrade = props
            .get("AutoMinorVersionUpgrade")
            .and_then(|v| v.as_bool())
            .unwrap_or(true);
        let default_port = if engine == "memcached" { 11211 } else { 6379 };
        let port = props
            .get("Port")
            .and_then(|v| v.as_i64())
            .map(|n| n as u16)
            .unwrap_or(default_port);
        let cache_parameter_group_name = props
            .get("CacheParameterGroupName")
            .and_then(|v| v.as_str())
            .map(String::from);
        let security_group_ids: Vec<String> = props
            .get("VpcSecurityGroupIds")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();
        let cache_security_group_names: Vec<String> = props
            .get("CacheSecurityGroupNames")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();
        let preferred_availability_zones: Vec<String> = props
            .get("PreferredAvailabilityZones")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();
        let snapshot_arns: Vec<String> = props
            .get("SnapshotArns")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();
        let snapshot_name = props
            .get("SnapshotName")
            .and_then(|v| v.as_str())
            .map(String::from);
        let snapshot_retention_limit = props
            .get("SnapshotRetentionLimit")
            .and_then(|v| v.as_i64())
            .map(|n| n as i32)
            .unwrap_or(0);
        let snapshot_window = props
            .get("SnapshotWindow")
            .and_then(|v| v.as_str())
            .map(String::from);
        let preferred_maintenance_window = props
            .get("PreferredMaintenanceWindow")
            .and_then(|v| v.as_str())
            .map(String::from);
        let notification_topic_arn = props
            .get("NotificationTopicArn")
            .and_then(|v| v.as_str())
            .map(String::from);
        let transit_encryption_enabled = props
            .get("TransitEncryptionEnabled")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let auth_token = props
            .get("AuthToken")
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
            .map(String::from);
        let auth_token_enabled = auth_token.is_some();
        let network_type = props
            .get("NetworkType")
            .and_then(|v| v.as_str())
            .map(String::from)
            .or_else(|| Some("ipv4".to_string()));
        let ip_discovery = props
            .get("IpDiscovery")
            .and_then(|v| v.as_str())
            .map(String::from)
            .or_else(|| Some("ipv4".to_string()));
        let az_mode = props
            .get("AZMode")
            .and_then(|v| v.as_str())
            .map(String::from)
            .or_else(|| Some("single-az".to_string()));
        let outpost_mode = props
            .get("OutpostMode")
            .and_then(|v| v.as_str())
            .map(String::from);
        let preferred_outpost_arn = props
            .get("PreferredOutpostArn")
            .and_then(|v| v.as_str())
            .map(String::from);

        let mut accounts = self.elasticache_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let arn = format!(
            "arn:aws:elasticache:{}:{}:cluster:{}",
            state.region, state.account_id, id
        );
        let endpoint_address = format!("{id}.fakecloud.{}.cache.amazonaws.com", state.region);
        let cluster = EcCacheCluster {
            cache_cluster_id: id.clone(),
            cache_node_type,
            engine,
            engine_version,
            cache_cluster_status: "available".to_string(),
            num_cache_nodes,
            preferred_availability_zone: preferred_az,
            cache_subnet_group_name,
            auto_minor_version_upgrade,
            arn: arn.clone(),
            created_at: Utc::now().to_rfc3339(),
            endpoint_address: endpoint_address.clone(),
            endpoint_port: port,
            container_id: String::new(),
            host_port: 0,
            replication_group_id: None,
            cache_parameter_group_name,
            security_group_ids,
            log_delivery_configurations: Vec::new(),
            transit_encryption_enabled,
            at_rest_encryption_enabled: false,
            auth_token_enabled,
            port,
            preferred_maintenance_window,
            preferred_availability_zones,
            notification_topic_arn,
            cache_security_group_names,
            snapshot_arns,
            snapshot_name,
            snapshot_retention_limit,
            snapshot_window,
            outpost_mode,
            preferred_outpost_arn,
            network_type,
            ip_discovery,
            az_mode,
            auth_token,
            kms_key_id: None,
            transit_encryption_mode: None,
            data_tiering_enabled: None,
            cluster_mode: None,
            preferred_outpost_arns: Vec::new(),
        };
        state.cache_clusters.insert(id.clone(), cluster);
        Ok(ProvisionResult::new(id.clone())
            .with("Arn", arn)
            .with("RedisEndpoint.Address", endpoint_address.clone())
            .with("RedisEndpoint.Port", port.to_string())
            .with("ConfigurationEndpoint.Address", endpoint_address)
            .with("ConfigurationEndpoint.Port", port.to_string()))
    }

    fn delete_ec_cache_cluster(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.elasticache_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.cache_clusters.remove(physical_id);
        Ok(())
    }

    fn create_ec_replication_group(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let id = props
            .get("ReplicationGroupId")
            .and_then(|v| v.as_str())
            .map(String::from)
            .unwrap_or_else(|| format!("cfn-rg-{}", resource.logical_id.to_lowercase()));
        let description = props
            .get("ReplicationGroupDescription")
            .and_then(|v| v.as_str())
            .unwrap_or("CFN-provisioned replication group")
            .to_string();
        let cache_node_type = props
            .get("CacheNodeType")
            .and_then(|v| v.as_str())
            .unwrap_or("cache.t4g.micro")
            .to_string();
        let engine = props
            .get("Engine")
            .and_then(|v| v.as_str())
            .unwrap_or("redis")
            .to_string();
        let engine_version = props
            .get("EngineVersion")
            .and_then(|v| v.as_str())
            .unwrap_or("7.1")
            .to_string();
        let num_cache_clusters = props
            .get("NumCacheClusters")
            .and_then(|v| v.as_i64())
            .map(|n| n as i32)
            .unwrap_or(1);
        let num_node_groups = props
            .get("NumNodeGroups")
            .and_then(|v| v.as_i64())
            .map(|n| n as i32)
            .unwrap_or(0);
        let replicas_per_node_group = props
            .get("ReplicasPerNodeGroup")
            .and_then(|v| v.as_i64())
            .map(|n| n as i32);
        let automatic_failover_enabled = props
            .get("AutomaticFailoverEnabled")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let multi_az_enabled = props
            .get("MultiAZEnabled")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let transit_encryption_enabled = props
            .get("TransitEncryptionEnabled")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let at_rest_encryption_enabled = props
            .get("AtRestEncryptionEnabled")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let kms_key_id = props
            .get("KmsKeyId")
            .and_then(|v| v.as_str())
            .map(String::from);
        let auth_token_enabled = props
            .get("AuthToken")
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
            .is_some();
        let user_group_ids: Vec<String> = props
            .get("UserGroupIds")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();
        let snapshot_retention_limit = props
            .get("SnapshotRetentionLimit")
            .and_then(|v| v.as_i64())
            .map(|n| n as i32)
            .unwrap_or(0);
        let snapshot_window = props
            .get("SnapshotWindow")
            .and_then(|v| v.as_str())
            .unwrap_or("00:00-01:00")
            .to_string();
        let port = props
            .get("Port")
            .and_then(|v| v.as_i64())
            .map(|n| n as u16)
            .unwrap_or(6379);
        let cluster_enabled = num_node_groups > 1
            || props
                .get("ClusterEnabled")
                .and_then(|v| v.as_bool())
                .unwrap_or(false);

        let mut accounts = self.elasticache_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let arn = format!(
            "arn:aws:elasticache:{}:{}:replicationgroup:{}",
            state.region, state.account_id, id
        );
        let endpoint_address = format!(
            "{id}.fakecloud.ng.0001.{}.cache.amazonaws.com",
            state.region
        );
        let configuration_endpoint = if cluster_enabled {
            Some(format!(
                "{id}.fakecloud.cfg.{}.cache.amazonaws.com",
                state.region
            ))
        } else {
            None
        };

        let group = EcReplicationGroup {
            replication_group_id: id.clone(),
            description,
            global_replication_group_id: None,
            global_replication_group_role: None,
            status: "available".to_string(),
            cache_node_type,
            engine,
            engine_version,
            num_cache_clusters,
            automatic_failover_enabled,
            endpoint_address: endpoint_address.clone(),
            endpoint_port: port,
            arn: arn.clone(),
            created_at: Utc::now().to_rfc3339(),
            container_id: String::new(),
            host_port: 0,
            member_clusters: Vec::new(),
            snapshot_retention_limit,
            snapshot_window,
            transit_encryption_enabled,
            at_rest_encryption_enabled,
            cluster_enabled,
            kms_key_id,
            auth_token_enabled,
            user_group_ids,
            multi_az_enabled,
            log_delivery_configurations: Vec::new(),
            data_tiering: props
                .get("DataTieringEnabled")
                .and_then(|v| v.as_bool())
                .map(|b| if b { "enabled" } else { "disabled" }.to_string()),
            ip_discovery: props
                .get("IpDiscovery")
                .and_then(|v| v.as_str())
                .map(String::from),
            network_type: props
                .get("NetworkType")
                .and_then(|v| v.as_str())
                .map(String::from),
            transit_encryption_mode: props
                .get("TransitEncryptionMode")
                .and_then(|v| v.as_str())
                .map(String::from),
            num_node_groups,
            configuration_endpoint_address: configuration_endpoint.clone(),
            configuration_endpoint_port: configuration_endpoint.as_ref().map(|_| port),
            replicas_per_node_group,
            auth_token: props
                .get("AuthToken")
                .and_then(|v| v.as_str())
                .filter(|s| !s.is_empty())
                .map(String::from),
            port,
            notification_topic_arn: props
                .get("NotificationTopicArn")
                .and_then(|v| v.as_str())
                .map(String::from),
            cluster_mode: props
                .get("ClusterMode")
                .and_then(|v| v.as_str())
                .map(String::from),
            data_tiering_enabled: props.get("DataTieringEnabled").and_then(|v| v.as_bool()),
            notification_topic_status: None,
            cache_parameter_group_name: props
                .get("CacheParameterGroupName")
                .and_then(|v| v.as_str())
                .map(String::from),
            cache_subnet_group_name: props
                .get("CacheSubnetGroupName")
                .and_then(|v| v.as_str())
                .map(String::from),
            security_group_ids: props
                .get("SecurityGroupIds")
                .and_then(|v| v.as_array())
                .map(|arr| {
                    arr.iter()
                        .filter_map(|v| v.as_str().map(String::from))
                        .collect()
                })
                .unwrap_or_default(),
            preferred_maintenance_window: props
                .get("PreferredMaintenanceWindow")
                .and_then(|v| v.as_str())
                .map(String::from),
            snapshot_name: props
                .get("SnapshotName")
                .and_then(|v| v.as_str())
                .map(String::from),
            snapshot_arns: props
                .get("SnapshotArns")
                .and_then(|v| v.as_array())
                .map(|arr| {
                    arr.iter()
                        .filter_map(|v| v.as_str().map(String::from))
                        .collect()
                })
                .unwrap_or_default(),
            auto_minor_version_upgrade: props
                .get("AutoMinorVersionUpgrade")
                .and_then(|v| v.as_bool())
                .unwrap_or(true),
        };
        state.replication_groups.insert(id.clone(), group);

        let mut result = ProvisionResult::new(id.clone())
            .with("Arn", arn)
            .with("PrimaryEndPoint.Address", endpoint_address.clone())
            .with("PrimaryEndPoint.Port", port.to_string())
            .with("ReadEndPoint.Addresses", endpoint_address.clone())
            .with("ReadEndPoint.Ports", port.to_string());
        if let Some(cfg) = configuration_endpoint {
            result = result
                .with("ConfigurationEndPoint.Address", cfg)
                .with("ConfigurationEndPoint.Port", port.to_string());
        }
        Ok(result)
    }

    fn delete_ec_replication_group(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.elasticache_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.replication_groups.remove(physical_id);
        Ok(())
    }

    // --- Route53 ---

    fn create_route53_hosted_zone(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let name = props
            .get("Name")
            .and_then(|v| v.as_str())
            .ok_or("Name is required")?
            .to_string();
        let normalized_name = if name.ends_with('.') {
            name.clone()
        } else {
            format!("{name}.")
        };
        let comment = props
            .get("HostedZoneConfig")
            .and_then(|v| v.get("Comment"))
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let private_zone = props
            .get("VPCs")
            .and_then(|v| v.as_array())
            .map(|a| !a.is_empty())
            .unwrap_or(false);
        let vpcs: Vec<fakecloud_route53::model::VPC> = props
            .get("VPCs")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .map(|vpc| fakecloud_route53::model::VPC {
                        vpc_id: vpc.get("VPCId").and_then(|v| v.as_str()).map(String::from),
                        vpc_region: vpc
                            .get("VPCRegion")
                            .and_then(|v| v.as_str())
                            .map(String::from),
                    })
                    .collect()
            })
            .unwrap_or_default();

        let id = format!(
            "Z{}",
            Uuid::new_v4().simple().to_string()[..14].to_uppercase()
        );
        let name_servers = (1..=4)
            .map(|i| format!("ns-{}.awsdns-{:02}.com", 100 + i, i))
            .collect::<Vec<_>>();

        let zone = StoredHostedZone {
            id: id.clone(),
            name: normalized_name,
            caller_reference: format!("cfn-{}", resource.logical_id),
            comment,
            private_zone,
            features: Some(HostedZoneFeatures::default()),
            vpcs,
            delegation_set_id: None,
            name_servers: name_servers.clone(),
            created_time: Utc::now(),
            resource_record_sets: Vec::new(),
        };

        let mut accounts = self.route53_state.write();
        // Route53 is a global service in fakecloud; all entries share the
        // default account bucket so SDK reads land on the same data.
        let state = accounts.entry("000000000000");
        state.hosted_zones.insert(id.clone(), zone);

        let mut result = ProvisionResult::new(id.clone()).with("Id", id);
        for (i, ns) in name_servers.iter().enumerate() {
            result = result.with(&format!("NameServers.{i}"), ns.clone());
        }
        result = result.with("NameServers", name_servers.join(","));
        Ok(result)
    }

    fn delete_route53_hosted_zone(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.route53_state.write();
        // Route53 is a global service in fakecloud; all entries share the
        // default account bucket so SDK reads land on the same data.
        let state = accounts.entry("000000000000");
        state.hosted_zones.remove(physical_id);
        Ok(())
    }

    fn create_route53_record_set(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let zone_id = props
            .get("HostedZoneId")
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                "HostedZoneId is required (HostedZoneName lookups not supported)".to_string()
            })?
            .to_string();
        let name = props
            .get("Name")
            .and_then(|v| v.as_str())
            .ok_or("Name is required")?
            .to_string();
        let normalized_name = if name.ends_with('.') {
            name.clone()
        } else {
            format!("{name}.")
        };
        let record_type = props
            .get("Type")
            .and_then(|v| v.as_str())
            .ok_or("Type is required")?
            .to_string();
        let ttl = props.get("TTL").and_then(|v| {
            v.as_str()
                .and_then(|s| s.parse::<i64>().ok())
                .or_else(|| v.as_i64())
        });
        let resource_records = props
            .get("ResourceRecords")
            .and_then(|v| v.as_array())
            .map(|arr| {
                let recs: Vec<fakecloud_route53::model::ResourceRecord> = arr
                    .iter()
                    .filter_map(|v| {
                        v.as_str()
                            .map(|s| fakecloud_route53::model::ResourceRecord {
                                value: s.to_string(),
                            })
                    })
                    .collect();
                fakecloud_route53::model::ResourceRecords {
                    resource_record: recs,
                }
            });
        let alias_target =
            props
                .get("AliasTarget")
                .map(|v| fakecloud_route53::model::AliasTarget {
                    hosted_zone_id: v
                        .get("HostedZoneId")
                        .and_then(|x| x.as_str())
                        .unwrap_or("")
                        .to_string(),
                    dns_name: v
                        .get("DNSName")
                        .and_then(|x| x.as_str())
                        .unwrap_or("")
                        .to_string(),
                    evaluate_target_health: v
                        .get("EvaluateTargetHealth")
                        .and_then(|x| x.as_bool())
                        .unwrap_or(false),
                });
        let set_identifier = props
            .get("SetIdentifier")
            .and_then(|v| v.as_str())
            .map(String::from);
        let weight = props.get("Weight").and_then(|v| {
            v.as_i64()
                .or_else(|| v.as_str().and_then(|s| s.parse::<i64>().ok()))
        });
        let region = props
            .get("Region")
            .and_then(|v| v.as_str())
            .map(String::from);
        let failover = props
            .get("Failover")
            .and_then(|v| v.as_str())
            .map(String::from);
        let multi_value_answer = props.get("MultiValueAnswer").and_then(|v| v.as_bool());
        let health_check_id = props
            .get("HealthCheckId")
            .and_then(|v| v.as_str())
            .map(String::from);

        let rrset = ResourceRecordSet {
            name: normalized_name.clone(),
            record_type: record_type.clone(),
            set_identifier: set_identifier.clone(),
            weight,
            region,
            geo_location: None,
            failover,
            multi_value_answer,
            ttl,
            resource_records,
            alias_target,
            health_check_id,
            traffic_policy_instance_id: None,
            cidr_routing_config: None,
            geo_proximity_location: None,
        };

        let mut accounts = self.route53_state.write();
        // Route53 is a global service in fakecloud; all entries share the
        // default account bucket so SDK reads land on the same data.
        let state = accounts.entry("000000000000");
        let zone = state.hosted_zones.get_mut(&zone_id).ok_or_else(|| {
            format!(
                "HostedZone {zone_id} not yet provisioned; will retry once it has been provisioned"
            )
        })?;
        // Replace existing record with matching (name, type, set_identifier).
        zone.resource_record_sets.retain(|r| {
            !(r.name == rrset.name
                && r.record_type == rrset.record_type
                && r.set_identifier == rrset.set_identifier)
        });
        zone.resource_record_sets.push(rrset);

        let physical_id = match &set_identifier {
            Some(sid) => format!("{zone_id}|{normalized_name}|{record_type}|{sid}"),
            None => format!("{zone_id}|{normalized_name}|{record_type}"),
        };
        Ok(ProvisionResult::new(physical_id))
    }

    fn delete_route53_record_set(
        &self,
        physical_id: &str,
        _attributes: &BTreeMap<String, String>,
    ) -> Result<(), String> {
        let parts: Vec<&str> = physical_id.split('|').collect();
        if parts.len() < 3 {
            return Ok(());
        }
        let zone_id = parts[0];
        let name = parts[1];
        let record_type = parts[2];
        let set_identifier = parts.get(3).map(|s| s.to_string());

        let mut accounts = self.route53_state.write();
        // Route53 is a global service in fakecloud; all entries share the
        // default account bucket so SDK reads land on the same data.
        let state = accounts.entry("000000000000");
        if let Some(zone) = state.hosted_zones.get_mut(zone_id) {
            zone.resource_record_sets.retain(|r| {
                !(r.name == name
                    && r.record_type == record_type
                    && r.set_identifier == set_identifier)
            });
        }
        Ok(())
    }

    fn create_route53_health_check(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let cfg_value = props
            .get("HealthCheckConfig")
            .ok_or("HealthCheckConfig is required")?;

        let health_check_type = cfg_value
            .get("Type")
            .and_then(|v| v.as_str())
            .ok_or("HealthCheckConfig.Type is required")?
            .to_string();

        let cfg = HealthCheckConfig {
            ip_address: cfg_value
                .get("IPAddress")
                .and_then(|v| v.as_str())
                .map(String::from),
            port: cfg_value
                .get("Port")
                .and_then(|v| {
                    v.as_i64()
                        .or_else(|| v.as_str().and_then(|s| s.parse::<i64>().ok()))
                })
                .map(|n| n as i32),
            health_check_type,
            resource_path: cfg_value
                .get("ResourcePath")
                .and_then(|v| v.as_str())
                .map(String::from),
            fully_qualified_domain_name: cfg_value
                .get("FullyQualifiedDomainName")
                .and_then(|v| v.as_str())
                .map(String::from),
            search_string: cfg_value
                .get("SearchString")
                .and_then(|v| v.as_str())
                .map(String::from),
            request_interval: cfg_value
                .get("RequestInterval")
                .and_then(|v| {
                    v.as_i64()
                        .or_else(|| v.as_str().and_then(|s| s.parse::<i64>().ok()))
                })
                .map(|n| n as i32),
            failure_threshold: cfg_value
                .get("FailureThreshold")
                .and_then(|v| {
                    v.as_i64()
                        .or_else(|| v.as_str().and_then(|s| s.parse::<i64>().ok()))
                })
                .map(|n| n as i32),
            measure_latency: cfg_value.get("MeasureLatency").and_then(|v| v.as_bool()),
            inverted: cfg_value.get("Inverted").and_then(|v| v.as_bool()),
            disabled: cfg_value.get("Disabled").and_then(|v| v.as_bool()),
            health_threshold: cfg_value
                .get("HealthThreshold")
                .and_then(|v| {
                    v.as_i64()
                        .or_else(|| v.as_str().and_then(|s| s.parse::<i64>().ok()))
                })
                .map(|n| n as i32),
            child_health_checks: cfg_value
                .get("ChildHealthChecks")
                .and_then(|v| v.as_array())
                .map(|arr| fakecloud_route53::model::ChildHealthChecks {
                    child_health_check: arr
                        .iter()
                        .filter_map(|v| v.as_str().map(String::from))
                        .collect(),
                }),
            enable_sni: cfg_value.get("EnableSNI").and_then(|v| v.as_bool()),
            regions: cfg_value
                .get("Regions")
                .and_then(|v| v.as_array())
                .map(|arr| fakecloud_route53::model::HealthCheckRegions {
                    region: arr
                        .iter()
                        .filter_map(|v| v.as_str().map(String::from))
                        .collect(),
                }),
            alarm_identifier: cfg_value.get("AlarmIdentifier").map(|v| {
                fakecloud_route53::model::AlarmIdentifier {
                    region: v
                        .get("Region")
                        .and_then(|x| x.as_str())
                        .unwrap_or("")
                        .to_string(),
                    name: v
                        .get("Name")
                        .and_then(|x| x.as_str())
                        .unwrap_or("")
                        .to_string(),
                }
            }),
            insufficient_data_health_status: cfg_value
                .get("InsufficientDataHealthStatus")
                .and_then(|v| v.as_str())
                .map(String::from),
            routing_control_arn: cfg_value
                .get("RoutingControlArn")
                .and_then(|v| v.as_str())
                .map(String::from),
        };

        let id = Uuid::new_v4().to_string();
        let hc = StoredHealthCheck {
            id: id.clone(),
            caller_reference: format!("cfn-{}", resource.logical_id),
            version: 1,
            config: cfg,
            created_time: Utc::now(),
            status: fakecloud_route53::HealthCheckStatus::Success,
            last_failure_reason: None,
        };

        let mut accounts = self.route53_state.write();
        // Route53 is a global service in fakecloud; all entries share the
        // default account bucket so SDK reads land on the same data.
        let state = accounts.entry("000000000000");
        state.health_checks.insert(id.clone(), hc);

        Ok(ProvisionResult::new(id.clone()).with("HealthCheckId", id))
    }

    fn delete_route53_health_check(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.route53_state.write();
        // Route53 is a global service in fakecloud; all entries share the
        // default account bucket so SDK reads land on the same data.
        let state = accounts.entry("000000000000");
        state.health_checks.remove(physical_id);
        Ok(())
    }

    /// `AWS::Route53::DNSSEC` flips the hosted zone's `dnssec_status` to
    /// `SIGNING`. Physical id is the hosted zone id so the delete path can
    /// flip it back without consulting the template.
    fn create_route53_dnssec(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let zone_id = resource
            .properties
            .get("HostedZoneId")
            .and_then(|v| v.as_str())
            .ok_or_else(|| "HostedZoneId is required".to_string())?
            .trim_start_matches("/hostedzone/")
            .to_string();
        let mut accounts = self.route53_state.write();
        let state = accounts.entry("000000000000");
        if !state.hosted_zones.contains_key(&zone_id) {
            return Err(format!("HostedZone {zone_id} does not exist"));
        }
        state
            .dnssec_status
            .insert(zone_id.clone(), "SIGNING".to_string());
        Ok(ProvisionResult::new(zone_id))
    }

    fn delete_route53_dnssec(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.route53_state.write();
        let state = accounts.entry("000000000000");
        state.dnssec_status.remove(physical_id);
        Ok(())
    }

    /// `AWS::Route53::KeySigningKey` registers a KSK against a hosted
    /// zone. Physical id encodes `<hosted_zone_id>/<name>` so the delete
    /// path can find the (zone, name) tuple without re-reading inputs.
    fn create_route53_key_signing_key(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let zone_id = props
            .get("HostedZoneId")
            .and_then(|v| v.as_str())
            .ok_or_else(|| "HostedZoneId is required".to_string())?
            .trim_start_matches("/hostedzone/")
            .to_string();
        let name = props
            .get("Name")
            .and_then(|v| v.as_str())
            .ok_or_else(|| "Name is required".to_string())?
            .to_string();
        let kms_arn = props
            .get("KeyManagementServiceArn")
            .and_then(|v| v.as_str())
            .ok_or_else(|| "KeyManagementServiceArn is required".to_string())?
            .to_string();
        let status = props
            .get("Status")
            .and_then(|v| v.as_str())
            .unwrap_or("ACTIVE")
            .to_string();
        let now = Utc::now();
        // Derive the same deterministic ECDSA P-256 KSK material the
        // direct CreateKeySigningKey path produces so DNSSEC features
        // (DS digest, RRSIG signing, the
        // `/_fakecloud/route53/zones/{id}/dnssec` admin endpoint)
        // round-trip identically through CloudFormation.
        let key_material = fakecloud_route53::dnssec::derive_keypair(&zone_id, &name);
        let key_tag = fakecloud_route53::dnssec::key_tag_for(&key_material.dnskey_public_key);
        let mut accounts = self.route53_state.write();
        let state = accounts.entry("000000000000");
        if !state.hosted_zones.contains_key(&zone_id) {
            return Err(format!("HostedZone {zone_id} does not exist"));
        }
        let zone_name = state
            .hosted_zones
            .get(&zone_id)
            .map(|z| z.name.clone())
            .unwrap_or_else(|| ".".to_string());
        let ds_digest_hex = fakecloud_route53::dnssec::ds_digest_sha256(
            &zone_name,
            key_tag,
            &key_material.dnskey_public_key,
        );
        let ksk = fakecloud_route53::StoredKeySigningKey {
            hosted_zone_id: zone_id.clone(),
            name: name.clone(),
            kms_arn,
            status,
            caller_reference: format!("cfn-{}", Uuid::new_v4()),
            created_date: now,
            last_modified_date: now,
            key_tag: key_tag as i32,
            private_key_pem: key_material.private_key_pem,
            public_key_der: key_material.public_key_der,
            ds_digest_hex,
        };
        state
            .key_signing_keys
            .insert((zone_id.clone(), name.clone()), ksk);
        Ok(ProvisionResult::new(format!("{zone_id}/{name}")))
    }

    fn delete_route53_key_signing_key(&self, physical_id: &str) -> Result<(), String> {
        let (zone_id, name) = match physical_id.split_once('/') {
            Some(parts) => parts,
            None => return Ok(()),
        };
        let mut accounts = self.route53_state.write();
        let state = accounts.entry("000000000000");
        state
            .key_signing_keys
            .remove(&(zone_id.to_string(), name.to_string()));
        Ok(())
    }

    // --- CloudFront ---
    //
    // CloudFront is a global service that stores data under the default
    // account bucket; mirror that here so SDK reads land on the same data
    // CFN wrote.

    fn create_cf_origin_access_identity(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let cfg = props
            .get("CloudFrontOriginAccessIdentityConfig")
            .ok_or("CloudFrontOriginAccessIdentityConfig is required")?;
        let comment = cfg
            .get("Comment")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let caller_reference = format!("cfn-{}", resource.logical_id);

        let id = format!(
            "E{}",
            Uuid::new_v4().simple().to_string()[..13].to_uppercase()
        );
        let etag = format!(
            "E{}",
            Uuid::new_v4().simple().to_string()[..7].to_uppercase()
        );
        let s3_canonical_user_id = format!(
            "{:0<64}",
            Uuid::new_v4().simple().to_string().to_lowercase()
        );

        let oai = StoredOriginAccessIdentity {
            id: id.clone(),
            etag,
            s3_canonical_user_id: s3_canonical_user_id.clone(),
            config: CloudFrontOriginAccessIdentityConfig {
                caller_reference,
                comment,
            },
        };

        let mut accounts = self.cloudfront_state.write();
        let state = accounts.entry("000000000000");
        state.origin_access_identities.insert(id.clone(), oai);

        Ok(ProvisionResult::new(id.clone())
            .with("Id", id)
            .with("S3CanonicalUserId", s3_canonical_user_id))
    }

    fn delete_cf_origin_access_identity(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.cloudfront_state.write();
        let state = accounts.entry("000000000000");
        state.origin_access_identities.remove(physical_id);
        Ok(())
    }

    /// Provision an `AWS::CloudFront::Distribution`. Reads
    /// DistributionConfig.Origins/DefaultCacheBehavior/etc. and persists
    /// a StoredDistribution in CloudFront state. CFN's Origins property
    /// is a flat array, so we wrap it back into the wire shape with a
    /// quantity + Items.Origin nesting.
    fn create_cf_distribution(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let cfg = resource
            .properties
            .get("DistributionConfig")
            .ok_or_else(|| "DistributionConfig is required".to_string())?;

        // CFN Origins is a flat JSON array; the wire shape is
        // { Quantity, Items: { Origin: [...] } }. Translate. CFN's
        // PascalCase doesn't always match the wire model — Origin's
        // CustomOriginConfig uses HTTPPort/HTTPSPort while the model
        // expects HttpPort/HttpsPort, so we patch a few well-known
        // renames before letting serde finish the parse.
        let origin_entries: Vec<Origin> = cfg
            .get("Origins")
            .and_then(|v| v.as_array())
            .ok_or_else(|| "DistributionConfig.Origins is required".to_string())?
            .iter()
            .map(|o| {
                let mut patched = o.clone();
                if let Some(custom) = patched
                    .get_mut("CustomOriginConfig")
                    .and_then(|v| v.as_object_mut())
                {
                    if let Some(v) = custom.remove("HTTPPort") {
                        custom.insert("HttpPort".to_string(), v);
                    }
                    if let Some(v) = custom.remove("HTTPSPort") {
                        custom.insert("HttpsPort".to_string(), v);
                    }
                }
                serde_json::from_value::<Origin>(patched)
                    .map_err(|e| format!("Invalid Origin entry: {e}"))
            })
            .collect::<Result<Vec<_>, _>>()?;
        if origin_entries.is_empty() {
            return Err("DistributionConfig.Origins must contain at least one origin".to_string());
        }
        let origins = Origins {
            quantity: origin_entries.len() as i32,
            items: Some(OriginItems {
                origin: origin_entries,
            }),
        };

        let dcb_value = cfg
            .get("DefaultCacheBehavior")
            .ok_or_else(|| "DistributionConfig.DefaultCacheBehavior is required".to_string())?;
        let default_cache_behavior: DefaultCacheBehavior =
            serde_json::from_value(dcb_value.clone())
                .map_err(|e| format!("Invalid DefaultCacheBehavior: {e}"))?;

        let comment = cfg
            .get("Comment")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let enabled = cfg.get("Enabled").and_then(|v| v.as_bool()).unwrap_or(true);
        let price_class = cfg
            .get("PriceClass")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let http_version = cfg
            .get("HttpVersion")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let is_ipv6_enabled = cfg.get("IPV6Enabled").and_then(|v| v.as_bool());
        let default_root_object = cfg
            .get("DefaultRootObject")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let web_acl_id = cfg
            .get("WebACLId")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());

        let viewer_certificate: Option<ViewerCertificate> = cfg
            .get("ViewerCertificate")
            .map(|v| serde_json::from_value(v.clone()))
            .transpose()
            .map_err(|e| format!("Invalid ViewerCertificate: {e}"))?;

        let caller_reference = format!("cfn-{}-{}", resource.logical_id, Uuid::new_v4().simple());

        let mut config = DistributionConfig {
            caller_reference,
            comment,
            enabled,
            origins,
            default_cache_behavior,
            ..Default::default()
        };
        config.price_class = price_class;
        config.http_version = http_version;
        config.is_ipv6_enabled = is_ipv6_enabled;
        config.default_root_object = default_root_object;
        config.web_acl_id = web_acl_id;
        config.viewer_certificate = viewer_certificate;

        // Mint distribution id + ARN + domain in the same shape the
        // CloudFront service uses.
        let id_suffix: String = Uuid::new_v4()
            .simple()
            .to_string()
            .chars()
            .take(13)
            .collect::<String>()
            .to_uppercase();
        let id = format!("E{id_suffix}");
        let etag_suffix: String = Uuid::new_v4()
            .simple()
            .to_string()
            .chars()
            .take(7)
            .collect::<String>()
            .to_uppercase();
        let etag = format!("E{etag_suffix}");
        let domain_name = format!("{}.cloudfront.net", id.to_lowercase());
        let arn = format!(
            "arn:aws:cloudfront::{}:distribution/{}",
            self.account_id, id
        );

        let stored = StoredDistribution {
            id: id.clone(),
            arn: arn.clone(),
            // CloudFront flips this to Deployed on the first GetDistribution
            // poll, matching the rest of the service.
            status: "InProgress".to_string(),
            last_modified_time: Utc::now(),
            domain_name: domain_name.clone(),
            in_progress_invalidation_batches: 0,
            etag,
            config,
        };

        let mut accounts = self.cloudfront_state.write();
        let state = accounts.entry("000000000000");
        state.distributions.insert(id.clone(), stored);
        Ok(ProvisionResult::new(id.clone())
            .with("Id", id)
            .with("DomainName", domain_name)
            .with("Arn", arn))
    }

    fn delete_cf_distribution(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.cloudfront_state.write();
        let state = accounts.entry("000000000000");
        state.distributions.remove(physical_id);
        Ok(())
    }

    fn create_cf_origin_access_control(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let cfg = props
            .get("OriginAccessControlConfig")
            .ok_or("OriginAccessControlConfig is required")?;
        let name = cfg
            .get("Name")
            .and_then(|v| v.as_str())
            .ok_or("OriginAccessControlConfig.Name is required")?
            .to_string();
        let signing_protocol = cfg
            .get("SigningProtocol")
            .and_then(|v| v.as_str())
            .unwrap_or("sigv4")
            .to_string();
        let signing_behavior = cfg
            .get("SigningBehavior")
            .and_then(|v| v.as_str())
            .unwrap_or("always")
            .to_string();
        let origin_type = cfg
            .get("OriginAccessControlOriginType")
            .and_then(|v| v.as_str())
            .ok_or("OriginAccessControlConfig.OriginAccessControlOriginType is required")?
            .to_string();
        let description = cfg
            .get("Description")
            .and_then(|v| v.as_str())
            .map(String::from);

        let id = format!(
            "E{}",
            Uuid::new_v4().simple().to_string()[..13].to_uppercase()
        );
        let etag = format!(
            "E{}",
            Uuid::new_v4().simple().to_string()[..7].to_uppercase()
        );
        let oac = StoredOriginAccessControl {
            id: id.clone(),
            etag,
            config: OriginAccessControlConfig {
                name,
                description,
                signing_protocol,
                signing_behavior,
                origin_access_control_origin_type: origin_type,
            },
        };

        let mut accounts = self.cloudfront_state.write();
        let state = accounts.entry("000000000000");
        state.origin_access_controls.insert(id.clone(), oac);

        Ok(ProvisionResult::new(id.clone()).with("Id", id))
    }

    fn delete_cf_origin_access_control(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.cloudfront_state.write();
        let state = accounts.entry("000000000000");
        state.origin_access_controls.remove(physical_id);
        Ok(())
    }

    fn create_cf_public_key(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let cfg = props
            .get("PublicKeyConfig")
            .ok_or("PublicKeyConfig is required")?;
        let name = cfg
            .get("Name")
            .and_then(|v| v.as_str())
            .ok_or("PublicKeyConfig.Name is required")?
            .to_string();
        let encoded_key = cfg
            .get("EncodedKey")
            .and_then(|v| v.as_str())
            .ok_or("PublicKeyConfig.EncodedKey is required")?
            .to_string();
        let comment = cfg
            .get("Comment")
            .and_then(|v| v.as_str())
            .map(String::from);
        let caller_reference = cfg
            .get("CallerReference")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let caller_reference = if caller_reference.is_empty() {
            format!("cfn-{}", resource.logical_id)
        } else {
            caller_reference
        };

        let id = format!(
            "K{}",
            Uuid::new_v4().simple().to_string()[..13].to_uppercase()
        );
        let etag = format!(
            "E{}",
            Uuid::new_v4().simple().to_string()[..7].to_uppercase()
        );

        let pk = StoredPublicKey {
            id: id.clone(),
            etag,
            created_time: Utc::now(),
            config: PublicKeyConfig {
                caller_reference,
                name,
                encoded_key,
                comment,
            },
        };

        let mut accounts = self.cloudfront_state.write();
        let state = accounts.entry("000000000000");
        state.public_keys.insert(id.clone(), pk);

        Ok(ProvisionResult::new(id.clone()).with("Id", id))
    }

    fn delete_cf_public_key(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.cloudfront_state.write();
        let state = accounts.entry("000000000000");
        state.public_keys.remove(physical_id);
        Ok(())
    }

    fn create_cf_key_group(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let cfg = props
            .get("KeyGroupConfig")
            .ok_or("KeyGroupConfig is required")?;
        let name = cfg
            .get("Name")
            .and_then(|v| v.as_str())
            .ok_or("KeyGroupConfig.Name is required")?
            .to_string();
        let items: Vec<String> = cfg
            .get("Items")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();
        let comment = cfg
            .get("Comment")
            .and_then(|v| v.as_str())
            .map(String::from);

        let id = format!(
            "KG{}",
            Uuid::new_v4().simple().to_string()[..12].to_uppercase()
        );
        let etag = format!(
            "E{}",
            Uuid::new_v4().simple().to_string()[..7].to_uppercase()
        );

        let kg = StoredKeyGroup {
            id: id.clone(),
            etag,
            last_modified_time: Utc::now(),
            config: KeyGroupConfig {
                name,
                items: KeyGroupItems { public_key: items },
                comment,
            },
        };

        let mut accounts = self.cloudfront_state.write();
        let state = accounts.entry("000000000000");
        state.key_groups.insert(id.clone(), kg);

        Ok(ProvisionResult::new(id.clone()).with("Id", id))
    }

    fn delete_cf_key_group(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.cloudfront_state.write();
        let state = accounts.entry("000000000000");
        state.key_groups.remove(physical_id);
        Ok(())
    }

    fn create_cf_function(&self, resource: &ResourceDefinition) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let name = props
            .get("Name")
            .and_then(|v| v.as_str())
            .ok_or("Name is required")?
            .to_string();
        let function_code = props
            .get("FunctionCode")
            .and_then(|v| v.as_str())
            .ok_or("FunctionCode is required")?
            .to_string();
        let cfg = props
            .get("FunctionConfig")
            .ok_or("FunctionConfig is required")?;
        let runtime = cfg
            .get("Runtime")
            .and_then(|v| v.as_str())
            .unwrap_or("cloudfront-js-2.0")
            .to_string();
        let comment = cfg
            .get("Comment")
            .and_then(|v| v.as_str())
            .map(String::from);

        let id = format!(
            "FN{}",
            Uuid::new_v4().simple().to_string()[..12].to_uppercase()
        );
        let etag = format!(
            "E{}",
            Uuid::new_v4().simple().to_string()[..7].to_uppercase()
        );
        let function_arn = format!("arn:aws:cloudfront::{}:function/{}", self.account_id, name);

        let now = Utc::now();
        let func = StoredFunction {
            name: name.clone(),
            etag,
            status: "UNPUBLISHED".to_string(),
            stage: "DEVELOPMENT".to_string(),
            function_arn: function_arn.clone(),
            created_time: now,
            last_modified_time: now,
            config: FunctionConfig {
                comment,
                runtime,
                key_value_store_associations: None,
            },
            function_code,
            live_function_code: None,
        };

        let mut accounts = self.cloudfront_state.write();
        let state = accounts.entry("000000000000");
        // Use the function's ARN/name as the registry key so subsequent
        // operations (Get/Update/Delete) keyed by name resolve.
        state.functions.insert(name.clone(), func);

        Ok(ProvisionResult::new(name.clone())
            .with("FunctionARN", function_arn)
            .with("FunctionMetadata.FunctionARN", id)
            .with("Stage", "DEVELOPMENT"))
    }

    fn delete_cf_function(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.cloudfront_state.write();
        let state = accounts.entry("000000000000");
        state.functions.remove(physical_id);
        Ok(())
    }

    fn create_cf_cache_policy(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let cfg = props
            .get("CachePolicyConfig")
            .ok_or("CachePolicyConfig is required")?;
        let name = cfg
            .get("Name")
            .and_then(|v| v.as_str())
            .ok_or("CachePolicyConfig.Name is required")?
            .to_string();
        let min_ttl = cfg
            .get("MinTTL")
            .and_then(|v| {
                v.as_i64()
                    .or_else(|| v.as_str().and_then(|s| s.parse::<i64>().ok()))
            })
            .unwrap_or(0);
        let default_ttl = cfg.get("DefaultTTL").and_then(|v| {
            v.as_i64()
                .or_else(|| v.as_str().and_then(|s| s.parse::<i64>().ok()))
        });
        let max_ttl = cfg.get("MaxTTL").and_then(|v| {
            v.as_i64()
                .or_else(|| v.as_str().and_then(|s| s.parse::<i64>().ok()))
        });
        let comment = cfg
            .get("Comment")
            .and_then(|v| v.as_str())
            .map(String::from);

        let id = format!(
            "CP{}",
            Uuid::new_v4().simple().to_string()[..12].to_uppercase()
        );
        let etag = format!(
            "E{}",
            Uuid::new_v4().simple().to_string()[..7].to_uppercase()
        );

        let cache_policy = StoredCachePolicy {
            id: id.clone(),
            etag,
            last_modified_time: Utc::now(),
            config: CachePolicyConfig {
                comment,
                name,
                default_ttl,
                max_ttl,
                min_ttl,
                parameters_in_cache_key_and_forwarded_to_origin: None,
            },
            policy_type: "custom".to_string(),
        };

        let mut accounts = self.cloudfront_state.write();
        let state = accounts.entry("000000000000");
        state.cache_policies.insert(id.clone(), cache_policy);

        Ok(ProvisionResult::new(id.clone()).with("Id", id))
    }

    fn delete_cf_cache_policy(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.cloudfront_state.write();
        let state = accounts.entry("000000000000");
        state.cache_policies.remove(physical_id);
        Ok(())
    }

    fn create_cf_origin_request_policy(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let cfg = props
            .get("OriginRequestPolicyConfig")
            .ok_or("OriginRequestPolicyConfig is required")?;
        let name = cfg
            .get("Name")
            .and_then(|v| v.as_str())
            .ok_or("OriginRequestPolicyConfig.Name is required")?
            .to_string();
        let header_behavior = cfg
            .get("HeadersConfig")
            .and_then(|v| v.get("HeaderBehavior"))
            .and_then(|v| v.as_str())
            .unwrap_or("none")
            .to_string();
        let cookie_behavior = cfg
            .get("CookiesConfig")
            .and_then(|v| v.get("CookieBehavior"))
            .and_then(|v| v.as_str())
            .unwrap_or("none")
            .to_string();
        let query_string_behavior = cfg
            .get("QueryStringsConfig")
            .and_then(|v| v.get("QueryStringBehavior"))
            .and_then(|v| v.as_str())
            .unwrap_or("none")
            .to_string();
        let comment = cfg
            .get("Comment")
            .and_then(|v| v.as_str())
            .map(String::from);

        let id = format!(
            "ORP{}",
            Uuid::new_v4().simple().to_string()[..11].to_uppercase()
        );
        let etag = format!(
            "E{}",
            Uuid::new_v4().simple().to_string()[..7].to_uppercase()
        );

        let policy = StoredOriginRequestPolicy {
            id: id.clone(),
            etag,
            last_modified_time: Utc::now(),
            config: OriginRequestPolicyConfig {
                comment,
                name,
                headers_config: OriginRequestPolicyHeadersConfig {
                    header_behavior,
                    headers: None,
                },
                cookies_config: OriginRequestPolicyCookiesConfig {
                    cookie_behavior,
                    cookies: None,
                },
                query_strings_config: OriginRequestPolicyQueryStringsConfig {
                    query_string_behavior,
                    query_strings: None,
                },
            },
            policy_type: "custom".to_string(),
        };

        let mut accounts = self.cloudfront_state.write();
        let state = accounts.entry("000000000000");
        state.origin_request_policies.insert(id.clone(), policy);

        Ok(ProvisionResult::new(id.clone()).with("Id", id))
    }

    fn delete_cf_origin_request_policy(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.cloudfront_state.write();
        let state = accounts.entry("000000000000");
        state.origin_request_policies.remove(physical_id);
        Ok(())
    }

    fn create_cf_response_headers_policy(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let cfg = props
            .get("ResponseHeadersPolicyConfig")
            .ok_or("ResponseHeadersPolicyConfig is required")?;
        let name = cfg
            .get("Name")
            .and_then(|v| v.as_str())
            .ok_or("ResponseHeadersPolicyConfig.Name is required")?
            .to_string();
        let comment = cfg
            .get("Comment")
            .and_then(|v| v.as_str())
            .map(String::from);

        let id = format!(
            "RHP{}",
            Uuid::new_v4().simple().to_string()[..11].to_uppercase()
        );
        let etag = format!(
            "E{}",
            Uuid::new_v4().simple().to_string()[..7].to_uppercase()
        );

        let policy = StoredResponseHeadersPolicy {
            id: id.clone(),
            etag,
            last_modified_time: Utc::now(),
            config: ResponseHeadersPolicyConfig {
                comment,
                name,
                cors_config: None,
                security_headers_config: None,
                server_timing_headers_config: None,
                custom_headers_config: None,
                remove_headers_config: None,
            },
            policy_type: "custom".to_string(),
        };

        let mut accounts = self.cloudfront_state.write();
        let state = accounts.entry("000000000000");
        state.response_headers_policies.insert(id.clone(), policy);

        Ok(ProvisionResult::new(id.clone()).with("Id", id))
    }

    fn delete_cf_response_headers_policy(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.cloudfront_state.write();
        let state = accounts.entry("000000000000");
        state.response_headers_policies.remove(physical_id);
        Ok(())
    }

    // --- Step Functions ---

    fn create_sfn_state_machine(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let name = props
            .get("StateMachineName")
            .and_then(|v| v.as_str())
            .map(String::from)
            .unwrap_or_else(|| {
                let suffix = Uuid::new_v4().simple().to_string();
                format!("{}-{}", resource.logical_id, &suffix[..8])
            });
        let role_arn = props
            .get("RoleArn")
            .and_then(|v| v.as_str())
            .ok_or("RoleArn is required")?
            .to_string();
        let machine_type_str = props
            .get("StateMachineType")
            .and_then(|v| v.as_str())
            .unwrap_or("STANDARD");
        let machine_type = StateMachineType::parse(machine_type_str)
            .ok_or_else(|| format!("Invalid StateMachineType: {machine_type_str}"))?;
        let definition = props
            .get("DefinitionString")
            .and_then(|v| v.as_str())
            .map(String::from)
            .or_else(|| {
                props
                    .get("Definition")
                    .map(|v| serde_json::to_string(v).unwrap_or_default())
            })
            .ok_or("Definition or DefinitionString is required")?;
        let logging_configuration = props.get("LoggingConfiguration").cloned();
        let tracing_configuration = props.get("TracingConfiguration").cloned();

        let arn = format!(
            "arn:aws:states:{}:{}:stateMachine:{}",
            self.region, self.account_id, name
        );
        let now = Utc::now();
        let revision_id = Uuid::new_v4().to_string();

        let sm = StateMachine {
            name: name.clone(),
            arn: arn.clone(),
            definition,
            role_arn,
            machine_type,
            status: StateMachineStatus::Active,
            creation_date: now,
            update_date: now,
            tags: BTreeMap::new(),
            revision_id,
            logging_configuration,
            tracing_configuration,
            description: String::new(),
        };

        let mut accounts = self.stepfunctions_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.state_machines.insert(arn.clone(), sm);

        Ok(ProvisionResult::new(arn.clone())
            .with("Arn", arn.clone())
            .with("Name", name)
            .with("StateMachineRevisionId", "INITIAL"))
    }

    fn delete_sfn_state_machine(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.stepfunctions_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.state_machines.remove(physical_id);
        Ok(())
    }

    fn create_sfn_activity(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let name = props
            .get("Name")
            .and_then(|v| v.as_str())
            .ok_or("Name is required")?
            .to_string();
        let arn = format!(
            "arn:aws:states:{}:{}:activity:{}",
            self.region, self.account_id, name
        );
        let activity = SfnActivity {
            name: name.clone(),
            arn: arn.clone(),
            creation_date: Utc::now(),
            tags: BTreeMap::new(),
        };

        let mut accounts = self.stepfunctions_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.activities.insert(arn.clone(), activity);

        Ok(ProvisionResult::new(arn.clone())
            .with("Arn", arn)
            .with("Name", name))
    }

    fn delete_sfn_activity(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.stepfunctions_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.activities.remove(physical_id);
        Ok(())
    }

    fn create_sfn_version(&self, resource: &ResourceDefinition) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let sm_arn = props
            .get("StateMachineArn")
            .and_then(|v| v.as_str())
            .ok_or("StateMachineArn is required")?
            .to_string();
        let description = props
            .get("Description")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let revision_id = props
            .get("StateMachineRevisionId")
            .and_then(|v| v.as_str())
            .unwrap_or("INITIAL")
            .to_string();

        let mut accounts = self.stepfunctions_state.write();
        let state = accounts.get_or_create(&self.account_id);

        // Derive next version number for this state machine.
        let next_version = state
            .state_machine_versions
            .values()
            .filter(|v| v.state_machine_arn == sm_arn)
            .map(|v| v.version)
            .max()
            .unwrap_or(0)
            + 1;
        let version_arn = format!("{sm_arn}:{next_version}");

        let version = StateMachineVersion {
            state_machine_arn: sm_arn,
            version: next_version,
            revision_id,
            description,
            creation_date: Utc::now(),
        };
        state
            .state_machine_versions
            .insert(version_arn.clone(), version);

        Ok(ProvisionResult::new(version_arn.clone()).with("Arn", version_arn))
    }

    fn delete_sfn_version(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.stepfunctions_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.state_machine_versions.remove(physical_id);
        Ok(())
    }

    fn create_sfn_alias(&self, resource: &ResourceDefinition) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let name = props
            .get("Name")
            .and_then(|v| v.as_str())
            .ok_or("Name is required")?
            .to_string();
        let description = props
            .get("Description")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let routes_value = props
            .get("RoutingConfiguration")
            .and_then(|v| v.as_array())
            .ok_or("RoutingConfiguration is required")?;
        let routing_configuration: Vec<AliasRoute> = routes_value
            .iter()
            .map(|r| AliasRoute {
                state_machine_version_arn: r
                    .get("StateMachineVersionArn")
                    .and_then(|x| x.as_str())
                    .unwrap_or("")
                    .to_string(),
                weight: r
                    .get("Weight")
                    .and_then(|x| {
                        x.as_i64()
                            .or_else(|| x.as_str().and_then(|s| s.parse::<i64>().ok()))
                    })
                    .map(|w| w as i32)
                    .unwrap_or(0),
            })
            .collect();

        let first_version_arn = routing_configuration
            .first()
            .map(|r| r.state_machine_version_arn.clone())
            .unwrap_or_default();
        // Alias ARN derives from the parent state machine ARN (everything
        // before `:<version>`) + the alias name.
        let sm_arn_root = first_version_arn
            .rsplit_once(':')
            .map(|(root, _)| root.to_string())
            .unwrap_or_else(|| {
                format!(
                    "arn:aws:states:{}:{}:stateMachine:unknown",
                    self.region, self.account_id
                )
            });
        let arn = format!("{sm_arn_root}:{name}");
        let now = Utc::now();
        let alias = StateMachineAlias {
            name: name.clone(),
            arn: arn.clone(),
            description,
            routing_configuration,
            creation_date: now,
            update_date: now,
        };

        let mut accounts = self.stepfunctions_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.state_machine_aliases.insert(arn.clone(), alias);

        Ok(ProvisionResult::new(arn.clone())
            .with("Arn", arn)
            .with("Name", name))
    }

    fn delete_sfn_alias(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.stepfunctions_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.state_machine_aliases.remove(physical_id);
        Ok(())
    }

    // --- WAFv2 ---
    //
    // CFN exclusively writes WAFv2 resources at the global scope
    // (`CLOUDFRONT`) for global resources or `REGIONAL` for everything
    // else. We honor whatever the template specifies via the `Scope`
    // property and store under (scope, name).

    fn create_wafv2_web_acl(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let name = props
            .get("Name")
            .and_then(|v| v.as_str())
            .ok_or("Name is required")?
            .to_string();
        let scope = props
            .get("Scope")
            .and_then(|v| v.as_str())
            .ok_or("Scope is required")?
            .to_string();
        let default_action = props
            .get("DefaultAction")
            .cloned()
            .unwrap_or_else(|| serde_json::json!({"Allow": {}}));
        let description = props
            .get("Description")
            .and_then(|v| v.as_str())
            .map(String::from);
        let rules = props
            .get("Rules")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        let visibility_config = props
            .get("VisibilityConfig")
            .cloned()
            .unwrap_or_else(|| serde_json::json!({}));
        let capacity = props.get("Capacity").and_then(|v| v.as_i64()).unwrap_or(0);

        let id = Uuid::new_v4().to_string();
        let (region_in_arn, scope_seg): (&str, String) = if scope == "CLOUDFRONT" {
            ("us-east-1", "global".to_string())
        } else {
            (self.region.as_str(), self.region.clone())
        };
        let arn = format!(
            "arn:aws:wafv2:{}:{}:{}/webacl/{}/{}",
            region_in_arn, self.account_id, scope_seg, name, id
        );
        let acl = WebAcl {
            id: id.clone(),
            name: name.clone(),
            arn: arn.clone(),
            scope: scope.clone(),
            default_action,
            description,
            rules,
            visibility_config,
            capacity,
            lock_token: Uuid::new_v4().simple().to_string(),
            label_namespace: format!("awswaf:{}:webacl:{}:", self.account_id, name),
            custom_response_bodies: BTreeMap::new(),
            captcha_config: None,
            challenge_config: None,
            token_domains: Vec::new(),
            association_config: None,
            data_protection_config: None,
            on_source_d_do_s_protection_config: None,
            application_config: None,
            retrofitted_by_firewall_manager: false,
            pre_process_firewall_manager_rule_groups: Vec::new(),
            post_process_firewall_manager_rule_groups: Vec::new(),
            managed_by_firewall_manager: false,
            created_time: Utc::now(),
        };

        let mut accounts = self.wafv2_state.write();
        let state = accounts
            .accounts
            .entry(self.account_id.clone())
            .or_default();
        state.web_acls.insert((scope.clone(), name.clone()), acl);

        Ok(ProvisionResult::new(arn.clone())
            .with("Arn", arn)
            .with("Id", id)
            .with("Name", name)
            .with("Capacity", capacity.to_string()))
    }

    fn delete_wafv2_web_acl(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.wafv2_state.write();
        let state = accounts
            .accounts
            .entry(self.account_id.clone())
            .or_default();
        state.web_acls.retain(|_, v| v.arn != physical_id);
        Ok(())
    }

    fn create_wafv2_ip_set(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let name = props
            .get("Name")
            .and_then(|v| v.as_str())
            .ok_or("Name is required")?
            .to_string();
        let scope = props
            .get("Scope")
            .and_then(|v| v.as_str())
            .ok_or("Scope is required")?
            .to_string();
        let ip_address_version = props
            .get("IPAddressVersion")
            .and_then(|v| v.as_str())
            .ok_or("IPAddressVersion is required")?
            .to_string();
        let addresses: Vec<String> = props
            .get("Addresses")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();
        let description = props
            .get("Description")
            .and_then(|v| v.as_str())
            .map(String::from);

        let id = Uuid::new_v4().to_string();
        let (region_in_arn, scope_seg): (&str, String) = if scope == "CLOUDFRONT" {
            ("us-east-1", "global".to_string())
        } else {
            (self.region.as_str(), self.region.clone())
        };
        let arn = format!(
            "arn:aws:wafv2:{}:{}:{}/ipset/{}/{}",
            region_in_arn, self.account_id, scope_seg, name, id
        );
        let ip_set = IpSet {
            id: id.clone(),
            name: name.clone(),
            arn: arn.clone(),
            scope: scope.clone(),
            description,
            ip_address_version,
            addresses,
            lock_token: Uuid::new_v4().simple().to_string(),
            created_time: Utc::now(),
        };

        let mut accounts = self.wafv2_state.write();
        let state = accounts
            .accounts
            .entry(self.account_id.clone())
            .or_default();
        state.ip_sets.insert((scope, name.clone()), ip_set);

        Ok(ProvisionResult::new(arn.clone())
            .with("Arn", arn)
            .with("Id", id)
            .with("Name", name))
    }

    fn delete_wafv2_ip_set(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.wafv2_state.write();
        let state = accounts
            .accounts
            .entry(self.account_id.clone())
            .or_default();
        state.ip_sets.retain(|_, v| v.arn != physical_id);
        Ok(())
    }

    fn create_wafv2_regex_pattern_set(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let name = props
            .get("Name")
            .and_then(|v| v.as_str())
            .ok_or("Name is required")?
            .to_string();
        let scope = props
            .get("Scope")
            .and_then(|v| v.as_str())
            .ok_or("Scope is required")?
            .to_string();
        let regular_expressions: Vec<serde_json::Value> = props
            .get("RegularExpressionList")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .map(|s| {
                        if let Some(s) = s.as_str() {
                            serde_json::json!({"RegexString": s})
                        } else {
                            s.clone()
                        }
                    })
                    .collect()
            })
            .unwrap_or_default();
        let description = props
            .get("Description")
            .and_then(|v| v.as_str())
            .map(String::from);

        let id = Uuid::new_v4().to_string();
        let (region_in_arn, scope_seg): (&str, String) = if scope == "CLOUDFRONT" {
            ("us-east-1", "global".to_string())
        } else {
            (self.region.as_str(), self.region.clone())
        };
        let arn = format!(
            "arn:aws:wafv2:{}:{}:{}/regexpatternset/{}/{}",
            region_in_arn, self.account_id, scope_seg, name, id
        );
        let set = RegexPatternSet {
            id: id.clone(),
            name: name.clone(),
            arn: arn.clone(),
            scope: scope.clone(),
            description,
            regular_expressions,
            lock_token: Uuid::new_v4().simple().to_string(),
            created_time: Utc::now(),
        };

        let mut accounts = self.wafv2_state.write();
        let state = accounts
            .accounts
            .entry(self.account_id.clone())
            .or_default();
        state.regex_pattern_sets.insert((scope, name.clone()), set);

        Ok(ProvisionResult::new(arn.clone())
            .with("Arn", arn)
            .with("Id", id)
            .with("Name", name))
    }

    fn delete_wafv2_regex_pattern_set(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.wafv2_state.write();
        let state = accounts
            .accounts
            .entry(self.account_id.clone())
            .or_default();
        state.regex_pattern_sets.retain(|_, v| v.arn != physical_id);
        Ok(())
    }

    fn create_wafv2_rule_group(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let name = props
            .get("Name")
            .and_then(|v| v.as_str())
            .ok_or("Name is required")?
            .to_string();
        let scope = props
            .get("Scope")
            .and_then(|v| v.as_str())
            .ok_or("Scope is required")?
            .to_string();
        let capacity = props
            .get("Capacity")
            .and_then(|v| v.as_i64())
            .ok_or("Capacity is required")?;
        let description = props
            .get("Description")
            .and_then(|v| v.as_str())
            .map(String::from);
        let rules = props
            .get("Rules")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        let visibility_config = props
            .get("VisibilityConfig")
            .cloned()
            .unwrap_or_else(|| serde_json::json!({}));

        let id = Uuid::new_v4().to_string();
        let (region_in_arn, scope_seg): (&str, String) = if scope == "CLOUDFRONT" {
            ("us-east-1", "global".to_string())
        } else {
            (self.region.as_str(), self.region.clone())
        };
        let arn = format!(
            "arn:aws:wafv2:{}:{}:{}/rulegroup/{}/{}",
            region_in_arn, self.account_id, scope_seg, name, id
        );
        let rg = RuleGroup {
            id: id.clone(),
            name: name.clone(),
            arn: arn.clone(),
            scope: scope.clone(),
            capacity,
            description,
            rules,
            visibility_config,
            lock_token: Uuid::new_v4().simple().to_string(),
            label_namespace: format!("awswaf:{}:rulegroup:{}:", self.account_id, name),
            custom_response_bodies: BTreeMap::new(),
            available_labels: Vec::new(),
            consumed_labels: Vec::new(),
            created_time: Utc::now(),
        };

        let mut accounts = self.wafv2_state.write();
        let state = accounts
            .accounts
            .entry(self.account_id.clone())
            .or_default();
        state.rule_groups.insert((scope, name.clone()), rg);

        Ok(ProvisionResult::new(arn.clone())
            .with("Arn", arn)
            .with("Id", id)
            .with("Name", name)
            .with("Capacity", capacity.to_string()))
    }

    fn delete_wafv2_rule_group(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.wafv2_state.write();
        let state = accounts
            .accounts
            .entry(self.account_id.clone())
            .or_default();
        state.rule_groups.retain(|_, v| v.arn != physical_id);
        Ok(())
    }

    fn create_wafv2_logging_configuration(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let resource_arn = props
            .get("ResourceArn")
            .and_then(|v| v.as_str())
            .ok_or("ResourceArn is required")?
            .to_string();
        let cfg = serde_json::json!({
            "ResourceArn": resource_arn,
            "LogDestinationConfigs": props.get("LogDestinationConfigs").cloned().unwrap_or_else(|| serde_json::json!([])),
            "RedactedFields": props.get("RedactedFields").cloned().unwrap_or_else(|| serde_json::json!([])),
            "LoggingFilter": props.get("LoggingFilter").cloned(),
        });

        let mut accounts = self.wafv2_state.write();
        let state = accounts
            .accounts
            .entry(self.account_id.clone())
            .or_default();
        state.logging_configs.insert(resource_arn.clone(), cfg);

        Ok(ProvisionResult::new(resource_arn))
    }

    fn delete_wafv2_logging_configuration(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.wafv2_state.write();
        let state = accounts
            .accounts
            .entry(self.account_id.clone())
            .or_default();
        state.logging_configs.remove(physical_id);
        Ok(())
    }

    fn create_wafv2_web_acl_association(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let resource_arn = props
            .get("ResourceArn")
            .and_then(|v| v.as_str())
            .ok_or("ResourceArn is required")?
            .to_string();
        let web_acl_arn = props
            .get("WebACLArn")
            .and_then(|v| v.as_str())
            .ok_or("WebACLArn is required")?
            .to_string();

        let mut accounts = self.wafv2_state.write();
        let state = accounts
            .accounts
            .entry(self.account_id.clone())
            .or_default();
        state.associations.insert(resource_arn.clone(), web_acl_arn);

        // Physical id encodes the resource arn so delete can find it.
        Ok(ProvisionResult::new(resource_arn))
    }

    fn delete_wafv2_web_acl_association(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.wafv2_state.write();
        let state = accounts
            .accounts
            .entry(self.account_id.clone())
            .or_default();
        state.associations.remove(physical_id);
        Ok(())
    }

    // --- API Gateway v1 ---

    fn create_apigw_rest_api(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let name = props
            .get("Name")
            .and_then(|v| v.as_str())
            .ok_or("Name is required")?
            .to_string();
        let description = props
            .get("Description")
            .and_then(|v| v.as_str())
            .map(String::from);
        let api_key_source = props
            .get("ApiKeySourceType")
            .and_then(|v| v.as_str())
            .unwrap_or("HEADER")
            .to_string();
        let endpoint_configuration = props
            .get("EndpointConfiguration")
            .cloned()
            .unwrap_or_else(|| serde_json::json!({"types": ["EDGE"]}));
        let policy = props
            .get("Policy")
            .map(|v| v.to_string().trim_matches('"').to_string());
        let binary_media_types: Vec<String> = props
            .get("BinaryMediaTypes")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();
        let minimum_compression_size = props.get("MinimumCompressionSize").and_then(|v| v.as_i64());
        let disable_execute_api_endpoint = props
            .get("DisableExecuteApiEndpoint")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        // CFN exposes optional `Body`/`BodyS3Location`/`CloneFrom` for OpenAPI
        // import. We don't run a full Swagger import — we record the source
        // in the api's `import_source` field so callers can reason about it.
        let import_source = if props.get("Body").is_some() {
            Some("Body".to_string())
        } else if props.get("BodyS3Location").is_some() {
            Some("BodyS3Location".to_string())
        } else if props.get("CloneFrom").is_some() {
            Some("CloneFrom".to_string())
        } else {
            None
        };
        let tags = parse_acm_tags(props.get("Tags"));

        let id = apigw_make_id();
        let root_resource_id = apigw_make_id();
        let now = Utc::now();

        let api = ApiGwRestApi {
            id: id.clone(),
            name,
            description,
            version: props
                .get("Version")
                .and_then(|v| v.as_str())
                .map(String::from),
            created_date: now,
            api_key_source,
            endpoint_configuration,
            policy,
            binary_media_types,
            minimum_compression_size,
            disable_execute_api_endpoint,
            root_resource_id: root_resource_id.clone(),
            tags,
            import_source,
        };

        let mut accounts = self.apigateway_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.apis.insert(id.clone(), api);
        let mut resources = BTreeMap::new();
        resources.insert(
            root_resource_id.clone(),
            ApiGwResource {
                id: root_resource_id.clone(),
                parent_id: None,
                path_part: None,
                path: "/".to_string(),
            },
        );
        state.resources.insert(id.clone(), resources);

        Ok(ProvisionResult::new(id.clone())
            .with("RestApiId", id.clone())
            .with("RootResourceId", root_resource_id))
    }

    fn update_apigw_rest_api(
        &self,
        existing: &StackResource,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let id = existing.physical_id.clone();
        let mut accounts = self.apigateway_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let api = state
            .apis
            .get_mut(&id)
            .ok_or_else(|| format!("RestApi {id} not found for update"))?;
        if let Some(name) = props.get("Name").and_then(|v| v.as_str()) {
            api.name = name.to_string();
        }
        if let Some(desc) = props.get("Description").and_then(|v| v.as_str()) {
            api.description = Some(desc.to_string());
        }
        if let Some(source) = props.get("ApiKeySourceType").and_then(|v| v.as_str()) {
            api.api_key_source = source.to_string();
        }
        if let Some(ep) = props.get("EndpointConfiguration").cloned() {
            api.endpoint_configuration = ep;
        }
        if let Some(arr) = props.get("BinaryMediaTypes").and_then(|v| v.as_array()) {
            api.binary_media_types = arr
                .iter()
                .filter_map(|v| v.as_str().map(String::from))
                .collect();
        }
        if let Some(size) = props.get("MinimumCompressionSize").and_then(|v| v.as_i64()) {
            api.minimum_compression_size = Some(size);
        }
        if let Some(b) = props
            .get("DisableExecuteApiEndpoint")
            .and_then(|v| v.as_bool())
        {
            api.disable_execute_api_endpoint = b;
        }
        if props.get("Tags").is_some() {
            api.tags = parse_acm_tags(props.get("Tags"));
        }
        let root = api.root_resource_id.clone();
        Ok(ProvisionResult::new(id.clone())
            .with("RestApiId", id)
            .with("RootResourceId", root))
    }

    fn delete_apigw_rest_api(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.apigateway_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.apis.remove(physical_id);
        state.resources.remove(physical_id);
        let prefix = format!("{physical_id}/");
        state.methods.retain(|k, _| !k.starts_with(&prefix));
        state.integrations.retain(|k, _| !k.starts_with(&prefix));
        state
            .integration_responses
            .retain(|k, _| !k.starts_with(&prefix));
        state
            .method_responses
            .retain(|k, _| !k.starts_with(&prefix));
        state.deployments.remove(physical_id);
        state.stages.remove(physical_id);
        state.models.remove(physical_id);
        state.request_validators.remove(physical_id);
        state.authorizers.remove(physical_id);
        state.gateway_responses.remove(physical_id);
        Ok(())
    }

    fn create_apigw_resource(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let rest_api_id = props
            .get("RestApiId")
            .and_then(|v| v.as_str())
            .ok_or("RestApiId is required")?
            .to_string();
        let parent_id = props
            .get("ParentId")
            .and_then(|v| v.as_str())
            .ok_or("ParentId is required")?
            .to_string();
        let path_part = props
            .get("PathPart")
            .and_then(|v| v.as_str())
            .ok_or("PathPart is required")?
            .to_string();

        let mut accounts = self.apigateway_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let api_resources = state
            .resources
            .get(&rest_api_id)
            .ok_or_else(|| format!("RestApi {rest_api_id} not found"))?;
        let parent = api_resources
            .get(&parent_id)
            .ok_or_else(|| format!("Parent resource {parent_id} not found"))?;
        let parent_path = parent.path.clone();
        let path = if parent_path == "/" {
            format!("/{path_part}")
        } else {
            format!("{parent_path}/{path_part}")
        };

        let id = apigw_make_id();
        let new_resource = ApiGwResource {
            id: id.clone(),
            parent_id: Some(parent_id),
            path_part: Some(path_part),
            path,
        };
        state
            .resources
            .entry(rest_api_id.clone())
            .or_default()
            .insert(id.clone(), new_resource);

        Ok(ProvisionResult::new(id.clone())
            .with("ResourceId", id)
            .with("RestApiId", rest_api_id))
    }

    fn delete_apigw_resource(
        &self,
        physical_id: &str,
        attributes: &BTreeMap<String, String>,
    ) -> Result<(), String> {
        let Some(rest_api_id) = attributes.get("RestApiId") else {
            return Ok(());
        };
        let mut accounts = self.apigateway_state.write();
        let state = accounts.get_or_create(&self.account_id);
        if let Some(map) = state.resources.get_mut(rest_api_id) {
            map.remove(physical_id);
        }
        let prefix = format!("{rest_api_id}/{physical_id}/");
        state.methods.retain(|k, _| !k.starts_with(&prefix));
        state.integrations.retain(|k, _| !k.starts_with(&prefix));
        Ok(())
    }

    fn create_apigw_method(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let rest_api_id = props
            .get("RestApiId")
            .and_then(|v| v.as_str())
            .ok_or("RestApiId is required")?
            .to_string();
        let resource_id = props
            .get("ResourceId")
            .and_then(|v| v.as_str())
            .ok_or("ResourceId is required")?
            .to_string();
        let http_method = props
            .get("HttpMethod")
            .and_then(|v| v.as_str())
            .ok_or("HttpMethod is required")?
            .to_uppercase();
        let authorization_type = props
            .get("AuthorizationType")
            .and_then(|v| v.as_str())
            .unwrap_or("NONE")
            .to_string();
        let authorizer_id = props
            .get("AuthorizerId")
            .and_then(|v| v.as_str())
            .map(String::from);
        let api_key_required = props
            .get("ApiKeyRequired")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let operation_name = props
            .get("OperationName")
            .and_then(|v| v.as_str())
            .map(String::from);
        let request_validator_id = props
            .get("RequestValidatorId")
            .and_then(|v| v.as_str())
            .map(String::from);
        let request_parameters: BTreeMap<String, bool> = props
            .get("RequestParameters")
            .and_then(|v| v.as_object())
            .map(|obj| {
                obj.iter()
                    .map(|(k, v)| (k.clone(), v.as_bool().unwrap_or(false)))
                    .collect()
            })
            .unwrap_or_default();
        let request_models: BTreeMap<String, String> = props
            .get("RequestModels")
            .and_then(|v| v.as_object())
            .map(|obj| {
                obj.iter()
                    .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                    .collect()
            })
            .unwrap_or_default();
        let authorization_scopes: Vec<String> = props
            .get("AuthorizationScopes")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();

        let composite_key = format!("{rest_api_id}/{resource_id}/{http_method}");
        let method = ApiGwMethod {
            rest_api_id: rest_api_id.clone(),
            resource_id: resource_id.clone(),
            http_method: http_method.clone(),
            authorization_type,
            authorizer_id,
            api_key_required,
            operation_name,
            request_parameters,
            request_models,
            request_validator_id,
            authorization_scopes,
        };

        let mut accounts = self.apigateway_state.write();
        let state = accounts.get_or_create(&self.account_id);
        if !state.apis.contains_key(&rest_api_id) {
            return Err(format!("RestApi {rest_api_id} not found"));
        }
        // Multi-pass provisioning: if `Ref: SomeResource` resolved to the
        // logical id (because the referenced resource hasn't been
        // provisioned yet on this pass), bail so CFN retries us next pass.
        let resource_known = state
            .resources
            .get(&rest_api_id)
            .map(|m| m.contains_key(&resource_id))
            .unwrap_or(false);
        if !resource_known {
            return Err(format!(
                "Resource {resource_id} not yet provisioned for api {rest_api_id}"
            ));
        }
        state.methods.insert(composite_key.clone(), method);

        if let Some(integ_props) = props.get("Integration").and_then(|v| v.as_object()) {
            let integration = ApiGwIntegration {
                rest_api_id: rest_api_id.clone(),
                resource_id: resource_id.clone(),
                http_method: http_method.clone(),
                integration_type: integ_props
                    .get("Type")
                    .and_then(|v| v.as_str())
                    .unwrap_or("MOCK")
                    .to_string(),
                integration_http_method: integ_props
                    .get("IntegrationHttpMethod")
                    .and_then(|v| v.as_str())
                    .map(String::from),
                uri: integ_props
                    .get("Uri")
                    .and_then(|v| v.as_str())
                    .map(String::from),
                credentials: integ_props
                    .get("Credentials")
                    .and_then(|v| v.as_str())
                    .map(String::from),
                request_parameters: integ_props
                    .get("RequestParameters")
                    .and_then(|v| v.as_object())
                    .map(|obj| {
                        obj.iter()
                            .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                            .collect()
                    })
                    .unwrap_or_default(),
                request_templates: integ_props
                    .get("RequestTemplates")
                    .and_then(|v| v.as_object())
                    .map(|obj| {
                        obj.iter()
                            .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                            .collect()
                    })
                    .unwrap_or_default(),
                passthrough_behavior: integ_props
                    .get("PassthroughBehavior")
                    .and_then(|v| v.as_str())
                    .unwrap_or("WHEN_NO_MATCH")
                    .to_string(),
                timeout_in_millis: integ_props
                    .get("TimeoutInMillis")
                    .and_then(|v| v.as_i64())
                    .map(|n| n as i32),
                cache_namespace: integ_props
                    .get("CacheNamespace")
                    .and_then(|v| v.as_str())
                    .map(String::from),
                cache_key_parameters: integ_props
                    .get("CacheKeyParameters")
                    .and_then(|v| v.as_array())
                    .map(|arr| {
                        arr.iter()
                            .filter_map(|v| v.as_str().map(String::from))
                            .collect()
                    })
                    .unwrap_or_default(),
                content_handling: integ_props
                    .get("ContentHandling")
                    .and_then(|v| v.as_str())
                    .map(String::from),
                connection_type: integ_props
                    .get("ConnectionType")
                    .and_then(|v| v.as_str())
                    .map(String::from),
                connection_id: integ_props
                    .get("ConnectionId")
                    .and_then(|v| v.as_str())
                    .map(String::from),
                tls_config: integ_props.get("TlsConfig").cloned(),
            };
            state
                .integrations
                .insert(composite_key.clone(), integration);
        }

        Ok(ProvisionResult::new(composite_key.clone())
            .with("MethodKey", composite_key)
            .with("RestApiId", rest_api_id)
            .with("ResourceId", resource_id)
            .with("HttpMethod", http_method))
    }

    fn delete_apigw_method(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.apigateway_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.methods.remove(physical_id);
        state.integrations.remove(physical_id);
        let prefix = format!("{physical_id}/");
        state
            .integration_responses
            .retain(|k, _| !k.starts_with(&prefix));
        state
            .method_responses
            .retain(|k, _| !k.starts_with(&prefix));
        Ok(())
    }

    fn create_apigw_deployment(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let rest_api_id = props
            .get("RestApiId")
            .and_then(|v| v.as_str())
            .ok_or("RestApiId is required")?
            .to_string();
        let description = props
            .get("Description")
            .and_then(|v| v.as_str())
            .map(String::from);

        let id = apigw_make_id();
        let mut accounts = self.apigateway_state.write();
        let state = accounts.get_or_create(&self.account_id);
        if !state.apis.contains_key(&rest_api_id) {
            return Err(format!("RestApi {rest_api_id} not found"));
        }
        let api_summary = serde_json::to_value(
            state
                .resources
                .get(&rest_api_id)
                .cloned()
                .unwrap_or_default(),
        )
        .unwrap_or(serde_json::Value::Null);
        let deployment = ApiGwDeployment {
            id: id.clone(),
            description,
            created_date: Utc::now(),
            api_summary,
        };
        state
            .deployments
            .entry(rest_api_id.clone())
            .or_default()
            .insert(id.clone(), deployment);

        // CFN inline `StageName` creates a Stage referencing this deployment.
        if let Some(stage_name) = props
            .get("StageName")
            .and_then(|v| v.as_str())
            .map(String::from)
        {
            let stage = ApiGwStage {
                stage_name: stage_name.clone(),
                deployment_id: id.clone(),
                description: props
                    .get("StageDescription")
                    .and_then(|v| v.get("Description"))
                    .and_then(|v| v.as_str())
                    .map(String::from),
                cache_cluster_enabled: false,
                cache_cluster_size: None,
                variables: BTreeMap::new(),
                method_settings: BTreeMap::new(),
                created_date: Utc::now(),
                last_updated_date: Utc::now(),
                tracing_enabled: false,
                web_acl_arn: None,
                canary_settings: None,
                access_log_settings: None,
                tags: BTreeMap::new(),
            };
            state
                .stages
                .entry(rest_api_id.clone())
                .or_default()
                .insert(stage_name, stage);
        }

        Ok(ProvisionResult::new(id.clone())
            .with("DeploymentId", id)
            .with("RestApiId", rest_api_id))
    }

    fn delete_apigw_deployment(
        &self,
        physical_id: &str,
        attributes: &BTreeMap<String, String>,
    ) -> Result<(), String> {
        let Some(rest_api_id) = attributes.get("RestApiId") else {
            return Ok(());
        };
        let mut accounts = self.apigateway_state.write();
        let state = accounts.get_or_create(&self.account_id);
        if let Some(map) = state.deployments.get_mut(rest_api_id) {
            map.remove(physical_id);
        }
        Ok(())
    }

    fn create_apigw_stage(&self, resource: &ResourceDefinition) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let rest_api_id = props
            .get("RestApiId")
            .and_then(|v| v.as_str())
            .ok_or("RestApiId is required")?
            .to_string();
        let stage_name = props
            .get("StageName")
            .and_then(|v| v.as_str())
            .ok_or("StageName is required")?
            .to_string();
        let deployment_id = props
            .get("DeploymentId")
            .and_then(|v| v.as_str())
            .ok_or("DeploymentId is required")?
            .to_string();

        let variables: BTreeMap<String, String> = props
            .get("Variables")
            .and_then(|v| v.as_object())
            .map(|obj| {
                obj.iter()
                    .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                    .collect()
            })
            .unwrap_or_default();
        let tracing_enabled = props
            .get("TracingEnabled")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let cache_cluster_enabled = props
            .get("CacheClusterEnabled")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let cache_cluster_size = props
            .get("CacheClusterSize")
            .and_then(|v| v.as_str())
            .map(String::from);
        // CFN models MethodSettings as a list of `{ResourcePath,HttpMethod,...}`
        // entries; the live API stores them as a `path/method -> settings`
        // map. Translate by joining ResourcePath + HttpMethod into the key.
        let method_settings: BTreeMap<String, serde_json::Value> = props
            .get("MethodSettings")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|s| {
                        let path = s.get("ResourcePath").and_then(|v| v.as_str())?;
                        let http = s.get("HttpMethod").and_then(|v| v.as_str())?;
                        let key = format!("{}/{http}", path.strip_prefix('/').unwrap_or(path));
                        Some((key, s.clone()))
                    })
                    .collect()
            })
            .unwrap_or_default();
        let tags = parse_acm_tags(props.get("Tags"));

        let stage = ApiGwStage {
            stage_name: stage_name.clone(),
            deployment_id,
            description: props
                .get("Description")
                .and_then(|v| v.as_str())
                .map(String::from),
            cache_cluster_enabled,
            cache_cluster_size,
            variables,
            method_settings,
            created_date: Utc::now(),
            last_updated_date: Utc::now(),
            tracing_enabled,
            web_acl_arn: None,
            canary_settings: props.get("CanarySetting").cloned(),
            access_log_settings: props.get("AccessLogSetting").cloned(),
            tags,
        };

        let mut accounts = self.apigateway_state.write();
        let state = accounts.get_or_create(&self.account_id);
        if !state.apis.contains_key(&rest_api_id) {
            return Err(format!("RestApi {rest_api_id} not found"));
        }
        let dep_known = state
            .deployments
            .get(&rest_api_id)
            .map(|m| m.contains_key(&stage.deployment_id))
            .unwrap_or(false);
        if !dep_known {
            return Err(format!(
                "Deployment {} not yet provisioned for api {rest_api_id}",
                stage.deployment_id
            ));
        }
        state
            .stages
            .entry(rest_api_id.clone())
            .or_default()
            .insert(stage_name.clone(), stage);

        Ok(ProvisionResult::new(stage_name.clone())
            .with("StageName", stage_name)
            .with("RestApiId", rest_api_id))
    }

    fn delete_apigw_stage(
        &self,
        physical_id: &str,
        attributes: &BTreeMap<String, String>,
    ) -> Result<(), String> {
        let Some(rest_api_id) = attributes.get("RestApiId") else {
            return Ok(());
        };
        let mut accounts = self.apigateway_state.write();
        let state = accounts.get_or_create(&self.account_id);
        if let Some(map) = state.stages.get_mut(rest_api_id) {
            map.remove(physical_id);
        }
        Ok(())
    }

    fn create_apigw_authorizer(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let rest_api_id = props
            .get("RestApiId")
            .and_then(|v| v.as_str())
            .ok_or("RestApiId is required")?
            .to_string();
        let name = props
            .get("Name")
            .and_then(|v| v.as_str())
            .ok_or("Name is required")?
            .to_string();
        let authorizer_type = props
            .get("Type")
            .and_then(|v| v.as_str())
            .unwrap_or("TOKEN")
            .to_string();
        let provider_arns: Vec<String> = props
            .get("ProviderARNs")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();

        let id = apigw_make_id();
        let auth = ApiGwAuthorizer {
            id: id.clone(),
            name,
            authorizer_type,
            provider_arns,
            auth_type: props
                .get("AuthType")
                .and_then(|v| v.as_str())
                .map(String::from),
            authorizer_uri: props
                .get("AuthorizerUri")
                .and_then(|v| v.as_str())
                .map(String::from),
            authorizer_credentials: props
                .get("AuthorizerCredentials")
                .and_then(|v| v.as_str())
                .map(String::from),
            identity_source: props
                .get("IdentitySource")
                .and_then(|v| v.as_str())
                .map(String::from),
            identity_validation_expression: props
                .get("IdentityValidationExpression")
                .and_then(|v| v.as_str())
                .map(String::from),
            authorizer_result_ttl_in_seconds: props
                .get("AuthorizerResultTtlInSeconds")
                .and_then(|v| v.as_i64())
                .map(|n| n as i32),
        };

        let mut accounts = self.apigateway_state.write();
        let state = accounts.get_or_create(&self.account_id);
        if !state.apis.contains_key(&rest_api_id) {
            return Err(format!("RestApi {rest_api_id} not found"));
        }
        state
            .authorizers
            .entry(rest_api_id.clone())
            .or_default()
            .insert(id.clone(), auth);

        Ok(ProvisionResult::new(id.clone())
            .with("AuthorizerId", id)
            .with("RestApiId", rest_api_id))
    }

    fn delete_apigw_authorizer(
        &self,
        physical_id: &str,
        attributes: &BTreeMap<String, String>,
    ) -> Result<(), String> {
        let Some(rest_api_id) = attributes.get("RestApiId") else {
            return Ok(());
        };
        let mut accounts = self.apigateway_state.write();
        let state = accounts.get_or_create(&self.account_id);
        if let Some(map) = state.authorizers.get_mut(rest_api_id) {
            map.remove(physical_id);
        }
        Ok(())
    }

    fn create_apigw_request_validator(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let rest_api_id = props
            .get("RestApiId")
            .and_then(|v| v.as_str())
            .ok_or("RestApiId is required")?
            .to_string();
        let name = props.get("Name").and_then(|v| v.as_str()).map(String::from);
        let validate_body = props
            .get("ValidateRequestBody")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let validate_params = props
            .get("ValidateRequestParameters")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let id = apigw_make_id();
        let body = serde_json::json!({
            "id": id,
            "name": name,
            "validateRequestBody": validate_body,
            "validateRequestParameters": validate_params,
        });
        let mut accounts = self.apigateway_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state
            .request_validators
            .entry(rest_api_id.clone())
            .or_default()
            .insert(id.clone(), body);
        Ok(ProvisionResult::new(id.clone())
            .with("RequestValidatorId", id)
            .with("RestApiId", rest_api_id))
    }

    fn delete_apigw_request_validator(
        &self,
        physical_id: &str,
        attributes: &BTreeMap<String, String>,
    ) -> Result<(), String> {
        let Some(rest_api_id) = attributes.get("RestApiId") else {
            return Ok(());
        };
        let mut accounts = self.apigateway_state.write();
        let state = accounts.get_or_create(&self.account_id);
        if let Some(map) = state.request_validators.get_mut(rest_api_id) {
            map.remove(physical_id);
        }
        Ok(())
    }

    fn create_apigw_model(&self, resource: &ResourceDefinition) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let rest_api_id = props
            .get("RestApiId")
            .and_then(|v| v.as_str())
            .ok_or("RestApiId is required")?
            .to_string();
        let name = props
            .get("Name")
            .and_then(|v| v.as_str())
            .ok_or("Name is required")?
            .to_string();
        let content_type = props
            .get("ContentType")
            .and_then(|v| v.as_str())
            .unwrap_or("application/json")
            .to_string();
        let schema = props.get("Schema").map(|v| {
            if let Some(s) = v.as_str() {
                s.to_string()
            } else {
                v.to_string()
            }
        });
        let id = apigw_make_id();
        let model = ApiGwModel {
            id: id.clone(),
            name: name.clone(),
            description: props
                .get("Description")
                .and_then(|v| v.as_str())
                .map(String::from),
            schema,
            content_type,
        };
        let mut accounts = self.apigateway_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state
            .models
            .entry(rest_api_id.clone())
            .or_default()
            .insert(name.clone(), model);
        Ok(ProvisionResult::new(name.clone())
            .with("ModelName", name)
            .with("RestApiId", rest_api_id))
    }

    fn delete_apigw_model(
        &self,
        physical_id: &str,
        attributes: &BTreeMap<String, String>,
    ) -> Result<(), String> {
        let Some(rest_api_id) = attributes.get("RestApiId") else {
            return Ok(());
        };
        let mut accounts = self.apigateway_state.write();
        let state = accounts.get_or_create(&self.account_id);
        if let Some(map) = state.models.get_mut(rest_api_id) {
            map.remove(physical_id);
        }
        Ok(())
    }

    fn create_apigw_gateway_response(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let rest_api_id = props
            .get("RestApiId")
            .and_then(|v| v.as_str())
            .ok_or("RestApiId is required")?
            .to_string();
        let response_type = props
            .get("ResponseType")
            .and_then(|v| v.as_str())
            .ok_or("ResponseType is required")?
            .to_string();
        let body = serde_json::json!({
            "responseType": response_type,
            "statusCode": props.get("StatusCode").and_then(|v| v.as_str()),
            "responseParameters": props.get("ResponseParameters").cloned().unwrap_or(serde_json::json!({})),
            "responseTemplates": props.get("ResponseTemplates").cloned().unwrap_or(serde_json::json!({})),
        });
        let mut accounts = self.apigateway_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state
            .gateway_responses
            .entry(rest_api_id.clone())
            .or_default()
            .insert(response_type.clone(), body);
        Ok(ProvisionResult::new(response_type.clone())
            .with("ResponseType", response_type)
            .with("RestApiId", rest_api_id))
    }

    fn delete_apigw_gateway_response(
        &self,
        physical_id: &str,
        attributes: &BTreeMap<String, String>,
    ) -> Result<(), String> {
        let Some(rest_api_id) = attributes.get("RestApiId") else {
            return Ok(());
        };
        let mut accounts = self.apigateway_state.write();
        let state = accounts.get_or_create(&self.account_id);
        if let Some(map) = state.gateway_responses.get_mut(rest_api_id) {
            map.remove(physical_id);
        }
        Ok(())
    }

    fn create_apigw_usage_plan(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let name = props
            .get("UsagePlanName")
            .and_then(|v| v.as_str())
            .ok_or("UsagePlanName is required")?
            .to_string();
        let id = apigw_make_id();
        let plan = ApiGwUsagePlan {
            id: id.clone(),
            name,
            description: props
                .get("Description")
                .and_then(|v| v.as_str())
                .map(String::from),
            api_stages: props
                .get("ApiStages")
                .and_then(|v| v.as_array())
                .cloned()
                .unwrap_or_default()
                .into_iter()
                .map(lowercase_first_keys)
                .collect(),
            throttle: props.get("Throttle").cloned().map(lowercase_first_keys),
            quota: props.get("Quota").cloned().map(lowercase_first_keys),
            product_code: None,
            tags: parse_acm_tags(props.get("Tags")),
        };
        let mut accounts = self.apigateway_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.usage_plans.insert(id.clone(), plan);
        Ok(ProvisionResult::new(id.clone()).with("UsagePlanId", id))
    }

    fn delete_apigw_usage_plan(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.apigateway_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.usage_plans.remove(physical_id);
        state.usage_plan_keys.remove(physical_id);
        Ok(())
    }

    fn create_apigw_api_key(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let generate_distinct_id = props
            .get("GenerateDistinctId")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let name = props
            .get("Name")
            .and_then(|v| v.as_str())
            .map(String::from)
            .unwrap_or_else(|| {
                if generate_distinct_id {
                    format!("cfn-key-{}-{}", resource.logical_id, apigw_make_id())
                } else {
                    format!("cfn-key-{}", resource.logical_id)
                }
            });
        let value = props
            .get("Value")
            .and_then(|v| v.as_str())
            .map(String::from)
            .unwrap_or_else(|| Uuid::new_v4().simple().to_string());
        let enabled = props
            .get("Enabled")
            .and_then(|v| v.as_bool())
            .unwrap_or(true);
        // CFN's StageKeys are `[{RestApiId, StageName}, ...]` — we store each
        // as `restApiId/stageName` per the live API key shape.
        let stage_keys: Vec<String> = props
            .get("StageKeys")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|s| {
                        let rest = s.get("RestApiId").and_then(|v| v.as_str())?;
                        let stage = s.get("StageName").and_then(|v| v.as_str())?;
                        Some(format!("{rest}/{stage}"))
                    })
                    .collect()
            })
            .unwrap_or_default();
        let id = apigw_make_id();
        let now = Utc::now();
        let key = ApiGwApiKey {
            id: id.clone(),
            value,
            name,
            description: props
                .get("Description")
                .and_then(|v| v.as_str())
                .map(String::from),
            enabled,
            created_date: now,
            last_updated_date: now,
            stage_keys,
            tags: parse_acm_tags(props.get("Tags")),
            customer_id: props
                .get("CustomerId")
                .and_then(|v| v.as_str())
                .map(String::from),
        };
        let mut accounts = self.apigateway_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.api_keys.insert(id.clone(), key);
        Ok(ProvisionResult::new(id.clone()).with("ApiKeyId", id))
    }

    fn delete_apigw_api_key(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.apigateway_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.api_keys.remove(physical_id);
        Ok(())
    }

    fn create_apigw_usage_plan_key(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let usage_plan_id = props
            .get("UsagePlanId")
            .and_then(|v| v.as_str())
            .ok_or("UsagePlanId is required")?
            .to_string();
        let key_id = props
            .get("KeyId")
            .and_then(|v| v.as_str())
            .ok_or("KeyId is required")?
            .to_string();
        let key_type = props
            .get("KeyType")
            .and_then(|v| v.as_str())
            .unwrap_or("API_KEY")
            .to_string();
        let body = serde_json::json!({
            "id": key_id,
            "type": key_type,
        });
        let mut accounts = self.apigateway_state.write();
        let state = accounts.get_or_create(&self.account_id);
        if !state.usage_plans.contains_key(&usage_plan_id) {
            return Err(format!("UsagePlan {usage_plan_id} not yet provisioned"));
        }
        if !state.api_keys.contains_key(&key_id) {
            return Err(format!("ApiKey {key_id} not yet provisioned"));
        }
        state
            .usage_plan_keys
            .entry(usage_plan_id.clone())
            .or_default()
            .insert(key_id.clone(), body);
        let physical = format!("{usage_plan_id}/{key_id}");
        Ok(ProvisionResult::new(physical)
            .with("UsagePlanId", usage_plan_id)
            .with("KeyId", key_id))
    }

    fn delete_apigw_usage_plan_key(
        &self,
        physical_id: &str,
        _attributes: &BTreeMap<String, String>,
    ) -> Result<(), String> {
        let mut parts = physical_id.splitn(2, '/');
        let Some(plan_id) = parts.next() else {
            return Ok(());
        };
        let Some(key_id) = parts.next() else {
            return Ok(());
        };
        let mut accounts = self.apigateway_state.write();
        let state = accounts.get_or_create(&self.account_id);
        if let Some(map) = state.usage_plan_keys.get_mut(plan_id) {
            map.remove(key_id);
        }
        Ok(())
    }

    fn create_apigw_domain_name(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let domain_name = props
            .get("DomainName")
            .and_then(|v| v.as_str())
            .ok_or("DomainName is required")?
            .to_string();
        let mtls = props
            .get("MutualTlsAuthentication")
            .cloned()
            .map(lowercase_first_keys);
        let regional_domain = format!(
            "d-{}.execute-api.{}.amazonaws.com",
            apigw_make_id(),
            self.region
        );
        let distribution_domain = format!("d{}.cloudfront.net", apigw_make_id());
        let body = serde_json::json!({
            "domainName": domain_name,
            "certificateArn": props.get("CertificateArn").and_then(|v| v.as_str()),
            "regionalCertificateArn": props.get("RegionalCertificateArn").and_then(|v| v.as_str()),
            "endpointConfiguration": props.get("EndpointConfiguration").cloned().unwrap_or(serde_json::json!({"types": ["EDGE"]})),
            "securityPolicy": props.get("SecurityPolicy").and_then(|v| v.as_str()),
            "ownershipVerificationCertificateArn": props.get("OwnershipVerificationCertificateArn").and_then(|v| v.as_str()),
            "regionalDomainName": regional_domain,
            "regionalHostedZoneId": "Z2FDTNDATAQYW2",
            "distributionDomainName": distribution_domain,
            "distributionHostedZoneId": "Z2FDTNDATAQYW2",
            "mutualTlsAuthentication": mtls,
            "tags": serde_json::Value::Object(
                parse_acm_tags(props.get("Tags"))
                    .into_iter()
                    .map(|(k, v)| (k, serde_json::Value::String(v)))
                    .collect(),
            ),
        });
        let mut accounts = self.apigateway_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.domain_names.insert(domain_name.clone(), body);
        Ok(ProvisionResult::new(domain_name.clone())
            .with("DomainName", domain_name)
            .with("RegionalHostedZoneId", "Z2FDTNDATAQYW2".to_string())
            .with("DistributionHostedZoneId", "Z2FDTNDATAQYW2".to_string()))
    }

    fn delete_apigw_domain_name(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.apigateway_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.domain_names.remove(physical_id);
        state.base_path_mappings.remove(physical_id);
        Ok(())
    }

    fn create_apigw_base_path_mapping(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let domain_name = props
            .get("DomainName")
            .and_then(|v| v.as_str())
            .ok_or("DomainName is required")?
            .to_string();
        let rest_api_id = props
            .get("RestApiId")
            .and_then(|v| v.as_str())
            .ok_or("RestApiId is required")?
            .to_string();
        let base_path = props
            .get("BasePath")
            .and_then(|v| v.as_str())
            .unwrap_or("(none)")
            .to_string();
        let stage = props
            .get("Stage")
            .and_then(|v| v.as_str())
            .map(String::from);
        let body = serde_json::json!({
            "basePath": base_path,
            "restApiId": rest_api_id,
            "stage": stage,
        });
        let mut accounts = self.apigateway_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state
            .base_path_mappings
            .entry(domain_name.clone())
            .or_default()
            .insert(base_path.clone(), body);
        let physical = format!("{domain_name}/{base_path}");
        Ok(ProvisionResult::new(physical)
            .with("DomainName", domain_name)
            .with("BasePath", base_path))
    }

    fn delete_apigw_base_path_mapping(
        &self,
        physical_id: &str,
        _attributes: &BTreeMap<String, String>,
    ) -> Result<(), String> {
        let mut parts = physical_id.splitn(2, '/');
        let Some(domain) = parts.next() else {
            return Ok(());
        };
        let Some(base_path) = parts.next() else {
            return Ok(());
        };
        let mut accounts = self.apigateway_state.write();
        let state = accounts.get_or_create(&self.account_id);
        if let Some(map) = state.base_path_mappings.get_mut(domain) {
            map.remove(base_path);
        }
        Ok(())
    }

    // --- API Gateway v1 update paths ---
    //
    // These mirror the create_* helpers above but mutate an existing
    // resource instead of inserting a new one. The physical id is
    // preserved across updates so other stack resources keep referencing
    // the same logical entity.

    fn update_apigw_resource(
        &self,
        existing: &StackResource,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let rest_api_id = existing
            .attributes
            .get("RestApiId")
            .cloned()
            .or_else(|| {
                props
                    .get("RestApiId")
                    .and_then(|v| v.as_str())
                    .map(String::from)
            })
            .ok_or("RestApiId is required")?;
        let physical = existing.physical_id.clone();
        let mut accounts = self.apigateway_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let api_resources = state
            .resources
            .get_mut(&rest_api_id)
            .ok_or_else(|| format!("RestApi {rest_api_id} not found"))?;
        if !api_resources.contains_key(&physical) {
            return Err(format!("Resource {physical} not found"));
        }
        if let Some(part) = props.get("PathPart").and_then(|v| v.as_str()) {
            // Read parent's path first (immutable borrow), then mutate.
            let parent_id = api_resources
                .get(&physical)
                .and_then(|r| r.parent_id.clone());
            let parent_path = parent_id
                .as_ref()
                .and_then(|pid| api_resources.get(pid).map(|p| p.path.clone()))
                .unwrap_or_else(|| "/".to_string());
            let new_path = if parent_path == "/" {
                format!("/{part}")
            } else {
                format!("{parent_path}/{part}")
            };
            let res = api_resources
                .get_mut(&physical)
                .expect("contains_key checked above");
            res.path_part = Some(part.to_string());
            res.path = new_path;
        }
        Ok(ProvisionResult::new(physical.clone())
            .with("ResourceId", physical)
            .with("RestApiId", rest_api_id))
    }

    fn update_apigw_method(
        &self,
        existing: &StackResource,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        // Method's physical id is the composite "rest/resource/method"
        // key. Identity props can't change without replacement, so we
        // simply rewrite the stored Method/Integration with current
        // properties. We delegate to create_apigw_method which already
        // handles the insert-or-replace semantics.
        self.create_apigw_method(resource).map(|r| {
            // Make sure the physical id stays stable.
            ProvisionResult {
                physical_id: existing.physical_id.clone(),
                attributes: r.attributes,
            }
        })
    }

    fn update_apigw_deployment(
        &self,
        existing: &StackResource,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let rest_api_id = existing
            .attributes
            .get("RestApiId")
            .cloned()
            .or_else(|| {
                props
                    .get("RestApiId")
                    .and_then(|v| v.as_str())
                    .map(String::from)
            })
            .ok_or("RestApiId is required")?;
        let physical = existing.physical_id.clone();
        let mut accounts = self.apigateway_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let dep = state
            .deployments
            .get_mut(&rest_api_id)
            .and_then(|m| m.get_mut(&physical))
            .ok_or_else(|| format!("Deployment {physical} not found"))?;
        if let Some(desc) = props.get("Description").and_then(|v| v.as_str()) {
            dep.description = Some(desc.to_string());
        }
        Ok(ProvisionResult::new(physical.clone())
            .with("DeploymentId", physical)
            .with("RestApiId", rest_api_id))
    }

    fn update_apigw_stage(
        &self,
        existing: &StackResource,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let rest_api_id = existing
            .attributes
            .get("RestApiId")
            .cloned()
            .or_else(|| {
                props
                    .get("RestApiId")
                    .and_then(|v| v.as_str())
                    .map(String::from)
            })
            .ok_or("RestApiId is required")?;
        let stage_name = existing.physical_id.clone();
        let mut accounts = self.apigateway_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let stage = state
            .stages
            .get_mut(&rest_api_id)
            .and_then(|m| m.get_mut(&stage_name))
            .ok_or_else(|| format!("Stage {stage_name} not found"))?;
        if let Some(desc) = props.get("Description").and_then(|v| v.as_str()) {
            stage.description = Some(desc.to_string());
        }
        if let Some(b) = props.get("TracingEnabled").and_then(|v| v.as_bool()) {
            stage.tracing_enabled = b;
        }
        if let Some(b) = props.get("CacheClusterEnabled").and_then(|v| v.as_bool()) {
            stage.cache_cluster_enabled = b;
        }
        if let Some(s) = props.get("CacheClusterSize").and_then(|v| v.as_str()) {
            stage.cache_cluster_size = Some(s.to_string());
        }
        if let Some(obj) = props.get("Variables").and_then(|v| v.as_object()) {
            stage.variables = obj
                .iter()
                .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                .collect();
        }
        if let Some(dep) = props.get("DeploymentId").and_then(|v| v.as_str()) {
            stage.deployment_id = dep.to_string();
        }
        if props.get("Tags").is_some() {
            stage.tags = parse_acm_tags(props.get("Tags"));
        }
        if let Some(arr) = props.get("MethodSettings").and_then(|v| v.as_array()) {
            stage.method_settings = arr
                .iter()
                .filter_map(|s| {
                    let path = s.get("ResourcePath").and_then(|v| v.as_str())?;
                    let http = s.get("HttpMethod").and_then(|v| v.as_str())?;
                    let key = format!("{}/{http}", path.strip_prefix('/').unwrap_or(path));
                    Some((key, s.clone()))
                })
                .collect();
        }
        if let Some(canary) = props.get("CanarySetting").cloned() {
            stage.canary_settings = Some(canary);
        }
        if let Some(access) = props.get("AccessLogSetting").cloned() {
            stage.access_log_settings = Some(access);
        }
        stage.last_updated_date = Utc::now();
        Ok(ProvisionResult::new(stage_name.clone())
            .with("StageName", stage_name)
            .with("RestApiId", rest_api_id))
    }

    fn update_apigw_authorizer(
        &self,
        existing: &StackResource,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let rest_api_id = existing
            .attributes
            .get("RestApiId")
            .cloned()
            .or_else(|| {
                props
                    .get("RestApiId")
                    .and_then(|v| v.as_str())
                    .map(String::from)
            })
            .ok_or("RestApiId is required")?;
        let physical = existing.physical_id.clone();
        let mut accounts = self.apigateway_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let auth = state
            .authorizers
            .get_mut(&rest_api_id)
            .and_then(|m| m.get_mut(&physical))
            .ok_or_else(|| format!("Authorizer {physical} not found"))?;
        if let Some(name) = props.get("Name").and_then(|v| v.as_str()) {
            auth.name = name.to_string();
        }
        if let Some(t) = props.get("Type").and_then(|v| v.as_str()) {
            auth.authorizer_type = t.to_string();
        }
        if let Some(uri) = props.get("AuthorizerUri").and_then(|v| v.as_str()) {
            auth.authorizer_uri = Some(uri.to_string());
        }
        if let Some(arr) = props.get("ProviderARNs").and_then(|v| v.as_array()) {
            auth.provider_arns = arr
                .iter()
                .filter_map(|v| v.as_str().map(String::from))
                .collect();
        }
        if let Some(s) = props.get("IdentitySource").and_then(|v| v.as_str()) {
            auth.identity_source = Some(s.to_string());
        }
        if let Some(s) = props
            .get("IdentityValidationExpression")
            .and_then(|v| v.as_str())
        {
            auth.identity_validation_expression = Some(s.to_string());
        }
        if let Some(n) = props
            .get("AuthorizerResultTtlInSeconds")
            .and_then(|v| v.as_i64())
        {
            auth.authorizer_result_ttl_in_seconds = Some(n as i32);
        }
        if let Some(s) = props.get("AuthType").and_then(|v| v.as_str()) {
            auth.auth_type = Some(s.to_string());
        }
        if let Some(s) = props.get("AuthorizerCredentials").and_then(|v| v.as_str()) {
            auth.authorizer_credentials = Some(s.to_string());
        }
        Ok(ProvisionResult::new(physical.clone())
            .with("AuthorizerId", physical)
            .with("RestApiId", rest_api_id))
    }

    fn update_apigw_request_validator(
        &self,
        existing: &StackResource,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let rest_api_id = existing
            .attributes
            .get("RestApiId")
            .cloned()
            .or_else(|| {
                props
                    .get("RestApiId")
                    .and_then(|v| v.as_str())
                    .map(String::from)
            })
            .ok_or("RestApiId is required")?;
        let physical = existing.physical_id.clone();
        let mut accounts = self.apigateway_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let body = state
            .request_validators
            .get_mut(&rest_api_id)
            .and_then(|m| m.get_mut(&physical))
            .ok_or_else(|| format!("RequestValidator {physical} not found"))?;
        let obj = body.as_object_mut().ok_or("validator body not object")?;
        if let Some(name) = props.get("Name").and_then(|v| v.as_str()) {
            obj.insert("name".into(), serde_json::Value::String(name.into()));
        }
        if let Some(b) = props.get("ValidateRequestBody").and_then(|v| v.as_bool()) {
            obj.insert("validateRequestBody".into(), serde_json::Value::Bool(b));
        }
        if let Some(b) = props
            .get("ValidateRequestParameters")
            .and_then(|v| v.as_bool())
        {
            obj.insert(
                "validateRequestParameters".into(),
                serde_json::Value::Bool(b),
            );
        }
        Ok(ProvisionResult::new(physical.clone())
            .with("RequestValidatorId", physical)
            .with("RestApiId", rest_api_id))
    }

    fn update_apigw_model(
        &self,
        existing: &StackResource,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let rest_api_id = existing
            .attributes
            .get("RestApiId")
            .cloned()
            .or_else(|| {
                props
                    .get("RestApiId")
                    .and_then(|v| v.as_str())
                    .map(String::from)
            })
            .ok_or("RestApiId is required")?;
        let model_name = existing.physical_id.clone();
        let mut accounts = self.apigateway_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let model = state
            .models
            .get_mut(&rest_api_id)
            .and_then(|m| m.get_mut(&model_name))
            .ok_or_else(|| format!("Model {model_name} not found"))?;
        if let Some(desc) = props.get("Description").and_then(|v| v.as_str()) {
            model.description = Some(desc.to_string());
        }
        if let Some(s) = props.get("ContentType").and_then(|v| v.as_str()) {
            model.content_type = s.to_string();
        }
        if let Some(schema) = props.get("Schema") {
            model.schema = Some(if let Some(s) = schema.as_str() {
                s.to_string()
            } else {
                schema.to_string()
            });
        }
        Ok(ProvisionResult::new(model_name.clone())
            .with("ModelName", model_name)
            .with("RestApiId", rest_api_id))
    }

    fn update_apigw_gateway_response(
        &self,
        existing: &StackResource,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let rest_api_id = existing
            .attributes
            .get("RestApiId")
            .cloned()
            .or_else(|| {
                props
                    .get("RestApiId")
                    .and_then(|v| v.as_str())
                    .map(String::from)
            })
            .ok_or("RestApiId is required")?;
        let response_type = existing.physical_id.clone();
        let mut accounts = self.apigateway_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let body = state
            .gateway_responses
            .get_mut(&rest_api_id)
            .and_then(|m| m.get_mut(&response_type))
            .ok_or_else(|| format!("GatewayResponse {response_type} not found"))?;
        let obj = body.as_object_mut().ok_or("response body not object")?;
        if let Some(s) = props.get("StatusCode").and_then(|v| v.as_str()) {
            obj.insert("statusCode".into(), serde_json::Value::String(s.into()));
        }
        if let Some(v) = props.get("ResponseParameters").cloned() {
            obj.insert("responseParameters".into(), v);
        }
        if let Some(v) = props.get("ResponseTemplates").cloned() {
            obj.insert("responseTemplates".into(), v);
        }
        Ok(ProvisionResult::new(response_type.clone())
            .with("ResponseType", response_type)
            .with("RestApiId", rest_api_id))
    }

    fn update_apigw_usage_plan(
        &self,
        existing: &StackResource,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let physical = existing.physical_id.clone();
        let mut accounts = self.apigateway_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let plan = state
            .usage_plans
            .get_mut(&physical)
            .ok_or_else(|| format!("UsagePlan {physical} not found"))?;
        if let Some(name) = props.get("UsagePlanName").and_then(|v| v.as_str()) {
            plan.name = name.to_string();
        }
        if let Some(s) = props.get("Description").and_then(|v| v.as_str()) {
            plan.description = Some(s.to_string());
        }
        if let Some(arr) = props.get("ApiStages").and_then(|v| v.as_array()) {
            plan.api_stages = arr.iter().cloned().map(lowercase_first_keys).collect();
        }
        if let Some(t) = props.get("Throttle").cloned() {
            plan.throttle = Some(lowercase_first_keys(t));
        }
        if let Some(q) = props.get("Quota").cloned() {
            plan.quota = Some(lowercase_first_keys(q));
        }
        if props.get("Tags").is_some() {
            plan.tags = parse_acm_tags(props.get("Tags"));
        }
        Ok(ProvisionResult::new(physical.clone()).with("UsagePlanId", physical))
    }

    fn update_apigw_api_key(
        &self,
        existing: &StackResource,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let physical = existing.physical_id.clone();
        let mut accounts = self.apigateway_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let key = state
            .api_keys
            .get_mut(&physical)
            .ok_or_else(|| format!("ApiKey {physical} not found"))?;
        if let Some(name) = props.get("Name").and_then(|v| v.as_str()) {
            key.name = name.to_string();
        }
        if let Some(s) = props.get("Description").and_then(|v| v.as_str()) {
            key.description = Some(s.to_string());
        }
        if let Some(b) = props.get("Enabled").and_then(|v| v.as_bool()) {
            key.enabled = b;
        }
        if let Some(s) = props.get("CustomerId").and_then(|v| v.as_str()) {
            key.customer_id = Some(s.to_string());
        }
        if props.get("Tags").is_some() {
            key.tags = parse_acm_tags(props.get("Tags"));
        }
        if let Some(arr) = props.get("StageKeys").and_then(|v| v.as_array()) {
            key.stage_keys = arr
                .iter()
                .filter_map(|s| {
                    let rest = s.get("RestApiId").and_then(|v| v.as_str())?;
                    let stage = s.get("StageName").and_then(|v| v.as_str())?;
                    Some(format!("{rest}/{stage}"))
                })
                .collect();
        }
        key.last_updated_date = Utc::now();
        Ok(ProvisionResult::new(physical.clone()).with("ApiKeyId", physical))
    }

    fn update_apigw_usage_plan_key(
        &self,
        existing: &StackResource,
        _resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        // UsagePlanKey is a pure association (UsagePlan + ApiKey + Type) —
        // CFN treats every property as `requires-replacement`, so a real
        // UpdateStack would Delete+Create. Here we just preserve the
        // existing physical id so resolution stays stable.
        let physical = existing.physical_id.clone();
        let mut parts = physical.splitn(2, '/');
        let plan = parts.next().unwrap_or("").to_string();
        let key = parts.next().unwrap_or("").to_string();
        Ok(ProvisionResult::new(physical)
            .with("UsagePlanId", plan)
            .with("KeyId", key))
    }

    fn update_apigw_domain_name(
        &self,
        existing: &StackResource,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let domain = existing.physical_id.clone();
        let mut accounts = self.apigateway_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let body = state
            .domain_names
            .get_mut(&domain)
            .ok_or_else(|| format!("DomainName {domain} not found"))?;
        let obj = body.as_object_mut().ok_or("domain body not object")?;
        if let Some(s) = props.get("CertificateArn").and_then(|v| v.as_str()) {
            obj.insert("certificateArn".into(), serde_json::Value::String(s.into()));
        }
        if let Some(s) = props.get("RegionalCertificateArn").and_then(|v| v.as_str()) {
            obj.insert(
                "regionalCertificateArn".into(),
                serde_json::Value::String(s.into()),
            );
        }
        if let Some(v) = props.get("EndpointConfiguration").cloned() {
            obj.insert("endpointConfiguration".into(), v);
        }
        if let Some(s) = props.get("SecurityPolicy").and_then(|v| v.as_str()) {
            obj.insert("securityPolicy".into(), serde_json::Value::String(s.into()));
        }
        if let Some(v) = props.get("MutualTlsAuthentication").cloned() {
            obj.insert("mutualTlsAuthentication".into(), lowercase_first_keys(v));
        }
        if let Some(s) = props
            .get("OwnershipVerificationCertificateArn")
            .and_then(|v| v.as_str())
        {
            obj.insert(
                "ownershipVerificationCertificateArn".into(),
                serde_json::Value::String(s.into()),
            );
        }
        if props.get("Tags").is_some() {
            obj.insert(
                "tags".into(),
                serde_json::Value::Object(
                    parse_acm_tags(props.get("Tags"))
                        .into_iter()
                        .map(|(k, v)| (k, serde_json::Value::String(v)))
                        .collect(),
                ),
            );
        }
        Ok(ProvisionResult::new(domain.clone())
            .with("DomainName", domain)
            .with("RegionalHostedZoneId", "Z2FDTNDATAQYW2".to_string())
            .with("DistributionHostedZoneId", "Z2FDTNDATAQYW2".to_string()))
    }

    fn update_apigw_base_path_mapping(
        &self,
        existing: &StackResource,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let physical = existing.physical_id.clone();
        let mut parts = physical.splitn(2, '/');
        let domain = parts
            .next()
            .ok_or("malformed base path mapping id")?
            .to_string();
        let base_path = parts
            .next()
            .ok_or("malformed base path mapping id")?
            .to_string();
        let mut accounts = self.apigateway_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let map = state
            .base_path_mappings
            .get_mut(&domain)
            .ok_or_else(|| format!("DomainName {domain} not found"))?;
        let body = map
            .get_mut(&base_path)
            .ok_or_else(|| format!("BasePath {base_path} not found"))?;
        let obj = body.as_object_mut().ok_or("mapping body not object")?;
        if let Some(s) = props.get("RestApiId").and_then(|v| v.as_str()) {
            obj.insert("restApiId".into(), serde_json::Value::String(s.into()));
        }
        if let Some(s) = props.get("Stage").and_then(|v| v.as_str()) {
            obj.insert("stage".into(), serde_json::Value::String(s.into()));
        }
        Ok(ProvisionResult::new(physical)
            .with("DomainName", domain)
            .with("BasePath", base_path))
    }

    // --- API Gateway v2 (HTTP/WebSocket APIs) ---

    fn create_apigwv2_api(&self, resource: &ResourceDefinition) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let name = props
            .get("Name")
            .and_then(|v| v.as_str())
            .ok_or("Name is required")?
            .to_string();
        let protocol_type = props
            .get("ProtocolType")
            .and_then(|v| v.as_str())
            .unwrap_or("HTTP")
            .to_string();
        let description = props
            .get("Description")
            .and_then(|v| v.as_str())
            .map(String::from);
        let tags: Option<BTreeMap<String, String>> =
            props.get("Tags").and_then(|v| v.as_object()).map(|obj| {
                obj.iter()
                    .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                    .collect()
            });

        let id = make_apigwv2_id(10);
        let mut api = ApiGwV2HttpApi::new(id.clone(), name, description, tags, &self.region);
        api.protocol_type = protocol_type.clone();
        if let Some(expr) = props
            .get("RouteSelectionExpression")
            .and_then(|v| v.as_str())
        {
            api.route_selection_expression = expr.to_string();
        }
        if let Some(expr) = props
            .get("ApiKeySelectionExpression")
            .and_then(|v| v.as_str())
        {
            api.api_key_selection_expression = expr.to_string();
        }
        if let Some(b) = props
            .get("DisableExecuteApiEndpoint")
            .and_then(|v| v.as_bool())
        {
            api.disable_execute_api_endpoint = b;
        }
        if let Some(s) = props.get("IpAddressType").and_then(|v| v.as_str()) {
            api.ip_address_type = s.to_string();
        }
        if let Some(cors) = props.get("CorsConfiguration").and_then(|v| v.as_object()) {
            api.cors_configuration = Some(ApiGwV2CorsConfiguration {
                allow_credentials: cors.get("AllowCredentials").and_then(|v| v.as_bool()),
                allow_headers: cors
                    .get("AllowHeaders")
                    .and_then(|v| v.as_array())
                    .map(|a| {
                        a.iter()
                            .filter_map(|v| v.as_str().map(String::from))
                            .collect()
                    }),
                allow_methods: cors
                    .get("AllowMethods")
                    .and_then(|v| v.as_array())
                    .map(|a| {
                        a.iter()
                            .filter_map(|v| v.as_str().map(String::from))
                            .collect()
                    }),
                allow_origins: cors
                    .get("AllowOrigins")
                    .and_then(|v| v.as_array())
                    .map(|a| {
                        a.iter()
                            .filter_map(|v| v.as_str().map(String::from))
                            .collect()
                    }),
                expose_headers: cors
                    .get("ExposeHeaders")
                    .and_then(|v| v.as_array())
                    .map(|a| {
                        a.iter()
                            .filter_map(|v| v.as_str().map(String::from))
                            .collect()
                    }),
                max_age: cors
                    .get("MaxAge")
                    .and_then(|v| v.as_i64())
                    .map(|n| n as i32),
            });
        }

        let api_endpoint = api.api_endpoint.clone();
        let mut accounts = self.apigatewayv2_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.apis.insert(id.clone(), api);

        Ok(ProvisionResult::new(id.clone())
            .with("ApiId", id)
            .with("ApiEndpoint", api_endpoint))
    }

    fn delete_apigwv2_api(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.apigatewayv2_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.apis.remove(physical_id);
        state.routes.remove(physical_id);
        state.integrations.remove(physical_id);
        state.stages.remove(physical_id);
        state.deployments.remove(physical_id);
        state.authorizers.remove(physical_id);
        state.models.remove(physical_id);
        state.integration_responses.remove(physical_id);
        state.route_responses.remove(physical_id);
        Ok(())
    }

    fn create_apigwv2_route(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let api_id = props
            .get("ApiId")
            .and_then(|v| v.as_str())
            .ok_or("ApiId is required")?
            .to_string();
        let route_key = props
            .get("RouteKey")
            .and_then(|v| v.as_str())
            .ok_or("RouteKey is required")?
            .to_string();

        let mut accounts = self.apigatewayv2_state.write();
        let state = accounts.get_or_create(&self.account_id);
        if !state.apis.contains_key(&api_id) {
            return Err(format!("Api {api_id} not yet provisioned"));
        }
        let id = make_apigwv2_id(10);
        let route = ApiGwV2Route {
            route_id: id.clone(),
            route_key,
            target: props
                .get("Target")
                .and_then(|v| v.as_str())
                .map(String::from),
            authorization_type: props
                .get("AuthorizationType")
                .and_then(|v| v.as_str())
                .map(String::from),
            authorizer_id: props
                .get("AuthorizerId")
                .and_then(|v| v.as_str())
                .map(String::from),
        };
        state
            .routes
            .entry(api_id.clone())
            .or_default()
            .insert(id.clone(), route);

        Ok(ProvisionResult::new(id.clone())
            .with("RouteId", id)
            .with("ApiId", api_id))
    }

    fn delete_apigwv2_route(
        &self,
        physical_id: &str,
        attributes: &BTreeMap<String, String>,
    ) -> Result<(), String> {
        let Some(api_id) = attributes.get("ApiId") else {
            return Ok(());
        };
        let mut accounts = self.apigatewayv2_state.write();
        let state = accounts.get_or_create(&self.account_id);
        if let Some(map) = state.routes.get_mut(api_id) {
            map.remove(physical_id);
        }
        Ok(())
    }

    fn create_apigwv2_integration(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let api_id = props
            .get("ApiId")
            .and_then(|v| v.as_str())
            .ok_or("ApiId is required")?
            .to_string();
        let integration_type = props
            .get("IntegrationType")
            .and_then(|v| v.as_str())
            .ok_or("IntegrationType is required")?
            .to_string();

        let mut accounts = self.apigatewayv2_state.write();
        let state = accounts.get_or_create(&self.account_id);
        if !state.apis.contains_key(&api_id) {
            return Err(format!("Api {api_id} not yet provisioned"));
        }
        let id = make_apigwv2_id(10);
        let integration = ApiGwV2Integration {
            integration_id: id.clone(),
            integration_type,
            integration_uri: props
                .get("IntegrationUri")
                .and_then(|v| v.as_str())
                .map(String::from),
            payload_format_version: props
                .get("PayloadFormatVersion")
                .and_then(|v| v.as_str())
                .map(String::from),
            timeout_in_millis: props.get("TimeoutInMillis").and_then(|v| v.as_i64()),
        };
        state
            .integrations
            .entry(api_id.clone())
            .or_default()
            .insert(id.clone(), integration);

        Ok(ProvisionResult::new(id.clone())
            .with("IntegrationId", id)
            .with("ApiId", api_id))
    }

    fn delete_apigwv2_integration(
        &self,
        physical_id: &str,
        attributes: &BTreeMap<String, String>,
    ) -> Result<(), String> {
        let Some(api_id) = attributes.get("ApiId") else {
            return Ok(());
        };
        let mut accounts = self.apigatewayv2_state.write();
        let state = accounts.get_or_create(&self.account_id);
        if let Some(map) = state.integrations.get_mut(api_id) {
            map.remove(physical_id);
        }
        Ok(())
    }

    fn create_apigwv2_integration_response(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let api_id = props
            .get("ApiId")
            .and_then(|v| v.as_str())
            .ok_or("ApiId is required")?
            .to_string();
        let integration_id = props
            .get("IntegrationId")
            .and_then(|v| v.as_str())
            .ok_or("IntegrationId is required")?
            .to_string();
        let key_expr = props
            .get("IntegrationResponseKey")
            .and_then(|v| v.as_str())
            .ok_or("IntegrationResponseKey is required")?
            .to_string();
        let id = make_apigwv2_id(10);
        let body = serde_json::json!({
            "integrationResponseId": id,
            "integrationId": integration_id,
            "integrationResponseKey": key_expr,
            "responseTemplates": props.get("ResponseTemplates").cloned().unwrap_or(serde_json::json!({})),
            "responseParameters": props.get("ResponseParameters").cloned().unwrap_or(serde_json::json!({})),
            "templateSelectionExpression": props.get("TemplateSelectionExpression").and_then(|v| v.as_str()),
            "contentHandlingStrategy": props.get("ContentHandlingStrategy").and_then(|v| v.as_str()),
        });
        let composite_key = format!("{integration_id}/{id}");
        let mut accounts = self.apigatewayv2_state.write();
        let state = accounts.get_or_create(&self.account_id);
        if !state
            .integrations
            .get(&api_id)
            .map(|m| m.contains_key(&integration_id))
            .unwrap_or(false)
        {
            return Err(format!(
                "Integration {integration_id} not yet provisioned for api {api_id}"
            ));
        }
        state
            .integration_responses
            .entry(api_id.clone())
            .or_default()
            .insert(composite_key.clone(), body);
        Ok(ProvisionResult::new(composite_key.clone())
            .with("IntegrationResponseId", id)
            .with("IntegrationId", integration_id)
            .with("ApiId", api_id))
    }

    fn delete_apigwv2_integration_response(
        &self,
        physical_id: &str,
        attributes: &BTreeMap<String, String>,
    ) -> Result<(), String> {
        let Some(api_id) = attributes.get("ApiId") else {
            return Ok(());
        };
        let mut accounts = self.apigatewayv2_state.write();
        let state = accounts.get_or_create(&self.account_id);
        if let Some(map) = state.integration_responses.get_mut(api_id) {
            map.remove(physical_id);
        }
        Ok(())
    }

    fn create_apigwv2_route_response(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let api_id = props
            .get("ApiId")
            .and_then(|v| v.as_str())
            .ok_or("ApiId is required")?
            .to_string();
        let route_id = props
            .get("RouteId")
            .and_then(|v| v.as_str())
            .ok_or("RouteId is required")?
            .to_string();
        let key_expr = props
            .get("RouteResponseKey")
            .and_then(|v| v.as_str())
            .ok_or("RouteResponseKey is required")?
            .to_string();
        let id = make_apigwv2_id(10);
        let body = serde_json::json!({
            "routeResponseId": id,
            "routeId": route_id,
            "routeResponseKey": key_expr,
            "responseModels": props.get("ResponseModels").cloned().unwrap_or(serde_json::json!({})),
            "modelSelectionExpression": props.get("ModelSelectionExpression").and_then(|v| v.as_str()),
            "responseParameters": props.get("ResponseParameters").cloned().unwrap_or(serde_json::json!({})),
        });
        let composite = format!("{route_id}/{id}");
        let mut accounts = self.apigatewayv2_state.write();
        let state = accounts.get_or_create(&self.account_id);
        if !state
            .routes
            .get(&api_id)
            .map(|m| m.contains_key(&route_id))
            .unwrap_or(false)
        {
            return Err(format!(
                "Route {route_id} not yet provisioned for api {api_id}"
            ));
        }
        state
            .route_responses
            .entry(api_id.clone())
            .or_default()
            .insert(composite.clone(), body);
        Ok(ProvisionResult::new(composite.clone())
            .with("RouteResponseId", id)
            .with("RouteId", route_id)
            .with("ApiId", api_id))
    }

    fn delete_apigwv2_route_response(
        &self,
        physical_id: &str,
        attributes: &BTreeMap<String, String>,
    ) -> Result<(), String> {
        let Some(api_id) = attributes.get("ApiId") else {
            return Ok(());
        };
        let mut accounts = self.apigatewayv2_state.write();
        let state = accounts.get_or_create(&self.account_id);
        if let Some(map) = state.route_responses.get_mut(api_id) {
            map.remove(physical_id);
        }
        Ok(())
    }

    fn create_apigwv2_stage(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let api_id = props
            .get("ApiId")
            .and_then(|v| v.as_str())
            .ok_or("ApiId is required")?
            .to_string();
        let stage_name = props
            .get("StageName")
            .and_then(|v| v.as_str())
            .ok_or("StageName is required")?
            .to_string();
        let auto_deploy = props
            .get("AutoDeploy")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let deployment_id = props
            .get("DeploymentId")
            .and_then(|v| v.as_str())
            .map(String::from);

        let stage_variables = props
            .get("StageVariables")
            .and_then(|v| v.as_object())
            .map(|obj| {
                obj.iter()
                    .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                    .collect()
            });

        let access_log_settings = props.get("AccessLogSettings").and_then(|v| {
            let destination_arn = v.get("DestinationArn")?.as_str()?.to_string();
            let format = v.get("Format").and_then(|f| f.as_str().map(String::from));
            Some(fakecloud_apigatewayv2::AccessLogSettings {
                destination_arn,
                format,
            })
        });

        let stage = ApiGwV2Stage {
            stage_name: stage_name.clone(),
            description: props
                .get("Description")
                .and_then(|v| v.as_str())
                .map(String::from),
            deployment_id: deployment_id.clone(),
            auto_deploy,
            created_date: Utc::now(),
            last_updated_date: None,
            web_acl_arn: None,
            stage_variables,
            access_log_settings,
        };

        let mut accounts = self.apigatewayv2_state.write();
        let state = accounts.get_or_create(&self.account_id);
        if !state.apis.contains_key(&api_id) {
            return Err(format!("Api {api_id} not yet provisioned"));
        }
        if let Some(dep) = &deployment_id {
            if !state
                .deployments
                .get(&api_id)
                .map(|m| m.contains_key(dep))
                .unwrap_or(false)
            {
                return Err(format!(
                    "Deployment {dep} not yet provisioned for api {api_id}"
                ));
            }
        }
        state
            .stages
            .entry(api_id.clone())
            .or_default()
            .insert(stage_name.clone(), stage);

        Ok(ProvisionResult::new(stage_name.clone())
            .with("StageName", stage_name)
            .with("ApiId", api_id))
    }

    fn delete_apigwv2_stage(
        &self,
        physical_id: &str,
        attributes: &BTreeMap<String, String>,
    ) -> Result<(), String> {
        let Some(api_id) = attributes.get("ApiId") else {
            return Ok(());
        };
        let mut accounts = self.apigatewayv2_state.write();
        let state = accounts.get_or_create(&self.account_id);
        if let Some(map) = state.stages.get_mut(api_id) {
            map.remove(physical_id);
        }
        Ok(())
    }

    fn create_apigwv2_deployment(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let api_id = props
            .get("ApiId")
            .and_then(|v| v.as_str())
            .ok_or("ApiId is required")?
            .to_string();
        let id = make_apigwv2_id(10);
        let deployment = ApiGwV2Deployment {
            deployment_id: id.clone(),
            description: props
                .get("Description")
                .and_then(|v| v.as_str())
                .map(String::from),
            created_date: Utc::now(),
            auto_deployed: false,
        };
        let mut accounts = self.apigatewayv2_state.write();
        let state = accounts.get_or_create(&self.account_id);
        if !state.apis.contains_key(&api_id) {
            return Err(format!("Api {api_id} not yet provisioned"));
        }
        state
            .deployments
            .entry(api_id.clone())
            .or_default()
            .insert(id.clone(), deployment);
        Ok(ProvisionResult::new(id.clone())
            .with("DeploymentId", id)
            .with("ApiId", api_id))
    }

    fn delete_apigwv2_deployment(
        &self,
        physical_id: &str,
        attributes: &BTreeMap<String, String>,
    ) -> Result<(), String> {
        let Some(api_id) = attributes.get("ApiId") else {
            return Ok(());
        };
        let mut accounts = self.apigatewayv2_state.write();
        let state = accounts.get_or_create(&self.account_id);
        if let Some(map) = state.deployments.get_mut(api_id) {
            map.remove(physical_id);
        }
        Ok(())
    }

    fn create_apigwv2_authorizer(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let api_id = props
            .get("ApiId")
            .and_then(|v| v.as_str())
            .ok_or("ApiId is required")?
            .to_string();
        let name = props
            .get("Name")
            .and_then(|v| v.as_str())
            .ok_or("Name is required")?
            .to_string();
        let authorizer_type = props
            .get("AuthorizerType")
            .and_then(|v| v.as_str())
            .unwrap_or("REQUEST")
            .to_string();
        let identity_source = props
            .get("IdentitySource")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect::<Vec<String>>()
            });
        let jwt_configuration = props
            .get("JwtConfiguration")
            .and_then(|v| v.as_object())
            .map(|obj| ApiGwV2JwtConfiguration {
                audience: obj.get("Audience").and_then(|v| v.as_array()).map(|a| {
                    a.iter()
                        .filter_map(|v| v.as_str().map(String::from))
                        .collect()
                }),
                issuer: obj.get("Issuer").and_then(|v| v.as_str()).map(String::from),
            });

        let id = make_apigwv2_id(10);
        let auth = ApiGwV2Authorizer {
            authorizer_id: id.clone(),
            name,
            authorizer_type,
            authorizer_uri: props
                .get("AuthorizerUri")
                .and_then(|v| v.as_str())
                .map(String::from),
            identity_source,
            jwt_configuration,
        };
        let mut accounts = self.apigatewayv2_state.write();
        let state = accounts.get_or_create(&self.account_id);
        if !state.apis.contains_key(&api_id) {
            return Err(format!("Api {api_id} not yet provisioned"));
        }
        state
            .authorizers
            .entry(api_id.clone())
            .or_default()
            .insert(id.clone(), auth);
        Ok(ProvisionResult::new(id.clone())
            .with("AuthorizerId", id)
            .with("ApiId", api_id))
    }

    fn delete_apigwv2_authorizer(
        &self,
        physical_id: &str,
        attributes: &BTreeMap<String, String>,
    ) -> Result<(), String> {
        let Some(api_id) = attributes.get("ApiId") else {
            return Ok(());
        };
        let mut accounts = self.apigatewayv2_state.write();
        let state = accounts.get_or_create(&self.account_id);
        if let Some(map) = state.authorizers.get_mut(api_id) {
            map.remove(physical_id);
        }
        Ok(())
    }

    fn create_apigwv2_domain_name(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let domain_name = props
            .get("DomainName")
            .and_then(|v| v.as_str())
            .ok_or("DomainName is required")?
            .to_string();
        let body = serde_json::json!({
            "domainName": domain_name,
            "domainNameConfigurations": props.get("DomainNameConfigurations").cloned().unwrap_or(serde_json::json!([])),
            "mutualTlsAuthentication": props.get("MutualTlsAuthentication").cloned(),
            "apiMappingSelectionExpression": null,
        });
        let mut accounts = self.apigatewayv2_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.domain_names.insert(domain_name.clone(), body);
        Ok(ProvisionResult::new(domain_name.clone()).with("DomainName", domain_name))
    }

    fn delete_apigwv2_domain_name(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.apigatewayv2_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.domain_names.remove(physical_id);
        state.api_mappings.remove(physical_id);
        Ok(())
    }

    fn create_apigwv2_api_mapping(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let domain_name = props
            .get("DomainName")
            .and_then(|v| v.as_str())
            .ok_or("DomainName is required")?
            .to_string();
        let api_id = props
            .get("ApiId")
            .and_then(|v| v.as_str())
            .ok_or("ApiId is required")?
            .to_string();
        let stage = props
            .get("Stage")
            .and_then(|v| v.as_str())
            .ok_or("Stage is required")?
            .to_string();
        let api_mapping_key = props
            .get("ApiMappingKey")
            .and_then(|v| v.as_str())
            .map(String::from);
        let id = make_apigwv2_id(10);
        let body = serde_json::json!({
            "apiMappingId": id,
            "apiId": api_id,
            "stage": stage,
            "apiMappingKey": api_mapping_key,
        });
        let mut accounts = self.apigatewayv2_state.write();
        let state = accounts.get_or_create(&self.account_id);
        if !state.domain_names.contains_key(&domain_name) {
            return Err(format!("DomainName {domain_name} not yet provisioned"));
        }
        if !state.apis.contains_key(&api_id) {
            return Err(format!("Api {api_id} not yet provisioned"));
        }
        state
            .api_mappings
            .entry(domain_name.clone())
            .or_default()
            .insert(id.clone(), body);
        Ok(ProvisionResult::new(id.clone())
            .with("ApiMappingId", id)
            .with("DomainName", domain_name))
    }

    fn delete_apigwv2_api_mapping(
        &self,
        physical_id: &str,
        attributes: &BTreeMap<String, String>,
    ) -> Result<(), String> {
        let Some(domain) = attributes.get("DomainName") else {
            return Ok(());
        };
        let mut accounts = self.apigatewayv2_state.write();
        let state = accounts.get_or_create(&self.account_id);
        if let Some(map) = state.api_mappings.get_mut(domain) {
            map.remove(physical_id);
        }
        Ok(())
    }

    fn create_apigwv2_vpc_link(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let name = props
            .get("Name")
            .and_then(|v| v.as_str())
            .ok_or("Name is required")?
            .to_string();
        let id = make_apigwv2_id(10);
        let body = serde_json::json!({
            "vpcLinkId": id,
            "name": name,
            "subnetIds": props.get("SubnetIds").cloned().unwrap_or(serde_json::json!([])),
            "securityGroupIds": props.get("SecurityGroupIds").cloned().unwrap_or(serde_json::json!([])),
            "tags": props.get("Tags").cloned().unwrap_or(serde_json::json!({})),
            "vpcLinkStatus": "AVAILABLE",
            "vpcLinkVersion": "V2",
            "createdDate": Utc::now().to_rfc3339(),
        });
        let mut accounts = self.apigatewayv2_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.vpc_links.insert(id.clone(), body);
        Ok(ProvisionResult::new(id.clone()).with("VpcLinkId", id))
    }

    fn delete_apigwv2_vpc_link(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.apigatewayv2_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.vpc_links.remove(physical_id);
        Ok(())
    }

    fn create_apigwv2_model(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let api_id = props
            .get("ApiId")
            .and_then(|v| v.as_str())
            .ok_or("ApiId is required")?
            .to_string();
        let name = props
            .get("Name")
            .and_then(|v| v.as_str())
            .ok_or("Name is required")?
            .to_string();
        let id = make_apigwv2_id(10);
        let body = serde_json::json!({
            "modelId": id,
            "name": name,
            "contentType": props.get("ContentType").and_then(|v| v.as_str()).unwrap_or("application/json"),
            "description": props.get("Description").and_then(|v| v.as_str()),
            "schema": props.get("Schema").map(|v| if let Some(s) = v.as_str() { s.to_string() } else { v.to_string() }),
        });
        let mut accounts = self.apigatewayv2_state.write();
        let state = accounts.get_or_create(&self.account_id);
        if !state.apis.contains_key(&api_id) {
            return Err(format!("Api {api_id} not yet provisioned"));
        }
        state
            .models
            .entry(api_id.clone())
            .or_default()
            .insert(id.clone(), body);
        Ok(ProvisionResult::new(id.clone())
            .with("ModelId", id)
            .with("ApiId", api_id))
    }

    fn delete_apigwv2_model(
        &self,
        physical_id: &str,
        attributes: &BTreeMap<String, String>,
    ) -> Result<(), String> {
        let Some(api_id) = attributes.get("ApiId") else {
            return Ok(());
        };
        let mut accounts = self.apigatewayv2_state.write();
        let state = accounts.get_or_create(&self.account_id);
        if let Some(map) = state.models.get_mut(api_id) {
            map.remove(physical_id);
        }
        Ok(())
    }

    /// In-place update for AWS::ApiGatewayV2::Api. CFN keeps the same
    /// ApiId across updates; only mutable properties (Name, Description,
    /// CORS, RouteSelectionExpression, etc.) are rewritten.
    fn update_apigwv2_api(
        &self,
        existing: &StackResource,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let api_id = existing.physical_id.clone();
        let mut accounts = self.apigatewayv2_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let api = state
            .apis
            .get_mut(&api_id)
            .ok_or_else(|| format!("Api {api_id} no longer exists in state"))?;

        if let Some(s) = props.get("Name").and_then(|v| v.as_str()) {
            api.name = s.to_string();
        }
        if let Some(s) = props.get("ProtocolType").and_then(|v| v.as_str()) {
            api.protocol_type = s.to_string();
        }
        api.description = props
            .get("Description")
            .and_then(|v| v.as_str())
            .map(String::from)
            .or_else(|| api.description.clone());
        if let Some(s) = props
            .get("RouteSelectionExpression")
            .and_then(|v| v.as_str())
        {
            api.route_selection_expression = s.to_string();
        }
        if let Some(s) = props
            .get("ApiKeySelectionExpression")
            .and_then(|v| v.as_str())
        {
            api.api_key_selection_expression = s.to_string();
        }
        if let Some(b) = props
            .get("DisableExecuteApiEndpoint")
            .and_then(|v| v.as_bool())
        {
            api.disable_execute_api_endpoint = b;
        }
        if let Some(s) = props.get("IpAddressType").and_then(|v| v.as_str()) {
            api.ip_address_type = s.to_string();
        }
        if let Some(cors) = props.get("CorsConfiguration").and_then(|v| v.as_object()) {
            api.cors_configuration = Some(ApiGwV2CorsConfiguration {
                allow_credentials: cors.get("AllowCredentials").and_then(|v| v.as_bool()),
                allow_headers: cors
                    .get("AllowHeaders")
                    .and_then(|v| v.as_array())
                    .map(|a| {
                        a.iter()
                            .filter_map(|v| v.as_str().map(String::from))
                            .collect()
                    }),
                allow_methods: cors
                    .get("AllowMethods")
                    .and_then(|v| v.as_array())
                    .map(|a| {
                        a.iter()
                            .filter_map(|v| v.as_str().map(String::from))
                            .collect()
                    }),
                allow_origins: cors
                    .get("AllowOrigins")
                    .and_then(|v| v.as_array())
                    .map(|a| {
                        a.iter()
                            .filter_map(|v| v.as_str().map(String::from))
                            .collect()
                    }),
                expose_headers: cors
                    .get("ExposeHeaders")
                    .and_then(|v| v.as_array())
                    .map(|a| {
                        a.iter()
                            .filter_map(|v| v.as_str().map(String::from))
                            .collect()
                    }),
                max_age: cors
                    .get("MaxAge")
                    .and_then(|v| v.as_i64())
                    .map(|n| n as i32),
            });
        }
        if let Some(obj) = props.get("Tags").and_then(|v| v.as_object()) {
            let tags: BTreeMap<String, String> = obj
                .iter()
                .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                .collect();
            api.tags = Some(tags);
        }

        let api_endpoint = api.api_endpoint.clone();
        Ok(ProvisionResult::new(api_id.clone())
            .with("ApiId", api_id)
            .with("ApiEndpoint", api_endpoint))
    }

    /// In-place update for AWS::ApiGatewayV2::Route. The RouteId is
    /// stable across updates; only Target / AuthorizationType /
    /// AuthorizerId / RouteKey are rewritten.
    fn update_apigwv2_route(
        &self,
        existing: &StackResource,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let api_id = props
            .get("ApiId")
            .and_then(|v| v.as_str())
            .ok_or("ApiId is required")?
            .to_string();
        let route_id = existing.physical_id.clone();

        let mut accounts = self.apigatewayv2_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let routes = state
            .routes
            .get_mut(&api_id)
            .ok_or_else(|| format!("Api {api_id} not yet provisioned"))?;
        let route = routes
            .get_mut(&route_id)
            .ok_or_else(|| format!("Route {route_id} not yet provisioned for api {api_id}"))?;
        if let Some(s) = props.get("RouteKey").and_then(|v| v.as_str()) {
            route.route_key = s.to_string();
        }
        if let Some(s) = props.get("Target").and_then(|v| v.as_str()) {
            route.target = Some(s.to_string());
        }
        if let Some(s) = props.get("AuthorizationType").and_then(|v| v.as_str()) {
            route.authorization_type = Some(s.to_string());
        }
        if let Some(s) = props.get("AuthorizerId").and_then(|v| v.as_str()) {
            route.authorizer_id = Some(s.to_string());
        }
        Ok(ProvisionResult::new(route_id.clone())
            .with("RouteId", route_id)
            .with("ApiId", api_id))
    }

    /// In-place update for AWS::ApiGatewayV2::Integration.
    fn update_apigwv2_integration(
        &self,
        existing: &StackResource,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let api_id = props
            .get("ApiId")
            .and_then(|v| v.as_str())
            .ok_or("ApiId is required")?
            .to_string();
        let integration_id = existing.physical_id.clone();

        let mut accounts = self.apigatewayv2_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let integrations = state
            .integrations
            .get_mut(&api_id)
            .ok_or_else(|| format!("Api {api_id} not yet provisioned"))?;
        let integ = integrations.get_mut(&integration_id).ok_or_else(|| {
            format!("Integration {integration_id} not yet provisioned for api {api_id}")
        })?;
        if let Some(s) = props.get("IntegrationType").and_then(|v| v.as_str()) {
            integ.integration_type = s.to_string();
        }
        if let Some(s) = props.get("IntegrationUri").and_then(|v| v.as_str()) {
            integ.integration_uri = Some(s.to_string());
        }
        if let Some(s) = props.get("PayloadFormatVersion").and_then(|v| v.as_str()) {
            integ.payload_format_version = Some(s.to_string());
        }
        if let Some(n) = props.get("TimeoutInMillis").and_then(|v| v.as_i64()) {
            integ.timeout_in_millis = Some(n);
        }
        Ok(ProvisionResult::new(integration_id.clone())
            .with("IntegrationId", integration_id)
            .with("ApiId", api_id))
    }

    /// In-place update for AWS::ApiGatewayV2::IntegrationResponse. The
    /// composite physical id `{integration_id}/{response_id}` is stable
    /// across updates; only the embedded response body is rewritten.
    fn update_apigwv2_integration_response(
        &self,
        existing: &StackResource,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let api_id = props
            .get("ApiId")
            .and_then(|v| v.as_str())
            .ok_or("ApiId is required")?
            .to_string();
        let composite_key = existing.physical_id.clone();
        let (integration_id, response_id) = composite_key
            .split_once('/')
            .map(|(a, b)| (a.to_string(), b.to_string()))
            .ok_or_else(|| format!("Invalid IntegrationResponse physical id: {composite_key}"))?;
        let key_expr = props
            .get("IntegrationResponseKey")
            .and_then(|v| v.as_str())
            .ok_or("IntegrationResponseKey is required")?
            .to_string();
        let body = serde_json::json!({
            "integrationResponseId": response_id,
            "integrationId": integration_id,
            "integrationResponseKey": key_expr,
            "responseTemplates": props.get("ResponseTemplates").cloned().unwrap_or(serde_json::json!({})),
            "responseParameters": props.get("ResponseParameters").cloned().unwrap_or(serde_json::json!({})),
            "templateSelectionExpression": props.get("TemplateSelectionExpression").and_then(|v| v.as_str()),
            "contentHandlingStrategy": props.get("ContentHandlingStrategy").and_then(|v| v.as_str()),
        });
        let mut accounts = self.apigatewayv2_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let map = state
            .integration_responses
            .get_mut(&api_id)
            .ok_or_else(|| format!("No integration responses found for api {api_id}"))?;
        if !map.contains_key(&composite_key) {
            return Err(format!(
                "IntegrationResponse {composite_key} not yet provisioned for api {api_id}"
            ));
        }
        map.insert(composite_key.clone(), body);
        Ok(ProvisionResult::new(composite_key)
            .with("IntegrationResponseId", response_id)
            .with("IntegrationId", integration_id)
            .with("ApiId", api_id))
    }

    /// In-place update for AWS::ApiGatewayV2::RouteResponse. Composite
    /// physical id `{route_id}/{response_id}` is preserved.
    fn update_apigwv2_route_response(
        &self,
        existing: &StackResource,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let api_id = props
            .get("ApiId")
            .and_then(|v| v.as_str())
            .ok_or("ApiId is required")?
            .to_string();
        let composite = existing.physical_id.clone();
        let (route_id, response_id) = composite
            .split_once('/')
            .map(|(a, b)| (a.to_string(), b.to_string()))
            .ok_or_else(|| format!("Invalid RouteResponse physical id: {composite}"))?;
        let key_expr = props
            .get("RouteResponseKey")
            .and_then(|v| v.as_str())
            .ok_or("RouteResponseKey is required")?
            .to_string();
        let body = serde_json::json!({
            "routeResponseId": response_id,
            "routeId": route_id,
            "routeResponseKey": key_expr,
            "responseModels": props.get("ResponseModels").cloned().unwrap_or(serde_json::json!({})),
            "modelSelectionExpression": props.get("ModelSelectionExpression").and_then(|v| v.as_str()),
            "responseParameters": props.get("ResponseParameters").cloned().unwrap_or(serde_json::json!({})),
        });
        let mut accounts = self.apigatewayv2_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let map = state
            .route_responses
            .get_mut(&api_id)
            .ok_or_else(|| format!("No route responses found for api {api_id}"))?;
        if !map.contains_key(&composite) {
            return Err(format!(
                "RouteResponse {composite} not yet provisioned for api {api_id}"
            ));
        }
        map.insert(composite.clone(), body);
        Ok(ProvisionResult::new(composite)
            .with("RouteResponseId", response_id)
            .with("RouteId", route_id)
            .with("ApiId", api_id))
    }

    /// In-place update for AWS::ApiGatewayV2::Stage. StageName is the
    /// physical id and stays stable; description / deployment id /
    /// auto-deploy can change.
    fn update_apigwv2_stage(
        &self,
        existing: &StackResource,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let api_id = props
            .get("ApiId")
            .and_then(|v| v.as_str())
            .ok_or("ApiId is required")?
            .to_string();
        let stage_name = existing.physical_id.clone();

        let mut accounts = self.apigatewayv2_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let stages = state
            .stages
            .get_mut(&api_id)
            .ok_or_else(|| format!("Api {api_id} not yet provisioned"))?;
        let stage = stages
            .get_mut(&stage_name)
            .ok_or_else(|| format!("Stage {stage_name} not yet provisioned for api {api_id}"))?;
        if let Some(s) = props.get("Description").and_then(|v| v.as_str()) {
            stage.description = Some(s.to_string());
        }
        if let Some(s) = props.get("DeploymentId").and_then(|v| v.as_str()) {
            stage.deployment_id = Some(s.to_string());
        }
        if let Some(b) = props.get("AutoDeploy").and_then(|v| v.as_bool()) {
            stage.auto_deploy = b;
        }
        stage.last_updated_date = Some(Utc::now());
        Ok(ProvisionResult::new(stage_name.clone())
            .with("StageName", stage_name)
            .with("ApiId", api_id))
    }

    /// In-place update for AWS::ApiGatewayV2::Deployment. CFN allows
    /// editing the Description; the DeploymentId is stable.
    fn update_apigwv2_deployment(
        &self,
        existing: &StackResource,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let api_id = props
            .get("ApiId")
            .and_then(|v| v.as_str())
            .ok_or("ApiId is required")?
            .to_string();
        let deployment_id = existing.physical_id.clone();
        let mut accounts = self.apigatewayv2_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let deps = state
            .deployments
            .get_mut(&api_id)
            .ok_or_else(|| format!("Api {api_id} not yet provisioned"))?;
        let dep = deps.get_mut(&deployment_id).ok_or_else(|| {
            format!("Deployment {deployment_id} not yet provisioned for api {api_id}")
        })?;
        if let Some(s) = props.get("Description").and_then(|v| v.as_str()) {
            dep.description = Some(s.to_string());
        }
        Ok(ProvisionResult::new(deployment_id.clone())
            .with("DeploymentId", deployment_id)
            .with("ApiId", api_id))
    }

    /// In-place update for AWS::ApiGatewayV2::Authorizer. Mutates the
    /// stored authorizer in place; the AuthorizerId remains stable.
    fn update_apigwv2_authorizer(
        &self,
        existing: &StackResource,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let api_id = props
            .get("ApiId")
            .and_then(|v| v.as_str())
            .ok_or("ApiId is required")?
            .to_string();
        let authorizer_id = existing.physical_id.clone();

        let mut accounts = self.apigatewayv2_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let auths = state
            .authorizers
            .get_mut(&api_id)
            .ok_or_else(|| format!("Api {api_id} not yet provisioned"))?;
        let auth = auths.get_mut(&authorizer_id).ok_or_else(|| {
            format!("Authorizer {authorizer_id} not yet provisioned for api {api_id}")
        })?;
        if let Some(s) = props.get("Name").and_then(|v| v.as_str()) {
            auth.name = s.to_string();
        }
        if let Some(s) = props.get("AuthorizerType").and_then(|v| v.as_str()) {
            auth.authorizer_type = s.to_string();
        }
        if let Some(s) = props.get("AuthorizerUri").and_then(|v| v.as_str()) {
            auth.authorizer_uri = Some(s.to_string());
        }
        if let Some(arr) = props.get("IdentitySource").and_then(|v| v.as_array()) {
            auth.identity_source = Some(
                arr.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect(),
            );
        }
        if let Some(obj) = props.get("JwtConfiguration").and_then(|v| v.as_object()) {
            auth.jwt_configuration = Some(ApiGwV2JwtConfiguration {
                audience: obj.get("Audience").and_then(|v| v.as_array()).map(|a| {
                    a.iter()
                        .filter_map(|v| v.as_str().map(String::from))
                        .collect()
                }),
                issuer: obj.get("Issuer").and_then(|v| v.as_str()).map(String::from),
            });
        }
        Ok(ProvisionResult::new(authorizer_id.clone())
            .with("AuthorizerId", authorizer_id)
            .with("ApiId", api_id))
    }

    /// In-place update for AWS::ApiGatewayV2::DomainName. The DomainName
    /// is the physical id and stays stable; configuration / mTLS can
    /// change.
    fn update_apigwv2_domain_name(
        &self,
        existing: &StackResource,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let domain_name = existing.physical_id.clone();
        let body = serde_json::json!({
            "domainName": domain_name,
            "domainNameConfigurations": props.get("DomainNameConfigurations").cloned().unwrap_or(serde_json::json!([])),
            "mutualTlsAuthentication": props.get("MutualTlsAuthentication").cloned(),
            "apiMappingSelectionExpression": null,
        });
        let mut accounts = self.apigatewayv2_state.write();
        let state = accounts.get_or_create(&self.account_id);
        if !state.domain_names.contains_key(&domain_name) {
            return Err(format!("DomainName {domain_name} no longer exists"));
        }
        state.domain_names.insert(domain_name.clone(), body);
        Ok(ProvisionResult::new(domain_name.clone()).with("DomainName", domain_name))
    }

    /// In-place update for AWS::ApiGatewayV2::ApiMapping. The ApiMappingId
    /// is the physical id; ApiId / Stage / ApiMappingKey can change.
    fn update_apigwv2_api_mapping(
        &self,
        existing: &StackResource,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let domain_name = props
            .get("DomainName")
            .and_then(|v| v.as_str())
            .ok_or("DomainName is required")?
            .to_string();
        let api_id = props
            .get("ApiId")
            .and_then(|v| v.as_str())
            .ok_or("ApiId is required")?
            .to_string();
        let stage = props
            .get("Stage")
            .and_then(|v| v.as_str())
            .ok_or("Stage is required")?
            .to_string();
        let api_mapping_key = props
            .get("ApiMappingKey")
            .and_then(|v| v.as_str())
            .map(String::from);
        let id = existing.physical_id.clone();
        let body = serde_json::json!({
            "apiMappingId": id,
            "apiId": api_id,
            "stage": stage,
            "apiMappingKey": api_mapping_key,
        });
        let mut accounts = self.apigatewayv2_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let map = state
            .api_mappings
            .get_mut(&domain_name)
            .ok_or_else(|| format!("DomainName {domain_name} no longer exists"))?;
        map.insert(id.clone(), body);
        Ok(ProvisionResult::new(id.clone())
            .with("ApiMappingId", id)
            .with("DomainName", domain_name))
    }

    /// In-place update for AWS::ApiGatewayV2::VpcLink. The VpcLinkId is
    /// the physical id; Name / SubnetIds / SecurityGroupIds / Tags can
    /// change.
    fn update_apigwv2_vpc_link(
        &self,
        existing: &StackResource,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let id = existing.physical_id.clone();
        let mut accounts = self.apigatewayv2_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let body = state
            .vpc_links
            .get_mut(&id)
            .ok_or_else(|| format!("VpcLink {id} no longer exists"))?;
        if let Some(s) = props.get("Name").and_then(|v| v.as_str()) {
            body["name"] = serde_json::Value::String(s.to_string());
        }
        if let Some(v) = props.get("SubnetIds").cloned() {
            body["subnetIds"] = v;
        }
        if let Some(v) = props.get("SecurityGroupIds").cloned() {
            body["securityGroupIds"] = v;
        }
        if let Some(v) = props.get("Tags").cloned() {
            body["tags"] = v;
        }
        Ok(ProvisionResult::new(id.clone()).with("VpcLinkId", id))
    }

    /// In-place update for AWS::ApiGatewayV2::Model. The ModelId is the
    /// physical id and stays stable; Schema / Description / ContentType
    /// / Name can change.
    fn update_apigwv2_model(
        &self,
        existing: &StackResource,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let api_id = props
            .get("ApiId")
            .and_then(|v| v.as_str())
            .ok_or("ApiId is required")?
            .to_string();
        let id = existing.physical_id.clone();
        let mut accounts = self.apigatewayv2_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let map = state
            .models
            .get_mut(&api_id)
            .ok_or_else(|| format!("Api {api_id} not yet provisioned"))?;
        let body = map
            .get_mut(&id)
            .ok_or_else(|| format!("Model {id} not yet provisioned for api {api_id}"))?;
        if let Some(s) = props.get("Name").and_then(|v| v.as_str()) {
            body["name"] = serde_json::Value::String(s.to_string());
        }
        if let Some(s) = props.get("ContentType").and_then(|v| v.as_str()) {
            body["contentType"] = serde_json::Value::String(s.to_string());
        }
        if let Some(s) = props.get("Description").and_then(|v| v.as_str()) {
            body["description"] = serde_json::Value::String(s.to_string());
        }
        if let Some(v) = props.get("Schema") {
            body["schema"] = serde_json::Value::String(if let Some(s) = v.as_str() {
                s.to_string()
            } else {
                v.to_string()
            });
        }
        Ok(ProvisionResult::new(id.clone())
            .with("ModelId", id)
            .with("ApiId", api_id))
    }

    // --- SES ---

    fn create_ses_configuration_set(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let name = props
            .get("Name")
            .and_then(|v| v.as_str())
            .map(String::from)
            .unwrap_or_else(|| format!("cfn-cs-{}", resource.logical_id));
        let sending_enabled = props
            .get("SendingOptions")
            .and_then(|v| v.get("SendingEnabled"))
            .and_then(|v| v.as_bool())
            .unwrap_or(true);
        let tls_policy = props
            .get("DeliveryOptions")
            .and_then(|v| v.get("TlsPolicy"))
            .and_then(|v| v.as_str())
            .unwrap_or("OPTIONAL")
            .to_string();
        let sending_pool_name = props
            .get("DeliveryOptions")
            .and_then(|v| v.get("SendingPoolName"))
            .and_then(|v| v.as_str())
            .map(String::from);
        let custom_redirect_domain = props
            .get("TrackingOptions")
            .and_then(|v| v.get("CustomRedirectDomain"))
            .and_then(|v| v.as_str())
            .map(String::from);
        let suppressed_reasons: Vec<String> = props
            .get("SuppressionOptions")
            .and_then(|v| v.get("SuppressedReasons"))
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();
        let reputation_metrics_enabled = props
            .get("ReputationOptions")
            .and_then(|v| v.get("ReputationMetricsEnabled"))
            .and_then(|v| v.as_bool())
            .unwrap_or(false);

        let cs = SesConfigurationSet {
            name: name.clone(),
            sending_enabled,
            tls_policy,
            sending_pool_name,
            custom_redirect_domain,
            https_policy: props
                .get("TrackingOptions")
                .and_then(|v| v.get("HttpsPolicy"))
                .and_then(|v| v.as_str())
                .map(String::from),
            suppressed_reasons,
            reputation_metrics_enabled,
            vdm_options: props.get("VdmOptions").cloned(),
            archive_arn: props
                .get("ArchivingOptions")
                .and_then(|v| v.get("ArchiveArn"))
                .and_then(|v| v.as_str())
                .map(String::from),
        };
        let mut accounts = self.ses_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.configuration_sets.insert(name.clone(), cs);
        Ok(ProvisionResult::new(name.clone()).with("Name", name))
    }

    fn delete_ses_configuration_set(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.ses_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.configuration_sets.remove(physical_id);
        state.event_destinations.remove(physical_id);
        Ok(())
    }

    fn create_ses_event_destination(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let cs_name = props
            .get("ConfigurationSetName")
            .and_then(|v| v.as_str())
            .ok_or("ConfigurationSetName is required")?
            .to_string();
        let dest_props = props
            .get("EventDestination")
            .and_then(|v| v.as_object())
            .ok_or("EventDestination is required")?;
        let name = dest_props
            .get("Name")
            .and_then(|v| v.as_str())
            .map(String::from)
            .unwrap_or_else(|| format!("cfn-ed-{}", resource.logical_id));
        let enabled = dest_props
            .get("Enabled")
            .and_then(|v| v.as_bool())
            .unwrap_or(true);
        let matching_event_types: Vec<String> = dest_props
            .get("MatchingEventTypes")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();
        let dest = SesEventDestination {
            name: name.clone(),
            enabled,
            matching_event_types,
            kinesis_firehose_destination: dest_props.get("KinesisFirehoseDestination").cloned(),
            cloud_watch_destination: dest_props.get("CloudWatchDestination").cloned(),
            sns_destination: dest_props.get("SnsDestination").cloned(),
            event_bridge_destination: dest_props.get("EventBridgeDestination").cloned(),
            pinpoint_destination: dest_props.get("PinpointDestination").cloned(),
        };
        let mut accounts = self.ses_state.write();
        let state = accounts.get_or_create(&self.account_id);
        if !state.configuration_sets.contains_key(&cs_name) {
            return Err(format!("ConfigurationSet {cs_name} not yet provisioned"));
        }
        let dests = state.event_destinations.entry(cs_name.clone()).or_default();
        dests.retain(|d| d.name != name);
        dests.push(dest);
        let physical = format!("{cs_name}|{name}");
        Ok(ProvisionResult::new(physical)
            .with("Name", name)
            .with("ConfigurationSetName", cs_name))
    }

    fn delete_ses_event_destination(
        &self,
        physical_id: &str,
        _attributes: &BTreeMap<String, String>,
    ) -> Result<(), String> {
        let mut parts = physical_id.splitn(2, '|');
        let Some(cs) = parts.next() else {
            return Ok(());
        };
        let Some(name) = parts.next() else {
            return Ok(());
        };
        let mut accounts = self.ses_state.write();
        let state = accounts.get_or_create(&self.account_id);
        if let Some(dests) = state.event_destinations.get_mut(cs) {
            dests.retain(|d| d.name != name);
        }
        Ok(())
    }

    fn create_ses_email_identity(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let identity_name = props
            .get("EmailIdentity")
            .and_then(|v| v.as_str())
            .ok_or("EmailIdentity is required")?
            .to_string();
        let identity_type = if identity_name.contains('@') {
            "EMAIL_ADDRESS"
        } else {
            "DOMAIN"
        }
        .to_string();
        let dkim_signing_enabled = props
            .get("DkimAttributes")
            .and_then(|v| v.get("SigningEnabled"))
            .and_then(|v| v.as_bool())
            .unwrap_or(true);
        let dkim_signing_attributes_origin = props
            .get("DkimSigningAttributes")
            .map(|_| "EXTERNAL")
            .unwrap_or("AWS_SES")
            .to_string();
        let mail_from_domain = props
            .get("MailFromAttributes")
            .and_then(|v| v.get("MailFromDomain"))
            .and_then(|v| v.as_str())
            .map(String::from);
        let mail_from_behavior = props
            .get("MailFromAttributes")
            .and_then(|v| v.get("BehaviorOnMxFailure"))
            .and_then(|v| v.as_str())
            .unwrap_or("USE_DEFAULT_VALUE")
            .to_string();
        let configuration_set_name = props
            .get("ConfigurationSetAttributes")
            .and_then(|v| v.get("ConfigurationSetName"))
            .and_then(|v| v.as_str())
            .map(String::from);
        let email_forwarding_enabled = props
            .get("FeedbackAttributes")
            .and_then(|v| v.get("EmailForwardingEnabled"))
            .and_then(|v| v.as_bool())
            .unwrap_or(true);

        let identity = SesEmailIdentity {
            identity_name: identity_name.clone(),
            identity_type,
            verified: true,
            created_at: Utc::now(),
            dkim_signing_enabled,
            dkim_signing_attributes_origin,
            dkim_domain_signing_private_key: None,
            dkim_domain_signing_selector: None,
            dkim_next_signing_key_length: None,
            dkim_public_key_b64: None,
            email_forwarding_enabled,
            mail_from_domain,
            mail_from_behavior_on_mx_failure: mail_from_behavior,
            mail_from_domain_status: "Success".to_string(),
            configuration_set_name,
        };
        let mut accounts = self.ses_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.identities.insert(identity_name.clone(), identity);
        Ok(ProvisionResult::new(identity_name.clone()).with("EmailIdentity", identity_name))
    }

    fn delete_ses_email_identity(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.ses_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.identities.remove(physical_id);
        Ok(())
    }

    fn create_ses_template(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let template_block = props
            .get("Template")
            .and_then(|v| v.as_object())
            .ok_or("Template is required")?;
        let template_name = template_block
            .get("TemplateName")
            .and_then(|v| v.as_str())
            .map(String::from)
            .unwrap_or_else(|| format!("cfn-tpl-{}", resource.logical_id));
        let tpl = SesEmailTemplate {
            template_name: template_name.clone(),
            subject: template_block
                .get("SubjectPart")
                .and_then(|v| v.as_str())
                .map(String::from),
            html_body: template_block
                .get("HtmlPart")
                .and_then(|v| v.as_str())
                .map(String::from),
            text_body: template_block
                .get("TextPart")
                .and_then(|v| v.as_str())
                .map(String::from),
            created_at: Utc::now(),
        };
        let mut accounts = self.ses_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.templates.insert(template_name.clone(), tpl);
        Ok(ProvisionResult::new(template_name.clone()).with("TemplateName", template_name))
    }

    fn delete_ses_template(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.ses_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.templates.remove(physical_id);
        Ok(())
    }

    fn create_ses_contact_list(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let name = props
            .get("ContactListName")
            .and_then(|v| v.as_str())
            .map(String::from)
            .unwrap_or_else(|| format!("cfn-cl-{}", resource.logical_id));
        let description = props
            .get("Description")
            .and_then(|v| v.as_str())
            .map(String::from);
        let now = Utc::now();
        let cl = SesContactList {
            contact_list_name: name.clone(),
            description,
            topics: Vec::new(),
            created_at: now,
            last_updated_at: now,
        };
        let mut accounts = self.ses_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.contact_lists.insert(name.clone(), cl);
        Ok(ProvisionResult::new(name.clone()).with("ContactListName", name))
    }

    fn delete_ses_contact_list(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.ses_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.contact_lists.remove(physical_id);
        state.contacts.remove(physical_id);
        Ok(())
    }

    fn create_ses_dedicated_ip_pool(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let name = props
            .get("PoolName")
            .and_then(|v| v.as_str())
            .map(String::from)
            .unwrap_or_else(|| format!("cfn-pool-{}", resource.logical_id));
        let scaling_mode = props
            .get("ScalingMode")
            .and_then(|v| v.as_str())
            .unwrap_or("STANDARD")
            .to_string();
        let pool = SesDedicatedIpPool {
            pool_name: name.clone(),
            scaling_mode,
        };
        let mut accounts = self.ses_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.dedicated_ip_pools.insert(name.clone(), pool);
        Ok(ProvisionResult::new(name.clone()).with("PoolName", name))
    }

    fn delete_ses_dedicated_ip_pool(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.ses_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.dedicated_ip_pools.remove(physical_id);
        Ok(())
    }

    fn create_ses_receipt_rule_set(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let name = props
            .get("RuleSetName")
            .and_then(|v| v.as_str())
            .map(String::from)
            .unwrap_or_else(|| format!("cfn-rs-{}", resource.logical_id));
        let rs = SesReceiptRuleSet {
            name: name.clone(),
            rules: Vec::new(),
            created_at: Utc::now(),
        };
        let mut accounts = self.ses_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.receipt_rule_sets.insert(name.clone(), rs);
        Ok(ProvisionResult::new(name.clone()).with("RuleSetName", name))
    }

    fn delete_ses_receipt_rule_set(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.ses_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.receipt_rule_sets.remove(physical_id);
        Ok(())
    }

    fn create_ses_receipt_rule(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let rule_set_name = props
            .get("RuleSetName")
            .and_then(|v| v.as_str())
            .ok_or("RuleSetName is required")?
            .to_string();
        let rule_block = props
            .get("Rule")
            .and_then(|v| v.as_object())
            .ok_or("Rule is required")?;
        let name = rule_block
            .get("Name")
            .and_then(|v| v.as_str())
            .map(String::from)
            .unwrap_or_else(|| format!("cfn-rule-{}", resource.logical_id));
        let enabled = rule_block
            .get("Enabled")
            .and_then(|v| v.as_bool())
            .unwrap_or(true);
        let scan_enabled = rule_block
            .get("ScanEnabled")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let tls_policy = rule_block
            .get("TlsPolicy")
            .and_then(|v| v.as_str())
            .unwrap_or("Optional")
            .to_string();
        let recipients: Vec<String> = rule_block
            .get("Recipients")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();
        let actions: Vec<SesReceiptAction> = rule_block
            .get("Actions")
            .and_then(|v| v.as_array())
            .map(|arr| arr.iter().filter_map(parse_ses_receipt_action).collect())
            .unwrap_or_default();

        let rule = SesReceiptRule {
            name: name.clone(),
            enabled,
            scan_enabled,
            tls_policy,
            recipients,
            actions,
        };
        let mut accounts = self.ses_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let rs = state
            .receipt_rule_sets
            .get_mut(&rule_set_name)
            .ok_or_else(|| format!("ReceiptRuleSet {rule_set_name} not yet provisioned"))?;
        rs.rules.retain(|r| r.name != name);
        rs.rules.push(rule);
        let physical = format!("{rule_set_name}|{name}");
        Ok(ProvisionResult::new(physical)
            .with("Name", name)
            .with("RuleSetName", rule_set_name))
    }

    fn delete_ses_receipt_rule(
        &self,
        physical_id: &str,
        _attributes: &BTreeMap<String, String>,
    ) -> Result<(), String> {
        let mut parts = physical_id.splitn(2, '|');
        let Some(rs_name) = parts.next() else {
            return Ok(());
        };
        let Some(rule_name) = parts.next() else {
            return Ok(());
        };
        let mut accounts = self.ses_state.write();
        let state = accounts.get_or_create(&self.account_id);
        if let Some(rs) = state.receipt_rule_sets.get_mut(rs_name) {
            rs.rules.retain(|r| r.name != rule_name);
        }
        Ok(())
    }

    fn create_ses_receipt_filter(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let filter_block = props
            .get("Filter")
            .and_then(|v| v.as_object())
            .ok_or("Filter is required")?;
        let name = filter_block
            .get("Name")
            .and_then(|v| v.as_str())
            .map(String::from)
            .unwrap_or_else(|| format!("cfn-filter-{}", resource.logical_id));
        let ip_block = filter_block
            .get("IpFilter")
            .and_then(|v| v.as_object())
            .ok_or("Filter.IpFilter is required")?;
        let cidr = ip_block
            .get("Cidr")
            .and_then(|v| v.as_str())
            .ok_or("Filter.IpFilter.Cidr is required")?
            .to_string();
        let policy = ip_block
            .get("Policy")
            .and_then(|v| v.as_str())
            .unwrap_or("Block")
            .to_string();
        let filter = SesReceiptFilter {
            name: name.clone(),
            ip_filter: SesIpFilter { cidr, policy },
        };
        let mut accounts = self.ses_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.receipt_filters.insert(name.clone(), filter);
        Ok(ProvisionResult::new(name.clone()).with("Name", name))
    }

    fn delete_ses_receipt_filter(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.ses_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.receipt_filters.remove(physical_id);
        Ok(())
    }

    fn create_ses_vdm_attributes(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let mut accounts = self.ses_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.account_settings.vdm_attributes = Some(props.clone());
        Ok(ProvisionResult::new(format!("vdm-{}", resource.logical_id)))
    }

    // --- SecretsManager extras ---

    fn create_secrets_manager_rotation_schedule(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let secret_id = props
            .get("SecretId")
            .and_then(|v| v.as_str())
            .ok_or("SecretId is required")?
            .to_string();
        let rotation_lambda_arn = props
            .get("RotationLambdaARN")
            .and_then(|v| v.as_str())
            .map(String::from);
        let automatically_after_days = props
            .get("RotationRules")
            .and_then(|v| v.get("AutomaticallyAfterDays"))
            .and_then(|v| v.as_i64());
        let mut accounts = self.secretsmanager_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let secret_arn = if state.secrets.contains_key(&secret_id) {
            secret_id.clone()
        } else {
            let candidate = format!(
                "arn:aws:secretsmanager:{}:{}:secret:{}",
                state.region, state.account_id, secret_id
            );
            if state.secrets.contains_key(&candidate) {
                candidate
            } else {
                return Err(format!("Secret {secret_id} not yet provisioned"));
            }
        };
        let secret = state
            .secrets
            .get_mut(&secret_arn)
            .ok_or_else(|| format!("Secret {secret_arn} not found"))?;
        secret.rotation_enabled = Some(true);
        secret.rotation_lambda_arn = rotation_lambda_arn;
        secret.rotation_rules = Some(RotationRules {
            automatically_after_days,
        });
        secret.last_changed_at = Utc::now();
        Ok(ProvisionResult::new(secret_arn.clone()).with("SecretArn", secret_arn))
    }

    fn delete_secrets_manager_rotation_schedule(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.secretsmanager_state.write();
        let state = accounts.get_or_create(&self.account_id);
        if let Some(secret) = state.secrets.get_mut(physical_id) {
            secret.rotation_enabled = Some(false);
            secret.rotation_lambda_arn = None;
            secret.rotation_rules = None;
            secret.last_changed_at = Utc::now();
        }
        Ok(())
    }

    fn create_secrets_manager_resource_policy(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let secret_id = props
            .get("SecretId")
            .and_then(|v| v.as_str())
            .ok_or("SecretId is required")?
            .to_string();
        let policy_doc = props
            .get("ResourcePolicy")
            .ok_or("ResourcePolicy is required")?;
        let policy_str = match policy_doc {
            serde_json::Value::String(s) => s.clone(),
            other => other.to_string(),
        };
        let mut accounts = self.secretsmanager_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let secret_arn = if state.secrets.contains_key(&secret_id) {
            secret_id.clone()
        } else {
            let candidate = format!(
                "arn:aws:secretsmanager:{}:{}:secret:{}",
                state.region, state.account_id, secret_id
            );
            if state.secrets.contains_key(&candidate) {
                candidate
            } else {
                return Err(format!("Secret {secret_id} not yet provisioned"));
            }
        };
        let secret = state
            .secrets
            .get_mut(&secret_arn)
            .ok_or_else(|| format!("Secret {secret_arn} not found"))?;
        secret.resource_policy = Some(policy_str);
        secret.last_changed_at = Utc::now();
        Ok(ProvisionResult::new(secret_arn.clone()).with("SecretArn", secret_arn))
    }

    fn delete_secrets_manager_resource_policy(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.secretsmanager_state.write();
        let state = accounts.get_or_create(&self.account_id);
        if let Some(secret) = state.secrets.get_mut(physical_id) {
            secret.resource_policy = None;
            secret.last_changed_at = Utc::now();
        }
        Ok(())
    }

    fn create_secrets_manager_target_attachment(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let secret_id = props
            .get("SecretId")
            .and_then(|v| v.as_str())
            .ok_or("SecretId is required")?
            .to_string();
        let target_type = props
            .get("TargetType")
            .and_then(|v| v.as_str())
            .ok_or("TargetType is required")?;
        let target_id = props
            .get("TargetId")
            .and_then(|v| v.as_str())
            .ok_or("TargetId is required")?;
        let mut accounts = self.secretsmanager_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let secret_arn = if state.secrets.contains_key(&secret_id) {
            secret_id.clone()
        } else {
            let candidate = format!(
                "arn:aws:secretsmanager:{}:{}:secret:{}",
                state.region, state.account_id, secret_id
            );
            if state.secrets.contains_key(&candidate) {
                candidate
            } else {
                return Err(format!("Secret {secret_id} not yet provisioned"));
            }
        };
        let secret = state
            .secrets
            .get_mut(&secret_arn)
            .ok_or_else(|| format!("Secret {secret_arn} not found"))?;
        // Patch the AWSCURRENT version with engine/host/dbInstanceIdentifier
        // so it shows as "attached" via the RDS-style schema CFN expects.
        // If the secret has no version yet (created without SecretString or
        // GenerateSecretString), seed one — this matches CFN's behaviour of
        // making the attachment usable on its own.
        let now = Utc::now();
        if secret.current_version_id.is_none() {
            let version_id = Uuid::new_v4().to_string();
            secret.versions.insert(
                version_id.clone(),
                SecretVersion {
                    version_id: version_id.clone(),
                    secret_string: Some("{}".to_string()),
                    secret_binary: None,
                    stages: vec!["AWSCURRENT".to_string()],
                    created_at: now,
                },
            );
            secret.current_version_id = Some(version_id);
        }
        if let Some(version_id) = secret.current_version_id.clone() {
            if let Some(version) = secret.versions.get_mut(&version_id) {
                let mut existing: serde_json::Value = version
                    .secret_string
                    .as_deref()
                    .and_then(|s| serde_json::from_str(s).ok())
                    .unwrap_or_else(|| serde_json::json!({}));
                if let Some(obj) = existing.as_object_mut() {
                    let engine = match target_type {
                        "AWS::RDS::DBInstance" | "AWS::RDS::DBCluster" => "postgres",
                        _ => "unknown",
                    };
                    obj.entry("engine".to_string())
                        .or_insert(serde_json::json!(engine));
                    obj.insert("host".to_string(), serde_json::json!(target_id));
                    obj.entry("dbInstanceIdentifier".to_string())
                        .or_insert(serde_json::json!(target_id));
                }
                version.secret_string = Some(existing.to_string());
            }
        }
        secret.last_changed_at = now;
        Ok(ProvisionResult::new(secret_arn.clone()).with("SecretArn", secret_arn))
    }

    // --- Athena ---

    fn create_athena_work_group(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let name = props
            .get("Name")
            .and_then(|v| v.as_str())
            .unwrap_or(&resource.logical_id)
            .to_string();
        let description = props
            .get("Description")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let configuration = props.get("Configuration").cloned();
        let state_str = props
            .get("State")
            .and_then(|v| v.as_str())
            .unwrap_or("ENABLED");
        let tags = Self::parse_athena_tags(props.get("Tags"));

        let mut accounts = self.athena_state.write();
        let account = accounts
            .accounts
            .entry(self.account_id.clone())
            .or_default();
        account.ensure_initialized();
        if account.work_groups.contains_key(&name) {
            return Err(format!("WorkGroup {name} already exists"));
        }
        let wg = WorkGroup {
            name: name.clone(),
            state: state_str.to_string(),
            description,
            configuration,
            creation_time: Utc::now(),
            engine_version: Some("AUTO".to_string()),
        };
        let arn = format!(
            "arn:aws:athena:{}:{}:workgroup/{}",
            self.region, self.account_id, name
        );
        account.work_groups.insert(name.clone(), wg);
        if !tags.is_empty() {
            account.tags.insert(arn.clone(), tags);
        }
        Ok(ProvisionResult::new(name.clone())
            .with("Arn", arn)
            .with("Name", name))
    }

    fn delete_athena_work_group(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.athena_state.write();
        let account = accounts
            .accounts
            .entry(self.account_id.clone())
            .or_default();
        account.work_groups.remove(physical_id);
        Ok(())
    }

    fn get_att_athena_work_group(&self, physical_id: &str, attribute: &str) -> Option<String> {
        let mut accounts = self.athena_state.write();
        let account = accounts
            .accounts
            .entry(self.account_id.clone())
            .or_default();
        let wg = account.work_groups.get(physical_id)?;
        match attribute {
            "Arn" => Some(format!(
                "arn:aws:athena:{}:{}:workgroup/{}",
                self.region, self.account_id, wg.name
            )),
            "Name" => Some(wg.name.clone()),
            _ => None,
        }
    }

    fn create_athena_data_catalog(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let name = props
            .get("Name")
            .and_then(|v| v.as_str())
            .unwrap_or(&resource.logical_id)
            .to_string();
        let cat_type = props
            .get("Type")
            .and_then(|v| v.as_str())
            .ok_or("Type is required")?
            .to_string();
        let description = props
            .get("Description")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let parameters = props
            .get("Parameters")
            .and_then(|v| v.as_object())
            .map(|m| {
                m.iter()
                    .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                    .collect()
            })
            .unwrap_or_default();
        let connection_type = props
            .get("ConnectionType")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let tags = Self::parse_athena_tags(props.get("Tags"));

        let mut accounts = self.athena_state.write();
        let account = accounts
            .accounts
            .entry(self.account_id.clone())
            .or_default();
        account.ensure_initialized();
        if account.data_catalogs.contains_key(&name) {
            return Err(format!("DataCatalog {name} already exists"));
        }
        let cat = DataCatalog {
            name: name.clone(),
            description,
            cat_type,
            parameters,
            status: "CREATE_COMPLETE".to_string(),
            connection_type,
            error: None,
        };
        let arn = format!(
            "arn:aws:athena:{}:{}:datacatalog/{}",
            self.region, self.account_id, name
        );
        account.data_catalogs.insert(name.clone(), cat);
        if !tags.is_empty() {
            account.tags.insert(arn.clone(), tags);
        }
        Ok(ProvisionResult::new(name.clone())
            .with("Arn", arn)
            .with("Name", name))
    }

    fn delete_athena_data_catalog(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.athena_state.write();
        let account = accounts
            .accounts
            .entry(self.account_id.clone())
            .or_default();
        account.data_catalogs.remove(physical_id);
        Ok(())
    }

    fn get_att_athena_data_catalog(&self, physical_id: &str, attribute: &str) -> Option<String> {
        let mut accounts = self.athena_state.write();
        let account = accounts
            .accounts
            .entry(self.account_id.clone())
            .or_default();
        let cat = account.data_catalogs.get(physical_id)?;
        match attribute {
            "Arn" => Some(format!(
                "arn:aws:athena:{}:{}:datacatalog/{}",
                self.region, self.account_id, cat.name
            )),
            "Name" => Some(cat.name.clone()),
            _ => None,
        }
    }

    fn create_athena_named_query(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let name = props
            .get("Name")
            .and_then(|v| v.as_str())
            .ok_or("Name is required")?
            .to_string();
        let database = props
            .get("Database")
            .and_then(|v| v.as_str())
            .ok_or("Database is required")?
            .to_string();
        let query_string = props
            .get("QueryString")
            .and_then(|v| v.as_str())
            .ok_or("QueryString is required")?
            .to_string();
        let description = props
            .get("Description")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let work_group = props
            .get("WorkGroup")
            .and_then(|v| v.as_str())
            .unwrap_or("primary")
            .to_string();

        let mut accounts = self.athena_state.write();
        let account = accounts
            .accounts
            .entry(self.account_id.clone())
            .or_default();
        account.ensure_initialized();
        if !account.work_groups.contains_key(&work_group) {
            return Err(format!("Workgroup {work_group} not found"));
        }
        let id = Uuid::new_v4().to_string();
        let nq = NamedQuery {
            named_query_id: id.clone(),
            name,
            description,
            database,
            query_string,
            work_group,
            last_used_at: None,
        };
        account.named_queries.insert(id.clone(), nq);
        Ok(ProvisionResult::new(id.clone()).with("NamedQueryId", id))
    }

    fn delete_athena_named_query(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.athena_state.write();
        let account = accounts
            .accounts
            .entry(self.account_id.clone())
            .or_default();
        account.named_queries.remove(physical_id);
        Ok(())
    }

    fn get_att_athena_named_query(&self, physical_id: &str, attribute: &str) -> Option<String> {
        let mut accounts = self.athena_state.write();
        let account = accounts
            .accounts
            .entry(self.account_id.clone())
            .or_default();
        let nq = account.named_queries.get(physical_id)?;
        match attribute {
            "NamedQueryId" => Some(nq.named_query_id.clone()),
            _ => None,
        }
    }

    fn create_athena_prepared_statement(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let statement_name = props
            .get("StatementName")
            .and_then(|v| v.as_str())
            .ok_or("StatementName is required")?
            .to_string();
        let work_group_name = props
            .get("WorkGroupName")
            .and_then(|v| v.as_str())
            .ok_or("WorkGroupName is required")?
            .to_string();
        let query_statement = props
            .get("QueryStatement")
            .and_then(|v| v.as_str())
            .ok_or("QueryStatement is required")?
            .to_string();
        let description = props
            .get("Description")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());

        let mut accounts = self.athena_state.write();
        let account = accounts
            .accounts
            .entry(self.account_id.clone())
            .or_default();
        account.ensure_initialized();
        if !account.work_groups.contains_key(&work_group_name) {
            return Err(format!("Workgroup {work_group_name} not found"));
        }
        let key = (work_group_name.clone(), statement_name.clone());
        if account.prepared_statements.contains_key(&key) {
            return Err(format!(
                "PreparedStatement {statement_name} already exists in {work_group_name}"
            ));
        }
        let ps = PreparedStatement {
            statement_name: statement_name.clone(),
            work_group_name: work_group_name.clone(),
            query_statement,
            description,
            last_modified_time: Utc::now(),
        };
        let physical_id = format!("{work_group_name}|{statement_name}");
        account.prepared_statements.insert(key, ps);
        Ok(ProvisionResult::new(physical_id))
    }

    fn delete_athena_prepared_statement(
        &self,
        physical_id: &str,
        _attrs: &BTreeMap<String, String>,
    ) -> Result<(), String> {
        let mut accounts = self.athena_state.write();
        let account = accounts
            .accounts
            .entry(self.account_id.clone())
            .or_default();
        let parts: Vec<&str> = physical_id.split('|').collect();
        if parts.len() != 2 {
            return Err(format!(
                "Invalid PreparedStatement physical id: {physical_id}"
            ));
        }
        let key = (parts[0].to_string(), parts[1].to_string());
        account.prepared_statements.remove(&key);
        Ok(())
    }

    fn get_att_athena_prepared_statement(
        &self,
        physical_id: &str,
        attribute: &str,
    ) -> Option<String> {
        let mut accounts = self.athena_state.write();
        let account = accounts
            .accounts
            .entry(self.account_id.clone())
            .or_default();
        let parts: Vec<&str> = physical_id.split('|').collect();
        if parts.len() != 2 {
            return None;
        }
        let ps = account
            .prepared_statements
            .get(&(parts[0].to_string(), parts[1].to_string()))?;
        match attribute {
            "StatementName" => Some(ps.statement_name.clone()),
            "WorkGroupName" => Some(ps.work_group_name.clone()),
            _ => None,
        }
    }

    fn parse_athena_tags(value: Option<&serde_json::Value>) -> BTreeMap<String, String> {
        let mut out = BTreeMap::new();
        let Some(arr) = value.and_then(|v| v.as_array()) else {
            return out;
        };
        for tag in arr {
            if let (Some(k), Some(v)) = (
                tag.get("Key").and_then(|v| v.as_str()),
                tag.get("Value").and_then(|v| v.as_str()),
            ) {
                out.insert(k.to_string(), v.to_string());
            }
        }
        out
    }

    fn create_glue_database(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let input = props
            .get("DatabaseInput")
            .ok_or("DatabaseInput is required")?;
        let name = input
            .get("Name")
            .and_then(|v| v.as_str())
            .unwrap_or(&resource.logical_id)
            .to_string();
        let description = input
            .get("Description")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let location_uri = input
            .get("LocationUri")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let parameters = input
            .get("Parameters")
            .and_then(|v| v.as_object())
            .map(|m| {
                m.iter()
                    .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                    .collect()
            })
            .unwrap_or_default();

        let mut accounts = self.glue_state.write();
        let state = accounts.get_or_create(&self.account_id, &self.region);
        let dbs = state.dbs_in_mut(&self.region);
        if dbs.contains_key(&name) {
            return Err(format!("Database {name} already exists"));
        }
        dbs.insert(
            name.clone(),
            fakecloud_glue::Database {
                name: name.clone(),
                description,
                location_uri,
                parameters,
                created_at: Utc::now(),
                catalog_id: self.account_id.clone(),
                tables: BTreeMap::new(),
            },
        );
        Ok(ProvisionResult::new(name.clone()))
    }

    fn delete_glue_database(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.glue_state.write();
        let state = accounts.get_or_create(&self.account_id, &self.region);
        state.dbs_in_mut(&self.region).remove(physical_id);
        Ok(())
    }

    fn create_glue_table(&self, resource: &ResourceDefinition) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let db_name = props
            .get("DatabaseName")
            .and_then(|v| v.as_str())
            .ok_or("DatabaseName is required")?
            .to_string();
        let input = props.get("TableInput").ok_or("TableInput is required")?;
        let name = input
            .get("Name")
            .and_then(|v| v.as_str())
            .ok_or("TableInput.Name is required")?
            .to_string();
        let now = Utc::now();

        let mut accounts = self.glue_state.write();
        let state = accounts.get_or_create(&self.account_id, &self.region);
        let dbs = state.dbs_in_mut(&self.region);
        let db = dbs
            .get_mut(&db_name)
            .ok_or_else(|| format!("Database {db_name} not found"))?;
        if db.tables.contains_key(&name) {
            return Err(format!("Table {name} already exists"));
        }
        db.tables.insert(
            name.clone(),
            fakecloud_glue::Table {
                name: name.clone(),
                database_name: db_name.clone(),
                description: input
                    .get("Description")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string()),
                owner: input
                    .get("Owner")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string()),
                create_time: now,
                update_time: now,
                last_access_time: None,
                retention: input.get("Retention").and_then(|v| v.as_i64()).unwrap_or(0),
                storage_descriptor: None,
                partition_keys: Vec::new(),
                view_original_text: input
                    .get("ViewOriginalText")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string()),
                view_expanded_text: input
                    .get("ViewExpandedText")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string()),
                table_type: input
                    .get("TableType")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string()),
                parameters: BTreeMap::new(),
                partitions: BTreeMap::new(),
            },
        );
        let physical_id = format!("{db_name}|{name}");
        Ok(ProvisionResult::new(physical_id))
    }

    fn delete_glue_table(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.glue_state.write();
        let state = accounts.get_or_create(&self.account_id, &self.region);
        let parts: Vec<&str> = physical_id.split('|').collect();
        if parts.len() != 2 {
            return Err(format!("Invalid Glue table physical id: {physical_id}"));
        }
        let dbs = state.dbs_in_mut(&self.region);
        let db = dbs
            .get_mut(parts[0])
            .ok_or_else(|| format!("Database {} not found", parts[0]))?;
        db.tables.remove(parts[1]);
        Ok(())
    }

    fn create_glue_partition(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let db_name = props
            .get("DatabaseName")
            .and_then(|v| v.as_str())
            .ok_or("DatabaseName is required")?
            .to_string();
        let table_name = props
            .get("TableName")
            .and_then(|v| v.as_str())
            .ok_or("TableName is required")?
            .to_string();
        let input = props
            .get("PartitionInput")
            .ok_or("PartitionInput is required")?;
        let values: Vec<String> = input
            .get("Values")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                    .collect()
            })
            .unwrap_or_default();
        if values.is_empty() {
            return Err("PartitionInput.Values is required".to_string());
        }
        let key = values
            .iter()
            .map(|v| {
                v.replace('%', "%25")
                    .replace('|', "%7C")
                    .replace('/', "%2F")
            })
            .collect::<Vec<_>>()
            .join("/");

        let mut accounts = self.glue_state.write();
        let state = accounts.get_or_create(&self.account_id, &self.region);
        let dbs = state.dbs_in_mut(&self.region);
        let db = dbs
            .get_mut(&db_name)
            .ok_or_else(|| format!("Database {db_name} not found"))?;
        let table = db
            .tables
            .get_mut(&table_name)
            .ok_or_else(|| format!("Table {table_name} not found"))?;
        if table.partitions.contains_key(&key) {
            return Err(format!("Partition {key} already exists"));
        }
        table.partitions.insert(
            key.clone(),
            fakecloud_glue::Partition {
                values: values.clone(),
                database_name: db_name.clone(),
                table_name: table_name.clone(),
                create_time: Utc::now(),
                last_access_time: None,
                storage_descriptor: None,
                parameters: BTreeMap::new(),
            },
        );
        let physical_id = format!("{db_name}|{table_name}|{key}");
        Ok(ProvisionResult::new(physical_id))
    }

    fn delete_glue_partition(
        &self,
        physical_id: &str,
        _attrs: &BTreeMap<String, String>,
    ) -> Result<(), String> {
        let mut accounts = self.glue_state.write();
        let state = accounts.get_or_create(&self.account_id, &self.region);
        let parts: Vec<&str> = physical_id.split('|').collect();
        if parts.len() != 3 {
            return Err(format!("Invalid Glue partition physical id: {physical_id}"));
        }
        let dbs = state.dbs_in_mut(&self.region);
        let db = dbs
            .get_mut(parts[0])
            .ok_or_else(|| format!("Database {} not found", parts[0]))?;
        let table = db
            .tables
            .get_mut(parts[1])
            .ok_or_else(|| format!("Table {} not found", parts[1]))?;
        table.partitions.remove(parts[2]);
        Ok(())
    }

    // --- CloudFormation Nested Stack ---

    fn create_cloudformation_stack(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;

        let template_body = if let Some(body) = props.get("TemplateBody").and_then(|v| v.as_str()) {
            body.to_string()
        } else if let Some(url) = props.get("TemplateURL").and_then(|v| v.as_str()) {
            self.fetch_template_from_url(url)?
        } else {
            return Err(
                "AWS::CloudFormation::Stack requires TemplateURL or TemplateBody".to_string(),
            );
        };

        let child_parameters =
            if let Some(params) = props.get("Parameters").and_then(|v| v.as_object()) {
                let mut map = BTreeMap::new();
                for (k, v) in params {
                    if let Some(s) = v.as_str() {
                        map.insert(k.clone(), s.to_string());
                    } else {
                        map.insert(k.clone(), v.to_string());
                    }
                }
                map
            } else {
                BTreeMap::new()
            };

        let child_tags = if let Some(tags) = props.get("Tags").and_then(|v| v.as_object()) {
            let mut map = BTreeMap::new();
            for (k, v) in tags {
                if let Some(s) = v.as_str() {
                    map.insert(k.clone(), s.to_string());
                }
            }
            map
        } else {
            BTreeMap::new()
        };

        let parsed = crate::template::parse_template(&template_body, &child_parameters)
            .map_err(|e| format!("Failed to parse nested template: {e}"))?;

        let child_stack_name = format!(
            "{}-Nested-{}",
            resource.logical_id,
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0)
        );
        let child_stack_id = format!(
            "arn:aws:cloudformation:{}:{}:stack/{}/{}",
            self.region,
            self.account_id,
            child_stack_name,
            Uuid::new_v4()
        );

        let child_provisioner = ResourceProvisioner {
            sqs_state: self.sqs_state.clone(),
            sns_state: self.sns_state.clone(),
            ssm_state: self.ssm_state.clone(),
            iam_state: self.iam_state.clone(),
            s3_state: self.s3_state.clone(),
            eventbridge_state: self.eventbridge_state.clone(),
            dynamodb_state: self.dynamodb_state.clone(),
            logs_state: self.logs_state.clone(),
            lambda_state: self.lambda_state.clone(),
            secretsmanager_state: self.secretsmanager_state.clone(),
            kinesis_state: self.kinesis_state.clone(),
            kms_state: self.kms_state.clone(),
            ecr_state: self.ecr_state.clone(),
            cloudwatch_state: self.cloudwatch_state.clone(),
            elbv2_state: self.elbv2_state.clone(),
            organizations_state: self.organizations_state.clone(),
            cognito_state: self.cognito_state.clone(),
            rds_state: self.rds_state.clone(),
            ecs_state: self.ecs_state.clone(),
            acm_state: self.acm_state.clone(),
            elasticache_state: self.elasticache_state.clone(),
            route53_state: self.route53_state.clone(),
            cloudfront_state: self.cloudfront_state.clone(),
            stepfunctions_state: self.stepfunctions_state.clone(),
            wafv2_state: self.wafv2_state.clone(),
            apigateway_state: self.apigateway_state.clone(),
            apigatewayv2_state: self.apigatewayv2_state.clone(),
            ses_state: self.ses_state.clone(),
            app_autoscaling_state: self.app_autoscaling_state.clone(),
            athena_state: self.athena_state.clone(),
            firehose_state: self.firehose_state.clone(),
            glue_state: self.glue_state.clone(),
            cloudformation_state: self.cloudformation_state.clone(),
            delivery: self.delivery.clone(),
            account_id: self.account_id.clone(),
            region: self.region.clone(),
            stack_id: child_stack_id.clone(),
        };

        let child_resources = crate::service::provision_stack_resources(
            &child_provisioner,
            &parsed.resources,
            &template_body,
            &child_parameters,
        )
        .map_err(|e| format!("Failed to provision nested stack: {e}"))?;

        let child_outputs = parsed.outputs;

        let stack = crate::state::Stack {
            name: child_stack_name.clone(),
            stack_id: child_stack_id.clone(),
            template: template_body.clone(),
            status: "CREATE_COMPLETE".to_string(),
            resources: child_resources.clone(),
            parameters: child_parameters,
            tags: child_tags,
            created_at: Utc::now(),
            updated_at: None,
            description: parsed.description,
            notification_arns: Vec::new(),
            outputs: child_outputs
                .iter()
                .map(|o| crate::state::StackOutput {
                    key: o.logical_id.clone(),
                    value: o.value.clone(),
                    description: o.description.clone(),
                    export_name: o.export_name.clone(),
                })
                .collect(),
        };

        {
            let mut accounts = self.cloudformation_state.write();
            let state = accounts.get_or_create(&self.account_id);
            state.stacks.insert(child_stack_name.clone(), stack);

            crate::service::record_stack_status_event(
                state,
                &child_stack_id,
                &child_stack_name,
                "AWS::CloudFormation::Stack",
                "CREATE_IN_PROGRESS",
            );
            let changes: Vec<crate::service::ResourceChange> = child_resources
                .iter()
                .map(|r| crate::service::ResourceChange {
                    action: crate::service::ResourceChangeAction::Create,
                    logical_id: r.logical_id.clone(),
                    physical_id: r.physical_id.clone(),
                    resource_type: r.resource_type.clone(),
                })
                .collect();
            crate::service::record_stack_events(
                state,
                &child_stack_id,
                &child_stack_name,
                &changes,
            );
            crate::service::record_stack_status_event(
                state,
                &child_stack_id,
                &child_stack_name,
                "AWS::CloudFormation::Stack",
                "CREATE_COMPLETE",
            );
        }

        let mut result = ProvisionResult::new(child_stack_id.clone());
        for output in &child_outputs {
            result.attributes.insert(
                format!("Outputs.{}", output.logical_id),
                output.value.clone(),
            );
        }
        Ok(result)
    }

    fn delete_cloudformation_stack(&self, physical_id: &str) -> Result<(), String> {
        let stack = {
            let accounts = self.cloudformation_state.read();
            let state = accounts.get(&self.account_id);
            state.and_then(|s| {
                s.stacks
                    .values()
                    .find(|st| st.stack_id == physical_id)
                    .cloned()
            })
        };

        if let Some(stack) = stack {
            let stack_name = stack.name.clone();
            let stack_id = stack.stack_id.clone();

            for resource in stack.resources.iter().rev() {
                let _ = self.delete_resource(resource);
            }

            {
                let mut accounts = self.cloudformation_state.write();
                let state = accounts.get_or_create(&self.account_id);
                state.stacks.remove(&stack_name);

                crate::service::record_stack_status_event(
                    state,
                    &stack_id,
                    &stack_name,
                    "AWS::CloudFormation::Stack",
                    "DELETE_IN_PROGRESS",
                );
                crate::service::record_stack_status_event(
                    state,
                    &stack_id,
                    &stack_name,
                    "AWS::CloudFormation::Stack",
                    "DELETE_COMPLETE",
                );
            }
        }

        Ok(())
    }

    fn get_att_cloudformation_stack(&self, physical_id: &str, attribute: &str) -> Option<String> {
        let accounts = self.cloudformation_state.read();
        let state = accounts.get(&self.account_id)?;
        let stack = state.stacks.values().find(|s| s.stack_id == physical_id)?;

        if let Some(output_key) = attribute.strip_prefix("Outputs.") {
            return stack
                .outputs
                .iter()
                .find(|o| o.key == output_key)
                .map(|o| o.value.clone());
        }

        match attribute {
            "Outputs" => Some(
                serde_json::to_string(
                    &stack
                        .outputs
                        .iter()
                        .map(|o| (o.key.clone(), o.value.clone()))
                        .collect::<std::collections::BTreeMap<String, String>>(),
                )
                .unwrap_or_default(),
            ),
            _ => None,
        }
    }

    fn fetch_template_from_url(&self, url: &str) -> Result<String, String> {
        if let Some(rest) = url.strip_prefix("s3://") {
            let parts: Vec<&str> = rest.splitn(2, '/').collect();
            if parts.len() != 2 {
                return Err("Invalid s3:// URL".to_string());
            }
            return self.fetch_s3_template(parts[0], parts[1]);
        }

        if let Some(rest) = url.strip_prefix("https://s3.amazonaws.com/") {
            let parts: Vec<&str> = rest.splitn(2, '/').collect();
            if parts.len() != 2 {
                return Err("Invalid S3 HTTPS URL".to_string());
            }
            return self.fetch_s3_template(parts[0], parts[1]);
        }

        if let Some(host_rest) = url.strip_prefix("https://") {
            if let Some(slash_pos) = host_rest.find('/') {
                let host = &host_rest[..slash_pos];
                let key = &host_rest[slash_pos + 1..];
                if let Some(bucket) = host.strip_suffix(".s3.amazonaws.com") {
                    return self.fetch_s3_template(bucket, key);
                }
                if host.contains(".s3.") && host.ends_with(".amazonaws.com") {
                    let bucket = host.split(".s3.").next().unwrap_or("");
                    if !bucket.is_empty() {
                        return self.fetch_s3_template(bucket, key);
                    }
                }
            }
        }

        Err(format!("Unsupported TemplateURL: {url}"))
    }

    fn fetch_s3_template(&self, bucket: &str, key: &str) -> Result<String, String> {
        let mut s3_accounts = self.s3_state.write();
        let s3_state = s3_accounts.get_or_create(&self.account_id);
        let bucket_obj = s3_state
            .buckets
            .get(bucket)
            .ok_or_else(|| format!("S3 bucket not found: {bucket}"))?;
        let obj = bucket_obj
            .objects
            .get(key)
            .ok_or_else(|| format!("S3 object not found: {bucket}/{key}"))?;
        let bytes = s3_state
            .read_body(&obj.body)
            .map_err(|e| format!("Failed to read S3 object body: {e}"))?;
        String::from_utf8(bytes.to_vec()).map_err(|e| format!("S3 object is not valid UTF-8: {e}"))
    }
}

/// Implements the CloudFormation `GenerateSecretString` shape on
/// `AWS::SecretsManager::Secret`. Produces the plaintext payload that
/// will become the AWSCURRENT version of the secret.
///
/// When `SecretStringTemplate` is set together with `GenerateStringKey`,
/// the generated password is inserted under `GenerateStringKey` in the
/// JSON template (this is the standard "DB credential" pattern).
/// Without those two, the bare generated password is returned.
fn generate_secret_string_payload(gen: &serde_json::Value) -> Result<String, String> {
    let length = gen
        .get("PasswordLength")
        .and_then(|v| v.as_i64())
        .unwrap_or(32) as usize;
    let exclude_lowercase = gen
        .get("ExcludeLowercase")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let exclude_uppercase = gen
        .get("ExcludeUppercase")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let exclude_numbers = gen
        .get("ExcludeNumbers")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let exclude_punctuation = gen
        .get("ExcludePunctuation")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let include_space = gen
        .get("IncludeSpace")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let exclude_chars = gen
        .get("ExcludeCharacters")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();

    let lowercase = "abcdefghijklmnopqrstuvwxyz";
    let uppercase = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    let digits = "0123456789";
    let punctuation = "!\"#$%&'()*+,-./:;<=>?@[\\]^_`{|}~";

    let mut pool = String::new();
    if !exclude_lowercase {
        pool.extend(lowercase.chars().filter(|c| !exclude_chars.contains(*c)));
    }
    if !exclude_uppercase {
        pool.extend(uppercase.chars().filter(|c| !exclude_chars.contains(*c)));
    }
    if !exclude_numbers {
        pool.extend(digits.chars().filter(|c| !exclude_chars.contains(*c)));
    }
    if !exclude_punctuation {
        pool.extend(punctuation.chars().filter(|c| !exclude_chars.contains(*c)));
    }
    if include_space && !exclude_chars.contains(' ') {
        pool.push(' ');
    }
    if pool.is_empty() {
        return Err("GenerateSecretString character pool is empty".to_string());
    }

    let pool_chars: Vec<char> = pool.chars().collect();
    let mut password = String::with_capacity(length);
    let mut counter: u64 = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);
    while password.len() < length {
        // Splitmix64 — deterministic, no_std-friendly, good enough for
        // fake-cloud password generation. Real entropy isn't a goal here.
        counter = counter.wrapping_add(0x9E3779B97F4A7C15);
        let mut z = counter;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D049BB133111EB);
        z ^= z >> 31;
        let idx = (z as usize) % pool_chars.len();
        password.push(pool_chars[idx]);
    }

    let template = gen.get("SecretStringTemplate").and_then(|v| v.as_str());
    let key = gen.get("GenerateStringKey").and_then(|v| v.as_str());
    match (template, key) {
        (Some(tmpl), Some(k)) => {
            let mut value: serde_json::Value = serde_json::from_str(tmpl)
                .map_err(|e| format!("SecretStringTemplate is not valid JSON: {e}"))?;
            if let Some(obj) = value.as_object_mut() {
                obj.insert(k.to_string(), serde_json::Value::String(password));
                Ok(value.to_string())
            } else {
                Err("SecretStringTemplate must be a JSON object".to_string())
            }
        }
        _ => Ok(password),
    }
}

fn parse_ses_receipt_action(value: &serde_json::Value) -> Option<SesReceiptAction> {
    let obj = value.as_object()?;
    if let Some(s3) = obj.get("S3Action").and_then(|v| v.as_object()) {
        let bucket_name = s3.get("BucketName").and_then(|v| v.as_str())?.to_string();
        return Some(SesReceiptAction::S3 {
            bucket_name,
            object_key_prefix: s3
                .get("ObjectKeyPrefix")
                .and_then(|v| v.as_str())
                .map(String::from),
            topic_arn: s3
                .get("TopicArn")
                .and_then(|v| v.as_str())
                .map(String::from),
            kms_key_arn: s3
                .get("KmsKeyArn")
                .and_then(|v| v.as_str())
                .map(String::from),
        });
    }
    if let Some(sns) = obj.get("SNSAction").and_then(|v| v.as_object()) {
        return Some(SesReceiptAction::Sns {
            topic_arn: sns.get("TopicArn").and_then(|v| v.as_str())?.to_string(),
            encoding: sns
                .get("Encoding")
                .and_then(|v| v.as_str())
                .map(String::from),
        });
    }
    if let Some(la) = obj.get("LambdaAction").and_then(|v| v.as_object()) {
        return Some(SesReceiptAction::Lambda {
            function_arn: la.get("FunctionArn").and_then(|v| v.as_str())?.to_string(),
            invocation_type: la
                .get("InvocationType")
                .and_then(|v| v.as_str())
                .map(String::from),
            topic_arn: la
                .get("TopicArn")
                .and_then(|v| v.as_str())
                .map(String::from),
        });
    }
    if let Some(b) = obj.get("BounceAction").and_then(|v| v.as_object()) {
        return Some(SesReceiptAction::Bounce {
            smtp_reply_code: b
                .get("SmtpReplyCode")
                .and_then(|v| v.as_str())
                .unwrap_or("550")
                .to_string(),
            message: b
                .get("Message")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string(),
            sender: b
                .get("Sender")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string(),
            status_code: b
                .get("StatusCode")
                .and_then(|v| v.as_str())
                .map(String::from),
            topic_arn: b.get("TopicArn").and_then(|v| v.as_str()).map(String::from),
        });
    }
    if let Some(ah) = obj.get("AddHeaderAction").and_then(|v| v.as_object()) {
        return Some(SesReceiptAction::AddHeader {
            header_name: ah.get("HeaderName").and_then(|v| v.as_str())?.to_string(),
            header_value: ah.get("HeaderValue").and_then(|v| v.as_str())?.to_string(),
        });
    }
    if let Some(s) = obj.get("StopAction").and_then(|v| v.as_object()) {
        return Some(SesReceiptAction::Stop {
            scope: s
                .get("Scope")
                .and_then(|v| v.as_str())
                .unwrap_or("RuleSet")
                .to_string(),
            topic_arn: s.get("TopicArn").and_then(|v| v.as_str()).map(String::from),
        });
    }
    None
}

/// Generate an N-character alphanumeric id for API Gateway v2 resources.
/// AWS HTTP API ids are 10 chars; routes/integrations/etc are similarly
/// short. This mirrors the runtime crate's id shape.
fn make_apigwv2_id(n: usize) -> String {
    let s = uuid::Uuid::new_v4().simple().to_string();
    s[..n.min(s.len())].to_string()
}

/// Lowercase the first letter of each key in a JSON object, recursively.
/// CloudFormation property names are PascalCase (`BurstLimit`,
/// `RateLimit`); the runtime API Gateway service stores values keyed in
/// camelCase (`burstLimit`, `rateLimit`). Used at the CFN/service
/// boundary so JSON pulled from the template can flow into the runtime
/// state without renaming each leaf by hand.
/// Coerce a CFN-resolved Number/String parameter into i64. CFN
/// parameters surface here as `Value::String` after `Ref` substitution,
/// even when the parameter `Type` is `Number`, so we need both paths.
fn cfn_as_i64(v: &serde_json::Value) -> Option<i64> {
    if let Some(n) = v.as_i64() {
        return Some(n);
    }
    v.as_str().and_then(|s| s.parse::<i64>().ok())
}

fn lowercase_first_keys(value: serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::Object(map) => {
            let mut out = serde_json::Map::new();
            for (k, v) in map {
                let new_key = if let Some(first) = k.chars().next() {
                    let mut s = String::with_capacity(k.len());
                    s.extend(first.to_lowercase());
                    s.push_str(&k[first.len_utf8()..]);
                    s
                } else {
                    k
                };
                out.insert(new_key, lowercase_first_keys(v));
            }
            serde_json::Value::Object(out)
        }
        serde_json::Value::Array(arr) => {
            serde_json::Value::Array(arr.into_iter().map(lowercase_first_keys).collect())
        }
        other => other,
    }
}

/// Synthesize the per-domain DNS validation record list for a
/// CFN-provisioned cert. Mirrors the runtime ACM path: each name gets
/// a SUCCESS record (since CFN-issued certs are auto-validated above)
/// and an `_amzn-validations.<domain>.` resource record so callers
/// that read DescribeCertificate see the same shape they'd expect
/// from a real ACM-issued cert.
fn synth_acm_domain_validation(
    domain_name: &str,
    sans: &[String],
    validation_method: &str,
) -> Vec<AcmDomainValidation> {
    let mut all = vec![domain_name.to_string()];
    for s in sans {
        if !all.contains(s) {
            all.push(s.clone());
        }
    }
    all.into_iter()
        .map(|name| AcmDomainValidation {
            domain_name: name.clone(),
            validation_status: "SUCCESS".to_string(),
            validation_method: validation_method.to_string(),
            resource_record_name: Some(format!("_amzn-validations.{name}.")),
            resource_record_type: Some("CNAME".to_string()),
            resource_record_value: Some(format!("{}.acm-validations.aws.", Uuid::new_v4())),
        })
        .collect()
}

/// Convert CFN `Tags` array into the ACM crate's tag map form.
fn parse_acm_tags(value: Option<&serde_json::Value>) -> BTreeMap<String, String> {
    let mut out = BTreeMap::new();
    if let Some(arr) = value.and_then(|v| v.as_array()) {
        for t in arr {
            if let (Some(k), Some(v)) = (
                t.get("Key").and_then(|v| v.as_str()),
                t.get("Value").and_then(|v| v.as_str()),
            ) {
                out.insert(k.to_string(), v.to_string());
            }
        }
    }
    out
}

/// Convert CFN `Tags` array into the ECS `TagEntry` form.
fn parse_ecs_tags(value: Option<&serde_json::Value>) -> Vec<EcsTagEntry> {
    let Some(arr) = value.and_then(|v| v.as_array()) else {
        return Vec::new();
    };
    arr.iter()
        .filter_map(|t| {
            let key = t.get("Key").and_then(|v| v.as_str())?.to_string();
            let value = t.get("Value").and_then(|v| v.as_str())?.to_string();
            Some(EcsTagEntry { key, value })
        })
        .collect()
}

/// Strip the cluster ARN prefix when CFN passes a Ref-resolved name or
/// a GetAtt-resolved ARN; ECS internal state keys clusters by name.
fn parse_ecs_cluster_name(input: &str) -> String {
    if let Some(after) = input.split(":cluster/").nth(1) {
        return after.to_string();
    }
    input.to_string()
}

/// Pull `(family, revision)` out of a task definition ARN tail like
/// `task-definition/web:3`. Returns `(family, revision)` or
/// `(input, 1)` for unrecognised shapes.
fn parse_td_arn(input: &str) -> (String, i32) {
    let suffix = input.rsplit('/').next().unwrap_or(input);
    if let Some((family, rev)) = suffix.split_once(':') {
        if let Ok(revision) = rev.parse::<i32>() {
            return (family.to_string(), revision);
        }
    }
    (input.to_string(), 1)
}

/// Pull `(cluster, service)` out of a service ARN like
/// `arn:aws:ecs:us-east-1:000000000000:service/<cluster>/<service>`.
fn parse_service_arn(input: &str) -> Option<(String, String)> {
    let after = input.split(":service/").nth(1)?;
    let mut parts = after.splitn(2, '/');
    let cluster = parts.next()?.to_string();
    let service = parts.next()?.to_string();
    Some((cluster, service))
}

/// Parse CFN-shape Tags array into the RDS crate's tag form.
fn parse_rds_tags(value: Option<&serde_json::Value>) -> Vec<RdsTag> {
    let Some(arr) = value.and_then(|v| v.as_array()) else {
        return Vec::new();
    };
    arr.iter()
        .filter_map(|t| {
            let key = t.get("Key").and_then(|v| v.as_str())?.to_string();
            let value = t.get("Value").and_then(|v| v.as_str())?.to_string();
            Some(RdsTag { key, value })
        })
        .collect()
}

/// Lazy-create an entry in the RDS `extras` bucket so the provisioner
/// doesn't have to re-implement the `BTreeMap::entry` boilerplate per
/// resource type.
fn rds_extras_mut<'a>(
    state: &'a mut fakecloud_rds::RdsState,
    category: &str,
) -> &'a mut BTreeMap<String, serde_json::Value> {
    state.extras.entry(category.to_string()).or_default()
}

/// Parse a JSON array-of-strings property. Returns empty Vec when the
/// value is missing or shaped wrong; matches the tolerant input handling
/// used by the runtime Cognito service.
fn parse_cognito_string_array(value: Option<&serde_json::Value>) -> Vec<String> {
    value
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(|s| s.to_string()))
                .collect()
        })
        .unwrap_or_default()
}

fn parse_cognito_password_policy(value: Option<&serde_json::Value>) -> PasswordPolicy {
    let Some(inner) = value
        .and_then(|v| v.get("PasswordPolicy"))
        .and_then(|v| v.as_object())
    else {
        return PasswordPolicy::default();
    };
    let mut p = PasswordPolicy::default();
    if let Some(n) = inner.get("MinimumLength").and_then(|v| v.as_i64()) {
        p.minimum_length = n;
    }
    if let Some(b) = inner.get("RequireUppercase").and_then(|v| v.as_bool()) {
        p.require_uppercase = b;
    }
    if let Some(b) = inner.get("RequireLowercase").and_then(|v| v.as_bool()) {
        p.require_lowercase = b;
    }
    if let Some(b) = inner.get("RequireNumbers").and_then(|v| v.as_bool()) {
        p.require_numbers = b;
    }
    if let Some(b) = inner.get("RequireSymbols").and_then(|v| v.as_bool()) {
        p.require_symbols = b;
    }
    if let Some(n) = inner
        .get("TemporaryPasswordValidityDays")
        .and_then(|v| v.as_i64())
    {
        p.temporary_password_validity_days = n;
    }
    p
}

fn parse_cognito_schema_attribute(value: &serde_json::Value) -> Option<SchemaAttribute> {
    let name = value.get("Name").and_then(|v| v.as_str())?.to_string();
    Some(SchemaAttribute {
        name,
        attribute_data_type: value
            .get("AttributeDataType")
            .and_then(|v| v.as_str())
            .unwrap_or("String")
            .to_string(),
        developer_only_attribute: value
            .get("DeveloperOnlyAttribute")
            .and_then(|v| v.as_bool())
            .unwrap_or(false),
        mutable: value
            .get("Mutable")
            .and_then(|v| v.as_bool())
            .unwrap_or(true),
        required: value
            .get("Required")
            .and_then(|v| v.as_bool())
            .unwrap_or(false),
        string_attribute_constraints: None,
        number_attribute_constraints: None,
    })
}

fn parse_cognito_tags(value: Option<&serde_json::Value>) -> BTreeMap<String, String> {
    let mut out = BTreeMap::new();
    if let Some(obj) = value.and_then(|v| v.as_object()) {
        for (k, v) in obj {
            if let Some(s) = v.as_str() {
                out.insert(k.clone(), s.to_string());
            }
        }
    }
    out
}

fn parse_cognito_email_configuration(
    value: Option<&serde_json::Value>,
) -> Option<EmailConfiguration> {
    let inner = value?.as_object()?;
    Some(EmailConfiguration {
        source_arn: inner
            .get("SourceArn")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string()),
        reply_to_email_address: inner
            .get("ReplyToEmailAddress")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string()),
        email_sending_account: inner
            .get("EmailSendingAccount")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string()),
        from_email_address: inner
            .get("From")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string()),
        configuration_set: inner
            .get("ConfigurationSet")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string()),
    })
}

fn parse_cognito_sms_configuration(value: Option<&serde_json::Value>) -> Option<SmsConfiguration> {
    let inner = value?.as_object()?;
    Some(SmsConfiguration {
        sns_caller_arn: inner
            .get("SnsCallerArn")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string()),
        external_id: inner
            .get("ExternalId")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string()),
        sns_region: inner
            .get("SnsRegion")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string()),
    })
}

fn parse_cognito_admin_create_user_config(
    value: Option<&serde_json::Value>,
) -> Option<AdminCreateUserConfig> {
    let inner = value?.as_object()?;
    Some(AdminCreateUserConfig {
        allow_admin_create_user_only: inner
            .get("AllowAdminCreateUserOnly")
            .and_then(|v| v.as_bool()),
        invite_message_template: None,
        unused_account_validity_days: inner
            .get("UnusedAccountValidityDays")
            .and_then(|v| v.as_i64()),
    })
}

fn parse_cognito_account_recovery(
    value: Option<&serde_json::Value>,
) -> Option<AccountRecoverySetting> {
    let arr = value?.get("RecoveryMechanisms")?.as_array()?;
    Some(AccountRecoverySetting {
        recovery_mechanisms: arr
            .iter()
            .filter_map(|m| {
                let name = m.get("Name").and_then(|v| v.as_str())?.to_string();
                let priority = m.get("Priority").and_then(|v| v.as_i64()).unwrap_or(1);
                Some(RecoveryOption { name, priority })
            })
            .collect(),
    })
}

fn parse_firehose_s3_destination(value: &serde_json::Value) -> Result<S3Destination, String> {
    let role_arn = value
        .get("RoleARN")
        .and_then(|v| v.as_str())
        .ok_or("S3 destination requires RoleARN")?
        .to_string();
    let bucket_arn = value
        .get("BucketARN")
        .and_then(|v| v.as_str())
        .ok_or("S3 destination requires BucketARN")?
        .to_string();
    let prefix = value
        .get("Prefix")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());
    let error_output_prefix = value
        .get("ErrorOutputPrefix")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());
    let mut buffering_size_mb = None;
    let mut buffering_interval_seconds = None;
    if let Some(hints) = value.get("BufferingHints") {
        buffering_size_mb = hints.get("SizeInMBs").and_then(|v| v.as_i64());
        buffering_interval_seconds = hints.get("IntervalInSeconds").and_then(|v| v.as_i64());
    }
    let compression_format = value
        .get("CompressionFormat")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());

    Ok(S3Destination {
        destination_id: "destination-1".to_string(),
        role_arn,
        bucket_arn,
        prefix,
        error_output_prefix,
        buffering_size_mb,
        buffering_interval_seconds,
        compression_format,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use parking_lot::RwLock;

    fn make_provisioner() -> ResourceProvisioner {
        ResourceProvisioner {
            sqs_state: Arc::new(RwLock::new(
                fakecloud_core::multi_account::MultiAccountState::new(
                    "123456789012",
                    "us-east-1",
                    "http://localhost:4566",
                ),
            )),
            sns_state: Arc::new(RwLock::new(
                fakecloud_core::multi_account::MultiAccountState::new(
                    "123456789012",
                    "us-east-1",
                    "http://localhost:4566",
                ),
            )),
            ssm_state: Arc::new(RwLock::new(
                fakecloud_core::multi_account::MultiAccountState::new(
                    "123456789012",
                    "us-east-1",
                    "http://localhost:4566",
                ),
            )),
            iam_state: Arc::new(RwLock::new(
                fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", "http://localhost:4566"),
            )),
            s3_state: Arc::new(RwLock::new(fakecloud_core::multi_account::MultiAccountState::new(
                "123456789012",
                "us-east-1", "",
            ))),
            eventbridge_state: Arc::new(RwLock::new(
                fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
            )),
            dynamodb_state: Arc::new(RwLock::new(fakecloud_core::multi_account::MultiAccountState::new(
                "123456789012",
                "us-east-1", "",
            ))),
            logs_state: Arc::new(RwLock::new(
                fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
            )),
            lambda_state: Arc::new(RwLock::new(
                fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
            )),
            secretsmanager_state: Arc::new(RwLock::new(
                fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
            )),
            kinesis_state: Arc::new(RwLock::new(
                fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
            )),
            kms_state: Arc::new(RwLock::new(
                fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
            )),
            ecr_state: Arc::new(RwLock::new(
                fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
            )),
            cloudwatch_state: Arc::new(RwLock::new(fakecloud_cloudwatch::CloudWatchAccounts::new())),
            elbv2_state: Arc::new(RwLock::new(fakecloud_elbv2::Elbv2Accounts::new())),
            organizations_state: Arc::new(RwLock::new(None)),
            cognito_state: Arc::new(RwLock::new(
                fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
            )),
            rds_state: Arc::new(RwLock::new(
                fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
            )),
            ecs_state: Arc::new(RwLock::new(
                fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
            )),
            acm_state: Arc::new(RwLock::new(fakecloud_acm::AcmAccounts::new())),
            elasticache_state: Arc::new(RwLock::new(
                fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
            )),
            route53_state: Arc::new(RwLock::new(fakecloud_route53::Route53Accounts::new())),
            cloudfront_state: Arc::new(RwLock::new(
                fakecloud_cloudfront::CloudFrontAccounts::new(),
            )),
            cloudformation_state: Arc::new(RwLock::new(
                fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
            )),
            stepfunctions_state: Arc::new(RwLock::new(
                fakecloud_core::multi_account::MultiAccountState::new(
                    "123456789012",
                    "us-east-1",
                    "",
                ),
            )),
            wafv2_state: Arc::new(RwLock::new(fakecloud_wafv2::Wafv2Accounts::default())),
            apigateway_state: Arc::new(RwLock::new(
                fakecloud_core::multi_account::MultiAccountState::new(
                    "123456789012",
                    "us-east-1",
                    "",
                ),
            )),
            apigatewayv2_state: Arc::new(RwLock::new(
                fakecloud_core::multi_account::MultiAccountState::new(
                    "123456789012",
                    "us-east-1",
                    "",
                ),
            )),
            ses_state: Arc::new(RwLock::new(
                fakecloud_core::multi_account::MultiAccountState::new(
                    "123456789012",
                    "us-east-1",
                    "",
                ),
            )),
            app_autoscaling_state: Arc::new(parking_lot::RwLock::new(
                fakecloud_application_autoscaling::ApplicationAutoScalingAccounts::new(),
            )),
            athena_state: Arc::new(parking_lot::RwLock::new(
                fakecloud_athena::AthenaAccounts::new(),
            )),
            firehose_state: Arc::new(parking_lot::RwLock::new(
                fakecloud_firehose::FirehoseAccounts::new(),
            )),
            glue_state: Arc::new(parking_lot::RwLock::new(
                fakecloud_glue::GlueAccounts::new(),
            )),
            delivery: Arc::new(DeliveryBus::new()),
            account_id: "123456789012".to_string(),
            region: "us-east-1".to_string(),
            stack_id: "arn:aws:cloudformation:us-east-1:123456789012:stack/test/00000000-0000-0000-0000-000000000000".to_string(),
        }
    }

    fn make_resource(
        resource_type: &str,
        logical_id: &str,
        props: serde_json::Value,
    ) -> ResourceDefinition {
        ResourceDefinition {
            logical_id: logical_id.to_string(),
            resource_type: resource_type.to_string(),
            properties: props,
        }
    }

    #[test]
    fn sns_subscription_rejects_nonexistent_topic() {
        let prov = make_provisioner();
        let resource = make_resource(
            "AWS::SNS::Subscription",
            "MySub",
            serde_json::json!({
                "TopicArn": "arn:aws:sns:us-east-1:123456789012:NonExistent",
                "Protocol": "sqs",
                "Endpoint": "arn:aws:sqs:us-east-1:123456789012:my-queue"
            }),
        );
        let result = prov.create_resource(&resource);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("does not exist"));
    }

    #[test]
    fn sns_subscription_succeeds_when_topic_exists() {
        let prov = make_provisioner();
        // First create the topic
        let topic = make_resource(
            "AWS::SNS::Topic",
            "MyTopic",
            serde_json::json!({ "TopicName": "my-topic" }),
        );
        let topic_result = prov.create_resource(&topic);
        assert!(topic_result.is_ok());
        let topic_arn = topic_result.unwrap().physical_id;

        // Now create subscription referencing that topic
        let sub = make_resource(
            "AWS::SNS::Subscription",
            "MySub",
            serde_json::json!({
                "TopicArn": topic_arn,
                "Protocol": "sqs",
                "Endpoint": "arn:aws:sqs:us-east-1:123456789012:my-queue"
            }),
        );
        let result = prov.create_resource(&sub);
        assert!(result.is_ok());
    }

    #[test]
    fn eventbridge_rule_arn_default_bus_omits_bus_name() {
        let prov = make_provisioner();
        let resource = make_resource(
            "AWS::Events::Rule",
            "MyRule",
            serde_json::json!({
                "Name": "my-rule",
                "ScheduleExpression": "rate(1 hour)"
            }),
        );
        let result = prov.create_resource(&resource).unwrap();
        // For default bus, ARN should be rule/<name> without /default/
        assert_eq!(
            result.physical_id,
            "arn:aws:events:us-east-1:123456789012:rule/my-rule"
        );
        assert!(!result.physical_id.contains("rule/default/"));
    }

    #[test]
    fn eventbridge_rule_arn_custom_bus_includes_bus_name() {
        let prov = make_provisioner();
        // Create a custom bus first
        {
            let mut eb_accounts = prov.eventbridge_state.write();
            let state = eb_accounts.default_mut();
            state.buses.insert(
                "custom-bus".to_string(),
                fakecloud_eventbridge::EventBus {
                    name: "custom-bus".to_string(),
                    arn: "arn:aws:events:us-east-1:123456789012:event-bus/custom-bus".to_string(),
                    policy: None,
                    creation_time: Utc::now(),
                    last_modified_time: Utc::now(),
                    description: None,
                    kms_key_identifier: None,
                    dead_letter_config: None,
                    tags: std::collections::BTreeMap::new(),
                },
            );
        }
        let resource = make_resource(
            "AWS::Events::Rule",
            "MyRule",
            serde_json::json!({
                "Name": "my-rule",
                "EventBusName": "custom-bus",
                "ScheduleExpression": "rate(1 hour)"
            }),
        );
        let result = prov.create_resource(&resource).unwrap();
        assert_eq!(
            result.physical_id,
            "arn:aws:events:us-east-1:123456789012:rule/custom-bus/my-rule"
        );
    }

    #[test]
    fn eventbridge_rule_rejects_nonexistent_bus() {
        let prov = make_provisioner();
        let resource = make_resource(
            "AWS::Events::Rule",
            "MyRule",
            serde_json::json!({
                "Name": "my-rule",
                "EventBusName": "nonexistent-bus",
                "ScheduleExpression": "rate(1 hour)"
            }),
        );
        let result = prov.create_resource(&resource);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("does not exist"));
    }

    #[test]
    fn custom_resource_requires_service_token() {
        let prov = make_provisioner();
        let resource = make_resource(
            "Custom::MyResource",
            "MyCustom",
            serde_json::json!({
                "Foo": "bar"
            }),
        );
        let result = prov.create_resource(&resource);
        assert!(result.is_err());
        assert!(
            result.unwrap_err().contains("ServiceToken"),
            "Should require ServiceToken property"
        );
    }

    #[test]
    fn custom_resource_succeeds_without_lambda_delivery() {
        // When no Lambda delivery is configured, custom resource creation
        // should still succeed (the invocation is silently skipped).
        let prov = make_provisioner();
        let resource = make_resource(
            "Custom::MyResource",
            "MyCustom",
            serde_json::json!({
                "ServiceToken": "arn:aws:lambda:us-east-1:123456789012:function:my-func",
                "Foo": "bar"
            }),
        );
        let result = prov.create_resource(&resource);
        assert!(result.is_ok());
        let sr = result.unwrap();
        assert_eq!(sr.logical_id, "MyCustom");
        assert_eq!(sr.resource_type, "Custom::MyResource");
        assert!(sr.physical_id.starts_with("MyCustom-"));
    }

    #[test]
    fn cloudformation_custom_resource_type_succeeds() {
        let prov = make_provisioner();
        let resource = make_resource(
            "AWS::CloudFormation::CustomResource",
            "MyCustom2",
            serde_json::json!({
                "ServiceToken": "arn:aws:lambda:us-east-1:123456789012:function:my-func",
                "Key": "value"
            }),
        );
        let result = prov.create_resource(&resource);
        assert!(result.is_ok());
        let sr = result.unwrap();
        assert_eq!(sr.resource_type, "AWS::CloudFormation::CustomResource");
    }

    // ── Resource create/delete lifecycle tests ──

    #[test]
    fn sqs_queue_create_and_delete() {
        let prov = make_provisioner();
        let res = make_resource(
            "AWS::SQS::Queue",
            "MyQ",
            serde_json::json!({"QueueName": "my-q"}),
        );
        let sr = prov.create_resource(&res).unwrap();
        assert!(sr.physical_id.contains("my-q"));
        assert_eq!(sr.resource_type, "AWS::SQS::Queue");
        prov.delete_resource(&sr).unwrap();
    }

    #[test]
    fn sqs_queue_fifo_with_suffix() {
        let prov = make_provisioner();
        let res = make_resource(
            "AWS::SQS::Queue",
            "FifoQ",
            serde_json::json!({"QueueName": "my-fifo.fifo", "FifoQueue": true}),
        );
        let sr = prov.create_resource(&res).unwrap();
        assert!(sr.physical_id.contains(".fifo"));
    }

    #[test]
    fn sns_topic_create_and_delete() {
        let prov = make_provisioner();
        let res = make_resource(
            "AWS::SNS::Topic",
            "MyTopic",
            serde_json::json!({"TopicName": "t1"}),
        );
        let sr = prov.create_resource(&res).unwrap();
        assert!(sr.physical_id.contains("t1"));
        prov.delete_resource(&sr).unwrap();
    }

    #[test]
    fn ssm_parameter_create_and_delete() {
        let prov = make_provisioner();
        let res = make_resource(
            "AWS::SSM::Parameter",
            "MyParam",
            serde_json::json!({
                "Name": "/my/param",
                "Type": "String",
                "Value": "v1"
            }),
        );
        let sr = prov.create_resource(&res).unwrap();
        assert_eq!(sr.physical_id, "/my/param");
        prov.delete_resource(&sr).unwrap();
    }

    #[test]
    fn iam_role_create_and_delete() {
        let prov = make_provisioner();
        let res = make_resource(
            "AWS::IAM::Role",
            "MyRole",
            serde_json::json!({
                "RoleName": "my-role",
                "AssumeRolePolicyDocument": {"Version": "2012-10-17", "Statement": []}
            }),
        );
        let sr = prov.create_resource(&res).unwrap();
        assert!(sr.physical_id.contains("my-role"));
        prov.delete_resource(&sr).unwrap();
    }

    #[test]
    fn iam_policy_create_and_delete() {
        let prov = make_provisioner();
        let res = make_resource(
            "AWS::IAM::Policy",
            "MyPolicy",
            serde_json::json!({
                "PolicyName": "my-policy",
                "PolicyDocument": {"Version": "2012-10-17", "Statement": []}
            }),
        );
        let sr = prov.create_resource(&res).unwrap();
        assert!(sr.physical_id.contains("my-policy"));
        prov.delete_resource(&sr).unwrap();
    }

    #[test]
    fn s3_bucket_create_and_delete() {
        let prov = make_provisioner();
        let res = make_resource(
            "AWS::S3::Bucket",
            "MyBucket",
            serde_json::json!({"BucketName": "my-bucket"}),
        );
        let sr = prov.create_resource(&res).unwrap();
        assert_eq!(sr.physical_id, "my-bucket");
        prov.delete_resource(&sr).unwrap();
    }

    #[test]
    fn dynamodb_table_create_and_delete() {
        let prov = make_provisioner();
        let res = make_resource(
            "AWS::DynamoDB::Table",
            "MyTable",
            serde_json::json!({
                "TableName": "my-table",
                "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
                "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
                "BillingMode": "PAY_PER_REQUEST"
            }),
        );
        let sr = prov.create_resource(&res).unwrap();
        assert!(sr.physical_id.contains("my-table"));
        prov.delete_resource(&sr).unwrap();
    }

    #[test]
    fn log_group_create_and_delete() {
        let prov = make_provisioner();
        let res = make_resource(
            "AWS::Logs::LogGroup",
            "MyLogs",
            serde_json::json!({"LogGroupName": "/app/logs"}),
        );
        let sr = prov.create_resource(&res).unwrap();
        assert!(sr.physical_id.contains("/app/logs"));
        prov.delete_resource(&sr).unwrap();
    }

    #[test]
    fn lambda_function_create_and_delete() {
        let prov = make_provisioner();
        let res = make_resource(
            "AWS::Lambda::Function",
            "MyFn",
            serde_json::json!({
                "FunctionName": "my-fn",
                "Runtime": "nodejs20.x",
                "Role": "arn:aws:iam::123456789012:role/lambda-role",
                "Handler": "index.handler",
                "MemorySize": 256,
                "Timeout": 10,
                "Environment": {"Variables": {"FOO": "bar"}}
            }),
        );
        let sr = prov.create_resource(&res).unwrap();
        assert_eq!(sr.physical_id, "my-fn");
        assert_eq!(
            sr.attributes.get("Arn").map(String::as_str),
            Some("arn:aws:lambda:us-east-1:123456789012:function:my-fn")
        );
        // Verify it landed in lambda state
        {
            let lam = prov.lambda_state.read();
            let st = lam.get("123456789012").unwrap();
            let f = st.functions.get("my-fn").unwrap();
            assert_eq!(f.runtime, "nodejs20.x");
            assert_eq!(f.memory_size, 256);
            assert_eq!(f.environment.get("FOO").unwrap(), "bar");
        }
        prov.delete_resource(&sr).unwrap();
        let lam = prov.lambda_state.read();
        let st = lam.get("123456789012").unwrap();
        assert!(!st.functions.contains_key("my-fn"));
    }

    #[test]
    fn unsupported_resource_type_fails() {
        let prov = make_provisioner();
        let res = make_resource("AWS::NonExistent::Thing", "X", serde_json::json!({}));
        assert!(prov.create_resource(&res).is_err());
    }

    #[test]
    fn iam_role_with_inline_policies() {
        let prov = make_provisioner();
        let res = make_resource(
            "AWS::IAM::Role",
            "MyRole",
            serde_json::json!({
                "RoleName": "role-inline",
                "AssumeRolePolicyDocument": {"Version": "2012-10-17", "Statement": []},
                "Policies": [
                    {
                        "PolicyName": "inline-1",
                        "PolicyDocument": {"Version": "2012-10-17", "Statement": []}
                    }
                ]
            }),
        );
        let sr = prov.create_resource(&res).unwrap();
        assert!(sr.physical_id.contains("role-inline"));
    }

    #[test]
    fn sqs_queue_auto_name() {
        let prov = make_provisioner();
        let res = make_resource("AWS::SQS::Queue", "AutoQ", serde_json::json!({}));
        let sr = prov.create_resource(&res).unwrap();
        // Generated queue name should exist
        assert!(!sr.physical_id.is_empty());
    }

    #[test]
    fn sns_topic_auto_name() {
        let prov = make_provisioner();
        let res = make_resource("AWS::SNS::Topic", "AutoT", serde_json::json!({}));
        let sr = prov.create_resource(&res).unwrap();
        assert!(!sr.physical_id.is_empty());
    }

    // ── additional resource types ──

    #[test]
    fn unsupported_resource_type_errors() {
        let prov = make_provisioner();
        let res = make_resource("AWS::FooBar::Thing", "X", serde_json::json!({}));
        assert!(prov.create_resource(&res).is_err());
    }

    #[test]
    fn sqs_queue_with_redrive_policy() {
        let prov = make_provisioner();
        // Create DLQ first
        let dlq = make_resource(
            "AWS::SQS::Queue",
            "DLQ",
            serde_json::json!({"QueueName": "dlq1"}),
        );
        let dlq_resource = prov.create_resource(&dlq).unwrap();
        let _ = dlq_resource.physical_id;

        // Create source queue with redrive policy
        let src = make_resource(
            "AWS::SQS::Queue",
            "Src",
            serde_json::json!({
                "QueueName": "src1",
                "RedrivePolicy": {
                    "deadLetterTargetArn": "arn:aws:sqs:us-east-1:123456789012:dlq1",
                    "maxReceiveCount": 3
                }
            }),
        );
        let sr = prov.create_resource(&src).unwrap();
        assert!(!sr.physical_id.is_empty());
    }

    #[test]
    fn sns_topic_with_display_name() {
        let prov = make_provisioner();
        let res = make_resource(
            "AWS::SNS::Topic",
            "WithName",
            serde_json::json!({"TopicName": "named-topic", "DisplayName": "Named"}),
        );
        let sr = prov.create_resource(&res).unwrap();
        assert!(sr.physical_id.contains("named-topic"));
    }

    #[test]
    fn ssm_parameter_with_explicit_name() {
        let prov = make_provisioner();
        let res = make_resource(
            "AWS::SSM::Parameter",
            "Param",
            serde_json::json!({"Name": "/my/param", "Value": "v", "Type": "String"}),
        );
        let sr = prov.create_resource(&res).unwrap();
        assert!(sr.physical_id.contains("/my/param"));
    }

    #[test]
    fn ssm_parameter_missing_name_errors() {
        let prov = make_provisioner();
        let res = make_resource(
            "AWS::SSM::Parameter",
            "AutoP",
            serde_json::json!({"Value": "v", "Type": "String"}),
        );
        assert!(prov.create_resource(&res).is_err());
    }

    #[test]
    fn iam_managed_policy_auto_name() {
        let prov = make_provisioner();
        let res = make_resource(
            "AWS::IAM::Policy",
            "AutoPol",
            serde_json::json!({
                "PolicyName": "inline-pol",
                "PolicyDocument": {"Version": "2012-10-17", "Statement": []},
                "Users": []
            }),
        );
        let sr = prov.create_resource(&res).unwrap();
        assert!(!sr.physical_id.is_empty());
    }

    #[test]
    fn delete_resource_works_for_queue() {
        let prov = make_provisioner();
        let res = make_resource(
            "AWS::SQS::Queue",
            "ToDel",
            serde_json::json!({"QueueName": "todel"}),
        );
        let sr = prov.create_resource(&res).unwrap();
        assert!(prov.delete_resource(&sr).is_ok());
    }

    #[test]
    fn delete_resource_works_for_topic() {
        let prov = make_provisioner();
        let res = make_resource(
            "AWS::SNS::Topic",
            "DelT",
            serde_json::json!({"TopicName": "delt"}),
        );
        let sr = prov.create_resource(&res).unwrap();
        assert!(prov.delete_resource(&sr).is_ok());
    }

    #[test]
    fn application_autoscaling_scalable_target_round_trip() {
        let prov = make_provisioner();
        let res = make_resource(
            "AWS::ApplicationAutoScaling::ScalableTarget",
            "Target",
            serde_json::json!({
                "ServiceNamespace": "ecs",
                "ResourceId": "service/my-cluster/my-service",
                "ScalableDimension": "ecs:service:DesiredCount",
                "MinCapacity": 1,
                "MaxCapacity": 10,
                "RoleARN": "arn:aws:iam::123456789012:role/my-role",
            }),
        );
        let sr = prov.create_resource(&res).unwrap();
        assert_eq!(sr.physical_id, "service/my-cluster/my-service");
        assert!(sr.attributes.contains_key("ScalableTargetARN"));
        assert!(prov.delete_resource(&sr).is_ok());
    }

    #[test]
    fn application_autoscaling_scaling_policy_requires_target() {
        let prov = make_provisioner();
        let res = make_resource(
            "AWS::ApplicationAutoScaling::ScalingPolicy",
            "Policy",
            serde_json::json!({
                "PolicyName": "my-policy",
                "ServiceNamespace": "ecs",
                "ResourceId": "service/my-cluster/my-service",
                "ScalableDimension": "ecs:service:DesiredCount",
                "PolicyType": "TargetTrackingScaling",
                "TargetTrackingScalingPolicyConfiguration": {
                    "TargetValue": 50.0,
                    "PredefinedMetricSpecification": {
                        "PredefinedMetricType": "ECSServiceAverageCPUUtilization"
                    }
                },
            }),
        );
        // Should fail because scalable target does not exist
        assert!(prov.create_resource(&res).is_err());
    }

    #[test]
    fn application_autoscaling_scaling_policy_round_trip() {
        let prov = make_provisioner();
        let target = make_resource(
            "AWS::ApplicationAutoScaling::ScalableTarget",
            "Target",
            serde_json::json!({
                "ServiceNamespace": "ecs",
                "ResourceId": "service/my-cluster/my-service",
                "ScalableDimension": "ecs:service:DesiredCount",
                "MinCapacity": 1,
                "MaxCapacity": 10,
            }),
        );
        let sr = prov.create_resource(&target).unwrap();

        let policy = make_resource(
            "AWS::ApplicationAutoScaling::ScalingPolicy",
            "Policy",
            serde_json::json!({
                "PolicyName": "my-policy",
                "ServiceNamespace": "ecs",
                "ResourceId": "service/my-cluster/my-service",
                "ScalableDimension": "ecs:service:DesiredCount",
                "PolicyType": "TargetTrackingScaling",
                "TargetTrackingScalingPolicyConfiguration": {
                    "TargetValue": 50.0,
                    "PredefinedMetricSpecification": {
                        "PredefinedMetricType": "ECSServiceAverageCPUUtilization"
                    }
                },
            }),
        );
        let psr = prov.create_resource(&policy).unwrap();
        assert!(psr.physical_id.starts_with("arn:aws:autoscaling:"));
        assert!(prov.delete_resource(&psr).is_ok());
        assert!(prov.delete_resource(&sr).is_ok());
    }

    #[test]
    fn sqs_queue_with_fifo_suffix() {
        let prov = make_provisioner();
        let res = make_resource(
            "AWS::SQS::Queue",
            "Fifo",
            serde_json::json!({"QueueName": "fq.fifo", "FifoQueue": true}),
        );
        let sr = prov.create_resource(&res).unwrap();
        assert!(sr.physical_id.ends_with(".fifo"));
    }

    // ── get_att dispatch ──

    #[test]
    fn getatt_s3_bucket_arn_returns_arn() {
        let prov = make_provisioner();
        let bucket = make_resource(
            "AWS::S3::Bucket",
            "MyBucket",
            serde_json::json!({"BucketName": "my-bucket"}),
        );
        let sr = prov.create_resource(&bucket).unwrap();
        assert_eq!(
            prov.get_att(&sr, "Arn"),
            Some("arn:aws:s3:::my-bucket".to_string())
        );
    }

    #[test]
    fn getatt_s3_bucket_domain_name_returns_dns_name() {
        let prov = make_provisioner();
        let bucket = make_resource(
            "AWS::S3::Bucket",
            "MyBucket",
            serde_json::json!({"BucketName": "my-bucket"}),
        );
        let sr = prov.create_resource(&bucket).unwrap();
        assert_eq!(
            prov.get_att(&sr, "DomainName"),
            Some("my-bucket.s3.amazonaws.com".to_string())
        );
    }

    #[test]
    fn getatt_lambda_function_arn_returns_function_arn() {
        let prov = make_provisioner();
        // Lambda needs an existing IAM role to validate the function role.
        let role = make_resource(
            "AWS::IAM::Role",
            "MyRole",
            serde_json::json!({
                "RoleName": "my-role",
                "AssumeRolePolicyDocument": {"Version": "2012-10-17", "Statement": []}
            }),
        );
        let role_sr = prov.create_resource(&role).unwrap();
        let fn_res = make_resource(
            "AWS::Lambda::Function",
            "MyFn",
            serde_json::json!({
                "FunctionName": "my-fn",
                "Runtime": "python3.11",
                "Handler": "index.handler",
                "Role": role_sr.physical_id,
                "Code": {"ZipFile": "def handler(e,c): return e"}
            }),
        );
        let fn_sr = prov.create_resource(&fn_res).unwrap();
        let arn = prov.get_att(&fn_sr, "Arn").expect("Arn should resolve");
        assert!(arn.starts_with("arn:aws:lambda:"));
        assert!(arn.contains(":function:my-fn"));
    }

    #[test]
    fn getatt_iam_role_arn_returns_role_arn() {
        let prov = make_provisioner();
        let role = make_resource(
            "AWS::IAM::Role",
            "MyRole",
            serde_json::json!({
                "RoleName": "my-role",
                "AssumeRolePolicyDocument": {"Version": "2012-10-17", "Statement": []}
            }),
        );
        let sr = prov.create_resource(&role).unwrap();
        assert_eq!(
            prov.get_att(&sr, "Arn"),
            Some("arn:aws:iam::123456789012:role/my-role".to_string())
        );
        // RoleId is a real value generated at create time (FKIA-prefixed).
        let role_id = prov.get_att(&sr, "RoleId").expect("RoleId should resolve");
        assert!(role_id.starts_with("FKIA"));
    }

    #[test]
    fn getatt_unknown_attribute_returns_none() {
        let prov = make_provisioner();
        let bucket = make_resource(
            "AWS::S3::Bucket",
            "MyBucket",
            serde_json::json!({"BucketName": "my-bucket"}),
        );
        let sr = prov.create_resource(&bucket).unwrap();
        // Fall-back behaviour: unknown attribute on a known type returns
        // None; the resolver in template.rs surfaces a "Logical.Attr"
        // placeholder so the existing template still builds.
        assert_eq!(prov.get_att(&sr, "NotARealAttr"), None);
    }

    #[test]
    fn getatt_unknown_resource_type_returns_none() {
        let prov = make_provisioner();
        // Hand-crafted StackResource with a resource_type we don't dispatch
        // on at all. The captured attributes map is also empty, so there's
        // nothing to return.
        let stack_resource = StackResource {
            logical_id: "Mystery".to_string(),
            physical_id: "mystery-id".to_string(),
            resource_type: "AWS::Made::Up".to_string(),
            status: "CREATE_COMPLETE".to_string(),
            service_token: None,
            attributes: BTreeMap::new(),
        };
        assert_eq!(prov.get_att(&stack_resource, "Arn"), None);
    }

    #[test]
    fn getatt_falls_back_to_captured_attributes() {
        let prov = make_provisioner();
        // Captured attributes always win — even if live-state dispatch
        // would return something different. This keeps `Fn::GetAtt`
        // deterministic for resources with cached attrs.
        let stack_resource = StackResource {
            logical_id: "MyTopic".to_string(),
            physical_id: "arn:aws:sns:us-east-1:123456789012:my-topic".to_string(),
            resource_type: "AWS::SNS::Topic".to_string(),
            status: "CREATE_COMPLETE".to_string(),
            service_token: None,
            attributes: {
                let mut m = BTreeMap::new();
                m.insert("TopicArn".to_string(), "captured-arn".to_string());
                m
            },
        };
        assert_eq!(
            prov.get_att(&stack_resource, "TopicArn"),
            Some("captured-arn".to_string())
        );
    }

    #[test]
    fn getatt_secrets_manager_arn_resolves_via_live_state() {
        // Secrets create handler captures Id but not Arn; live-state
        // fallback fills in Arn.
        let prov = make_provisioner();
        let res = make_resource(
            "AWS::SecretsManager::Secret",
            "MySecret",
            serde_json::json!({"Name": "my-secret", "SecretString": "hunter2"}),
        );
        let sr = prov.create_resource(&res).unwrap();
        let arn = prov.get_att(&sr, "Arn").expect("Arn should resolve");
        assert!(arn.starts_with("arn:aws:secretsmanager:"));
        assert!(arn.ends_with(":secret:my-secret"));
    }

    #[test]
    fn wafv2_web_acl_lifecycle() {
        let prov = make_provisioner();
        let res = make_resource(
            "AWS::WAFv2::WebACL",
            "MyAcl",
            serde_json::json!({
                "Name": "my-acl",
                "Scope": "REGIONAL",
                "DefaultAction": {"Allow": {}},
                "Rules": [{"Name": "rule1", "Priority": 1, "Statement": {}, "VisibilityConfig": {}}],
                "VisibilityConfig": {"SampledRequestsEnabled": true, "CloudWatchMetricsEnabled": true, "MetricName": "my-acl-metric"},
                "Capacity": 100,
            }),
        );
        let sr = prov.create_resource(&res).unwrap();
        assert!(sr.physical_id.starts_with("arn:aws:wafv2:"));
        assert_eq!(prov.get_att(&sr, "Arn"), Some(sr.physical_id.clone()));
        assert_eq!(prov.get_att(&sr, "Name"), Some("my-acl".to_string()));
        assert!(prov.get_att(&sr, "Id").is_some());
        assert_eq!(prov.get_att(&sr, "Capacity"), Some("100".to_string()));

        prov.delete_resource(&sr.clone()).unwrap();
        // Verify live-state fallback returns None by using a fresh resource
        // with empty attributes (captured attrs would still win).
        let fresh = StackResource {
            logical_id: "MyAcl".to_string(),
            physical_id: sr.physical_id.clone(),
            resource_type: "AWS::WAFv2::WebACL".to_string(),
            status: "CREATE_COMPLETE".to_string(),
            service_token: None,
            attributes: BTreeMap::new(),
        };
        assert_eq!(prov.get_att(&fresh, "Arn"), None);
    }

    #[test]
    fn wafv2_ip_set_lifecycle() {
        let prov = make_provisioner();
        let res = make_resource(
            "AWS::WAFv2::IPSet",
            "MyIpSet",
            serde_json::json!({
                "Name": "my-ipset",
                "Scope": "REGIONAL",
                "IPAddressVersion": "IPV4",
                "Addresses": ["10.0.0.0/8"],
            }),
        );
        let sr = prov.create_resource(&res).unwrap();
        assert!(sr.physical_id.starts_with("arn:aws:wafv2:"));
        assert_eq!(prov.get_att(&sr, "Arn"), Some(sr.physical_id.clone()));
        assert_eq!(prov.get_att(&sr, "Name"), Some("my-ipset".to_string()));

        prov.delete_resource(&sr.clone()).unwrap();
        let fresh = StackResource {
            logical_id: "MyIpSet".to_string(),
            physical_id: sr.physical_id.clone(),
            resource_type: "AWS::WAFv2::IPSet".to_string(),
            status: "CREATE_COMPLETE".to_string(),
            service_token: None,
            attributes: BTreeMap::new(),
        };
        assert_eq!(prov.get_att(&fresh, "Arn"), None);
    }

    #[test]
    fn wafv2_regex_pattern_set_lifecycle() {
        let prov = make_provisioner();
        let res = make_resource(
            "AWS::WAFv2::RegexPatternSet",
            "MyRegexSet",
            serde_json::json!({
                "Name": "my-regex",
                "Scope": "REGIONAL",
                "RegularExpressions": [{"RegexString": "^test"}],
            }),
        );
        let sr = prov.create_resource(&res).unwrap();
        assert!(sr.physical_id.starts_with("arn:aws:wafv2:"));
        assert_eq!(prov.get_att(&sr, "Arn"), Some(sr.physical_id.clone()));
        assert_eq!(prov.get_att(&sr, "Name"), Some("my-regex".to_string()));

        prov.delete_resource(&sr.clone()).unwrap();
        let fresh = StackResource {
            logical_id: "MyRegexSet".to_string(),
            physical_id: sr.physical_id.clone(),
            resource_type: "AWS::WAFv2::RegexPatternSet".to_string(),
            status: "CREATE_COMPLETE".to_string(),
            service_token: None,
            attributes: BTreeMap::new(),
        };
        assert_eq!(prov.get_att(&fresh, "Arn"), None);
    }

    #[test]
    fn wafv2_rule_group_lifecycle() {
        let prov = make_provisioner();
        let res = make_resource(
            "AWS::WAFv2::RuleGroup",
            "MyRuleGroup",
            serde_json::json!({
                "Name": "my-rg",
                "Scope": "REGIONAL",
                "Capacity": 50,
                "Rules": [{"Name": "r1", "Priority": 1, "Statement": {}, "VisibilityConfig": {}}],
                "VisibilityConfig": {"SampledRequestsEnabled": true, "CloudWatchMetricsEnabled": true, "MetricName": "rg-metric"},
            }),
        );
        let sr = prov.create_resource(&res).unwrap();
        assert!(sr.physical_id.starts_with("arn:aws:wafv2:"));
        assert_eq!(prov.get_att(&sr, "Arn"), Some(sr.physical_id.clone()));
        assert_eq!(prov.get_att(&sr, "Name"), Some("my-rg".to_string()));

        prov.delete_resource(&sr.clone()).unwrap();
        let fresh = StackResource {
            logical_id: "MyRuleGroup".to_string(),
            physical_id: sr.physical_id.clone(),
            resource_type: "AWS::WAFv2::RuleGroup".to_string(),
            status: "CREATE_COMPLETE".to_string(),
            service_token: None,
            attributes: BTreeMap::new(),
        };
        assert_eq!(prov.get_att(&fresh, "Arn"), None);
    }

    #[test]
    fn wafv2_logging_configuration_lifecycle() {
        let prov = make_provisioner();
        let res = make_resource(
            "AWS::WAFv2::LoggingConfiguration",
            "MyLogConfig",
            serde_json::json!({
                "ResourceArn": "arn:aws:wafv2:us-east-1:123456789012:regional/webacl/test/abc",
                "LogDestinationConfigs": ["arn:aws:logs:us-east-1:123456789012:log-group:/aws/waf"],
            }),
        );
        let sr = prov.create_resource(&res).unwrap();
        assert_eq!(
            sr.physical_id,
            "arn:aws:wafv2:us-east-1:123456789012:regional/webacl/test/abc"
        );

        prov.delete_resource(&sr.clone()).unwrap();
    }

    #[test]
    fn wafv2_web_acl_association_lifecycle() {
        let prov = make_provisioner();
        let res = make_resource(
            "AWS::WAFv2::WebACLAssociation",
            "MyAssoc",
            serde_json::json!({
                "ResourceArn": "arn:aws:elasticloadbalancing:us-east-1:123456789012:loadbalancer/app/my-alb/50dc6c495c0c9188",
                "WebACLArn": "arn:aws:wafv2:us-east-1:123456789012:regional/webacl/my-acl/abc",
            }),
        );
        let sr = prov.create_resource(&res).unwrap();
        assert_eq!(sr.physical_id, "arn:aws:elasticloadbalancing:us-east-1:123456789012:loadbalancer/app/my-alb/50dc6c495c0c9188");

        prov.delete_resource(&sr.clone()).unwrap();
    }

    #[test]
    fn ses_configuration_set_lifecycle() {
        let prov = make_provisioner();
        let res = make_resource(
            "AWS::SES::ConfigurationSet",
            "MyConfigSet",
            serde_json::json!({
                "Name": "my-cs",
                "SendingOptions": {"SendingEnabled": true},
                "DeliveryOptions": {"TlsPolicy": "REQUIRE"},
            }),
        );
        let sr = prov.create_resource(&res).unwrap();
        assert_eq!(sr.physical_id, "my-cs");
        assert_eq!(prov.get_att(&sr, "Name"), Some("my-cs".to_string()));

        prov.delete_resource(&sr.clone()).unwrap();
        let fresh = StackResource {
            logical_id: "MyConfigSet".to_string(),
            physical_id: "my-cs".to_string(),
            resource_type: "AWS::SES::ConfigurationSet".to_string(),
            status: "CREATE_COMPLETE".to_string(),
            service_token: None,
            attributes: BTreeMap::new(),
        };
        assert_eq!(prov.get_att(&fresh, "Name"), None);
    }

    #[test]
    fn ses_email_identity_lifecycle() {
        let prov = make_provisioner();
        let res = make_resource(
            "AWS::SES::EmailIdentity",
            "MyIdentity",
            serde_json::json!({"EmailIdentity": "example.com"}),
        );
        let sr = prov.create_resource(&res).unwrap();
        assert_eq!(sr.physical_id, "example.com");
        assert_eq!(
            prov.get_att(&sr, "IdentityName"),
            Some("example.com".to_string())
        );

        prov.delete_resource(&sr.clone()).unwrap();
        let fresh = StackResource {
            logical_id: "MyIdentity".to_string(),
            physical_id: "example.com".to_string(),
            resource_type: "AWS::SES::EmailIdentity".to_string(),
            status: "CREATE_COMPLETE".to_string(),
            service_token: None,
            attributes: BTreeMap::new(),
        };
        assert_eq!(prov.get_att(&fresh, "IdentityName"), None);
    }

    #[test]
    fn ses_template_lifecycle() {
        let prov = make_provisioner();
        let res = make_resource(
            "AWS::SES::Template",
            "MyTemplate",
            serde_json::json!({
                "Template": {
                    "TemplateName": "my-tpl",
                    "SubjectPart": "Hello",
                    "HtmlPart": "<h1>Hi</h1>",
                    "TextPart": "Hi",
                },
            }),
        );
        let sr = prov.create_resource(&res).unwrap();
        assert_eq!(sr.physical_id, "my-tpl");
        assert_eq!(
            prov.get_att(&sr, "TemplateName"),
            Some("my-tpl".to_string())
        );

        prov.delete_resource(&sr.clone()).unwrap();
        let fresh = StackResource {
            logical_id: "MyTemplate".to_string(),
            physical_id: "my-tpl".to_string(),
            resource_type: "AWS::SES::Template".to_string(),
            status: "CREATE_COMPLETE".to_string(),
            service_token: None,
            attributes: BTreeMap::new(),
        };
        assert_eq!(prov.get_att(&fresh, "TemplateName"), None);
    }

    #[test]
    fn ses_contact_list_lifecycle() {
        let prov = make_provisioner();
        let res = make_resource(
            "AWS::SES::ContactList",
            "MyContactList",
            serde_json::json!({
                "ContactListName": "my-cl",
                "Description": "Test contacts",
                "Topics": [{"TopicName": "news", "DisplayName": "Newsletter", "Description": "Weekly news"}],
            }),
        );
        let sr = prov.create_resource(&res).unwrap();
        assert_eq!(sr.physical_id, "my-cl");
        assert_eq!(
            prov.get_att(&sr, "ContactListName"),
            Some("my-cl".to_string())
        );

        prov.delete_resource(&sr.clone()).unwrap();
        let fresh = StackResource {
            logical_id: "MyContactList".to_string(),
            physical_id: "my-cl".to_string(),
            resource_type: "AWS::SES::ContactList".to_string(),
            status: "CREATE_COMPLETE".to_string(),
            service_token: None,
            attributes: BTreeMap::new(),
        };
        assert_eq!(prov.get_att(&fresh, "ContactListName"), None);
    }

    #[test]
    fn ses_dedicated_ip_pool_lifecycle() {
        let prov = make_provisioner();
        let res = make_resource(
            "AWS::SES::DedicatedIpPool",
            "MyPool",
            serde_json::json!({"PoolName": "my-pool", "ScalingMode": "STANDARD"}),
        );
        let sr = prov.create_resource(&res).unwrap();
        assert_eq!(sr.physical_id, "my-pool");
        assert_eq!(prov.get_att(&sr, "PoolName"), Some("my-pool".to_string()));

        prov.delete_resource(&sr.clone()).unwrap();
        let fresh = StackResource {
            logical_id: "MyPool".to_string(),
            physical_id: "my-pool".to_string(),
            resource_type: "AWS::SES::DedicatedIpPool".to_string(),
            status: "CREATE_COMPLETE".to_string(),
            service_token: None,
            attributes: BTreeMap::new(),
        };
        assert_eq!(prov.get_att(&fresh, "PoolName"), None);
    }

    #[test]
    fn ses_receipt_rule_set_lifecycle() {
        let prov = make_provisioner();
        let res = make_resource(
            "AWS::SES::ReceiptRuleSet",
            "MyRuleSet",
            serde_json::json!({"RuleSetName": "my-rs"}),
        );
        let sr = prov.create_resource(&res).unwrap();
        assert_eq!(sr.physical_id, "my-rs");
        assert_eq!(prov.get_att(&sr, "RuleSetName"), Some("my-rs".to_string()));

        prov.delete_resource(&sr.clone()).unwrap();
        let fresh = StackResource {
            logical_id: "MyRuleSet".to_string(),
            physical_id: "my-rs".to_string(),
            resource_type: "AWS::SES::ReceiptRuleSet".to_string(),
            status: "CREATE_COMPLETE".to_string(),
            service_token: None,
            attributes: BTreeMap::new(),
        };
        assert_eq!(prov.get_att(&fresh, "RuleSetName"), None);
    }

    #[test]
    fn ses_receipt_rule_lifecycle() {
        let prov = make_provisioner();
        let rs = make_resource(
            "AWS::SES::ReceiptRuleSet",
            "MyRuleSet",
            serde_json::json!({"RuleSetName": "my-rs2"}),
        );
        prov.create_resource(&rs).unwrap();

        let res = make_resource(
            "AWS::SES::ReceiptRule",
            "MyRule",
            serde_json::json!({
                "RuleSetName": "my-rs2",
                "Rule": {
                    "Name": "rule1",
                    "Priority": 1,
                    "Enabled": true,
                    "Actions": [{"S3Action": {"BucketName": "my-bucket"}}],
                },
            }),
        );
        let sr = prov.create_resource(&res).unwrap();
        assert_eq!(sr.physical_id, "my-rs2|rule1");

        prov.delete_resource(&sr.clone()).unwrap();
    }

    #[test]
    fn ses_receipt_filter_lifecycle() {
        let prov = make_provisioner();
        let res = make_resource(
            "AWS::SES::ReceiptFilter",
            "MyFilter",
            serde_json::json!({
                "Filter": {
                    "Name": "my-filter",
                    "IpFilter": {"Policy": "Block", "Cidr": "10.0.0.0/8"},
                },
            }),
        );
        let sr = prov.create_resource(&res).unwrap();
        assert_eq!(sr.physical_id, "my-filter");

        prov.delete_resource(&sr.clone()).unwrap();
    }

    #[test]
    fn ses_vdm_attributes_lifecycle() {
        let prov = make_provisioner();
        let res = make_resource(
            "AWS::SES::VdmAttributes",
            "MyVdm",
            serde_json::json!({
                "DashboardAttributes": {"EngagementMetrics": "ENABLED"},
                "GuardianAttributes": {"OptimizedSharedDelivery": "ENABLED"},
            }),
        );
        let sr = prov.create_resource(&res).unwrap();
        assert_eq!(sr.physical_id, "vdm-MyVdm");

        prov.delete_resource(&sr.clone()).unwrap();
    }

    #[test]
    fn athena_work_group_lifecycle() {
        let prov = make_provisioner();
        let res = make_resource(
            "AWS::Athena::WorkGroup",
            "MyWg",
            serde_json::json!({
                "Name": "my-wg",
                "Description": "test wg",
                "Configuration": {"EnforceWorkGroupConfiguration": true},
            }),
        );
        let sr = prov.create_resource(&res).unwrap();
        assert_eq!(sr.physical_id, "my-wg");
        assert_eq!(sr.attributes.get("Name"), Some(&"my-wg".to_string()));
        assert!(sr
            .attributes
            .get("Arn")
            .unwrap()
            .contains("workgroup/my-wg"));

        assert_eq!(
            prov.get_att(
                &StackResource {
                    resource_type: "AWS::Athena::WorkGroup".to_string(),
                    physical_id: sr.physical_id.clone(),
                    logical_id: "MyWg".to_string(),
                    status: "CREATE_COMPLETE".to_string(),
                    service_token: None,
                    attributes: BTreeMap::new(),
                },
                "Name",
            ),
            Some("my-wg".to_string()),
        );

        prov.delete_resource(&sr.clone()).unwrap();
    }

    #[test]
    fn athena_data_catalog_lifecycle() {
        let prov = make_provisioner();
        let res = make_resource(
            "AWS::Athena::DataCatalog",
            "MyCatalog",
            serde_json::json!({
                "Name": "my-catalog",
                "Type": "GLUE",
                "Description": "test catalog",
            }),
        );
        let sr = prov.create_resource(&res).unwrap();
        assert_eq!(sr.physical_id, "my-catalog");
        assert_eq!(sr.attributes.get("Name"), Some(&"my-catalog".to_string()));
        assert!(sr
            .attributes
            .get("Arn")
            .unwrap()
            .contains("datacatalog/my-catalog"));

        prov.delete_resource(&sr.clone()).unwrap();
    }

    #[test]
    fn athena_named_query_lifecycle() {
        let prov = make_provisioner();
        let res = make_resource(
            "AWS::Athena::NamedQuery",
            "MyQuery",
            serde_json::json!({
                "Name": "my-query",
                "Database": "mydb",
                "QueryString": "SELECT 1",
                "WorkGroup": "primary",
            }),
        );
        let sr = prov.create_resource(&res).unwrap();
        assert!(!sr.physical_id.is_empty());
        assert_eq!(sr.attributes.get("NamedQueryId"), Some(&sr.physical_id));

        prov.delete_resource(&sr.clone()).unwrap();
    }

    #[test]
    fn athena_prepared_statement_lifecycle() {
        let prov = make_provisioner();
        let res = make_resource(
            "AWS::Athena::PreparedStatement",
            "MyPs",
            serde_json::json!({
                "StatementName": "my-ps",
                "WorkGroupName": "primary",
                "QueryStatement": "SELECT 1",
            }),
        );
        let sr = prov.create_resource(&res).unwrap();
        assert_eq!(sr.physical_id, "primary|my-ps");

        prov.delete_resource(&sr.clone()).unwrap();
    }
}
