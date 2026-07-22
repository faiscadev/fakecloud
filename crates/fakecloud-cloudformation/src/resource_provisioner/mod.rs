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
use fakecloud_acmpca::SharedAcmPcaState;
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
use fakecloud_aws::arn::Arn;
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
    SharedCloudFrontState, StoredDistribution,
};
use fakecloud_cloudwatch::{
    AlarmMetricQuery, AlarmMetricStat, AlarmState, Dashboard, MetricAlarm, SharedCloudWatchState,
};
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
use fakecloud_eks::SharedEksState;
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
use fakecloud_firehose::{DeliveryStream, S3Destination};
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
use fakecloud_persistence::{BucketSubresource, S3Store};
use fakecloud_rds::{DbInstance, DbParameterGroup, DbSubnetGroup, RdsTag, SharedRdsState};
use fakecloud_route53::{
    model::{HealthCheckConfig, HostedZoneFeatures, ResourceRecordSet},
    SharedRoute53State, StoredHealthCheck, StoredHostedZone,
};
use fakecloud_s3::persistence::bucket_meta_snapshot;
use fakecloud_s3::{S3Bucket, SharedS3State};
use fakecloud_secretsmanager::{RotationRules, Secret, SecretVersion, SharedSecretsManagerState};
use fakecloud_servicediscovery::SharedServiceDiscoveryState;
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

/// Pull the bare function name out of any shape AWS accepts for a Lambda
/// `FunctionName`: a name, a partial ARN, or a full ARN — each optionally
/// `:qualified` with an alias or version. CFN feeds this `{Ref: SomeFunction}`
/// (resolves to a name, or — for an alias — the alias ARN), `{Fn::GetAtt:
/// [F, Arn]}` (full ARN), or a literal; all shapes must land at the same map
/// key. Function names cannot contain `:`, so any trailing `:segment` on a
/// non-ARN input is always a qualifier.
fn parse_lambda_function_name(input: &str) -> String {
    // Full ARN: arn:aws:lambda:region:account:function:name[:qualifier]
    if let Some(rest) = input.strip_prefix("arn:aws:lambda:") {
        if let Some(after) = rest.split(":function:").nth(1) {
            return after.split(':').next().unwrap_or(after).to_string();
        }
    }
    // Partial ARN: account:function:name[:qualifier]
    if let Some(after) = input.split(":function:").nth(1) {
        return after.split(':').next().unwrap_or(after).to_string();
    }
    // Bare name, optionally qualified: name[:qualifier]
    input.split(':').next().unwrap_or(input).to_string()
}

/// Recover the internal `{function}:{alias}` key used by the lambda
/// state's `aliases` / `provisioned_concurrency` maps from an alias
/// resource's physical id. The physical id is the alias ARN
/// (`arn:aws:lambda:region:account:function:name:alias`); a legacy bare
/// `name:alias` value is returned unchanged.
fn alias_state_key(physical_id: &str) -> String {
    if let Some(rest) = physical_id.strip_prefix("arn:aws:lambda:") {
        if let Some(after) = rest.split(":function:").nth(1) {
            return after.to_string();
        }
    }
    physical_id.to_string()
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

/// Lowercase the first ASCII character of a key. `RoleArn` -> `roleArn`.
pub(super) fn lower_first_char(s: &str) -> String {
    let mut chars = s.chars();
    match chars.next() {
        Some(c) => c.to_ascii_lowercase().to_string() + chars.as_str(),
        None => String::new(),
    }
}

/// Recursively convert a CloudFormation PascalCase property tree into the
/// camelCase API shape the AWS services store internally, so a CFN-provisioned
/// resource reads back through `Describe*`/`Get*` identically to a direct-API
/// one. Lowercases the first character of every object key and recurses into
/// arrays and objects.
///
/// Keys listed in `opaque` carry their VALUE through verbatim (the value's own
/// keys are not transformed): these are provider-defined free-form maps such as
/// a CodePipeline action's `Configuration`, whose inner keys AWS preserves
/// exactly as written.
pub(super) fn cfn_props_to_camel(value: &serde_json::Value, opaque: &[&str]) -> serde_json::Value {
    match value {
        serde_json::Value::Object(map) => {
            let mut out = serde_json::Map::new();
            for (k, v) in map {
                let key = lower_first_char(k);
                let converted = if opaque.contains(&k.as_str()) {
                    v.clone()
                } else {
                    cfn_props_to_camel(v, opaque)
                };
                out.insert(key, converted);
            }
            serde_json::Value::Object(out)
        }
        serde_json::Value::Array(arr) => {
            serde_json::Value::Array(arr.iter().map(|v| cfn_props_to_camel(v, opaque)).collect())
        }
        other => other.clone(),
    }
}

/// Extract a resource policy's `PolicyDocument` property as a JSON string,
/// accepting either an inline object or an already-serialized string. Shared by
/// the SQS/SNS/S3 resource-policy provisioners.
fn policy_document_string(props: &serde_json::Value) -> Result<String, String> {
    match props.get("PolicyDocument") {
        Some(serde_json::Value::String(s)) => Ok(s.clone()),
        Some(other) => Ok(other.to_string()),
        None => Err("PolicyDocument is required".to_string()),
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
    pub ec2_state: fakecloud_ec2::SharedEc2State,
    pub autoscaling_state: fakecloud_autoscaling::SharedAutoScalingState,
    pub batch_state: fakecloud_batch::SharedBatchState,
    pub pipes_state: fakecloud_pipes::SharedPipesState,
    pub ecs_state: SharedEcsState,
    pub acm_state: SharedAcmState,
    pub acmpca_state: SharedAcmPcaState,
    pub config_state: fakecloud_config::SharedConfigState,
    pub route53resolver_state: fakecloud_route53resolver::SharedRoute53ResolverState,
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
    pub eks_state: SharedEksState,
    pub servicediscovery_state: SharedServiceDiscoveryState,
    pub codeartifact_state: fakecloud_codeartifact::SharedCodeArtifactState,
    pub codecommit_state: fakecloud_codecommit::SharedCodeCommitState,
    pub efs_state: fakecloud_efs::SharedEfsState,
    pub elasticbeanstalk_state: fakecloud_elasticbeanstalk::SharedEbState,
    pub mq_state: fakecloud_mq::SharedMqState,
    pub kafka_state: fakecloud_kafka::SharedKafkaState,
    pub ka2_state: fakecloud_kinesisanalyticsv2::SharedKa2State,
    pub redshift_state: fakecloud_redshift::SharedRedshiftState,
    pub docdb_state: fakecloud_docdb::SharedDocDbState,
    pub neptune_state: fakecloud_neptune::SharedNeptuneState,
    pub codepipeline_state: fakecloud_codepipeline::SharedCodePipelineState,
    pub codebuild_state: fakecloud_codebuild::SharedCodeBuildState,
    pub codedeploy_state: fakecloud_codedeploy::SharedCodeDeployState,
    pub opensearch_state: fakecloud_opensearch::SharedOpenSearchState,
    pub emr_state: fakecloud_emr::SharedEmrState,
    pub sagemaker_state: fakecloud_sagemaker::SharedSageMakerState,
    pub backup_state: fakecloud_backup::SharedBackupState,
    pub appsync_state: fakecloud_appsync::SharedAppSyncState,
    pub timestream_state: fakecloud_timestream::SharedTimestreamState,
    pub mwaa_state: fakecloud_mwaa::SharedMwaaState,
    pub amplify_state: fakecloud_amplify::SharedAmplifyState,
    pub appconfig_state: fakecloud_appconfig::SharedAppConfigState,
    pub cloudformation_state: SharedCloudFormationState,
    pub delivery: Arc<DeliveryBus>,
    /// Lambda container runtime for pre-pulling CFN-provisioned function
    /// images (see `CloudFormationDeps::lambda_runtime`). `None` outside a
    /// configured runtime (e.g. unit tests).
    pub lambda_runtime: Option<Arc<fakecloud_lambda::runtime::ContainerRuntime>>,
    /// Container runtimes for stateful services whose CFN-provisioned resources
    /// must be backed by REAL containers. See `CloudFormationDeps`. `None`
    /// (no Docker/Podman, e.g. CI/unit tests) keeps metadata-only provisioning.
    pub rds_runtime: Option<Arc<fakecloud_rds::runtime::RdsRuntime>>,
    pub ec2_runtime: Option<Arc<fakecloud_ec2::runtime::Ec2Runtime>>,
    pub ecs_runtime: Option<Arc<fakecloud_ecs::runtime::EcsRuntime>>,
    pub elasticache_runtime: Option<Arc<fakecloud_elasticache::runtime::ElastiCacheRuntime>>,
    pub mq_runtime: Option<Arc<fakecloud_mq::MqRuntime>>,
    pub kafka_runtime: Option<Arc<fakecloud_kafka::KafkaRuntime>>,
    /// Intents queued by container-backed provisioners during the synchronous
    /// provisioning pass. After provisioning, `CreateStack` drains these and
    /// backs each freshly-inserted record with a real container in the
    /// background (so `CreateStack` returns without blocking on a container
    /// boot — the #1539/#1730 timeout lesson). Shared via `Arc` so the drain
    /// can read it after the provisioner is moved into `spawn_blocking`.
    pub pending_container_spawns: Arc<parking_lot::Mutex<Vec<ContainerSpawnIntent>>>,
    /// Teardown intents queued by container-backed delete provisioners during a
    /// synchronous delete pass (stack delete, or a stack update that removes a
    /// resource). The in-memory record is removed synchronously (so
    /// `DescribeStacks` reflects the deletion at once); the REAL backing
    /// container is reaped in the background by the CloudFormation delete drain,
    /// mirroring `pending_container_spawns` for teardown. Without this drain a
    /// stack delete would leak the running RDS / ElastiCache / ECS / EC2
    /// containers (the create-side #2031-#2034 hardening never reached delete).
    pub pending_container_teardowns: Arc<parking_lot::Mutex<Vec<ContainerTeardownIntent>>>,
    /// Custom-resource (`Custom::*`) Lambda invoke intents queued during a
    /// changeset/update provision when `defer_custom_invokes` is set. Invoking
    /// the Lambda synchronously (`invoke_lambda_sync`) can cold-pull a container
    /// image for minutes -- far past the client's 60s read timeout -- and, on
    /// the changeset/update path, it ran while holding the CloudFormation state
    /// write lock, stalling every other CFN op behind it. Queueing here lets the
    /// caller drain + `tokio::spawn` the invokes off the request path after the
    /// lock is dropped, mirroring how `CreateStack` provisions custom resources
    /// off the request path.
    pub pending_custom_invokes: Arc<parking_lot::Mutex<Vec<CustomInvokeIntent>>>,
    /// When `true`, `create_custom_resource` / `delete_custom_resource` queue
    /// their Lambda invoke onto `pending_custom_invokes` instead of running it
    /// synchronously. Set on the changeset/update/delete provisioners; left
    /// `false` for `CreateStack` (which already provisions off the request path
    /// in a detached task, so its synchronous invoke never blocks the client or
    /// the state lock).
    pub defer_custom_invokes: bool,
    /// Fine-grained S3 disk store. Bucket create/delete (and bucket-policy
    /// updates) write through this so a CFN-provisioned bucket lands on disk,
    /// matching the real `CreateBucket`/`DeleteBucket` handlers. A
    /// `MemoryS3Store` (memory mode) makes the writes no-ops.
    pub s3_store: Arc<dyn S3Store>,
    pub account_id: String,
    pub region: String,
    pub stack_id: String,
    /// When `true`, `create_resource`'s fallback arm for unmodeled resource
    /// types returns an error instead of recording a phantom resource with no
    /// backing state. Cloud Control API sets this so `CreateResource` rejects a
    /// `TypeName` fakecloud has no provisioner for, rather than reporting
    /// success for a resource `Get`/`List` would then surface with no owning
    /// service state. `CreateStack` leaves it `false` to keep accepting full
    /// templates (SAM/CDK output routinely includes types fakecloud does not
    /// model).
    pub strict_unknown_types: bool,
}

/// A container-backed resource the synchronous provisioning pass inserted as a
/// "creating" record that now needs a real backing container. Drained by
/// `CreateStack` after provisioning and backgrounded so the stack create call
/// never blocks on a container boot/pull.
#[derive(Debug, Clone)]
pub enum ContainerSpawnIntent {
    /// `AWS::RDS::DBInstance` — back the inserted DbInstance with a real
    /// Postgres/MySQL container via the RDS runtime.
    RdsInstance { identifier: String },
    /// `AWS::AutoScaling::AutoScalingGroup` — reconcile the inserted group to
    /// its desired capacity by launching real container-backed EC2 instances
    /// via the EC2 runtime, matching the direct `CreateAutoScalingGroup` path.
    AsgInstances { group_name: String },
    /// `AWS::EC2::Instance` — back the inserted (control-plane `pending`)
    /// instance with a real container via the EC2 runtime, matching the direct
    /// `RunInstances` path.
    Ec2Instance { instance_id: String },
    /// `AWS::ElastiCache::CacheCluster` — back the inserted CacheCluster with a
    /// real Redis/Memcached container via the ElastiCache runtime, matching the
    /// direct `CreateCacheCluster` path.
    ElastiCacheCluster { cache_cluster_id: String },
    /// `AWS::ElastiCache::ReplicationGroup` — back the inserted ReplicationGroup
    /// with a real Redis container via the ElastiCache runtime, matching the
    /// direct `CreateReplicationGroup` path.
    ElastiCacheReplicationGroup { replication_group_id: String },
    /// `AWS::ECS::Service` — launch the REAL tasks (containers) the inserted
    /// service needs to reach its `desiredCount` via the ECS runtime, matching
    /// the direct `CreateService` path. The cluster + service name locate the
    /// inserted record (and its desired count) when the drain runs.
    EcsServiceTasks {
        cluster_name: String,
        service_name: String,
    },
    /// `AWS::AmazonMQ::Broker` — back the inserted broker with a real
    /// ActiveMQ/RabbitMQ container via the MQ runtime, matching the direct
    /// `CreateBroker` path.
    MqBroker { broker_id: String },
    /// `AWS::MSK::Cluster` — back the inserted PROVISIONED cluster with a real
    /// Apache Kafka container via the Kafka runtime, matching the direct
    /// `CreateCluster` path.
    MskCluster { cluster_arn: String },
}

/// A container-backed resource the synchronous delete pass removed from memory
/// that still has a REAL backing container to reap. Drained by the
/// CloudFormation delete path (stack delete / update-removed) and backgrounded
/// so the stack op never blocks on a container stop. Mirrors
/// [`ContainerSpawnIntent`] for teardown.
#[derive(Debug, Clone)]
pub enum ContainerTeardownIntent {
    /// `AWS::RDS::DBInstance` -- stop + remove the Postgres/MySQL container and
    /// its persisted data volume.
    RdsInstance { identifier: String },
    /// `AWS::ElastiCache::CacheCluster` -- stop + remove the Redis/Memcached
    /// container and its data volume.
    ElastiCacheCluster { cache_cluster_id: String },
    /// `AWS::ElastiCache::ReplicationGroup` -- stop + remove the Redis container
    /// and its data volume.
    ElastiCacheReplicationGroup { replication_group_id: String },
    /// `AWS::ECS::Service` -- stop the REAL tasks (containers) the service was
    /// running. The cluster + service name locate the orphaned task records.
    EcsService {
        cluster_name: String,
        service_name: String,
    },
    /// `AWS::AutoScaling::AutoScalingGroup` -- terminate the REAL EC2 instances
    /// the group launched (captured before the group record was removed).
    AsgInstances { instance_ids: Vec<String> },
    /// `AWS::EC2::Instance` -- terminate the REAL EC2 instance + reap its
    /// backing container when the stack is deleted.
    Ec2Instance { instance_id: String },
    /// `AWS::AmazonMQ::Broker` -- stop + remove the ActiveMQ/RabbitMQ container.
    MqBroker { broker_id: String },
    /// `AWS::MSK::Cluster` -- stop + remove the backing Apache Kafka container.
    MskCluster { cluster_arn: String },
}

/// A queued custom-resource (`Custom::*`) Lambda invocation. Built by
/// `create_custom_resource` / `delete_custom_resource` when
/// `defer_custom_invokes` is set, drained and `tokio::spawn`ed off the request
/// path so a cold image pull never blocks the client or the CFN state lock.
#[derive(Debug, Clone)]
pub struct CustomInvokeIntent {
    pub service_token: String,
    pub payload: String,
}

mod acm;
mod acmpca;
mod amplify;
mod apigw;
mod apigwv2;
mod appautoscaling;
mod appconfig;
mod appsync;
mod athena;
mod autoscaling;
mod backup;
mod batch;
mod cloudformation;
mod cloudfront;
mod cloudwatch;
mod codeartifact;
mod codebuild;
mod codecommit;
mod codedeploy;
mod codepipeline;
mod cognito;
mod config;
mod cwlogs;
mod dynamodb;
mod ec2;
mod ecr;
mod ecs;
mod efs;
mod eks;
mod elasticache;
mod elasticbeanstalk;
mod elbv2;
mod emr;
mod eventbridge;
mod firehose;
mod glue;
mod iam;
mod kafka;
mod kinesis;
mod kinesisanalyticsv2;
mod kms;
mod lambda;
mod logs;
mod mq;
mod mwaa;
mod opensearch;
mod organizations;
mod pipes;
mod rds;
mod redshiftlike;
mod route;
mod route53resolver;
mod s3;
mod sagemaker;
mod secrets;
mod servicediscovery;
mod ses;
mod sns;
mod sqs;
mod ssm;
mod stepfunctions;
mod timestream;
mod wafv2;

impl ResourceProvisioner {
    /// Create a resource and return the StackResource with physical ID.
    pub fn create_resource(&self, resource: &ResourceDefinition) -> Result<StackResource, String> {
        let result = match resource.resource_type.as_str() {
            "AWS::SQS::Queue" => self.create_sqs_queue(resource),
            "AWS::SQS::QueuePolicy" => self.create_sqs_queue_policy(resource),
            "AWS::SNS::Topic" => self.create_sns_topic(resource),
            "AWS::SNS::TopicPolicy" => self.create_sns_topic_policy(resource),
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
            "AWS::S3::BucketPolicy" => self.create_s3_bucket_policy(resource),
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
            "AWS::Redshift::Cluster" => self.create_redshift_cluster(resource),
            "AWS::DocDB::DBCluster" => self.create_docdb_cluster(resource),
            "AWS::Neptune::DBCluster" => self.create_neptune_cluster(resource),
            "AWS::AutoScaling::LaunchConfiguration" => {
                self.create_autoscaling_launch_configuration(resource)
            }
            "AWS::AutoScaling::AutoScalingGroup" => self.create_autoscaling_group(resource),
            "AWS::Batch::ComputeEnvironment" => self.create_batch_compute_environment(resource),
            "AWS::Batch::JobQueue" => self.create_batch_job_queue(resource),
            "AWS::Batch::JobDefinition" => self.create_batch_job_definition(resource),
            "AWS::Batch::SchedulingPolicy" => self.create_batch_scheduling_policy(resource),
            "AWS::Pipes::Pipe" => self.create_pipes_pipe(resource),
            "AWS::CodeArtifact::Domain" => self.create_codeartifact_domain(resource),
            "AWS::CodeArtifact::Repository" => self.create_codeartifact_repository(resource),
            "AWS::AmazonMQ::Broker" => self.create_mq_broker(resource),
            "AWS::AmazonMQ::Configuration" => self.create_mq_configuration(resource),
            "AWS::AmazonMQ::ConfigurationAssociation" => {
                self.create_mq_configuration_association(resource)
            }
            "AWS::MSK::Cluster" => self.create_msk_cluster(resource),
            "AWS::MSK::ServerlessCluster" => self.create_msk_serverless_cluster(resource),
            "AWS::MSK::Configuration" => self.create_msk_configuration(resource),
            "AWS::MSK::ClusterPolicy" => self.create_msk_cluster_policy(resource),
            "AWS::MSK::BatchScramSecret" => self.create_msk_batch_scram_secret(resource),
            "AWS::MSK::VpcConnection" => self.create_msk_vpc_connection(resource),
            "AWS::MSK::Replicator" => self.create_msk_replicator(resource),
            "AWS::KinesisAnalyticsV2::Application" => self.create_ka2_application(resource),
            "AWS::KinesisAnalyticsV2::ApplicationOutput" => {
                self.create_ka2_application_output(resource)
            }
            "AWS::KinesisAnalyticsV2::ApplicationReferenceDataSource" => {
                self.create_ka2_reference_data_source(resource)
            }
            "AWS::KinesisAnalyticsV2::ApplicationCloudWatchLoggingOption" => {
                self.create_ka2_cloudwatch_logging_option(resource)
            }
            "AWS::CodeCommit::Repository" => self.create_codecommit_repository(resource),
            "AWS::EFS::FileSystem" => self.create_efs_file_system(resource),
            "AWS::EFS::MountTarget" => self.create_efs_mount_target(resource),
            "AWS::EFS::AccessPoint" => self.create_efs_access_point(resource),
            "AWS::ElasticBeanstalk::Application" => self.create_eb_application(resource),
            "AWS::ElasticBeanstalk::ApplicationVersion" => {
                self.create_eb_application_version(resource)
            }
            "AWS::ElasticBeanstalk::Environment" => self.create_eb_environment(resource),
            "AWS::ElasticBeanstalk::ConfigurationTemplate" => {
                self.create_eb_configuration_template(resource)
            }
            "AWS::EC2::VPC" => self.create_ec2_vpc(resource),
            "AWS::EC2::Instance" => self.create_ec2_instance(resource),
            "AWS::EC2::Subnet" => self.create_ec2_subnet(resource),
            "AWS::EC2::SecurityGroup" => self.create_ec2_security_group(resource),
            "AWS::EC2::InternetGateway" => self.create_ec2_internet_gateway(resource),
            "AWS::EC2::RouteTable" => self.create_ec2_route_table(resource),
            "AWS::ECS::Cluster" => self.create_ecs_cluster(resource),
            "AWS::ECS::TaskDefinition" => self.create_ecs_task_definition(resource),
            "AWS::ECS::Service" => self.create_ecs_service(resource),
            "AWS::ECS::CapacityProvider" => self.create_ecs_capacity_provider(resource),
            "AWS::CertificateManager::Certificate" => self.create_acm_certificate(resource),
            "AWS::CertificateManager::Account" => self.create_acm_account(resource),
            "AWS::ACMPCA::CertificateAuthority" => {
                self.create_acmpca_certificate_authority(resource)
            }
            "AWS::ACMPCA::Certificate" => self.create_acmpca_certificate(resource),
            "AWS::ACMPCA::CertificateAuthorityActivation" => {
                self.create_acmpca_certificate_authority_activation(resource)
            }
            "AWS::ACMPCA::Permission" => self.create_acmpca_permission(resource),
            "AWS::Route53Resolver::ResolverEndpoint" => {
                self.create_r53r_resolver_endpoint(resource)
            }
            "AWS::Route53Resolver::ResolverRule" => self.create_r53r_resolver_rule(resource),
            "AWS::Route53Resolver::ResolverRuleAssociation" => {
                self.create_r53r_rule_association(resource)
            }
            "AWS::Route53Resolver::ResolverQueryLoggingConfig" => {
                self.create_r53r_query_log_config(resource)
            }
            "AWS::Route53Resolver::ResolverQueryLoggingConfigAssociation" => {
                self.create_r53r_query_log_association(resource)
            }
            "AWS::Route53Resolver::FirewallDomainList" => {
                self.create_r53r_firewall_domain_list(resource)
            }
            "AWS::Route53Resolver::FirewallRuleGroup" => {
                self.create_r53r_firewall_rule_group(resource)
            }
            "AWS::Route53Resolver::FirewallRuleGroupAssociation" => {
                self.create_r53r_firewall_rule_group_association(resource)
            }
            "AWS::Route53Resolver::FirewallConfig" => self.create_r53r_firewall_config(resource),
            "AWS::Route53Resolver::ResolverConfig" => self.create_r53r_resolver_config(resource),
            "AWS::Route53Resolver::ResolverDNSSECConfig" => {
                self.create_r53r_dnssec_config(resource)
            }
            "AWS::Config::ConfigurationRecorder"
            | "AWS::Config::DeliveryChannel"
            | "AWS::Config::ConfigRule"
            | "AWS::Config::ConfigurationAggregator"
            | "AWS::Config::AggregationAuthorization"
            | "AWS::Config::ConformancePack"
            | "AWS::Config::OrganizationConfigRule" => self.create_config_resource(resource),
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
            "AWS::EKS::Cluster" => self.create_eks_cluster(resource),
            "AWS::EKS::Nodegroup" => self.create_eks_nodegroup(resource),
            "AWS::EKS::FargateProfile" => self.create_eks_fargate_profile(resource),
            "AWS::EKS::Addon" => self.create_eks_addon(resource),
            "AWS::EKS::AccessEntry" => self.create_eks_access_entry(resource),
            "AWS::EKS::IdentityProviderConfig" => {
                self.create_eks_identity_provider_config(resource)
            }
            "AWS::EKS::PodIdentityAssociation" => {
                self.create_eks_pod_identity_association(resource)
            }
            "AWS::ServiceDiscovery::HttpNamespace" => self.create_sd_http_namespace(resource),
            "AWS::ServiceDiscovery::PublicDnsNamespace" => {
                self.create_sd_public_dns_namespace(resource)
            }
            "AWS::ServiceDiscovery::PrivateDnsNamespace" => {
                self.create_sd_private_dns_namespace(resource)
            }
            "AWS::ServiceDiscovery::Service" => self.create_sd_service(resource),
            "AWS::ServiceDiscovery::Instance" => self.create_sd_instance(resource),
            "AWS::CloudFormation::Stack" => self.create_cloudformation_stack(resource),
            "AWS::Glue::Table" => self.create_glue_table(resource),
            "AWS::Glue::Partition" => self.create_glue_partition(resource),
            "AWS::CodePipeline::Pipeline" => self.create_codepipeline_pipeline(resource),
            "AWS::CodeBuild::Project" => self.create_codebuild_project(resource),
            "AWS::CodeDeploy::Application" => self.create_codedeploy_application(resource),
            "AWS::CodeDeploy::DeploymentGroup" => self.create_codedeploy_deployment_group(resource),
            "AWS::OpenSearchService::Domain" => self.create_opensearch_domain(resource),
            "AWS::Elasticsearch::Domain" => self.create_elasticsearch_domain(resource),
            "AWS::EMR::Cluster" => self.create_emr_cluster(resource),
            "AWS::SageMaker::Model" => self.create_sagemaker_model(resource),
            "AWS::SageMaker::EndpointConfig" => self.create_sagemaker_endpoint_config(resource),
            "AWS::SageMaker::Endpoint" => self.create_sagemaker_endpoint(resource),
            "AWS::SageMaker::NotebookInstance" => self.create_sagemaker_notebook_instance(resource),
            "AWS::Backup::BackupVault" => self.create_backup_vault(resource),
            "AWS::Backup::BackupPlan" => self.create_backup_plan(resource),
            "AWS::AppSync::GraphQLApi" => self.create_appsync_graphql_api(resource),
            "AWS::AppSync::DataSource" => self.create_appsync_data_source(resource),
            "AWS::AppSync::Resolver" => self.create_appsync_resolver(resource),
            "AWS::Timestream::Database" => self.create_timestream_database(resource),
            "AWS::Timestream::Table" => self.create_timestream_table(resource),
            "AWS::MWAA::Environment" => self.create_mwaa_environment(resource),
            "AWS::Amplify::App" => self.create_amplify_app(resource),
            "AWS::AppConfig::Application" => self.create_appconfig_application(resource),
            "AWS::AppConfig::Environment" => self.create_appconfig_environment(resource),
            "AWS::AppConfig::ConfigurationProfile" => {
                self.create_appconfig_configuration_profile(resource)
            }
            t if t.starts_with("Custom::") || t == "AWS::CloudFormation::CustomResource" => self
                .create_custom_resource(resource)
                .map(ProvisionResult::new),
            other if self.strict_unknown_types => {
                // Cloud Control API path: reject a resource type fakecloud has
                // no provisioner for instead of recording a phantom resource.
                // Otherwise `GetResource`/`ListResources` would report a
                // resource whose owning service never created any backing state.
                return Err(format!(
                    "Resource type '{other}' is not supported by Cloud Control API on fakecloud."
                ));
            }
            other => {
                // No provisioner for this type. Real CloudFormation provisions
                // many resource types fakecloud doesn't model, and SAM/CDK
                // output routinely includes ones like
                // `AWS::CloudFormation::WaitConditionHandle`; failing the whole
                // stack on each would block otherwise-valid templates. Match the
                // documented contract (docs/services/cloudformation.md): accept
                // and record the resource as provisioned with no backing state.
                // Dependent operations that need real state may still fail —
                // that is the honest outcome. The physical id is the logical id
                // so `Ref` on the resource resolves to a stable value.
                tracing::warn!(
                    resource_type = %other,
                    logical_id = %resource.logical_id,
                    "CloudFormation: no provisioner for resource type; recording it as provisioned with no backing state"
                );
                Ok(ProvisionResult::new(resource.logical_id.clone()))
            }
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
            "AWS::IAM::Role" => Some(self.update_iam_role(existing, new_def)?),
            "AWS::IAM::Policy" => Some(self.update_iam_policy(existing, new_def)?),
            "AWS::IAM::ManagedPolicy" => Some(self.update_iam_policy(existing, new_def)?),
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
            "AWS::StepFunctions::StateMachine" => {
                Some(self.update_sfn_state_machine(existing, new_def)?)
            }
            "AWS::SQS::Queue" => Some(self.update_sqs_queue(existing, new_def)?),
            "AWS::SQS::QueuePolicy" => Some(self.update_sqs_queue_policy(existing, new_def)?),
            "AWS::SNS::Topic" => Some(self.update_sns_topic(existing, new_def)?),
            "AWS::SNS::TopicPolicy" => Some(self.update_sns_topic_policy(existing, new_def)?),
            "AWS::SNS::Subscription" => Some(self.update_sns_subscription(existing, new_def)?),
            "AWS::SSM::Parameter" => Some(self.update_ssm_parameter(existing, new_def)?),
            "AWS::Logs::LogGroup" => Some(self.update_log_group(existing, new_def)?),
            "AWS::Kinesis::Stream" => Some(self.update_kinesis_stream(existing, new_def)?),
            "AWS::Events::Rule" => Some(self.update_eventbridge_rule(existing, new_def)?),
            "AWS::DynamoDB::Table" => Some(self.update_dynamodb_table(existing, new_def)?),
            "AWS::SecretsManager::Secret" => {
                Some(self.update_secrets_manager_secret(existing, new_def)?)
            }
            "AWS::Cognito::UserPool" => Some(self.update_cognito_user_pool(existing, new_def)?),
            "AWS::Cognito::UserPoolClient" => {
                Some(self.update_cognito_user_pool_client(existing, new_def)?)
            }
            "AWS::RDS::DBInstance" => Some(self.update_rds_db_instance(existing, new_def)?),
            "AWS::S3::Bucket" => Some(self.update_s3_bucket(existing, new_def)?),
            "AWS::S3::BucketPolicy" => Some(self.update_s3_bucket_policy(existing, new_def)?),
            "AWS::Pipes::Pipe" => Some(self.update_pipes_pipe(existing, new_def)?),
            "AWS::CodeArtifact::Domain" => {
                Some(self.update_codeartifact_domain(existing, new_def)?)
            }
            "AWS::CodeArtifact::Repository" => {
                Some(self.update_codeartifact_repository(existing, new_def)?)
            }
            "AWS::AmazonMQ::Broker" => Some(self.update_mq_broker(existing, new_def)?),
            "AWS::AmazonMQ::Configuration" => {
                Some(self.update_mq_configuration(existing, new_def)?)
            }
            "AWS::AmazonMQ::ConfigurationAssociation" => {
                Some(self.create_mq_configuration_association(new_def)?)
            }
            "AWS::MSK::Cluster" => Some(self.update_msk_cluster(existing, new_def)?),
            "AWS::MSK::ServerlessCluster" => Some(self.update_msk_cluster(existing, new_def)?),
            "AWS::MSK::Configuration" => Some(self.update_msk_configuration(existing, new_def)?),
            "AWS::MSK::ClusterPolicy" => Some(self.update_msk_cluster_policy(existing, new_def)?),
            "AWS::MSK::BatchScramSecret" => {
                Some(self.update_msk_batch_scram_secret(existing, new_def)?)
            }
            "AWS::MSK::VpcConnection" => Some(self.update_msk_vpc_connection(existing, new_def)?),
            "AWS::MSK::Replicator" => Some(self.update_msk_replicator(existing, new_def)?),
            "AWS::KinesisAnalyticsV2::Application" => {
                Some(self.update_ka2_application(existing, new_def)?)
            }
            "AWS::KinesisAnalyticsV2::ApplicationOutput" => {
                Some(self.update_ka2_application_output(existing, new_def)?)
            }
            "AWS::KinesisAnalyticsV2::ApplicationReferenceDataSource" => {
                Some(self.update_ka2_reference_data_source(existing, new_def)?)
            }
            "AWS::KinesisAnalyticsV2::ApplicationCloudWatchLoggingOption" => {
                Some(self.update_ka2_cloudwatch_logging_option(existing, new_def)?)
            }
            "AWS::CodeCommit::Repository" => {
                Some(self.update_codecommit_repository(existing, new_def)?)
            }
            "AWS::EFS::FileSystem" => Some(self.update_efs_file_system(existing, new_def)?),
            "AWS::EFS::MountTarget" => Some(self.update_efs_mount_target(existing, new_def)?),
            "AWS::EFS::AccessPoint" => Some(self.update_efs_access_point(existing, new_def)?),
            "AWS::ElasticBeanstalk::Application" => {
                Some(self.update_eb_application(existing, new_def)?)
            }
            "AWS::ElasticBeanstalk::ApplicationVersion" => {
                Some(self.update_eb_application_version(existing, new_def)?)
            }
            "AWS::ElasticBeanstalk::Environment" => {
                Some(self.update_eb_environment(existing, new_def)?)
            }
            "AWS::ElasticBeanstalk::ConfigurationTemplate" => {
                Some(self.update_eb_configuration_template(existing, new_def)?)
            }
            // No dedicated in-place update arm for this type. Real
            // CloudFormation, lacking an in-place mutator for a changed
            // property, falls back to REPLACEMENT: it deletes the old backing
            // resource and creates a new one from the updated definition. Do
            // the same here so the update actually applies (the owning
            // service's stored resource reflects the new properties) instead
            // of silently reporting UPDATE_COMPLETE while the old config
            // lingers. See `reprovision_resource`.
            _ => self.reprovision_resource(existing, new_def)?,
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

    /// Fallback update path for resource types that have create/delete
    /// provisioners but no dedicated in-place `update_*` arm.
    ///
    /// Before this existed, `update_resource` returned `Ok(None)` for every
    /// such type and the update driver treated that as "leave the resource
    /// untouched" while still transitioning the stack to `UPDATE_COMPLETE`
    /// and recording no `ResourceChange` -- so `update-stack` / `cdk deploy`
    /// changing a property on any of the many write-through services
    /// (AppConfig, Timestream, OpenSearch, EMR, Glue, RDS DBCluster, ...)
    /// reported success while the backing resource kept its old config.
    ///
    /// Mirroring CloudFormation replacement semantics, this tears down the
    /// old backing resource and re-provisions it from the new definition
    /// through the exact same write-through path `create_resource` uses, so
    /// the stored resource reflects the update. For name-keyed services the
    /// physical id is derived from an unchanged `Name`/`Id` property and stays
    /// stable (an effectively in-place update); for others it is regenerated,
    /// matching replacement. Genuinely-unmodeled types (no backing state) have
    /// a no-op delete and a create that records `physical_id == logical_id`;
    /// they still yield `Some(..)` here so the caller records a
    /// `ResourceChange` rather than a silent no-op.
    fn reprovision_resource(
        &self,
        existing: &StackResource,
        new_def: &ResourceDefinition,
    ) -> Result<Option<ProvisionResult>, String> {
        self.delete_resource(existing)?;
        let created = self.create_resource(new_def)?;
        Ok(Some(ProvisionResult {
            physical_id: created.physical_id,
            attributes: created.attributes,
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
        // Amazon MQ broker endpoint / IP attributes are the ONE captured set that
        // legitimately goes stale: at create time the backing container is not up
        // yet, so they are the cosmetic `*.amazonaws.com` synthesis, but they
        // become the REAL bound `tcp://127.0.0.1:<port>` once the container
        // settles. Resolve them LIVE so CFN GetAtt / outputs match what the direct
        // DescribeBroker returns rather than serving the frozen cosmetic values.
        const MQ_BROKER_LIVE_ATTRS: &[&str] = &[
            "IpAddresses",
            "OpenWireEndpoints",
            "AmqpEndpoints",
            "StompEndpoints",
            "MqttEndpoints",
            "WssEndpoints",
        ];
        if resource.resource_type == "AWS::AmazonMQ::Broker"
            && MQ_BROKER_LIVE_ATTRS.contains(&attribute)
        {
            if let Some(v) = self.get_att_mq_broker(&resource.physical_id, attribute) {
                return Some(v);
            }
        }
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
            "AWS::EC2::VPC"
            | "AWS::EC2::Subnet"
            | "AWS::EC2::SecurityGroup"
            | "AWS::EC2::InternetGateway"
            | "AWS::EC2::Instance"
            | "AWS::EC2::RouteTable" => self.get_att_ec2(resource, attribute),
            "AWS::ECS::CapacityProvider" => {
                self.get_att_ecs_capacity_provider(&resource.physical_id, attribute)
            }
            "AWS::ECR::Repository" => self.get_att_ecr_repository(&resource.physical_id, attribute),
            "AWS::Redshift::Cluster" => {
                self.get_att_redshift_cluster(&resource.physical_id, attribute)
            }
            "AWS::DocDB::DBCluster" => self.get_att_docdb_cluster(&resource.physical_id, attribute),
            "AWS::Neptune::DBCluster" => {
                self.get_att_neptune_cluster(&resource.physical_id, attribute)
            }
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
            "AWS::Pipes::Pipe" => self.get_att_pipes_pipe(&resource.physical_id, attribute),
            "AWS::CodeArtifact::Domain" => {
                self.get_att_codeartifact_domain(&resource.physical_id, attribute)
            }
            "AWS::CodeArtifact::Repository" => {
                self.get_att_codeartifact_repository(&resource.physical_id, attribute)
            }
            "AWS::AmazonMQ::Broker" => self.get_att_mq_broker(&resource.physical_id, attribute),
            "AWS::AmazonMQ::Configuration" => {
                self.get_att_mq_configuration(&resource.physical_id, attribute)
            }
            "AWS::MSK::Cluster" | "AWS::MSK::ServerlessCluster" => {
                self.get_att_msk_cluster(&resource.physical_id, attribute)
            }
            "AWS::MSK::Configuration" => {
                self.get_att_msk_configuration(&resource.physical_id, attribute)
            }
            "AWS::MSK::ClusterPolicy" => {
                self.get_att_msk_cluster_policy(&resource.physical_id, attribute)
            }
            "AWS::MSK::VpcConnection" => {
                self.get_att_msk_vpc_connection(&resource.physical_id, attribute)
            }
            "AWS::MSK::Replicator" => self.get_att_msk_replicator(&resource.physical_id, attribute),
            "AWS::CodeCommit::Repository" => {
                self.get_att_codecommit_repository(&resource.physical_id, attribute)
            }
            "AWS::EFS::FileSystem" => {
                self.get_att_efs_file_system(&resource.physical_id, attribute)
            }
            "AWS::EFS::MountTarget" => {
                self.get_att_efs_mount_target(&resource.physical_id, attribute)
            }
            "AWS::EFS::AccessPoint" => {
                self.get_att_efs_access_point(&resource.physical_id, attribute)
            }
            "AWS::ElasticBeanstalk::Environment" => {
                self.get_att_eb_environment(&resource.physical_id, attribute)
            }
            "AWS::EKS::Cluster" => self.get_att_eks_cluster(&resource.physical_id, attribute),
            "AWS::EKS::Nodegroup" => self.get_att_eks_nodegroup(&resource.physical_id, attribute),
            "AWS::EKS::FargateProfile" => {
                self.get_att_eks_fargate_profile(&resource.physical_id, attribute)
            }
            "AWS::EKS::Addon" => self.get_att_eks_addon(&resource.physical_id, attribute),
            "AWS::EKS::AccessEntry" => {
                self.get_att_eks_access_entry(&resource.physical_id, attribute)
            }
            "AWS::EKS::IdentityProviderConfig" => {
                self.get_att_eks_identity_provider_config(&resource.physical_id, attribute)
            }
            "AWS::EKS::PodIdentityAssociation" => {
                self.get_att_eks_pod_identity_association(&resource.physical_id, attribute)
            }
            "AWS::ServiceDiscovery::HttpNamespace"
            | "AWS::ServiceDiscovery::PublicDnsNamespace"
            | "AWS::ServiceDiscovery::PrivateDnsNamespace" => {
                self.get_att_sd_namespace(&resource.physical_id, attribute)
            }
            "AWS::ServiceDiscovery::Service" => {
                self.get_att_sd_service(&resource.physical_id, attribute)
            }
            "AWS::CodePipeline::Pipeline" => {
                self.get_att_codepipeline_pipeline(&resource.physical_id, attribute)
            }
            "AWS::CodeBuild::Project" => {
                self.get_att_codebuild_project(&resource.physical_id, attribute)
            }
            "AWS::CodeDeploy::DeploymentGroup" => {
                self.get_att_codedeploy_deployment_group(&resource.physical_id, attribute)
            }
            "AWS::OpenSearchService::Domain" | "AWS::Elasticsearch::Domain" => {
                self.get_att_opensearch_domain(&resource.physical_id, attribute)
            }
            "AWS::EMR::Cluster" => self.get_att_emr_cluster(&resource.physical_id, attribute),
            "AWS::SageMaker::Model" => {
                self.get_att_sagemaker_resource("Model", &resource.physical_id, attribute)
            }
            "AWS::SageMaker::EndpointConfig" => {
                self.get_att_sagemaker_resource("EndpointConfig", &resource.physical_id, attribute)
            }
            "AWS::SageMaker::Endpoint" => {
                self.get_att_sagemaker_resource("Endpoint", &resource.physical_id, attribute)
            }
            "AWS::SageMaker::NotebookInstance" => self.get_att_sagemaker_resource(
                "NotebookInstance",
                &resource.physical_id,
                attribute,
            ),
            "AWS::Backup::BackupVault" => {
                self.get_att_backup_vault(&resource.physical_id, attribute)
            }
            "AWS::Backup::BackupPlan" => self.get_att_backup_plan(&resource.physical_id, attribute),
            "AWS::AppSync::GraphQLApi" => {
                self.get_att_appsync_graphql_api(&resource.physical_id, attribute)
            }
            "AWS::AppSync::DataSource" => {
                self.get_att_appsync_data_source(&resource.physical_id, attribute)
            }
            "AWS::AppSync::Resolver" => {
                self.get_att_appsync_resolver(&resource.physical_id, attribute)
            }
            "AWS::Timestream::Database" => {
                self.get_att_timestream_database(&resource.physical_id, attribute)
            }
            "AWS::Timestream::Table" => {
                self.get_att_timestream_table(&resource.physical_id, attribute)
            }
            "AWS::MWAA::Environment" => {
                self.get_att_mwaa_environment(&resource.physical_id, attribute)
            }
            "AWS::Amplify::App" => self.get_att_amplify_app(&resource.physical_id, attribute),
            "AWS::AppConfig::Environment" => {
                self.get_att_appconfig_environment(&resource.physical_id, attribute)
            }
            "AWS::AppConfig::ConfigurationProfile" => {
                self.get_att_appconfig_configuration_profile(&resource.physical_id, attribute)
            }
            t if t.starts_with("AWS::Route53Resolver::") => {
                self.get_att_route53resolver(resource, attribute)
            }
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
            "AWS::SQS::QueuePolicy" => self.delete_sqs_queue_policy(&resource.physical_id),
            "AWS::SNS::Topic" => self.delete_sns_topic(&resource.physical_id),
            "AWS::SNS::TopicPolicy" => self.delete_sns_topic_policy(&resource.physical_id),
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
            "AWS::S3::BucketPolicy" => self.delete_s3_bucket_policy(&resource.physical_id),
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
            "AWS::Redshift::Cluster" => self.delete_redshift_cluster(&resource.physical_id),
            "AWS::DocDB::DBCluster" => self.delete_docdb_cluster(&resource.physical_id),
            "AWS::Neptune::DBCluster" => self.delete_neptune_cluster(&resource.physical_id),
            "AWS::EC2::Instance" => {
                // Queue terminating the REAL instance + reaping its backing
                // container so the stack delete drain doesn't leak an EC2
                // container.
                self.pending_container_teardowns.lock().push(
                    ContainerTeardownIntent::Ec2Instance {
                        instance_id: resource.physical_id.clone(),
                    },
                );
                Ok(())
            }
            "AWS::EC2::VPC"
            | "AWS::EC2::Subnet"
            | "AWS::EC2::SecurityGroup"
            | "AWS::EC2::InternetGateway"
            | "AWS::EC2::RouteTable" => {
                self.delete_ec2_resource(&resource.resource_type, &resource.physical_id)
            }
            "AWS::AutoScaling::LaunchConfiguration" | "AWS::AutoScaling::AutoScalingGroup" => {
                self.delete_autoscaling(&resource.resource_type, &resource.physical_id);
                Ok(())
            }
            "AWS::Batch::ComputeEnvironment"
            | "AWS::Batch::JobQueue"
            | "AWS::Batch::JobDefinition"
            | "AWS::Batch::SchedulingPolicy" => {
                self.delete_batch(&resource.resource_type, &resource.physical_id);
                Ok(())
            }
            "AWS::Pipes::Pipe" => {
                self.delete_pipes_pipe(&resource.physical_id);
                Ok(())
            }
            "AWS::CodeArtifact::Domain" => {
                self.delete_codeartifact_domain(&resource.physical_id);
                Ok(())
            }
            "AWS::CodeArtifact::Repository" => {
                self.delete_codeartifact_repository(&resource.physical_id);
                Ok(())
            }
            "AWS::AmazonMQ::Broker" => {
                self.delete_mq_broker(&resource.physical_id);
                Ok(())
            }
            "AWS::AmazonMQ::Configuration" => {
                self.delete_mq_configuration(&resource.physical_id);
                Ok(())
            }
            "AWS::AmazonMQ::ConfigurationAssociation" => Ok(()),
            "AWS::MSK::Cluster" | "AWS::MSK::ServerlessCluster" => {
                self.delete_msk_cluster(&resource.physical_id);
                Ok(())
            }
            "AWS::MSK::Configuration" => {
                self.delete_msk_configuration(&resource.physical_id);
                Ok(())
            }
            "AWS::MSK::ClusterPolicy" => {
                self.delete_msk_cluster_policy(&resource.physical_id);
                Ok(())
            }
            "AWS::MSK::BatchScramSecret" => {
                self.delete_msk_batch_scram_secret(&resource.physical_id);
                Ok(())
            }
            "AWS::MSK::VpcConnection" => {
                self.delete_msk_vpc_connection(&resource.physical_id);
                Ok(())
            }
            "AWS::MSK::Replicator" => {
                self.delete_msk_replicator(&resource.physical_id);
                Ok(())
            }
            "AWS::KinesisAnalyticsV2::Application" => {
                self.delete_ka2_application(&resource.physical_id);
                Ok(())
            }
            "AWS::KinesisAnalyticsV2::ApplicationOutput" => {
                self.delete_ka2_application_output(resource);
                Ok(())
            }
            "AWS::KinesisAnalyticsV2::ApplicationReferenceDataSource" => {
                self.delete_ka2_reference_data_source(resource);
                Ok(())
            }
            "AWS::KinesisAnalyticsV2::ApplicationCloudWatchLoggingOption" => {
                self.delete_ka2_cloudwatch_logging_option(resource);
                Ok(())
            }
            "AWS::CodeCommit::Repository" => {
                self.delete_codecommit_repository(&resource.physical_id);
                Ok(())
            }
            "AWS::EFS::FileSystem" => {
                self.delete_efs_file_system(&resource.physical_id);
                Ok(())
            }
            "AWS::EFS::MountTarget" => {
                self.delete_efs_mount_target(&resource.physical_id);
                Ok(())
            }
            "AWS::EFS::AccessPoint" => {
                self.delete_efs_access_point(&resource.physical_id);
                Ok(())
            }
            "AWS::ElasticBeanstalk::Application" => {
                self.delete_eb_application(&resource.physical_id);
                Ok(())
            }
            "AWS::ElasticBeanstalk::ApplicationVersion" => {
                self.delete_eb_application_version(resource);
                Ok(())
            }
            "AWS::ElasticBeanstalk::Environment" => {
                self.delete_eb_environment(resource);
                Ok(())
            }
            "AWS::ElasticBeanstalk::ConfigurationTemplate" => {
                self.delete_eb_configuration_template(resource);
                Ok(())
            }
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
            "AWS::ACMPCA::CertificateAuthority" => {
                self.delete_acmpca_certificate_authority(&resource.physical_id)
            }
            "AWS::ACMPCA::Certificate" => Ok(()),
            "AWS::ACMPCA::CertificateAuthorityActivation" => Ok(()),
            "AWS::ACMPCA::Permission" => self.delete_acmpca_permission(&resource.physical_id),
            "AWS::Route53Resolver::ResolverEndpoint" => {
                self.delete_r53r_resolver_endpoint(&resource.physical_id)
            }
            "AWS::Route53Resolver::ResolverRule" => {
                self.delete_r53r_resolver_rule(&resource.physical_id)
            }
            "AWS::Route53Resolver::ResolverRuleAssociation" => {
                self.delete_r53r_rule_association(&resource.physical_id)
            }
            "AWS::Route53Resolver::ResolverQueryLoggingConfig" => {
                self.delete_r53r_query_log_config(&resource.physical_id)
            }
            "AWS::Route53Resolver::ResolverQueryLoggingConfigAssociation" => {
                self.delete_r53r_query_log_association(&resource.physical_id)
            }
            "AWS::Route53Resolver::FirewallDomainList" => {
                self.delete_r53r_firewall_domain_list(&resource.physical_id)
            }
            "AWS::Route53Resolver::FirewallRuleGroup" => {
                self.delete_r53r_firewall_rule_group(&resource.physical_id)
            }
            "AWS::Route53Resolver::FirewallRuleGroupAssociation" => {
                self.delete_r53r_firewall_rule_group_association(&resource.physical_id)
            }
            "AWS::Route53Resolver::FirewallConfig" => {
                self.delete_r53r_firewall_config(&resource.physical_id)
            }
            "AWS::Route53Resolver::ResolverConfig" => {
                self.delete_r53r_resolver_config(&resource.physical_id)
            }
            "AWS::Route53Resolver::ResolverDNSSECConfig" => {
                self.delete_r53r_dnssec_config(&resource.physical_id)
            }
            "AWS::Config::ConfigurationRecorder"
            | "AWS::Config::DeliveryChannel"
            | "AWS::Config::ConfigRule"
            | "AWS::Config::ConfigurationAggregator"
            | "AWS::Config::AggregationAuthorization"
            | "AWS::Config::ConformancePack"
            | "AWS::Config::OrganizationConfigRule" => {
                self.delete_config_resource(&resource.resource_type, &resource.physical_id)
            }
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
            "AWS::EKS::Cluster" => self.delete_eks_cluster(&resource.physical_id),
            "AWS::EKS::Nodegroup" => self.delete_eks_nodegroup(&resource.physical_id),
            "AWS::EKS::FargateProfile" => self.delete_eks_fargate_profile(&resource.physical_id),
            "AWS::EKS::Addon" => self.delete_eks_addon(&resource.physical_id),
            "AWS::EKS::AccessEntry" => self.delete_eks_access_entry(&resource.physical_id),
            "AWS::EKS::IdentityProviderConfig" => {
                self.delete_eks_identity_provider_config(&resource.physical_id)
            }
            "AWS::EKS::PodIdentityAssociation" => {
                self.delete_eks_pod_identity_association(&resource.physical_id)
            }
            "AWS::ServiceDiscovery::HttpNamespace"
            | "AWS::ServiceDiscovery::PublicDnsNamespace"
            | "AWS::ServiceDiscovery::PrivateDnsNamespace" => {
                self.delete_sd_namespace(&resource.physical_id)
            }
            "AWS::ServiceDiscovery::Service" => self.delete_sd_service(&resource.physical_id),
            "AWS::ServiceDiscovery::Instance" => {
                self.delete_sd_instance(&resource.physical_id, &resource.attributes)
            }
            "AWS::CodePipeline::Pipeline" => {
                self.delete_codepipeline_pipeline(&resource.physical_id)
            }
            "AWS::CodeBuild::Project" => self.delete_codebuild_project(&resource.physical_id),
            "AWS::CodeDeploy::Application" => {
                self.delete_codedeploy_application(&resource.physical_id)
            }
            "AWS::CodeDeploy::DeploymentGroup" => {
                self.delete_codedeploy_deployment_group(&resource.physical_id)
            }
            "AWS::OpenSearchService::Domain" | "AWS::Elasticsearch::Domain" => {
                self.delete_opensearch_domain(&resource.physical_id)
            }
            "AWS::EMR::Cluster" => self.delete_emr_cluster(&resource.physical_id),
            "AWS::SageMaker::Model" => {
                self.delete_sagemaker_resource("Model", &resource.physical_id)
            }
            "AWS::SageMaker::EndpointConfig" => {
                self.delete_sagemaker_resource("EndpointConfig", &resource.physical_id)
            }
            "AWS::SageMaker::Endpoint" => {
                self.delete_sagemaker_resource("Endpoint", &resource.physical_id)
            }
            "AWS::SageMaker::NotebookInstance" => {
                self.delete_sagemaker_resource("NotebookInstance", &resource.physical_id)
            }
            "AWS::Backup::BackupVault" => self.delete_backup_vault(&resource.physical_id),
            "AWS::Backup::BackupPlan" => self.delete_backup_plan(&resource.physical_id),
            "AWS::AppSync::GraphQLApi" => self.delete_appsync_graphql_api(&resource.physical_id),
            "AWS::AppSync::DataSource" => self.delete_appsync_data_source(&resource.physical_id),
            "AWS::AppSync::Resolver" => self.delete_appsync_resolver(&resource.physical_id),
            "AWS::Timestream::Database" => self.delete_timestream_database(&resource.physical_id),
            "AWS::Timestream::Table" => self.delete_timestream_table(&resource.physical_id),
            "AWS::MWAA::Environment" => self.delete_mwaa_environment(&resource.physical_id),
            "AWS::Amplify::App" => self.delete_amplify_app(&resource.physical_id),
            "AWS::AppConfig::Application" => {
                self.delete_appconfig_application(&resource.physical_id)
            }
            "AWS::AppConfig::Environment" => {
                self.delete_appconfig_environment(&resource.physical_id)
            }
            "AWS::AppConfig::ConfigurationProfile" => {
                self.delete_appconfig_configuration_profile(&resource.physical_id)
            }
            t if t.starts_with("Custom::") || t == "AWS::CloudFormation::CustomResource" => {
                self.delete_custom_resource(resource)
            }
            // No provisioner for this type — it was recorded with no backing
            // state at create time (see `create_resource`), so there is nothing
            // to delete. Succeed so stack teardown isn't blocked.
            _ => Ok(()),
        }
    }

    // --- CloudWatch Logs ---

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

    /// Read a specific object version's bytes. Used when a CFN property
    /// pins an S3 object `Version` (e.g.
    /// `DefinitionS3Location.Version`), so the exact referenced body is
    /// loaded rather than whatever is current. Falls through the current
    /// object and the version history.
    fn read_s3_object_version_bytes(
        &self,
        bucket: &str,
        key: &str,
        version_id: &str,
    ) -> Result<Vec<u8>, String> {
        let mut accounts = self.s3_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let body_ref = {
            let b = state
                .buckets
                .get(bucket)
                .ok_or_else(|| format!("S3 bucket {bucket} does not exist"))?;
            let from_current = b
                .objects
                .get(key)
                .filter(|o| o.version_id.as_deref() == Some(version_id))
                .map(|o| o.body.clone());
            from_current
                .or_else(|| {
                    b.object_versions.get(key).and_then(|versions| {
                        versions
                            .iter()
                            .find(|o| o.version_id.as_deref() == Some(version_id))
                            .map(|o| o.body.clone())
                    })
                })
                .ok_or_else(|| {
                    format!("S3 object s3://{bucket}/{key} version {version_id} does not exist")
                })?
        };
        state
            .read_body(&body_ref)
            .map(|b| b.to_vec())
            .map_err(|e| format!("S3 read failed: {e}"))
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

    // --- ELBv2 ---

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
        if self.defer_custom_invokes {
            // Changeset/update path: queue the invoke so the caller runs it off
            // the request path after the state write lock is dropped. The Lambda
            // response is not consumed for `GetAtt` (the physical id is generated
            // below regardless), so deferring loses nothing.
            self.pending_custom_invokes.lock().push(CustomInvokeIntent {
                service_token: service_token.to_string(),
                payload,
            });
        } else {
            self.invoke_lambda_sync(service_token, &payload)?;
        }

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

        if self.defer_custom_invokes {
            // Stack-delete / update-removed path: queue the Delete invoke so the
            // caller runs it off the request path after the lock is dropped.
            self.pending_custom_invokes.lock().push(CustomInvokeIntent {
                service_token,
                payload,
            });
        } else if let Err(e) = self.invoke_lambda_sync(&service_token, &payload) {
            // Best-effort: don't fail stack deletion if Lambda invocation fails
            tracing::warn!(
                "Custom resource delete Lambda invocation failed for {}: {e}",
                resource.logical_id
            );
        }
        Ok(())
    }

    // --- Application Auto Scaling ---

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
            http_redirect_from: None,
            http_redirect_to: None,
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
        processing_configuration: None,
        data_format_conversion_configuration: None,
        cloudwatch_logging_options: None,
        custom_time_zone: None,
        s3_backup_mode: None,
        file_extension: None,
        dynamic_partitioning_configuration: None,
        encryption_configuration: None,
        s3_backup_description: None,
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
            ec2_state: Arc::new(RwLock::new(
                fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
            )),
            autoscaling_state: Arc::new(RwLock::new(
                fakecloud_autoscaling::AutoScalingAccounts::new(),
            )),
            batch_state: Arc::new(RwLock::new(fakecloud_batch::BatchAccounts::new())),
            pipes_state: Arc::new(RwLock::new(fakecloud_pipes::PipesAccounts::new())),
            ecs_state: Arc::new(RwLock::new(
                fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
            )),
            acm_state: Arc::new(RwLock::new(fakecloud_acm::AcmAccounts::new())),
            acmpca_state: Arc::new(RwLock::new(fakecloud_acmpca::AcmPcaAccounts::new())),
            config_state: Arc::new(RwLock::new(fakecloud_config::ConfigAccounts::new())),
            route53resolver_state: Arc::new(RwLock::new(
                fakecloud_route53resolver::Route53ResolverAccounts::new(),
            )),
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
            eks_state: Arc::new(parking_lot::RwLock::new(
                fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
            )),
            servicediscovery_state: Arc::new(parking_lot::RwLock::new(
                fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
            )),
            codeartifact_state: Arc::new(parking_lot::RwLock::new(
                fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
            )),
            codecommit_state: Arc::new(parking_lot::RwLock::new(
                fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
            )),
            efs_state: Arc::new(parking_lot::RwLock::new(
                fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
            )),
            elasticbeanstalk_state: Arc::new(parking_lot::RwLock::new(
                fakecloud_elasticbeanstalk::EbAccounts::new(),
            )),
            mq_state: Arc::new(parking_lot::RwLock::new(
                fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
            )),
            kafka_state: Arc::new(parking_lot::RwLock::new(
                fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
            )),
            ka2_state: Arc::new(parking_lot::RwLock::new(
                fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
            )),
            redshift_state: Arc::new(parking_lot::RwLock::new(
                fakecloud_redshift::RedshiftAccounts::new(),
            )),
            docdb_state: Arc::new(parking_lot::RwLock::new(
                fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
            )),
            neptune_state: Arc::new(parking_lot::RwLock::new(
                fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
            )),
            codepipeline_state: Arc::new(parking_lot::RwLock::new(
                fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
            )),
            codebuild_state: Arc::new(parking_lot::RwLock::new(
                fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
            )),
            codedeploy_state: Arc::new(parking_lot::RwLock::new(
                fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
            )),
            opensearch_state: Arc::new(parking_lot::RwLock::new(
                fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
            )),
            emr_state: Arc::new(parking_lot::RwLock::new(
                fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
            )),
            sagemaker_state: Arc::new(parking_lot::RwLock::new(
                fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
            )),
            backup_state: Arc::new(parking_lot::RwLock::new(
                fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
            )),
            appsync_state: Arc::new(parking_lot::RwLock::new(
                fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
            )),
            timestream_state: Arc::new(parking_lot::RwLock::new(
                fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
            )),
            mwaa_state: Arc::new(parking_lot::RwLock::new(
                fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
            )),
            amplify_state: Arc::new(parking_lot::RwLock::new(
                fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
            )),
            appconfig_state: Arc::new(parking_lot::RwLock::new(
                fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
            )),
            delivery: Arc::new(DeliveryBus::new()),
            lambda_runtime: None,
            rds_runtime: None,
            ec2_runtime: None,
            ecs_runtime: None,
            elasticache_runtime: None,
            mq_runtime: None,
            kafka_runtime: None,
            pending_container_spawns: Arc::new(parking_lot::Mutex::new(Vec::new())),
            pending_container_teardowns: Arc::new(parking_lot::Mutex::new(Vec::new())),
            pending_custom_invokes: Arc::new(parking_lot::Mutex::new(Vec::new())),
            defer_custom_invokes: false,
            s3_store: Arc::new(fakecloud_persistence::s3::MemoryS3Store::new()),
            account_id: "123456789012".to_string(),
            region: "us-east-1".to_string(),
            stack_id: "arn:aws:cloudformation:us-east-1:123456789012:stack/test/00000000-0000-0000-0000-000000000000".to_string(),
            strict_unknown_types: false,
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
    fn cloudwatch_alarm_provisions_and_updates_metrics() {
        let prov = make_provisioner();
        let created = prov
            .create_resource(&make_resource(
                "AWS::CloudWatch::Alarm",
                "A",
                serde_json::json!({
                    "AlarmName": "math-alarm",
                    "ComparisonOperator": "GreaterThanUpperThreshold",
                    "EvaluationPeriods": 1,
                    "ThresholdMetricId": "ad1",
                    "Metrics": [
                        {"Id": "e1", "Expression": "m1", "Label": "expr", "ReturnData": true},
                        {"Id": "m1", "ReturnData": false, "MetricStat": {
                            "Metric": {
                                "Namespace": "AWS/EC2",
                                "MetricName": "CPUUtilization",
                                "Dimensions": [{"Name": "InstanceId", "Value": "i-123"}]
                            },
                            "Period": 300,
                            "Stat": "Average"
                        }}
                    ]
                }),
            ))
            .expect("alarm provisions");

        {
            let cw = prov.cloudwatch_state.read();
            let acct = cw.get("123456789012").unwrap();
            let alarm = acct
                .alarms_in("us-east-1")
                .unwrap()
                .get("math-alarm")
                .unwrap();
            assert_eq!(alarm.metrics.len(), 2, "Metrics parsed on create");
            assert_eq!(alarm.metrics[0].id, "e1");
            assert_eq!(alarm.metrics[0].expression.as_deref(), Some("m1"));
            assert_eq!(alarm.metrics[0].return_data, Some(true));
            assert_eq!(alarm.threshold_metric_id.as_deref(), Some("ad1"));
            let stat = alarm.metrics[1].metric_stat.as_ref().unwrap();
            assert_eq!(stat.metric_name.as_deref(), Some("CPUUtilization"));
            assert_eq!(
                stat.dimensions.get("InstanceId").map(String::as_str),
                Some("i-123")
            );
            assert_eq!(stat.stat.as_deref(), Some("Average"));
            assert_eq!(stat.period, Some(300));
        }

        // Update replaces the Metrics list AND refreshes ThresholdMetricId.
        prov.update_resource(
            &created,
            &make_resource(
                "AWS::CloudWatch::Alarm",
                "A",
                serde_json::json!({
                    "AlarmName": "math-alarm",
                    "ComparisonOperator": "GreaterThanUpperThreshold",
                    "EvaluationPeriods": 1,
                    "ThresholdMetricId": "ad2",
                    "Metrics": [{"Id": "e2", "Expression": "m1*2", "ReturnData": true}]
                }),
            ),
        )
        .expect("update succeeds")
        .expect("AWS::CloudWatch::Alarm is updatable");

        let cw = prov.cloudwatch_state.read();
        let acct = cw.get("123456789012").unwrap();
        let alarm = acct
            .alarms_in("us-east-1")
            .unwrap()
            .get("math-alarm")
            .unwrap();
        assert_eq!(alarm.metrics.len(), 1, "Metrics re-parsed on update");
        assert_eq!(alarm.metrics[0].id, "e2");
        assert_eq!(alarm.metrics[0].expression.as_deref(), Some("m1*2"));
        // ThresholdMetricId reflects the updated definition, not the stale one.
        assert_eq!(
            alarm.threshold_metric_id.as_deref(),
            Some("ad2"),
            "ThresholdMetricId refreshed on update"
        );
    }

    #[test]
    fn update_stack_reconciles_iam_role_policies() {
        let prov = make_provisioner();
        let created = prov
            .create_resource(&make_resource(
                "AWS::IAM::Role",
                "R",
                serde_json::json!({
                    "RoleName": "r1",
                    "AssumeRolePolicyDocument": {"Version": "2012-10-17"},
                    "ManagedPolicyArns": ["arn:aws:iam::aws:policy/ReadOnlyAccess"],
                }),
            ))
            .expect("role provisions");
        prov.update_resource(
            &created,
            &make_resource(
                "AWS::IAM::Role",
                "R",
                serde_json::json!({
                    "RoleName": "r1",
                    "AssumeRolePolicyDocument": {"Version": "2012-10-17", "Statement": []},
                    "Description": "updated",
                    "ManagedPolicyArns": ["arn:aws:iam::aws:policy/AdministratorAccess"],
                    "Policies": [{"PolicyName": "inline1", "PolicyDocument": {"k": "v"}}],
                }),
            ),
        )
        .expect("update succeeds")
        .expect("IAM::Role is updatable");
        let iam = prov.iam_state.read();
        let acct = iam.get("123456789012").unwrap();
        let role = acct.roles.get("r1").unwrap();
        assert_eq!(role.description.as_deref(), Some("updated"));
        assert!(role.assume_role_policy_document.contains("Statement"));
        // Attached managed policies replaced, not appended.
        let attached = acct.role_policies.get("r1").unwrap();
        assert_eq!(
            attached,
            &vec!["arn:aws:iam::aws:policy/AdministratorAccess".to_string()]
        );
        assert!(acct
            .role_inline_policies
            .get("r1")
            .unwrap()
            .contains_key("inline1"));
    }

    #[test]
    fn update_stack_bumps_managed_policy_version() {
        let prov = make_provisioner();
        let created = prov
            .create_resource(&make_resource(
                "AWS::IAM::ManagedPolicy",
                "P",
                serde_json::json!({
                    "ManagedPolicyName": "p1",
                    "PolicyDocument": {"Version": "2012-10-17", "Statement": [{"Effect": "Allow"}]},
                }),
            ))
            .expect("managed policy provisions");
        prov.update_resource(
            &created,
            &make_resource(
                "AWS::IAM::ManagedPolicy",
                "P",
                serde_json::json!({
                    "ManagedPolicyName": "p1",
                    "PolicyDocument": {"Version": "2012-10-17", "Statement": [{"Effect": "Deny"}]},
                }),
            ),
        )
        .expect("update succeeds")
        .expect("IAM::ManagedPolicy is updatable");
        let iam = prov.iam_state.read();
        let acct = iam.get("123456789012").unwrap();
        let policy = acct.policies.get(&created.physical_id).unwrap();
        assert_eq!(policy.versions.len(), 2);
        assert_eq!(policy.default_version_id, "v2");
        let default = policy.versions.iter().find(|v| v.is_default).unwrap();
        assert!(default.document.contains("Deny"));
    }

    fn pipe_props(name: &str, target: &str, description: Option<&str>) -> serde_json::Value {
        let mut m = serde_json::json!({
            "Name": name,
            "Source": "arn:aws:sqs:us-east-1:123456789012:src",
            "Target": target,
            "RoleArn": "arn:aws:iam::123456789012:role/pipe",
        });
        if let Some(d) = description {
            m["Description"] = serde_json::json!(d);
        }
        m
    }

    #[test]
    fn update_stack_applies_pipes_pipe_change() {
        let prov = make_provisioner();
        let created = prov
            .create_resource(&make_resource(
                "AWS::Pipes::Pipe",
                "P",
                pipe_props(
                    "my-pipe",
                    "arn:aws:sqs:us-east-1:123456789012:dst",
                    Some("v1"),
                ),
            ))
            .expect("pipe provisions");

        // UpdateStack on a Pipes resource previously fell through to `_ => None`,
        // so CFN reported UPDATE_COMPLETE while discarding the change.
        let updated = prov
            .update_resource(
                &created,
                &make_resource(
                    "AWS::Pipes::Pipe",
                    "P",
                    pipe_props(
                        "my-pipe",
                        "arn:aws:sqs:us-east-1:123456789012:dst2",
                        Some("v2"),
                    ),
                ),
            )
            .expect("update succeeds")
            .expect("Pipes::Pipe is updatable (not a silent no-op)");
        assert_eq!(updated.status, "UPDATE_COMPLETE");

        // The change is actually applied to the pipes service state.
        let pipes = prov.pipes_state.read();
        let pipe = pipes
            .get("123456789012")
            .unwrap()
            .pipes
            .get("my-pipe")
            .unwrap();
        assert_eq!(pipe["Description"], "v2");
        assert_eq!(pipe["Target"], "arn:aws:sqs:us-east-1:123456789012:dst2");
        // CFN provisioning is synchronous, so the pipe stays settled.
        assert_eq!(pipe["CurrentState"], "RUNNING");
    }

    #[test]
    fn update_stack_replaces_pipe_on_source_change() {
        // Source is replacement-required on AWS::Pipes::Pipe: a changed Source
        // must actually take effect (via delete+recreate), not be silently
        // dropped by an in-place update that only touches the mutable fields.
        let prov = make_provisioner();
        let created = prov
            .create_resource(&make_resource(
                "AWS::Pipes::Pipe",
                "P",
                serde_json::json!({
                    "Name": "src-pipe",
                    "Source": "arn:aws:sqs:us-east-1:123456789012:src-old",
                    "Target": "arn:aws:sqs:us-east-1:123456789012:dst",
                    "RoleArn": "arn:aws:iam::123456789012:role/pipe",
                }),
            ))
            .expect("pipe provisions");
        prov.update_resource(
            &created,
            &make_resource(
                "AWS::Pipes::Pipe",
                "P",
                serde_json::json!({
                    "Name": "src-pipe",
                    "Source": "arn:aws:sqs:us-east-1:123456789012:src-new",
                    "Target": "arn:aws:sqs:us-east-1:123456789012:dst",
                    "RoleArn": "arn:aws:iam::123456789012:role/pipe",
                }),
            ),
        )
        .expect("update succeeds")
        .expect("Pipes::Pipe is updatable");
        let pipes = prov.pipes_state.read();
        let acct = pipes.get("123456789012").unwrap();
        // Exactly one pipe (the recreate reused the same Name after deleting).
        assert_eq!(acct.pipes.len(), 1);
        let pipe = acct.pipes.get("src-pipe").unwrap();
        assert_eq!(
            pipe["Source"], "arn:aws:sqs:us-east-1:123456789012:src-new",
            "changed Source is applied via replacement, not silently dropped"
        );
    }

    #[test]
    fn update_stack_clears_omitted_pipe_field() {
        let prov = make_provisioner();
        let created = prov
            .create_resource(&make_resource(
                "AWS::Pipes::Pipe",
                "P",
                pipe_props(
                    "p2",
                    "arn:aws:sqs:us-east-1:123456789012:dst",
                    Some("drop-me"),
                ),
            ))
            .expect("pipe provisions");
        prov.update_resource(
            &created,
            &make_resource(
                "AWS::Pipes::Pipe",
                "P",
                pipe_props("p2", "arn:aws:sqs:us-east-1:123456789012:dst", None),
            ),
        )
        .expect("update succeeds")
        .expect("updatable");
        let pipes = prov.pipes_state.read();
        let pipe = pipes.get("123456789012").unwrap().pipes.get("p2").unwrap();
        assert!(
            pipe.get("Description").is_none(),
            "an omitted updatable field is cleared (full-replace semantics)"
        );
    }

    #[test]
    fn create_pipe_rejects_empty_required_field() {
        let prov = make_provisioner();
        let err = prov
            .create_resource(&make_resource(
                "AWS::Pipes::Pipe",
                "P",
                serde_json::json!({
                    "Name": "p3",
                    "Source": "",
                    "Target": "arn:aws:sqs:us-east-1:123456789012:dst",
                    "RoleArn": "arn:aws:iam::123456789012:role/pipe",
                }),
            ))
            .unwrap_err();
        assert!(err.contains("Source"), "empty Source rejected: {err}");
    }

    #[test]
    fn create_pipe_rejects_duplicate_name() {
        let prov = make_provisioner();
        let props = pipe_props("dup", "arn:aws:sqs:us-east-1:123456789012:dst", None);
        prov.create_resource(&make_resource("AWS::Pipes::Pipe", "P", props.clone()))
            .expect("first create ok");
        // A second resource with the same Name must conflict, not overwrite.
        let err = prov
            .create_resource(&make_resource("AWS::Pipes::Pipe", "P2", props))
            .unwrap_err();
        assert!(err.contains("already exists"), "duplicate rejected: {err}");
    }

    #[test]
    fn update_stack_applies_sqs_queue_property_change() {
        let prov = make_provisioner();
        let created = prov
            .create_resource(&make_resource(
                "AWS::SQS::Queue",
                "Q",
                serde_json::json!({ "QueueName": "q1", "VisibilityTimeout": "30" }),
            ))
            .expect("queue provisions");
        // UpdateStack changes a property; previously a no-op (`_ => None`).
        let updated = prov
            .update_resource(
                &created,
                &make_resource(
                    "AWS::SQS::Queue",
                    "Q",
                    serde_json::json!({ "QueueName": "q1", "VisibilityTimeout": "120" }),
                ),
            )
            .expect("update succeeds")
            .expect("SQS::Queue is an updatable type");
        assert_eq!(updated.physical_id, created.physical_id);
        let sqs = prov.sqs_state.read();
        let acct = sqs.get("123456789012").unwrap();
        let queue = acct.queues.get(&created.physical_id).unwrap();
        assert_eq!(
            queue
                .attributes
                .get("VisibilityTimeout")
                .map(String::as_str),
            Some("120")
        );
    }

    #[test]
    fn update_stack_applies_sns_topic_property_change() {
        let prov = make_provisioner();
        let created = prov
            .create_resource(&make_resource(
                "AWS::SNS::Topic",
                "T",
                serde_json::json!({ "TopicName": "t1", "DisplayName": "before" }),
            ))
            .expect("topic provisions");
        let updated = prov
            .update_resource(
                &created,
                &make_resource(
                    "AWS::SNS::Topic",
                    "T",
                    serde_json::json!({ "TopicName": "t1", "DisplayName": "after" }),
                ),
            )
            .expect("update succeeds")
            .expect("SNS::Topic is an updatable type");
        assert_eq!(updated.physical_id, created.physical_id);
        let sns = prov.sns_state.read();
        let acct = sns.get("123456789012").unwrap();
        let topic = acct.topics.get(&created.physical_id).unwrap();
        assert_eq!(
            topic.attributes.get("DisplayName").map(String::as_str),
            Some("after")
        );
    }

    #[test]
    fn ec2_vpc_subnet_provision_through_real_handlers() {
        let prov = make_provisioner();
        let vpc = prov
            .create_resource(&make_resource(
                "AWS::EC2::VPC",
                "Vpc",
                serde_json::json!({ "CidrBlock": "10.1.0.0/16" }),
            ))
            .expect("VPC provisions");
        assert!(
            vpc.physical_id.starts_with("vpc-"),
            "got {}",
            vpc.physical_id
        );
        // The real VPC handler ran: a VPC exists in EC2 state (with the default
        // SG/route-table/NACL the handler creates), not a recorded no-op.
        {
            let ec2 = prov.ec2_state.read();
            let acct = ec2.get("123456789012").unwrap();
            assert!(acct.vpcs.contains_key(&vpc.physical_id));
        }
        // GetAtt VpcId/CidrBlock resolve; Ref is the vpc id.
        assert_eq!(
            prov.get_att(&vpc, "VpcId").as_deref(),
            Some(vpc.physical_id.as_str())
        );
        assert_eq!(
            prov.get_att(&vpc, "CidrBlock").as_deref(),
            Some("10.1.0.0/16")
        );

        // A subnet referencing the VPC (Ref already resolved to the physical id).
        let subnet = prov
            .create_resource(&make_resource(
                "AWS::EC2::Subnet",
                "Subnet",
                serde_json::json!({ "VpcId": vpc.physical_id, "CidrBlock": "10.1.1.0/24" }),
            ))
            .expect("subnet provisions");
        assert!(subnet.physical_id.starts_with("subnet-"));
        {
            let ec2 = prov.ec2_state.read();
            let acct = ec2.get("123456789012").unwrap();
            assert!(acct.subnets.contains_key(&subnet.physical_id));
        }

        // Delete routes through the real handler.
        prov.delete_resource(&subnet).expect("subnet deletes");
        prov.delete_resource(&vpc).expect("vpc deletes");
    }

    #[test]
    fn unknown_resource_type_records_instead_of_failing() {
        let prov = make_provisioner();
        // A real AWS type fakecloud has no provisioner for. It must NOT fail
        // the stack — it's recorded as provisioned with no backing state, and
        // `Ref` (its physical id) resolves to a stable value (the logical id).
        let sr = prov
            .create_resource(&make_resource(
                "AWS::CloudFormation::WaitConditionHandle",
                "Handle",
                serde_json::json!({}),
            ))
            .expect("unknown resource type should record, not fail");
        assert_eq!(sr.physical_id, "Handle");
        assert_eq!(sr.status, "CREATE_COMPLETE");
        // Teardown of a recorded no-op resource also succeeds.
        prov.delete_resource(&sr)
            .expect("delete no-op should succeed");
    }

    #[test]
    fn ecr_repository_uri_uses_bound_endpoint_not_public_dns() {
        // A CFN-provisioned RepositoryUri must point at fakecloud's own registry
        // endpoint, not the public AWS DNS, so `docker push` against the GetAtt
        // RepositoryUri reaches it (bug-audit 2026-06-20, 1.4).
        let prov = make_provisioner();
        let sr = prov
            .create_resource(&make_resource(
                "AWS::ECR::Repository",
                "Repo",
                serde_json::json!({ "RepositoryName": "my-repo" }),
            ))
            .expect("ECR repo provisions");
        let uri = sr
            .attributes
            .get("RepositoryUri")
            .expect("RepositoryUri attribute");
        assert!(
            !uri.contains("amazonaws.com"),
            "RepositoryUri must not use the public ECR DNS, got {uri}"
        );
        assert!(uri.contains("my-repo"), "uri should name the repo: {uri}");
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
    fn sqs_queue_policy_stored_on_queue_and_cleared_on_delete() {
        let prov = make_provisioner();
        let queue = prov
            .create_resource(&make_resource(
                "AWS::SQS::Queue",
                "Q",
                serde_json::json!({"QueueName": "q1"}),
            ))
            .unwrap();
        // `Queues` carries the queue URL — what `Ref` resolves to.
        let policy = make_resource(
            "AWS::SQS::QueuePolicy",
            "QP",
            serde_json::json!({
                "Queues": [queue.physical_id.clone()],
                "PolicyDocument": {"Version": "2012-10-17", "Statement": [{
                    "Effect": "Allow",
                    "Principal": {"Service": "sns.amazonaws.com"},
                    "Action": "sqs:SendMessage",
                    "Resource": "*"
                }]}
            }),
        );
        let sr = prov.create_resource(&policy).unwrap();

        {
            let mut accounts = prov.sqs_state.write();
            let state = accounts.get_or_create(&prov.account_id);
            let stored = state.queues[&queue.physical_id]
                .attributes
                .get("Policy")
                .expect("policy stored on queue");
            assert!(stored.contains("sqs:SendMessage"));
        }

        prov.delete_resource(&sr).unwrap();
        {
            let mut accounts = prov.sqs_state.write();
            let state = accounts.get_or_create(&prov.account_id);
            assert!(!state.queues[&queue.physical_id]
                .attributes
                .contains_key("Policy"));
        }
    }

    #[test]
    fn sns_topic_policy_stored_on_topic_and_cleared_on_delete() {
        let prov = make_provisioner();
        let topic = prov
            .create_resource(&make_resource(
                "AWS::SNS::Topic",
                "T",
                serde_json::json!({"TopicName": "t1"}),
            ))
            .unwrap();
        let policy = make_resource(
            "AWS::SNS::TopicPolicy",
            "TP",
            serde_json::json!({
                "Topics": [topic.physical_id.clone()],
                "PolicyDocument": {"Version": "2012-10-17", "Statement": [{
                    "Effect": "Allow",
                    "Principal": {"Service": "events.amazonaws.com"},
                    "Action": "sns:Publish",
                    "Resource": "*"
                }]}
            }),
        );
        let sr = prov.create_resource(&policy).unwrap();

        {
            let mut accounts = prov.sns_state.write();
            let state = accounts.get_or_create(&prov.account_id);
            let stored = state.topics[&topic.physical_id]
                .attributes
                .get("Policy")
                .expect("policy stored on topic");
            assert!(stored.contains("sns:Publish"));
        }

        prov.delete_resource(&sr).unwrap();
        {
            let mut accounts = prov.sns_state.write();
            let state = accounts.get_or_create(&prov.account_id);
            assert!(!state.topics[&topic.physical_id]
                .attributes
                .contains_key("Policy"));
        }
    }

    #[test]
    fn s3_bucket_policy_stored_on_bucket_and_cleared_on_delete() {
        let prov = make_provisioner();
        let bucket = prov
            .create_resource(&make_resource(
                "AWS::S3::Bucket",
                "B",
                serde_json::json!({"BucketName": "b1"}),
            ))
            .unwrap();
        let policy = make_resource(
            "AWS::S3::BucketPolicy",
            "BP",
            serde_json::json!({
                "Bucket": bucket.physical_id.clone(),
                "PolicyDocument": {"Version": "2012-10-17", "Statement": [{
                    "Effect": "Allow",
                    "Principal": "*",
                    "Action": "s3:GetObject",
                    "Resource": "arn:aws:s3:::b1/*"
                }]}
            }),
        );
        let sr = prov.create_resource(&policy).unwrap();
        assert_eq!(sr.physical_id, "b1-policy");

        {
            let mut accounts = prov.s3_state.write();
            let state = accounts.get_or_create(&prov.account_id);
            let stored = state.buckets[&bucket.physical_id]
                .policy
                .as_ref()
                .expect("policy stored on bucket");
            assert!(stored.contains("s3:GetObject"));
        }

        prov.delete_resource(&sr).unwrap();
        {
            let mut accounts = prov.s3_state.write();
            let state = accounts.get_or_create(&prov.account_id);
            assert!(state.buckets[&bucket.physical_id].policy.is_none());
        }
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
    fn unsupported_resource_type_is_recorded_not_failed() {
        // An unmodelled type is recorded as provisioned (no backing state)
        // rather than failing the stack — see
        // `unknown_resource_type_records_instead_of_failing`.
        let prov = make_provisioner();
        let res = make_resource("AWS::NonExistent::Thing", "X", serde_json::json!({}));
        let sr = prov.create_resource(&res).unwrap();
        assert_eq!(sr.physical_id, "X");
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
    fn unsupported_resource_type_recorded_with_logical_id() {
        // Recorded, not failed; physical id is the logical id so `Ref` resolves.
        let prov = make_provisioner();
        let res = make_resource("AWS::FooBar::Thing", "X", serde_json::json!({}));
        let sr = prov.create_resource(&res).unwrap();
        assert_eq!(sr.physical_id, "X");
        assert_eq!(sr.status, "CREATE_COMPLETE");
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

    // ── S3 bucket config-property provisioning (bug-hunt 1.26) ──

    #[test]
    fn s3_bucket_provisions_all_config_properties() {
        let prov = make_provisioner();
        let bucket = make_resource(
            "AWS::S3::Bucket",
            "MyBucket",
            serde_json::json!({
                "BucketName": "protected-bucket",
                "VersioningConfiguration": { "Status": "Enabled" },
                "BucketEncryption": {
                    "ServerSideEncryptionConfiguration": [{
                        "ServerSideEncryptionByDefault": { "SSEAlgorithm": "AES256" }
                    }]
                },
                "PublicAccessBlockConfiguration": {
                    "BlockPublicAcls": true,
                    "BlockPublicPolicy": true,
                    "IgnorePublicAcls": true,
                    "RestrictPublicBuckets": true
                },
                "NotificationConfiguration": {
                    "QueueConfigurations": [{
                        "Event": "s3:ObjectCreated:*",
                        "Queue": "arn:aws:sqs:us-east-1:123456789012:q"
                    }],
                    "EventBridgeConfiguration": {}
                },
                "Tags": [{ "Key": "env", "Value": "prod" }]
            }),
        );
        let sr = prov.create_resource(&bucket).unwrap();
        assert_eq!(sr.physical_id, "protected-bucket");

        let s3 = prov.s3_state.read();
        let acct = s3.get("123456789012").unwrap();
        let b = acct.buckets.get("protected-bucket").unwrap();
        // GetBucketVersioning
        assert_eq!(b.versioning.as_deref(), Some("Enabled"));
        // GetBucketEncryption
        assert!(b
            .encryption_config
            .as_deref()
            .unwrap()
            .contains("<SSEAlgorithm>AES256</SSEAlgorithm>"));
        // GetPublicAccessBlock
        assert!(b
            .public_access_block
            .as_deref()
            .unwrap()
            .contains("<BlockPublicAcls>true</BlockPublicAcls>"));
        // GetBucketNotification + EventBridge
        assert!(b
            .notification_config
            .as_deref()
            .unwrap()
            .contains("<Queue>arn:aws:sqs:us-east-1:123456789012:q</Queue>"));
        assert!(b.eventbridge_enabled);
        // GetBucketTagging
        assert_eq!(b.tags.get("env").map(String::as_str), Some("prod"));
    }

    #[test]
    fn s3_bucket_update_enables_versioning() {
        let prov = make_provisioner();
        let created = prov
            .create_resource(&make_resource(
                "AWS::S3::Bucket",
                "MyBucket",
                serde_json::json!({ "BucketName": "later-versioned" }),
            ))
            .unwrap();
        // Freshly created: no versioning.
        {
            let s3 = prov.s3_state.read();
            let b = s3
                .get("123456789012")
                .unwrap()
                .buckets
                .get("later-versioned")
                .unwrap();
            assert!(b.versioning.is_none());
        }
        // A follow-up update turning on versioning must be applied, not a no-op.
        prov.update_resource(
            &created,
            &make_resource(
                "AWS::S3::Bucket",
                "MyBucket",
                serde_json::json!({
                    "BucketName": "later-versioned",
                    "VersioningConfiguration": { "Status": "Enabled" }
                }),
            ),
        )
        .unwrap();
        let s3 = prov.s3_state.read();
        let b = s3
            .get("123456789012")
            .unwrap()
            .buckets
            .get("later-versioned")
            .unwrap();
        assert_eq!(b.versioning.as_deref(), Some("Enabled"));
    }

    #[test]
    fn sqs_queue_provisions_tags() {
        let prov = make_provisioner();
        let created = prov
            .create_resource(&make_resource(
                "AWS::SQS::Queue",
                "TaggedQ",
                serde_json::json!({
                    "QueueName": "tagged-q",
                    "Tags": [
                        { "Key": "team", "Value": "core" },
                        { "Key": "env", "Value": "prod" }
                    ]
                }),
            ))
            .unwrap();
        {
            let sqs = prov.sqs_state.read();
            let q = sqs
                .get("123456789012")
                .unwrap()
                .queues
                .get(&created.physical_id)
                .unwrap();
            assert_eq!(q.tags.get("team").map(String::as_str), Some("core"));
            assert_eq!(q.tags.get("env").map(String::as_str), Some("prod"));
        }
        // Update replaces the tag set.
        prov.update_resource(
            &created,
            &make_resource(
                "AWS::SQS::Queue",
                "TaggedQ",
                serde_json::json!({
                    "QueueName": "tagged-q",
                    "Tags": [{ "Key": "team", "Value": "platform" }]
                }),
            ),
        )
        .unwrap();
        let sqs = prov.sqs_state.read();
        let q = sqs
            .get("123456789012")
            .unwrap()
            .queues
            .get(&created.physical_id)
            .unwrap();
        assert_eq!(q.tags.get("team").map(String::as_str), Some("platform"));
        assert!(!q.tags.contains_key("env"));
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
    fn getatt_mq_broker_endpoints_track_the_live_data_plane() {
        // An MQ broker's endpoint attributes are captured cosmetic at create
        // time (no container yet), but once a backing container binds they must
        // resolve to the REAL `tcp://`/`amqp://<host>:<port>` the direct
        // DescribeBroker returns -- CFN GetAtt is NOT frozen to the cosmetic set.
        let prov = make_provisioner();
        let created = prov
            .create_resource(&make_resource(
                "AWS::AmazonMQ::Broker",
                "MyBroker",
                serde_json::json!({
                    "BrokerName": "cfn-live-ep",
                    "EngineType": "ACTIVEMQ",
                    "HostInstanceType": "mq.m5.large",
                    "DeploymentMode": "SINGLE_INSTANCE",
                    "PubliclyAccessible": false
                }),
            ))
            .expect("create broker");
        let broker_id = created.physical_id.clone();
        // The captured attribute is the cosmetic amazonaws endpoint.
        assert!(
            created.attributes["AmqpEndpoints"].contains("amazonaws.com"),
            "create-time capture is cosmetic: {}",
            created.attributes["AmqpEndpoints"]
        );
        // Bind a live data plane, as settle_broker_up does once the container is up.
        {
            let mut g = prov.mq_state.write();
            let acct = g.get_or_create("123456789012");
            let mut ports = std::collections::BTreeMap::new();
            ports.insert("amqp".to_string(), 15672u16);
            ports.insert("openwire".to_string(), 15616u16);
            acct.data_plane.insert(
                broker_id.clone(),
                fakecloud_mq::BrokerDataPlane {
                    container_id: "c-live".to_string(),
                    host: "127.0.0.1".to_string(),
                    ports,
                },
            );
        }
        // GetAtt now returns the REAL bound endpoints, not the cosmetic capture.
        let amqp = prov
            .get_att(&created, "AmqpEndpoints")
            .expect("AmqpEndpoints");
        assert!(
            amqp.contains("amqp://127.0.0.1:15672") && !amqp.contains("amazonaws.com"),
            "GetAtt must project the live data plane, got: {amqp}"
        );
        let ips = prov.get_att(&created, "IpAddresses").expect("IpAddresses");
        assert!(ips.contains("127.0.0.1"), "IpAddresses live: {ips}");
        let openwire = prov
            .get_att(&created, "OpenWireEndpoints")
            .expect("OpenWireEndpoints");
        assert!(
            openwire.contains("tcp://127.0.0.1:15616"),
            "OpenWireEndpoints live: {openwire}"
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

    #[test]
    fn parse_lambda_function_name_handles_every_shape() {
        // Plain name, unqualified — passes through.
        assert_eq!(parse_lambda_function_name("my-func"), "my-func");
        // Bare name with an alias/version qualifier (what `Ref` on an
        // alias used to feed the ESM create path).
        assert_eq!(parse_lambda_function_name("my-func:live"), "my-func");
        assert_eq!(parse_lambda_function_name("my-func:42"), "my-func");
        // Full ARN, unqualified and qualified.
        assert_eq!(
            parse_lambda_function_name("arn:aws:lambda:us-east-1:123456789012:function:my-func"),
            "my-func"
        );
        assert_eq!(
            parse_lambda_function_name(
                "arn:aws:lambda:us-east-1:123456789012:function:my-func:live"
            ),
            "my-func"
        );
        // Partial ARN, unqualified and qualified.
        assert_eq!(
            parse_lambda_function_name("123456789012:function:my-func"),
            "my-func"
        );
        assert_eq!(
            parse_lambda_function_name("123456789012:function:my-func:live"),
            "my-func"
        );
    }

    #[test]
    fn alias_state_key_recovers_internal_key_from_arn() {
        // Alias ARN -> `{function}:{alias}` internal map key.
        assert_eq!(
            alias_state_key("arn:aws:lambda:us-east-1:123456789012:function:my-func:live"),
            "my-func:live"
        );
        // Legacy bare `name:alias` physical id is returned unchanged.
        assert_eq!(alias_state_key("my-func:live"), "my-func:live");
    }

    // ----------------------------------------------------------------- MQ

    #[test]
    fn cfn_mq_broker_creates_declared_users() {
        // The required `Users` property must actually create users on the
        // broker (previously the CFN path inserted an empty map).
        let prov = make_provisioner();
        let created = prov
            .create_resource(&make_resource(
                "AWS::AmazonMQ::Broker",
                "B",
                serde_json::json!({
                    "BrokerName": "cfn-users",
                    "EngineType": "ACTIVEMQ",
                    "DeploymentMode": "SINGLE_INSTANCE",
                    "HostInstanceType": "mq.m5.large",
                    "PubliclyAccessible": false,
                    "Users": [ { "Username": "admin", "Password": "SuperSecret1234" } ]
                }),
            ))
            .expect("broker provisions");

        let guard = prov.mq_state.read();
        let acct = guard.get("123456789012").unwrap();
        let users = acct
            .users
            .get(&created.physical_id)
            .expect("broker has a users map");
        assert!(users.contains_key("admin"), "declared user was created");
    }

    #[test]
    fn cfn_mq_broker_synthesizes_subnets_matching_api_path() {
        // Omitting SubnetIds must synthesize the same default-VPC subnet ids the
        // direct CreateBroker path produces (no import drift).
        let prov = make_provisioner();
        let created = prov
            .create_resource(&make_resource(
                "AWS::AmazonMQ::Broker",
                "B",
                serde_json::json!({
                    "BrokerName": "cfn-subnets",
                    "EngineType": "ACTIVEMQ",
                    "DeploymentMode": "SINGLE_INSTANCE",
                    "HostInstanceType": "mq.m5.large",
                    "PubliclyAccessible": false
                }),
            ))
            .expect("broker provisions");

        let guard = prov.mq_state.read();
        let acct = guard.get("123456789012").unwrap();
        let broker = acct.brokers.get(&created.physical_id).unwrap();
        let subnets: Vec<String> = broker["subnetIds"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_str().unwrap().to_string())
            .collect();
        let expected: Vec<String> =
            fakecloud_mq::shared::synthesize_subnets(&created.physical_id, "SINGLE_INSTANCE")
                .into_iter()
                .map(|v| v.as_str().unwrap().to_string())
                .collect();
        assert!(!subnets.is_empty(), "subnets are synthesized, not empty");
        assert_eq!(subnets, expected, "CFN subnets match the shared API path");
    }

    #[test]
    fn cfn_mq_configuration_association_preserves_history() {
        // Associating a new configuration must push the broker's prior current
        // configuration onto history rather than wiping it.
        let prov = make_provisioner();
        let broker = prov
            .create_resource(&make_resource(
                "AWS::AmazonMQ::Broker",
                "B",
                serde_json::json!({
                    "BrokerName": "cfn-hist",
                    "EngineType": "ACTIVEMQ",
                    "DeploymentMode": "SINGLE_INSTANCE",
                    "HostInstanceType": "mq.m5.large",
                    "PubliclyAccessible": false
                }),
            ))
            .expect("broker provisions");

        // The auto-generated current configuration id (pushed to history next).
        let prior_id = {
            let guard = prov.mq_state.read();
            let acct = guard.get("123456789012").unwrap();
            acct.brokers.get(&broker.physical_id).unwrap()["configurations"]["current"]["id"]
                .as_str()
                .unwrap()
                .to_string()
        };

        prov.create_resource(&make_resource(
            "AWS::AmazonMQ::ConfigurationAssociation",
            "A",
            serde_json::json!({
                "Broker": broker.physical_id,
                "Configuration": { "Id": "c-new-config", "Revision": 2 }
            }),
        ))
        .expect("association provisions");

        let guard = prov.mq_state.read();
        let acct = guard.get("123456789012").unwrap();
        let configs = &acct.brokers.get(&broker.physical_id).unwrap()["configurations"];
        assert_eq!(configs["current"]["id"].as_str(), Some("c-new-config"));
        assert_eq!(configs["current"]["revision"].as_i64(), Some(2));
        let history = configs["history"].as_array().unwrap();
        assert!(
            history
                .iter()
                .any(|h| h["id"].as_str() == Some(prior_id.as_str())),
            "prior current configuration is preserved in history"
        );
    }

    #[test]
    fn config_rule_getatt_config_rule_id_resolves() {
        let prov = make_provisioner();
        let created = prov
            .create_resource(&make_resource(
                "AWS::Config::ConfigRule",
                "MyRule",
                serde_json::json!({
                    "ConfigRuleName": "s3-versioning",
                    "Source": { "Owner": "AWS", "SourceIdentifier": "S3_BUCKET_VERSIONING_ENABLED" },
                }),
            ))
            .expect("provision config rule");
        // The real rule id stored in Config state.
        let stored_id = prov
            .config_state
            .read()
            .account("123456789012")
            .and_then(|a| a.rules.get("s3-versioning").map(|r| r.rule_id.clone()))
            .expect("rule stored");
        // `!GetAtt MyRule.ConfigRuleId` resolves to that real id, not a
        // placeholder, and `.ComplianceType` / `.Arn` are populated too.
        let got = created.attributes.get("ConfigRuleId").cloned();
        assert_eq!(got.as_deref(), Some(stored_id.as_str()));
        assert!(!stored_id.is_empty());
        assert_eq!(
            created.attributes.get("ComplianceType").map(String::as_str),
            Some("INSUFFICIENT_DATA")
        );
        assert!(created.attributes.contains_key("Arn"));
    }

    // A CFN-provisioned Route 53 Resolver endpoint resolves its HostVPCId from
    // the referenced subnet's real VPC (created via the EC2 provisioner), and a
    // ResolverConfig's declared GetAtt attributes (Id/OwnerId/ResourceId) all
    // resolve to real values.
    #[test]
    fn route53resolver_endpoint_host_vpc_and_config_getatt() {
        let prov = make_provisioner();
        let vpc = prov
            .create_resource(&make_resource(
                "AWS::EC2::VPC",
                "Vpc",
                serde_json::json!({ "CidrBlock": "10.0.0.0/16" }),
            ))
            .expect("vpc provisions");
        let vpc_id = vpc.physical_id.clone();
        let subnet = prov
            .create_resource(&make_resource(
                "AWS::EC2::Subnet",
                "Subnet",
                serde_json::json!({
                    "VpcId": vpc_id,
                    "CidrBlock": "10.0.1.0/24",
                    "AvailabilityZone": "us-east-1a",
                }),
            ))
            .expect("subnet provisions");
        let subnet_id = subnet.physical_id.clone();

        let endpoint = prov
            .create_resource(&make_resource(
                "AWS::Route53Resolver::ResolverEndpoint",
                "Endpoint",
                serde_json::json!({
                    "Direction": "OUTBOUND",
                    "SecurityGroupIds": ["sg-1"],
                    "IpAddresses": [{ "SubnetId": subnet_id }],
                }),
            ))
            .expect("endpoint provisions");
        // HostVPCId is the real VPC id, not an empty string.
        assert_eq!(
            endpoint.attributes.get("HostVPCId").map(String::as_str),
            Some(vpc_id.as_str())
        );
        assert_eq!(
            prov.get_att(&endpoint, "HostVPCId").as_deref(),
            Some(vpc_id.as_str())
        );

        // A ResolverConfig's declared GetAtt attributes all resolve.
        let cfg = prov
            .create_resource(&make_resource(
                "AWS::Route53Resolver::ResolverConfig",
                "Cfg",
                serde_json::json!({ "ResourceId": vpc_id, "AutodefinedReverseFlag": "DISABLE" }),
            ))
            .expect("resolver config provisions");
        assert_eq!(
            prov.get_att(&cfg, "ResourceId").as_deref(),
            Some(vpc_id.as_str())
        );
        assert_eq!(
            prov.get_att(&cfg, "OwnerId").as_deref(),
            Some("123456789012")
        );
        assert!(prov.get_att(&cfg, "Id").is_some());
    }

    // --- UpdateStack in-place fidelity for common freely-mutable types ---
    // Each of these creates a resource, runs an UpdateStack that changes a
    // supported property, and asserts the owning service's live state reflects
    // the new value (regression guard for the silent `_ => None` update arm).

    #[test]
    fn update_stack_applies_ssm_parameter_value() {
        let prov = make_provisioner();
        let created = prov
            .create_resource(&make_resource(
                "AWS::SSM::Parameter",
                "P",
                serde_json::json!({"Name": "/app/db", "Type": "String", "Value": "v1"}),
            ))
            .expect("param provisions");
        prov.update_resource(
            &created,
            &make_resource(
                "AWS::SSM::Parameter",
                "P",
                serde_json::json!({"Name": "/app/db", "Type": "String", "Value": "v2"}),
            ),
        )
        .expect("update succeeds")
        .expect("AWS::SSM::Parameter is updatable");
        let ssm = prov.ssm_state.read();
        let acct = ssm.get("123456789012").unwrap();
        let param = acct.parameters.get("/app/db").unwrap();
        assert_eq!(param.value, "v2", "GetParameter must return the new value");
        assert_eq!(param.version, 2, "overwrite bumps the version");
        assert_eq!(param.history.len(), 1, "old value rotated into history");
    }

    #[test]
    fn update_stack_applies_log_group_retention() {
        let prov = make_provisioner();
        let created = prov
            .create_resource(&make_resource(
                "AWS::Logs::LogGroup",
                "LG",
                serde_json::json!({"LogGroupName": "/svc/logs", "RetentionInDays": 7}),
            ))
            .expect("log group provisions");
        prov.update_resource(
            &created,
            &make_resource(
                "AWS::Logs::LogGroup",
                "LG",
                serde_json::json!({"LogGroupName": "/svc/logs", "RetentionInDays": 30}),
            ),
        )
        .expect("update succeeds")
        .expect("AWS::Logs::LogGroup is updatable");
        let logs = prov.logs_state.read();
        let acct = logs.get("123456789012").unwrap();
        let group = acct.log_groups.get("/svc/logs").unwrap();
        assert_eq!(group.retention_in_days, Some(30));
    }

    #[test]
    fn update_stack_applies_kinesis_stream_retention_and_shards() {
        let prov = make_provisioner();
        let created = prov
            .create_resource(&make_resource(
                "AWS::Kinesis::Stream",
                "KS",
                serde_json::json!({"Name": "events", "ShardCount": 1, "RetentionPeriodHours": 24}),
            ))
            .expect("stream provisions");
        prov.update_resource(
            &created,
            &make_resource(
                "AWS::Kinesis::Stream",
                "KS",
                serde_json::json!({"Name": "events", "ShardCount": 2, "RetentionPeriodHours": 48}),
            ),
        )
        .expect("update succeeds")
        .expect("AWS::Kinesis::Stream is updatable");
        let kinesis = prov.kinesis_state.read();
        let acct = kinesis.get("123456789012").unwrap();
        let stream = acct.streams.get("events").unwrap();
        assert_eq!(stream.retention_period_hours, 48);
        assert_eq!(stream.open_shard_count, 2);
        assert_eq!(stream.shard_count, 2);
    }

    #[test]
    fn update_stack_applies_eventbridge_rule_schedule_and_state() {
        let prov = make_provisioner();
        let created = prov
            .create_resource(&make_resource(
                "AWS::Events::Rule",
                "R",
                serde_json::json!({"Name": "r1", "ScheduleExpression": "rate(1 hour)", "State": "ENABLED"}),
            ))
            .expect("rule provisions");
        prov.update_resource(
            &created,
            &make_resource(
                "AWS::Events::Rule",
                "R",
                serde_json::json!({
                    "Name": "r1",
                    "ScheduleExpression": "rate(5 minutes)",
                    "State": "DISABLED",
                    "Targets": [{"Id": "t1", "Arn": "arn:aws:lambda:us-east-1:123456789012:function:fn"}]
                }),
            ),
        )
        .expect("update succeeds")
        .expect("AWS::Events::Rule is updatable");
        let eb = prov.eventbridge_state.read();
        let acct = eb.get("123456789012").unwrap();
        let rule = acct
            .rules
            .get(&("default".to_string(), "r1".to_string()))
            .unwrap();
        assert_eq!(rule.schedule_expression.as_deref(), Some("rate(5 minutes)"));
        assert_eq!(rule.state, "DISABLED");
        assert_eq!(rule.targets.len(), 1, "Targets re-applied on update");
    }

    #[test]
    fn update_stack_applies_dynamodb_table_throughput() {
        let prov = make_provisioner();
        let created = prov
            .create_resource(&make_resource(
                "AWS::DynamoDB::Table",
                "T",
                serde_json::json!({
                    "TableName": "items",
                    "AttributeDefinitions": [{"AttributeName": "id", "AttributeType": "S"}],
                    "KeySchema": [{"AttributeName": "id", "KeyType": "HASH"}],
                    "BillingMode": "PROVISIONED",
                    "ProvisionedThroughput": {"ReadCapacityUnits": 5, "WriteCapacityUnits": 5}
                }),
            ))
            .expect("table provisions");
        prov.update_resource(
            &created,
            &make_resource(
                "AWS::DynamoDB::Table",
                "T",
                serde_json::json!({
                    "TableName": "items",
                    "AttributeDefinitions": [{"AttributeName": "id", "AttributeType": "S"}],
                    "KeySchema": [{"AttributeName": "id", "KeyType": "HASH"}],
                    "BillingMode": "PROVISIONED",
                    "ProvisionedThroughput": {"ReadCapacityUnits": 25, "WriteCapacityUnits": 40}
                }),
            ),
        )
        .expect("update succeeds")
        .expect("AWS::DynamoDB::Table is updatable");
        let ddb = prov.dynamodb_state.read();
        let acct = ddb.get("123456789012").unwrap();
        let table = acct.tables.get("items").unwrap();
        assert_eq!(table.provisioned_throughput.read_capacity_units, 25);
        assert_eq!(table.provisioned_throughput.write_capacity_units, 40);
    }

    #[test]
    fn update_stack_applies_sns_subscription_attributes() {
        let prov = make_provisioner();
        let topic = prov
            .create_resource(&make_resource(
                "AWS::SNS::Topic",
                "Topic",
                serde_json::json!({"TopicName": "t"}),
            ))
            .expect("topic provisions");
        let created = prov
            .create_resource(&make_resource(
                "AWS::SNS::Subscription",
                "Sub",
                serde_json::json!({
                    "TopicArn": topic.physical_id,
                    "Protocol": "sqs",
                    "Endpoint": "arn:aws:sqs:us-east-1:123456789012:q"
                }),
            ))
            .expect("subscription provisions");
        prov.update_resource(
            &created,
            &make_resource(
                "AWS::SNS::Subscription",
                "Sub",
                serde_json::json!({
                    "TopicArn": topic.physical_id,
                    "Protocol": "sqs",
                    "Endpoint": "arn:aws:sqs:us-east-1:123456789012:q",
                    "RawMessageDelivery": true,
                    "FilterPolicy": {"store": ["s1"]}
                }),
            ),
        )
        .expect("update succeeds")
        .expect("AWS::SNS::Subscription is updatable");
        let sns = prov.sns_state.read();
        let acct = sns.get("123456789012").unwrap();
        let sub = acct.subscriptions.get(&created.physical_id).unwrap();
        assert_eq!(
            sub.attributes.get("RawMessageDelivery").map(String::as_str),
            Some("true")
        );
        assert!(sub
            .attributes
            .get("FilterPolicy")
            .is_some_and(|p| p.contains("store")));
    }

    #[test]
    fn update_stack_applies_secretsmanager_secret_string() {
        let prov = make_provisioner();
        let created = prov
            .create_resource(&make_resource(
                "AWS::SecretsManager::Secret",
                "S",
                serde_json::json!({"Name": "db-cred", "SecretString": "old"}),
            ))
            .expect("secret provisions");
        prov.update_resource(
            &created,
            &make_resource(
                "AWS::SecretsManager::Secret",
                "S",
                serde_json::json!({"Name": "db-cred", "SecretString": "new", "Description": "updated"}),
            ),
        )
        .expect("update succeeds")
        .expect("AWS::SecretsManager::Secret is updatable");
        let sm = prov.secretsmanager_state.read();
        let acct = sm.get("123456789012").unwrap();
        let secret = acct.secrets.get(&created.physical_id).unwrap();
        let current = secret
            .current_version_id
            .as_ref()
            .and_then(|vid| secret.versions.get(vid))
            .unwrap();
        assert_eq!(current.secret_string.as_deref(), Some("new"));
        assert_eq!(secret.description.as_deref(), Some("updated"));
        let previous_count = secret
            .versions
            .values()
            .filter(|v| v.stages.iter().any(|s| s == "AWSPREVIOUS"))
            .count();
        assert_eq!(previous_count, 1, "exactly one AWSPREVIOUS version");
    }

    #[test]
    fn update_stack_applies_cognito_user_pool_config() {
        let prov = make_provisioner();
        let created = prov
            .create_resource(&make_resource(
                "AWS::Cognito::UserPool",
                "UP",
                serde_json::json!({"PoolName": "pool", "MfaConfiguration": "OFF"}),
            ))
            .expect("pool provisions");
        prov.update_resource(
            &created,
            &make_resource(
                "AWS::Cognito::UserPool",
                "UP",
                serde_json::json!({"PoolName": "pool", "MfaConfiguration": "ON", "DeletionProtection": "ACTIVE"}),
            ),
        )
        .expect("update succeeds")
        .expect("AWS::Cognito::UserPool is updatable");
        let cognito = prov.cognito_state.read();
        let acct = cognito.get("123456789012").unwrap();
        let pool = acct.user_pools.get(&created.physical_id).unwrap();
        assert_eq!(pool.mfa_configuration, "ON");
        assert_eq!(pool.deletion_protection.as_deref(), Some("ACTIVE"));
    }

    #[test]
    fn update_stack_applies_cognito_user_pool_client_config() {
        let prov = make_provisioner();
        let pool = prov
            .create_resource(&make_resource(
                "AWS::Cognito::UserPool",
                "UP",
                serde_json::json!({"PoolName": "pool"}),
            ))
            .expect("pool provisions");
        let created = prov
            .create_resource(&make_resource(
                "AWS::Cognito::UserPoolClient",
                "Client",
                serde_json::json!({
                    "UserPoolId": pool.physical_id,
                    "ClientName": "c1",
                    "CallbackURLs": ["https://old.example.com/cb"]
                }),
            ))
            .expect("client provisions");
        prov.update_resource(
            &created,
            &make_resource(
                "AWS::Cognito::UserPoolClient",
                "Client",
                serde_json::json!({
                    "UserPoolId": pool.physical_id,
                    "ClientName": "c1-renamed",
                    "CallbackURLs": ["https://new.example.com/cb"]
                }),
            ),
        )
        .expect("update succeeds")
        .expect("AWS::Cognito::UserPoolClient is updatable");
        let cognito = prov.cognito_state.read();
        let acct = cognito.get("123456789012").unwrap();
        let client = acct.user_pool_clients.get(&created.physical_id).unwrap();
        assert_eq!(client.client_name, "c1-renamed");
        assert_eq!(
            client.callback_urls,
            vec!["https://new.example.com/cb".to_string()]
        );
    }

    #[test]
    fn update_stack_applies_rds_db_instance_config() {
        let prov = make_provisioner();
        let created = prov
            .create_resource(&make_resource(
                "AWS::RDS::DBInstance",
                "DB",
                serde_json::json!({
                    "DBInstanceIdentifier": "app-db",
                    "DBInstanceClass": "db.t3.micro",
                    "Engine": "postgres",
                    "AllocatedStorage": "20"
                }),
            ))
            .expect("instance provisions");
        prov.update_resource(
            &created,
            &make_resource(
                "AWS::RDS::DBInstance",
                "DB",
                serde_json::json!({
                    "DBInstanceIdentifier": "app-db",
                    "DBInstanceClass": "db.t3.large",
                    "Engine": "postgres",
                    "AllocatedStorage": "100",
                    "BackupRetentionPeriod": 7
                }),
            ),
        )
        .expect("update succeeds")
        .expect("AWS::RDS::DBInstance is updatable");
        let rds = prov.rds_state.read();
        let acct = rds.get("123456789012").unwrap();
        let inst = acct.instances.get("app-db").unwrap();
        assert_eq!(inst.db_instance_class, "db.t3.large");
        assert_eq!(inst.allocated_storage, 100);
        assert_eq!(inst.backup_retention_period, 7);
    }

    // --- Orchestration/persistence sweep (#1766 family): CFN write-through for
    // codepipeline/codebuild/codedeploy/opensearch/emr/sagemaker/backup/appsync/
    // timestream/mwaa/amplify/appconfig. Each test provisions via the CFN
    // provisioner, asserts the resource landed in the OWNING service's store
    // (so Describe/Get reads it), and checks the returned Ref/GetAtt.

    fn attr<'a>(r: &'a StackResource, k: &str) -> &'a str {
        r.attributes.get(k).map(String::as_str).unwrap_or("")
    }

    #[test]
    fn codepipeline_pipeline_writes_through_and_survives_snapshot() {
        let prov = make_provisioner();
        let r = prov
            .create_resource(&make_resource(
                "AWS::CodePipeline::Pipeline",
                "P",
                serde_json::json!({
                    "Name": "my-pipe",
                    "RoleArn": "arn:aws:iam::123456789012:role/cp",
                    "ArtifactStore": {"Type": "S3", "Location": "bkt"},
                    "Stages": [{
                        "Name": "Source",
                        "Actions": [{
                            "Name": "Src",
                            "ActionTypeId": {"Category": "Source", "Owner": "AWS",
                                "Provider": "S3", "Version": "1"},
                            "Configuration": {"S3Bucket": "bkt", "S3ObjectKey": "k.zip"}
                        }]
                    }]
                }),
            ))
            .expect("pipeline provisions");
        assert_eq!(r.physical_id, "my-pipe");
        assert_eq!(attr(&r, "Version"), "1");
        {
            let g = prov.codepipeline_state.read();
            let st = g.get("123456789012").unwrap();
            let decl = st.pipelines.get("my-pipe").expect("pipeline in store");
            assert_eq!(decl["name"], "my-pipe");
            assert_eq!(decl["roleArn"], "arn:aws:iam::123456789012:role/cp");
            // Action Configuration keys are carried verbatim (not camel-cased).
            assert_eq!(
                decl["stages"][0]["actions"][0]["configuration"]["S3Bucket"],
                "bkt"
            );
            assert_eq!(decl["stages"][0]["actions"][0]["runOrder"], 1);
            assert!(st.pipeline_order.contains(&"my-pipe".to_string()));
            assert!(st.pipeline_meta.contains_key("my-pipe"));
        }
        // Snapshot round-trip: the record must survive serialize/deserialize of
        // the whole MultiAccountState (what the snapshot hook persists).
        let json = serde_json::to_string(&*prov.codepipeline_state.read()).unwrap();
        let restored: fakecloud_core::multi_account::MultiAccountState<
            fakecloud_codepipeline::CodePipelineState,
        > = serde_json::from_str(&json).unwrap();
        assert!(restored
            .get("123456789012")
            .unwrap()
            .pipelines
            .contains_key("my-pipe"));

        prov.delete_resource(&r).expect("delete");
        assert!(prov
            .codepipeline_state
            .read()
            .get("123456789012")
            .unwrap()
            .pipelines
            .is_empty());
    }

    #[test]
    fn codebuild_project_writes_through() {
        let prov = make_provisioner();
        let r = prov
            .create_resource(&make_resource(
                "AWS::CodeBuild::Project",
                "B",
                serde_json::json!({
                    "Name": "builder",
                    "ServiceRole": "arn:aws:iam::123456789012:role/cb",
                    "Source": {"Type": "GITHUB", "Location": "https://x", "BuildSpec": "y"},
                    "Artifacts": {"Type": "NO_ARTIFACTS"},
                    "Environment": {"Type": "LINUX_CONTAINER", "Image": "img",
                        "ComputeType": "BUILD_GENERAL1_SMALL"}
                }),
            ))
            .expect("project provisions");
        assert_eq!(r.physical_id, "builder");
        let g = prov.codebuild_state.read();
        let st = g.get("123456789012").unwrap();
        let p = st.projects.get("builder").expect("project in store");
        assert_eq!(p["name"], "builder");
        assert_eq!(p["arn"], attr(&r, "Arn"));
        assert_eq!(p["serviceRole"], "arn:aws:iam::123456789012:role/cb");
        // BuildSpec is normalized to the API's lowercase `buildspec`.
        assert_eq!(p["source"]["buildspec"], "y");
        assert_eq!(p["badge"]["badgeEnabled"], false);
    }

    #[test]
    fn codedeploy_application_and_group_write_through() {
        let prov = make_provisioner();
        let app = prov
            .create_resource(&make_resource(
                "AWS::CodeDeploy::Application",
                "App",
                serde_json::json!({"ApplicationName": "app1", "ComputePlatform": "Lambda"}),
            ))
            .expect("app provisions");
        assert_eq!(app.physical_id, "app1");
        let dg = prov
            .create_resource(&make_resource(
                "AWS::CodeDeploy::DeploymentGroup",
                "Dg",
                serde_json::json!({
                    "ApplicationName": "app1",
                    "DeploymentGroupName": "dg1",
                    "ServiceRoleArn": "arn:aws:iam::123456789012:role/cd"
                }),
            ))
            .expect("group provisions");
        assert_eq!(dg.physical_id, "app1/dg1");
        assert!(!attr(&dg, "Id").is_empty());
        let g = prov.codedeploy_state.read();
        let st = g.get("123456789012").unwrap();
        assert_eq!(
            st.applications.get("app1").unwrap()["computePlatform"],
            "Lambda"
        );
        let group = st
            .deployment_groups
            .get("app1")
            .unwrap()
            .get("dg1")
            .unwrap();
        assert_eq!(group["computePlatform"], "Lambda");
        assert_eq!(group["serviceRoleArn"], "arn:aws:iam::123456789012:role/cd");
    }

    #[test]
    fn opensearch_domain_writes_through() {
        let prov = make_provisioner();
        let r = prov
            .create_resource(&make_resource(
                "AWS::OpenSearchService::Domain",
                "D",
                serde_json::json!({
                    "DomainName": "logs",
                    "EngineVersion": "OpenSearch_2.11",
                    "ClusterConfig": {"InstanceType": "t3.small.search", "InstanceCount": 2}
                }),
            ))
            .expect("domain provisions");
        assert_eq!(r.physical_id, "logs");
        let g = prov.opensearch_state.read();
        let d = g
            .get("123456789012")
            .unwrap()
            .domains
            .get("logs")
            .expect("domain in store");
        assert_eq!(d.arn, attr(&r, "Arn"));
        assert_eq!(d.engine_version, "OpenSearch_2.11");
        assert_eq!(d.config["ClusterConfig"]["InstanceCount"], 2);
    }

    #[test]
    fn emr_cluster_writes_through() {
        let prov = make_provisioner();
        let r = prov
            .create_resource(&make_resource(
                "AWS::EMR::Cluster",
                "C",
                serde_json::json!({
                    "Name": "analytics",
                    "ReleaseLabel": "emr-6.15.0",
                    "ServiceRole": "EMR_DefaultRole"
                }),
            ))
            .expect("cluster provisions");
        assert!(r.physical_id.starts_with("j-"));
        let g = prov.emr_state.read();
        let st = g.get("123456789012").unwrap();
        let c = st.clusters.get(&r.physical_id).expect("cluster in store");
        assert_eq!(c["Name"], "analytics");
        assert_eq!(c["ReleaseLabel"], "emr-6.15.0");
        assert!(st.cluster_order.contains(&r.physical_id));
    }

    #[test]
    fn sagemaker_model_writes_through() {
        let prov = make_provisioner();
        let r = prov
            .create_resource(&make_resource(
                "AWS::SageMaker::Model",
                "M",
                serde_json::json!({
                    "ModelName": "m1",
                    "ExecutionRoleArn": "arn:aws:iam::123456789012:role/sm"
                }),
            ))
            .expect("model provisions");
        assert_eq!(r.physical_id, "m1");
        let g = prov.sagemaker_state.read();
        let rec = g
            .get("123456789012")
            .unwrap()
            .get_resource("Model", "m1")
            .expect("model in store");
        assert_eq!(rec["ModelName"], "m1");
        assert_eq!(
            rec["ModelArn"],
            "arn:aws:sagemaker:us-east-1:123456789012:model/m1"
        );
        assert!(rec["CreationTime"].is_number());
    }

    #[test]
    fn backup_vault_and_plan_write_through() {
        let prov = make_provisioner();
        let vault = prov
            .create_resource(&make_resource(
                "AWS::Backup::BackupVault",
                "V",
                serde_json::json!({"BackupVaultName": "vault1"}),
            ))
            .expect("vault provisions");
        assert_eq!(vault.physical_id, "vault1");
        let plan = prov
            .create_resource(&make_resource(
                "AWS::Backup::BackupPlan",
                "Pl",
                serde_json::json!({
                    "BackupPlan": {
                        "BackupPlanName": "daily",
                        "BackupPlanRule": [{"RuleName": "r1", "TargetBackupVault": "vault1"}]
                    }
                }),
            ))
            .expect("plan provisions");
        assert!(!attr(&plan, "BackupPlanId").is_empty());
        let g = prov.backup_state.read();
        let st = g.get("123456789012").unwrap();
        assert!(st.vaults.contains_key("vault1"));
        let p = st.plans.get(&plan.physical_id).expect("plan in store");
        assert_eq!(p.plan["BackupPlanName"], "daily");
        // BackupPlanRule normalized to the API's `Rules`.
        assert!(p.plan.get("Rules").is_some());
    }

    #[test]
    fn appsync_api_datasource_resolver_write_through() {
        let prov = make_provisioner();
        let api = prov
            .create_resource(&make_resource(
                "AWS::AppSync::GraphQLApi",
                "Api",
                serde_json::json!({"Name": "gql", "AuthenticationType": "API_KEY"}),
            ))
            .expect("api provisions");
        let api_id = api.physical_id.clone();
        let ds = prov
            .create_resource(&make_resource(
                "AWS::AppSync::DataSource",
                "Ds",
                serde_json::json!({"ApiId": api_id, "Name": "src", "Type": "NONE"}),
            ))
            .expect("datasource provisions");
        let res = prov
            .create_resource(&make_resource(
                "AWS::AppSync::Resolver",
                "Res",
                serde_json::json!({
                    "ApiId": api.physical_id, "TypeName": "Query", "FieldName": "getX",
                    "DataSourceName": "src"
                }),
            ))
            .expect("resolver provisions");
        let g = prov.appsync_state.read();
        let data = g.get("123456789012").unwrap();
        let stored = data
            .graphql_apis
            .get(&api.physical_id)
            .expect("api in store");
        assert_eq!(stored["name"], "gql");
        assert_eq!(stored["authenticationType"], "API_KEY");
        assert!(!attr(&api, "GraphQLUrl").is_empty());
        assert!(data
            .data_sources
            .get(&api.physical_id)
            .unwrap()
            .contains_key("src"));
        assert_eq!(ds.physical_id, format!("{}|src", api.physical_id));
        assert!(data
            .resolvers
            .get(&api.physical_id)
            .unwrap()
            .contains_key("Query::getX"));
        assert!(res.physical_id.ends_with("|Query|getX"));
    }

    #[test]
    fn timestream_database_and_table_write_through() {
        let prov = make_provisioner();
        let db = prov
            .create_resource(&make_resource(
                "AWS::Timestream::Database",
                "Db",
                serde_json::json!({"DatabaseName": "metrics"}),
            ))
            .expect("db provisions");
        assert_eq!(db.physical_id, "metrics");
        let table = prov
            .create_resource(&make_resource(
                "AWS::Timestream::Table",
                "T",
                serde_json::json!({"DatabaseName": "metrics", "TableName": "cpu"}),
            ))
            .expect("table provisions");
        assert_eq!(table.physical_id, "metrics|cpu");
        let g = prov.timestream_state.read();
        let data = g.get("123456789012").unwrap();
        assert!(data.databases.contains_key("metrics"));
        assert_eq!(data.databases.get("metrics").unwrap().table_count, 1);
        let key = fakecloud_timestream::shared::table_key("metrics", "cpu");
        assert!(data.tables.contains_key(&key));
    }

    // Fix 1: UpdateStack changing a property on a type that has a create/delete
    // provisioner but no dedicated `update_*` arm must re-provision so the
    // service's stored resource reflects the change -- and must return a
    // resource (not `None`) so the driver records a ResourceChange -- instead
    // of silently reporting UPDATE_COMPLETE while applying nothing.
    #[test]
    fn update_reprovisions_type_without_update_arm() {
        let prov = make_provisioner();
        let created = prov
            .create_resource(&make_resource(
                "AWS::Timestream::Database",
                "Db",
                serde_json::json!({
                    "DatabaseName": "metrics",
                    "KmsKeyId": "arn:aws:kms:us-east-1:123456789012:key/old"
                }),
            ))
            .expect("db provisions");
        // Sanity: stored with the old KMS key.
        {
            let g = prov.timestream_state.read();
            let data = g.get("123456789012").unwrap();
            assert_eq!(
                data.databases.get("metrics").unwrap().kms_key_id.as_deref(),
                Some("arn:aws:kms:us-east-1:123456789012:key/old")
            );
        }

        // Change the KmsKeyId. Timestream::Database has no dedicated update arm.
        let new_def = make_resource(
            "AWS::Timestream::Database",
            "Db",
            serde_json::json!({
                "DatabaseName": "metrics",
                "KmsKeyId": "arn:aws:kms:us-east-1:123456789012:key/new"
            }),
        );
        let updated = prov
            .update_resource(&created, &new_def)
            .expect("update ok")
            .expect("update returns a resource (not None) so a ResourceChange is recorded");
        // Name-keyed service: physical id is preserved (in-place-style update).
        assert_eq!(updated.physical_id, "metrics");
        assert_eq!(updated.status, "UPDATE_COMPLETE");

        // The stored resource now reflects the new property -- not a no-op.
        let g = prov.timestream_state.read();
        let data = g.get("123456789012").unwrap();
        assert_eq!(
            data.databases.get("metrics").unwrap().kms_key_id.as_deref(),
            Some("arn:aws:kms:us-east-1:123456789012:key/new")
        );
    }

    #[test]
    fn mwaa_environment_writes_through() {
        let prov = make_provisioner();
        let r = prov
            .create_resource(&make_resource(
                "AWS::MWAA::Environment",
                "E",
                serde_json::json!({
                    "Name": "airflow",
                    "ExecutionRoleArn": "arn:aws:iam::123456789012:role/mwaa",
                    "SourceBucketArn": "arn:aws:s3:::dags",
                    "DagS3Path": "dags"
                }),
            ))
            .expect("env provisions");
        assert_eq!(r.physical_id, "airflow");
        let g = prov.mwaa_state.read();
        let env = g
            .get("123456789012")
            .unwrap()
            .environments
            .get("airflow")
            .expect("env in store");
        assert_eq!(env["Name"], "airflow");
        assert_eq!(
            env["ExecutionRoleArn"],
            "arn:aws:iam::123456789012:role/mwaa"
        );
        assert_eq!(env["Arn"], attr(&r, "Arn"));
    }

    #[test]
    fn amplify_app_writes_through() {
        let prov = make_provisioner();
        let r = prov
            .create_resource(&make_resource(
                "AWS::Amplify::App",
                "A",
                serde_json::json!({"Name": "web", "Repository": "https://git/x"}),
            ))
            .expect("app provisions");
        let g = prov.amplify_state.read();
        let rec = g
            .get("123456789012")
            .unwrap()
            .apps
            .get(&r.physical_id)
            .expect("app in store");
        assert_eq!(rec.app["name"], "web");
        assert_eq!(rec.app["appId"], r.physical_id);
        assert_eq!(attr(&r, "AppName"), "web");
        assert!(!attr(&r, "DefaultDomain").is_empty());
    }

    #[test]
    fn appconfig_application_environment_profile_write_through() {
        let prov = make_provisioner();
        let app = prov
            .create_resource(&make_resource(
                "AWS::AppConfig::Application",
                "App",
                serde_json::json!({"Name": "svc"}),
            ))
            .expect("app provisions");
        let app_id = app.physical_id.clone();
        let env = prov
            .create_resource(&make_resource(
                "AWS::AppConfig::Environment",
                "Env",
                serde_json::json!({"ApplicationId": app_id, "Name": "prod"}),
            ))
            .expect("env provisions");
        let prof = prov
            .create_resource(&make_resource(
                "AWS::AppConfig::ConfigurationProfile",
                "Prof",
                serde_json::json!({
                    "ApplicationId": app.physical_id, "Name": "cfg",
                    "LocationUri": "hosted"
                }),
            ))
            .expect("profile provisions");
        let g = prov.appconfig_state.read();
        let st = g.get("123456789012").unwrap();
        let a = st.applications.get(&app.physical_id).expect("app in store");
        assert_eq!(a.name, "svc");
        assert_eq!(a.environments.len(), 1);
        assert_eq!(a.profiles.len(), 1);
        assert!(env
            .physical_id
            .starts_with(&format!("{}|", app.physical_id)));
        assert!(prof
            .physical_id
            .starts_with(&format!("{}|", app.physical_id)));
        let env_id = env.physical_id.split('|').nth(1).unwrap();
        assert_eq!(a.environments.get(env_id).unwrap().name, "prod");
    }
}
