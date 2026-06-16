//! State for API Gateway v1 (REST APIs).
//!
//! The control plane stores all the entities AWS exposes through the
//! REST-style management API: REST APIs, the resource tree, methods,
//! integrations, deployments, stages, models, request validators,
//! authorizers, API keys, usage plans + keys, VPC links, domain names
//! + base path mappings, client certificates, and documentation parts.
//!
//! Most fields lean on `serde_json::Value` for the bag-of-properties
//! types (e.g. method request parameters, model schemas) so the JSON
//! we accept on the way in is the JSON we hand back on the way out.
//! Strongly-typed fields are reserved for things the data plane (Lambda
//! invocation, HTTP forwarding) actually needs to dispatch correctly.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::Arc;

pub type SharedApiGatewayState =
    Arc<parking_lot::RwLock<fakecloud_core::multi_account::MultiAccountState<ApiGatewayState>>>;

impl fakecloud_core::multi_account::AccountState for ApiGatewayState {
    fn new_for_account(account_id: &str, region: &str, _endpoint: &str) -> Self {
        Self::new(account_id, region)
    }
}

pub const APIGATEWAY_SNAPSHOT_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Serialize, Deserialize)]
pub struct ApiGatewaySnapshot {
    pub schema_version: u32,
    #[serde(default)]
    pub accounts: Option<fakecloud_core::multi_account::MultiAccountState<ApiGatewayState>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApiGatewayState {
    pub account_id: String,
    pub region: String,
    /// REST APIs keyed by `restApiId`.
    #[serde(default)]
    pub apis: BTreeMap<String, RestApi>,
    /// Resources keyed by `restApiId` -> `resourceId`.
    #[serde(default)]
    pub resources: BTreeMap<String, BTreeMap<String, Resource>>,
    /// Methods keyed by `restApiId/resourceId/HTTP_METHOD`. Stored
    /// flat so dispatch can do a single lookup.
    #[serde(default)]
    pub methods: BTreeMap<String, Method>,
    /// Integrations keyed by the same `restApiId/resourceId/HTTP_METHOD`
    /// composite key as methods. One integration per method.
    #[serde(default)]
    pub integrations: BTreeMap<String, Integration>,
    /// Method-level integration responses keyed by
    /// `restApiId/resourceId/HTTP_METHOD/statusCode`.
    #[serde(default)]
    pub integration_responses: BTreeMap<String, serde_json::Value>,
    /// Method responses (response shape declarations) keyed by
    /// `restApiId/resourceId/HTTP_METHOD/statusCode`.
    #[serde(default)]
    pub method_responses: BTreeMap<String, serde_json::Value>,
    /// Deployments keyed by `restApiId` -> `deploymentId`.
    #[serde(default)]
    pub deployments: BTreeMap<String, BTreeMap<String, Deployment>>,
    /// Stages keyed by `restApiId` -> `stageName`.
    #[serde(default)]
    pub stages: BTreeMap<String, BTreeMap<String, Stage>>,
    /// Models keyed by `restApiId` -> `modelName`.
    #[serde(default)]
    pub models: BTreeMap<String, BTreeMap<String, Model>>,
    /// Request validators keyed by `restApiId` -> `validatorId`.
    #[serde(default)]
    pub request_validators: BTreeMap<String, BTreeMap<String, serde_json::Value>>,
    /// Authorizers keyed by `restApiId` -> `authorizerId`.
    #[serde(default)]
    pub authorizers: BTreeMap<String, BTreeMap<String, Authorizer>>,
    /// API keys keyed by `apiKeyId`.
    #[serde(default)]
    pub api_keys: BTreeMap<String, ApiKey>,
    /// Usage plans keyed by `usagePlanId`.
    #[serde(default)]
    pub usage_plans: BTreeMap<String, UsagePlan>,
    /// Usage plan keys keyed by `usagePlanId` -> `apiKeyId`.
    #[serde(default)]
    pub usage_plan_keys: BTreeMap<String, BTreeMap<String, serde_json::Value>>,
    /// Per-(usagePlanId, apiKeyId) remaining quota, as observed/adjusted
    /// via GetUsage / UpdateUsage. Absent until a key is metered; falls
    /// back to the plan's quota limit.
    #[serde(default)]
    pub usage_remaining: BTreeMap<String, BTreeMap<String, i64>>,
    /// VPC links keyed by id.
    #[serde(default)]
    pub vpc_links: BTreeMap<String, serde_json::Value>,
    /// Domain names keyed by domain.
    #[serde(default)]
    pub domain_names: BTreeMap<String, serde_json::Value>,
    /// Domain name access associations keyed by ARN.
    #[serde(default)]
    pub domain_name_access_associations: BTreeMap<String, serde_json::Value>,
    /// Base path mappings keyed by `domain` -> `basePath`.
    #[serde(default)]
    pub base_path_mappings: BTreeMap<String, BTreeMap<String, serde_json::Value>>,
    /// Client certificates keyed by id.
    #[serde(default)]
    pub client_certificates: BTreeMap<String, serde_json::Value>,
    /// Documentation parts keyed by `restApiId` -> `documentationPartId`.
    #[serde(default)]
    pub documentation_parts: BTreeMap<String, BTreeMap<String, serde_json::Value>>,
    /// Documentation versions keyed by `restApiId` -> `version`.
    #[serde(default)]
    pub documentation_versions: BTreeMap<String, BTreeMap<String, serde_json::Value>>,
    /// Gateway responses keyed by `restApiId` -> `responseType`.
    #[serde(default)]
    pub gateway_responses: BTreeMap<String, BTreeMap<String, serde_json::Value>>,
    /// Account-wide settings (the `Account` resource — singleton per
    /// AWS account).
    #[serde(default)]
    pub account_settings: serde_json::Value,
    /// Tags keyed by resource ARN.
    #[serde(default)]
    pub tags: BTreeMap<String, BTreeMap<String, String>>,
    /// Introspection-only request history (not persisted).
    #[serde(default, skip_serializing)]
    pub request_history: Vec<ApiRequest>,
    /// In-memory authorizer result cache keyed by `<authorizerId>|<token>`.
    /// Mirrors AWS's identity caching: a hit short-circuits the
    /// authorizer Lambda invocation and reuses the prior policy +
    /// context for `authorizerResultTtlInSeconds`. Not persisted so a
    /// restart forces re-evaluation.
    #[serde(default, skip)]
    pub authorizer_cache: BTreeMap<String, CachedAuthorizerResult>,
}

#[derive(Debug, Clone)]
pub struct CachedAuthorizerResult {
    pub principal_id: String,
    pub effect: AuthEffect,
    pub context: serde_json::Value,
    pub claims: Option<serde_json::Value>,
    pub expires_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthEffect {
    Allow,
    Deny,
}

impl ApiGatewayState {
    pub fn new(account_id: &str, region: &str) -> Self {
        Self {
            account_id: account_id.to_string(),
            region: region.to_string(),
            apis: BTreeMap::new(),
            resources: BTreeMap::new(),
            methods: BTreeMap::new(),
            integrations: BTreeMap::new(),
            integration_responses: BTreeMap::new(),
            method_responses: BTreeMap::new(),
            deployments: BTreeMap::new(),
            stages: BTreeMap::new(),
            models: BTreeMap::new(),
            request_validators: BTreeMap::new(),
            authorizers: BTreeMap::new(),
            api_keys: BTreeMap::new(),
            usage_plans: BTreeMap::new(),
            usage_plan_keys: BTreeMap::new(),
            usage_remaining: BTreeMap::new(),
            vpc_links: BTreeMap::new(),
            domain_names: BTreeMap::new(),
            domain_name_access_associations: BTreeMap::new(),
            base_path_mappings: BTreeMap::new(),
            client_certificates: BTreeMap::new(),
            documentation_parts: BTreeMap::new(),
            documentation_versions: BTreeMap::new(),
            gateway_responses: BTreeMap::new(),
            account_settings: default_account_settings(),
            tags: BTreeMap::new(),
            request_history: Vec::new(),
            authorizer_cache: BTreeMap::new(),
        }
    }

    pub fn reset(&mut self) {
        self.apis.clear();
        self.resources.clear();
        self.methods.clear();
        self.integrations.clear();
        self.integration_responses.clear();
        self.method_responses.clear();
        self.deployments.clear();
        self.stages.clear();
        self.models.clear();
        self.request_validators.clear();
        self.authorizers.clear();
        self.api_keys.clear();
        self.usage_plans.clear();
        self.usage_plan_keys.clear();
        self.usage_remaining.clear();
        self.vpc_links.clear();
        self.domain_names.clear();
        self.domain_name_access_associations.clear();
        self.base_path_mappings.clear();
        self.client_certificates.clear();
        self.documentation_parts.clear();
        self.documentation_versions.clear();
        self.gateway_responses.clear();
        self.account_settings = default_account_settings();
        self.tags.clear();
        self.request_history.clear();
        self.authorizer_cache.clear();
    }
}

fn default_account_settings() -> serde_json::Value {
    serde_json::json!({
        "cloudwatchRoleArn": null,
        "throttleSettings": {"burstLimit": 5000, "rateLimit": 10000.0},
        "features": [],
        "apiKeyVersion": "4",
    })
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RestApi {
    pub id: String,
    pub name: String,
    pub description: Option<String>,
    pub version: Option<String>,
    pub created_date: DateTime<Utc>,
    pub api_key_source: String,
    pub endpoint_configuration: serde_json::Value,
    pub policy: Option<String>,
    pub binary_media_types: Vec<String>,
    pub minimum_compression_size: Option<i64>,
    pub disable_execute_api_endpoint: bool,
    pub root_resource_id: String,
    pub tags: BTreeMap<String, String>,
    /// Body of an OpenAPI/Swagger import, when the API was created by
    /// `ImportRestApi`. Kept around so `GetExport` can round-trip it.
    pub import_source: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Resource {
    pub id: String,
    pub parent_id: Option<String>,
    pub path_part: Option<String>,
    pub path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Method {
    pub rest_api_id: String,
    pub resource_id: String,
    pub http_method: String,
    pub authorization_type: String,
    pub authorizer_id: Option<String>,
    pub api_key_required: bool,
    pub operation_name: Option<String>,
    pub request_parameters: BTreeMap<String, bool>,
    pub request_models: BTreeMap<String, String>,
    pub request_validator_id: Option<String>,
    pub authorization_scopes: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Integration {
    pub rest_api_id: String,
    pub resource_id: String,
    pub http_method: String,
    /// AWS integration `type`: `AWS`, `AWS_PROXY`, `HTTP`, `HTTP_PROXY`,
    /// or `MOCK`.
    pub integration_type: String,
    /// HTTP method the integration backend expects (e.g. `POST` for
    /// Lambda, mirrors the request method for `*_PROXY`).
    pub integration_http_method: Option<String>,
    /// Backend URI: ARN for AWS_PROXY (Lambda), HTTPS for HTTP_PROXY.
    pub uri: Option<String>,
    pub credentials: Option<String>,
    pub request_parameters: BTreeMap<String, String>,
    pub request_templates: BTreeMap<String, String>,
    pub passthrough_behavior: String,
    pub timeout_in_millis: Option<i32>,
    pub cache_namespace: Option<String>,
    pub cache_key_parameters: Vec<String>,
    pub content_handling: Option<String>,
    pub connection_type: Option<String>,
    pub connection_id: Option<String>,
    pub tls_config: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Deployment {
    pub id: String,
    pub description: Option<String>,
    pub created_date: DateTime<Utc>,
    /// Snapshot of API state as of the deployment so updates after this
    /// deployment don't affect previously-deployed stages until the
    /// stage is re-pointed at a newer deployment.
    pub api_summary: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Stage {
    pub stage_name: String,
    pub deployment_id: String,
    pub description: Option<String>,
    pub cache_cluster_enabled: bool,
    pub cache_cluster_size: Option<String>,
    pub variables: BTreeMap<String, String>,
    pub method_settings: BTreeMap<String, serde_json::Value>,
    pub created_date: DateTime<Utc>,
    pub last_updated_date: DateTime<Utc>,
    pub tracing_enabled: bool,
    pub web_acl_arn: Option<String>,
    pub canary_settings: Option<serde_json::Value>,
    pub access_log_settings: Option<serde_json::Value>,
    pub tags: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Model {
    pub id: String,
    pub name: String,
    pub description: Option<String>,
    pub schema: Option<String>,
    pub content_type: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Authorizer {
    pub id: String,
    pub name: String,
    pub authorizer_type: String,
    pub provider_arns: Vec<String>,
    pub auth_type: Option<String>,
    pub authorizer_uri: Option<String>,
    pub authorizer_credentials: Option<String>,
    pub identity_source: Option<String>,
    pub identity_validation_expression: Option<String>,
    pub authorizer_result_ttl_in_seconds: Option<i32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApiKey {
    pub id: String,
    pub value: String,
    pub name: String,
    pub description: Option<String>,
    pub enabled: bool,
    pub created_date: DateTime<Utc>,
    pub last_updated_date: DateTime<Utc>,
    pub stage_keys: Vec<String>,
    pub tags: BTreeMap<String, String>,
    pub customer_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UsagePlan {
    pub id: String,
    pub name: String,
    pub description: Option<String>,
    pub api_stages: Vec<serde_json::Value>,
    pub throttle: Option<serde_json::Value>,
    pub quota: Option<serde_json::Value>,
    pub product_code: Option<String>,
    pub tags: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApiRequest {
    pub api_id: String,
    pub stage: String,
    pub method: String,
    pub path: String,
    pub status: u16,
    pub created_at: DateTime<Utc>,
}

pub fn make_id() -> String {
    // Real API GW resource IDs are 10-character alphanumeric strings.
    // Mirror the shape so downstream tooling that pattern-matches on
    // them (Terraform, CDK) sees the same length and charset.
    let s = uuid::Uuid::new_v4().simple().to_string();
    s[..10].to_string()
}
