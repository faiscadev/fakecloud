use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use tokio::sync::mpsc::UnboundedSender;

pub type SharedApiGatewayV2State =
    Arc<parking_lot::RwLock<fakecloud_core::multi_account::MultiAccountState<ApiGatewayV2State>>>;

/// Registry of live WebSocket connections, keyed by AWS-style connection id.
/// Lives outside the persisted `ApiGatewayV2State` because senders can't be
/// serialized; a single registry covers all accounts and APIs since connection
/// ids are globally unique within a fakecloud process.
pub type SharedWebSocketRegistry = Arc<parking_lot::RwLock<WebSocketRegistry>>;

#[derive(Debug, Default)]
pub struct WebSocketRegistry {
    pub connections: HashMap<String, ConnectionInfo>,
}

impl WebSocketRegistry {
    pub fn insert(&mut self, info: ConnectionInfo) {
        self.connections.insert(info.connection_id.clone(), info);
    }

    pub fn remove(&mut self, connection_id: &str) -> Option<ConnectionInfo> {
        self.connections.remove(connection_id)
    }

    pub fn get(&self, connection_id: &str) -> Option<&ConnectionInfo> {
        self.connections.get(connection_id)
    }

    pub fn contains(&self, connection_id: &str) -> bool {
        self.connections.contains_key(connection_id)
    }

    /// Bump `last_active_at` to now. AWS updates this whenever traffic flows
    /// in either direction; we mirror that on inbound frames and outbound
    /// `PostToConnection` calls.
    pub fn touch(&mut self, connection_id: &str) {
        if let Some(info) = self.connections.get_mut(connection_id) {
            info.last_active_at = Utc::now();
        }
    }
}

#[derive(Debug, Clone)]
pub struct ConnectionInfo {
    pub connection_id: String,
    pub api_id: String,
    pub stage: String,
    pub connected_at: DateTime<Utc>,
    pub last_active_at: DateTime<Utc>,
    pub source_ip: String,
    /// User-Agent header captured at WebSocket upgrade. Returned via
    /// `GetConnection.identity.userAgent`. Optional: real AWS omits the
    /// field entirely when the client didn't send a User-Agent.
    pub user_agent: Option<String>,
    /// Outbound channel — `axum::extract::ws::Message::Close` triggers a close.
    pub sender: UnboundedSender<axum::extract::ws::Message>,
}

impl fakecloud_core::multi_account::AccountState for ApiGatewayV2State {
    fn new_for_account(account_id: &str, region: &str, _endpoint: &str) -> Self {
        Self::new(account_id, region)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApiGatewayV2State {
    pub account_id: String,
    pub region: String,
    #[serde(default)]
    pub apis: BTreeMap<String, HttpApi>,
    #[serde(default)]
    pub routes: BTreeMap<String, BTreeMap<String, Route>>,
    #[serde(default)]
    pub integrations: BTreeMap<String, BTreeMap<String, Integration>>,
    #[serde(default)]
    pub stages: BTreeMap<String, BTreeMap<String, Stage>>,
    #[serde(default)]
    pub deployments: BTreeMap<String, BTreeMap<String, Deployment>>,
    #[serde(default)]
    pub authorizers: BTreeMap<String, BTreeMap<String, Authorizer>>,
    /// Introspection-only buffer backing `/_fakecloud/apigatewayv2/requests`.
    /// Intentionally not persisted across restarts.
    #[serde(default, skip_serializing)]
    pub request_history: Vec<ApiRequest>,
    /// Per-resource generic stores for ops added in the closure batch.
    /// Each map values are JSON bodies the API gateway returns verbatim.
    #[serde(default)]
    pub domain_names: BTreeMap<String, serde_json::Value>,
    /// Per-domain api mappings keyed by mapping id.
    #[serde(default)]
    pub api_mappings: BTreeMap<String, BTreeMap<String, serde_json::Value>>,
    /// Per-api models keyed by model id.
    #[serde(default)]
    pub models: BTreeMap<String, BTreeMap<String, serde_json::Value>>,
    /// Per-api integration responses keyed by `{integration}/{response}`.
    #[serde(default)]
    pub integration_responses: BTreeMap<String, BTreeMap<String, serde_json::Value>>,
    /// Per-api route responses keyed by `{route}/{response}`.
    #[serde(default)]
    pub route_responses: BTreeMap<String, BTreeMap<String, serde_json::Value>>,
    /// Routing rules keyed by domain name + rule id.
    #[serde(default)]
    pub routing_rules: BTreeMap<String, BTreeMap<String, serde_json::Value>>,
    /// VPC links keyed by id.
    #[serde(default)]
    pub vpc_links: BTreeMap<String, serde_json::Value>,
    /// Tags keyed by resource ARN.
    #[serde(default)]
    pub tags: BTreeMap<String, BTreeMap<String, String>>,
    /// Portals + portal products by id.
    #[serde(default)]
    pub portals: BTreeMap<String, serde_json::Value>,
    #[serde(default)]
    pub portal_products: BTreeMap<String, serde_json::Value>,
    /// Sharing policies keyed by portal product id.
    #[serde(default)]
    pub portal_product_sharing_policies: BTreeMap<String, serde_json::Value>,
    /// Product pages and rest endpoint pages keyed by `{portal-product}/{page}`.
    #[serde(default)]
    pub product_pages: BTreeMap<String, BTreeMap<String, serde_json::Value>>,
    #[serde(default)]
    pub product_rest_endpoint_pages: BTreeMap<String, BTreeMap<String, serde_json::Value>>,
}

pub const APIGATEWAYV2_SNAPSHOT_SCHEMA_VERSION: u32 = 2;

#[derive(Debug, Serialize, Deserialize)]
pub struct ApiGatewayV2Snapshot {
    pub schema_version: u32,
    #[serde(default)]
    pub accounts: Option<fakecloud_core::multi_account::MultiAccountState<ApiGatewayV2State>>,
    #[serde(default)]
    pub state: Option<ApiGatewayV2State>,
}

impl ApiGatewayV2State {
    pub fn new(account_id: &str, region: &str) -> Self {
        Self {
            account_id: account_id.to_string(),
            region: region.to_string(),
            apis: BTreeMap::new(),
            routes: BTreeMap::new(),
            integrations: BTreeMap::new(),
            stages: BTreeMap::new(),
            deployments: BTreeMap::new(),
            authorizers: BTreeMap::new(),
            request_history: Vec::new(),
            domain_names: BTreeMap::new(),
            api_mappings: BTreeMap::new(),
            models: BTreeMap::new(),
            integration_responses: BTreeMap::new(),
            route_responses: BTreeMap::new(),
            routing_rules: BTreeMap::new(),
            vpc_links: BTreeMap::new(),
            tags: BTreeMap::new(),
            portals: BTreeMap::new(),
            portal_products: BTreeMap::new(),
            portal_product_sharing_policies: BTreeMap::new(),
            product_pages: BTreeMap::new(),
            product_rest_endpoint_pages: BTreeMap::new(),
        }
    }

    pub fn reset(&mut self) {
        self.apis.clear();
        self.routes.clear();
        self.integrations.clear();
        self.stages.clear();
        self.deployments.clear();
        self.authorizers.clear();
        self.request_history.clear();
        self.domain_names.clear();
        self.api_mappings.clear();
        self.models.clear();
        self.integration_responses.clear();
        self.route_responses.clear();
        self.routing_rules.clear();
        self.vpc_links.clear();
        self.tags.clear();
        self.portals.clear();
        self.portal_products.clear();
        self.portal_product_sharing_policies.clear();
        self.product_pages.clear();
        self.product_rest_endpoint_pages.clear();
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HttpApi {
    pub api_id: String,
    pub name: String,
    pub protocol_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cors_configuration: Option<CorsConfiguration>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tags: Option<BTreeMap<String, String>>,
    pub created_date: DateTime<Utc>,
    pub api_endpoint: String,
    /// Real AWS API Gateway v2 always returns this on GetApi, defaulting
    /// to `$request.header.x-api-key` for HTTP APIs. The Terraform
    /// `aws_apigatewayv2_api` provider asserts on the value.
    pub api_key_selection_expression: String,
    /// Real AWS always returns this on GetApi, defaulting to
    /// `$request.method $request.path` for HTTP APIs. Same Terraform
    /// assertion pattern as the api_key_selection_expression above.
    pub route_selection_expression: String,
    /// Disabled by default; honoured at create time if the caller sets it.
    pub disable_execute_api_endpoint: bool,
    /// `ipv4` (default) or `dualstack`. Real AWS always returns this on
    /// GetApi and Terraform's provider asserts on it.
    pub ip_address_type: String,
}

impl HttpApi {
    pub fn new(
        api_id: String,
        name: String,
        description: Option<String>,
        tags: Option<BTreeMap<String, String>>,
        region: &str,
    ) -> Self {
        let created_date = Utc::now();
        let api_endpoint = format!("https://{}.execute-api.{}.amazonaws.com", api_id, region);

        Self {
            api_id,
            name,
            protocol_type: "HTTP".to_string(),
            description,
            cors_configuration: None,
            tags,
            created_date,
            api_endpoint,
            api_key_selection_expression: "$request.header.x-api-key".to_string(),
            route_selection_expression: "$request.method $request.path".to_string(),
            disable_execute_api_endpoint: false,
            ip_address_type: "ipv4".to_string(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CorsConfiguration {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub allow_credentials: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub allow_headers: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub allow_methods: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub allow_origins: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expose_headers: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_age: Option<i32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Route {
    pub route_id: String,
    pub route_key: String, // "GET /pets/{id}"
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target: Option<String>, // "integrations/{integration-id}"
    #[serde(skip_serializing_if = "Option::is_none")]
    pub authorization_type: Option<String>, // "NONE", "JWT", "CUSTOM"
    #[serde(skip_serializing_if = "Option::is_none")]
    pub authorizer_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Integration {
    pub integration_id: String,
    pub integration_type: String, // "AWS_PROXY", "HTTP_PROXY", "MOCK"
    #[serde(skip_serializing_if = "Option::is_none")]
    pub integration_uri: Option<String>, // Lambda ARN or HTTP endpoint
    #[serde(skip_serializing_if = "Option::is_none")]
    pub payload_format_version: Option<String>, // "2.0"
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timeout_in_millis: Option<i64>,
    /// HTTP method for an HTTP/HTTP_PROXY integration (e.g. `GET`). Round-tripped
    /// so the `aws_apigatewayv2_integration` resource doesn't re-plan.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub integration_method: Option<String>,
    /// Selection expression for the integration response. WEBSOCKET APIs default
    /// this to `${integration.response.statuscode}`; HTTP APIs leave it unset.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub integration_response_selection_expression: Option<String>,
    /// Passthrough behavior (`WHEN_NO_MATCH`/`WHEN_NO_TEMPLATES`/`NEVER`).
    /// WEBSOCKET APIs default to `WHEN_NO_MATCH`; HTTP APIs leave it unset.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub passthrough_behavior: Option<String>,
    /// "INTERNET" (default) or "VPC_LINK". AWS always reports it, so the
    /// Terraform provider re-plans an integration whose GetIntegration response
    /// omits it.
    #[serde(default = "default_connection_type")]
    pub connection_type: String,
}

fn default_connection_type() -> String {
    "INTERNET".to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Stage {
    pub stage_name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deployment_id: Option<String>,
    pub auto_deploy: bool,
    pub created_date: DateTime<Utc>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_updated_date: Option<DateTime<Utc>>,
    /// WebACL ARN attached to this stage. Set via `AssociateWebACL`
    /// (the WAFv2 service tracks the association keyed by the stage
    /// ARN); the v2 dataplane consults this on every request before
    /// invoking the authorizer or integration. `None` when no WebACL
    /// is attached, which is the common case.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub web_acl_arn: Option<String>,
    /// Stage variables substituted into integration URIs at request
    /// time via `${stageVariables.<name>}`.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub stage_variables: Option<BTreeMap<String, String>>,
    /// Access log destination + format for this stage.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub access_log_settings: Option<AccessLogSettings>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AccessLogSettings {
    pub destination_arn: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub format: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Deployment {
    pub deployment_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    pub created_date: DateTime<Utc>,
    pub auto_deployed: bool,
    /// fakecloud applies deployments synchronously, so they are immediately
    /// `DEPLOYED`. The Terraform provider's create-waiter polls GetDeployment
    /// for this status, so it must be reported.
    #[serde(default = "default_deployment_status")]
    pub deployment_status: String,
}

fn default_deployment_status() -> String {
    "DEPLOYED".to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Authorizer {
    pub authorizer_id: String,
    pub name: String,
    pub authorizer_type: String, // "JWT" or "REQUEST"
    #[serde(skip_serializing_if = "Option::is_none")]
    pub authorizer_uri: Option<String>, // Lambda ARN for REQUEST type
    #[serde(skip_serializing_if = "Option::is_none")]
    pub identity_source: Option<Vec<String>>, // e.g., ["$request.header.Authorization"]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub jwt_configuration: Option<JwtConfiguration>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct JwtConfiguration {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub audience: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub issuer: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ApiRequest {
    pub request_id: String,
    pub api_id: String,
    pub stage: String,
    pub method: String,
    pub path: String,
    pub headers: BTreeMap<String, String>,
    pub query_params: BTreeMap<String, String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub body: Option<String>,
    pub timestamp: DateTime<Utc>,
    pub status_code: u16,
}

/// Result of evaluating a Lambda-authorizer policy document.
#[derive(Debug, Clone, PartialEq)]
pub enum AuthEffect {
    Allow,
    Deny,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_state_empty() {
        let state = ApiGatewayV2State::new("123456789012", "us-east-1");
        assert_eq!(state.account_id, "123456789012");
        assert_eq!(state.region, "us-east-1");
        assert!(state.apis.is_empty());
        assert!(state.routes.is_empty());
        assert!(state.request_history.is_empty());
    }

    #[test]
    fn new_http_api_defaults() {
        let api = HttpApi::new(
            "abc123".to_string(),
            "my-api".to_string(),
            Some("desc".to_string()),
            None,
            "us-east-1",
        );
        assert_eq!(api.api_id, "abc123");
        assert_eq!(api.name, "my-api");
        assert_eq!(api.protocol_type, "HTTP");
        assert_eq!(
            api.api_key_selection_expression,
            "$request.header.x-api-key"
        );
        assert_eq!(
            api.route_selection_expression,
            "$request.method $request.path"
        );
        assert!(api.api_endpoint.contains("abc123"));
        assert!(api.api_endpoint.contains("us-east-1"));
        assert!(!api.disable_execute_api_endpoint);
        assert_eq!(api.ip_address_type, "ipv4");
    }
}
