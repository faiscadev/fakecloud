use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

pub type SharedApiGatewayV2State = Arc<parking_lot::RwLock<ApiGatewayV2State>>;

#[derive(Debug, Clone)]
pub struct ApiGatewayV2State {
    pub account_id: String,
    pub region: String,
    pub apis: HashMap<String, HttpApi>,
    pub routes: HashMap<String, HashMap<String, Route>>, // api-id -> (route-id -> Route)
    pub integrations: HashMap<String, HashMap<String, Integration>>, // api-id -> (integration-id -> Integration)
}

impl ApiGatewayV2State {
    pub fn new(account_id: &str, region: &str) -> Self {
        Self {
            account_id: account_id.to_string(),
            region: region.to_string(),
            apis: HashMap::new(),
            routes: HashMap::new(),
            integrations: HashMap::new(),
        }
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
    pub tags: Option<HashMap<String, String>>,
    pub created_date: DateTime<Utc>,
    pub api_endpoint: String,
}

impl HttpApi {
    pub fn new(
        api_id: String,
        name: String,
        description: Option<String>,
        tags: Option<HashMap<String, String>>,
        region: &str,
    ) -> Self {
        let created_date = Utc::now();
        let api_endpoint = format!("https://{}.execute-api.{}.amazonaws.com", api_id, region);

        Self {
            api_id,
            name,
            protocol_type: "HTTP".to_string(),
            description,
            tags,
            created_date,
            api_endpoint,
        }
    }
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
}
