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
}

impl ApiGatewayV2State {
    pub fn new(account_id: &str, region: &str) -> Self {
        Self {
            account_id: account_id.to_string(),
            region: region.to_string(),
            apis: HashMap::new(),
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
