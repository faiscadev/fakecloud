//! Data types for CloudFront Batch 6b: Connection Groups, Domain ops,
//! Managed Certificate Details, UpdateDistributionWithStagingConfig.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

fn skip_if_none<T>(x: &Option<T>) -> bool {
    x.is_none()
}

// ─── Connection Group ─────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct CreateConnectionGroupRequest {
    pub name: String,
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub ipv6_enabled: Option<bool>,
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub anycast_ip_list_id: Option<String>,
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub enabled: Option<bool>,
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub tags: Option<crate::model::Tags>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct UpdateConnectionGroupRequest {
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub ipv6_enabled: Option<bool>,
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub anycast_ip_list_id: Option<String>,
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub enabled: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredConnectionGroup {
    pub id: String,
    pub name: String,
    pub arn: String,
    pub routing_endpoint: String,
    pub status: String,
    pub etag: String,
    pub created_time: DateTime<Utc>,
    pub last_modified_time: DateTime<Utc>,
    pub ipv6_enabled: bool,
    pub anycast_ip_list_id: Option<String>,
    pub enabled: bool,
    pub is_default: bool,
}
