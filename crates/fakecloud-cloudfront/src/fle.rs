//! Data types for CloudFront Batch 5 resources: Field-Level Encryption
//! configs + profiles, and Realtime Log Configs.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

fn skip_if_none<T>(x: &Option<T>) -> bool {
    x.is_none()
}

// ─── Field-Level Encryption Config ────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct FieldLevelEncryptionConfig {
    pub caller_reference: String,
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub comment: Option<String>,
    // Both profile configs are optional in the API; a config may carry
    // either, both, or neither.
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub query_arg_profile_config: Option<QueryArgProfileConfig>,
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub content_type_profile_config: Option<ContentTypeProfileConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct QueryArgProfileConfig {
    pub forward_when_query_arg_profile_is_unknown: bool,
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub query_arg_profiles: Option<QueryArgProfiles>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct QueryArgProfiles {
    pub quantity: i32,
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub items: Option<QueryArgProfileItems>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct QueryArgProfileItems {
    #[serde(default, rename = "QueryArgProfile")]
    pub query_arg_profile: Vec<QueryArgProfile>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct QueryArgProfile {
    pub query_arg: String,
    pub profile_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct ContentTypeProfileConfig {
    pub forward_when_content_type_is_unknown: bool,
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub content_type_profiles: Option<ContentTypeProfiles>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct ContentTypeProfiles {
    pub quantity: i32,
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub items: Option<ContentTypeProfileItems>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct ContentTypeProfileItems {
    #[serde(default, rename = "ContentTypeProfile")]
    pub content_type_profile: Vec<ContentTypeProfile>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct ContentTypeProfile {
    pub format: String,
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub profile_id: Option<String>,
    pub content_type: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredFieldLevelEncryption {
    pub id: String,
    pub etag: String,
    pub last_modified_time: DateTime<Utc>,
    pub config: FieldLevelEncryptionConfig,
}

// ─── Field-Level Encryption Profile ───────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct FieldLevelEncryptionProfileConfig {
    pub name: String,
    pub caller_reference: String,
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub comment: Option<String>,
    pub encryption_entities: EncryptionEntities,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct EncryptionEntities {
    pub quantity: i32,
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub items: Option<EncryptionEntityItems>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct EncryptionEntityItems {
    #[serde(default, rename = "EncryptionEntity")]
    pub encryption_entity: Vec<EncryptionEntity>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct EncryptionEntity {
    pub public_key_id: String,
    pub provider_id: String,
    pub field_patterns: FieldPatterns,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct FieldPatterns {
    pub quantity: i32,
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub items: Option<FieldPatternItems>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct FieldPatternItems {
    #[serde(default, rename = "FieldPattern")]
    pub field_pattern: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredFieldLevelEncryptionProfile {
    pub id: String,
    pub etag: String,
    pub last_modified_time: DateTime<Utc>,
    pub config: FieldLevelEncryptionProfileConfig,
}

// ─── Realtime Log Config ──────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct CreateRealtimeLogConfigRequest {
    pub end_points: EndPoints,
    pub fields: FieldsList,
    pub name: String,
    pub sampling_rate: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct UpdateRealtimeLogConfigRequest {
    // Every member is optional: the config is addressed by Name or ARN, and
    // only the settings present in the request are replaced.
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub end_points: Option<EndPoints>,
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub fields: Option<FieldsList>,
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub name: Option<String>,
    #[serde(default, skip_serializing_if = "skip_if_none", rename = "ARN")]
    pub arn: Option<String>,
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub sampling_rate: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct GetOrDeleteRealtimeLogConfigRequest {
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub name: Option<String>,
    #[serde(default, skip_serializing_if = "skip_if_none", rename = "ARN")]
    pub arn: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct EndPoints {
    #[serde(default, rename = "member")]
    pub member: Vec<EndPoint>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct EndPoint {
    pub stream_type: String,
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub kinesis_stream_config: Option<KinesisStreamConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct KinesisStreamConfig {
    #[serde(rename = "RoleARN")]
    pub role_arn: String,
    #[serde(rename = "StreamARN")]
    pub stream_arn: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct FieldsList {
    #[serde(default, rename = "Field")]
    pub field: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredRealtimeLogConfig {
    pub arn: String,
    pub name: String,
    pub sampling_rate: i64,
    pub end_points: EndPoints,
    pub fields: FieldsList,
}
