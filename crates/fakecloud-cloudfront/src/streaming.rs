//! Data types for CloudFront Batch 4 resources: Streaming Distributions
//! (legacy RTMP), Field-Level Encryption, Realtime Log Configs.
//!
//! Batch 4 ships Streaming Distributions; Field-Level Encryption and
//! Realtime Log Configs land in subsequent batches as they're independent
//! resource families.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

fn skip_if_none<T>(x: &Option<T>) -> bool {
    x.is_none()
}

// ─── Streaming Distribution (legacy RTMP) ─────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct StreamingDistributionConfig {
    pub caller_reference: String,
    pub s3_origin: S3Origin,
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub aliases: Option<StreamingAliases>,
    pub comment: String,
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub logging: Option<StreamingLoggingConfig>,
    pub trusted_signers: TrustedSigners,
    /// Optional in the API; AWS applies (and reports) `PriceClass_All` when
    /// it is omitted.
    #[serde(default = "default_price_class")]
    pub price_class: String,
    pub enabled: bool,
}

fn default_price_class() -> String {
    "PriceClass_All".to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct S3Origin {
    pub domain_name: String,
    pub origin_access_identity: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct StreamingAliases {
    pub quantity: i32,
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub items: Option<AliasItems>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct AliasItems {
    #[serde(default, rename = "CNAME")]
    pub cname: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct StreamingLoggingConfig {
    pub enabled: bool,
    pub bucket: String,
    pub prefix: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct TrustedSigners {
    pub enabled: bool,
    pub quantity: i32,
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub items: Option<AwsAccountItems>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct AwsAccountItems {
    #[serde(default, rename = "AwsAccountNumber")]
    pub aws_account_number: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct StreamingDistributionConfigWithTags {
    pub streaming_distribution_config: StreamingDistributionConfig,
    pub tags: crate::model::Tags,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredStreamingDistribution {
    pub id: String,
    pub arn: String,
    pub status: String,
    pub last_modified_time: DateTime<Utc>,
    pub domain_name: String,
    pub etag: String,
    pub config: StreamingDistributionConfig,
}
