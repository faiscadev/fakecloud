//! Data types for CloudFront Batch 6a: VPC Origins, Anycast IP Lists,
//! Trust Stores, Resource Policies.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

fn skip_if_none<T>(x: &Option<T>) -> bool {
    x.is_none()
}

// ─── VPC Origin ───────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct VpcOriginEndpointConfig {
    pub name: String,
    pub arn: String,
    #[serde(rename = "HTTPPort")]
    pub http_port: i32,
    #[serde(rename = "HTTPSPort")]
    pub https_port: i32,
    pub origin_protocol_policy: String,
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub origin_ssl_protocols: Option<OriginSslProtocols>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct OriginSslProtocols {
    pub quantity: i32,
    pub items: SslProtocolItems,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct SslProtocolItems {
    #[serde(default, rename = "SslProtocol")]
    pub ssl_protocol: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct CreateVpcOriginRequest {
    pub vpc_origin_endpoint_config: VpcOriginEndpointConfig,
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub tags: Option<crate::model::Tags>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredVpcOrigin {
    pub id: String,
    pub arn: String,
    pub status: String,
    pub etag: String,
    pub created_time: DateTime<Utc>,
    pub last_modified_time: DateTime<Utc>,
    pub config: VpcOriginEndpointConfig,
}

// ─── Anycast IP List ──────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct CreateAnycastIpListRequest {
    pub name: String,
    pub ip_count: i32,
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub ip_address_type: Option<String>,
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub ipam_cidr_configs: Option<IpamCidrConfigList>,
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub tags: Option<crate::model::Tags>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct UpdateAnycastIpListRequest {
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub ip_address_type: Option<String>,
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub ipam_cidr_configs: Option<IpamCidrConfigList>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct IpamCidrConfigList {
    #[serde(default, rename = "IpamCidrConfig")]
    pub ipam_cidr_config: Vec<IpamCidrConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct IpamCidrConfig {
    pub cidr: String,
    pub ipam_pool_arn: String,
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub anycast_ip: Option<String>,
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub status: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredAnycastIpList {
    pub id: String,
    pub name: String,
    pub status: String,
    pub arn: String,
    pub ip_count: i32,
    pub ip_address_type: Option<String>,
    pub anycast_ips: Vec<String>,
    pub last_modified_time: DateTime<Utc>,
    pub etag: String,
    /// The create-time IPAM CIDR configuration, retained so GetAnycastIpList
    /// echoes it back as `IpamConfig`. `None` when the list was created without
    /// bring-your-own-IP IPAM configs.
    #[serde(default)]
    pub ipam_cidr_configs: Vec<IpamCidrConfig>,
}

// ─── Trust Store ──────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct CreateTrustStoreRequest {
    pub name: String,
    pub ca_certificates_bundle_source: CaCertificatesBundleSource,
    #[serde(
        rename = "UseClientCertificateOCSPEndpoint",
        default,
        skip_serializing_if = "skip_if_none"
    )]
    pub use_client_certificate_ocsp_endpoint: Option<bool>,
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub tags: Option<crate::model::Tags>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct CaCertificatesBundleSource {
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub ca_certificates_bundle_s3_location: Option<CaCertificatesBundleS3Location>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct CaCertificatesBundleS3Location {
    pub bucket: String,
    pub key: String,
    pub region: String,
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub version: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredTrustStore {
    pub id: String,
    pub arn: String,
    pub name: String,
    pub etag: String,
    pub last_modified_time: DateTime<Utc>,
    pub ca_certificates_bundle_source: CaCertificatesBundleSource,
    /// Whether OCSP revocation checking of client certificates is enabled,
    /// supplied at CreateTrustStore and echoed by GetTrustStore. Previously
    /// dropped (bug-hunt).
    #[serde(default)]
    pub use_client_certificate_ocsp_endpoint: Option<bool>,
}

// ─── Resource Policy ──────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct ResourcePolicyRequest {
    pub resource_arn: String,
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub policy_document: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredResourcePolicy {
    pub resource_arn: String,
    pub policy_document: String,
}
