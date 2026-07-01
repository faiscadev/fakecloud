// CloudFront DistributionTenant data types — multi-tenant distribution
// service that lets callers carve a base distribution into per-tenant
// configurations (custom domains, certs, parameter overrides). Wire
// protocol mirrors the parent Distribution: REST-XML with ETag-based
// concurrency control.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredDistributionTenant {
    pub id: String,
    pub arn: String,
    pub name: String,
    pub distribution_id: String,
    pub domains: Vec<String>,
    pub connection_group_id: Option<String>,
    pub web_acl_arn: Option<String>,
    pub enabled: bool,
    pub status: String,
    pub etag: String,
    pub created_time: DateTime<Utc>,
    pub last_modified_time: DateTime<Utc>,
    /// Per-tenant parameter overrides (Name/Value pairs).
    #[serde(default)]
    pub parameters: Vec<TenantParameter>,
    /// WebAcl / Certificate / GeoRestrictions overrides.
    #[serde(default)]
    pub customizations: Option<TenantCustomizations>,
    /// The managed-certificate request captured at create/update time.
    #[serde(default)]
    pub managed_certificate_request: Option<TenantManagedCertificateRequest>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct TenantParameter {
    pub name: String,
    pub value: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct TenantCustomizations {
    pub web_acl: Option<TenantWebAclCustomization>,
    pub certificate: Option<String>,
    pub geo_restrictions: Option<TenantGeoRestrictionCustomization>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct TenantWebAclCustomization {
    pub action: String,
    pub arn: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct TenantGeoRestrictionCustomization {
    pub restriction_type: String,
    pub locations: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct TenantManagedCertificateRequest {
    pub validation_token_host: Option<String>,
    pub primary_domain_name: Option<String>,
    pub certificate_transparency_logging_preference: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredTenantInvalidation {
    pub id: String,
    pub tenant_id: String,
    pub status: String,
    pub create_time: DateTime<Utc>,
    pub paths: Vec<String>,
    pub caller_reference: String,
}
