//! Origin Access Control + the four policy resources (cache, origin
//! request, response headers, continuous deployment). Models are defined
//! here and the [`CloudFrontService`] handlers live in this module too —
//! `service.rs` only dispatches by action name.
//!
//! AWS-managed policies for `CachePolicy`, `OriginRequestPolicy`, and
//! `ResponseHeadersPolicy` are pre-seeded by [`seed_managed`] so
//! Terraform / CDK code that looks them up by their well-known IDs
//! resolves them without the caller having to create them first.

use chrono::{DateTime, Utc};
use http::header::{ETAG, IF_MATCH, LOCATION};
use http::{HeaderMap, HeaderValue, StatusCode};
use serde::{Deserialize, Serialize};

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError, ResponseBody};

use crate::router::Route;
use crate::service::{aws_error, esc, invalid_argument, xml_response};
use crate::state::AccountState;

const XML_DECL: &str = r#"<?xml version="1.0" encoding="UTF-8"?>"#;
const NS: &str = crate::NAMESPACE;

fn skip_if_none<T>(x: &Option<T>) -> bool {
    x.is_none()
}

// ─── Origin Access Control ────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct OriginAccessControlConfig {
    pub name: String,
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub description: Option<String>,
    pub signing_protocol: String,
    pub signing_behavior: String,
    pub origin_access_control_origin_type: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredOriginAccessControl {
    pub id: String,
    pub etag: String,
    pub config: OriginAccessControlConfig,
}

// ─── Cache Policy ─────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct CachePolicyConfig {
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub comment: Option<String>,
    pub name: String,
    #[serde(rename = "DefaultTTL", default, skip_serializing_if = "skip_if_none")]
    pub default_ttl: Option<i64>,
    #[serde(rename = "MaxTTL", default, skip_serializing_if = "skip_if_none")]
    pub max_ttl: Option<i64>,
    #[serde(rename = "MinTTL")]
    pub min_ttl: i64,
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub parameters_in_cache_key_and_forwarded_to_origin:
        Option<ParametersInCacheKeyAndForwardedToOrigin>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct ParametersInCacheKeyAndForwardedToOrigin {
    pub enable_accept_encoding_gzip: bool,
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub enable_accept_encoding_brotli: Option<bool>,
    pub headers_config: CachePolicyHeadersConfig,
    pub cookies_config: CachePolicyCookiesConfig,
    pub query_strings_config: CachePolicyQueryStringsConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct CachePolicyHeadersConfig {
    pub header_behavior: String,
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub headers: Option<NameWrapper>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct NameWrapper {
    pub quantity: i32,
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub items: Option<NameItems>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct NameItems {
    #[serde(default, rename = "Name")]
    pub name: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct CachePolicyCookiesConfig {
    pub cookie_behavior: String,
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub cookies: Option<NameWrapper>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct CachePolicyQueryStringsConfig {
    pub query_string_behavior: String,
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub query_strings: Option<NameWrapper>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredCachePolicy {
    pub id: String,
    pub etag: String,
    pub last_modified_time: DateTime<Utc>,
    pub config: CachePolicyConfig,
    /// "managed" or "custom".
    pub policy_type: String,
}

// ─── Origin Request Policy ────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct OriginRequestPolicyConfig {
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub comment: Option<String>,
    pub name: String,
    pub headers_config: OriginRequestPolicyHeadersConfig,
    pub cookies_config: OriginRequestPolicyCookiesConfig,
    pub query_strings_config: OriginRequestPolicyQueryStringsConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct OriginRequestPolicyHeadersConfig {
    pub header_behavior: String,
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub headers: Option<NameWrapper>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct OriginRequestPolicyCookiesConfig {
    pub cookie_behavior: String,
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub cookies: Option<NameWrapper>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct OriginRequestPolicyQueryStringsConfig {
    pub query_string_behavior: String,
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub query_strings: Option<NameWrapper>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredOriginRequestPolicy {
    pub id: String,
    pub etag: String,
    pub last_modified_time: DateTime<Utc>,
    pub config: OriginRequestPolicyConfig,
    pub policy_type: String,
}

// ─── Response Headers Policy ──────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct ResponseHeadersPolicyConfig {
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub comment: Option<String>,
    pub name: String,
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub cors_config: Option<ResponseHeadersPolicyCorsConfig>,
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub security_headers_config: Option<ResponseHeadersPolicySecurityHeadersConfig>,
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub server_timing_headers_config: Option<ResponseHeadersPolicyServerTimingHeadersConfig>,
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub custom_headers_config: Option<ResponseHeadersPolicyCustomHeadersConfig>,
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub remove_headers_config: Option<ResponseHeadersPolicyRemoveHeadersConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct ResponseHeadersPolicyCorsConfig {
    pub access_control_allow_origins: NameWrapper,
    pub access_control_allow_headers: NameWrapper,
    pub access_control_allow_methods: ResponseHeadersPolicyAccessControlAllowMethods,
    pub access_control_allow_credentials: bool,
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub access_control_expose_headers: Option<NameWrapper>,
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub access_control_max_age_sec: Option<i32>,
    pub origin_override: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct ResponseHeadersPolicyAccessControlAllowMethods {
    pub quantity: i32,
    pub items: ResponseHeadersPolicyMethodItems,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct ResponseHeadersPolicyMethodItems {
    #[serde(default, rename = "Method")]
    pub method: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct ResponseHeadersPolicySecurityHeadersConfig {
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub xss_protection: Option<XssProtection>,
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub frame_options: Option<FrameOptions>,
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub referrer_policy: Option<ReferrerPolicy>,
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub content_security_policy: Option<ContentSecurityPolicy>,
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub content_type_options: Option<ContentTypeOptions>,
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub strict_transport_security: Option<StrictTransportSecurity>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct XssProtection {
    #[serde(rename = "Override")]
    pub override_: bool,
    pub protection: bool,
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub mode_block: Option<bool>,
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub report_uri: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct FrameOptions {
    #[serde(rename = "Override")]
    pub override_: bool,
    pub frame_option: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct ReferrerPolicy {
    #[serde(rename = "Override")]
    pub override_: bool,
    pub referrer_policy: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct ContentSecurityPolicy {
    #[serde(rename = "Override")]
    pub override_: bool,
    pub content_security_policy: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct ContentTypeOptions {
    #[serde(rename = "Override")]
    pub override_: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct StrictTransportSecurity {
    #[serde(rename = "Override")]
    pub override_: bool,
    pub access_control_max_age_sec: i32,
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub include_subdomains: Option<bool>,
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub preload: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct ResponseHeadersPolicyServerTimingHeadersConfig {
    pub enabled: bool,
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub sampling_rate: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct ResponseHeadersPolicyCustomHeadersConfig {
    pub quantity: i32,
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub items: Option<ResponseHeadersPolicyCustomHeaderItems>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct ResponseHeadersPolicyCustomHeaderItems {
    #[serde(default, rename = "ResponseHeadersPolicyCustomHeader")]
    pub response_headers_policy_custom_header: Vec<ResponseHeadersPolicyCustomHeader>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct ResponseHeadersPolicyCustomHeader {
    pub header: String,
    pub value: String,
    #[serde(rename = "Override")]
    pub override_: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct ResponseHeadersPolicyRemoveHeadersConfig {
    pub quantity: i32,
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub items: Option<ResponseHeadersPolicyRemoveHeaderItems>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct ResponseHeadersPolicyRemoveHeaderItems {
    #[serde(default, rename = "ResponseHeadersPolicyRemoveHeader")]
    pub response_headers_policy_remove_header: Vec<ResponseHeadersPolicyRemoveHeader>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct ResponseHeadersPolicyRemoveHeader {
    pub header: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredResponseHeadersPolicy {
    pub id: String,
    pub etag: String,
    pub last_modified_time: DateTime<Utc>,
    pub config: ResponseHeadersPolicyConfig,
    pub policy_type: String,
}

// ─── Continuous Deployment Policy ─────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct ContinuousDeploymentPolicyConfig {
    pub staging_distribution_dns_names: StagingDistributionDnsNames,
    pub enabled: bool,
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub traffic_config: Option<TrafficConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct StagingDistributionDnsNames {
    pub quantity: i32,
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub items: Option<StagingDistributionDnsNameItems>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct StagingDistributionDnsNameItems {
    #[serde(default, rename = "DnsName")]
    pub dns_name: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct TrafficConfig {
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub single_weight_config: Option<ContinuousDeploymentSingleWeightConfig>,
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub single_header_config: Option<ContinuousDeploymentSingleHeaderConfig>,
    #[serde(rename = "Type")]
    pub traffic_type: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct ContinuousDeploymentSingleWeightConfig {
    pub weight: f32,
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub session_stickiness_config: Option<SessionStickinessConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct SessionStickinessConfig {
    pub idle_ttl: i32,
    pub maximum_ttl: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct ContinuousDeploymentSingleHeaderConfig {
    pub header: String,
    pub value: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredContinuousDeploymentPolicy {
    pub id: String,
    pub etag: String,
    pub last_modified_time: DateTime<Utc>,
    pub config: ContinuousDeploymentPolicyConfig,
}

// ─── Managed-policy seeding ───────────────────────────────────────────

/// Pre-populate AWS-managed policies. Called once per account on first
/// touch from `CloudFrontService`. Mirrors the IDs / names AWS returns
/// from `aws cloudfront list-cache-policies --type managed` so
/// Terraform / CDK lookups by well-known ID resolve.
pub fn seed_managed(account: &mut AccountState) {
    if !account.cache_policies.is_empty() {
        return;
    }
    let now = Utc::now();
    for (id, name, default_ttl, max_ttl, gzip, brotli) in MANAGED_CACHE_POLICIES {
        account.cache_policies.insert(
            (*id).to_string(),
            StoredCachePolicy {
                id: (*id).to_string(),
                etag: format!("MANAGED-{id}"),
                last_modified_time: now,
                policy_type: "managed".to_string(),
                config: CachePolicyConfig {
                    name: (*name).to_string(),
                    comment: Some(format!("AWS managed cache policy {name}")),
                    default_ttl: Some(*default_ttl),
                    max_ttl: Some(*max_ttl),
                    min_ttl: 1,
                    parameters_in_cache_key_and_forwarded_to_origin: Some(
                        ParametersInCacheKeyAndForwardedToOrigin {
                            enable_accept_encoding_gzip: *gzip,
                            enable_accept_encoding_brotli: Some(*brotli),
                            headers_config: CachePolicyHeadersConfig {
                                header_behavior: "none".to_string(),
                                headers: None,
                            },
                            cookies_config: CachePolicyCookiesConfig {
                                cookie_behavior: "none".to_string(),
                                cookies: None,
                            },
                            query_strings_config: CachePolicyQueryStringsConfig {
                                query_string_behavior: "none".to_string(),
                                query_strings: None,
                            },
                        },
                    ),
                },
            },
        );
    }
    for (id, name) in MANAGED_ORIGIN_REQUEST_POLICIES {
        account.origin_request_policies.insert(
            (*id).to_string(),
            StoredOriginRequestPolicy {
                id: (*id).to_string(),
                etag: format!("MANAGED-{id}"),
                last_modified_time: now,
                policy_type: "managed".to_string(),
                config: OriginRequestPolicyConfig {
                    name: (*name).to_string(),
                    comment: Some(format!("AWS managed origin request policy {name}")),
                    headers_config: OriginRequestPolicyHeadersConfig {
                        header_behavior: "allViewer".to_string(),
                        headers: None,
                    },
                    cookies_config: OriginRequestPolicyCookiesConfig {
                        cookie_behavior: "all".to_string(),
                        cookies: None,
                    },
                    query_strings_config: OriginRequestPolicyQueryStringsConfig {
                        query_string_behavior: "all".to_string(),
                        query_strings: None,
                    },
                },
            },
        );
    }
    for (id, name) in MANAGED_RESPONSE_HEADERS_POLICIES {
        account.response_headers_policies.insert(
            (*id).to_string(),
            StoredResponseHeadersPolicy {
                id: (*id).to_string(),
                etag: format!("MANAGED-{id}"),
                last_modified_time: now,
                policy_type: "managed".to_string(),
                config: ResponseHeadersPolicyConfig {
                    name: (*name).to_string(),
                    comment: Some(format!("AWS managed response headers policy {name}")),
                    cors_config: None,
                    security_headers_config: None,
                    server_timing_headers_config: None,
                    custom_headers_config: None,
                    remove_headers_config: None,
                },
            },
        );
    }
}

const MANAGED_CACHE_POLICIES: &[(&str, &str, i64, i64, bool, bool)] = &[
    (
        "658327ea-f89d-4fab-a63d-7e88639e58f6",
        "Managed-CachingOptimized",
        86400,
        31536000,
        true,
        true,
    ),
    (
        "4135ea2d-6df8-44a3-9df3-4b5a84be39ad",
        "Managed-CachingDisabled",
        0,
        0,
        false,
        false,
    ),
    (
        "b2884449-e4de-46a7-ac36-70bc7f1ddd6d",
        "Managed-CachingOptimizedForUncompressedObjects",
        86400,
        31536000,
        false,
        false,
    ),
    (
        "08627262-05a9-4f76-9ded-b50ca2e3a84f",
        "Managed-Elemental-MediaPackage",
        86400,
        31536000,
        true,
        true,
    ),
    (
        "83da9c7e-98b4-4e11-a168-04f0df8e2c65",
        "Managed-AmplifyDefault",
        2,
        600,
        true,
        true,
    ),
];

const MANAGED_ORIGIN_REQUEST_POLICIES: &[(&str, &str)] = &[
    (
        "88a5eaf4-2fd4-4709-b370-b4c650ea3fcf",
        "Managed-CORS-S3Origin",
    ),
    (
        "59781a5b-3903-41f3-afcb-af62929ccde1",
        "Managed-CORS-CustomOrigin",
    ),
    ("b689b0a8-53d0-40ab-baf2-68738e2966ac", "Managed-AllViewer"),
    (
        "33f36d7e-f396-46d9-90e0-52428a34d9dc",
        "Managed-UserAgentRefererHeaders",
    ),
    (
        "775133bc-15f2-49f9-abea-afb2e0bf67d2",
        "Managed-AllViewerExceptHostHeader",
    ),
    (
        "acba4595-bd28-49b8-b9fe-13317c0390fa",
        "Managed-AllViewerAndCloudFrontHeaders-2022-06",
    ),
];

const MANAGED_RESPONSE_HEADERS_POLICIES: &[(&str, &str)] = &[
    (
        "5cc3b908-e619-4b99-88e5-2cf7f45965bd",
        "Managed-CORS-with-preflight",
    ),
    (
        "e61eb60c-9c35-4d20-a928-2b84e02af89c",
        "Managed-CORS-and-SecurityHeadersPolicy",
    ),
    (
        "67f7725c-6f97-4210-82d7-5512b31e9d03",
        "Managed-SecurityHeadersPolicy",
    ),
    (
        "eaab4381-ed33-4a86-88ca-d9558dc6cd63",
        "Managed-CORS-with-preflight-and-SecurityHeadersPolicy",
    ),
    ("60669652-455b-4ae9-85a4-c4c02393f86c", "Managed-SimpleCORS"),
];

// ─── Handlers (impl on CloudFrontService is in service.rs via these
// free functions, which take `&self_state` rather than `&self` so this
// module doesn't have to know about the service type) ────────────────

use crate::state::SharedCloudFrontState;

pub(crate) fn touch_account(state: &SharedCloudFrontState, account_id: &str) {
    let mut s = state.write();
    let needs_seed = s
        .accounts
        .get(account_id)
        .is_none_or(|a| a.cache_policies.is_empty());
    if needs_seed {
        let account = s.entry(account_id);
        seed_managed(account);
    }
}

#[derive(Clone)]
pub(crate) struct PolicyView {
    pub id: String,
    pub last_modified_time: DateTime<Utc>,
    pub config_xml: String,
}

impl From<StoredCachePolicy> for PolicyView {
    fn from(p: StoredCachePolicy) -> Self {
        let config_xml =
            quick_xml::se::to_string_with_root("CachePolicyConfig", &p.config).unwrap_or_default();
        Self {
            id: p.id,
            last_modified_time: p.last_modified_time,
            config_xml,
        }
    }
}

impl From<StoredOriginRequestPolicy> for PolicyView {
    fn from(p: StoredOriginRequestPolicy) -> Self {
        let config_xml = quick_xml::se::to_string_with_root("OriginRequestPolicyConfig", &p.config)
            .unwrap_or_default();
        Self {
            id: p.id,
            last_modified_time: p.last_modified_time,
            config_xml,
        }
    }
}

impl From<StoredResponseHeadersPolicy> for PolicyView {
    fn from(p: StoredResponseHeadersPolicy) -> Self {
        let config_xml =
            quick_xml::se::to_string_with_root("ResponseHeadersPolicyConfig", &p.config)
                .unwrap_or_default();
        Self {
            id: p.id,
            last_modified_time: p.last_modified_time,
            config_xml,
        }
    }
}

impl From<StoredContinuousDeploymentPolicy> for PolicyView {
    fn from(p: StoredContinuousDeploymentPolicy) -> Self {
        let config_xml =
            quick_xml::se::to_string_with_root("ContinuousDeploymentPolicyConfig", &p.config)
                .unwrap_or_default();
        Self {
            id: p.id,
            last_modified_time: p.last_modified_time,
            config_xml,
        }
    }
}

pub(crate) fn render_simple_policy(p: PolicyView, root: &str) -> String {
    let mut out = String::with_capacity(512);
    out.push_str(XML_DECL);
    out.push_str(&format!("<{root} xmlns=\"{NS}\">"));
    out.push_str(&format!("<Id>{}</Id>", esc(&p.id)));
    out.push_str(&format!(
        "<LastModifiedTime>{}</LastModifiedTime>",
        rfc3339(&p.last_modified_time)
    ));
    out.push_str(&p.config_xml);
    out.push_str(&format!("</{root}>"));
    out
}

pub(crate) fn render_oac(oac: &StoredOriginAccessControl) -> String {
    let mut out = String::with_capacity(384);
    out.push_str(XML_DECL);
    out.push_str(&format!("<OriginAccessControl xmlns=\"{NS}\">"));
    out.push_str(&format!("<Id>{}</Id>", esc(&oac.id)));
    out.push_str(
        &quick_xml::se::to_string_with_root("OriginAccessControlConfig", &oac.config)
            .unwrap_or_default(),
    );
    out.push_str("</OriginAccessControl>");
    out
}

pub(crate) fn xml_with_etag(
    status: StatusCode,
    body: String,
    etag: &str,
    location_id: Option<&str>,
) -> AwsResponse {
    let mut headers = HeaderMap::new();
    if let Ok(v) = HeaderValue::from_str(etag) {
        headers.insert(ETAG, v);
    }
    if let Some(id) = location_id {
        if let Ok(v) = HeaderValue::from_str(id) {
            headers.insert(LOCATION, v);
        }
    }
    xml_response(status, body, headers)
}

pub(crate) fn empty(status: StatusCode) -> AwsResponse {
    AwsResponse {
        status,
        content_type: "text/xml".to_string(),
        body: ResponseBody::Bytes(bytes::Bytes::new()),
        headers: HeaderMap::new(),
    }
}

pub(crate) fn route_id(route: &Route, what: &str) -> Result<String, AwsServiceError> {
    route
        .id
        .clone()
        .ok_or_else(|| invalid_argument(format!("missing {what} id")))
}

pub(crate) fn require_if_match(req: &AwsRequest) -> Result<String, AwsServiceError> {
    req.headers
        .get(IF_MATCH)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string())
        .ok_or_else(|| {
            aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidIfMatchVersion",
                "Missing If-Match header",
            )
        })
}

/// Map a logical resource "kind" to the Smithy-declared error shape name used
/// on the CloudFront wire. Most resources use the `NoSuch{Kind}` pattern, but
/// several do not:
///
/// - `Function` -> `NoSuchFunctionExists` (the actual Smithy shape name)
/// - `KeyGroup` -> `NoSuchResource` (per Get/Update/DeleteKeyGroup errors)
/// - `FieldLevelEncryption` -> `NoSuchFieldLevelEncryptionConfig`
/// - Newer "tenant-era" resources (DistributionTenant, ConnectionGroup,
///   ConnectionFunction, VpcOrigin, AnycastIpList, KeyValueStore, TrustStore,
///   ResourcePolicy) all share the unified `EntityNotFound` error shape.
fn not_found_code(kind: &str) -> &'static str {
    match kind {
        "Function" => "NoSuchFunctionExists",
        "KeyGroup" => "NoSuchResource",
        "FieldLevelEncryption" => "NoSuchFieldLevelEncryptionConfig",
        "AnycastIpList" | "ConnectionFunction" | "ConnectionGroup" | "DistributionTenant"
        | "KeyValueStore" | "ResourcePolicy" | "TrustStore" | "VpcOrigin" => "EntityNotFound",
        _ => "",
    }
}

pub(crate) fn not_found(kind: &str, id: &str) -> AwsServiceError {
    let code = not_found_code(kind);
    let code = if code.is_empty() {
        // Default to `NoSuch{Kind}` for the long tail of resources whose
        // Smithy shape follows that convention (Distribution, CachePolicy,
        // PublicKey, OriginAccessControl, OriginRequestPolicy,
        // ResponseHeadersPolicy, ContinuousDeploymentPolicy,
        // StreamingDistribution, FieldLevelEncryptionProfile,
        // CloudFrontOriginAccessIdentity, RealtimeLogConfig, Invalidation).
        format!("NoSuch{kind}")
    } else {
        code.to_string()
    };
    aws_error(
        StatusCode::NOT_FOUND,
        code,
        format!("The specified {kind} does not exist: {id}"),
    )
}

pub(crate) fn precondition_failed() -> AwsServiceError {
    aws_error(
        StatusCode::PRECONDITION_FAILED,
        "PreconditionFailed",
        "If-Match header does not match the current ETag",
    )
}

pub(crate) fn rfc3339(t: &DateTime<Utc>) -> String {
    t.to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn managed_cache_policy_ids_are_unique() {
        let mut seen = HashSet::new();
        for entry in MANAGED_CACHE_POLICIES {
            let id = entry.0;
            assert!(seen.insert(id), "duplicate cache policy id: {id}");
        }
    }

    #[test]
    fn managed_origin_request_policy_ids_are_unique() {
        let mut seen = HashSet::new();
        for (id, _) in MANAGED_ORIGIN_REQUEST_POLICIES {
            assert!(seen.insert(*id), "duplicate origin request policy id: {id}");
        }
    }

    #[test]
    fn managed_response_headers_policy_ids_are_unique() {
        let mut seen = HashSet::new();
        for (id, _) in MANAGED_RESPONSE_HEADERS_POLICIES {
            assert!(
                seen.insert(*id),
                "duplicate response headers policy id: {id}"
            );
        }
    }

    #[test]
    fn seeded_response_headers_count_matches_unique_ids() {
        let mut acc = AccountState::default();
        seed_managed(&mut acc);
        assert_eq!(
            acc.response_headers_policies.len(),
            MANAGED_RESPONSE_HEADERS_POLICIES.len(),
            "duplicate IDs would reduce the seeded count"
        );
    }
}
