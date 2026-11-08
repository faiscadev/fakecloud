//! Serde-modeled CloudFront payload types.
//!
//! CloudFront uses XML on the wire with deeply nested optional fields. We
//! define typed structs that round-trip via `quick-xml` + serde so a
//! `DistributionConfig` parsed from a `CreateDistribution` body can be
//! emitted byte-equivalent in a later `GetDistributionConfig`. Everything
//! that AWS treats as optional is `Option<_>`; lists keep the AWS
//! `Quantity`/`Items` envelope so SDK readers see the same shape.

use serde::{Deserialize, Serialize};

fn skip_if_none<T>(x: &Option<T>) -> bool {
    x.is_none()
}

// ─── DistributionConfig ───────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct DistributionConfig {
    pub caller_reference: String,
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub aliases: Option<Aliases>,
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub default_root_object: Option<String>,
    pub origins: Origins,
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub origin_groups: Option<OriginGroups>,
    pub default_cache_behavior: DefaultCacheBehavior,
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub cache_behaviors: Option<CacheBehaviors>,
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub custom_error_responses: Option<CustomErrorResponses>,
    pub comment: String,
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub logging: Option<LoggingConfig>,
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub price_class: Option<String>,
    pub enabled: bool,
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub viewer_certificate: Option<ViewerCertificate>,
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub restrictions: Option<Restrictions>,
    // AWS spells this `WebACLId` (upper-case ACL). The default PascalCase
    // rule would emit `WebAclId`, which drops the field on parse from real
    // SDK requests and mis-names it on the wire. Pin the exact name.
    #[serde(default, rename = "WebACLId", skip_serializing_if = "skip_if_none")]
    pub web_acl_id: Option<String>,
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub http_version: Option<String>,
    // AWS spells this `IsIPV6Enabled`; PascalCase would give `IsIpv6Enabled`.
    #[serde(
        default,
        rename = "IsIPV6Enabled",
        skip_serializing_if = "skip_if_none"
    )]
    pub is_ipv6_enabled: Option<bool>,
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub continuous_deployment_policy_id: Option<String>,
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub staging: Option<bool>,
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub anycast_ip_list_id: Option<String>,
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub tenant_config: Option<TenantConfig>,
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub connection_mode: Option<String>,
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub viewer_mtls_config: Option<ViewerMtlsConfig>,
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub connection_function_association: Option<ConnectionFunctionAssociation>,
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub cache_tag_config: Option<CacheTagConfig>,
}

impl DistributionConfig {
    pub fn ensure_aws_defaults(&mut self) {
        if self.origin_groups.is_none() {
            self.origin_groups = Some(OriginGroups::default());
        }
        if self.restrictions.is_none() {
            self.restrictions = Some(Restrictions::default());
        }
        if self.custom_error_responses.is_none() {
            self.custom_error_responses = Some(CustomErrorResponses::default());
        }
        if self.aliases.is_none() {
            self.aliases = Some(Aliases::default());
        }
        if self.viewer_certificate.is_none() {
            self.viewer_certificate = Some(ViewerCertificate::default());
        }
        if self.price_class.is_none() {
            self.price_class = Some("PriceClass_All".to_string());
        }
        if self.http_version.is_none() {
            self.http_version = Some("http2".to_string());
        }
        if self.is_ipv6_enabled.is_none() {
            self.is_ipv6_enabled = Some(true);
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct ViewerMtlsConfig {
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub mode: Option<String>,
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub trust_store_config: Option<TrustStoreConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct TrustStoreConfig {
    pub trust_store_id: String,
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub advertise_trust_store_ca_names: Option<bool>,
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub ignore_certificate_expiry: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct ConnectionFunctionAssociation {
    pub id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct CacheTagConfig {
    pub header_name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct OriginMtlsConfig {
    pub client_certificate_arn: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct Aliases {
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
pub struct Origins {
    pub quantity: i32,
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub items: Option<OriginItems>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct OriginItems {
    #[serde(default)]
    pub origin: Vec<Origin>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct Origin {
    pub id: String,
    pub domain_name: String,
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub origin_path: Option<String>,
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub custom_headers: Option<CustomHeaders>,
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub s3_origin_config: Option<S3OriginConfig>,
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub custom_origin_config: Option<CustomOriginConfig>,
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub vpc_origin_config: Option<VpcOriginConfig>,
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub connection_attempts: Option<i32>,
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub connection_timeout: Option<i32>,
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub origin_shield: Option<OriginShield>,
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub origin_access_control_id: Option<String>,
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub response_completion_timeout: Option<i32>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct CustomHeaders {
    pub quantity: i32,
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub items: Option<CustomHeaderItems>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct CustomHeaderItems {
    #[serde(default)]
    pub origin_custom_header: Vec<OriginCustomHeader>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct OriginCustomHeader {
    pub header_name: String,
    pub header_value: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct S3OriginConfig {
    pub origin_access_identity: String,
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub origin_read_timeout: Option<i32>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct CustomOriginConfig {
    // AWS spells these HTTPPort / HTTPSPort (not the PascalCase HttpPort), so a
    // real SDK's CreateDistribution with a custom origin round-trips. Matches
    // the casing already used in extras.rs.
    #[serde(rename = "HTTPPort")]
    pub http_port: i32,
    #[serde(rename = "HTTPSPort")]
    pub https_port: i32,
    pub origin_protocol_policy: String,
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub origin_ssl_protocols: Option<OriginSslProtocols>,
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub origin_read_timeout: Option<i32>,
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub origin_keepalive_timeout: Option<i32>,
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub ip_address_type: Option<String>,
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub origin_mtls_config: Option<OriginMtlsConfig>,
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
pub struct VpcOriginConfig {
    pub vpc_origin_id: String,
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub owner_account_id: Option<String>,
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub origin_read_timeout: Option<i32>,
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub origin_keepalive_timeout: Option<i32>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct OriginShield {
    pub enabled: bool,
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub origin_shield_region: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct OriginGroups {
    pub quantity: i32,
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub items: Option<OriginGroupItems>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct OriginGroupItems {
    #[serde(default)]
    pub origin_group: Vec<OriginGroup>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct OriginGroup {
    pub id: String,
    pub failover_criteria: OriginGroupFailoverCriteria,
    pub members: OriginGroupMembers,
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub selection_criteria: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct OriginGroupFailoverCriteria {
    pub status_codes: StatusCodes,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct StatusCodes {
    pub quantity: i32,
    pub items: StatusCodeItems,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct StatusCodeItems {
    #[serde(default, rename = "StatusCode")]
    pub status_code: Vec<i32>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct OriginGroupMembers {
    pub quantity: i32,
    pub items: OriginGroupMemberItems,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct OriginGroupMemberItems {
    #[serde(default)]
    pub origin_group_member: Vec<OriginGroupMember>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct OriginGroupMember {
    pub origin_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct DefaultCacheBehavior {
    pub target_origin_id: String,
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub trusted_signers: Option<TrustedSigners>,
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub trusted_key_groups: Option<TrustedKeyGroups>,
    pub viewer_protocol_policy: String,
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub allowed_methods: Option<AllowedMethods>,
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub smooth_streaming: Option<bool>,
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub compress: Option<bool>,
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub lambda_function_associations: Option<LambdaFunctionAssociations>,
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub function_associations: Option<FunctionAssociations>,
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub field_level_encryption_id: Option<String>,
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub realtime_log_config_arn: Option<String>,
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub cache_policy_id: Option<String>,
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub origin_request_policy_id: Option<String>,
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub response_headers_policy_id: Option<String>,
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub grpc_config: Option<GrpcConfig>,
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub forwarded_values: Option<ForwardedValues>,
    // AWS spells the TTLs `MinTTL` / `DefaultTTL` / `MaxTTL`; PascalCase would
    // give `MinTtl`, dropping them from real SDK requests.
    #[serde(default, rename = "MinTTL", skip_serializing_if = "skip_if_none")]
    pub min_ttl: Option<i64>,
    #[serde(default, rename = "DefaultTTL", skip_serializing_if = "skip_if_none")]
    pub default_ttl: Option<i64>,
    #[serde(default, rename = "MaxTTL", skip_serializing_if = "skip_if_none")]
    pub max_ttl: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct TrustedSigners {
    pub enabled: bool,
    pub quantity: i32,
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub items: Option<AwsAccountNumberList>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct AwsAccountNumberList {
    #[serde(default, rename = "AwsAccountNumber")]
    pub aws_account_number: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct TrustedKeyGroups {
    pub enabled: bool,
    pub quantity: i32,
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub items: Option<TrustedKeyGroupIdList>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct TrustedKeyGroupIdList {
    #[serde(default, rename = "KeyGroup")]
    pub key_group: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct AllowedMethods {
    pub quantity: i32,
    pub items: MethodList,
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub cached_methods: Option<CachedMethods>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct MethodList {
    #[serde(default, rename = "Method")]
    pub method: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct CachedMethods {
    pub quantity: i32,
    pub items: MethodList,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct LambdaFunctionAssociations {
    pub quantity: i32,
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub items: Option<LambdaFunctionAssociationItems>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct LambdaFunctionAssociationItems {
    #[serde(default)]
    pub lambda_function_association: Vec<LambdaFunctionAssociation>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct LambdaFunctionAssociation {
    #[serde(rename = "LambdaFunctionARN")]
    pub lambda_function_arn: String,
    pub event_type: String,
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub include_body: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct FunctionAssociations {
    pub quantity: i32,
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub items: Option<FunctionAssociationItems>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct FunctionAssociationItems {
    #[serde(default)]
    pub function_association: Vec<FunctionAssociation>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct FunctionAssociation {
    #[serde(rename = "FunctionARN")]
    pub function_arn: String,
    pub event_type: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct GrpcConfig {
    pub enabled: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct ForwardedValues {
    pub query_string: bool,
    pub cookies: CookiePreference,
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub headers: Option<Headers>,
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub query_string_cache_keys: Option<QueryStringCacheKeys>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct CookiePreference {
    pub forward: String,
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub whitelisted_names: Option<CookieNames>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct CookieNames {
    pub quantity: i32,
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub items: Option<CookieNameList>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct CookieNameList {
    #[serde(default, rename = "Name")]
    pub name: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct Headers {
    pub quantity: i32,
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub items: Option<HeaderList>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct HeaderList {
    #[serde(default, rename = "Name")]
    pub name: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct QueryStringCacheKeys {
    pub quantity: i32,
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub items: Option<QueryStringCacheKeyList>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct QueryStringCacheKeyList {
    #[serde(default, rename = "Name")]
    pub name: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct CacheBehaviors {
    pub quantity: i32,
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub items: Option<CacheBehaviorItems>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct CacheBehaviorItems {
    #[serde(default)]
    pub cache_behavior: Vec<CacheBehavior>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct CacheBehavior {
    pub path_pattern: String,
    pub target_origin_id: String,
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub trusted_signers: Option<TrustedSigners>,
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub trusted_key_groups: Option<TrustedKeyGroups>,
    pub viewer_protocol_policy: String,
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub allowed_methods: Option<AllowedMethods>,
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub smooth_streaming: Option<bool>,
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub compress: Option<bool>,
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub lambda_function_associations: Option<LambdaFunctionAssociations>,
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub function_associations: Option<FunctionAssociations>,
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub field_level_encryption_id: Option<String>,
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub realtime_log_config_arn: Option<String>,
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub cache_policy_id: Option<String>,
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub origin_request_policy_id: Option<String>,
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub response_headers_policy_id: Option<String>,
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub grpc_config: Option<GrpcConfig>,
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub forwarded_values: Option<ForwardedValues>,
    // AWS spells the TTLs `MinTTL` / `DefaultTTL` / `MaxTTL`; PascalCase would
    // give `MinTtl`, dropping them from real SDK requests.
    #[serde(default, rename = "MinTTL", skip_serializing_if = "skip_if_none")]
    pub min_ttl: Option<i64>,
    #[serde(default, rename = "DefaultTTL", skip_serializing_if = "skip_if_none")]
    pub default_ttl: Option<i64>,
    #[serde(default, rename = "MaxTTL", skip_serializing_if = "skip_if_none")]
    pub max_ttl: Option<i64>,
}

impl CacheBehavior {
    /// A path-scoped behavior carrying the same settings as `default`.
    pub fn from_default(path_pattern: String, default: DefaultCacheBehavior) -> Self {
        let DefaultCacheBehavior {
            target_origin_id,
            trusted_signers,
            trusted_key_groups,
            viewer_protocol_policy,
            allowed_methods,
            smooth_streaming,
            compress,
            lambda_function_associations,
            function_associations,
            field_level_encryption_id,
            realtime_log_config_arn,
            cache_policy_id,
            origin_request_policy_id,
            response_headers_policy_id,
            grpc_config,
            forwarded_values,
            min_ttl,
            default_ttl,
            max_ttl,
        } = default;
        Self {
            path_pattern,
            target_origin_id,
            trusted_signers,
            trusted_key_groups,
            viewer_protocol_policy,
            allowed_methods,
            smooth_streaming,
            compress,
            lambda_function_associations,
            function_associations,
            field_level_encryption_id,
            realtime_log_config_arn,
            cache_policy_id,
            origin_request_policy_id,
            response_headers_policy_id,
            grpc_config,
            forwarded_values,
            min_ttl,
            default_ttl,
            max_ttl,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct CustomErrorResponses {
    pub quantity: i32,
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub items: Option<CustomErrorResponseItems>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct CustomErrorResponseItems {
    #[serde(default)]
    pub custom_error_response: Vec<CustomErrorResponse>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct CustomErrorResponse {
    pub error_code: i32,
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub response_page_path: Option<String>,
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub response_code: Option<String>,
    // AWS spells this `ErrorCachingMinTTL` (upper-case TTL). The default
    // PascalCase rule would emit `ErrorCachingMinTtl`, which drops the field on
    // parse from real SDK requests and mis-names it on the wire. Pin the exact
    // name, as `WebACLId` above does.
    #[serde(
        default,
        rename = "ErrorCachingMinTTL",
        skip_serializing_if = "skip_if_none"
    )]
    pub error_caching_min_ttl: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct LoggingConfig {
    pub enabled: bool,
    pub include_cookies: bool,
    pub bucket: String,
    pub prefix: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct ViewerCertificate {
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub cloud_front_default_certificate: Option<bool>,
    #[serde(
        default,
        rename = "IAMCertificateId",
        skip_serializing_if = "skip_if_none"
    )]
    pub iam_certificate_id: Option<String>,
    #[serde(
        default,
        rename = "ACMCertificateArn",
        skip_serializing_if = "skip_if_none"
    )]
    pub acm_certificate_arn: Option<String>,
    #[serde(
        default,
        rename = "SSLSupportMethod",
        skip_serializing_if = "skip_if_none"
    )]
    pub ssl_support_method: Option<String>,
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub minimum_protocol_version: Option<String>,
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub certificate: Option<String>,
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub certificate_source: Option<String>,
}

impl Default for ViewerCertificate {
    fn default() -> Self {
        Self {
            cloud_front_default_certificate: Some(true),
            minimum_protocol_version: Some("TLSv1".to_string()),
            certificate_source: Some("cloudfront".to_string()),
            iam_certificate_id: None,
            acm_certificate_arn: None,
            ssl_support_method: None,
            certificate: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct Restrictions {
    pub geo_restriction: GeoRestriction,
}

impl Default for Restrictions {
    fn default() -> Self {
        Self {
            geo_restriction: GeoRestriction::default(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct GeoRestriction {
    pub restriction_type: String,
    pub quantity: i32,
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub items: Option<LocationList>,
}

impl Default for GeoRestriction {
    fn default() -> Self {
        Self {
            restriction_type: "none".to_string(),
            quantity: 0,
            items: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct LocationList {
    #[serde(default, rename = "Location")]
    pub location: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct TenantConfig {
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub parameter_definitions: Option<ParameterDefinitions>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct ParameterDefinitions {
    #[serde(default, rename = "ParameterDefinition")]
    pub parameter_definition: Vec<ParameterDefinition>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct ParameterDefinition {
    pub name: String,
    pub definition: ParameterDefinitionSchema,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct ParameterDefinitionSchema {
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub string_schema: Option<StringSchemaConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct StringSchemaConfig {
    pub required: bool,
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub comment: Option<String>,
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub default_value: Option<String>,
}

// ─── Tags ─────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct Tags {
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub items: Option<TagList>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct TagList {
    #[serde(default, rename = "Tag")]
    pub tag: Vec<Tag>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct Tag {
    pub key: String,
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub value: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct TagKeys {
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub items: Option<TagKeyList>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct TagKeyList {
    #[serde(default, rename = "Key")]
    pub key: Vec<String>,
}

// ─── Distribution + Wrappers ─────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct DistributionConfigWithTags {
    pub distribution_config: DistributionConfig,
    pub tags: Tags,
}

// ─── Invalidation ─────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct InvalidationBatch {
    pub paths: Paths,
    pub caller_reference: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct Paths {
    pub quantity: i32,
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub items: Option<PathList>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct PathList {
    #[serde(default, rename = "Path")]
    pub path: Vec<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A config whose acronym members PascalCase would mis-spell.
    fn acronym_config_xml() -> &'static str {
        r#"<DistributionConfig>
  <CallerReference>ref</CallerReference>
  <Origins><Quantity>0</Quantity></Origins>
  <DefaultCacheBehavior>
    <TargetOriginId>o1</TargetOriginId>
    <ViewerProtocolPolicy>allow-all</ViewerProtocolPolicy>
    <FunctionAssociations><Quantity>1</Quantity><Items><FunctionAssociation>
      <FunctionARN>arn:f</FunctionARN><EventType>viewer-request</EventType>
    </FunctionAssociation></Items></FunctionAssociations>
    <LambdaFunctionAssociations><Quantity>1</Quantity><Items><LambdaFunctionAssociation>
      <LambdaFunctionARN>arn:l</LambdaFunctionARN><EventType>origin-request</EventType>
    </LambdaFunctionAssociation></Items></LambdaFunctionAssociations>
    <MinTTL>1</MinTTL><DefaultTTL>2</DefaultTTL><MaxTTL>3</MaxTTL>
  </DefaultCacheBehavior>
  <CustomErrorResponses><Quantity>1</Quantity><Items><CustomErrorResponse>
    <ErrorCode>404</ErrorCode><ErrorCachingMinTTL>10</ErrorCachingMinTTL>
  </CustomErrorResponse></Items></CustomErrorResponses>
  <Comment></Comment>
  <Enabled>true</Enabled>
  <ViewerCertificate>
    <IAMCertificateId>iam-1</IAMCertificateId>
    <ACMCertificateArn>arn:c</ACMCertificateArn>
    <SSLSupportMethod>sni-only</SSLSupportMethod>
  </ViewerCertificate>
  <IsIPV6Enabled>true</IsIPV6Enabled>
</DistributionConfig>"#
    }

    #[test]
    fn acronym_members_parse_and_render_under_aws_spelling() {
        let config: DistributionConfig = quick_xml::de::from_str(acronym_config_xml()).unwrap();
        let dcb = &config.default_cache_behavior;
        assert_eq!(
            (dcb.min_ttl, dcb.default_ttl, dcb.max_ttl),
            (Some(1), Some(2), Some(3))
        );
        let fa = &dcb
            .function_associations
            .as_ref()
            .unwrap()
            .items
            .as_ref()
            .unwrap();
        assert_eq!(fa.function_association[0].function_arn, "arn:f");
        let la = &dcb.lambda_function_associations.as_ref().unwrap();
        assert_eq!(
            la.items.as_ref().unwrap().lambda_function_association[0].lambda_function_arn,
            "arn:l"
        );
        let rule = &config.custom_error_responses.as_ref().unwrap();
        assert_eq!(
            rule.items.as_ref().unwrap().custom_error_response[0].error_caching_min_ttl,
            Some(10)
        );
        let vc = config.viewer_certificate.as_ref().unwrap();
        assert_eq!(vc.iam_certificate_id.as_deref(), Some("iam-1"));
        assert_eq!(vc.acm_certificate_arn.as_deref(), Some("arn:c"));
        assert_eq!(vc.ssl_support_method.as_deref(), Some("sni-only"));
        assert_eq!(config.is_ipv6_enabled, Some(true));

        let xml = quick_xml::se::to_string_with_root("DistributionConfig", &config).unwrap();
        for tag in [
            "<MinTTL>1</MinTTL>",
            "<DefaultTTL>2</DefaultTTL>",
            "<MaxTTL>3</MaxTTL>",
            "<FunctionARN>arn:f</FunctionARN>",
            "<LambdaFunctionARN>arn:l</LambdaFunctionARN>",
            "<ErrorCachingMinTTL>10</ErrorCachingMinTTL>",
            "<IAMCertificateId>iam-1</IAMCertificateId>",
            "<ACMCertificateArn>arn:c</ACMCertificateArn>",
            "<SSLSupportMethod>sni-only</SSLSupportMethod>",
            "<IsIPV6Enabled>true</IsIPV6Enabled>",
        ] {
            assert!(xml.contains(tag), "missing {tag} in {xml}");
        }
    }

    #[test]
    fn old_pascal_case_spellings_are_not_accepted_on_the_wire() {
        // `MinTtl` is not a CloudFront member name; it must not be read as
        // `MinTTL`.
        let xml = acronym_config_xml()
            .replace("<MinTTL>1</MinTTL>", "<MinTtl>1</MinTtl>")
            .replace(
                "<ACMCertificateArn>arn:c</ACMCertificateArn>",
                "<AcmCertificateArn>arn:c</AcmCertificateArn>",
            );
        let config: DistributionConfig = quick_xml::de::from_str(&xml).unwrap();
        assert_eq!(config.default_cache_behavior.min_ttl, None);
        assert_eq!(config.viewer_certificate.unwrap().acm_certificate_arn, None);
    }

    #[test]
    fn mtls_connection_function_and_cache_tag_members_round_trip() {
        let xml = r#"<DistributionConfig>
  <CallerReference>ref</CallerReference>
  <Origins><Quantity>1</Quantity><Items><Origin>
    <Id>o1</Id><DomainName>o.example.com</DomainName>
    <CustomOriginConfig>
      <HTTPPort>80</HTTPPort><HTTPSPort>443</HTTPSPort>
      <OriginProtocolPolicy>https-only</OriginProtocolPolicy>
      <OriginMtlsConfig><ClientCertificateArn>arn:cert</ClientCertificateArn></OriginMtlsConfig>
    </CustomOriginConfig>
  </Origin></Items></Origins>
  <DefaultCacheBehavior>
    <TargetOriginId>o1</TargetOriginId>
    <ViewerProtocolPolicy>allow-all</ViewerProtocolPolicy>
  </DefaultCacheBehavior>
  <Comment></Comment>
  <Enabled>true</Enabled>
  <ViewerMtlsConfig>
    <Mode>required</Mode>
    <TrustStoreConfig>
      <TrustStoreId>ts-1</TrustStoreId>
      <AdvertiseTrustStoreCaNames>true</AdvertiseTrustStoreCaNames>
      <IgnoreCertificateExpiry>false</IgnoreCertificateExpiry>
    </TrustStoreConfig>
  </ViewerMtlsConfig>
  <ConnectionFunctionAssociation><Id>cf-1</Id></ConnectionFunctionAssociation>
  <CacheTagConfig><HeaderName>Cache-Tag</HeaderName></CacheTagConfig>
</DistributionConfig>"#;
        let config: DistributionConfig = quick_xml::de::from_str(xml).unwrap();
        let mtls = config.viewer_mtls_config.as_ref().unwrap();
        assert_eq!(mtls.mode.as_deref(), Some("required"));
        let ts = mtls.trust_store_config.as_ref().unwrap();
        assert_eq!(ts.trust_store_id, "ts-1");
        assert_eq!(ts.advertise_trust_store_ca_names, Some(true));
        assert_eq!(ts.ignore_certificate_expiry, Some(false));
        assert_eq!(
            config.connection_function_association.as_ref().unwrap().id,
            "cf-1"
        );
        assert_eq!(
            config.cache_tag_config.as_ref().unwrap().header_name,
            "Cache-Tag"
        );
        let origin = &config.origins.items.as_ref().unwrap().origin[0];
        assert_eq!(
            origin
                .custom_origin_config
                .as_ref()
                .unwrap()
                .origin_mtls_config
                .as_ref()
                .unwrap()
                .client_certificate_arn,
            "arn:cert"
        );

        let rendered = quick_xml::se::to_string_with_root("DistributionConfig", &config).unwrap();
        for tag in [
            "<Mode>required</Mode>",
            "<TrustStoreId>ts-1</TrustStoreId>",
            "<AdvertiseTrustStoreCaNames>true</AdvertiseTrustStoreCaNames>",
            "<IgnoreCertificateExpiry>false</IgnoreCertificateExpiry>",
            "<ConnectionFunctionAssociation><Id>cf-1</Id></ConnectionFunctionAssociation>",
            "<CacheTagConfig><HeaderName>Cache-Tag</HeaderName></CacheTagConfig>",
            "<OriginMtlsConfig><ClientCertificateArn>arn:cert</ClientCertificateArn></OriginMtlsConfig>",
        ] {
            assert!(rendered.contains(tag), "missing {tag} in {rendered}");
        }
    }
}