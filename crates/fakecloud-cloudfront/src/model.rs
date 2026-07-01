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
    #[serde(default, skip_serializing_if = "skip_if_none")]
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
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub min_ttl: Option<i64>,
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub default_ttl: Option<i64>,
    #[serde(default, skip_serializing_if = "skip_if_none")]
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
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub min_ttl: Option<i64>,
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub default_ttl: Option<i64>,
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub max_ttl: Option<i64>,
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
    #[serde(default, skip_serializing_if = "skip_if_none")]
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

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct ViewerCertificate {
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub cloud_front_default_certificate: Option<bool>,
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub iam_certificate_id: Option<String>,
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub acm_certificate_arn: Option<String>,
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub ssl_support_method: Option<String>,
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub minimum_protocol_version: Option<String>,
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub certificate: Option<String>,
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub certificate_source: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct Restrictions {
    pub geo_restriction: GeoRestriction,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct GeoRestriction {
    pub restriction_type: String,
    pub quantity: i32,
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub items: Option<LocationList>,
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
