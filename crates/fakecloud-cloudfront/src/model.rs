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

/// CloudFront leaves `Items` out of an empty list, however the list got empty.
fn drop_empty<T>(items: &mut Option<T>, is_empty: impl FnOnce(&T) -> bool) {
    if items.as_ref().is_some_and(is_empty) {
        *items = None;
    }
}

/// Fill the members CloudFront defaults on a cache behavior.
/// `DefaultCacheBehavior` and `CacheBehavior` share these fields but no type.
macro_rules! fill_behavior_defaults {
    ($behavior:expr, $multi_tenant:expr) => {{
        let b = $behavior;
        let head_get = || MethodList {
            method: vec!["HEAD".to_string(), "GET".to_string()],
        };
        drop_empty(
            &mut b
                .trusted_key_groups
                .get_or_insert_with(Default::default)
                .items,
            |i| i.key_group.is_empty(),
        );
        b.allowed_methods
            .get_or_insert_with(|| AllowedMethods {
                quantity: 2,
                items: head_get(),
                cached_methods: None,
            })
            .cached_methods
            .get_or_insert_with(|| CachedMethods {
                quantity: 2,
                items: head_get(),
            });
        b.compress.get_or_insert(false);
        drop_empty(
            &mut b
                .lambda_function_associations
                .get_or_insert_with(Default::default)
                .items,
            |i| i.lambda_function_association.is_empty(),
        );
        drop_empty(
            &mut b
                .function_associations
                .get_or_insert_with(Default::default)
                .items,
            |i| i.function_association.is_empty(),
        );
        b.field_level_encryption_id.get_or_insert_with(String::new);
        b.grpc_config.get_or_insert_with(Default::default);
        if !$multi_tenant {
            drop_empty(
                &mut b.trusted_signers.get_or_insert_with(Default::default).items,
                |i| i.aws_account_number.is_empty(),
            );
            b.smooth_streaming.get_or_insert(false);
        }
        if let Some(forwarded) = b.forwarded_values.as_mut() {
            drop_empty(
                &mut forwarded.headers.get_or_insert_with(Default::default).items,
                |i| i.name.is_empty(),
            );
            drop_empty(
                &mut forwarded
                    .query_string_cache_keys
                    .get_or_insert_with(Default::default)
                    .items,
                |i| i.name.is_empty(),
            );
            if let Some(names) = forwarded.cookies.whitelisted_names.as_mut() {
                drop_empty(&mut names.items, |i| i.name.is_empty());
            }
            // The legacy cache settings, standard-only: a behavior on a cache
            // policy has no TTLs.
            // A day and a year, unless the bounds supplied exceed them: AWS
            // documents that the default `DefaultTTL` then follows `MinTTL`,
            // and the default `MaxTTL` follows `DefaultTTL`.
            if !$multi_tenant {
                let min = *b.min_ttl.get_or_insert(0);
                let default = *b.default_ttl.get_or_insert(min.max(86_400));
                b.max_ttl.get_or_insert(default.max(31_536_000));
            }
        }
    }};
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
    /// Make the config canonical: fill in what CloudFront fills in for members
    /// a request left out, so `GetDistribution` / `GetDistributionConfig` carry
    /// the same envelopes AWS returns. What a request supplied is kept, except
    /// the viewer certificate members CloudFront itself derives. This is
    /// idempotent, so a config read back and PUT unchanged compares equal to the
    /// stored one.
    ///
    /// Applied wherever a config enters state (the API, CloudFormation, snapshot
    /// load); the read paths rely on it.
    ///
    /// Members AWS documents as standard-distribution only stay absent on a
    /// multi-tenant (`tenant-only`) distribution.
    pub fn ensure_aws_defaults(&mut self) {
        let multi_tenant = self.connection_mode.as_deref() == Some("tenant-only");

        self.default_root_object.get_or_insert_with(String::new);
        drop_empty(
            &mut self
                .origin_groups
                .get_or_insert_with(Default::default)
                .items,
            |i| i.origin_group.is_empty(),
        );
        drop_empty(
            &mut self
                .cache_behaviors
                .get_or_insert_with(Default::default)
                .items,
            |i| i.cache_behavior.is_empty(),
        );
        drop_empty(
            &mut self
                .custom_error_responses
                .get_or_insert_with(Default::default)
                .items,
            |i| i.custom_error_response.is_empty(),
        );
        drop_empty(
            &mut self
                .restrictions
                .get_or_insert_with(Default::default)
                .geo_restriction
                .items,
            |i| i.location.is_empty(),
        );
        self.web_acl_id.get_or_insert_with(String::new);
        self.http_version.get_or_insert_with(|| "http2".to_string());
        if !multi_tenant {
            drop_empty(
                &mut self.aliases.get_or_insert_with(Default::default).items,
                |i| i.cname.is_empty(),
            );
            self.price_class
                .get_or_insert_with(|| "PriceClass_All".to_string());
            self.is_ipv6_enabled.get_or_insert(true);
            self.staging.get_or_insert(false);
            // No documented default to report: empty means no policy.
            drop_empty(&mut self.continuous_deployment_policy_id, String::is_empty);
        }
        self.logging.get_or_insert_with(Default::default);
        self.viewer_certificate
            .get_or_insert_with(Default::default)
            .ensure_aws_defaults(multi_tenant);

        for origin in self.origins.items.iter_mut().flat_map(|i| &mut i.origin) {
            origin.ensure_aws_defaults();
        }
        fill_behavior_defaults!(&mut self.default_cache_behavior, multi_tenant);
        for behavior in self
            .cache_behaviors
            .iter_mut()
            .filter_map(|b| b.items.as_mut())
            .flat_map(|i| &mut i.cache_behavior)
        {
            fill_behavior_defaults!(behavior, multi_tenant);
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

impl Origin {
    fn ensure_aws_defaults(&mut self) {
        self.origin_path.get_or_insert_with(String::new);
        drop_empty(
            &mut self
                .custom_headers
                .get_or_insert_with(Default::default)
                .items,
            |i| i.origin_custom_header.is_empty(),
        );
        self.connection_attempts.get_or_insert(3);
        self.connection_timeout.get_or_insert(10);
        self.origin_shield.get_or_insert_with(Default::default);
        self.origin_access_control_id
            .get_or_insert_with(String::new);
        if let Some(custom) = self.custom_origin_config.as_mut() {
            custom.origin_read_timeout.get_or_insert(30);
            custom.origin_keepalive_timeout.get_or_insert(5);
        }
        if let Some(s3) = self.s3_origin_config.as_mut() {
            s3.origin_read_timeout.get_or_insert(30);
        }
        if let Some(vpc) = self.vpc_origin_config.as_mut() {
            vpc.origin_read_timeout.get_or_insert(30);
            vpc.origin_keepalive_timeout.get_or_insert(5);
        }
    }
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
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub include_cookies: bool,
    #[serde(default)]
    pub bucket: String,
    #[serde(default)]
    pub prefix: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct ViewerCertificate {
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub cloud_front_default_certificate: Option<bool>,
    // AWS spells these `IAMCertificateId` / `ACMCertificateArn` /
    // `SSLSupportMethod`, not the PascalCase `IamCertificateId` etc.
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

impl ViewerCertificate {
    /// Settle which certificate is in use and derive what follows from it, so a
    /// config edited after being read back carries no stale mirror.
    ///
    /// `multi_tenant` selects the security policy a custom certificate gets when
    /// none was given; a standard distribution has no documented one, so it
    /// stays absent there.
    fn ensure_aws_defaults(&mut self, multi_tenant: bool) {
        // An empty element names no certificate.
        drop_empty(&mut self.acm_certificate_arn, String::is_empty);
        drop_empty(&mut self.iam_certificate_id, String::is_empty);
        drop_empty(&mut self.certificate, String::is_empty);

        // A request naming the certificate only through the deprecated pair.
        // An explicit `CloudFrontDefaultCertificate` outranks that pair, which
        // may just be left over from an earlier read.
        if self.acm_certificate_arn.is_none()
            && self.iam_certificate_id.is_none()
            && self.cloud_front_default_certificate != Some(true)
        {
            match (self.certificate_source.as_deref(), self.certificate.take()) {
                (Some("acm"), Some(certificate)) => self.acm_certificate_arn = Some(certificate),
                (Some("iam"), Some(certificate)) => self.iam_certificate_id = Some(certificate),
                (None, Some(certificate)) if certificate.starts_with("arn:") => {
                    self.acm_certificate_arn = Some(certificate);
                }
                (None, Some(certificate)) => self.iam_certificate_id = Some(certificate),
                _ => {}
            }
        }

        let custom = match (&self.acm_certificate_arn, &self.iam_certificate_id) {
            (Some(arn), _) => Some(("acm", arn.clone())),
            (None, Some(id)) => Some(("iam", id.clone())),
            (None, None) => None,
        };
        if let Some((source, certificate)) = custom {
            self.cloud_front_default_certificate = Some(false);
            self.certificate = Some(certificate);
            self.certificate_source = Some(source.to_string());
            if multi_tenant {
                self.minimum_protocol_version
                    .get_or_insert_with(|| "TLSv1.2_2021".to_string());
            }
        } else if self.cloud_front_default_certificate == Some(false) {
            self.certificate = None;
            self.certificate_source = None;
        } else {
            self.cloud_front_default_certificate = Some(true);
            self.certificate = None;
            self.certificate_source = Some("cloudfront".to_string());
            // "CloudFront automatically sets the security policy to TLSv1
            // regardless of the value that you set here."
            self.minimum_protocol_version = Some("TLSv1".to_string());
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct Restrictions {
    pub geo_restriction: GeoRestriction,
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

    /// Close to what the AWS CLI's `--origin-domain-name` template sends: everything
    /// optional is left for CloudFront to fill in.
    fn sparse_config_xml() -> String {
        r#"<DistributionConfig>
  <CallerReference>ref</CallerReference>
  <Origins><Quantity>1</Quantity><Items><Origin>
    <Id>o1</Id><DomainName>o.example.com</DomainName>
    <CustomOriginConfig>
      <HTTPPort>80</HTTPPort><HTTPSPort>443</HTTPSPort>
      <OriginProtocolPolicy>http-only</OriginProtocolPolicy>
    </CustomOriginConfig>
  </Origin></Items></Origins>
  <DefaultCacheBehavior>
    <TargetOriginId>o1</TargetOriginId>
    <ViewerProtocolPolicy>allow-all</ViewerProtocolPolicy>
    <ForwardedValues>
      <QueryString>false</QueryString><Cookies><Forward>none</Forward></Cookies>
    </ForwardedValues>
  </DefaultCacheBehavior>
  <Comment></Comment>
  <Enabled>true</Enabled>
</DistributionConfig>"#
            .to_string()
    }

    fn parse(xml: &str) -> DistributionConfig {
        quick_xml::de::from_str(xml).unwrap()
    }

    fn render(config: &DistributionConfig) -> String {
        quick_xml::se::to_string_with_root("DistributionConfig", config).unwrap()
    }

    #[test]
    fn a_sparse_config_gets_the_values_cloudfront_fills_in() {
        let mut config = parse(&sparse_config_xml());
        config.ensure_aws_defaults();

        assert_eq!(config.aliases.as_ref().unwrap().quantity, 0);
        assert_eq!(config.default_root_object.as_deref(), Some(""));
        assert_eq!(config.origin_groups.as_ref().unwrap().quantity, 0);
        assert_eq!(config.cache_behaviors.as_ref().unwrap().quantity, 0);
        assert_eq!(config.custom_error_responses.as_ref().unwrap().quantity, 0);
        assert_eq!(config.price_class.as_deref(), Some("PriceClass_All"));
        assert_eq!(config.http_version.as_deref(), Some("http2"));
        assert_eq!(config.is_ipv6_enabled, Some(true));
        assert_eq!(config.web_acl_id.as_deref(), Some(""));

        let logging = config.logging.as_ref().unwrap();
        assert!(!logging.enabled && !logging.include_cookies);
        assert_eq!((logging.bucket.as_str(), logging.prefix.as_str()), ("", ""));

        let vc = config.viewer_certificate.as_ref().unwrap();
        assert_eq!(vc.cloud_front_default_certificate, Some(true));
        assert_eq!(vc.minimum_protocol_version.as_deref(), Some("TLSv1"));
        assert_eq!(vc.certificate_source.as_deref(), Some("cloudfront"));
        assert_eq!(vc.acm_certificate_arn, None);

        let geo = &config.restrictions.as_ref().unwrap().geo_restriction;
        assert_eq!(geo.restriction_type, "none");
        assert_eq!(geo.quantity, 0);
        assert!(geo.items.is_none());

        let origin = &config.origins.items.as_ref().unwrap().origin[0];
        assert_eq!(origin.origin_path.as_deref(), Some(""));
        assert_eq!(origin.custom_headers.as_ref().unwrap().quantity, 0);
        assert_eq!(origin.connection_attempts, Some(3));
        assert_eq!(origin.connection_timeout, Some(10));
        assert!(!origin.origin_shield.as_ref().unwrap().enabled);
        assert_eq!(origin.origin_access_control_id.as_deref(), Some(""));
        let custom = origin.custom_origin_config.as_ref().unwrap();
        assert_eq!(custom.origin_read_timeout, Some(30));
        assert_eq!(custom.origin_keepalive_timeout, Some(5));

        let dcb = &config.default_cache_behavior;
        let signers = dcb.trusted_signers.as_ref().unwrap();
        assert_eq!((signers.enabled, signers.quantity), (false, 0));
        let groups = dcb.trusted_key_groups.as_ref().unwrap();
        assert_eq!((groups.enabled, groups.quantity), (false, 0));
        let allowed = dcb.allowed_methods.as_ref().unwrap();
        assert_eq!(allowed.quantity, 2);
        assert_eq!(allowed.items.method, ["HEAD", "GET"]);
        let cached = allowed.cached_methods.as_ref().unwrap();
        assert_eq!(cached.quantity, 2);
        assert_eq!(cached.items.method, ["HEAD", "GET"]);
        assert_eq!(dcb.smooth_streaming, Some(false));
        assert_eq!(dcb.compress, Some(false));
        assert_eq!(
            dcb.lambda_function_associations.as_ref().unwrap().quantity,
            0
        );
        assert_eq!(dcb.function_associations.as_ref().unwrap().quantity, 0);
        assert_eq!(dcb.field_level_encryption_id.as_deref(), Some(""));
        assert!(!dcb.grpc_config.as_ref().unwrap().enabled);
        let forwarded = dcb.forwarded_values.as_ref().unwrap();
        assert_eq!(forwarded.headers.as_ref().unwrap().quantity, 0);
        assert_eq!(
            forwarded.query_string_cache_keys.as_ref().unwrap().quantity,
            0
        );
        assert_eq!(
            (dcb.min_ttl, dcb.default_ttl, dcb.max_ttl),
            (Some(0), Some(86_400), Some(31_536_000))
        );
    }

    #[test]
    fn defaults_are_stable_across_a_serialize_parse_cycle() {
        let mut config = parse(&sparse_config_xml());
        config.ensure_aws_defaults();
        let first = render(&config);

        let mut reread = parse(&first);
        reread.ensure_aws_defaults();
        assert_eq!(render(&reread), first);
    }

    #[test]
    fn values_the_caller_supplied_are_never_replaced() {
        let xml = sparse_config_xml().replace(
            "<Comment></Comment>",
            r#"<Aliases><Quantity>1</Quantity><Items><CNAME>a.example.com</CNAME></Items></Aliases>
  <Comment></Comment>
  <PriceClass>PriceClass_100</PriceClass>
  <HttpVersion>http3</HttpVersion>
  <IsIPV6Enabled>false</IsIPV6Enabled>
  <Logging><Enabled>true</Enabled><IncludeCookies>true</IncludeCookies><Bucket>b</Bucket><Prefix>p</Prefix></Logging>
  <Restrictions><GeoRestriction><RestrictionType>whitelist</RestrictionType><Quantity>1</Quantity><Items><Location>BR</Location></Items></GeoRestriction></Restrictions>
  <WebACLId>acl-1</WebACLId>"#,
        );
        let mut config = parse(&xml);
        config.default_cache_behavior.compress = Some(true);
        config.default_cache_behavior.max_ttl = Some(5);
        config.ensure_aws_defaults();

        assert_eq!(config.aliases.as_ref().unwrap().quantity, 1);
        assert_eq!(config.price_class.as_deref(), Some("PriceClass_100"));
        assert_eq!(config.http_version.as_deref(), Some("http3"));
        assert_eq!(config.is_ipv6_enabled, Some(false));
        let logging = config.logging.as_ref().unwrap();
        assert_eq!(
            (logging.bucket.as_str(), logging.prefix.as_str()),
            ("b", "p")
        );
        let geo = &config.restrictions.as_ref().unwrap().geo_restriction;
        assert_eq!(geo.restriction_type, "whitelist");
        assert_eq!(geo.items.as_ref().unwrap().location, ["BR"]);
        assert_eq!(config.web_acl_id.as_deref(), Some("acl-1"));
        assert_eq!(config.default_cache_behavior.compress, Some(true));
        assert_eq!(config.default_cache_behavior.max_ttl, Some(5));
    }

    #[test]
    fn logging_values_the_caller_supplied_survive_when_enabled_is_omitted() {
        let xml = sparse_config_xml().replace(
            "<Comment></Comment>",
            "<Comment></Comment><Logging><Bucket>b</Bucket><Prefix>p</Prefix></Logging>",
        );
        let mut config = parse(&xml);
        config.ensure_aws_defaults();
        let logging = config.logging.unwrap();
        assert!(!logging.enabled);
        assert_eq!(
            (logging.bucket.as_str(), logging.prefix.as_str()),
            ("b", "p")
        );
    }

    #[test]
    fn a_partial_logging_block_parses() {
        let xml = sparse_config_xml().replace(
            "<Comment></Comment>",
            "<Comment></Comment><Logging><Enabled>false</Enabled></Logging>",
        );
        assert!(!parse(&xml).logging.unwrap().enabled);
    }

    #[test]
    fn the_default_certificate_always_gets_the_tlsv1_policy() {
        for given in [None, Some("TLSv1.2_2021")] {
            let mut vc = ViewerCertificate {
                cloud_front_default_certificate: Some(true),
                minimum_protocol_version: given.map(Into::into),
                ..Default::default()
            };
            vc.ensure_aws_defaults(false);
            assert_eq!(vc.minimum_protocol_version.as_deref(), Some("TLSv1"));
            assert_eq!(vc.certificate_source.as_deref(), Some("cloudfront"));
        }
    }

    #[test]
    fn certificate_mirrors_follow_the_certificate_in_use() {
        let arn_a = "arn:aws:acm:us-east-1:1:certificate/a";
        let arn_b = "arn:aws:acm:us-east-1:1:certificate/b";

        // Default -> ACM, editing what was read back.
        let mut vc = ViewerCertificate::default();
        vc.ensure_aws_defaults(false);
        vc.acm_certificate_arn = Some(arn_a.into());
        vc.minimum_protocol_version = Some("TLSv1.2_2021".into());
        vc.ensure_aws_defaults(false);
        assert_eq!(vc.cloud_front_default_certificate, Some(false));
        assert_eq!(vc.certificate.as_deref(), Some(arn_a));
        assert_eq!(vc.certificate_source.as_deref(), Some("acm"));
        assert_eq!(vc.minimum_protocol_version.as_deref(), Some("TLSv1.2_2021"));

        // ACM A -> ACM B.
        vc.acm_certificate_arn = Some(arn_b.into());
        vc.ensure_aws_defaults(false);
        assert_eq!(vc.certificate.as_deref(), Some(arn_b));

        // ACM -> default.
        vc.acm_certificate_arn = None;
        vc.cloud_front_default_certificate = Some(true);
        vc.ensure_aws_defaults(false);
        assert_eq!(vc.acm_certificate_arn, None);
        assert_eq!(vc.certificate, None);
        assert_eq!(vc.certificate_source.as_deref(), Some("cloudfront"));
        assert_eq!(vc.minimum_protocol_version.as_deref(), Some("TLSv1"));
    }

    #[test]
    fn the_deprecated_certificate_pair_maps_onto_the_current_members() {
        let cases = [
            (Some("acm"), Some("arn:c"), Some("arn:c"), None, Some(false)),
            (Some("iam"), Some("iam-1"), None, Some("iam-1"), Some(false)),
            (None, Some("arn:c"), Some("arn:c"), None, Some(false)),
            (None, Some("iam-1"), None, Some("iam-1"), Some(false)),
            (Some("cloudfront"), None, None, None, Some(true)),
        ];
        for (source, certificate, acm, iam, default_certificate) in cases {
            let mut vc = ViewerCertificate {
                certificate_source: source.map(Into::into),
                certificate: certificate.map(Into::into),
                ..Default::default()
            };
            vc.ensure_aws_defaults(false);
            assert_eq!(vc.acm_certificate_arn.as_deref(), acm, "{source:?}");
            assert_eq!(vc.iam_certificate_id.as_deref(), iam, "{source:?}");
            assert_eq!(vc.cloud_front_default_certificate, default_certificate);
        }
    }

    #[test]
    fn empty_certificate_members_name_no_certificate() {
        let mut vc = ViewerCertificate {
            acm_certificate_arn: Some(String::new()),
            iam_certificate_id: Some(String::new()),
            certificate: Some(String::new()),
            ..Default::default()
        };
        vc.ensure_aws_defaults(false);
        assert_eq!(vc.acm_certificate_arn, None);
        assert_eq!(vc.iam_certificate_id, None);
        assert_eq!(vc.cloud_front_default_certificate, Some(true));
        assert_eq!(vc.certificate_source.as_deref(), Some("cloudfront"));
    }

    #[test]
    fn staging_and_an_empty_policy_id_have_one_canonical_form() {
        let mut omitted = parse(&sparse_config_xml());
        omitted.ensure_aws_defaults();
        assert_eq!(omitted.staging, Some(false));

        let mut explicit = parse(&sparse_config_xml());
        explicit.staging = Some(false);
        explicit.continuous_deployment_policy_id = Some(String::new());
        explicit.ensure_aws_defaults();
        assert_eq!(render(&explicit), render(&omitted));

        let mut staging = parse(&sparse_config_xml());
        staging.staging = Some(true);
        staging.ensure_aws_defaults();
        assert_eq!(staging.staging, Some(true));
    }

    #[test]
    fn an_empty_items_element_matches_an_omitted_one() {
        let mut omitted = parse(&sparse_config_xml());
        omitted.ensure_aws_defaults();

        let xml = sparse_config_xml().replace(
            "<Comment></Comment>",
            r#"<Aliases><Quantity>0</Quantity><Items/></Aliases>
  <OriginGroups><Quantity>0</Quantity><Items/></OriginGroups>
  <CacheBehaviors><Quantity>0</Quantity><Items/></CacheBehaviors>
  <CustomErrorResponses><Quantity>0</Quantity><Items/></CustomErrorResponses>
  <Comment></Comment>
  <Restrictions><GeoRestriction><RestrictionType>none</RestrictionType><Quantity>0</Quantity><Items/></GeoRestriction></Restrictions>"#,
        );
        let mut explicit = parse(&xml);
        let dcb = &mut explicit.default_cache_behavior;
        dcb.function_associations = Some(FunctionAssociations {
            quantity: 0,
            items: Some(FunctionAssociationItems::default()),
        });
        dcb.trusted_signers = Some(TrustedSigners {
            items: Some(AwsAccountNumberList::default()),
            ..Default::default()
        });
        explicit.ensure_aws_defaults();
        assert_eq!(render(&explicit), render(&omitted));
    }

    #[test]
    fn s3_and_vpc_origins_get_their_documented_timeouts() {
        let mut origin = Origin {
            s3_origin_config: Some(S3OriginConfig::default()),
            vpc_origin_config: Some(VpcOriginConfig::default()),
            ..Default::default()
        };
        origin.ensure_aws_defaults();
        let s3 = origin.s3_origin_config.as_ref().unwrap();
        assert_eq!(s3.origin_read_timeout, Some(30));
        let vpc = origin.vpc_origin_config.as_ref().unwrap();
        assert_eq!(vpc.origin_read_timeout, Some(30));
        assert_eq!(vpc.origin_keepalive_timeout, Some(5));

        let mut origin = Origin {
            vpc_origin_config: Some(VpcOriginConfig {
                origin_read_timeout: Some(60),
                ..Default::default()
            }),
            ..Default::default()
        };
        origin.ensure_aws_defaults();
        assert_eq!(
            origin.vpc_origin_config.unwrap().origin_read_timeout,
            Some(60)
        );
    }

    #[test]
    fn an_acm_certificate_is_never_turned_into_the_default_one() {
        let arn = "arn:aws:acm:us-east-1:123456789012:certificate/abc";
        let mut vc = ViewerCertificate {
            acm_certificate_arn: Some(arn.into()),
            ssl_support_method: Some("sni-only".into()),
            minimum_protocol_version: Some("TLSv1.2_2021".into()),
            ..Default::default()
        };
        vc.ensure_aws_defaults(false);
        assert_eq!(vc.cloud_front_default_certificate, Some(false));
        assert_eq!(vc.acm_certificate_arn.as_deref(), Some(arn));
        assert_eq!(vc.certificate.as_deref(), Some(arn));
        assert_eq!(vc.certificate_source.as_deref(), Some("acm"));
        assert_eq!(vc.ssl_support_method.as_deref(), Some("sni-only"));
        assert_eq!(vc.minimum_protocol_version.as_deref(), Some("TLSv1.2_2021"));
    }

    #[test]
    fn an_iam_certificate_is_reported_as_the_iam_source() {
        let mut vc = ViewerCertificate {
            iam_certificate_id: Some("iam-1".into()),
            ..Default::default()
        };
        vc.ensure_aws_defaults(false);
        assert_eq!(vc.cloud_front_default_certificate, Some(false));
        assert_eq!(vc.certificate.as_deref(), Some("iam-1"));
        assert_eq!(vc.certificate_source.as_deref(), Some("iam"));
        // No documented security policy for a standard distribution.
        assert_eq!(vc.minimum_protocol_version, None);
    }

    #[test]
    fn an_explicit_opt_out_of_the_default_certificate_is_left_alone() {
        let mut vc = ViewerCertificate {
            cloud_front_default_certificate: Some(false),
            ..Default::default()
        };
        vc.ensure_aws_defaults(false);
        assert_eq!(vc.cloud_front_default_certificate, Some(false));
        assert_eq!(vc.certificate_source, None);
    }

    #[test]
    fn a_multi_tenant_distribution_keeps_standard_only_members_absent() {
        let xml = sparse_config_xml().replace(
            "<Enabled>true</Enabled>",
            "<Enabled>true</Enabled><ConnectionMode>tenant-only</ConnectionMode>",
        );
        let mut config = parse(&xml);
        config.viewer_certificate = Some(ViewerCertificate {
            acm_certificate_arn: Some("arn:aws:acm:us-east-1:1:certificate/abc".into()),
            ..Default::default()
        });
        config.ensure_aws_defaults();

        assert!(config.aliases.is_none());
        assert_eq!(config.price_class, None);
        assert_eq!(config.is_ipv6_enabled, None);
        assert_eq!(config.staging, None);
        let dcb = &config.default_cache_behavior;
        assert!(dcb.trusted_signers.is_none());
        assert_eq!(dcb.smooth_streaming, None);
        assert_eq!(
            (dcb.min_ttl, dcb.default_ttl, dcb.max_ttl),
            (None, None, None)
        );
        assert!(dcb.trusted_key_groups.is_some());
        let vc = config.viewer_certificate.as_ref().unwrap();
        assert_eq!(vc.minimum_protocol_version.as_deref(), Some("TLSv1.2_2021"));
    }

    #[test]
    fn omitted_ttls_follow_the_bounds_the_request_supplied() {
        const DAY: i64 = 86_400;
        const YEAR: i64 = 31_536_000;
        const LONG: i64 = 40_000_000;
        for (supplied, expected) in [
            ((None, None, None), (0, DAY, YEAR)),
            // A MinTTL above a day raises the default DefaultTTL to it.
            ((Some(172_800), None, None), (172_800, 172_800, YEAR)),
            // A MinTTL or DefaultTTL above a year raises the default MaxTTL.
            ((None, Some(LONG), None), (0, LONG, LONG)),
            ((Some(LONG), None, None), (LONG, LONG, LONG)),
            // Below those thresholds the fixed defaults stand.
            ((Some(10), None, None), (10, DAY, YEAR)),
            ((None, Some(10), None), (0, 10, YEAR)),
            // What a request supplied is kept.
            ((Some(1), Some(2), Some(3)), (1, 2, 3)),
            ((None, None, Some(600)), (0, DAY, 600)),
        ] {
            let mut config = parse(&sparse_config_xml());
            let dcb = &mut config.default_cache_behavior;
            (dcb.min_ttl, dcb.default_ttl, dcb.max_ttl) = supplied;
            config.cache_behaviors = Some(CacheBehaviors {
                quantity: 1,
                items: Some(CacheBehaviorItems {
                    cache_behavior: vec![CacheBehavior {
                        path_pattern: "/x".into(),
                        target_origin_id: "o1".into(),
                        viewer_protocol_policy: "allow-all".into(),
                        forwarded_values: dcb.forwarded_values.clone(),
                        min_ttl: supplied.0,
                        default_ttl: supplied.1,
                        max_ttl: supplied.2,
                        ..Default::default()
                    }],
                }),
            });
            config.ensure_aws_defaults();
            let expected = (Some(expected.0), Some(expected.1), Some(expected.2));
            let dcb = &config.default_cache_behavior;
            assert_eq!((dcb.min_ttl, dcb.default_ttl, dcb.max_ttl), expected);
            let b = &config
                .cache_behaviors
                .as_ref()
                .unwrap()
                .items
                .as_ref()
                .unwrap()
                .cache_behavior[0];
            assert_eq!((b.min_ttl, b.default_ttl, b.max_ttl), expected);

            // Read back and sent again, the derived values do not move.
            let mut again = config.clone();
            again.ensure_aws_defaults();
            assert_eq!(render(&again), render(&config));
        }
    }

    #[test]
    fn a_behavior_on_a_cache_policy_gets_no_legacy_ttls() {
        let mut config = parse(&sparse_config_xml());
        config.default_cache_behavior.forwarded_values = None;
        config.default_cache_behavior.cache_policy_id = Some("cp".into());
        config.ensure_aws_defaults();
        let dcb = &config.default_cache_behavior;
        assert_eq!(
            (dcb.min_ttl, dcb.default_ttl, dcb.max_ttl),
            (None, None, None)
        );
        assert!(dcb.forwarded_values.is_none());
    }

    #[test]
    fn additional_cache_behaviors_get_the_same_defaults() {
        let mut config = parse(&sparse_config_xml());
        config.cache_behaviors = Some(CacheBehaviors {
            quantity: 1,
            items: Some(CacheBehaviorItems {
                cache_behavior: vec![CacheBehavior {
                    path_pattern: "/x".into(),
                    target_origin_id: "o1".into(),
                    viewer_protocol_policy: "allow-all".into(),
                    ..Default::default()
                }],
            }),
        });
        config.ensure_aws_defaults();
        let b = &config
            .cache_behaviors
            .unwrap()
            .items
            .unwrap()
            .cache_behavior[0];
        assert!(b.trusted_signers.is_some() && b.trusted_key_groups.is_some());
        assert!(b.function_associations.is_some() && b.allowed_methods.is_some());
    }

    #[test]
    fn an_allowed_methods_block_without_cached_methods_caches_get_and_head() {
        let mut config = parse(&sparse_config_xml());
        config.default_cache_behavior.allowed_methods = Some(AllowedMethods {
            quantity: 3,
            items: MethodList {
                method: vec!["GET".into(), "HEAD".into(), "OPTIONS".into()],
            },
            cached_methods: None,
        });
        config.ensure_aws_defaults();
        let allowed = config.default_cache_behavior.allowed_methods.unwrap();
        assert_eq!(allowed.quantity, 3);
        assert_eq!(
            allowed.cached_methods.unwrap().items.method,
            ["HEAD", "GET"]
        );
    }

    #[test]
    fn struct_defaults_stay_neutral_for_partial_construction() {
        // `Default` backs `..Default::default()` and a parsed struct's absent
        // members: a bare certificate must not claim the CloudFront default.
        assert_eq!(
            ViewerCertificate::default().cloud_front_default_certificate,
            None
        );
        assert_eq!(GeoRestriction::default().restriction_type, "none");
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
