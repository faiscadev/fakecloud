//! `DistributionConfig` validation shared by `CreateDistribution`,
//! `UpdateDistribution` and the CloudFormation provisioner, so a config is
//! accepted or rejected the same way whichever door it comes in through.

use std::collections::HashSet;

use fakecloud_core::service::AwsServiceError;
use http::StatusCode;

use crate::model::{
    has_cache_policy, AllowedMethods, DistributionConfig, FunctionAssociations,
    LambdaFunctionAssociations,
};
use crate::service::{aws_error, invalid_argument};

/// The `EventType` values CloudFront accepts for Lambda@Edge and CloudFront
/// Functions associations.
const EVENT_TYPES: &[&str] = &[
    "viewer-request",
    "viewer-response",
    "origin-request",
    "origin-response",
];

/// The method sets CloudFront accepts for `AllowedMethods`.
const ALLOWED_METHOD_SETS: &[&[&str]] = &[
    &["GET", "HEAD"],
    &["GET", "HEAD", "OPTIONS"],
    &["DELETE", "GET", "HEAD", "OPTIONS", "PATCH", "POST", "PUT"],
];

/// The method sets CloudFront accepts for `CachedMethods`.
const CACHED_METHOD_SETS: &[&[&str]] = &[&["GET", "HEAD"], &["GET", "HEAD", "OPTIONS"]];

/// Which request a config arrived on. CloudFront applies different cache-setting
/// rules to each (recorded from real AWS on 2026-11-09).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConfigRequest {
    /// Omitted TTLs are derived, and they must be in order.
    Create,
    /// All three TTLs are required, and they need not be in order.
    Update,
}

/// Take a `DistributionConfig` a client sent to its canonical stored form, or
/// refuse it. The single door for every path that accepts a whole config.
///
/// Order matters: whether a member was supplied can only be told before
/// [`DistributionConfig::ensure_aws_defaults`] fills it in, and the remaining
/// rules read the filled-in members.
pub fn canonicalize_distribution_config(
    config: &mut DistributionConfig,
    request: ConfigRequest,
) -> Result<(), AwsServiceError> {
    validate_supplied_members(config, request)?;
    config.ensure_aws_defaults();
    validate_distribution_config(config)
}

/// The rules about what a request supplied, with messages recorded from real
/// AWS on 2026-11-09 on `CreateDistribution` (default behavior and
/// `CacheBehaviors`) and on `UpdateDistribution` (default behavior). Where several
/// rules could apply, the order follows the recorded pairs; the remaining pairs
/// are inferred from them. That `UpdateDistribution` treats a `CacheBehaviors`
/// entry like the default behavior is inferred too.
fn validate_supplied_members(
    config: &DistributionConfig,
    request: ConfigRequest,
) -> Result<(), AwsServiceError> {
    let multi_tenant = config.connection_mode.as_deref() == Some("tenant-only");
    if multi_tenant {
        if config.is_ipv6_enabled.is_some() {
            return Err(refused_for_multi_tenant("IsIPV6Enabled"));
        }
        if config.staging.is_some() {
            return Err(refused_for_multi_tenant("Staging"));
        }
        if config.price_class.is_some() {
            return Err(refused_for_multi_tenant("PriceClass"));
        }
    }
    let dcb = &config.default_cache_behavior;
    validate_cache_settings(
        multi_tenant,
        request,
        dcb.cache_policy_id.as_deref(),
        dcb.forwarded_values.is_some(),
        (dcb.min_ttl, dcb.default_ttl, dcb.max_ttl),
    )?;
    for b in config
        .cache_behaviors
        .iter()
        .filter_map(|b| b.items.as_ref())
        .flat_map(|i| i.cache_behavior.iter())
    {
        validate_cache_settings(
            multi_tenant,
            request,
            b.cache_policy_id.as_deref(),
            b.forwarded_values.is_some(),
            (b.min_ttl, b.default_ttl, b.max_ttl),
        )?;
    }
    Ok(())
}

fn refused_for_multi_tenant(member: &str) -> AwsServiceError {
    invalid_argument(format!(
        "You can't specify the {member} field for a multi-tenant distribution."
    ))
}

fn required(member: &str) -> AwsServiceError {
    invalid_argument(format!("The parameter {member} is required."))
}

fn validate_cache_settings(
    multi_tenant: bool,
    request: ConfigRequest,
    cache_policy_id: Option<&str>,
    has_forwarded_values: bool,
    (min, default, max): (Option<i64>, Option<i64>, Option<i64>),
) -> Result<(), AwsServiceError> {
    let has_policy = has_cache_policy(cache_policy_id);
    let ttls = [("MinTTL", min), ("DefaultTTL", default), ("MaxTTL", max)];
    let first_ttl_set = ttls
        .iter()
        .find(|(_, v)| v.is_some())
        .map(|(name, _)| *name);

    // Recorded with a cache policy present too: the multi-tenant refusal wins.
    if multi_tenant {
        if has_forwarded_values {
            return Err(refused_for_multi_tenant("ForwardedValues"));
        }
        if let Some(member) = first_ttl_set {
            return Err(refused_for_multi_tenant(member));
        }
        if !has_policy {
            return Err(invalid_argument(
                "The CachePolicyId field is required for a multi-tenant distribution.",
            ));
        }
        return Ok(());
    }

    if has_policy {
        // Recorded: a cache-policy refusal wins over TTLs out of order, and
        // MinTTL is named ahead of ForwardedValues.
        let forwarded = has_forwarded_values.then_some("ForwardedValues");
        return match first_ttl_set.or(forwarded) {
            Some(member) => Err(invalid_argument(format!(
                "The parameter {member} cannot be used when a cache policy is associated to the cache behavior."
            ))),
            None => Ok(()),
        };
    }

    let Some(min) = min else {
        return Err(required("MinTTL"));
    };
    if request == ConfigRequest::Update {
        if default.is_none() {
            return Err(required("DefaultTTL"));
        }
        if max.is_none() {
            return Err(required("MaxTTL"));
        }
    }
    if !has_forwarded_values {
        return Err(required("ForwardedValues"));
    }
    match request {
        // Recorded: an update stores TTLs out of order as given.
        ConfigRequest::Update => Ok(()),
        ConfigRequest::Create => check_ttl_order(min, default, max),
    }
}

fn check_ttl_order(
    min: i64,
    default: Option<i64>,
    max: Option<i64>,
) -> Result<(), AwsServiceError> {
    let message = match (default, max) {
        (Some(default), Some(max)) if min > default || default > max => {
            "MinTTL, MaxTTL and DefaultTTL should follow order MinTTL less than or equal to \
             DefaultTTL less than or equal to MaxTTL"
        }
        (Some(default), None) if min > default => {
            "MinTTL should be less than or equal to DefaultTTL"
        }
        (None, Some(max)) if min > max => "MinTTL should be less than or equal to MaxTTL",
        _ => return Ok(()),
    };
    Err(aws_error(
        StatusCode::BAD_REQUEST,
        "InvalidTTLOrder",
        message,
    ))
}

/// `CopyDistribution`'s refusals, recorded from real AWS on 2026-11-09: a copy
/// must be a staging one, a multi-tenant primary can't have one, and a primary
/// whose TTLs an update left out of order can't either. A `Staging` of false
/// getting the same refusal as an absent one is inferred from its message, as is
/// the absent check running first on a multi-tenant primary.
pub(crate) fn validate_copy(
    primary: &DistributionConfig,
    staging: Option<bool>,
) -> Result<(), AwsServiceError> {
    if staging != Some(true) {
        return Err(invalid_argument(
            "Your request must specify Staging as true.",
        ));
    }
    if primary.connection_mode.as_deref() == Some("tenant-only") {
        return Err(invalid_argument(
            "You can't enable continuous deployment for a multi-tenant distribution.",
        ));
    }
    let dcb = &primary.default_cache_behavior;
    let in_order =
        |min: Option<i64>, default: Option<i64>, max: Option<i64>| match (min, default, max) {
            (Some(min), Some(default), Some(max)) => min <= default && default <= max,
            _ => true,
        };
    let all_in_order = in_order(dcb.min_ttl, dcb.default_ttl, dcb.max_ttl)
        && primary
            .cache_behaviors
            .iter()
            .filter_map(|b| b.items.as_ref())
            .flat_map(|i| i.cache_behavior.iter())
            .all(|b| in_order(b.min_ttl, b.default_ttl, b.max_ttl));
    if all_in_order {
        return Ok(());
    }
    Err(aws_error(
        StatusCode::BAD_REQUEST,
        "InvalidTTLOrder",
        "We couldn\u{2019}t create the staging distribution. One or more cache behaviors for your \
         primary distribution has invalid time to live (TTL) settings. The minimum TTL must be \
         less than or equal to the default TTL. The default TTL must be less than or equal to \
         the maximum TTL. To create the staging distribution, update your TTL settings and try \
         again.",
    ))
}

/// Validate a canonical `DistributionConfig` the way CloudFront does on
/// `CreateDistribution` / `UpdateDistribution`.
pub fn validate_distribution_config(config: &DistributionConfig) -> Result<(), AwsServiceError> {
    if config.caller_reference.is_empty() {
        return Err(invalid_argument("CallerReference is required"));
    }
    let origins = config
        .origins
        .items
        .as_ref()
        .map(|i| i.origin.as_slice())
        .unwrap_or_default();
    if config.origins.quantity < 1 || origins.is_empty() {
        return Err(invalid_argument(
            "DistributionConfig.Origins must contain at least one origin",
        ));
    }

    // A behavior may target an origin or an origin group.
    let targets: HashSet<&str> = origins
        .iter()
        .map(|o| o.id.as_str())
        .chain(
            config
                .origin_groups
                .iter()
                .filter_map(|g| g.items.as_ref())
                .flat_map(|i| i.origin_group.iter().map(|g| g.id.as_str())),
        )
        .collect();

    let dcb = &config.default_cache_behavior;
    validate_behavior(
        &targets,
        &dcb.target_origin_id,
        dcb.allowed_methods.as_ref(),
        dcb.lambda_function_associations.as_ref(),
        dcb.function_associations.as_ref(),
    )?;
    for b in config
        .cache_behaviors
        .iter()
        .filter_map(|b| b.items.as_ref())
        .flat_map(|i| i.cache_behavior.iter())
    {
        validate_behavior(
            &targets,
            &b.target_origin_id,
            b.allowed_methods.as_ref(),
            b.lambda_function_associations.as_ref(),
            b.function_associations.as_ref(),
        )?;
    }
    if let Some(arn) = config
        .viewer_certificate
        .as_ref()
        .and_then(|vc| vc.acm_certificate_arn.as_deref())
    {
        validate_acm_certificate_region(arn)?;
    }
    if config
        .logging
        .as_ref()
        .is_some_and(|l| l.enabled && l.bucket.is_empty())
    {
        return Err(invalid_argument(
            "Logging.Bucket is required when Logging.Enabled is true",
        ));
    }
    Ok(())
}

/// CloudFront only serves ACM certificates from the partition's global
/// region (`us-east-1` for `aws`); ACM certificates are regional, so one
/// requested anywhere else is not visible to CloudFront.
fn validate_acm_certificate_region(arn: &str) -> Result<(), AwsServiceError> {
    use fakecloud_aws::arn::{arn_resource, implicit_global_region, partition_of, region_of};
    if arn_resource(arn, "acm").is_none() {
        return Ok(());
    }
    if region_of(arn) == Some(implicit_global_region(partition_of(arn))) {
        return Ok(());
    }
    Err(aws_error(
        StatusCode::BAD_REQUEST,
        "InvalidViewerCertificate",
        "The specified SSL certificate doesn't exist, isn't in us-east-1 region, isn't valid, \
         or doesn't include a valid certificate chain.",
    ))
}

fn validate_behavior(
    targets: &HashSet<&str>,
    target_origin_id: &str,
    allowed_methods: Option<&AllowedMethods>,
    lambda_associations: Option<&LambdaFunctionAssociations>,
    function_associations: Option<&FunctionAssociations>,
) -> Result<(), AwsServiceError> {
    if !targets.contains(target_origin_id) {
        return Err(aws_error(
            StatusCode::NOT_FOUND,
            "NoSuchOrigin",
            format!(
                "One or more of your origins or origin groups do not exist: {target_origin_id}"
            ),
        ));
    }

    if let Some(allowed) = allowed_methods {
        let allowed_set = method_set(&allowed.items.method);
        if !ALLOWED_METHOD_SETS.contains(&allowed_set.as_slice()) {
            return Err(invalid_argument(format!(
                "AllowedMethods must be GET,HEAD or GET,HEAD,OPTIONS or all seven methods, got {}",
                allowed_set.join(",")
            )));
        }
        if let Some(cached) = &allowed.cached_methods {
            let cached_set = method_set(&cached.items.method);
            if !CACHED_METHOD_SETS.contains(&cached_set.as_slice())
                || !cached_set.iter().all(|m| allowed_set.contains(m))
            {
                return Err(invalid_argument(format!(
                    "CachedMethods must be GET,HEAD or GET,HEAD,OPTIONS and a subset of AllowedMethods, got {}",
                    cached_set.join(",")
                )));
            }
        }
    }

    for a in lambda_associations
        .and_then(|l| l.items.as_ref())
        .map(|i| i.lambda_function_association.as_slice())
        .unwrap_or_default()
    {
        if a.lambda_function_arn.is_empty() || !EVENT_TYPES.contains(&a.event_type.as_str()) {
            return Err(aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidLambdaFunctionAssociation",
                "A Lambda function association needs a LambdaFunctionARN and a valid EventType",
            ));
        }
    }
    for a in function_associations
        .and_then(|f| f.items.as_ref())
        .map(|i| i.function_association.as_slice())
        .unwrap_or_default()
    {
        if a.function_arn.is_empty() || !EVENT_TYPES.contains(&a.event_type.as_str()) {
            return Err(aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidFunctionAssociation",
                "A function association needs a FunctionARN and a valid EventType",
            ));
        }
    }
    Ok(())
}

/// Sorted, de-duplicated, upper-cased method names.
fn method_set(methods: &[String]) -> Vec<&'static str> {
    let mut set: Vec<&'static str> = methods
        .iter()
        .filter_map(|m| {
            ["DELETE", "GET", "HEAD", "OPTIONS", "PATCH", "POST", "PUT"]
                .into_iter()
                .find(|known| known.eq_ignore_ascii_case(m))
        })
        .collect();
    // An unknown method never matches a valid set; keep it visible as such.
    if set.len() != methods.len() {
        set.push("?");
    }
    set.sort_unstable();
    set.dedup();
    set
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{
        CachedMethods, FunctionAssociation, FunctionAssociationItems, MethodList, Origin,
        OriginItems, Origins,
    };

    fn config() -> DistributionConfig {
        let mut c = DistributionConfig {
            caller_reference: "ref".into(),
            origins: Origins {
                quantity: 1,
                items: Some(OriginItems {
                    origin: vec![Origin {
                        id: "o1".into(),
                        domain_name: "o.example.com".into(),
                        ..Default::default()
                    }],
                }),
            },
            ..Default::default()
        };
        c.default_cache_behavior.target_origin_id = "o1".into();
        c.default_cache_behavior.viewer_protocol_policy = "allow-all".into();
        c
    }

    fn methods(m: &[&str]) -> MethodList {
        MethodList {
            method: m.iter().map(|s| s.to_string()).collect(),
        }
    }

    #[test]
    fn an_acm_certificate_outside_us_east_1_is_rejected() {
        let mut c = config();
        c.viewer_certificate = Some(crate::model::ViewerCertificate {
            acm_certificate_arn: Some("arn:aws:acm:eu-west-1:123456789012:certificate/abc".into()),
            ..Default::default()
        });
        assert_eq!(
            validate_distribution_config(&c).unwrap_err().code(),
            "InvalidViewerCertificate"
        );
        c.viewer_certificate = Some(crate::model::ViewerCertificate {
            acm_certificate_arn: Some("arn:aws:acm:us-east-1:123456789012:certificate/abc".into()),
            ..Default::default()
        });
        validate_distribution_config(&c).unwrap();
        // China's CloudFront uses certificates from its global region.
        c.viewer_certificate = Some(crate::model::ViewerCertificate {
            acm_certificate_arn: Some(
                "arn:aws-cn:acm:cn-northwest-1:123456789012:certificate/abc".into(),
            ),
            ..Default::default()
        });
        validate_distribution_config(&c).unwrap();
    }

    #[test]
    fn a_deprecated_acm_certificate_outside_us_east_1_is_rejected() {
        let mut c = config();
        c.viewer_certificate = Some(crate::model::ViewerCertificate {
            certificate: Some("arn:aws:acm:eu-west-1:123456789012:certificate/abc".into()),
            certificate_source: Some("acm".into()),
            ..Default::default()
        });
        c.ensure_aws_defaults();
        assert_eq!(
            validate_distribution_config(&c).unwrap_err().code(),
            "InvalidViewerCertificate"
        );
    }

    #[test]
    fn enabled_logging_needs_a_bucket() {
        let mut c = config();
        c.logging = Some(crate::model::LoggingConfig {
            enabled: true,
            ..Default::default()
        });
        assert_eq!(
            validate_distribution_config(&c).unwrap_err().code(),
            "InvalidArgument"
        );
        c.logging.as_mut().unwrap().bucket = "logs.s3.amazonaws.com".into();
        validate_distribution_config(&c).unwrap();
        c.logging = Some(crate::model::LoggingConfig::default());
        validate_distribution_config(&c).unwrap();
    }

    const ORDER: &str = "MinTTL, MaxTTL and DefaultTTL should follow order MinTTL less than or \
                         equal to DefaultTTL less than or equal to MaxTTL";
    const MIN_MAX: &str = "MinTTL should be less than or equal to MaxTTL";
    const MIN_DEFAULT: &str = "MinTTL should be less than or equal to DefaultTTL";
    const POLICY: &str = "658327ea-f89d-4fab-a63d-7e88639e58f6";

    fn req(member: &str) -> String {
        format!("The parameter {member} is required.")
    }

    fn with_policy(member: &str) -> String {
        format!(
            "The parameter {member} cannot be used when a cache policy is associated to the cache behavior."
        )
    }

    fn tenant_refusal(member: &str) -> String {
        format!("You can't specify the {member} field for a multi-tenant distribution.")
    }

    fn forwarded() -> crate::model::ForwardedValues {
        crate::model::ForwardedValues {
            cookies: crate::model::CookiePreference {
                forward: "none".into(),
                whitelisted_names: None,
            },
            ..Default::default()
        }
    }

    /// One cache-settings case for a behavior.
    #[derive(Clone, Copy, Default)]
    struct Case {
        policy: Option<&'static str>,
        forwarded: bool,
        min: Option<i64>,
        default: Option<i64>,
        max: Option<i64>,
    }

    const LEGACY: Case = Case {
        policy: None,
        forwarded: true,
        min: None,
        default: None,
        max: None,
    };

    const ON_POLICY: Case = Case {
        policy: Some(POLICY),
        forwarded: false,
        min: None,
        default: None,
        max: None,
    };

    fn ttls(min: Option<i64>, default: Option<i64>, max: Option<i64>, base: Case) -> Case {
        Case {
            min,
            default,
            max,
            ..base
        }
    }

    fn refusal(
        config: &mut DistributionConfig,
        request: ConfigRequest,
    ) -> Option<(String, String)> {
        canonicalize_distribution_config(config, request)
            .err()
            .map(|e| (e.code().to_string(), e.message().to_string()))
    }

    /// The refusal for the case on the default behavior; a `CacheBehaviors` entry
    /// must get the same one.
    fn outcome(case: Case, tenant_only: bool, request: ConfigRequest) -> Option<(String, String)> {
        let policy = case.policy.map(str::to_string);
        let fv = case.forwarded.then(forwarded);
        let mode = tenant_only.then(|| "tenant-only".to_string());

        let mut on_default = config();
        on_default.connection_mode = mode.clone();
        let dcb = &mut on_default.default_cache_behavior;
        dcb.cache_policy_id = policy.clone();
        dcb.forwarded_values = fv.clone();
        (dcb.min_ttl, dcb.default_ttl, dcb.max_ttl) = (case.min, case.default, case.max);
        let from_default = refusal(&mut on_default, request);

        let mut on_path = config();
        on_path.connection_mode = mode;
        on_path.default_cache_behavior.cache_policy_id = Some(POLICY.into());
        on_path.cache_behaviors = Some(crate::model::CacheBehaviors {
            quantity: 1,
            items: Some(crate::model::CacheBehaviorItems {
                cache_behavior: vec![crate::model::CacheBehavior {
                    path_pattern: "/x".into(),
                    target_origin_id: "o1".into(),
                    viewer_protocol_policy: "allow-all".into(),
                    cache_policy_id: policy,
                    forwarded_values: fv,
                    min_ttl: case.min,
                    default_ttl: case.default,
                    max_ttl: case.max,
                    ..Default::default()
                }],
            }),
        });
        assert_eq!(from_default, refusal(&mut on_path, request));
        from_default
    }

    fn invalid(message: &str) -> Option<(String, String)> {
        Some(("InvalidArgument".to_string(), message.to_string()))
    }

    fn order(message: &str) -> Option<(String, String)> {
        Some(("InvalidTTLOrder".to_string(), message.to_string()))
    }

    /// Every row was recorded from real AWS `CreateDistribution` on 2026-11-09.
    /// `CacheBehaviors` were recorded for the no-MinTTL, order and cache-policy
    /// rules, and share the code path of the rest.
    #[test]
    fn create_refuses_cache_settings_as_recorded() {
        let (some, none) = (Some, None::<i64>);
        let neither = Case::default();
        for (case, expected) in [
            (LEGACY, invalid(&req("MinTTL"))),
            (ttls(none, none, some(600), LEGACY), invalid(&req("MinTTL"))),
            (
                ttls(none, some(40_000_000), none, LEGACY),
                invalid(&req("MinTTL")),
            ),
            (neither, invalid(&req("MinTTL"))),
            (
                ttls(some(0), none, none, neither),
                invalid(&req("ForwardedValues")),
            ),
            (
                ttls(some(0), none, some(600), neither),
                invalid(&req("ForwardedValues")),
            ),
            (
                ttls(
                    some(0),
                    none,
                    none,
                    Case {
                        policy: Some(""),
                        ..neither
                    },
                ),
                invalid(&req("ForwardedValues")),
            ),
            (ttls(some(700), some(600), some(800), LEGACY), order(ORDER)),
            (ttls(some(0), some(900), some(800), LEGACY), order(ORDER)),
            (ttls(some(700), none, some(600), LEGACY), order(MIN_MAX)),
            (ttls(some(700), some(600), none, LEGACY), order(MIN_DEFAULT)),
            (
                ttls(some(0), none, some(600), ON_POLICY),
                invalid(&with_policy("MinTTL")),
            ),
            // The cache-policy refusal wins over TTLs out of order.
            (
                ttls(some(700), none, some(600), ON_POLICY),
                invalid(&with_policy("MinTTL")),
            ),
            (
                ttls(none, some(600), none, ON_POLICY),
                invalid(&with_policy("DefaultTTL")),
            ),
            (
                ttls(none, none, some(600), ON_POLICY),
                invalid(&with_policy("MaxTTL")),
            ),
            (
                Case {
                    forwarded: true,
                    ..ON_POLICY
                },
                invalid(&with_policy("ForwardedValues")),
            ),
        ] {
            assert_eq!(outcome(case, false, ConfigRequest::Create), expected);
        }
    }

    /// Recorded from real AWS `CreateDistribution` on 2026-11-09.
    #[test]
    fn create_accepts_cache_settings_as_recorded() {
        let (some, none) = (Some, None::<i64>);
        for case in [
            ON_POLICY,
            ttls(some(0), none, none, LEGACY),
            ttls(some(0), none, some(600), LEGACY),
            ttls(some(0), some(600), none, LEGACY),
            ttls(some(0), some(90_000), none, LEGACY),
            ttls(some(0), some(40_000_000), none, LEGACY),
            ttls(some(100_000), none, none, LEGACY),
            ttls(some(40_000_000), none, none, LEGACY),
            ttls(
                some(0),
                none,
                none,
                Case {
                    policy: Some(""),
                    ..LEGACY
                },
            ),
        ] {
            assert_eq!(outcome(case, false, ConfigRequest::Create), None);
        }
    }

    /// Recorded from real AWS `UpdateDistribution` on 2026-11-09, which differs
    /// from `CreateDistribution`: nothing is derived and TTLs need no order.
    #[test]
    fn update_requires_every_ttl_but_not_their_order() {
        let (some, none) = (Some, None::<i64>);
        for (case, expected) in [
            (
                ttls(some(0), none, some(31_536_000), LEGACY),
                invalid(&req("DefaultTTL")),
            ),
            (
                ttls(some(0), some(86_400), none, LEGACY),
                invalid(&req("MaxTTL")),
            ),
            (
                ttls(none, some(86_400), some(31_536_000), LEGACY),
                invalid(&req("MinTTL")),
            ),
            (
                ttls(some(700), none, some(600), LEGACY),
                invalid(&req("DefaultTTL")),
            ),
            // Out of order, and accepted.
            (ttls(some(700), some(600), some(800), LEGACY), None),
            (ttls(some(700), some(600), some(600), LEGACY), None),
            (
                Case {
                    policy: Some(POLICY),
                    ..ttls(some(0), some(86_400), some(31_536_000), LEGACY)
                },
                invalid(&with_policy("MinTTL")),
            ),
        ] {
            assert_eq!(outcome(case, false, ConfigRequest::Update), expected);
        }
    }

    #[test]
    fn an_update_keeps_ttls_out_of_order_as_given() {
        let mut c = config();
        let dcb = &mut c.default_cache_behavior;
        dcb.forwarded_values = Some(forwarded());
        (dcb.min_ttl, dcb.default_ttl, dcb.max_ttl) = (Some(700), Some(600), Some(800));
        canonicalize_distribution_config(&mut c, ConfigRequest::Update).unwrap();
        let dcb = &c.default_cache_behavior;
        assert_eq!(
            (dcb.min_ttl, dcb.default_ttl, dcb.max_ttl),
            (Some(700), Some(600), Some(800))
        );
    }

    /// Recorded: accepted, returned without `CachePolicyId`, TTLs derived.
    #[test]
    fn an_empty_cache_policy_id_is_no_cache_policy() {
        let mut c = config();
        c.default_cache_behavior.cache_policy_id = Some(String::new());
        c.default_cache_behavior.forwarded_values = Some(forwarded());
        c.default_cache_behavior.min_ttl = Some(0);
        canonicalize_distribution_config(&mut c, ConfigRequest::Create).unwrap();
        let dcb = &c.default_cache_behavior;
        assert_eq!(dcb.cache_policy_id, None);
        assert_eq!(
            (dcb.min_ttl, dcb.default_ttl, dcb.max_ttl),
            (Some(0), Some(86_400), Some(31_536_000))
        );
    }

    /// Recorded from real AWS on 2026-11-09.
    #[test]
    fn multi_tenant_refusals_are_as_recorded() {
        let (some, none) = (Some, None::<i64>);
        let create = ConfigRequest::Create;
        for (case, expected) in [
            (ON_POLICY, None),
            (
                ttls(some(0), none, none, ON_POLICY),
                invalid(&tenant_refusal("MinTTL")),
            ),
            (
                ttls(none, some(60), none, ON_POLICY),
                invalid(&tenant_refusal("DefaultTTL")),
            ),
            (
                ttls(none, none, some(60), ON_POLICY),
                invalid(&tenant_refusal("MaxTTL")),
            ),
            (LEGACY, invalid(&tenant_refusal("ForwardedValues"))),
            (
                Case::default(),
                invalid("The CachePolicyId field is required for a multi-tenant distribution."),
            ),
        ] {
            assert_eq!(outcome(case, true, create), expected);
        }

        let refused = |set: fn(&mut DistributionConfig)| {
            let mut c = config();
            c.connection_mode = Some("tenant-only".into());
            c.default_cache_behavior.cache_policy_id = Some(POLICY.into());
            set(&mut c);
            refusal(&mut c, ConfigRequest::Create)
        };
        for (set, member) in [
            (
                (|c| c.is_ipv6_enabled = Some(false)) as fn(&mut DistributionConfig),
                "IsIPV6Enabled",
            ),
            (|c| c.is_ipv6_enabled = Some(true), "IsIPV6Enabled"),
            (|c| c.staging = Some(false), "Staging"),
            (
                |c| c.price_class = Some("PriceClass_100".into()),
                "PriceClass",
            ),
            // Both set: IsIPV6Enabled is named first.
            (
                |c| {
                    c.is_ipv6_enabled = Some(false);
                    c.staging = Some(false);
                },
                "IsIPV6Enabled",
            ),
        ] {
            assert_eq!(refused(set), invalid(&tenant_refusal(member)));
        }
        for version in ["http1.1", "http2", "http2and3"] {
            let mut c = config();
            c.connection_mode = Some("tenant-only".into());
            c.default_cache_behavior.cache_policy_id = Some(POLICY.into());
            c.http_version = Some(version.into());
            canonicalize_distribution_config(&mut c, ConfigRequest::Create).unwrap();
            assert_eq!(c.http_version.as_deref(), Some(version));
        }
    }

    /// Recorded from real AWS `CopyDistribution` on 2026-11-09, except `false`.
    #[test]
    fn copy_refusals_are_as_recorded() {
        let mut standard = config();
        standard.default_cache_behavior.forwarded_values = Some(forwarded());
        standard.default_cache_behavior.min_ttl = Some(0);
        canonicalize_distribution_config(&mut standard, ConfigRequest::Create).unwrap();

        let must = "Your request must specify Staging as true.";
        for staging in [None, Some(false)] {
            let err = validate_copy(&standard, staging).unwrap_err();
            assert_eq!(
                (err.code(), err.message().as_str()),
                ("InvalidArgument", must)
            );
        }
        validate_copy(&standard, Some(true)).unwrap();

        let mut tenant = config();
        tenant.connection_mode = Some("tenant-only".into());
        let err = validate_copy(&tenant, Some(true)).unwrap_err();
        assert_eq!(
            err.message(),
            "You can't enable continuous deployment for a multi-tenant distribution."
        );

        let mut inverted = standard.clone();
        let dcb = &mut inverted.default_cache_behavior;
        (dcb.min_ttl, dcb.default_ttl, dcb.max_ttl) = (Some(700), Some(600), Some(600));
        let err = validate_copy(&inverted, Some(true)).unwrap_err();
        assert_eq!(err.code(), "InvalidTTLOrder");
        assert_eq!(
            err.message(),
            "We couldn\u{2019}t create the staging distribution. One or more cache behaviors \
             for your primary distribution has invalid time to live (TTL) settings. The minimum \
             TTL must be less than or equal to the default TTL. The default TTL must be less \
             than or equal to the maximum TTL. To create the staging distribution, update your \
             TTL settings and try again."
        );
    }

    /// `Staging=true` was accepted by real AWS `CreateDistribution` on a
    /// standard distribution.
    #[test]
    fn a_standard_distribution_may_set_staging_and_turn_ipv6_off() {
        let mut c = config();
        c.default_cache_behavior.cache_policy_id = Some(POLICY.into());
        c.staging = Some(true);
        c.is_ipv6_enabled = Some(false);
        canonicalize_distribution_config(&mut c, ConfigRequest::Create).unwrap();
        assert_eq!(c.staging, Some(true));
        assert_eq!(c.is_ipv6_enabled, Some(false));
    }

    /// A config is what `GetDistributionConfig` hands back, so it must pass an
    /// update unchanged: defaults filled in are not "supplied" on the way back.
    #[test]
    fn a_canonical_config_is_accepted_by_an_update() {
        let mut standard = config();
        standard.default_cache_behavior.min_ttl = Some(0);
        standard.default_cache_behavior.forwarded_values = Some(forwarded());
        canonicalize_distribution_config(&mut standard, ConfigRequest::Create).unwrap();
        canonicalize_distribution_config(&mut standard, ConfigRequest::Update).unwrap();

        let mut tenant = config();
        tenant.connection_mode = Some("tenant-only".into());
        tenant.default_cache_behavior.cache_policy_id = Some(POLICY.into());
        canonicalize_distribution_config(&mut tenant, ConfigRequest::Create).unwrap();
        canonicalize_distribution_config(&mut tenant, ConfigRequest::Update).unwrap();
    }

    #[test]
    fn a_valid_config_passes() {
        validate_distribution_config(&config()).unwrap();
    }

    #[test]
    fn a_target_naming_no_origin_is_no_such_origin() {
        let mut c = config();
        c.default_cache_behavior.target_origin_id = "missing".into();
        let err = validate_distribution_config(&c).unwrap_err();
        assert_eq!(err.code(), "NoSuchOrigin");
    }

    #[test]
    fn cached_methods_outside_allowed_methods_are_rejected() {
        let mut c = config();
        c.default_cache_behavior.allowed_methods = Some(AllowedMethods {
            quantity: 2,
            items: methods(&["GET", "HEAD"]),
            cached_methods: Some(CachedMethods {
                quantity: 3,
                items: methods(&["GET", "HEAD", "OPTIONS"]),
            }),
        });
        let err = validate_distribution_config(&c).unwrap_err();
        assert_eq!(err.code(), "InvalidArgument");
    }

    #[test]
    fn an_allowed_method_set_cloudfront_does_not_offer_is_rejected() {
        let mut c = config();
        c.default_cache_behavior.allowed_methods = Some(AllowedMethods {
            quantity: 3,
            items: methods(&["GET", "HEAD", "POST"]),
            cached_methods: None,
        });
        assert_eq!(
            validate_distribution_config(&c).unwrap_err().code(),
            "InvalidArgument"
        );
    }

    #[test]
    fn a_function_association_without_an_arn_is_rejected() {
        let mut c = config();
        c.default_cache_behavior.function_associations = Some(FunctionAssociations {
            quantity: 1,
            items: Some(FunctionAssociationItems {
                function_association: vec![FunctionAssociation {
                    function_arn: String::new(),
                    event_type: "viewer-request".into(),
                }],
            }),
        });
        assert_eq!(
            validate_distribution_config(&c).unwrap_err().code(),
            "InvalidFunctionAssociation"
        );
    }
}
