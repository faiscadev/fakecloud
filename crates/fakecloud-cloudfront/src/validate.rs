//! `DistributionConfig` validation shared by `CreateDistribution`,
//! `UpdateDistribution` and the CloudFormation provisioner, so a config is
//! accepted or rejected the same way whichever door it comes in through.

use std::collections::HashSet;

use fakecloud_core::service::AwsServiceError;
use http::StatusCode;

use crate::model::{
    AllowedMethods, DistributionConfig, FunctionAssociations, LambdaFunctionAssociations,
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

/// Validate a `DistributionConfig` the way CloudFront does on
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
