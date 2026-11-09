//! Translate an `AWS::CloudFront::Distribution` `DistributionConfig` property
//! into the CloudFront wire model.
//!
//! The CloudFormation shape is not the API shape, so nothing here goes through
//! `serde_json::from_value` into the wire structs:
//!
//! - lists are flat (`AllowedMethods: [..]`, `TrustedSigners: [..]`,
//!   `CustomErrorResponses: [..]`) where the API nests them under
//!   `Quantity` + `Items`;
//! - several members are named differently (`OriginCustomHeaders` vs
//!   `CustomHeaders`, `OriginSSLProtocols` vs `OriginSslProtocols`,
//!   `AcmCertificateArn` vs `ACMCertificateArn`, `IPV6Enabled` vs
//!   `IsIPV6Enabled`, and `CachedMethods` sits beside `AllowedMethods` rather
//!   than inside it);
//! - scalar types differ (`ResponseCode` is an Integer in CloudFormation and a
//!   string in the API; the TTLs are Doubles in CloudFormation and Longs in the
//!   API);
//! - YAML templates and resolved intrinsics (`Ref` to a parameter, `Fn::If`)
//!   hand numbers and booleans over as strings, and CloudFormation stringifies
//!   scalars given for String properties;
//! - the legacy `S3Origin` / `CustomOrigin` members describe a single origin
//!   in place of `Origins`.
//!
//! Every member is mapped explicitly. A missing required member or a
//! wrongly-typed value fails the resource the way CloudFormation's schema
//! validation would, and the translated config then goes through the same
//! validation `CreateDistribution` / `UpdateDistribution` apply, so a config
//! the API would reject fails the resource with the API's error.

use fakecloud_cloudfront::model::{
    has_cache_policy, AliasItems, Aliases, AllowedMethods, AwsAccountNumberList, CacheBehavior,
    CacheBehaviorItems, CacheBehaviors, CacheTagConfig, CachedMethods,
    ConnectionFunctionAssociation, CookieNameList, CookieNames, CookiePreference,
    CustomErrorResponse, CustomErrorResponseItems, CustomErrorResponses, CustomHeaderItems,
    CustomHeaders, CustomOriginConfig, DefaultCacheBehavior, DistributionConfig, ForwardedValues,
    FunctionAssociation, FunctionAssociationItems, FunctionAssociations, GeoRestriction,
    GrpcConfig, HeaderList, Headers, LambdaFunctionAssociation, LambdaFunctionAssociationItems,
    LambdaFunctionAssociations, LocationList, LoggingConfig, MethodList, Origin,
    OriginCustomHeader, OriginGroup, OriginGroupFailoverCriteria, OriginGroupItems,
    OriginGroupMember, OriginGroupMemberItems, OriginGroupMembers, OriginGroups, OriginItems,
    OriginMtlsConfig, OriginShield, OriginSslProtocols, Origins, ParameterDefinition,
    ParameterDefinitionSchema, ParameterDefinitions, QueryStringCacheKeyList, QueryStringCacheKeys,
    Restrictions, S3OriginConfig, SslProtocolItems, StatusCodeItems, StatusCodes,
    StringSchemaConfig, TenantConfig, TrustStoreConfig, TrustedKeyGroupIdList, TrustedKeyGroups,
    TrustedSigners, ViewerCertificate, ViewerMtlsConfig, VpcOriginConfig,
};
use fakecloud_cloudfront::{canonicalize_distribution_config, ConfigRequest};
use serde_json::Value;

type CfnResult<T> = Result<T, String>;

/// Build the wire `DistributionConfig` from a CloudFormation
/// `DistributionConfig` and validate it as CloudFront would. Members absent
/// from the template take CloudFront's defaults, so an update that drops one
/// resets it.
pub(super) fn cfn_distribution_config(
    cfg: &Value,
    caller_reference: String,
) -> CfnResult<DistributionConfig> {
    let at = |path: &'static str| move |e: String| format!("DistributionConfig.{path}: {e}");

    let default_cache_behavior = cfn_default_cache_behavior(
        opt_obj(cfg, "DefaultCacheBehavior")?
            .ok_or("DistributionConfig.DefaultCacheBehavior is required")?,
    )
    .map_err(at("DefaultCacheBehavior"))?;

    let origin = cfn_origins(cfg, &default_cache_behavior.target_origin_id)?;

    let cache_behaviors = obj_items(cfg, "CacheBehaviors")?
        .map(|items| {
            let cache_behavior = items
                .into_iter()
                .enumerate()
                .map(|(i, b)| {
                    cfn_cache_behavior(b)
                        .map_err(|e| format!("DistributionConfig.CacheBehaviors[{i}]: {e}"))
                })
                .collect::<CfnResult<Vec<_>>>()?;
            let (quantity, items) = counted(cache_behavior, |cache_behavior| CacheBehaviorItems {
                cache_behavior,
            });
            Ok::<_, String>(CacheBehaviors { quantity, items })
        })
        .transpose()?;

    let custom_error_responses = obj_items(cfg, "CustomErrorResponses")?
        .map(|items| {
            let custom_error_response = items
                .into_iter()
                .enumerate()
                .map(|(i, r)| {
                    cfn_custom_error_response(r)
                        .map_err(|e| format!("DistributionConfig.CustomErrorResponses[{i}]: {e}"))
                })
                .collect::<CfnResult<Vec<_>>>()?;
            let (quantity, items) = counted(custom_error_response, |custom_error_response| {
                CustomErrorResponseItems {
                    custom_error_response,
                }
            });
            Ok::<_, String>(CustomErrorResponses { quantity, items })
        })
        .transpose()?;

    // `CNAMEs` is the legacy spelling of `Aliases`; honor it when `Aliases` is
    // absent.
    let aliases = match str_list(cfg, "Aliases")? {
        Some(cname) => Some(cname),
        None => str_list(cfg, "CNAMEs")?,
    }
    .map(|cname| {
        let (quantity, items) = counted(cname, |cname| AliasItems { cname });
        Aliases { quantity, items }
    });

    let mut config = DistributionConfig {
        caller_reference,
        aliases,
        default_root_object: opt_str(cfg, "DefaultRootObject")?,
        origins: Origins {
            quantity: len(&origin),
            items: Some(OriginItems { origin }),
        },
        origin_groups: opt_obj(cfg, "OriginGroups")?
            .map(cfn_origin_groups)
            .transpose()
            .map_err(at("OriginGroups"))?,
        default_cache_behavior,
        cache_behaviors,
        custom_error_responses,
        comment: opt_str(cfg, "Comment")?.unwrap_or_default(),
        logging: opt_obj(cfg, "Logging")?
            .map(cfn_logging)
            .transpose()
            .map_err(at("Logging"))?,
        price_class: opt_str(cfg, "PriceClass")?,
        enabled: opt_bool(cfg, "Enabled")?.ok_or("DistributionConfig.Enabled is required")?,
        viewer_certificate: opt_obj(cfg, "ViewerCertificate")?
            .map(cfn_viewer_certificate)
            .transpose()
            .map_err(at("ViewerCertificate"))?,
        restrictions: opt_obj(cfg, "Restrictions")?
            .map(cfn_restrictions)
            .transpose()
            .map_err(at("Restrictions"))?,
        web_acl_id: opt_str(cfg, "WebACLId")?,
        // CloudFormation documents its own default here, unlike the API's http2.
        http_version: Some(opt_str(cfg, "HttpVersion")?.unwrap_or_else(|| "http1.1".to_string())),
        is_ipv6_enabled: opt_bool(cfg, "IPV6Enabled")?,
        continuous_deployment_policy_id: opt_str(cfg, "ContinuousDeploymentPolicyId")?,
        staging: opt_bool(cfg, "Staging")?,
        anycast_ip_list_id: opt_str(cfg, "AnycastIpListId")?,
        tenant_config: opt_obj(cfg, "TenantConfig")?
            .map(cfn_tenant_config)
            .transpose()
            .map_err(at("TenantConfig"))?,
        connection_mode: opt_str(cfg, "ConnectionMode")?,
        viewer_mtls_config: opt_obj(cfg, "ViewerMtlsConfig")?
            .map(cfn_viewer_mtls_config)
            .transpose()
            .map_err(at("ViewerMtlsConfig"))?,
        connection_function_association: opt_obj(cfg, "ConnectionFunctionAssociation")?
            .map(|a| {
                Ok::<_, String>(ConnectionFunctionAssociation {
                    id: req_str(a, "Id")?,
                })
            })
            .transpose()
            .map_err(at("ConnectionFunctionAssociation"))?,
        cache_tag_config: opt_obj(cfg, "CacheTagConfig")?
            .map(|c| {
                Ok::<_, String>(CacheTagConfig {
                    header_name: req_str(c, "HeaderName")?,
                })
            })
            .transpose()
            .map_err(at("CacheTagConfig"))?,
    };

    apply_schema_ttl_defaults(&mut config);
    let invalid_request = |message: &str| {
        format!("Invalid request provided: AWS::CloudFront::Distribution: {message}")
    };
    // Before the API's own defaults fill it in.
    let omits_protocol_version = config
        .viewer_certificate
        .as_ref()
        .is_none_or(|vc| vc.minimum_protocol_version.is_none());
    // A template states the whole resource and omitted members take defaults,
    // on a stack update too, so both go through the rules of a create.
    canonicalize_distribution_config(&mut config, ConfigRequest::Create)
        .map_err(|e| invalid_request(e.message().as_str()))?;
    // Recorded from real CloudFormation on 2026-11-09 for a multi-tenant template
    // with no `ViewerCertificate` at all. Taking one that names a certificate but
    // no protocol version to fail alike, and an explicit version to pass, is
    // inferred.
    if config.is_multi_tenant() && omits_protocol_version {
        return Err(invalid_request(
            "The parameter ViewerCertificate The specified distribution is configured for SNI \
             and the minimum SSL protocol version is specified as SSLv3. You cannot specify both.",
        ));
    }
    Ok(config)
}

/// Recorded from real CloudFormation on 2026-11-09: `MinTTL` 0, `DefaultTTL` 86400
/// and `MaxTTL` 31536000 are each filled in independently of the others and sent to
/// the API, so unlike an API request's derivation `MaxTTL` 600 alone fails as out of
/// order. A behavior on a cache policy, or any behavior of a multi-tenant
/// distribution, must not carry them at all.
fn apply_schema_ttl_defaults(config: &mut DistributionConfig) {
    if config.is_multi_tenant() {
        return;
    }
    let fill = |min: &mut Option<i64>, default: &mut Option<i64>, max: &mut Option<i64>| {
        min.get_or_insert(0);
        default.get_or_insert(86_400);
        max.get_or_insert(31_536_000);
    };
    let dcb = &mut config.default_cache_behavior;
    if !has_cache_policy(dcb.cache_policy_id.as_deref()) {
        fill(&mut dcb.min_ttl, &mut dcb.default_ttl, &mut dcb.max_ttl);
    }
    for b in config
        .cache_behaviors
        .iter_mut()
        .filter_map(|b| b.items.as_mut())
        .flat_map(|i| &mut i.cache_behavior)
    {
        if !has_cache_policy(b.cache_policy_id.as_deref()) {
            fill(&mut b.min_ttl, &mut b.default_ttl, &mut b.max_ttl);
        }
    }
}

/// The distribution's origins: `Origins`, or the single origin the legacy
/// `S3Origin` / `CustomOrigin` member describes.
///
/// A legacy origin carries no `Id` of its own, so it takes the default cache
/// behavior's `TargetOriginId`, the one name the template already routes to it
/// by.
fn cfn_origins(cfg: &Value, default_target: &str) -> CfnResult<Vec<Origin>> {
    let origins = obj_items(cfg, "Origins")?;
    let s3 = opt_obj(cfg, "S3Origin")?;
    let custom = opt_obj(cfg, "CustomOrigin")?;
    match (origins, s3, custom) {
        (Some(items), None, None) => items
            .into_iter()
            .enumerate()
            .map(|(i, o)| {
                cfn_origin(o).map_err(|e| format!("DistributionConfig.Origins[{i}]: {e}"))
            })
            .collect(),
        (None, Some(s3), None) => Ok(vec![Origin {
            id: default_target.to_string(),
            domain_name: req_str(s3, "DNSName")
                .map_err(|e| format!("DistributionConfig.S3Origin: {e}"))?,
            s3_origin_config: Some(S3OriginConfig {
                origin_access_identity: opt_str(s3, "OriginAccessIdentity")?.unwrap_or_default(),
                origin_read_timeout: None,
            }),
            ..Default::default()
        }]),
        (None, None, Some(c)) => {
            let at = |e: String| format!("DistributionConfig.CustomOrigin: {e}");
            let ssl_protocol = str_list(c, "OriginSSLProtocols")
                .map_err(at)?
                .ok_or_else(|| at("OriginSSLProtocols is required".to_string()))?;
            Ok(vec![Origin {
                id: default_target.to_string(),
                domain_name: req_str(c, "DNSName").map_err(at)?,
                custom_origin_config: Some(CustomOriginConfig {
                    http_port: opt_i32(c, "HTTPPort").map_err(at)?.unwrap_or(80),
                    https_port: opt_i32(c, "HTTPSPort").map_err(at)?.unwrap_or(443),
                    origin_protocol_policy: req_str(c, "OriginProtocolPolicy").map_err(at)?,
                    origin_ssl_protocols: Some(OriginSslProtocols {
                        quantity: len(&ssl_protocol),
                        items: SslProtocolItems { ssl_protocol },
                    }),
                    ..Default::default()
                }),
                ..Default::default()
            }])
        }
        (None, None, None) => Err(
            "DistributionConfig.Origins is required (or the legacy S3Origin / CustomOrigin)"
                .to_string(),
        ),
        _ => Err(
            "DistributionConfig: specify only one of Origins, S3Origin and CustomOrigin"
                .to_string(),
        ),
    }
}

fn cfn_origin(o: &Value) -> CfnResult<Origin> {
    let custom_headers = obj_items(o, "OriginCustomHeaders")?
        .map(|items| {
            let origin_custom_header = items
                .into_iter()
                .map(|h| {
                    Ok(OriginCustomHeader {
                        header_name: req_str(h, "HeaderName")?,
                        header_value: req_str(h, "HeaderValue")?,
                    })
                })
                .collect::<CfnResult<Vec<_>>>()
                .map_err(|e| format!("OriginCustomHeaders: {e}"))?;
            let (quantity, items) = counted(origin_custom_header, |origin_custom_header| {
                CustomHeaderItems {
                    origin_custom_header,
                }
            });
            Ok::<_, String>(CustomHeaders { quantity, items })
        })
        .transpose()?;

    Ok(Origin {
        id: req_str(o, "Id")?,
        domain_name: req_str(o, "DomainName")?,
        origin_path: opt_str(o, "OriginPath")?,
        custom_headers,
        s3_origin_config: opt_obj(o, "S3OriginConfig")?
            .map(|s| {
                Ok::<_, String>(S3OriginConfig {
                    // Optional in CloudFormation (an origin using an origin
                    // access control carries an empty one); the API spells
                    // "no identity" as the empty string.
                    origin_access_identity: opt_str(s, "OriginAccessIdentity")?.unwrap_or_default(),
                    origin_read_timeout: opt_i32(s, "OriginReadTimeout")?,
                })
            })
            .transpose()
            .map_err(|e| format!("S3OriginConfig: {e}"))?,
        custom_origin_config: opt_obj(o, "CustomOriginConfig")?
            .map(cfn_custom_origin_config)
            .transpose()
            .map_err(|e| format!("CustomOriginConfig: {e}"))?,
        vpc_origin_config: opt_obj(o, "VpcOriginConfig")?
            .map(|v| {
                Ok::<_, String>(VpcOriginConfig {
                    vpc_origin_id: req_str(v, "VpcOriginId")?,
                    owner_account_id: opt_str(v, "OwnerAccountId")?,
                    origin_read_timeout: opt_i32(v, "OriginReadTimeout")?,
                    origin_keepalive_timeout: opt_i32(v, "OriginKeepaliveTimeout")?,
                })
            })
            .transpose()
            .map_err(|e| format!("VpcOriginConfig: {e}"))?,
        connection_attempts: opt_i32(o, "ConnectionAttempts")?,
        connection_timeout: opt_i32(o, "ConnectionTimeout")?,
        origin_shield: opt_obj(o, "OriginShield")?
            .map(|s| {
                Ok::<_, String>(OriginShield {
                    enabled: opt_bool(s, "Enabled")?.unwrap_or(false),
                    origin_shield_region: opt_str(s, "OriginShieldRegion")?,
                })
            })
            .transpose()
            .map_err(|e| format!("OriginShield: {e}"))?,
        origin_access_control_id: opt_str(o, "OriginAccessControlId")?,
        response_completion_timeout: opt_i32(o, "ResponseCompletionTimeout")?,
    })
}

fn cfn_custom_origin_config(c: &Value) -> CfnResult<CustomOriginConfig> {
    Ok(CustomOriginConfig {
        // CloudFormation defaults the ports; the API requires them.
        http_port: opt_i32(c, "HTTPPort")?.unwrap_or(80),
        https_port: opt_i32(c, "HTTPSPort")?.unwrap_or(443),
        origin_protocol_policy: req_str(c, "OriginProtocolPolicy")?,
        origin_ssl_protocols: {
            // Recorded from real CloudFormation on 2026-11-09, unlike the API's
            // own default.
            let ssl_protocol = str_list(c, "OriginSSLProtocols")?
                .unwrap_or_else(|| vec!["SSLv3".to_string(), "TLSv1".to_string()]);
            Some(OriginSslProtocols {
                quantity: len(&ssl_protocol),
                items: SslProtocolItems { ssl_protocol },
            })
        },
        origin_read_timeout: opt_i32(c, "OriginReadTimeout")?,
        origin_keepalive_timeout: opt_i32(c, "OriginKeepaliveTimeout")?,
        ip_address_type: opt_str(c, "IpAddressType")?,
        origin_mtls_config: opt_obj(c, "OriginMtlsConfig")?
            .map(|m| {
                Ok::<_, String>(OriginMtlsConfig {
                    client_certificate_arn: req_str(m, "ClientCertificateArn")?,
                })
            })
            .transpose()
            .map_err(|e| format!("OriginMtlsConfig: {e}"))?,
    })
}

fn cfn_origin_groups(og: &Value) -> CfnResult<OriginGroups> {
    let origin_group = obj_items(og, "Items")?
        .map(|items| {
            items
                .into_iter()
                .enumerate()
                .map(|(i, g)| cfn_origin_group(g).map_err(|e| format!("Items[{i}]: {e}")))
                .collect::<CfnResult<Vec<_>>>()
        })
        .transpose()?
        .unwrap_or_default();
    let (quantity, items) = counted(origin_group, |origin_group| OriginGroupItems {
        origin_group,
    });
    Ok(OriginGroups { quantity, items })
}

fn cfn_origin_group(g: &Value) -> CfnResult<OriginGroup> {
    let status_codes = opt_obj(
        opt_obj(g, "FailoverCriteria")?.ok_or("FailoverCriteria is required")?,
        "StatusCodes",
    )
    .map_err(|e| format!("FailoverCriteria.{e}"))?
    .ok_or("FailoverCriteria.StatusCodes is required")?;
    let status_code = obj_list(status_codes, "Items")?
        .ok_or("FailoverCriteria.StatusCodes.Items is required")?
        .iter()
        .map(|v| {
            cfn_integer(v)
                .and_then(|n| i32::try_from(n).map_err(|_| "is out of range".to_string()))
                .map_err(|e| format!("FailoverCriteria.StatusCodes.Items {e}"))
        })
        .collect::<CfnResult<Vec<_>>>()?;
    let origin_group_member = obj_items(
        opt_obj(g, "Members")?.ok_or("Members is required")?,
        "Items",
    )?
    .ok_or("Members.Items is required")?
    .into_iter()
    .map(|m| {
        Ok(OriginGroupMember {
            origin_id: req_str(m, "OriginId")?,
        })
    })
    .collect::<CfnResult<Vec<_>>>()
    .map_err(|e| format!("Members.Items: {e}"))?;
    Ok(OriginGroup {
        id: req_str(g, "Id")?,
        failover_criteria: OriginGroupFailoverCriteria {
            status_codes: StatusCodes {
                quantity: len(&status_code),
                items: StatusCodeItems { status_code },
            },
        },
        members: OriginGroupMembers {
            quantity: len(&origin_group_member),
            items: OriginGroupMemberItems {
                origin_group_member,
            },
        },
        selection_criteria: opt_str(g, "SelectionCriteria")?,
    })
}

/// `CacheBehavior` is `DefaultCacheBehavior` plus `PathPattern`.
fn cfn_cache_behavior(b: &Value) -> CfnResult<CacheBehavior> {
    let path_pattern = req_str(b, "PathPattern")?;
    Ok(CacheBehavior::from_default(
        path_pattern,
        cfn_default_cache_behavior(b)?,
    ))
}

fn cfn_default_cache_behavior(b: &Value) -> CfnResult<DefaultCacheBehavior> {
    // CloudFormation carries `CachedMethods` beside `AllowedMethods`; the API
    // nests it inside. Both default to GET/HEAD in CloudFormation, so a
    // template that only narrows `CachedMethods` still gets the default
    // allowed set to hang it on.
    let allowed = str_list(b, "AllowedMethods")?;
    let cached = str_list(b, "CachedMethods")?;
    let allowed_methods = if allowed.is_none() && cached.is_none() {
        None
    } else {
        let method = allowed.unwrap_or_else(|| vec!["HEAD".to_string(), "GET".to_string()]);
        Some(AllowedMethods {
            quantity: len(&method),
            items: MethodList { method },
            cached_methods: cached.map(|method| CachedMethods {
                quantity: len(&method),
                items: MethodList { method },
            }),
        })
    };

    // Every member of an association is optional in CloudFormation's schema;
    // an incomplete one is left for CloudFront's own validation to reject.
    let lambda_function_associations = obj_items(b, "LambdaFunctionAssociations")?
        .map(|items| {
            let lambda_function_association = items
                .into_iter()
                .map(|a| {
                    Ok(LambdaFunctionAssociation {
                        lambda_function_arn: opt_str(a, "LambdaFunctionARN")?.unwrap_or_default(),
                        event_type: opt_str(a, "EventType")?.unwrap_or_default(),
                        include_body: opt_bool(a, "IncludeBody")?,
                    })
                })
                .collect::<CfnResult<Vec<_>>>()
                .map_err(|e| format!("LambdaFunctionAssociations: {e}"))?;
            let (quantity, items) =
                counted(lambda_function_association, |lambda_function_association| {
                    LambdaFunctionAssociationItems {
                        lambda_function_association,
                    }
                });
            Ok::<_, String>(LambdaFunctionAssociations { quantity, items })
        })
        .transpose()?;

    let function_associations = obj_items(b, "FunctionAssociations")?
        .map(|items| {
            let function_association = items
                .into_iter()
                .map(|a| {
                    Ok(FunctionAssociation {
                        function_arn: opt_str(a, "FunctionARN")?.unwrap_or_default(),
                        event_type: opt_str(a, "EventType")?.unwrap_or_default(),
                    })
                })
                .collect::<CfnResult<Vec<_>>>()
                .map_err(|e| format!("FunctionAssociations: {e}"))?;
            let (quantity, items) = counted(function_association, |function_association| {
                FunctionAssociationItems {
                    function_association,
                }
            });
            Ok::<_, String>(FunctionAssociations { quantity, items })
        })
        .transpose()?;

    Ok(DefaultCacheBehavior {
        target_origin_id: req_str(b, "TargetOriginId")?,
        trusted_signers: str_list(b, "TrustedSigners")?.map(|ids| {
            let enabled = !ids.is_empty();
            let (quantity, items) = counted(ids, |aws_account_number| AwsAccountNumberList {
                aws_account_number,
            });
            TrustedSigners {
                enabled,
                quantity,
                items,
            }
        }),
        trusted_key_groups: str_list(b, "TrustedKeyGroups")?.map(|ids| {
            let enabled = !ids.is_empty();
            let (quantity, items) = counted(ids, |key_group| TrustedKeyGroupIdList { key_group });
            TrustedKeyGroups {
                enabled,
                quantity,
                items,
            }
        }),
        viewer_protocol_policy: req_str(b, "ViewerProtocolPolicy")?,
        allowed_methods,
        smooth_streaming: opt_bool(b, "SmoothStreaming")?,
        compress: opt_bool(b, "Compress")?,
        lambda_function_associations,
        function_associations,
        field_level_encryption_id: opt_str(b, "FieldLevelEncryptionId")?,
        realtime_log_config_arn: opt_str(b, "RealtimeLogConfigArn")?,
        cache_policy_id: opt_str(b, "CachePolicyId")?,
        origin_request_policy_id: opt_str(b, "OriginRequestPolicyId")?,
        response_headers_policy_id: opt_str(b, "ResponseHeadersPolicyId")?,
        grpc_config: opt_obj(b, "GrpcConfig")?
            .map(|g| {
                Ok::<_, String>(GrpcConfig {
                    enabled: opt_bool(g, "Enabled")?.ok_or("Enabled is required")?,
                })
            })
            .transpose()
            .map_err(|e| format!("GrpcConfig: {e}"))?,
        forwarded_values: opt_obj(b, "ForwardedValues")?
            .map(cfn_forwarded_values)
            .transpose()
            .map_err(|e| format!("ForwardedValues: {e}"))?,
        min_ttl: opt_seconds(b, "MinTTL")?,
        default_ttl: opt_seconds(b, "DefaultTTL")?,
        max_ttl: opt_seconds(b, "MaxTTL")?,
    })
}

fn cfn_forwarded_values(fv: &Value) -> CfnResult<ForwardedValues> {
    let cookies = match opt_obj(fv, "Cookies")? {
        Some(c) => CookiePreference {
            forward: req_str(c, "Forward").map_err(|e| format!("Cookies: {e}"))?,
            whitelisted_names: str_list(c, "WhitelistedNames")?.map(|name| {
                let (quantity, items) = counted(name, |name| CookieNameList { name });
                CookieNames { quantity, items }
            }),
        },
        // Optional in CloudFormation (defaulting to `Forward: none`), required
        // by the API.
        None => CookiePreference {
            forward: "none".to_string(),
            whitelisted_names: None,
        },
    };
    Ok(ForwardedValues {
        query_string: opt_bool(fv, "QueryString")?.ok_or("QueryString is required")?,
        cookies,
        headers: str_list(fv, "Headers")?.map(|name| {
            let (quantity, items) = counted(name, |name| HeaderList { name });
            Headers { quantity, items }
        }),
        query_string_cache_keys: str_list(fv, "QueryStringCacheKeys")?.map(|name| {
            let (quantity, items) = counted(name, |name| QueryStringCacheKeyList { name });
            QueryStringCacheKeys { quantity, items }
        }),
    })
}

fn cfn_custom_error_response(r: &Value) -> CfnResult<CustomErrorResponse> {
    Ok(CustomErrorResponse {
        error_code: opt_i32(r, "ErrorCode")?.ok_or("ErrorCode is required")?,
        response_page_path: opt_str(r, "ResponsePagePath")?,
        // An Integer in CloudFormation, a string in the API.
        response_code: opt_i32(r, "ResponseCode")?.map(|n| n.to_string()),
        error_caching_min_ttl: opt_seconds(r, "ErrorCachingMinTTL")?,
    })
}

fn cfn_logging(log: &Value) -> CfnResult<LoggingConfig> {
    // CloudFormation has no `Enabled`: the presence of the block turns
    // logging on.
    Ok(LoggingConfig {
        enabled: true,
        include_cookies: opt_bool(log, "IncludeCookies")?.unwrap_or(false),
        bucket: opt_str(log, "Bucket")?.unwrap_or_default(),
        prefix: opt_str(log, "Prefix")?.unwrap_or_default(),
    })
}

fn cfn_viewer_certificate(vc: &Value) -> CfnResult<ViewerCertificate> {
    Ok(ViewerCertificate {
        cloud_front_default_certificate: opt_bool(vc, "CloudFrontDefaultCertificate")?,
        iam_certificate_id: opt_str(vc, "IamCertificateId")?,
        acm_certificate_arn: opt_str(vc, "AcmCertificateArn")?,
        ssl_support_method: opt_str(vc, "SslSupportMethod")?,
        minimum_protocol_version: opt_str(vc, "MinimumProtocolVersion")?,
        certificate: None,
        certificate_source: None,
    })
}

fn cfn_viewer_mtls_config(m: &Value) -> CfnResult<ViewerMtlsConfig> {
    Ok(ViewerMtlsConfig {
        mode: opt_str(m, "Mode")?,
        trust_store_config: opt_obj(m, "TrustStoreConfig")?
            .map(|t| {
                Ok::<_, String>(TrustStoreConfig {
                    trust_store_id: req_str(t, "TrustStoreId")?,
                    advertise_trust_store_ca_names: opt_bool(t, "AdvertiseTrustStoreCaNames")?,
                    ignore_certificate_expiry: opt_bool(t, "IgnoreCertificateExpiry")?,
                })
            })
            .transpose()
            .map_err(|e| format!("TrustStoreConfig: {e}"))?,
    })
}

fn cfn_restrictions(r: &Value) -> CfnResult<Restrictions> {
    let geo = opt_obj(r, "GeoRestriction")?.ok_or("GeoRestriction is required")?;
    let location = str_list(geo, "Locations")?.unwrap_or_default();
    let restriction_type =
        req_str(geo, "RestrictionType").map_err(|e| format!("GeoRestriction: {e}"))?;
    let (quantity, items) = counted(location, |location| LocationList { location });
    Ok(Restrictions {
        geo_restriction: GeoRestriction {
            restriction_type,
            quantity,
            items,
        },
    })
}

fn cfn_tenant_config(t: &Value) -> CfnResult<TenantConfig> {
    let parameter_definitions = obj_items(t, "ParameterDefinitions")?
        .map(|items| {
            let parameter_definition = items
                .into_iter()
                .map(cfn_parameter_definition)
                .collect::<CfnResult<Vec<_>>>()
                .map_err(|e| format!("ParameterDefinitions: {e}"))?;
            Ok::<_, String>(ParameterDefinitions {
                parameter_definition,
            })
        })
        .transpose()?;
    Ok(TenantConfig {
        parameter_definitions,
    })
}

fn cfn_parameter_definition(p: &Value) -> CfnResult<ParameterDefinition> {
    let definition = opt_obj(p, "Definition")?.ok_or("Definition is required")?;
    Ok(ParameterDefinition {
        name: req_str(p, "Name")?,
        definition: ParameterDefinitionSchema {
            string_schema: opt_obj(definition, "StringSchema")?
                .map(|s| {
                    Ok::<_, String>(StringSchemaConfig {
                        required: opt_bool(s, "Required")?.ok_or("Required is required")?,
                        comment: opt_str(s, "Comment")?,
                        default_value: opt_str(s, "DefaultValue")?,
                    })
                })
                .transpose()
                .map_err(|e| format!("Definition.StringSchema: {e}"))?,
        },
    })
}

// --- Quantity/Items envelope -----------------------------------------------

fn len<T>(list: &[T]) -> i32 {
    i32::try_from(list.len()).unwrap_or(i32::MAX)
}

/// The `Quantity` and optional `Items` of a CloudFront list. CloudFront leaves
/// `Items` out of an empty optional list, so an empty one maps to `None`.
fn counted<T, I>(list: Vec<T>, items: impl FnOnce(Vec<T>) -> I) -> (i32, Option<I>) {
    let quantity = len(&list);
    (quantity, (!list.is_empty()).then(|| items(list)))
}

// --- CloudFormation scalar readers -----------------------------------------

/// A present, non-null member.
fn field<'a>(obj: &'a Value, key: &str) -> Option<&'a Value> {
    obj.get(key).filter(|v| !v.is_null())
}

/// An object-typed member; anything else fails rather than being read as an
/// empty object.
fn opt_obj<'a>(obj: &'a Value, key: &str) -> CfnResult<Option<&'a Value>> {
    match field(obj, key) {
        None => Ok(None),
        Some(v) if v.is_object() => Ok(Some(v)),
        Some(_) => Err(format!("{key} must be an object")),
    }
}

/// A String-typed value. CloudFormation stringifies a number or boolean given
/// for a String property.
fn scalar_string(v: &Value) -> Option<String> {
    match v {
        Value::String(s) => Some(s.clone()),
        Value::Number(n) => Some(n.to_string()),
        Value::Bool(b) => Some(b.to_string()),
        _ => None,
    }
}

fn opt_str(obj: &Value, key: &str) -> CfnResult<Option<String>> {
    field(obj, key)
        .map(|v| scalar_string(v).ok_or_else(|| format!("{key} must be a string")))
        .transpose()
}

fn req_str(obj: &Value, key: &str) -> CfnResult<String> {
    opt_str(obj, key)?.ok_or_else(|| format!("{key} is required"))
}

/// A CloudFormation Integer. Templates carry these as JSON numbers, but YAML
/// templates and resolved intrinsics quote them. A fractional or out-of-range
/// value is rejected rather than truncated or saturated.
fn cfn_integer(v: &Value) -> CfnResult<i64> {
    match v {
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(i)
            } else if n.is_u64() {
                Err("is out of range".to_string())
            } else {
                Err("must be an integer".to_string())
            }
        }
        Value::String(s) => s.trim().parse::<i64>().map_err(|e| match e.kind() {
            std::num::IntErrorKind::PosOverflow | std::num::IntErrorKind::NegOverflow => {
                "is out of range".to_string()
            }
            _ => "must be an integer".to_string(),
        }),
        _ => Err("must be an integer".to_string()),
    }
}

/// A CloudFormation Double carrying whole seconds (the TTLs, which the API
/// takes as a Long). An integral double such as `86400.0` is accepted; a
/// fractional or out-of-range one is rejected.
fn cfn_seconds(v: &Value) -> CfnResult<i64> {
    if let Ok(i) = cfn_integer(v) {
        return Ok(i);
    }
    let f = match v {
        Value::Number(n) => n.as_f64(),
        Value::String(s) => s.trim().parse::<f64>().ok(),
        _ => None,
    }
    .ok_or("must be a number")?;
    // i64::MAX is not exactly representable; 2^63 is the first double past it.
    const LIMIT: f64 = 9_223_372_036_854_775_808.0;
    if f.fract() != 0.0 || !f.is_finite() {
        Err("must be a whole number of seconds".to_string())
    } else if !(-LIMIT..LIMIT).contains(&f) {
        Err("is out of range".to_string())
    } else {
        Ok(f as i64)
    }
}

fn opt_i64(obj: &Value, key: &str) -> CfnResult<Option<i64>> {
    field(obj, key)
        .map(|v| cfn_integer(v).map_err(|e| format!("{key} {e}")))
        .transpose()
}

fn opt_i32(obj: &Value, key: &str) -> CfnResult<Option<i32>> {
    opt_i64(obj, key)?
        .map(|n| i32::try_from(n).map_err(|_| format!("{key} is out of range")))
        .transpose()
}

fn opt_seconds(obj: &Value, key: &str) -> CfnResult<Option<i64>> {
    field(obj, key)
        .map(|v| cfn_seconds(v).map_err(|e| format!("{key} {e}")))
        .transpose()
}

/// A CloudFormation boolean, which a parameter `Ref` hands over as a string.
fn opt_bool(obj: &Value, key: &str) -> CfnResult<Option<bool>> {
    match field(obj, key) {
        None => Ok(None),
        Some(Value::Bool(b)) => Ok(Some(*b)),
        Some(Value::String(s)) if s.eq_ignore_ascii_case("true") => Ok(Some(true)),
        Some(Value::String(s)) if s.eq_ignore_ascii_case("false") => Ok(Some(false)),
        Some(_) => Err(format!("{key} must be a boolean")),
    }
}

fn obj_list<'a>(obj: &'a Value, key: &str) -> CfnResult<Option<&'a Vec<Value>>> {
    match field(obj, key) {
        None => Ok(None),
        Some(Value::Array(arr)) => Ok(Some(arr)),
        Some(_) => Err(format!("{key} must be a list")),
    }
}

/// A list of objects.
fn obj_items<'a>(obj: &'a Value, key: &str) -> CfnResult<Option<Vec<&'a Value>>> {
    obj_list(obj, key)?
        .map(|arr| {
            arr.iter()
                .enumerate()
                .map(|(i, v)| {
                    if v.is_object() {
                        Ok(v)
                    } else {
                        Err(format!("{key}[{i}] must be an object"))
                    }
                })
                .collect()
        })
        .transpose()
}

/// A list of strings, stringifying scalar entries as CloudFormation does.
fn str_list(obj: &Value, key: &str) -> CfnResult<Option<Vec<String>>> {
    obj_list(obj, key)?
        .map(|arr| {
            arr.iter()
                .map(|v| scalar_string(v).ok_or_else(|| format!("{key} must be a list of strings")))
                .collect()
        })
        .transpose()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn base() -> Value {
        json!({
            "Enabled": true,
            "Origins": [{"Id": "o1", "DomainName": "origin.example.com",
                         "CustomOriginConfig": {"OriginProtocolPolicy": "https-only"}}],
            "DefaultCacheBehavior": {"TargetOriginId": "o1", "ViewerProtocolPolicy": "allow-all",
                                     "ForwardedValues": {"QueryString": false}}
        })
    }

    /// The base with members replaced. A default behavior given here gets
    /// `ForwardedValues` unless it names a cache policy, since CloudFront requires
    /// one or the other and most tests are about something else.
    fn with(extra: Value) -> Value {
        let mut cfg = base();
        for (k, v) in extra.as_object().unwrap() {
            cfg[k] = v.clone();
        }
        let dcb = cfg["DefaultCacheBehavior"].as_object_mut().unwrap();
        if !dcb.contains_key("CachePolicyId") && !dcb.contains_key("ForwardedValues") {
            dcb.insert("ForwardedValues".into(), json!({"QueryString": false}));
        }
        cfg
    }

    fn translate(cfg: &Value) -> DistributionConfig {
        cfn_distribution_config(cfg, "ref".to_string()).expect("translates")
    }

    /// The `CustomErrorResponses` block CDK synthesizes for a SPA
    /// distribution. CloudFormation types `ResponseCode` and
    /// `ErrorCachingMinTTL` as Integer; the API carries `ResponseCode` as a
    /// string.
    #[test]
    fn custom_error_responses_survive_translation() {
        let config = translate(&with(json!({
            "CustomErrorResponses": [
                {"ErrorCode": 403, "ResponseCode": 200, "ResponsePagePath": "/index.html", "ErrorCachingMinTTL": 300},
                {"ErrorCode": 404, "ResponseCode": 200, "ResponsePagePath": "/index.html", "ErrorCachingMinTTL": 300}
            ]
        })));
        let rules = config.custom_error_responses.expect("translated");
        assert_eq!(rules.quantity, 2);
        let items = rules.items.expect("items").custom_error_response;
        assert_eq!(
            items.iter().map(|r| r.error_code).collect::<Vec<_>>(),
            vec![403, 404]
        );
        for rule in &items {
            assert_eq!(rule.response_code.as_deref(), Some("200"));
            assert_eq!(rule.response_page_path.as_deref(), Some("/index.html"));
            assert_eq!(rule.error_caching_min_ttl, Some(300));
        }
    }

    #[test]
    fn a_custom_origin_without_ssl_protocols_gets_the_schema_default() {
        let config = translate(&with(json!({"Origins": [{
            "Id": "o1", "DomainName": "o.example.com",
            "CustomOriginConfig": {"OriginProtocolPolicy": "https-only"}
        }]})));
        let origin = &config.origins.items.unwrap().origin[0];
        let ssl = origin
            .custom_origin_config
            .as_ref()
            .unwrap()
            .origin_ssl_protocols
            .as_ref()
            .unwrap();
        assert_eq!(ssl.quantity, 2);
        assert_eq!(ssl.items.ssl_protocol, ["SSLv3", "TLSv1"]);
    }

    #[test]
    fn members_the_template_omits_take_the_cloudfront_defaults() {
        let config = translate(&with(json!({})));
        assert_eq!(config.aliases.as_ref().unwrap().quantity, 0);
        assert_eq!(config.origin_groups.as_ref().unwrap().quantity, 0);
        assert_eq!(config.cache_behaviors.as_ref().unwrap().quantity, 0);
        assert_eq!(config.custom_error_responses.as_ref().unwrap().quantity, 0);
        assert_eq!(config.price_class.as_deref(), Some("PriceClass_All"));
        let geo = config.restrictions.as_ref().unwrap();
        assert_eq!(geo.geo_restriction.restriction_type, "none");
        let vc = config.viewer_certificate.as_ref().unwrap();
        assert_eq!(vc.cloud_front_default_certificate, Some(true));
        assert!(config.default_cache_behavior.trusted_signers.is_some());
        // CloudFormation's documented default, not the API's http2.
        assert_eq!(config.http_version.as_deref(), Some("http1.1"));
    }

    #[test]
    fn stringified_numbers_and_booleans_are_accepted() {
        // YAML templates and parameter `Ref`s hand these over as strings.
        let config = translate(&with(json!({
            "Enabled": "false",
            "IPV6Enabled": "true",
            "CustomErrorResponses": [{"ErrorCode": "404", "ResponseCode": "200"}],
            "DefaultCacheBehavior": {"TargetOriginId": "o1", "ViewerProtocolPolicy": "allow-all",
                                     "MinTTL": "0", "DefaultTTL": 86400.0, "MaxTTL": "31536000",
                                     "Compress": "true"}
        })));
        assert!(!config.enabled);
        assert_eq!(config.is_ipv6_enabled, Some(true));
        let rule = &config
            .custom_error_responses
            .unwrap()
            .items
            .unwrap()
            .custom_error_response[0];
        assert_eq!(rule.error_code, 404);
        assert_eq!(rule.response_code.as_deref(), Some("200"));
        let dcb = &config.default_cache_behavior;
        assert_eq!(
            (dcb.min_ttl, dcb.default_ttl, dcb.max_ttl),
            (Some(0), Some(86400), Some(31_536_000))
        );
        assert_eq!(dcb.compress, Some(true));
    }

    #[test]
    fn a_rule_without_an_error_code_fails_the_resource() {
        // CloudFormation's schema validation rejects this; silently dropping
        // the rule would leave the distribution without the fallback.
        let err = cfn_distribution_config(
            &with(json!({"CustomErrorResponses": [{"ResponsePagePath": "/index.html"}]})),
            "ref".to_string(),
        )
        .unwrap_err();
        assert!(err.contains("CustomErrorResponses[0]"), "{err}");
        assert!(err.contains("ErrorCode is required"), "{err}");
    }

    #[test]
    fn cache_behaviors_translate_flat_lists_into_the_wire_shape() {
        let config = translate(&with(json!({
            "CacheBehaviors": [{
                "PathPattern": "/api/*",
                "TargetOriginId": "o1",
                "ViewerProtocolPolicy": "https-only",
                "AllowedMethods": ["GET", "HEAD", "OPTIONS", "PUT", "POST", "PATCH", "DELETE"],
                "CachedMethods": ["GET", "HEAD"],
                "TrustedKeyGroups": ["kg-1"],
                "FunctionAssociations": [{"EventType": "viewer-request", "FunctionARN": "arn:aws:cloudfront::123456789012:function/f"}],
                "LambdaFunctionAssociations": [{"EventType": "origin-request", "LambdaFunctionARN": "arn:aws:lambda:us-east-1:123456789012:function:f:1", "IncludeBody": true}],
                "ForwardedValues": {"QueryString": true, "Cookies": {"Forward": "whitelist", "WhitelistedNames": ["session"]},
                                    "Headers": ["Authorization"], "QueryStringCacheKeys": ["page"]},
                "MinTTL": 0, "DefaultTTL": 0, "MaxTTL": 0
            }]
        })));
        let behaviors = config.cache_behaviors.expect("translated");
        assert_eq!(behaviors.quantity, 1, "the behavior must not be dropped");
        let b = &behaviors.items.unwrap().cache_behavior[0];
        assert_eq!(b.path_pattern, "/api/*");
        let allowed = b.allowed_methods.as_ref().unwrap();
        assert_eq!(allowed.quantity, 7);
        let cached = allowed.cached_methods.as_ref().unwrap();
        assert_eq!(cached.items.method, vec!["GET", "HEAD"]);
        let kg = b.trusted_key_groups.as_ref().unwrap();
        assert!(kg.enabled);
        assert_eq!(kg.items.as_ref().unwrap().key_group, vec!["kg-1"]);
        let fa = b.function_associations.as_ref().unwrap();
        assert_eq!(fa.quantity, 1);
        assert_eq!(
            fa.items.as_ref().unwrap().function_association[0].event_type,
            "viewer-request"
        );
        let la = b.lambda_function_associations.as_ref().unwrap();
        assert_eq!(
            la.items.as_ref().unwrap().lambda_function_association[0].include_body,
            Some(true)
        );
        let fv = b.forwarded_values.as_ref().unwrap();
        assert!(fv.query_string);
        assert_eq!(fv.cookies.forward, "whitelist");
        assert_eq!(fv.cookies.whitelisted_names.as_ref().unwrap().quantity, 1);
        assert_eq!(fv.headers.as_ref().unwrap().quantity, 1);
        assert_eq!(fv.query_string_cache_keys.as_ref().unwrap().quantity, 1);
        assert_eq!(
            (b.min_ttl, b.default_ttl, b.max_ttl),
            (Some(0), Some(0), Some(0))
        );
    }

    #[test]
    fn a_malformed_cache_behavior_fails_the_resource_instead_of_vanishing() {
        let err = cfn_distribution_config(
            &with(json!({"CacheBehaviors": [{"TargetOriginId": "o1", "ViewerProtocolPolicy": "allow-all"}]})),
            "ref".to_string(),
        )
        .unwrap_err();
        assert!(
            err.contains("CacheBehaviors[0]: PathPattern is required"),
            "{err}"
        );
    }

    #[test]
    fn cached_methods_alone_hang_off_the_default_allowed_set() {
        let config = translate(&with(json!({
            "DefaultCacheBehavior": {"TargetOriginId": "o1", "ViewerProtocolPolicy": "allow-all",
                                     "CachedMethods": ["GET", "HEAD"]}
        })));
        let allowed = config.default_cache_behavior.allowed_methods.unwrap();
        assert_eq!(allowed.items.method, vec!["HEAD", "GET"]);
        assert_eq!(allowed.cached_methods.unwrap().quantity, 2);
    }

    #[test]
    fn origins_translate_cfn_member_names_and_default_ports() {
        let config = translate(&with(json!({
            "DefaultCacheBehavior": {"TargetOriginId": "web", "ViewerProtocolPolicy": "allow-all"},
            "Origins": [
                {"Id": "web", "DomainName": "web.example.com",
                 "OriginCustomHeaders": [{"HeaderName": "X-Secret", "HeaderValue": "s"}],
                 "OriginShield": {"Enabled": true, "OriginShieldRegion": "us-east-1"},
                 "ConnectionAttempts": "3",
                 "CustomOriginConfig": {"OriginProtocolPolicy": "https-only",
                                        "OriginSSLProtocols": ["TLSv1.2"],
                                        "OriginReadTimeout": 30}},
                {"Id": "bucket", "DomainName": "b.s3.us-east-1.amazonaws.com",
                 "OriginAccessControlId": "E123",
                 "S3OriginConfig": {}}
            ]
        })));
        let origins = config.origins.items.unwrap().origin;
        assert_eq!(origins.len(), 2);
        let web = &origins[0];
        let custom = web.custom_origin_config.as_ref().unwrap();
        assert_eq!((custom.http_port, custom.https_port), (80, 443));
        assert_eq!(
            custom
                .origin_ssl_protocols
                .as_ref()
                .unwrap()
                .items
                .ssl_protocol,
            vec!["TLSv1.2"]
        );
        assert_eq!(custom.origin_read_timeout, Some(30));
        let headers = web.custom_headers.as_ref().unwrap();
        assert_eq!(headers.quantity, 1);
        assert_eq!(
            headers.items.as_ref().unwrap().origin_custom_header[0].header_name,
            "X-Secret"
        );
        assert!(web.origin_shield.as_ref().unwrap().enabled);
        assert_eq!(web.connection_attempts, Some(3));
        let bucket = &origins[1];
        assert_eq!(
            bucket
                .s3_origin_config
                .as_ref()
                .unwrap()
                .origin_access_identity,
            ""
        );
        assert_eq!(bucket.origin_access_control_id.as_deref(), Some("E123"));
    }

    #[test]
    fn viewer_certificate_reads_the_cfn_member_names() {
        let config = translate(&with(json!({
            "ViewerCertificate": {
                "AcmCertificateArn": "arn:aws:acm:us-east-1:123456789012:certificate/abc",
                "SslSupportMethod": "sni-only",
                "MinimumProtocolVersion": "TLSv1.2_2021"
            }
        })));
        let vc = config.viewer_certificate.unwrap();
        assert_eq!(
            vc.acm_certificate_arn.as_deref(),
            Some("arn:aws:acm:us-east-1:123456789012:certificate/abc")
        );
        assert_eq!(vc.ssl_support_method.as_deref(), Some("sni-only"));
        assert_eq!(vc.minimum_protocol_version.as_deref(), Some("TLSv1.2_2021"));
    }

    #[test]
    fn origin_groups_and_scalar_extras_are_kept() {
        let config = translate(&with(json!({
            "OriginGroups": {"Quantity": 1, "Items": [{
                "Id": "group",
                "FailoverCriteria": {"StatusCodes": {"Quantity": 2, "Items": [500, "502"]}},
                "Members": {"Quantity": 1, "Items": [{"OriginId": "o1"}]}
            }]},
            "ContinuousDeploymentPolicyId": "cdp-1",
            "Staging": false,
            "CNAMEs": ["legacy.example.com"]
        })));
        let groups = config.origin_groups.unwrap();
        assert_eq!(groups.quantity, 1);
        let group = &groups.items.unwrap().origin_group[0];
        assert_eq!(
            group.failover_criteria.status_codes.items.status_code,
            vec![500, 502]
        );
        assert_eq!(group.members.items.origin_group_member[0].origin_id, "o1");
        assert_eq!(
            config.continuous_deployment_policy_id.as_deref(),
            Some("cdp-1")
        );
        assert_eq!(config.staging, Some(false));
        assert_eq!(
            config.aliases.unwrap().items.unwrap().cname,
            vec!["legacy.example.com"]
        );
    }

    fn translate_err(cfg: &Value) -> String {
        cfn_distribution_config(cfg, "ref".to_string()).unwrap_err()
    }

    #[test]
    fn scalars_given_for_string_members_are_stringified() {
        let config = translate(&with(json!({
            "Comment": 42,
            "Aliases": ["a.example.com", 7],
            "DefaultCacheBehavior": {"TargetOriginId": "o1", "ViewerProtocolPolicy": "allow-all",
                                     "ForwardedValues": {"QueryString": false, "Headers": [true]}}
        })));
        assert_eq!(config.comment, "42");
        assert_eq!(
            config.aliases.unwrap().items.unwrap().cname,
            vec!["a.example.com", "7"]
        );
        let fv = config.default_cache_behavior.forwarded_values.unwrap();
        assert_eq!(fv.headers.unwrap().items.unwrap().name, vec!["true"]);
    }

    #[test]
    fn object_members_given_a_scalar_fail_the_resource() {
        let err = translate_err(&with(json!({"Logging": "s3://logs"})));
        assert!(err.contains("Logging must be an object"), "{err}");
        let err = translate_err(&with(json!({"CustomErrorResponses": ["404"]})));
        assert!(
            err.contains("CustomErrorResponses[0] must be an object"),
            "{err}"
        );
    }

    #[test]
    fn schema_required_members_are_not_defaulted() {
        let mut no_enabled = base();
        no_enabled.as_object_mut().unwrap().remove("Enabled");
        assert!(translate_err(&no_enabled).contains("Enabled is required"));

        let err = translate_err(&with(json!({
            "DefaultCacheBehavior": {"TargetOriginId": "o1", "ViewerProtocolPolicy": "allow-all",
                                     "GrpcConfig": {}}
        })));
        assert!(err.contains("GrpcConfig: Enabled is required"), "{err}");

        let err = translate_err(&with(json!({
            "TenantConfig": {"ParameterDefinitions": [{"Name": "p"}]}
        })));
        assert!(err.contains("Definition is required"), "{err}");

        let err = translate_err(&with(json!({
            "TenantConfig": {"ParameterDefinitions": [{"Name": "p", "Definition": {"StringSchema": {}}}]}
        })));
        assert!(err.contains("Required is required"), "{err}");
    }

    #[test]
    fn incomplete_associations_fail_with_cloudfronts_error_not_a_schema_error() {
        let err = translate_err(&with(json!({
            "DefaultCacheBehavior": {"TargetOriginId": "o1", "ViewerProtocolPolicy": "allow-all",
                                     "FunctionAssociations": [{"EventType": "viewer-request"}]}
        })));
        assert!(err.contains("A function association needs"), "{err}");
        let err = translate_err(&with(json!({
            "DefaultCacheBehavior": {"TargetOriginId": "o1", "ViewerProtocolPolicy": "allow-all",
                                     "LambdaFunctionAssociations": [{"LambdaFunctionARN": "arn:l"}]}
        })));
        assert!(err.contains("A Lambda function association needs"), "{err}");
    }

    #[test]
    fn integers_are_neither_truncated_nor_saturated() {
        let err = translate_err(&with(
            json!({"CustomErrorResponses": [{"ErrorCode": 404.5}]}),
        ));
        assert!(err.contains("ErrorCode must be an integer"), "{err}");
        let err = translate_err(&with(json!({
            "Origins": [{"Id": "o1", "DomainName": "o.example.com", "ConnectionAttempts": u64::MAX}]
        })));
        assert!(err.contains("ConnectionAttempts is out of range"), "{err}");
        let err = translate_err(&with(json!({
            "Origins": [{"Id": "o1", "DomainName": "o.example.com", "ConnectionAttempts": "99999999999999999999"}]
        })));
        assert!(err.contains("ConnectionAttempts is out of range"), "{err}");
    }

    #[test]
    fn ttls_accept_integral_doubles_only() {
        let config = translate(&with(json!({
            "DefaultCacheBehavior": {"TargetOriginId": "o1", "ViewerProtocolPolicy": "allow-all",
                                     "DefaultTTL": 86400.0, "MaxTTL": "90000.0"},
            "CustomErrorResponses": [{"ErrorCode": 404, "ErrorCachingMinTTL": 10.0}]
        })));
        assert_eq!(config.default_cache_behavior.default_ttl, Some(86400));
        assert_eq!(config.default_cache_behavior.max_ttl, Some(90000));
        let err = translate_err(&with(json!({
            "DefaultCacheBehavior": {"TargetOriginId": "o1", "ViewerProtocolPolicy": "allow-all",
                                     "MinTTL": 1.5}
        })));
        assert!(
            err.contains("MinTTL must be a whole number of seconds"),
            "{err}"
        );
        let err = translate_err(&with(json!({
            "DefaultCacheBehavior": {"TargetOriginId": "o1", "ViewerProtocolPolicy": "allow-all",
                                     "MaxTTL": 1e30}
        })));
        assert!(err.contains("MaxTTL is out of range"), "{err}");
    }

    /// Recorded from real CloudFormation on 2026-11-09: omitted TTLs take their
    /// schema defaults independently, unlike an API request's derivation.
    #[test]
    fn omitted_ttls_take_independent_schema_defaults() {
        let config = translate(&with(json!({
            "DefaultCacheBehavior": {"TargetOriginId": "o1", "ViewerProtocolPolicy": "allow-all",
                                     "ForwardedValues": {"QueryString": false}, "DefaultTTL": 600},
            // Inferred from the default behavior.
            "CacheBehaviors": [{"PathPattern": "/x", "TargetOriginId": "o1",
                                "ViewerProtocolPolicy": "allow-all",
                                "ForwardedValues": {"QueryString": false}, "DefaultTTL": 600}]
        })));
        let dcb = &config.default_cache_behavior;
        assert_eq!(
            (dcb.min_ttl, dcb.default_ttl, dcb.max_ttl),
            (Some(0), Some(600), Some(31_536_000))
        );
        let behaviors = config.cache_behaviors.unwrap().items.unwrap();
        let b = &behaviors.cache_behavior[0];
        assert_eq!(
            (b.min_ttl, b.default_ttl, b.max_ttl),
            (Some(0), Some(600), Some(31_536_000))
        );

        let order = "Invalid request provided: AWS::CloudFront::Distribution: MinTTL, MaxTTL and \
                     DefaultTTL should follow order MinTTL less than or equal to DefaultTTL less \
                     than or equal to MaxTTL";
        for ttl in [json!({"MaxTTL": 600}), json!({"MinTTL": 100_000})] {
            let mut cfg = base();
            for (k, v) in ttl.as_object().unwrap() {
                cfg["DefaultCacheBehavior"][k] = v.clone();
            }
            assert_eq!(translate_err(&cfg), order);
        }
        let mut negative = base();
        negative["DefaultCacheBehavior"]["MinTTL"] = json!(-5);
        assert_eq!(
            translate_err(&negative),
            "Invalid request provided: AWS::CloudFront::Distribution: The parameter MinTTL is \
             not within allowed range."
        );
    }

    #[test]
    fn a_template_on_a_cache_policy_gets_no_ttls() {
        let config = translate(&with(json!({
            "DefaultCacheBehavior": {"TargetOriginId": "o1", "ViewerProtocolPolicy": "allow-all",
                                     "CachePolicyId": "658327ea-f89d-4fab-a63d-7e88639e58f6"}
        })));
        let dcb = &config.default_cache_behavior;
        assert_eq!(
            (dcb.min_ttl, dcb.default_ttl, dcb.max_ttl),
            (None, None, None)
        );
    }

    /// Recorded from real CloudFormation on 2026-11-09: a template omitting
    /// `IPV6Enabled`, `HttpVersion`, `MinTTL`, `Cookies` and `OriginSSLProtocols`.
    #[test]
    fn omitted_members_take_the_values_cloudformation_recorded() {
        let config = translate(&base());
        assert_eq!(config.is_ipv6_enabled, Some(true));
        assert_eq!(config.staging, Some(false));
        assert_eq!(config.http_version.as_deref(), Some("http1.1"));
        let dcb = &config.default_cache_behavior;
        assert_eq!(
            (dcb.min_ttl, dcb.default_ttl, dcb.max_ttl),
            (Some(0), Some(86_400), Some(31_536_000))
        );
        assert_eq!(
            dcb.forwarded_values.as_ref().unwrap().cookies.forward,
            "none"
        );
        let origins = config.origins.items.unwrap();
        let custom = origins.origin[0].custom_origin_config.as_ref().unwrap();
        assert_eq!(
            custom
                .origin_ssl_protocols
                .as_ref()
                .unwrap()
                .items
                .ssl_protocol,
            ["SSLv3", "TLSv1"]
        );
    }

    /// The translated template meets the same refusals as an API create, under
    /// the handler's prefix. Recorded from real CloudFormation on 2026-11-09 for
    /// the rows named below; the others are inferred from those.
    #[test]
    fn a_template_the_api_would_refuse_fails_with_the_apis_error() {
        const PREFIX: &str = "Invalid request provided: AWS::CloudFront::Distribution: ";
        let behavior = |extra: Value| {
            let mut b = json!({"TargetOriginId": "o1", "ViewerProtocolPolicy": "allow-all"});
            for (k, v) in extra.as_object().unwrap() {
                b[k] = v.clone();
            }
            with(json!({"DefaultCacheBehavior": b}))
        };
        let err = translate_err(&behavior(json!({"DefaultTTL": 900, "MaxTTL": 800})));
        assert!(
            err.starts_with(&format!("{PREFIX}MinTTL, MaxTTL and DefaultTTL")),
            "{err}"
        );
        let err = translate_err(&behavior(json!({"CachePolicyId": "cp", "MinTTL": 0})));
        assert!(err.contains("The parameter MinTTL cannot be used"), "{err}");

        // Recorded: nothing at all.
        let mut neither = base();
        neither["DefaultCacheBehavior"]
            .as_object_mut()
            .unwrap()
            .remove("ForwardedValues");
        assert_eq!(
            translate_err(&neither),
            format!("{PREFIX}The parameter ForwardedValues is required.")
        );

        let tls = json!({"CloudFrontDefaultCertificate": true, "MinimumProtocolVersion": "TLSv1"});
        // Recorded: a multi-tenant template with no certificate is refused over the
        // protocol default CloudFormation applies.
        let mut tenant = behavior(json!({"CachePolicyId": "cp"}));
        tenant["ConnectionMode"] = json!("tenant-only");
        assert_eq!(
            translate_err(&tenant),
            format!(
                "{PREFIX}The parameter ViewerCertificate The specified distribution is configured \
                 for SNI and the minimum SSL protocol version is specified as SSLv3. You cannot \
                 specify both."
            )
        );
        // Inferred: naming a version gets past that, with no schema TTLs added.
        tenant["ViewerCertificate"] = tls.clone();
        let config = translate(&tenant);
        assert_eq!(config.default_cache_behavior.min_ttl, None);
        tenant["IPV6Enabled"] = json!(true);
        assert!(
            translate_err(&tenant).contains("IsIPV6Enabled field for a multi-tenant"),
            "IPV6Enabled"
        );
        neither["ConnectionMode"] = json!("tenant-only");
        neither["ViewerCertificate"] = tls;
        assert_eq!(
            translate_err(&neither),
            format!("{PREFIX}The CachePolicyId field is required for a multi-tenant distribution.")
        );
    }

    #[test]
    fn legacy_s3_origin_becomes_the_default_behaviors_origin() {
        let mut cfg = base();
        let obj = cfg.as_object_mut().unwrap();
        obj.remove("Origins");
        obj.insert(
            "S3Origin".into(),
            json!({"DNSName": "b.s3.amazonaws.com", "OriginAccessIdentity": "origin-access-identity/cloudfront/E1"}),
        );
        let config = translate(&cfg);
        let origins = config.origins.items.unwrap().origin;
        assert_eq!(origins.len(), 1);
        assert_eq!(origins[0].id, "o1");
        assert_eq!(origins[0].domain_name, "b.s3.amazonaws.com");
        assert_eq!(
            origins[0]
                .s3_origin_config
                .as_ref()
                .unwrap()
                .origin_access_identity,
            "origin-access-identity/cloudfront/E1"
        );
    }

    #[test]
    fn legacy_custom_origin_becomes_the_default_behaviors_origin() {
        let mut cfg = base();
        let obj = cfg.as_object_mut().unwrap();
        obj.remove("Origins");
        obj.insert(
            "CustomOrigin".into(),
            json!({"DNSName": "api.example.com", "OriginProtocolPolicy": "https-only",
                   "OriginSSLProtocols": ["TLSv1.2"], "HTTPSPort": 8443}),
        );
        let config = translate(&cfg);
        let origin = &config.origins.items.unwrap().origin[0];
        assert_eq!(origin.id, "o1");
        let custom = origin.custom_origin_config.as_ref().unwrap();
        assert_eq!((custom.http_port, custom.https_port), (80, 8443));
        assert_eq!(
            custom
                .origin_ssl_protocols
                .as_ref()
                .unwrap()
                .items
                .ssl_protocol,
            vec!["TLSv1.2"]
        );
    }

    #[test]
    fn origins_are_required_only_without_a_legacy_origin() {
        let mut cfg = base();
        cfg.as_object_mut().unwrap().remove("Origins");
        assert!(translate_err(&cfg).contains("Origins is required"));
        let err = translate_err(&with(
            json!({"S3Origin": {"DNSName": "b.s3.amazonaws.com"}}),
        ));
        assert!(
            err.contains("only one of Origins, S3Origin and CustomOrigin"),
            "{err}"
        );
    }

    #[test]
    fn the_translated_config_goes_through_cloudfronts_validation() {
        let err = translate_err(&with(json!({
            "DefaultCacheBehavior": {"TargetOriginId": "nowhere", "ViewerProtocolPolicy": "allow-all"}
        })));
        assert!(err.contains("One or more of your origins"), "{err}");
        let err = translate_err(&with(json!({
            "DefaultCacheBehavior": {"TargetOriginId": "o1", "ViewerProtocolPolicy": "allow-all",
                                     "AllowedMethods": ["GET", "HEAD"],
                                     "CachedMethods": ["GET", "HEAD", "OPTIONS"]}
        })));
        assert!(err.contains("CachedMethods must be"), "{err}");
    }

    #[test]
    fn mtls_connection_function_and_cache_tag_members_translate() {
        let config = translate(&with(json!({
            "Origins": [{"Id": "o1", "DomainName": "o.example.com",
                         "CustomOriginConfig": {"OriginProtocolPolicy": "https-only",
                                                "OriginMtlsConfig": {"ClientCertificateArn": "arn:cert"}}}],
            "ViewerMtlsConfig": {"Mode": "required",
                                 "TrustStoreConfig": {"TrustStoreId": "ts-1", "IgnoreCertificateExpiry": true}},
            "ConnectionFunctionAssociation": {"Id": "cf-1"},
            "CacheTagConfig": {"HeaderName": "Cache-Tag"}
        })));
        let mtls = config.viewer_mtls_config.unwrap();
        assert_eq!(mtls.mode.as_deref(), Some("required"));
        let ts = mtls.trust_store_config.unwrap();
        assert_eq!(ts.trust_store_id, "ts-1");
        assert_eq!(ts.ignore_certificate_expiry, Some(true));
        assert_eq!(config.connection_function_association.unwrap().id, "cf-1");
        assert_eq!(config.cache_tag_config.unwrap().header_name, "Cache-Tag");
        let origin = &config.origins.items.unwrap().origin[0];
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
    }

    #[test]
    fn empty_optional_lists_omit_items() {
        let config = translate(&with(json!({"Aliases": [], "CustomErrorResponses": []})));
        let aliases = config.aliases.unwrap();
        assert_eq!((aliases.quantity, aliases.items.is_none()), (0, true));
        let rules = config.custom_error_responses.unwrap();
        assert_eq!((rules.quantity, rules.items.is_none()), (0, true));
    }
}
