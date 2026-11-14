//! EC2 capacity manager operations (extracted from the rest long-tail module).

#![allow(clippy::too_many_lines)]

use super::*;

fn cm_status(enabled: bool) -> &'static str {
    if enabled {
        "ENABLED"
    } else {
        "DISABLED"
    }
}

fn data_export_xml(e: &CapacityManagerDataExport) -> String {
    format!(
        "{}{}{}{}{}<latestDeliveryStatus>PENDING</latestDeliveryStatus>",
        ec2_elem("capacityManagerDataExportId", &e.id),
        ec2_elem("s3BucketName", &e.s3_bucket_name),
        ec2_elem("s3BucketPrefix", &e.s3_bucket_prefix),
        ec2_elem("schedule", &e.schedule),
        ec2_elem("outputFormat", &e.output_format),
    )
}

pub(crate) fn create_capacity_manager_data_export(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let s3_bucket_name = require(&req.query_params, "S3BucketName")?;
    let schedule = require(&req.query_params, "Schedule")?;
    let output_format = require(&req.query_params, "OutputFormat")?;
    validate_enum(&req.query_params, "Schedule", &["hourly"])?;
    validate_enum(&req.query_params, "OutputFormat", &["csv", "parquet"])?;
    let id = gen_id("cmde");
    let export = CapacityManagerDataExport {
        id: id.clone(),
        s3_bucket_name,
        s3_bucket_prefix: req
            .query_params
            .get("S3BucketPrefix")
            .cloned()
            .unwrap_or_default(),
        schedule,
        output_format,
    };
    {
        let mut accounts = svc.state.write();
        accounts
            .regional_mut(&req.account_id, &req.region)
            .capacity_manager_data_exports
            .insert(id.clone(), export);
    }
    Ok(Ec2Service::respond(
        "CreateCapacityManagerDataExport",
        &req.request_id,
        &ec2_elem("capacityManagerDataExportId", &id),
    ))
}

pub(crate) fn delete_capacity_manager_data_export(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "CapacityManagerDataExportId")?;
    {
        let mut accounts = svc.state.write();
        accounts
            .regional_mut(&req.account_id, &req.region)
            .capacity_manager_data_exports
            .remove(&id);
    }
    Ok(Ec2Service::respond(
        "DeleteCapacityManagerDataExport",
        &req.request_id,
        &ec2_elem("capacityManagerDataExportId", &id),
    ))
}

pub(crate) fn describe_capacity_manager_data_exports(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    validate_max_results(&req.query_params, 1, 1000)?;
    let accounts = svc.state.read();
    let empty = Ec2State::new(&req.account_id, &req.region);
    let state = accounts
        .regional(&req.account_id, &req.region)
        .unwrap_or(&empty);
    let items: Vec<String> = state
        .capacity_manager_data_exports
        .values()
        .map(data_export_xml)
        .collect();
    Ok(Ec2Service::respond(
        "DescribeCapacityManagerDataExports",
        &req.request_id,
        &ec2_list("capacityManagerDataExportSet", &items),
    ))
}

/// Render `<capacityManagerStatus>` + `<organizationsAccess>`, shared by the
/// enable/disable/update-org-access ops.
fn cm_status_body(enabled: bool, org_access: &str) -> String {
    // `OrganizationsAccess` is modeled as `Boolean` (com.amazonaws.ec2#Boolean),
    // so it must render as `true`/`false` — emitting a status string like
    // "disabled" makes the SDK's XML decoder reject the response.
    let org_bool = matches!(
        org_access.trim().to_ascii_lowercase().as_str(),
        "enabled" | "true"
    );
    format!(
        "{}{}",
        ec2_elem("capacityManagerStatus", cm_status(enabled)),
        ec2_elem(
            "organizationsAccess",
            if org_bool { "true" } else { "false" }
        ),
    )
}

pub(crate) fn disable_capacity_manager(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let org_access = {
        let mut accounts = svc.state.write();
        let state = accounts.regional_mut(&req.account_id, &req.region);
        state.capacity_manager_enabled = false;
        state
            .capacity_manager_org_access
            .clone()
            .unwrap_or_else(|| "disabled".to_string())
    };
    Ok(Ec2Service::respond(
        "DisableCapacityManager",
        &req.request_id,
        &cm_status_body(false, &org_access),
    ))
}

pub(crate) fn enable_capacity_manager(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let org_access = {
        let mut accounts = svc.state.write();
        let state = accounts.regional_mut(&req.account_id, &req.region);
        state.capacity_manager_enabled = true;
        state
            .capacity_manager_org_access
            .clone()
            .unwrap_or_else(|| "disabled".to_string())
    };
    Ok(Ec2Service::respond(
        "EnableCapacityManager",
        &req.request_id,
        &cm_status_body(true, &org_access),
    ))
}

pub(crate) fn get_capacity_manager_attributes(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let accounts = svc.state.read();
    let empty = Ec2State::new(&req.account_id, &req.region);
    let state = accounts
        .regional(&req.account_id, &req.region)
        .unwrap_or(&empty);
    let org_access = state
        .capacity_manager_org_access
        .clone()
        .unwrap_or_else(|| "disabled".to_string());
    let body = format!(
        "{}{}",
        cm_status_body(state.capacity_manager_enabled, &org_access),
        ec2_elem(
            "dataExportCount",
            &state.capacity_manager_data_exports.len().to_string()
        ),
    );
    Ok(Ec2Service::respond(
        "GetCapacityManagerAttributes",
        &req.request_id,
        &body,
    ))
}

pub(crate) fn get_capacity_manager_metric_data(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    validate_int_range(&req.query_params, "Period", 3600, i64::MAX)?;
    require(&req.query_params, "StartTime")?;
    require(&req.query_params, "EndTime")?;
    require(&req.query_params, "Period")?;
    Ok(Ec2Service::respond(
        "GetCapacityManagerMetricData",
        &req.request_id,
        "",
    ))
}

pub(crate) fn get_capacity_manager_metric_dimensions(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "StartTime")?;
    require(&req.query_params, "EndTime")?;
    Ok(Ec2Service::respond(
        "GetCapacityManagerMetricDimensions",
        &req.request_id,
        "",
    ))
}

/// The `(region, zone name)` an Availability Zone id denotes, looking first in
/// the request's Region and then in every other one (a request may score
/// zones of any Region).
fn zone_for_id(home_region: &str, zone_id: &str) -> Option<(String, String)> {
    std::iter::once(home_region)
        .chain(
            fakecloud_aws::regions::COMMERCIAL_REGIONS
                .iter()
                .map(|(r, _)| *r),
        )
        .find_map(|region| {
            crate::defaults::zone_name_for_id(region, zone_id).map(|z| (region.to_string(), z))
        })
}

/// How well one instance type in one zone supports the request. fakecloud's
/// capacity pool is unbounded, so a combination EC2 sells there (in the
/// requested market) holds several times any amount: `high`. A type the zone
/// does not sell, or does not sell in the requested market, is `unsupported`.
fn supportability(
    spec: &crate::instance_types::InstanceTypeSpec,
    region: &str,
    zone: &str,
    market: &str,
) -> &'static str {
    let usage_class = if market == "spot" {
        "spot"
    } else {
        "on-demand"
    };
    if spec.offered_in_zone(region, zone)
        && spec
            .values_in(region, "supportedUsageClasses")
            .contains(&usage_class)
    {
        "high"
    } else {
        "unsupported"
    }
}

pub(crate) fn get_capacity_manager_supportability_scores(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let params = &req.query_params;
    let market = require(params, "MarketType")?;
    require(params, "CapacityAmount")?;
    require(params, "CapacityUnit")?;
    validate_enum(params, "MarketType", &["on-demand", "spot"])?;
    validate_enum(params, "CapacityUnit", &["vcpus", "instances"])?;
    validate_int_range(params, "CapacityAmount", 1, i64::from(i32::MAX))?;
    if params
        .get("CapacityAmount")
        .is_some_and(|v| v.parse::<i64>().is_err())
    {
        return Err(invalid_parameter_value("CapacityAmount must be an integer"));
    }
    let zone_ids = indexed_list(params, "CapacityAvailabilityZoneId");
    if zone_ids.is_empty() {
        return Err(missing_parameter("CapacityAvailabilityZoneId"));
    }
    if zone_ids.len() > 10 {
        return Err(invalid_parameter_value(
            "You can specify up to 10 Availability Zones",
        ));
    }
    let named_types = indexed_list(params, "InstanceType");
    let by_requirements = params
        .keys()
        .any(|k| k.starts_with("InstanceRequirementsWithMetadata."));
    if !named_types.is_empty() && by_requirements {
        return Err(AwsServiceError::aws_error(
            http::StatusCode::BAD_REQUEST,
            "InvalidParameterCombination",
            "Specify either InstanceTypes or InstanceRequirementsWithMetadata, but not both",
        ));
    }
    if named_types.len() > 50 {
        return Err(invalid_parameter_value(
            "You can specify up to 50 instance types",
        ));
    }
    let home = region_of(req);
    let zones: Vec<(String, String, String)> = zone_ids
        .iter()
        .map(|id| {
            zone_for_id(&home, id)
                .map(|(region, zone)| (id.clone(), region, zone))
                .ok_or_else(|| {
                    invalid_parameter_value(format!("The Availability Zone ID '{id}' is not valid"))
                })
        })
        .collect::<Result<_, _>>()?;
    let specs: Vec<&crate::instance_types::InstanceTypeSpec> = if by_requirements {
        crate::service::instance::types_matching_requirements_with_metadata(
            params,
            "InstanceRequirementsWithMetadata",
            &home,
        )?
    } else if named_types.is_empty() {
        return Err(missing_parameter("InstanceType"));
    } else {
        named_types
            .iter()
            .map(|t| {
                crate::instance_types::lookup(t).ok_or_else(|| {
                    invalid_parameter_value(format!("The instance type '{t}' is not valid"))
                })
            })
            .collect::<Result<_, _>>()?
    };
    let mut aggregate = "unsupported";
    let mut items = Vec::new();
    for spec in &specs {
        for (zone_id, region, zone) in &zones {
            let score = supportability(spec, region, zone, &market);
            if score == "high" {
                aggregate = "high";
            }
            items.push(format!(
                "{}{}{}",
                ec2_elem("instanceType", &spec.name),
                ec2_elem("availabilityZoneId", zone_id),
                ec2_elem("score", score),
            ));
        }
    }
    let body = format!(
        "{}{}",
        ec2_elem("aggregateScore", aggregate),
        ec2_list("capacityManagerSupportabilityScoreSet", &items),
    );
    Ok(Ec2Service::respond(
        "GetCapacityManagerSupportabilityScores",
        &req.request_id,
        &body,
    ))
}

pub(crate) fn get_capacity_manager_monitored_tag_keys(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    validate_max_results(&req.query_params, 1, 1000)?;
    let accounts = svc.state.read();
    let empty = Ec2State::new(&req.account_id, &req.region);
    let state = accounts
        .regional(&req.account_id, &req.region)
        .unwrap_or(&empty);
    let items = state.capacity_manager_monitored_tag_keys.clone();
    Ok(Ec2Service::respond(
        "GetCapacityManagerMonitoredTagKeys",
        &req.request_id,
        &ec2_list("capacityManagerTagKeySet", &items),
    ))
}

pub(crate) fn update_capacity_manager_monitored_tag_keys(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let activate = indexed_list(&req.query_params, "ActivateTagKey");
    let deactivate = indexed_list(&req.query_params, "DeactivateTagKey");
    let keys = {
        let mut accounts = svc.state.write();
        let state = accounts.regional_mut(&req.account_id, &req.region);
        for k in &activate {
            if !state.capacity_manager_monitored_tag_keys.contains(k) {
                state.capacity_manager_monitored_tag_keys.push(k.clone());
            }
        }
        state
            .capacity_manager_monitored_tag_keys
            .retain(|k| !deactivate.contains(k));
        state.capacity_manager_monitored_tag_keys.clone()
    };
    Ok(Ec2Service::respond(
        "UpdateCapacityManagerMonitoredTagKeys",
        &req.request_id,
        &ec2_list("capacityManagerTagKeySet", &keys),
    ))
}

pub(crate) fn update_capacity_manager_organizations_access(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let org_access = require(&req.query_params, "OrganizationsAccess")?;
    let enabled = {
        let mut accounts = svc.state.write();
        let state = accounts.regional_mut(&req.account_id, &req.region);
        state.capacity_manager_org_access = Some(org_access.clone());
        state.capacity_manager_enabled
    };
    Ok(Ec2Service::respond(
        "UpdateCapacityManagerOrganizationsAccess",
        &req.request_id,
        &cm_status_body(enabled, &org_access),
    ))
}

#[cfg(test)]
mod supportability_tests {
    use super::*;
    use crate::test_support::{ec2_request as req, err_of};

    fn scores(extra: &[(&str, &str)]) -> Result<String, String> {
        let mut q = vec![
            ("MarketType", "on-demand"),
            ("CapacityAmount", "2"),
            ("CapacityUnit", "instances"),
        ];
        q.extend_from_slice(extra);
        get_capacity_manager_supportability_scores(
            &Ec2Service::new(),
            &req("GetCapacityManagerSupportabilityScores", &q),
        )
        .map(|r| String::from_utf8_lossy(r.body.expect_bytes()).to_string())
        .map_err(|e| e.code().to_string())
    }

    fn score_of(xml: &str, instance_type: &str, zone_id: &str) -> String {
        let item = format!(
            "<instanceType>{instance_type}</instanceType><availabilityZoneId>{zone_id}</availabilityZoneId><score>"
        );
        xml.split(&item)
            .nth(1)
            .and_then(|s| s.split('<').next())
            .unwrap_or_else(|| panic!("no score for {instance_type} in {zone_id}: {xml}"))
            .to_string()
    }

    #[test]
    fn each_type_and_zone_is_scored_by_where_ec2_sells_it() {
        // mac1.metal is sold in us-east-1c/d only, on demand only; m5.large
        // everywhere in us-east-1 but 1e, and in us-west-2.
        let xml = scores(&[
            ("CapacityAvailabilityZoneId.1", "use1-az1"),
            ("CapacityAvailabilityZoneId.2", "use1-az3"),
            ("CapacityAvailabilityZoneId.3", "use1-az5"),
            ("CapacityAvailabilityZoneId.4", "usw2-az1"),
            ("InstanceType.1", "mac1.metal"),
            ("InstanceType.2", "m5.large"),
        ])
        .unwrap();
        assert_eq!(score_of(&xml, "mac1.metal", "use1-az1"), "unsupported");
        assert_eq!(score_of(&xml, "mac1.metal", "use1-az3"), "high");
        assert_eq!(score_of(&xml, "m5.large", "use1-az5"), "unsupported");
        assert_eq!(score_of(&xml, "m5.large", "usw2-az1"), "high");
        assert!(
            xml.contains("<aggregateScore>high</aggregateScore>"),
            "{xml}"
        );

        let spot = get_capacity_manager_supportability_scores(
            &Ec2Service::new(),
            &req(
                "GetCapacityManagerSupportabilityScores",
                &[
                    ("MarketType", "spot"),
                    ("CapacityAmount", "8"),
                    ("CapacityUnit", "vcpus"),
                    ("CapacityAvailabilityZoneId.1", "use1-az3"),
                    ("InstanceType.1", "mac1.metal"),
                ],
            ),
        )
        .unwrap();
        let spot = String::from_utf8_lossy(spot.body.expect_bytes()).to_string();
        assert!(
            spot.contains("<aggregateScore>unsupported</aggregateScore>"),
            "{spot}"
        );
    }

    #[test]
    fn instance_requirements_resolve_to_types() {
        let xml = scores(&[
            ("CapacityAvailabilityZoneId.1", "use1-az1"),
            (
                "InstanceRequirementsWithMetadata.InstanceRequirements.VCpuCount.Min",
                "2",
            ),
            (
                "InstanceRequirementsWithMetadata.InstanceRequirements.VCpuCount.Max",
                "2",
            ),
            (
                "InstanceRequirementsWithMetadata.InstanceRequirements.MemoryMiB.Min",
                "8192",
            ),
            (
                "InstanceRequirementsWithMetadata.InstanceRequirements.MemoryMiB.Max",
                "8192",
            ),
            (
                "InstanceRequirementsWithMetadata.InstanceRequirements.AllowedInstanceType.1",
                "m5.*",
            ),
        ])
        .unwrap();
        assert_eq!(score_of(&xml, "m5.large", "use1-az1"), "high");
        assert!(!xml.contains("m5.xlarge"), "{xml}");
    }

    #[test]
    fn requests_are_validated() {
        let zone = ("CapacityAvailabilityZoneId.1", "use1-az1");
        let it = ("InstanceType.1", "m5.large");
        assert_eq!(scores(&[it]).unwrap_err(), "MissingParameter");
        assert_eq!(scores(&[zone]).unwrap_err(), "MissingParameter");
        assert_eq!(
            scores(&[("CapacityAvailabilityZoneId.1", "use1-az99"), it]).unwrap_err(),
            "InvalidParameterValue"
        );
        assert_eq!(
            scores(&[zone, ("InstanceType.1", "m5.nope")]).unwrap_err(),
            "InvalidParameterValue"
        );
        assert_eq!(
            scores(&[
                zone,
                it,
                (
                    "InstanceRequirementsWithMetadata.InstanceRequirements.VCpuCount.Min",
                    "1"
                ),
            ])
            .unwrap_err(),
            "InvalidParameterCombination"
        );
        let e = err_of(get_capacity_manager_supportability_scores(
            &Ec2Service::new(),
            &req(
                "GetCapacityManagerSupportabilityScores",
                &[
                    ("MarketType", "reserved"),
                    ("CapacityAmount", "1"),
                    ("CapacityUnit", "instances"),
                    zone,
                    it,
                ],
            ),
        ));
        assert_eq!(e.code(), "InvalidParameterValue");
    }
}
