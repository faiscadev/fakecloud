//! Region / Availability-Zone / account-attribute describe primitives.
//!
//! These return a faithful, static view of AWS's standard commercial regions
//! and zones. SDK clients call them implicitly (e.g. to resolve `Describe
//! AvailabilityZones` before launching), so they must exist from the
//! foundation even though they hold no per-account state.

use fakecloud_aws::ec2query::{ec2_elem, ec2_list};
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use crate::service::Ec2Service;
use crate::service_helpers::{indexed_list, parse_filters};

use fakecloud_aws::regions::COMMERCIAL_REGIONS;

pub(crate) fn describe_regions(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    // Honor an explicit RegionName.N allow-list and a `region-name` filter.
    let requested = indexed_list(&req.query_params, "RegionName");
    let filters = parse_filters(&req.query_params);
    let name_filter: Vec<String> = filters
        .iter()
        .filter(|f| f.name == "region-name")
        .flat_map(|f| f.values.clone())
        .collect();

    // Every Region accepts requests here, so an opt-in Region behaves like
    // one the account has enabled: AWS reports those as `opted-in`.
    let items: Vec<String> = COMMERCIAL_REGIONS
        .iter()
        .filter(|(r, _)| requested.is_empty() || requested.iter().any(|x| x == r))
        .filter(|(r, _)| name_filter.is_empty() || name_filter.iter().any(|x| x == r))
        .map(|(r, opt_in)| {
            format!(
                "{}{}{}",
                ec2_elem("regionName", r),
                ec2_elem("regionEndpoint", &format!("ec2.{r}.amazonaws.com")),
                ec2_elem(
                    "optInStatus",
                    if *opt_in {
                        "opted-in"
                    } else {
                        "opt-in-not-required"
                    }
                ),
            )
        })
        .collect();

    let body = ec2_list("regionInfo", &items);
    Ok(Ec2Service::respond(
        "DescribeRegions",
        &req.request_id,
        &body,
    ))
}

pub(crate) fn describe_availability_zones(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    // Three zones (a/b/c) for the request's region, matching the common case.
    let region = if req.region.is_empty() {
        "us-east-1"
    } else {
        &req.region
    };
    let requested = indexed_list(&req.query_params, "ZoneName");
    // A standard AZ's group name is its region; ModifyAvailabilityZoneGroup
    // opt-in status (if set) is reflected here so the round-trip is observable.
    let optin = {
        let accounts = svc.state.read();
        accounts
            .regional(&req.account_id, &req.region)
            .and_then(|s| s.az_group_optin.get(region).cloned())
    };
    let opt_in_status = optin.as_deref().unwrap_or("opt-in-not-required");

    let items: Vec<String> = crate::defaults::region_zones(region)
        .into_iter()
        .filter(|(zone, _)| requested.is_empty() || requested.iter().any(|x| x == zone))
        .map(|(zone, zone_id)| {
            format!(
                "{}{}{}{}{}{}{}",
                ec2_elem("zoneName", &zone),
                ec2_elem("zoneState", "available"),
                ec2_elem("optInStatus", opt_in_status),
                ec2_elem("regionName", region),
                ec2_elem("zoneId", &zone_id),
                ec2_elem("zoneType", "availability-zone"),
                ec2_elem("groupName", region),
            )
        })
        .collect();

    let body = ec2_list("availabilityZoneInfo", &items);
    Ok(Ec2Service::respond(
        "DescribeAvailabilityZones",
        &req.request_id,
        &body,
    ))
}

pub(crate) fn describe_account_attributes(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    // The canonical account attributes AWS returns for a modern VPC-only account.
    let attrs: &[(&str, &[&str])] = &[
        ("supported-platforms", &["VPC"]),
        ("default-vpc", &["vpc-00000000"]),
        ("max-instances", &["20"]),
        ("vpc-max-security-groups-per-interface", &["5"]),
        ("max-elastic-ips", &["5"]),
        ("vpc-max-elastic-ips", &["5"]),
    ];

    let requested = indexed_list(&req.query_params, "AttributeName");

    let items: Vec<String> = attrs
        .iter()
        .filter(|(name, _)| requested.is_empty() || requested.iter().any(|x| x == name))
        .map(|(name, values)| {
            let value_items: Vec<String> = values
                .iter()
                .map(|v| ec2_elem("attributeValue", v))
                .collect();
            format!(
                "{}{}",
                ec2_elem("attributeName", name),
                ec2_list("attributeValueSet", &value_items),
            )
        })
        .collect();

    let body = ec2_list("accountAttributeSet", &items);
    Ok(Ec2Service::respond(
        "DescribeAccountAttributes",
        &req.request_id,
        &body,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn describe_regions_lists_every_commercial_region() {
        let names: Vec<&str> = COMMERCIAL_REGIONS.iter().map(|(r, _)| *r).collect();
        for r in [
            "ap-east-2",
            "ap-southeast-5",
            "ap-southeast-6",
            "ap-southeast-7",
            "ca-west-1",
            "il-central-1",
            "mx-central-1",
        ] {
            assert!(names.contains(&r), "{r} missing");
        }
    }
}
