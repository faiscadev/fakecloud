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

/// Standard commercial regions surfaced by `DescribeRegions`.
const REGIONS: &[&str] = &[
    "us-east-1",
    "us-east-2",
    "us-west-1",
    "us-west-2",
    "af-south-1",
    "ap-east-1",
    "ap-south-1",
    "ap-south-2",
    "ap-northeast-1",
    "ap-northeast-2",
    "ap-northeast-3",
    "ap-southeast-1",
    "ap-southeast-2",
    "ap-southeast-3",
    "ap-southeast-4",
    "ca-central-1",
    "eu-central-1",
    "eu-central-2",
    "eu-west-1",
    "eu-west-2",
    "eu-west-3",
    "eu-north-1",
    "eu-south-1",
    "eu-south-2",
    "me-south-1",
    "me-central-1",
    "sa-east-1",
];

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

    let items: Vec<String> = REGIONS
        .iter()
        .filter(|r| requested.is_empty() || requested.iter().any(|x| x == *r))
        .filter(|r| name_filter.is_empty() || name_filter.iter().any(|x| x == *r))
        .map(|r| {
            format!(
                "{}{}{}",
                ec2_elem("regionName", r),
                ec2_elem("regionEndpoint", &format!("ec2.{r}.amazonaws.com")),
                ec2_elem("optInStatus", "opt-in-not-required"),
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
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    // Three zones (a/b/c) for the request's region, matching the common case.
    let region = if req.region.is_empty() {
        "us-east-1"
    } else {
        &req.region
    };
    let requested = indexed_list(&req.query_params, "ZoneName");

    let items: Vec<String> = ["a", "b", "c"]
        .iter()
        .enumerate()
        .map(|(i, suffix)| (format!("{region}{suffix}"), i + 1))
        .filter(|(zone, _)| requested.is_empty() || requested.iter().any(|x| x == zone))
        .map(|(zone, idx)| {
            // zoneId uses AWS's `<region-short>-az<N>` convention.
            let short = region_short_code(region);
            format!(
                "{}{}{}{}{}",
                ec2_elem("zoneName", &zone),
                ec2_elem("zoneState", "available"),
                ec2_elem("regionName", region),
                ec2_elem("zoneId", &format!("{short}-az{idx}")),
                ec2_elem("zoneType", "availability-zone"),
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

/// Map a region to AWS's short zone-id prefix (e.g. `us-east-1` -> `use1`).
fn region_short_code(region: &str) -> String {
    let parts: Vec<&str> = region.split('-').collect();
    if parts.len() < 3 {
        return region.replace('-', "");
    }
    let first: String = parts[0].chars().take(2).collect();
    let middle: String = parts[1].chars().take(1).collect();
    format!("{first}{middle}{}", parts[2])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn region_short_code_matches_aws_convention() {
        assert_eq!(region_short_code("us-east-1"), "use1");
        assert_eq!(region_short_code("ap-southeast-2"), "aps2");
        assert_eq!(region_short_code("eu-central-1"), "euc1");
    }
}
