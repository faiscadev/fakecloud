//! `DescribeTags`.
//!
//! The mutating `CreateTags`/`DeleteTags` (which write [`crate::state::Ec2State`]
//! via `upsert_tags`/`remove_tags`) land in the next batch alongside the L1
//! probe's ec2Query request encoder — input-bearing ops can't be verified at
//! Level 1 until the probe renames members via `ec2QueryName` and flattens
//! lists as `.N` instead of `.member.N`.

use fakecloud_aws::ec2query::{ec2_elem, ec2_list};
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use crate::service::Ec2Service;
use crate::service_helpers::{parse_filters, Filter};
use crate::state::Tag;

pub(crate) fn describe_tags(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let filters = parse_filters(&req.query_params);

    let accounts = svc.state.read();
    let empty = crate::state::Ec2State::new(&req.account_id, &req.region);
    let state = accounts.get(&req.account_id).unwrap_or(&empty);

    let mut items = Vec::new();
    for (resource_id, tags) in &state.tags {
        for tag in tags {
            if !tag_matches_filters(resource_id, tag, &filters) {
                continue;
            }
            items.push(format!(
                "{}{}{}{}",
                ec2_elem("resourceId", resource_id),
                ec2_elem("resourceType", &infer_resource_type(resource_id)),
                ec2_elem("key", &tag.key),
                ec2_elem("value", &tag.value),
            ));
        }
    }
    // Stable ordering so responses are deterministic.
    items.sort();

    let body = ec2_list("tagSet", &items);
    Ok(Ec2Service::respond("DescribeTags", &req.request_id, &body))
}

/// Apply the `DescribeTags` filter set (`key`, `value`, `resource-id`,
/// `resource-type`). AND across filters, OR within a filter's values.
fn tag_matches_filters(resource_id: &str, tag: &Tag, filters: &[Filter]) -> bool {
    filters.iter().all(|f| {
        let candidate = match f.name.as_str() {
            "key" => tag.key.clone(),
            "value" => tag.value.clone(),
            "resource-id" => resource_id.to_string(),
            "resource-type" => infer_resource_type(resource_id),
            // Unknown filter names match nothing (AWS rejects, but matching
            // nothing is the safe, test-stable behavior for the foundation).
            _ => return false,
        };
        f.values.iter().any(|v| {
            // EC2 filter values support a trailing `*` wildcard.
            if let Some(prefix) = v.strip_suffix('*') {
                candidate.starts_with(prefix)
            } else {
                candidate == *v
            }
        })
    })
}

/// Best-effort resource-type inference from an EC2 resource id prefix. Covers
/// the families that exist today; extended as new id prefixes are introduced.
pub(crate) fn infer_resource_type(resource_id: &str) -> String {
    let ty = match resource_id.split('-').next().unwrap_or("") {
        "i" => "instance",
        "vpc" => "vpc",
        "subnet" => "subnet",
        "sg" => "security-group",
        "vol" => "volume",
        "snap" => "snapshot",
        "ami" => "image",
        "eni" => "network-interface",
        "igw" => "internet-gateway",
        "rtb" => "route-table",
        "eipalloc" => "elastic-ip",
        _ => "resource",
    };
    ty.to_string()
}
