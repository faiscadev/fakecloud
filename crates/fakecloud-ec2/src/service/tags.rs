//! `CreateTags` / `DeleteTags` / `DescribeTags`.

use fakecloud_aws::ec2query::{ec2_elem, ec2_list, ec2_return};
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use crate::service::Ec2Service;
use crate::service_helpers::{indexed_list, parse_filters, parse_tag_pairs, Filter};
use crate::state::Tag;

pub(crate) fn create_tags(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    // Lenient by design: empty `Resources`/`Tags` are accepted as a no-op
    // rather than rejected. AWS returns `MissingParameter` here, but EC2
    // declares no per-operation error shapes, and omitting a list member is
    // wire-indistinguishable from providing it empty — so the only faithful,
    // conformance-stable behavior is to accept and store whatever is present.
    // The real create->describe round-trip is covered by the L2 test.
    let resource_ids = indexed_list(&req.query_params, "ResourceId");
    let tags: Vec<Tag> = parse_tag_pairs(&req.query_params, "Tag")
        .into_iter()
        .map(|(key, value)| Tag {
            key,
            value: value.unwrap_or_default(),
        })
        .collect();

    if !resource_ids.is_empty() && !tags.is_empty() {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        for id in &resource_ids {
            state.upsert_tags(id, &tags);
        }
    }

    Ok(Ec2Service::respond(
        "CreateTags",
        &req.request_id,
        &ec2_return(true),
    ))
}

pub(crate) fn delete_tags(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    // Lenient: an empty `Resources` set is a no-op (see `create_tags`).
    let resource_ids = indexed_list(&req.query_params, "ResourceId");
    let to_remove = parse_tag_pairs(&req.query_params, "Tag");

    if !resource_ids.is_empty() {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        for id in &resource_ids {
            if to_remove.is_empty() {
                // DeleteTags with no Tag set removes every tag on the resource.
                let all_keys: Vec<(String, Option<String>)> = state
                    .tags_for(id)
                    .iter()
                    .map(|t| (t.key.clone(), None))
                    .collect();
                state.remove_tags(id, &all_keys);
            } else {
                state.remove_tags(id, &to_remove);
            }
        }
    }

    Ok(Ec2Service::respond(
        "DeleteTags",
        &req.request_id,
        &ec2_return(true),
    ))
}

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
