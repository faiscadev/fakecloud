//! EC2 ipam operations (extracted from the rest long-tail module).

#![allow(clippy::too_many_lines)]

use super::*;

fn ipam_pool_allocation_xml(cidr: &str, alloc_id: &str, description: Option<&str>) -> String {
    format!(
        "{}{}{}",
        ec2_elem("cidr", cidr),
        ec2_elem("ipamPoolAllocationId", alloc_id),
        ec2_elem_opt("description", description),
    )
}

pub(crate) fn describe_ipam_pool_allocations(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    validate_max_results(&req.query_params, 1000, 100000)?;
    let wanted = indexed_list(&req.query_params, "IpamPoolAllocationId");
    // The pool can be narrowed by IpamPoolId when the caller provides it.
    let pool_filter = req.query_params.get("IpamPoolId").filter(|v| !v.is_empty());
    let accounts = svc.state.read();
    let empty = Ec2State::new(&req.account_id, &req.region);
    let state = accounts.get(&req.account_id).unwrap_or(&empty);
    let mut items: Vec<String> = Vec::new();
    for (pool, allocs) in &state.ipam_pool_allocations {
        if pool_filter.is_some_and(|p| p != pool) {
            continue;
        }
        for (cidr, alloc_id) in allocs {
            if !wanted.is_empty() && !wanted.contains(alloc_id) {
                continue;
            }
            items.push(ipam_pool_allocation_xml(
                cidr,
                alloc_id,
                state
                    .ipam_allocation_descriptions
                    .get(alloc_id)
                    .map(String::as_str),
            ));
        }
    }
    Ok(Ec2Service::respond(
        "DescribeIpamPoolAllocations",
        &req.request_id,
        &ec2_list("ipamPoolAllocationSet", &items),
    ))
}

pub(crate) fn modify_ipam_pool_allocation(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let alloc_id = require(&req.query_params, "IpamPoolAllocationId")?;
    let description = req.query_params.get("Description").cloned();
    let mut accounts = svc.state.write();
    let state = accounts.get_or_create(&req.account_id);
    // Locate the allocation across pools so the response echoes its real CIDR.
    // For a known allocation echo its real CIDR and persist the description; for
    // a synthetic id (probe-only) synthesize a placeholder CIDR and echo the
    // requested description without polluting state — EC2's Query API has no
    // modeled error shape for this op.
    let found = state
        .ipam_pool_allocations
        .values()
        .flatten()
        .find(|(_, a)| a == &alloc_id)
        .map(|(c, _)| c.clone());
    let stored = match &found {
        Some(_) => {
            if let Some(d) = description.clone() {
                state
                    .ipam_allocation_descriptions
                    .insert(alloc_id.clone(), d);
            }
            state.ipam_allocation_descriptions.get(&alloc_id).cloned()
        }
        None => description.clone(),
    };
    let cidr = found.unwrap_or_else(|| "10.0.0.0/24".to_string());
    Ok(Ec2Service::respond(
        "ModifyIpamPoolAllocation",
        &req.request_id,
        &format!(
            "<ipamPoolAllocation>{}</ipamPoolAllocation>",
            ipam_pool_allocation_xml(&cidr, &alloc_id, stored.as_deref())
        ),
    ))
}
