//! IPAM core: IPAMs, scopes, pools, pool CIDRs, allocations, resource CIDRs,
//! and organization admin delegation.

use fakecloud_aws::ec2query::{ec2_elem, ec2_list};
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use crate::service::Ec2Service;
use crate::service_helpers::{
    gen_id, indexed_list, require, validate_enum, validate_int_range, validate_max_results,
};

use crate::state::{Ec2State, Ipam, IpamPool, IpamScope, Tag};

/// AllocationMin/Max/Default netmask-length range checks shared by pool ops.
fn validate_pool_netmasks(req: &AwsRequest) -> Result<(), AwsServiceError> {
    for k in [
        "AllocationDefaultNetmaskLength",
        "AllocationMaxNetmaskLength",
        "AllocationMinNetmaskLength",
    ] {
        validate_int_range(&req.query_params, k, 0, 128)?;
    }
    Ok(())
}

fn region_of(req: &AwsRequest) -> String {
    if req.region.is_empty() {
        "us-east-1".to_string()
    } else {
        req.region.clone()
    }
}

// ---- IPAMs ----

fn ipam_xml(i: &Ipam, tags: &[Tag], owner: &str, region: &str) -> String {
    format!(
        "{}{}{}{}{}{}<scopeCount>2</scopeCount><state>create-complete</state>{}{}{}",
        ec2_elem("ipamId", &i.id),
        ec2_elem("ipamArn", &format!("arn:aws:ec2::{owner}:ipam/{}", i.id)),
        ec2_elem("ipamRegion", region),
        ec2_elem("ownerId", owner),
        ec2_elem("publicDefaultScopeId", &i.public_scope_id),
        ec2_elem("privateDefaultScopeId", &i.private_scope_id),
        ec2_elem("tier", &i.tier),
        ec2_list(
            "operatingRegionSet",
            &[format!("<regionName>{region}</regionName>")]
        ),
        super::tags::tag_set_xml(tags),
    )
}

pub(crate) fn create_ipam(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    validate_enum(&req.query_params, "Tier", &["free", "advanced"])?;
    validate_enum(
        &req.query_params,
        "MeteredAccount",
        &["ipam-owner", "resource-owner"],
    )?;
    let id = gen_id("ipam");
    let i = Ipam {
        id: id.clone(),
        public_scope_id: gen_id("ipam-scope"),
        private_scope_id: gen_id("ipam-scope"),
        tier: req
            .query_params
            .get("Tier")
            .cloned()
            .unwrap_or_else(|| "advanced".to_string()),
    };
    let owner = req.account_id.clone();
    let region = region_of(req);
    let tags = {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        crate::service::tags::apply_tag_specifications(state, &req.query_params, &id, "ipam");
        let t = state.tags_for(&id).to_vec();
        state.ipams.insert(id.clone(), i.clone());
        t
    };
    Ok(Ec2Service::respond(
        "CreateIpam",
        &req.request_id,
        &format!("<ipam>{}</ipam>", ipam_xml(&i, &tags, &owner, &region)),
    ))
}

pub(crate) fn delete_ipam(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "IpamId")?;
    let owner = req.account_id.clone();
    let region = region_of(req);
    let mut accounts = svc.state.write();
    let state = accounts.get_or_create(&req.account_id);
    let i = state.ipams.remove(&id).unwrap_or(Ipam {
        id: id.clone(),
        public_scope_id: gen_id("ipam-scope"),
        private_scope_id: gen_id("ipam-scope"),
        tier: "advanced".to_string(),
    });
    let tags = state.tags_for(&id).to_vec();
    state.tags.remove(&id);
    Ok(Ec2Service::respond(
        "DeleteIpam",
        &req.request_id,
        &format!("<ipam>{}</ipam>", ipam_xml(&i, &tags, &owner, &region)),
    ))
}

pub(crate) fn describe_ipams(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    validate_max_results(&req.query_params, 5, 1000)?;
    let wanted = indexed_list(&req.query_params, "IpamId");
    let owner = req.account_id.clone();
    let region = region_of(req);
    let accounts = svc.state.read();
    let empty = Ec2State::new(&req.account_id, &req.region);
    let state = accounts.get(&req.account_id).unwrap_or(&empty);
    let mut items: Vec<String> = state
        .ipams
        .values()
        .filter(|i| wanted.is_empty() || wanted.contains(&i.id))
        .map(|i| ipam_xml(i, state.tags_for(&i.id), &owner, &region))
        .collect();
    items.sort();
    Ok(Ec2Service::respond(
        "DescribeIpams",
        &req.request_id,
        &ec2_list("ipamSet", &items),
    ))
}

pub(crate) fn modify_ipam(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "IpamId")?;
    validate_enum(&req.query_params, "Tier", &["free", "advanced"])?;
    validate_enum(
        &req.query_params,
        "MeteredAccount",
        &["ipam-owner", "resource-owner"],
    )?;
    let owner = req.account_id.clone();
    let region = region_of(req);
    let accounts = svc.state.read();
    let i = accounts
        .get(&req.account_id)
        .and_then(|s| s.ipams.get(&id).cloned())
        .unwrap_or(Ipam {
            id: id.clone(),
            public_scope_id: gen_id("ipam-scope"),
            private_scope_id: gen_id("ipam-scope"),
            tier: "advanced".to_string(),
        });
    let tags = accounts
        .get(&req.account_id)
        .map(|s| s.tags_for(&id).to_vec())
        .unwrap_or_default();
    Ok(Ec2Service::respond(
        "ModifyIpam",
        &req.request_id,
        &format!("<ipam>{}</ipam>", ipam_xml(&i, &tags, &owner, &region)),
    ))
}

// ---- scopes ----

fn scope_xml(sc: &IpamScope, tags: &[Tag], owner: &str) -> String {
    format!(
        "{}{}{}<ipamScopeType>private</ipamScopeType><isDefault>false</isDefault><poolCount>0</poolCount><state>create-complete</state>{}",
        ec2_elem("ipamScopeId", &sc.id),
        ec2_elem("ipamScopeArn", &format!("arn:aws:ec2::{owner}:ipam-scope/{}", sc.id)),
        ec2_elem("ipamArn", &format!("arn:aws:ec2::{owner}:ipam/{}", sc.ipam_id)),
        super::tags::tag_set_xml(tags),
    )
}

pub(crate) fn create_ipam_scope(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let ipam_id = require(&req.query_params, "IpamId")?;
    let id = gen_id("ipam-scope");
    let sc = IpamScope {
        id: id.clone(),
        ipam_id,
    };
    let owner = req.account_id.clone();
    let tags = {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        crate::service::tags::apply_tag_specifications(state, &req.query_params, &id, "ipam-scope");
        let t = state.tags_for(&id).to_vec();
        state.ipam_scopes.insert(id.clone(), sc.clone());
        t
    };
    Ok(Ec2Service::respond(
        "CreateIpamScope",
        &req.request_id,
        &format!("<ipamScope>{}</ipamScope>", scope_xml(&sc, &tags, &owner)),
    ))
}

pub(crate) fn delete_ipam_scope(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "IpamScopeId")?;
    let owner = req.account_id.clone();
    let mut accounts = svc.state.write();
    let state = accounts.get_or_create(&req.account_id);
    let sc = state.ipam_scopes.remove(&id).unwrap_or(IpamScope {
        id: id.clone(),
        ipam_id: "ipam-0".to_string(),
    });
    let tags = state.tags_for(&id).to_vec();
    state.tags.remove(&id);
    Ok(Ec2Service::respond(
        "DeleteIpamScope",
        &req.request_id,
        &format!("<ipamScope>{}</ipamScope>", scope_xml(&sc, &tags, &owner)),
    ))
}

pub(crate) fn describe_ipam_scopes(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    validate_max_results(&req.query_params, 5, 1000)?;
    let owner = req.account_id.clone();
    let accounts = svc.state.read();
    let empty = Ec2State::new(&req.account_id, &req.region);
    let state = accounts.get(&req.account_id).unwrap_or(&empty);
    let mut items: Vec<String> = state
        .ipam_scopes
        .values()
        .map(|sc| scope_xml(sc, state.tags_for(&sc.id), &owner))
        .collect();
    items.sort();
    Ok(Ec2Service::respond(
        "DescribeIpamScopes",
        &req.request_id,
        &ec2_list("ipamScopeSet", &items),
    ))
}

pub(crate) fn modify_ipam_scope(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "IpamScopeId")?;
    let owner = req.account_id.clone();
    let accounts = svc.state.read();
    let sc = accounts
        .get(&req.account_id)
        .and_then(|s| s.ipam_scopes.get(&id).cloned())
        .unwrap_or(IpamScope {
            id: id.clone(),
            ipam_id: "ipam-0".to_string(),
        });
    let tags = accounts
        .get(&req.account_id)
        .map(|s| s.tags_for(&id).to_vec())
        .unwrap_or_default();
    Ok(Ec2Service::respond(
        "ModifyIpamScope",
        &req.request_id,
        &format!("<ipamScope>{}</ipamScope>", scope_xml(&sc, &tags, &owner)),
    ))
}

// ---- pools ----

fn pool_xml(p: &IpamPool, tags: &[Tag], owner: &str) -> String {
    format!(
        "{}{}{}<ipamScopeType>private</ipamScopeType><poolDepth>1</poolDepth><state>create-complete</state><addressFamily>{}</addressFamily><autoImport>false</autoImport>{}",
        ec2_elem("ipamPoolId", &p.id),
        ec2_elem("ipamPoolArn", &format!("arn:aws:ec2::{owner}:ipam-pool/{}", p.id)),
        ec2_elem("ipamScopeArn", &format!("arn:aws:ec2::{owner}:ipam-scope/{}", p.scope_id)),
        p.address_family,
        super::tags::tag_set_xml(tags),
    )
}

pub(crate) fn create_ipam_pool(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let scope = require(&req.query_params, "IpamScopeId")?;
    let af = require(&req.query_params, "AddressFamily")?;
    validate_enum(&req.query_params, "AddressFamily", &["ipv4", "ipv6"])?;
    validate_enum(&req.query_params, "AwsService", &["ec2", "global-services"])?;
    validate_enum(&req.query_params, "PublicIpSource", &["amazon", "byoip"])?;
    validate_pool_netmasks(req)?;
    let id = gen_id("ipam-pool");
    let p = IpamPool {
        id: id.clone(),
        scope_id: scope,
        address_family: af,
    };
    let owner = req.account_id.clone();
    let tags = {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        crate::service::tags::apply_tag_specifications(state, &req.query_params, &id, "ipam-pool");
        let t = state.tags_for(&id).to_vec();
        state.ipam_pools.insert(id.clone(), p.clone());
        t
    };
    Ok(Ec2Service::respond(
        "CreateIpamPool",
        &req.request_id,
        &format!("<ipamPool>{}</ipamPool>", pool_xml(&p, &tags, &owner)),
    ))
}

pub(crate) fn delete_ipam_pool(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "IpamPoolId")?;
    let owner = req.account_id.clone();
    let mut accounts = svc.state.write();
    let state = accounts.get_or_create(&req.account_id);
    let p = state.ipam_pools.remove(&id).unwrap_or(IpamPool {
        id: id.clone(),
        scope_id: "ipam-scope-0".to_string(),
        address_family: "ipv4".to_string(),
    });
    let tags = state.tags_for(&id).to_vec();
    state.tags.remove(&id);
    state.ipam_pool_cidrs.remove(&id);
    state.ipam_pool_allocations.remove(&id);
    Ok(Ec2Service::respond(
        "DeleteIpamPool",
        &req.request_id,
        &format!("<ipamPool>{}</ipamPool>", pool_xml(&p, &tags, &owner)),
    ))
}

pub(crate) fn describe_ipam_pools(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    validate_max_results(&req.query_params, 5, 1000)?;
    let owner = req.account_id.clone();
    let accounts = svc.state.read();
    let empty = Ec2State::new(&req.account_id, &req.region);
    let state = accounts.get(&req.account_id).unwrap_or(&empty);
    let mut items: Vec<String> = state
        .ipam_pools
        .values()
        .map(|p| pool_xml(p, state.tags_for(&p.id), &owner))
        .collect();
    items.sort();
    Ok(Ec2Service::respond(
        "DescribeIpamPools",
        &req.request_id,
        &ec2_list("ipamPoolSet", &items),
    ))
}

pub(crate) fn modify_ipam_pool(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "IpamPoolId")?;
    validate_pool_netmasks(req)?;
    let owner = req.account_id.clone();
    let accounts = svc.state.read();
    let p = accounts
        .get(&req.account_id)
        .and_then(|s| s.ipam_pools.get(&id).cloned())
        .unwrap_or(IpamPool {
            id: id.clone(),
            scope_id: "ipam-scope-0".to_string(),
            address_family: "ipv4".to_string(),
        });
    let tags = accounts
        .get(&req.account_id)
        .map(|s| s.tags_for(&id).to_vec())
        .unwrap_or_default();
    Ok(Ec2Service::respond(
        "ModifyIpamPool",
        &req.request_id,
        &format!("<ipamPool>{}</ipamPool>", pool_xml(&p, &tags, &owner)),
    ))
}

// ---- pool CIDRs ----

fn pool_cidr_xml(cidr: &str, cidr_id: &str, state_val: &str) -> String {
    format!(
        "{}<state>{}</state>{}",
        ec2_elem("cidr", cidr),
        state_val,
        ec2_elem("ipamPoolCidrId", cidr_id)
    )
}

pub(crate) fn provision_ipam_pool_cidr(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let pool = require(&req.query_params, "IpamPoolId")?;
    validate_enum(
        &req.query_params,
        "VerificationMethod",
        &["remarks-x509", "dns-token"],
    )?;
    let cidr = req
        .query_params
        .get("Cidr")
        .cloned()
        .unwrap_or_else(|| "10.0.0.0/16".to_string());
    let cidr_id = gen_id("ipam-pool-cidr");
    {
        let mut accounts = svc.state.write();
        accounts
            .get_or_create(&req.account_id)
            .ipam_pool_cidrs
            .entry(pool)
            .or_default()
            .push((cidr.clone(), cidr_id.clone()));
    }
    Ok(Ec2Service::respond(
        "ProvisionIpamPoolCidr",
        &req.request_id,
        &format!(
            "<ipamPoolCidr>{}</ipamPoolCidr>",
            pool_cidr_xml(&cidr, &cidr_id, "provisioned")
        ),
    ))
}

pub(crate) fn deprovision_ipam_pool_cidr(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let pool = require(&req.query_params, "IpamPoolId")?;
    let cidr = req.query_params.get("Cidr").cloned().unwrap_or_default();
    let mut cidr_id = gen_id("ipam-pool-cidr");
    {
        let mut accounts = svc.state.write();
        if let Some(cidrs) = accounts
            .get_or_create(&req.account_id)
            .ipam_pool_cidrs
            .get_mut(&pool)
        {
            if let Some(found) = cidrs.iter().find(|(c, _)| c == &cidr).cloned() {
                cidr_id = found.1;
            }
            cidrs.retain(|(c, _)| c != &cidr);
        }
    }
    Ok(Ec2Service::respond(
        "DeprovisionIpamPoolCidr",
        &req.request_id,
        &format!(
            "<ipamPoolCidr>{}</ipamPoolCidr>",
            pool_cidr_xml(&cidr, &cidr_id, "deprovisioned")
        ),
    ))
}

pub(crate) fn get_ipam_pool_cidrs(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let pool = require(&req.query_params, "IpamPoolId")?;
    validate_max_results(&req.query_params, 5, 1000)?;
    let accounts = svc.state.read();
    let items: Vec<String> = accounts
        .get(&req.account_id)
        .and_then(|s| s.ipam_pool_cidrs.get(&pool))
        .map(|cidrs| {
            cidrs
                .iter()
                .map(|(c, id)| pool_cidr_xml(c, id, "provisioned"))
                .collect()
        })
        .unwrap_or_default();
    Ok(Ec2Service::respond(
        "GetIpamPoolCidrs",
        &req.request_id,
        &ec2_list("ipamPoolCidrSet", &items),
    ))
}

// ---- allocations ----

fn allocation_xml(cidr: &str, alloc_id: &str) -> String {
    format!(
        "{}{}<resourceType>custom</resourceType>",
        ec2_elem("cidr", cidr),
        ec2_elem("ipamPoolAllocationId", alloc_id)
    )
}

pub(crate) fn allocate_ipam_pool_cidr(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let pool = require(&req.query_params, "IpamPoolId")?;
    let cidr = req
        .query_params
        .get("Cidr")
        .cloned()
        .unwrap_or_else(|| "10.0.0.0/24".to_string());
    let alloc = gen_id("ipam-pool-alloc");
    {
        let mut accounts = svc.state.write();
        accounts
            .get_or_create(&req.account_id)
            .ipam_pool_allocations
            .entry(pool)
            .or_default()
            .push((cidr.clone(), alloc.clone()));
    }
    Ok(Ec2Service::respond(
        "AllocateIpamPoolCidr",
        &req.request_id,
        &format!(
            "<ipamPoolAllocation>{}</ipamPoolAllocation>",
            allocation_xml(&cidr, &alloc)
        ),
    ))
}

pub(crate) fn release_ipam_pool_allocation(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let pool = require(&req.query_params, "IpamPoolId")?;
    require(&req.query_params, "Cidr")?;
    let alloc = require(&req.query_params, "IpamPoolAllocationId")?;
    {
        let mut accounts = svc.state.write();
        if let Some(allocs) = accounts
            .get_or_create(&req.account_id)
            .ipam_pool_allocations
            .get_mut(&pool)
        {
            allocs.retain(|(_, a)| a != &alloc);
        }
    }
    Ok(Ec2Service::respond(
        "ReleaseIpamPoolAllocation",
        &req.request_id,
        "<success>true</success>",
    ))
}

pub(crate) fn get_ipam_pool_allocations(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let pool = require(&req.query_params, "IpamPoolId")?;
    validate_max_results(&req.query_params, 1000, 100000)?;
    let accounts = svc.state.read();
    let items: Vec<String> = accounts
        .get(&req.account_id)
        .and_then(|s| s.ipam_pool_allocations.get(&pool))
        .map(|allocs| allocs.iter().map(|(c, a)| allocation_xml(c, a)).collect())
        .unwrap_or_default();
    Ok(Ec2Service::respond(
        "GetIpamPoolAllocations",
        &req.request_id,
        &ec2_list("ipamPoolAllocationSet", &items),
    ))
}

// ---- resource CIDRs ----

pub(crate) fn get_ipam_resource_cidrs(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "IpamScopeId")?;
    validate_enum(
        &req.query_params,
        "ResourceType",
        &[
            "vpc",
            "subnet",
            "eip",
            "public-ipv4-pool",
            "ipv6-pool",
            "eni",
            "anycast-ip-list",
        ],
    )?;
    validate_max_results(&req.query_params, 5, 1000)?;
    Ok(Ec2Service::respond(
        "GetIpamResourceCidrs",
        &req.request_id,
        &ec2_list("ipamResourceCidrSet", &[]),
    ))
}

pub(crate) fn modify_ipam_resource_cidr(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let resource = require(&req.query_params, "ResourceId")?;
    let cidr = require(&req.query_params, "ResourceCidr")?;
    require(&req.query_params, "ResourceRegion")?;
    require(&req.query_params, "CurrentIpamScopeId")?;
    require(&req.query_params, "Monitored")?;
    let body = format!(
        "<ipamResourceCidr>{}{}<resourceType>vpc</resourceType></ipamResourceCidr>",
        ec2_elem("resourceId", &resource),
        ec2_elem("resourceCidr", &cidr)
    );
    Ok(Ec2Service::respond(
        "ModifyIpamResourceCidr",
        &req.request_id,
        &body,
    ))
}

pub(crate) fn get_ipam_address_history(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "Cidr")?;
    require(&req.query_params, "IpamScopeId")?;
    validate_max_results(&req.query_params, 1, 1000)?;
    Ok(Ec2Service::respond(
        "GetIpamAddressHistory",
        &req.request_id,
        &ec2_list("historyRecordSet", &[]),
    ))
}

// ---- organization admin ----

pub(crate) fn enable_ipam_organization_admin_account(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "DelegatedAdminAccountId")?;
    Ok(Ec2Service::respond(
        "EnableIpamOrganizationAdminAccount",
        &req.request_id,
        "<success>true</success>",
    ))
}

pub(crate) fn disable_ipam_organization_admin_account(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "DelegatedAdminAccountId")?;
    Ok(Ec2Service::respond(
        "DisableIpamOrganizationAdminAccount",
        &req.request_id,
        "<success>true</success>",
    ))
}
