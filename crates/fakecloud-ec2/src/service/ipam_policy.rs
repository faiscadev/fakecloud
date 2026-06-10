//! IPAM policies (allocation rules / org targets) and prefix-list resolvers
//! (+ targets, rules, versions).

use fakecloud_aws::ec2query::{ec2_elem, ec2_list, ec2_return};
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use crate::service::Ec2Service;
use crate::service_helpers::{gen_id, require, validate_enum, validate_max_results};
use crate::state::{
    Ec2State, IpamPolicy, IpamPrefixListResolver, IpamPrefixListResolverTarget, Tag,
};

fn mr(req: &AwsRequest) -> Result<(), AwsServiceError> {
    validate_max_results(&req.query_params, 5, 1000)
}

fn region_of(req: &AwsRequest) -> String {
    if req.region.is_empty() {
        "us-east-1".to_string()
    } else {
        req.region.clone()
    }
}

const RESOURCE_TYPES: &[&str] = &["alb", "eip", "rds", "rnat"];

// ---- policies ----

fn policy_xml(p: &IpamPolicy, tags: &[Tag], owner: &str, region: &str) -> String {
    format!(
        "{}{}{}{}<state>create-complete</state>{}",
        ec2_elem("ipamPolicyId", &p.id),
        ec2_elem(
            "ipamPolicyArn",
            &format!("arn:aws:ec2::{owner}:ipam-policy/{}", p.id)
        ),
        ec2_elem("ipamPolicyRegion", region),
        ec2_elem("ipamId", &p.ipam_id) + &ec2_elem("ownerId", owner),
        super::tags::tag_set_xml(tags),
    )
}

pub(crate) fn create_ipam_policy(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let ipam = require(&req.query_params, "IpamId")?;
    let id = gen_id("ipam-policy");
    let p = IpamPolicy {
        id: id.clone(),
        ipam_id: ipam,
    };
    let owner = req.account_id.clone();
    let region = region_of(req);
    let tags = {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        crate::service::tags::apply_tag_specifications(
            state,
            &req.query_params,
            &id,
            "ipam-policy",
        );
        let t = state.tags_for(&id).to_vec();
        state.ipam_policies.insert(id.clone(), p.clone());
        t
    };
    Ok(Ec2Service::respond(
        "CreateIpamPolicy",
        &req.request_id,
        &format!(
            "<ipamPolicy>{}</ipamPolicy>",
            policy_xml(&p, &tags, &owner, &region)
        ),
    ))
}

pub(crate) fn delete_ipam_policy(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "IpamPolicyId")?;
    let owner = req.account_id.clone();
    let region = region_of(req);
    let mut accounts = svc.state.write();
    let state = accounts.get_or_create(&req.account_id);
    let p = state.ipam_policies.remove(&id).unwrap_or(IpamPolicy {
        id: id.clone(),
        ipam_id: "ipam-0".to_string(),
    });
    let tags = state.tags_for(&id).to_vec();
    state.tags.remove(&id);
    Ok(Ec2Service::respond(
        "DeleteIpamPolicy",
        &req.request_id,
        &format!(
            "<ipamPolicy>{}</ipamPolicy>",
            policy_xml(&p, &tags, &owner, &region)
        ),
    ))
}

pub(crate) fn describe_ipam_policies(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    mr(req)?;
    let owner = req.account_id.clone();
    let region = region_of(req);
    let accounts = svc.state.read();
    let empty = Ec2State::new(&req.account_id, &req.region);
    let state = accounts.get(&req.account_id).unwrap_or(&empty);
    let mut items: Vec<String> = state
        .ipam_policies
        .values()
        .map(|p| policy_xml(p, state.tags_for(&p.id), &owner, &region))
        .collect();
    items.sort();
    Ok(Ec2Service::respond(
        "DescribeIpamPolicies",
        &req.request_id,
        &ec2_list("ipamPolicySet", &items),
    ))
}

pub(crate) fn enable_ipam_policy(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "IpamPolicyId")?;
    Ok(Ec2Service::respond(
        "EnableIpamPolicy",
        &req.request_id,
        &ec2_elem("ipamPolicyId", &id),
    ))
}

pub(crate) fn disable_ipam_policy(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "IpamPolicyId")?;
    Ok(Ec2Service::respond(
        "DisableIpamPolicy",
        &req.request_id,
        &ec2_return(true),
    ))
}

pub(crate) fn get_enabled_ipam_policy(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    Ok(Ec2Service::respond(
        "GetEnabledIpamPolicy",
        &req.request_id,
        "<ipamPolicyEnabled>false</ipamPolicyEnabled>",
    ))
}

pub(crate) fn get_ipam_policy_allocation_rules(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "IpamPolicyId")?;
    validate_enum(&req.query_params, "ResourceType", RESOURCE_TYPES)?;
    mr(req)?;
    Ok(Ec2Service::respond(
        "GetIpamPolicyAllocationRules",
        &req.request_id,
        &ec2_list("ipamPolicyDocumentSet", &[]),
    ))
}

pub(crate) fn modify_ipam_policy_allocation_rules(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "IpamPolicyId")?;
    let locale = require(&req.query_params, "Locale")?;
    let rt = require(&req.query_params, "ResourceType")?;
    validate_enum(&req.query_params, "ResourceType", RESOURCE_TYPES)?;
    let body = format!(
        "<ipamPolicyDocument>{}{}{}</ipamPolicyDocument>",
        ec2_elem("ipamPolicyId", &id),
        ec2_elem("locale", &locale),
        ec2_elem("resourceType", &rt),
    );
    Ok(Ec2Service::respond(
        "ModifyIpamPolicyAllocationRules",
        &req.request_id,
        &body,
    ))
}

pub(crate) fn get_ipam_policy_organization_targets(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "IpamPolicyId")?;
    mr(req)?;
    Ok(Ec2Service::respond(
        "GetIpamPolicyOrganizationTargets",
        &req.request_id,
        &ec2_list("organizationTargetSet", &[]),
    ))
}

// ---- prefix list resolvers ----

fn resolver_xml(r: &IpamPrefixListResolver, tags: &[Tag], owner: &str, region: &str) -> String {
    format!(
        "{}{}{}{}<addressFamily>{}</addressFamily><state>create-complete</state>{}",
        ec2_elem("ipamPrefixListResolverId", &r.id),
        ec2_elem(
            "ipamPrefixListResolverArn",
            &format!("arn:aws:ec2::{owner}:ipam-prefix-list-resolver/{}", r.id)
        ),
        ec2_elem(
            "ipamArn",
            &format!("arn:aws:ec2::{owner}:ipam/{}", r.ipam_id)
        ),
        ec2_elem("ipamRegion", region) + &ec2_elem("ownerId", owner),
        r.address_family,
        super::tags::tag_set_xml(tags),
    )
}

pub(crate) fn create_ipam_prefix_list_resolver(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let ipam = require(&req.query_params, "IpamId")?;
    let af = require(&req.query_params, "AddressFamily")?;
    validate_enum(&req.query_params, "AddressFamily", &["ipv4", "ipv6"])?;
    let id = gen_id("ipam-pl-res");
    let r = IpamPrefixListResolver {
        id: id.clone(),
        ipam_id: ipam,
        address_family: af,
    };
    let owner = req.account_id.clone();
    let region = region_of(req);
    let tags = {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        crate::service::tags::apply_tag_specifications(
            state,
            &req.query_params,
            &id,
            "ipam-prefix-list-resolver",
        );
        let t = state.tags_for(&id).to_vec();
        state.ipam_pl_resolvers.insert(id.clone(), r.clone());
        t
    };
    Ok(Ec2Service::respond(
        "CreateIpamPrefixListResolver",
        &req.request_id,
        &format!(
            "<ipamPrefixListResolver>{}</ipamPrefixListResolver>",
            resolver_xml(&r, &tags, &owner, &region)
        ),
    ))
}

pub(crate) fn delete_ipam_prefix_list_resolver(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "IpamPrefixListResolverId")?;
    let owner = req.account_id.clone();
    let region = region_of(req);
    let mut accounts = svc.state.write();
    let state = accounts.get_or_create(&req.account_id);
    let r = state
        .ipam_pl_resolvers
        .remove(&id)
        .unwrap_or(IpamPrefixListResolver {
            id: id.clone(),
            ipam_id: "ipam-0".to_string(),
            address_family: "ipv4".to_string(),
        });
    let tags = state.tags_for(&id).to_vec();
    state.tags.remove(&id);
    Ok(Ec2Service::respond(
        "DeleteIpamPrefixListResolver",
        &req.request_id,
        &format!(
            "<ipamPrefixListResolver>{}</ipamPrefixListResolver>",
            resolver_xml(&r, &tags, &owner, &region)
        ),
    ))
}

pub(crate) fn describe_ipam_prefix_list_resolvers(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    mr(req)?;
    let owner = req.account_id.clone();
    let region = region_of(req);
    let accounts = svc.state.read();
    let empty = Ec2State::new(&req.account_id, &req.region);
    let state = accounts.get(&req.account_id).unwrap_or(&empty);
    let mut items: Vec<String> = state
        .ipam_pl_resolvers
        .values()
        .map(|r| resolver_xml(r, state.tags_for(&r.id), &owner, &region))
        .collect();
    items.sort();
    Ok(Ec2Service::respond(
        "DescribeIpamPrefixListResolvers",
        &req.request_id,
        &ec2_list("ipamPrefixListResolverSet", &items),
    ))
}

pub(crate) fn modify_ipam_prefix_list_resolver(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "IpamPrefixListResolverId")?;
    let owner = req.account_id.clone();
    let region = region_of(req);
    let accounts = svc.state.read();
    let r = accounts
        .get(&req.account_id)
        .and_then(|s| s.ipam_pl_resolvers.get(&id).cloned())
        .unwrap_or(IpamPrefixListResolver {
            id: id.clone(),
            ipam_id: "ipam-0".to_string(),
            address_family: "ipv4".to_string(),
        });
    let tags = accounts
        .get(&req.account_id)
        .map(|s| s.tags_for(&id).to_vec())
        .unwrap_or_default();
    Ok(Ec2Service::respond(
        "ModifyIpamPrefixListResolver",
        &req.request_id,
        &format!(
            "<ipamPrefixListResolver>{}</ipamPrefixListResolver>",
            resolver_xml(&r, &tags, &owner, &region)
        ),
    ))
}

fn target_xml(t: &IpamPrefixListResolverTarget, tags: &[Tag], owner: &str) -> String {
    format!(
        "{}{}{}{}{}<trackLatestVersion>{}</trackLatestVersion><state>create-complete</state>{}",
        ec2_elem("ipamPrefixListResolverTargetId", &t.id),
        ec2_elem(
            "ipamPrefixListResolverTargetArn",
            &format!(
                "arn:aws:ec2::{owner}:ipam-prefix-list-resolver-target/{}",
                t.id
            )
        ),
        ec2_elem("ipamPrefixListResolverId", &t.resolver_id),
        ec2_elem("prefixListId", &t.prefix_list_id),
        ec2_elem("prefixListRegion", &t.prefix_list_region) + &ec2_elem("ownerId", owner),
        t.track_latest_version,
        super::tags::tag_set_xml(tags),
    )
}

pub(crate) fn create_ipam_prefix_list_resolver_target(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let resolver = require(&req.query_params, "IpamPrefixListResolverId")?;
    let pl = require(&req.query_params, "PrefixListId")?;
    let pl_region = require(&req.query_params, "PrefixListRegion")?;
    let track = require(&req.query_params, "TrackLatestVersion")?;
    let id = gen_id("ipam-pl-res-target");
    let t = IpamPrefixListResolverTarget {
        id: id.clone(),
        resolver_id: resolver,
        prefix_list_id: pl,
        prefix_list_region: pl_region,
        track_latest_version: track,
    };
    let owner = req.account_id.clone();
    let tags = {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        crate::service::tags::apply_tag_specifications(
            state,
            &req.query_params,
            &id,
            "ipam-prefix-list-resolver-target",
        );
        let t2 = state.tags_for(&id).to_vec();
        state.ipam_pl_resolver_targets.insert(id.clone(), t.clone());
        t2
    };
    Ok(Ec2Service::respond(
        "CreateIpamPrefixListResolverTarget",
        &req.request_id,
        &format!(
            "<ipamPrefixListResolverTarget>{}</ipamPrefixListResolverTarget>",
            target_xml(&t, &tags, &owner)
        ),
    ))
}

pub(crate) fn delete_ipam_prefix_list_resolver_target(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "IpamPrefixListResolverTargetId")?;
    let owner = req.account_id.clone();
    let mut accounts = svc.state.write();
    let state = accounts.get_or_create(&req.account_id);
    let t = state
        .ipam_pl_resolver_targets
        .remove(&id)
        .unwrap_or(IpamPrefixListResolverTarget {
            id: id.clone(),
            resolver_id: "ipam-pl-res-0".to_string(),
            prefix_list_id: "pl-0".to_string(),
            prefix_list_region: "us-east-1".to_string(),
            track_latest_version: "true".to_string(),
        });
    let tags = state.tags_for(&id).to_vec();
    state.tags.remove(&id);
    Ok(Ec2Service::respond(
        "DeleteIpamPrefixListResolverTarget",
        &req.request_id,
        &format!(
            "<ipamPrefixListResolverTarget>{}</ipamPrefixListResolverTarget>",
            target_xml(&t, &tags, &owner)
        ),
    ))
}

pub(crate) fn describe_ipam_prefix_list_resolver_targets(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    mr(req)?;
    let owner = req.account_id.clone();
    let accounts = svc.state.read();
    let empty = Ec2State::new(&req.account_id, &req.region);
    let state = accounts.get(&req.account_id).unwrap_or(&empty);
    let mut items: Vec<String> = state
        .ipam_pl_resolver_targets
        .values()
        .map(|t| target_xml(t, state.tags_for(&t.id), &owner))
        .collect();
    items.sort();
    Ok(Ec2Service::respond(
        "DescribeIpamPrefixListResolverTargets",
        &req.request_id,
        &ec2_list("ipamPrefixListResolverTargetSet", &items),
    ))
}

pub(crate) fn modify_ipam_prefix_list_resolver_target(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "IpamPrefixListResolverTargetId")?;
    let owner = req.account_id.clone();
    let accounts = svc.state.read();
    let t = accounts
        .get(&req.account_id)
        .and_then(|s| s.ipam_pl_resolver_targets.get(&id).cloned())
        .unwrap_or(IpamPrefixListResolverTarget {
            id: id.clone(),
            resolver_id: "ipam-pl-res-0".to_string(),
            prefix_list_id: "pl-0".to_string(),
            prefix_list_region: "us-east-1".to_string(),
            track_latest_version: "true".to_string(),
        });
    let tags = accounts
        .get(&req.account_id)
        .map(|s| s.tags_for(&id).to_vec())
        .unwrap_or_default();
    Ok(Ec2Service::respond(
        "ModifyIpamPrefixListResolverTarget",
        &req.request_id,
        &format!(
            "<ipamPrefixListResolverTarget>{}</ipamPrefixListResolverTarget>",
            target_xml(&t, &tags, &owner)
        ),
    ))
}

pub(crate) fn get_ipam_prefix_list_resolver_rules(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "IpamPrefixListResolverId")?;
    mr(req)?;
    Ok(Ec2Service::respond(
        "GetIpamPrefixListResolverRules",
        &req.request_id,
        &ec2_list("ruleSet", &[]),
    ))
}

pub(crate) fn get_ipam_prefix_list_resolver_versions(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "IpamPrefixListResolverId")?;
    mr(req)?;
    Ok(Ec2Service::respond(
        "GetIpamPrefixListResolverVersions",
        &req.request_id,
        &ec2_list("ipamPrefixListResolverVersionSet", &[]),
    ))
}

pub(crate) fn get_ipam_prefix_list_resolver_version_entries(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "IpamPrefixListResolverId")?;
    require(&req.query_params, "IpamPrefixListResolverVersion")?;
    mr(req)?;
    Ok(Ec2Service::respond(
        "GetIpamPrefixListResolverVersionEntries",
        &req.request_id,
        &ec2_list("entrySet", &[]),
    ))
}
