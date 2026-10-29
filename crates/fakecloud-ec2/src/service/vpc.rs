//! VPC operations: create/delete/describe, attributes, tenancy, and secondary
//! CIDR-block association.

use fakecloud_aws::ec2query::{ec2_elem, ec2_list, ec2_return};
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use crate::service::resource_quotas as rq;
use crate::service::Ec2Service;
use crate::service_helpers::{
    filter_value_matches, gen_id, indexed_list, not_found, paginate, parse_filters, require,
    validate_enum, validate_max_results, Filter,
};
use crate::state::{Ec2State, Tag, Vpc, VpcCidrAssoc};

/// The id part of a VPC id (`vpc-0abc -> 0abc`). Never byte-slices, which
/// panics on an id shorter than the prefix or off a UTF-8 boundary.
fn vpc_suffix(vpc_id: &str) -> &str {
    vpc_id.strip_prefix("vpc-").unwrap_or(vpc_id)
}

/// Association id of a VPC's primary IPv4 CIDR block.
fn primary_cidr_assoc_id(vpc_id: &str) -> String {
    format!("vpc-cidr-assoc-{}", vpc_suffix(vpc_id))
}

/// Association id of a VPC's first IPv6 CIDR block.
fn primary_ipv6_assoc_id(vpc_id: &str) -> String {
    format!("vpc-cidr-assoc-ipv6-{}", vpc_suffix(vpc_id))
}

/// Every IPv6 CIDR block associated with a VPC: the first one (kept in
/// `ipv6_cidr_block`) and the ones associated after it.
fn ipv6_assocs(vpc: &Vpc) -> Vec<VpcCidrAssoc> {
    vpc.ipv6_cidr_block
        .iter()
        .map(|cidr| VpcCidrAssoc {
            association_id: primary_ipv6_assoc_id(&vpc.vpc_id),
            cidr_block: cidr.clone(),
            state: "associated".to_string(),
        })
        .chain(vpc.ipv6_cidr_associations.iter().cloned())
        .collect()
}

/// Render the inner XML of a `<vpc>` / `<vpcSet><item>` element (lowerCamel
/// wire names, the casing the AWS SDK deserializer expects).
pub(crate) fn vpc_xml(vpc: &Vpc, tags: &[Tag], owner_id: &str) -> String {
    let cidr_assocs: Vec<String> = std::iter::once(VpcCidrAssoc {
        association_id: primary_cidr_assoc_id(&vpc.vpc_id),
        cidr_block: vpc.cidr_block.clone(),
        state: "associated".to_string(),
    })
    .chain(vpc.cidr_associations.iter().cloned())
    .map(|a| {
        format!(
            "{}{}<cidrBlockState><state>{}</state></cidrBlockState>",
            ec2_elem("associationId", &a.association_id),
            ec2_elem("cidrBlock", &a.cidr_block),
            a.state,
        )
    })
    .collect();

    // Amazon-provided IPv6 CIDRs are reported in the
    // `ipv6CidrBlockAssociationSet`. The `aws_vpc` resource reads
    // `ipv6_cidr_block` (and infers `assign_generated_ipv6_cidr_block`) from
    // the first one.
    let ipv6_items: Vec<String> = ipv6_assocs(vpc)
        .iter()
        .map(|a| {
            format!(
                "{}{}<ipv6CidrBlockState><state>{}</state></ipv6CidrBlockState>{}",
                ec2_elem("associationId", &a.association_id),
                ec2_elem("ipv6CidrBlock", &a.cidr_block),
                a.state,
                ec2_elem("ipv6Pool", "Amazon"),
            )
        })
        .collect();
    let ipv6_set = if ipv6_items.is_empty() {
        String::new()
    } else {
        ec2_list("ipv6CidrBlockAssociationSet", &ipv6_items)
    };

    format!(
        "{}{}{}{}{}{}{}{}{}{}",
        ec2_elem("vpcId", &vpc.vpc_id),
        ec2_elem("state", &vpc.state),
        ec2_elem("cidrBlock", &vpc.cidr_block),
        ec2_elem("dhcpOptionsId", &vpc.dhcp_options_id),
        ec2_elem("instanceTenancy", &vpc.instance_tenancy),
        format_args!("<isDefault>{}</isDefault>", vpc.is_default),
        ec2_elem("ownerId", owner_id),
        ec2_list("cidrBlockAssociationSet", &cidr_assocs),
        ipv6_set,
        super::tags::tag_set_xml(tags),
    )
}

/// Deterministic Amazon-style IPv6 /56 for a VPC. Real Amazon CIDRs come from
/// `2600:1f00::/24`; we derive a stable suffix from the VPC id so reads are
/// consistent. A /56 puts the network boundary at the high byte of the 4th
/// hextet, so the low byte (and everything after) MUST be zero — otherwise the
/// CIDR has host bits set and Terraform's `cidrsubnet()` rejects it.
fn generated_ipv6_cidr(vpc_id: &str) -> String {
    let h = vpc_id
        .bytes()
        .fold(0u32, |acc, b| acc.wrapping_mul(31).wrapping_add(b as u32));
    format!(
        "2600:1f16:{:04x}:{:02x}00::/56",
        (h >> 16) & 0xffff,
        h & 0xff
    )
}

/// Shared `Filter.N` matcher for VPC describe: supports `vpc-id`, `cidr`,
/// CIDR associations, `dhcp-options-id`, `isDefault`, `state`, and tags.
fn vpc_matches(vpc: &Vpc, tags: &[Tag], filters: &[Filter]) -> bool {
    filters.iter().all(|f| {
        let candidates: Vec<String> = match f.name.as_str() {
            "vpc-id" => vec![vpc.vpc_id.clone()],
            "cidr" => vec![vpc.cidr_block.clone()],
            "cidr-block-association.cidr-block" => std::iter::once(vpc.cidr_block.clone())
                .chain(vpc.cidr_associations.iter().map(|a| a.cidr_block.clone()))
                .collect(),
            // Terraform uses this filter to poll and refresh secondary CIDRs.
            // Include the primary association with the same id as vpc_xml.
            "cidr-block-association.association-id" => {
                std::iter::once(primary_cidr_assoc_id(&vpc.vpc_id))
                    .chain(
                        vpc.cidr_associations
                            .iter()
                            .map(|a| a.association_id.clone()),
                    )
                    .collect()
            }
            "dhcp-options-id" => vec![vpc.dhcp_options_id.clone()],
            "state" => vec![vpc.state.clone()],
            "isDefault" | "is-default" => vec![vpc.is_default.to_string()],
            "ipv6-cidr-block-association.association-id" => ipv6_assocs(vpc)
                .into_iter()
                .map(|a| a.association_id)
                .collect(),
            "ipv6-cidr-block-association.ipv6-cidr-block" => {
                ipv6_assocs(vpc).into_iter().map(|a| a.cidr_block).collect()
            }
            "tag-key" => tags.iter().map(|t| t.key.clone()).collect(),
            "tag-value" => tags.iter().map(|t| t.value.clone()).collect(),
            name => {
                if let Some(key) = name.strip_prefix("tag:") {
                    tags.iter()
                        .filter(|t| t.key == key)
                        .map(|t| t.value.clone())
                        .collect()
                } else {
                    return false; // unknown filter: match nothing
                }
            }
        };
        f.values
            .iter()
            .any(|v| candidates.iter().any(|c| filter_value_matches(v, c)))
    })
}

pub(crate) fn create_vpc(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    validate_enum(
        &req.query_params,
        "InstanceTenancy",
        &["default", "dedicated", "host"],
    )?;
    let cidr = req
        .query_params
        .get("CidrBlock")
        .cloned()
        .unwrap_or_else(|| "10.0.0.0/16".to_string());
    let tenancy = req
        .query_params
        .get("InstanceTenancy")
        .cloned()
        .unwrap_or_else(|| "default".to_string());
    let mut vpc = build_vpc(cidr, tenancy, false);
    let vpc_id = vpc.vpc_id.clone();
    // `AmazonProvidedIpv6CidrBlock=true` requests an Amazon-assigned /56; the
    // `aws_vpc` resource then expects `ipv6_cidr_block` to be set.
    if req
        .query_params
        .get("AmazonProvidedIpv6CidrBlock")
        .map(|v| v == "true")
        .unwrap_or(false)
    {
        vpc.ipv6_cidr_block = Some(generated_ipv6_cidr(&vpc_id));
        vpc.ipv6_blocks_minted = 1;
    }

    let owner = req.account_id.clone();
    // Only "VPCs per Region" holds CreateVpc back; the default security
    // group, network ACL and main route table it provisions count toward
    // their quotas' usage without refusing the VPC.
    let limit = svc.enforced_count_quota(&req.account_id, &req.region, rq::VPCS_PER_REGION);
    let body = {
        let mut accounts = svc.state.write();
        let state = accounts.regional_mut(&req.account_id, &req.region);
        check_vpc_count(state, limit)?;
        crate::service::tags::apply_tag_specifications(state, &req.query_params, &vpc_id, "vpc");
        let tags = state.tags_for(&vpc_id).to_vec();
        state.vpcs.insert(vpc_id.clone(), vpc.clone());
        // AWS provisions a default SG, default NACL, and main route table for
        // every new VPC; the `aws_vpc` resource reads their ids back.
        crate::defaults::create_vpc_default_resources(state, &vpc_id, &vpc.cidr_block);
        format!("<vpc>{}</vpc>", vpc_xml(&vpc, &tags, &owner))
    };
    Ok(Ec2Service::respond("CreateVpc", &req.request_id, &body))
}

pub(crate) fn create_default_vpc(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    // Every account already ships a seeded default VPC (bootstrapped at state
    // construction), so CreateDefaultVpc must return THAT one, not mint a
    // second `isDefault=true` VPC — a state impossible on AWS, which returns
    // `DefaultVpcAlreadyExists` when one exists (bug-hunt 2026-06-18 finding
    // 1.3). We surface the existing default rather than the error so callers
    // that defensively call CreateDefaultVpc keep working.
    let owner = req.account_id.clone();
    let limit = svc.enforced_count_quota(&req.account_id, &req.region, rq::VPCS_PER_REGION);
    let body = {
        let mut accounts = svc.state.write();
        let state = accounts.regional_mut(&req.account_id, &req.region);
        let vpc = match state.vpcs.values().find(|v| v.is_default).cloned() {
            Some(v) => v,
            // No default VPC somehow (e.g. it was deleted): re-create one,
            // which counts toward the VPC quota like any other.
            None => {
                check_vpc_count(state, limit)?;
                let v = build_vpc("172.31.0.0/16".to_string(), "default".to_string(), true);
                state.vpcs.insert(v.vpc_id.clone(), v.clone());
                v
            }
        };
        let tags = state.tags_for(&vpc.vpc_id).to_vec();
        format!("<vpc>{}</vpc>", vpc_xml(&vpc, &tags, &owner))
    };
    Ok(Ec2Service::respond(
        "CreateDefaultVpc",
        &req.request_id,
        &body,
    ))
}

/// `VpcLimitExceeded` once the account holds as many VPCs as "VPCs per
/// Region" allows.
fn check_vpc_count(state: &Ec2State, limit: Option<usize>) -> Result<(), AwsServiceError> {
    rq::check(limit, rq::vpcs(state), 1, "VpcLimitExceeded", |_| {
        "The maximum number of VPCs has been reached.".to_string()
    })
}

fn build_vpc(cidr: String, tenancy: String, is_default: bool) -> Vpc {
    Vpc {
        vpc_id: gen_id("vpc"),
        cidr_block: cidr,
        state: "available".to_string(),
        dhcp_options_id: "default".to_string(),
        instance_tenancy: tenancy,
        is_default,
        enable_dns_support: true,
        enable_dns_hostnames: is_default,
        enable_network_address_usage_metrics: false,
        cidr_associations: Vec::new(),
        ipv6_cidr_block: None,
        ipv6_cidr_associations: Vec::new(),
        ipv6_blocks_minted: 0,
    }
}

pub(crate) fn delete_vpc(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "VpcId")?;
    {
        let mut accounts = svc.state.write();
        let state = accounts.regional_mut(&req.account_id, &req.region);
        state.vpcs.remove(&id);
        state.tags.remove(&id);
    }
    Ok(Ec2Service::respond(
        "DeleteVpc",
        &req.request_id,
        &ec2_return(true),
    ))
}

pub(crate) fn describe_vpcs(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    validate_max_results(&req.query_params, 5, 1000)?;
    let filters = parse_filters(&req.query_params);
    let wanted = indexed_list(&req.query_params, "VpcId");
    let owner = req.account_id.clone();

    let accounts = svc.state.read();
    let empty = Ec2State::new(&req.account_id, &req.region);
    let state = accounts
        .regional(&req.account_id, &req.region)
        .unwrap_or(&empty);

    // An explicitly-requested VpcId that does not exist is a hard error on AWS
    // (InvalidVpcID.NotFound), not a silently-empty result.
    for id in &wanted {
        if !state.vpcs.contains_key(id) {
            return Err(not_found("InvalidVpcID.NotFound", id));
        }
    }

    let mut items: Vec<String> = state
        .vpcs
        .values()
        .filter(|v| wanted.is_empty() || wanted.contains(&v.vpc_id))
        .filter(|v| vpc_matches(v, state.tags_for(&v.vpc_id), &filters))
        .map(|v| vpc_xml(v, state.tags_for(&v.vpc_id), &owner))
        .collect();
    items.sort();

    // DescribeVpcs is a `paginated` operation in the model; honor MaxResults +
    // NextToken instead of always returning the full set.
    let max_results = req
        .query_params
        .get("MaxResults")
        .filter(|v| !v.is_empty())
        .and_then(|v| v.parse::<usize>().ok());
    let next_token = req.query_params.get("NextToken").map(String::as_str);
    let (page, token) = paginate(&items, next_token, max_results)?;
    let body = format!(
        "{}{}",
        ec2_list("vpcSet", &page),
        token.map(|t| ec2_elem("nextToken", &t)).unwrap_or_default(),
    );
    Ok(Ec2Service::respond("DescribeVpcs", &req.request_id, &body))
}

pub(crate) fn modify_vpc_attribute(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let vpc_id = require(&req.query_params, "VpcId")?;
    let mut accounts = svc.state.write();
    let state = accounts.regional_mut(&req.account_id, &req.region);
    if let Some(vpc) = state.vpcs.get_mut(&vpc_id) {
        if let Some(v) = bool_attr(&req.query_params, "EnableDnsSupport.Value") {
            vpc.enable_dns_support = v;
        }
        if let Some(v) = bool_attr(&req.query_params, "EnableDnsHostnames.Value") {
            vpc.enable_dns_hostnames = v;
        }
        if let Some(v) = bool_attr(&req.query_params, "EnableNetworkAddressUsageMetrics.Value") {
            vpc.enable_network_address_usage_metrics = v;
        }
    }
    Ok(Ec2Service::respond(
        "ModifyVpcAttribute",
        &req.request_id,
        &ec2_return(true),
    ))
}

pub(crate) fn describe_vpc_attribute(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let vpc_id = require(&req.query_params, "VpcId")?;
    let attribute = require(&req.query_params, "Attribute")?;
    validate_enum(
        &req.query_params,
        "Attribute",
        &[
            "enableDnsSupport",
            "enableDnsHostnames",
            "enableNetworkAddressUsageMetrics",
        ],
    )?;

    // Lenient: a well-formed VpcId that isn't in state returns attribute
    // defaults rather than NotFound (EC2 declares no error shapes, so the
    // probe's positive variants must not 4xx).
    let accounts = svc.state.read();
    let empty = Ec2State::new(&req.account_id, &req.region);
    let state = accounts
        .regional(&req.account_id, &req.region)
        .unwrap_or(&empty);
    let (dns_support, dns_hostnames, nau_metrics) = state
        .vpcs
        .get(&vpc_id)
        .map(|v| {
            (
                v.enable_dns_support,
                v.enable_dns_hostnames,
                v.enable_network_address_usage_metrics,
            )
        })
        .unwrap_or((true, false, false));

    let attr_xml = match attribute.as_str() {
        "enableDnsHostnames" => {
            format!("<enableDnsHostnames><value>{dns_hostnames}</value></enableDnsHostnames>")
        }
        "enableNetworkAddressUsageMetrics" => {
            format!("<enableNetworkAddressUsageMetrics><value>{nau_metrics}</value></enableNetworkAddressUsageMetrics>")
        }
        _ => format!("<enableDnsSupport><value>{dns_support}</value></enableDnsSupport>"),
    };
    let body = format!("{}{}", ec2_elem("vpcId", &vpc_id), attr_xml);
    Ok(Ec2Service::respond(
        "DescribeVpcAttribute",
        &req.request_id,
        &body,
    ))
}

pub(crate) fn modify_vpc_tenancy(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let vpc_id = require(&req.query_params, "VpcId")?;
    let tenancy = require(&req.query_params, "InstanceTenancy")?;
    validate_enum(&req.query_params, "InstanceTenancy", &["default"])?;
    {
        let mut accounts = svc.state.write();
        let state = accounts.regional_mut(&req.account_id, &req.region);
        if let Some(vpc) = state.vpcs.get_mut(&vpc_id) {
            vpc.instance_tenancy = tenancy;
        }
    }
    Ok(Ec2Service::respond(
        "ModifyVpcTenancy",
        &req.request_id,
        &ec2_return(true),
    ))
}

pub(crate) fn associate_vpc_cidr_block(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let vpc_id = require(&req.query_params, "VpcId")?;
    let cidr = req.query_params.get("CidrBlock").cloned();

    // IPv6: `AmazonProvidedIpv6CidrBlock=true` assigns a /56. The resource
    // waits (DescribeVpcs) for the returned association id to show up, so the
    // id here must match the one vpc_xml renders.
    if req
        .query_params
        .get("AmazonProvidedIpv6CidrBlock")
        .map(|v| v == "true")
        .unwrap_or(false)
    {
        // The VPC's first block ever keeps its id derived from the VPC id;
        // every later one (a VPC holds up to five) gets a freshly minted
        // association id and a /56 not issued to it before, even when it
        // replaces a disassociated block.
        let limit =
            svc.enforced_count_quota(&req.account_id, &req.region, rq::IPV6_CIDR_BLOCKS_PER_VPC);
        let border_group = req
            .query_params
            .get("Ipv6CidrBlockNetworkBorderGroup")
            .filter(|g| !g.is_empty())
            .cloned()
            .unwrap_or_else(|| req.region.clone());
        let (assoc_id, ipv6) = {
            let mut accounts = svc.state.write();
            let state = accounts.regional_mut(&req.account_id, &req.region);
            match state.vpcs.get_mut(&vpc_id) {
                Some(vpc) => {
                    let existing = ipv6_assocs(vpc);
                    rq::check(limit, existing.len(), 1, "CidrLimitExceeded", |limit| {
                        format!(
                            "The network '{vpc_id}' has met its maximum number of allowed CIDRs ( {limit} ) for network border group '{border_group}'"
                        )
                    })?;
                    let issued = vpc.ipv6_blocks_issued();
                    if issued == 0 && vpc.ipv6_cidr_block.is_none() {
                        let ipv6 = generated_ipv6_cidr(&vpc_id);
                        vpc.ipv6_cidr_block = Some(ipv6.clone());
                        vpc.ipv6_blocks_minted = 1;
                        (primary_ipv6_assoc_id(&vpc_id), ipv6)
                    } else {
                        let (n, ipv6) = (issued.max(1)..)
                            .map(|n| (n, generated_ipv6_cidr(&format!("{vpc_id}/{n}"))))
                            .find(|(_, c)| existing.iter().all(|a| &a.cidr_block != c))
                            .expect("an unbounded sequence yields a free block");
                        vpc.ipv6_blocks_minted = n.saturating_add(1);
                        let assoc = VpcCidrAssoc {
                            association_id: gen_id("vpc-cidr-assoc"),
                            cidr_block: ipv6.clone(),
                            state: "associated".to_string(),
                        };
                        let id = assoc.association_id.clone();
                        vpc.ipv6_cidr_associations.push(assoc);
                        (id, ipv6)
                    }
                }
                // Lenient: an unknown VPC still gets a well-formed response
                // (random-id positive variants must not 4xx).
                None => (primary_ipv6_assoc_id(&vpc_id), generated_ipv6_cidr(&vpc_id)),
            }
        };
        let body = format!(
            "{}<ipv6CidrBlockAssociation>{}{}{}<ipv6CidrBlockState><state>associated</state></ipv6CidrBlockState></ipv6CidrBlockAssociation>",
            ec2_elem("vpcId", &vpc_id),
            ec2_elem("associationId", &assoc_id),
            ec2_elem("ipv6CidrBlock", &ipv6),
            ec2_elem("ipv6Pool", "Amazon"),
        );
        return Ok(Ec2Service::respond(
            "AssociateVpcCidrBlock",
            &req.request_id,
            &body,
        ));
    }

    let assoc = VpcCidrAssoc {
        association_id: gen_id("vpc-cidr-assoc"),
        cidr_block: cidr.clone().unwrap_or_else(|| "10.1.0.0/16".to_string()),
        state: "associated".to_string(),
    };
    // Lenient: record on the VPC when it exists, but always return a valid
    // association response (random-id positive variants must not 4xx).
    let limit =
        svc.enforced_count_quota(&req.account_id, &req.region, rq::IPV4_CIDR_BLOCKS_PER_VPC);
    {
        let mut accounts = svc.state.write();
        let state = accounts.regional_mut(&req.account_id, &req.region);
        if let Some(vpc) = state.vpcs.get_mut(&vpc_id) {
            // A new block must not overlap (equal, contain or sit inside) any
            // block already associated with the VPC, primary included.
            let overlaps = std::iter::once(vpc.cidr_block.as_str())
                .chain(
                    vpc.cidr_associations
                        .iter()
                        .filter(|a| a.state == "associated" || a.state == "associating")
                        .map(|a| a.cidr_block.as_str()),
                )
                .any(|existing| crate::vpc_lookup::ipv4_cidrs_overlap(existing, &assoc.cidr_block));
            if overlaps {
                return Err(AwsServiceError::aws_error(
                    http::StatusCode::BAD_REQUEST,
                    "InvalidVpc.Range",
                    format!(
                        "The CIDR '{}' conflicts with another CIDR block associated with vpc '{vpc_id}'.",
                        assoc.cidr_block,
                    ),
                ));
            }
            rq::check(
                limit,
                rq::ipv4_cidr_blocks(vpc),
                1,
                "CidrLimitExceeded",
                |limit| {
                    format!("This network '{vpc_id}' has met its maximum number of allowed CIDRs: {limit}")
                },
            )?;
            vpc.cidr_associations.push(assoc.clone());
        }
    }

    let body = format!(
        "{}<cidrBlockAssociation>{}{}<cidrBlockState><state>{}</state></cidrBlockState></cidrBlockAssociation>",
        ec2_elem("vpcId", &vpc_id),
        ec2_elem("associationId", &assoc.association_id),
        ec2_elem("cidrBlock", &assoc.cidr_block),
        assoc.state,
    );
    Ok(Ec2Service::respond(
        "AssociateVpcCidrBlock",
        &req.request_id,
        &body,
    ))
}

pub(crate) fn disassociate_vpc_cidr_block(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let assoc_id = require(&req.query_params, "AssociationId")?;

    // An IPv6 association answers with `ipv6CidrBlockAssociation`.
    let ipv6 = {
        let mut accounts = svc.state.write();
        let state = accounts.regional_mut(&req.account_id, &req.region);
        state.vpcs.values_mut().find_map(|vpc| {
            // Remember how many blocks were issued before releasing one, so
            // a later association never reuses its id or /56.
            if vpc.ipv6_cidr_block.is_some() && primary_ipv6_assoc_id(&vpc.vpc_id) == assoc_id {
                vpc.ipv6_blocks_minted = vpc.ipv6_blocks_issued();
                let cidr = vpc.ipv6_cidr_block.take()?;
                return Some((vpc.vpc_id.clone(), cidr));
            }
            let pos = vpc
                .ipv6_cidr_associations
                .iter()
                .position(|a| a.association_id == assoc_id)?;
            vpc.ipv6_blocks_minted = vpc.ipv6_blocks_issued();
            let a = vpc.ipv6_cidr_associations.remove(pos);
            Some((vpc.vpc_id.clone(), a.cidr_block))
        })
    };
    if let Some((vpc_id, cidr)) = ipv6 {
        let body = format!(
            "{}<ipv6CidrBlockAssociation>{}{}<ipv6CidrBlockState><state>disassociating</state></ipv6CidrBlockState></ipv6CidrBlockAssociation>",
            ec2_elem("vpcId", &vpc_id),
            ec2_elem("associationId", &assoc_id),
            ec2_elem("ipv6CidrBlock", &cidr),
        );
        return Ok(Ec2Service::respond(
            "DisassociateVpcCidrBlock",
            &req.request_id,
            &body,
        ));
    }

    let mut found: Option<(String, VpcCidrAssoc)> = None;
    {
        let mut accounts = svc.state.write();
        let state = accounts.regional_mut(&req.account_id, &req.region);
        for vpc in state.vpcs.values_mut() {
            if let Some(pos) = vpc
                .cidr_associations
                .iter()
                .position(|a| a.association_id == assoc_id)
            {
                let mut a = vpc.cidr_associations.remove(pos);
                a.state = "disassociated".to_string();
                found = Some((vpc.vpc_id.clone(), a));
                break;
            }
        }
    }
    // Lenient: synthesize a disassociating response for an unknown id.
    let (vpc_id, assoc) = found.unwrap_or_else(|| {
        (
            "vpc-00000000000000000".to_string(),
            VpcCidrAssoc {
                association_id: assoc_id.clone(),
                cidr_block: "10.0.0.0/16".to_string(),
                state: "disassociating".to_string(),
            },
        )
    });

    let body = format!(
        "{}<cidrBlockAssociation>{}{}<cidrBlockState><state>{}</state></cidrBlockState></cidrBlockAssociation>",
        ec2_elem("vpcId", &vpc_id),
        ec2_elem("associationId", &assoc.association_id),
        ec2_elem("cidrBlock", &assoc.cidr_block),
        assoc.state,
    );
    Ok(Ec2Service::respond(
        "DisassociateVpcCidrBlock",
        &req.request_id,
        &body,
    ))
}

fn bool_attr(params: &std::collections::HashMap<String, String>, key: &str) -> Option<bool> {
    params.get(key).map(|v| v == "true")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{ec2_request as req, err_of};

    fn body_of(resp: AwsResponse) -> String {
        String::from_utf8(resp.body.expect_bytes().to_vec()).unwrap()
    }

    /// Create a VPC and return its id.
    fn make_vpc(svc: &Ec2Service, cidr: &str) -> String {
        let body = body_of(create_vpc(svc, &req("CreateVpc", &[("CidrBlock", cidr)])).unwrap());
        body.split("<vpcId>")
            .nth(1)
            .and_then(|s| s.split("</vpcId>").next())
            .unwrap()
            .to_string()
    }

    fn req_in(region: &str, action: &str, query: &[(&str, &str)]) -> AwsRequest {
        let mut r = req(action, query);
        r.region = region.to_string();
        r
    }

    fn make_vpc_in(svc: &Ec2Service, region: &str, cidr: &str) -> String {
        let body =
            body_of(create_vpc(svc, &req_in(region, "CreateVpc", &[("CidrBlock", cidr)])).unwrap());
        body.split("<vpcId>")
            .nth(1)
            .and_then(|s| s.split("</vpcId>").next())
            .unwrap()
            .to_string()
    }

    #[test]
    fn vpcs_are_region_isolated() {
        let svc = Ec2Service::new();
        let east_vpc = make_vpc_in(&svc, "us-east-1", "10.0.0.0/16");
        let west_vpc = make_vpc_in(&svc, "us-west-2", "10.1.0.0/16");

        let east_body =
            body_of(describe_vpcs(&svc, &req_in("us-east-1", "DescribeVpcs", &[])).unwrap());
        assert!(
            east_body.contains(&format!("<vpcId>{east_vpc}</vpcId>")),
            "us-east-1 VPC should appear in us-east-1 describe: {east_body}"
        );
        assert!(
            !east_body.contains(&format!("<vpcId>{west_vpc}</vpcId>")),
            "us-west-2 VPC should NOT appear in us-east-1 describe: {east_body}"
        );

        let west_body =
            body_of(describe_vpcs(&svc, &req_in("us-west-2", "DescribeVpcs", &[])).unwrap());
        assert!(
            west_body.contains(&format!("<vpcId>{west_vpc}</vpcId>")),
            "us-west-2 VPC should appear in us-west-2 describe: {west_body}"
        );
        assert!(
            !west_body.contains(&format!("<vpcId>{east_vpc}</vpcId>")),
            "us-east-1 VPC should NOT appear in us-west-2 describe: {west_body}"
        );
    }

    #[test]
    fn ipv4_cidr_association_filters_follow_lifecycle() {
        let svc = Ec2Service::new();
        let vpc_id = make_vpc(&svc, "10.0.0.0/16");
        let other = make_vpc(&svc, "10.2.0.0/16");
        let response = body_of(
            associate_vpc_cidr_block(
                &svc,
                &req(
                    "AssociateVpcCidrBlock",
                    &[("VpcId", &vpc_id), ("CidrBlock", "10.1.0.0/16")],
                ),
            )
            .unwrap(),
        );
        let association_id = response
            .split("<associationId>")
            .nth(1)
            .unwrap()
            .split("</associationId>")
            .next()
            .unwrap();
        let primary_id = format!("vpc-cidr-assoc-{}", &vpc_id[4..]);
        for (filter, value, matches) in [
            (
                "cidr-block-association.association-id",
                association_id,
                true,
            ),
            (
                "cidr-block-association.association-id",
                primary_id.as_str(),
                true,
            ),
            (
                "cidr-block-association.association-id",
                "vpc-cidr-assoc-missing",
                false,
            ),
            ("cidr-block-association.cidr-block", "10.1.0.0/16", true),
            ("cidr-block-association.cidr-block", "10.0.0.0/16", true),
            ("cidr", "10.1.0.0/16", false),
        ] {
            let body = body_of(
                describe_vpcs(
                    &svc,
                    &req(
                        "DescribeVpcs",
                        &[("Filter.1.Name", filter), ("Filter.1.Value.1", value)],
                    ),
                )
                .unwrap(),
            );
            assert_eq!(
                body.contains(&format!("<vpcId>{vpc_id}</vpcId>")),
                matches,
                "{filter}={value}: {body}"
            );
            assert!(!body.contains(&format!("<vpcId>{other}</vpcId>")), "{body}");
        }
        disassociate_vpc_cidr_block(
            &svc,
            &req(
                "DisassociateVpcCidrBlock",
                &[("AssociationId", association_id)],
            ),
        )
        .unwrap();
        for (filter, value) in [
            ("cidr-block-association.association-id", association_id),
            ("cidr-block-association.cidr-block", "10.1.0.0/16"),
        ] {
            let body = body_of(
                describe_vpcs(
                    &svc,
                    &req(
                        "DescribeVpcs",
                        &[("Filter.1.Name", filter), ("Filter.1.Value.1", value)],
                    ),
                )
                .unwrap(),
            );
            assert!(
                !body.contains(&format!("<vpcId>{vpc_id}</vpcId>")),
                "{body}"
            );
        }
    }

    #[test]
    fn overlapping_cidr_association_rejected() {
        let svc = Ec2Service::new();
        let vpc_id = make_vpc(&svc, "10.0.0.0/16");
        let assoc = |cidr: &str| {
            associate_vpc_cidr_block(
                &svc,
                &req(
                    "AssociateVpcCidrBlock",
                    &[("VpcId", &vpc_id), ("CidrBlock", cidr)],
                ),
            )
        };
        assert!(assoc("10.1.0.0/16").is_ok(), "first association succeeds");
        // The same secondary, the primary, a block inside either, and a block
        // inside neither but overlapping both are all refused.
        for cidr in ["10.1.0.0/16", "10.0.0.0/16", "10.0.128.0/24", "10.1.4.0/28"] {
            let err = err_of(assoc(cidr));
            assert_eq!(err.code(), "InvalidVpc.Range", "{cidr}");
            assert!(err.message().contains(cidr), "{}", err.message());
        }
        // A disjoint block is still accepted.
        assert!(assoc("10.2.0.0/16").is_ok());
        // Once disassociated, a block may be associated again.
        let id = {
            let accounts = svc.state.read();
            accounts.regional("000000000000", "us-east-1").unwrap().vpcs[&vpc_id]
                .cidr_associations
                .iter()
                .find(|a| a.cidr_block == "10.1.0.0/16")
                .unwrap()
                .association_id
                .clone()
        };
        disassociate_vpc_cidr_block(
            &svc,
            &req("DisassociateVpcCidrBlock", &[("AssociationId", &id)]),
        )
        .unwrap();
        assert!(assoc("10.1.0.0/16").is_ok());
    }

    /// A VPC holds several Amazon-provided IPv6 blocks, each with its own
    /// association id and /56; DescribeVpcs reports them all, and each can be
    /// disassociated on its own.
    #[test]
    fn several_amazon_ipv6_blocks_associate_and_disassociate() {
        let svc = Ec2Service::new();
        let vpc_id = make_vpc(&svc, "10.0.0.0/16");
        let assoc_ipv6 = || {
            body_of(
                associate_vpc_cidr_block(
                    &svc,
                    &req(
                        "AssociateVpcCidrBlock",
                        &[("VpcId", &vpc_id), ("AmazonProvidedIpv6CidrBlock", "true")],
                    ),
                )
                .unwrap(),
            )
        };
        let field = |xml: &str, name: &str| {
            xml.split(&format!("<{name}>"))
                .nth(1)
                .and_then(|s| s.split(&format!("</{name}>")).next())
                .unwrap()
                .to_string()
        };
        let first = assoc_ipv6();
        let second = assoc_ipv6();
        let (first_id, second_id) = (
            field(&first, "associationId"),
            field(&second, "associationId"),
        );
        assert_ne!(first_id, second_id);
        assert_ne!(
            field(&first, "ipv6CidrBlock"),
            field(&second, "ipv6CidrBlock")
        );
        assert!(field(&second, "ipv6CidrBlock").ends_with("00::/56"));
        let described =
            body_of(describe_vpcs(&svc, &req("DescribeVpcs", &[("VpcId.1", &vpc_id)])).unwrap());
        assert!(described.contains(&first_id) && described.contains(&second_id));

        let out = body_of(
            disassociate_vpc_cidr_block(
                &svc,
                &req("DisassociateVpcCidrBlock", &[("AssociationId", &second_id)]),
            )
            .unwrap(),
        );
        assert!(out.contains("<ipv6CidrBlockAssociation>"), "{out}");
        assert_eq!(
            field(&out, "ipv6CidrBlock"),
            field(&second, "ipv6CidrBlock")
        );
        let described =
            body_of(describe_vpcs(&svc, &req("DescribeVpcs", &[("VpcId.1", &vpc_id)])).unwrap());
        assert!(described.contains(&first_id) && !described.contains(&second_id));
    }

    /// Re-associating after the primary Amazon IPv6 block was disassociated
    /// mints a fresh association id and a /56 never issued to the VPC, as
    /// AWS does; so does re-associating after a later block is released.
    #[test]
    fn ipv6_reassociation_mints_fresh_id_and_block() {
        let svc = Ec2Service::new();
        let vpc_id = make_vpc(&svc, "10.0.0.0/16");
        let assoc_ipv6 = || {
            body_of(
                associate_vpc_cidr_block(
                    &svc,
                    &req(
                        "AssociateVpcCidrBlock",
                        &[("VpcId", &vpc_id), ("AmazonProvidedIpv6CidrBlock", "true")],
                    ),
                )
                .unwrap(),
            )
        };
        let disassoc = |id: &str| {
            disassociate_vpc_cidr_block(
                &svc,
                &req("DisassociateVpcCidrBlock", &[("AssociationId", id)]),
            )
            .unwrap();
        };
        let field = |xml: &str, name: &str| {
            xml.split(&format!("<{name}>"))
                .nth(1)
                .and_then(|s| s.split(&format!("</{name}>")).next())
                .unwrap()
                .to_string()
        };
        let mut seen_ids = std::collections::HashSet::new();
        let mut seen_blocks = std::collections::HashSet::new();
        let first = assoc_ipv6();
        let first_id = field(&first, "associationId");
        assert_eq!(first_id, primary_ipv6_assoc_id(&vpc_id));
        seen_ids.insert(first_id.clone());
        seen_blocks.insert(field(&first, "ipv6CidrBlock"));
        disassoc(&first_id);
        let mut last_id = first_id;
        for _ in 0..3 {
            let next = assoc_ipv6();
            let id = field(&next, "associationId");
            let block = field(&next, "ipv6CidrBlock");
            assert!(seen_ids.insert(id.clone()), "association id {id} reused");
            assert!(seen_blocks.insert(block.clone()), "block {block} reused");
            assert!(block.ends_with("00::/56"), "{block}");
            let described = body_of(
                describe_vpcs(&svc, &req("DescribeVpcs", &[("VpcId.1", &vpc_id)])).unwrap(),
            );
            assert!(described.contains(&id), "{described}");
            assert!(!described.contains(&last_id), "{described}");
            disassoc(&id);
            last_id = id;
        }
    }

    /// With the IPv6-blocks-per-VPC quota enforced, the block past it is
    /// refused with `CidrLimitExceeded`.
    #[test]
    fn ipv6_blocks_past_the_quota_are_refused() {
        let svc = Ec2Service::new().with_quota_provider(Some(std::sync::Arc::new(
            fakecloud_core::quota::FixedQuotas::default().with(
                rq::IPV6_CIDR_BLOCKS_PER_VPC.service,
                rq::IPV6_CIDR_BLOCKS_PER_VPC.code,
                2.0,
            ),
        )));
        let vpc_id = make_vpc(&svc, "10.0.0.0/16");
        let assoc_ipv6 = || {
            associate_vpc_cidr_block(
                &svc,
                &req(
                    "AssociateVpcCidrBlock",
                    &[("VpcId", &vpc_id), ("AmazonProvidedIpv6CidrBlock", "true")],
                ),
            )
        };
        assert!(assoc_ipv6().is_ok());
        assert!(assoc_ipv6().is_ok());
        let err = err_of(assoc_ipv6());
        assert_eq!(err.code(), "CidrLimitExceeded");
        assert!(
            err.message()
                .contains("maximum number of allowed CIDRs ( 2 )"),
            "{}",
            err.message()
        );
    }

    #[test]
    fn modify_vpc_attribute_persists_network_address_usage_metrics() {
        // bug-audit 2026-07-27 (cycle 6): the attr was ignored on modify and
        // describe hardcoded false -> aws_vpc perpetual drift.
        let svc = Ec2Service::new();
        let vpc_id = make_vpc(&svc, "10.9.0.0/16");
        modify_vpc_attribute(
            &svc,
            &req(
                "ModifyVpcAttribute",
                &[
                    ("VpcId", &vpc_id),
                    ("EnableNetworkAddressUsageMetrics.Value", "true"),
                ],
            ),
        )
        .unwrap();
        let body = body_of(
            describe_vpc_attribute(
                &svc,
                &req(
                    "DescribeVpcAttribute",
                    &[
                        ("VpcId", &vpc_id),
                        ("Attribute", "enableNetworkAddressUsageMetrics"),
                    ],
                ),
            )
            .unwrap(),
        );
        assert!(
            body.contains("<enableNetworkAddressUsageMetrics><value>true</value>"),
            "{body}"
        );
    }

    #[test]
    fn describe_vpcs_explicit_missing_id_errors() {
        let svc = Ec2Service::new();
        make_vpc(&svc, "10.9.0.0/16");
        let err = err_of(describe_vpcs(
            &svc,
            &req("DescribeVpcs", &[("VpcId.1", "vpc-missing")]),
        ));
        assert_eq!(err.code(), "InvalidVpcID.NotFound");
    }

    #[test]
    fn describe_vpcs_paginates() {
        let svc = Ec2Service::new();
        // Seed enough VPCs (plus the default VPC) to force a second page at the
        // minimum MaxResults of 5.
        for i in 0..6 {
            make_vpc(&svc, &format!("10.{i}.0.0/16"));
        }
        let body =
            body_of(describe_vpcs(&svc, &req("DescribeVpcs", &[("MaxResults", "5")])).unwrap());
        assert!(body.contains("<nextToken>"), "expected a NextToken: {body}");
    }

    #[test]
    fn describe_vpcs_tag_value_filter() {
        let svc = Ec2Service::new();
        let id = make_vpc(&svc, "10.8.0.0/16");
        {
            let mut accounts = svc.state.write();
            let state = accounts.regional_mut("000000000000", "us-east-1");
            state.tags.insert(
                id.clone(),
                vec![Tag {
                    key: "env".into(),
                    value: "staging".into(),
                }],
            );
        }
        let body = body_of(
            describe_vpcs(
                &svc,
                &req(
                    "DescribeVpcs",
                    &[
                        ("Filter.1.Name", "tag-value"),
                        ("Filter.1.Value.1", "staging"),
                    ],
                ),
            )
            .unwrap(),
        );
        assert!(body.contains(&format!("<vpcId>{id}</vpcId>")), "{body}");
    }
}
