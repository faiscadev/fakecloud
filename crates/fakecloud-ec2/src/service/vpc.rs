//! VPC operations: create/delete/describe, attributes, tenancy, and secondary
//! CIDR-block association.

use fakecloud_aws::ec2query::{ec2_elem, ec2_list, ec2_return};
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use crate::service::Ec2Service;
use crate::service_helpers::{
    gen_id, indexed_list, parse_filters, require, validate_enum, validate_max_results, Filter,
};
use crate::state::{Ec2State, Tag, Vpc, VpcCidrAssoc};

/// Render the inner XML of a `<vpc>` / `<vpcSet><item>` element (lowerCamel
/// wire names, the casing the AWS SDK deserializer expects).
pub(crate) fn vpc_xml(vpc: &Vpc, tags: &[Tag], owner_id: &str) -> String {
    let cidr_assocs: Vec<String> = std::iter::once(VpcCidrAssoc {
        association_id: format!("vpc-cidr-assoc-{}", &vpc.vpc_id[4..]),
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

    format!(
        "{}{}{}{}{}{}{}{}{}",
        ec2_elem("vpcId", &vpc.vpc_id),
        ec2_elem("state", &vpc.state),
        ec2_elem("cidrBlock", &vpc.cidr_block),
        ec2_elem("dhcpOptionsId", &vpc.dhcp_options_id),
        ec2_elem("instanceTenancy", &vpc.instance_tenancy),
        format_args!("<isDefault>{}</isDefault>", vpc.is_default),
        ec2_elem("ownerId", owner_id),
        ec2_list("cidrBlockAssociationSet", &cidr_assocs),
        super::tags::tag_set_xml(tags),
    )
}

/// Shared `Filter.N` matcher for VPC describe: supports `vpc-id`, `cidr`,
/// `dhcp-options-id`, `isDefault`, `state`, and the `tag:`/`tag-key` family.
fn vpc_matches(vpc: &Vpc, tags: &[Tag], filters: &[Filter]) -> bool {
    filters.iter().all(|f| {
        let candidates: Vec<String> = match f.name.as_str() {
            "vpc-id" => vec![vpc.vpc_id.clone()],
            "cidr" | "cidr-block-association.cidr-block" => vec![vpc.cidr_block.clone()],
            "dhcp-options-id" => vec![vpc.dhcp_options_id.clone()],
            "state" => vec![vpc.state.clone()],
            "isDefault" | "is-default" => vec![vpc.is_default.to_string()],
            "tag-key" => tags.iter().map(|t| t.key.clone()).collect(),
            name => {
                if let Some(key) = name.strip_prefix("tag:") {
                    tags.iter()
                        .filter(|t| t.key == key)
                        .map(|t| t.value.clone())
                        .collect()
                } else {
                    return true; // unknown filter: don't exclude
                }
            }
        };
        f.values.iter().any(|v| candidates.iter().any(|c| c == v))
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
    let vpc = build_vpc(cidr, tenancy, false);
    let vpc_id = vpc.vpc_id.clone();

    let owner = req.account_id.clone();
    let body = {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        crate::service::tags::apply_tag_specifications(state, &req.query_params, &vpc_id, "vpc");
        let tags = state.tags_for(&vpc_id).to_vec();
        state.vpcs.insert(vpc_id.clone(), vpc.clone());
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
    let body = {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let vpc = state
            .vpcs
            .values()
            .find(|v| v.is_default)
            .cloned()
            // No default VPC somehow (e.g. it was deleted): re-create one.
            .unwrap_or_else(|| {
                let v = build_vpc("172.31.0.0/16".to_string(), "default".to_string(), true);
                state.vpcs.insert(v.vpc_id.clone(), v.clone());
                v
            });
        let tags = state.tags_for(&vpc.vpc_id).to_vec();
        format!("<vpc>{}</vpc>", vpc_xml(&vpc, &tags, &owner))
    };
    Ok(Ec2Service::respond(
        "CreateDefaultVpc",
        &req.request_id,
        &body,
    ))
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
        cidr_associations: Vec::new(),
    }
}

pub(crate) fn delete_vpc(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "VpcId")?;
    {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
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
    let state = accounts.get(&req.account_id).unwrap_or(&empty);

    let mut items: Vec<String> = state
        .vpcs
        .values()
        .filter(|v| wanted.is_empty() || wanted.contains(&v.vpc_id))
        .filter(|v| vpc_matches(v, state.tags_for(&v.vpc_id), &filters))
        .map(|v| vpc_xml(v, state.tags_for(&v.vpc_id), &owner))
        .collect();
    items.sort();

    let body = ec2_list("vpcSet", &items);
    Ok(Ec2Service::respond("DescribeVpcs", &req.request_id, &body))
}

pub(crate) fn modify_vpc_attribute(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let vpc_id = require(&req.query_params, "VpcId")?;
    let mut accounts = svc.state.write();
    let state = accounts.get_or_create(&req.account_id);
    if let Some(vpc) = state.vpcs.get_mut(&vpc_id) {
        if let Some(v) = bool_attr(&req.query_params, "EnableDnsSupport.Value") {
            vpc.enable_dns_support = v;
        }
        if let Some(v) = bool_attr(&req.query_params, "EnableDnsHostnames.Value") {
            vpc.enable_dns_hostnames = v;
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
    let state = accounts.get(&req.account_id).unwrap_or(&empty);
    let (dns_support, dns_hostnames) = state
        .vpcs
        .get(&vpc_id)
        .map(|v| (v.enable_dns_support, v.enable_dns_hostnames))
        .unwrap_or((true, false));

    let attr_xml = match attribute.as_str() {
        "enableDnsHostnames" => format!(
            "<enableDnsHostnames><value>{dns_hostnames}</value></enableDnsHostnames>"
        ),
        "enableNetworkAddressUsageMetrics" => {
            "<enableNetworkAddressUsageMetrics><value>false</value></enableNetworkAddressUsageMetrics>".to_string()
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
        let state = accounts.get_or_create(&req.account_id);
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

    let assoc = VpcCidrAssoc {
        association_id: gen_id("vpc-cidr-assoc"),
        cidr_block: cidr.clone().unwrap_or_else(|| "10.1.0.0/16".to_string()),
        state: "associated".to_string(),
    };
    // Lenient: record on the VPC when it exists, but always return a valid
    // association response (random-id positive variants must not 4xx).
    {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        if let Some(vpc) = state.vpcs.get_mut(&vpc_id) {
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

    let mut found: Option<(String, VpcCidrAssoc)> = None;
    {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
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
