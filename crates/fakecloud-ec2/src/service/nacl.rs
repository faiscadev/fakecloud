//! Network ACLs (+ entries/associations) and VPC peering connections.

use fakecloud_aws::ec2query::{ec2_elem, ec2_list, ec2_return};
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use crate::service::Ec2Service;
use crate::service_helpers::{
    gen_id, indexed_list, parse_filters, require, validate_enum, validate_max_results, Filter,
};
use crate::state::{Ec2State, NetworkAcl, NetworkAclAssoc, NetworkAclEntry, Tag, VpcPeering};

// ---- network ACLs ----

fn entry_xml(e: &NetworkAclEntry) -> String {
    let mut out = format!(
        "<ruleNumber>{}</ruleNumber>{}{}<egress>{}</egress>",
        e.rule_number,
        ec2_elem("protocol", &e.protocol),
        ec2_elem("ruleAction", &e.rule_action),
        e.egress,
    );
    if let Some(c) = &e.cidr_block {
        out.push_str(&ec2_elem("cidrBlock", c));
    }
    if let Some(c) = &e.ipv6_cidr_block {
        out.push_str(&ec2_elem("ipv6CidrBlock", c));
    }
    out
}

fn nacl_xml(a: &NetworkAcl, tags: &[Tag], owner: &str) -> String {
    let entries: Vec<String> = a.entries.iter().map(entry_xml).collect();
    let assocs: Vec<String> = a
        .associations
        .iter()
        .map(|s| {
            format!(
                "{}{}{}",
                ec2_elem("networkAclAssociationId", &s.association_id),
                ec2_elem("networkAclId", &a.network_acl_id),
                ec2_elem("subnetId", &s.subnet_id),
            )
        })
        .collect();
    format!(
        "{}{}{}<default>{}</default>{}{}{}",
        ec2_elem("networkAclId", &a.network_acl_id),
        ec2_elem("vpcId", &a.vpc_id),
        ec2_elem("ownerId", owner),
        a.is_default,
        ec2_list("entrySet", &entries),
        ec2_list("associationSet", &assocs),
        super::tags::tag_set_xml(tags),
    )
}

fn default_entries() -> Vec<NetworkAclEntry> {
    vec![
        NetworkAclEntry {
            rule_number: 32767,
            protocol: "-1".to_string(),
            rule_action: "deny".to_string(),
            egress: false,
            cidr_block: Some("0.0.0.0/0".to_string()),
            ipv6_cidr_block: None,
        },
        NetworkAclEntry {
            rule_number: 32767,
            protocol: "-1".to_string(),
            rule_action: "deny".to_string(),
            egress: true,
            cidr_block: Some("0.0.0.0/0".to_string()),
            ipv6_cidr_block: None,
        },
    ]
}

pub(crate) fn create_network_acl(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let vpc_id = require(&req.query_params, "VpcId")?;
    let id = gen_id("acl");
    let acl = NetworkAcl {
        network_acl_id: id.clone(),
        vpc_id,
        is_default: false,
        entries: default_entries(),
        associations: Vec::new(),
    };
    let owner = req.account_id.clone();
    let tags = {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        crate::service::tags::apply_tag_specifications(
            state,
            &req.query_params,
            &id,
            "network-acl",
        );
        let t = state.tags_for(&id).to_vec();
        state.network_acls.insert(id.clone(), acl.clone());
        t
    };
    let body = format!(
        "<networkAcl>{}</networkAcl>{}",
        nacl_xml(&acl, &tags, &owner),
        ec2_elem("clientToken", &gen_id("token")),
    );
    Ok(Ec2Service::respond(
        "CreateNetworkAcl",
        &req.request_id,
        &body,
    ))
}

pub(crate) fn delete_network_acl(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "NetworkAclId")?;
    let mut accounts = svc.state.write();
    let state = accounts.get_or_create(&req.account_id);
    state.network_acls.remove(&id);
    state.tags.remove(&id);
    Ok(Ec2Service::respond(
        "DeleteNetworkAcl",
        &req.request_id,
        &ec2_return(true),
    ))
}

pub(crate) fn describe_network_acls(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    validate_max_results(&req.query_params, 5, 1000)?;
    let filters = parse_filters(&req.query_params);
    let wanted = indexed_list(&req.query_params, "NetworkAclId");
    let owner = req.account_id.clone();
    let accounts = svc.state.read();
    let empty = Ec2State::new(&req.account_id, &req.region);
    let state = accounts.get(&req.account_id).unwrap_or(&empty);
    let mut items: Vec<String> = state
        .network_acls
        .values()
        .filter(|a| wanted.is_empty() || wanted.contains(&a.network_acl_id))
        .filter(|a| nacl_match(a, state.tags_for(&a.network_acl_id), &filters))
        .map(|a| nacl_xml(a, state.tags_for(&a.network_acl_id), &owner))
        .collect();
    items.sort();
    Ok(Ec2Service::respond(
        "DescribeNetworkAcls",
        &req.request_id,
        &ec2_list("networkAclSet", &items),
    ))
}

fn nacl_match(a: &NetworkAcl, tags: &[Tag], filters: &[Filter]) -> bool {
    filters.iter().all(|f| {
        let candidates: Vec<String> = match f.name.as_str() {
            "network-acl-id" => vec![a.network_acl_id.clone()],
            "vpc-id" => vec![a.vpc_id.clone()],
            "default" => vec![a.is_default.to_string()],
            "tag-key" => tags.iter().map(|t| t.key.clone()).collect(),
            name => {
                if let Some(key) = name.strip_prefix("tag:") {
                    tags.iter()
                        .filter(|t| t.key == key)
                        .map(|t| t.value.clone())
                        .collect()
                } else {
                    return true;
                }
            }
        };
        f.values.iter().any(|v| candidates.iter().any(|c| c == v))
    })
}

fn parse_entry(req: &AwsRequest) -> NetworkAclEntry {
    NetworkAclEntry {
        rule_number: req
            .query_params
            .get("RuleNumber")
            .and_then(|v| v.parse().ok())
            .unwrap_or(100),
        protocol: req
            .query_params
            .get("Protocol")
            .cloned()
            .unwrap_or_else(|| "-1".to_string()),
        rule_action: req
            .query_params
            .get("RuleAction")
            .cloned()
            .unwrap_or_else(|| "allow".to_string()),
        egress: req
            .query_params
            .get("Egress")
            .map(|v| v == "true")
            .unwrap_or(false),
        cidr_block: req.query_params.get("CidrBlock").cloned(),
        ipv6_cidr_block: req.query_params.get("Ipv6CidrBlock").cloned(),
    }
}

pub(crate) fn create_network_acl_entry(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "NetworkAclId")?;
    require(&req.query_params, "RuleNumber")?;
    require(&req.query_params, "Protocol")?;
    require(&req.query_params, "RuleAction")?;
    require(&req.query_params, "Egress")?;
    validate_enum(&req.query_params, "RuleAction", &["allow", "deny"])?;
    {
        let mut accounts = svc.state.write();
        if let Some(acl) = accounts
            .get_or_create(&req.account_id)
            .network_acls
            .get_mut(&id)
        {
            acl.entries.push(parse_entry(req));
        }
    }
    Ok(Ec2Service::respond(
        "CreateNetworkAclEntry",
        &req.request_id,
        &ec2_return(true),
    ))
}

pub(crate) fn replace_network_acl_entry(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "NetworkAclId")?;
    let num: i64 = require(&req.query_params, "RuleNumber")?
        .parse()
        .unwrap_or(0);
    require(&req.query_params, "Protocol")?;
    require(&req.query_params, "RuleAction")?;
    let egress = require(&req.query_params, "Egress")? == "true";
    validate_enum(&req.query_params, "RuleAction", &["allow", "deny"])?;
    {
        let mut accounts = svc.state.write();
        if let Some(acl) = accounts
            .get_or_create(&req.account_id)
            .network_acls
            .get_mut(&id)
        {
            let new = parse_entry(req);
            if let Some(e) = acl
                .entries
                .iter_mut()
                .find(|e| e.rule_number == num && e.egress == egress)
            {
                *e = new;
            } else {
                acl.entries.push(new);
            }
        }
    }
    Ok(Ec2Service::respond(
        "ReplaceNetworkAclEntry",
        &req.request_id,
        &ec2_return(true),
    ))
}

pub(crate) fn delete_network_acl_entry(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "NetworkAclId")?;
    let num: i64 = require(&req.query_params, "RuleNumber")?
        .parse()
        .unwrap_or(0);
    let egress = require(&req.query_params, "Egress")? == "true";
    {
        let mut accounts = svc.state.write();
        if let Some(acl) = accounts
            .get_or_create(&req.account_id)
            .network_acls
            .get_mut(&id)
        {
            acl.entries
                .retain(|e| !(e.rule_number == num && e.egress == egress));
        }
    }
    Ok(Ec2Service::respond(
        "DeleteNetworkAclEntry",
        &req.request_id,
        &ec2_return(true),
    ))
}

pub(crate) fn replace_network_acl_association(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let assoc_id = require(&req.query_params, "AssociationId")?;
    let acl_id = require(&req.query_params, "NetworkAclId")?;
    let new_id = gen_id("aclassoc");
    let subnet = {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let mut subnet = String::new();
        for acl in state.network_acls.values_mut() {
            acl.associations.retain(|s| {
                if s.association_id == assoc_id {
                    subnet = s.subnet_id.clone();
                    false
                } else {
                    true
                }
            });
        }
        if let Some(acl) = state.network_acls.get_mut(&acl_id) {
            acl.associations.push(NetworkAclAssoc {
                association_id: new_id.clone(),
                subnet_id: subnet.clone(),
            });
        }
        subnet
    };
    let _ = subnet;
    Ok(Ec2Service::respond(
        "ReplaceNetworkAclAssociation",
        &req.request_id,
        &ec2_elem("newAssociationId", &new_id),
    ))
}

// ---- VPC peering ----

fn peering_xml(p: &VpcPeering, tags: &[Tag], owner: &str) -> String {
    format!(
        "{}<status><code>{}</code><message>{}</message></status>\
         <requesterVpcInfo>{}{}</requesterVpcInfo>\
         <accepterVpcInfo>{}{}</accepterVpcInfo>{}",
        ec2_elem("vpcPeeringConnectionId", &p.id),
        p.status,
        p.status,
        ec2_elem("vpcId", &p.requester_vpc_id),
        ec2_elem("ownerId", owner),
        ec2_elem("vpcId", &p.accepter_vpc_id),
        ec2_elem("ownerId", owner),
        super::tags::tag_set_xml(tags),
    )
}

pub(crate) fn create_vpc_peering_connection(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let vpc_id = require(&req.query_params, "VpcId")?;
    let id = gen_id("pcx");
    let p = VpcPeering {
        id: id.clone(),
        requester_vpc_id: vpc_id,
        accepter_vpc_id: req
            .query_params
            .get("PeerVpcId")
            .cloned()
            .unwrap_or_default(),
        status: "pending-acceptance".to_string(),
    };
    let owner = req.account_id.clone();
    let tags = {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        crate::service::tags::apply_tag_specifications(
            state,
            &req.query_params,
            &id,
            "vpc-peering-connection",
        );
        let t = state.tags_for(&id).to_vec();
        state.vpc_peerings.insert(id.clone(), p.clone());
        t
    };
    Ok(Ec2Service::respond(
        "CreateVpcPeeringConnection",
        &req.request_id,
        &format!(
            "<vpcPeeringConnection>{}</vpcPeeringConnection>",
            peering_xml(&p, &tags, &owner)
        ),
    ))
}

pub(crate) fn delete_vpc_peering_connection(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "VpcPeeringConnectionId")?;
    {
        let mut accounts = svc.state.write();
        accounts
            .get_or_create(&req.account_id)
            .vpc_peerings
            .remove(&id);
    }
    Ok(Ec2Service::respond(
        "DeleteVpcPeeringConnection",
        &req.request_id,
        &ec2_return(true),
    ))
}

pub(crate) fn describe_vpc_peering_connections(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    validate_max_results(&req.query_params, 5, 1000)?;
    let wanted = indexed_list(&req.query_params, "VpcPeeringConnectionId");
    let owner = req.account_id.clone();
    let accounts = svc.state.read();
    let empty = Ec2State::new(&req.account_id, &req.region);
    let state = accounts.get(&req.account_id).unwrap_or(&empty);
    let mut items: Vec<String> = state
        .vpc_peerings
        .values()
        .filter(|p| wanted.is_empty() || wanted.contains(&p.id))
        .map(|p| peering_xml(p, state.tags_for(&p.id), &owner))
        .collect();
    items.sort();
    Ok(Ec2Service::respond(
        "DescribeVpcPeeringConnections",
        &req.request_id,
        &ec2_list("vpcPeeringConnectionSet", &items),
    ))
}

pub(crate) fn accept_vpc_peering_connection(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "VpcPeeringConnectionId")?;
    let owner = req.account_id.clone();
    let (p, tags) = {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        if let Some(p) = state.vpc_peerings.get_mut(&id) {
            p.status = "active".to_string();
        }
        let p = state.vpc_peerings.get(&id).cloned().unwrap_or(VpcPeering {
            id: id.clone(),
            requester_vpc_id: String::new(),
            accepter_vpc_id: String::new(),
            status: "active".to_string(),
        });
        let t = state.tags_for(&id).to_vec();
        (p, t)
    };
    Ok(Ec2Service::respond(
        "AcceptVpcPeeringConnection",
        &req.request_id,
        &format!(
            "<vpcPeeringConnection>{}</vpcPeeringConnection>",
            peering_xml(&p, &tags, &owner)
        ),
    ))
}

pub(crate) fn reject_vpc_peering_connection(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "VpcPeeringConnectionId")?;
    {
        let mut accounts = svc.state.write();
        if let Some(p) = accounts
            .get_or_create(&req.account_id)
            .vpc_peerings
            .get_mut(&id)
        {
            p.status = "rejected".to_string();
        }
    }
    Ok(Ec2Service::respond(
        "RejectVpcPeeringConnection",
        &req.request_id,
        &ec2_return(true),
    ))
}

pub(crate) fn modify_vpc_peering_connection_options(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "VpcPeeringConnectionId")?;
    let opts = "<allowDnsResolutionFromRemoteVpc>false</allowDnsResolutionFromRemoteVpc>";
    let body = format!(
        "<accepterPeeringConnectionOptions>{opts}</accepterPeeringConnectionOptions>\
         <requesterPeeringConnectionOptions>{opts}</requesterPeeringConnectionOptions>"
    );
    Ok(Ec2Service::respond(
        "ModifyVpcPeeringConnectionOptions",
        &req.request_id,
        &body,
    ))
}
