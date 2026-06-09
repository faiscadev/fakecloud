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
    if let Some((from, to)) = e.port_range {
        out.push_str(&format!(
            "<portRange><from>{from}</from><to>{to}</to></portRange>"
        ));
    }
    if let Some((t, code)) = e.icmp_type_code {
        out.push_str(&format!(
            "<icmpTypeCode><type>{t}</type><code>{code}</code></icmpTypeCode>"
        ));
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

fn default_entry(egress: bool) -> NetworkAclEntry {
    NetworkAclEntry {
        rule_number: 32767,
        protocol: "-1".to_string(),
        rule_action: "deny".to_string(),
        egress,
        cidr_block: Some("0.0.0.0/0".to_string()),
        ipv6_cidr_block: None,
        port_range: None,
        icmp_type_code: None,
    }
}

fn default_entries() -> Vec<NetworkAclEntry> {
    vec![default_entry(false), default_entry(true)]
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
        port_range: parse_pair(req, "PortRange.From", "PortRange.To"),
        icmp_type_code: parse_pair(req, "Icmp.Type", "Icmp.Code"),
    }
}

/// Parse a paired `(from, to)` integer structure, present only when at least
/// one half is supplied.
fn parse_pair(req: &AwsRequest, lo: &str, hi: &str) -> Option<(i64, i64)> {
    let a = req.query_params.get(lo).and_then(|v| v.parse().ok());
    let b = req.query_params.get(hi).and_then(|v| v.parse().ok());
    match (a, b) {
        (None, None) => None,
        _ => Some((a.unwrap_or(0), b.unwrap_or(0))),
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
    {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        // Resolve the source association's subnet read-only BEFORE mutating.
        // Only move the association when both the source association and the
        // destination ACL actually exist — otherwise leave state untouched
        // (EC2 declares no error shapes here, so unknown ids stay lenient and
        // never corrupt the store with an empty-subnet placeholder).
        let subnet = state
            .network_acls
            .values()
            .flat_map(|acl| acl.associations.iter())
            .find(|s| s.association_id == assoc_id)
            .map(|s| s.subnet_id.clone());
        if let (Some(subnet), true) = (subnet, state.network_acls.contains_key(&acl_id)) {
            for acl in state.network_acls.values_mut() {
                acl.associations.retain(|s| s.association_id != assoc_id);
            }
            if let Some(acl) = state.network_acls.get_mut(&acl_id) {
                acl.associations.push(NetworkAclAssoc {
                    association_id: new_id.clone(),
                    subnet_id: subnet,
                });
            }
        }
    }
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
    // PeerVpcId is optional in the EC2 model (self/same-account peering omits
    // it), so it is not required here; default to the requester VPC when absent
    // rather than leaving a silently-empty peer.
    let peer_vpc_id = req
        .query_params
        .get("PeerVpcId")
        .filter(|v| !v.is_empty())
        .cloned()
        .unwrap_or_else(|| vpc_id.clone());
    let p = VpcPeering {
        id: id.clone(),
        requester_vpc_id: vpc_id,
        accepter_vpc_id: peer_vpc_id,
        status: "pending-acceptance".to_string(),
        requester_allow_dns: false,
        accepter_allow_dns: false,
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
        let state = accounts.get_or_create(&req.account_id);
        state.vpc_peerings.remove(&id);
        state.tags.remove(&id);
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
            requester_allow_dns: false,
            accepter_allow_dns: false,
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
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "VpcPeeringConnectionId")?;
    let req_dns = req
        .query_params
        .get("RequesterPeeringConnectionOptions.AllowDnsResolutionFromRemoteVpc")
        .map(|v| v == "true");
    let acc_dns = req
        .query_params
        .get("AccepterPeeringConnectionOptions.AllowDnsResolutionFromRemoteVpc")
        .map(|v| v == "true");
    // Persist whichever side was supplied; echo back the resulting options.
    let (requester, accepter) = {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        if let Some(p) = state.vpc_peerings.get_mut(&id) {
            if let Some(v) = req_dns {
                p.requester_allow_dns = v;
            }
            if let Some(v) = acc_dns {
                p.accepter_allow_dns = v;
            }
            (p.requester_allow_dns, p.accepter_allow_dns)
        } else {
            (req_dns.unwrap_or(false), acc_dns.unwrap_or(false))
        }
    };
    let opts = |allow: bool| {
        format!("<allowDnsResolutionFromRemoteVpc>{allow}</allowDnsResolutionFromRemoteVpc>")
    };
    let body = format!(
        "<accepterPeeringConnectionOptions>{}</accepterPeeringConnectionOptions>\
         <requesterPeeringConnectionOptions>{}</requesterPeeringConnectionOptions>",
        opts(accepter),
        opts(requester),
    );
    Ok(Ec2Service::respond(
        "ModifyVpcPeeringConnectionOptions",
        &req.request_id,
        &body,
    ))
}
