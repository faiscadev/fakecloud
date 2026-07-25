//! Transit Gateway multicast domains, metering policies, and client-VPN
//! attachment accept/reject/delete.

use fakecloud_aws::ec2query::{ec2_elem, ec2_list};
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use crate::service::Ec2Service;
use crate::service_helpers::{gen_id, indexed_list, require, validate_max_results};
use crate::state::{
    Ec2State, TgwMcastAssociation, TgwMcastGroup, TgwMeteringPolicy, TgwMulticastDomain,
};

const FIXED_TIME: &str = "2024-01-01T00:00:00.000Z";

fn mr(req: &AwsRequest) -> Result<(), AwsServiceError> {
    validate_max_results(&req.query_params, 5, 1000)
}

// ---- multicast domains ----

fn mcast_xml(d: &TgwMulticastDomain, owner: &str, region: &str) -> String {
    format!(
        "{}{}{}{}<state>available</state><options><igmpv2Support>disable</igmpv2Support>\
         <staticSourcesSupport>disable</staticSourcesSupport><autoAcceptSharedAssociations>disable</autoAcceptSharedAssociations></options>{}",
        ec2_elem("transitGatewayMulticastDomainId", &d.id),
        ec2_elem("transitGatewayId", &d.tgw_id),
        ec2_elem("transitGatewayMulticastDomainArn", &format!("arn:aws:ec2:{region}:{owner}:transit-gateway-multicast-domain/{}", d.id)),
        ec2_elem("ownerId", owner),
        ec2_elem("creationTime", FIXED_TIME),
    )
}

pub(crate) fn create_transit_gateway_multicast_domain(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let tgw_id = require(&req.query_params, "TransitGatewayId")?;
    let id = gen_id("tgw-mcast-domain");
    let d = TgwMulticastDomain {
        id: id.clone(),
        tgw_id,
    };
    {
        let mut accounts = svc.state.write();
        accounts
            .get_or_create(&req.account_id)
            .tgw_multicast_domains
            .insert(id.clone(), d.clone());
    }
    Ok(Ec2Service::respond(
        "CreateTransitGatewayMulticastDomain",
        &req.request_id,
        &format!(
            "<transitGatewayMulticastDomain>{}</transitGatewayMulticastDomain>",
            mcast_xml(&d, &req.account_id, &req.region)
        ),
    ))
}

pub(crate) fn delete_transit_gateway_multicast_domain(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "TransitGatewayMulticastDomainId")?;
    let d = {
        let mut accounts = svc.state.write();
        accounts
            .get_or_create(&req.account_id)
            .tgw_multicast_domains
            .remove(&id)
            .unwrap_or(TgwMulticastDomain {
                id: id.clone(),
                tgw_id: "tgw-0".to_string(),
            })
    };
    Ok(Ec2Service::respond(
        "DeleteTransitGatewayMulticastDomain",
        &req.request_id,
        &format!(
            "<transitGatewayMulticastDomain>{}</transitGatewayMulticastDomain>",
            mcast_xml(&d, &req.account_id, &req.region)
        ),
    ))
}

pub(crate) fn describe_transit_gateway_multicast_domains(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    mr(req)?;
    let owner = req.account_id.clone();
    let accounts = svc.state.read();
    let empty = Ec2State::new(&req.account_id, &req.region);
    let state = accounts.get(&req.account_id).unwrap_or(&empty);
    let mut items: Vec<String> = state
        .tgw_multicast_domains
        .values()
        .map(|d| mcast_xml(d, &owner, &req.region))
        .collect();
    items.sort();
    Ok(Ec2Service::respond(
        "DescribeTransitGatewayMulticastDomains",
        &req.request_id,
        &ec2_list("transitGatewayMulticastDomains", &items),
    ))
}

/// Render an `<associations>` structure (single attachment, its subnet list)
/// for the associate/disassociate responses.
fn mcast_assoc_xml(domain: &str, att: &str, assocs: &[TgwMcastAssociation]) -> String {
    let resource_id = assocs
        .first()
        .map(|a| a.resource_id.clone())
        .unwrap_or_default();
    let resource_type = assocs
        .first()
        .map(|a| a.resource_type.clone())
        .unwrap_or_else(|| "vpc".to_string());
    let subnets: Vec<String> = assocs
        .iter()
        .map(|a| {
            format!(
                "{}{}",
                ec2_elem("subnetId", &a.subnet_id),
                ec2_elem("state", &a.state)
            )
        })
        .collect();
    format!(
        "{}{}{}{}{}",
        ec2_elem("transitGatewayMulticastDomainId", domain),
        ec2_elem("transitGatewayAttachmentId", att),
        ec2_elem("resourceId", &resource_id),
        ec2_elem("resourceType", &resource_type),
        ec2_list("subnets", &subnets),
    )
}

pub(crate) fn associate_transit_gateway_multicast_domain(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let domain = require(&req.query_params, "TransitGatewayMulticastDomainId")?;
    let att = require(&req.query_params, "TransitGatewayAttachmentId")?;
    let subnet_ids = indexed_list(&req.query_params, "SubnetIds");
    let new_assocs: Vec<TgwMcastAssociation> = {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        // Resolve the attachment's backing resource (VPC) so describes are real.
        let (resource_id, resource_type) = state
            .tgw_attachments
            .get(&att)
            .map(|a| (a.resource_id.clone(), a.resource_type.clone()))
            .unwrap_or_else(|| (String::new(), "vpc".to_string()));
        let entries = state
            .tgw_mcast_associations
            .entry(domain.clone())
            .or_default();
        let mut added = Vec::new();
        for subnet_id in &subnet_ids {
            if entries
                .iter()
                .any(|e| &e.subnet_id == subnet_id && e.attachment_id == att)
            {
                continue;
            }
            let assoc = TgwMcastAssociation {
                attachment_id: att.clone(),
                subnet_id: subnet_id.clone(),
                resource_id: resource_id.clone(),
                resource_type: resource_type.clone(),
                state: "associated".to_string(),
            };
            entries.push(assoc.clone());
            added.push(assoc);
        }
        added
    };
    Ok(Ec2Service::respond(
        "AssociateTransitGatewayMulticastDomain",
        &req.request_id,
        &format!(
            "<associations>{}</associations>",
            mcast_assoc_xml(&domain, &att, &new_assocs)
        ),
    ))
}

pub(crate) fn disassociate_transit_gateway_multicast_domain(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let domain = require(&req.query_params, "TransitGatewayMulticastDomainId")?;
    let att = require(&req.query_params, "TransitGatewayAttachmentId")?;
    let subnet_ids = indexed_list(&req.query_params, "SubnetIds");
    let removed: Vec<TgwMcastAssociation> = {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let mut removed = Vec::new();
        if let Some(entries) = state.tgw_mcast_associations.get_mut(&domain) {
            entries.retain(|e| {
                let hit = e.attachment_id == att
                    && (subnet_ids.is_empty() || subnet_ids.contains(&e.subnet_id));
                if hit {
                    let mut r = e.clone();
                    r.state = "disassociated".to_string();
                    removed.push(r);
                }
                !hit
            });
        }
        removed
    };
    Ok(Ec2Service::respond(
        "DisassociateTransitGatewayMulticastDomain",
        &req.request_id,
        &format!(
            "<associations>{}</associations>",
            mcast_assoc_xml(&domain, &att, &removed)
        ),
    ))
}

/// Look up the persisted subnet associations for `domain`/`att` and render the
/// `<associations>` structure. Shared by accept/reject.
fn assoc_response(svc: &Ec2Service, req: &AwsRequest, domain: &str, att: &str) -> String {
    let accounts = svc.state.read();
    let assocs: Vec<TgwMcastAssociation> = accounts
        .get(&req.account_id)
        .and_then(|s| s.tgw_mcast_associations.get(domain))
        .map(|v| {
            v.iter()
                .filter(|a| a.attachment_id == att)
                .cloned()
                .collect()
        })
        .unwrap_or_default();
    format!(
        "<associations>{}</associations>",
        mcast_assoc_xml(domain, att, &assocs)
    )
}

pub(crate) fn accept_transit_gateway_multicast_domain_associations(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let domain = require(&req.query_params, "TransitGatewayMulticastDomainId")?;
    let att = require(&req.query_params, "TransitGatewayAttachmentId")?;
    let body = assoc_response(svc, req, &domain, &att);
    Ok(Ec2Service::respond(
        "AcceptTransitGatewayMulticastDomainAssociations",
        &req.request_id,
        &body,
    ))
}

pub(crate) fn reject_transit_gateway_multicast_domain_associations(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let domain = require(&req.query_params, "TransitGatewayMulticastDomainId")?;
    let att = require(&req.query_params, "TransitGatewayAttachmentId")?;
    let body = assoc_response(svc, req, &domain, &att);
    Ok(Ec2Service::respond(
        "RejectTransitGatewayMulticastDomainAssociations",
        &req.request_id,
        &body,
    ))
}

pub(crate) fn get_transit_gateway_multicast_domain_associations(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let domain = require(&req.query_params, "TransitGatewayMulticastDomainId")?;
    mr(req)?;
    let accounts = svc.state.read();
    let items: Vec<String> = accounts
        .get(&req.account_id)
        .and_then(|s| s.tgw_mcast_associations.get(&domain))
        .map(|v| {
            v.iter()
                .map(|a| {
                    format!(
                        "{}{}{}<subnet>{}{}</subnet>",
                        ec2_elem("transitGatewayAttachmentId", &a.attachment_id),
                        ec2_elem("resourceId", &a.resource_id),
                        ec2_elem("resourceType", &a.resource_type),
                        ec2_elem("subnetId", &a.subnet_id),
                        ec2_elem("state", &a.state),
                    )
                })
                .collect()
        })
        .unwrap_or_default();
    Ok(Ec2Service::respond(
        "GetTransitGatewayMulticastDomainAssociations",
        &req.request_id,
        &ec2_list("multicastDomainAssociations", &items),
    ))
}

/// Render a `registered*` group-members/sources structure for `domain`.
fn registered_group_xml(domain: &str, group_ip: &str, enis: &[String]) -> String {
    let eni_items: Vec<String> = enis.to_vec();
    format!(
        "{}{}{}",
        ec2_elem("transitGatewayMulticastDomainId", domain),
        ec2_list("registeredNetworkInterfaceIds", &eni_items),
        ec2_elem("groupIpAddress", group_ip),
    )
}

/// Register `enis` under `domain` for the given group IP (member or source).
fn register_group(svc: &Ec2Service, req: &AwsRequest, is_member: bool) -> (String, Vec<String>) {
    let domain = req
        .query_params
        .get("TransitGatewayMulticastDomainId")
        .cloned()
        .unwrap_or_default();
    let group_ip = req
        .query_params
        .get("GroupIpAddress")
        .cloned()
        .unwrap_or_default();
    let enis = indexed_list(&req.query_params, "NetworkInterfaceIds");
    let mut accounts = svc.state.write();
    let groups = accounts
        .get_or_create(&req.account_id)
        .tgw_mcast_groups
        .entry(domain)
        .or_default();
    for eni in &enis {
        if !groups
            .iter()
            .any(|g| &g.eni_id == eni && g.group_ip == group_ip && g.is_member == is_member)
        {
            groups.push(TgwMcastGroup {
                group_ip: group_ip.clone(),
                eni_id: eni.clone(),
                is_member,
            });
        }
    }
    (group_ip, enis)
}

/// Deregister `enis` from `domain` for the given group IP (member or source).
fn deregister_group(svc: &Ec2Service, req: &AwsRequest, is_member: bool) -> (String, Vec<String>) {
    let domain = req
        .query_params
        .get("TransitGatewayMulticastDomainId")
        .cloned()
        .unwrap_or_default();
    let group_ip = req
        .query_params
        .get("GroupIpAddress")
        .cloned()
        .unwrap_or_default();
    let enis = indexed_list(&req.query_params, "NetworkInterfaceIds");
    let mut accounts = svc.state.write();
    if let Some(groups) = accounts
        .get_or_create(&req.account_id)
        .tgw_mcast_groups
        .get_mut(&domain)
    {
        groups.retain(|g| {
            !(g.is_member == is_member
                && g.group_ip == group_ip
                && (enis.is_empty() || enis.contains(&g.eni_id)))
        });
    }
    (group_ip, enis)
}

pub(crate) fn register_transit_gateway_multicast_group_members(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let domain = require(&req.query_params, "TransitGatewayMulticastDomainId")?;
    let (group_ip, enis) = register_group(svc, req, true);
    Ok(Ec2Service::respond(
        "RegisterTransitGatewayMulticastGroupMembers",
        &req.request_id,
        &format!(
            "<registeredMulticastGroupMembers>{}</registeredMulticastGroupMembers>",
            registered_group_xml(&domain, &group_ip, &enis)
        ),
    ))
}

pub(crate) fn register_transit_gateway_multicast_group_sources(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let domain = require(&req.query_params, "TransitGatewayMulticastDomainId")?;
    let (group_ip, enis) = register_group(svc, req, false);
    Ok(Ec2Service::respond(
        "RegisterTransitGatewayMulticastGroupSources",
        &req.request_id,
        &format!(
            "<registeredMulticastGroupSources>{}</registeredMulticastGroupSources>",
            registered_group_xml(&domain, &group_ip, &enis)
        ),
    ))
}

pub(crate) fn deregister_transit_gateway_multicast_group_members(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let domain = require(&req.query_params, "TransitGatewayMulticastDomainId")?;
    let (group_ip, enis) = deregister_group(svc, req, true);
    Ok(Ec2Service::respond(
        "DeregisterTransitGatewayMulticastGroupMembers",
        &req.request_id,
        &format!(
            "<deregisteredMulticastGroupMembers>{}</deregisteredMulticastGroupMembers>",
            registered_group_xml(&domain, &group_ip, &enis)
        ),
    ))
}

pub(crate) fn deregister_transit_gateway_multicast_group_sources(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let domain = require(&req.query_params, "TransitGatewayMulticastDomainId")?;
    let (group_ip, enis) = deregister_group(svc, req, false);
    Ok(Ec2Service::respond(
        "DeregisterTransitGatewayMulticastGroupSources",
        &req.request_id,
        &format!(
            "<deregisteredMulticastGroupSources>{}</deregisteredMulticastGroupSources>",
            registered_group_xml(&domain, &group_ip, &enis)
        ),
    ))
}

pub(crate) fn search_transit_gateway_multicast_groups(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let domain = require(&req.query_params, "TransitGatewayMulticastDomainId")?;
    mr(req)?;
    let accounts = svc.state.read();
    let items: Vec<String> = accounts
        .get(&req.account_id)
        .and_then(|s| s.tgw_mcast_groups.get(&domain))
        .map(|v| {
            v.iter()
                .map(|g| {
                    format!(
                        "{}{}<groupMember>{}</groupMember><groupSource>{}</groupSource>",
                        ec2_elem("groupIpAddress", &g.group_ip),
                        ec2_elem("networkInterfaceId", &g.eni_id),
                        g.is_member,
                        !g.is_member,
                    )
                })
                .collect()
        })
        .unwrap_or_default();
    Ok(Ec2Service::respond(
        "SearchTransitGatewayMulticastGroups",
        &req.request_id,
        &ec2_list("multicastGroups", &items),
    ))
}

// ---- metering policies ----

fn metering_xml(p: &TgwMeteringPolicy) -> String {
    format!(
        "{}{}<state>available</state>{}{}",
        ec2_elem("transitGatewayMeteringPolicyId", &p.id),
        ec2_elem("transitGatewayId", &p.tgw_id),
        ec2_list("middleboxAttachmentIdSet", &[]),
        ec2_elem("updateEffectiveAt", FIXED_TIME),
    )
}

pub(crate) fn create_transit_gateway_metering_policy(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let tgw_id = require(&req.query_params, "TransitGatewayId")?;
    let id = gen_id("tgw-mp");
    let p = TgwMeteringPolicy {
        id: id.clone(),
        tgw_id,
    };
    {
        let mut accounts = svc.state.write();
        accounts
            .get_or_create(&req.account_id)
            .tgw_metering_policies
            .insert(id.clone(), p.clone());
    }
    Ok(Ec2Service::respond(
        "CreateTransitGatewayMeteringPolicy",
        &req.request_id,
        &format!(
            "<transitGatewayMeteringPolicy>{}</transitGatewayMeteringPolicy>",
            metering_xml(&p)
        ),
    ))
}

pub(crate) fn delete_transit_gateway_metering_policy(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "TransitGatewayMeteringPolicyId")?;
    let p = {
        let mut accounts = svc.state.write();
        accounts
            .get_or_create(&req.account_id)
            .tgw_metering_policies
            .remove(&id)
            .unwrap_or(TgwMeteringPolicy {
                id: id.clone(),
                tgw_id: "tgw-0".to_string(),
            })
    };
    Ok(Ec2Service::respond(
        "DeleteTransitGatewayMeteringPolicy",
        &req.request_id,
        &format!(
            "<transitGatewayMeteringPolicy>{}</transitGatewayMeteringPolicy>",
            metering_xml(&p)
        ),
    ))
}

pub(crate) fn describe_transit_gateway_metering_policies(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    mr(req)?;
    let accounts = svc.state.read();
    let empty = Ec2State::new(&req.account_id, &req.region);
    let state = accounts.get(&req.account_id).unwrap_or(&empty);
    let mut items: Vec<String> = state
        .tgw_metering_policies
        .values()
        .map(metering_xml)
        .collect();
    items.sort();
    Ok(Ec2Service::respond(
        "DescribeTransitGatewayMeteringPolicies",
        &req.request_id,
        &ec2_list("transitGatewayMeteringPolicies", &items),
    ))
}

pub(crate) fn modify_transit_gateway_metering_policy(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "TransitGatewayMeteringPolicyId")?;
    let p = {
        let accounts = svc.state.read();
        accounts
            .get(&req.account_id)
            .and_then(|s| s.tgw_metering_policies.get(&id).cloned())
            .unwrap_or(TgwMeteringPolicy {
                id: id.clone(),
                tgw_id: "tgw-0".to_string(),
            })
    };
    Ok(Ec2Service::respond(
        "ModifyTransitGatewayMeteringPolicy",
        &req.request_id,
        &format!(
            "<transitGatewayMeteringPolicy>{}</transitGatewayMeteringPolicy>",
            metering_xml(&p)
        ),
    ))
}

fn metering_entry_xml(policy: &str, rule: &str, metered_account: &str) -> String {
    format!(
        "{}<policyRuleNumber>{}</policyRuleNumber>{}",
        ec2_elem("transitGatewayMeteringPolicyId", policy),
        rule,
        ec2_elem("meteredAccount", metered_account),
    )
}

pub(crate) fn create_transit_gateway_metering_policy_entry(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let policy = require(&req.query_params, "TransitGatewayMeteringPolicyId")?;
    let rule = require(&req.query_params, "PolicyRuleNumber")?;
    require(&req.query_params, "MeteredAccount")?;
    crate::service_helpers::validate_enum(
        &req.query_params,
        "MeteredAccount",
        &[
            "source-attachment-owner",
            "destination-attachment-owner",
            "transit-gateway-owner",
        ],
    )?;
    let att_types = &[
        "vpc",
        "vpn",
        "vpn-concentrator",
        "direct-connect-gateway",
        "connect",
        "peering",
        "tgw-peering",
        "network-function",
        "client-vpn",
    ];
    crate::service_helpers::validate_enum(
        &req.query_params,
        "SourceTransitGatewayAttachmentType",
        att_types,
    )?;
    crate::service_helpers::validate_enum(
        &req.query_params,
        "DestinationTransitGatewayAttachmentType",
        att_types,
    )?;
    let metered = req
        .query_params
        .get("MeteredAccount")
        .cloned()
        .unwrap_or_else(|| "source-attachment-owner".to_string());
    Ok(Ec2Service::respond(
        "CreateTransitGatewayMeteringPolicyEntry",
        &req.request_id,
        &format!(
            "<transitGatewayMeteringPolicyEntry>{}</transitGatewayMeteringPolicyEntry>",
            metering_entry_xml(&policy, &rule, &metered)
        ),
    ))
}

pub(crate) fn delete_transit_gateway_metering_policy_entry(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let policy = require(&req.query_params, "TransitGatewayMeteringPolicyId")?;
    let rule = require(&req.query_params, "PolicyRuleNumber")?;
    Ok(Ec2Service::respond(
        "DeleteTransitGatewayMeteringPolicyEntry",
        &req.request_id,
        &format!(
            "<transitGatewayMeteringPolicyEntry>{}</transitGatewayMeteringPolicyEntry>",
            metering_entry_xml(&policy, &rule, "source-attachment-owner")
        ),
    ))
}

pub(crate) fn get_transit_gateway_metering_policy_entries(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "TransitGatewayMeteringPolicyId")?;
    mr(req)?;
    Ok(Ec2Service::respond(
        "GetTransitGatewayMeteringPolicyEntries",
        &req.request_id,
        &ec2_list("transitGatewayMeteringPolicyEntries", &[]),
    ))
}

// ---- client VPN attachments ----

fn client_vpn_att_xml(id: &str, state: &str) -> String {
    format!(
        "{}{}<state>{}</state>{}",
        ec2_elem("transitGatewayAttachmentId", id),
        ec2_elem("transitGatewayId", "tgw-0"),
        state,
        ec2_elem("creationTime", FIXED_TIME),
    )
}

pub(crate) fn accept_transit_gateway_client_vpn_attachment(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "TransitGatewayAttachmentId")?;
    Ok(Ec2Service::respond(
        "AcceptTransitGatewayClientVpnAttachment",
        &req.request_id,
        &format!(
            "<transitGatewayClientVpnAttachment>{}</transitGatewayClientVpnAttachment>",
            client_vpn_att_xml(&id, "available")
        ),
    ))
}

pub(crate) fn delete_transit_gateway_client_vpn_attachment(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "TransitGatewayAttachmentId")?;
    Ok(Ec2Service::respond(
        "DeleteTransitGatewayClientVpnAttachment",
        &req.request_id,
        &format!(
            "<transitGatewayClientVpnAttachment>{}</transitGatewayClientVpnAttachment>",
            client_vpn_att_xml(&id, "deleting")
        ),
    ))
}

pub(crate) fn reject_transit_gateway_client_vpn_attachment(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "TransitGatewayAttachmentId")?;
    Ok(Ec2Service::respond(
        "RejectTransitGatewayClientVpnAttachment",
        &req.request_id,
        &format!(
            "<transitGatewayClientVpnAttachment>{}</transitGatewayClientVpnAttachment>",
            client_vpn_att_xml(&id, "rejected")
        ),
    ))
}

#[cfg(test)]
mod mcast_tests {
    use super::*;
    use crate::test_support::ec2_request;

    fn body(resp: AwsResponse) -> String {
        String::from_utf8_lossy(resp.body.expect_bytes()).to_string()
    }

    #[test]
    fn associate_domain_persists_subnets() {
        let svc = Ec2Service::new();
        associate_transit_gateway_multicast_domain(
            &svc,
            &ec2_request(
                "AssociateTransitGatewayMulticastDomain",
                &[
                    ("TransitGatewayMulticastDomainId", "tgw-mcast-domain-1"),
                    ("TransitGatewayAttachmentId", "tgw-attach-1"),
                    ("SubnetIds.1", "subnet-aaa"),
                    ("SubnetIds.2", "subnet-bbb"),
                ],
            ),
        )
        .unwrap();

        let out = body(
            get_transit_gateway_multicast_domain_associations(
                &svc,
                &ec2_request(
                    "GetTransitGatewayMulticastDomainAssociations",
                    &[("TransitGatewayMulticastDomainId", "tgw-mcast-domain-1")],
                ),
            )
            .unwrap(),
        );
        assert!(out.contains("<subnetId>subnet-aaa</subnetId>"), "{out}");
        assert!(out.contains("<subnetId>subnet-bbb</subnetId>"), "{out}");
        // No hardcoded placeholder ids leak through.
        assert!(!out.contains("subnet-0<"), "{out}");
    }

    #[test]
    fn register_group_members_persist_and_search() {
        let svc = Ec2Service::new();
        register_transit_gateway_multicast_group_members(
            &svc,
            &ec2_request(
                "RegisterTransitGatewayMulticastGroupMembers",
                &[
                    ("TransitGatewayMulticastDomainId", "tgw-mcast-domain-1"),
                    ("GroupIpAddress", "224.0.0.9"),
                    ("NetworkInterfaceIds.1", "eni-abc"),
                ],
            ),
        )
        .unwrap();
        let out = body(
            search_transit_gateway_multicast_groups(
                &svc,
                &ec2_request(
                    "SearchTransitGatewayMulticastGroups",
                    &[("TransitGatewayMulticastDomainId", "tgw-mcast-domain-1")],
                ),
            )
            .unwrap(),
        );
        assert!(
            out.contains("<groupIpAddress>224.0.0.9</groupIpAddress>"),
            "{out}"
        );
        assert!(
            out.contains("<networkInterfaceId>eni-abc</networkInterfaceId>"),
            "{out}"
        );
        assert!(out.contains("<groupMember>true</groupMember>"), "{out}");
    }
}
