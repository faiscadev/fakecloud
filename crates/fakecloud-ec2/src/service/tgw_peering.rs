//! Transit Gateway peering attachments, Connect/Connect-peers, policy tables,
//! and route-table announcements.

use fakecloud_aws::ec2query::{ec2_elem, ec2_list};
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use crate::service::Ec2Service;
use crate::service_helpers::{gen_id, require, require_struct, validate_max_results};
use crate::state::{Ec2State, TgwPeering, TgwPolicyTableEntry};

const FIXED_TIME: &str = "2024-01-01T00:00:00.000Z";

fn mr(req: &AwsRequest) -> Result<(), AwsServiceError> {
    validate_max_results(&req.query_params, 5, 1000)
}

// ---- peering attachments ----

fn peering_xml(p: &TgwPeering, owner: &str, region: &str) -> String {
    format!(
        "{}<requesterTgwInfo>{}{}<region>{region}</region></requesterTgwInfo>\
         <accepterTgwInfo>{}{}<region>{}</region></accepterTgwInfo>\
         <status><code>available</code></status><state>{}</state>{}",
        ec2_elem("transitGatewayAttachmentId", &p.id),
        ec2_elem("transitGatewayId", &p.tgw_id),
        ec2_elem("ownerId", owner),
        ec2_elem("transitGatewayId", &p.peer_tgw_id),
        ec2_elem("ownerId", &p.peer_account),
        p.peer_region,
        p.state,
        ec2_elem("creationTime", FIXED_TIME),
    )
}

pub(crate) fn create_transit_gateway_peering_attachment(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let tgw_id = require(&req.query_params, "TransitGatewayId")?;
    let peer_tgw_id = require(&req.query_params, "PeerTransitGatewayId")?;
    let peer_account = require(&req.query_params, "PeerAccountId")?;
    let peer_region = require(&req.query_params, "PeerRegion")?;
    let id = gen_id("tgw-attach");
    let p = TgwPeering {
        id: id.clone(),
        tgw_id,
        peer_tgw_id,
        peer_account,
        peer_region,
        state: "pendingAcceptance".to_string(),
    };
    {
        let mut accounts = svc.state.write();
        accounts
            .get_or_create(&req.account_id)
            .tgw_peerings
            .insert(id.clone(), p.clone());
    }
    Ok(Ec2Service::respond(
        "CreateTransitGatewayPeeringAttachment",
        &req.request_id,
        &format!(
            "<transitGatewayPeeringAttachment>{}</transitGatewayPeeringAttachment>",
            peering_xml(&p, &req.account_id, &req.region)
        ),
    ))
}

fn peering_lookup(svc: &Ec2Service, req: &AwsRequest, id: &str) -> TgwPeering {
    svc.state
        .read()
        .get(&req.account_id)
        .and_then(|s| s.tgw_peerings.get(id).cloned())
        .unwrap_or(TgwPeering {
            id: id.to_string(),
            tgw_id: "tgw-0".to_string(),
            peer_tgw_id: "tgw-1".to_string(),
            peer_account: "000000000000".to_string(),
            peer_region: "us-east-1".to_string(),
            state: "available".to_string(),
        })
}

fn peering_state_change(
    svc: &Ec2Service,
    req: &AwsRequest,
    action: &str,
    new_state: &str,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "TransitGatewayAttachmentId")?;
    let mut p = peering_lookup(svc, req, &id);
    if !new_state.is_empty() {
        p.state = new_state.to_string();
    }
    {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        if action == "DeleteTransitGatewayPeeringAttachment" {
            state.tgw_peerings.remove(&id);
        } else if let Some(stored) = state.tgw_peerings.get_mut(&id) {
            stored.state = p.state.clone();
        }
    }
    Ok(Ec2Service::respond(
        action,
        &req.request_id,
        &format!(
            "<transitGatewayPeeringAttachment>{}</transitGatewayPeeringAttachment>",
            peering_xml(&p, &req.account_id, &req.region)
        ),
    ))
}

pub(crate) fn delete_transit_gateway_peering_attachment(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    peering_state_change(
        svc,
        req,
        "DeleteTransitGatewayPeeringAttachment",
        "deleting",
    )
}
pub(crate) fn accept_transit_gateway_peering_attachment(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    peering_state_change(
        svc,
        req,
        "AcceptTransitGatewayPeeringAttachment",
        "available",
    )
}
pub(crate) fn reject_transit_gateway_peering_attachment(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    peering_state_change(
        svc,
        req,
        "RejectTransitGatewayPeeringAttachment",
        "rejected",
    )
}

pub(crate) fn describe_transit_gateway_peering_attachments(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    mr(req)?;
    let owner = req.account_id.clone();
    let accounts = svc.state.read();
    let empty = Ec2State::new(&req.account_id, &req.region);
    let state = accounts.get(&req.account_id).unwrap_or(&empty);
    let mut items: Vec<String> = state
        .tgw_peerings
        .values()
        .map(|p| peering_xml(p, &owner, &req.region))
        .collect();
    items.sort();
    Ok(Ec2Service::respond(
        "DescribeTransitGatewayPeeringAttachments",
        &req.request_id,
        &ec2_list("transitGatewayPeeringAttachments", &items),
    ))
}

// ---- connect ----

pub(crate) fn create_transit_gateway_connect(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let transport = require(&req.query_params, "TransportTransitGatewayAttachmentId")?;
    require_struct(&req.query_params, "Options")?;
    let id = gen_id("tgw-attach");
    // Inherit the transit gateway from the transport attachment so the connect
    // reports the real parent rather than a placeholder.
    let tgw_id = {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let tgw_id = state
            .tgw_attachments
            .get(&transport)
            .map(|a| a.tgw_id.clone())
            .unwrap_or_else(|| "tgw-0".to_string());
        state
            .tgw_connects
            .insert(id.clone(), (transport.clone(), tgw_id.clone()));
        tgw_id
    };
    let body = format!(
        "{}{}{}<state>available</state><options><protocol>gre</protocol></options>{}",
        ec2_elem("transitGatewayAttachmentId", &id),
        ec2_elem("transportTransitGatewayAttachmentId", &transport),
        ec2_elem("transitGatewayId", &tgw_id),
        ec2_elem("creationTime", FIXED_TIME),
    );
    Ok(Ec2Service::respond(
        "CreateTransitGatewayConnect",
        &req.request_id,
        &format!("<transitGatewayConnect>{}</transitGatewayConnect>", body),
    ))
}

pub(crate) fn delete_transit_gateway_connect(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "TransitGatewayAttachmentId")?;
    let transport = {
        let mut accounts = svc.state.write();
        accounts
            .get_or_create(&req.account_id)
            .tgw_connects
            .remove(&id)
            .map(|(t, _)| t)
            .unwrap_or_default()
    };
    let body = format!(
        "{}{}<state>deleting</state>",
        ec2_elem("transitGatewayAttachmentId", &id),
        ec2_elem("transportTransitGatewayAttachmentId", &transport),
    );
    Ok(Ec2Service::respond(
        "DeleteTransitGatewayConnect",
        &req.request_id,
        &format!("<transitGatewayConnect>{}</transitGatewayConnect>", body),
    ))
}

pub(crate) fn describe_transit_gateway_connects(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    mr(req)?;
    let accounts = svc.state.read();
    let empty = Ec2State::new(&req.account_id, &req.region);
    let state = accounts.get(&req.account_id).unwrap_or(&empty);
    let mut items: Vec<String> = state
        .tgw_connects
        .iter()
        .map(|(id, (transport, tgw))| {
            format!(
                "{}{}{}<state>available</state>",
                ec2_elem("transitGatewayAttachmentId", id),
                ec2_elem("transportTransitGatewayAttachmentId", transport),
                ec2_elem("transitGatewayId", tgw)
            )
        })
        .collect();
    items.sort();
    Ok(Ec2Service::respond(
        "DescribeTransitGatewayConnects",
        &req.request_id,
        &ec2_list("transitGatewayConnectSet", &items),
    ))
}

// ---- connect peers ----

pub(crate) fn create_transit_gateway_connect_peer(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let att = require(&req.query_params, "TransitGatewayAttachmentId")?;
    require(&req.query_params, "PeerAddress")?;
    let id = gen_id("tgw-connect-peer");
    {
        let mut accounts = svc.state.write();
        accounts
            .get_or_create(&req.account_id)
            .tgw_connect_peers
            .insert(id.clone(), att.clone());
    }
    let body = format!(
        "{}{}<state>available</state>{}<connectPeerConfiguration><protocol>gre</protocol></connectPeerConfiguration>",
        ec2_elem("transitGatewayAttachmentId", &att),
        ec2_elem("transitGatewayConnectPeerId", &id),
        ec2_elem("creationTime", FIXED_TIME),
    );
    Ok(Ec2Service::respond(
        "CreateTransitGatewayConnectPeer",
        &req.request_id,
        &format!(
            "<transitGatewayConnectPeer>{}</transitGatewayConnectPeer>",
            body
        ),
    ))
}

pub(crate) fn delete_transit_gateway_connect_peer(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "TransitGatewayConnectPeerId")?;
    let att = {
        let mut accounts = svc.state.write();
        accounts
            .get_or_create(&req.account_id)
            .tgw_connect_peers
            .remove(&id)
            .unwrap_or_default()
    };
    let body = format!(
        "{}{}<state>deleting</state>",
        ec2_elem("transitGatewayAttachmentId", &att),
        ec2_elem("transitGatewayConnectPeerId", &id)
    );
    Ok(Ec2Service::respond(
        "DeleteTransitGatewayConnectPeer",
        &req.request_id,
        &format!(
            "<transitGatewayConnectPeer>{}</transitGatewayConnectPeer>",
            body
        ),
    ))
}

pub(crate) fn describe_transit_gateway_connect_peers(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    mr(req)?;
    let accounts = svc.state.read();
    let empty = Ec2State::new(&req.account_id, &req.region);
    let state = accounts.get(&req.account_id).unwrap_or(&empty);
    let mut items: Vec<String> = state
        .tgw_connect_peers
        .iter()
        .map(|(id, att)| {
            format!(
                "{}{}<state>available</state>",
                ec2_elem("transitGatewayAttachmentId", att),
                ec2_elem("transitGatewayConnectPeerId", id)
            )
        })
        .collect();
    items.sort();
    Ok(Ec2Service::respond(
        "DescribeTransitGatewayConnectPeers",
        &req.request_id,
        &ec2_list("transitGatewayConnectPeerSet", &items),
    ))
}

// ---- policy tables ----

fn policy_table_xml(id: &str, tgw_id: &str) -> String {
    format!(
        "{}{}<state>available</state>{}",
        ec2_elem("transitGatewayPolicyTableId", id),
        ec2_elem("transitGatewayId", tgw_id),
        ec2_elem("creationTime", FIXED_TIME),
    )
}

pub(crate) fn create_transit_gateway_policy_table(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let tgw_id = require(&req.query_params, "TransitGatewayId")?;
    let id = gen_id("tgw-ptb");
    {
        let mut accounts = svc.state.write();
        accounts
            .get_or_create(&req.account_id)
            .tgw_policy_tables
            .insert(id.clone(), tgw_id.clone());
    }
    Ok(Ec2Service::respond(
        "CreateTransitGatewayPolicyTable",
        &req.request_id,
        &format!(
            "<transitGatewayPolicyTable>{}</transitGatewayPolicyTable>",
            policy_table_xml(&id, &tgw_id)
        ),
    ))
}

pub(crate) fn delete_transit_gateway_policy_table(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "TransitGatewayPolicyTableId")?;
    let tgw = {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        state.tgw_policy_table_associations.remove(&id);
        state
            .tgw_policy_tables
            .remove(&id)
            .unwrap_or_else(|| "tgw-0".to_string())
    };
    Ok(Ec2Service::respond(
        "DeleteTransitGatewayPolicyTable",
        &req.request_id,
        &format!(
            "<transitGatewayPolicyTable>{}</transitGatewayPolicyTable>",
            policy_table_xml(&id, &tgw)
        ),
    ))
}

pub(crate) fn describe_transit_gateway_policy_tables(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    mr(req)?;
    let accounts = svc.state.read();
    let empty = Ec2State::new(&req.account_id, &req.region);
    let state = accounts.get(&req.account_id).unwrap_or(&empty);
    let mut items: Vec<String> = state
        .tgw_policy_tables
        .iter()
        .map(|(id, tgw)| policy_table_xml(id, tgw))
        .collect();
    items.sort();
    Ok(Ec2Service::respond(
        "DescribeTransitGatewayPolicyTables",
        &req.request_id,
        &ec2_list("transitGatewayPolicyTables", &items),
    ))
}

pub(crate) fn associate_transit_gateway_policy_table(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let pt = require(&req.query_params, "TransitGatewayPolicyTableId")?;
    let att = require(&req.query_params, "TransitGatewayAttachmentId")?;
    {
        let mut accounts = svc.state.write();
        let assocs = accounts
            .get_or_create(&req.account_id)
            .tgw_policy_table_associations
            .entry(pt.clone())
            .or_default();
        if !assocs.contains(&att) {
            assocs.push(att.clone());
        }
    }
    let body = format!(
        "<association>{}{}<resourceId>vpc-0</resourceId><resourceType>vpc</resourceType><state>associated</state></association>",
        ec2_elem("transitGatewayPolicyTableId", &pt),
        ec2_elem("transitGatewayAttachmentId", &att),
    );
    Ok(Ec2Service::respond(
        "AssociateTransitGatewayPolicyTable",
        &req.request_id,
        &body,
    ))
}

pub(crate) fn disassociate_transit_gateway_policy_table(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let pt = require(&req.query_params, "TransitGatewayPolicyTableId")?;
    let att = require(&req.query_params, "TransitGatewayAttachmentId")?;
    {
        let mut accounts = svc.state.write();
        if let Some(assocs) = accounts
            .get_or_create(&req.account_id)
            .tgw_policy_table_associations
            .get_mut(&pt)
        {
            assocs.retain(|a| a != &att);
        }
    }
    let body = format!(
        "<association>{}{}<resourceId>vpc-0</resourceId><resourceType>vpc</resourceType><state>disassociating</state></association>",
        ec2_elem("transitGatewayPolicyTableId", &pt),
        ec2_elem("transitGatewayAttachmentId", &att),
    );
    Ok(Ec2Service::respond(
        "DisassociateTransitGatewayPolicyTable",
        &req.request_id,
        &body,
    ))
}

pub(crate) fn get_transit_gateway_policy_table_associations(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let pt = require(&req.query_params, "TransitGatewayPolicyTableId")?;
    mr(req)?;
    let accounts = svc.state.read();
    let items: Vec<String> = accounts
        .get(&req.account_id)
        .and_then(|s| s.tgw_policy_table_associations.get(&pt))
        .map(|atts| {
            atts.iter()
                .map(|att| {
                    format!(
                        "{}{}<resourceId>vpc-0</resourceId><resourceType>vpc</resourceType><state>associated</state>",
                        ec2_elem("transitGatewayPolicyTableId", &pt),
                        ec2_elem("transitGatewayAttachmentId", att),
                    )
                })
                .collect()
        })
        .unwrap_or_default();
    Ok(Ec2Service::respond(
        "GetTransitGatewayPolicyTableAssociations",
        &req.request_id,
        &ec2_list("associations", &items),
    ))
}

fn policy_entry_from_req(req: &AwsRequest, _pt: &str, rule_number: &str) -> TgwPolicyTableEntry {
    let g = |k: &str| req.query_params.get(k).cloned();
    TgwPolicyTableEntry {
        policy_rule_number: rule_number.to_string(),
        target_route_table_id: g("TargetRouteTableId").unwrap_or_default(),
        source_cidr_block: g("PolicyRule.SourceCidrBlock"),
        source_port_range: g("PolicyRule.SourcePortRange"),
        destination_cidr_block: g("PolicyRule.DestinationCidrBlock"),
        destination_port_range: g("PolicyRule.DestinationPortRange"),
        protocol: g("PolicyRule.Protocol"),
        meta_data_key: g("PolicyRule.MetaData.MetaDataKey"),
        meta_data_value: g("PolicyRule.MetaData.MetaDataValue"),
    }
}

fn policy_entry_xml(e: &TgwPolicyTableEntry) -> String {
    let mut rule = String::new();
    if let Some(v) = &e.source_cidr_block {
        rule.push_str(&ec2_elem("sourceCidrBlock", v));
    }
    if let Some(v) = &e.source_port_range {
        rule.push_str(&ec2_elem("sourcePortRange", v));
    }
    if let Some(v) = &e.destination_cidr_block {
        rule.push_str(&ec2_elem("destinationCidrBlock", v));
    }
    if let Some(v) = &e.destination_port_range {
        rule.push_str(&ec2_elem("destinationPortRange", v));
    }
    if let Some(v) = &e.protocol {
        rule.push_str(&ec2_elem("protocol", v));
    }
    if e.meta_data_key.is_some() || e.meta_data_value.is_some() {
        rule.push_str(&format!(
            "<metaData>{}{}</metaData>",
            ec2_elem("metaDataKey", e.meta_data_key.as_deref().unwrap_or("")),
            ec2_elem("metaDataValue", e.meta_data_value.as_deref().unwrap_or("")),
        ));
    }
    format!(
        "{}<policyRule>{}</policyRule>{}<state>active</state>",
        ec2_elem("policyRuleNumber", &e.policy_rule_number),
        rule,
        ec2_elem("targetRouteTableId", &e.target_route_table_id),
    )
}

pub(crate) fn create_transit_gateway_policy_table_entry(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let pt = require(&req.query_params, "TransitGatewayPolicyTableId")?;
    let rule_number = require(&req.query_params, "PolicyRuleNumber")?;
    require(&req.query_params, "TargetRouteTableId")?;
    let entry = policy_entry_from_req(req, &pt, &rule_number);
    {
        let mut accounts = svc.state.write();
        accounts
            .get_or_create(&req.account_id)
            .tgw_policy_table_entries
            .entry(pt.clone())
            .or_default()
            .insert(rule_number.clone(), entry.clone());
    }
    Ok(Ec2Service::respond(
        "CreateTransitGatewayPolicyTableEntry",
        &req.request_id,
        &format!(
            "<transitGatewayPolicyTableEntry>{}</transitGatewayPolicyTableEntry>",
            policy_entry_xml(&entry)
        ),
    ))
}

pub(crate) fn modify_transit_gateway_policy_table_entry(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let pt = require(&req.query_params, "TransitGatewayPolicyTableId")?;
    let rule_number = require(&req.query_params, "PolicyRuleNumber")?;
    let entry = {
        let mut accounts = svc.state.write();
        let entries = accounts
            .get_or_create(&req.account_id)
            .tgw_policy_table_entries
            .entry(pt.clone())
            .or_default();
        let mut e = entries
            .get(&rule_number)
            .cloned()
            .unwrap_or_else(|| policy_entry_from_req(req, &pt, &rule_number));
        if let Some(v) = req.query_params.get("TargetRouteTableId") {
            e.target_route_table_id = v.clone();
        }
        let over = |cur: &mut Option<String>, k: &str| {
            if let Some(v) = req.query_params.get(k) {
                *cur = Some(v.clone());
            }
        };
        over(&mut e.source_cidr_block, "PolicyRule.SourceCidrBlock");
        over(&mut e.source_port_range, "PolicyRule.SourcePortRange");
        over(
            &mut e.destination_cidr_block,
            "PolicyRule.DestinationCidrBlock",
        );
        over(
            &mut e.destination_port_range,
            "PolicyRule.DestinationPortRange",
        );
        over(&mut e.protocol, "PolicyRule.Protocol");
        over(&mut e.meta_data_key, "PolicyRule.MetaData.MetaDataKey");
        over(&mut e.meta_data_value, "PolicyRule.MetaData.MetaDataValue");
        entries.insert(rule_number.clone(), e.clone());
        e
    };
    Ok(Ec2Service::respond(
        "ModifyTransitGatewayPolicyTableEntry",
        &req.request_id,
        &format!(
            "<transitGatewayPolicyTableEntry>{}</transitGatewayPolicyTableEntry>",
            policy_entry_xml(&entry)
        ),
    ))
}

pub(crate) fn delete_transit_gateway_policy_table_entry(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let pt = require(&req.query_params, "TransitGatewayPolicyTableId")?;
    let rule_number = require(&req.query_params, "PolicyRuleNumber")?;
    let entry = {
        let mut accounts = svc.state.write();
        accounts
            .get_or_create(&req.account_id)
            .tgw_policy_table_entries
            .get_mut(&pt)
            .and_then(|m| m.remove(&rule_number))
            .unwrap_or_else(|| policy_entry_from_req(req, &pt, &rule_number))
    };
    Ok(Ec2Service::respond(
        "DeleteTransitGatewayPolicyTableEntry",
        &req.request_id,
        &format!(
            "<transitGatewayPolicyTableEntry>{}</transitGatewayPolicyTableEntry>",
            policy_entry_xml(&entry)
        ),
    ))
}

pub(crate) fn get_transit_gateway_policy_table_entries(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let pt = require(&req.query_params, "TransitGatewayPolicyTableId")?;
    mr(req)?;
    let accounts = svc.state.read();
    let items: Vec<String> = accounts
        .get(&req.account_id)
        .and_then(|s| s.tgw_policy_table_entries.get(&pt))
        .map(|m| m.values().map(policy_entry_xml).collect())
        .unwrap_or_default();
    Ok(Ec2Service::respond(
        "GetTransitGatewayPolicyTableEntries",
        &req.request_id,
        &ec2_list("transitGatewayPolicyTableEntries", &items),
    ))
}

// ---- route table announcements ----

fn announcement_xml(id: &str, rtb: &str, peering: &str, tgw_id: &str) -> String {
    format!(
        "{}{}<peeringAttachmentId>{}</peeringAttachmentId><announcementDirection>outgoing</announcementDirection>{}<state>available</state>{}",
        ec2_elem("transitGatewayRouteTableAnnouncementId", id),
        ec2_elem("transitGatewayId", tgw_id),
        peering,
        ec2_elem("transitGatewayRouteTableId", rtb),
        ec2_elem("creationTime", FIXED_TIME),
    )
}

/// Resolve the parent transit gateway of a route table (placeholder if absent).
fn tgw_of_rtb(state: &Ec2State, rtb: &str) -> String {
    state
        .tgw_route_tables
        .get(rtb)
        .map(|r| r.tgw_id.clone())
        .unwrap_or_else(|| "tgw-0".to_string())
}

pub(crate) fn create_transit_gateway_route_table_announcement(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let rtb = require(&req.query_params, "TransitGatewayRouteTableId")?;
    let peering = require(&req.query_params, "PeeringAttachmentId")?;
    let id = gen_id("tgw-rtb-announce");
    let tgw_id = {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let tgw_id = tgw_of_rtb(state, &rtb);
        state
            .tgw_announcements
            .insert(id.clone(), (rtb.clone(), peering.clone()));
        tgw_id
    };
    Ok(Ec2Service::respond(
        "CreateTransitGatewayRouteTableAnnouncement",
        &req.request_id,
        &format!(
            "<transitGatewayRouteTableAnnouncement>{}</transitGatewayRouteTableAnnouncement>",
            announcement_xml(&id, &rtb, &peering, &tgw_id)
        ),
    ))
}

pub(crate) fn delete_transit_gateway_route_table_announcement(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "TransitGatewayRouteTableAnnouncementId")?;
    let (rtb, peering, tgw_id) = {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let (rtb, peering) = state.tgw_announcements.remove(&id).unwrap_or_default();
        let tgw_id = tgw_of_rtb(state, &rtb);
        (rtb, peering, tgw_id)
    };
    Ok(Ec2Service::respond(
        "DeleteTransitGatewayRouteTableAnnouncement",
        &req.request_id,
        &format!(
            "<transitGatewayRouteTableAnnouncement>{}</transitGatewayRouteTableAnnouncement>",
            announcement_xml(&id, &rtb, &peering, &tgw_id)
        ),
    ))
}

pub(crate) fn describe_transit_gateway_route_table_announcements(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    mr(req)?;
    let accounts = svc.state.read();
    let empty = Ec2State::new(&req.account_id, &req.region);
    let state = accounts.get(&req.account_id).unwrap_or(&empty);
    let mut items: Vec<String> = state
        .tgw_announcements
        .iter()
        .map(|(id, (rtb, p))| announcement_xml(id, rtb, p, &tgw_of_rtb(state, rtb)))
        .collect();
    items.sort();
    Ok(Ec2Service::respond(
        "DescribeTransitGatewayRouteTableAnnouncements",
        &req.request_id,
        &ec2_list("transitGatewayRouteTableAnnouncements", &items),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::ec2_request;

    fn body(resp: AwsResponse) -> String {
        String::from_utf8_lossy(resp.body.expect_bytes()).to_string()
    }

    #[test]
    fn requester_tgw_info_uses_request_region() {
        let svc = Ec2Service::new();
        // Client operates in eu-west-1; the requester side of the peering must
        // reflect that region, not a hardcoded us-east-1.
        let mut r = ec2_request(
            "CreateTransitGatewayPeeringAttachment",
            &[
                ("TransitGatewayId", "tgw-req"),
                ("PeerTransitGatewayId", "tgw-peer"),
                ("PeerAccountId", "999999999999"),
                ("PeerRegion", "ap-south-1"),
            ],
        );
        r.region = "eu-west-1".to_string();
        let created = body(create_transit_gateway_peering_attachment(&svc, &r).unwrap());
        assert!(
            created.contains(
                "<requesterTgwInfo><transitGatewayId>tgw-req</transitGatewayId>\
                 <ownerId>000000000000</ownerId><region>eu-west-1</region></requesterTgwInfo>"
            ),
            "requester region not request-scoped: {created}"
        );
        // Accepter side keeps the explicit peer region.
        assert!(
            created.contains("<region>ap-south-1</region></accepterTgwInfo>"),
            "accepter region wrong: {created}"
        );

        // Describe round-trips the same request region.
        let mut d = ec2_request("DescribeTransitGatewayPeeringAttachments", &[]);
        d.region = "eu-west-1".to_string();
        let desc = body(describe_transit_gateway_peering_attachments(&svc, &d).unwrap());
        assert!(
            desc.contains("<region>eu-west-1</region></requesterTgwInfo>"),
            "describe requester region not request-scoped: {desc}"
        );
    }
}
