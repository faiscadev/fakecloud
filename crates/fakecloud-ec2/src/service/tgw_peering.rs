//! Transit Gateway peering attachments, Connect/Connect-peers, policy tables,
//! and route-table announcements.

use fakecloud_aws::ec2query::{ec2_elem, ec2_list};
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use crate::service::Ec2Service;
use crate::service_helpers::{gen_id, require, require_struct, validate_max_results};
use crate::state::{Ec2State, TgwPeering};

const FIXED_TIME: &str = "2024-01-01T00:00:00.000Z";

fn mr(req: &AwsRequest) -> Result<(), AwsServiceError> {
    validate_max_results(&req.query_params, 5, 1000)
}

// ---- peering attachments ----

fn peering_xml(p: &TgwPeering, owner: &str) -> String {
    format!(
        "{}<requesterTgwInfo>{}{}<region>us-east-1</region></requesterTgwInfo>\
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
            peering_xml(&p, &req.account_id)
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
            peering_xml(&p, &req.account_id)
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
        .map(|p| peering_xml(p, &owner))
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

pub(crate) fn get_transit_gateway_policy_table_entries(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "TransitGatewayPolicyTableId")?;
    mr(req)?;
    Ok(Ec2Service::respond(
        "GetTransitGatewayPolicyTableEntries",
        &req.request_id,
        &ec2_list("transitGatewayPolicyTableEntries", &[]),
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
