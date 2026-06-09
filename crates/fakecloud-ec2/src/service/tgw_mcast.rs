//! Transit Gateway multicast domains, metering policies, and client-VPN
//! attachment accept/reject/delete.

use fakecloud_aws::ec2query::{ec2_elem, ec2_list};
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use crate::service::Ec2Service;
use crate::service_helpers::{gen_id, require, validate_max_results};
use crate::state::{Ec2State, TgwMeteringPolicy, TgwMulticastDomain};

const FIXED_TIME: &str = "2024-01-01T00:00:00.000Z";

fn mr(req: &AwsRequest) -> Result<(), AwsServiceError> {
    validate_max_results(&req.query_params, 5, 1000)
}

// ---- multicast domains ----

fn mcast_xml(d: &TgwMulticastDomain, owner: &str) -> String {
    format!(
        "{}{}{}{}<state>available</state><options><igmpv2Support>disable</igmpv2Support>\
         <staticSourcesSupport>disable</staticSourcesSupport><autoAcceptSharedAssociations>disable</autoAcceptSharedAssociations></options>{}",
        ec2_elem("transitGatewayMulticastDomainId", &d.id),
        ec2_elem("transitGatewayId", &d.tgw_id),
        ec2_elem("transitGatewayMulticastDomainArn", &format!("arn:aws:ec2:us-east-1:{owner}:transit-gateway-multicast-domain/{}", d.id)),
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
            mcast_xml(&d, &req.account_id)
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
            mcast_xml(&d, &req.account_id)
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
        .map(|d| mcast_xml(d, &owner))
        .collect();
    items.sort();
    Ok(Ec2Service::respond(
        "DescribeTransitGatewayMulticastDomains",
        &req.request_id,
        &ec2_list("transitGatewayMulticastDomains", &items),
    ))
}

fn mcast_assoc_xml(domain: &str, att: &str) -> String {
    format!(
        "{}{}<resourceId>vpc-0</resourceId><resourceType>vpc</resourceType><subnets><item><subnetId>subnet-0</subnetId><state>associated</state></item></subnets>",
        ec2_elem("transitGatewayMulticastDomainId", domain),
        ec2_elem("transitGatewayAttachmentId", att),
    )
}

pub(crate) fn associate_transit_gateway_multicast_domain(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let domain = require(&req.query_params, "TransitGatewayMulticastDomainId")?;
    let att = require(&req.query_params, "TransitGatewayAttachmentId")?;
    Ok(Ec2Service::respond(
        "AssociateTransitGatewayMulticastDomain",
        &req.request_id,
        &format!(
            "<associations>{}</associations>",
            mcast_assoc_xml(&domain, &att)
        ),
    ))
}

pub(crate) fn disassociate_transit_gateway_multicast_domain(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let domain = require(&req.query_params, "TransitGatewayMulticastDomainId")?;
    let att = require(&req.query_params, "TransitGatewayAttachmentId")?;
    Ok(Ec2Service::respond(
        "DisassociateTransitGatewayMulticastDomain",
        &req.request_id,
        &format!(
            "<associations>{}</associations>",
            mcast_assoc_xml(&domain, &att)
        ),
    ))
}

pub(crate) fn accept_transit_gateway_multicast_domain_associations(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let domain = req
        .query_params
        .get("TransitGatewayMulticastDomainId")
        .cloned()
        .unwrap_or_else(|| "tgw-mcast-domain-0".to_string());
    let att = req
        .query_params
        .get("TransitGatewayAttachmentId")
        .cloned()
        .unwrap_or_else(|| "tgw-attach-0".to_string());
    Ok(Ec2Service::respond(
        "AcceptTransitGatewayMulticastDomainAssociations",
        &req.request_id,
        &format!(
            "<associations>{}</associations>",
            mcast_assoc_xml(&domain, &att)
        ),
    ))
}

pub(crate) fn reject_transit_gateway_multicast_domain_associations(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let domain = req
        .query_params
        .get("TransitGatewayMulticastDomainId")
        .cloned()
        .unwrap_or_else(|| "tgw-mcast-domain-0".to_string());
    let att = req
        .query_params
        .get("TransitGatewayAttachmentId")
        .cloned()
        .unwrap_or_else(|| "tgw-attach-0".to_string());
    Ok(Ec2Service::respond(
        "RejectTransitGatewayMulticastDomainAssociations",
        &req.request_id,
        &format!(
            "<associations>{}</associations>",
            mcast_assoc_xml(&domain, &att)
        ),
    ))
}

pub(crate) fn get_transit_gateway_multicast_domain_associations(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "TransitGatewayMulticastDomainId")?;
    mr(req)?;
    Ok(Ec2Service::respond(
        "GetTransitGatewayMulticastDomainAssociations",
        &req.request_id,
        &ec2_list("multicastDomainAssociations", &[]),
    ))
}

fn group_members_xml(domain: &str, kind: &str) -> String {
    format!(
        "{}<registeredNetworkInterfaceIds><item>eni-0</item></registeredNetworkInterfaceIds><groupIpAddress>224.0.0.1</groupIpAddress><{kind}>true</{kind}>",
        ec2_elem("transitGatewayMulticastDomainId", domain),
    )
}

pub(crate) fn register_transit_gateway_multicast_group_members(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let domain = require(&req.query_params, "TransitGatewayMulticastDomainId")?;
    Ok(Ec2Service::respond(
        "RegisterTransitGatewayMulticastGroupMembers",
        &req.request_id,
        &format!(
            "<registeredMulticastGroupMembers>{}</registeredMulticastGroupMembers>",
            group_members_xml(&domain, "groupMember")
        ),
    ))
}

pub(crate) fn register_transit_gateway_multicast_group_sources(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let domain = require(&req.query_params, "TransitGatewayMulticastDomainId")?;
    Ok(Ec2Service::respond(
        "RegisterTransitGatewayMulticastGroupSources",
        &req.request_id,
        &format!(
            "<registeredMulticastGroupSources>{}</registeredMulticastGroupSources>",
            group_members_xml(&domain, "groupSource")
        ),
    ))
}

pub(crate) fn deregister_transit_gateway_multicast_group_members(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let domain = req
        .query_params
        .get("TransitGatewayMulticastDomainId")
        .cloned()
        .unwrap_or_else(|| "tgw-mcast-domain-0".to_string());
    Ok(Ec2Service::respond(
        "DeregisterTransitGatewayMulticastGroupMembers",
        &req.request_id,
        &format!(
            "<deregisteredMulticastGroupMembers>{}</deregisteredMulticastGroupMembers>",
            group_members_xml(&domain, "groupMember")
        ),
    ))
}

pub(crate) fn deregister_transit_gateway_multicast_group_sources(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let domain = req
        .query_params
        .get("TransitGatewayMulticastDomainId")
        .cloned()
        .unwrap_or_else(|| "tgw-mcast-domain-0".to_string());
    Ok(Ec2Service::respond(
        "DeregisterTransitGatewayMulticastGroupSources",
        &req.request_id,
        &format!(
            "<deregisteredMulticastGroupSources>{}</deregisteredMulticastGroupSources>",
            group_members_xml(&domain, "groupSource")
        ),
    ))
}

pub(crate) fn search_transit_gateway_multicast_groups(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "TransitGatewayMulticastDomainId")?;
    mr(req)?;
    Ok(Ec2Service::respond(
        "SearchTransitGatewayMulticastGroups",
        &req.request_id,
        &ec2_list("multicastGroups", &[]),
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
