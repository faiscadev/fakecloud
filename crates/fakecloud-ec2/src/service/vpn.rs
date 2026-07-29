//! Site-to-Site VPN: customer gateways, virtual private gateways, VPN
//! connections (+ routes/tunnels), and VPN concentrators.

use fakecloud_aws::ec2query::{ec2_elem, ec2_list, ec2_return};
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use crate::service::Ec2Service;
use crate::service_helpers::{gen_id, indexed_list, require, validate_enum, validate_max_results};
use crate::state::{CustomerGateway, Ec2State, Tag, VpnConcentrator, VpnConnection, VpnGateway};

// ---- customer gateways ----

fn cgw_xml(c: &CustomerGateway, tags: &[Tag]) -> String {
    format!(
        "{}<state>{}</state>{}{}<bgpAsn>{}</bgpAsn>{}",
        ec2_elem("customerGatewayId", &c.id),
        c.state,
        ec2_elem("type", "ipsec.1"),
        ec2_elem("ipAddress", &c.ip_address),
        c.bgp_asn,
        super::tags::tag_set_xml(tags),
    )
}

pub(crate) fn create_customer_gateway(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "Type")?;
    validate_enum(&req.query_params, "Type", &["ipsec.1"])?;
    let id = gen_id("cgw");
    let c = CustomerGateway {
        id: id.clone(),
        state: "available".to_string(),
        ip_address: req
            .query_params
            .get("IpAddress")
            .or_else(|| req.query_params.get("PublicIp"))
            .cloned()
            .unwrap_or_else(|| "203.0.113.1".to_string()),
        bgp_asn: req
            .query_params
            .get("BgpAsn")
            .cloned()
            .unwrap_or_else(|| "65000".to_string()),
    };
    let tags = {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        crate::service::tags::apply_tag_specifications(
            state,
            &req.query_params,
            &id,
            "customer-gateway",
        );
        let t = state.tags_for(&id).to_vec();
        state.customer_gateways.insert(id.clone(), c.clone());
        t
    };
    Ok(Ec2Service::respond(
        "CreateCustomerGateway",
        &req.request_id,
        &format!("<customerGateway>{}</customerGateway>", cgw_xml(&c, &tags)),
    ))
}

pub(crate) fn delete_customer_gateway(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "CustomerGatewayId")?;
    let mut accounts = svc.state.write();
    let state = accounts.get_or_create(&req.account_id);
    state.customer_gateways.remove(&id);
    state.tags.remove(&id);
    Ok(Ec2Service::respond(
        "DeleteCustomerGateway",
        &req.request_id,
        &ec2_return(true),
    ))
}

pub(crate) fn describe_customer_gateways(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let wanted = indexed_list(&req.query_params, "CustomerGatewayId");
    let accounts = svc.state.read();
    let empty = Ec2State::new(&req.account_id, &req.region);
    let state = accounts.get(&req.account_id).unwrap_or(&empty);
    let mut items: Vec<String> = state
        .customer_gateways
        .values()
        .filter(|c| wanted.is_empty() || wanted.contains(&c.id))
        .map(|c| cgw_xml(c, state.tags_for(&c.id)))
        .collect();
    items.sort();
    Ok(Ec2Service::respond(
        "DescribeCustomerGateways",
        &req.request_id,
        &ec2_list("customerGatewaySet", &items),
    ))
}

// ---- VPN gateways ----

fn vgw_xml(g: &VpnGateway, tags: &[Tag]) -> String {
    let atts: Vec<String> = g
        .attachments
        .iter()
        .map(|v| format!("{}<state>attached</state>", ec2_elem("vpcId", v)))
        .collect();
    format!(
        "{}<state>{}</state>{}<amazonSideAsn>{}</amazonSideAsn>{}{}{}",
        ec2_elem("vpnGatewayId", &g.id),
        g.state,
        ec2_elem("type", "ipsec.1"),
        g.amazon_side_asn,
        g.availability_zone
            .as_ref()
            .map(|z| ec2_elem("availabilityZone", z))
            .unwrap_or_default(),
        ec2_list("attachments", &atts),
        super::tags::tag_set_xml(tags),
    )
}

pub(crate) fn create_vpn_gateway(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "Type")?;
    validate_enum(&req.query_params, "Type", &["ipsec.1"])?;
    let id = gen_id("vgw");
    let g = VpnGateway {
        id: id.clone(),
        state: "available".to_string(),
        attachments: Vec::new(),
        amazon_side_asn: req
            .query_params
            .get("AmazonSideAsn")
            .and_then(|v| v.parse().ok())
            .unwrap_or(64512),
        availability_zone: req.query_params.get("AvailabilityZone").cloned(),
    };
    let tags = {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        crate::service::tags::apply_tag_specifications(
            state,
            &req.query_params,
            &id,
            "vpn-gateway",
        );
        let t = state.tags_for(&id).to_vec();
        state.vpn_gateways.insert(id.clone(), g.clone());
        t
    };
    Ok(Ec2Service::respond(
        "CreateVpnGateway",
        &req.request_id,
        &format!("<vpnGateway>{}</vpnGateway>", vgw_xml(&g, &tags)),
    ))
}

pub(crate) fn delete_vpn_gateway(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "VpnGatewayId")?;
    let mut accounts = svc.state.write();
    let state = accounts.get_or_create(&req.account_id);
    state.vpn_gateways.remove(&id);
    state.tags.remove(&id);
    Ok(Ec2Service::respond(
        "DeleteVpnGateway",
        &req.request_id,
        &ec2_return(true),
    ))
}

pub(crate) fn describe_vpn_gateways(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let wanted = indexed_list(&req.query_params, "VpnGatewayId");
    let accounts = svc.state.read();
    let empty = Ec2State::new(&req.account_id, &req.region);
    let state = accounts.get(&req.account_id).unwrap_or(&empty);
    let mut items: Vec<String> = state
        .vpn_gateways
        .values()
        .filter(|g| wanted.is_empty() || wanted.contains(&g.id))
        .map(|g| vgw_xml(g, state.tags_for(&g.id)))
        .collect();
    items.sort();
    Ok(Ec2Service::respond(
        "DescribeVpnGateways",
        &req.request_id,
        &ec2_list("vpnGatewaySet", &items),
    ))
}

pub(crate) fn attach_vpn_gateway(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let vpc = require(&req.query_params, "VpcId")?;
    let gw = require(&req.query_params, "VpnGatewayId")?;
    {
        let mut accounts = svc.state.write();
        if let Some(g) = accounts
            .get_or_create(&req.account_id)
            .vpn_gateways
            .get_mut(&gw)
        {
            if !g.attachments.contains(&vpc) {
                g.attachments.push(vpc.clone());
            }
        }
    }
    Ok(Ec2Service::respond(
        "AttachVpnGateway",
        &req.request_id,
        &format!(
            "<attachment>{}<state>attached</state></attachment>",
            ec2_elem("vpcId", &vpc)
        ),
    ))
}

pub(crate) fn detach_vpn_gateway(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let vpc = require(&req.query_params, "VpcId")?;
    let gw = require(&req.query_params, "VpnGatewayId")?;
    {
        let mut accounts = svc.state.write();
        if let Some(g) = accounts
            .get_or_create(&req.account_id)
            .vpn_gateways
            .get_mut(&gw)
        {
            g.attachments.retain(|v| v != &vpc);
        }
    }
    Ok(Ec2Service::respond(
        "DetachVpnGateway",
        &req.request_id,
        &ec2_return(true),
    ))
}

// ---- VPN connections ----

fn vpn_conn_xml(c: &VpnConnection, tags: &[Tag]) -> String {
    let routes: Vec<String> = c
        .routes
        .iter()
        .map(|r| {
            format!(
                "{}<state>available</state>",
                ec2_elem("destinationCidrBlock", r)
            )
        })
        .collect();
    format!(
        "{}<state>{}</state>{}{}{}<customerGatewayConfiguration>&lt;vpn_connection/&gt;</customerGatewayConfiguration>\
         <options><staticRoutesOnly>{}</staticRoutesOnly></options>{}{}{}",
        ec2_elem("vpnConnectionId", &c.id),
        c.state,
        ec2_elem("type", "ipsec.1"),
        ec2_elem("customerGatewayId", &c.customer_gateway_id),
        format_args!(
            "{}{}",
            c.vpn_gateway_id.as_ref().map(|g| ec2_elem("vpnGatewayId", g)).unwrap_or_default(),
            c.transit_gateway_id.as_ref().map(|g| ec2_elem("transitGatewayId", g)).unwrap_or_default(),
        ),
        c.static_routes_only,
        ec2_list("routes", &routes),
        ec2_list("vgwTelemetry", &[]),
        super::tags::tag_set_xml(tags),
    )
}

pub(crate) fn create_vpn_connection(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let cgw = require(&req.query_params, "CustomerGatewayId")?;
    require(&req.query_params, "Type")?;
    // CreateVpnConnection types Type as a plain string in the Smithy model
    // (unlike CGW/VGW which use the ipsec.1 enum), so the conformance harness
    // generates arbitrary-string positive variants that must return 200 — we
    // can't enum-validate it here without failing those.
    let id = gen_id("vpn");
    let c = VpnConnection {
        id: id.clone(),
        state: "available".to_string(),
        customer_gateway_id: cgw,
        vpn_gateway_id: req.query_params.get("VpnGatewayId").cloned(),
        transit_gateway_id: req.query_params.get("TransitGatewayId").cloned(),
        static_routes_only: req
            .query_params
            .get("Options.StaticRoutesOnly")
            .is_some_and(|v| v == "true"),
        routes: Vec::new(),
    };
    let tags = {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        crate::service::tags::apply_tag_specifications(
            state,
            &req.query_params,
            &id,
            "vpn-connection",
        );
        let t = state.tags_for(&id).to_vec();
        state.vpn_connections.insert(id.clone(), c.clone());
        t
    };
    Ok(Ec2Service::respond(
        "CreateVpnConnection",
        &req.request_id,
        &format!("<vpnConnection>{}</vpnConnection>", vpn_conn_xml(&c, &tags)),
    ))
}

pub(crate) fn delete_vpn_connection(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "VpnConnectionId")?;
    let mut accounts = svc.state.write();
    let state = accounts.get_or_create(&req.account_id);
    state.vpn_connections.remove(&id);
    state.tags.remove(&id);
    Ok(Ec2Service::respond(
        "DeleteVpnConnection",
        &req.request_id,
        &ec2_return(true),
    ))
}

pub(crate) fn describe_vpn_connections(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let wanted = indexed_list(&req.query_params, "VpnConnectionId");
    let accounts = svc.state.read();
    let empty = Ec2State::new(&req.account_id, &req.region);
    let state = accounts.get(&req.account_id).unwrap_or(&empty);
    let mut items: Vec<String> = state
        .vpn_connections
        .values()
        .filter(|c| wanted.is_empty() || wanted.contains(&c.id))
        .map(|c| vpn_conn_xml(c, state.tags_for(&c.id)))
        .collect();
    items.sort();
    Ok(Ec2Service::respond(
        "DescribeVpnConnections",
        &req.request_id,
        &ec2_list("vpnConnectionSet", &items),
    ))
}

fn vpn_conn_lookup(svc: &Ec2Service, req: &AwsRequest, id: &str) -> (VpnConnection, Vec<Tag>) {
    let accounts = svc.state.read();
    let c = accounts
        .get(&req.account_id)
        .and_then(|s| s.vpn_connections.get(id).cloned())
        .unwrap_or(VpnConnection {
            id: id.to_string(),
            state: "available".to_string(),
            customer_gateway_id: "cgw-0".to_string(),
            vpn_gateway_id: None,
            transit_gateway_id: None,
            static_routes_only: false,
            routes: Vec::new(),
        });
    let tags = accounts
        .get(&req.account_id)
        .map(|s| s.tags_for(id).to_vec())
        .unwrap_or_default();
    (c, tags)
}

fn modify_vpn_conn(
    svc: &Ec2Service,
    req: &AwsRequest,
    action: &str,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "VpnConnectionId")?;
    let (c, tags) = vpn_conn_lookup(svc, req, &id);
    Ok(Ec2Service::respond(
        action,
        &req.request_id,
        &format!("<vpnConnection>{}</vpnConnection>", vpn_conn_xml(&c, &tags)),
    ))
}

pub(crate) fn modify_vpn_connection(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "VpnConnectionId")?;
    // Apply the target gateway(s) to the persisted connection so the paired
    // DescribeVpnConnections reflects them (fixes re-rendering the old gateway).
    // AWS moves the VPN's attachment: setting one gateway type clears the other.
    {
        let mut accounts = svc.state.write();
        if let Some(c) = accounts
            .get_or_create(&req.account_id)
            .vpn_connections
            .get_mut(&id)
        {
            if let Some(v) = req.query_params.get("CustomerGatewayId") {
                c.customer_gateway_id = v.clone();
            }
            if let Some(v) = req.query_params.get("VpnGatewayId") {
                c.vpn_gateway_id = Some(v.clone());
                c.transit_gateway_id = None;
            }
            if let Some(v) = req.query_params.get("TransitGatewayId") {
                c.transit_gateway_id = Some(v.clone());
                c.vpn_gateway_id = None;
            }
        }
    }
    let (c, tags) = vpn_conn_lookup(svc, req, &id);
    Ok(Ec2Service::respond(
        "ModifyVpnConnection",
        &req.request_id,
        &format!("<vpnConnection>{}</vpnConnection>", vpn_conn_xml(&c, &tags)),
    ))
}

pub(crate) fn modify_vpn_connection_options(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "VpnConnectionId")?;
    validate_enum(&req.query_params, "TunnelBandwidth", &["standard", "large"])?;
    modify_vpn_conn(svc, req, "ModifyVpnConnectionOptions")
}

pub(crate) fn create_vpn_connection_route(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let cidr = require(&req.query_params, "DestinationCidrBlock")?;
    let id = require(&req.query_params, "VpnConnectionId")?;
    {
        let mut accounts = svc.state.write();
        if let Some(c) = accounts
            .get_or_create(&req.account_id)
            .vpn_connections
            .get_mut(&id)
        {
            if !c.routes.contains(&cidr) {
                c.routes.push(cidr);
            }
        }
    }
    Ok(Ec2Service::respond(
        "CreateVpnConnectionRoute",
        &req.request_id,
        &ec2_return(true),
    ))
}

pub(crate) fn delete_vpn_connection_route(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let cidr = require(&req.query_params, "DestinationCidrBlock")?;
    let id = require(&req.query_params, "VpnConnectionId")?;
    {
        let mut accounts = svc.state.write();
        if let Some(c) = accounts
            .get_or_create(&req.account_id)
            .vpn_connections
            .get_mut(&id)
        {
            c.routes.retain(|r| r != &cidr);
        }
    }
    Ok(Ec2Service::respond(
        "DeleteVpnConnectionRoute",
        &req.request_id,
        &ec2_return(true),
    ))
}

// ---- VPN tunnels ----

fn require_tunnel(req: &AwsRequest) -> Result<String, AwsServiceError> {
    let id = require(&req.query_params, "VpnConnectionId")?;
    require(&req.query_params, "VpnTunnelOutsideIpAddress")?;
    Ok(id)
}

pub(crate) fn modify_vpn_tunnel_options(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require_tunnel(req)?;
    let (c, tags) = vpn_conn_lookup(svc, req, &id);
    Ok(Ec2Service::respond(
        "ModifyVpnTunnelOptions",
        &req.request_id,
        &format!("<vpnConnection>{}</vpnConnection>", vpn_conn_xml(&c, &tags)),
    ))
}

pub(crate) fn modify_vpn_tunnel_certificate(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require_tunnel(req)?;
    let (c, tags) = vpn_conn_lookup(svc, req, &id);
    Ok(Ec2Service::respond(
        "ModifyVpnTunnelCertificate",
        &req.request_id,
        &format!("<vpnConnection>{}</vpnConnection>", vpn_conn_xml(&c, &tags)),
    ))
}

pub(crate) fn replace_vpn_tunnel(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require_tunnel(req)?;
    Ok(Ec2Service::respond(
        "ReplaceVpnTunnel",
        &req.request_id,
        &ec2_return(true),
    ))
}

pub(crate) fn get_active_vpn_tunnel_status(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require_tunnel(req)?;
    Ok(Ec2Service::respond("GetActiveVpnTunnelStatus", &req.request_id, "<activeVpnTunnelStatus><phase1EncryptionAlgorithm>AES256</phase1EncryptionAlgorithm></activeVpnTunnelStatus>"))
}

pub(crate) fn get_vpn_tunnel_replacement_status(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "VpnConnectionId")?;
    let ip = require(&req.query_params, "VpnTunnelOutsideIpAddress")?;
    let body = format!(
        "{}{}<maintenanceDetails><pendingMaintenance>No</pendingMaintenance></maintenanceDetails>",
        ec2_elem("vpnConnectionId", &id),
        ec2_elem("vpnTunnelOutsideIpAddress", &ip),
    );
    Ok(Ec2Service::respond(
        "GetVpnTunnelReplacementStatus",
        &req.request_id,
        &body,
    ))
}

pub(crate) fn get_vpn_connection_device_types(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    validate_max_results(&req.query_params, 200, 1000)?;
    let item = "<vpnConnectionDeviceTypeId>0123abcd</vpnConnectionDeviceTypeId><vendor>Generic</vendor><platform>Generic</platform><software>Generic</software>";
    Ok(Ec2Service::respond(
        "GetVpnConnectionDeviceTypes",
        &req.request_id,
        &ec2_list("vpnConnectionDeviceTypeSet", &[item.to_string()]),
    ))
}

pub(crate) fn get_vpn_connection_device_sample_configuration(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "VpnConnectionId")?;
    require(&req.query_params, "VpnConnectionDeviceTypeId")?;
    Ok(Ec2Service::respond(
        "GetVpnConnectionDeviceSampleConfiguration",
        &req.request_id,
        &ec2_elem("vpnConnectionDeviceSampleConfiguration", "! sample config"),
    ))
}

// ---- VPN concentrators ----

fn concentrator_xml(c: &VpnConcentrator, tags: &[Tag]) -> String {
    format!(
        "{}<state>{}</state>{}{}",
        ec2_elem("vpnConcentratorId", &c.id),
        c.state,
        ec2_elem("type", "ipsec.1"),
        super::tags::tag_set_xml(tags),
    )
}

pub(crate) fn create_vpn_concentrator(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "Type")?;
    validate_enum(&req.query_params, "Type", &["ipsec.1"])?;
    let id = gen_id("vpnc");
    let c = VpnConcentrator {
        id: id.clone(),
        state: "available".to_string(),
    };
    let tags = {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        crate::service::tags::apply_tag_specifications(
            state,
            &req.query_params,
            &id,
            "vpn-concentrator",
        );
        let t = state.tags_for(&id).to_vec();
        state.vpn_concentrators.insert(id.clone(), c.clone());
        t
    };
    Ok(Ec2Service::respond(
        "CreateVpnConcentrator",
        &req.request_id,
        &format!(
            "<vpnConcentrator>{}</vpnConcentrator>",
            concentrator_xml(&c, &tags)
        ),
    ))
}

pub(crate) fn delete_vpn_concentrator(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "VpnConcentratorId")?;
    let mut accounts = svc.state.write();
    let state = accounts.get_or_create(&req.account_id);
    state.vpn_concentrators.remove(&id);
    state.tags.remove(&id);
    Ok(Ec2Service::respond(
        "DeleteVpnConcentrator",
        &req.request_id,
        &ec2_return(true),
    ))
}

pub(crate) fn describe_vpn_concentrators(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    validate_max_results(&req.query_params, 200, 1000)?;
    let accounts = svc.state.read();
    let empty = Ec2State::new(&req.account_id, &req.region);
    let state = accounts.get(&req.account_id).unwrap_or(&empty);
    let mut items: Vec<String> = state
        .vpn_concentrators
        .values()
        .map(|c| concentrator_xml(c, state.tags_for(&c.id)))
        .collect();
    items.sort();
    Ok(Ec2Service::respond(
        "DescribeVpnConcentrators",
        &req.request_id,
        &ec2_list("vpnConcentratorSet", &items),
    ))
}

#[cfg(test)]
mod modify_vpn_tests {
    use super::*;
    use crate::test_support::ec2_request;

    fn body(resp: AwsResponse) -> String {
        String::from_utf8_lossy(resp.body.expect_bytes()).to_string()
    }

    #[test]
    fn create_vpn_connection_persists_static_routes_only() {
        // bug-audit 2026-07-29 (cycle 8) E2-4: Options.StaticRoutesOnly was
        // dropped; the describe render hardcoded false.
        let svc = Ec2Service::new();
        create_vpn_connection(
            &svc,
            &ec2_request(
                "CreateVpnConnection",
                &[
                    ("CustomerGatewayId", "cgw-1"),
                    ("Type", "ipsec.1"),
                    ("Options.StaticRoutesOnly", "true"),
                ],
            ),
        )
        .unwrap();
        let desc = body(
            describe_vpn_connections(&svc, &ec2_request("DescribeVpnConnections", &[])).unwrap(),
        );
        assert!(
            desc.contains("<staticRoutesOnly>true</staticRoutesOnly>"),
            "{desc}"
        );
    }

    #[test]
    fn create_vpn_gateway_persists_asn_and_az() {
        // bug-audit 2026-07-29 (cycle 8) E2-5: AmazonSideAsn/AvailabilityZone
        // were dropped; the render hardcoded ASN 64512 and emitted no AZ.
        let svc = Ec2Service::new();
        create_vpn_gateway(
            &svc,
            &ec2_request(
                "CreateVpnGateway",
                &[
                    ("Type", "ipsec.1"),
                    ("AmazonSideAsn", "65001"),
                    ("AvailabilityZone", "us-east-1b"),
                ],
            ),
        )
        .unwrap();
        let desc =
            body(describe_vpn_gateways(&svc, &ec2_request("DescribeVpnGateways", &[])).unwrap());
        assert!(
            desc.contains("<amazonSideAsn>65001</amazonSideAsn>"),
            "{desc}"
        );
        assert!(
            desc.contains("<availabilityZone>us-east-1b</availabilityZone>"),
            "{desc}"
        );
    }

    #[test]
    fn modify_vpn_connection_applies_new_gateway() {
        let svc = Ec2Service::new();
        // Create with a VPN gateway...
        let out = body(
            create_vpn_connection(
                &svc,
                &ec2_request(
                    "CreateVpnConnection",
                    &[
                        ("CustomerGatewayId", "cgw-1"),
                        ("Type", "ipsec.1"),
                        ("VpnGatewayId", "vgw-1"),
                    ],
                ),
            )
            .unwrap(),
        );
        let id = out
            .split("<vpnConnectionId>")
            .nth(1)
            .and_then(|s| s.split("</vpnConnectionId>").next())
            .unwrap()
            .to_string();
        assert!(out.contains("<vpnGatewayId>vgw-1</vpnGatewayId>"), "{out}");

        // ...then modify to a transit gateway; the new gateway must persist.
        let modified = body(
            modify_vpn_connection(
                &svc,
                &ec2_request(
                    "ModifyVpnConnection",
                    &[("VpnConnectionId", &id), ("TransitGatewayId", "tgw-9")],
                ),
            )
            .unwrap(),
        );
        assert!(
            modified.contains("<transitGatewayId>tgw-9</transitGatewayId>"),
            "{modified}"
        );
        assert!(
            !modified.contains("vgw-1"),
            "old gateway re-rendered: {modified}"
        );

        // Describe reflects the change (not the original gateway).
        let described = body(
            describe_vpn_connections(
                &svc,
                &ec2_request("DescribeVpnConnections", &[("VpnConnectionId.1", &id)]),
            )
            .unwrap(),
        );
        assert!(
            described.contains("<transitGatewayId>tgw-9</transitGatewayId>"),
            "{described}"
        );
        assert!(!described.contains("vgw-1"), "{described}");
    }
}
