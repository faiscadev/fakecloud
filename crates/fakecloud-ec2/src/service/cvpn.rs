//! Client VPN: endpoints, routes, authorization rules, target networks, and
//! connections.

use fakecloud_aws::ec2query::{ec2_elem, ec2_list, ec2_return};
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use crate::service::Ec2Service;
use crate::service_helpers::{gen_id, require, validate_enum, validate_max_results};
use crate::state::{ClientVpnEndpoint, Ec2State, Tag};

const FIXED_TIME: &str = "2024-01-01T00:00:00.000Z";

fn mr(req: &AwsRequest) -> Result<(), AwsServiceError> {
    validate_max_results(&req.query_params, 5, 1000)
}

fn status_xml(tag: &str, code: &str) -> String {
    format!("<{tag}><code>{code}</code></{tag}>")
}

fn endpoint_xml(e: &ClientVpnEndpoint, tags: &[Tag]) -> String {
    format!(
        "{}{}{}{}{}{}<transportProtocol>{}</transportProtocol><vpnPort>443</vpnPort>{}{}",
        ec2_elem("clientVpnEndpointId", &e.id),
        ec2_elem("description", &e.description),
        status_xml("status", &e.status),
        ec2_elem("creationTime", FIXED_TIME),
        ec2_elem(
            "dnsName",
            &format!("*.{}.clientvpn.us-east-1.amazonaws.com", e.id)
        ),
        ec2_elem("clientCidrBlock", &e.client_cidr),
        e.transport_protocol,
        ec2_elem("serverCertificateArn", &e.server_cert_arn),
        super::tags::tag_set_xml(tags),
    )
}

pub(crate) fn create_client_vpn_endpoint(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let cert = require(&req.query_params, "ServerCertificateArn")?;
    validate_enum(&req.query_params, "TransportProtocol", &["tcp", "udp"])?;
    validate_enum(
        &req.query_params,
        "SelfServicePortal",
        &["enabled", "disabled"],
    )?;
    validate_enum(
        &req.query_params,
        "EndpointIpAddressType",
        &["ipv4", "ipv6", "dual-stack"],
    )?;
    validate_enum(
        &req.query_params,
        "TrafficIpAddressType",
        &["ipv4", "ipv6", "dual-stack"],
    )?;
    let id = gen_id("cvpn-endpoint");
    let e = ClientVpnEndpoint {
        id: id.clone(),
        description: req
            .query_params
            .get("Description")
            .cloned()
            .unwrap_or_default(),
        status: "pending-associate".to_string(),
        server_cert_arn: cert,
        transport_protocol: req
            .query_params
            .get("TransportProtocol")
            .cloned()
            .unwrap_or_else(|| "udp".to_string()),
        client_cidr: req
            .query_params
            .get("ClientCidrBlock")
            .cloned()
            .unwrap_or_else(|| "10.0.0.0/22".to_string()),
        routes: Vec::new(),
        target_networks: Vec::new(),
        auth_rules: Vec::new(),
    };
    {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        crate::service::tags::apply_tag_specifications(
            state,
            &req.query_params,
            &id,
            "client-vpn-endpoint",
        );
        state.client_vpn_endpoints.insert(id.clone(), e);
    }
    let body = format!(
        "{}{}{}",
        ec2_elem("clientVpnEndpointId", &id),
        status_xml("status", "pending-associate"),
        ec2_elem(
            "dnsName",
            &format!("*.{id}.clientvpn.us-east-1.amazonaws.com")
        )
    );
    Ok(Ec2Service::respond(
        "CreateClientVpnEndpoint",
        &req.request_id,
        &body,
    ))
}

pub(crate) fn delete_client_vpn_endpoint(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "ClientVpnEndpointId")?;
    {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        state.client_vpn_endpoints.remove(&id);
        state.tags.remove(&id);
    }
    Ok(Ec2Service::respond(
        "DeleteClientVpnEndpoint",
        &req.request_id,
        &status_xml("status", "deleting"),
    ))
}

pub(crate) fn describe_client_vpn_endpoints(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    mr(req)?;
    let wanted = crate::service_helpers::indexed_list(&req.query_params, "ClientVpnEndpointId");
    let accounts = svc.state.read();
    let empty = Ec2State::new(&req.account_id, &req.region);
    let state = accounts.get(&req.account_id).unwrap_or(&empty);
    let mut items: Vec<String> = state
        .client_vpn_endpoints
        .values()
        .filter(|e| wanted.is_empty() || wanted.contains(&e.id))
        .map(|e| endpoint_xml(e, state.tags_for(&e.id)))
        .collect();
    items.sort();
    Ok(Ec2Service::respond(
        "DescribeClientVpnEndpoints",
        &req.request_id,
        &ec2_list("clientVpnEndpoint", &items),
    ))
}

pub(crate) fn modify_client_vpn_endpoint(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "ClientVpnEndpointId")?;
    validate_enum(
        &req.query_params,
        "SelfServicePortal",
        &["enabled", "disabled"],
    )?;
    {
        let mut accounts = svc.state.write();
        if let Some(e) = accounts
            .get_or_create(&req.account_id)
            .client_vpn_endpoints
            .get_mut(&id)
        {
            if let Some(d) = req.query_params.get("Description") {
                e.description = d.clone();
            }
        }
    }
    Ok(Ec2Service::respond(
        "ModifyClientVpnEndpoint",
        &req.request_id,
        &ec2_return(true),
    ))
}

pub(crate) fn create_client_vpn_route(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "ClientVpnEndpointId")?;
    let cidr = require(&req.query_params, "DestinationCidrBlock")?;
    {
        let mut accounts = svc.state.write();
        if let Some(e) = accounts
            .get_or_create(&req.account_id)
            .client_vpn_endpoints
            .get_mut(&id)
        {
            if !e.routes.contains(&cidr) {
                e.routes.push(cidr);
            }
        }
    }
    Ok(Ec2Service::respond(
        "CreateClientVpnRoute",
        &req.request_id,
        &status_xml("status", "creating"),
    ))
}

pub(crate) fn delete_client_vpn_route(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "ClientVpnEndpointId")?;
    let cidr = require(&req.query_params, "DestinationCidrBlock")?;
    {
        let mut accounts = svc.state.write();
        if let Some(e) = accounts
            .get_or_create(&req.account_id)
            .client_vpn_endpoints
            .get_mut(&id)
        {
            e.routes.retain(|r| r != &cidr);
        }
    }
    Ok(Ec2Service::respond(
        "DeleteClientVpnRoute",
        &req.request_id,
        &status_xml("status", "deleting"),
    ))
}

pub(crate) fn describe_client_vpn_routes(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "ClientVpnEndpointId")?;
    mr(req)?;
    let accounts = svc.state.read();
    let items: Vec<String> = accounts
        .get(&req.account_id)
        .and_then(|s| s.client_vpn_endpoints.get(&id))
        .map(|e| {
            e.routes
                .iter()
                .map(|r| {
                    format!(
                        "{}{}{}<type>Nat</type>",
                        ec2_elem("clientVpnEndpointId", &id),
                        ec2_elem("destinationCidr", r),
                        status_xml("status", "active")
                    )
                })
                .collect()
        })
        .unwrap_or_default();
    Ok(Ec2Service::respond(
        "DescribeClientVpnRoutes",
        &req.request_id,
        &ec2_list("routes", &items),
    ))
}

pub(crate) fn authorize_client_vpn_ingress(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "ClientVpnEndpointId")?;
    let cidr = require(&req.query_params, "TargetNetworkCidr")?;
    {
        let mut accounts = svc.state.write();
        if let Some(e) = accounts
            .get_or_create(&req.account_id)
            .client_vpn_endpoints
            .get_mut(&id)
        {
            if !e.auth_rules.contains(&cidr) {
                e.auth_rules.push(cidr);
            }
        }
    }
    Ok(Ec2Service::respond(
        "AuthorizeClientVpnIngress",
        &req.request_id,
        &status_xml("status", "authorizing"),
    ))
}

pub(crate) fn revoke_client_vpn_ingress(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "ClientVpnEndpointId")?;
    let cidr = require(&req.query_params, "TargetNetworkCidr")?;
    {
        let mut accounts = svc.state.write();
        if let Some(e) = accounts
            .get_or_create(&req.account_id)
            .client_vpn_endpoints
            .get_mut(&id)
        {
            e.auth_rules.retain(|c| c != &cidr);
        }
    }
    Ok(Ec2Service::respond(
        "RevokeClientVpnIngress",
        &req.request_id,
        &status_xml("status", "revoking"),
    ))
}

pub(crate) fn describe_client_vpn_authorization_rules(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "ClientVpnEndpointId")?;
    mr(req)?;
    let accounts = svc.state.read();
    let items: Vec<String> = accounts
        .get(&req.account_id)
        .and_then(|s| s.client_vpn_endpoints.get(&id))
        .map(|e| {
            e.auth_rules
                .iter()
                .map(|cidr| {
                    format!(
                        "{}{}<groupId/><accessAll>true</accessAll>{}",
                        ec2_elem("clientVpnEndpointId", &id),
                        ec2_elem("destinationCidr", cidr),
                        status_xml("status", "active"),
                    )
                })
                .collect()
        })
        .unwrap_or_default();
    Ok(Ec2Service::respond(
        "DescribeClientVpnAuthorizationRules",
        &req.request_id,
        &ec2_list("authorizationRule", &items),
    ))
}

pub(crate) fn associate_client_vpn_target_network(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "ClientVpnEndpointId")?;
    // SubnetId is optional in the Smithy model, so don't require it; store the
    // supplied subnet (or a placeholder) so DescribeClientVpnTargetNetworks
    // reflects the real target.
    let subnet = req
        .query_params
        .get("SubnetId")
        .filter(|v| !v.is_empty())
        .cloned()
        .unwrap_or_else(|| "subnet-0".to_string());
    let assoc = gen_id("cvpn-assoc");
    {
        let mut accounts = svc.state.write();
        if let Some(e) = accounts
            .get_or_create(&req.account_id)
            .client_vpn_endpoints
            .get_mut(&id)
        {
            e.target_networks.push((assoc.clone(), subnet));
        }
    }
    let body = format!(
        "{}{}",
        ec2_elem("associationId", &assoc),
        status_xml("status", "associating")
    );
    Ok(Ec2Service::respond(
        "AssociateClientVpnTargetNetwork",
        &req.request_id,
        &body,
    ))
}

pub(crate) fn disassociate_client_vpn_target_network(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "ClientVpnEndpointId")?;
    let assoc = require(&req.query_params, "AssociationId")?;
    {
        let mut accounts = svc.state.write();
        if let Some(e) = accounts
            .get_or_create(&req.account_id)
            .client_vpn_endpoints
            .get_mut(&id)
        {
            e.target_networks.retain(|(a, _)| a != &assoc);
        }
    }
    let body = format!(
        "{}{}",
        ec2_elem("associationId", &assoc),
        status_xml("status", "disassociating")
    );
    Ok(Ec2Service::respond(
        "DisassociateClientVpnTargetNetwork",
        &req.request_id,
        &body,
    ))
}

pub(crate) fn describe_client_vpn_target_networks(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "ClientVpnEndpointId")?;
    mr(req)?;
    let accounts = svc.state.read();
    let items: Vec<String> = accounts
        .get(&req.account_id)
        .and_then(|s| s.client_vpn_endpoints.get(&id))
        .map(|e| {
            e.target_networks
                .iter()
                .map(|(a, subnet)| {
                    format!(
                        "{}{}{}{}",
                        ec2_elem("associationId", a),
                        ec2_elem("clientVpnEndpointId", &id),
                        ec2_elem("targetNetworkId", subnet),
                        status_xml("status", "associated")
                    )
                })
                .collect()
        })
        .unwrap_or_default();
    Ok(Ec2Service::respond(
        "DescribeClientVpnTargetNetworks",
        &req.request_id,
        &ec2_list("clientVpnTargetNetworks", &items),
    ))
}

pub(crate) fn apply_security_groups_to_client_vpn_target_network(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "ClientVpnEndpointId")?;
    require(&req.query_params, "VpcId")?;
    let sgs = crate::service_helpers::indexed_list(&req.query_params, "SecurityGroupId");
    Ok(Ec2Service::respond(
        "ApplySecurityGroupsToClientVpnTargetNetwork",
        &req.request_id,
        &ec2_list("securityGroupIds", &sgs),
    ))
}

pub(crate) fn describe_client_vpn_connections(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "ClientVpnEndpointId")?;
    mr(req)?;
    Ok(Ec2Service::respond(
        "DescribeClientVpnConnections",
        &req.request_id,
        &ec2_list("connections", &[]),
    ))
}

pub(crate) fn terminate_client_vpn_connections(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "ClientVpnEndpointId")?;
    let body = format!(
        "{}{}",
        ec2_elem("clientVpnEndpointId", &id),
        ec2_list("connectionStatuses", &[])
    );
    Ok(Ec2Service::respond(
        "TerminateClientVpnConnections",
        &req.request_id,
        &body,
    ))
}

pub(crate) fn export_client_vpn_client_certificate_revocation_list(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "ClientVpnEndpointId")?;
    let body = format!(
        "{}{}",
        ec2_elem("certificateRevocationList", ""),
        status_xml("status", "active")
    );
    Ok(Ec2Service::respond(
        "ExportClientVpnClientCertificateRevocationList",
        &req.request_id,
        &body,
    ))
}

pub(crate) fn export_client_vpn_client_configuration(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "ClientVpnEndpointId")?;
    Ok(Ec2Service::respond(
        "ExportClientVpnClientConfiguration",
        &req.request_id,
        &ec2_elem("clientConfiguration", "client\ndev tun"),
    ))
}

pub(crate) fn import_client_vpn_client_certificate_revocation_list(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "ClientVpnEndpointId")?;
    require(&req.query_params, "CertificateRevocationList")?;
    Ok(Ec2Service::respond(
        "ImportClientVpnClientCertificateRevocationList",
        &req.request_id,
        &ec2_return(true),
    ))
}
