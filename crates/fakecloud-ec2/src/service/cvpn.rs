//! Client VPN: endpoints, routes, authorization rules, target networks, and
//! connections.

use std::collections::HashMap;

use fakecloud_aws::ec2query::{ec2_elem, ec2_elem_opt, ec2_list, ec2_return};
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use crate::service::ipam_registry::{now_rfc3339, prune_client_tokens, token_expired};
use crate::service::Ec2Service;
use crate::service_helpers::{
    gen_id, invalid_parameter_value, missing_parameter, require, validate_enum,
    validate_max_results,
};
use crate::state::{
    ClientVpnAuthorizationPolicy, ClientVpnConnectionLog, ClientVpnEndpoint,
    ClientVpnTrustProvider, Ec2State, IpamIdempotencyRecord, Tag,
};

const FIXED_TIME: &str = "2024-01-01T00:00:00.000Z";

fn mr(req: &AwsRequest) -> Result<(), AwsServiceError> {
    validate_max_results(&req.query_params, 5, 1000)
}

fn status_xml(tag: &str, code: &str) -> String {
    format!("<{tag}><code>{code}</code></{tag}>")
}

const TRUST_PROVIDER_TYPES: &[&str] = &["crowdstrike", "jamf", "jumpcloud"];
const SHADOW_MODES: &[&str] = &["enabled", "disabled"];

/// `InvalidClientVpnEndpointId.NotFound` -- the endpoint does not exist.
fn endpoint_not_found(id: &str) -> AwsServiceError {
    AwsServiceError::aws_error(
        http::StatusCode::BAD_REQUEST,
        "InvalidClientVpnEndpointId.NotFound",
        format!("The Client VPN endpoint ID '{id}' does not exist"),
    )
}

fn dry_run(req: &AwsRequest) -> bool {
    req.query_params
        .get("DryRun")
        .is_some_and(|v| v.eq_ignore_ascii_case("true"))
}

/// An optional boolean query parameter; anything but `true`/`false` is
/// rejected rather than read as `false`.
fn opt_bool(params: &HashMap<String, String>, key: &str) -> Result<Option<bool>, AwsServiceError> {
    match params.get(key).filter(|v| !v.is_empty()) {
        None => Ok(None),
        Some(v) if v.eq_ignore_ascii_case("true") => Ok(Some(true)),
        Some(v) if v.eq_ignore_ascii_case("false") => Ok(Some(false)),
        Some(v) => Err(invalid_parameter_value(format!(
            "Invalid value '{v}' for {key}"
        ))),
    }
}

fn opt_string(params: &HashMap<String, String>, key: &str) -> Option<String> {
    params.get(key).filter(|v| !v.is_empty()).cloned()
}

fn has_prefix(params: &HashMap<String, String>, prefix: &str) -> bool {
    params.keys().any(|k| k.starts_with(prefix))
}

/// `ConnectionLogOptions.*`, or `None` when the request carries none.
fn parse_connection_log(
    params: &HashMap<String, String>,
) -> Result<Option<ClientVpnConnectionLog>, AwsServiceError> {
    if !has_prefix(params, "ConnectionLogOptions.") {
        return Ok(None);
    }
    let log = ClientVpnConnectionLog {
        enabled: opt_bool(params, "ConnectionLogOptions.Enabled")?.unwrap_or(false),
        log_group: opt_string(params, "ConnectionLogOptions.CloudwatchLogGroup"),
        log_stream: opt_string(params, "ConnectionLogOptions.CloudwatchLogStream"),
        include_authorization_policy_context: opt_bool(
            params,
            "ConnectionLogOptions.IncludeAuthorizationPolicyContext",
        )?,
    };
    // The log group is "required if connection logging is enabled": there is
    // nowhere to publish the connection data without one.
    if log.enabled && log.log_group.is_none() {
        return Err(invalid_parameter_value(
            "ConnectionLogOptions.CloudwatchLogGroup is required when connection logging is enabled",
        ));
    }
    Ok(Some(log))
}

/// The trust providers a `DevicePostureOptions` block configures, or `None`
/// when the request carries no such block. The block replaces the endpoint's
/// whole device posture configuration, and `Enabled=false` clears it.
fn parse_device_posture(
    params: &HashMap<String, String>,
) -> Result<Option<Vec<ClientVpnTrustProvider>>, AwsServiceError> {
    if !has_prefix(params, "DevicePostureOptions.") {
        return Ok(None);
    }
    let enabled = opt_bool(params, "DevicePostureOptions.Enabled")?;
    // `TrustProviders` carries `xmlName: TrustProvider`, which is the spelling
    // official clients put on the wire; the member name is accepted too.
    let list = if has_prefix(params, "DevicePostureOptions.TrustProvider.") {
        "DevicePostureOptions.TrustProvider"
    } else {
        "DevicePostureOptions.TrustProviders"
    };
    let mut providers = Vec::new();
    for i in 1.. {
        let prefix = format!("{list}.{i}.");
        if !has_prefix(params, &prefix) {
            break;
        }
        let kind_key = format!("{prefix}TrustProviderType");
        validate_enum(params, &kind_key, TRUST_PROVIDER_TYPES)?;
        let provider = ClientVpnTrustProvider {
            trust_provider_type: opt_string(params, &kind_key),
            tenant_id: opt_string(params, &format!("{prefix}TenantId")),
            public_signing_key_url: opt_string(params, &format!("{prefix}PublicSigningKeyUrl")),
        };
        if !providers.contains(&provider) {
            providers.push(provider);
        }
    }
    if enabled == Some(false) {
        providers.clear();
    }
    Ok(Some(providers))
}

fn connection_log_xml(log: &ClientVpnConnectionLog) -> String {
    // `ConnectionLogResponseOptions` members carry no `xmlName`, so they go on
    // the wire under their capitalised member names.
    let mut inner = format!("<Enabled>{}</Enabled>", log.enabled);
    inner.push_str(&ec2_elem_opt(
        "CloudwatchLogGroup",
        log.log_group.as_deref(),
    ));
    inner.push_str(&ec2_elem_opt(
        "CloudwatchLogStream",
        log.log_stream.as_deref(),
    ));
    if let Some(include) = log.include_authorization_policy_context {
        inner.push_str(&format!(
            "<IncludeAuthorizationPolicyContext>{include}</IncludeAuthorizationPolicyContext>"
        ));
    }
    format!("<connectionLogOptions>{inner}</connectionLogOptions>")
}

fn device_posture_xml(providers: &[ClientVpnTrustProvider]) -> String {
    if providers.is_empty() {
        return String::new();
    }
    let items: Vec<String> = providers
        .iter()
        .map(|p| {
            format!(
                "{}{}{}",
                ec2_elem_opt("trustProviderType", p.trust_provider_type.as_deref()),
                ec2_elem_opt("tenantId", p.tenant_id.as_deref()),
                ec2_elem_opt("publicSigningKeyUrl", p.public_signing_key_url.as_deref()),
            )
        })
        .collect();
    format!(
        "<devicePostureOptions>{}</devicePostureOptions>",
        ec2_list("trustProviderSet", &items)
    )
}

fn endpoint_xml(e: &ClientVpnEndpoint, tags: &[Tag], region: &str) -> String {
    format!(
        "{}{}{}{}{}{}<transportProtocol>{}</transportProtocol><vpnPort>443</vpnPort>{}{}{}{}",
        ec2_elem("clientVpnEndpointId", &e.id),
        ec2_elem("description", &e.description),
        status_xml("status", &e.status),
        ec2_elem("creationTime", FIXED_TIME),
        ec2_elem(
            "dnsName",
            &format!("*.{}.clientvpn.{region}.amazonaws.com", e.id)
        ),
        ec2_elem("clientCidrBlock", &e.client_cidr),
        e.transport_protocol,
        ec2_elem("serverCertificateArn", &e.server_cert_arn),
        connection_log_xml(&e.connection_log),
        super::tags::tag_set_xml(tags),
        device_posture_xml(&e.trust_providers),
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
    let connection_log = parse_connection_log(&req.query_params)?.unwrap_or_default();
    let trust_providers = parse_device_posture(&req.query_params)?.unwrap_or_default();
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
        connection_log,
        trust_providers,
        authorization_policy: None,
    };
    {
        let mut accounts = svc.state.write();
        let state = accounts.regional_mut(&req.account_id, &req.region);
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
            &format!("*.{id}.clientvpn.{}.amazonaws.com", req.region)
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
        let state = accounts.regional_mut(&req.account_id, &req.region);
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
    let state = accounts
        .regional(&req.account_id, &req.region)
        .unwrap_or(&empty);
    let mut items: Vec<String> = state
        .client_vpn_endpoints
        .values()
        .filter(|e| wanted.is_empty() || wanted.contains(&e.id))
        .map(|e| endpoint_xml(e, state.tags_for(&e.id), &req.region))
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
    let connection_log = parse_connection_log(&req.query_params)?;
    let trust_providers = parse_device_posture(&req.query_params)?;
    {
        let mut accounts = svc.state.write();
        let e = accounts
            .regional_mut(&req.account_id, &req.region)
            .client_vpn_endpoints
            .get_mut(&id)
            .ok_or_else(|| endpoint_not_found(&id))?;
        if let Some(d) = req.query_params.get("Description") {
            e.description = d.clone();
        }
        if let Some(log) = connection_log {
            e.connection_log = log;
        }
        if let Some(providers) = trust_providers {
            e.trust_providers = providers;
        }
    }
    Ok(Ec2Service::respond(
        "ModifyClientVpnEndpoint",
        &req.request_id,
        &ec2_return(true),
    ))
}

// ---- authorization policy ----

fn authorization_policy_xml(id: &str, p: &ClientVpnAuthorizationPolicy) -> String {
    format!(
        "{}{}{}{}{}",
        ec2_elem("clientVpnEndpointId", id),
        ec2_elem("policyDocument", &p.policy_document),
        ec2_elem_opt(
            "description",
            p.description.as_deref().filter(|d| !d.is_empty())
        ),
        ec2_elem("shadowMode", &p.shadow_mode),
        ec2_elem("status", &p.status),
    )
}

/// Creates the endpoint's authorization policy, or updates the members the
/// request names on the one it already has.
pub(crate) fn modify_client_vpn_endpoint_authorization_policy(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    const ACTION: &str = "ModifyClientVpnEndpointAuthorizationPolicy";
    let id = require(&req.query_params, "ClientVpnEndpointId")?;
    validate_enum(&req.query_params, "ShadowMode", SHADOW_MODES)?;
    let document = opt_string(&req.query_params, "PolicyDocument");
    let description = req.query_params.get("Description").cloned();
    let shadow_mode = opt_string(&req.query_params, "ShadowMode");
    let token = opt_string(&req.query_params, "ClientToken");
    let fingerprint = format!("{document:?}|{description:?}|{shadow_mode:?}");

    let mut accounts = svc.state.write();
    let e = accounts
        .regional_mut(&req.account_id, &req.region)
        .client_vpn_endpoints
        .get_mut(&id)
        .ok_or_else(|| endpoint_not_found(&id))?;

    if let (Some(token), Some(policy)) = (&token, &e.authorization_policy) {
        if let Some(record) = policy
            .client_tokens
            .get(token)
            .filter(|r| !token_expired(r))
        {
            if record.fingerprint != fingerprint {
                return Err(AwsServiceError::aws_error(
                    http::StatusCode::BAD_REQUEST,
                    "IdempotentParameterMismatch",
                    format!(
                        "The client token '{token}' was already used with different parameters"
                    ),
                ));
            }
            return Ok(Ec2Service::respond(
                ACTION,
                &req.request_id,
                &ec2_elem("status", &record.result_id),
            ));
        }
    }

    // The document is required when there is no policy yet to update.
    if e.authorization_policy.is_none() && document.is_none() {
        return Err(missing_parameter("PolicyDocument"));
    }
    if dry_run(req) {
        return Ok(Ec2Service::respond(ACTION, &req.request_id, ""));
    }

    let status = match &mut e.authorization_policy {
        Some(policy) => {
            if let Some(d) = document {
                policy.policy_document = d;
            }
            if let Some(d) = description {
                policy.description = Some(d);
            }
            if let Some(m) = shadow_mode {
                policy.shadow_mode = m;
            }
            "updating"
        }
        None => {
            e.authorization_policy = Some(ClientVpnAuthorizationPolicy {
                policy_document: document.unwrap_or_default(),
                description,
                shadow_mode: shadow_mode.unwrap_or_else(|| "disabled".to_string()),
                status: "active".to_string(),
                client_tokens: Default::default(),
            });
            "creating"
        }
    };
    if let (Some(token), Some(policy)) = (token, &mut e.authorization_policy) {
        prune_client_tokens(&mut policy.client_tokens);
        policy.client_tokens.insert(
            token,
            IpamIdempotencyRecord {
                result_id: status.to_string(),
                fingerprint,
                recorded_at: now_rfc3339(),
            },
        );
    }
    Ok(Ec2Service::respond(
        ACTION,
        &req.request_id,
        &ec2_elem("status", status),
    ))
}

pub(crate) fn get_client_vpn_endpoint_authorization_policy(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "ClientVpnEndpointId")?;
    let accounts = svc.state.read();
    let e = accounts
        .regional(&req.account_id, &req.region)
        .and_then(|s| s.client_vpn_endpoints.get(&id))
        .ok_or_else(|| endpoint_not_found(&id))?;
    let body = if dry_run(req) {
        String::new()
    } else {
        match &e.authorization_policy {
            Some(p) => authorization_policy_xml(&id, p),
            // An endpoint without a policy has nothing to describe beyond
            // its own id.
            None => ec2_elem("clientVpnEndpointId", &id),
        }
    };
    Ok(Ec2Service::respond(
        "GetClientVpnEndpointAuthorizationPolicy",
        &req.request_id,
        &body,
    ))
}

pub(crate) fn delete_client_vpn_endpoint_authorization_policy(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    const ACTION: &str = "DeleteClientVpnEndpointAuthorizationPolicy";
    let id = require(&req.query_params, "ClientVpnEndpointId")?;
    let mut accounts = svc.state.write();
    let e = accounts
        .regional_mut(&req.account_id, &req.region)
        .client_vpn_endpoints
        .get_mut(&id)
        .ok_or_else(|| endpoint_not_found(&id))?;
    if e.authorization_policy.is_none() {
        return Err(invalid_parameter_value(format!(
            "The Client VPN endpoint '{id}' does not have an authorization policy"
        )));
    }
    if dry_run(req) {
        return Ok(Ec2Service::respond(ACTION, &req.request_id, ""));
    }
    e.authorization_policy = None;
    Ok(Ec2Service::respond(
        ACTION,
        &req.request_id,
        &ec2_elem("status", "deleting"),
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
            .regional_mut(&req.account_id, &req.region)
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
            .regional_mut(&req.account_id, &req.region)
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
        .regional(&req.account_id, &req.region)
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
            .regional_mut(&req.account_id, &req.region)
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
            .regional_mut(&req.account_id, &req.region)
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
        .regional(&req.account_id, &req.region)
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
            .regional_mut(&req.account_id, &req.region)
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
            .regional_mut(&req.account_id, &req.region)
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
        .regional(&req.account_id, &req.region)
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{ec2_request, err_of};

    fn create(svc: &Ec2Service, extra: &[(&str, &str)]) -> String {
        let mut params = vec![
            (
                "ServerCertificateArn",
                "arn:aws:acm:us-east-1:0:certificate/c",
            ),
            ("ClientCidrBlock", "10.0.0.0/22"),
        ];
        params.extend_from_slice(extra);
        let b = body(
            create_client_vpn_endpoint(svc, &ec2_request("CreateClientVpnEndpoint", &params))
                .unwrap(),
        );
        b.split("<clientVpnEndpointId>")
            .nth(1)
            .unwrap()
            .split("</clientVpnEndpointId>")
            .next()
            .unwrap()
            .to_string()
    }

    fn describe(svc: &Ec2Service, id: &str) -> String {
        body(
            describe_client_vpn_endpoints(
                svc,
                &ec2_request(
                    "DescribeClientVpnEndpoints",
                    &[("ClientVpnEndpointId.1", id)],
                ),
            )
            .unwrap(),
        )
    }

    fn modify(
        svc: &Ec2Service,
        id: &str,
        extra: &[(&str, &str)],
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut params = vec![("ClientVpnEndpointId", id)];
        params.extend_from_slice(extra);
        modify_client_vpn_endpoint(svc, &ec2_request("ModifyClientVpnEndpoint", &params))
    }

    fn modify_policy(
        svc: &Ec2Service,
        id: &str,
        extra: &[(&str, &str)],
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut params = vec![("ClientVpnEndpointId", id)];
        params.extend_from_slice(extra);
        modify_client_vpn_endpoint_authorization_policy(
            svc,
            &ec2_request("ModifyClientVpnEndpointAuthorizationPolicy", &params),
        )
    }

    fn get_policy(svc: &Ec2Service, id: &str) -> Result<AwsResponse, AwsServiceError> {
        get_client_vpn_endpoint_authorization_policy(
            svc,
            &ec2_request(
                "GetClientVpnEndpointAuthorizationPolicy",
                &[("ClientVpnEndpointId", id)],
            ),
        )
    }

    fn delete_policy(svc: &Ec2Service, id: &str) -> Result<AwsResponse, AwsServiceError> {
        delete_client_vpn_endpoint_authorization_policy(
            svc,
            &ec2_request(
                "DeleteClientVpnEndpointAuthorizationPolicy",
                &[("ClientVpnEndpointId", id)],
            ),
        )
    }

    const POLICY: &str = "permit(principal, action, resource);";

    #[test]
    fn connection_log_options_round_trip_through_create_and_modify() {
        let svc = Ec2Service::new();
        let id = create(
            &svc,
            &[
                ("ConnectionLogOptions.Enabled", "true"),
                ("ConnectionLogOptions.CloudwatchLogGroup", "vpn-logs"),
                ("ConnectionLogOptions.CloudwatchLogStream", "conn"),
                (
                    "ConnectionLogOptions.IncludeAuthorizationPolicyContext",
                    "true",
                ),
            ],
        );
        let d = describe(&svc, &id);
        assert!(
            d.contains(
                "<connectionLogOptions><Enabled>true</Enabled><CloudwatchLogGroup>vpn-logs</CloudwatchLogGroup>\
                 <CloudwatchLogStream>conn</CloudwatchLogStream>\
                 <IncludeAuthorizationPolicyContext>true</IncludeAuthorizationPolicyContext></connectionLogOptions>"
            ),
            "{d}"
        );

        modify(
            &svc,
            &id,
            &[
                ("ConnectionLogOptions.Enabled", "false"),
                (
                    "ConnectionLogOptions.IncludeAuthorizationPolicyContext",
                    "false",
                ),
            ],
        )
        .unwrap();
        let d = describe(&svc, &id);
        assert!(
            d.contains(
                "<connectionLogOptions><Enabled>false</Enabled>\
                 <IncludeAuthorizationPolicyContext>false</IncludeAuthorizationPolicyContext></connectionLogOptions>"
            ),
            "{d}"
        );

        // A Modify that does not name the options leaves them alone.
        modify(&svc, &id, &[("Description", "x")]).unwrap();
        assert!(describe(&svc, &id).contains("<IncludeAuthorizationPolicyContext>false"));
    }

    #[test]
    fn enabled_connection_logging_needs_a_log_group() {
        let svc = Ec2Service::new();
        let err = err_of(create_client_vpn_endpoint(
            &svc,
            &ec2_request(
                "CreateClientVpnEndpoint",
                &[
                    (
                        "ServerCertificateArn",
                        "arn:aws:acm:us-east-1:0:certificate/c",
                    ),
                    ("ConnectionLogOptions.Enabled", "true"),
                ],
            ),
        ));
        assert_eq!(err.code(), "InvalidParameterValue");
        let id = create(&svc, &[("ConnectionLogOptions.Enabled", "false")]);
        let err = err_of(modify(
            &svc,
            &id,
            &[("ConnectionLogOptions.Enabled", "true")],
        ));
        assert_eq!(err.code(), "InvalidParameterValue");
    }

    #[test]
    fn device_posture_options_are_stored_replaced_and_cleared() {
        let svc = Ec2Service::new();
        let id = create(
            &svc,
            &[
                ("DevicePostureOptions.Enabled", "true"),
                (
                    "DevicePostureOptions.TrustProvider.1.TrustProviderType",
                    "crowdstrike",
                ),
                ("DevicePostureOptions.TrustProvider.1.TenantId", "tenant-1"),
                (
                    "DevicePostureOptions.TrustProvider.1.PublicSigningKeyUrl",
                    "https://keys.example.com/cs",
                ),
            ],
        );
        let d = describe(&svc, &id);
        assert!(
            d.contains(
                "<devicePostureOptions><trustProviderSet><item><trustProviderType>crowdstrike</trustProviderType>\
                 <tenantId>tenant-1</tenantId><publicSigningKeyUrl>https://keys.example.com/cs</publicSigningKeyUrl>\
                 </item></trustProviderSet></devicePostureOptions>"
            ),
            "{d}"
        );

        // The block replaces the whole configuration.
        modify(
            &svc,
            &id,
            &[
                (
                    "DevicePostureOptions.TrustProvider.1.TrustProviderType",
                    "jamf",
                ),
                ("DevicePostureOptions.TrustProvider.1.TenantId", "t-j"),
                (
                    "DevicePostureOptions.TrustProvider.2.TrustProviderType",
                    "jumpcloud",
                ),
                ("DevicePostureOptions.TrustProvider.2.TenantId", "t-jc"),
            ],
        )
        .unwrap();
        let d = describe(&svc, &id);
        assert!(!d.contains("crowdstrike"), "{d}");
        assert!(
            d.contains("<trustProviderType>jamf</trustProviderType>"),
            "{d}"
        );
        assert!(
            d.contains("<trustProviderType>jumpcloud</trustProviderType>"),
            "{d}"
        );

        // Disabling device posture clears the providers.
        modify(&svc, &id, &[("DevicePostureOptions.Enabled", "false")]).unwrap();
        assert!(!describe(&svc, &id).contains("devicePostureOptions"));

        let err = err_of(modify(
            &svc,
            &id,
            &[(
                "DevicePostureOptions.TrustProvider.1.TrustProviderType",
                "okta",
            )],
        ));
        assert_eq!(err.code(), "InvalidParameterValue");
    }

    #[test]
    fn modify_on_a_missing_endpoint_is_not_found() {
        let svc = Ec2Service::new();
        let err = err_of(modify(&svc, "cvpn-endpoint-ghost", &[("Description", "x")]));
        assert_eq!(err.code(), "InvalidClientVpnEndpointId.NotFound");
    }

    #[test]
    fn authorization_policy_crud() {
        let svc = Ec2Service::new();
        let id = create(&svc, &[]);

        // No policy yet: Get describes only the endpoint, Delete refuses, and
        // a Modify without a document cannot create one.
        let g = body(get_policy(&svc, &id).unwrap());
        assert!(g.contains(&format!("<clientVpnEndpointId>{id}</clientVpnEndpointId>")));
        assert!(!g.contains("<policyDocument>"), "{g}");
        assert_eq!(
            err_of(delete_policy(&svc, &id)).code(),
            "InvalidParameterValue"
        );
        assert_eq!(
            err_of(modify_policy(&svc, &id, &[("Description", "d")])).code(),
            "MissingParameter"
        );

        let r = body(
            modify_policy(
                &svc,
                &id,
                &[("PolicyDocument", POLICY), ("Description", "first")],
            )
            .unwrap(),
        );
        assert!(r.contains("<status>creating</status>"), "{r}");
        let g = body(get_policy(&svc, &id).unwrap());
        assert!(
            g.contains(&format!("<policyDocument>{POLICY}</policyDocument>")),
            "{g}"
        );
        assert!(g.contains("<description>first</description>"), "{g}");
        assert!(g.contains("<shadowMode>disabled</shadowMode>"), "{g}");
        assert!(g.contains("<status>active</status>"), "{g}");

        // An update replaces only what it names.
        let r = body(modify_policy(&svc, &id, &[("ShadowMode", "enabled")]).unwrap());
        assert!(r.contains("<status>updating</status>"), "{r}");
        let g = body(get_policy(&svc, &id).unwrap());
        assert!(g.contains("<shadowMode>enabled</shadowMode>"), "{g}");
        assert!(
            g.contains(&format!("<policyDocument>{POLICY}</policyDocument>")),
            "{g}"
        );
        assert!(g.contains("<description>first</description>"), "{g}");

        let r = body(delete_policy(&svc, &id).unwrap());
        assert!(r.contains("<status>deleting</status>"), "{r}");
        assert!(!body(get_policy(&svc, &id).unwrap()).contains("<policyDocument>"));
        assert_eq!(
            err_of(delete_policy(&svc, &id)).code(),
            "InvalidParameterValue"
        );
    }

    #[test]
    fn authorization_policy_ops_validate_endpoint_and_shadow_mode() {
        let svc = Ec2Service::new();
        let ghost = "cvpn-endpoint-ghost";
        for err in [
            err_of(modify_policy(&svc, ghost, &[("PolicyDocument", POLICY)])),
            err_of(get_policy(&svc, ghost)),
            err_of(delete_policy(&svc, ghost)),
        ] {
            assert_eq!(err.code(), "InvalidClientVpnEndpointId.NotFound");
        }
        let id = create(&svc, &[]);
        let err = err_of(modify_policy(
            &svc,
            &id,
            &[("PolicyDocument", POLICY), ("ShadowMode", "sometimes")],
        ));
        assert_eq!(err.code(), "InvalidParameterValue");
    }

    #[test]
    fn authorization_policy_modify_replays_its_client_token() {
        let svc = Ec2Service::new();
        let id = create(&svc, &[]);
        let args = [("PolicyDocument", POLICY), ("ClientToken", "tok-1")];
        let first = body(modify_policy(&svc, &id, &args).unwrap());
        assert!(first.contains("<status>creating</status>"), "{first}");
        // The retry answers as the original call did, not as an update.
        let retry = body(modify_policy(&svc, &id, &args).unwrap());
        assert!(retry.contains("<status>creating</status>"), "{retry}");
        let err = err_of(modify_policy(
            &svc,
            &id,
            &[
                ("PolicyDocument", "forbid(principal, action, resource);"),
                ("ClientToken", "tok-1"),
            ],
        ));
        assert_eq!(err.code(), "IdempotentParameterMismatch");
    }

    #[test]
    fn authorization_policy_client_tokens_are_aged_out_and_capped() {
        use crate::service::ipam_registry::{CLIENT_TOKEN_MAX_RECORDS, CLIENT_TOKEN_TTL_SECONDS};
        let svc = Ec2Service::new();
        let id = create(&svc, &[]);
        modify_policy(
            &svc,
            &id,
            &[("PolicyDocument", POLICY), ("ClientToken", "old")],
        )
        .unwrap();
        // Age the first record past the idempotency window.
        {
            let mut accounts = svc.state.write();
            let policy = accounts
                .regional_mut("000000000000", "us-east-1")
                .client_vpn_endpoints
                .get_mut(&id)
                .unwrap()
                .authorization_policy
                .as_mut()
                .unwrap();
            let stale =
                chrono::Utc::now() - chrono::Duration::seconds(CLIENT_TOKEN_TTL_SECONDS + 60);
            policy.client_tokens.get_mut("old").unwrap().recorded_at = stale.to_rfc3339();
        }
        // An expired token no longer replays: the call is applied as new.
        let again = body(
            modify_policy(
                &svc,
                &id,
                &[("PolicyDocument", POLICY), ("ClientToken", "old")],
            )
            .unwrap(),
        );
        assert!(again.contains("<status>updating</status>"), "{again}");

        // SDKs mint a fresh token per call; the records stay capped.
        for i in 0..CLIENT_TOKEN_MAX_RECORDS + 50 {
            let token = format!("tok-{i}");
            modify_policy(&svc, &id, &[("ClientToken", token.as_str())]).unwrap();
        }
        let accounts = svc.state.read();
        let kept = accounts
            .regional("000000000000", "us-east-1")
            .unwrap()
            .client_vpn_endpoints[&id]
            .authorization_policy
            .as_ref()
            .unwrap()
            .client_tokens
            .len();
        assert!(kept <= CLIENT_TOKEN_MAX_RECORDS, "{kept} records kept");
    }

    #[test]
    fn authorization_policy_dry_run_changes_nothing() {
        let svc = Ec2Service::new();
        let id = create(&svc, &[]);
        modify_policy(&svc, &id, &[("PolicyDocument", POLICY), ("DryRun", "true")]).unwrap();
        assert!(!body(get_policy(&svc, &id).unwrap()).contains("<policyDocument>"));
        modify_policy(&svc, &id, &[("PolicyDocument", POLICY)]).unwrap();
        delete_client_vpn_endpoint_authorization_policy(
            &svc,
            &ec2_request(
                "DeleteClientVpnEndpointAuthorizationPolicy",
                &[("ClientVpnEndpointId", &id), ("DryRun", "true")],
            ),
        )
        .unwrap();
        assert!(body(get_policy(&svc, &id).unwrap()).contains("<policyDocument>"));
    }

    fn body(resp: AwsResponse) -> String {
        String::from_utf8_lossy(resp.body.expect_bytes()).to_string()
    }

    #[test]
    fn dns_name_uses_request_region() {
        let svc = Ec2Service::new();
        // Client is in eu-west-1; the returned Client VPN DNS name host must
        // carry that region, not a hardcoded us-east-1.
        let mut r = ec2_request(
            "CreateClientVpnEndpoint",
            &[
                (
                    "ServerCertificateArn",
                    "arn:aws:acm:eu-west-1:0:certificate/c",
                ),
                ("TransportProtocol", "udp"),
                ("ClientCidrBlock", "10.0.0.0/22"),
            ],
        );
        r.region = "eu-west-1".to_string();
        let created = body(create_client_vpn_endpoint(&svc, &r).unwrap());
        assert!(
            created.contains(".clientvpn.eu-west-1.amazonaws.com"),
            "create dnsName not request-scoped: {created}"
        );
        assert!(
            !created.contains("us-east-1"),
            "create dnsName leaked us-east-1: {created}"
        );

        // Describe emits the same regional host from endpoint_xml.
        let mut d = ec2_request("DescribeClientVpnEndpoints", &[]);
        d.region = "eu-west-1".to_string();
        let desc = body(describe_client_vpn_endpoints(&svc, &d).unwrap());
        assert!(
            desc.contains(".clientvpn.eu-west-1.amazonaws.com"),
            "describe dnsName not request-scoped: {desc}"
        );
    }
}
