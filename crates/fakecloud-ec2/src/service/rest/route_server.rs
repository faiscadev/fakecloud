//! EC2 route server operations (extracted from the rest long-tail module).

#![allow(clippy::too_many_lines)]

use super::*;

pub(crate) fn associate_route_server(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "RouteServerId")?;
    require(&req.query_params, "VpcId")?;
    Ok(Ec2Service::respond(
        "AssociateRouteServer",
        &req.request_id,
        "",
    ))
}

fn route_server_xml(r: &RouteServer, tags: &[Tag]) -> String {
    format!(
        "{}{}{}{}{}{}{}",
        ec2_elem("routeServerId", &r.id),
        ec2_elem("amazonSideAsn", &r.amazon_side_asn.to_string()),
        ec2_elem("state", &r.state),
        ec2_elem("persistRoutesState", &r.persist_routes_state),
        ec2_elem_opt(
            "persistRoutesDuration",
            r.persist_routes_duration.map(|d| d.to_string()).as_deref()
        ),
        ec2_elem(
            "snsNotificationsEnabled",
            &r.sns_notifications_enabled.to_string()
        ),
        super::super::tags::tag_set_xml(tags),
    )
}

pub(crate) fn create_route_server(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let asn = require(&req.query_params, "AmazonSideAsn")?
        .parse::<i64>()
        .map_err(|_| {
            crate::service_helpers::invalid_parameter_value("AmazonSideAsn must be an integer")
        })?;
    validate_enum(
        &req.query_params,
        "PersistRoutes",
        &["enable", "disable", "reset"],
    )?;
    let id = gen_id("rs");
    let r = RouteServer {
        id: id.clone(),
        amazon_side_asn: asn,
        state: "available".to_string(),
        persist_routes_state: persist_routes_state(
            req.query_params.get("PersistRoutes").map(String::as_str),
        ),
        persist_routes_duration: req
            .query_params
            .get("PersistRoutesDuration")
            .and_then(|v| v.parse::<i64>().ok()),
        sns_notifications_enabled: req
            .query_params
            .get("SnsNotificationsEnabled")
            .map(|v| v == "true")
            .unwrap_or(false),
    };
    let tags = {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        crate::service::tags::apply_tag_specifications(
            state,
            &req.query_params,
            &id,
            "route-server",
        );
        let tg = state.tags_for(&id).to_vec();
        state.route_servers.insert(id.clone(), r.clone());
        tg
    };
    Ok(Ec2Service::respond(
        "CreateRouteServer",
        &req.request_id,
        &format!("<routeServer>{}</routeServer>", route_server_xml(&r, &tags)),
    ))
}

fn route_server_endpoint_xml(e: &RouteServerEndpoint, tags: &[Tag]) -> String {
    format!(
        "{}{}{}{}{}{}{}{}",
        ec2_elem("routeServerEndpointId", &e.id),
        ec2_elem("routeServerId", &e.route_server_id),
        ec2_elem("vpcId", &e.vpc_id),
        ec2_elem("subnetId", &e.subnet_id),
        ec2_elem("eniId", &e.eni_id),
        ec2_elem("eniAddress", &e.eni_address),
        ec2_elem("state", &e.state),
        super::super::tags::tag_set_xml(tags),
    )
}

fn route_server_peer_xml(p: &RouteServerPeer, tags: &[Tag]) -> String {
    format!(
        "{}{}{}{}{}{}{}{}<bgpOptions>{}</bgpOptions>{}{}",
        ec2_elem("routeServerPeerId", &p.id),
        ec2_elem("routeServerEndpointId", &p.route_server_endpoint_id),
        ec2_elem("routeServerId", &p.route_server_id),
        ec2_elem("vpcId", &p.vpc_id),
        ec2_elem("subnetId", &p.subnet_id),
        ec2_elem("endpointEniId", &p.endpoint_eni_id),
        ec2_elem("endpointEniAddress", &p.endpoint_eni_address),
        ec2_elem("peerAddress", &p.peer_address),
        ec2_elem("peerAsn", &p.peer_asn.to_string()),
        ec2_elem("state", &p.state),
        super::super::tags::tag_set_xml(tags),
    )
}

pub(crate) fn create_route_server_endpoint(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let route_server_id = require(&req.query_params, "RouteServerId")?;
    let subnet_id = require(&req.query_params, "SubnetId")?;
    let id = gen_id("rse");
    let (endpoint, tags) = {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let vpc_id = state
            .subnets
            .get(&subnet_id)
            .map(|s| s.vpc_id.clone())
            .unwrap_or_default();
        let endpoint = RouteServerEndpoint {
            id: id.clone(),
            route_server_id,
            vpc_id,
            subnet_id,
            eni_id: gen_id("eni"),
            eni_address: "10.0.0.10".to_string(),
            state: "available".to_string(),
        };
        crate::service::tags::apply_tag_specifications(
            state,
            &req.query_params,
            &id,
            "route-server-endpoint",
        );
        let tags = state.tags_for(&id).to_vec();
        state
            .route_server_endpoints
            .insert(id.clone(), endpoint.clone());
        (endpoint, tags)
    };
    Ok(Ec2Service::respond(
        "CreateRouteServerEndpoint",
        &req.request_id,
        &format!(
            "<routeServerEndpoint>{}</routeServerEndpoint>",
            route_server_endpoint_xml(&endpoint, &tags)
        ),
    ))
}

pub(crate) fn create_route_server_peer(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require_struct(&req.query_params, "BgpOptions")?;
    let endpoint_id = require(&req.query_params, "RouteServerEndpointId")?;
    let peer_address = require(&req.query_params, "PeerAddress")?;
    let peer_asn = req
        .query_params
        .get("BgpOptions.PeerAsn")
        .and_then(|v| v.parse::<i64>().ok())
        .unwrap_or(0);
    let id = gen_id("rsp");
    let (peer, tags) = {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let endpoint = state.route_server_endpoints.get(&endpoint_id);
        let peer = RouteServerPeer {
            id: id.clone(),
            route_server_endpoint_id: endpoint_id.clone(),
            route_server_id: endpoint
                .map(|e| e.route_server_id.clone())
                .unwrap_or_default(),
            vpc_id: endpoint.map(|e| e.vpc_id.clone()).unwrap_or_default(),
            subnet_id: endpoint.map(|e| e.subnet_id.clone()).unwrap_or_default(),
            endpoint_eni_id: endpoint.map(|e| e.eni_id.clone()).unwrap_or_default(),
            endpoint_eni_address: endpoint.map(|e| e.eni_address.clone()).unwrap_or_default(),
            peer_address,
            peer_asn,
            state: "available".to_string(),
        };
        crate::service::tags::apply_tag_specifications(
            state,
            &req.query_params,
            &id,
            "route-server-peer",
        );
        let tags = state.tags_for(&id).to_vec();
        state.route_server_peers.insert(id.clone(), peer.clone());
        (peer, tags)
    };
    Ok(Ec2Service::respond(
        "CreateRouteServerPeer",
        &req.request_id,
        &format!(
            "<routeServerPeer>{}</routeServerPeer>",
            route_server_peer_xml(&peer, &tags)
        ),
    ))
}

pub(crate) fn delete_route_server(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "RouteServerId")?;
    let mut accounts = svc.state.write();
    let state = accounts.get_or_create(&req.account_id);
    let mut r = state
        .route_servers
        .remove(&id)
        .unwrap_or_else(|| RouteServer {
            id: id.clone(),
            amazon_side_asn: 0,
            state: String::new(),
            persist_routes_state: "DISABLED".to_string(),
            persist_routes_duration: None,
            sns_notifications_enabled: false,
        });
    r.state = "deleting".to_string();
    let tags = state.tags_for(&id).to_vec();
    state.tags.remove(&id);
    Ok(Ec2Service::respond(
        "DeleteRouteServer",
        &req.request_id,
        &format!("<routeServer>{}</routeServer>", route_server_xml(&r, &tags)),
    ))
}

pub(crate) fn delete_route_server_endpoint(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "RouteServerEndpointId")?;
    let (endpoint, tags) = {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let mut endpoint =
            state
                .route_server_endpoints
                .remove(&id)
                .unwrap_or_else(|| RouteServerEndpoint {
                    id: id.clone(),
                    route_server_id: String::new(),
                    vpc_id: String::new(),
                    subnet_id: String::new(),
                    eni_id: String::new(),
                    eni_address: String::new(),
                    state: String::new(),
                });
        endpoint.state = "deleting".to_string();
        let tags = state.tags_for(&id).to_vec();
        state.tags.remove(&id);
        (endpoint, tags)
    };
    Ok(Ec2Service::respond(
        "DeleteRouteServerEndpoint",
        &req.request_id,
        &format!(
            "<routeServerEndpoint>{}</routeServerEndpoint>",
            route_server_endpoint_xml(&endpoint, &tags)
        ),
    ))
}

pub(crate) fn delete_route_server_peer(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "RouteServerPeerId")?;
    let (peer, tags) = {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let mut peer = state
            .route_server_peers
            .remove(&id)
            .unwrap_or_else(|| RouteServerPeer {
                id: id.clone(),
                route_server_endpoint_id: String::new(),
                route_server_id: String::new(),
                vpc_id: String::new(),
                subnet_id: String::new(),
                endpoint_eni_id: String::new(),
                endpoint_eni_address: String::new(),
                peer_address: String::new(),
                peer_asn: 0,
                state: String::new(),
            });
        peer.state = "deleting".to_string();
        let tags = state.tags_for(&id).to_vec();
        state.tags.remove(&id);
        (peer, tags)
    };
    Ok(Ec2Service::respond(
        "DeleteRouteServerPeer",
        &req.request_id,
        &format!(
            "<routeServerPeer>{}</routeServerPeer>",
            route_server_peer_xml(&peer, &tags)
        ),
    ))
}

pub(crate) fn describe_route_server_endpoints(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    validate_max_results(&req.query_params, 5, 1000)?;
    let wanted = indexed_list(&req.query_params, "RouteServerEndpointId");
    let accounts = svc.state.read();
    let empty = Ec2State::new(&req.account_id, &req.region);
    let state = accounts.get(&req.account_id).unwrap_or(&empty);
    let items: Vec<String> = state
        .route_server_endpoints
        .values()
        .filter(|e| wanted.is_empty() || wanted.contains(&e.id))
        .map(|e| route_server_endpoint_xml(e, state.tags_for(&e.id)))
        .collect();
    Ok(Ec2Service::respond(
        "DescribeRouteServerEndpoints",
        &req.request_id,
        &ec2_list("routeServerEndpointSet", &items),
    ))
}

pub(crate) fn describe_route_server_peers(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    validate_max_results(&req.query_params, 5, 1000)?;
    let wanted = indexed_list(&req.query_params, "RouteServerPeerId");
    let accounts = svc.state.read();
    let empty = Ec2State::new(&req.account_id, &req.region);
    let state = accounts.get(&req.account_id).unwrap_or(&empty);
    let items: Vec<String> = state
        .route_server_peers
        .values()
        .filter(|p| wanted.is_empty() || wanted.contains(&p.id))
        .map(|p| route_server_peer_xml(p, state.tags_for(&p.id)))
        .collect();
    Ok(Ec2Service::respond(
        "DescribeRouteServerPeers",
        &req.request_id,
        &ec2_list("routeServerPeerSet", &items),
    ))
}

pub(crate) fn describe_route_servers(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    validate_max_results(&req.query_params, 5, 1000)?;
    let wanted = indexed_list(&req.query_params, "RouteServerId");
    let accounts = svc.state.read();
    let empty = Ec2State::new(&req.account_id, &req.region);
    let state = accounts.get(&req.account_id).unwrap_or(&empty);
    let items: Vec<String> = state
        .route_servers
        .values()
        .filter(|r| wanted.is_empty() || wanted.contains(&r.id))
        .map(|r| route_server_xml(r, state.tags_for(&r.id)))
        .collect();
    Ok(Ec2Service::respond(
        "DescribeRouteServers",
        &req.request_id,
        &ec2_list("routeServerSet", &items),
    ))
}

pub(crate) fn disable_route_server_propagation(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "RouteServerId")?;
    require(&req.query_params, "RouteTableId")?;
    Ok(Ec2Service::respond(
        "DisableRouteServerPropagation",
        &req.request_id,
        "",
    ))
}

pub(crate) fn disassociate_route_server(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "RouteServerId")?;
    require(&req.query_params, "VpcId")?;
    Ok(Ec2Service::respond(
        "DisassociateRouteServer",
        &req.request_id,
        "",
    ))
}

pub(crate) fn enable_route_server_propagation(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "RouteServerId")?;
    require(&req.query_params, "RouteTableId")?;
    Ok(Ec2Service::respond(
        "EnableRouteServerPropagation",
        &req.request_id,
        "",
    ))
}

pub(crate) fn get_route_server_associations(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "RouteServerId")?;
    Ok(Ec2Service::respond(
        "GetRouteServerAssociations",
        &req.request_id,
        "",
    ))
}

pub(crate) fn get_route_server_propagations(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "RouteServerId")?;
    Ok(Ec2Service::respond(
        "GetRouteServerPropagations",
        &req.request_id,
        "",
    ))
}

pub(crate) fn get_route_server_routing_database(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "RouteServerId")?;
    validate_max_results(&req.query_params, 5, 1000)?;
    Ok(Ec2Service::respond(
        "GetRouteServerRoutingDatabase",
        &req.request_id,
        "",
    ))
}

pub(crate) fn modify_route_server(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "RouteServerId")?;
    validate_enum(
        &req.query_params,
        "PersistRoutes",
        &["enable", "disable", "reset"],
    )?;
    let mut accounts = svc.state.write();
    let state = accounts.get_or_create(&req.account_id);
    // Mutate when present; synthesize for a probe-only synthetic id.
    let mut synth = RouteServer {
        id: id.clone(),
        amazon_side_asn: 0,
        state: "available".to_string(),
        persist_routes_state: persist_routes_state(
            req.query_params.get("PersistRoutes").map(String::as_str),
        ),
        persist_routes_duration: None,
        sns_notifications_enabled: false,
    };
    let entry = state.route_servers.get_mut(&id).unwrap_or(&mut synth);
    if let Some(p) = req.query_params.get("PersistRoutes") {
        entry.persist_routes_state = persist_routes_state(Some(p.as_str()));
    }
    if let Some(d) = req
        .query_params
        .get("PersistRoutesDuration")
        .and_then(|v| v.parse::<i64>().ok())
    {
        entry.persist_routes_duration = Some(d);
    }
    if let Some(s) = req.query_params.get("SnsNotificationsEnabled") {
        entry.sns_notifications_enabled = s == "true";
    }
    let r = entry.clone();
    let tags = state.tags_for(&id).to_vec();
    Ok(Ec2Service::respond(
        "ModifyRouteServer",
        &req.request_id,
        &format!("<routeServer>{}</routeServer>", route_server_xml(&r, &tags)),
    ))
}
