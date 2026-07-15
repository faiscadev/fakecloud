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

pub(crate) fn create_route_server_endpoint(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "RouteServerId")?;
    require(&req.query_params, "SubnetId")?;
    Ok(Ec2Service::respond(
        "CreateRouteServerEndpoint",
        &req.request_id,
        "",
    ))
}

pub(crate) fn create_route_server_peer(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require_struct(&req.query_params, "BgpOptions")?;
    require(&req.query_params, "RouteServerEndpointId")?;
    require(&req.query_params, "PeerAddress")?;
    Ok(Ec2Service::respond(
        "CreateRouteServerPeer",
        &req.request_id,
        "",
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
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "RouteServerEndpointId")?;
    Ok(Ec2Service::respond(
        "DeleteRouteServerEndpoint",
        &req.request_id,
        "",
    ))
}

pub(crate) fn delete_route_server_peer(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "RouteServerPeerId")?;
    Ok(Ec2Service::respond(
        "DeleteRouteServerPeer",
        &req.request_id,
        "",
    ))
}

pub(crate) fn describe_route_server_endpoints(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    validate_max_results(&req.query_params, 5, 1000)?;
    Ok(Ec2Service::respond(
        "DescribeRouteServerEndpoints",
        &req.request_id,
        "",
    ))
}

pub(crate) fn describe_route_server_peers(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    validate_max_results(&req.query_params, 5, 1000)?;
    Ok(Ec2Service::respond(
        "DescribeRouteServerPeers",
        &req.request_id,
        "",
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
