//! VPC endpoints, endpoint service configurations, connection notifications,
//! and VPC flow logs.

use fakecloud_aws::ec2query::{ec2_elem, ec2_list, ec2_return, ec2_scalar_list};
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use crate::service::Ec2Service;
use crate::service_helpers::{gen_id, indexed_list, require, validate_enum, validate_max_results};
use crate::state::{ConnectionNotification, Ec2State, EndpointService, FlowLog, Tag, VpcEndpoint};

const FIXED_TIME: &str = "2024-01-01T00:00:00.000Z";

// ---- VPC endpoints ----

/// Render the endpoint `<groupSet>` from attached security-group ids.
fn groups_xml(group_ids: &[String]) -> String {
    let items: Vec<String> = group_ids
        .iter()
        .map(|g| format!("<groupId>{}</groupId>", fakecloud_aws::xml::xml_escape(g)))
        .collect();
    ec2_list("groupSet", &items)
}

fn endpoint_xml(e: &VpcEndpoint, tags: &[Tag], owner: &str) -> String {
    format!(
        "{}{}{}{}{}<privateDnsEnabled>{}</privateDnsEnabled><requesterManaged>false</requesterManaged>\
         {}{}{}{}{}{}{}{}",
        ec2_elem("vpcEndpointId", &e.id),
        ec2_elem("vpcEndpointType", &e.endpoint_type),
        ec2_elem("vpcId", &e.vpc_id),
        ec2_elem("serviceName", &e.service_name),
        ec2_elem("state", &e.state),
        e.private_dns_enabled,
        ec2_elem("ipAddressType", "ipv4"),
        ec2_list("subnetIdSet", &e.subnet_ids),
        groups_xml(&e.security_group_ids),
        ec2_list("routeTableIdSet", &e.route_table_ids),
        ec2_list("networkInterfaceIdSet", &[]),
        ec2_elem("creationTimestamp", FIXED_TIME),
        ec2_elem("ownerId", owner),
        super::tags::tag_set_xml(tags),
    )
}

pub(crate) fn create_vpc_endpoint(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let vpc_id = require(&req.query_params, "VpcId")?;
    validate_enum(
        &req.query_params,
        "VpcEndpointType",
        &[
            "Interface",
            "Gateway",
            "GatewayLoadBalancer",
            "Resource",
            "ServiceNetwork",
        ],
    )?;
    validate_enum(
        &req.query_params,
        "IpAddressType",
        &["ipv4", "dualstack", "ipv6"],
    )?;
    let id = gen_id("vpce");
    let e = VpcEndpoint {
        id: id.clone(),
        endpoint_type: req
            .query_params
            .get("VpcEndpointType")
            .cloned()
            .unwrap_or_else(|| "Gateway".to_string()),
        vpc_id,
        service_name: req
            .query_params
            .get("ServiceName")
            .cloned()
            .unwrap_or_default(),
        state: "available".to_string(),
        subnet_ids: indexed_list(&req.query_params, "SubnetId"),
        route_table_ids: indexed_list(&req.query_params, "RouteTableId"),
        private_dns_enabled: req
            .query_params
            .get("PrivateDnsEnabled")
            .map(|v| v == "true")
            .unwrap_or(false),
        security_group_ids: indexed_list(&req.query_params, "SecurityGroupId"),
        payer_responsibility: "vpc-endpoint-account".to_string(),
    };
    let owner = req.account_id.clone();
    let tags = {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        crate::service::tags::apply_tag_specifications(
            state,
            &req.query_params,
            &id,
            "vpc-endpoint",
        );
        let t = state.tags_for(&id).to_vec();
        state.vpc_endpoints.insert(id.clone(), e.clone());
        t
    };
    let body = format!(
        "<vpcEndpoint>{}</vpcEndpoint>{}",
        endpoint_xml(&e, &tags, &owner),
        ec2_elem("clientToken", &gen_id("token"))
    );
    Ok(Ec2Service::respond(
        "CreateVpcEndpoint",
        &req.request_id,
        &body,
    ))
}

pub(crate) fn delete_vpc_endpoints(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let ids = indexed_list(&req.query_params, "VpcEndpointId");
    {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        for id in &ids {
            state.vpc_endpoints.remove(id);
            state.tags.remove(id);
        }
    }
    Ok(Ec2Service::respond(
        "DeleteVpcEndpoints",
        &req.request_id,
        &ec2_list("unsuccessful", &[]),
    ))
}

pub(crate) fn describe_vpc_endpoints(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let wanted = indexed_list(&req.query_params, "VpcEndpointId");
    let owner = req.account_id.clone();
    let accounts = svc.state.read();
    let empty = Ec2State::new(&req.account_id, &req.region);
    let state = accounts.get(&req.account_id).unwrap_or(&empty);
    let mut items: Vec<String> = state
        .vpc_endpoints
        .values()
        .filter(|e| wanted.is_empty() || wanted.contains(&e.id))
        .map(|e| endpoint_xml(e, state.tags_for(&e.id), &owner))
        .collect();
    items.sort();
    Ok(Ec2Service::respond(
        "DescribeVpcEndpoints",
        &req.request_id,
        &ec2_list("vpcEndpointSet", &items),
    ))
}

pub(crate) fn modify_vpc_endpoint(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "VpcEndpointId")?;
    validate_enum(
        &req.query_params,
        "IpAddressType",
        &["ipv4", "dualstack", "ipv6"],
    )?;
    let add_subnets = indexed_list(&req.query_params, "AddSubnetId");
    let remove_subnets = indexed_list(&req.query_params, "RemoveSubnetId");
    let add_rts = indexed_list(&req.query_params, "AddRouteTableId");
    let remove_rts = indexed_list(&req.query_params, "RemoveRouteTableId");
    let add_sgs = indexed_list(&req.query_params, "AddSecurityGroupId");
    let remove_sgs = indexed_list(&req.query_params, "RemoveSecurityGroupId");
    let private_dns = req
        .query_params
        .get("PrivateDnsEnabled")
        .map(|v| v == "true");
    {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let e = state.vpc_endpoints.get_mut(&id).ok_or_else(|| {
            AwsServiceError::aws_error(
                http::StatusCode::BAD_REQUEST,
                "InvalidVpcEndpointId.NotFound",
                format!("The Vpc Endpoint Id '{id}' does not exist"),
            )
        })?;
        for s in &add_subnets {
            if !e.subnet_ids.contains(s) {
                e.subnet_ids.push(s.clone());
            }
        }
        e.subnet_ids.retain(|s| !remove_subnets.contains(s));
        for r in &add_rts {
            if !e.route_table_ids.contains(r) {
                e.route_table_ids.push(r.clone());
            }
        }
        e.route_table_ids.retain(|r| !remove_rts.contains(r));
        for g in &add_sgs {
            if !e.security_group_ids.contains(g) {
                e.security_group_ids.push(g.clone());
            }
        }
        e.security_group_ids.retain(|g| !remove_sgs.contains(g));
        if let Some(v) = private_dns {
            e.private_dns_enabled = v;
        }
    }
    Ok(Ec2Service::respond(
        "ModifyVpcEndpoint",
        &req.request_id,
        &ec2_return(true),
    ))
}

/// `ModifyVpcEndpointPayerResponsibility` — set who pays for an endpoint's
/// charges. Persists the choice on the endpoint and echoes the resulting
/// `payerResponsibilitySet`.
pub(crate) fn modify_vpc_endpoint_payer_responsibility(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "VpcEndpointId")?;
    let payer = require(&req.query_params, "PayerResponsibility")?;
    validate_enum(
        &req.query_params,
        "PayerResponsibility",
        &["vpc-endpoint-account", "vpc-endpoint-service-account"],
    )?;
    let scope = require(&req.query_params, "Scope")?;
    validate_enum(&req.query_params, "Scope", &["vpc-endpoint-charges"])?;
    {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let e = state.vpc_endpoints.get_mut(&id).ok_or_else(|| {
            AwsServiceError::aws_error(
                http::StatusCode::BAD_REQUEST,
                "InvalidVpcEndpointId.NotFound",
                format!("The Vpc Endpoint Id '{id}' does not exist"),
            )
        })?;
        e.payer_responsibility = payer.clone();
    }
    let entry = format!(
        "<item>{}{}</item>",
        ec2_elem("scope", &scope),
        ec2_elem("payerResponsibilityType", &payer),
    );
    let body = format!(
        "{}<payerResponsibilitySet>{}</payerResponsibilitySet>",
        ec2_elem("vpcEndpointId", &id),
        entry,
    );
    Ok(Ec2Service::respond(
        "ModifyVpcEndpointPayerResponsibility",
        &req.request_id,
        &body,
    ))
}

pub(crate) fn describe_vpc_endpoint_services(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let region = if req.region.is_empty() {
        "us-east-1"
    } else {
        &req.region
    };
    let names: Vec<String> = ["s3", "dynamodb", "ec2"]
        .iter()
        .map(|s| format!("com.amazonaws.{region}.{s}"))
        .collect();
    let name_items: Vec<String> = names.clone();
    let details: Vec<String> = names
        .iter()
        .map(|n| {
            format!(
                "{}{}<serviceType><item><serviceType>Gateway</serviceType></item></serviceType><acceptanceRequired>false</acceptanceRequired>{}",
                ec2_elem("serviceName", n),
                ec2_elem("owner", "amazon"),
                ec2_elem("serviceId", &gen_id("vpce-svc")),
            )
        })
        .collect();
    let body = format!(
        "{}{}",
        ec2_scalar_list("serviceNameSet", &name_items),
        ec2_list("serviceDetailSet", &details)
    );
    Ok(Ec2Service::respond(
        "DescribeVpcEndpointServices",
        &req.request_id,
        &body,
    ))
}

pub(crate) fn describe_vpc_endpoint_connections(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    Ok(Ec2Service::respond(
        "DescribeVpcEndpointConnections",
        &req.request_id,
        &ec2_list("vpcEndpointConnectionSet", &[]),
    ))
}

pub(crate) fn accept_vpc_endpoint_connections(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "ServiceId")?;
    Ok(Ec2Service::respond(
        "AcceptVpcEndpointConnections",
        &req.request_id,
        &ec2_list("unsuccessful", &[]),
    ))
}

pub(crate) fn reject_vpc_endpoint_connections(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "ServiceId")?;
    Ok(Ec2Service::respond(
        "RejectVpcEndpointConnections",
        &req.request_id,
        &ec2_list("unsuccessful", &[]),
    ))
}

// ---- endpoint service configurations ----

fn service_config_xml(s: &EndpointService, tags: &[Tag]) -> String {
    format!(
        "{}{}{}<acceptanceRequired>{}</acceptanceRequired><managesVpcEndpoints>false</managesVpcEndpoints>{}{}{}",
        ec2_elem("serviceId", &s.service_id),
        ec2_elem("serviceName", &s.service_name),
        ec2_elem("serviceState", &s.state),
        s.acceptance_required,
        ec2_elem("payerResponsibility", &s.payer_responsibility),
        ec2_list("networkLoadBalancerArnSet", &s.nlb_arns),
        super::tags::tag_set_xml(tags),
    )
}

pub(crate) fn create_vpc_endpoint_service_configuration(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = gen_id("vpce-svc");
    let s = EndpointService {
        service_id: id.clone(),
        service_name: format!(
            "com.amazonaws.vpce.{}.{id}",
            if req.region.is_empty() {
                "us-east-1"
            } else {
                &req.region
            }
        ),
        state: "Available".to_string(),
        acceptance_required: req
            .query_params
            .get("AcceptanceRequired")
            .map(|v| v == "true")
            .unwrap_or(true),
        payer_responsibility: "ServiceOwner".to_string(),
        nlb_arns: indexed_list(&req.query_params, "NetworkLoadBalancerArn"),
    };
    let tags = {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        crate::service::tags::apply_tag_specifications(
            state,
            &req.query_params,
            &id,
            "vpc-endpoint-service",
        );
        let t = state.tags_for(&id).to_vec();
        state.endpoint_services.insert(id.clone(), s.clone());
        t
    };
    let body = format!(
        "<serviceConfiguration>{}</serviceConfiguration>{}",
        service_config_xml(&s, &tags),
        ec2_elem("clientToken", &gen_id("token"))
    );
    Ok(Ec2Service::respond(
        "CreateVpcEndpointServiceConfiguration",
        &req.request_id,
        &body,
    ))
}

pub(crate) fn delete_vpc_endpoint_service_configurations(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let ids = indexed_list(&req.query_params, "ServiceId");
    {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        for id in &ids {
            state.endpoint_services.remove(id);
            state.tags.remove(id);
        }
    }
    Ok(Ec2Service::respond(
        "DeleteVpcEndpointServiceConfigurations",
        &req.request_id,
        &ec2_list("unsuccessful", &[]),
    ))
}

pub(crate) fn describe_vpc_endpoint_service_configurations(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let wanted = indexed_list(&req.query_params, "ServiceId");
    let accounts = svc.state.read();
    let empty = Ec2State::new(&req.account_id, &req.region);
    let state = accounts.get(&req.account_id).unwrap_or(&empty);
    let mut items: Vec<String> = state
        .endpoint_services
        .values()
        .filter(|s| wanted.is_empty() || wanted.contains(&s.service_id))
        .map(|s| service_config_xml(s, state.tags_for(&s.service_id)))
        .collect();
    items.sort();
    Ok(Ec2Service::respond(
        "DescribeVpcEndpointServiceConfigurations",
        &req.request_id,
        &ec2_list("serviceConfigurationSet", &items),
    ))
}

pub(crate) fn modify_vpc_endpoint_service_configuration(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "ServiceId")?;
    Ok(Ec2Service::respond(
        "ModifyVpcEndpointServiceConfiguration",
        &req.request_id,
        &ec2_return(true),
    ))
}

pub(crate) fn describe_vpc_endpoint_service_permissions(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "ServiceId")?;
    Ok(Ec2Service::respond(
        "DescribeVpcEndpointServicePermissions",
        &req.request_id,
        &ec2_list("allowedPrincipals", &[]),
    ))
}

pub(crate) fn modify_vpc_endpoint_service_permissions(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "ServiceId")?;
    let body = format!("{}{}", ec2_list("addedPrincipalSet", &[]), ec2_return(true));
    Ok(Ec2Service::respond(
        "ModifyVpcEndpointServicePermissions",
        &req.request_id,
        &body,
    ))
}

pub(crate) fn modify_vpc_endpoint_service_payer_responsibility(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "ServiceId")?;
    require(&req.query_params, "PayerResponsibility")?;
    validate_enum(&req.query_params, "PayerResponsibility", &["ServiceOwner"])?;
    Ok(Ec2Service::respond(
        "ModifyVpcEndpointServicePayerResponsibility",
        &req.request_id,
        &ec2_return(true),
    ))
}

pub(crate) fn start_vpc_endpoint_service_private_dns_verification(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "ServiceId")?;
    Ok(Ec2Service::respond(
        "StartVpcEndpointServicePrivateDnsVerification",
        &req.request_id,
        &ec2_return(true),
    ))
}

// ---- connection notifications ----

fn notification_xml(n: &ConnectionNotification) -> String {
    format!(
        "{}{}{}{}<connectionNotificationState>Enabled</connectionNotificationState>{}",
        ec2_elem("connectionNotificationId", &n.id),
        ec2_elem("connectionNotificationArn", &n.arn),
        ec2_elem("connectionNotificationType", "Topic"),
        n.service_id
            .as_ref()
            .map(|s| ec2_elem("serviceId", s))
            .unwrap_or_default(),
        ec2_list("connectionEvents", &n.events),
    )
}

pub(crate) fn create_vpc_endpoint_connection_notification(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let arn = require(&req.query_params, "ConnectionNotificationArn")?;
    let id = gen_id("vpce-nfn");
    let n = ConnectionNotification {
        id: id.clone(),
        arn,
        service_id: req.query_params.get("ServiceId").cloned(),
        events: indexed_list(&req.query_params, "ConnectionEvents"),
    };
    {
        let mut accounts = svc.state.write();
        accounts
            .get_or_create(&req.account_id)
            .connection_notifications
            .insert(id.clone(), n.clone());
    }
    let body = format!(
        "<connectionNotification>{}</connectionNotification>{}",
        notification_xml(&n),
        ec2_elem("clientToken", &gen_id("token"))
    );
    Ok(Ec2Service::respond(
        "CreateVpcEndpointConnectionNotification",
        &req.request_id,
        &body,
    ))
}

pub(crate) fn delete_vpc_endpoint_connection_notifications(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let ids = indexed_list(&req.query_params, "ConnectionNotificationId");
    {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        for id in &ids {
            state.connection_notifications.remove(id);
        }
    }
    Ok(Ec2Service::respond(
        "DeleteVpcEndpointConnectionNotifications",
        &req.request_id,
        &ec2_list("unsuccessful", &[]),
    ))
}

pub(crate) fn describe_vpc_endpoint_connection_notifications(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let accounts = svc.state.read();
    let empty = Ec2State::new(&req.account_id, &req.region);
    let state = accounts.get(&req.account_id).unwrap_or(&empty);
    let mut items: Vec<String> = state
        .connection_notifications
        .values()
        .map(notification_xml)
        .collect();
    items.sort();
    Ok(Ec2Service::respond(
        "DescribeVpcEndpointConnectionNotifications",
        &req.request_id,
        &ec2_list("connectionNotificationSet", &items),
    ))
}

pub(crate) fn modify_vpc_endpoint_connection_notification(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "ConnectionNotificationId")?;
    Ok(Ec2Service::respond(
        "ModifyVpcEndpointConnectionNotification",
        &req.request_id,
        &ec2_return(true),
    ))
}

pub(crate) fn describe_vpc_endpoint_associations(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    validate_max_results(&req.query_params, 1, 100)?;
    Ok(Ec2Service::respond(
        "DescribeVpcEndpointAssociations",
        &req.request_id,
        &ec2_list("vpcEndpointAssociationSet", &[]),
    ))
}

// ---- flow logs ----

fn flow_log_xml(f: &FlowLog, tags: &[Tag]) -> String {
    format!(
        "{}{}{}{}<flowLogStatus>ACTIVE</flowLogStatus><deliverLogsStatus>SUCCESS</deliverLogsStatus>{}{}{}{}{}{}",
        ec2_elem("flowLogId", &f.id),
        ec2_elem("resourceId", &f.resource_id),
        ec2_elem("trafficType", &f.traffic_type),
        ec2_elem("logDestinationType", &f.log_destination_type),
        f.log_group_name.as_ref().map(|n| ec2_elem("logGroupName", n)).unwrap_or_default(),
        f.log_destination.as_ref().map(|d| ec2_elem("logDestination", d)).unwrap_or_default(),
        f.deliver_logs_permission_arn.as_ref().map(|a| ec2_elem("deliverLogsPermissionArn", a)).unwrap_or_default(),
        ec2_elem("maxAggregationInterval", &f.max_aggregation_interval.to_string()),
        ec2_elem("creationTime", FIXED_TIME),
        super::tags::tag_set_xml(tags),
    )
}

pub(crate) fn create_flow_logs(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "ResourceType")?;
    validate_enum(
        &req.query_params,
        "ResourceType",
        &[
            "VPC",
            "Subnet",
            "NetworkInterface",
            "TransitGateway",
            "TransitGatewayAttachment",
            "RegionalNatGateway",
        ],
    )?;
    validate_enum(
        &req.query_params,
        "TrafficType",
        &["ACCEPT", "REJECT", "ALL"],
    )?;
    validate_enum(
        &req.query_params,
        "LogDestinationType",
        &["cloud-watch-logs", "s3", "kinesis-data-firehose"],
    )?;
    let resources = indexed_list(&req.query_params, "ResourceId");
    let traffic = req
        .query_params
        .get("TrafficType")
        .cloned()
        .unwrap_or_else(|| "ALL".to_string());
    let dest = req
        .query_params
        .get("LogDestinationType")
        .cloned()
        .unwrap_or_else(|| "cloud-watch-logs".to_string());
    let log_destination = req.query_params.get("LogDestination").cloned();
    let deliver_logs_permission_arn = req
        .query_params
        .get("DeliverLogsPermissionArn")
        .filter(|v| !v.is_empty())
        .cloned();
    // AWS accepts 60 or 600 and defaults to 600 when the field is omitted.
    let max_aggregation_interval = req
        .query_params
        .get("MaxAggregationInterval")
        .and_then(|v| v.parse::<i64>().ok())
        .unwrap_or(600);
    let mut ids = Vec::new();
    {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        for r in &resources {
            let id = gen_id("fl");
            // `vpc-flow-log` TagSpecifications apply to each created flow log.
            crate::service::tags::apply_tag_specifications(
                state,
                &req.query_params,
                &id,
                "vpc-flow-log",
            );
            state.flow_logs.insert(
                id.clone(),
                FlowLog {
                    id: id.clone(),
                    resource_id: r.clone(),
                    traffic_type: traffic.clone(),
                    log_destination_type: dest.clone(),
                    log_group_name: req.query_params.get("LogGroupName").cloned(),
                    log_destination: log_destination.clone(),
                    deliver_logs_permission_arn: deliver_logs_permission_arn.clone(),
                    max_aggregation_interval,
                },
            );
            ids.push(id);
        }
    }
    let body = format!(
        "{}{}{}",
        ec2_elem("clientToken", &gen_id("token")),
        ec2_list("flowLogIdSet", &ids),
        ec2_list("unsuccessful", &[])
    );
    Ok(Ec2Service::respond(
        "CreateFlowLogs",
        &req.request_id,
        &body,
    ))
}

pub(crate) fn delete_flow_logs(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let ids = indexed_list(&req.query_params, "FlowLogId");
    {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        for id in &ids {
            state.flow_logs.remove(id);
            state.tags.remove(id);
        }
    }
    Ok(Ec2Service::respond(
        "DeleteFlowLogs",
        &req.request_id,
        &ec2_list("unsuccessful", &[]),
    ))
}

pub(crate) fn describe_flow_logs(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let wanted = indexed_list(&req.query_params, "FlowLogId");
    let accounts = svc.state.read();
    let empty = Ec2State::new(&req.account_id, &req.region);
    let state = accounts.get(&req.account_id).unwrap_or(&empty);
    let mut items: Vec<String> = state
        .flow_logs
        .values()
        .filter(|f| wanted.is_empty() || wanted.contains(&f.id))
        .map(|f| flow_log_xml(f, state.tags_for(&f.id)))
        .collect();
    items.sort();
    Ok(Ec2Service::respond(
        "DescribeFlowLogs",
        &req.request_id,
        &ec2_list("flowLogSet", &items),
    ))
}

pub(crate) fn get_flow_logs_integration_template(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "FlowLogId")?;
    require(&req.query_params, "ConfigDeliveryS3DestinationArn")?;
    Ok(Ec2Service::respond(
        "GetFlowLogsIntegrationTemplate",
        &req.request_id,
        &ec2_elem("result", "https://s3.amazonaws.com/flow-logs-template.json"),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn req(action: &str, query: &[(&str, &str)]) -> AwsRequest {
        AwsRequest {
            service: "ec2".into(),
            action: action.into(),
            region: "us-east-1".into(),
            account_id: "000000000000".into(),
            request_id: "rid".into(),
            headers: http::HeaderMap::new(),
            query_params: query
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
            body: bytes::Bytes::new(),
            body_stream: parking_lot::Mutex::new(None),
            path_segments: Vec::new(),
            raw_path: "/".into(),
            raw_query: String::new(),
            method: http::Method::POST,
            is_query_protocol: true,
            access_key_id: None,
            principal: None,
        }
    }

    fn seed_endpoint(svc: &Ec2Service) {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create("000000000000");
        state.vpc_endpoints.insert(
            "vpce-1".to_string(),
            VpcEndpoint {
                id: "vpce-1".into(),
                endpoint_type: "Interface".into(),
                vpc_id: "vpc-1".into(),
                service_name: "com.amazonaws.us-east-1.s3".into(),
                state: "available".into(),
                subnet_ids: vec!["subnet-a".into()],
                route_table_ids: vec![],
                private_dns_enabled: false,
                security_group_ids: vec![],
                payer_responsibility: "vpc-endpoint-account".into(),
            },
        );
    }

    #[test]
    fn modify_vpc_endpoint_persists_subnets_and_dns() {
        // The handler validated then returned true without persisting
        // (bug-hunt 2026-07-01, Tier-0 EC2 Modify*).
        let svc = Ec2Service::new();
        seed_endpoint(&svc);
        modify_vpc_endpoint(
            &svc,
            &req(
                "ModifyVpcEndpoint",
                &[
                    ("VpcEndpointId", "vpce-1"),
                    ("AddSubnetId.1", "subnet-b"),
                    ("RemoveSubnetId.1", "subnet-a"),
                    ("AddRouteTableId.1", "rtb-1"),
                    ("PrivateDnsEnabled", "true"),
                ],
            ),
        )
        .unwrap();

        let accounts = svc.state.read();
        let e = &accounts.get("000000000000").unwrap().vpc_endpoints["vpce-1"];
        assert_eq!(e.subnet_ids, vec!["subnet-b".to_string()]);
        assert_eq!(e.route_table_ids, vec!["rtb-1".to_string()]);
        assert!(e.private_dns_enabled);
    }

    #[test]
    fn modify_vpc_endpoint_persists_security_groups() {
        // Add/RemoveSecurityGroupId were silently dropped (Cubic P2 on #2057).
        let svc = Ec2Service::new();
        seed_endpoint(&svc);
        svc.state
            .write()
            .get_or_create("000000000000")
            .vpc_endpoints
            .get_mut("vpce-1")
            .unwrap()
            .security_group_ids = vec!["sg-old".into()];
        modify_vpc_endpoint(
            &svc,
            &req(
                "ModifyVpcEndpoint",
                &[
                    ("VpcEndpointId", "vpce-1"),
                    ("AddSecurityGroupId.1", "sg-new"),
                    ("RemoveSecurityGroupId.1", "sg-old"),
                ],
            ),
        )
        .unwrap();
        {
            let accounts = svc.state.read();
            let e = &accounts.get("000000000000").unwrap().vpc_endpoints["vpce-1"];
            assert_eq!(e.security_group_ids, vec!["sg-new".to_string()]);
        }
        // And the groupSet round-trips in DescribeVpcEndpoints.
        let resp = describe_vpc_endpoints(&svc, &req("DescribeVpcEndpoints", &[])).unwrap();
        let body = String::from_utf8_lossy(resp.body.expect_bytes()).to_string();
        assert!(body.contains("<groupId>sg-new</groupId>"), "got: {body}");
    }

    #[test]
    fn modify_vpc_endpoint_unknown_id_errors() {
        let svc = Ec2Service::new();
        let err = match modify_vpc_endpoint(
            &svc,
            &req("ModifyVpcEndpoint", &[("VpcEndpointId", "vpce-missing")]),
        ) {
            Err(e) => e,
            Ok(_) => panic!("expected NotFound error"),
        };
        assert!(format!("{err:?}").contains("InvalidVpcEndpointId.NotFound"));
    }
}
