//! VPC endpoints, endpoint service configurations, connection notifications,
//! and VPC flow logs.

use fakecloud_aws::ec2query::{ec2_elem, ec2_list, ec2_return, ec2_scalar_list};
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use crate::service::Ec2Service;
use crate::service_helpers::{gen_id, indexed_list, require, validate_enum, validate_max_results};
use crate::state::{ConnectionNotification, Ec2State, EndpointService, FlowLog, Tag, VpcEndpoint};

const FIXED_TIME: &str = "2024-01-01T00:00:00.000Z";

// ---- VPC endpoints ----

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
        ec2_list("groupSet", &[]),
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
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "VpcEndpointId")?;
    validate_enum(
        &req.query_params,
        "IpAddressType",
        &["ipv4", "dualstack", "ipv6"],
    )?;
    Ok(Ec2Service::respond(
        "ModifyVpcEndpoint",
        &req.request_id,
        &ec2_return(true),
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
