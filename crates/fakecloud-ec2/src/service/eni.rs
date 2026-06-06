//! Elastic network interfaces, their permissions, and IP assignment.

use fakecloud_aws::ec2query::{ec2_elem, ec2_list, ec2_return, ec2_scalar_list};
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use crate::service::Ec2Service;
use crate::service_helpers::{
    gen_id, indexed_list, parse_filters, require, validate_enum, validate_max_results, Filter,
};
use crate::state::{Ec2State, EniAttachment, NetworkInterface, NetworkInterfacePermission, Tag};

fn eni_xml(n: &NetworkInterface, tags: &[Tag], owner: &str) -> String {
    let groups: Vec<String> = n
        .group_ids
        .iter()
        .map(|g| format!("{}{}", ec2_elem("groupId", g), ec2_elem("groupName", g)))
        .collect();
    let priv_ips: Vec<String> = std::iter::once(&n.private_ip_address)
        .chain(n.private_ips.iter())
        .enumerate()
        .map(|(i, ip)| {
            format!(
                "{}<primary>{}</primary>",
                ec2_elem("privateIpAddress", ip),
                i == 0
            )
        })
        .collect();
    let ipv6: Vec<String> = n
        .ipv6_addresses
        .iter()
        .map(|a| ec2_elem("ipv6Address", a))
        .collect();
    let attachment = n
        .attachment
        .as_ref()
        .map(|a| format!("<attachment>{}</attachment>", attachment_inner(a)))
        .unwrap_or_default();
    format!(
        "{}{}{}{}{}{}{}{}{}{}{}{}{}{}{}",
        ec2_elem("networkInterfaceId", &n.network_interface_id),
        ec2_elem("subnetId", &n.subnet_id),
        ec2_elem("vpcId", &n.vpc_id),
        ec2_elem("availabilityZone", &n.availability_zone),
        ec2_elem("description", &n.description),
        ec2_elem("ownerId", owner),
        ec2_elem("macAddress", &n.mac_address),
        ec2_elem("privateIpAddress", &n.private_ip_address),
        format_args!("<sourceDestCheck>{}</sourceDestCheck>", n.source_dest_check),
        ec2_elem("status", &n.status),
        ec2_elem("interfaceType", &n.interface_type),
        ec2_list("groupSet", &groups),
        ec2_list("privateIpAddressesSet", &priv_ips),
        format_args!("{}{}", ec2_list("ipv6AddressesSet", &ipv6), attachment),
        super::tags::tag_set_xml(tags),
    )
}

fn attachment_inner(a: &EniAttachment) -> String {
    format!(
        "{}{}<deviceIndex>{}</deviceIndex>{}<deleteOnTermination>false</deleteOnTermination>",
        ec2_elem("attachmentId", &a.attachment_id),
        ec2_elem("instanceId", &a.instance_id),
        a.device_index,
        ec2_elem("status", &a.status),
    )
}

pub(crate) fn create_network_interface(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let subnet_id = require(&req.query_params, "SubnetId")?;
    validate_enum(
        &req.query_params,
        "InterfaceType",
        &["efa", "efa-only", "branch", "trunk"],
    )?;
    let id = gen_id("eni");
    let eni = NetworkInterface {
        network_interface_id: id.clone(),
        subnet_id,
        vpc_id: String::new(),
        availability_zone: format!(
            "{}a",
            if req.region.is_empty() {
                "us-east-1"
            } else {
                &req.region
            }
        ),
        description: req
            .query_params
            .get("Description")
            .cloned()
            .unwrap_or_default(),
        mac_address: "0a:1b:2c:3d:4e:5f".to_string(),
        private_ip_address: req
            .query_params
            .get("PrivateIpAddress")
            .cloned()
            .unwrap_or_else(|| "10.0.0.20".to_string()),
        status: "available".to_string(),
        interface_type: req
            .query_params
            .get("InterfaceType")
            .cloned()
            .unwrap_or_else(|| "interface".to_string()),
        source_dest_check: true,
        group_ids: indexed_list(&req.query_params, "SecurityGroupId"),
        private_ips: Vec::new(),
        ipv6_addresses: Vec::new(),
        attachment: None,
    };
    let owner = req.account_id.clone();
    let tags = {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        crate::service::tags::apply_tag_specifications(
            state,
            &req.query_params,
            &id,
            "network-interface",
        );
        let t = state.tags_for(&id).to_vec();
        state.network_interfaces.insert(id.clone(), eni.clone());
        t
    };
    let body = format!(
        "<networkInterface>{}</networkInterface>{}",
        eni_xml(&eni, &tags, &owner),
        ec2_elem("clientToken", &gen_id("token")),
    );
    Ok(Ec2Service::respond(
        "CreateNetworkInterface",
        &req.request_id,
        &body,
    ))
}

pub(crate) fn delete_network_interface(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "NetworkInterfaceId")?;
    {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        state.network_interfaces.remove(&id);
        state.tags.remove(&id);
    }
    Ok(Ec2Service::respond(
        "DeleteNetworkInterface",
        &req.request_id,
        &ec2_return(true),
    ))
}

pub(crate) fn describe_network_interfaces(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    validate_max_results(&req.query_params, 5, 1000)?;
    let filters = parse_filters(&req.query_params);
    let wanted = indexed_list(&req.query_params, "NetworkInterfaceId");
    let owner = req.account_id.clone();
    let accounts = svc.state.read();
    let empty = Ec2State::new(&req.account_id, &req.region);
    let state = accounts.get(&req.account_id).unwrap_or(&empty);
    let mut items: Vec<String> = state
        .network_interfaces
        .values()
        .filter(|n| wanted.is_empty() || wanted.contains(&n.network_interface_id))
        .filter(|n| eni_match(n, state.tags_for(&n.network_interface_id), &filters))
        .map(|n| eni_xml(n, state.tags_for(&n.network_interface_id), &owner))
        .collect();
    items.sort();
    Ok(Ec2Service::respond(
        "DescribeNetworkInterfaces",
        &req.request_id,
        &ec2_list("networkInterfaceSet", &items),
    ))
}

fn eni_match(n: &NetworkInterface, tags: &[Tag], filters: &[Filter]) -> bool {
    filters.iter().all(|f| {
        let candidates: Vec<String> = match f.name.as_str() {
            "network-interface-id" => vec![n.network_interface_id.clone()],
            "subnet-id" => vec![n.subnet_id.clone()],
            "status" => vec![n.status.clone()],
            "tag-key" => tags.iter().map(|t| t.key.clone()).collect(),
            name => {
                if let Some(key) = name.strip_prefix("tag:") {
                    tags.iter()
                        .filter(|t| t.key == key)
                        .map(|t| t.value.clone())
                        .collect()
                } else {
                    return true;
                }
            }
        };
        f.values.iter().any(|v| candidates.iter().any(|c| c == v))
    })
}

pub(crate) fn attach_network_interface(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let eni_id = require(&req.query_params, "NetworkInterfaceId")?;
    let instance_id = require(&req.query_params, "InstanceId")?;
    let device_index: i64 = require(&req.query_params, "DeviceIndex")?
        .parse()
        .unwrap_or(0);
    let attachment_id = gen_id("eni-attach");
    {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        if let Some(eni) = state.network_interfaces.get_mut(&eni_id) {
            eni.status = "in-use".to_string();
            eni.attachment = Some(EniAttachment {
                attachment_id: attachment_id.clone(),
                instance_id,
                device_index,
                status: "attached".to_string(),
            });
        }
    }
    let body = format!(
        "{}<networkCardIndex>0</networkCardIndex>",
        ec2_elem("attachmentId", &attachment_id)
    );
    Ok(Ec2Service::respond(
        "AttachNetworkInterface",
        &req.request_id,
        &body,
    ))
}

pub(crate) fn detach_network_interface(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let attachment_id = require(&req.query_params, "AttachmentId")?;
    {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        for eni in state.network_interfaces.values_mut() {
            if eni.attachment.as_ref().map(|a| a.attachment_id.as_str())
                == Some(attachment_id.as_str())
            {
                eni.attachment = None;
                eni.status = "available".to_string();
            }
        }
    }
    Ok(Ec2Service::respond(
        "DetachNetworkInterface",
        &req.request_id,
        &ec2_return(true),
    ))
}

pub(crate) fn modify_network_interface_attribute(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "NetworkInterfaceId")?;
    Ok(Ec2Service::respond(
        "ModifyNetworkInterfaceAttribute",
        &req.request_id,
        &ec2_return(true),
    ))
}

pub(crate) fn reset_network_interface_attribute(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "NetworkInterfaceId")?;
    Ok(Ec2Service::respond(
        "ResetNetworkInterfaceAttribute",
        &req.request_id,
        &ec2_return(true),
    ))
}

pub(crate) fn describe_network_interface_attribute(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "NetworkInterfaceId")?;
    let attribute = req
        .query_params
        .get("Attribute")
        .cloned()
        .unwrap_or_default();
    validate_enum(
        &req.query_params,
        "Attribute",
        &[
            "description",
            "groupSet",
            "sourceDestCheck",
            "attachment",
            "associatePublicIpAddress",
        ],
    )?;
    let attr_xml = match attribute.as_str() {
        "description" => "<description><value>eni</value></description>".to_string(),
        "groupSet" => ec2_list("groupSet", &[]),
        "attachment" => String::new(),
        "associatePublicIpAddress" => {
            "<associatePublicIpAddress>false</associatePublicIpAddress>".to_string()
        }
        _ => "<sourceDestCheck><value>true</value></sourceDestCheck>".to_string(),
    };
    let body = format!("{}{}", ec2_elem("networkInterfaceId", &id), attr_xml);
    Ok(Ec2Service::respond(
        "DescribeNetworkInterfaceAttribute",
        &req.request_id,
        &body,
    ))
}

// ---- permissions ----

pub(crate) fn create_network_interface_permission(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let eni_id = require(&req.query_params, "NetworkInterfaceId")?;
    let permission = require(&req.query_params, "Permission")?;
    validate_enum(
        &req.query_params,
        "Permission",
        &["INSTANCE-ATTACH", "EIP-ASSOCIATE"],
    )?;
    let perm = NetworkInterfacePermission {
        permission_id: gen_id("eni-perm"),
        network_interface_id: eni_id,
        aws_account_id: req
            .query_params
            .get("AwsAccountId")
            .cloned()
            .unwrap_or_else(|| req.account_id.clone()),
        permission,
    };
    let pid = perm.permission_id.clone();
    {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        state.eni_permissions.insert(pid, perm.clone());
    }
    let body = format!(
        "<interfacePermission>{}</interfacePermission>",
        perm_xml(&perm)
    );
    Ok(Ec2Service::respond(
        "CreateNetworkInterfacePermission",
        &req.request_id,
        &body,
    ))
}

fn perm_xml(p: &NetworkInterfacePermission) -> String {
    format!(
        "{}{}{}{}<permissionState><state>granted</state></permissionState>",
        ec2_elem("networkInterfacePermissionId", &p.permission_id),
        ec2_elem("networkInterfaceId", &p.network_interface_id),
        ec2_elem("awsAccountId", &p.aws_account_id),
        ec2_elem("permission", &p.permission),
    )
}

pub(crate) fn delete_network_interface_permission(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "NetworkInterfacePermissionId")?;
    {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        state.eni_permissions.remove(&id);
    }
    Ok(Ec2Service::respond(
        "DeleteNetworkInterfacePermission",
        &req.request_id,
        &ec2_return(true),
    ))
}

pub(crate) fn describe_network_interface_permissions(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    validate_max_results(&req.query_params, 5, 255)?;
    let wanted = indexed_list(&req.query_params, "NetworkInterfacePermissionId");
    let accounts = svc.state.read();
    let empty = Ec2State::new(&req.account_id, &req.region);
    let state = accounts.get(&req.account_id).unwrap_or(&empty);
    let mut items: Vec<String> = state
        .eni_permissions
        .values()
        .filter(|p| wanted.is_empty() || wanted.contains(&p.permission_id))
        .map(perm_xml)
        .collect();
    items.sort();
    Ok(Ec2Service::respond(
        "DescribeNetworkInterfacePermissions",
        &req.request_id,
        &ec2_list("networkInterfacePermissions", &items),
    ))
}

// ---- IP assignment ----

pub(crate) fn assign_private_ip_addresses(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "NetworkInterfaceId")?;
    let assigned: Vec<String> = indexed_list(&req.query_params, "PrivateIpAddress")
        .into_iter()
        .map(|ip| {
            format!(
                "{}<isPrimary>false</isPrimary>",
                ec2_elem("privateIpAddress", &ip)
            )
        })
        .collect();
    let assigned = if assigned.is_empty() {
        vec![
            "<privateIpAddress>10.0.0.30</privateIpAddress><isPrimary>false</isPrimary>"
                .to_string(),
        ]
    } else {
        assigned
    };
    let body = format!(
        "{}{}",
        ec2_elem("networkInterfaceId", &id),
        ec2_list("assignedPrivateIpAddressesSet", &assigned),
    );
    Ok(Ec2Service::respond(
        "AssignPrivateIpAddresses",
        &req.request_id,
        &body,
    ))
}

pub(crate) fn unassign_private_ip_addresses(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "NetworkInterfaceId")?;
    Ok(Ec2Service::respond(
        "UnassignPrivateIpAddresses",
        &req.request_id,
        &ec2_return(true),
    ))
}

pub(crate) fn assign_ipv6_addresses(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "NetworkInterfaceId")?;
    let addrs: Vec<String> = indexed_list(&req.query_params, "Ipv6Addresses");
    let addrs = if addrs.is_empty() {
        vec!["2600:1f00::5".to_string()]
    } else {
        addrs
    };
    let body = format!(
        "{}{}",
        ec2_elem("networkInterfaceId", &id),
        ec2_scalar_list("assignedIpv6Addresses", &addrs),
    );
    Ok(Ec2Service::respond(
        "AssignIpv6Addresses",
        &req.request_id,
        &body,
    ))
}

pub(crate) fn unassign_ipv6_addresses(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "NetworkInterfaceId")?;
    let addrs: Vec<String> = indexed_list(&req.query_params, "Ipv6Addresses");
    let body = format!(
        "{}{}",
        ec2_elem("networkInterfaceId", &id),
        ec2_scalar_list("unassignedIpv6Addresses", &addrs),
    );
    Ok(Ec2Service::respond(
        "UnassignIpv6Addresses",
        &req.request_id,
        &body,
    ))
}
