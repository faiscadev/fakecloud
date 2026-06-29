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
        "{}{}{}{}{}{}{}{}{}{}{}{}{}{}{}{}",
        ec2_elem("networkInterfaceId", &n.network_interface_id),
        ec2_elem("subnetId", &n.subnet_id),
        ec2_elem("vpcId", &n.vpc_id),
        ec2_elem("availabilityZone", &n.availability_zone),
        ec2_elem("description", &n.description),
        ec2_elem("ownerId", owner),
        ec2_elem("macAddress", &n.mac_address),
        ec2_elem("privateIpAddress", &n.private_ip_address),
        // AWS derives the private DNS name from the primary private IP; the
        // `aws_network_interface` resource reads it back.
        ec2_elem(
            "privateDnsName",
            &format!("ip-{}.ec2.internal", n.private_ip_address.replace('.', "-"))
        ),
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
    // Resolve the subnet's VPC, and default the security group to that VPC's
    // `default` group when the caller specified none (AWS does this, and the
    // `aws_network_interface` resource asserts `security_groups.# == 1`).
    let (resolved_vpc_id, default_groups) = {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let vpc_id = state
            .subnets
            .get(&subnet_id)
            .map(|s| s.vpc_id.clone())
            .unwrap_or_default();
        let mut groups = indexed_list(&req.query_params, "SecurityGroupId");
        if groups.is_empty() {
            if let Some(default_sg) = state
                .security_groups
                .values()
                .find(|g| g.vpc_id == vpc_id && g.group_name == "default")
            {
                groups.push(default_sg.group_id.clone());
            }
        }
        (vpc_id, groups)
    };
    let eni = NetworkInterface {
        network_interface_id: id.clone(),
        subnet_id,
        vpc_id: resolved_vpc_id,
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
        group_ids: default_groups,
        private_ips: Vec::new(),
        ipv6_addresses: Vec::new(),
        attachment: None,
        public_ip_dns_hostname_type: None,
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
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let eni_id = require(&req.query_params, "NetworkInterfaceId")?;
    // The old handler validated the id and returned true, persisting nothing --
    // so SourceDestCheck (NAT-instance source/dest disable), Description, and
    // Groups changes silently no-op'd (bug-audit 2026-06-20, 1.22). Apply each
    // attribute present in the request.
    {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        if let Some(eni) = state.network_interfaces.get_mut(&eni_id) {
            if let Some(v) = req.query_params.get("SourceDestCheck.Value") {
                eni.source_dest_check = v == "true";
            }
            if let Some(d) = req.query_params.get("Description.Value") {
                eni.description = d.clone();
            }
            // Groups: SecurityGroupId.1, SecurityGroupId.2, ...
            let mut groups = Vec::new();
            for i in 1.. {
                match req.query_params.get(&format!("SecurityGroupId.{i}")) {
                    Some(g) => groups.push(g.clone()),
                    None => break,
                }
            }
            if !groups.is_empty() {
                eni.group_ids = groups;
            }
        }
    }
    Ok(Ec2Service::respond(
        "ModifyNetworkInterfaceAttribute",
        &req.request_id,
        &ec2_return(true),
    ))
}

pub(crate) fn reset_network_interface_attribute(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let eni_id = require(&req.query_params, "NetworkInterfaceId")?;
    // The only resettable attribute is sourceDestCheck (back to the default
    // true). Previously this validated then persisted nothing.
    {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        if let Some(eni) = state.network_interfaces.get_mut(&eni_id) {
            eni.source_dest_check = true;
        }
    }
    Ok(Ec2Service::respond(
        "ResetNetworkInterfaceAttribute",
        &req.request_id,
        &ec2_return(true),
    ))
}

pub(crate) fn describe_network_interface_attribute(
    svc: &Ec2Service,
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
    // Read the real stored ENI so a Modify is reflected on read-back. Previously
    // every attribute returned a hardcoded constant (description="eni", empty
    // groupSet, sourceDestCheck=true) regardless of stored state.
    let accounts = svc.state.read();
    let eni = accounts
        .get(&req.account_id)
        .and_then(|a| a.network_interfaces.get(&id));
    let attr_xml = match attribute.as_str() {
        "description" => format!(
            "<description><value>{}</value></description>",
            eni.map(|e| e.description.as_str()).unwrap_or("")
        ),
        "groupSet" => {
            let groups: Vec<String> = eni
                .map(|e| {
                    e.group_ids
                        .iter()
                        .map(|g| format!("{}{}", ec2_elem("groupId", g), ec2_elem("groupName", g)))
                        .collect()
                })
                .unwrap_or_default();
            ec2_list("groupSet", &groups)
        }
        "attachment" => eni
            .and_then(|e| e.attachment.as_ref())
            .map(|a| format!("<attachment>{}</attachment>", attachment_inner(a)))
            .unwrap_or_default(),
        "associatePublicIpAddress" => {
            "<associatePublicIpAddress>false</associatePublicIpAddress>".to_string()
        }
        _ => format!(
            "<sourceDestCheck><value>{}</value></sourceDestCheck>",
            eni.map(|e| e.source_dest_check).unwrap_or(true)
        ),
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

#[cfg(test)]
mod tests {
    use super::*;

    fn req(query: &[(&str, &str)]) -> AwsRequest {
        AwsRequest {
            service: "ec2".into(),
            action: "ModifyNetworkInterfaceAttribute".into(),
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

    #[test]
    fn modify_network_interface_attribute_persists_fields() {
        // The handler was a validate-then-return-true stub; SourceDestCheck,
        // Description, and Groups must actually persist (bug-audit 2026-06-20,
        // 1.22).
        let svc = Ec2Service::new();
        {
            let mut accounts = svc.state.write();
            let state = accounts.get_or_create("000000000000");
            state.network_interfaces.insert(
                "eni-1".to_string(),
                NetworkInterface {
                    network_interface_id: "eni-1".into(),
                    subnet_id: "subnet-1".into(),
                    vpc_id: "vpc-1".into(),
                    availability_zone: "us-east-1a".into(),
                    description: "old".into(),
                    mac_address: "02:00:00:00:00:01".into(),
                    private_ip_address: "10.0.0.5".into(),
                    status: "available".into(),
                    interface_type: "interface".into(),
                    source_dest_check: true,
                    group_ids: vec!["sg-old".into()],
                    private_ips: vec![],
                    ipv6_addresses: vec![],
                    attachment: None,
                    public_ip_dns_hostname_type: None,
                },
            );
        }

        modify_network_interface_attribute(
            &svc,
            &req(&[
                ("NetworkInterfaceId", "eni-1"),
                ("SourceDestCheck.Value", "false"),
                ("Description.Value", "new-desc"),
                ("SecurityGroupId.1", "sg-a"),
                ("SecurityGroupId.2", "sg-b"),
            ]),
        )
        .unwrap();

        let accounts = svc.state.read();
        let eni = &accounts.get("000000000000").unwrap().network_interfaces["eni-1"];
        assert!(!eni.source_dest_check, "SourceDestCheck must persist");
        assert_eq!(eni.description, "new-desc");
        assert_eq!(eni.group_ids, vec!["sg-a".to_string(), "sg-b".to_string()]);
    }

    fn seed_eni(svc: &Ec2Service) {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create("000000000000");
        state.network_interfaces.insert(
            "eni-1".to_string(),
            NetworkInterface {
                network_interface_id: "eni-1".into(),
                subnet_id: "subnet-1".into(),
                vpc_id: "vpc-1".into(),
                availability_zone: "us-east-1a".into(),
                description: "old".into(),
                mac_address: "02:00:00:00:00:01".into(),
                private_ip_address: "10.0.0.5".into(),
                status: "available".into(),
                interface_type: "interface".into(),
                source_dest_check: true,
                group_ids: vec!["sg-old".into()],
                private_ips: vec![],
                ipv6_addresses: vec![],
                attachment: None,
                public_ip_dns_hostname_type: None,
            },
        );
    }

    fn describe_attr(svc: &Ec2Service, attr: &str) -> String {
        let resp = describe_network_interface_attribute(
            svc,
            &req(&[("NetworkInterfaceId", "eni-1"), ("Attribute", attr)]),
        )
        .unwrap();
        String::from_utf8_lossy(resp.body.expect_bytes()).to_string()
    }

    #[test]
    fn describe_network_interface_attribute_reads_state() {
        let svc = Ec2Service::new();
        seed_eni(&svc);
        modify_network_interface_attribute(
            &svc,
            &req(&[
                ("NetworkInterfaceId", "eni-1"),
                ("SourceDestCheck.Value", "false"),
                ("Description.Value", "new-desc"),
                ("SecurityGroupId.1", "sg-a"),
            ]),
        )
        .unwrap();

        assert!(describe_attr(&svc, "sourceDestCheck")
            .contains("<sourceDestCheck><value>false</value></sourceDestCheck>"));
        assert!(describe_attr(&svc, "description")
            .contains("<description><value>new-desc</value></description>"));
        assert!(describe_attr(&svc, "groupSet").contains("<groupId>sg-a</groupId>"));
    }

    #[test]
    fn reset_network_interface_attribute_resets_source_dest_check() {
        let svc = Ec2Service::new();
        seed_eni(&svc);
        modify_network_interface_attribute(
            &svc,
            &req(&[
                ("NetworkInterfaceId", "eni-1"),
                ("SourceDestCheck.Value", "false"),
            ]),
        )
        .unwrap();
        reset_network_interface_attribute(&svc, &req(&[("NetworkInterfaceId", "eni-1")])).unwrap();
        let accounts = svc.state.read();
        assert!(
            accounts.get("000000000000").unwrap().network_interfaces["eni-1"].source_dest_check
        );
    }
}
