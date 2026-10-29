//! Standalone VPC networking resources: `AWS::EC2::SecurityGroupIngress`,
//! `SecurityGroupEgress`, `VPCGatewayAttachment`, `Route`, `EIP` and
//! `NatGateway`. Each creates and deletes through the real EC2 handlers (the
//! same ones the API dispatches to), so a stack's ingress rule, IGW
//! attachment, default route, Elastic IP and NAT gateway exist in EC2 instead
//! of being recorded with no backing state.

use std::collections::HashMap;

use serde_json::Value;

use super::{ProvisionResult, ResourceDefinition, ResourceProvisioner, StackResource};

fn prop_str<'a>(props: &'a Value, key: &str) -> Option<&'a str> {
    props.get(key).and_then(|v| v.as_str())
}

fn num_str(v: &Value) -> Option<String> {
    match v {
        Value::Number(n) => Some(n.to_string()),
        Value::String(s) => Some(s.clone()),
        _ => None,
    }
}

/// The text of the first `<tag>...</tag>` element of an EC2 query response.
fn xml_elem(body: &str, tag: &str) -> Option<String> {
    let open = format!("<{tag}>");
    let close = format!("</{tag}>");
    let start = body.find(&open)? + open.len();
    let end = body[start..].find(&close)? + start;
    Some(body[start..end].to_string())
}

/// CFN `Tags` as `TagSpecification.1.*` params for a create call.
fn tag_spec_params(props: &Value, resource_type: &str, params: &mut HashMap<String, String>) {
    let Some(tags) = props.get("Tags").and_then(|v| v.as_array()) else {
        return;
    };
    params.insert(
        "TagSpecification.1.ResourceType".to_string(),
        resource_type.to_string(),
    );
    for (i, t) in tags.iter().enumerate() {
        if let (Some(k), Some(v)) = (prop_str(t, "Key"), prop_str(t, "Value")) {
            params.insert(
                format!("TagSpecification.1.Tag.{}.Key", i + 1),
                k.to_string(),
            );
            params.insert(
                format!("TagSpecification.1.Tag.{}.Value", i + 1),
                v.to_string(),
            );
        }
    }
}

/// The route destination a CFN `AWS::EC2::Route` names, as the
/// `(query param, value)` CreateRoute / DeleteRoute take.
fn route_destination(props: &Value) -> Option<(&'static str, String)> {
    [
        "DestinationCidrBlock",
        "DestinationIpv6CidrBlock",
        "DestinationPrefixListId",
    ]
    .into_iter()
    .find_map(|k| prop_str(props, k).map(|v| (k, v.to_string())))
}

/// The query param a route destination value goes in, recovered from the
/// value alone (for delete, which only has the physical id).
fn destination_param(dest: &str) -> &'static str {
    if dest.starts_with("pl-") {
        "DestinationPrefixListId"
    } else if dest.contains(':') {
        "DestinationIpv6CidrBlock"
    } else {
        "DestinationCidrBlock"
    }
}

impl ResourceProvisioner {
    /// `AWS::EC2::SecurityGroupIngress` / `SecurityGroupEgress`: one rule
    /// authorized on the group. `Ref` returns the security group rule id.
    pub(super) fn create_ec2_sg_rule(
        &self,
        resource: &ResourceDefinition,
        egress: bool,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let group_id = match prop_str(props, "GroupId") {
            Some(id) => id.to_string(),
            // Ingress may name a default-VPC group by name instead.
            None => {
                let name = prop_str(props, "GroupName")
                    .filter(|_| !egress)
                    .ok_or("SecurityGroup rule requires GroupId")?;
                fakecloud_ec2::vpc_lookup::with_account(
                    &self.ec2_state,
                    &self.account_id,
                    &self.region,
                    |state| {
                        let default_vpc = fakecloud_ec2::vpc_lookup::default_vpc_id_in(state)?;
                        state
                            .security_groups
                            .values()
                            .find(|g| g.group_name == name && g.vpc_id == default_vpc)
                            .map(|g| g.group_id.clone())
                    },
                )
                .ok_or_else(|| format!("Security group {name} does not exist"))?
            }
        };
        let mut params = HashMap::new();
        params.insert("GroupId".to_string(), group_id.clone());
        let p = "IpPermissions.1";
        params.insert(
            format!("{p}.IpProtocol"),
            prop_str(props, "IpProtocol").unwrap_or("-1").to_string(),
        );
        if let Some(from) = props.get("FromPort").and_then(num_str) {
            params.insert(format!("{p}.FromPort"), from);
        }
        if let Some(to) = props.get("ToPort").and_then(num_str) {
            params.insert(format!("{p}.ToPort"), to);
        }
        let description = prop_str(props, "Description");
        let mut source = |kind: &str, field: &str, value: &str| {
            params.insert(format!("{p}.{kind}.1.{field}"), value.to_string());
            if let Some(d) = description {
                params.insert(format!("{p}.{kind}.1.Description"), d.to_string());
            }
        };
        if let Some(cidr) = prop_str(props, "CidrIp") {
            source("IpRanges", "CidrIp", cidr);
        } else if let Some(cidr) = prop_str(props, "CidrIpv6") {
            source("Ipv6Ranges", "CidrIpv6", cidr);
        } else if let Some(g) = prop_str(props, "SourceSecurityGroupId")
            .or_else(|| prop_str(props, "DestinationSecurityGroupId"))
        {
            source("Groups", "GroupId", g);
        } else if let Some(pl) = prop_str(props, "SourcePrefixListId")
            .or_else(|| prop_str(props, "DestinationPrefixListId"))
        {
            source("PrefixListIds", "PrefixListId", pl);
        } else if let Some(name) = prop_str(props, "SourceSecurityGroupName") {
            source("Groups", "GroupName", name);
        }
        let action = if egress {
            "AuthorizeSecurityGroupEgress"
        } else {
            "AuthorizeSecurityGroupIngress"
        };
        let body = self.ec2_dispatch(action, params)?;
        let rule_id = xml_elem(&body, "securityGroupRuleId")
            .ok_or_else(|| format!("{action} returned no securityGroupRuleId"))?;
        Ok(ProvisionResult::new(rule_id.clone()).with("Id", rule_id))
    }

    /// Revoke a standalone SG rule by its rule id.
    fn delete_ec2_sg_rule(&self, rule_id: &str, egress: bool) {
        let group_id = {
            let accounts = self.ec2_state.read();
            accounts
                .regional(&self.account_id, &self.region)
                .and_then(|s| {
                    s.security_groups
                        .values()
                        .find(|g| g.rules.iter().any(|r| r.rule_id == rule_id))
                        .map(|g| g.group_id.clone())
                })
        };
        let Some(group_id) = group_id else {
            return;
        };
        let mut params = HashMap::new();
        params.insert("GroupId".to_string(), group_id);
        params.insert("SecurityGroupRuleId.1".to_string(), rule_id.to_string());
        let action = if egress {
            "RevokeSecurityGroupEgress"
        } else {
            "RevokeSecurityGroupIngress"
        };
        let _ = self.ec2_dispatch(action, params);
    }

    /// `AWS::EC2::VPCGatewayAttachment`: attach an internet or VPN gateway to
    /// a VPC. The physical id is `IGW|<vpc>` / `VGW|<vpc>`, as on AWS.
    pub(super) fn create_ec2_vpc_gateway_attachment(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let vpc_id = prop_str(props, "VpcId").ok_or("VPCGatewayAttachment requires VpcId")?;
        let mut params = HashMap::new();
        params.insert("VpcId".to_string(), vpc_id.to_string());
        let kind = if let Some(igw) = prop_str(props, "InternetGatewayId") {
            params.insert("InternetGatewayId".to_string(), igw.to_string());
            self.ec2_dispatch("AttachInternetGateway", params)?;
            "IGW"
        } else if let Some(vgw) = prop_str(props, "VpnGatewayId") {
            params.insert("VpnGatewayId".to_string(), vgw.to_string());
            self.ec2_dispatch("AttachVpnGateway", params)?;
            "VGW"
        } else {
            return Err(
                "VPCGatewayAttachment requires InternetGatewayId or VpnGatewayId".to_string(),
            );
        };
        Ok(ProvisionResult::new(format!("{kind}|{vpc_id}")))
    }

    /// Detach the gateway a `VPCGatewayAttachment` attached.
    fn delete_ec2_vpc_gateway_attachment(&self, physical_id: &str) {
        let Some((kind, vpc_id)) = physical_id.split_once('|') else {
            return;
        };
        let gateway = {
            let accounts = self.ec2_state.read();
            accounts
                .regional(&self.account_id, &self.region)
                .and_then(|s| match kind {
                    "IGW" => s
                        .internet_gateways
                        .values()
                        .find(|g| g.attachments.iter().any(|(v, _)| v == vpc_id))
                        .map(|g| g.internet_gateway_id.clone()),
                    _ => s
                        .vpn_gateways
                        .values()
                        .find(|g| g.attachments.iter().any(|v| v == vpc_id))
                        .map(|g| g.id.clone()),
                })
        };
        let Some(gateway) = gateway else {
            return;
        };
        let mut params = HashMap::new();
        params.insert("VpcId".to_string(), vpc_id.to_string());
        let action = if kind == "IGW" {
            params.insert("InternetGatewayId".to_string(), gateway);
            "DetachInternetGateway"
        } else {
            params.insert("VpnGatewayId".to_string(), gateway);
            "DetachVpnGateway"
        };
        let _ = self.ec2_dispatch(action, params);
    }

    /// `AWS::EC2::Route`: a route in a route table. The physical id is
    /// `<route table>|<destination>`.
    pub(super) fn create_ec2_route(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let rtb = prop_str(props, "RouteTableId").ok_or("Route requires RouteTableId")?;
        let (dest_key, dest) = route_destination(props).ok_or(
            "Route requires DestinationCidrBlock, DestinationIpv6CidrBlock or DestinationPrefixListId",
        )?;
        let mut params = HashMap::new();
        params.insert("RouteTableId".to_string(), rtb.to_string());
        params.insert(dest_key.to_string(), dest.clone());
        for target in [
            "GatewayId",
            "NatGatewayId",
            "InstanceId",
            "NetworkInterfaceId",
            "VpcPeeringConnectionId",
            "TransitGatewayId",
            "EgressOnlyInternetGatewayId",
            "CarrierGatewayId",
            "LocalGatewayId",
            "VpcEndpointId",
            "CoreNetworkArn",
        ] {
            if let Some(v) = prop_str(props, target) {
                params.insert(target.to_string(), v.to_string());
            }
        }
        self.ec2_dispatch("CreateRoute", params)?;
        Ok(ProvisionResult::new(format!("{rtb}|{dest}")))
    }

    fn delete_ec2_route(&self, physical_id: &str) {
        let Some((rtb, dest)) = physical_id.split_once('|') else {
            return;
        };
        let mut params = HashMap::new();
        params.insert("RouteTableId".to_string(), rtb.to_string());
        params.insert(destination_param(dest).to_string(), dest.to_string());
        let _ = self.ec2_dispatch("DeleteRoute", params);
    }

    /// `AWS::EC2::EIP`: allocate an Elastic IP (and associate it with
    /// `InstanceId` when given). `Ref` returns the public IP.
    pub(super) fn create_ec2_eip(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let mut params = HashMap::new();
        params.insert(
            "Domain".to_string(),
            prop_str(props, "Domain").unwrap_or("vpc").to_string(),
        );
        for key in [
            "NetworkBorderGroup",
            "PublicIpv4Pool",
            "Address",
            "IpamPoolId",
        ] {
            if let Some(v) = prop_str(props, key) {
                params.insert(key.to_string(), v.to_string());
            }
        }
        tag_spec_params(props, "elastic-ip", &mut params);
        let body = self.ec2_dispatch("AllocateAddress", params)?;
        let public_ip =
            xml_elem(&body, "publicIp").ok_or("AllocateAddress returned no publicIp")?;
        let allocation_id =
            xml_elem(&body, "allocationId").ok_or("AllocateAddress returned no allocationId")?;
        if let Some(instance) = prop_str(props, "InstanceId") {
            let mut assoc = HashMap::new();
            assoc.insert("AllocationId".to_string(), allocation_id.clone());
            assoc.insert("InstanceId".to_string(), instance.to_string());
            if let Err(e) = self.ec2_dispatch("AssociateAddress", assoc) {
                let _ = self.release_eip(&allocation_id);
                return Err(e);
            }
        }
        Ok(ProvisionResult::new(public_ip.clone())
            .with("PublicIp", public_ip)
            .with("AllocationId", allocation_id))
    }

    fn release_eip(&self, allocation_id: &str) -> Result<(), String> {
        let mut params = HashMap::new();
        params.insert("AllocationId".to_string(), allocation_id.to_string());
        self.ec2_dispatch("ReleaseAddress", params).map(|_| ())
    }

    /// Disassociate (when associated) and release a stack's Elastic IP. An
    /// address that is already gone is not a failure; a disassociate or
    /// release that fails is, so a replacement never leaks the old address.
    fn delete_ec2_eip(&self, public_ip: &str) -> Result<(), String> {
        let found = {
            let accounts = self.ec2_state.read();
            accounts
                .regional(&self.account_id, &self.region)
                .and_then(|s| {
                    s.elastic_ips
                        .values()
                        .find(|e| e.public_ip == public_ip || e.allocation_id == public_ip)
                        .map(|e| (e.allocation_id.clone(), e.association_id.clone()))
                })
        };
        let Some((allocation_id, association_id)) = found else {
            return Ok(());
        };
        if let Some(association_id) = association_id {
            let mut params = HashMap::new();
            params.insert("AssociationId".to_string(), association_id);
            self.ec2_dispatch("DisassociateAddress", params)?;
        }
        self.release_eip(&allocation_id)
    }

    /// The allocation behind a stack's EIP (physical id = public IP).
    fn eip_record(&self, public_ip: &str) -> Option<fakecloud_ec2::state::ElasticIp> {
        let accounts = self.ec2_state.read();
        accounts
            .regional(&self.account_id, &self.region)
            .and_then(|s| {
                s.elastic_ips
                    .values()
                    .find(|e| e.public_ip == public_ip || e.allocation_id == public_ip)
                    .cloned()
            })
    }

    /// Whether an `AWS::EC2::EIP` update changes a property that requires
    /// replacement (`Domain`, `NetworkBorderGroup`, `PublicIpv4Pool`,
    /// `Address`); `InstanceId` and `Tags` update in place.
    pub(super) fn eip_requires_replacement(
        &self,
        existing: &StackResource,
        new_def: &ResourceDefinition,
    ) -> bool {
        let Some(eip) = self.eip_record(&existing.physical_id) else {
            return true;
        };
        let props = &new_def.properties;
        let differs = |key: &str, current: &str, default: &str| {
            prop_str(props, key).unwrap_or(default) != current
        };
        differs("Domain", &eip.domain, "vpc")
            || prop_str(props, "NetworkBorderGroup").is_some_and(|v| v != eip.network_border_group)
            || prop_str(props, "PublicIpv4Pool").is_some_and(|v| v != eip.public_ipv4_pool)
            || prop_str(props, "Address").is_some_and(|v| v != eip.public_ip)
    }

    /// In-place `AWS::EC2::EIP` update: re-associate to a changed
    /// `InstanceId` (or disassociate when dropped) and replace the tags.
    pub(super) fn update_ec2_eip(
        &self,
        existing: &StackResource,
        new_def: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &new_def.properties;
        let eip = self
            .eip_record(&existing.physical_id)
            .ok_or_else(|| format!("Elastic IP {} not found", existing.physical_id))?;
        let wanted = prop_str(props, "InstanceId");
        if wanted != eip.instance_id.as_deref() {
            if let Some(association_id) = &eip.association_id {
                let mut params = HashMap::new();
                params.insert("AssociationId".to_string(), association_id.clone());
                self.ec2_dispatch("DisassociateAddress", params)?;
            }
            if let Some(instance) = wanted {
                let mut params = HashMap::new();
                params.insert("AllocationId".to_string(), eip.allocation_id.clone());
                params.insert("InstanceId".to_string(), instance.to_string());
                self.ec2_dispatch("AssociateAddress", params)?;
            }
        }
        self.replace_ec2_tags(&eip.allocation_id, props)?;
        Ok(ProvisionResult::new(existing.physical_id.clone())
            .with("PublicIp", eip.public_ip)
            .with("AllocationId", eip.allocation_id))
    }

    /// Whether an `AWS::EC2::NatGateway` update changes a property that
    /// requires replacement (`SubnetId`, `AllocationId`, `ConnectivityType`);
    /// `Tags` update in place.
    pub(super) fn nat_gateway_requires_replacement(
        &self,
        existing: &StackResource,
        new_def: &ResourceDefinition,
    ) -> bool {
        let nat = {
            let accounts = self.ec2_state.read();
            accounts
                .regional(&self.account_id, &self.region)
                .and_then(|s| s.nat_gateways.get(&existing.physical_id).cloned())
        };
        let Some(nat) = nat else {
            return true;
        };
        let props = &new_def.properties;
        prop_str(props, "SubnetId").unwrap_or_default() != nat.subnet_id
            || prop_str(props, "ConnectivityType").unwrap_or("public") != nat.connectivity_type
            || prop_str(props, "AllocationId") != nat.allocation_id.as_deref()
    }

    /// In-place `AWS::EC2::NatGateway` update: replace the tags.
    pub(super) fn update_ec2_nat_gateway(
        &self,
        existing: &StackResource,
        new_def: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        self.replace_ec2_tags(&existing.physical_id, &new_def.properties)?;
        Ok(ProvisionResult::new(existing.physical_id.clone())
            .with("NatGatewayId", existing.physical_id.clone()))
    }

    /// Make a resource's tags exactly the template's `Tags`.
    fn replace_ec2_tags(&self, resource_id: &str, props: &Value) -> Result<(), String> {
        let mut clear = HashMap::new();
        clear.insert("ResourceId.1".to_string(), resource_id.to_string());
        self.ec2_dispatch("DeleteTags", clear)?;
        let mut params = HashMap::new();
        params.insert("ResourceId.1".to_string(), resource_id.to_string());
        let mut n = 0;
        for t in props
            .get("Tags")
            .and_then(|v| v.as_array())
            .into_iter()
            .flatten()
        {
            if let (Some(k), Some(v)) = (prop_str(t, "Key"), prop_str(t, "Value")) {
                n += 1;
                params.insert(format!("Tag.{n}.Key"), k.to_string());
                params.insert(format!("Tag.{n}.Value"), v.to_string());
            }
        }
        if n > 0 {
            self.ec2_dispatch("CreateTags", params)?;
        }
        Ok(())
    }

    /// `AWS::EC2::NatGateway`: a NAT gateway in a subnet. `Ref` returns the
    /// NAT gateway id.
    pub(super) fn create_ec2_nat_gateway(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let subnet = prop_str(props, "SubnetId").ok_or("NatGateway requires SubnetId")?;
        let connectivity = prop_str(props, "ConnectivityType").unwrap_or("public");
        let mut params = HashMap::new();
        params.insert("SubnetId".to_string(), subnet.to_string());
        params.insert("ConnectivityType".to_string(), connectivity.to_string());
        if let Some(alloc) = prop_str(props, "AllocationId") {
            params.insert("AllocationId".to_string(), alloc.to_string());
        } else if connectivity == "public" {
            return Err("A public NatGateway requires AllocationId".to_string());
        }
        if let Some(ip) = prop_str(props, "PrivateIpAddress") {
            params.insert("PrivateIpAddress".to_string(), ip.to_string());
        }
        tag_spec_params(props, "natgateway", &mut params);
        let body = self.ec2_dispatch("CreateNatGateway", params)?;
        let id =
            xml_elem(&body, "natGatewayId").ok_or("CreateNatGateway returned no natGatewayId")?;
        Ok(ProvisionResult::new(id.clone()).with("NatGatewayId", id))
    }

    /// Delete the backing EC2 state of a standalone networking resource. A
    /// resource already gone is not a failure; a failed EIP release or NAT
    /// gateway delete is, so a replacement never silently leaks it.
    pub(super) fn delete_ec2_network_resource(
        &self,
        resource: &StackResource,
    ) -> Result<(), String> {
        let id = resource.physical_id.as_str();
        match resource.resource_type.as_str() {
            "AWS::EC2::SecurityGroupIngress" => self.delete_ec2_sg_rule(id, false),
            "AWS::EC2::SecurityGroupEgress" => self.delete_ec2_sg_rule(id, true),
            "AWS::EC2::VPCGatewayAttachment" => self.delete_ec2_vpc_gateway_attachment(id),
            "AWS::EC2::Route" => self.delete_ec2_route(id),
            "AWS::EC2::EIP" => return self.delete_ec2_eip(id),
            "AWS::EC2::NatGateway" => {
                let exists = self
                    .ec2_state
                    .read()
                    .regional(&self.account_id, &self.region)
                    .is_some_and(|s| s.nat_gateways.contains_key(id));
                if exists {
                    let mut params = HashMap::new();
                    params.insert("NatGatewayId".to_string(), id.to_string());
                    self.ec2_dispatch("DeleteNatGateway", params)?;
                }
            }
            _ => {}
        }
        Ok(())
    }
}
