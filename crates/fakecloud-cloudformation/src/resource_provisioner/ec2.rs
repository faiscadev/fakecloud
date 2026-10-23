//! `AWS::EC2::*` CloudFormation provisioning. Routes through the real EC2
//! control-plane handlers (`Ec2Service::provision_sync`) so CFN-created VPCs,
//! subnets, security groups, route tables, and internet gateways match
//! API-created ones (default SG/NACL/route-table, id formats, tags) instead of
//! being recorded as no-op unknown resources. Bug-hunt 2026-06-25 (1.10).

use std::collections::HashMap;

use bytes::Bytes;
use fakecloud_core::service::AwsRequest;
use fakecloud_ec2::Ec2Service;
use http::{HeaderMap, Method};
use serde_json::Value;

use super::{ProvisionResult, ResourceDefinition, ResourceProvisioner, StackResource};

/// Pull the text of the first `<tag>...</tag>` element out of an EC2 query
/// response. The control-plane responses are flat, so a substring scan is
/// sufficient and avoids a full XML parse.
fn xml_elem(body: &str, tag: &str) -> Option<String> {
    let open = format!("<{tag}>");
    let close = format!("</{tag}>");
    let start = body.find(&open)? + open.len();
    let end = body[start..].find(&close)? + start;
    Some(body[start..end].to_string())
}

fn prop_str<'a>(props: &'a Value, key: &str) -> Option<&'a str> {
    props.get(key).and_then(|v| v.as_str())
}

/// A CloudFormation boolean property may arrive as a JSON bool or as the
/// string `"true"`/`"false"` (templates often quote them).
fn prop_bool(props: &Value, key: &str) -> Option<bool> {
    match props.get(key) {
        Some(Value::Bool(b)) => Some(*b),
        Some(Value::String(s)) => match s.as_str() {
            "true" => Some(true),
            "false" => Some(false),
            _ => None,
        },
        _ => None,
    }
}

/// A CFN numeric property may be a JSON number or a quoted string.
fn num_str(v: &Value) -> Option<String> {
    match v {
        Value::Number(n) => Some(n.to_string()),
        Value::String(s) => Some(s.clone()),
        _ => None,
    }
}

/// Translate a CFN inline `SecurityGroupIngress` / `SecurityGroupEgress` array
/// into `IpPermissions.N.*` query params for
/// `AuthorizeSecurityGroup{Ingress,Egress}`.
fn sg_rule_params(group_id: &str, rules: &[Value]) -> HashMap<String, String> {
    let mut p = HashMap::new();
    p.insert("GroupId".to_string(), group_id.to_string());
    for (i, r) in rules.iter().enumerate() {
        let n = i + 1;
        let proto = prop_str(r, "IpProtocol").unwrap_or("-1");
        p.insert(format!("IpPermissions.{n}.IpProtocol"), proto.to_string());
        if let Some(from) = r.get("FromPort").and_then(num_str) {
            p.insert(format!("IpPermissions.{n}.FromPort"), from);
        }
        if let Some(to) = r.get("ToPort").and_then(num_str) {
            p.insert(format!("IpPermissions.{n}.ToPort"), to);
        }
        if let Some(cidr) = prop_str(r, "CidrIp") {
            p.insert(
                format!("IpPermissions.{n}.IpRanges.1.CidrIp"),
                cidr.to_string(),
            );
        }
        if let Some(cidr6) = prop_str(r, "CidrIpv6") {
            p.insert(
                format!("IpPermissions.{n}.Ipv6Ranges.1.CidrIpv6"),
                cidr6.to_string(),
            );
        }
        if let Some(g) = prop_str(r, "SourceSecurityGroupId")
            .or_else(|| prop_str(r, "DestinationSecurityGroupId"))
        {
            p.insert(format!("IpPermissions.{n}.Groups.1.GroupId"), g.to_string());
        }
        if let Some(pl) =
            prop_str(r, "SourcePrefixListId").or_else(|| prop_str(r, "DestinationPrefixListId"))
        {
            p.insert(
                format!("IpPermissions.{n}.PrefixListIds.1.PrefixListId"),
                pl.to_string(),
            );
        }
    }
    p
}

/// A CFN scalar (string, number or bool) as its query-string form.
fn cfn_scalar(v: &Value) -> Option<String> {
    match v {
        Value::String(s) => Some(s.clone()),
        Value::Number(n) => Some(n.to_string()),
        Value::Bool(b) => Some(b.to_string()),
        _ => None,
    }
}

/// CFN `BlockDeviceMappings` (on `AWS::EC2::Instance`) in RunInstances query
/// form: `BlockDeviceMapping.N.DeviceName`, `.VirtualName`, `.NoDevice` and
/// `.Ebs.<field>`, so the instance gets the EBS volumes a direct launch with
/// the same mappings creates.
fn cfn_block_device_params(props: &Value) -> HashMap<String, String> {
    let mut params = HashMap::new();
    let Some(mappings) = props.get("BlockDeviceMappings").and_then(|v| v.as_array()) else {
        return params;
    };
    for (i, m) in mappings.iter().enumerate() {
        let prefix = format!("BlockDeviceMapping.{}", i + 1);
        for field in ["DeviceName", "VirtualName"] {
            if let Some(v) = m.get(field).and_then(cfn_scalar) {
                params.insert(format!("{prefix}.{field}"), v);
            }
        }
        if m.get("NoDevice").is_some_and(|v| !v.is_null()) {
            params.insert(format!("{prefix}.NoDevice"), String::new());
        }
        if let Some(ebs) = m.get("Ebs").and_then(|v| v.as_object()) {
            for (field, v) in ebs {
                if let Some(v) = cfn_scalar(v) {
                    params.insert(format!("{prefix}.Ebs.{field}"), v);
                }
            }
        }
    }
    params
}

/// The EC2 query member name of a CloudFormation list property. CFN names
/// lists in the plural (`BlockDeviceMappings`, `SecurityGroupIds`, `Tags`),
/// the EC2 query protocol by their singular member name (`BlockDeviceMapping.N`,
/// `SecurityGroupId.N`, `Tag.N`).
fn cfn_list_member(name: &str) -> &str {
    match name {
        "BlockDeviceMappings" => "BlockDeviceMapping",
        "NetworkInterfaces" => "NetworkInterface",
        "Groups" => "SecurityGroupId",
        "SecurityGroupIds" => "SecurityGroupId",
        "SecurityGroups" => "SecurityGroup",
        "TagSpecifications" => "TagSpecification",
        "Tags" => "Tag",
        "ElasticGpuSpecifications" => "ElasticGpuSpecification",
        "ElasticInferenceAccelerators" => "ElasticInferenceAccelerator",
        "LicenseSpecifications" => "LicenseSpecification",
        "Ipv4Prefixes" => "Ipv4Prefix",
        "Ipv6Prefixes" => "Ipv6Prefix",
        other => other,
    }
}

/// Flatten a CloudFormation property value into EC2 query parameters under
/// `prefix`: objects become `prefix.Member`, lists `prefix.Member.N`
/// (1-based, with the singular member name), scalars the value.
fn flatten_cfn_query(prefix: &str, value: &Value, out: &mut HashMap<String, String>) {
    match value {
        Value::Object(map) => {
            for (k, v) in map {
                let name = if v.is_array() {
                    cfn_list_member(k)
                } else {
                    k.as_str()
                };
                let key = if prefix.is_empty() {
                    name.to_string()
                } else {
                    format!("{prefix}.{name}")
                };
                flatten_cfn_query(&key, v, out);
            }
        }
        Value::Array(items) => {
            for (i, item) in items.iter().enumerate() {
                flatten_cfn_query(&format!("{prefix}.{}", i + 1), item, out);
            }
        }
        Value::Null => {}
        scalar => {
            if let Some(v) = cfn_scalar(scalar) {
                out.insert(prefix.to_string(), v);
            }
        }
    }
}

/// A CFN `LaunchTemplate` (`LaunchTemplateSpecification`) property.
fn cfn_launch_template_ref(
    props: &Value,
) -> Option<fakecloud_ec2::cfn_provision::LaunchTemplateRef> {
    let lt = props.get("LaunchTemplate")?;
    let field = |k: &str| lt.get(k).and_then(cfn_scalar).filter(|v| !v.is_empty());
    Some(fakecloud_ec2::cfn_provision::LaunchTemplateRef {
        id: field("LaunchTemplateId"),
        name: field("LaunchTemplateName"),
        version: field("Version"),
    })
}

/// CFN `Tags` as `(key, value)` pairs.
fn cfn_tag_pairs(props: &Value) -> Vec<(String, String)> {
    props
        .get("Tags")
        .and_then(|v| v.as_array())
        .map(|tags| {
            tags.iter()
                .filter_map(|t| {
                    Some((
                        t.get("Key")?.as_str()?.to_string(),
                        t.get("Value").and_then(cfn_scalar).unwrap_or_default(),
                    ))
                })
                .collect()
        })
        .unwrap_or_default()
}

impl ResourceProvisioner {
    fn ec2_request(&self, action: &str, params: HashMap<String, String>) -> AwsRequest {
        AwsRequest {
            service: "ec2".to_string(),
            action: action.to_string(),
            region: self.region.clone(),
            account_id: self.account_id.clone(),
            request_id: "cfn".to_string(),
            headers: HeaderMap::new(),
            query_params: params,
            body: Bytes::new(),
            body_stream: parking_lot::Mutex::new(None),
            path_segments: Vec::new(),
            raw_path: "/".to_string(),
            raw_query: String::new(),
            method: Method::POST,
            is_query_protocol: true,
            access_key_id: None,
            principal: None,
        }
    }

    /// Dispatch one EC2 control-plane action through the real handler and
    /// return the response body as a string for id extraction.
    pub(super) fn ec2_dispatch(
        &self,
        action: &str,
        params: HashMap<String, String>,
    ) -> Result<String, String> {
        let svc = Ec2Service::with_state(self.ec2_state.clone())
            .with_kms_hook(self.kms_hook.clone())
            .with_quota_provider(self.quota_provider.clone());
        let req = self.ec2_request(action, params);
        let resp = svc
            .provision_sync(&req)
            .map_err(|e| format!("EC2 {action} failed: {}", e.message()))?;
        Ok(String::from_utf8_lossy(resp.body.expect_bytes()).to_string())
    }

    /// CFN `Tags` -> repeated `TagSpecification.1.Tag.N.{Key,Value}` params so
    /// the created resource carries its tags, matching a direct CreateTags.
    fn ec2_tag_params(
        &self,
        props: &Value,
        resource_type: &str,
        params: &mut HashMap<String, String>,
    ) {
        let Some(tags) = props.get("Tags").and_then(|v| v.as_array()) else {
            return;
        };
        params.insert(
            "TagSpecification.1.ResourceType".to_string(),
            resource_type.to_string(),
        );
        for (i, t) in tags.iter().enumerate() {
            if let (Some(k), Some(v)) = (
                t.get("Key").and_then(|v| v.as_str()),
                t.get("Value").and_then(|v| v.as_str()),
            ) {
                let n = i + 1;
                params.insert(format!("TagSpecification.1.Tag.{n}.Key"), k.to_string());
                params.insert(format!("TagSpecification.1.Tag.{n}.Value"), v.to_string());
            }
        }
    }

    pub(super) fn create_ec2_vpc(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let mut params = HashMap::new();
        if let Some(cidr) = prop_str(props, "CidrBlock") {
            params.insert("CidrBlock".to_string(), cidr.to_string());
        }
        if let Some(t) = prop_str(props, "InstanceTenancy") {
            params.insert("InstanceTenancy".to_string(), t.to_string());
        }
        self.ec2_tag_params(props, "vpc", &mut params);
        let body = self.ec2_dispatch("CreateVpc", params)?;
        let id = xml_elem(&body, "vpcId").ok_or("CreateVpc returned no vpcId")?;
        let cidr = xml_elem(&body, "cidrBlock").unwrap_or_default();

        // Apply DNS attributes the template requested (CreateVpc ignores them).
        let dns_support = prop_bool(props, "EnableDnsSupport");
        let dns_hostnames = prop_bool(props, "EnableDnsHostnames");
        if dns_support.is_some() || dns_hostnames.is_some() {
            let mut mp = HashMap::new();
            mp.insert("VpcId".to_string(), id.clone());
            if let Some(v) = dns_support {
                mp.insert("EnableDnsSupport.Value".to_string(), v.to_string());
            }
            if let Some(v) = dns_hostnames {
                mp.insert("EnableDnsHostnames.Value".to_string(), v.to_string());
            }
            self.ec2_dispatch("ModifyVpcAttribute", mp)?;
        }

        Ok(ProvisionResult::new(id.clone())
            .with("VpcId", id)
            .with("CidrBlock", cidr))
    }

    pub(super) fn create_ec2_subnet(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let mut params = HashMap::new();
        let vpc_id = prop_str(props, "VpcId").ok_or("Subnet requires VpcId")?;
        params.insert("VpcId".to_string(), vpc_id.to_string());
        if let Some(cidr) = prop_str(props, "CidrBlock") {
            params.insert("CidrBlock".to_string(), cidr.to_string());
        }
        if let Some(az) = prop_str(props, "AvailabilityZone") {
            params.insert("AvailabilityZone".to_string(), az.to_string());
        }
        self.ec2_tag_params(props, "subnet", &mut params);
        let body = self.ec2_dispatch("CreateSubnet", params)?;
        let id = xml_elem(&body, "subnetId").ok_or("CreateSubnet returned no subnetId")?;
        let az = xml_elem(&body, "availabilityZone").unwrap_or_default();
        let cidr = xml_elem(&body, "cidrBlock")
            .or_else(|| prop_str(props, "CidrBlock").map(str::to_string))
            .unwrap_or_default();

        // Apply MapPublicIpOnLaunch the template requested (CreateSubnet
        // ignores it).
        if let Some(v) = prop_bool(props, "MapPublicIpOnLaunch") {
            let mut mp = HashMap::new();
            mp.insert("SubnetId".to_string(), id.clone());
            mp.insert("MapPublicIpOnLaunch.Value".to_string(), v.to_string());
            self.ec2_dispatch("ModifySubnetAttribute", mp)?;
        }

        // Capture VpcId + CidrBlock so Fn::GetAtt on the subnet resolves them
        // (real AWS exposes both), not just SubnetId / AvailabilityZone.
        Ok(ProvisionResult::new(id.clone())
            .with("SubnetId", id)
            .with("AvailabilityZone", az)
            .with("VpcId", vpc_id.to_string())
            .with("CidrBlock", cidr))
    }

    pub(super) fn create_ec2_security_group(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let mut params = HashMap::new();
        let desc = prop_str(props, "GroupDescription").unwrap_or("Managed by CloudFormation");
        params.insert("GroupDescription".to_string(), desc.to_string());
        let generated_name = self.physical_name(resource);
        let name = prop_str(props, "GroupName").unwrap_or(&generated_name);
        params.insert("GroupName".to_string(), name.to_string());
        if let Some(vpc) = prop_str(props, "VpcId") {
            params.insert("VpcId".to_string(), vpc.to_string());
        }
        self.ec2_tag_params(props, "security-group", &mut params);
        let body = self.ec2_dispatch("CreateSecurityGroup", params)?;
        let id = xml_elem(&body, "groupId").ok_or("CreateSecurityGroup returned no groupId")?;

        // Apply inline ingress/egress rules (CreateSecurityGroup only creates
        // the empty group; without this the template's rules are silently
        // dropped and the SG denies everything).
        // AWS CloudFormation replaces the group's default allow-all egress
        // rule with the template's SecurityGroupEgress when one is given, so
        // the template's rules are not stacked on top of it.
        if props
            .get("SecurityGroupEgress")
            .and_then(|v| v.as_array())
            .is_some_and(|rules| !rules.is_empty())
        {
            let mut accounts = self.ec2_state.write();
            let state = accounts.regional_mut(&self.account_id, &self.region);
            if let Some(sg) = state.security_groups.get_mut(&id) {
                sg.rules.retain(|r| !r.is_egress);
            }
        }
        let authorize = || -> Result<(), String> {
            for (key, action) in [
                ("SecurityGroupIngress", "AuthorizeSecurityGroupIngress"),
                ("SecurityGroupEgress", "AuthorizeSecurityGroupEgress"),
            ] {
                if let Some(rules) = props.get(key).and_then(|v| v.as_array()) {
                    if !rules.is_empty() {
                        self.ec2_dispatch(action, sg_rule_params(&id, rules))?;
                    }
                }
            }
            Ok(())
        };
        // A rejected rule set (over the rules-per-group quota, say) fails the
        // resource, which then has no physical id to roll back through, so the
        // group created above is removed here rather than orphaned.
        if let Err(err) = authorize() {
            let mut params = HashMap::new();
            params.insert("GroupId".to_string(), id.clone());
            let _ = self.ec2_dispatch("DeleteSecurityGroup", params);
            return Err(err);
        }

        Ok(ProvisionResult::new(id.clone())
            .with("GroupId", id)
            .with("VpcId", prop_str(props, "VpcId").unwrap_or("").to_string()))
    }

    pub(super) fn create_ec2_internet_gateway(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let mut params = HashMap::new();
        self.ec2_tag_params(&resource.properties, "internet-gateway", &mut params);
        let body = self.ec2_dispatch("CreateInternetGateway", params)?;
        let id = xml_elem(&body, "internetGatewayId")
            .ok_or("CreateInternetGateway returned no internetGatewayId")?;
        Ok(ProvisionResult::new(id.clone()).with("InternetGatewayId", id))
    }

    pub(super) fn create_ec2_route_table(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let mut params = HashMap::new();
        let vpc_id = prop_str(props, "VpcId").ok_or("RouteTable requires VpcId")?;
        params.insert("VpcId".to_string(), vpc_id.to_string());
        self.ec2_tag_params(props, "route-table", &mut params);
        let body = self.ec2_dispatch("CreateRouteTable", params)?;
        let id =
            xml_elem(&body, "routeTableId").ok_or("CreateRouteTable returned no routeTableId")?;
        Ok(ProvisionResult::new(id.clone()).with("RouteTableId", id))
    }

    // --- In-place updates ---
    //
    // These VPC networking resources mint an id (`vpc-`/`subnet-`/`rtb-`) that
    // every child and sibling references via `Ref` (subnets, security groups,
    // route tables, routes, associations, instances, ENIs). Reprovision
    // (delete + create) on a mutable-attribute or tag edit churns that id and
    // orphans all of them. AWS applies these changes in place. The update arms
    // re-dispatch the specific Modify* EC2 call against the EXISTING id and
    // return it unchanged.

    /// `EnableDnsSupport`/`EnableDnsHostnames` are in-place (ModifyVpcAttribute)
    /// in AWS; `CidrBlock`/`InstanceTenancy` are immutable. Preserve the vpc id.
    pub(super) fn update_ec2_vpc(
        &self,
        existing: &StackResource,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let id = existing.physical_id.clone();

        let dns_support = prop_bool(props, "EnableDnsSupport");
        let dns_hostnames = prop_bool(props, "EnableDnsHostnames");
        if dns_support.is_some() || dns_hostnames.is_some() {
            let mut mp = HashMap::new();
            mp.insert("VpcId".to_string(), id.clone());
            if let Some(v) = dns_support {
                mp.insert("EnableDnsSupport.Value".to_string(), v.to_string());
            }
            if let Some(v) = dns_hostnames {
                mp.insert("EnableDnsHostnames.Value".to_string(), v.to_string());
            }
            self.ec2_dispatch("ModifyVpcAttribute", mp)?;
        }

        let cidr = prop_str(props, "CidrBlock").unwrap_or("").to_string();
        Ok(ProvisionResult::new(id.clone())
            .with("VpcId", id)
            .with("CidrBlock", cidr))
    }

    /// `MapPublicIpOnLaunch` is in-place (ModifySubnetAttribute) in AWS;
    /// `CidrBlock`/`AvailabilityZone`/`VpcId` are immutable. Preserve the subnet
    /// id.
    pub(super) fn update_ec2_subnet(
        &self,
        existing: &StackResource,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let id = existing.physical_id.clone();

        if let Some(v) = prop_bool(props, "MapPublicIpOnLaunch") {
            let mut mp = HashMap::new();
            mp.insert("SubnetId".to_string(), id.clone());
            mp.insert("MapPublicIpOnLaunch.Value".to_string(), v.to_string());
            self.ec2_dispatch("ModifySubnetAttribute", mp)?;
        }

        Ok(ProvisionResult::new(id.clone())
            .with("SubnetId", id)
            .with(
                "AvailabilityZone",
                prop_str(props, "AvailabilityZone")
                    .unwrap_or("")
                    .to_string(),
            )
            .with("VpcId", prop_str(props, "VpcId").unwrap_or("").to_string())
            .with(
                "CidrBlock",
                prop_str(props, "CidrBlock").unwrap_or("").to_string(),
            ))
    }

    /// A route table has no in-place-mutable property (routes are separate
    /// `AWS::EC2::Route` resources; tags are re-applied by the create-time
    /// TagSpecification path, not a standalone CreateTags the internal EC2
    /// dispatch supports). The whole point of the arm is to AVOID reprovision,
    /// which would churn the rtb id and orphan every Route /
    /// SubnetRouteTableAssociation child. So preserve the id and report success.
    pub(super) fn update_ec2_route_table(
        &self,
        existing: &StackResource,
        _resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let id = existing.physical_id.clone();
        Ok(ProvisionResult::new(id.clone()).with("RouteTableId", id))
    }

    /// An internet gateway's only property is `Tags` (in-place in AWS), and the
    /// internal EC2 dispatch has no standalone CreateTags. The point of the arm
    /// is to AVOID reprovision, which would churn the igw-id and orphan every
    /// `VPCGatewayAttachment` + route that references it. Preserve the id.
    pub(super) fn update_ec2_internet_gateway(
        &self,
        existing: &StackResource,
        _resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let id = existing.physical_id.clone();
        Ok(ProvisionResult::new(id.clone()).with("InternetGatewayId", id))
    }

    /// In-place `AWS::EC2::SecurityGroup` update. Reprovision would churn the
    /// sg id -- breaking every `Ref`/`SourceSecurityGroupId` that stored it and
    /// dropping the group's rules -- and `GroupDescription`/`GroupName`/`VpcId`
    /// are all immutable anyway, so replacement would fail on those. Preserve
    /// the sg id and reconcile the inline ingress/egress rules in place.
    pub(super) fn update_ec2_security_group(
        &self,
        existing: &StackResource,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let id = existing.physical_id.clone();

        self.reconcile_sg_rules(&id, props, "SecurityGroupIngress", false)?;
        self.reconcile_sg_rules(&id, props, "SecurityGroupEgress", true)?;

        Ok(ProvisionResult::new(id.clone())
            .with("GroupId", id)
            .with("VpcId", prop_str(props, "VpcId").unwrap_or("").to_string()))
    }

    /// Reconcile one rule direction to the template's desired set. A direction
    /// is only touched when the template specifies it INLINE (revoke the
    /// group's current rules in that direction, then authorize the template's)
    /// -- when the property is absent the rules are left alone, since they may
    /// be managed by separate `AWS::EC2::SecurityGroupIngress`/`Egress`
    /// resources whose state must not be wiped by this update.
    fn reconcile_sg_rules(
        &self,
        group_id: &str,
        props: &Value,
        prop_key: &str,
        is_egress: bool,
    ) -> Result<(), String> {
        let Some(rules) = props.get(prop_key).and_then(|v| v.as_array()) else {
            return Ok(());
        };

        // Revoke the group's current rules in this direction. The internal EC2
        // provision dispatch has no Revoke action, so drop them directly in
        // state (equivalent to a revoke-all for the direction the template
        // manages inline), then authorize the desired set via the real handler.
        let removed: Vec<_> = {
            let mut accounts = self.ec2_state.write();
            let state = accounts.regional_mut(&self.account_id, &self.region);
            match state.security_groups.get_mut(group_id) {
                Some(sg) => {
                    let (removed, kept) = std::mem::take(&mut sg.rules)
                        .into_iter()
                        .partition(|r| r.is_egress == is_egress);
                    sg.rules = kept;
                    removed
                }
                None => Vec::new(),
            }
        };
        if !rules.is_empty() {
            let action = if is_egress {
                "AuthorizeSecurityGroupEgress"
            } else {
                "AuthorizeSecurityGroupIngress"
            };
            if let Err(err) = self.ec2_dispatch(action, sg_rule_params(group_id, rules)) {
                // The update failed (the new set is over the rules-per-group
                // quota, say): put the direction's previous rules back so the
                // group is left as it was, not stripped.
                let mut accounts = self.ec2_state.write();
                let state = accounts.regional_mut(&self.account_id, &self.region);
                if let Some(sg) = state.security_groups.get_mut(group_id) {
                    sg.rules.extend(removed);
                }
                return Err(err);
            }
        }
        // The replaced rules are gone for good: their tags go with them.
        let mut accounts = self.ec2_state.write();
        let state = accounts.regional_mut(&self.account_id, &self.region);
        for r in &removed {
            state.tags.remove(&r.rule_id);
        }
        Ok(())
    }

    /// `AWS::EC2::Instance` — create a REAL control-plane instance synchronously
    /// (so `Ref` resolves to the `i-...` id and `Fn::GetAtt`
    /// PrivateIp/PublicIp/AvailabilityZone resolve during provisioning), then
    /// queue a spawn intent that backs it with a real container via the EC2
    /// runtime — the same instance the direct `RunInstances` path launches.
    /// Previously this fell through to the no-op `other =>` catch-all and `Ref`
    /// resolved to the bare logical id, inconsistent with ASG launching real
    /// instances.
    pub(super) fn create_ec2_instance(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let security_group_ids: Vec<String> = props
            .get("SecurityGroupIds")
            .and_then(|v| v.as_array())
            .map(|a| {
                a.iter()
                    .filter_map(|x| x.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();
        // MetadataOptions: only the fields the resource sets, so the rest come
        // from a launch template (or the AWS defaults).
        let metadata_options = props
            .get("MetadataOptions")
            .map(|m| {
                let s = |key: &str| m.get(key).and_then(cfn_scalar);
                fakecloud_ec2::cfn_provision::CfnMetadataOptions {
                    http_tokens: s("HttpTokens"),
                    http_endpoint: s("HttpEndpoint"),
                    http_put_response_hop_limit: s("HttpPutResponseHopLimit")
                        .and_then(|v| v.parse().ok()),
                    http_protocol_ipv6: s("HttpProtocolIpv6"),
                    instance_metadata_tags: s("InstanceMetadataTags"),
                }
            })
            .unwrap_or_default();

        let (iam_instance_profile_arn, iam_instance_profile_name) = cfn_iam_instance_profile(props);
        // Validate here, the way Associate/Replace do, so a template that
        // resolves IamInstanceProfile to a role ARN (a common Fn::GetAtt
        // mistake) fails the create instead of storing a value the next
        // UpdateStack cannot re-submit.
        validate_cfn_iam_instance_profile(&iam_instance_profile_arn, &iam_instance_profile_name)?;
        // A `Ref` to an AWS::IAM::InstanceProfile is the profile name; resolve
        // it to the ARN IAM stored so a non-default Path survives.
        let iam_instance_profile_arn = iam_instance_profile_arn.or_else(|| {
            iam_instance_profile_name
                .as_deref()
                .and_then(|n| self.resolve_instance_profile_arn(n))
        });

        let spec = fakecloud_ec2::cfn_provision::CfnInstanceSpec {
            image_id: prop_str(props, "ImageId").map(String::from),
            instance_type: prop_str(props, "InstanceType").map(String::from),
            subnet_id: prop_str(props, "SubnetId").map(String::from),
            availability_zone: props
                .get("AvailabilityZone")
                .and_then(|v| v.as_str())
                .map(String::from)
                .or_else(|| {
                    props
                        .get("Placement")
                        .and_then(|p| p.get("AvailabilityZone"))
                        .and_then(|v| v.as_str())
                        .map(String::from)
                }),
            security_group_ids,
            key_name: prop_str(props, "KeyName").map(String::from),
            user_data: prop_str(props, "UserData").map(String::from),
            private_ip: prop_str(props, "PrivateIpAddress").map(String::from),
            metadata_options,
            // CFN `EbsOptimized` / `Monitoring` are booleans; templates may pass
            // them as JSON bool or the stringified form after Ref resolution.
            ebs_optimized: prop_bool(props, "EbsOptimized"),
            monitoring: prop_bool(props, "Monitoring"),
            iam_instance_profile_arn,
            iam_instance_profile_name,
            block_device_params: cfn_block_device_params(props),
            // `LaunchTemplate`: the template version fills every property the
            // resource leaves out, through the same resolution RunInstances
            // applies.
            launch_template: cfn_launch_template_ref(props),
            tags: cfn_tag_pairs(props),
            propagate_tags_to_volumes: prop_bool(props, "PropagateTagsToVolumeOnCreation")
                .unwrap_or(false),
        };

        let attrs = fakecloud_ec2::cfn_provision::cfn_create(
            self.ec2_state.clone(),
            self.kms_hook.clone(),
            self.quota_provider.clone(),
            &self.account_id,
            &self.region,
            &spec,
        )?;

        // Background the container boot via the spawn-intent drain so stack
        // creation never blocks on a cold image pull / Pod readiness.
        self.pending_container_spawns
            .lock()
            .push(super::ContainerSpawnIntent::Ec2Instance {
                instance_id: attrs.instance_id.clone(),
            });

        let mut result = ProvisionResult::new(attrs.instance_id.clone())
            .with("PrivateIp", attrs.private_ip)
            .with("AvailabilityZone", attrs.availability_zone);
        if let Some(public_ip) = attrs.public_ip {
            result = result.with("PublicIp", public_ip);
        }
        Ok(result)
    }

    /// Whether an `AWS::EC2::Instance` update points at a different launch
    /// template version than the instance was launched from (compared via the
    /// `aws:ec2launchtemplate:*` tags the launch recorded), which AWS handles
    /// by replacing the instance.
    pub(super) fn instance_launch_template_changed(
        &self,
        existing: &StackResource,
        resource: &ResourceDefinition,
    ) -> bool {
        let launched = {
            let accounts = self.ec2_state.read();
            let Some(st) = accounts.regional(&self.account_id, &self.region) else {
                return false;
            };
            let tag = |k: &str| {
                st.tags_for(&existing.physical_id)
                    .iter()
                    .find(|t| t.key == k)
                    .map(|t| t.value.clone())
            };
            tag("aws:ec2launchtemplate:id").zip(tag("aws:ec2launchtemplate:version"))
        };
        let wanted = cfn_launch_template_ref(&resource.properties).map(|lt| {
            fakecloud_ec2::service::launch_template::resolve_launch_template_in(
                &self.ec2_state,
                &self.account_id,
                &self.region,
                lt.id.as_deref(),
                lt.name.as_deref(),
                lt.version.as_deref(),
            )
            .map(|r| (r.id, r.version.to_string()))
        });
        match (launched, wanted) {
            (None, None) => false,
            // An unresolvable template: let the replacement surface the error.
            (_, Some(Err(_))) => true,
            (Some(l), Some(Ok(w))) => l != w,
            _ => true,
        }
    }

    /// In-place stack update for `AWS::EC2::Instance`. An instance is stateful
    /// (a real backing container / attached EBS with data), so a benign
    /// property or tag change must NOT terminate + relaunch it the way the
    /// reprovision fallback would -- that destroys the container and its data.
    /// This applies the mutable-without-replacement attributes through the REAL
    /// `ModifyInstanceAttribute` handler (instance type, EBS-optimized flag,
    /// user data, security groups) and re-applies tags through `CreateTags`,
    /// keeping the instance id and its backing container intact. Properties that
    /// genuinely force replacement in real AWS (AMI, subnet, AZ) are left to the
    /// instance's existing value here; an in-place update never replaces it.
    pub(super) fn update_ec2_instance(
        &self,
        existing: &StackResource,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let instance_id = existing.physical_id.clone();

        // Mutable attributes via ModifyInstanceAttribute (convenience form).
        let mut attr_params: HashMap<String, String> = HashMap::new();
        attr_params.insert("InstanceId".to_string(), instance_id.clone());
        let mut has_attr_change = false;
        if let Some(v) = prop_str(props, "InstanceType") {
            attr_params.insert("InstanceType.Value".to_string(), v.to_string());
            has_attr_change = true;
        }
        if let Some(v) = prop_bool(props, "EbsOptimized") {
            attr_params.insert("EbsOptimized.Value".to_string(), v.to_string());
            has_attr_change = true;
        }
        if let Some(v) = prop_str(props, "UserData") {
            attr_params.insert("UserData.Value".to_string(), v.to_string());
            has_attr_change = true;
        }
        if let Some(sgs) = props.get("SecurityGroupIds").and_then(|v| v.as_array()) {
            for (i, sg) in sgs.iter().enumerate() {
                if let Some(s) = sg.as_str() {
                    let n = i + 1;
                    attr_params.insert(format!("GroupId.{n}"), s.to_string());
                    has_attr_change = true;
                }
            }
        }
        if has_attr_change {
            self.ec2_dispatch("ModifyInstanceAttribute", attr_params)?;
        }

        // Re-apply the template's Tags in place (mirrors a direct CreateTags),
        // so an added / changed tag never triggers replacement.
        if let Some(tags) = props.get("Tags").and_then(|v| v.as_array()) {
            if !tags.is_empty() {
                let mut params = HashMap::new();
                params.insert("ResourceId.1".to_string(), instance_id.clone());
                for (i, t) in tags.iter().enumerate() {
                    if let (Some(k), Some(v)) = (
                        t.get("Key").and_then(|v| v.as_str()),
                        t.get("Value").and_then(|v| v.as_str()),
                    ) {
                        let n = i + 1;
                        params.insert(format!("Tag.{n}.Key"), k.to_string());
                        params.insert(format!("Tag.{n}.Value"), v.to_string());
                    }
                }
                self.ec2_dispatch("CreateTags", params)?;
            }
        }

        self.sync_ec2_instance_profile(props, &instance_id)?;

        // The identity attributes (private ip, AZ, public ip) do not change on
        // an in-place modify; carry the ones captured at create time forward.
        Ok(ProvisionResult::new(instance_id).merge_attributes(existing.attributes.clone()))
    }

    /// The ARN of a profile this stack's IAM state knows by name. `Ref` on an
    /// `AWS::IAM::InstanceProfile` resolves to the profile *name*, and only IAM
    /// knows the Path that name's ARN carries, so resolving here is what keeps
    /// a pathed profile's ARN (and the id derived from it) the same on the
    /// instance as in `GetInstanceProfile`. `None` for a profile IAM does not
    /// hold, which stays a name-addressed association as before.
    fn resolve_instance_profile_arn(&self, name: &str) -> Option<String> {
        let accounts = self.iam_state.read();
        let state = accounts.get(&self.account_id)?;
        state
            .instance_profiles
            .get(name)
            .map(|profile| profile.arn.clone())
    }

    /// Bring the instance's IAM instance-profile association in line with the
    /// template. AWS updates `IamInstanceProfile` in place ("some interruption",
    /// no replacement), so this replaces an existing association, associates
    /// when the instance has none, and disassociates when the template dropped
    /// the property.
    fn sync_ec2_instance_profile(&self, props: &Value, instance_id: &str) -> Result<(), String> {
        let (arn, name) = cfn_iam_instance_profile(props);
        validate_cfn_iam_instance_profile(&arn, &name)?;
        // Prefer an ARN: the template's own, else the one IAM stored for that
        // name (which carries the Path). A name IAM does not hold stays a
        // name-addressed association, as before.
        // Whether the template addressed the profile by name. Kept because the
        // "unchanged" test below has to compare on the name in that case: the
        // stored association may hold a path-less ARN synthesized before IAM
        // had the profile, which names the same profile the template does.
        let by_name = arn.is_none() && name.is_some();
        let wanted = arn
            .or_else(|| {
                name.as_deref()
                    .and_then(|n| self.resolve_instance_profile_arn(n))
            })
            .map(|a| ("IamInstanceProfile.Arn", a))
            .or_else(|| name.map(|n| ("IamInstanceProfile.Name", n)));

        let mut lookup = HashMap::new();
        lookup.insert("Filter.1.Name".to_string(), "instance-id".to_string());
        lookup.insert("Filter.1.Value.1".to_string(), instance_id.to_string());
        let existing = self.ec2_dispatch("DescribeIamInstanceProfileAssociations", lookup)?;
        let existing_id = xml_elem(&existing, "associationId");
        let existing_arn = xml_elem(&existing, "arn");

        match (wanted, existing_id) {
            (None, None) => Ok(()),
            (None, Some(id)) => {
                let mut params = HashMap::new();
                params.insert("AssociationId".to_string(), id);
                self.ec2_dispatch("DisassociateIamInstanceProfile", params)?;
                Ok(())
            }
            (Some((key, value)), Some(id)) => {
                // An in-place update runs for any changed property, so only
                // replace when the profile itself changed. Replacing anyway
                // would retire the association id on an unrelated edit (an
                // InstanceType bump, a new tag), which AWS leaves alone.
                let profile_name = |arn: &str| arn.rsplit('/').next().unwrap_or(arn).to_string();
                let unchanged = existing_arn.as_deref().is_some_and(|arn| {
                    arn == value || (by_name && profile_name(arn) == profile_name(&value))
                });
                if unchanged {
                    return Ok(());
                }
                let mut params = HashMap::new();
                params.insert("AssociationId".to_string(), id);
                params.insert(key.to_string(), value);
                self.ec2_dispatch("ReplaceIamInstanceProfileAssociation", params)?;
                Ok(())
            }
            (Some((key, value)), None) => {
                let mut params = HashMap::new();
                params.insert("InstanceId".to_string(), instance_id.to_string());
                params.insert(key.to_string(), value);
                self.ec2_dispatch("AssociateIamInstanceProfile", params)?;
                Ok(())
            }
        }
    }

    /// The `CreateLaunchTemplate` / `CreateLaunchTemplateVersion` parameters
    /// for an `AWS::EC2::LaunchTemplate`'s `LaunchTemplateData` and
    /// `VersionDescription`.
    fn launch_template_data_params(props: &Value) -> HashMap<String, String> {
        let mut params = HashMap::new();
        if let Some(data) = props.get("LaunchTemplateData") {
            flatten_cfn_query("LaunchTemplateData", data, &mut params);
        }
        if let Some(v) = props.get("VersionDescription").and_then(cfn_scalar) {
            params.insert("VersionDescription".to_string(), v);
        }
        params
    }

    /// `AWS::EC2::LaunchTemplate` through the real CreateLaunchTemplate
    /// handler, so the stack template's `LaunchTemplateData` is stored as
    /// version 1 and resolvable by every launch (RunInstances, an
    /// `AWS::EC2::Instance` or an Auto Scaling group that references it).
    pub(super) fn create_ec2_launch_template(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let name = prop_str(props, "LaunchTemplateName")
            .map(String::from)
            .unwrap_or_else(|| self.physical_name(resource));
        let mut params = Self::launch_template_data_params(props);
        params.insert("LaunchTemplateName".to_string(), name);
        if let Some(specs) = props.get("TagSpecifications").and_then(|v| v.as_array()) {
            for (i, spec) in specs.iter().enumerate() {
                flatten_cfn_query(&format!("TagSpecification.{}", i + 1), spec, &mut params);
            }
        }
        let body = self.ec2_dispatch("CreateLaunchTemplate", params)?;
        let id = xml_elem(&body, "launchTemplateId")
            .ok_or("CreateLaunchTemplate returned no launchTemplateId")?;
        Ok(ProvisionResult::new(id.clone())
            .with("LaunchTemplateId", id)
            .with("LatestVersionNumber", "1".to_string())
            .with("DefaultVersionNumber", "1".to_string()))
    }

    /// Whether an `AWS::EC2::LaunchTemplate` update keeps the template's name
    /// (an explicit `LaunchTemplateName` change replaces the template).
    pub(super) fn launch_template_name_kept(
        &self,
        existing: &StackResource,
        resource: &ResourceDefinition,
    ) -> bool {
        let accounts = self.ec2_state.read();
        let Some(current) = accounts
            .regional(&self.account_id, &self.region)
            .and_then(|s| s.launch_templates.get(&existing.physical_id))
            .map(|t| t.name.clone())
        else {
            return false;
        };
        match prop_str(&resource.properties, "LaunchTemplateName") {
            Some(wanted) => current == wanted,
            // Dropping an explicit name replaces the template with a
            // generated-name one; a generated name is kept.
            None => super::naming::is_generated_name(
                &self.stack_id,
                &resource.logical_id,
                &resource.resource_type,
                &current,
            ),
        }
    }

    /// In-place update of an `AWS::EC2::LaunchTemplate`: new
    /// `LaunchTemplateData` becomes a new version that is made the default,
    /// keeping the template id (a rename replaces the template).
    pub(super) fn update_ec2_launch_template(
        &self,
        existing: &StackResource,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let id = existing.physical_id.clone();
        let mut params = Self::launch_template_data_params(props);
        params.insert("LaunchTemplateId".to_string(), id.clone());
        let body = self.ec2_dispatch("CreateLaunchTemplateVersion", params)?;
        let version = xml_elem(&body, "versionNumber")
            .ok_or("CreateLaunchTemplateVersion returned no versionNumber")?;
        let mut modify = HashMap::new();
        modify.insert("LaunchTemplateId".to_string(), id.clone());
        modify.insert("SetDefaultVersion".to_string(), version.clone());
        self.ec2_dispatch("ModifyLaunchTemplate", modify)?;
        // The template's own tags follow `TagSpecifications` (resource type
        // `launch-template`): added, changed and removed in place.
        let desired: Vec<fakecloud_ec2::state::Tag> = props
            .get("TagSpecifications")
            .and_then(|v| v.as_array())
            .into_iter()
            .flatten()
            .filter(|spec| {
                spec.get("ResourceType").and_then(|v| v.as_str()) == Some("launch-template")
            })
            .flat_map(cfn_tag_pairs)
            .map(|(key, value)| fakecloud_ec2::state::Tag { key, value })
            .collect();
        {
            let mut accounts = self.ec2_state.write();
            let state = accounts.regional_mut(&self.account_id, &self.region);
            if desired.is_empty() {
                state.tags.remove(&id);
            } else {
                state.tags.insert(id.clone(), desired);
            }
        }
        Ok(ProvisionResult::new(id.clone())
            .with("LaunchTemplateId", id)
            .with("LatestVersionNumber", version.clone())
            .with("DefaultVersionNumber", version))
    }

    /// Delete an EC2 resource by its physical id, routing through the real
    /// handler so dependent default resources are cleaned up correctly.
    /// `AWS::EC2::Volume` through the real CreateVolume handler, so a stack
    /// volume is sized, typed and encrypted exactly as a direct CreateVolume
    /// (encryption by default, the account's EBS default key, a named key
    /// reported as its ARN, a source snapshot's size and key).
    pub(super) fn create_ec2_volume(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let mut params = HashMap::new();
        for field in [
            "AvailabilityZone",
            "Size",
            "VolumeType",
            "Iops",
            "Throughput",
            "Encrypted",
            "KmsKeyId",
            "SnapshotId",
            "MultiAttachEnabled",
            "OutpostArn",
        ] {
            if let Some(v) = props.get(field).and_then(cfn_scalar) {
                params.insert(field.to_string(), v);
            }
        }
        self.ec2_tag_params(props, "volume", &mut params);
        let body = self.ec2_dispatch("CreateVolume", params)?;
        let id = xml_elem(&body, "volumeId").ok_or("CreateVolume returned no volumeId")?;
        Ok(ProvisionResult::new(id.clone()).with("VolumeId", id))
    }

    pub(super) fn delete_ec2_resource(
        &self,
        resource_type: &str,
        physical_id: &str,
    ) -> Result<(), String> {
        let (action, id_param) = match resource_type {
            "AWS::EC2::VPC" => ("DeleteVpc", "VpcId"),
            "AWS::EC2::Subnet" => ("DeleteSubnet", "SubnetId"),
            "AWS::EC2::SecurityGroup" => ("DeleteSecurityGroup", "GroupId"),
            "AWS::EC2::InternetGateway" => ("DeleteInternetGateway", "InternetGatewayId"),
            "AWS::EC2::RouteTable" => ("DeleteRouteTable", "RouteTableId"),
            "AWS::EC2::Volume" => ("DeleteVolume", "VolumeId"),
            "AWS::EC2::LaunchTemplate" => ("DeleteLaunchTemplate", "LaunchTemplateId"),
            _ => return Ok(()),
        };
        let mut params = HashMap::new();
        params.insert(id_param.to_string(), physical_id.to_string());
        // A delete of an already-gone resource is not a stack failure.
        let _ = self.ec2_dispatch(action, params);
        Ok(())
    }

    /// `Fn::GetAtt` for EC2 resources. The id-style attributes are the physical
    /// id; the rest were eagerly captured at create time.
    pub(super) fn get_att_ec2(&self, resource: &StackResource, attribute: &str) -> Option<String> {
        match (resource.resource_type.as_str(), attribute) {
            ("AWS::EC2::VPC", "VpcId")
            | ("AWS::EC2::Subnet", "SubnetId")
            | ("AWS::EC2::SecurityGroup", "GroupId")
            | ("AWS::EC2::SecurityGroup", "Id")
            | ("AWS::EC2::InternetGateway", "InternetGatewayId")
            | ("AWS::EC2::RouteTable", "RouteTableId")
            | ("AWS::EC2::Volume", "VolumeId")
            | ("AWS::EC2::LaunchTemplate", "LaunchTemplateId") => {
                Some(resource.physical_id.clone())
            }
            _ => resource.attributes.get(attribute).cloned(),
        }
    }
}

/// `IamInstanceProfile` as CloudFormation writes it: either a bare string
/// (profile name, or an ARN) or an object with `Arn` / `Name`. Returns
/// `(arn, name)`.
fn cfn_iam_instance_profile(props: &Value) -> (Option<String>, Option<String>) {
    match props.get("IamInstanceProfile") {
        Some(Value::String(s)) => {
            // A bare string is the profile name (or an ARN); classify by prefix
            // so both round-trip.
            if s.starts_with("arn:") {
                (Some(s.clone()), None)
            } else {
                (None, Some(s.clone()))
            }
        }
        Some(Value::Object(o)) => (
            o.get("Arn").and_then(|v| v.as_str()).map(String::from),
            o.get("Name").and_then(|v| v.as_str()).map(String::from),
        ),
        _ => (None, None),
    }
}

/// Reject an `IamInstanceProfile` the EC2 handlers would reject, so a bad
/// template value fails the stack operation rather than being stored.
fn validate_cfn_iam_instance_profile(
    arn: &Option<String>,
    name: &Option<String>,
) -> Result<(), String> {
    if let Some(arn) = arn {
        if !fakecloud_ec2::service_helpers::is_instance_profile_arn(arn) {
            return Err(format!("The IAM instance profile ARN '{arn}' is malformed"));
        }
    }
    if let Some(name) = name {
        if !fakecloud_ec2::service_helpers::is_instance_profile_name(name) {
            return Err(format!("Invalid IAM Instance Profile name: {name}"));
        }
    }
    Ok(())
}
