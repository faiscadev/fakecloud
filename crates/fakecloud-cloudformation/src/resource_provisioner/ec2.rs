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
    fn ec2_dispatch(
        &self,
        action: &str,
        params: HashMap<String, String>,
    ) -> Result<String, String> {
        let svc = Ec2Service::with_state(self.ec2_state.clone());
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
        let name = prop_str(props, "GroupName").unwrap_or(&resource.logical_id);
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
        if let Some(rules) = props.get("SecurityGroupIngress").and_then(|v| v.as_array()) {
            if !rules.is_empty() {
                self.ec2_dispatch("AuthorizeSecurityGroupIngress", sg_rule_params(&id, rules))?;
            }
        }
        if let Some(rules) = props.get("SecurityGroupEgress").and_then(|v| v.as_array()) {
            if !rules.is_empty() {
                self.ec2_dispatch("AuthorizeSecurityGroupEgress", sg_rule_params(&id, rules))?;
            }
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
        {
            let mut accounts = self.ec2_state.write();
            let state = accounts.get_or_create(&self.account_id);
            if let Some(sg) = state.security_groups.get_mut(group_id) {
                sg.rules.retain(|r| r.is_egress != is_egress);
            }
        }
        if !rules.is_empty() {
            let action = if is_egress {
                "AuthorizeSecurityGroupEgress"
            } else {
                "AuthorizeSecurityGroupIngress"
            };
            self.ec2_dispatch(action, sg_rule_params(group_id, rules))?;
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
        // MetadataOptions: only build one when the template supplies the block,
        // so an instance without it keeps the AWS defaults. Each field falls
        // back to the DescribeInstances-default when omitted.
        let metadata_options = props
            .get("MetadataOptions")
            .and_then(|v| v.as_object())
            .map(|m| {
                let default = fakecloud_ec2::state::MetadataOptions::default();
                let get_str = |key: &str, dflt: &str| {
                    m.get(key)
                        .and_then(|v| v.as_str())
                        .map(String::from)
                        .unwrap_or_else(|| dflt.to_string())
                };
                fakecloud_ec2::state::MetadataOptions {
                    http_tokens: get_str("HttpTokens", &default.http_tokens),
                    http_endpoint: get_str("HttpEndpoint", &default.http_endpoint),
                    http_put_response_hop_limit: m
                        .get("HttpPutResponseHopLimit")
                        .and_then(|v| v.as_i64())
                        .unwrap_or(default.http_put_response_hop_limit),
                    http_protocol_ipv6: get_str("HttpProtocolIpv6", &default.http_protocol_ipv6),
                    instance_metadata_tags: get_str(
                        "InstanceMetadataTags",
                        &default.instance_metadata_tags,
                    ),
                }
            });

        // IamInstanceProfile can be a string (profile name / ARN) or an object
        // with `Arn` / `Name` (the CFN property shape).
        let iam_profile = props.get("IamInstanceProfile");
        let (iam_instance_profile_arn, iam_instance_profile_name) = match iam_profile {
            Some(serde_json::Value::String(s)) => {
                // A bare string is the profile name (or an ARN); classify by
                // prefix so both round-trip.
                if s.starts_with("arn:") {
                    (Some(s.clone()), None)
                } else {
                    (None, Some(s.clone()))
                }
            }
            Some(serde_json::Value::Object(o)) => (
                o.get("Arn").and_then(|v| v.as_str()).map(String::from),
                o.get("Name").and_then(|v| v.as_str()).map(String::from),
            ),
            _ => (None, None),
        };

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
            ebs_optimized: prop_bool(props, "EbsOptimized").unwrap_or(false),
            monitoring: prop_bool(props, "Monitoring").unwrap_or(false),
            iam_instance_profile_arn,
            iam_instance_profile_name,
        };

        let attrs = fakecloud_ec2::cfn_provision::cfn_create(
            self.ec2_state.clone(),
            &self.account_id,
            &self.region,
            &spec,
        );

        // Apply the template's Tags to the created instance (mirrors a direct
        // CreateTags) so DescribeInstances / cost reports reflect them.
        if let Some(tags) = props.get("Tags").and_then(|v| v.as_array()) {
            if !tags.is_empty() {
                let mut params = HashMap::new();
                params.insert("ResourceId.1".to_string(), attrs.instance_id.clone());
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
                let _ = self.ec2_dispatch("CreateTags", params);
            }
        }

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

        // The identity attributes (private ip, AZ, public ip) do not change on
        // an in-place modify; carry the ones captured at create time forward.
        Ok(ProvisionResult::new(instance_id).merge_attributes(existing.attributes.clone()))
    }

    /// Delete an EC2 resource by its physical id, routing through the real
    /// handler so dependent default resources are cleaned up correctly.
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
            | ("AWS::EC2::RouteTable", "RouteTableId") => Some(resource.physical_id.clone()),
            _ => resource.attributes.get(attribute).cloned(),
        }
    }
}
