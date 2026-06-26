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
        Ok(ProvisionResult::new(id.clone())
            .with("SubnetId", id)
            .with("AvailabilityZone", az))
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
