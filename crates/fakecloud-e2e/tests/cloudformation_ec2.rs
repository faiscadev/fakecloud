//! CloudFormation provisioning fidelity for AWS::EC2::* resources. The 1.10
//! provisioner created the VPC/Subnet/SecurityGroup but silently dropped inline
//! SecurityGroup ingress/egress rules, VPC DNS attributes, and subnet
//! MapPublicIpOnLaunch, and Fn::GetAtt on a Subnet's VpcId/CidrBlock returned
//! empty. This drives a template that exercises all of them through the real
//! aws-sdk EC2 client.

mod helpers;

use aws_sdk_cloudformation::types::Capability;
use helpers::TestServer;

const TEMPLATE: &str = r#"{
  "AWSTemplateFormatVersion": "2010-09-09",
  "Resources": {
    "Vpc": {
      "Type": "AWS::EC2::VPC",
      "Properties": {
        "CidrBlock": "10.20.0.0/16",
        "EnableDnsSupport": true,
        "EnableDnsHostnames": true,
        "Tags": [{"Key": "Name", "Value": "cfn-ec2-vpc"}]
      }
    },
    "Subnet": {
      "Type": "AWS::EC2::Subnet",
      "Properties": {
        "VpcId": {"Ref": "Vpc"},
        "CidrBlock": "10.20.1.0/24",
        "AvailabilityZone": "us-east-1a",
        "MapPublicIpOnLaunch": true
      }
    },
    "Sg": {
      "Type": "AWS::EC2::SecurityGroup",
      "Properties": {
        "GroupName": "cfn-ec2-sg",
        "GroupDescription": "managed by cfn",
        "VpcId": {"Ref": "Vpc"},
        "SecurityGroupIngress": [
          {"IpProtocol": "tcp", "FromPort": 443, "ToPort": 443, "CidrIp": "0.0.0.0/0"},
          {"IpProtocol": "tcp", "FromPort": 22, "ToPort": 22, "CidrIp": "10.0.0.0/8"}
        ]
      }
    }
  },
  "Outputs": {
    "SubnetVpcId": {"Value": {"Fn::GetAtt": ["Subnet", "VpcId"]}},
    "SubnetCidr": {"Value": {"Fn::GetAtt": ["Subnet", "CidrBlock"]}},
    "VpcId": {"Value": {"Ref": "Vpc"}},
    "SgId": {"Value": {"Fn::GetAtt": ["Sg", "GroupId"]}}
  }
}"#;

#[tokio::test]
async fn cfn_provisions_ec2_with_rules_and_attributes() {
    let server = TestServer::start().await;
    let cfn = server.cloudformation_client().await;
    let ec2 = server.ec2_client().await;

    cfn.create_stack()
        .stack_name("ec2-stack")
        .template_body(TEMPLATE)
        .capabilities(Capability::CapabilityIam)
        .send()
        .await
        .expect("create_stack");

    let described = cfn
        .describe_stacks()
        .stack_name("ec2-stack")
        .send()
        .await
        .expect("describe_stacks");
    let stack = described.stacks().first().expect("stack present");
    assert_eq!(stack.stack_status().unwrap().as_str(), "CREATE_COMPLETE");

    let outputs: std::collections::HashMap<&str, &str> = stack
        .outputs()
        .iter()
        .filter_map(|o| Some((o.output_key()?, o.output_value()?)))
        .collect();

    let vpc_id = *outputs.get("VpcId").expect("VpcId output");
    let sg_id = *outputs.get("SgId").expect("SgId output");

    // M3: Fn::GetAtt on the subnet resolves VpcId + CidrBlock (was empty).
    assert_eq!(
        *outputs.get("SubnetVpcId").expect("SubnetVpcId output"),
        vpc_id,
        "Subnet.VpcId GetAtt should equal the VPC id"
    );
    assert_eq!(
        *outputs.get("SubnetCidr").expect("SubnetCidr output"),
        "10.20.1.0/24"
    );

    // M2: inline SecurityGroupIngress rules were applied (not dropped).
    let sgs = ec2
        .describe_security_groups()
        .group_ids(sg_id)
        .send()
        .await
        .expect("describe_security_groups");
    let sg = sgs.security_groups().first().expect("sg present");
    let ports: Vec<i32> = sg
        .ip_permissions()
        .iter()
        .filter_map(|p| p.from_port())
        .collect();
    assert!(ports.contains(&443), "443 ingress missing: {ports:?}");
    assert!(ports.contains(&22), "22 ingress missing: {ports:?}");

    // L3: VPC DNS attributes were applied.
    let dns_hostnames = ec2
        .describe_vpc_attribute()
        .vpc_id(vpc_id)
        .attribute(aws_sdk_ec2::types::VpcAttributeName::EnableDnsHostnames)
        .send()
        .await
        .expect("describe_vpc_attribute");
    assert_eq!(
        dns_hostnames.enable_dns_hostnames().and_then(|a| a.value()),
        Some(true),
        "EnableDnsHostnames should be applied"
    );

    // L3: subnet MapPublicIpOnLaunch was applied.
    let subnets = ec2
        .describe_subnets()
        .filters(
            aws_sdk_ec2::types::Filter::builder()
                .name("vpc-id")
                .values(vpc_id)
                .build(),
        )
        .send()
        .await
        .expect("describe_subnets");
    let subnet = subnets.subnets().first().expect("subnet present");
    assert_eq!(
        subnet.map_public_ip_on_launch(),
        Some(true),
        "MapPublicIpOnLaunch should be applied"
    );

    cfn.delete_stack()
        .stack_name("ec2-stack")
        .send()
        .await
        .expect("delete_stack");
}
