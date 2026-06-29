//! CloudFormation provisioner fixes:
//! - An RDS DBInstance with no Port defaults the port from the engine
//!   (MySQL -> 3306) via Fn::GetAtt Endpoint.Port, not a hardcoded 5432.
//! - Fn::GetAtt DbiResourceId matches the value DescribeDBInstances returns
//!   (previously the GetAtt refabricated `db-<identifier>` while the record
//!   stored a real `db-<uuid>`).
//! - AWS::EC2::Instance provisions a REAL instance: Ref resolves to the
//!   `i-...` id and DescribeInstances finds it (previously it fell to the
//!   accept-and-ignore catch-all and Ref returned the bare logical id).

mod helpers;

use aws_sdk_cloudformation::types::Capability;
use helpers::TestServer;

const TEMPLATE: &str = r#"{
  "AWSTemplateFormatVersion": "2010-09-09",
  "Resources": {
    "Db": {
      "Type": "AWS::RDS::DBInstance",
      "Properties": {
        "DBInstanceIdentifier": "mysql-noport",
        "DBInstanceClass": "db.t4g.micro",
        "Engine": "mysql",
        "EngineVersion": "8.0",
        "MasterUsername": "admin",
        "MasterUserPassword": "hunter2-secret",
        "AllocatedStorage": "20"
      }
    },
    "Instance": {
      "Type": "AWS::EC2::Instance",
      "Properties": {
        "ImageId": "ami-0123456789abcdef0",
        "InstanceType": "t3.micro"
      }
    }
  },
  "Outputs": {
    "DbPort": {"Value": {"Fn::GetAtt": ["Db", "Endpoint.Port"]}},
    "DbResourceId": {"Value": {"Fn::GetAtt": ["Db", "DbiResourceId"]}},
    "InstanceRef": {"Value": {"Ref": "Instance"}},
    "InstanceAz": {"Value": {"Fn::GetAtt": ["Instance", "AvailabilityZone"]}}
  }
}"#;

fn output<'a>(stack: &'a aws_sdk_cloudformation::types::Stack, key: &str) -> &'a str {
    stack
        .outputs()
        .iter()
        .find(|o| o.output_key() == Some(key))
        .and_then(|o| o.output_value())
        .unwrap_or_else(|| panic!("output {key} present"))
}

#[tokio::test]
async fn cfn_rds_engine_port_and_real_ec2_instance() {
    let server = TestServer::start().await;
    let cfn = server.cloudformation_client().await;
    let rds = server.rds_client().await;
    let ec2 = server.ec2_client().await;

    cfn.create_stack()
        .stack_name("port-ec2-stack")
        .template_body(TEMPLATE)
        .capabilities(Capability::CapabilityIam)
        .send()
        .await
        .expect("create_stack");

    let described = cfn
        .describe_stacks()
        .stack_name("port-ec2-stack")
        .send()
        .await
        .expect("describe_stacks");
    let stack = described.stacks().first().expect("stack present");
    assert_eq!(stack.stack_status().unwrap().as_str(), "CREATE_COMPLETE");

    // MySQL with no Port -> 3306, not 5432.
    assert_eq!(output(stack, "DbPort"), "3306");

    // DbiResourceId GetAtt matches DescribeDBInstances.
    let getatt_resource_id = output(stack, "DbResourceId");
    let inst = rds
        .describe_db_instances()
        .db_instance_identifier("mysql-noport")
        .send()
        .await
        .expect("describe_db_instances");
    let db = inst.db_instances().first().expect("db present");
    assert_eq!(db.dbi_resource_id(), Some(getatt_resource_id));
    assert!(
        getatt_resource_id.starts_with("db-"),
        "resource id should be db-<uuid>: {getatt_resource_id}"
    );
    // Stored DescribeDBInstances port also reflects the engine default.
    assert_eq!(db.endpoint().and_then(|e| e.port()), Some(3306));

    // EC2::Instance Ref resolves to a real i- id and DescribeInstances finds it.
    let instance_ref = output(stack, "InstanceRef");
    assert!(
        instance_ref.starts_with("i-"),
        "Ref should resolve to an instance id: {instance_ref}"
    );
    assert!(!output(stack, "InstanceAz").is_empty(), "AZ GetAtt present");

    let di = ec2
        .describe_instances()
        .instance_ids(instance_ref)
        .send()
        .await
        .expect("describe_instances");
    let found = di
        .reservations()
        .iter()
        .flat_map(|r| r.instances())
        .any(|i| i.instance_id() == Some(instance_ref));
    assert!(
        found,
        "CFN-created instance {instance_ref} must be described"
    );
}
