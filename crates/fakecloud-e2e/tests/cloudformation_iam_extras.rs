//! CloudFormation provisioner for AWS::IAM extras: OIDCProvider,
//! SAMLProvider, ServiceLinkedRole, VirtualMFADevice. Complements the
//! existing User/Group/Role/ManagedPolicy/AccessKey/InstanceProfile
//! coverage.

mod helpers;

use aws_sdk_cloudformation::types::{Capability, OnFailure};
use helpers::TestServer;

const TEMPLATE: &str = r#"{
  "AWSTemplateFormatVersion": "2010-09-09",
  "Resources": {
    "OIDC": {
      "Type": "AWS::IAM::OIDCProvider",
      "Properties": {
        "Url": "https://accounts.example.com",
        "ClientIdList": ["my-app", "another-app"],
        "ThumbprintList": ["aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"]
      }
    },
    "SAML": {
      "Type": "AWS::IAM::SAMLProvider",
      "Properties": {
        "Name": "cfn-saml",
        "SamlMetadataDocument": "<EntityDescriptor xmlns=\"urn:oasis:names:tc:SAML:2.0:metadata\"/>"
      }
    },
    "SLR": {
      "Type": "AWS::IAM::ServiceLinkedRole",
      "Properties": {
        "AWSServiceName": "elasticbeanstalk.amazonaws.com",
        "Description": "managed by cfn"
      }
    },
    "VMFA": {
      "Type": "AWS::IAM::VirtualMFADevice",
      "Properties": {
        "VirtualMfaDeviceName": "cfn-mfa-device",
        "Path": "/"
      }
    }
  },
  "Outputs": {
    "OIDCArn": {"Value": {"Ref": "OIDC"}},
    "SAMLArn": {"Value": {"Ref": "SAML"}},
    "SLRName": {"Value": {"Ref": "SLR"}},
    "VMFASerial": {"Value": {"Ref": "VMFA"}}
  }
}"#;

#[tokio::test]
async fn cfn_provisions_iam_extras() {
    let server = TestServer::start().await;
    let cfn = server.cloudformation_client().await;
    let iam = aws_sdk_iam::Client::new(&server.aws_config().await);

    cfn.create_stack()
        .stack_name("iam-extras-stack")
        .template_body(TEMPLATE)
        .capabilities(Capability::CapabilityIam)
        .on_failure(OnFailure::Rollback)
        .send()
        .await
        .expect("create_stack");

    let described = cfn
        .describe_stacks()
        .stack_name("iam-extras-stack")
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

    let oidc_arn = outputs.get("OIDCArn").expect("OIDCArn");
    assert!(oidc_arn.contains(":oidc-provider/accounts.example.com"));
    let saml_arn = outputs.get("SAMLArn").expect("SAMLArn");
    assert!(saml_arn.contains(":saml-provider/cfn-saml"));
    let slr_name = outputs.get("SLRName").expect("SLRName");
    assert!(slr_name.starts_with("AWSServiceRoleFor"));
    let vmfa_serial = outputs.get("VMFASerial").expect("VMFASerial");
    assert!(vmfa_serial.contains(":mfa/cfn-mfa-device"));

    // Verify SAML provider via SDK.
    let saml = iam
        .get_saml_provider()
        .saml_provider_arn(*saml_arn)
        .send()
        .await
        .expect("get_saml_provider");
    assert!(saml.saml_metadata_document().is_some());

    // Verify OIDC provider via SDK.
    let oidc = iam
        .get_open_id_connect_provider()
        .open_id_connect_provider_arn(*oidc_arn)
        .send()
        .await
        .expect("get_open_id_connect_provider");
    // Stored like CreateOpenIDConnectProvider stores it: without the scheme.
    assert_eq!(oidc.url(), Some("accounts.example.com"));
    assert!(oidc.client_id_list().contains(&"my-app".to_string()));

    // Verify service-linked role via SDK.
    let role = iam
        .get_role()
        .role_name(*slr_name)
        .send()
        .await
        .expect("get_role");
    assert_eq!(role.role().map(|r| r.role_name()), Some(*slr_name));

    cfn.delete_stack()
        .stack_name("iam-extras-stack")
        .send()
        .await
        .expect("delete_stack");
}
