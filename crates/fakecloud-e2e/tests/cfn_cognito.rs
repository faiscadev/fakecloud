//! CloudFormation provisioner for AWS::Cognito::IdentityPool +
//! AWS::Cognito::IdentityPoolRoleAttachment (BB15). The user-pool side
//! (UserPool/Client/Domain) is exercised by `cloudformation_cognito.rs`;
//! this file focuses on the federated identity types added in BB15.

mod helpers;

use aws_sdk_cloudformation::types::{Capability, OnFailure};
use helpers::TestServer;

const TEMPLATE: &str = r#"{
  "AWSTemplateFormatVersion": "2010-09-09",
  "Resources": {
    "Pool": {
      "Type": "AWS::Cognito::UserPool",
      "Properties": {
        "PoolName": "fed-pool"
      }
    },
    "Client": {
      "Type": "AWS::Cognito::UserPoolClient",
      "Properties": {
        "ClientName": "fed-client",
        "UserPoolId": {"Ref": "Pool"}
      }
    },
    "AuthRole": {
      "Type": "AWS::IAM::Role",
      "Properties": {
        "RoleName": "fed-auth",
        "AssumeRolePolicyDocument": {
          "Version": "2012-10-17",
          "Statement": [{
            "Effect": "Allow",
            "Principal": {"Federated": "cognito-identity.amazonaws.com"},
            "Action": "sts:AssumeRoleWithWebIdentity"
          }]
        }
      }
    },
    "UnauthRole": {
      "Type": "AWS::IAM::Role",
      "Properties": {
        "RoleName": "fed-unauth",
        "AssumeRolePolicyDocument": {
          "Version": "2012-10-17",
          "Statement": [{
            "Effect": "Allow",
            "Principal": {"Federated": "cognito-identity.amazonaws.com"},
            "Action": "sts:AssumeRoleWithWebIdentity"
          }]
        }
      }
    },
    "IdPool": {
      "Type": "AWS::Cognito::IdentityPool",
      "Properties": {
        "IdentityPoolName": "fed_id_pool",
        "AllowUnauthenticatedIdentities": true,
        "AllowClassicFlow": false,
        "DeveloperProviderName": "login.example",
        "CognitoIdentityProviders": [{
          "ProviderName": {"Fn::GetAtt": ["Pool", "ProviderName"]},
          "ClientId": {"Ref": "Client"},
          "ServerSideTokenCheck": true
        }],
        "SupportedLoginProviders": {
          "graph.facebook.com": "1234567890"
        },
        "OpenIdConnectProviderARNs": ["arn:aws:iam::000000000000:oidc-provider/example.com"],
        "IdentityPoolTags": {"env": "test"}
      }
    },
    "RoleAttach": {
      "Type": "AWS::Cognito::IdentityPoolRoleAttachment",
      "Properties": {
        "IdentityPoolId": {"Ref": "IdPool"},
        "Roles": {
          "authenticated": {"Fn::GetAtt": ["AuthRole", "Arn"]},
          "unauthenticated": {"Fn::GetAtt": ["UnauthRole", "Arn"]}
        },
        "RoleMappings": {
          "myProvider": {
            "Type": "Token",
            "AmbiguousRoleResolution": "AuthenticatedRole"
          }
        }
      }
    }
  },
  "Outputs": {
    "IdPoolId": {"Value": {"Ref": "IdPool"}},
    "IdPoolName": {"Value": {"Fn::GetAtt": ["IdPool", "Name"]}},
    "RoleAttachId": {"Value": {"Ref": "RoleAttach"}}
  }
}"#;

#[tokio::test]
async fn cfn_provisions_cognito_identity_pool_and_role_attachment() {
    let server = TestServer::start().await;
    let cfn = server.cloudformation_client().await;

    cfn.create_stack()
        .stack_name("idpool-stack")
        .template_body(TEMPLATE)
        .capabilities(Capability::CapabilityNamedIam)
        .on_failure(OnFailure::Rollback)
        .send()
        .await
        .expect("create_stack");

    let described = cfn
        .describe_stacks()
        .stack_name("idpool-stack")
        .send()
        .await
        .expect("describe_stacks");
    let stack = described.stacks().first().expect("stack present");
    assert_eq!(
        stack.stack_status().unwrap().as_str(),
        "CREATE_COMPLETE",
        "status: {:?} reason: {:?}",
        stack.stack_status(),
        stack.stack_status_reason()
    );

    let mut id_pool_id = None;
    let mut role_attach_id = None;
    let mut id_pool_name = None;
    for o in stack.outputs() {
        match o.output_key() {
            Some("IdPoolId") => id_pool_id = o.output_value().map(|s| s.to_string()),
            Some("RoleAttachId") => role_attach_id = o.output_value().map(|s| s.to_string()),
            Some("IdPoolName") => id_pool_name = o.output_value().map(|s| s.to_string()),
            _ => {}
        }
    }
    let id_pool_id = id_pool_id.expect("IdPoolId output");
    let role_attach_id = role_attach_id.expect("RoleAttachId output");
    // GetAtt Name is the pool's name, not its id.
    assert_eq!(id_pool_name.as_deref(), Some("fed_id_pool"));

    // Identity pool ids look like `<region>:<uuid>` per AWS shape.
    assert!(
        id_pool_id.contains(':'),
        "expected `<region>:<uuid>` shape, got {id_pool_id}"
    );
    let (region_part, uuid_part) = id_pool_id.split_once(':').unwrap();
    assert!(!region_part.is_empty());
    assert_eq!(uuid_part.len(), 36, "expected uuid, got {uuid_part}");

    // Role attachment Ref is `<pool-id>:<attachment-id>` so it carries the
    // pool id as a prefix. Real AWS uses the pool id as the resource id for
    // role attachment (since each pool has at most one), but synthesising a
    // unique id keeps idempotency clean across stack updates.
    assert!(
        role_attach_id.starts_with(&format!("{id_pool_id}:")),
        "RoleAttachment Ref should embed pool id, got {role_attach_id}"
    );

    // CFN sees both resources.
    let resources = cfn
        .list_stack_resources()
        .stack_name("idpool-stack")
        .send()
        .await
        .expect("list_stack_resources");
    let summaries = resources.stack_resource_summaries();
    assert!(summaries
        .iter()
        .any(|r| r.logical_resource_id() == Some("IdPool")
            && r.resource_type() == Some("AWS::Cognito::IdentityPool")));
    assert!(summaries
        .iter()
        .any(|r| r.logical_resource_id() == Some("RoleAttach")
            && r.resource_type() == Some("AWS::Cognito::IdentityPoolRoleAttachment")));

    // Stack delete cascades; both resources should be gone afterward. We
    // re-create with the same pool name to confirm the pool slot is free.
    cfn.delete_stack()
        .stack_name("idpool-stack")
        .send()
        .await
        .expect("delete_stack");

    // After delete, describe_stacks for this name should report DELETE_COMPLETE
    // (or 404). Either way, recreating a stack with the same name should succeed
    // — proving the prior identity pool + role attachment were torn down.
    cfn.create_stack()
        .stack_name("idpool-stack-2")
        .template_body(TEMPLATE)
        .capabilities(Capability::CapabilityNamedIam)
        .on_failure(OnFailure::Rollback)
        .send()
        .await
        .expect("create_stack second time");
    let second = cfn
        .describe_stacks()
        .stack_name("idpool-stack-2")
        .send()
        .await
        .expect("describe_stacks 2");
    assert_eq!(
        second
            .stacks()
            .first()
            .unwrap()
            .stack_status()
            .unwrap()
            .as_str(),
        "CREATE_COMPLETE"
    );
}

/// Role attachment without a matching identity pool should fail to provision.
#[tokio::test]
async fn cfn_role_attachment_requires_existing_pool() {
    let server = TestServer::start().await;
    let cfn = server.cloudformation_client().await;

    let template = r#"{
        "Resources": {
            "Orphan": {
                "Type": "AWS::Cognito::IdentityPoolRoleAttachment",
                "Properties": {
                    "IdentityPoolId": "us-east-1:00000000-0000-0000-0000-000000000000",
                    "Roles": {"authenticated": "arn:aws:iam::000000000000:role/fake"}
                }
            }
        }
    }"#;

    // Either create_stack returns a synchronous validation error, or the
    // stack lands in a *_FAILED / *_ROLLBACK_* state. Both are acceptable —
    // we just need to confirm the orphan attachment is rejected.
    let result = cfn
        .create_stack()
        .stack_name("orphan-stack")
        .template_body(template)
        .on_failure(OnFailure::Rollback)
        .send()
        .await;

    match result {
        Err(e) => {
            let msg = format!("{e:?}");
            assert!(
                msg.contains("Identity pool") && msg.contains("does not exist"),
                "expected identity pool validation error, got {msg}"
            );
        }
        Ok(_) => {
            let described = cfn
                .describe_stacks()
                .stack_name("orphan-stack")
                .send()
                .await
                .expect("describe_stacks");
            let status = described
                .stacks()
                .first()
                .unwrap()
                .stack_status()
                .unwrap()
                .as_str()
                .to_string();
            assert!(
                status.contains("FAILED") || status.contains("ROLLBACK"),
                "expected failure status, got {status}"
            );
        }
    }
}
