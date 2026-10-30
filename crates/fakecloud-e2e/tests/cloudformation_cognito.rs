//! CloudFormation provisioner for AWS::Cognito::UserPool + UserPoolClient + UserPoolDomain.

mod helpers;

use aws_sdk_cloudformation::types::{Capability, OnFailure};
use helpers::TestServer;

const TEMPLATE: &str = r#"{
  "AWSTemplateFormatVersion": "2010-09-09",
  "Resources": {
    "Pool": {
      "Type": "AWS::Cognito::UserPool",
      "Properties": {
        "PoolName": "cfn-pool",
        "AutoVerifiedAttributes": ["email"],
        "Policies": {
          "PasswordPolicy": {
            "MinimumLength": 12,
            "RequireUppercase": true,
            "RequireLowercase": true,
            "RequireNumbers": true,
            "RequireSymbols": false
          }
        },
        "MfaConfiguration": "OFF",
        "AccountRecoverySetting": {
          "RecoveryMechanisms": [
            {"Name": "verified_email", "Priority": 1}
          ]
        }
      }
    },
    "Client": {
      "Type": "AWS::Cognito::UserPoolClient",
      "Properties": {
        "ClientName": "cfn-client",
        "UserPoolId": {"Ref": "Pool"},
        "GenerateSecret": true,
        "ExplicitAuthFlows": ["ALLOW_USER_PASSWORD_AUTH", "ALLOW_REFRESH_TOKEN_AUTH"],
        "CallbackURLs": ["https://example.com/cb"]
      }
    },
    "Domain": {
      "Type": "AWS::Cognito::UserPoolDomain",
      "Properties": {
        "Domain": "cfn-cognito-test",
        "UserPoolId": {"Ref": "Pool"}
      }
    }
  },
  "Outputs": {
    "PoolId": {"Value": {"Ref": "Pool"}},
    "PoolArn": {"Value": {"Fn::GetAtt": ["Pool", "Arn"]}},
    "ProviderName": {"Value": {"Fn::GetAtt": ["Pool", "ProviderName"]}},
    "ClientId": {"Value": {"Ref": "Client"}},
    "ClientSecret": {"Value": {"Fn::GetAtt": ["Client", "ClientSecret"]}},
    "DomainName": {"Value": {"Ref": "Domain"}},
    "CloudFrontDist": {"Value": {"Fn::GetAtt": ["Domain", "CloudFrontDistribution"]}}
  }
}"#;

#[tokio::test]
async fn cfn_provisions_cognito_user_pool_client_domain() {
    let server = TestServer::start().await;
    let cfn = server.cloudformation_client().await;
    let cognito = aws_sdk_cognitoidentityprovider::Client::new(&server.aws_config().await);

    cfn.create_stack()
        .stack_name("cognito-stack")
        .template_body(TEMPLATE)
        .capabilities(Capability::CapabilityIam)
        .on_failure(OnFailure::Rollback)
        .send()
        .await
        .expect("create_stack");

    let described = cfn
        .describe_stacks()
        .stack_name("cognito-stack")
        .send()
        .await
        .expect("describe_stacks");
    let stack = described.stacks().first().expect("stack present");
    assert_eq!(stack.stack_status().unwrap().as_str(), "CREATE_COMPLETE");

    let mut pool_id = None;
    let mut pool_arn = None;
    let mut provider_name = None;
    let mut client_id = None;
    let mut client_secret = None;
    let mut domain_name = None;
    let mut cf_dist = None;
    for o in stack.outputs() {
        match o.output_key() {
            Some("PoolId") => pool_id = o.output_value().map(|s| s.to_string()),
            Some("PoolArn") => pool_arn = o.output_value().map(|s| s.to_string()),
            Some("ProviderName") => provider_name = o.output_value().map(|s| s.to_string()),
            Some("ClientId") => client_id = o.output_value().map(|s| s.to_string()),
            Some("ClientSecret") => client_secret = o.output_value().map(|s| s.to_string()),
            Some("DomainName") => domain_name = o.output_value().map(|s| s.to_string()),
            Some("CloudFrontDist") => cf_dist = o.output_value().map(|s| s.to_string()),
            _ => {}
        }
    }
    let pool_id = pool_id.expect("PoolId");
    let pool_arn = pool_arn.expect("PoolArn");
    let provider_name = provider_name.expect("ProviderName");
    let client_id = client_id.expect("ClientId");
    let client_secret = client_secret.expect("ClientSecret");
    let domain_name = domain_name.expect("DomainName");
    let cf_dist = cf_dist.expect("CloudFrontDist");

    assert!(pool_arn.contains(":userpool/"));
    assert!(provider_name.starts_with("cognito-idp."));
    assert!(client_id.len() == 26, "client_id len: {}", client_id.len());
    assert!(!client_secret.is_empty());
    assert_eq!(domain_name, "cfn-cognito-test");
    assert!(cf_dist.contains("cfn-cognito-test"));

    // Pool-level checks via Cognito SDK.
    let described_pool = cognito
        .describe_user_pool()
        .user_pool_id(&pool_id)
        .send()
        .await
        .expect("describe_user_pool");
    let pool = described_pool.user_pool().expect("pool");
    assert_eq!(pool.name(), Some("cfn-pool"));
    assert_eq!(
        pool.policies()
            .and_then(|p| p.password_policy())
            .and_then(|pp| pp.minimum_length()),
        Some(12)
    );

    // Client-level checks.
    let described_client = cognito
        .describe_user_pool_client()
        .user_pool_id(&pool_id)
        .client_id(&client_id)
        .send()
        .await
        .expect("describe_user_pool_client");
    let client = described_client.user_pool_client().expect("client");
    assert_eq!(client.client_name(), Some("cfn-client"));
    assert!(!client.callback_urls().is_empty());

    // Domain check.
    let described_domain = cognito
        .describe_user_pool_domain()
        .domain(&domain_name)
        .send()
        .await
        .expect("describe_user_pool_domain");
    let domain = described_domain.domain_description().expect("domain");
    assert_eq!(domain.user_pool_id(), Some(pool_id.as_str()));

    // Stack delete cascades pool + client + domain removal.
    cfn.delete_stack()
        .stack_name("cognito-stack")
        .send()
        .await
        .expect("delete_stack");

    let after_pool = cognito
        .describe_user_pool()
        .user_pool_id(&pool_id)
        .send()
        .await;
    assert!(
        after_pool.is_err(),
        "pool should be gone after stack deletion"
    );
}

fn rotation_template(client_name: &str) -> String {
    format!(
        r#"{{
  "AWSTemplateFormatVersion": "2010-09-09",
  "Resources": {{
    "Pool": {{
      "Type": "AWS::Cognito::UserPool",
      "Properties": {{"PoolName": "cfn-rotation-pool"}}
    }},
    "Client": {{
      "Type": "AWS::Cognito::UserPoolClient",
      "Properties": {{
        "ClientName": "{client_name}",
        "UserPoolId": {{"Ref": "Pool"}},
        "GenerateSecret": true
      }}
    }}
  }},
  "Outputs": {{
    "PoolId": {{"Value": {{"Ref": "Pool"}}}},
    "ClientId": {{"Value": {{"Ref": "Client"}}}},
    "ClientName": {{"Value": {{"Fn::GetAtt": ["Client", "Name"]}}}},
    "ClientSecret": {{"Value": {{"Fn::GetAtt": ["Client", "ClientSecret"]}}}}
  }}
}}"#
    )
}

async fn stack_outputs(
    cfn: &aws_sdk_cloudformation::Client,
    stack: &str,
) -> std::collections::HashMap<String, String> {
    let described = cfn
        .describe_stacks()
        .stack_name(stack)
        .send()
        .await
        .expect("describe_stacks");
    described.stacks()[0]
        .outputs()
        .iter()
        .filter_map(|o| Some((o.output_key()?.to_string(), o.output_value()?.to_string())))
        .collect()
}

/// Rotating the client secret through the Cognito API (add a second secret,
/// delete the original primary) and then updating the client through the
/// stack refreshes GetAtt ClientSecret to the surviving secret, and GetAtt
/// Name follows the rename.
#[tokio::test]
async fn cfn_update_reports_rotated_client_secret() {
    let server = TestServer::start().await;
    let cfn = server.cloudformation_client().await;
    let cognito = aws_sdk_cognitoidentityprovider::Client::new(&server.aws_config().await);

    cfn.create_stack()
        .stack_name("rotation-stack")
        .template_body(rotation_template("rotation-client"))
        .send()
        .await
        .expect("create_stack");
    let outputs = stack_outputs(&cfn, "rotation-stack").await;
    let pool_id = outputs["PoolId"].clone();
    let client_id = outputs["ClientId"].clone();
    let original = outputs["ClientSecret"].clone();
    assert_eq!(outputs["ClientName"], "rotation-client");

    let added = cognito
        .add_user_pool_client_secret()
        .user_pool_id(&pool_id)
        .client_id(&client_id)
        .send()
        .await
        .expect("add_user_pool_client_secret");
    let added = added.client_secret_descriptor().expect("descriptor");
    let added_value = added.client_secret_value().expect("generated value");
    assert_ne!(added_value, original);
    let primary_id = cognito
        .list_user_pool_client_secrets()
        .user_pool_id(&pool_id)
        .client_id(&client_id)
        .send()
        .await
        .expect("list_user_pool_client_secrets")
        .client_secrets()[0]
        .client_secret_id()
        .expect("primary id")
        .to_string();
    cognito
        .delete_user_pool_client_secret()
        .user_pool_id(&pool_id)
        .client_id(&client_id)
        .client_secret_id(&primary_id)
        .send()
        .await
        .expect("delete_user_pool_client_secret");

    cfn.update_stack()
        .stack_name("rotation-stack")
        .template_body(rotation_template("rotation-client-renamed"))
        .send()
        .await
        .expect("update_stack");
    let status = cfn
        .describe_stacks()
        .stack_name("rotation-stack")
        .send()
        .await
        .expect("describe_stacks")
        .stacks()[0]
        .stack_status()
        .map(|s| s.as_str().to_string());
    assert_eq!(status.as_deref(), Some("UPDATE_COMPLETE"));
    let outputs = stack_outputs(&cfn, "rotation-stack").await;
    assert_eq!(outputs["ClientId"], client_id);
    assert_eq!(outputs["ClientSecret"], added_value);
    assert_eq!(outputs["ClientName"], "rotation-client-renamed");
}
