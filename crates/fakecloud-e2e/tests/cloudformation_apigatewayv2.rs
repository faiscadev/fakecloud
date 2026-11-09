//! CloudFormation provisioner for AWS::ApiGatewayV2::* (BB10). Provisions
//! an HTTP API + Integration + Route + Stage + Authorizer (JWT) and
//! asserts via the real apigatewayv2 SDK.

mod helpers;

use aws_sdk_cloudformation::types::{Capability, OnFailure};
use helpers::TestServer;

const TEMPLATE: &str = r#"{
  "AWSTemplateFormatVersion": "2010-09-09",
  "Resources": {
    "Api": {
      "Type": "AWS::ApiGatewayV2::Api",
      "Properties": {
        "Name": "cfn-http-api",
        "ProtocolType": "HTTP",
        "Description": "from cfn",
        "CorsConfiguration": {
          "AllowOrigins": ["https://example.com"],
          "AllowMethods": ["GET","POST"]
        }
      }
    },
    "Integ": {
      "Type": "AWS::ApiGatewayV2::Integration",
      "Properties": {
        "ApiId": {"Ref": "Api"},
        "IntegrationType": "HTTP_PROXY",
        "IntegrationUri": "https://example.com/upstream",
        "PayloadFormatVersion": "2.0"
      }
    },
    "GetPets": {
      "Type": "AWS::ApiGatewayV2::Route",
      "Properties": {
        "ApiId": {"Ref": "Api"},
        "RouteKey": "GET /pets",
        "Target": {"Fn::Join": ["/", ["integrations", {"Ref": "Integ"}]]},
        "AuthorizationType": "NONE"
      }
    },
    "Deploy": {
      "Type": "AWS::ApiGatewayV2::Deployment",
      "DependsOn": "GetPets",
      "Properties": {
        "ApiId": {"Ref": "Api"},
        "Description": "first"
      }
    },
    "ProdStage": {
      "Type": "AWS::ApiGatewayV2::Stage",
      "Properties": {
        "ApiId": {"Ref": "Api"},
        "StageName": "prod",
        "DeploymentId": {"Ref": "Deploy"},
        "AutoDeploy": false
      }
    },
    "JwtAuth": {
      "Type": "AWS::ApiGatewayV2::Authorizer",
      "Properties": {
        "ApiId": {"Ref": "Api"},
        "Name": "jwt-auth",
        "AuthorizerType": "JWT",
        "IdentitySource": ["$request.header.Authorization"],
        "JwtConfiguration": {
          "Audience": ["my-aud"],
          "Issuer": "https://issuer.example.com"
        }
      }
    }
  },
  "Outputs": {
    "ApiId": {"Value": {"Ref": "Api"}},
    "IntegId": {"Value": {"Ref": "Integ"}},
    "RouteId": {"Value": {"Ref": "GetPets"}},
    "DeployId": {"Value": {"Ref": "Deploy"}},
    "AuthorizerId": {"Value": {"Ref": "JwtAuth"}}
  }
}"#;

#[tokio::test]
async fn cfn_provisions_apigatewayv2() {
    let server = TestServer::start().await;
    let cfn = server.cloudformation_client().await;
    let agw = aws_sdk_apigatewayv2::Client::new(&server.aws_config().await);

    cfn.create_stack()
        .stack_name("apigwv2-stack")
        .template_body(TEMPLATE)
        .capabilities(Capability::CapabilityIam)
        .on_failure(OnFailure::Rollback)
        .send()
        .await
        .expect("create_stack");

    let described = cfn
        .describe_stacks()
        .stack_name("apigwv2-stack")
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
    let api_id = outputs.get("ApiId").copied().expect("ApiId");
    let integ_id = outputs.get("IntegId").copied().expect("IntegId");
    let route_id = outputs.get("RouteId").copied().expect("RouteId");
    let deploy_id = outputs.get("DeployId").copied().expect("DeployId");
    let authz_id = outputs.get("AuthorizerId").copied().expect("AuthorizerId");

    let api = agw.get_api().api_id(api_id).send().await.expect("get_api");
    assert_eq!(api.name(), Some("cfn-http-api"));
    assert_eq!(api.protocol_type().map(|p| p.as_str()), Some("HTTP"));
    let cors = api.cors_configuration().expect("cors present");
    assert!(cors
        .allow_origins()
        .iter()
        .any(|o| o == "https://example.com"));

    let integ = agw
        .get_integration()
        .api_id(api_id)
        .integration_id(integ_id)
        .send()
        .await
        .expect("get_integration");
    assert_eq!(
        integ.integration_type().map(|t| t.as_str()),
        Some("HTTP_PROXY")
    );
    assert_eq!(
        integ.integration_uri(),
        Some("https://example.com/upstream")
    );

    let route = agw
        .get_route()
        .api_id(api_id)
        .route_id(route_id)
        .send()
        .await
        .expect("get_route");
    assert_eq!(route.route_key(), Some("GET /pets"));

    let stage = agw
        .get_stage()
        .api_id(api_id)
        .stage_name("prod")
        .send()
        .await
        .expect("get_stage");
    assert_eq!(stage.stage_name(), Some("prod"));
    assert_eq!(stage.deployment_id(), Some(deploy_id));

    let auth = agw
        .get_authorizer()
        .api_id(api_id)
        .authorizer_id(authz_id)
        .send()
        .await
        .expect("get_authorizer");
    assert_eq!(auth.name(), Some("jwt-auth"));
    assert_eq!(auth.authorizer_type().map(|t| t.as_str()), Some("JWT"));
    let jwt = auth.jwt_configuration().expect("jwt cfg");
    assert!(jwt.audience().iter().any(|a| a == "my-aud"));

    cfn.delete_stack()
        .stack_name("apigwv2-stack")
        .send()
        .await
        .expect("delete_stack");

    let after = agw.get_api().api_id(api_id).send().await;
    assert!(after.is_err(), "api should be gone after delete");
}

const TEMPLATE_V1: &str = r#"{
  "AWSTemplateFormatVersion": "2010-09-09",
  "Resources": {
    "Api": {
      "Type": "AWS::ApiGatewayV2::Api",
      "Properties": {
        "Name": "upd-http-api",
        "ProtocolType": "HTTP",
        "Description": "v1",
        "RouteSelectionExpression": "$request.method $request.path"
      }
    },
    "Integ": {
      "Type": "AWS::ApiGatewayV2::Integration",
      "Properties": {
        "ApiId": {"Ref": "Api"},
        "IntegrationType": "HTTP_PROXY",
        "IntegrationUri": "https://example.com/v1",
        "PayloadFormatVersion": "1.0",
        "TimeoutInMillis": 5000
      }
    },
    "GetPets": {
      "Type": "AWS::ApiGatewayV2::Route",
      "Properties": {
        "ApiId": {"Ref": "Api"},
        "RouteKey": "GET /pets",
        "Target": {"Fn::Join": ["/", ["integrations", {"Ref": "Integ"}]]},
        "AuthorizationType": "NONE"
      }
    },
    "Deploy": {
      "Type": "AWS::ApiGatewayV2::Deployment",
      "DependsOn": "GetPets",
      "Properties": {
        "ApiId": {"Ref": "Api"},
        "Description": "v1-deploy"
      }
    },
    "ProdStage": {
      "Type": "AWS::ApiGatewayV2::Stage",
      "Properties": {
        "ApiId": {"Ref": "Api"},
        "StageName": "prod",
        "DeploymentId": {"Ref": "Deploy"},
        "AutoDeploy": false,
        "Description": "stage v1"
      }
    },
    "Authz": {
      "Type": "AWS::ApiGatewayV2::Authorizer",
      "Properties": {
        "ApiId": {"Ref": "Api"},
        "Name": "auth-v1",
        "AuthorizerType": "JWT",
        "IdentitySource": ["$request.header.Authorization"],
        "JwtConfiguration": {"Audience": ["aud-v1"], "Issuer": "https://issuer.example.com"}
      }
    },
    "Vpc": {
      "Type": "AWS::ApiGatewayV2::VpcLink",
      "Properties": {
        "Name": "vpc-v1",
        "SubnetIds": ["subnet-aaa", "subnet-bbb"],
        "SecurityGroupIds": ["sg-111"]
      }
    },
    "Domain": {
      "Type": "AWS::ApiGatewayV2::DomainName",
      "Properties": {
        "DomainName": "api.cfnv2.example.com",
        "DomainNameConfigurations": [
          {"CertificateArn": "arn:aws:acm:us-east-1:000000000000:certificate/abc", "EndpointType": "REGIONAL"}
        ]
      }
    },
    "Mapping": {
      "Type": "AWS::ApiGatewayV2::ApiMapping",
      "DependsOn": ["Domain", "ProdStage"],
      "Properties": {
        "DomainName": "api.cfnv2.example.com",
        "ApiId": {"Ref": "Api"},
        "Stage": "prod",
        "ApiMappingKey": "v1"
      }
    },
    "PetModel": {
      "Type": "AWS::ApiGatewayV2::Model",
      "Properties": {
        "ApiId": {"Ref": "Api"},
        "Name": "PetModel",
        "ContentType": "application/json",
        "Description": "model v1",
        "Schema": "{\"type\":\"object\"}"
      }
    },
    "GetPetsResp": {
      "Type": "AWS::ApiGatewayV2::RouteResponse",
      "Properties": {
        "ApiId": {"Ref": "Api"},
        "RouteId": {"Ref": "GetPets"},
        "RouteResponseKey": "$default"
      }
    },
    "IntegResp": {
      "Type": "AWS::ApiGatewayV2::IntegrationResponse",
      "Properties": {
        "ApiId": {"Ref": "Api"},
        "IntegrationId": {"Ref": "Integ"},
        "IntegrationResponseKey": "/200/"
      }
    }
  },
  "Outputs": {
    "ApiId": {"Value": {"Ref": "Api"}},
    "IntegId": {"Value": {"Ref": "Integ"}},
    "RouteId": {"Value": {"Ref": "GetPets"}},
    "AuthzId": {"Value": {"Ref": "Authz"}},
    "VpcId": {"Value": {"Ref": "Vpc"}},
    "MappingId": {"Value": {"Ref": "Mapping"}},
    "ModelId": {"Value": {"Ref": "PetModel"}}
  }
}"#;

const TEMPLATE_V2: &str = r#"{
  "AWSTemplateFormatVersion": "2010-09-09",
  "Resources": {
    "Api": {
      "Type": "AWS::ApiGatewayV2::Api",
      "Properties": {
        "Name": "upd-http-api-renamed",
        "ProtocolType": "HTTP",
        "Description": "v2",
        "RouteSelectionExpression": "$request.method $request.path"
      }
    },
    "Integ": {
      "Type": "AWS::ApiGatewayV2::Integration",
      "Properties": {
        "ApiId": {"Ref": "Api"},
        "IntegrationType": "HTTP_PROXY",
        "IntegrationUri": "https://example.com/v2",
        "PayloadFormatVersion": "2.0",
        "TimeoutInMillis": 12000
      }
    },
    "GetPets": {
      "Type": "AWS::ApiGatewayV2::Route",
      "Properties": {
        "ApiId": {"Ref": "Api"},
        "RouteKey": "POST /pets",
        "Target": {"Fn::Join": ["/", ["integrations", {"Ref": "Integ"}]]},
        "AuthorizationType": "NONE"
      }
    },
    "Deploy": {
      "Type": "AWS::ApiGatewayV2::Deployment",
      "DependsOn": "GetPets",
      "Properties": {
        "ApiId": {"Ref": "Api"},
        "Description": "v2-deploy"
      }
    },
    "ProdStage": {
      "Type": "AWS::ApiGatewayV2::Stage",
      "Properties": {
        "ApiId": {"Ref": "Api"},
        "StageName": "prod",
        "DeploymentId": {"Ref": "Deploy"},
        "AutoDeploy": true,
        "Description": "stage v2"
      }
    },
    "Authz": {
      "Type": "AWS::ApiGatewayV2::Authorizer",
      "Properties": {
        "ApiId": {"Ref": "Api"},
        "Name": "auth-v2",
        "AuthorizerType": "JWT",
        "IdentitySource": ["$request.header.Authorization"],
        "JwtConfiguration": {"Audience": ["aud-v2"], "Issuer": "https://issuer.example.com"}
      }
    },
    "Vpc": {
      "Type": "AWS::ApiGatewayV2::VpcLink",
      "Properties": {
        "Name": "vpc-v2",
        "SubnetIds": ["subnet-aaa", "subnet-bbb"],
        "SecurityGroupIds": ["sg-111"]
      }
    },
    "Domain": {
      "Type": "AWS::ApiGatewayV2::DomainName",
      "Properties": {
        "DomainName": "api.cfnv2.example.com",
        "DomainNameConfigurations": [
          {"CertificateArn": "arn:aws:acm:us-east-1:000000000000:certificate/abc", "EndpointType": "EDGE"}
        ]
      }
    },
    "Mapping": {
      "Type": "AWS::ApiGatewayV2::ApiMapping",
      "DependsOn": ["Domain", "ProdStage"],
      "Properties": {
        "DomainName": "api.cfnv2.example.com",
        "ApiId": {"Ref": "Api"},
        "Stage": "prod",
        "ApiMappingKey": "v2"
      }
    },
    "PetModel": {
      "Type": "AWS::ApiGatewayV2::Model",
      "Properties": {
        "ApiId": {"Ref": "Api"},
        "Name": "PetModelV2",
        "ContentType": "application/json",
        "Description": "model v2",
        "Schema": "{\"type\":\"object\",\"required\":[\"id\"]}"
      }
    },
    "GetPetsResp": {
      "Type": "AWS::ApiGatewayV2::RouteResponse",
      "Properties": {
        "ApiId": {"Ref": "Api"},
        "RouteId": {"Ref": "GetPets"},
        "RouteResponseKey": "$default"
      }
    },
    "IntegResp": {
      "Type": "AWS::ApiGatewayV2::IntegrationResponse",
      "Properties": {
        "ApiId": {"Ref": "Api"},
        "IntegrationId": {"Ref": "Integ"},
        "IntegrationResponseKey": "/202/"
      }
    }
  },
  "Outputs": {
    "ApiId": {"Value": {"Ref": "Api"}},
    "IntegId": {"Value": {"Ref": "Integ"}},
    "RouteId": {"Value": {"Ref": "GetPets"}},
    "AuthzId": {"Value": {"Ref": "Authz"}},
    "VpcId": {"Value": {"Ref": "Vpc"}},
    "MappingId": {"Value": {"Ref": "Mapping"}},
    "ModelId": {"Value": {"Ref": "PetModel"}}
  }
}"#;

#[tokio::test]
async fn cfn_updates_apigatewayv2_in_place() {
    let server = TestServer::start().await;
    let cfn = server.cloudformation_client().await;
    let agw = aws_sdk_apigatewayv2::Client::new(&server.aws_config().await);

    cfn.create_stack()
        .stack_name("apigwv2-update-stack")
        .template_body(TEMPLATE_V1)
        .capabilities(Capability::CapabilityIam)
        .on_failure(OnFailure::Rollback)
        .send()
        .await
        .expect("create_stack v1");

    let described = cfn
        .describe_stacks()
        .stack_name("apigwv2-update-stack")
        .send()
        .await
        .expect("describe v1");
    let stack = described.stacks().first().expect("stack v1");
    assert_eq!(stack.stack_status().unwrap().as_str(), "CREATE_COMPLETE");
    let outputs: std::collections::HashMap<&str, &str> = stack
        .outputs()
        .iter()
        .filter_map(|o| Some((o.output_key()?, o.output_value()?)))
        .collect();
    let api_id = outputs.get("ApiId").copied().expect("ApiId").to_string();
    let integ_id = outputs
        .get("IntegId")
        .copied()
        .expect("IntegId")
        .to_string();
    let route_id = outputs
        .get("RouteId")
        .copied()
        .expect("RouteId")
        .to_string();
    let authz_id = outputs
        .get("AuthzId")
        .copied()
        .expect("AuthzId")
        .to_string();
    let vpc_id = outputs.get("VpcId").copied().expect("VpcId").to_string();
    let model_id = outputs
        .get("ModelId")
        .copied()
        .expect("ModelId")
        .to_string();

    cfn.update_stack()
        .stack_name("apigwv2-update-stack")
        .template_body(TEMPLATE_V2)
        .capabilities(Capability::CapabilityIam)
        .send()
        .await
        .expect("update_stack v2");

    let described2 = cfn
        .describe_stacks()
        .stack_name("apigwv2-update-stack")
        .send()
        .await
        .expect("describe v2");
    let stack2 = described2.stacks().first().expect("stack v2");
    assert_eq!(stack2.stack_status().unwrap().as_str(), "UPDATE_COMPLETE");
    let outputs2: std::collections::HashMap<&str, &str> = stack2
        .outputs()
        .iter()
        .filter_map(|o| Some((o.output_key()?, o.output_value()?)))
        .collect();
    // Physical ids must be stable across the update.
    assert_eq!(outputs2.get("ApiId").copied(), Some(api_id.as_str()));
    assert_eq!(outputs2.get("IntegId").copied(), Some(integ_id.as_str()));
    assert_eq!(outputs2.get("RouteId").copied(), Some(route_id.as_str()));
    assert_eq!(outputs2.get("AuthzId").copied(), Some(authz_id.as_str()));
    assert_eq!(outputs2.get("VpcId").copied(), Some(vpc_id.as_str()));
    assert_eq!(outputs2.get("ModelId").copied(), Some(model_id.as_str()));

    // Api mutated in place.
    let api = agw
        .get_api()
        .api_id(&api_id)
        .send()
        .await
        .expect("get_api v2");
    assert_eq!(api.name(), Some("upd-http-api-renamed"));
    assert_eq!(api.description(), Some("v2"));

    // Integration mutated in place.
    let integ = agw
        .get_integration()
        .api_id(&api_id)
        .integration_id(&integ_id)
        .send()
        .await
        .expect("get_integration v2");
    assert_eq!(integ.integration_uri(), Some("https://example.com/v2"));
    assert_eq!(integ.payload_format_version(), Some("2.0"));
    assert_eq!(integ.timeout_in_millis(), Some(12000));

    // Route mutated in place.
    let route = agw
        .get_route()
        .api_id(&api_id)
        .route_id(&route_id)
        .send()
        .await
        .expect("get_route v2");
    assert_eq!(route.route_key(), Some("POST /pets"));

    // Stage AutoDeploy + Description updated.
    let stage = agw
        .get_stage()
        .api_id(&api_id)
        .stage_name("prod")
        .send()
        .await
        .expect("get_stage v2");
    assert_eq!(stage.description(), Some("stage v2"));
    assert_eq!(stage.auto_deploy(), Some(true));

    // Authorizer renamed + new audience.
    let auth = agw
        .get_authorizer()
        .api_id(&api_id)
        .authorizer_id(&authz_id)
        .send()
        .await
        .expect("get_authorizer v2");
    assert_eq!(auth.name(), Some("auth-v2"));
    assert!(auth
        .jwt_configuration()
        .expect("jwt v2")
        .audience()
        .iter()
        .any(|a| a == "aud-v2"));

    cfn.delete_stack()
        .stack_name("apigwv2-update-stack")
        .send()
        .await
        .expect("delete_stack");

    let after = agw.get_api().api_id(&api_id).send().await;
    assert!(after.is_err(), "api gone after delete");
}
