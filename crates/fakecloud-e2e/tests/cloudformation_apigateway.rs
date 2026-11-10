//! CloudFormation provisioner for AWS::ApiGateway::* (BB9).
//!
//! The first test covers the full surface in one stack: RestApi,
//! Resource, Method (with Integration), Deployment, Stage, Authorizer,
//! RequestValidator, Model, GatewayResponse, UsagePlan, ApiKey,
//! UsagePlanKey, DomainName, BasePathMapping. After CreateStack we
//! assert each resource is visible via the matching API Gateway v1
//! SDK call so we know the CFN side and the runtime side agree on the
//! stored shape.
//!
//! The second test exercises UpdateStack on every resource type that
//! supports an in-place update (everything except `UsagePlanKey`,
//! whose properties are all "requires-replacement" in CFN). We mutate
//! one or two well-known properties per resource and re-read via SDK.
//!
//! Finally DeleteStack should remove every resource — we verify the
//! RestApi GET 404s after teardown.

mod helpers;

use aws_sdk_cloudformation::types::{Capability, OnFailure};
use helpers::TestServer;

const TEMPLATE_V1: &str = r#"{
  "AWSTemplateFormatVersion": "2010-09-09",
  "Resources": {
    "Api": {
      "Type": "AWS::ApiGateway::RestApi",
      "Properties": {
        "Name": "cfn-api",
        "Description": "from cfn",
        "EndpointConfiguration": {"Types": ["REGIONAL"]},
        "BinaryMediaTypes": ["image/png"],
        "MinimumCompressionSize": 1024,
        "ApiKeySourceType": "HEADER",
        "DisableExecuteApiEndpoint": false,
        "Tags": [{"Key": "env", "Value": "test"}]
      }
    },
    "PetsResource": {
      "Type": "AWS::ApiGateway::Resource",
      "Properties": {
        "RestApiId": {"Ref": "Api"},
        "ParentId": {"Fn::GetAtt": ["Api", "RootResourceId"]},
        "PathPart": "pets"
      }
    },
    "PetModel": {
      "Type": "AWS::ApiGateway::Model",
      "Properties": {
        "RestApiId": {"Ref": "Api"},
        "Name": "Pet",
        "Description": "Pet model",
        "ContentType": "application/json",
        "Schema": {"type": "object", "properties": {"id": {"type": "string"}}}
      }
    },
    "BodyValidator": {
      "Type": "AWS::ApiGateway::RequestValidator",
      "Properties": {
        "RestApiId": {"Ref": "Api"},
        "Name": "body-only",
        "ValidateRequestBody": true,
        "ValidateRequestParameters": false
      }
    },
    "TokenAuth": {
      "Type": "AWS::ApiGateway::Authorizer",
      "Properties": {
        "RestApiId": {"Ref": "Api"},
        "Name": "token-auth",
        "Type": "TOKEN",
        "AuthorizerUri": "arn:aws:apigateway:us-east-1:lambda:path/2015-03-31/functions/arn:aws:lambda:us-east-1:000000000000:function:auth/invocations",
        "IdentitySource": "method.request.header.Authorization",
        "AuthorizerResultTtlInSeconds": 300
      }
    },
    "GetPets": {
      "Type": "AWS::ApiGateway::Method",
      "Properties": {
        "RestApiId": {"Ref": "Api"},
        "ResourceId": {"Ref": "PetsResource"},
        "HttpMethod": "GET",
        "AuthorizationType": "NONE",
        "Integration": {
          "Type": "MOCK",
          "PassthroughBehavior": "WHEN_NO_MATCH",
          "RequestTemplates": {"application/json": "{\"statusCode\":200}"}
        }
      }
    },
    "Default4xx": {
      "Type": "AWS::ApiGateway::GatewayResponse",
      "Properties": {
        "RestApiId": {"Ref": "Api"},
        "ResponseType": "DEFAULT_4XX",
        "StatusCode": "400",
        "ResponseTemplates": {"application/json": "{\"message\": \"oops\"}"}
      }
    },
    "Deploy": {
      "Type": "AWS::ApiGateway::Deployment",
      "DependsOn": "GetPets",
      "Properties": {
        "RestApiId": {"Ref": "Api"},
        "Description": "initial deployment"
      }
    },
    "ProdStage": {
      "Type": "AWS::ApiGateway::Stage",
      "Properties": {
        "RestApiId": {"Ref": "Api"},
        "DeploymentId": {"Ref": "Deploy"},
        "StageName": "prod",
        "Description": "production",
        "TracingEnabled": false,
        "Variables": {"foo": "bar"},
        "MethodSettings": [{
          "ResourcePath": "/pets",
          "HttpMethod": "GET",
          "ThrottlingBurstLimit": 100,
          "ThrottlingRateLimit": 50
        }],
        "Tags": [{"Key": "stage", "Value": "prod"}]
      }
    },
    "Plan": {
      "Type": "AWS::ApiGateway::UsagePlan",
      "Properties": {
        "UsagePlanName": "cfn-plan",
        "Description": "test plan",
        "Throttle": {"BurstLimit": 100, "RateLimit": 50},
        "Quota": {"Limit": 1000, "Period": "DAY"},
        "Tags": [{"Key": "tier", "Value": "free"}]
      }
    },
    "Key": {
      "Type": "AWS::ApiGateway::ApiKey",
      "Properties": {
        "Name": "cfn-key",
        "Description": "test key",
        "Enabled": true,
        "Tags": [{"Key": "owner", "Value": "fakecloud"}]
      }
    },
    "PlanKey": {
      "Type": "AWS::ApiGateway::UsagePlanKey",
      "Properties": {
        "UsagePlanId": {"Ref": "Plan"},
        "KeyId": {"Ref": "Key"},
        "KeyType": "API_KEY"
      }
    },
    "Domain": {
      "Type": "AWS::ApiGateway::DomainName",
      "Properties": {
        "DomainName": "api.example.test",
        "RegionalCertificateArn": "arn:aws:acm:us-east-1:000000000000:certificate/abcd",
        "EndpointConfiguration": {"Types": ["REGIONAL"]},
        "SecurityPolicy": "TLS_1_2"
      }
    },
    "Mapping": {
      "Type": "AWS::ApiGateway::BasePathMapping",
      "DependsOn": "ProdStage",
      "Properties": {
        "DomainName": {"Ref": "Domain"},
        "RestApiId": {"Ref": "Api"},
        "Stage": "prod",
        "BasePath": "v1"
      }
    }
  },
  "Outputs": {
    "ApiId": {"Value": {"Ref": "Api"}},
    "RootId": {"Value": {"Fn::GetAtt": ["Api", "RootResourceId"]}},
    "PetsId": {"Value": {"Ref": "PetsResource"}},
    "ModelName": {"Value": {"Ref": "PetModel"}},
    "ValidatorId": {"Value": {"Ref": "BodyValidator"}},
    "AuthId": {"Value": {"Ref": "TokenAuth"}},
    "DeployId": {"Value": {"Ref": "Deploy"}},
    "StageName": {"Value": {"Ref": "ProdStage"}},
    "PlanId": {"Value": {"Ref": "Plan"}},
    "KeyId": {"Value": {"Ref": "Key"}},
    "DomainName": {"Value": {"Ref": "Domain"}}
  }
}"#;

const TEMPLATE_V2: &str = r#"{
  "AWSTemplateFormatVersion": "2010-09-09",
  "Resources": {
    "Api": {
      "Type": "AWS::ApiGateway::RestApi",
      "Properties": {
        "Name": "cfn-api-renamed",
        "Description": "updated description",
        "EndpointConfiguration": {"Types": ["REGIONAL"]},
        "BinaryMediaTypes": ["image/png", "application/octet-stream"],
        "MinimumCompressionSize": 4096,
        "Tags": [{"Key": "env", "Value": "prod"}]
      }
    },
    "PetsResource": {
      "Type": "AWS::ApiGateway::Resource",
      "Properties": {
        "RestApiId": {"Ref": "Api"},
        "ParentId": {"Fn::GetAtt": ["Api", "RootResourceId"]},
        "PathPart": "pets"
      }
    },
    "PetModel": {
      "Type": "AWS::ApiGateway::Model",
      "Properties": {
        "RestApiId": {"Ref": "Api"},
        "Name": "Pet",
        "Description": "Pet model v2",
        "ContentType": "application/json",
        "Schema": {"type": "object"}
      }
    },
    "BodyValidator": {
      "Type": "AWS::ApiGateway::RequestValidator",
      "Properties": {
        "RestApiId": {"Ref": "Api"},
        "Name": "body-only",
        "ValidateRequestBody": true,
        "ValidateRequestParameters": true
      }
    },
    "TokenAuth": {
      "Type": "AWS::ApiGateway::Authorizer",
      "Properties": {
        "RestApiId": {"Ref": "Api"},
        "Name": "token-auth-renamed",
        "Type": "TOKEN",
        "AuthorizerUri": "arn:aws:apigateway:us-east-1:lambda:path/2015-03-31/functions/arn:aws:lambda:us-east-1:000000000000:function:auth/invocations",
        "IdentitySource": "method.request.header.Authorization",
        "AuthorizerResultTtlInSeconds": 600
      }
    },
    "GetPets": {
      "Type": "AWS::ApiGateway::Method",
      "Properties": {
        "RestApiId": {"Ref": "Api"},
        "ResourceId": {"Ref": "PetsResource"},
        "HttpMethod": "GET",
        "AuthorizationType": "NONE",
        "Integration": {
          "Type": "MOCK",
          "PassthroughBehavior": "WHEN_NO_MATCH",
          "RequestTemplates": {"application/json": "{\"statusCode\":200}"}
        }
      }
    },
    "Default4xx": {
      "Type": "AWS::ApiGateway::GatewayResponse",
      "Properties": {
        "RestApiId": {"Ref": "Api"},
        "ResponseType": "DEFAULT_4XX",
        "StatusCode": "418",
        "ResponseTemplates": {"application/json": "{\"message\": \"updated\"}"}
      }
    },
    "Deploy": {
      "Type": "AWS::ApiGateway::Deployment",
      "DependsOn": "GetPets",
      "Properties": {
        "RestApiId": {"Ref": "Api"},
        "Description": "updated deployment"
      }
    },
    "ProdStage": {
      "Type": "AWS::ApiGateway::Stage",
      "Properties": {
        "RestApiId": {"Ref": "Api"},
        "DeploymentId": {"Ref": "Deploy"},
        "StageName": "prod",
        "Description": "production-renamed",
        "TracingEnabled": true,
        "Variables": {"foo": "baz", "extra": "yes"},
        "Tags": [{"Key": "stage", "Value": "prod-v2"}]
      }
    },
    "Plan": {
      "Type": "AWS::ApiGateway::UsagePlan",
      "Properties": {
        "UsagePlanName": "cfn-plan-renamed",
        "Description": "updated plan",
        "Throttle": {"BurstLimit": 200, "RateLimit": 100},
        "Quota": {"Limit": 5000, "Period": "WEEK"},
        "Tags": [{"Key": "tier", "Value": "pro"}]
      }
    },
    "Key": {
      "Type": "AWS::ApiGateway::ApiKey",
      "Properties": {
        "Name": "cfn-key",
        "Description": "updated key",
        "Enabled": false,
        "Tags": [{"Key": "owner", "Value": "fakecloud-v2"}]
      }
    },
    "PlanKey": {
      "Type": "AWS::ApiGateway::UsagePlanKey",
      "Properties": {
        "UsagePlanId": {"Ref": "Plan"},
        "KeyId": {"Ref": "Key"},
        "KeyType": "API_KEY"
      }
    },
    "Domain": {
      "Type": "AWS::ApiGateway::DomainName",
      "Properties": {
        "DomainName": "api.example.test",
        "RegionalCertificateArn": "arn:aws:acm:us-east-1:000000000000:certificate/wxyz",
        "EndpointConfiguration": {"Types": ["REGIONAL"]},
        "SecurityPolicy": "TLS_1_2"
      }
    },
    "Mapping": {
      "Type": "AWS::ApiGateway::BasePathMapping",
      "DependsOn": "ProdStage",
      "Properties": {
        "DomainName": {"Ref": "Domain"},
        "RestApiId": {"Ref": "Api"},
        "Stage": "prod",
        "BasePath": "v1"
      }
    }
  },
  "Outputs": {
    "ApiId": {"Value": {"Ref": "Api"}},
    "ModelName": {"Value": {"Ref": "PetModel"}},
    "ValidatorId": {"Value": {"Ref": "BodyValidator"}},
    "AuthId": {"Value": {"Ref": "TokenAuth"}},
    "PlanId": {"Value": {"Ref": "Plan"}},
    "KeyId": {"Value": {"Ref": "Key"}}
  }
}"#;

#[tokio::test]
async fn cfn_provisions_all_apigateway_v1_types() {
    let server = TestServer::start().await;
    let cfn = server.cloudformation_client().await;
    let agw = server.apigateway_client().await;

    cfn.create_stack()
        .stack_name("apigw-stack")
        .template_body(TEMPLATE_V1)
        .capabilities(Capability::CapabilityIam)
        .on_failure(OnFailure::Rollback)
        .send()
        .await
        .expect("create_stack");

    let described = cfn
        .describe_stacks()
        .stack_name("apigw-stack")
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
    let api_id = outputs.get("ApiId").copied().expect("ApiId output");
    let root_id = outputs.get("RootId").copied().expect("RootId output");
    let pets_id = outputs.get("PetsId").copied().expect("PetsId output");
    let model_name = outputs.get("ModelName").copied().expect("ModelName output");
    let validator_id = outputs
        .get("ValidatorId")
        .copied()
        .expect("ValidatorId output");
    let auth_id = outputs.get("AuthId").copied().expect("AuthId output");
    let plan_id = outputs.get("PlanId").copied().expect("PlanId output");
    let key_id = outputs.get("KeyId").copied().expect("KeyId output");
    let domain = outputs
        .get("DomainName")
        .copied()
        .expect("DomainName output");

    // RestApi
    let api = agw
        .get_rest_api()
        .rest_api_id(api_id)
        .send()
        .await
        .expect("get_rest_api");
    assert_eq!(api.name(), Some("cfn-api"));
    assert_eq!(api.description(), Some("from cfn"));
    assert_eq!(api.root_resource_id(), Some(root_id));
    assert_eq!(api.minimum_compression_size(), Some(1024));
    assert!(api.binary_media_types().iter().any(|s| s == "image/png"));
    assert_eq!(
        api.tags().expect("tags").get("env").map(String::as_str),
        Some("test")
    );

    // Resource
    let resources = agw
        .get_resources()
        .rest_api_id(api_id)
        .send()
        .await
        .expect("get_resources");
    let pets = resources
        .items()
        .iter()
        .find(|r| r.id() == Some(pets_id))
        .expect("pets resource present");
    assert_eq!(pets.path_part(), Some("pets"));
    assert_eq!(pets.path(), Some("/pets"));

    // Model
    let model = agw
        .get_model()
        .rest_api_id(api_id)
        .model_name(model_name)
        .send()
        .await
        .expect("get_model");
    assert_eq!(model.name(), Some("Pet"));
    assert_eq!(model.content_type(), Some("application/json"));
    assert_eq!(model.description(), Some("Pet model"));

    // RequestValidator
    let validator = agw
        .get_request_validator()
        .rest_api_id(api_id)
        .request_validator_id(validator_id)
        .send()
        .await
        .expect("get_request_validator");
    assert_eq!(validator.name(), Some("body-only"));
    assert!(validator.validate_request_body());
    assert!(!validator.validate_request_parameters());

    // Authorizer
    let auth = agw
        .get_authorizer()
        .rest_api_id(api_id)
        .authorizer_id(auth_id)
        .send()
        .await
        .expect("get_authorizer");
    assert_eq!(auth.name(), Some("token-auth"));
    assert_eq!(auth.r#type().map(|t| t.as_str()), Some("TOKEN"));
    assert_eq!(auth.authorizer_result_ttl_in_seconds(), Some(300));

    // Method + Integration
    let method = agw
        .get_method()
        .rest_api_id(api_id)
        .resource_id(pets_id)
        .http_method("GET")
        .send()
        .await
        .expect("get_method");
    assert_eq!(method.http_method(), Some("GET"));
    assert_eq!(method.authorization_type(), Some("NONE"));

    let integration = agw
        .get_integration()
        .rest_api_id(api_id)
        .resource_id(pets_id)
        .http_method("GET")
        .send()
        .await
        .expect("get_integration");
    assert_eq!(integration.r#type().map(|t| t.as_str()), Some("MOCK"));

    // GatewayResponse
    let gr = agw
        .get_gateway_response()
        .rest_api_id(api_id)
        .response_type("DEFAULT_4XX".into())
        .send()
        .await
        .expect("get_gateway_response");
    assert_eq!(gr.status_code(), Some("400"));

    // Stage (verifies MethodSettings applied + Tags + Variables)
    let stage = agw
        .get_stage()
        .rest_api_id(api_id)
        .stage_name("prod")
        .send()
        .await
        .expect("get_stage");
    assert_eq!(stage.stage_name(), Some("prod"));
    assert!(!stage.deployment_id().unwrap_or_default().is_empty());
    assert_eq!(stage.description(), Some("production"));
    assert_eq!(
        stage
            .variables()
            .expect("variables")
            .get("foo")
            .map(String::as_str),
        Some("bar")
    );
    assert_eq!(
        stage.tags().expect("tags").get("stage").map(String::as_str),
        Some("prod")
    );
    let ms = stage.method_settings().expect("method_settings");
    assert!(ms.contains_key("pets/GET"), "MethodSettings entry missing");

    // UsagePlan
    let plan = agw
        .get_usage_plan()
        .usage_plan_id(plan_id)
        .send()
        .await
        .expect("get_usage_plan");
    assert_eq!(plan.name(), Some("cfn-plan"));
    let throttle = plan.throttle().expect("throttle present");
    assert_eq!(throttle.burst_limit(), 100);
    assert!((throttle.rate_limit() - 50.0).abs() < f64::EPSILON);
    let quota = plan.quota().expect("quota present");
    assert_eq!(quota.limit(), 1000);
    assert_eq!(
        plan.tags().expect("tags").get("tier").map(String::as_str),
        Some("free")
    );

    // ApiKey
    let key = agw
        .get_api_key()
        .api_key(key_id)
        .send()
        .await
        .expect("get_api_key");
    assert_eq!(key.name(), Some("cfn-key"));
    assert!(key.enabled());
    assert_eq!(
        key.tags().expect("tags").get("owner").map(String::as_str),
        Some("fakecloud")
    );

    // UsagePlanKey
    let upk = agw
        .get_usage_plan_keys()
        .usage_plan_id(plan_id)
        .send()
        .await
        .expect("get_usage_plan_keys");
    assert!(
        upk.items().iter().any(|k| k.id() == Some(key_id)),
        "usage plan key missing"
    );

    // DomainName
    let dom = agw
        .get_domain_name()
        .domain_name(domain)
        .send()
        .await
        .expect("get_domain_name");
    assert_eq!(dom.domain_name(), Some(domain));
    assert_eq!(dom.security_policy().map(|p| p.as_str()), Some("TLS_1_2"));

    // BasePathMapping
    let mapping = agw
        .get_base_path_mapping()
        .domain_name(domain)
        .base_path("v1")
        .send()
        .await
        .expect("get_base_path_mapping");
    assert_eq!(mapping.base_path(), Some("v1"));
    assert_eq!(mapping.stage(), Some("prod"));
    assert_eq!(mapping.rest_api_id(), Some(api_id));

    // ---- UpdateStack: mutate one property on each updatable type. ----
    cfn.update_stack()
        .stack_name("apigw-stack")
        .template_body(TEMPLATE_V2)
        .capabilities(Capability::CapabilityIam)
        .send()
        .await
        .expect("update_stack");

    let after_described = cfn
        .describe_stacks()
        .stack_name("apigw-stack")
        .send()
        .await
        .expect("describe_stacks v2");
    let after_stack = after_described.stacks().first().expect("stack v2");
    assert_eq!(
        after_stack.stack_status().unwrap().as_str(),
        "UPDATE_COMPLETE"
    );

    // RestApi rename + binary media types extended
    let api2 = agw
        .get_rest_api()
        .rest_api_id(api_id)
        .send()
        .await
        .expect("get_rest_api v2");
    assert_eq!(api2.name(), Some("cfn-api-renamed"));
    assert_eq!(api2.description(), Some("updated description"));
    assert_eq!(api2.minimum_compression_size(), Some(4096));
    assert!(api2
        .binary_media_types()
        .iter()
        .any(|s| s == "application/octet-stream"));
    assert_eq!(
        api2.tags().expect("tags").get("env").map(String::as_str),
        Some("prod")
    );

    // Model description updated
    let model2 = agw
        .get_model()
        .rest_api_id(api_id)
        .model_name("Pet")
        .send()
        .await
        .expect("get_model v2");
    assert_eq!(model2.description(), Some("Pet model v2"));

    // Both flags true, in place. (A validator's Name is create-only, so a rename
    // would be a replacement under a new id.)
    let validator2 = agw
        .get_request_validator()
        .rest_api_id(api_id)
        .request_validator_id(validator_id)
        .send()
        .await
        .expect("get_request_validator v2");
    assert_eq!(validator2.name(), Some("body-only"));
    assert!(validator2.validate_request_body());
    assert!(validator2.validate_request_parameters());

    // Authorizer rename + ttl bumped
    let auth2 = agw
        .get_authorizer()
        .rest_api_id(api_id)
        .authorizer_id(auth_id)
        .send()
        .await
        .expect("get_authorizer v2");
    assert_eq!(auth2.name(), Some("token-auth-renamed"));
    assert_eq!(auth2.authorizer_result_ttl_in_seconds(), Some(600));

    // GatewayResponse status code rotated
    let gr2 = agw
        .get_gateway_response()
        .rest_api_id(api_id)
        .response_type("DEFAULT_4XX".into())
        .send()
        .await
        .expect("get_gateway_response v2");
    assert_eq!(gr2.status_code(), Some("418"));

    // Stage description + tracing flipped + new variable
    let stage2 = agw
        .get_stage()
        .rest_api_id(api_id)
        .stage_name("prod")
        .send()
        .await
        .expect("get_stage v2");
    assert_eq!(stage2.description(), Some("production-renamed"));
    assert!(stage2.tracing_enabled());
    assert_eq!(
        stage2
            .variables()
            .expect("variables")
            .get("extra")
            .map(String::as_str),
        Some("yes")
    );

    // UsagePlan rename + throttle bumped + quota updated
    let plan2 = agw
        .get_usage_plan()
        .usage_plan_id(plan_id)
        .send()
        .await
        .expect("get_usage_plan v2");
    assert_eq!(plan2.name(), Some("cfn-plan-renamed"));
    assert_eq!(plan2.throttle().expect("throttle v2").burst_limit(), 200);
    assert_eq!(plan2.quota().expect("quota v2").limit(), 5000);
    assert_eq!(
        plan2.tags().expect("tags").get("tier").map(String::as_str),
        Some("pro")
    );

    // ApiKey rename + disabled
    let key2 = agw
        .get_api_key()
        .api_key(key_id)
        .send()
        .await
        .expect("get_api_key v2");
    // A key's Name is create-only: a rename would be a replacement under a new id.
    assert_eq!(key2.name(), Some("cfn-key"));
    assert!(!key2.enabled());
    assert_eq!(
        key2.tags().expect("tags").get("owner").map(String::as_str),
        Some("fakecloud-v2")
    );

    // ---- DeleteStack: every resource is gone afterwards. ----
    cfn.delete_stack()
        .stack_name("apigw-stack")
        .send()
        .await
        .expect("delete_stack");

    let after = agw.get_rest_api().rest_api_id(api_id).send().await;
    assert!(after.is_err(), "api should be gone after delete");
    let after_plan = agw.get_usage_plan().usage_plan_id(plan_id).send().await;
    assert!(
        after_plan.is_err(),
        "usage plan should be gone after delete"
    );
    let after_key = agw.get_api_key().api_key(key_id).send().await;
    assert!(after_key.is_err(), "api key should be gone after delete");
    let after_dom = agw.get_domain_name().domain_name(domain).send().await;
    assert!(after_dom.is_err(), "domain should be gone after delete");
}
