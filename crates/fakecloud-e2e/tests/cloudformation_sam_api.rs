//! SAM `AWS::Serverless::Function` `Api`/`HttpApi` events expand into a working
//! implicit API: a function with an Api event previously deployed with no
//! routes (every call 404'd). The transform now synthesizes the implicit
//! RestApi (resources + methods + deployment + stage) and HttpApi (integration
//! + route + stage) so the routes exist.

mod helpers;

use aws_sdk_cloudformation::types::Capability;
use helpers::TestServer;

const TEMPLATE: &str = r#"
AWSTemplateFormatVersion: '2010-09-09'
Transform: AWS::Serverless-2016-10-31
Resources:
  Api:
    Type: AWS::Serverless::Function
    Properties:
      FunctionName: sam-api-fn
      Runtime: python3.12
      Handler: index.handler
      InlineCode: |
        def handler(event, context):
            return {"statusCode": 200, "body": "ok"}
      Events:
        Hello:
          Type: Api
          Properties:
            Path: /hello
            Method: get
"#;

#[tokio::test]
async fn sam_function_api_event_creates_implicit_rest_api() {
    let server = TestServer::start().await;
    let cfn = server.cloudformation_client().await;

    cfn.create_stack()
        .stack_name("sam-api")
        .template_body(TEMPLATE)
        .capabilities(Capability::CapabilityNamedIam)
        .send()
        .await
        .expect("create_stack");

    let described = cfn
        .describe_stacks()
        .stack_name("sam-api")
        .send()
        .await
        .expect("describe_stacks");
    assert_eq!(
        described
            .stacks()
            .first()
            .unwrap()
            .stack_status()
            .unwrap()
            .as_str(),
        "CREATE_COMPLETE"
    );

    // The implicit ServerlessRestApi must exist with a /hello resource and a GET
    // method wired to the function via an AWS_PROXY integration.
    let apigw = server.apigateway_client().await;
    let apis = apigw.get_rest_apis().send().await.expect("get_rest_apis");
    let api = apis
        .items()
        .iter()
        .find(|a| a.name() == Some("ServerlessRestApi"))
        .expect("implicit ServerlessRestApi created");
    let api_id = api.id().unwrap();

    let resources = apigw
        .get_resources()
        .rest_api_id(api_id)
        .send()
        .await
        .expect("get_resources");
    let hello = resources
        .items()
        .iter()
        .find(|r| r.path() == Some("/hello"))
        .expect("/hello resource synthesized");
    assert!(
        hello
            .resource_methods()
            .map(|m| m.contains_key("GET"))
            .unwrap_or(false),
        "GET method on /hello: {:?}",
        hello.resource_methods()
    );

    // A deployed stage must exist so the route is reachable.
    let stages = apigw
        .get_stages()
        .rest_api_id(api_id)
        .send()
        .await
        .expect("get_stages");
    assert!(
        !stages.item().is_empty(),
        "a stage must be deployed for the implicit API"
    );
}
