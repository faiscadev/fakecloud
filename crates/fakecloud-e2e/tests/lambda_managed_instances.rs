//! Lambda Managed Instances functions end to end with the official SDK: a
//! function's `CapacityProviderConfig` round-trips through every
//! `FunctionConfiguration`, versions show up under the capacity provider,
//! and the provider can't be deleted while they use it.

mod helpers;

use aws_sdk_lambda::types::{
    CapacityProviderConfig, CapacityProviderPermissionsConfig, CapacityProviderVpcConfig,
    FunctionCode, FunctionVersionLatestPublished, LambdaManagedInstancesCapacityProviderConfig,
    Runtime,
};
use aws_sdk_lambda::Client;
use helpers::TestServer;

async fn create_provider(client: &Client, name: &str) -> String {
    let resp = client
        .create_capacity_provider()
        .capacity_provider_name(name)
        .vpc_config(
            CapacityProviderVpcConfig::builder()
                .subnet_ids("subnet-0123456789abcdef0")
                .security_group_ids("sg-0123456789abcdef0")
                .build()
                .unwrap(),
        )
        .permissions_config(
            CapacityProviderPermissionsConfig::builder()
                .capacity_provider_operator_role_arn("arn:aws:iam::123456789012:role/operator")
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();
    resp.capacity_provider()
        .unwrap()
        .capacity_provider_arn()
        .to_string()
}

fn config(arn: &str, ratio: Option<f64>, concurrency: Option<i32>) -> CapacityProviderConfig {
    CapacityProviderConfig::builder()
        .lambda_managed_instances_capacity_provider_config(
            LambdaManagedInstancesCapacityProviderConfig::builder()
                .capacity_provider_arn(arn)
                .set_execution_environment_memory_gib_per_v_cpu(ratio)
                .set_per_execution_environment_max_concurrency(concurrency)
                .build()
                .unwrap(),
        )
        .build()
}

fn code() -> FunctionCode {
    FunctionCode::builder()
        .zip_file(aws_sdk_lambda::primitives::Blob::new(
            b"PK\x05\x06".to_vec(),
        ))
        .build()
}

#[tokio::test]
async fn lambda_managed_instances_function_round_trips_its_capacity_provider() {
    let server = TestServer::start().await;
    let client = server.lambda_client().await;
    let cp_arn = create_provider(&client, "mi-cp").await;
    assert!(
        cp_arn.ends_with(":capacity-provider:mi-cp"),
        "capacity provider ARN uses a colon: {cp_arn}"
    );

    let created = client
        .create_function()
        .function_name("mi-fn")
        .runtime(Runtime::Python313)
        .role("arn:aws:iam::123456789012:role/test-role")
        .handler("index.handler")
        .code(code())
        .capacity_provider_config(config(&cp_arn, None, Some(16)))
        .send()
        .await
        .unwrap();
    assert_eq!(created.memory_size(), Some(2048));
    let inner = created
        .capacity_provider_config()
        .unwrap()
        .lambda_managed_instances_capacity_provider_config()
        .unwrap();
    assert_eq!(inner.capacity_provider_arn(), cp_arn);
    assert_eq!(inner.per_execution_environment_max_concurrency(), Some(16));
    assert_eq!(
        inner.execution_environment_memory_gib_per_v_cpu(),
        Some(2.0)
    );

    let got = client
        .get_function_configuration()
        .function_name("mi-fn")
        .send()
        .await
        .unwrap();
    assert_eq!(
        got.capacity_provider_config()
            .unwrap()
            .lambda_managed_instances_capacity_provider_config()
            .unwrap()
            .capacity_provider_arn(),
        cp_arn
    );
    let listed = client.list_functions().send().await.unwrap();
    let row = listed
        .functions()
        .iter()
        .find(|f| f.function_name() == Some("mi-fn"))
        .unwrap();
    assert!(row.capacity_provider_config().is_some());

    // Retune the memory ratio in place.
    let updated = client
        .update_function_configuration()
        .function_name("mi-fn")
        .memory_size(4096)
        .capacity_provider_config(config(&cp_arn, Some(4.0), None))
        .send()
        .await
        .unwrap();
    assert_eq!(
        updated
            .capacity_provider_config()
            .unwrap()
            .lambda_managed_instances_capacity_provider_config()
            .unwrap()
            .execution_environment_memory_gib_per_v_cpu(),
        Some(4.0)
    );

    // Numbered and $LATEST.PUBLISHED versions carry the config.
    let v1 = client
        .publish_version()
        .function_name("mi-fn")
        .send()
        .await
        .unwrap();
    assert_eq!(v1.version(), Some("1"));
    assert!(v1.capacity_provider_config().is_some());
    let latest_published = client
        .publish_version()
        .function_name("mi-fn")
        .publish_to(FunctionVersionLatestPublished::LatestPublished)
        .send()
        .await
        .unwrap();
    assert_eq!(latest_published.version(), Some("$LATEST.PUBLISHED"));
    let fn_arn = created.function_arn().unwrap().to_string();
    assert_eq!(
        latest_published.function_arn(),
        Some(format!("{fn_arn}:$LATEST.PUBLISHED").as_str())
    );
    let got = client
        .get_function()
        .function_name("mi-fn")
        .qualifier("$LATEST.PUBLISHED")
        .send()
        .await
        .unwrap();
    assert_eq!(
        got.configuration().unwrap().version(),
        Some("$LATEST.PUBLISHED")
    );

    let versions = client
        .list_function_versions_by_capacity_provider()
        .capacity_provider_name("mi-cp")
        .send()
        .await
        .unwrap();
    assert_eq!(versions.capacity_provider_arn(), cp_arn);
    let arns: Vec<&str> = versions
        .function_versions()
        .iter()
        .map(|v| v.function_arn())
        .collect();
    assert_eq!(
        arns,
        vec![
            format!("{fn_arn}:$LATEST"),
            format!("{fn_arn}:$LATEST.PUBLISHED"),
            format!("{fn_arn}:1"),
        ]
    );
    assert!(versions
        .function_versions()
        .iter()
        .all(|v| v.state().as_str() == "Active"));

    // The provider can't be deleted while function versions use it.
    let err = client
        .delete_capacity_provider()
        .capacity_provider_name("mi-cp")
        .send()
        .await
        .unwrap_err()
        .into_service_error();
    assert!(err.is_resource_conflict_exception(), "{err:?}");

    client
        .delete_function()
        .function_name("mi-fn")
        .send()
        .await
        .unwrap();
    let versions = client
        .list_function_versions_by_capacity_provider()
        .capacity_provider_name(&cp_arn)
        .send()
        .await
        .unwrap();
    assert!(versions.function_versions().is_empty());
    client
        .delete_capacity_provider()
        .capacity_provider_name("mi-cp")
        .send()
        .await
        .unwrap();
}

#[tokio::test]
async fn lambda_managed_instances_function_refuses_bad_capacity_provider_config() {
    let server = TestServer::start().await;
    let client = server.lambda_client().await;
    let cp_arn = create_provider(&client, "mi-cp").await;
    let account = cp_arn.split(':').nth(4).unwrap().to_string();
    let create = |name: &str, runtime: Runtime, cfg: CapacityProviderConfig| {
        client
            .create_function()
            .function_name(name)
            .runtime(runtime)
            .role("arn:aws:iam::123456789012:role/test-role")
            .handler("index.handler")
            .code(code())
            .capacity_provider_config(cfg)
    };

    let missing = format!("arn:aws:lambda:us-east-1:{account}:capacity-provider:missing");
    let err = create("f1", Runtime::Python313, config(&missing, None, None))
        .send()
        .await
        .unwrap_err()
        .into_service_error();
    assert!(err.is_resource_not_found_exception(), "{err:?}");
    assert!(
        err.meta()
            .message()
            .unwrap()
            .starts_with(&format!("Capacity provider not found: {missing}")),
        "{err:?}"
    );

    let err = create("f2", Runtime::Python39, config(&cp_arn, None, None))
        .send()
        .await
        .unwrap_err()
        .into_service_error();
    assert!(err.is_invalid_parameter_value_exception(), "{err:?}");
    assert_eq!(
        err.meta().message(),
        Some("Runtime Enum python3.9 does not support specified feature: Lambda Managed Instances")
    );

    let err = create("f3", Runtime::Python313, config(&cp_arn, None, None))
        .memory_size(1024)
        .send()
        .await
        .unwrap_err()
        .into_service_error();
    assert_eq!(err.meta().code(), Some("ValidationException"));

    let err = create("f4", Runtime::Python313, config(&cp_arn, Some(3.0), None))
        .memory_size(8192)
        .send()
        .await
        .unwrap_err()
        .into_service_error();
    assert!(err.is_invalid_parameter_value_exception(), "{err:?}");

    // A Lambda (default) function can't take a capacity provider later, and
    // has no $LATEST.PUBLISHED version to publish.
    client
        .create_function()
        .function_name("plain")
        .runtime(Runtime::Python313)
        .role("arn:aws:iam::123456789012:role/test-role")
        .handler("index.handler")
        .code(code())
        .send()
        .await
        .unwrap();
    let err = client
        .update_function_configuration()
        .function_name("plain")
        .capacity_provider_config(config(&cp_arn, None, None))
        .send()
        .await
        .unwrap_err()
        .into_service_error();
    assert!(err.is_invalid_parameter_value_exception(), "{err:?}");
    assert_eq!(
        err.meta().message(),
        Some("CapacityProviderConfig isn't supported for Lambda Default functions.")
    );
    let err = client
        .publish_version()
        .function_name("plain")
        .publish_to(FunctionVersionLatestPublished::LatestPublished)
        .send()
        .await
        .unwrap_err()
        .into_service_error();
    assert!(err.is_invalid_parameter_value_exception(), "{err:?}");

    // None of the refused creates left a function behind.
    let listed = client.list_functions().send().await.unwrap();
    let names: Vec<&str> = listed
        .functions()
        .iter()
        .filter_map(|f| f.function_name())
        .collect();
    assert_eq!(names, vec!["plain"]);
}

#[tokio::test]
async fn lambda_managed_instances_function_via_cloudformation() {
    let server = TestServer::start().await;
    let cfn = server.cloudformation_client().await;
    let client = server.lambda_client().await;
    let template = r#"{
      "Resources": {
        "Cp": {
          "Type": "AWS::Lambda::CapacityProvider",
          "Properties": {
            "CapacityProviderName": "cfn-mi-cp",
            "VpcConfig": {"SubnetIds": ["subnet-0123456789abcdef0"], "SecurityGroupIds": ["sg-0123456789abcdef0"]},
            "PermissionsConfig": {"CapacityProviderOperatorRoleArn": "arn:aws:iam::123456789012:role/operator"}
          }
        },
        "Fn": {
          "Type": "AWS::Lambda::Function",
          "Properties": {
            "FunctionName": "cfn-mi-fn",
            "Runtime": "python3.13",
            "Handler": "index.handler",
            "Role": "arn:aws:iam::123456789012:role/test-role",
            "Code": {"ZipFile": "def handler(event, context):\n    return event\n"},
            "CapacityProviderConfig": {
              "LambdaManagedInstancesCapacityProviderConfig": {
                "CapacityProviderArn": {"Fn::GetAtt": ["Cp", "Arn"]}
              }
            },
            "PublishToLatestPublished": true
          }
        }
      },
      "Outputs": {
        "CpArn": {"Value": {"Fn::GetAtt": ["Cp", "Arn"]}}
      }
    }"#;
    cfn.create_stack()
        .stack_name("mi-stack")
        .template_body(template)
        .send()
        .await
        .unwrap();
    let stack = cfn
        .describe_stacks()
        .stack_name("mi-stack")
        .send()
        .await
        .unwrap();
    let stack = &stack.stacks()[0];
    assert_eq!(
        stack.stack_status().map(|s| s.as_str()),
        Some("CREATE_COMPLETE"),
        "{:?}",
        stack.stack_status_reason()
    );
    let cp_arn = stack
        .outputs()
        .iter()
        .find(|o| o.output_key() == Some("CpArn"))
        .and_then(|o| o.output_value())
        .unwrap()
        .to_string();

    let got = client
        .get_function_configuration()
        .function_name("cfn-mi-fn")
        .send()
        .await
        .unwrap();
    assert_eq!(
        got.capacity_provider_config()
            .unwrap()
            .lambda_managed_instances_capacity_provider_config()
            .unwrap()
            .capacity_provider_arn(),
        cp_arn
    );
    let published = client
        .get_function_configuration()
        .function_name("cfn-mi-fn")
        .qualifier("$LATEST.PUBLISHED")
        .send()
        .await
        .unwrap();
    assert_eq!(published.version(), Some("$LATEST.PUBLISHED"));

    // Deleting the stack removes the function before its provider.
    cfn.delete_stack()
        .stack_name("mi-stack")
        .send()
        .await
        .unwrap();
    let providers = client.list_capacity_providers().send().await.unwrap();
    assert!(providers.capacity_providers().is_empty());
}
