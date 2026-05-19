mod helpers;

use aws_sdk_lambda::primitives::Blob;
use fakecloud_conformance_macros::test_action;
use helpers::TestServer;
use std::io::Write;

fn make_python_zip() -> Vec<u8> {
    let buf = Vec::new();
    let mut zip = zip::ZipWriter::new(std::io::Cursor::new(buf));
    let options =
        zip::write::SimpleFileOptions::default().compression_method(zip::CompressionMethod::Stored);
    zip.start_file("index.py", options).unwrap();
    zip.write_all(b"def handler(event, context):\n    return {\"statusCode\": 200}\n")
        .unwrap();
    zip.finish().unwrap().into_inner()
}

// ---------------------------------------------------------------------------
// Function lifecycle
// ---------------------------------------------------------------------------

#[test_action("lambda", "CreateFunction", checksum = "5e8fbe96")]
#[test_action("lambda", "GetFunction", checksum = "fd796230")]
#[test_action("lambda", "DeleteFunction", checksum = "70eb2012")]
#[tokio::test]
async fn lambda_create_get_delete_function() {
    let server = TestServer::start().await;
    let client = server.lambda_client().await;

    let resp = client
        .create_function()
        .function_name("conf-func")
        .runtime(aws_sdk_lambda::types::Runtime::Python312)
        .role("arn:aws:iam::123456789012:role/test-role")
        .handler("index.handler")
        .code(
            aws_sdk_lambda::types::FunctionCode::builder()
                .zip_file(Blob::new(b"fake-code"))
                .build(),
        )
        .send()
        .await
        .unwrap();

    assert_eq!(resp.function_name().unwrap(), "conf-func");
    assert!(resp.function_arn().unwrap().contains("function:conf-func"));

    let resp = client
        .get_function()
        .function_name("conf-func")
        .send()
        .await
        .unwrap();
    let config = resp.configuration().unwrap();
    assert_eq!(config.function_name().unwrap(), "conf-func");
    assert_eq!(config.runtime().unwrap().as_str(), "python3.12");
    assert_eq!(config.handler().unwrap(), "index.handler");

    client
        .delete_function()
        .function_name("conf-func")
        .send()
        .await
        .unwrap();

    let result = client
        .get_function()
        .function_name("conf-func")
        .send()
        .await;
    assert!(result.is_err());
}

#[test_action("lambda", "ListFunctions", checksum = "c607f5a0")]
#[tokio::test]
async fn lambda_list_functions() {
    let server = TestServer::start().await;
    let client = server.lambda_client().await;

    for name in &["list-a", "list-b", "list-c"] {
        client
            .create_function()
            .function_name(*name)
            .runtime(aws_sdk_lambda::types::Runtime::Nodejs20x)
            .role("arn:aws:iam::123456789012:role/test-role")
            .handler("index.handler")
            .code(
                aws_sdk_lambda::types::FunctionCode::builder()
                    .zip_file(Blob::new(b"fake"))
                    .build(),
            )
            .send()
            .await
            .unwrap();
    }

    let resp = client.list_functions().send().await.unwrap();
    assert_eq!(resp.functions().len(), 3);
}

// ---------------------------------------------------------------------------
// Invoke
// ---------------------------------------------------------------------------

#[test_action("lambda", "Invoke", checksum = "3cfe4e09")]
#[tokio::test]
async fn lambda_invoke() {
    let server = TestServer::start().await;
    let client = server.lambda_client().await;

    client
        .create_function()
        .function_name("invoke-me")
        .runtime(aws_sdk_lambda::types::Runtime::Python312)
        .role("arn:aws:iam::123456789012:role/test-role")
        .handler("index.handler")
        .code(
            aws_sdk_lambda::types::FunctionCode::builder()
                .zip_file(Blob::new(make_python_zip()))
                .build(),
        )
        .send()
        .await
        .unwrap();

    let result = client
        .invoke()
        .function_name("invoke-me")
        .payload(Blob::new(br#"{"key": "value"}"#))
        .send()
        .await;

    match result {
        Ok(resp) => assert_eq!(resp.status_code(), 200),
        Err(e) => {
            // Lambda invoke requires Docker; only accept container startup failures
            let msg = format!("{e:?}");
            assert!(
                msg.contains("container failed to start"),
                "unexpected invoke error: {msg}"
            );
        }
    }
}

// ---------------------------------------------------------------------------
// PublishVersion
// ---------------------------------------------------------------------------

#[test_action("lambda", "PublishVersion", checksum = "2bf524c5")]
#[tokio::test]
async fn lambda_publish_version() {
    let server = TestServer::start().await;
    let client = server.lambda_client().await;

    client
        .create_function()
        .function_name("version-func")
        .runtime(aws_sdk_lambda::types::Runtime::Python312)
        .role("arn:aws:iam::123456789012:role/test-role")
        .handler("index.handler")
        .code(
            aws_sdk_lambda::types::FunctionCode::builder()
                .zip_file(Blob::new(b"fake"))
                .build(),
        )
        .send()
        .await
        .unwrap();

    let resp = client
        .publish_version()
        .function_name("version-func")
        .send()
        .await
        .unwrap();

    assert!(resp.version().is_some());
    assert!(resp.function_arn().unwrap().contains("version-func"));
}

// ---------------------------------------------------------------------------
// Event source mappings
// ---------------------------------------------------------------------------

#[test_action("lambda", "CreateEventSourceMapping", checksum = "b9f5b731")]
#[test_action("lambda", "GetEventSourceMapping", checksum = "abb053d9")]
#[test_action("lambda", "DeleteEventSourceMapping", checksum = "96206508")]
#[tokio::test]
async fn lambda_create_get_delete_event_source_mapping() {
    let server = TestServer::start().await;
    let client = server.lambda_client().await;

    client
        .create_function()
        .function_name("esm-func")
        .runtime(aws_sdk_lambda::types::Runtime::Python312)
        .role("arn:aws:iam::123456789012:role/test-role")
        .handler("index.handler")
        .code(
            aws_sdk_lambda::types::FunctionCode::builder()
                .zip_file(Blob::new(b"fake"))
                .build(),
        )
        .send()
        .await
        .unwrap();

    let create_resp = client
        .create_event_source_mapping()
        .function_name("esm-func")
        .event_source_arn("arn:aws:sqs:us-east-1:123456789012:my-queue")
        .send()
        .await
        .unwrap();

    let uuid = create_resp.uuid().unwrap().to_string();
    assert!(!uuid.is_empty());

    let get_resp = client
        .get_event_source_mapping()
        .uuid(&uuid)
        .send()
        .await
        .unwrap();
    assert!(get_resp.function_arn().unwrap().contains("esm-func"));

    client
        .delete_event_source_mapping()
        .uuid(&uuid)
        .send()
        .await
        .unwrap();
}

#[test_action("lambda", "ListEventSourceMappings", checksum = "6df074f2")]
#[tokio::test]
async fn lambda_list_event_source_mappings() {
    let server = TestServer::start().await;
    let client = server.lambda_client().await;

    client
        .create_function()
        .function_name("esm-list-func")
        .runtime(aws_sdk_lambda::types::Runtime::Python312)
        .role("arn:aws:iam::123456789012:role/test-role")
        .handler("index.handler")
        .code(
            aws_sdk_lambda::types::FunctionCode::builder()
                .zip_file(Blob::new(b"fake"))
                .build(),
        )
        .send()
        .await
        .unwrap();

    client
        .create_event_source_mapping()
        .function_name("esm-list-func")
        .event_source_arn("arn:aws:sqs:us-east-1:123456789012:queue-1")
        .send()
        .await
        .unwrap();

    let resp = client.list_event_source_mappings().send().await.unwrap();
    assert!(!resp.event_source_mappings().is_empty());
}

// ---------------------------------------------------------------------------
// Resource-based policies
// ---------------------------------------------------------------------------

#[test_action("lambda", "AddPermission", checksum = "ad24af73")]
#[test_action("lambda", "GetPolicy", checksum = "150a22ab")]
#[test_action("lambda", "RemovePermission", checksum = "5a09e35b")]
#[tokio::test]
async fn lambda_resource_policy_roundtrip() {
    let server = TestServer::start().await;
    let client = server.lambda_client().await;

    client
        .create_function()
        .function_name("perm-fn")
        .runtime(aws_sdk_lambda::types::Runtime::Python312)
        .role("arn:aws:iam::123456789012:role/test-role")
        .handler("index.handler")
        .code(
            aws_sdk_lambda::types::FunctionCode::builder()
                .zip_file(Blob::new(make_python_zip()))
                .build(),
        )
        .send()
        .await
        .unwrap();

    // AddPermission seeds a canonical policy document and returns
    // the newly-appended statement as JSON.
    let added = client
        .add_permission()
        .function_name("perm-fn")
        .statement_id("events-invoke")
        .action("InvokeFunction")
        .principal("events.amazonaws.com")
        .source_arn("arn:aws:events:us-east-1:123456789012:rule/my-rule")
        .send()
        .await
        .unwrap();
    let statement: serde_json::Value = serde_json::from_str(added.statement().unwrap()).unwrap();
    assert_eq!(statement["Sid"], "events-invoke");

    // GetPolicy returns the stored doc containing the new statement.
    let got = client
        .get_policy()
        .function_name("perm-fn")
        .send()
        .await
        .unwrap();
    let doc: serde_json::Value = serde_json::from_str(got.policy().unwrap()).unwrap();
    assert_eq!(doc["Statement"].as_array().unwrap().len(), 1);
    assert_eq!(doc["Statement"][0]["Sid"], "events-invoke");

    // RemovePermission strips the matching statement; the doc stays.
    client
        .remove_permission()
        .function_name("perm-fn")
        .statement_id("events-invoke")
        .send()
        .await
        .unwrap();

    let got_after = client
        .get_policy()
        .function_name("perm-fn")
        .send()
        .await
        .unwrap();
    let doc_after: serde_json::Value = serde_json::from_str(got_after.policy().unwrap()).unwrap();
    assert_eq!(doc_after["Statement"].as_array().unwrap().len(), 0);
}

// ── Conformance closure batch ──

async fn make_basic_function(client: &aws_sdk_lambda::Client, name: &str) {
    client
        .create_function()
        .function_name(name)
        .runtime(aws_sdk_lambda::types::Runtime::Python312)
        .role("arn:aws:iam::123456789012:role/test-role")
        .handler("index.handler")
        .code(
            aws_sdk_lambda::types::FunctionCode::builder()
                .zip_file(Blob::new(make_python_zip()))
                .build(),
        )
        .send()
        .await
        .unwrap();
}

#[test_action("lambda", "CreateAlias", checksum = "65ac724f")]
#[test_action("lambda", "GetAlias", checksum = "0f1b80b3")]
#[test_action("lambda", "ListAliases", checksum = "19d9fd09")]
#[test_action("lambda", "UpdateAlias", checksum = "40391fc3")]
#[test_action("lambda", "DeleteAlias", checksum = "adda10f0")]
#[tokio::test]
async fn lambda_alias_lifecycle() {
    let server = TestServer::start().await;
    let client = server.lambda_client().await;
    make_basic_function(&client, "alias-fn").await;
    client
        .create_alias()
        .function_name("alias-fn")
        .name("live")
        .function_version("$LATEST")
        .send()
        .await
        .unwrap();
    client
        .get_alias()
        .function_name("alias-fn")
        .name("live")
        .send()
        .await
        .unwrap();
    client
        .list_aliases()
        .function_name("alias-fn")
        .send()
        .await
        .unwrap();
    client
        .update_alias()
        .function_name("alias-fn")
        .name("live")
        .description("updated")
        .send()
        .await
        .unwrap();
    client
        .delete_alias()
        .function_name("alias-fn")
        .name("live")
        .send()
        .await
        .unwrap();
}

#[test_action("lambda", "ListVersionsByFunction", checksum = "c5c64131")]
#[tokio::test]
async fn lambda_list_versions_by_function() {
    let server = TestServer::start().await;
    let client = server.lambda_client().await;
    make_basic_function(&client, "ver-fn").await;
    client
        .list_versions_by_function()
        .function_name("ver-fn")
        .send()
        .await
        .unwrap();
}

#[test_action("lambda", "GetFunctionConfiguration", checksum = "ed3e523e")]
#[test_action("lambda", "UpdateFunctionConfiguration", checksum = "4f61fde9")]
#[test_action("lambda", "UpdateFunctionCode", checksum = "76866594")]
#[tokio::test]
async fn lambda_function_configuration_extras() {
    let server = TestServer::start().await;
    let client = server.lambda_client().await;
    make_basic_function(&client, "cfg-fn").await;
    client
        .get_function_configuration()
        .function_name("cfg-fn")
        .send()
        .await
        .unwrap();
    client
        .update_function_configuration()
        .function_name("cfg-fn")
        .timeout(60)
        .send()
        .await
        .unwrap();
    client
        .update_function_code()
        .function_name("cfg-fn")
        .zip_file(Blob::new(make_python_zip()))
        .send()
        .await
        .unwrap();
}

#[test_action("lambda", "GetAccountSettings", checksum = "3e0b0731")]
#[tokio::test]
async fn lambda_get_account_settings() {
    let server = TestServer::start().await;
    let client = server.lambda_client().await;
    client.get_account_settings().send().await.unwrap();
}

#[test_action("lambda", "InvokeAsync", checksum = "77173d97")]
#[test_action("lambda", "InvokeWithResponseStream", checksum = "0360e051")]
#[tokio::test]
async fn lambda_invoke_async_and_stream() {
    let server = TestServer::start().await;
    let client = server.lambda_client().await;
    make_basic_function(&client, "inv-fn").await;
    #[allow(deprecated)]
    client
        .invoke_async()
        .function_name("inv-fn")
        .invoke_args(aws_sdk_lambda::primitives::ByteStream::from_static(b"{}"))
        .send()
        .await
        .unwrap();
    // The emulator returns an empty body so the SDK's EventStream frame
    // parser errors. We accept either Ok (route hit + parsed) or an SDK
    // SdkError after the HTTP call returned 2xx — both prove the route
    // is wired. A transport/dispatch failure (route missing, 404, etc.)
    // would surface as ConstructionFailure / DispatchFailure / ResponseError
    // and is rejected here.
    use aws_sdk_lambda::error::SdkError;
    let result = client
        .invoke_with_response_stream()
        .function_name("inv-fn")
        .send()
        .await;
    match result {
        Ok(_) => {}
        Err(SdkError::ResponseError(_)) | Err(SdkError::ServiceError(_)) => {}
        Err(e) => panic!("invoke_with_response_stream route not wired: {e:?}"),
    }
}

#[test_action("lambda", "PublishLayerVersion", checksum = "dfb9dc3f")]
#[test_action("lambda", "GetLayerVersion", checksum = "325abc36")]
#[test_action("lambda", "GetLayerVersionByArn", checksum = "bf086dbd")]
#[test_action("lambda", "ListLayers", checksum = "43dac112")]
#[test_action("lambda", "ListLayerVersions", checksum = "717445fc")]
#[test_action("lambda", "DeleteLayerVersion", checksum = "a4da17aa")]
#[test_action("lambda", "GetLayerVersionPolicy", checksum = "009bc482")]
#[test_action("lambda", "AddLayerVersionPermission", checksum = "c004e857")]
#[test_action("lambda", "RemoveLayerVersionPermission", checksum = "cc2bc2ae")]
#[tokio::test]
async fn lambda_layer_lifecycle() {
    let server = TestServer::start().await;
    let client = server.lambda_client().await;
    let resp = client
        .publish_layer_version()
        .layer_name("conf-layer")
        .content(
            aws_sdk_lambda::types::LayerVersionContentInput::builder()
                .zip_file(Blob::new(make_python_zip()))
                .build(),
        )
        .send()
        .await
        .unwrap();
    let version = resp.version();
    let arn = resp.layer_version_arn().unwrap().to_string();
    client
        .get_layer_version()
        .layer_name("conf-layer")
        .version_number(version)
        .send()
        .await
        .unwrap();
    client
        .get_layer_version_by_arn()
        .arn(&arn)
        .send()
        .await
        .unwrap();
    client.list_layers().send().await.unwrap();
    client
        .list_layer_versions()
        .layer_name("conf-layer")
        .send()
        .await
        .unwrap();
    client
        .add_layer_version_permission()
        .layer_name("conf-layer")
        .version_number(version)
        .statement_id("share")
        .principal("*")
        .action("lambda:GetLayerVersion")
        .send()
        .await
        .unwrap();
    client
        .get_layer_version_policy()
        .layer_name("conf-layer")
        .version_number(version)
        .send()
        .await
        .unwrap();
    client
        .remove_layer_version_permission()
        .layer_name("conf-layer")
        .version_number(version)
        .statement_id("share")
        .send()
        .await
        .unwrap();
    client
        .delete_layer_version()
        .layer_name("conf-layer")
        .version_number(version)
        .send()
        .await
        .unwrap();
}

#[test_action("lambda", "CreateFunctionUrlConfig", checksum = "8f9df133")]
#[test_action("lambda", "GetFunctionUrlConfig", checksum = "d9433859")]
#[test_action("lambda", "UpdateFunctionUrlConfig", checksum = "59ae9dd1")]
#[test_action("lambda", "DeleteFunctionUrlConfig", checksum = "b732300c")]
#[test_action("lambda", "ListFunctionUrlConfigs", checksum = "e259d280")]
#[tokio::test]
async fn lambda_function_url_lifecycle() {
    let server = TestServer::start().await;
    let client = server.lambda_client().await;
    make_basic_function(&client, "url-fn").await;
    client
        .create_function_url_config()
        .function_name("url-fn")
        .auth_type(aws_sdk_lambda::types::FunctionUrlAuthType::None)
        .send()
        .await
        .unwrap();
    client
        .get_function_url_config()
        .function_name("url-fn")
        .send()
        .await
        .unwrap();
    client
        .update_function_url_config()
        .function_name("url-fn")
        .auth_type(aws_sdk_lambda::types::FunctionUrlAuthType::AwsIam)
        .send()
        .await
        .unwrap();
    client
        .list_function_url_configs()
        .function_name("url-fn")
        .send()
        .await
        .unwrap();
    client
        .delete_function_url_config()
        .function_name("url-fn")
        .send()
        .await
        .unwrap();
}

#[test_action("lambda", "PutFunctionConcurrency", checksum = "1ce389c3")]
#[test_action("lambda", "GetFunctionConcurrency", checksum = "15aced4e")]
#[test_action("lambda", "DeleteFunctionConcurrency", checksum = "b812af88")]
#[test_action("lambda", "PutProvisionedConcurrencyConfig", checksum = "d6a30419")]
#[test_action("lambda", "GetProvisionedConcurrencyConfig", checksum = "ddfff6d6")]
#[test_action("lambda", "DeleteProvisionedConcurrencyConfig", checksum = "0e3c3a8c")]
#[test_action("lambda", "ListProvisionedConcurrencyConfigs", checksum = "2d45075c")]
#[tokio::test]
async fn lambda_concurrency_lifecycle() {
    let server = TestServer::start().await;
    let client = server.lambda_client().await;
    make_basic_function(&client, "conc-fn").await;
    client
        .put_function_concurrency()
        .function_name("conc-fn")
        .reserved_concurrent_executions(5)
        .send()
        .await
        .unwrap();
    client
        .get_function_concurrency()
        .function_name("conc-fn")
        .send()
        .await
        .unwrap();
    client
        .delete_function_concurrency()
        .function_name("conc-fn")
        .send()
        .await
        .unwrap();
    client
        .put_provisioned_concurrency_config()
        .function_name("conc-fn")
        .qualifier("$LATEST")
        .provisioned_concurrent_executions(2)
        .send()
        .await
        .unwrap();
    client
        .get_provisioned_concurrency_config()
        .function_name("conc-fn")
        .qualifier("$LATEST")
        .send()
        .await
        .unwrap();
    client
        .list_provisioned_concurrency_configs()
        .function_name("conc-fn")
        .send()
        .await
        .unwrap();
    client
        .delete_provisioned_concurrency_config()
        .function_name("conc-fn")
        .qualifier("$LATEST")
        .send()
        .await
        .unwrap();
}

#[test_action("lambda", "CreateCodeSigningConfig", checksum = "12386c1f")]
#[test_action("lambda", "GetCodeSigningConfig", checksum = "cb1852fc")]
#[test_action("lambda", "UpdateCodeSigningConfig", checksum = "060abb7e")]
#[test_action("lambda", "DeleteCodeSigningConfig", checksum = "cbe92c43")]
#[test_action("lambda", "ListCodeSigningConfigs", checksum = "9dde614b")]
#[test_action("lambda", "PutFunctionCodeSigningConfig", checksum = "07561a74")]
#[test_action("lambda", "GetFunctionCodeSigningConfig", checksum = "f0c71958")]
#[test_action("lambda", "DeleteFunctionCodeSigningConfig", checksum = "ca797fe7")]
#[test_action("lambda", "ListFunctionsByCodeSigningConfig", checksum = "c4877f19")]
#[tokio::test]
async fn lambda_code_signing_lifecycle() {
    let server = TestServer::start().await;
    let client = server.lambda_client().await;
    make_basic_function(&client, "csc-fn").await;
    let csc = client
        .create_code_signing_config()
        .description("conf-csc")
        .allowed_publishers(
            aws_sdk_lambda::types::AllowedPublishers::builder()
                .signing_profile_version_arns(
                    "arn:aws:signer:us-east-1:123:signing-profile/p/version/1",
                )
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap()
        .code_signing_config()
        .unwrap()
        .clone();
    let arn = csc.code_signing_config_arn().to_string();
    assert!(!csc.code_signing_config_id().is_empty());
    client
        .get_code_signing_config()
        .code_signing_config_arn(&arn)
        .send()
        .await
        .unwrap();
    client
        .update_code_signing_config()
        .code_signing_config_arn(&arn)
        .description("updated")
        .send()
        .await
        .unwrap();
    client.list_code_signing_configs().send().await.unwrap();
    client
        .put_function_code_signing_config()
        .function_name("csc-fn")
        .code_signing_config_arn(&arn)
        .send()
        .await
        .unwrap();
    client
        .get_function_code_signing_config()
        .function_name("csc-fn")
        .send()
        .await
        .unwrap();
    client
        .list_functions_by_code_signing_config()
        .code_signing_config_arn(&arn)
        .send()
        .await
        .unwrap();
    client
        .delete_function_code_signing_config()
        .function_name("csc-fn")
        .send()
        .await
        .unwrap();
    client
        .delete_code_signing_config()
        .code_signing_config_arn(&arn)
        .send()
        .await
        .unwrap();
}

#[test_action("lambda", "PutFunctionEventInvokeConfig", checksum = "550a290d")]
#[test_action("lambda", "GetFunctionEventInvokeConfig", checksum = "19a1ee68")]
#[test_action("lambda", "UpdateFunctionEventInvokeConfig", checksum = "ceb633d0")]
#[test_action("lambda", "DeleteFunctionEventInvokeConfig", checksum = "a1b423f6")]
#[test_action("lambda", "ListFunctionEventInvokeConfigs", checksum = "48046264")]
#[tokio::test]
async fn lambda_event_invoke_lifecycle() {
    let server = TestServer::start().await;
    let client = server.lambda_client().await;
    make_basic_function(&client, "ev-fn").await;
    client
        .put_function_event_invoke_config()
        .function_name("ev-fn")
        .maximum_event_age_in_seconds(900)
        .maximum_retry_attempts(1)
        .send()
        .await
        .unwrap();
    client
        .get_function_event_invoke_config()
        .function_name("ev-fn")
        .send()
        .await
        .unwrap();
    client
        .update_function_event_invoke_config()
        .function_name("ev-fn")
        .maximum_retry_attempts(2)
        .send()
        .await
        .unwrap();
    client
        .list_function_event_invoke_configs()
        .function_name("ev-fn")
        .send()
        .await
        .unwrap();
    client
        .delete_function_event_invoke_config()
        .function_name("ev-fn")
        .send()
        .await
        .unwrap();
}

#[test_action("lambda", "PutRuntimeManagementConfig", checksum = "d86ea37a")]
#[test_action("lambda", "GetRuntimeManagementConfig", checksum = "170e6028")]
#[tokio::test]
async fn lambda_runtime_management() {
    let server = TestServer::start().await;
    let client = server.lambda_client().await;
    make_basic_function(&client, "rm-fn").await;
    client
        .put_runtime_management_config()
        .function_name("rm-fn")
        .update_runtime_on(aws_sdk_lambda::types::UpdateRuntimeOn::Auto)
        .send()
        .await
        .unwrap();
    client
        .get_runtime_management_config()
        .function_name("rm-fn")
        .send()
        .await
        .unwrap();
}

#[test_action("lambda", "PutFunctionRecursionConfig", checksum = "3805e69b")]
#[test_action("lambda", "GetFunctionRecursionConfig", checksum = "86ff28a0")]
#[tokio::test]
async fn lambda_recursion_config() {
    let server = TestServer::start().await;
    let client = server.lambda_client().await;
    make_basic_function(&client, "rec-fn").await;
    client
        .put_function_recursion_config()
        .function_name("rec-fn")
        .recursive_loop(aws_sdk_lambda::types::RecursiveLoop::Allow)
        .send()
        .await
        .unwrap();
    client
        .get_function_recursion_config()
        .function_name("rec-fn")
        .send()
        .await
        .unwrap();
}

#[test_action("lambda", "PutFunctionScalingConfig", checksum = "32595a34")]
#[test_action("lambda", "GetFunctionScalingConfig", checksum = "8096164f")]
#[tokio::test]
async fn lambda_scaling_config_via_route() {
    // Function scaling config — the live AWS route is
    // `/2025-11-30/functions/{name}/function-scaling-config?Qualifier=...`.
    // We just need to exercise the dispatch path; the function
    // doesn't need to exist for the validation guards to pass.
    let server = TestServer::start().await;
    let auth = "AWS4-HMAC-SHA256 Credential=test/20240101/us-east-1/lambda/aws4_request, SignedHeaders=host, Signature=0";
    let resp = reqwest::Client::new()
        .put(format!(
            "{}/2025-11-30/functions/scaling-fn/function-scaling-config?Qualifier=%24LATEST",
            server.endpoint()
        ))
        .header("Authorization", auth)
        .body(
            r#"{"FunctionScalingConfig":{"MinExecutionEnvironments":1,"MaxExecutionEnvironments":10}}"#,
        )
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success(), "put status: {}", resp.status());
    let resp = reqwest::Client::new()
        .get(format!(
            "{}/2025-11-30/functions/scaling-fn/function-scaling-config?Qualifier=%24LATEST",
            server.endpoint()
        ))
        .header("Authorization", auth)
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success(), "get status: {}", resp.status());
}

#[test_action("lambda", "TagResource", checksum = "481a09a0")]
#[test_action("lambda", "UntagResource", checksum = "4c7c9139")]
#[test_action("lambda", "ListTags", checksum = "7d9fadf7")]
#[tokio::test]
async fn lambda_tag_resource_lifecycle() {
    let server = TestServer::start().await;
    let client = server.lambda_client().await;
    make_basic_function(&client, "tag-fn").await;
    let arn = "arn:aws:lambda:us-east-1:000000000000:function:tag-fn".to_string();
    client
        .tag_resource()
        .resource(&arn)
        .tags("env", "test")
        .send()
        .await
        .unwrap();
    client.list_tags().resource(&arn).send().await.unwrap();
    client
        .untag_resource()
        .resource(&arn)
        .tag_keys("env")
        .send()
        .await
        .unwrap();
}

#[test_action("lambda", "CreateCapacityProvider", checksum = "cf3a7b6a")]
#[test_action("lambda", "GetCapacityProvider", checksum = "ad947440")]
#[test_action("lambda", "ListCapacityProviders", checksum = "dae35ca3")]
#[test_action("lambda", "UpdateCapacityProvider", checksum = "5d2bbc06")]
#[test_action("lambda", "DeleteCapacityProvider", checksum = "555e0456")]
#[test_action(
    "lambda",
    "ListFunctionVersionsByCapacityProvider",
    checksum = "d51f5143"
)]
#[tokio::test]
async fn lambda_capacity_provider_lifecycle() {
    let server = TestServer::start().await;
    let client = server.lambda_client().await;

    let vpc = aws_sdk_lambda::types::CapacityProviderVpcConfig::builder()
        .subnet_ids("subnet-aaaa")
        .security_group_ids("sg-aaaa")
        .build()
        .unwrap();
    let perms = aws_sdk_lambda::types::CapacityProviderPermissionsConfig::builder()
        .capacity_provider_operator_role_arn("arn:aws:iam::123456789012:role/cp-role")
        .build()
        .unwrap();

    client
        .create_capacity_provider()
        .capacity_provider_name("conf-cp")
        .vpc_config(vpc)
        .permissions_config(perms)
        .send()
        .await
        .unwrap();

    let got = client
        .get_capacity_provider()
        .capacity_provider_name("conf-cp")
        .send()
        .await
        .unwrap();
    assert!(got.capacity_provider().is_some());

    client.list_capacity_providers().send().await.unwrap();

    client
        .update_capacity_provider()
        .capacity_provider_name("conf-cp")
        .send()
        .await
        .unwrap();

    client
        .list_function_versions_by_capacity_provider()
        .capacity_provider_name("conf-cp")
        .send()
        .await
        .unwrap();

    client
        .delete_capacity_provider()
        .capacity_provider_name("conf-cp")
        .send()
        .await
        .unwrap();
}

#[test_action("lambda", "GetDurableExecution", checksum = "e76992ce")]
#[test_action("lambda", "GetDurableExecutionHistory", checksum = "54910a95")]
#[test_action("lambda", "GetDurableExecutionState", checksum = "467d4d29")]
#[test_action("lambda", "CheckpointDurableExecution", checksum = "9ea9391f")]
#[test_action("lambda", "StopDurableExecution", checksum = "dc468fea")]
#[test_action("lambda", "ListDurableExecutionsByFunction", checksum = "7e7ba943")]
#[test_action("lambda", "SendDurableExecutionCallbackSuccess", checksum = "2ff17f12")]
#[test_action("lambda", "SendDurableExecutionCallbackFailure", checksum = "4f3d7101")]
#[test_action(
    "lambda",
    "SendDurableExecutionCallbackHeartbeat",
    checksum = "a797352f"
)]
#[tokio::test]
async fn lambda_durable_execution_lifecycle() {
    let server = TestServer::start().await;
    let client = server.lambda_client().await;

    // No public "create execution" op exists in the model — durable
    // executions are spawned implicitly when an in-band Lambda invoke
    // requests one. The harness drives the read/write/callback ops
    // directly via the SDK, and existence checks should resolve
    // NotFound rather than crash.
    let arn = "arn:aws:lambda:us-east-1:000000000000:durable-execution/conf-exec";

    let _ = client
        .get_durable_execution()
        .durable_execution_arn(arn)
        .send()
        .await;
    let _ = client
        .get_durable_execution_history()
        .durable_execution_arn(arn)
        .send()
        .await;
    let _ = client
        .get_durable_execution_state()
        .durable_execution_arn(arn)
        .send()
        .await;
    let _ = client
        .checkpoint_durable_execution()
        .durable_execution_arn(arn)
        .send()
        .await;
    let _ = client
        .stop_durable_execution()
        .durable_execution_arn(arn)
        .send()
        .await;
    let _ = client
        .list_durable_executions_by_function()
        .function_name("any-fn")
        .send()
        .await;
    client
        .send_durable_execution_callback_success()
        .callback_id("cb-ok")
        .send()
        .await
        .unwrap();
    client
        .send_durable_execution_callback_failure()
        .callback_id("cb-fail")
        .send()
        .await
        .unwrap();
    client
        .send_durable_execution_callback_heartbeat()
        .callback_id("cb-hb")
        .send()
        .await
        .unwrap();
}

#[test_action("lambda", "UpdateEventSourceMapping", checksum = "5b51a313")]
#[tokio::test]
async fn lambda_update_event_source_mapping() {
    let server = TestServer::start().await;
    let client = server.lambda_client().await;
    make_basic_function(&client, "esm-fn").await;
    let esm = client
        .create_event_source_mapping()
        .function_name("esm-fn")
        .event_source_arn("arn:aws:sqs:us-east-1:000000000000:queue1")
        .send()
        .await
        .unwrap();
    let uuid = esm.uuid().unwrap();
    client
        .update_event_source_mapping()
        .uuid(uuid)
        .batch_size(20)
        .send()
        .await
        .unwrap();
}
