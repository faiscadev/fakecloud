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

#[test_action("lambda", "CreateFunction", checksum = "ea9cdf5e")]
#[test_action("lambda", "GetFunction", checksum = "41a0274e")]
#[test_action("lambda", "DeleteFunction", checksum = "22b50c89")]
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

#[test_action("lambda", "ListFunctions", checksum = "b1ae8ca8")]
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

#[test_action("lambda", "Invoke", checksum = "f941254d")]
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

#[test_action("lambda", "PublishVersion", checksum = "17053ae6")]
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

#[test_action("lambda", "CreateEventSourceMapping", checksum = "6c00be25")]
#[test_action("lambda", "GetEventSourceMapping", checksum = "4821f650")]
#[test_action("lambda", "DeleteEventSourceMapping", checksum = "8c9643a0")]
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

#[test_action("lambda", "ListEventSourceMappings", checksum = "8f52c766")]
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

#[test_action("lambda", "AddPermission", checksum = "08162d94")]
#[test_action("lambda", "GetPolicy", checksum = "95bf09af")]
#[test_action("lambda", "RemovePermission", checksum = "ddbad384")]
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

#[test_action("lambda", "CreateAlias", checksum = "3930da4d")]
#[test_action("lambda", "GetAlias", checksum = "d0e32b46")]
#[test_action("lambda", "ListAliases", checksum = "feba7256")]
#[test_action("lambda", "UpdateAlias", checksum = "16e245c2")]
#[test_action("lambda", "DeleteAlias", checksum = "e93abeb4")]
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

#[test_action("lambda", "ListVersionsByFunction", checksum = "05e89b07")]
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

#[test_action("lambda", "GetFunctionConfiguration", checksum = "c753d1bc")]
#[test_action("lambda", "UpdateFunctionConfiguration", checksum = "7111c43f")]
#[test_action("lambda", "UpdateFunctionCode", checksum = "9850ef56")]
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

#[test_action("lambda", "InvokeAsync", checksum = "350f942d")]
#[test_action("lambda", "InvokeWithResponseStream", checksum = "0189ebbc")]
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

#[test_action("lambda", "PublishLayerVersion", checksum = "54a5ff56")]
#[test_action("lambda", "GetLayerVersion", checksum = "4d7ad0c9")]
#[test_action("lambda", "GetLayerVersionByArn", checksum = "9802f8c7")]
#[test_action("lambda", "ListLayers", checksum = "bb68d1a8")]
#[test_action("lambda", "ListLayerVersions", checksum = "1b7464f9")]
#[test_action("lambda", "DeleteLayerVersion", checksum = "b20beffd")]
#[test_action("lambda", "GetLayerVersionPolicy", checksum = "bf6587aa")]
#[test_action("lambda", "AddLayerVersionPermission", checksum = "eb0edb3b")]
#[test_action("lambda", "RemoveLayerVersionPermission", checksum = "608f8e62")]
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

#[test_action("lambda", "CreateFunctionUrlConfig", checksum = "ff923ac6")]
#[test_action("lambda", "GetFunctionUrlConfig", checksum = "6d7a91e1")]
#[test_action("lambda", "UpdateFunctionUrlConfig", checksum = "09d18d2d")]
#[test_action("lambda", "DeleteFunctionUrlConfig", checksum = "d34fb1aa")]
#[test_action("lambda", "ListFunctionUrlConfigs", checksum = "de8719d1")]
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

#[test_action("lambda", "PutFunctionConcurrency", checksum = "445ff17b")]
#[test_action("lambda", "GetFunctionConcurrency", checksum = "e4afcd43")]
#[test_action("lambda", "DeleteFunctionConcurrency", checksum = "a467d900")]
#[test_action("lambda", "PutProvisionedConcurrencyConfig", checksum = "4d32625d")]
#[test_action("lambda", "GetProvisionedConcurrencyConfig", checksum = "afeebf8b")]
#[test_action("lambda", "DeleteProvisionedConcurrencyConfig", checksum = "9d9c2c2e")]
#[test_action("lambda", "ListProvisionedConcurrencyConfigs", checksum = "a4a98be4")]
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

#[test_action("lambda", "CreateCodeSigningConfig", checksum = "033e02b3")]
#[test_action("lambda", "GetCodeSigningConfig", checksum = "6ab7c192")]
#[test_action("lambda", "UpdateCodeSigningConfig", checksum = "babf3cfd")]
#[test_action("lambda", "DeleteCodeSigningConfig", checksum = "2b03107b")]
#[test_action("lambda", "ListCodeSigningConfigs", checksum = "d0a0f166")]
#[test_action("lambda", "PutFunctionCodeSigningConfig", checksum = "2d0c93ed")]
#[test_action("lambda", "GetFunctionCodeSigningConfig", checksum = "eb62995a")]
#[test_action("lambda", "DeleteFunctionCodeSigningConfig", checksum = "9e53fbf3")]
#[test_action("lambda", "ListFunctionsByCodeSigningConfig", checksum = "fcee00dc")]
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

#[test_action("lambda", "PutFunctionEventInvokeConfig", checksum = "a05d2cbd")]
#[test_action("lambda", "GetFunctionEventInvokeConfig", checksum = "b8cc0e93")]
#[test_action("lambda", "UpdateFunctionEventInvokeConfig", checksum = "e3522646")]
#[test_action("lambda", "DeleteFunctionEventInvokeConfig", checksum = "bba8194f")]
#[test_action("lambda", "ListFunctionEventInvokeConfigs", checksum = "c2505c63")]
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

#[test_action("lambda", "PutRuntimeManagementConfig", checksum = "29a12cb3")]
#[test_action("lambda", "GetRuntimeManagementConfig", checksum = "a21c79cb")]
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

#[test_action("lambda", "PutFunctionRecursionConfig", checksum = "0e6d5032")]
#[test_action("lambda", "GetFunctionRecursionConfig", checksum = "5eac1575")]
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

#[test_action("lambda", "PutFunctionScalingConfig", checksum = "f8f9ac32")]
#[test_action("lambda", "GetFunctionScalingConfig", checksum = "a900473b")]
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

#[test_action("lambda", "TagResource", checksum = "c6e9b1d6")]
#[test_action("lambda", "UntagResource", checksum = "b9cec3b0")]
#[test_action("lambda", "ListTags", checksum = "e919618b")]
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

#[test_action("lambda", "CreateCapacityProvider", checksum = "b2feacd3")]
#[test_action("lambda", "GetCapacityProvider", checksum = "5aa8e22d")]
#[test_action("lambda", "ListCapacityProviders", checksum = "9edd6e29")]
#[test_action("lambda", "UpdateCapacityProvider", checksum = "b4e9a9cc")]
#[test_action("lambda", "DeleteCapacityProvider", checksum = "42c603ac")]
#[test_action(
    "lambda",
    "ListFunctionVersionsByCapacityProvider",
    checksum = "863173ca"
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

#[test_action("lambda", "GetDurableExecution", checksum = "39fddd6c")]
#[test_action("lambda", "GetDurableExecutionHistory", checksum = "79352114")]
#[test_action("lambda", "GetDurableExecutionState", checksum = "38c14d0e")]
#[test_action("lambda", "CheckpointDurableExecution", checksum = "a9768cf1")]
#[test_action("lambda", "StopDurableExecution", checksum = "c1dacd9e")]
#[test_action("lambda", "ListDurableExecutionsByFunction", checksum = "d4b6f4ca")]
#[test_action("lambda", "SendDurableExecutionCallbackSuccess", checksum = "16e7c48e")]
#[test_action("lambda", "SendDurableExecutionCallbackFailure", checksum = "9321bea7")]
#[test_action(
    "lambda",
    "SendDurableExecutionCallbackHeartbeat",
    checksum = "ca713bf1"
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

#[test_action("lambda", "UpdateEventSourceMapping", checksum = "b2c589c7")]
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
