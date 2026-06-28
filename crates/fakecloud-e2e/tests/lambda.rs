mod helpers;

use std::io::Write;

use aws_sdk_lambda::primitives::Blob;
use helpers::TestServer;

fn make_python_zip() -> Vec<u8> {
    let buf = Vec::new();
    let cursor = std::io::Cursor::new(buf);
    let mut writer = zip::ZipWriter::new(cursor);
    let options = zip::write::SimpleFileOptions::default();
    writer.start_file("index.py", options).unwrap();
    writer
        .write_all(b"def handler(event, context):\n    return {\"statusCode\": 200}\n")
        .unwrap();
    let cursor = writer.finish().unwrap();
    cursor.into_inner()
}

#[tokio::test]
async fn lambda_create_get_delete_function() {
    let server = TestServer::start().await;
    let client = server.lambda_client().await;

    // Create
    let resp = client
        .create_function()
        .function_name("my-func")
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

    assert_eq!(resp.function_name().unwrap(), "my-func");
    assert!(resp.function_arn().unwrap().contains("function:my-func"));

    // Get
    let resp = client
        .get_function()
        .function_name("my-func")
        .send()
        .await
        .unwrap();
    let config = resp.configuration().unwrap();
    assert_eq!(config.function_name().unwrap(), "my-func");
    assert_eq!(config.runtime().unwrap().as_str(), "python3.12");
    assert_eq!(config.handler().unwrap(), "index.handler");

    // Delete
    client
        .delete_function()
        .function_name("my-func")
        .send()
        .await
        .unwrap();

    // Verify deleted
    let result = client.get_function().function_name("my-func").send().await;
    assert!(result.is_err());
}

#[tokio::test]
async fn lambda_get_function_code_location_is_downloadable() {
    // Regression for #1375: AWS Toolkit + `aws lambda get-function` need
    // Code.Location to resolve to the actual ZIP body.
    let server = TestServer::start().await;
    let client = server.lambda_client().await;

    let zip = make_python_zip();
    client
        .create_function()
        .function_name("dl-target")
        .runtime(aws_sdk_lambda::types::Runtime::Python312)
        .role("arn:aws:iam::123456789012:role/test-role")
        .handler("index.handler")
        .code(
            aws_sdk_lambda::types::FunctionCode::builder()
                .zip_file(Blob::new(zip.clone()))
                .build(),
        )
        .send()
        .await
        .unwrap();

    let resp = client
        .get_function()
        .function_name("dl-target")
        .send()
        .await
        .unwrap();
    let location = resp.code().unwrap().location().unwrap();
    assert!(
        location.contains("/_fakecloud/lambda/function-code/"),
        "Code.Location should point at fakecloud route, got {location}"
    );

    let body = reqwest::get(location)
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .bytes()
        .await
        .unwrap();
    assert_eq!(body.as_ref(), zip.as_slice());

    // Published version is downloadable through the same route.
    let publish = client
        .publish_version()
        .function_name("dl-target")
        .send()
        .await
        .unwrap();
    let v = publish.version().unwrap();
    let resp = client
        .get_function()
        .function_name("dl-target")
        .qualifier(v)
        .send()
        .await
        .unwrap();
    let location = resp.code().unwrap().location().unwrap();
    let body = reqwest::get(location)
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .bytes()
        .await
        .unwrap();
    assert_eq!(body.as_ref(), zip.as_slice());
}

#[tokio::test]
async fn lambda_get_function_accepts_arn_partial_arn_and_qualifier() {
    let server = TestServer::start().await;
    let client = server.lambda_client().await;

    client
        .create_function()
        .function_name("arn-target")
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

    // Full ARN — what the VS Code AWS Toolkit sends.
    let resp = client
        .get_function()
        .function_name("arn:aws:lambda:us-east-1:123456789012:function:arn-target")
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.configuration().unwrap().function_name().unwrap(),
        "arn-target"
    );

    // Partial ARN.
    let resp = client
        .get_function()
        .function_name("123456789012:function:arn-target")
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.configuration().unwrap().function_name().unwrap(),
        "arn-target"
    );

    // Bare name with version qualifier.
    let resp = client
        .get_function()
        .function_name("arn-target:1")
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.configuration().unwrap().function_name().unwrap(),
        "arn-target"
    );
}

#[tokio::test]
async fn lambda_list_functions() {
    let server = TestServer::start().await;
    let client = server.lambda_client().await;

    for name in &["func-a", "func-b", "func-c"] {
        client
            .create_function()
            .function_name(*name)
            .runtime(aws_sdk_lambda::types::Runtime::Nodejs20x)
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

    let resp = client.list_functions().send().await.unwrap();
    assert_eq!(resp.functions().len(), 3);
}

async fn invoke_with_cli(cli: &str) {
    let available = std::process::Command::new(cli)
        .arg("info")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false);

    if !available {
        if std::env::var("CI").is_ok() {
            panic!("{cli} is not available but is required in CI");
        }
        eprintln!("skipping: {cli} is not available");
        return;
    }

    let server = TestServer::start_with_env(&[("FAKECLOUD_CONTAINER_CLI", cli)]).await;
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

    let resp = client
        .invoke()
        .function_name("invoke-me")
        .payload(Blob::new(br#"{"key": "value"}"#))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status_code(), 200);
    let body: serde_json::Value = serde_json::from_slice(resp.payload().unwrap().as_ref()).unwrap();
    assert_eq!(body["statusCode"], 200);
}

#[tokio::test]
async fn lambda_invoke_docker() {
    invoke_with_cli("docker").await;
}

#[tokio::test]
async fn lambda_invoke_podman() {
    invoke_with_cli("podman").await;
}

#[tokio::test]
async fn lambda_create_function_conflict() {
    let server = TestServer::start().await;
    let client = server.lambda_client().await;

    client
        .create_function()
        .function_name("dup-func")
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

    // Creating again should fail
    let result = client
        .create_function()
        .function_name("dup-func")
        .runtime(aws_sdk_lambda::types::Runtime::Python312)
        .role("arn:aws:iam::123456789012:role/test-role")
        .handler("index.handler")
        .code(
            aws_sdk_lambda::types::FunctionCode::builder()
                .zip_file(Blob::new(make_python_zip()))
                .build(),
        )
        .send()
        .await;
    assert!(result.is_err());
}

#[tokio::test]
async fn lambda_add_get_remove_permission_roundtrip() {
    // Drives AddPermission / GetPolicy / RemovePermission through
    // aws-sdk-lambda against the real fakecloud binary — verifies the
    // canonical-policy-doc round trip we rely on in the IAM evaluator
    // path. FAKECLOUD_IAM is off for this test; it only exercises the
    // handler shape, not enforcement.
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

    // GetPolicy on a function with no resource policy -> ResourceNotFoundException.
    let missing = client.get_policy().function_name("perm-fn").send().await;
    assert!(
        missing.is_err(),
        "GetPolicy on unpolicied function should 404"
    );

    // AddPermission for EventBridge with SourceArn + SourceAccount.
    let add_resp = client
        .add_permission()
        .function_name("perm-fn")
        .statement_id("events-invoke")
        .action("InvokeFunction")
        .principal("events.amazonaws.com")
        .source_arn("arn:aws:events:us-east-1:123456789012:rule/my-rule")
        .source_account("123456789012")
        .send()
        .await
        .unwrap();
    let statement_str = add_resp.statement().unwrap();
    let statement: serde_json::Value = serde_json::from_str(statement_str).unwrap();
    assert_eq!(statement["Sid"], "events-invoke");
    assert_eq!(statement["Principal"]["Service"], "events.amazonaws.com");
    // Action is stored verbatim — caller passed `InvokeFunction`, so
    // the round-trip preserves that, matching real AWS behavior.
    assert_eq!(statement["Action"], "InvokeFunction");
    assert_eq!(
        statement["Condition"]["ArnLike"]["aws:SourceArn"],
        "arn:aws:events:us-east-1:123456789012:rule/my-rule"
    );

    // Add a second statement so RemovePermission has something to
    // leave behind after it strips the first one.
    client
        .add_permission()
        .function_name("perm-fn")
        .statement_id("s3-invoke")
        .action("InvokeFunction")
        .principal("s3.amazonaws.com")
        .send()
        .await
        .unwrap();

    // GetPolicy returns the composed document with both statements.
    let got = client
        .get_policy()
        .function_name("perm-fn")
        .send()
        .await
        .unwrap();
    let doc: serde_json::Value = serde_json::from_str(got.policy().unwrap()).unwrap();
    let statements = doc["Statement"].as_array().unwrap();
    assert_eq!(statements.len(), 2);
    let ids: Vec<&str> = statements
        .iter()
        .map(|s| s["Sid"].as_str().unwrap())
        .collect();
    assert!(ids.contains(&"events-invoke"));
    assert!(ids.contains(&"s3-invoke"));

    // RemovePermission strips only the named statement.
    client
        .remove_permission()
        .function_name("perm-fn")
        .statement_id("events-invoke")
        .send()
        .await
        .unwrap();

    let got = client
        .get_policy()
        .function_name("perm-fn")
        .send()
        .await
        .unwrap();
    let doc: serde_json::Value = serde_json::from_str(got.policy().unwrap()).unwrap();
    let statements = doc["Statement"].as_array().unwrap();
    assert_eq!(statements.len(), 1);
    assert_eq!(statements[0]["Sid"], "s3-invoke");

    // Removing a non-existent statement id is a 404.
    let err = client
        .remove_permission()
        .function_name("perm-fn")
        .statement_id("nope")
        .send()
        .await;
    assert!(err.is_err());
}

#[tokio::test]
async fn lambda_add_permission_with_qualified_action_no_double_prefix() {
    // Caller passes `lambda:InvokeFunction` (already fully qualified).
    // The stored policy should round-trip exactly that string back, with
    // no `lambda:lambda:InvokeFunction` double-prefix on read.
    let server = TestServer::start().await;
    let client = server.lambda_client().await;

    client
        .create_function()
        .function_name("qprefix-fn")
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

    client
        .add_permission()
        .function_name("qprefix-fn")
        .statement_id("with-prefix")
        .action("lambda:InvokeFunction")
        .principal("events.amazonaws.com")
        .send()
        .await
        .unwrap();

    let got = client
        .get_policy()
        .function_name("qprefix-fn")
        .send()
        .await
        .unwrap();
    let doc: serde_json::Value = serde_json::from_str(got.policy().unwrap()).unwrap();
    let stmts = doc["Statement"].as_array().unwrap();
    assert_eq!(stmts.len(), 1);
    // Round-trip preserves the qualified verb verbatim — no lambda:lambda: prefix.
    assert_eq!(stmts[0]["Action"], "lambda:InvokeFunction");
}

#[tokio::test]
async fn lambda_tag_list_untag_roundtrip() {
    // TagResource -> ListTagsForResource -> UntagResource end-to-end
    // against the real fakecloud binary via aws-sdk-lambda. Pins the
    // unified storage path: tags live on the function record, the
    // SDK's UntagResource (which sends `tagKeys` as a query parameter)
    // hits the right key, and DeleteFunction wipes them clean.
    let server = TestServer::start().await;
    let client = server.lambda_client().await;

    client
        .create_function()
        .function_name("tag-fn")
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

    let arn = "arn:aws:lambda:us-east-1:123456789012:function:tag-fn";

    // TagResource adds env=prod, team=core.
    client
        .tag_resource()
        .resource(arn)
        .tags("env", "prod")
        .tags("team", "core")
        .send()
        .await
        .unwrap();

    let listed = client.list_tags().resource(arn).send().await.unwrap();
    let tags = listed.tags().unwrap();
    assert_eq!(tags.get("env").map(String::as_str), Some("prod"));
    assert_eq!(tags.get("team").map(String::as_str), Some("core"));

    // UntagResource removes env, leaves team.
    client
        .untag_resource()
        .resource(arn)
        .tag_keys("env")
        .send()
        .await
        .unwrap();

    let listed = client.list_tags().resource(arn).send().await.unwrap();
    let tags = listed.tags().unwrap();
    assert!(!tags.contains_key("env"));
    assert_eq!(tags.get("team").map(String::as_str), Some("core"));

    // DeleteFunction wipes the function and (transitively) its tags.
    client
        .delete_function()
        .function_name("tag-fn")
        .send()
        .await
        .unwrap();

    // ListTagsForResource on the deleted function -> 404, confirming no
    // stale state.tags entry hangs around.
    let err = client.list_tags().resource(arn).send().await;
    assert!(err.is_err(), "ListTags after DeleteFunction must 404");
}

fn make_python_zip_returning(payload: &str) -> Vec<u8> {
    // A second-flavor zip whose handler returns a payload-derived value,
    // so callers can confirm UpdateFunctionCode actually swapped the code
    // bundle (rather than just the metadata).
    let buf = Vec::new();
    let cursor = std::io::Cursor::new(buf);
    let mut writer = zip::ZipWriter::new(cursor);
    let options = zip::write::SimpleFileOptions::default();
    writer.start_file("index.py", options).unwrap();
    writer
        .write_all(
            format!("def handler(event, context):\n    return {{\"payload\": \"{payload}\"}}\n")
                .as_bytes(),
        )
        .unwrap();
    let cursor = writer.finish().unwrap();
    cursor.into_inner()
}

#[tokio::test]
async fn lambda_update_function_code_replaces_zip_and_recomputes_hash() {
    // Fresh zip -> CodeSha256 + CodeSize must move; same zip again ->
    // RevisionId stays put. GetFunctionConfiguration round-trips the new
    // hash, proving the update persisted in state and not just the
    // immediate response.
    let server = TestServer::start().await;
    let client = server.lambda_client().await;

    let initial_zip = make_python_zip();
    client
        .create_function()
        .function_name("upd-code")
        .runtime(aws_sdk_lambda::types::Runtime::Python312)
        .role("arn:aws:iam::123456789012:role/r")
        .handler("index.handler")
        .code(
            aws_sdk_lambda::types::FunctionCode::builder()
                .zip_file(Blob::new(initial_zip.clone()))
                .build(),
        )
        .send()
        .await
        .unwrap();

    let pre = client
        .get_function_configuration()
        .function_name("upd-code")
        .send()
        .await
        .unwrap();
    let pre_sha = pre.code_sha256().unwrap().to_string();
    let pre_rev = pre.revision_id().unwrap().to_string();
    let pre_size = pre.code_size();
    assert_eq!(pre_size, initial_zip.len() as i64);

    // Replace with a different zip -- CodeSha256 + CodeSize must change,
    // RevisionId must rotate.
    let new_zip = make_python_zip_returning("v2");
    let updated = client
        .update_function_code()
        .function_name("upd-code")
        .zip_file(Blob::new(new_zip.clone()))
        .send()
        .await
        .unwrap();
    let post_sha = updated.code_sha256().unwrap().to_string();
    let post_rev = updated.revision_id().unwrap().to_string();
    assert_ne!(post_sha, pre_sha, "CodeSha256 should change");
    assert_ne!(post_rev, pre_rev, "RevisionId should rotate on real change");
    assert_eq!(updated.code_size(), new_zip.len() as i64);

    // Persisted in state.
    let cfg = client
        .get_function_configuration()
        .function_name("upd-code")
        .send()
        .await
        .unwrap();
    assert_eq!(cfg.code_sha256().unwrap(), post_sha);
    assert_eq!(cfg.code_size(), new_zip.len() as i64);

    // Same bytes again -> RevisionId must stay put.
    let same = client
        .update_function_code()
        .function_name("upd-code")
        .zip_file(Blob::new(new_zip.clone()))
        .send()
        .await
        .unwrap();
    assert_eq!(same.revision_id().unwrap(), post_rev);
    assert_eq!(same.code_sha256().unwrap(), post_sha);
}

#[tokio::test]
async fn lambda_update_function_code_with_s3_descriptor_rotates_hash() {
    // After R4 wired real S3 fetches, UpdateFunctionCode with S3Bucket+Key
    // pulls the object bytes. The hash now reflects the actual ZIP, so
    // seed the S3 bucket with the artifact before pointing Lambda at it.
    let server = TestServer::start().await;
    let lambda = server.lambda_client().await;
    let s3 = server.s3_client().await;

    s3.create_bucket()
        .bucket("deploy-bucket")
        .send()
        .await
        .unwrap();
    let v1 = make_python_zip();
    s3.put_object()
        .bucket("deploy-bucket")
        .key("lambdas/v2.zip")
        .body(aws_sdk_s3::primitives::ByteStream::from(v1.clone()))
        .send()
        .await
        .unwrap();

    lambda
        .create_function()
        .function_name("upd-s3")
        .runtime(aws_sdk_lambda::types::Runtime::Python312)
        .role("arn:aws:iam::123456789012:role/r")
        .handler("index.handler")
        .code(
            aws_sdk_lambda::types::FunctionCode::builder()
                .zip_file(Blob::new(v1.clone()))
                .build(),
        )
        .send()
        .await
        .unwrap();

    // Seed a second, different artifact for the S3-source update.
    let mut v2 = make_python_zip();
    v2.extend_from_slice(b"-v2-tail");
    s3.put_object()
        .bucket("deploy-bucket")
        .key("lambdas/v2.zip")
        .body(aws_sdk_s3::primitives::ByteStream::from(v2.clone()))
        .send()
        .await
        .unwrap();

    let pre_sha = lambda
        .get_function_configuration()
        .function_name("upd-s3")
        .send()
        .await
        .unwrap()
        .code_sha256()
        .unwrap()
        .to_string();

    let updated = lambda
        .update_function_code()
        .function_name("upd-s3")
        .s3_bucket("deploy-bucket")
        .s3_key("lambdas/v2.zip")
        .send()
        .await
        .unwrap();
    let post_sha = updated.code_sha256().unwrap().to_string();
    assert_ne!(post_sha, pre_sha);
}

#[tokio::test]
async fn lambda_update_function_code_with_image_uri_clears_size_and_sha() {
    // Real AWS reports CodeSize=0 and an empty CodeSha256 for image
    // functions; verify UpdateFunctionCode lines those fields up when
    // swapping to a new image URI.
    let server = TestServer::start().await;
    let client = server.lambda_client().await;

    client
        .create_function()
        .function_name("upd-img")
        .runtime(aws_sdk_lambda::types::Runtime::Python312)
        .role("arn:aws:iam::123456789012:role/r")
        .handler("index.handler")
        .package_type(aws_sdk_lambda::types::PackageType::Image)
        .code(
            aws_sdk_lambda::types::FunctionCode::builder()
                .image_uri("old.example.com/image:1")
                .build(),
        )
        .send()
        .await
        .unwrap();

    let updated = client
        .update_function_code()
        .function_name("upd-img")
        .image_uri("new.example.com/image:2")
        .send()
        .await
        .unwrap();
    assert_eq!(updated.code_size(), 0);
    assert_eq!(updated.code_sha256().unwrap_or(""), "");
    assert_eq!(
        updated.package_type().unwrap(),
        &aws_sdk_lambda::types::PackageType::Image
    );
}

fn make_zip_with(payload: &[u8]) -> Vec<u8> {
    // Each call returns different ZIP bytes when `payload` differs, which
    // bumps `CodeSha256` so PublishVersion stops being a no-op idempotent
    // re-publish on the second call. Real callers do this implicitly via
    // CI bumping the artifact between deploys.
    let buf = Vec::new();
    let cursor = std::io::Cursor::new(buf);
    let mut writer = zip::ZipWriter::new(cursor);
    let options = zip::write::SimpleFileOptions::default();
    writer.start_file("index.py", options).unwrap();
    writer.write_all(payload).unwrap();
    let cursor = writer.finish().unwrap();
    cursor.into_inner()
}

#[tokio::test]
async fn lambda_publish_version_snapshots_and_lists() {
    // End-to-end PublishVersion / ListVersionsByFunction / Get* with
    // Qualifier / DeleteFunction(Qualifier) / idempotent re-publish /
    // PreconditionFailedException on stale CodeSha256.
    let server = TestServer::start().await;
    let client = server.lambda_client().await;

    client
        .create_function()
        .function_name("ver-fn")
        .runtime(aws_sdk_lambda::types::Runtime::Python312)
        .role("arn:aws:iam::123456789012:role/test-role")
        .handler("index.handler")
        .code(
            aws_sdk_lambda::types::FunctionCode::builder()
                .zip_file(Blob::new(make_zip_with(b"v0\n")))
                .build(),
        )
        .send()
        .await
        .unwrap();

    // Newly-created function has $LATEST and no numbered versions.
    let listed = client
        .list_versions_by_function()
        .function_name("ver-fn")
        .send()
        .await
        .unwrap();
    let versions: Vec<String> = listed
        .versions()
        .iter()
        .map(|v| v.version().unwrap_or("").to_string())
        .collect();
    assert_eq!(versions, vec!["$LATEST".to_string()]);

    // PublishVersion returns Version="1" with FunctionArn ending in :1.
    let v1 = client
        .publish_version()
        .function_name("ver-fn")
        .send()
        .await
        .unwrap();
    assert_eq!(v1.version().unwrap(), "1");
    assert!(v1.function_arn().unwrap().ends_with(":1"));
    let v1_sha = v1.code_sha256().unwrap().to_string();

    // GetFunction(Qualifier="1") returns the v1 snapshot config.
    let v1_get = client
        .get_function()
        .function_name("ver-fn")
        .qualifier("1")
        .send()
        .await
        .unwrap();
    let v1_cfg = v1_get.configuration().unwrap();
    assert_eq!(v1_cfg.version().unwrap(), "1");
    assert!(v1_cfg.function_arn().unwrap().ends_with(":1"));
    assert_eq!(v1_cfg.code_sha256().unwrap(), v1_sha);

    // Mutate $LATEST: UpdateFunctionConfiguration on description must
    // not mutate the v1 snapshot.
    client
        .update_function_configuration()
        .function_name("ver-fn")
        .description("after-v1")
        .send()
        .await
        .unwrap();

    let v1_recheck = client
        .get_function_configuration()
        .function_name("ver-fn")
        .qualifier("1")
        .send()
        .await
        .unwrap();
    assert_eq!(v1_recheck.description().unwrap_or(""), "");
    assert_eq!(v1_recheck.code_sha256().unwrap(), v1_sha);

    // PublishVersion with same code+description is idempotent: returns v1.
    // We reset the description to keep parity with the v1 snapshot first.
    client
        .update_function_configuration()
        .function_name("ver-fn")
        .description("")
        .send()
        .await
        .unwrap();
    let again = client
        .publish_version()
        .function_name("ver-fn")
        .send()
        .await
        .unwrap();
    assert_eq!(again.version().unwrap(), "1");

    // UpdateFunctionCode with new bytes -> bumps $LATEST sha.
    let new_zip = make_zip_with(b"v2-payload\n");
    client
        .update_function_code()
        .function_name("ver-fn")
        .zip_file(Blob::new(new_zip))
        .send()
        .await
        .unwrap();

    // Stale CodeSha256 precondition -> 412 PreconditionFailedException.
    let stale = client
        .publish_version()
        .function_name("ver-fn")
        .code_sha256(v1_sha.clone())
        .send()
        .await;
    assert!(stale.is_err(), "expected PreconditionFailedException");
    let err_str = format!("{:?}", stale.err().unwrap());
    assert!(
        err_str.contains("PreconditionFailed"),
        "expected PreconditionFailed, got {err_str}"
    );

    // PublishVersion without preconditions -> v2.
    let v2 = client
        .publish_version()
        .function_name("ver-fn")
        .send()
        .await
        .unwrap();
    assert_eq!(v2.version().unwrap(), "2");
    assert!(v2.function_arn().unwrap().ends_with(":2"));
    assert_ne!(v2.code_sha256().unwrap(), v1_sha);

    // ListVersionsByFunction returns 3 entries: $LATEST, 1, 2 with
    // full FunctionConfiguration each (not just version strings).
    let listed = client
        .list_versions_by_function()
        .function_name("ver-fn")
        .send()
        .await
        .unwrap();
    let entries = listed.versions();
    assert_eq!(entries.len(), 3);
    let versions: Vec<&str> = entries.iter().map(|v| v.version().unwrap_or("")).collect();
    assert_eq!(versions, vec!["$LATEST", "1", "2"]);
    // Each entry carries a Runtime / Handler / Role — i.e. a full
    // FunctionConfiguration, not just a version label.
    for v in entries {
        assert_eq!(v.runtime().unwrap().as_str(), "python3.12");
        assert_eq!(v.handler().unwrap(), "index.handler");
    }

    // DeleteFunction(Qualifier="1") drops only that version.
    client
        .delete_function()
        .function_name("ver-fn")
        .qualifier("1")
        .send()
        .await
        .unwrap();

    let listed = client
        .list_versions_by_function()
        .function_name("ver-fn")
        .send()
        .await
        .unwrap();
    let versions: Vec<&str> = listed
        .versions()
        .iter()
        .map(|v| v.version().unwrap_or(""))
        .collect();
    assert_eq!(versions, vec!["$LATEST", "2"]);

    // GetFunction(Qualifier="1") now 404s.
    let missing_v1 = client
        .get_function()
        .function_name("ver-fn")
        .qualifier("1")
        .send()
        .await;
    assert!(missing_v1.is_err());

    // DeleteFunction without Qualifier removes everything.
    client
        .delete_function()
        .function_name("ver-fn")
        .send()
        .await
        .unwrap();
    assert!(client
        .get_function()
        .function_name("ver-fn")
        .send()
        .await
        .is_err());
}

#[tokio::test]
async fn lambda_alias_targets_published_version_snapshot() {
    // Aliases pointing at a numbered version must resolve to the
    // immutable snapshot at GetFunction time even after $LATEST is
    // rewritten — a key reason version snapshots exist at all.
    let server = TestServer::start().await;
    let client = server.lambda_client().await;

    client
        .create_function()
        .function_name("alias-fn")
        .runtime(aws_sdk_lambda::types::Runtime::Python312)
        .role("arn:aws:iam::123456789012:role/test-role")
        .handler("index.handler")
        .code(
            aws_sdk_lambda::types::FunctionCode::builder()
                .zip_file(Blob::new(make_zip_with(b"alias-v1\n")))
                .build(),
        )
        .send()
        .await
        .unwrap();

    let v1 = client
        .publish_version()
        .function_name("alias-fn")
        .send()
        .await
        .unwrap();
    assert_eq!(v1.version().unwrap(), "1");
    let v1_sha = v1.code_sha256().unwrap().to_string();

    // Create alias prod -> 1.
    client
        .create_alias()
        .function_name("alias-fn")
        .name("prod")
        .function_version("1")
        .send()
        .await
        .unwrap();

    // Mutate $LATEST.
    client
        .update_function_code()
        .function_name("alias-fn")
        .zip_file(Blob::new(make_zip_with(b"alias-v2\n")))
        .send()
        .await
        .unwrap();

    // GetFunction(Qualifier="prod") must hit the v1 snapshot, not $LATEST.
    let via_alias = client
        .get_function()
        .function_name("alias-fn")
        .qualifier("prod")
        .send()
        .await
        .unwrap();
    let cfg = via_alias.configuration().unwrap();
    assert_eq!(cfg.code_sha256().unwrap(), v1_sha);
    assert!(cfg.function_arn().unwrap().ends_with(":1"));
}

#[tokio::test]
async fn lambda_get_alias_wire_shape_and_routing_removal() {
    let server = TestServer::start().await;
    let client = server.lambda_client().await;

    client
        .create_function()
        .function_name("rc-fn")
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
    client
        .publish_version()
        .function_name("rc-fn")
        .send()
        .await
        .unwrap();
    client
        .update_function_code()
        .function_name("rc-fn")
        .zip_file(Blob::new(make_zip_with(b"v2\n")))
        .send()
        .await
        .unwrap();
    client
        .publish_version()
        .function_name("rc-fn")
        .send()
        .await
        .unwrap();

    // Create alias with a routing config (90/10 between v1 and v2).
    client
        .create_alias()
        .function_name("rc-fn")
        .name("live")
        .function_version("1")
        .routing_config(
            aws_sdk_lambda::types::AliasRoutingConfiguration::builder()
                .additional_version_weights("2", 0.1)
                .build(),
        )
        .send()
        .await
        .unwrap();

    // GetAlias must return the AWS wire shape (PascalCase fields parse into the
    // typed SDK struct) including the routing config.
    let got = client
        .get_alias()
        .function_name("rc-fn")
        .name("live")
        .send()
        .await
        .expect("get alias");
    assert_eq!(got.name(), Some("live"));
    assert_eq!(got.function_version(), Some("1"));
    assert!(got.alias_arn().unwrap().ends_with(":live"));
    assert!(got.revision_id().is_some());
    assert_eq!(
        got.routing_config()
            .and_then(|r| r.additional_version_weights())
            .and_then(|w| w.get("2"))
            .copied(),
        Some(0.1)
    );

    // Removing the routing config (empty) must clear it, not leave it present.
    client
        .update_alias()
        .function_name("rc-fn")
        .name("live")
        .routing_config(aws_sdk_lambda::types::AliasRoutingConfiguration::builder().build())
        .send()
        .await
        .unwrap();
    let after = client
        .get_alias()
        .function_name("rc-fn")
        .name("live")
        .send()
        .await
        .unwrap();
    assert!(
        after
            .routing_config()
            .map(|r| r.additional_version_weights().is_none_or(|w| w.is_empty()))
            .unwrap_or(true),
        "routing config must be cleared after removal"
    );
}

#[tokio::test]
async fn lambda_get_layer_version_returns_layer_arn() {
    let server = TestServer::start().await;
    let client = server.lambda_client().await;

    let published = client
        .publish_layer_version()
        .layer_name("mylayer")
        .content(
            aws_sdk_lambda::types::LayerVersionContentInput::builder()
                .zip_file(Blob::new(make_python_zip()))
                .build(),
        )
        .send()
        .await
        .expect("publish layer");
    let version = published.version();

    // GetLayerVersion must echo LayerArn (the Terraform resource reads it).
    let got = client
        .get_layer_version()
        .layer_name("mylayer")
        .version_number(version)
        .send()
        .await
        .expect("get layer version");
    assert_eq!(
        got.layer_arn(),
        Some("arn:aws:lambda:us-east-1:123456789012:layer:mylayer")
    );
}

#[tokio::test]
async fn lambda_event_invoke_config_omits_unset_max_age() {
    let server = TestServer::start().await;
    let client = server.lambda_client().await;

    client
        .create_function()
        .function_name("eic-fn")
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

    // Set only the retry attempts; leave MaximumEventAgeInSeconds unset.
    client
        .put_function_event_invoke_config()
        .function_name("eic-fn")
        .maximum_retry_attempts(1)
        .send()
        .await
        .unwrap();

    let got = client
        .get_function_event_invoke_config()
        .function_name("eic-fn")
        .send()
        .await
        .unwrap();
    assert_eq!(got.maximum_retry_attempts(), Some(1));
    // AWS does not synthesise a default age, so it comes back unset.
    assert!(got.maximum_event_age_in_seconds().is_none());
}

#[tokio::test]
async fn lambda_get_function_returns_default_logging_config() {
    let server = TestServer::start().await;
    let client = server.lambda_client().await;

    client
        .create_function()
        .function_name("lg-fn")
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

    let got = client
        .get_function()
        .function_name("lg-fn")
        .send()
        .await
        .unwrap();
    let lc = got
        .configuration()
        .and_then(|c| c.logging_config())
        .expect("default logging config");
    assert_eq!(
        lc.log_format(),
        Some(&aws_sdk_lambda::types::LogFormat::Text)
    );
    assert_eq!(lc.log_group(), Some("/aws/lambda/lg-fn"));
}

#[tokio::test]
async fn lambda_add_permission_round_trips_event_source_token() {
    let server = TestServer::start().await;
    let client = server.lambda_client().await;

    client
        .create_function()
        .function_name("est-fn")
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

    client
        .add_permission()
        .function_name("est-fn")
        .statement_id("s1")
        .action("lambda:InvokeFunction")
        .principal("events.amazonaws.com")
        .event_source_token("my-token")
        .send()
        .await
        .unwrap();

    let policy = client
        .get_policy()
        .function_name("est-fn")
        .send()
        .await
        .unwrap();
    let doc: serde_json::Value = serde_json::from_str(policy.policy().unwrap()).unwrap();
    let cond = &doc["Statement"][0]["Condition"]["StringEquals"]["lambda:EventSourceToken"];
    assert_eq!(cond.as_str(), Some("my-token"));
}

#[tokio::test]
async fn lambda_runtime_management_config_404s_after_function_delete() {
    let server = TestServer::start().await;
    let client = server.lambda_client().await;

    client
        .create_function()
        .function_name("rmc-fn")
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

    // While the function exists, GetRuntimeManagementConfig returns the config.
    client
        .get_runtime_management_config()
        .function_name("rmc-fn")
        .send()
        .await
        .expect("config while function exists");

    client
        .delete_function()
        .function_name("rmc-fn")
        .send()
        .await
        .unwrap();

    // Once the function is gone it must 404 (the Terraform CheckDestroy relies
    // on this), not synthesise a default config.
    let err = client
        .get_runtime_management_config()
        .function_name("rmc-fn")
        .send()
        .await
        .expect_err("must 404 after function delete");
    assert!(err.into_service_error().is_resource_not_found_exception());
}

// bug-audit 2026-06-27, T1.4: GetFunctionConcurrency on a function with no
// reserved concurrency returns an empty body, not ReservedConcurrentExecutions:0
// (0 means "throttle to zero" in Lambda, not "unset").
#[tokio::test]
async fn lambda_get_function_concurrency_unset_is_empty() {
    let server = TestServer::start().await;
    let client = server.lambda_client().await;

    client
        .create_function()
        .function_name("conc-func")
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

    let unset = client
        .get_function_concurrency()
        .function_name("conc-func")
        .send()
        .await
        .unwrap();
    assert_eq!(
        unset.reserved_concurrent_executions(),
        None,
        "unset reserved concurrency is absent, not 0"
    );

    // After putting a real value, it round-trips.
    client
        .put_function_concurrency()
        .function_name("conc-func")
        .reserved_concurrent_executions(5)
        .send()
        .await
        .unwrap();
    let set = client
        .get_function_concurrency()
        .function_name("conc-func")
        .send()
        .await
        .unwrap();
    assert_eq!(set.reserved_concurrent_executions(), Some(5));
}

// bug-audit 2026-06-27, T1.13: ListFunctions(FunctionVersion=ALL) returns the
// published numbered versions too (not only $LATEST), and the alias/version
// list MaxItems range is 1..10000 (no spurious 400 above 50).
#[tokio::test]
async fn lambda_list_functions_all_versions_and_maxitems() {
    let server = TestServer::start().await;
    let client = server.lambda_client().await;

    client
        .create_function()
        .function_name("verfn")
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
    client
        .publish_version()
        .function_name("verfn")
        .send()
        .await
        .unwrap();
    // Change $LATEST so the next publish creates a distinct version (AWS dedupes
    // an unchanged publish).
    client
        .update_function_configuration()
        .function_name("verfn")
        .description("v2")
        .send()
        .await
        .unwrap();
    client
        .publish_version()
        .function_name("verfn")
        .send()
        .await
        .unwrap();

    let all = client
        .list_functions()
        .function_version(aws_sdk_lambda::types::FunctionVersion::All)
        .send()
        .await
        .unwrap();
    let versions: Vec<&str> = all
        .functions()
        .iter()
        .filter(|f| f.function_name() == Some("verfn"))
        .map(|f| f.version().unwrap())
        .collect();
    assert!(
        versions.contains(&"$LATEST") && versions.contains(&"1") && versions.contains(&"2"),
        "ListFunctions(ALL) lists $LATEST + published versions, got {versions:?}"
    );

    // MaxItems above 50 must be accepted for ListVersionsByFunction.
    client
        .list_versions_by_function()
        .function_name("verfn")
        .max_items(100)
        .send()
        .await
        .expect("MaxItems=100 accepted for ListVersionsByFunction");
    // ...and for ListAliases.
    client
        .list_aliases()
        .function_name("verfn")
        .max_items(100)
        .send()
        .await
        .expect("MaxItems=100 accepted for ListAliases");
}

// bug-audit 2026-06-27, T1.13: GetAccountSettings must decrement
// UnreservedConcurrentExecutions by the concurrency reserved on functions.
#[tokio::test]
async fn lambda_account_settings_unreserved_concurrency_decrements() {
    let server = TestServer::start().await;
    let client = server.lambda_client().await;

    let before = client.get_account_settings().send().await.unwrap();
    let limit = before.account_limit().unwrap().concurrent_executions();
    assert_eq!(
        before
            .account_limit()
            .unwrap()
            .unreserved_concurrent_executions(),
        Some(limit),
        "no reservations -> unreserved equals the limit"
    );

    client
        .create_function()
        .function_name("reserved-fn")
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
    client
        .put_function_concurrency()
        .function_name("reserved-fn")
        .reserved_concurrent_executions(100)
        .send()
        .await
        .unwrap();

    let after = client.get_account_settings().send().await.unwrap();
    assert_eq!(
        after
            .account_limit()
            .unwrap()
            .unreserved_concurrent_executions(),
        Some(limit - 100),
        "100 reserved -> unreserved drops by 100"
    );
}
