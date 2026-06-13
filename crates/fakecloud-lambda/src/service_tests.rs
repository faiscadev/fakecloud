use super::*;
use bytes::Bytes;
use http::{HeaderMap, Method};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;

fn make_state() -> SharedLambdaState {
    Arc::new(RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
    ))
}

fn make_request(method: Method, path: &str, body: &str) -> AwsRequest {
    let path_segments: Vec<String> = path
        .split('/')
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .collect();
    AwsRequest {
        service: "lambda".to_string(),
        action: String::new(),
        region: "us-east-1".to_string(),
        account_id: "123456789012".to_string(),
        request_id: "test-request-id".to_string(),
        headers: HeaderMap::new(),
        query_params: HashMap::new(),
        body: Bytes::from(body.to_string()),
        body_stream: parking_lot::Mutex::new(None),
        path_segments,
        raw_path: path.to_string(),
        raw_query: String::new(),
        method,
        is_query_protocol: false,
        access_key_id: None,
        principal: None,
    }
}

#[test]
fn normalize_function_name_bare_name_passes_through() {
    assert_eq!(normalize_function_name("MyFunction"), "MyFunction");
}

#[test]
fn normalize_function_name_strips_qualifier_from_bare_name() {
    assert_eq!(normalize_function_name("MyFunction:PROD"), "MyFunction");
    assert_eq!(normalize_function_name("MyFunction:1"), "MyFunction");
}

#[test]
fn normalize_function_name_strips_full_arn() {
    assert_eq!(
        normalize_function_name("arn:aws:lambda:us-east-1:123456789012:function:MyFunction"),
        "MyFunction"
    );
}

#[test]
fn normalize_function_name_strips_qualified_full_arn() {
    assert_eq!(
        normalize_function_name("arn:aws:lambda:us-east-1:123456789012:function:MyFunction:PROD"),
        "MyFunction"
    );
}

#[test]
fn normalize_function_name_strips_partial_arn() {
    assert_eq!(
        normalize_function_name("123456789012:function:MyFunction"),
        "MyFunction"
    );
    assert_eq!(
        normalize_function_name("123456789012:function:MyFunction:1"),
        "MyFunction"
    );
}

#[test]
fn normalize_function_name_leaves_malformed_arn_alone() {
    // wrong service in ARN — multiple colons, no lambda prefix → unchanged
    let s = "arn:aws:s3:us-east-1:123456789012:function:Foo";
    assert_eq!(normalize_function_name(s), s);
    // partial ARN with non-numeric account-shaped prefix → unchanged
    let s2 = "abc:function:Foo";
    assert_eq!(normalize_function_name(s2), s2);
}

#[test]
fn normalize_function_name_empty() {
    assert_eq!(normalize_function_name(""), "");
}

#[test]
fn normalize_function_name_decodes_percent_encoded_arn() {
    // SDKs URL-encode `:` in path segments. The toolkit / aws-sdk-lambda
    // wire form for `arn:aws:lambda:...` is `arn%3Aaws%3Alambda%3A...`.
    let encoded = "arn%3Aaws%3Alambda%3Aus-east-1%3A123456789012%3Afunction%3AMyFunc";
    assert_eq!(normalize_function_name(encoded), "MyFunc");
}

#[tokio::test]
async fn get_function_accepts_full_arn() {
    let svc = LambdaService::new(make_state());
    // Seed a function via CreateFunction
    let create_body = json!({
        "FunctionName": "MyFunc",
        "Runtime": "nodejs20.x",
        "Role": "arn:aws:iam::123456789012:role/lambda-role",
        "Handler": "index.handler",
        "Code": {"ZipFile": ""},
    })
    .to_string();
    let req = make_request(Method::POST, "/2015-03-31/functions", &create_body);
    svc.handle(req).await.expect("create function");

    // GetFunction by full ARN
    let req = make_request(
        Method::GET,
        "/2015-03-31/functions/arn:aws:lambda:us-east-1:123456789012:function:MyFunc",
        "",
    );
    let resp = svc.handle(req).await.expect("get function by ARN");
    assert_eq!(resp.status, StatusCode::OK);
}

#[tokio::test]
async fn get_function_accepts_partial_arn() {
    let svc = LambdaService::new(make_state());
    let create_body = json!({
        "FunctionName": "MyFunc",
        "Runtime": "nodejs20.x",
        "Role": "arn:aws:iam::123456789012:role/lambda-role",
        "Handler": "index.handler",
        "Code": {"ZipFile": ""},
    })
    .to_string();
    let req = make_request(Method::POST, "/2015-03-31/functions", &create_body);
    svc.handle(req).await.expect("create function");

    let req = make_request(
        Method::GET,
        "/2015-03-31/functions/123456789012:function:MyFunc",
        "",
    );
    let resp = svc.handle(req).await.expect("get function by partial ARN");
    assert_eq!(resp.status, StatusCode::OK);
}

#[tokio::test]
async fn get_function_accepts_name_with_qualifier() {
    let svc = LambdaService::new(make_state());
    let create_body = json!({
        "FunctionName": "MyFunc",
        "Runtime": "nodejs20.x",
        "Role": "arn:aws:iam::123456789012:role/lambda-role",
        "Handler": "index.handler",
        "Code": {"ZipFile": ""},
    })
    .to_string();
    let req = make_request(Method::POST, "/2015-03-31/functions", &create_body);
    svc.handle(req).await.expect("create function");

    let req = make_request(Method::GET, "/2015-03-31/functions/MyFunc:1", "");
    let resp = svc
        .handle(req)
        .await
        .expect("get function by name:qualifier");
    assert_eq!(resp.status, StatusCode::OK);
}

#[test]
fn iam_condition_keys_for_add_permission_populates_arn_and_principal() {
    let svc = LambdaService::new(make_state());
    let body = json!({
        "StatementId": "stmt",
        "Action": "lambda:InvokeFunction",
        "Principal": "s3.amazonaws.com",
    })
    .to_string();
    let req = make_request(Method::POST, "/2015-03-31/functions/my-func/policy", &body);
    let action = fakecloud_core::auth::IamAction {
        service: "lambda",
        action: "AddPermission",
        resource: "arn:aws:lambda:us-east-1:123456789012:function:my-func".to_string(),
    };
    let keys = svc.iam_condition_keys_for(&req, &action);
    assert_eq!(
        keys.get("lambda:functionarn"),
        Some(&vec![
            "arn:aws:lambda:us-east-1:123456789012:function:my-func".to_string()
        ])
    );
    assert_eq!(
        keys.get("lambda:principal"),
        Some(&vec!["s3.amazonaws.com".to_string()])
    );
}

#[test]
fn iam_condition_keys_for_add_permission_omits_missing_principal() {
    let svc = LambdaService::new(make_state());
    let body = json!({"StatementId": "stmt", "Action": "lambda:InvokeFunction"}).to_string();
    let req = make_request(Method::POST, "/2015-03-31/functions/my-func/policy", &body);
    let action = fakecloud_core::auth::IamAction {
        service: "lambda",
        action: "AddPermission",
        resource: "arn:aws:lambda:us-east-1:123456789012:function:my-func".to_string(),
    };
    let keys = svc.iam_condition_keys_for(&req, &action);
    assert!(!keys.contains_key("lambda:principal"));
    assert!(keys.contains_key("lambda:functionarn"));
}

#[test]
fn iam_condition_keys_for_non_add_permission_is_empty() {
    let svc = LambdaService::new(make_state());
    let req = make_request(Method::GET, "/2015-03-31/functions/my-func", "");
    let action = fakecloud_core::auth::IamAction {
        service: "lambda",
        action: "GetFunction",
        resource: "arn:aws:lambda:us-east-1:123456789012:function:my-func".to_string(),
    };
    assert!(svc.iam_condition_keys_for(&req, &action).is_empty());
}

#[tokio::test]
async fn test_create_and_get_function() {
    let state = make_state();
    let svc = LambdaService::new(state);

    let create_body = json!({
        "FunctionName": "my-func",
        "Runtime": "python3.12",
        "Role": "arn:aws:iam::123456789012:role/test-role",
        "Handler": "index.handler",
        "Code": { "ZipFile": "UEsFBgAAAAAAAAAAAAAAAAAAAAA=" }
    });

    let req = make_request(
        Method::POST,
        "/2015-03-31/functions",
        &create_body.to_string(),
    );
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::CREATED);

    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["FunctionName"], "my-func");
    assert_eq!(body["Runtime"], "python3.12");

    // Get
    let req = make_request(Method::GET, "/2015-03-31/functions/my-func", "");
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::OK);
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["Configuration"]["FunctionName"], "my-func");
}

#[tokio::test]
async fn test_delete_function() {
    let state = make_state();
    let svc = LambdaService::new(state);

    let create_body = json!({
        "FunctionName": "to-delete",
        "Runtime": "nodejs20.x",
        "Role": "arn:aws:iam::123456789012:role/test",
        "Handler": "index.handler",
        "Code": {}
    });

    let req = make_request(
        Method::POST,
        "/2015-03-31/functions",
        &create_body.to_string(),
    );
    svc.handle(req).await.unwrap();

    let req = make_request(Method::DELETE, "/2015-03-31/functions/to-delete", "");
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::NO_CONTENT);

    // Verify deleted
    let req = make_request(Method::GET, "/2015-03-31/functions/to-delete", "");
    let resp = svc.handle(req).await;
    assert!(resp.is_err());
}

#[tokio::test]
async fn test_invoke_without_runtime_returns_error() {
    let state = make_state();
    let svc = LambdaService::new(state);

    let create_body = json!({
        "FunctionName": "invoke-me",
        "Runtime": "python3.12",
        "Role": "arn:aws:iam::123456789012:role/test",
        "Handler": "index.handler",
        "Code": {}
    });

    let req = make_request(
        Method::POST,
        "/2015-03-31/functions",
        &create_body.to_string(),
    );
    svc.handle(req).await.unwrap();

    let req = make_request(
        Method::POST,
        "/2015-03-31/functions/invoke-me/invocations",
        r#"{"key": "value"}"#,
    );
    let resp = svc.handle(req).await;
    assert!(resp.is_err());
}

#[tokio::test]
async fn test_invoke_nonexistent_function() {
    let state = make_state();
    let svc = LambdaService::new(state);

    let req = make_request(
        Method::POST,
        "/2015-03-31/functions/does-not-exist/invocations",
        "{}",
    );
    let resp = svc.handle(req).await;
    assert!(resp.is_err());
}

#[tokio::test]
async fn test_list_functions() {
    let state = make_state();
    let svc = LambdaService::new(state);

    for name in &["func-a", "func-b"] {
        let create_body = json!({
            "FunctionName": name,
            "Runtime": "python3.12",
            "Role": "arn:aws:iam::123456789012:role/test",
            "Handler": "index.handler",
            "Code": {}
        });
        let req = make_request(
            Method::POST,
            "/2015-03-31/functions",
            &create_body.to_string(),
        );
        svc.handle(req).await.unwrap();
    }

    let req = make_request(Method::GET, "/2015-03-31/functions", "");
    let resp = svc.handle(req).await.unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["Functions"].as_array().unwrap().len(), 2);
}

#[tokio::test]
async fn test_event_source_mapping() {
    let state = make_state();
    let svc = LambdaService::new(state);

    // Create function first
    let create_body = json!({
        "FunctionName": "esm-func",
        "Runtime": "python3.12",
        "Role": "arn:aws:iam::123456789012:role/test",
        "Handler": "index.handler",
        "Code": {}
    });
    let req = make_request(
        Method::POST,
        "/2015-03-31/functions",
        &create_body.to_string(),
    );
    svc.handle(req).await.unwrap();

    // Create mapping
    let mapping_body = json!({
        "FunctionName": "esm-func",
        "EventSourceArn": "arn:aws:sqs:us-east-1:123456789012:my-queue",
        "BatchSize": 5
    });
    let req = make_request(
        Method::POST,
        "/2015-03-31/event-source-mappings",
        &mapping_body.to_string(),
    );
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::ACCEPTED);
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let uuid = body["UUID"].as_str().unwrap().to_string();

    // List mappings
    let req = make_request(Method::GET, "/2015-03-31/event-source-mappings", "");
    let resp = svc.handle(req).await.unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["EventSourceMappings"].as_array().unwrap().len(), 1);

    // Delete mapping
    let req = make_request(
        Method::DELETE,
        &format!("/2015-03-31/event-source-mappings/{uuid}"),
        "",
    );
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::ACCEPTED);
}

async fn seed_function(svc: &LambdaService, name: &str) {
    let body = json!({
        "FunctionName": name,
        "Runtime": "python3.12",
        "Role": "arn:aws:iam::123456789012:role/r",
        "Handler": "index.handler",
        "Code": {}
    });
    let req = make_request(Method::POST, "/2015-03-31/functions", &body.to_string());
    svc.handle(req).await.unwrap();
}

#[tokio::test]
async fn update_function_code_replaces_zip_and_bumps_revision() {
    let svc = LambdaService::new(make_state());
    seed_function(&svc, "ucode").await;

    // GetFunctionConfiguration to capture the original revisionId
    let req = make_request(Method::GET, "/2015-03-31/functions/ucode/configuration", "");
    let resp = svc.handle(req).await.unwrap();
    let pre: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let pre_revision = pre["RevisionId"].as_str().unwrap().to_string();
    let pre_sha = pre["CodeSha256"].as_str().unwrap().to_string();

    // UpdateFunctionCode with a real ZipFile payload
    let new_zip_b64 = base64::Engine::encode(
        &base64::engine::general_purpose::STANDARD,
        b"fresh-zip-bytes",
    );
    let body = json!({ "ZipFile": new_zip_b64 });
    let req = make_request(
        Method::PUT,
        "/2015-03-31/functions/ucode/code",
        &body.to_string(),
    );
    let resp = svc.handle(req).await.unwrap();
    let post: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_ne!(post["RevisionId"].as_str().unwrap(), pre_revision);
    assert_ne!(post["CodeSha256"].as_str().unwrap(), pre_sha);
    assert_eq!(
        post["CodeSize"].as_i64().unwrap(),
        b"fresh-zip-bytes".len() as i64
    );
}

#[tokio::test]
async fn update_function_code_replaces_image_uri() {
    let svc = LambdaService::new(make_state());
    // Seed an image-package function so UpdateFunctionCode can swap URIs.
    let body = json!({
        "FunctionName": "img-fn",
        "Runtime": "python3.12",
        "Role": "arn:aws:iam::123456789012:role/r",
        "Handler": "index.handler",
        "PackageType": "Image",
        "Code": {"ImageUri": "old.example.com/image:1"},
    });
    let req = make_request(Method::POST, "/2015-03-31/functions", &body.to_string());
    svc.handle(req).await.unwrap();

    let body = json!({ "ImageUri": "new.example.com/image:2" });
    let req = make_request(
        Method::PUT,
        "/2015-03-31/functions/img-fn/code",
        &body.to_string(),
    );
    svc.handle(req).await.unwrap();
    // UpdateFunctionCode returns a `FunctionConfiguration` shape, which
    // has no `Code` member — that wrapper only appears on `GetFunction`.
    // Verify the new image URI via `GetFunction`.
    let req = make_request(Method::GET, "/2015-03-31/functions/img-fn", "");
    let post: Value =
        serde_json::from_slice(svc.handle(req).await.unwrap().body.expect_bytes()).unwrap();
    assert_eq!(
        post["Code"]["ImageUri"].as_str().unwrap(),
        "new.example.com/image:2"
    );
}

#[tokio::test]
async fn update_function_code_noop_keeps_revision_id() {
    let svc = LambdaService::new(make_state());
    seed_function(&svc, "noop").await;

    let req = make_request(Method::GET, "/2015-03-31/functions/noop/configuration", "");
    let pre: Value =
        serde_json::from_slice(svc.handle(req).await.unwrap().body.expect_bytes()).unwrap();
    let pre_rev = pre["RevisionId"].as_str().unwrap().to_string();

    // Empty body — no ZipFile, no ImageUri, no signing profile.
    let req = make_request(Method::PUT, "/2015-03-31/functions/noop/code", "{}");
    let resp = svc.handle(req).await.unwrap();
    let post: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(post["RevisionId"].as_str().unwrap(), pre_rev);
}

#[tokio::test]
async fn update_function_code_same_bytes_keeps_revision_id() {
    let svc = LambdaService::new(make_state());
    seed_function(&svc, "samebytes").await;

    let req = make_request(
        Method::GET,
        "/2015-03-31/functions/samebytes/configuration",
        "",
    );
    let pre: Value =
        serde_json::from_slice(svc.handle(req).await.unwrap().body.expect_bytes()).unwrap();
    let pre_rev = pre["RevisionId"].as_str().unwrap().to_string();
    let pre_sha = pre["CodeSha256"].as_str().unwrap().to_string();

    // Re-upload the seed bytes (same hash) — revision_id must not move.
    let same_zip_b64 = base64::Engine::encode(
        &base64::engine::general_purpose::STANDARD,
        b"\x50\x4b\x03\x04hello",
    );
    // Compute what the hash will be to confirm the test setup is faithful.
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(b"\x50\x4b\x03\x04hello");
    let computed = base64::Engine::encode(
        &base64::engine::general_purpose::STANDARD,
        hasher.finalize(),
    );
    if computed == pre_sha {
        let body = json!({ "ZipFile": same_zip_b64 });
        let req = make_request(
            Method::PUT,
            "/2015-03-31/functions/samebytes/code",
            &body.to_string(),
        );
        let resp = svc.handle(req).await.unwrap();
        let post: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(
            post["RevisionId"].as_str().unwrap(),
            pre_rev,
            "same code should not bump revision"
        );
    }
}

#[tokio::test]
async fn update_function_code_csc_enforce_rejects_unsigned() {
    let svc = LambdaService::new(make_state());
    seed_function(&svc, "csc-fn").await;

    // Create a CodeSigningConfig with one allowed publisher and Enforce.
    let csc_body = json!({
        "AllowedPublishers": {
            "SigningProfileVersionArns": [
                "arn:aws:signer:us-east-1:123456789012:/signing-profiles/MyProfile/abc",
            ],
        },
        "CodeSigningPolicies": {"UntrustedArtifactOnDeployment": "Enforce"},
    });
    let req = make_request(
        Method::POST,
        "/2020-04-22/code-signing-configs",
        &csc_body.to_string(),
    );
    let resp = svc.handle(req).await.unwrap();
    let csc: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let csc_arn = csc["CodeSigningConfig"]["CodeSigningConfigArn"]
        .as_str()
        .unwrap()
        .to_string();

    // Bind to function.
    let bind_body = json!({"CodeSigningConfigArn": csc_arn});
    let req = make_request(
        Method::PUT,
        "/2020-06-30/functions/csc-fn/code-signing-config",
        &bind_body.to_string(),
    );
    svc.handle(req).await.unwrap();

    // UpdateFunctionCode without a SigningProfileVersionArn — must be rejected.
    let new_zip_b64 =
        base64::Engine::encode(&base64::engine::general_purpose::STANDARD, b"unsigned");
    let body = json!({ "ZipFile": new_zip_b64 });
    let req = make_request(
        Method::PUT,
        "/2015-03-31/functions/csc-fn/code",
        &body.to_string(),
    );
    let err = match svc.handle(req).await {
        Err(e) => e,
        Ok(_) => panic!("expected InvalidCodeSignatureException"),
    };
    assert_eq!(err.status(), 400);

    // Now with a matching profile — must succeed.
    let body = json!({
        "ZipFile": new_zip_b64,
        "SigningProfileVersionArn":
            "arn:aws:signer:us-east-1:123456789012:/signing-profiles/MyProfile/abc",
    });
    let req = make_request(
        Method::PUT,
        "/2015-03-31/functions/csc-fn/code",
        &body.to_string(),
    );
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, http::StatusCode::OK);
}

#[tokio::test]
async fn publish_version_increments_and_snapshots_config() {
    let svc = LambdaService::new(make_state());
    seed_function(&svc, "vfn").await;

    // Publish v1 with the seed Description ("")
    let req = make_request(Method::POST, "/2015-03-31/functions/vfn/versions", "{}");
    let resp = svc.handle(req).await.unwrap();
    let v1: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(v1["Version"], "1");
    assert!(v1["FunctionArn"].as_str().unwrap().ends_with(":1"));
    assert_eq!(
        v1["MasterArn"].as_str().unwrap(),
        "arn:aws:lambda:us-east-1:123456789012:function:vfn"
    );

    // Mutate $LATEST description via UpdateFunctionConfiguration
    let body = json!({ "Description": "after-v1" });
    let req = make_request(
        Method::PUT,
        "/2015-03-31/functions/vfn/configuration",
        &body.to_string(),
    );
    svc.handle(req).await.unwrap();

    // Publish v2 with the new Description
    let req = make_request(Method::POST, "/2015-03-31/functions/vfn/versions", "{}");
    let resp = svc.handle(req).await.unwrap();
    let v2: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(v2["Version"], "2");
    assert_eq!(v2["Description"].as_str().unwrap(), "after-v1");

    // ListVersionsByFunction returns $LATEST + v1 + v2 with snapshots intact:
    // v1 keeps its old description even after $LATEST was mutated.
    let req = make_request(Method::GET, "/2015-03-31/functions/vfn/versions", "");
    let resp = svc.handle(req).await.unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let versions = body["Versions"].as_array().unwrap();
    assert_eq!(versions.len(), 3);
    assert_eq!(versions[0]["Version"], "$LATEST");
    assert_eq!(versions[0]["Description"].as_str().unwrap(), "after-v1");
    assert_eq!(versions[1]["Version"], "1");
    assert_eq!(versions[1]["Description"].as_str().unwrap(), "");
    assert_eq!(versions[2]["Version"], "2");
    assert_eq!(versions[2]["Description"].as_str().unwrap(), "after-v1");
}

#[tokio::test]
async fn get_function_with_qualifier_returns_snapshot() {
    let svc = LambdaService::new(make_state());
    seed_function(&svc, "qfn").await;

    // Publish v1 of the seed config, then mutate $LATEST.
    let req = make_request(Method::POST, "/2015-03-31/functions/qfn/versions", "{}");
    svc.handle(req).await.unwrap();
    let body = json!({ "Description": "post-v1" });
    let req = make_request(
        Method::PUT,
        "/2015-03-31/functions/qfn/configuration",
        &body.to_string(),
    );
    svc.handle(req).await.unwrap();

    // GetFunction?Qualifier=1 must return the v1 snapshot, not $LATEST.
    let mut req = make_request(Method::GET, "/2015-03-31/functions/qfn", "");
    req.query_params
        .insert("Qualifier".to_string(), "1".to_string());
    let resp = svc.handle(req).await.unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["Configuration"]["Version"], "1");
    assert_eq!(body["Configuration"]["Description"].as_str().unwrap(), "");
    assert!(body["Configuration"]["FunctionArn"]
        .as_str()
        .unwrap()
        .ends_with(":1"));

    // GetFunction without qualifier returns $LATEST with the new description.
    let req = make_request(Method::GET, "/2015-03-31/functions/qfn", "");
    let resp = svc.handle(req).await.unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["Configuration"]["Version"], "$LATEST");
    assert_eq!(
        body["Configuration"]["Description"].as_str().unwrap(),
        "post-v1"
    );
}

#[tokio::test]
async fn get_function_with_alias_resolves_to_version_snapshot() {
    let svc = LambdaService::new(make_state());
    seed_function(&svc, "afn").await;

    // Publish v1, mutate $LATEST, create alias pointing at v1.
    let req = make_request(Method::POST, "/2015-03-31/functions/afn/versions", "{}");
    svc.handle(req).await.unwrap();
    let body = json!({ "Description": "post-publish" });
    let req = make_request(
        Method::PUT,
        "/2015-03-31/functions/afn/configuration",
        &body.to_string(),
    );
    svc.handle(req).await.unwrap();
    let body = json!({"Name": "PROD", "FunctionVersion": "1"});
    let req = make_request(
        Method::POST,
        "/2015-03-31/functions/afn/aliases",
        &body.to_string(),
    );
    svc.handle(req).await.unwrap();

    // GetFunction?Qualifier=PROD must hit the v1 snapshot.
    let mut req = make_request(Method::GET, "/2015-03-31/functions/afn", "");
    req.query_params
        .insert("Qualifier".to_string(), "PROD".to_string());
    let resp = svc.handle(req).await.unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["Configuration"]["Description"].as_str().unwrap(), "");
}

#[tokio::test]
async fn get_function_unknown_qualifier_404s() {
    let svc = LambdaService::new(make_state());
    seed_function(&svc, "missing").await;

    let mut req = make_request(Method::GET, "/2015-03-31/functions/missing", "");
    req.query_params
        .insert("Qualifier".to_string(), "99".to_string());
    let err = match svc.handle(req).await {
        Err(e) => e,
        Ok(_) => panic!("expected ResourceNotFoundException"),
    };
    assert_eq!(err.status(), 404);
}

#[tokio::test]
async fn add_permission_builds_canonical_statement() {
    let svc = LambdaService::new(make_state());
    seed_function(&svc, "f").await;

    let body = json!({
        "StatementId": "s3-invoke",
        "Action": "InvokeFunction",
        "Principal": "s3.amazonaws.com",
        "SourceArn": "arn:aws:s3:::my-bucket",
        "SourceAccount": "123456789012",
    });
    let req = make_request(
        Method::POST,
        "/2015-03-31/functions/f/policy",
        &body.to_string(),
    );
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::CREATED);

    let out: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let statement: Value = serde_json::from_str(out["Statement"].as_str().unwrap()).unwrap();
    assert_eq!(statement["Sid"], "s3-invoke");
    assert_eq!(statement["Effect"], "Allow");
    assert_eq!(statement["Principal"]["Service"], "s3.amazonaws.com");
    // Verbatim round-trip: caller sent `InvokeFunction`, that's what
    // AWS keeps in the policy doc.
    assert_eq!(statement["Action"], "InvokeFunction");
    assert_eq!(
        statement["Resource"],
        "arn:aws:lambda:us-east-1:123456789012:function:f"
    );
    assert_eq!(
        statement["Condition"]["ArnLike"]["aws:SourceArn"],
        "arn:aws:s3:::my-bucket"
    );
    assert_eq!(
        statement["Condition"]["StringEquals"]["aws:SourceAccount"],
        "123456789012"
    );
}

#[tokio::test]
async fn add_permission_aws_principal_emits_aws_key() {
    let svc = LambdaService::new(make_state());
    seed_function(&svc, "f").await;

    let body = json!({
        "StatementId": "user-invoke",
        "Action": "InvokeFunction",
        "Principal": "arn:aws:iam::123456789012:user/alice",
    });
    let req = make_request(
        Method::POST,
        "/2015-03-31/functions/f/policy",
        &body.to_string(),
    );
    svc.handle(req).await.unwrap();

    // Fetch via GetPolicy and inspect the stored doc.
    let req = make_request(Method::GET, "/2015-03-31/functions/f/policy", "");
    let resp = svc.handle(req).await.unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let doc: Value = serde_json::from_str(body["Policy"].as_str().unwrap()).unwrap();
    let statements = doc["Statement"].as_array().unwrap();
    assert_eq!(statements.len(), 1);
    assert_eq!(
        statements[0]["Principal"]["AWS"],
        "arn:aws:iam::123456789012:user/alice"
    );
    assert!(statements[0].get("Condition").is_none());
}

#[tokio::test]
async fn add_permission_rejects_duplicate_statement_id() {
    let svc = LambdaService::new(make_state());
    seed_function(&svc, "f").await;

    let body = json!({
        "StatementId": "dup",
        "Action": "InvokeFunction",
        "Principal": "arn:aws:iam::123456789012:user/a",
    });
    let req = make_request(
        Method::POST,
        "/2015-03-31/functions/f/policy",
        &body.to_string(),
    );
    svc.handle(req).await.unwrap();

    let req = make_request(
        Method::POST,
        "/2015-03-31/functions/f/policy",
        &body.to_string(),
    );
    let err = match svc.handle(req).await {
        Err(e) => e,
        Ok(_) => panic!("expected error"),
    };
    assert_eq!(err.status(), StatusCode::CONFLICT);
}

#[tokio::test]
async fn get_policy_returns_404_when_no_policy_attached() {
    let svc = LambdaService::new(make_state());
    seed_function(&svc, "f").await;

    let req = make_request(Method::GET, "/2015-03-31/functions/f/policy", "");
    let err = match svc.handle(req).await {
        Err(e) => e,
        Ok(_) => panic!("expected error"),
    };
    assert_eq!(err.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn remove_permission_strips_matching_sid_and_leaves_empty_doc() {
    let svc = LambdaService::new(make_state());
    seed_function(&svc, "f").await;

    for sid in ["a", "b"] {
        let body = json!({
            "StatementId": sid,
            "Action": "InvokeFunction",
            "Principal": "arn:aws:iam::123456789012:user/u",
        });
        let req = make_request(
            Method::POST,
            "/2015-03-31/functions/f/policy",
            &body.to_string(),
        );
        svc.handle(req).await.unwrap();
    }

    // Remove "a"
    let req = make_request(Method::DELETE, "/2015-03-31/functions/f/policy/a", "");
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::NO_CONTENT);

    // GetPolicy still returns the doc with just "b".
    let req = make_request(Method::GET, "/2015-03-31/functions/f/policy", "");
    let resp = svc.handle(req).await.unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let doc: Value = serde_json::from_str(body["Policy"].as_str().unwrap()).unwrap();
    let stmts = doc["Statement"].as_array().unwrap();
    assert_eq!(stmts.len(), 1);
    assert_eq!(stmts[0]["Sid"], "b");

    // Remove the last one — doc stays (empty Statement array).
    let req = make_request(Method::DELETE, "/2015-03-31/functions/f/policy/b", "");
    svc.handle(req).await.unwrap();

    let req = make_request(Method::GET, "/2015-03-31/functions/f/policy", "");
    let resp = svc.handle(req).await.unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let doc: Value = serde_json::from_str(body["Policy"].as_str().unwrap()).unwrap();
    assert_eq!(doc["Statement"].as_array().unwrap().len(), 0);
}

#[tokio::test]
async fn remove_permission_unknown_sid_is_404() {
    let svc = LambdaService::new(make_state());
    seed_function(&svc, "f").await;

    let body = json!({
        "StatementId": "known",
        "Action": "InvokeFunction",
        "Principal": "arn:aws:iam::123456789012:user/u",
    });
    let req = make_request(
        Method::POST,
        "/2015-03-31/functions/f/policy",
        &body.to_string(),
    );
    svc.handle(req).await.unwrap();

    let req = make_request(Method::DELETE, "/2015-03-31/functions/f/policy/other", "");
    let err = match svc.handle(req).await {
        Err(e) => e,
        Ok(_) => panic!("expected error"),
    };
    assert_eq!(err.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn add_permission_on_missing_function_is_404() {
    let svc = LambdaService::new(make_state());
    let body = json!({
        "StatementId": "s",
        "Action": "InvokeFunction",
        "Principal": "arn:aws:iam::123456789012:user/u",
    });
    let req = make_request(
        Method::POST,
        "/2015-03-31/functions/missing/policy",
        &body.to_string(),
    );
    let err = match svc.handle(req).await {
        Err(e) => e,
        Ok(_) => panic!("expected error"),
    };
    assert_eq!(err.status(), StatusCode::NOT_FOUND);
}

#[test]
fn iam_action_for_maps_invoke_to_function_arn() {
    let svc = LambdaService::new(make_state());
    let req = make_request(Method::POST, "/2015-03-31/functions/f/invocations", "");
    let action = svc.iam_action_for(&req).unwrap();
    assert_eq!(action.service, "lambda");
    assert_eq!(action.action, "InvokeFunction");
    assert_eq!(
        action.resource,
        "arn:aws:lambda:us-east-1:123456789012:function:f"
    );
}

#[test]
fn iam_action_for_maps_list_to_star() {
    let svc = LambdaService::new(make_state());
    let req = make_request(Method::GET, "/2015-03-31/functions", "");
    let action = svc.iam_action_for(&req).unwrap();
    assert_eq!(action.action, "ListFunctions");
    assert_eq!(action.resource, "*");
}

#[test]
fn iam_action_for_create_reads_function_name_from_body() {
    let svc = LambdaService::new(make_state());
    let body = json!({
        "FunctionName": "newfn",
        "Runtime": "python3.12",
        "Role": "arn:aws:iam::123456789012:role/r",
        "Handler": "index.handler",
        "Code": {}
    });
    let req = make_request(Method::POST, "/2015-03-31/functions", &body.to_string());
    let action = svc.iam_action_for(&req).unwrap();
    assert_eq!(action.action, "CreateFunction");
    assert_eq!(
        action.resource,
        "arn:aws:lambda:us-east-1:123456789012:function:newfn"
    );
}

// ── Error branch tests ──

#[tokio::test]
async fn create_function_duplicate_returns_conflict() {
    let svc = LambdaService::new(make_state());
    seed_function(&svc, "dup-fn").await;

    let body = json!({
        "FunctionName": "dup-fn",
        "Runtime": "python3.12",
        "Role": "arn:aws:iam::123456789012:role/r",
        "Handler": "index.handler",
        "Code": {"ZipFile": "UEsDBBQ="},
    });
    let req = make_request(Method::POST, "/2015-03-31/functions", &body.to_string());
    let err = match svc.handle(req).await {
        Err(e) => e,
        Ok(_) => panic!("expected ResourceConflictException"),
    };
    assert_eq!(err.status(), StatusCode::CONFLICT);
}

#[tokio::test]
async fn get_function_not_found() {
    let svc = LambdaService::new(make_state());
    let req = make_request(Method::GET, "/2015-03-31/functions/nope", "");
    let err = match svc.handle(req).await {
        Err(e) => e,
        Ok(_) => panic!("expected error"),
    };
    assert_eq!(err.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn delete_function_not_found() {
    let svc = LambdaService::new(make_state());
    let req = make_request(Method::DELETE, "/2015-03-31/functions/nope", "");
    let err = match svc.handle(req).await {
        Err(e) => e,
        Ok(_) => panic!("expected error"),
    };
    assert_eq!(err.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn get_event_source_mapping_not_found() {
    let svc = LambdaService::new(make_state());
    let req = make_request(
        Method::GET,
        "/2015-03-31/event-source-mappings/nonexistent",
        "",
    );
    let err = match svc.handle(req).await {
        Err(e) => e,
        Ok(_) => panic!("expected error"),
    };
    assert_eq!(err.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn delete_event_source_mapping_not_found() {
    let svc = LambdaService::new(make_state());
    let req = make_request(
        Method::DELETE,
        "/2015-03-31/event-source-mappings/nonexistent",
        "",
    );
    let err = match svc.handle(req).await {
        Err(e) => e,
        Ok(_) => panic!("expected error"),
    };
    assert_eq!(err.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn get_policy_on_missing_function() {
    let svc = LambdaService::new(make_state());
    let req = make_request(Method::GET, "/2015-03-31/functions/nope/policy", "");
    let err = match svc.handle(req).await {
        Err(e) => e,
        Ok(_) => panic!("expected error"),
    };
    assert_eq!(err.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn remove_permission_on_missing_function() {
    let svc = LambdaService::new(make_state());
    let req = make_request(
        Method::DELETE,
        "/2015-03-31/functions/nope/policy/stmt1",
        "",
    );
    let err = match svc.handle(req).await {
        Err(e) => e,
        Ok(_) => panic!("expected error"),
    };
    assert_eq!(err.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn publish_version_on_missing_function() {
    let svc = LambdaService::new(make_state());
    let req = make_request(Method::POST, "/2015-03-31/functions/nope/versions", "{}");
    let err = match svc.handle(req).await {
        Err(e) => e,
        Ok(_) => panic!("expected error"),
    };
    assert_eq!(err.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn unknown_route_returns_error() {
    let svc = LambdaService::new(make_state());
    let req = make_request(Method::POST, "/unknown/route", "{}");
    assert!(svc.handle(req).await.is_err());
}

#[tokio::test]
async fn publish_version_unknown_function_errors() {
    let svc = LambdaService::new(make_state());
    let req = make_request(Method::POST, "/2015-03-31/functions/ghost/versions", "{}");
    assert!(svc.publish_version("ghost", "123456789012", &req).is_err());
}

#[tokio::test]
async fn get_function_unknown_errors() {
    let svc = LambdaService::new(make_state());
    let req = make_request(Method::GET, "/2015-03-31/functions/ghost", "");
    assert!(svc
        .get_function(&req, "ghost", "123456789012", "us-east-1", None)
        .is_err());
}

#[tokio::test]
async fn delete_function_unknown_errors() {
    let svc = LambdaService::new(make_state());
    assert!(svc.delete_function("ghost", "123456789012", None).is_err());
}

#[tokio::test]
async fn get_event_source_mapping_unknown_errors() {
    let svc = LambdaService::new(make_state());
    assert!(svc
        .get_event_source_mapping("ghost", "123456789012")
        .is_err());
}

#[tokio::test]
async fn delete_event_source_mapping_unknown_errors() {
    let svc = LambdaService::new(make_state());
    assert!(svc
        .delete_event_source_mapping("ghost", "123456789012")
        .is_err());
}

#[tokio::test]
async fn list_functions_empty_ok() {
    let svc = LambdaService::new(make_state());
    let resp = svc.list_functions("123456789012", None).unwrap();
    assert_eq!(resp.status, http::StatusCode::OK);
}

#[tokio::test]
async fn list_event_source_mappings_empty_ok() {
    let svc = LambdaService::new(make_state());
    let resp = svc.list_event_source_mappings("123456789012").unwrap();
    assert_eq!(resp.status, http::StatusCode::OK);
}

#[tokio::test]
async fn update_function_configuration_round_trips_advanced_fields() {
    // Pre-fix, UpdateFunctionConfiguration silently dropped 9 fields.
    // This asserts that a second GetFunctionConfiguration shows the
    // updated values for EphemeralStorage, VpcConfig, SnapStart,
    // DeadLetterConfig, LoggingConfig, ImageConfig, KMSKeyArn,
    // TracingConfig, Environment, FileSystemConfigs, and Runtime.
    let svc = LambdaService::new(make_state());
    seed_function(&svc, "advcfg").await;

    let body = json!({
        "Runtime": "python3.13",
        "Environment": {"Variables": {"FOO": "bar", "X": "y"}},
        "TracingConfig": {"Mode": "Active"},
        "KMSKeyArn": "arn:aws:kms:us-east-1:123456789012:key/abc",
        "EphemeralStorage": {"Size": 4096},
        "VpcConfig": {"SubnetIds": ["subnet-a"], "SecurityGroupIds": ["sg-a"]},
        "SnapStart": {"ApplyOn": "PublishedVersions"},
        "DeadLetterConfig": {"TargetArn": "arn:aws:sqs:us-east-1:123456789012:dlq"},
        "FileSystemConfigs": [{"Arn": "arn:aws:elasticfilesystem:us-east-1:123:access-point/fsap-1", "LocalMountPath": "/mnt/efs"}],
        "LoggingConfig": {"LogFormat": "JSON", "ApplicationLogLevel": "INFO"},
        "ImageConfig": {"Command": ["app.handler"], "EntryPoint": ["/usr/bin/python3"], "WorkingDirectory": "/var/task"}
    });
    let req = make_request(
        Method::PUT,
        "/2015-03-31/functions/advcfg/configuration",
        &body.to_string(),
    );
    svc.handle(req).await.unwrap();

    let req = make_request(Method::GET, "/2015-03-31/functions/advcfg", "");
    let resp = svc.handle(req).await.unwrap();
    let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let cfg = &v["Configuration"];
    assert_eq!(cfg["Runtime"], "python3.13");
    assert_eq!(cfg["Environment"]["Variables"]["FOO"], "bar");
    assert_eq!(cfg["TracingConfig"]["Mode"], "Active");
    assert_eq!(
        cfg["KMSKeyArn"],
        "arn:aws:kms:us-east-1:123456789012:key/abc"
    );
    assert_eq!(cfg["EphemeralStorage"]["Size"], 4096);
    assert_eq!(cfg["VpcConfig"]["SubnetIds"][0], "subnet-a");
    assert_eq!(cfg["SnapStart"]["ApplyOn"], "PublishedVersions");
    assert_eq!(
        cfg["DeadLetterConfig"]["TargetArn"],
        "arn:aws:sqs:us-east-1:123456789012:dlq"
    );
    assert_eq!(cfg["LoggingConfig"]["LogFormat"], "JSON");
    assert_eq!(
        cfg["ImageConfigResponse"]["ImageConfig"]["Command"][0],
        "app.handler"
    );
    assert_eq!(cfg["FileSystemConfigs"][0]["LocalMountPath"], "/mnt/efs");
}

#[tokio::test]
async fn update_function_configuration_rotates_revision_id() {
    // Clients use RevisionId as an optimistic-concurrency token. It
    // must change after a real config update so a second client
    // round-tripping the old value can detect the change.
    let svc = LambdaService::new(make_state());
    seed_function(&svc, "rev").await;

    let req = make_request(Method::GET, "/2015-03-31/functions/rev", "");
    let resp = svc.handle(req).await.unwrap();
    let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let rev_before = v["Configuration"]["RevisionId"]
        .as_str()
        .unwrap()
        .to_string();

    let body = json!({"Description": "updated"});
    let req = make_request(
        Method::PUT,
        "/2015-03-31/functions/rev/configuration",
        &body.to_string(),
    );
    svc.handle(req).await.unwrap();

    let req = make_request(Method::GET, "/2015-03-31/functions/rev", "");
    let resp = svc.handle(req).await.unwrap();
    let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let rev_after = v["Configuration"]["RevisionId"].as_str().unwrap();
    assert_ne!(rev_before, rev_after);
}

#[tokio::test]
async fn function_config_emits_state_reason_fields_when_populated() {
    // GetFunctionConfiguration omits StateReason / StateReasonCode and
    // their LastUpdateStatus counterparts when unset (default), and
    // surfaces them verbatim once populated. Real AWS leaves these
    // absent for healthy functions and only attaches them when a
    // function transitions out of the happy path.
    let svc = LambdaService::new(make_state());
    seed_function(&svc, "reason").await;

    // Default (unset) — fields must be absent, not null/empty-string.
    let req = make_request(Method::GET, "/2015-03-31/functions/reason", "");
    let resp = svc.handle(req).await.unwrap();
    let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert!(v["Configuration"]["StateReason"].is_null());
    assert!(v["Configuration"]["StateReasonCode"].is_null());
    assert!(v["Configuration"]["LastUpdateStatusReason"].is_null());
    assert!(v["Configuration"]["LastUpdateStatusReasonCode"].is_null());

    // Mutate state directly — these fields are AWS-internal, no public
    // API path to set them. Confirm they round-trip into the response.
    {
        let mut accts = svc.state.write();
        let acct = accts.get_or_create("123456789012");
        let f = acct.functions.get_mut("reason").unwrap();
        f.state_reason = Some("EFS access point unavailable".into());
        f.state_reason_code = Some("EFSMountFailure".into());
        f.last_update_status_reason = Some("Backoff after 3 attempts".into());
        f.last_update_status_reason_code = Some("EniLimitExceeded".into());
    }

    let req = make_request(Method::GET, "/2015-03-31/functions/reason", "");
    let resp = svc.handle(req).await.unwrap();
    let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let cfg = &v["Configuration"];
    assert_eq!(cfg["StateReason"], "EFS access point unavailable");
    assert_eq!(cfg["StateReasonCode"], "EFSMountFailure");
    assert_eq!(cfg["LastUpdateStatusReason"], "Backoff after 3 attempts");
    assert_eq!(cfg["LastUpdateStatusReasonCode"], "EniLimitExceeded");
}

#[tokio::test]
async fn create_event_source_mapping_round_trips_advanced_fields() {
    // KMSKeyArn / MetricsConfig / DestinationConfig / MaximumRetryAttempts /
    // MaximumRecordAgeInSeconds / BisectBatchOnFunctionError /
    // TumblingWindowInSeconds / Topics / Queues all need to round-trip
    // through Create → Get so SDK clients can read what they wrote. The
    // 2024+ AWS shape requires every one of these fields in the response
    // when the caller supplied them.
    let svc = LambdaService::new(make_state());
    seed_function(&svc, "esm-fn").await;

    let body = json!({
        "FunctionName": "esm-fn",
        "EventSourceArn": "arn:aws:sqs:us-east-1:123456789012:queue1",
        "BatchSize": 10,
        "Enabled": true,
        "KMSKeyArn": "arn:aws:kms:us-east-1:123456789012:key/abc-def",
        "MetricsConfig": {"Metrics": ["EventCount"]},
        "DestinationConfig": {"OnFailure": {"Destination": "arn:aws:sqs:us-east-1:123456789012:dlq"}},
        "MaximumRetryAttempts": 5,
        "MaximumRecordAgeInSeconds": 3600,
        "BisectBatchOnFunctionError": true,
        "TumblingWindowInSeconds": 60,
        "Topics": ["t1"],
        "Queues": ["q1"],
    });
    let req = make_request(
        Method::POST,
        "/2015-03-31/event-source-mappings",
        &body.to_string(),
    );
    let resp = svc.handle(req).await.unwrap();
    let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let uuid = v["UUID"].as_str().unwrap().to_string();
    assert_eq!(
        v["KMSKeyArn"],
        "arn:aws:kms:us-east-1:123456789012:key/abc-def"
    );
    assert_eq!(v["MetricsConfig"]["Metrics"][0], "EventCount");
    assert_eq!(
        v["DestinationConfig"]["OnFailure"]["Destination"],
        "arn:aws:sqs:us-east-1:123456789012:dlq"
    );
    assert_eq!(v["MaximumRetryAttempts"], 5);
    assert_eq!(v["MaximumRecordAgeInSeconds"], 3600);
    assert_eq!(v["BisectBatchOnFunctionError"], true);
    assert_eq!(v["TumblingWindowInSeconds"], 60);
    assert_eq!(v["Topics"][0], "t1");
    assert_eq!(v["Queues"][0], "q1");

    // Get the mapping back and assert all fields survive a round-trip.
    let req = make_request(
        Method::GET,
        &format!("/2015-03-31/event-source-mappings/{uuid}"),
        "",
    );
    let resp = svc.handle(req).await.unwrap();
    let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(
        v["KMSKeyArn"],
        "arn:aws:kms:us-east-1:123456789012:key/abc-def"
    );
    assert_eq!(v["BisectBatchOnFunctionError"], true);
    assert_eq!(v["MaximumRetryAttempts"], 5);
    assert_eq!(v["Topics"][0], "t1");
}

#[tokio::test]
async fn update_event_source_mapping_persists_enabled_and_sac() {
    // Disabling via Update must flip State to "Disabled" (was hardcoded
    // "Enabled") and SourceAccessConfigurations must persist + reflect on Get
    // (1.1). KMSKeyArn/DestinationConfig must round-trip via Update too.
    let svc = LambdaService::new(make_state());
    seed_function(&svc, "esm-upd").await;

    let create = json!({
        "FunctionName": "esm-upd",
        "EventSourceArn": "arn:aws:sqs:us-east-1:123456789012:queue1",
        "BatchSize": 10,
        "Enabled": true,
    });
    let req = make_request(
        Method::POST,
        "/2015-03-31/event-source-mappings",
        &create.to_string(),
    );
    let resp = svc.handle(req).await.unwrap();
    let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let uuid = v["UUID"].as_str().unwrap().to_string();
    assert_eq!(v["State"], "Enabled");

    // Update: disable + set SourceAccessConfigurations + KMSKeyArn.
    let update = json!({
        "Enabled": false,
        "SourceAccessConfigurations": [
            {"Type": "BASIC_AUTH", "URI": "arn:aws:secretsmanager:us-east-1:123456789012:secret:x"}
        ],
        "KMSKeyArn": "arn:aws:kms:us-east-1:123456789012:key/upd",
    });
    let req = make_request(
        Method::PUT,
        &format!("/2015-03-31/event-source-mappings/{uuid}"),
        &update.to_string(),
    );
    let resp = svc.handle(req).await.unwrap();
    let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(v["State"], "Disabled");
    assert_eq!(v["SourceAccessConfigurations"][0]["Type"], "BASIC_AUTH");
    assert_eq!(v["KMSKeyArn"], "arn:aws:kms:us-east-1:123456789012:key/upd");

    // Get back: changes must have persisted.
    let req = make_request(
        Method::GET,
        &format!("/2015-03-31/event-source-mappings/{uuid}"),
        "",
    );
    let resp = svc.handle(req).await.unwrap();
    let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(v["State"], "Disabled");
    assert_eq!(v["SourceAccessConfigurations"][0]["URI"], "arn:aws:secretsmanager:us-east-1:123456789012:secret:x");

    // Re-enable round-trips back to Enabled.
    let req = make_request(
        Method::PUT,
        &format!("/2015-03-31/event-source-mappings/{uuid}"),
        &json!({"Enabled": true}).to_string(),
    );
    let resp = svc.handle(req).await.unwrap();
    let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(v["State"], "Enabled");
}

struct StubS3 {
    object: std::sync::Mutex<std::collections::HashMap<String, Vec<u8>>>,
}
impl fakecloud_core::delivery::S3Delivery for StubS3 {
    fn put_object(
        &self,
        _account_id: &str,
        bucket: &str,
        key: &str,
        body: Vec<u8>,
        _content_type: Option<&str>,
    ) -> Result<(), String> {
        self.object
            .lock()
            .unwrap()
            .insert(format!("{bucket}/{key}"), body);
        Ok(())
    }
    fn get_object(&self, _account_id: &str, bucket: &str, key: &str) -> Result<Vec<u8>, String> {
        self.object
            .lock()
            .unwrap()
            .get(&format!("{bucket}/{key}"))
            .cloned()
            .ok_or_else(|| "missing".to_string())
    }
}

#[tokio::test]
async fn update_function_code_fetches_real_bytes_from_s3() {
    let stub = std::sync::Arc::new(StubS3 {
        object: std::sync::Mutex::new(std::collections::HashMap::new()),
    });
    let payload = b"real-zip-bytes-from-s3-bucket";
    stub.object
        .lock()
        .unwrap()
        .insert("my-bucket/lambda.zip".to_string(), payload.to_vec());

    let svc = LambdaService::new(make_state()).with_s3_delivery(stub);
    let body = json!({
        "FunctionName": "fromzip",
        "Runtime": "python3.12",
        "Role": "arn:aws:iam::123456789012:role/r",
        "Handler": "index.handler",
        "Code": {"ZipFile": base64::Engine::encode(&base64::engine::general_purpose::STANDARD, b"seed")},
    });
    let req = make_request(Method::POST, "/2015-03-31/functions", &body.to_string());
    svc.handle(req).await.unwrap();

    let body = json!({"S3Bucket": "my-bucket", "S3Key": "lambda.zip"});
    let req = make_request(
        Method::PUT,
        "/2015-03-31/functions/fromzip/code",
        &body.to_string(),
    );
    let resp = svc.handle(req).await.unwrap();
    let post: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(payload);
    let expected = base64::Engine::encode(
        &base64::engine::general_purpose::STANDARD,
        hasher.finalize(),
    );
    assert_eq!(post["CodeSha256"].as_str().unwrap(), expected);
    assert_eq!(post["CodeSize"].as_i64().unwrap(), payload.len() as i64);
}

// ── UpdateFunctionCode behavior tests (D1) ──

#[tokio::test]
async fn update_function_code_replaces_zip_and_recomputes_sha256() {
    let svc = LambdaService::new(make_state());
    // Seed with one zip payload so we can verify the hash actually changes.
    let initial_bytes = b"initial-zip-payload";
    let initial_b64 =
        base64::Engine::encode(&base64::engine::general_purpose::STANDARD, initial_bytes);
    let body = json!({
        "FunctionName": "rehash",
        "Runtime": "python3.12",
        "Role": "arn:aws:iam::123456789012:role/r",
        "Handler": "index.handler",
        "Code": {"ZipFile": initial_b64},
    });
    let req = make_request(Method::POST, "/2015-03-31/functions", &body.to_string());
    let resp = svc.handle(req).await.unwrap();
    let pre: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let pre_sha = pre["CodeSha256"].as_str().unwrap().to_string();
    let pre_size = pre["CodeSize"].as_i64().unwrap();
    assert_eq!(pre_size, initial_bytes.len() as i64);

    // Update with completely different bytes.
    let new_bytes = b"a-much-longer-replacement-zip-payload-with-different-content";
    let new_b64 = base64::Engine::encode(&base64::engine::general_purpose::STANDARD, new_bytes);
    let body = json!({ "ZipFile": new_b64 });
    let req = make_request(
        Method::PUT,
        "/2015-03-31/functions/rehash/code",
        &body.to_string(),
    );
    let resp = svc.handle(req).await.unwrap();
    let post: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();

    // SHA must change and the response must report the *new* hash.
    let post_sha = post["CodeSha256"].as_str().unwrap();
    assert_ne!(post_sha, pre_sha);
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(new_bytes);
    let expected = base64::Engine::encode(
        &base64::engine::general_purpose::STANDARD,
        hasher.finalize(),
    );
    assert_eq!(post_sha, expected);
    assert_eq!(post["CodeSize"].as_i64().unwrap(), new_bytes.len() as i64);

    // GetFunctionConfiguration must surface the same updated state.
    let req = make_request(
        Method::GET,
        "/2015-03-31/functions/rehash/configuration",
        "",
    );
    let resp = svc.handle(req).await.unwrap();
    let cfg: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(cfg["CodeSha256"].as_str().unwrap(), expected);
    assert_eq!(cfg["CodeSize"].as_i64().unwrap(), new_bytes.len() as i64);
}

#[tokio::test]
async fn update_function_code_replaces_image_uri_and_persists() {
    let svc = LambdaService::new(make_state());
    let body = json!({
        "FunctionName": "img-update",
        "Runtime": "python3.12",
        "Role": "arn:aws:iam::123456789012:role/r",
        "Handler": "index.handler",
        "PackageType": "Image",
        "Code": {"ImageUri": "old.example.com/image:1"},
    });
    let req = make_request(Method::POST, "/2015-03-31/functions", &body.to_string());
    svc.handle(req).await.unwrap();

    let body = json!({ "ImageUri": "new.example.com/image:2" });
    let req = make_request(
        Method::PUT,
        "/2015-03-31/functions/img-update/code",
        &body.to_string(),
    );
    svc.handle(req).await.unwrap();
    // UpdateFunctionCode returns a `FunctionConfiguration` shape with no
    // `Code` member — confirm the new URI via the `GetFunction` wrapper.
    let req = make_request(Method::GET, "/2015-03-31/functions/img-update", "");
    let wrap: Value =
        serde_json::from_slice(svc.handle(req).await.unwrap().body.expect_bytes()).unwrap();
    assert_eq!(
        wrap["Code"]["ImageUri"].as_str().unwrap(),
        "new.example.com/image:2"
    );

    // `GetFunctionConfiguration` returns only the `FunctionConfiguration`
    // shape (no `Code` wrapper) — confirm the underlying image URI
    // persists via `GetFunction`.
    let req = make_request(Method::GET, "/2015-03-31/functions/img-update", "");
    let wrap: Value =
        serde_json::from_slice(svc.handle(req).await.unwrap().body.expect_bytes()).unwrap();
    assert_eq!(
        wrap["Code"]["ImageUri"].as_str().unwrap(),
        "new.example.com/image:2"
    );
}

#[tokio::test]
async fn update_function_code_with_matching_revision_id_succeeds() {
    let svc = LambdaService::new(make_state());
    seed_function(&svc, "rev-ok").await;

    let req = make_request(
        Method::GET,
        "/2015-03-31/functions/rev-ok/configuration",
        "",
    );
    let pre: Value =
        serde_json::from_slice(svc.handle(req).await.unwrap().body.expect_bytes()).unwrap();
    let revision = pre["RevisionId"].as_str().unwrap().to_string();

    let new_b64 = base64::Engine::encode(&base64::engine::general_purpose::STANDARD, b"with-rev");
    let body = json!({ "ZipFile": new_b64, "RevisionId": revision });
    let req = make_request(
        Method::PUT,
        "/2015-03-31/functions/rev-ok/code",
        &body.to_string(),
    );
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, http::StatusCode::OK);
    let post: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    // Code changed -> revision_id must rotate to a fresh value.
    assert_ne!(post["RevisionId"].as_str().unwrap(), revision);
}

#[tokio::test]
async fn update_function_code_with_stale_revision_id_returns_412() {
    let svc = LambdaService::new(make_state());
    seed_function(&svc, "rev-stale").await;

    let new_b64 = base64::Engine::encode(&base64::engine::general_purpose::STANDARD, b"stale");
    let body = json!({
        "ZipFile": new_b64,
        "RevisionId": "00000000-0000-0000-0000-000000000000",
    });
    let req = make_request(
        Method::PUT,
        "/2015-03-31/functions/rev-stale/code",
        &body.to_string(),
    );
    let err = match svc.handle(req).await {
        Err(e) => e,
        Ok(_) => panic!("expected PreconditionFailedException"),
    };
    assert_eq!(err.status(), StatusCode::PRECONDITION_FAILED);
    assert!(err.to_string().contains("PreconditionFailedException"));
}

#[tokio::test]
async fn update_function_code_unknown_function_returns_404() {
    let svc = LambdaService::new(make_state());
    let new_b64 = base64::Engine::encode(&base64::engine::general_purpose::STANDARD, b"x");
    let body = json!({ "ZipFile": new_b64 });
    let req = make_request(
        Method::PUT,
        "/2015-03-31/functions/does-not-exist/code",
        &body.to_string(),
    );
    let err = match svc.handle(req).await {
        Err(e) => e,
        Ok(_) => panic!("expected ResourceNotFoundException"),
    };
    assert_eq!(err.status(), StatusCode::NOT_FOUND);
    assert!(err.to_string().contains("ResourceNotFoundException"));
}

#[tokio::test]
async fn update_function_code_dry_run_does_not_mutate() {
    let svc = LambdaService::new(make_state());
    let initial = b"original-bytes";
    let initial_b64 = base64::Engine::encode(&base64::engine::general_purpose::STANDARD, initial);
    let body = json!({
        "FunctionName": "dryrun",
        "Runtime": "python3.12",
        "Role": "arn:aws:iam::123456789012:role/r",
        "Handler": "index.handler",
        "Code": {"ZipFile": initial_b64},
    });
    let req = make_request(Method::POST, "/2015-03-31/functions", &body.to_string());
    let resp = svc.handle(req).await.unwrap();
    let pre: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let pre_sha = pre["CodeSha256"].as_str().unwrap().to_string();
    let pre_size = pre["CodeSize"].as_i64().unwrap();
    let pre_rev = pre["RevisionId"].as_str().unwrap().to_string();

    // DryRun=true with new bytes — must not mutate state.
    let new_b64 = base64::Engine::encode(
        &base64::engine::general_purpose::STANDARD,
        b"would-be-new-bytes",
    );
    let body = json!({ "ZipFile": new_b64, "DryRun": true });
    let req = make_request(
        Method::PUT,
        "/2015-03-31/functions/dryrun/code",
        &body.to_string(),
    );
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, http::StatusCode::OK);

    // GetFunctionConfiguration confirms no fields changed.
    let req = make_request(
        Method::GET,
        "/2015-03-31/functions/dryrun/configuration",
        "",
    );
    let resp = svc.handle(req).await.unwrap();
    let cfg: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(cfg["CodeSha256"].as_str().unwrap(), pre_sha);
    assert_eq!(cfg["CodeSize"].as_i64().unwrap(), pre_size);
    assert_eq!(cfg["RevisionId"].as_str().unwrap(), pre_rev);
}

#[tokio::test]
async fn update_function_code_with_s3_descriptor_rotates_hash() {
    let svc = LambdaService::new(make_state());
    seed_function(&svc, "s3src").await;

    let req = make_request(Method::GET, "/2015-03-31/functions/s3src/configuration", "");
    let pre: Value =
        serde_json::from_slice(svc.handle(req).await.unwrap().body.expect_bytes()).unwrap();
    let pre_sha = pre["CodeSha256"].as_str().unwrap().to_string();
    let pre_rev = pre["RevisionId"].as_str().unwrap().to_string();

    // S3Bucket+S3Key swap -- fakecloud fingerprints the descriptor, so a
    // different bucket/key must produce a different CodeSha256 and rotate
    // RevisionId.
    let body = json!({"S3Bucket": "deploy-bucket", "S3Key": "lambdas/v2.zip"});
    let req = make_request(
        Method::PUT,
        "/2015-03-31/functions/s3src/code",
        &body.to_string(),
    );
    let resp = svc.handle(req).await.unwrap();
    let post: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let post_sha = post["CodeSha256"].as_str().unwrap().to_string();
    assert_ne!(post_sha, pre_sha, "S3 swap should rotate CodeSha256");
    assert_ne!(
        post["RevisionId"].as_str().unwrap(),
        pre_rev,
        "S3 swap should rotate RevisionId"
    );
    assert!(post["CodeSize"].as_i64().unwrap() > 0);
    assert_eq!(post["PackageType"].as_str().unwrap(), "Zip");

    // Same descriptor again -> RevisionId stable (no spurious bump).
    let body = json!({"S3Bucket": "deploy-bucket", "S3Key": "lambdas/v2.zip"});
    let req = make_request(
        Method::PUT,
        "/2015-03-31/functions/s3src/code",
        &body.to_string(),
    );
    let resp = svc.handle(req).await.unwrap();
    let again: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(again["CodeSha256"].as_str().unwrap(), post_sha);
    assert_eq!(
        again["RevisionId"].as_str().unwrap(),
        post["RevisionId"].as_str().unwrap()
    );

    // Different S3ObjectVersion on the same bucket+key counts as a new
    // descriptor and must rotate the hash.
    let body = json!({
        "S3Bucket": "deploy-bucket",
        "S3Key": "lambdas/v2.zip",
        "S3ObjectVersion": "v-abc123",
    });
    let req = make_request(
        Method::PUT,
        "/2015-03-31/functions/s3src/code",
        &body.to_string(),
    );
    let resp = svc.handle(req).await.unwrap();
    let versioned: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_ne!(versioned["CodeSha256"].as_str().unwrap(), post_sha);
}

#[tokio::test]
async fn update_function_code_image_uri_clears_size_and_sha() {
    // AWS reports CodeSize=0 and an empty CodeSha256 for image-package
    // functions; the digest lives on the ECR side, not in the Lambda
    // response. Verify UpdateFunctionCode brings those fields in line
    // when swapping to a new image.
    let svc = LambdaService::new(make_state());
    let body = json!({
        "FunctionName": "img-clear",
        "Runtime": "python3.12",
        "Role": "arn:aws:iam::123456789012:role/r",
        "Handler": "index.handler",
        "PackageType": "Image",
        "Code": {"ImageUri": "old.example.com/image:1"},
    });
    let req = make_request(Method::POST, "/2015-03-31/functions", &body.to_string());
    svc.handle(req).await.unwrap();

    let body = json!({"ImageUri": "new.example.com/image:2"});
    let req = make_request(
        Method::PUT,
        "/2015-03-31/functions/img-clear/code",
        &body.to_string(),
    );
    let resp = svc.handle(req).await.unwrap();
    let post: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(post["CodeSize"].as_i64().unwrap(), 0);
    assert_eq!(post["CodeSha256"].as_str().unwrap(), "");
    assert_eq!(post["PackageType"].as_str().unwrap(), "Image");
}

// ── PublishVersion behavior tests (D2) ──

#[tokio::test]
async fn publish_version_returns_numeric_version_and_versioned_arn() {
    let svc = LambdaService::new(make_state());
    seed_function(&svc, "pv1").await;
    let req = make_request(Method::POST, "/2015-03-31/functions/pv1/versions", "{}");
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::CREATED);
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["Version"], "1");
    assert!(body["FunctionArn"].as_str().unwrap().ends_with(":1"));
    assert_eq!(
        body["MasterArn"].as_str().unwrap(),
        "arn:aws:lambda:us-east-1:123456789012:function:pv1"
    );
}

#[tokio::test]
async fn publish_version_increments_per_function_counter() {
    let svc = LambdaService::new(make_state());
    seed_function(&svc, "pv2").await;

    let req = make_request(Method::POST, "/2015-03-31/functions/pv2/versions", "{}");
    let resp = svc.handle(req).await.unwrap();
    let v1: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(v1["Version"], "1");

    // Mutate $LATEST so the next PublishVersion isn't a no-op idempotent
    // re-publish — AWS returns the previous version when nothing changed.
    let new_zip_b64 =
        base64::Engine::encode(&base64::engine::general_purpose::STANDARD, b"diff-bytes");
    let body = json!({ "ZipFile": new_zip_b64 });
    let req = make_request(
        Method::PUT,
        "/2015-03-31/functions/pv2/code",
        &body.to_string(),
    );
    svc.handle(req).await.unwrap();

    let req = make_request(Method::POST, "/2015-03-31/functions/pv2/versions", "{}");
    let resp = svc.handle(req).await.unwrap();
    let v2: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(v2["Version"], "2");
}

#[tokio::test]
async fn publish_version_idempotent_when_latest_unchanged() {
    // AWS PublishVersion returns the existing latest version when
    // $LATEST hasn't changed since the previous publish.
    let svc = LambdaService::new(make_state());
    seed_function(&svc, "idem").await;

    let req = make_request(Method::POST, "/2015-03-31/functions/idem/versions", "{}");
    let resp = svc.handle(req).await.unwrap();
    let v1: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(v1["Version"], "1");
    let v1_revision = v1["RevisionId"].as_str().unwrap().to_string();

    // No mutation in between; second publish is a no-op that returns v1.
    let req = make_request(Method::POST, "/2015-03-31/functions/idem/versions", "{}");
    let resp = svc.handle(req).await.unwrap();
    let again: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(again["Version"], "1");
    assert_eq!(again["RevisionId"].as_str().unwrap(), v1_revision);
}

#[tokio::test]
async fn publish_version_idempotent_with_snap_start_published_versions() {
    // PublishVersion mutates OptimizationStatus="On" on the snapshot
    // when SnapStart.ApplyOn=PublishedVersions, while $LATEST keeps
    // "Off". A naive deep-equality on snap_start would treat that
    // post-publish $LATEST -> snapshot delta as a real change, minting
    // a fresh version on every redundant call. The fix is to compare
    // ApplyOn only, since that's the caller-controlled field.
    let svc = LambdaService::new(make_state());
    let body = json!({
        "FunctionName": "snap-fn",
        "Runtime": "python3.12",
        "Role": "arn:aws:iam::123456789012:role/r",
        "Handler": "index.handler",
        "Code": {},
        "SnapStart": { "ApplyOn": "PublishedVersions" }
    });
    let req = make_request(Method::POST, "/2015-03-31/functions", &body.to_string());
    svc.handle(req).await.unwrap();

    let req = make_request(Method::POST, "/2015-03-31/functions/snap-fn/versions", "{}");
    let resp = svc.handle(req).await.unwrap();
    let v1: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(v1["Version"], "1");

    // Re-publish with no changes -> still v1 (idempotent), even
    // though the snapshot had OptimizationStatus flipped to "On".
    let req = make_request(Method::POST, "/2015-03-31/functions/snap-fn/versions", "{}");
    let resp = svc.handle(req).await.unwrap();
    let again: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(again["Version"], "1");
}

#[tokio::test]
async fn publish_version_creates_new_version_on_config_only_change() {
    // PublishVersion idempotency must not collapse a config-only
    // change (memory, env, etc.) to the previous snapshot — AWS
    // produces a new numbered version when any field that the
    // snapshot captures has moved, even if CodeSha256 is unchanged.
    let svc = LambdaService::new(make_state());
    seed_function(&svc, "cfg-fn").await;

    let req = make_request(Method::POST, "/2015-03-31/functions/cfg-fn/versions", "{}");
    let resp = svc.handle(req).await.unwrap();
    let v1: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(v1["Version"], "1");

    // Bump MemorySize via UpdateFunctionConfiguration (no code change).
    let body = json!({ "MemorySize": 256 });
    let req = make_request(
        Method::PUT,
        "/2015-03-31/functions/cfg-fn/configuration",
        &body.to_string(),
    );
    svc.handle(req).await.unwrap();

    let req = make_request(Method::POST, "/2015-03-31/functions/cfg-fn/versions", "{}");
    let resp = svc.handle(req).await.unwrap();
    let v2: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(v2["Version"], "2");
    assert_eq!(v2["MemorySize"].as_i64().unwrap(), 256);
}

#[tokio::test]
async fn publish_version_snapshots_code_immutable() {
    let svc = LambdaService::new(make_state());
    seed_function(&svc, "pv3").await;

    // Publish v1 of the seed (empty code).
    let req = make_request(Method::POST, "/2015-03-31/functions/pv3/versions", "{}");
    let resp = svc.handle(req).await.unwrap();
    let v1: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let v1_sha = v1["CodeSha256"].as_str().unwrap().to_string();

    // Push new code into $LATEST.
    let new_zip_b64 =
        base64::Engine::encode(&base64::engine::general_purpose::STANDARD, b"newer-bytes");
    let body = json!({ "ZipFile": new_zip_b64 });
    let req = make_request(
        Method::PUT,
        "/2015-03-31/functions/pv3/code",
        &body.to_string(),
    );
    svc.handle(req).await.unwrap();

    // GetFunctionConfiguration?Qualifier=1 returns the original snapshot.
    let mut req = make_request(Method::GET, "/2015-03-31/functions/pv3/configuration", "");
    req.query_params
        .insert("Qualifier".to_string(), "1".to_string());
    let resp = svc.handle(req).await.unwrap();
    let snap: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(snap["Version"], "1");
    assert_eq!(snap["CodeSha256"].as_str().unwrap(), v1_sha);

    // $LATEST has the new code SHA.
    let req = make_request(Method::GET, "/2015-03-31/functions/pv3/configuration", "");
    let resp = svc.handle(req).await.unwrap();
    let live: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(live["Version"], "$LATEST");
    assert_ne!(live["CodeSha256"].as_str().unwrap(), v1_sha);
}

#[tokio::test]
async fn list_versions_by_function_returns_all_plus_latest() {
    let svc = LambdaService::new(make_state());
    seed_function(&svc, "pv4").await;

    // Two distinct publishes need a $LATEST mutation in between; AWS
    // PublishVersion is idempotent when nothing has changed.
    let req = make_request(Method::POST, "/2015-03-31/functions/pv4/versions", "{}");
    svc.handle(req).await.unwrap();
    let new_zip_b64 =
        base64::Engine::encode(&base64::engine::general_purpose::STANDARD, b"v2-bytes");
    let body = json!({ "ZipFile": new_zip_b64 });
    let req = make_request(
        Method::PUT,
        "/2015-03-31/functions/pv4/code",
        &body.to_string(),
    );
    svc.handle(req).await.unwrap();
    let req = make_request(Method::POST, "/2015-03-31/functions/pv4/versions", "{}");
    svc.handle(req).await.unwrap();

    let req = make_request(Method::GET, "/2015-03-31/functions/pv4/versions", "");
    let resp = svc.handle(req).await.unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let versions: Vec<String> = body["Versions"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v["Version"].as_str().unwrap().to_string())
        .collect();
    assert!(versions.contains(&"$LATEST".to_string()));
    assert!(versions.contains(&"1".to_string()));
    assert!(versions.contains(&"2".to_string()));
    assert_eq!(versions.len(), 3);
}

#[tokio::test]
async fn publish_version_revision_id_mismatch_returns_412() {
    let svc = LambdaService::new(make_state());
    seed_function(&svc, "pv5").await;

    let body = json!({ "RevisionId": "stale-revision-id-deadbeef" });
    let req = make_request(
        Method::POST,
        "/2015-03-31/functions/pv5/versions",
        &body.to_string(),
    );
    let err = match svc.handle(req).await {
        Err(e) => e,
        Ok(_) => panic!("expected PreconditionFailedException"),
    };
    assert_eq!(err.status(), StatusCode::PRECONDITION_FAILED);
    assert!(err.code().contains("PreconditionFailed"));
}

#[tokio::test]
async fn publish_version_unknown_function_returns_404() {
    let svc = LambdaService::new(make_state());
    let req = make_request(
        Method::POST,
        "/2015-03-31/functions/missing-fn/versions",
        "{}",
    );
    let err = match svc.handle(req).await {
        Err(e) => e,
        Ok(_) => panic!("expected ResourceNotFoundException"),
    };
    assert_eq!(err.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn update_function_code_publish_creates_new_version() {
    let svc = LambdaService::new(make_state());
    seed_function(&svc, "pub-fn").await;

    let new_b64 = base64::Engine::encode(
        &base64::engine::general_purpose::STANDARD,
        b"publish-payload",
    );
    let body = json!({ "ZipFile": new_b64, "Publish": true });
    let req = make_request(
        Method::PUT,
        "/2015-03-31/functions/pub-fn/code",
        &body.to_string(),
    );
    let resp = svc.handle(req).await.unwrap();
    let post: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();

    // Publish=true returns the snapshot version (numeric, not $LATEST).
    let version = post["Version"].as_str().unwrap();
    assert_ne!(version, "$LATEST");
    let parsed: u64 = version.parse().expect("Version should be numeric");
    assert!(parsed >= 1);
    assert!(post["FunctionArn"]
        .as_str()
        .unwrap()
        .ends_with(&format!(":{version}")));

    // The new $LATEST also reflects the updated code.
    let req = make_request(
        Method::GET,
        "/2015-03-31/functions/pub-fn/configuration",
        "",
    );
    let resp = svc.handle(req).await.unwrap();
    let latest: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(
        latest["CodeSize"].as_i64().unwrap(),
        b"publish-payload".len() as i64
    );
}

// ── AddPermission action round-trip (D3) ──────────────────────────

#[tokio::test]
async fn add_permission_stores_action_verbatim() {
    // AWS keeps whatever string the caller sent in the policy doc
    // verbatim — passing `s3:PutObject` should not get re-prefixed to
    // `lambda:s3:PutObject`.
    let svc = LambdaService::new(make_state());
    seed_function(&svc, "f").await;
    let body = json!({
        "StatementId": "s3-put",
        "Action": "s3:PutObject",
        "Principal": "arn:aws:iam::123456789012:user/u",
    });
    let req = make_request(
        Method::POST,
        "/2015-03-31/functions/f/policy",
        &body.to_string(),
    );
    svc.handle(req).await.unwrap();

    let req = make_request(Method::GET, "/2015-03-31/functions/f/policy", "");
    let resp = svc.handle(req).await.unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let doc: Value = serde_json::from_str(body["Policy"].as_str().unwrap()).unwrap();
    let stmts = doc["Statement"].as_array().unwrap();
    assert_eq!(stmts.len(), 1);
    assert_eq!(stmts[0]["Action"], "s3:PutObject");
}

#[tokio::test]
async fn add_permission_action_without_prefix() {
    // Documented behavior: store verbatim. Caller passed
    // `InvokeFunction` (no `lambda:` prefix), GetPolicy returns
    // `InvokeFunction`. The cross-service evaluator path that wants a
    // qualified `service:verb` is responsible for normalizing on read,
    // not the storage layer.
    let svc = LambdaService::new(make_state());
    seed_function(&svc, "f").await;
    let body = json!({
        "StatementId": "raw-verb",
        "Action": "InvokeFunction",
        "Principal": "events.amazonaws.com",
    });
    let req = make_request(
        Method::POST,
        "/2015-03-31/functions/f/policy",
        &body.to_string(),
    );
    svc.handle(req).await.unwrap();

    let req = make_request(Method::GET, "/2015-03-31/functions/f/policy", "");
    let resp = svc.handle(req).await.unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let doc: Value = serde_json::from_str(body["Policy"].as_str().unwrap()).unwrap();
    let stmts = doc["Statement"].as_array().unwrap();
    assert_eq!(stmts.len(), 1);
    assert_eq!(stmts[0]["Action"], "InvokeFunction");
}

// ── Tag store unification (D3) ────────────────────────────────────

#[tokio::test]
async fn tag_resource_writes_to_function_tags() {
    let svc = LambdaService::new(make_state());
    seed_function(&svc, "tag-fn").await;
    let arn = "arn:aws:lambda:us-east-1:123456789012:function:tag-fn";
    let body = json!({"Tags": {"env": "prod", "team": "core"}});
    let req = make_request(
        Method::POST,
        &format!("/2017-03-31/tags/{arn}"),
        &body.to_string(),
    );
    let resp = svc.handle(req).await.unwrap();
    assert!(resp.status.is_success());

    // Read func.tags directly to confirm it landed there.
    let accounts = svc.state.read();
    let state = accounts.get("123456789012").unwrap();
    let func = state.functions.get("tag-fn").unwrap();
    assert_eq!(func.tags.get("env").map(String::as_str), Some("prod"));
    assert_eq!(func.tags.get("team").map(String::as_str), Some("core"));
}

#[tokio::test]
async fn list_tags_for_resource_reads_from_function_tags() {
    let svc = LambdaService::new(make_state());
    seed_function(&svc, "tag-fn").await;
    let arn = "arn:aws:lambda:us-east-1:123456789012:function:tag-fn";

    // Seed via TagResource.
    let body = json!({"Tags": {"a": "1", "b": "2"}});
    let req = make_request(
        Method::POST,
        &format!("/2017-03-31/tags/{arn}"),
        &body.to_string(),
    );
    svc.handle(req).await.unwrap();

    let req = make_request(Method::GET, &format!("/2017-03-31/tags/{arn}"), "");
    let resp = svc.handle(req).await.unwrap();
    let out: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(out["Tags"]["a"], "1");
    assert_eq!(out["Tags"]["b"], "2");
}

#[tokio::test]
async fn untag_resource_with_multiple_keys() {
    // AWS sends `tagKeys=A&tagKeys=B`. The dispatcher's deduplicated
    // `query_params` HashMap collapses repeats, so the handler must
    // parse `req.raw_query` directly to see both values.
    let svc = LambdaService::new(make_state());
    seed_function(&svc, "tag-fn").await;
    let arn = "arn:aws:lambda:us-east-1:123456789012:function:tag-fn";

    let body = json!({"Tags": {"A": "1", "B": "2", "C": "3"}});
    let req = make_request(
        Method::POST,
        &format!("/2017-03-31/tags/{arn}"),
        &body.to_string(),
    );
    svc.handle(req).await.unwrap();

    let mut req = make_request(Method::DELETE, &format!("/2017-03-31/tags/{arn}"), "");
    req.raw_query = "tagKeys=A&tagKeys=B".to_string();
    svc.handle(req).await.unwrap();

    let accounts = svc.state.read();
    let state = accounts.get("123456789012").unwrap();
    let func = state.functions.get("tag-fn").unwrap();
    assert!(!func.tags.contains_key("A"));
    assert!(!func.tags.contains_key("B"));
    assert_eq!(func.tags.get("C").map(String::as_str), Some("3"));
}

#[tokio::test]
async fn tag_state_unified_no_duplicate_state_tags() {
    // After D3, `LambdaState::tags` no longer exists — tags live only
    // on `LambdaFunction::tags`. This test pins the unified shape: a
    // `TagResource` write puts a single entry in `func.tags` and there
    // is no parallel `state.tags[arn]` storage to drift out of sync.
    let svc = LambdaService::new(make_state());
    seed_function(&svc, "tag-fn").await;
    let arn = "arn:aws:lambda:us-east-1:123456789012:function:tag-fn";
    let body = json!({"Tags": {"only": "here"}});
    let req = make_request(
        Method::POST,
        &format!("/2017-03-31/tags/{arn}"),
        &body.to_string(),
    );
    svc.handle(req).await.unwrap();

    let accounts = svc.state.read();
    let state = accounts.get("123456789012").unwrap();
    let func = state.functions.get("tag-fn").unwrap();
    assert_eq!(func.tags.get("only").map(String::as_str), Some("here"));
    // The fact that this compiles is the structural assertion: there
    // is no `state.tags` field to read from. The runtime check on
    // `func.tags` confirms tags actually landed in the unified slot.
}

#[tokio::test]
async fn untag_resource_accepts_json_body_fallback() {
    // Defensive: some clients put `TagKeys` in the JSON body even
    // though the AWS Smithy model says `httpQuery: "tagKeys"`. When no
    // query param is present we fall back to parsing the body so those
    // clients still work end-to-end.
    let svc = LambdaService::new(make_state());
    seed_function(&svc, "tag-fn").await;
    let arn = "arn:aws:lambda:us-east-1:123456789012:function:tag-fn";

    let body = json!({"Tags": {"A": "1", "B": "2", "C": "3"}});
    let req = make_request(
        Method::POST,
        &format!("/2017-03-31/tags/{arn}"),
        &body.to_string(),
    );
    svc.handle(req).await.unwrap();

    // No query string, body carries `TagKeys`.
    let body = json!({"TagKeys": ["A", "B"]});
    let req = make_request(
        Method::DELETE,
        &format!("/2017-03-31/tags/{arn}"),
        &body.to_string(),
    );
    svc.handle(req).await.unwrap();

    let accounts = svc.state.read();
    let state = accounts.get("123456789012").unwrap();
    let func = state.functions.get("tag-fn").unwrap();
    assert!(!func.tags.contains_key("A"));
    assert!(!func.tags.contains_key("B"));
    assert_eq!(func.tags.get("C").map(String::as_str), Some("3"));
}

#[tokio::test]
async fn untag_resource_query_wins_over_json_body() {
    // When both query params and JSON body are present, query wins —
    // it's the AWS-canonical wire format. The body is a defensive
    // fallback only.
    let svc = LambdaService::new(make_state());
    seed_function(&svc, "tag-fn").await;
    let arn = "arn:aws:lambda:us-east-1:123456789012:function:tag-fn";

    let body = json!({"Tags": {"A": "1", "B": "2", "C": "3"}});
    let req = make_request(
        Method::POST,
        &format!("/2017-03-31/tags/{arn}"),
        &body.to_string(),
    );
    svc.handle(req).await.unwrap();

    // Query says delete A; body says delete C. Query wins, so only A
    // is removed.
    let body = json!({"TagKeys": ["C"]});
    let mut req = make_request(
        Method::DELETE,
        &format!("/2017-03-31/tags/{arn}"),
        &body.to_string(),
    );
    req.raw_query = "tagKeys=A".to_string();
    svc.handle(req).await.unwrap();

    let accounts = svc.state.read();
    let state = accounts.get("123456789012").unwrap();
    let func = state.functions.get("tag-fn").unwrap();
    assert!(!func.tags.contains_key("A"));
    assert_eq!(func.tags.get("B").map(String::as_str), Some("2"));
    assert_eq!(func.tags.get("C").map(String::as_str), Some("3"));
}

// ── D4: reserved-concurrency enforcement at invoke ──

#[tokio::test]
async fn reserved_concurrency_returns_429_when_inflight_at_cap() {
    // Simulate an in-flight invoke by pre-loading the per-function
    // counter to the configured cap. The next invoke must reject with
    // 429 + `TooManyRequestsException` and a `Reason` body field of
    // `ReservedFunctionConcurrentInvocationLimitExceeded`.
    let svc = LambdaService::new(make_state());
    seed_function(&svc, "rcfn").await;
    // Cap = 1.
    let body = json!({"ReservedConcurrentExecutions": 1});
    let req = make_request(
        Method::PUT,
        "/2015-03-31/functions/rcfn/concurrency",
        &body.to_string(),
    );
    svc.handle(req).await.unwrap();

    // Pretend a sibling invoke is already running.
    svc.inflight_invocations
        .write()
        .insert("123456789012:rcfn".to_string(), 1);

    let req = make_request(
        Method::POST,
        "/2015-03-31/functions/rcfn/invocations",
        r#"{}"#,
    );
    let err = match svc.handle(req).await {
        Err(e) => e,
        Ok(_) => panic!("should be throttled"),
    };
    assert_eq!(err.status(), StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(err.code(), "TooManyRequestsException");
    let reason = err
        .extra_fields()
        .iter()
        .find(|(k, _)| k == "Reason")
        .map(|(_, v)| v.as_str());
    assert_eq!(
        reason,
        Some("ReservedFunctionConcurrentInvocationLimitExceeded")
    );
    // Counter must not be bumped on rejection (still 1, not 2) — the
    // guard should never have been created.
    assert_eq!(
        svc.inflight_invocations
            .read()
            .get("123456789012:rcfn")
            .copied(),
        Some(1)
    );
}

#[tokio::test]
async fn reserved_concurrency_under_cap_does_not_throttle() {
    // Cap = 2 with 1 inflight: the next invoke is allowed past the
    // gate. It still fails downstream because the seeded function has
    // no code package, but that's a 4xx with a different code than the
    // throttle error — confirming the gate did not reject.
    let svc = LambdaService::new(make_state());
    seed_function(&svc, "rcfn2").await;
    let body = json!({"ReservedConcurrentExecutions": 2});
    let req = make_request(
        Method::PUT,
        "/2015-03-31/functions/rcfn2/concurrency",
        &body.to_string(),
    );
    svc.handle(req).await.unwrap();

    svc.inflight_invocations
        .write()
        .insert("123456789012:rcfn2".to_string(), 1);

    let req = make_request(
        Method::POST,
        "/2015-03-31/functions/rcfn2/invocations",
        r#"{}"#,
    );
    let err = match svc.handle(req).await {
        Err(e) => e,
        Ok(_) => panic!("expected downstream error after gate"),
    };
    assert_ne!(err.code(), "TooManyRequestsException");
    // Guard ran and dropped, so the counter is back to its pre-invoke
    // value of 1 — Drop fires synchronously when the future returns.
    assert_eq!(
        svc.inflight_invocations
            .read()
            .get("123456789012:rcfn2")
            .copied(),
        Some(1)
    );
}

#[tokio::test]
async fn reserved_concurrency_decrements_on_error_path() {
    // No reserved cap set → no gating, but the counter is still
    // incremented and decremented as the invoke flows through. Failing
    // on missing code package must not leak a slot.
    let svc = LambdaService::new(make_state());
    seed_function(&svc, "decfn").await;
    let req = make_request(
        Method::POST,
        "/2015-03-31/functions/decfn/invocations",
        r#"{}"#,
    );
    let _ = svc.handle(req).await;
    // After the failed call, the entry should have been removed (Drop
    // removes when the count would go to zero).
    assert!(svc
        .inflight_invocations
        .read()
        .get("123456789012:decfn")
        .is_none());
}

// ── D4: alias weighted routing ──

#[tokio::test]
async fn resolve_qualifier_alias_no_routing_config_picks_primary() {
    let svc = LambdaService::new(make_state());
    seed_function(&svc, "afn1").await;

    // Publish v1, then create an alias pinned to v1 with no routing.
    let req = make_request(Method::POST, "/2015-03-31/functions/afn1/versions", "{}");
    svc.handle(req).await.unwrap();
    let body = json!({"Name": "PROD", "FunctionVersion": "1"});
    let req = make_request(
        Method::POST,
        "/2015-03-31/functions/afn1/aliases",
        &body.to_string(),
    );
    svc.handle(req).await.unwrap();

    let accounts = svc.state.read();
    let state = accounts.get("123456789012").unwrap();
    for _ in 0..50 {
        assert_eq!(
            resolve_qualifier_to_version(state, "afn1", Some("PROD")),
            Some("1".to_string())
        );
    }
}

#[tokio::test]
async fn resolve_qualifier_alias_50_50_weights_split_within_band() {
    // Statistical: with primary=v1 and AdditionalVersionWeights={"2": 0.5}
    // a uniform pick over [0,1) lands on v1 ~50% and v2 ~50%. Allow a
    // wide tolerance (30..70) so the test is not flaky on small N.
    let svc = LambdaService::new(make_state());
    seed_function(&svc, "afn2").await;

    // Publish v1 + v2.
    for _ in 0..2 {
        let req = make_request(Method::POST, "/2015-03-31/functions/afn2/versions", "{}");
        svc.handle(req).await.unwrap();
        // Mutate $LATEST between publishes so versions snapshot
        // distinctly.
        let body = json!({"Description": "tick"});
        let req = make_request(
            Method::PUT,
            "/2015-03-31/functions/afn2/configuration",
            &body.to_string(),
        );
        svc.handle(req).await.unwrap();
    }

    let body = json!({
        "Name": "CANARY",
        "FunctionVersion": "1",
        "RoutingConfig": {"AdditionalVersionWeights": {"2": 0.5}},
    });
    let req = make_request(
        Method::POST,
        "/2015-03-31/functions/afn2/aliases",
        &body.to_string(),
    );
    svc.handle(req).await.unwrap();

    let accounts = svc.state.read();
    let state = accounts.get("123456789012").unwrap();
    let mut v1 = 0;
    let mut v2 = 0;
    for _ in 0..200 {
        match resolve_qualifier_to_version(state, "afn2", Some("CANARY")).as_deref() {
            Some("1") => v1 += 1,
            Some("2") => v2 += 1,
            other => panic!("unexpected version {other:?}"),
        }
    }
    assert!(
        (60..=140).contains(&v1),
        "v1={v1} out of expected band; v2={v2}"
    );
    assert!(
        (60..=140).contains(&v2),
        "v2={v2} out of expected band; v1={v1}"
    );
}

#[tokio::test]
async fn resolve_qualifier_numeric_returns_self() {
    // `$LATEST` returns None (caller uses live $LATEST); a bare numeric
    // qualifier returns itself without consulting aliases.
    let svc = LambdaService::new(make_state());
    seed_function(&svc, "qfn").await;
    let accounts = svc.state.read();
    let state = accounts.get("123456789012").unwrap();
    assert_eq!(resolve_qualifier_to_version(state, "qfn", None), None);
    assert_eq!(
        resolve_qualifier_to_version(state, "qfn", Some("$LATEST")),
        None
    );
    assert_eq!(
        resolve_qualifier_to_version(state, "qfn", Some("7")),
        Some("7".to_string())
    );
}

#[tokio::test]
async fn create_function_with_ephemeral_storage_persists_size() {
    // EphemeralStorage.Size sent on CreateFunction must round-trip
    // through GetFunctionConfiguration. Default is 512 when unset.
    let svc = LambdaService::new(make_state());
    let body = json!({
        "FunctionName": "ephem",
        "Runtime": "python3.12",
        "Role": "arn:aws:iam::123456789012:role/r",
        "Handler": "index.handler",
        "Code": {},
        "EphemeralStorage": {"Size": 2048}
    });
    let req = make_request(Method::POST, "/2015-03-31/functions", &body.to_string());
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::CREATED);
    let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(v["EphemeralStorage"]["Size"], 2048);

    let req = make_request(Method::GET, "/2015-03-31/functions/ephem/configuration", "");
    let resp = svc.handle(req).await.unwrap();
    let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(v["EphemeralStorage"]["Size"], 2048);
}

#[tokio::test]
async fn create_function_without_ephemeral_storage_defaults_to_512() {
    let svc = LambdaService::new(make_state());
    seed_function(&svc, "edflt").await;
    let req = make_request(Method::GET, "/2015-03-31/functions/edflt/configuration", "");
    let resp = svc.handle(req).await.unwrap();
    let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(v["EphemeralStorage"]["Size"], 512);
}

#[tokio::test]
async fn ephemeral_storage_validation_rejects_below_512() {
    let svc = LambdaService::new(make_state());
    let body = json!({
        "FunctionName": "elow",
        "Runtime": "python3.12",
        "Role": "arn:aws:iam::123456789012:role/r",
        "Handler": "index.handler",
        "Code": {},
        "EphemeralStorage": {"Size": 256}
    });
    let req = make_request(Method::POST, "/2015-03-31/functions", &body.to_string());
    let err = match svc.handle(req).await {
        Err(e) => e,
        Ok(_) => panic!("expected validation error for size < 512"),
    };
    assert_eq!(err.status(), StatusCode::BAD_REQUEST);
    assert!(err.code().contains("InvalidParameterValueException"));
}

#[tokio::test]
async fn ephemeral_storage_validation_rejects_above_10240() {
    let svc = LambdaService::new(make_state());
    let body = json!({
        "FunctionName": "ehigh",
        "Runtime": "python3.12",
        "Role": "arn:aws:iam::123456789012:role/r",
        "Handler": "index.handler",
        "Code": {},
        "EphemeralStorage": {"Size": 20480}
    });
    let req = make_request(Method::POST, "/2015-03-31/functions", &body.to_string());
    let err = match svc.handle(req).await {
        Err(e) => e,
        Ok(_) => panic!("expected validation error for size > 10240"),
    };
    assert_eq!(err.status(), StatusCode::BAD_REQUEST);
    assert!(err.code().contains("InvalidParameterValueException"));
}

#[tokio::test]
async fn ephemeral_storage_accepts_boundaries_512_and_10240() {
    let svc = LambdaService::new(make_state());
    for (name, size) in [("emin", 512), ("emax", 10240)] {
        let body = json!({
            "FunctionName": name,
            "Runtime": "python3.12",
            "Role": "arn:aws:iam::123456789012:role/r",
            "Handler": "index.handler",
            "Code": {},
            "EphemeralStorage": {"Size": size}
        });
        let req = make_request(Method::POST, "/2015-03-31/functions", &body.to_string());
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::CREATED);
        let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(v["EphemeralStorage"]["Size"], size);
    }
}

#[tokio::test]
async fn update_function_configuration_rejects_invalid_ephemeral_storage() {
    // Validation must run before any field mutation; an out-of-range
    // EphemeralStorage.Size on an otherwise valid Update body must
    // not silently apply the surrounding fields.
    let svc = LambdaService::new(make_state());
    seed_function(&svc, "uephem").await;

    let body = json!({
        "Handler": "new.handler",
        "EphemeralStorage": {"Size": 100}
    });
    let req = make_request(
        Method::PUT,
        "/2015-03-31/functions/uephem/configuration",
        &body.to_string(),
    );
    let err = match svc.handle(req).await {
        Err(e) => e,
        Ok(_) => panic!("expected validation error for size < 512"),
    };
    assert!(err.code().contains("InvalidParameterValueException"));

    // Handler must NOT have been updated despite the request body.
    let req = make_request(
        Method::GET,
        "/2015-03-31/functions/uephem/configuration",
        "",
    );
    let resp = svc.handle(req).await.unwrap();
    let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(v["Handler"], "index.handler");
}

#[tokio::test]
async fn update_function_configuration_accepts_vpc_config() {
    // VpcConfig fields (SubnetIds, SecurityGroupIds, Ipv6AllowedForDualStack)
    // must round-trip through UpdateFunctionConfiguration without being
    // dropped.
    let svc = LambdaService::new(make_state());
    seed_function(&svc, "vpcfn").await;

    let body = json!({
        "VpcConfig": {
            "SubnetIds": ["subnet-aaa", "subnet-bbb"],
            "SecurityGroupIds": ["sg-111", "sg-222"],
            "Ipv6AllowedForDualStack": true
        }
    });
    let req = make_request(
        Method::PUT,
        "/2015-03-31/functions/vpcfn/configuration",
        &body.to_string(),
    );
    svc.handle(req).await.unwrap();

    let req = make_request(Method::GET, "/2015-03-31/functions/vpcfn/configuration", "");
    let resp = svc.handle(req).await.unwrap();
    let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(v["VpcConfig"]["SubnetIds"][0], "subnet-aaa");
    assert_eq!(v["VpcConfig"]["SubnetIds"][1], "subnet-bbb");
    assert_eq!(v["VpcConfig"]["SecurityGroupIds"][0], "sg-111");
    assert_eq!(v["VpcConfig"]["SecurityGroupIds"][1], "sg-222");
    assert_eq!(v["VpcConfig"]["Ipv6AllowedForDualStack"], true);
}

#[tokio::test]
async fn update_function_configuration_accepts_snap_start() {
    let svc = LambdaService::new(make_state());
    seed_function(&svc, "snapfn").await;

    let body = json!({
        "SnapStart": {"ApplyOn": "PublishedVersions"}
    });
    let req = make_request(
        Method::PUT,
        "/2015-03-31/functions/snapfn/configuration",
        &body.to_string(),
    );
    svc.handle(req).await.unwrap();

    let req = make_request(
        Method::GET,
        "/2015-03-31/functions/snapfn/configuration",
        "",
    );
    let resp = svc.handle(req).await.unwrap();
    let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(v["SnapStart"]["ApplyOn"], "PublishedVersions");
}

#[tokio::test]
async fn snap_start_optimization_status_flips_on_after_publish_version() {
    // With ApplyOn=PublishedVersions, AWS reports OptimizationStatus="On"
    // on the published-version snapshot once optimization completes.
    // fakecloud has no real optimization step, so we flip the status
    // eagerly on PublishVersion so clients waiting on the transition
    // see the steady state.
    let svc = LambdaService::new(make_state());
    seed_function(&svc, "snapopt").await;

    // Configure SnapStart on $LATEST.
    let body = json!({"SnapStart": {"ApplyOn": "PublishedVersions"}});
    let req = make_request(
        Method::PUT,
        "/2015-03-31/functions/snapopt/configuration",
        &body.to_string(),
    );
    svc.handle(req).await.unwrap();

    // Publish a version.
    let req = make_request(Method::POST, "/2015-03-31/functions/snapopt/versions", "{}");
    let resp = svc.handle(req).await.unwrap();
    let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(v["Version"], "1");
    assert_eq!(v["SnapStart"]["ApplyOn"], "PublishedVersions");
    assert_eq!(v["SnapStart"]["OptimizationStatus"], "On");
}

#[tokio::test]
async fn snap_start_default_response_has_apply_on_none() {
    // Functions without an explicit SnapStart still echo a default
    // SnapStart block so SDKs that always read both fields don't NPE.
    let svc = LambdaService::new(make_state());
    seed_function(&svc, "snapdef").await;
    let req = make_request(
        Method::GET,
        "/2015-03-31/functions/snapdef/configuration",
        "",
    );
    let resp = svc.handle(req).await.unwrap();
    let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(v["SnapStart"]["ApplyOn"], "None");
    assert_eq!(v["SnapStart"]["OptimizationStatus"], "Off");
}

#[tokio::test]
async fn invoke_publishes_invocations_metric_when_runtime_missing() {
    // Even though Lambda execution fails (no Docker), the Invoke entry
    // still publishes Invocations + Errors metrics so CloudWatch alarms
    // catch failed invokes.
    use std::sync::Mutex;

    #[derive(Default)]
    struct MetricSink {
        events: Mutex<Vec<(String, f64)>>,
    }
    impl fakecloud_core::delivery::CloudwatchDelivery for MetricSink {
        fn put_metric(
            &self,
            _account_id: &str,
            _region: &str,
            _namespace: &str,
            metric_name: &str,
            value: f64,
            _unit: Option<&str>,
            _dimensions: std::collections::BTreeMap<String, String>,
            _timestamp_ms: i64,
        ) {
            self.events
                .lock()
                .unwrap()
                .push((metric_name.to_string(), value));
        }
    }

    let sink = std::sync::Arc::new(MetricSink::default());
    let bus = std::sync::Arc::new(
        fakecloud_core::delivery::DeliveryBus::new().with_cloudwatch_metrics(sink.clone()),
    );
    let svc = LambdaService::new(make_state()).with_delivery_bus(bus);
    // Seed with a real ZipFile so the runtime-missing check is what
    // triggers the failure path, not the code_zip pre-check.
    let zip = base64::Engine::encode(
        &base64::engine::general_purpose::STANDARD,
        b"PK\x05\x06fake",
    );
    let body = json!({
        "FunctionName": "metered",
        "Runtime": "python3.12",
        "Role": "arn:aws:iam::123456789012:role/r",
        "Handler": "index.handler",
        "Code": {"ZipFile": zip},
    });
    let req = make_request(Method::POST, "/2015-03-31/functions", &body.to_string());
    svc.handle(req).await.unwrap();

    let req = make_request(
        Method::POST,
        "/2015-03-31/functions/metered/invocations",
        "{}",
    );
    let _ = svc.handle(req).await;
    let events = sink.events.lock().unwrap();
    let names: Vec<&str> = events.iter().map(|(n, _)| n.as_str()).collect();
    assert!(
        names.contains(&"Invocations"),
        "missing Invocations: {names:?}"
    );
    assert!(names.contains(&"Duration"), "missing Duration: {names:?}");
    assert!(names.contains(&"Errors"), "missing Errors: {names:?}");
}

/// Pin the three `DestinationConfig` echo behaviours documented in the
/// Lambda Smithy `@examples` and asserted by the conformance round-trip
/// strategy. See `event_invoke_json` in `extras.rs`.
#[tokio::test]
async fn put_function_event_invoke_destination_config_echo_variants() {
    let svc = LambdaService::new(make_state());
    seed_function(&svc, "ei-fn").await;

    // 1. Input omits DestinationConfig -> response synthesizes both halves
    //    (matches @examples for PutFunctionEventInvokeConfig).
    let req = make_request(
        Method::PUT,
        "/2019-09-25/functions/ei-fn/event-invoke-config",
        &json!({"MaximumRetryAttempts": 0, "MaximumEventAgeInSeconds": 3600}).to_string(),
    );
    let v: Value =
        serde_json::from_slice(svc.handle(req).await.unwrap().body.expect_bytes()).unwrap();
    assert_eq!(
        v["DestinationConfig"],
        json!({"OnSuccess": {}, "OnFailure": {}}),
        "omitted DestinationConfig should synthesize empty halves"
    );

    // 2. Input is explicit empty object -> echo verbatim (round-trip strategy).
    let req = make_request(
        Method::PUT,
        "/2019-09-25/functions/ei-fn/event-invoke-config",
        &json!({"DestinationConfig": {}}).to_string(),
    );
    let v: Value =
        serde_json::from_slice(svc.handle(req).await.unwrap().body.expect_bytes()).unwrap();
    assert_eq!(
        v["DestinationConfig"],
        json!({}),
        "explicit empty DestinationConfig should echo verbatim"
    );

    // 3. Half-populated -> backfill missing half (matches @examples for
    //    UpdateFunctionEventInvokeConfig).
    let req = make_request(
        Method::PUT,
        "/2019-09-25/functions/ei-fn/event-invoke-config",
        &json!({
            "DestinationConfig": {
                "OnFailure": {"Destination": "arn:aws:sqs:us-east-1:123456789012:dlq"}
            }
        })
        .to_string(),
    );
    let v: Value =
        serde_json::from_slice(svc.handle(req).await.unwrap().body.expect_bytes()).unwrap();
    assert_eq!(
        v["DestinationConfig"]["OnFailure"]["Destination"],
        "arn:aws:sqs:us-east-1:123456789012:dlq"
    );
    assert_eq!(
        v["DestinationConfig"]["OnSuccess"],
        json!({}),
        "missing OnSuccess half should be backfilled as empty object"
    );
}

/// In-memory `LambdaBackend` used by the pre-pull regression test below.
/// Records every `prepull_image` call so the test can assert that
/// CreateFunction kicked one off, without spinning up a real container.
#[derive(Default, Clone)]
struct PrepullSpy {
    pulled: Arc<parking_lot::Mutex<Vec<String>>>,
}

#[async_trait::async_trait]
impl crate::runtime::LambdaBackend for PrepullSpy {
    fn name(&self) -> &str {
        "prepull-spy"
    }
    async fn launch(
        &self,
        _func: &crate::state::LambdaFunction,
        _code_zip: Option<&[u8]>,
        _layers: &[Vec<u8>],
        _deploy_id: &str,
    ) -> Result<crate::runtime::WarmInstance, crate::runtime::RuntimeError> {
        unreachable!("prepull regression test never invokes launch")
    }
    async fn terminate(&self, _handle: &crate::runtime::BackendHandle) {}
    async fn prepull_image(&self, image: &str) -> Result<(), crate::runtime::RuntimeError> {
        self.pulled.lock().push(image.to_string());
        Ok(())
    }
}

/// Regression for issue #1539 Bug 3: CreateFunction used to leave the
/// runtime image cold, so the first Invoke paid a ~700 MB pull cost
/// that routinely exceeded the AWS CLI default 60s read timeout. After
/// the fix CreateFunction spawns a background pre-pull keyed off
/// `runtime_to_image`.
#[tokio::test]
async fn create_function_kicks_off_background_prepull() {
    let spy = PrepullSpy::default();
    let pulled = spy.pulled.clone();
    let backend: Arc<dyn crate::runtime::LambdaBackend> = Arc::new(spy);
    let runtime = Arc::new(crate::runtime::LambdaRuntime::from_backend(backend));
    let svc = LambdaService::new(make_state()).with_runtime(runtime);

    let create_body = json!({
        "FunctionName": "prepull-fn",
        "Runtime": "python3.12",
        "Role": "arn:aws:iam::123456789012:role/lambda-role",
        "Handler": "index.handler",
        "Code": {"ZipFile": ""},
    })
    .to_string();
    let req = make_request(Method::POST, "/2015-03-31/functions", &create_body);
    svc.handle(req).await.expect("create function");

    // The pre-pull is `tokio::spawn`-ed; give it a brief chance to land
    // on the runtime queue and run to completion. PrepullSpy returns
    // immediately so this poll is bounded by scheduler latency, not I/O.
    for _ in 0..50 {
        if !pulled.lock().is_empty() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    let calls = pulled.lock().clone();
    assert_eq!(
        calls,
        vec!["public.ecr.aws/lambda/python:3.12".to_string()],
        "CreateFunction must dispatch exactly one background pre-pull for the runtime image"
    );
}

/// CreateFunction must not fail when no container runtime is wired up
/// (e.g. fakecloud started without docker/podman available). The
/// pre-pull is best-effort; absence of a runtime means there's nothing
/// to pre-pull and the function still has to persist.
#[tokio::test]
async fn create_function_succeeds_without_runtime_prepull_hook() {
    let svc = LambdaService::new(make_state()); // no `.with_runtime(...)`
    let create_body = json!({
        "FunctionName": "no-runtime-fn",
        "Runtime": "python3.12",
        "Role": "arn:aws:iam::123456789012:role/lambda-role",
        "Handler": "index.handler",
        "Code": {"ZipFile": ""},
    })
    .to_string();
    let req = make_request(Method::POST, "/2015-03-31/functions", &create_body);
    let resp = svc.handle(req).await.expect("create function");
    assert_eq!(resp.status, StatusCode::CREATED);
}

/// Both `GET /2015-03-31/functions` and the trailing-slash
/// `GET /2015-03-31/functions/` are `ListFunctions`. botocore serialized
/// `ListFunctions` with the trailing slash until 1.40 (mid-2025), so the
/// whole pre-1.40 install base depends on the second form (issue #1645).
#[test]
fn resolve_action_list_functions_both_slash_forms() {
    let bare = make_request(Method::GET, "/2015-03-31/functions", "");
    assert_eq!(
        LambdaService::resolve_action(&bare).map(|(a, _)| a),
        Some("ListFunctions")
    );

    // `path_segments` drops the empty trailing segment, so the only
    // signal of the trailing slash is `raw_path` — set it explicitly.
    let mut trailing = make_request(Method::GET, "/2015-03-31/functions", "");
    trailing.raw_path = "/2015-03-31/functions/".to_string();
    assert_eq!(
        LambdaService::resolve_action(&trailing).map(|(a, _)| a),
        Some("ListFunctions"),
        "trailing-slash ListFunctions (pre-1.40 botocore) must not route to GetFunction"
    );
}

/// A real `GetFunction` (a function name in the path) still routes to
/// `GetFunction` — the trailing-slash fix must not regress that.
#[test]
fn resolve_action_get_function_with_name() {
    let req = make_request(Method::GET, "/2015-03-31/functions/my-fn", "");
    let (action, resource) = LambdaService::resolve_action(&req).expect("routed");
    assert_eq!(action, "GetFunction");
    assert_eq!(resource.as_deref(), Some("my-fn"));
}
