//! Lambda Managed Instances functions through the service handler:
//! `CapacityProviderConfig` on CreateFunction / UpdateFunctionConfiguration,
//! its round trip through every `FunctionConfiguration`, `$LATEST.PUBLISHED`,
//! `ListFunctionVersionsByCapacityProvider` and the in-use refusal of
//! `DeleteCapacityProvider`.

use super::*;
use bytes::Bytes;
use http::{HeaderMap, Method};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;

const ACCOUNT: &str = "123456789012";
const CP_ARN: &str = "arn:aws:lambda:us-east-1:123456789012:capacity-provider:cp1";

fn make_state() -> SharedLambdaState {
    Arc::new(RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new(ACCOUNT, "us-east-1", ""),
    ))
}

fn request(method: Method, path: &str, body: &Value) -> AwsRequest {
    let body = if body.is_null() {
        String::new()
    } else {
        body.to_string()
    };
    AwsRequest {
        service: "lambda".to_string(),
        action: String::new(),
        region: "us-east-1".to_string(),
        account_id: ACCOUNT.to_string(),
        request_id: "test-request-id".to_string(),
        headers: HeaderMap::new(),
        query_params: HashMap::new(),
        body: Bytes::from(body),
        body_stream: parking_lot::Mutex::new(None),
        path_segments: fakecloud_core::path::split_path_segments(path),
        raw_path: path.to_string(),
        raw_query: String::new(),
        method,
        is_query_protocol: false,
        access_key_id: None,
        principal: None,
    }
}

fn json_body(resp: &AwsResponse) -> Value {
    serde_json::from_slice(resp.body.expect_bytes()).unwrap()
}

async fn call(svc: &LambdaService, req: AwsRequest) -> Result<Value, AwsServiceError> {
    svc.handle(req).await.map(|r| json_body(&r))
}

async fn create_provider(svc: &LambdaService, name: &str) {
    let body = json!({
        "CapacityProviderName": name,
        "VpcConfig": {"SubnetIds": ["subnet-1"], "SecurityGroupIds": ["sg-1"]},
        "PermissionsConfig": {
            "CapacityProviderOperatorRoleArn": "arn:aws:iam::123456789012:role/operator"
        },
    });
    call(
        svc,
        request(Method::POST, "/2025-11-30/capacity-providers", &body),
    )
    .await
    .unwrap();
}

fn managed_function(name: &str, cpc: Value) -> Value {
    json!({
        "FunctionName": name,
        "Runtime": "python3.13",
        "Role": "arn:aws:iam::123456789012:role/r",
        "Handler": "index.handler",
        "Code": {},
        "CapacityProviderConfig": cpc,
    })
}

fn cpc(arn: &str) -> Value {
    json!({"LambdaManagedInstancesCapacityProviderConfig": {"CapacityProviderArn": arn}})
}

async fn create_function(svc: &LambdaService, body: Value) -> Result<Value, AwsServiceError> {
    call(svc, request(Method::POST, "/2015-03-31/functions", &body)).await
}

async fn versions_on(svc: &LambdaService, provider: &str) -> Value {
    call(
        svc,
        request(
            Method::GET,
            &format!("/2025-11-30/capacity-providers/{provider}/function-versions"),
            &Value::Null,
        ),
    )
    .await
    .unwrap()
}

#[tokio::test]
async fn capacity_provider_arn_uses_a_colon() {
    let svc = LambdaService::new(make_state());
    create_provider(&svc, "cp1").await;
    let got = call(
        &svc,
        request(
            Method::GET,
            "/2025-11-30/capacity-providers/cp1",
            &Value::Null,
        ),
    )
    .await
    .unwrap();
    assert_eq!(got["CapacityProvider"]["CapacityProviderArn"], CP_ARN);
    // The name label also takes the provider's ARN.
    let got = call(
        &svc,
        request(
            Method::GET,
            &format!("/2025-11-30/capacity-providers/{CP_ARN}"),
            &Value::Null,
        ),
    )
    .await
    .unwrap();
    assert_eq!(got["CapacityProvider"]["CapacityProviderArn"], CP_ARN);
}

#[tokio::test]
async fn create_function_stores_and_returns_the_capacity_provider_config() {
    let svc = LambdaService::new(make_state());
    create_provider(&svc, "cp1").await;
    let mut body = managed_function("mi", cpc(CP_ARN));
    body["CapacityProviderConfig"]["LambdaManagedInstancesCapacityProviderConfig"]
        ["PerExecutionEnvironmentMaxConcurrency"] = json!(32);
    let created = create_function(&svc, body).await.unwrap();
    let expected = json!({"LambdaManagedInstancesCapacityProviderConfig": {
        "CapacityProviderArn": CP_ARN,
        "PerExecutionEnvironmentMaxConcurrency": 32,
        "ExecutionEnvironmentMemoryGiBPerVCpu": 2.0,
    }});
    assert_eq!(created["CapacityProviderConfig"], expected);
    // Managed Instances functions default to 2048 MB, not 128.
    assert_eq!(created["MemorySize"], 2048);

    let got = call(
        &svc,
        request(
            Method::GET,
            "/2015-03-31/functions/mi/configuration",
            &Value::Null,
        ),
    )
    .await
    .unwrap();
    assert_eq!(got["CapacityProviderConfig"], expected);
    let got = call(
        &svc,
        request(Method::GET, "/2015-03-31/functions/mi", &Value::Null),
    )
    .await
    .unwrap();
    assert_eq!(got["Configuration"]["CapacityProviderConfig"], expected);
    let listed = call(
        &svc,
        request(Method::GET, "/2015-03-31/functions", &Value::Null),
    )
    .await
    .unwrap();
    assert_eq!(listed["Functions"][0]["CapacityProviderConfig"], expected);

    // A published version carries the config too.
    let v1 = call(
        &svc,
        request(
            Method::POST,
            "/2015-03-31/functions/mi/versions",
            &json!({}),
        ),
    )
    .await
    .unwrap();
    assert_eq!(v1["Version"], "1");
    assert_eq!(v1["CapacityProviderConfig"], expected);
}

#[tokio::test]
async fn plain_functions_carry_no_capacity_provider_config() {
    let svc = LambdaService::new(make_state());
    let created = create_function(
        &svc,
        json!({
            "FunctionName": "plain",
            "Runtime": "python3.12",
            "Role": "arn:aws:iam::123456789012:role/r",
            "Handler": "index.handler",
            "Code": {},
        }),
    )
    .await
    .unwrap();
    assert!(created.get("CapacityProviderConfig").is_none());
    assert_eq!(created["MemorySize"], 128);
}

#[tokio::test]
async fn create_function_refuses_a_missing_or_misplaced_provider() {
    let svc = LambdaService::new(make_state());
    let err = create_function(&svc, managed_function("mi", cpc(CP_ARN)))
        .await
        .unwrap_err();
    assert_eq!(err.status(), StatusCode::NOT_FOUND);
    assert_eq!(err.code(), "ResourceNotFoundException");
    assert_eq!(
        err.message(),
        format!(
            "Capacity provider not found: {CP_ARN}. Check that the ARN is correct and the capacity provider exists in this Region."
        )
    );

    let err = create_function(
        &svc,
        managed_function(
            "mi",
            cpc("arn:aws:lambda:eu-west-1:123456789012:capacity-provider:cp1"),
        ),
    )
    .await
    .unwrap_err();
    assert_eq!(err.code(), "InvalidParameterValueException");

    let err = create_function(
        &svc,
        managed_function(
            "mi",
            cpc("arn:aws:lambda:us-east-1:123456789012:capacity-provider/cp1"),
        ),
    )
    .await
    .unwrap_err();
    assert_eq!(err.code(), "ValidationException");

    // Nothing was created by the refused requests.
    let listed = call(
        &svc,
        request(Method::GET, "/2015-03-31/functions", &Value::Null),
    )
    .await
    .unwrap();
    assert_eq!(listed["Functions"].as_array().unwrap().len(), 0);
}

#[tokio::test]
async fn cross_account_provider_is_refused_under_strict_iam() {
    let svc = LambdaService::new(make_state()).with_iam_mode(fakecloud_core::auth::IamMode::Strict);
    let err = create_function(
        &svc,
        managed_function(
            "mi",
            cpc("arn:aws:lambda:us-east-1:111111111111:capacity-provider:cp1"),
        ),
    )
    .await
    .unwrap_err();
    assert_eq!(err.status(), StatusCode::FORBIDDEN);
    assert_eq!(err.code(), "AccessDeniedException");
    assert_eq!(
        err.message(),
        "Cross-account pass capacity-provider is not allowed."
    );
}

#[tokio::test]
async fn create_function_refuses_settings_managed_instances_cannot_run() {
    let svc = LambdaService::new(make_state());
    create_provider(&svc, "cp1").await;
    let mut low_memory = managed_function("mi", cpc(CP_ARN));
    low_memory["MemorySize"] = json!(1024);
    let err = create_function(&svc, low_memory).await.unwrap_err();
    assert_eq!(err.code(), "ValidationException");

    let mut old_runtime = managed_function("mi", cpc(CP_ARN));
    old_runtime["Runtime"] = json!("python3.9");
    let err = create_function(&svc, old_runtime).await.unwrap_err();
    assert_eq!(err.code(), "InvalidParameterValueException");

    let mut snap = managed_function("mi", cpc(CP_ARN));
    snap["Runtime"] = json!("java21");
    snap["SnapStart"] = json!({"ApplyOn": "PublishedVersions"});
    let err = create_function(&svc, snap).await.unwrap_err();
    assert!(err.message().contains("SnapStart"));

    let mut ratio = managed_function("mi", cpc(CP_ARN));
    ratio["CapacityProviderConfig"]["LambdaManagedInstancesCapacityProviderConfig"]
        ["ExecutionEnvironmentMemoryGiBPerVCpu"] = json!(4.0);
    let err = create_function(&svc, ratio).await.unwrap_err();
    assert_eq!(err.code(), "ValidationException");
    assert!(err.message().contains("exceeds function's total memory"));
}

#[tokio::test]
async fn update_function_configuration_adjusts_but_never_adds_a_provider() {
    let svc = LambdaService::new(make_state());
    create_provider(&svc, "cp1").await;
    create_provider(&svc, "cp2").await;
    create_function(
        &svc,
        json!({
            "FunctionName": "plain",
            "Runtime": "python3.13",
            "Role": "arn:aws:iam::123456789012:role/r",
            "Handler": "index.handler",
            "Code": {},
        }),
    )
    .await
    .unwrap();
    let err = call(
        &svc,
        request(
            Method::PUT,
            "/2015-03-31/functions/plain/configuration",
            &json!({"CapacityProviderConfig": cpc(CP_ARN), "MemorySize": 2048}),
        ),
    )
    .await
    .unwrap_err();
    assert_eq!(err.code(), "InvalidParameterValueException");
    assert_eq!(
        err.message(),
        "CapacityProviderConfig isn't supported for Lambda Default functions."
    );
    // The refused update changed nothing.
    let got = call(
        &svc,
        request(
            Method::GET,
            "/2015-03-31/functions/plain/configuration",
            &Value::Null,
        ),
    )
    .await
    .unwrap();
    assert_eq!(got["MemorySize"], 128);

    create_function(&svc, managed_function("mi", cpc(CP_ARN)))
        .await
        .unwrap();
    let cp2 = "arn:aws:lambda:us-east-1:123456789012:capacity-provider:cp2";
    let mut moved = cpc(cp2);
    moved["LambdaManagedInstancesCapacityProviderConfig"]["ExecutionEnvironmentMemoryGiBPerVCpu"] =
        json!(4.0);
    let updated = call(
        &svc,
        request(
            Method::PUT,
            "/2015-03-31/functions/mi/configuration",
            &json!({"CapacityProviderConfig": moved, "MemorySize": 4096}),
        ),
    )
    .await
    .unwrap();
    let inner = &updated["CapacityProviderConfig"]["LambdaManagedInstancesCapacityProviderConfig"];
    assert_eq!(inner["CapacityProviderArn"], cp2);
    assert_eq!(inner["ExecutionEnvironmentMemoryGiBPerVCpu"], 4.0);

    // Dropping below the Managed Instances minimum is refused as a whole.
    let err = call(
        &svc,
        request(
            Method::PUT,
            "/2015-03-31/functions/mi/configuration",
            &json!({"MemorySize": 1024, "Description": "x"}),
        ),
    )
    .await
    .unwrap_err();
    assert_eq!(err.code(), "ValidationException");
    let got = call(
        &svc,
        request(
            Method::GET,
            "/2015-03-31/functions/mi/configuration",
            &Value::Null,
        ),
    )
    .await
    .unwrap();
    assert_eq!(got["MemorySize"], 4096);
    assert_eq!(got["Description"], "");
}

#[tokio::test]
async fn list_function_versions_by_capacity_provider_reports_every_version() {
    let svc = LambdaService::new(make_state());
    create_provider(&svc, "cp1").await;
    create_provider(&svc, "empty").await;
    let empty = versions_on(&svc, "empty").await;
    assert_eq!(
        empty["CapacityProviderArn"],
        "arn:aws:lambda:us-east-1:123456789012:capacity-provider:empty"
    );
    assert_eq!(empty["FunctionVersions"], json!([]));

    create_function(&svc, managed_function("mi", cpc(CP_ARN)))
        .await
        .unwrap();
    call(
        &svc,
        request(
            Method::POST,
            "/2015-03-31/functions/mi/versions",
            &json!({}),
        ),
    )
    .await
    .unwrap();
    let published = call(
        &svc,
        request(
            Method::POST,
            "/2015-03-31/functions/mi/versions",
            &json!({"PublishTo": "LATEST_PUBLISHED"}),
        ),
    )
    .await
    .unwrap();
    assert_eq!(published["Version"], "$LATEST.PUBLISHED");
    let fn_arn = "arn:aws:lambda:us-east-1:123456789012:function:mi";
    assert_eq!(
        published["FunctionArn"],
        format!("{fn_arn}:$LATEST.PUBLISHED")
    );

    let listed = versions_on(&svc, "cp1").await;
    assert_eq!(listed["CapacityProviderArn"], CP_ARN);
    assert_eq!(
        listed["FunctionVersions"],
        json!([
            {"FunctionArn": format!("{fn_arn}:$LATEST"), "State": "Active"},
            {"FunctionArn": format!("{fn_arn}:$LATEST.PUBLISHED"), "State": "Active"},
            {"FunctionArn": format!("{fn_arn}:1"), "State": "Active"},
        ])
    );

    // Paginated with MaxItems / Marker.
    let mut req = request(
        Method::GET,
        "/2025-11-30/capacity-providers/cp1/function-versions",
        &Value::Null,
    );
    req.query_params.insert("MaxItems".into(), "2".into());
    let page = call(&svc, req).await.unwrap();
    assert_eq!(page["FunctionVersions"].as_array().unwrap().len(), 2);
    let marker = page["NextMarker"].as_str().unwrap().to_string();
    let mut req = request(
        Method::GET,
        "/2025-11-30/capacity-providers/cp1/function-versions",
        &Value::Null,
    );
    req.query_params.insert("Marker".into(), marker);
    let page = call(&svc, req).await.unwrap();
    assert_eq!(
        page["FunctionVersions"][0]["FunctionArn"],
        format!("{fn_arn}:1")
    );
    assert!(page.get("NextMarker").is_none());

    let mut req = request(
        Method::GET,
        "/2025-11-30/capacity-providers/cp1/function-versions",
        &Value::Null,
    );
    req.query_params.insert("MaxItems".into(), "51".into());
    assert_eq!(
        call(&svc, req).await.unwrap_err().status(),
        StatusCode::BAD_REQUEST
    );

    let err = call(
        &svc,
        request(
            Method::GET,
            "/2025-11-30/capacity-providers/missing/function-versions",
            &Value::Null,
        ),
    )
    .await
    .unwrap_err();
    assert_eq!(
        err.message(),
        "Capacity provider not found: missing. Check that the ARN is correct and the capacity provider exists in this Region."
    );
}

#[tokio::test]
async fn delete_capacity_provider_refuses_while_function_versions_use_it() {
    let svc = LambdaService::new(make_state());
    create_provider(&svc, "cp1").await;
    create_function(&svc, managed_function("mi", cpc(CP_ARN)))
        .await
        .unwrap();
    let err = call(
        &svc,
        request(
            Method::DELETE,
            "/2025-11-30/capacity-providers/cp1",
            &Value::Null,
        ),
    )
    .await
    .unwrap_err();
    assert_eq!(err.status(), StatusCode::CONFLICT);
    assert_eq!(err.code(), "ResourceConflictException");

    // Deleting the function frees the provider.
    svc.handle(request(
        Method::DELETE,
        "/2015-03-31/functions/mi",
        &Value::Null,
    ))
    .await
    .unwrap();
    assert_eq!(
        versions_on(&svc, "cp1").await["FunctionVersions"],
        json!([])
    );
    call(
        &svc,
        request(
            Method::DELETE,
            "/2025-11-30/capacity-providers/cp1",
            &Value::Null,
        ),
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn latest_published_is_managed_instances_only_and_readable() {
    let svc = LambdaService::new(make_state());
    create_function(
        &svc,
        json!({
            "FunctionName": "plain",
            "Runtime": "python3.13",
            "Role": "arn:aws:iam::123456789012:role/r",
            "Handler": "index.handler",
            "Code": {},
        }),
    )
    .await
    .unwrap();
    let err = call(
        &svc,
        request(
            Method::POST,
            "/2015-03-31/functions/plain/versions",
            &json!({"PublishTo": "LATEST_PUBLISHED"}),
        ),
    )
    .await
    .unwrap_err();
    assert_eq!(err.code(), "InvalidParameterValueException");
    let mut req = request(Method::GET, "/2015-03-31/functions/plain", &Value::Null);
    req.query_params
        .insert("Qualifier".into(), "$LATEST.PUBLISHED".into());
    let err = call(&svc, req).await.unwrap_err();
    assert_eq!(err.status(), StatusCode::NOT_FOUND);
    assert_eq!(
        err.message(),
        "Function not found: arn:aws:lambda:us-east-1:123456789012:function:plain:$LATEST.PUBLISHED"
    );

    create_provider(&svc, "cp1").await;
    let mut body = managed_function("mi", cpc(CP_ARN));
    body["PublishTo"] = json!("LATEST_PUBLISHED");
    body["Description"] = json!("first");
    let created = create_function(&svc, body).await.unwrap();
    assert_eq!(created["Version"], "$LATEST");
    let mut req = request(Method::GET, "/2015-03-31/functions/mi", &Value::Null);
    req.query_params
        .insert("Qualifier".into(), "$LATEST.PUBLISHED".into());
    let got = call(&svc, req).await.unwrap();
    assert_eq!(got["Configuration"]["Version"], "$LATEST.PUBLISHED");
    assert_eq!(got["Configuration"]["Description"], "first");

    // Republishing replaces it; no numbered version is created.
    call(
        &svc,
        request(
            Method::PUT,
            "/2015-03-31/functions/mi/configuration",
            &json!({"Description": "second"}),
        ),
    )
    .await
    .unwrap();
    call(
        &svc,
        request(
            Method::POST,
            "/2015-03-31/functions/mi/versions",
            &json!({"PublishTo": "LATEST_PUBLISHED"}),
        ),
    )
    .await
    .unwrap();
    let mut req = request(
        Method::GET,
        "/2015-03-31/functions/mi/configuration",
        &Value::Null,
    );
    req.query_params
        .insert("Qualifier".into(), "$LATEST.PUBLISHED".into());
    let got = call(&svc, req).await.unwrap();
    assert_eq!(got["Description"], "second");
    let versions = call(
        &svc,
        request(
            Method::GET,
            "/2015-03-31/functions/mi/versions",
            &Value::Null,
        ),
    )
    .await
    .unwrap();
    let listed: Vec<&str> = versions["Versions"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v["Version"].as_str().unwrap())
        .collect();
    assert_eq!(listed, vec!["$LATEST"]);

    // Unqualified invokes of a Managed Instances function target it.
    let accounts = svc.state.read();
    let state = accounts.regional(ACCOUNT, "us-east-1").unwrap();
    assert_eq!(
        default_invoke_qualifier(state, "mi"),
        Some(crate::state::LATEST_PUBLISHED)
    );
    assert_eq!(default_invoke_qualifier(state, "plain"), None);
}

#[tokio::test]
async fn create_function_with_publish_answers_with_version_1() {
    let svc = LambdaService::new(make_state());
    let created = create_function(
        &svc,
        json!({
            "FunctionName": "pub",
            "Runtime": "python3.12",
            "Role": "arn:aws:iam::123456789012:role/r",
            "Handler": "index.handler",
            "Code": {},
            "Publish": true,
        }),
    )
    .await
    .unwrap();
    assert_eq!(created["Version"], "1");
    assert_eq!(
        created["FunctionArn"],
        "arn:aws:lambda:us-east-1:123456789012:function:pub:1"
    );
}

#[tokio::test]
async fn function_version_quota_per_provider_is_enforced() {
    let svc = LambdaService::new(make_state());
    create_provider(&svc, "cp1").await;
    create_function(&svc, managed_function("mi", cpc(CP_ARN)))
        .await
        .unwrap();
    for i in 0..99 {
        // Each publish needs a change, or it returns the previous version.
        call(
            &svc,
            request(
                Method::PUT,
                "/2015-03-31/functions/mi/configuration",
                &json!({"Description": format!("d{i}")}),
            ),
        )
        .await
        .unwrap();
        call(
            &svc,
            request(
                Method::POST,
                "/2015-03-31/functions/mi/versions",
                &json!({}),
            ),
        )
        .await
        .unwrap();
    }
    call(
        &svc,
        request(
            Method::PUT,
            "/2015-03-31/functions/mi/configuration",
            &json!({"Description": "full"}),
        ),
    )
    .await
    .unwrap();
    let err = call(
        &svc,
        request(
            Method::POST,
            "/2015-03-31/functions/mi/versions",
            &json!({}),
        ),
    )
    .await
    .unwrap_err();
    assert_eq!(
        err.code(),
        "FunctionVersionsPerCapacityProviderLimitExceededException"
    );
}

#[test]
fn iam_actions_for_managed_instances_include_pass_capacity_provider() {
    let svc = LambdaService::new(make_state());
    let req = request(
        Method::POST,
        "/2015-03-31/functions",
        &managed_function("mi", cpc(CP_ARN)),
    );
    let actions = svc.iam_actions_for(&req);
    let pass = actions
        .iter()
        .find(|a| a.action == "PassCapacityProvider")
        .expect("lambda:PassCapacityProvider");
    assert_eq!(pass.service, "lambda");
    assert_eq!(pass.resource, CP_ARN);

    let req = request(
        Method::PUT,
        "/2015-03-31/functions/mi/configuration",
        &json!({"Timeout": 10}),
    );
    assert!(svc
        .iam_actions_for(&req)
        .iter()
        .all(|a| a.action != "PassCapacityProvider"));
}

#[test]
fn snapshots_round_trip_the_capacity_provider_config() {
    let mut state = LambdaState::new(ACCOUNT, "us-east-1");
    let func = LambdaFunction {
        function_name: "mi".into(),
        function_arn: crate::state::function_arn("us-east-1", ACCOUNT, "mi"),
        capacity_provider_config: Some(crate::state::FunctionCapacityProviderConfig {
            capacity_provider_arn: CP_ARN.into(),
            per_execution_environment_max_concurrency: Some(8),
            execution_environment_memory_gib_per_vcpu: Some(4.0),
        }),
        ..Default::default()
    };
    state.functions.insert("mi".into(), func.clone());
    state.latest_published.insert("mi".into(), func);
    let back: LambdaState = serde_json::from_value(serde_json::to_value(&state).unwrap()).unwrap();
    assert_eq!(
        back.functions["mi"].capacity_provider_config,
        state.functions["mi"].capacity_provider_config
    );
    assert!(back.latest_published.contains_key("mi"));

    // Snapshots predating the fields load with none.
    let mut legacy = serde_json::to_value(&state).unwrap();
    legacy.as_object_mut().unwrap().remove("latest_published");
    legacy["functions"]["mi"]
        .as_object_mut()
        .unwrap()
        .remove("capacity_provider_config");
    let back: LambdaState = serde_json::from_value(legacy).unwrap();
    assert!(back.functions["mi"].capacity_provider_config.is_none());
    assert!(back.latest_published.is_empty());
}

#[test]
fn legacy_slash_capacity_provider_arns_are_canonicalized_on_load() {
    let raw = json!({
        "name": "cp1",
        "arn": "arn:aws:lambda:us-east-1:123456789012:capacity-provider/cp1",
        "state": "Active",
        "vpc_config": {},
        "permissions_config": {},
        "instance_requirements": null,
        "scaling_config": null,
        "kms_key_arn": null,
        "tags": {},
        "last_modified": "2026-01-01T00:00:00Z",
        "function_versions": [],
    });
    let cp: crate::state::CapacityProvider = serde_json::from_value(raw).unwrap();
    assert_eq!(cp.arn, CP_ARN);
}
