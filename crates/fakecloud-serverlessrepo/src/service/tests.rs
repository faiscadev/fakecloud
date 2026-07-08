use super::*;
use fakecloud_core::multi_account::MultiAccountState;
use parking_lot::RwLock;

const SAM: &str = r#"{
    "AWSTemplateFormatVersion": "2010-09-09",
    "Transform": "AWS::Serverless-2016-10-31",
    "Parameters": {
        "TableName": { "Type": "String", "Default": "items", "Description": "table" }
    },
    "Resources": {
        "Fn": {
            "Type": "AWS::Serverless::Function",
            "Properties": {
                "Policies": ["AmazonDynamoDBFullAccess"],
                "Environment": { "Variables": { "TABLE": { "Ref": "TableName" } } }
            }
        }
    }
}"#;

fn service() -> ServerlessRepoService {
    let state: SharedServerlessRepoState = Arc::new(RwLock::new(MultiAccountState::new(
        "000000000000",
        "us-east-1",
        "",
    )));
    ServerlessRepoService::new(state)
}

fn ctx() -> Ctx {
    Ctx {
        account: "000000000000".to_string(),
        region: "us-east-1".to_string(),
        host: "localhost:4566".to_string(),
    }
}

fn body_of(resp: &AwsResponse) -> Value {
    serde_json::from_slice(resp.body.expect_bytes()).unwrap()
}

/// Extract the error from a handler result (`AwsResponse` is not `Debug`, so
/// `Result::unwrap_err` cannot be used directly).
fn expect_err(result: Result<AwsResponse, AwsServiceError>) -> AwsServiceError {
    match result {
        Ok(_) => panic!("expected an error, got a response"),
        Err(e) => e,
    }
}

#[test]
fn create_get_update_delete_lifecycle() {
    let svc = service();
    let ctx = ctx();
    let create = svc
        .create_application(
            &ctx,
            &json!({ "author": "Jane", "description": "d", "name": "my-app",
                     "homePageUrl": "https://example.com", "labels": ["web"] }),
        )
        .unwrap();
    assert_eq!(create.status, StatusCode::CREATED);
    let created = body_of(&create);
    let app_id = created["applicationId"].as_str().unwrap().to_string();
    assert!(app_id.ends_with(":applications/my-app"));
    assert_eq!(created["homePageUrl"], "https://example.com");
    // No version was seeded, so no Version block.
    assert!(created.get("version").is_none());
    // Output never leaks create-only members.
    assert!(created.get("licenseBody").is_none());

    let got = body_of(&svc.get_application(&ctx, &app_id, &[]).unwrap());
    assert_eq!(got["author"], "Jane");
    assert_eq!(got["homePageUrl"], "https://example.com");

    let updated = body_of(
        &svc.update_application(&ctx, &app_id, &json!({ "description": "new" }))
            .unwrap(),
    );
    assert_eq!(updated["description"], "new");

    let del = svc.delete_application(&ctx, &app_id).unwrap();
    assert_eq!(del.status, StatusCode::NO_CONTENT);
    let err = expect_err(svc.get_application(&ctx, &app_id, &[]));
    assert!(
        matches!(err, AwsServiceError::AwsError { ref code, .. } if code == "NotFoundException")
    );
}

#[test]
fn duplicate_name_conflicts() {
    let svc = service();
    let ctx = ctx();
    let b = json!({ "author": "a", "description": "d", "name": "dup" });
    svc.create_application(&ctx, &b).unwrap();
    let err = expect_err(svc.create_application(&ctx, &b));
    assert!(
        matches!(err, AwsServiceError::AwsError { ref code, .. } if code == "ConflictException")
    );
}

#[test]
fn get_missing_application_is_not_found() {
    let svc = service();
    let err = expect_err(svc.get_application(&ctx(), "does-not-exist", &[]));
    assert!(
        matches!(err, AwsServiceError::AwsError { ref code, .. } if code == "NotFoundException")
    );
}

#[test]
fn version_seeded_on_create_parses_template() {
    let svc = service();
    let ctx = ctx();
    let create = body_of(
        &svc.create_application(
            &ctx,
            &json!({ "author": "a", "description": "d", "name": "seeded",
                     "semanticVersion": "1.0.0", "templateBody": SAM }),
        )
        .unwrap(),
    );
    let version = &create["version"];
    assert_eq!(version["semanticVersion"], "1.0.0");
    assert_eq!(version["resourcesSupported"], true);
    assert!(version["templateUrl"]
        .as_str()
        .unwrap()
        .contains("localhost"));
    let defs = version["parameterDefinitions"].as_array().unwrap();
    assert_eq!(defs.len(), 1);
    assert_eq!(defs[0]["name"], "TableName");
    assert_eq!(defs[0]["defaultValue"], "items");
    assert_eq!(defs[0]["referencedByResources"], json!(["Fn"]));
    let caps = version["requiredCapabilities"].as_array().unwrap();
    assert!(caps.iter().any(|c| c == "CAPABILITY_IAM"));
}

#[test]
fn create_version_and_list() {
    let svc = service();
    let ctx = ctx();
    svc.create_application(
        &ctx,
        &json!({ "author": "a", "description": "d", "name": "app" }),
    )
    .unwrap();
    let app_id = shared::application_arn("us-east-1", "000000000000", "app");
    let v = body_of(
        &svc.create_application_version(
            &ctx,
            &app_id,
            "2.3.4",
            &json!({ "templateBody": SAM, "sourceCodeUrl": "https://git/x" }),
        )
        .unwrap(),
    );
    assert_eq!(v["semanticVersion"], "2.3.4");
    assert_eq!(v["sourceCodeUrl"], "https://git/x");
    assert_eq!(v["parameterDefinitions"].as_array().unwrap().len(), 1);

    let list = body_of(&svc.list_application_versions(&ctx, &app_id, &[]).unwrap());
    let versions = list["versions"].as_array().unwrap();
    assert_eq!(versions.len(), 1);
    assert_eq!(versions[0]["semanticVersion"], "2.3.4");
    assert_eq!(versions[0]["sourceCodeUrl"], "https://git/x");
}

#[test]
fn create_version_upserts_missing_application() {
    let svc = service();
    let ctx = ctx();
    // No CreateApplication first: the PUT should still succeed.
    let v = body_of(
        &svc.create_application_version(&ctx, "some-app-id", "1.0.0", &json!({}))
            .unwrap(),
    );
    assert_eq!(v["semanticVersion"], "1.0.0");
    assert_eq!(v["parameterDefinitions"], json!([]));
}

#[test]
fn policy_put_get_and_unshare() {
    let svc = service();
    let ctx = ctx();
    svc.create_application(
        &ctx,
        &json!({ "author": "a", "description": "d", "name": "shared" }),
    )
    .unwrap();
    let app_id = shared::application_arn("us-east-1", "000000000000", "shared");
    let put = body_of(
        &svc.put_application_policy(
            &ctx,
            &app_id,
            &json!({ "statements": [{
                "actions": ["Deploy"],
                "principals": ["123456789012"],
                "principalOrgIDs": ["o-abc123"]
            }] }),
        )
        .unwrap(),
    );
    let stmts = put["statements"].as_array().unwrap();
    assert_eq!(stmts.len(), 1);
    assert!(stmts[0]["statementId"].is_string());

    let got = body_of(&svc.get_application_policy(&ctx, &app_id).unwrap());
    assert_eq!(got["statements"].as_array().unwrap().len(), 1);

    let unshare = svc
        .unshare_application(&ctx, &app_id, &json!({ "organizationId": "o-abc123" }))
        .unwrap();
    assert_eq!(unshare.status, StatusCode::NO_CONTENT);
    let after = body_of(&svc.get_application_policy(&ctx, &app_id).unwrap());
    let orgs = after["statements"][0]["principalOrgIDs"]
        .as_array()
        .unwrap();
    assert!(orgs.is_empty());
}

#[test]
fn cloudformation_template_prepares_then_active() {
    let svc = service();
    let ctx = ctx();
    svc.create_application(
        &ctx,
        &json!({ "author": "a", "description": "d", "name": "tmpl",
                 "semanticVersion": "1.0.0", "templateBody": SAM }),
    )
    .unwrap();
    let app_id = shared::application_arn("us-east-1", "000000000000", "tmpl");
    let created = body_of(
        &svc.create_cloudformation_template(&ctx, &app_id, &json!({ "semanticVersion": "1.0.0" }))
            .unwrap(),
    );
    assert_eq!(created["status"], "PREPARING");
    assert!(created["templateUrl"].as_str().unwrap().starts_with("http"));
    let template_id = created["templateId"].as_str().unwrap().to_string();

    let fetched = body_of(
        &svc.get_cloudformation_template(&ctx, &app_id, &template_id)
            .unwrap(),
    );
    assert_eq!(fetched["status"], "ACTIVE");
}

#[test]
fn change_set_mints_identifiers() {
    let svc = service();
    let ctx = ctx();
    let out = body_of(
        &svc.create_cloudformation_change_set(
            &ctx,
            "arn:aws:serverlessrepo:us-east-1:000000000000:applications/x",
            &json!({ "stackName": "my-stack" }),
        )
        .unwrap(),
    );
    assert!(out["changeSetId"].is_string());
    assert!(out["stackId"]
        .as_str()
        .unwrap()
        .contains(":stack/my-stack/"));
}

#[test]
fn list_dependencies_from_nested_application() {
    let svc = service();
    let ctx = ctx();
    let nested = r#"{"Resources":{"Nested":{"Type":"AWS::Serverless::Application",
        "Properties":{"Location":{"ApplicationId":"arn:aws:serverlessrepo:us-east-1:1:applications/dep","SemanticVersion":"1.2.3"}}}}}"#;
    svc.create_application(
        &ctx,
        &json!({ "author": "a", "description": "d", "name": "parent",
                 "semanticVersion": "1.0.0", "templateBody": nested }),
    )
    .unwrap();
    let app_id = shared::application_arn("us-east-1", "000000000000", "parent");
    let deps = body_of(
        &svc.list_application_dependencies(&ctx, &app_id, &[])
            .unwrap(),
    );
    let list = deps["dependencies"].as_array().unwrap();
    assert_eq!(list.len(), 1);
    assert_eq!(list[0]["semanticVersion"], "1.2.3");
}

#[test]
fn list_applications_paginates() {
    let svc = service();
    let ctx = ctx();
    for i in 0..5 {
        svc.create_application(
            &ctx,
            &json!({ "author": "a", "description": "d", "name": format!("app-{i}") }),
        )
        .unwrap();
    }
    let page1 = body_of(
        &svc.list_applications(&ctx, &[("maxItems".into(), "2".into())])
            .unwrap(),
    );
    assert_eq!(page1["applications"].as_array().unwrap().len(), 2);
    let token = page1["nextToken"].as_str().unwrap().to_string();
    let page2 = body_of(
        &svc.list_applications(
            &ctx,
            &[("maxItems".into(), "2".into()), ("nextToken".into(), token)],
        )
        .unwrap(),
    );
    assert_eq!(page2["applications"].as_array().unwrap().len(), 2);
}

#[test]
fn resolve_action_routes_methods() {
    // Sanity check a few routes via the resolver-facing path parsing.
    let cases = [
        ("POST", "/applications", "CreateApplication"),
        ("GET", "/applications", "ListApplications"),
        (
            "PUT",
            "/applications/abc/versions/1.0.0",
            "CreateApplicationVersion",
        ),
        ("GET", "/applications/abc/policy", "GetApplicationPolicy"),
        ("PATCH", "/applications/abc", "UpdateApplication"),
    ];
    for (method, path, expected) in cases {
        let req = crate::service::tests::mk_req(method, path);
        let (action, _) = ServerlessRepoService::resolve_action(&req).unwrap();
        assert_eq!(action, expected, "{method} {path}");
    }
}

/// Build a minimal `AwsRequest` for route-resolution tests.
fn mk_req(method: &str, path: &str) -> AwsRequest {
    use fakecloud_core::service::AwsRequest;
    AwsRequest {
        service: "serverlessrepo".into(),
        action: String::new(),
        region: "us-east-1".into(),
        account_id: "000000000000".into(),
        request_id: "req".into(),
        headers: http::HeaderMap::new(),
        query_params: std::collections::HashMap::new(),
        body: bytes::Bytes::new(),
        body_stream: parking_lot::Mutex::new(None),
        path_segments: Vec::new(),
        raw_path: path.into(),
        raw_query: String::new(),
        method: Method::from_bytes(method.as_bytes()).unwrap(),
        is_query_protocol: false,
        access_key_id: None,
        principal: None,
    }
}
