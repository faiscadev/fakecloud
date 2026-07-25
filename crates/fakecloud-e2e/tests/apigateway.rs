//! End-to-end coverage for API Gateway v1 (REST APIs).
//!
//! Verifies the full create -> resource -> method -> integration ->
//! deployment -> stage chain that AWS SDK callers walk to stand up a
//! REST API. Hits the AWS JSON surface directly through the SDK so
//! the test passes only if both the URL routing (REST-style) and the
//! response shapes match what the SDK expects.

mod helpers;

use helpers::TestServer;

use aws_sdk_cognitoidentityprovider::types::{
    ExplicitAuthFlowsType, PasswordPolicyType, UserPoolPolicyType,
};

#[tokio::test]
async fn create_rest_api_round_trip() {
    let server = TestServer::start().await;
    let client = server.apigateway_client().await;

    let api = client
        .create_rest_api()
        .name("petstore")
        .description("integration test")
        .send()
        .await
        .expect("create_rest_api");
    assert_eq!(api.name(), Some("petstore"));
    let api_id = api.id().expect("id").to_string();
    assert!(!api_id.is_empty());
    let root_resource_id = api.root_resource_id().expect("root").to_string();

    let described = client
        .get_rest_api()
        .rest_api_id(&api_id)
        .send()
        .await
        .expect("get_rest_api");
    assert_eq!(described.name(), Some("petstore"));

    let listed = client.get_rest_apis().send().await.expect("get_rest_apis");
    assert_eq!(listed.items().len(), 1);

    // Create a child resource and a method on it.
    let resource = client
        .create_resource()
        .rest_api_id(&api_id)
        .parent_id(&root_resource_id)
        .path_part("pets")
        .send()
        .await
        .expect("create_resource");
    let res_id = resource.id().expect("res id").to_string();
    assert_eq!(resource.path_part(), Some("pets"));
    assert_eq!(resource.path(), Some("/pets"));

    client
        .put_method()
        .rest_api_id(&api_id)
        .resource_id(&res_id)
        .http_method("GET")
        .authorization_type("NONE")
        .send()
        .await
        .expect("put_method");

    client
        .put_integration()
        .rest_api_id(&api_id)
        .resource_id(&res_id)
        .http_method("GET")
        .r#type(aws_sdk_apigateway::types::IntegrationType::Mock)
        .send()
        .await
        .expect("put_integration");

    let deployment = client
        .create_deployment()
        .rest_api_id(&api_id)
        .stage_name("prod")
        .send()
        .await
        .expect("create_deployment");
    assert!(deployment.id().is_some());

    // The auto-created stage from create_deployment with stageName.
    let stage = client
        .get_stage()
        .rest_api_id(&api_id)
        .stage_name("prod")
        .send()
        .await
        .expect("get_stage");
    assert_eq!(stage.stage_name(), Some("prod"));

    // Cleanup.
    client
        .delete_rest_api()
        .rest_api_id(&api_id)
        .send()
        .await
        .expect("delete_rest_api");
    let listed_after = client
        .get_rest_apis()
        .send()
        .await
        .expect("get after delete");
    assert!(listed_after.items().is_empty());
}

#[tokio::test]
async fn api_keys_and_usage_plans() {
    let server = TestServer::start().await;
    let client = server.apigateway_client().await;

    let key = client
        .create_api_key()
        .name("my-key")
        .enabled(true)
        .send()
        .await
        .expect("create_api_key");
    let key_id = key.id().expect("key id").to_string();
    assert_eq!(key.name(), Some("my-key"));

    let plan = client
        .create_usage_plan()
        .name("standard")
        .send()
        .await
        .expect("create_usage_plan");
    let plan_id = plan.id().expect("plan id").to_string();

    client
        .create_usage_plan_key()
        .usage_plan_id(&plan_id)
        .key_id(&key_id)
        .key_type("API_KEY")
        .send()
        .await
        .expect("create_usage_plan_key");

    let listed = client
        .get_usage_plan_keys()
        .usage_plan_id(&plan_id)
        .send()
        .await
        .expect("get_usage_plan_keys");
    assert_eq!(listed.items().len(), 1);

    client
        .delete_usage_plan_key()
        .usage_plan_id(&plan_id)
        .key_id(&key_id)
        .send()
        .await
        .expect("delete_usage_plan_key");
    client
        .delete_usage_plan()
        .usage_plan_id(&plan_id)
        .send()
        .await
        .expect("delete_usage_plan");
    client
        .delete_api_key()
        .api_key(&key_id)
        .send()
        .await
        .expect("delete_api_key");
}

#[tokio::test]
async fn usage_plan_api_stage_add_then_remove() {
    use aws_sdk_apigateway::types::{Op, PatchOperation};
    let server = TestServer::start().await;
    let client = server.apigateway_client().await;

    let plan_id = client
        .create_usage_plan()
        .name("stage-plan")
        .send()
        .await
        .expect("create_usage_plan")
        .id()
        .expect("plan id")
        .to_string();

    // Attach an API stage the way Terraform/CDK does: {op:add, /apiStages}.
    let added = client
        .update_usage_plan()
        .usage_plan_id(&plan_id)
        .patch_operations(
            PatchOperation::builder()
                .op(Op::Add)
                .path("/apiStages")
                .value("abc123:prod")
                .build(),
        )
        .send()
        .await
        .expect("add api stage");
    assert_eq!(added.api_stages().len(), 1);
    assert_eq!(added.api_stages()[0].api_id(), Some("abc123"));
    assert_eq!(added.api_stages()[0].stage(), Some("prod"));

    // Remove it via {op:remove, /apiStages/<apiId>:<stage>} — the arm that was
    // previously dead because the op guard rejected "remove".
    let removed = client
        .update_usage_plan()
        .usage_plan_id(&plan_id)
        .patch_operations(
            PatchOperation::builder()
                .op(Op::Remove)
                .path("/apiStages/abc123:prod")
                .build(),
        )
        .send()
        .await
        .expect("remove api stage");
    assert!(
        removed.api_stages().is_empty(),
        "api stage should be removed, got {:?}",
        removed.api_stages()
    );
}

#[tokio::test]
async fn get_export_returns_openapi_with_real_paths() {
    let server = TestServer::start().await;
    let client = server.apigateway_client().await;

    let api = client
        .create_rest_api()
        .name("export-target")
        .send()
        .await
        .expect("create_rest_api");
    let api_id = api.id().unwrap().to_string();
    let root = api.root_resource_id().unwrap().to_string();

    let resource = client
        .create_resource()
        .rest_api_id(&api_id)
        .parent_id(&root)
        .path_part("widgets")
        .send()
        .await
        .expect("create_resource");
    let res_id = resource.id().unwrap().to_string();

    client
        .put_method()
        .rest_api_id(&api_id)
        .resource_id(&res_id)
        .http_method("GET")
        .authorization_type("NONE")
        .operation_name("GetWidgets")
        .request_parameters("method.request.querystring.limit", false)
        .send()
        .await
        .expect("put_method GET");

    client
        .put_integration()
        .rest_api_id(&api_id)
        .resource_id(&res_id)
        .http_method("GET")
        .r#type(aws_sdk_apigateway::types::IntegrationType::Mock)
        .send()
        .await
        .expect("put_integration");

    client
        .create_deployment()
        .rest_api_id(&api_id)
        .stage_name("v1")
        .send()
        .await
        .expect("create_deployment");

    let export = client
        .get_export()
        .rest_api_id(&api_id)
        .stage_name("v1")
        .export_type("oas30")
        .send()
        .await
        .expect("get_export");
    // AWS returns the export as a download attachment; the export data source
    // reads `content_disposition` from this header.
    assert!(export
        .content_disposition()
        .is_some_and(|d| d.starts_with("attachment;")));
    let body_blob = export.body().expect("export body should be present");
    let body: serde_json::Value =
        serde_json::from_slice(body_blob.as_ref()).expect("export body must be JSON");
    assert_eq!(body["openapi"], "3.0.1");
    assert_eq!(body["info"]["title"], "export-target");
    let paths = body["paths"].as_object().expect("paths object");
    let widgets = paths
        .get("/widgets")
        .expect("widgets path should be in OpenAPI export");
    let get_op = widgets
        .as_object()
        .unwrap()
        .get("get")
        .expect("GET operation should be present");
    assert_eq!(get_op["operationId"], "GetWidgets");
    let params = get_op["parameters"].as_array().expect("parameters array");
    assert!(params
        .iter()
        .any(|p| p["name"] == "limit" && p["in"] == "query"));
}

// ── Authorizer enforcement (Phase R1) ──
//
// Drives the data plane through real HTTP requests so we exercise the
// facade routing (host header `{api-id}.execute-api...`), the authorizer
// machinery, and the integration handoff end-to-end. Lambda authorizers
// would require a Docker runtime; the unit tests in
// `fakecloud_apigateway::data_plane::tests` cover Allow/Deny against a
// stub LambdaDelivery, so the e2e cases focus on the wiring fakecloud
// owns: status codes, header plumbing, and the Cognito JWT path that
// runs entirely in-process via the StateBackedJwtVerifier.

/// Build a synthetic execute-api Host header so the
/// `ApiGatewayFacade` routes the unsigned request to v1.
fn execute_api_host(api_id: &str) -> String {
    format!("{api_id}.execute-api.us-east-1.amazonaws.com")
}

/// Stand up an API with a single `/items` resource, the requested
/// authorization config on its GET method, and a MOCK integration so
/// allowed requests succeed without needing a backend Lambda.
async fn provision_protected_api(
    client: &aws_sdk_apigateway::Client,
    auth_type: &str,
    authorizer_id: Option<&str>,
) -> String {
    let api = client
        .create_rest_api()
        .name("auth-test")
        .send()
        .await
        .expect("create_rest_api");
    let api_id = api.id().unwrap().to_string();
    let root = api.root_resource_id().unwrap().to_string();
    let resource = client
        .create_resource()
        .rest_api_id(&api_id)
        .parent_id(&root)
        .path_part("items")
        .send()
        .await
        .expect("create_resource");
    let res_id = resource.id().unwrap().to_string();
    let mut put_method = client
        .put_method()
        .rest_api_id(&api_id)
        .resource_id(&res_id)
        .http_method("GET")
        .authorization_type(auth_type);
    if let Some(aid) = authorizer_id {
        put_method = put_method.authorizer_id(aid);
    }
    put_method.send().await.expect("put_method");
    client
        .put_integration()
        .rest_api_id(&api_id)
        .resource_id(&res_id)
        .http_method("GET")
        .r#type(aws_sdk_apigateway::types::IntegrationType::Mock)
        .send()
        .await
        .expect("put_integration");
    client
        .create_deployment()
        .rest_api_id(&api_id)
        .stage_name("prod")
        .send()
        .await
        .expect("create_deployment");
    api_id
}

#[tokio::test]
async fn data_plane_allows_method_without_authorizer() {
    let server = TestServer::start().await;
    let client = server.apigateway_client().await;
    let api_id = provision_protected_api(&client, "NONE", None).await;

    let http = reqwest::Client::new();
    let resp = http
        .get(format!("{}/prod/items", server.endpoint()))
        .header("host", execute_api_host(&api_id))
        .send()
        .await
        .expect("send");
    assert_eq!(resp.status(), 200);
}

#[tokio::test]
async fn data_plane_token_authorizer_missing_header_returns_401() {
    let server = TestServer::start().await;
    let client = server.apigateway_client().await;

    // Create a TOKEN authorizer pointing at a non-existent Lambda;
    // identity-source enforcement runs before the Lambda invocation, so
    // the missing token must short-circuit to 401 without needing a
    // real backend.
    let api = client
        .create_rest_api()
        .name("auth-token-401")
        .send()
        .await
        .expect("create_rest_api");
    let api_id = api.id().unwrap().to_string();
    let root = api.root_resource_id().unwrap().to_string();

    let authorizer = client
        .create_authorizer()
        .rest_api_id(&api_id)
        .name("tok-auth")
        .r#type(aws_sdk_apigateway::types::AuthorizerType::Token)
        .authorizer_uri("arn:aws:apigateway:us-east-1:lambda:path/2015-03-31/functions/arn:aws:lambda:us-east-1:000000000000:function:nope/invocations")
        .identity_source("method.request.header.Authorization")
        .send()
        .await
        .expect("create_authorizer");
    let auth_id = authorizer.id().unwrap().to_string();

    let resource = client
        .create_resource()
        .rest_api_id(&api_id)
        .parent_id(&root)
        .path_part("items")
        .send()
        .await
        .expect("create_resource");
    let res_id = resource.id().unwrap().to_string();

    client
        .put_method()
        .rest_api_id(&api_id)
        .resource_id(&res_id)
        .http_method("GET")
        .authorization_type("CUSTOM")
        .authorizer_id(&auth_id)
        .send()
        .await
        .expect("put_method");
    client
        .put_integration()
        .rest_api_id(&api_id)
        .resource_id(&res_id)
        .http_method("GET")
        .r#type(aws_sdk_apigateway::types::IntegrationType::Mock)
        .send()
        .await
        .expect("put_integration");
    client
        .create_deployment()
        .rest_api_id(&api_id)
        .stage_name("prod")
        .send()
        .await
        .expect("create_deployment");

    let http = reqwest::Client::new();
    let resp = http
        .get(format!("{}/prod/items", server.endpoint()))
        .header("host", execute_api_host(&api_id))
        .send()
        .await
        .expect("send");
    assert_eq!(resp.status(), 401);
}

#[tokio::test]
async fn data_plane_cognito_authorizer_accepts_real_jwt_and_rejects_garbage() {
    use aws_sdk_cognitoidentityprovider::types::AuthFlowType;

    let server = TestServer::start().await;
    let cog = server.cognito_client().await;
    let apigw = server.apigateway_client().await;

    // Stand up a Cognito user pool with a usable password policy and a
    // user we can sign in as.
    let pool = cog
        .create_user_pool()
        .pool_name("apigw-cognito")
        .policies(
            UserPoolPolicyType::builder()
                .password_policy(
                    PasswordPolicyType::builder()
                        .minimum_length(6)
                        .require_uppercase(false)
                        .require_lowercase(false)
                        .require_numbers(false)
                        .require_symbols(false)
                        .build(),
                )
                .build(),
        )
        .send()
        .await
        .expect("create_user_pool");
    let pool_id = pool.user_pool().unwrap().id().unwrap().to_string();
    let pool_arn = format!("arn:aws:cognito-idp:us-east-1:000000000000:userpool/{pool_id}");
    let app = cog
        .create_user_pool_client()
        .user_pool_id(&pool_id)
        .client_name("c")
        .explicit_auth_flows(ExplicitAuthFlowsType::AllowAdminUserPasswordAuth)
        .explicit_auth_flows(ExplicitAuthFlowsType::AllowRefreshTokenAuth)
        .send()
        .await
        .expect("create_user_pool_client");
    let client_id = app
        .user_pool_client()
        .unwrap()
        .client_id()
        .unwrap()
        .to_string();
    cog.admin_create_user()
        .user_pool_id(&pool_id)
        .username("alice")
        .send()
        .await
        .expect("admin_create_user");
    cog.admin_set_user_password()
        .user_pool_id(&pool_id)
        .username("alice")
        .password("secret1")
        .permanent(true)
        .send()
        .await
        .expect("admin_set_user_password");
    let auth = cog
        .admin_initiate_auth()
        .user_pool_id(&pool_id)
        .client_id(&client_id)
        .auth_flow(AuthFlowType::AdminUserPasswordAuth)
        .auth_parameters("USERNAME", "alice")
        .auth_parameters("PASSWORD", "secret1")
        .send()
        .await
        .expect("admin_initiate_auth");
    let id_token = auth
        .authentication_result()
        .and_then(|r| r.id_token())
        .expect("id_token must be present after admin_initiate_auth")
        .to_string();

    // Stand up a REST API protected by a COGNITO_USER_POOLS authorizer
    // pointing at the pool we just provisioned.
    let api = apigw
        .create_rest_api()
        .name("auth-cognito")
        .send()
        .await
        .expect("create_rest_api");
    let api_id = api.id().unwrap().to_string();
    let root = api.root_resource_id().unwrap().to_string();

    let authorizer = apigw
        .create_authorizer()
        .rest_api_id(&api_id)
        .name("cog-auth")
        .r#type(aws_sdk_apigateway::types::AuthorizerType::CognitoUserPools)
        .provider_arns(&pool_arn)
        .identity_source("method.request.header.Authorization")
        .send()
        .await
        .expect("create_authorizer");
    let auth_id = authorizer.id().unwrap().to_string();

    let resource = apigw
        .create_resource()
        .rest_api_id(&api_id)
        .parent_id(&root)
        .path_part("items")
        .send()
        .await
        .expect("create_resource");
    let res_id = resource.id().unwrap().to_string();
    apigw
        .put_method()
        .rest_api_id(&api_id)
        .resource_id(&res_id)
        .http_method("GET")
        .authorization_type("COGNITO_USER_POOLS")
        .authorizer_id(&auth_id)
        .send()
        .await
        .expect("put_method");
    apigw
        .put_integration()
        .rest_api_id(&api_id)
        .resource_id(&res_id)
        .http_method("GET")
        .r#type(aws_sdk_apigateway::types::IntegrationType::Mock)
        .send()
        .await
        .expect("put_integration");
    apigw
        .create_deployment()
        .rest_api_id(&api_id)
        .stage_name("prod")
        .send()
        .await
        .expect("create_deployment");

    let http = reqwest::Client::new();

    // Tampered JWT must short-circuit at signature verification.
    let bad = http
        .get(format!("{}/prod/items", server.endpoint()))
        .header("host", execute_api_host(&api_id))
        .header("authorization", "Bearer not-a-real-jwt")
        .send()
        .await
        .expect("send");
    assert_eq!(bad.status(), 401);

    // Real pool-issued JWT must verify against the in-process JWKS and
    // let the request through to the MOCK integration.
    let good = http
        .get(format!("{}/prod/items", server.endpoint()))
        .header("host", execute_api_host(&api_id))
        .header("authorization", format!("Bearer {id_token}"))
        .send()
        .await
        .expect("send");
    assert_eq!(good.status(), 200);
}

// ── Usage plan throttle + quota (Phase R2) ──
//
// Drives the api-key + usage-plan enforcement path end-to-end through
// real HTTP requests so we exercise the facade routing, the api-key
// header check, and the in-memory token bucket. The unit tests cover
// the meter math and method-level overrides; this case proves the wiring
// is connected through the public API surface.

/// Provision an API with `apiKeyRequired = true` on a MOCK GET method and
/// associate a fresh API key + usage plan with the given throttle. Returns
/// `(api_id, api_key_value)`.
async fn provision_metered_api(
    client: &aws_sdk_apigateway::Client,
    plan_throttle: aws_sdk_apigateway::types::ThrottleSettings,
) -> (String, String) {
    use aws_sdk_apigateway::types::{ApiStage, IntegrationType};

    let api = client
        .create_rest_api()
        .name("metered")
        .send()
        .await
        .expect("create_rest_api");
    let api_id = api.id().unwrap().to_string();
    let root = api.root_resource_id().unwrap().to_string();
    let resource = client
        .create_resource()
        .rest_api_id(&api_id)
        .parent_id(&root)
        .path_part("items")
        .send()
        .await
        .expect("create_resource");
    let res_id = resource.id().unwrap().to_string();
    client
        .put_method()
        .rest_api_id(&api_id)
        .resource_id(&res_id)
        .http_method("GET")
        .authorization_type("NONE")
        .api_key_required(true)
        .send()
        .await
        .expect("put_method");
    client
        .put_integration()
        .rest_api_id(&api_id)
        .resource_id(&res_id)
        .http_method("GET")
        .r#type(IntegrationType::Mock)
        .send()
        .await
        .expect("put_integration");
    client
        .create_deployment()
        .rest_api_id(&api_id)
        .stage_name("prod")
        .send()
        .await
        .expect("create_deployment");

    let key = client
        .create_api_key()
        .name("metered-key")
        .enabled(true)
        .send()
        .await
        .expect("create_api_key");
    let key_id = key.id().unwrap().to_string();
    let key_value = key.value().unwrap().to_string();

    let plan = client
        .create_usage_plan()
        .name("metered-plan")
        .api_stages(ApiStage::builder().api_id(&api_id).stage("prod").build())
        .throttle(plan_throttle)
        .send()
        .await
        .expect("create_usage_plan");
    let plan_id = plan.id().unwrap().to_string();
    client
        .create_usage_plan_key()
        .usage_plan_id(&plan_id)
        .key_id(&key_id)
        .key_type("API_KEY")
        .send()
        .await
        .expect("create_usage_plan_key");

    (api_id, key_value)
}

#[tokio::test]
async fn data_plane_missing_api_key_returns_403() {
    use aws_sdk_apigateway::types::ThrottleSettings;

    let server = TestServer::start().await;
    let client = server.apigateway_client().await;
    let (api_id, _key) = provision_metered_api(
        &client,
        ThrottleSettings::builder()
            .rate_limit(100.0)
            .burst_limit(100)
            .build(),
    )
    .await;

    let http = reqwest::Client::new();
    let resp = http
        .get(format!("{}/prod/items", server.endpoint()))
        .header("host", execute_api_host(&api_id))
        .send()
        .await
        .expect("send");
    assert_eq!(resp.status(), 403);
}

#[tokio::test]
async fn data_plane_throttle_returns_429_when_burst_exhausted() {
    use aws_sdk_apigateway::types::ThrottleSettings;

    let server = TestServer::start().await;
    let client = server.apigateway_client().await;
    // 1 RPS / burst=1: the bucket has exactly one token at start, the
    // second request must trip 429 before the rate refills a fresh
    // token.
    let (api_id, key_value) = provision_metered_api(
        &client,
        ThrottleSettings::builder()
            .rate_limit(1.0)
            .burst_limit(1)
            .build(),
    )
    .await;

    let http = reqwest::Client::new();
    let first = http
        .get(format!("{}/prod/items", server.endpoint()))
        .header("host", execute_api_host(&api_id))
        .header("x-api-key", &key_value)
        .send()
        .await
        .expect("send");
    assert_eq!(first.status(), 200);

    let second = http
        .get(format!("{}/prod/items", server.endpoint()))
        .header("host", execute_api_host(&api_id))
        .header("x-api-key", &key_value)
        .send()
        .await
        .expect("send");
    assert_eq!(second.status(), 429);
}

// ── Request validator enforcement (Phase R3) ──

#[tokio::test]
async fn data_plane_request_validator_rejects_missing_body_field() {
    let server = TestServer::start().await;
    let client = server.apigateway_client().await;

    let api = client
        .create_rest_api()
        .name("validator-test")
        .send()
        .await
        .expect("create_rest_api");
    let api_id = api.id().unwrap().to_string();
    let root = api.root_resource_id().unwrap().to_string();

    let resource = client
        .create_resource()
        .rest_api_id(&api_id)
        .parent_id(&root)
        .path_part("items")
        .send()
        .await
        .expect("create_resource");
    let res_id = resource.id().unwrap().to_string();

    // Create a model with required "name" field.
    client
        .create_model()
        .rest_api_id(&api_id)
        .name("ItemModel")
        .content_type("application/json")
        .schema(r#"{"type":"object","required":["name"],"properties":{"name":{"type":"string"}}}"#)
        .send()
        .await
        .expect("create_model");

    // Create a validator that validates the request body.
    let validator = client
        .create_request_validator()
        .rest_api_id(&api_id)
        .name("body-validator")
        .validate_request_body(true)
        .send()
        .await
        .expect("create_request_validator");
    let validator_id = validator.id().unwrap().to_string();

    client
        .put_method()
        .rest_api_id(&api_id)
        .resource_id(&res_id)
        .http_method("POST")
        .authorization_type("NONE")
        .request_validator_id(&validator_id)
        .request_models("application/json", "ItemModel")
        .send()
        .await
        .expect("put_method");

    client
        .put_integration()
        .rest_api_id(&api_id)
        .resource_id(&res_id)
        .http_method("POST")
        .r#type(aws_sdk_apigateway::types::IntegrationType::Mock)
        .send()
        .await
        .expect("put_integration");

    client
        .create_deployment()
        .rest_api_id(&api_id)
        .stage_name("prod")
        .send()
        .await
        .expect("create_deployment");

    let http = reqwest::Client::new();

    // Missing required "name" field must return 400.
    let bad = http
        .post(format!("{}/prod/items", server.endpoint()))
        .header("host", execute_api_host(&api_id))
        .header("content-type", "application/json")
        .body(r#"{"count": 42}"#)
        .send()
        .await
        .expect("send");
    assert_eq!(bad.status(), 400);

    // Valid body with required "name" field must pass.
    let good = http
        .post(format!("{}/prod/items", server.endpoint()))
        .header("host", execute_api_host(&api_id))
        .header("content-type", "application/json")
        .body(r#"{"name": "hello"}"#)
        .send()
        .await
        .expect("send");
    assert_eq!(good.status(), 200);
}

// bug-audit 2026-05-28, 1.18: TestInvokeAuthorizer must deny (clientStatus
// 401) when the authorizer's identity-source header is absent, not always
// return a canned Allow.
#[tokio::test]
async fn test_invoke_authorizer_honors_identity_source() {
    let server = TestServer::start().await;
    let client = server.apigateway_client().await;

    let api = client
        .create_rest_api()
        .name("testinvoke-authz")
        .send()
        .await
        .expect("create_rest_api");
    let api_id = api.id().unwrap().to_string();

    let authorizer = client
        .create_authorizer()
        .rest_api_id(&api_id)
        .name("tok-authz")
        .r#type(aws_sdk_apigateway::types::AuthorizerType::Token)
        .identity_source("method.request.header.Authorization")
        .send()
        .await
        .expect("create_authorizer");
    let authorizer_id = authorizer.id().unwrap().to_string();

    // Identity source configured but no matching request header -> 401.
    let denied = client
        .test_invoke_authorizer()
        .rest_api_id(&api_id)
        .authorizer_id(&authorizer_id)
        .send()
        .await
        .expect("test_invoke_authorizer denied");
    assert_eq!(denied.client_status(), 401);

    // An authorizer with no identity source requires nothing -> 200.
    let open_authorizer = client
        .create_authorizer()
        .rest_api_id(&api_id)
        .name("open-authz")
        .r#type(aws_sdk_apigateway::types::AuthorizerType::Token)
        .send()
        .await
        .expect("create open authorizer");
    let open_id = open_authorizer.id().unwrap().to_string();

    let allowed = client
        .test_invoke_authorizer()
        .rest_api_id(&api_id)
        .authorizer_id(&open_id)
        .send()
        .await
        .expect("test_invoke_authorizer allowed");
    assert_eq!(allowed.client_status(), 200);
}

#[tokio::test]
async fn update_usage_plan_adds_api_stage() {
    use aws_sdk_apigateway::types::{Op, PatchOperation};

    let server = TestServer::start().await;
    let client = server.apigateway_client().await;

    let api_id = client
        .create_rest_api()
        .name("metered")
        .send()
        .await
        .unwrap()
        .id()
        .unwrap()
        .to_string();

    let plan_id = client
        .create_usage_plan()
        .name("plan-no-stages")
        .send()
        .await
        .unwrap()
        .id()
        .unwrap()
        .to_string();

    // Attach an API stage via PATCH — previously dropped, so the standard
    // "create plan then attach stage" flow silently no-op'd.
    client
        .update_usage_plan()
        .usage_plan_id(&plan_id)
        .patch_operations(
            PatchOperation::builder()
                .op(Op::Add)
                .path("/apiStages")
                .value(format!("{api_id}:prod"))
                .build(),
        )
        .send()
        .await
        .expect("update_usage_plan");

    let got = client
        .get_usage_plan()
        .usage_plan_id(&plan_id)
        .send()
        .await
        .unwrap();
    assert!(
        got.api_stages()
            .iter()
            .any(|s| s.api_id() == Some(api_id.as_str()) && s.stage() == Some("prod")),
        "apiStage must be attached: {:?}",
        got.api_stages()
    );
}

#[tokio::test]
async fn update_request_validator_keeps_boolean_flags() {
    // UpdateRequestValidator applies JSON-Patch string values; the boolean
    // validator flags must be stored as real booleans so GetRequestValidator
    // returns the type the SDK expects (not a string).
    let server = TestServer::start().await;
    let client = server.apigateway_client().await;

    let api = client
        .create_rest_api()
        .name("rv-api")
        .send()
        .await
        .expect("create_rest_api");
    let api_id = api.id().unwrap().to_string();

    let rv = client
        .create_request_validator()
        .rest_api_id(&api_id)
        .name("rv")
        .validate_request_body(false)
        .validate_request_parameters(false)
        .send()
        .await
        .expect("create_request_validator");
    let rv_id = rv.id().unwrap().to_string();

    client
        .update_request_validator()
        .rest_api_id(&api_id)
        .request_validator_id(&rv_id)
        .patch_operations(
            aws_sdk_apigateway::types::PatchOperation::builder()
                .op(aws_sdk_apigateway::types::Op::Replace)
                .path("/validateRequestBody")
                .value("true")
                .build(),
        )
        .send()
        .await
        .expect("update_request_validator");

    // Deserializes only if the flag is stored as a real boolean.
    let got = client
        .get_request_validator()
        .rest_api_id(&api_id)
        .request_validator_id(&rv_id)
        .send()
        .await
        .expect("get_request_validator");
    assert!(got.validate_request_body());
}

// bug-audit 2026-06-27, T6.3: GenerateClientCertificate must return a real,
// parseable PEM certificate, not a placeholder string.
#[tokio::test]
async fn apigw_generate_client_certificate_returns_real_pem() {
    let server = TestServer::start().await;
    let client = server.apigateway_client().await;

    let cert = client
        .generate_client_certificate()
        .send()
        .await
        .expect("generate client certificate");
    let pem = cert.pem_encoded_certificate().expect("pem present");
    assert!(pem.contains("-----BEGIN CERTIFICATE-----"));
    assert!(!pem.contains("fakecloud-stub"), "not a placeholder");

    // The body between the PEM markers must be valid base64 DER (a real cert).
    let body: String = pem.lines().filter(|l| !l.starts_with("-----")).collect();
    use base64::Engine;
    let der = base64::engine::general_purpose::STANDARD
        .decode(body.trim())
        .expect("PEM body is valid base64 DER");
    assert!(der.len() > 100, "decoded certificate is non-trivial");
}

fn docker_available() -> bool {
    std::process::Command::new("docker")
        .arg("info")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

fn require_docker_or_skip(test: &str) -> bool {
    if docker_available() {
        return true;
    }
    if std::env::var("CI").is_ok() {
        panic!("docker is required for {test} in CI");
    }
    eprintln!("skipping {test}: docker is not available");
    false
}

/// A REST (v1) AWS_PROXY Lambda that throws surfaces as HTTP 502
/// `{"message":"Internal server error"}`, matching real API Gateway, rather
/// than a 200 echoing the raw `{errorMessage,errorType}` envelope.
#[tokio::test]
async fn data_plane_lambda_function_error_returns_502() {
    if !require_docker_or_skip("data_plane_lambda_function_error_returns_502") {
        return;
    }
    use aws_sdk_lambda::primitives::Blob;
    use std::io::Write;

    let server = TestServer::start().await;
    let apigw = server.apigateway_client().await;
    let lambda = server.lambda_client().await;

    // Package a throwing handler.
    let cursor = std::io::Cursor::new(Vec::new());
    let mut writer = zip::ZipWriter::new(cursor);
    let opts = zip::write::SimpleFileOptions::default();
    writer.start_file("index.js", opts).unwrap();
    writer
        .write_all(b"exports.handler = async () => { throw new Error('boom'); };")
        .unwrap();
    let zip_bytes = writer.finish().unwrap().into_inner();

    lambda
        .create_function()
        .function_name("v1-throwing-fn")
        .runtime(aws_sdk_lambda::types::Runtime::Nodejs20x)
        .role("arn:aws:iam::123456789012:role/lambda-role")
        .handler("index.handler")
        .code(
            aws_sdk_lambda::types::FunctionCode::builder()
                .zip_file(Blob::new(zip_bytes))
                .build(),
        )
        .send()
        .await
        .expect("create_function");

    let api = apigw
        .create_rest_api()
        .name("v1-error-api")
        .send()
        .await
        .expect("create_rest_api");
    let api_id = api.id().unwrap().to_string();
    let root = api.root_resource_id().unwrap().to_string();
    let resource = apigw
        .create_resource()
        .rest_api_id(&api_id)
        .parent_id(&root)
        .path_part("boom")
        .send()
        .await
        .expect("create_resource");
    let res_id = resource.id().unwrap().to_string();
    apigw
        .put_method()
        .rest_api_id(&api_id)
        .resource_id(&res_id)
        .http_method("GET")
        .authorization_type("NONE")
        .send()
        .await
        .expect("put_method");
    apigw
        .put_integration()
        .rest_api_id(&api_id)
        .resource_id(&res_id)
        .http_method("GET")
        .r#type(aws_sdk_apigateway::types::IntegrationType::AwsProxy)
        .integration_http_method("POST")
        .uri("arn:aws:apigateway:us-east-1:lambda:path/2015-03-31/functions/arn:aws:lambda:us-east-1:123456789012:function:v1-throwing-fn/invocations")
        .send()
        .await
        .expect("put_integration");
    apigw
        .create_deployment()
        .rest_api_id(&api_id)
        .stage_name("prod")
        .send()
        .await
        .expect("create_deployment");

    let resp = reqwest::Client::new()
        .get(format!("{}/prod/boom", server.endpoint()))
        .header("host", execute_api_host(&api_id))
        .send()
        .await
        .expect("send");
    assert_eq!(resp.status(), 502);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["message"], "Internal server error");
}
