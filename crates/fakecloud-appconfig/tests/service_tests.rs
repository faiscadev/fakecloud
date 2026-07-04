//! End-to-end handler tests for AWS AppConfig + AppConfig Data.
//!
//! Each test drives [`AppConfigService::handle`] with a hand-built restJson1
//! `AwsRequest`, proving real round-trip behaviour: create -> get/list reflect
//! persisted state, update persists, delete removes (and cascades to children),
//! hosted configuration version bytes round-trip with an auto-incrementing
//! version number, deployments settle to COMPLETE, predefined deployment
//! strategies resolve, the AppConfig Data plane returns deployed bytes, tags
//! round-trip, and documented error codes fire.

use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;
use http::{HeaderMap, Method};
use parking_lot::{Mutex, RwLock};
use serde_json::{json, Value};

use fakecloud_appconfig::{AppConfigService, SharedAppConfigState};
use fakecloud_core::multi_account::MultiAccountState;
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsService, ResponseBody};

fn service() -> AppConfigService {
    let state: SharedAppConfigState = Arc::new(RwLock::new(MultiAccountState::new(
        "000000000000",
        "us-east-1",
        "",
    )));
    AppConfigService::new(state)
}

fn build(method: Method, path: &str, body: Bytes, headers: HeaderMap) -> AwsRequest {
    let raw_path = path.split('?').next().unwrap_or(path).to_string();
    let raw_query = path
        .split_once('?')
        .map(|(_, q)| q.to_string())
        .unwrap_or_default();
    let mut query_params = HashMap::new();
    for pair in raw_query.split('&').filter(|s| !s.is_empty()) {
        if let Some((k, v)) = pair.split_once('=') {
            query_params.insert(k.to_string(), v.to_string());
        }
    }
    let path_segments = raw_path
        .split('/')
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .collect();
    AwsRequest {
        service: "appconfig".to_string(),
        action: String::new(),
        region: "us-east-1".to_string(),
        account_id: "000000000000".to_string(),
        request_id: "test".to_string(),
        headers,
        query_params,
        body,
        body_stream: Mutex::new(None),
        path_segments,
        raw_path,
        raw_query,
        method,
        is_query_protocol: false,
        access_key_id: None,
        principal: None,
    }
}

fn req(method: Method, path: &str, body: Value) -> AwsRequest {
    build(
        method,
        path,
        Bytes::from(serde_json::to_vec(&body).unwrap()),
        HeaderMap::new(),
    )
}

async fn call(svc: &AppConfigService, r: AwsRequest) -> AwsResponse {
    svc.handle(r).await.expect("handler returned an error")
}

async fn call_err(svc: &AppConfigService, r: AwsRequest) -> (u16, String) {
    match svc.handle(r).await {
        Ok(_) => panic!("expected an error, got success"),
        Err(e) => (e.status().as_u16(), e.code().to_string()),
    }
}

fn raw_bytes(resp: &AwsResponse) -> Vec<u8> {
    match &resp.body {
        ResponseBody::Bytes(b) => b.to_vec(),
        _ => panic!("non-bytes body"),
    }
}

fn body_of(resp: &AwsResponse) -> Value {
    let bytes = raw_bytes(resp);
    if bytes.is_empty() {
        return Value::Null;
    }
    serde_json::from_slice(&bytes).unwrap()
}

fn header(resp: &AwsResponse, name: &str) -> String {
    resp.headers
        .get(name)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string()
}

fn id_of(v: &Value) -> String {
    v.get("Id").and_then(|x| x.as_str()).unwrap().to_string()
}

async fn make_app(svc: &AppConfigService, name: &str) -> String {
    let resp = call(
        svc,
        req(Method::POST, "/applications", json!({ "Name": name })),
    )
    .await;
    assert_eq!(resp.status.as_u16(), 201);
    id_of(&body_of(&resp))
}

async fn make_profile(svc: &AppConfigService, app: &str, name: &str) -> String {
    let resp = call(
        svc,
        req(
            Method::POST,
            &format!("/applications/{app}/configurationprofiles"),
            json!({ "Name": name, "LocationUri": "hosted" }),
        ),
    )
    .await;
    assert_eq!(resp.status.as_u16(), 201);
    id_of(&body_of(&resp))
}

async fn make_env(svc: &AppConfigService, app: &str, name: &str) -> String {
    let resp = call(
        svc,
        req(
            Method::POST,
            &format!("/applications/{app}/environments"),
            json!({ "Name": name }),
        ),
    )
    .await;
    assert_eq!(resp.status.as_u16(), 201);
    id_of(&body_of(&resp))
}

/// Create a hosted configuration version with a raw body + Content-Type header.
async fn make_hosted_version(
    svc: &AppConfigService,
    app: &str,
    profile: &str,
    content: &[u8],
    content_type: &str,
) -> AwsResponse {
    let mut headers = HeaderMap::new();
    headers.insert("Content-Type", content_type.parse().unwrap());
    let r = build(
        Method::POST,
        &format!("/applications/{app}/configurationprofiles/{profile}/hostedconfigurationversions"),
        Bytes::copy_from_slice(content),
        headers,
    );
    call(svc, r).await
}

#[tokio::test]
async fn application_crud_roundtrip() {
    let svc = service();
    let app = make_app(&svc, "my-app").await;

    let got = body_of(
        &call(
            &svc,
            req(Method::GET, &format!("/applications/{app}"), Value::Null),
        )
        .await,
    );
    assert_eq!(got["Name"], "my-app");

    // Update mutates.
    let updated = body_of(
        &call(
            &svc,
            req(
                Method::PATCH,
                &format!("/applications/{app}"),
                json!({ "Description": "desc" }),
            ),
        )
        .await,
    );
    assert_eq!(updated["Description"], "desc");

    // List reflects.
    let list = body_of(&call(&svc, req(Method::GET, "/applications", Value::Null)).await);
    assert_eq!(list["Items"].as_array().unwrap().len(), 1);

    // Delete removes.
    let del = call(
        &svc,
        req(Method::DELETE, &format!("/applications/{app}"), Value::Null),
    )
    .await;
    assert_eq!(del.status.as_u16(), 204);
    let (code, name) = call_err(
        &svc,
        req(Method::GET, &format!("/applications/{app}"), Value::Null),
    )
    .await;
    assert_eq!(code, 404);
    assert_eq!(name, "ResourceNotFoundException");
}

#[tokio::test]
async fn delete_application_cascades_to_children() {
    let svc = service();
    let app = make_app(&svc, "app").await;
    let profile = make_profile(&svc, &app, "prof").await;
    let env = make_env(&svc, &app, "env").await;

    call(
        &svc,
        req(Method::DELETE, &format!("/applications/{app}"), Value::Null),
    )
    .await;

    // Both children are gone (their parent lookups now 404).
    let (c1, _) = call_err(
        &svc,
        req(
            Method::GET,
            &format!("/applications/{app}/configurationprofiles/{profile}"),
            Value::Null,
        ),
    )
    .await;
    assert_eq!(c1, 404);
    let (c2, _) = call_err(
        &svc,
        req(
            Method::GET,
            &format!("/applications/{app}/environments/{env}"),
            Value::Null,
        ),
    )
    .await;
    assert_eq!(c2, 404);
}

#[tokio::test]
async fn hosted_version_bytes_roundtrip_and_autoincrement() {
    let svc = service();
    let app = make_app(&svc, "app").await;
    let profile = make_profile(&svc, &app, "prof").await;

    let content1 = b"{\"flag\": true}";
    let v1 = make_hosted_version(&svc, &app, &profile, content1, "application/json").await;
    assert_eq!(v1.status.as_u16(), 201);
    assert_eq!(header(&v1, "Version-Number"), "1");
    assert_eq!(raw_bytes(&v1), content1);

    // Second version auto-increments to 2 and keeps distinct bytes.
    let content2 = &[0u8, 1, 2, 3, 255];
    let v2 = make_hosted_version(&svc, &app, &profile, content2, "application/octet-stream").await;
    assert_eq!(header(&v2, "Version-Number"), "2");

    // Get returns the exact raw bytes + metadata headers.
    let got = call(
        &svc,
        req(
            Method::GET,
            &format!(
                "/applications/{app}/configurationprofiles/{profile}/hostedconfigurationversions/2"
            ),
            Value::Null,
        ),
    )
    .await;
    assert_eq!(raw_bytes(&got), content2);
    assert_eq!(header(&got, "Content-Type"), "application/octet-stream");
    // The id headers the model binds are populated on both create and get.
    assert_eq!(header(&got, "Application-Id"), app);
    assert_eq!(header(&got, "Configuration-Profile-Id"), profile);
    assert_eq!(header(&v1, "Application-Id"), app);
    assert_eq!(header(&v1, "Configuration-Profile-Id"), profile);

    // List returns both summaries.
    let list = body_of(
        &call(
            &svc,
            req(
                Method::GET,
                &format!("/applications/{app}/configurationprofiles/{profile}/hostedconfigurationversions"),
                Value::Null,
            ),
        )
        .await,
    );
    assert_eq!(list["Items"].as_array().unwrap().len(), 2);

    // Delete removes v1; deleting the profile cascades away the rest.
    let del = call(
        &svc,
        req(
            Method::DELETE,
            &format!(
                "/applications/{app}/configurationprofiles/{profile}/hostedconfigurationversions/1"
            ),
            Value::Null,
        ),
    )
    .await;
    assert_eq!(del.status.as_u16(), 204);
    call(
        &svc,
        req(
            Method::DELETE,
            &format!("/applications/{app}/configurationprofiles/{profile}"),
            Value::Null,
        ),
    )
    .await;
    let (code, _) = call_err(
        &svc,
        req(
            Method::GET,
            &format!(
                "/applications/{app}/configurationprofiles/{profile}/hostedconfigurationversions/2"
            ),
            Value::Null,
        ),
    )
    .await;
    assert_eq!(code, 404);
}

#[tokio::test]
async fn deployment_strategy_custom_and_predefined() {
    let svc = service();
    let created = body_of(
        &call(
            &svc,
            req(
                Method::POST,
                "/deploymentstrategies",
                json!({
                    "Name": "custom",
                    "DeploymentDurationInMinutes": 10,
                    "GrowthFactor": 25.0,
                    "GrowthType": "LINEAR",
                }),
            ),
        )
        .await,
    );
    let sid = id_of(&created);
    let got = body_of(
        &call(
            &svc,
            req(
                Method::GET,
                &format!("/deploymentstrategies/{sid}"),
                Value::Null,
            ),
        )
        .await,
    );
    assert_eq!(got["Name"], "custom");
    assert_eq!(got["DeploymentDurationInMinutes"], 10);

    // Predefined strategies resolve by their well-known ids.
    for id in [
        "AppConfig.AllAtOnce",
        "AppConfig.Linear50PercentEvery30Seconds",
        "AppConfig.Canary10Percent20Minutes",
    ] {
        let p = body_of(
            &call(
                &svc,
                req(
                    Method::GET,
                    &format!("/deploymentstrategies/{id}"),
                    Value::Null,
                ),
            )
            .await,
        );
        assert_eq!(p["Id"], id);
        assert_eq!(p["Name"], id);
    }

    // List includes the four predefined plus the custom one.
    let list = body_of(&call(&svc, req(Method::GET, "/deploymentstrategies", Value::Null)).await);
    assert!(list["Items"].as_array().unwrap().len() >= 5);

    // DELETE uses AWS's misspelled `deployementstrategies` path.
    let del = call(
        &svc,
        req(
            Method::DELETE,
            &format!("/deployementstrategies/{sid}"),
            Value::Null,
        ),
    )
    .await;
    assert_eq!(del.status.as_u16(), 204);
}

#[tokio::test]
async fn deployment_settles_to_complete() {
    let svc = service();
    let app = make_app(&svc, "app").await;
    let profile = make_profile(&svc, &app, "prof").await;
    let env = make_env(&svc, &app, "env").await;
    make_hosted_version(&svc, &app, &profile, b"content", "text/plain").await;

    let dep = body_of(
        &call(
            &svc,
            req(
                Method::POST,
                &format!("/applications/{app}/environments/{env}/deployments"),
                json!({
                    "DeploymentStrategyId": "AppConfig.AllAtOnce",
                    "ConfigurationProfileId": profile,
                    "ConfigurationVersion": "1",
                }),
            ),
        )
        .await,
    );
    assert_eq!(dep["State"], "COMPLETE");
    assert_eq!(dep["PercentageComplete"], 100.0);
    assert!(!dep["EventLog"].as_array().unwrap().is_empty());
    let number = dep["DeploymentNumber"].as_i64().unwrap();
    assert_eq!(number, 1);

    // Get reflects the settled deployment.
    let got = body_of(
        &call(
            &svc,
            req(
                Method::GET,
                &format!("/applications/{app}/environments/{env}/deployments/{number}"),
                Value::Null,
            ),
        )
        .await,
    );
    assert_eq!(got["State"], "COMPLETE");

    // Stop returns 202 and marks it rolled back.
    let stopped = call(
        &svc,
        req(
            Method::DELETE,
            &format!("/applications/{app}/environments/{env}/deployments/{number}"),
            Value::Null,
        ),
    )
    .await;
    assert_eq!(stopped.status.as_u16(), 202);
    assert_eq!(body_of(&stopped)["State"], "ROLLED_BACK");

    // List shows the deployment summary.
    let list = body_of(
        &call(
            &svc,
            req(
                Method::GET,
                &format!("/applications/{app}/environments/{env}/deployments"),
                Value::Null,
            ),
        )
        .await,
    );
    assert_eq!(list["Items"].as_array().unwrap().len(), 1);
}

#[tokio::test]
async fn extension_and_association_crud() {
    let svc = service();
    let ext = body_of(
        &call(
            &svc,
            req(
                Method::POST,
                "/extensions",
                json!({ "Name": "ext", "Actions": { "PRE_START_DEPLOYMENT": [] } }),
            ),
        )
        .await,
    );
    let ext_id = id_of(&ext);
    assert_eq!(ext["VersionNumber"], 1);

    let got = body_of(
        &call(
            &svc,
            req(Method::GET, &format!("/extensions/{ext_id}"), Value::Null),
        )
        .await,
    );
    assert_eq!(got["Name"], "ext");

    // UpdateExtension bumps VersionNumber (AWS increments on every update).
    let updated = body_of(
        &call(
            &svc,
            req(
                Method::PATCH,
                &format!("/extensions/{ext_id}"),
                json!({ "Description": "v2" }),
            ),
        )
        .await,
    );
    assert_eq!(updated["VersionNumber"], 2);
    assert_eq!(updated["Description"], "v2");

    let assoc = body_of(
        &call(
            &svc,
            req(
                Method::POST,
                "/extensionassociations",
                json!({ "ExtensionIdentifier": ext_id, "ResourceIdentifier": "arn:aws:appconfig:us-east-1:000000000000:application/abc1234" }),
            ),
        )
        .await,
    );
    let assoc_id = id_of(&assoc);
    let got_assoc = body_of(
        &call(
            &svc,
            req(
                Method::GET,
                &format!("/extensionassociations/{assoc_id}"),
                Value::Null,
            ),
        )
        .await,
    );
    assert_eq!(
        got_assoc["ResourceArn"],
        "arn:aws:appconfig:us-east-1:000000000000:application/abc1234"
    );

    let del = call(
        &svc,
        req(
            Method::DELETE,
            &format!("/extensionassociations/{assoc_id}"),
            Value::Null,
        ),
    )
    .await;
    assert_eq!(del.status.as_u16(), 204);
}

#[tokio::test]
async fn delete_application_cascades_child_tags() {
    // Regression: deleting an application must drop the tags of its child
    // environments and profiles, not just the application's own tags.
    let svc = service();
    let app = make_app(&svc, "app").await;
    let profile = make_profile(&svc, &app, "prof").await;
    let env = make_env(&svc, &app, "env").await;

    let app_arn = format!("arn:aws:appconfig:us-east-1:000000000000:application/{app}");
    let env_arn = format!("{app_arn}/environment/{env}");
    let profile_arn = format!("{app_arn}/configurationprofile/{profile}");
    for arn in [&app_arn, &env_arn, &profile_arn] {
        let enc = arn.replace('/', "%2F");
        call(
            &svc,
            req(
                Method::POST,
                &format!("/tags/{enc}"),
                json!({ "Tags": { "team": "infra" } }),
            ),
        )
        .await;
    }

    // Delete the application; its cascade must sweep child tags too.
    let del = call(
        &svc,
        req(Method::DELETE, &format!("/applications/{app}"), Value::Null),
    )
    .await;
    assert_eq!(del.status.as_u16(), 204);

    for arn in [&app_arn, &env_arn, &profile_arn] {
        let enc = arn.replace('/', "%2F");
        let tags =
            body_of(&call(&svc, req(Method::GET, &format!("/tags/{enc}"), Value::Null)).await);
        assert!(
            tags["Tags"].as_object().unwrap().is_empty(),
            "tags for {arn} should have been cascaded away"
        );
    }
}

#[tokio::test]
async fn delete_extension_association_removes_referenced_extension() {
    // Regression: deleting the sole association referencing an AWS-authored
    // extension must drop the referenced-extension shim it created.
    let svc = service();
    let assoc = body_of(
        &call(
            &svc,
            req(
                Method::POST,
                "/extensionassociations",
                json!({
                    "ExtensionIdentifier": "AWS.AppConfig.JiraServiceManagement",
                    "ResourceIdentifier": "arn:aws:appconfig:us-east-1:000000000000:application/abc1234",
                }),
            ),
        )
        .await,
    );
    let assoc_id = id_of(&assoc);

    // GetExtension resolves via the referenced-extension shim.
    let got = body_of(
        &call(
            &svc,
            req(
                Method::GET,
                "/extensions/AWS.AppConfig.JiraServiceManagement",
                Value::Null,
            ),
        )
        .await,
    );
    assert_eq!(got["Name"], "AWS.AppConfig.JiraServiceManagement");

    call(
        &svc,
        req(
            Method::DELETE,
            &format!("/extensionassociations/{assoc_id}"),
            Value::Null,
        ),
    )
    .await;

    // The shim is gone now that no association references it.
    let (code, _) = call_err(
        &svc,
        req(
            Method::GET,
            "/extensions/AWS.AppConfig.JiraServiceManagement",
            Value::Null,
        ),
    )
    .await;
    assert_eq!(code, 404);
}

#[tokio::test]
async fn experiment_definition_and_run_lifecycle() {
    let svc = service();
    let app = make_app(&svc, "app").await;
    let profile = make_profile(&svc, &app, "prof").await;
    let env = make_env(&svc, &app, "env").await;

    let def = body_of(
        &call(
            &svc,
            req(
                Method::POST,
                &format!("/applications/{app}/experimentdefinitions"),
                json!({
                    "Name": "exp",
                    "ConfigurationProfileIdentifier": profile,
                    "EnvironmentIdentifier": env,
                    "FlagKey": "flag",
                    "Treatments": [{ "Name": "t1", "VariantKey": "v1" }],
                    "Control": { "Name": "c", "VariantKey": "v0" },
                    "AudienceRule": "true",
                }),
            ),
        )
        .await,
    );
    let def_id = id_of(&def);
    assert_eq!(def["Status"], "IDLE");

    let run = body_of(
        &call(
            &svc,
            req(
                Method::POST,
                &format!("/applications/{app}/experimentdefinitions/{def_id}/experimentruns"),
                json!({ "Description": "run one" }),
            ),
        )
        .await,
    );
    let run_no = run["Run"].as_i64().unwrap();
    assert_eq!(run["Status"], "RUNNING");

    let stopped = body_of(
        &call(
            &svc,
            req(
                Method::PATCH,
                &format!("/applications/{app}/experimentdefinitions/{def_id}/experimentruns/{run_no}/stop"),
                json!({}),
            ),
        )
        .await,
    );
    assert_eq!(stopped["Status"], "DONE");
}

#[tokio::test]
async fn account_settings_get_and_update() {
    let svc = service();
    let default = body_of(&call(&svc, req(Method::GET, "/settings", Value::Null)).await);
    assert_eq!(default["DeletionProtection"]["Enabled"], false);

    let updated = body_of(
        &call(
            &svc,
            req(
                Method::PATCH,
                "/settings",
                json!({ "DeletionProtection": { "Enabled": true, "ProtectionPeriodInMinutes": 30 } }),
            ),
        )
        .await,
    );
    assert_eq!(updated["DeletionProtection"]["Enabled"], true);

    let reread = body_of(&call(&svc, req(Method::GET, "/settings", Value::Null)).await);
    assert_eq!(reread["DeletionProtection"]["Enabled"], true);
}

#[tokio::test]
async fn tags_roundtrip() {
    let svc = service();
    let app = make_app(&svc, "app").await;
    let arn = format!("arn:aws:appconfig:us-east-1:000000000000:application/{app}");
    let enc = arn.replace('/', "%2F");

    call(
        &svc,
        req(
            Method::POST,
            &format!("/tags/{enc}"),
            json!({ "Tags": { "team": "infra" } }),
        ),
    )
    .await;
    let tags = body_of(&call(&svc, req(Method::GET, &format!("/tags/{enc}"), Value::Null)).await);
    assert_eq!(tags["Tags"]["team"], "infra");

    call(
        &svc,
        req(
            Method::DELETE,
            &format!("/tags/{enc}?tagKeys=team"),
            Value::Null,
        ),
    )
    .await;
    let after = body_of(&call(&svc, req(Method::GET, &format!("/tags/{enc}"), Value::Null)).await);
    assert!(after["Tags"].as_object().unwrap().is_empty());
}

/// Deploy `version` of `profile` to `env` via AllAtOnce (settles COMPLETE).
async fn deploy(svc: &AppConfigService, app: &str, env: &str, profile: &str, version: &str) {
    let resp = call(
        svc,
        req(
            Method::POST,
            &format!("/applications/{app}/environments/{env}/deployments"),
            json!({
                "DeploymentStrategyId": "AppConfig.AllAtOnce",
                "ConfigurationProfileId": profile,
                "ConfigurationVersion": version,
            }),
        ),
    )
    .await;
    assert_eq!(resp.status.as_u16(), 201);
}

/// Start a data-plane session, returning its InitialConfigurationToken.
async fn start_session(svc: &AppConfigService, app: &str, env: &str, profile: &str) -> String {
    let session = body_of(
        &call(
            svc,
            req(
                Method::POST,
                "/configurationsessions",
                json!({
                    "ApplicationIdentifier": app,
                    "EnvironmentIdentifier": env,
                    "ConfigurationProfileIdentifier": profile,
                }),
            ),
        )
        .await,
    );
    session["InitialConfigurationToken"]
        .as_str()
        .unwrap()
        .to_string()
}

#[tokio::test]
async fn appconfigdata_session_returns_deployed_bytes() {
    let svc = service();
    let app = make_app(&svc, "app").await;
    let profile = make_profile(&svc, &app, "prof").await;
    let env = make_env(&svc, &app, "env").await;
    let content = b"{\"feature\": \"on\"}";
    make_hosted_version(&svc, &app, &profile, content, "application/json").await;
    deploy(&svc, &app, &env, &profile, "1").await;

    let token = start_session(&svc, &app, &env, &profile).await;

    // GetLatestConfiguration returns the deployed hosted-config bytes.
    let cfg = call(
        &svc,
        req(
            Method::GET,
            &format!("/configuration?configuration_token={token}"),
            Value::Null,
        ),
    )
    .await;
    assert_eq!(raw_bytes(&cfg), content);
    assert_eq!(header(&cfg, "Content-Type"), "application/json");
    assert!(!header(&cfg, "Next-Poll-Configuration-Token").is_empty());

    // An unknown token is a BadRequestException.
    let (code, name) = call_err(
        &svc,
        req(
            Method::GET,
            "/configuration?configuration_token=bogus",
            Value::Null,
        ),
    )
    .await;
    assert_eq!(code, 400);
    assert_eq!(name, "BadRequestException");
}

#[tokio::test]
async fn appconfigdata_next_poll_token_resolves_on_second_poll() {
    // Regression: the rotated Next-Poll-Configuration-Token must be a live
    // token, or the AWS poll loop dies with "Invalid ConfigurationToken".
    let svc = service();
    let app = make_app(&svc, "app").await;
    let profile = make_profile(&svc, &app, "prof").await;
    let env = make_env(&svc, &app, "env").await;
    let content = b"{\"k\": 1}";
    make_hosted_version(&svc, &app, &profile, content, "application/json").await;
    deploy(&svc, &app, &env, &profile, "1").await;

    let token = start_session(&svc, &app, &env, &profile).await;
    let first = call(
        &svc,
        req(
            Method::GET,
            &format!("/configuration?configuration_token={token}"),
            Value::Null,
        ),
    )
    .await;
    assert_eq!(first.status.as_u16(), 200);
    let next = header(&first, "Next-Poll-Configuration-Token");
    assert!(!next.is_empty());

    // Poll again with the rotated token: still 200, still the bytes.
    let second = call(
        &svc,
        req(
            Method::GET,
            &format!("/configuration?configuration_token={next}"),
            Value::Null,
        ),
    )
    .await;
    assert_eq!(second.status.as_u16(), 200);
    assert_eq!(raw_bytes(&second), content);
    assert!(!header(&second, "Next-Poll-Configuration-Token").is_empty());
}

#[tokio::test]
async fn appconfigdata_serves_deployed_version_not_latest() {
    // Regression: the data plane must serve the DEPLOYED version, not merely
    // the newest hosted version.
    let svc = service();
    let app = make_app(&svc, "app").await;
    let profile = make_profile(&svc, &app, "prof").await;
    let env = make_env(&svc, &app, "env").await;

    let v1_bytes = b"{\"v\": 1}";
    make_hosted_version(&svc, &app, &profile, v1_bytes, "application/json").await;
    deploy(&svc, &app, &env, &profile, "1").await;

    // Create v2 but DO NOT deploy it.
    make_hosted_version(&svc, &app, &profile, b"{\"v\": 2}", "application/json").await;

    let token = start_session(&svc, &app, &env, &profile).await;
    let cfg = call(
        &svc,
        req(
            Method::GET,
            &format!("/configuration?configuration_token={token}"),
            Value::Null,
        ),
    )
    .await;
    // Deployed v1 wins over the newer, undeployed v2.
    assert_eq!(raw_bytes(&cfg), v1_bytes);

    // Legacy GetConfiguration agrees.
    let legacy = call(
        &svc,
        req(
            Method::GET,
            &format!(
                "/applications/{app}/environments/{env}/configurations/{profile}?client_id=c1"
            ),
            Value::Null,
        ),
    )
    .await;
    assert_eq!(raw_bytes(&legacy), v1_bytes);
    assert_eq!(header(&legacy, "Configuration-Version"), "1");
}

#[tokio::test]
async fn appconfigdata_no_deployment_returns_empty() {
    // An environment with no deployment serves an empty configuration.
    let svc = service();
    let app = make_app(&svc, "app").await;
    let profile = make_profile(&svc, &app, "prof").await;
    let env = make_env(&svc, &app, "env").await;
    make_hosted_version(&svc, &app, &profile, b"{\"x\": 1}", "application/json").await;

    let token = start_session(&svc, &app, &env, &profile).await;
    let cfg = call(
        &svc,
        req(
            Method::GET,
            &format!("/configuration?configuration_token={token}"),
            Value::Null,
        ),
    )
    .await;
    assert_eq!(cfg.status.as_u16(), 200);
    assert!(raw_bytes(&cfg).is_empty());
    // No served version => no Version-Label header (never the literal "null").
    assert_eq!(header(&cfg, "Version-Label"), "");
}

#[tokio::test]
async fn appconfigdata_version_label_is_the_label_not_number() {
    // Regression: Version-Label must carry the served version's label string,
    // not its number, and be omitted when unlabeled.
    let svc = service();
    let app = make_app(&svc, "app").await;
    let profile = make_profile(&svc, &app, "prof").await;
    let env = make_env(&svc, &app, "env").await;

    // Hosted version 1 carries a VersionLabel header.
    let mut headers = HeaderMap::new();
    headers.insert("Content-Type", "application/json".parse().unwrap());
    headers.insert("VersionLabel", "release-2024".parse().unwrap());
    let r = build(
        Method::POST,
        &format!("/applications/{app}/configurationprofiles/{profile}/hostedconfigurationversions"),
        Bytes::from_static(b"{\"labeled\": true}"),
        headers,
    );
    call(&svc, r).await;
    deploy(&svc, &app, &env, &profile, "1").await;

    let token = start_session(&svc, &app, &env, &profile).await;
    let cfg = call(
        &svc,
        req(
            Method::GET,
            &format!("/configuration?configuration_token={token}"),
            Value::Null,
        ),
    )
    .await;
    assert_eq!(header(&cfg, "Version-Label"), "release-2024");
}

#[tokio::test]
async fn missing_and_invalid_inputs_error() {
    let svc = service();
    // Missing required Name -> BadRequestException.
    let (code, name) = call_err(&svc, req(Method::POST, "/applications", json!({}))).await;
    assert_eq!(code, 400);
    assert_eq!(name, "BadRequestException");

    // Unknown application -> ResourceNotFoundException.
    let (code, name) = call_err(&svc, req(Method::GET, "/applications/zzz9999", Value::Null)).await;
    assert_eq!(code, 404);
    assert_eq!(name, "ResourceNotFoundException");

    // Out-of-range MaxResults is rejected by model validation.
    let (code, _) = call_err(
        &svc,
        req(Method::GET, "/applications?max_results=0", Value::Null),
    )
    .await;
    assert_eq!(code, 400);
}
