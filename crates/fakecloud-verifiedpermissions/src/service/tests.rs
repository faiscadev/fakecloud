use super::*;
use bytes::Bytes;
use fakecloud_core::multi_account::MultiAccountState;
use http::{HeaderMap, Method};
use parking_lot::{Mutex, RwLock};
use std::collections::HashMap;

fn svc() -> VerifiedPermissionsService {
    VerifiedPermissionsService::new(Arc::new(RwLock::new(MultiAccountState::new(
        "000000000000",
        "us-east-1",
        "",
    ))))
}

fn req(action: &str, body: Value) -> AwsRequest {
    AwsRequest {
        service: "verifiedpermissions".into(),
        action: action.into(),
        region: "us-east-1".into(),
        account_id: "000000000000".into(),
        request_id: "req".into(),
        headers: HeaderMap::new(),
        query_params: HashMap::new(),
        body: Bytes::from(serde_json::to_vec(&body).unwrap()),
        body_stream: Mutex::new(None),
        path_segments: vec![],
        raw_path: String::new(),
        raw_query: String::new(),
        method: Method::POST,
        is_query_protocol: false,
        access_key_id: None,
        principal: None,
    }
}

fn call(s: &VerifiedPermissionsService, action: &str, body: Value) -> Value {
    let resp = dispatch(s, &req(action, body)).expect("op ok");
    serde_json::from_slice(resp.body.expect_bytes()).unwrap()
}

fn call_err(s: &VerifiedPermissionsService, action: &str, body: Value) -> AwsServiceError {
    dispatch(s, &req(action, body)).err().expect("op err")
}

fn new_store(s: &VerifiedPermissionsService) -> String {
    call(
        s,
        "CreatePolicyStore",
        json!({ "validationSettings": { "mode": "OFF" } }),
    )["policyStoreId"]
        .as_str()
        .unwrap()
        .to_string()
}

#[test]
fn create_get_policy_store_round_trip() {
    let s = svc();
    let created = call(
        &s,
        "CreatePolicyStore",
        json!({ "validationSettings": { "mode": "STRICT" }, "description": "prod" }),
    );
    let id = created["policyStoreId"].as_str().unwrap().to_string();
    assert!(created["arn"].as_str().unwrap().contains(&id));
    let got = call(&s, "GetPolicyStore", json!({ "policyStoreId": id }));
    assert_eq!(got["validationSettings"]["mode"], json!("STRICT"));
    assert_eq!(got["description"], json!("prod"));
}

#[test]
fn create_policy_store_requires_mode() {
    let s = svc();
    let err = call_err(&s, "CreatePolicyStore", json!({ "validationSettings": {} }));
    assert_eq!(err.code(), "ValidationException");
}

#[test]
fn update_policy_store_changes_mode() {
    let s = svc();
    let id = new_store(&s);
    call(
        &s,
        "UpdatePolicyStore",
        json!({ "policyStoreId": id, "validationSettings": { "mode": "STRICT" }, "description": "d" }),
    );
    let got = call(&s, "GetPolicyStore", json!({ "policyStoreId": id }));
    assert_eq!(got["validationSettings"]["mode"], json!("STRICT"));
}

#[test]
fn delete_policy_store_is_idempotent_and_honors_protection() {
    let s = svc();
    // idempotent on missing store
    call(
        &s,
        "DeletePolicyStore",
        json!({ "policyStoreId": "missing" }),
    );
    let id = call(
        &s,
        "CreatePolicyStore",
        json!({ "validationSettings": { "mode": "OFF" }, "deletionProtection": "ENABLED" }),
    )["policyStoreId"]
        .as_str()
        .unwrap()
        .to_string();
    let err = call_err(&s, "DeletePolicyStore", json!({ "policyStoreId": id }));
    assert_eq!(err.code(), "InvalidStateException");
}

#[test]
fn list_policy_stores_paginates() {
    let s = svc();
    for _ in 0..3 {
        new_store(&s);
    }
    let page1 = call(&s, "ListPolicyStores", json!({ "maxResults": 2 }));
    assert_eq!(page1["policyStores"].as_array().unwrap().len(), 2);
    let token = page1["nextToken"].as_str().unwrap().to_string();
    let page2 = call(
        &s,
        "ListPolicyStores",
        json!({ "maxResults": 2, "nextToken": token }),
    );
    assert_eq!(page2["policyStores"].as_array().unwrap().len(), 1);
    assert!(page2.get("nextToken").is_none());
}

#[test]
fn put_get_schema_round_trip() {
    let s = svc();
    let id = new_store(&s);
    let schema = r#"{"PhotoApp":{"entityTypes":{"User":{}},"actions":{"view":{}}}}"#;
    let put = call(
        &s,
        "PutSchema",
        json!({ "policyStoreId": id, "definition": { "cedarJson": schema } }),
    );
    assert_eq!(put["namespaces"], json!(["PhotoApp"]));
    let got = call(&s, "GetSchema", json!({ "policyStoreId": id }));
    assert_eq!(got["schema"].as_str().unwrap(), schema);
}

#[test]
fn get_schema_before_put_is_not_found() {
    let s = svc();
    let id = new_store(&s);
    let err = call_err(&s, "GetSchema", json!({ "policyStoreId": id }));
    assert_eq!(err.code(), "ResourceNotFoundException");
}

#[test]
fn static_policy_crud() {
    let s = svc();
    let id = new_store(&s);
    let created = call(
        &s,
        "CreatePolicy",
        json!({
            "policyStoreId": id,
            "definition": { "static": { "statement": "permit(principal, action, resource);", "description": "all" } }
        }),
    );
    assert_eq!(created["policyType"], json!("STATIC"));
    assert_eq!(created["effect"], json!("Permit"));
    let pid = created["policyId"].as_str().unwrap().to_string();
    let got = call(
        &s,
        "GetPolicy",
        json!({ "policyStoreId": id, "policyId": pid }),
    );
    assert_eq!(
        got["definition"]["static"]["statement"],
        json!("permit(principal, action, resource);")
    );
    call(
        &s,
        "DeletePolicy",
        json!({ "policyStoreId": id, "policyId": pid }),
    );
    let err = call_err(
        &s,
        "GetPolicy",
        json!({ "policyStoreId": id, "policyId": pid }),
    );
    assert_eq!(err.code(), "ResourceNotFoundException");
}

#[test]
fn create_policy_on_missing_store_is_not_found() {
    let s = svc();
    let err = call_err(
        &s,
        "CreatePolicy",
        json!({
            "policyStoreId": "nope",
            "definition": { "static": { "statement": "permit(principal, action, resource);" } }
        }),
    );
    assert_eq!(err.code(), "ResourceNotFoundException");
}

#[test]
fn invalid_cedar_statement_is_validation_error() {
    let s = svc();
    let id = new_store(&s);
    let err = call_err(
        &s,
        "CreatePolicy",
        json!({
            "policyStoreId": id,
            "definition": { "static": { "statement": "this is not cedar" } }
        }),
    );
    assert_eq!(err.code(), "ValidationException");
}

#[test]
fn template_linked_policy() {
    let s = svc();
    let id = new_store(&s);
    let tid = call(
        &s,
        "CreatePolicyTemplate",
        json!({ "policyStoreId": id, "statement": "permit(principal == ?principal, action, resource);" }),
    )["policyTemplateId"]
        .as_str()
        .unwrap()
        .to_string();
    let created = call(
        &s,
        "CreatePolicy",
        json!({
            "policyStoreId": id,
            "definition": { "templateLinked": {
                "policyTemplateId": tid,
                "principal": { "entityType": "User", "entityId": "alice" }
            } }
        }),
    );
    assert_eq!(created["policyType"], json!("TEMPLATE_LINKED"));
    let pid = created["policyId"].as_str().unwrap().to_string();
    let got = call(
        &s,
        "GetPolicy",
        json!({ "policyStoreId": id, "policyId": pid }),
    );
    assert_eq!(
        got["definition"]["templateLinked"]["policyTemplateId"],
        json!(tid)
    );
}

#[test]
fn identity_source_lifecycle() {
    let s = svc();
    let id = new_store(&s);
    let config = json!({ "cognitoUserPoolConfiguration": {
        "userPoolArn": "arn:aws:cognito-idp:us-east-1:000000000000:userpool/us-east-1_abc123",
        "clientIds": ["client1"]
    } });
    let isid = call(
        &s,
        "CreateIdentitySource",
        json!({ "policyStoreId": id, "configuration": config, "principalEntityType": "User" }),
    )["identitySourceId"]
        .as_str()
        .unwrap()
        .to_string();
    let got = call(
        &s,
        "GetIdentitySource",
        json!({ "policyStoreId": id, "identitySourceId": isid }),
    );
    assert_eq!(got["principalEntityType"], json!("User"));
    let list = call(&s, "ListIdentitySources", json!({ "policyStoreId": id }));
    assert_eq!(list["identitySources"].as_array().unwrap().len(), 1);
}

#[test]
fn convert_openid_source_to_cognito_resets_principal_entity_type() {
    // Converting an OpenID identity source (explicit principal entity type) to
    // a Cognito one without specifying a principal entity type resets it to the
    // `AWS::Cognito` default, matching real Verified Permissions.
    let s = svc();
    let id = new_store(&s);
    let isid = call(
        &s,
        "CreateIdentitySource",
        json!({
            "policyStoreId": id,
            "principalEntityType": "MyCorp::User",
            "configuration": { "openIdConnectConfiguration": {
                "issuer": "https://issuer.example.com",
                "tokenSelection": { "accessTokenOnly": { "audiences": ["aud"] } }
            } }
        }),
    )["identitySourceId"]
        .as_str()
        .unwrap()
        .to_string();
    call(
        &s,
        "UpdateIdentitySource",
        json!({
            "policyStoreId": id,
            "identitySourceId": isid,
            "updateConfiguration": { "cognitoUserPoolConfiguration": {
                "userPoolArn": "arn:aws:cognito-idp:us-east-1:000000000000:userpool/us-east-1_abc123"
            } }
        }),
    );
    let got = call(
        &s,
        "GetIdentitySource",
        json!({ "policyStoreId": id, "identitySourceId": isid }),
    );
    assert_eq!(got["principalEntityType"], json!("AWS::Cognito"));
}

#[test]
fn alias_lifecycle_and_conflict() {
    let s = svc();
    let id = new_store(&s);
    let alias = "policy-store-alias/my-store";
    call(
        &s,
        "CreatePolicyStoreAlias",
        json!({ "aliasName": alias, "policyStoreId": id }),
    );
    let err = call_err(
        &s,
        "CreatePolicyStoreAlias",
        json!({ "aliasName": alias, "policyStoreId": id }),
    );
    assert_eq!(err.code(), "ConflictException");
    let got = call(&s, "GetPolicyStoreAlias", json!({ "aliasName": alias }));
    assert_eq!(got["state"], json!("Active"));
    let list = call(&s, "ListPolicyStoreAliases", json!({}));
    assert_eq!(list["policyStoreAliases"].as_array().unwrap().len(), 1);
    call(&s, "DeletePolicyStoreAlias", json!({ "aliasName": alias }));
    let err = call_err(&s, "GetPolicyStoreAlias", json!({ "aliasName": alias }));
    assert_eq!(err.code(), "ResourceNotFoundException");
}

#[test]
fn tagging_round_trip() {
    let s = svc();
    let created = call(
        &s,
        "CreatePolicyStore",
        json!({ "validationSettings": { "mode": "OFF" } }),
    );
    let arn = created["arn"].as_str().unwrap().to_string();
    call(
        &s,
        "TagResource",
        json!({ "resourceArn": arn, "tags": { "team": "sec", "env": "prod" } }),
    );
    let got = call(&s, "ListTagsForResource", json!({ "resourceArn": arn }));
    assert_eq!(got["tags"]["team"], json!("sec"));
    call(
        &s,
        "UntagResource",
        json!({ "resourceArn": arn, "tagKeys": ["team"] }),
    );
    let got = call(&s, "ListTagsForResource", json!({ "resourceArn": arn }));
    assert!(got["tags"].get("team").is_none());
    assert_eq!(got["tags"]["env"], json!("prod"));
}

#[test]
fn is_authorized_allow_and_deny() {
    let s = svc();
    let id = new_store(&s);
    call(
        &s,
        "CreatePolicy",
        json!({
            "policyStoreId": id,
            "definition": { "static": { "statement": "permit(principal == User::\"alice\", action == Action::\"view\", resource == Photo::\"vacation\");" } }
        }),
    );
    let allow = call(
        &s,
        "IsAuthorized",
        json!({
            "policyStoreId": id,
            "principal": { "entityType": "User", "entityId": "alice" },
            "action": { "actionType": "Action", "actionId": "view" },
            "resource": { "entityType": "Photo", "entityId": "vacation" }
        }),
    );
    assert_eq!(allow["decision"], json!("ALLOW"));
    assert_eq!(allow["determiningPolicies"].as_array().unwrap().len(), 1);

    let deny = call(
        &s,
        "IsAuthorized",
        json!({
            "policyStoreId": id,
            "principal": { "entityType": "User", "entityId": "bob" },
            "action": { "actionType": "Action", "actionId": "view" },
            "resource": { "entityType": "Photo", "entityId": "vacation" }
        }),
    );
    assert_eq!(deny["decision"], json!("DENY"));
    assert!(deny["determiningPolicies"].as_array().unwrap().is_empty());
}

#[test]
fn is_authorized_on_missing_store_is_not_found() {
    let s = svc();
    let err = call_err(
        &s,
        "IsAuthorized",
        json!({
            "policyStoreId": "missing",
            "principal": { "entityType": "User", "entityId": "alice" },
            "action": { "actionType": "Action", "actionId": "view" },
            "resource": { "entityType": "Photo", "entityId": "x" }
        }),
    );
    assert_eq!(err.code(), "ResourceNotFoundException");
}

#[test]
fn is_authorized_with_token_resolves_principal_from_sub() {
    let s = svc();
    let id = new_store(&s);
    call(
        &s,
        "CreateIdentitySource",
        json!({
            "policyStoreId": id,
            "principalEntityType": "MyApp::User",
            "configuration": { "cognitoUserPoolConfiguration": {
                "userPoolArn": "arn:aws:cognito-idp:us-east-1:000000000000:userpool/us-east-1_abc123",
                "clientIds": ["c1"]
            } }
        }),
    );
    call(
        &s,
        "CreatePolicy",
        json!({
            "policyStoreId": id,
            "definition": { "static": { "statement": "permit(principal == MyApp::User::\"alice\", action == Action::\"view\", resource);" } }
        }),
    );
    // JWT with payload {"sub":"alice"} (unsigned; only the claim set matters).
    let token = "eyJhbGciOiJub25lIn0.eyJzdWIiOiJhbGljZSJ9.sig";
    let out = call(
        &s,
        "IsAuthorizedWithToken",
        json!({
            "policyStoreId": id,
            "identityToken": token,
            "action": { "actionType": "Action", "actionId": "view" },
            "resource": { "entityType": "Photo", "entityId": "p1" }
        }),
    );
    assert_eq!(out["principal"]["entityId"], json!("alice"));
    assert_eq!(out["principal"]["entityType"], json!("MyApp::User"));
    assert_eq!(out["decision"], json!("ALLOW"));
}

#[test]
fn batch_get_policy_mixes_found_and_errors() {
    let s = svc();
    let id = new_store(&s);
    let pid = call(
        &s,
        "CreatePolicy",
        json!({
            "policyStoreId": id,
            "definition": { "static": { "statement": "permit(principal, action, resource);" } }
        }),
    )["policyId"]
        .as_str()
        .unwrap()
        .to_string();
    let out = call(
        &s,
        "BatchGetPolicy",
        json!({ "requests": [
            { "policyStoreId": id, "policyId": pid },
            { "policyStoreId": id, "policyId": "missing" },
            { "policyStoreId": "nostore", "policyId": "x" }
        ] }),
    );
    assert_eq!(out["results"].as_array().unwrap().len(), 1);
    let errors = out["errors"].as_array().unwrap();
    assert_eq!(errors.len(), 2);
    assert_eq!(errors[0]["code"], json!("POLICY_NOT_FOUND"));
    assert_eq!(errors[1]["code"], json!("POLICY_STORE_NOT_FOUND"));
}
