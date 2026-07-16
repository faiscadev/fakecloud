use super::*;
use bytes::Bytes;
use http::{HeaderMap, Method};
use parking_lot::RwLock;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

fn make_state() -> SharedSecretsManagerState {
    Arc::new(RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new(
            "123456789012",
            "us-east-1",
            "http://localhost:4566",
        ),
    ))
}

fn expect_err(result: Result<AwsResponse, AwsServiceError>) -> AwsServiceError {
    match result {
        Err(e) => e,
        Ok(_) => panic!("expected error, got Ok"),
    }
}

fn make_request(action: &str, body: &str) -> AwsRequest {
    AwsRequest {
        service: "secretsmanager".to_string(),
        action: action.to_string(),
        region: "us-east-1".to_string(),
        account_id: "123456789012".to_string(),
        request_id: "test-request-id".to_string(),
        headers: HeaderMap::new(),
        query_params: HashMap::new(),
        body: Bytes::from(body.to_string()),
        body_stream: parking_lot::Mutex::new(None),
        path_segments: vec![],
        raw_path: "/".to_string(),
        raw_query: String::new(),
        method: Method::POST,
        is_query_protocol: false,
        access_key_id: None,
        principal: None,
    }
}

#[tokio::test]
async fn test_create_and_get_secret() {
    let state = make_state();
    let svc = SecretsManagerService::new(state);

    let req = make_request(
        "CreateSecret",
        r#"{"Name": "test/secret", "SecretString": "mysecretvalue"}"#,
    );
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::OK);
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["Name"], "test/secret");
    assert!(body["ARN"].as_str().unwrap().contains("test/secret"));

    let req = make_request("GetSecretValue", r#"{"SecretId": "test/secret"}"#);
    let resp = svc.handle(req).await.unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["SecretString"], "mysecretvalue");
}

#[tokio::test]
async fn test_create_secret_without_value() {
    let state = make_state();
    let svc = SecretsManagerService::new(state);

    let req = make_request("CreateSecret", r#"{"Name": "empty-secret"}"#);
    let resp = svc.handle(req).await.unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["Name"], "empty-secret");
    assert!(body.get("VersionId").is_none());
}

#[tokio::test]
async fn test_put_secret_value_creates_version() {
    let state = make_state();
    let svc = SecretsManagerService::new(state);

    let req = make_request(
        "CreateSecret",
        r#"{"Name": "versioned", "SecretString": "v1"}"#,
    );
    svc.handle(req).await.unwrap();

    let req = make_request(
        "PutSecretValue",
        r#"{"SecretId": "versioned", "SecretString": "v2"}"#,
    );
    let resp = svc.handle(req).await.unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["Name"], "versioned");

    // Get should return v2
    let req = make_request("GetSecretValue", r#"{"SecretId": "versioned"}"#);
    let resp = svc.handle(req).await.unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["SecretString"], "v2");
}

#[tokio::test]
async fn test_delete_and_restore_secret() {
    let state = make_state();
    let svc = SecretsManagerService::new(state);

    let req = make_request(
        "CreateSecret",
        r#"{"Name": "deleteme", "SecretString": "value"}"#,
    );
    svc.handle(req).await.unwrap();

    // Delete (soft)
    let req = make_request("DeleteSecret", r#"{"SecretId": "deleteme"}"#);
    let resp = svc.handle(req).await.unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert!(body["DeletionDate"].as_f64().is_some());

    // GetSecretValue should fail
    let req = make_request("GetSecretValue", r#"{"SecretId": "deleteme"}"#);
    assert!(svc.handle(req).await.is_err());

    // Restore
    let req = make_request("RestoreSecret", r#"{"SecretId": "deleteme"}"#);
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::OK);

    // GetSecretValue should work again
    let req = make_request("GetSecretValue", r#"{"SecretId": "deleteme"}"#);
    let resp = svc.handle(req).await.unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["SecretString"], "value");
}

#[tokio::test]
async fn test_list_secrets() {
    let state = make_state();
    let svc = SecretsManagerService::new(state);

    for name in &["alpha", "beta", "gamma"] {
        let req = make_request(
            "CreateSecret",
            &format!(r#"{{"Name": "{name}", "SecretString": "val"}}"#),
        );
        svc.handle(req).await.unwrap();
    }

    let req = make_request("ListSecrets", "{}");
    let resp = svc.handle(req).await.unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["SecretList"].as_array().unwrap().len(), 3);
}

#[tokio::test]
async fn test_tags() {
    let state = make_state();
    let svc = SecretsManagerService::new(state);

    let req = make_request(
        "CreateSecret",
        r#"{"Name": "tagged", "SecretString": "val"}"#,
    );
    svc.handle(req).await.unwrap();

    let req = make_request(
        "TagResource",
        r#"{"SecretId": "tagged", "Tags": [{"Key": "env", "Value": "prod"}]}"#,
    );
    svc.handle(req).await.unwrap();

    let req = make_request("DescribeSecret", r#"{"SecretId": "tagged"}"#);
    let resp = svc.handle(req).await.unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let tags = body["Tags"].as_array().unwrap();
    assert!(tags
        .iter()
        .any(|t| t["Key"] == "env" && t["Value"] == "prod"));

    let req = make_request(
        "UntagResource",
        r#"{"SecretId": "tagged", "TagKeys": ["env"]}"#,
    );
    svc.handle(req).await.unwrap();

    let req = make_request("DescribeSecret", r#"{"SecretId": "tagged"}"#);
    let resp = svc.handle(req).await.unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    // Tags should be empty list after untagging all (but key present since tags were set)
    assert_eq!(body["Tags"].as_array().unwrap().len(), 0);
}

#[tokio::test]
async fn test_get_random_password() {
    let state = make_state();
    let svc = SecretsManagerService::new(state);

    let req = make_request("GetRandomPassword", "{}");
    let resp = svc.handle(req).await.unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["RandomPassword"].as_str().unwrap().len(), 32);
}

#[tokio::test]
async fn test_replication_ops_return_arn() {
    let state = make_state();
    let svc = SecretsManagerService::new(state);

    let req = make_request(
        "CreateSecret",
        r#"{"Name": "repl-secret", "SecretString": "val"}"#,
    );
    let resp = svc.handle(req).await.unwrap();
    let create_body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let expected_arn = create_body["ARN"].as_str().unwrap();

    for action in &[
        "ReplicateSecretToRegions",
        "RemoveRegionsFromReplication",
        "StopReplicationToReplica",
    ] {
        let req = make_request(action, r#"{"SecretId": "repl-secret"}"#);
        let resp = svc.handle(req).await.unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(
            body["ARN"].as_str().unwrap(),
            expected_arn,
            "{action} should return the secret's actual ARN"
        );
    }
}

#[tokio::test]
async fn replicate_secret_to_regions_persists_replicas() {
    // ReplicateSecretToRegions was a no-op returning empty ReplicationStatus
    // (bug-audit 2026-06-20, 1.24): the added regions must persist and show up
    // in ReplicationStatus and DescribeSecret, and be removable.
    let state = make_state();
    let svc = SecretsManagerService::new(state);
    svc.handle(make_request(
        "CreateSecret",
        r#"{"Name": "repl", "SecretString": "v"}"#,
    ))
    .await
    .unwrap();

    let resp = svc
        .handle(make_request(
            "ReplicateSecretToRegions",
            r#"{"SecretId": "repl", "AddReplicaRegions": [{"Region": "us-west-2"}, {"Region": "eu-west-1"}]}"#,
        ))
        .await
        .unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let statuses = body["ReplicationStatus"].as_array().unwrap();
    assert_eq!(statuses.len(), 2, "{body}");
    assert!(statuses
        .iter()
        .any(|s| s["Region"] == "us-west-2" && s["Status"] == "InSync"));

    // DescribeSecret reflects the replicas.
    let d = svc
        .handle(make_request("DescribeSecret", r#"{"SecretId": "repl"}"#))
        .await
        .unwrap();
    let db: Value = serde_json::from_slice(d.body.expect_bytes()).unwrap();
    assert_eq!(db["ReplicationStatus"].as_array().unwrap().len(), 2);

    // Removing one region drops it.
    let r = svc
        .handle(make_request(
            "RemoveRegionsFromReplication",
            r#"{"SecretId": "repl", "RemoveReplicaRegions": ["us-west-2"]}"#,
        ))
        .await
        .unwrap();
    let rb: Value = serde_json::from_slice(r.body.expect_bytes()).unwrap();
    let left = rb["ReplicationStatus"].as_array().unwrap();
    assert_eq!(left.len(), 1);
    assert_eq!(left[0]["Region"], "eu-west-1");
}

#[tokio::test]
async fn test_secret_id_length_validation() {
    let state = make_state();
    let svc = SecretsManagerService::new(state);

    // SecretId too long (> 2048)
    let long_id = "x".repeat(2049);
    let req = make_request("GetSecretValue", &format!(r#"{{"SecretId": "{long_id}"}}"#));
    match svc.handle(req).await {
        Err(e) => assert!(e.to_string().contains("InvalidParameterException")),
        Ok(_) => panic!("expected InvalidParameterException"),
    }
}

#[tokio::test]
async fn test_name_length_validation() {
    let state = make_state();
    let svc = SecretsManagerService::new(state);

    // Name too long (> 512)
    let long_name = "x".repeat(513);
    let req = make_request(
        "CreateSecret",
        &format!(r#"{{"Name": "{long_name}", "SecretString": "val"}}"#),
    );
    match svc.handle(req).await {
        Err(e) => assert!(e.to_string().contains("InvalidParameterException")),
        Ok(_) => panic!("expected InvalidParameterException"),
    }
}

#[tokio::test]
async fn test_next_token_length_validation() {
    let state = make_state();
    let svc = SecretsManagerService::new(state);

    // NextToken too long (> 4096)
    let long_token = "x".repeat(4097);
    let req = make_request(
        "ListSecrets",
        &format!(r#"{{"NextToken": "{long_token}"}}"#),
    );
    match svc.handle(req).await {
        Err(e) => assert!(e.to_string().contains("InvalidParameterException")),
        Ok(_) => panic!("expected InvalidParameterException"),
    }
}

#[tokio::test]
async fn test_client_request_token_length_validation() {
    let state = make_state();
    let svc = SecretsManagerService::new(state);

    // ClientRequestToken too short (< 32)
    let req = make_request(
        "CreateSecret",
        r#"{"Name": "test", "SecretString": "val", "ClientRequestToken": "short"}"#,
    );
    match svc.handle(req).await {
        Err(e) => assert!(e.to_string().contains("InvalidParameterException")),
        Ok(_) => panic!("expected InvalidParameterException"),
    }
}

#[tokio::test]
async fn test_rotate_secret_with_lambda_creates_pending_version() {
    let state = make_state();
    let svc = SecretsManagerService::new(state.clone());

    // Create a secret
    let req = make_request(
        "CreateSecret",
        r#"{"Name": "rotate-me", "SecretString": "old-password"}"#,
    );
    svc.handle(req).await.unwrap();

    // Rotate with a Lambda ARN
    let token = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee";
    let body = serde_json::json!({
        "SecretId": "rotate-me",
        "RotationLambdaARN": "arn:aws:lambda:us-east-1:123456789012:function:rotator",
        "ClientRequestToken": token,
    });
    let req = make_request("RotateSecret", &body.to_string());
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::OK);
    let resp_body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(resp_body["VersionId"], token);

    // Real AWS leaves the AWSPENDING version creation to the rotation
    // Lambda's createSecret step, so we should NOT pre-create it. Verify
    // that no version with the rotation token exists yet.
    let _accts = state.read();
    let s = _accts.default_ref();
    let secret = s.secrets.get("rotate-me").unwrap();
    assert!(
        !secret.versions.contains_key(token),
        "AWSPENDING version must not be pre-created; the rotation Lambda creates it"
    );

    // Verify rotation config was set
    assert_eq!(
        secret.rotation_lambda_arn.as_deref(),
        Some("arn:aws:lambda:us-east-1:123456789012:function:rotator")
    );
    assert_eq!(secret.rotation_enabled, Some(true));
}

#[tokio::test]
async fn test_rotate_secret_without_lambda_promotes_directly() {
    let state = make_state();
    let svc = SecretsManagerService::new(state.clone());

    // Create a secret
    let req = make_request(
        "CreateSecret",
        r#"{"Name": "rotate-no-lambda", "SecretString": "value1"}"#,
    );
    svc.handle(req).await.unwrap();

    // Rotate without Lambda ARN
    let token = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee";
    let body = serde_json::json!({
        "SecretId": "rotate-no-lambda",
        "ClientRequestToken": token,
    });
    let req = make_request("RotateSecret", &body.to_string());
    svc.handle(req).await.unwrap();

    // Verify the new version is AWSCURRENT (no pending)
    let _accts = state.read();
    let s = _accts.default_ref();
    let secret = s.secrets.get("rotate-no-lambda").unwrap();
    let new_ver = secret.versions.get(token).unwrap();
    assert!(new_ver.stages.contains(&"AWSCURRENT".to_string()));
    assert_eq!(secret.current_version_id.as_deref(), Some(token));
}

#[tokio::test]
async fn test_rotate_secret_stores_rotation_config() {
    let state = make_state();
    let svc = SecretsManagerService::new(state.clone());

    let req = make_request(
        "CreateSecret",
        r#"{"Name": "rot-cfg", "SecretString": "pw"}"#,
    );
    svc.handle(req).await.unwrap();

    let token = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee";
    let body = serde_json::json!({
        "SecretId": "rot-cfg",
        "RotationLambdaARN": "arn:aws:lambda:us-east-1:123456789012:function:my-rotator",
        "RotationRules": { "AutomaticallyAfterDays": 30 },
        "ClientRequestToken": token,
    });
    let req = make_request("RotateSecret", &body.to_string());
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::OK);

    let _accts = state.read();
    let s = _accts.default_ref();
    let secret = s.secrets.get("rot-cfg").unwrap();
    assert_eq!(secret.rotation_enabled, Some(true));
    assert_eq!(
        secret.rotation_lambda_arn.as_deref(),
        Some("arn:aws:lambda:us-east-1:123456789012:function:my-rotator")
    );
    // A Lambda-driven rotation runs asynchronously and, with no delivery bus
    // wired up, never completes. LastRotatedDate must therefore stay unset --
    // RotateSecret must not falsely claim the value rotated before the Lambda
    // finished (the simple, no-Lambda path is covered separately).
    assert!(secret.last_rotated_at.is_none());
    let rules = secret.rotation_rules.as_ref().unwrap();
    assert_eq!(rules.automatically_after_days, Some(30));

    // The AWSPENDING version is created by the rotation Lambda's
    // createSecret step, not by RotateSecret itself, so verify that no
    // version with this token exists yet.
    assert!(!secret.versions.contains_key(token));
}

#[tokio::test]
async fn test_rotate_secret_version_stages_change() {
    let state = make_state();
    let svc = SecretsManagerService::new(state.clone());

    let req = make_request(
        "CreateSecret",
        r#"{"Name": "rot-stages", "SecretString": "original"}"#,
    );
    svc.handle(req).await.unwrap();

    // Get original version id
    let original_vid = {
        let _accts = state.read();
        let s = _accts.default_ref();
        let secret = s.secrets.get("rot-stages").unwrap();
        secret.current_version_id.clone().unwrap()
    };

    // Rotate without Lambda (simple rotation)
    let token = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee";
    let body = serde_json::json!({
        "SecretId": "rot-stages",
        "ClientRequestToken": token,
    });
    let req = make_request("RotateSecret", &body.to_string());
    svc.handle(req).await.unwrap();

    let _accts = state.read();
    let s = _accts.default_ref();
    let secret = s.secrets.get("rot-stages").unwrap();

    // New version should be AWSCURRENT
    let new_ver = secret.versions.get(token).unwrap();
    assert!(new_ver.stages.contains(&"AWSCURRENT".to_string()));

    // Old version should be AWSPREVIOUS
    let old_ver = secret.versions.get(&original_vid).unwrap();
    assert!(old_ver.stages.contains(&"AWSPREVIOUS".to_string()));
    assert!(!old_ver.stages.contains(&"AWSCURRENT".to_string()));
}

#[tokio::test]
async fn test_cancel_rotate_secret() {
    let state = make_state();
    let svc = SecretsManagerService::new(state.clone());

    let req = make_request(
        "CreateSecret",
        r#"{"Name": "cancel-rot", "SecretString": "pw"}"#,
    );
    svc.handle(req).await.unwrap();

    // Enable rotation first
    let token = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee";
    let body = serde_json::json!({
        "SecretId": "cancel-rot",
        "ClientRequestToken": token,
    });
    let req = make_request("RotateSecret", &body.to_string());
    svc.handle(req).await.unwrap();

    // Verify rotation is enabled
    {
        let _accts = state.read();
        let s = _accts.default_ref();
        let secret = s.secrets.get("cancel-rot").unwrap();
        assert_eq!(secret.rotation_enabled, Some(true));
    }

    // Cancel rotation
    let req = make_request("CancelRotateSecret", r#"{"SecretId": "cancel-rot"}"#);
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::OK);
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["Name"], "cancel-rot");

    // Verify rotation is disabled
    let _accts = state.read();
    let s = _accts.default_ref();
    let secret = s.secrets.get("cancel-rot").unwrap();
    assert_eq!(secret.rotation_enabled, Some(false));
}

#[tokio::test]
async fn test_cancel_rotate_secret_fails_when_not_enabled() {
    let state = make_state();
    let svc = SecretsManagerService::new(state);

    let req = make_request(
        "CreateSecret",
        r#"{"Name": "no-rot", "SecretString": "pw"}"#,
    );
    svc.handle(req).await.unwrap();

    let req = make_request("CancelRotateSecret", r#"{"SecretId": "no-rot"}"#);
    let result = svc.handle(req).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_batch_get_secret_value_multiple() {
    let state = make_state();
    let svc = SecretsManagerService::new(state);

    for (name, val) in &[("batch-a", "va"), ("batch-b", "vb"), ("batch-c", "vc")] {
        let req = make_request(
            "CreateSecret",
            &format!(r#"{{"Name": "{name}", "SecretString": "{val}"}}"#),
        );
        svc.handle(req).await.unwrap();
    }

    let body = serde_json::json!({
        "SecretIdList": ["batch-a", "batch-b", "batch-c"]
    });
    let req = make_request("BatchGetSecretValue", &body.to_string());
    let resp = svc.handle(req).await.unwrap();
    let resp_body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();

    let values = resp_body["SecretValues"].as_array().unwrap();
    assert_eq!(values.len(), 3);

    // Verify each secret has the right value
    let names: Vec<&str> = values.iter().map(|v| v["Name"].as_str().unwrap()).collect();
    assert!(names.contains(&"batch-a"));
    assert!(names.contains(&"batch-b"));
    assert!(names.contains(&"batch-c"));

    // Verify no errors
    assert!(resp_body.get("Errors").is_none());
}

#[tokio::test]
async fn test_batch_get_secret_value_with_missing() {
    let state = make_state();
    let svc = SecretsManagerService::new(state);

    let req = make_request(
        "CreateSecret",
        r#"{"Name": "exists", "SecretString": "val"}"#,
    );
    svc.handle(req).await.unwrap();

    let body = serde_json::json!({
        "SecretIdList": ["exists", "nonexistent"]
    });
    let req = make_request("BatchGetSecretValue", &body.to_string());
    let resp = svc.handle(req).await.unwrap();
    let resp_body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();

    let values = resp_body["SecretValues"].as_array().unwrap();
    assert_eq!(values.len(), 1);
    assert_eq!(values[0]["Name"], "exists");

    let errors = resp_body["Errors"].as_array().unwrap();
    assert_eq!(errors.len(), 1);
    assert_eq!(errors[0]["SecretId"], "nonexistent");
    assert_eq!(errors[0]["ErrorCode"], "ResourceNotFoundException");
}

#[tokio::test]
async fn test_update_secret_changes_description_and_kms() {
    let state = make_state();
    let svc = SecretsManagerService::new(state);

    let req = make_request(
        "CreateSecret",
        r#"{"Name": "updatable", "SecretString": "val", "Description": "old desc"}"#,
    );
    svc.handle(req).await.unwrap();

    // Update description and KmsKeyId
    let body = serde_json::json!({
        "SecretId": "updatable",
        "Description": "new desc",
        "KmsKeyId": "arn:aws:kms:us-east-1:123456789012:key/my-key"
    });
    let req = make_request("UpdateSecret", &body.to_string());
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::OK);
    let resp_body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(resp_body["Name"], "updatable");
    // No VersionId since no new value was provided
    assert!(resp_body.get("VersionId").is_none());

    // Describe to verify changes
    let req = make_request("DescribeSecret", r#"{"SecretId": "updatable"}"#);
    let resp = svc.handle(req).await.unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["Description"], "new desc");
    assert_eq!(
        body["KmsKeyId"],
        "arn:aws:kms:us-east-1:123456789012:key/my-key"
    );
}

#[tokio::test]
async fn test_update_secret_with_new_value() {
    let state = make_state();
    let svc = SecretsManagerService::new(state);

    let req = make_request(
        "CreateSecret",
        r#"{"Name": "upd-val", "SecretString": "old"}"#,
    );
    svc.handle(req).await.unwrap();

    // Update with a new value
    let body = serde_json::json!({
        "SecretId": "upd-val",
        "SecretString": "new-value"
    });
    let req = make_request("UpdateSecret", &body.to_string());
    let resp = svc.handle(req).await.unwrap();
    let resp_body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert!(resp_body["VersionId"].as_str().is_some());

    // Get should return new value
    let req = make_request("GetSecretValue", r#"{"SecretId": "upd-val"}"#);
    let resp = svc.handle(req).await.unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["SecretString"], "new-value");
}

#[tokio::test]
async fn test_get_random_password_custom_length() {
    let state = make_state();
    let svc = SecretsManagerService::new(state);

    let req = make_request("GetRandomPassword", r#"{"PasswordLength": 64}"#);
    let resp = svc.handle(req).await.unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["RandomPassword"].as_str().unwrap().len(), 64);
}

#[tokio::test]
async fn test_get_random_password_exclude_chars() {
    let state = make_state();
    let svc = SecretsManagerService::new(state);

    let req = make_request(
        "GetRandomPassword",
        r#"{"PasswordLength": 100, "ExcludeCharacters": "abc123"}"#,
    );
    let resp = svc.handle(req).await.unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let password = body["RandomPassword"].as_str().unwrap();
    assert_eq!(password.len(), 100);
    assert!(!password.contains('a'));
    assert!(!password.contains('b'));
    assert!(!password.contains('c'));
    assert!(!password.contains('1'));
    assert!(!password.contains('2'));
    assert!(!password.contains('3'));
}

#[tokio::test]
async fn test_get_random_password_exclude_types() {
    let state = make_state();
    let svc = SecretsManagerService::new(state);

    // Exclude everything except lowercase
    let body = serde_json::json!({
        "PasswordLength": 50,
        "ExcludeUppercase": true,
        "ExcludeNumbers": true,
        "ExcludePunctuation": true,
        "RequireEachIncludedType": false,
    });
    let req = make_request("GetRandomPassword", &body.to_string());
    let resp = svc.handle(req).await.unwrap();
    let resp_body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let password = resp_body["RandomPassword"].as_str().unwrap();
    assert_eq!(password.len(), 50);
    assert!(password.chars().all(|c| c.is_ascii_lowercase()));
}

#[tokio::test]
async fn test_get_random_password_too_short() {
    let state = make_state();
    let svc = SecretsManagerService::new(state);

    let req = make_request("GetRandomPassword", r#"{"PasswordLength": 3}"#);
    assert!(svc.handle(req).await.is_err());
}

#[tokio::test]
async fn test_get_random_password_too_long() {
    let state = make_state();
    let svc = SecretsManagerService::new(state);

    let req = make_request("GetRandomPassword", r#"{"PasswordLength": 4097}"#);
    let err = svc
        .handle(req)
        .await
        .err()
        .expect("over-long PasswordLength errors");
    // AWS SecretsManager uses InvalidParameterException, not InvalidParameterValue.
    assert_eq!(err.code(), "InvalidParameterException");
}

#[tokio::test]
async fn test_update_secret_version_stage_move_current() {
    let state = make_state();
    let svc = SecretsManagerService::new(state.clone());

    let req = make_request(
        "CreateSecret",
        r#"{"Name": "stage-test", "SecretString": "v1"}"#,
    );
    svc.handle(req).await.unwrap();

    // Put a second version
    let req = make_request(
        "PutSecretValue",
        r#"{"SecretId": "stage-test", "SecretString": "v2"}"#,
    );
    svc.handle(req).await.unwrap();

    // Get version IDs
    let (v1_id, v2_id) = {
        let _accts = state.read();
        let s = _accts.default_ref();
        let secret = s.secrets.get("stage-test").unwrap();
        let current = secret.current_version_id.clone().unwrap();
        let previous = secret
            .versions
            .iter()
            .find(|(id, _)| **id != current)
            .map(|(id, _)| id.clone())
            .unwrap();
        (previous, current)
    };

    // Move AWSCURRENT from v2 back to v1
    let body = serde_json::json!({
        "SecretId": "stage-test",
        "VersionStage": "AWSCURRENT",
        "MoveToVersionId": v1_id,
        "RemoveFromVersionId": v2_id,
    });
    let req = make_request("UpdateSecretVersionStage", &body.to_string());
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::OK);

    // Verify v1 is now AWSCURRENT
    let _accts = state.read();
    let s = _accts.default_ref();
    let secret = s.secrets.get("stage-test").unwrap();
    let v1 = secret.versions.get(&v1_id).unwrap();
    assert!(v1.stages.contains(&"AWSCURRENT".to_string()));

    // v2 should have AWSPREVIOUS
    let v2 = secret.versions.get(&v2_id).unwrap();
    assert!(v2.stages.contains(&"AWSPREVIOUS".to_string()));
    assert!(!v2.stages.contains(&"AWSCURRENT".to_string()));

    assert_eq!(secret.current_version_id.as_deref(), Some(v1_id.as_str()));
}

#[tokio::test]
async fn test_update_secret_version_stage_custom_label() {
    let state = make_state();
    let svc = SecretsManagerService::new(state.clone());

    let req = make_request(
        "CreateSecret",
        r#"{"Name": "custom-stage", "SecretString": "v1"}"#,
    );
    svc.handle(req).await.unwrap();

    let vid = {
        let _accts = state.read();
        let s = _accts.default_ref();
        s.secrets
            .get("custom-stage")
            .unwrap()
            .current_version_id
            .clone()
            .unwrap()
    };

    // Add a custom label
    let body = serde_json::json!({
        "SecretId": "custom-stage",
        "VersionStage": "MYAPP_LIVE",
        "MoveToVersionId": vid,
    });
    let req = make_request("UpdateSecretVersionStage", &body.to_string());
    svc.handle(req).await.unwrap();

    let _accts = state.read();
    let s = _accts.default_ref();
    let secret = s.secrets.get("custom-stage").unwrap();
    let ver = secret.versions.get(&vid).unwrap();
    assert!(ver.stages.contains(&"MYAPP_LIVE".to_string()));
    assert!(ver.stages.contains(&"AWSCURRENT".to_string()));
}

#[tokio::test]
async fn test_validate_resource_policy() {
    let state = make_state();
    let svc = SecretsManagerService::new(state);

    let policy = serde_json::json!({
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": {"AWS": "arn:aws:iam::123456789012:root"},
            "Action": "secretsmanager:GetSecretValue",
            "Resource": "*"
        }]
    });

    let body = serde_json::json!({
        "ResourcePolicy": policy.to_string(),
    });
    let req = make_request("ValidateResourcePolicy", &body.to_string());
    let resp = svc.handle(req).await.unwrap();
    let resp_body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(resp_body["PolicyValidationPassed"], true);
    assert_eq!(resp_body["ValidationErrors"].as_array().unwrap().len(), 0);
}

#[tokio::test]
async fn test_validate_resource_policy_requires_policy() {
    let state = make_state();
    let svc = SecretsManagerService::new(state);

    let req = make_request("ValidateResourcePolicy", r#"{}"#);
    assert!(svc.handle(req).await.is_err());
}

#[tokio::test]
async fn test_put_get_delete_resource_policy() {
    let state = make_state();
    let svc = SecretsManagerService::new(state);

    let req = make_request(
        "CreateSecret",
        r#"{"Name": "policy-secret", "SecretString": "val"}"#,
    );
    svc.handle(req).await.unwrap();

    // Get policy (should be empty initially)
    let req = make_request("GetResourcePolicy", r#"{"SecretId": "policy-secret"}"#);
    let resp = svc.handle(req).await.unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["Name"], "policy-secret");
    assert!(body.get("ResourcePolicy").is_none());

    // Put policy
    let policy = r#"{"Version":"2012-10-17","Statement":[]}"#;
    let put_body = serde_json::json!({
        "SecretId": "policy-secret",
        "ResourcePolicy": policy,
    });
    let req = make_request("PutResourcePolicy", &put_body.to_string());
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::OK);

    // Get policy (should have it now)
    let req = make_request("GetResourcePolicy", r#"{"SecretId": "policy-secret"}"#);
    let resp = svc.handle(req).await.unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["ResourcePolicy"], policy);

    // Delete policy
    let req = make_request("DeleteResourcePolicy", r#"{"SecretId": "policy-secret"}"#);
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::OK);

    // Get again (should be gone)
    let req = make_request("GetResourcePolicy", r#"{"SecretId": "policy-secret"}"#);
    let resp = svc.handle(req).await.unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert!(body.get("ResourcePolicy").is_none());
}

#[tokio::test]
async fn test_batch_get_secret_value_with_deleted() {
    let state = make_state();
    let svc = SecretsManagerService::new(state);

    let req = make_request(
        "CreateSecret",
        r#"{"Name": "batch-del", "SecretString": "val"}"#,
    );
    svc.handle(req).await.unwrap();

    // Soft-delete it
    let req = make_request("DeleteSecret", r#"{"SecretId": "batch-del"}"#);
    svc.handle(req).await.unwrap();

    let body = serde_json::json!({
        "SecretIdList": ["batch-del"]
    });
    let req = make_request("BatchGetSecretValue", &body.to_string());
    let resp = svc.handle(req).await.unwrap();
    let resp_body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();

    // Should have 0 values and 1 error
    assert_eq!(resp_body["SecretValues"].as_array().unwrap().len(), 0);
    let errors = resp_body["Errors"].as_array().unwrap();
    assert_eq!(errors.len(), 1);
    assert_eq!(errors[0]["ErrorCode"], "InvalidRequestException");
}

// ── CreateSecret idempotency ──

#[tokio::test]
async fn create_secret_idempotent_same_value() {
    let state = make_state();
    let svc = SecretsManagerService::new(state);

    let token = "a".repeat(32);
    let body = serde_json::json!({
        "Name": "idem",
        "SecretString": "val",
        "ClientRequestToken": token,
    });
    let req = make_request("CreateSecret", &body.to_string());
    svc.handle(req).await.unwrap();

    // Same token + same value -> success (idempotent)
    let req = make_request("CreateSecret", &body.to_string());
    let resp = svc.handle(req).await.unwrap();
    let b: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(b["Name"], "idem");
    assert_eq!(b["VersionId"], token);
}

#[tokio::test]
async fn create_secret_idempotent_conflict() {
    let state = make_state();
    let svc = SecretsManagerService::new(state);

    let token = "a".repeat(32);
    let body = serde_json::json!({
        "Name": "conflict",
        "SecretString": "val1",
        "ClientRequestToken": token,
    });
    let req = make_request("CreateSecret", &body.to_string());
    svc.handle(req).await.unwrap();

    // Same token + different value -> ResourceExistsException
    let body2 = serde_json::json!({
        "Name": "conflict",
        "SecretString": "val2",
        "ClientRequestToken": token,
    });
    let req = make_request("CreateSecret", &body2.to_string());
    let err = expect_err(svc.handle(req).await);
    assert!(err.to_string().contains("ResourceExistsException"));
}

#[tokio::test]
async fn create_secret_duplicate_name_no_token() {
    let state = make_state();
    let svc = SecretsManagerService::new(state);

    let req = make_request("CreateSecret", r#"{"Name": "dup", "SecretString": "v1"}"#);
    svc.handle(req).await.unwrap();

    let req = make_request("CreateSecret", r#"{"Name": "dup", "SecretString": "v2"}"#);
    let err = expect_err(svc.handle(req).await);
    assert!(err.to_string().contains("ResourceExistsException"));
}

#[tokio::test]
async fn create_secret_with_tags_and_description() {
    let state = make_state();
    let svc = SecretsManagerService::new(state);

    let body = serde_json::json!({
        "Name": "full-secret",
        "SecretString": "v",
        "Description": "my secret desc",
        "KmsKeyId": "alias/my-key",
        "Tags": [{"Key": "env", "Value": "staging"}],
    });
    let req = make_request("CreateSecret", &body.to_string());
    svc.handle(req).await.unwrap();

    let req = make_request("DescribeSecret", r#"{"SecretId": "full-secret"}"#);
    let resp = svc.handle(req).await.unwrap();
    let b: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(b["Description"], "my secret desc");
    assert_eq!(b["KmsKeyId"], "alias/my-key");
    assert_eq!(b["Tags"][0]["Key"], "env");
}

// ── PutSecretValue edge cases ──

#[tokio::test]
async fn put_secret_value_requires_value() {
    let state = make_state();
    let svc = SecretsManagerService::new(state);

    let req = make_request(
        "CreateSecret",
        r#"{"Name": "novalue", "SecretString": "v"}"#,
    );
    svc.handle(req).await.unwrap();

    let req = make_request("PutSecretValue", r#"{"SecretId": "novalue"}"#);
    let err = expect_err(svc.handle(req).await);
    assert!(err.to_string().contains("InvalidRequestException"));
}

#[tokio::test]
async fn put_secret_value_not_found() {
    let state = make_state();
    let svc = SecretsManagerService::new(state);

    let req = make_request(
        "PutSecretValue",
        r#"{"SecretId": "ghost", "SecretString": "v"}"#,
    );
    let err = expect_err(svc.handle(req).await);
    assert!(err.to_string().contains("ResourceNotFoundException"));
}

#[tokio::test]
async fn put_secret_value_on_deleted_secret() {
    let state = make_state();
    let svc = SecretsManagerService::new(state);

    let req = make_request(
        "CreateSecret",
        r#"{"Name": "del-put", "SecretString": "v"}"#,
    );
    svc.handle(req).await.unwrap();
    let req = make_request("DeleteSecret", r#"{"SecretId": "del-put"}"#);
    svc.handle(req).await.unwrap();

    let req = make_request(
        "PutSecretValue",
        r#"{"SecretId": "del-put", "SecretString": "v2"}"#,
    );
    let err = expect_err(svc.handle(req).await);
    assert!(err.to_string().contains("InvalidRequestException"));
}

#[tokio::test]
async fn put_secret_value_idempotent_match() {
    let state = make_state();
    let svc = SecretsManagerService::new(state);

    let req = make_request(
        "CreateSecret",
        r#"{"Name": "put-idem", "SecretString": "original"}"#,
    );
    svc.handle(req).await.unwrap();

    let token = "b".repeat(32);
    let body = serde_json::json!({
        "SecretId": "put-idem",
        "SecretString": "new-val",
        "ClientRequestToken": token,
    });
    let req = make_request("PutSecretValue", &body.to_string());
    svc.handle(req).await.unwrap();

    // Same token + same value -> idempotent success
    let req = make_request("PutSecretValue", &body.to_string());
    let resp = svc.handle(req).await.unwrap();
    let b: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(b["VersionId"], token);
}

#[tokio::test]
async fn put_secret_value_idempotent_conflict() {
    let state = make_state();
    let svc = SecretsManagerService::new(state);

    let req = make_request(
        "CreateSecret",
        r#"{"Name": "put-conflict", "SecretString": "original"}"#,
    );
    svc.handle(req).await.unwrap();

    let token = "c".repeat(32);
    let body = serde_json::json!({
        "SecretId": "put-conflict",
        "SecretString": "val-a",
        "ClientRequestToken": token,
    });
    let req = make_request("PutSecretValue", &body.to_string());
    svc.handle(req).await.unwrap();

    // Same token + different value -> conflict
    let body2 = serde_json::json!({
        "SecretId": "put-conflict",
        "SecretString": "val-b",
        "ClientRequestToken": token,
    });
    let req = make_request("PutSecretValue", &body2.to_string());
    let err = expect_err(svc.handle(req).await);
    assert!(err.to_string().contains("ResourceExistsException"));
}

#[tokio::test]
async fn put_secret_value_with_custom_stages() {
    let state = make_state();
    let svc = SecretsManagerService::new(state.clone());

    let req = make_request(
        "CreateSecret",
        r#"{"Name": "staged", "SecretString": "v1"}"#,
    );
    svc.handle(req).await.unwrap();

    let body = serde_json::json!({
        "SecretId": "staged",
        "SecretString": "v2",
        "VersionStages": ["AWSCURRENT", "MYAPP_V2"],
    });
    let req = make_request("PutSecretValue", &body.to_string());
    let resp = svc.handle(req).await.unwrap();
    let b: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let stages = b["VersionStages"].as_array().unwrap();
    assert!(stages.iter().any(|s| s == "MYAPP_V2"));
}

// ── UpdateSecret edge cases ──

#[tokio::test]
async fn update_secret_not_found() {
    let state = make_state();
    let svc = SecretsManagerService::new(state);

    let body = serde_json::json!({
        "SecretId": "ghost",
        "Description": "new",
    });
    let req = make_request("UpdateSecret", &body.to_string());
    let err = expect_err(svc.handle(req).await);
    assert!(err.to_string().contains("ResourceNotFoundException"));
}

#[tokio::test]
async fn update_secret_on_deleted() {
    let state = make_state();
    let svc = SecretsManagerService::new(state);

    let req = make_request(
        "CreateSecret",
        r#"{"Name": "upd-del", "SecretString": "v"}"#,
    );
    svc.handle(req).await.unwrap();
    let req = make_request("DeleteSecret", r#"{"SecretId": "upd-del"}"#);
    svc.handle(req).await.unwrap();

    let body = serde_json::json!({
        "SecretId": "upd-del",
        "Description": "new",
    });
    let req = make_request("UpdateSecret", &body.to_string());
    let err = expect_err(svc.handle(req).await);
    assert!(err.to_string().contains("InvalidRequestException"));
}

#[tokio::test]
async fn update_secret_idempotent_match() {
    let state = make_state();
    let svc = SecretsManagerService::new(state);

    let req = make_request(
        "CreateSecret",
        r#"{"Name": "upd-idem", "SecretString": "orig"}"#,
    );
    svc.handle(req).await.unwrap();

    let token = "d".repeat(32);
    let body = serde_json::json!({
        "SecretId": "upd-idem",
        "SecretString": "new-val",
        "ClientRequestToken": token,
    });
    let req = make_request("UpdateSecret", &body.to_string());
    svc.handle(req).await.unwrap();

    // Repeat -> idempotent
    let req = make_request("UpdateSecret", &body.to_string());
    let resp = svc.handle(req).await.unwrap();
    let b: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(b["VersionId"], token);
}

// ── DeleteSecret edge cases ──

#[tokio::test]
async fn delete_secret_force() {
    let state = make_state();
    let svc = SecretsManagerService::new(state.clone());

    let req = make_request(
        "CreateSecret",
        r#"{"Name": "force-del", "SecretString": "v"}"#,
    );
    svc.handle(req).await.unwrap();

    let body = serde_json::json!({
        "SecretId": "force-del",
        "ForceDeleteWithoutRecovery": true,
    });
    let req = make_request("DeleteSecret", &body.to_string());
    let resp = svc.handle(req).await.unwrap();
    let b: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(b["Name"], "force-del");

    // Secret should be gone entirely
    let _accts = state.read();
    let s = _accts.default_ref();
    assert!(!s.secrets.contains_key("force-del"));
}

#[tokio::test]
async fn delete_secret_force_nonexistent() {
    let state = make_state();
    let svc = SecretsManagerService::new(state);

    let body = serde_json::json!({
        "SecretId": "not-here",
        "ForceDeleteWithoutRecovery": true,
    });
    let req = make_request("DeleteSecret", &body.to_string());
    let resp = svc.handle(req).await.unwrap();
    let b: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(b["Name"], "not-here");
}

#[tokio::test]
async fn delete_secret_recovery_window() {
    let state = make_state();
    let svc = SecretsManagerService::new(state);

    let req = make_request(
        "CreateSecret",
        r#"{"Name": "rec-win", "SecretString": "v"}"#,
    );
    svc.handle(req).await.unwrap();

    let body = serde_json::json!({
        "SecretId": "rec-win",
        "RecoveryWindowInDays": 7,
    });
    let req = make_request("DeleteSecret", &body.to_string());
    let resp = svc.handle(req).await.unwrap();
    let b: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert!(b["DeletionDate"].as_f64().is_some());
}

#[tokio::test]
async fn delete_secret_invalid_recovery_window() {
    let state = make_state();
    let svc = SecretsManagerService::new(state);

    let req = make_request(
        "CreateSecret",
        r#"{"Name": "bad-win", "SecretString": "v"}"#,
    );
    svc.handle(req).await.unwrap();

    // Too short
    let body = serde_json::json!({
        "SecretId": "bad-win",
        "RecoveryWindowInDays": 3,
    });
    let req = make_request("DeleteSecret", &body.to_string());
    let err = expect_err(svc.handle(req).await);
    assert!(err.to_string().contains("InvalidParameterException"));

    // Too long
    let body = serde_json::json!({
        "SecretId": "bad-win",
        "RecoveryWindowInDays": 31,
    });
    let req = make_request("DeleteSecret", &body.to_string());
    let err = expect_err(svc.handle(req).await);
    assert!(err.to_string().contains("InvalidParameterException"));
}

#[tokio::test]
async fn delete_secret_force_and_recovery_conflict() {
    let state = make_state();
    let svc = SecretsManagerService::new(state);

    let req = make_request("CreateSecret", r#"{"Name": "both", "SecretString": "v"}"#);
    svc.handle(req).await.unwrap();

    let body = serde_json::json!({
        "SecretId": "both",
        "ForceDeleteWithoutRecovery": true,
        "RecoveryWindowInDays": 7,
    });
    let req = make_request("DeleteSecret", &body.to_string());
    let err = expect_err(svc.handle(req).await);
    assert!(err.to_string().contains("InvalidParameterException"));
}

#[tokio::test]
async fn delete_already_deleted_secret() {
    let state = make_state();
    let svc = SecretsManagerService::new(state);

    let req = make_request(
        "CreateSecret",
        r#"{"Name": "dbl-del", "SecretString": "v"}"#,
    );
    svc.handle(req).await.unwrap();

    let req = make_request("DeleteSecret", r#"{"SecretId": "dbl-del"}"#);
    svc.handle(req).await.unwrap();

    let req = make_request("DeleteSecret", r#"{"SecretId": "dbl-del"}"#);
    let err = expect_err(svc.handle(req).await);
    assert!(err.to_string().contains("InvalidRequestException"));
}

// ── GetSecretValue edge cases ──

#[tokio::test]
async fn get_secret_value_by_version_id() {
    let state = make_state();
    let svc = SecretsManagerService::new(state.clone());

    let req = make_request(
        "CreateSecret",
        r#"{"Name": "ver-get", "SecretString": "v1"}"#,
    );
    svc.handle(req).await.unwrap();

    let v1_id = {
        let _accts = state.read();
        let s = _accts.default_ref();
        s.secrets
            .get("ver-get")
            .unwrap()
            .current_version_id
            .clone()
            .unwrap()
    };

    let req = make_request(
        "PutSecretValue",
        r#"{"SecretId": "ver-get", "SecretString": "v2"}"#,
    );
    svc.handle(req).await.unwrap();

    // Get old version by ID
    let body = serde_json::json!({
        "SecretId": "ver-get",
        "VersionId": v1_id,
        "VersionStage": "AWSPREVIOUS",
    });
    let req = make_request("GetSecretValue", &body.to_string());
    let resp = svc.handle(req).await.unwrap();
    let b: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(b["SecretString"], "v1");
}

#[tokio::test]
async fn get_secret_value_version_stage_mismatch() {
    let state = make_state();
    let svc = SecretsManagerService::new(state.clone());

    let req = make_request(
        "CreateSecret",
        r#"{"Name": "mismatch", "SecretString": "v1"}"#,
    );
    svc.handle(req).await.unwrap();

    let vid = {
        let _accts = state.read();
        let s = _accts.default_ref();
        s.secrets
            .get("mismatch")
            .unwrap()
            .current_version_id
            .clone()
            .unwrap()
    };

    // Request with VersionId but wrong stage
    let body = serde_json::json!({
        "SecretId": "mismatch",
        "VersionId": vid,
        "VersionStage": "AWSPREVIOUS",
    });
    let req = make_request("GetSecretValue", &body.to_string());
    let err = expect_err(svc.handle(req).await);
    assert!(err.to_string().contains("ResourceNotFoundException"));
}

#[tokio::test]
async fn get_secret_value_not_found() {
    let state = make_state();
    let svc = SecretsManagerService::new(state);

    let req = make_request("GetSecretValue", r#"{"SecretId": "nope"}"#);
    let err = expect_err(svc.handle(req).await);
    assert!(err.to_string().contains("ResourceNotFoundException"));
}

#[tokio::test]
async fn get_secret_value_no_versions() {
    let state = make_state();
    let svc = SecretsManagerService::new(state);

    let req = make_request("CreateSecret", r#"{"Name": "empty-ver"}"#);
    svc.handle(req).await.unwrap();

    let req = make_request("GetSecretValue", r#"{"SecretId": "empty-ver"}"#);
    let err = expect_err(svc.handle(req).await);
    assert!(err.to_string().contains("ResourceNotFoundException"));
}

#[tokio::test]
async fn get_secret_value_with_binary() {
    let state = make_state();
    let svc = SecretsManagerService::new(state);

    // SecretBinary is base64 encoded
    let body = serde_json::json!({
        "Name": "bin-secret",
        "SecretBinary": "SGVsbG8=",  // "Hello" in base64
    });
    let req = make_request("CreateSecret", &body.to_string());
    svc.handle(req).await.unwrap();

    let req = make_request("GetSecretValue", r#"{"SecretId": "bin-secret"}"#);
    let resp = svc.handle(req).await.unwrap();
    let b: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert!(b.get("SecretBinary").is_some());
    assert!(b.get("SecretString").is_none());
}

// ── ListSecrets with filters ──

#[tokio::test]
async fn list_secrets_filter_by_name() {
    let state = make_state();
    let svc = SecretsManagerService::new(state);

    for name in &["prod/db", "prod/api", "staging/db"] {
        let body = serde_json::json!({"Name": name, "SecretString": "v"});
        let req = make_request("CreateSecret", &body.to_string());
        svc.handle(req).await.unwrap();
    }

    let body = serde_json::json!({
        "Filters": [{"Key": "name", "Values": ["prod/"]}]
    });
    let req = make_request("ListSecrets", &body.to_string());
    let resp = svc.handle(req).await.unwrap();
    let b: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(b["SecretList"].as_array().unwrap().len(), 2);
}

#[tokio::test]
async fn list_secrets_filter_by_tag_key() {
    let state = make_state();
    let svc = SecretsManagerService::new(state);

    let body = serde_json::json!({
        "Name": "tagged-s",
        "SecretString": "v",
        "Tags": [{"Key": "team", "Value": "backend"}],
    });
    let req = make_request("CreateSecret", &body.to_string());
    svc.handle(req).await.unwrap();

    let body = serde_json::json!({"Name": "untagged-s", "SecretString": "v"});
    let req = make_request("CreateSecret", &body.to_string());
    svc.handle(req).await.unwrap();

    let body = serde_json::json!({
        "Filters": [{"Key": "tag-key", "Values": ["team"]}]
    });
    let req = make_request("ListSecrets", &body.to_string());
    let resp = svc.handle(req).await.unwrap();
    let b: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(b["SecretList"].as_array().unwrap().len(), 1);
    assert_eq!(b["SecretList"][0]["Name"], "tagged-s");
}

#[tokio::test]
async fn list_secrets_filter_by_description() {
    let state = make_state();
    let svc = SecretsManagerService::new(state);

    let body = serde_json::json!({
        "Name": "desc-match",
        "SecretString": "v",
        "Description": "Database credentials for production",
    });
    let req = make_request("CreateSecret", &body.to_string());
    svc.handle(req).await.unwrap();

    let body = serde_json::json!({"Name": "no-desc", "SecretString": "v"});
    let req = make_request("CreateSecret", &body.to_string());
    svc.handle(req).await.unwrap();

    let body = serde_json::json!({
        "Filters": [{"Key": "description", "Values": ["Database"]}]
    });
    let req = make_request("ListSecrets", &body.to_string());
    let resp = svc.handle(req).await.unwrap();
    let b: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(b["SecretList"].as_array().unwrap().len(), 1);
}

#[tokio::test]
async fn list_secrets_include_planned_deletion() {
    let state = make_state();
    let svc = SecretsManagerService::new(state);

    let req = make_request("CreateSecret", r#"{"Name": "alive", "SecretString": "v"}"#);
    svc.handle(req).await.unwrap();

    let req = make_request("CreateSecret", r#"{"Name": "doomed", "SecretString": "v"}"#);
    svc.handle(req).await.unwrap();
    let req = make_request("DeleteSecret", r#"{"SecretId": "doomed"}"#);
    svc.handle(req).await.unwrap();

    // Without IncludePlannedDeletion
    let req = make_request("ListSecrets", "{}");
    let resp = svc.handle(req).await.unwrap();
    let b: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(b["SecretList"].as_array().unwrap().len(), 1);

    // With IncludePlannedDeletion
    let body = serde_json::json!({"IncludePlannedDeletion": true});
    let req = make_request("ListSecrets", &body.to_string());
    let resp = svc.handle(req).await.unwrap();
    let b: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(b["SecretList"].as_array().unwrap().len(), 2);
}

#[tokio::test]
async fn list_secrets_pagination() {
    let state = make_state();
    let svc = SecretsManagerService::new(state);

    for i in 0..5 {
        let body = serde_json::json!({
            "Name": format!("page-{i}"),
            "SecretString": "v",
        });
        let req = make_request("CreateSecret", &body.to_string());
        svc.handle(req).await.unwrap();
    }

    let body = serde_json::json!({"MaxResults": 2});
    let req = make_request("ListSecrets", &body.to_string());
    let resp = svc.handle(req).await.unwrap();
    let b: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(b["SecretList"].as_array().unwrap().len(), 2);
    assert!(b["NextToken"].as_str().is_some());
}

#[tokio::test]
async fn list_secrets_invalid_filter_key() {
    let state = make_state();
    let svc = SecretsManagerService::new(state);

    let body = serde_json::json!({
        "Filters": [{"Key": "bogus", "Values": ["x"]}]
    });
    let req = make_request("ListSecrets", &body.to_string());
    let err = expect_err(svc.handle(req).await);
    assert!(err.to_string().contains("InvalidParameterException"));
}

#[tokio::test]
async fn list_secrets_empty_filter_values() {
    let state = make_state();
    let svc = SecretsManagerService::new(state);

    let body = serde_json::json!({
        "Filters": [{"Key": "name", "Values": []}]
    });
    let req = make_request("ListSecrets", &body.to_string());
    let err = expect_err(svc.handle(req).await);
    assert!(err.to_string().contains("InvalidParameterException"));
}

// ── ListSecretVersionIds ──

#[tokio::test]
async fn list_secret_version_ids() {
    let state = make_state();
    let svc = SecretsManagerService::new(state);

    let req = make_request(
        "CreateSecret",
        r#"{"Name": "multi-ver", "SecretString": "v1"}"#,
    );
    svc.handle(req).await.unwrap();

    let req = make_request(
        "PutSecretValue",
        r#"{"SecretId": "multi-ver", "SecretString": "v2"}"#,
    );
    svc.handle(req).await.unwrap();

    let req = make_request("ListSecretVersionIds", r#"{"SecretId": "multi-ver"}"#);
    let resp = svc.handle(req).await.unwrap();
    let b: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(b["Name"], "multi-ver");
    assert_eq!(b["Versions"].as_array().unwrap().len(), 2);
}

#[tokio::test]
async fn list_secret_version_ids_paginates_by_token() {
    // MaxResults/NextToken were ignored: every version returned at once with
    // no token (bug-audit 2026-06-20, 1.14).
    let state = make_state();
    let svc = SecretsManagerService::new(state);

    svc.handle(make_request(
        "CreateSecret",
        r#"{"Name": "pager", "SecretString": "v1"}"#,
    ))
    .await
    .unwrap();
    svc.handle(make_request(
        "PutSecretValue",
        r#"{"SecretId": "pager", "SecretString": "v2"}"#,
    ))
    .await
    .unwrap();

    // Two staged versions (AWSCURRENT + AWSPREVIOUS); page 1 of 1 carries a
    // NextToken.
    let resp = svc
        .handle(make_request(
            "ListSecretVersionIds",
            r#"{"SecretId": "pager", "MaxResults": 1}"#,
        ))
        .await
        .unwrap();
    let b: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(b["Versions"].as_array().unwrap().len(), 1);
    let token = b["NextToken"]
        .as_str()
        .expect("NextToken present")
        .to_string();

    // Resume: remaining version, no NextToken.
    let body = format!(r#"{{"SecretId": "pager", "MaxResults": 1, "NextToken": "{token}"}}"#);
    let resp = svc
        .handle(make_request("ListSecretVersionIds", &body))
        .await
        .unwrap();
    let b: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(b["Versions"].as_array().unwrap().len(), 1);
    assert!(b.get("NextToken").is_none());
}

// ── DescribeSecret with rotation info ──

#[tokio::test]
async fn describe_secret_with_rotation_and_next_date() {
    let state = make_state();
    let svc = SecretsManagerService::new(state);

    let req = make_request(
        "CreateSecret",
        r#"{"Name": "rot-desc", "SecretString": "pw"}"#,
    );
    svc.handle(req).await.unwrap();

    let token = "e".repeat(32);
    let body = serde_json::json!({
        "SecretId": "rot-desc",
        "RotationRules": {"AutomaticallyAfterDays": 14},
        "ClientRequestToken": token,
    });
    let req = make_request("RotateSecret", &body.to_string());
    svc.handle(req).await.unwrap();

    let req = make_request("DescribeSecret", r#"{"SecretId": "rot-desc"}"#);
    let resp = svc.handle(req).await.unwrap();
    let b: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(b["RotationEnabled"], true);
    assert!(b["LastRotatedDate"].as_f64().is_some());
    assert!(b["NextRotationDate"].as_f64().is_some());
    assert_eq!(b["RotationRules"]["AutomaticallyAfterDays"], 14);
}

#[tokio::test]
async fn describe_secret_deleted_shows_deletion_date() {
    let state = make_state();
    let svc = SecretsManagerService::new(state);

    let req = make_request(
        "CreateSecret",
        r#"{"Name": "del-desc", "SecretString": "v"}"#,
    );
    svc.handle(req).await.unwrap();
    let req = make_request("DeleteSecret", r#"{"SecretId": "del-desc"}"#);
    svc.handle(req).await.unwrap();

    let req = make_request("DescribeSecret", r#"{"SecretId": "del-desc"}"#);
    let resp = svc.handle(req).await.unwrap();
    let b: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert!(b["DeletedDate"].as_f64().is_some());
}

// ── BatchGetSecretValue edge cases ──

#[tokio::test]
async fn batch_get_secret_value_both_list_and_filters() {
    let state = make_state();
    let svc = SecretsManagerService::new(state);

    let body = serde_json::json!({
        "SecretIdList": ["a"],
        "Filters": [{"Key": "name", "Values": ["a"]}],
    });
    let req = make_request("BatchGetSecretValue", &body.to_string());
    let err = expect_err(svc.handle(req).await);
    assert!(err.to_string().contains("InvalidParameterException"));
}

#[tokio::test]
async fn batch_get_secret_value_max_results_without_filters() {
    let state = make_state();
    let svc = SecretsManagerService::new(state);

    let body = serde_json::json!({
        "SecretIdList": ["a"],
        "MaxResults": 10,
    });
    let req = make_request("BatchGetSecretValue", &body.to_string());
    let err = expect_err(svc.handle(req).await);
    assert!(err.to_string().contains("InvalidParameterException"));
}

#[tokio::test]
async fn batch_get_secret_value_with_filters() {
    let state = make_state();
    let svc = SecretsManagerService::new(state);

    for name in &["batch-f-a", "batch-f-b", "other-c"] {
        let body = serde_json::json!({"Name": name, "SecretString": "v"});
        let req = make_request("CreateSecret", &body.to_string());
        svc.handle(req).await.unwrap();
    }

    let body = serde_json::json!({
        "Filters": [{"Key": "name", "Values": ["batch-f"]}],
    });
    let req = make_request("BatchGetSecretValue", &body.to_string());
    let resp = svc.handle(req).await.unwrap();
    let b: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(b["SecretValues"].as_array().unwrap().len(), 2);
}

#[tokio::test]
async fn batch_get_secret_value_filters_paginate() {
    let state = make_state();
    let svc = SecretsManagerService::new(state);

    // Five matching secrets; MaxResults=2 forces three pages.
    for i in 0..5 {
        let body = serde_json::json!({"Name": format!("batch-page-{i}"), "SecretString": "v"});
        let req = make_request("CreateSecret", &body.to_string());
        svc.handle(req).await.unwrap();
    }

    let mut seen = std::collections::BTreeSet::new();
    let mut token: Option<String> = None;
    let mut pages = 0;
    loop {
        let mut body = serde_json::json!({
            "Filters": [{"Key": "name", "Values": ["batch-page"]}],
            "MaxResults": 2,
        });
        if let Some(t) = &token {
            body["NextToken"] = serde_json::json!(t);
        }
        let req = make_request("BatchGetSecretValue", &body.to_string());
        let resp = svc.handle(req).await.unwrap();
        let b: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        for v in b["SecretValues"].as_array().unwrap() {
            seen.insert(v["Name"].as_str().unwrap().to_string());
        }
        pages += 1;
        match b["NextToken"].as_str() {
            Some(t) => token = Some(t.to_string()),
            None => break,
        }
        assert!(pages < 10, "pagination did not terminate");
    }
    // All five reachable across pages, not just the first page of 2.
    assert_eq!(seen.len(), 5);
    assert_eq!(pages, 3);
}

#[tokio::test]
async fn list_secrets_stale_token_ends_listing() {
    let state = make_state();
    let svc = SecretsManagerService::new(state);

    for i in 0..4 {
        let body = serde_json::json!({"Name": format!("stale-{i}"), "SecretString": "v"});
        let req = make_request("CreateSecret", &body.to_string());
        svc.handle(req).await.unwrap();
    }

    // First page of 2 yields a NextToken (the name of the next secret).
    let body = serde_json::json!({"MaxResults": 2});
    let req = make_request("ListSecrets", &body.to_string());
    let resp = svc.handle(req).await.unwrap();
    let b: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let token = b["NextToken"].as_str().unwrap().to_string();

    // Delete the token's secret, then resume: the token no longer resolves.
    // Listing must END (empty page, no token) instead of restarting at page 1.
    let del = serde_json::json!({"SecretId": token, "ForceDeleteWithoutRecovery": true});
    let req = make_request("DeleteSecret", &del.to_string());
    svc.handle(req).await.unwrap();

    let body = serde_json::json!({"MaxResults": 2, "NextToken": token});
    let req = make_request("ListSecrets", &body.to_string());
    let resp = svc.handle(req).await.unwrap();
    let b: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(b["SecretList"].as_array().unwrap().len(), 0);
    assert!(b["NextToken"].is_null());
}

// ── RotateSecret validation ──

#[tokio::test]
async fn rotate_secret_invalid_token_length() {
    let state = make_state();
    let svc = SecretsManagerService::new(state);

    let req = make_request(
        "CreateSecret",
        r#"{"Name": "rot-val", "SecretString": "v"}"#,
    );
    svc.handle(req).await.unwrap();

    let body = serde_json::json!({
        "SecretId": "rot-val",
        "ClientRequestToken": "short",
    });
    let req = make_request("RotateSecret", &body.to_string());
    let err = expect_err(svc.handle(req).await);
    assert!(err.to_string().contains("InvalidParameterException"));
}

#[tokio::test]
async fn rotate_secret_invalid_rules() {
    let state = make_state();
    let svc = SecretsManagerService::new(state);

    let req = make_request(
        "CreateSecret",
        r#"{"Name": "rot-rules", "SecretString": "v"}"#,
    );
    svc.handle(req).await.unwrap();

    let body = serde_json::json!({
        "SecretId": "rot-rules",
        "RotationRules": {"AutomaticallyAfterDays": 0},
    });
    let req = make_request("RotateSecret", &body.to_string());
    let err = expect_err(svc.handle(req).await);
    assert!(err.to_string().contains("InvalidParameterException"));
}

#[tokio::test]
async fn rotate_secret_on_deleted() {
    let state = make_state();
    let svc = SecretsManagerService::new(state);

    let req = make_request(
        "CreateSecret",
        r#"{"Name": "rot-del", "SecretString": "v"}"#,
    );
    svc.handle(req).await.unwrap();
    let req = make_request("DeleteSecret", r#"{"SecretId": "rot-del"}"#);
    svc.handle(req).await.unwrap();

    let body = serde_json::json!({"SecretId": "rot-del"});
    let req = make_request("RotateSecret", &body.to_string());
    let err = expect_err(svc.handle(req).await);
    assert!(err.to_string().contains("InvalidRequestException"));
}

// ── CancelRotateSecret on deleted ──

#[tokio::test]
async fn cancel_rotate_on_deleted() {
    let state = make_state();
    let svc = SecretsManagerService::new(state);

    let req = make_request("CreateSecret", r#"{"Name": "cr-del", "SecretString": "v"}"#);
    svc.handle(req).await.unwrap();
    let req = make_request("DeleteSecret", r#"{"SecretId": "cr-del"}"#);
    svc.handle(req).await.unwrap();

    let req = make_request("CancelRotateSecret", r#"{"SecretId": "cr-del"}"#);
    let err = expect_err(svc.handle(req).await);
    assert!(err.to_string().contains("InvalidRequestException"));
}

// ── UpdateSecretVersionStage edge cases ──

#[tokio::test]
async fn update_version_stage_missing_remove_from() {
    let state = make_state();
    let svc = SecretsManagerService::new(state.clone());

    let req = make_request(
        "CreateSecret",
        r#"{"Name": "stage-err", "SecretString": "v1"}"#,
    );
    svc.handle(req).await.unwrap();

    let req = make_request(
        "PutSecretValue",
        r#"{"SecretId": "stage-err", "SecretString": "v2"}"#,
    );
    svc.handle(req).await.unwrap();

    let new_vid = {
        let _accts = state.read();
        let s = _accts.default_ref();
        let secret = s.secrets.get("stage-err").unwrap();
        secret
            .versions
            .iter()
            .find(|(_, v)| v.stages.contains(&"AWSPREVIOUS".to_string()))
            .map(|(id, _)| id.clone())
            .unwrap()
    };

    // Move AWSCURRENT without RemoveFromVersionId -> error
    let body = serde_json::json!({
        "SecretId": "stage-err",
        "VersionStage": "AWSCURRENT",
        "MoveToVersionId": new_vid,
    });
    let req = make_request("UpdateSecretVersionStage", &body.to_string());
    let err = expect_err(svc.handle(req).await);
    assert!(err.to_string().contains("InvalidParameterException"));
}

// ── Find secret by ARN ──

#[tokio::test]
async fn find_secret_by_arn() {
    let state = make_state();
    let svc = SecretsManagerService::new(state);

    let req = make_request(
        "CreateSecret",
        r#"{"Name": "arn-lookup", "SecretString": "v"}"#,
    );
    let resp = svc.handle(req).await.unwrap();
    let b: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let arn = b["ARN"].as_str().unwrap();

    // Lookup by full ARN
    let body = serde_json::json!({"SecretId": arn});
    let req = make_request("GetSecretValue", &body.to_string());
    let resp = svc.handle(req).await.unwrap();
    let b: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(b["SecretString"], "v");
}

#[tokio::test]
async fn find_secret_by_partial_arn() {
    let state = make_state();
    let svc = SecretsManagerService::new(state);

    let req = make_request(
        "CreateSecret",
        r#"{"Name": "partial-arn", "SecretString": "v"}"#,
    );
    svc.handle(req).await.unwrap();

    // Partial ARN (without the random suffix)
    let partial = "arn:aws:secretsmanager:us-east-1:123456789012:secret:partial-arn";
    let body = serde_json::json!({"SecretId": partial});
    let req = make_request("GetSecretValue", &body.to_string());
    let resp = svc.handle(req).await.unwrap();
    let b: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(b["SecretString"], "v");
}

// ── ValidateResourcePolicy edge cases ──

#[tokio::test]
async fn validate_resource_policy_with_secret_id() {
    let state = make_state();
    let svc = SecretsManagerService::new(state);

    let req = make_request(
        "CreateSecret",
        r#"{"Name": "pol-val", "SecretString": "v"}"#,
    );
    svc.handle(req).await.unwrap();

    let body = serde_json::json!({
        "SecretId": "pol-val",
        "ResourcePolicy": r#"{"Version":"2012-10-17","Statement":[]}"#,
    });
    let req = make_request("ValidateResourcePolicy", &body.to_string());
    let resp = svc.handle(req).await.unwrap();
    let b: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(b["PolicyValidationPassed"], true);
}

#[tokio::test]
async fn validate_resource_policy_nonexistent_secret() {
    let state = make_state();
    let svc = SecretsManagerService::new(state);

    let body = serde_json::json!({
        "SecretId": "ghost",
        "ResourcePolicy": r#"{"Version":"2012-10-17","Statement":[]}"#,
    });
    let req = make_request("ValidateResourcePolicy", &body.to_string());
    let err = expect_err(svc.handle(req).await);
    assert!(err.to_string().contains("ResourceNotFoundException"));
}

// ── Tag operations edge cases ──

#[tokio::test]
async fn tag_resource_updates_existing_tag() {
    let state = make_state();
    let svc = SecretsManagerService::new(state);

    let body = serde_json::json!({
        "Name": "tag-upd",
        "SecretString": "v",
        "Tags": [{"Key": "env", "Value": "dev"}],
    });
    let req = make_request("CreateSecret", &body.to_string());
    svc.handle(req).await.unwrap();

    // Update existing tag value
    let body = serde_json::json!({
        "SecretId": "tag-upd",
        "Tags": [{"Key": "env", "Value": "prod"}],
    });
    let req = make_request("TagResource", &body.to_string());
    svc.handle(req).await.unwrap();

    let req = make_request("DescribeSecret", r#"{"SecretId": "tag-upd"}"#);
    let resp = svc.handle(req).await.unwrap();
    let b: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let tags = b["Tags"].as_array().unwrap();
    assert_eq!(tags.len(), 1);
    assert_eq!(tags[0]["Value"], "prod");
}

// ── Unsupported action ──

#[tokio::test]
async fn unsupported_action_returns_error() {
    let state = make_state();
    let svc = SecretsManagerService::new(state);

    let req = make_request("BogusAction", "{}");
    let err = expect_err(svc.handle(req).await);
    assert!(err.to_string().contains("BogusAction"));
}

// ── Helper function tests ──

#[test]
fn test_split_words_basic() {
    assert_eq!(split_words("hello"), vec!["hello"]);
    assert_eq!(split_words("HelloWorld"), vec!["Hello", "World"]);
    assert_eq!(split_words("my/secret/name"), vec!["my", "secret", "name"]);
    assert_eq!(split_words("my-secret-name"), vec!["my", "secret", "name"]);
    assert_eq!(split_words("my_secret_name"), vec!["my", "secret", "name"]);
}

#[test]
fn test_split_words_multiple_delimiters() {
    // Multiple different special chars -> don't split
    assert_eq!(split_words("my/secret-name"), vec!["my/secret-name"]);
}

#[test]
fn test_split_words_with_spaces() {
    let words = split_words("hello world");
    assert_eq!(words, vec!["hello", "world"]);
}

#[test]
fn test_match_pattern_prefix() {
    assert!(match_pattern("prod", "production", true, true));
    assert!(!match_pattern("Prod", "production", true, true));
    assert!(match_pattern("Prod", "production", true, false));
}

#[test]
fn test_match_pattern_word() {
    assert!(match_pattern("hello", "HelloWorld", false, false));
    assert!(match_pattern("world", "HelloWorld", false, false));
}

#[test]
fn test_matcher_negation() {
    // Negated: "!prod" matches strings that DON'T match "prod"
    assert!(matcher(&["!prod"], &["staging"], true, true));
}

#[test]
fn test_base64_roundtrip() {
    let data = b"Hello, World!";
    let encoded = base64_encode(data);
    let decoded = base64_decode(&encoded).unwrap();
    assert_eq!(&decoded, data);
}

#[test]
fn test_base64_decode_invalid() {
    // Invalid base64 char
    assert!(base64_decode("!!!").is_none());
}

#[test]
fn test_check_version_idempotency() {
    let mut versions = BTreeMap::new();
    versions.insert(
        "v1".to_string(),
        SecretVersion {
            version_id: "v1".to_string(),
            secret_string: Some("hello".to_string()),
            secret_binary: None,
            stages: vec!["AWSCURRENT".to_string()],
            created_at: Utc::now(),
        },
    );

    // Not found
    assert!(matches!(
        check_secret_version_idempotency(&versions, "v2", None, &Some("x".to_string()), &None),
        VersionIdempotency::NotFound
    ));

    // Match
    assert!(matches!(
        check_secret_version_idempotency(
            &versions,
            "v1",
            Some("hello".to_string()),
            &Some("hello".to_string()),
            &None
        ),
        VersionIdempotency::Match
    ));

    // Conflict
    assert!(matches!(
        check_secret_version_idempotency(
            &versions,
            "v1",
            Some("hello".to_string()),
            &Some("different".to_string()),
            &None
        ),
        VersionIdempotency::Conflict
    ));
}

#[test]
fn test_is_mutating_action() {
    assert!(is_mutating_action("CreateSecret"));
    assert!(is_mutating_action("DeleteSecret"));
    assert!(is_mutating_action("TagResource"));
    assert!(!is_mutating_action("GetSecretValue"));
    assert!(!is_mutating_action("ListSecrets"));
    assert!(!is_mutating_action("DescribeSecret"));
}

#[test]
fn test_parse_tags_empty() {
    let val = serde_json::json!(null);
    assert_eq!(parse_tags(&val), vec![]);
}

#[test]
fn test_tags_to_json_roundtrip() {
    let tags = vec![
        ("k1".to_string(), "v1".to_string()),
        ("k2".to_string(), "v2".to_string()),
    ];
    let json = tags_to_json(&tags);
    assert_eq!(json.len(), 2);
    assert_eq!(json[0]["Key"], "k1");
    assert_eq!(json[1]["Value"], "v2");
}

#[test]
fn test_filter_name_prefix() {
    let secret = Secret {
        name: "prod/database".to_string(),
        arn: "arn".to_string(),
        description: None,
        kms_key_id: None,
        versions: BTreeMap::new(),
        current_version_id: None,
        tags: vec![],
        tags_ever_set: false,
        deleted: false,
        deletion_date: None,
        created_at: Utc::now(),
        last_changed_at: Utc::now(),
        last_accessed_at: None,
        rotation_enabled: None,
        rotation_lambda_arn: None,
        rotation_rules: None,
        last_rotated_at: None,
        resource_policy: None,
        replica_regions: Vec::new(),
    };
    assert!(filter_name(&secret, &["prod/"]));
    assert!(!filter_name(&secret, &["staging/"]));
}

#[test]
fn test_filter_tag_value() {
    let secret = Secret {
        name: "s".to_string(),
        arn: "arn".to_string(),
        description: None,
        kms_key_id: None,
        versions: BTreeMap::new(),
        current_version_id: None,
        tags: vec![("env".to_string(), "production".to_string())],
        tags_ever_set: true,
        deleted: false,
        deletion_date: None,
        created_at: Utc::now(),
        last_changed_at: Utc::now(),
        last_accessed_at: None,
        rotation_enabled: None,
        rotation_lambda_arn: None,
        rotation_rules: None,
        last_rotated_at: None,
        resource_policy: None,
        replica_regions: Vec::new(),
    };
    assert!(filter_tag_value(&secret, &["prod"]));
    assert!(!filter_tag_value(&secret, &["staging"]));
}

#[test]
fn test_filter_all_searches_name_desc_tags() {
    let secret = Secret {
        name: "my-secret".to_string(),
        arn: "arn".to_string(),
        description: Some("important database".to_string()),
        kms_key_id: None,
        versions: BTreeMap::new(),
        current_version_id: None,
        tags: vec![("team".to_string(), "backend".to_string())],
        tags_ever_set: true,
        deleted: false,
        deletion_date: None,
        created_at: Utc::now(),
        last_changed_at: Utc::now(),
        last_accessed_at: None,
        rotation_enabled: None,
        rotation_lambda_arn: None,
        rotation_rules: None,
        last_rotated_at: None,
        resource_policy: None,
        replica_regions: Vec::new(),
    };
    // Matches name
    assert!(filter_all(&secret, &["my"]));
    // Matches description
    assert!(filter_all(&secret, &["database"]));
    // Matches tag key
    assert!(filter_all(&secret, &["team"]));
    // Matches tag value
    assert!(filter_all(&secret, &["backend"]));
    // No match
    assert!(!filter_all(&secret, &["zzzz"]));
}

// ── Cross-account GetSecretValue: resource policy enforcement ────

fn make_request_for(action: &str, account: &str, body: &str) -> AwsRequest {
    let mut req = make_request(action, body);
    req.account_id = account.to_string();
    req
}

#[tokio::test]
async fn cross_account_get_secret_value_denied_without_policy() {
    let state = make_state();
    let svc = SecretsManagerService::new(state);

    // Owner creates the secret in account 111111111111.
    let req = make_request_for(
        "CreateSecret",
        "111111111111",
        r#"{"Name": "shared/secret", "SecretString": "ssss"}"#,
    );
    let resp = svc.handle(req).await.unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let arn = body["ARN"].as_str().unwrap().to_string();

    // Another account asks for it without any resource policy in
    // place — must be denied.
    let req = make_request_for(
        "GetSecretValue",
        "222222222222",
        &format!(r#"{{"SecretId": "{arn}"}}"#),
    );
    let err = expect_err(svc.handle(req).await);
    assert_eq!(err.code(), "AccessDeniedException");
}

#[tokio::test]
async fn cross_account_get_secret_value_allowed_with_matching_policy() {
    let state = make_state();
    let svc = SecretsManagerService::new(state.clone());

    // Owner creates the secret.
    let req = make_request_for(
        "CreateSecret",
        "111111111111",
        r#"{"Name": "shared/secret", "SecretString": "shhh"}"#,
    );
    let resp = svc.handle(req).await.unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let arn = body["ARN"].as_str().unwrap().to_string();

    // Owner attaches a resource policy granting GetSecretValue to
    // the cross-account principal.
    let policy = serde_json::json!({
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": {"AWS": "arn:aws:iam::222222222222:root"},
            "Action": "secretsmanager:GetSecretValue",
            "Resource": "*"
        }]
    });
    let put_policy = make_request_for(
        "PutResourcePolicy",
        "111111111111",
        &format!(
            r#"{{"SecretId": "{arn}", "ResourcePolicy": {}}}"#,
            serde_json::to_string(&policy.to_string()).unwrap()
        ),
    );
    svc.handle(put_policy).await.unwrap();

    // Cross-account caller now succeeds.
    let req = make_request_for(
        "GetSecretValue",
        "222222222222",
        &format!(r#"{{"SecretId": "{arn}"}}"#),
    );
    let resp = svc.handle(req).await.unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["SecretString"].as_str().unwrap(), "shhh");
}

#[test]
fn secret_owner_account_extracts_from_arn() {
    assert_eq!(
        secret_owner_account(
            "arn:aws:secretsmanager:us-east-1:111111111111:secret:s-abc123",
            "999999999999"
        ),
        "111111111111"
    );
    assert_eq!(
        secret_owner_account("plain-name", "999999999999"),
        "999999999999"
    );
}

/// No snapshot store (memory mode) -> no persist hook for the CFN provisioner.
#[test]
fn snapshot_hook_is_none_without_store() {
    let svc = SecretsManagerService::new(make_state());
    assert!(svc.snapshot_hook().is_none());
}

/// With a store, the hook is present and invoking it runs the whole-state
/// persist path the CloudFormation provisioner uses after mutating
/// Secrets Manager state directly.
#[tokio::test]
async fn snapshot_hook_fires_with_store() {
    let store: Arc<dyn fakecloud_persistence::SnapshotStore> =
        Arc::new(fakecloud_persistence::MemorySnapshotStore::new());
    let svc = SecretsManagerService::new(make_state()).with_snapshot_store(store);
    let hook = svc
        .snapshot_hook()
        .expect("hook present when a store is set");
    hook().await;
}

/// A KMS hook that "encrypts" by prefixing `ENC:` and "decrypts" by stripping
/// it, so a test can prove a read path returns plaintext, not the stored
/// ciphertext.
struct PrefixKmsHook;
impl fakecloud_core::delivery::KmsHook for PrefixKmsHook {
    fn encrypt(
        &self,
        _account_id: &str,
        _region: &str,
        _key_id: &str,
        plaintext: &[u8],
        _service_principal: &str,
        _ctx: std::collections::HashMap<String, String>,
    ) -> Result<String, String> {
        Ok(format!("ENC:{}", String::from_utf8_lossy(plaintext)))
    }
    fn decrypt(
        &self,
        _account_id: &str,
        ciphertext_b64: &str,
        _service_principal: &str,
        _ctx: std::collections::HashMap<String, String>,
    ) -> Result<Vec<u8>, String> {
        Ok(ciphertext_b64
            .strip_prefix("ENC:")
            .unwrap_or(ciphertext_b64)
            .as_bytes()
            .to_vec())
    }
}

#[tokio::test]
async fn batch_get_secret_value_returns_plaintext_not_ciphertext() {
    // GetSecretValue decrypts a KMS-backed secret, but BatchGetSecretValue
    // pushed the raw stored ciphertext -- so a batch fetch returned an
    // unusable encrypted blob (bug-audit 2026-06-20, 1.10).
    let state = make_state();
    let svc = SecretsManagerService::new(state).with_kms_hook(std::sync::Arc::new(PrefixKmsHook));

    let req = make_request(
        "CreateSecret",
        r#"{"Name":"enc-secret","SecretString":"topsecret","KmsKeyId":"alias/test"}"#,
    );
    svc.handle(req).await.unwrap();

    // Sanity: single-get decrypts.
    let req = make_request("GetSecretValue", r#"{"SecretId":"enc-secret"}"#);
    let single: Value =
        serde_json::from_slice(svc.handle(req).await.unwrap().body.expect_bytes()).unwrap();
    assert_eq!(single["SecretString"], "topsecret");

    // The fix: batch must decrypt too.
    let body = serde_json::json!({ "SecretIdList": ["enc-secret"] });
    let req = make_request("BatchGetSecretValue", &body.to_string());
    let batch: Value =
        serde_json::from_slice(svc.handle(req).await.unwrap().body.expect_bytes()).unwrap();
    let v = &batch["SecretValues"].as_array().unwrap()[0];
    assert_eq!(
        v["SecretString"], "topsecret",
        "BatchGetSecretValue must return plaintext, not ciphertext"
    );
}

#[tokio::test]
async fn test_rotate_secret_rotate_immediately_false_does_not_rotate_value() {
    let state = make_state();
    let svc = SecretsManagerService::new(state.clone());

    let req = make_request(
        "CreateSecret",
        r#"{"Name": "rot-defer", "SecretString": "original"}"#,
    );
    let resp = svc.handle(req).await.unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let original_vid = body["VersionId"].as_str().unwrap().to_string();

    // Rotate with RotateImmediately=false: config saved, value untouched.
    let token = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee";
    let rot = serde_json::json!({
        "SecretId": "rot-defer",
        "RotationRules": { "AutomaticallyAfterDays": 30 },
        "ClientRequestToken": token,
        "RotateImmediately": false,
    });
    let req = make_request("RotateSecret", &rot.to_string());
    svc.handle(req).await.unwrap();

    let accts = state.read();
    let s = accts.default_ref();
    let secret = s.secrets.get("rot-defer").unwrap();
    // Rotation config is saved and enabled.
    assert_eq!(secret.rotation_enabled, Some(true));
    assert!(secret.rotation_rules.is_some());
    // But the value was NOT rotated: no new version, current unchanged,
    // and LastRotatedDate is not set (no rotation happened).
    assert!(!secret.versions.contains_key(token));
    assert_eq!(
        secret.current_version_id.as_deref(),
        Some(original_vid.as_str())
    );
    assert!(secret.last_rotated_at.is_none());
}

#[tokio::test]
async fn test_rotate_secret_immediately_false_with_lambda_runs_test_step_only() {
    let state = make_state();
    let svc = SecretsManagerService::new(state.clone());

    svc.handle(make_request(
        "CreateSecret",
        r#"{"Name": "rot-test-step", "SecretString": "original"}"#,
    ))
    .await
    .unwrap();

    // RotateImmediately=false with a Lambda: AWS still validates the config by
    // running ONLY the testSecret step. The value is not rotated.
    let token = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee";
    let rot = serde_json::json!({
        "SecretId": "rot-test-step",
        "RotationLambdaARN": "arn:aws:lambda:us-east-1:123456789012:function:rotator",
        "RotationRules": { "AutomaticallyAfterDays": 30 },
        "ClientRequestToken": token,
        "RotateImmediately": false,
    });
    // Capture the AWSCURRENT version id before rotation.
    let original_current = {
        let accts = state.read();
        accts
            .default_ref()
            .secrets
            .get("rot-test-step")
            .unwrap()
            .current_version_id
            .clone()
    };

    let (_resp, invocation) = svc
        .rotate_secret(&make_request("RotateSecret", &rot.to_string()))
        .unwrap();
    let inv = invocation.expect("a Lambda invocation must be scheduled to test the config");
    assert_eq!(inv.steps, vec!["testSecret"]);
    let cleanup = inv
        .cleanup_pending
        .expect("test-only rotation must stage a temporary AWSPENDING version to clean up");

    {
        let accts = state.read();
        let secret = accts.default_ref().secrets.get("rot-test-step").unwrap();
        // A temporary AWSPENDING version was staged for the testSecret step,
        // carrying a copy of the current value.
        let pending = secret
            .versions
            .get(token)
            .expect("AWSPENDING test version must exist for the Lambda to read");
        assert!(pending.stages.contains(&"AWSPENDING".to_string()));
        assert_eq!(pending.secret_string.as_deref(), Some("original"));
        // The current value is NOT rotated and LastRotatedDate is untouched.
        assert_eq!(secret.current_version_id, original_current);
        assert!(secret.last_rotated_at.is_none());
    }

    // The cleanup removes the temporary AWSPENDING version.
    super::remove_rotation_test_pending(&state, &cleanup);
    let accts = state.read();
    let secret = accts.default_ref().secrets.get("rot-test-step").unwrap();
    assert!(
        !secret.versions.contains_key(token),
        "temporary AWSPENDING version must be cleaned up after the test step"
    );
}

/// A RotateImmediately=false rotation must NOT persist the temporary
/// AWSPENDING test version: the mutating-action snapshot taken by `handle`
/// must capture the post-cleanup state so a restart never restores a stale
/// AWSPENDING version.
#[tokio::test]
async fn test_rotate_immediately_false_pending_not_persisted() {
    #[derive(Default)]
    struct RecordingStore {
        bytes: std::sync::Mutex<Option<Vec<u8>>>,
    }
    impl fakecloud_persistence::SnapshotStore for RecordingStore {
        fn load(&self) -> std::io::Result<Option<Vec<u8>>> {
            Ok(self.bytes.lock().unwrap().clone())
        }
        fn save(&self, bytes: &[u8]) -> std::io::Result<()> {
            *self.bytes.lock().unwrap() = Some(bytes.to_vec());
            Ok(())
        }
    }

    let state = make_state();
    let store = Arc::new(RecordingStore::default());
    let svc = SecretsManagerService::new(state.clone())
        .with_snapshot_store(store.clone() as Arc<dyn fakecloud_persistence::SnapshotStore>);

    svc.handle(make_request(
        "CreateSecret",
        r#"{"Name": "rot-persist", "SecretString": "original"}"#,
    ))
    .await
    .unwrap();

    // RotateImmediately=false with a Lambda but no delivery bus: the testSecret
    // step is skipped, but the temporary AWSPENDING version is staged and then
    // cleaned up synchronously before the snapshot is taken.
    let token = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee";
    let rot = serde_json::json!({
        "SecretId": "rot-persist",
        "RotationLambdaARN": "arn:aws:lambda:us-east-1:123456789012:function:rotator",
        "ClientRequestToken": token,
        "RotateImmediately": false,
    });
    svc.handle(make_request("RotateSecret", &rot.to_string()))
        .await
        .unwrap();

    // In-memory state is already clean.
    {
        let accts = state.read();
        let secret = accts.default_ref().secrets.get("rot-persist").unwrap();
        assert!(!secret.versions.contains_key(token));
        assert!(secret
            .versions
            .values()
            .all(|v| !v.stages.contains(&"AWSPENDING".to_string())));
    }

    // The persisted snapshot (taken by the mutating-action save at the end of
    // `handle`) must also be free of the temporary AWSPENDING version.
    let bytes = fakecloud_persistence::SnapshotStore::load(store.as_ref())
        .unwrap()
        .expect("RotateSecret must persist a snapshot");
    let snap: crate::SecretsManagerSnapshot = serde_json::from_slice(&bytes).unwrap();
    let accounts = snap.accounts.expect("multi-account snapshot");
    let persisted = &accounts.default_ref().secrets["rot-persist"];
    // First prove we're inspecting the POST-RotateSecret snapshot (not the
    // earlier CreateSecret one): the rotation config set by this RotateSecret
    // call must be present. Otherwise "no AWSPENDING" would prove nothing.
    assert_eq!(
        persisted.rotation_enabled,
        Some(true),
        "snapshot must be the post-RotateSecret one (rotation enabled)"
    );
    assert_eq!(
        persisted.rotation_lambda_arn.as_deref(),
        Some("arn:aws:lambda:us-east-1:123456789012:function:rotator"),
        "snapshot must be the post-RotateSecret one (rotation Lambda configured)"
    );
    assert!(
        !persisted.versions.contains_key(token),
        "temporary AWSPENDING version must not be persisted"
    );
    assert!(
        persisted
            .versions
            .values()
            .all(|v| !v.stages.contains(&"AWSPENDING".to_string())),
        "no AWSPENDING version may remain in the persisted snapshot"
    );
}

#[tokio::test]
async fn test_rotate_secret_immediately_with_lambda_runs_full_steps() {
    let state = make_state();
    let svc = SecretsManagerService::new(state.clone());

    svc.handle(make_request(
        "CreateSecret",
        r#"{"Name": "rot-full", "SecretString": "original"}"#,
    ))
    .await
    .unwrap();

    let token = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee";
    let rot = serde_json::json!({
        "SecretId": "rot-full",
        "RotationLambdaARN": "arn:aws:lambda:us-east-1:123456789012:function:rotator",
        "ClientRequestToken": token,
    });
    let (_resp, invocation) = svc
        .rotate_secret(&make_request("RotateSecret", &rot.to_string()))
        .unwrap();
    let inv = invocation.expect("full rotation must schedule a Lambda invocation");
    assert_eq!(
        inv.steps,
        vec!["createSecret", "setSecret", "testSecret", "finishSecret"]
    );
}

#[tokio::test]
async fn test_rotate_secret_rotate_immediately_default_true_rotates() {
    let state = make_state();
    let svc = SecretsManagerService::new(state.clone());

    svc.handle(make_request(
        "CreateSecret",
        r#"{"Name": "rot-now", "SecretString": "original"}"#,
    ))
    .await
    .unwrap();

    let token = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee";
    let rot = serde_json::json!({
        "SecretId": "rot-now",
        "ClientRequestToken": token,
    });
    svc.handle(make_request("RotateSecret", &rot.to_string()))
        .await
        .unwrap();

    let accts = state.read();
    let s = accts.default_ref();
    let secret = s.secrets.get("rot-now").unwrap();
    // Default (RotateImmediately omitted == true): value rotated.
    assert_eq!(secret.current_version_id.as_deref(), Some(token));
    assert!(secret.last_rotated_at.is_some());
}

#[tokio::test]
async fn test_create_secret_with_add_replica_regions() {
    let state = make_state();
    let svc = SecretsManagerService::new(state.clone());

    let req = make_request(
        "CreateSecret",
        r#"{"Name": "replicated", "SecretString": "v1",
            "AddReplicaRegions": [{"Region": "us-west-2"}, {"Region": "eu-west-1"}]}"#,
    );
    let resp = svc.handle(req).await.unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let statuses = body["ReplicationStatus"].as_array().unwrap();
    let regions: Vec<&str> = statuses
        .iter()
        .map(|s| s["Region"].as_str().unwrap())
        .collect();
    assert!(regions.contains(&"us-west-2"));
    assert!(regions.contains(&"eu-west-1"));

    // Persisted: DescribeSecret echoes ReplicationStatus too.
    let req = make_request("DescribeSecret", r#"{"SecretId": "replicated"}"#);
    let resp = svc.handle(req).await.unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let dregions: Vec<&str> = body["ReplicationStatus"]
        .as_array()
        .unwrap()
        .iter()
        .map(|s| s["Region"].as_str().unwrap())
        .collect();
    assert!(dregions.contains(&"us-west-2"));
    assert!(dregions.contains(&"eu-west-1"));
}

// ---------------------------------------------------------------------------
// Hardening: single AWSPREVIOUS / AWSCURRENT, input validation, expiry.
// ---------------------------------------------------------------------------

/// Count how many versions of a secret carry a given staging label.
fn stage_count(state: &SharedSecretsManagerState, name: &str, stage: &str) -> usize {
    let accts = state.read();
    accts.default_ref().secrets[name]
        .versions
        .values()
        .filter(|v| v.stages.iter().any(|s| s == stage))
        .count()
}

#[tokio::test]
async fn test_update_secret_keeps_single_awsprevious() {
    // Repeated UpdateSecret must leave exactly one AWSPREVIOUS version, and it
    // must be the immediately-previous value -- not accumulate across updates.
    let state = make_state();
    let svc = SecretsManagerService::new(state.clone());

    let req = make_request("CreateSecret", r#"{"Name": "prev", "SecretString": "v1"}"#);
    svc.handle(req).await.unwrap();
    let req = make_request(
        "UpdateSecret",
        r#"{"SecretId": "prev", "SecretString": "v2"}"#,
    );
    svc.handle(req).await.unwrap();
    let req = make_request(
        "UpdateSecret",
        r#"{"SecretId": "prev", "SecretString": "v3"}"#,
    );
    svc.handle(req).await.unwrap();

    assert_eq!(
        stage_count(&state, "prev", "AWSPREVIOUS"),
        1,
        "exactly one version must hold AWSPREVIOUS"
    );
    assert_eq!(stage_count(&state, "prev", "AWSCURRENT"), 1);

    // AWSPREVIOUS resolves to the immediately-previous value, deterministically.
    let req = make_request(
        "GetSecretValue",
        r#"{"SecretId": "prev", "VersionStage": "AWSPREVIOUS"}"#,
    );
    let resp = svc.handle(req).await.unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["SecretString"], "v2");
}

#[tokio::test]
async fn test_rotate_secret_simple_keeps_single_awsprevious() {
    // The no-Lambda rotation path must also keep AWSPREVIOUS unique across
    // repeated rotations.
    let state = make_state();
    let svc = SecretsManagerService::new(state.clone());

    let req = make_request("CreateSecret", r#"{"Name": "rot", "SecretString": "v1"}"#);
    svc.handle(req).await.unwrap();

    for _ in 0..3 {
        let req = make_request("RotateSecret", r#"{"SecretId": "rot"}"#);
        svc.handle(req).await.unwrap();
    }

    assert_eq!(stage_count(&state, "rot", "AWSPREVIOUS"), 1);
    assert_eq!(stage_count(&state, "rot", "AWSCURRENT"), 1);
}

#[tokio::test]
async fn test_update_version_stage_wrong_remove_from_no_double_current() {
    // Moving AWSCURRENT with a RemoveFromVersionId that does NOT hold AWSCURRENT
    // must still leave exactly one AWSCURRENT (the demotion targets the real
    // holder, not the caller-supplied id).
    let state = make_state();
    let svc = SecretsManagerService::new(state.clone());

    let v1 = "11111111-1111-1111-1111-111111111111";
    let v2 = "22222222-2222-2222-2222-222222222222";

    let body = serde_json::json!({
        "Name": "stage", "SecretString": "v1", "ClientRequestToken": v1,
    });
    svc.handle(make_request("CreateSecret", &body.to_string()))
        .await
        .unwrap();

    // Stage a second version as AWSPENDING.
    let body = serde_json::json!({
        "SecretId": "stage", "SecretString": "v2",
        "ClientRequestToken": v2, "VersionStages": ["AWSPENDING"],
    });
    svc.handle(make_request("PutSecretValue", &body.to_string()))
        .await
        .unwrap();

    // Move AWSCURRENT to v2 but pass v2 (wrong) as RemoveFromVersionId.
    let body = serde_json::json!({
        "SecretId": "stage", "VersionStage": "AWSCURRENT",
        "MoveToVersionId": v2, "RemoveFromVersionId": v2,
    });
    svc.handle(make_request("UpdateSecretVersionStage", &body.to_string()))
        .await
        .unwrap();

    assert_eq!(
        stage_count(&state, "stage", "AWSCURRENT"),
        1,
        "there must be exactly one AWSCURRENT"
    );
    // And it must be v2.
    let req = make_request("GetSecretValue", r#"{"SecretId": "stage"}"#);
    let resp = svc.handle(req).await.unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["SecretString"], "v2");
}

#[tokio::test]
async fn test_create_secret_invalid_binary_rejected() {
    let state = make_state();
    let svc = SecretsManagerService::new(state);

    let req = make_request(
        "CreateSecret",
        r#"{"Name": "badbin", "SecretBinary": "!!!not_base64!!!"}"#,
    );
    let err = expect_err(svc.handle(req).await);
    assert!(err.to_string().contains("InvalidParameterException"));
}

#[tokio::test]
async fn test_put_secret_value_invalid_binary_rejected() {
    let state = make_state();
    let svc = SecretsManagerService::new(state);

    svc.handle(make_request(
        "CreateSecret",
        r#"{"Name": "bin2", "SecretString": "v1"}"#,
    ))
    .await
    .unwrap();

    let req = make_request(
        "PutSecretValue",
        r#"{"SecretId": "bin2", "SecretBinary": "!!!not_base64!!!"}"#,
    );
    let err = expect_err(svc.handle(req).await);
    assert!(err.to_string().contains("InvalidParameterException"));
}

#[tokio::test]
async fn test_create_secret_invalid_name_charset_rejected() {
    let state = make_state();
    let svc = SecretsManagerService::new(state);

    let req = make_request(
        "CreateSecret",
        r#"{"Name": "bad$name", "SecretString": "v"}"#,
    );
    let err = expect_err(svc.handle(req).await);
    assert!(err.to_string().contains("InvalidParameterException"));

    // A name using every allowed special character is accepted.
    let req = make_request(
        "CreateSecret",
        r#"{"Name": "ok/name_+=.@-1", "SecretString": "v"}"#,
    );
    assert_eq!(svc.handle(req).await.unwrap().status, StatusCode::OK);
}

#[tokio::test]
async fn test_get_random_password_short_length_includes_each_type() {
    let state = make_state();
    let svc = SecretsManagerService::new(state);

    // Minimum length with all four required classes: each class must survive.
    let req = make_request("GetRandomPassword", r#"{"PasswordLength": 4}"#);
    let resp = svc.handle(req).await.unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let pw = body["RandomPassword"].as_str().unwrap();
    assert_eq!(pw.chars().count(), 4);
    assert!(pw.chars().any(|c| c.is_ascii_lowercase()), "pw={pw}");
    assert!(pw.chars().any(|c| c.is_ascii_uppercase()), "pw={pw}");
    assert!(pw.chars().any(|c| c.is_ascii_digit()), "pw={pw}");
    assert!(
        pw.chars().any(|c| !c.is_ascii_alphanumeric()),
        "pw={pw} must contain punctuation"
    );
}

#[tokio::test]
async fn test_get_random_password_length_too_short_for_required_types() {
    let state = make_state();
    let svc = SecretsManagerService::new(state);

    // Length 4 can't hold all four classes plus a required space.
    let req = make_request(
        "GetRandomPassword",
        r#"{"PasswordLength": 4, "IncludeSpace": true}"#,
    );
    let err = expect_err(svc.handle(req).await);
    assert!(err.to_string().contains("InvalidParameterException"));
}

#[tokio::test]
async fn test_rotate_secret_simple_path_stamps_last_rotated() {
    // The no-Lambda simple rotation completes synchronously, so LastRotatedDate
    // is stamped.
    let state = make_state();
    let svc = SecretsManagerService::new(state.clone());

    svc.handle(make_request(
        "CreateSecret",
        r#"{"Name": "rotstamp", "SecretString": "v1"}"#,
    ))
    .await
    .unwrap();

    svc.handle(make_request("RotateSecret", r#"{"SecretId": "rotstamp"}"#))
        .await
        .unwrap();

    let accts = state.read();
    let secret = &accts.default_ref().secrets["rotstamp"];
    assert!(
        secret.last_rotated_at.is_some(),
        "synchronous rotation must stamp LastRotatedDate"
    );
}

#[tokio::test]
async fn test_scheduled_deletion_expires_on_read() {
    // Once the recovery window elapses, the secret is treated as gone.
    let state = make_state();
    let svc = SecretsManagerService::new(state.clone());

    svc.handle(make_request(
        "CreateSecret",
        r#"{"Name": "expiring", "SecretString": "v1"}"#,
    ))
    .await
    .unwrap();
    svc.handle(make_request("DeleteSecret", r#"{"SecretId": "expiring"}"#))
        .await
        .unwrap();

    // Force the recovery window to have already elapsed.
    {
        let mut accts = state.write();
        accts
            .default_mut()
            .secrets
            .get_mut("expiring")
            .unwrap()
            .deletion_date = Some(Utc::now() - chrono::Duration::days(1));
    }

    let err = expect_err(
        svc.handle(make_request(
            "GetSecretValue",
            r#"{"SecretId": "expiring"}"#,
        ))
        .await,
    );
    assert!(err.to_string().contains("ResourceNotFoundException"));

    // A still-within-window deletion is NOT treated as gone (it reports the
    // marked-for-deletion error instead of not-found).
    svc.handle(make_request(
        "CreateSecret",
        r#"{"Name": "pending-del", "SecretString": "v1"}"#,
    ))
    .await
    .unwrap();
    svc.handle(make_request(
        "DeleteSecret",
        r#"{"SecretId": "pending-del"}"#,
    ))
    .await
    .unwrap();
    let err = expect_err(
        svc.handle(make_request(
            "GetSecretValue",
            r#"{"SecretId": "pending-del"}"#,
        ))
        .await,
    );
    assert!(err.to_string().contains("marked for deletion"));
}
