use super::*;

use crate::state::{
    default_schema_attributes, AccessTokenData, AuthEvent, ChallengeResult, SessionData,
};
use crate::triggers;

/// Helper to run an async fn in sync test context.
fn block_on<F: std::future::Future>(f: F) -> F::Output {
    tokio::runtime::Runtime::new().unwrap().block_on(f)
}

#[test]
fn pool_id_format() {
    let id = generate_pool_id("us-east-1");
    assert!(
        id.starts_with("us-east-1_"),
        "ID should start with region prefix: {id}"
    );
    let suffix = id.strip_prefix("us-east-1_").unwrap();
    assert_eq!(suffix.len(), 9, "Suffix should be 9 chars: {suffix}");
    assert!(
        suffix.chars().all(|c| c.is_alphanumeric()),
        "Suffix should be alphanumeric: {suffix}"
    );
}

#[test]
fn pool_id_format_other_region() {
    let id = generate_pool_id("eu-west-1");
    assert!(id.starts_with("eu-west-1_"));
    let suffix = id.strip_prefix("eu-west-1_").unwrap();
    assert_eq!(suffix.len(), 9);
}

#[test]
fn default_password_policy_values() {
    let policy = PasswordPolicy::default();
    assert_eq!(policy.minimum_length, 8);
    assert!(policy.require_uppercase);
    assert!(policy.require_lowercase);
    assert!(policy.require_numbers);
    assert!(policy.require_symbols);
    assert_eq!(policy.temporary_password_validity_days, 7);
}

#[test]
fn parse_password_policy_from_json() {
    let val = json!({
        "MinimumLength": 12,
        "RequireUppercase": false,
        "RequireLowercase": true,
        "RequireNumbers": true,
        "RequireSymbols": false,
        "TemporaryPasswordValidityDays": 3,
    });
    let policy = parse_password_policy(&val);
    assert_eq!(policy.minimum_length, 12);
    assert!(!policy.require_uppercase);
    assert!(policy.require_lowercase);
    assert!(policy.require_numbers);
    assert!(!policy.require_symbols);
    assert_eq!(policy.temporary_password_validity_days, 3);
}

#[test]
fn parse_password_policy_null_returns_default() {
    let policy = parse_password_policy(&Value::Null);
    assert_eq!(policy.minimum_length, 8);
    assert!(policy.require_uppercase);
}

#[test]
fn default_schema_has_expected_attributes() {
    let attrs = default_schema_attributes();
    let names: Vec<&str> = attrs.iter().map(|a| a.name.as_str()).collect();
    assert!(names.contains(&"sub"));
    assert!(names.contains(&"email"));
    assert!(names.contains(&"phone_number"));
    assert!(names.contains(&"email_verified"));
    assert!(names.contains(&"phone_number_verified"));
    assert!(names.contains(&"updated_at"));
}

#[test]
fn create_user_pool_missing_name() {
    let state = std::sync::Arc::new(parking_lot::RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new(
            "123456789012",
            "us-east-1",
            "http://localhost:4569",
        ),
    ));
    let svc = CognitoService::new(state);
    let req = AwsRequest {
        service: "cognito-idp".to_string(),
        action: "CreateUserPool".to_string(),
        region: "us-east-1".to_string(),
        account_id: "123456789012".to_string(),
        request_id: "test".to_string(),
        headers: http::HeaderMap::new(),
        query_params: std::collections::HashMap::new(),
        body: bytes::Bytes::from(r#"{}"#),
        body_stream: parking_lot::Mutex::new(None),
        path_segments: vec![],
        raw_path: "/".to_string(),
        raw_query: String::new(),
        method: http::Method::POST,
        is_query_protocol: false,
        access_key_id: None,
        principal: None,
    };
    match block_on(svc.create_user_pool(&req)) {
        Err(e) => assert_eq!(e.code(), "InvalidParameterException"),
        Ok(_) => panic!("Expected InvalidParameterException error"),
    }
}

#[test]
fn client_id_format() {
    let id = generate_client_id();
    assert_eq!(id.len(), 26, "Client ID should be 26 chars: {id}");
    assert!(
        id.chars().all(|c| c.is_ascii_alphanumeric()),
        "Client ID should be alphanumeric: {id}"
    );
    assert!(
        id.chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit()),
        "Client ID should be lowercase: {id}"
    );
}

#[test]
fn client_id_uniqueness() {
    let id1 = generate_client_id();
    let id2 = generate_client_id();
    assert_ne!(id1, id2, "Client IDs should be unique");
}

#[test]
fn client_secret_format() {
    let secret = generate_client_secret();
    assert_eq!(
        secret.len(),
        51,
        "Client secret should be 51 chars: {secret}"
    );
}

#[test]
fn client_secret_not_generated_by_default() {
    let state = std::sync::Arc::new(parking_lot::RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new(
            "123456789012",
            "us-east-1",
            "http://localhost:4569",
        ),
    ));
    let svc = CognitoService::new(state.clone());

    // First create a pool
    let create_pool_req = AwsRequest {
        service: "cognito-idp".to_string(),
        action: "CreateUserPool".to_string(),
        region: "us-east-1".to_string(),
        account_id: "123456789012".to_string(),
        request_id: "test".to_string(),
        headers: http::HeaderMap::new(),
        query_params: std::collections::HashMap::new(),
        body: bytes::Bytes::from(r#"{"PoolName":"test"}"#),
        body_stream: parking_lot::Mutex::new(None),
        path_segments: vec![],
        raw_path: "/".to_string(),
        raw_query: String::new(),
        method: http::Method::POST,
        is_query_protocol: false,
        access_key_id: None,
        principal: None,
    };
    let pool_resp = block_on(svc.create_user_pool(&create_pool_req)).unwrap();
    let pool_json: Value =
        serde_json::from_str(core::str::from_utf8(pool_resp.body.expect_bytes()).unwrap()).unwrap();
    let pool_id = pool_json["UserPool"]["Id"].as_str().unwrap();

    // Create client without GenerateSecret
    let req = AwsRequest {
        service: "cognito-idp".to_string(),
        action: "CreateUserPoolClient".to_string(),
        region: "us-east-1".to_string(),
        account_id: "123456789012".to_string(),
        request_id: "test".to_string(),
        headers: http::HeaderMap::new(),
        query_params: std::collections::HashMap::new(),
        body: bytes::Bytes::from(
            serde_json::to_string(&json!({
                "UserPoolId": pool_id,
                "ClientName": "test-client"
            }))
            .unwrap(),
        ),
        body_stream: parking_lot::Mutex::new(None),
        path_segments: vec![],
        raw_path: "/".to_string(),
        raw_query: String::new(),
        method: http::Method::POST,
        is_query_protocol: false,
        access_key_id: None,
        principal: None,
    };
    let resp = svc.create_user_pool_client(&req).unwrap();
    let resp_json: Value =
        serde_json::from_str(core::str::from_utf8(resp.body.expect_bytes()).unwrap()).unwrap();
    assert!(resp_json["UserPoolClient"]["ClientSecret"].is_null());
}

#[test]
fn client_secret_generated_when_requested() {
    let state = std::sync::Arc::new(parking_lot::RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new(
            "123456789012",
            "us-east-1",
            "http://localhost:4569",
        ),
    ));
    let svc = CognitoService::new(state.clone());

    // Create a pool
    let create_pool_req = AwsRequest {
        service: "cognito-idp".to_string(),
        action: "CreateUserPool".to_string(),
        region: "us-east-1".to_string(),
        account_id: "123456789012".to_string(),
        request_id: "test".to_string(),
        headers: http::HeaderMap::new(),
        query_params: std::collections::HashMap::new(),
        body: bytes::Bytes::from(r#"{"PoolName":"test"}"#),
        body_stream: parking_lot::Mutex::new(None),
        path_segments: vec![],
        raw_path: "/".to_string(),
        raw_query: String::new(),
        method: http::Method::POST,
        is_query_protocol: false,
        access_key_id: None,
        principal: None,
    };
    let pool_resp = block_on(svc.create_user_pool(&create_pool_req)).unwrap();
    let pool_json: Value =
        serde_json::from_str(core::str::from_utf8(pool_resp.body.expect_bytes()).unwrap()).unwrap();
    let pool_id = pool_json["UserPool"]["Id"].as_str().unwrap();

    // Create client with GenerateSecret=true
    let req = AwsRequest {
        service: "cognito-idp".to_string(),
        action: "CreateUserPoolClient".to_string(),
        region: "us-east-1".to_string(),
        account_id: "123456789012".to_string(),
        request_id: "test".to_string(),
        headers: http::HeaderMap::new(),
        query_params: std::collections::HashMap::new(),
        body: bytes::Bytes::from(
            serde_json::to_string(&json!({
                "UserPoolId": pool_id,
                "ClientName": "secret-client",
                "GenerateSecret": true
            }))
            .unwrap(),
        ),
        body_stream: parking_lot::Mutex::new(None),
        path_segments: vec![],
        raw_path: "/".to_string(),
        raw_query: String::new(),
        method: http::Method::POST,
        is_query_protocol: false,
        access_key_id: None,
        principal: None,
    };
    let resp = svc.create_user_pool_client(&req).unwrap();
    let resp_json: Value =
        serde_json::from_str(core::str::from_utf8(resp.body.expect_bytes()).unwrap()).unwrap();
    let secret = resp_json["UserPoolClient"]["ClientSecret"]
        .as_str()
        .unwrap();
    assert_eq!(secret.len(), 51);
}

#[test]
fn client_belongs_to_correct_pool() {
    let state = std::sync::Arc::new(parking_lot::RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new(
            "123456789012",
            "us-east-1",
            "http://localhost:4569",
        ),
    ));
    let svc = CognitoService::new(state.clone());

    // Create two pools
    for name in &["pool-a", "pool-b"] {
        let req = AwsRequest {
            service: "cognito-idp".to_string(),
            action: "CreateUserPool".to_string(),
            region: "us-east-1".to_string(),
            account_id: "123456789012".to_string(),
            request_id: "test".to_string(),
            headers: http::HeaderMap::new(),
            query_params: std::collections::HashMap::new(),
            body: bytes::Bytes::from(serde_json::to_string(&json!({"PoolName": name})).unwrap()),
            body_stream: parking_lot::Mutex::new(None),
            path_segments: vec![],
            raw_path: "/".to_string(),
            raw_query: String::new(),
            method: http::Method::POST,
            is_query_protocol: false,
            access_key_id: None,
            principal: None,
        };
        block_on(svc.create_user_pool(&req)).unwrap();
    }

    let _mas = state.read();
    let s = _mas.default_ref();
    let pool_ids: Vec<String> = s.user_pools.keys().cloned().collect();
    drop(_mas);

    // Create client in pool A
    let req = AwsRequest {
        service: "cognito-idp".to_string(),
        action: "CreateUserPoolClient".to_string(),
        region: "us-east-1".to_string(),
        account_id: "123456789012".to_string(),
        request_id: "test".to_string(),
        headers: http::HeaderMap::new(),
        query_params: std::collections::HashMap::new(),
        body: bytes::Bytes::from(
            serde_json::to_string(&json!({
                "UserPoolId": pool_ids[0],
                "ClientName": "client-a"
            }))
            .unwrap(),
        ),
        body_stream: parking_lot::Mutex::new(None),
        path_segments: vec![],
        raw_path: "/".to_string(),
        raw_query: String::new(),
        method: http::Method::POST,
        is_query_protocol: false,
        access_key_id: None,
        principal: None,
    };
    let resp = svc.create_user_pool_client(&req).unwrap();
    let resp_json: Value =
        serde_json::from_str(core::str::from_utf8(resp.body.expect_bytes()).unwrap()).unwrap();
    let client_id = resp_json["UserPoolClient"]["ClientId"]
        .as_str()
        .unwrap()
        .to_string();

    // Describe client with pool B should fail
    let req = AwsRequest {
        service: "cognito-idp".to_string(),
        action: "DescribeUserPoolClient".to_string(),
        region: "us-east-1".to_string(),
        account_id: "123456789012".to_string(),
        request_id: "test".to_string(),
        headers: http::HeaderMap::new(),
        query_params: std::collections::HashMap::new(),
        body: bytes::Bytes::from(
            serde_json::to_string(&json!({
                "UserPoolId": pool_ids[1],
                "ClientId": client_id
            }))
            .unwrap(),
        ),
        body_stream: parking_lot::Mutex::new(None),
        path_segments: vec![],
        raw_path: "/".to_string(),
        raw_query: String::new(),
        method: http::Method::POST,
        is_query_protocol: false,
        access_key_id: None,
        principal: None,
    };
    match svc.describe_user_pool_client(&req) {
        Err(e) => assert_eq!(e.code(), "ResourceNotFoundException"),
        Ok(_) => panic!("Expected ResourceNotFoundException"),
    }
}

#[test]
fn parse_user_attributes_from_json() {
    let val = json!([
        { "Name": "email", "Value": "test@example.com" },
        { "Name": "name", "Value": "Test User" }
    ]);
    let attrs = parse_user_attributes(&val);
    assert_eq!(attrs.len(), 2);
    assert_eq!(attrs[0].name, "email");
    assert_eq!(attrs[0].value, "test@example.com");
    assert_eq!(attrs[1].name, "name");
    assert_eq!(attrs[1].value, "Test User");
}

#[test]
fn parse_user_attributes_null() {
    let attrs = parse_user_attributes(&Value::Null);
    assert!(attrs.is_empty());
}

#[test]
fn parse_filter_expression_equals() {
    let filter = parse_filter_expression(r#"email = "test@example.com""#).unwrap();
    assert_eq!(filter.attribute, "email");
    assert_eq!(filter.value, "test@example.com");
    assert!(matches!(filter.operator, FilterOp::Equals));
}

#[test]
fn parse_filter_expression_starts_with() {
    let filter = parse_filter_expression(r#"email ^= "test""#).unwrap();
    assert_eq!(filter.attribute, "email");
    assert_eq!(filter.value, "test");
    assert!(matches!(filter.operator, FilterOp::StartsWith));
}

#[test]
fn filter_matches_username() {
    let user = User {
        username: "testuser".to_string(),
        sub: Uuid::new_v4().to_string(),
        attributes: vec![],
        enabled: true,
        user_status: "CONFIRMED".to_string(),
        user_create_date: Utc::now(),
        user_last_modified_date: Utc::now(),
        password: None,
        temporary_password: None,
        confirmation_code: None,
        attribute_verification_codes: BTreeMap::new(),
        mfa_preferences: None,
        totp_secret: None,
        totp_verified: false,
        devices: BTreeMap::new(),
        linked_providers: Vec::new(),
    };

    let filter = parse_filter_expression(r#"username = "testuser""#).unwrap();
    assert!(matches_filter(&user, &filter));

    let filter = parse_filter_expression(r#"username = "other""#).unwrap();
    assert!(!matches_filter(&user, &filter));

    let filter = parse_filter_expression(r#"username ^= "test""#).unwrap();
    assert!(matches_filter(&user, &filter));
}

#[test]
fn filter_matches_attribute() {
    let user = User {
        username: "testuser".to_string(),
        sub: Uuid::new_v4().to_string(),
        attributes: vec![UserAttribute {
            name: "email".to_string(),
            value: "test@example.com".to_string(),
        }],
        enabled: true,
        user_status: "CONFIRMED".to_string(),
        user_create_date: Utc::now(),
        user_last_modified_date: Utc::now(),
        password: None,
        temporary_password: None,
        confirmation_code: None,
        attribute_verification_codes: BTreeMap::new(),
        mfa_preferences: None,
        totp_secret: None,
        totp_verified: false,
        devices: BTreeMap::new(),
        linked_providers: Vec::new(),
    };

    let filter = parse_filter_expression(r#"email = "test@example.com""#).unwrap();
    assert!(matches_filter(&user, &filter));

    let filter = parse_filter_expression(r#"email ^= "test@""#).unwrap();
    assert!(matches_filter(&user, &filter));

    let filter = parse_filter_expression(r#"email = "other@example.com""#).unwrap();
    assert!(!matches_filter(&user, &filter));
}

#[test]
fn filter_matches_user_status() {
    let user = User {
        username: "testuser".to_string(),
        sub: Uuid::new_v4().to_string(),
        attributes: vec![],
        enabled: true,
        user_status: "FORCE_CHANGE_PASSWORD".to_string(),
        user_create_date: Utc::now(),
        user_last_modified_date: Utc::now(),
        password: None,
        temporary_password: None,
        confirmation_code: None,
        attribute_verification_codes: BTreeMap::new(),
        mfa_preferences: None,
        totp_secret: None,
        totp_verified: false,
        devices: BTreeMap::new(),
        linked_providers: Vec::new(),
    };

    let filter =
        parse_filter_expression(r#"cognito:user_status = "FORCE_CHANGE_PASSWORD""#).unwrap();
    assert!(matches_filter(&user, &filter));

    let filter = parse_filter_expression(r#"status = "FORCE_CHANGE_PASSWORD""#).unwrap();
    assert!(matches_filter(&user, &filter));
}

#[test]
fn user_default_status_is_force_change_password() {
    // When a user is admin-created, the status should be FORCE_CHANGE_PASSWORD
    let state = std::sync::Arc::new(parking_lot::RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new(
            "123456789012",
            "us-east-1",
            "http://localhost:4569",
        ),
    ));
    let svc = CognitoService::new(state.clone());

    // Create a pool
    let req = AwsRequest {
        service: "cognito-idp".to_string(),
        action: "CreateUserPool".to_string(),
        region: "us-east-1".to_string(),
        account_id: "123456789012".to_string(),
        request_id: "test".to_string(),
        headers: http::HeaderMap::new(),
        query_params: std::collections::HashMap::new(),
        body: bytes::Bytes::from(r#"{"PoolName":"test"}"#),
        body_stream: parking_lot::Mutex::new(None),
        path_segments: vec![],
        raw_path: "/".to_string(),
        raw_query: String::new(),
        method: http::Method::POST,
        is_query_protocol: false,
        access_key_id: None,
        principal: None,
    };
    let pool_resp = block_on(svc.create_user_pool(&req)).unwrap();
    let pool_json: Value =
        serde_json::from_str(core::str::from_utf8(pool_resp.body.expect_bytes()).unwrap()).unwrap();
    let pool_id = pool_json["UserPool"]["Id"].as_str().unwrap();

    // Admin create user
    let req = AwsRequest {
        service: "cognito-idp".to_string(),
        action: "AdminCreateUser".to_string(),
        region: "us-east-1".to_string(),
        account_id: "123456789012".to_string(),
        request_id: "test".to_string(),
        headers: http::HeaderMap::new(),
        query_params: std::collections::HashMap::new(),
        body: bytes::Bytes::from(
            serde_json::to_string(&json!({
                "UserPoolId": pool_id,
                "Username": "testuser"
            }))
            .unwrap(),
        ),
        body_stream: parking_lot::Mutex::new(None),
        path_segments: vec![],
        raw_path: "/".to_string(),
        raw_query: String::new(),
        method: http::Method::POST,
        is_query_protocol: false,
        access_key_id: None,
        principal: None,
    };
    let resp = block_on(svc.admin_create_user(&req)).unwrap();
    let resp_json: Value =
        serde_json::from_str(core::str::from_utf8(resp.body.expect_bytes()).unwrap()).unwrap();

    assert_eq!(
        resp_json["User"]["UserStatus"].as_str().unwrap(),
        "FORCE_CHANGE_PASSWORD"
    );
    assert!(resp_json["User"]["Enabled"].as_bool().unwrap());

    // Verify sub is in attributes
    let attrs = resp_json["User"]["Attributes"].as_array().unwrap();
    let sub_attr = attrs.iter().find(|a| a["Name"] == "sub").unwrap();
    assert!(!sub_attr["Value"].as_str().unwrap().is_empty());
}

#[test]
fn jwt_format_three_base64url_segments() {
    let tokens = generate_tokens(
        "us-east-1_abc123456",
        "client123",
        "sub-uuid",
        "user1",
        "us-east-1",
        None,
    );
    // Each token should have 3 dot-separated segments
    for (name, token) in [("id", &tokens.id_token), ("access", &tokens.access_token)] {
        let parts: Vec<&str> = token.split('.').collect();
        assert_eq!(
            parts.len(),
            3,
            "{name} token should have 3 segments, got {}",
            parts.len()
        );
        // Each segment should be valid base64url (no padding, no + or /)
        for (i, part) in parts.iter().enumerate() {
            assert!(
                !part.is_empty(),
                "{name} token segment {i} should not be empty"
            );
            assert!(
                !part.contains('+'),
                "{name} token segment {i} should not contain '+'"
            );
            assert!(
                !part.contains('/'),
                "{name} token segment {i} should not contain '/'"
            );
            assert!(
                !part.contains('='),
                "{name} token segment {i} should not contain '='"
            );
        }
    }
    // Refresh token is just a random base64url string (no dots)
    assert!(
        !tokens.refresh_token.is_empty(),
        "refresh token should not be empty"
    );
    assert!(
        tokens.refresh_token.len() >= 96,
        "refresh token should be at least 96 chars, got {}",
        tokens.refresh_token.len()
    );
}

#[test]
fn jwt_id_token_payload_contains_required_fields() {
    let b64url = base64::engine::general_purpose::URL_SAFE_NO_PAD;
    let tokens = generate_tokens(
        "us-east-1_abc123456",
        "client123",
        "sub-uuid",
        "user1",
        "us-east-1",
        None,
    );
    let parts: Vec<&str> = tokens.id_token.split('.').collect();
    let header: Value = serde_json::from_slice(&b64url.decode(parts[0]).unwrap()).unwrap();
    let payload: Value = serde_json::from_slice(&b64url.decode(parts[1]).unwrap()).unwrap();

    assert_eq!(header["alg"], "RS256");
    // No `signing` was passed, so generate_tokens synthesized a fresh
    // keypair on the fly. The kid is the deterministic 16-hex-char
    // SHA-256 prefix of the public key's SPKI DER.
    let kid = header["kid"].as_str().unwrap();
    assert_eq!(kid.len(), 16, "kid must be 16 hex chars: {kid}");
    assert!(
        kid.chars().all(|c| c.is_ascii_hexdigit()),
        "kid must be lowercase hex: {kid}"
    );
    assert_eq!(payload["sub"], "sub-uuid");
    assert_eq!(payload["aud"], "client123");
    assert_eq!(payload["cognito:username"], "user1");
    assert_eq!(payload["token_use"], "id");
    assert!(payload["iss"]
        .as_str()
        .unwrap()
        .contains("us-east-1_abc123456"));
    assert!(payload["exp"].is_number());
    assert!(payload["iat"].is_number());
    assert!(payload["jti"].is_string());
}

#[test]
fn jwt_access_token_payload_contains_required_fields() {
    let b64url = base64::engine::general_purpose::URL_SAFE_NO_PAD;
    let tokens = generate_tokens(
        "us-east-1_abc123456",
        "client123",
        "sub-uuid",
        "user1",
        "us-east-1",
        None,
    );
    let parts: Vec<&str> = tokens.access_token.split('.').collect();
    let payload: Value = serde_json::from_slice(&b64url.decode(parts[1]).unwrap()).unwrap();

    assert_eq!(payload["sub"], "sub-uuid");
    assert_eq!(payload["client_id"], "client123");
    assert_eq!(payload["token_use"], "access");
    assert_eq!(payload["scope"], "aws.cognito.signin.user.admin");
    assert!(payload["exp"].is_number());
    assert!(payload["iat"].is_number());
}

#[test]
fn password_policy_rejects_short_password() {
    let policy = PasswordPolicy {
        minimum_length: 8,
        require_uppercase: false,
        require_lowercase: false,
        require_numbers: false,
        require_symbols: false,
        temporary_password_validity_days: 7,
    };
    let err = validate_password("short", &policy).unwrap_err();
    assert_eq!(err.code(), "InvalidPasswordException");
}

#[test]
fn password_policy_rejects_missing_uppercase() {
    let policy = PasswordPolicy {
        minimum_length: 1,
        require_uppercase: true,
        require_lowercase: false,
        require_numbers: false,
        require_symbols: false,
        temporary_password_validity_days: 7,
    };
    let err = validate_password("lowercase", &policy).unwrap_err();
    assert_eq!(err.code(), "InvalidPasswordException");
    assert!(validate_password("Uppercase", &policy).is_ok());
}

#[test]
fn password_policy_rejects_missing_numbers() {
    let policy = PasswordPolicy {
        minimum_length: 1,
        require_uppercase: false,
        require_lowercase: false,
        require_numbers: true,
        require_symbols: false,
        temporary_password_validity_days: 7,
    };
    let err = validate_password("nodigits", &policy).unwrap_err();
    assert_eq!(err.code(), "InvalidPasswordException");
    assert!(validate_password("has1digit", &policy).is_ok());
}

#[test]
fn password_policy_rejects_missing_symbols() {
    let policy = PasswordPolicy {
        minimum_length: 1,
        require_uppercase: false,
        require_lowercase: false,
        require_numbers: false,
        require_symbols: true,
        temporary_password_validity_days: 7,
    };
    let err = validate_password("nosymbols", &policy).unwrap_err();
    assert_eq!(err.code(), "InvalidPasswordException");
    assert!(validate_password("has!symbol", &policy).is_ok());
}

#[test]
fn session_token_is_uuid_format() {
    let session = Uuid::new_v4().to_string();
    // UUID v4 format: 8-4-4-4-12 hex chars
    assert_eq!(session.len(), 36);
    let parts: Vec<&str> = session.split('-').collect();
    assert_eq!(parts.len(), 5);
}

#[test]
fn confirmation_code_is_six_digits() {
    for _ in 0..100 {
        let code = generate_confirmation_code();
        assert_eq!(code.len(), 6, "Code should be 6 chars: {code}");
        assert!(
            code.chars().all(|c| c.is_ascii_digit()),
            "Code should be all digits: {code}"
        );
    }
}

#[test]
fn confirmation_code_uniqueness() {
    let code1 = generate_confirmation_code();
    // Generate many codes and check we get at least some different ones
    let mut found_different = false;
    for _ in 0..20 {
        if generate_confirmation_code() != code1 {
            found_different = true;
            break;
        }
    }
    assert!(found_different, "Codes should vary across calls");
}

#[test]
fn access_token_lookup() {
    let state = std::sync::Arc::new(parking_lot::RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new(
            "123456789012",
            "us-east-1",
            "http://localhost:4569",
        ),
    ));
    {
        let mut _mas_w = state.write();
        let s: &mut crate::state::CognitoState = _mas_w.default_mut();
        s.access_tokens.insert(
            "test-access-token".to_string(),
            AccessTokenData {
                user_pool_id: "us-east-1_TestPool1".to_string(),
                username: "testuser".to_string(),
                client_id: "testclient123".to_string(),
                issued_at: Utc::now(),
            },
        );
    }

    let _mas = state.read();
    let s = _mas.default_ref();
    let token_data = s.access_tokens.get("test-access-token");
    assert!(token_data.is_some());
    let data = token_data.unwrap();
    assert_eq!(data.user_pool_id, "us-east-1_TestPool1");
    assert_eq!(data.username, "testuser");
    assert_eq!(data.client_id, "testclient123");

    // Non-existent token returns None
    assert!(!s.access_tokens.contains_key("nonexistent"));
}

#[test]
fn group_name_uniqueness() {
    let state = std::sync::Arc::new(parking_lot::RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new(
            "123456789012",
            "us-east-1",
            "http://localhost:4569",
        ),
    ));
    let svc = CognitoService::new(state.clone());

    // Create a pool first
    let create_pool_req = AwsRequest {
        service: "cognito-idp".to_string(),
        action: "CreateUserPool".to_string(),
        region: "us-east-1".to_string(),
        account_id: "123456789012".to_string(),
        request_id: "test".to_string(),
        headers: http::HeaderMap::new(),
        query_params: std::collections::HashMap::new(),
        body: bytes::Bytes::from(
            serde_json::to_string(&json!({ "PoolName": "test-pool" })).unwrap(),
        ),
        body_stream: parking_lot::Mutex::new(None),
        path_segments: vec![],
        raw_path: "/".to_string(),
        raw_query: String::new(),
        method: http::Method::POST,
        is_query_protocol: false,
        access_key_id: None,
        principal: None,
    };
    let resp = block_on(svc.create_user_pool(&create_pool_req)).unwrap();
    let resp_json: Value =
        serde_json::from_str(core::str::from_utf8(resp.body.expect_bytes()).unwrap()).unwrap();
    let pool_id = resp_json["UserPool"]["Id"].as_str().unwrap().to_string();

    // Create a group
    let create_group_req = AwsRequest {
        service: "cognito-idp".to_string(),
        action: "CreateGroup".to_string(),
        region: "us-east-1".to_string(),
        account_id: "123456789012".to_string(),
        request_id: "test".to_string(),
        headers: http::HeaderMap::new(),
        query_params: std::collections::HashMap::new(),
        body: bytes::Bytes::from(
            serde_json::to_string(&json!({
                "UserPoolId": pool_id,
                "GroupName": "admins"
            }))
            .unwrap(),
        ),
        body_stream: parking_lot::Mutex::new(None),
        path_segments: vec![],
        raw_path: "/".to_string(),
        raw_query: String::new(),
        method: http::Method::POST,
        is_query_protocol: false,
        access_key_id: None,
        principal: None,
    };
    let result = svc.create_group(&create_group_req);
    assert!(result.is_ok());

    // Creating the same group again should fail with GroupExistsException
    let result = svc.create_group(&create_group_req);
    match result {
        Err(e) => {
            let msg = format!("{e:?}");
            assert!(
                msg.contains("GroupExistsException"),
                "Should be GroupExistsException: {msg}"
            );
        }
        Ok(_) => panic!("Expected GroupExistsException"),
    }
}

#[test]
fn user_group_association() {
    let state = std::sync::Arc::new(parking_lot::RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new(
            "123456789012",
            "us-east-1",
            "http://localhost:4569",
        ),
    ));
    let svc = CognitoService::new(state.clone());

    // Create a pool
    let create_pool_req = AwsRequest {
        service: "cognito-idp".to_string(),
        action: "CreateUserPool".to_string(),
        region: "us-east-1".to_string(),
        account_id: "123456789012".to_string(),
        request_id: "test".to_string(),
        headers: http::HeaderMap::new(),
        query_params: std::collections::HashMap::new(),
        body: bytes::Bytes::from(
            serde_json::to_string(&json!({ "PoolName": "test-pool" })).unwrap(),
        ),
        body_stream: parking_lot::Mutex::new(None),
        path_segments: vec![],
        raw_path: "/".to_string(),
        raw_query: String::new(),
        method: http::Method::POST,
        is_query_protocol: false,
        access_key_id: None,
        principal: None,
    };
    let resp = block_on(svc.create_user_pool(&create_pool_req)).unwrap();
    let resp_json: Value =
        serde_json::from_str(core::str::from_utf8(resp.body.expect_bytes()).unwrap()).unwrap();
    let pool_id = resp_json["UserPool"]["Id"].as_str().unwrap().to_string();

    // Create a user
    let create_user_req = AwsRequest {
        service: "cognito-idp".to_string(),
        action: "AdminCreateUser".to_string(),
        region: "us-east-1".to_string(),
        account_id: "123456789012".to_string(),
        request_id: "test".to_string(),
        headers: http::HeaderMap::new(),
        query_params: std::collections::HashMap::new(),
        body: bytes::Bytes::from(
            serde_json::to_string(&json!({
                "UserPoolId": pool_id,
                "Username": "testuser"
            }))
            .unwrap(),
        ),
        body_stream: parking_lot::Mutex::new(None),
        path_segments: vec![],
        raw_path: "/".to_string(),
        raw_query: String::new(),
        method: http::Method::POST,
        is_query_protocol: false,
        access_key_id: None,
        principal: None,
    };
    block_on(svc.admin_create_user(&create_user_req)).unwrap();

    // Create a group
    let create_group_req = AwsRequest {
        service: "cognito-idp".to_string(),
        action: "CreateGroup".to_string(),
        region: "us-east-1".to_string(),
        account_id: "123456789012".to_string(),
        request_id: "test".to_string(),
        headers: http::HeaderMap::new(),
        query_params: std::collections::HashMap::new(),
        body: bytes::Bytes::from(
            serde_json::to_string(&json!({
                "UserPoolId": pool_id,
                "GroupName": "admins"
            }))
            .unwrap(),
        ),
        body_stream: parking_lot::Mutex::new(None),
        path_segments: vec![],
        raw_path: "/".to_string(),
        raw_query: String::new(),
        method: http::Method::POST,
        is_query_protocol: false,
        access_key_id: None,
        principal: None,
    };
    svc.create_group(&create_group_req).unwrap();

    // Add user to group
    let add_req = AwsRequest {
        service: "cognito-idp".to_string(),
        action: "AdminAddUserToGroup".to_string(),
        region: "us-east-1".to_string(),
        account_id: "123456789012".to_string(),
        request_id: "test".to_string(),
        headers: http::HeaderMap::new(),
        query_params: std::collections::HashMap::new(),
        body: bytes::Bytes::from(
            serde_json::to_string(&json!({
                "UserPoolId": pool_id,
                "Username": "testuser",
                "GroupName": "admins"
            }))
            .unwrap(),
        ),
        body_stream: parking_lot::Mutex::new(None),
        path_segments: vec![],
        raw_path: "/".to_string(),
        raw_query: String::new(),
        method: http::Method::POST,
        is_query_protocol: false,
        access_key_id: None,
        principal: None,
    };
    svc.admin_add_user_to_group(&add_req).unwrap();

    // Verify membership via state
    {
        let _mas = state.read();
        let s = _mas.default_ref();
        let groups = s.user_groups.get(&pool_id).unwrap();
        let user_groups = groups.get("testuser").unwrap();
        assert!(user_groups.contains(&"admins".to_string()));
    }

    // Remove user from group
    let remove_req = AwsRequest {
        service: "cognito-idp".to_string(),
        action: "AdminRemoveUserFromGroup".to_string(),
        region: "us-east-1".to_string(),
        account_id: "123456789012".to_string(),
        request_id: "test".to_string(),
        headers: http::HeaderMap::new(),
        query_params: std::collections::HashMap::new(),
        body: bytes::Bytes::from(
            serde_json::to_string(&json!({
                "UserPoolId": pool_id,
                "Username": "testuser",
                "GroupName": "admins"
            }))
            .unwrap(),
        ),
        body_stream: parking_lot::Mutex::new(None),
        path_segments: vec![],
        raw_path: "/".to_string(),
        raw_query: String::new(),
        method: http::Method::POST,
        is_query_protocol: false,
        access_key_id: None,
        principal: None,
    };
    svc.admin_remove_user_from_group(&remove_req).unwrap();

    // Verify no longer in group
    {
        let _mas = state.read();
        let s = _mas.default_ref();
        let groups = s.user_groups.get(&pool_id).unwrap();
        let user_groups = groups.get("testuser").unwrap();
        assert!(!user_groups.contains(&"admins".to_string()));
    }
}

#[test]
fn self_service_get_user_via_access_token() {
    let state = std::sync::Arc::new(parking_lot::RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new(
            "123456789012",
            "us-east-1",
            "http://localhost:4569",
        ),
    ));
    let svc = CognitoService::new(state.clone());

    // Create pool
    let req = AwsRequest {
        service: "cognito-idp".to_string(),
        action: "CreateUserPool".to_string(),
        region: "us-east-1".to_string(),
        account_id: "123456789012".to_string(),
        request_id: "test".to_string(),
        headers: http::HeaderMap::new(),
        query_params: std::collections::HashMap::new(),
        body: bytes::Bytes::from(r#"{"PoolName":"test"}"#),
        body_stream: parking_lot::Mutex::new(None),
        path_segments: vec![],
        raw_path: "/".to_string(),
        raw_query: String::new(),
        method: http::Method::POST,
        is_query_protocol: false,
        access_key_id: None,
        principal: None,
    };
    let pool_resp = block_on(svc.create_user_pool(&req)).unwrap();
    let pool_json: Value =
        serde_json::from_str(core::str::from_utf8(pool_resp.body.expect_bytes()).unwrap()).unwrap();
    let pool_id = pool_json["UserPool"]["Id"].as_str().unwrap().to_string();

    // Create user
    let req = AwsRequest {
        service: "cognito-idp".to_string(),
        action: "AdminCreateUser".to_string(),
        region: "us-east-1".to_string(),
        account_id: "123456789012".to_string(),
        request_id: "test".to_string(),
        headers: http::HeaderMap::new(),
        query_params: std::collections::HashMap::new(),
        body: bytes::Bytes::from(
            serde_json::to_string(&json!({
                "UserPoolId": pool_id,
                "Username": "selfuser",
                "UserAttributes": [
                    {"Name": "email", "Value": "self@example.com"}
                ]
            }))
            .unwrap(),
        ),
        body_stream: parking_lot::Mutex::new(None),
        path_segments: vec![],
        raw_path: "/".to_string(),
        raw_query: String::new(),
        method: http::Method::POST,
        is_query_protocol: false,
        access_key_id: None,
        principal: None,
    };
    block_on(svc.admin_create_user(&req)).unwrap();

    // Manually insert an access token
    {
        let mut _mas_w = state.write();
        let s = _mas_w.default_mut();
        s.access_tokens.insert(
            "test-access-token".to_string(),
            crate::state::AccessTokenData {
                user_pool_id: pool_id.clone(),
                username: "selfuser".to_string(),
                client_id: "test-client".to_string(),
                issued_at: Utc::now(),
            },
        );
    }

    // GetUser with valid token
    let req = AwsRequest {
        service: "cognito-idp".to_string(),
        action: "GetUser".to_string(),
        region: "us-east-1".to_string(),
        account_id: "123456789012".to_string(),
        request_id: "test".to_string(),
        headers: http::HeaderMap::new(),
        query_params: std::collections::HashMap::new(),
        body: bytes::Bytes::from(
            serde_json::to_string(&json!({"AccessToken": "test-access-token"})).unwrap(),
        ),
        body_stream: parking_lot::Mutex::new(None),
        path_segments: vec![],
        raw_path: "/".to_string(),
        raw_query: String::new(),
        method: http::Method::POST,
        is_query_protocol: false,
        access_key_id: None,
        principal: None,
    };
    let resp = svc.get_user(&req).unwrap();
    let resp_json: Value =
        serde_json::from_str(core::str::from_utf8(resp.body.expect_bytes()).unwrap()).unwrap();
    assert_eq!(resp_json["Username"], "selfuser");

    // GetUser with invalid token
    let req = AwsRequest {
        service: "cognito-idp".to_string(),
        action: "GetUser".to_string(),
        region: "us-east-1".to_string(),
        account_id: "123456789012".to_string(),
        request_id: "test".to_string(),
        headers: http::HeaderMap::new(),
        query_params: std::collections::HashMap::new(),
        body: bytes::Bytes::from(
            serde_json::to_string(&json!({"AccessToken": "invalid-token"})).unwrap(),
        ),
        body_stream: parking_lot::Mutex::new(None),
        path_segments: vec![],
        raw_path: "/".to_string(),
        raw_query: String::new(),
        method: http::Method::POST,
        is_query_protocol: false,
        access_key_id: None,
        principal: None,
    };
    match svc.get_user(&req) {
        Err(e) => assert_eq!(e.code(), "NotAuthorizedException"),
        Ok(_) => panic!("Expected NotAuthorizedException"),
    }
}

#[test]
fn self_service_delete_user_cleans_up_tokens() {
    let state = std::sync::Arc::new(parking_lot::RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new(
            "123456789012",
            "us-east-1",
            "http://localhost:4569",
        ),
    ));
    let svc = CognitoService::new(state.clone());

    // Create pool
    let req = AwsRequest {
        service: "cognito-idp".to_string(),
        action: "CreateUserPool".to_string(),
        region: "us-east-1".to_string(),
        account_id: "123456789012".to_string(),
        request_id: "test".to_string(),
        headers: http::HeaderMap::new(),
        query_params: std::collections::HashMap::new(),
        body: bytes::Bytes::from(r#"{"PoolName":"test"}"#),
        body_stream: parking_lot::Mutex::new(None),
        path_segments: vec![],
        raw_path: "/".to_string(),
        raw_query: String::new(),
        method: http::Method::POST,
        is_query_protocol: false,
        access_key_id: None,
        principal: None,
    };
    let pool_resp = block_on(svc.create_user_pool(&req)).unwrap();
    let pool_json: Value =
        serde_json::from_str(core::str::from_utf8(pool_resp.body.expect_bytes()).unwrap()).unwrap();
    let pool_id = pool_json["UserPool"]["Id"].as_str().unwrap().to_string();

    // Create user
    let req = AwsRequest {
        service: "cognito-idp".to_string(),
        action: "AdminCreateUser".to_string(),
        region: "us-east-1".to_string(),
        account_id: "123456789012".to_string(),
        request_id: "test".to_string(),
        headers: http::HeaderMap::new(),
        query_params: std::collections::HashMap::new(),
        body: bytes::Bytes::from(
            serde_json::to_string(&json!({
                "UserPoolId": pool_id,
                "Username": "deluser"
            }))
            .unwrap(),
        ),
        body_stream: parking_lot::Mutex::new(None),
        path_segments: vec![],
        raw_path: "/".to_string(),
        raw_query: String::new(),
        method: http::Method::POST,
        is_query_protocol: false,
        access_key_id: None,
        principal: None,
    };
    block_on(svc.admin_create_user(&req)).unwrap();

    // Insert access token and refresh token
    {
        let mut _mas_w = state.write();
        let s = _mas_w.default_mut();
        s.access_tokens.insert(
            "del-token".to_string(),
            crate::state::AccessTokenData {
                user_pool_id: pool_id.clone(),
                username: "deluser".to_string(),
                client_id: "test-client".to_string(),
                issued_at: Utc::now(),
            },
        );
        s.refresh_tokens.insert(
            "del-refresh".to_string(),
            crate::state::RefreshTokenData {
                user_pool_id: pool_id.clone(),
                username: "deluser".to_string(),
                client_id: "test-client".to_string(),
                issued_at: Utc::now(),
            },
        );
    }

    // Delete user via self-service
    let req = AwsRequest {
        service: "cognito-idp".to_string(),
        action: "DeleteUser".to_string(),
        region: "us-east-1".to_string(),
        account_id: "123456789012".to_string(),
        request_id: "test".to_string(),
        headers: http::HeaderMap::new(),
        query_params: std::collections::HashMap::new(),
        body: bytes::Bytes::from(
            serde_json::to_string(&json!({"AccessToken": "del-token"})).unwrap(),
        ),
        body_stream: parking_lot::Mutex::new(None),
        path_segments: vec![],
        raw_path: "/".to_string(),
        raw_query: String::new(),
        method: http::Method::POST,
        is_query_protocol: false,
        access_key_id: None,
        principal: None,
    };
    svc.delete_user(&req).unwrap();

    // Verify cleanup
    let _mas = state.read();
    let s = _mas.default_ref();
    assert!(s.access_tokens.is_empty());
    assert!(s.refresh_tokens.is_empty());
    assert!(s
        .users
        .get(&pool_id)
        .and_then(|u| u.get("deluser"))
        .is_none());
}

#[test]
fn verify_user_attribute_with_correct_code() {
    let state = std::sync::Arc::new(parking_lot::RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new(
            "123456789012",
            "us-east-1",
            "http://localhost:4569",
        ),
    ));
    let svc = CognitoService::new(state.clone());

    // Create pool
    let req = AwsRequest {
        service: "cognito-idp".to_string(),
        action: "CreateUserPool".to_string(),
        region: "us-east-1".to_string(),
        account_id: "123456789012".to_string(),
        request_id: "test".to_string(),
        headers: http::HeaderMap::new(),
        query_params: std::collections::HashMap::new(),
        body: bytes::Bytes::from(r#"{"PoolName":"test"}"#),
        body_stream: parking_lot::Mutex::new(None),
        path_segments: vec![],
        raw_path: "/".to_string(),
        raw_query: String::new(),
        method: http::Method::POST,
        is_query_protocol: false,
        access_key_id: None,
        principal: None,
    };
    let pool_resp = block_on(svc.create_user_pool(&req)).unwrap();
    let pool_json: Value =
        serde_json::from_str(core::str::from_utf8(pool_resp.body.expect_bytes()).unwrap()).unwrap();
    let pool_id = pool_json["UserPool"]["Id"].as_str().unwrap().to_string();

    // Create user with email
    let req = AwsRequest {
        service: "cognito-idp".to_string(),
        action: "AdminCreateUser".to_string(),
        region: "us-east-1".to_string(),
        account_id: "123456789012".to_string(),
        request_id: "test".to_string(),
        headers: http::HeaderMap::new(),
        query_params: std::collections::HashMap::new(),
        body: bytes::Bytes::from(
            serde_json::to_string(&json!({
                "UserPoolId": pool_id,
                "Username": "verifyuser",
                "UserAttributes": [{"Name": "email", "Value": "verify@example.com"}]
            }))
            .unwrap(),
        ),
        body_stream: parking_lot::Mutex::new(None),
        path_segments: vec![],
        raw_path: "/".to_string(),
        raw_query: String::new(),
        method: http::Method::POST,
        is_query_protocol: false,
        access_key_id: None,
        principal: None,
    };
    block_on(svc.admin_create_user(&req)).unwrap();

    // Insert access token
    {
        let mut _mas_w = state.write();
        let s = _mas_w.default_mut();
        s.access_tokens.insert(
            "verify-token".to_string(),
            crate::state::AccessTokenData {
                user_pool_id: pool_id.clone(),
                username: "verifyuser".to_string(),
                client_id: "test-client".to_string(),
                issued_at: Utc::now(),
            },
        );
    }

    // Get verification code
    let req = AwsRequest {
        service: "cognito-idp".to_string(),
        action: "GetUserAttributeVerificationCode".to_string(),
        region: "us-east-1".to_string(),
        account_id: "123456789012".to_string(),
        request_id: "test".to_string(),
        headers: http::HeaderMap::new(),
        query_params: std::collections::HashMap::new(),
        body: bytes::Bytes::from(
            serde_json::to_string(&json!({
                "AccessToken": "verify-token",
                "AttributeName": "email"
            }))
            .unwrap(),
        ),
        body_stream: parking_lot::Mutex::new(None),
        path_segments: vec![],
        raw_path: "/".to_string(),
        raw_query: String::new(),
        method: http::Method::POST,
        is_query_protocol: false,
        access_key_id: None,
        principal: None,
    };
    let resp = svc.get_user_attribute_verification_code(&req).unwrap();
    let resp_json: Value =
        serde_json::from_str(core::str::from_utf8(resp.body.expect_bytes()).unwrap()).unwrap();
    assert_eq!(resp_json["CodeDeliveryDetails"]["DeliveryMedium"], "EMAIL");
    assert_eq!(resp_json["CodeDeliveryDetails"]["AttributeName"], "email");

    // Read the code from state
    let code = {
        let _mas = state.read();
        let s = _mas.default_ref();
        let user = s.users.get(&pool_id).unwrap().get("verifyuser").unwrap();
        user.attribute_verification_codes
            .get("email")
            .unwrap()
            .clone()
    };

    // Verify with correct code
    let req = AwsRequest {
        service: "cognito-idp".to_string(),
        action: "VerifyUserAttribute".to_string(),
        region: "us-east-1".to_string(),
        account_id: "123456789012".to_string(),
        request_id: "test".to_string(),
        headers: http::HeaderMap::new(),
        query_params: std::collections::HashMap::new(),
        body: bytes::Bytes::from(
            serde_json::to_string(&json!({
                "AccessToken": "verify-token",
                "AttributeName": "email",
                "Code": code
            }))
            .unwrap(),
        ),
        body_stream: parking_lot::Mutex::new(None),
        path_segments: vec![],
        raw_path: "/".to_string(),
        raw_query: String::new(),
        method: http::Method::POST,
        is_query_protocol: false,
        access_key_id: None,
        principal: None,
    };
    svc.verify_user_attribute(&req).unwrap();

    // Verify email_verified is set
    let _mas = state.read();
    let s = _mas.default_ref();
    let user = s.users.get(&pool_id).unwrap().get("verifyuser").unwrap();
    let email_verified = user
        .attributes
        .iter()
        .find(|a| a.name == "email_verified")
        .unwrap();
    assert_eq!(email_verified.value, "true");

    // Verify with wrong code should fail
    drop(_mas);
    // First get a new code
    let req = AwsRequest {
        service: "cognito-idp".to_string(),
        action: "GetUserAttributeVerificationCode".to_string(),
        region: "us-east-1".to_string(),
        account_id: "123456789012".to_string(),
        request_id: "test".to_string(),
        headers: http::HeaderMap::new(),
        query_params: std::collections::HashMap::new(),
        body: bytes::Bytes::from(
            serde_json::to_string(&json!({
                "AccessToken": "verify-token",
                "AttributeName": "email"
            }))
            .unwrap(),
        ),
        body_stream: parking_lot::Mutex::new(None),
        path_segments: vec![],
        raw_path: "/".to_string(),
        raw_query: String::new(),
        method: http::Method::POST,
        is_query_protocol: false,
        access_key_id: None,
        principal: None,
    };
    svc.get_user_attribute_verification_code(&req).unwrap();

    let req = AwsRequest {
        service: "cognito-idp".to_string(),
        action: "VerifyUserAttribute".to_string(),
        region: "us-east-1".to_string(),
        account_id: "123456789012".to_string(),
        request_id: "test".to_string(),
        headers: http::HeaderMap::new(),
        query_params: std::collections::HashMap::new(),
        body: bytes::Bytes::from(
            serde_json::to_string(&json!({
                "AccessToken": "verify-token",
                "AttributeName": "email",
                "Code": "000000"
            }))
            .unwrap(),
        ),
        body_stream: parking_lot::Mutex::new(None),
        path_segments: vec![],
        raw_path: "/".to_string(),
        raw_query: String::new(),
        method: http::Method::POST,
        is_query_protocol: false,
        access_key_id: None,
        principal: None,
    };
    match svc.verify_user_attribute(&req) {
        Err(e) => assert_eq!(e.code(), "CodeMismatchException"),
        Ok(_) => panic!("Expected CodeMismatchException"),
    }
}

#[test]
fn totp_secret_format() {
    let secret = generate_totp_secret();
    assert_eq!(secret.len(), 32, "TOTP secret should be 32 chars: {secret}");
    assert!(
        secret
            .chars()
            .all(|c| "ABCDEFGHIJKLMNOPQRSTUVWXYZ234567".contains(c)),
        "TOTP secret should be base32: {secret}"
    );
}

#[test]
fn totp_secret_uniqueness() {
    let s1 = generate_totp_secret();
    let s2 = generate_totp_secret();
    assert_ne!(s1, s2, "TOTP secrets should be unique");
}

#[test]
fn mfa_preference_storage() {
    use std::sync::Arc;

    let state = Arc::new(parking_lot::RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new(
            "123456789012",
            "us-east-1",
            "http://localhost:4569",
        ),
    ));
    let svc = CognitoService::new(state.clone());

    // Create a pool and user first
    let create_pool_req = AwsRequest {
        service: "cognito-idp".to_string(),
        action: "CreateUserPool".to_string(),
        region: "us-east-1".to_string(),
        account_id: "123456789012".to_string(),
        request_id: "test".to_string(),
        headers: http::HeaderMap::new(),
        query_params: std::collections::HashMap::new(),
        body: bytes::Bytes::from(
            serde_json::to_string(&json!({ "PoolName": "mfa-pool" })).unwrap(),
        ),
        body_stream: parking_lot::Mutex::new(None),
        path_segments: vec![],
        raw_path: "/".to_string(),
        raw_query: String::new(),
        method: http::Method::POST,
        is_query_protocol: false,
        access_key_id: None,
        principal: None,
    };
    let pool_resp = block_on(svc.create_user_pool(&create_pool_req)).unwrap();
    let pool_body: Value = serde_json::from_slice(pool_resp.body.expect_bytes()).unwrap();
    let pool_id = pool_body["UserPool"]["Id"].as_str().unwrap().to_string();

    let create_user_req = AwsRequest {
        service: "cognito-idp".to_string(),
        action: "AdminCreateUser".to_string(),
        region: "us-east-1".to_string(),
        account_id: "123456789012".to_string(),
        request_id: "test".to_string(),
        headers: http::HeaderMap::new(),
        query_params: std::collections::HashMap::new(),
        body: bytes::Bytes::from(
            serde_json::to_string(&json!({
                "UserPoolId": pool_id,
                "Username": "mfauser"
            }))
            .unwrap(),
        ),
        body_stream: parking_lot::Mutex::new(None),
        path_segments: vec![],
        raw_path: "/".to_string(),
        raw_query: String::new(),
        method: http::Method::POST,
        is_query_protocol: false,
        access_key_id: None,
        principal: None,
    };
    block_on(svc.admin_create_user(&create_user_req)).unwrap();

    // Set MFA preference via admin
    let set_pref_req = AwsRequest {
        service: "cognito-idp".to_string(),
        action: "AdminSetUserMFAPreference".to_string(),
        region: "us-east-1".to_string(),
        account_id: "123456789012".to_string(),
        request_id: "test".to_string(),
        headers: http::HeaderMap::new(),
        query_params: std::collections::HashMap::new(),
        body: bytes::Bytes::from(
            serde_json::to_string(&json!({
                "UserPoolId": pool_id,
                "Username": "mfauser",
                "SoftwareTokenMfaSettings": {
                    "Enabled": true,
                    "PreferredMfa": true
                },
                "SMSMfaSettings": {
                    "Enabled": false,
                    "PreferredMfa": false
                }
            }))
            .unwrap(),
        ),
        body_stream: parking_lot::Mutex::new(None),
        path_segments: vec![],
        raw_path: "/".to_string(),
        raw_query: String::new(),
        method: http::Method::POST,
        is_query_protocol: false,
        access_key_id: None,
        principal: None,
    };
    svc.admin_set_user_mfa_preference(&set_pref_req).unwrap();

    // Verify preferences were stored
    let _mas = state.read();
    let st = _mas.default_ref();
    let user = st.users.get(&pool_id).unwrap().get("mfauser").unwrap();
    let prefs = user.mfa_preferences.as_ref().unwrap();
    assert!(prefs.software_token_enabled);
    assert!(prefs.software_token_preferred);
    assert!(!prefs.sms_enabled);
    assert!(!prefs.sms_preferred);
}

fn make_req(action: &str, body: &str) -> AwsRequest {
    AwsRequest {
        service: "cognito-idp".to_string(),
        action: action.to_string(),
        region: "us-east-1".to_string(),
        account_id: "123456789012".to_string(),
        request_id: "test".to_string(),
        headers: http::HeaderMap::new(),
        query_params: std::collections::HashMap::new(),
        body: bytes::Bytes::from(body.to_string()),
        body_stream: parking_lot::Mutex::new(None),
        path_segments: vec![],
        raw_path: "/".to_string(),
        raw_query: String::new(),
        method: http::Method::POST,
        is_query_protocol: false,
        access_key_id: None,
        principal: None,
    }
}

fn setup_svc_with_pool() -> (CognitoService, String) {
    let state = std::sync::Arc::new(parking_lot::RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new(
            "123456789012",
            "us-east-1",
            "http://localhost:4569",
        ),
    ));
    let svc = CognitoService::new(state);
    let req = make_req("CreateUserPool", r#"{"PoolName":"test"}"#);
    let resp = block_on(svc.create_user_pool(&req)).unwrap();
    let resp_body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let pool_id = resp_body["UserPool"]["Id"].as_str().unwrap().to_string();
    (svc, pool_id)
}

#[test]
fn list_users_requires_user_pool_id() {
    let (svc, _) = setup_svc_with_pool();

    for body in [r#"{}"#, ""] {
        let req = make_req("ListUsers", body);
        match svc.list_users(&req) {
            Err(e) => assert_eq!(e.code(), "InvalidParameterException"),
            Ok(_) => panic!("Expected InvalidParameterException for body {body:?}"),
        }
    }
}

#[test]
fn list_users_validates_limit_bounds() {
    let (svc, pool_id) = setup_svc_with_pool();

    for limit in [0, 61] {
        let body = serde_json::to_string(&json!({
            "UserPoolId": pool_id,
            "Limit": limit,
        }))
        .unwrap();
        let req = make_req("ListUsers", &body);
        match svc.list_users(&req) {
            Err(e) => assert_eq!(e.code(), "InvalidParameterException"),
            Ok(_) => panic!("Expected InvalidParameterException for limit {limit}"),
        }
    }
}

#[test]
fn list_users_validates_optional_field_lengths() {
    let (svc, pool_id) = setup_svc_with_pool();

    let long_filter = "a".repeat(257);
    let body = serde_json::to_string(&json!({
        "UserPoolId": pool_id,
        "Filter": long_filter,
    }))
    .unwrap();
    let req = make_req("ListUsers", &body);
    match svc.list_users(&req) {
        Err(e) => assert_eq!(e.code(), "InvalidParameterException"),
        Ok(_) => panic!("Expected InvalidParameterException for oversized filter"),
    }

    let body = serde_json::to_string(&json!({
        "UserPoolId": pool_id,
        "PaginationToken": "",
    }))
    .unwrap();
    let req = make_req("ListUsers", &body);
    match svc.list_users(&req) {
        Err(e) => assert_eq!(e.code(), "InvalidParameterException"),
        Ok(_) => panic!("Expected InvalidParameterException for empty pagination token"),
    }
}

#[test]
fn list_users_validates_user_pool_id_length() {
    let (svc, _) = setup_svc_with_pool();

    let body = serde_json::to_string(&json!({
        "UserPoolId": "",
    }))
    .unwrap();
    let req = make_req("ListUsers", &body);
    match svc.list_users(&req) {
        Err(e) => assert_eq!(e.code(), "InvalidParameterException"),
        Ok(_) => panic!("Expected InvalidParameterException for empty UserPoolId"),
    }

    let long_pool_id = format!("{}suffix", "a".repeat(50));
    let body = serde_json::to_string(&json!({
        "UserPoolId": long_pool_id,
    }))
    .unwrap();
    let req = make_req("ListUsers", &body);
    match svc.list_users(&req) {
        Err(e) => assert_eq!(e.code(), "InvalidParameterException"),
        Ok(_) => panic!("Expected InvalidParameterException for oversized UserPoolId"),
    }
}

#[test]
fn identity_provider_name_uniqueness() {
    let (svc, pool_id) = setup_svc_with_pool();

    let body = serde_json::to_string(&json!({
        "UserPoolId": pool_id,
        "ProviderName": "MyGoogle",
        "ProviderType": "Google",
        "ProviderDetails": {"client_id": "123", "client_secret": "secret"}
    }))
    .unwrap();
    let req = make_req("CreateIdentityProvider", &body);
    svc.create_identity_provider(&req).unwrap();

    // Duplicate name should fail
    let req2 = make_req("CreateIdentityProvider", &body);
    match svc.create_identity_provider(&req2) {
        Err(e) => assert_eq!(e.code(), "DuplicateProviderException"),
        Ok(_) => panic!("Expected DuplicateProviderException"),
    }
}

#[test]
fn identity_provider_type_validation() {
    let (svc, pool_id) = setup_svc_with_pool();

    let body = serde_json::to_string(&json!({
        "UserPoolId": pool_id,
        "ProviderName": "MyInvalid",
        "ProviderType": "InvalidType",
        "ProviderDetails": {}
    }))
    .unwrap();
    let req = make_req("CreateIdentityProvider", &body);
    match svc.create_identity_provider(&req) {
        Err(e) => assert_eq!(e.code(), "InvalidParameterException"),
        Ok(_) => panic!("Expected InvalidParameterException"),
    }

    // Valid types should all work
    for provider_type in &[
        "SAML",
        "Facebook",
        "Google",
        "LoginWithAmazon",
        "SignInWithApple",
        "OIDC",
    ] {
        let body = serde_json::to_string(&json!({
            "UserPoolId": pool_id,
            "ProviderName": format!("prov_{provider_type}"),
            "ProviderType": provider_type,
            "ProviderDetails": {}
        }))
        .unwrap();
        let req = make_req("CreateIdentityProvider", &body);
        assert!(
            svc.create_identity_provider(&req).is_ok(),
            "ProviderType {provider_type} should be valid"
        );
    }
}

#[test]
fn resource_server_identifier_uniqueness() {
    let (svc, pool_id) = setup_svc_with_pool();

    let body = serde_json::to_string(&json!({
        "UserPoolId": pool_id,
        "Identifier": "https://api.example.com",
        "Name": "My API",
        "Scopes": [{"ScopeName": "read", "ScopeDescription": "Read access"}]
    }))
    .unwrap();
    let req = make_req("CreateResourceServer", &body);
    svc.create_resource_server(&req).unwrap();

    // Duplicate identifier should fail
    let body2 = serde_json::to_string(&json!({
        "UserPoolId": pool_id,
        "Identifier": "https://api.example.com",
        "Name": "My API 2",
        "Scopes": []
    }))
    .unwrap();
    let req2 = make_req("CreateResourceServer", &body2);
    match svc.create_resource_server(&req2) {
        Err(e) => assert_eq!(e.code(), "InvalidParameterException"),
        Ok(_) => panic!("Expected InvalidParameterException for duplicate identifier"),
    }
}

#[test]
fn domain_uniqueness() {
    let (svc, pool_id) = setup_svc_with_pool();

    let body = serde_json::to_string(&json!({
        "UserPoolId": pool_id,
        "Domain": "my-unique-domain"
    }))
    .unwrap();
    let req = make_req("CreateUserPoolDomain", &body);
    svc.create_user_pool_domain(&req).unwrap();

    // Duplicate domain should fail
    let body2 = serde_json::to_string(&json!({
        "UserPoolId": pool_id,
        "Domain": "my-unique-domain"
    }))
    .unwrap();
    let req2 = make_req("CreateUserPoolDomain", &body2);
    match svc.create_user_pool_domain(&req2) {
        Err(e) => assert_eq!(e.code(), "InvalidParameterException"),
        Ok(_) => panic!("Expected InvalidParameterException for duplicate domain"),
    }
}

fn setup_svc_with_pool_and_user() -> (CognitoService, String, String) {
    let (svc, pool_id) = setup_svc_with_pool();
    let body = serde_json::to_string(&json!({
        "UserPoolId": pool_id,
        "Username": "deviceuser",
        "TemporaryPassword": "Temp1234!"
    }))
    .unwrap();
    let req = make_req("AdminCreateUser", &body);
    block_on(svc.admin_create_user(&req)).unwrap();
    (svc, pool_id, "deviceuser".to_string())
}

#[test]
fn device_key_storage() {
    let (svc, pool_id, username) = setup_svc_with_pool_and_user();

    // Directly insert a device into the user's devices map
    {
        let mut mas = svc.state.write();
        let state = mas.default_mut();
        let user = state
            .users
            .get_mut(&pool_id)
            .unwrap()
            .get_mut(&username)
            .unwrap();
        user.devices.insert(
            "dev-key-1".to_string(),
            Device {
                device_key: "dev-key-1".to_string(),
                device_attributes: BTreeMap::new(),
                device_create_date: Utc::now(),
                device_last_modified_date: Utc::now(),
                device_last_authenticated_date: None,
                device_remembered_status: None,
            },
        );
    }

    // AdminGetDevice
    let body = serde_json::to_string(&json!({
        "UserPoolId": pool_id,
        "Username": username,
        "DeviceKey": "dev-key-1"
    }))
    .unwrap();
    let req = make_req("AdminGetDevice", &body);
    let resp = svc.admin_get_device(&req).unwrap();
    let resp_body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(resp_body["Device"]["DeviceKey"], "dev-key-1");

    // AdminForgetDevice
    let req = make_req("AdminForgetDevice", &body);
    svc.admin_forget_device(&req).unwrap();

    // Device should be gone
    let req = make_req("AdminGetDevice", &body);
    match svc.admin_get_device(&req) {
        Err(e) => assert_eq!(e.code(), "ResourceNotFoundException"),
        Ok(_) => panic!("Expected ResourceNotFoundException"),
    }
}

#[test]
fn tag_management() {
    let (svc, pool_id) = setup_svc_with_pool();
    let arn = {
        let mas = svc.state.read();
        mas.default_ref()
            .user_pools
            .get(&pool_id)
            .unwrap()
            .arn
            .clone()
    };

    // Tag
    let body = serde_json::to_string(&json!({
        "ResourceArn": arn,
        "Tags": {"env": "test", "team": "core"}
    }))
    .unwrap();
    let req = make_req("TagResource", &body);
    svc.tag_resource(&req).unwrap();

    // List
    let body = serde_json::to_string(&json!({"ResourceArn": arn})).unwrap();
    let req = make_req("ListTagsForResource", &body);
    let resp = svc.list_tags_for_resource(&req).unwrap();
    let resp_body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(resp_body["Tags"]["env"], "test");
    assert_eq!(resp_body["Tags"]["team"], "core");

    // Untag
    let body = serde_json::to_string(&json!({
        "ResourceArn": arn,
        "TagKeys": ["team"]
    }))
    .unwrap();
    let req = make_req("UntagResource", &body);
    svc.untag_resource(&req).unwrap();

    // Verify
    let body = serde_json::to_string(&json!({"ResourceArn": arn})).unwrap();
    let req = make_req("ListTagsForResource", &body);
    let resp = svc.list_tags_for_resource(&req).unwrap();
    let resp_body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(resp_body["Tags"]["env"], "test");
    assert!(resp_body["Tags"]["team"].is_null());
}

#[test]
fn import_job_creation() {
    let (svc, pool_id) = setup_svc_with_pool();

    let body = serde_json::to_string(&json!({
        "UserPoolId": pool_id,
        "JobName": "my-import",
        "CloudWatchLogsRoleArn": "arn:aws:iam::123456789012:role/CognitoImport"
    }))
    .unwrap();
    let req = make_req("CreateUserImportJob", &body);
    let resp = svc.create_user_import_job(&req).unwrap();
    let resp_body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let job = &resp_body["UserImportJob"];
    assert_eq!(job["JobName"], "my-import");
    assert_eq!(job["Status"], "Created");
    assert!(job["JobId"].as_str().unwrap().starts_with("import-"));
    assert!(job["PreSignedUrl"].as_str().is_some());

    // Describe
    let job_id = job["JobId"].as_str().unwrap();
    let body = serde_json::to_string(&json!({
        "UserPoolId": pool_id,
        "JobId": job_id
    }))
    .unwrap();
    let req = make_req("DescribeUserImportJob", &body);
    let resp = svc.describe_user_import_job(&req).unwrap();
    let resp_body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(resp_body["UserImportJob"]["JobName"], "my-import");

    // List
    let body = serde_json::to_string(&json!({
        "UserPoolId": pool_id,
        "MaxResults": 10
    }))
    .unwrap();
    let req = make_req("ListUserImportJobs", &body);
    let resp = svc.list_user_import_jobs(&req).unwrap();
    let resp_body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(resp_body["UserImportJobs"].as_array().unwrap().len(), 1);
}

#[test]
fn auth_events_recorded_on_sign_up() {
    let state = std::sync::Arc::new(parking_lot::RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new(
            "123456789012",
            "us-east-1",
            "http://localhost:4569",
        ),
    ));
    let svc = CognitoService::new(state.clone());

    // Create pool and client
    let req = make_req("CreateUserPool", r#"{"PoolName": "evpool"}"#);
    let resp = block_on(svc.create_user_pool(&req)).unwrap();
    let resp_body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let pool_id = resp_body["UserPool"]["Id"].as_str().unwrap().to_string();

    let body = serde_json::to_string(&json!({
        "UserPoolId": pool_id,
        "ClientName": "evclient",
        "ExplicitAuthFlows": ["ALLOW_USER_PASSWORD_AUTH", "ALLOW_REFRESH_TOKEN_AUTH"]
    }))
    .unwrap();
    let req = make_req("CreateUserPoolClient", &body);
    let resp = svc.create_user_pool_client(&req).unwrap();
    let resp_body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let client_id = resp_body["UserPoolClient"]["ClientId"]
        .as_str()
        .unwrap()
        .to_string();

    // Sign up
    let body = serde_json::to_string(&json!({
        "ClientId": client_id,
        "Username": "testevuser",
        "Password": "P@ssw0rd!",
        "UserAttributes": [{"Name": "email", "Value": "test@example.com"}]
    }))
    .unwrap();
    let req = make_req("SignUp", &body);
    block_on(svc.sign_up(&req)).unwrap();

    // Check auth events
    let _mas = state.read();
    let st = _mas.default_ref();
    assert_eq!(st.auth_events.len(), 1);
    assert_eq!(st.auth_events[0].event_type, "SIGN_UP");
    assert_eq!(st.auth_events[0].username, "testevuser");
    assert!(st.auth_events[0].success);
}

#[test]
fn auth_events_recorded_on_sign_in_and_failure() {
    let state = std::sync::Arc::new(parking_lot::RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new(
            "123456789012",
            "us-east-1",
            "http://localhost:4569",
        ),
    ));
    let svc = CognitoService::new(state.clone());

    // Create pool, client, user
    let req = make_req("CreateUserPool", r#"{"PoolName": "authpool"}"#);
    let resp = block_on(svc.create_user_pool(&req)).unwrap();
    let resp_body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let pool_id = resp_body["UserPool"]["Id"].as_str().unwrap().to_string();

    let body = serde_json::to_string(&json!({
        "UserPoolId": pool_id,
        "ClientName": "authclient",
        "ExplicitAuthFlows": ["ALLOW_ADMIN_USER_PASSWORD_AUTH", "ALLOW_REFRESH_TOKEN_AUTH"]
    }))
    .unwrap();
    let req = make_req("CreateUserPoolClient", &body);
    let resp = svc.create_user_pool_client(&req).unwrap();
    let resp_body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let client_id = resp_body["UserPoolClient"]["ClientId"]
        .as_str()
        .unwrap()
        .to_string();

    // Create user and set permanent password
    let body = serde_json::to_string(&json!({
        "UserPoolId": pool_id,
        "Username": "authuser",
        "TemporaryPassword": "TempP@ss1!"
    }))
    .unwrap();
    let req = make_req("AdminCreateUser", &body);
    block_on(svc.admin_create_user(&req)).unwrap();

    let body = serde_json::to_string(&json!({
        "UserPoolId": pool_id,
        "Username": "authuser",
        "Password": "P@ssw0rd!",
        "Permanent": true
    }))
    .unwrap();
    let req = make_req("AdminSetUserPassword", &body);
    svc.admin_set_user_password(&req).unwrap();

    // Successful auth
    let body = serde_json::to_string(&json!({
        "UserPoolId": pool_id,
        "ClientId": client_id,
        "AuthFlow": "ADMIN_USER_PASSWORD_AUTH",
        "AuthParameters": {"USERNAME": "authuser", "PASSWORD": "P@ssw0rd!"}
    }))
    .unwrap();
    let req = make_req("AdminInitiateAuth", &body);
    block_on(svc.admin_initiate_auth(&req)).unwrap();

    // Failed auth
    let body = serde_json::to_string(&json!({
        "UserPoolId": pool_id,
        "ClientId": client_id,
        "AuthFlow": "ADMIN_USER_PASSWORD_AUTH",
        "AuthParameters": {"USERNAME": "authuser", "PASSWORD": "WrongPass!"}
    }))
    .unwrap();
    let req = make_req("AdminInitiateAuth", &body);
    let _ = block_on(svc.admin_initiate_auth(&req));

    // Check events
    let _mas = state.read();
    let st = _mas.default_ref();
    assert_eq!(st.auth_events.len(), 2);
    assert_eq!(st.auth_events[0].event_type, "SIGN_IN");
    assert!(st.auth_events[0].success);
    assert_eq!(st.auth_events[1].event_type, "SIGN_IN_FAILURE");
    assert!(!st.auth_events[1].success);
}

#[test]
fn auth_events_cleared_on_reset() {
    let state: crate::state::SharedCognitoState = std::sync::Arc::new(parking_lot::RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new(
            "123456789012",
            "us-east-1",
            "http://localhost:4569",
        ),
    ));
    state.write().default_mut().auth_events.push(AuthEvent {
        event_id: Uuid::new_v4().to_string(),
        event_type: "SIGN_UP".to_string(),
        username: "test".to_string(),
        user_pool_id: "pool".to_string(),
        client_id: None,
        timestamp: Utc::now(),
        success: true,
        feedback_value: None,
    });
    assert_eq!(state.read().default_ref().auth_events.len(), 1);
    state.write().default_mut().reset();
    assert!(state.read().default_ref().auth_events.is_empty());
}

#[test]
fn custom_auth_rejected_when_not_in_explicit_auth_flows() {
    let state = std::sync::Arc::new(parking_lot::RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new(
            "123456789012",
            "us-east-1",
            "http://localhost:4569",
        ),
    ));
    let svc = CognitoService::new(state.clone());

    // Create pool
    let req = make_req("CreateUserPool", r#"{"PoolName": "capool"}"#);
    let resp = block_on(svc.create_user_pool(&req)).unwrap();
    let resp_body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let pool_id = resp_body["UserPool"]["Id"].as_str().unwrap().to_string();

    // Create client WITHOUT ALLOW_CUSTOM_AUTH
    let body = serde_json::to_string(&json!({
        "UserPoolId": pool_id,
        "ClientName": "caclient",
        "ExplicitAuthFlows": ["ALLOW_USER_PASSWORD_AUTH"]
    }))
    .unwrap();
    let req = make_req("CreateUserPoolClient", &body);
    let resp = svc.create_user_pool_client(&req).unwrap();
    let resp_body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let client_id = resp_body["UserPoolClient"]["ClientId"]
        .as_str()
        .unwrap()
        .to_string();

    // Try CUSTOM_AUTH — should fail
    let body = serde_json::to_string(&json!({
        "ClientId": client_id,
        "AuthFlow": "CUSTOM_AUTH",
        "AuthParameters": {"USERNAME": "someuser"}
    }))
    .unwrap();
    let req = make_req("InitiateAuth", &body);
    let result = block_on(svc.initiate_auth(&req));
    let err = result.err().expect("Expected error for CUSTOM_AUTH");
    let err_str = format!("{err}");
    assert!(
        err_str.contains("CUSTOM_AUTH flow is not enabled"),
        "Expected CUSTOM_AUTH rejection, got: {err_str}"
    );
}

#[test]
fn custom_auth_fails_without_delivery_context() {
    let state = std::sync::Arc::new(parking_lot::RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new(
            "123456789012",
            "us-east-1",
            "http://localhost:4569",
        ),
    ));
    // No delivery context — no Lambda support
    let svc = CognitoService::new(state.clone());

    // Create pool and client
    let req = make_req("CreateUserPool", r#"{"PoolName": "capool2"}"#);
    let resp = block_on(svc.create_user_pool(&req)).unwrap();
    let resp_body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let pool_id = resp_body["UserPool"]["Id"].as_str().unwrap().to_string();

    let body = serde_json::to_string(&json!({
        "UserPoolId": pool_id,
        "ClientName": "caclient2",
        "ExplicitAuthFlows": ["ALLOW_CUSTOM_AUTH"]
    }))
    .unwrap();
    let req = make_req("CreateUserPoolClient", &body);
    let resp = svc.create_user_pool_client(&req).unwrap();
    let resp_body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let client_id = resp_body["UserPoolClient"]["ClientId"]
        .as_str()
        .unwrap()
        .to_string();

    // Create user
    let body = serde_json::to_string(&json!({
        "UserPoolId": pool_id,
        "Username": "customuser",
        "TemporaryPassword": "TempP@ss1!"
    }))
    .unwrap();
    let req = make_req("AdminCreateUser", &body);
    block_on(svc.admin_create_user(&req)).unwrap();

    let body = serde_json::to_string(&json!({
        "UserPoolId": pool_id,
        "Username": "customuser",
        "Password": "P@ssw0rd!",
        "Permanent": true
    }))
    .unwrap();
    let req = make_req("AdminSetUserPassword", &body);
    svc.admin_set_user_password(&req).unwrap();

    // Try CUSTOM_AUTH without delivery context
    let body = serde_json::to_string(&json!({
        "ClientId": client_id,
        "AuthFlow": "CUSTOM_AUTH",
        "AuthParameters": {"USERNAME": "customuser"}
    }))
    .unwrap();
    let req = make_req("InitiateAuth", &body);
    let err = block_on(svc.initiate_auth(&req))
        .err()
        .expect("Expected error for missing delivery context");
    let err_str = format!("{err}");
    assert!(
        err_str.contains("InvalidLambdaResponseException")
            || err_str.contains("DefineAuthChallenge"),
        "Expected Lambda error, got: {err_str}"
    );
}

#[test]
fn custom_auth_fails_without_define_trigger_configured() {
    let state = std::sync::Arc::new(parking_lot::RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new(
            "123456789012",
            "us-east-1",
            "http://localhost:4569",
        ),
    ));
    let delivery_bus = std::sync::Arc::new(fakecloud_core::delivery::DeliveryBus::new());
    let ctx = triggers::CognitoDeliveryContext::new(delivery_bus.clone());
    let svc = CognitoService::new(state.clone()).with_delivery(ctx);

    // Create pool WITHOUT DefineAuthChallenge Lambda configured
    let req = make_req("CreateUserPool", r#"{"PoolName": "capool3"}"#);
    let resp = block_on(svc.create_user_pool(&req)).unwrap();
    let resp_body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let pool_id = resp_body["UserPool"]["Id"].as_str().unwrap().to_string();

    let body = serde_json::to_string(&json!({
        "UserPoolId": pool_id,
        "ClientName": "caclient3",
        "ExplicitAuthFlows": ["ALLOW_CUSTOM_AUTH"]
    }))
    .unwrap();
    let req = make_req("CreateUserPoolClient", &body);
    let resp = svc.create_user_pool_client(&req).unwrap();
    let resp_body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let client_id = resp_body["UserPoolClient"]["ClientId"]
        .as_str()
        .unwrap()
        .to_string();

    // Create user
    let body = serde_json::to_string(&json!({
        "UserPoolId": pool_id,
        "Username": "customuser2",
        "TemporaryPassword": "TempP@ss1!"
    }))
    .unwrap();
    let req = make_req("AdminCreateUser", &body);
    block_on(svc.admin_create_user(&req)).unwrap();

    let body = serde_json::to_string(&json!({
        "UserPoolId": pool_id,
        "Username": "customuser2",
        "Password": "P@ssw0rd!",
        "Permanent": true
    }))
    .unwrap();
    let req = make_req("AdminSetUserPassword", &body);
    svc.admin_set_user_password(&req).unwrap();

    // CUSTOM_AUTH — has delivery context but no DefineAuthChallenge Lambda
    let body = serde_json::to_string(&json!({
        "ClientId": client_id,
        "AuthFlow": "CUSTOM_AUTH",
        "AuthParameters": {"USERNAME": "customuser2"}
    }))
    .unwrap();
    let req = make_req("InitiateAuth", &body);
    let err = block_on(svc.initiate_auth(&req))
        .err()
        .expect("Expected error for missing DefineAuthChallenge trigger");
    let err_str = format!("{err}");
    assert!(
        err_str.contains("DefineAuthChallenge"),
        "Expected DefineAuthChallenge error, got: {err_str}"
    );
}

#[test]
fn custom_challenge_response_fails_without_delivery_context() {
    let state = std::sync::Arc::new(parking_lot::RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new(
            "123456789012",
            "us-east-1",
            "http://localhost:4569",
        ),
    ));
    let svc = CognitoService::new(state.clone());

    // Create pool, client, and user so we get past user lookup
    let req = make_req("CreateUserPool", r#"{"PoolName": "ccpool"}"#);
    let resp = block_on(svc.create_user_pool(&req)).unwrap();
    let resp_body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let pool_id = resp_body["UserPool"]["Id"].as_str().unwrap().to_string();

    let body = serde_json::to_string(&json!({
        "UserPoolId": pool_id,
        "ClientName": "ccclient",
        "ExplicitAuthFlows": ["ALLOW_CUSTOM_AUTH"]
    }))
    .unwrap();
    let req = make_req("CreateUserPoolClient", &body);
    let resp = svc.create_user_pool_client(&req).unwrap();
    let resp_body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let client_id = resp_body["UserPoolClient"]["ClientId"]
        .as_str()
        .unwrap()
        .to_string();

    let body = serde_json::to_string(&json!({
        "UserPoolId": pool_id,
        "Username": "ccuser",
        "TemporaryPassword": "TempP@ss1!"
    }))
    .unwrap();
    let req = make_req("AdminCreateUser", &body);
    block_on(svc.admin_create_user(&req)).unwrap();

    let body = serde_json::to_string(&json!({
        "UserPoolId": pool_id,
        "Username": "ccuser",
        "Password": "P@ssw0rd!",
        "Permanent": true
    }))
    .unwrap();
    let req = make_req("AdminSetUserPassword", &body);
    svc.admin_set_user_password(&req).unwrap();

    // Manually insert a CUSTOM_CHALLENGE session
    let session_token = "test-session-123".to_string();
    {
        let mut _mas_w = state.write();
        let st = _mas_w.default_mut();
        st.sessions.insert(
            session_token.clone(),
            SessionData {
                user_pool_id: pool_id,
                username: "ccuser".to_string(),
                client_id: client_id.clone(),
                challenge_name: "CUSTOM_CHALLENGE".to_string(),
                challenge_results: vec![],
                challenge_metadata: None,
            },
        );
    }

    let body = serde_json::to_string(&json!({
        "ClientId": client_id,
        "ChallengeName": "CUSTOM_CHALLENGE",
        "Session": session_token,
        "ChallengeResponses": {"ANSWER": "my-answer"}
    }))
    .unwrap();
    let req = make_req("RespondToAuthChallenge", &body);
    let err = block_on(svc.respond_to_auth_challenge(&req))
        .err()
        .expect("Expected error for missing VerifyAuthChallengeResponse trigger");
    let err_str = format!("{err}");
    assert!(
        err_str.contains("InvalidLambdaResponseException")
            || err_str.contains("VerifyAuthChallengeResponse"),
        "Expected Lambda error, got: {err_str}"
    );
}

#[test]
fn custom_challenge_response_requires_answer() {
    let state = std::sync::Arc::new(parking_lot::RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new(
            "123456789012",
            "us-east-1",
            "http://localhost:4569",
        ),
    ));
    let svc = CognitoService::new(state.clone());

    // Create pool and client so we have valid IDs
    let req = make_req("CreateUserPool", r#"{"PoolName": "anspool"}"#);
    let resp = block_on(svc.create_user_pool(&req)).unwrap();
    let resp_body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let pool_id = resp_body["UserPool"]["Id"].as_str().unwrap().to_string();

    let body = serde_json::to_string(&json!({
        "UserPoolId": pool_id,
        "ClientName": "ansclient",
        "ExplicitAuthFlows": ["ALLOW_CUSTOM_AUTH"]
    }))
    .unwrap();
    let req = make_req("CreateUserPoolClient", &body);
    let resp = svc.create_user_pool_client(&req).unwrap();
    let resp_body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let client_id = resp_body["UserPoolClient"]["ClientId"]
        .as_str()
        .unwrap()
        .to_string();

    let session_token = "test-session-456".to_string();
    {
        let mut _mas_w = state.write();
        let st = _mas_w.default_mut();
        st.sessions.insert(
            session_token.clone(),
            SessionData {
                user_pool_id: pool_id,
                username: "testuser".to_string(),
                client_id: client_id.clone(),
                challenge_name: "CUSTOM_CHALLENGE".to_string(),
                challenge_results: vec![],
                challenge_metadata: None,
            },
        );
    }

    // Missing ANSWER
    let body = serde_json::to_string(&json!({
        "ClientId": client_id,
        "ChallengeName": "CUSTOM_CHALLENGE",
        "Session": session_token,
        "ChallengeResponses": {}
    }))
    .unwrap();
    let req = make_req("RespondToAuthChallenge", &body);
    let err = block_on(svc.respond_to_auth_challenge(&req))
        .err()
        .expect("Expected error for missing ANSWER");
    let err_str = format!("{err}");
    assert!(
        err_str.contains("ANSWER"),
        "Expected ANSWER required error, got: {err_str}"
    );
}

#[test]
fn session_data_stores_challenge_results() {
    let cr = ChallengeResult {
        challenge_name: "CUSTOM_CHALLENGE".to_string(),
        challenge_result: true,
        challenge_metadata: None,
    };
    let session = SessionData {
        user_pool_id: "pool-1".to_string(),
        username: "user1".to_string(),
        client_id: "client-1".to_string(),
        challenge_name: "CUSTOM_CHALLENGE".to_string(),
        challenge_results: vec![cr.clone()],
        challenge_metadata: Some("meta".to_string()),
    };
    assert_eq!(session.challenge_results.len(), 1);
    assert!(session.challenge_results[0].challenge_result);
    assert_eq!(session.challenge_metadata.as_deref(), Some("meta"));
}

// ── Helpers for auth/user tests ──

fn make_svc() -> (CognitoService, crate::state::SharedCognitoState) {
    let state = std::sync::Arc::new(parking_lot::RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new(
            "123456789012",
            "us-east-1",
            "http://localhost:4569",
        ),
    ));
    let svc = CognitoService::new(state.clone());
    (svc, state)
}

fn expect_err(result: Result<AwsResponse, AwsServiceError>) -> AwsServiceError {
    match result {
        Err(e) => e,
        Ok(_) => panic!("expected error, got Ok"),
    }
}

fn resp_json(resp: &AwsResponse) -> Value {
    serde_json::from_slice(resp.body.expect_bytes()).unwrap()
}

/// Create a user pool and return the pool ID.
fn create_pool(svc: &CognitoService) -> String {
    let req = make_req("CreateUserPool", r#"{"PoolName":"test-pool"}"#);
    let resp = block_on(svc.create_user_pool(&req)).unwrap();
    let b = resp_json(&resp);
    b["UserPool"]["Id"].as_str().unwrap().to_string()
}

/// Create a user pool client and return the client ID.
fn create_client(svc: &CognitoService, pool_id: &str) -> String {
    let body = json!({
        "UserPoolId": pool_id,
        "ClientName": "test-client",
        "ExplicitAuthFlows": ["ADMIN_NO_SRP_AUTH", "ALLOW_USER_PASSWORD_AUTH"],
    });
    let req = make_req("CreateUserPoolClient", &body.to_string());
    let resp = svc.create_user_pool_client(&req).unwrap();
    let b = resp_json(&resp);
    b["UserPoolClient"]["ClientId"]
        .as_str()
        .unwrap()
        .to_string()
}

/// Admin-create a user with a temporary password.
fn admin_create_user_helper(svc: &CognitoService, pool_id: &str, username: &str) {
    let body = json!({
        "UserPoolId": pool_id,
        "Username": username,
        "TemporaryPassword": "TempPass1!",
        "UserAttributes": [
            {"Name": "email", "Value": format!("{username}@example.com")},
        ],
    });
    let req = make_req("AdminCreateUser", &body.to_string());
    block_on(svc.admin_create_user(&req)).unwrap();
}

/// Set a confirmed password for a user.
fn set_user_password(svc: &CognitoService, pool_id: &str, username: &str, password: &str) {
    let body = json!({
        "UserPoolId": pool_id,
        "Username": username,
        "Password": password,
        "Permanent": true,
    });
    let req = make_req("AdminSetUserPassword", &body.to_string());
    svc.admin_set_user_password(&req).unwrap();
}

// ── AdminCreateUser ──

#[test]
fn admin_create_user_basic() {
    let (svc, _) = make_svc();
    let pool_id = create_pool(&svc);

    let body = json!({
        "UserPoolId": pool_id,
        "Username": "alice",
        "TemporaryPassword": "TempPass1!",
        "UserAttributes": [{"Name": "email", "Value": "alice@example.com"}],
    });
    let req = make_req("AdminCreateUser", &body.to_string());
    let resp = block_on(svc.admin_create_user(&req)).unwrap();
    let b = resp_json(&resp);
    assert_eq!(b["User"]["Username"], "alice");
    assert_eq!(b["User"]["UserStatus"], "FORCE_CHANGE_PASSWORD");
    assert!(b["User"]["Enabled"].as_bool().unwrap());
}

#[test]
fn admin_create_user_duplicate_fails() {
    let (svc, _) = make_svc();
    let pool_id = create_pool(&svc);
    admin_create_user_helper(&svc, &pool_id, "bob");

    let body = json!({
        "UserPoolId": pool_id,
        "Username": "bob",
    });
    let req = make_req("AdminCreateUser", &body.to_string());
    let err = expect_err(block_on(svc.admin_create_user(&req)));
    assert_eq!(err.code(), "UsernameExistsException");
}

#[test]
fn admin_create_user_missing_pool() {
    let (svc, _) = make_svc();

    let body = json!({
        "UserPoolId": "us-east-1_NONEXIST",
        "Username": "alice",
    });
    let req = make_req("AdminCreateUser", &body.to_string());
    let err = expect_err(block_on(svc.admin_create_user(&req)));
    assert_eq!(err.code(), "ResourceNotFoundException");
}

#[test]
fn admin_create_user_missing_username() {
    let (svc, _) = make_svc();
    let pool_id = create_pool(&svc);

    let body = json!({"UserPoolId": pool_id});
    let req = make_req("AdminCreateUser", &body.to_string());
    let err = expect_err(block_on(svc.admin_create_user(&req)));
    assert_eq!(err.code(), "InvalidParameterException");
}

// ── AdminGetUser ──

#[test]
fn admin_get_user_found() {
    let (svc, _) = make_svc();
    let pool_id = create_pool(&svc);
    admin_create_user_helper(&svc, &pool_id, "carol");

    let body = json!({"UserPoolId": pool_id, "Username": "carol"});
    let req = make_req("AdminGetUser", &body.to_string());
    let resp = svc.admin_get_user(&req).unwrap();
    let b = resp_json(&resp);
    assert_eq!(b["Username"], "carol");
    assert_eq!(b["UserStatus"], "FORCE_CHANGE_PASSWORD");
    // Has email attribute
    let attrs = b["UserAttributes"].as_array().unwrap();
    assert!(attrs.iter().any(|a| a["Name"] == "email"));
}

#[test]
fn admin_get_user_not_found() {
    let (svc, _) = make_svc();
    let pool_id = create_pool(&svc);

    let body = json!({"UserPoolId": pool_id, "Username": "ghost"});
    let req = make_req("AdminGetUser", &body.to_string());
    let err = expect_err(svc.admin_get_user(&req));
    assert_eq!(err.code(), "UserNotFoundException");
}

// ── AdminDeleteUser ──

#[test]
fn admin_delete_user_removes_user() {
    let (svc, _) = make_svc();
    let pool_id = create_pool(&svc);
    admin_create_user_helper(&svc, &pool_id, "dave");

    let body = json!({"UserPoolId": pool_id, "Username": "dave"});
    let req = make_req("AdminDeleteUser", &body.to_string());
    svc.admin_delete_user(&req).unwrap();

    // Get should fail
    let body = json!({"UserPoolId": pool_id, "Username": "dave"});
    let req = make_req("AdminGetUser", &body.to_string());
    let err = expect_err(svc.admin_get_user(&req));
    assert_eq!(err.code(), "UserNotFoundException");
}

#[test]
fn admin_delete_user_not_found() {
    let (svc, _) = make_svc();
    let pool_id = create_pool(&svc);

    let body = json!({"UserPoolId": pool_id, "Username": "ghost"});
    let req = make_req("AdminDeleteUser", &body.to_string());
    let err = expect_err(svc.admin_delete_user(&req));
    assert_eq!(err.code(), "UserNotFoundException");
}

// ── AdminUpdateUserAttributes ──

#[test]
fn admin_update_user_attributes() {
    let (svc, _) = make_svc();
    let pool_id = create_pool(&svc);
    admin_create_user_helper(&svc, &pool_id, "eve");

    let body = json!({
        "UserPoolId": pool_id,
        "Username": "eve",
        "UserAttributes": [
            {"Name": "custom:role", "Value": "admin"},
            {"Name": "email", "Value": "eve-new@example.com"},
        ],
    });
    let req = make_req("AdminUpdateUserAttributes", &body.to_string());
    svc.admin_update_user_attributes(&req).unwrap();

    // Verify
    let body = json!({"UserPoolId": pool_id, "Username": "eve"});
    let req = make_req("AdminGetUser", &body.to_string());
    let resp = svc.admin_get_user(&req).unwrap();
    let b = resp_json(&resp);
    let attrs = b["UserAttributes"].as_array().unwrap();
    assert!(attrs
        .iter()
        .any(|a| a["Name"] == "custom:role" && a["Value"] == "admin"));
    assert!(attrs
        .iter()
        .any(|a| a["Name"] == "email" && a["Value"] == "eve-new@example.com"));
}

// ── AdminDisableUser / AdminEnableUser ──

#[test]
fn admin_disable_and_enable_user() {
    let (svc, _) = make_svc();
    let pool_id = create_pool(&svc);
    admin_create_user_helper(&svc, &pool_id, "frank");

    // Disable
    let body = json!({"UserPoolId": pool_id, "Username": "frank"});
    let req = make_req("AdminDisableUser", &body.to_string());
    svc.admin_disable_user(&req).unwrap();

    let body = json!({"UserPoolId": pool_id, "Username": "frank"});
    let req = make_req("AdminGetUser", &body.to_string());
    let resp = svc.admin_get_user(&req).unwrap();
    let b = resp_json(&resp);
    assert!(!b["Enabled"].as_bool().unwrap());

    // Enable
    let body = json!({"UserPoolId": pool_id, "Username": "frank"});
    let req = make_req("AdminEnableUser", &body.to_string());
    svc.admin_enable_user(&req).unwrap();

    let body = json!({"UserPoolId": pool_id, "Username": "frank"});
    let req = make_req("AdminGetUser", &body.to_string());
    let resp = svc.admin_get_user(&req).unwrap();
    let b = resp_json(&resp);
    assert!(b["Enabled"].as_bool().unwrap());
}

// ── AdminSetUserPassword ──

#[test]
fn admin_set_user_password_permanent() {
    let (svc, _) = make_svc();
    let pool_id = create_pool(&svc);
    admin_create_user_helper(&svc, &pool_id, "gina");

    set_user_password(&svc, &pool_id, "gina", "NewPass1!");

    let body = json!({"UserPoolId": pool_id, "Username": "gina"});
    let req = make_req("AdminGetUser", &body.to_string());
    let resp = svc.admin_get_user(&req).unwrap();
    let b = resp_json(&resp);
    assert_eq!(b["UserStatus"], "CONFIRMED");
}

#[test]
fn admin_set_user_password_temporary() {
    let (svc, _) = make_svc();
    let pool_id = create_pool(&svc);
    admin_create_user_helper(&svc, &pool_id, "hank");

    let body = json!({
        "UserPoolId": pool_id,
        "Username": "hank",
        "Password": "TempNew1!",
        "Permanent": false,
    });
    let req = make_req("AdminSetUserPassword", &body.to_string());
    svc.admin_set_user_password(&req).unwrap();

    let body = json!({"UserPoolId": pool_id, "Username": "hank"});
    let req = make_req("AdminGetUser", &body.to_string());
    let resp = svc.admin_get_user(&req).unwrap();
    let b = resp_json(&resp);
    // Temporary password keeps FORCE_CHANGE_PASSWORD
    assert_eq!(b["UserStatus"], "FORCE_CHANGE_PASSWORD");
}

// ── AdminInitiateAuth ──

#[test]
fn admin_initiate_auth_success() {
    let (svc, _) = make_svc();
    let pool_id = create_pool(&svc);
    let client_id = create_client(&svc, &pool_id);
    admin_create_user_helper(&svc, &pool_id, "ivan");
    set_user_password(&svc, &pool_id, "ivan", "SecurePass1!");

    let body = json!({
        "UserPoolId": pool_id,
        "ClientId": client_id,
        "AuthFlow": "ADMIN_NO_SRP_AUTH",
        "AuthParameters": {
            "USERNAME": "ivan",
            "PASSWORD": "SecurePass1!",
        },
    });
    let req = make_req("AdminInitiateAuth", &body.to_string());
    let resp = block_on(svc.admin_initiate_auth(&req)).unwrap();
    let b = resp_json(&resp);
    assert!(b["AuthenticationResult"]["AccessToken"].as_str().is_some());
    assert!(b["AuthenticationResult"]["IdToken"].as_str().is_some());
    assert!(b["AuthenticationResult"]["RefreshToken"].as_str().is_some());
}

#[test]
fn admin_initiate_auth_wrong_password() {
    let (svc, _) = make_svc();
    let pool_id = create_pool(&svc);
    let client_id = create_client(&svc, &pool_id);
    admin_create_user_helper(&svc, &pool_id, "judy");
    set_user_password(&svc, &pool_id, "judy", "CorrectPass1!");

    let body = json!({
        "UserPoolId": pool_id,
        "ClientId": client_id,
        "AuthFlow": "ADMIN_NO_SRP_AUTH",
        "AuthParameters": {
            "USERNAME": "judy",
            "PASSWORD": "WrongPass1!",
        },
    });
    let req = make_req("AdminInitiateAuth", &body.to_string());
    let err = expect_err(block_on(svc.admin_initiate_auth(&req)));
    assert_eq!(err.code(), "NotAuthorizedException");
}

#[test]
fn admin_initiate_auth_user_not_found() {
    let (svc, _) = make_svc();
    let pool_id = create_pool(&svc);
    let client_id = create_client(&svc, &pool_id);

    let body = json!({
        "UserPoolId": pool_id,
        "ClientId": client_id,
        "AuthFlow": "ADMIN_NO_SRP_AUTH",
        "AuthParameters": {
            "USERNAME": "nobody",
            "PASSWORD": "Password1!",
        },
    });
    let req = make_req("AdminInitiateAuth", &body.to_string());
    let err = expect_err(block_on(svc.admin_initiate_auth(&req)));
    assert_eq!(err.code(), "UserNotFoundException");
}

#[test]
fn admin_initiate_auth_new_password_required() {
    let (svc, _) = make_svc();
    let pool_id = create_pool(&svc);
    let client_id = create_client(&svc, &pool_id);
    admin_create_user_helper(&svc, &pool_id, "karl");
    // Don't set permanent password -> user is FORCE_CHANGE_PASSWORD

    let body = json!({
        "UserPoolId": pool_id,
        "ClientId": client_id,
        "AuthFlow": "ADMIN_NO_SRP_AUTH",
        "AuthParameters": {
            "USERNAME": "karl",
            "PASSWORD": "TempPass1!",
        },
    });
    let req = make_req("AdminInitiateAuth", &body.to_string());
    let resp = block_on(svc.admin_initiate_auth(&req)).unwrap();
    let b = resp_json(&resp);
    assert_eq!(b["ChallengeName"], "NEW_PASSWORD_REQUIRED");
    assert!(b["Session"].as_str().is_some());
}

#[test]
fn admin_initiate_auth_unsupported_flow() {
    let (svc, _) = make_svc();
    let pool_id = create_pool(&svc);
    let client_id = create_client(&svc, &pool_id);

    let body = json!({
        "UserPoolId": pool_id,
        "ClientId": client_id,
        "AuthFlow": "USER_SRP_AUTH",
        "AuthParameters": {
            "USERNAME": "x",
            "PASSWORD": "x",
        },
    });
    let req = make_req("AdminInitiateAuth", &body.to_string());
    let err = expect_err(block_on(svc.admin_initiate_auth(&req)));
    assert_eq!(err.code(), "InvalidParameterException");
}

// ── SignUp ──

#[test]
fn sign_up_and_confirm() {
    let (svc, _) = make_svc();
    let pool_id = create_pool(&svc);
    let client_id = create_client(&svc, &pool_id);

    let body = json!({
        "ClientId": client_id,
        "Username": "selfuser",
        "Password": "MyPass1!",
        "UserAttributes": [{"Name": "email", "Value": "self@example.com"}],
    });
    let req = make_req("SignUp", &body.to_string());
    let resp = block_on(svc.sign_up(&req)).unwrap();
    let b = resp_json(&resp);
    assert!(!b["UserConfirmed"].as_bool().unwrap());
    assert!(b["UserSub"].as_str().is_some());

    // Confirm via ConfirmSignUp (accepts any code in current impl)
    let body = json!({
        "ClientId": client_id,
        "Username": "selfuser",
        "ConfirmationCode": "123456",
    });
    let req = make_req("ConfirmSignUp", &body.to_string());
    block_on(svc.confirm_sign_up(&req)).unwrap();

    // User should be confirmed
    let body = json!({"UserPoolId": pool_id, "Username": "selfuser"});
    let req = make_req("AdminGetUser", &body.to_string());
    let resp = svc.admin_get_user(&req).unwrap();
    let b = resp_json(&resp);
    assert_eq!(b["UserStatus"], "CONFIRMED");
}

#[test]
fn sign_up_duplicate_username() {
    let (svc, _) = make_svc();
    let pool_id = create_pool(&svc);
    let client_id = create_client(&svc, &pool_id);

    let body = json!({
        "ClientId": client_id,
        "Username": "dupuser",
        "Password": "MyPass1!",
    });
    let req = make_req("SignUp", &body.to_string());
    block_on(svc.sign_up(&req)).unwrap();

    // Same username again -> error
    let req = make_req("SignUp", &body.to_string());
    let err = expect_err(block_on(svc.sign_up(&req)));
    assert_eq!(err.code(), "UsernameExistsException");
}

// ── AdminConfirmSignUp ──

#[test]
fn admin_confirm_sign_up() {
    let (svc, _) = make_svc();
    let pool_id = create_pool(&svc);
    let client_id = create_client(&svc, &pool_id);

    let body = json!({
        "ClientId": client_id,
        "Username": "unconfirmed",
        "Password": "MyPass1!",
    });
    let req = make_req("SignUp", &body.to_string());
    block_on(svc.sign_up(&req)).unwrap();

    let body = json!({
        "UserPoolId": pool_id,
        "Username": "unconfirmed",
    });
    let req = make_req("AdminConfirmSignUp", &body.to_string());
    block_on(svc.admin_confirm_sign_up(&req)).unwrap();

    let body = json!({"UserPoolId": pool_id, "Username": "unconfirmed"});
    let req = make_req("AdminGetUser", &body.to_string());
    let resp = svc.admin_get_user(&req).unwrap();
    let b = resp_json(&resp);
    assert_eq!(b["UserStatus"], "CONFIRMED");
}

// ── AdminResetUserPassword ──

#[test]
fn admin_reset_user_password() {
    let (svc, _state) = make_svc();
    let pool_id = create_pool(&svc);
    admin_create_user_helper(&svc, &pool_id, "resetme");
    set_user_password(&svc, &pool_id, "resetme", "OldPass1!");

    let body = json!({"UserPoolId": pool_id, "Username": "resetme"});
    let req = make_req("AdminResetUserPassword", &body.to_string());
    svc.admin_reset_user_password(&req).unwrap();

    let body = json!({"UserPoolId": pool_id, "Username": "resetme"});
    let req = make_req("AdminGetUser", &body.to_string());
    let resp = svc.admin_get_user(&req).unwrap();
    let b = resp_json(&resp);
    assert_eq!(b["UserStatus"], "RESET_REQUIRED");
}

// ── ListUsers ──

#[test]
fn list_users_returns_created_users() {
    let (svc, _) = make_svc();
    let pool_id = create_pool(&svc);
    admin_create_user_helper(&svc, &pool_id, "user-a");
    admin_create_user_helper(&svc, &pool_id, "user-b");

    let body = json!({"UserPoolId": pool_id});
    let req = make_req("ListUsers", &body.to_string());
    let resp = svc.list_users(&req).unwrap();
    let b = resp_json(&resp);
    let users = b["Users"].as_array().unwrap();
    assert_eq!(users.len(), 2);
}

#[test]
fn list_users_with_filter() {
    let (svc, _) = make_svc();
    let pool_id = create_pool(&svc);
    admin_create_user_helper(&svc, &pool_id, "alice");
    admin_create_user_helper(&svc, &pool_id, "bob");

    let body = json!({
        "UserPoolId": pool_id,
        "Filter": r#"username = "alice""#,
    });
    let req = make_req("ListUsers", &body.to_string());
    let resp = svc.list_users(&req).unwrap();
    let b = resp_json(&resp);
    let users = b["Users"].as_array().unwrap();
    assert_eq!(users.len(), 1);
    assert_eq!(users[0]["Username"], "alice");
}

#[test]
fn list_users_with_limit() {
    let (svc, _) = make_svc();
    let pool_id = create_pool(&svc);
    for i in 0..5 {
        admin_create_user_helper(&svc, &pool_id, &format!("u{i}"));
    }

    let body = json!({"UserPoolId": pool_id, "Limit": 2});
    let req = make_req("ListUsers", &body.to_string());
    let resp = svc.list_users(&req).unwrap();
    let b = resp_json(&resp);
    let users = b["Users"].as_array().unwrap();
    assert_eq!(users.len(), 2);
    assert!(b["PaginationToken"].as_str().is_some());
}

// ── AdminAddUserToGroup / AdminRemoveUserFromGroup ──

#[test]
fn admin_add_and_remove_user_from_group() {
    let (svc, _) = make_svc();
    let pool_id = create_pool(&svc);
    admin_create_user_helper(&svc, &pool_id, "groupuser");

    // Create group
    let body = json!({
        "UserPoolId": pool_id,
        "GroupName": "admins",
    });
    let req = make_req("CreateGroup", &body.to_string());
    svc.create_group(&req).unwrap();

    // Add to group
    let body = json!({
        "UserPoolId": pool_id,
        "Username": "groupuser",
        "GroupName": "admins",
    });
    let req = make_req("AdminAddUserToGroup", &body.to_string());
    svc.admin_add_user_to_group(&req).unwrap();

    // List groups for user
    let body = json!({"UserPoolId": pool_id, "Username": "groupuser"});
    let req = make_req("AdminListGroupsForUser", &body.to_string());
    let resp = svc.admin_list_groups_for_user(&req).unwrap();
    let b = resp_json(&resp);
    let groups = b["Groups"].as_array().unwrap();
    assert_eq!(groups.len(), 1);
    assert_eq!(groups[0]["GroupName"], "admins");

    // Remove from group
    let body = json!({
        "UserPoolId": pool_id,
        "Username": "groupuser",
        "GroupName": "admins",
    });
    let req = make_req("AdminRemoveUserFromGroup", &body.to_string());
    svc.admin_remove_user_from_group(&req).unwrap();

    // Verify removal
    let body = json!({"UserPoolId": pool_id, "Username": "groupuser"});
    let req = make_req("AdminListGroupsForUser", &body.to_string());
    let resp = svc.admin_list_groups_for_user(&req).unwrap();
    let b = resp_json(&resp);
    let groups = b["Groups"].as_array().unwrap();
    assert!(groups.is_empty());
}

// ── AdminRespondToAuthChallenge ──

#[test]
fn admin_respond_to_auth_challenge_new_password() {
    let (svc, _) = make_svc();
    let pool_id = create_pool(&svc);
    let client_id = create_client(&svc, &pool_id);
    admin_create_user_helper(&svc, &pool_id, "challenge");

    // Auth -> NEW_PASSWORD_REQUIRED
    let body = json!({
        "UserPoolId": pool_id,
        "ClientId": client_id,
        "AuthFlow": "ADMIN_NO_SRP_AUTH",
        "AuthParameters": {
            "USERNAME": "challenge",
            "PASSWORD": "TempPass1!",
        },
    });
    let req = make_req("AdminInitiateAuth", &body.to_string());
    let resp = block_on(svc.admin_initiate_auth(&req)).unwrap();
    let b = resp_json(&resp);
    let session = b["Session"].as_str().unwrap().to_string();

    // Respond with new password
    let body = json!({
        "UserPoolId": pool_id,
        "ClientId": client_id,
        "ChallengeName": "NEW_PASSWORD_REQUIRED",
        "Session": session,
        "ChallengeResponses": {
            "USERNAME": "challenge",
            "NEW_PASSWORD": "Permanent1!",
        },
    });
    let req = make_req("AdminRespondToAuthChallenge", &body.to_string());
    let resp = block_on(svc.admin_respond_to_auth_challenge(&req)).unwrap();
    let b = resp_json(&resp);
    assert!(b["AuthenticationResult"]["AccessToken"].as_str().is_some());

    // User should now be CONFIRMED
    let body = json!({"UserPoolId": pool_id, "Username": "challenge"});
    let req = make_req("AdminGetUser", &body.to_string());
    let resp = svc.admin_get_user(&req).unwrap();
    let b = resp_json(&resp);
    assert_eq!(b["UserStatus"], "CONFIRMED");
}

// ── User Pool CRUD ──

#[test]
fn describe_user_pool() {
    let (svc, _) = make_svc();
    let pool_id = create_pool(&svc);

    let req = make_req(
        "DescribeUserPool",
        &json!({"UserPoolId": pool_id}).to_string(),
    );
    let resp = svc.describe_user_pool(&req).unwrap();
    let b = resp_json(&resp);
    assert_eq!(b["UserPool"]["Id"], pool_id);
    assert_eq!(b["UserPool"]["Name"], "test-pool");
}

#[test]
fn update_user_pool() {
    let (svc, _) = make_svc();
    let pool_id = create_pool(&svc);

    let body = json!({
        "UserPoolId": pool_id,
        "AdminCreateUserConfig": {"AllowAdminCreateUserOnly": true},
    });
    let req = make_req("UpdateUserPool", &body.to_string());
    svc.update_user_pool(&req).unwrap();

    let req = make_req(
        "DescribeUserPool",
        &json!({"UserPoolId": pool_id}).to_string(),
    );
    let resp = svc.describe_user_pool(&req).unwrap();
    let b = resp_json(&resp);
    assert_eq!(
        b["UserPool"]["AdminCreateUserConfig"]["AllowAdminCreateUserOnly"],
        true
    );
}

#[test]
fn delete_user_pool() {
    let (svc, _) = make_svc();
    let pool_id = create_pool(&svc);

    let req = make_req(
        "DeleteUserPool",
        &json!({"UserPoolId": pool_id}).to_string(),
    );
    svc.delete_user_pool(&req).unwrap();

    let req = make_req(
        "DescribeUserPool",
        &json!({"UserPoolId": pool_id}).to_string(),
    );
    assert!(svc.describe_user_pool(&req).is_err());
}

#[test]
fn list_user_pools() {
    let (svc, _) = make_svc();
    create_pool(&svc);

    let req = make_req("ListUserPools", &json!({"MaxResults": 10}).to_string());
    let resp = svc.list_user_pools(&req).unwrap();
    let b = resp_json(&resp);
    assert!(!b["UserPools"].as_array().unwrap().is_empty());
}

// ── User Pool Client CRUD ──

#[test]
fn describe_user_pool_client() {
    let (svc, _) = make_svc();
    let pool_id = create_pool(&svc);
    let client_id = create_client(&svc, &pool_id);

    let body = json!({"UserPoolId": pool_id, "ClientId": client_id});
    let req = make_req("DescribeUserPoolClient", &body.to_string());
    let resp = svc.describe_user_pool_client(&req).unwrap();
    let b = resp_json(&resp);
    assert_eq!(b["UserPoolClient"]["ClientId"], client_id);
}

#[test]
fn update_user_pool_client() {
    let (svc, _) = make_svc();
    let pool_id = create_pool(&svc);
    let client_id = create_client(&svc, &pool_id);

    let body = json!({
        "UserPoolId": pool_id,
        "ClientId": client_id,
        "ClientName": "renamed-client",
    });
    let req = make_req("UpdateUserPoolClient", &body.to_string());
    let resp = svc.update_user_pool_client(&req).unwrap();
    let b = resp_json(&resp);
    assert_eq!(b["UserPoolClient"]["ClientName"], "renamed-client");
}

#[test]
fn delete_user_pool_client() {
    let (svc, _) = make_svc();
    let pool_id = create_pool(&svc);
    let client_id = create_client(&svc, &pool_id);

    let body = json!({"UserPoolId": pool_id, "ClientId": client_id});
    let req = make_req("DeleteUserPoolClient", &body.to_string());
    svc.delete_user_pool_client(&req).unwrap();

    // Describe should fail
    let body = json!({"UserPoolId": pool_id, "ClientId": client_id});
    let req = make_req("DescribeUserPoolClient", &body.to_string());
    assert!(svc.describe_user_pool_client(&req).is_err());
}

#[test]
fn list_user_pool_clients() {
    let (svc, _) = make_svc();
    let pool_id = create_pool(&svc);
    create_client(&svc, &pool_id);

    let body = json!({"UserPoolId": pool_id});
    let req = make_req("ListUserPoolClients", &body.to_string());
    let resp = svc.list_user_pool_clients(&req).unwrap();
    let b = resp_json(&resp);
    assert_eq!(b["UserPoolClients"].as_array().unwrap().len(), 1);
}

// ── Group CRUD (extended) ──

#[test]
fn group_crud_full() {
    let (svc, _) = make_svc();
    let pool_id = create_pool(&svc);

    // Create
    let body = json!({
        "UserPoolId": pool_id,
        "GroupName": "editors",
        "Description": "Editor group",
        "Precedence": 5,
    });
    let req = make_req("CreateGroup", &body.to_string());
    let resp = svc.create_group(&req).unwrap();
    let b = resp_json(&resp);
    assert_eq!(b["Group"]["GroupName"], "editors");

    // Get
    let body = json!({"UserPoolId": pool_id, "GroupName": "editors"});
    let req = make_req("GetGroup", &body.to_string());
    let resp = svc.get_group(&req).unwrap();
    let b = resp_json(&resp);
    assert_eq!(b["Group"]["Description"], "Editor group");

    // List
    let body = json!({"UserPoolId": pool_id});
    let req = make_req("ListGroups", &body.to_string());
    let resp = svc.list_groups(&req).unwrap();
    let b = resp_json(&resp);
    assert_eq!(b["Groups"].as_array().unwrap().len(), 1);

    // Update
    let body = json!({
        "UserPoolId": pool_id,
        "GroupName": "editors",
        "Description": "Updated desc",
    });
    let req = make_req("UpdateGroup", &body.to_string());
    let resp = svc.update_group(&req).unwrap();
    let b = resp_json(&resp);
    assert_eq!(b["Group"]["Description"], "Updated desc");

    // Delete
    let body = json!({"UserPoolId": pool_id, "GroupName": "editors"});
    let req = make_req("DeleteGroup", &body.to_string());
    svc.delete_group(&req).unwrap();

    // Get should fail
    let body = json!({"UserPoolId": pool_id, "GroupName": "editors"});
    let req = make_req("GetGroup", &body.to_string());
    assert!(svc.get_group(&req).is_err());
}

#[test]
fn list_users_in_group() {
    let (svc, _) = make_svc();
    let pool_id = create_pool(&svc);
    admin_create_user_helper(&svc, &pool_id, "grp-member");

    let body = json!({"UserPoolId": pool_id, "GroupName": "team"});
    let req = make_req("CreateGroup", &body.to_string());
    svc.create_group(&req).unwrap();

    svc.admin_add_user_to_group(&make_req(
        "AdminAddUserToGroup",
        &json!({"UserPoolId": pool_id, "Username": "grp-member", "GroupName": "team"}).to_string(),
    ))
    .unwrap();

    let body = json!({"UserPoolId": pool_id, "GroupName": "team"});
    let req = make_req("ListUsersInGroup", &body.to_string());
    let resp = svc.list_users_in_group(&req).unwrap();
    let b = resp_json(&resp);
    assert_eq!(b["Users"].as_array().unwrap().len(), 1);
}

// ── Identity Provider CRUD ──

#[test]
fn identity_provider_crud() {
    let (svc, _) = make_svc();
    let pool_id = create_pool(&svc);

    // Create
    let body = json!({
        "UserPoolId": pool_id,
        "ProviderName": "Google",
        "ProviderType": "Google",
        "ProviderDetails": {"client_id": "gid", "client_secret": "gsec", "authorize_scopes": "openid"},
    });
    let req = make_req("CreateIdentityProvider", &body.to_string());
    svc.create_identity_provider(&req).unwrap();

    // Describe
    let body = json!({"UserPoolId": pool_id, "ProviderName": "Google"});
    let req = make_req("DescribeIdentityProvider", &body.to_string());
    let resp = svc.describe_identity_provider(&req).unwrap();
    let b = resp_json(&resp);
    assert_eq!(b["IdentityProvider"]["ProviderName"], "Google");

    // List
    let body = json!({"UserPoolId": pool_id});
    let req = make_req("ListIdentityProviders", &body.to_string());
    let resp = svc.list_identity_providers(&req).unwrap();
    let b = resp_json(&resp);
    assert_eq!(b["Providers"].as_array().unwrap().len(), 1);

    // Update
    let body = json!({
        "UserPoolId": pool_id,
        "ProviderName": "Google",
        "ProviderDetails": {"client_id": "new-gid", "client_secret": "gsec", "authorize_scopes": "openid email"},
    });
    let req = make_req("UpdateIdentityProvider", &body.to_string());
    svc.update_identity_provider(&req).unwrap();

    // Delete
    let body = json!({"UserPoolId": pool_id, "ProviderName": "Google"});
    let req = make_req("DeleteIdentityProvider", &body.to_string());
    svc.delete_identity_provider(&req).unwrap();

    // List should be empty
    let body = json!({"UserPoolId": pool_id});
    let req = make_req("ListIdentityProviders", &body.to_string());
    let resp = svc.list_identity_providers(&req).unwrap();
    let b = resp_json(&resp);
    assert!(b["Providers"].as_array().unwrap().is_empty());
}

// ── Resource Server CRUD ──

#[test]
fn resource_server_crud() {
    let (svc, _) = make_svc();
    let pool_id = create_pool(&svc);

    // Create
    let body = json!({
        "UserPoolId": pool_id,
        "Identifier": "https://api.example.com",
        "Name": "My API",
        "Scopes": [{"ScopeName": "read", "ScopeDescription": "Read access"}],
    });
    let req = make_req("CreateResourceServer", &body.to_string());
    svc.create_resource_server(&req).unwrap();

    // Describe
    let body = json!({"UserPoolId": pool_id, "Identifier": "https://api.example.com"});
    let req = make_req("DescribeResourceServer", &body.to_string());
    let resp = svc.describe_resource_server(&req).unwrap();
    let b = resp_json(&resp);
    assert_eq!(b["ResourceServer"]["Name"], "My API");

    // List
    let body = json!({"UserPoolId": pool_id});
    let req = make_req("ListResourceServers", &body.to_string());
    let resp = svc.list_resource_servers(&req).unwrap();
    let b = resp_json(&resp);
    assert_eq!(b["ResourceServers"].as_array().unwrap().len(), 1);

    // Update
    let body = json!({
        "UserPoolId": pool_id,
        "Identifier": "https://api.example.com",
        "Name": "Updated API",
        "Scopes": [
            {"ScopeName": "read", "ScopeDescription": "Read"},
            {"ScopeName": "write", "ScopeDescription": "Write"},
        ],
    });
    let req = make_req("UpdateResourceServer", &body.to_string());
    svc.update_resource_server(&req).unwrap();

    // Delete
    let body = json!({"UserPoolId": pool_id, "Identifier": "https://api.example.com"});
    let req = make_req("DeleteResourceServer", &body.to_string());
    svc.delete_resource_server(&req).unwrap();
}

// ── MFA Config ──

#[test]
fn mfa_config_set_and_get() {
    let (svc, _) = make_svc();
    let pool_id = create_pool(&svc);

    let body = json!({
        "UserPoolId": pool_id,
        "MfaConfiguration": "OPTIONAL",
        "SoftwareTokenMfaConfiguration": {"Enabled": true},
    });
    let req = make_req("SetUserPoolMfaConfig", &body.to_string());
    svc.set_user_pool_mfa_config(&req).unwrap();

    let body = json!({"UserPoolId": pool_id});
    let req = make_req("GetUserPoolMfaConfig", &body.to_string());
    let resp = svc.get_user_pool_mfa_config(&req).unwrap();
    let b = resp_json(&resp);
    assert_eq!(b["MfaConfiguration"], "OPTIONAL");
    assert_eq!(b["SoftwareTokenMfaConfiguration"]["Enabled"], true);
}

// ── Domain CRUD ──

#[test]
fn user_pool_domain_crud() {
    let (svc, _) = make_svc();
    let pool_id = create_pool(&svc);

    // Create
    let body = json!({"UserPoolId": pool_id, "Domain": "my-auth-domain"});
    let req = make_req("CreateUserPoolDomain", &body.to_string());
    svc.create_user_pool_domain(&req).unwrap();

    // Describe
    let body = json!({"Domain": "my-auth-domain"});
    let req = make_req("DescribeUserPoolDomain", &body.to_string());
    let resp = svc.describe_user_pool_domain(&req).unwrap();
    let b = resp_json(&resp);
    assert_eq!(b["DomainDescription"]["Domain"], "my-auth-domain");

    // Delete
    let body = json!({"UserPoolId": pool_id, "Domain": "my-auth-domain"});
    let req = make_req("DeleteUserPoolDomain", &body.to_string());
    svc.delete_user_pool_domain(&req).unwrap();
}

// ── UI Customization ──

#[test]
fn ui_customization_set_and_get() {
    let (svc, _) = make_svc();
    let pool_id = create_pool(&svc);

    let body = json!({
        "UserPoolId": pool_id,
        "CSS": ".banner { background: red; }",
    });
    let req = make_req("SetUICustomization", &body.to_string());
    svc.set_ui_customization(&req).unwrap();

    let body = json!({"UserPoolId": pool_id});
    let req = make_req("GetUICustomization", &body.to_string());
    let resp = svc.get_ui_customization(&req).unwrap();
    let b = resp_json(&resp);
    assert_eq!(b["UICustomization"]["CSS"], ".banner { background: red; }");
}

// ── Device management ──

#[test]
fn device_management_admin() {
    let (svc, _) = make_svc();
    let pool_id = create_pool(&svc);
    let client_id = create_client(&svc, &pool_id);
    admin_create_user_helper(&svc, &pool_id, "devuser");
    set_user_password(&svc, &pool_id, "devuser", "Pass123!");

    // Auth to get a device key
    let body = json!({
        "UserPoolId": pool_id,
        "ClientId": client_id,
        "AuthFlow": "ADMIN_NO_SRP_AUTH",
        "AuthParameters": {"USERNAME": "devuser", "PASSWORD": "Pass123!"},
    });
    let req = make_req("AdminInitiateAuth", &body.to_string());
    block_on(svc.admin_initiate_auth(&req)).unwrap();

    // AdminListDevices - should return empty initially
    let body = json!({"UserPoolId": pool_id, "Username": "devuser"});
    let req = make_req("AdminListDevices", &body.to_string());
    let resp = svc.admin_list_devices(&req).unwrap();
    let b = resp_json(&resp);
    assert!(b["Devices"].as_array().unwrap().is_empty());
}

// ── AdminDeleteUserAttributes ──

#[test]
fn admin_delete_user_attributes() {
    let (svc, _) = make_svc();
    let pool_id = create_pool(&svc);
    admin_create_user_helper(&svc, &pool_id, "delattr");

    // Add custom attribute
    let body = json!({
        "UserPoolId": pool_id,
        "Username": "delattr",
        "UserAttributes": [{"Name": "custom:role", "Value": "admin"}],
    });
    let req = make_req("AdminUpdateUserAttributes", &body.to_string());
    svc.admin_update_user_attributes(&req).unwrap();

    // Delete it
    let body = json!({
        "UserPoolId": pool_id,
        "Username": "delattr",
        "UserAttributeNames": ["custom:role"],
    });
    let req = make_req("AdminDeleteUserAttributes", &body.to_string());
    svc.admin_delete_user_attributes(&req).unwrap();

    // Verify removed
    let req = make_req(
        "AdminGetUser",
        &json!({"UserPoolId": pool_id, "Username": "delattr"}).to_string(),
    );
    let resp = svc.admin_get_user(&req).unwrap();
    let b = resp_json(&resp);
    let attrs = b["UserAttributes"].as_array().unwrap();
    assert!(!attrs.iter().any(|a| a["Name"] == "custom:role"));
}

// ── Managed Login Branding ──

#[test]
fn managed_login_branding_crud() {
    let (svc, _) = make_svc();
    let pool_id = create_pool(&svc);
    let client_id = create_client(&svc, &pool_id);

    // Create
    let body = json!({
        "UserPoolId": pool_id,
        "ClientId": client_id,
        "UseCognitoProvidedValues": true,
        "Settings": {"theme": "dark"},
    });
    let req = make_req("CreateManagedLoginBranding", &body.to_string());
    let resp = svc.create_managed_login_branding(&req).unwrap();
    let b = resp_json(&resp);
    let branding_id = b["ManagedLoginBranding"]["ManagedLoginBrandingId"]
        .as_str()
        .unwrap()
        .to_string();

    // Describe
    let body = json!({
        "ManagedLoginBrandingId": branding_id,
        "UserPoolId": pool_id,
    });
    let req = make_req("DescribeManagedLoginBranding", &body.to_string());
    let resp = svc.describe_managed_login_branding(&req).unwrap();
    let b = resp_json(&resp);
    assert_eq!(b["ManagedLoginBranding"]["UseCognitoProvidedValues"], true);

    // Describe by client
    let body = json!({
        "UserPoolId": pool_id,
        "ClientId": client_id,
    });
    let req = make_req("DescribeManagedLoginBrandingByClient", &body.to_string());
    let resp = svc.describe_managed_login_branding_by_client(&req).unwrap();
    let b = resp_json(&resp);
    assert!(b["ManagedLoginBranding"]["ManagedLoginBrandingId"].is_string());

    // Update
    let body = json!({
        "ManagedLoginBrandingId": branding_id,
        "UserPoolId": pool_id,
        "Settings": {"theme": "light"},
    });
    let req = make_req("UpdateManagedLoginBranding", &body.to_string());
    svc.update_managed_login_branding(&req).unwrap();

    // Delete
    let body = json!({
        "ManagedLoginBrandingId": branding_id,
        "UserPoolId": pool_id,
    });
    let req = make_req("DeleteManagedLoginBranding", &body.to_string());
    svc.delete_managed_login_branding(&req).unwrap();
}

#[test]
fn managed_login_branding_missing_client() {
    let (svc, _) = make_svc();
    let pool_id = create_pool(&svc);

    let body = json!({
        "UserPoolId": pool_id,
        "ClientId": "nonexistent-client",
    });
    let req = make_req("CreateManagedLoginBranding", &body.to_string());
    let err = expect_err(svc.create_managed_login_branding(&req));
    assert_eq!(err.code(), "ResourceNotFoundException");
}

// ── Terms of Service ──

#[test]
fn terms_crud() {
    let (svc, _) = make_svc();
    let pool_id = create_pool(&svc);

    // Create
    let body = json!({
        "UserPoolId": pool_id,
        "TermsName": "tos-v1",
    });
    let req = make_req("CreateTerms", &body.to_string());
    let resp = svc.create_terms(&req).unwrap();
    let b = resp_json(&resp);
    assert!(b["Terms"]["TermsId"].is_string());

    // List
    let body = json!({"UserPoolId": pool_id});
    let req = make_req("ListTerms", &body.to_string());
    let resp = svc.list_terms(&req).unwrap();
    let b = resp_json(&resp);
    assert_eq!(b["Terms"].as_array().unwrap().len(), 1);

    // Describe
    let terms_id = {
        let _mas = svc.state.read();
        let s = _mas.default_ref();
        s.terms.keys().next().unwrap().clone()
    };
    let body = json!({"UserPoolId": pool_id, "TermsId": terms_id});
    let req = make_req("DescribeTerms", &body.to_string());
    svc.describe_terms(&req).unwrap();

    // Update
    let body = json!({
        "UserPoolId": pool_id,
        "TermsId": terms_id,
        "TermsContent": "Updated terms",
    });
    let req = make_req("UpdateTerms", &body.to_string());
    svc.update_terms(&req).unwrap();

    // Delete
    let body = json!({"UserPoolId": pool_id, "TermsId": terms_id});
    let req = make_req("DeleteTerms", &body.to_string());
    svc.delete_terms(&req).unwrap();
}

// ── WebAuthn Credentials ──

#[test]
fn web_authn_registration_and_list() {
    let (svc, _) = make_svc();
    let pool_id = create_pool(&svc);
    let client_id = create_client(&svc, &pool_id);
    admin_create_user_helper(&svc, &pool_id, "webauthn-user");
    set_user_password(&svc, &pool_id, "webauthn-user", "Pass123!");

    // Auth to get access token
    let body = json!({
        "UserPoolId": pool_id,
        "ClientId": client_id,
        "AuthFlow": "ADMIN_NO_SRP_AUTH",
        "AuthParameters": {"USERNAME": "webauthn-user", "PASSWORD": "Pass123!"},
    });
    let req = make_req("AdminInitiateAuth", &body.to_string());
    let resp = block_on(svc.admin_initiate_auth(&req)).unwrap();
    let b = resp_json(&resp);
    let token = b["AuthenticationResult"]["AccessToken"]
        .as_str()
        .unwrap()
        .to_string();

    // Start registration
    let body = json!({"AccessToken": token});
    let req = make_req("StartWebAuthnRegistration", &body.to_string());
    let resp = svc.start_web_authn_registration(&req).unwrap();
    let b = resp_json(&resp);
    assert!(b["CredentialCreationOptions"].is_object());

    // List (should be empty before completion)
    let body = json!({"AccessToken": token});
    let req = make_req("ListWebAuthnCredentials", &body.to_string());
    let resp = svc.list_web_authn_credentials(&req).unwrap();
    let b = resp_json(&resp);
    assert!(b["Credentials"].as_array().unwrap().is_empty());
}

// ── Legacy Operations ──

#[test]
fn admin_set_user_settings_legacy() {
    let (svc, _) = make_svc();
    let pool_id = create_pool(&svc);
    admin_create_user_helper(&svc, &pool_id, "legacy-user");

    let body = json!({
        "UserPoolId": pool_id,
        "Username": "legacy-user",
        "MFAOptions": [{"DeliveryMedium": "SMS", "AttributeName": "phone_number"}],
    });
    let req = make_req("AdminSetUserSettings", &body.to_string());
    svc.admin_set_user_settings(&req).unwrap();
}

#[test]
fn admin_set_user_settings_pool_not_found() {
    let (svc, _) = make_svc();

    let body = json!({
        "UserPoolId": "us-east-1_NONEXIST",
        "Username": "ghost",
        "MFAOptions": [],
    });
    let req = make_req("AdminSetUserSettings", &body.to_string());
    let err = expect_err(svc.admin_set_user_settings(&req));
    assert_eq!(err.code(), "ResourceNotFoundException");
}

#[test]
fn admin_set_user_settings_user_not_found() {
    let (svc, _) = make_svc();
    let pool_id = create_pool(&svc);
    // Add a user so the pool has a users entry, then test with a different username
    admin_create_user_helper(&svc, &pool_id, "exists");

    let body = json!({
        "UserPoolId": pool_id,
        "Username": "ghost",
        "MFAOptions": [],
    });
    let req = make_req("AdminSetUserSettings", &body.to_string());
    let err = expect_err(svc.admin_set_user_settings(&req));
    assert_eq!(err.code(), "UserNotFoundException");
}

#[test]
fn admin_link_provider_for_user() {
    let (svc, _) = make_svc();
    let pool_id = create_pool(&svc);
    admin_create_user_helper(&svc, &pool_id, "link-user");

    let body = json!({
        "UserPoolId": pool_id,
        "DestinationUser": {"ProviderName": "Cognito", "ProviderAttributeValue": "link-user"},
        "SourceUser": {"ProviderName": "Google", "ProviderAttributeName": "Cognito_Subject", "ProviderAttributeValue": "google-sub-123"},
    });
    let req = make_req("AdminLinkProviderForUser", &body.to_string());
    svc.admin_link_provider_for_user(&req).unwrap();
}

#[test]
fn admin_disable_provider_for_user() {
    let (svc, _) = make_svc();
    let pool_id = create_pool(&svc);
    admin_create_user_helper(&svc, &pool_id, "prov-user");

    // Link a provider first
    let body = json!({
        "UserPoolId": pool_id,
        "DestinationUser": {"ProviderName": "Cognito", "ProviderAttributeValue": "prov-user"},
        "SourceUser": {"ProviderName": "Facebook", "ProviderAttributeName": "Cognito_Subject", "ProviderAttributeValue": "fb-123"},
    });
    let req = make_req("AdminLinkProviderForUser", &body.to_string());
    svc.admin_link_provider_for_user(&req).unwrap();

    // Disable
    let body = json!({
        "UserPoolId": pool_id,
        "User": {"ProviderName": "Facebook", "ProviderAttributeName": "Cognito_Subject", "ProviderAttributeValue": "fb-123"},
    });
    let req = make_req("AdminDisableProviderForUser", &body.to_string());
    svc.admin_disable_provider_for_user(&req).unwrap();
}

#[test]
fn admin_list_user_auth_events() {
    let (svc, _) = make_svc();
    let pool_id = create_pool(&svc);
    admin_create_user_helper(&svc, &pool_id, "event-user");

    let body = json!({
        "UserPoolId": pool_id,
        "Username": "event-user",
    });
    let req = make_req("AdminListUserAuthEvents", &body.to_string());
    let resp = svc.admin_list_user_auth_events(&req).unwrap();
    let b = resp_json(&resp);
    assert!(b["AuthEvents"].as_array().is_some());
}

#[test]
fn admin_update_auth_event_feedback_user_not_found() {
    let (svc, _) = make_svc();
    let pool_id = create_pool(&svc);

    let body = json!({
        "UserPoolId": pool_id,
        "Username": "nope",
        "EventId": "fake-event-id",
        "FeedbackValue": "Valid",
    });
    let req = make_req("AdminUpdateAuthEventFeedback", &body.to_string());
    let err = expect_err(svc.admin_update_auth_event_feedback(&req));
    assert_eq!(err.code(), "UserNotFoundException");
}

#[test]
fn admin_update_auth_event_feedback_event_not_found() {
    let (svc, _) = make_svc();
    let pool_id = create_pool(&svc);
    admin_create_user_helper(&svc, &pool_id, "feedback-user");

    let body = json!({
        "UserPoolId": pool_id,
        "Username": "feedback-user",
        "EventId": "fake-event-id",
        "FeedbackValue": "Valid",
    });
    let req = make_req("AdminUpdateAuthEventFeedback", &body.to_string());
    let err = expect_err(svc.admin_update_auth_event_feedback(&req));
    assert_eq!(err.code(), "ResourceNotFoundException");
}

#[test]
fn set_user_settings_legacy_invalid_token() {
    let (svc, _) = make_svc();

    let body = json!({
        "AccessToken": "invalid-token",
        "MFAOptions": [],
    });
    let req = make_req("SetUserSettings", &body.to_string());
    let err = expect_err(svc.set_user_settings(&req));
    assert_eq!(err.code(), "NotAuthorizedException");
}

// ── Auth flow: forgot password + confirm ──

#[test]
fn forgot_password_and_confirm() {
    let (svc, state) = make_svc();
    let pool_id = create_pool(&svc);
    let client_id = create_client(&svc, &pool_id);
    admin_create_user_helper(&svc, &pool_id, "forgot-user");
    set_user_password(&svc, &pool_id, "forgot-user", "OldPass1!");

    // ForgotPassword
    let body = json!({
        "ClientId": client_id,
        "Username": "forgot-user",
    });
    let req = make_req("ForgotPassword", &body.to_string());
    let resp = block_on(svc.forgot_password(&req)).unwrap();
    let b = resp_json(&resp);
    assert!(b["CodeDeliveryDetails"]["Destination"].as_str().is_some());

    // Get confirmation code from state
    let code = {
        let _mas = state.read();
        let s = _mas.default_ref();
        let users = s.users.get(&pool_id).unwrap();
        users
            .get("forgot-user")
            .unwrap()
            .confirmation_code
            .clone()
            .unwrap()
    };

    // ConfirmForgotPassword
    let body = json!({
        "ClientId": client_id,
        "Username": "forgot-user",
        "ConfirmationCode": code,
        "Password": "NewPass1!",
    });
    let req = make_req("ConfirmForgotPassword", &body.to_string());
    svc.confirm_forgot_password(&req).unwrap();

    // Verify can auth with new password
    let body = json!({
        "UserPoolId": pool_id,
        "ClientId": client_id,
        "AuthFlow": "ADMIN_NO_SRP_AUTH",
        "AuthParameters": {"USERNAME": "forgot-user", "PASSWORD": "NewPass1!"},
    });
    let req = make_req("AdminInitiateAuth", &body.to_string());
    let resp = block_on(svc.admin_initiate_auth(&req)).unwrap();
    let b = resp_json(&resp);
    assert!(b["AuthenticationResult"]["AccessToken"].as_str().is_some());
}

#[test]
fn forgot_password_user_not_found() {
    let (svc, _) = make_svc();
    let pool_id = create_pool(&svc);
    let client_id = create_client(&svc, &pool_id);

    let body = json!({"ClientId": client_id, "Username": "ghost"});
    let req = make_req("ForgotPassword", &body.to_string());
    let err = expect_err(block_on(svc.forgot_password(&req)));
    assert_eq!(err.code(), "UserNotFoundException");
}

#[test]
fn confirm_forgot_password_wrong_code() {
    let (svc, _) = make_svc();
    let pool_id = create_pool(&svc);
    let client_id = create_client(&svc, &pool_id);
    admin_create_user_helper(&svc, &pool_id, "badcode");
    set_user_password(&svc, &pool_id, "badcode", "OldPass1!");

    block_on(svc.forgot_password(&make_req(
        "ForgotPassword",
        &json!({"ClientId": client_id, "Username": "badcode"}).to_string(),
    )))
    .unwrap();

    let body = json!({
        "ClientId": client_id,
        "Username": "badcode",
        "ConfirmationCode": "000000",
        "Password": "NewPass1!",
    });
    let req = make_req("ConfirmForgotPassword", &body.to_string());
    let err = expect_err(svc.confirm_forgot_password(&req));
    assert_eq!(err.code(), "CodeMismatchException");
}

// ── Change password ──

#[test]
fn change_password_success() {
    let (svc, _) = make_svc();
    let pool_id = create_pool(&svc);
    let client_id = create_client(&svc, &pool_id);
    admin_create_user_helper(&svc, &pool_id, "chpw");
    set_user_password(&svc, &pool_id, "chpw", "OldPass1!");

    // Auth to get access token
    let body = json!({
        "UserPoolId": pool_id,
        "ClientId": client_id,
        "AuthFlow": "ADMIN_NO_SRP_AUTH",
        "AuthParameters": {"USERNAME": "chpw", "PASSWORD": "OldPass1!"},
    });
    let req = make_req("AdminInitiateAuth", &body.to_string());
    let resp = block_on(svc.admin_initiate_auth(&req)).unwrap();
    let b = resp_json(&resp);
    let token = b["AuthenticationResult"]["AccessToken"]
        .as_str()
        .unwrap()
        .to_string();

    // Change password
    let body = json!({
        "AccessToken": token,
        "PreviousPassword": "OldPass1!",
        "ProposedPassword": "NewPass1!",
    });
    let req = make_req("ChangePassword", &body.to_string());
    svc.change_password(&req).unwrap();
}

#[test]
fn change_password_wrong_old_password() {
    let (svc, _) = make_svc();
    let pool_id = create_pool(&svc);
    let client_id = create_client(&svc, &pool_id);
    admin_create_user_helper(&svc, &pool_id, "chpw2");
    set_user_password(&svc, &pool_id, "chpw2", "OldPass1!");

    let body = json!({
        "UserPoolId": pool_id,
        "ClientId": client_id,
        "AuthFlow": "ADMIN_NO_SRP_AUTH",
        "AuthParameters": {"USERNAME": "chpw2", "PASSWORD": "OldPass1!"},
    });
    let req = make_req("AdminInitiateAuth", &body.to_string());
    let resp = block_on(svc.admin_initiate_auth(&req)).unwrap();
    let b = resp_json(&resp);
    let token = b["AuthenticationResult"]["AccessToken"]
        .as_str()
        .unwrap()
        .to_string();

    let body = json!({
        "AccessToken": token,
        "PreviousPassword": "WrongPass1!",
        "ProposedPassword": "NewPass1!",
    });
    let req = make_req("ChangePassword", &body.to_string());
    let err = expect_err(svc.change_password(&req));
    assert_eq!(err.code(), "NotAuthorizedException");
}

#[test]
fn change_password_invalid_token() {
    let (svc, _) = make_svc();
    let body = json!({
        "AccessToken": "bad-token",
        "PreviousPassword": "Old1!",
        "ProposedPassword": "New1!",
    });
    let req = make_req("ChangePassword", &body.to_string());
    let err = expect_err(svc.change_password(&req));
    assert_eq!(err.code(), "NotAuthorizedException");
}

// ── Global sign out ──

#[test]
fn global_sign_out_success() {
    let (svc, _) = make_svc();
    let pool_id = create_pool(&svc);
    let client_id = create_client(&svc, &pool_id);
    admin_create_user_helper(&svc, &pool_id, "signout");
    set_user_password(&svc, &pool_id, "signout", "Password1!");

    let body = json!({
        "UserPoolId": pool_id,
        "ClientId": client_id,
        "AuthFlow": "ADMIN_NO_SRP_AUTH",
        "AuthParameters": {"USERNAME": "signout", "PASSWORD": "Password1!"},
    });
    let req = make_req("AdminInitiateAuth", &body.to_string());
    let resp = block_on(svc.admin_initiate_auth(&req)).unwrap();
    let b = resp_json(&resp);
    let token = b["AuthenticationResult"]["AccessToken"]
        .as_str()
        .unwrap()
        .to_string();

    let body = json!({"AccessToken": token});
    let req = make_req("GlobalSignOut", &body.to_string());
    svc.global_sign_out(&req).unwrap();
}

#[test]
fn global_sign_out_invalid_token() {
    let (svc, _) = make_svc();
    let body = json!({"AccessToken": "bad-token"});
    let req = make_req("GlobalSignOut", &body.to_string());
    let err = expect_err(svc.global_sign_out(&req));
    assert_eq!(err.code(), "NotAuthorizedException");
}

#[test]
fn admin_user_global_sign_out() {
    let (svc, _) = make_svc();
    let pool_id = create_pool(&svc);
    admin_create_user_helper(&svc, &pool_id, "adm-signout");

    let body = json!({"UserPoolId": pool_id, "Username": "adm-signout"});
    let req = make_req("AdminUserGlobalSignOut", &body.to_string());
    svc.admin_user_global_sign_out(&req).unwrap();
}

#[test]
fn admin_user_global_sign_out_user_not_found() {
    let (svc, _) = make_svc();
    let pool_id = create_pool(&svc);

    let body = json!({"UserPoolId": pool_id, "Username": "ghost"});
    let req = make_req("AdminUserGlobalSignOut", &body.to_string());
    let err = expect_err(svc.admin_user_global_sign_out(&req));
    assert_eq!(err.code(), "UserNotFoundException");
}

// ── InitiateAuth (user-facing, not admin) ──

#[test]
fn initiate_auth_user_password() {
    let (svc, _) = make_svc();
    let pool_id = create_pool(&svc);
    let client_id = create_client(&svc, &pool_id);
    admin_create_user_helper(&svc, &pool_id, "userauth");
    set_user_password(&svc, &pool_id, "userauth", "Password1!");

    let body = json!({
        "ClientId": client_id,
        "AuthFlow": "USER_PASSWORD_AUTH",
        "AuthParameters": {"USERNAME": "userauth", "PASSWORD": "Password1!"},
    });
    let req = make_req("InitiateAuth", &body.to_string());
    let resp = block_on(svc.initiate_auth(&req)).unwrap();
    let b = resp_json(&resp);
    assert!(b["AuthenticationResult"]["AccessToken"].as_str().is_some());
}

#[test]
fn initiate_auth_wrong_password() {
    let (svc, _) = make_svc();
    let pool_id = create_pool(&svc);
    let client_id = create_client(&svc, &pool_id);
    admin_create_user_helper(&svc, &pool_id, "badpw");
    set_user_password(&svc, &pool_id, "badpw", "Password1!");

    let body = json!({
        "ClientId": client_id,
        "AuthFlow": "USER_PASSWORD_AUTH",
        "AuthParameters": {"USERNAME": "badpw", "PASSWORD": "Wrong1!"},
    });
    let req = make_req("InitiateAuth", &body.to_string());
    let err = expect_err(block_on(svc.initiate_auth(&req)));
    assert_eq!(err.code(), "NotAuthorizedException");
}

#[test]
fn initiate_auth_user_not_found() {
    let (svc, _) = make_svc();
    let pool_id = create_pool(&svc);
    let client_id = create_client(&svc, &pool_id);

    let body = json!({
        "ClientId": client_id,
        "AuthFlow": "USER_PASSWORD_AUTH",
        "AuthParameters": {"USERNAME": "ghost", "PASSWORD": "Password1!"},
    });
    let req = make_req("InitiateAuth", &body.to_string());
    let err = expect_err(block_on(svc.initiate_auth(&req)));
    assert_eq!(err.code(), "NotAuthorizedException");
}

// ── Helper: auth and get token ──

fn auth_get_token(
    svc: &CognitoService,
    pool_id: &str,
    client_id: &str,
    username: &str,
    password: &str,
) -> String {
    let body = json!({
        "UserPoolId": pool_id,
        "ClientId": client_id,
        "AuthFlow": "ADMIN_NO_SRP_AUTH",
        "AuthParameters": {"USERNAME": username, "PASSWORD": password},
    });
    let resp = block_on(svc.admin_initiate_auth(&make_req("AdminInitiateAuth", &body.to_string())))
        .unwrap();
    let b = resp_json(&resp);
    b["AuthenticationResult"]["AccessToken"]
        .as_str()
        .unwrap()
        .to_string()
}

// ── cognito/misc.rs coverage ──

#[test]
fn revoke_token_invalid() {
    let (svc, _) = make_svc();
    let pool_id = create_pool(&svc);
    let client_id = create_client(&svc, &pool_id);

    let body = json!({"Token": "invalid-refresh-token", "ClientId": client_id});
    let req = make_req("RevokeToken", &body.to_string());
    // RevokeToken should handle gracefully
    let _ = svc.revoke_token(&req);
}

#[test]
fn get_tokens_from_refresh_token_invalid() {
    let (svc, _) = make_svc();
    let pool_id = create_pool(&svc);
    let client_id = create_client(&svc, &pool_id);

    let body = json!({
        "ClientId": client_id,
        "AuthFlow": "REFRESH_TOKEN_AUTH",
        "AuthParameters": {"REFRESH_TOKEN": "invalid-token"},
    });
    let req = make_req("InitiateAuth", &body.to_string());
    let err = expect_err(block_on(svc.initiate_auth(&req)));
    assert_eq!(err.code(), "NotAuthorizedException");
}

#[test]
fn get_csv_header() {
    let (svc, _) = make_svc();
    let pool_id = create_pool(&svc);

    let body = json!({"UserPoolId": pool_id});
    let req = make_req("GetCSVHeader", &body.to_string());
    let resp = svc.get_csv_header(&req).unwrap();
    let b = resp_json(&resp);
    assert!(b["CSVHeader"].as_array().is_some());
    assert!(b["UserPoolId"].as_str().is_some());
}

#[test]
fn start_and_stop_user_import_job() {
    let (svc, _) = make_svc();
    let pool_id = create_pool(&svc);

    // Create import job
    let body = json!({
        "UserPoolId": pool_id,
        "JobName": "import-1",
        "CloudWatchLogsRoleArn": "arn:aws:iam::123456789012:role/CognitoImport",
    });
    let req = make_req("CreateUserImportJob", &body.to_string());
    let resp = svc.create_user_import_job(&req).unwrap();
    let b = resp_json(&resp);
    let job_id = b["UserImportJob"]["JobId"].as_str().unwrap().to_string();

    // Start
    let body = json!({"UserPoolId": pool_id, "JobId": job_id});
    let req = make_req("StartUserImportJob", &body.to_string());
    svc.start_user_import_job(&req).unwrap();

    // Stop
    let body = json!({"UserPoolId": pool_id, "JobId": job_id});
    let req = make_req("StopUserImportJob", &body.to_string());
    svc.stop_user_import_job(&req).unwrap();
}

#[test]
fn confirm_device_and_list() {
    let (svc, _) = make_svc();
    let pool_id = create_pool(&svc);
    let client_id = create_client(&svc, &pool_id);
    admin_create_user_helper(&svc, &pool_id, "devuser");
    set_user_password(&svc, &pool_id, "devuser", "Password1!");
    let token = auth_get_token(&svc, &pool_id, &client_id, "devuser", "Password1!");

    // ConfirmDevice
    let body = json!({
        "AccessToken": token,
        "DeviceKey": "us-east-1_device-abc",
        "DeviceName": "My Phone",
    });
    let req = make_req("ConfirmDevice", &body.to_string());
    svc.confirm_device(&req).unwrap();

    // ListDevices
    let body = json!({"AccessToken": token});
    let req = make_req("ListDevices", &body.to_string());
    let resp = svc.list_devices(&req).unwrap();
    let b = resp_json(&resp);
    assert!(!b["Devices"].as_array().unwrap().is_empty());

    // GetDevice
    let body = json!({"AccessToken": token, "DeviceKey": "us-east-1_device-abc"});
    let req = make_req("GetDevice", &body.to_string());
    let resp = svc.get_device(&req).unwrap();
    let b = resp_json(&resp);
    assert_eq!(b["Device"]["DeviceKey"], "us-east-1_device-abc");

    // UpdateDeviceStatus
    let body = json!({
        "AccessToken": token,
        "DeviceKey": "us-east-1_device-abc",
        "DeviceRememberedStatus": "remembered",
    });
    let req = make_req("UpdateDeviceStatus", &body.to_string());
    svc.update_device_status(&req).unwrap();

    // ForgetDevice
    let body = json!({"AccessToken": token, "DeviceKey": "us-east-1_device-abc"});
    let req = make_req("ForgetDevice", &body.to_string());
    svc.forget_device(&req).unwrap();
}

#[test]
fn update_user_pool_domain() {
    let (svc, _) = make_svc();
    let pool_id = create_pool(&svc);

    svc.create_user_pool_domain(&make_req(
        "CreateUserPoolDomain",
        &json!({"UserPoolId": pool_id, "Domain": "upd-domain"}).to_string(),
    ))
    .unwrap();

    let body = json!({
        "UserPoolId": pool_id,
        "Domain": "upd-domain",
        "CustomDomainConfig": {"CertificateArn": "arn:aws:acm:us-east-1:123:cert/abc"},
    });
    let req = make_req("UpdateUserPoolDomain", &body.to_string());
    svc.update_user_pool_domain(&req).unwrap();
}

#[test]
fn device_operations_invalid_token() {
    let (svc, _) = make_svc();

    let body = json!({"AccessToken": "bad", "DeviceKey": "dk"});
    let req = make_req("GetDevice", &body.to_string());
    let err = expect_err(svc.get_device(&req));
    assert_eq!(err.code(), "NotAuthorizedException");

    let body = json!({"AccessToken": "bad"});
    let req = make_req("ListDevices", &body.to_string());
    let err = expect_err(svc.list_devices(&req));
    assert_eq!(err.code(), "NotAuthorizedException");

    let body = json!({"AccessToken": "bad", "DeviceKey": "dk"});
    let req = make_req("ForgetDevice", &body.to_string());
    let err = expect_err(svc.forget_device(&req));
    assert_eq!(err.code(), "NotAuthorizedException");
}

#[test]
fn admin_update_device_status() {
    let (svc, _) = make_svc();
    let pool_id = create_pool(&svc);
    let client_id = create_client(&svc, &pool_id);
    admin_create_user_helper(&svc, &pool_id, "admdev");
    set_user_password(&svc, &pool_id, "admdev", "Password1!");
    let token = auth_get_token(&svc, &pool_id, &client_id, "admdev", "Password1!");

    // Confirm a device first
    svc.confirm_device(&make_req(
        "ConfirmDevice",
        &json!({"AccessToken": token, "DeviceKey": "dk-1"}).to_string(),
    ))
    .unwrap();

    // AdminUpdateDeviceStatus
    let body = json!({
        "UserPoolId": pool_id,
        "Username": "admdev",
        "DeviceKey": "dk-1",
        "DeviceRememberedStatus": "not_remembered",
    });
    let req = make_req("AdminUpdateDeviceStatus", &body.to_string());
    svc.admin_update_device_status(&req).unwrap();
}

// ── auth.rs additional coverage ──

#[test]
fn admin_initiate_auth_missing_username_errors() {
    let (svc, _) = make_svc();
    let pool_id = create_pool(&svc);
    let client_id = create_client(&svc, &pool_id);
    let body = json!({
        "UserPoolId": pool_id,
        "ClientId": client_id,
        "AuthFlow": "ADMIN_NO_SRP_AUTH",
        "AuthParameters": {"PASSWORD": "Pass1!"}
    });
    let req = make_req("AdminInitiateAuth", &body.to_string());
    assert!(block_on(svc.admin_initiate_auth(&req)).is_err());
}

#[test]
fn admin_initiate_auth_missing_password_errors() {
    let (svc, _) = make_svc();
    let pool_id = create_pool(&svc);
    let client_id = create_client(&svc, &pool_id);
    admin_create_user_helper(&svc, &pool_id, "nopass");
    let body = json!({
        "UserPoolId": pool_id,
        "ClientId": client_id,
        "AuthFlow": "ADMIN_NO_SRP_AUTH",
        "AuthParameters": {"USERNAME": "nopass"}
    });
    let req = make_req("AdminInitiateAuth", &body.to_string());
    assert!(block_on(svc.admin_initiate_auth(&req)).is_err());
}

#[test]
fn initiate_auth_refresh_token_errors_on_invalid() {
    let (svc, _) = make_svc();
    let pool_id = create_pool(&svc);
    let client_id = create_client(&svc, &pool_id);
    let _ = pool_id;
    let body = json!({
        "ClientId": client_id,
        "AuthFlow": "REFRESH_TOKEN_AUTH",
        "AuthParameters": {"REFRESH_TOKEN": "bad-token"}
    });
    let req = make_req("InitiateAuth", &body.to_string());
    assert!(block_on(svc.initiate_auth(&req)).is_err());
}

#[test]
fn initiate_auth_missing_client_id_errors() {
    let (svc, _) = make_svc();
    let body = json!({
        "AuthFlow": "USER_PASSWORD_AUTH",
        "AuthParameters": {"USERNAME": "u", "PASSWORD": "p"}
    });
    let req = make_req("InitiateAuth", &body.to_string());
    assert!(block_on(svc.initiate_auth(&req)).is_err());
}

#[test]
fn initiate_auth_unknown_client_errors() {
    let (svc, _) = make_svc();
    let body = json!({
        "ClientId": "ghost-client",
        "AuthFlow": "USER_PASSWORD_AUTH",
        "AuthParameters": {"USERNAME": "u", "PASSWORD": "p"}
    });
    let req = make_req("InitiateAuth", &body.to_string());
    assert!(block_on(svc.initiate_auth(&req)).is_err());
}

#[test]
fn sign_up_missing_username_errors() {
    let (svc, _) = make_svc();
    let pool_id = create_pool(&svc);
    let client_id = create_client(&svc, &pool_id);
    let body = json!({
        "ClientId": client_id,
        "Password": "Password1!"
    });
    let req = make_req("SignUp", &body.to_string());
    assert!(block_on(svc.sign_up(&req)).is_err());
}

#[test]
fn sign_up_missing_password_errors() {
    let (svc, _) = make_svc();
    let pool_id = create_pool(&svc);
    let client_id = create_client(&svc, &pool_id);
    let body = json!({
        "ClientId": client_id,
        "Username": "u"
    });
    let req = make_req("SignUp", &body.to_string());
    assert!(block_on(svc.sign_up(&req)).is_err());
}

#[test]
fn sign_up_weak_password_errors() {
    let (svc, _) = make_svc();
    let pool_id = create_pool(&svc);
    let client_id = create_client(&svc, &pool_id);
    let body = json!({
        "ClientId": client_id,
        "Username": "weak",
        "Password": "weak"
    });
    let req = make_req("SignUp", &body.to_string());
    assert!(block_on(svc.sign_up(&req)).is_err());
}

#[test]
fn confirm_sign_up_unknown_user_errors() {
    let (svc, _) = make_svc();
    let pool_id = create_pool(&svc);
    let client_id = create_client(&svc, &pool_id);
    let _ = pool_id;
    let body = json!({
        "ClientId": client_id,
        "Username": "ghost",
        "ConfirmationCode": "123456"
    });
    let req = make_req("ConfirmSignUp", &body.to_string());
    assert!(block_on(svc.confirm_sign_up(&req)).is_err());
}

#[test]
fn admin_confirm_sign_up_unknown_user_errors() {
    let (svc, _) = make_svc();
    let pool_id = create_pool(&svc);
    let body = json!({
        "UserPoolId": pool_id,
        "Username": "ghost"
    });
    let req = make_req("AdminConfirmSignUp", &body.to_string());
    assert!(block_on(svc.admin_confirm_sign_up(&req)).is_err());
}

#[test]
fn admin_reset_user_password_missing_user_errors() {
    let (svc, _) = make_svc();
    let pool_id = create_pool(&svc);
    let body = json!({"UserPoolId": pool_id, "Username": "ghost"});
    let req = make_req("AdminResetUserPassword", &body.to_string());
    assert!(svc.admin_reset_user_password(&req).is_err());
}

#[test]
fn forgot_password_missing_client_errors() {
    let (svc, _) = make_svc();
    let body = json!({"Username": "u"});
    let req = make_req("ForgotPassword", &body.to_string());
    assert!(block_on(svc.forgot_password(&req)).is_err());
}

#[test]
fn confirm_forgot_password_unknown_user_errors() {
    let (svc, _) = make_svc();
    let pool_id = create_pool(&svc);
    let client_id = create_client(&svc, &pool_id);
    let _ = pool_id;
    let body = json!({
        "ClientId": client_id,
        "Username": "ghost",
        "ConfirmationCode": "123456",
        "Password": "NewPass1!"
    });
    let req = make_req("ConfirmForgotPassword", &body.to_string());
    assert!(svc.confirm_forgot_password(&req).is_err());
}

#[test]
fn change_password_missing_access_token_errors() {
    let (svc, _) = make_svc();
    let body = json!({
        "PreviousPassword": "p1",
        "ProposedPassword": "p2"
    });
    let req = make_req("ChangePassword", &body.to_string());
    assert!(svc.change_password(&req).is_err());
}

#[test]
fn global_sign_out_missing_token_errors() {
    let (svc, _) = make_svc();
    let body = json!({});
    let req = make_req("GlobalSignOut", &body.to_string());
    assert!(svc.global_sign_out(&req).is_err());
}

// ── groups.rs error branches ──

#[test]
fn create_group_duplicate_errors_g() {
    let (svc, _) = make_svc();
    let pool_id = create_pool(&svc);
    let body = json!({"UserPoolId": pool_id, "GroupName": "dup"});
    let req = make_req("CreateGroup", &body.to_string());
    svc.create_group(&req).unwrap();
    let req = make_req("CreateGroup", &body.to_string());
    assert!(svc.create_group(&req).is_err());
}

#[test]
fn get_group_not_found_g() {
    let (svc, _) = make_svc();
    let pool_id = create_pool(&svc);
    let body = json!({"UserPoolId": pool_id, "GroupName": "ghost"});
    let req = make_req("GetGroup", &body.to_string());
    assert!(svc.get_group(&req).is_err());
}

#[test]
fn update_group_not_found_g() {
    let (svc, _) = make_svc();
    let pool_id = create_pool(&svc);
    let body = json!({"UserPoolId": pool_id, "GroupName": "ghost"});
    let req = make_req("UpdateGroup", &body.to_string());
    assert!(svc.update_group(&req).is_err());
}

#[test]
fn delete_group_not_found_g() {
    let (svc, _) = make_svc();
    let pool_id = create_pool(&svc);
    let body = json!({"UserPoolId": pool_id, "GroupName": "ghost"});
    let req = make_req("DeleteGroup", &body.to_string());
    assert!(svc.delete_group(&req).is_err());
}

#[test]
fn admin_add_user_to_unknown_group_errors() {
    let (svc, _) = make_svc();
    let pool_id = create_pool(&svc);
    admin_create_user_helper(&svc, &pool_id, "u1");
    let body = json!({
        "UserPoolId": pool_id,
        "Username": "u1",
        "GroupName": "ghost"
    });
    let req = make_req("AdminAddUserToGroup", &body.to_string());
    assert!(svc.admin_add_user_to_group(&req).is_err());
}

#[test]
fn admin_add_unknown_user_to_group_errors() {
    let (svc, _) = make_svc();
    let pool_id = create_pool(&svc);
    svc.create_group(&make_req(
        "CreateGroup",
        &json!({"UserPoolId": pool_id, "GroupName": "g1"}).to_string(),
    ))
    .unwrap();
    let body = json!({
        "UserPoolId": pool_id,
        "Username": "ghost",
        "GroupName": "g1"
    });
    let req = make_req("AdminAddUserToGroup", &body.to_string());
    assert!(svc.admin_add_user_to_group(&req).is_err());
}

#[test]
fn admin_remove_user_from_group_not_found() {
    let (svc, _) = make_svc();
    let pool_id = create_pool(&svc);
    let body = json!({
        "UserPoolId": pool_id,
        "Username": "ghost",
        "GroupName": "ghost"
    });
    let req = make_req("AdminRemoveUserFromGroup", &body.to_string());
    assert!(svc.admin_remove_user_from_group(&req).is_err());
}

// ── identity_providers.rs ──

#[test]
fn create_identity_provider_duplicate_errors() {
    let (svc, _) = make_svc();
    let pool_id = create_pool(&svc);
    let body = json!({
        "UserPoolId": pool_id,
        "ProviderName": "dup",
        "ProviderType": "Google",
        "ProviderDetails": {}
    });
    svc.create_identity_provider(&make_req("CreateIdentityProvider", &body.to_string()))
        .unwrap();
    assert!(svc
        .create_identity_provider(&make_req("CreateIdentityProvider", &body.to_string()))
        .is_err());
}

#[test]
fn describe_identity_provider_not_found() {
    let (svc, _) = make_svc();
    let pool_id = create_pool(&svc);
    let body = json!({"UserPoolId": pool_id, "ProviderName": "ghost"});
    let req = make_req("DescribeIdentityProvider", &body.to_string());
    assert!(svc.describe_identity_provider(&req).is_err());
}

#[test]
fn delete_identity_provider_not_found() {
    let (svc, _) = make_svc();
    let pool_id = create_pool(&svc);
    let body = json!({"UserPoolId": pool_id, "ProviderName": "ghost"});
    let req = make_req("DeleteIdentityProvider", &body.to_string());
    assert!(svc.delete_identity_provider(&req).is_err());
}

#[test]
fn update_identity_provider_not_found() {
    let (svc, _) = make_svc();
    let pool_id = create_pool(&svc);
    let body = json!({
        "UserPoolId": pool_id,
        "ProviderName": "ghost",
        "ProviderDetails": {}
    });
    let req = make_req("UpdateIdentityProvider", &body.to_string());
    assert!(svc.update_identity_provider(&req).is_err());
}

// ── resource_servers.rs ──

#[test]
fn describe_resource_server_not_found() {
    let (svc, _) = make_svc();
    let pool_id = create_pool(&svc);
    let body = json!({"UserPoolId": pool_id, "Identifier": "ghost"});
    let req = make_req("DescribeResourceServer", &body.to_string());
    assert!(svc.describe_resource_server(&req).is_err());
}

#[test]
fn update_resource_server_not_found() {
    let (svc, _) = make_svc();
    let pool_id = create_pool(&svc);
    let body = json!({
        "UserPoolId": pool_id,
        "Identifier": "ghost",
        "Name": "x",
        "Scopes": []
    });
    let req = make_req("UpdateResourceServer", &body.to_string());
    assert!(svc.update_resource_server(&req).is_err());
}

#[test]
fn delete_resource_server_not_found() {
    let (svc, _) = make_svc();
    let pool_id = create_pool(&svc);
    let body = json!({"UserPoolId": pool_id, "Identifier": "ghost"});
    let req = make_req("DeleteResourceServer", &body.to_string());
    assert!(svc.delete_resource_server(&req).is_err());
}

// ── user_pools.rs error branches ──

#[test]
fn describe_user_pool_not_found() {
    let (svc, _) = make_svc();
    let body = json!({"UserPoolId": "us-east-1_ghost000"});
    let req = make_req("DescribeUserPool", &body.to_string());
    assert!(svc.describe_user_pool(&req).is_err());
}

#[test]
fn delete_user_pool_not_found() {
    let (svc, _) = make_svc();
    let body = json!({"UserPoolId": "us-east-1_ghost000"});
    let req = make_req("DeleteUserPool", &body.to_string());
    assert!(svc.delete_user_pool(&req).is_err());
}

#[test]
fn update_user_pool_not_found() {
    let (svc, _) = make_svc();
    let body = json!({"UserPoolId": "us-east-1_ghost000"});
    let req = make_req("UpdateUserPool", &body.to_string());
    assert!(svc.update_user_pool(&req).is_err());
}

#[test]
fn describe_user_pool_client_not_found() {
    let (svc, _) = make_svc();
    let pool_id = create_pool(&svc);
    let body = json!({"UserPoolId": pool_id, "ClientId": "ghost"});
    let req = make_req("DescribeUserPoolClient", &body.to_string());
    assert!(svc.describe_user_pool_client(&req).is_err());
}

#[test]
fn delete_user_pool_client_not_found() {
    let (svc, _) = make_svc();
    let pool_id = create_pool(&svc);
    let body = json!({"UserPoolId": pool_id, "ClientId": "ghost"});
    let req = make_req("DeleteUserPoolClient", &body.to_string());
    assert!(svc.delete_user_pool_client(&req).is_err());
}

#[test]
fn update_user_pool_client_not_found() {
    let (svc, _) = make_svc();
    let pool_id = create_pool(&svc);
    let body = json!({"UserPoolId": pool_id, "ClientId": "ghost"});
    let req = make_req("UpdateUserPoolClient", &body.to_string());
    assert!(svc.update_user_pool_client(&req).is_err());
}

#[test]
fn admin_get_device_not_found() {
    let (svc, _) = make_svc();
    let pool_id = create_pool(&svc);
    let body = json!({
        "UserPoolId": pool_id,
        "Username": "ghost",
        "DeviceKey": "dk-ghost"
    });
    let req = make_req("AdminGetDevice", &body.to_string());
    assert!(svc.admin_get_device(&req).is_err());
}

#[test]
fn admin_list_devices_unknown_user_errors() {
    let (svc, _) = make_svc();
    let pool_id = create_pool(&svc);
    let body = json!({"UserPoolId": pool_id, "Username": "ghost"});
    let req = make_req("AdminListDevices", &body.to_string());
    assert!(svc.admin_list_devices(&req).is_err());
}

#[test]
fn admin_forget_device_unknown_user_errors() {
    let (svc, _) = make_svc();
    let pool_id = create_pool(&svc);
    let body = json!({
        "UserPoolId": pool_id,
        "Username": "ghost",
        "DeviceKey": "dk"
    });
    let req = make_req("AdminForgetDevice", &body.to_string());
    assert!(svc.admin_forget_device(&req).is_err());
}

#[test]
fn revoke_token_invalid_errors() {
    let (svc, _) = make_svc();
    let body = json!({"ClientId": "c", "Token": "bogus"});
    let req = make_req("RevokeToken", &body.to_string());
    assert!(svc.revoke_token(&req).is_err());
}

#[test]
fn describe_user_import_job_not_found() {
    let (svc, _) = make_svc();
    let pool_id = create_pool(&svc);
    let body = json!({"UserPoolId": pool_id, "JobId": "ghost"});
    let req = make_req("DescribeUserImportJob", &body.to_string());
    assert!(svc.describe_user_import_job(&req).is_err());
}

#[test]
fn start_user_import_job_not_found() {
    let (svc, _) = make_svc();
    let pool_id = create_pool(&svc);
    let body = json!({"UserPoolId": pool_id, "JobId": "ghost"});
    let req = make_req("StartUserImportJob", &body.to_string());
    assert!(svc.start_user_import_job(&req).is_err());
}

#[test]
fn stop_user_import_job_not_found() {
    let (svc, _) = make_svc();
    let pool_id = create_pool(&svc);
    let body = json!({"UserPoolId": pool_id, "JobId": "ghost"});
    let req = make_req("StopUserImportJob", &body.to_string());
    assert!(svc.stop_user_import_job(&req).is_err());
}

#[test]
fn get_csv_header_basic() {
    let (svc, _) = make_svc();
    let pool_id = create_pool(&svc);
    let body = json!({"UserPoolId": pool_id});
    let req = make_req("GetCSVHeader", &body.to_string());
    let resp = svc.get_csv_header(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert!(body["CSVHeader"].is_array());
}

#[test]
fn admin_disable_user_not_found() {
    let (svc, _) = make_svc();
    let pool_id = create_pool(&svc);
    let body = json!({"UserPoolId": pool_id, "Username": "ghost"});
    let req = make_req("AdminDisableUser", &body.to_string());
    assert!(svc.admin_disable_user(&req).is_err());
}

#[test]
fn admin_enable_user_not_found() {
    let (svc, _) = make_svc();
    let pool_id = create_pool(&svc);
    let body = json!({"UserPoolId": pool_id, "Username": "ghost"});
    let req = make_req("AdminEnableUser", &body.to_string());
    assert!(svc.admin_enable_user(&req).is_err());
}

#[test]
fn admin_update_user_attributes_not_found() {
    let (svc, _) = make_svc();
    let pool_id = create_pool(&svc);
    let body = json!({
        "UserPoolId": pool_id,
        "Username": "ghost",
        "UserAttributes": [{"Name": "email", "Value": "x@y.com"}]
    });
    let req = make_req("AdminUpdateUserAttributes", &body.to_string());
    assert!(svc.admin_update_user_attributes(&req).is_err());
}

#[test]
fn admin_delete_user_attributes_not_found() {
    let (svc, _) = make_svc();
    let pool_id = create_pool(&svc);
    let body = json!({
        "UserPoolId": pool_id,
        "Username": "ghost",
        "UserAttributeNames": ["email"]
    });
    let req = make_req("AdminDeleteUserAttributes", &body.to_string());
    assert!(svc.admin_delete_user_attributes(&req).is_err());
}

#[test]
fn admin_set_user_password_not_found() {
    let (svc, _) = make_svc();
    let pool_id = create_pool(&svc);
    let body = json!({
        "UserPoolId": pool_id,
        "Username": "ghost",
        "Password": "Pass1!",
        "Permanent": true
    });
    let req = make_req("AdminSetUserPassword", &body.to_string());
    assert!(svc.admin_set_user_password(&req).is_err());
}

#[test]
fn list_users_pool_not_found() {
    let (svc, _) = make_svc();
    let body = json!({"UserPoolId": "us-east-1_ghost000"});
    let req = make_req("ListUsers", &body.to_string());
    assert!(svc.list_users(&req).is_err());
}

#[test]
fn get_user_invalid_token() {
    let (svc, _) = make_svc();
    let body = json!({"AccessToken": "bogus"});
    let req = make_req("GetUser", &body.to_string());
    assert!(svc.get_user(&req).is_err());
}

#[test]
fn delete_user_invalid_token() {
    let (svc, _) = make_svc();
    let body = json!({"AccessToken": "bogus"});
    let req = make_req("DeleteUser", &body.to_string());
    assert!(svc.delete_user(&req).is_err());
}

#[test]
fn update_user_attributes_invalid_token() {
    let (svc, _) = make_svc();
    let body = json!({
        "AccessToken": "bogus",
        "UserAttributes": [{"Name": "email", "Value": "x@y.com"}]
    });
    let req = make_req("UpdateUserAttributes", &body.to_string());
    assert!(svc.update_user_attributes(&req).is_err());
}

#[test]
fn delete_user_attributes_invalid_token() {
    let (svc, _) = make_svc();
    let body = json!({
        "AccessToken": "bogus",
        "UserAttributeNames": ["email"]
    });
    let req = make_req("DeleteUserAttributes", &body.to_string());
    assert!(svc.delete_user_attributes(&req).is_err());
}

// ── UI / Log / Risk configuration (config.rs) ──

#[test]
fn ui_customization_client_specific_falls_back_to_pool_level() {
    let (svc, _) = make_svc();
    let pool_id = create_pool(&svc);

    // Set pool-level customization (empty ClientId).
    let body = json!({
        "UserPoolId": pool_id,
        "CSS": ".banner { color: blue; }",
        "ImageFile": "base64imagedata==",
    });
    let req = make_req("SetUICustomization", &body.to_string());
    let resp = svc.set_ui_customization(&req).unwrap();
    let b = resp_json(&resp);
    assert_eq!(b["UICustomization"]["ClientId"], "ALL");
    assert!(b["UICustomization"]["ImageUrl"]
        .as_str()
        .unwrap()
        .ends_with("/logo.png"));

    // Get client-specific -> falls back to pool-level CSS.
    let body = json!({"UserPoolId": pool_id, "ClientId": "client-123"});
    let req = make_req("GetUICustomization", &body.to_string());
    let resp = svc.get_ui_customization(&req).unwrap();
    let b = resp_json(&resp);
    assert_eq!(b["UICustomization"]["CSS"], ".banner { color: blue; }");
}

#[test]
fn ui_customization_default_when_not_set() {
    let (svc, _) = make_svc();
    let pool_id = create_pool(&svc);
    let body = json!({"UserPoolId": pool_id});
    let req = make_req("GetUICustomization", &body.to_string());
    let resp = svc.get_ui_customization(&req).unwrap();
    let b = resp_json(&resp);
    assert_eq!(b["UICustomization"]["UserPoolId"], pool_id);
    assert_eq!(b["UICustomization"]["ClientId"], "ALL");
    assert!(b["UICustomization"]["CSS"].is_null());
}

#[test]
fn ui_customization_rejects_unknown_pool() {
    let (svc, _) = make_svc();
    let body = json!({"UserPoolId": "us-east-1_nosuch"});
    let req = make_req("GetUICustomization", &body.to_string());
    assert!(svc.get_ui_customization(&req).is_err());
}

#[test]
fn log_delivery_configuration_set_and_get() {
    let (svc, _) = make_svc();
    let pool_id = create_pool(&svc);

    let body = json!({
        "UserPoolId": pool_id,
        "LogConfigurations": [
            {"LogLevel": "INFO", "EventSource": "userNotification",
             "CloudWatchLogsConfiguration": {"LogGroupArn": "arn:aws:logs:us-east-1:123:log-group:g"}}
        ]
    });
    let req = make_req("SetLogDeliveryConfiguration", &body.to_string());
    svc.set_log_delivery_configuration(&req).unwrap();

    let body = json!({"UserPoolId": pool_id});
    let req = make_req("GetLogDeliveryConfiguration", &body.to_string());
    let resp = svc.get_log_delivery_configuration(&req).unwrap();
    let b = resp_json(&resp);
    assert_eq!(
        b["LogDeliveryConfiguration"]["LogConfigurations"][0]["LogLevel"],
        "INFO"
    );
}

#[test]
fn log_delivery_configuration_default_when_absent() {
    let (svc, _) = make_svc();
    let pool_id = create_pool(&svc);
    let body = json!({"UserPoolId": pool_id});
    let req = make_req("GetLogDeliveryConfiguration", &body.to_string());
    let resp = svc.get_log_delivery_configuration(&req).unwrap();
    let b = resp_json(&resp);
    assert_eq!(
        b["LogDeliveryConfiguration"]["LogConfigurations"]
            .as_array()
            .unwrap()
            .len(),
        0
    );
}

#[test]
fn risk_configuration_set_and_describe_pool_level() {
    let (svc, _) = make_svc();
    let pool_id = create_pool(&svc);

    let body = json!({
        "UserPoolId": pool_id,
        "CompromisedCredentialsRiskConfiguration": {"EventFilter": ["SIGN_IN"], "Actions": {"EventAction": "BLOCK"}},
        "AccountTakeoverRiskConfiguration": {"NotifyConfiguration": {"From": "no-reply@x"}},
        "RiskExceptionConfiguration": {"BlockedIPRangeList": ["10.0.0.0/24"]}
    });
    let req = make_req("SetRiskConfiguration", &body.to_string());
    svc.set_risk_configuration(&req).unwrap();

    let body = json!({"UserPoolId": pool_id});
    let req = make_req("DescribeRiskConfiguration", &body.to_string());
    let resp = svc.describe_risk_configuration(&req).unwrap();
    let b = resp_json(&resp);
    assert_eq!(
        b["RiskConfiguration"]["CompromisedCredentialsRiskConfiguration"]["Actions"]["EventAction"],
        "BLOCK"
    );
}

#[test]
fn risk_configuration_client_specific_falls_back_to_pool_level() {
    let (svc, _) = make_svc();
    let pool_id = create_pool(&svc);

    // Pool-level config
    let body = json!({
        "UserPoolId": pool_id,
        "RiskExceptionConfiguration": {"BlockedIPRangeList": ["10.0.0.0/24"]}
    });
    let req = make_req("SetRiskConfiguration", &body.to_string());
    svc.set_risk_configuration(&req).unwrap();

    // Describe for specific client -> falls back to pool level
    let body = json!({"UserPoolId": pool_id, "ClientId": "abc"});
    let req = make_req("DescribeRiskConfiguration", &body.to_string());
    let resp = svc.describe_risk_configuration(&req).unwrap();
    let b = resp_json(&resp);
    assert_eq!(
        b["RiskConfiguration"]["RiskExceptionConfiguration"]["BlockedIPRangeList"][0],
        "10.0.0.0/24"
    );
}

#[test]
fn risk_configuration_default_when_absent() {
    let (svc, _) = make_svc();
    let pool_id = create_pool(&svc);
    let body = json!({"UserPoolId": pool_id, "ClientId": "c1"});
    let req = make_req("DescribeRiskConfiguration", &body.to_string());
    let resp = svc.describe_risk_configuration(&req).unwrap();
    let b = resp_json(&resp);
    assert_eq!(b["RiskConfiguration"]["UserPoolId"], pool_id);
    assert_eq!(b["RiskConfiguration"]["ClientId"], "c1");
}

// ── MFA (mfa.rs) coverage ─────────────────────────────────────────

fn issue_access_token(
    state: &crate::state::SharedCognitoState,
    pool_id: &str,
    username: &str,
    client_id: &str,
) -> String {
    let token = format!("access-{}", uuid::Uuid::new_v4());
    let mut st = state.write();
    let acct = st.get_or_create("123456789012");
    acct.access_tokens.insert(
        token.clone(),
        AccessTokenData {
            user_pool_id: pool_id.to_string(),
            username: username.to_string(),
            client_id: client_id.to_string(),
            issued_at: chrono::Utc::now(),
        },
    );
    token
}

#[test]
fn set_user_pool_mfa_config_full_shape() {
    let (svc, _) = make_svc();
    let pool_id = create_pool(&svc);
    let body = json!({
        "UserPoolId": pool_id,
        "MfaConfiguration": "ON",
        "SoftwareTokenMfaConfiguration": {"Enabled": true},
        "SmsMfaConfiguration": {
            "Enabled": true,
            "SmsConfiguration": {
                "SnsCallerArn": "arn:aws:iam::123:role/sms",
                "ExternalId": "eid",
                "SnsRegion": "us-east-1"
            }
        }
    });
    let req = make_req("SetUserPoolMfaConfig", &body.to_string());
    let resp = svc.set_user_pool_mfa_config(&req).unwrap();
    let b = resp_json(&resp);
    assert_eq!(b["MfaConfiguration"], "ON");
    assert_eq!(b["SoftwareTokenMfaConfiguration"]["Enabled"], true);
    assert_eq!(b["SmsMfaConfiguration"]["Enabled"], true);
    assert_eq!(
        b["SmsMfaConfiguration"]["SmsConfiguration"]["SnsCallerArn"],
        "arn:aws:iam::123:role/sms"
    );
}

#[test]
fn set_user_pool_mfa_config_unknown_pool() {
    let (svc, _) = make_svc();
    let body = json!({"UserPoolId": "us-east-1_no", "MfaConfiguration": "ON"});
    let req = make_req("SetUserPoolMfaConfig", &body.to_string());
    assert!(svc.set_user_pool_mfa_config(&req).is_err());
}

#[test]
fn get_user_pool_mfa_config_unknown_pool() {
    let (svc, _) = make_svc();
    let body = json!({"UserPoolId": "us-east-1_no"});
    let req = make_req("GetUserPoolMfaConfig", &body.to_string());
    assert!(svc.get_user_pool_mfa_config(&req).is_err());
}

#[test]
fn get_user_pool_mfa_config_returns_stored_shape() {
    let (svc, _) = make_svc();
    let pool_id = create_pool(&svc);
    let body = json!({
        "UserPoolId": pool_id,
        "MfaConfiguration": "OPTIONAL",
        "SoftwareTokenMfaConfiguration": {"Enabled": true},
        "SmsMfaConfiguration": {"Enabled": true}
    });
    let req = make_req("SetUserPoolMfaConfig", &body.to_string());
    svc.set_user_pool_mfa_config(&req).unwrap();

    let req = make_req(
        "GetUserPoolMfaConfig",
        &json!({"UserPoolId": pool_id}).to_string(),
    );
    let resp = svc.get_user_pool_mfa_config(&req).unwrap();
    let b = resp_json(&resp);
    assert_eq!(b["MfaConfiguration"], "OPTIONAL");
    assert_eq!(b["SoftwareTokenMfaConfiguration"]["Enabled"], true);
}

#[test]
fn admin_set_user_mfa_preference_unknown_user() {
    let (svc, _) = make_svc();
    let pool_id = create_pool(&svc);
    let body = json!({
        "UserPoolId": pool_id,
        "Username": "ghost",
        "SMSMfaSettings": {"Enabled": true, "PreferredMfa": true}
    });
    let req = make_req("AdminSetUserMFAPreference", &body.to_string());
    assert!(svc.admin_set_user_mfa_preference(&req).is_err());
}

#[test]
fn admin_set_user_mfa_preference_updates_prefs() {
    let (svc, _) = make_svc();
    let pool_id = create_pool(&svc);
    admin_create_user_helper(&svc, &pool_id, "alice");
    let body = json!({
        "UserPoolId": pool_id,
        "Username": "alice",
        "SMSMfaSettings": {"Enabled": true, "PreferredMfa": false},
        "SoftwareTokenMfaSettings": {"Enabled": true, "PreferredMfa": true},
    });
    let req = make_req("AdminSetUserMFAPreference", &body.to_string());
    svc.admin_set_user_mfa_preference(&req).unwrap();
}

#[test]
fn set_user_mfa_preference_invalid_token() {
    let (svc, _) = make_svc();
    let body = json!({
        "AccessToken": "bad",
        "SMSMfaSettings": {"Enabled": true}
    });
    let req = make_req("SetUserMFAPreference", &body.to_string());
    assert!(svc.set_user_mfa_preference(&req).is_err());
}

#[test]
fn set_user_mfa_preference_valid_token() {
    let (svc, state) = make_svc();
    let pool_id = create_pool(&svc);
    admin_create_user_helper(&svc, &pool_id, "alice");
    let token = issue_access_token(&state, &pool_id, "alice", "client-id");
    let body = json!({
        "AccessToken": token,
        "SMSMfaSettings": {"Enabled": true, "PreferredMfa": true},
        "SoftwareTokenMfaSettings": {"Enabled": true, "PreferredMfa": false},
    });
    let req = make_req("SetUserMFAPreference", &body.to_string());
    svc.set_user_mfa_preference(&req).unwrap();
}

#[test]
fn associate_software_token_requires_token_or_session() {
    let (svc, _) = make_svc();
    let req = make_req("AssociateSoftwareToken", "{}");
    assert!(svc.associate_software_token(&req).is_err());
}

#[test]
fn associate_software_token_invalid_token() {
    let (svc, _) = make_svc();
    let body = json!({"AccessToken": "nope"});
    let req = make_req("AssociateSoftwareToken", &body.to_string());
    assert!(svc.associate_software_token(&req).is_err());
}

#[test]
fn associate_software_token_returns_secret() {
    let (svc, state) = make_svc();
    let pool_id = create_pool(&svc);
    admin_create_user_helper(&svc, &pool_id, "bob");
    let token = issue_access_token(&state, &pool_id, "bob", "client-id");
    let body = json!({"AccessToken": token});
    let req = make_req("AssociateSoftwareToken", &body.to_string());
    let resp = svc.associate_software_token(&req).unwrap();
    let b = resp_json(&resp);
    assert!(!b["SecretCode"].as_str().unwrap().is_empty());
    assert!(!b["Session"].as_str().unwrap().is_empty());
}

#[test]
fn verify_software_token_invalid_code_format() {
    let (svc, _) = make_svc();
    let body = json!({"UserCode": "abcdef", "AccessToken": "t"});
    let req = make_req("VerifySoftwareToken", &body.to_string());
    assert!(svc.verify_software_token(&req).is_err());
}

#[test]
fn verify_software_token_requires_token_or_session() {
    let (svc, _) = make_svc();
    let body = json!({"UserCode": "123456"});
    let req = make_req("VerifySoftwareToken", &body.to_string());
    assert!(svc.verify_software_token(&req).is_err());
}

#[test]
fn verify_software_token_without_associated_secret() {
    let (svc, state) = make_svc();
    let pool_id = create_pool(&svc);
    admin_create_user_helper(&svc, &pool_id, "carl");
    let token = issue_access_token(&state, &pool_id, "carl", "client-id");
    let body = json!({"UserCode": "123456", "AccessToken": token});
    let req = make_req("VerifySoftwareToken", &body.to_string());
    assert!(svc.verify_software_token(&req).is_err());
}

#[test]
fn verify_software_token_after_associate_succeeds() {
    let (svc, state) = make_svc();
    let pool_id = create_pool(&svc);
    admin_create_user_helper(&svc, &pool_id, "dave");
    let token = issue_access_token(&state, &pool_id, "dave", "client-id");
    let body = json!({"AccessToken": token});
    let req = make_req("AssociateSoftwareToken", &body.to_string());
    svc.associate_software_token(&req).unwrap();
    let body = json!({"UserCode": "123456", "AccessToken": token});
    let req = make_req("VerifySoftwareToken", &body.to_string());
    let resp = svc.verify_software_token(&req).unwrap();
    let b = resp_json(&resp);
    assert_eq!(b["Status"], "SUCCESS");
}

#[test]
fn get_user_auth_factors_invalid_token() {
    let (svc, _) = make_svc();
    let body = json!({"AccessToken": "none"});
    let req = make_req("GetUserAuthFactors", &body.to_string());
    assert!(svc.get_user_auth_factors(&req).is_err());
}

#[test]
fn get_user_auth_factors_returns_factors() {
    let (svc, state) = make_svc();
    let pool_id = create_pool(&svc);
    admin_create_user_helper(&svc, &pool_id, "eve");
    let token = issue_access_token(&state, &pool_id, "eve", "client-id");
    let body = json!({"AccessToken": token});
    let req = make_req("GetUserAuthFactors", &body.to_string());
    let resp = svc.get_user_auth_factors(&req).unwrap();
    let b = resp_json(&resp);
    assert!(b["ConfiguredUserAuthFactors"].is_array());
}

// ── Legacy operations (legacy.rs) coverage ─────────────────────────

fn issue_access_token_for(
    state: &crate::state::SharedCognitoState,
    pool_id: &str,
    username: &str,
    client_id: &str,
) -> String {
    let token = format!("access-{}", uuid::Uuid::new_v4());
    let mut st = state.write();
    let acct = st.get_or_create("123456789012");
    acct.access_tokens.insert(
        token.clone(),
        AccessTokenData {
            user_pool_id: pool_id.to_string(),
            username: username.to_string(),
            client_id: client_id.to_string(),
            issued_at: chrono::Utc::now(),
        },
    );
    token
}

#[test]
fn set_user_settings_invalid_token() {
    let (svc, _) = make_svc();
    let body = json!({"AccessToken": "nope", "MFAOptions": []});
    let req = make_req("SetUserSettings", &body.to_string());
    assert!(svc.set_user_settings(&req).is_err());
}

#[test]
fn set_user_settings_valid_token() {
    let (svc, state) = make_svc();
    let pool_id = create_pool(&svc);
    admin_create_user_helper(&svc, &pool_id, "alice");
    let token = issue_access_token_for(&state, &pool_id, "alice", "c1");
    let body = json!({
        "AccessToken": token,
        "MFAOptions": [{"DeliveryMedium": "SMS", "AttributeName": "phone_number"}]
    });
    let req = make_req("SetUserSettings", &body.to_string());
    svc.set_user_settings(&req).unwrap();
}

#[test]
fn admin_link_and_disable_provider_for_user() {
    let (svc, _) = make_svc();
    let pool_id = create_pool(&svc);
    admin_create_user_helper(&svc, &pool_id, "eve");
    let body = json!({
        "UserPoolId": pool_id,
        "DestinationUser": {"ProviderName": "Cognito", "ProviderAttributeValue": "eve"},
        "SourceUser": {
            "ProviderName": "Google",
            "ProviderAttributeName": "Cognito_Subject",
            "ProviderAttributeValue": "google-sub-123"
        }
    });
    let req = make_req("AdminLinkProviderForUser", &body.to_string());
    svc.admin_link_provider_for_user(&req).unwrap();

    let body = json!({
        "UserPoolId": pool_id,
        "User": {"ProviderName": "Google", "ProviderAttributeValue": "google-sub-123"}
    });
    let req = make_req("AdminDisableProviderForUser", &body.to_string());
    svc.admin_disable_provider_for_user(&req).unwrap();
}

#[test]
fn admin_link_provider_pool_not_found() {
    let (svc, _) = make_svc();
    let body = json!({
        "UserPoolId": "us-east-1_no",
        "DestinationUser": {"ProviderName": "Cognito", "ProviderAttributeValue": "x"},
        "SourceUser": {"ProviderName": "Google", "ProviderAttributeValue": "v"}
    });
    let req = make_req("AdminLinkProviderForUser", &body.to_string());
    assert!(svc.admin_link_provider_for_user(&req).is_err());
}

#[test]
fn admin_link_provider_destination_not_found() {
    let (svc, _) = make_svc();
    let pool_id = create_pool(&svc);
    let body = json!({
        "UserPoolId": pool_id,
        "DestinationUser": {"ProviderName": "Cognito", "ProviderAttributeValue": "ghost"},
        "SourceUser": {"ProviderName": "Google", "ProviderAttributeValue": "v"}
    });
    let req = make_req("AdminLinkProviderForUser", &body.to_string());
    assert!(svc.admin_link_provider_for_user(&req).is_err());
}

#[test]
fn admin_disable_provider_user_not_found() {
    let (svc, _) = make_svc();
    let pool_id = create_pool(&svc);
    let body = json!({
        "UserPoolId": pool_id,
        "User": {"ProviderName": "Google", "ProviderAttributeValue": "missing"}
    });
    let req = make_req("AdminDisableProviderForUser", &body.to_string());
    assert!(svc.admin_disable_provider_for_user(&req).is_err());
}

#[test]
fn admin_list_user_auth_events_empty() {
    let (svc, _) = make_svc();
    let pool_id = create_pool(&svc);
    admin_create_user_helper(&svc, &pool_id, "alice");
    let body = json!({"UserPoolId": pool_id, "Username": "alice"});
    let req = make_req("AdminListUserAuthEvents", &body.to_string());
    let resp = svc.admin_list_user_auth_events(&req).unwrap();
    let b = resp_json(&resp);
    assert!(b["AuthEvents"].as_array().unwrap().is_empty());
}

#[test]
fn admin_list_user_auth_events_returns_stored() {
    let (svc, state) = make_svc();
    let pool_id = create_pool(&svc);
    admin_create_user_helper(&svc, &pool_id, "alice");
    {
        let mut st = state.write();
        let acct = st.get_or_create("123456789012");
        acct.auth_events.push(AuthEvent {
            event_id: "ev-1".to_string(),
            event_type: "SignIn".to_string(),
            username: "alice".to_string(),
            user_pool_id: pool_id.clone(),
            client_id: None,
            timestamp: chrono::Utc::now(),
            success: true,
            feedback_value: Some("Valid".to_string()),
        });
    }
    let body = json!({"UserPoolId": pool_id, "Username": "alice"});
    let req = make_req("AdminListUserAuthEvents", &body.to_string());
    let resp = svc.admin_list_user_auth_events(&req).unwrap();
    let b = resp_json(&resp);
    let events = b["AuthEvents"].as_array().unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0]["EventResponse"], "Pass");
    assert_eq!(events[0]["EventFeedback"]["FeedbackValue"], "Valid");
}

#[test]
fn admin_list_user_auth_events_user_not_found() {
    let (svc, _) = make_svc();
    let pool_id = create_pool(&svc);
    let body = json!({"UserPoolId": pool_id, "Username": "ghost"});
    let req = make_req("AdminListUserAuthEvents", &body.to_string());
    assert!(svc.admin_list_user_auth_events(&req).is_err());
}

#[test]
fn admin_update_auth_event_feedback_updates_event() {
    let (svc, state) = make_svc();
    let pool_id = create_pool(&svc);
    admin_create_user_helper(&svc, &pool_id, "alice");
    {
        let mut st = state.write();
        let acct = st.get_or_create("123456789012");
        acct.auth_events.push(AuthEvent {
            event_id: "ev-7".to_string(),
            event_type: "SignIn".to_string(),
            username: "alice".to_string(),
            user_pool_id: pool_id.clone(),
            client_id: None,
            timestamp: chrono::Utc::now(),
            success: false,
            feedback_value: None,
        });
    }
    let body = json!({
        "UserPoolId": pool_id,
        "Username": "alice",
        "EventId": "ev-7",
        "FeedbackValue": "Invalid"
    });
    let req = make_req("AdminUpdateAuthEventFeedback", &body.to_string());
    svc.admin_update_auth_event_feedback(&req).unwrap();
    let st = state.read();
    assert_eq!(
        st.default_ref().auth_events[0].feedback_value.as_deref(),
        Some("Invalid")
    );
}

#[test]
fn admin_update_auth_event_feedback_missing_event_id() {
    let (svc, _) = make_svc();
    let pool_id = create_pool(&svc);
    admin_create_user_helper(&svc, &pool_id, "alice2");
    let body = json!({
        "UserPoolId": pool_id,
        "Username": "alice2",
        "EventId": "missing",
        "FeedbackValue": "Invalid"
    });
    let req = make_req("AdminUpdateAuthEventFeedback", &body.to_string());
    assert!(svc.admin_update_auth_event_feedback(&req).is_err());
}

#[test]
fn update_auth_event_feedback_updates_event() {
    let (svc, state) = make_svc();
    let pool_id = create_pool(&svc);
    admin_create_user_helper(&svc, &pool_id, "alice");
    {
        let mut st = state.write();
        let acct = st.get_or_create("123456789012");
        acct.auth_events.push(AuthEvent {
            event_id: "ev-9".to_string(),
            event_type: "SignIn".to_string(),
            username: "alice".to_string(),
            user_pool_id: pool_id.clone(),
            client_id: None,
            timestamp: chrono::Utc::now(),
            success: true,
            feedback_value: None,
        });
    }
    let body = json!({
        "UserPoolId": pool_id,
        "Username": "alice",
        "EventId": "ev-9",
        "FeedbackToken": "ft",
        "FeedbackValue": "Valid"
    });
    let req = make_req("UpdateAuthEventFeedback", &body.to_string());
    svc.update_auth_event_feedback(&req).unwrap();
}

#[test]
fn update_auth_event_feedback_user_not_found() {
    let (svc, _) = make_svc();
    let pool_id = create_pool(&svc);
    let body = json!({
        "UserPoolId": pool_id,
        "Username": "ghost",
        "EventId": "ev",
        "FeedbackToken": "t",
        "FeedbackValue": "Valid"
    });
    let req = make_req("UpdateAuthEventFeedback", &body.to_string());
    assert!(svc.update_auth_event_feedback(&req).is_err());
}

// ── RespondToAuthChallenge coverage (auth.rs) ─────────────────────

#[test]
fn respond_to_auth_challenge_new_password_flow_completes_user() {
    let (svc, _) = make_svc();
    let pool_id = create_pool(&svc);
    let client_id = create_client(&svc, &pool_id);
    admin_create_user_helper(&svc, &pool_id, "alex");

    let body = json!({
        "ClientId": client_id,
        "AuthFlow": "USER_PASSWORD_AUTH",
        "AuthParameters": {"USERNAME": "alex", "PASSWORD": "TempPass1!"}
    });
    let req = make_req("InitiateAuth", &body.to_string());
    let resp = block_on(svc.initiate_auth(&req)).unwrap();
    let b = resp_json(&resp);
    assert_eq!(b["ChallengeName"], "NEW_PASSWORD_REQUIRED");
    let session = b["Session"].as_str().unwrap().to_string();

    let body = json!({
        "ClientId": client_id,
        "ChallengeName": "NEW_PASSWORD_REQUIRED",
        "Session": session,
        "ChallengeResponses": {
            "USERNAME": "alex",
            "NEW_PASSWORD": "Permanent1!"
        }
    });
    let req = make_req("RespondToAuthChallenge", &body.to_string());
    let resp = block_on(svc.respond_to_auth_challenge(&req)).unwrap();
    let b = resp_json(&resp);
    assert!(b["AuthenticationResult"]["AccessToken"].as_str().is_some());
}

#[test]
fn respond_to_auth_challenge_unsupported_challenge() {
    let (svc, _) = make_svc();
    let _pool_id = create_pool(&svc);
    let client_id = create_client(&svc, &_pool_id);
    let body = json!({
        "ClientId": client_id,
        "ChallengeName": "UNKNOWN_CHALLENGE",
        "Session": "some-session",
        "ChallengeResponses": {}
    });
    let req = make_req("RespondToAuthChallenge", &body.to_string());
    assert!(block_on(svc.respond_to_auth_challenge(&req)).is_err());
}

#[test]
fn respond_to_auth_challenge_invalid_session() {
    let (svc, _) = make_svc();
    let pool_id = create_pool(&svc);
    let client_id = create_client(&svc, &pool_id);
    let body = json!({
        "ClientId": client_id,
        "ChallengeName": "NEW_PASSWORD_REQUIRED",
        "Session": "bogus",
        "ChallengeResponses": {"USERNAME": "u", "NEW_PASSWORD": "Permanent1!"}
    });
    let req = make_req("RespondToAuthChallenge", &body.to_string());
    assert!(block_on(svc.respond_to_auth_challenge(&req)).is_err());
}

#[test]
fn respond_new_password_missing_new_password_errors() {
    let (svc, _) = make_svc();
    let pool_id = create_pool(&svc);
    let client_id = create_client(&svc, &pool_id);
    admin_create_user_helper(&svc, &pool_id, "joy");

    let body = json!({
        "ClientId": client_id,
        "AuthFlow": "USER_PASSWORD_AUTH",
        "AuthParameters": {"USERNAME": "joy", "PASSWORD": "TempPass1!"}
    });
    let req = make_req("InitiateAuth", &body.to_string());
    let resp = block_on(svc.initiate_auth(&req)).unwrap();
    let b = resp_json(&resp);
    let session = b["Session"].as_str().unwrap().to_string();

    let body = json!({
        "ClientId": client_id,
        "ChallengeName": "NEW_PASSWORD_REQUIRED",
        "Session": session,
        "ChallengeResponses": {"USERNAME": "joy"}
    });
    let req = make_req("RespondToAuthChallenge", &body.to_string());
    assert!(block_on(svc.respond_to_auth_challenge(&req)).is_err());
}

#[test]
fn admin_respond_to_auth_challenge_missing_challenge_responses() {
    let (svc, _) = make_svc();
    let pool_id = create_pool(&svc);
    let client_id = create_client(&svc, &pool_id);
    let body = json!({
        "UserPoolId": pool_id,
        "ClientId": client_id,
        "ChallengeName": "NEW_PASSWORD_REQUIRED",
        "Session": "s",
    });
    let req = make_req("AdminRespondToAuthChallenge", &body.to_string());
    assert!(block_on(svc.admin_respond_to_auth_challenge(&req)).is_err());
}

// ── Identity provider extra coverage ──────────────────────────────

fn make_idp_request(pool_id: &str, name: &str, ptype: &str) -> AwsRequest {
    let body = json!({
        "UserPoolId": pool_id,
        "ProviderName": name,
        "ProviderType": ptype,
        "ProviderDetails": {"client_id": "cid", "client_secret": "sec"},
        "AttributeMapping": {"email": "email"},
        "IdpIdentifiers": ["id-a", "id-b"]
    });
    make_req("CreateIdentityProvider", &body.to_string())
}

#[test]
fn describe_identity_provider_unknown_provider_errors() {
    let (svc, _) = make_svc();
    let pool_id = create_pool(&svc);
    let body = json!({"UserPoolId": pool_id, "ProviderName": "ghost"});
    let req = make_req("DescribeIdentityProvider", &body.to_string());
    assert!(svc.describe_identity_provider(&req).is_err());
}

#[test]
fn update_identity_provider_updates_and_not_found() {
    let (svc, _) = make_svc();
    let pool_id = create_pool(&svc);
    let req = make_idp_request(&pool_id, "Google", "Google");
    svc.create_identity_provider(&req).unwrap();

    let update_body = json!({
        "UserPoolId": pool_id,
        "ProviderName": "Google",
        "ProviderDetails": {"client_id": "new-cid"},
        "AttributeMapping": {"sub": "sub"},
        "IdpIdentifiers": ["new-id"]
    });
    let req = make_req("UpdateIdentityProvider", &update_body.to_string());
    svc.update_identity_provider(&req).unwrap();

    let miss_body = json!({"UserPoolId": pool_id, "ProviderName": "missing"});
    let req = make_req("UpdateIdentityProvider", &miss_body.to_string());
    assert!(svc.update_identity_provider(&req).is_err());
}

#[test]
fn delete_identity_provider_unknown_errors() {
    let (svc, _) = make_svc();
    let pool_id = create_pool(&svc);
    let body = json!({"UserPoolId": pool_id, "ProviderName": "ghost"});
    let req = make_req("DeleteIdentityProvider", &body.to_string());
    assert!(svc.delete_identity_provider(&req).is_err());
}

#[test]
fn list_identity_providers_paginates() {
    let (svc, _) = make_svc();
    let pool_id = create_pool(&svc);
    for (idx, ptype) in ["Google", "Facebook", "SAML"].iter().enumerate() {
        let req = make_idp_request(&pool_id, &format!("p{idx}"), ptype);
        svc.create_identity_provider(&req).unwrap();
    }
    let body = json!({"UserPoolId": pool_id, "MaxResults": 2});
    let req = make_req("ListIdentityProviders", &body.to_string());
    let resp = svc.list_identity_providers(&req).unwrap();
    let b = resp_json(&resp);
    assert_eq!(b["Providers"].as_array().unwrap().len(), 2);
    assert!(b["NextToken"].is_string());
}

#[test]
fn get_identity_provider_by_identifier_hits_and_misses() {
    let (svc, _) = make_svc();
    let pool_id = create_pool(&svc);
    let req = make_idp_request(&pool_id, "Google", "Google");
    svc.create_identity_provider(&req).unwrap();

    let body = json!({"UserPoolId": pool_id, "IdpIdentifier": "id-a"});
    let req = make_req("GetIdentityProviderByIdentifier", &body.to_string());
    let resp = svc.get_identity_provider_by_identifier(&req).unwrap();
    let b = resp_json(&resp);
    assert_eq!(b["IdentityProvider"]["ProviderName"], "Google");

    let body = json!({"UserPoolId": pool_id, "IdpIdentifier": "missing"});
    let req = make_req("GetIdentityProviderByIdentifier", &body.to_string());
    assert!(svc.get_identity_provider_by_identifier(&req).is_err());
}

// ── users.rs extra coverage ──────────────────────────────────────

fn issue_at_for_users(
    state: &crate::state::SharedCognitoState,
    pool_id: &str,
    username: &str,
    client_id: &str,
) -> String {
    let token = format!("access-{}", uuid::Uuid::new_v4());
    let mut st = state.write();
    let acct = st.get_or_create("123456789012");
    acct.access_tokens.insert(
        token.clone(),
        AccessTokenData {
            user_pool_id: pool_id.to_string(),
            username: username.to_string(),
            client_id: client_id.to_string(),
            issued_at: chrono::Utc::now(),
        },
    );
    token
}

#[test]
fn resend_confirmation_code_unknown_client_errors() {
    let (svc, _) = make_svc();
    let body = json!({"ClientId": "ghost", "Username": "u"});
    let req = make_req("ResendConfirmationCode", &body.to_string());
    assert!(svc.resend_confirmation_code(&req).is_err());
}

#[test]
fn resend_confirmation_code_unknown_user_errors() {
    let (svc, _) = make_svc();
    let pool_id = create_pool(&svc);
    let client_id = create_client(&svc, &pool_id);
    let body = json!({"ClientId": client_id, "Username": "ghost"});
    let req = make_req("ResendConfirmationCode", &body.to_string());
    assert!(svc.resend_confirmation_code(&req).is_err());
}

#[test]
fn resend_confirmation_code_returns_masked_email() {
    let (svc, _) = make_svc();
    let pool_id = create_pool(&svc);
    let client_id = create_client(&svc, &pool_id);
    admin_create_user_helper(&svc, &pool_id, "steve");
    let body = json!({"ClientId": client_id, "Username": "steve"});
    let req = make_req("ResendConfirmationCode", &body.to_string());
    let resp = svc.resend_confirmation_code(&req).unwrap();
    let b = resp_json(&resp);
    let dest = b["CodeDeliveryDetails"]["Destination"].as_str().unwrap();
    assert!(dest.contains("***"));
    assert!(dest.contains("@example.com"));
}

#[test]
fn get_user_attribute_verification_code_invalid_token() {
    let (svc, _) = make_svc();
    let body = json!({"AccessToken": "bad", "AttributeName": "email"});
    let req = make_req("GetUserAttributeVerificationCode", &body.to_string());
    assert!(svc.get_user_attribute_verification_code(&req).is_err());
}

#[test]
fn get_user_attribute_verification_code_email_path() {
    let (svc, state) = make_svc();
    let pool_id = create_pool(&svc);
    admin_create_user_helper(&svc, &pool_id, "kim");
    let token = issue_at_for_users(&state, &pool_id, "kim", "c");

    let body = json!({"AccessToken": token, "AttributeName": "email"});
    let req = make_req("GetUserAttributeVerificationCode", &body.to_string());
    let resp = svc.get_user_attribute_verification_code(&req).unwrap();
    let b = resp_json(&resp);
    assert_eq!(b["CodeDeliveryDetails"]["DeliveryMedium"], "EMAIL");
}

#[test]
fn get_user_attribute_verification_code_phone_path() {
    let (svc, state) = make_svc();
    let pool_id = create_pool(&svc);
    admin_create_user_helper(&svc, &pool_id, "lee");

    {
        let mut st = state.write();
        let acct = st.get_or_create("123456789012");
        let user = acct
            .users
            .get_mut(&pool_id)
            .unwrap()
            .get_mut("lee")
            .unwrap();
        user.attributes.push(crate::state::UserAttribute {
            name: "phone_number".to_string(),
            value: "+15551234567".to_string(),
        });
    }

    let token = issue_at_for_users(&state, &pool_id, "lee", "c");
    let body = json!({"AccessToken": token, "AttributeName": "phone_number"});
    let req = make_req("GetUserAttributeVerificationCode", &body.to_string());
    let resp = svc.get_user_attribute_verification_code(&req).unwrap();
    let b = resp_json(&resp);
    assert_eq!(b["CodeDeliveryDetails"]["DeliveryMedium"], "SMS");
    let dest = b["CodeDeliveryDetails"]["Destination"].as_str().unwrap();
    assert!(dest.contains("***"));
}

#[test]
fn verify_user_attribute_no_code_set() {
    let (svc, state) = make_svc();
    let pool_id = create_pool(&svc);
    admin_create_user_helper(&svc, &pool_id, "meg");
    let token = issue_at_for_users(&state, &pool_id, "meg", "c");

    let body = json!({"AccessToken": token, "AttributeName": "email", "Code": "123456"});
    let req = make_req("VerifyUserAttribute", &body.to_string());
    assert!(svc.verify_user_attribute(&req).is_err());
}

#[test]
fn verify_user_attribute_wrong_code() {
    let (svc, state) = make_svc();
    let pool_id = create_pool(&svc);
    admin_create_user_helper(&svc, &pool_id, "nin");
    let token = issue_at_for_users(&state, &pool_id, "nin", "c");

    let body = json!({"AccessToken": token, "AttributeName": "email"});
    let req = make_req("GetUserAttributeVerificationCode", &body.to_string());
    svc.get_user_attribute_verification_code(&req).unwrap();

    let body = json!({"AccessToken": token, "AttributeName": "email", "Code": "000000"});
    let req = make_req("VerifyUserAttribute", &body.to_string());
    assert!(svc.verify_user_attribute(&req).is_err());
}

// ── User pool custom attributes + client secrets ───────────────────

#[test]
fn add_custom_attributes_adds_with_prefix() {
    let (svc, _) = make_svc();
    let pool_id = create_pool(&svc);
    let body = json!({
        "UserPoolId": pool_id,
        "CustomAttributes": [
            {"Name": "tier", "AttributeDataType": "String"},
            {"Name": "custom:ready", "AttributeDataType": "Boolean"}
        ]
    });
    let req = make_req("AddCustomAttributes", &body.to_string());
    svc.add_custom_attributes(&req).unwrap();

    let req = make_req(
        "DescribeUserPool",
        &json!({"UserPoolId": pool_id}).to_string(),
    );
    let resp = svc.describe_user_pool(&req).unwrap();
    let b = resp_json(&resp);
    let schema = b["UserPool"]["SchemaAttributes"].as_array().unwrap();
    assert!(schema.iter().any(|s| s["Name"] == "custom:tier"));
    assert!(schema.iter().any(|s| s["Name"] == "custom:ready"));
}

#[test]
fn add_custom_attributes_missing_array_errors() {
    let (svc, _) = make_svc();
    let pool_id = create_pool(&svc);
    let body = json!({"UserPoolId": pool_id});
    let req = make_req("AddCustomAttributes", &body.to_string());
    assert!(svc.add_custom_attributes(&req).is_err());
}

#[test]
fn add_custom_attributes_unknown_pool_errors() {
    let (svc, _) = make_svc();
    let body = json!({"UserPoolId": "us-east-1_no", "CustomAttributes": []});
    let req = make_req("AddCustomAttributes", &body.to_string());
    assert!(svc.add_custom_attributes(&req).is_err());
}

#[test]
fn user_pool_client_secret_crud() {
    let (svc, _) = make_svc();
    let pool_id = create_pool(&svc);
    let client_id = create_client(&svc, &pool_id);

    let body = json!({
        "UserPoolId": pool_id,
        "ClientId": client_id,
        "ClientSecret": "super-secret"
    });
    let req = make_req("AddUserPoolClientSecret", &body.to_string());
    let resp = svc.add_user_pool_client_secret(&req).unwrap();
    let b = resp_json(&resp);
    let secret_id = b["ClientSecretDescriptor"]["ClientSecretId"]
        .as_str()
        .unwrap()
        .to_string();
    assert_eq!(
        b["ClientSecretDescriptor"]["ClientSecretValue"],
        "super-secret"
    );

    let body = json!({"UserPoolId": pool_id, "ClientId": client_id});
    let req = make_req("ListUserPoolClientSecrets", &body.to_string());
    let resp = svc.list_user_pool_client_secrets(&req).unwrap();
    let b = resp_json(&resp);
    assert_eq!(b["ClientSecrets"].as_array().unwrap().len(), 1);

    let body = json!({
        "UserPoolId": pool_id,
        "ClientId": client_id,
        "ClientSecretId": secret_id
    });
    let req = make_req("DeleteUserPoolClientSecret", &body.to_string());
    svc.delete_user_pool_client_secret(&req).unwrap();

    let body = json!({"UserPoolId": pool_id, "ClientId": client_id});
    let req = make_req("ListUserPoolClientSecrets", &body.to_string());
    let resp = svc.list_user_pool_client_secrets(&req).unwrap();
    let b = resp_json(&resp);
    assert_eq!(b["ClientSecrets"].as_array().unwrap().len(), 0);
}

#[test]
fn add_user_pool_client_secret_generates_secret_when_missing() {
    let (svc, _) = make_svc();
    let pool_id = create_pool(&svc);
    let client_id = create_client(&svc, &pool_id);
    let body = json!({"UserPoolId": pool_id, "ClientId": client_id});
    let req = make_req("AddUserPoolClientSecret", &body.to_string());
    let resp = svc.add_user_pool_client_secret(&req).unwrap();
    let b = resp_json(&resp);
    assert!(!b["ClientSecretDescriptor"]["ClientSecretValue"]
        .as_str()
        .unwrap()
        .is_empty());
}

#[test]
fn add_user_pool_client_secret_unknown_client_errors() {
    let (svc, _) = make_svc();
    let pool_id = create_pool(&svc);
    let body = json!({
        "UserPoolId": pool_id,
        "ClientId": "missing-client"
    });
    let req = make_req("AddUserPoolClientSecret", &body.to_string());
    assert!(svc.add_user_pool_client_secret(&req).is_err());
}

#[test]
fn delete_user_pool_client_secret_unknown_secret_errors() {
    let (svc, _) = make_svc();
    let pool_id = create_pool(&svc);
    let client_id = create_client(&svc, &pool_id);
    let body = json!({
        "UserPoolId": pool_id,
        "ClientId": client_id,
        "ClientSecretId": "nope"
    });
    let req = make_req("DeleteUserPoolClientSecret", &body.to_string());
    assert!(svc.delete_user_pool_client_secret(&req).is_err());
}

#[test]
fn list_user_pool_client_secrets_unknown_client_errors() {
    let (svc, _) = make_svc();
    let pool_id = create_pool(&svc);
    let body = json!({"UserPoolId": pool_id, "ClientId": "nope"});
    let req = make_req("ListUserPoolClientSecrets", &body.to_string());
    assert!(svc.list_user_pool_client_secrets(&req).is_err());
}

// ── misc.rs additional coverage (refresh tokens, revoke) ─────────

#[test]
fn revoke_token_unknown_client_errors() {
    let (svc, _) = make_svc();
    let body = json!({"Token": "t", "ClientId": "nope"});
    let req = make_req("RevokeToken", &body.to_string());
    assert!(svc.revoke_token(&req).is_err());
}

#[test]
fn revoke_token_removes_refresh_token() {
    let (svc, state) = make_svc();
    let pool_id = create_pool(&svc);
    let client_id = create_client(&svc, &pool_id);
    let token = "rt-abc".to_string();
    {
        let mut st = state.write();
        let acct = st.get_or_create("123456789012");
        acct.refresh_tokens.insert(
            token.clone(),
            crate::state::RefreshTokenData {
                user_pool_id: pool_id.clone(),
                username: "alice".to_string(),
                client_id: client_id.clone(),
                issued_at: chrono::Utc::now(),
            },
        );
    }
    let body = json!({"Token": token, "ClientId": client_id});
    let req = make_req("RevokeToken", &body.to_string());
    svc.revoke_token(&req).unwrap();
    assert!(!state
        .read()
        .default_ref()
        .refresh_tokens
        .contains_key(&token));
}

#[test]
fn get_tokens_from_refresh_token_unknown_client_errors() {
    let (svc, _) = make_svc();
    let body = json!({"RefreshToken": "rt", "ClientId": "nope"});
    let req = make_req("GetTokensFromRefreshToken", &body.to_string());
    assert!(svc.get_tokens_from_refresh_token(&req).is_err());
}

#[test]
fn get_tokens_from_refresh_token_invalid_refresh_token() {
    let (svc, _) = make_svc();
    let pool_id = create_pool(&svc);
    let client_id = create_client(&svc, &pool_id);
    let body = json!({"RefreshToken": "bogus", "ClientId": client_id});
    let req = make_req("GetTokensFromRefreshToken", &body.to_string());
    assert!(svc.get_tokens_from_refresh_token(&req).is_err());
}

#[test]
fn get_tokens_from_refresh_token_client_mismatch_errors() {
    let (svc, state) = make_svc();
    let pool_id = create_pool(&svc);
    let client_a = create_client(&svc, &pool_id);
    let client_b = create_client(&svc, &pool_id);
    admin_create_user_helper(&svc, &pool_id, "may");
    let rt = "rt-x".to_string();
    {
        let mut st = state.write();
        let acct = st.get_or_create("123456789012");
        acct.refresh_tokens.insert(
            rt.clone(),
            crate::state::RefreshTokenData {
                user_pool_id: pool_id.clone(),
                username: "may".to_string(),
                client_id: client_a,
                issued_at: chrono::Utc::now(),
            },
        );
    }
    let body = json!({"RefreshToken": rt, "ClientId": client_b});
    let req = make_req("GetTokensFromRefreshToken", &body.to_string());
    assert!(svc.get_tokens_from_refresh_token(&req).is_err());
}

#[test]
fn get_tokens_from_refresh_token_returns_new_tokens() {
    let (svc, state) = make_svc();
    let pool_id = create_pool(&svc);
    let client_id = create_client(&svc, &pool_id);
    admin_create_user_helper(&svc, &pool_id, "oli");
    let rt = "rt-ok".to_string();
    {
        let mut st = state.write();
        let acct = st.get_or_create("123456789012");
        acct.refresh_tokens.insert(
            rt.clone(),
            crate::state::RefreshTokenData {
                user_pool_id: pool_id.clone(),
                username: "oli".to_string(),
                client_id: client_id.clone(),
                issued_at: chrono::Utc::now(),
            },
        );
    }
    let body = json!({"RefreshToken": rt, "ClientId": client_id});
    let req = make_req("GetTokensFromRefreshToken", &body.to_string());
    let resp = svc.get_tokens_from_refresh_token(&req).unwrap();
    let b = resp_json(&resp);
    assert!(b["AuthenticationResult"]["AccessToken"].as_str().is_some());
    assert!(b["AuthenticationResult"]["IdToken"].as_str().is_some());
}

// ── Branding + WebAuthn extra coverage (branding.rs) ──────────────

fn issue_at(
    state: &crate::state::SharedCognitoState,
    pool_id: &str,
    username: &str,
    client_id: &str,
) -> String {
    let token = format!("access-{}", uuid::Uuid::new_v4());
    let mut st = state.write();
    let acct = st.get_or_create("123456789012");
    acct.access_tokens.insert(
        token.clone(),
        AccessTokenData {
            user_pool_id: pool_id.to_string(),
            username: username.to_string(),
            client_id: client_id.to_string(),
            issued_at: chrono::Utc::now(),
        },
    );
    token
}

#[test]
fn describe_managed_login_branding_unknown_errors() {
    let (svc, _) = make_svc();
    let body = json!({"ManagedLoginBrandingId": "nope"});
    let req = make_req("DescribeManagedLoginBranding", &body.to_string());
    assert!(svc.describe_managed_login_branding(&req).is_err());
}

#[test]
fn describe_managed_login_branding_by_client_unknown_client() {
    let (svc, _) = make_svc();
    let pool_id = create_pool(&svc);
    let body = json!({"UserPoolId": pool_id, "ClientId": "ghost"});
    let req = make_req("DescribeManagedLoginBrandingByClient", &body.to_string());
    assert!(svc.describe_managed_login_branding_by_client(&req).is_err());
}

#[test]
fn delete_managed_login_branding_unknown_errors() {
    let (svc, _) = make_svc();
    let body = json!({"UserPoolId": "us-east-1_x", "ManagedLoginBrandingId": "bid"});
    let req = make_req("DeleteManagedLoginBranding", &body.to_string());
    assert!(svc.delete_managed_login_branding(&req).is_err());
}

#[test]
fn update_managed_login_branding_unknown_errors() {
    let (svc, _) = make_svc();
    let body = json!({"UserPoolId": "us-east-1_x", "ManagedLoginBrandingId": "bid"});
    let req = make_req("UpdateManagedLoginBranding", &body.to_string());
    assert!(svc.update_managed_login_branding(&req).is_err());
}

#[test]
fn describe_terms_unknown_errors() {
    let (svc, _) = make_svc();
    let pool_id = create_pool(&svc);
    let body = json!({"UserPoolId": pool_id, "TermsId": "no-such"});
    let req = make_req("DescribeTerms", &body.to_string());
    assert!(svc.describe_terms(&req).is_err());
}

#[test]
fn delete_terms_unknown_errors() {
    let (svc, _) = make_svc();
    let pool_id = create_pool(&svc);
    let body = json!({"UserPoolId": pool_id, "TermsId": "no-such"});
    let req = make_req("DeleteTerms", &body.to_string());
    assert!(svc.delete_terms(&req).is_err());
}

#[test]
fn update_terms_unknown_errors() {
    let (svc, _) = make_svc();
    let pool_id = create_pool(&svc);
    let body = json!({"UserPoolId": pool_id, "TermsId": "no-such", "Links": []});
    let req = make_req("UpdateTerms", &body.to_string());
    assert!(svc.update_terms(&req).is_err());
}

#[test]
fn start_web_authn_registration_invalid_token() {
    let (svc, _) = make_svc();
    let body = json!({"AccessToken": "nope"});
    let req = make_req("StartWebAuthnRegistration", &body.to_string());
    assert!(svc.start_web_authn_registration(&req).is_err());
}

#[test]
fn start_web_authn_registration_returns_options() {
    let (svc, state) = make_svc();
    let pool_id = create_pool(&svc);
    admin_create_user_helper(&svc, &pool_id, "bill");
    let token = issue_at(&state, &pool_id, "bill", "c");
    let body = json!({"AccessToken": token});
    let req = make_req("StartWebAuthnRegistration", &body.to_string());
    let resp = svc.start_web_authn_registration(&req).unwrap();
    let b = resp_json(&resp);
    assert!(b["CredentialCreationOptions"]["challenge"].is_string());
}

#[test]
fn complete_web_authn_registration_missing_credential() {
    let (svc, state) = make_svc();
    let pool_id = create_pool(&svc);
    admin_create_user_helper(&svc, &pool_id, "ally");
    let token = issue_at(&state, &pool_id, "ally", "c");
    let body = json!({"AccessToken": token});
    let req = make_req("CompleteWebAuthnRegistration", &body.to_string());
    assert!(svc.complete_web_authn_registration(&req).is_err());
}

#[test]
fn complete_web_authn_registration_invalid_token() {
    let (svc, _) = make_svc();
    let body = json!({"AccessToken": "bad", "Credential": {"id": "abc"}});
    let req = make_req("CompleteWebAuthnRegistration", &body.to_string());
    assert!(svc.complete_web_authn_registration(&req).is_err());
}

#[test]
fn delete_web_authn_credential_invalid_token() {
    let (svc, _) = make_svc();
    let body = json!({"AccessToken": "bad", "CredentialId": "c"});
    let req = make_req("DeleteWebAuthnCredential", &body.to_string());
    assert!(svc.delete_web_authn_credential(&req).is_err());
}

#[test]
fn delete_web_authn_credential_no_credentials_registered() {
    let (svc, state) = make_svc();
    let pool_id = create_pool(&svc);
    admin_create_user_helper(&svc, &pool_id, "zed");
    let token = issue_at(&state, &pool_id, "zed", "c");
    let body = json!({"AccessToken": token, "CredentialId": "c"});
    let req = make_req("DeleteWebAuthnCredential", &body.to_string());
    assert!(svc.delete_web_authn_credential(&req).is_err());
}

#[test]
fn list_web_authn_credentials_invalid_token() {
    let (svc, _) = make_svc();
    let body = json!({"AccessToken": "bad"});
    let req = make_req("ListWebAuthnCredentials", &body.to_string());
    assert!(svc.list_web_authn_credentials(&req).is_err());
}

#[test]
fn list_web_authn_credentials_empty_when_none() {
    let (svc, state) = make_svc();
    let pool_id = create_pool(&svc);
    admin_create_user_helper(&svc, &pool_id, "fred");
    let token = issue_at(&state, &pool_id, "fred", "c");
    let body = json!({"AccessToken": token});
    let req = make_req("ListWebAuthnCredentials", &body.to_string());
    let resp = svc.list_web_authn_credentials(&req).unwrap();
    let b = resp_json(&resp);
    assert!(b["Credentials"].as_array().unwrap().is_empty());
}

// ── Y1: real RSA-2048 RS256 JWT signing ──

#[test]
fn create_user_pool_generates_rsa_keypair() {
    use rsa::pkcs8::DecodePrivateKey;

    let (svc, state) = make_svc();
    let pool_id = create_pool(&svc);

    let pem = {
        let mas = state.read();
        mas.default_ref()
            .user_pools
            .get(&pool_id)
            .unwrap()
            .signing_key_pem
            .clone()
            .expect("signing_key_pem should be set on CreateUserPool")
    };

    let private_key = rsa::RsaPrivateKey::from_pkcs8_pem(&pem)
        .expect("stored PEM should round-trip through RsaPrivateKey::from_pkcs8_pem");
    // Sanity-check 2048-bit modulus.
    use rsa::traits::PublicKeyParts;
    assert_eq!(private_key.n().bits(), 2048);
}

#[test]
fn initiate_auth_returns_real_rs256_signed_id_token() {
    use base64::Engine as _;
    use rsa::pkcs1v15::{Signature, VerifyingKey};
    use rsa::pkcs8::DecodePrivateKey;
    use rsa::sha2::Sha256;
    use rsa::signature::Verifier;

    let (svc, state) = make_svc();
    let pool_id = create_pool(&svc);
    let client_id = create_client(&svc, &pool_id);
    admin_create_user_helper(&svc, &pool_id, "rs256user");
    set_user_password(&svc, &pool_id, "rs256user", "SecurePass1!");

    let body = json!({
        "ClientId": client_id,
        "AuthFlow": "USER_PASSWORD_AUTH",
        "AuthParameters": {
            "USERNAME": "rs256user",
            "PASSWORD": "SecurePass1!",
        },
    });
    let req = make_req("InitiateAuth", &body.to_string());
    let resp = block_on(svc.initiate_auth(&req)).unwrap();
    let b = resp_json(&resp);
    let id_token = b["AuthenticationResult"]["IdToken"]
        .as_str()
        .expect("IdToken in response")
        .to_string();

    let parts: Vec<&str> = id_token.split('.').collect();
    assert_eq!(parts.len(), 3, "JWT must be three dot-separated segments");

    let b64url = base64::engine::general_purpose::URL_SAFE_NO_PAD;
    let header: Value = serde_json::from_slice(&b64url.decode(parts[0]).unwrap()).unwrap();
    assert_eq!(header["alg"], "RS256");
    assert!(
        header["kid"].as_str().is_some_and(|k| !k.is_empty()),
        "kid must be non-empty: {header:?}"
    );

    let pem = {
        let mas = state.read();
        mas.default_ref()
            .user_pools
            .get(&pool_id)
            .unwrap()
            .signing_key_pem
            .clone()
            .unwrap()
    };
    let private_key = rsa::RsaPrivateKey::from_pkcs8_pem(&pem).unwrap();
    let public_key = rsa::RsaPublicKey::from(&private_key);
    let verifying_key = VerifyingKey::<Sha256>::new(public_key);
    let signing_input = format!("{}.{}", parts[0], parts[1]);
    let sig_bytes = b64url.decode(parts[2]).unwrap();
    let signature = Signature::try_from(sig_bytes.as_slice()).unwrap();
    verifying_key
        .verify(signing_input.as_bytes(), &signature)
        .expect("ID token signature must verify against pool's public key");
}

#[test]
fn id_token_signature_changes_with_payload() {
    let (svc, _) = make_svc();
    let pool_id = create_pool(&svc);
    let client_id = create_client(&svc, &pool_id);
    admin_create_user_helper(&svc, &pool_id, "siguser1");
    set_user_password(&svc, &pool_id, "siguser1", "SecurePass1!");
    admin_create_user_helper(&svc, &pool_id, "siguser2");
    set_user_password(&svc, &pool_id, "siguser2", "SecurePass1!");

    let auth = |username: &str| -> String {
        let body = json!({
            "ClientId": client_id,
            "AuthFlow": "USER_PASSWORD_AUTH",
            "AuthParameters": {"USERNAME": username, "PASSWORD": "SecurePass1!"},
        });
        let req = make_req("InitiateAuth", &body.to_string());
        let resp = block_on(svc.initiate_auth(&req)).unwrap();
        resp_json(&resp)["AuthenticationResult"]["IdToken"]
            .as_str()
            .unwrap()
            .to_string()
    };

    let a = auth("siguser1");
    let b = auth("siguser2");
    let sig_a = a.split('.').nth(2).unwrap();
    let sig_b = b.split('.').nth(2).unwrap();
    assert_ne!(
        sig_a, sig_b,
        "two different InitiateAuth payloads should produce distinct signatures"
    );
}

#[test]
fn id_token_signature_invalid_when_tampered() {
    use base64::Engine as _;
    use rsa::pkcs1v15::{Signature, VerifyingKey};
    use rsa::pkcs8::DecodePrivateKey;
    use rsa::sha2::Sha256;
    use rsa::signature::Verifier;

    let (svc, state) = make_svc();
    let pool_id = create_pool(&svc);
    let client_id = create_client(&svc, &pool_id);
    admin_create_user_helper(&svc, &pool_id, "tamperuser");
    set_user_password(&svc, &pool_id, "tamperuser", "SecurePass1!");

    let body = json!({
        "ClientId": client_id,
        "AuthFlow": "USER_PASSWORD_AUTH",
        "AuthParameters": {"USERNAME": "tamperuser", "PASSWORD": "SecurePass1!"},
    });
    let req = make_req("InitiateAuth", &body.to_string());
    let resp = block_on(svc.initiate_auth(&req)).unwrap();
    let id_token = resp_json(&resp)["AuthenticationResult"]["IdToken"]
        .as_str()
        .unwrap()
        .to_string();
    let parts: Vec<&str> = id_token.split('.').collect();

    let b64url = base64::engine::general_purpose::URL_SAFE_NO_PAD;
    let mut payload: Value = serde_json::from_slice(&b64url.decode(parts[1]).unwrap()).unwrap();
    payload["sub"] = json!("attacker");
    let tampered_payload_b64 = b64url.encode(payload.to_string().as_bytes());
    let tampered_input = format!("{}.{}", parts[0], tampered_payload_b64);

    let pem = {
        let mas = state.read();
        mas.default_ref()
            .user_pools
            .get(&pool_id)
            .unwrap()
            .signing_key_pem
            .clone()
            .unwrap()
    };
    let private_key = rsa::RsaPrivateKey::from_pkcs8_pem(&pem).unwrap();
    let public_key = rsa::RsaPublicKey::from(&private_key);
    let verifying_key = VerifyingKey::<Sha256>::new(public_key);
    let sig_bytes = b64url.decode(parts[2]).unwrap();
    let signature = Signature::try_from(sig_bytes.as_slice()).unwrap();
    verifying_key
        .verify(tampered_input.as_bytes(), &signature)
        .expect_err("tampered payload must not verify with original signature");
}

#[test]
fn two_pools_have_distinct_keys() {
    let (svc, state) = make_svc();
    // create_pool reuses the same name; CreateUserPool happily creates
    // multiple pools with the same name (each gets a unique ID).
    let pool_a = create_pool(&svc);
    let pool_b = create_pool(&svc);
    assert_ne!(pool_a, pool_b);

    let (pem_a, pem_b) = {
        let mas = state.read();
        let pools = &mas.default_ref().user_pools;
        (
            pools.get(&pool_a).unwrap().signing_key_pem.clone().unwrap(),
            pools.get(&pool_b).unwrap().signing_key_pem.clone().unwrap(),
        )
    };
    assert_ne!(
        pem_a, pem_b,
        "two distinct pools must hold distinct RSA keypairs"
    );
}

// ── Y2: JWKS + OIDC discovery endpoints ──

#[test]
fn jwks_endpoint_returns_pool_public_key() {
    let (svc, state) = make_svc();
    let pool_id = create_pool(&svc);

    let jwks = block_on(pool_jwks_document(&state, &pool_id))
        .expect("pool_jwks_document should return a JWKS for an existing pool");
    let keys = jwks["keys"].as_array().expect("keys array must be present");
    assert_eq!(keys.len(), 1, "exactly one key per pool");
    let key = &keys[0];
    assert_eq!(key["kty"], "RSA");
    assert_eq!(key["alg"], "RS256");
    assert_eq!(key["use"], "sig");
    assert_eq!(key["e"], "AQAB");
    // kid is the deterministic SHA-256 prefix of the pool's public
    // SubjectPublicKeyInfo DER, not derived from `pool_id`.
    let kid = key["kid"].as_str().expect("kid must be a string");
    assert_eq!(kid.len(), 16, "kid must be 16 hex chars: {kid}");
    assert!(
        kid.chars().all(|c| c.is_ascii_hexdigit()),
        "kid must be lowercase hex: {kid}"
    );
}

#[test]
fn jwks_endpoint_n_is_valid_base64url() {
    use base64::Engine as _;
    use rsa::pkcs8::DecodePrivateKey;
    use rsa::traits::PublicKeyParts;

    let (svc, state) = make_svc();
    let pool_id = create_pool(&svc);

    let jwks = block_on(pool_jwks_document(&state, &pool_id)).unwrap();
    let n_b64 = jwks["keys"][0]["n"].as_str().unwrap();
    assert!(
        !n_b64.contains('='),
        "JWK `n` must be base64url-no-pad, got padding: {n_b64}"
    );
    let n_bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(n_b64)
        .expect("`n` must decode as base64url-no-pad");

    let pem = {
        let mas = state.read();
        mas.default_ref()
            .user_pools
            .get(&pool_id)
            .unwrap()
            .signing_key_pem
            .clone()
            .unwrap()
    };
    let private_key = rsa::RsaPrivateKey::from_pkcs8_pem(&pem).unwrap();
    let public_key = rsa::RsaPublicKey::from(&private_key);
    assert_eq!(
        n_bytes,
        public_key.n().to_bytes_be(),
        "JWK `n` must match the RSA modulus from the pool's stored private key"
    );
}

#[test]
fn jwks_endpoint_unknown_pool_returns_404() {
    let (_svc, state) = make_svc();
    // pool_jwks_document returns None for unknown pools; the HTTP layer
    // in main.rs maps that to a 404 response.
    let result = block_on(pool_jwks_document(&state, "us-east-1_DOESNOTEXIST"));
    assert!(
        result.is_none(),
        "unknown pool must return None so the HTTP wrapper can emit 404"
    );
}

#[test]
fn openid_configuration_returns_standard_doc() {
    let (svc, _state) = make_svc();
    let pool_id = create_pool(&svc);
    let doc = oidc_discovery_document(
        &pool_id,
        "us-east-1",
        "http://localhost:4569",
        Some("myapp"),
    );

    assert_eq!(
        doc["issuer"],
        format!("https://cognito-idp.us-east-1.amazonaws.com/{pool_id}")
    );
    assert_eq!(
        doc["authorization_endpoint"],
        "http://localhost:4569/oauth2/authorize"
    );
    assert_eq!(doc["token_endpoint"], "http://localhost:4569/oauth2/token");
    assert_eq!(
        doc["userinfo_endpoint"],
        "http://localhost:4569/oauth2/userInfo"
    );
    assert_eq!(
        doc["id_token_signing_alg_values_supported"],
        json!(["RS256"])
    );
    assert_eq!(doc["subject_types_supported"], json!(["public"]));
    let response_types = doc["response_types_supported"].as_array().unwrap();
    assert!(response_types.contains(&json!("code")));
    assert!(response_types.contains(&json!("token")));
    let scopes = doc["scopes_supported"].as_array().unwrap();
    for required in ["openid", "email", "phone", "profile"] {
        assert!(
            scopes.contains(&json!(required)),
            "scopes_supported must include {required}: {scopes:?}"
        );
    }
    let auth_methods = doc["token_endpoint_auth_methods_supported"]
        .as_array()
        .unwrap();
    assert!(auth_methods.contains(&json!("client_secret_basic")));
    assert!(auth_methods.contains(&json!("client_secret_post")));
}

#[test]
fn openid_configuration_jwks_uri_matches_jwks_endpoint() {
    let (svc, _state) = make_svc();
    let pool_id = create_pool(&svc);
    let doc = oidc_discovery_document(&pool_id, "us-east-1", "http://localhost:4569", None);
    assert_eq!(
        doc["jwks_uri"],
        format!("http://localhost:4569/{pool_id}/.well-known/jwks.json"),
        "discovery doc's jwks_uri must point at the same pool's JWKS endpoint"
    );
}

#[test]
fn openid_configuration_omits_oauth_endpoints_without_domain() {
    let (svc, _state) = make_svc();
    let pool_id = create_pool(&svc);
    // Real Cognito only advertises OAuth endpoints once the pool has a
    // hosted-UI domain — until then the URLs literally don't resolve.
    let doc = oidc_discovery_document(&pool_id, "us-east-1", "http://localhost:4569", None);
    assert!(
        doc.get("authorization_endpoint").is_none(),
        "authorization_endpoint must be omitted without a domain: {doc}"
    );
    assert!(
        doc.get("token_endpoint").is_none(),
        "token_endpoint must be omitted without a domain"
    );
    assert!(
        doc.get("userinfo_endpoint").is_none(),
        "userinfo_endpoint must be omitted without a domain"
    );
    assert!(
        doc.get("revocation_endpoint").is_none(),
        "revocation_endpoint must be omitted without a domain"
    );
    // jwks_uri and issuer are still required and unconditional.
    assert!(doc["jwks_uri"].is_string());
    assert!(doc["issuer"].is_string());
}

#[test]
fn id_token_verifies_against_jwks_public_key() {
    use base64::Engine as _;
    use rsa::pkcs1v15::{Signature, VerifyingKey};
    use rsa::sha2::Sha256;
    use rsa::signature::Verifier;
    use rsa::{BigUint, RsaPublicKey};

    let (svc, state) = make_svc();
    let pool_id = create_pool(&svc);
    let client_id = create_client(&svc, &pool_id);
    admin_create_user_helper(&svc, &pool_id, "jwksuser");
    set_user_password(&svc, &pool_id, "jwksuser", "SecurePass1!");

    let body = json!({
        "ClientId": client_id,
        "AuthFlow": "USER_PASSWORD_AUTH",
        "AuthParameters": {"USERNAME": "jwksuser", "PASSWORD": "SecurePass1!"},
    });
    let req = make_req("InitiateAuth", &body.to_string());
    let resp = block_on(svc.initiate_auth(&req)).unwrap();
    let id_token = resp_json(&resp)["AuthenticationResult"]["IdToken"]
        .as_str()
        .unwrap()
        .to_string();
    let parts: Vec<&str> = id_token.split('.').collect();

    let b64url = base64::engine::general_purpose::URL_SAFE_NO_PAD;
    let header: Value = serde_json::from_slice(&b64url.decode(parts[0]).unwrap()).unwrap();
    let token_kid = header["kid"].as_str().unwrap();

    // Pull the public key strictly from the JWKS document (matches what
    // an SDK would do via the discovery endpoint).
    let jwks = block_on(pool_jwks_document(&state, &pool_id)).unwrap();
    let key = jwks["keys"]
        .as_array()
        .unwrap()
        .iter()
        .find(|k| k["kid"].as_str() == Some(token_kid))
        .expect("JWKS must contain a key with the JWT's kid");

    let n_bytes = b64url.decode(key["n"].as_str().unwrap()).unwrap();
    let e_bytes = b64url.decode(key["e"].as_str().unwrap()).unwrap();
    let n = BigUint::from_bytes_be(&n_bytes);
    let e = BigUint::from_bytes_be(&e_bytes);
    let public_key = RsaPublicKey::new(n, e).unwrap();
    let verifying_key = VerifyingKey::<Sha256>::new(public_key);

    let signing_input = format!("{}.{}", parts[0], parts[1]);
    let sig_bytes = b64url.decode(parts[2]).unwrap();
    let signature = Signature::try_from(sig_bytes.as_slice()).unwrap();
    verifying_key
        .verify(signing_input.as_bytes(), &signature)
        .expect("ID token must verify against the JWKS-published public key");
}

#[test]
fn pretoken_overrides_merge_into_id_and_access_tokens() {
    use crate::service::generate_tokens_with_overrides;
    use base64::Engine;

    let b64url = base64::engine::general_purpose::URL_SAFE_NO_PAD;
    let overrides = serde_json::json!({
        "claimsAndScopeOverrideDetails": {
            "idTokenGeneration": {
                "claimsToAddOrOverride": {"custom:tier": "gold", "email": "x@y.z"},
                "claimsToSuppress": ["jti"],
            },
            "accessTokenGeneration": {
                "claimsToAddOrOverride": {"custom:tier": "gold"},
                "scopesToAdd": ["openid", "email"],
            },
            "groupOverrideDetails": {
                "groupsToOverride": ["admins", "beta"],
            },
        }
    });

    let tokens = generate_tokens_with_overrides(
        "us-east-1_abc",
        "client1",
        "sub-1",
        "alice",
        "us-east-1",
        None,
        None,
        None,
        Some(&overrides),
    );

    let parts: Vec<&str> = tokens.id_token.split('.').collect();
    let id_payload: serde_json::Value =
        serde_json::from_slice(&b64url.decode(parts[1]).unwrap()).unwrap();
    assert_eq!(id_payload["custom:tier"], "gold");
    assert_eq!(id_payload["email"], "x@y.z");
    assert!(id_payload.get("jti").is_none(), "jti should be suppressed");
    assert_eq!(
        id_payload["cognito:groups"],
        serde_json::json!(["admins", "beta"])
    );

    let parts: Vec<&str> = tokens.access_token.split('.').collect();
    let access_payload: serde_json::Value =
        serde_json::from_slice(&b64url.decode(parts[1]).unwrap()).unwrap();
    assert_eq!(access_payload["custom:tier"], "gold");
    let scope = access_payload["scope"].as_str().unwrap();
    assert!(scope.contains("openid"));
    assert!(scope.contains("email"));
    assert_eq!(
        access_payload["cognito:groups"],
        serde_json::json!(["admins", "beta"])
    );
}

#[test]
fn pretoken_v1_claims_override_details_shape() {
    use crate::service::generate_tokens_with_overrides;
    use base64::Engine;

    let b64url = base64::engine::general_purpose::URL_SAFE_NO_PAD;
    let overrides = serde_json::json!({
        "claimsOverrideDetails": {
            "claimsToAddOrOverride": {"custom:plan": "pro"},
            "claimsToSuppress": ["aud"],
            "groupOverrideDetails": {"groupsToOverride": ["g1"]},
        }
    });

    let tokens = generate_tokens_with_overrides(
        "us-east-1_abc",
        "client1",
        "sub-1",
        "alice",
        "us-east-1",
        None,
        None,
        None,
        Some(&overrides),
    );

    let parts: Vec<&str> = tokens.id_token.split('.').collect();
    let id_payload: serde_json::Value =
        serde_json::from_slice(&b64url.decode(parts[1]).unwrap()).unwrap();
    assert_eq!(id_payload["custom:plan"], "pro");
    assert!(id_payload.get("aud").is_none());
    assert_eq!(id_payload["cognito:groups"], serde_json::json!(["g1"]));
}

#[test]
fn get_signing_certificate_returns_real_x509_matching_pool_jwt_key() {
    use base64::Engine;
    let (svc, pool_id) = setup_svc_with_pool();

    let req = make_req(
        "GetSigningCertificate",
        &format!(r#"{{"UserPoolId":"{pool_id}"}}"#),
    );
    let resp = svc.get_signing_certificate(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let cert_b64 = body["Certificate"].as_str().unwrap();
    let der = base64::engine::general_purpose::STANDARD
        .decode(cert_b64)
        .unwrap();

    assert!(!der.is_empty(), "cert DER should be non-empty");
    assert_eq!(&der[0..2], &[0x30, 0x82], "DER must start with SEQUENCE");
    assert!(
        der.windows(8).any(|w| w == b"fakeclou"),
        "subject should mention fakecloud"
    );
}
