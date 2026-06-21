use super::*;
use parking_lot::RwLock;
use serde_json::json;
use std::collections::HashMap;
use std::sync::Arc;

fn make_service() -> KmsService {
    let state: SharedKmsState = Arc::new(RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new(
            "123456789012",
            "us-east-1",
            "http://localhost:4566",
        ),
    ));
    KmsService::new(state)
}

fn make_request(action: &str, body: Value) -> AwsRequest {
    AwsRequest {
        service: "kms".to_string(),
        action: action.to_string(),
        region: "us-east-1".to_string(),
        account_id: "123456789012".to_string(),
        request_id: "test-id".to_string(),
        headers: http::HeaderMap::new(),
        query_params: HashMap::new(),
        body: serde_json::to_vec(&body).unwrap().into(),
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

fn create_key(svc: &KmsService) -> String {
    let req = make_request("CreateKey", json!({}));
    let resp = svc.create_key(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    body["KeyMetadata"]["KeyId"].as_str().unwrap().to_string()
}

/// Helper: run GetParametersForImport, RSA-OAEP-wrap `material` under
/// the returned public key, and call ImportKeyMaterial. Used by every
/// import-flow test so they exercise the real OAEP unwrap path
/// instead of relying on the previous fake-bytes shim.
fn import_external_material(svc: &KmsService, key_id: &str, material: &[u8]) {
    use base64::Engine;
    use rsa::pkcs8::DecodePublicKey;

    let resp = svc
        .get_parameters_for_import(&make_request(
            "GetParametersForImport",
            json!({
                "KeyId": key_id,
                "WrappingAlgorithm": "RSAES_OAEP_SHA_256",
                "WrappingKeySpec": "RSA_2048",
            }),
        ))
        .unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let token = body["ImportToken"].as_str().unwrap().to_string();
    let pub_der = base64::engine::general_purpose::STANDARD
        .decode(body["PublicKey"].as_str().unwrap())
        .unwrap();
    let public = rsa::RsaPublicKey::from_public_key_der(&pub_der).unwrap();
    let mut rng = rand::thread_rng();
    let wrapped = public
        .encrypt(&mut rng, rsa::Oaep::new::<rsa::sha2::Sha256>(), material)
        .unwrap();
    let wrapped_b64 = base64::engine::general_purpose::STANDARD.encode(&wrapped);

    svc.import_key_material(&make_request(
        "ImportKeyMaterial",
        json!({
            "KeyId": key_id,
            "ImportToken": token,
            "EncryptedKeyMaterial": wrapped_b64,
            "WrappingAlgorithm": "RSAES_OAEP_SHA_256",
            "ExpirationModel": "KEY_MATERIAL_DOES_NOT_EXPIRE"
        }),
    ))
    .unwrap();
}

#[test]
fn list_keys_pagination_no_duplicates() {
    let svc = make_service();
    let mut all_key_ids: Vec<String> = Vec::new();
    for _ in 0..5 {
        all_key_ids.push(create_key(&svc));
    }

    let mut collected_ids: Vec<String> = Vec::new();
    let mut marker: Option<String> = None;

    loop {
        let mut body = json!({ "Limit": 2 });
        if let Some(ref m) = marker {
            body["Marker"] = json!(m);
        }
        let req = make_request("ListKeys", body);
        let resp = svc.list_keys(&req).unwrap();
        let resp_body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();

        for key in resp_body["Keys"].as_array().unwrap() {
            collected_ids.push(key["KeyId"].as_str().unwrap().to_string());
        }

        if resp_body["Truncated"].as_bool().unwrap_or(false) {
            marker = resp_body["NextMarker"].as_str().map(|s| s.to_string());
        } else {
            break;
        }
    }

    // Verify no duplicates
    let mut deduped = collected_ids.clone();
    deduped.sort();
    deduped.dedup();
    assert_eq!(
        collected_ids.len(),
        deduped.len(),
        "pagination produced duplicate keys"
    );

    // Verify all keys returned
    for kid in &all_key_ids {
        assert!(
            collected_ids.contains(kid),
            "key {kid} missing from paginated results"
        );
    }
}

#[test]
fn list_retirable_grants_pagination() {
    let svc = make_service();
    let key_id = create_key(&svc);
    let retiring = "arn:aws:iam::123456789012:user/retiring-user";

    // Create 5 grants with the same retiring principal
    for i in 0..5 {
        let req = make_request(
            "CreateGrant",
            json!({
                "KeyId": key_id,
                "GranteePrincipal": format!("arn:aws:iam::123456789012:user/grantee-{i}"),
                "RetiringPrincipal": retiring,
                "Operations": ["Encrypt"]
            }),
        );
        svc.create_grant(&req).unwrap();
    }

    let mut collected_ids: Vec<String> = Vec::new();
    let mut marker: Option<String> = None;

    loop {
        let mut body = json!({
            "RetiringPrincipal": retiring,
            "Limit": 2
        });
        if let Some(ref m) = marker {
            body["Marker"] = json!(m);
        }
        let req = make_request("ListRetirableGrants", body);
        let resp = svc.list_retirable_grants(&req).unwrap();
        let resp_body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();

        for grant in resp_body["Grants"].as_array().unwrap() {
            collected_ids.push(grant["GrantId"].as_str().unwrap().to_string());
        }

        if resp_body["Truncated"].as_bool().unwrap_or(false) {
            marker = resp_body["NextMarker"].as_str().map(|s| s.to_string());
        } else {
            break;
        }
    }

    // Verify no duplicates
    let mut deduped = collected_ids.clone();
    deduped.sort();
    deduped.dedup();
    assert_eq!(
        collected_ids.len(),
        deduped.len(),
        "pagination produced duplicate grants"
    );

    // All 5 grants returned
    assert_eq!(collected_ids.len(), 5, "expected 5 grants total");
}

fn create_key_with_opts(svc: &KmsService, body: Value) -> String {
    let req = make_request("CreateKey", body);
    let resp = svc.create_key(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    body["KeyMetadata"]["KeyId"].as_str().unwrap().to_string()
}

#[test]
fn generate_data_key_pair_returns_all_fields() {
    let svc = make_service();
    let key_id = create_key(&svc);

    let req = make_request(
        "GenerateDataKeyPair",
        json!({ "KeyId": key_id, "KeyPairSpec": "RSA_2048" }),
    );
    let resp = svc.generate_data_key_pair(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();

    assert!(body["PublicKey"].as_str().is_some());
    assert!(body["PrivateKeyPlaintext"].as_str().is_some());
    assert!(body["PrivateKeyCiphertextBlob"].as_str().is_some());
    assert_eq!(body["KeyPairSpec"].as_str().unwrap(), "RSA_2048");
    assert!(body["KeyId"].as_str().unwrap().contains(":key/"));
}

#[test]
fn generate_data_key_pair_disabled_key_fails() {
    let svc = make_service();
    let key_id = create_key(&svc);

    let disable_req = make_request("DisableKey", json!({ "KeyId": key_id }));
    svc.disable_key(&disable_req).unwrap();

    let req = make_request(
        "GenerateDataKeyPair",
        json!({ "KeyId": key_id, "KeyPairSpec": "RSA_2048" }),
    );
    assert!(svc.generate_data_key_pair(&req).is_err());
}

#[test]
fn generate_data_key_pair_without_plaintext_omits_private_plaintext() {
    let svc = make_service();
    let key_id = create_key(&svc);

    let req = make_request(
        "GenerateDataKeyPairWithoutPlaintext",
        json!({ "KeyId": key_id, "KeyPairSpec": "ECC_NIST_P256" }),
    );
    let resp = svc.generate_data_key_pair_without_plaintext(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();

    assert!(body["PublicKey"].as_str().is_some());
    assert!(body["PrivateKeyCiphertextBlob"].as_str().is_some());
    assert!(body.get("PrivateKeyPlaintext").is_none());
    assert_eq!(body["KeyPairSpec"].as_str().unwrap(), "ECC_NIST_P256");
}

#[test]
fn generate_data_key_pair_rsa_returns_parseable_pkcs8_and_spki() {
    use base64::Engine;
    use rsa::pkcs8::{DecodePrivateKey, DecodePublicKey};
    use rsa::{RsaPrivateKey, RsaPublicKey};

    let svc = make_service();
    let key_id = create_key(&svc);

    let req = make_request(
        "GenerateDataKeyPair",
        json!({ "KeyId": key_id, "KeyPairSpec": "RSA_2048" }),
    );
    let resp = svc.generate_data_key_pair(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();

    let priv_der = base64::engine::general_purpose::STANDARD
        .decode(body["PrivateKeyPlaintext"].as_str().unwrap())
        .unwrap();
    let pub_der = base64::engine::general_purpose::STANDARD
        .decode(body["PublicKey"].as_str().unwrap())
        .unwrap();

    // Both halves must parse as standards-compliant DER. The fake-bytes
    // path returned random noise that any real client would reject; the
    // real path produces a key any standard tool can load.
    let private = RsaPrivateKey::from_pkcs8_der(&priv_der)
        .expect("PrivateKeyPlaintext must be parseable PKCS#8 DER");
    let public = RsaPublicKey::from_public_key_der(&pub_der)
        .expect("PublicKey must be parseable SubjectPublicKeyInfo DER");
    // The public half must match the private half; otherwise verify
    // would reject signatures the caller produced with the private key.
    assert_eq!(RsaPublicKey::from(&private), public);
}

#[test]
fn generate_data_key_pair_ecc_returns_parseable_pkcs8_and_spki() {
    use base64::Engine;
    use p256::pkcs8::{DecodePrivateKey, DecodePublicKey};
    use p256::{PublicKey, SecretKey};

    let svc = make_service();
    let key_id = create_key(&svc);

    let req = make_request(
        "GenerateDataKeyPair",
        json!({ "KeyId": key_id, "KeyPairSpec": "ECC_NIST_P256" }),
    );
    let resp = svc.generate_data_key_pair(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();

    let priv_der = base64::engine::general_purpose::STANDARD
        .decode(body["PrivateKeyPlaintext"].as_str().unwrap())
        .unwrap();
    let pub_der = base64::engine::general_purpose::STANDARD
        .decode(body["PublicKey"].as_str().unwrap())
        .unwrap();

    let secret = SecretKey::from_pkcs8_der(&priv_der)
        .expect("PrivateKeyPlaintext must be parseable PKCS#8 DER for P-256");
    let public = PublicKey::from_public_key_der(&pub_der)
        .expect("PublicKey must be parseable SubjectPublicKeyInfo DER for P-256");
    assert_eq!(secret.public_key(), public);
}

#[test]
fn derive_shared_secret_success() {
    let svc = make_service();
    let key_id = create_key_with_opts(
        &svc,
        json!({
            "KeyUsage": "KEY_AGREEMENT",
            "KeySpec": "ECC_NIST_P256"
        }),
    );

    let fake_pub = base64::engine::general_purpose::STANDARD.encode(b"fake-public-key");
    let req = make_request(
        "DeriveSharedSecret",
        json!({
            "KeyId": key_id,
            "KeyAgreementAlgorithm": "ECDH",
            "PublicKey": fake_pub
        }),
    );
    let resp = svc.derive_shared_secret(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();

    assert!(body["SharedSecret"].as_str().is_some());
    assert!(body["KeyId"].as_str().unwrap().contains(":key/"));
    assert_eq!(body["KeyAgreementAlgorithm"].as_str().unwrap(), "ECDH");
}

#[test]
fn derive_shared_secret_wrong_usage_fails() {
    let svc = make_service();
    let key_id = create_key(&svc); // Default is ENCRYPT_DECRYPT

    let fake_pub = base64::engine::general_purpose::STANDARD.encode(b"fake-public-key");
    let req = make_request(
        "DeriveSharedSecret",
        json!({
            "KeyId": key_id,
            "KeyAgreementAlgorithm": "ECDH",
            "PublicKey": fake_pub
        }),
    );
    assert!(svc.derive_shared_secret(&req).is_err());
}

#[test]
fn get_parameters_for_import_success() {
    let svc = make_service();
    let key_id = create_key_with_opts(&svc, json!({ "Origin": "EXTERNAL" }));

    let req = make_request(
        "GetParametersForImport",
        json!({ "KeyId": key_id, "WrappingAlgorithm": "RSAES_OAEP_SHA_256", "WrappingKeySpec": "RSA_2048" }),
    );
    let resp = svc.get_parameters_for_import(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();

    assert!(body["ImportToken"].as_str().is_some());
    assert!(body["PublicKey"].as_str().is_some());
    assert!(body["ParametersValidTo"].as_f64().is_some());
    assert!(body["KeyId"].as_str().unwrap().contains(":key/"));
}

#[test]
fn get_parameters_for_import_non_external_fails() {
    let svc = make_service();
    let key_id = create_key(&svc); // Default origin is AWS_KMS

    let req = make_request("GetParametersForImport", json!({ "KeyId": key_id }));
    assert!(svc.get_parameters_for_import(&req).is_err());
}

#[test]
fn import_key_material_lifecycle() {
    let svc = make_service();
    let key_id = create_key_with_opts(&svc, json!({ "Origin": "EXTERNAL" }));

    // Real RSA-OAEP wrap+unwrap flow.
    import_external_material(&svc, &key_id, b"my-secret-aes-key-material!12345");

    // Key should be enabled
    {
        let _accts = svc.state.read();
        let state = _accts.default_ref();
        let key = state.keys.get(&key_id).unwrap();
        assert!(key.imported_key_material);
        assert!(key.enabled);
    }

    // Delete imported material
    let req = make_request("DeleteImportedKeyMaterial", json!({ "KeyId": key_id }));
    svc.delete_imported_key_material(&req).unwrap();

    // Key should be disabled and pending import
    {
        let _accts = svc.state.read();
        let state = _accts.default_ref();
        let key = state.keys.get(&key_id).unwrap();
        assert!(!key.imported_key_material);
        assert!(!key.enabled);
        assert_eq!(key.key_state, "PendingImport");
    }
}

#[test]
fn import_key_material_non_external_fails() {
    let svc = make_service();
    let key_id = create_key(&svc);

    // Non-EXTERNAL key rejects ImportKeyMaterial regardless of token
    // validity, so the fake token + material here is fine — we never
    // reach the unwrap step.
    let fake_token = base64::engine::general_purpose::STANDARD.encode(b"token");
    let fake_material = base64::engine::general_purpose::STANDARD.encode(b"material");

    let req = make_request(
        "ImportKeyMaterial",
        json!({
            "KeyId": key_id,
            "ImportToken": fake_token,
            "EncryptedKeyMaterial": fake_material
        }),
    );
    assert!(svc.import_key_material(&req).is_err());
}

#[test]
fn delete_imported_key_material_non_external_fails() {
    let svc = make_service();
    let key_id = create_key(&svc);

    let req = make_request("DeleteImportedKeyMaterial", json!({ "KeyId": key_id }));
    assert!(svc.delete_imported_key_material(&req).is_err());
}

#[test]
fn update_primary_region_success() {
    let svc = make_service();
    let key_id = create_key_with_opts(&svc, json!({ "MultiRegion": true }));

    let req = make_request(
        "UpdatePrimaryRegion",
        json!({ "KeyId": key_id, "PrimaryRegion": "eu-west-1" }),
    );
    svc.update_primary_region(&req).unwrap();

    let _accts = svc.state.read();
    let state = _accts.default_ref();
    let key = state.keys.get(&key_id).unwrap();
    assert_eq!(key.primary_region.as_deref(), Some("eu-west-1"));
    assert!(key.arn.contains("eu-west-1"));
}

#[test]
fn update_primary_region_non_multi_region_fails() {
    let svc = make_service();
    let key_id = create_key(&svc); // Not multi-region

    let req = make_request(
        "UpdatePrimaryRegion",
        json!({ "KeyId": key_id, "PrimaryRegion": "eu-west-1" }),
    );
    assert!(svc.update_primary_region(&req).is_err());
}

#[test]
fn custom_key_store_lifecycle() {
    let svc = make_service();

    // Create
    let req = make_request(
        "CreateCustomKeyStore",
        json!({
            "CustomKeyStoreName": "my-store",
            "CloudHsmClusterId": "cluster-1234",
            "TrustAnchorCertificate": "cert-data",
            "KeyStorePassword": "password123"
        }),
    );
    let resp = svc.create_custom_key_store(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let store_id = body["CustomKeyStoreId"].as_str().unwrap().to_string();
    assert!(store_id.starts_with("cks-"));

    // Describe
    let req = make_request(
        "DescribeCustomKeyStores",
        json!({ "CustomKeyStoreId": store_id }),
    );
    let resp = svc.describe_custom_key_stores(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let stores = body["CustomKeyStores"].as_array().unwrap();
    assert_eq!(stores.len(), 1);
    assert_eq!(
        stores[0]["CustomKeyStoreName"].as_str().unwrap(),
        "my-store"
    );
    assert_eq!(
        stores[0]["ConnectionState"].as_str().unwrap(),
        "DISCONNECTED"
    );
    assert_eq!(
        stores[0]["CloudHsmClusterId"].as_str().unwrap(),
        "cluster-1234"
    );

    // Connect
    let req = make_request(
        "ConnectCustomKeyStore",
        json!({ "CustomKeyStoreId": store_id }),
    );
    svc.connect_custom_key_store(&req).unwrap();

    // Verify connected
    let req = make_request(
        "DescribeCustomKeyStores",
        json!({ "CustomKeyStoreId": store_id }),
    );
    let resp = svc.describe_custom_key_stores(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(
        body["CustomKeyStores"][0]["ConnectionState"]
            .as_str()
            .unwrap(),
        "CONNECTED"
    );

    // Cannot delete when connected
    let req = make_request(
        "DeleteCustomKeyStore",
        json!({ "CustomKeyStoreId": store_id }),
    );
    assert!(svc.delete_custom_key_store(&req).is_err());

    // Disconnect
    let req = make_request(
        "DisconnectCustomKeyStore",
        json!({ "CustomKeyStoreId": store_id }),
    );
    svc.disconnect_custom_key_store(&req).unwrap();

    // Update name
    let req = make_request(
        "UpdateCustomKeyStore",
        json!({
            "CustomKeyStoreId": store_id,
            "NewCustomKeyStoreName": "renamed-store"
        }),
    );
    svc.update_custom_key_store(&req).unwrap();

    // Verify update
    let req = make_request(
        "DescribeCustomKeyStores",
        json!({ "CustomKeyStoreId": store_id }),
    );
    let resp = svc.describe_custom_key_stores(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(
        body["CustomKeyStores"][0]["CustomKeyStoreName"]
            .as_str()
            .unwrap(),
        "renamed-store"
    );

    // Delete
    let req = make_request(
        "DeleteCustomKeyStore",
        json!({ "CustomKeyStoreId": store_id }),
    );
    svc.delete_custom_key_store(&req).unwrap();

    // Describe all should return empty
    let req = make_request("DescribeCustomKeyStores", json!({}));
    let resp = svc.describe_custom_key_stores(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert!(body["CustomKeyStores"].as_array().unwrap().is_empty());
}

#[test]
fn custom_key_store_duplicate_name_fails() {
    let svc = make_service();

    let req = make_request(
        "CreateCustomKeyStore",
        json!({ "CustomKeyStoreName": "dup-store" }),
    );
    svc.create_custom_key_store(&req).unwrap();

    let req = make_request(
        "CreateCustomKeyStore",
        json!({ "CustomKeyStoreName": "dup-store" }),
    );
    assert!(svc.create_custom_key_store(&req).is_err());
}

#[test]
fn describe_custom_key_store_not_found() {
    let svc = make_service();

    let req = make_request(
        "DescribeCustomKeyStores",
        json!({ "CustomKeyStoreId": "cks-nonexistent" }),
    );
    assert!(svc.describe_custom_key_stores(&req).is_err());
}

#[test]
fn delete_nonexistent_custom_key_store_fails() {
    let svc = make_service();

    let req = make_request(
        "DeleteCustomKeyStore",
        json!({ "CustomKeyStoreId": "cks-nonexistent" }),
    );
    assert!(svc.delete_custom_key_store(&req).is_err());
}

#[test]
fn connect_nonexistent_custom_key_store_fails() {
    let svc = make_service();

    let req = make_request(
        "ConnectCustomKeyStore",
        json!({ "CustomKeyStoreId": "cks-nonexistent" }),
    );
    assert!(svc.connect_custom_key_store(&req).is_err());
}

#[test]
fn describe_custom_key_stores_by_name() {
    let svc = make_service();

    let req = make_request(
        "CreateCustomKeyStore",
        json!({ "CustomKeyStoreName": "store-a" }),
    );
    svc.create_custom_key_store(&req).unwrap();

    let req = make_request(
        "CreateCustomKeyStore",
        json!({ "CustomKeyStoreName": "store-b" }),
    );
    svc.create_custom_key_store(&req).unwrap();

    // Filter by name
    let req = make_request(
        "DescribeCustomKeyStores",
        json!({ "CustomKeyStoreName": "store-a" }),
    );
    let resp = svc.describe_custom_key_stores(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let stores = body["CustomKeyStores"].as_array().unwrap();
    assert_eq!(stores.len(), 1);
    assert_eq!(stores[0]["CustomKeyStoreName"].as_str().unwrap(), "store-a");
}

#[test]
fn update_custom_key_store_name_conflict() {
    let svc = make_service();

    let req = make_request(
        "CreateCustomKeyStore",
        json!({ "CustomKeyStoreName": "store-x" }),
    );
    svc.create_custom_key_store(&req).unwrap();

    let req = make_request(
        "CreateCustomKeyStore",
        json!({ "CustomKeyStoreName": "store-y" }),
    );
    let resp = svc.create_custom_key_store(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let store_y_id = body["CustomKeyStoreId"].as_str().unwrap().to_string();

    // Try to rename store-y to store-x
    let req = make_request(
        "UpdateCustomKeyStore",
        json!({
            "CustomKeyStoreId": store_y_id,
            "NewCustomKeyStoreName": "store-x"
        }),
    );
    assert!(svc.update_custom_key_store(&req).is_err());
}

#[test]
fn derive_shared_secret_is_deterministic() {
    let svc = make_service();
    let key_id = create_key_with_opts(
        &svc,
        json!({
            "KeyUsage": "KEY_AGREEMENT",
            "KeySpec": "ECC_NIST_P256"
        }),
    );

    let pub_key = base64::engine::general_purpose::STANDARD.encode(b"counterparty-public-key");
    let req = make_request(
        "DeriveSharedSecret",
        json!({
            "KeyId": key_id,
            "KeyAgreementAlgorithm": "ECDH",
            "PublicKey": pub_key
        }),
    );

    let resp1 = svc.derive_shared_secret(&req).unwrap();
    let body1: Value = serde_json::from_slice(resp1.body.expect_bytes()).unwrap();
    let secret1 = body1["SharedSecret"].as_str().unwrap().to_string();

    // Same inputs must produce the same shared secret
    let resp2 = svc.derive_shared_secret(&req).unwrap();
    let body2: Value = serde_json::from_slice(resp2.body.expect_bytes()).unwrap();
    let secret2 = body2["SharedSecret"].as_str().unwrap().to_string();

    assert_eq!(secret1, secret2, "DeriveSharedSecret must be deterministic");

    // Different public key must produce a different shared secret
    let other_pub = base64::engine::general_purpose::STANDARD.encode(b"different-public-key");
    let req2 = make_request(
        "DeriveSharedSecret",
        json!({
            "KeyId": key_id,
            "KeyAgreementAlgorithm": "ECDH",
            "PublicKey": other_pub
        }),
    );
    let resp3 = svc.derive_shared_secret(&req2).unwrap();
    let body3: Value = serde_json::from_slice(resp3.body.expect_bytes()).unwrap();
    let secret3 = body3["SharedSecret"].as_str().unwrap().to_string();
    assert_ne!(
        secret1, secret3,
        "Different public keys must yield different shared secrets"
    );
}

#[test]
fn imported_key_material_encrypt_decrypt_roundtrip() {
    let svc = make_service();
    let key_id = create_key_with_opts(&svc, json!({ "Origin": "EXTERNAL" }));
    let material = b"my-secret-aes-key-material!12345";

    // Real RSA-OAEP wrap+unwrap flow.
    import_external_material(&svc, &key_id, material);

    // Encrypt
    let plaintext = b"Hello imported key!";
    let plaintext_b64 = base64::engine::general_purpose::STANDARD.encode(plaintext);
    let req = make_request(
        "Encrypt",
        json!({ "KeyId": key_id, "Plaintext": plaintext_b64 }),
    );
    let resp = svc.encrypt(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let ciphertext = body["CiphertextBlob"].as_str().unwrap().to_string();

    // Verify ciphertext uses the imported envelope
    let ct_bytes = base64::engine::general_purpose::STANDARD
        .decode(&ciphertext)
        .unwrap();
    let envelope = String::from_utf8(ct_bytes).unwrap();
    assert!(
        envelope.starts_with("fakecloud-imported:"),
        "Imported key should use fakecloud-imported envelope"
    );

    // Decrypt
    let req = make_request("Decrypt", json!({ "CiphertextBlob": ciphertext }));
    let resp = svc.decrypt(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let decrypted_b64 = body["Plaintext"].as_str().unwrap();
    let decrypted = base64::engine::general_purpose::STANDARD
        .decode(decrypted_b64)
        .unwrap();
    assert_eq!(
        decrypted, plaintext,
        "Decrypt must recover the original plaintext"
    );
}

#[test]
fn imported_key_material_decrypt_fails_after_deletion() {
    let svc = make_service();
    let key_id = create_key_with_opts(&svc, json!({ "Origin": "EXTERNAL" }));
    import_external_material(&svc, &key_id, b"some-key-material-32bytes!!");

    let plaintext_b64 = base64::engine::general_purpose::STANDARD.encode(b"secret");
    let resp = svc
        .encrypt(&make_request(
            "Encrypt",
            json!({ "KeyId": key_id, "Plaintext": plaintext_b64 }),
        ))
        .unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let ciphertext = body["CiphertextBlob"].as_str().unwrap().to_string();

    // Delete imported material
    svc.delete_imported_key_material(&make_request(
        "DeleteImportedKeyMaterial",
        json!({ "KeyId": key_id }),
    ))
    .unwrap();

    // Re-import to re-enable the key but material bytes are gone for old ciphertext path
    // Actually, after deletion the key is disabled, so decrypt will fail with DisabledException
    let result = svc.decrypt(&make_request(
        "Decrypt",
        json!({ "CiphertextBlob": ciphertext }),
    ));
    assert!(
        result.is_err(),
        "Decrypt should fail after key material deletion"
    );
}

#[test]
fn list_keys_rejects_non_integer_limit() {
    let svc = make_service();
    // String value should fail validation
    let req = make_request("ListKeys", json!({ "Limit": "abc" }));
    let result = svc.list_keys(&req);
    assert!(result.is_err(), "non-integer Limit should be rejected");
}

#[test]
fn list_keys_rejects_large_unsigned_limit() {
    let svc = make_service();
    // Value larger than i64::MAX should fail validation
    let req = make_request("ListKeys", json!({ "Limit": u64::MAX }));
    let result = svc.list_keys(&req);
    assert!(result.is_err(), "large unsigned Limit should be rejected");
}

#[test]
fn list_keys_rejects_out_of_range_limit() {
    let svc = make_service();
    let req = make_request("ListKeys", json!({ "Limit": 0 }));
    let result = svc.list_keys(&req);
    assert!(result.is_err(), "Limit=0 should be rejected");

    let req = make_request("ListKeys", json!({ "Limit": 1001 }));
    let result = svc.list_keys(&req);
    assert!(result.is_err(), "Limit=1001 should be rejected");
}

#[test]
fn enable_key_with_nonexistent_id_returns_error() {
    let svc = make_service();
    // Manually insert a resolved key ID into the state, then remove it to simulate
    // a race condition where resolve_required_key succeeds but get_mut fails
    let key_id = create_key(&svc);

    // Delete the key from state directly to simulate inconsistency
    svc.state.write().default_mut().keys.remove(&key_id);

    let req = make_request("EnableKey", json!({ "KeyId": key_id }));
    let result = svc.enable_key(&req);
    assert!(result.is_err(), "Should return error for missing key");
}

#[test]
fn disable_key_with_nonexistent_id_returns_error() {
    let svc = make_service();
    let key_id = create_key(&svc);
    svc.state.write().default_mut().keys.remove(&key_id);

    let req = make_request("DisableKey", json!({ "KeyId": key_id }));
    let result = svc.disable_key(&req);
    assert!(result.is_err(), "Should return error for missing key");
}

#[test]
fn tag_resource_with_nonexistent_key_returns_error() {
    let svc = make_service();
    let key_id = create_key(&svc);
    svc.state.write().default_mut().keys.remove(&key_id);

    let req = make_request(
        "TagResource",
        json!({ "KeyId": key_id, "Tags": [{"TagKey": "k", "TagValue": "v"}] }),
    );
    let result = svc.tag_resource(&req);
    assert!(result.is_err(), "Should return error for missing key");
}

#[test]
fn cancel_key_deletion_re_enables_key() {
    let svc = make_service();
    let key_id = create_key(&svc);

    // Schedule deletion
    let req = make_request(
        "ScheduleKeyDeletion",
        json!({ "KeyId": key_id, "PendingWindowInDays": 7 }),
    );
    let resp = svc.schedule_key_deletion(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["KeyState"].as_str().unwrap(), "PendingDeletion");

    // Verify key is pending deletion
    {
        let _accts = svc.state.read();
        let state = _accts.default_ref();
        let key = state.keys.get(&key_id).unwrap();
        assert_eq!(key.key_state, "PendingDeletion");
        assert!(!key.enabled);
        assert!(key.deletion_date.is_some());
    }

    // Cancel deletion
    let req = make_request("CancelKeyDeletion", json!({ "KeyId": key_id }));
    let resp = svc.cancel_key_deletion(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["KeyId"].as_str().unwrap(), key_id);

    // Key should be disabled (not enabled) with no deletion date
    {
        let _accts = svc.state.read();
        let state = _accts.default_ref();
        let key = state.keys.get(&key_id).unwrap();
        assert_eq!(key.key_state, "Disabled");
        assert!(key.deletion_date.is_none());
    }

    // Re-enable the key
    let req = make_request("EnableKey", json!({ "KeyId": key_id }));
    svc.enable_key(&req).unwrap();

    {
        let _accts = svc.state.read();
        let state = _accts.default_ref();
        let key = state.keys.get(&key_id).unwrap();
        assert!(key.enabled);
        assert_eq!(key.key_state, "Enabled");
    }
}

#[test]
fn key_rotation_lifecycle() {
    let svc = make_service();
    let key_id = create_key(&svc);

    // Initially rotation is disabled
    let req = make_request("GetKeyRotationStatus", json!({ "KeyId": key_id }));
    let resp = svc.get_key_rotation_status(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert!(!body["KeyRotationEnabled"].as_bool().unwrap());

    // Enable rotation
    let req = make_request("EnableKeyRotation", json!({ "KeyId": key_id }));
    svc.enable_key_rotation(&req).unwrap();

    let req = make_request("GetKeyRotationStatus", json!({ "KeyId": key_id }));
    let resp = svc.get_key_rotation_status(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert!(body["KeyRotationEnabled"].as_bool().unwrap());

    // Disable rotation
    let req = make_request("DisableKeyRotation", json!({ "KeyId": key_id }));
    svc.disable_key_rotation(&req).unwrap();

    let req = make_request("GetKeyRotationStatus", json!({ "KeyId": key_id }));
    let resp = svc.get_key_rotation_status(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert!(!body["KeyRotationEnabled"].as_bool().unwrap());
}

#[test]
fn rotate_key_on_demand_and_list_rotations() {
    let svc = make_service();
    let key_id = create_key(&svc);

    // No rotations initially
    let req = make_request("ListKeyRotations", json!({ "KeyId": key_id }));
    let resp = svc.list_key_rotations(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert!(body["Rotations"].as_array().unwrap().is_empty());

    // Rotate on demand
    let req = make_request("RotateKeyOnDemand", json!({ "KeyId": key_id }));
    let resp = svc.rotate_key_on_demand(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["KeyId"].as_str().unwrap(), key_id);

    // Rotate again
    let req = make_request("RotateKeyOnDemand", json!({ "KeyId": key_id }));
    svc.rotate_key_on_demand(&req).unwrap();

    // List rotations
    let req = make_request("ListKeyRotations", json!({ "KeyId": key_id }));
    let resp = svc.list_key_rotations(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let rotations = body["Rotations"].as_array().unwrap();
    assert_eq!(rotations.len(), 2);
    assert_eq!(rotations[0]["RotationType"].as_str().unwrap(), "ON_DEMAND");
    assert_eq!(rotations[0]["KeyId"].as_str().unwrap(), key_id);
    assert!(rotations[0]["RotationDate"].as_f64().is_some());
}

#[test]
fn key_policy_get_put_list() {
    let svc = make_service();
    let key_id = create_key(&svc);

    // Get default policy
    let req = make_request("GetKeyPolicy", json!({ "KeyId": key_id }));
    let resp = svc.get_key_policy(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let policy_str = body["Policy"].as_str().unwrap();
    assert!(policy_str.contains("Enable IAM User Permissions"));

    // Put custom policy
    let custom_policy = r#"{"Version":"2012-10-17","Statement":[]}"#;
    let req = make_request(
        "PutKeyPolicy",
        json!({ "KeyId": key_id, "Policy": custom_policy }),
    );
    svc.put_key_policy(&req).unwrap();

    // Get updated policy
    let req = make_request("GetKeyPolicy", json!({ "KeyId": key_id }));
    let resp = svc.get_key_policy(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["Policy"].as_str().unwrap(), custom_policy);

    // List key policies always returns ["default"]
    let req = make_request("ListKeyPolicies", json!({ "KeyId": key_id }));
    let resp = svc.list_key_policies(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let names = body["PolicyNames"].as_array().unwrap();
    assert_eq!(names.len(), 1);
    assert_eq!(names[0].as_str().unwrap(), "default");
}

#[test]
fn grant_create_list_revoke() {
    let svc = make_service();
    let key_id = create_key(&svc);

    // Create a grant
    let req = make_request(
        "CreateGrant",
        json!({
            "KeyId": key_id,
            "GranteePrincipal": "arn:aws:iam::123456789012:user/alice",
            "Operations": ["Encrypt", "Decrypt"],
            "Name": "test-grant"
        }),
    );
    let resp = svc.create_grant(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let grant_id = body["GrantId"].as_str().unwrap().to_string();
    let grant_token = body["GrantToken"].as_str().unwrap().to_string();
    assert!(!grant_id.is_empty());
    assert!(!grant_token.is_empty());

    // List grants
    let req = make_request("ListGrants", json!({ "KeyId": key_id }));
    let resp = svc.list_grants(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let grants = body["Grants"].as_array().unwrap();
    assert_eq!(grants.len(), 1);
    assert_eq!(grants[0]["GrantId"].as_str().unwrap(), grant_id);
    assert_eq!(
        grants[0]["GranteePrincipal"].as_str().unwrap(),
        "arn:aws:iam::123456789012:user/alice"
    );
    assert_eq!(grants[0]["Operations"].as_array().unwrap().len(), 2);

    // Revoke the grant
    let req = make_request(
        "RevokeGrant",
        json!({ "KeyId": key_id, "GrantId": grant_id }),
    );
    svc.revoke_grant(&req).unwrap();

    // List grants should be empty
    let req = make_request("ListGrants", json!({ "KeyId": key_id }));
    let resp = svc.list_grants(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert!(body["Grants"].as_array().unwrap().is_empty());
}

#[test]
fn grant_retire_by_token() {
    let svc = make_service();
    let key_id = create_key(&svc);

    let req = make_request(
        "CreateGrant",
        json!({
            "KeyId": key_id,
            "GranteePrincipal": "arn:aws:iam::123456789012:user/bob",
            "RetiringPrincipal": "arn:aws:iam::123456789012:user/admin",
            "Operations": ["Encrypt"]
        }),
    );
    let resp = svc.create_grant(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let grant_token = body["GrantToken"].as_str().unwrap().to_string();

    // Retire by token
    let req = make_request("RetireGrant", json!({ "GrantToken": grant_token }));
    svc.retire_grant(&req).unwrap();

    // Verify grant is gone
    let req = make_request("ListGrants", json!({ "KeyId": key_id }));
    let resp = svc.list_grants(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert!(body["Grants"].as_array().unwrap().is_empty());
}

#[test]
fn grant_retire_by_key_and_grant_id() {
    let svc = make_service();
    let key_id = create_key(&svc);

    let req = make_request(
        "CreateGrant",
        json!({
            "KeyId": key_id,
            "GranteePrincipal": "arn:aws:iam::123456789012:user/charlie",
            "Operations": ["Decrypt"]
        }),
    );
    let resp = svc.create_grant(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let grant_id = body["GrantId"].as_str().unwrap().to_string();

    // Retire by key ID + grant ID
    let req = make_request(
        "RetireGrant",
        json!({ "KeyId": key_id, "GrantId": grant_id }),
    );
    svc.retire_grant(&req).unwrap();

    // Verify grant is gone
    let req = make_request("ListGrants", json!({ "KeyId": key_id }));
    let resp = svc.list_grants(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert!(body["Grants"].as_array().unwrap().is_empty());
}

#[test]
fn sign_verify_roundtrip() {
    let svc = make_service();
    let key_id = create_key_with_opts(
        &svc,
        json!({ "KeyUsage": "SIGN_VERIFY", "KeySpec": "RSA_2048" }),
    );

    let message = b"data to sign";
    let message_b64 = base64::engine::general_purpose::STANDARD.encode(message);

    // Sign
    let req = make_request(
        "Sign",
        json!({
            "KeyId": key_id,
            "Message": message_b64,
            "SigningAlgorithm": "RSASSA_PKCS1_V1_5_SHA_256"
        }),
    );
    let resp = svc.sign(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let signature = body["Signature"].as_str().unwrap().to_string();
    assert!(!signature.is_empty());
    assert_eq!(
        body["SigningAlgorithm"].as_str().unwrap(),
        "RSASSA_PKCS1_V1_5_SHA_256"
    );

    // Verify with correct signature
    let req = make_request(
        "Verify",
        json!({
            "KeyId": key_id,
            "Message": message_b64,
            "Signature": signature,
            "SigningAlgorithm": "RSASSA_PKCS1_V1_5_SHA_256"
        }),
    );
    let resp = svc.verify(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert!(body["SignatureValid"].as_bool().unwrap());

    // Verify with wrong signature should fail with KMSInvalidSignatureException
    let wrong_sig = base64::engine::general_purpose::STANDARD.encode(b"wrong-signature-data");
    let req = make_request(
        "Verify",
        json!({
            "KeyId": key_id,
            "Message": message_b64,
            "Signature": wrong_sig,
            "SigningAlgorithm": "RSASSA_PKCS1_V1_5_SHA_256"
        }),
    );
    assert!(svc.verify(&req).is_err());
}

#[test]
fn sign_with_ecc_key() {
    let svc = make_service();
    let key_id = create_key_with_opts(
        &svc,
        json!({ "KeyUsage": "SIGN_VERIFY", "KeySpec": "ECC_NIST_P256" }),
    );

    let message_b64 = base64::engine::general_purpose::STANDARD.encode(b"ecc data");
    let req = make_request(
        "Sign",
        json!({
            "KeyId": key_id,
            "Message": message_b64,
            "SigningAlgorithm": "ECDSA_SHA_256"
        }),
    );
    let resp = svc.sign(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert!(body["Signature"].as_str().is_some());
    assert_eq!(body["SigningAlgorithm"].as_str().unwrap(), "ECDSA_SHA_256");
}

#[test]
fn ecdsa_p256_sign_verify_round_trips_with_real_signature() {
    let svc = make_service();
    let key_id = create_key_with_opts(
        &svc,
        json!({ "KeyUsage": "SIGN_VERIFY", "KeySpec": "ECC_NIST_P256" }),
    );
    let message = b"ecc roundtrip";
    let message_b64 = base64::engine::general_purpose::STANDARD.encode(message);

    let resp = svc
        .sign(&make_request(
            "Sign",
            json!({
                "KeyId": key_id,
                "Message": message_b64,
                "SigningAlgorithm": "ECDSA_SHA_256",
            }),
        ))
        .unwrap();
    let sig_body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let signature = sig_body["Signature"].as_str().unwrap().to_string();

    let resp = svc
        .verify(&make_request(
            "Verify",
            json!({
                "KeyId": key_id,
                "Message": message_b64,
                "Signature": signature,
                "SigningAlgorithm": "ECDSA_SHA_256",
            }),
        ))
        .unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert!(body["SignatureValid"].as_bool().unwrap());

    // Tampered message must fail.
    let bad_b64 = base64::engine::general_purpose::STANDARD.encode(b"different");
    assert!(svc
        .verify(&make_request(
            "Verify",
            json!({
                "KeyId": key_id,
                "Message": bad_b64,
                "Signature": signature,
                "SigningAlgorithm": "ECDSA_SHA_256",
            }),
        ))
        .is_err());
}

#[test]
fn ecdsa_p256_get_public_key_is_real_parseable_spki() {
    use base64::Engine;
    use p256::pkcs8::DecodePublicKey;
    let svc = make_service();
    let key_id = create_key_with_opts(
        &svc,
        json!({ "KeyUsage": "SIGN_VERIFY", "KeySpec": "ECC_NIST_P256" }),
    );
    let resp = svc
        .get_public_key(&make_request("GetPublicKey", json!({ "KeyId": key_id })))
        .unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let der = base64::engine::general_purpose::STANDARD
        .decode(body["PublicKey"].as_str().unwrap())
        .unwrap();
    assert!(p256::PublicKey::from_public_key_der(&der).is_ok());
}

#[test]
fn ecdsa_p384_sign_verify_round_trips() {
    let svc = make_service();
    let key_id = create_key_with_opts(
        &svc,
        json!({ "KeyUsage": "SIGN_VERIFY", "KeySpec": "ECC_NIST_P384" }),
    );
    let message_b64 = base64::engine::general_purpose::STANDARD.encode(b"p384");
    let resp = svc
        .sign(&make_request(
            "Sign",
            json!({
                "KeyId": key_id,
                "Message": message_b64,
                "SigningAlgorithm": "ECDSA_SHA_384",
            }),
        ))
        .unwrap();
    let sig = serde_json::from_slice::<Value>(resp.body.expect_bytes()).unwrap()["Signature"]
        .as_str()
        .unwrap()
        .to_string();
    let resp = svc
        .verify(&make_request(
            "Verify",
            json!({
                "KeyId": key_id,
                "Message": message_b64,
                "Signature": sig,
                "SigningAlgorithm": "ECDSA_SHA_384",
            }),
        ))
        .unwrap();
    assert!(
        serde_json::from_slice::<Value>(resp.body.expect_bytes()).unwrap()["SignatureValid"]
            .as_bool()
            .unwrap()
    );
}

#[test]
fn sign_wrong_key_usage_fails() {
    let svc = make_service();
    let key_id = create_key(&svc); // ENCRYPT_DECRYPT

    let message_b64 = base64::engine::general_purpose::STANDARD.encode(b"test");
    let req = make_request(
        "Sign",
        json!({
            "KeyId": key_id,
            "Message": message_b64,
            "SigningAlgorithm": "RSASSA_PKCS1_V1_5_SHA_256"
        }),
    );
    assert!(svc.sign(&req).is_err());
}

#[test]
fn generate_random_various_lengths() {
    let svc = make_service();

    for num_bytes in [1, 16, 32, 64, 256, 1024] {
        let req = make_request("GenerateRandom", json!({ "NumberOfBytes": num_bytes }));
        let resp = svc.generate_random(&req).unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        let b64 = body["Plaintext"].as_str().unwrap();
        let decoded = base64::engine::general_purpose::STANDARD
            .decode(b64)
            .unwrap();
        assert_eq!(
            decoded.len(),
            num_bytes as usize,
            "GenerateRandom({num_bytes}) returned wrong length"
        );
    }
}

#[test]
fn generate_random_zero_bytes_fails() {
    let svc = make_service();
    let req = make_request("GenerateRandom", json!({ "NumberOfBytes": 0 }));
    assert!(svc.generate_random(&req).is_err());
}

#[test]
fn generate_random_too_many_bytes_fails() {
    let svc = make_service();
    let req = make_request("GenerateRandom", json!({ "NumberOfBytes": 1025 }));
    assert!(svc.generate_random(&req).is_err());
}

#[test]
fn generate_mac_verify_mac_roundtrip() {
    let svc = make_service();
    let key_id = create_key_with_opts(
        &svc,
        json!({ "KeyUsage": "GENERATE_VERIFY_MAC", "KeySpec": "HMAC_256" }),
    );

    let message_b64 = base64::engine::general_purpose::STANDARD.encode(b"mac message");

    // Generate MAC
    let req = make_request(
        "GenerateMac",
        json!({
            "KeyId": key_id,
            "Message": message_b64,
            "MacAlgorithm": "HMAC_SHA_256"
        }),
    );
    let resp = svc.generate_mac(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let mac = body["Mac"].as_str().unwrap().to_string();
    assert!(!mac.is_empty());

    // Verify MAC
    let req = make_request(
        "VerifyMac",
        json!({
            "KeyId": key_id,
            "Message": message_b64,
            "Mac": mac,
            "MacAlgorithm": "HMAC_SHA_256"
        }),
    );
    let resp = svc.verify_mac(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert!(body["MacValid"].as_bool().unwrap());
}

#[test]
fn verify_mac_wrong_mac_fails() {
    let svc = make_service();
    let key_id = create_key_with_opts(
        &svc,
        json!({ "KeyUsage": "GENERATE_VERIFY_MAC", "KeySpec": "HMAC_256" }),
    );

    let message_b64 = base64::engine::general_purpose::STANDARD.encode(b"msg");
    let wrong_mac = base64::engine::general_purpose::STANDARD.encode(b"wrong-mac");

    let req = make_request(
        "VerifyMac",
        json!({
            "KeyId": key_id,
            "Message": message_b64,
            "Mac": wrong_mac,
            "MacAlgorithm": "HMAC_SHA_256"
        }),
    );
    assert!(svc.verify_mac(&req).is_err());
}

#[test]
fn generate_mac_wrong_key_usage_fails() {
    let svc = make_service();
    let key_id = create_key(&svc); // ENCRYPT_DECRYPT

    let message_b64 = base64::engine::general_purpose::STANDARD.encode(b"msg");
    let req = make_request(
        "GenerateMac",
        json!({
            "KeyId": key_id,
            "Message": message_b64,
            "MacAlgorithm": "HMAC_SHA_256"
        }),
    );
    assert!(svc.generate_mac(&req).is_err());
}

#[test]
fn re_encrypt_between_keys() {
    let svc = make_service();
    let key_a = create_key(&svc);
    let key_b = create_key(&svc);

    // Encrypt with key A
    let plaintext = b"re-encrypt test data";
    let plaintext_b64 = base64::engine::general_purpose::STANDARD.encode(plaintext);
    let req = make_request(
        "Encrypt",
        json!({ "KeyId": key_a, "Plaintext": plaintext_b64 }),
    );
    let resp = svc.encrypt(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let ciphertext_a = body["CiphertextBlob"].as_str().unwrap().to_string();

    // Re-encrypt from key A to key B
    let req = make_request(
        "ReEncrypt",
        json!({
            "CiphertextBlob": ciphertext_a,
            "DestinationKeyId": key_b
        }),
    );
    let resp = svc.re_encrypt(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let ciphertext_b = body["CiphertextBlob"].as_str().unwrap().to_string();
    assert_ne!(ciphertext_a, ciphertext_b);
    assert!(body["KeyId"].as_str().unwrap().contains(&key_b));
    assert!(body["SourceKeyId"].as_str().unwrap().contains(&key_a));

    // Decrypt with key B (the ciphertext is self-describing in fakecloud)
    let req = make_request("Decrypt", json!({ "CiphertextBlob": ciphertext_b }));
    let resp = svc.decrypt(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let decrypted_b64 = body["Plaintext"].as_str().unwrap();
    let decrypted = base64::engine::general_purpose::STANDARD
        .decode(decrypted_b64)
        .unwrap();
    assert_eq!(decrypted, plaintext);
}

#[test]
fn update_alias_points_to_different_key() {
    let svc = make_service();
    let key_a = create_key(&svc);
    let key_b = create_key(&svc);

    // Create alias pointing to key A
    let req = make_request(
        "CreateAlias",
        json!({ "AliasName": "alias/switchable", "TargetKeyId": key_a }),
    );
    svc.create_alias(&req).unwrap();

    // Verify alias points to key A
    {
        let _accts = svc.state.read();
        let state = _accts.default_ref();
        let alias = state.aliases.get("alias/switchable").unwrap();
        assert_eq!(alias.target_key_id, key_a);
    }

    // Update alias to point to key B
    let req = make_request(
        "UpdateAlias",
        json!({ "AliasName": "alias/switchable", "TargetKeyId": key_b }),
    );
    svc.update_alias(&req).unwrap();

    // Verify alias now points to key B
    {
        let _accts = svc.state.read();
        let state = _accts.default_ref();
        let alias = state.aliases.get("alias/switchable").unwrap();
        assert_eq!(alias.target_key_id, key_b);
    }
}

#[test]
fn update_key_description_changes_description() {
    let svc = make_service();
    let key_id = create_key(&svc);

    // Initially empty description
    {
        let _accts = svc.state.read();
        let state = _accts.default_ref();
        let key = state.keys.get(&key_id).unwrap();
        assert_eq!(key.description, "");
    }

    // Update description
    let req = make_request(
        "UpdateKeyDescription",
        json!({ "KeyId": key_id, "Description": "new description" }),
    );
    svc.update_key_description(&req).unwrap();

    {
        let _accts = svc.state.read();
        let state = _accts.default_ref();
        let key = state.keys.get(&key_id).unwrap();
        assert_eq!(key.description, "new description");
    }

    // Update again
    let req = make_request(
        "UpdateKeyDescription",
        json!({ "KeyId": key_id, "Description": "updated again" }),
    );
    svc.update_key_description(&req).unwrap();

    {
        let _accts = svc.state.read();
        let state = _accts.default_ref();
        let key = state.keys.get(&key_id).unwrap();
        assert_eq!(key.description, "updated again");
    }
}

#[test]
fn get_public_key_for_asymmetric_key() {
    let svc = make_service();

    // RSA signing key
    let key_id = create_key_with_opts(
        &svc,
        json!({ "KeyUsage": "SIGN_VERIFY", "KeySpec": "RSA_2048" }),
    );

    let req = make_request("GetPublicKey", json!({ "KeyId": key_id }));
    let resp = svc.get_public_key(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();

    assert!(body["PublicKey"].as_str().is_some());
    assert_eq!(body["KeySpec"].as_str().unwrap(), "RSA_2048");
    assert_eq!(body["KeyUsage"].as_str().unwrap(), "SIGN_VERIFY");
    assert!(body["SigningAlgorithms"].as_array().is_some());
    assert!(body["KeyId"].as_str().unwrap().contains(":key/"));

    // ECC key
    let ecc_key_id = create_key_with_opts(
        &svc,
        json!({ "KeyUsage": "SIGN_VERIFY", "KeySpec": "ECC_NIST_P256" }),
    );

    let req = make_request("GetPublicKey", json!({ "KeyId": ecc_key_id }));
    let resp = svc.get_public_key(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert!(body["PublicKey"].as_str().is_some());
    assert_eq!(body["KeySpec"].as_str().unwrap(), "ECC_NIST_P256");
}

#[test]
fn rsa_get_public_key_is_real_parseable_spki() {
    use base64::Engine;
    use rsa::pkcs8::DecodePublicKey;
    let svc = make_service();
    let key_id = create_key_with_opts(
        &svc,
        json!({ "KeyUsage": "SIGN_VERIFY", "KeySpec": "RSA_2048" }),
    );
    let req = make_request("GetPublicKey", json!({ "KeyId": key_id }));
    let resp = svc.get_public_key(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let der = base64::engine::general_purpose::STANDARD
        .decode(body["PublicKey"].as_str().unwrap())
        .unwrap();
    let parsed = rsa::RsaPublicKey::from_public_key_der(&der);
    assert!(
        parsed.is_ok(),
        "GetPublicKey for RSA_2048 must return real parseable SPKI DER"
    );
}

#[test]
fn rsa_sign_verify_round_trips_against_get_public_key() {
    // Sanity-check the full external loop: client signs via Sign,
    // pulls the public key via GetPublicKey, verifies locally with
    // a real RSA library. If our Sign output isn't a real RSA
    // signature this test fails.
    use base64::Engine;
    use rsa::pkcs8::DecodePublicKey;
    use rsa::sha2::Sha256;
    use signature::Verifier as _;

    let svc = make_service();
    let key_id = create_key_with_opts(
        &svc,
        json!({ "KeyUsage": "SIGN_VERIFY", "KeySpec": "RSA_2048" }),
    );
    let message = b"sign me";
    let message_b64 = base64::engine::general_purpose::STANDARD.encode(message);

    let resp = svc
        .sign(&make_request(
            "Sign",
            json!({
                "KeyId": key_id,
                "Message": message_b64,
                "SigningAlgorithm": "RSASSA_PKCS1_V1_5_SHA_256"
            }),
        ))
        .unwrap();
    let sig_body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let signature_bytes = base64::engine::general_purpose::STANDARD
        .decode(sig_body["Signature"].as_str().unwrap())
        .unwrap();

    let resp = svc
        .get_public_key(&make_request("GetPublicKey", json!({ "KeyId": key_id })))
        .unwrap();
    let pub_body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let pub_der = base64::engine::general_purpose::STANDARD
        .decode(pub_body["PublicKey"].as_str().unwrap())
        .unwrap();
    let public = rsa::RsaPublicKey::from_public_key_der(&pub_der).unwrap();
    let vk: rsa::pkcs1v15::VerifyingKey<Sha256> = rsa::pkcs1v15::VerifyingKey::new(public);
    let parsed_sig = rsa::pkcs1v15::Signature::try_from(signature_bytes.as_slice()).unwrap();
    assert!(vk.verify(message, &parsed_sig).is_ok());
}

// ── Batch 8 additions: key lifecycle, alias, policy handlers ─────

fn body_json(resp: AwsResponse) -> Value {
    serde_json::from_slice(resp.body.expect_bytes()).unwrap()
}

#[test]
fn describe_key_returns_metadata_for_new_key() {
    let svc = make_service();
    let key_id = create_key(&svc);
    let resp = svc
        .describe_key(&make_request("DescribeKey", json!({ "KeyId": key_id })))
        .unwrap();
    let body = body_json(resp);
    assert_eq!(body["KeyMetadata"]["KeyId"], json!(key_id));
    assert_eq!(body["KeyMetadata"]["Enabled"], json!(true));
    assert_eq!(body["KeyMetadata"]["KeyState"], json!("Enabled"));
}

#[test]
fn describe_key_requires_key_id() {
    let svc = make_service();
    let err = svc
        .describe_key(&make_request("DescribeKey", json!({})))
        .err()
        .expect("expected error");
    assert_eq!(err.code(), "ValidationException");
}

#[test]
fn describe_key_unknown_errors() {
    let svc = make_service();
    let err = svc
        .describe_key(&make_request(
            "DescribeKey",
            json!({ "KeyId": "00000000-0000-0000-0000-000000000000" }),
        ))
        .err()
        .expect("expected error");
    assert_eq!(err.code(), "NotFoundException");
}

#[test]
fn enable_disable_key_flip_the_state() {
    let svc = make_service();
    let key_id = create_key(&svc);
    svc.disable_key(&make_request("DisableKey", json!({ "KeyId": key_id })))
        .unwrap();
    let resp = svc
        .describe_key(&make_request("DescribeKey", json!({ "KeyId": key_id })))
        .unwrap();
    assert_eq!(
        body_json(resp)["KeyMetadata"]["KeyState"],
        json!("Disabled")
    );

    svc.enable_key(&make_request("EnableKey", json!({ "KeyId": key_id })))
        .unwrap();
    let resp = svc
        .describe_key(&make_request("DescribeKey", json!({ "KeyId": key_id })))
        .unwrap();
    assert_eq!(body_json(resp)["KeyMetadata"]["KeyState"], json!("Enabled"));
}

#[test]
fn schedule_key_deletion_sets_pending_deletion_state() {
    let svc = make_service();
    let key_id = create_key(&svc);
    let resp = svc
        .schedule_key_deletion(&make_request(
            "ScheduleKeyDeletion",
            json!({ "KeyId": key_id, "PendingWindowInDays": 7 }),
        ))
        .unwrap();
    let body = body_json(resp);
    assert_eq!(body["KeyState"], json!("PendingDeletion"));
    assert_eq!(body["PendingWindowInDays"], json!(7));
    assert!(body["DeletionDate"].as_f64().unwrap() > 0.0);
}

#[test]
fn schedule_key_deletion_defaults_to_30_days() {
    let svc = make_service();
    let key_id = create_key(&svc);
    let resp = svc
        .schedule_key_deletion(&make_request(
            "ScheduleKeyDeletion",
            json!({ "KeyId": key_id }),
        ))
        .unwrap();
    assert_eq!(body_json(resp)["PendingWindowInDays"], json!(30));
}

#[test]
fn list_keys_returns_created_keys() {
    let svc = make_service();
    let id1 = create_key(&svc);
    let id2 = create_key(&svc);
    let resp = svc.list_keys(&make_request("ListKeys", json!({}))).unwrap();
    let body = body_json(resp);
    let ids: Vec<String> = body["Keys"]
        .as_array()
        .unwrap()
        .iter()
        .map(|k| k["KeyId"].as_str().unwrap().to_string())
        .collect();
    assert!(ids.contains(&id1));
    assert!(ids.contains(&id2));
}

#[test]
fn list_keys_respects_limit_and_next_marker() {
    let svc = make_service();
    let _ = create_key(&svc);
    let _ = create_key(&svc);
    let _ = create_key(&svc);

    let resp = svc
        .list_keys(&make_request("ListKeys", json!({ "Limit": 2 })))
        .unwrap();
    let body = body_json(resp);
    assert_eq!(body["Keys"].as_array().unwrap().len(), 2);
    assert_eq!(body["Truncated"], json!(true));
    assert!(body["NextMarker"].is_string());
}

// ── Aliases ──────────────────────────────────────────────────────

fn create_alias(svc: &KmsService, alias: &str, target: &str) {
    svc.create_alias(&make_request(
        "CreateAlias",
        json!({ "AliasName": alias, "TargetKeyId": target }),
    ))
    .unwrap();
}

#[test]
fn create_alias_rejects_malformed_name() {
    let svc = make_service();
    let key_id = create_key(&svc);
    let err = svc
        .create_alias(&make_request(
            "CreateAlias",
            json!({ "AliasName": "missing-prefix", "TargetKeyId": key_id }),
        ))
        .err()
        .expect("expected error");
    // CreateAlias's Smithy contract declares InvalidAliasNameException for
    // alias-shape failures, so we surface that rather than ValidationException.
    assert_eq!(err.code(), "InvalidAliasNameException");
}

#[test]
fn create_alias_rejects_unknown_target() {
    let svc = make_service();
    let err = svc
        .create_alias(&make_request(
            "CreateAlias",
            json!({
                "AliasName": "alias/my-key",
                "TargetKeyId": "00000000-0000-0000-0000-000000000000"
            }),
        ))
        .err()
        .expect("expected error");
    assert_eq!(err.code(), "NotFoundException");
}

/// 1.22: ListAliases must honor Limit/Marker with correct
/// Truncated/NextMarker instead of hardcoding Truncated:false.
#[test]
fn list_aliases_paginates_with_limit_and_marker() {
    let svc = make_service();
    let key_id = create_key(&svc);
    for i in 0..5 {
        create_alias(&svc, &format!("alias/cust-{i}"), &key_id);
    }

    // First page: Limit=2 -> 2 aliases (plus any AWS-managed) and a marker.
    let resp = svc
        .list_aliases(&make_request("ListAliases", json!({ "Limit": 2 })))
        .unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let page1 = body["Aliases"].as_array().unwrap();
    assert_eq!(page1.len(), 2, "Limit must cap the page size");
    assert_eq!(body["Truncated"].as_bool(), Some(true));
    let marker = body["NextMarker"]
        .as_str()
        .expect("NextMarker present")
        .to_string();
    assert_eq!(
        marker,
        page1.last().unwrap()["AliasName"].as_str().unwrap(),
        "NextMarker is the last alias on the page"
    );

    // Second page: resume after the marker, no overlap.
    let resp = svc
        .list_aliases(&make_request(
            "ListAliases",
            json!({ "Limit": 2, "Marker": marker }),
        ))
        .unwrap();
    let body2: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let page2 = body2["Aliases"].as_array().unwrap();
    assert!(!page2.is_empty());
    let p1_names: Vec<&str> = page1
        .iter()
        .map(|a| a["AliasName"].as_str().unwrap())
        .collect();
    for a in page2 {
        assert!(
            !p1_names.contains(&a["AliasName"].as_str().unwrap()),
            "second page must not repeat the first"
        );
    }
}

#[test]
fn create_alias_duplicate_errors() {
    let svc = make_service();
    let key_id = create_key(&svc);
    create_alias(&svc, "alias/dup", &key_id);
    let err = svc
        .create_alias(&make_request(
            "CreateAlias",
            json!({ "AliasName": "alias/dup", "TargetKeyId": key_id }),
        ))
        .err()
        .expect("expected error");
    assert_eq!(err.code(), "AlreadyExistsException");
}

#[test]
fn delete_alias_removes_entry() {
    let svc = make_service();
    let key_id = create_key(&svc);
    create_alias(&svc, "alias/deleteme", &key_id);
    svc.delete_alias(&make_request(
        "DeleteAlias",
        json!({ "AliasName": "alias/deleteme" }),
    ))
    .unwrap();
    let err = svc
        .delete_alias(&make_request(
            "DeleteAlias",
            json!({ "AliasName": "alias/deleteme" }),
        ))
        .err()
        .expect("expected error");
    assert_eq!(err.code(), "NotFoundException");
}

#[test]
fn delete_alias_rejects_missing_prefix() {
    let svc = make_service();
    let err = svc
        .delete_alias(&make_request(
            "DeleteAlias",
            json!({ "AliasName": "no-prefix" }),
        ))
        .err()
        .expect("expected error");
    // DeleteAlias only declares NotFoundException / KMSInternal /
    // KMSInvalidState / DependencyTimeout — malformed input collapses into
    // NotFoundException.
    assert_eq!(err.code(), "NotFoundException");
}

#[test]
fn list_aliases_filters_by_key_id() {
    let svc = make_service();
    let k1 = create_key(&svc);
    let k2 = create_key(&svc);
    create_alias(&svc, "alias/a", &k1);
    create_alias(&svc, "alias/b", &k2);

    let resp = svc
        .list_aliases(&make_request("ListAliases", json!({ "KeyId": k1 })))
        .unwrap();
    let body = body_json(resp);
    let names: Vec<String> = body["Aliases"]
        .as_array()
        .unwrap()
        .iter()
        .map(|a| a["AliasName"].as_str().unwrap().to_string())
        .collect();
    assert_eq!(names, vec!["alias/a".to_string()]);
}

// ── Key policies ─────────────────────────────────────────────────

#[test]
fn get_key_policy_returns_policy_field() {
    let svc = make_service();
    let key_id = create_key(&svc);
    let resp = svc
        .get_key_policy(&make_request(
            "GetKeyPolicy",
            json!({ "KeyId": key_id, "PolicyName": "default" }),
        ))
        .unwrap();
    let body = body_json(resp);
    assert!(body["Policy"].as_str().is_some());
}

#[test]
fn get_key_policy_rejects_alias_as_key_id() {
    let svc = make_service();
    let err = svc
        .get_key_policy(&make_request(
            "GetKeyPolicy",
            json!({ "KeyId": "alias/anything", "PolicyName": "default" }),
        ))
        .err()
        .expect("expected error");
    assert_eq!(err.code(), "NotFoundException");
}

#[test]
fn list_key_policies_returns_default_name() {
    let svc = make_service();
    let key_id = create_key(&svc);
    let resp = svc
        .list_key_policies(&make_request("ListKeyPolicies", json!({ "KeyId": key_id })))
        .unwrap();
    let body = body_json(resp);
    assert_eq!(
        body["PolicyNames"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_str().unwrap())
            .collect::<Vec<_>>(),
        vec!["default"]
    );
}

#[test]
fn put_get_key_policy_round_trip() {
    let svc = make_service();
    let key_id = create_key(&svc);
    let custom_policy = json!({
        "Version": "2012-10-17",
        "Statement": [{
            "Sid": "Custom",
            "Effect": "Allow",
            "Principal": { "AWS": "arn:aws:iam::123456789012:root" },
            "Action": "kms:*",
            "Resource": "*"
        }]
    })
    .to_string();

    svc.put_key_policy(&make_request(
        "PutKeyPolicy",
        json!({
            "KeyId": key_id,
            "PolicyName": "default",
            "Policy": custom_policy
        }),
    ))
    .unwrap();

    let resp = svc
        .get_key_policy(&make_request(
            "GetKeyPolicy",
            json!({ "KeyId": key_id, "PolicyName": "default" }),
        ))
        .unwrap();
    let body = body_json(resp);
    let got: Value = serde_json::from_str(body["Policy"].as_str().unwrap()).unwrap();
    assert_eq!(got["Statement"][0]["Sid"], json!("Custom"));
}

#[test]
fn put_key_policy_rejects_alias_as_key_id() {
    let svc = make_service();
    let err = svc
        .put_key_policy(&make_request(
            "PutKeyPolicy",
            json!({
                "KeyId": "alias/anything",
                "PolicyName": "default",
                "Policy": "{}"
            }),
        ))
        .err()
        .expect("expected error");
    assert_eq!(err.code(), "NotFoundException");
}

// ── GenerateRandom (not already covered by existing tests) ──────

#[test]
fn generate_random_returns_base64_encoded_payload() {
    let svc = make_service();
    let resp = svc
        .generate_random(&make_request(
            "GenerateRandom",
            json!({ "NumberOfBytes": 32 }),
        ))
        .unwrap();
    let body = body_json(resp);
    let plaintext = body["Plaintext"].as_str().unwrap();
    let decoded = base64::engine::general_purpose::STANDARD
        .decode(plaintext)
        .unwrap();
    assert_eq!(decoded.len(), 32);
}

// ── missing params / unknown key error branches ──

#[test]
fn encrypt_missing_key_id_errors() {
    let svc = make_service();
    let req = make_request("Encrypt", json!({"Plaintext": "aGVsbG8="}));
    assert!(svc.encrypt(&req).is_err());
}

#[test]
fn encrypt_unknown_key_errors() {
    let svc = make_service();
    let req = make_request(
        "Encrypt",
        json!({"KeyId": "00000000-0000-0000-0000-000000000000", "Plaintext": "aGVsbG8="}),
    );
    assert!(svc.encrypt(&req).is_err());
}

#[test]
fn decrypt_invalid_ciphertext_errors() {
    let svc = make_service();
    let req = make_request("Decrypt", json!({"CiphertextBlob": "not-base64!!!"}));
    assert!(svc.decrypt(&req).is_err());
}

#[test]
fn generate_data_key_missing_key_errors() {
    let svc = make_service();
    let req = make_request("GenerateDataKey", json!({"KeySpec": "AES_256"}));
    assert!(svc.generate_data_key(&req).is_err());
}

#[test]
fn generate_data_key_without_plaintext_missing_key_errors() {
    let svc = make_service();
    let req = make_request(
        "GenerateDataKeyWithoutPlaintext",
        json!({"KeySpec": "AES_256"}),
    );
    assert!(svc.generate_data_key_without_plaintext(&req).is_err());
}

#[test]
fn generate_random_too_many_bytes_errors() {
    let svc = make_service();
    let req = make_request("GenerateRandom", json!({"NumberOfBytes": 2048}));
    assert!(svc.generate_random(&req).is_err());
}

#[test]
fn generate_random_zero_bytes_errors() {
    let svc = make_service();
    let req = make_request("GenerateRandom", json!({"NumberOfBytes": 0}));
    assert!(svc.generate_random(&req).is_err());
}

#[test]
fn schedule_key_deletion_missing_key_errors() {
    let svc = make_service();
    let req = make_request("ScheduleKeyDeletion", json!({}));
    assert!(svc.schedule_key_deletion(&req).is_err());
}

#[test]
fn cancel_key_deletion_unknown_key_errors() {
    let svc = make_service();
    let req = make_request(
        "CancelKeyDeletion",
        json!({"KeyId": "00000000-0000-0000-0000-000000000000"}),
    );
    assert!(svc.cancel_key_deletion(&req).is_err());
}

#[test]
fn tag_resource_unknown_key_errors() {
    let svc = make_service();
    let req = make_request(
        "TagResource",
        json!({
            "KeyId": "00000000-0000-0000-0000-000000000000",
            "Tags": [{"TagKey": "k", "TagValue": "v"}]
        }),
    );
    assert!(svc.tag_resource(&req).is_err());
}

#[test]
fn untag_resource_unknown_key_errors() {
    let svc = make_service();
    let req = make_request(
        "UntagResource",
        json!({
            "KeyId": "00000000-0000-0000-0000-000000000000",
            "TagKeys": ["k"]
        }),
    );
    assert!(svc.untag_resource(&req).is_err());
}

#[test]
fn get_key_policy_unknown_key_errors() {
    let svc = make_service();
    let req = make_request(
        "GetKeyPolicy",
        json!({"KeyId": "00000000-0000-0000-0000-000000000000"}),
    );
    assert!(svc.get_key_policy(&req).is_err());
}

#[test]
fn put_key_policy_unknown_key_errors() {
    let svc = make_service();
    let req = make_request(
        "PutKeyPolicy",
        json!({
            "KeyId": "00000000-0000-0000-0000-000000000000",
            "Policy": "{}"
        }),
    );
    assert!(svc.put_key_policy(&req).is_err());
}

#[test]
fn sign_missing_message_errors() {
    let svc = make_service();
    let req = make_request("Sign", json!({"KeyId": "00000000"}));
    assert!(svc.sign(&req).is_err());
}

#[test]
fn verify_missing_signature_errors() {
    let svc = make_service();
    let req = make_request(
        "Verify",
        json!({"KeyId": "00000000", "Message": "aGVsbG8="}),
    );
    assert!(svc.verify(&req).is_err());
}

#[test]
fn rotate_key_on_demand_missing_key_errors() {
    let svc = make_service();
    let req = make_request("RotateKeyOnDemand", json!({}));
    assert!(svc.rotate_key_on_demand(&req).is_err());
}

#[test]
fn generate_mac_missing_message_errors() {
    let svc = make_service();
    let req = make_request(
        "GenerateMac",
        json!({"KeyId": "x", "MacAlgorithm": "HMAC_SHA_256"}),
    );
    assert!(svc.generate_mac(&req).is_err());
}

#[test]
fn verify_mac_missing_message_errors() {
    let svc = make_service();
    let req = make_request(
        "VerifyMac",
        json!({"KeyId": "x", "MacAlgorithm": "HMAC_SHA_256", "Mac": "abc"}),
    );
    assert!(svc.verify_mac(&req).is_err());
}

#[test]
fn replicate_key_missing_key_id_errors() {
    let svc = make_service();
    let req = make_request("ReplicateKey", json!({"ReplicaRegion": "eu-west-1"}));
    assert!(svc.replicate_key(&req).is_err());
}

#[test]
fn replicate_key_unknown_key_errors() {
    let svc = make_service();
    let req = make_request(
        "ReplicateKey",
        json!({
            "KeyId": "00000000-0000-0000-0000-000000000000",
            "ReplicaRegion": "eu-west-1"
        }),
    );
    assert!(svc.replicate_key(&req).is_err());
}

#[test]
fn derive_shared_secret_missing_key_errors() {
    let svc = make_service();
    let req = make_request("DeriveSharedSecret", json!({}));
    assert!(svc.derive_shared_secret(&req).is_err());
}

#[test]
fn generate_data_key_pair_missing_key_errors() {
    let svc = make_service();
    let req = make_request("GenerateDataKeyPair", json!({"KeyPairSpec": "RSA_2048"}));
    assert!(svc.generate_data_key_pair(&req).is_err());
}

#[test]
fn generate_data_key_pair_without_plaintext_missing_key_errors() {
    let svc = make_service();
    let req = make_request(
        "GenerateDataKeyPairWithoutPlaintext",
        json!({"KeyPairSpec": "RSA_2048"}),
    );
    assert!(svc.generate_data_key_pair_without_plaintext(&req).is_err());
}

#[test]
fn import_key_material_missing_key_errors() {
    let svc = make_service();
    let req = make_request("ImportKeyMaterial", json!({}));
    assert!(svc.import_key_material(&req).is_err());
}

#[test]
fn describe_key_returns_metadata() {
    let svc = make_service();
    let key_id = create_key(&svc);
    let req = make_request("DescribeKey", json!({"KeyId": key_id}));
    let resp = svc.describe_key(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert!(body["KeyMetadata"].is_object());
}

#[test]
fn enable_key_unknown_errors() {
    let svc = make_service();
    let req = make_request(
        "EnableKey",
        json!({"KeyId": "00000000-0000-0000-0000-000000000000"}),
    );
    assert!(svc.enable_key(&req).is_err());
}

#[test]
fn disable_key_unknown_errors() {
    let svc = make_service();
    let req = make_request(
        "DisableKey",
        json!({"KeyId": "00000000-0000-0000-0000-000000000000"}),
    );
    assert!(svc.disable_key(&req).is_err());
}

#[test]
fn enable_key_rotation_accepts_alias() {
    let svc = make_service();
    let key_id = create_key(&svc);

    let req = make_request(
        "CreateAlias",
        json!({
            "AliasName": "alias/my-key",
            "TargetKeyId": key_id,
        }),
    );
    svc.create_alias(&req).unwrap();

    let req = make_request("EnableKeyRotation", json!({ "KeyId": "alias/my-key" }));
    svc.enable_key_rotation(&req).unwrap();

    let req = make_request("GetKeyRotationStatus", json!({ "KeyId": "alias/my-key" }));
    let resp = svc.get_key_rotation_status(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert!(body["KeyRotationEnabled"].as_bool().unwrap());
}

#[test]
fn enable_key_rotation_persists_rotation_period() {
    // RotationPeriodInDays was dropped (bug-audit 2026-06-20, 1.24): it must
    // round-trip through GetKeyRotationStatus, and default to 365.
    let svc = make_service();
    let key_id = create_key(&svc);

    // Default cadence is 365 when omitted.
    svc.enable_key_rotation(&make_request(
        "EnableKeyRotation",
        json!({ "KeyId": key_id }),
    ))
    .unwrap();
    let resp = svc
        .get_key_rotation_status(&make_request(
            "GetKeyRotationStatus",
            json!({ "KeyId": key_id }),
        ))
        .unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert!(body["KeyRotationEnabled"].as_bool().unwrap());
    assert_eq!(body["RotationPeriodInDays"].as_i64(), Some(365));

    // A custom cadence is stored and echoed.
    svc.enable_key_rotation(&make_request(
        "EnableKeyRotation",
        json!({ "KeyId": key_id, "RotationPeriodInDays": 120 }),
    ))
    .unwrap();
    let resp = svc
        .get_key_rotation_status(&make_request(
            "GetKeyRotationStatus",
            json!({ "KeyId": key_id }),
        ))
        .unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["RotationPeriodInDays"].as_i64(), Some(120));

    // Out-of-range cadence is rejected.
    assert!(svc
        .enable_key_rotation(&make_request(
            "EnableKeyRotation",
            json!({ "KeyId": key_id, "RotationPeriodInDays": 5 }),
        ))
        .is_err());
}

#[test]
fn enable_disable_key_rotation_unknown_errors() {
    let svc = make_service();
    let req = make_request(
        "EnableKeyRotation",
        json!({"KeyId": "00000000-0000-0000-0000-000000000000"}),
    );
    assert!(svc.enable_key_rotation(&req).is_err());

    let req = make_request(
        "DisableKeyRotation",
        json!({"KeyId": "00000000-0000-0000-0000-000000000000"}),
    );
    assert!(svc.disable_key_rotation(&req).is_err());
}

#[test]
fn get_key_rotation_status_unknown_errors() {
    let svc = make_service();
    let req = make_request(
        "GetKeyRotationStatus",
        json!({"KeyId": "00000000-0000-0000-0000-000000000000"}),
    );
    assert!(svc.get_key_rotation_status(&req).is_err());
}

#[test]
fn update_key_description_unknown_errors() {
    let svc = make_service();
    let req = make_request(
        "UpdateKeyDescription",
        json!({
            "KeyId": "00000000-0000-0000-0000-000000000000",
            "Description": "new"
        }),
    );
    assert!(svc.update_key_description(&req).is_err());
}

#[test]
fn list_resource_tags_unknown_errors() {
    let svc = make_service();
    let req = make_request(
        "ListResourceTags",
        json!({"KeyId": "00000000-0000-0000-0000-000000000000"}),
    );
    assert!(svc.list_resource_tags(&req).is_err());
}

#[test]
fn list_aliases_empty_returns_ok() {
    let svc = make_service();
    let req = make_request("ListAliases", json!({}));
    let resp = svc.list_aliases(&req).unwrap();
    assert_eq!(resp.status, http::StatusCode::OK);
}

#[test]
fn create_alias_missing_name_errors() {
    let svc = make_service();
    let req = make_request("CreateAlias", json!({"TargetKeyId": "k"}));
    assert!(svc.create_alias(&req).is_err());
}

#[test]
fn delete_alias_not_found() {
    let svc = make_service();
    let req = make_request("DeleteAlias", json!({"AliasName": "alias/ghost"}));
    assert!(svc.delete_alias(&req).is_err());
}

#[test]
fn create_grant_unknown_key_errors() {
    let svc = make_service();
    let req = make_request(
        "CreateGrant",
        json!({
            "KeyId": "00000000-0000-0000-0000-000000000000",
            "GranteePrincipal": "arn:aws:iam::123:role/r",
            "Operations": ["Encrypt"]
        }),
    );
    assert!(svc.create_grant(&req).is_err());
}

#[test]
fn revoke_grant_unknown_errors() {
    let svc = make_service();
    let req = make_request(
        "RevokeGrant",
        json!({
            "KeyId": "00000000-0000-0000-0000-000000000000",
            "GrantId": "ghost"
        }),
    );
    assert!(svc.revoke_grant(&req).is_err());
}

#[test]
fn tag_resource_on_known_key_succeeds() {
    let svc = make_service();
    let key_id = create_key(&svc);
    let req = make_request(
        "TagResource",
        json!({
            "KeyId": key_id,
            "Tags": [{"TagKey": "env", "TagValue": "prod"}]
        }),
    );
    svc.tag_resource(&req).unwrap();
}

// ── G5: key_state enforcement on crypto ops ──

fn force_key_state(svc: &KmsService, key_id: &str, new_state: &str) {
    let mut accounts = svc.state.write();
    let s = accounts.get_or_create("123456789012");
    if let Some(key) = s.keys.get_mut(key_id) {
        key.key_state = new_state.to_string();
        if new_state == "Disabled" {
            key.enabled = false;
        }
    }
}

fn b64(s: &str) -> String {
    use base64::Engine;
    base64::engine::general_purpose::STANDARD.encode(s.as_bytes())
}

#[test]
fn encrypt_rejects_pending_deletion_with_invalid_state() {
    let svc = make_service();
    let key_id = create_key(&svc);
    force_key_state(&svc, &key_id, "PendingDeletion");
    let req = make_request(
        "Encrypt",
        json!({"KeyId": key_id, "Plaintext": b64("hello")}),
    );
    let err = match svc.encrypt(&req) {
        Err(e) => e,
        Ok(_) => panic!("expected KMSInvalidStateException"),
    };
    let msg = format!("{:?}", err);
    assert!(msg.contains("KMSInvalidStateException"), "got: {msg}");
}

#[test]
fn encrypt_disabled_still_returns_disabled_exception() {
    let svc = make_service();
    let key_id = create_key(&svc);
    force_key_state(&svc, &key_id, "Disabled");
    let req = make_request(
        "Encrypt",
        json!({"KeyId": key_id, "Plaintext": b64("hello")}),
    );
    let err = match svc.encrypt(&req) {
        Err(e) => e,
        Ok(_) => panic!("expected DisabledException"),
    };
    let msg = format!("{:?}", err);
    assert!(msg.contains("DisabledException"), "got: {msg}");
}

#[test]
fn generate_data_key_rejects_unavailable_state() {
    let svc = make_service();
    let key_id = create_key(&svc);
    force_key_state(&svc, &key_id, "Unavailable");
    let req = make_request(
        "GenerateDataKey",
        json!({"KeyId": key_id, "KeySpec": "AES_256"}),
    );
    let err = match svc.generate_data_key(&req) {
        Err(e) => e,
        Ok(_) => panic!("expected KMSInvalidStateException"),
    };
    let msg = format!("{:?}", err);
    assert!(msg.contains("KMSInvalidStateException"), "got: {msg}");
}

#[test]
fn decrypt_rejects_when_source_key_is_pending_deletion() {
    let svc = make_service();
    let key_id = create_key(&svc);
    let enc_req = make_request(
        "Encrypt",
        json!({"KeyId": key_id, "Plaintext": b64("hello")}),
    );
    let enc_resp = svc.encrypt(&enc_req).unwrap();
    let enc_body: Value = serde_json::from_slice(enc_resp.body.expect_bytes()).unwrap();
    let ct = enc_body["CiphertextBlob"].as_str().unwrap().to_string();

    force_key_state(&svc, &key_id, "PendingDeletion");
    let dec_req = make_request("Decrypt", json!({"CiphertextBlob": ct}));
    let err = match svc.decrypt(&dec_req) {
        Err(e) => e,
        Ok(_) => panic!("expected KMSInvalidStateException"),
    };
    let msg = format!("{:?}", err);
    assert!(msg.contains("KMSInvalidStateException"), "got: {msg}");
}

// ── G7: ImportKeyMaterial RSA-OAEP unwrap ──

#[test]
fn import_key_material_rejects_unknown_token() {
    let svc = make_service();
    let key_id = create_key_with_opts(&svc, json!({ "Origin": "EXTERNAL" }));
    let bogus_token = base64::engine::general_purpose::STANDARD.encode(b"never-issued");
    let bogus_material = base64::engine::general_purpose::STANDARD.encode(b"raw-bytes");
    let err = match svc.import_key_material(&make_request(
        "ImportKeyMaterial",
        json!({
            "KeyId": key_id,
            "ImportToken": bogus_token,
            "EncryptedKeyMaterial": bogus_material,
        }),
    )) {
        Err(e) => e,
        Ok(_) => panic!("expected InvalidImportTokenException"),
    };
    assert!(format!("{:?}", err).contains("InvalidImportTokenException"));
}

#[test]
fn import_key_material_rejects_token_for_other_key() {
    use base64::Engine;
    use rsa::pkcs8::DecodePublicKey;

    let svc = make_service();
    let key_a = create_key_with_opts(&svc, json!({ "Origin": "EXTERNAL" }));
    let key_b = create_key_with_opts(&svc, json!({ "Origin": "EXTERNAL" }));

    // Token issued for key_a, but caller tries to use it for key_b.
    let resp = svc
        .get_parameters_for_import(&make_request(
            "GetParametersForImport",
            json!({"KeyId": key_a, "WrappingAlgorithm": "RSAES_OAEP_SHA_256"}),
        ))
        .unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let token = body["ImportToken"].as_str().unwrap().to_string();
    let pub_der = base64::engine::general_purpose::STANDARD
        .decode(body["PublicKey"].as_str().unwrap())
        .unwrap();
    let public = rsa::RsaPublicKey::from_public_key_der(&pub_der).unwrap();
    let mut rng = rand::thread_rng();
    let wrapped = public
        .encrypt(&mut rng, rsa::Oaep::new::<rsa::sha2::Sha256>(), b"material")
        .unwrap();
    let wrapped_b64 = base64::engine::general_purpose::STANDARD.encode(&wrapped);

    let err = match svc.import_key_material(&make_request(
        "ImportKeyMaterial",
        json!({
            "KeyId": key_b,
            "ImportToken": token,
            "EncryptedKeyMaterial": wrapped_b64,
            "WrappingAlgorithm": "RSAES_OAEP_SHA_256",
        }),
    )) {
        Err(e) => e,
        Ok(_) => panic!("expected InvalidImportTokenException for cross-key token"),
    };
    assert!(format!("{:?}", err).contains("InvalidImportTokenException"));
}

#[test]
fn import_key_material_token_is_single_use() {
    use base64::Engine;
    use rsa::pkcs8::DecodePublicKey;

    let svc = make_service();
    let key_id = create_key_with_opts(&svc, json!({ "Origin": "EXTERNAL" }));

    let resp = svc
        .get_parameters_for_import(&make_request(
            "GetParametersForImport",
            json!({"KeyId": key_id, "WrappingAlgorithm": "RSAES_OAEP_SHA_256"}),
        ))
        .unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let token = body["ImportToken"].as_str().unwrap().to_string();
    let pub_der = base64::engine::general_purpose::STANDARD
        .decode(body["PublicKey"].as_str().unwrap())
        .unwrap();
    let public = rsa::RsaPublicKey::from_public_key_der(&pub_der).unwrap();
    let mut rng = rand::thread_rng();
    let wrapped = public
        .encrypt(&mut rng, rsa::Oaep::new::<rsa::sha2::Sha256>(), b"material")
        .unwrap();
    let wrapped_b64 = base64::engine::general_purpose::STANDARD.encode(&wrapped);

    // First import succeeds.
    svc.import_key_material(&make_request(
        "ImportKeyMaterial",
        json!({
            "KeyId": key_id,
            "ImportToken": token.clone(),
            "EncryptedKeyMaterial": wrapped_b64.clone(),
            "WrappingAlgorithm": "RSAES_OAEP_SHA_256",
        }),
    ))
    .unwrap();

    // Replay with the same token must fail — the token was single-use.
    let err = match svc.import_key_material(&make_request(
        "ImportKeyMaterial",
        json!({
            "KeyId": key_id,
            "ImportToken": token,
            "EncryptedKeyMaterial": wrapped_b64,
            "WrappingAlgorithm": "RSAES_OAEP_SHA_256",
        }),
    )) {
        Err(e) => e,
        Ok(_) => panic!("expected InvalidImportTokenException on token replay"),
    };
    assert!(format!("{:?}", err).contains("InvalidImportTokenException"));
}

#[test]
fn import_key_material_rejects_garbage_ciphertext() {
    let svc = make_service();
    let key_id = create_key_with_opts(&svc, json!({ "Origin": "EXTERNAL" }));

    let resp = svc
        .get_parameters_for_import(&make_request(
            "GetParametersForImport",
            json!({"KeyId": key_id, "WrappingAlgorithm": "RSAES_OAEP_SHA_256"}),
        ))
        .unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let token = body["ImportToken"].as_str().unwrap().to_string();

    // Random 256-byte payload — won't OAEP-decrypt under any key.
    let garbage = vec![0xab_u8; 256];
    let garbage_b64 = base64::engine::general_purpose::STANDARD.encode(&garbage);

    let err = match svc.import_key_material(&make_request(
        "ImportKeyMaterial",
        json!({
            "KeyId": key_id,
            "ImportToken": token,
            "EncryptedKeyMaterial": garbage_b64,
            "WrappingAlgorithm": "RSAES_OAEP_SHA_256",
        }),
    )) {
        Err(e) => e,
        Ok(_) => panic!("expected InvalidCiphertextException"),
    };
    assert!(format!("{:?}", err).contains("InvalidCiphertextException"));
}

#[test]
fn encrypt_decrypt_round_trip_with_matching_encryption_context() {
    use base64::Engine;
    let svc = make_service();
    let key_id = create_key(&svc);
    let plaintext = base64::engine::general_purpose::STANDARD.encode(b"hello-world");

    let enc = svc
        .encrypt(&make_request(
            "Encrypt",
            json!({
                "KeyId": key_id,
                "Plaintext": plaintext,
                "EncryptionContext": {"app": "prod", "owner": "platform"}
            }),
        ))
        .unwrap();
    let enc_body: Value = serde_json::from_slice(enc.body.expect_bytes()).unwrap();
    let ciphertext = enc_body["CiphertextBlob"].as_str().unwrap().to_string();

    let dec = svc
        .decrypt(&make_request(
            "Decrypt",
            json!({
                "CiphertextBlob": ciphertext,
                // Same EC, different key order — canonicalization sorts.
                "EncryptionContext": {"owner": "platform", "app": "prod"}
            }),
        ))
        .unwrap();
    let dec_body: Value = serde_json::from_slice(dec.body.expect_bytes()).unwrap();
    assert_eq!(dec_body["Plaintext"].as_str().unwrap(), plaintext);
}

#[test]
fn decrypt_rejects_mismatched_encryption_context() {
    use base64::Engine;
    let svc = make_service();
    let key_id = create_key(&svc);
    let plaintext = base64::engine::general_purpose::STANDARD.encode(b"hello-world");

    let enc = svc
        .encrypt(&make_request(
            "Encrypt",
            json!({
                "KeyId": key_id,
                "Plaintext": plaintext,
                "EncryptionContext": {"app": "prod"}
            }),
        ))
        .unwrap();
    let enc_body: Value = serde_json::from_slice(enc.body.expect_bytes()).unwrap();
    let ciphertext = enc_body["CiphertextBlob"].as_str().unwrap().to_string();

    // Different EC at decrypt — must surface InvalidCiphertextException.
    let err = svc
        .decrypt(&make_request(
            "Decrypt",
            json!({
                "CiphertextBlob": &ciphertext,
                "EncryptionContext": {"app": "staging"}
            }),
        ))
        .err()
        .expect("EC mismatch must error");
    assert!(format!("{err:?}").contains("InvalidCiphertextException"));

    // Missing EC at decrypt — same outcome.
    let err = svc
        .decrypt(&make_request(
            "Decrypt",
            json!({"CiphertextBlob": &ciphertext}),
        ))
        .err()
        .expect("missing EC must error when ciphertext was encrypted with one");
    assert!(format!("{err:?}").contains("InvalidCiphertextException"));
}

#[test]
fn re_encrypt_rejects_mismatched_source_encryption_context() {
    use base64::Engine;
    let svc = make_service();
    let src_key = create_key(&svc);
    let dst_key = create_key(&svc);
    let plaintext = base64::engine::general_purpose::STANDARD.encode(b"swap-me");

    let enc = svc
        .encrypt(&make_request(
            "Encrypt",
            json!({
                "KeyId": src_key,
                "Plaintext": plaintext,
                "EncryptionContext": {"src": "yes"}
            }),
        ))
        .unwrap();
    let enc_body: Value = serde_json::from_slice(enc.body.expect_bytes()).unwrap();
    let ciphertext = enc_body["CiphertextBlob"].as_str().unwrap().to_string();

    let err = svc
        .re_encrypt(&make_request(
            "ReEncrypt",
            json!({
                "CiphertextBlob": ciphertext,
                "DestinationKeyId": dst_key,
                // Wrong source EC.
                "SourceEncryptionContext": {"src": "no"}
            }),
        ))
        .err()
        .expect("source EC mismatch must abort ReEncrypt");
    assert!(format!("{err:?}").contains("InvalidCiphertextException"));
}

#[test]
fn decrypt_with_correct_key_id_succeeds() {
    let svc = make_service();
    let key_id = create_key(&svc);

    let plaintext = base64::engine::general_purpose::STANDARD.encode(b"top-secret");
    let enc = svc
        .encrypt(&make_request(
            "Encrypt",
            json!({ "KeyId": key_id, "Plaintext": plaintext }),
        ))
        .unwrap();
    let enc_body: Value = serde_json::from_slice(enc.body.expect_bytes()).unwrap();
    let ciphertext = enc_body["CiphertextBlob"].as_str().unwrap().to_string();

    // Passing the SAME KeyId that produced the blob must succeed.
    let dec = svc
        .decrypt(&make_request(
            "Decrypt",
            json!({ "CiphertextBlob": ciphertext, "KeyId": key_id }),
        ))
        .unwrap();
    let dec_body: Value = serde_json::from_slice(dec.body.expect_bytes()).unwrap();
    assert_eq!(dec_body["Plaintext"].as_str().unwrap(), plaintext);
}

#[test]
fn decrypt_with_wrong_key_id_returns_incorrect_key_exception() {
    let svc = make_service();
    let producing_key = create_key(&svc);
    let other_key = create_key(&svc);

    let plaintext = base64::engine::general_purpose::STANDARD.encode(b"top-secret");
    let enc = svc
        .encrypt(&make_request(
            "Encrypt",
            json!({ "KeyId": producing_key, "Plaintext": plaintext }),
        ))
        .unwrap();
    let enc_body: Value = serde_json::from_slice(enc.body.expect_bytes()).unwrap();
    let ciphertext = enc_body["CiphertextBlob"].as_str().unwrap().to_string();

    // Supplying a DIFFERENT KeyId must be rejected, not silently decrypted.
    let err = svc
        .decrypt(&make_request(
            "Decrypt",
            json!({ "CiphertextBlob": ciphertext, "KeyId": other_key }),
        ))
        .err()
        .expect("wrong KeyId must be rejected");
    assert_eq!(err.code(), "IncorrectKeyException");
}

#[test]
fn decrypt_without_key_id_still_succeeds() {
    let svc = make_service();
    let key_id = create_key(&svc);

    let plaintext = base64::engine::general_purpose::STANDARD.encode(b"hi");
    let enc = svc
        .encrypt(&make_request(
            "Encrypt",
            json!({ "KeyId": key_id, "Plaintext": plaintext }),
        ))
        .unwrap();
    let enc_body: Value = serde_json::from_slice(enc.body.expect_bytes()).unwrap();
    let ciphertext = enc_body["CiphertextBlob"].as_str().unwrap().to_string();

    // KeyId is optional for symmetric decrypt; omitting it works.
    let dec = svc
        .decrypt(&make_request(
            "Decrypt",
            json!({ "CiphertextBlob": ciphertext }),
        ))
        .unwrap();
    let dec_body: Value = serde_json::from_slice(dec.body.expect_bytes()).unwrap();
    assert_eq!(dec_body["Plaintext"].as_str().unwrap(), plaintext);
}

/// No snapshot store (memory mode) -> no persist hook for the CFN provisioner.
#[test]
fn snapshot_hook_is_none_without_store() {
    let svc = make_service();
    assert!(svc.snapshot_hook().is_none());
}

/// With a store, the hook is present and invoking it runs the whole-state
/// persist path the CloudFormation provisioner uses after mutating KMS state
/// directly.
#[tokio::test]
async fn snapshot_hook_fires_with_store() {
    let store: Arc<dyn fakecloud_persistence::SnapshotStore> =
        Arc::new(fakecloud_persistence::MemorySnapshotStore::new());
    let svc = make_service().with_snapshot_store(store);
    let hook = svc
        .snapshot_hook()
        .expect("hook present when a store is set");
    // Must not panic; exercises the closure and the snapshot save path.
    hook().await;
}

#[test]
fn encrypt_rejects_non_encrypt_decrypt_key() {
    // A SIGN_VERIFY key must not encrypt -- the old code ignored key_usage and
    // encrypted symmetrically regardless (bug-audit 2026-06-20, 1.11).
    let svc = make_service();
    let key_id = create_key_with_opts(
        &svc,
        json!({ "KeyUsage": "SIGN_VERIFY", "KeySpec": "RSA_2048" }),
    );
    let req = make_request(
        "Encrypt",
        json!({
            "KeyId": key_id,
            "Plaintext": base64::engine::general_purpose::STANDARD.encode(b"x"),
        }),
    );
    match svc.encrypt(&req) {
        Err(e) => assert_eq!(e.code(), "InvalidKeyUsageException"),
        Ok(_) => panic!("a SIGN_VERIFY key must not encrypt"),
    }
}

#[test]
fn encrypt_symmetric_rejects_asymmetric_algorithm() {
    // A symmetric key only supports SYMMETRIC_DEFAULT; the old code ignored the
    // requested EncryptionAlgorithm entirely (1.11).
    let svc = make_service();
    let key_id = create_key(&svc);
    let req = make_request(
        "Encrypt",
        json!({
            "KeyId": key_id,
            "Plaintext": base64::engine::general_purpose::STANDARD.encode(b"x"),
            "EncryptionAlgorithm": "RSAES_OAEP_SHA_256",
        }),
    );
    match svc.encrypt(&req) {
        Err(e) => assert_eq!(e.code(), "InvalidKeyUsageException"),
        Ok(_) => panic!("a symmetric key must reject an RSA algorithm"),
    }
}

#[test]
fn encrypt_rsa_key_uses_real_oaep_and_roundtrips() {
    // An RSA ENCRYPT_DECRYPT key encrypts with real RSA-OAEP under its public
    // half (not the symmetric AES blob), echoes the requested algorithm, and
    // Decrypt recovers the plaintext via the private half (1.11).
    let svc = make_service();
    let key_id = create_key_with_opts(
        &svc,
        json!({ "KeyUsage": "ENCRYPT_DECRYPT", "KeySpec": "RSA_2048" }),
    );
    let plaintext = b"asymmetric hello";
    let pt_b64 = base64::engine::general_purpose::STANDARD.encode(plaintext);

    let resp = svc
        .encrypt(&make_request(
            "Encrypt",
            json!({
                "KeyId": key_id,
                "Plaintext": pt_b64,
                "EncryptionAlgorithm": "RSAES_OAEP_SHA_256",
            }),
        ))
        .unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["EncryptionAlgorithm"], "RSAES_OAEP_SHA_256");
    let ciphertext = body["CiphertextBlob"].as_str().unwrap().to_string();
    let env = String::from_utf8(
        base64::engine::general_purpose::STANDARD
            .decode(&ciphertext)
            .unwrap(),
    )
    .unwrap();
    assert!(
        env.starts_with("fakecloud-rsa:"),
        "RSA key must use the RSA envelope, got {env}"
    );

    let resp = svc
        .decrypt(&make_request(
            "Decrypt",
            json!({ "CiphertextBlob": ciphertext }),
        ))
        .unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["EncryptionAlgorithm"], "RSAES_OAEP_SHA_256");
    let decrypted = base64::engine::general_purpose::STANDARD
        .decode(body["Plaintext"].as_str().unwrap())
        .unwrap();
    assert_eq!(decrypted, plaintext);
}
