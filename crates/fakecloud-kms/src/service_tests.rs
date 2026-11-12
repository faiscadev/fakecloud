use super::*;
use fakecloud_core::delivery::KmsCaller;
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

#[test]
fn key_and_alias_arns_use_request_region_not_server_default() {
    // Server default region is us-east-1 (see make_service); a client scoped
    // to eu-central-1 must get eu-central-1 key + alias ARNs.
    let svc = make_service();

    let mut create = make_request("CreateKey", json!({}));
    create.region = "eu-central-1".to_string();
    let resp = svc.create_key(&create).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let key_arn = body["KeyMetadata"]["Arn"].as_str().unwrap();
    let key_id = body["KeyMetadata"]["KeyId"].as_str().unwrap().to_string();
    assert!(
        key_arn.starts_with("arn:aws:kms:eu-central-1:123456789012:key/"),
        "key ARN must carry the request region, got {key_arn}"
    );

    let mut alias = make_request(
        "CreateAlias",
        json!({ "AliasName": "alias/region-test", "TargetKeyId": key_id }),
    );
    alias.region = "eu-central-1".to_string();
    svc.create_alias(&alias).unwrap();

    let mut list = make_request("ListAliases", json!({}));
    list.region = "eu-central-1".to_string();
    let resp = svc.list_aliases(&list).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let alias_arn = body["Aliases"]
        .as_array()
        .unwrap()
        .iter()
        .find(|a| a["AliasName"].as_str() == Some("alias/region-test"))
        .expect("created alias present")["AliasArn"]
        .as_str()
        .unwrap();
    assert_eq!(
        alias_arn,
        "arn:aws:kms:eu-central-1:123456789012:alias/region-test"
    );
}

#[test]
fn china_region_key_alias_policy_and_grant_use_the_aws_cn_partition() {
    let svc = make_service();
    let in_cn = |action: &str, body: Value| {
        let mut r = make_request(action, body);
        r.region = "cn-north-1".to_string();
        r
    };
    let json_of =
        |resp: AwsResponse| -> Value { serde_json::from_slice(resp.body.expect_bytes()).unwrap() };

    let created = json_of(svc.create_key(&in_cn("CreateKey", json!({}))).unwrap());
    let key_arn = created["KeyMetadata"]["Arn"].as_str().unwrap().to_string();
    let key_id = created["KeyMetadata"]["KeyId"]
        .as_str()
        .unwrap()
        .to_string();
    assert_eq!(
        key_arn,
        format!("arn:aws-cn:kms:cn-north-1:123456789012:key/{key_id}")
    );

    // The key is found by its own ARN.
    let described = json_of(
        svc.describe_key(&in_cn("DescribeKey", json!({ "KeyId": key_arn })))
            .unwrap(),
    );
    assert_eq!(described["KeyMetadata"]["KeyId"], key_id);

    let policy = json_of(
        svc.get_key_policy(&in_cn(
            "GetKeyPolicy",
            json!({ "KeyId": key_id, "PolicyName": "default" }),
        ))
        .unwrap(),
    );
    let policy: Value = serde_json::from_str(policy["Policy"].as_str().unwrap()).unwrap();
    assert_eq!(
        policy["Statement"][0]["Principal"]["AWS"],
        "arn:aws-cn:iam::123456789012:root"
    );

    svc.create_alias(&in_cn(
        "CreateAlias",
        json!({ "AliasName": "alias/cn", "TargetKeyId": key_arn }),
    ))
    .unwrap();
    let aliases = json_of(svc.list_aliases(&in_cn("ListAliases", json!({}))).unwrap());
    let alias_arn = aliases["Aliases"]
        .as_array()
        .unwrap()
        .iter()
        .find(|a| a["AliasName"] == "alias/cn")
        .unwrap()["AliasArn"]
        .as_str()
        .unwrap()
        .to_string();
    assert_eq!(alias_arn, "arn:aws-cn:kms:cn-north-1:123456789012:alias/cn");
    let by_alias = json_of(
        svc.describe_key(&in_cn("DescribeKey", json!({ "KeyId": alias_arn })))
            .unwrap(),
    );
    assert_eq!(by_alias["KeyMetadata"]["KeyId"], key_id);

    svc.create_grant(&in_cn(
        "CreateGrant",
        json!({
            "KeyId": key_arn,
            "GranteePrincipal": "arn:aws-cn:iam::123456789012:role/r",
            "Operations": ["Decrypt"]
        }),
    ))
    .unwrap();
    let grants = json_of(
        svc.list_grants(&in_cn("ListGrants", json!({ "KeyId": key_arn })))
            .unwrap(),
    );
    assert_eq!(
        grants["Grants"][0]["IssuingAccount"],
        "arn:aws-cn:iam::123456789012:root"
    );

    let policy_provider = crate::resource_policy::KmsResourcePolicyProvider::new(svc.state.clone());
    use fakecloud_core::auth::ResourcePolicyProvider;
    assert!(policy_provider.resource_policy("kms", &key_arn).is_some());
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

/// A fresh, valid P-256 SubjectPublicKeyInfo (base64), for use as a
/// DeriveSharedSecret counterparty key. Real ECDH requires a parseable SPKI,
/// so tests can no longer pass arbitrary bytes.
fn real_p256_public_key_b64() -> String {
    let (_priv_der, pub_der) = crate::service::asym_ecdsa::generate_keypair("ECC_NIST_P256")
        .unwrap()
        .unwrap();
    base64::engine::general_purpose::STANDARD.encode(pub_der)
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

    let fake_pub = real_p256_public_key_b64();
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

    let pub_key = real_p256_public_key_b64();
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
    let other_pub = real_p256_public_key_b64();
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

    // The ciphertext is a symmetric blob naming its key, like any other
    // symmetric key's, and never holds the plaintext.
    let ct_bytes = base64::engine::general_purpose::STANDARD
        .decode(&ciphertext)
        .unwrap();
    assert_eq!(
        crate::blob::embedded_key_id(&ct_bytes),
        Some(format!("arn:aws:kms:us-east-1:123456789012:key/{key_id}"))
    );
    assert!(ct_bytes.windows(plaintext.len()).all(|w| w != plaintext));

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
    import_external_material(&svc, &key_id, b"my-secret-aes-key-material!12345");

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
        let alias = state.alias("us-east-1", "alias/switchable").unwrap();
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
        let alias = state.alias("us-east-1", "alias/switchable").unwrap();
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
fn create_key_accepts_ml_dsa_post_quantum_spec() {
    // ML-DSA (post-quantum) signing keys must be accepted at CreateKey and
    // echoed back verbatim by DescribeKey, even though the backend does not
    // implement real ML-DSA signing crypto.
    let svc = make_service();
    let key_id = create_key_with_opts(
        &svc,
        json!({
            "KeyUsage": "SIGN_VERIFY",
            "KeySpec": "ML_DSA_65"
        }),
    );

    let resp = svc
        .describe_key(&make_request("DescribeKey", json!({ "KeyId": &key_id })))
        .unwrap();
    let body = body_json(resp);
    assert_eq!(body["KeyMetadata"]["KeySpec"], json!("ML_DSA_65"));
    assert_eq!(body["KeyMetadata"]["KeyUsage"], json!("SIGN_VERIFY"));
    assert_eq!(
        body["KeyMetadata"]["SigningAlgorithms"],
        json!(["ML_DSA_SHAKE_256"])
    );
}

#[test]
fn create_key_rejects_ml_dsa_with_encrypt_decrypt() {
    // ML-DSA is signing-only; ENCRYPT_DECRYPT must still be rejected.
    let svc = make_service();
    let req = make_request(
        "CreateKey",
        json!({ "KeyUsage": "ENCRYPT_DECRYPT", "KeySpec": "ML_DSA_65" }),
    );
    let err = svc.create_key(&req).err().expect("expected error");
    assert_eq!(err.code(), "ValidationException");
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
    assert_eq!(err.code(), "InvalidArnException");
    assert!(
        format!("{err:?}").contains("Key Aliases are not supported for this operation."),
        "{err:?}"
    );
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
        .encrypt(
            &mut rng,
            rsa::Oaep::new::<rsa::sha2::Sha256>(),
            b"my-secret-aes-key-material!12345",
        )
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
    // The raw RSA-OAEP output, as AWS returns it: one modulus long, and
    // opened by the private half outside KMS too.
    let raw = base64::engine::general_purpose::STANDARD
        .decode(&ciphertext)
        .unwrap();
    assert_eq!(raw.len(), 256);
    {
        let accounts = svc.state.read();
        let key = &accounts.get("123456789012").unwrap().keys[&key_id];
        let priv_der = key.asymmetric_private_key_der.as_ref().unwrap();
        assert_eq!(
            asym::rsa_oaep_unwrap(priv_der, "RSAES_OAEP_SHA_256", &raw).unwrap(),
            plaintext
        );
    }

    // AWS requires the caller to name the key and supply EncryptionAlgorithm
    // when decrypting ciphertext produced by an asymmetric KMS key.
    let resp = svc
        .decrypt(&make_request(
            "Decrypt",
            json!({
                "CiphertextBlob": ciphertext,
                "KeyId": key_id,
                "EncryptionAlgorithm": "RSAES_OAEP_SHA_256",
            }),
        ))
        .unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["EncryptionAlgorithm"], "RSAES_OAEP_SHA_256");
    let decrypted = base64::engine::general_purpose::STANDARD
        .decode(body["Plaintext"].as_str().unwrap())
        .unwrap();
    assert_eq!(decrypted, plaintext);
}

#[test]
fn asymmetric_decrypt_wrong_algorithm_is_rejected() {
    // Ciphertext produced by an RSA ENCRYPT_DECRYPT key records the algorithm
    // used (RSAES_OAEP_SHA_256). AWS rejects a Decrypt that supplies a
    // different EncryptionAlgorithm with InvalidCiphertextException, and
    // requires the algorithm to be present for asymmetric ciphertext (an
    // omitted algorithm defaults to SYMMETRIC_DEFAULT, which never matches).
    let svc = make_service();
    let key_id = create_key_with_opts(
        &svc,
        json!({ "KeyUsage": "ENCRYPT_DECRYPT", "KeySpec": "RSA_2048" }),
    );
    let pt_b64 = base64::engine::general_purpose::STANDARD.encode(b"asymmetric hello");
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
    let ciphertext = body["CiphertextBlob"].as_str().unwrap().to_string();

    // Wrong algorithm -> rejected.
    match svc.decrypt(&make_request(
        "Decrypt",
        json!({
            "CiphertextBlob": ciphertext.clone(),
            "KeyId": key_id,
            "EncryptionAlgorithm": "RSAES_OAEP_SHA_1",
        }),
    )) {
        Err(e) => assert_eq!(e.code(), "InvalidCiphertextException"),
        Ok(_) => panic!("a wrong EncryptionAlgorithm must be rejected"),
    }

    // Omitted algorithm (defaults to SYMMETRIC_DEFAULT) -> also rejected.
    match svc.decrypt(&make_request(
        "Decrypt",
        json!({ "CiphertextBlob": ciphertext.clone(), "KeyId": key_id }),
    )) {
        Err(e) => assert_eq!(e.code(), "InvalidCiphertextException"),
        Ok(_) => panic!("an omitted EncryptionAlgorithm must be rejected for asymmetric"),
    }

    // An RSA ciphertext names no key, so without KeyId it is not one KMS can
    // open, whatever the algorithm.
    match svc.decrypt(&make_request(
        "Decrypt",
        json!({
            "CiphertextBlob": ciphertext.clone(),
            "EncryptionAlgorithm": "RSAES_OAEP_SHA_256",
        }),
    )) {
        Err(e) => assert_eq!(e.code(), "InvalidCiphertextException"),
        Ok(_) => panic!("an RSA ciphertext needs its KeyId"),
    }

    // Naming a symmetric key instead: the bytes are no symmetric ciphertext.
    let symmetric = create_key_with_opts(&svc, json!({}));
    match svc.decrypt(&make_request(
        "Decrypt",
        json!({ "CiphertextBlob": ciphertext.clone(), "KeyId": symmetric }),
    )) {
        Err(e) => assert_eq!(e.code(), "InvalidCiphertextException"),
        Ok(_) => panic!("another key must not open the ciphertext"),
    }

    // Correct algorithm -> succeeds.
    let resp = svc
        .decrypt(&make_request(
            "Decrypt",
            json!({
                "CiphertextBlob": ciphertext,
                "KeyId": key_id,
                "EncryptionAlgorithm": "RSAES_OAEP_SHA_256",
            }),
        ))
        .unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let decrypted = base64::engine::general_purpose::STANDARD
        .decode(body["Plaintext"].as_str().unwrap())
        .unwrap();
    assert_eq!(decrypted, b"asymmetric hello");
}

/// Recorded (list-aliases-before.json / unmaterialized.txt): every account
/// lists the predefined AWS-managed aliases before their service first uses
/// them, with no `TargetKeyId`; an alias of another AWS service
/// (`alias/aws/sns`) is not listed until its key exists. Using the alias
/// directly (DescribeKey, Encrypt) creates the key, which the alias then
/// targets; a name AWS has no AWS-managed key for stays not found.
#[tokio::test]
async fn aws_managed_aliases_exist_from_first_use() {
    let svc = make_service();
    let list = |svc: &KmsService| {
        body_json(
            svc.list_aliases(&make_request("ListAliases", json!({})))
                .unwrap(),
        )["Aliases"]
            .as_array()
            .unwrap()
            .clone()
    };
    let aliases = list(&svc);
    let names: Vec<&str> = aliases
        .iter()
        .filter_map(|a| a["AliasName"].as_str())
        .collect();
    assert_eq!(
        names,
        [
            "alias/aws/dynamodb",
            "alias/aws/ebs",
            "alias/aws/elasticfilesystem",
            "alias/aws/es",
            "alias/aws/glue",
            "alias/aws/kinesisvideo",
            "alias/aws/rds",
            "alias/aws/redshift",
            "alias/aws/s3",
            "alias/aws/ssm",
            "alias/aws/xray",
        ]
    );
    for alias in &aliases {
        let mut fields: Vec<_> = alias.as_object().unwrap().keys().collect();
        fields.sort();
        assert_eq!(fields, ["AliasArn", "AliasName"], "{alias}");
    }
    assert!(svc
        .state
        .read()
        .get("123456789012")
        .is_none_or(|s| s.keys.is_empty()));

    // Encrypt on a listed alias creates its key, then refuses the call.
    let err = svc
        .handle(make_request(
            "Encrypt",
            json!({"KeyId": "alias/aws/dynamodb", "Plaintext": "aGVsbG8="}),
        ))
        .await
        .err()
        .unwrap();
    assert_eq!(err.code(), "AccessDeniedException");
    // DescribeKey on an unlisted alias of an AWS service creates it too.
    let sns = body_json(
        svc.handle(make_request(
            "DescribeKey",
            json!({"KeyId": "alias/aws/sns"}),
        ))
        .await
        .unwrap(),
    );
    assert_eq!(sns["KeyMetadata"]["KeyManager"], "AWS");
    assert_eq!(
        sns["KeyMetadata"]["Description"],
        "Default key that protects my SNS data when no other key is defined"
    );
    let aliases = list(&svc);
    let target = |name: &str| {
        aliases
            .iter()
            .find(|a| a["AliasName"] == name)
            .and_then(|a| a["TargetKeyId"].as_str())
            .map(str::to_string)
    };
    assert!(target("alias/aws/dynamodb").is_some());
    assert_eq!(
        target("alias/aws/sns").as_deref(),
        sns["KeyMetadata"]["KeyId"].as_str()
    );
    assert_eq!(target("alias/aws/rds"), None);

    for name in ["alias/aws/logs", "alias/aws/nosuchservice"] {
        for (action, body) in [
            ("DescribeKey", json!({"KeyId": name})),
            ("Encrypt", json!({"KeyId": name, "Plaintext": "aGVsbG8="})),
        ] {
            let err = svc.handle(make_request(action, body)).await.err().unwrap();
            assert_eq!(
                (err.code(), err.message()),
                (
                    "NotFoundException",
                    format!("Alias arn:aws:kms:us-east-1:123456789012:{name} is not found.")
                ),
                "{action} {name}"
            );
        }
    }
}

#[test]
fn create_external_origin_key_is_pending_import_and_disabled() {
    // A key created with Origin=EXTERNAL has no material yet, so AWS returns it
    // disabled in KeyState PendingImport until ImportKeyMaterial runs. The
    // Terraform aws_kms_external_key resource asserts enabled=false on create.
    let svc = make_service();
    let resp = svc
        .create_key(&make_request("CreateKey", json!({ "Origin": "EXTERNAL" })))
        .unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let md = &body["KeyMetadata"];
    assert_eq!(
        md["Enabled"],
        json!(false),
        "external key should be disabled"
    );
    assert_eq!(md["KeyState"], json!("PendingImport"));
    assert_eq!(md["Origin"], json!("EXTERNAL"));

    // A normal (AWS_KMS) key is still created Enabled.
    let resp = svc
        .create_key(&make_request("CreateKey", json!({})))
        .unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["KeyMetadata"]["Enabled"], json!(true));
    assert_eq!(body["KeyMetadata"]["KeyState"], json!("Enabled"));
}

#[test]
fn generate_data_key_without_plaintext_binds_encryption_context() {
    let svc = make_service();
    let key_id = create_key(&svc);

    let gen = svc
        .generate_data_key_without_plaintext(&make_request(
            "GenerateDataKeyWithoutPlaintext",
            json!({
                "KeyId": key_id,
                "KeySpec": "AES_256",
                "EncryptionContext": {"app": "prod"}
            }),
        ))
        .unwrap();
    let gen_body: Value = serde_json::from_slice(gen.body.expect_bytes()).unwrap();
    let ciphertext = gen_body["CiphertextBlob"].as_str().unwrap().to_string();

    // Matching context decrypts.
    svc.decrypt(&make_request(
        "Decrypt",
        json!({
            "CiphertextBlob": &ciphertext,
            "EncryptionContext": {"app": "prod"}
        }),
    ))
    .expect("matching EC must decrypt the WithoutPlaintext data key");

    // Wrong context fails.
    let err = svc
        .decrypt(&make_request(
            "Decrypt",
            json!({
                "CiphertextBlob": &ciphertext,
                "EncryptionContext": {"app": "staging"}
            }),
        ))
        .err()
        .expect("wrong EC must error");
    assert!(format!("{err:?}").contains("InvalidCiphertextException"));

    // Absent context fails (context must provide real protection).
    let err = svc
        .decrypt(&make_request(
            "Decrypt",
            json!({"CiphertextBlob": &ciphertext}),
        ))
        .err()
        .expect("missing EC must error");
    assert!(format!("{err:?}").contains("InvalidCiphertextException"));
}

#[test]
fn generate_data_key_pair_without_plaintext_binds_encryption_context() {
    let svc = make_service();
    let key_id = create_key(&svc);

    let gen = svc
        .generate_data_key_pair_without_plaintext(&make_request(
            "GenerateDataKeyPairWithoutPlaintext",
            json!({
                "KeyId": key_id,
                "KeyPairSpec": "RSA_2048",
                "EncryptionContext": {"app": "prod"}
            }),
        ))
        .unwrap();
    let gen_body: Value = serde_json::from_slice(gen.body.expect_bytes()).unwrap();
    let ciphertext = gen_body["PrivateKeyCiphertextBlob"]
        .as_str()
        .unwrap()
        .to_string();

    // Matching context decrypts.
    svc.decrypt(&make_request(
        "Decrypt",
        json!({
            "CiphertextBlob": &ciphertext,
            "EncryptionContext": {"app": "prod"}
        }),
    ))
    .expect("matching EC must decrypt the WithoutPlaintext private key");

    // Wrong context fails.
    let err = svc
        .decrypt(&make_request(
            "Decrypt",
            json!({
                "CiphertextBlob": &ciphertext,
                "EncryptionContext": {"app": "staging"}
            }),
        ))
        .err()
        .expect("wrong EC must error");
    assert!(format!("{err:?}").contains("InvalidCiphertextException"));

    // Absent context fails.
    let err = svc
        .decrypt(&make_request(
            "Decrypt",
            json!({"CiphertextBlob": &ciphertext}),
        ))
        .err()
        .expect("missing EC must error");
    assert!(format!("{err:?}").contains("InvalidCiphertextException"));
}

// ---- Hardening regression tests (kms hardening batch) ----

/// H2: empty/short imported key material must be rejected, not stored —
/// storing zero-length material later panics with a divide-by-zero in the
/// XOR encrypt/decrypt path (`byte ^ material[i % material.len()]`).
#[test]
fn import_key_material_rejects_wrong_length_no_panic() {
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
    // Zero-length material is the divide-by-zero vector.
    let wrapped = public
        .encrypt(&mut rng, rsa::Oaep::new::<rsa::sha2::Sha256>(), b"")
        .unwrap();
    let wrapped_b64 = base64::engine::general_purpose::STANDARD.encode(&wrapped);

    let err = svc
        .import_key_material(&make_request(
            "ImportKeyMaterial",
            json!({
                "KeyId": key_id,
                "ImportToken": token,
                "EncryptedKeyMaterial": wrapped_b64,
                "WrappingAlgorithm": "RSAES_OAEP_SHA_256",
            }),
        ))
        .err()
        .expect("empty imported material must be rejected");
    assert!(
        format!("{err:?}").contains("ValidationException"),
        "got {err:?}"
    );
}

/// M1: a key scheduled for deletion is `PendingDeletion` — crypto ops on it
/// return `KMSInvalidStateException`, not `DisabledException`.
#[test]
fn encrypt_on_pending_deletion_key_is_invalid_state() {
    use base64::Engine;
    let svc = make_service();
    let key_id = create_key(&svc);
    svc.schedule_key_deletion(&make_request(
        "ScheduleKeyDeletion",
        json!({"KeyId": key_id, "PendingWindowInDays": 7}),
    ))
    .unwrap();

    let err = svc
        .encrypt(&make_request(
            "Encrypt",
            json!({
                "KeyId": key_id,
                "Plaintext": base64::engine::general_purpose::STANDARD.encode(b"hi"),
            }),
        ))
        .err()
        .expect("encrypt on pending-deletion key must fail");
    assert!(
        format!("{err:?}").contains("KMSInvalidStateException"),
        "got {err:?}"
    );
}

/// M1 (companion): a `Disabled` key still returns `DisabledException`.
#[test]
fn encrypt_on_disabled_key_is_disabled_exception() {
    use base64::Engine;
    let svc = make_service();
    let key_id = create_key(&svc);
    svc.disable_key(&make_request("DisableKey", json!({"KeyId": key_id})))
        .unwrap();

    let err = svc
        .encrypt(&make_request(
            "Encrypt",
            json!({
                "KeyId": key_id,
                "Plaintext": base64::engine::general_purpose::STANDARD.encode(b"hi"),
            }),
        ))
        .err()
        .expect("encrypt on disabled key must fail");
    assert!(
        format!("{err:?}").contains("DisabledException"),
        "got {err:?}"
    );
}

/// M2: GenerateMac / VerifyMac reject a MacAlgorithm the key does not
/// advertise (a HMAC_256 key only supports HMAC_SHA_256).
#[test]
fn mac_rejects_algorithm_not_advertised_by_key() {
    use base64::Engine;
    let svc = make_service();
    let key_id = create_key_with_opts(
        &svc,
        json!({ "KeyUsage": "GENERATE_VERIFY_MAC", "KeySpec": "HMAC_256" }),
    );
    let msg_b64 = base64::engine::general_purpose::STANDARD.encode(b"payload");

    let err = svc
        .generate_mac(&make_request(
            "GenerateMac",
            json!({"KeyId": key_id, "Message": msg_b64, "MacAlgorithm": "HMAC_SHA_512"}),
        ))
        .err()
        .expect("mismatched MacAlgorithm must be rejected");
    assert!(
        format!("{err:?}").contains("InvalidKeyUsageException"),
        "got {err:?}"
    );

    let err = svc
        .verify_mac(&make_request(
            "VerifyMac",
            json!({
                "KeyId": key_id,
                "Message": msg_b64,
                "Mac": base64::engine::general_purpose::STANDARD.encode(b"whatever"),
                "MacAlgorithm": "HMAC_SHA_512",
            }),
        ))
        .err()
        .expect("mismatched MacAlgorithm must be rejected on verify too");
    assert!(
        format!("{err:?}").contains("InvalidKeyUsageException"),
        "got {err:?}"
    );
}

/// M3: HMAC_224 keys advertise HMAC_SHA_224 and must round-trip through
/// GenerateMac -> VerifyMac.
#[test]
fn hmac_224_generate_and_verify_round_trip() {
    use base64::Engine;
    let svc = make_service();
    let key_id = create_key_with_opts(
        &svc,
        json!({ "KeyUsage": "GENERATE_VERIFY_MAC", "KeySpec": "HMAC_224" }),
    );
    let msg_b64 = base64::engine::general_purpose::STANDARD.encode(b"mac me");

    let resp = svc
        .generate_mac(&make_request(
            "GenerateMac",
            json!({"KeyId": key_id, "Message": msg_b64, "MacAlgorithm": "HMAC_SHA_224"}),
        ))
        .unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let mac = body["Mac"].as_str().unwrap().to_string();

    let resp = svc
        .verify_mac(&make_request(
            "VerifyMac",
            json!({
                "KeyId": key_id,
                "Message": msg_b64,
                "Mac": mac,
                "MacAlgorithm": "HMAC_SHA_224",
            }),
        ))
        .unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["MacValid"].as_bool(), Some(true));
}

/// M7: DescribeKey on a replica ARN returns the replica (REPLICA type, its
/// own region), not the primary that shares the same bare key id.
#[test]
fn describe_key_on_replica_arn_returns_replica() {
    let svc = make_service();
    let key_id = create_key_with_opts(&svc, json!({ "MultiRegion": true }));

    let resp = svc
        .replicate_key(&make_request(
            "ReplicateKey",
            json!({"KeyId": key_id, "ReplicaRegion": "us-west-2"}),
        ))
        .unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let replica_arn = body["ReplicaKeyMetadata"]["Arn"]
        .as_str()
        .unwrap()
        .to_string();
    assert!(replica_arn.contains(":us-west-2:"), "got {replica_arn}");

    let resp = svc
        .describe_key(&make_request("DescribeKey", json!({"KeyId": replica_arn})))
        .unwrap();
    let meta: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let meta = &meta["KeyMetadata"];
    assert!(meta["Arn"].as_str().unwrap().contains(":us-west-2:"));
    assert_eq!(
        meta["MultiRegionConfiguration"]["MultiRegionKeyType"].as_str(),
        Some("REPLICA")
    );
    assert_eq!(
        meta["MultiRegionConfiguration"]["PrimaryKey"]["Region"].as_str(),
        Some("us-east-1")
    );

    // The primary ARN still describes as PRIMARY.
    let resp = svc
        .describe_key(&make_request("DescribeKey", json!({"KeyId": key_id})))
        .unwrap();
    let meta: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(
        meta["KeyMetadata"]["MultiRegionConfiguration"]["MultiRegionKeyType"].as_str(),
        Some("PRIMARY")
    );
}

/// L2: ScheduleKeyDeletion enforces the 7..=30 day window.
#[test]
fn schedule_key_deletion_rejects_out_of_range_window() {
    let svc = make_service();
    let key_id = create_key(&svc);
    for bad in [0_i64, 3, 31, 400] {
        let err = svc
            .schedule_key_deletion(&make_request(
                "ScheduleKeyDeletion",
                json!({"KeyId": key_id, "PendingWindowInDays": bad}),
            ))
            .err()
            .unwrap_or_else(|| panic!("window {bad} must be rejected"));
        assert!(
            format!("{err:?}").contains("ValidationException"),
            "window {bad}: got {err:?}"
        );
    }
    // A valid window still works.
    svc.schedule_key_deletion(&make_request(
        "ScheduleKeyDeletion",
        json!({"KeyId": key_id, "PendingWindowInDays": 7}),
    ))
    .unwrap();
}

/// L3: ReEncrypt refuses a destination key that is not ENCRYPT_DECRYPT.
#[test]
fn re_encrypt_rejects_non_encrypt_destination() {
    use base64::Engine;
    let svc = make_service();
    let src = create_key(&svc);
    let ciphertext = {
        let resp = svc
            .encrypt(&make_request(
                "Encrypt",
                json!({
                    "KeyId": src,
                    "Plaintext": base64::engine::general_purpose::STANDARD.encode(b"secret"),
                }),
            ))
            .unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        body["CiphertextBlob"].as_str().unwrap().to_string()
    };

    let signing_key = create_key_with_opts(
        &svc,
        json!({ "KeyUsage": "SIGN_VERIFY", "KeySpec": "RSA_2048" }),
    );
    let err = svc
        .re_encrypt(&make_request(
            "ReEncrypt",
            json!({"CiphertextBlob": ciphertext, "DestinationKeyId": signing_key}),
        ))
        .err()
        .expect("re-encrypt into a SIGN_VERIFY key must fail");
    assert!(
        format!("{err:?}").contains("InvalidKeyUsageException"),
        "got {err:?}"
    );
}

/// L3: ReEncrypt into an RSA ENCRYPT_DECRYPT key produces a real RSA-OAEP
/// ciphertext that Decrypt recovers, and echoes the true source/dest
/// algorithms rather than a hardcoded SYMMETRIC_DEFAULT pair.
#[test]
fn re_encrypt_to_rsa_destination_round_trips() {
    use base64::Engine;
    let svc = make_service();
    let src = create_key(&svc);
    let resp = svc
        .encrypt(&make_request(
            "Encrypt",
            json!({
                "KeyId": src,
                "Plaintext": base64::engine::general_purpose::STANDARD.encode(b"top secret"),
            }),
        ))
        .unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let ciphertext = body["CiphertextBlob"].as_str().unwrap().to_string();

    let rsa_key = create_key_with_opts(
        &svc,
        json!({ "KeyUsage": "ENCRYPT_DECRYPT", "KeySpec": "RSA_2048" }),
    );
    let resp = svc
        .re_encrypt(&make_request(
            "ReEncrypt",
            json!({"CiphertextBlob": ciphertext, "DestinationKeyId": rsa_key}),
        ))
        .unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(
        body["SourceEncryptionAlgorithm"].as_str(),
        Some("SYMMETRIC_DEFAULT")
    );
    assert_eq!(
        body["DestinationEncryptionAlgorithm"].as_str(),
        Some("RSAES_OAEP_SHA_256")
    );
    let new_ct = body["CiphertextBlob"].as_str().unwrap().to_string();

    let resp = svc
        .decrypt(&make_request(
            "Decrypt",
            json!({
                "CiphertextBlob": new_ct,
                "KeyId": rsa_key,
                "EncryptionAlgorithm": "RSAES_OAEP_SHA_256",
            }),
        ))
        .unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let recovered = base64::engine::general_purpose::STANDARD
        .decode(body["Plaintext"].as_str().unwrap())
        .unwrap();
    assert_eq!(recovered, b"top secret");
}

/// L5: a non-string EncryptionContext value is rejected, not silently
/// dropped (which would change the AAD bound at encrypt time).
#[test]
fn list_keys_resumes_past_a_deleted_marker_without_restarting() {
    // Regression: resolving the ListKeys marker by exact match with a
    // `.unwrap_or(0)` fallback restarted the listing from the first key whenever
    // the key named by the marker was deleted between pages, so a delete-as-you-go
    // loop re-emitted page 1 forever. The marker must advance past a missing key.
    let svc = make_service();

    // Create enough keys to force a second page at Limit=2.
    for _ in 0..5 {
        create_key(&svc);
    }

    let page1_resp = svc
        .list_keys(&make_request("ListKeys", json!({ "Limit": 2 })))
        .unwrap();
    let page1: Value = serde_json::from_slice(page1_resp.body.expect_bytes()).unwrap();
    assert_eq!(
        page1["Truncated"],
        json!(true),
        "first page must be truncated"
    );
    let page1_ids: Vec<String> = page1["Keys"]
        .as_array()
        .unwrap()
        .iter()
        .map(|k| k["KeyId"].as_str().unwrap().to_string())
        .collect();
    assert_eq!(page1_ids.len(), 2);
    let marker = page1["NextMarker"].as_str().unwrap().to_string();
    assert_eq!(marker, page1_ids[1], "marker is the last KeyId of the page");

    // Delete the key the marker names, simulating a delete between pages.
    {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create("123456789012");
        assert!(state.keys.remove(&marker).is_some(), "marker key existed");
    }

    let page2_resp = svc
        .list_keys(&make_request(
            "ListKeys",
            json!({ "Limit": 2, "Marker": marker }),
        ))
        .unwrap();
    let page2: Value = serde_json::from_slice(page2_resp.body.expect_bytes()).unwrap();
    let page2_ids: Vec<String> = page2["Keys"]
        .as_array()
        .unwrap()
        .iter()
        .map(|k| k["KeyId"].as_str().unwrap().to_string())
        .collect();

    assert!(
        !page2_ids.is_empty(),
        "second page must make forward progress"
    );
    // No restart: the first key of page 1 must not reappear on page 2.
    assert!(
        !page2_ids.contains(&page1_ids[0]),
        "page 2 restarted at page 1 (got {page2_ids:?}, page1 started with {})",
        page1_ids[0]
    );
    // Every returned key sorts strictly after the (now deleted) marker.
    for id in &page2_ids {
        assert!(
            id.as_str() > marker.as_str(),
            "page 2 key {id} is not past the marker {marker}"
        );
    }
}

#[test]
fn encrypt_rejects_non_string_encryption_context_value() {
    use base64::Engine;
    let svc = make_service();
    let key_id = create_key(&svc);
    let err = svc
        .encrypt(&make_request(
            "Encrypt",
            json!({
                "KeyId": key_id,
                "Plaintext": base64::engine::general_purpose::STANDARD.encode(b"hi"),
                "EncryptionContext": { "purpose": 42 },
            }),
        ))
        .err()
        .expect("non-string EC value must be rejected");
    assert!(
        format!("{err:?}").contains("ValidationException"),
        "got {err:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rsa_key_generation_does_not_hold_the_state_lock() {
    // Regression: `create_key` used to generate the RSA keypair *after*
    // taking `state.write()`, so one RSA CreateKey stalled every other KMS
    // request — symmetric CreateKey included — for the whole generation.
    // Key material depends only on `KeySpec`, so it is now minted before the
    // lock is taken.
    use std::sync::atomic::{AtomicBool, Ordering};

    let svc = Arc::new(make_service());
    let rsa_done = Arc::new(AtomicBool::new(false));

    let rsa_svc = svc.clone();
    let rsa_flag = rsa_done.clone();
    let rsa = tokio::task::spawn(async move {
        let req = make_request(
            "CreateKey",
            json!({ "KeySpec": "RSA_3072", "KeyUsage": "SIGN_VERIFY" }),
        );
        let resp = rsa_svc.create_key(&req).expect("rsa CreateKey ok");
        rsa_flag.store(true, Ordering::SeqCst);
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        body["KeyMetadata"]["KeyId"].as_str().unwrap().to_string()
    });

    // Give the RSA task a moment to reach generation, then hammer the same
    // service with symmetric work. Under the old code these blocked on the
    // write lock until the RSA key was done.
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    let mut ids = Vec::new();
    for _ in 0..20 {
        let req = make_request("CreateKey", json!({}));
        let resp = svc.create_key(&req).expect("symmetric CreateKey ok");
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        ids.push(body["KeyMetadata"]["KeyId"].as_str().unwrap().to_string());
    }
    assert!(
        !rsa_done.load(Ordering::SeqCst),
        "20 symmetric CreateKey calls should finish long before one RSA-3072 keygen"
    );

    let rsa_id = rsa.await.expect("rsa task joined");
    assert_eq!(ids.len(), 20);
    // Every key is distinct and the RSA one really carries generated material.
    let req = make_request("GetPublicKey", json!({ "KeyId": rsa_id }));
    let resp = svc.get_public_key(&req).expect("GetPublicKey ok");
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert!(!body["PublicKey"].as_str().unwrap().is_empty());
}

/// ListAliases pagination walks every alias of the request region exactly
/// once, in AliasName order, when customer aliases and the pre-listed
/// AWS-managed aliases are mixed, including after services minted
/// AWS-managed keys in other regions (whose aliases live in those regions).
#[test]
fn list_aliases_pagination_covers_customer_and_managed_aliases() {
    let svc = make_service();
    let key_id = create_key(&svc);
    create_alias(&svc, "alias/aaa-first", &key_id);
    create_alias(&svc, "alias/zzz-last", &key_id);
    let hook = crate::hook::KmsServiceHook::new(svc.state.clone(), Default::default());
    for region in ["us-west-2", "cn-north-1"] {
        hook.aws_managed_key_arn_tracked(
            "123456789012",
            region,
            "timestream",
            "timestream.amazonaws.com",
        );
    }
    let list = |region: &str, body: Value| -> Value {
        let mut req = make_request("ListAliases", body);
        req.region = region.to_string();
        serde_json::from_slice(svc.list_aliases(&req).unwrap().body.expect_bytes()).unwrap()
    };
    let names = |page: &Value| -> Vec<String> {
        page["Aliases"]
            .as_array()
            .unwrap()
            .iter()
            .map(|a| a["AliasName"].as_str().unwrap().to_string())
            .collect()
    };
    let all = names(&list("us-east-1", json!({})));
    assert!(all.iter().any(|n| n == "alias/aaa-first"));
    assert!(all.iter().any(|n| n == "alias/zzz-last"));
    assert!(all.iter().any(|n| n == "alias/aws/s3"));
    assert!(
        !all.iter().any(|n| n == "alias/aws/timestream"),
        "other regions' managed aliases are not listed here"
    );
    let mut sorted = all.clone();
    sorted.sort();
    assert_eq!(all, sorted, "listed in AliasName order");

    let west = names(&list("us-west-2", json!({})));
    assert_eq!(
        west.iter().filter(|n| *n == "alias/aws/timestream").count(),
        1,
        "one managed alias entry per region"
    );
    assert!(
        !west.iter().any(|n| n == "alias/aaa-first"),
        "customer aliases stay in their region"
    );

    for (region, expected) in [("us-east-1", &all), ("us-west-2", &west)] {
        let mut paged: Vec<String> = Vec::new();
        let mut marker: Option<String> = None;
        loop {
            let mut body = json!({ "Limit": 3 });
            if let Some(m) = &marker {
                body["Marker"] = json!(m);
            }
            let page = list(region, body);
            paged.extend(names(&page));
            if page["Truncated"] != json!(true) {
                break;
            }
            marker = page["NextMarker"].as_str().map(str::to_string);
        }
        assert_eq!(
            &paged, expected,
            "{region}: pages cover every alias once, in order"
        );
    }
}

fn regional_request(action: &str, region: &str, body: Value) -> AwsRequest {
    let mut req = make_request(action, body);
    req.region = region.to_string();
    req
}

fn create_key_in(svc: &KmsService, region: &str) -> String {
    let resp = svc
        .create_key(&regional_request("CreateKey", region, json!({})))
        .unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    body["KeyMetadata"]["KeyId"].as_str().unwrap().to_string()
}

fn describe_key_id_in(svc: &KmsService, region: &str, key: &str) -> Result<String, String> {
    svc.describe_key(&regional_request(
        "DescribeKey",
        region,
        json!({ "KeyId": key }),
    ))
    .map(|resp| {
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        body["KeyMetadata"]["KeyId"].as_str().unwrap().to_string()
    })
    .map_err(|e| e.code().to_string())
}

fn customer_alias_names_in(svc: &KmsService, region: &str) -> Vec<(String, String)> {
    let resp = svc
        .list_aliases(&regional_request("ListAliases", region, json!({})))
        .unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    body["Aliases"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|a| !a["AliasName"].as_str().unwrap().starts_with("alias/aws/"))
        .map(|a| {
            (
                a["AliasName"].as_str().unwrap().to_string(),
                a["AliasArn"].as_str().unwrap().to_string(),
            )
        })
        .collect()
}

/// An alias is regional: the same name exists independently in two regions,
/// each pointing at its own region's key, listed and resolved only there,
/// while an alias ARN resolves in the region it names from anywhere.
#[test]
fn same_alias_name_is_independent_per_region() {
    let svc = make_service();
    let east_key = create_key_in(&svc, "us-east-1");
    let west_key = create_key_in(&svc, "eu-west-1");
    for (region, key) in [("us-east-1", &east_key), ("eu-west-1", &west_key)] {
        svc.create_alias(&regional_request(
            "CreateAlias",
            region,
            json!({ "AliasName": "alias/app", "TargetKeyId": key }),
        ))
        .unwrap_or_else(|e| panic!("CreateAlias in {region}: {}", e.code()));
    }
    // A second CreateAlias of the name in the same region still conflicts.
    let dup = svc
        .create_alias(&regional_request(
            "CreateAlias",
            "eu-west-1",
            json!({ "AliasName": "alias/app", "TargetKeyId": west_key }),
        ))
        .err()
        .expect("expected an error");
    assert_eq!(dup.code(), "AlreadyExistsException");

    assert_eq!(
        describe_key_id_in(&svc, "us-east-1", "alias/app").unwrap(),
        east_key
    );
    assert_eq!(
        describe_key_id_in(&svc, "eu-west-1", "alias/app").unwrap(),
        west_key
    );
    assert_eq!(
        describe_key_id_in(
            &svc,
            "us-east-1",
            "arn:aws:kms:eu-west-1:123456789012:alias/app"
        )
        .unwrap(),
        west_key
    );
    assert_eq!(
        describe_key_id_in(&svc, "ap-south-1", "alias/app").unwrap_err(),
        "NotFoundException"
    );

    assert_eq!(
        customer_alias_names_in(&svc, "us-east-1"),
        vec![(
            "alias/app".to_string(),
            "arn:aws:kms:us-east-1:123456789012:alias/app".to_string()
        )]
    );
    assert_eq!(
        customer_alias_names_in(&svc, "eu-west-1"),
        vec![(
            "alias/app".to_string(),
            "arn:aws:kms:eu-west-1:123456789012:alias/app".to_string()
        )]
    );
    assert!(customer_alias_names_in(&svc, "ap-south-1").is_empty());

    // Encrypt under the alias uses the request region's key.
    let resp = svc
        .encrypt(&regional_request(
            "Encrypt",
            "eu-west-1",
            json!({ "KeyId": "alias/app", "Plaintext": "aGk=" }),
        ))
        .unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert!(body["KeyId"].as_str().unwrap().ends_with(&west_key));

    // Deleting one region's alias leaves the other.
    svc.delete_alias(&regional_request(
        "DeleteAlias",
        "us-east-1",
        json!({ "AliasName": "alias/app" }),
    ))
    .unwrap();
    assert!(customer_alias_names_in(&svc, "us-east-1").is_empty());
    assert_eq!(
        describe_key_id_in(&svc, "eu-west-1", "alias/app").unwrap(),
        west_key
    );
    let gone = svc
        .delete_alias(&regional_request(
            "DeleteAlias",
            "us-east-1",
            json!({ "AliasName": "alias/app" }),
        ))
        .err()
        .expect("expected an error");
    assert_eq!(gone.code(), "NotFoundException");
}

/// An alias and its key share a region: CreateAlias / UpdateAlias reject a
/// key from another region, and UpdateAlias only sees the request region's
/// alias.
#[test]
fn alias_target_must_be_in_the_alias_region() {
    let svc = make_service();
    let east_key = create_key_in(&svc, "us-east-1");
    let west_key = create_key_in(&svc, "eu-west-1");
    let east_arn = format!("arn:aws:kms:us-east-1:123456789012:key/{east_key}");
    for target in [east_key.as_str(), east_arn.as_str()] {
        let err = svc
            .create_alias(&regional_request(
                "CreateAlias",
                "eu-west-1",
                json!({ "AliasName": "alias/cross", "TargetKeyId": target }),
            ))
            .err()
            .expect("expected an error");
        assert_eq!(err.code(), "NotFoundException", "target {target}");
    }
    svc.create_alias(&regional_request(
        "CreateAlias",
        "eu-west-1",
        json!({ "AliasName": "alias/cross", "TargetKeyId": west_key }),
    ))
    .unwrap();
    let err = svc
        .update_alias(&regional_request(
            "UpdateAlias",
            "eu-west-1",
            json!({ "AliasName": "alias/cross", "TargetKeyId": east_key }),
        ))
        .err()
        .expect("expected an error");
    assert_eq!(err.code(), "NotFoundException");
    let err = svc
        .update_alias(&regional_request(
            "UpdateAlias",
            "us-east-1",
            json!({ "AliasName": "alias/cross", "TargetKeyId": east_key }),
        ))
        .err()
        .expect("expected an error");
    assert_eq!(
        err.code(),
        "NotFoundException",
        "no alias/cross in us-east-1"
    );
}

/// A multi-region key's replica stands in for the primary when an alias in
/// the replica's region names the shared key id.
#[test]
fn alias_in_replica_region_targets_the_replica() {
    let svc = make_service();
    let resp = svc
        .create_key(&regional_request(
            "CreateKey",
            "us-east-1",
            json!({ "MultiRegion": true }),
        ))
        .unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let key_id = body["KeyMetadata"]["KeyId"].as_str().unwrap().to_string();
    svc.replicate_key(&regional_request(
        "ReplicateKey",
        "us-east-1",
        json!({ "KeyId": key_id, "ReplicaRegion": "eu-west-1" }),
    ))
    .unwrap();
    svc.create_alias(&regional_request(
        "CreateAlias",
        "eu-west-1",
        json!({ "AliasName": "alias/mrk", "TargetKeyId": key_id }),
    ))
    .unwrap();
    let resp = svc
        .describe_key(&regional_request(
            "DescribeKey",
            "eu-west-1",
            json!({ "KeyId": "alias/mrk" }),
        ))
        .unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(
        body["KeyMetadata"]["Arn"],
        format!("arn:aws:kms:eu-west-1:123456789012:key/{key_id}")
    );
    // ListAliases reports the key id (not the replica's storage key), and a
    // KeyId filter of the shared id finds the alias on the replica.
    for filter in [None, Some(key_id.clone())] {
        let mut body = json!({});
        if let Some(f) = &filter {
            body["KeyId"] = json!(f);
        }
        let resp = svc
            .list_aliases(&regional_request("ListAliases", "eu-west-1", body))
            .unwrap();
        let listed: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        let mrk: Vec<&Value> = listed["Aliases"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|a| a["AliasName"] == "alias/mrk")
            .collect();
        assert_eq!(mrk.len(), 1, "filter {filter:?}");
        assert_eq!(mrk[0]["TargetKeyId"], json!(key_id), "filter {filter:?}");
    }
}

// ---- Service Quotas: customer managed keys ----

fn quota_service(limit: f64) -> KmsService {
    make_service().with_quota_provider(Some(Arc::new(
        fakecloud_core::quota::FixedQuotas::default().with("kms", "L-C2F1777E", limit),
    )))
}

fn in_region(action: &str, body: Value, region: &str) -> AwsRequest {
    let mut req = make_request(action, body);
    req.region = region.to_string();
    req
}

fn assert_key_limit(result: Result<AwsResponse, AwsServiceError>) {
    let err = result.err().expect("CreateKey should be refused");
    assert_eq!(err.status(), StatusCode::BAD_REQUEST);
    assert_eq!(err.code(), "LimitExceededException");
}

#[test]
fn key_quota_is_not_enforced_without_a_provider() {
    let svc = make_service();
    for _ in 0..3 {
        create_key(&svc);
    }
}

#[test]
fn key_quota_counts_customer_keys_in_any_state_per_region() {
    let svc = quota_service(2.0);
    // AWS managed keys (minted for the `alias/aws/*` aliases) do not count.
    svc.list_aliases(&make_request("ListAliases", json!({})))
        .unwrap();
    let first = create_key(&svc);
    create_key(&svc);
    assert_key_limit(svc.create_key(&make_request("CreateKey", json!({}))));

    // A key pending deletion still counts.
    svc.schedule_key_deletion(&make_request(
        "ScheduleKeyDeletion",
        json!({"KeyId": first, "PendingWindowInDays": 7}),
    ))
    .unwrap();
    assert_key_limit(svc.create_key(&make_request("CreateKey", json!({}))));

    // The quota is per Region.
    svc.create_key(&in_region("CreateKey", json!({}), "eu-west-1"))
        .unwrap();
}

#[test]
fn key_quota_counts_a_replica_in_its_own_region() {
    let svc = quota_service(1.0);
    let resp = svc
        .create_key(&make_request("CreateKey", json!({"MultiRegion": true})))
        .unwrap();
    let key_id = body_json(resp)["KeyMetadata"]["KeyId"]
        .as_str()
        .unwrap()
        .to_string();
    svc.create_key(&in_region("CreateKey", json!({}), "eu-west-1"))
        .unwrap();
    assert_key_limit(svc.replicate_key(&make_request(
        "ReplicateKey",
        json!({"KeyId": key_id, "ReplicaRegion": "eu-west-1"}),
    )));
    svc.replicate_key(&make_request(
        "ReplicateKey",
        json!({"KeyId": key_id, "ReplicaRegion": "us-west-2"}),
    ))
    .unwrap();
}

#[test]
fn key_usage_source_counts_customer_keys_per_region() {
    use fakecloud_core::quota::QuotaUsageSource;
    let svc = make_service();
    svc.list_aliases(&make_request("ListAliases", json!({})))
        .unwrap();
    create_key(&svc);
    create_key(&svc);
    let usage = crate::quota::KmsQuotaUsage::new(svc.state.clone());
    assert_eq!(
        usage.usage("123456789012", "us-east-1", "kms", "L-C2F1777E"),
        Some(2.0)
    );
    assert_eq!(
        usage.usage("123456789012", "eu-west-1", "kms", "L-C2F1777E"),
        Some(0.0)
    );
}

/// AWS refuses key management on AWS-managed keys to every caller
/// (recorded against `alias/aws/lambda`'s key: 400 AccessDeniedException
/// naming the action and the key), whatever the IAM mode. Reads and
/// customer managed keys are unaffected.
#[tokio::test]
async fn aws_managed_keys_refuse_key_management() {
    let svc = make_service();
    let target = aws_managed_target(&svc, "lambda").await;
    let arn = format!("arn:aws:kms:us-east-1:123456789012:key/{target}");
    let policy_before = body_json(
        svc.handle(make_request("GetKeyPolicy", json!({"KeyId": target})))
            .await
            .unwrap(),
    )["Policy"]
        .clone();
    for (action, body) in [
        (
            "PutKeyPolicy",
            json!({"KeyId": target, "PolicyName": "default", "Policy": "{}"}),
        ),
        (
            "ScheduleKeyDeletion",
            json!({"KeyId": target, "PendingWindowInDays": 30}),
        ),
        ("CancelKeyDeletion", json!({"KeyId": target})),
        ("DisableKey", json!({"KeyId": arn})),
        ("EnableKey", json!({"KeyId": target})),
        ("EnableKeyRotation", json!({"KeyId": target})),
        ("DisableKeyRotation", json!({"KeyId": target})),
        ("RotateKeyOnDemand", json!({"KeyId": target})),
        (
            "TagResource",
            json!({"KeyId": target, "Tags": [{"TagKey": "a", "TagValue": "b"}]}),
        ),
        ("UntagResource", json!({"KeyId": target, "TagKeys": ["a"]})),
        (
            "UpdateKeyDescription",
            json!({"KeyId": target, "Description": "x"}),
        ),
        (
            "CreateGrant",
            json!({"KeyId": target, "GranteePrincipal": "arn:aws:iam::123456789012:root", "Operations": ["Decrypt"]}),
        ),
        ("DeleteImportedKeyMaterial", json!({"KeyId": target})),
        (
            "ImportKeyMaterial",
            json!({"KeyId": target, "ImportToken": "YWJjZA==", "EncryptedKeyMaterial": "YWJjZA==", "ExpirationModel": "KEY_MATERIAL_DOES_NOT_EXPIRE"}),
        ),
        (
            "ReplicateKey",
            json!({"KeyId": target, "ReplicaRegion": "us-west-2"}),
        ),
        (
            "UpdatePrimaryRegion",
            json!({"KeyId": target, "PrimaryRegion": "us-west-2"}),
        ),
        (
            "CreateAlias",
            json!({"AliasName": "alias/mine", "TargetKeyId": target}),
        ),
    ] {
        let err = svc.handle(make_request(action, body)).await.err().unwrap();
        assert_eq!(
            (err.status(), err.code(), err.message()),
            (
                StatusCode::BAD_REQUEST,
                "AccessDeniedException",
                format!("User: arn:aws:iam::123456789012:root is not authorized to perform: kms:{action} on resource: {arn} because no resource-based policy allows the kms:{action} action")
            ),
            "{action}"
        );
    }
    let after = body_json(
        svc.handle(make_request("DescribeKey", json!({"KeyId": target})))
            .await
            .unwrap(),
    );
    assert_eq!(after["KeyMetadata"]["KeyState"], "Enabled");
    let policy_after = body_json(
        svc.handle(make_request("GetKeyPolicy", json!({"KeyId": target})))
            .await
            .unwrap(),
    )["Policy"]
        .clone();
    assert_eq!(policy_before, policy_after);
    // A customer managed key is managed as before.
    let mine = body_json(
        svc.handle(make_request("CreateKey", json!({})))
            .await
            .unwrap(),
    )["KeyMetadata"]["KeyId"]
        .as_str()
        .unwrap()
        .to_string();
    assert!(svc
        .handle(make_request("DisableKey", json!({"KeyId": mine})))
        .await
        .is_ok());
}

/// The key id the `alias/aws/<service>` alias of `svc` targets in us-east-1.
/// The key id behind `alias/aws/<service>` in us-east-1: DescribeKey on
/// the alias creates the key on first use, as AWS does.
async fn aws_managed_target(svc: &KmsService, service: &str) -> String {
    let described = body_json(
        svc.handle(make_request(
            "DescribeKey",
            json!({"KeyId": format!("alias/aws/{service}")}),
        ))
        .await
        .unwrap(),
    );
    described["KeyMetadata"]["KeyId"]
        .as_str()
        .unwrap()
        .to_string()
}

fn assert_key_policy_denied(err: AwsServiceError, caller: &str, action: &str, arn: &str) {
    assert_eq!(
        (err.status(), err.code(), err.message()),
        (
            StatusCode::BAD_REQUEST,
            "AccessDeniedException",
            format!("User: {caller} is not authorized to perform: {action} on resource: {arn} because no resource-based policy allows the {action} action")
        ),
        "{action}"
    );
}

/// AWS refuses direct cryptographic use of AWS-managed keys (recorded:
/// `alias/aws/s3`, `alias/aws/secretsmanager`, `alias/aws/ebs` Encrypt and
/// GenerateDataKey, 400 AccessDeniedException naming the resolved key): their
/// key policies grant it only through the owning service. The refusal names
/// the key the alias resolves to and the operation's IAM action.
#[tokio::test]
async fn aws_managed_keys_refuse_direct_crypto() {
    let svc = make_service();
    let root = "arn:aws:iam::123456789012:root";
    for service in ["s3", "secretsmanager", "ebs", "sqs", "lambda"] {
        let target = aws_managed_target(&svc, service).await;
        let arn = format!("arn:aws:kms:us-east-1:123456789012:key/{target}");
        let alias = format!("alias/aws/{service}");
        for (action, body) in [
            ("Encrypt", json!({"KeyId": alias, "Plaintext": "aGVsbG8="})),
            (
                "GenerateDataKey",
                json!({"KeyId": alias, "KeySpec": "AES_256"}),
            ),
            (
                "GenerateDataKeyWithoutPlaintext",
                json!({"KeyId": arn, "KeySpec": "AES_256"}),
            ),
            (
                "GenerateDataKeyPair",
                json!({"KeyId": target, "KeyPairSpec": "RSA_2048"}),
            ),
            (
                "GenerateDataKeyPairWithoutPlaintext",
                json!({"KeyId": target, "KeyPairSpec": "RSA_2048"}),
            ),
        ] {
            let err = svc.handle(make_request(action, body)).await.err().unwrap();
            assert_key_policy_denied(err, root, &format!("kms:{action}"), &arn);
        }
        // Metadata reads stay allowed.
        svc.handle(make_request("DescribeKey", json!({"KeyId": alias})))
            .await
            .unwrap();
    }
}

/// A ciphertext a service produced under its AWS-managed key (through the
/// KMS hook) cannot be decrypted or re-encrypted directly, and a customer
/// key's ciphertext cannot be re-encrypted into an AWS-managed key.
#[tokio::test]
async fn aws_managed_key_ciphertexts_stay_with_their_service() {
    let svc = make_service();
    let hook = crate::hook::KmsServiceHook::new(svc.state.clone(), Default::default());
    let ct = hook
        .encrypt(
            "123456789012",
            "us-east-1",
            "alias/aws/s3",
            b"object",
            KmsCaller::via("s3.amazonaws.com"),
            HashMap::new(),
        )
        .unwrap();
    let target = aws_managed_target(&svc, "s3").await;
    let s3_arn = format!("arn:aws:kms:us-east-1:123456789012:key/{target}");
    let root = "arn:aws:iam::123456789012:root";
    let err = svc
        .handle(make_request("Decrypt", json!({"CiphertextBlob": ct})))
        .await
        .err()
        .unwrap();
    assert_key_policy_denied(err, root, "kms:Decrypt", &s3_arn);

    let customer = create_key(&svc);
    let err = svc
        .handle(make_request(
            "ReEncrypt",
            json!({"CiphertextBlob": ct, "DestinationKeyId": customer}),
        ))
        .await
        .err()
        .unwrap();
    assert_key_policy_denied(err, root, "kms:ReEncryptFrom", &s3_arn);

    let mine = body_json(
        svc.handle(make_request(
            "Encrypt",
            json!({"KeyId": customer, "Plaintext": "aGVsbG8="}),
        ))
        .await
        .unwrap(),
    )["CiphertextBlob"]
        .as_str()
        .unwrap()
        .to_string();
    let err = svc
        .handle(make_request(
            "ReEncrypt",
            json!({"CiphertextBlob": mine, "DestinationKeyId": "alias/aws/s3"}),
        ))
        .await
        .err()
        .unwrap();
    assert_key_policy_denied(err, root, "kms:ReEncryptTo", &s3_arn);
    // The customer key itself is unaffected.
    svc.handle(make_request("Decrypt", json!({"CiphertextBlob": mine})))
        .await
        .unwrap();
}

/// The recorded `aws/lambda` policy lets account principals decrypt
/// ciphertexts bound to one of the account's functions directly (its
/// `kms:EncryptionContext:aws:lambda:FunctionArn` statement); any other
/// context is refused. The refusal names the calling IAM identity.
#[tokio::test]
async fn aws_lambda_key_decrypts_function_ciphertexts_directly() {
    let svc = make_service();
    let function = "arn:aws:lambda:us-east-1:123456789012:function:f";
    let target = aws_managed_target(&svc, "lambda").await;
    // Lambda seals a value for the function through the KMS hook, bound to
    // the function's ARN as its encryption context.
    let hook = crate::hook::KmsServiceHook::new(svc.state.clone(), Default::default());
    let ct = hook
        .encrypt(
            "123456789012",
            "us-east-1",
            "alias/aws/lambda",
            b"env",
            KmsCaller::via("lambda.amazonaws.com"),
            [("aws:lambda:FunctionArn".to_string(), function.to_string())].into(),
        )
        .unwrap();
    let out = body_json(
        svc.handle(make_request(
            "Decrypt",
            json!({"CiphertextBlob": ct, "EncryptionContext": {"aws:lambda:FunctionArn": function}}),
        ))
        .await
        .unwrap(),
    );
    assert_eq!(out["Plaintext"], "ZW52");
    let err = svc
        .handle(make_request("Decrypt", json!({"CiphertextBlob": ct})))
        .await
        .err()
        .unwrap();
    assert_eq!(err.code(), "InvalidCiphertextException");

    let arn = format!("arn:aws:kms:us-east-1:123456789012:key/{target}");
    let mut req = make_request(
        "Encrypt",
        json!({"KeyId": "alias/aws/lambda", "Plaintext": "ZW52", "EncryptionContext": {"aws:lambda:FunctionArn": function}}),
    );
    req.principal = Some(fakecloud_core::auth::Principal {
        arn: "arn:aws:iam::123456789012:user/admin".to_string(),
        user_id: "AIDAEXAMPLE".to_string(),
        account_id: "123456789012".to_string(),
        principal_type: fakecloud_core::auth::PrincipalType::User,
        source_identity: None,
        tags: None,
    });
    let err = svc.handle(req).await.err().unwrap();
    assert_key_policy_denied(
        err,
        "arn:aws:iam::123456789012:user/admin",
        "kms:Encrypt",
        &arn,
    );
}

/// GetKeyPolicy returns the policy AWS gives each AWS-managed key, with the
/// account and region substituted.
#[tokio::test]
async fn aws_managed_key_policies_are_the_recorded_ones() {
    let svc = make_service();
    for service in ["s3", "secretsmanager", "ebs", "lambda", "dynamodb"] {
        let target = aws_managed_target(&svc, service).await;
        let policy = body_json(
            svc.handle(make_request(
                "GetKeyPolicy",
                json!({"KeyId": target, "PolicyName": "default"}),
            ))
            .await
            .unwrap(),
        )["Policy"]
            .as_str()
            .unwrap()
            .to_string();
        assert_eq!(
            policy,
            crate::aws_managed::key_policy(service, "123456789012", "us-east-1").unwrap()
        );
        let via = match service {
            "ebs" => "\"ec2.us-east-1.amazonaws.com\"",
            "dynamodb" => "\"dynamodb.*.amazonaws.com\"",
            "secretsmanager" => "\"secretsmanager.us-east-1.amazonaws.com\"",
            "s3" => "\"s3.us-east-1.amazonaws.com\"",
            _ => "\"lambda.us-east-1.amazonaws.com\"",
        };
        assert!(
            policy.contains(&format!("\"kms:ViaService\" : {via}")),
            "{service}: {policy}"
        );
    }
}

/// Under `--iam`, key policies see `kms:CallerAccount` and the request's
/// encryption context.
#[test]
fn iam_condition_keys_carry_caller_account_and_encryption_context() {
    let svc = make_service();
    let req = make_request(
        "Decrypt",
        json!({"CiphertextBlob": "x", "EncryptionContext": {"aws:lambda:FunctionArn": "arn"}}),
    );
    let action = svc.iam_action_for(&req).unwrap();
    let keys = svc.iam_condition_keys_for(&req, &action);
    assert_eq!(keys["kms:calleraccount"], vec!["123456789012".to_string()]);
    assert_eq!(
        keys["kms:encryptioncontext:aws:lambda:FunctionArn"],
        vec!["arn".to_string()]
    );
    assert_eq!(
        keys["kms:encryptioncontextkeys"],
        vec!["aws:lambda:FunctionArn".to_string()]
    );
    assert!(!keys.contains_key("kms:viaservice"));
}

/// `make_request` as a user of `account`.
fn request_as(account: &str, action: &str, body: Value) -> AwsRequest {
    let mut req = make_request(action, body);
    req.account_id = account.to_string();
    req.principal = Some(fakecloud_core::auth::Principal {
        arn: format!("arn:aws:iam::{account}:user/u"),
        user_id: "AIDAEXAMPLE".to_string(),
        account_id: account.to_string(),
        principal_type: fakecloud_core::auth::PrincipalType::User,
        source_identity: None,
        tags: None,
    });
    req
}

async fn json_of(svc: &KmsService, req: AwsRequest) -> Value {
    let resp = svc.handle(req).await.expect("request succeeds");
    serde_json::from_slice(resp.body.expect_bytes()).unwrap()
}

/// A key of another account, named by its key or alias ARN, is served from
/// that account: a ciphertext sealed by the other account's caller names
/// the key by its ARN, so that caller's `Decrypt` without a `KeyId` finds
/// it, and so does the key owner's. A bare key id stays the caller's own.
#[tokio::test]
async fn keys_of_another_account_are_used_by_their_arn() {
    let svc = make_service();
    let owner = "111111111111";
    let other = "222222222222";
    let key_arn = json_of(&svc, request_as(owner, "CreateKey", json!({}))).await["KeyMetadata"]
        ["Arn"]
        .as_str()
        .unwrap()
        .to_string();
    svc.handle(request_as(
        owner,
        "CreateAlias",
        json!({"AliasName": "alias/shared", "TargetKeyId": key_arn}),
    ))
    .await
    .unwrap();
    let alias_arn = format!("arn:aws:kms:us-east-1:{owner}:alias/shared");
    let plaintext = base64::engine::general_purpose::STANDARD.encode(b"secret");
    for key_ref in [&key_arn, &alias_arn] {
        let sealed = json_of(
            &svc,
            request_as(
                other,
                "Encrypt",
                json!({"KeyId": key_ref, "Plaintext": plaintext, "EncryptionContext": {"k": "v"}}),
            ),
        )
        .await;
        assert_eq!(sealed["KeyId"], json!(key_arn));
        let blob = sealed["CiphertextBlob"].as_str().unwrap();
        assert_eq!(ciphertext_key_ref(blob).as_deref(), Some(key_arn.as_str()));
        for account in [other, owner] {
            let opened = json_of(
                &svc,
                request_as(
                    account,
                    "Decrypt",
                    json!({"CiphertextBlob": blob, "EncryptionContext": {"k": "v"}}),
                ),
            )
            .await;
            assert_eq!(opened["Plaintext"], json!(plaintext), "{account}");
            assert_eq!(opened["KeyId"], json!(key_arn));
        }
    }
    // DescribeKey and the grant operations take the key ARN.
    let described = json_of(
        &svc,
        request_as(other, "DescribeKey", json!({"KeyId": key_arn})),
    )
    .await;
    assert_eq!(described["KeyMetadata"]["Arn"], json!(key_arn));
    // A bare key id or alias name names the caller's own key.
    let key_id = key_arn.rsplit('/').next().unwrap();
    for key_ref in [key_id, "alias/shared"] {
        let err = svc
            .handle(request_as(other, "DescribeKey", json!({"KeyId": key_ref})))
            .await
            .err()
            .unwrap();
        assert_eq!(err.code(), "NotFoundException", "{key_ref}");
    }
    // Key management is never served across accounts.
    let err = svc
        .handle(request_as(other, "DisableKey", json!({"KeyId": key_arn})))
        .await
        .err()
        .unwrap();
    assert_eq!(err.code(), "NotFoundException");
}

/// The ARN of a new key of `account` created with `body`.
async fn key_arn_in(svc: &KmsService, account: &str, body: Value) -> String {
    json_of(svc, request_as(account, "CreateKey", body)).await["KeyMetadata"]["Arn"]
        .as_str()
        .unwrap()
        .to_string()
}

/// `ReEncrypt` takes a source and a destination key in different accounts
/// (the caller's and others', each named by its ARN): the ciphertext is
/// opened by the source key in its account and resealed under the
/// destination key in its, so the destination's account, or the caller,
/// decrypts it without naming a key; the same holds when neither key is the
/// caller's.
#[tokio::test]
async fn re_encrypt_moves_ciphertext_between_accounts() {
    let svc = make_service();
    let (a, b, c) = ("111111111111", "222222222222", "333333333333");
    let key_a = key_arn_in(&svc, a, json!({})).await;
    let key_b = key_arn_in(&svc, b, json!({})).await;
    let key_c = key_arn_in(&svc, c, json!({})).await;
    let plaintext = base64::engine::general_purpose::STANDARD.encode(b"moving");
    let decrypt = |account: &str, blob: &str, context: Value| {
        request_as(
            account,
            "Decrypt",
            json!({"CiphertextBlob": blob, "EncryptionContext": context}),
        )
    };

    // The caller's own key to another account's.
    let sealed = json_of(
        &svc,
        request_as(
            a,
            "Encrypt",
            json!({"KeyId": key_a, "Plaintext": plaintext, "EncryptionContext": {"s": "1"}}),
        ),
    )
    .await;
    let moved = json_of(
        &svc,
        request_as(
            a,
            "ReEncrypt",
            json!({
                "CiphertextBlob": sealed["CiphertextBlob"],
                "SourceEncryptionContext": {"s": "1"},
                "DestinationKeyId": key_b,
                "DestinationEncryptionContext": {"d": "2"},
            }),
        ),
    )
    .await;
    assert_eq!(moved["SourceKeyId"], json!(key_a));
    assert_eq!(moved["KeyId"], json!(key_b));
    let blob = moved["CiphertextBlob"].as_str().unwrap().to_string();
    assert_eq!(ciphertext_key_ref(&blob).as_deref(), Some(key_b.as_str()));
    for account in [a, b] {
        let opened = json_of(&svc, decrypt(account, &blob, json!({"d": "2"}))).await;
        assert_eq!(opened["Plaintext"], json!(plaintext), "{account}");
        assert_eq!(opened["KeyId"], json!(key_b));
    }

    // Another account's key back to the caller's own.
    let back = json_of(
        &svc,
        request_as(
            a,
            "ReEncrypt",
            json!({
                "CiphertextBlob": blob,
                "SourceKeyId": key_b,
                "SourceEncryptionContext": {"d": "2"},
                "DestinationKeyId": key_a,
            }),
        ),
    )
    .await;
    assert_eq!(back["SourceKeyId"], json!(key_b));
    let blob_a = back["CiphertextBlob"].as_str().unwrap().to_string();
    let opened = json_of(&svc, decrypt(a, &blob_a, json!({}))).await;
    assert_eq!(opened["Plaintext"], json!(plaintext));
    assert_eq!(opened["KeyId"], json!(key_a));

    // Between two accounts, neither the caller's.
    let in_b = json_of(
        &svc,
        request_as(
            a,
            "Encrypt",
            json!({"KeyId": key_b, "Plaintext": plaintext}),
        ),
    )
    .await;
    let to_c = json_of(
        &svc,
        request_as(
            a,
            "ReEncrypt",
            json!({"CiphertextBlob": in_b["CiphertextBlob"], "DestinationKeyId": key_c}),
        ),
    )
    .await;
    assert_eq!(to_c["SourceKeyId"], json!(key_b));
    assert_eq!(to_c["KeyId"], json!(key_c));
    let blob_c = to_c["CiphertextBlob"].as_str().unwrap();
    for account in [a, c] {
        let opened = json_of(&svc, decrypt(account, blob_c, json!({}))).await;
        assert_eq!(opened["Plaintext"], json!(plaintext), "{account}");
    }

    // A SourceKeyId naming another key than the one that sealed the
    // ciphertext is IncorrectKeyException.
    let err = svc
        .handle(request_as(
            a,
            "ReEncrypt",
            json!({
                "CiphertextBlob": in_b["CiphertextBlob"],
                "SourceKeyId": key_c,
                "DestinationKeyId": key_a,
            }),
        ))
        .await
        .err()
        .unwrap();
    assert_eq!(err.code(), "IncorrectKeyException");
}

/// A symmetric ciphertext names its key by ARN whoever sealed it, so one the
/// key's own account sealed (Encrypt, GenerateDataKey) is decrypted from
/// another account it is handed to without naming the key.
#[tokio::test]
async fn own_account_ciphertexts_open_from_other_accounts() {
    let svc = make_service();
    let (owner, other) = ("111111111111", "222222222222");
    let key = key_arn_in(&svc, owner, json!({})).await;
    let plaintext = base64::engine::general_purpose::STANDARD.encode(b"handed over");
    let sealed = json_of(
        &svc,
        request_as(
            owner,
            "Encrypt",
            json!({"KeyId": key, "Plaintext": plaintext}),
        ),
    )
    .await;
    let data_key = json_of(
        &svc,
        request_as(
            owner,
            "GenerateDataKey",
            json!({"KeyId": key, "KeySpec": "AES_256"}),
        ),
    )
    .await;
    let encrypted = json!(plaintext);
    for (blob, expected) in [
        (&sealed["CiphertextBlob"], &encrypted),
        (&data_key["CiphertextBlob"], &data_key["Plaintext"]),
    ] {
        let blob = blob.as_str().unwrap();
        assert_eq!(ciphertext_key_ref(blob).as_deref(), Some(key.as_str()));
        let opened = json_of(
            &svc,
            request_as(other, "Decrypt", json!({"CiphertextBlob": blob})),
        )
        .await;
        assert_eq!(&opened["Plaintext"], expected);
        assert_eq!(opened["KeyId"], json!(key));
    }
}

/// A symmetric key with imported material seals like any symmetric key: its
/// ciphertext names it, so a caller of another account decrypts what it
/// encrypted under the key without naming it, the encryption context binds,
/// and deleting the material leaves the key PendingImport, which refuses the
/// decrypt.
#[tokio::test]
async fn imported_material_ciphertexts_name_their_key_across_accounts() {
    let svc = make_service();
    let owner = "123456789012";
    let other = "222222222222";
    let key_id = create_key_with_opts(&svc, json!({ "Origin": "EXTERNAL" }));
    import_external_material(&svc, &key_id, b"my-secret-aes-key-material!12345");
    let key_arn = format!("arn:aws:kms:us-east-1:{owner}:key/{key_id}");
    let plaintext = base64::engine::general_purpose::STANDARD.encode(b"imported");
    let sealed = json_of(
        &svc,
        request_as(
            other,
            "Encrypt",
            json!({"KeyId": key_arn, "Plaintext": plaintext, "EncryptionContext": {"k": "v"}}),
        ),
    )
    .await;
    let blob = sealed["CiphertextBlob"].as_str().unwrap().to_string();
    assert_eq!(ciphertext_key_ref(&blob).as_deref(), Some(key_arn.as_str()));
    let decrypt = |context: Value| {
        request_as(
            other,
            "Decrypt",
            json!({"CiphertextBlob": blob, "EncryptionContext": context}),
        )
    };
    let opened = json_of(&svc, decrypt(json!({"k": "v"}))).await;
    assert_eq!(opened["Plaintext"], json!(plaintext));
    assert_eq!(opened["KeyId"], json!(key_arn));
    let err = svc.handle(decrypt(json!({}))).await.err().unwrap();
    assert_eq!(err.code(), "InvalidCiphertextException");

    svc.delete_imported_key_material(&make_request(
        "DeleteImportedKeyMaterial",
        json!({"KeyId": key_id}),
    ))
    .unwrap();
    let err = svc.handle(decrypt(json!({"k": "v"}))).await.err().unwrap();
    assert_eq!(err.code(), "KMSInvalidStateException");
}

/// An RSA ciphertext names no key: `Decrypt` and `ReEncrypt` open it only
/// with the key named (`KeyId` / `SourceKeyId`) and its algorithm, from the
/// key's own account or another; without the key it is
/// InvalidCiphertextException. `ReEncrypt` into an RSA key yields a raw
/// RSA-OAEP ciphertext of that key.
#[tokio::test]
async fn rsa_ciphertexts_are_opened_by_the_key_named() {
    let svc = make_service();
    let (owner, other) = ("111111111111", "222222222222");
    let rsa = key_arn_in(
        &svc,
        owner,
        json!({"KeySpec": "RSA_2048", "KeyUsage": "ENCRYPT_DECRYPT"}),
    )
    .await;
    let symmetric = key_arn_in(&svc, other, json!({})).await;
    let plaintext = base64::engine::general_purpose::STANDARD.encode(b"asymmetric");
    let alg = "RSAES_OAEP_SHA_1";
    let sealed = json_of(
        &svc,
        request_as(
            other,
            "Encrypt",
            json!({"KeyId": rsa, "Plaintext": plaintext, "EncryptionAlgorithm": alg}),
        ),
    )
    .await;
    let blob = sealed["CiphertextBlob"].as_str().unwrap().to_string();
    assert_eq!(ciphertext_key_ref(&blob), None);

    for account in [other, owner] {
        let opened = json_of(
            &svc,
            request_as(
                account,
                "Decrypt",
                json!({"CiphertextBlob": blob, "KeyId": rsa, "EncryptionAlgorithm": alg}),
            ),
        )
        .await;
        assert_eq!(opened["Plaintext"], json!(plaintext), "{account}");
        assert_eq!(opened["KeyId"], json!(rsa));
        assert_eq!(opened["EncryptionAlgorithm"], json!(alg));
        let err = svc
            .handle(request_as(
                account,
                "Decrypt",
                json!({"CiphertextBlob": blob, "EncryptionAlgorithm": alg}),
            ))
            .await
            .err()
            .unwrap();
        assert_eq!(err.code(), "InvalidCiphertextException", "{account}");
    }

    let err = svc
        .handle(request_as(
            other,
            "ReEncrypt",
            json!({
                "CiphertextBlob": blob,
                "SourceEncryptionAlgorithm": alg,
                "DestinationKeyId": symmetric,
            }),
        ))
        .await
        .err()
        .unwrap();
    assert_eq!(err.code(), "InvalidCiphertextException");
    let moved = json_of(
        &svc,
        request_as(
            other,
            "ReEncrypt",
            json!({
                "CiphertextBlob": blob,
                "SourceKeyId": rsa,
                "SourceEncryptionAlgorithm": alg,
                "DestinationKeyId": symmetric,
            }),
        ),
    )
    .await;
    assert_eq!(moved["SourceKeyId"], json!(rsa));
    assert_eq!(moved["SourceEncryptionAlgorithm"], json!(alg));
    let opened = json_of(
        &svc,
        request_as(
            other,
            "Decrypt",
            json!({"CiphertextBlob": moved["CiphertextBlob"]}),
        ),
    )
    .await;
    assert_eq!(opened["Plaintext"], json!(plaintext));

    // Back into the RSA key: a raw RSA-OAEP ciphertext of it.
    let back = json_of(
        &svc,
        request_as(
            other,
            "ReEncrypt",
            json!({
                "CiphertextBlob": moved["CiphertextBlob"],
                "DestinationKeyId": rsa,
                "DestinationEncryptionAlgorithm": "RSAES_OAEP_SHA_256",
            }),
        ),
    )
    .await;
    let raw = base64::engine::general_purpose::STANDARD
        .decode(back["CiphertextBlob"].as_str().unwrap())
        .unwrap();
    assert_eq!(raw.len(), 256);
    let opened = json_of(
        &svc,
        request_as(
            owner,
            "Decrypt",
            json!({
                "CiphertextBlob": back["CiphertextBlob"],
                "KeyId": rsa,
                "EncryptionAlgorithm": "RSAES_OAEP_SHA_256",
            }),
        ),
    )
    .await;
    assert_eq!(opened["Plaintext"], json!(plaintext));
}

/// `ReEncrypt` is authorized as `kms:ReEncryptFrom` on the source key with
/// the source context and `kms:ReEncryptTo` on the destination key with the
/// destination context; a `Decrypt` naming no key is authorized on the key
/// its ciphertext names.
#[test]
fn re_encrypt_and_decrypt_authorize_the_keys_they_use() {
    let svc = make_service();
    let source = create_key(&svc);
    let destination = create_key(&svc);
    let sealed = svc
        .encrypt(&make_request(
            "Encrypt",
            json!({"KeyId": source, "Plaintext": "aGk=", "EncryptionContext": {"s": "1"}}),
        ))
        .unwrap();
    let sealed: Value = serde_json::from_slice(sealed.body.expect_bytes()).unwrap();
    let blob = sealed["CiphertextBlob"].as_str().unwrap();
    let arn_of = |id: &str| format!("arn:aws:kms:us-east-1:123456789012:key/{id}");

    let decrypt = make_request("Decrypt", json!({"CiphertextBlob": blob}));
    assert_eq!(
        svc.iam_action_for(&decrypt).unwrap().resource,
        arn_of(&source)
    );

    let req = make_request(
        "ReEncrypt",
        json!({
            "CiphertextBlob": blob,
            "SourceEncryptionContext": {"s": "1"},
            "DestinationKeyId": destination,
            "DestinationEncryptionContext": {"d": "2"},
        }),
    );
    let actions = svc.iam_actions_for(&req);
    let shape: Vec<(&str, String)> = actions
        .iter()
        .map(|a| (a.action, a.resource.clone()))
        .collect();
    assert_eq!(
        shape,
        vec![
            ("ReEncryptFrom", arn_of(&source)),
            ("ReEncryptTo", arn_of(&destination)),
        ]
    );
    let from = svc.iam_condition_keys_for(&req, &actions[0]);
    assert_eq!(from["kms:encryptioncontext:s"], vec!["1".to_string()]);
    assert!(!from.contains_key("kms:encryptioncontext:d"));
    let to = svc.iam_condition_keys_for(&req, &actions[1]);
    assert_eq!(to["kms:encryptioncontext:d"], vec!["2".to_string()]);
    assert!(!to.contains_key("kms:encryptioncontext:s"));
}

/// `CreateGrant` takes exactly one of `GranteePrincipal` and
/// `GranteeServicePrincipal`, at most one retiring principal of either kind,
/// and a grant to a service principal needs a `SourceArn` constraint and a
/// retiring principal. `ListGrants` shows a service grantee as
/// `GranteeServicePrincipal` and filters by either grantee kind (not both);
/// `ListRetirableGrants` lists by `RetiringServicePrincipal` too.
#[test]
fn grants_to_service_principals() {
    let svc = make_service();
    let key = create_key(&svc);
    let user = "arn:aws:iam::123456789012:user/u";
    let source = json!({"SourceArn": "arn:aws:ses:us-east-1:123456789012:receipt-rule-set/s"});
    let create = |extra: Value| {
        let mut body = json!({"KeyId": key, "Operations": ["GenerateDataKey"]});
        body.as_object_mut()
            .unwrap()
            .extend(extra.as_object().unwrap().clone());
        svc.create_grant(&make_request("CreateGrant", body))
    };
    let refused = |extra: Value| {
        let err = create(extra.clone()).err().expect("refused");
        assert_eq!(err.code(), "ValidationException", "{extra}");
    };
    refused(json!({}));
    refused(json!({"GranteePrincipal": user, "GranteeServicePrincipal": "ses.amazonaws.com"}));
    refused(json!({
        "GranteePrincipal": user,
        "RetiringPrincipal": user,
        "RetiringServicePrincipal": "ses.amazonaws.com",
    }));
    refused(json!({"GranteeServicePrincipal": "ses.amazonaws.com", "RetiringPrincipal": user}));
    refused(json!({"GranteeServicePrincipal": "ses.amazonaws.com", "Constraints": source}));

    let service_grant = body_json(
        create(json!({
            "GranteeServicePrincipal": "ses.amazonaws.com",
            "RetiringServicePrincipal": "ses.amazonaws.com",
            "Constraints": source,
        }))
        .unwrap(),
    )["GrantId"]
        .clone();
    let user_grant =
        body_json(create(json!({"GranteePrincipal": user})).unwrap())["GrantId"].clone();

    let list = |filter: Value| {
        let mut body = json!({"KeyId": key});
        body.as_object_mut()
            .unwrap()
            .extend(filter.as_object().unwrap().clone());
        svc.list_grants(&make_request("ListGrants", body))
    };
    let all = body_json(list(json!({})).unwrap());
    let grants = all["Grants"].as_array().unwrap();
    let by_id = |id: &Value| grants.iter().find(|g| g["GrantId"] == *id).unwrap().clone();
    let service = by_id(&service_grant);
    assert_eq!(service["GranteeServicePrincipal"], "ses.amazonaws.com");
    assert_eq!(service["RetiringServicePrincipal"], "ses.amazonaws.com");
    assert!(service.get("GranteePrincipal").is_none());
    assert_eq!(service["Constraints"], source);
    assert_eq!(by_id(&user_grant)["GranteePrincipal"], user);

    let ids = |filter: Value| -> Vec<Value> {
        body_json(list(filter).unwrap())["Grants"]
            .as_array()
            .unwrap()
            .iter()
            .map(|g| g["GrantId"].clone())
            .collect()
    };
    assert_eq!(
        ids(json!({"GranteeServicePrincipal": "ses.amazonaws.com"})),
        vec![service_grant.clone()]
    );
    assert_eq!(ids(json!({"GranteePrincipal": user})), vec![user_grant]);
    assert_eq!(
        list(json!({"GranteePrincipal": user, "GranteeServicePrincipal": "ses.amazonaws.com"}))
            .err()
            .unwrap()
            .code(),
        "ValidationException"
    );

    let retirable = body_json(
        svc.list_retirable_grants(&make_request(
            "ListRetirableGrants",
            json!({"RetiringServicePrincipal": "ses.amazonaws.com"}),
        ))
        .unwrap(),
    );
    let retirable: Vec<&Value> = retirable["Grants"]
        .as_array()
        .unwrap()
        .iter()
        .map(|g| &g["GrantId"])
        .collect();
    assert_eq!(retirable, vec![&service_grant]);
    for bad in [String::new(), "s".repeat(129)] {
        let err = svc
            .list_retirable_grants(&make_request(
                "ListRetirableGrants",
                json!({"RetiringServicePrincipal": bad}),
            ))
            .err()
            .unwrap();
        assert_eq!(err.code(), "InvalidArnException");
    }
}

/// Encryption-context grant constraints are refused for an operation that
/// takes no encryption context (DescribeKey, RetireGrant and CreateGrant
/// excepted) and on a key other than a symmetric encryption key.
#[test]
fn create_grant_refuses_context_constraints_on_operations_without_a_context() {
    let svc = make_service();
    let symmetric = create_key(&svc);
    let create = |key: &str, operations: Value, constraints: Value| {
        svc.create_grant(&make_request(
            "CreateGrant",
            json!({
                "KeyId": key,
                "GranteePrincipal": "arn:aws:iam::123456789012:user/u",
                "Operations": operations,
                "Constraints": constraints,
            }),
        ))
    };
    let subset = json!({"EncryptionContextSubset": {"k": "v"}});
    let equals = json!({"EncryptionContextEquals": {"k": "v"}});
    for constraints in [&subset, &equals] {
        create(
            &symmetric,
            json!([
                "Decrypt",
                "Encrypt",
                "DescribeKey",
                "RetireGrant",
                "CreateGrant"
            ]),
            constraints.clone(),
        )
        .expect("context operations plus DescribeKey/RetireGrant/CreateGrant");
        for op in ["GetPublicKey", "Sign", "GenerateMac"] {
            let err = create(&symmetric, json!(["Decrypt", op]), constraints.clone())
                .err()
                .unwrap();
            assert_eq!(err.code(), "ValidationException", "{op}");
        }
    }
    // A SourceArn constraint alone is not an encryption-context constraint.
    create(
        &symmetric,
        json!(["Sign"]),
        json!({"SourceArn": "arn:aws:s3:::bucket"}),
    )
    .expect("SourceArn constraint");
    // Constraints on an asymmetric key are refused whatever the operations.
    let rsa = create_key_with_opts(
        &svc,
        json!({"KeySpec": "RSA_2048", "KeyUsage": "ENCRYPT_DECRYPT"}),
    );
    let err = create(&rsa, json!(["Decrypt"]), subset).err().unwrap();
    assert_eq!(err.code(), "ValidationException");
    create(&rsa, json!(["Decrypt"]), Value::Null).expect("an unconstrained grant");
}

/// A bare key id names the key in the request's region, so the replica of a
/// multi-Region key in its region, and a multi-Region ciphertext is opened
/// (and authorized, its grants included) as the related key of the region
/// of the Decrypt call; a non-multi-Region key and the primary region are
/// unaffected.
#[test]
fn multi_region_bare_ids_and_ciphertexts_resolve_in_the_call_region() {
    use fakecloud_core::auth::{GrantAuthorization, IamAction, ResourcePolicyProvider};
    let svc = make_service();
    let key_id = create_key_with_opts(&svc, json!({ "MultiRegion": true }));
    svc.replicate_key(&make_request(
        "ReplicateKey",
        json!({"KeyId": key_id, "ReplicaRegion": "us-west-2"}),
    ))
    .unwrap();
    let primary_arn = format!("arn:aws:kms:us-east-1:123456789012:key/{key_id}");
    let replica_arn = format!("arn:aws:kms:us-west-2:123456789012:key/{key_id}");
    let in_region = |region: &str, action: &str, body: Value| {
        let mut req = make_request(action, body);
        req.region = region.to_string();
        req
    };
    let json_of =
        |resp: AwsResponse| -> Value { serde_json::from_slice(resp.body.expect_bytes()).unwrap() };

    // Encrypt with the bare id in the replica's region uses the replica.
    let sealed = json_of(
        svc.encrypt(&in_region(
            "us-west-2",
            "Encrypt",
            json!({"KeyId": key_id, "Plaintext": "aGk="}),
        ))
        .unwrap(),
    );
    assert_eq!(sealed["KeyId"], json!(replica_arn));
    let blob = sealed["CiphertextBlob"].as_str().unwrap().to_string();
    // Decrypting names the related key of the call's region, and so does
    // the resource IAM authorizes.
    for (region, arn) in [("us-west-2", &replica_arn), ("us-east-1", &primary_arn)] {
        let req = in_region(region, "Decrypt", json!({"CiphertextBlob": blob}));
        assert_eq!(&svc.iam_action_for(&req).unwrap().resource, arn, "{region}");
        let opened = json_of(svc.decrypt(&req).unwrap());
        assert_eq!(&opened["KeyId"], &json!(arn), "{region}");
    }

    // A grant created with the bare id in the replica's region is the
    // replica's: it authorizes a Decrypt in that region, not in the
    // primary's, and a grant on the primary does not cover the replica.
    let grantee = "arn:aws:iam::123456789012:user/grantee";
    let grant = |region: &str| {
        svc.create_grant(&in_region(
            region,
            "CreateGrant",
            json!({"KeyId": key_id, "GranteePrincipal": grantee, "Operations": ["Decrypt"]}),
        ))
        .unwrap();
    };
    let provider = crate::resource_policy::KmsResourcePolicyProvider::new(svc.state.clone());
    let principal = fakecloud_core::auth::Principal {
        arn: grantee.to_string(),
        user_id: "AIDAGRANTEE".to_string(),
        account_id: "123456789012".to_string(),
        principal_type: fakecloud_core::auth::PrincipalType::User,
        source_identity: None,
        tags: None,
    };
    let granted = |region: &str| {
        let req = in_region(region, "Decrypt", json!({"CiphertextBlob": blob}));
        let action: IamAction = svc.iam_action_for(&req).unwrap();
        provider.grant_authorization("kms", &req, &action, &principal)
    };
    grant("us-east-1");
    assert_eq!(granted("us-east-1"), Some(GrantAuthorization::Grantee));
    assert_eq!(granted("us-west-2"), None, "the primary's grant");
    grant("us-west-2");
    assert_eq!(granted("us-west-2"), Some(GrantAuthorization::Grantee));

    // A key that is not multi-Region resolves by its bare id as before.
    let plain = create_key(&svc);
    let described = json_of(
        svc.describe_key(&in_region(
            "us-east-1",
            "DescribeKey",
            json!({"KeyId": plain}),
        ))
        .unwrap(),
    );
    assert!(described["KeyMetadata"]["Arn"]
        .as_str()
        .unwrap()
        .ends_with(&plain));
}

/// ListGrants and ListRetirableGrants name each grant's key by its ARN, as
/// AWS does: a replica's grant by the replica's ARN, never the internal
/// "<region>:<id>" it is stored under.
#[test]
fn listed_grants_name_their_key_by_its_arn() {
    let svc = make_service();
    let key_id = create_key_with_opts(&svc, json!({ "MultiRegion": true }));
    svc.replicate_key(&make_request(
        "ReplicateKey",
        json!({"KeyId": key_id, "ReplicaRegion": "us-west-2"}),
    ))
    .unwrap();
    let in_region = |region: &str, action: &str, body: Value| {
        let mut req = make_request(action, body);
        req.region = region.to_string();
        req
    };
    let retiring = "arn:aws:iam::123456789012:user/retirer";
    for region in ["us-east-1", "us-west-2"] {
        svc.create_grant(&in_region(
            region,
            "CreateGrant",
            json!({
                "KeyId": key_id,
                "GranteePrincipal": "arn:aws:iam::123456789012:user/u",
                "RetiringPrincipal": retiring,
                "Operations": ["Decrypt"],
            }),
        ))
        .unwrap();
    }
    let arn_in = |region: &str| format!("arn:aws:kms:{region}:123456789012:key/{key_id}");
    for region in ["us-east-1", "us-west-2"] {
        let listed: Value = serde_json::from_slice(
            svc.list_grants(&in_region(region, "ListGrants", json!({"KeyId": key_id})))
                .unwrap()
                .body
                .expect_bytes(),
        )
        .unwrap();
        let ids: Vec<&str> = listed["Grants"]
            .as_array()
            .unwrap()
            .iter()
            .map(|g| g["KeyId"].as_str().unwrap())
            .collect();
        assert_eq!(ids, vec![arn_in(region).as_str()], "{region}");
    }
    let retirable: Value = serde_json::from_slice(
        svc.list_retirable_grants(&make_request(
            "ListRetirableGrants",
            json!({"RetiringPrincipal": retiring}),
        ))
        .unwrap()
        .body
        .expect_bytes(),
    )
    .unwrap();
    let mut ids: Vec<String> = retirable["Grants"]
        .as_array()
        .unwrap()
        .iter()
        .map(|g| g["KeyId"].as_str().unwrap().to_string())
        .collect();
    ids.sort();
    assert_eq!(ids, vec![arn_in("us-east-1"), arn_in("us-west-2")]);
}
