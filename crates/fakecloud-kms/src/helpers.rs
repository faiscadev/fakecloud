use super::*;

/// Canonicalize a KMS `EncryptionContext` (a JSON string-string map)
/// into a deterministic byte string for use as AEAD AAD. Keys are
/// sorted via `BTreeMap`'s ordering and the result is JSON-encoded
/// without whitespace. An empty EC produces zero bytes so blobs
/// encoded without EC stay byte-compatible with the original AAD shape.
pub(crate) fn canonical_encryption_context(value: &serde_json::Value) -> Vec<u8> {
    let Some(obj) = value.as_object() else {
        return Vec::new();
    };
    if obj.is_empty() {
        return Vec::new();
    }
    let sorted: std::collections::BTreeMap<&str, String> = obj
        .iter()
        .filter_map(|(k, v)| v.as_str().map(|s| (k.as_str(), s.to_string())))
        .collect();
    serde_json::to_vec(&sorted).unwrap_or_default()
}

/// Decode a FakeCloud KMS ciphertext envelope back into its plaintext.
///
/// Ciphertexts come in two flavours: `fakecloud-kms:<key-id>:<b64>`
/// stores the plaintext directly (we're a simulator, not a real KMS),
/// and `fakecloud-imported:<key-id>:<b64>` stores bytes XOR'd with
/// caller-provided imported key material which we un-XOR here. Both
/// flavours look up the source key to return its ARN, so `Decrypt`
/// and `ReEncrypt` can surface the same `KeyId` / `SourceKeyId` the
/// real service does.
///
/// `encryption_context_aad` is the canonicalized EC bytes
/// (see [`canonical_encryption_context`]). When the supplied EC
/// doesn't match what was bound at encrypt time, `crate::blob::decode_with_context`
/// fails AEAD verification and we surface `InvalidCiphertextException`.
pub(crate) fn decode_ciphertext_envelope(
    state: &KmsState,
    ciphertext_b64: &str,
    encryption_context_aad: &[u8],
) -> Result<DecodedCiphertext, AwsServiceError> {
    let ciphertext_bytes = base64::engine::general_purpose::STANDARD
        .decode(ciphertext_b64)
        .map_err(|_| invalid_ciphertext())?;

    // Modern AWS-shaped blob (AES-256-GCM under the per-account master
    // key) — try this first. Older textual envelopes fall through.
    if let Some(decoded) = crate::blob::decode_with_context(
        &state.master_key_bytes,
        &ciphertext_bytes,
        encryption_context_aad,
    ) {
        let key = state.keys.get(&decoded.key_id).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "NotFoundException",
                format!("Key '{}' does not exist", decoded.key_id),
            )
        })?;
        return Ok(DecodedCiphertext {
            source_arn: key.arn.clone(),
            plaintext_b64: base64::engine::general_purpose::STANDARD.encode(&decoded.plaintext),
            encryption_algorithm: "SYMMETRIC_DEFAULT".to_string(),
        });
    }

    // Legacy textual envelopes: `fakecloud-kms:<key>:<b64>` and
    // `fakecloud-imported:<key>:<xored-b64>`. Kept for back-compat with
    // ciphertexts persisted before the binary blob format shipped.
    let envelope = String::from_utf8(ciphertext_bytes).map_err(|_| invalid_ciphertext())?;

    if let Some(rest) = envelope.strip_prefix(IMPORTED_ENVELOPE_PREFIX) {
        let (key_id, xored_b64) = rest.split_once(':').ok_or_else(invalid_ciphertext)?;
        let xored_bytes = base64::engine::general_purpose::STANDARD
            .decode(xored_b64)
            .map_err(|_| invalid_ciphertext())?;

        let key = state.keys.get(key_id).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "NotFoundException",
                format!("Key '{key_id}' does not exist"),
            )
        })?;

        let material = key.imported_material_bytes.as_ref().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidCiphertextException",
                "Key material has been deleted",
            )
        })?;

        let plaintext_bytes: Vec<u8> = xored_bytes
            .iter()
            .enumerate()
            .map(|(i, b)| b ^ material[i % material.len()])
            .collect();
        return Ok(DecodedCiphertext {
            source_arn: key.arn.clone(),
            plaintext_b64: base64::engine::general_purpose::STANDARD.encode(&plaintext_bytes),
            encryption_algorithm: "SYMMETRIC_DEFAULT".to_string(),
        });
    }

    if let Some(rest) = envelope.strip_prefix(FAKE_ENVELOPE_PREFIX) {
        let (key_id, plaintext_b64) = rest.split_once(':').ok_or_else(invalid_ciphertext)?;
        let key = state.keys.get(key_id).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "NotFoundException",
                format!("Key '{key_id}' does not exist"),
            )
        })?;
        return Ok(DecodedCiphertext {
            source_arn: key.arn.clone(),
            plaintext_b64: plaintext_b64.to_string(),
            encryption_algorithm: "SYMMETRIC_DEFAULT".to_string(),
        });
    }

    // Asymmetric RSA ciphertext produced by Encrypt against an RSA
    // ENCRYPT_DECRYPT key: `fakecloud-rsa:<key_id>:<algorithm>:<b64>`. Reverse
    // it with the key's private half (bug-audit 2026-06-20, 1.11).
    if let Some(rest) = envelope.strip_prefix(RSA_ENVELOPE_PREFIX) {
        let mut parts = rest.splitn(3, ':');
        let key_id = parts.next().ok_or_else(invalid_ciphertext)?;
        let algorithm = parts.next().ok_or_else(invalid_ciphertext)?;
        let raw_b64 = parts.next().ok_or_else(invalid_ciphertext)?;
        let ciphertext = base64::engine::general_purpose::STANDARD
            .decode(raw_b64)
            .map_err(|_| invalid_ciphertext())?;
        let key = state.keys.get(key_id).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "NotFoundException",
                format!("Key '{key_id}' does not exist"),
            )
        })?;
        let priv_der = key
            .asymmetric_private_key_der
            .as_ref()
            .ok_or_else(invalid_ciphertext)?;
        let plaintext = super::asym::rsa_oaep_unwrap(priv_der, algorithm, &ciphertext)
            .map_err(|_| invalid_ciphertext())?;
        return Ok(DecodedCiphertext {
            source_arn: key.arn.clone(),
            plaintext_b64: base64::engine::general_purpose::STANDARD.encode(&plaintext),
            encryption_algorithm: algorithm.to_string(),
        });
    }

    Err(AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        "InvalidCiphertextException",
        "The ciphertext is not a valid FakeCloud KMS ciphertext",
    ))
}

pub(crate) fn invalid_ciphertext() -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        "InvalidCiphertextException",
        "The ciphertext is invalid",
    )
}

/// Actions that mutate KMS state.
pub(crate) fn is_mutating_action(action: &str) -> bool {
    matches!(
        action,
        "CreateKey"
            | "EnableKey"
            | "DisableKey"
            | "ScheduleKeyDeletion"
            | "CancelKeyDeletion"
            | "CreateAlias"
            | "DeleteAlias"
            | "UpdateAlias"
            | "TagResource"
            | "UntagResource"
            | "UpdateKeyDescription"
            | "PutKeyPolicy"
            | "EnableKeyRotation"
            | "DisableKeyRotation"
            | "RotateKeyOnDemand"
            | "CreateGrant"
            | "RevokeGrant"
            | "RetireGrant"
            | "ReplicateKey"
            | "ImportKeyMaterial"
            | "DeleteImportedKeyMaterial"
            | "UpdatePrimaryRegion"
            | "CreateCustomKeyStore"
            | "DeleteCustomKeyStore"
            | "ConnectCustomKeyStore"
            | "DisconnectCustomKeyStore"
            | "UpdateCustomKeyStore"
    )
}

/// Derive the resource ARN for a KMS IAM action.
///
/// Key-targeted operations resolve their `KeyId` parameter (which may
/// be a UUID, ARN, or alias) to the key's canonical ARN. Operations
/// that don't target a specific key (CreateKey, ListKeys, etc.) use `*`.
pub(crate) fn kms_resource_for(
    action: &str,
    state: &SharedKmsState,
    request: &AwsRequest,
) -> String {
    // Operations that don't target a specific key.
    if matches!(
        action,
        "CreateKey"
            | "ListKeys"
            | "ListAliases"
            | "GenerateRandom"
            | "ListRetirableGrants"
            | "CreateCustomKeyStore"
            | "DeleteCustomKeyStore"
            | "DescribeCustomKeyStores"
            | "ConnectCustomKeyStore"
            | "DisconnectCustomKeyStore"
            | "UpdateCustomKeyStore"
    ) {
        return "*".to_string();
    }

    // Alias-targeted operations carry an AliasName instead of KeyId.
    if matches!(action, "CreateAlias" | "DeleteAlias" | "UpdateAlias") {
        let body = request.json_body();
        // Resolve alias -> key ARN when possible.
        if let Some(alias_name) = body["AliasName"].as_str() {
            let accts = state.read();
            let empty = KmsState::new(&request.account_id, &request.region);
            let s = accts.get(&request.account_id).unwrap_or(&empty);
            if let Some(alias) = s.aliases.get(alias_name) {
                if let Some(key) = s.keys.get(&alias.target_key_id) {
                    return key.arn.clone();
                }
            }
            // For CreateAlias the target may be in TargetKeyId.
            if let Some(target) = body["TargetKeyId"].as_str() {
                if let Some(key_id) = KmsService::resolve_key_id_with_state(s, target) {
                    if let Some(key) = s.keys.get(&key_id) {
                        return key.arn.clone();
                    }
                }
            }
        }
        return "*".to_string();
    }

    // All remaining operations carry a KeyId parameter.
    let body = request.json_body();
    if let Some(key_id_input) = body["KeyId"].as_str() {
        let accts = state.read();
        let empty = KmsState::new(&request.account_id, &request.region);
        let s = accts.get(&request.account_id).unwrap_or(&empty);
        if let Some(key_id) = KmsService::resolve_key_id_with_state(s, key_id_input) {
            if let Some(key) = s.keys.get(&key_id) {
                return key.arn.clone();
            }
        }
    }
    // Key not found or no KeyId — fall back to wildcard. The handler
    // will return NotFoundException anyway; this avoids blocking the
    // request at the IAM layer with a confusing error.
    "*".to_string()
}

pub(crate) fn default_key_policy(account_id: &str) -> String {
    serde_json::to_string(&json!({
        "Version": "2012-10-17",
        "Id": "key-default-1",
        "Statement": [
            {
                "Sid": "Enable IAM User Permissions",
                "Effect": "Allow",
                "Principal": {"AWS": Arn::global("iam", account_id, "root").to_string()},
                "Action": "kms:*",
                "Resource": "*",
            }
        ],
    }))
    .unwrap()
}

pub(crate) fn signing_algorithms_for_key_spec(key_spec: &str) -> Option<Vec<String>> {
    match key_spec {
        "RSA_2048" | "RSA_3072" | "RSA_4096" => Some(vec![
            "RSASSA_PKCS1_V1_5_SHA_256".into(),
            "RSASSA_PKCS1_V1_5_SHA_384".into(),
            "RSASSA_PKCS1_V1_5_SHA_512".into(),
            "RSASSA_PSS_SHA_256".into(),
            "RSASSA_PSS_SHA_384".into(),
            "RSASSA_PSS_SHA_512".into(),
        ]),
        "ECC_NIST_P256" | "ECC_SECG_P256K1" => Some(vec!["ECDSA_SHA_256".into()]),
        "ECC_NIST_P384" => Some(vec!["ECDSA_SHA_384".into()]),
        "ECC_NIST_P521" => Some(vec!["ECDSA_SHA_512".into()]),
        _ => None,
    }
}

/// Map a generic `ValidationException` (which KMS's Smithy contract never
/// declares) onto the operation-appropriate KMS error code. All other errors
/// pass through unchanged so callers don't accidentally lose more specific
/// failures.
pub(crate) fn recode_validation(err: AwsServiceError, target_code: &str) -> AwsServiceError {
    if let AwsServiceError::AwsError {
        status,
        code,
        message,
        extra_fields,
        headers,
    } = err
    {
        if code == "ValidationException" {
            return AwsServiceError::AwsError {
                status,
                code: target_code.to_string(),
                message,
                extra_fields,
                headers,
            };
        }
        return AwsServiceError::AwsError {
            status,
            code,
            message,
            extra_fields,
            headers,
        };
    }
    err
}

/// Run a validator chain and re-code any resulting `ValidationException` to
/// the supplied KMS-native code. Use when an operation's Smithy contract
/// lacks `ValidationException` (which is most of KMS).
pub(crate) fn recoded<F: FnOnce() -> Result<(), AwsServiceError>>(
    code: &str,
    f: F,
) -> Result<(), AwsServiceError> {
    f().map_err(|e| recode_validation(e, code))
}

pub(crate) fn require_string_field(body: &Value, field: &str) -> Result<String, AwsServiceError> {
    body[field].as_str().map(|s| s.to_string()).ok_or_else(|| {
        AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "ValidationException",
            format!("{field} is required"),
        )
    })
}

pub(crate) fn validate_alias_name(alias_name: &str) -> Result<(), AwsServiceError> {
    if !alias_name.starts_with("alias/") {
        return Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "ValidationException",
            "Invalid identifier",
        ));
    }
    if alias_name.starts_with("alias/aws/") {
        return Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "NotAuthorizedException",
            "",
        ));
    }
    let alias_suffix = &alias_name["alias/".len()..];
    if alias_suffix.contains(':') {
        return Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "ValidationException",
            format!("{alias_name} contains invalid characters for an alias"),
        ));
    }
    let valid_chars = alias_name
        .chars()
        .all(|c| c.is_alphanumeric() || c == '/' || c == '_' || c == '-' || c == ':');
    if !valid_chars {
        return Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "ValidationException",
            format!(
                "1 validation error detected: Value '{alias_name}' at 'aliasName' failed to satisfy constraint: Member must satisfy regular expression pattern: ^[a-zA-Z0-9:/_-]+$"
            ),
        ));
    }
    Ok(())
}

pub(crate) fn validate_alias_target(target_key_id: &str) -> Result<(), AwsServiceError> {
    if target_key_id.starts_with("alias/") {
        return Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "ValidationException",
            "Aliases must refer to keys. Not aliases",
        ));
    }
    Ok(())
}

/// Decode + length-check an Encrypt plaintext: must be 1..=4096 bytes.
pub(crate) fn decode_plaintext(plaintext_b64: &str) -> Result<Vec<u8>, AwsServiceError> {
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(plaintext_b64)
        .unwrap_or_default();
    if bytes.is_empty() {
        return Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "ValidationException",
            "1 validation error detected: Value at 'plaintext' failed to satisfy constraint: Member must have length greater than or equal to 1",
        ));
    }
    if bytes.len() > 4096 {
        return Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "ValidationException",
            "1 validation error detected: Value at 'plaintext' failed to satisfy constraint: Member must have length less than or equal to 4096",
        ));
    }
    Ok(bytes)
}

/// Build the base64-encoded ciphertext envelope for `Encrypt`. Two
/// shapes: when imported key material is present, XOR the plaintext
/// against the material so the ciphertext is deterministic in the
/// imported key; otherwise the fakecloud envelope just wraps the
/// original base64 plaintext under a fixed prefix.
pub(crate) fn build_encrypt_ciphertext(
    state: &KmsState,
    key: &KmsKey,
    plaintext_b64: &str,
    plaintext_bytes: &[u8],
    encryption_context_aad: &[u8],
) -> String {
    let _ = plaintext_b64;
    if let Some(ref material) = key.imported_material_bytes {
        // Imported key material path: legacy XOR envelope wrapped in the
        // textual `fakecloud-imported:<key>:<b64>` form, base64-encoded for
        // wire transport. This format is preserved for back-compat with
        // snapshots and external callers that may already have stored
        // ciphertexts produced before the AES-GCM blob format landed.
        let xored: Vec<u8> = plaintext_bytes
            .iter()
            .enumerate()
            .map(|(i, b)| b ^ material[i % material.len()])
            .collect();
        let xored_b64 = base64::engine::general_purpose::STANDARD.encode(&xored);
        let envelope = format!("fakecloud-imported:{}:{xored_b64}", key.key_id);
        return base64::engine::general_purpose::STANDARD.encode(envelope.as_bytes());
    }
    // Default path: AWS-shaped binary blob with AES-256-GCM under the
    // per-account master key persisted in `KmsState`. Round-trips through
    // real SDKs and does not leak plaintext to anyone inspecting the bytes.
    let blob = crate::blob::encode_with_context(
        &state.master_key_bytes,
        &key.key_id,
        plaintext_bytes,
        encryption_context_aad,
    );
    base64::engine::general_purpose::STANDARD.encode(&blob)
}

/// Reject empty/undecodable base64 for `Verify`'s Message and Signature.
/// Verify's Smithy contract declares KMSInvalidSignatureException for
/// signature problems and InvalidKeyUsageException for usage / message
/// problems, but not ValidationException — so we lean on the latter
/// because both Message and Signature are key-usage-shape concerns.
pub(crate) fn require_non_empty_b64(field: &str, b64: &str) -> Result<(), AwsServiceError> {
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(b64)
        .unwrap_or_default();
    if bytes.is_empty() {
        return Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "InvalidKeyUsageException",
            format!(
                "1 validation error detected: Value at '{field}' failed to satisfy constraint: Member must have length greater than or equal to 1"
            ),
        ));
    }
    Ok(())
}

/// Reject the request when the key isn't usable for crypto operations.
/// AWS surfaces `KMSInvalidStateException` for every state other than
/// `Enabled`; `Disabled` keeps its dedicated `DisabledException` for
/// backwards compatibility with the old enabled-flag check that the
/// existing tests assert on. All other lifecycle states
/// (`PendingDeletion`, `PendingImport`, `Unavailable`, `Updating`,
/// `Creating`) collapse into `KMSInvalidStateException` so callers
/// don't see a misleading success when the key is mid-transition.
pub(crate) fn require_usable_key_state(key: &KmsKey) -> Result<(), AwsServiceError> {
    if key.key_state == "Enabled" {
        return Ok(());
    }
    if key.key_state == "Disabled" || !key.enabled {
        return Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "DisabledException",
            format!("Key '{}' is disabled", key.arn),
        ));
    }
    Err(AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        "KMSInvalidStateException",
        format!(
            "Key '{}' is not in a state that allows this operation (current state: {})",
            key.arn, key.key_state
        ),
    ))
}

pub(crate) fn validate_key_usage_signing(
    key: &KmsKey,
    resolved: &str,
) -> Result<(), AwsServiceError> {
    if key.key_usage != "SIGN_VERIFY" {
        // Both Sign and Verify declare InvalidKeyUsageException; use it
        // rather than the unmodelled ValidationException.
        return Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "InvalidKeyUsageException",
            format!(
                "1 validation error detected: Value '{resolved}' at 'KeyId' failed to satisfy constraint: Member must point to a key with usage: 'SIGN_VERIFY'"
            ),
        ));
    }
    Ok(())
}

pub(crate) fn validate_signing_algorithm(
    key: &KmsKey,
    signing_algorithm: &str,
) -> Result<(), AwsServiceError> {
    let valid_algs = key.signing_algorithms.as_deref().unwrap_or(&[]);
    if !valid_algs.iter().any(|a| a == signing_algorithm) {
        let set: Vec<String> = if valid_algs.is_empty() {
            VALID_SIGNING_ALGORITHMS
                .iter()
                .map(|s| s.to_string())
                .collect()
        } else {
            valid_algs.to_vec()
        };
        return Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "InvalidKeyUsageException",
            format!(
                "1 validation error detected: Value '{signing_algorithm}' at 'SigningAlgorithm' failed to satisfy constraint: Member must satisfy enum value set: {}",
                fmt_enum_set(&set)
            ),
        ));
    }
    Ok(())
}

pub(crate) fn encryption_algorithms_for_key(
    key_usage: &str,
    key_spec: &str,
) -> Option<Vec<String>> {
    if key_usage == "ENCRYPT_DECRYPT" {
        match key_spec {
            "SYMMETRIC_DEFAULT" => Some(vec!["SYMMETRIC_DEFAULT".into()]),
            "RSA_2048" | "RSA_3072" | "RSA_4096" => {
                Some(vec!["RSAES_OAEP_SHA_1".into(), "RSAES_OAEP_SHA_256".into()])
            }
            _ => None,
        }
    } else {
        None
    }
}

pub(crate) fn mac_algorithms_for_key_spec(key_spec: &str) -> Option<Vec<String>> {
    match key_spec {
        "HMAC_224" => Some(vec!["HMAC_SHA_224".into()]),
        "HMAC_256" => Some(vec!["HMAC_SHA_256".into()]),
        "HMAC_384" => Some(vec!["HMAC_SHA_384".into()]),
        "HMAC_512" => Some(vec!["HMAC_SHA_512".into()]),
        _ => None,
    }
}

pub(crate) fn rand_bytes(n: usize) -> Vec<u8> {
    (0..n)
        .map(|_| {
            let u = Uuid::new_v4();
            u.as_bytes()[0]
        })
        .collect()
}

pub(crate) fn custom_key_store_json(store: &CustomKeyStore) -> Value {
    let mut obj = json!({
        "CustomKeyStoreId": store.custom_key_store_id,
        "CustomKeyStoreName": store.custom_key_store_name,
        "CustomKeyStoreType": store.custom_key_store_type,
        "ConnectionState": store.connection_state,
        "CreationDate": store.creation_date,
    });
    if let Some(ref v) = store.cloud_hsm_cluster_id {
        obj["CloudHsmClusterId"] = json!(v);
    }
    if let Some(ref v) = store.trust_anchor_certificate {
        obj["TrustAnchorCertificate"] = json!(v);
    }
    if let Some(ref v) = store.xks_proxy_uri_endpoint {
        obj["XksProxyConfiguration"] = json!({});
        obj["XksProxyConfiguration"]["UriEndpoint"] = json!(v);
        if let Some(ref p) = store.xks_proxy_uri_path {
            obj["XksProxyConfiguration"]["UriPath"] = json!(p);
        }
        if let Some(ref c) = store.xks_proxy_connectivity {
            obj["XksProxyConfiguration"]["Connectivity"] = json!(c);
        }
        if let Some(ref s) = store.xks_proxy_vpc_endpoint_service_name {
            obj["XksProxyConfiguration"]["VpcEndpointServiceName"] = json!(s);
        }
    }
    obj
}

pub(crate) fn key_metadata_json(key: &KmsKey, account_id: &str) -> Value {
    let mut meta = json!({
        "KeyId": key.key_id,
        "Arn": key.arn,
        "AWSAccountId": account_id,
        "CreationDate": key.creation_date,
        "Description": key.description,
        "Enabled": key.enabled,
        "KeyUsage": key.key_usage,
        "KeySpec": key.key_spec,
        "CustomerMasterKeySpec": key.key_spec,
        "KeyManager": key.key_manager,
        "KeyState": key.key_state,
        "Origin": key.origin,
        "MultiRegion": key.multi_region,
    });

    if let Some(ref enc_algs) = key.encryption_algorithms {
        meta["EncryptionAlgorithms"] = json!(enc_algs);
    }
    if let Some(ref sig_algs) = key.signing_algorithms {
        meta["SigningAlgorithms"] = json!(sig_algs);
    }
    if let Some(ref mac_algs) = key.mac_algorithms {
        meta["MacAlgorithms"] = json!(mac_algs);
    }
    if let Some(dd) = key.deletion_date {
        meta["DeletionDate"] = json!(dd);
    }
    if let Some(ref cks_id) = key.custom_key_store_id {
        meta["CustomKeyStoreId"] = json!(cks_id);
    }

    if key.multi_region {
        // Distinguish primary vs replica via the stored `primary_region`
        // pointer — when set, this key is a replica that points back at
        // its primary's region and arn.
        let (key_type, primary_arn, primary_region) = match &key.primary_region {
            Some(region) => {
                let primary_arn = key
                    .arn
                    .splitn(6, ':')
                    .enumerate()
                    .map(|(idx, part)| if idx == 3 { region.as_str() } else { part })
                    .collect::<Vec<&str>>()
                    .join(":");
                ("REPLICA", primary_arn, region.clone())
            }
            None => {
                let region = key.arn.split(':').nth(3).unwrap_or("us-east-1").to_string();
                ("PRIMARY", key.arn.clone(), region)
            }
        };
        meta["MultiRegionConfiguration"] = json!({
            "MultiRegionKeyType": key_type,
            "PrimaryKey": {
                "Arn": primary_arn,
                "Region": primary_region,
            },
            "ReplicaKeys": [],
        });
    }

    meta
}

pub(crate) fn fmt_enum_set(items: &[String]) -> String {
    let inner: Vec<String> = items.iter().map(|s| format!("'{s}'")).collect();
    format!("[{}]", inner.join(", "))
}

pub(crate) fn grant_to_json(grant: &KmsGrant, account_id: &str) -> Value {
    let mut v = json!({
        "KeyId": grant.key_id,
        "GrantId": grant.grant_id,
        "GranteePrincipal": grant.grantee_principal,
        "Operations": grant.operations,
        "IssuingAccount": fakecloud_aws::arn::Arn::global("iam", account_id, "root").to_string(),
        "CreationDate": grant.creation_date,
    });

    if let Some(ref rp) = grant.retiring_principal {
        v["RetiringPrincipal"] = json!(rp);
    }
    if let Some(ref c) = grant.constraints {
        v["Constraints"] = c.clone();
    }
    if let Some(ref n) = grant.name {
        v["Name"] = json!(n);
    }

    v
}

pub(crate) fn data_key_size_from_body(body: &Value) -> Result<usize, AwsServiceError> {
    let key_spec = body["KeySpec"].as_str();
    let number_of_bytes = body["NumberOfBytes"].as_u64();

    match (key_spec, number_of_bytes) {
        (Some(_), Some(_)) => Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "ValidationException",
            "KeySpec and NumberOfBytes are mutually exclusive",
        )),
        (Some("AES_256"), None) => Ok(32),
        (Some("AES_128"), None) => Ok(16),
        (Some(spec), None) => Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "ValidationException",
            format!("1 validation error detected: Value '{spec}' at 'keySpec' failed to satisfy constraint: Member must satisfy enum value set: [AES_256, AES_128]"),
        )),
        (None, Some(n)) => {
            if n > 1024 {
                Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ValidationException",
                    format!("1 validation error detected: Value '{n}' at 'numberOfBytes' failed to satisfy constraint: Member must have value less than or equal to 1024"),
                ))
            } else {
                Ok(n as usize)
            }
        }
        (None, None) => Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "ValidationException",
            "KeySpec or NumberOfBytes is required",
        )),
    }
}

pub(crate) fn generate_fake_public_key(key_spec: &str) -> Vec<u8> {
    // Return a minimal but valid-looking DER-encoded SubjectPublicKeyInfo
    // This is a fake RSA 2048-bit public key structure for testing
    match key_spec {
        "RSA_2048" | "RSA_3072" | "RSA_4096" => {
            // A minimal ASN.1 DER structure for RSA public key
            let mut key = vec![
                0x30, 0x82, 0x01, 0x22, // SEQUENCE, length 290
                0x30, 0x0d, // SEQUENCE, length 13
                0x06, 0x09, 0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 0x01, 0x01,
                0x01, // OID rsaEncryption
                0x05, 0x00, // NULL
                0x03, 0x82, 0x01, 0x0f, // BIT STRING, length 271
                0x00, // unused bits
                0x30, 0x82, 0x01, 0x0a, // SEQUENCE, length 266
                0x02, 0x82, 0x01, 0x01, // INTEGER, length 257
            ];
            // Fake modulus (257 bytes: 0x00 + 256 bytes of random-looking data)
            key.push(0x00);
            key.extend_from_slice(&rand_bytes(256));
            // Exponent
            key.extend_from_slice(&[0x02, 0x03, 0x01, 0x00, 0x01]); // 65537
            key
        }
        "ECC_NIST_P256" | "ECC_SECG_P256K1" => {
            // Minimal EC public key for P-256
            let mut key = vec![
                0x30, 0x59, // SEQUENCE, length 89
                0x30, 0x13, // SEQUENCE, length 19
                0x06, 0x07, 0x2a, 0x86, 0x48, 0xce, 0x3d, 0x02, 0x01, // OID ecPublicKey
                0x06, 0x08, 0x2a, 0x86, 0x48, 0xce, 0x3d, 0x03, 0x01, 0x07, // OID prime256v1
                0x03, 0x42, // BIT STRING, length 66
                0x00, // unused bits
                0x04, // uncompressed point
            ];
            key.extend_from_slice(&rand_bytes(64)); // x and y coordinates
            key
        }
        "ECC_NIST_P384" => {
            let mut key = vec![
                0x30, 0x76, // SEQUENCE, length 118
                0x30, 0x10, // SEQUENCE, length 16
                0x06, 0x07, 0x2a, 0x86, 0x48, 0xce, 0x3d, 0x02, 0x01, // OID ecPublicKey
                0x06, 0x05, 0x2b, 0x81, 0x04, 0x00, 0x22, // OID secp384r1
                0x03, 0x62, // BIT STRING, length 98
                0x00, // unused bits
                0x04, // uncompressed point
            ];
            key.extend_from_slice(&rand_bytes(96)); // x and y coordinates
            key
        }
        "ECC_NIST_P521" => {
            let mut key = vec![
                0x30, 0x81, 0x9b, // SEQUENCE, length 155
                0x30, 0x10, // SEQUENCE, length 16
                0x06, 0x07, 0x2a, 0x86, 0x48, 0xce, 0x3d, 0x02, 0x01, // OID ecPublicKey
                0x06, 0x05, 0x2b, 0x81, 0x04, 0x00, 0x23, // OID secp521r1
                0x03, 0x81, 0x86, // BIT STRING, length 134
                0x00, // unused bits
                0x04, // uncompressed point
            ];
            key.extend_from_slice(&rand_bytes(132)); // x and y coordinates
            key
        }
        _ => rand_bytes(32),
    }
}

pub(crate) fn check_policy_deny(key: &KmsKey, action: &str) -> Result<(), AwsServiceError> {
    // Parse the policy and check for Deny statements
    let policy: Value = match serde_json::from_str(&key.policy) {
        Ok(v) => v,
        Err(_) => return Ok(()), // If policy can't be parsed, allow
    };

    let statements = match policy["Statement"].as_array() {
        Some(s) => s,
        None => return Ok(()),
    };

    for stmt in statements {
        let effect = stmt["Effect"].as_str().unwrap_or("");
        if !effect.eq_ignore_ascii_case("deny") {
            continue;
        }

        // Check Resource - only deny if resource is "*"
        let resource = &stmt["Resource"];
        let resource_matches = if let Some(r) = resource.as_str() {
            r == "*"
        } else if let Some(arr) = resource.as_array() {
            arr.iter().any(|r| r.as_str() == Some("*"))
        } else {
            false
        };

        if !resource_matches {
            continue;
        }

        // Check Action
        let actions = if let Some(a) = stmt["Action"].as_str() {
            vec![a.to_string()]
        } else if let Some(arr) = stmt["Action"].as_array() {
            arr.iter()
                .filter_map(|a| a.as_str().map(|s| s.to_string()))
                .collect()
        } else {
            continue;
        };

        for policy_action in &actions {
            if action_matches(policy_action, action) {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "AccessDeniedException",
                    format!(
                        "User is not authorized to perform: {} on resource: {}",
                        action, key.arn
                    ),
                ));
            }
        }
    }

    Ok(())
}

pub(crate) fn action_matches(policy_action: &str, requested_action: &str) -> bool {
    if policy_action == "kms:*" {
        return true;
    }
    if policy_action == requested_action {
        return true;
    }
    // Wildcard matching: "kms:Describe*" matches "kms:DescribeKey"
    if let Some(prefix) = policy_action.strip_suffix('*') {
        if requested_action.starts_with(prefix) {
            return true;
        }
    }
    false
}
