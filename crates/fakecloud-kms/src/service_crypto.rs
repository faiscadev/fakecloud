// Auto-extracted from service.rs as part of carryover service.rs split.

#![allow(clippy::too_many_arguments)]

use base64::Engine;
use serde_json::json;

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use super::*;

impl KmsService {
    pub(super) fn encrypt(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        // Encrypt declares NotFound / Disabled / InvalidKeyUsage / KeyUnavailable /
        // InvalidGrantToken / DryRunOperation / KMSInternal / KMSInvalidState /
        // DependencyTimeout (no ValidationException). Map any input-shape
        // failures onto InvalidKeyUsageException, which is the closest match
        // for "the request to use the key is invalid".
        let key_id =
            Self::require_key_id(&body).map_err(|e| recode_validation(e, "NotFoundException"))?;
        let plaintext_b64 = body["Plaintext"].as_str().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidKeyUsageException",
                "Plaintext is required",
            )
        })?;
        let plaintext_bytes = decode_plaintext(plaintext_b64)
            .map_err(|e| recode_validation(e, "InvalidKeyUsageException"))?;

        let resolved = self
            .resolve_key_id_for(&req.account_id, &req.region, &key_id)
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "NotFoundException",
                    format!("Key '{key_id}' does not exist"),
                )
            })?;

        let accounts = self.state.read();
        let empty = KmsState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
        let key = state.keys.get(&resolved).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "KMSInternalException",
                "Key state became inconsistent",
            )
        })?;
        require_usable_key_state(key)?;

        // Encrypt is only valid for ENCRYPT_DECRYPT keys. A SIGN_VERIFY /
        // GENERATE_VERIFY_MAC / KEY_AGREEMENT key must be rejected, not silently
        // encrypted under the symmetric path (bug-audit 2026-06-20, 1.11).
        if key.key_usage != "ENCRYPT_DECRYPT" {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidKeyUsageException",
                format!(
                    "The operation failed because the KMS key {} is not enabled for the requested operation. The key usage must be ENCRYPT_DECRYPT but is {}.",
                    key.arn, key.key_usage
                ),
            ));
        }

        let requested_alg = body["EncryptionAlgorithm"].as_str();
        let ec_aad = canonical_encryption_context(&body["EncryptionContext"])?;

        let (ciphertext_b64, echoed_alg) = if key.key_spec.starts_with("RSA_") {
            // Asymmetric ENCRYPT_DECRYPT (RSA): real RSA-OAEP under the key's
            // public half, so the ciphertext round-trips through external RSA
            // tooling and KMS Decrypt -- not the symmetric AES blob the old code
            // returned regardless of key type and key spec.
            let alg = requested_alg.unwrap_or("RSAES_OAEP_SHA_256");
            if alg != "RSAES_OAEP_SHA_1" && alg != "RSAES_OAEP_SHA_256" {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidKeyUsageException",
                    format!(
                        "Algorithm '{alg}' is incompatible with the key spec '{}'.",
                        key.key_spec
                    ),
                ));
            }
            let pub_der = key.asymmetric_public_key_der.as_ref().ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "KMSInternalException",
                    "asymmetric public key missing",
                )
            })?;
            let raw = super::asym::rsa_oaep_wrap(pub_der, alg, &plaintext_bytes).map_err(|e| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidKeyUsageException",
                    format!("RSA encrypt failed: {e}"),
                )
            })?;
            let raw_b64 = base64::engine::general_purpose::STANDARD.encode(&raw);
            let envelope = format!("fakecloud-rsa:{}:{}:{}", key.key_id, alg, raw_b64);
            let env_b64 = base64::engine::general_purpose::STANDARD.encode(envelope.as_bytes());
            (env_b64, alg.to_string())
        } else {
            // Symmetric key: only SYMMETRIC_DEFAULT is valid.
            if let Some(alg) = requested_alg {
                if alg != "SYMMETRIC_DEFAULT" {
                    return Err(AwsServiceError::aws_error(
                        StatusCode::BAD_REQUEST,
                        "InvalidKeyUsageException",
                        format!(
                            "Algorithm '{alg}' is incompatible with the key spec 'SYMMETRIC_DEFAULT'."
                        ),
                    ));
                }
            }
            let ct = build_encrypt_ciphertext(state, key, plaintext_b64, &plaintext_bytes, &ec_aad);
            (ct, "SYMMETRIC_DEFAULT".to_string())
        };

        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&json!({
                "CiphertextBlob": ciphertext_b64,
                "KeyId": key.arn,
                "EncryptionAlgorithm": echoed_alg,
            }))
            .unwrap(),
        ))
    }

    pub(super) fn decrypt(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        // Decrypt's Smithy contract doesn't declare ValidationException;
        // bad ciphertext / missing fields surface as InvalidCiphertextException.
        let ciphertext_b64 = body["CiphertextBlob"].as_str().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidCiphertextException",
                "CiphertextBlob is required",
            )
        })?;
        // KeyId / EncryptionAlgorithm length+enum validation.
        recoded("NotFoundException", || {
            validate_optional_string_length("keyId", body["KeyId"].as_str(), 1, 2048)
        })?;
        recoded("InvalidKeyUsageException", || {
            validate_optional_enum(
                "encryptionAlgorithm",
                body["EncryptionAlgorithm"].as_str(),
                &[
                    "SYMMETRIC_DEFAULT",
                    "RSAES_OAEP_SHA_1",
                    "RSAES_OAEP_SHA_256",
                    "SM2PKE",
                ],
            )
        })?;

        let accounts = self.state.read();
        let empty = KmsState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
        let ec_aad = canonical_encryption_context(&body["EncryptionContext"])?;
        let decoded = decode_ciphertext_envelope(state, ciphertext_b64, &ec_aad)?;

        // For ciphertext produced by an asymmetric (RSA) KMS key, AWS requires
        // the caller to supply the EncryptionAlgorithm and it must match the
        // algorithm recorded in the ciphertext envelope. An omitted algorithm
        // defaults to SYMMETRIC_DEFAULT, which never matches an RSA envelope,
        // so both an omitted and a mismatched algorithm surface as
        // InvalidCiphertextException. Symmetric decrypt is unaffected.
        if decoded.encryption_algorithm != "SYMMETRIC_DEFAULT" {
            let requested = body["EncryptionAlgorithm"]
                .as_str()
                .unwrap_or("SYMMETRIC_DEFAULT");
            if requested != decoded.encryption_algorithm {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidCiphertextException",
                    "The specified EncryptionAlgorithm does not match the algorithm used to produce the ciphertext.",
                ));
            }
        }

        // When the caller supplies KeyId for a symmetric decrypt, AWS
        // validates it identifies the same CMK that produced the blob and
        // returns IncorrectKeyException otherwise (1.5). For symmetric keys
        // KeyId is optional (AWS recovers it from the blob), but if present
        // it must match. We resolve the caller KeyId (raw id / key ARN /
        // alias / alias ARN) to its key ARN and compare against the
        // producing key's ARN.
        if let Some(caller_key_id) = body["KeyId"].as_str().filter(|s| !s.is_empty()) {
            let resolved = Self::resolve_key_id_with_state(state, caller_key_id)
                .and_then(|id| state.keys.get(&id))
                .map(|k| k.arn.clone());
            match resolved {
                Some(arn) if arn == decoded.source_arn => {}
                Some(_) => {
                    return Err(AwsServiceError::aws_error(
                        StatusCode::BAD_REQUEST,
                        "IncorrectKeyException",
                        "The key ID in the request does not identify a CMK that can perform this operation.",
                    ));
                }
                None => {
                    return Err(AwsServiceError::aws_error(
                        StatusCode::BAD_REQUEST,
                        "NotFoundException",
                        format!("Key '{caller_key_id}' does not exist"),
                    ));
                }
            }
        }

        // Gate Decrypt on the source key's lifecycle state. AWS rejects
        // Decrypt against a key in any state other than `Enabled`.
        if let Some(key_id_only) = decoded.source_arn.rsplit('/').next() {
            if let Some(source_key) = state.keys.get(key_id_only) {
                require_usable_key_state(source_key)?;
            }
        }

        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&json!({
                "Plaintext": decoded.plaintext_b64,
                "KeyId": decoded.source_arn,
                "EncryptionAlgorithm": decoded.encryption_algorithm,
            }))
            .unwrap(),
        ))
    }

    pub(super) fn re_encrypt(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        // ReEncrypt declares NotFound / Disabled / InvalidCiphertext /
        // InvalidKeyUsage / IncorrectKey / KeyUnavailable /
        // InvalidGrantToken / DryRunOperation / KMSInternal /
        // KMSInvalidState / DependencyTimeout. Map missing/bad shapes onto
        // InvalidCiphertextException (source) and NotFoundException (dest).
        let ciphertext_b64 = body["CiphertextBlob"].as_str().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidCiphertextException",
                "CiphertextBlob is required",
            )
        })?;
        let dest_key_id = body["DestinationKeyId"].as_str().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "NotFoundException",
                "DestinationKeyId is required",
            )
        })?;
        recoded("NotFoundException", || {
            validate_string_length("destinationKeyId", dest_key_id, 1, 2048)
        })?;
        recoded("NotFoundException", || {
            validate_optional_string_length("sourceKeyId", body["SourceKeyId"].as_str(), 1, 2048)
        })?;
        recoded("InvalidKeyUsageException", || {
            validate_optional_enum(
                "sourceEncryptionAlgorithm",
                body["SourceEncryptionAlgorithm"].as_str(),
                &[
                    "SYMMETRIC_DEFAULT",
                    "RSAES_OAEP_SHA_1",
                    "RSAES_OAEP_SHA_256",
                    "SM2PKE",
                ],
            )
        })?;
        recoded("InvalidKeyUsageException", || {
            validate_optional_enum(
                "destinationEncryptionAlgorithm",
                body["DestinationEncryptionAlgorithm"].as_str(),
                &[
                    "SYMMETRIC_DEFAULT",
                    "RSAES_OAEP_SHA_1",
                    "RSAES_OAEP_SHA_256",
                    "SM2PKE",
                ],
            )
        })?;

        let accounts = self.state.read();
        let empty = KmsState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
        let source_ec_aad = canonical_encryption_context(&body["SourceEncryptionContext"])?;
        let dest_ec_aad = canonical_encryption_context(&body["DestinationEncryptionContext"])?;
        let decoded = decode_ciphertext_envelope(state, ciphertext_b64, &source_ec_aad)?;

        let dest_resolved =
            Self::resolve_key_id_with_state(state, dest_key_id).ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "NotFoundException",
                    format!("Key '{dest_key_id}' does not exist"),
                )
            })?;

        let dest_key = state.keys.get(&dest_resolved).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "KMSInternalException",
                "Key state became inconsistent",
            )
        })?;
        require_usable_key_state(dest_key)?;

        // The destination CMK must be an encryption key. Re-encrypting into a
        // SIGN_VERIFY / GENERATE_VERIFY_MAC / KEY_AGREEMENT key is invalid;
        // AWS rejects it with InvalidKeyUsageException. Previously any dest key
        // was accepted and its plaintext silently wrapped under the symmetric
        // path regardless of usage or spec.
        if dest_key.key_usage != "ENCRYPT_DECRYPT" {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidKeyUsageException",
                format!(
                    "The operation failed because the KMS key {} is not enabled for the requested operation. The key usage must be ENCRYPT_DECRYPT but is {}.",
                    dest_key.arn, dest_key.key_usage
                ),
            ));
        }

        // Source key gate too — AWS rejects ReEncrypt when either side
        // is in a non-Enabled state.
        if let Some(src_key_id) = decoded.source_arn.rsplit('/').next() {
            if let Some(source_key) = state.keys.get(src_key_id) {
                require_usable_key_state(source_key)?;
            }
        }

        let plaintext_bytes = base64::engine::general_purpose::STANDARD
            .decode(&decoded.plaintext_b64)
            .unwrap_or_default();
        let (new_ciphertext_b64, dest_algorithm) = if dest_key.key_spec.starts_with("RSA_") {
            // Asymmetric destination: produce a real RSA-OAEP ciphertext under
            // the destination key's public half, mirroring the Encrypt path,
            // rather than the symmetric AES blob the old code emitted for every
            // destination. The echoed DestinationEncryptionAlgorithm reflects
            // the algorithm actually used.
            let alg = body["DestinationEncryptionAlgorithm"]
                .as_str()
                .unwrap_or("RSAES_OAEP_SHA_256");
            if alg != "RSAES_OAEP_SHA_1" && alg != "RSAES_OAEP_SHA_256" {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidKeyUsageException",
                    format!(
                        "Algorithm '{alg}' is incompatible with the key spec '{}'.",
                        dest_key.key_spec
                    ),
                ));
            }
            let pub_der = dest_key.asymmetric_public_key_der.as_ref().ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "KMSInternalException",
                    "asymmetric public key missing",
                )
            })?;
            let raw = super::asym::rsa_oaep_wrap(pub_der, alg, &plaintext_bytes).map_err(|e| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidKeyUsageException",
                    format!("RSA re-encrypt failed: {e}"),
                )
            })?;
            let raw_b64 = base64::engine::general_purpose::STANDARD.encode(&raw);
            let envelope = format!("fakecloud-rsa:{}:{}:{}", dest_key.key_id, alg, raw_b64);
            let env_b64 = base64::engine::general_purpose::STANDARD.encode(envelope.as_bytes());
            (env_b64, alg.to_string())
        } else if let Some(ref material) = dest_key.imported_material_bytes {
            // Imported-key path: keep the legacy XOR envelope so consumers
            // that already round-trip via key material can still decrypt.
            let xored: Vec<u8> = plaintext_bytes
                .iter()
                .enumerate()
                .map(|(i, b)| b ^ material[i % material.len()])
                .collect();
            let xored_b64 = base64::engine::general_purpose::STANDARD.encode(&xored);
            let envelope = format!("fakecloud-imported:{}:{xored_b64}", dest_key.key_id);
            (
                base64::engine::general_purpose::STANDARD.encode(envelope.as_bytes()),
                "SYMMETRIC_DEFAULT".to_string(),
            )
        } else {
            // Default path: wrap the recovered plaintext under the
            // destination key with the AWS-shaped binary blob, binding
            // the caller's DestinationEncryptionContext into the AAD.
            let blob = crate::blob::encode_with_context(
                &state.master_key_bytes,
                &dest_key.key_id,
                &plaintext_bytes,
                &dest_ec_aad,
            );
            (
                base64::engine::general_purpose::STANDARD.encode(&blob),
                "SYMMETRIC_DEFAULT".to_string(),
            )
        };

        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&json!({
                "CiphertextBlob": new_ciphertext_b64,
                "KeyId": dest_key.arn,
                "SourceKeyId": decoded.source_arn,
                "SourceEncryptionAlgorithm": decoded.encryption_algorithm,
                "DestinationEncryptionAlgorithm": dest_algorithm,
            }))
            .unwrap(),
        ))
    }

    pub(super) fn generate_data_key(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let key_id = Self::require_key_id(&body)?;

        let resolved = self
            .resolve_key_id_for(&req.account_id, &req.region, &key_id)
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "NotFoundException",
                    format!("Key '{key_id}' does not exist"),
                )
            })?;

        let accounts = self.state.read();
        let empty = KmsState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
        let key = state.keys.get(&resolved).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "KMSInternalException",
                "Key state became inconsistent",
            )
        })?;
        require_usable_key_state(key)?;
        require_key_usage_encrypt_decrypt(key)?;

        let num_bytes = data_key_size_from_body(&body)?;

        let data_key_bytes: Vec<u8> = rand_bytes(num_bytes);
        let plaintext_b64 = base64::engine::general_purpose::STANDARD.encode(&data_key_bytes);

        // Wrap the data key in the AWS-shaped binary blob, binding the
        // caller's EncryptionContext into the AAD so Decrypt with the same
        // context succeeds and Decrypt with a different/absent context fails —
        // matching real KMS and the Encrypt/ReEncrypt paths. Previously the
        // context was ignored, so the recommended envelope-encryption pattern
        // (GenerateDataKey + EncryptionContext) could never decrypt its key.
        let ec_aad = canonical_encryption_context(&body["EncryptionContext"])?;
        let blob = crate::blob::encode_with_context(
            &state.master_key_bytes,
            &key.key_id,
            &data_key_bytes,
            &ec_aad,
        );
        let ciphertext_b64 = base64::engine::general_purpose::STANDARD.encode(&blob);

        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&json!({
                "Plaintext": plaintext_b64,
                "CiphertextBlob": ciphertext_b64,
                "KeyId": key.arn,
            }))
            .unwrap(),
        ))
    }

    pub(super) fn generate_data_key_without_plaintext(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let key_id = Self::require_key_id(&body)?;

        let resolved = self
            .resolve_key_id_for(&req.account_id, &req.region, &key_id)
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "NotFoundException",
                    format!("Key '{key_id}' does not exist"),
                )
            })?;

        let accounts = self.state.read();
        let empty = KmsState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
        let key = state.keys.get(&resolved).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "KMSInternalException",
                "Key state became inconsistent",
            )
        })?;
        require_usable_key_state(key)?;
        require_key_usage_encrypt_decrypt(key)?;

        let num_bytes = data_key_size_from_body(&body)?;
        let data_key_bytes: Vec<u8> = rand_bytes(num_bytes);

        // Bind the caller's EncryptionContext into the AAD, identical to the
        // GenerateDataKey (with-plaintext) sibling. Previously this used
        // crate::blob::encode (AAD = key-id only), so a data key generated here
        // with an EncryptionContext could never be decrypted with that context
        // (AEAD mismatch -> InvalidCiphertextException) and Decrypt with no
        // context wrongly succeeded.
        let ec_aad = canonical_encryption_context(&body["EncryptionContext"])?;
        let blob = crate::blob::encode_with_context(
            &state.master_key_bytes,
            &key.key_id,
            &data_key_bytes,
            &ec_aad,
        );
        let ciphertext_b64 = base64::engine::general_purpose::STANDARD.encode(&blob);

        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&json!({
                "CiphertextBlob": ciphertext_b64,
                "KeyId": key.arn,
            }))
            .unwrap(),
        ))
    }

    pub(super) fn generate_random(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();

        // CustomKeyStoreId is accepted for API compatibility but has no effect on
        // random number generation in this emulator.
        validate_optional_string_length(
            "customKeyStoreId",
            body["CustomKeyStoreId"].as_str(),
            1,
            64,
        )?;

        let num_bytes = body["NumberOfBytes"].as_u64().unwrap_or(32) as usize;

        validate_range_i64("numberOfBytes", num_bytes as i64, 1, 1024)?;

        let random_bytes = rand_bytes(num_bytes);
        let b64 = base64::engine::general_purpose::STANDARD.encode(&random_bytes);

        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&json!({
                "Plaintext": b64,
            }))
            .unwrap(),
        ))
    }

    pub(super) fn sign(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let key_id = Self::require_key_id(&body)?;
        let message_b64 = body["Message"].as_str().unwrap_or("");
        let signing_algorithm = body["SigningAlgorithm"].as_str().unwrap_or("");

        // Validate message
        let message_bytes = base64::engine::general_purpose::STANDARD
            .decode(message_b64)
            .unwrap_or_default();

        if message_bytes.is_empty() {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidKeyUsageException",
                "1 validation error detected: Value at 'Message' failed to satisfy constraint: Member must have length greater than or equal to 1",
            ));
        }

        let resolved = self
            .resolve_key_id_for(&req.account_id, &req.region, &key_id)
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "NotFoundException",
                    format!("Key '{key_id}' does not exist"),
                )
            })?;

        let accounts = self.state.read();
        let empty = KmsState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
        let key = state.keys.get(&resolved).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "KMSInternalException",
                "Key state became inconsistent",
            )
        })?;
        require_usable_key_state(key)?;

        // Validate key usage
        if key.key_usage != "SIGN_VERIFY" {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidKeyUsageException",
                format!(
                    "1 validation error detected: Value '{}' at 'KeyId' failed to satisfy constraint: Member must point to a key with usage: 'SIGN_VERIFY'",
                    resolved
                ),
            ));
        }

        // Validate signing algorithm against key's supported algorithms
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
                    "1 validation error detected: Value '{}' at 'SigningAlgorithm' failed to satisfy constraint: Member must satisfy enum value set: {}",
                    signing_algorithm, fmt_enum_set(&set)
                ),
            ));
        }

        let message_is_digest = body["MessageType"].as_str() == Some("DIGEST");

        let priv_der = key.asymmetric_private_key_der.as_ref().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "UnsupportedOperationException",
                format!(
                    "KeySpec '{}' has no signing key material in this fakecloud build",
                    key.key_spec
                ),
            )
        })?;
        let signature_bytes = if signing_algorithm.starts_with("ECDSA") {
            super::asym_ecdsa::sign(
                &key.key_spec,
                priv_der,
                signing_algorithm,
                &message_bytes,
                message_is_digest,
            )
            .map_err(|e| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidKeyUsageException",
                    format!("Sign failed: {e}"),
                )
            })?
        } else {
            super::asym::rsa_sign(
                priv_der,
                signing_algorithm,
                &message_bytes,
                message_is_digest,
            )
            .map_err(|e| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidKeyUsageException",
                    format!("Sign failed: {e}"),
                )
            })?
        };

        let signature_b64 = base64::engine::general_purpose::STANDARD.encode(&signature_bytes);

        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&json!({
                "Signature": signature_b64,
                "SigningAlgorithm": signing_algorithm,
                "KeyId": key.arn,
            }))
            .unwrap(),
        ))
    }

    pub(super) fn verify(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let key_id = Self::require_key_id(&body)?;
        let message_b64 = body["Message"].as_str().unwrap_or("");
        let signature_b64 = body["Signature"].as_str().unwrap_or("");
        let signing_algorithm = body["SigningAlgorithm"].as_str().unwrap_or("");

        require_non_empty_b64("Message", message_b64)?;
        require_non_empty_b64("Signature", signature_b64)?;

        let resolved = self
            .resolve_key_id_for(&req.account_id, &req.region, &key_id)
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "NotFoundException",
                    format!("Key '{key_id}' does not exist"),
                )
            })?;

        let accounts = self.state.read();
        let empty = KmsState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
        let key = state.keys.get(&resolved).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "KMSInternalException",
                "Key state became inconsistent",
            )
        })?;

        require_usable_key_state(key)?;
        validate_key_usage_signing(key, &resolved)?;
        validate_signing_algorithm(key, signing_algorithm)?;

        let message_bytes = base64::engine::general_purpose::STANDARD
            .decode(message_b64)
            .unwrap_or_default();
        let signature_bytes = base64::engine::general_purpose::STANDARD
            .decode(signature_b64)
            .unwrap_or_default();
        let message_is_digest = body["MessageType"].as_str() == Some("DIGEST");

        let priv_der = key.asymmetric_private_key_der.as_ref().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "UnsupportedOperationException",
                format!(
                    "KeySpec '{}' has no signing key material in this fakecloud build",
                    key.key_spec
                ),
            )
        })?;
        let signature_valid = if signing_algorithm.starts_with("ECDSA") {
            super::asym_ecdsa::verify(
                &key.key_spec,
                priv_der,
                signing_algorithm,
                &message_bytes,
                &signature_bytes,
                message_is_digest,
            )
            .unwrap_or(false)
        } else {
            super::asym::rsa_verify(
                priv_der,
                signing_algorithm,
                &message_bytes,
                &signature_bytes,
                message_is_digest,
            )
            .unwrap_or(false)
        };

        if !signature_valid {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "KMSInvalidSignatureException",
                "The signature is not valid",
            ));
        }

        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&json!({
                "SignatureValid": signature_valid,
                "SigningAlgorithm": signing_algorithm,
                "KeyId": key.arn,
            }))
            .unwrap(),
        ))
    }

    pub(super) fn get_public_key(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let key_id = Self::require_key_id(&body)?;

        let resolved = self
            .resolve_key_id_for(&req.account_id, &req.region, &key_id)
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "NotFoundException",
                    format!("Key '{key_id}' does not exist"),
                )
            })?;

        let accounts = self.state.read();
        let empty = KmsState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
        let key = state.keys.get(&resolved).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "KMSInternalException",
                "Key state became inconsistent",
            )
        })?;

        // For specs whose keypair was generated at CreateKey time
        // (RSA_*), return the real SubjectPublicKeyInfo DER. For
        // specs we don't yet generate (ECDSA / SM2 in later G batches),
        // fall back to the structurally-valid placeholder so existing
        // round-trip tests still pass.
        let public_key_bytes = key
            .asymmetric_public_key_der
            .clone()
            .unwrap_or_else(|| generate_fake_public_key(&key.key_spec));
        let public_key_b64 = base64::engine::general_purpose::STANDARD.encode(&public_key_bytes);

        let mut response = json!({
            "KeyId": key.arn,
            "KeySpec": key.key_spec,
            "KeyUsage": key.key_usage,
            "PublicKey": public_key_b64,
            "CustomerMasterKeySpec": key.key_spec,
        });

        if let Some(ref signing_algs) = key.signing_algorithms {
            response["SigningAlgorithms"] = json!(signing_algs);
        }
        if let Some(ref enc_algs) = key.encryption_algorithms {
            response["EncryptionAlgorithms"] = json!(enc_algs);
        }

        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&response).unwrap(),
        ))
    }

    pub(super) fn generate_mac(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let key_id = Self::require_key_id(&body)?;
        let mac_algorithm = body["MacAlgorithm"].as_str().unwrap_or("").to_string();
        let message_b64 = body["Message"].as_str().unwrap_or("");

        let resolved = self
            .resolve_key_id_for(&req.account_id, &req.region, &key_id)
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "NotFoundException",
                    format!("Key '{key_id}' does not exist"),
                )
            })?;

        let accounts = self.state.read();
        let empty = KmsState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
        let key = state.keys.get(&resolved).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "KMSInternalException",
                "Key state became inconsistent",
            )
        })?;

        require_usable_key_state(key)?;

        // Validate key usage
        if key.key_usage != "GENERATE_VERIFY_MAC" {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidKeyUsageException",
                format!("Key '{}' is not a GENERATE_VERIFY_MAC key", key.arn),
            ));
        }

        // Validate key spec supports MAC
        let mac_algs = key.mac_algorithms.as_deref().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidKeyUsageException",
                format!("Key '{}' does not support MAC operations", key.arn),
            )
        })?;

        // The requested MacAlgorithm must be one the key actually advertises
        // (an HMAC_256 key supports only HMAC_SHA_256, etc.). AWS rejects a
        // mismatch with InvalidKeyUsageException; previously any spec-shaped
        // string was accepted and silently HMAC'd, so a HMAC_512 request
        // against a HMAC_256 key wrongly succeeded.
        if !mac_algs.iter().any(|a| a == &mac_algorithm) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidKeyUsageException",
                format!(
                    "1 validation error detected: Value '{mac_algorithm}' at 'MacAlgorithm' failed to satisfy constraint: Member must satisfy enum value set: {}",
                    fmt_enum_set(mac_algs)
                ),
            ));
        }

        // Real HMAC over the message keyed by master_key_bytes. The
        // legacy fake-bytes path is gone; tampering with either the
        // mac, key, or message no longer round-trips.
        let message_bytes = base64::engine::general_purpose::STANDARD
            .decode(message_b64)
            .map_err(|_| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ValidationException",
                    "Message is not valid base64",
                )
            })?;
        let mac_bytes = super::mac::compute(&mac_algorithm, &key.private_key_seed, &message_bytes)
            .map_err(|e| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ValidationException",
                    format!("GenerateMac failed: {e}"),
                )
            })?;
        let mac_b64 = base64::engine::general_purpose::STANDARD.encode(&mac_bytes);

        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&json!({
                "Mac": mac_b64,
                "KeyId": key.key_id,
                "MacAlgorithm": mac_algorithm,
            }))
            .unwrap(),
        ))
    }

    pub(super) fn verify_mac(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let key_id = Self::require_key_id(&body)?;
        let mac_algorithm = body["MacAlgorithm"].as_str().unwrap_or("").to_string();
        let message_b64 = body["Message"].as_str().unwrap_or("");
        let mac_b64 = body["Mac"].as_str().unwrap_or("");

        let resolved = self
            .resolve_key_id_for(&req.account_id, &req.region, &key_id)
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "NotFoundException",
                    format!("Key '{key_id}' does not exist"),
                )
            })?;

        let accounts = self.state.read();
        let empty = KmsState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
        let key = state.keys.get(&resolved).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "KMSInternalException",
                "Key state became inconsistent",
            )
        })?;

        require_usable_key_state(key)?;

        // Validate key usage
        if key.key_usage != "GENERATE_VERIFY_MAC" {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidKeyUsageException",
                format!("Key '{}' is not a GENERATE_VERIFY_MAC key", key.arn),
            ));
        }

        // The requested MacAlgorithm must be one the key advertises — same
        // rule as GenerateMac. Without this, VerifyMac against a HMAC_256
        // key with MacAlgorithm=HMAC_SHA_512 would recompute under the wrong
        // digest and report a spurious KMSInvalidMacException instead of the
        // AWS InvalidKeyUsageException.
        let mac_algs = key.mac_algorithms.as_deref().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidKeyUsageException",
                format!("Key '{}' does not support MAC operations", key.arn),
            )
        })?;
        if !mac_algs.iter().any(|a| a == &mac_algorithm) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidKeyUsageException",
                format!(
                    "1 validation error detected: Value '{mac_algorithm}' at 'MacAlgorithm' failed to satisfy constraint: Member must satisfy enum value set: {}",
                    fmt_enum_set(mac_algs)
                ),
            ));
        }

        // Real HMAC verify with constant-time comparison via the
        // hmac crate's verify_slice. Replaces the legacy stringified
        // expected-bytes equality compare.
        let message_bytes = base64::engine::general_purpose::STANDARD
            .decode(message_b64)
            .map_err(|_| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ValidationException",
                    "Message is not valid base64",
                )
            })?;
        let supplied_mac_bytes = base64::engine::general_purpose::STANDARD
            .decode(mac_b64)
            .map_err(|_| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ValidationException",
                    "Mac is not valid base64",
                )
            })?;
        let mac_valid = super::mac::verify(
            &mac_algorithm,
            &key.private_key_seed,
            &message_bytes,
            &supplied_mac_bytes,
        )
        .map_err(|e| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                format!("VerifyMac failed: {e}"),
            )
        })?;

        if !mac_valid {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "KMSInvalidMacException",
                "MAC verification failed",
            ));
        }

        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&json!({
                "KeyId": key.key_id,
                "MacAlgorithm": mac_algorithm,
                "MacValid": true,
            }))
            .unwrap(),
        ))
    }

    pub(super) fn generate_data_key_pair(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let key_id = Self::require_key_id(&body)?;
        let key_pair_spec = body["KeyPairSpec"]
            .as_str()
            .unwrap_or("RSA_2048")
            .to_string();

        let resolved = self
            .resolve_key_id_for(&req.account_id, &req.region, &key_id)
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "NotFoundException",
                    format!("Key '{key_id}' does not exist"),
                )
            })?;

        let accounts = self.state.read();
        let empty = KmsState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
        let key = state.keys.get(&resolved).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "KMSInternalException",
                "Key state became inconsistent",
            )
        })?;
        require_usable_key_state(key)?;
        require_key_usage_encrypt_decrypt(key)?;

        let (private_key_bytes, public_key_bytes) = generate_data_keypair_bytes(&key_pair_spec)?;
        let public_key_b64 = base64::engine::general_purpose::STANDARD.encode(&public_key_bytes);
        let private_plaintext_b64 =
            base64::engine::general_purpose::STANDARD.encode(&private_key_bytes);

        // Wrap the private key in the AWS-shaped binary blob, binding the
        // caller's EncryptionContext into the AAD (see GenerateDataKey).
        let ec_aad = canonical_encryption_context(&body["EncryptionContext"])?;
        let blob = crate::blob::encode_with_context(
            &state.master_key_bytes,
            &key.key_id,
            &private_key_bytes,
            &ec_aad,
        );
        let private_ciphertext_b64 = base64::engine::general_purpose::STANDARD.encode(&blob);

        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&json!({
                "KeyId": key.arn,
                "KeyPairSpec": key_pair_spec,
                "PublicKey": public_key_b64,
                "PrivateKeyPlaintext": private_plaintext_b64,
                "PrivateKeyCiphertextBlob": private_ciphertext_b64,
            }))
            .unwrap(),
        ))
    }

    pub(super) fn generate_data_key_pair_without_plaintext(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let key_id = Self::require_key_id(&body)?;
        let key_pair_spec = body["KeyPairSpec"]
            .as_str()
            .unwrap_or("RSA_2048")
            .to_string();

        let resolved = self
            .resolve_key_id_for(&req.account_id, &req.region, &key_id)
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "NotFoundException",
                    format!("Key '{key_id}' does not exist"),
                )
            })?;

        let accounts = self.state.read();
        let empty = KmsState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
        let key = state.keys.get(&resolved).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "KMSInternalException",
                "Key state became inconsistent",
            )
        })?;
        require_usable_key_state(key)?;
        require_key_usage_encrypt_decrypt(key)?;

        let (private_key_bytes, public_key_bytes) = generate_data_keypair_bytes(&key_pair_spec)?;
        let public_key_b64 = base64::engine::general_purpose::STANDARD.encode(&public_key_bytes);

        // Bind the caller's EncryptionContext into the AAD, identical to the
        // GenerateDataKeyPair (with-plaintext) sibling. Previously this used
        // crate::blob::encode (AAD = key-id only), so the private key could
        // never be decrypted with its EncryptionContext.
        let ec_aad = canonical_encryption_context(&body["EncryptionContext"])?;
        let blob = crate::blob::encode_with_context(
            &state.master_key_bytes,
            &key.key_id,
            &private_key_bytes,
            &ec_aad,
        );
        let private_ciphertext_b64 = base64::engine::general_purpose::STANDARD.encode(&blob);

        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&json!({
                "KeyId": key.arn,
                "KeyPairSpec": key_pair_spec,
                "PublicKey": public_key_b64,
                "PrivateKeyCiphertextBlob": private_ciphertext_b64,
            }))
            .unwrap(),
        ))
    }

    pub(super) fn derive_shared_secret(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let key_id = Self::require_key_id(&body)?;
        let _key_agreement_algorithm = body["KeyAgreementAlgorithm"]
            .as_str()
            .unwrap_or("ECDH")
            .to_string();
        let public_key_b64 = body["PublicKey"].as_str().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                "PublicKey is required",
            )
        })?;

        let resolved = self
            .resolve_key_id_for(&req.account_id, &req.region, &key_id)
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "NotFoundException",
                    format!("Key '{key_id}' does not exist"),
                )
            })?;

        let accounts = self.state.read();
        let empty = KmsState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
        let key = state.keys.get(&resolved).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "KMSInternalException",
                "Key state became inconsistent",
            )
        })?;

        require_usable_key_state(key)?;

        // Key must be asymmetric (KEY_AGREEMENT usage)
        if key.key_usage != "KEY_AGREEMENT" {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidKeyUsageException",
                format!(
                    "Key '{}' usage is '{}', not KEY_AGREEMENT",
                    key.arn, key.key_usage
                ),
            ));
        }

        // Real ECDH so both parties converge on the same secret:
        // ECDH(privA, pubB) == ECDH(privB, pubA). The previous
        // SHA-256(private_seed || peer_public) was asymmetric in its inputs,
        // so A and B derived *different* secrets and the agreement was useless.
        // KEY_AGREEMENT keys can only be ECC_NIST_P256/P384 here (CreateKey
        // refuses P521/SM2), so those two curves cover every case.
        let peer_spki_der = base64::engine::general_purpose::STANDARD
            .decode(public_key_b64)
            .map_err(|_| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ValidationException",
                    "PublicKey is not valid base64",
                )
            })?;
        let priv_der = key.asymmetric_private_key_der.as_deref().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidKeyUsageException",
                format!("Key '{}' has no asymmetric key material", key.arn),
            )
        })?;
        let shared_secret_bytes = ecdh_shared_secret(&key.key_spec, priv_der, &peer_spki_der)
            .map_err(|e| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ValidationException",
                    format!("ECDH failed: {e}"),
                )
            })?;
        let shared_secret_b64 =
            base64::engine::general_purpose::STANDARD.encode(shared_secret_bytes);

        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&json!({
                "KeyId": key.arn,
                "SharedSecret": shared_secret_b64,
                "KeyAgreementAlgorithm": "ECDH",
                "KeyOrigin": key.origin,
            }))
            .unwrap(),
        ))
    }
}

/// Real ECDH shared-secret derivation for KMS KEY_AGREEMENT keys. `priv_der`
/// is our key's PKCS#8 private key; `peer_spki_der` is the counterparty's
/// SubjectPublicKeyInfo. Returns the raw shared secret (the X coordinate),
/// which is symmetric: `ecdh(a, B) == ecdh(b, A)`.
fn ecdh_shared_secret(
    key_spec: &str,
    priv_der: &[u8],
    peer_spki_der: &[u8],
) -> Result<Vec<u8>, String> {
    match key_spec {
        "ECC_NIST_P256" => {
            use p256::pkcs8::{DecodePrivateKey, DecodePublicKey};
            let sk = p256::SecretKey::from_pkcs8_der(priv_der)
                .map_err(|e| format!("bad P256 private key: {e}"))?;
            let pk = p256::PublicKey::from_public_key_der(peer_spki_der)
                .map_err(|e| format!("bad P256 peer public key: {e}"))?;
            let shared = p256::ecdh::diffie_hellman(sk.to_nonzero_scalar(), pk.as_affine());
            Ok(shared.raw_secret_bytes().to_vec())
        }
        "ECC_NIST_P384" => {
            use p384::pkcs8::{DecodePrivateKey, DecodePublicKey};
            let sk = p384::SecretKey::from_pkcs8_der(priv_der)
                .map_err(|e| format!("bad P384 private key: {e}"))?;
            let pk = p384::PublicKey::from_public_key_der(peer_spki_der)
                .map_err(|e| format!("bad P384 peer public key: {e}"))?;
            let shared = p384::ecdh::diffie_hellman(sk.to_nonzero_scalar(), pk.as_affine());
            Ok(shared.raw_secret_bytes().to_vec())
        }
        other => Err(format!("unsupported KEY_AGREEMENT KeySpec: {other}")),
    }
}

/// Generate (private_pkcs8_der, public_spki_der) bytes for a KMS
/// `KeyPairSpec`. Real keypair via `rsa` for RSA specs and `ecdsa` /
/// `p256` / `p384` / `k256` for ECC specs — the resulting DER is
/// parseable with any standard tool, matching real AWS KMS so callers
/// can sign locally with `PrivateKeyPlaintext` and verify with
/// `PublicKey` end-to-end.
fn generate_data_keypair_bytes(key_pair_spec: &str) -> Result<(Vec<u8>, Vec<u8>), AwsServiceError> {
    if key_pair_spec.starts_with("RSA_") {
        return super::asym::generate_keypair(key_pair_spec)
            .map_err(|e| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ValidationException",
                    format!("RSA keypair generation failed: {e}"),
                )
            })?
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ValidationException",
                    format!("Unsupported KeyPairSpec: {key_pair_spec}"),
                )
            });
    }
    if key_pair_spec.starts_with("ECC_") {
        return super::asym_ecdsa::generate_keypair(key_pair_spec)
            .map_err(|e| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ValidationException",
                    format!("ECC keypair generation failed: {e}"),
                )
            })?
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ValidationException",
                    format!("Unsupported KeyPairSpec: {key_pair_spec}"),
                )
            });
    }
    Err(AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        "ValidationException",
        format!("Unsupported KeyPairSpec: {key_pair_spec}"),
    ))
}

#[cfg(test)]
mod ecdh_tests {
    use super::ecdh_shared_secret;

    /// ECDH must converge: ecdh(privA, pubB) == ecdh(privB, pubA). The prior
    /// SHA-256(seed || peer) construction did not, making DeriveSharedSecret
    /// useless for actual key agreement.
    #[test]
    fn ecdh_converges_p256() {
        let (priv_a, pub_a) = super::super::asym_ecdsa::generate_keypair("ECC_NIST_P256")
            .unwrap()
            .unwrap();
        let (priv_b, pub_b) = super::super::asym_ecdsa::generate_keypair("ECC_NIST_P256")
            .unwrap()
            .unwrap();
        let a = ecdh_shared_secret("ECC_NIST_P256", &priv_a, &pub_b).unwrap();
        let b = ecdh_shared_secret("ECC_NIST_P256", &priv_b, &pub_a).unwrap();
        assert_eq!(a, b, "both parties must derive the same secret");
        assert_eq!(
            a.len(),
            32,
            "P256 shared secret is the 32-byte X coordinate"
        );
    }

    #[test]
    fn ecdh_converges_p384() {
        let (priv_a, pub_a) = super::super::asym_ecdsa::generate_keypair("ECC_NIST_P384")
            .unwrap()
            .unwrap();
        let (priv_b, pub_b) = super::super::asym_ecdsa::generate_keypair("ECC_NIST_P384")
            .unwrap()
            .unwrap();
        let a = ecdh_shared_secret("ECC_NIST_P384", &priv_a, &pub_b).unwrap();
        let b = ecdh_shared_secret("ECC_NIST_P384", &priv_b, &pub_a).unwrap();
        assert_eq!(a, b);
        assert_eq!(a.len(), 48);
    }
}
