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
                    crate::hook::not_found_message(&req.account_id, &req.region, &key_id),
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
        Self::authorize_direct_key_use(req, key, "kms:Encrypt", &body["EncryptionContext"], &[])?;
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
            rsa_encrypt(key, requested_alg, &plaintext_bytes)?
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
            let ct = build_encrypt_ciphertext(
                state,
                sealed_key_ref(req, key),
                &plaintext_bytes,
                &ec_aad,
            );
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
        let ec_aad = canonical_encryption_context(&body["EncryptionContext"])?;
        let requested_alg = body["EncryptionAlgorithm"].as_str();
        let (decoded, source_key) = open_ciphertext(
            &accounts,
            &empty,
            req,
            ciphertext_b64,
            &ec_aad,
            body["KeyId"].as_str().filter(|s| !s.is_empty()),
            requested_alg,
        )?;
        require_matching_algorithm(&decoded, requested_alg)?;

        // Gate Decrypt on the source key's policy (AWS-managed keys) and
        // lifecycle state. AWS rejects Decrypt against a key in any state
        // other than `Enabled`.
        if let Some(source_key) = source_key {
            Self::authorize_direct_key_use(
                req,
                source_key,
                "kms:Decrypt",
                &body["EncryptionContext"],
                &[],
            )?;
            require_usable_key_state(source_key)?;
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
        let source_ec_aad = canonical_encryption_context(&body["SourceEncryptionContext"])?;
        let dest_ec_aad = canonical_encryption_context(&body["DestinationEncryptionContext"])?;
        // The source and destination keys may be in different accounts (the
        // caller's or others', each named by its key or alias ARN): each is
        // looked up, and opens or seals, in its own account.
        let source_alg = body["SourceEncryptionAlgorithm"].as_str();
        let (decoded, source_key) = open_ciphertext(
            &accounts,
            &empty,
            req,
            ciphertext_b64,
            &source_ec_aad,
            body["SourceKeyId"].as_str().filter(|s| !s.is_empty()),
            source_alg,
        )?;
        require_matching_algorithm(&decoded, source_alg)?;

        let dest_state = accounts
            .get(crate::hook::key_owner_account(&req.account_id, dest_key_id))
            .unwrap_or(&empty);
        let dest_key = Self::resolve_key_id_with_state(dest_state, &req.region, dest_key_id)
            .and_then(|id| dest_state.keys.get(&id))
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "NotFoundException",
                    crate::hook::not_found_message(&req.account_id, &req.region, dest_key_id),
                )
            })?;
        // ReEncrypt is authorized as `kms:ReEncryptFrom` on the source key
        // and `kms:ReEncryptTo` on the destination key.
        let same_key = if source_key.is_some_and(|k| k.arn == dest_key.arn) {
            "true"
        } else {
            "false"
        };
        let same = [("kms:ReEncryptOnSameKey", same_key)];
        if let Some(source_key) = source_key {
            Self::authorize_direct_key_use(
                req,
                source_key,
                "kms:ReEncryptFrom",
                &body["SourceEncryptionContext"],
                &same,
            )?;
        }
        Self::authorize_direct_key_use(
            req,
            dest_key,
            "kms:ReEncryptTo",
            &body["DestinationEncryptionContext"],
            &same,
        )?;
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
        if let Some(source_key) = source_key {
            require_usable_key_state(source_key)?;
        }

        let plaintext_bytes = base64::engine::general_purpose::STANDARD
            .decode(&decoded.plaintext_b64)
            .unwrap_or_default();
        let (new_ciphertext_b64, dest_algorithm) = if dest_key.key_spec.starts_with("RSA_") {
            // Asymmetric destination: a real RSA-OAEP ciphertext under the
            // destination key's public half, as Encrypt produces.
            rsa_encrypt(
                dest_key,
                body["DestinationEncryptionAlgorithm"].as_str(),
                &plaintext_bytes,
            )?
        } else {
            // Sealed under the destination key, in its account, binding the
            // caller's DestinationEncryptionContext into the AAD.
            (
                build_encrypt_ciphertext(
                    dest_state,
                    sealed_key_ref(req, dest_key),
                    &plaintext_bytes,
                    &dest_ec_aad,
                ),
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
                    crate::hook::not_found_message(&req.account_id, &req.region, &key_id),
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
        Self::authorize_direct_key_use(
            req,
            key,
            "kms:GenerateDataKey",
            &body["EncryptionContext"],
            &[],
        )?;
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
            sealed_key_ref(req, key),
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
                    crate::hook::not_found_message(&req.account_id, &req.region, &key_id),
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
        Self::authorize_direct_key_use(
            req,
            key,
            "kms:GenerateDataKeyWithoutPlaintext",
            &body["EncryptionContext"],
            &[],
        )?;
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
            sealed_key_ref(req, key),
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
                    crate::hook::not_found_message(&req.account_id, &req.region, &key_id),
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
        Self::authorize_direct_key_use(req, key, "kms:Sign", &Value::Null, &[])?;
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
                    crate::hook::not_found_message(&req.account_id, &req.region, &key_id),
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

        Self::authorize_direct_key_use(req, key, "kms:Verify", &Value::Null, &[])?;
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
                    crate::hook::not_found_message(&req.account_id, &req.region, &key_id),
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
                    crate::hook::not_found_message(&req.account_id, &req.region, &key_id),
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

        Self::authorize_direct_key_use(req, key, "kms:GenerateMac", &Value::Null, &[])?;
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
                    crate::hook::not_found_message(&req.account_id, &req.region, &key_id),
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

        Self::authorize_direct_key_use(req, key, "kms:VerifyMac", &Value::Null, &[])?;
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
                    crate::hook::not_found_message(&req.account_id, &req.region, &key_id),
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
        Self::authorize_direct_key_use(
            req,
            key,
            "kms:GenerateDataKeyPair",
            &body["EncryptionContext"],
            &[],
        )?;
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
            sealed_key_ref(req, key),
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
                    crate::hook::not_found_message(&req.account_id, &req.region, &key_id),
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
        Self::authorize_direct_key_use(
            req,
            key,
            "kms:GenerateDataKeyPairWithoutPlaintext",
            &body["EncryptionContext"],
            &[],
        )?;
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
            sealed_key_ref(req, key),
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
                    crate::hook::not_found_message(&req.account_id, &req.region, &key_id),
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

        Self::authorize_direct_key_use(req, key, "kms:DeriveSharedSecret", &Value::Null, &[])?;
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
/// For a ciphertext an asymmetric (RSA) key produced, AWS requires the
/// request's EncryptionAlgorithm to be the one that produced it. An omitted
/// algorithm defaults to SYMMETRIC_DEFAULT, which never matches, so both an
/// omitted and a mismatched algorithm are InvalidCiphertextException.
/// Symmetric ciphertexts are unaffected.
fn require_matching_algorithm(
    decoded: &DecodedCiphertext,
    requested: Option<&str>,
) -> Result<(), AwsServiceError> {
    if decoded.encryption_algorithm != "SYMMETRIC_DEFAULT"
        && requested.unwrap_or("SYMMETRIC_DEFAULT") != decoded.encryption_algorithm
    {
        return Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "InvalidCiphertextException",
            "The specified EncryptionAlgorithm does not match the algorithm used to produce the ciphertext.",
        ));
    }
    Ok(())
}

/// Encrypt `plaintext` under an RSA `ENCRYPT_DECRYPT` key with RSA-OAEP
/// (`RSAES_OAEP_SHA_256` by default), returning the base64 ciphertext and
/// the algorithm. The ciphertext is the raw RSA-OAEP output, as AWS returns
/// it: it carries no metadata naming the key, so external RSA tooling holding
/// the public key produces the same kind of ciphertext, and `Decrypt` /
/// `ReEncrypt` need the key named (`KeyId` / `SourceKeyId`) to open it.
fn rsa_encrypt(
    key: &KmsKey,
    requested_alg: Option<&str>,
    plaintext: &[u8],
) -> Result<(String, String), AwsServiceError> {
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
    let raw = super::asym::rsa_oaep_wrap(pub_der, alg, plaintext).map_err(|e| {
        AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "InvalidKeyUsageException",
            format!("RSA encrypt failed: {e}"),
        )
    })?;
    Ok((
        base64::engine::general_purpose::STANDARD.encode(&raw),
        alg.to_string(),
    ))
}

/// Open the ciphertext a `Decrypt` (or the source of a `ReEncrypt`) names,
/// as AWS opens it, returning what it opens to and the key that opens it.
///
/// `key_ref` is the request's `KeyId` (`SourceKeyId`) and `algorithm` its
/// `EncryptionAlgorithm` (`SourceEncryptionAlgorithm`). The key is looked up
/// in the account it names: a key or alias ARN of another account in that
/// account, anything else in the request's (its key's account once routed,
/// see `KmsService::cross_account_key_owner`); with no `key_ref`, the
/// account of the key the ciphertext names. A symmetric ciphertext names its
/// key, so `KeyId` is optional and, when given, must identify that key
/// (`IncorrectKeyException`). An RSA ciphertext names no key ("This parameter
/// is required only when the ciphertext was encrypted under an asymmetric
/// KMS key", Decrypt API reference): without `KeyId` it is not a ciphertext
/// KMS can open (`InvalidCiphertextException`), and with it, it is opened
/// with that key's private half under `algorithm`.
fn open_ciphertext<'a>(
    accounts: &'a fakecloud_core::multi_account::MultiAccountState<KmsState>,
    empty: &'a KmsState,
    req: &AwsRequest,
    ciphertext_b64: &str,
    ec_aad: &[u8],
    key_ref: Option<&str>,
    algorithm: Option<&str>,
) -> Result<(DecodedCiphertext, Option<&'a KmsKey>), AwsServiceError> {
    let embedded = ciphertext_key_ref(ciphertext_b64);
    let account = key_ref
        .or(embedded.as_deref())
        .map_or(req.account_id.as_str(), |r| {
            crate::hook::key_owner_account(&req.account_id, r)
        });
    let state = accounts.get(account).unwrap_or(empty);
    let incorrect_key = || {
        AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "IncorrectKeyException",
            "The key ID in the request does not identify a CMK that can perform this operation.",
        )
    };

    let Some(key_ref) = key_ref else {
        let decoded = decode_ciphertext_envelope(state, &req.region, ciphertext_b64, ec_aad)?;
        // A ciphertext of the textual RSA envelope earlier builds produced
        // names its key, but an RSA ciphertext is opened only with the key
        // named in the request, as on AWS.
        if decoded.encryption_algorithm != "SYMMETRIC_DEFAULT" {
            return Err(invalid_ciphertext());
        }
        let key = crate::hook::stored_key(state, "", &decoded.source_arn).map(|(_, key)| key);
        return Ok((decoded, key));
    };

    let key = KmsService::resolve_key_id_with_state(state, &req.region, key_ref)
        .and_then(|id| state.keys.get(&id))
        .ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "NotFoundException",
                crate::hook::not_found_message(&req.account_id, &req.region, key_ref),
            )
        })?;
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

    if key.key_spec.starts_with("RSA_") {
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(ciphertext_b64)
            .map_err(|_| invalid_ciphertext())?;
        let requested = algorithm.unwrap_or("SYMMETRIC_DEFAULT");
        // A textual RSA envelope earlier builds produced: opened as before,
        // by the key it names, which must be this one.
        if bytes.starts_with(RSA_ENVELOPE_PREFIX.as_bytes()) {
            let decoded = decode_ciphertext_envelope(state, &req.region, ciphertext_b64, ec_aad)?;
            if decoded.source_arn != key.arn {
                return Err(incorrect_key());
            }
            return Ok((decoded, Some(key)));
        }
        if requested != "RSAES_OAEP_SHA_1" && requested != "RSAES_OAEP_SHA_256" {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidCiphertextException",
                "The specified EncryptionAlgorithm does not match the algorithm used to produce the ciphertext.",
            ));
        }
        let priv_der = key
            .asymmetric_private_key_der
            .as_ref()
            .ok_or_else(invalid_ciphertext)?;
        let plaintext = super::asym::rsa_oaep_unwrap(priv_der, requested, &bytes)
            .map_err(|_| invalid_ciphertext())?;
        return Ok((
            DecodedCiphertext {
                source_arn: key.arn.clone(),
                plaintext_b64: base64::engine::general_purpose::STANDARD.encode(&plaintext),
                encryption_algorithm: requested.to_string(),
            },
            Some(key),
        ));
    }

    // A symmetric ciphertext names the key that sealed it: one naming
    // another key (of any account) is not this key's.
    if let Some(sealed_by) = embedded
        .as_deref()
        .and_then(|embedded| ciphertext_key_arn(accounts, req, embedded))
    {
        if sealed_by != key.arn {
            return Err(incorrect_key());
        }
    }
    let decoded = decode_ciphertext_envelope(state, &req.region, ciphertext_b64, ec_aad)?;
    if decoded.source_arn != key.arn || decoded.encryption_algorithm != "SYMMETRIC_DEFAULT" {
        return Err(incorrect_key());
    }
    Ok((decoded, Some(key)))
}

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
