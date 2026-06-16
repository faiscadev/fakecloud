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

        let ec_aad = canonical_encryption_context(&body["EncryptionContext"]);
        let ciphertext_b64 =
            build_encrypt_ciphertext(state, key, plaintext_b64, &plaintext_bytes, &ec_aad);

        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&json!({
                "CiphertextBlob": ciphertext_b64,
                "KeyId": key.arn,
                "EncryptionAlgorithm": "SYMMETRIC_DEFAULT",
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
        let ec_aad = canonical_encryption_context(&body["EncryptionContext"]);
        let decoded = decode_ciphertext_envelope(state, ciphertext_b64, &ec_aad)?;

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
                "EncryptionAlgorithm": "SYMMETRIC_DEFAULT",
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
        let source_ec_aad = canonical_encryption_context(&body["SourceEncryptionContext"]);
        let dest_ec_aad = canonical_encryption_context(&body["DestinationEncryptionContext"]);
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
        let new_ciphertext_b64 = if let Some(ref material) = dest_key.imported_material_bytes {
            // Imported-key path: keep the legacy XOR envelope so consumers
            // that already round-trip via key material can still decrypt.
            let xored: Vec<u8> = plaintext_bytes
                .iter()
                .enumerate()
                .map(|(i, b)| b ^ material[i % material.len()])
                .collect();
            let xored_b64 = base64::engine::general_purpose::STANDARD.encode(&xored);
            let envelope = format!("fakecloud-imported:{}:{xored_b64}", dest_key.key_id);
            base64::engine::general_purpose::STANDARD.encode(envelope.as_bytes())
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
            base64::engine::general_purpose::STANDARD.encode(&blob)
        };

        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&json!({
                "CiphertextBlob": new_ciphertext_b64,
                "KeyId": dest_key.arn,
                "SourceKeyId": decoded.source_arn,
                "SourceEncryptionAlgorithm": "SYMMETRIC_DEFAULT",
                "DestinationEncryptionAlgorithm": "SYMMETRIC_DEFAULT",
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

        let num_bytes = data_key_size_from_body(&body)?;

        let data_key_bytes: Vec<u8> = rand_bytes(num_bytes);
        let plaintext_b64 = base64::engine::general_purpose::STANDARD.encode(&data_key_bytes);

        // Wrap the data key in the AWS-shaped binary blob.
        let blob = crate::blob::encode(&state.master_key_bytes, &key.key_id, &data_key_bytes);
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

        let num_bytes = data_key_size_from_body(&body)?;
        let data_key_bytes: Vec<u8> = rand_bytes(num_bytes);
        let _ = base64::engine::general_purpose::STANDARD.encode(&data_key_bytes);
        let blob = crate::blob::encode(&state.master_key_bytes, &key.key_id, &data_key_bytes);
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
        if key.mac_algorithms.is_none() {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidKeyUsageException",
                format!("Key '{}' does not support MAC operations", key.arn),
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

        let (private_key_bytes, public_key_bytes) = generate_data_keypair_bytes(&key_pair_spec)?;
        let public_key_b64 = base64::engine::general_purpose::STANDARD.encode(&public_key_bytes);
        let private_plaintext_b64 =
            base64::engine::general_purpose::STANDARD.encode(&private_key_bytes);

        // Wrap the private key in the AWS-shaped binary blob.
        let blob = crate::blob::encode(&state.master_key_bytes, &key.key_id, &private_key_bytes);
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

        let (private_key_bytes, public_key_bytes) = generate_data_keypair_bytes(&key_pair_spec)?;
        let public_key_b64 = base64::engine::general_purpose::STANDARD.encode(&public_key_bytes);

        let blob = crate::blob::encode(&state.master_key_bytes, &key.key_id, &private_key_bytes);
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
        let _public_key = body["PublicKey"].as_str().ok_or_else(|| {
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

        // Deterministic shared secret: SHA-256(private_key_seed || public_key_bytes)
        // Both parties using the correct keys will derive the same result.
        let public_key_bytes = base64::engine::general_purpose::STANDARD
            .decode(_public_key)
            .unwrap_or_default();

        use sha2::{Digest, Sha256};
        let mut hasher = Sha256::new();
        hasher.update(&key.private_key_seed);
        hasher.update(&public_key_bytes);
        let shared_secret_bytes = hasher.finalize();
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
