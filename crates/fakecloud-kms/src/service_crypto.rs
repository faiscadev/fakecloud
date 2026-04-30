// Auto-extracted from service.rs as part of carryover service.rs split.

#![allow(clippy::too_many_arguments)]

use base64::Engine;
use serde_json::json;

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use super::*;

impl KmsService {
    pub(super) fn encrypt(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let key_id = Self::require_key_id(&body)?;
        let plaintext_b64 = body["Plaintext"].as_str().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                "Plaintext is required",
            )
        })?;
        let plaintext_bytes = decode_plaintext(plaintext_b64)?;

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
        if !key.enabled {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "DisabledException",
                format!("Key '{}' is disabled", key.arn),
            ));
        }

        let ciphertext_b64 = build_encrypt_ciphertext(state, key, plaintext_b64, &plaintext_bytes);

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
        let ciphertext_b64 = body["CiphertextBlob"].as_str().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                "CiphertextBlob is required",
            )
        })?;

        let accounts = self.state.read();
        let empty = KmsState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
        let decoded = decode_ciphertext_envelope(state, ciphertext_b64)?;

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
        let ciphertext_b64 = body["CiphertextBlob"].as_str().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                "CiphertextBlob is required",
            )
        })?;
        let dest_key_id = body["DestinationKeyId"].as_str().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                "DestinationKeyId is required",
            )
        })?;

        let accounts = self.state.read();
        let empty = KmsState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
        let decoded = decode_ciphertext_envelope(state, ciphertext_b64)?;

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
            // destination key with the AWS-shaped binary blob.
            let blob =
                crate::blob::encode(&state.master_key_bytes, &dest_key.key_id, &plaintext_bytes);
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
        if !key.enabled {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "DisabledException",
                format!("Key '{}' is disabled", key.arn),
            ));
        }

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
        if !key.enabled {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "DisabledException",
                format!("Key '{}' is disabled", key.arn),
            ));
        }

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
                "ValidationException",
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

        // Validate key usage
        if key.key_usage != "SIGN_VERIFY" {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
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
                "ValidationException",
                format!(
                    "1 validation error detected: Value '{}' at 'SigningAlgorithm' failed to satisfy constraint: Member must satisfy enum value set: {}",
                    signing_algorithm, fmt_enum_set(&set)
                ),
            ));
        }

        let message_is_digest = body["MessageType"].as_str() == Some("DIGEST");

        let signature_bytes = if let Some(priv_der) = &key.asymmetric_private_key_der {
            super::asym::rsa_sign(
                priv_der,
                signing_algorithm,
                &message_bytes,
                message_is_digest,
            )
            .map_err(|e| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ValidationException",
                    format!("Sign failed: {e}"),
                )
            })?
        } else {
            // Legacy fake-bytes path for specs whose real-crypto branch
            // hasn't landed yet (ECDSA in G2, etc.). Keeps the rest of
            // the surface working until the per-spec branches replace
            // this with `super::asym::*` paths.
            let sig_data = format!(
                "fakecloud-sig:{}:{}:{}",
                key.key_id, signing_algorithm, message_b64
            );
            sig_data.into_bytes()
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

        validate_key_usage_signing(key, &resolved)?;
        validate_signing_algorithm(key, signing_algorithm)?;

        let message_bytes = base64::engine::general_purpose::STANDARD
            .decode(message_b64)
            .unwrap_or_default();
        let signature_bytes = base64::engine::general_purpose::STANDARD
            .decode(signature_b64)
            .unwrap_or_default();
        let message_is_digest = body["MessageType"].as_str() == Some("DIGEST");

        let signature_valid = if let Some(priv_der) = &key.asymmetric_private_key_der {
            super::asym::rsa_verify(
                priv_der,
                signing_algorithm,
                &message_bytes,
                &signature_bytes,
                message_is_digest,
            )
            .unwrap_or(false)
        } else {
            // Legacy fake-bytes verify (paired with the legacy Sign
            // path above). Replaced spec-by-spec as later G batches
            // ship real crypto for ECDSA / ECDH-derived specs.
            let expected_sig_data = format!(
                "fakecloud-sig:{}:{}:{}",
                key.key_id, signing_algorithm, message_b64
            );
            let expected_signature_b64 =
                base64::engine::general_purpose::STANDARD.encode(expected_sig_data.as_bytes());
            signature_b64 == expected_signature_b64
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

        // Generate fake MAC
        let mac_data = format!(
            "fakecloud-mac:{}:{}:{}",
            key.key_id, mac_algorithm, message_b64
        );
        let mac_b64 = base64::engine::general_purpose::STANDARD.encode(mac_data.as_bytes());

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

        // Validate key usage
        if key.key_usage != "GENERATE_VERIFY_MAC" {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidKeyUsageException",
                format!("Key '{}' is not a GENERATE_VERIFY_MAC key", key.arn),
            ));
        }

        // Check if MAC matches
        let expected_mac_data = format!(
            "fakecloud-mac:{}:{}:{}",
            key.key_id, mac_algorithm, message_b64
        );
        let expected_mac_b64 =
            base64::engine::general_purpose::STANDARD.encode(expected_mac_data.as_bytes());

        let mac_valid = mac_b64 == expected_mac_b64;

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
        if !key.enabled {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "DisabledException",
                format!("Key '{}' is disabled", key.arn),
            ));
        }

        let public_key_bytes = generate_fake_public_key(&key_pair_spec);
        let private_key_bytes = rand_bytes(256);
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
        if !key.enabled {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "DisabledException",
                format!("Key '{}' is disabled", key.arn),
            ));
        }

        let public_key_bytes = generate_fake_public_key(&key_pair_spec);
        let private_key_bytes = rand_bytes(256);
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

        if !key.enabled {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "DisabledException",
                format!("Key '{}' is disabled", key.arn),
            ));
        }

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
