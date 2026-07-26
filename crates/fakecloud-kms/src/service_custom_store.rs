// Auto-extracted from service.rs as part of carryover service.rs split.

#![allow(clippy::too_many_arguments)]

use base64::Engine;
use chrono::Utc;
use serde_json::{json, Value};

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use super::*;

impl KmsService {
    pub(super) fn get_parameters_for_import(
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

        if key.origin != "EXTERNAL" {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "UnsupportedOperationException",
                format!("Key '{}' origin is '{}', not EXTERNAL", key.arn, key.origin),
            ));
        }

        // Drop the read lock — generating an RSA-2048 keypair is slow
        // (~50ms) and we'll need a write lock to stash the private half.
        let key_arn = key.arn.clone();
        drop(accounts);

        // Real RSA-2048 wrapping keypair. The public half goes back to
        // the caller as SubjectPublicKeyInfo DER so they can RSA-OAEP
        // encrypt their key material under it; the private half is
        // stashed keyed by the import token so ImportKeyMaterial can
        // unwrap on the way in.
        let (priv_der, pub_der) = super::asym::generate_keypair("RSA_2048")
            .map_err(|e| {
                AwsServiceError::aws_error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "KMSInternalException",
                    format!("failed to generate import wrapping key: {e}"),
                )
            })?
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "KMSInternalException",
                    "RSA_2048 keygen returned None",
                )
            })?;

        let import_token_bytes = rand_bytes(64);
        let import_token_b64 =
            base64::engine::general_purpose::STANDARD.encode(&import_token_bytes);
        let public_key_b64 = base64::engine::general_purpose::STANDARD.encode(&pub_der);

        // Valid for 24 hours
        let parameters_valid_to = Utc::now().timestamp() as f64 + 86400.0;

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        state.import_wrapping_keys.insert(
            import_token_b64.clone(),
            crate::state::ImportWrapEntry {
                private_key_der: priv_der,
                key_id: resolved.clone(),
            },
        );

        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&json!({
                "KeyId": key_arn,
                "ImportToken": import_token_b64,
                "PublicKey": public_key_b64,
                "ParametersValidTo": parameters_valid_to,
            }))
            .unwrap(),
        ))
    }

    pub(super) fn import_key_material(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let key_id = Self::require_key_id(&body)?;

        let import_token = body["ImportToken"].as_str().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                "ImportToken is required",
            )
        })?;

        let encrypted_key_material = body["EncryptedKeyMaterial"].as_str().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                "EncryptedKeyMaterial is required",
            )
        })?;

        let wrapping_algorithm = body["WrappingAlgorithm"]
            .as_str()
            .unwrap_or("RSAES_OAEP_SHA_256")
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

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let key = state.keys.get_mut(&resolved).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "NotFoundException",
                format!("Key '{key_id}' does not exist"),
            )
        })?;

        if key.origin != "EXTERNAL" {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "UnsupportedOperationException",
                format!("Key '{}' origin is '{}', not EXTERNAL", key.arn, key.origin),
            ));
        }

        // Look up the wrapping keypair stashed by GetParametersForImport
        // and verify the token belongs to this CMK.
        let entry = state
            .import_wrapping_keys
            .get(import_token)
            .cloned()
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidImportTokenException",
                    "ImportToken is invalid or expired",
                )
            })?;
        if entry.key_id != resolved {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidImportTokenException",
                "ImportToken was issued for a different key",
            ));
        }

        let wrapped_bytes = base64::engine::general_purpose::STANDARD
            .decode(encrypted_key_material)
            .map_err(|_| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ValidationException",
                    "EncryptedKeyMaterial is not valid base64",
                )
            })?;
        if wrapped_bytes.is_empty() {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                "EncryptedKeyMaterial must not be empty",
            ));
        }

        // RSA-OAEP unwrap with the wrapping private key. AWS rejects a
        // bad cipher with InvalidCiphertextException; mirror that.
        let unwrapped = super::asym::rsa_oaep_unwrap(
            &entry.private_key_der,
            &wrapping_algorithm,
            &wrapped_bytes,
        )
        .map_err(|_| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidCiphertextException",
                "EncryptedKeyMaterial could not be unwrapped",
            )
        })?;

        // AWS KMS requires symmetric imported key material to be exactly
        // 256 bits (32 bytes). Reject anything else before it reaches the
        // encrypt/decrypt XOR path — zero-length material there would
        // panic with a divide-by-zero (`byte ^ material[i % material.len()]`),
        // turning a malformed import into a server crash (DoS).
        if unwrapped.len() != 32 {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                format!(
                    "Key material must be exactly 256 bits (32 bytes); got {} bytes",
                    unwrapped.len()
                ),
            ));
        }

        key.imported_key_material = true;
        key.imported_material_bytes = Some(unwrapped);
        key.enabled = true;
        key.key_state = "Enabled".to_string();
        // Token is single-use; drop it now that the import succeeded
        // so a replay on the same token is rejected with the same
        // InvalidImportTokenException as an unknown token.
        state.import_wrapping_keys.remove(import_token);

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    pub(super) fn delete_imported_key_material(
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

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let key = state.keys.get_mut(&resolved).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "NotFoundException",
                format!("Key '{key_id}' does not exist"),
            )
        })?;

        if key.origin != "EXTERNAL" {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "UnsupportedOperationException",
                format!("Key '{}' origin is '{}', not EXTERNAL", key.arn, key.origin),
            ));
        }

        key.imported_key_material = false;
        key.imported_material_bytes = None;
        key.enabled = false;
        key.key_state = "PendingImport".to_string();

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    pub(super) fn create_custom_key_store(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();

        let name = body["CustomKeyStoreName"]
            .as_str()
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ValidationException",
                    "CustomKeyStoreName is required",
                )
            })?
            .to_string();

        validate_string_length("customKeyStoreName", &name, 1, 256)?;

        let store_type = body["CustomKeyStoreType"]
            .as_str()
            .unwrap_or("AWS_CLOUDHSM")
            .to_string();

        validate_optional_enum(
            "customKeyStoreType",
            Some(store_type.as_str()),
            &["AWS_CLOUDHSM", "EXTERNAL_KEY_STORE"],
        )?;

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);

        // Name must be unique
        if state
            .custom_key_stores
            .values()
            .any(|s| s.custom_key_store_name == name)
        {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "CustomKeyStoreNameInUseException",
                format!("Custom key store name '{name}' is already in use"),
            ));
        }

        let store_id = format!("cks-{}", Uuid::new_v4().as_simple());
        let now = Utc::now().timestamp() as f64;

        let store = CustomKeyStore {
            custom_key_store_id: store_id.clone(),
            custom_key_store_name: name,
            custom_key_store_type: store_type,
            cloud_hsm_cluster_id: body["CloudHsmClusterId"].as_str().map(|s| s.to_string()),
            trust_anchor_certificate: body["TrustAnchorCertificate"]
                .as_str()
                .map(|s| s.to_string()),
            connection_state: "DISCONNECTED".to_string(),
            creation_date: now,
            xks_proxy_uri_endpoint: body["XksProxyUriEndpoint"].as_str().map(|s| s.to_string()),
            xks_proxy_uri_path: body["XksProxyUriPath"].as_str().map(|s| s.to_string()),
            xks_proxy_vpc_endpoint_service_name: body["XksProxyVpcEndpointServiceName"]
                .as_str()
                .map(|s| s.to_string()),
            xks_proxy_connectivity: body["XksProxyConnectivity"].as_str().map(|s| s.to_string()),
        };

        state.custom_key_stores.insert(store_id.clone(), store);

        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&json!({ "CustomKeyStoreId": store_id })).unwrap(),
        ))
    }

    pub(super) fn delete_custom_key_store(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();

        let store_id = body["CustomKeyStoreId"]
            .as_str()
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ValidationException",
                    "CustomKeyStoreId is required",
                )
            })?
            .to_string();

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);

        let store = state.custom_key_stores.get(&store_id).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "CustomKeyStoreNotFoundException",
                format!("Custom key store '{store_id}' does not exist"),
            )
        })?;

        if store.connection_state == "CONNECTED" {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "CustomKeyStoreHasCMKsException",
                "Cannot delete a connected custom key store. Disconnect it first.",
            ));
        }

        state.custom_key_stores.remove(&store_id);

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    pub(super) fn describe_custom_key_stores(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        validate_optional_string_length(
            "customKeyStoreName",
            body["CustomKeyStoreName"].as_str(),
            1,
            256,
        )?;
        validate_optional_json_range("limit", &body["Limit"], 1, 1000)?;
        validate_optional_string_length("marker", body["Marker"].as_str(), 1, 1024)?;

        let filter_id = body["CustomKeyStoreId"].as_str();
        let filter_name = body["CustomKeyStoreName"].as_str();
        let limit = body["Limit"].as_i64().unwrap_or(1000) as usize;
        let marker = body["Marker"].as_str();

        let accounts = self.state.read();
        let empty = KmsState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);

        let mut stores: Vec<&CustomKeyStore> = state
            .custom_key_stores
            .values()
            .filter(|s| {
                if let Some(id) = filter_id {
                    return s.custom_key_store_id == id;
                }
                if let Some(name) = filter_name {
                    return s.custom_key_store_name == name;
                }
                true
            })
            .collect();

        stores.sort_by(|a, b| a.custom_key_store_id.cmp(&b.custom_key_store_id));

        // If filtering by ID and not found, return error
        if let Some(id) = filter_id {
            if stores.is_empty() {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "CustomKeyStoreNotFoundException",
                    format!("Custom key store '{id}' does not exist"),
                ));
            }
        }

        // Stores are sorted by custom_key_store_id and the marker is the last
        // id of the previous page, so resume at the first store strictly
        // greater than the marker. A strict `>` keeps the pager moving forward
        // even when the marked store was deleted between pages (a plain "find
        // marker, start after" would restart at page 1).
        let start = marker
            .map(|m| {
                stores
                    .iter()
                    .position(|s| s.custom_key_store_id.as_str() > m)
                    .unwrap_or(stores.len())
            })
            .unwrap_or(0);

        let page: Vec<_> = stores.iter().skip(start).take(limit).collect();
        let truncated = start + page.len() < stores.len();

        let entries: Vec<Value> = page.iter().map(|s| custom_key_store_json(s)).collect();

        let mut resp = json!({ "CustomKeyStores": entries, "Truncated": truncated });
        if truncated {
            if let Some(last) = page.last() {
                resp["NextMarker"] = json!(last.custom_key_store_id);
            }
        }

        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&resp).unwrap(),
        ))
    }

    pub(super) fn connect_custom_key_store(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();

        let store_id = body["CustomKeyStoreId"]
            .as_str()
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ValidationException",
                    "CustomKeyStoreId is required",
                )
            })?
            .to_string();

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);

        let store = state.custom_key_stores.get_mut(&store_id).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "CustomKeyStoreNotFoundException",
                format!("Custom key store '{store_id}' does not exist"),
            )
        })?;

        store.connection_state = "CONNECTED".to_string();

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    pub(super) fn disconnect_custom_key_store(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();

        let store_id = body["CustomKeyStoreId"]
            .as_str()
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ValidationException",
                    "CustomKeyStoreId is required",
                )
            })?
            .to_string();

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);

        let store = state.custom_key_stores.get_mut(&store_id).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "CustomKeyStoreNotFoundException",
                format!("Custom key store '{store_id}' does not exist"),
            )
        })?;

        store.connection_state = "DISCONNECTED".to_string();

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    pub(super) fn update_custom_key_store(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();

        let store_id = body["CustomKeyStoreId"]
            .as_str()
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ValidationException",
                    "CustomKeyStoreId is required",
                )
            })?
            .to_string();

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);

        // Check uniqueness of new name before borrowing store mutably
        if let Some(new_name) = body["NewCustomKeyStoreName"].as_str() {
            if state
                .custom_key_stores
                .values()
                .any(|s| s.custom_key_store_name == new_name && s.custom_key_store_id != store_id)
            {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "CustomKeyStoreNameInUseException",
                    format!("Custom key store name '{new_name}' is already in use"),
                ));
            }
        }

        let store = state.custom_key_stores.get_mut(&store_id).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "CustomKeyStoreNotFoundException",
                format!("Custom key store '{store_id}' does not exist"),
            )
        })?;

        if let Some(new_name) = body["NewCustomKeyStoreName"].as_str() {
            store.custom_key_store_name = new_name.to_string();
        }
        if let Some(v) = body["CloudHsmClusterId"].as_str() {
            store.cloud_hsm_cluster_id = Some(v.to_string());
        }
        if let Some(v) = body["KeyStorePassword"].as_str() {
            // In a real implementation this would update the password;
            // we just accept it silently.
            let _ = v;
        }
        if let Some(v) = body["XksProxyUriEndpoint"].as_str() {
            store.xks_proxy_uri_endpoint = Some(v.to_string());
        }
        if let Some(v) = body["XksProxyUriPath"].as_str() {
            store.xks_proxy_uri_path = Some(v.to_string());
        }
        if let Some(v) = body["XksProxyVpcEndpointServiceName"].as_str() {
            store.xks_proxy_vpc_endpoint_service_name = Some(v.to_string());
        }
        if let Some(v) = body["XksProxyConnectivity"].as_str() {
            store.xks_proxy_connectivity = Some(v.to_string());
        }

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }
}
