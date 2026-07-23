//! `EcrService` `layers` family — extracted from service.rs by audit-2026-05-19.

use super::*;

impl EcrService {
    pub(super) fn batch_check_layer_availability(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = request.json_body();
        let name = req_str(&body, "repositoryName")?.to_string();
        let digests: Vec<String> = body
            .get("layerDigests")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                    .collect()
            })
            .unwrap_or_default();
        if digests.is_empty() {
            return Err(invalid_parameter(
                "At least one layerDigest must be supplied to BatchCheckLayerAvailability",
            ));
        }
        let account = target_account_id(request, &body);
        let accounts = self.state.read();
        let state = accounts
            .get(&account)
            .ok_or_else(|| repository_not_found(&name))?;
        let repo = state
            .repositories
            .get(&name)
            .ok_or_else(|| repository_not_found(&name))?;
        check_repo_policy(
            &account,
            &request.account_id,
            &repo.repository_arn,
            &name,
            repo.policy.as_deref(),
            "ecr:BatchCheckLayerAvailability",
        )?;
        let mut layers: Vec<Value> = Vec::new();
        let mut failures: Vec<Value> = Vec::new();
        for digest in &digests {
            match repo.layers.get(digest) {
                Some(layer) => layers.push(json!({
                    "layerDigest": layer.digest,
                    "layerAvailability": "AVAILABLE",
                    "layerSize": layer.size,
                    "mediaType": layer.media_type,
                })),
                None => failures.push(json!({
                    "layerDigest": digest,
                    "failureCode": "MissingLayerDigest",
                    "failureReason": "Layer not found in repository",
                })),
            }
        }
        Ok(AwsResponse::ok_json(json!({
            "layers": layers,
            "failures": failures,
        })))
    }

    pub(super) fn get_download_url_for_layer(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = request.json_body();
        let name = req_str(&body, "repositoryName")?.to_string();
        let digest = req_str(&body, "layerDigest")?.to_string();
        let account = target_account_id(request, &body);
        let mut accounts = self.state.write();
        let state = accounts
            .get_mut(&account)
            .ok_or_else(|| repository_not_found(&name))?;
        let exclusions = pull_time_exclusion_set(state);
        let repo = state
            .repositories
            .get_mut(&name)
            .ok_or_else(|| repository_not_found(&name))?;
        check_repo_policy(
            &account,
            &request.account_id,
            &repo.repository_arn,
            &name,
            repo.policy.as_deref(),
            "ecr:GetDownloadUrlForLayer",
        )?;
        if !repo.layers.contains_key(&digest) {
            return Err(layer_not_found(&digest, &name));
        }
        // Pull bookkeeping: the OCI client requested a layer blob, which
        // means at least one image whose manifest references that layer
        // is being pulled. Touch every such image so DescribeImages
        // reflects the access. Don't touch unrelated images.
        let mut touched: Vec<String> = Vec::new();
        for (img_digest, img) in &repo.images {
            let parsed: Value = match serde_json::from_str(&img.image_manifest) {
                Ok(v) => v,
                Err(_) => continue,
            };
            let references = parsed
                .get("layers")
                .and_then(|v| v.as_array())
                .map(|arr| {
                    arr.iter()
                        .any(|l| l.get("digest").and_then(|d| d.as_str()) == Some(digest.as_str()))
                })
                .unwrap_or(false);
            if references {
                touched.push(img_digest.clone());
            }
        }
        let caller_arn = request.principal.as_ref().map(|p| p.arn.as_str());
        touch_image_pull(repo, &touched, caller_arn, &exclusions);
        // The OCI v2 endpoint hosts `/v2/<name>/blobs/<digest>` — return
        // that absolute URL so callers that trust the endpoint they're
        // already talking to can resolve it.
        let endpoint = accounts.endpoint();
        let url = format!(
            "{}/v2/{}/blobs/{}",
            endpoint.trim_end_matches('/'),
            name,
            digest
        );
        Ok(AwsResponse::ok_json(json!({
            "downloadUrl": url,
            "layerDigest": digest,
        })))
    }

    pub(super) fn initiate_layer_upload(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = request.json_body();
        let name = req_str(&body, "repositoryName")?.to_string();
        let account = target_account_id(request, &body);
        let mut accounts = self.state.write();
        let state = accounts
            .get_mut(&account)
            .ok_or_else(|| repository_not_found(&name))?;
        let repo = state
            .repositories
            .get(&name)
            .ok_or_else(|| repository_not_found(&name))?;
        check_repo_policy(
            &account,
            &request.account_id,
            &repo.repository_arn,
            &name,
            repo.policy.as_deref(),
            "ecr:InitiateLayerUpload",
        )?;
        let upload_id = Uuid::new_v4().to_string();
        let spool = crate::oci::create_upload_spool(&upload_id).map_err(|e| {
            AwsServiceError::aws_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "InternalError",
                format!("failed to create upload spool: {e}"),
            )
        })?;
        state.layer_uploads.insert(
            upload_id.clone(),
            LayerUpload {
                upload_id: upload_id.clone(),
                repository_name: name,
                created_at: Utc::now(),
                spool_path: spool.to_string_lossy().to_string(),
                last_byte_received: 0,
                append_in_flight: false,
            },
        );
        Ok(AwsResponse::ok_json(json!({
            "uploadId": upload_id,
            // Matches the real AWS default of 10 MiB.
            "partSize": 10_485_760u64,
        })))
    }

    pub(super) async fn upload_layer_part(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = request.json_body();
        let name = req_str(&body, "repositoryName")?.to_string();
        let upload_id = req_str(&body, "uploadId")?.to_string();
        let first_byte = body
            .get("partFirstByte")
            .and_then(|v| v.as_u64())
            .ok_or_else(|| invalid_parameter("Missing partFirstByte"))?;
        let last_byte = body
            .get("partLastByte")
            .and_then(|v| v.as_u64())
            .ok_or_else(|| invalid_parameter("Missing partLastByte"))?;
        let part_blob_b64 = req_str(&body, "layerPartBlob")?.to_string();
        let part_bytes = B64
            .decode(part_blob_b64.as_bytes())
            .map_err(|_| invalid_layer("layerPartBlob is not valid base64"))?;
        let account = target_account_id(request, &body);
        // Validate under the lock, capture the spool path, then release the
        // lock before touching the filesystem so the blocking append never
        // runs while the state lock is held.
        let spool = {
            let mut accounts = self.state.write();
            let state = accounts
                .get_mut(&account)
                .ok_or_else(|| repository_not_found(&name))?;
            let repo = state
                .repositories
                .get(&name)
                .ok_or_else(|| repository_not_found(&name))?;
            check_repo_policy(
                &account,
                &request.account_id,
                &repo.repository_arn,
                &name,
                repo.policy.as_deref(),
                "ecr:UploadLayerPart",
            )?;
            let upload = state
                .layer_uploads
                .get_mut(&upload_id)
                .ok_or_else(|| upload_not_found(&upload_id))?;
            if upload.repository_name != name {
                return Err(upload_not_found(&upload_id));
            }
            // A part for this upload is already being appended (validated and
            // reserved here, appended with the lock released below). Reject a
            // concurrent second part BEFORE it appends so two parts racing on
            // the same start offset can't both write into the spool.
            if upload.append_in_flight {
                return Err(invalid_layer(
                    "Layer part upload out of order: another part is already being appended",
                ));
            }
            if first_byte != upload.last_byte_received {
                return Err(invalid_layer(format!(
                    "Layer part upload out of order: expected partFirstByte {} got {}",
                    upload.last_byte_received, first_byte,
                )));
            }
            let expected_len = last_byte
                .checked_sub(first_byte)
                .and_then(|d| d.checked_add(1))
                .ok_or_else(|| invalid_layer("partLastByte < partFirstByte"))?;
            if part_bytes.len() as u64 != expected_len {
                return Err(invalid_layer(format!(
                    "Layer part size mismatch: bytes {} doesn't match range [{first_byte}, {last_byte}]",
                    part_bytes.len()
                )));
            }
            // Reserve the append. The flag is cleared once the append
            // completes (below) or fails (the map entry is left intact but
            // unreserved so the client can retry the same offset).
            upload.append_in_flight = true;
            std::path::PathBuf::from(&upload.spool_path)
        };

        // Helper: clear the in-flight reservation. Used on the append-failure
        // path so a failed append doesn't wedge the upload as permanently
        // "in flight" and block later retries at the same offset.
        let clear_reservation = || {
            let mut accounts = self.state.write();
            if let Some(state) = accounts.get_mut(&account) {
                if let Some(upload) = state.layer_uploads.get_mut(&upload_id) {
                    upload.append_in_flight = false;
                }
            }
        };

        // Append the (potentially multi-hundred-MB) chunk on a blocking
        // thread so the JSON `aws ecr` upload path doesn't stall a tokio
        // worker (bug-audit 2026-06-13, 3.2). The OCI `docker push` path
        // already streams asynchronously.
        let append_spool = spool.clone();
        let append_result = tokio::task::spawn_blocking(move || {
            crate::oci::append_bytes_sync(&append_spool, &part_bytes)
        })
        .await;
        let append_result = match append_result {
            Ok(inner) => inner,
            Err(e) => {
                clear_reservation();
                return Err(AwsServiceError::aws_error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "InternalError",
                    format!("layer part append task failed: {e}"),
                ));
            }
        };
        if let Err(e) = append_result {
            clear_reservation();
            return Err(AwsServiceError::aws_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "InternalError",
                format!("failed to append upload chunk: {e}"),
            ));
        }

        // Re-acquire the lock to advance the received-byte cursor and clear
        // the reservation. Because the reservation blocked any concurrent
        // append, the cursor is exactly where we left it.
        let mut accounts = self.state.write();
        let state = accounts
            .get_mut(&account)
            .ok_or_else(|| repository_not_found(&name))?;
        {
            let upload = state
                .layer_uploads
                .get_mut(&upload_id)
                .ok_or_else(|| upload_not_found(&upload_id))?;
            upload.last_byte_received = last_byte + 1;
            upload.append_in_flight = false;
        }
        let registry_id = state.registry_id();
        Ok(AwsResponse::ok_json(json!({
            "registryId": registry_id,
            "repositoryName": name,
            "uploadId": upload_id,
            "lastByteReceived": last_byte,
        })))
    }

    pub(super) async fn complete_layer_upload(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = request.json_body();
        let name = req_str(&body, "repositoryName")?.to_string();
        let upload_id = req_str(&body, "uploadId")?.to_string();
        let digests: Vec<String> = body
            .get("layerDigests")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                    .collect()
            })
            .unwrap_or_default();
        if digests.is_empty() {
            return Err(invalid_parameter(
                "At least one layerDigest must be supplied to CompleteLayerUpload",
            ));
        }
        let account = target_account_id(request, &body);
        // Validate under the lock and capture the spool path, then release
        // the lock before reading and hashing the (potentially
        // multi-hundred-MB) spool file.
        let spool = {
            let mut accounts = self.state.write();
            let state = accounts
                .get_mut(&account)
                .ok_or_else(|| repository_not_found(&name))?;
            let repo = state
                .repositories
                .get(&name)
                .ok_or_else(|| repository_not_found(&name))?;
            check_repo_policy(
                &account,
                &request.account_id,
                &repo.repository_arn,
                &name,
                repo.policy.as_deref(),
                "ecr:CompleteLayerUpload",
            )?;
            // Peek, validate, then commit — so a digest mismatch lets the
            // caller retry CompleteLayerUpload with the correct digest
            // instead of having to re-upload the entire blob.
            let upload = state
                .layer_uploads
                .get(&upload_id)
                .ok_or_else(|| upload_not_found(&upload_id))?;
            if upload.repository_name != name {
                return Err(upload_not_found(&upload_id));
            }
            std::path::PathBuf::from(&upload.spool_path)
        };

        // Read the full spool and compute its SHA-256 on a blocking thread so
        // the JSON `aws ecr` CompleteLayerUpload path doesn't stall a tokio
        // worker (bug-audit 2026-06-13, 3.2). The OCI `docker push` path
        // already streams the blob asynchronously.
        let read_spool = spool.clone();
        let (blob_bytes, computed) =
            tokio::task::spawn_blocking(move || -> std::io::Result<(Vec<u8>, String)> {
                let bytes = crate::oci::read_spool(&read_spool)?;
                let digest = sha256_digest(&bytes);
                Ok((bytes, digest))
            })
            .await
            .map_err(|e| {
                AwsServiceError::aws_error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "InternalError",
                    format!("layer read/hash task failed: {e}"),
                )
            })?
            .map_err(|e| {
                AwsServiceError::aws_error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "InternalError",
                    format!("failed to read upload spool: {e}"),
                )
            })?;

        if !digests.iter().any(|d| d == &computed) {
            // Spool stays — caller can retry with the correct digest
            // without re-uploading every UploadLayerPart chunk.
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "LayerDigestMismatchException",
                format!(
                    "The layer digest from the client ({}) does not match the digest of the received bytes ({computed})",
                    digests.join(",")
                ),
            ));
        }

        // Re-acquire to commit: remove the upload record and unlink the spool.
        {
            let mut accounts = self.state.write();
            let state = accounts
                .get_mut(&account)
                .ok_or_else(|| repository_not_found(&name))?;
            if state.layer_uploads.remove(&upload_id).is_none() {
                return Err(upload_not_found(&upload_id));
            }
        }
        crate::oci::unlink_spool(&spool);
        let size = blob_bytes.len() as u64;
        let (stored_bytes, encrypted_with) =
            crate::oci::encrypt_layer_bytes(self, &account, &name, &blob_bytes);
        let mut accounts = self.state.write();
        let state = accounts
            .get_mut(&account)
            .ok_or_else(|| repository_not_found(&name))?;
        let repo = state
            .repositories
            .get_mut(&name)
            .ok_or_else(|| repository_not_found(&name))?;
        repo.layers.insert(
            computed.clone(),
            Layer {
                digest: computed.clone(),
                size,
                blob_b64: B64.encode(&stored_bytes),
                media_type: "application/vnd.docker.image.rootfs.diff.tar.gzip".to_string(),
                encrypted_with_kms_key: encrypted_with,
            },
        );
        let registry_id = repo.registry_id.clone();
        Ok(AwsResponse::ok_json(json!({
            "registryId": registry_id,
            "repositoryName": name,
            "uploadId": upload_id,
            "layerDigest": computed,
        })))
    }
}

#[cfg(test)]
mod concurrency_tests {
    use crate::service::EcrService;
    use crate::state::{EcrState, Repository, SharedEcrState};
    use base64::engine::general_purpose::STANDARD as B64;
    use base64::Engine;
    use bytes::Bytes;
    use fakecloud_core::multi_account::MultiAccountState;
    use fakecloud_core::service::AwsRequest;
    use http::{HeaderMap, Method};
    use parking_lot::RwLock;
    use serde_json::{json, Value};
    use std::collections::HashMap;
    use std::sync::Arc;

    const ACCOUNT: &str = "111111111111";

    fn make_request(body: Value) -> AwsRequest {
        AwsRequest {
            service: "ecr".into(),
            action: "UploadLayerPart".into(),
            region: "us-east-1".into(),
            account_id: ACCOUNT.into(),
            request_id: "req-1".into(),
            headers: HeaderMap::new(),
            query_params: HashMap::new(),
            body: Bytes::from(serde_json::to_vec(&body).unwrap()),
            body_stream: parking_lot::Mutex::new(None),
            path_segments: vec![],
            raw_path: "/".into(),
            raw_query: String::new(),
            method: Method::POST,
            is_query_protocol: false,
            access_key_id: None,
            principal: None,
        }
    }

    fn fixture() -> (EcrService, SharedEcrState) {
        let mut mas: MultiAccountState<EcrState> =
            MultiAccountState::new(ACCOUNT, "us-east-1", "http://fakecloud:4566");
        let state = mas.get_or_create(ACCOUNT);
        let arn = state.repository_arn("app");
        let repo = Repository::new("app", arn, ACCOUNT, "fakecloud:4566");
        state.repositories.insert("app".to_string(), repo);
        let shared: SharedEcrState = Arc::new(RwLock::new(mas));
        let svc = EcrService::new(shared.clone());
        (svc, shared)
    }

    fn initiate(svc: &EcrService) -> String {
        let req = make_request(json!({ "repositoryName": "app" }));
        let resp = svc
            .initiate_layer_upload(&req)
            .expect("initiate should succeed");
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        body["uploadId"].as_str().unwrap().to_string()
    }

    fn part(upload_id: &str, first: u64, last: u64, bytes: &[u8]) -> AwsRequest {
        make_request(json!({
            "repositoryName": "app",
            "uploadId": upload_id,
            "partFirstByte": first,
            "partLastByte": last,
            "layerPartBlob": B64.encode(bytes),
        }))
    }

    #[tokio::test]
    async fn second_same_offset_part_is_rejected() {
        let (svc, _shared) = fixture();
        let upload_id = initiate(&svc);

        // First part at offset 0 succeeds and advances the cursor.
        let ok = svc
            .upload_layer_part(&part(&upload_id, 0, 3, b"test"))
            .await
            .expect("first part should succeed");
        let ok_body: Value = serde_json::from_slice(ok.body.expect_bytes()).unwrap();
        assert_eq!(ok_body["lastByteReceived"].as_u64(), Some(3));

        // A second part starting at the same offset 0 is now out of order and
        // must be rejected, rather than appended into the spool.
        let err = svc
            .upload_layer_part(&part(&upload_id, 0, 3, b"test"))
            .await
            .err()
            .expect("second same-offset part should be rejected");
        assert!(
            format!("{err:?}").contains("out of order"),
            "unexpected error: {err:?}"
        );
    }

    #[tokio::test]
    async fn in_flight_reservation_rejects_concurrent_part() {
        let (svc, shared) = fixture();
        let upload_id = initiate(&svc);

        // Simulate a first part that has passed validation and is mid-append:
        // the reservation flag is set while its file append runs with the
        // state lock released.
        {
            let mut guard = shared.write();
            let state = guard.get_mut(ACCOUNT).unwrap();
            state
                .layer_uploads
                .get_mut(&upload_id)
                .unwrap()
                .append_in_flight = true;
        }

        // A concurrent part for the same upload -- even at the correct next
        // offset -- must be rejected before it appends, so two parts can't both
        // write into the spool and corrupt it.
        let err = svc
            .upload_layer_part(&part(&upload_id, 0, 3, b"test"))
            .await
            .err()
            .expect("in-flight upload should reject a concurrent part");
        assert!(
            format!("{err:?}").contains("already being appended"),
            "unexpected error: {err:?}"
        );
    }
}
