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
            },
        );
        Ok(AwsResponse::ok_json(json!({
            "uploadId": upload_id,
            // Matches the real AWS default of 10 MiB.
            "partSize": 10_485_760u64,
        })))
    }

    pub(super) fn upload_layer_part(
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
        let spool = std::path::PathBuf::from(&upload.spool_path);
        crate::oci::append_bytes_sync(&spool, &part_bytes).map_err(|e| {
            AwsServiceError::aws_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "InternalError",
                format!("failed to append upload chunk: {e}"),
            )
        })?;
        upload.last_byte_received = last_byte + 1;
        Ok(AwsResponse::ok_json(json!({
            "registryId": state.registry_id(),
            "repositoryName": name,
            "uploadId": upload_id,
            "lastByteReceived": last_byte,
        })))
    }

    pub(super) fn complete_layer_upload(
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
        let spool = std::path::PathBuf::from(&upload.spool_path);
        let blob_bytes = crate::oci::read_spool(&spool).map_err(|e| {
            AwsServiceError::aws_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "InternalError",
                format!("failed to read upload spool: {e}"),
            )
        })?;
        let computed = sha256_digest(&blob_bytes);
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
        let _upload = state.layer_uploads.remove(&upload_id).unwrap();
        crate::oci::unlink_spool(&spool);
        let size = blob_bytes.len() as u64;
        // Drop the write guard before the KMS encrypt call (which takes
        // its own lock). Re-acquire to insert.
        drop(accounts);
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
