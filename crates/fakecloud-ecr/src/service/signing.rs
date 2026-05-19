//! `EcrService` `signing` family — extracted from service.rs by audit-2026-05-19.

use super::*;

impl EcrService {
    pub(super) fn get_signing_configuration(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = request.json_body();
        let account = target_account_id(request, &body);
        let accounts = self.state.read();
        let state = accounts.get(&account);
        let rules: Vec<Value> = state
            .and_then(|s| s.signing_configuration.as_ref())
            .map(|c| c.rules.clone())
            .unwrap_or_default();
        Ok(AwsResponse::ok_json(json!({
            "registryId": state.map(|s| s.account_id.clone()).unwrap_or_default(),
            "signingConfiguration": {"rules": rules},
        })))
    }

    pub(super) fn put_signing_configuration(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        use crate::signing::TrustedKey;
        use crate::state::SigningConfiguration;
        let body = request.json_body();
        let cfg = body
            .get("signingConfiguration")
            .ok_or_else(|| invalid_parameter("Missing required field: signingConfiguration"))?;
        let rules: Vec<Value> = cfg
            .get("rules")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();

        // Extract trusted keys from the rules. AWS's real signing-config
        // schema is nested; we accept the minimal fakecloud-friendly
        // shape `{trustedKeys: [{keyId, pem, algorithm}]}` per rule.
        // Rules that don't carry recognised keys are still
        // round-trippable via the raw `rules` passthrough.
        let mut trusted_keys: Vec<TrustedKey> = Vec::new();
        for rule in &rules {
            let keys = match rule.get("trustedKeys").and_then(|v| v.as_array()) {
                Some(k) => k,
                None => continue,
            };
            for k in keys {
                let key_id = k
                    .get("keyId")
                    .and_then(|v| v.as_str())
                    .unwrap_or_default()
                    .to_string();
                let pem = match k.get("pem").and_then(|v| v.as_str()) {
                    Some(p) => p.to_string(),
                    None => continue,
                };
                let algorithm = k
                    .get("algorithm")
                    .and_then(|v| v.as_str())
                    .unwrap_or("ECDSA-P256")
                    .to_string();
                // Validate the PEM up front so bad rules fail
                // PutSigningConfiguration rather than silently skipping
                // verification at describe time.
                if <p256::ecdsa::VerifyingKey as p256::pkcs8::DecodePublicKey>::from_public_key_pem(
                    &pem,
                )
                .is_err()
                {
                    return Err(invalid_parameter(format!(
                        "trusted key {key_id} is not a valid ECDSA-P256 PEM-encoded public key"
                    )));
                }
                trusted_keys.push(TrustedKey {
                    key_id,
                    pem,
                    algorithm,
                });
            }
        }

        let account = target_account_id(request, &body);
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&account);
        state.signing_configuration = Some(SigningConfiguration {
            rules: rules.clone(),
            trusted_keys,
        });
        Ok(AwsResponse::ok_json(json!({
            "signingConfiguration": {"rules": rules},
        })))
    }

    pub(super) fn delete_signing_configuration(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = request.json_body();
        let account = target_account_id(request, &body);
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&account);
        state.signing_configuration = None;
        Ok(AwsResponse::ok_json(json!({})))
    }

    pub(super) fn describe_image_signing_status(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = request.json_body();
        let name = req_str(&body, "repositoryName")?.to_string();
        let image_id = body
            .get("imageId")
            .cloned()
            .ok_or_else(|| invalid_parameter("Missing imageId"))?;
        let account = target_account_id(request, &body);
        let accounts = self.state.read();
        let state = accounts
            .get(&account)
            .ok_or_else(|| repository_not_found(&name))?;
        let repo = state
            .repositories
            .get(&name)
            .ok_or_else(|| repository_not_found(&name))?;
        let image_digest = resolve_image_digest(repo, &image_id)
            .ok_or_else(|| image_not_found(&name, &image_id))?;

        let trusted_keys: &[crate::signing::TrustedKey] = state
            .signing_configuration
            .as_ref()
            .map(|c| c.trusted_keys.as_slice())
            .unwrap_or(&[]);

        // Locate the cosign companion signature tag
        // (`sha256-<hex>.sig`) in the same repo. Absent -> UNSIGNED.
        let sig_tag = match crate::signing::companion_sig_tag(&image_digest) {
            Some(t) => t,
            None => {
                return Ok(AwsResponse::ok_json(json!({
                    "registryId": repo.registry_id,
                    "repositoryName": name,
                    "imageId": image_id,
                    "imageSignatures": [],
                    "signingStatus": "UNSIGNED",
                })));
            }
        };
        let sig_manifest_digest = match repo.image_tags.get(&sig_tag) {
            Some(d) => d,
            None => {
                return Ok(AwsResponse::ok_json(json!({
                    "registryId": repo.registry_id,
                    "repositoryName": name,
                    "imageId": image_id,
                    "imageSignatures": [],
                    "signingStatus": "UNSIGNED",
                })));
            }
        };
        let sig_image = match repo.images.get(sig_manifest_digest) {
            Some(i) => i,
            None => {
                return Ok(AwsResponse::ok_json(json!({
                    "registryId": repo.registry_id,
                    "repositoryName": name,
                    "imageId": image_id,
                    "imageSignatures": [],
                    "signingStatus": "UNSIGNED",
                })));
            }
        };

        let manifest_json: Value = match serde_json::from_str(&sig_image.image_manifest) {
            Ok(v) => v,
            Err(_) => {
                return Ok(AwsResponse::ok_json(json!({
                    "registryId": repo.registry_id,
                    "repositoryName": name,
                    "imageId": image_id,
                    "imageSignatures": [],
                    "signingStatus": "INVALID_SIGNATURE",
                })));
            }
        };
        let (layer_digest, signature_b64) =
            match crate::signing::extract_signature_annotation(&manifest_json) {
                Some(x) => x,
                None => {
                    return Ok(AwsResponse::ok_json(json!({
                        "registryId": repo.registry_id,
                        "repositoryName": name,
                        "imageId": image_id,
                        "imageSignatures": [],
                        "signingStatus": "UNSIGNED",
                    })));
                }
            };

        // Fetch the payload (signed bytes = simple-signing JSON blob).
        let payload_bytes: Vec<u8> = match repo.layers.get(&layer_digest) {
            Some(layer) => base64::Engine::decode(
                &base64::engine::general_purpose::STANDARD,
                layer.blob_b64.as_bytes(),
            )
            .unwrap_or_default(),
            None => {
                return Ok(AwsResponse::ok_json(json!({
                    "registryId": repo.registry_id,
                    "repositoryName": name,
                    "imageId": image_id,
                    "imageSignatures": [],
                    "signingStatus": "UNSIGNED",
                })));
            }
        };

        // The payload must name the signed image — catches copy-paste
        // of one image's signature onto another.
        if let Some(named) = crate::signing::referenced_image_digest(&payload_bytes) {
            if named != image_digest {
                return Ok(AwsResponse::ok_json(json!({
                    "registryId": repo.registry_id,
                    "repositoryName": name,
                    "imageId": image_id,
                    "imageSignatures": [],
                    "signingStatus": "INVALID_SIGNATURE",
                    "statusReason": "signature payload references a different image digest",
                })));
            }
        }

        // Verify against each trusted key until one matches.
        let mut matched: Option<&crate::signing::TrustedKey> = None;
        for key in trusted_keys {
            if crate::signing::verify_cosign_signature(&key.pem, &payload_bytes, &signature_b64)
                .is_ok()
            {
                matched = Some(key);
                break;
            }
        }

        let mut response = json!({
            "registryId": repo.registry_id,
            "repositoryName": name,
            "imageId": image_id,
        });
        if let Some(key) = matched {
            response["imageSignatures"] = json!([{
                "signatureFormat": "COSIGN",
                "keyId": key.key_id,
                "algorithm": key.algorithm,
                "valid": true,
            }]);
            response["signingStatus"] = json!("SIGNED");
        } else if trusted_keys.is_empty() {
            // A valid-looking signature exists but no trusted keys are
            // configured to verify against — surface the signature but
            // mark it UNVERIFIED so the caller knows to configure keys.
            response["imageSignatures"] = json!([{
                "signatureFormat": "COSIGN",
                "valid": false,
                "statusReason": "no trusted keys configured"
            }]);
            response["signingStatus"] = json!("UNVERIFIED");
        } else {
            response["imageSignatures"] = json!([{
                "signatureFormat": "COSIGN",
                "valid": false,
                "statusReason": "signature did not match any trusted key"
            }]);
            response["signingStatus"] = json!("INVALID_SIGNATURE");
        }
        Ok(AwsResponse::ok_json(response))
    }

    pub(super) fn register_pull_time_update_exclusion(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        use crate::state::PullTimeExclusion;
        let body = request.json_body();
        let principal_arn = req_str(&body, "principalArn")?.to_string();
        // Smithy `com.amazonaws.ecr#PrincipalArn` length 0..=200.
        validate_string_length("principalArn", &principal_arn, 0, 200)?;
        let account = target_account_id(request, &body);
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&account);
        state
            .pull_time_exclusions
            .entry(principal_arn.clone())
            .or_insert_with(|| PullTimeExclusion {
                principal_arn: principal_arn.clone(),
                registered_at: Utc::now(),
            });
        Ok(AwsResponse::ok_json(json!({
            "principalArn": principal_arn,
        })))
    }

    pub(super) fn deregister_pull_time_update_exclusion(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = request.json_body();
        let principal_arn = req_str(&body, "principalArn")?.to_string();
        validate_string_length("principalArn", &principal_arn, 0, 200)?;
        let account = target_account_id(request, &body);
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&account);
        state.pull_time_exclusions.remove(&principal_arn);
        Ok(AwsResponse::ok_json(json!({
            "principalArn": principal_arn,
        })))
    }

    pub(super) fn list_pull_time_update_exclusions(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = request.json_body();
        validate_max_results(&body)?;
        let max_results = body
            .get("maxResults")
            .and_then(|v| v.as_u64())
            .map(|n| n as usize);
        let offset = match body.get("nextToken").and_then(|v| v.as_str()) {
            Some(raw) => raw.parse::<usize>().map_err(|_| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterException",
                    "The specified parameter is invalid: nextToken",
                )
            })?,
            None => 0,
        };
        let account = target_account_id(request, &body);
        let accounts = self.state.read();
        let state = accounts.get(&account);
        let mut all: Vec<Value> = state
            .map(|s| {
                s.pull_time_exclusions
                    .values()
                    .map(|e| {
                        json!({
                            "principalArn": e.principal_arn,
                            "registeredAt": e.registered_at.timestamp(),
                        })
                    })
                    .collect()
            })
            .unwrap_or_default();
        let total = all.len();
        let start = offset.min(total);
        all.drain(..start);
        let next_token = match max_results {
            Some(n) if all.len() > n => {
                all.truncate(n);
                Some((start + n).to_string())
            }
            _ => None,
        };
        let mut out = json!({ "pullTimeUpdateExclusions": all });
        if let Some(tok) = next_token {
            out["nextToken"] = json!(tok);
        }
        Ok(AwsResponse::ok_json(out))
    }
}
