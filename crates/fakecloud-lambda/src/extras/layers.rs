//! `LambdaService` `layers` family — extracted from service.rs by audit-2026-05-19.

use super::*;

impl LambdaService {
    // ── Layers ──

    pub(super) fn publish_layer_version(
        &self,
        layer_name: &str,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        if layer_name.is_empty() {
            return Err(missing("LayerName"));
        }
        let limit = if layer_name.starts_with("arn:") {
            200
        } else {
            140
        };
        if layer_name.chars().count() > limit {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterValueException",
                "LayerName exceeds the 140-character maximum",
            ));
        }
        let body = body(req);
        // `Content` is `@required` on `PublishLayerVersionRequest` —
        // reject when missing rather than silently publishing a
        // zero-byte layer.
        if body.get("Content").is_none() || body["Content"].is_null() {
            return Err(missing("Content"));
        }
        // `Description` is bound to Smithy's `Description` shape
        // (`length 0..256`). Reject overlong values up front.
        if let Some(desc) = body["Description"].as_str() {
            if desc.chars().count() > 256 {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterValueException",
                    "Description exceeds the 256-character maximum",
                ));
            }
        }
        // `LicenseInfo` Smithy shape: `length 0..512`.
        if let Some(li) = body["LicenseInfo"].as_str() {
            if li.chars().count() > 512 {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterValueException",
                    "LicenseInfo exceeds the 512-character maximum",
                ));
            }
        }
        let zip_bytes: Option<Vec<u8>> = match body["Content"]["ZipFile"].as_str() {
            Some(b64) => Some(
                base64::Engine::decode(&base64::engine::general_purpose::STANDARD, b64).map_err(
                    |_| {
                        AwsServiceError::aws_error(
                            StatusCode::BAD_REQUEST,
                            "InvalidParameterValueException",
                            "Could not decode Content.ZipFile: invalid base64",
                        )
                    },
                )?,
            ),
            None => None,
        };
        let (code_sha256, code_size) = match zip_bytes.as_deref() {
            Some(bytes) => {
                let mut hasher = Sha256::new();
                hasher.update(bytes);
                let digest = hasher.finalize();
                (
                    base64::Engine::encode(&base64::engine::general_purpose::STANDARD, digest),
                    bytes.len() as i64,
                )
            }
            None => (String::new(), 0),
        };

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let account_id = state.account_id.clone();
        let layer = state
            .layers
            .entry(layer_name.to_string())
            .or_insert_with(|| Layer {
                layer_name: layer_name.to_string(),
                layer_arn: format!(
                    "arn:aws:lambda:{}:{}:layer:{}",
                    state.region, state.account_id, layer_name
                ),
                versions: Vec::new(),
            });
        let next_version = (layer.versions.len() as i64) + 1;
        let version_arn = format!("{}:{}", layer.layer_arn, next_version);
        let runtimes: Vec<String> = body["CompatibleRuntimes"]
            .as_array()
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();
        let architectures: Vec<String> = body["CompatibleArchitectures"]
            .as_array()
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();
        let layer_arn = layer.layer_arn.clone();
        let lv = LayerVersion {
            version: next_version,
            layer_version_arn: version_arn.clone(),
            description: body["Description"].as_str().unwrap_or("").to_string(),
            created_date: Utc::now(),
            compatible_runtimes: runtimes,
            license_info: body["LicenseInfo"].as_str().unwrap_or("").to_string(),
            policy: None,
            code_zip: zip_bytes,
            code_sha256: code_sha256.clone(),
            code_size,
            compatible_architectures: architectures,
        };
        layer.versions.push(lv.clone());
        let location = layer_content_url(req, &account_id, layer_name, next_version);
        ok(json!({
            "LayerArn": layer_arn,
            "LayerVersionArn": version_arn,
            "Version": next_version,
            "Description": lv.description,
            "CreatedDate": lv.created_date.format("%Y-%m-%dT%H:%M:%S.%3fZ").to_string(),
            "CompatibleRuntimes": lv.compatible_runtimes,
            "CompatibleArchitectures": lv.compatible_architectures,
            "LicenseInfo": lv.license_info,
            "Content": {
                "Location": location,
                "CodeSha256": code_sha256,
                "CodeSize": code_size,
            },
        }))
    }

    pub(super) fn list_layers(
        &self,
        account_id: &str,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let marker = req.query_params.get("Marker").map(String::as_str);
        let max_items = crate::service::marker_page_size(req);
        let region = self.region_for(account_id);
        self.with_state_read(account_id, &region, |state| {
            let layers: Vec<Value> = state
                .layers
                .values()
                .map(|l| {
                    json!({
                        "LayerName": l.layer_name,
                        "LayerArn": l.layer_arn,
                        "LatestMatchingVersion": l.versions.last().map(|v| json!({
                            "LayerVersionArn": v.layer_version_arn,
                            "Version": v.version,
                            "Description": v.description,
                            "CreatedDate": v.created_date.format("%Y-%m-%dT%H:%M:%S.%3fZ").to_string(),
                            "CompatibleRuntimes": v.compatible_runtimes,
                            "CompatibleArchitectures": v.compatible_architectures,
                        })),
                    })
                })
                .collect();
            let (page, next_marker) =
                crate::service::paginate_marker(layers, marker, max_items, |l| {
                    l["LayerName"].as_str().unwrap_or_default().to_string()
                });
            ok(json!({"Layers": page, "NextMarker": next_marker}))
        })
    }

    pub(super) fn list_layer_versions(
        &self,
        layer_name: &str,
        account_id: &str,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let marker = req.query_params.get("Marker").map(String::as_str);
        let max_items = crate::service::marker_page_size(req);
        let region = self.region_for(account_id);
        self.with_state_read(account_id, &region, |state| {
            let versions: Vec<Value> = state
                .layers
                .get(layer_name)
                .map(|l| {
                    l.versions
                        .iter()
                        .map(|v| {
                            json!({
                                "LayerVersionArn": v.layer_version_arn,
                                "Version": v.version,
                                "Description": v.description,
                                "CreatedDate": v.created_date.format("%Y-%m-%dT%H:%M:%S.%3fZ").to_string(),
                                "CompatibleRuntimes": v.compatible_runtimes,
                                "CompatibleArchitectures": v.compatible_architectures,
                                "LicenseInfo": v.license_info,
                            })
                        })
                        .collect()
                })
                .unwrap_or_default();
            // Paginate by Version using a zero-padded key so the string sort
            // matches numeric order (preserving the existing ascending order).
            let (page, next_marker) =
                crate::service::paginate_marker(versions, marker, max_items, |v| {
                    format!("{:020}", v["Version"].as_i64().unwrap_or(0))
                });
            ok(json!({"LayerVersions": page, "NextMarker": next_marker}))
        })
    }

    pub(super) fn get_layer_version(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let layer_name = req.path_segments.get(2).cloned().unwrap_or_default();
        let version: i64 = req
            .path_segments
            .get(4)
            .and_then(|s| s.parse().ok())
            .ok_or_else(|| missing("VersionNumber"))?;
        let region = self.region_for(&req.account_id);
        let location = layer_content_url(req, &req.account_id, &layer_name, version);
        self.with_state_read(&req.account_id, &region, |state| {
            state
                .layers
                .get(&layer_name)
                .and_then(|l| l.versions.iter().find(|v| v.version == version))
                .map(|v| {
                    ok(json!({
                        "LayerVersionArn": v.layer_version_arn,
                        "Version": v.version,
                        "Description": v.description,
                        "CreatedDate": v.created_date.format("%Y-%m-%dT%H:%M:%S.%3fZ").to_string(),
                        "CompatibleRuntimes": v.compatible_runtimes,
                        "CompatibleArchitectures": v.compatible_architectures,
                        "LicenseInfo": v.license_info,
                        "Content": {
                            "Location": location,
                            "CodeSha256": v.code_sha256,
                            "CodeSize": v.code_size,
                        },
                    }))
                })
                .unwrap_or_else(|| Err(not_found("LayerVersion", &layer_name)))
        })
    }

    pub(super) fn get_layer_version_by_arn(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let arn = req
            .query_params
            .get("Arn")
            .or_else(|| req.query_params.get("find"))
            .cloned()
            .unwrap_or_default();
        let (account_id, layer_name, version) =
            parse_layer_version_arn(&arn).ok_or_else(|| missing("Arn"))?;
        let region = self.region_for(&account_id);
        let location = layer_content_url(req, &account_id, &layer_name, version);
        self.with_state_read(&account_id, &region, |state| {
            state
                .layers
                .get(&layer_name)
                .and_then(|l| l.versions.iter().find(|v| v.version == version))
                .map(|v| {
                    ok(json!({
                        "LayerVersionArn": v.layer_version_arn,
                        "Version": v.version,
                        "Description": v.description,
                        "CreatedDate": v.created_date.format("%Y-%m-%dT%H:%M:%S.%3fZ").to_string(),
                        "CompatibleRuntimes": v.compatible_runtimes,
                        "CompatibleArchitectures": v.compatible_architectures,
                        "LicenseInfo": v.license_info,
                        "Content": {
                            "Location": location,
                            "CodeSha256": v.code_sha256,
                            "CodeSize": v.code_size,
                        },
                    }))
                })
                .unwrap_or_else(|| Err(not_found("LayerVersion", &arn)))
        })
    }

    pub(super) fn delete_layer_version(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let layer_name = req.path_segments.get(2).cloned().unwrap_or_default();
        if layer_name.is_empty() {
            return Err(missing("LayerName"));
        }
        let limit = if layer_name.starts_with("arn:") {
            200
        } else {
            140
        };
        if layer_name.chars().count() > limit {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterValueException",
                "LayerName exceeds the 140-character maximum",
            ));
        }
        let version_raw = req.path_segments.get(4).map(|s| s.as_str()).unwrap_or("");
        if version_raw.is_empty() {
            return Err(missing("VersionNumber"));
        }
        let version: i64 = version_raw.parse().map_err(|_| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterValueException",
                "VersionNumber must be an integer",
            )
        })?;
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        if let Some(layer) = state.layers.get_mut(&layer_name) {
            layer.versions.retain(|v| v.version != version);
        }
        empty()
    }

    pub(super) fn get_layer_version_policy(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let layer_name = req.path_segments.get(2).cloned().unwrap_or_default();
        let version: i64 = req
            .path_segments
            .get(4)
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);
        let region = self.region_for(&req.account_id);
        self.with_state_read(&req.account_id, &region, |state| {
            let policy = state
                .layers
                .get(&layer_name)
                .and_then(|l| l.versions.iter().find(|v| v.version == version))
                .and_then(|v| v.policy.clone())
                .unwrap_or_else(|| "{}".to_string());
            ok(json!({"Policy": policy, "RevisionId": id_from_time("rev-")}))
        })
    }

    pub(super) fn add_layer_version_permission(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let layer_name = req.path_segments.get(2).cloned().unwrap_or_default();
        let version: i64 = req
            .path_segments
            .get(4)
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);
        let body = body(req);
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        if let Some(layer) = state.layers.get_mut(&layer_name) {
            if let Some(v) = layer.versions.iter_mut().find(|v| v.version == version) {
                let policy = v.policy.clone().unwrap_or_else(|| "{}".to_string());
                let mut policy_doc: Value = serde_json::from_str(&policy).unwrap_or(json!({}));
                let statements = policy_doc["Statement"].as_array_mut();
                let new_stmt = json!({
                    "Sid": body["StatementId"].as_str().unwrap_or("default"),
                    "Effect": "Allow",
                    "Principal": body["Principal"].clone(),
                    "Action": body["Action"].clone(),
                    "Resource": v.layer_version_arn.clone(),
                });
                if let Some(s) = statements {
                    s.push(new_stmt);
                } else {
                    policy_doc = json!({"Version": "2012-10-17", "Statement": [new_stmt]});
                }
                v.policy = Some(policy_doc.to_string());
            }
        }
        ok(json!({
            "Statement": body["StatementId"],
            "RevisionId": id_from_time("rev-"),
        }))
    }

    pub(super) fn remove_layer_version_permission(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let layer_name = req.path_segments.get(2).cloned().unwrap_or_default();
        let version: i64 = req
            .path_segments
            .get(4)
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);
        let sid = req.path_segments.get(6).cloned().unwrap_or_default();
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        if let Some(layer) = state.layers.get_mut(&layer_name) {
            if let Some(v) = layer.versions.iter_mut().find(|v| v.version == version) {
                if let Some(policy) = v.policy.clone() {
                    let mut policy_doc: Value = serde_json::from_str(&policy).unwrap_or(json!({}));
                    if let Some(stmts) = policy_doc["Statement"].as_array_mut() {
                        stmts.retain(|s| s["Sid"].as_str() != Some(&sid));
                    }
                    v.policy = Some(policy_doc.to_string());
                }
            }
        }
        empty()
    }
}
