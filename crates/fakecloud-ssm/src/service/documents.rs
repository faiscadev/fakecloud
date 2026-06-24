use std::collections::BTreeMap;

use chrono::Utc;
use http::StatusCode;
use serde_json::{json, Value};

use fakecloud_core::pagination::paginate_checked;
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};
use fakecloud_core::validation::*;

use crate::state::{SsmDocument, SsmDocumentVersion, SsmResourcePolicy, SsmState};
use sha2::{Digest, Sha256};

use super::{missing, missing_with_code, SsmService};

fn review_status_for_action(action: &str) -> &'static str {
    match action {
        "Approve" => "APPROVED",
        "Reject" => "REJECTED",
        "SendForReview" => "PENDING",
        _ => "NOT_REVIEWED",
    }
}

/// Resolve which `SsmDocumentVersion` a `GetDocument`-style request is
/// asking for. AWS lets the caller pin a version with `DocumentVersion`,
/// `VersionName`, both, or neither (in which case we return the document's
/// default version). `$LATEST` is the only special string the caller is
/// allowed to pass for `DocumentVersion`.
fn select_document_version<'a>(
    doc: &'a SsmDocument,
    version: Option<&str>,
    version_name: Option<&str>,
) -> Option<&'a SsmDocumentVersion> {
    if let Some(vn) = version_name {
        let mut candidates = doc
            .versions
            .iter()
            .filter(|v| v.version_name.as_deref() == Some(vn));

        return match version {
            Some(doc_ver) => candidates.find(|v| v.document_version == doc_ver),
            None => candidates.next(),
        };
    }

    if let Some(doc_ver) = version {
        let target = if doc_ver == "$LATEST" {
            doc.latest_version.as_str()
        } else {
            doc_ver
        };
        return doc.versions.iter().find(|v| v.document_version == target);
    }

    doc.versions
        .iter()
        .find(|v| v.document_version == doc.default_version)
}

/// `UpdateDocument` rejects a `DocumentVersion` that doesn't refer to
/// either `$LATEST` or an existing version on the document.
fn validate_update_document_target_version(
    doc: &SsmDocument,
    target_version: Option<&str>,
) -> Result<(), AwsServiceError> {
    let Some(ver) = target_version else {
        return Ok(());
    };
    if ver == "$LATEST" || doc.versions.iter().any(|v| v.document_version == ver) {
        return Ok(());
    }
    Err(AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        "InvalidDocument",
        "The document version is not valid or does not exist.",
    ))
}

/// `UpdateDocument` rejects content whose SHA-256 already matches some
/// other version on the document. Mirrors AWS's
/// `DuplicateDocumentContent` behavior.
fn validate_update_document_unique_content(
    doc: &SsmDocument,
    content: &str,
) -> Result<(), AwsServiceError> {
    let new_hash = format!("{:x}", Sha256::digest(content.as_bytes()));
    for v in &doc.versions {
        let existing_hash = format!("{:x}", Sha256::digest(v.content.as_bytes()));
        if new_hash == existing_hash {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "DuplicateDocumentContent",
                "The content of the association document matches another \
                 document. Change the content of the document and try again.",
            ));
        }
    }
    Ok(())
}

/// `UpdateDocument` rejects a `VersionName` that another version on the
/// same document already uses.
fn validate_update_document_unique_version_name(
    doc: &SsmDocument,
    version_name: Option<&str>,
) -> Result<(), AwsServiceError> {
    let Some(vn) = version_name else {
        return Ok(());
    };
    if doc
        .versions
        .iter()
        .any(|v| v.version_name.as_deref() == Some(vn))
    {
        return Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "DuplicateDocumentVersionName",
            "The specified version name is a duplicate.",
        ));
    }
    Ok(())
}

/// Apply the `ListDocuments` filter list to a single document. Supports
/// `Owner` (with the `Self` sentinel meaning current account),
/// `TargetType`, and `Name` filters; unknown filter keys are silently
/// passed (mirrors the prior inline behavior).
fn document_matches_list_filters(
    doc: &SsmDocument,
    filters: Option<&Vec<Value>>,
    account_id: &str,
) -> bool {
    let Some(filters) = filters else {
        return true;
    };
    for filter in filters {
        let key = filter["Key"].as_str().unwrap_or("");
        let values: Vec<&str> = filter["Values"]
            .as_array()
            .map(|a| a.iter().filter_map(|v| v.as_str()).collect())
            .unwrap_or_default();
        match key {
            "Owner" if values.contains(&"Self") && doc.owner != account_id => {
                return false;
            }
            "TargetType" => match &doc.target_type {
                Some(tt) if values.contains(&tt.as_str()) => {}
                _ => return false,
            },
            "Name" if !values.contains(&doc.name.as_str()) => {
                return false;
            }
            // Unknown filter keys: AWS silently ignores them.
            _ => {}
        }
    }
    true
}

/// Build a `ListDocuments` `DocumentIdentifier` JSON entry for one document.
fn document_identifier_json(doc: &SsmDocument) -> Value {
    let mut v = json!({
        "Name": doc.name,
        "DocumentType": doc.document_type,
        "DocumentFormat": doc.document_format,
        "DocumentVersion": doc.default_version,
        "Owner": doc.owner,
        "SchemaVersion": "2.2",
        "PlatformTypes": ["Linux", "MacOS", "Windows"],
    });
    if let Some(tt) = &doc.target_type {
        v["TargetType"] = json!(tt);
    }
    v
}

impl SsmService {
    pub(super) fn create_document(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let name = body["Name"]
            .as_str()
            .ok_or_else(|| missing("Name"))?
            .to_string();
        let content = body["Content"]
            .as_str()
            .ok_or_else(|| missing("Content"))?
            .to_string();
        let doc_type = body["DocumentType"]
            .as_str()
            .unwrap_or("Command")
            .to_string();
        let doc_format = body["DocumentFormat"]
            .as_str()
            .unwrap_or("JSON")
            .to_string();
        let target_type = body["TargetType"].as_str().map(|s| s.to_string());
        let version_name = body["VersionName"].as_str().map(|s| s.to_string());

        let tags: BTreeMap<String, String> = body["Tags"]
            .as_array()
            .map(|arr| {
                arr.iter()
                    .filter_map(|t| {
                        let k = t["Key"].as_str()?;
                        let v = t["Value"].as_str()?;
                        Some((k.to_string(), v.to_string()))
                    })
                    .collect()
            })
            .unwrap_or_default();

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);

        if state.documents.contains_key(&name) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "DocumentAlreadyExists",
                "The specified document already exists.".to_string(),
            ));
        }

        // Validate content matches declared format
        if doc_format == "JSON" && serde_json::from_str::<Value>(&content).is_err() {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidDocumentContent",
                "The content for the document is not valid.",
            ));
        }

        let now = Utc::now();
        let content_hash = format!("{:x}", Sha256::digest(content.as_bytes()));

        let version = SsmDocumentVersion {
            content: content.clone(),
            document_version: "1".to_string(),
            version_name: version_name.clone(),
            created_date: now,
            status: "Active".to_string(),
            document_format: doc_format.clone(),
            is_default_version: true,
        };

        let doc = SsmDocument {
            name: name.clone(),
            content: content.clone(),
            document_type: doc_type.clone(),
            document_format: doc_format.clone(),
            target_type: target_type.clone(),
            version_name,
            tags,
            versions: vec![version],
            default_version: "1".to_string(),
            latest_version: "1".to_string(),
            created_date: now,
            owner: state.account_id.clone(),
            status: "Active".to_string(),
            permissions: BTreeMap::new(),
            reviews: Vec::new(),
        };

        state.documents.insert(name.clone(), doc);

        let (doc_description, schema_ver, doc_params) =
            extract_document_metadata(&content, &doc_format);

        let mut desc_json = json!({
            "Name": name,
            "DocumentType": doc_type,
            "DocumentFormat": doc_format,
            "DocumentVersion": "1",
            "LatestVersion": "1",
            "DefaultVersion": "1",
            "Status": "Active",
            "CreatedDate": now.timestamp_millis() as f64 / 1000.0,
            "Owner": state.account_id,
            "SchemaVersion": schema_ver.as_deref().unwrap_or("2.2"),
            "PlatformTypes": ["Linux", "MacOS", "Windows"],
            "Hash": content_hash,
            "HashType": "Sha256",
        });
        if let Some(tt) = &target_type {
            desc_json["TargetType"] = json!(tt);
        }
        if let Some(vn) = state
            .documents
            .get(&name)
            .and_then(|d| d.version_name.as_ref())
        {
            desc_json["VersionName"] = json!(vn);
        }
        if let Some(d) = doc_description {
            desc_json["Description"] = json!(d);
        }
        if !doc_params.is_empty() {
            desc_json["Parameters"] = json!(doc_params);
        }
        // Include tags if present
        if let Some(doc) = state.documents.get(&name) {
            if !doc.tags.is_empty() {
                let tags_list: Vec<Value> = doc
                    .tags
                    .iter()
                    .map(|(k, v)| json!({"Key": k, "Value": v}))
                    .collect();
                desc_json["Tags"] = json!(tags_list);
            }
        }

        Ok(AwsResponse::ok_json(
            json!({ "DocumentDescription": desc_json }),
        ))
    }

    pub(super) fn get_document(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let name = body["Name"].as_str().ok_or_else(|| missing("Name"))?;
        let version = body["DocumentVersion"].as_str();
        let version_name = body["VersionName"].as_str();
        let requested_format = body["DocumentFormat"].as_str();

        let accounts = self.state.read();
        let empty = SsmState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
        let doc = state
            .documents
            .get(name)
            .ok_or_else(|| doc_not_found(name))?;

        let ver = select_document_version(doc, version, version_name)
            .ok_or_else(|| doc_not_found(name))?;

        // If a stored YAML version is fetched without an explicit format we
        // still hand it back as JSON, matching the AWS default.
        let target_format = requested_format.unwrap_or("JSON");
        let content = convert_document_content(&ver.content, &ver.document_format, target_format);

        let mut resp = json!({
            "Name": doc.name,
            "Content": content,
            "DocumentType": doc.document_type,
            "DocumentFormat": target_format,
            "DocumentVersion": ver.document_version,
            "Status": ver.status,
            "CreatedDate": ver.created_date.timestamp_millis() as f64 / 1000.0,
        });

        if let Some(ref vn) = ver.version_name {
            resp["VersionName"] = json!(vn);
        }

        Ok(AwsResponse::ok_json(resp))
    }

    pub(super) fn delete_document(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let name = body["Name"].as_str().ok_or_else(|| missing("Name"))?;
        let doc_version = body["DocumentVersion"].as_str();
        let version_name = body["VersionName"].as_str();

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);

        if doc_version.is_some() || version_name.is_some() {
            // Deleting a specific version
            let doc = state
                .documents
                .get_mut(name)
                .ok_or_else(|| doc_not_found(name))?;

            // Find the target version
            let target_ver = if let Some(vn) = version_name {
                doc.versions
                    .iter()
                    .find(|v| v.version_name.as_deref() == Some(vn))
                    .map(|v| v.document_version.clone())
            } else {
                doc_version.map(|v| v.to_string())
            };

            if let Some(ver) = target_ver {
                if ver == doc.default_version {
                    return Err(AwsServiceError::aws_error(
                        StatusCode::BAD_REQUEST,
                        "InvalidDocumentOperation",
                        "Default version of the document can't be deleted.",
                    ));
                }
                doc.versions.retain(|v| v.document_version != ver);
                // Update latest_version if we deleted the latest
                if doc.latest_version == ver {
                    doc.latest_version = doc
                        .versions
                        .iter()
                        .filter_map(|v| v.document_version.parse::<u64>().ok())
                        .max()
                        .map(|n| n.to_string())
                        .unwrap_or_else(|| "1".to_string());
                }
            } else {
                return Err(doc_not_found(name));
            }
        } else {
            // Delete the entire document
            if state.documents.remove(name).is_none() {
                return Err(doc_not_found(name));
            }
        }

        Ok(AwsResponse::ok_json(json!({})))
    }

    pub(super) fn update_document(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let name = body["Name"].as_str().ok_or_else(|| missing("Name"))?;
        let content = body["Content"]
            .as_str()
            .ok_or_else(|| missing("Content"))?
            .to_string();
        let version_name = body["VersionName"].as_str().map(|s| s.to_string());
        let doc_format = body["DocumentFormat"].as_str();
        let target_version = body["DocumentVersion"].as_str();

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let doc = state
            .documents
            .get_mut(name)
            .ok_or_else(|| doc_not_found(name))?;

        validate_update_document_target_version(doc, target_version)?;
        validate_update_document_unique_content(doc, &content)?;
        validate_update_document_unique_version_name(doc, version_name.as_deref())?;

        let new_version_num = (doc.versions.len() + 1).to_string();
        let format = doc_format.unwrap_or(&doc.document_format).to_string();

        let (doc_description, schema_ver, doc_params) =
            extract_document_metadata(&content, &format);

        let version = SsmDocumentVersion {
            content: content.clone(),
            document_version: new_version_num.clone(),
            version_name,
            created_date: Utc::now(),
            status: "Active".to_string(),
            document_format: format.clone(),
            is_default_version: false,
        };

        doc.versions.push(version);
        doc.latest_version = new_version_num.clone();
        doc.content = content;

        let mut desc_json = json!({
            "Name": doc.name,
            "DocumentType": doc.document_type,
            "DocumentFormat": format,
            "DocumentVersion": new_version_num,
            "LatestVersion": doc.latest_version,
            "DefaultVersion": doc.default_version,
            "Status": "Active",
            "CreatedDate": doc.created_date.timestamp_millis() as f64 / 1000.0,
            "Owner": doc.owner,
            "SchemaVersion": schema_ver.as_deref().unwrap_or("2.2"),
        });
        if let Some(d) = doc_description {
            desc_json["Description"] = json!(d);
        }
        if !doc_params.is_empty() {
            desc_json["Parameters"] = json!(doc_params);
        }
        // Include VersionName from the newly created version
        if let Some(ver) = doc.versions.last() {
            if let Some(ref vn) = ver.version_name {
                desc_json["VersionName"] = json!(vn);
            }
        }

        Ok(AwsResponse::ok_json(
            json!({ "DocumentDescription": desc_json }),
        ))
    }

    pub(super) fn describe_document(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let name = body["Name"].as_str().ok_or_else(|| missing("Name"))?;

        let accounts = self.state.read();
        let empty = SsmState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
        let doc = state
            .documents
            .get(name)
            .ok_or_else(|| doc_not_found(name))?;

        // Use the default version's content for the hash and metadata
        let default_ver = doc
            .versions
            .iter()
            .find(|v| v.document_version == doc.default_version);
        let content_for_hash = default_ver
            .map(|v| v.content.as_str())
            .unwrap_or(&doc.content);
        let format_for_hash = default_ver
            .map(|v| v.document_format.as_str())
            .unwrap_or(&doc.document_format);

        let (doc_description, schema_ver, doc_params) =
            extract_document_metadata(content_for_hash, format_for_hash);

        let mut desc_json = json!({
            "Name": doc.name,
            "DocumentType": doc.document_type,
            "DocumentFormat": format_for_hash,
            "DocumentVersion": doc.default_version,
            "LatestVersion": doc.latest_version,
            "DefaultVersion": doc.default_version,
            "Status": doc.status,
            "CreatedDate": doc.created_date.timestamp_millis() as f64 / 1000.0,
            "Owner": doc.owner,
            "SchemaVersion": schema_ver.as_deref().unwrap_or("2.2"),
            "PlatformTypes": ["Linux", "MacOS", "Windows"],
            "Hash": format!("{:x}", Sha256::digest(content_for_hash.as_bytes())),
            "HashType": "Sha256",
        });
        if let Some(tt) = &doc.target_type {
            desc_json["TargetType"] = json!(tt);
        }
        if let Some(d) = doc_description {
            desc_json["Description"] = json!(d);
        }
        if !doc_params.is_empty() {
            desc_json["Parameters"] = json!(doc_params);
        }
        if let Some(ref vn) = doc.version_name {
            desc_json["VersionName"] = json!(vn);
        }
        // Find default version name
        if let Some(ver) = doc
            .versions
            .iter()
            .find(|v| v.document_version == doc.default_version)
        {
            if let Some(ref vn) = ver.version_name {
                desc_json["DefaultVersionName"] = json!(vn);
            }
        }

        Ok(AwsResponse::ok_json(json!({ "Document": desc_json })))
    }

    pub(super) fn update_document_default_version(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let name = body["Name"].as_str().ok_or_else(|| missing("Name"))?;
        let version = body["DocumentVersion"]
            .as_str()
            .ok_or_else(|| missing("DocumentVersion"))?;

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let doc = state
            .documents
            .get_mut(name)
            .ok_or_else(|| doc_not_found(name))?;

        // Validate version exists
        if !doc.versions.iter().any(|v| v.document_version == version) {
            return Err(doc_not_found(name));
        }

        doc.default_version = version.to_string();

        // Update is_default_version flags
        for v in &mut doc.versions {
            v.is_default_version = v.document_version == version;
        }

        // Find version name for the new default version
        let version_name = doc
            .versions
            .iter()
            .find(|v| v.document_version == version)
            .and_then(|v| v.version_name.clone());

        let mut desc = json!({
            "Name": name,
            "DefaultVersion": version,
        });
        if let Some(vn) = version_name {
            desc["DefaultVersionName"] = json!(vn);
        }

        Ok(AwsResponse::ok_json(json!({ "Description": desc })))
    }

    pub(super) fn list_documents(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        validate_optional_range_i64("MaxResults", body["MaxResults"].as_i64(), 1, 50)?;
        let max_results = body["MaxResults"].as_i64().unwrap_or(10) as usize;
        let filters = body["Filters"].as_array();

        let accounts = self.state.read();
        let empty = SsmState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
        let all_docs: Vec<Value> = state
            .documents
            .values()
            .filter(|doc| document_matches_list_filters(doc, filters, &state.account_id))
            .map(document_identifier_json)
            .collect();

        let (result, next_token) =
            paginate_checked(&all_docs, body["NextToken"].as_str(), max_results)
                .map_err(|_| super::invalid_next_token())?;
        let mut resp = json!({ "DocumentIdentifiers": result });
        // Omit NextToken on the last page rather than emitting an empty string:
        // an empty token echoed back by a client would now be rejected by
        // paginate_checked as a malformed offset (and AWS omits it too).
        if let Some(token) = next_token {
            resp["NextToken"] = json!(token);
        }

        Ok(AwsResponse::ok_json(resp))
    }

    pub(super) fn describe_document_permission(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let name = body["Name"].as_str().ok_or_else(|| missing("Name"))?;

        let accounts = self.state.read();
        let empty = SsmState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
        let doc = state.documents.get(name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidDocument",
                "The specified document does not exist.".to_string(),
            )
        })?;

        let account_ids = doc.permissions.get("Share").cloned().unwrap_or_default();
        let shared_version = doc
            .permissions
            .get("__shared_version")
            .and_then(|v| v.first())
            .cloned()
            .unwrap_or_else(|| "$DEFAULT".to_string());

        Ok(AwsResponse::ok_json(json!({
            "AccountIds": account_ids,
            "AccountSharingInfoList": account_ids.iter().map(|id| json!({
                "AccountId": id,
                "SharedDocumentVersion": shared_version
            })).collect::<Vec<_>>(),
        })))
    }

    pub(super) fn modify_document_permission(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        // ModifyDocumentPermission declares InvalidDocument, not the
        // generic ValidationException, so missing-input + bad-version
        // surface as InvalidDocument.
        let name = body["Name"]
            .as_str()
            .ok_or_else(|| missing_with_code("Name", "InvalidDocument"))?;
        let permission_type = body["PermissionType"].as_str().unwrap_or("Share");
        let accounts_to_add = body["AccountIdsToAdd"].as_array();
        let accounts_to_remove = body["AccountIdsToRemove"].as_array();

        let shared_doc_version = body["SharedDocumentVersion"].as_str();

        if permission_type != "Share" {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidPermissionType",
                format!(
                    "1 validation error detected: Value '{permission_type}' at 'permissionType' \
                     failed to satisfy constraint: Member must satisfy enum value set: [Share]"
                ),
            ));
        }

        // Validate SharedDocumentVersion if provided
        if let Some(ver) = shared_doc_version {
            if ver != "$DEFAULT" && ver != "$LATEST" && ver.parse::<i64>().is_err() {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidDocument",
                    format!(
                        "1 validation error detected: Value '{ver}' at 'sharedDocumentVersion' \
                         failed to satisfy constraint: \
                         Member must satisfy regular expression pattern: \
                         ([$]LATEST|[$]DEFAULT|[$]ALL)"
                    ),
                ));
            }
        }

        // Validate account IDs
        fn validate_account_ids(ids: &[Value]) -> Result<Vec<String>, AwsServiceError> {
            let mut result = Vec::new();
            let mut has_all = false;
            for id_val in ids {
                if let Some(id) = id_val.as_str() {
                    if id.eq_ignore_ascii_case("all") {
                        has_all = true;
                        result.push(id.to_string());
                    } else if id.len() != 12 || !id.chars().all(|c| c.is_ascii_digit()) {
                        return Err(AwsServiceError::aws_error(
                            StatusCode::BAD_REQUEST,
                            "InvalidPermissionType",
                            format!(
                                "1 validation error detected: Value '[{id}]' at \
                                 'accountIdsToAdd' failed to satisfy constraint: \
                                 Member must satisfy regular expression pattern: \
                                 (?i)all|[0-9]{{12}}"
                            ),
                        ));
                    } else {
                        result.push(id.to_string());
                    }
                }
            }
            if has_all && result.len() > 1 {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "DocumentPermissionLimit",
                    "Accounts can either be all or a group of AWS accounts",
                ));
            }
            Ok(result)
        }

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let doc = state.documents.get_mut(name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidDocument",
                "The specified document does not exist.".to_string(),
            )
        })?;

        if let Some(add_ids) = accounts_to_add {
            let ids = validate_account_ids(add_ids)?;
            let entry = doc.permissions.entry("Share".to_string()).or_default();
            for id in ids {
                if !entry.contains(&id) {
                    entry.push(id);
                }
            }
        }

        if let Some(remove_ids) = accounts_to_remove {
            let ids = validate_account_ids(remove_ids)?;
            if let Some(entry) = doc.permissions.get_mut("Share") {
                entry.retain(|id| !ids.contains(id));
            }
        }

        // Persist the shared version so DescribeDocumentPermission reflects it.
        // Stored under a reserved key separate from the "Share" account list.
        if let Some(ver) = shared_doc_version {
            doc.permissions
                .insert("__shared_version".to_string(), vec![ver.to_string()]);
        }

        Ok(AwsResponse::ok_json(json!({})))
    }

    // ===== Command operations =====

    pub(super) fn list_document_versions(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let name = body["Name"].as_str().ok_or_else(|| missing("Name"))?;
        let max_results = body["MaxResults"].as_i64().unwrap_or(50) as usize;

        let accounts = self.state.read();
        let empty = SsmState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
        let doc = state
            .documents
            .get(name)
            .ok_or_else(|| doc_not_found(name))?;

        let all: Vec<Value> = doc
            .versions
            .iter()
            .map(|v| {
                json!({
                    "Name": name,
                    "DocumentVersion": v.document_version,
                    "CreatedDate": v.created_date.timestamp_millis() as f64 / 1000.0,
                    "IsDefaultVersion": v.is_default_version,
                    "DocumentFormat": v.document_format,
                    "Status": v.status,
                    "VersionName": v.version_name,
                })
            })
            .collect();

        let (items, next_token) = paginate_checked(&all, body["NextToken"].as_str(), max_results)
            .map_err(|_| super::invalid_next_token())?;
        let mut resp = json!({ "DocumentVersions": items });
        if let Some(token) = next_token {
            resp["NextToken"] = json!(token);
        }
        Ok(AwsResponse::ok_json(resp))
    }

    pub(super) fn list_document_metadata_history(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let name = body["Name"].as_str().ok_or_else(|| missing("Name"))?;
        validate_required("Metadata", &body["Metadata"])?;
        validate_optional_enum("Metadata", body["Metadata"].as_str(), &["DocumentReviews"])?;
        let _metadata = body["Metadata"]
            .as_str()
            .ok_or_else(|| missing("Metadata"))?;
        validate_optional_range_i64("MaxResults", body["MaxResults"].as_i64(), 1, 50)?;
        let max_results = body["MaxResults"].as_i64().unwrap_or(50) as usize;

        let accounts = self.state.read();
        let empty = SsmState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
        let doc = state
            .documents
            .get(name)
            .ok_or_else(|| doc_not_found(name))?;

        let all_responses: Vec<Value> = doc
            .reviews
            .iter()
            .map(|r| {
                json!({
                    "Reviewer": r.reviewer,
                    "ReviewStatus": review_status_for_action(&r.action),
                    "Comment": r.comment.iter().map(|c| json!({
                        "Type": c.comment_type,
                        "Content": c.content,
                    })).collect::<Vec<_>>(),
                    "CreateTime": r.created_time.timestamp_millis() as f64 / 1000.0,
                    "UpdatedTime": r.updated_time.timestamp_millis() as f64 / 1000.0,
                })
            })
            .collect();

        let (page, next_token) =
            paginate_checked(&all_responses, body["NextToken"].as_str(), max_results)
                .map_err(|_| super::invalid_next_token())?;

        let mut resp = json!({
            "Name": name,
            "DocumentVersion": doc.default_version,
            "Author": doc.owner,
            "Metadata": {
                "ReviewerResponse": page,
            },
        });
        if let Some(token) = next_token {
            resp["NextToken"] = json!(token);
        }

        Ok(AwsResponse::ok_json(resp))
    }

    pub(super) fn update_document_metadata(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let name = body["Name"]
            .as_str()
            .ok_or_else(|| missing("Name"))?
            .to_string();
        validate_required("DocumentReviews", &body["DocumentReviews"])?;
        let action = body["DocumentReviews"]["Action"]
            .as_str()
            .ok_or_else(|| missing("DocumentReviews.Action"))?;
        validate_optional_enum(
            "DocumentReviews.Action",
            Some(action),
            &["SendForReview", "UpdateReview", "Approve", "Reject"],
        )?;

        let comments: Vec<crate::state::DocumentReviewComment> = body["DocumentReviews"]["Comment"]
            .as_array()
            .map(|arr| {
                arr.iter()
                    .filter_map(|c| {
                        let content = c["Content"].as_str()?;
                        Some(crate::state::DocumentReviewComment {
                            comment_type: c["Type"].as_str().unwrap_or("Comment").to_string(),
                            content: content.to_string(),
                        })
                    })
                    .collect()
            })
            .unwrap_or_default();

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let doc = state
            .documents
            .get_mut(&name)
            .ok_or_else(|| doc_not_found(&name))?;

        let now = chrono::Utc::now();
        doc.reviews.push(crate::state::DocumentReview {
            reviewer: state.account_id.clone(),
            action: action.to_string(),
            comment: comments,
            created_time: now,
            updated_time: now,
        });

        Ok(AwsResponse::ok_json(json!({})))
    }

    // Resource policies

    pub(super) fn put_resource_policy(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        validate_optional_string_length("ResourceArn", body["ResourceArn"].as_str(), 20, 2048)?;
        let resource_arn = body["ResourceArn"]
            .as_str()
            .ok_or_else(|| missing("ResourceArn"))?
            .to_string();
        let policy = body["Policy"]
            .as_str()
            .ok_or_else(|| missing("Policy"))?
            .to_string();

        let policy_id = body["PolicyId"].as_str().map(|s| s.to_string());
        let policy_hash = body["PolicyHash"].as_str().map(|s| s.to_string());

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);

        // If PolicyId is provided, update existing
        if let Some(ref pid) = policy_id {
            if let Some(existing) = state
                .resource_policies
                .iter_mut()
                .find(|p| p.policy_id == *pid && p.resource_arn == resource_arn)
            {
                // Verify hash matches if provided
                if let Some(ref expected_hash) = policy_hash {
                    if existing.policy_hash != *expected_hash {
                        return Err(AwsServiceError::aws_error(
                            StatusCode::BAD_REQUEST,
                            "ResourcePolicyConflictException",
                            "The policy hash does not match.".to_string(),
                        ));
                    }
                }
                existing.policy = policy;
                let new_hash = format!("{:x}", md5::compute(existing.policy.as_bytes()));
                existing.policy_hash = new_hash.clone();
                return Ok(AwsResponse::ok_json(json!({
                    "PolicyId": pid,
                    "PolicyHash": new_hash,
                })));
            }
        }

        // Create new
        let new_id = uuid::Uuid::new_v4().to_string();
        let new_hash = format!("{:x}", md5::compute(policy.as_bytes()));
        state.resource_policies.push(SsmResourcePolicy {
            policy_id: new_id.clone(),
            policy_hash: new_hash.clone(),
            policy,
            resource_arn,
        });

        Ok(AwsResponse::ok_json(json!({
            "PolicyId": new_id,
            "PolicyHash": new_hash,
        })))
    }

    pub(super) fn get_resource_policies(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        validate_optional_string_length("ResourceArn", body["ResourceArn"].as_str(), 20, 2048)?;
        validate_optional_range_i64("MaxResults", body["MaxResults"].as_i64(), 1, 50)?;
        let resource_arn = body["ResourceArn"]
            .as_str()
            .ok_or_else(|| missing("ResourceArn"))?;

        let accounts = self.state.read();
        let empty = SsmState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
        let policies: Vec<Value> = state
            .resource_policies
            .iter()
            .filter(|p| p.resource_arn == resource_arn)
            .map(|p| {
                json!({
                    "PolicyId": p.policy_id,
                    "PolicyHash": p.policy_hash,
                    "Policy": p.policy,
                })
            })
            .collect();

        Ok(AwsResponse::ok_json(json!({ "Policies": policies })))
    }

    pub(super) fn delete_resource_policy(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let resource_arn = body["ResourceArn"]
            .as_str()
            .ok_or_else(|| missing("ResourceArn"))?;
        let policy_id = body["PolicyId"]
            .as_str()
            .ok_or_else(|| missing("PolicyId"))?;
        let policy_hash = body["PolicyHash"]
            .as_str()
            .ok_or_else(|| missing("PolicyHash"))?;

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let idx = state
            .resource_policies
            .iter()
            .position(|p| p.resource_arn == resource_arn && p.policy_id == policy_id);

        match idx {
            Some(i) => {
                if state.resource_policies[i].policy_hash != policy_hash {
                    return Err(AwsServiceError::aws_error(
                        StatusCode::BAD_REQUEST,
                        "ResourcePolicyConflictException",
                        "The policy hash does not match.".to_string(),
                    ));
                }
                state.resource_policies.remove(i);
                Ok(AwsResponse::ok_json(json!({})))
            }
            None => Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourcePolicyNotFoundException",
                "The resource policy was not found.".to_string(),
            )),
        }
    }
}

/// Convert document content between JSON and YAML formats.
/// Falls back to returning content as-is if conversion isn't possible.
pub(super) fn convert_document_content(
    content: &str,
    from_format: &str,
    to_format: &str,
) -> String {
    if from_format == to_format {
        return content.to_string();
    }

    // Parse the content from its source format
    let parsed: Option<serde_json::Value> = match from_format {
        "YAML" => serde_yaml::from_str(content).ok(),
        _ => serde_json::from_str(content).ok(),
    };

    if let Some(val) = parsed {
        match to_format {
            "JSON" => serde_json::to_string(&val).unwrap_or_else(|_| content.to_string()),
            "YAML" => serde_yaml::to_string(&val).unwrap_or_else(|_| content.to_string()),
            _ => content.to_string(),
        }
    } else {
        content.to_string()
    }
}

pub(super) fn doc_not_found(_name: &str) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        "InvalidDocument",
        "The specified document does not exist.".to_string(),
    )
}

/// Parse a document content string (JSON or YAML) and extract metadata fields.
/// Returns (description, schema_version, parameters_json).
pub(super) fn extract_document_metadata(
    content: &str,
    format: &str,
) -> (Option<String>, Option<String>, Vec<Value>) {
    let parsed: Option<Value> = match format {
        "YAML" => serde_yaml::from_str(content).ok(),
        _ => serde_json::from_str(content).ok(),
    };

    let parsed = match parsed {
        Some(v) => v,
        None => return (None, None, Vec::new()),
    };

    let description = parsed
        .get("description")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());
    let schema_version = parsed
        .get("schemaVersion")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());

    let parameters = if let Some(params_obj) = parsed.get("parameters").and_then(|v| v.as_object())
    {
        params_obj
            .iter()
            .map(|(name, def)| {
                let mut param = json!({ "Name": name });
                if let Some(t) = def.get("type").and_then(|v| v.as_str()) {
                    param["Type"] = json!(t);
                }
                if let Some(d) = def.get("description").and_then(|v| v.as_str()) {
                    param["Description"] = json!(d);
                }
                if let Some(dv) = def.get("default") {
                    let default_str = match dv {
                        Value::String(s) => s.clone(),
                        Value::Number(n) => n.to_string(),
                        Value::Bool(b) => if *b { "True" } else { "False" }.to_string(),
                        other => json_dumps(other),
                    };
                    param["DefaultValue"] = json!(default_str);
                }
                param
            })
            .collect()
    } else {
        Vec::new()
    };

    (description, schema_version, parameters)
}

/// Serialize a JSON value with Python-style separators (`, ` and `: `).
pub(super) fn json_dumps(val: &Value) -> String {
    match val {
        Value::Null => "null".to_string(),
        Value::Bool(b) => if *b { "true" } else { "false" }.to_string(),
        Value::Number(n) => n.to_string(),
        Value::String(s) => format!("\"{}\"", s.replace('\\', "\\\\").replace('"', "\\\"")),
        Value::Array(arr) => {
            let items: Vec<String> = arr.iter().map(json_dumps).collect();
            format!("[{}]", items.join(", "))
        }
        Value::Object(obj) => {
            let items: Vec<String> = obj
                .iter()
                .map(|(k, v)| {
                    format!(
                        "\"{}\": {}",
                        k.replace('\\', "\\\\").replace('"', "\\\""),
                        json_dumps(v)
                    )
                })
                .collect();
            format!("{{{}}}", items.join(", "))
        }
    }
}
