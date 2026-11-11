//! `LambdaService` `layers` family — extracted from service.rs by audit-2026-05-19.

use super::*;
use crate::layer_access::{find_layer_version, find_layer_version_mut, policy_revision_id};

impl LambdaService {
    // ── Layers ──

    pub(super) fn publish_layer_version(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let layer_name = layer_name_label(req)?;
        let layer_name = layer_name.as_str();
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
        // A layer ARN publishes into the layer it names; only its owner may.
        let target = layer_target(layer_name, &req.account_id, &req.region)?;
        refuse_foreign(
            self,
            req,
            &target,
            "PublishLayerVersion",
            &target.layer_arn(),
        )?;
        let layer_name = target.layer_name.as_str();
        let storage_limit = self.code_storage_limit(&target.account_id, &target.region);

        let mut accounts = self.state.write();
        let state = accounts.regional_mut(&target.account_id, &target.region);
        crate::quota::check_new_code(state, storage_limit, code_size)?;
        let account_id = state.account_id.clone();
        let layer = state
            .layers
            .entry(layer_name.to_string())
            // Layer ARN carries the request's credential-scope region
            // (`req.region`, or the region a layer ARN names), not the
            // server default (`state.region`); the version ARN below is
            // derived from it, so both stay in that region.
            .or_insert_with(|| {
                Layer::new(
                    layer_name,
                    crate::state::layer_arn(&target.region, &state.account_id, layer_name),
                )
            });
        let next_version = layer.next_version();
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
            policy_revision_id: None,
            code_zip: zip_bytes,
            code_sha256: code_sha256.clone(),
            code_size,
            compatible_architectures: architectures,
        };
        layer.versions.push(lv.clone());
        let location =
            layer_content_url(req, &account_id, &target.region, layer_name, next_version);
        created(json!({
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
        let region = req.region.clone();
        self.with_state_read(account_id, &region, |state| {
            // A layer is listed with its latest version matching the
            // runtime / architecture filters; a layer none of whose versions
            // match (or whose versions were all deleted) is not listed.
            let layers: Vec<Value> = state
                .layers
                .values()
                .filter_map(|l| {
                    let v = l
                        .versions
                        .iter()
                        .filter(|v| matches_compatibility_filters(v, req))
                        .max_by_key(|v| v.version)?;
                    Some(json!({
                        "LayerName": l.layer_name,
                        "LayerArn": l.layer_arn,
                        "LatestMatchingVersion": {
                            "LayerVersionArn": v.layer_version_arn,
                            "Version": v.version,
                            "Description": v.description,
                            "CreatedDate": v.created_date.format("%Y-%m-%dT%H:%M:%S.%3fZ").to_string(),
                            "CompatibleRuntimes": v.compatible_runtimes,
                            "CompatibleArchitectures": v.compatible_architectures,
                        },
                    }))
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
        account_id: &str,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let layer_name = layer_name_label(req)?;
        let target = layer_target(&layer_name, account_id, &req.region)?;
        // A layer version policy can only grant `lambda:GetLayerVersion`, so
        // another account's version list is never readable.
        refuse_foreign(self, req, &target, "ListLayerVersions", &target.layer_arn())?;
        let marker = req.query_params.get("Marker").map(String::as_str);
        let max_items = crate::service::marker_page_size(req);
        self.with_state_read(&target.account_id, &target.region, |state| {
            let versions: Vec<Value> = state
                .layers
                .get(&target.layer_name)
                .map(|l| {
                    l.versions
                        .iter()
                        .filter(|v| matches_compatibility_filters(v, req))
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
        let layer_name = layer_name_label(req)?;
        let target = layer_target(&layer_name, &req.account_id, &req.region)?;
        let version = layer_version_number(req)?;
        self.layer_version_response(req, &target, version)
    }

    pub(super) fn get_layer_version_by_arn(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let arn = req
            .query_params
            .get("Arn")
            .filter(|a| !a.is_empty())
            .ok_or_else(|| missing("Arn"))?;
        let (target, version) = LayerArn::parse(arn)
            .and_then(|t| t.version.map(|v| (t, v)))
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterValueException",
                    format!("Invalid layer version ARN: {arn}"),
                )
            })?;
        self.layer_version_response(req, &target, version)
    }

    /// `GetLayerVersion` / `GetLayerVersionByArn`: the version in the account
    /// and region `target` names, if the caller owns it or it is shared with
    /// them.
    fn layer_version_response(
        &self,
        req: &AwsRequest,
        target: &LayerArn,
        version: i64,
    ) -> Result<AwsResponse, AwsServiceError> {
        let caller = self.layer_caller(req);
        let location = layer_content_url(
            req,
            &target.account_id,
            &target.region,
            &target.layer_name,
            version,
        );
        let accounts = self.state.read();
        let (layer, v) = crate::layer_access::authorized_layer_version(
            &accounts,
            target,
            version,
            &caller,
            "GetLayerVersion",
        )?
        .ok_or_else(|| not_found("LayerVersion", &target.version_arn(version)))?;
        ok(json!({
            "LayerArn": layer.layer_arn,
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
    }

    pub(super) fn delete_layer_version(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let layer_name = layer_name_label(req)?;
        let target = layer_target(&layer_name, &req.account_id, &req.region)?;
        let version = layer_version_number(req)?;
        refuse_foreign(
            self,
            req,
            &target,
            "DeleteLayerVersion",
            &target.version_arn(version),
        )?;
        let mut accounts = self.state.write();
        let state = accounts.regional_mut(&target.account_id, &target.region);
        // Idempotent: deleting a version that is already gone succeeds.
        if let Some(layer) = state.layers.get_mut(&target.layer_name) {
            layer.versions.retain(|v| v.version != version);
        }
        no_content()
    }

    pub(super) fn get_layer_version_policy(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let layer_name = layer_name_label(req)?;
        let target = layer_target(&layer_name, &req.account_id, &req.region)?;
        let version = layer_version_number(req)?;
        refuse_foreign(
            self,
            req,
            &target,
            "GetLayerVersionPolicy",
            &target.version_arn(version),
        )?;
        let accounts = self.state.read();
        let (_, v) = find_layer_version(&accounts, &target, version)
            .ok_or_else(|| layer_version_not_found(&target, version))?;
        let policy = v.policy.clone().ok_or_else(no_policy)?;
        ok(json!({"Policy": policy, "RevisionId": policy_revision_id(v)}))
    }

    pub(super) fn add_layer_version_permission(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let layer_name = layer_name_label(req)?;
        let target = layer_target(&layer_name, &req.account_id, &req.region)?;
        let version = layer_version_number(req)?;
        refuse_foreign(
            self,
            req,
            &target,
            "AddLayerVersionPermission",
            &target.version_arn(version),
        )?;
        let body = body(req);
        let sid = required_str(&body, "StatementId")?;
        let action = required_str(&body, "Action")?;
        if action != "lambda:GetLayerVersion" {
            return Err(invalid_parameter(format!(
                "Action '{action}' is not supported; only lambda:GetLayerVersion can be granted on a layer version"
            )));
        }
        let principal_raw = required_str(&body, "Principal")?;
        let principal = layer_policy_principal(principal_raw, &target.partition)?;
        let org_id = match body["OrganizationId"].as_str() {
            Some(org) if !org.is_empty() => {
                // `OrganizationId` narrows a grant to every account (`*`) down
                // to one organization; it means nothing next to an account.
                if principal_raw != "*" {
                    return Err(invalid_parameter(
                        "OrganizationId can only be specified with Principal '*'".to_string(),
                    ));
                }
                Some(validate_org_id(org)?)
            }
            _ => None,
        };

        let mut accounts = self.state.write();
        let v = find_layer_version_mut(&mut accounts, &target, version)
            .ok_or_else(|| layer_version_not_found(&target, version))?;
        check_policy_revision(req, v)?;
        let mut policy_doc: Value = v
            .policy
            .as_deref()
            .and_then(|p| serde_json::from_str(p).ok())
            .unwrap_or_else(|| json!({"Version": "2012-10-17", "Statement": []}));
        if !policy_doc["Statement"].is_array() {
            policy_doc["Statement"] = json!([]);
        }
        let statements = policy_doc["Statement"]
            .as_array_mut()
            .expect("Statement was just made an array");
        if statements.iter().any(|s| s["Sid"].as_str() == Some(sid)) {
            return Err(AwsServiceError::aws_error(
                StatusCode::CONFLICT,
                "ResourceConflictException",
                format!("The statement id ({sid}) provided already exists. Please provide a new statement id, or remove the existing statement."),
            ));
        }
        let mut new_stmt = json!({
            "Sid": sid,
            "Effect": "Allow",
            "Principal": principal,
            "Action": action,
            "Resource": v.layer_version_arn.clone(),
        });
        if let Some(org) = org_id {
            new_stmt["Condition"] = json!({"StringEquals": {"aws:PrincipalOrgID": org}});
        }
        statements.push(new_stmt.clone());
        v.policy = Some(policy_doc.to_string());
        let revision = uuid::Uuid::new_v4().to_string();
        v.policy_revision_id = Some(revision.clone());
        created(json!({
            "Statement": new_stmt.to_string(),
            "RevisionId": revision,
        }))
    }

    pub(super) fn remove_layer_version_permission(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let layer_name = layer_name_label(req)?;
        let target = layer_target(&layer_name, &req.account_id, &req.region)?;
        let version = layer_version_number(req)?;
        refuse_foreign(
            self,
            req,
            &target,
            "RemoveLayerVersionPermission",
            &target.version_arn(version),
        )?;
        let sid = req.path_segments.get(6).cloned().unwrap_or_default();
        let mut accounts = self.state.write();
        let v = find_layer_version_mut(&mut accounts, &target, version)
            .ok_or_else(|| layer_version_not_found(&target, version))?;
        check_policy_revision(req, v)?;
        let statement_missing = || {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "ResourceNotFoundException",
                format!("Statement {sid} is not found in resource policy."),
            )
        };
        let mut policy_doc: Value = v
            .policy
            .as_deref()
            .and_then(|p| serde_json::from_str(p).ok())
            .ok_or_else(statement_missing)?;
        let stmts = policy_doc["Statement"]
            .as_array_mut()
            .ok_or_else(statement_missing)?;
        let before = stmts.len();
        stmts.retain(|s| s["Sid"].as_str() != Some(sid.as_str()));
        if stmts.len() == before {
            return Err(statement_missing());
        }
        // The last statement removed leaves the version with no policy.
        if stmts.is_empty() {
            v.policy = None;
            v.policy_revision_id = None;
        } else {
            v.policy = Some(policy_doc.to_string());
            v.policy_revision_id = Some(uuid::Uuid::new_v4().to_string());
        }
        no_content()
    }
}

/// The layer a `LayerName` (a bare name or a layer ARN) addresses. A bare
/// name is the caller's layer in the request region; a layer ARN names its
/// own account and region.
/// Whether a layer version satisfies the `CompatibleRuntime` /
/// `CompatibleArchitecture` filters of ListLayers / ListLayerVersions: it
/// declares the requested runtime (and architecture) among its compatible
/// ones. An absent filter matches every version.
fn matches_compatibility_filters(v: &crate::state::LayerVersion, req: &AwsRequest) -> bool {
    let wants = |param: &str, declared: &[String]| {
        req.query_params
            .get(param)
            .is_none_or(|want| declared.iter().any(|d| d == want))
    };
    wants("CompatibleRuntime", &v.compatible_runtimes)
        && wants("CompatibleArchitecture", &v.compatible_architectures)
}

fn layer_target(raw: &str, account_id: &str, region: &str) -> Result<LayerArn, AwsServiceError> {
    if raw.starts_with("arn:") {
        return match LayerArn::parse(raw) {
            Some(arn) if arn.version.is_none() => Ok(arn),
            _ => Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterValueException",
                format!("Invalid LayerName: '{raw}' is neither a layer name nor a layer ARN"),
            )),
        };
    }
    Ok(LayerArn {
        partition: fakecloud_aws::arn::partition_for(region).to_string(),
        region: region.to_string(),
        account_id: account_id.to_string(),
        layer_name: raw.to_string(),
        version: None,
    })
}

/// The `{VersionNumber}` path label of a layer version operation.
fn layer_version_number(req: &AwsRequest) -> Result<i64, AwsServiceError> {
    let raw = req.path_segments.get(4).map(String::as_str).unwrap_or("");
    if raw.is_empty() {
        return Err(missing("VersionNumber"));
    }
    raw.parse().map_err(|_| {
        AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "InvalidParameterValueException",
            "VersionNumber must be an integer",
        )
    })
}

/// Only a layer's owner may change it or read its policy: no layer version
/// policy can grant those actions, so another account is always refused, and
/// nothing happens in the caller's own account instead.
fn refuse_foreign(
    svc: &LambdaService,
    req: &AwsRequest,
    target: &LayerArn,
    action: &str,
    resource: &str,
) -> Result<(), AwsServiceError> {
    if target.account_id == req.account_id {
        return Ok(());
    }
    Err(crate::layer_access::access_denied(
        &svc.layer_caller(req),
        action,
        resource,
    ))
}

fn layer_version_not_found(target: &LayerArn, version: i64) -> AwsServiceError {
    not_found("LayerVersion", &target.version_arn(version))
}

fn invalid_parameter(message: String) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        "InvalidParameterValueException",
        message,
    )
}

/// `AddLayerVersionPermission` / `RemoveLayerVersionPermission` `RevisionId`:
/// when given, it must be the policy's current revision.
fn check_policy_revision(req: &AwsRequest, v: &LayerVersion) -> Result<(), AwsServiceError> {
    let Some(expected) = req.query_params.get("RevisionId").filter(|r| !r.is_empty()) else {
        return Ok(());
    };
    if policy_revision_id(v).as_deref() == Some(expected.as_str()) {
        return Ok(());
    }
    Err(AwsServiceError::aws_error(
        StatusCode::PRECONDITION_FAILED,
        "PreconditionFailedException",
        format!(
            "The RevisionId provided ({expected}) does not match the latest RevisionId for the layer version policy. Call GetLayerVersionPolicy to retrieve the latest RevisionId."
        ),
    ))
}

fn no_policy() -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::NOT_FOUND,
        "ResourceNotFoundException",
        "No policy is associated with the given resource.",
    )
}

fn required_str<'a>(body: &'a Value, field: &str) -> Result<&'a str, AwsServiceError> {
    body[field]
        .as_str()
        .filter(|s| !s.is_empty())
        .ok_or_else(|| missing(field))
}

fn is_account_id(s: &str) -> bool {
    s.len() == 12 && s.bytes().all(|b| b.is_ascii_digit())
}

/// The policy `Principal` for an `AddLayerVersionPermission` principal, as
/// Lambda writes it. The principal must match the model's
/// `\d{12}|\*|arn:(aws[a-zA-Z-]*):iam::\d{12}:root`: `*` stays `*`; an
/// account ID or account root ARN becomes
/// `{"AWS": "arn:<partition>:iam::<account>:root"}`, and a root ARN must be
/// in the layer's own partition (an account in another partition can never
/// use the layer).
fn layer_policy_principal(principal: &str, partition: &str) -> Result<Value, AwsServiceError> {
    if principal == "*" {
        return Ok(json!("*"));
    }
    if is_account_id(principal) {
        return Ok(json!({"AWS": format!("arn:{partition}:iam::{principal}:root")}));
    }
    let root = principal
        .strip_prefix("arn:")
        .and_then(|rest| rest.split_once(":iam::"))
        .and_then(|(p, rest)| Some((p, rest.strip_suffix(":root")?)))
        .filter(|(p, account)| {
            p.strip_prefix("aws")
                .is_some_and(|tail| tail.bytes().all(|b| b.is_ascii_alphabetic() || b == b'-'))
                && is_account_id(account)
        });
    match root {
        Some((p, _)) if p == partition => Ok(json!({"AWS": principal})),
        Some((p, _)) => Err(invalid_parameter(format!(
            "Principal '{principal}' is in partition '{p}', but the layer is in partition '{partition}'"
        ))),
        None => Err(invalid_parameter(format!(
            "Principal '{principal}' must be a 12-digit account ID, an account root ARN (arn:<partition>:iam::<account>:root), or *"
        ))),
    }
}

/// `OrganizationId` pattern `o-[a-z0-9]{10,32}`.
fn validate_org_id(org: &str) -> Result<&str, AwsServiceError> {
    let ok = org.strip_prefix("o-").is_some_and(|id| {
        (10..=32).contains(&id.len())
            && id
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit())
    });
    if ok {
        Ok(org)
    } else {
        Err(invalid_parameter(format!(
            "OrganizationId '{org}' must match o-[a-z0-9]{{10,32}}"
        )))
    }
}

#[cfg(test)]
mod tests {
    use crate::service::LambdaService;
    use crate::state::{LambdaState, SharedLambdaState};
    use bytes::Bytes;
    use fakecloud_core::auth::{Principal, ScpResolver};
    use fakecloud_core::multi_account::MultiRegionState;
    use fakecloud_core::service::{AwsRequest, AwsResponse, AwsService, AwsServiceError};
    use http::{HeaderMap, Method, StatusCode};
    use parking_lot::RwLock;
    use serde_json::{json, Value};
    use std::collections::HashMap;
    use std::sync::Arc;

    const OWNER: &str = "111111111111";
    const OTHER: &str = "222222222222";
    const REGION: &str = "us-east-1";
    const ORG: &str = "o-abcdefghij";

    /// `OTHER` belongs to `ORG`; nobody else is in an organization.
    struct OneOrg;
    impl ScpResolver for OneOrg {
        fn scps_for(&self, _: &Principal) -> Option<Vec<String>> {
            None
        }
        fn principal_org(&self, account_id: &str) -> Option<(String, String)> {
            (account_id == OTHER).then(|| (ORG.to_string(), format!("{ORG}/r-root/")))
        }
    }

    fn svc() -> LambdaService {
        let state: SharedLambdaState = Arc::new(RwLock::new(MultiRegionState::<LambdaState>::new(
            OWNER, REGION, "",
        )));
        LambdaService::new(state).with_org_resolver(Arc::new(OneOrg))
    }

    fn request(method: Method, path: &str, body: &str, account: &str, region: &str) -> AwsRequest {
        let (path, query) = path.split_once('?').unwrap_or((path, ""));
        AwsRequest {
            service: "lambda".to_string(),
            action: String::new(),
            region: region.to_string(),
            account_id: account.to_string(),
            request_id: "rid".to_string(),
            headers: HeaderMap::new(),
            query_params: query
                .split('&')
                .filter_map(|kv| kv.split_once('='))
                .map(|(k, v)| (k.to_string(), v.replace("%3A", ":")))
                .collect::<HashMap<_, _>>(),
            body: Bytes::from(body.to_string()),
            body_stream: parking_lot::Mutex::new(None),
            path_segments: fakecloud_core::path::split_path_segments(path),
            raw_path: path.to_string(),
            raw_query: query.to_string(),
            method,
            is_query_protocol: false,
            access_key_id: None,
            principal: None,
        }
    }

    async fn call(
        s: &LambdaService,
        method: Method,
        path: &str,
        body: Value,
        account: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = if body.is_null() {
            String::new()
        } else {
            body.to_string()
        };
        s.handle(request(method, path, &body, account, REGION))
            .await
    }

    fn json_of(resp: &AwsResponse) -> Value {
        serde_json::from_slice(resp.body.expect_bytes()).unwrap()
    }

    /// A layer ARN as an SDK puts it in the path.
    fn enc(arn: &str) -> String {
        arn.replace(':', "%3A")
    }

    fn layer_arn(account: &str, name: &str) -> String {
        format!("arn:aws:lambda:{REGION}:{account}:layer:{name}")
    }

    async fn publish(s: &LambdaService, account: &str, layer: &str) -> Value {
        let resp = call(
            s,
            Method::POST,
            &format!("/2018-10-31/layers/{}/versions", enc(layer)),
            json!({"Content": {"ZipFile": "UEsFBgAAAAAAAAAAAAAAAAAAAAAAAA=="}}),
            account,
        )
        .await
        .expect("publish");
        assert_eq!(resp.status, StatusCode::CREATED);
        json_of(&resp)
    }

    async fn grant(s: &LambdaService, version: i64, body: Value) -> Value {
        let resp = call(
            s,
            Method::POST,
            &format!("/2018-10-31/layers/shared/versions/{version}/policy"),
            body,
            OWNER,
        )
        .await
        .expect("AddLayerVersionPermission");
        assert_eq!(resp.status, StatusCode::CREATED);
        json_of(&resp)
    }

    fn get_version_path(version: i64) -> String {
        format!(
            "/2018-10-31/layers/{}/versions/{version}",
            enc(&layer_arn(OWNER, "shared"))
        )
    }

    fn denied(r: Result<AwsResponse, AwsServiceError>) -> AwsServiceError {
        let err = r.err().expect("must be refused");
        assert_eq!(err.code(), "AccessDeniedException", "{}", err.message());
        assert_eq!(err.status(), StatusCode::FORBIDDEN);
        err
    }

    #[tokio::test]
    async fn a_layer_arn_names_the_layer_not_a_new_key() {
        let s = svc();
        let out = publish(&s, OWNER, &layer_arn(OWNER, "by-arn")).await;
        assert_eq!(
            out["LayerVersionArn"],
            format!("{}:1", layer_arn(OWNER, "by-arn"))
        );
        let again = publish(&s, OWNER, "by-arn").await;
        assert_eq!(again["Version"], 2, "bare name and ARN are one layer");
        let keys: Vec<String> = s
            .state
            .read()
            .regional(OWNER, REGION)
            .unwrap()
            .layers
            .keys()
            .cloned()
            .collect();
        assert_eq!(keys, vec!["by-arn"]);
        let listed = call(
            &s,
            Method::GET,
            &format!(
                "/2018-10-31/layers/{}/versions",
                enc(&layer_arn(OWNER, "by-arn"))
            ),
            Value::Null,
            OWNER,
        )
        .await
        .unwrap();
        assert_eq!(
            json_of(&listed)["LayerVersions"].as_array().unwrap().len(),
            2
        );
    }

    #[tokio::test]
    async fn foreign_reads_need_a_grant_and_leak_nothing() {
        let s = svc();
        publish(&s, OWNER, "shared").await;

        // Unshared and missing versions look the same from outside.
        let unshared =
            denied(call(&s, Method::GET, &get_version_path(1), Value::Null, OTHER).await);
        let missing = denied(call(&s, Method::GET, &get_version_path(7), Value::Null, OTHER).await);
        assert_eq!(
            unshared.message().replace("shared:1 ", "shared:N "),
            missing.message().replace("shared:7 ", "shared:N ")
        );
        assert!(
            unshared.message().contains(&format!(
                "User: arn:aws:iam::{OTHER}:root is not authorized to perform: lambda:GetLayerVersion"
            )),
            "{}",
            unshared.message()
        );
        let missing_layer = denied(
            call(
                &s,
                Method::GET,
                &format!(
                    "/2018-10-31/layers/{}/versions/1",
                    enc(&layer_arn(OWNER, "never-published"))
                ),
                Value::Null,
                OTHER,
            )
            .await,
        );
        assert!(missing_layer.message().contains("never-published:1"));
        // The owner still sees a missing version as not found.
        let own_missing = call(&s, Method::GET, &get_version_path(7), Value::Null, OWNER)
            .await
            .err()
            .unwrap();
        assert_eq!(own_missing.code(), "ResourceNotFoundException");

        let stmt = grant(
            &s,
            1,
            json!({"StatementId": "xacct", "Action": "lambda:GetLayerVersion", "Principal": OTHER}),
        )
        .await;
        let stmt: Value = serde_json::from_str(stmt["Statement"].as_str().unwrap()).unwrap();
        assert_eq!(
            stmt["Principal"],
            json!({"AWS": format!("arn:aws:iam::{OTHER}:root")})
        );
        assert_eq!(
            stmt["Resource"],
            format!("{}:1", layer_arn(OWNER, "shared"))
        );

        let got = call(&s, Method::GET, &get_version_path(1), Value::Null, OTHER)
            .await
            .expect("shared version is readable");
        assert_eq!(json_of(&got)["LayerArn"], layer_arn(OWNER, "shared"));
        let by_arn = call(
            &s,
            Method::GET,
            &format!(
                "/2018-10-31/layers?find=LayerVersion&Arn={}",
                enc(&format!("{}:1", layer_arn(OWNER, "shared")))
            ),
            Value::Null,
            OTHER,
        )
        .await
        .expect("GetLayerVersionByArn of a shared version");
        assert_eq!(json_of(&by_arn)["Version"], 1);
        // A third account is still refused.
        denied(
            call(
                &s,
                Method::GET,
                &get_version_path(1),
                Value::Null,
                "333333333333",
            )
            .await,
        );
        // Listing another account's versions is never granted by a layer
        // version policy.
        denied(
            call(
                &s,
                Method::GET,
                &format!(
                    "/2018-10-31/layers/{}/versions",
                    enc(&layer_arn(OWNER, "shared"))
                ),
                Value::Null,
                OTHER,
            )
            .await,
        );
    }

    #[tokio::test]
    async fn an_organization_grant_admits_member_accounts_only() {
        let s = svc();
        publish(&s, OWNER, "shared").await;
        let stmt = grant(
            &s,
            1,
            json!({"StatementId": "org", "Action": "lambda:GetLayerVersion",
                   "Principal": "*", "OrganizationId": ORG}),
        )
        .await;
        let stmt: Value = serde_json::from_str(stmt["Statement"].as_str().unwrap()).unwrap();
        assert_eq!(stmt["Principal"], "*");
        assert_eq!(
            stmt["Condition"],
            json!({"StringEquals": {"aws:PrincipalOrgID": ORG}})
        );
        call(&s, Method::GET, &get_version_path(1), Value::Null, OTHER)
            .await
            .expect("a member of the organization");
        denied(
            call(
                &s,
                Method::GET,
                &get_version_path(1),
                Value::Null,
                "333333333333",
            )
            .await,
        );
    }

    #[tokio::test]
    async fn writes_through_a_foreign_arn_are_refused_and_change_nothing() {
        let s = svc();
        publish(&s, OWNER, "shared").await;
        grant(
            &s,
            1,
            json!({"StatementId": "all", "Action": "lambda:GetLayerVersion", "Principal": "*"}),
        )
        .await;
        let foreign = enc(&layer_arn(OWNER, "shared"));
        for (method, path, body, action) in [
            (
                Method::POST,
                format!("/2018-10-31/layers/{foreign}/versions"),
                json!({"Content": {"ZipFile": ""}}),
                "PublishLayerVersion",
            ),
            (
                Method::DELETE,
                format!("/2018-10-31/layers/{foreign}/versions/1"),
                Value::Null,
                "DeleteLayerVersion",
            ),
            (
                Method::GET,
                format!("/2018-10-31/layers/{foreign}/versions/1/policy"),
                Value::Null,
                "GetLayerVersionPolicy",
            ),
            (
                Method::POST,
                format!("/2018-10-31/layers/{foreign}/versions/1/policy"),
                json!({"StatementId": "mine", "Action": "lambda:GetLayerVersion", "Principal": "*"}),
                "AddLayerVersionPermission",
            ),
            (
                Method::DELETE,
                format!("/2018-10-31/layers/{foreign}/versions/1/policy/all"),
                Value::Null,
                "RemoveLayerVersionPermission",
            ),
        ] {
            let err = denied(call(&s, method, &path, body, OTHER).await);
            assert!(
                err.message()
                    .contains(&format!("lambda:{action} on resource")),
                "{action}: {}",
                err.message()
            );
        }
        let accounts = s.state.read();
        let layer = &accounts.regional(OWNER, REGION).unwrap().layers["shared"];
        assert_eq!(layer.versions.len(), 1);
        let policy: Value =
            serde_json::from_str(layer.versions[0].policy.as_ref().unwrap()).unwrap();
        assert_eq!(policy["Statement"].as_array().unwrap().len(), 1);
        assert!(
            accounts
                .regional(OTHER, REGION)
                .is_none_or(|st| st.layers.is_empty()),
            "nothing may be created in the caller's account"
        );
    }

    #[tokio::test]
    async fn permission_ops_on_missing_versions_are_not_found() {
        let s = svc();
        let body =
            json!({"StatementId": "s", "Action": "lambda:GetLayerVersion", "Principal": "*"});
        for path in [
            "/2018-10-31/layers/nope/versions/1/policy",
            "/2018-10-31/layers/shared/versions/2/policy",
        ] {
            if path.contains("shared") {
                publish(&s, OWNER, "shared").await;
            }
            for (method, path, body) in [
                (Method::POST, path.to_string(), body.clone()),
                (Method::GET, path.to_string(), Value::Null),
                (Method::DELETE, format!("{path}/s"), Value::Null),
            ] {
                let err = call(&s, method.clone(), &path, body, OWNER)
                    .await
                    .err()
                    .unwrap_or_else(|| panic!("{method} {path} must fail"));
                assert_eq!(err.code(), "ResourceNotFoundException", "{method} {path}");
            }
        }
        // A version label that is not a number is invalid, not version 0.
        for (method, body) in [(Method::POST, body.clone()), (Method::GET, Value::Null)] {
            let err = call(
                &s,
                method,
                "/2018-10-31/layers/shared/versions/abc/policy",
                body,
                OWNER,
            )
            .await
            .err()
            .unwrap();
            assert_eq!(err.code(), "InvalidParameterValueException");
        }
    }

    #[tokio::test]
    async fn permission_statements_follow_lambda_rules() {
        let s = svc();
        publish(&s, OWNER, "shared").await;
        let path = "/2018-10-31/layers/shared/versions/1/policy";
        // No policy yet.
        let err = call(&s, Method::GET, path, Value::Null, OWNER)
            .await
            .err()
            .unwrap();
        assert_eq!(err.code(), "ResourceNotFoundException");
        grant(
            &s,
            1,
            json!({"StatementId": "a", "Action": "lambda:GetLayerVersion",
                   "Principal": format!("arn:aws:iam::{OTHER}:root")}),
        )
        .await;
        let dup = call(
            &s,
            Method::POST,
            path,
            json!({"StatementId": "a", "Action": "lambda:GetLayerVersion", "Principal": "*"}),
            OWNER,
        )
        .await
        .err()
        .unwrap();
        assert_eq!(dup.code(), "ResourceConflictException");
        for bad in [
            json!({"StatementId": "b", "Action": "lambda:*", "Principal": "*"}),
            json!({"StatementId": "b", "Action": "lambda:GetLayerVersion", "Principal": "someone"}),
            json!({"StatementId": "b", "Action": "lambda:GetLayerVersion"}),
            json!({"Action": "lambda:GetLayerVersion", "Principal": "*"}),
            json!({"StatementId": "b", "Action": "lambda:GetLayerVersion", "Principal": "*", "OrganizationId": "org"}),
        ] {
            let err = call(&s, Method::POST, path, bad.clone(), OWNER)
                .await
                .err()
                .unwrap();
            assert_eq!(err.code(), "InvalidParameterValueException", "{bad}");
        }
        let policy = call(&s, Method::GET, path, Value::Null, OWNER)
            .await
            .unwrap();
        let doc: Value =
            serde_json::from_str(json_of(&policy)["Policy"].as_str().unwrap()).unwrap();
        assert_eq!(doc["Statement"].as_array().unwrap().len(), 1);

        let unknown = call(
            &s,
            Method::DELETE,
            &format!("{path}/zzz"),
            Value::Null,
            OWNER,
        )
        .await
        .err()
        .unwrap();
        assert_eq!(unknown.code(), "ResourceNotFoundException");
        let removed = call(&s, Method::DELETE, &format!("{path}/a"), Value::Null, OWNER)
            .await
            .unwrap();
        assert_eq!(removed.status, StatusCode::NO_CONTENT);
        let err = call(&s, Method::GET, path, Value::Null, OWNER)
            .await
            .err()
            .unwrap();
        assert_eq!(err.code(), "ResourceNotFoundException", "policy is gone");
    }

    #[tokio::test]
    async fn principals_follow_the_model_pattern_and_partition() {
        let s = svc();
        publish(&s, OWNER, "shared").await;
        let path = "/2018-10-31/layers/shared/versions/1/policy";
        for (i, (principal, org)) in [
            ("12345", None),
            ("1234567890123", None),
            ("arn:aws:iam::12345:root", None),
            ("arn:aws:iam::222222222222:user/u", None),
            ("arn:gcp:iam::222222222222:root", None),
            // A root ARN from another partition cannot use an `aws` layer.
            ("arn:aws-cn:iam::222222222222:root", None),
            // `OrganizationId` only narrows `*`.
            ("222222222222", Some(ORG)),
            ("arn:aws:iam::222222222222:root", Some(ORG)),
        ]
        .into_iter()
        .enumerate()
        {
            let mut body = json!({"StatementId": format!("s{i}"), "Action": "lambda:GetLayerVersion", "Principal": principal});
            if let Some(org) = org {
                body["OrganizationId"] = json!(org);
            }
            let err = call(&s, Method::POST, path, body, OWNER)
                .await
                .err()
                .unwrap_or_else(|| panic!("{principal} {org:?} must be refused"));
            assert_eq!(err.code(), "InvalidParameterValueException", "{principal}");
        }
        assert!(
            s.state.read().regional(OWNER, REGION).unwrap().layers["shared"].versions[0]
                .policy
                .is_none()
        );
        grant(
            &s,
            1,
            json!({"StatementId": "ok", "Action": "lambda:GetLayerVersion",
                   "Principal": "arn:aws:iam::222222222222:root"}),
        )
        .await;
    }

    #[tokio::test]
    async fn the_policy_revision_is_stable_and_guards_changes() {
        let s = svc();
        publish(&s, OWNER, "shared").await;
        let path = "/2018-10-31/layers/shared/versions/1/policy";
        // No policy yet: any expected revision is stale.
        let err = call(
            &s,
            Method::POST,
            &format!("{path}?RevisionId=nope"),
            json!({"StatementId": "a", "Action": "lambda:GetLayerVersion", "Principal": "*"}),
            OWNER,
        )
        .await
        .err()
        .unwrap();
        assert_eq!(err.code(), "PreconditionFailedException");
        assert_eq!(err.status(), StatusCode::PRECONDITION_FAILED);

        let first = grant(
            &s,
            1,
            json!({"StatementId": "a", "Action": "lambda:GetLayerVersion", "Principal": "*"}),
        )
        .await["RevisionId"]
            .as_str()
            .unwrap()
            .to_string();
        async fn read(s: &LambdaService) -> String {
            let resp = call(
                s,
                Method::GET,
                "/2018-10-31/layers/shared/versions/1/policy",
                Value::Null,
                OWNER,
            )
            .await
            .unwrap();
            json_of(&resp)["RevisionId"].as_str().unwrap().to_string()
        }
        assert_eq!(read(&s).await, first);
        assert_eq!(read(&s).await, first, "stable until the policy changes");

        // A stale revision refuses the change and leaves the policy alone.
        for (method, p, body) in [
            (
                Method::POST,
                format!("{path}?RevisionId=stale"),
                json!({"StatementId": "b", "Action": "lambda:GetLayerVersion", "Principal": OTHER}),
            ),
            (
                Method::DELETE,
                format!("{path}/a?RevisionId=stale"),
                Value::Null,
            ),
        ] {
            let err = call(&s, method, &p, body, OWNER).await.err().unwrap();
            assert_eq!(err.code(), "PreconditionFailedException", "{p}");
        }
        assert_eq!(read(&s).await, first);

        let second = json_of(
            &call(
                &s,
                Method::POST,
                &format!("{path}?RevisionId={first}"),
                json!({"StatementId": "b", "Action": "lambda:GetLayerVersion", "Principal": OTHER}),
                OWNER,
            )
            .await
            .expect("current revision"),
        )["RevisionId"]
            .as_str()
            .unwrap()
            .to_string();
        assert_ne!(second, first);
        assert_eq!(read(&s).await, second);
        call(
            &s,
            Method::DELETE,
            &format!("{path}/b?RevisionId={second}"),
            Value::Null,
            OWNER,
        )
        .await
        .expect("current revision");
        let third = read(&s).await;
        assert_ne!(third, second);
    }

    #[tokio::test]
    async fn attaching_a_missing_own_version_is_invalid() {
        let s = svc();
        publish(&s, OWNER, "own").await;
        let mut body = function_body("f", &[format!("{}:2", layer_arn(OWNER, "own"))]);
        body["Role"] = json!(format!("arn:aws:iam::{OWNER}:role/r"));
        let err = call(&s, Method::POST, "/2015-03-31/functions", body, OWNER)
            .await
            .err()
            .unwrap();
        assert_eq!(err.code(), "InvalidParameterValueException");
        assert_eq!(
            err.message(),
            format!(
                "Layer version {}:2 does not exist.",
                layer_arn(OWNER, "own")
            )
        );
        // A public layer fakecloud holds no copy of still attaches.
        let mut body = function_body(
            "g",
            &["arn:aws:lambda:us-east-1:017000801446:layer:AWSLambdaPowertoolsPythonV2:68".into()],
        );
        body["Role"] = json!(format!("arn:aws:iam::{OWNER}:role/r"));
        let created = call(&s, Method::POST, "/2015-03-31/functions", body, OWNER)
            .await
            .expect("external layer attaches");
        assert_eq!(json_of(&created)["Layers"][0]["CodeSize"], 0);
    }

    #[tokio::test]
    async fn account_principals_take_the_layer_partition() {
        let s = svc();
        let resp = s
            .handle(request(
                Method::POST,
                "/2018-10-31/layers/cn-layer/versions",
                &json!({"Content": {"ZipFile": ""}}).to_string(),
                OWNER,
                "cn-north-1",
            ))
            .await
            .unwrap();
        assert_eq!(
            json_of(&resp)["LayerVersionArn"],
            format!("arn:aws-cn:lambda:cn-north-1:{OWNER}:layer:cn-layer:1")
        );
        let resp = s
            .handle(request(
                Method::POST,
                "/2018-10-31/layers/cn-layer/versions/1/policy",
                &json!({"StatementId": "x", "Action": "lambda:GetLayerVersion", "Principal": OTHER})
                    .to_string(),
                OWNER,
                "cn-north-1",
            ))
            .await
            .unwrap();
        let stmt: Value =
            serde_json::from_str(json_of(&resp)["Statement"].as_str().unwrap()).unwrap();
        assert_eq!(
            stmt["Principal"],
            json!({"AWS": format!("arn:aws-cn:iam::{OTHER}:root")})
        );
        let read = s
            .handle(request(
                Method::GET,
                &format!(
                    "/2018-10-31/layers/{}/versions/1",
                    enc(&format!(
                        "arn:aws-cn:lambda:cn-north-1:{OWNER}:layer:cn-layer"
                    ))
                ),
                "",
                OTHER,
                "cn-north-1",
            ))
            .await;
        assert!(read.is_ok(), "{:?}", read.err().map(|e| e.message()));
    }

    fn function_body(name: &str, layers: &[String]) -> Value {
        json!({
            "FunctionName": name,
            "Runtime": "python3.12",
            "Role": format!("arn:aws:iam::{OTHER}:role/r"),
            "Handler": "index.handler",
            "Code": {"ZipFile": "UEsFBgAAAAAAAAAAAAAAAAAAAAAAAA=="},
            "Layers": layers,
        })
    }

    #[tokio::test]
    async fn attaching_a_foreign_layer_needs_a_grant() {
        let s = svc();
        publish(&s, OWNER, "shared").await;
        publish(&s, OWNER, "shared").await;
        let v1 = format!("{}:1", layer_arn(OWNER, "shared"));
        let v2 = format!("{}:2", layer_arn(OWNER, "shared"));
        let v9 = format!("{}:9", layer_arn(OWNER, "shared"));
        for arn in [&v1, &v9] {
            let err = denied(
                call(
                    &s,
                    Method::POST,
                    "/2015-03-31/functions",
                    function_body("f", std::slice::from_ref(arn)),
                    OTHER,
                )
                .await,
            );
            assert!(
                err.message().contains("lambda:GetLayerVersion"),
                "{}",
                err.message()
            );
        }
        assert!(s
            .state
            .read()
            .regional(OTHER, REGION)
            .is_none_or(|st| st.functions.is_empty()));

        grant(
            &s,
            1,
            json!({"StatementId": "x", "Action": "lambda:GetLayerVersion", "Principal": OTHER}),
        )
        .await;
        let created = call(
            &s,
            Method::POST,
            "/2015-03-31/functions",
            function_body("f", std::slice::from_ref(&v1)),
            OTHER,
        )
        .await
        .expect("shared layer attaches");
        assert_eq!(json_of(&created)["Layers"][0]["Arn"], v1);
        assert_eq!(json_of(&created)["Layers"][0]["CodeSize"], 22);

        // Version 2 was not shared: switching to it is refused and the
        // function keeps its layers.
        denied(
            call(
                &s,
                Method::PUT,
                "/2015-03-31/functions/f/configuration",
                json!({"Layers": [v2]}),
                OTHER,
            )
            .await,
        );
        let accounts = s.state.read();
        let f = &accounts.regional(OTHER, REGION).unwrap().functions["f"];
        assert_eq!(f.layers.len(), 1);
        assert_eq!(f.layers[0].arn, v1);
        // The attached layer's content is what invoke mounts.
        assert_eq!(crate::state::attached_layer_zips(&accounts, f).len(), 1);
    }

    /// Under `--region local` another account's layer ARN (which does not
    /// match AWS's region pattern) still names the shared layer, while a
    /// malformed name is still refused by input validation.
    #[tokio::test]
    async fn a_shared_layer_is_addressable_by_arn_under_a_local_region() {
        let s = svc();
        let local = |method: Method, path: &str, body: Value, account: &str| {
            let body = if body.is_null() {
                String::new()
            } else {
                body.to_string()
            };
            request(method, path, &body, account, "local")
        };
        s.handle(local(
            Method::POST,
            "/2018-10-31/layers/lyr/versions",
            json!({"Content": {"ZipFile": ""}}),
            OWNER,
        ))
        .await
        .unwrap();
        s.handle(local(
            Method::POST,
            "/2018-10-31/layers/lyr/versions/1/policy",
            json!({"StatementId": "x", "Action": "lambda:GetLayerVersion", "Principal": OTHER}),
            OWNER,
        ))
        .await
        .unwrap();
        let arn = format!("arn:aws:lambda:local:{OWNER}:layer:lyr");
        let got = s
            .handle(local(
                Method::GET,
                &format!("/2018-10-31/layers/{}/versions/1", enc(&arn)),
                Value::Null,
                OTHER,
            ))
            .await
            .expect("shared layer by ARN under --region local");
        assert_eq!(json_of(&got)["LayerArn"], arn);
        let err = s
            .handle(local(
                Method::GET,
                "/2018-10-31/layers/bad!name/versions/1",
                Value::Null,
                OTHER,
            ))
            .await
            .err()
            .unwrap();
        assert_eq!(err.code(), "ValidationException");
        // Only the region is exempt from the `LayerName` pattern: an account
        // that is not 12 digits is refused, in a fakecloud region or an AWS
        // one.
        for (arn, region) in [
            ("arn:aws:lambda:local:42:layer:lyr", "local"),
            ("arn:aws:lambda:us-east-1:42:layer:lyr", "us-east-1"),
        ] {
            let err = s
                .handle(request(
                    Method::GET,
                    &format!("/2018-10-31/layers/{}/versions/1", enc(arn)),
                    "",
                    OTHER,
                    region,
                ))
                .await
                .err()
                .unwrap_or_else(|| panic!("{arn} must be refused"));
            assert_eq!(err.code(), "ValidationException", "{arn}");
        }
    }

    /// `--region local` with a non-AWS account id: the layer ARN fakecloud
    /// mints there attaches, resolves for invoke, and reads back by ARN.
    #[tokio::test]
    async fn layers_minted_under_a_local_region_round_trip() {
        let s = svc();
        let local = |method: Method, path: &str, body: Value| {
            request(method, path, &body.to_string(), "000000000000", "local")
        };
        let published = s
            .handle(local(
                Method::POST,
                "/2018-10-31/layers/lyr/versions",
                json!({"Content": {"ZipFile": "UEsFBgAAAAAAAAAAAAAAAAAAAAAAAA=="}}),
            ))
            .await
            .unwrap();
        let arn = json_of(&published)["LayerVersionArn"]
            .as_str()
            .unwrap()
            .to_string();
        assert_eq!(arn, "arn:aws:lambda:local:000000000000:layer:lyr:1");
        let mut body = function_body("f", std::slice::from_ref(&arn));
        body["Role"] = json!("arn:aws:iam::000000000000:role/r");
        s.handle(local(Method::POST, "/2015-03-31/functions", body))
            .await
            .expect("create");
        {
            let accounts = s.state.read();
            let f = &accounts
                .regional("000000000000", "local")
                .unwrap()
                .functions["f"];
            assert_eq!(f.layers[0].code_size, 22);
            assert_eq!(crate::state::attached_layer_zips(&accounts, f).len(), 1);
        }
        let by_arn = s
            .handle(local(
                Method::GET,
                &format!("/2018-10-31/layers?find=LayerVersion&Arn={}", enc(&arn)),
                Value::Null,
            ))
            .await;
        assert!(by_arn.is_ok(), "{:?}", by_arn.err().map(|e| e.message()));
    }
}
