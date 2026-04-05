use std::collections::HashMap;

use async_trait::async_trait;
use chrono::Utc;
use http::StatusCode;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsService, AwsServiceError};

use crate::state::{
    MaintenanceWindow, MaintenanceWindowTarget, MaintenanceWindowTask, PatchBaseline, PatchGroup,
    SharedSsmState, SsmCommand, SsmDocument, SsmDocumentVersion, SsmParameter, SsmParameterVersion,
};

use fakecloud_secretsmanager::state::SharedSecretsManagerState;

const PARAMETER_VERSION_LIMIT: i64 = 100;

pub struct SsmService {
    state: SharedSsmState,
    secretsmanager_state: Option<SharedSecretsManagerState>,
}

impl SsmService {
    pub fn new(state: SharedSsmState) -> Self {
        Self {
            state,
            secretsmanager_state: None,
        }
    }

    pub fn with_secretsmanager(mut self, sm_state: SharedSecretsManagerState) -> Self {
        self.secretsmanager_state = Some(sm_state);
        self
    }
}

#[async_trait]
impl AwsService for SsmService {
    fn service_name(&self) -> &str {
        "ssm"
    }

    async fn handle(&self, req: AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        match req.action.as_str() {
            "PutParameter" => self.put_parameter(&req),
            "GetParameter" => self.get_parameter(&req),
            "GetParameters" => self.get_parameters(&req),
            "GetParametersByPath" => self.get_parameters_by_path(&req),
            "DeleteParameter" => self.delete_parameter(&req),
            "DeleteParameters" => self.delete_parameters(&req),
            "DescribeParameters" => self.describe_parameters(&req),
            "GetParameterHistory" => self.get_parameter_history(&req),
            "AddTagsToResource" => self.add_tags_to_resource(&req),
            "RemoveTagsFromResource" => self.remove_tags_from_resource(&req),
            "ListTagsForResource" => self.list_tags_for_resource(&req),
            "LabelParameterVersion" => self.label_parameter_version(&req),
            "UnlabelParameterVersion" => self.unlabel_parameter_version(&req),
            "CreateDocument" => self.create_document(&req),
            "GetDocument" => self.get_document(&req),
            "DeleteDocument" => self.delete_document(&req),
            "UpdateDocument" => self.update_document(&req),
            "DescribeDocument" => self.describe_document(&req),
            "UpdateDocumentDefaultVersion" => self.update_document_default_version(&req),
            "ListDocuments" => self.list_documents(&req),
            "DescribeDocumentPermission" => self.describe_document_permission(&req),
            "ModifyDocumentPermission" => self.modify_document_permission(&req),
            "SendCommand" => self.send_command(&req),
            "ListCommands" => self.list_commands(&req),
            "GetCommandInvocation" => self.get_command_invocation(&req),
            "ListCommandInvocations" => self.list_command_invocations(&req),
            "CancelCommand" => self.cancel_command(&req),
            "CreateMaintenanceWindow" => self.create_maintenance_window(&req),
            "DescribeMaintenanceWindows" => self.describe_maintenance_windows(&req),
            "GetMaintenanceWindow" => self.get_maintenance_window(&req),
            "DeleteMaintenanceWindow" => self.delete_maintenance_window(&req),
            "UpdateMaintenanceWindow" => self.update_maintenance_window(&req),
            "RegisterTargetWithMaintenanceWindow" => {
                self.register_target_with_maintenance_window(&req)
            }
            "DeregisterTargetFromMaintenanceWindow" => {
                self.deregister_target_from_maintenance_window(&req)
            }
            "DescribeMaintenanceWindowTargets" => self.describe_maintenance_window_targets(&req),
            "RegisterTaskWithMaintenanceWindow" => self.register_task_with_maintenance_window(&req),
            "DeregisterTaskFromMaintenanceWindow" => {
                self.deregister_task_from_maintenance_window(&req)
            }
            "DescribeMaintenanceWindowTasks" => self.describe_maintenance_window_tasks(&req),
            "CreatePatchBaseline" => self.create_patch_baseline(&req),
            "DeletePatchBaseline" => self.delete_patch_baseline(&req),
            "DescribePatchBaselines" => self.describe_patch_baselines(&req),
            "GetPatchBaseline" => self.get_patch_baseline(&req),
            "RegisterPatchBaselineForPatchGroup" => {
                self.register_patch_baseline_for_patch_group(&req)
            }
            "DeregisterPatchBaselineForPatchGroup" => {
                self.deregister_patch_baseline_for_patch_group(&req)
            }
            "GetPatchBaselineForPatchGroup" => self.get_patch_baseline_for_patch_group(&req),
            "DescribePatchGroups" => self.describe_patch_groups(&req),
            _ => Err(AwsServiceError::action_not_implemented("ssm", &req.action)),
        }
    }

    fn supported_actions(&self) -> &[&str] {
        &[
            "PutParameter",
            "GetParameter",
            "GetParameters",
            "GetParametersByPath",
            "DeleteParameter",
            "DeleteParameters",
            "DescribeParameters",
            "GetParameterHistory",
            "AddTagsToResource",
            "RemoveTagsFromResource",
            "ListTagsForResource",
            "LabelParameterVersion",
            "UnlabelParameterVersion",
            "CreateDocument",
            "GetDocument",
            "DeleteDocument",
            "UpdateDocument",
            "DescribeDocument",
            "UpdateDocumentDefaultVersion",
            "ListDocuments",
            "DescribeDocumentPermission",
            "ModifyDocumentPermission",
            "SendCommand",
            "ListCommands",
            "GetCommandInvocation",
            "ListCommandInvocations",
            "CancelCommand",
            "CreateMaintenanceWindow",
            "DescribeMaintenanceWindows",
            "GetMaintenanceWindow",
            "DeleteMaintenanceWindow",
            "UpdateMaintenanceWindow",
            "RegisterTargetWithMaintenanceWindow",
            "DeregisterTargetFromMaintenanceWindow",
            "DescribeMaintenanceWindowTargets",
            "RegisterTaskWithMaintenanceWindow",
            "DeregisterTaskFromMaintenanceWindow",
            "DescribeMaintenanceWindowTasks",
            "CreatePatchBaseline",
            "DeletePatchBaseline",
            "DescribePatchBaselines",
            "GetPatchBaseline",
            "RegisterPatchBaselineForPatchGroup",
            "DeregisterPatchBaselineForPatchGroup",
            "GetPatchBaselineForPatchGroup",
            "DescribePatchGroups",
        ]
    }
}

fn parse_body(req: &AwsRequest) -> Value {
    serde_json::from_slice(&req.body).unwrap_or(Value::Object(Default::default()))
}

fn json_resp(body: Value) -> AwsResponse {
    AwsResponse::json(StatusCode::OK, serde_json::to_string(&body).unwrap())
}

/// Normalize a parameter name - try looking up with and without leading slash
fn lookup_param<'a>(
    parameters: &'a std::collections::BTreeMap<String, SsmParameter>,
    name: &str,
) -> Option<&'a SsmParameter> {
    // Direct lookup
    if let Some(p) = parameters.get(name) {
        return Some(p);
    }
    // Try with leading slash added/removed
    if let Some(stripped) = name.strip_prefix('/') {
        parameters.get(stripped)
    } else {
        parameters.get(&format!("/{name}"))
    }
}

fn lookup_param_mut<'a>(
    parameters: &'a mut std::collections::BTreeMap<String, SsmParameter>,
    name: &str,
) -> Option<&'a mut SsmParameter> {
    // Direct lookup first
    if parameters.contains_key(name) {
        return parameters.get_mut(name);
    }
    // Try alternate form
    let alt = if let Some(stripped) = name.strip_prefix('/') {
        stripped.to_string()
    } else {
        format!("/{name}")
    };
    parameters.get_mut(&alt)
}

fn remove_param(
    parameters: &mut std::collections::BTreeMap<String, SsmParameter>,
    name: &str,
) -> Option<SsmParameter> {
    if let Some(p) = parameters.remove(name) {
        return Some(p);
    }
    let alt = if let Some(stripped) = name.strip_prefix('/') {
        stripped.to_string()
    } else {
        format!("/{name}")
    };
    parameters.remove(&alt)
}

/// Convert document content between JSON and YAML formats.
/// Falls back to returning content as-is if conversion isn't possible.
fn convert_document_content(content: &str, from_format: &str, to_format: &str) -> String {
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

fn param_arn(region: &str, account_id: &str, name: &str) -> String {
    if name.starts_with('/') {
        format!("arn:aws:ssm:{region}:{account_id}:parameter{name}")
    } else {
        format!("arn:aws:ssm:{region}:{account_id}:parameter/{name}")
    }
}

/// Rewrite the region component of a parameter ARN.
fn rewrite_arn_region(arn: &str, region: &str) -> String {
    let parts: Vec<&str> = arn.splitn(6, ':').collect();
    if parts.len() == 6 {
        format!(
            "{}:{}:{}:{}:{}:{}",
            parts[0], parts[1], parts[2], region, parts[4], parts[5]
        )
    } else {
        arn.to_string()
    }
}

fn param_to_json(p: &SsmParameter, with_value: bool, with_decryption: bool, region: &str) -> Value {
    let arn = rewrite_arn_region(&p.arn, region);
    let mut v = json!({
        "Name": p.name,
        "Type": p.param_type,
        "Version": p.version,
        "ARN": arn,
        "LastModifiedDate": p.last_modified.timestamp_millis() as f64 / 1000.0,
        "DataType": p.data_type,
    });
    if with_value {
        if p.param_type == "SecureString" {
            let key_id = p.key_id.as_deref().unwrap_or("alias/aws/ssm");
            if with_decryption {
                // Decrypted: return plain value
                v["Value"] = json!(p.value);
            } else {
                // Not decrypted: return kms:KEY_ID:VALUE (Moto format)
                v["Value"] = json!(format!("kms:{}:{}", key_id, p.value));
            }
        } else {
            v["Value"] = json!(p.value);
        }
    }
    v
}

fn param_to_describe_json(p: &SsmParameter, region: &str) -> Value {
    let arn = rewrite_arn_region(&p.arn, region);
    let mut v = json!({
        "Name": p.name,
        "Type": p.param_type,
        "Version": p.version,
        "ARN": arn,
        "LastModifiedDate": p.last_modified.timestamp_millis() as f64 / 1000.0,
        "LastModifiedUser": "N/A",
        "DataType": p.data_type,
        "Tier": p.tier,
    });
    if let Some(desc) = &p.description {
        v["Description"] = json!(desc);
    }
    if let Some(pattern) = &p.allowed_pattern {
        v["AllowedPattern"] = json!(pattern);
    }
    if let Some(key_id) = &p.key_id {
        v["KeyId"] = json!(key_id);
    }
    // Add policies if present and valid JSON
    if let Some(policies_str) = &p.policies {
        if let Ok(parsed) = serde_json::from_str::<Value>(policies_str) {
            if let Some(arr) = parsed.as_array() {
                let policy_objects: Vec<Value> = arr
                    .iter()
                    .filter_map(|p| p.as_str())
                    .map(|p| {
                        json!({
                            "PolicyText": p,
                            "PolicyType": p,
                            "PolicyStatus": "Finished",
                        })
                    })
                    .collect();
                if !policy_objects.is_empty() {
                    v["Policies"] = json!(policy_objects);
                }
            }
        }
    }
    v
}

/// Validate parameter name restrictions. Returns an error message string on failure.
fn validate_param_name(name: &str) -> Option<AwsServiceError> {
    let lower = name.to_lowercase();

    if let Some(stripped) = name.strip_prefix('/') {
        // Path-style names
        let first_segment = stripped.split('/').next().unwrap_or("");
        let first_lower = first_segment.to_lowercase();
        if first_lower.starts_with("aws") {
            return Some(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                format!("No access to reserved parameter name: {name}."),
            ));
        }
        if first_lower.starts_with("ssm") {
            return Some(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                "Parameter name: can't be prefixed with \"ssm\" (case-insensitive). \
                 If formed as a path, it can consist of sub-paths divided by slash \
                 symbol; each sub-path can be formed as a mix of letters, numbers \
                 and the following 3 symbols .-_"
                    .to_string(),
            ));
        }
    } else {
        // Non-path names
        if lower.starts_with("aws") || lower.starts_with("ssm") {
            return Some(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                "Parameter name: can't be prefixed with \"aws\" or \"ssm\" (case-insensitive)."
                    .to_string(),
            ));
        }
    }
    None
}

/// Parse a parameter name that may include version or label selector.
/// Returns (base_name, selector) where selector can be version number or label string.
enum ParamSelector {
    None,
    Version(i64),
    Label(String),
    Invalid(String), // name with too many colons
}

fn parse_param_selector(name: &str) -> (&str, ParamSelector) {
    // Check for `:` separator (version or label)
    if let Some(colon_pos) = name.rfind(':') {
        let base = &name[..colon_pos];
        let selector = &name[colon_pos + 1..];

        // Check if there's another colon (invalid)
        if base.contains(':') {
            return (name, ParamSelector::Invalid(name.to_string()));
        }

        if let Ok(version) = selector.parse::<i64>() {
            (base, ParamSelector::Version(version))
        } else {
            (base, ParamSelector::Label(selector.to_string()))
        }
    } else {
        (name, ParamSelector::None)
    }
}

impl SsmService {
    fn put_parameter(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let name = body["Name"]
            .as_str()
            .ok_or_else(|| missing("Name"))?
            .to_string();
        let value = body["Value"]
            .as_str()
            .ok_or_else(|| missing("Value"))?
            .to_string();

        // Validate empty value
        if value.is_empty() {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                "1 validation error detected: \
                 Value '' at 'value' failed to satisfy constraint: \
                 Member must have length greater than or equal to 1.",
            ));
        }

        let param_type = body["Type"].as_str().map(|s| s.to_string());
        let overwrite = body["Overwrite"].as_bool().unwrap_or(false);
        let description = body["Description"].as_str().map(|s| s.to_string());
        let key_id = body["KeyId"].as_str().map(|s| s.to_string());
        let allowed_pattern = body["AllowedPattern"].as_str().map(|s| s.to_string());
        let data_type = body["DataType"].as_str().unwrap_or("text").to_string();
        let tier = body["Tier"].as_str().unwrap_or("Standard").to_string();
        let policies = body["Policies"].as_str().map(|s| s.to_string());
        let tags: Option<Vec<(String, String)>> = body["Tags"].as_array().map(|arr| {
            arr.iter()
                .filter_map(|t| {
                    let k = t["Key"].as_str()?;
                    let v = t["Value"].as_str()?;
                    Some((k.to_string(), v.to_string()))
                })
                .collect()
        });

        // Validate data type
        if !["text", "aws:ec2:image"].contains(&data_type.as_str()) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                format!(
                    "The following data type is not supported: {data_type} \
                     (Data type names are all lowercase.)"
                ),
            ));
        }

        // Validate param type
        if let Some(ref pt) = param_type {
            if !["String", "StringList", "SecureString"].contains(&pt.as_str()) {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ValidationException",
                    format!(
                        "1 validation error detected: Value '{pt}' at 'type' \
                         failed to satisfy constraint: Member must satisfy enum value set: \
                         [SecureString, StringList, String]"
                    ),
                ));
            }
        }

        // Validate name
        if let Some(err) = validate_param_name(&name) {
            return Err(err);
        }

        let mut state = self.state.write();

        if let Some(existing) = lookup_param_mut(&mut state.parameters, &name) {
            if !overwrite {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ParameterAlreadyExists",
                    "The parameter already exists. To overwrite this value, set the \
                     overwrite option in the request to true.",
                ));
            }

            // Cannot have tags and overwrite at the same time
            if tags.is_some() {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ValidationException",
                    "Invalid request: tags and overwrite can't be used together.",
                ));
            }

            // Check version limit
            if existing.version >= PARAMETER_VERSION_LIMIT {
                // Check if oldest version has a label
                let oldest_version = existing
                    .history
                    .first()
                    .map(|h| h.version)
                    .unwrap_or(existing.version);
                let oldest_has_label = existing
                    .labels
                    .get(&oldest_version)
                    .is_some_and(|l| !l.is_empty());

                if oldest_has_label {
                    return Err(AwsServiceError::aws_error(
                        StatusCode::BAD_REQUEST,
                        "ParameterMaxVersionLimitExceeded",
                        format!(
                            "You attempted to create a new version of {} by calling \
                             the PutParameter API with the overwrite flag. Version {}, \
                             the oldest version, can't be deleted because it has a label \
                             associated with it. Move the label to another version of the \
                             parameter, and try again.",
                            name, oldest_version
                        ),
                    ));
                }

                // Delete oldest version from history to make room
                if !existing.history.is_empty() {
                    let removed = existing.history.remove(0);
                    existing.labels.remove(&removed.version);
                }
            }

            let now = Utc::now();
            let current_labels = existing
                .labels
                .get(&existing.version)
                .cloned()
                .unwrap_or_default();
            existing.history.push(SsmParameterVersion {
                value: existing.value.clone(),
                version: existing.version,
                last_modified: existing.last_modified,
                param_type: existing.param_type.clone(),
                description: existing.description.clone(),
                key_id: existing.key_id.clone(),
                labels: current_labels,
            });
            existing.version += 1;
            existing.value = value;
            existing.last_modified = now;

            // Only update these if provided
            if let Some(pt) = param_type {
                existing.param_type = pt;
            }
            if description.is_some() {
                existing.description = description;
            }
            if key_id.is_some() {
                existing.key_id = key_id;
            }
            // Always update data_type if explicitly provided
            if body["DataType"].as_str().is_some() {
                existing.data_type = data_type;
            }

            let resp_tier = existing.tier.clone();
            return Ok(json_resp(json!({
                "Version": existing.version,
                "Tier": resp_tier,
            })));
        }

        // New parameter - type is required
        let param_type = match param_type {
            Some(pt) => pt,
            None => {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ValidationException",
                    "A parameter type is required when you create a parameter.",
                ));
            }
        };

        let now = Utc::now();
        let arn = param_arn(&req.region, &state.account_id, &name);

        let mut tag_map = HashMap::new();
        if let Some(tag_list) = tags {
            for (k, v) in tag_list {
                tag_map.insert(k, v);
            }
        }

        let param = SsmParameter {
            name: name.clone(),
            value,
            param_type,
            version: 1,
            arn,
            last_modified: now,
            history: Vec::new(),
            labels: HashMap::new(),
            tags: tag_map,
            description,
            allowed_pattern,
            key_id,
            data_type,
            tier: tier.clone(),
            policies,
        };

        state.parameters.insert(name, param);
        Ok(json_resp(json!({
            "Version": 1,
            "Tier": tier,
        })))
    }

    /// Resolve a Secrets Manager reference parameter.
    /// Path format: /aws/reference/secretsmanager/{secret-name}
    fn resolve_secretsmanager_param(
        &self,
        raw_name: &str,
        secret_name: &str,
        region: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let sm_state = self.secretsmanager_state.as_ref().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ParameterNotFound",
                format!(
                    "An error occurred (ParameterNotFound) when referencing \
                     Secrets Manager: Secret {raw_name} not found.",
                ),
            )
        })?;

        let sm = sm_state.read();
        let secret = sm.secrets.get(secret_name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ParameterNotFound",
                format!(
                    "An error occurred (ParameterNotFound) when referencing \
                     Secrets Manager: Secret {raw_name} not found.",
                ),
            )
        })?;

        if secret.deleted {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ParameterNotFound",
                format!(
                    "An error occurred (ParameterNotFound) when referencing \
                     Secrets Manager: Secret {raw_name} not found.",
                ),
            ));
        }

        // Get the current version's secret string
        let version = secret
            .versions
            .get(&secret.current_version_id)
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ParameterNotFound",
                    format!(
                        "An error occurred (ParameterNotFound) when referencing \
                     Secrets Manager: Secret {raw_name} not found.",
                    ),
                )
            })?;

        let value = version.secret_string.as_deref().unwrap_or("").to_string();

        let ssm_state = self.state.read();
        let arn = format!(
            "arn:aws:ssm:{region}:{}:parameter{}",
            ssm_state.account_id, raw_name
        );

        Ok(json_resp(json!({
            "Parameter": {
                "Name": raw_name,
                "Type": "SecureString",
                "Value": value,
                "Version": 0,
                "ARN": arn,
                "LastModifiedDate": version.created_at.timestamp_millis() as f64 / 1000.0,
                "DataType": "text",
                "SourceResult": secret.arn,
            }
        })))
    }

    fn get_parameter(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let raw_name = body["Name"].as_str().ok_or_else(|| missing("Name"))?;
        let with_decryption = body["WithDecryption"].as_bool().unwrap_or(false);

        // Check for Secrets Manager references - require WithDecryption=true
        if raw_name.starts_with("/aws/reference/secretsmanager/") && !with_decryption {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                "WithDecryption flag must be True for retrieving a Secret Manager secret.",
            ));
        }

        // Resolve Secrets Manager references via cross-service lookup
        if let Some(secret_name) = raw_name.strip_prefix("/aws/reference/secretsmanager/") {
            return self.resolve_secretsmanager_param(raw_name, secret_name, &req.region);
        }

        let state = self.state.read();

        // Handle ARN-style names directly (they contain many colons)
        if raw_name.starts_with("arn:aws:ssm:") {
            let param = resolve_param_by_name_or_arn(&state, raw_name)?;
            return Ok(json_resp(json!({
                "Parameter": param_to_json(param, true, with_decryption, &req.region),
            })));
        }

        let (base_name, selector) = parse_param_selector(raw_name);

        // Check for invalid selectors (too many colons)
        if let ParamSelector::Invalid(n) = selector {
            return Err(param_not_found(&n));
        }

        // Try looking up by name or by ARN - use raw_name in error for full context
        let param = resolve_param_by_name_or_arn(&state, base_name)
            .map_err(|_| param_not_found(raw_name))?;

        match selector {
            ParamSelector::None => Ok(json_resp(json!({
                "Parameter": param_to_json(param, true, with_decryption, &req.region),
            }))),
            ParamSelector::Version(ver) => {
                if param.version == ver {
                    return Ok(json_resp(json!({
                        "Parameter": param_to_json(param, true, with_decryption, &req.region),
                    })));
                }
                // Look in history
                if let Some(hist) = param.history.iter().find(|h| h.version == ver) {
                    let mut v = json!({
                        "Name": param.name,
                        "Type": hist.param_type,
                        "Version": hist.version,
                        "ARN": rewrite_arn_region(&param.arn, &req.region),
                        "LastModifiedDate": hist.last_modified.timestamp_millis() as f64 / 1000.0,
                        "DataType": param.data_type,
                    });
                    if param.param_type == "SecureString" && !with_decryption {
                        v["Value"] = json!("****");
                    } else {
                        v["Value"] = json!(hist.value);
                    }
                    return Ok(json_resp(json!({ "Parameter": v })));
                }
                Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ParameterVersionNotFound",
                    format!(
                        "Systems Manager could not find version {} of {}. \
                         Verify the version and try again.",
                        ver, base_name
                    ),
                ))
            }
            ParamSelector::Label(label) => {
                // Find version with this label
                for (ver, labels) in &param.labels {
                    if labels.contains(&label) {
                        if *ver == param.version {
                            return Ok(json_resp(json!({
                                "Parameter": param_to_json(param, true, with_decryption, &req.region),
                            })));
                        }
                        if let Some(hist) = param.history.iter().find(|h| h.version == *ver) {
                            let mut v = json!({
                                "Name": param.name,
                                "Type": hist.param_type,
                                "Version": hist.version,
                                "ARN": rewrite_arn_region(&param.arn, &req.region),
                                "LastModifiedDate": hist.last_modified.timestamp_millis() as f64 / 1000.0,
                                "DataType": param.data_type,
                            });
                            if param.param_type == "SecureString" && !with_decryption {
                                v["Value"] = json!("****");
                            } else {
                                v["Value"] = json!(hist.value);
                            }
                            return Ok(json_resp(json!({ "Parameter": v })));
                        }
                    }
                }
                Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ParameterVersionLabelNotFound",
                    format!(
                        "Systems Manager could not find label {} for parameter {}. \
                         Verify the label and try again.",
                        label, base_name
                    ),
                ))
            }
            ParamSelector::Invalid(_) => unreachable!(),
        }
    }

    fn get_parameters(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let names = body["Names"].as_array().ok_or_else(|| missing("Names"))?;
        let with_decryption = body["WithDecryption"].as_bool().unwrap_or(false);

        // Validate max 10 names
        if names.len() > 10 {
            let name_strs: Vec<&str> = names.iter().filter_map(|n| n.as_str()).collect();
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                format!(
                    "1 validation error detected: \
                     Value '[{}]' at 'names' failed to satisfy constraint: \
                     Member must have length less than or equal to 10.",
                    name_strs.join(", ")
                ),
            ));
        }

        let state = self.state.read();
        let mut parameters = Vec::new();
        let mut invalid = Vec::new();
        let mut seen_names = std::collections::HashSet::new();

        for name_val in names {
            if let Some(raw_name) = name_val.as_str() {
                // Deduplicate
                if !seen_names.insert(raw_name.to_string()) {
                    continue;
                }

                let (base_name, selector) = parse_param_selector(raw_name);

                match selector {
                    ParamSelector::Invalid(_) => {
                        invalid.push(raw_name.to_string());
                    }
                    ParamSelector::None => {
                        if let Some(param) = lookup_param(&state.parameters, base_name) {
                            parameters.push(param_to_json(
                                param,
                                true,
                                with_decryption,
                                &req.region,
                            ));
                        } else {
                            invalid.push(raw_name.to_string());
                        }
                    }
                    ParamSelector::Version(ver) => {
                        if let Some(param) = lookup_param(&state.parameters, base_name) {
                            if param.version == ver {
                                parameters.push(param_to_json(
                                    param,
                                    true,
                                    with_decryption,
                                    &req.region,
                                ));
                            } else if let Some(hist) =
                                param.history.iter().find(|h| h.version == ver)
                            {
                                let mut v = json!({
                                    "Name": param.name,
                                    "Type": hist.param_type,
                                    "Version": hist.version,
                                    "ARN": rewrite_arn_region(&param.arn, &req.region),
                                    "LastModifiedDate": hist.last_modified.timestamp_millis() as f64 / 1000.0,
                                    "DataType": param.data_type,
                                });
                                v["Value"] = json!(hist.value);
                                parameters.push(v);
                            } else {
                                invalid.push(raw_name.to_string());
                            }
                        } else {
                            invalid.push(raw_name.to_string());
                        }
                    }
                    ParamSelector::Label(ref label) => {
                        if let Some(param) = lookup_param(&state.parameters, base_name) {
                            let mut found = false;
                            for (ver, labels) in &param.labels {
                                if labels.contains(label) {
                                    if *ver == param.version {
                                        parameters.push(param_to_json(
                                            param,
                                            true,
                                            with_decryption,
                                            &req.region,
                                        ));
                                    } else if let Some(hist) =
                                        param.history.iter().find(|h| h.version == *ver)
                                    {
                                        let mut v = json!({
                                            "Name": param.name,
                                            "Type": hist.param_type,
                                            "Version": hist.version,
                                            "ARN": rewrite_arn_region(&param.arn, &req.region),
                                            "LastModifiedDate": hist.last_modified.timestamp_millis() as f64 / 1000.0,
                                            "DataType": param.data_type,
                                        });
                                        v["Value"] = json!(hist.value);
                                        parameters.push(v);
                                    }
                                    found = true;
                                    break;
                                }
                            }
                            if !found {
                                invalid.push(raw_name.to_string());
                            }
                        } else {
                            invalid.push(raw_name.to_string());
                        }
                    }
                }
            }
        }

        Ok(json_resp(json!({
            "Parameters": parameters,
            "InvalidParameters": invalid,
        })))
    }

    fn get_parameters_by_path(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let path = body["Path"].as_str().ok_or_else(|| missing("Path"))?;
        let recursive = body["Recursive"].as_bool().unwrap_or(false);
        let with_decryption = body["WithDecryption"].as_bool().unwrap_or(false);
        let filters = body["ParameterFilters"].as_array().cloned();
        let max_results = body["MaxResults"].as_i64().unwrap_or(10) as usize;
        let next_token_offset: usize = body["NextToken"]
            .as_str()
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);

        // Validate MaxResults
        if max_results > 10 {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                format!(
                    "1 validation error detected: \
                     Value {} at 'maxResults' failed to satisfy constraint: \
                     Member must have value less than or equal to 10",
                    max_results
                ),
            ));
        }

        // Validate path
        if !is_valid_param_path(path) {
            return Err(invalid_path_error(path));
        }

        // Validate ParameterFilters for by-path (only Type, KeyId, Label, tag:* allowed)
        if let Some(ref f) = filters {
            validate_parameter_filters_by_path(f)?;
        }

        let state = self.state.read();
        let prefix = if path.ends_with('/') {
            path.to_string()
        } else {
            format!("{path}/")
        };

        let is_root = path == "/";
        let targets_aws = path.starts_with("/aws/") || path.starts_with("/aws");

        let all_params: Vec<&SsmParameter> = state
            .parameters
            .values()
            .filter(|p| {
                // Exclude /aws/ prefix params unless path explicitly targets them
                if !targets_aws && p.name.starts_with("/aws/") {
                    return false;
                }
                true
            })
            .filter(|p| {
                if is_root {
                    if recursive {
                        true
                    } else {
                        // Root level: params without '/' or with exactly one leading '/'
                        // and no further '/' in the name
                        if p.name.starts_with('/') {
                            // e.g., "/foo" is root-level, "/foo/bar" is not
                            !p.name[1..].contains('/')
                        } else {
                            // Non-path params like "foo" are at root
                            !p.name.contains('/')
                        }
                    }
                } else {
                    if p.name.starts_with(&prefix) {
                        if recursive {
                            true
                        } else {
                            !p.name[prefix.len()..].contains('/')
                        }
                    } else {
                        false
                    }
                }
            })
            .filter(|p| apply_parameter_filters(p, filters.as_ref()))
            .collect();

        let page = if next_token_offset < all_params.len() {
            &all_params[next_token_offset..]
        } else {
            &[]
        };
        let has_more = page.len() >= max_results;
        let parameters: Vec<Value> = page
            .iter()
            .take(max_results)
            .map(|p| param_to_json(p, true, with_decryption, &req.region))
            .collect();

        let mut resp = json!({ "Parameters": parameters });
        if has_more {
            resp["NextToken"] = json!((next_token_offset + max_results).to_string());
        }

        Ok(json_resp(resp))
    }

    fn delete_parameter(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let name = body["Name"].as_str().ok_or_else(|| missing("Name"))?;

        let mut state = self.state.write();
        if remove_param(&mut state.parameters, name).is_none() {
            return Err(param_not_found(name));
        }

        Ok(json_resp(json!({})))
    }

    fn delete_parameters(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let names = body["Names"].as_array().ok_or_else(|| missing("Names"))?;

        let mut state = self.state.write();
        let mut deleted = Vec::new();
        let mut invalid = Vec::new();

        for name_val in names {
            if let Some(name) = name_val.as_str() {
                if remove_param(&mut state.parameters, name).is_some() {
                    deleted.push(name.to_string());
                } else {
                    invalid.push(name.to_string());
                }
            }
        }

        Ok(json_resp(json!({
            "DeletedParameters": deleted,
            "InvalidParameters": invalid,
        })))
    }

    fn describe_parameters(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let param_filters = body["ParameterFilters"].as_array().cloned();
        let old_filters = body["Filters"].as_array().cloned();
        let max_results = body["MaxResults"].as_i64().unwrap_or(10) as usize;
        let next_token_offset: usize = body["NextToken"]
            .as_str()
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);

        // Can't use both Filters and ParameterFilters
        if param_filters.is_some() && old_filters.is_some() {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                "You can use either Filters or ParameterFilters in a single request.",
            ));
        }

        // Validate ParameterFilters
        if let Some(ref filters) = param_filters {
            validate_parameter_filters(filters)?;
        }

        let state = self.state.read();

        // Check if any filter explicitly targets /aws/ prefix paths
        let targets_aws_prefix = param_filters.as_ref().is_some_and(|filters| {
            filters.iter().any(|f| {
                let key = f["Key"].as_str().unwrap_or("");
                if key == "Path" {
                    f["Values"].as_array().is_some_and(|vals| {
                        vals.iter()
                            .any(|v| v.as_str().is_some_and(|s| s.starts_with("/aws")))
                    })
                } else if key == "Name" {
                    f["Values"].as_array().is_some_and(|vals| {
                        vals.iter().any(|v| {
                            v.as_str().is_some_and(|s| {
                                let n = s.strip_prefix('/').unwrap_or(s);
                                n.starts_with("aws/") || n.starts_with("aws")
                            })
                        })
                    })
                } else {
                    false
                }
            })
        });

        let all_params: Vec<&SsmParameter> = state
            .parameters
            .values()
            .filter(|p| {
                // Exclude /aws/ prefix params from user queries unless explicitly targeted
                if !targets_aws_prefix && p.name.starts_with("/aws/") {
                    return false;
                }
                true
            })
            .filter(|p| apply_parameter_filters(p, param_filters.as_ref()))
            .filter(|p| apply_old_filters(p, old_filters.as_ref()))
            .collect();

        let page = if next_token_offset < all_params.len() {
            &all_params[next_token_offset..]
        } else {
            &[]
        };
        let has_more = page.len() >= max_results;
        let parameters: Vec<Value> = page
            .iter()
            .take(max_results)
            .map(|p| param_to_describe_json(p, &req.region))
            .collect();

        let mut resp = json!({ "Parameters": parameters });
        if has_more {
            resp["NextToken"] = json!((next_token_offset + max_results).to_string());
        }

        Ok(json_resp(resp))
    }

    fn get_parameter_history(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let name = body["Name"].as_str().ok_or_else(|| missing("Name"))?;
        let with_decryption = body["WithDecryption"].as_bool().unwrap_or(false);
        let max_results = body["MaxResults"].as_i64();
        let next_token_offset: usize = body["NextToken"]
            .as_str()
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);

        // Validate MaxResults
        if let Some(mr) = max_results {
            if mr > 50 {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ValidationException",
                    format!(
                        "1 validation error detected: Value '{mr}' at 'maxResults' \
                         failed to satisfy constraint: Member must have value less than \
                         or equal to 50."
                    ),
                ));
            }
        }
        let max_results = max_results.unwrap_or(50) as usize;

        let state = self.state.read();
        let param = state
            .parameters
            .get(name)
            .ok_or_else(|| param_not_found(name))?;

        let mut all_history: Vec<Value> = param
            .history
            .iter()
            .map(|h| {
                let value = if h.param_type == "SecureString" && !with_decryption {
                    let kid = h.key_id.as_deref().unwrap_or("alias/aws/ssm");
                    format!("kms:{}:{}", kid, h.value)
                } else {
                    h.value.clone()
                };
                let mut entry = json!({
                    "Name": param.name,
                    "Value": value,
                    "Version": h.version,
                    "LastModifiedDate": h.last_modified.timestamp_millis() as f64 / 1000.0,
                    "Type": h.param_type,
                });
                if let Some(desc) = &h.description {
                    entry["Description"] = json!(desc);
                }
                if let Some(kid) = &h.key_id {
                    entry["KeyId"] = json!(kid);
                }
                let labels = param.labels.get(&h.version).cloned().unwrap_or_default();
                entry["Labels"] = json!(labels);
                entry
            })
            .collect();

        // Include current version
        let current_value = if param.param_type == "SecureString" && !with_decryption {
            let kid = param.key_id.as_deref().unwrap_or("alias/aws/ssm");
            format!("kms:{}:{}", kid, param.value)
        } else {
            param.value.clone()
        };
        let mut current = json!({
            "Name": param.name,
            "Value": current_value,
            "Version": param.version,
            "LastModifiedDate": param.last_modified.timestamp_millis() as f64 / 1000.0,
            "Type": param.param_type,
        });
        if let Some(desc) = &param.description {
            current["Description"] = json!(desc);
        }
        if let Some(kid) = &param.key_id {
            current["KeyId"] = json!(kid);
        }
        let current_labels = param
            .labels
            .get(&param.version)
            .cloned()
            .unwrap_or_default();
        current["Labels"] = json!(current_labels);
        all_history.push(current);

        let page = if next_token_offset < all_history.len() {
            &all_history[next_token_offset..]
        } else {
            &[]
        };
        let has_more = page.len() >= max_results;
        let result: Vec<Value> = page.iter().take(max_results).cloned().collect();

        let mut resp = json!({ "Parameters": result });
        if has_more {
            resp["NextToken"] = json!((next_token_offset + max_results).to_string());
        }

        Ok(json_resp(resp))
    }

    fn add_tags_to_resource(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let resource_type = body["ResourceType"].as_str().unwrap_or("Parameter");
        let resource_id = body["ResourceId"]
            .as_str()
            .ok_or_else(|| missing("ResourceId"))?;
        let tags = body["Tags"].as_array().ok_or_else(|| missing("Tags"))?;

        let mut state = self.state.write();

        match resource_type {
            "Parameter" => {
                let param = lookup_param_mut(&mut state.parameters, resource_id)
                    .ok_or_else(|| invalid_resource_id(resource_id))?;

                for tag in tags {
                    if let (Some(key), Some(val)) = (tag["Key"].as_str(), tag["Value"].as_str()) {
                        param.tags.insert(key.to_string(), val.to_string());
                    }
                }
            }
            "Document" => {
                let doc = state
                    .documents
                    .get_mut(resource_id)
                    .ok_or_else(|| invalid_resource_id(resource_id))?;

                for tag in tags {
                    if let (Some(key), Some(val)) = (tag["Key"].as_str(), tag["Value"].as_str()) {
                        doc.tags.insert(key.to_string(), val.to_string());
                    }
                }
            }
            "MaintenanceWindow" => {
                let mw = state
                    .maintenance_windows
                    .get_mut(resource_id)
                    .ok_or_else(|| invalid_resource_id(resource_id))?;

                for tag in tags {
                    if let (Some(key), Some(val)) = (tag["Key"].as_str(), tag["Value"].as_str()) {
                        mw.tags.insert(key.to_string(), val.to_string());
                    }
                }
            }
            "PatchBaseline" => {
                let pb = state
                    .patch_baselines
                    .get_mut(resource_id)
                    .ok_or_else(|| invalid_resource_id(resource_id))?;

                for tag in tags {
                    if let (Some(key), Some(val)) = (tag["Key"].as_str(), tag["Value"].as_str()) {
                        pb.tags.insert(key.to_string(), val.to_string());
                    }
                }
            }
            _ => {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidResourceType",
                    format!(
                        "{resource_type} is not a valid resource type. \
                         Valid resource types are: ManagedInstance, MaintenanceWindow, \
                         Parameter, PatchBaseline, OpsItem, Document."
                    ),
                ));
            }
        }

        Ok(json_resp(json!({})))
    }

    fn remove_tags_from_resource(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let resource_type = body["ResourceType"].as_str().unwrap_or("Parameter");
        let resource_id = body["ResourceId"]
            .as_str()
            .ok_or_else(|| missing("ResourceId"))?;
        let tag_keys = body["TagKeys"]
            .as_array()
            .ok_or_else(|| missing("TagKeys"))?;

        let mut state = self.state.write();

        match resource_type {
            "Parameter" => {
                let param = lookup_param_mut(&mut state.parameters, resource_id)
                    .ok_or_else(|| invalid_resource_id(resource_id))?;

                for key in tag_keys {
                    if let Some(k) = key.as_str() {
                        param.tags.remove(k);
                    }
                }
            }
            "Document" => {
                let doc = state
                    .documents
                    .get_mut(resource_id)
                    .ok_or_else(|| invalid_resource_id(resource_id))?;

                for key in tag_keys {
                    if let Some(k) = key.as_str() {
                        doc.tags.remove(k);
                    }
                }
            }
            "MaintenanceWindow" => {
                let mw = state
                    .maintenance_windows
                    .get_mut(resource_id)
                    .ok_or_else(|| invalid_resource_id(resource_id))?;

                for key in tag_keys {
                    if let Some(k) = key.as_str() {
                        mw.tags.remove(k);
                    }
                }
            }
            "PatchBaseline" => {
                let pb = state
                    .patch_baselines
                    .get_mut(resource_id)
                    .ok_or_else(|| invalid_resource_id(resource_id))?;

                for key in tag_keys {
                    if let Some(k) = key.as_str() {
                        pb.tags.remove(k);
                    }
                }
            }
            _ => {}
        }

        Ok(json_resp(json!({})))
    }

    fn list_tags_for_resource(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let resource_type = body["ResourceType"].as_str().unwrap_or("Parameter");
        let resource_id = body["ResourceId"]
            .as_str()
            .ok_or_else(|| missing("ResourceId"))?;

        let state = self.state.read();

        let tags: Vec<Value> = match resource_type {
            "Parameter" => {
                let param = lookup_param(&state.parameters, resource_id)
                    .ok_or_else(|| invalid_resource_id(resource_id))?;
                param
                    .tags
                    .iter()
                    .map(|(k, v)| json!({"Key": k, "Value": v}))
                    .collect()
            }
            "Document" => {
                let doc = state
                    .documents
                    .get(resource_id)
                    .ok_or_else(|| invalid_resource_id(resource_id))?;
                doc.tags
                    .iter()
                    .map(|(k, v)| json!({"Key": k, "Value": v}))
                    .collect()
            }
            "MaintenanceWindow" => {
                let mw = state
                    .maintenance_windows
                    .get(resource_id)
                    .ok_or_else(|| invalid_resource_id(resource_id))?;
                mw.tags
                    .iter()
                    .map(|(k, v)| json!({"Key": k, "Value": v}))
                    .collect()
            }
            "PatchBaseline" => {
                let pb = state
                    .patch_baselines
                    .get(resource_id)
                    .ok_or_else(|| invalid_resource_id(resource_id))?;
                pb.tags
                    .iter()
                    .map(|(k, v)| json!({"Key": k, "Value": v}))
                    .collect()
            }
            _ => {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidResourceType",
                    format!(
                        "{resource_type} is not a valid resource type. \
                         Valid resource types are: ManagedInstance, MaintenanceWindow, \
                         Parameter, PatchBaseline, OpsItem, Document."
                    ),
                ));
            }
        };

        let mut tags = tags;
        tags.sort_by(|a, b| {
            let ka = a["Key"].as_str().unwrap_or("");
            let kb = b["Key"].as_str().unwrap_or("");
            ka.cmp(kb)
        });

        Ok(json_resp(json!({ "TagList": tags })))
    }

    fn label_parameter_version(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let name = body["Name"].as_str().ok_or_else(|| missing("Name"))?;
        let labels = body["Labels"].as_array().ok_or_else(|| missing("Labels"))?;
        let version = body["ParameterVersion"].as_i64();

        let mut state = self.state.write();
        let param =
            lookup_param_mut(&mut state.parameters, name).ok_or_else(|| param_not_found(name))?;

        let target_version = version.unwrap_or(param.version);

        // Validate version exists
        let version_exists = param.version == target_version
            || param.history.iter().any(|h| h.version == target_version);
        if !version_exists {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ParameterVersionNotFound",
                format!(
                    "Systems Manager could not find version {target_version} of {name}. \
                     Verify the version and try again."
                ),
            ));
        }

        let label_strings: Vec<String> = labels
            .iter()
            .filter_map(|l| l.as_str().map(|s| s.to_string()))
            .collect();

        // Validate label length (max 100)
        for label in &label_strings {
            if label.len() > 100 {
                let labels_display: Vec<&str> = label_strings.iter().map(|s| s.as_str()).collect();
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ValidationException",
                    format!(
                        "1 validation error detected: \
                         Value '[{}]' at 'labels' failed to satisfy constraint: \
                         Member must satisfy constraint: \
                         [Member must have length less than or equal to 100, Member must \
                         have length greater than or equal to 1]",
                        labels_display.join(", ")
                    ),
                ));
            }
        }

        // Validate invalid labels (aws/ssm prefix, starts with digit, contains / or :)
        let mut invalid_labels = Vec::new();
        for label in &label_strings {
            let lower = label.to_lowercase();
            let is_invalid = lower.starts_with("aws")
                || lower.starts_with("ssm")
                || label.starts_with(|c: char| c.is_ascii_digit())
                || label.contains('/')
                || label.contains(':');
            if is_invalid {
                invalid_labels.push(label.clone());
            }
        }
        if !invalid_labels.is_empty() {
            return Ok(json_resp(json!({
                "InvalidLabels": invalid_labels,
                "ParameterVersion": target_version,
            })));
        }

        // Count current labels for target version
        let current_count = param
            .labels
            .get(&target_version)
            .map(|l| l.len())
            .unwrap_or(0);
        let new_unique: Vec<&String> = label_strings
            .iter()
            .filter(|l| {
                !param
                    .labels
                    .get(&target_version)
                    .is_some_and(|existing| existing.contains(l))
            })
            .collect();

        if current_count + new_unique.len() > 10 {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ParameterVersionLabelLimitExceeded",
                "An error occurred (ParameterVersionLabelLimitExceeded) when \
                 calling the LabelParameterVersion operation: \
                 A parameter version can have maximum 10 labels.\
                 Move one or more labels to another version and try again.",
            ));
        }

        // Remove these labels from any other version (labels are unique across versions)
        for existing_labels in param.labels.values_mut() {
            existing_labels.retain(|l| !label_strings.contains(l));
        }
        // Remove empty entries
        param.labels.retain(|_, v| !v.is_empty());

        // Add labels to target version
        let entry = param.labels.entry(target_version).or_default();
        for label in &label_strings {
            if !entry.contains(label) {
                entry.push(label.clone());
            }
        }

        Ok(json_resp(json!({
            "InvalidLabels": [],
            "ParameterVersion": target_version,
        })))
    }

    fn unlabel_parameter_version(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let name = body["Name"].as_str().ok_or_else(|| missing("Name"))?;
        let labels = body["Labels"].as_array().ok_or_else(|| missing("Labels"))?;
        let version = body["ParameterVersion"]
            .as_i64()
            .ok_or_else(|| missing("ParameterVersion"))?;

        let mut state = self.state.write();
        let param =
            lookup_param_mut(&mut state.parameters, name).ok_or_else(|| param_not_found(name))?;

        // Validate version exists
        let version_exists =
            param.version == version || param.history.iter().any(|h| h.version == version);
        if !version_exists {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ParameterVersionNotFound",
                format!(
                    "Systems Manager could not find version {version} of {name}. \
                     Verify the version and try again."
                ),
            ));
        }

        let label_strings: Vec<String> = labels
            .iter()
            .filter_map(|l| l.as_str().map(|s| s.to_string()))
            .collect();

        // Find which labels don't exist on this version
        let invalid: Vec<String> = if let Some(existing) = param.labels.get(&version) {
            label_strings
                .iter()
                .filter(|l| !existing.contains(l))
                .cloned()
                .collect()
        } else {
            label_strings.clone()
        };

        // Remove labels
        if let Some(existing) = param.labels.get_mut(&version) {
            existing.retain(|l| !label_strings.contains(l));
        }

        // Clean up empty entries
        param.labels.retain(|_, v| !v.is_empty());

        Ok(json_resp(json!({
            "InvalidLabels": invalid,
            "RemovedLabels": label_strings.iter().filter(|l| !invalid.contains(l)).collect::<Vec<_>>(),
        })))
    }

    // ===== Document operations =====

    fn create_document(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
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

        let tags: HashMap<String, String> = body["Tags"]
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

        let mut state = self.state.write();

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
            permissions: HashMap::new(),
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

        Ok(json_resp(json!({ "DocumentDescription": desc_json })))
    }

    fn get_document(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let name = body["Name"].as_str().ok_or_else(|| missing("Name"))?;
        let version = body["DocumentVersion"].as_str();
        let version_name = body["VersionName"].as_str();
        let requested_format = body["DocumentFormat"].as_str();

        let state = self.state.read();
        let doc = state
            .documents
            .get(name)
            .ok_or_else(|| doc_not_found(name))?;

        // Find the target version
        let ver = if let Some(vn) = version_name {
            // Lookup by VersionName (and optionally DocumentVersion)
            let candidates: Vec<&_> = doc
                .versions
                .iter()
                .filter(|v| v.version_name.as_deref() == Some(vn))
                .collect();
            if let Some(doc_ver) = version {
                // Both VersionName and DocumentVersion specified - must match
                candidates
                    .into_iter()
                    .find(|v| v.document_version == doc_ver)
                    .ok_or_else(|| doc_not_found(name))?
            } else {
                candidates
                    .first()
                    .copied()
                    .ok_or_else(|| doc_not_found(name))?
            }
        } else if let Some(doc_ver) = version {
            let target = if doc_ver == "$LATEST" {
                &doc.latest_version
            } else {
                doc_ver
            };
            doc.versions
                .iter()
                .find(|v| v.document_version == target)
                .ok_or_else(|| doc_not_found(name))?
        } else {
            doc.versions
                .iter()
                .find(|v| v.document_version == doc.default_version)
                .ok_or_else(|| doc_not_found(name))?
        };

        // Convert content format if requested
        let (content, format) = if let Some(fmt) = requested_format {
            let converted = convert_document_content(&ver.content, &ver.document_format, fmt);
            (converted, fmt.to_string())
        } else {
            // If stored as YAML but no explicit format requested, return as JSON
            let converted = convert_document_content(&ver.content, &ver.document_format, "JSON");
            (converted, "JSON".to_string())
        };

        let mut resp = json!({
            "Name": doc.name,
            "Content": content,
            "DocumentType": doc.document_type,
            "DocumentFormat": format,
            "DocumentVersion": ver.document_version,
            "Status": ver.status,
            "CreatedDate": ver.created_date.timestamp_millis() as f64 / 1000.0,
        });

        if let Some(ref vn) = ver.version_name {
            resp["VersionName"] = json!(vn);
        }

        Ok(json_resp(resp))
    }

    fn delete_document(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let name = body["Name"].as_str().ok_or_else(|| missing("Name"))?;
        let doc_version = body["DocumentVersion"].as_str();
        let version_name = body["VersionName"].as_str();

        let mut state = self.state.write();

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

        Ok(json_resp(json!({})))
    }

    fn update_document(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let name = body["Name"].as_str().ok_or_else(|| missing("Name"))?;
        let content = body["Content"]
            .as_str()
            .ok_or_else(|| missing("Content"))?
            .to_string();
        let version_name = body["VersionName"].as_str().map(|s| s.to_string());
        let doc_format = body["DocumentFormat"].as_str();
        let target_version = body["DocumentVersion"].as_str();

        let mut state = self.state.write();
        let doc = state
            .documents
            .get_mut(name)
            .ok_or_else(|| doc_not_found(name))?;

        // Validate target version exists (if specified and not $LATEST)
        if let Some(ver) = target_version {
            if ver != "$LATEST" && !doc.versions.iter().any(|v| v.document_version == ver) {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidDocument",
                    "The document version is not valid or does not exist.",
                ));
            }
        }

        // Check for duplicate content
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

        // Check for duplicate version name
        if let Some(ref vn) = version_name {
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
        }

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

        Ok(json_resp(json!({ "DocumentDescription": desc_json })))
    }

    fn describe_document(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let name = body["Name"].as_str().ok_or_else(|| missing("Name"))?;

        let state = self.state.read();
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

        Ok(json_resp(json!({ "Document": desc_json })))
    }

    fn update_document_default_version(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let name = body["Name"].as_str().ok_or_else(|| missing("Name"))?;
        let version = body["DocumentVersion"]
            .as_str()
            .ok_or_else(|| missing("DocumentVersion"))?;

        let mut state = self.state.write();
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

        Ok(json_resp(json!({ "Description": desc })))
    }

    fn list_documents(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let max_results = body["MaxResults"].as_i64().unwrap_or(10) as usize;
        let next_token_offset: usize = body["NextToken"]
            .as_str()
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);
        let filters = body["Filters"].as_array();

        let state = self.state.read();
        let all_docs: Vec<Value> = state
            .documents
            .values()
            .filter(|doc| {
                if let Some(filters) = filters {
                    for filter in filters {
                        let key = filter["Key"].as_str().unwrap_or("");
                        let values: Vec<&str> = filter["Values"]
                            .as_array()
                            .map(|a| a.iter().filter_map(|v| v.as_str()).collect())
                            .unwrap_or_default();
                        match key {
                            "Owner" => {
                                // "Self" means owned by current account
                                if values.contains(&"Self") && doc.owner != state.account_id {
                                    return false;
                                }
                            }
                            "TargetType" => {
                                if let Some(tt) = &doc.target_type {
                                    if !values.contains(&tt.as_str()) {
                                        return false;
                                    }
                                } else {
                                    return false;
                                }
                            }
                            "Name" => {
                                if !values.contains(&doc.name.as_str()) {
                                    return false;
                                }
                            }
                            _ => {}
                        }
                    }
                }
                true
            })
            .map(|doc| {
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
            })
            .collect();

        let page = if next_token_offset < all_docs.len() {
            &all_docs[next_token_offset..]
        } else {
            &[]
        };
        let has_more = page.len() > max_results;
        let result: Vec<Value> = page.iter().take(max_results).cloned().collect();

        let mut resp = json!({ "DocumentIdentifiers": result });
        if has_more {
            resp["NextToken"] = json!((next_token_offset + max_results).to_string());
        } else {
            resp["NextToken"] = json!("");
        }

        Ok(json_resp(resp))
    }

    fn describe_document_permission(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let name = body["Name"].as_str().ok_or_else(|| missing("Name"))?;

        let state = self.state.read();
        let doc = state.documents.get(name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidDocument",
                "The specified document does not exist.".to_string(),
            )
        })?;

        let account_ids = doc.permissions.get("Share").cloned().unwrap_or_default();

        Ok(json_resp(json!({
            "AccountIds": account_ids,
            "AccountSharingInfoList": account_ids.iter().map(|id| json!({
                "AccountId": id,
                "SharedDocumentVersion": "$DEFAULT"
            })).collect::<Vec<_>>(),
        })))
    }

    fn modify_document_permission(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let name = body["Name"].as_str().ok_or_else(|| missing("Name"))?;
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
                    "ValidationException",
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
                            "ValidationException",
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

        let mut state = self.state.write();
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

        Ok(json_resp(json!({})))
    }

    // ===== Command operations =====

    fn send_command(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let document_name = body["DocumentName"]
            .as_str()
            .ok_or_else(|| missing("DocumentName"))?
            .to_string();
        let instance_ids: Vec<String> = body["InstanceIds"]
            .as_array()
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                    .collect()
            })
            .unwrap_or_default();
        let targets: Vec<Value> = body["Targets"].as_array().cloned().unwrap_or_default();
        let parameters: HashMap<String, Vec<String>> = body["Parameters"]
            .as_object()
            .map(|obj| {
                obj.iter()
                    .map(|(k, v)| {
                        let vals = v
                            .as_array()
                            .map(|a| {
                                a.iter()
                                    .filter_map(|x| x.as_str().map(|s| s.to_string()))
                                    .collect()
                            })
                            .unwrap_or_default();
                        (k.clone(), vals)
                    })
                    .collect()
            })
            .unwrap_or_default();
        let comment = body["Comment"].as_str().map(|s| s.to_string());
        let output_s3_bucket = body["OutputS3BucketName"].as_str().map(|s| s.to_string());
        let output_s3_prefix = body["OutputS3KeyPrefix"].as_str().map(|s| s.to_string());
        let output_s3_region = body["OutputS3Region"].as_str().map(|s| s.to_string());
        let timeout = body["TimeoutSeconds"].as_i64();
        let max_concurrency = body["MaxConcurrency"].as_str().map(|s| s.to_string());
        let max_errors = body["MaxErrors"].as_str().map(|s| s.to_string());
        let service_role = body["ServiceRoleArn"].as_str().map(|s| s.to_string());
        let notification = body.get("NotificationConfig").cloned();

        let command_id = uuid::Uuid::new_v4().to_string();
        let now = Utc::now();

        // Resolve targets to instance IDs (for tag-based targets, just use the instance_ids)
        let effective_instance_ids = if instance_ids.is_empty() && !targets.is_empty() {
            // For tag-based targets, we'll simulate with some dummy instance IDs
            vec!["i-placeholder".to_string()]
        } else {
            instance_ids.clone()
        };

        let cmd = SsmCommand {
            command_id: command_id.clone(),
            document_name: document_name.clone(),
            instance_ids: effective_instance_ids.clone(),
            parameters: parameters.clone(),
            status: "Success".to_string(),
            requested_date_time: now,
            comment: comment.clone(),
            output_s3_bucket_name: output_s3_bucket.clone(),
            output_s3_key_prefix: output_s3_prefix.clone(),
            timeout_seconds: timeout,
            service_role_arn: service_role.clone(),
            notification_config: notification.clone(),
            targets: targets.clone(),
        };

        let mut state = self.state.write();
        state.commands.push(cmd);

        let expires = now + chrono::Duration::seconds(timeout.unwrap_or(3600));
        let cmd_json = json!({
            "Command": {
                "CommandId": command_id,
                "DocumentName": document_name,
                "InstanceIds": effective_instance_ids,
                "Targets": targets,
                "Parameters": parameters,
                "Status": "Success",
                "StatusDetails": "Details placeholder",
                "RequestedDateTime": now.timestamp_millis() as f64 / 1000.0,
                "ExpiresAfter": expires.timestamp_millis() as f64 / 1000.0,
                "Comment": comment,
                "OutputS3Region": output_s3_region,
                "OutputS3BucketName": output_s3_bucket,
                "OutputS3KeyPrefix": output_s3_prefix,
                "ServiceRoleArn": service_role,
                "TimeoutSeconds": timeout,
                "MaxConcurrency": max_concurrency.unwrap_or_default(),
                "MaxErrors": max_errors.unwrap_or_default(),
                "DeliveryTimedOutCount": 0,
            }
        });

        Ok(json_resp(cmd_json))
    }

    fn list_commands(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let command_id = body["CommandId"].as_str();
        let instance_id = body["InstanceId"].as_str();

        let state = self.state.read();
        let commands: Vec<Value> = state
            .commands
            .iter()
            .filter(|c| {
                if let Some(cid) = command_id {
                    if c.command_id != cid {
                        return false;
                    }
                }
                if let Some(iid) = instance_id {
                    if !c.instance_ids.contains(&iid.to_string()) {
                        return false;
                    }
                }
                true
            })
            .map(|c| {
                let expires = c.requested_date_time
                    + chrono::Duration::seconds(c.timeout_seconds.unwrap_or(3600));
                json!({
                    "CommandId": c.command_id,
                    "DocumentName": c.document_name,
                    "InstanceIds": c.instance_ids,
                    "Targets": c.targets,
                    "Parameters": c.parameters,
                    "Status": c.status,
                    "StatusDetails": "Details placeholder",
                    "RequestedDateTime": c.requested_date_time.timestamp_millis() as f64 / 1000.0,
                    "ExpiresAfter": expires.timestamp_millis() as f64 / 1000.0,
                    "Comment": c.comment,
                    "OutputS3Region": c.output_s3_bucket_name,
                    "OutputS3BucketName": c.output_s3_bucket_name,
                    "OutputS3KeyPrefix": c.output_s3_key_prefix,
                    "DeliveryTimedOutCount": 0,
                })
            })
            .collect();

        // If a specific CommandId was requested and not found, return an error
        if let Some(cid) = command_id {
            if commands.is_empty() {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidCommandId",
                    format!("Command with id {cid} does not exist."),
                ));
            }
        }

        Ok(json_resp(json!({ "Commands": commands })))
    }

    fn get_command_invocation(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let command_id = body["CommandId"]
            .as_str()
            .ok_or_else(|| missing("CommandId"))?;
        let instance_id = body["InstanceId"]
            .as_str()
            .ok_or_else(|| missing("InstanceId"))?;
        let plugin_name = body["PluginName"].as_str();

        let state = self.state.read();
        let cmd = state
            .commands
            .iter()
            .find(|c| c.command_id == command_id)
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvocationDoesNotExist",
                    format!("Command {command_id} not found"),
                )
            })?;

        // Check instance is part of the command
        if !cmd.instance_ids.contains(&instance_id.to_string()) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvocationDoesNotExist",
                "An error occurred (InvocationDoesNotExist) when calling the GetCommandInvocation operation",
            ));
        }

        // Validate plugin name if provided
        if let Some(pn) = plugin_name {
            let known_plugins = ["aws:runShellScript", "aws:runPowerShellScript"];
            if !known_plugins.contains(&pn) {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvocationDoesNotExist",
                    "An error occurred (InvocationDoesNotExist) when calling the GetCommandInvocation operation",
                ));
            }
        }

        Ok(json_resp(json!({
            "CommandId": cmd.command_id,
            "InstanceId": instance_id,
            "DocumentName": cmd.document_name,
            "Status": "Success",
            "StatusDetails": "Success",
            "ResponseCode": 0,
            "StandardOutputContent": "",
            "StandardOutputUrl": "",
            "StandardErrorContent": "",
            "StandardErrorUrl": "",
        })))
    }

    fn list_command_invocations(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let command_id = body["CommandId"].as_str();

        let state = self.state.read();
        let invocations: Vec<Value> = state
            .commands
            .iter()
            .filter(|c| {
                if let Some(cid) = command_id {
                    c.command_id == cid
                } else {
                    true
                }
            })
            .flat_map(|c| {
                c.instance_ids.iter().map(|iid| {
                    json!({
                        "CommandId": c.command_id,
                        "InstanceId": iid,
                        "DocumentName": c.document_name,
                        "Status": "Success",
                        "StatusDetails": "Success",
                        "RequestedDateTime": c.requested_date_time.timestamp_millis() as f64 / 1000.0,
                        "Comment": c.comment,
                    })
                })
            })
            .collect();

        Ok(json_resp(json!({ "CommandInvocations": invocations })))
    }

    fn cancel_command(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let command_id = body["CommandId"]
            .as_str()
            .ok_or_else(|| missing("CommandId"))?;

        let mut state = self.state.write();
        if let Some(cmd) = state
            .commands
            .iter_mut()
            .find(|c| c.command_id == command_id)
        {
            cmd.status = "Cancelled".to_string();
        }

        Ok(json_resp(json!({})))
    }

    // ===== Maintenance Window operations =====

    fn create_maintenance_window(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let name = body["Name"]
            .as_str()
            .ok_or_else(|| missing("Name"))?
            .to_string();
        let schedule = body["Schedule"]
            .as_str()
            .ok_or_else(|| missing("Schedule"))?
            .to_string();
        let duration = body["Duration"]
            .as_i64()
            .ok_or_else(|| missing("Duration"))?;
        let cutoff = body["Cutoff"].as_i64().ok_or_else(|| missing("Cutoff"))?;
        let allow_unassociated_targets =
            body["AllowUnassociatedTargets"].as_bool().unwrap_or(false);
        let description = body["Description"].as_str().map(|s| s.to_string());
        let schedule_timezone = body["ScheduleTimezone"].as_str().map(|s| s.to_string());
        let schedule_offset = body["ScheduleOffset"].as_i64();
        let start_date = body["StartDate"].as_str().map(|s| s.to_string());
        let end_date = body["EndDate"].as_str().map(|s| s.to_string());

        let tags: HashMap<String, String> = body["Tags"]
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

        let window_id = format!("mw-{}", &uuid::Uuid::new_v4().to_string()[..17]);

        let mw = MaintenanceWindow {
            id: window_id.clone(),
            name,
            schedule,
            duration,
            cutoff,
            allow_unassociated_targets,
            enabled: true,
            description,
            tags,
            targets: Vec::new(),
            tasks: Vec::new(),
            schedule_timezone,
            schedule_offset,
            start_date,
            end_date,
        };

        let mut state = self.state.write();
        state.maintenance_windows.insert(window_id.clone(), mw);

        Ok(json_resp(json!({ "WindowId": window_id })))
    }

    fn describe_maintenance_windows(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let filters = body["Filters"].as_array();

        let state = self.state.read();
        let windows: Vec<Value> = state
            .maintenance_windows
            .values()
            .filter(|mw| {
                if let Some(filters) = filters {
                    for filter in filters {
                        let key = filter["Key"].as_str().unwrap_or("");
                        let values: Vec<&str> = filter["Values"]
                            .as_array()
                            .map(|a| a.iter().filter_map(|v| v.as_str()).collect())
                            .unwrap_or_default();
                        match key {
                            "Name" => {
                                if !values.iter().any(|v| *v == mw.name) {
                                    return false;
                                }
                            }
                            "Enabled" => {
                                let enabled_str = if mw.enabled { "true" } else { "false" };
                                if !values.contains(&enabled_str) {
                                    return false;
                                }
                            }
                            _ => {}
                        }
                    }
                }
                true
            })
            .map(|mw| {
                let mut v = json!({
                    "WindowId": mw.id,
                    "Name": mw.name,
                    "Schedule": mw.schedule,
                    "Duration": mw.duration,
                    "Cutoff": mw.cutoff,
                    "AllowUnassociatedTargets": mw.allow_unassociated_targets,
                    "Enabled": mw.enabled,
                });
                if let Some(ref desc) = mw.description {
                    v["Description"] = json!(desc);
                }
                if let Some(ref tz) = mw.schedule_timezone {
                    v["ScheduleTimezone"] = json!(tz);
                }
                if let Some(offset) = mw.schedule_offset {
                    v["ScheduleOffset"] = json!(offset);
                }
                if let Some(ref sd) = mw.start_date {
                    v["StartDate"] = json!(sd);
                }
                if let Some(ref ed) = mw.end_date {
                    v["EndDate"] = json!(ed);
                }
                v
            })
            .collect();

        Ok(json_resp(json!({ "WindowIdentities": windows })))
    }

    fn get_maintenance_window(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let window_id = body["WindowId"]
            .as_str()
            .ok_or_else(|| missing("WindowId"))?;

        let state = self.state.read();
        let mw = state
            .maintenance_windows
            .get(window_id)
            .ok_or_else(|| mw_not_found(window_id))?;

        let mut resp = json!({
            "WindowId": mw.id,
            "Name": mw.name,
            "Schedule": mw.schedule,
            "Duration": mw.duration,
            "Cutoff": mw.cutoff,
            "AllowUnassociatedTargets": mw.allow_unassociated_targets,
            "Enabled": mw.enabled,
        });
        if let Some(ref desc) = mw.description {
            resp["Description"] = json!(desc);
        }
        if let Some(ref tz) = mw.schedule_timezone {
            resp["ScheduleTimezone"] = json!(tz);
        }
        if let Some(offset) = mw.schedule_offset {
            resp["ScheduleOffset"] = json!(offset);
        }
        if let Some(ref sd) = mw.start_date {
            resp["StartDate"] = json!(sd);
        }
        if let Some(ref ed) = mw.end_date {
            resp["EndDate"] = json!(ed);
        }

        Ok(json_resp(resp))
    }

    fn delete_maintenance_window(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let window_id = body["WindowId"]
            .as_str()
            .ok_or_else(|| missing("WindowId"))?;

        let mut state = self.state.write();
        if state.maintenance_windows.remove(window_id).is_none() {
            return Err(mw_not_found(window_id));
        }

        Ok(json_resp(json!({ "WindowId": window_id })))
    }

    fn update_maintenance_window(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let window_id = body["WindowId"]
            .as_str()
            .ok_or_else(|| missing("WindowId"))?;

        let mut state = self.state.write();
        let mw = state
            .maintenance_windows
            .get_mut(window_id)
            .ok_or_else(|| mw_not_found(window_id))?;

        if let Some(name) = body["Name"].as_str() {
            mw.name = name.to_string();
        }
        if let Some(schedule) = body["Schedule"].as_str() {
            mw.schedule = schedule.to_string();
        }
        if let Some(duration) = body["Duration"].as_i64() {
            mw.duration = duration;
        }
        if let Some(cutoff) = body["Cutoff"].as_i64() {
            mw.cutoff = cutoff;
        }
        if let Some(enabled) = body["Enabled"].as_bool() {
            mw.enabled = enabled;
        }
        if let Some(allow) = body["AllowUnassociatedTargets"].as_bool() {
            mw.allow_unassociated_targets = allow;
        }
        if body.get("Description").is_some() {
            mw.description = body["Description"].as_str().map(|s| s.to_string());
        }

        let mut resp = json!({
            "WindowId": mw.id,
            "Name": mw.name,
            "Schedule": mw.schedule,
            "Duration": mw.duration,
            "Cutoff": mw.cutoff,
            "AllowUnassociatedTargets": mw.allow_unassociated_targets,
            "Enabled": mw.enabled,
        });
        if let Some(ref desc) = mw.description {
            resp["Description"] = json!(desc);
        }

        Ok(json_resp(resp))
    }

    fn register_target_with_maintenance_window(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let window_id = body["WindowId"]
            .as_str()
            .ok_or_else(|| missing("WindowId"))?;
        let resource_type = body["ResourceType"]
            .as_str()
            .ok_or_else(|| missing("ResourceType"))?
            .to_string();
        let targets = body["Targets"]
            .as_array()
            .cloned()
            .ok_or_else(|| missing("Targets"))?;
        let name = body["Name"].as_str().map(|s| s.to_string());
        let description = body["Description"].as_str().map(|s| s.to_string());
        let owner_information = body["OwnerInformation"].as_str().map(|s| s.to_string());

        let target_id = format!(
            "{}-{}",
            window_id,
            &uuid::Uuid::new_v4().to_string().replace('-', "")
        );

        let mut state = self.state.write();
        let mw = state
            .maintenance_windows
            .get_mut(window_id)
            .ok_or_else(|| mw_not_found(window_id))?;

        let target = MaintenanceWindowTarget {
            window_target_id: target_id.clone(),
            window_id: window_id.to_string(),
            resource_type,
            targets,
            name,
            description,
            owner_information,
        };
        mw.targets.push(target);

        Ok(json_resp(json!({ "WindowTargetId": target_id })))
    }

    fn deregister_target_from_maintenance_window(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let window_id = body["WindowId"]
            .as_str()
            .ok_or_else(|| missing("WindowId"))?;
        let target_id = body["WindowTargetId"]
            .as_str()
            .ok_or_else(|| missing("WindowTargetId"))?;

        let mut state = self.state.write();
        let mw = state
            .maintenance_windows
            .get_mut(window_id)
            .ok_or_else(|| mw_not_found(window_id))?;

        mw.targets.retain(|t| t.window_target_id != target_id);

        Ok(json_resp(json!({
            "WindowId": window_id,
            "WindowTargetId": target_id,
        })))
    }

    fn describe_maintenance_window_targets(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let window_id = body["WindowId"]
            .as_str()
            .ok_or_else(|| missing("WindowId"))?;

        let state = self.state.read();
        let mw = state
            .maintenance_windows
            .get(window_id)
            .ok_or_else(|| mw_not_found(window_id))?;

        let targets: Vec<Value> = mw
            .targets
            .iter()
            .map(|t| {
                let mut v = json!({
                    "WindowId": t.window_id,
                    "WindowTargetId": t.window_target_id,
                    "ResourceType": t.resource_type,
                    "Targets": t.targets,
                });
                if let Some(ref name) = t.name {
                    v["Name"] = json!(name);
                }
                if let Some(ref desc) = t.description {
                    v["Description"] = json!(desc);
                }
                if let Some(ref oi) = t.owner_information {
                    v["OwnerInformation"] = json!(oi);
                }
                v
            })
            .collect();

        Ok(json_resp(json!({ "Targets": targets })))
    }

    fn register_task_with_maintenance_window(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let window_id = body["WindowId"]
            .as_str()
            .ok_or_else(|| missing("WindowId"))?;
        let task_arn = body["TaskArn"]
            .as_str()
            .ok_or_else(|| missing("TaskArn"))?
            .to_string();
        let task_type = body["TaskType"]
            .as_str()
            .ok_or_else(|| missing("TaskType"))?
            .to_string();
        let targets = body["Targets"].as_array().cloned().unwrap_or_default();
        let max_concurrency = body["MaxConcurrency"].as_str().map(|s| s.to_string());
        let max_errors = body["MaxErrors"].as_str().map(|s| s.to_string());
        let priority = body["Priority"].as_i64().unwrap_or(1);
        let service_role_arn = body["ServiceRoleArn"].as_str().map(|s| s.to_string());
        let name = body["Name"].as_str().map(|s| s.to_string());
        let description = body["Description"].as_str().map(|s| s.to_string());

        let task_id = format!(
            "{}-{}",
            window_id,
            &uuid::Uuid::new_v4().to_string().replace('-', "")
        );

        let mut state = self.state.write();
        let mw = state
            .maintenance_windows
            .get_mut(window_id)
            .ok_or_else(|| mw_not_found(window_id))?;

        let task = MaintenanceWindowTask {
            window_task_id: task_id.clone(),
            window_id: window_id.to_string(),
            task_arn,
            task_type,
            targets,
            max_concurrency,
            max_errors,
            priority,
            service_role_arn,
            name,
            description,
        };
        mw.tasks.push(task);

        Ok(json_resp(json!({ "WindowTaskId": task_id })))
    }

    fn deregister_task_from_maintenance_window(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let window_id = body["WindowId"]
            .as_str()
            .ok_or_else(|| missing("WindowId"))?;
        let task_id = body["WindowTaskId"]
            .as_str()
            .ok_or_else(|| missing("WindowTaskId"))?;

        let mut state = self.state.write();
        let mw = state
            .maintenance_windows
            .get_mut(window_id)
            .ok_or_else(|| mw_not_found(window_id))?;

        mw.tasks.retain(|t| t.window_task_id != task_id);

        Ok(json_resp(json!({
            "WindowId": window_id,
            "WindowTaskId": task_id,
        })))
    }

    fn describe_maintenance_window_tasks(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let window_id = body["WindowId"]
            .as_str()
            .ok_or_else(|| missing("WindowId"))?;

        let state = self.state.read();
        let mw = state
            .maintenance_windows
            .get(window_id)
            .ok_or_else(|| mw_not_found(window_id))?;

        let tasks: Vec<Value> = mw
            .tasks
            .iter()
            .map(|t| {
                let mut v = json!({
                    "WindowId": t.window_id,
                    "WindowTaskId": t.window_task_id,
                    "TaskArn": t.task_arn,
                    "Type": t.task_type,
                    "Targets": t.targets,
                    "Priority": t.priority,
                });
                if let Some(ref mc) = t.max_concurrency {
                    v["MaxConcurrency"] = json!(mc);
                }
                if let Some(ref me) = t.max_errors {
                    v["MaxErrors"] = json!(me);
                }
                if let Some(ref sr) = t.service_role_arn {
                    v["ServiceRoleArn"] = json!(sr);
                }
                if let Some(ref name) = t.name {
                    v["Name"] = json!(name);
                }
                if let Some(ref desc) = t.description {
                    v["Description"] = json!(desc);
                }
                v
            })
            .collect();

        Ok(json_resp(json!({ "Tasks": tasks })))
    }

    // ===== Patch Baseline operations =====

    fn create_patch_baseline(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let name = body["Name"]
            .as_str()
            .ok_or_else(|| missing("Name"))?
            .to_string();
        let operating_system = body["OperatingSystem"]
            .as_str()
            .unwrap_or("WINDOWS")
            .to_string();
        let description = body["Description"].as_str().map(|s| s.to_string());
        let approval_rules = body.get("ApprovalRules").cloned();
        let approved_patches: Vec<String> = body["ApprovedPatches"]
            .as_array()
            .map(|a| {
                a.iter()
                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                    .collect()
            })
            .unwrap_or_default();
        let rejected_patches: Vec<String> = body["RejectedPatches"]
            .as_array()
            .map(|a| {
                a.iter()
                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                    .collect()
            })
            .unwrap_or_default();
        let approved_patches_compliance_level = body["ApprovedPatchesComplianceLevel"]
            .as_str()
            .unwrap_or("UNSPECIFIED")
            .to_string();
        let rejected_patches_action = body["RejectedPatchesAction"]
            .as_str()
            .unwrap_or("ALLOW_AS_DEPENDENCY")
            .to_string();
        let global_filters = body.get("GlobalFilters").cloned();
        let sources: Vec<Value> = body["Sources"].as_array().cloned().unwrap_or_default();
        let approved_patches_enable_non_security = body["ApprovedPatchesEnableNonSecurity"]
            .as_bool()
            .unwrap_or(false);
        let tags: HashMap<String, String> = body["Tags"]
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

        let baseline_id = format!(
            "pb-{}",
            &uuid::Uuid::new_v4().to_string().replace('-', "")[..17]
        );

        let pb = PatchBaseline {
            id: baseline_id.clone(),
            name,
            operating_system,
            description,
            approval_rules,
            approved_patches,
            rejected_patches,
            tags,
            approved_patches_compliance_level,
            rejected_patches_action,
            global_filters,
            sources,
            approved_patches_enable_non_security,
        };

        let mut state = self.state.write();
        state.patch_baselines.insert(baseline_id.clone(), pb);

        Ok(json_resp(json!({ "BaselineId": baseline_id })))
    }

    fn delete_patch_baseline(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let baseline_id = body["BaselineId"]
            .as_str()
            .ok_or_else(|| missing("BaselineId"))?;

        let mut state = self.state.write();
        state.patch_baselines.remove(baseline_id);
        // Also remove any patch group associations
        state
            .patch_groups
            .retain(|pg| pg.baseline_id != baseline_id);

        Ok(json_resp(json!({ "BaselineId": baseline_id })))
    }

    fn describe_patch_baselines(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let filters = body["Filters"].as_array();

        let state = self.state.read();
        let baselines: Vec<Value> = state
            .patch_baselines
            .values()
            .filter(|pb| {
                if let Some(filters) = filters {
                    for filter in filters {
                        let key = filter["Key"].as_str().unwrap_or("");
                        let values: Vec<&str> = filter["Values"]
                            .as_array()
                            .map(|a| a.iter().filter_map(|v| v.as_str()).collect())
                            .unwrap_or_default();
                        match key {
                            "NAME_PREFIX" => {
                                if !values.iter().any(|v| pb.name.starts_with(v)) {
                                    return false;
                                }
                            }
                            "OWNER" => {
                                // We don't track owner, but "Self" means user-created
                                if values.contains(&"AWS") {
                                    return false;
                                }
                            }
                            "OPERATING_SYSTEM" => {
                                if !values.contains(&pb.operating_system.as_str()) {
                                    return false;
                                }
                            }
                            _ => {}
                        }
                    }
                }
                true
            })
            .map(|pb| {
                let mut v = json!({
                    "BaselineId": pb.id,
                    "BaselineName": pb.name,
                    "OperatingSystem": pb.operating_system,
                    "DefaultBaseline": false,
                });
                if let Some(ref desc) = pb.description {
                    v["BaselineDescription"] = json!(desc);
                }
                v
            })
            .collect();

        Ok(json_resp(json!({ "BaselineIdentities": baselines })))
    }

    fn get_patch_baseline(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let baseline_id = body["BaselineId"]
            .as_str()
            .ok_or_else(|| missing("BaselineId"))?;

        let state = self.state.read();
        let pb = state.patch_baselines.get(baseline_id).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "DoesNotExistException",
                format!("Baseline {baseline_id} does not exist."),
            )
        })?;

        let mut resp = json!({
            "BaselineId": pb.id,
            "Name": pb.name,
            "OperatingSystem": pb.operating_system,
            "ApprovedPatches": pb.approved_patches,
            "RejectedPatches": pb.rejected_patches,
            "ApprovedPatchesComplianceLevel": pb.approved_patches_compliance_level,
            "RejectedPatchesAction": pb.rejected_patches_action,
            "ApprovedPatchesEnableNonSecurity": pb.approved_patches_enable_non_security,
            "Sources": pb.sources,
            "PatchGroups": state.patch_groups.iter()
                .filter(|pg| pg.baseline_id == baseline_id)
                .map(|pg| pg.patch_group.clone())
                .collect::<Vec<_>>(),
        });
        if let Some(ref desc) = pb.description {
            resp["Description"] = json!(desc);
        }
        if let Some(ref rules) = pb.approval_rules {
            resp["ApprovalRules"] = rules.clone();
        }
        if let Some(ref gf) = pb.global_filters {
            resp["GlobalFilters"] = gf.clone();
        }

        Ok(json_resp(resp))
    }

    fn register_patch_baseline_for_patch_group(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let baseline_id = body["BaselineId"]
            .as_str()
            .ok_or_else(|| missing("BaselineId"))?
            .to_string();
        let patch_group = body["PatchGroup"]
            .as_str()
            .ok_or_else(|| missing("PatchGroup"))?
            .to_string();

        let mut state = self.state.write();

        // Check baseline exists (AWS returns "Maintenance window" in this error, not "Patch baseline")
        if !state.patch_baselines.contains_key(&baseline_id) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "DoesNotExistException",
                format!("Maintenance window {baseline_id} does not exist"),
            ));
        }

        // Check if this patch group is already registered to a baseline with same OS
        let os = state.patch_baselines[&baseline_id].operating_system.clone();
        if let Some(existing) = state
            .patch_groups
            .iter()
            .find(|pg| pg.patch_group == patch_group)
        {
            if let Some(existing_pb) = state.patch_baselines.get(&existing.baseline_id) {
                if existing_pb.operating_system == os {
                    return Err(AwsServiceError::aws_error(
                        StatusCode::BAD_REQUEST,
                        "AlreadyExistsException",
                        format!(
                            "Patch Group baseline already has a baseline registered for OperatingSystem {os}."
                        ),
                    ));
                }
            }
        }

        state.patch_groups.push(PatchGroup {
            baseline_id: baseline_id.clone(),
            patch_group: patch_group.clone(),
        });

        Ok(json_resp(json!({
            "BaselineId": baseline_id,
            "PatchGroup": patch_group,
        })))
    }

    fn deregister_patch_baseline_for_patch_group(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let baseline_id = body["BaselineId"]
            .as_str()
            .ok_or_else(|| missing("BaselineId"))?;
        let patch_group = body["PatchGroup"]
            .as_str()
            .ok_or_else(|| missing("PatchGroup"))?;

        let mut state = self.state.write();

        // Check if the association exists
        let exists = state
            .patch_groups
            .iter()
            .any(|pg| pg.baseline_id == baseline_id && pg.patch_group == patch_group);
        if exists {
            state
                .patch_groups
                .retain(|pg| !(pg.baseline_id == baseline_id && pg.patch_group == patch_group));
        } else {
            // Allow deregistering default baselines (they are implicitly registered)
            let is_default = is_default_patch_baseline(baseline_id);
            if !is_default {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "DoesNotExistException",
                    "Patch Baseline to be retrieved does not exist.",
                ));
            }
        }

        Ok(json_resp(json!({
            "BaselineId": baseline_id,
            "PatchGroup": patch_group,
        })))
    }

    fn get_patch_baseline_for_patch_group(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let patch_group = body["PatchGroup"]
            .as_str()
            .ok_or_else(|| missing("PatchGroup"))?;
        let operating_system = body["OperatingSystem"].as_str().unwrap_or("WINDOWS");

        let state = self.state.read();

        // Find a patch group association matching both patch group and OS
        let found = state.patch_groups.iter().find(|pg| {
            pg.patch_group == patch_group
                && state
                    .patch_baselines
                    .get(&pg.baseline_id)
                    .is_some_and(|pb| pb.operating_system == operating_system)
        });

        if let Some(pg) = found {
            Ok(json_resp(json!({
                "BaselineId": pg.baseline_id,
                "PatchGroup": pg.patch_group,
                "OperatingSystem": operating_system,
            })))
        } else {
            // Fall back to default baseline for the region/OS
            let mut resp = json!({
                "PatchGroup": patch_group,
                "OperatingSystem": operating_system,
            });
            if let Some(baseline_id) = default_patch_baseline(&req.region, operating_system) {
                resp["BaselineId"] = json!(baseline_id);
            }
            Ok(json_resp(resp))
        }
    }

    fn describe_patch_groups(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let filters = body["Filters"].as_array();

        let state = self.state.read();
        let mappings: Vec<Value> = state
            .patch_groups
            .iter()
            .filter(|pg| {
                if let Some(filters) = filters {
                    for filter in filters {
                        let key = filter["Key"].as_str().unwrap_or("");
                        let values: Vec<&str> = filter["Values"]
                            .as_array()
                            .map(|a| a.iter().filter_map(|v| v.as_str()).collect())
                            .unwrap_or_default();
                        match key {
                            "NAME_PREFIX" => {
                                if !values.iter().any(|v| pg.patch_group.starts_with(v)) {
                                    return false;
                                }
                            }
                            "OPERATING_SYSTEM" => {
                                if let Some(pb) = state.patch_baselines.get(&pg.baseline_id) {
                                    if !values.contains(&pb.operating_system.as_str()) {
                                        return false;
                                    }
                                }
                            }
                            _ => {}
                        }
                    }
                }
                true
            })
            .map(|pg| {
                let mut baseline_identity = json!({
                    "BaselineId": pg.baseline_id,
                    "DefaultBaseline": false,
                });
                if let Some(pb) = state.patch_baselines.get(&pg.baseline_id) {
                    baseline_identity["BaselineName"] = json!(pb.name);
                    baseline_identity["OperatingSystem"] = json!(pb.operating_system);
                    if let Some(ref desc) = pb.description {
                        baseline_identity["BaselineDescription"] = json!(desc);
                    }
                }
                json!({
                    "PatchGroup": pg.patch_group,
                    "BaselineIdentity": baseline_identity,
                })
            })
            .collect();

        Ok(json_resp(json!({ "Mappings": mappings })))
    }
}

/// Validate a path value for parameter path filters.
fn is_valid_param_path(path: &str) -> bool {
    if !path.starts_with('/') {
        return false;
    }
    if path == "//" {
        return false;
    }
    // Each segment between slashes must contain only letters, numbers, . - _
    let segments: Vec<&str> = path.split('/').collect();
    for seg in &segments[1..] {
        if seg.is_empty() {
            continue;
        }
        if !seg
            .chars()
            .all(|c| c.is_alphanumeric() || c == '.' || c == '-' || c == '_')
        {
            return false;
        }
    }
    true
}

/// Full invalid-path error message (matches AWS format).
fn invalid_path_error(value: &str) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        "ValidationException",
        format!(
            "The parameter doesn't meet the parameter name requirements. \
             The parameter name must begin with a forward slash \"/\". \
             It can't be prefixed with \"aws\" or \"ssm\" (case-insensitive). \
             It must use only letters, numbers, or the following symbols: . \
             (period), - (hyphen), _ (underscore). \
             Special characters are not allowed. All sub-paths, if specified, \
             must use the forward slash symbol \"/\". \
             Valid example: /get/parameters2-/by1./path0_. \
             Invalid parameter name: {value}"
        ),
    )
}

/// Validate ParameterFilters for DescribeParameters.
fn validate_parameter_filters(filters: &[Value]) -> Result<(), AwsServiceError> {
    let valid_keys = ["Path", "Name", "Type", "KeyId", "Tier"];
    let valid_key_pattern = "tag:.+|Name|Type|KeyId|Path|Label|Tier";

    // Collect structural validation errors first
    let mut errors: Vec<String> = Vec::new();

    for (i, filter) in filters.iter().enumerate() {
        let key = filter["Key"].as_str().unwrap_or("");
        let option = filter["Option"].as_str();
        let values = filter["Values"].as_array();

        // Key must match pattern
        let key_valid = valid_keys.contains(&key) || key.starts_with("tag:") || key == "Label";
        if !key_valid {
            errors.push(format!(
                "Value '{}' at 'parameterFilters.{}.key' failed to satisfy constraint: \
                 Member must satisfy regular expression pattern: {}",
                key,
                i + 1,
                valid_key_pattern
            ));
        }

        // Key length <= 132
        if key.len() > 132 {
            errors.push(format!(
                "Value '{}' at 'parameterFilters.{}.key' failed to satisfy constraint: \
                 Member must have length less than or equal to 132",
                key,
                i + 1
            ));
        }

        // Option length <= 10
        if let Some(opt) = option {
            if opt.len() > 10 {
                errors.push(format!(
                    "Value '{}' at 'parameterFilters.{}.option' failed to satisfy constraint: \
                     Member must have length less than or equal to 10",
                    opt,
                    i + 1
                ));
            }
        }

        // Values length <= 50
        if let Some(vals) = values {
            if vals.len() > 50 {
                let vals_str: Vec<&str> = vals.iter().filter_map(|v| v.as_str()).collect();
                errors.push(format!(
                    "Value '[{}]' at 'parameterFilters.{}.values' failed to satisfy constraint: \
                     Member must have length less than or equal to 50",
                    vals_str.join(", "),
                    i + 1
                ));
            }
            // Each value <= 1024
            for val in vals {
                if let Some(v) = val.as_str() {
                    if v.len() > 1024 {
                        errors.push(format!(
                            "Value '[{}]' at 'parameterFilters.{}.values' failed to satisfy constraint: \
                             Member must have length less than or equal to 1024, \
                             Member must have length greater than or equal to 1",
                            v,
                            i + 1
                        ));
                    }
                }
            }
        }
    }

    if !errors.is_empty() {
        let msg = if errors.len() == 1 {
            format!("1 validation error detected: {}", errors[0])
        } else {
            format!(
                "{} validation errors detected: {}",
                errors.len(),
                errors.join("; ")
            )
        };
        return Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "ValidationException",
            msg,
        ));
    }

    // Semantic validation (after structural validation passes)

    // Label is not valid for DescribeParameters
    for filter in filters {
        let key = filter["Key"].as_str().unwrap_or("");
        if key == "Label" {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                "The following filter key is not valid: Label. \
                 Valid filter keys include: [Path, Name, Type, KeyId, Tier]",
            ));
        }
    }

    // Check for missing values (tag: filters are allowed without values - means "tag exists")
    for filter in filters {
        let key = filter["Key"].as_str().unwrap_or("");
        let values = filter["Values"].as_array();
        if !key.starts_with("tag:") && (values.is_none() || values.is_some_and(|v| v.is_empty())) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                format!("The following filter values are missing : null for filter key {key}"),
            ));
        }
    }

    // Check for duplicate keys
    let mut seen_keys = std::collections::HashSet::new();
    for filter in filters {
        let key = filter["Key"].as_str().unwrap_or("");
        if !seen_keys.insert(key) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                format!(
                    "The following filter is duplicated in the request: {key}. \
                     A request can contain only one occurrence of a specific filter."
                ),
            ));
        }
    }

    // Validate per-key constraints
    for filter in filters {
        let key = filter["Key"].as_str().unwrap_or("");
        let option = filter["Option"].as_str();
        let values = filter["Values"].as_array();

        if key == "Path" {
            // Path option must be Recursive or OneLevel, not Equals
            if let Some(opt) = option {
                if opt != "Recursive" && opt != "OneLevel" {
                    return Err(AwsServiceError::aws_error(
                        StatusCode::BAD_REQUEST,
                        "ValidationException",
                        format!(
                            "The following filter option is not valid: {opt}. \
                             Valid options include: [Recursive, OneLevel]"
                        ),
                    ));
                }
            }

            // Path values can't start with aws or ssm
            if let Some(vals) = values {
                for val in vals {
                    if let Some(v) = val.as_str() {
                        if !is_valid_param_path(v) {
                            return Err(invalid_path_error(v));
                        }
                        let stripped = v.strip_prefix('/').unwrap_or(v);
                        let first_segment = stripped.split('/').next().unwrap_or("");
                        let lower = first_segment.to_lowercase();
                        if lower.starts_with("aws") || lower.starts_with("ssm") {
                            return Err(AwsServiceError::aws_error(
                                StatusCode::BAD_REQUEST,
                                "ValidationException",
                                "Filters for common parameters can't be prefixed with \
                                 \"aws\" or \"ssm\" (case-insensitive).",
                            ));
                        }
                    }
                }
            }
        }

        if key == "Tier" {
            if let Some(vals) = values {
                for val in vals {
                    if let Some(v) = val.as_str() {
                        if !["Standard", "Advanced", "Intelligent-Tiering"].contains(&v) {
                            return Err(AwsServiceError::aws_error(
                                StatusCode::BAD_REQUEST,
                                "ValidationException",
                                format!(
                                    "The following filter value is not valid: {v}. Valid \
                                     values include: [Standard, Advanced, Intelligent-Tiering]"
                                ),
                            ));
                        }
                    }
                }
            }
        }

        if key == "Type" {
            if let Some(vals) = values {
                for val in vals {
                    if let Some(v) = val.as_str() {
                        if !["String", "StringList", "SecureString"].contains(&v) {
                            return Err(AwsServiceError::aws_error(
                                StatusCode::BAD_REQUEST,
                                "ValidationException",
                                format!(
                                    "The following filter value is not valid: {v}. Valid \
                                     values include: [String, StringList, SecureString]"
                                ),
                            ));
                        }
                    }
                }
            }
        }

        if key == "Name" {
            if let Some(opt) = option {
                if !["BeginsWith", "Equals", "Contains"].contains(&opt) {
                    return Err(AwsServiceError::aws_error(
                        StatusCode::BAD_REQUEST,
                        "ValidationException",
                        format!(
                            "The following filter option is not valid: {opt}. Valid \
                             options include: [BeginsWith, Equals]."
                        ),
                    ));
                }
            }
        }
    }

    Ok(())
}

/// Validate ParameterFilters for GetParametersByPath (only Type, KeyId, Label, tag:* allowed).
fn validate_parameter_filters_by_path(filters: &[Value]) -> Result<(), AwsServiceError> {
    for filter in filters {
        let key = filter["Key"].as_str().unwrap_or("");
        if !["Type", "KeyId", "Label"].contains(&key) && !key.starts_with("tag:") {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                format!(
                    "The following filter key is not valid: {key}. \
                     Valid filter keys include: [Type, KeyId]."
                ),
            ));
        }
    }
    Ok(())
}

/// Apply ParameterFilters to a parameter.
fn apply_parameter_filters(param: &SsmParameter, filters: Option<&Vec<Value>>) -> bool {
    let filters = match filters {
        Some(f) => f,
        None => return true,
    };

    for filter in filters {
        let key = match filter["Key"].as_str() {
            Some(k) => k,
            None => continue,
        };
        let option = filter["Option"].as_str().unwrap_or("Equals");
        let values: Vec<&str> = filter["Values"]
            .as_array()
            .map(|arr| arr.iter().filter_map(|v| v.as_str()).collect())
            .unwrap_or_default();

        let matches = match key {
            "Name" => match option {
                "BeginsWith" => values.iter().any(|v| {
                    param.name.starts_with(v) || {
                        // Normalize: /foo matches foo, foo matches /foo
                        let normalized_v = v.strip_prefix('/').unwrap_or(v);
                        let normalized_name = param.name.strip_prefix('/').unwrap_or(&param.name);
                        normalized_name.starts_with(normalized_v)
                    }
                }),
                "Contains" => {
                    // Normalize name to always have leading /
                    let what = if param.name.starts_with('/') {
                        param.name.clone()
                    } else {
                        format!("/{}", param.name)
                    };
                    // Values NOT normalized for Contains (unlike Equals/BeginsWith)
                    values.iter().any(|v| what.contains(v))
                }
                "Equals" => values.iter().any(|v| {
                    param.name == *v || {
                        // Normalize: /foo matches foo, foo matches /foo
                        let normalized_v = v.strip_prefix('/').unwrap_or(v);
                        let normalized_name = param.name.strip_prefix('/').unwrap_or(&param.name);
                        normalized_name == normalized_v
                    }
                }),
                _ => true,
            },
            "Path" => {
                // Default option for Path is OneLevel
                let path_option = if option == "Equals" {
                    "OneLevel"
                } else {
                    option
                };
                match path_option {
                    "Recursive" => values.iter().any(|v| {
                        if *v == "/" {
                            true // All params are under root
                        } else {
                            let prefix = if v.ends_with('/') {
                                v.to_string()
                            } else {
                                format!("{v}/")
                            };
                            param.name.starts_with(&prefix)
                        }
                    }),
                    _ => values.iter().any(|v| {
                        if *v == "/" {
                            // Root level: no-slash params or single-level /params
                            if param.name.starts_with('/') {
                                !param.name[1..].contains('/')
                            } else {
                                !param.name.contains('/')
                            }
                        } else {
                            let prefix = if v.ends_with('/') {
                                v.to_string()
                            } else {
                                format!("{v}/")
                            };
                            param.name.starts_with(&prefix)
                                && !param.name[prefix.len()..].contains('/')
                        }
                    }),
                }
            }
            "Type" => {
                if values.is_empty() {
                    true
                } else {
                    match option {
                        "BeginsWith" => values.iter().any(|v| param.param_type.starts_with(v)),
                        _ => values.iter().any(|v| param.param_type == *v),
                    }
                }
            }
            "KeyId" => {
                // For SecureString params without explicit KeyId, default is alias/aws/ssm
                let effective_key_id = if param.param_type == "SecureString" {
                    Some(
                        param
                            .key_id
                            .as_deref()
                            .unwrap_or("alias/aws/ssm")
                            .to_string(),
                    )
                } else {
                    param.key_id.clone()
                };
                if values.is_empty() {
                    effective_key_id.is_some()
                } else {
                    match option {
                        "BeginsWith" => effective_key_id
                            .as_ref()
                            .is_some_and(|kid| values.iter().any(|v| kid.starts_with(v))),
                        _ => effective_key_id
                            .as_ref()
                            .is_some_and(|kid| values.contains(&kid.as_str())),
                    }
                }
            }
            "Tier" => values.iter().any(|v| param.tier == *v),
            "Label" => {
                let all_labels: Vec<&String> =
                    param.labels.values().flat_map(|v| v.iter()).collect();
                if values.is_empty() {
                    !all_labels.is_empty()
                } else {
                    match option {
                        "BeginsWith" => values
                            .iter()
                            .any(|v| all_labels.iter().any(|l| l.starts_with(v))),
                        "Contains" => values
                            .iter()
                            .any(|v| all_labels.iter().any(|l| l.contains(v))),
                        _ => values
                            .iter()
                            .any(|v| all_labels.iter().any(|l| l.as_str() == *v)),
                    }
                }
            }
            _ if key.starts_with("tag:") => {
                let tag_key = &key[4..];
                if let Some(tag_val) = param.tags.get(tag_key) {
                    if values.is_empty() {
                        true
                    } else {
                        match option {
                            "BeginsWith" => values.iter().any(|v| tag_val.starts_with(v)),
                            "Contains" => values.iter().any(|v| tag_val.contains(v)),
                            _ => values.contains(&tag_val.as_str()),
                        }
                    }
                } else {
                    false
                }
            }
            _ => true,
        };

        if !matches {
            return false;
        }
    }

    true
}

/// Apply old-style Filters (Filters key, not ParameterFilters).
fn apply_old_filters(param: &SsmParameter, filters: Option<&Vec<Value>>) -> bool {
    let filters = match filters {
        Some(f) => f,
        None => return true,
    };

    for filter in filters {
        let key = match filter["Key"].as_str() {
            Some(k) => k,
            None => continue,
        };
        let values: Vec<&str> = filter["Values"]
            .as_array()
            .map(|arr| arr.iter().filter_map(|v| v.as_str()).collect())
            .unwrap_or_default();

        let matches = match key {
            "Name" => values.iter().any(|v| param.name.contains(v)),
            "Type" => values.iter().any(|v| param.param_type == *v),
            "KeyId" => param
                .key_id
                .as_ref()
                .is_some_and(|kid| values.contains(&kid.as_str())),
            _ => true,
        };

        if !matches {
            return false;
        }
    }

    true
}

fn resolve_param_by_name_or_arn<'a>(
    state: &'a crate::state::SsmState,
    name: &str,
) -> Result<&'a SsmParameter, AwsServiceError> {
    // Direct name lookup with normalization
    if let Some(p) = lookup_param(&state.parameters, name) {
        return Ok(p);
    }

    // ARN lookup: arn:aws:ssm:REGION:ACCOUNT:parameter/NAME
    if name.starts_with("arn:aws:ssm:") {
        if let Some(param_part) = name.split(":parameter").nth(1) {
            if let Some(p) = lookup_param(&state.parameters, param_part) {
                return Ok(p);
            }
        }
    }

    Err(param_not_found(name))
}

fn missing(name: &str) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        "ValidationException",
        format!("The request must contain the parameter {name}"),
    )
}

fn param_not_found(name: &str) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        "ParameterNotFound",
        format!("Parameter {name} not found."),
    )
}

fn doc_not_found(_name: &str) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        "InvalidDocument",
        "The specified document does not exist.".to_string(),
    )
}

fn invalid_resource_id(id: &str) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        "InvalidResourceId",
        format!("The resource ID \"{id}\" is not valid. Verify the ID and try again."),
    )
}

fn mw_not_found(id: &str) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        "DoesNotExistException",
        format!("Maintenance window {id} does not exist"),
    )
}

/// Parse a document content string (JSON or YAML) and extract metadata fields.
/// Returns (description, schema_version, parameters_json).
fn extract_document_metadata(
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

/// Look up the default patch baseline for a given region and OS.
fn default_patch_baseline(region: &str, operating_system: &str) -> Option<String> {
    static DEFAULT_BASELINES: std::sync::LazyLock<Value> = std::sync::LazyLock::new(|| {
        serde_json::from_str(include_str!("default_baselines.json")).unwrap_or(json!({}))
    });
    DEFAULT_BASELINES
        .get(region)
        .and_then(|r| r.get(operating_system))
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
}

/// Check if a baseline ID is a known default baseline.
fn is_default_patch_baseline(baseline_id: &str) -> bool {
    static DEFAULT_BASELINES: std::sync::LazyLock<Value> = std::sync::LazyLock::new(|| {
        serde_json::from_str(include_str!("default_baselines.json")).unwrap_or(json!({}))
    });
    if let Some(obj) = DEFAULT_BASELINES.as_object() {
        for region_data in obj.values() {
            if let Some(region_obj) = region_data.as_object() {
                for val in region_obj.values() {
                    if val.as_str() == Some(baseline_id) {
                        return true;
                    }
                }
            }
        }
    }
    false
}

/// Serialize a JSON value with Python-style separators (`, ` and `: `).
fn json_dumps(val: &Value) -> String {
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
