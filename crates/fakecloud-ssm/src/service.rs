use std::collections::HashMap;

use async_trait::async_trait;
use chrono::Utc;
use http::StatusCode;
use serde_json::{json, Value};

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsService, AwsServiceError};

use crate::state::{
    SharedSsmState, SsmCommand, SsmDocument, SsmDocumentVersion, SsmParameter, SsmParameterVersion,
};

const PARAMETER_VERSION_LIMIT: i64 = 100;

pub struct SsmService {
    state: SharedSsmState,
}

impl SsmService {
    pub fn new(state: SharedSsmState) -> Self {
        Self { state }
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

fn param_arn(region: &str, account_id: &str, name: &str) -> String {
    if name.starts_with('/') {
        format!("arn:aws:ssm:{region}:{account_id}:parameter{name}")
    } else {
        format!("arn:aws:ssm:{region}:{account_id}:parameter/{name}")
    }
}

fn param_to_json(p: &SsmParameter, with_value: bool, with_decryption: bool) -> Value {
    let mut v = json!({
        "Name": p.name,
        "Type": p.param_type,
        "Version": p.version,
        "ARN": p.arn,
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

fn param_to_describe_json(p: &SsmParameter) -> Value {
    let mut v = json!({
        "Name": p.name,
        "Type": p.param_type,
        "Version": p.version,
        "ARN": p.arn,
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
        let arn = param_arn(&state.region, &state.account_id, &name);

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

    fn get_parameter(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let raw_name = body["Name"].as_str().ok_or_else(|| missing("Name"))?;
        let with_decryption = body["WithDecryption"].as_bool().unwrap_or(false);

        let state = self.state.read();

        // Handle ARN-style names directly (they contain many colons)
        if raw_name.starts_with("arn:aws:ssm:") {
            let param = resolve_param_by_name_or_arn(&state, raw_name)?;
            return Ok(json_resp(json!({
                "Parameter": param_to_json(param, true, with_decryption),
            })));
        }

        let (base_name, selector) = parse_param_selector(raw_name);

        // Check for invalid selectors (too many colons)
        if let ParamSelector::Invalid(n) = selector {
            return Err(param_not_found(&n));
        }

        // Try looking up by name or by ARN
        let param = resolve_param_by_name_or_arn(&state, base_name)?;

        match selector {
            ParamSelector::None => Ok(json_resp(json!({
                "Parameter": param_to_json(param, true, with_decryption),
            }))),
            ParamSelector::Version(ver) => {
                if param.version == ver {
                    return Ok(json_resp(json!({
                        "Parameter": param_to_json(param, true, with_decryption),
                    })));
                }
                // Look in history
                if let Some(hist) = param.history.iter().find(|h| h.version == ver) {
                    let mut v = json!({
                        "Name": param.name,
                        "Type": hist.param_type,
                        "Version": hist.version,
                        "ARN": param.arn,
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
                                "Parameter": param_to_json(param, true, with_decryption),
                            })));
                        }
                        if let Some(hist) = param.history.iter().find(|h| h.version == *ver) {
                            let mut v = json!({
                                "Name": param.name,
                                "Type": hist.param_type,
                                "Version": hist.version,
                                "ARN": param.arn,
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
                            parameters.push(param_to_json(param, true, with_decryption));
                        } else {
                            invalid.push(raw_name.to_string());
                        }
                    }
                    ParamSelector::Version(ver) => {
                        if let Some(param) = lookup_param(&state.parameters, base_name) {
                            if param.version == ver {
                                parameters.push(param_to_json(param, true, with_decryption));
                            } else if let Some(hist) =
                                param.history.iter().find(|h| h.version == ver)
                            {
                                let mut v = json!({
                                    "Name": param.name,
                                    "Type": hist.param_type,
                                    "Version": hist.version,
                                    "ARN": param.arn,
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
                                        ));
                                    } else if let Some(hist) =
                                        param.history.iter().find(|h| h.version == *ver)
                                    {
                                        let mut v = json!({
                                            "Name": param.name,
                                            "Type": hist.param_type,
                                            "Version": hist.version,
                                            "ARN": param.arn,
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

        let state = self.state.read();
        let prefix = if path.ends_with('/') {
            path.to_string()
        } else {
            format!("{path}/")
        };

        let is_root = path == "/";

        let all_params: Vec<&SsmParameter> = state
            .parameters
            .values()
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
            .map(|p| param_to_json(p, true, with_decryption))
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

        let state = self.state.read();
        let all_params: Vec<&SsmParameter> = state
            .parameters
            .values()
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
            .map(|p| param_to_describe_json(p))
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
                let mut entry = json!({
                    "Name": param.name,
                    "Value": h.value,
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
        let mut current = json!({
            "Name": param.name,
            "Value": param.value,
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

        // Validate invalid labels (aws/ssm prefix, starts with digit, contains /)
        let mut invalid_labels = Vec::new();
        for label in &label_strings {
            let lower = label.to_lowercase();
            let is_invalid = lower.starts_with("aws")
                || lower.starts_with("ssm")
                || label.starts_with(|c: char| c.is_ascii_digit())
                || label.contains('/');
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
                format!(
                    "A parameter version can have a maximum of 10 labels. \
                     Attempting to add {} labels to version {} of parameter {} \
                     would result in {} labels. Move one or more labels to \
                     a different version and try again.",
                    label_strings.len(),
                    target_version,
                    name,
                    current_count + new_unique.len()
                ),
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

        let now = Utc::now();
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
            content,
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

        Ok(json_resp(json!({
            "DocumentDescription": {
                "Name": name,
                "DocumentType": doc_type,
                "DocumentFormat": doc_format,
                "TargetType": target_type,
                "DocumentVersion": "1",
                "LatestVersion": "1",
                "DefaultVersion": "1",
                "Status": "Active",
                "CreatedDate": now.timestamp_millis() as f64 / 1000.0,
                "Owner": state.account_id,
                "SchemaVersion": "2.2",
                "PlatformTypes": ["Linux", "MacOS", "Windows"],
                "Hash": format!("{:x}", md5::compute(b"placeholder")),
                "HashType": "Sha256",
            }
        })))
    }

    fn get_document(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let name = body["Name"].as_str().ok_or_else(|| missing("Name"))?;
        let version = body["DocumentVersion"].as_str();

        let state = self.state.read();
        let doc = state
            .documents
            .get(name)
            .ok_or_else(|| doc_not_found(name))?;

        let target_version = version.unwrap_or(&doc.default_version);
        let ver = doc
            .versions
            .iter()
            .find(|v| v.document_version == target_version)
            .ok_or_else(|| doc_not_found(name))?;

        Ok(json_resp(json!({
            "Name": doc.name,
            "Content": ver.content,
            "DocumentType": doc.document_type,
            "DocumentFormat": ver.document_format,
            "DocumentVersion": ver.document_version,
            "VersionName": ver.version_name,
            "Status": ver.status,
            "CreatedDate": ver.created_date.timestamp_millis() as f64 / 1000.0,
        })))
    }

    fn delete_document(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let name = body["Name"].as_str().ok_or_else(|| missing("Name"))?;

        let mut state = self.state.write();
        if state.documents.remove(name).is_none() {
            return Err(doc_not_found(name));
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

        let mut state = self.state.write();
        let doc = state
            .documents
            .get_mut(name)
            .ok_or_else(|| doc_not_found(name))?;

        let new_version_num = (doc.versions.len() + 1).to_string();
        let format = doc_format.unwrap_or(&doc.document_format).to_string();

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

        Ok(json_resp(json!({
            "DocumentDescription": {
                "Name": doc.name,
                "DocumentType": doc.document_type,
                "DocumentFormat": format,
                "DocumentVersion": new_version_num,
                "LatestVersion": doc.latest_version,
                "DefaultVersion": doc.default_version,
                "Status": "Active",
                "CreatedDate": doc.created_date.timestamp_millis() as f64 / 1000.0,
                "Owner": doc.owner,
                "SchemaVersion": "2.2",
            }
        })))
    }

    fn describe_document(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let name = body["Name"].as_str().ok_or_else(|| missing("Name"))?;

        let state = self.state.read();
        let doc = state
            .documents
            .get(name)
            .ok_or_else(|| doc_not_found(name))?;

        Ok(json_resp(json!({
            "Document": {
                "Name": doc.name,
                "DocumentType": doc.document_type,
                "DocumentFormat": doc.document_format,
                "TargetType": doc.target_type,
                "DocumentVersion": doc.default_version,
                "LatestVersion": doc.latest_version,
                "DefaultVersion": doc.default_version,
                "Status": doc.status,
                "CreatedDate": doc.created_date.timestamp_millis() as f64 / 1000.0,
                "Owner": doc.owner,
                "SchemaVersion": "2.2",
                "PlatformTypes": ["Linux", "MacOS", "Windows"],
                "Hash": format!("{:x}", md5::compute(b"placeholder")),
                "HashType": "Sha256",
            }
        })))
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

        Ok(json_resp(json!({
            "Description": {
                "Name": name,
                "DefaultVersion": version,
            }
        })))
    }

    fn list_documents(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let max_results = body["MaxResults"].as_i64().unwrap_or(10) as usize;
        let next_token_offset: usize = body["NextToken"]
            .as_str()
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);

        let state = self.state.read();
        let all_docs: Vec<Value> = state
            .documents
            .values()
            .map(|doc| {
                json!({
                    "Name": doc.name,
                    "DocumentType": doc.document_type,
                    "DocumentFormat": doc.document_format,
                    "DocumentVersion": doc.default_version,
                    "Owner": doc.owner,
                    "SchemaVersion": "2.2",
                    "PlatformTypes": ["Linux", "MacOS", "Windows"],
                })
            })
            .collect();

        let page = if next_token_offset < all_docs.len() {
            &all_docs[next_token_offset..]
        } else {
            &[]
        };
        let has_more = page.len() >= max_results;
        let result: Vec<Value> = page.iter().take(max_results).cloned().collect();

        let mut resp = json!({ "DocumentIdentifiers": result });
        if has_more {
            resp["NextToken"] = json!((next_token_offset + max_results).to_string());
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
                "SharedDocumentVersion": "$Default"
            })).collect::<Vec<_>>(),
        })))
    }

    fn modify_document_permission(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let name = body["Name"].as_str().ok_or_else(|| missing("Name"))?;
        let permission_type = body["PermissionType"].as_str().unwrap_or("Share");
        let accounts_to_add = body["AccountIdsToAdd"].as_array();
        let accounts_to_remove = body["AccountIdsToRemove"].as_array();

        if permission_type != "Share" {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidPermissionType",
                format!(
                    "The permission type {permission_type} is not supported. \
                     Only Share is supported."
                ),
            ));
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
                    "You cannot specify \"all\" as well as specific account IDs".to_string(),
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
                "BeginsWith" => values.iter().any(|v| param.name.starts_with(v)),
                "Contains" => values.iter().any(|v| param.name.contains(v)),
                "Equals" => values.iter().any(|v| param.name == *v),
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
                if values.is_empty() {
                    param.key_id.is_some()
                } else {
                    param
                        .key_id
                        .as_ref()
                        .is_some_and(|kid| values.contains(&kid.as_str()))
                }
            }
            "Tier" => values.iter().any(|v| param.tier == *v),
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
