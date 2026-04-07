use std::collections::HashMap;

use chrono::Utc;
use http::StatusCode;
use serde_json::{json, Value};

use fakecloud_aws::arn::Arn;
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};
use fakecloud_core::validation::*;

use crate::state::{SsmParameter, SsmParameterVersion};

use super::{json_resp, missing, parse_body, SsmService, PARAMETER_VERSION_LIMIT};

impl SsmService {
    pub(super) fn put_parameter(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
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
    pub(super) fn resolve_secretsmanager_param(
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
        let current_vid = secret.current_version_id.as_ref().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ParameterNotFound",
                format!(
                    "An error occurred (ParameterNotFound) when referencing \
                     Secrets Manager: Secret {raw_name} not found.",
                ),
            )
        })?;
        let version = secret.versions.get(current_vid).ok_or_else(|| {
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

    pub(super) fn get_parameter(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
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

    pub(super) fn get_parameters(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
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
                                if hist.param_type == "SecureString" && !with_decryption {
                                    v["Value"] = json!("****");
                                } else {
                                    v["Value"] = json!(hist.value);
                                }
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
                                        if hist.param_type == "SecureString" && !with_decryption {
                                            v["Value"] = json!("****");
                                        } else {
                                            v["Value"] = json!(hist.value);
                                        }
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

    pub(super) fn get_parameters_by_path(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
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

    pub(super) fn delete_parameter(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let name = body["Name"].as_str().ok_or_else(|| missing("Name"))?;

        let mut state = self.state.write();
        if remove_param(&mut state.parameters, name).is_none() {
            return Err(param_not_found(name));
        }

        Ok(json_resp(json!({})))
    }

    pub(super) fn delete_parameters(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
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

    pub(super) fn describe_parameters(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        validate_optional_range_i64("MaxResults", body["MaxResults"].as_i64(), 1, 50)?;
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

    pub(super) fn get_parameter_history(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
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

    pub(super) fn label_parameter_version(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let name = body["Name"].as_str().ok_or_else(|| missing("Name"))?;
        let labels = body["Labels"].as_array().ok_or_else(|| missing("Labels"))?;
        let version = if body["ParameterVersion"].is_null() {
            None
        } else {
            Some(body["ParameterVersion"].as_i64().ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ValidationException",
                    "ParameterVersion must be a valid integer",
                )
            })?)
        };

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

    pub(super) fn unlabel_parameter_version(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let name = body["Name"].as_str().ok_or_else(|| missing("Name"))?;
        let labels = body["Labels"].as_array().ok_or_else(|| missing("Labels"))?;
        let version_opt = if body["ParameterVersion"].is_null() {
            None
        } else {
            Some(body["ParameterVersion"].as_i64().ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ValidationException",
                    "ParameterVersion must be a valid integer",
                )
            })?)
        };

        let mut state = self.state.write();
        let param =
            lookup_param_mut(&mut state.parameters, name).ok_or_else(|| param_not_found(name))?;

        let version = version_opt.unwrap_or(param.version);

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
}

/// Normalize a parameter name - try looking up with and without leading slash
pub(super) fn lookup_param<'a>(
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

pub(super) fn lookup_param_mut<'a>(
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

pub(super) fn remove_param(
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

pub(super) fn param_arn(region: &str, account_id: &str, name: &str) -> String {
    let resource = if name.starts_with('/') {
        format!("parameter{name}")
    } else {
        format!("parameter/{name}")
    };
    Arn::new("ssm", region, account_id, &resource).to_string()
}

/// Rewrite the region component of a parameter ARN.
pub(super) fn rewrite_arn_region(arn: &str, region: &str) -> String {
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

pub(super) fn param_to_json(
    p: &SsmParameter,
    with_value: bool,
    with_decryption: bool,
    region: &str,
) -> Value {
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
                // Not decrypted: return kms:KEY_ID:VALUE placeholder
                v["Value"] = json!(format!("kms:{}:{}", key_id, p.value));
            }
        } else {
            v["Value"] = json!(p.value);
        }
    }
    v
}

pub(super) fn param_to_describe_json(p: &SsmParameter, region: &str) -> Value {
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
pub(super) fn validate_param_name(name: &str) -> Option<AwsServiceError> {
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
pub(super) enum ParamSelector {
    None,
    Version(i64),
    Label(String),
    Invalid(String), // name with too many colons
}

pub(super) fn parse_param_selector(name: &str) -> (&str, ParamSelector) {
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

/// Validate a path value for parameter path filters.
pub(super) fn is_valid_param_path(path: &str) -> bool {
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
pub(super) fn invalid_path_error(value: &str) -> AwsServiceError {
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
pub(super) fn validate_parameter_filters(filters: &[Value]) -> Result<(), AwsServiceError> {
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

pub(super) fn validate_parameter_filters_by_path(filters: &[Value]) -> Result<(), AwsServiceError> {
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

pub(super) fn apply_parameter_filters(param: &SsmParameter, filters: Option<&Vec<Value>>) -> bool {
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

pub(super) fn apply_old_filters(param: &SsmParameter, filters: Option<&Vec<Value>>) -> bool {
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

pub(super) fn resolve_param_by_name_or_arn<'a>(
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

pub(super) fn param_not_found(name: &str) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        "ParameterNotFound",
        format!("Parameter {name} not found."),
    )
}
