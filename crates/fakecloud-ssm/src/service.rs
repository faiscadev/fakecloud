use async_trait::async_trait;
use chrono::Utc;
use http::StatusCode;
use serde_json::{json, Value};

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsService, AwsServiceError};

use crate::state::{SharedSsmState, SsmParameter, SsmParameterVersion};

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
        ]
    }
}

fn parse_body(req: &AwsRequest) -> Value {
    serde_json::from_slice(&req.body).unwrap_or(Value::Object(Default::default()))
}

fn json_resp(body: Value) -> AwsResponse {
    AwsResponse::json(StatusCode::OK, serde_json::to_string(&body).unwrap())
}

fn param_to_json(p: &SsmParameter, with_value: bool) -> Value {
    let mut v = json!({
        "Name": p.name,
        "Type": p.param_type,
        "Version": p.version,
        "ARN": p.arn,
        "LastModifiedDate": p.last_modified.timestamp() as f64,
        "DataType": "text",
    });
    if with_value {
        v["Value"] = json!(p.value);
    }
    v
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
        let param_type = body["Type"].as_str().unwrap_or("String").to_string();
        let overwrite = body["Overwrite"].as_bool().unwrap_or(false);

        let mut state = self.state.write();

        if let Some(existing) = state.parameters.get_mut(&name) {
            if !overwrite {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ParameterAlreadyExists",
                    format!("The parameter {name} already exists."),
                ));
            }
            let now = Utc::now();
            existing.history.push(SsmParameterVersion {
                value: existing.value.clone(),
                version: existing.version,
                last_modified: existing.last_modified,
            });
            existing.version += 1;
            existing.value = value;
            existing.param_type = param_type;
            existing.last_modified = now;

            return Ok(json_resp(json!({
                "Version": existing.version,
                "Tier": "Standard",
            })));
        }

        let now = Utc::now();
        let arn = format!(
            "arn:aws:ssm:{}:{}:parameter{}",
            state.region, state.account_id, name
        );

        let param = SsmParameter {
            name: name.clone(),
            value,
            param_type,
            version: 1,
            arn,
            last_modified: now,
            history: Vec::new(),
        };

        state.parameters.insert(name, param);
        Ok(json_resp(json!({
            "Version": 1,
            "Tier": "Standard",
        })))
    }

    fn get_parameter(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let name = body["Name"].as_str().ok_or_else(|| missing("Name"))?;

        let state = self.state.read();
        let param = state
            .parameters
            .get(name)
            .ok_or_else(|| param_not_found(name))?;

        Ok(json_resp(json!({
            "Parameter": param_to_json(param, true),
        })))
    }

    fn get_parameters(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let names = body["Names"].as_array().ok_or_else(|| missing("Names"))?;

        let state = self.state.read();
        let mut parameters = Vec::new();
        let mut invalid = Vec::new();

        for name_val in names {
            if let Some(name) = name_val.as_str() {
                if let Some(param) = state.parameters.get(name) {
                    parameters.push(param_to_json(param, true));
                } else {
                    invalid.push(name.to_string());
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

        let state = self.state.read();
        let prefix = if path.ends_with('/') {
            path.to_string()
        } else {
            format!("{path}/")
        };

        let parameters: Vec<Value> = state
            .parameters
            .range(prefix.clone()..)
            .take_while(|(k, _)| k.starts_with(&prefix))
            .filter(|(k, _)| {
                if recursive {
                    true
                } else {
                    // Only direct children (no more slashes after prefix)
                    !k[prefix.len()..].contains('/')
                }
            })
            .map(|(_, p)| param_to_json(p, true))
            .collect();

        Ok(json_resp(json!({ "Parameters": parameters })))
    }

    fn delete_parameter(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let name = body["Name"].as_str().ok_or_else(|| missing("Name"))?;

        let mut state = self.state.write();
        if state.parameters.remove(name).is_none() {
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
                if state.parameters.remove(name).is_some() {
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
        let state = self.state.read();
        let parameters: Vec<Value> = state
            .parameters
            .values()
            .map(|p| param_to_json(p, false))
            .collect();

        let _ = req;
        Ok(json_resp(json!({ "Parameters": parameters })))
    }

    fn get_parameter_history(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let name = body["Name"].as_str().ok_or_else(|| missing("Name"))?;

        let state = self.state.read();
        let param = state
            .parameters
            .get(name)
            .ok_or_else(|| param_not_found(name))?;

        let mut history: Vec<Value> = param
            .history
            .iter()
            .map(|h| {
                json!({
                    "Name": param.name,
                    "Value": h.value,
                    "Version": h.version,
                    "LastModifiedDate": h.last_modified.timestamp() as f64,
                    "Type": param.param_type,
                })
            })
            .collect();

        // Include current version
        history.push(json!({
            "Name": param.name,
            "Value": param.value,
            "Version": param.version,
            "LastModifiedDate": param.last_modified.timestamp() as f64,
            "Type": param.param_type,
        }));

        Ok(json_resp(json!({ "Parameters": history })))
    }
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
