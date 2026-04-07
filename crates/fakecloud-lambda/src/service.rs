use std::sync::Arc;

use async_trait::async_trait;
use chrono::Utc;
use http::{Method, StatusCode};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsService, AwsServiceError};

use crate::runtime::ContainerRuntime;
use crate::state::{EventSourceMapping, LambdaFunction, SharedLambdaState};

pub struct LambdaService {
    state: SharedLambdaState,
    runtime: Option<Arc<ContainerRuntime>>,
}

impl LambdaService {
    pub fn new(state: SharedLambdaState) -> Self {
        Self {
            state,
            runtime: None,
        }
    }

    pub fn with_runtime(mut self, runtime: Arc<ContainerRuntime>) -> Self {
        self.runtime = Some(runtime);
        self
    }

    /// Determine the action from the HTTP method and path segments.
    /// Lambda uses REST-style routing:
    ///   POST   /2015-03-31/functions                         -> CreateFunction
    ///   GET    /2015-03-31/functions                         -> ListFunctions
    ///   GET    /2015-03-31/functions/{name}                  -> GetFunction
    ///   DELETE /2015-03-31/functions/{name}                  -> DeleteFunction
    ///   POST   /2015-03-31/functions/{name}/invocations      -> Invoke
    ///   POST   /2015-03-31/functions/{name}/versions         -> PublishVersion
    ///   POST   /2015-03-31/event-source-mappings             -> CreateEventSourceMapping
    ///   GET    /2015-03-31/event-source-mappings             -> ListEventSourceMappings
    ///   GET    /2015-03-31/event-source-mappings/{uuid}      -> GetEventSourceMapping
    ///   DELETE /2015-03-31/event-source-mappings/{uuid}      -> DeleteEventSourceMapping
    fn resolve_action(req: &AwsRequest) -> Option<(&str, Option<String>)> {
        let segs = &req.path_segments;
        if segs.is_empty() {
            return None;
        }

        // Expect first segment to be "2015-03-31"
        if segs[0] != "2015-03-31" {
            return None;
        }

        match (req.method.clone(), segs.len()) {
            // /2015-03-31/functions
            (Method::POST, 2) if segs[1] == "functions" => Some(("CreateFunction", None)),
            (Method::GET, 2) if segs[1] == "functions" => Some(("ListFunctions", None)),
            // /2015-03-31/functions/{name}
            (Method::GET, 3) if segs[1] == "functions" => {
                Some(("GetFunction", Some(segs[2].clone())))
            }
            (Method::DELETE, 3) if segs[1] == "functions" => {
                Some(("DeleteFunction", Some(segs[2].clone())))
            }
            // /2015-03-31/functions/{name}/invocations
            (Method::POST, 4) if segs[1] == "functions" && segs[3] == "invocations" => {
                Some(("Invoke", Some(segs[2].clone())))
            }
            // /2015-03-31/functions/{name}/versions
            (Method::POST, 4) if segs[1] == "functions" && segs[3] == "versions" => {
                Some(("PublishVersion", Some(segs[2].clone())))
            }
            // /2015-03-31/event-source-mappings
            (Method::POST, 2) if segs[1] == "event-source-mappings" => {
                Some(("CreateEventSourceMapping", None))
            }
            (Method::GET, 2) if segs[1] == "event-source-mappings" => {
                Some(("ListEventSourceMappings", None))
            }
            // /2015-03-31/event-source-mappings/{uuid}
            (Method::GET, 3) if segs[1] == "event-source-mappings" => {
                Some(("GetEventSourceMapping", Some(segs[2].clone())))
            }
            (Method::DELETE, 3) if segs[1] == "event-source-mappings" => {
                Some(("DeleteEventSourceMapping", Some(segs[2].clone())))
            }
            _ => None,
        }
    }

    fn create_function(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = serde_json::from_slice(&req.body).unwrap_or_default();
        let function_name = body["FunctionName"]
            .as_str()
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterValueException",
                    "FunctionName is required",
                )
            })?
            .to_string();

        let mut state = self.state.write();

        if state.functions.contains_key(&function_name) {
            return Err(AwsServiceError::aws_error(
                StatusCode::CONFLICT,
                "ResourceConflictException",
                format!("Function already exist: {}", function_name),
            ));
        }

        let runtime = body["Runtime"].as_str().unwrap_or("python3.12").to_string();
        let role = body["Role"].as_str().unwrap_or("").to_string();
        let handler = body["Handler"]
            .as_str()
            .unwrap_or("index.handler")
            .to_string();
        let description = body["Description"].as_str().unwrap_or("").to_string();
        let timeout = body["Timeout"].as_i64().unwrap_or(3);
        let memory_size = body["MemorySize"].as_i64().unwrap_or(128);
        let package_type = body["PackageType"].as_str().unwrap_or("Zip").to_string();

        let tags: std::collections::HashMap<String, String> = body["Tags"]
            .as_object()
            .map(|m| {
                m.iter()
                    .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                    .collect()
            })
            .unwrap_or_default();

        let environment: std::collections::HashMap<String, String> = body["Environment"]
            ["Variables"]
            .as_object()
            .map(|m| {
                m.iter()
                    .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                    .collect()
            })
            .unwrap_or_default();

        let architectures = body["Architectures"]
            .as_array()
            .map(|a| {
                a.iter()
                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                    .collect()
            })
            .unwrap_or_else(|| vec!["x86_64".to_string()]);

        // Decode Code.ZipFile if present (base64-encoded ZIP)
        let code_zip: Option<Vec<u8>> = match body["Code"]["ZipFile"].as_str() {
            Some(b64) => Some(
                base64::Engine::decode(&base64::engine::general_purpose::STANDARD, b64).map_err(
                    |_| {
                        AwsServiceError::aws_error(
                            StatusCode::BAD_REQUEST,
                            "InvalidParameterValueException",
                            "Could not decode Code.ZipFile: invalid base64",
                        )
                    },
                )?,
            ),
            None => None,
        };

        // Compute a code hash from the actual ZIP bytes (or from the Code JSON as fallback)
        let code_fallback = serde_json::to_vec(&body["Code"]).unwrap_or_default();
        let code_bytes = code_zip.as_deref().unwrap_or(&code_fallback);
        let mut hasher = Sha256::new();
        hasher.update(code_bytes);
        let hash = hasher.finalize();
        let code_sha256 = base64::Engine::encode(&base64::engine::general_purpose::STANDARD, hash);
        let code_size = code_bytes.len() as i64;

        let function_arn = format!(
            "arn:aws:lambda:{}:{}:function:{}",
            state.region, state.account_id, function_name
        );
        let now = Utc::now();

        let func = LambdaFunction {
            function_name: function_name.clone(),
            function_arn: function_arn.clone(),
            runtime: runtime.clone(),
            role: role.clone(),
            handler: handler.clone(),
            description: description.clone(),
            timeout,
            memory_size,
            code_sha256: code_sha256.clone(),
            code_size,
            version: "$LATEST".to_string(),
            last_modified: now,
            tags,
            environment: environment.clone(),
            architectures: architectures.clone(),
            package_type: package_type.clone(),
            code_zip,
        };

        let response = self.function_config_json(&func);

        state.functions.insert(function_name, func);

        Ok(AwsResponse::json(StatusCode::CREATED, response.to_string()))
    }

    fn get_function(&self, function_name: &str) -> Result<AwsResponse, AwsServiceError> {
        let state = self.state.read();
        let func = state.functions.get(function_name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "ResourceNotFoundException",
                format!(
                    "Function not found: arn:aws:lambda:{}:{}:function:{}",
                    state.region, state.account_id, function_name
                ),
            )
        })?;

        let config = self.function_config_json(func);
        let response = json!({
            "Code": {
                "Location": format!("https://awslambda-{}-tasks.s3.{}.amazonaws.com/stub",
                    func.function_arn.split(':').nth(3).unwrap_or("us-east-1"),
                    func.function_arn.split(':').nth(3).unwrap_or("us-east-1")),
                "RepositoryType": "S3"
            },
            "Configuration": config,
            "Tags": func.tags,
        });

        Ok(AwsResponse::json(StatusCode::OK, response.to_string()))
    }

    fn delete_function(&self, function_name: &str) -> Result<AwsResponse, AwsServiceError> {
        let mut state = self.state.write();
        let region = state.region.clone();
        let account_id = state.account_id.clone();
        if state.functions.remove(function_name).is_none() {
            return Err(AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "ResourceNotFoundException",
                format!(
                    "Function not found: arn:aws:lambda:{}:{}:function:{}",
                    region, account_id, function_name
                ),
            ));
        }

        // Clean up any running container for this function
        if let Some(ref runtime) = self.runtime {
            let rt = runtime.clone();
            let name = function_name.to_string();
            tokio::spawn(async move { rt.stop_container(&name).await });
        }

        Ok(AwsResponse::json(StatusCode::NO_CONTENT, ""))
    }

    fn list_functions(&self) -> Result<AwsResponse, AwsServiceError> {
        let state = self.state.read();
        let functions: Vec<Value> = state
            .functions
            .values()
            .map(|f| self.function_config_json(f))
            .collect();

        let response = json!({
            "Functions": functions,
        });

        Ok(AwsResponse::json(StatusCode::OK, response.to_string()))
    }

    async fn invoke(
        &self,
        function_name: &str,
        payload: &[u8],
    ) -> Result<AwsResponse, AwsServiceError> {
        let func = {
            let state = self.state.read();
            state.functions.get(function_name).cloned().ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::NOT_FOUND,
                    "ResourceNotFoundException",
                    format!(
                        "Function not found: arn:aws:lambda:{}:{}:function:{}",
                        state.region, state.account_id, function_name
                    ),
                )
            })?
        };

        let runtime = self.runtime.as_ref().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "ServiceException",
                "Docker/Podman is required for Lambda execution but is not available",
            )
        })?;

        if func.code_zip.is_none() {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterValueException",
                "Function has no deployment package",
            ));
        }

        match runtime.invoke(&func, payload).await {
            Ok(response_bytes) => {
                let mut resp = AwsResponse::json(StatusCode::OK, response_bytes);
                resp.headers.insert(
                    http::header::HeaderName::from_static("x-amz-executed-version"),
                    http::header::HeaderValue::from_static("$LATEST"),
                );
                Ok(resp)
            }
            Err(e) => {
                tracing::error!(function = %function_name, error = %e, "Lambda invocation failed");
                Err(AwsServiceError::aws_error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "ServiceException",
                    format!("Lambda execution failed: {e}"),
                ))
            }
        }
    }

    fn publish_version(&self, function_name: &str) -> Result<AwsResponse, AwsServiceError> {
        let state = self.state.read();
        let func = state.functions.get(function_name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "ResourceNotFoundException",
                format!(
                    "Function not found: arn:aws:lambda:{}:{}:function:{}",
                    state.region, state.account_id, function_name
                ),
            )
        })?;

        let mut config = self.function_config_json(func);
        // Stub: always return version "1"
        config["Version"] = json!("1");
        config["FunctionArn"] = json!(format!("{}:1", func.function_arn));

        Ok(AwsResponse::json(StatusCode::CREATED, config.to_string()))
    }

    fn create_event_source_mapping(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = serde_json::from_slice(&req.body).unwrap_or_default();
        let event_source_arn = body["EventSourceArn"]
            .as_str()
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterValueException",
                    "EventSourceArn is required",
                )
            })?
            .to_string();

        let function_name = body["FunctionName"]
            .as_str()
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterValueException",
                    "FunctionName is required",
                )
            })?
            .to_string();

        let mut state = self.state.write();

        // Resolve function name to ARN
        let function_arn = if function_name.starts_with("arn:") {
            function_name.clone()
        } else {
            let func = state.functions.get(&function_name).ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::NOT_FOUND,
                    "ResourceNotFoundException",
                    format!(
                        "Function not found: arn:aws:lambda:{}:{}:function:{}",
                        state.region, state.account_id, function_name
                    ),
                )
            })?;
            func.function_arn.clone()
        };

        let batch_size = body["BatchSize"].as_i64().unwrap_or(10);
        let enabled = body["Enabled"].as_bool().unwrap_or(true);
        let mapping_uuid = uuid::Uuid::new_v4().to_string();
        let now = Utc::now();

        let mapping = EventSourceMapping {
            uuid: mapping_uuid.clone(),
            function_arn: function_arn.clone(),
            event_source_arn: event_source_arn.clone(),
            batch_size,
            enabled,
            state: if enabled {
                "Enabled".to_string()
            } else {
                "Disabled".to_string()
            },
            last_modified: now,
        };

        let response = self.event_source_mapping_json(&mapping);
        state.event_source_mappings.insert(mapping_uuid, mapping);

        Ok(AwsResponse::json(
            StatusCode::ACCEPTED,
            response.to_string(),
        ))
    }

    fn list_event_source_mappings(&self) -> Result<AwsResponse, AwsServiceError> {
        let state = self.state.read();
        let mappings: Vec<Value> = state
            .event_source_mappings
            .values()
            .map(|m| self.event_source_mapping_json(m))
            .collect();

        let response = json!({
            "EventSourceMappings": mappings,
        });

        Ok(AwsResponse::json(StatusCode::OK, response.to_string()))
    }

    fn get_event_source_mapping(&self, uuid: &str) -> Result<AwsResponse, AwsServiceError> {
        let state = self.state.read();
        let mapping = state.event_source_mappings.get(uuid).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "ResourceNotFoundException",
                format!("The resource you requested does not exist. (Service: Lambda, Status Code: 404, Request ID: {uuid})"),
            )
        })?;

        let response = self.event_source_mapping_json(mapping);
        Ok(AwsResponse::json(StatusCode::OK, response.to_string()))
    }

    fn delete_event_source_mapping(&self, uuid: &str) -> Result<AwsResponse, AwsServiceError> {
        let mut state = self.state.write();
        let mapping = state.event_source_mappings.remove(uuid).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "ResourceNotFoundException",
                format!("The resource you requested does not exist. (Service: Lambda, Status Code: 404, Request ID: {uuid})"),
            )
        })?;

        let mut response = self.event_source_mapping_json(&mapping);
        response["State"] = json!("Deleting");
        Ok(AwsResponse::json(
            StatusCode::ACCEPTED,
            response.to_string(),
        ))
    }

    fn function_config_json(&self, func: &LambdaFunction) -> Value {
        let mut env_vars = json!({});
        if !func.environment.is_empty() {
            env_vars = json!({ "Variables": func.environment });
        }

        json!({
            "FunctionName": func.function_name,
            "FunctionArn": func.function_arn,
            "Runtime": func.runtime,
            "Role": func.role,
            "Handler": func.handler,
            "Description": func.description,
            "Timeout": func.timeout,
            "MemorySize": func.memory_size,
            "CodeSha256": func.code_sha256,
            "CodeSize": func.code_size,
            "Version": func.version,
            "LastModified": func.last_modified.format("%Y-%m-%dT%H:%M:%S%.3f+0000").to_string(),
            "PackageType": func.package_type,
            "Architectures": func.architectures,
            "Environment": env_vars,
            "State": "Active",
            "LastUpdateStatus": "Successful",
            "TracingConfig": { "Mode": "PassThrough" },
            "RevisionId": uuid::Uuid::new_v4().to_string(),
        })
    }

    fn event_source_mapping_json(&self, mapping: &EventSourceMapping) -> Value {
        json!({
            "UUID": mapping.uuid,
            "FunctionArn": mapping.function_arn,
            "EventSourceArn": mapping.event_source_arn,
            "BatchSize": mapping.batch_size,
            "State": mapping.state,
            "LastModified": mapping.last_modified.timestamp_millis() as f64 / 1000.0,
        })
    }
}

#[async_trait]
impl AwsService for LambdaService {
    fn service_name(&self) -> &str {
        "lambda"
    }

    async fn handle(&self, req: AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let (action, resource_name) = Self::resolve_action(&req).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "UnknownOperationException",
                format!("Unknown operation: {} {}", req.method, req.raw_path),
            )
        })?;

        match action {
            "CreateFunction" => self.create_function(&req),
            "ListFunctions" => self.list_functions(),
            "GetFunction" => self.get_function(resource_name.as_deref().unwrap_or("")),
            "DeleteFunction" => self.delete_function(resource_name.as_deref().unwrap_or("")),
            "Invoke" => {
                self.invoke(resource_name.as_deref().unwrap_or(""), &req.body)
                    .await
            }
            "PublishVersion" => self.publish_version(resource_name.as_deref().unwrap_or("")),
            "CreateEventSourceMapping" => self.create_event_source_mapping(&req),
            "ListEventSourceMappings" => self.list_event_source_mappings(),
            "GetEventSourceMapping" => {
                self.get_event_source_mapping(resource_name.as_deref().unwrap_or(""))
            }
            "DeleteEventSourceMapping" => {
                self.delete_event_source_mapping(resource_name.as_deref().unwrap_or(""))
            }
            _ => Err(AwsServiceError::action_not_implemented("lambda", action)),
        }
    }

    fn supported_actions(&self) -> &[&str] {
        &[
            "CreateFunction",
            "GetFunction",
            "DeleteFunction",
            "ListFunctions",
            "Invoke",
            "PublishVersion",
            "CreateEventSourceMapping",
            "ListEventSourceMappings",
            "GetEventSourceMapping",
            "DeleteEventSourceMapping",
        ]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::LambdaState;
    use bytes::Bytes;
    use http::{HeaderMap, Method};
    use parking_lot::RwLock;
    use std::collections::HashMap;
    use std::sync::Arc;

    fn make_state() -> SharedLambdaState {
        Arc::new(RwLock::new(LambdaState::new("123456789012", "us-east-1")))
    }

    fn make_request(method: Method, path: &str, body: &str) -> AwsRequest {
        let path_segments: Vec<String> = path
            .split('/')
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string())
            .collect();
        AwsRequest {
            service: "lambda".to_string(),
            action: String::new(),
            region: "us-east-1".to_string(),
            account_id: "123456789012".to_string(),
            request_id: "test-request-id".to_string(),
            headers: HeaderMap::new(),
            query_params: HashMap::new(),
            body: Bytes::from(body.to_string()),
            path_segments,
            raw_path: path.to_string(),
            raw_query: String::new(),
            method,
            is_query_protocol: false,
            access_key_id: None,
        }
    }

    #[tokio::test]
    async fn test_create_and_get_function() {
        let state = make_state();
        let svc = LambdaService::new(state);

        let create_body = json!({
            "FunctionName": "my-func",
            "Runtime": "python3.12",
            "Role": "arn:aws:iam::123456789012:role/test-role",
            "Handler": "index.handler",
            "Code": { "ZipFile": "UEsFBgAAAAAAAAAAAAAAAAAAAAA=" }
        });

        let req = make_request(
            Method::POST,
            "/2015-03-31/functions",
            &create_body.to_string(),
        );
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::CREATED);

        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["FunctionName"], "my-func");
        assert_eq!(body["Runtime"], "python3.12");

        // Get
        let req = make_request(Method::GET, "/2015-03-31/functions/my-func", "");
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::OK);
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["Configuration"]["FunctionName"], "my-func");
    }

    #[tokio::test]
    async fn test_delete_function() {
        let state = make_state();
        let svc = LambdaService::new(state);

        let create_body = json!({
            "FunctionName": "to-delete",
            "Runtime": "nodejs20.x",
            "Role": "arn:aws:iam::123456789012:role/test",
            "Handler": "index.handler",
            "Code": {}
        });

        let req = make_request(
            Method::POST,
            "/2015-03-31/functions",
            &create_body.to_string(),
        );
        svc.handle(req).await.unwrap();

        let req = make_request(Method::DELETE, "/2015-03-31/functions/to-delete", "");
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::NO_CONTENT);

        // Verify deleted
        let req = make_request(Method::GET, "/2015-03-31/functions/to-delete", "");
        let resp = svc.handle(req).await;
        assert!(resp.is_err());
    }

    #[tokio::test]
    async fn test_invoke_without_runtime_returns_error() {
        let state = make_state();
        let svc = LambdaService::new(state);

        let create_body = json!({
            "FunctionName": "invoke-me",
            "Runtime": "python3.12",
            "Role": "arn:aws:iam::123456789012:role/test",
            "Handler": "index.handler",
            "Code": {}
        });

        let req = make_request(
            Method::POST,
            "/2015-03-31/functions",
            &create_body.to_string(),
        );
        svc.handle(req).await.unwrap();

        let req = make_request(
            Method::POST,
            "/2015-03-31/functions/invoke-me/invocations",
            r#"{"key": "value"}"#,
        );
        let resp = svc.handle(req).await;
        assert!(resp.is_err());
    }

    #[tokio::test]
    async fn test_invoke_nonexistent_function() {
        let state = make_state();
        let svc = LambdaService::new(state);

        let req = make_request(
            Method::POST,
            "/2015-03-31/functions/does-not-exist/invocations",
            "{}",
        );
        let resp = svc.handle(req).await;
        assert!(resp.is_err());
    }

    #[tokio::test]
    async fn test_list_functions() {
        let state = make_state();
        let svc = LambdaService::new(state);

        for name in &["func-a", "func-b"] {
            let create_body = json!({
                "FunctionName": name,
                "Runtime": "python3.12",
                "Role": "arn:aws:iam::123456789012:role/test",
                "Handler": "index.handler",
                "Code": {}
            });
            let req = make_request(
                Method::POST,
                "/2015-03-31/functions",
                &create_body.to_string(),
            );
            svc.handle(req).await.unwrap();
        }

        let req = make_request(Method::GET, "/2015-03-31/functions", "");
        let resp = svc.handle(req).await.unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["Functions"].as_array().unwrap().len(), 2);
    }

    #[tokio::test]
    async fn test_event_source_mapping() {
        let state = make_state();
        let svc = LambdaService::new(state);

        // Create function first
        let create_body = json!({
            "FunctionName": "esm-func",
            "Runtime": "python3.12",
            "Role": "arn:aws:iam::123456789012:role/test",
            "Handler": "index.handler",
            "Code": {}
        });
        let req = make_request(
            Method::POST,
            "/2015-03-31/functions",
            &create_body.to_string(),
        );
        svc.handle(req).await.unwrap();

        // Create mapping
        let mapping_body = json!({
            "FunctionName": "esm-func",
            "EventSourceArn": "arn:aws:sqs:us-east-1:123456789012:my-queue",
            "BatchSize": 5
        });
        let req = make_request(
            Method::POST,
            "/2015-03-31/event-source-mappings",
            &mapping_body.to_string(),
        );
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::ACCEPTED);
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        let uuid = body["UUID"].as_str().unwrap().to_string();

        // List mappings
        let req = make_request(Method::GET, "/2015-03-31/event-source-mappings", "");
        let resp = svc.handle(req).await.unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["EventSourceMappings"].as_array().unwrap().len(), 1);

        // Delete mapping
        let req = make_request(
            Method::DELETE,
            &format!("/2015-03-31/event-source-mappings/{uuid}"),
            "",
        );
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::ACCEPTED);
    }
}
