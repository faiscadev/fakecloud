//! Cloud Control API awsJson1.0 dispatch + provisioner bridge.

use std::sync::Arc;

use async_trait::async_trait;
use chrono::Utc;
use http::StatusCode;
use serde_json::{json, Value};
use tokio::sync::Mutex as AsyncMutex;

use fakecloud_cloudformation::CloudFormationService;
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsService, AwsServiceError};
use fakecloud_persistence::SnapshotStore;

use crate::patch::apply_json_patch;
use crate::persistence::save_snapshot;
use crate::state::{CloudControlState, ManagedResource, ResourceRequest, SharedCloudControlState};

/// Every operation name in the Cloud Control Smithy model.
pub const CLOUDCONTROL_ACTIONS: &[&str] = &[
    "CreateResource",
    "GetResource",
    "UpdateResource",
    "DeleteResource",
    "ListResources",
    "GetResourceRequestStatus",
    "ListResourceRequests",
    "CancelResourceRequest",
];

pub struct CloudControlService {
    cfn: Arc<CloudFormationService>,
    state: SharedCloudControlState,
    snapshot_store: Option<Arc<dyn SnapshotStore>>,
    snapshot_lock: Arc<AsyncMutex<()>>,
}

impl CloudControlService {
    pub fn new(cfn: Arc<CloudFormationService>, state: SharedCloudControlState) -> Self {
        Self {
            cfn,
            state,
            snapshot_store: None,
            snapshot_lock: Arc::new(AsyncMutex::new(())),
        }
    }

    pub fn with_snapshot_store(mut self, store: Arc<dyn SnapshotStore>) -> Self {
        self.snapshot_store = Some(store);
        self
    }

    async fn persist(&self) {
        save_snapshot(
            &self.state,
            self.snapshot_store.clone(),
            &self.snapshot_lock,
        )
        .await;
    }

    // --- CreateResource ---------------------------------------------------

    async fn create_resource(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_json(&req.body)?;
        // Enforce the input members' Smithy @length/@pattern constraints before
        // touching the provisioner.
        validate_type_name(&body)?;
        validate_len(&body, "DesiredState", 1, 262144)?;
        validate_len(&body, "ClientToken", 1, 128)?;
        validate_len(&body, "RoleArn", 20, 2048)?;
        let type_name = require_str(&body, "TypeName")?;
        let desired = require_str(&body, "DesiredState")?;
        let desired_state: Value = serde_json::from_str(desired)
            .map_err(|e| invalid_request(&format!("DesiredState is not valid JSON: {e}")))?;
        let client_token = opt_str(&body, "ClientToken");
        let fingerprint =
            fingerprint_of("CREATE", type_name, None, Some(&desired_state.to_string()));

        // ClientToken idempotency: replay the original terminal event when the
        // parameters match, reject the reuse when they differ.
        if let Some(token) = &client_token {
            if let Some(resp) =
                self.client_token_replay_or_conflict(&req.account_id, token, &fingerprint)
            {
                return resp;
            }
        }

        let request_token = new_token();
        let outcome = self.cfn.cloudcontrol_create(
            type_name,
            desired_state.clone(),
            &req.account_id,
            &req.region,
        );

        let record = match outcome {
            Ok(res) => {
                let managed = ManagedResource {
                    type_name: type_name.to_string(),
                    identifier: res.physical_id.clone(),
                    properties: desired_state.clone(),
                    attributes: res.attributes.clone(),
                    created_at: Utc::now(),
                };
                let mut accounts = self.state.write();
                let st = accounts.get_or_create(&req.account_id);
                st.resources.insert(
                    CloudControlState::resource_key(type_name, &res.physical_id),
                    managed,
                );
                success_request(
                    &request_token,
                    type_name,
                    Some(res.physical_id),
                    "CREATE",
                    Some(desired_state),
                    client_token,
                    Some(fingerprint),
                )
            }
            Err(msg) => failed_request(
                &request_token,
                type_name,
                None,
                "CREATE",
                &msg,
                client_token,
                Some(fingerprint),
            ),
        };

        self.store_request(&req.account_id, record.clone());
        if record.operation_status == "SUCCESS" {
            // Persist the backing service state the provisioner just mutated,
            // so a restart doesn't leave this resource with no owning state.
            self.cfn.cloudcontrol_persist_type(type_name).await;
        }
        self.persist().await;
        Ok(progress_event_response(&record))
    }

    // --- GetResource ------------------------------------------------------

    fn get_resource(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_json(&req.body)?;
        validate_type_name(&body)?;
        validate_len(&body, "Identifier", 1, 1024)?;
        let type_name = require_str(&body, "TypeName")?;
        let identifier = require_str(&body, "Identifier")?;
        let accounts = self.state.read();
        let managed = accounts
            .get(&req.account_id)
            .and_then(|s| {
                s.resources
                    .get(&CloudControlState::resource_key(type_name, identifier))
            })
            .ok_or_else(|| resource_not_found(type_name, identifier))?;
        Ok(AwsResponse::json_value(
            StatusCode::OK,
            json!({
                "TypeName": type_name,
                "ResourceDescription": resource_description(managed),
            }),
        ))
    }

    // --- UpdateResource ---------------------------------------------------

    async fn update_resource(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_json(&req.body)?;
        validate_type_name(&body)?;
        validate_len(&body, "Identifier", 1, 1024)?;
        validate_len(&body, "PatchDocument", 1, 262144)?;
        validate_len(&body, "ClientToken", 1, 128)?;
        validate_len(&body, "RoleArn", 20, 2048)?;
        let type_name = require_str(&body, "TypeName")?;
        let identifier = require_str(&body, "Identifier")?;
        let patch_str = require_str(&body, "PatchDocument")?;
        let patch: Value = serde_json::from_str(patch_str)
            .map_err(|e| invalid_request(&format!("PatchDocument is not valid JSON: {e}")))?;
        let client_token = opt_str(&body, "ClientToken");
        let fingerprint = fingerprint_of("UPDATE", type_name, Some(identifier), Some(patch_str));

        if let Some(token) = &client_token {
            if let Some(resp) =
                self.client_token_replay_or_conflict(&req.account_id, token, &fingerprint)
            {
                return resp;
            }
        }

        // Load current desired state + attributes.
        let key = CloudControlState::resource_key(type_name, identifier);
        let (mut properties, attributes) = {
            let accounts = self.state.read();
            let managed = accounts
                .get(&req.account_id)
                .and_then(|s| s.resources.get(&key))
                .ok_or_else(|| resource_not_found(type_name, identifier))?;
            (managed.properties.clone(), managed.attributes.clone())
        };

        apply_json_patch(&mut properties, &patch)
            .map_err(|e| invalid_request(&format!("invalid PatchDocument: {e}")))?;

        let request_token = new_token();
        let outcome = self.cfn.cloudcontrol_update(
            type_name,
            identifier,
            &attributes,
            properties.clone(),
            &req.account_id,
            &req.region,
        );

        let record = match outcome {
            Ok(res) => {
                let new_id = res.physical_id.clone();
                let mut accounts = self.state.write();
                let st = accounts.get_or_create(&req.account_id);
                if new_id != identifier {
                    // Replacement update: the provisioner assigned a new
                    // physical id. Re-key the managed resource so subsequent
                    // Get/Delete track the new identity instead of the stale one.
                    let mut managed = st.resources.remove(&key).unwrap_or(ManagedResource {
                        type_name: type_name.to_string(),
                        identifier: new_id.clone(),
                        properties: properties.clone(),
                        attributes: res.attributes.clone(),
                        created_at: Utc::now(),
                    });
                    managed.identifier = new_id.clone();
                    managed.properties = properties.clone();
                    managed.attributes = res.attributes.clone();
                    st.resources
                        .insert(CloudControlState::resource_key(type_name, &new_id), managed);
                } else if let Some(managed) = st.resources.get_mut(&key) {
                    managed.properties = properties.clone();
                    managed.attributes = res.attributes.clone();
                }
                success_request(
                    &request_token,
                    type_name,
                    Some(new_id),
                    "UPDATE",
                    Some(properties),
                    client_token,
                    Some(fingerprint),
                )
            }
            Err(msg) => failed_request(
                &request_token,
                type_name,
                Some(identifier.to_string()),
                "UPDATE",
                &msg,
                client_token,
                Some(fingerprint),
            ),
        };

        self.store_request(&req.account_id, record.clone());
        if record.operation_status == "SUCCESS" {
            self.cfn.cloudcontrol_persist_type(type_name).await;
        }
        self.persist().await;
        Ok(progress_event_response(&record))
    }

    // --- DeleteResource ---------------------------------------------------

    async fn delete_resource(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_json(&req.body)?;
        validate_type_name(&body)?;
        validate_len(&body, "Identifier", 1, 1024)?;
        validate_len(&body, "ClientToken", 1, 128)?;
        validate_len(&body, "RoleArn", 20, 2048)?;
        let type_name = require_str(&body, "TypeName")?;
        let identifier = require_str(&body, "Identifier")?;
        let client_token = opt_str(&body, "ClientToken");
        let fingerprint = fingerprint_of("DELETE", type_name, Some(identifier), None);

        if let Some(token) = &client_token {
            if let Some(resp) =
                self.client_token_replay_or_conflict(&req.account_id, token, &fingerprint)
            {
                return resp;
            }
        }

        let key = CloudControlState::resource_key(type_name, identifier);
        let attributes = {
            let accounts = self.state.read();
            let managed = accounts
                .get(&req.account_id)
                .and_then(|s| s.resources.get(&key))
                .ok_or_else(|| resource_not_found(type_name, identifier))?;
            managed.attributes.clone()
        };

        let request_token = new_token();
        let outcome = self.cfn.cloudcontrol_delete(
            type_name,
            identifier,
            &attributes,
            &req.account_id,
            &req.region,
        );

        let record = match outcome {
            Ok(()) => {
                let mut accounts = self.state.write();
                let st = accounts.get_or_create(&req.account_id);
                st.resources.remove(&key);
                success_request(
                    &request_token,
                    type_name,
                    Some(identifier.to_string()),
                    "DELETE",
                    None,
                    client_token,
                    Some(fingerprint),
                )
            }
            Err(msg) => failed_request(
                &request_token,
                type_name,
                Some(identifier.to_string()),
                "DELETE",
                &msg,
                client_token,
                Some(fingerprint),
            ),
        };

        self.store_request(&req.account_id, record.clone());
        if record.operation_status == "SUCCESS" {
            self.cfn.cloudcontrol_persist_type(type_name).await;
        }
        self.persist().await;
        Ok(progress_event_response(&record))
    }

    // --- ListResources ----------------------------------------------------

    fn list_resources(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_json(&req.body)?;
        // Enforce the input members' Smithy @length/@pattern/@range constraints.
        validate_type_name(&body)?;
        validate_len(&body, "TypeVersionId", 1, 128)?;
        validate_len(&body, "RoleArn", 20, 2048)?;
        validate_len(&body, "NextToken", 1, 4096)?;
        validate_len(&body, "ResourceModel", 1, 262144)?;
        validate_max_results(&body)?;
        let type_name = require_str(&body, "TypeName")?;
        let all: Vec<Value> = {
            let accounts = self.state.read();
            accounts
                .get(&req.account_id)
                .map(|s| {
                    s.resources
                        .values()
                        .filter(|m| m.type_name == type_name)
                        .map(resource_description)
                        .collect()
                })
                .unwrap_or_default()
        };
        let (page, next) = paginate(all, &body);
        let mut resp = json!({ "TypeName": type_name, "ResourceDescriptions": page });
        if let Some(nt) = next {
            resp["NextToken"] = json!(nt);
        }
        Ok(AwsResponse::json_value(StatusCode::OK, resp))
    }

    // --- Request ledger ops -----------------------------------------------

    fn get_request_status(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_json(&req.body)?;
        let token = require_str(&body, "RequestToken")?;
        let accounts = self.state.read();
        let record = accounts
            .get(&req.account_id)
            .and_then(|s| s.requests.get(token))
            .ok_or_else(|| request_token_not_found(token))?;
        Ok(progress_event_response(record))
    }

    fn list_requests(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_json(&req.body)?;
        validate_len(&body, "NextToken", 1, 2048)?;
        validate_max_results(&body)?;
        let filter_statuses: Vec<String> = body
            .get("ResourceRequestStatusFilter")
            .and_then(|f| f.get("OperationStatuses"))
            .and_then(|v| v.as_array())
            .map(|a| {
                a.iter()
                    .filter_map(|s| s.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();
        let filter_ops: Vec<String> = body
            .get("ResourceRequestStatusFilter")
            .and_then(|f| f.get("Operations"))
            .and_then(|v| v.as_array())
            .map(|a| {
                a.iter()
                    .filter_map(|s| s.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();
        let all: Vec<Value> = {
            let accounts = self.state.read();
            accounts
                .get(&req.account_id)
                .map(|s| {
                    s.requests
                        .values()
                        .filter(|r| {
                            filter_statuses.is_empty()
                                || filter_statuses.contains(&r.operation_status)
                        })
                        .filter(|r| filter_ops.is_empty() || filter_ops.contains(&r.operation))
                        .map(progress_event_json)
                        .collect()
                })
                .unwrap_or_default()
        };
        let (page, next) = paginate(all, &body);
        let mut resp = json!({ "ResourceRequestStatusSummaries": page });
        if let Some(nt) = next {
            resp["NextToken"] = json!(nt);
        }
        Ok(AwsResponse::json_value(StatusCode::OK, resp))
    }

    async fn cancel_request(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_json(&req.body)?;
        let token = require_str(&body, "RequestToken")?;
        // Provisioning is synchronous, so a request is already terminal by the
        // time a client could cancel it. Cancelling a terminal request is a
        // no-op that echoes its final ProgressEvent (AWS rejects cancelling an
        // already-completed request, but never invents a new state for it).
        // Mutate (if non-terminal) inside a scoped block so the write guard is
        // dropped before any await -- and capture the resulting record.
        let (record, mutated) = {
            let mut accounts = self.state.write();
            let st = accounts.get_or_create(&req.account_id);
            let r = st
                .requests
                .get_mut(token)
                .ok_or_else(|| request_token_not_found(token))?;
            let mutated = if !is_terminal(&r.operation_status) {
                r.operation_status = "CANCEL_COMPLETE".to_string();
                true
            } else {
                false
            };
            (r.clone(), mutated)
        };
        if mutated {
            self.persist().await;
        }
        Ok(progress_event_response(&record))
    }

    // --- helpers ----------------------------------------------------------

    fn store_request(&self, account_id: &str, record: ResourceRequest) {
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(account_id);
        st.requests.insert(record.request_token.clone(), record);
    }

    fn find_by_client_token(&self, account_id: &str, token: &str) -> Option<ResourceRequest> {
        let accounts = self.state.read();
        accounts.get(account_id).and_then(|s| {
            s.requests
                .values()
                .find(|r| r.client_token.as_deref() == Some(token))
                .cloned()
        })
    }

    /// Resolve a `ClientToken` against the request ledger: `Some(Ok(..))` to
    /// replay the original terminal event (same parameters), `Some(Err(..))` to
    /// reject conflicting reuse, or `None` when the token is unseen.
    fn client_token_replay_or_conflict(
        &self,
        account_id: &str,
        token: &str,
        fingerprint: &str,
    ) -> Option<Result<AwsResponse, AwsServiceError>> {
        let existing = self.find_by_client_token(account_id, token)?;
        if existing.fingerprint.as_deref() == Some(fingerprint) {
            Some(Ok(progress_event_response(&existing)))
        } else {
            Some(Err(client_token_conflict(token)))
        }
    }
}

#[async_trait]
impl AwsService for CloudControlService {
    fn service_name(&self) -> &str {
        "cloudcontrolapi"
    }

    fn supported_actions(&self) -> &[&str] {
        CLOUDCONTROL_ACTIONS
    }

    async fn handle(&self, req: AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        match req.action.as_str() {
            "CreateResource" => self.create_resource(&req).await,
            "GetResource" => self.get_resource(&req),
            "UpdateResource" => self.update_resource(&req).await,
            "DeleteResource" => self.delete_resource(&req).await,
            "ListResources" => self.list_resources(&req),
            "GetResourceRequestStatus" => self.get_request_status(&req),
            "ListResourceRequests" => self.list_requests(&req),
            "CancelResourceRequest" => self.cancel_request(&req).await,
            other => Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidRequestException",
                format!("Unknown operation: {other}"),
            )),
        }
    }
}

// ---------------------------------------------------------------------------
// Request-record + response builders
// ---------------------------------------------------------------------------

#[allow(clippy::too_many_arguments)]
fn success_request(
    token: &str,
    type_name: &str,
    identifier: Option<String>,
    operation: &str,
    resource_model: Option<Value>,
    client_token: Option<String>,
    fingerprint: Option<String>,
) -> ResourceRequest {
    ResourceRequest {
        request_token: token.to_string(),
        type_name: type_name.to_string(),
        identifier,
        operation: operation.to_string(),
        operation_status: "SUCCESS".to_string(),
        event_time: Utc::now(),
        resource_model,
        status_message: None,
        error_code: None,
        client_token,
        fingerprint,
    }
}

fn failed_request(
    token: &str,
    type_name: &str,
    identifier: Option<String>,
    operation: &str,
    message: &str,
    client_token: Option<String>,
    fingerprint: Option<String>,
) -> ResourceRequest {
    ResourceRequest {
        request_token: token.to_string(),
        type_name: type_name.to_string(),
        identifier,
        operation: operation.to_string(),
        operation_status: "FAILED".to_string(),
        event_time: Utc::now(),
        resource_model: None,
        status_message: Some(message.to_string()),
        error_code: Some("GeneralServiceException".to_string()),
        client_token,
        fingerprint,
    }
}

/// Stable fingerprint of a mutating request's parameters, used to distinguish
/// an idempotent `ClientToken` replay from a conflicting reuse.
fn fingerprint_of(
    operation: &str,
    type_name: &str,
    identifier: Option<&str>,
    payload: Option<&str>,
) -> String {
    format!(
        "{operation}\u{1f}{type_name}\u{1f}{}\u{1f}{}",
        identifier.unwrap_or(""),
        payload.unwrap_or(""),
    )
}

fn progress_event_json(record: &ResourceRequest) -> Value {
    let mut ev = json!({
        "TypeName": record.type_name,
        "RequestToken": record.request_token,
        "Operation": record.operation,
        "OperationStatus": record.operation_status,
        "EventTime": record.event_time.timestamp() as f64
            + record.event_time.timestamp_subsec_millis() as f64 / 1000.0,
    });
    if let Some(id) = &record.identifier {
        ev["Identifier"] = json!(id);
    }
    if let Some(model) = &record.resource_model {
        // ResourceModel is the Properties type: a JSON *string*.
        ev["ResourceModel"] = json!(model.to_string());
    }
    if let Some(msg) = &record.status_message {
        ev["StatusMessage"] = json!(msg);
    }
    if let Some(code) = &record.error_code {
        ev["ErrorCode"] = json!(code);
    }
    ev
}

fn progress_event_response(record: &ResourceRequest) -> AwsResponse {
    AwsResponse::json_value(
        StatusCode::OK,
        json!({ "ProgressEvent": progress_event_json(record) }),
    )
}

fn resource_description(managed: &ManagedResource) -> Value {
    json!({
        "Identifier": managed.identifier,
        // Properties is the Properties type: a JSON *string*.
        "Properties": managed.properties.to_string(),
    })
}

fn is_terminal(status: &str) -> bool {
    matches!(status, "SUCCESS" | "FAILED" | "CANCEL_COMPLETE")
}

// ---------------------------------------------------------------------------
// Parsing + errors
// ---------------------------------------------------------------------------

fn parse_json(body: &[u8]) -> Result<Value, AwsServiceError> {
    if body.is_empty() {
        return Ok(json!({}));
    }
    serde_json::from_slice(body).map_err(|e| invalid_request(&format!("invalid request body: {e}")))
}

fn require_str<'a>(body: &'a Value, field: &str) -> Result<&'a str, AwsServiceError> {
    body.get(field)
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .ok_or_else(|| invalid_request(&format!("{field} is required.")))
}

fn opt_str(body: &Value, field: &str) -> Option<String> {
    body.get(field).and_then(|v| v.as_str()).map(String::from)
}

fn new_token() -> String {
    uuid::Uuid::new_v4().to_string()
}

/// Enforce a string member's `@length` when present.
fn validate_len(body: &Value, field: &str, min: usize, max: usize) -> Result<(), AwsServiceError> {
    if let Some(s) = body.get(field).and_then(|v| v.as_str()) {
        if s.len() < min || s.len() > max {
            return Err(invalid_request(&format!(
                "Value at '{field}' failed to satisfy constraint: Member must have length between {min} and {max}."
            )));
        }
    }
    Ok(())
}

/// `TypeName` is required and constrained to the `Service::Type::Name` shape
/// (`^[A-Za-z0-9]{2,64}::...`, length 10-196).
fn validate_type_name(body: &Value) -> Result<(), AwsServiceError> {
    let name = require_str(body, "TypeName")?;
    if name.len() < 10 || name.len() > 196 || !is_valid_type_name(name) {
        return Err(invalid_request(
            "Value at 'TypeName' failed to satisfy constraint: Member must match the resource type name pattern (e.g. AWS::S3::Bucket).",
        ));
    }
    Ok(())
}

/// `^[A-Za-z0-9]{2,64}::[A-Za-z0-9]{2,64}::[A-Za-z0-9]{2,64}$`.
fn is_valid_type_name(name: &str) -> bool {
    let parts: Vec<&str> = name.split("::").collect();
    parts.len() == 3
        && parts
            .iter()
            .all(|p| (2..=64).contains(&p.len()) && p.chars().all(|c| c.is_ascii_alphanumeric()))
}

/// `MaxResults` is range 1..=100 when present.
fn validate_max_results(body: &Value) -> Result<(), AwsServiceError> {
    if let Some(v) = body.get("MaxResults") {
        let n = v
            .as_i64()
            .ok_or_else(|| invalid_request("MaxResults must be an integer."))?;
        if !(1..=100).contains(&n) {
            return Err(invalid_request("MaxResults must be between 1 and 100."));
        }
    }
    Ok(())
}

/// Slice a full result list by `MaxResults`/`NextToken`. The `NextToken` is an
/// opaque zero-based offset into the (stably ordered) full list; a continuation
/// token is returned only when more results remain.
fn paginate(items: Vec<Value>, body: &Value) -> (Vec<Value>, Option<String>) {
    let max = body
        .get("MaxResults")
        .and_then(|v| v.as_i64())
        .map(|n| n as usize)
        .unwrap_or(100);
    let start = body
        .get("NextToken")
        .and_then(|v| v.as_str())
        .and_then(|t| t.parse::<usize>().ok())
        .unwrap_or(0);
    let total = items.len();
    if start >= total {
        return (Vec::new(), None);
    }
    let end = start.saturating_add(max).min(total);
    let next = if end < total {
        Some(end.to_string())
    } else {
        None
    };
    (items[start..end].to_vec(), next)
}

fn invalid_request(msg: &str) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::BAD_REQUEST, "InvalidRequestException", msg)
}

fn resource_not_found(type_name: &str, identifier: &str) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::NOT_FOUND,
        "ResourceNotFoundException",
        format!("Resource of type '{type_name}' with identifier '{identifier}' was not found."),
    )
}

fn client_token_conflict(token: &str) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        "ClientTokenConflictException",
        format!("The client token '{token}' is already in use with different request parameters."),
    )
}

fn request_token_not_found(token: &str) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::NOT_FOUND,
        "RequestTokenNotFoundException",
        format!("Request token '{token}' was not found."),
    )
}
