//! AWS Transfer Family (`transfer`) awsJson1.1 dispatch + operation handlers.
//!
//! The full 71-operation Transfer Family control plane. State is
//! account-partitioned and persisted. Each resource is stored as its
//! already-output-valid `DescribedX` JSON object so `Describe`/`List` echo
//! exactly what `Create`/`Import`/`Update` persisted, and nested configuration
//! objects round-trip verbatim.

use std::sync::Arc;

use async_trait::async_trait;
use http::StatusCode;
use serde_json::{json, Map, Value};
use tokio::sync::Mutex as AsyncMutex;
use uuid::Uuid;

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsService, AwsServiceError};
use fakecloud_persistence::SnapshotStore;

use crate::persistence::save_snapshot;
use crate::state::{SharedTransferState, TransferData};

/// Every operation name in the Transfer Family Smithy model (71 operations).
pub const TRANSFER_ACTIONS: &[&str] = &[
    "CreateAccess",
    "CreateAgreement",
    "CreateConnector",
    "CreateProfile",
    "CreateServer",
    "CreateUser",
    "CreateWebApp",
    "CreateWorkflow",
    "DeleteAccess",
    "DeleteAgreement",
    "DeleteCertificate",
    "DeleteConnector",
    "DeleteHostKey",
    "DeleteProfile",
    "DeleteServer",
    "DeleteSshPublicKey",
    "DeleteUser",
    "DeleteWebApp",
    "DeleteWebAppCustomization",
    "DeleteWorkflow",
    "DescribeAccess",
    "DescribeAgreement",
    "DescribeCertificate",
    "DescribeConnector",
    "DescribeExecution",
    "DescribeHostKey",
    "DescribeProfile",
    "DescribeSecurityPolicy",
    "DescribeServer",
    "DescribeUser",
    "DescribeWebApp",
    "DescribeWebAppCustomization",
    "DescribeWorkflow",
    "ImportCertificate",
    "ImportHostKey",
    "ImportSshPublicKey",
    "ListAccesses",
    "ListAgreements",
    "ListCertificates",
    "ListConnectors",
    "ListExecutions",
    "ListFileTransferResults",
    "ListHostKeys",
    "ListProfiles",
    "ListSecurityPolicies",
    "ListServers",
    "ListTagsForResource",
    "ListUsers",
    "ListWebApps",
    "ListWorkflows",
    "SendWorkflowStepState",
    "StartDirectoryListing",
    "StartFileTransfer",
    "StartRemoteDelete",
    "StartRemoteMove",
    "StartServer",
    "StopServer",
    "TagResource",
    "TestConnection",
    "TestIdentityProvider",
    "UntagResource",
    "UpdateAccess",
    "UpdateAgreement",
    "UpdateCertificate",
    "UpdateConnector",
    "UpdateHostKey",
    "UpdateProfile",
    "UpdateServer",
    "UpdateUser",
    "UpdateWebApp",
    "UpdateWebAppCustomization",
];

pub struct TransferService {
    state: SharedTransferState,
    snapshot_store: Option<Arc<dyn SnapshotStore>>,
    snapshot_lock: Arc<AsyncMutex<()>>,
}

impl TransferService {
    pub fn new(state: SharedTransferState) -> Self {
        Self {
            state,
            snapshot_store: None,
            snapshot_lock: Arc::new(AsyncMutex::new(())),
        }
    }

    pub fn with_snapshot_store(mut self, store: Arc<dyn SnapshotStore>) -> Self {
        self.snapshot_store = Some(store);
        self
    }

    async fn save(&self) {
        save_snapshot(
            &self.state,
            self.snapshot_store.clone(),
            &self.snapshot_lock,
        )
        .await;
    }
}

#[async_trait]
impl AwsService for TransferService {
    fn service_name(&self) -> &str {
        "transfer"
    }

    async fn handle(&self, request: AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let mutates = is_mutating(request.action.as_str());
        let result = dispatch(self, &request);
        if mutates && matches!(result.as_ref(), Ok(resp) if resp.status.is_success()) {
            self.save().await;
        }
        result
    }

    fn supported_actions(&self) -> &[&str] {
        TRANSFER_ACTIONS
    }
}

/// Read/test operations never mutate state. Everything else (Create, Import,
/// Update, Delete, Start, Stop, Send, Tag, Untag) can, so it triggers a
/// snapshot save on success.
fn is_mutating(action: &str) -> bool {
    !(action.starts_with("Describe") || action.starts_with("List") || action.starts_with("Test"))
}

fn dispatch(s: &TransferService, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
    let b = parse(req)?;
    crate::validate::validate_input(req.action.as_str(), &b)?;
    let ctx = Ctx {
        account: req.account_id.clone(),
        region: req.region.clone(),
    };
    match req.action.as_str() {
        // servers
        "CreateServer" => s.create_server(&ctx, &b),
        "DescribeServer" => s.describe_server(&ctx, &b),
        "UpdateServer" => s.update_server(&ctx, &b),
        "DeleteServer" => s.delete_server(&ctx, &b),
        "StartServer" => s.start_server(&ctx, &b),
        "StopServer" => s.stop_server(&ctx, &b),
        "ListServers" => s.list_servers(&ctx, &b),
        // users
        "CreateUser" => s.create_user(&ctx, &b),
        "DescribeUser" => s.describe_user(&ctx, &b),
        "UpdateUser" => s.update_user(&ctx, &b),
        "DeleteUser" => s.delete_user(&ctx, &b),
        "ListUsers" => s.list_users(&ctx, &b),
        // ssh public keys
        "ImportSshPublicKey" => s.import_ssh_public_key(&ctx, &b),
        "DeleteSshPublicKey" => s.delete_ssh_public_key(&ctx, &b),
        // host keys
        "ImportHostKey" => s.import_host_key(&ctx, &b),
        "UpdateHostKey" => s.update_host_key(&ctx, &b),
        "DescribeHostKey" => s.describe_host_key(&ctx, &b),
        "DeleteHostKey" => s.delete_host_key(&ctx, &b),
        "ListHostKeys" => s.list_host_keys(&ctx, &b),
        // accesses
        "CreateAccess" => s.create_access(&ctx, &b),
        "UpdateAccess" => s.update_access(&ctx, &b),
        "DescribeAccess" => s.describe_access(&ctx, &b),
        "DeleteAccess" => s.delete_access(&ctx, &b),
        "ListAccesses" => s.list_accesses(&ctx, &b),
        // workflows
        "CreateWorkflow" => s.create_workflow(&ctx, &b),
        "DescribeWorkflow" => s.describe_workflow(&ctx, &b),
        "DeleteWorkflow" => s.delete_workflow(&ctx, &b),
        "ListWorkflows" => s.list_workflows(&ctx, &b),
        "SendWorkflowStepState" => s.send_workflow_step_state(&ctx, &b),
        // executions
        "DescribeExecution" => s.describe_execution(&ctx, &b),
        "ListExecutions" => s.list_executions(&ctx, &b),
        // agreements
        "CreateAgreement" => s.create_agreement(&ctx, &b),
        "UpdateAgreement" => s.update_agreement(&ctx, &b),
        "DescribeAgreement" => s.describe_agreement(&ctx, &b),
        "DeleteAgreement" => s.delete_agreement(&ctx, &b),
        "ListAgreements" => s.list_agreements(&ctx, &b),
        // connectors
        "CreateConnector" => s.create_connector(&ctx, &b),
        "UpdateConnector" => s.update_connector(&ctx, &b),
        "DescribeConnector" => s.describe_connector(&ctx, &b),
        "DeleteConnector" => s.delete_connector(&ctx, &b),
        "ListConnectors" => s.list_connectors(&ctx, &b),
        "TestConnection" => s.test_connection(&ctx, &b),
        "StartDirectoryListing" => s.start_directory_listing(&ctx, &b),
        "StartFileTransfer" => s.start_file_transfer(&ctx, &b),
        "StartRemoteDelete" => s.start_remote_delete(&ctx, &b),
        "StartRemoteMove" => s.start_remote_move(&ctx, &b),
        "ListFileTransferResults" => s.list_file_transfer_results(&ctx, &b),
        // profiles
        "CreateProfile" => s.create_profile(&ctx, &b),
        "UpdateProfile" => s.update_profile(&ctx, &b),
        "DescribeProfile" => s.describe_profile(&ctx, &b),
        "DeleteProfile" => s.delete_profile(&ctx, &b),
        "ListProfiles" => s.list_profiles(&ctx, &b),
        // certificates
        "ImportCertificate" => s.import_certificate(&ctx, &b),
        "UpdateCertificate" => s.update_certificate(&ctx, &b),
        "DescribeCertificate" => s.describe_certificate(&ctx, &b),
        "DeleteCertificate" => s.delete_certificate(&ctx, &b),
        "ListCertificates" => s.list_certificates(&ctx, &b),
        // security policies
        "DescribeSecurityPolicy" => s.describe_security_policy(&b),
        "ListSecurityPolicies" => s.list_security_policies(&b),
        // web apps
        "CreateWebApp" => s.create_web_app(&ctx, &b),
        "UpdateWebApp" => s.update_web_app(&ctx, &b),
        "DescribeWebApp" => s.describe_web_app(&ctx, &b),
        "DeleteWebApp" => s.delete_web_app(&ctx, &b),
        "ListWebApps" => s.list_web_apps(&ctx, &b),
        "UpdateWebAppCustomization" => s.update_web_app_customization(&ctx, &b),
        "DescribeWebAppCustomization" => s.describe_web_app_customization(&ctx, &b),
        "DeleteWebAppCustomization" => s.delete_web_app_customization(&ctx, &b),
        // identity provider
        "TestIdentityProvider" => s.test_identity_provider(&ctx, &b),
        // tagging
        "TagResource" => s.tag_resource(&ctx, &b),
        "UntagResource" => s.untag_resource(&ctx, &b),
        "ListTagsForResource" => s.list_tags_for_resource(&ctx, &b),
        _ => Err(AwsServiceError::action_not_implemented(
            s.service_name(),
            &req.action,
        )),
    }
}

// ===================== helpers =====================

/// Per-request account + region context.
struct Ctx {
    account: String,
    region: String,
}

fn ok(v: Value) -> Result<AwsResponse, AwsServiceError> {
    Ok(AwsResponse::json_value(StatusCode::OK, v))
}

fn parse(req: &AwsRequest) -> Result<Value, AwsServiceError> {
    if req.body.is_empty() {
        return Ok(json!({}));
    }
    serde_json::from_slice(&req.body)
        .map_err(|e| invalid_request(&format!("Request body is malformed: {e}")))
}

fn invalid_request(msg: &str) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::BAD_REQUEST, "InvalidRequestException", msg)
}

fn not_found(msg: &str) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::NOT_FOUND, "ResourceNotFoundException", msg)
}

fn resource_exists(msg: &str) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::BAD_REQUEST, "ResourceExistsException", msg)
}

/// Read a required, non-empty string field.
fn req_str<'a>(b: &'a Value, f: &str) -> Result<&'a str, AwsServiceError> {
    b.get(f)
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| invalid_request(&format!("{f} is required.")))
}

fn opt_str<'a>(b: &'a Value, f: &str) -> Option<&'a str> {
    b.get(f).and_then(Value::as_str).filter(|s| !s.is_empty())
}

fn now_ts() -> f64 {
    chrono::Utc::now().timestamp() as f64
}

/// 17 lowercase hex characters, matching Transfer's resource-id suffix form.
fn hex17() -> String {
    let raw = Uuid::new_v4().simple().to_string();
    raw.chars()
        .filter(|c| c.is_ascii_hexdigit())
        .take(17)
        .collect()
}

fn arn(ctx: &Ctx, path: &str) -> String {
    format!("arn:aws:transfer:{}:{}:{}", ctx.region, ctx.account, path)
}

/// Copy the given fields verbatim from `src` into `dst` when present.
fn copy_present(src: &Value, dst: &mut Map<String, Value>, fields: &[&str]) {
    for f in fields {
        if let Some(v) = src.get(*f) {
            dst.insert((*f).to_string(), v.clone());
        }
    }
}

/// Collect a resource's `Tags` input into the account tag map under its ARN.
fn store_tags(data: &mut TransferData, arn: &str, b: &Value) {
    if let Some(tags) = b.get("Tags").and_then(Value::as_array) {
        let entry = data.tags.entry(arn.to_string()).or_default();
        for t in tags {
            if let (Some(k), Some(v)) = (
                t.get("Key").and_then(Value::as_str),
                t.get("Value").and_then(Value::as_str),
            ) {
                entry.insert(k.to_string(), v.to_string());
            }
        }
    }
}

/// Return a resource's `Tags` list (from the account tag map) for embedding in
/// a `Describe` response, or `None` when the resource carries no tags.
fn tags_list(data: &TransferData, arn: &str) -> Option<Value> {
    data.tags.get(arn).filter(|m| !m.is_empty()).map(|m| {
        Value::Array(
            m.iter()
                .map(|(k, v)| json!({ "Key": k, "Value": v }))
                .collect(),
        )
    })
}

/// Paginate an ordered list of rows using Transfer's `MaxResults` +
/// `NextToken` (opaque start-index) window.
fn paginate(rows: Vec<Value>, b: &Value) -> (Vec<Value>, Option<String>) {
    let start = b
        .get("NextToken")
        .and_then(Value::as_str)
        .and_then(|t| t.parse::<usize>().ok())
        .unwrap_or(0);
    let max = b
        .get("MaxResults")
        .and_then(Value::as_u64)
        .map(|m| m.clamp(1, 1000) as usize)
        .unwrap_or(1000);
    let end = start.saturating_add(max).min(rows.len());
    let page = rows.get(start..end).unwrap_or(&[]).to_vec();
    let next = if end < rows.len() {
        Some(end.to_string())
    } else {
        None
    };
    (page, next)
}

/// Build a `{key: [...], NextToken?: ...}` list response with optional extra
/// top-level fields (e.g. the echoed `ServerId`).
fn list_response(key: &str, rows: Vec<Value>, b: &Value, extra: &[(&str, Value)]) -> Value {
    let (page, next) = paginate(rows, b);
    let mut out = Map::new();
    for (k, v) in extra {
        out.insert((*k).to_string(), v.clone());
    }
    out.insert(key.to_string(), Value::Array(page));
    if let Some(t) = next {
        out.insert("NextToken".to_string(), json!(t));
    }
    Value::Object(out)
}

// ===================== servers =====================

/// Input fields that echo verbatim into a `DescribedServer`.
const SERVER_FIELDS: &[&str] = &[
    "Certificate",
    "ProtocolDetails",
    "EndpointDetails",
    "EndpointType",
    "IdentityProviderDetails",
    "IdentityProviderType",
    "LoggingRole",
    "PostAuthenticationLoginBanner",
    "PreAuthenticationLoginBanner",
    "Protocols",
    "SecurityPolicyName",
    "WorkflowDetails",
    "StructuredLogDestinations",
    "S3StorageOptions",
    "IpAddressType",
    "Domain",
];

impl TransferService {
    fn require_server<'a>(
        &self,
        data: &'a TransferData,
        server_id: &str,
    ) -> Result<&'a Value, AwsServiceError> {
        data.servers
            .get(server_id)
            .ok_or_else(|| not_found(&format!("Server {server_id} does not exist.")))
    }

    fn create_server(&self, ctx: &Ctx, b: &Value) -> Result<AwsResponse, AwsServiceError> {
        let server_id = format!("s-{}", hex17());
        let server_arn = arn(ctx, &format!("server/{server_id}"));
        let mut srv = Map::new();
        srv.insert("Arn".into(), json!(server_arn));
        srv.insert("ServerId".into(), json!(server_id));
        srv.insert(
            "Domain".into(),
            b.get("Domain").cloned().unwrap_or(json!("S3")),
        );
        srv.insert(
            "EndpointType".into(),
            b.get("EndpointType").cloned().unwrap_or(json!("PUBLIC")),
        );
        srv.insert(
            "IdentityProviderType".into(),
            b.get("IdentityProviderType")
                .cloned()
                .unwrap_or(json!("SERVICE_MANAGED")),
        );
        // Settle to ONLINE immediately so `server-online` waiters complete
        // against the synchronous control plane.
        srv.insert("State".into(), json!("ONLINE"));
        srv.insert("UserCount".into(), json!(0));
        srv.insert(
            "HostKeyFingerprint".into(),
            json!(format!("SHA256:{}", hex17())),
        );
        copy_present(b, &mut srv, SERVER_FIELDS);
        let srv = Value::Object(srv);
        self.state
            .write()
            .get_or_create(&ctx.account)
            .servers
            .insert(server_id.clone(), srv);
        {
            let mut guard = self.state.write();
            let data = guard.get_or_create(&ctx.account);
            store_tags(data, &server_arn, b);
        }
        ok(json!({ "ServerId": server_id }))
    }

    fn describe_server(&self, ctx: &Ctx, b: &Value) -> Result<AwsResponse, AwsServiceError> {
        let server_id = req_str(b, "ServerId")?;
        let guard = self.state.read();
        let data = guard
            .get(&ctx.account)
            .ok_or_else(|| not_found(&format!("Server {server_id} does not exist.")))?;
        let srv = self.require_server(data, server_id)?;
        let mut out = srv.as_object().unwrap().clone();
        let arn = out
            .get("Arn")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        if let Some(tags) = tags_list(data, &arn) {
            out.insert("Tags".into(), tags);
        }
        // Reflect the live user count.
        let count = data
            .users
            .keys()
            .filter(|k| k.starts_with(&format!("{server_id}/")))
            .count();
        out.insert("UserCount".into(), json!(count));
        ok(json!({ "Server": Value::Object(out) }))
    }

    fn update_server(&self, ctx: &Ctx, b: &Value) -> Result<AwsResponse, AwsServiceError> {
        let server_id = req_str(b, "ServerId")?.to_string();
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        let srv = data
            .servers
            .get_mut(&server_id)
            .ok_or_else(|| not_found(&format!("Server {server_id} does not exist.")))?;
        let obj = srv.as_object_mut().unwrap();
        copy_present(b, obj, SERVER_FIELDS);
        ok(json!({ "ServerId": server_id }))
    }

    fn delete_server(&self, ctx: &Ctx, b: &Value) -> Result<AwsResponse, AwsServiceError> {
        let server_id = req_str(b, "ServerId")?.to_string();
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        let srv = data
            .servers
            .remove(&server_id)
            .ok_or_else(|| not_found(&format!("Server {server_id} does not exist.")))?;
        if let Some(arn) = srv.get("Arn").and_then(Value::as_str) {
            data.tags.remove(arn);
        }
        let prefix = format!("{server_id}/");
        data.users.retain(|k, _| !k.starts_with(&prefix));
        data.accesses.retain(|k, _| !k.starts_with(&prefix));
        data.host_keys.retain(|k, _| !k.starts_with(&prefix));
        data.agreements.retain(|k, _| !k.starts_with(&prefix));
        ok(json!({}))
    }

    fn start_server(&self, ctx: &Ctx, b: &Value) -> Result<AwsResponse, AwsServiceError> {
        self.set_server_state(ctx, b, "ONLINE")
    }

    fn stop_server(&self, ctx: &Ctx, b: &Value) -> Result<AwsResponse, AwsServiceError> {
        self.set_server_state(ctx, b, "OFFLINE")
    }

    fn set_server_state(
        &self,
        ctx: &Ctx,
        b: &Value,
        state: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let server_id = req_str(b, "ServerId")?.to_string();
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        let srv = data
            .servers
            .get_mut(&server_id)
            .ok_or_else(|| not_found(&format!("Server {server_id} does not exist.")))?;
        // Settle synchronously to the requested steady state so a describe
        // waiter observes ONLINE/OFFLINE immediately.
        srv.as_object_mut()
            .unwrap()
            .insert("State".into(), json!(state));
        ok(json!({}))
    }

    fn list_servers(&self, ctx: &Ctx, b: &Value) -> Result<AwsResponse, AwsServiceError> {
        let guard = self.state.read();
        let rows = guard
            .get(&ctx.account)
            .map(|d| d.servers.values().map(|s| listed_server(s, d)).collect())
            .unwrap_or_default();
        ok(list_response("Servers", rows, b, &[]))
    }
}

fn listed_server(srv: &Value, data: &TransferData) -> Value {
    let server_id = srv.get("ServerId").and_then(Value::as_str).unwrap_or("");
    let count = data
        .users
        .keys()
        .filter(|k| k.starts_with(&format!("{server_id}/")))
        .count();
    let mut out = Map::new();
    for f in [
        "Arn",
        "Domain",
        "IdentityProviderType",
        "EndpointType",
        "LoggingRole",
        "ServerId",
        "State",
    ] {
        if let Some(v) = srv.get(f) {
            out.insert(f.to_string(), v.clone());
        }
    }
    out.insert("UserCount".into(), json!(count));
    Value::Object(out)
}

// ===================== users + ssh keys =====================

const USER_FIELDS: &[&str] = &[
    "HomeDirectory",
    "HomeDirectoryMappings",
    "HomeDirectoryType",
    "Policy",
    "PosixProfile",
    "Role",
];

impl TransferService {
    fn create_user(&self, ctx: &Ctx, b: &Value) -> Result<AwsResponse, AwsServiceError> {
        let server_id = req_str(b, "ServerId")?.to_string();
        let user_name = req_str(b, "UserName")?.to_string();
        req_str(b, "Role")?;
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        self.require_server(data, &server_id)?;
        let key = format!("{server_id}/{user_name}");
        if data.users.contains_key(&key) {
            return Err(resource_exists(&format!(
                "User {user_name} already exists on server {server_id}."
            )));
        }
        let user_arn = arn(ctx, &format!("user/{server_id}/{user_name}"));
        let mut user = Map::new();
        user.insert("Arn".into(), json!(user_arn));
        user.insert("UserName".into(), json!(user_name));
        copy_present(b, &mut user, USER_FIELDS);
        let mut ssh_keys = Vec::new();
        if let Some(body) = opt_str(b, "SshPublicKeyBody") {
            ssh_keys.push(json!({
                "SshPublicKeyId": format!("key-{}", hex17()),
                "SshPublicKeyBody": body,
                "DateImported": now_ts()
            }));
        }
        user.insert("SshPublicKeys".into(), json!(ssh_keys));
        data.users.insert(key, Value::Object(user));
        store_tags(data, &user_arn, b);
        ok(json!({ "ServerId": server_id, "UserName": user_name }))
    }

    fn describe_user(&self, ctx: &Ctx, b: &Value) -> Result<AwsResponse, AwsServiceError> {
        let server_id = req_str(b, "ServerId")?.to_string();
        let user_name = req_str(b, "UserName")?.to_string();
        let guard = self.state.read();
        let data = guard
            .get(&ctx.account)
            .ok_or_else(|| not_found(&format!("Server {server_id} does not exist.")))?;
        self.require_server(data, &server_id)?;
        let user = data
            .users
            .get(&format!("{server_id}/{user_name}"))
            .ok_or_else(|| not_found(&format!("User {user_name} does not exist.")))?;
        let mut out = user.as_object().unwrap().clone();
        let arn = out
            .get("Arn")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        if let Some(tags) = tags_list(data, &arn) {
            out.insert("Tags".into(), tags);
        }
        ok(json!({ "ServerId": server_id, "User": Value::Object(out) }))
    }

    fn update_user(&self, ctx: &Ctx, b: &Value) -> Result<AwsResponse, AwsServiceError> {
        let server_id = req_str(b, "ServerId")?.to_string();
        let user_name = req_str(b, "UserName")?.to_string();
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        self.require_server(data, &server_id)?;
        let user = data
            .users
            .get_mut(&format!("{server_id}/{user_name}"))
            .ok_or_else(|| not_found(&format!("User {user_name} does not exist.")))?;
        copy_present(b, user.as_object_mut().unwrap(), USER_FIELDS);
        ok(json!({ "ServerId": server_id, "UserName": user_name }))
    }

    fn delete_user(&self, ctx: &Ctx, b: &Value) -> Result<AwsResponse, AwsServiceError> {
        let server_id = req_str(b, "ServerId")?.to_string();
        let user_name = req_str(b, "UserName")?.to_string();
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        self.require_server(data, &server_id)?;
        let user = data
            .users
            .remove(&format!("{server_id}/{user_name}"))
            .ok_or_else(|| not_found(&format!("User {user_name} does not exist.")))?;
        if let Some(arn) = user.get("Arn").and_then(Value::as_str) {
            data.tags.remove(arn);
        }
        ok(json!({}))
    }

    fn list_users(&self, ctx: &Ctx, b: &Value) -> Result<AwsResponse, AwsServiceError> {
        let server_id = req_str(b, "ServerId")?.to_string();
        let guard = self.state.read();
        let data = guard
            .get(&ctx.account)
            .ok_or_else(|| not_found(&format!("Server {server_id} does not exist.")))?;
        self.require_server(data, &server_id)?;
        let prefix = format!("{server_id}/");
        let rows: Vec<Value> = data
            .users
            .iter()
            .filter(|(k, _)| k.starts_with(&prefix))
            .map(|(_, u)| listed_user(u))
            .collect();
        ok(list_response(
            "Users",
            rows,
            b,
            &[("ServerId", json!(server_id))],
        ))
    }

    fn import_ssh_public_key(&self, ctx: &Ctx, b: &Value) -> Result<AwsResponse, AwsServiceError> {
        let server_id = req_str(b, "ServerId")?.to_string();
        let user_name = req_str(b, "UserName")?.to_string();
        let body = req_str(b, "SshPublicKeyBody")?.to_string();
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        self.require_server(data, &server_id)?;
        let user = data
            .users
            .get_mut(&format!("{server_id}/{user_name}"))
            .ok_or_else(|| not_found(&format!("User {user_name} does not exist.")))?;
        let key_id = format!("key-{}", hex17());
        let keys = user
            .as_object_mut()
            .unwrap()
            .entry("SshPublicKeys")
            .or_insert_with(|| json!([]));
        if let Some(arr) = keys.as_array_mut() {
            if arr
                .iter()
                .any(|k| k.get("SshPublicKeyBody").and_then(Value::as_str) == Some(body.as_str()))
            {
                return Err(resource_exists("The SSH public key already exists."));
            }
            arr.push(json!({
                "SshPublicKeyId": key_id,
                "SshPublicKeyBody": body,
                "DateImported": now_ts()
            }));
        }
        ok(json!({
            "ServerId": server_id,
            "SshPublicKeyId": key_id,
            "UserName": user_name
        }))
    }

    fn delete_ssh_public_key(&self, ctx: &Ctx, b: &Value) -> Result<AwsResponse, AwsServiceError> {
        let server_id = req_str(b, "ServerId")?.to_string();
        let user_name = req_str(b, "UserName")?.to_string();
        let key_id = req_str(b, "SshPublicKeyId")?.to_string();
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        self.require_server(data, &server_id)?;
        let user = data
            .users
            .get_mut(&format!("{server_id}/{user_name}"))
            .ok_or_else(|| not_found(&format!("User {user_name} does not exist.")))?;
        let keys = user
            .as_object_mut()
            .unwrap()
            .get_mut("SshPublicKeys")
            .and_then(Value::as_array_mut);
        let removed = match keys {
            Some(arr) => {
                let before = arr.len();
                arr.retain(|k| {
                    k.get("SshPublicKeyId").and_then(Value::as_str) != Some(key_id.as_str())
                });
                arr.len() != before
            }
            None => false,
        };
        if !removed {
            return Err(not_found(&format!(
                "SSH public key {key_id} does not exist."
            )));
        }
        ok(json!({}))
    }
}

fn listed_user(user: &Value) -> Value {
    let mut out = Map::new();
    for f in [
        "Arn",
        "HomeDirectory",
        "HomeDirectoryType",
        "Role",
        "UserName",
    ] {
        if let Some(v) = user.get(f) {
            out.insert(f.to_string(), v.clone());
        }
    }
    let count = user
        .get("SshPublicKeys")
        .and_then(Value::as_array)
        .map(|a| a.len())
        .unwrap_or(0);
    out.insert("SshPublicKeyCount".into(), json!(count));
    Value::Object(out)
}

// ===================== host keys =====================

impl TransferService {
    fn import_host_key(&self, ctx: &Ctx, b: &Value) -> Result<AwsResponse, AwsServiceError> {
        let server_id = req_str(b, "ServerId")?.to_string();
        req_str(b, "HostKeyBody")?;
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        self.require_server(data, &server_id)?;
        let host_key_id = format!("hostkey-{}", hex17());
        let key = format!("{server_id}/{host_key_id}");
        let hk_arn = arn(ctx, &format!("host-key/{server_id}/{host_key_id}"));
        let mut hk = Map::new();
        hk.insert("Arn".into(), json!(hk_arn));
        hk.insert("HostKeyId".into(), json!(host_key_id));
        hk.insert(
            "HostKeyFingerprint".into(),
            json!(format!("SHA256:{}", hex17())),
        );
        hk.insert("Type".into(), json!("ssh-rsa"));
        hk.insert("DateImported".into(), json!(now_ts()));
        copy_present(b, &mut hk, &["Description"]);
        data.host_keys.insert(key, Value::Object(hk));
        store_tags(data, &hk_arn, b);
        ok(json!({ "ServerId": server_id, "HostKeyId": host_key_id }))
    }

    fn update_host_key(&self, ctx: &Ctx, b: &Value) -> Result<AwsResponse, AwsServiceError> {
        let server_id = req_str(b, "ServerId")?.to_string();
        let host_key_id = req_str(b, "HostKeyId")?.to_string();
        let description = req_str(b, "Description")?.to_string();
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        let hk = data
            .host_keys
            .get_mut(&format!("{server_id}/{host_key_id}"))
            .ok_or_else(|| not_found(&format!("Host key {host_key_id} does not exist.")))?;
        hk.as_object_mut()
            .unwrap()
            .insert("Description".into(), json!(description));
        ok(json!({ "ServerId": server_id, "HostKeyId": host_key_id }))
    }

    fn describe_host_key(&self, ctx: &Ctx, b: &Value) -> Result<AwsResponse, AwsServiceError> {
        let server_id = req_str(b, "ServerId")?.to_string();
        let host_key_id = req_str(b, "HostKeyId")?.to_string();
        let guard = self.state.read();
        let data = guard
            .get(&ctx.account)
            .ok_or_else(|| not_found(&format!("Host key {host_key_id} does not exist.")))?;
        let hk = data
            .host_keys
            .get(&format!("{server_id}/{host_key_id}"))
            .ok_or_else(|| not_found(&format!("Host key {host_key_id} does not exist.")))?;
        let mut out = hk.as_object().unwrap().clone();
        let arn = out
            .get("Arn")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        if let Some(tags) = tags_list(data, &arn) {
            out.insert("Tags".into(), tags);
        }
        ok(json!({ "HostKey": Value::Object(out) }))
    }

    fn delete_host_key(&self, ctx: &Ctx, b: &Value) -> Result<AwsResponse, AwsServiceError> {
        let server_id = req_str(b, "ServerId")?.to_string();
        let host_key_id = req_str(b, "HostKeyId")?.to_string();
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        let hk = data
            .host_keys
            .remove(&format!("{server_id}/{host_key_id}"))
            .ok_or_else(|| not_found(&format!("Host key {host_key_id} does not exist.")))?;
        if let Some(arn) = hk.get("Arn").and_then(Value::as_str) {
            data.tags.remove(arn);
        }
        ok(json!({}))
    }

    fn list_host_keys(&self, ctx: &Ctx, b: &Value) -> Result<AwsResponse, AwsServiceError> {
        let server_id = req_str(b, "ServerId")?.to_string();
        let guard = self.state.read();
        let data = guard
            .get(&ctx.account)
            .ok_or_else(|| not_found(&format!("Server {server_id} does not exist.")))?;
        self.require_server(data, &server_id)?;
        let prefix = format!("{server_id}/");
        let rows: Vec<Value> = data
            .host_keys
            .iter()
            .filter(|(k, _)| k.starts_with(&prefix))
            .map(|(_, hk)| listed_host_key(hk))
            .collect();
        ok(list_response(
            "HostKeys",
            rows,
            b,
            &[("ServerId", json!(server_id))],
        ))
    }
}

fn listed_host_key(hk: &Value) -> Value {
    let mut out = Map::new();
    for (src, dst) in [
        ("Arn", "Arn"),
        ("HostKeyId", "HostKeyId"),
        ("HostKeyFingerprint", "Fingerprint"),
        ("Description", "Description"),
        ("Type", "Type"),
        ("DateImported", "DateImported"),
    ] {
        if let Some(v) = hk.get(src) {
            out.insert(dst.to_string(), v.clone());
        }
    }
    Value::Object(out)
}

// ===================== accesses =====================

const ACCESS_FIELDS: &[&str] = &[
    "HomeDirectory",
    "HomeDirectoryMappings",
    "HomeDirectoryType",
    "Policy",
    "PosixProfile",
    "Role",
];

impl TransferService {
    fn create_access(&self, ctx: &Ctx, b: &Value) -> Result<AwsResponse, AwsServiceError> {
        let server_id = req_str(b, "ServerId")?.to_string();
        let external_id = req_str(b, "ExternalId")?.to_string();
        req_str(b, "Role")?;
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        self.require_server(data, &server_id)?;
        let key = format!("{server_id}/{external_id}");
        if data.accesses.contains_key(&key) {
            return Err(resource_exists(&format!(
                "Access for {external_id} already exists."
            )));
        }
        let mut acc = Map::new();
        acc.insert("ExternalId".into(), json!(external_id));
        copy_present(b, &mut acc, ACCESS_FIELDS);
        data.accesses.insert(key, Value::Object(acc));
        ok(json!({ "ServerId": server_id, "ExternalId": external_id }))
    }

    fn update_access(&self, ctx: &Ctx, b: &Value) -> Result<AwsResponse, AwsServiceError> {
        let server_id = req_str(b, "ServerId")?.to_string();
        let external_id = req_str(b, "ExternalId")?.to_string();
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        self.require_server(data, &server_id)?;
        let acc = data
            .accesses
            .get_mut(&format!("{server_id}/{external_id}"))
            .ok_or_else(|| not_found(&format!("Access {external_id} does not exist.")))?;
        copy_present(b, acc.as_object_mut().unwrap(), ACCESS_FIELDS);
        ok(json!({ "ServerId": server_id, "ExternalId": external_id }))
    }

    fn describe_access(&self, ctx: &Ctx, b: &Value) -> Result<AwsResponse, AwsServiceError> {
        let server_id = req_str(b, "ServerId")?.to_string();
        let external_id = req_str(b, "ExternalId")?.to_string();
        let guard = self.state.read();
        let data = guard
            .get(&ctx.account)
            .ok_or_else(|| not_found(&format!("Access {external_id} does not exist.")))?;
        let acc = data
            .accesses
            .get(&format!("{server_id}/{external_id}"))
            .ok_or_else(|| not_found(&format!("Access {external_id} does not exist.")))?;
        ok(json!({ "ServerId": server_id, "Access": acc }))
    }

    fn delete_access(&self, ctx: &Ctx, b: &Value) -> Result<AwsResponse, AwsServiceError> {
        let server_id = req_str(b, "ServerId")?.to_string();
        let external_id = req_str(b, "ExternalId")?.to_string();
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        data.accesses
            .remove(&format!("{server_id}/{external_id}"))
            .ok_or_else(|| not_found(&format!("Access {external_id} does not exist.")))?;
        ok(json!({}))
    }

    fn list_accesses(&self, ctx: &Ctx, b: &Value) -> Result<AwsResponse, AwsServiceError> {
        let server_id = req_str(b, "ServerId")?.to_string();
        let guard = self.state.read();
        let data = guard
            .get(&ctx.account)
            .ok_or_else(|| not_found(&format!("Server {server_id} does not exist.")))?;
        self.require_server(data, &server_id)?;
        let prefix = format!("{server_id}/");
        let rows: Vec<Value> = data
            .accesses
            .iter()
            .filter(|(k, _)| k.starts_with(&prefix))
            .map(|(_, a)| {
                let mut out = Map::new();
                for f in ["HomeDirectory", "HomeDirectoryType", "Role", "ExternalId"] {
                    if let Some(v) = a.get(f) {
                        out.insert(f.to_string(), v.clone());
                    }
                }
                Value::Object(out)
            })
            .collect();
        ok(list_response(
            "Accesses",
            rows,
            b,
            &[("ServerId", json!(server_id))],
        ))
    }
}

// ===================== workflows + executions =====================

impl TransferService {
    fn create_workflow(&self, ctx: &Ctx, b: &Value) -> Result<AwsResponse, AwsServiceError> {
        if b.get("Steps").and_then(Value::as_array).is_none() {
            return Err(invalid_request("Steps is required."));
        }
        let workflow_id = format!("w-{}", hex17());
        let wf_arn = arn(ctx, &format!("workflow/{workflow_id}"));
        let mut wf = Map::new();
        wf.insert("Arn".into(), json!(wf_arn));
        wf.insert("WorkflowId".into(), json!(workflow_id));
        copy_present(b, &mut wf, &["Description", "Steps", "OnExceptionSteps"]);
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        data.workflows
            .insert(workflow_id.clone(), Value::Object(wf));
        store_tags(data, &wf_arn, b);
        ok(json!({ "WorkflowId": workflow_id }))
    }

    fn describe_workflow(&self, ctx: &Ctx, b: &Value) -> Result<AwsResponse, AwsServiceError> {
        let workflow_id = req_str(b, "WorkflowId")?.to_string();
        let guard = self.state.read();
        let data = guard
            .get(&ctx.account)
            .ok_or_else(|| not_found(&format!("Workflow {workflow_id} does not exist.")))?;
        let wf = data
            .workflows
            .get(&workflow_id)
            .ok_or_else(|| not_found(&format!("Workflow {workflow_id} does not exist.")))?;
        let mut out = wf.as_object().unwrap().clone();
        let arn = out
            .get("Arn")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        if let Some(tags) = tags_list(data, &arn) {
            out.insert("Tags".into(), tags);
        }
        ok(json!({ "Workflow": Value::Object(out) }))
    }

    fn delete_workflow(&self, ctx: &Ctx, b: &Value) -> Result<AwsResponse, AwsServiceError> {
        let workflow_id = req_str(b, "WorkflowId")?.to_string();
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        let wf = data
            .workflows
            .remove(&workflow_id)
            .ok_or_else(|| not_found(&format!("Workflow {workflow_id} does not exist.")))?;
        if let Some(arn) = wf.get("Arn").and_then(Value::as_str) {
            data.tags.remove(arn);
        }
        let prefix = format!("{workflow_id}/");
        data.executions.retain(|k, _| !k.starts_with(&prefix));
        ok(json!({}))
    }

    fn list_workflows(&self, ctx: &Ctx, b: &Value) -> Result<AwsResponse, AwsServiceError> {
        let guard = self.state.read();
        let rows: Vec<Value> = guard
            .get(&ctx.account)
            .map(|d| {
                d.workflows
                    .values()
                    .map(|w| {
                        let mut out = Map::new();
                        for f in ["WorkflowId", "Description", "Arn"] {
                            if let Some(v) = w.get(f) {
                                out.insert(f.to_string(), v.clone());
                            }
                        }
                        Value::Object(out)
                    })
                    .collect()
            })
            .unwrap_or_default();
        ok(list_response("Workflows", rows, b, &[]))
    }

    fn send_workflow_step_state(
        &self,
        ctx: &Ctx,
        b: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let workflow_id = req_str(b, "WorkflowId")?.to_string();
        let execution_id = req_str(b, "ExecutionId")?.to_string();
        req_str(b, "Token")?;
        req_str(b, "Status")?;
        let guard = self.state.read();
        let data = guard
            .get(&ctx.account)
            .ok_or_else(|| not_found(&format!("Execution {execution_id} does not exist.")))?;
        if !data
            .executions
            .contains_key(&format!("{workflow_id}/{execution_id}"))
        {
            return Err(not_found(&format!(
                "Execution {execution_id} does not exist."
            )));
        }
        ok(json!({}))
    }

    fn describe_execution(&self, ctx: &Ctx, b: &Value) -> Result<AwsResponse, AwsServiceError> {
        let workflow_id = req_str(b, "WorkflowId")?.to_string();
        let execution_id = req_str(b, "ExecutionId")?.to_string();
        let guard = self.state.read();
        let data = guard
            .get(&ctx.account)
            .ok_or_else(|| not_found(&format!("Execution {execution_id} does not exist.")))?;
        let exec = data
            .executions
            .get(&format!("{workflow_id}/{execution_id}"))
            .ok_or_else(|| not_found(&format!("Execution {execution_id} does not exist.")))?;
        ok(json!({ "WorkflowId": workflow_id, "Execution": exec }))
    }

    fn list_executions(&self, ctx: &Ctx, b: &Value) -> Result<AwsResponse, AwsServiceError> {
        let workflow_id = req_str(b, "WorkflowId")?.to_string();
        let guard = self.state.read();
        let data = guard
            .get(&ctx.account)
            .ok_or_else(|| not_found(&format!("Workflow {workflow_id} does not exist.")))?;
        if !data.workflows.contains_key(&workflow_id) {
            return Err(not_found(&format!(
                "Workflow {workflow_id} does not exist."
            )));
        }
        let prefix = format!("{workflow_id}/");
        let rows: Vec<Value> = data
            .executions
            .iter()
            .filter(|(k, _)| k.starts_with(&prefix))
            .map(|(_, e)| {
                let mut out = Map::new();
                for f in [
                    "ExecutionId",
                    "InitialFileLocation",
                    "ServiceMetadata",
                    "Status",
                ] {
                    if let Some(v) = e.get(f) {
                        out.insert(f.to_string(), v.clone());
                    }
                }
                Value::Object(out)
            })
            .collect();
        ok(list_response(
            "Executions",
            rows,
            b,
            &[("WorkflowId", json!(workflow_id))],
        ))
    }
}

// ===================== agreements =====================

const AGREEMENT_FIELDS: &[&str] = &[
    "Description",
    "Status",
    "LocalProfileId",
    "PartnerProfileId",
    "BaseDirectory",
    "AccessRole",
    "PreserveFilename",
    "EnforceMessageSigning",
    "CustomDirectories",
];

impl TransferService {
    fn create_agreement(&self, ctx: &Ctx, b: &Value) -> Result<AwsResponse, AwsServiceError> {
        let server_id = req_str(b, "ServerId")?.to_string();
        req_str(b, "LocalProfileId")?;
        req_str(b, "PartnerProfileId")?;
        req_str(b, "AccessRole")?;
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        self.require_server(data, &server_id)?;
        let agreement_id = format!("a-{}", hex17());
        let ag_arn = arn(ctx, &format!("agreement/{agreement_id}"));
        let mut ag = Map::new();
        ag.insert("Arn".into(), json!(ag_arn));
        ag.insert("AgreementId".into(), json!(agreement_id));
        ag.insert("ServerId".into(), json!(server_id));
        ag.insert(
            "Status".into(),
            b.get("Status").cloned().unwrap_or(json!("ACTIVE")),
        );
        copy_present(b, &mut ag, AGREEMENT_FIELDS);
        data.agreements
            .insert(format!("{server_id}/{agreement_id}"), Value::Object(ag));
        store_tags(data, &ag_arn, b);
        ok(json!({ "AgreementId": agreement_id }))
    }

    fn update_agreement(&self, ctx: &Ctx, b: &Value) -> Result<AwsResponse, AwsServiceError> {
        let agreement_id = req_str(b, "AgreementId")?.to_string();
        let server_id = req_str(b, "ServerId")?.to_string();
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        let ag = data
            .agreements
            .get_mut(&format!("{server_id}/{agreement_id}"))
            .ok_or_else(|| not_found(&format!("Agreement {agreement_id} does not exist.")))?;
        copy_present(b, ag.as_object_mut().unwrap(), AGREEMENT_FIELDS);
        ok(json!({ "AgreementId": agreement_id }))
    }

    fn describe_agreement(&self, ctx: &Ctx, b: &Value) -> Result<AwsResponse, AwsServiceError> {
        let agreement_id = req_str(b, "AgreementId")?.to_string();
        let server_id = req_str(b, "ServerId")?.to_string();
        let guard = self.state.read();
        let data = guard
            .get(&ctx.account)
            .ok_or_else(|| not_found(&format!("Agreement {agreement_id} does not exist.")))?;
        let ag = data
            .agreements
            .get(&format!("{server_id}/{agreement_id}"))
            .ok_or_else(|| not_found(&format!("Agreement {agreement_id} does not exist.")))?;
        let mut out = ag.as_object().unwrap().clone();
        let arn = out
            .get("Arn")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        if let Some(tags) = tags_list(data, &arn) {
            out.insert("Tags".into(), tags);
        }
        ok(json!({ "Agreement": Value::Object(out) }))
    }

    fn delete_agreement(&self, ctx: &Ctx, b: &Value) -> Result<AwsResponse, AwsServiceError> {
        let agreement_id = req_str(b, "AgreementId")?.to_string();
        let server_id = req_str(b, "ServerId")?.to_string();
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        let ag = data
            .agreements
            .remove(&format!("{server_id}/{agreement_id}"))
            .ok_or_else(|| not_found(&format!("Agreement {agreement_id} does not exist.")))?;
        if let Some(arn) = ag.get("Arn").and_then(Value::as_str) {
            data.tags.remove(arn);
        }
        ok(json!({}))
    }

    fn list_agreements(&self, ctx: &Ctx, b: &Value) -> Result<AwsResponse, AwsServiceError> {
        let server_id = req_str(b, "ServerId")?.to_string();
        let guard = self.state.read();
        let data = guard
            .get(&ctx.account)
            .ok_or_else(|| not_found(&format!("Server {server_id} does not exist.")))?;
        self.require_server(data, &server_id)?;
        let prefix = format!("{server_id}/");
        let rows: Vec<Value> = data
            .agreements
            .iter()
            .filter(|(k, _)| k.starts_with(&prefix))
            .map(|(_, a)| {
                let mut out = Map::new();
                for f in [
                    "Arn",
                    "AgreementId",
                    "Description",
                    "Status",
                    "ServerId",
                    "LocalProfileId",
                    "PartnerProfileId",
                ] {
                    if let Some(v) = a.get(f) {
                        out.insert(f.to_string(), v.clone());
                    }
                }
                Value::Object(out)
            })
            .collect();
        ok(list_response("Agreements", rows, b, &[]))
    }
}

// ===================== connectors =====================

const CONNECTOR_FIELDS: &[&str] = &[
    "Url",
    "As2Config",
    "AccessRole",
    "LoggingRole",
    "SftpConfig",
    "SecurityPolicyName",
    "IpAddressType",
];

impl TransferService {
    fn require_connector<'a>(
        &self,
        data: &'a TransferData,
        connector_id: &str,
    ) -> Result<&'a Value, AwsServiceError> {
        data.connectors
            .get(connector_id)
            .ok_or_else(|| not_found(&format!("Connector {connector_id} does not exist.")))
    }

    fn create_connector(&self, ctx: &Ctx, b: &Value) -> Result<AwsResponse, AwsServiceError> {
        req_str(b, "AccessRole")?;
        let connector_id = format!("c-{}", hex17());
        let c_arn = arn(ctx, &format!("connector/{connector_id}"));
        let mut c = Map::new();
        c.insert("Arn".into(), json!(c_arn));
        c.insert("ConnectorId".into(), json!(connector_id));
        c.insert("EgressType".into(), json!("SERVICE_MANAGED"));
        c.insert("Status".into(), json!("ACTIVE"));
        c.insert(
            "ServiceManagedEgressIpAddresses".into(),
            json!(["3.219.0.10", "52.4.0.20"]),
        );
        copy_present(b, &mut c, CONNECTOR_FIELDS);
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        data.connectors
            .insert(connector_id.clone(), Value::Object(c));
        store_tags(data, &c_arn, b);
        ok(json!({ "ConnectorId": connector_id }))
    }

    fn update_connector(&self, ctx: &Ctx, b: &Value) -> Result<AwsResponse, AwsServiceError> {
        let connector_id = req_str(b, "ConnectorId")?.to_string();
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        let c = data
            .connectors
            .get_mut(&connector_id)
            .ok_or_else(|| not_found(&format!("Connector {connector_id} does not exist.")))?;
        copy_present(b, c.as_object_mut().unwrap(), CONNECTOR_FIELDS);
        ok(json!({ "ConnectorId": connector_id }))
    }

    fn describe_connector(&self, ctx: &Ctx, b: &Value) -> Result<AwsResponse, AwsServiceError> {
        let connector_id = req_str(b, "ConnectorId")?.to_string();
        let guard = self.state.read();
        let data = guard
            .get(&ctx.account)
            .ok_or_else(|| not_found(&format!("Connector {connector_id} does not exist.")))?;
        let c = self.require_connector(data, &connector_id)?;
        let mut out = c.as_object().unwrap().clone();
        let arn = out
            .get("Arn")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        if let Some(tags) = tags_list(data, &arn) {
            out.insert("Tags".into(), tags);
        }
        ok(json!({ "Connector": Value::Object(out) }))
    }

    fn delete_connector(&self, ctx: &Ctx, b: &Value) -> Result<AwsResponse, AwsServiceError> {
        let connector_id = req_str(b, "ConnectorId")?.to_string();
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        let c = data
            .connectors
            .remove(&connector_id)
            .ok_or_else(|| not_found(&format!("Connector {connector_id} does not exist.")))?;
        if let Some(arn) = c.get("Arn").and_then(Value::as_str) {
            data.tags.remove(arn);
        }
        let prefix = format!("{connector_id}/");
        data.file_transfers.retain(|k, _| !k.starts_with(&prefix));
        ok(json!({}))
    }

    fn list_connectors(&self, ctx: &Ctx, b: &Value) -> Result<AwsResponse, AwsServiceError> {
        let guard = self.state.read();
        let rows: Vec<Value> = guard
            .get(&ctx.account)
            .map(|d| {
                d.connectors
                    .values()
                    .map(|c| {
                        let mut out = Map::new();
                        for f in ["Arn", "ConnectorId", "Url"] {
                            if let Some(v) = c.get(f) {
                                out.insert(f.to_string(), v.clone());
                            }
                        }
                        Value::Object(out)
                    })
                    .collect()
            })
            .unwrap_or_default();
        ok(list_response("Connectors", rows, b, &[]))
    }

    fn test_connection(&self, ctx: &Ctx, b: &Value) -> Result<AwsResponse, AwsServiceError> {
        let connector_id = req_str(b, "ConnectorId")?.to_string();
        let guard = self.state.read();
        let data = guard
            .get(&ctx.account)
            .ok_or_else(|| not_found(&format!("Connector {connector_id} does not exist.")))?;
        self.require_connector(data, &connector_id)?;
        ok(json!({
            "ConnectorId": connector_id,
            "Status": "OK",
            "StatusMessage": "Connection succeeded"
        }))
    }

    fn start_directory_listing(
        &self,
        ctx: &Ctx,
        b: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let connector_id = req_str(b, "ConnectorId")?.to_string();
        req_str(b, "RemoteDirectoryPath")?;
        req_str(b, "OutputDirectoryPath")?;
        let guard = self.state.read();
        let data = guard
            .get(&ctx.account)
            .ok_or_else(|| not_found(&format!("Connector {connector_id} does not exist.")))?;
        self.require_connector(data, &connector_id)?;
        let listing_id = Uuid::new_v4().to_string();
        ok(json!({
            "ListingId": listing_id,
            "OutputFileName": format!("{connector_id}-{listing_id}.json")
        }))
    }

    fn start_file_transfer(&self, ctx: &Ctx, b: &Value) -> Result<AwsResponse, AwsServiceError> {
        let connector_id = req_str(b, "ConnectorId")?.to_string();
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        self.require_connector(data, &connector_id)?;
        let transfer_id = Uuid::new_v4().to_string();
        // Record a completed result per requested path so
        // ListFileTransferResults reflects the transfer.
        let mut results = Vec::new();
        for field in ["SendFilePaths", "RetrieveFilePaths"] {
            if let Some(paths) = b.get(field).and_then(Value::as_array) {
                for p in paths.iter().filter_map(Value::as_str) {
                    results.push(json!({ "FilePath": p, "StatusCode": "COMPLETED" }));
                }
            }
        }
        data.file_transfers.insert(
            format!("{connector_id}/{transfer_id}"),
            json!({ "Results": results }),
        );
        ok(json!({ "TransferId": transfer_id }))
    }

    fn start_remote_delete(&self, ctx: &Ctx, b: &Value) -> Result<AwsResponse, AwsServiceError> {
        let connector_id = req_str(b, "ConnectorId")?.to_string();
        req_str(b, "DeletePath")?;
        let guard = self.state.read();
        let data = guard
            .get(&ctx.account)
            .ok_or_else(|| not_found(&format!("Connector {connector_id} does not exist.")))?;
        self.require_connector(data, &connector_id)?;
        ok(json!({ "DeleteId": Uuid::new_v4().to_string() }))
    }

    fn start_remote_move(&self, ctx: &Ctx, b: &Value) -> Result<AwsResponse, AwsServiceError> {
        let connector_id = req_str(b, "ConnectorId")?.to_string();
        req_str(b, "SourcePath")?;
        req_str(b, "TargetPath")?;
        let guard = self.state.read();
        let data = guard
            .get(&ctx.account)
            .ok_or_else(|| not_found(&format!("Connector {connector_id} does not exist.")))?;
        self.require_connector(data, &connector_id)?;
        ok(json!({ "MoveId": Uuid::new_v4().to_string() }))
    }

    fn list_file_transfer_results(
        &self,
        ctx: &Ctx,
        b: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let connector_id = req_str(b, "ConnectorId")?.to_string();
        let transfer_id = req_str(b, "TransferId")?.to_string();
        let guard = self.state.read();
        let data = guard
            .get(&ctx.account)
            .ok_or_else(|| not_found(&format!("Connector {connector_id} does not exist.")))?;
        self.require_connector(data, &connector_id)?;
        let rows: Vec<Value> = data
            .file_transfers
            .get(&format!("{connector_id}/{transfer_id}"))
            .and_then(|t| t.get("Results"))
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        ok(list_response("FileTransferResults", rows, b, &[]))
    }
}

// ===================== profiles =====================

impl TransferService {
    fn create_profile(&self, ctx: &Ctx, b: &Value) -> Result<AwsResponse, AwsServiceError> {
        req_str(b, "As2Id")?;
        req_str(b, "ProfileType")?;
        let profile_id = format!("p-{}", hex17());
        let p_arn = arn(ctx, &format!("profile/{profile_id}"));
        let mut p = Map::new();
        p.insert("Arn".into(), json!(p_arn));
        p.insert("ProfileId".into(), json!(profile_id));
        copy_present(b, &mut p, &["As2Id", "ProfileType", "CertificateIds"]);
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        data.profiles.insert(profile_id.clone(), Value::Object(p));
        store_tags(data, &p_arn, b);
        ok(json!({ "ProfileId": profile_id }))
    }

    fn update_profile(&self, ctx: &Ctx, b: &Value) -> Result<AwsResponse, AwsServiceError> {
        let profile_id = req_str(b, "ProfileId")?.to_string();
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        let p = data
            .profiles
            .get_mut(&profile_id)
            .ok_or_else(|| not_found(&format!("Profile {profile_id} does not exist.")))?;
        copy_present(b, p.as_object_mut().unwrap(), &["CertificateIds"]);
        ok(json!({ "ProfileId": profile_id }))
    }

    fn describe_profile(&self, ctx: &Ctx, b: &Value) -> Result<AwsResponse, AwsServiceError> {
        let profile_id = req_str(b, "ProfileId")?.to_string();
        let guard = self.state.read();
        let data = guard
            .get(&ctx.account)
            .ok_or_else(|| not_found(&format!("Profile {profile_id} does not exist.")))?;
        let p = data
            .profiles
            .get(&profile_id)
            .ok_or_else(|| not_found(&format!("Profile {profile_id} does not exist.")))?;
        let mut out = p.as_object().unwrap().clone();
        let arn = out
            .get("Arn")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        if let Some(tags) = tags_list(data, &arn) {
            out.insert("Tags".into(), tags);
        }
        ok(json!({ "Profile": Value::Object(out) }))
    }

    fn delete_profile(&self, ctx: &Ctx, b: &Value) -> Result<AwsResponse, AwsServiceError> {
        let profile_id = req_str(b, "ProfileId")?.to_string();
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        let p = data
            .profiles
            .remove(&profile_id)
            .ok_or_else(|| not_found(&format!("Profile {profile_id} does not exist.")))?;
        if let Some(arn) = p.get("Arn").and_then(Value::as_str) {
            data.tags.remove(arn);
        }
        ok(json!({}))
    }

    fn list_profiles(&self, ctx: &Ctx, b: &Value) -> Result<AwsResponse, AwsServiceError> {
        let filter = opt_str(b, "ProfileType").map(String::from);
        let guard = self.state.read();
        let rows: Vec<Value> = guard
            .get(&ctx.account)
            .map(|d| {
                d.profiles
                    .values()
                    .filter(|p| {
                        filter
                            .as_deref()
                            .is_none_or(|f| p.get("ProfileType").and_then(Value::as_str) == Some(f))
                    })
                    .map(|p| {
                        let mut out = Map::new();
                        for f in ["Arn", "ProfileId", "As2Id", "ProfileType"] {
                            if let Some(v) = p.get(f) {
                                out.insert(f.to_string(), v.clone());
                            }
                        }
                        Value::Object(out)
                    })
                    .collect()
            })
            .unwrap_or_default();
        ok(list_response("Profiles", rows, b, &[]))
    }
}

// ===================== certificates =====================

impl TransferService {
    fn import_certificate(&self, ctx: &Ctx, b: &Value) -> Result<AwsResponse, AwsServiceError> {
        req_str(b, "Usage")?;
        req_str(b, "Certificate")?;
        let certificate_id = format!("cert-{}", hex17());
        let c_arn = arn(ctx, &format!("certificate/{certificate_id}"));
        let mut c = Map::new();
        c.insert("Arn".into(), json!(c_arn));
        c.insert("CertificateId".into(), json!(certificate_id));
        c.insert("Status".into(), json!("ACTIVE"));
        c.insert("Type".into(), json!("CERTIFICATE"));
        copy_present(
            b,
            &mut c,
            &[
                "Usage",
                "Certificate",
                "CertificateChain",
                "ActiveDate",
                "InactiveDate",
                "Description",
            ],
        );
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        data.certificates
            .insert(certificate_id.clone(), Value::Object(c));
        store_tags(data, &c_arn, b);
        ok(json!({ "CertificateId": certificate_id }))
    }

    fn update_certificate(&self, ctx: &Ctx, b: &Value) -> Result<AwsResponse, AwsServiceError> {
        let certificate_id = req_str(b, "CertificateId")?.to_string();
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        let c = data
            .certificates
            .get_mut(&certificate_id)
            .ok_or_else(|| not_found(&format!("Certificate {certificate_id} does not exist.")))?;
        copy_present(
            b,
            c.as_object_mut().unwrap(),
            &["ActiveDate", "InactiveDate", "Description"],
        );
        ok(json!({ "CertificateId": certificate_id }))
    }

    fn describe_certificate(&self, ctx: &Ctx, b: &Value) -> Result<AwsResponse, AwsServiceError> {
        let certificate_id = req_str(b, "CertificateId")?.to_string();
        let guard = self.state.read();
        let data = guard
            .get(&ctx.account)
            .ok_or_else(|| not_found(&format!("Certificate {certificate_id} does not exist.")))?;
        let c = data
            .certificates
            .get(&certificate_id)
            .ok_or_else(|| not_found(&format!("Certificate {certificate_id} does not exist.")))?;
        let mut out = c.as_object().unwrap().clone();
        let arn = out
            .get("Arn")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        if let Some(tags) = tags_list(data, &arn) {
            out.insert("Tags".into(), tags);
        }
        ok(json!({ "Certificate": Value::Object(out) }))
    }

    fn delete_certificate(&self, ctx: &Ctx, b: &Value) -> Result<AwsResponse, AwsServiceError> {
        let certificate_id = req_str(b, "CertificateId")?.to_string();
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        let c = data
            .certificates
            .remove(&certificate_id)
            .ok_or_else(|| not_found(&format!("Certificate {certificate_id} does not exist.")))?;
        if let Some(arn) = c.get("Arn").and_then(Value::as_str) {
            data.tags.remove(arn);
        }
        ok(json!({}))
    }

    fn list_certificates(&self, ctx: &Ctx, b: &Value) -> Result<AwsResponse, AwsServiceError> {
        let guard = self.state.read();
        let rows: Vec<Value> = guard
            .get(&ctx.account)
            .map(|d| {
                d.certificates
                    .values()
                    .map(|c| {
                        let mut out = Map::new();
                        for f in [
                            "Arn",
                            "CertificateId",
                            "Usage",
                            "Status",
                            "ActiveDate",
                            "InactiveDate",
                            "Type",
                            "Description",
                        ] {
                            if let Some(v) = c.get(f) {
                                out.insert(f.to_string(), v.clone());
                            }
                        }
                        Value::Object(out)
                    })
                    .collect()
            })
            .unwrap_or_default();
        ok(list_response("Certificates", rows, b, &[]))
    }
}

// ===================== security policies =====================

/// The real AWS Transfer Family managed security policy names.
const SECURITY_POLICIES: &[&str] = &[
    "TransferSecurityPolicy-2018-11",
    "TransferSecurityPolicy-2020-06",
    "TransferSecurityPolicy-2022-03",
    "TransferSecurityPolicy-2023-05",
    "TransferSecurityPolicy-2024-01",
    "TransferSecurityPolicy-FIPS-2020-06",
    "TransferSecurityPolicy-FIPS-2023-05",
    "TransferSecurityPolicy-FIPS-2024-01",
    "TransferSecurityPolicy-FIPS-2024-05",
    "TransferSecurityPolicy-PQ-SSH-Experimental-2023-04",
    "TransferSecurityPolicy-PQ-SSH-FIPS-Experimental-2023-04",
    "TransferSecurityPolicy-Restricted-2018-11",
    "TransferSecurityPolicy-Restricted-2020-06",
];

impl TransferService {
    fn describe_security_policy(&self, b: &Value) -> Result<AwsResponse, AwsServiceError> {
        let name = req_str(b, "SecurityPolicyName")?;
        if !SECURITY_POLICIES.contains(&name) {
            return Err(not_found(&format!(
                "Security policy {name} does not exist."
            )));
        }
        let fips = name.contains("FIPS");
        let policy_type = if name.contains("Connector") {
            "CONNECTOR"
        } else {
            "SERVER"
        };
        ok(json!({
            "SecurityPolicy": {
                "SecurityPolicyName": name,
                "Fips": fips,
                "Type": policy_type,
                "SshCiphers": ["aes256-gcm@openssh.com", "aes128-gcm@openssh.com"],
                "SshKexs": ["ecdh-sha2-nistp256", "diffie-hellman-group-exchange-sha256"],
                "SshMacs": ["hmac-sha2-256", "hmac-sha2-512"],
                "TlsCiphers": ["TLS_AES_128_GCM_SHA256", "TLS_AES_256_GCM_SHA384"]
            }
        }))
    }

    fn list_security_policies(&self, b: &Value) -> Result<AwsResponse, AwsServiceError> {
        let rows: Vec<Value> = SECURITY_POLICIES.iter().map(|p| json!(p)).collect();
        ok(list_response("SecurityPolicyNames", rows, b, &[]))
    }
}

// ===================== web apps =====================

const WEB_APP_FIELDS: &[&str] = &["AccessEndpoint", "WebAppUnits", "WebAppEndpointPolicy"];

impl TransferService {
    fn create_web_app(&self, ctx: &Ctx, b: &Value) -> Result<AwsResponse, AwsServiceError> {
        if b.get("IdentityProviderDetails").is_none() {
            return Err(invalid_request("IdentityProviderDetails is required."));
        }
        let web_app_id = format!("webapp-{}", hex17());
        let w_arn = arn(ctx, &format!("webapp/{web_app_id}"));
        let mut w = Map::new();
        w.insert("Arn".into(), json!(w_arn));
        w.insert("WebAppId".into(), json!(web_app_id));
        w.insert(
            "WebAppEndpoint".into(),
            json!(format!(
                "{web_app_id}.transfer.{}.amazonaws.com",
                ctx.region
            )),
        );
        w.insert("EndpointType".into(), json!("STANDARD"));
        if let Some(idp) = b.get("IdentityProviderDetails") {
            w.insert("DescribedIdentityProviderDetails".into(), idp.clone());
        }
        copy_present(b, &mut w, WEB_APP_FIELDS);
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        data.web_apps.insert(web_app_id.clone(), Value::Object(w));
        store_tags(data, &w_arn, b);
        ok(json!({ "WebAppId": web_app_id }))
    }

    fn update_web_app(&self, ctx: &Ctx, b: &Value) -> Result<AwsResponse, AwsServiceError> {
        let web_app_id = req_str(b, "WebAppId")?.to_string();
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        let w = data
            .web_apps
            .get_mut(&web_app_id)
            .ok_or_else(|| not_found(&format!("Web app {web_app_id} does not exist.")))?;
        let obj = w.as_object_mut().unwrap();
        if let Some(idp) = b.get("IdentityProviderDetails") {
            obj.insert("DescribedIdentityProviderDetails".into(), idp.clone());
        }
        copy_present(b, obj, WEB_APP_FIELDS);
        ok(json!({ "WebAppId": web_app_id }))
    }

    fn describe_web_app(&self, ctx: &Ctx, b: &Value) -> Result<AwsResponse, AwsServiceError> {
        let web_app_id = req_str(b, "WebAppId")?.to_string();
        let guard = self.state.read();
        let data = guard
            .get(&ctx.account)
            .ok_or_else(|| not_found(&format!("Web app {web_app_id} does not exist.")))?;
        let w = data
            .web_apps
            .get(&web_app_id)
            .ok_or_else(|| not_found(&format!("Web app {web_app_id} does not exist.")))?;
        let mut out = w.as_object().unwrap().clone();
        let arn = out
            .get("Arn")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        if let Some(tags) = tags_list(data, &arn) {
            out.insert("Tags".into(), tags);
        }
        ok(json!({ "WebApp": Value::Object(out) }))
    }

    fn delete_web_app(&self, ctx: &Ctx, b: &Value) -> Result<AwsResponse, AwsServiceError> {
        let web_app_id = req_str(b, "WebAppId")?.to_string();
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        let w = data
            .web_apps
            .remove(&web_app_id)
            .ok_or_else(|| not_found(&format!("Web app {web_app_id} does not exist.")))?;
        data.web_app_customizations.remove(&web_app_id);
        if let Some(arn) = w.get("Arn").and_then(Value::as_str) {
            data.tags.remove(arn);
        }
        ok(json!({}))
    }

    fn list_web_apps(&self, ctx: &Ctx, b: &Value) -> Result<AwsResponse, AwsServiceError> {
        let guard = self.state.read();
        let rows: Vec<Value> = guard
            .get(&ctx.account)
            .map(|d| {
                d.web_apps
                    .values()
                    .map(|w| {
                        let mut out = Map::new();
                        for f in [
                            "Arn",
                            "WebAppId",
                            "AccessEndpoint",
                            "WebAppEndpoint",
                            "EndpointType",
                        ] {
                            if let Some(v) = w.get(f) {
                                out.insert(f.to_string(), v.clone());
                            }
                        }
                        Value::Object(out)
                    })
                    .collect()
            })
            .unwrap_or_default();
        ok(list_response("WebApps", rows, b, &[]))
    }

    fn update_web_app_customization(
        &self,
        ctx: &Ctx,
        b: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let web_app_id = req_str(b, "WebAppId")?.to_string();
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        let w = data
            .web_apps
            .get(&web_app_id)
            .ok_or_else(|| not_found(&format!("Web app {web_app_id} does not exist.")))?;
        let arn = w
            .get("Arn")
            .and_then(Value::as_str)
            .unwrap_or("")
            .replace("webapp/", "webapp-customization/");
        let mut cust = Map::new();
        cust.insert("Arn".into(), json!(arn));
        cust.insert("WebAppId".into(), json!(web_app_id));
        copy_present(b, &mut cust, &["Title", "LogoFile", "FaviconFile"]);
        data.web_app_customizations
            .insert(web_app_id.clone(), Value::Object(cust));
        ok(json!({ "WebAppId": web_app_id }))
    }

    fn describe_web_app_customization(
        &self,
        ctx: &Ctx,
        b: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let web_app_id = req_str(b, "WebAppId")?.to_string();
        let guard = self.state.read();
        let data = guard
            .get(&ctx.account)
            .ok_or_else(|| not_found(&format!("Web app {web_app_id} does not exist.")))?;
        if let Some(cust) = data.web_app_customizations.get(&web_app_id) {
            return ok(json!({ "WebAppCustomization": cust }));
        }
        // A web app with no explicit customization still yields the default
        // (empty) customization record.
        let w = data
            .web_apps
            .get(&web_app_id)
            .ok_or_else(|| not_found(&format!("Web app {web_app_id} does not exist.")))?;
        let arn = w
            .get("Arn")
            .and_then(Value::as_str)
            .unwrap_or("")
            .replace("webapp/", "webapp-customization/");
        ok(json!({
            "WebAppCustomization": { "Arn": arn, "WebAppId": web_app_id }
        }))
    }

    fn delete_web_app_customization(
        &self,
        ctx: &Ctx,
        b: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let web_app_id = req_str(b, "WebAppId")?.to_string();
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        if !data.web_apps.contains_key(&web_app_id) {
            return Err(not_found(&format!("Web app {web_app_id} does not exist.")));
        }
        data.web_app_customizations.remove(&web_app_id);
        ok(json!({}))
    }
}

// ===================== identity provider =====================

impl TransferService {
    fn test_identity_provider(&self, ctx: &Ctx, b: &Value) -> Result<AwsResponse, AwsServiceError> {
        let server_id = req_str(b, "ServerId")?.to_string();
        req_str(b, "UserName")?;
        let guard = self.state.read();
        let data = guard
            .get(&ctx.account)
            .ok_or_else(|| not_found(&format!("Server {server_id} does not exist.")))?;
        let srv = self.require_server(data, &server_id)?;
        let idp_type = srv
            .get("IdentityProviderType")
            .and_then(Value::as_str)
            .unwrap_or("SERVICE_MANAGED");
        let url = srv
            .get("IdentityProviderDetails")
            .and_then(|d| d.get("Url"))
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        ok(json!({
            "Response": format!("{{\"IdentityProviderType\":\"{idp_type}\"}}"),
            "StatusCode": 200,
            "Message": "",
            "Url": url
        }))
    }
}

// ===================== tagging =====================

impl TransferService {
    fn tag_resource(&self, ctx: &Ctx, b: &Value) -> Result<AwsResponse, AwsServiceError> {
        let resource_arn = req_str(b, "Arn")?.to_string();
        if b.get("Tags").and_then(Value::as_array).is_none() {
            return Err(invalid_request("Tags is required."));
        }
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        store_tags(data, &resource_arn, b);
        ok(json!({}))
    }

    fn untag_resource(&self, ctx: &Ctx, b: &Value) -> Result<AwsResponse, AwsServiceError> {
        let resource_arn = req_str(b, "Arn")?.to_string();
        let keys: Vec<String> = b
            .get("TagKeys")
            .and_then(Value::as_array)
            .map(|a| {
                a.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .ok_or_else(|| invalid_request("TagKeys is required."))?;
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        if let Some(entry) = data.tags.get_mut(&resource_arn) {
            for k in &keys {
                entry.remove(k);
            }
        }
        ok(json!({}))
    }

    fn list_tags_for_resource(&self, ctx: &Ctx, b: &Value) -> Result<AwsResponse, AwsServiceError> {
        let resource_arn = req_str(b, "Arn")?.to_string();
        let guard = self.state.read();
        let tags: Vec<Value> = guard
            .get(&ctx.account)
            .and_then(|d| d.tags.get(&resource_arn))
            .map(|m| {
                m.iter()
                    .map(|(k, v)| json!({ "Key": k, "Value": v }))
                    .collect()
            })
            .unwrap_or_default();
        ok(json!({ "Arn": resource_arn, "Tags": tags }))
    }
}
