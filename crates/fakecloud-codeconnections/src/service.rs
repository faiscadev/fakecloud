//! AWS CodeConnections (`codeconnections`) awsJson1.0 dispatch + operation
//! handlers.
//!
//! The full 27-operation control plane from the AWS Smithy model: connections,
//! self-managed hosts, repository links, sync configurations, the sync-status /
//! sync-blocker read surface, and resource tagging. State is account-
//! partitioned and persisted. There is no backing source-provider handshake or
//! Git-sync engine, so connections stay `PENDING`, and the sync-status /
//! sync-blocker read paths report "not found" for resources that were never
//! synced (the honest AWS shape), while every other operation is real,
//! persisted CRUD.

use std::sync::Arc;

use async_trait::async_trait;
use http::StatusCode;
use serde_json::{json, Map, Value};
use tokio::sync::Mutex as AsyncMutex;

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsService, AwsServiceError};
use fakecloud_persistence::SnapshotStore;

use crate::persistence::save_snapshot;
use crate::state::{
    skey, ConnectionRecord, HostRecord, RepositoryLinkRecord, SharedCodeConnectionsState,
    SyncConfigurationRecord, TagMap,
};

/// Every operation name in the CodeConnections Smithy model.
pub const CODECONNECTIONS_ACTIONS: &[&str] = &[
    "CreateConnection",
    "CreateHost",
    "CreateRepositoryLink",
    "CreateSyncConfiguration",
    "DeleteConnection",
    "DeleteHost",
    "DeleteRepositoryLink",
    "DeleteSyncConfiguration",
    "GetConnection",
    "GetHost",
    "GetRepositoryLink",
    "GetRepositorySyncStatus",
    "GetResourceSyncStatus",
    "GetSyncBlockerSummary",
    "GetSyncConfiguration",
    "ListConnections",
    "ListHosts",
    "ListRepositoryLinks",
    "ListRepositorySyncDefinitions",
    "ListSyncConfigurations",
    "ListTagsForResource",
    "TagResource",
    "UntagResource",
    "UpdateHost",
    "UpdateRepositoryLink",
    "UpdateSyncBlocker",
    "UpdateSyncConfiguration",
];

/// Valid `ProviderType` enum values.
const PROVIDER_TYPES: &[&str] = &[
    "Bitbucket",
    "GitHub",
    "GitHubEnterpriseServer",
    "GitLab",
    "GitLabSelfManaged",
    "AzureDevOps",
];

/// Valid `SyncConfigurationType` enum values.
const SYNC_TYPES: &[&str] = &["CFN_STACK_SYNC"];

pub struct CodeConnectionsService {
    state: SharedCodeConnectionsState,
    snapshot_store: Option<Arc<dyn SnapshotStore>>,
    snapshot_lock: Arc<AsyncMutex<()>>,
}

impl CodeConnectionsService {
    pub fn new(state: SharedCodeConnectionsState) -> Self {
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

    /// Persist hook for the CloudFormation provisioner; `None` in memory mode.
    pub fn snapshot_hook(&self) -> Option<fakecloud_persistence::SnapshotHook> {
        let store = self.snapshot_store.clone()?;
        let state = self.state.clone();
        let lock = self.snapshot_lock.clone();
        Some(Arc::new(move || {
            let state = state.clone();
            let store = store.clone();
            let lock = lock.clone();
            Box::pin(async move {
                save_snapshot(&state, Some(store), &lock).await;
            })
        }))
    }
}

#[async_trait]
impl AwsService for CodeConnectionsService {
    fn service_name(&self) -> &str {
        "codeconnections"
    }

    async fn handle(&self, req: AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let mutates = is_mutating(req.action.as_str());
        let result = dispatch(self, &req);
        if mutates && matches!(result.as_ref(), Ok(resp) if resp.status.is_success()) {
            self.save().await;
        }
        result
    }

    fn supported_actions(&self) -> &[&str] {
        CODECONNECTIONS_ACTIONS
    }
}

fn is_mutating(action: &str) -> bool {
    action.starts_with("Create")
        || action.starts_with("Delete")
        || action.starts_with("Update")
        || action == "TagResource"
        || action == "UntagResource"
}

fn dispatch(s: &CodeConnectionsService, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
    let body = parse(req)?;
    match req.action.as_str() {
        "CreateConnection" => s.create_connection(req, &body),
        "CreateHost" => s.create_host(req, &body),
        "CreateRepositoryLink" => s.create_repository_link(req, &body),
        "CreateSyncConfiguration" => s.create_sync_configuration(req, &body),
        "DeleteConnection" => s.delete_connection(req, &body),
        "DeleteHost" => s.delete_host(req, &body),
        "DeleteRepositoryLink" => s.delete_repository_link(req, &body),
        "DeleteSyncConfiguration" => s.delete_sync_configuration(req, &body),
        "GetConnection" => s.get_connection(req, &body),
        "GetHost" => s.get_host(req, &body),
        "GetRepositoryLink" => s.get_repository_link(req, &body),
        "GetRepositorySyncStatus" => s.get_repository_sync_status(req, &body),
        "GetResourceSyncStatus" => s.get_resource_sync_status(req, &body),
        "GetSyncBlockerSummary" => s.get_sync_blocker_summary(req, &body),
        "GetSyncConfiguration" => s.get_sync_configuration(req, &body),
        "ListConnections" => s.list_connections(req, &body),
        "ListHosts" => s.list_hosts(req, &body),
        "ListRepositoryLinks" => s.list_repository_links(req, &body),
        "ListRepositorySyncDefinitions" => s.list_repository_sync_definitions(req, &body),
        "ListSyncConfigurations" => s.list_sync_configurations(req, &body),
        "ListTagsForResource" => s.list_tags_for_resource(req, &body),
        "TagResource" => s.tag_resource(req, &body),
        "UntagResource" => s.untag_resource(req, &body),
        "UpdateHost" => s.update_host(req, &body),
        "UpdateRepositoryLink" => s.update_repository_link(req, &body),
        "UpdateSyncBlocker" => s.update_sync_blocker(req, &body),
        "UpdateSyncConfiguration" => s.update_sync_configuration(req, &body),
        _ => Err(AwsServiceError::action_not_implemented(
            s.service_name(),
            &req.action,
        )),
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn ok(v: Value) -> Result<AwsResponse, AwsServiceError> {
    Ok(AwsResponse::json_value(StatusCode::OK, v))
}

fn parse(req: &AwsRequest) -> Result<Value, AwsServiceError> {
    if req.body.is_empty() {
        return Ok(json!({}));
    }
    serde_json::from_slice(&req.body)
        .map_err(|e| validation(format!("Request body is malformed: {e}")))
}

fn validation(msg: impl Into<String>) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::BAD_REQUEST, "ValidationException", msg)
}
fn invalid_input(msg: impl Into<String>) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::BAD_REQUEST, "InvalidInputException", msg)
}
fn not_found(msg: impl Into<String>) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::NOT_FOUND, "ResourceNotFoundException", msg)
}
fn already_exists(msg: impl Into<String>) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        "ResourceAlreadyExistsException",
        msg,
    )
}

fn req_str<'a>(b: &'a Value, f: &str) -> Result<&'a str, AwsServiceError> {
    b.get(f)
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| validation(format!("{f} is a required argument.")))
}

/// A required identifier field that must be *present* but whose `@length`
/// minimum is 0 (ARNs, resource names used only for lookup). The empty string
/// is a valid present value: it simply resolves to a non-existent resource, so
/// the handler returns the operation's declared "not found" error rather than a
/// (undeclared) `ValidationException`.
fn id_field(b: &Value, f: &str) -> Result<String, AwsServiceError> {
    b.get(f)
        .and_then(Value::as_str)
        .map(str::to_string)
        .ok_or_else(|| validation(format!("{f} is a required argument.")))
}

/// Enforce a string field's `@length` bounds (both min and max), so
/// too-long / too-short negative variants are rejected with a client error.
fn check_len(b: &Value, field: &str, min: usize, max: usize) -> Result<(), AwsServiceError> {
    if let Some(s) = b.get(field).and_then(Value::as_str) {
        let n = s.chars().count();
        if n < min || n > max {
            return Err(validation(format!(
                "{field} must have length between {min} and {max}, inclusive."
            )));
        }
    }
    Ok(())
}

fn opt_str(b: &Value, f: &str) -> Option<String> {
    b.get(f)
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
}

fn check_enum(b: &Value, field: &str, allowed: &[&str]) -> Result<(), AwsServiceError> {
    if let Some(s) = b.get(field).and_then(Value::as_str) {
        if !allowed.contains(&s) {
            return Err(validation(format!(
                "{field} must be one of [{}].",
                allowed.join(", ")
            )));
        }
    }
    Ok(())
}

fn gen_uuid() -> String {
    uuid::Uuid::new_v4().to_string()
}

fn connection_arn(region: &str, account: &str, id: &str) -> String {
    format!("arn:aws:codeconnections:{region}:{account}:connection/{id}")
}
fn host_arn(region: &str, account: &str, id: &str) -> String {
    format!("arn:aws:codeconnections:{region}:{account}:host/{id}")
}
fn repository_link_arn(region: &str, account: &str, id: &str) -> String {
    format!("arn:aws:codeconnections:{region}:{account}:repository-link/{id}")
}

/// Parse a `Tags` list (`[{Key,Value}]`) into a map.
fn parse_tags(b: &Value) -> TagMap {
    let mut out = TagMap::new();
    if let Some(arr) = b.get("Tags").and_then(Value::as_array) {
        for t in arr {
            if let (Some(k), Some(v)) = (
                t.get("Key").and_then(Value::as_str),
                t.get("Value").and_then(Value::as_str),
            ) {
                out.insert(k.to_string(), v.to_string());
            }
        }
    }
    out
}

fn tags_value(t: &TagMap) -> Value {
    Value::Array(
        t.iter()
            .map(|(k, v)| json!({ "Key": k, "Value": v }))
            .collect(),
    )
}

// ---------------------------------------------------------------------------
// Value builders
// ---------------------------------------------------------------------------

fn connection_value(r: &ConnectionRecord) -> Value {
    let mut m = Map::new();
    m.insert("ConnectionName".into(), json!(r.connection_name));
    m.insert("ConnectionArn".into(), json!(r.connection_arn));
    if let Some(pt) = &r.provider_type {
        m.insert("ProviderType".into(), json!(pt));
    }
    m.insert("OwnerAccountId".into(), json!(r.owner_account_id));
    m.insert("ConnectionStatus".into(), json!(r.connection_status));
    if let Some(h) = &r.host_arn {
        m.insert("HostArn".into(), json!(h));
    }
    Value::Object(m)
}

fn repository_link_info(r: &RepositoryLinkRecord) -> Value {
    let mut m = Map::new();
    m.insert("ConnectionArn".into(), json!(r.connection_arn));
    if let Some(k) = &r.encryption_key_arn {
        m.insert("EncryptionKeyArn".into(), json!(k));
    }
    m.insert("OwnerId".into(), json!(r.owner_id));
    m.insert("ProviderType".into(), json!(r.provider_type));
    m.insert("RepositoryLinkArn".into(), json!(r.repository_link_arn));
    m.insert("RepositoryLinkId".into(), json!(r.repository_link_id));
    m.insert("RepositoryName".into(), json!(r.repository_name));
    Value::Object(m)
}

fn sync_configuration_value(r: &SyncConfigurationRecord) -> Value {
    let mut m = Map::new();
    m.insert("Branch".into(), json!(r.branch));
    if let Some(c) = &r.config_file {
        m.insert("ConfigFile".into(), json!(c));
    }
    m.insert("OwnerId".into(), json!(r.owner_id));
    m.insert("ProviderType".into(), json!(r.provider_type));
    m.insert("RepositoryLinkId".into(), json!(r.repository_link_id));
    m.insert("RepositoryName".into(), json!(r.repository_name));
    m.insert("ResourceName".into(), json!(r.resource_name));
    m.insert("RoleArn".into(), json!(r.role_arn));
    m.insert("SyncType".into(), json!(r.sync_type));
    if let Some(v) = &r.publish_deployment_status {
        m.insert("PublishDeploymentStatus".into(), json!(v));
    }
    if let Some(v) = &r.trigger_resource_update_on {
        m.insert("TriggerResourceUpdateOn".into(), json!(v));
    }
    if let Some(v) = &r.pull_request_comment {
        m.insert("PullRequestComment".into(), json!(v));
    }
    Value::Object(m)
}

/// Numeric-offset pagination reading `MaxResults`/`NextToken` from the body.
///
/// `MaxResults` is bounded `[0, 100]` per the model's `@range` trait (min 0),
/// so an out-of-range value is rejected. An explicit `MaxResults` (including 0)
/// is honored literally; only an *absent* `MaxResults` falls back to the
/// service default page size. `NextToken` is an opaque numeric offset: an
/// unparseable token is tolerated as "start from the beginning" rather than
/// raising an (undeclared) error, matching the model's error contract for the
/// list operations.
fn page(
    items: Vec<Value>,
    list_key: &str,
    body: &Value,
    token_max: usize,
) -> Result<AwsResponse, AwsServiceError> {
    check_len(body, "NextToken", 1, token_max)?;
    // An explicit MaxResults (including the model-valid 0) is honored as-is; an
    // absent one uses the default page size. No falsy-zero coalescing: 0 means
    // "return zero items this page", not "use the default". A present-but-not-
    // integer value (for example the JSON float 50.0) is rejected rather than
    // silently defaulting: as_i64 returns None for a float, so validate against
    // the model's @range on the raw JSON number instead of coalescing to the
    // default page size.
    let max = match body.get("MaxResults") {
        None | Some(Value::Null) => 100,
        Some(v) => {
            let n = v.as_i64().ok_or_else(|| {
                validation("MaxResults must be an integer between 0 and 100, inclusive.")
            })?;
            if !(0..=100).contains(&n) {
                return Err(validation(
                    "MaxResults must be between 0 and 100, inclusive.",
                ));
            }
            n as usize
        }
    };
    let start = body
        .get("NextToken")
        .and_then(Value::as_str)
        .and_then(|t| t.parse::<usize>().ok())
        .unwrap_or(0)
        .min(items.len());
    let end = start.saturating_add(max).min(items.len());
    let mut out = Map::new();
    out.insert(
        list_key.to_string(),
        Value::Array(items[start..end].to_vec()),
    );
    if end < items.len() {
        out.insert("NextToken".to_string(), Value::String(end.to_string()));
    }
    ok(Value::Object(out))
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

impl CodeConnectionsService {
    fn create_connection(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = req_str(body, "ConnectionName")?.to_string();
        check_len(body, "ConnectionName", 1, 32)?;
        check_len(body, "HostArn", 0, 256)?;
        check_enum(body, "ProviderType", PROVIDER_TYPES)?;
        let host_arn = opt_str(body, "HostArn");
        let tags = parse_tags(body);
        let id = gen_uuid();
        let arn = connection_arn(&req.region, &req.account_id, &id);

        // The provider type is carried either directly (ProviderType) or by a
        // referenced host. A supplied HostArn must resolve to an existing host,
        // and the connection inherits that host's provider type when no explicit
        // ProviderType is given.
        let mut provider_type = opt_str(body, "ProviderType");
        if let Some(h) = &host_arn {
            let host = self
                .state
                .read()
                .get(&req.account_id)
                .and_then(|st| st.hosts.get(h).cloned())
                .ok_or_else(|| not_found(format!("Host {h} does not exist.")))?;
            if provider_type.is_none() {
                provider_type = Some(host.provider_type);
            }
        }

        // A connection must resolve to a provider type: either ProviderType is
        // given directly, or a HostArn supplies it. Neither is invalid input.
        if provider_type.is_none() {
            return Err(invalid_input(
                "Either ProviderType or HostArn must be specified.",
            ));
        }

        let record = ConnectionRecord {
            connection_arn: arn.clone(),
            connection_name: name,
            provider_type,
            owner_account_id: req.account_id.clone(),
            connection_status: "PENDING".into(),
            host_arn,
            tags: tags.clone(),
        };
        self.state
            .write()
            .get_or_create(&req.account_id)
            .connections
            .insert(arn.clone(), record);
        ok(json!({ "ConnectionArn": arn, "Tags": tags_value(&tags) }))
    }

    fn create_host(&self, req: &AwsRequest, body: &Value) -> Result<AwsResponse, AwsServiceError> {
        let name = req_str(body, "Name")?.to_string();
        check_len(body, "Name", 1, 64)?;
        let provider_type = req_str(body, "ProviderType")?.to_string();
        check_enum(body, "ProviderType", PROVIDER_TYPES)?;
        let provider_endpoint = req_str(body, "ProviderEndpoint")?.to_string();
        check_len(body, "ProviderEndpoint", 1, 512)?;
        let vpc_configuration = body.get("VpcConfiguration").cloned();
        let tags = parse_tags(body);
        let id = gen_uuid();
        let arn = host_arn(&req.region, &req.account_id, &id);

        let record = HostRecord {
            host_arn: arn.clone(),
            name,
            provider_type,
            provider_endpoint,
            status: "PENDING".into(),
            vpc_configuration,
            tags: tags.clone(),
        };
        self.state
            .write()
            .get_or_create(&req.account_id)
            .hosts
            .insert(arn.clone(), record);
        ok(json!({ "HostArn": arn, "Tags": tags_value(&tags) }))
    }

    fn create_repository_link(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let connection_arn = id_field(body, "ConnectionArn")?;
        let owner_id = req_str(body, "OwnerId")?.to_string();
        let repository_name = req_str(body, "RepositoryName")?.to_string();
        let encryption_key_arn = opt_str(body, "EncryptionKeyArn");
        let tags = parse_tags(body);

        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        // The connection must exist and resolve to a provider type; the link's
        // ProviderType is inherited from the connection (or its host).
        let connection = st
            .connections
            .get(&connection_arn)
            .cloned()
            .ok_or_else(|| invalid_input(format!("Connection {connection_arn} does not exist.")))?;
        let provider_type = connection
            .provider_type
            .clone()
            .or_else(|| {
                connection
                    .host_arn
                    .as_ref()
                    .and_then(|h| st.hosts.get(h))
                    .map(|h| h.provider_type.clone())
            })
            .ok_or_else(|| {
                invalid_input(format!(
                    "Connection {connection_arn} is not associated with a provider type."
                ))
            })?;

        // Refuse a duplicate link for the same connection + repository.
        if st.repository_links.values().any(|l| {
            l.connection_arn == connection_arn
                && l.owner_id == owner_id
                && l.repository_name == repository_name
        }) {
            return Err(already_exists(format!(
                "A repository link for {owner_id}/{repository_name} already exists."
            )));
        }

        let id = gen_uuid();
        let arn = repository_link_arn(&req.region, &req.account_id, &id);
        let record = RepositoryLinkRecord {
            repository_link_id: id.clone(),
            repository_link_arn: arn,
            connection_arn,
            owner_id,
            repository_name,
            provider_type,
            encryption_key_arn,
            tags,
        };
        let value = repository_link_info(&record);
        st.repository_links.insert(id, record);
        ok(json!({ "RepositoryLinkInfo": value }))
    }

    fn create_sync_configuration(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let branch = req_str(body, "Branch")?.to_string();
        let config_file = req_str(body, "ConfigFile")?.to_string();
        let repository_link_id = req_str(body, "RepositoryLinkId")?.to_string();
        let resource_name = req_str(body, "ResourceName")?.to_string();
        let role_arn = req_str(body, "RoleArn")?.to_string();
        let sync_type = req_str(body, "SyncType")?.to_string();
        // Enforce the model's @length bounds on every sync-config field that
        // declares one, so a create cannot store a value its own delete/update
        // path would later reject (create/delete asymmetry). ConfigFile
        // (DeploymentFilePath) and RepositoryLinkId carry no @length.
        check_len(body, "ResourceName", 1, 100)?;
        check_len(body, "Branch", 1, 255)?;
        check_len(body, "RoleArn", 1, 1024)?;
        check_enum(body, "SyncType", SYNC_TYPES)?;
        check_enum(body, "PublishDeploymentStatus", &["ENABLED", "DISABLED"])?;
        check_enum(
            body,
            "TriggerResourceUpdateOn",
            &["ANY_CHANGE", "FILE_CHANGE"],
        )?;
        check_enum(body, "PullRequestComment", &["ENABLED", "DISABLED"])?;

        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let link = st
            .repository_links
            .get(&repository_link_id)
            .cloned()
            .ok_or_else(|| {
                invalid_input(format!(
                    "Repository link {repository_link_id} does not exist."
                ))
            })?;

        let key = skey(&sync_type, &resource_name);
        if st.sync_configurations.contains_key(&key) {
            return Err(already_exists(format!(
                "A sync configuration for {resource_name} already exists."
            )));
        }

        let record = SyncConfigurationRecord {
            branch,
            config_file: Some(config_file),
            owner_id: link.owner_id,
            provider_type: link.provider_type,
            repository_link_id,
            repository_name: link.repository_name,
            resource_name,
            role_arn,
            sync_type,
            publish_deployment_status: opt_str(body, "PublishDeploymentStatus"),
            trigger_resource_update_on: opt_str(body, "TriggerResourceUpdateOn"),
            pull_request_comment: opt_str(body, "PullRequestComment"),
        };
        let value = sync_configuration_value(&record);
        st.sync_configurations.insert(key, record);
        ok(json!({ "SyncConfiguration": value }))
    }

    fn delete_connection(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let arn = id_field(body, "ConnectionArn")?;
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        if st.connections.remove(&arn).is_none() {
            return Err(not_found(format!("Connection {arn} does not exist.")));
        }
        ok(json!({}))
    }

    fn delete_host(&self, req: &AwsRequest, body: &Value) -> Result<AwsResponse, AwsServiceError> {
        let arn = id_field(body, "HostArn")?;
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        if st.hosts.remove(&arn).is_none() {
            return Err(not_found(format!("Host {arn} does not exist.")));
        }
        ok(json!({}))
    }

    fn delete_repository_link(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let id = req_str(body, "RepositoryLinkId")?.to_string();
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        if !st.repository_links.contains_key(&id) {
            return Err(not_found(format!("Repository link {id} does not exist.")));
        }
        // A repository link cannot be deleted while sync configurations still
        // reference it.
        if st
            .sync_configurations
            .values()
            .any(|c| c.repository_link_id == id)
        {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "SyncConfigurationStillExistsException",
                format!("Repository link {id} still has sync configurations."),
            ));
        }
        st.repository_links.remove(&id);
        ok(json!({}))
    }

    fn delete_sync_configuration(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let sync_type = req_str(body, "SyncType")?.to_string();
        let resource_name = req_str(body, "ResourceName")?.to_string();
        check_len(body, "ResourceName", 1, 100)?;
        check_enum(body, "SyncType", SYNC_TYPES)?;
        // DeleteSyncConfiguration is idempotent: the model declares no
        // resource-not-found error, so a delete of an absent configuration
        // succeeds as a no-op.
        self.state
            .write()
            .get_or_create(&req.account_id)
            .sync_configurations
            .remove(&skey(&sync_type, &resource_name));
        ok(json!({}))
    }

    fn get_connection(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let arn = id_field(body, "ConnectionArn")?;
        let accounts = self.state.read();
        let conn = accounts
            .get(&req.account_id)
            .and_then(|st| st.connections.get(&arn))
            .ok_or_else(|| not_found(format!("Connection {arn} does not exist.")))?;
        ok(json!({ "Connection": connection_value(conn) }))
    }

    fn get_host(&self, req: &AwsRequest, body: &Value) -> Result<AwsResponse, AwsServiceError> {
        let arn = id_field(body, "HostArn")?;
        let accounts = self.state.read();
        let host = accounts
            .get(&req.account_id)
            .and_then(|st| st.hosts.get(&arn))
            .ok_or_else(|| not_found(format!("Host {arn} does not exist.")))?;
        let mut m = Map::new();
        m.insert("Name".into(), json!(host.name));
        m.insert("Status".into(), json!(host.status));
        m.insert("ProviderType".into(), json!(host.provider_type));
        m.insert("ProviderEndpoint".into(), json!(host.provider_endpoint));
        if let Some(vpc) = &host.vpc_configuration {
            m.insert("VpcConfiguration".into(), vpc.clone());
        }
        ok(Value::Object(m))
    }

    fn get_repository_link(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let id = req_str(body, "RepositoryLinkId")?.to_string();
        let accounts = self.state.read();
        let link = accounts
            .get(&req.account_id)
            .and_then(|st| st.repository_links.get(&id))
            .ok_or_else(|| not_found(format!("Repository link {id} does not exist.")))?;
        ok(json!({ "RepositoryLinkInfo": repository_link_info(link) }))
    }

    fn get_repository_sync_status(
        &self,
        _req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        req_str(body, "Branch")?;
        req_str(body, "RepositoryLinkId")?;
        req_str(body, "SyncType")?;
        check_enum(body, "SyncType", SYNC_TYPES)?;
        // fakecloud runs no Git-sync engine, so no repository sync has ever been
        // attempted, so there is no latest-sync record to report.
        Err(not_found(
            "No repository sync status found for the requested repository link.",
        ))
    }

    fn get_resource_sync_status(
        &self,
        _req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        req_str(body, "ResourceName")?;
        req_str(body, "SyncType")?;
        check_enum(body, "SyncType", SYNC_TYPES)?;
        Err(not_found(
            "No resource sync status found for the requested resource.",
        ))
    }

    fn get_sync_blocker_summary(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let sync_type = req_str(body, "SyncType")?.to_string();
        let resource_name = req_str(body, "ResourceName")?.to_string();
        check_enum(body, "SyncType", SYNC_TYPES)?;
        let accounts = self.state.read();
        let exists = accounts.get(&req.account_id).is_some_and(|st| {
            st.sync_configurations
                .contains_key(&skey(&sync_type, &resource_name))
        });
        if !exists {
            return Err(not_found(format!(
                "No sync configuration found for {resource_name}."
            )));
        }
        // No Git-sync engine runs, so there are no active sync blockers.
        ok(json!({
            "SyncBlockerSummary": {
                "ResourceName": resource_name,
                "LatestBlockers": [],
            }
        }))
    }

    fn get_sync_configuration(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let sync_type = req_str(body, "SyncType")?.to_string();
        let resource_name = req_str(body, "ResourceName")?.to_string();
        check_enum(body, "SyncType", SYNC_TYPES)?;
        let accounts = self.state.read();
        let cfg = accounts
            .get(&req.account_id)
            .and_then(|st| {
                st.sync_configurations
                    .get(&skey(&sync_type, &resource_name))
            })
            .ok_or_else(|| {
                not_found(format!("No sync configuration found for {resource_name}."))
            })?;
        ok(json!({ "SyncConfiguration": sync_configuration_value(cfg) }))
    }

    fn list_connections(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        check_enum(body, "ProviderTypeFilter", PROVIDER_TYPES)?;
        check_len(body, "HostArnFilter", 0, 256)?;
        let provider_filter = opt_str(body, "ProviderTypeFilter");
        let host_filter = opt_str(body, "HostArnFilter");
        let accounts = self.state.read();
        let items: Vec<Value> = accounts
            .get(&req.account_id)
            .map(|st| {
                st.connections
                    .values()
                    .filter(|c| {
                        provider_filter
                            .as_deref()
                            .is_none_or(|p| c.provider_type.as_deref() == Some(p))
                    })
                    .filter(|c| {
                        host_filter
                            .as_deref()
                            .is_none_or(|h| c.host_arn.as_deref() == Some(h))
                    })
                    .map(connection_value)
                    .collect()
            })
            .unwrap_or_default();
        page(items, "Connections", body, 1024)
    }

    fn list_hosts(&self, req: &AwsRequest, body: &Value) -> Result<AwsResponse, AwsServiceError> {
        let accounts = self.state.read();
        let items: Vec<Value> = accounts
            .get(&req.account_id)
            .map(|st| {
                st.hosts
                    .values()
                    .map(|h| {
                        let mut m = Map::new();
                        m.insert("Name".into(), json!(h.name));
                        m.insert("HostArn".into(), json!(h.host_arn));
                        m.insert("ProviderType".into(), json!(h.provider_type));
                        m.insert("ProviderEndpoint".into(), json!(h.provider_endpoint));
                        m.insert("Status".into(), json!(h.status));
                        if let Some(vpc) = &h.vpc_configuration {
                            m.insert("VpcConfiguration".into(), vpc.clone());
                        }
                        Value::Object(m)
                    })
                    .collect()
            })
            .unwrap_or_default();
        page(items, "Hosts", body, 1024)
    }

    fn list_repository_links(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let accounts = self.state.read();
        let items: Vec<Value> = accounts
            .get(&req.account_id)
            .map(|st| {
                st.repository_links
                    .values()
                    .map(repository_link_info)
                    .collect()
            })
            .unwrap_or_default();
        page(items, "RepositoryLinks", body, 2048)
    }

    fn list_repository_sync_definitions(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let id = req_str(body, "RepositoryLinkId")?.to_string();
        req_str(body, "SyncType")?;
        check_enum(body, "SyncType", SYNC_TYPES)?;
        let accounts = self.state.read();
        let exists = accounts
            .get(&req.account_id)
            .is_some_and(|st| st.repository_links.contains_key(&id));
        if !exists {
            return Err(not_found(format!("Repository link {id} does not exist.")));
        }
        // No sync definitions exist without a running Git-sync engine.
        ok(json!({ "RepositorySyncDefinitions": [] }))
    }

    fn list_sync_configurations(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let id = req_str(body, "RepositoryLinkId")?.to_string();
        let sync_type = req_str(body, "SyncType")?.to_string();
        check_enum(body, "SyncType", SYNC_TYPES)?;
        let accounts = self.state.read();
        let st = accounts
            .get(&req.account_id)
            .filter(|st| st.repository_links.contains_key(&id))
            .ok_or_else(|| not_found(format!("Repository link {id} does not exist.")))?;
        let items: Vec<Value> = st
            .sync_configurations
            .values()
            .filter(|c| c.repository_link_id == id && c.sync_type == sync_type)
            .map(sync_configuration_value)
            .collect();
        page(items, "SyncConfigurations", body, 2048)
    }

    fn list_tags_for_resource(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let arn = req_str(body, "ResourceArn")?.to_string();
        let accounts = self.state.read();
        let st = accounts
            .get(&req.account_id)
            .ok_or_else(|| not_found(format!("Resource {arn} does not exist.")))?;
        let tags = resolve_tags(st, &arn)
            .ok_or_else(|| not_found(format!("Resource {arn} does not exist.")))?;
        ok(json!({ "Tags": tags_value(&tags) }))
    }

    fn tag_resource(&self, req: &AwsRequest, body: &Value) -> Result<AwsResponse, AwsServiceError> {
        let arn = req_str(body, "ResourceArn")?.to_string();
        let new_tags = parse_tags(body);
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let tags = resolve_tags_mut(st, &arn)
            .ok_or_else(|| not_found(format!("Resource {arn} does not exist.")))?;
        tags.extend(new_tags);
        ok(json!({}))
    }

    fn untag_resource(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let arn = req_str(body, "ResourceArn")?.to_string();
        let keys: Vec<String> = body
            .get("TagKeys")
            .and_then(Value::as_array)
            .map(|a| {
                a.iter()
                    .filter_map(|x| x.as_str().map(str::to_string))
                    .collect()
            })
            .unwrap_or_default();
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let tags = resolve_tags_mut(st, &arn)
            .ok_or_else(|| not_found(format!("Resource {arn} does not exist.")))?;
        for k in &keys {
            tags.remove(k);
        }
        ok(json!({}))
    }

    fn update_host(&self, req: &AwsRequest, body: &Value) -> Result<AwsResponse, AwsServiceError> {
        let arn = id_field(body, "HostArn")?;
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let host = st
            .hosts
            .get_mut(&arn)
            .ok_or_else(|| not_found(format!("Host {arn} does not exist.")))?;
        if let Some(endpoint) = opt_str(body, "ProviderEndpoint") {
            host.provider_endpoint = endpoint;
        }
        if let Some(vpc) = body.get("VpcConfiguration") {
            host.vpc_configuration = Some(vpc.clone());
        }
        ok(json!({}))
    }

    fn update_repository_link(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let id = req_str(body, "RepositoryLinkId")?.to_string();
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        if !st.repository_links.contains_key(&id) {
            return Err(not_found(format!("Repository link {id} does not exist.")));
        }
        // Repointing the link at a new connection must reference an existing
        // connection, and the link re-derives its provider type from that new
        // connection (as CreateRepositoryLink does). Resolve it up front with
        // immutable reads so it doesn't overlap the mutable link borrow below.
        let new_connection = match opt_str(body, "ConnectionArn") {
            Some(arn) => {
                let connection =
                    st.connections.get(&arn).cloned().ok_or_else(|| {
                        invalid_input(format!("Connection {arn} does not exist."))
                    })?;
                let provider_type = connection
                    .provider_type
                    .clone()
                    .or_else(|| {
                        connection
                            .host_arn
                            .as_ref()
                            .and_then(|h| st.hosts.get(h))
                            .map(|h| h.provider_type.clone())
                    })
                    .ok_or_else(|| {
                        invalid_input(format!(
                            "Connection {arn} is not associated with a provider type."
                        ))
                    })?;
                Some((arn, provider_type))
            }
            None => None,
        };

        let link = st
            .repository_links
            .get_mut(&id)
            .expect("repository link existence checked above");
        if let Some((arn, provider_type)) = new_connection {
            link.connection_arn = arn;
            link.provider_type = provider_type;
        }
        if let Some(key) = opt_str(body, "EncryptionKeyArn") {
            link.encryption_key_arn = Some(key);
        }
        let value = repository_link_info(link);
        ok(json!({ "RepositoryLinkInfo": value }))
    }

    fn update_sync_blocker(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let id = req_str(body, "Id")?.to_string();
        let sync_type = req_str(body, "SyncType")?.to_string();
        let resource_name = req_str(body, "ResourceName")?.to_string();
        req_str(body, "ResolvedReason")?;
        check_enum(body, "SyncType", SYNC_TYPES)?;
        let accounts = self.state.read();
        let exists = accounts.get(&req.account_id).is_some_and(|st| {
            st.sync_configurations
                .contains_key(&skey(&sync_type, &resource_name))
        });
        if !exists {
            return Err(not_found(format!(
                "No sync configuration found for {resource_name}."
            )));
        }
        // No Git-sync engine runs, so no sync blocker with this id can exist.
        Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "SyncBlockerDoesNotExistException",
            format!("Sync blocker {id} does not exist."),
        ))
    }

    fn update_sync_configuration(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let resource_name = req_str(body, "ResourceName")?.to_string();
        let sync_type = req_str(body, "SyncType")?.to_string();
        // Enforce the model's @length bounds on the mutable sync-config fields
        // that declare one, symmetric with CreateSyncConfiguration. Branch and
        // RoleArn are optional on update, so they are only checked when present
        // (check_len is a no-op for absent fields).
        check_len(body, "ResourceName", 1, 100)?;
        check_len(body, "Branch", 1, 255)?;
        check_len(body, "RoleArn", 1, 1024)?;
        check_enum(body, "SyncType", SYNC_TYPES)?;
        check_enum(body, "PublishDeploymentStatus", &["ENABLED", "DISABLED"])?;
        check_enum(
            body,
            "TriggerResourceUpdateOn",
            &["ANY_CHANGE", "FILE_CHANGE"],
        )?;
        check_enum(body, "PullRequestComment", &["ENABLED", "DISABLED"])?;

        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        // Resolve any new repository link up-front (immutable borrow) so the
        // mutable borrow of the configuration below doesn't overlap.
        let new_link = match opt_str(body, "RepositoryLinkId") {
            Some(link_id) => Some(st.repository_links.get(&link_id).cloned().ok_or_else(|| {
                invalid_input(format!("Repository link {link_id} does not exist."))
            })?),
            None => None,
        };

        let cfg = st
            .sync_configurations
            .get_mut(&skey(&sync_type, &resource_name))
            .ok_or_else(|| {
                not_found(format!("No sync configuration found for {resource_name}."))
            })?;
        if let Some(branch) = opt_str(body, "Branch") {
            cfg.branch = branch;
        }
        if let Some(config_file) = opt_str(body, "ConfigFile") {
            cfg.config_file = Some(config_file);
        }
        if let Some(role_arn) = opt_str(body, "RoleArn") {
            cfg.role_arn = role_arn;
        }
        if let Some(link) = new_link {
            cfg.repository_link_id = link.repository_link_id;
            cfg.repository_name = link.repository_name;
            cfg.owner_id = link.owner_id;
            cfg.provider_type = link.provider_type;
        }
        if let Some(v) = opt_str(body, "PublishDeploymentStatus") {
            cfg.publish_deployment_status = Some(v);
        }
        if let Some(v) = opt_str(body, "TriggerResourceUpdateOn") {
            cfg.trigger_resource_update_on = Some(v);
        }
        if let Some(v) = opt_str(body, "PullRequestComment") {
            cfg.pull_request_comment = Some(v);
        }
        let value = sync_configuration_value(cfg);
        ok(json!({ "SyncConfiguration": value }))
    }
}

/// Resolve a taggable resource's tag map by ARN (connection, host, or
/// repository link), returning a clone.
fn resolve_tags(st: &crate::state::CodeConnectionsState, arn: &str) -> Option<TagMap> {
    if let Some(c) = st.connections.get(arn) {
        return Some(c.tags.clone());
    }
    if let Some(h) = st.hosts.get(arn) {
        return Some(h.tags.clone());
    }
    st.repository_links
        .values()
        .find(|l| l.repository_link_arn == arn)
        .map(|l| l.tags.clone())
}

/// Resolve a taggable resource's tag map by ARN, returning a mutable reference.
fn resolve_tags_mut<'a>(
    st: &'a mut crate::state::CodeConnectionsState,
    arn: &str,
) -> Option<&'a mut TagMap> {
    if st.connections.contains_key(arn) {
        return st.connections.get_mut(arn).map(|c| &mut c.tags);
    }
    if st.hosts.contains_key(arn) {
        return st.hosts.get_mut(arn).map(|h| &mut h.tags);
    }
    st.repository_links
        .values_mut()
        .find(|l| l.repository_link_arn == arn)
        .map(|l| &mut l.tags)
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use fakecloud_core::multi_account::MultiAccountState;
    use http::{HeaderMap, Method};
    use parking_lot::{Mutex, RwLock};
    use std::collections::HashMap;

    fn svc() -> CodeConnectionsService {
        CodeConnectionsService::new(Arc::new(RwLock::new(MultiAccountState::new(
            "000000000000",
            "us-east-1",
            "",
        ))))
    }

    fn req(action: &str, body: Value) -> AwsRequest {
        AwsRequest {
            service: "codeconnections".into(),
            action: action.into(),
            region: "us-east-1".into(),
            account_id: "000000000000".into(),
            request_id: "req".into(),
            headers: HeaderMap::new(),
            query_params: HashMap::new(),
            body: Bytes::from(serde_json::to_vec(&body).unwrap()),
            body_stream: Mutex::new(None),
            path_segments: vec![],
            raw_path: String::new(),
            raw_query: String::new(),
            method: Method::POST,
            is_query_protocol: false,
            access_key_id: None,
            principal: None,
        }
    }

    fn call(s: &CodeConnectionsService, action: &str, body: Value) -> Value {
        let resp = dispatch(s, &req(action, body)).expect("op ok");
        serde_json::from_slice(resp.body.expect_bytes()).unwrap()
    }

    fn call_err(s: &CodeConnectionsService, action: &str, body: Value) -> AwsServiceError {
        dispatch(s, &req(action, body)).err().expect("op err")
    }

    /// A connection created with an explicit `ProviderType` succeeds and is
    /// `PENDING`; a repository link created against it inherits that type.
    fn connection_with_provider(s: &CodeConnectionsService) -> String {
        let out = call(
            s,
            "CreateConnection",
            json!({ "ConnectionName": "conn", "ProviderType": "GitHub" }),
        );
        out["ConnectionArn"].as_str().unwrap().to_string()
    }

    // ---- Defect 3: CreateConnection must resolve a provider type ----

    #[test]
    fn create_connection_with_provider_type_is_pending() {
        let s = svc();
        let arn = connection_with_provider(&s);
        let got = call(&s, "GetConnection", json!({ "ConnectionArn": arn }));
        assert_eq!(got["Connection"]["ConnectionStatus"], json!("PENDING"));
        assert_eq!(got["Connection"]["ProviderType"], json!("GitHub"));
    }

    #[test]
    fn create_connection_with_neither_provider_nor_host_is_rejected() {
        let s = svc();
        let err = call_err(&s, "CreateConnection", json!({ "ConnectionName": "conn" }));
        assert_eq!(err.code(), "InvalidInputException");
    }

    // ---- Defect 2: a supplied HostArn must resolve ----

    #[test]
    fn create_connection_with_unknown_host_is_rejected() {
        let s = svc();
        let err = call_err(
            &s,
            "CreateConnection",
            json!({
                "ConnectionName": "conn",
                "HostArn": "arn:aws:codeconnections:us-east-1:000000000000:host/does-not-exist"
            }),
        );
        assert_eq!(err.code(), "ResourceNotFoundException");
    }

    #[test]
    fn create_connection_inherits_provider_type_from_host() {
        let s = svc();
        let host = call(
            &s,
            "CreateHost",
            json!({
                "Name": "host",
                "ProviderType": "GitHubEnterpriseServer",
                "ProviderEndpoint": "https://example.com"
            }),
        );
        let host_arn = host["HostArn"].as_str().unwrap().to_string();
        let conn = call(
            &s,
            "CreateConnection",
            json!({ "ConnectionName": "conn", "HostArn": host_arn.clone() }),
        );
        let arn = conn["ConnectionArn"].as_str().unwrap().to_string();
        let got = call(&s, "GetConnection", json!({ "ConnectionArn": arn }));
        assert_eq!(
            got["Connection"]["ProviderType"],
            json!("GitHubEnterpriseServer")
        );
        assert_eq!(got["Connection"]["HostArn"], json!(host_arn));
    }

    // ---- Defect 1: UpdateRepositoryLink new ConnectionArn must exist + re-derive provider ----

    fn make_link(s: &CodeConnectionsService, connection_arn: &str) -> String {
        let out = call(
            s,
            "CreateRepositoryLink",
            json!({
                "ConnectionArn": connection_arn,
                "OwnerId": "owner",
                "RepositoryName": "repo"
            }),
        );
        out["RepositoryLinkInfo"]["RepositoryLinkId"]
            .as_str()
            .unwrap()
            .to_string()
    }

    #[test]
    fn update_repository_link_to_missing_connection_is_rejected() {
        let s = svc();
        let conn = connection_with_provider(&s);
        let link = make_link(&s, &conn);
        let err = call_err(
            &s,
            "UpdateRepositoryLink",
            json!({
                "RepositoryLinkId": link,
                "ConnectionArn": "arn:aws:codeconnections:us-east-1:000000000000:connection/missing"
            }),
        );
        assert_eq!(err.code(), "InvalidInputException");
    }

    #[test]
    fn update_repository_link_rederives_provider_from_new_connection() {
        let s = svc();
        let github = connection_with_provider(&s); // GitHub
        let link = make_link(&s, &github);
        // A second connection with a different provider type.
        let bitbucket = call(
            &s,
            "CreateConnection",
            json!({ "ConnectionName": "bb", "ProviderType": "Bitbucket" }),
        )["ConnectionArn"]
            .as_str()
            .unwrap()
            .to_string();
        let out = call(
            &s,
            "UpdateRepositoryLink",
            json!({ "RepositoryLinkId": link, "ConnectionArn": bitbucket.clone() }),
        );
        assert_eq!(out["RepositoryLinkInfo"]["ConnectionArn"], json!(bitbucket));
        assert_eq!(
            out["RepositoryLinkInfo"]["ProviderType"],
            json!("Bitbucket")
        );
    }

    // ---- Defect 4: MaxResults=0 honored literally (no falsy-zero fallback) ----

    #[test]
    fn list_connections_max_results_zero_returns_empty_page() {
        let s = svc();
        connection_with_provider(&s);
        let out = call(&s, "ListConnections", json!({ "MaxResults": 0 }));
        assert_eq!(out["Connections"], json!([]));
        // There is one connection, so a continuation token is offered.
        assert_eq!(out["NextToken"], json!("0"));
    }

    // ---- L3: a non-integer MaxResults is rejected, not silently defaulted ----

    #[test]
    fn list_connections_max_results_float_is_rejected() {
        let s = svc();
        connection_with_provider(&s);
        // 50.0 is a JSON float, not an integer: as_i64 returns None. It must be
        // rejected with a validation error, not fall back to the default page
        // size (which would return the connection instead of validating).
        let err = call_err(&s, "ListConnections", json!({ "MaxResults": 50.0 }));
        assert_eq!(err.code(), "ValidationException");
    }

    // ---- L1 + L2: sync-configuration create/update enforce @length ----

    /// A repository link backed by a provider-typed connection, ready to carry a
    /// sync configuration.
    fn link_for_sync(s: &CodeConnectionsService) -> String {
        let conn = connection_with_provider(s);
        make_link(s, &conn)
    }

    fn create_sync_body(link_id: &str, resource_name: &str) -> Value {
        json!({
            "Branch": "main",
            "ConfigFile": "config.yaml",
            "RepositoryLinkId": link_id,
            "ResourceName": resource_name,
            "RoleArn": "arn:aws:iam::000000000000:role/sync-role",
            "SyncType": "CFN_STACK_SYNC"
        })
    }

    #[test]
    fn create_sync_configuration_over_length_resource_name_is_rejected() {
        let s = svc();
        let link = link_for_sync(&s);
        // ResourceName @length max is 100; 101 chars must be rejected. Before
        // this fix the create stored it, but DeleteSyncConfiguration rejects a
        // >100-char ResourceName, so the record could never be deleted.
        let over = "a".repeat(101);
        let err = call_err(
            &s,
            "CreateSyncConfiguration",
            create_sync_body(&link, &over),
        );
        assert_eq!(err.code(), "ValidationException");
    }

    #[test]
    fn create_sync_configuration_happy_path_still_succeeds() {
        let s = svc();
        let link = link_for_sync(&s);
        let out = call(
            &s,
            "CreateSyncConfiguration",
            create_sync_body(&link, "MyStack"),
        );
        assert_eq!(out["SyncConfiguration"]["ResourceName"], json!("MyStack"));
    }

    #[test]
    fn update_sync_configuration_over_length_branch_is_rejected() {
        let s = svc();
        let link = link_for_sync(&s);
        call(
            &s,
            "CreateSyncConfiguration",
            create_sync_body(&link, "MyStack"),
        );
        // Branch @length max is 255; an over-length Branch on update must be
        // rejected symmetric with create.
        let over_branch = "b".repeat(256);
        let err = call_err(
            &s,
            "UpdateSyncConfiguration",
            json!({
                "ResourceName": "MyStack",
                "SyncType": "CFN_STACK_SYNC",
                "Branch": over_branch
            }),
        );
        assert_eq!(err.code(), "ValidationException");
    }
}
