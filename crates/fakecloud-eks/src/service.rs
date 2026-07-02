//! AWS EKS REST-JSON service handler (batch 1: cluster control plane).
//!
//! Implements the eleven batch-1 operations:
//! Create/Describe/List/DeleteCluster, UpdateClusterConfig,
//! UpdateClusterVersion, Describe/ListUpdates, Tag/Untag/ListTagsForResource.
//! Node groups, addons, Fargate profiles, access entries, and identity-provider
//! configs land in later batches.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use http::{Method, StatusCode};
use serde_json::{json, Value};
use std::sync::Arc;
use tokio::sync::Mutex as AsyncMutex;

use fakecloud_core::pagination::paginate_checked;
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsService, AwsServiceError};
use fakecloud_persistence::SnapshotStore;

use crate::state::{
    cluster_arn, Cluster, EksSnapshot, SharedEksState, Update, DEFAULT_K8S_VERSION,
    EKS_SNAPSHOT_SCHEMA_VERSION,
};

/// The set of control-plane log types EKS reports on every cluster.
const LOG_TYPES: &[&str] = &[
    "api",
    "audit",
    "authenticator",
    "controllerManager",
    "scheduler",
];

pub const EKS_ACTIONS: &[&str] = &[
    "CreateCluster",
    "DescribeCluster",
    "ListClusters",
    "DeleteCluster",
    "UpdateClusterConfig",
    "UpdateClusterVersion",
    "DescribeUpdate",
    "ListUpdates",
    "TagResource",
    "UntagResource",
    "ListTagsForResource",
];

pub struct EksService {
    state: SharedEksState,
    snapshot_store: Option<Arc<dyn SnapshotStore>>,
    snapshot_lock: Arc<AsyncMutex<()>>,
}

enum PathArgs {
    None,
    Name(String),
    Update { name: String, update_id: String },
    Arn(String),
}

impl EksService {
    pub fn new(state: SharedEksState) -> Self {
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

    async fn save_snapshot(&self) {
        save_eks_snapshot(
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
                save_eks_snapshot(&state, Some(store), &lock).await;
            })
        }))
    }

    fn resolve_action(req: &AwsRequest) -> Option<(&'static str, PathArgs)> {
        // Derive segments from the raw path, preserving INTERNAL empty segments
        // (only the leading empty from the leading `/` and a single trailing
        // empty are dropped). `req.path_segments` strips every empty part, which
        // would collapse a real 3-segment path with an empty middle label
        // (e.g. `/clusters//addons`, ListAddons with an empty clusterName) down
        // to `["clusters","addons"]` and mis-route it to DescribeCluster. Keeping
        // internal empties makes the exact-slice matches below precise, so a
        // not-yet-implemented sub-resource path falls through to None.
        let raw = req.raw_path.split('?').next().unwrap_or(&req.raw_path);
        let trimmed = raw.strip_prefix('/').unwrap_or(raw);
        let trimmed = trimmed.strip_suffix('/').unwrap_or(trimmed);
        let segs: Vec<&str> = if trimmed.is_empty() {
            Vec::new()
        } else {
            trimmed.split('/').collect()
        };
        match (&req.method, segs.as_slice()) {
            (&Method::POST, ["clusters"]) => Some(("CreateCluster", PathArgs::None)),
            (&Method::GET, ["clusters"]) => Some(("ListClusters", PathArgs::None)),
            (&Method::GET, ["clusters", name]) => {
                Some(("DescribeCluster", PathArgs::Name(decode(name))))
            }
            (&Method::DELETE, ["clusters", name]) => {
                Some(("DeleteCluster", PathArgs::Name(decode(name))))
            }
            (&Method::POST, ["clusters", name, "update-config"]) => {
                Some(("UpdateClusterConfig", PathArgs::Name(decode(name))))
            }
            (&Method::POST, ["clusters", name, "updates"]) => {
                Some(("UpdateClusterVersion", PathArgs::Name(decode(name))))
            }
            (&Method::GET, ["clusters", name, "updates"]) => {
                Some(("ListUpdates", PathArgs::Name(decode(name))))
            }
            (&Method::GET, ["clusters", name, "updates", update_id]) => Some((
                "DescribeUpdate",
                PathArgs::Update {
                    name: decode(name),
                    update_id: decode(update_id),
                },
            )),
            (&Method::POST, ["tags", arn]) => Some(("TagResource", PathArgs::Arn(decode(arn)))),
            (&Method::DELETE, ["tags", arn]) => Some(("UntagResource", PathArgs::Arn(decode(arn)))),
            (&Method::GET, ["tags", arn]) => {
                Some(("ListTagsForResource", PathArgs::Arn(decode(arn))))
            }
            _ => None,
        }
    }

    fn create_cluster(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = serde_json::from_slice(&req.body).unwrap_or_default();

        let name = body
            .get("name")
            .and_then(|v| v.as_str())
            .ok_or_else(|| invalid_parameter("name is required"))?
            .to_string();
        validate_cluster_name(&name)?;

        let role_arn = body
            .get("roleArn")
            .and_then(|v| v.as_str())
            .ok_or_else(|| invalid_parameter("roleArn is required"))?
            .to_string();

        let vpc_req = body
            .get("resourcesVpcConfig")
            .filter(|v| v.is_object())
            .ok_or_else(|| invalid_parameter("resourcesVpcConfig is required"))?;

        let version = body
            .get("version")
            .and_then(|v| v.as_str())
            .unwrap_or(DEFAULT_K8S_VERSION)
            .to_string();

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        if state.clusters.contains_key(&name) {
            return Err(AwsServiceError::aws_error(
                StatusCode::CONFLICT,
                "ResourceInUseException",
                format!("Cluster already exists with name: {name}"),
            ));
        }

        let region = req.region.clone();
        let account_id = req.account_id.clone();
        let arn = cluster_arn(&region, &account_id, &name);
        let id = uuid::Uuid::new_v4().to_string();

        let tags = parse_tag_map(body.get("tags"));

        let cluster = Cluster {
            name: name.clone(),
            arn: arn.clone(),
            version,
            role_arn,
            status: "CREATING".to_string(),
            created_at: Utc::now(),
            endpoint: format!(
                "https://{}.gr7.{region}.eks.amazonaws.com",
                id.replace('-', "").to_uppercase()
            ),
            platform_version: "eks.1".to_string(),
            certificate_authority_data: default_ca_data(),
            resources_vpc_config: build_vpc_config_response(vpc_req, &id),
            kubernetes_network_config: build_k8s_network_config(
                body.get("kubernetesNetworkConfig"),
            ),
            logging: build_logging(body.get("logging")),
            tags,
            updates: Default::default(),
        };

        let out = cluster_json(&cluster, &id);
        state.clusters.insert(name, cluster);
        Ok(AwsResponse::json(
            StatusCode::OK,
            json!({ "cluster": out }).to_string(),
        ))
    }

    fn describe_cluster(
        &self,
        req: &AwsRequest,
        name: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let cluster = state
            .clusters
            .get_mut(name)
            .ok_or_else(not_found_cluster(name))?;
        // Lazily settle CREATING -> ACTIVE on first describe, mirroring the
        // eventual-consistency of the real control plane.
        if cluster.status == "CREATING" {
            cluster.status = "ACTIVE".to_string();
        }
        let id = arn_cluster_id(&cluster.endpoint);
        Ok(AwsResponse::json(
            StatusCode::OK,
            json!({ "cluster": cluster_json(cluster, &id) }).to_string(),
        ))
    }

    fn list_clusters(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let max_results = validate_max_results(req)?;
        let next_token = req.query_params.get("nextToken").cloned();

        let accounts = self.state.read();
        let Some(state) = accounts.get(&req.account_id) else {
            return Ok(AwsResponse::json(
                StatusCode::OK,
                json!({ "clusters": [] }).to_string(),
            ));
        };
        let names: Vec<String> = state.clusters.keys().cloned().collect();
        let (page, token) = paginate_checked(&names, next_token.as_deref(), max_results)
            .map_err(|_| invalid_parameter("Invalid nextToken"))?;
        let mut out = json!({ "clusters": page });
        if let Some(t) = token {
            out["nextToken"] = Value::String(t);
        }
        Ok(AwsResponse::json(StatusCode::OK, out.to_string()))
    }

    fn delete_cluster(&self, req: &AwsRequest, name: &str) -> Result<AwsResponse, AwsServiceError> {
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let mut cluster = state
            .clusters
            .remove(name)
            .ok_or_else(not_found_cluster(name))?;
        cluster.status = "DELETING".to_string();
        let id = arn_cluster_id(&cluster.endpoint);
        Ok(AwsResponse::json(
            StatusCode::OK,
            json!({ "cluster": cluster_json(&cluster, &id) }).to_string(),
        ))
    }

    fn update_cluster_config(
        &self,
        req: &AwsRequest,
        name: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = serde_json::from_slice(&req.body).unwrap_or_default();
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let cluster = state
            .clusters
            .get_mut(name)
            .ok_or_else(not_found_cluster(name))?;

        let (update_type, params) = if let Some(logging) = body.get("logging") {
            cluster.logging = build_logging(Some(logging));
            (
                "LoggingUpdate",
                vec![("ClusterLogging".to_string(), logging.to_string())],
            )
        } else if let Some(vpc) = body.get("resourcesVpcConfig") {
            let id = arn_cluster_id(&cluster.endpoint);
            cluster.resources_vpc_config = build_vpc_config_response(vpc, &id);
            (
                "EndpointAccessUpdate",
                vec![("EndpointPublicAccess".to_string(), vpc.to_string())],
            )
        } else {
            ("ConfigUpdate", Vec::new())
        };

        let update = new_update(update_type, params);
        let out = update_json(&update);
        cluster.updates.insert(update.id.clone(), update);
        Ok(AwsResponse::json(
            StatusCode::OK,
            json!({ "update": out }).to_string(),
        ))
    }

    fn update_cluster_version(
        &self,
        req: &AwsRequest,
        name: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = serde_json::from_slice(&req.body).unwrap_or_default();
        let version = body
            .get("version")
            .and_then(|v| v.as_str())
            .ok_or_else(|| invalid_parameter("version is required"))?
            .to_string();

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let cluster = state
            .clusters
            .get_mut(name)
            .ok_or_else(not_found_cluster(name))?;
        cluster.version = version.clone();

        let update = new_update(
            "VersionUpdate",
            vec![
                ("Version".to_string(), version),
                (
                    "PlatformVersion".to_string(),
                    cluster.platform_version.clone(),
                ),
            ],
        );
        let out = update_json(&update);
        cluster.updates.insert(update.id.clone(), update);
        Ok(AwsResponse::json(
            StatusCode::OK,
            json!({ "update": out }).to_string(),
        ))
    }

    fn describe_update(
        &self,
        req: &AwsRequest,
        name: &str,
        update_id: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let cluster = state
            .clusters
            .get_mut(name)
            .ok_or_else(not_found_cluster(name))?;
        let update = cluster
            .updates
            .get_mut(update_id)
            .ok_or_else(not_found_update(update_id))?;
        // Updates settle to Successful on first describe.
        if update.status == "InProgress" {
            update.status = "Successful".to_string();
        }
        Ok(AwsResponse::json(
            StatusCode::OK,
            json!({ "update": update_json(update) }).to_string(),
        ))
    }

    fn list_updates(&self, req: &AwsRequest, name: &str) -> Result<AwsResponse, AwsServiceError> {
        let max_results = validate_max_results(req)?;
        let next_token = req.query_params.get("nextToken").cloned();

        let accounts = self.state.read();
        let state = accounts
            .get(&req.account_id)
            .ok_or_else(not_found_cluster(name))?;
        let cluster = state
            .clusters
            .get(name)
            .ok_or_else(not_found_cluster(name))?;
        let ids: Vec<String> = cluster.updates.keys().cloned().collect();
        let (page, token) = paginate_checked(&ids, next_token.as_deref(), max_results)
            .map_err(|_| invalid_parameter("Invalid nextToken"))?;
        let mut out = json!({ "updateIds": page });
        if let Some(t) = token {
            out["nextToken"] = Value::String(t);
        }
        Ok(AwsResponse::json(StatusCode::OK, out.to_string()))
    }

    fn tag_resource(&self, req: &AwsRequest, arn: &str) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = serde_json::from_slice(&req.body).unwrap_or_default();
        let tags = body
            .get("tags")
            .filter(|v| v.is_object())
            .ok_or_else(|| bad_request("tags is required"))?;
        let name = cluster_name_from_arn(arn)?;
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let cluster = state
            .clusters
            .get_mut(&name)
            .ok_or_else(not_found_arn(arn))?;
        for (k, v) in tags.as_object().unwrap() {
            if let Some(v) = v.as_str() {
                cluster.tags.insert(k.clone(), v.to_string());
            }
        }
        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    fn untag_resource(&self, req: &AwsRequest, arn: &str) -> Result<AwsResponse, AwsServiceError> {
        let keys = parse_multi_query(&req.raw_query, "tagKeys");
        let name = cluster_name_from_arn(arn)?;
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let cluster = state
            .clusters
            .get_mut(&name)
            .ok_or_else(not_found_arn(arn))?;
        for k in keys {
            cluster.tags.remove(&k);
        }
        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    fn list_tags_for_resource(
        &self,
        req: &AwsRequest,
        arn: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = cluster_name_from_arn(arn)?;
        let accounts = self.state.read();
        let state = accounts
            .get(&req.account_id)
            .ok_or_else(not_found_arn(arn))?;
        let cluster = state.clusters.get(&name).ok_or_else(not_found_arn(arn))?;
        let tags: serde_json::Map<String, Value> = cluster
            .tags
            .iter()
            .map(|(k, v)| (k.clone(), Value::String(v.clone())))
            .collect();
        Ok(AwsResponse::json(
            StatusCode::OK,
            json!({ "tags": tags }).to_string(),
        ))
    }
}

/// Persist the current EKS state as a snapshot. Shared by
/// `EksService::save_snapshot` and the CFN provisioner's persist hook.
pub async fn save_eks_snapshot(
    state: &SharedEksState,
    store: Option<Arc<dyn SnapshotStore>>,
    lock: &AsyncMutex<()>,
) {
    let Some(store) = store else {
        return;
    };
    let _guard = lock.lock().await;
    let snapshot = EksSnapshot {
        schema_version: EKS_SNAPSHOT_SCHEMA_VERSION,
        accounts: state.read().clone(),
    };
    let join = tokio::task::spawn_blocking(move || -> std::io::Result<()> {
        let bytes = serde_json::to_vec(&snapshot)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?;
        store.save(&bytes)
    })
    .await;
    match join {
        Ok(Ok(())) => {}
        Ok(Err(err)) => tracing::error!(%err, "failed to write eks snapshot"),
        Err(err) => tracing::error!(%err, "eks snapshot task panicked"),
    }
}

#[async_trait]
impl AwsService for EksService {
    fn service_name(&self) -> &str {
        "eks"
    }

    async fn handle(&self, req: AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let (action, args) = Self::resolve_action(&req).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "UnknownOperationException",
                format!("Unknown operation: {} {}", req.method, req.raw_path),
            )
        })?;

        let mutates = matches!(
            action,
            "CreateCluster"
                | "DeleteCluster"
                | "UpdateClusterConfig"
                | "UpdateClusterVersion"
                | "TagResource"
                | "UntagResource"
                // Describe settles CREATING -> ACTIVE and IN_PROGRESS updates,
                // which mutates persisted state.
                | "DescribeCluster"
                | "DescribeUpdate"
        );

        let result = match (action, &args) {
            ("CreateCluster", _) => self.create_cluster(&req),
            ("ListClusters", _) => self.list_clusters(&req),
            ("DescribeCluster", PathArgs::Name(n)) => self.describe_cluster(&req, n),
            ("DeleteCluster", PathArgs::Name(n)) => self.delete_cluster(&req, n),
            ("UpdateClusterConfig", PathArgs::Name(n)) => self.update_cluster_config(&req, n),
            ("UpdateClusterVersion", PathArgs::Name(n)) => self.update_cluster_version(&req, n),
            ("ListUpdates", PathArgs::Name(n)) => self.list_updates(&req, n),
            ("DescribeUpdate", PathArgs::Update { name, update_id }) => {
                self.describe_update(&req, name, update_id)
            }
            ("TagResource", PathArgs::Arn(a)) => self.tag_resource(&req, a),
            ("UntagResource", PathArgs::Arn(a)) => self.untag_resource(&req, a),
            ("ListTagsForResource", PathArgs::Arn(a)) => self.list_tags_for_resource(&req, a),
            _ => Err(AwsServiceError::action_not_implemented("eks", action)),
        };

        if mutates && matches!(result.as_ref(), Ok(resp) if resp.status.is_success()) {
            self.save_snapshot().await;
        }
        result
    }

    fn supported_actions(&self) -> &[&str] {
        EKS_ACTIONS
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn decode(s: &str) -> String {
    percent_encoding::percent_decode_str(s)
        .decode_utf8_lossy()
        .into_owned()
}

fn invalid_parameter(msg: impl Into<String>) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::BAD_REQUEST, "InvalidParameterException", msg)
}

fn bad_request(msg: impl Into<String>) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::BAD_REQUEST, "BadRequestException", msg)
}

/// Validate the shared `maxResults` query param (range 1-100) used by the list
/// operations, returning the effective page size.
fn validate_max_results(req: &AwsRequest) -> Result<usize, AwsServiceError> {
    match req.query_params.get("maxResults") {
        Some(raw) => {
            let n: i64 = raw
                .parse()
                .map_err(|_| invalid_parameter("maxResults must be an integer"))?;
            if !(1..=100).contains(&n) {
                return Err(invalid_parameter("maxResults must be between 1 and 100"));
            }
            Ok(n as usize)
        }
        None => Ok(100),
    }
}

fn validate_cluster_name(name: &str) -> Result<(), AwsServiceError> {
    if name.is_empty() || name.len() > 100 {
        return Err(invalid_parameter("name must be 1-100 characters"));
    }
    let mut chars = name.chars();
    let first = chars.next().unwrap();
    if !first.is_ascii_alphanumeric() {
        return Err(invalid_parameter(
            "name must start with an alphanumeric character",
        ));
    }
    if !name
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_'))
    {
        return Err(invalid_parameter(
            "name must match ^[0-9A-Za-z][A-Za-z0-9\\-_]*$",
        ));
    }
    Ok(())
}

fn not_found_cluster(name: &str) -> impl Fn() -> AwsServiceError + 'static {
    let name = name.to_string();
    move || {
        AwsServiceError::aws_error(
            StatusCode::NOT_FOUND,
            "ResourceNotFoundException",
            format!("No cluster found for name: {name}."),
        )
    }
}

fn not_found_update(id: &str) -> impl Fn() -> AwsServiceError + 'static {
    let id = id.to_string();
    move || {
        AwsServiceError::aws_error(
            StatusCode::NOT_FOUND,
            "ResourceNotFoundException",
            format!("No update found for id: {id}."),
        )
    }
}

fn not_found_arn(arn: &str) -> impl Fn() -> AwsServiceError + 'static {
    let arn = arn.to_string();
    move || {
        AwsServiceError::aws_error(
            StatusCode::NOT_FOUND,
            "NotFoundException",
            format!("Resource not found: {arn}"),
        )
    }
}

/// Extract the cluster name from an EKS cluster ARN, rejecting malformed or
/// non-cluster ARNs with a BadRequestException (matching the tag ops' error set).
fn cluster_name_from_arn(arn: &str) -> Result<String, AwsServiceError> {
    let parts: Vec<&str> = arn.split(':').collect();
    if parts.len() < 6 || parts[0] != "arn" || parts[2] != "eks" {
        return Err(bad_request(format!("Invalid EKS ARN: {arn}")));
    }
    let resource = parts[5];
    let name = resource
        .strip_prefix("cluster/")
        .filter(|n| !n.is_empty())
        .ok_or_else(|| bad_request(format!("Unsupported resource ARN: {arn}")))?;
    Ok(name.to_string())
}

fn parse_tag_map(v: Option<&Value>) -> crate::state::TagMap {
    let mut out = crate::state::TagMap::new();
    if let Some(obj) = v.and_then(|v| v.as_object()) {
        for (k, val) in obj {
            if let Some(s) = val.as_str() {
                out.insert(k.clone(), s.to_string());
            }
        }
    }
    out
}

/// Parse a repeated query-string key (`?tagKeys=a&tagKeys=b`) into decoded values.
fn parse_multi_query(raw_query: &str, key: &str) -> Vec<String> {
    let prefix = format!("{key}=");
    raw_query
        .split('&')
        .filter(|pair| pair.starts_with(&prefix))
        .map(|pair| decode(&pair[prefix.len()..]))
        .collect()
}

fn new_update(update_type: &str, params: Vec<(String, String)>) -> Update {
    Update {
        id: uuid::Uuid::new_v4().to_string(),
        status: "InProgress".to_string(),
        type_: update_type.to_string(),
        params,
        created_at: Utc::now(),
    }
}

fn timestamp_to_number(t: DateTime<Utc>) -> Value {
    let secs = t.timestamp() as f64;
    let frac = t.timestamp_subsec_millis() as f64 / 1000.0;
    Value::from(secs + frac)
}

/// Recover the synthetic cluster id from a stored endpoint URL so describe/delete
/// can re-derive the OIDC issuer without persisting the id separately.
fn arn_cluster_id(endpoint: &str) -> String {
    endpoint
        .strip_prefix("https://")
        .and_then(|s| s.split('.').next())
        .map(|s| s.to_lowercase())
        .unwrap_or_default()
}

fn default_ca_data() -> String {
    // A stable placeholder base64 blob; real clusters return a PEM-encoded CA.
    "LS0tLS1CRUdJTiBDRVJUSUZJQ0FURS0tLS0tCg==".to_string()
}

fn build_vpc_config_response(req: &Value, id: &str) -> Value {
    let subnet_ids = req.get("subnetIds").cloned().unwrap_or_else(|| json!([]));
    let security_group_ids = req
        .get("securityGroupIds")
        .cloned()
        .unwrap_or_else(|| json!([]));
    let public_access_cidrs = req
        .get("publicAccessCidrs")
        .cloned()
        .unwrap_or_else(|| json!(["0.0.0.0/0"]));
    let endpoint_public = req
        .get("endpointPublicAccess")
        .and_then(|v| v.as_bool())
        .unwrap_or(true);
    let endpoint_private = req
        .get("endpointPrivateAccess")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let mut out = json!({
        "subnetIds": subnet_ids,
        "securityGroupIds": security_group_ids,
        "clusterSecurityGroupId": format!("sg-{}", &id.replace('-', "")[..17.min(id.replace('-', "").len())]),
        "vpcId": format!("vpc-{}", &id.replace('-', "")[..17.min(id.replace('-', "").len())]),
        "endpointPublicAccess": endpoint_public,
        "endpointPrivateAccess": endpoint_private,
        "publicAccessCidrs": public_access_cidrs,
    });
    if let Some(mode) = req.get("controlPlaneEgressMode") {
        out["controlPlaneEgressMode"] = mode.clone();
    }
    out
}

fn build_k8s_network_config(req: Option<&Value>) -> Value {
    let ip_family = req
        .and_then(|v| v.get("ipFamily"))
        .and_then(|v| v.as_str())
        .unwrap_or("ipv4")
        .to_string();
    let service_cidr = req
        .and_then(|v| v.get("serviceIpv4Cidr"))
        .and_then(|v| v.as_str())
        .unwrap_or("10.100.0.0/16")
        .to_string();
    json!({
        "serviceIpv4Cidr": service_cidr,
        "ipFamily": ip_family,
    })
}

fn build_logging(req: Option<&Value>) -> Value {
    // If the caller supplies a Logging block, echo its clusterLogging; else
    // default to all types disabled (what AWS reports for a fresh cluster).
    if let Some(setups) = req
        .and_then(|v| v.get("clusterLogging"))
        .and_then(|v| v.as_array())
    {
        return json!({ "clusterLogging": setups });
    }
    json!({
        "clusterLogging": [{
            "types": LOG_TYPES,
            "enabled": false,
        }],
    })
}

fn cluster_json(c: &Cluster, id: &str) -> Value {
    json!({
        "name": c.name,
        "arn": c.arn,
        "createdAt": timestamp_to_number(c.created_at),
        "version": c.version,
        "endpoint": c.endpoint,
        "roleArn": c.role_arn,
        "resourcesVpcConfig": c.resources_vpc_config,
        "kubernetesNetworkConfig": c.kubernetes_network_config,
        "logging": c.logging,
        "identity": {
            "oidc": {
                "issuer": format!("https://oidc.eks.amazonaws.com/id/{}", id.to_uppercase()),
            },
        },
        "status": c.status,
        "certificateAuthority": { "data": c.certificate_authority_data },
        "platformVersion": c.platform_version,
        "tags": c.tags,
        "health": { "issues": [] },
    })
}

fn update_json(u: &Update) -> Value {
    let params: Vec<Value> = u
        .params
        .iter()
        .map(|(t, v)| json!({ "type": t, "value": v }))
        .collect();
    json!({
        "id": u.id,
        "status": u.status,
        "type": u.type_,
        "params": params,
        "createdAt": timestamp_to_number(u.created_at),
        "errors": [],
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use http::HeaderMap;
    use parking_lot::RwLock;
    use std::collections::HashMap;

    fn make_state() -> SharedEksState {
        Arc::new(RwLock::new(
            fakecloud_core::multi_account::MultiAccountState::new("111122223333", "us-east-1", ""),
        ))
    }

    fn make_request(method: Method, path: &str, body: &str) -> AwsRequest {
        let (p, q) = match path.find('?') {
            Some(i) => (&path[..i], &path[i + 1..]),
            None => (path, ""),
        };
        let path_segments: Vec<String> = p
            .split('/')
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string())
            .collect();
        let query_params: HashMap<String, String> = q
            .split('&')
            .filter(|s| !s.is_empty())
            .filter_map(|pair| {
                let (k, v) = pair.split_once('=')?;
                Some((k.to_string(), v.to_string()))
            })
            .collect();
        AwsRequest {
            service: "eks".to_string(),
            action: String::new(),
            region: "us-east-1".to_string(),
            account_id: "111122223333".to_string(),
            request_id: "test".to_string(),
            headers: HeaderMap::new(),
            query_params,
            body: Bytes::from(body.to_string()),
            body_stream: parking_lot::Mutex::new(None),
            path_segments,
            raw_path: p.to_string(),
            raw_query: q.to_string(),
            method,
            is_query_protocol: false,
            access_key_id: None,
            principal: None,
        }
    }

    fn create_body(name: &str) -> String {
        json!({
            "name": name,
            "roleArn": "arn:aws:iam::111122223333:role/eks-cluster",
            "resourcesVpcConfig": {
                "subnetIds": ["subnet-1", "subnet-2"],
                "endpointPublicAccess": true
            }
        })
        .to_string()
    }

    #[test]
    fn snapshot_hook_is_none_without_store() {
        let svc = EksService::new(make_state());
        assert!(svc.snapshot_hook().is_none());
    }

    #[tokio::test]
    async fn snapshot_hook_fires_with_store() {
        let store: Arc<dyn SnapshotStore> =
            Arc::new(fakecloud_persistence::MemorySnapshotStore::new());
        let svc = EksService::new(make_state()).with_snapshot_store(store);
        let hook = svc.snapshot_hook().expect("hook present when store set");
        hook().await;
    }

    #[tokio::test]
    async fn create_describe_round_trip_settles_active() {
        let svc = EksService::new(make_state());
        let resp = svc
            .handle(make_request(
                Method::POST,
                "/clusters",
                &create_body("demo"),
            ))
            .await
            .unwrap();
        let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(v["cluster"]["name"], "demo");
        assert_eq!(v["cluster"]["status"], "CREATING");
        assert_eq!(
            v["cluster"]["arn"],
            "arn:aws:eks:us-east-1:111122223333:cluster/demo"
        );
        assert_eq!(v["cluster"]["version"], DEFAULT_K8S_VERSION);

        let resp = svc
            .handle(make_request(Method::GET, "/clusters/demo", ""))
            .await
            .unwrap();
        let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(v["cluster"]["status"], "ACTIVE");
        assert_eq!(
            v["cluster"]["resourcesVpcConfig"]["endpointPublicAccess"],
            true
        );
    }

    #[tokio::test]
    async fn create_rejects_duplicate() {
        let svc = EksService::new(make_state());
        svc.handle(make_request(Method::POST, "/clusters", &create_body("dup")))
            .await
            .unwrap();
        let err = svc
            .handle(make_request(Method::POST, "/clusters", &create_body("dup")))
            .await
            .err()
            .unwrap();
        assert_eq!(err.status(), StatusCode::CONFLICT);
        assert_eq!(err.code(), "ResourceInUseException");
    }

    #[tokio::test]
    async fn describe_missing_cluster_is_not_found() {
        let svc = EksService::new(make_state());
        let err = svc
            .handle(make_request(Method::GET, "/clusters/ghost", ""))
            .await
            .err()
            .unwrap();
        assert_eq!(err.status(), StatusCode::NOT_FOUND);
        assert_eq!(err.code(), "ResourceNotFoundException");
    }

    #[tokio::test]
    async fn list_then_delete_cluster() {
        let svc = EksService::new(make_state());
        svc.handle(make_request(Method::POST, "/clusters", &create_body("c1")))
            .await
            .unwrap();
        let resp = svc
            .handle(make_request(Method::GET, "/clusters", ""))
            .await
            .unwrap();
        let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(v["clusters"], json!(["c1"]));

        let resp = svc
            .handle(make_request(Method::DELETE, "/clusters/c1", ""))
            .await
            .unwrap();
        let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(v["cluster"]["status"], "DELETING");

        let err = svc
            .handle(make_request(Method::GET, "/clusters/c1", ""))
            .await
            .err()
            .unwrap();
        assert_eq!(err.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn update_version_and_describe_update() {
        let svc = EksService::new(make_state());
        svc.handle(make_request(Method::POST, "/clusters", &create_body("up")))
            .await
            .unwrap();
        let resp = svc
            .handle(make_request(
                Method::POST,
                "/clusters/up/updates",
                &json!({ "version": "1.32" }).to_string(),
            ))
            .await
            .unwrap();
        let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(v["update"]["type"], "VersionUpdate");
        assert_eq!(v["update"]["status"], "InProgress");
        let update_id = v["update"]["id"].as_str().unwrap().to_string();

        let resp = svc
            .handle(make_request(
                Method::GET,
                &format!("/clusters/up/updates/{update_id}"),
                "",
            ))
            .await
            .unwrap();
        let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(v["update"]["status"], "Successful");

        // The cluster's version reflects the update.
        let resp = svc
            .handle(make_request(Method::GET, "/clusters/up", ""))
            .await
            .unwrap();
        let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(v["cluster"]["version"], "1.32");

        let resp = svc
            .handle(make_request(Method::GET, "/clusters/up/updates", ""))
            .await
            .unwrap();
        let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(v["updateIds"].as_array().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn tag_untag_round_trip() {
        let svc = EksService::new(make_state());
        svc.handle(make_request(Method::POST, "/clusters", &create_body("tg")))
            .await
            .unwrap();
        let arn = "arn:aws:eks:us-east-1:111122223333:cluster/tg";
        let encoded = arn.replace('/', "%2F");
        svc.handle(make_request(
            Method::POST,
            &format!("/tags/{encoded}"),
            &json!({ "tags": { "env": "prod" } }).to_string(),
        ))
        .await
        .unwrap();

        let resp = svc
            .handle(make_request(Method::GET, &format!("/tags/{encoded}"), ""))
            .await
            .unwrap();
        let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(v["tags"]["env"], "prod");

        svc.handle(make_request(
            Method::DELETE,
            &format!("/tags/{encoded}?tagKeys=env"),
            "",
        ))
        .await
        .unwrap();
        let resp = svc
            .handle(make_request(Method::GET, &format!("/tags/{encoded}"), ""))
            .await
            .unwrap();
        let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(v["tags"], json!({}));
    }
}
