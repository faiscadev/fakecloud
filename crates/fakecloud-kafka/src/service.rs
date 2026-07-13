//! Amazon MSK (`kafka`) restJson1 dispatch + operation handlers.
//!
//! The full 59-operation Amazon MSK control plane. Requests are routed to an
//! operation by HTTP method + `@http` URI path; path labels (full ARNs the
//! client percent-encodes into one segment) are captured positionally and
//! percent-decoded, and query parameters are read from the raw query string so
//! repeated multi-value keys survive. State is account-partitioned and
//! persisted; each cluster, configuration, operation, replicator, and VPC
//! connection is stored as its already-output-valid restJson1 wire object
//! (camelCase `jsonName`s) so `Describe` echoes exactly what `Create`/`Update`
//! persisted, while broker nodes and bootstrap-broker strings are derived at
//! read time from the cluster's `numberOfBrokerNodes`.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use http::{Method, StatusCode};
use percent_encoding::percent_decode_str;
use serde_json::{json, Map, Value};
use tokio::sync::Mutex as AsyncMutex;
use uuid::Uuid;

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsService, AwsServiceError};
use fakecloud_persistence::SnapshotStore;

use crate::builders::{self, default_bngi, gen_version};
use crate::persistence::save_snapshot;
use crate::runtime::{KafkaRuntime, RunningBroker, TopicError};
use crate::shared;
use crate::state::{ClusterDataPlane, KafkaData, SharedKafkaState};

/// Every operation name in the Amazon MSK Smithy model (59 operations).
pub const KAFKA_ACTIONS: &[&str] = &[
    "BatchAssociateScramSecret",
    "BatchDisassociateScramSecret",
    "CreateCluster",
    "CreateClusterV2",
    "CreateConfiguration",
    "CreateReplicator",
    "CreateTopic",
    "CreateVpcConnection",
    "DeleteCluster",
    "DeleteClusterPolicy",
    "DeleteConfiguration",
    "DeleteReplicator",
    "DeleteTopic",
    "DeleteVpcConnection",
    "DescribeCluster",
    "DescribeClusterOperation",
    "DescribeClusterOperationV2",
    "DescribeClusterV2",
    "DescribeConfiguration",
    "DescribeConfigurationRevision",
    "DescribeReplicator",
    "DescribeTopic",
    "DescribeTopicPartitions",
    "DescribeVpcConnection",
    "GetBootstrapBrokers",
    "GetClusterPolicy",
    "GetCompatibleKafkaVersions",
    "ListClientVpcConnections",
    "ListClusterOperations",
    "ListClusterOperationsV2",
    "ListClusters",
    "ListClustersV2",
    "ListConfigurationRevisions",
    "ListConfigurations",
    "ListKafkaVersions",
    "ListNodes",
    "ListReplicators",
    "ListScramSecrets",
    "ListTagsForResource",
    "ListTopics",
    "ListVpcConnections",
    "PutClusterPolicy",
    "RebootBroker",
    "RejectClientVpcConnection",
    "TagResource",
    "UntagResource",
    "UpdateBrokerCount",
    "UpdateBrokerStorage",
    "UpdateBrokerType",
    "UpdateClusterConfiguration",
    "UpdateClusterKafkaVersion",
    "UpdateConfiguration",
    "UpdateConnectivity",
    "UpdateMonitoring",
    "UpdateRebalancing",
    "UpdateReplicationInfo",
    "UpdateSecurity",
    "UpdateStorage",
    "UpdateTopic",
];

/// Operations that mutate persisted state on success (so a snapshot is taken).
const MUTATING: &[&str] = &[
    "BatchAssociateScramSecret",
    "BatchDisassociateScramSecret",
    "CreateCluster",
    "CreateClusterV2",
    "CreateConfiguration",
    "CreateReplicator",
    "CreateTopic",
    "CreateVpcConnection",
    "DeleteCluster",
    "DeleteClusterPolicy",
    "DeleteConfiguration",
    "DeleteReplicator",
    "DeleteTopic",
    "DeleteVpcConnection",
    "PutClusterPolicy",
    "RebootBroker",
    "RejectClientVpcConnection",
    "TagResource",
    "UntagResource",
    "UpdateBrokerCount",
    "UpdateBrokerStorage",
    "UpdateBrokerType",
    "UpdateClusterConfiguration",
    "UpdateClusterKafkaVersion",
    "UpdateConfiguration",
    "UpdateConnectivity",
    "UpdateMonitoring",
    "UpdateRebalancing",
    "UpdateReplicationInfo",
    "UpdateSecurity",
    "UpdateStorage",
    "UpdateTopic",
];

pub struct KafkaService {
    state: SharedKafkaState,
    snapshot_store: Option<Arc<dyn SnapshotStore>>,
    snapshot_lock: Arc<AsyncMutex<()>>,
    /// The backing-container runtime. `None` in the degraded / control-plane-
    /// only mode (no container CLI, or the backend disabled for tfacc). When
    /// present, the runtime owns a PROVISIONED cluster's CREATING/DELETING and
    /// broker-reboot transitions, and topic ops + `GetBootstrapBrokers` drive the
    /// REAL Kafka broker.
    runtime: Option<Arc<KafkaRuntime>>,
}

impl KafkaService {
    pub fn new(state: SharedKafkaState) -> Self {
        Self {
            state,
            snapshot_store: None,
            snapshot_lock: Arc::new(AsyncMutex::new(())),
            runtime: None,
        }
    }

    pub fn with_snapshot_store(mut self, store: Arc<dyn SnapshotStore>) -> Self {
        self.snapshot_store = Some(store);
        self
    }

    /// Attach the backing-container runtime that spawns real single-node Kafka
    /// brokers for provisioned clusters and drives topic operations against them.
    pub fn with_runtime(mut self, runtime: Arc<KafkaRuntime>) -> Self {
        self.runtime = Some(runtime);
        self
    }

    /// Whether a container runtime is attached. When `true` the runtime's
    /// background tasks own the container-backed lifecycle transitions; the
    /// in-memory state-machine settles only the metadata-only / no-container
    /// cases (mirrors the Amazon MQ `auto_settle` split).
    fn runtime_present(&self) -> bool {
        self.runtime.is_some()
    }

    async fn save(&self) {
        save_snapshot(
            &self.state,
            self.snapshot_store.clone(),
            &self.snapshot_lock,
        )
        .await;
    }

    /// Persist hook for the CloudFormation provisioner (registered by name in
    /// the server); `None` in memory mode. CFN resource provisioning itself is
    /// a later batch, but the hook is keyed now so persistence is wired.
    pub fn snapshot_hook(&self) -> Option<fakecloud_persistence::SnapshotHook> {
        let store = self.snapshot_store.clone()?;
        let state = self.state.clone();
        let lock = self.snapshot_lock.clone();
        Some(Arc::new(move || {
            let state = state.clone();
            let store = store.clone();
            let lock = lock.clone();
            Box::pin(async move {
                crate::persistence::save_snapshot(&state, Some(store), &lock).await;
            })
        }))
    }

    /// Route a request to an operation name + captured path labels by HTTP
    /// method + `@http` URI path. Returns `None` when no route matches.
    fn resolve_action(req: &AwsRequest) -> Option<(&'static str, Vec<String>)> {
        let raw = req.raw_path.split('?').next().unwrap_or(&req.raw_path);
        let trimmed = raw.strip_prefix('/').unwrap_or(raw);
        let trimmed = trimmed.strip_suffix('/').unwrap_or(trimmed);
        let segs: Vec<String> = if trimmed.is_empty() {
            Vec::new()
        } else {
            trimmed
                .split('/')
                .map(|s| percent_decode_str(s).decode_utf8_lossy().into_owned())
                .collect()
        };
        let s: Vec<&str> = segs.iter().map(String::as_str).collect();
        let m = &req.method;
        let get = m == Method::GET;
        let post = m == Method::POST;
        let put = m == Method::PUT;
        let del = m == Method::DELETE;
        let patch = m == Method::PATCH;
        let l = |v: &str| vec![v.to_string()];
        let two = |a: &str, b: &str| vec![a.to_string(), b.to_string()];
        let (action, labels): (&'static str, Vec<String>) = match s.as_slice() {
            // ---- v1 clusters ----
            ["v1", "clusters"] if post => ("CreateCluster", vec![]),
            ["v1", "clusters"] if get => ("ListClusters", vec![]),
            ["v1", "clusters", arn] if get => ("DescribeCluster", l(arn)),
            ["v1", "clusters", arn] if del => ("DeleteCluster", l(arn)),
            ["v1", "clusters", arn, "bootstrap-brokers"] if get => ("GetBootstrapBrokers", l(arn)),
            ["v1", "clusters", arn, "nodes"] if get => ("ListNodes", l(arn)),
            ["v1", "clusters", arn, "nodes", "count"] if put => ("UpdateBrokerCount", l(arn)),
            ["v1", "clusters", arn, "nodes", "storage"] if put => ("UpdateBrokerStorage", l(arn)),
            ["v1", "clusters", arn, "nodes", "type"] if put => ("UpdateBrokerType", l(arn)),
            ["v1", "clusters", arn, "configuration"] if put => {
                ("UpdateClusterConfiguration", l(arn))
            }
            ["v1", "clusters", arn, "version"] if put => ("UpdateClusterKafkaVersion", l(arn)),
            ["v1", "clusters", arn, "monitoring"] if put => ("UpdateMonitoring", l(arn)),
            ["v1", "clusters", arn, "security"] if patch => ("UpdateSecurity", l(arn)),
            ["v1", "clusters", arn, "connectivity"] if put => ("UpdateConnectivity", l(arn)),
            ["v1", "clusters", arn, "storage"] if put => ("UpdateStorage", l(arn)),
            ["v1", "clusters", arn, "rebalancing"] if put => ("UpdateRebalancing", l(arn)),
            ["v1", "clusters", arn, "reboot-broker"] if put => ("RebootBroker", l(arn)),
            ["v1", "clusters", arn, "operations"] if get => ("ListClusterOperations", l(arn)),
            ["v1", "operations", arn] if get => ("DescribeClusterOperation", l(arn)),
            ["v1", "clusters", arn, "scram-secrets"] if get => ("ListScramSecrets", l(arn)),
            ["v1", "clusters", arn, "scram-secrets"] if post => {
                ("BatchAssociateScramSecret", l(arn))
            }
            ["v1", "clusters", arn, "scram-secrets"] if patch => {
                ("BatchDisassociateScramSecret", l(arn))
            }
            ["v1", "clusters", arn, "policy"] if get => ("GetClusterPolicy", l(arn)),
            ["v1", "clusters", arn, "policy"] if put => ("PutClusterPolicy", l(arn)),
            ["v1", "clusters", arn, "policy"] if del => ("DeleteClusterPolicy", l(arn)),
            ["v1", "clusters", arn, "client-vpc-connections"] if get => {
                ("ListClientVpcConnections", l(arn))
            }
            ["v1", "clusters", arn, "client-vpc-connection"] if put => {
                ("RejectClientVpcConnection", l(arn))
            }
            ["v1", "clusters", arn, "topics"] if get => ("ListTopics", l(arn)),
            ["v1", "clusters", arn, "topics"] if post => ("CreateTopic", l(arn)),
            ["v1", "clusters", arn, "topics", topic] if get => ("DescribeTopic", two(arn, topic)),
            ["v1", "clusters", arn, "topics", topic] if put => ("UpdateTopic", two(arn, topic)),
            ["v1", "clusters", arn, "topics", topic] if del => ("DeleteTopic", two(arn, topic)),
            ["v1", "clusters", arn, "topics", topic, "partitions"] if get => {
                ("DescribeTopicPartitions", two(arn, topic))
            }
            // ---- api/v2 clusters ----
            ["api", "v2", "clusters"] if post => ("CreateClusterV2", vec![]),
            ["api", "v2", "clusters"] if get => ("ListClustersV2", vec![]),
            ["api", "v2", "clusters", arn] if get => ("DescribeClusterV2", l(arn)),
            ["api", "v2", "clusters", arn, "operations"] if get => {
                ("ListClusterOperationsV2", l(arn))
            }
            ["api", "v2", "operations", arn] if get => ("DescribeClusterOperationV2", l(arn)),
            // ---- configurations ----
            ["v1", "configurations"] if post => ("CreateConfiguration", vec![]),
            ["v1", "configurations"] if get => ("ListConfigurations", vec![]),
            ["v1", "configurations", arn] if get => ("DescribeConfiguration", l(arn)),
            ["v1", "configurations", arn] if put => ("UpdateConfiguration", l(arn)),
            ["v1", "configurations", arn] if del => ("DeleteConfiguration", l(arn)),
            ["v1", "configurations", arn, "revisions"] if get => {
                ("ListConfigurationRevisions", l(arn))
            }
            ["v1", "configurations", arn, "revisions", rev] if get => {
                ("DescribeConfigurationRevision", two(arn, rev))
            }
            // ---- versions ----
            ["v1", "kafka-versions"] if get => ("ListKafkaVersions", vec![]),
            ["v1", "compatible-kafka-versions"] if get => ("GetCompatibleKafkaVersions", vec![]),
            // ---- vpc connections ----
            ["v1", "vpc-connection"] if post => ("CreateVpcConnection", vec![]),
            ["v1", "vpc-connections"] if get => ("ListVpcConnections", vec![]),
            ["v1", "vpc-connection", arn] if get => ("DescribeVpcConnection", l(arn)),
            ["v1", "vpc-connection", arn] if del => ("DeleteVpcConnection", l(arn)),
            // ---- replicators ----
            ["replication", "v1", "replicators"] if post => ("CreateReplicator", vec![]),
            ["replication", "v1", "replicators"] if get => ("ListReplicators", vec![]),
            ["replication", "v1", "replicators", arn] if get => ("DescribeReplicator", l(arn)),
            ["replication", "v1", "replicators", arn] if del => ("DeleteReplicator", l(arn)),
            ["replication", "v1", "replicators", arn, "replication-info"] if put => {
                ("UpdateReplicationInfo", l(arn))
            }
            // ---- tags ----
            ["v1", "tags", arn] if post => ("TagResource", l(arn)),
            ["v1", "tags", arn] if get => ("ListTagsForResource", l(arn)),
            ["v1", "tags", arn] if del => ("UntagResource", l(arn)),
            _ => return None,
        };
        Some((action, labels))
    }
}

#[async_trait]
impl AwsService for KafkaService {
    fn service_name(&self) -> &str {
        "kafka"
    }

    async fn handle(&self, req: AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let Some((action, labels)) = Self::resolve_action(&req) else {
            return Err(AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "UnknownOperationException",
                format!("Unknown operation: {} {}", req.method, req.raw_path),
            ));
        };
        let (result, settled) = self.dispatch(action, &labels, &req).await;
        let success = matches!(result.as_ref(), Ok(resp) if resp.status.is_success());
        if settled || (MUTATING.contains(&action) && success) {
            self.save().await;
        }
        result
    }

    fn supported_actions(&self) -> &[&str] {
        KAFKA_ACTIONS
    }
}

/// Per-request account + region context.
struct Ctx {
    account: String,
    region: String,
}

impl KafkaService {
    /// Returns the operation result plus a `settled` flag: `true` when a read
    /// settled a real lifecycle transition and the change must be persisted.
    #[allow(clippy::too_many_lines)]
    async fn dispatch(
        &self,
        action: &str,
        labels: &[String],
        req: &AwsRequest,
    ) -> (Result<AwsResponse, AwsServiceError>, bool) {
        let body = match parse_body(req) {
            Ok(b) => b,
            Err(e) => return (Err(e), false),
        };
        if let Err(e) = crate::validate::validate_input(action, &body) {
            return (Err(e), false);
        }
        let ctx = Ctx {
            account: req.account_id.clone(),
            region: req.region.clone(),
        };
        let q = parse_query(&req.raw_query);
        if let Err(e) = crate::validate::validate_query(&q) {
            return (Err(e), false);
        }

        // Settle any in-flight lifecycle transition up front. With a Kafka
        // runtime attached the runtime's background tasks own the
        // container-backed transitions, so reconcile settles only the
        // metadata-only / no-container cases; without one it auto-settles the
        // whole pure state-machine. A read that fires a transition reports
        // `settled` so the handler persists.
        let mut settled = false;
        {
            let mut guard = self.state.write();
            if guard
                .get_or_create(&ctx.account)
                .reconcile(self.runtime_present())
            {
                settled = true;
            }
        }

        let a0 = labels.first().map(String::as_str).unwrap_or_default();
        let a1 = labels.get(1).map(String::as_str).unwrap_or_default();
        let result = match action {
            "CreateCluster" => self.create_cluster(&ctx, &body, false),
            "CreateClusterV2" => self.create_cluster(&ctx, &body, true),
            "ListClusters" => self.list_clusters(&ctx, &q, false),
            "ListClustersV2" => self.list_clusters(&ctx, &q, true),
            "DescribeCluster" => self.describe_cluster(&ctx, a0, false),
            "DescribeClusterV2" => self.describe_cluster(&ctx, a0, true),
            "DeleteCluster" => self.delete_cluster(&ctx, a0),
            "GetBootstrapBrokers" => self.get_bootstrap_brokers(&ctx, a0),
            "ListNodes" => self.list_nodes(&ctx, a0, &q),
            "UpdateBrokerCount" => self.update_broker_count(&ctx, a0, &body),
            "UpdateBrokerStorage" => self.update_broker_storage(&ctx, a0, &body),
            "UpdateBrokerType" => self.update_broker_type(&ctx, a0, &body),
            "UpdateClusterConfiguration" => self.update_cluster_configuration(&ctx, a0, &body),
            "UpdateClusterKafkaVersion" => self.update_cluster_kafka_version(&ctx, a0, &body),
            "UpdateMonitoring" => self.update_monitoring(&ctx, a0, &body),
            "UpdateSecurity" => self.update_security(&ctx, a0, &body),
            "UpdateConnectivity" => self.update_connectivity(&ctx, a0, &body),
            "UpdateStorage" => self.update_storage(&ctx, a0, &body),
            "UpdateRebalancing" => self.update_rebalancing(&ctx, a0, &body),
            "RebootBroker" => self.reboot_broker(&ctx, a0, &body),
            "ListClusterOperations" => self.list_cluster_operations(&ctx, a0, &q, false),
            "ListClusterOperationsV2" => self.list_cluster_operations(&ctx, a0, &q, true),
            "DescribeClusterOperation" => self.describe_cluster_operation(&ctx, a0, false),
            "DescribeClusterOperationV2" => self.describe_cluster_operation(&ctx, a0, true),
            "ListScramSecrets" => self.list_scram_secrets(&ctx, a0, &q),
            "BatchAssociateScramSecret" => self.batch_scram(&ctx, a0, &body, true),
            "BatchDisassociateScramSecret" => self.batch_scram(&ctx, a0, &body, false),
            "GetClusterPolicy" => self.get_cluster_policy(&ctx, a0),
            "PutClusterPolicy" => self.put_cluster_policy(&ctx, a0, &body),
            "DeleteClusterPolicy" => self.delete_cluster_policy(&ctx, a0),
            "ListClientVpcConnections" => self.list_client_vpc_connections(&ctx, a0, &q),
            "RejectClientVpcConnection" => self.reject_client_vpc_connection(&ctx, a0, &body),
            "ListTopics" => self.list_topics(&ctx, a0, &q).await,
            "CreateTopic" => self.create_topic(&ctx, a0, &body).await,
            "DescribeTopic" => self.describe_topic(&ctx, a0, a1).await,
            "UpdateTopic" => self.update_topic(&ctx, a0, a1, &body).await,
            "DeleteTopic" => self.delete_topic(&ctx, a0, a1).await,
            "DescribeTopicPartitions" => self.describe_topic_partitions(&ctx, a0, a1, &q).await,
            "CreateConfiguration" => self.create_configuration(&ctx, &body),
            "ListConfigurations" => self.list_configurations(&ctx, &q),
            "DescribeConfiguration" => self.describe_configuration(&ctx, a0),
            "UpdateConfiguration" => self.update_configuration(&ctx, a0, &body),
            "DeleteConfiguration" => self.delete_configuration(&ctx, a0),
            "ListConfigurationRevisions" => self.list_configuration_revisions(&ctx, a0, &q),
            "DescribeConfigurationRevision" => self.describe_configuration_revision(&ctx, a0, a1),
            "ListKafkaVersions" => self.list_kafka_versions(&q),
            "GetCompatibleKafkaVersions" => self.get_compatible_kafka_versions(&ctx, &q),
            "CreateVpcConnection" => self.create_vpc_connection(&ctx, &body),
            "ListVpcConnections" => self.list_vpc_connections(&ctx, &q),
            "DescribeVpcConnection" => self.describe_vpc_connection(&ctx, a0),
            "DeleteVpcConnection" => self.delete_vpc_connection(&ctx, a0),
            "CreateReplicator" => self.create_replicator(&ctx, &body),
            "ListReplicators" => self.list_replicators(&ctx, &q),
            "DescribeReplicator" => self.describe_replicator(&ctx, a0),
            "DeleteReplicator" => self.delete_replicator(&ctx, a0),
            "UpdateReplicationInfo" => self.update_replication_info(&ctx, a0, &body),
            "TagResource" => self.tag_resource(&ctx, a0, &body),
            "UntagResource" => self.untag_resource(&ctx, a0, &q),
            "ListTagsForResource" => self.list_tags(&ctx, a0),
            _ => Err(AwsServiceError::action_not_implemented("kafka", action)),
        };
        (result, settled)
    }
}

// ===================== helpers =====================

fn ok(v: Value) -> Result<AwsResponse, AwsServiceError> {
    Ok(AwsResponse::json_value(StatusCode::OK, v))
}

/// A 204 No Content response (the `TagResource` / `UntagResource` HTTP code).
fn no_content() -> Result<AwsResponse, AwsServiceError> {
    Ok(AwsResponse::json(StatusCode::NO_CONTENT, Vec::new()))
}

fn parse_body(req: &AwsRequest) -> Result<Value, AwsServiceError> {
    if req.body.is_empty() {
        return Ok(json!({}));
    }
    serde_json::from_slice(&req.body)
        .map_err(|e| bad_request(&format!("The request body is malformed: {e}")))
}

fn bad_request(msg: &str) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::BAD_REQUEST, "BadRequestException", msg)
}

fn not_found(msg: &str) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::NOT_FOUND, "NotFoundException", msg)
}

fn conflict(msg: &str) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::CONFLICT, "ConflictException", msg)
}

fn now_iso() -> String {
    shared::now_iso()
}

fn parse_query(raw: &str) -> Vec<(String, String)> {
    raw.split('&')
        .filter(|p| !p.is_empty())
        .map(|pair| {
            let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
            (
                percent_decode_str(k).decode_utf8_lossy().into_owned(),
                percent_decode_str(v).decode_utf8_lossy().into_owned(),
            )
        })
        .collect()
}

fn query_one<'a>(q: &'a [(String, String)], key: &str) -> Option<&'a str> {
    q.iter()
        .find(|(k, _)| k == key)
        .map(|(_, v)| v.as_str())
        .filter(|v| !v.is_empty())
}

fn query_all(q: &[(String, String)], key: &str) -> Vec<String> {
    q.iter()
        .filter(|(k, _)| k == key)
        .map(|(_, v)| v.clone())
        .collect()
}

/// Paginate a slice by `maxResults` (default 100) + numeric `nextToken` offset.
fn paginate(items: &[Value], q: &[(String, String)]) -> (Vec<Value>, Option<String>) {
    let max = query_one(q, "maxResults")
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|n| *n >= 1)
        .unwrap_or(100);
    let start = query_one(q, "nextToken")
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(0);
    let end = (start + max).min(items.len());
    let page = items.get(start..end).unwrap_or(&[]).to_vec();
    let next = if end < items.len() {
        Some(end.to_string())
    } else {
        None
    };
    (page, next)
}

/// Render an ARN-keyed tag map into its wire `tags` object.
fn render_tags(tags: &std::collections::BTreeMap<String, String>) -> Value {
    Value::Object(tags.iter().map(|(k, v)| (k.clone(), json!(v))).collect())
}

/// Strip internal `_`-prefixed helper keys from a stored wire object and add the
/// resource's tags (rendered from the single ARN-keyed tag map at read time).
fn public_view(data: &KafkaData, arn: &str, obj: &Value) -> Value {
    let mut out = Map::new();
    if let Some(m) = obj.as_object() {
        for (k, v) in m {
            if !k.starts_with('_') {
                out.insert(k.clone(), v.clone());
            }
        }
    }
    if let Some(t) = data.tags.get(arn).filter(|m| !m.is_empty()) {
        out.insert("tags".into(), render_tags(t));
    }
    Value::Object(out)
}

/// Whether an ARN refers to any taggable MSK resource in this account (cluster,
/// configuration, replicator, or VPC connection). Used by the tag operations,
/// which return a declared `NotFoundException` for an unknown resource.
fn resource_exists(data: &KafkaData, arn: &str) -> bool {
    data.clusters.contains_key(arn)
        || data.configurations.contains_key(arn)
        || data.replicators.contains_key(arn)
        || data.vpc_connections.contains_key(arn)
}

fn cluster_num_brokers(cluster: &Value) -> i64 {
    cluster
        .get("numberOfBrokerNodes")
        .and_then(Value::as_i64)
        .unwrap_or(3)
}

fn cluster_kafka_version(cluster: &Value) -> String {
    cluster
        .get("currentBrokerSoftwareInfo")
        .and_then(|s| s.get("kafkaVersion"))
        .and_then(Value::as_str)
        .unwrap_or(shared::DEFAULT_KAFKA_VERSION)
        .to_string()
}

/// Whether a stored cluster is serverless (no backing broker container).
fn cluster_is_serverless(cluster: &Value) -> bool {
    cluster.get("_clusterType").and_then(Value::as_str) == Some("SERVERLESS")
}

/// Merge an `UpdateReplicationInfo` sub-object (`topicReplication` /
/// `consumerGroupReplication`) field-wise onto the stored replication-info
/// description, so create-only members not present in the update payload (e.g.
/// `startingPosition`, `topicNameConfiguration`) survive while the updated
/// lists / flags are overwritten.
fn merge_replication_sub(info: &mut Map<String, Value>, key: &str, incoming: Option<Value>) {
    let Some(incoming) = incoming.as_ref().and_then(Value::as_object) else {
        return;
    };
    let slot = info.entry(key.to_string()).or_insert_with(|| json!({}));
    // A stored value that a prior create left as a non-object (create does not
    // normalize it) would make `as_object_mut()` return None; reset it rather
    // than panicking on the update path.
    if !slot.is_object() {
        *slot = Value::Object(Map::new());
    }
    let target = slot.as_object_mut().expect("slot set to object above");
    for (k, v) in incoming {
        target.insert(k.clone(), v.clone());
    }
}

/// Whether a nested `{ enabled: bool }` sub-object is present and enabled.
fn sub_enabled(v: Option<&Value>) -> bool {
    v.and_then(|x| x.get("enabled"))
        .and_then(Value::as_bool)
        .unwrap_or(false)
}

/// The set of bootstrap-broker connection strings MSK advertises for a cluster,
/// gated on its encryption-in-transit `ClientBroker` and enabled client-auth
/// schemes (private + public + VPC-connectivity). Only the applicable variants
/// are present; the disabled ones are omitted (the SDK reads a missing field as
/// empty, which the provider asserts). Every present string is the same
/// deterministic multi-broker synthesis on the per-variant port.
fn gated_bootstrap_brokers(arn: &str, cluster: &Value) -> Map<String, Value> {
    let num = cluster_num_brokers(cluster);
    let client_broker = cluster
        .get("encryptionInfo")
        .and_then(|e| e.get("encryptionInTransit"))
        .and_then(|t| t.get("clientBroker"))
        .and_then(Value::as_str)
        .unwrap_or("TLS");
    let allows_plaintext = matches!(client_broker, "PLAINTEXT" | "TLS_PLAINTEXT");
    let allows_tls = matches!(client_broker, "TLS" | "TLS_PLAINTEXT");

    let sasl = cluster
        .get("clientAuthentication")
        .and_then(|c| c.get("sasl"));
    let sasl_scram = sub_enabled(sasl.and_then(|s| s.get("scram")));
    let sasl_iam = sub_enabled(sasl.and_then(|s| s.get("iam")));
    let any_sasl = sasl_scram || sasl_iam;

    let connectivity = cluster
        .get("brokerNodeGroupInfo")
        .and_then(|b| b.get("connectivityInfo"));
    let public_enabled = connectivity
        .and_then(|c| c.get("publicAccess"))
        .and_then(|p| p.get("type"))
        .and_then(Value::as_str)
        .is_some_and(|t| t != "DISABLED");
    let vpc_auth = connectivity
        .and_then(|c| c.get("vpcConnectivity"))
        .and_then(|v| v.get("clientAuthentication"));
    let vpc_sasl = vpc_auth.and_then(|c| c.get("sasl"));
    let vpc_sasl_iam = sub_enabled(vpc_sasl.and_then(|s| s.get("iam")));
    let vpc_sasl_scram = sub_enabled(vpc_sasl.and_then(|s| s.get("scram")));
    let vpc_tls = sub_enabled(vpc_auth.and_then(|c| c.get("tls")));

    let mut out = Map::new();
    let mut put = |key: &str, present: bool, port: u16| {
        if present {
            out.insert(
                key.to_string(),
                json!(shared::bootstrap_broker_string(arn, num, port)),
            );
        }
    };
    // Private (in-VPC) endpoints. The plaintext-cert TLS endpoint is offered only
    // when NO SASL scheme is enabled (SASL clients use the SASL port instead).
    put("bootstrapBrokerString", allows_plaintext, 9092);
    put("bootstrapBrokerStringTls", allows_tls && !any_sasl, 9094);
    put("bootstrapBrokerStringSaslScram", sasl_scram, 9096);
    put("bootstrapBrokerStringSaslIam", sasl_iam, 9098);
    // Public-access endpoints (only when public access is enabled).
    put(
        "bootstrapBrokerStringPublicTls",
        public_enabled && allows_tls && !any_sasl,
        9194,
    );
    put(
        "bootstrapBrokerStringPublicSaslScram",
        public_enabled && sasl_scram,
        9196,
    );
    put(
        "bootstrapBrokerStringPublicSaslIam",
        public_enabled && sasl_iam,
        9198,
    );
    // VPC-connectivity (PrivateLink) endpoints.
    put("bootstrapBrokerStringVpcConnectivityTls", vpc_tls, 9094);
    put(
        "bootstrapBrokerStringVpcConnectivitySaslScram",
        vpc_sasl_scram,
        9096,
    );
    put(
        "bootstrapBrokerStringVpcConnectivitySaslIam",
        vpc_sasl_iam,
        9098,
    );
    out
}

/// Project a `RunningBroker` into the persisted, describe-facing data-plane
/// binding.
fn running_to_dp(r: &RunningBroker) -> ClusterDataPlane {
    ClusterDataPlane {
        container_id: r.container_id.clone(),
        host: r.host.clone(),
        ports: r.ports.clone(),
    }
}

/// Bounded bring-up attempts for restart-recovery, so a transient daemon or
/// readiness hiccup never terminally fails a cluster that was healthy.
const BRING_UP_ATTEMPTS: u32 = 3;

/// Linear backoff between bring-up attempts (2s, 4s, ...), capped.
async fn backoff(attempt: u32) {
    let secs = (u64::from(attempt) + 1).saturating_mul(2).min(15);
    tokio::time::sleep(Duration::from_secs(secs)).await;
}

/// Settle a cluster after a broker bring-up attempt: on success flip it to
/// `ACTIVE` and record the real host/port binding (reaping the container if the
/// cluster was deleted meanwhile); on failure mark it `FAILED` with a
/// diagnosable `stateInfo`.
pub(crate) async fn settle_cluster_up(
    state: &SharedKafkaState,
    account: &str,
    arn: &str,
    running: Result<RunningBroker, String>,
    runtime: &Arc<KafkaRuntime>,
) {
    match running {
        Ok(r) => {
            let present = {
                let mut guard = state.write();
                let data = guard.get_or_create(account);
                if let Some(obj) = data.clusters.get_mut(arn).and_then(Value::as_object_mut) {
                    obj.insert("state".into(), json!("ACTIVE"));
                    obj.insert("stateInfo".into(), json!({ "code": "NONE", "message": "" }));
                    obj.remove("activeOperationArn");
                    data.data_plane.insert(arn.to_string(), running_to_dp(&r));
                    true
                } else {
                    false
                }
            };
            if !present {
                // Deleted mid-bring-up: don't leak the container.
                runtime.stop_broker(arn).await;
            }
        }
        Err(reason) => {
            let mut guard = state.write();
            let data = guard.get_or_create(account);
            if let Some(obj) = data.clusters.get_mut(arn).and_then(Value::as_object_mut) {
                obj.insert("state".into(), json!("FAILED"));
                obj.insert(
                    "stateInfo".into(),
                    json!({ "code": "CREATION_FAILED", "message": reason }),
                );
            }
        }
    }
}

/// Bring a persisted cluster's broker back after a restart: re-attach the
/// persisted container if it still exists (preserving the topic log), otherwise
/// create a fresh one. Both phases retry with backoff so a transient failure
/// doesn't lose a previously-ACTIVE cluster. `None` only after retries exhaust.
async fn recover_bring_up(
    runtime: &Arc<KafkaRuntime>,
    arn: &str,
    persisted_container_id: Option<&str>,
) -> Option<RunningBroker> {
    if let Some(cid) = persisted_container_id {
        for attempt in 0..BRING_UP_ATTEMPTS {
            match runtime.reattach_broker(arn, cid).await {
                Ok(Some(r)) => return Some(r),
                Ok(None) => break, // container is gone -> create fresh below
                Err(error) => {
                    tracing::warn!(%error, cluster_arn = %arn, attempt, "re-attach attempt failed; retrying");
                    backoff(attempt).await;
                }
            }
        }
    }
    for attempt in 0..BRING_UP_ATTEMPTS {
        match runtime.ensure_broker(arn).await {
            Ok(r) => return Some(r),
            Err(error) => {
                tracing::warn!(%error, cluster_arn = %arn, attempt, "respawn attempt failed; retrying");
                backoff(attempt).await;
            }
        }
    }
    tracing::error!(cluster_arn = %arn, "gave up recovering broker container after retries");
    None
}

// ===================== clusters =====================

impl KafkaService {
    fn create_cluster(
        &self,
        ctx: &Ctx,
        b: &Value,
        v2: bool,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = b
            .get("clusterName")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        let serverless = v2 && b.get("serverless").is_some();

        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);

        // Build + insert the record through the SHARED builder (the same path the
        // CloudFormation `AWS::MSK::*` provisioner uses, so the two paths cannot
        // diverge). Returns the ARN, or a name-conflict message.
        let arn = builders::insert_cluster(data, &ctx.region, &ctx.account, b, v2)
            .map_err(|msg| conflict(&msg))?;

        let mut out = json!({
            "clusterArn": arn,
            "clusterName": name,
            "state": "CREATING",
        });
        if v2 {
            out["clusterType"] = json!(if serverless {
                "SERVERLESS"
            } else {
                "PROVISIONED"
            });
        }

        // Background-spawn the real backing Kafka broker container (never in the
        // request handler -- a synchronous image pull/start would time the client
        // out, #1539/#1730). The cluster stays CREATING and only settles to
        // ACTIVE once the broker actually serves. Only PROVISIONED clusters get a
        // container; a serverless cluster (no broker to run) and the no-runtime
        // fallback settle via the in-memory reconcile instead.
        if self.runtime.is_some() && !serverless {
            drop(guard);
            self.spawn_create(ctx.account.clone(), arn.clone());
        }

        ok(out)
    }

    fn list_clusters(
        &self,
        ctx: &Ctx,
        q: &[(String, String)],
        v2: bool,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name_filter = query_one(q, "clusterNameFilter");
        let type_filter = query_one(q, "clusterTypeFilter");
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        let items: Vec<Value> = data
            .clusters
            .values()
            .filter(|c| {
                c.get("clusterArn")
                    .and_then(Value::as_str)
                    .and_then(shared::arn_region)
                    == Some(ctx.region.as_str())
            })
            .filter(|c| {
                name_filter.is_none_or(|f| {
                    c.get("clusterName")
                        .and_then(Value::as_str)
                        .is_some_and(|n| n.starts_with(f))
                })
            })
            .filter(|c| {
                type_filter.is_none_or(|f| {
                    c.get("_clusterType")
                        .and_then(Value::as_str)
                        .unwrap_or("PROVISIONED")
                        == f
                })
            })
            .map(|c| {
                let arn = c
                    .get("clusterArn")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                if v2 {
                    render_cluster_v2(data, arn, c)
                } else {
                    public_view(data, arn, c)
                }
            })
            .collect();
        let (page, next) = paginate(&items, q);
        let mut out = json!({ "clusterInfoList": page });
        if let Some(n) = next {
            out["nextToken"] = json!(n);
        }
        ok(out)
    }

    fn describe_cluster(
        &self,
        ctx: &Ctx,
        arn: &str,
        v2: bool,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        let cluster = data
            .clusters
            .get(arn)
            .cloned()
            .ok_or_else(|| not_found(&format!("The cluster '{arn}' does not exist.")))?;
        let info = if v2 {
            render_cluster_v2(data, arn, &cluster)
        } else {
            public_view(data, arn, &cluster)
        };
        ok(json!({ "clusterInfo": info }))
    }

    fn delete_cluster(&self, ctx: &Ctx, arn: &str) -> Result<AwsResponse, AwsServiceError> {
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        let cluster = data
            .clusters
            .get_mut(arn)
            .ok_or_else(|| not_found(&format!("The cluster '{arn}' does not exist.")))?;
        let serverless = cluster_is_serverless(cluster);
        cluster
            .as_object_mut()
            .expect("cluster is object")
            .insert("state".into(), json!("DELETING"));
        // With a runtime, tear the real broker container down and free state in a
        // background task (a provisioned cluster only); a serverless cluster has
        // no container, and the no-runtime path reaps the DELETING cluster on the
        // next reconcile.
        if self.runtime.is_some() && !serverless {
            drop(guard);
            self.spawn_delete(ctx.account.clone(), arn.to_string());
        }
        ok(json!({ "clusterArn": arn, "state": "DELETING" }))
    }

    fn get_bootstrap_brokers(&self, ctx: &Ctx, arn: &str) -> Result<AwsResponse, AwsServiceError> {
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        // GetBootstrapBrokers declares no NotFoundException, so a missing cluster
        // surfaces as its (declared) BadRequestException.
        let cluster = data
            .clusters
            .get(arn)
            .ok_or_else(|| bad_request(&format!("The cluster '{arn}' does not exist.")))?;
        // Synthesize exactly the encryption-in-transit + client-auth-gated set of
        // bootstrap-broker strings MSK would advertise for this cluster's config
        // (AWS omits every variant the cluster does not enable; the provider
        // asserts the disabled ones are empty).
        let mut out = gated_bootstrap_brokers(arn, cluster);
        // When a real single-node Kafka broker backs this cluster (data plane on),
        // its reachable `host:port` IS the real PLAINTEXT endpoint a Kafka client
        // produces/consumes through, so surface it as `bootstrapBrokerString`
        // regardless of the cosmetic gating (the single local broker is PLAINTEXT;
        // the TLS/SASL variants stay format-synthesized). tfacc disables the
        // backend, so there it is pure gating.
        if let Some(bootstrap) = data
            .data_plane
            .get(arn)
            .and_then(|dp| dp.bootstrap_string())
        {
            out.insert("bootstrapBrokerString".into(), json!(bootstrap));
        }
        ok(Value::Object(out))
    }

    fn list_nodes(
        &self,
        ctx: &Ctx,
        arn: &str,
        q: &[(String, String)],
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        let cluster = data
            .clusters
            .get(arn)
            .ok_or_else(|| not_found(&format!("The cluster '{arn}' does not exist.")))?;
        let num = cluster_num_brokers(cluster);
        let kv = cluster_kafka_version(cluster);
        let mut nodes = shared::synthesize_nodes(arn, num, &kv);
        // A real MSK cluster has N brokers; fakecloud backs it with a SINGLE
        // Kafka container, so broker node 1 is the live one -- surface its
        // reachable endpoint as node 1's endpoint when a broker is up. The
        // remaining synthesized nodes keep the per-`numberOfBrokerNodes` view
        // faithful (a single-container simplification, honestly labeled).
        if let Some(bootstrap) = data
            .data_plane
            .get(arn)
            .and_then(|dp| dp.bootstrap_string())
        {
            if let Some(n0) = nodes.first_mut() {
                if let Some(bni) = n0.get_mut("brokerNodeInfo").and_then(Value::as_object_mut) {
                    bni.insert("endpoints".into(), json!([bootstrap]));
                }
            }
        }
        let (page, next) = paginate(&nodes, q);
        let mut out = json!({ "nodeInfoList": page });
        if let Some(n) = next {
            out["nextToken"] = json!(n);
        }
        ok(out)
    }
}

// ===================== data-plane container lifecycle =====================

impl KafkaService {
    /// Resolve a cluster's running backing-container id from the live data-plane
    /// binding, or `None` when no broker is up for it (no runtime, serverless,
    /// still creating, or control-plane-only mode). Topic ops use this to decide
    /// between driving the REAL broker and the in-memory fallback.
    fn cluster_container(&self, account: &str, arn: &str) -> Option<String> {
        self.runtime.as_ref()?;
        self.state
            .read()
            .get(account)
            .and_then(|d| d.data_plane.get(arn).map(|dp| dp.container_id.clone()))
    }

    /// Spawn the backing Kafka broker for a freshly-created cluster and settle it
    /// to `ACTIVE` (recording the real host/port) once reachable, or to `FAILED`
    /// if the broker can't come up. Reaps the container if the cluster was
    /// deleted mid-create.
    fn spawn_create(&self, account: String, arn: String) {
        let Some(runtime) = self.runtime.clone() else {
            return;
        };
        let state = self.state.clone();
        let store = self.snapshot_store.clone();
        let lock = self.snapshot_lock.clone();
        tokio::spawn(async move {
            let running = runtime.ensure_broker(&arn).await.map_err(|error| {
                tracing::error!(%error, cluster_arn = %arn, "MSK Kafka broker container failed to start");
                error.to_string()
            });
            settle_cluster_up(&state, &account, &arn, running, &runtime).await;
            save_snapshot(&state, store, &lock).await;
        });
    }

    /// Tear down a cluster's backing broker container and free its state. Removal
    /// is time-bounded; state cleanup is GUARANTEED so a hung daemon can never
    /// wedge the cluster in `DELETING` forever (issue #1338 sibling).
    fn spawn_delete(&self, account: String, arn: String) {
        let Some(runtime) = self.runtime.clone() else {
            return;
        };
        let state = self.state.clone();
        let store = self.snapshot_store.clone();
        let lock = self.snapshot_lock.clone();
        tokio::spawn(async move {
            if tokio::time::timeout(Duration::from_secs(30), runtime.stop_broker(&arn))
                .await
                .is_err()
            {
                tracing::warn!(cluster_arn = %arn, "broker container removal timed out; freeing cluster state anyway");
            }
            {
                let mut guard = state.write();
                let data = guard.get_or_create(&account);
                data.clusters.remove(&arn);
                data.topics.remove(&arn);
                data.scram_secrets.remove(&arn);
                data.policies.remove(&arn);
                data.data_plane.remove(&arn);
            }
            save_snapshot(&state, store, &lock).await;
        });
    }

    /// Restart a cluster's backing broker container in place (preserving the
    /// topic log) and settle back to `ACTIVE` once it serves again.
    fn spawn_reboot(&self, account: String, arn: String) {
        let Some(runtime) = self.runtime.clone() else {
            return;
        };
        let state = self.state.clone();
        let store = self.snapshot_store.clone();
        let lock = self.snapshot_lock.clone();
        tokio::spawn(async move {
            let running = runtime.reboot_broker(&arn).await.map_err(|error| {
                tracing::error!(%error, cluster_arn = %arn, "MSK Kafka broker reboot failed");
                error.to_string()
            });
            settle_cluster_up(&state, &account, &arn, running, &runtime).await;
            save_snapshot(&state, store, &lock).await;
        });
    }

    /// Recreate backing broker containers for persisted clusters the snapshot
    /// claims should be running (same restart-recovery contract as RDS #1338 /
    /// Amazon MQ). Every deserialized data-plane binding is dropped up front --
    /// its host port referred to a container owned by the previous process and is
    /// dead now. When a runtime is present, each recoverable PROVISIONED cluster
    /// is re-driven through its PERSISTED container (re-attached, preserving the
    /// topic log) or a fresh one if that container is gone; without a runtime the
    /// cleared binding is all that is needed (bootstrap falls back to cosmetic).
    pub async fn recover_persisted_containers(&self) {
        let runtime_present = self.runtime.is_some();
        struct Pending {
            account: String,
            arn: String,
            container_id: Option<String>,
        }
        let (pending, changed) = {
            let mut guard = self.state.write();
            let mut out: Vec<Pending> = Vec::new();
            let mut changed = false;
            for (account_id, data) in guard.iter_mut() {
                let account = account_id.to_string();
                let arns: Vec<String> = data.clusters.keys().cloned().collect();
                for arn in arns {
                    let persisted_container_id =
                        data.data_plane.remove(&arn).map(|dp| dp.container_id);
                    if persisted_container_id.is_some() {
                        changed = true;
                    }
                    let serverless = data
                        .clusters
                        .get(&arn)
                        .map(cluster_is_serverless)
                        .unwrap_or(false);
                    let st = data
                        .clusters
                        .get(&arn)
                        .and_then(|c| c.get("state"))
                        .and_then(Value::as_str)
                        .unwrap_or("")
                        .to_string();
                    if st == "DELETING" {
                        data.clusters.remove(&arn);
                        data.topics.remove(&arn);
                        data.scram_secrets.remove(&arn);
                        data.policies.remove(&arn);
                        changed = true;
                        continue;
                    }
                    // Only a runtime can re-drive a provisioned cluster's data
                    // plane; serverless and control-plane-only mode just need the
                    // cleared binding above.
                    if !runtime_present || serverless {
                        continue;
                    }
                    let recoverable =
                        matches!(st.as_str(), "ACTIVE" | "CREATING" | "REBOOTING_BROKER");
                    if !recoverable {
                        continue;
                    }
                    // Mark CREATING so a describe during recovery doesn't advertise
                    // a bootstrap endpoint before the broker is back up.
                    if let Some(obj) = data.clusters.get_mut(&arn).and_then(Value::as_object_mut) {
                        obj.insert("state".into(), json!("CREATING"));
                    }
                    changed = true;
                    out.push(Pending {
                        account: account.clone(),
                        arn,
                        container_id: persisted_container_id,
                    });
                }
            }
            (out, changed)
        };
        // Persist the cleared bindings / settled deletions so a subsequent restart
        // starts clean even if nothing else mutates state (the control-plane-only
        // path spawns no tasks).
        if changed && !runtime_present {
            save_snapshot(
                &self.state,
                self.snapshot_store.clone(),
                &self.snapshot_lock,
            )
            .await;
        }
        if pending.is_empty() {
            return;
        }
        tracing::info!(
            count = pending.len(),
            "recovering backing containers for persisted MSK clusters",
        );
        for p in pending {
            let Some(runtime) = self.runtime.clone() else {
                break;
            };
            let state = self.state.clone();
            let store = self.snapshot_store.clone();
            let lock = self.snapshot_lock.clone();
            tokio::spawn(async move {
                let running = recover_bring_up(&runtime, &p.arn, p.container_id.as_deref()).await;
                if let Some(r) = running {
                    settle_cluster_up(&state, &p.account, &p.arn, Ok(r), &runtime).await;
                } else {
                    tracing::error!(cluster_arn = %p.arn, "broker container recovery exhausted; will retry on next restart");
                }
                save_snapshot(&state, store, &lock).await;
            });
        }
    }
}

/// Render a stored cluster into the `CreateClusterV2`/`DescribeClusterV2`
/// `Cluster` union shape (provisioned vs serverless).
fn render_cluster_v2(data: &KafkaData, arn: &str, cluster: &Value) -> Value {
    let ctype = cluster
        .get("_clusterType")
        .and_then(Value::as_str)
        .unwrap_or("PROVISIONED");
    let mut out = json!({
        "clusterType": ctype,
        "clusterArn": cluster.get("clusterArn").cloned().unwrap_or(json!(arn)),
        "clusterName": cluster.get("clusterName").cloned().unwrap_or(json!("")),
        "creationTime": cluster.get("creationTime").cloned().unwrap_or(json!("")),
        "currentVersion": cluster.get("currentVersion").cloned().unwrap_or(json!("")),
        "state": cluster.get("state").cloned().unwrap_or(json!("")),
        "stateInfo": cluster.get("stateInfo").cloned().unwrap_or(json!({ "code": "NONE", "message": "" })),
    });
    if let Some(op) = cluster.get("activeOperationArn") {
        out["activeOperationArn"] = op.clone();
    }
    if let Some(t) = data.tags.get(arn).filter(|m| !m.is_empty()) {
        out["tags"] = render_tags(t);
    }
    if ctype == "SERVERLESS" {
        out["serverless"] = cluster
            .get("_serverless")
            .cloned()
            .unwrap_or(json!({ "vpcConfigs": [] }));
    } else {
        let mut prov = json!({
            "brokerNodeGroupInfo": cluster.get("brokerNodeGroupInfo").cloned().unwrap_or_else(default_bngi),
            "numberOfBrokerNodes": cluster.get("numberOfBrokerNodes").cloned().unwrap_or(json!(3)),
            "currentBrokerSoftwareInfo": cluster.get("currentBrokerSoftwareInfo").cloned().unwrap_or(json!({})),
            "enhancedMonitoring": cluster.get("enhancedMonitoring").cloned().unwrap_or(json!("DEFAULT")),
            "encryptionInfo": cluster.get("encryptionInfo").cloned().unwrap_or(json!({})),
            "storageMode": cluster.get("storageMode").cloned().unwrap_or(json!("LOCAL")),
            "zookeeperConnectString": cluster.get("zookeeperConnectString").cloned().unwrap_or(json!("")),
            "zookeeperConnectStringTls": cluster.get("zookeeperConnectStringTls").cloned().unwrap_or(json!("")),
        });
        // Echo the optional client-auth / logging / monitoring sub-objects only
        // when the cluster carries them (same as the V1 view).
        for key in ["clientAuthentication", "loggingInfo", "openMonitoring"] {
            if let Some(v) = cluster.get(key) {
                prov[key] = v.clone();
            }
        }
        out["provisioned"] = prov;
    }
    out
}

// ===================== cluster updates =====================

impl KafkaService {
    /// Record a cluster operation for an update, bumping the cluster's
    /// `currentVersion`, flipping it to `UPDATING` (settles on the next read),
    /// and returning the new operation ARN. Assumes the cluster exists.
    fn record_operation(&self, data: &mut KafkaData, cluster_arn: &str, op_type: &str) -> String {
        let n = data.next_seq();
        let op_uuid = Uuid::new_v4().to_string();
        let op_arn = shared::operation_arn_from_cluster(cluster_arn, &op_uuid);
        let version = gen_version(n);
        let ctype = data
            .clusters
            .get(cluster_arn)
            .and_then(|c| c.get("_clusterType"))
            .cloned()
            .unwrap_or(json!("PROVISIONED"));
        if let Some(obj) = data
            .clusters
            .get_mut(cluster_arn)
            .and_then(Value::as_object_mut)
        {
            obj.insert("state".into(), json!("UPDATING"));
            obj.insert("currentVersion".into(), json!(version));
            obj.insert("activeOperationArn".into(), json!(op_arn));
        }
        data.operations.insert(
            op_arn.clone(),
            json!({
                "clusterArn": cluster_arn,
                "clientRequestId": Uuid::new_v4().to_string(),
                "creationTime": now_iso(),
                "operationArn": op_arn,
                "operationState": "PENDING",
                "operationType": op_type,
                "operationSteps": [],
                "sourceClusterInfo": {},
                "targetClusterInfo": {},
                "_clusterType": ctype,
            }),
        );
        op_arn
    }

    /// Shared machinery for the eleven `Update*`/`RebootBroker` cluster ops:
    /// look up the cluster (declared-error on miss), apply `mutate` to the
    /// cluster wire object, record the operation, and return the standard
    /// `{clusterArn, clusterOperationArn}` response.
    fn cluster_update(
        &self,
        ctx: &Ctx,
        arn: &str,
        op_type: &str,
        missing_declares_not_found: bool,
        mutate: impl FnOnce(&mut Map<String, Value>),
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        if !data.clusters.contains_key(arn) {
            let msg = format!("The cluster '{arn}' does not exist.");
            return Err(if missing_declares_not_found {
                not_found(&msg)
            } else {
                bad_request(&msg)
            });
        }
        if let Some(obj) = data.clusters.get_mut(arn).and_then(Value::as_object_mut) {
            mutate(obj);
        }
        let op_arn = self.record_operation(data, arn, op_type);
        ok(json!({ "clusterArn": arn, "clusterOperationArn": op_arn }))
    }

    fn update_broker_count(
        &self,
        ctx: &Ctx,
        arn: &str,
        b: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let target = b.get("targetNumberOfBrokerNodes").cloned();
        self.cluster_update(ctx, arn, "UPDATE_BROKER_COUNT", false, |obj| {
            if let Some(t) = target {
                obj.insert("numberOfBrokerNodes".into(), t);
            }
        })
    }

    fn update_broker_storage(
        &self,
        ctx: &Ctx,
        arn: &str,
        b: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        // `UpdateBrokerStorage` carries the per-broker target EBS volume info
        // (a uniform `volumeSizeGB` + optional `provisionedThroughput`); reflect
        // it onto the cluster's `storageInfo.ebsStorageInfo` so a describe echoes
        // the resized volume + provisioned-throughput toggle.
        let target = b
            .get("targetBrokerEBSVolumeInfo")
            .and_then(Value::as_array)
            .and_then(|a| a.first())
            .cloned();
        self.cluster_update(ctx, arn, "UPDATE_BROKER_STORAGE", false, |obj| {
            let Some(target) = target else { return };
            let Some(ebs) = obj
                .get_mut("brokerNodeGroupInfo")
                .and_then(Value::as_object_mut)
                .and_then(|bngi| {
                    bngi.entry("storageInfo")
                        .or_insert_with(|| json!({}))
                        .as_object_mut()
                })
                .and_then(|si| {
                    si.entry("ebsStorageInfo")
                        .or_insert_with(|| json!({}))
                        .as_object_mut()
                })
            else {
                return;
            };
            if let Some(vs) = target.get("volumeSizeGB") {
                ebs.insert("volumeSize".into(), vs.clone());
            }
            // Echo the provisioned-throughput toggle verbatim (present once
            // configured; enabled=false when disabled/removed).
            if let Some(pt) = target.get("provisionedThroughput") {
                ebs.insert("provisionedThroughput".into(), pt.clone());
            }
        })
    }

    fn update_broker_type(
        &self,
        ctx: &Ctx,
        arn: &str,
        b: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let target = b
            .get("targetInstanceType")
            .and_then(Value::as_str)
            .map(str::to_string);
        self.cluster_update(ctx, arn, "UPDATE_BROKER_TYPE", true, |obj| {
            if let Some(t) = target {
                if let Some(bngi) = obj
                    .get_mut("brokerNodeGroupInfo")
                    .and_then(Value::as_object_mut)
                {
                    bngi.insert("instanceType".into(), json!(t));
                }
            }
        })
    }

    fn update_cluster_configuration(
        &self,
        ctx: &Ctx,
        arn: &str,
        b: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let info = b.get("configurationInfo").cloned();
        self.cluster_update(ctx, arn, "UPDATE_CLUSTER_CONFIGURATION", true, |obj| {
            if let Some(ci) = info {
                let carn = ci.get("arn").cloned().unwrap_or(json!(""));
                let rev = ci.get("revision").cloned().unwrap_or(json!(1));
                let sw = obj
                    .entry("currentBrokerSoftwareInfo")
                    .or_insert_with(|| json!({}));
                if let Some(swo) = sw.as_object_mut() {
                    swo.insert("configurationArn".into(), carn);
                    swo.insert("configurationRevision".into(), rev);
                }
            }
        })
    }

    fn update_cluster_kafka_version(
        &self,
        ctx: &Ctx,
        arn: &str,
        b: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let target = b
            .get("targetKafkaVersion")
            .and_then(Value::as_str)
            .map(str::to_string);
        // `UpdateClusterKafkaVersion` optionally carries a new configuration to
        // apply alongside the version bump (upgrade-with-info).
        let info = b.get("configurationInfo").cloned();
        self.cluster_update(ctx, arn, "UPDATE_CLUSTER_KAFKA_VERSION", true, |obj| {
            let sw = obj
                .entry("currentBrokerSoftwareInfo")
                .or_insert_with(|| json!({}));
            if let Some(swo) = sw.as_object_mut() {
                if let Some(t) = target {
                    swo.insert("kafkaVersion".into(), json!(t));
                }
                if let Some(ci) = info {
                    if let Some(a) = ci.get("arn") {
                        swo.insert("configurationArn".into(), a.clone());
                    }
                    if let Some(r) = ci.get("revision") {
                        swo.insert("configurationRevision".into(), r.clone());
                    }
                }
            }
        })
    }

    fn update_monitoring(
        &self,
        ctx: &Ctx,
        arn: &str,
        b: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let em = b.get("enhancedMonitoring").cloned();
        let li = b.get("loggingInfo").cloned();
        let om = b.get("openMonitoring").cloned();
        self.cluster_update(ctx, arn, "UPDATE_MONITORING", false, |obj| {
            if let Some(v) = em {
                obj.insert("enhancedMonitoring".into(), v);
            }
            // `UpdateMonitoring` also carries the logging + open-monitoring config
            // (the provider batches all three into one call); reflect them so a
            // describe echoes the enabled/disabled state.
            if let Some(v) = li {
                obj.insert("loggingInfo".into(), v);
            }
            if let Some(v) = om {
                obj.insert(
                    "openMonitoring".into(),
                    builders::normalize_open_monitoring(&v),
                );
            }
        })
    }

    fn update_security(
        &self,
        ctx: &Ctx,
        arn: &str,
        b: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let ca = b.get("clientAuthentication").cloned();
        let ei = b.get("encryptionInfo").cloned();
        self.cluster_update(ctx, arn, "UPDATE_SECURITY", true, |obj| {
            if let Some(v) = ca {
                obj.insert("clientAuthentication".into(), v);
            }
            // MSK's UpdateSecurity does NOT change the at-rest key or the
            // inter-broker (`inCluster`) encryption setting (the provider strips
            // both from the request), so merge the incoming `encryptionInfo` onto
            // the existing one field-wise instead of replacing it -- otherwise the
            // preserved `inCluster` / `dataVolumeKMSKeyId` would be dropped.
            if let Some(incoming) = ei.as_ref().and_then(Value::as_object) {
                let existing = obj
                    .get("encryptionInfo")
                    .and_then(Value::as_object)
                    .cloned()
                    .unwrap_or_default();
                let mut merged = existing.clone();
                if let Some(transit) = incoming
                    .get("encryptionInTransit")
                    .and_then(Value::as_object)
                {
                    let mut t = existing
                        .get("encryptionInTransit")
                        .and_then(Value::as_object)
                        .cloned()
                        .unwrap_or_default();
                    for (k, v) in transit {
                        t.insert(k.clone(), v.clone());
                    }
                    merged.insert("encryptionInTransit".into(), Value::Object(t));
                }
                if let Some(at_rest) = incoming.get("encryptionAtRest") {
                    merged.insert("encryptionAtRest".into(), at_rest.clone());
                }
                obj.insert("encryptionInfo".into(), Value::Object(merged));
            }
        })
    }

    fn update_connectivity(
        &self,
        ctx: &Ctx,
        arn: &str,
        b: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let ci = b.get("connectivityInfo").cloned();
        self.cluster_update(ctx, arn, "UPDATE_CONNECTIVITY", true, |obj| {
            if let Some(incoming) = ci.as_ref().and_then(Value::as_object) {
                if let Some(bngi) = obj
                    .get_mut("brokerNodeGroupInfo")
                    .and_then(Value::as_object_mut)
                {
                    // Merge the incoming `connectivityInfo` onto the existing one
                    // (the provider sends only the sub-object it is changing --
                    // `publicAccess` on a public-access change, `vpcConnectivity`
                    // on the post-create vpc-connectivity apply), then re-normalize
                    // so both sub-objects stay present with AWS defaults.
                    let mut merged = bngi
                        .get("connectivityInfo")
                        .and_then(Value::as_object)
                        .cloned()
                        .unwrap_or_default();
                    for (k, v) in incoming {
                        merged.insert(k.clone(), v.clone());
                    }
                    bngi.insert(
                        "connectivityInfo".into(),
                        builders::normalize_connectivity_info(Some(&Value::Object(merged))),
                    );
                }
            }
        })
    }

    fn update_storage(
        &self,
        ctx: &Ctx,
        arn: &str,
        b: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let sm = b.get("storageMode").cloned();
        let vs = b.get("volumeSizeGB").cloned();
        self.cluster_update(ctx, arn, "UPDATE_STORAGE", true, |obj| {
            if let Some(v) = sm {
                obj.insert("storageMode".into(), v);
            }
            if let Some(v) = vs {
                if let Some(bngi) = obj
                    .get_mut("brokerNodeGroupInfo")
                    .and_then(Value::as_object_mut)
                {
                    let si = bngi.entry("storageInfo").or_insert_with(|| json!({}));
                    if let Some(ebs) = si.as_object_mut().and_then(|o| {
                        o.entry("ebsStorageInfo")
                            .or_insert_with(|| json!({}))
                            .as_object_mut()
                    }) {
                        ebs.insert("volumeSize".into(), v);
                    }
                }
            }
        })
    }

    fn update_rebalancing(
        &self,
        ctx: &Ctx,
        arn: &str,
        b: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let reb = b.get("rebalancing").cloned();
        self.cluster_update(ctx, arn, "UPDATE_REBALANCING", true, |obj| {
            if let Some(v) = reb {
                obj.insert("rebalancing".into(), v);
            }
        })
    }

    fn reboot_broker(
        &self,
        ctx: &Ctx,
        arn: &str,
        _b: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        if !data.clusters.contains_key(arn) {
            return Err(not_found(&format!("The cluster '{arn}' does not exist.")));
        }
        let serverless = data
            .clusters
            .get(arn)
            .map(cluster_is_serverless)
            .unwrap_or(false);
        if let Some(obj) = data.clusters.get_mut(arn).and_then(Value::as_object_mut) {
            obj.insert("state".into(), json!("REBOOTING_BROKER"));
        }
        let op_arn = self.record_operation(data, arn, "REBOOT_NODE");
        // record_operation flips the cluster to UPDATING; a reboot must stay
        // REBOOTING_BROKER until the broker is back, so re-assert it.
        if let Some(obj) = data.clusters.get_mut(arn).and_then(Value::as_object_mut) {
            obj.insert("state".into(), json!("REBOOTING_BROKER"));
        }
        // With a runtime, restart the SAME broker container in place (preserving
        // the topic log) and settle back to ACTIVE once it serves again. Without
        // one, the in-memory reconcile settles it on the next read.
        if self.runtime.is_some() && !serverless {
            drop(guard);
            self.spawn_reboot(ctx.account.clone(), arn.to_string());
        }
        ok(json!({ "clusterArn": arn, "clusterOperationArn": op_arn }))
    }
}

// ===================== cluster operations =====================

impl KafkaService {
    fn list_cluster_operations(
        &self,
        ctx: &Ctx,
        arn: &str,
        q: &[(String, String)],
        v2: bool,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        // ListClusterOperationsV2 declares NotFoundException; the V1 variant does
        // not, so a missing cluster there surfaces as its (declared)
        // BadRequestException.
        if !data.clusters.contains_key(arn) {
            let msg = format!("The cluster '{arn}' does not exist.");
            return Err(if v2 {
                not_found(&msg)
            } else {
                bad_request(&msg)
            });
        }
        let items: Vec<Value> = data
            .operations
            .values()
            .filter(|o| o.get("clusterArn").and_then(Value::as_str) == Some(arn))
            .map(|o| if v2 { render_op_v2(o) } else { public_op(o) })
            .collect();
        let (page, next) = paginate(&items, q);
        let mut out = json!({ "clusterOperationInfoList": page });
        if let Some(n) = next {
            out["nextToken"] = json!(n);
        }
        ok(out)
    }

    fn describe_cluster_operation(
        &self,
        ctx: &Ctx,
        arn: &str,
        v2: bool,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        let op =
            data.operations.get(arn).cloned().ok_or_else(|| {
                not_found(&format!("The cluster operation '{arn}' does not exist."))
            })?;
        let info = if v2 {
            render_op_v2(&op)
        } else {
            public_op(&op)
        };
        ok(json!({ "clusterOperationInfo": info }))
    }
}

/// A cluster operation's public (V1) view: drop internal `_` keys.
fn public_op(op: &Value) -> Value {
    let mut out = Map::new();
    if let Some(m) = op.as_object() {
        for (k, v) in m {
            if !k.starts_with('_') {
                out.insert(k.clone(), v.clone());
            }
        }
    }
    Value::Object(out)
}

/// Project a stored operation into the `ClusterOperationV2` summary shape.
fn render_op_v2(op: &Value) -> Value {
    let ctype = op
        .get("_clusterType")
        .and_then(Value::as_str)
        .unwrap_or("PROVISIONED");
    let mut out = json!({
        "clusterArn": op.get("clusterArn").cloned().unwrap_or(json!("")),
        "clusterType": ctype,
        "startTime": op.get("creationTime").cloned().unwrap_or(json!("")),
        "operationArn": op.get("operationArn").cloned().unwrap_or(json!("")),
        "operationState": op.get("operationState").cloned().unwrap_or(json!("")),
        "operationType": op.get("operationType").cloned().unwrap_or(json!("")),
    });
    if let Some(e) = op.get("endTime") {
        out["endTime"] = e.clone();
    }
    if ctype == "SERVERLESS" {
        out["serverless"] = json!({ "vpcConnectionInfo": {} });
    } else {
        out["provisioned"] = json!({ "operationSteps": [] });
    }
    out
}

// ===================== scram secrets / policy / client vpc =====================

impl KafkaService {
    fn list_scram_secrets(
        &self,
        ctx: &Ctx,
        arn: &str,
        q: &[(String, String)],
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        if !data.clusters.contains_key(arn) {
            return Err(not_found(&format!("The cluster '{arn}' does not exist.")));
        }
        let secrets: Vec<Value> = data
            .scram_secrets
            .get(arn)
            .map(|v| v.iter().map(|s| json!(s)).collect())
            .unwrap_or_default();
        let (page, next) = paginate(&secrets, q);
        let mut out = json!({ "secretArnList": page });
        if let Some(n) = next {
            out["nextToken"] = json!(n);
        }
        ok(out)
    }

    fn batch_scram(
        &self,
        ctx: &Ctx,
        arn: &str,
        b: &Value,
        associate: bool,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        if !data.clusters.contains_key(arn) {
            return Err(not_found(&format!("The cluster '{arn}' does not exist.")));
        }
        let secrets: Vec<String> = b
            .get("secretArnList")
            .and_then(Value::as_array)
            .map(|a| {
                a.iter()
                    .filter_map(|v| v.as_str().map(str::to_string))
                    .collect()
            })
            .unwrap_or_default();
        let entry = data.scram_secrets.entry(arn.to_string()).or_default();
        if associate {
            for s in secrets {
                if !entry.contains(&s) {
                    entry.push(s);
                }
            }
        } else {
            entry.retain(|s| !secrets.contains(s));
        }
        ok(json!({ "clusterArn": arn, "unprocessedScramSecrets": [] }))
    }

    fn get_cluster_policy(&self, ctx: &Ctx, arn: &str) -> Result<AwsResponse, AwsServiceError> {
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        if !data.clusters.contains_key(arn) {
            return Err(not_found(&format!("The cluster '{arn}' does not exist.")));
        }
        let policy = data
            .policies
            .get(arn)
            .ok_or_else(|| not_found(&format!("No cluster policy is attached to '{arn}'.")))?;
        ok(json!({
            "currentVersion": policy.get("version").cloned().unwrap_or(json!("1")),
            "policy": policy.get("policy").cloned().unwrap_or(json!("")),
        }))
    }

    fn put_cluster_policy(
        &self,
        ctx: &Ctx,
        arn: &str,
        b: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        // PutClusterPolicy declares no NotFoundException.
        if !data.clusters.contains_key(arn) {
            return Err(bad_request(&format!("The cluster '{arn}' does not exist.")));
        }
        let policy = b.get("policy").cloned().unwrap_or(json!(""));
        let version = b
            .get("currentVersion")
            .and_then(Value::as_str)
            .map(str::to_string)
            .unwrap_or_else(|| "1".to_string());
        data.policies.insert(
            arn.to_string(),
            json!({ "policy": policy, "version": version }),
        );
        ok(json!({ "currentVersion": version }))
    }

    fn delete_cluster_policy(&self, ctx: &Ctx, arn: &str) -> Result<AwsResponse, AwsServiceError> {
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        if !data.clusters.contains_key(arn) {
            return Err(not_found(&format!("The cluster '{arn}' does not exist.")));
        }
        data.policies.remove(arn);
        ok(json!({}))
    }

    fn list_client_vpc_connections(
        &self,
        ctx: &Ctx,
        arn: &str,
        q: &[(String, String)],
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        // No NotFoundException declared: a missing cluster surfaces as the
        // (declared) BadRequestException.
        if !data.clusters.contains_key(arn) {
            return Err(bad_request(&format!("The cluster '{arn}' does not exist.")));
        }
        let items: Vec<Value> = data
            .vpc_connections
            .values()
            .filter(|v| v.get("targetClusterArn").and_then(Value::as_str) == Some(arn))
            .map(|v| {
                json!({
                    "vpcConnectionArn": v.get("vpcConnectionArn").cloned().unwrap_or(json!("")),
                    "authentication": v.get("authentication").cloned().unwrap_or(json!("")),
                    "creationTime": v.get("creationTime").cloned().unwrap_or(json!("")),
                    "state": v.get("state").cloned().unwrap_or(json!("AVAILABLE")),
                    "owner": ctx.account,
                })
            })
            .collect();
        let (page, next) = paginate(&items, q);
        let mut out = json!({ "clientVpcConnections": page });
        if let Some(n) = next {
            out["nextToken"] = json!(n);
        }
        ok(out)
    }

    fn reject_client_vpc_connection(
        &self,
        ctx: &Ctx,
        arn: &str,
        b: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        // No NotFoundException declared.
        if !data.clusters.contains_key(arn) {
            return Err(bad_request(&format!("The cluster '{arn}' does not exist.")));
        }
        if let Some(vpc_arn) = b.get("vpcConnectionArn").and_then(Value::as_str) {
            if let Some(obj) = data
                .vpc_connections
                .get_mut(vpc_arn)
                .and_then(Value::as_object_mut)
            {
                obj.insert("state".into(), json!("REJECTING"));
            }
        }
        ok(json!({}))
    }
}

// ===================== topics =====================
//
// When a real single-node Kafka broker backs the cluster (a runtime is attached
// AND the cluster's broker is up), topic operations are driven against that
// live broker with its own `/opt/kafka/bin/*.sh` tools -- a topic the API
// creates is genuinely created on Kafka, and a real client can produce/consume
// through it. Without a live broker (no runtime, serverless, or still creating)
// the ops fall back to the in-memory control-plane behavior (a write reflected
// by its read), returning the SAME response shapes so conformance is unchanged.
//
// SINGLE-BROKER SIMPLIFICATION: a real MSK cluster has >=3 brokers, but
// fakecloud runs ONE Kafka container, which can only satisfy replication factor
// 1. A requested `ReplicationFactor > 1` is therefore CLAMPED to 1 when creating
// on the broker (and the stored/described value reflects what the broker
// actually applied, so state never lies about the topic). Internal Kafka topics
// (`__consumer_offsets` etc.) are hidden from `ListTopics`, matching the
// user-topic view MSK presents.

impl KafkaService {
    async fn create_topic(
        &self,
        ctx: &Ctx,
        cluster_arn: &str,
        b: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = b
            .get("topicName")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        let partition_count = b.get("partitionCount").and_then(Value::as_i64).unwrap_or(1);
        let requested_rf = b
            .get("replicationFactor")
            .and_then(Value::as_i64)
            .unwrap_or(1);
        let configs_str = b
            .get("configs")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        let topic_arn = format!("{cluster_arn}/topic/{name}");

        // CreateTopic declares no NotFoundException; a missing cluster surfaces as
        // its (declared) BadRequestException.
        {
            let mut guard = self.state.write();
            let data = guard.get_or_create(&ctx.account);
            if !data.clusters.contains_key(cluster_arn) {
                return Err(bad_request(&format!(
                    "The cluster '{cluster_arn}' does not exist."
                )));
            }
            if data
                .topics
                .get(cluster_arn)
                .is_some_and(|m| m.contains_key(&name))
            {
                return Err(conflict(&format!("Topic '{name}' already exists.")));
            }
        }

        if let (Some(cid), Some(runtime)) = (
            self.cluster_container(&ctx.account, cluster_arn),
            self.runtime.clone(),
        ) {
            // Single-node broker -> replication factor 1 (clamped from request).
            let rf = requested_rf.clamp(1, 1);
            let configs = parse_topic_configs(&configs_str);
            match runtime
                .create_topic(&cid, &name, partition_count, rf, &configs)
                .await
            {
                Ok(()) => {}
                Err(TopicError::AlreadyExists) => {
                    return Err(conflict(&format!("Topic '{name}' already exists.")));
                }
                Err(TopicError::NotFound) => {
                    return Err(bad_request(&format!(
                        "Topic '{name}' could not be created."
                    )));
                }
                Err(TopicError::Broker(msg)) => {
                    return Err(bad_request(&format!(
                        "The broker rejected the topic '{name}': {msg}"
                    )));
                }
            }
            let topic = json!({
                "topicArn": topic_arn,
                "topicName": name,
                "partitionCount": partition_count.max(1),
                "replicationFactor": rf,
                "configs": configs_str,
                "outOfSyncReplicaCount": 0,
                "status": "ACTIVE",
            });
            let mut guard = self.state.write();
            guard
                .get_or_create(&ctx.account)
                .topics
                .entry(cluster_arn.to_string())
                .or_default()
                .insert(name.clone(), topic);
            return ok(json!({
                "topicArn": topic_arn,
                "topicName": name,
                "status": "ACTIVE",
            }));
        }

        // Control-plane-only fallback (no live broker): async CREATING settle.
        let topic = json!({
            "topicArn": topic_arn,
            "topicName": name,
            "partitionCount": b.get("partitionCount").cloned().unwrap_or(json!(1)),
            "replicationFactor": b.get("replicationFactor").cloned().unwrap_or(json!(1)),
            "configs": b.get("configs").cloned().unwrap_or(json!("")),
            "outOfSyncReplicaCount": 0,
            "status": "CREATING",
        });
        let mut guard = self.state.write();
        guard
            .get_or_create(&ctx.account)
            .topics
            .entry(cluster_arn.to_string())
            .or_default()
            .insert(name.clone(), topic);
        ok(json!({
            "topicArn": topic_arn,
            "topicName": name,
            "status": "CREATING",
        }))
    }

    async fn describe_topic(
        &self,
        ctx: &Ctx,
        cluster_arn: &str,
        name: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        if let (Some(cid), Some(runtime)) = (
            self.cluster_container(&ctx.account, cluster_arn),
            self.runtime.clone(),
        ) {
            let meta = match runtime.describe_topic(&cid, name).await {
                Ok(m) => m,
                Err(TopicError::NotFound) => {
                    return Err(not_found(&format!("Topic '{name}' does not exist.")));
                }
                Err(e) => return Err(bad_request(&e.to_string())),
            };
            return ok(json!({
                "topicArn": format!("{cluster_arn}/topic/{name}"),
                "topicName": name,
                "partitionCount": meta.partition_count,
                "replicationFactor": meta.replication_factor,
                "configs": meta.configs,
                "status": "ACTIVE",
            }));
        }
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        let topic = data
            .topics
            .get(cluster_arn)
            .and_then(|m| m.get(name))
            .cloned()
            .ok_or_else(|| not_found(&format!("Topic '{name}' does not exist.")))?;
        ok(json!({
            "topicArn": topic.get("topicArn").cloned().unwrap_or(json!("")),
            "topicName": topic.get("topicName").cloned().unwrap_or(json!(name)),
            "partitionCount": topic.get("partitionCount").cloned().unwrap_or(json!(1)),
            "replicationFactor": topic.get("replicationFactor").cloned().unwrap_or(json!(1)),
            "configs": topic.get("configs").cloned().unwrap_or(json!("")),
            "status": topic.get("status").cloned().unwrap_or(json!("ACTIVE")),
        }))
    }

    async fn update_topic(
        &self,
        ctx: &Ctx,
        cluster_arn: &str,
        name: &str,
        b: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let topic_arn = format!("{cluster_arn}/topic/{name}");
        let new_partitions = b.get("partitionCount").and_then(Value::as_i64);
        let new_configs = b.get("configs").and_then(Value::as_str).map(str::to_string);

        if let (Some(cid), Some(runtime)) = (
            self.cluster_container(&ctx.account, cluster_arn),
            self.runtime.clone(),
        ) {
            if let Some(p) = new_partitions {
                match runtime.alter_partitions(&cid, name, p).await {
                    Ok(()) => {}
                    Err(TopicError::NotFound) => {
                        return Err(not_found(&format!("Topic '{name}' does not exist.")));
                    }
                    Err(e) => return Err(bad_request(&e.to_string())),
                }
            }
            if let Some(cfg) = &new_configs {
                let parsed = parse_topic_configs(cfg);
                match runtime.alter_configs(&cid, name, &parsed).await {
                    Ok(()) => {}
                    Err(TopicError::NotFound) => {
                        return Err(not_found(&format!("Topic '{name}' does not exist.")));
                    }
                    Err(e) => return Err(bad_request(&e.to_string())),
                }
            }
            // Mirror the applied change into state (source of truth stays the
            // broker; this keeps the fallback view and snapshot consistent).
            let mut guard = self.state.write();
            let data = guard.get_or_create(&ctx.account);
            if let Some(obj) = data
                .topics
                .get_mut(cluster_arn)
                .and_then(|m| m.get_mut(name))
                .and_then(Value::as_object_mut)
            {
                if let Some(p) = new_partitions {
                    obj.insert("partitionCount".into(), json!(p));
                }
                if let Some(cfg) = &new_configs {
                    obj.insert("configs".into(), json!(cfg));
                }
                obj.insert("status".into(), json!("ACTIVE"));
            }
            return ok(json!({ "topicArn": topic_arn, "topicName": name, "status": "ACTIVE" }));
        }

        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        let topic = data
            .topics
            .get_mut(cluster_arn)
            .and_then(|m| m.get_mut(name))
            .ok_or_else(|| not_found(&format!("Topic '{name}' does not exist.")))?;
        let obj = topic.as_object_mut().expect("topic is object");
        if let Some(v) = b.get("configs") {
            obj.insert("configs".into(), v.clone());
        }
        if let Some(v) = b.get("partitionCount") {
            obj.insert("partitionCount".into(), v.clone());
        }
        obj.insert("status".into(), json!("UPDATING"));
        let arn = obj.get("topicArn").cloned().unwrap_or(json!(""));
        ok(json!({ "topicArn": arn, "topicName": name, "status": "UPDATING" }))
    }

    async fn delete_topic(
        &self,
        ctx: &Ctx,
        cluster_arn: &str,
        name: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let topic_arn = format!("{cluster_arn}/topic/{name}");
        if let (Some(cid), Some(runtime)) = (
            self.cluster_container(&ctx.account, cluster_arn),
            self.runtime.clone(),
        ) {
            match runtime.delete_topic(&cid, name).await {
                Ok(()) => {}
                Err(TopicError::NotFound) => {
                    return Err(not_found(&format!("Topic '{name}' does not exist.")));
                }
                Err(e) => return Err(bad_request(&e.to_string())),
            }
            // The broker deleted it synchronously, so free the state entry now
            // (reconcile does not settle topics in runtime mode).
            let mut guard = self.state.write();
            if let Some(m) = guard
                .get_or_create(&ctx.account)
                .topics
                .get_mut(cluster_arn)
            {
                m.remove(name);
            }
            return ok(json!({ "topicArn": topic_arn, "topicName": name, "status": "DELETING" }));
        }
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        let topic = data
            .topics
            .get_mut(cluster_arn)
            .and_then(|m| m.get_mut(name))
            .ok_or_else(|| not_found(&format!("Topic '{name}' does not exist.")))?;
        topic
            .as_object_mut()
            .expect("topic is object")
            .insert("status".into(), json!("DELETING"));
        let arn = topic.get("topicArn").cloned().unwrap_or(json!(""));
        ok(json!({ "topicArn": arn, "topicName": name, "status": "DELETING" }))
    }

    async fn describe_topic_partitions(
        &self,
        ctx: &Ctx,
        cluster_arn: &str,
        name: &str,
        q: &[(String, String)],
    ) -> Result<AwsResponse, AwsServiceError> {
        if let (Some(cid), Some(runtime)) = (
            self.cluster_container(&ctx.account, cluster_arn),
            self.runtime.clone(),
        ) {
            let meta = match runtime.describe_topic(&cid, name).await {
                Ok(m) => m,
                Err(TopicError::NotFound) => {
                    return Err(not_found(&format!("Topic '{name}' does not exist.")));
                }
                Err(e) => return Err(bad_request(&e.to_string())),
            };
            let items: Vec<Value> = meta
                .partitions
                .iter()
                .map(|p| {
                    json!({
                        "partition": p.partition,
                        "leader": p.leader,
                        "replicas": p.replicas,
                        "isr": p.isr,
                    })
                })
                .collect();
            let (page, next) = paginate(&items, q);
            let mut out = json!({ "partitions": page });
            if let Some(n) = next {
                out["nextToken"] = json!(n);
            }
            return ok(out);
        }

        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        let topic = data
            .topics
            .get(cluster_arn)
            .and_then(|m| m.get(name))
            .ok_or_else(|| not_found(&format!("Topic '{name}' does not exist.")))?;
        let partitions = topic
            .get("partitionCount")
            .and_then(Value::as_i64)
            .unwrap_or(1)
            .max(0);
        let brokers = data
            .clusters
            .get(cluster_arn)
            .map(cluster_num_brokers)
            .unwrap_or(3)
            .max(1);
        let items: Vec<Value> = (0..partitions)
            .map(|p| {
                let leader = (p % brokers) + 1;
                let replicas: Vec<Value> = (0..brokers.min(3))
                    .map(|r| json!(((p + r) % brokers) + 1))
                    .collect();
                json!({
                    "partition": p,
                    "leader": leader,
                    "replicas": replicas,
                    "isr": replicas,
                })
            })
            .collect();
        let (page, next) = paginate(&items, q);
        let mut out = json!({ "partitions": page });
        if let Some(n) = next {
            out["nextToken"] = json!(n);
        }
        ok(out)
    }

    async fn list_topics(
        &self,
        ctx: &Ctx,
        cluster_arn: &str,
        q: &[(String, String)],
    ) -> Result<AwsResponse, AwsServiceError> {
        let name_filter = query_one(q, "topicNameFilter").map(str::to_string);

        // A missing cluster surfaces as the (declared) BadRequestException.
        {
            let mut guard = self.state.write();
            if !guard
                .get_or_create(&ctx.account)
                .clusters
                .contains_key(cluster_arn)
            {
                return Err(bad_request(&format!(
                    "The cluster '{cluster_arn}' does not exist."
                )));
            }
        }

        if let (Some(cid), Some(runtime)) = (
            self.cluster_container(&ctx.account, cluster_arn),
            self.runtime.clone(),
        ) {
            let topics = runtime
                .list_topics(&cid)
                .await
                .map_err(|e| bad_request(&e.to_string()))?;
            let items: Vec<Value> = topics
                .iter()
                // Hide internal Kafka topics (`__consumer_offsets`, etc.).
                .filter(|t| !t.name.starts_with("__"))
                .filter(|t| name_filter.as_deref().is_none_or(|f| t.name.starts_with(f)))
                .map(|t| {
                    json!({
                        "topicArn": format!("{cluster_arn}/topic/{}", t.name),
                        "topicName": t.name,
                        "partitionCount": t.partition_count,
                        "replicationFactor": t.replication_factor,
                        "outOfSyncReplicaCount": 0,
                    })
                })
                .collect();
            let (page, next) = paginate(&items, q);
            let mut out = json!({ "topics": page });
            if let Some(n) = next {
                out["nextToken"] = json!(n);
            }
            return ok(out);
        }

        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        let items: Vec<Value> = data
            .topics
            .get(cluster_arn)
            .map(|m| {
                m.values()
                    .filter(|t| {
                        name_filter.as_deref().is_none_or(|f| {
                            t.get("topicName")
                                .and_then(Value::as_str)
                                .is_some_and(|n| n.starts_with(f))
                        })
                    })
                    .map(|t| {
                        json!({
                            "topicArn": t.get("topicArn").cloned().unwrap_or(json!("")),
                            "topicName": t.get("topicName").cloned().unwrap_or(json!("")),
                            "partitionCount": t.get("partitionCount").cloned().unwrap_or(json!(1)),
                            "replicationFactor": t.get("replicationFactor").cloned().unwrap_or(json!(1)),
                            "outOfSyncReplicaCount": 0,
                        })
                    })
                    .collect()
            })
            .unwrap_or_default();
        let (page, next) = paginate(&items, q);
        let mut out = json!({ "topics": page });
        if let Some(n) = next {
            out["nextToken"] = json!(n);
        }
        ok(out)
    }
}

/// Parse an MSK `Configs` string into Kafka topic config `(key, value)` pairs.
/// Tolerates comma / semicolon / newline separators and `key=value` entries
/// (whitespace-trimmed; blank and malformed entries are dropped).
fn parse_topic_configs(s: &str) -> Vec<(String, String)> {
    s.split([',', ';', '\n'])
        .filter_map(|entry| {
            let (k, v) = entry.split_once('=')?;
            let (k, v) = (k.trim(), v.trim());
            if k.is_empty() {
                None
            } else {
                Some((k.to_string(), v.to_string()))
            }
        })
        .collect()
}

// ===================== configurations =====================

impl KafkaService {
    fn create_configuration(&self, ctx: &Ctx, b: &Value) -> Result<AwsResponse, AwsServiceError> {
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        // Shared builder: same insert path the CloudFormation
        // `AWS::MSK::Configuration` provisioner uses.
        let arn = builders::insert_configuration(data, &ctx.region, &ctx.account, b);
        let cfg = data
            .configurations
            .get(&arn)
            .expect("configuration was just inserted");
        ok(json!({
            "arn": arn,
            "creationTime": cfg.get("creationTime").cloned().unwrap_or(json!("")),
            "latestRevision": cfg.get("latestRevision").cloned().unwrap_or(json!(null)),
            "name": cfg.get("name").cloned().unwrap_or(json!("")),
            "state": "ACTIVE",
        }))
    }

    fn describe_configuration(&self, ctx: &Ctx, arn: &str) -> Result<AwsResponse, AwsServiceError> {
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        // AWS MSK reports a missing configuration as a `BadRequestException`
        // whose message contains "Configuration ARN does not exist" (NOT a 404) --
        // the terraform provider keys its not-found detection on exactly that,
        // so its delete/read waiters depend on this shape.
        let cfg = data
            .configurations
            .get(arn)
            .cloned()
            .ok_or_else(|| bad_request(&format!("Configuration ARN does not exist : {arn}")))?;
        ok(cfg)
    }

    fn list_configurations(
        &self,
        ctx: &Ctx,
        q: &[(String, String)],
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        let items: Vec<Value> = data
            .configurations
            .values()
            .filter(|c| {
                c.get("arn")
                    .and_then(Value::as_str)
                    .and_then(shared::arn_region)
                    == Some(ctx.region.as_str())
            })
            .cloned()
            .collect();
        let (page, next) = paginate(&items, q);
        let mut out = json!({ "configurations": page });
        if let Some(n) = next {
            out["nextToken"] = json!(n);
        }
        ok(out)
    }

    fn update_configuration(
        &self,
        ctx: &Ctx,
        arn: &str,
        b: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let description = b
            .get("description")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        let server_properties = b
            .get("serverProperties")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        if !data.configurations.contains_key(arn) {
            return Err(not_found(&format!(
                "The configuration '{arn}' does not exist."
            )));
        }
        let created = now_iso();
        let revs = data
            .configuration_revisions
            .entry(arn.to_string())
            .or_default();
        let next_rev = revs
            .iter()
            .filter_map(|r| r.get("revision").and_then(Value::as_i64))
            .max()
            .unwrap_or(0)
            + 1;
        revs.push(json!({
            "creationTime": created,
            "description": description,
            "revision": next_rev,
            "serverProperties": server_properties,
        }));
        let latest =
            json!({ "creationTime": created, "description": description, "revision": next_rev });
        let cfg = data.configurations.get_mut(arn).expect("checked above");
        cfg["latestRevision"] = latest.clone();
        ok(json!({ "arn": arn, "latestRevision": latest }))
    }

    fn delete_configuration(&self, ctx: &Ctx, arn: &str) -> Result<AwsResponse, AwsServiceError> {
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        // A missing configuration surfaces as MSK's `BadRequestException`
        // ("Configuration ARN does not exist"), matching DescribeConfiguration --
        // the terraform provider treats exactly this shape as already-deleted.
        if !data.configurations.contains_key(arn) {
            return Err(bad_request(&format!(
                "Configuration ARN does not exist : {arn}"
            )));
        }
        data.configurations.remove(arn);
        data.configuration_revisions.remove(arn);
        data.tags.remove(arn);
        ok(json!({ "arn": arn, "state": "DELETING" }))
    }

    fn list_configuration_revisions(
        &self,
        ctx: &Ctx,
        arn: &str,
        q: &[(String, String)],
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        let revs = data
            .configuration_revisions
            .get(arn)
            .cloned()
            .ok_or_else(|| not_found(&format!("The configuration '{arn}' does not exist.")))?;
        let items: Vec<Value> = revs
            .iter()
            .map(|r| {
                json!({
                    "creationTime": r.get("creationTime").cloned().unwrap_or(json!("")),
                    "description": r.get("description").cloned().unwrap_or(json!("")),
                    "revision": r.get("revision").cloned().unwrap_or(json!(1)),
                })
            })
            .collect();
        let (page, next) = paginate(&items, q);
        let mut out = json!({ "revisions": page });
        if let Some(n) = next {
            out["nextToken"] = json!(n);
        }
        ok(out)
    }

    fn describe_configuration_revision(
        &self,
        ctx: &Ctx,
        arn: &str,
        rev: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        let revs = data
            .configuration_revisions
            .get(arn)
            .ok_or_else(|| not_found(&format!("The configuration '{arn}' does not exist.")))?;
        let rev_num: i64 = rev
            .parse()
            .map_err(|_| bad_request(&format!("Invalid revision '{rev}'.")))?;
        let r = revs
            .iter()
            .find(|r| r.get("revision").and_then(Value::as_i64) == Some(rev_num))
            .ok_or_else(|| not_found(&format!("Revision '{rev}' does not exist.")))?;
        ok(json!({
            "arn": arn,
            "creationTime": r.get("creationTime").cloned().unwrap_or(json!("")),
            "description": r.get("description").cloned().unwrap_or(json!("")),
            "revision": rev_num,
            "serverProperties": r.get("serverProperties").cloned().unwrap_or(json!("")),
        }))
    }
}

// ===================== versions =====================

impl KafkaService {
    fn list_kafka_versions(&self, q: &[(String, String)]) -> Result<AwsResponse, AwsServiceError> {
        let versions = shared::kafka_versions_list();
        let (page, next) = paginate(&versions, q);
        let mut out = json!({ "kafkaVersions": page });
        if let Some(n) = next {
            out["nextToken"] = json!(n);
        }
        ok(out)
    }

    fn get_compatible_kafka_versions(
        &self,
        ctx: &Ctx,
        q: &[(String, String)],
    ) -> Result<AwsResponse, AwsServiceError> {
        let source = match query_one(q, "clusterArn") {
            Some(arn) => {
                let mut guard = self.state.write();
                let data = guard.get_or_create(&ctx.account);
                data.clusters
                    .get(arn)
                    .map(cluster_kafka_version)
                    .unwrap_or_else(|| shared::DEFAULT_KAFKA_VERSION.to_string())
            }
            None => shared::DEFAULT_KAFKA_VERSION.to_string(),
        };
        ok(json!({ "compatibleKafkaVersions": shared::compatible_versions_for(&source) }))
    }
}

// ===================== vpc connections =====================

impl KafkaService {
    fn create_vpc_connection(&self, ctx: &Ctx, b: &Value) -> Result<AwsResponse, AwsServiceError> {
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        // Shared builder: same insert path the CloudFormation
        // `AWS::MSK::VpcConnection` provisioner uses.
        let arn = builders::insert_vpc_connection(data, &ctx.region, &ctx.account, b);
        let rec = data
            .vpc_connections
            .get(&arn)
            .expect("vpc connection was just inserted");
        ok(json!({
            "vpcConnectionArn": arn,
            "state": "CREATING",
            "authentication": rec.get("authentication").cloned().unwrap_or(json!("")),
            "vpcId": rec.get("vpcId").cloned().unwrap_or(json!("")),
            "clientSubnets": rec.get("subnets").cloned().unwrap_or(json!([])),
            "securityGroups": rec.get("securityGroups").cloned().unwrap_or(json!([])),
            "creationTime": rec.get("creationTime").cloned().unwrap_or(json!("")),
            "tags": b.get("tags").cloned().unwrap_or(json!({})),
        }))
    }

    fn list_vpc_connections(
        &self,
        ctx: &Ctx,
        q: &[(String, String)],
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        let items: Vec<Value> = data
            .vpc_connections
            .values()
            .filter(|v| {
                v.get("vpcConnectionArn")
                    .and_then(Value::as_str)
                    .and_then(shared::arn_region)
                    == Some(ctx.region.as_str())
            })
            .map(|v| {
                json!({
                    "vpcConnectionArn": v.get("vpcConnectionArn").cloned().unwrap_or(json!("")),
                    "targetClusterArn": v.get("targetClusterArn").cloned().unwrap_or(json!("")),
                    "creationTime": v.get("creationTime").cloned().unwrap_or(json!("")),
                    "authentication": v.get("authentication").cloned().unwrap_or(json!("")),
                    "vpcId": v.get("vpcId").cloned().unwrap_or(json!("")),
                    "state": v.get("state").cloned().unwrap_or(json!("AVAILABLE")),
                })
            })
            .collect();
        let (page, next) = paginate(&items, q);
        let mut out = json!({ "vpcConnections": page });
        if let Some(n) = next {
            out["nextToken"] = json!(n);
        }
        ok(out)
    }

    fn describe_vpc_connection(
        &self,
        ctx: &Ctx,
        arn: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        let v = data
            .vpc_connections
            .get(arn)
            .cloned()
            .ok_or_else(|| not_found(&format!("The VPC connection '{arn}' does not exist.")))?;
        let mut out = public_view(data, arn, &v);
        // Ensure the tags member is always present in the response object.
        if out.get("tags").is_none() {
            out["tags"] = json!({});
        }
        ok(out)
    }

    fn delete_vpc_connection(&self, ctx: &Ctx, arn: &str) -> Result<AwsResponse, AwsServiceError> {
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        let v = data
            .vpc_connections
            .get_mut(arn)
            .ok_or_else(|| not_found(&format!("The VPC connection '{arn}' does not exist.")))?;
        v.as_object_mut()
            .expect("vpc connection is object")
            .insert("state".into(), json!("DELETING"));
        ok(json!({ "vpcConnectionArn": arn, "state": "DELETING" }))
    }
}

// ===================== replicators =====================

impl KafkaService {
    fn create_replicator(&self, ctx: &Ctx, b: &Value) -> Result<AwsResponse, AwsServiceError> {
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        // Shared builder: same insert path the CloudFormation
        // `AWS::MSK::Replicator` provisioner uses.
        let arn = builders::insert_replicator(data, &ctx.region, &ctx.account, b)
            .map_err(|msg| conflict(&msg))?;
        let rec = data
            .replicators
            .get(&arn)
            .expect("replicator was just inserted");
        ok(json!({
            "replicatorArn": arn,
            "replicatorName": rec.get("replicatorName").cloned().unwrap_or(json!("")),
            "replicatorState": "CREATING",
        }))
    }

    fn list_replicators(
        &self,
        ctx: &Ctx,
        q: &[(String, String)],
    ) -> Result<AwsResponse, AwsServiceError> {
        let name_filter = query_one(q, "replicatorNameFilter");
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        let items: Vec<Value> = data
            .replicators
            .values()
            .filter(|r| {
                r.get("replicatorArn")
                    .and_then(Value::as_str)
                    .and_then(shared::arn_region)
                    == Some(ctx.region.as_str())
            })
            .filter(|r| {
                name_filter.is_none_or(|f| {
                    r.get("replicatorName")
                        .and_then(Value::as_str)
                        .is_some_and(|n| n.starts_with(f))
                })
            })
            .map(|r| {
                json!({
                    "creationTime": r.get("creationTime").cloned().unwrap_or(json!("")),
                    "currentVersion": r.get("currentVersion").cloned().unwrap_or(json!("")),
                    "isReplicatorReference": r.get("isReplicatorReference").cloned().unwrap_or(json!(false)),
                    "replicatorArn": r.get("replicatorArn").cloned().unwrap_or(json!("")),
                    "replicatorName": r.get("replicatorName").cloned().unwrap_or(json!("")),
                    "replicatorResourceArn": r.get("replicatorResourceArn").cloned().unwrap_or(json!("")),
                    "replicatorState": r.get("replicatorState").cloned().unwrap_or(json!("RUNNING")),
                })
            })
            .collect();
        let (page, next) = paginate(&items, q);
        let mut out = json!({ "replicators": page });
        if let Some(n) = next {
            out["nextToken"] = json!(n);
        }
        ok(out)
    }

    fn describe_replicator(&self, ctx: &Ctx, arn: &str) -> Result<AwsResponse, AwsServiceError> {
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        let r = data
            .replicators
            .get(arn)
            .cloned()
            .ok_or_else(|| not_found(&format!("The replicator '{arn}' does not exist.")))?;
        let mut out = public_view(data, arn, &r);
        if out.get("tags").is_none() {
            out["tags"] = json!({});
        }
        ok(out)
    }

    fn delete_replicator(&self, ctx: &Ctx, arn: &str) -> Result<AwsResponse, AwsServiceError> {
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        let r = data
            .replicators
            .get_mut(arn)
            .ok_or_else(|| not_found(&format!("The replicator '{arn}' does not exist.")))?;
        r.as_object_mut()
            .expect("replicator is object")
            .insert("replicatorState".into(), json!("DELETING"));
        ok(json!({ "replicatorArn": arn, "replicatorState": "DELETING" }))
    }

    fn update_replication_info(
        &self,
        ctx: &Ctx,
        arn: &str,
        b: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        let n = data.next_seq();
        let r = data
            .replicators
            .get_mut(arn)
            .ok_or_else(|| not_found(&format!("The replicator '{arn}' does not exist.")))?;
        let obj = r.as_object_mut().expect("replicator is object");
        obj.insert("replicatorState".into(), json!("UPDATING"));
        obj.insert("currentVersion".into(), json!(gen_version(n)));
        // Reflect the topic / consumer-group replication changes onto the stored
        // (description-shaped) replication info so a re-describe echoes them. The
        // Update shapes carry the same member jsonNames as the description ones,
        // so a field-wise merge preserves create-only members (e.g.
        // `startingPosition`) the update payload does not include.
        let topic = b.get("topicReplication").cloned();
        let consumer = b.get("consumerGroupReplication").cloned();
        if let Some(info) = obj
            .get_mut("replicationInfoList")
            .and_then(Value::as_array_mut)
            .and_then(|l| l.first_mut())
            .and_then(Value::as_object_mut)
        {
            merge_replication_sub(info, "topicReplication", topic);
            merge_replication_sub(info, "consumerGroupReplication", consumer);
        }
        ok(json!({ "replicatorArn": arn, "replicatorState": "UPDATING" }))
    }
}

// ===================== tags =====================

impl KafkaService {
    fn tag_resource(
        &self,
        ctx: &Ctx,
        arn: &str,
        b: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        if !resource_exists(data, arn) {
            return Err(not_found(&format!("The resource '{arn}' does not exist.")));
        }
        let entry = data.tags.entry(arn.to_string()).or_default();
        if let Some(tags) = b.get("tags").and_then(Value::as_object) {
            for (k, v) in tags {
                if let Some(s) = v.as_str() {
                    entry.insert(k.clone(), s.to_string());
                }
            }
        }
        no_content()
    }

    fn untag_resource(
        &self,
        ctx: &Ctx,
        arn: &str,
        q: &[(String, String)],
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        if !resource_exists(data, arn) {
            return Err(not_found(&format!("The resource '{arn}' does not exist.")));
        }
        if let Some(entry) = data.tags.get_mut(arn) {
            for key in query_all(q, "tagKeys") {
                entry.remove(&key);
            }
        }
        no_content()
    }

    fn list_tags(&self, ctx: &Ctx, arn: &str) -> Result<AwsResponse, AwsServiceError> {
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        if !resource_exists(data, arn) {
            return Err(not_found(&format!("The resource '{arn}' does not exist.")));
        }
        let tags: Map<String, Value> = data
            .tags
            .get(arn)
            .map(|m| m.iter().map(|(k, v)| (k.clone(), json!(v))).collect())
            .unwrap_or_default();
        ok(json!({ "tags": Value::Object(tags) }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fakecloud_core::multi_account::MultiAccountState;
    use parking_lot::RwLock;

    fn svc() -> KafkaService {
        let state = Arc::new(RwLock::new(MultiAccountState::new(
            "123456789012",
            "us-east-1",
            "",
        )));
        KafkaService::new(state)
    }

    fn ctx(region: &str) -> Ctx {
        Ctx {
            account: "123456789012".to_string(),
            region: region.to_string(),
        }
    }

    fn json_of(resp: AwsResponse) -> Value {
        serde_json::from_slice(resp.body.expect_bytes()).expect("json response body")
    }

    fn cluster_body(name: &str) -> Value {
        json!({
            "clusterName": name,
            "kafkaVersion": "3.6.0",
            "numberOfBrokerNodes": 3,
            "brokerNodeGroupInfo": {
                "clientSubnets": ["subnet-a", "subnet-b"],
                "instanceType": "kafka.m5.large",
            },
        })
    }

    fn settle(s: &KafkaService, c: &Ctx) {
        // No runtime attached in unit tests, so the full state-machine settles.
        s.state.write().get_or_create(&c.account).reconcile(false);
    }

    #[test]
    fn create_describe_settles_to_active() {
        let s = svc();
        let c = ctx("us-east-1");
        let created = json_of(s.create_cluster(&c, &cluster_body("demo"), false).unwrap());
        assert_eq!(created["state"].as_str(), Some("CREATING"));
        let arn = created["clusterArn"].as_str().unwrap().to_string();
        assert!(arn.contains(":cluster/demo/"));

        // First describe settles CREATING -> ACTIVE.
        settle(&s, &c);
        let described = json_of(s.describe_cluster(&c, &arn, false).unwrap());
        assert_eq!(described["clusterInfo"]["state"].as_str(), Some("ACTIVE"));
    }

    #[test]
    fn duplicate_cluster_name_conflicts() {
        let s = svc();
        let c = ctx("us-east-1");
        s.create_cluster(&c, &cluster_body("dup"), false).unwrap();
        let Err(err) = s.create_cluster(&c, &cluster_body("dup"), false) else {
            panic!("duplicate name must conflict");
        };
        assert_eq!(err.status(), StatusCode::CONFLICT);
    }

    #[test]
    fn arn_suffix_is_monotonic() {
        let s = svc();
        let c = ctx("us-east-1");
        let a = json_of(s.create_cluster(&c, &cluster_body("a"), false).unwrap());
        let b = json_of(s.create_cluster(&c, &cluster_body("b"), false).unwrap());
        let an: u64 = a["clusterArn"]
            .as_str()
            .unwrap()
            .rsplit('-')
            .next()
            .unwrap()
            .parse()
            .unwrap();
        let bn: u64 = b["clusterArn"]
            .as_str()
            .unwrap()
            .rsplit('-')
            .next()
            .unwrap()
            .parse()
            .unwrap();
        assert!(bn > an, "monotonic -N suffix: {an} then {bn}");
    }

    #[test]
    fn update_broker_count_records_operation_and_settles() {
        let s = svc();
        let c = ctx("us-east-1");
        let created = json_of(s.create_cluster(&c, &cluster_body("upd"), false).unwrap());
        let arn = created["clusterArn"].as_str().unwrap().to_string();
        settle(&s, &c);
        let resp = json_of(
            s.update_broker_count(
                &c,
                &arn,
                &json!({ "currentVersion": "x", "targetNumberOfBrokerNodes": 6 }),
            )
            .unwrap(),
        );
        let op_arn = resp["clusterOperationArn"].as_str().unwrap().to_string();
        assert!(op_arn.contains(":cluster-operation/"));
        // Cluster is UPDATING until the next reconcile.
        let d1 = json_of(s.describe_cluster(&c, &arn, false).unwrap());
        assert_eq!(d1["clusterInfo"]["numberOfBrokerNodes"], json!(6));
        // Operation settles to UPDATE_COMPLETE.
        settle(&s, &c);
        let op = json_of(s.describe_cluster_operation(&c, &op_arn, false).unwrap());
        assert_eq!(
            op["clusterOperationInfo"]["operationState"].as_str(),
            Some("UPDATE_COMPLETE")
        );
    }

    #[test]
    fn configuration_revisions_increment() {
        let s = svc();
        let c = ctx("us-east-1");
        let created = json_of(
            s.create_configuration(
                &c,
                &json!({ "name": "cfg", "serverProperties": "YXV0by5jcmVhdGU9dHJ1ZQ==" }),
            )
            .unwrap(),
        );
        let arn = created["arn"].as_str().unwrap().to_string();
        assert_eq!(created["latestRevision"]["revision"], json!(1));
        let updated = json_of(
            s.update_configuration(
                &c,
                &arn,
                &json!({ "serverProperties": "eA==", "description": "v2" }),
            )
            .unwrap(),
        );
        assert_eq!(updated["latestRevision"]["revision"], json!(2));
        let rev = json_of(s.describe_configuration_revision(&c, &arn, "2").unwrap());
        assert_eq!(rev["revision"], json!(2));
        assert_eq!(rev["serverProperties"].as_str(), Some("eA=="));
    }

    #[tokio::test]
    async fn topic_round_trip() {
        // No runtime attached -> control-plane-only fallback (in-memory topics).
        let s = svc();
        let c = ctx("us-east-1");
        let cluster = json_of(s.create_cluster(&c, &cluster_body("t"), false).unwrap());
        let arn = cluster["clusterArn"].as_str().unwrap().to_string();
        s.create_topic(
            &c,
            &arn,
            &json!({ "topicName": "events", "partitionCount": 3, "replicationFactor": 2 }),
        )
        .await
        .unwrap();
        let d = json_of(s.describe_topic(&c, &arn, "events").await.unwrap());
        assert_eq!(d["partitionCount"], json!(3));
        // The fallback echoes the requested replication factor (no broker to
        // clamp against in control-plane-only mode).
        assert_eq!(d["replicationFactor"], json!(2));
        let parts = json_of(
            s.describe_topic_partitions(&c, &arn, "events", &[])
                .await
                .unwrap(),
        );
        assert_eq!(parts["partitions"].as_array().unwrap().len(), 3);
    }

    #[tokio::test]
    async fn bootstrap_brokers_default_tls_only() {
        // A default cluster (client_broker=TLS, no SASL) advertises ONLY the TLS
        // variant; the plaintext + SASL + public + vpc variants are omitted (the
        // provider asserts those are empty).
        let s = svc();
        let c = ctx("us-east-1");
        let cluster = json_of(s.create_cluster(&c, &cluster_body("nb"), false).unwrap());
        let arn = cluster["clusterArn"].as_str().unwrap().to_string();
        let bb = json_of(s.get_bootstrap_brokers(&c, &arn).unwrap());
        assert!(bb.get("bootstrapBrokerString").is_none());
        assert!(bb["bootstrapBrokerStringTls"]
            .as_str()
            .unwrap()
            .contains(".amazonaws.com:9094"));
        assert!(bb.get("bootstrapBrokerStringSaslScram").is_none());
        assert!(bb.get("bootstrapBrokerStringSaslIam").is_none());
        assert!(bb.get("bootstrapBrokerStringPublicSaslIam").is_none());
    }

    #[tokio::test]
    async fn bootstrap_brokers_gated_on_client_auth_and_encryption() {
        // client_broker=PLAINTEXT offers the plaintext string and omits TLS;
        // enabling SASL/SCRAM offers the scram string and, being SASL, drops TLS.
        let s = svc();
        let c = ctx("us-east-1");
        // PLAINTEXT-only cluster.
        let mut body = cluster_body("pt");
        body["encryptionInfo"] = json!({ "encryptionInTransit": { "clientBroker": "PLAINTEXT" } });
        let arn = json_of(s.create_cluster(&c, &body, false).unwrap())["clusterArn"]
            .as_str()
            .unwrap()
            .to_string();
        let bb = json_of(s.get_bootstrap_brokers(&c, &arn).unwrap());
        assert!(bb["bootstrapBrokerString"]
            .as_str()
            .unwrap()
            .contains(":9092"));
        assert!(bb.get("bootstrapBrokerStringTls").is_none());

        // SASL/SCRAM cluster (default TLS in transit).
        let mut body = cluster_body("sc");
        body["clientAuthentication"] = json!({ "sasl": { "scram": { "enabled": true } } });
        let arn = json_of(s.create_cluster(&c, &body, false).unwrap())["clusterArn"]
            .as_str()
            .unwrap()
            .to_string();
        let bb = json_of(s.get_bootstrap_brokers(&c, &arn).unwrap());
        assert!(bb["bootstrapBrokerStringSaslScram"]
            .as_str()
            .unwrap()
            .contains(":9096"));
        assert!(bb.get("bootstrapBrokerStringTls").is_none());
        assert!(bb.get("bootstrapBrokerStringSaslIam").is_none());
    }

    #[tokio::test]
    async fn bootstrap_brokers_public_gated_on_public_access() {
        // Public-access + SASL/IAM advertises both the private and the public
        // SASL/IAM strings (on their distinct ports).
        let s = svc();
        let c = ctx("us-east-1");
        let mut body = cluster_body("pub");
        body["clientAuthentication"] = json!({ "sasl": { "iam": { "enabled": true } } });
        body["brokerNodeGroupInfo"]["connectivityInfo"] =
            json!({ "publicAccess": { "type": "SERVICE_PROVIDED_EIPS" } });
        let arn = json_of(s.create_cluster(&c, &body, false).unwrap())["clusterArn"]
            .as_str()
            .unwrap()
            .to_string();
        let bb = json_of(s.get_bootstrap_brokers(&c, &arn).unwrap());
        assert!(bb["bootstrapBrokerStringSaslIam"]
            .as_str()
            .unwrap()
            .contains(":9098"));
        assert!(bb["bootstrapBrokerStringPublicSaslIam"]
            .as_str()
            .unwrap()
            .contains(":9198"));
    }

    #[tokio::test]
    async fn bootstrap_brokers_with_data_plane_returns_real_endpoint() {
        // A recorded data-plane binding makes GetBootstrapBrokers return the
        // REAL reachable host:port as the plaintext bootstrap string, on top of
        // the cluster's gated (synthesized) TLS/SASL variants.
        let s = svc();
        let c = ctx("us-east-1");
        let cluster = json_of(s.create_cluster(&c, &cluster_body("rp"), false).unwrap());
        let arn = cluster["clusterArn"].as_str().unwrap().to_string();
        {
            let mut guard = s.state.write();
            guard.get_or_create(&c.account).data_plane.insert(
                arn.clone(),
                crate::state::ClusterDataPlane {
                    container_id: "cid".into(),
                    host: "127.0.0.1".into(),
                    ports: std::collections::BTreeMap::from([("plaintext".to_string(), 51234)]),
                },
            );
        }
        let bb = json_of(s.get_bootstrap_brokers(&c, &arn).unwrap());
        assert_eq!(
            bb["bootstrapBrokerString"].as_str(),
            Some("127.0.0.1:51234")
        );
        // The cluster's TLS variant is still synthesized (default TLS in transit).
        assert!(bb["bootstrapBrokerStringTls"]
            .as_str()
            .unwrap()
            .contains(":9094"));
    }

    #[test]
    fn bootstrap_brokers_derived_from_broker_count() {
        let s = svc();
        let c = ctx("us-east-1");
        // TLS_PLAINTEXT surfaces both plaintext and TLS strings, one per broker.
        let mut body = cluster_body("bb");
        body["encryptionInfo"] =
            json!({ "encryptionInTransit": { "clientBroker": "TLS_PLAINTEXT" } });
        let arn = json_of(s.create_cluster(&c, &body, false).unwrap())["clusterArn"]
            .as_str()
            .unwrap()
            .to_string();
        let bb = json_of(s.get_bootstrap_brokers(&c, &arn).unwrap());
        let s9092 = bb["bootstrapBrokerString"].as_str().unwrap();
        assert_eq!(s9092.split(',').count(), 3, "one endpoint per broker node");
        assert!(s9092.contains(":9092"));
        assert!(bb["bootstrapBrokerStringTls"]
            .as_str()
            .unwrap()
            .contains(":9094"));
    }

    #[test]
    fn describe_cluster_echoes_connectivity_and_storage_defaults() {
        // A minimal provisioned create round-trips the connectivity_info +
        // storage_info sub-objects with AWS defaults (the deferred-tfacc gap).
        let s = svc();
        let c = ctx("us-east-1");
        let arn = json_of(s.create_cluster(&c, &cluster_body("df"), false).unwrap())["clusterArn"]
            .as_str()
            .unwrap()
            .to_string();
        settle(&s, &c);
        let d = json_of(s.describe_cluster(&c, &arn, false).unwrap());
        let bngi = &d["clusterInfo"]["brokerNodeGroupInfo"];
        assert_eq!(bngi["brokerAZDistribution"], json!("DEFAULT"));
        assert_eq!(
            bngi["connectivityInfo"]["publicAccess"]["type"],
            json!("DISABLED")
        );
        assert_eq!(
            bngi["connectivityInfo"]["vpcConnectivity"]["clientAuthentication"]["sasl"]["iam"]
                ["enabled"],
            json!(false)
        );
        assert_eq!(
            bngi["connectivityInfo"]["vpcConnectivity"]["clientAuthentication"]["tls"]["enabled"],
            json!(false)
        );
        // No provisioned throughput on a cluster that never enabled it.
        assert!(bngi["storageInfo"]["ebsStorageInfo"]
            .get("provisionedThroughput")
            .is_none());
        // Encryption defaults + a synthesized KMS key ARN.
        let ei = &d["clusterInfo"]["encryptionInfo"];
        assert_eq!(ei["encryptionInTransit"]["clientBroker"], json!("TLS"));
        assert_eq!(ei["encryptionInTransit"]["inCluster"], json!(true));
        assert!(ei["encryptionAtRest"]["dataVolumeKMSKeyId"]
            .as_str()
            .unwrap()
            .starts_with("arn:aws:kms:"));
        // No client_authentication is echoed for an auth-less cluster.
        assert!(d["clusterInfo"].get("clientAuthentication").is_none());
    }

    #[test]
    fn get_bootstrap_brokers_missing_cluster_is_bad_request() {
        // GetBootstrapBrokers declares no NotFoundException.
        let s = svc();
        let c = ctx("us-east-1");
        let Err(err) =
            s.get_bootstrap_brokers(&c, "arn:aws:kafka:us-east-1:123456789012:cluster/x/u-1")
        else {
            panic!("missing cluster must be an error");
        };
        assert_eq!(err.status(), StatusCode::BAD_REQUEST);
    }

    #[test]
    fn describe_cluster_v2_renders_union() {
        let s = svc();
        let c = ctx("us-east-1");
        let created = json_of(
            s.create_cluster(
                &c,
                &json!({
                    "clusterName": "sv",
                    "serverless": { "vpcConfigs": [{ "subnetIds": ["subnet-a"] }] }
                }),
                true,
            )
            .unwrap(),
        );
        assert_eq!(created["clusterType"].as_str(), Some("SERVERLESS"));
        let arn = created["clusterArn"].as_str().unwrap().to_string();
        settle(&s, &c);
        let d = json_of(s.describe_cluster(&c, &arn, true).unwrap());
        assert_eq!(d["clusterInfo"]["clusterType"].as_str(), Some("SERVERLESS"));
        assert!(d["clusterInfo"]["serverless"].is_object());
    }

    #[test]
    fn delete_cluster_settles_removal() {
        let s = svc();
        let c = ctx("us-east-1");
        let created = json_of(s.create_cluster(&c, &cluster_body("del"), false).unwrap());
        let arn = created["clusterArn"].as_str().unwrap().to_string();
        s.delete_cluster(&c, &arn).unwrap();
        settle(&s, &c);
        assert!(s.describe_cluster(&c, &arn, false).is_err());
    }

    #[test]
    fn list_clusters_region_scoped() {
        let s = svc();
        let east = ctx("us-east-1");
        let west = ctx("us-west-2");
        s.create_cluster(&east, &cluster_body("e"), false).unwrap();
        s.create_cluster(&west, &cluster_body("w"), false).unwrap();
        let listed = json_of(s.list_clusters(&east, &[], false).unwrap());
        assert_eq!(listed["clusterInfoList"].as_array().unwrap().len(), 1);
    }

    #[test]
    fn list_kafka_versions_are_active() {
        let s = svc();
        let out = json_of(s.list_kafka_versions(&[]).unwrap());
        let versions = out["kafkaVersions"].as_array().unwrap();
        assert!(!versions.is_empty());
        assert!(versions.iter().all(|v| v["status"] == json!("ACTIVE")));
    }

    #[test]
    fn tag_and_list_tags() {
        let s = svc();
        let c = ctx("us-east-1");
        let created = json_of(s.create_cluster(&c, &cluster_body("tag"), false).unwrap());
        let arn = created["clusterArn"].as_str().unwrap().to_string();
        s.tag_resource(&c, &arn, &json!({ "tags": { "team": "data" } }))
            .unwrap();
        let listed = json_of(s.list_tags(&c, &arn).unwrap());
        assert_eq!(listed["tags"]["team"].as_str(), Some("data"));
        // Tags surface on describe too.
        settle(&s, &c);
        let d = json_of(s.describe_cluster(&c, &arn, false).unwrap());
        assert_eq!(d["clusterInfo"]["tags"]["team"].as_str(), Some("data"));
    }

    #[test]
    fn routes_resolve_for_arn_labels() {
        // A percent-encoded ARN label decodes back to one segment.
        let raw = "/v1/clusters/arn%3Aaws%3Akafka%3Aus-east-1%3A123%3Acluster%2Fx%2Fu-1/nodes";
        let req = AwsRequest {
            method: Method::GET,
            raw_path: raw.to_string(),
            ..test_req()
        };
        let (action, labels) = KafkaService::resolve_action(&req).unwrap();
        assert_eq!(action, "ListNodes");
        assert_eq!(labels[0], "arn:aws:kafka:us-east-1:123:cluster/x/u-1");
    }

    #[test]
    fn merge_replication_sub_overwrites_non_object() {
        // Regression: a stored non-object sub-value (a prior create does not
        // normalize it) used to panic the UpdateReplicationInfo path via
        // `.as_object_mut().expect(...)`.
        let mut info = Map::new();
        info.insert("topicReplication".into(), json!("notanobject"));
        merge_replication_sub(
            &mut info,
            "topicReplication",
            Some(json!({ "topicNameConfiguration": { "type": "PREFIX" } })),
        );
        assert!(info["topicReplication"].is_object());
        assert_eq!(
            info["topicReplication"]["topicNameConfiguration"]["type"],
            "PREFIX"
        );
    }

    fn test_req() -> AwsRequest {
        AwsRequest {
            service: "kafka".to_string(),
            action: String::new(),
            region: "us-east-1".to_string(),
            account_id: "123456789012".to_string(),
            request_id: "req-1".to_string(),
            headers: http::HeaderMap::new(),
            query_params: std::collections::HashMap::new(),
            body: bytes::Bytes::new(),
            body_stream: parking_lot::Mutex::new(None),
            path_segments: vec![],
            raw_path: "/".to_string(),
            raw_query: String::new(),
            method: Method::GET,
            is_query_protocol: false,
            access_key_id: None,
            principal: None,
        }
    }
}
