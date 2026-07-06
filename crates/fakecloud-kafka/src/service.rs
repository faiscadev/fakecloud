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

use async_trait::async_trait;
use http::{Method, StatusCode};
use percent_encoding::percent_decode_str;
use serde_json::{json, Map, Value};
use tokio::sync::Mutex as AsyncMutex;
use uuid::Uuid;

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsService, AwsServiceError};
use fakecloud_persistence::SnapshotStore;

use crate::persistence::save_snapshot;
use crate::shared;
use crate::state::{KafkaData, SharedKafkaState};

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
}

impl KafkaService {
    pub fn new(state: SharedKafkaState) -> Self {
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
        let (result, settled) = self.dispatch(action, &labels, &req);
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
    fn dispatch(
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

        // Settle any in-flight lifecycle transition up front (batch 1 has no
        // Kafka runtime, so the pure state-machine always auto-settles). A read
        // that fires a transition reports `settled` so the handler persists.
        let mut settled = false;
        {
            let mut guard = self.state.write();
            if guard.get_or_create(&ctx.account).reconcile() {
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
            "ListTopics" => self.list_topics(&ctx, a0, &q),
            "CreateTopic" => self.create_topic(&ctx, a0, &body),
            "DescribeTopic" => self.describe_topic(&ctx, a0, a1),
            "UpdateTopic" => self.update_topic(&ctx, a0, a1, &body),
            "DeleteTopic" => self.delete_topic(&ctx, a0, a1),
            "DescribeTopicPartitions" => self.describe_topic_partitions(&ctx, a0, a1, &q),
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

/// A synthesized cluster `currentVersion` token (MSK's opaque `K...` form).
fn gen_version(seq: u64) -> String {
    format!("K{:013X}", 0x1_0000_0000_u64 + seq)
}

/// The default `BrokerNodeGroupInfo` MSK fills in when the caller supplies none.
fn default_bngi() -> Value {
    json!({
        "brokerAZDistribution": "DEFAULT",
        "clientSubnets": ["subnet-0123456789abcdef0", "subnet-0123456789abcdef1"],
        "instanceType": "kafka.m5.large",
        "securityGroups": ["sg-0123456789abcdef0"],
        "storageInfo": { "ebsStorageInfo": { "volumeSize": 100 } },
    })
}

fn default_encryption(ctx: &Ctx) -> Value {
    json!({
        "encryptionAtRest": {
            "dataVolumeKMSKeyId": format!(
                "arn:aws:kms:{}:{}:key/msk-default-key", ctx.region, ctx.account
            )
        },
        "encryptionInTransit": { "clientBroker": "TLS", "inCluster": true },
    })
}

/// The ZooKeeper connect strings synthesized for a provisioned cluster.
fn zk_strings(cluster_arn: &str) -> (String, String) {
    let name = shared::cluster_name_from_arn(cluster_arn).unwrap_or("cluster");
    let region = shared::arn_region(cluster_arn).unwrap_or("us-east-1");
    let h = shared::hash_str(cluster_arn) & 0xffff_ffff;
    let mk = |port: u16| {
        (1..=3)
            .map(|z| format!("z-{z}.{name}.{h:x}.c2.kafka.{region}.amazonaws.com:{port}"))
            .collect::<Vec<_>>()
            .join(",")
    };
    (mk(2181), mk(2182))
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

        // A serverless V2 request carries a `serverless` block; otherwise the
        // cluster is provisioned (from `provisioned` in V2, or the flat fields
        // in V1).
        let serverless = v2 && b.get("serverless").is_some();
        let provisioned_src = if v2 {
            b.get("provisioned").cloned().unwrap_or_else(|| b.clone())
        } else {
            b.clone()
        };

        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);

        // Cluster names are unique within an account + region.
        if data.clusters.values().any(|c| {
            c.get("clusterName").and_then(Value::as_str) == Some(name.as_str())
                && c.get("clusterArn")
                    .and_then(Value::as_str)
                    .and_then(shared::arn_region)
                    == Some(ctx.region.as_str())
        }) {
            return Err(conflict(&format!(
                "A cluster with the name '{name}' already exists in this account and region."
            )));
        }

        let n = data.next_seq();
        let uuid = Uuid::new_v4().to_string();
        let arn = shared::cluster_arn(&ctx.region, &ctx.account, &name, &uuid, n);
        let version = gen_version(n);
        let (zk, zk_tls) = zk_strings(&arn);

        let mut cluster = Map::new();
        cluster.insert("clusterArn".into(), json!(arn));
        cluster.insert("clusterName".into(), json!(name));
        cluster.insert("state".into(), json!("CREATING"));
        cluster.insert("stateInfo".into(), json!({ "code": "NONE", "message": "" }));
        cluster.insert("creationTime".into(), json!(now_iso()));
        cluster.insert("currentVersion".into(), json!(version));

        if serverless {
            cluster.insert("_clusterType".into(), json!("SERVERLESS"));
            cluster.insert(
                "_serverless".into(),
                b.get("serverless")
                    .cloned()
                    .unwrap_or(json!({ "vpcConfigs": [] })),
            );
        } else {
            let kv = provisioned_src
                .get("kafkaVersion")
                .and_then(Value::as_str)
                .unwrap_or(shared::DEFAULT_KAFKA_VERSION)
                .to_string();
            let num = provisioned_src
                .get("numberOfBrokerNodes")
                .and_then(Value::as_i64)
                .unwrap_or(3);
            cluster.insert("_clusterType".into(), json!("PROVISIONED"));
            cluster.insert("numberOfBrokerNodes".into(), json!(num));
            cluster.insert(
                "brokerNodeGroupInfo".into(),
                provisioned_src
                    .get("brokerNodeGroupInfo")
                    .cloned()
                    .unwrap_or_else(default_bngi),
            );
            cluster.insert(
                "currentBrokerSoftwareInfo".into(),
                json!({ "kafkaVersion": kv }),
            );
            cluster.insert(
                "enhancedMonitoring".into(),
                provisioned_src
                    .get("enhancedMonitoring")
                    .cloned()
                    .unwrap_or(json!("DEFAULT")),
            );
            cluster.insert(
                "encryptionInfo".into(),
                provisioned_src
                    .get("encryptionInfo")
                    .cloned()
                    .unwrap_or_else(|| default_encryption(ctx)),
            );
            cluster.insert(
                "storageMode".into(),
                provisioned_src
                    .get("storageMode")
                    .cloned()
                    .unwrap_or(json!("LOCAL")),
            );
            cluster.insert("zookeeperConnectString".into(), json!(zk));
            cluster.insert("zookeeperConnectStringTls".into(), json!(zk_tls));
        }

        data.clusters.insert(arn.clone(), Value::Object(cluster));

        if let Some(tags) = b.get("tags").and_then(Value::as_object) {
            let mut map = std::collections::BTreeMap::new();
            for (k, val) in tags {
                if let Some(s) = val.as_str() {
                    map.insert(k.clone(), s.to_string());
                }
            }
            if !map.is_empty() {
                data.tags.insert(arn.clone(), map);
            }
        }

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
        cluster
            .as_object_mut()
            .expect("cluster is object")
            .insert("state".into(), json!("DELETING"));
        // The DELETING cluster is reaped on the next reconcile (no runtime).
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
        let num = cluster_num_brokers(cluster);
        // TODO(batch2): back with a real reachable Kafka broker endpoint.
        ok(json!({
            "bootstrapBrokerString": shared::bootstrap_broker_string(arn, num, 9092),
            "bootstrapBrokerStringTls": shared::bootstrap_broker_string(arn, num, 9094),
            "bootstrapBrokerStringSaslScram": shared::bootstrap_broker_string(arn, num, 9096),
            "bootstrapBrokerStringSaslIam": shared::bootstrap_broker_string(arn, num, 9098),
        }))
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
        let nodes = shared::synthesize_nodes(arn, num, &kv);
        let (page, next) = paginate(&nodes, q);
        let mut out = json!({ "nodeInfoList": page });
        if let Some(n) = next {
            out["nextToken"] = json!(n);
        }
        ok(out)
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
        out["provisioned"] = json!({
            "brokerNodeGroupInfo": cluster.get("brokerNodeGroupInfo").cloned().unwrap_or_else(default_bngi),
            "numberOfBrokerNodes": cluster.get("numberOfBrokerNodes").cloned().unwrap_or(json!(3)),
            "currentBrokerSoftwareInfo": cluster.get("currentBrokerSoftwareInfo").cloned().unwrap_or(json!({})),
            "enhancedMonitoring": cluster.get("enhancedMonitoring").cloned().unwrap_or(json!("DEFAULT")),
            "storageMode": cluster.get("storageMode").cloned().unwrap_or(json!("LOCAL")),
            "zookeeperConnectString": cluster.get("zookeeperConnectString").cloned().unwrap_or(json!("")),
        });
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
        _b: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        self.cluster_update(ctx, arn, "UPDATE_BROKER_STORAGE", false, |_| {})
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
        self.cluster_update(ctx, arn, "UPDATE_CLUSTER_KAFKA_VERSION", true, |obj| {
            if let Some(t) = target {
                let sw = obj
                    .entry("currentBrokerSoftwareInfo")
                    .or_insert_with(|| json!({}));
                if let Some(swo) = sw.as_object_mut() {
                    swo.insert("kafkaVersion".into(), json!(t));
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
        self.cluster_update(ctx, arn, "UPDATE_MONITORING", false, |obj| {
            if let Some(v) = em {
                obj.insert("enhancedMonitoring".into(), v);
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
            if let Some(v) = ei {
                obj.insert("encryptionInfo".into(), v);
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
            if let Some(v) = ci {
                if let Some(bngi) = obj
                    .get_mut("brokerNodeGroupInfo")
                    .and_then(Value::as_object_mut)
                {
                    bngi.insert("connectivityInfo".into(), v);
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
        if let Some(obj) = data.clusters.get_mut(arn).and_then(Value::as_object_mut) {
            obj.insert("state".into(), json!("REBOOTING_BROKER"));
        }
        let op_arn = self.record_operation(data, arn, "REBOOT_NODE");
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

impl KafkaService {
    fn create_topic(
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
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        // CreateTopic declares no NotFoundException.
        if !data.clusters.contains_key(cluster_arn) {
            return Err(bad_request(&format!(
                "The cluster '{cluster_arn}' does not exist."
            )));
        }
        let topics = data.topics.entry(cluster_arn.to_string()).or_default();
        if topics.contains_key(&name) {
            return Err(conflict(&format!("Topic '{name}' already exists.")));
        }
        let topic = json!({
            "topicArn": format!("{cluster_arn}/topic/{name}"),
            "topicName": name,
            "partitionCount": b.get("partitionCount").cloned().unwrap_or(json!(1)),
            "replicationFactor": b.get("replicationFactor").cloned().unwrap_or(json!(1)),
            "configs": b.get("configs").cloned().unwrap_or(json!("")),
            "outOfSyncReplicaCount": 0,
            "status": "CREATING",
        });
        topics.insert(name.clone(), topic.clone());
        ok(json!({
            "topicArn": topic["topicArn"],
            "topicName": name,
            "status": "CREATING",
        }))
    }

    fn describe_topic(
        &self,
        ctx: &Ctx,
        cluster_arn: &str,
        name: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
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

    fn update_topic(
        &self,
        ctx: &Ctx,
        cluster_arn: &str,
        name: &str,
        b: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
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

    fn delete_topic(
        &self,
        ctx: &Ctx,
        cluster_arn: &str,
        name: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
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

    fn describe_topic_partitions(
        &self,
        ctx: &Ctx,
        cluster_arn: &str,
        name: &str,
        q: &[(String, String)],
    ) -> Result<AwsResponse, AwsServiceError> {
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
        // TODO(batch2): back with real partition metadata from a Kafka broker.
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

    fn list_topics(
        &self,
        ctx: &Ctx,
        cluster_arn: &str,
        q: &[(String, String)],
    ) -> Result<AwsResponse, AwsServiceError> {
        let name_filter = query_one(q, "topicNameFilter");
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        // No NotFoundException declared: a missing cluster surfaces as the
        // (declared) BadRequestException.
        if !data.clusters.contains_key(cluster_arn) {
            return Err(bad_request(&format!(
                "The cluster '{cluster_arn}' does not exist."
            )));
        }
        let items: Vec<Value> = data
            .topics
            .get(cluster_arn)
            .map(|m| {
                m.values()
                    .filter(|t| {
                        name_filter.is_none_or(|f| {
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

// ===================== configurations =====================

impl KafkaService {
    fn create_configuration(&self, ctx: &Ctx, b: &Value) -> Result<AwsResponse, AwsServiceError> {
        let name = b
            .get("name")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
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
        let kafka_versions: Vec<Value> = b
            .get("kafkaVersions")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_else(|| shared::KAFKA_VERSIONS.iter().map(|v| json!(v)).collect());

        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        let n = data.next_seq();
        let uuid = Uuid::new_v4().to_string();
        let arn = shared::config_arn(&ctx.region, &ctx.account, &name, &uuid, n);
        let created = now_iso();
        let rev = json!({ "creationTime": created, "description": description, "revision": 1 });
        data.configurations.insert(
            arn.clone(),
            json!({
                "arn": arn,
                "creationTime": created,
                "description": description,
                "kafkaVersions": kafka_versions,
                "latestRevision": rev,
                "name": name,
                "state": "ACTIVE",
            }),
        );
        data.configuration_revisions.insert(
            arn.clone(),
            vec![json!({
                "creationTime": created,
                "description": description,
                "revision": 1,
                "serverProperties": server_properties,
            })],
        );
        ok(json!({
            "arn": arn,
            "creationTime": created,
            "latestRevision": rev,
            "name": name,
            "state": "ACTIVE",
        }))
    }

    fn describe_configuration(&self, ctx: &Ctx, arn: &str) -> Result<AwsResponse, AwsServiceError> {
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        let cfg = data
            .configurations
            .get(arn)
            .cloned()
            .ok_or_else(|| not_found(&format!("The configuration '{arn}' does not exist.")))?;
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
        if !data.configurations.contains_key(arn) {
            return Err(not_found(&format!(
                "The configuration '{arn}' does not exist."
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
        let n = data.next_seq();
        let uuid = Uuid::new_v4().to_string();
        let arn = shared::vpc_connection_arn(&ctx.region, &ctx.account, &uuid, n);
        let created = now_iso();
        let subnets = b.get("clientSubnets").cloned().unwrap_or(json!([]));
        let sgs = b.get("securityGroups").cloned().unwrap_or(json!([]));
        let auth = b.get("authentication").cloned().unwrap_or(json!(""));
        let vpc_id = b.get("vpcId").cloned().unwrap_or(json!(""));
        let target = b.get("targetClusterArn").cloned().unwrap_or(json!(""));
        let record = json!({
            "vpcConnectionArn": arn,
            "targetClusterArn": target,
            "state": "CREATING",
            "authentication": auth,
            "vpcId": vpc_id,
            "subnets": subnets,
            "securityGroups": sgs,
            "creationTime": created,
        });
        data.vpc_connections.insert(arn.clone(), record);
        if let Some(tags) = b.get("tags").and_then(Value::as_object) {
            let mut map = std::collections::BTreeMap::new();
            for (k, v) in tags {
                if let Some(s) = v.as_str() {
                    map.insert(k.clone(), s.to_string());
                }
            }
            if !map.is_empty() {
                data.tags.insert(arn.clone(), map);
            }
        }
        ok(json!({
            "vpcConnectionArn": arn,
            "state": "CREATING",
            "authentication": auth,
            "vpcId": vpc_id,
            "clientSubnets": subnets,
            "securityGroups": sgs,
            "creationTime": created,
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
        let name = b
            .get("replicatorName")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        if data.replicators.values().any(|r| {
            r.get("replicatorName").and_then(Value::as_str) == Some(name.as_str())
                && r.get("replicatorArn")
                    .and_then(Value::as_str)
                    .and_then(shared::arn_region)
                    == Some(ctx.region.as_str())
        }) {
            return Err(conflict(&format!(
                "A replicator with the name '{name}' already exists."
            )));
        }
        let n = data.next_seq();
        let uuid = Uuid::new_v4().to_string();
        let arn = shared::replicator_arn(&ctx.region, &ctx.account, &name, &uuid, n);
        let created = now_iso();
        let record = json!({
            "replicatorArn": arn,
            "replicatorName": name,
            "replicatorState": "CREATING",
            "replicatorResourceArn": arn,
            "creationTime": created,
            "currentVersion": gen_version(n),
            "isReplicatorReference": false,
            "serviceExecutionRoleArn": b.get("serviceExecutionRoleArn").cloned().unwrap_or(json!("")),
            "kafkaClusters": b.get("kafkaClusters").cloned().unwrap_or(json!([])),
            "replicationInfoList": b.get("replicationInfoList").cloned().unwrap_or(json!([])),
            "replicatorDescription": b.get("description").cloned().unwrap_or(json!("")),
        });
        data.replicators.insert(arn.clone(), record);
        if let Some(tags) = b.get("tags").and_then(Value::as_object) {
            let mut map = std::collections::BTreeMap::new();
            for (k, v) in tags {
                if let Some(s) = v.as_str() {
                    map.insert(k.clone(), s.to_string());
                }
            }
            if !map.is_empty() {
                data.tags.insert(arn.clone(), map);
            }
        }
        ok(json!({
            "replicatorArn": arn,
            "replicatorName": name,
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
        _b: &Value,
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
        s.state.write().get_or_create(&c.account).reconcile();
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

    #[test]
    fn topic_round_trip() {
        let s = svc();
        let c = ctx("us-east-1");
        let cluster = json_of(s.create_cluster(&c, &cluster_body("t"), false).unwrap());
        let arn = cluster["clusterArn"].as_str().unwrap().to_string();
        s.create_topic(
            &c,
            &arn,
            &json!({ "topicName": "events", "partitionCount": 3, "replicationFactor": 2 }),
        )
        .unwrap();
        let d = json_of(s.describe_topic(&c, &arn, "events").unwrap());
        assert_eq!(d["partitionCount"], json!(3));
        let parts = json_of(
            s.describe_topic_partitions(&c, &arn, "events", &[])
                .unwrap(),
        );
        assert_eq!(parts["partitions"].as_array().unwrap().len(), 3);
    }

    #[test]
    fn bootstrap_brokers_derived_from_broker_count() {
        let s = svc();
        let c = ctx("us-east-1");
        let cluster = json_of(s.create_cluster(&c, &cluster_body("bb"), false).unwrap());
        let arn = cluster["clusterArn"].as_str().unwrap().to_string();
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
