//! MemoryDB (`memorydb`) awsJson1.1 dispatch + operation handlers.
//!
//! Full 45-op control plane. Clusters transition `creating` -> `available`
//! lazily on describe (deterministic, no background timer needed for the
//! control-plane contract). The Redis/Valkey data-plane container backing is a
//! follow-on, mirroring how Aurora DSQL shipped its control plane first.

use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use chrono::Utc;
use http::StatusCode;
use serde_json::{json, Value};
use tokio::sync::Mutex as AsyncMutex;

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsService, AwsServiceError};
use fakecloud_persistence::SnapshotStore;

use crate::persistence::save_snapshot;
use crate::state::{
    Acl, Cluster, Endpoint, MemoryDbState, MultiRegionCluster, Node, ParameterGroup, ReservedNode,
    Shard, SharedMemoryDbState, Snapshot, SubnetGroup, User, UserAuthentication,
};

/// Every operation name in the MemoryDB Smithy model.
pub const MEMORYDB_ACTIONS: &[&str] = &[
    "BatchUpdateCluster",
    "CopySnapshot",
    "CreateACL",
    "CreateCluster",
    "CreateMultiRegionCluster",
    "CreateParameterGroup",
    "CreateSnapshot",
    "CreateSubnetGroup",
    "CreateUser",
    "DeleteACL",
    "DeleteCluster",
    "DeleteMultiRegionCluster",
    "DeleteParameterGroup",
    "DeleteSnapshot",
    "DeleteSubnetGroup",
    "DeleteUser",
    "DescribeACLs",
    "DescribeClusters",
    "DescribeEngineVersions",
    "DescribeEvents",
    "DescribeMultiRegionClusters",
    "DescribeMultiRegionParameterGroups",
    "DescribeMultiRegionParameters",
    "DescribeParameterGroups",
    "DescribeParameters",
    "DescribeReservedNodes",
    "DescribeReservedNodesOfferings",
    "DescribeServiceUpdates",
    "DescribeSnapshots",
    "DescribeSubnetGroups",
    "DescribeUsers",
    "FailoverShard",
    "ListAllowedMultiRegionClusterUpdates",
    "ListAllowedNodeTypeUpdates",
    "ListTags",
    "PurchaseReservedNodesOffering",
    "ResetParameterGroup",
    "TagResource",
    "UntagResource",
    "UpdateACL",
    "UpdateCluster",
    "UpdateMultiRegionCluster",
    "UpdateParameterGroup",
    "UpdateSubnetGroup",
    "UpdateUser",
];

const DEFAULT_ENGINE: &str = "redis";
const DEFAULT_ENGINE_VERSION: &str = "7.1";
const DEFAULT_PATCH_VERSION: &str = "7.1.1";

pub struct MemoryDbService {
    state: SharedMemoryDbState,
    snapshot_store: Option<Arc<dyn SnapshotStore>>,
    snapshot_lock: Arc<AsyncMutex<()>>,
}

impl MemoryDbService {
    pub fn new(state: SharedMemoryDbState) -> Self {
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
impl AwsService for MemoryDbService {
    fn service_name(&self) -> &str {
        "memorydb"
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
        MEMORYDB_ACTIONS
    }
}

fn is_mutating(action: &str) -> bool {
    action.starts_with("Create")
        || action.starts_with("Delete")
        || action.starts_with("Update")
        || action.starts_with("Tag")
        || action.starts_with("Untag")
        || action.starts_with("Copy")
        || action.starts_with("Reset")
        || action.starts_with("Batch")
        || action.starts_with("Failover")
        || action.starts_with("Purchase")
}

fn dispatch(s: &MemoryDbService, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
    match req.action.as_str() {
        "CreateCluster" => s.create_cluster(req),
        "DescribeClusters" => s.describe_clusters(req),
        "UpdateCluster" => s.update_cluster(req),
        "DeleteCluster" => s.delete_cluster(req),
        "BatchUpdateCluster" => s.batch_update_cluster(req),
        "FailoverShard" => s.failover_shard(req),
        "CreateACL" => s.create_acl(req),
        "DescribeACLs" => s.describe_acls(req),
        "UpdateACL" => s.update_acl(req),
        "DeleteACL" => s.delete_acl(req),
        "CreateUser" => s.create_user(req),
        "DescribeUsers" => s.describe_users(req),
        "UpdateUser" => s.update_user(req),
        "DeleteUser" => s.delete_user(req),
        "CreateParameterGroup" => s.create_parameter_group(req),
        "DescribeParameterGroups" => s.describe_parameter_groups(req),
        "UpdateParameterGroup" => s.update_parameter_group(req),
        "ResetParameterGroup" => s.reset_parameter_group(req),
        "DeleteParameterGroup" => s.delete_parameter_group(req),
        "DescribeParameters" => s.describe_parameters(req),
        "CreateSubnetGroup" => s.create_subnet_group(req),
        "DescribeSubnetGroups" => s.describe_subnet_groups(req),
        "UpdateSubnetGroup" => s.update_subnet_group(req),
        "DeleteSubnetGroup" => s.delete_subnet_group(req),
        "CreateSnapshot" => s.create_snapshot(req),
        "CopySnapshot" => s.copy_snapshot(req),
        "DescribeSnapshots" => s.describe_snapshots(req),
        "DeleteSnapshot" => s.delete_snapshot(req),
        "CreateMultiRegionCluster" => s.create_multi_region_cluster(req),
        "DescribeMultiRegionClusters" => s.describe_multi_region_clusters(req),
        "UpdateMultiRegionCluster" => s.update_multi_region_cluster(req),
        "DeleteMultiRegionCluster" => s.delete_multi_region_cluster(req),
        "ListAllowedMultiRegionClusterUpdates" => s.list_allowed_mr_updates(req),
        "DescribeMultiRegionParameterGroups" => s.describe_mr_parameter_groups(req),
        "DescribeMultiRegionParameters" => s.describe_mr_parameters(req),
        "TagResource" => s.tag_resource(req),
        "UntagResource" => s.untag_resource(req),
        "ListTags" => s.list_tags(req),
        "DescribeEngineVersions" => s.describe_engine_versions(req),
        "DescribeEvents" => s.describe_events(req),
        "DescribeServiceUpdates" => s.describe_service_updates(req),
        "DescribeReservedNodes" => s.describe_reserved_nodes(req),
        "DescribeReservedNodesOfferings" => s.describe_reserved_nodes_offerings(req),
        "PurchaseReservedNodesOffering" => s.purchase_reserved_nodes_offering(req),
        "ListAllowedNodeTypeUpdates" => s.list_allowed_node_type_updates(req),
        _ => Err(AwsServiceError::action_not_implemented(
            s.service_name(),
            &req.action,
        )),
    }
}

// ===== helpers =====

fn ok(v: Value) -> Result<AwsResponse, AwsServiceError> {
    Ok(AwsResponse::json_value(StatusCode::OK, v))
}

fn parse(req: &AwsRequest) -> Result<Value, AwsServiceError> {
    if req.body.is_empty() {
        return Ok(json!({}));
    }
    serde_json::from_slice(&req.body).map_err(|e| {
        fault(
            "InvalidParameterValueException",
            &format!("malformed body: {e}"),
        )
    })
}

fn fault(code: &str, msg: &str) -> AwsServiceError {
    // The MemoryDB model declares `httpError: 404` for the entire
    // `*NotFoundFault` family (ClusterNotFoundFault, ACLNotFoundFault, ...),
    // with the single exception of ServiceLinkedRoleNotFoundFault (400).
    // Everything else (InvalidParameterValueException, *AlreadyExistsFault,
    // etc.) is a 400.
    let status = if code.ends_with("NotFoundFault") && code != "ServiceLinkedRoleNotFoundFault" {
        StatusCode::NOT_FOUND
    } else {
        StatusCode::BAD_REQUEST
    };
    AwsServiceError::aws_error(status, code, msg)
}

fn req_str<'a>(b: &'a Value, f: &str) -> Result<&'a str, AwsServiceError> {
    b.get(f).and_then(Value::as_str).ok_or_else(|| {
        fault(
            "InvalidParameterValueException",
            &format!("{f} is required."),
        )
    })
}

fn opt_str(b: &Value, f: &str) -> Option<String> {
    b.get(f).and_then(Value::as_str).map(str::to_string)
}

fn str_list(b: &Value, f: &str) -> Vec<String> {
    b.get(f)
        .and_then(Value::as_array)
        .map(|a| {
            a.iter()
                .filter_map(|x| x.as_str().map(str::to_string))
                .collect()
        })
        .unwrap_or_default()
}

fn arn(kind: &str, region: &str, account: &str, name: &str) -> String {
    format!("arn:aws:memorydb:{region}:{account}:{kind}/{name}")
}

fn parse_tags(b: &Value) -> BTreeMap<String, String> {
    let mut out = BTreeMap::new();
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

fn tags_json(tags: &BTreeMap<String, String>) -> Vec<Value> {
    tags.iter()
        .map(|(k, v)| json!({ "Key": k, "Value": v }))
        .collect()
}

/// Decode a numeric next-token; unparseable/empty -> page start 0 (`None` for
/// a genuinely bad token so callers return a terminal empty page).
fn page_start(b: &Value) -> Option<usize> {
    match b.get("NextToken").and_then(Value::as_str) {
        None | Some("") => Some(0),
        Some(t) => t.parse::<usize>().ok(),
    }
}

fn max_results(b: &Value, default: usize) -> usize {
    b.get("MaxResults")
        .and_then(Value::as_i64)
        .map(|n| n.max(1) as usize)
        .unwrap_or(default)
}

/// Paginate a pre-sorted Vec of JSON items into (page, next_token).
fn paginate(items: Vec<Value>, b: &Value, default_max: usize) -> (Vec<Value>, Option<String>) {
    let Some(start) = page_start(b) else {
        return (Vec::new(), None);
    };
    let max = max_results(b, default_max);
    let total = items.len();
    if start >= total {
        return (Vec::new(), None);
    }
    let end = start.saturating_add(max).min(total);
    let next = (end < total).then(|| end.to_string());
    (items[start..end].to_vec(), next)
}

// ===== JSON builders (member names match the Smithy model) =====

fn endpoint_json(e: &Endpoint) -> Value {
    json!({ "Address": e.address, "Port": e.port })
}

fn node_json(n: &Node) -> Value {
    json!({
        "Name": n.name,
        "Status": n.status,
        "AvailabilityZone": n.availability_zone,
        "CreateTime": n.create_time.timestamp(),
        "Endpoint": endpoint_json(&n.endpoint),
    })
}

fn shard_json(sh: &Shard) -> Value {
    json!({
        "Name": sh.name,
        "Status": sh.status,
        "Slots": sh.slots,
        "Nodes": sh.nodes.iter().map(node_json).collect::<Vec<_>>(),
        "NumberOfNodes": sh.number_of_nodes,
    })
}

fn cluster_json(c: &Cluster) -> Value {
    json!({
        "Name": c.name,
        "Description": c.description,
        "Status": c.status,
        "NumberOfShards": c.number_of_shards,
        "Shards": c.shards.iter().map(shard_json).collect::<Vec<_>>(),
        "AvailabilityMode": c.availability_mode,
        "ClusterEndpoint": endpoint_json(&c.cluster_endpoint),
        "NodeType": c.node_type,
        "Engine": c.engine,
        "EngineVersion": c.engine_version,
        "EnginePatchVersion": c.engine_patch_version,
        "ParameterGroupName": c.parameter_group_name,
        "ParameterGroupStatus": c.parameter_group_status,
        "SecurityGroups": c.security_group_ids.iter().map(|id| json!({ "SecurityGroupId": id, "Status": "active" })).collect::<Vec<_>>(),
        "SubnetGroupName": c.subnet_group_name,
        "TLSEnabled": c.tls_enabled,
        "KmsKeyId": c.kms_key_id,
        "ARN": c.arn,
        "SnsTopicArn": c.sns_topic_arn,
        "SnsTopicStatus": c.sns_topic_arn.as_ref().map(|_| "active"),
        "SnapshotRetentionLimit": c.snapshot_retention_limit,
        "MaintenanceWindow": c.maintenance_window,
        "SnapshotWindow": c.snapshot_window,
        "ACLName": c.acl_name,
        "AutoMinorVersionUpgrade": c.auto_minor_version_upgrade,
        "DataTiering": c.data_tiering,
        "NetworkType": c.network_type,
        "IpDiscovery": c.ip_discovery,
    })
}

fn acl_json(a: &Acl) -> Value {
    json!({
        "Name": a.name,
        "Status": a.status,
        "UserNames": a.user_names,
        "MinimumEngineVersion": a.minimum_engine_version,
        "Clusters": a.clusters,
        "ARN": a.arn,
    })
}

fn user_json(u: &User) -> Value {
    json!({
        "Name": u.name,
        "Status": u.status,
        "AccessString": u.access_string,
        "ACLNames": u.acl_names,
        "MinimumEngineVersion": u.minimum_engine_version,
        "Authentication": {
            "Type": u.authentication.type_,
            "PasswordCount": u.authentication.password_count,
        },
        "ARN": u.arn,
    })
}

fn pg_json(p: &ParameterGroup) -> Value {
    json!({
        "Name": p.name,
        "Family": p.family,
        "Description": p.description,
        "ARN": p.arn,
    })
}

fn sg_json(g: &SubnetGroup) -> Value {
    json!({
        "Name": g.name,
        "Description": g.description,
        "VpcId": g.vpc_id,
        "Subnets": g.subnet_ids.iter().map(|id| json!({
            "Identifier": id,
            "AvailabilityZone": { "Name": "us-east-1a" },
            "SupportedNetworkTypes": ["ipv4"],
        })).collect::<Vec<_>>(),
        "ARN": g.arn,
        "SupportedNetworkTypes": g.supported_network_types,
    })
}

fn snapshot_json(s: &Snapshot) -> Value {
    json!({
        "Name": s.name,
        "Status": s.status,
        "Source": s.source,
        "KmsKeyId": s.kms_key_id,
        "ARN": s.arn,
        "ClusterConfiguration": s.cluster_configuration,
        "DataTiering": s.data_tiering,
    })
}

fn mrc_json(m: &MultiRegionCluster) -> Value {
    json!({
        "MultiRegionClusterName": m.name,
        "Description": m.description,
        "Status": m.status,
        "NodeType": m.node_type,
        "Engine": m.engine,
        "EngineVersion": m.engine_version,
        "NumberOfShards": m.number_of_shards,
        "Clusters": m.clusters,
        "MultiRegionParameterGroupName": m.multi_region_parameter_group_name,
        "TLSEnabled": m.tls_enabled,
        "ARN": m.arn,
    })
}

fn reserved_node_json(r: &ReservedNode) -> Value {
    json!({
        "ReservationId": r.reservation_id,
        "ReservedNodesOfferingId": r.reserved_nodes_offering_id,
        "NodeType": r.node_type,
        "StartTime": r.start_time.timestamp(),
        "Duration": r.duration,
        "FixedPrice": r.fixed_price,
        "NodeCount": r.node_count,
        "OfferingType": r.offering_type,
        "State": r.state,
        "ARN": r.arn,
    })
}

// ===== handlers =====

impl MemoryDbService {
    fn create_cluster(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        let name = req_str(&b, "ClusterName")?.to_string();
        let node_type = req_str(&b, "NodeType")?.to_string();
        let acl_name = req_str(&b, "ACLName")?.to_string();
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        if st.clusters.contains_key(&name) {
            return Err(fault(
                "ClusterAlreadyExistsFault",
                &format!("Cluster {name} already exists."),
            ));
        }
        // AWS caps: up to 500 shards per cluster, 0-5 replicas per shard.
        // Validate before building node vectors so an out-of-range request is a
        // clean 400 rather than an unbounded allocation.
        let num_shards = b.get("NumShards").and_then(Value::as_i64).unwrap_or(1);
        if !(1..=500).contains(&num_shards) {
            return Err(fault(
                "InvalidParameterValueException",
                "NumShards must be between 1 and 500.",
            ));
        }
        let num_shards = num_shards as i32;
        let replicas = b
            .get("NumReplicasPerShard")
            .and_then(Value::as_i64)
            .unwrap_or(1);
        if !(0..=5).contains(&replicas) {
            return Err(fault(
                "InvalidParameterValueException",
                "NumReplicasPerShard must be between 0 and 5.",
            ));
        }
        let replicas = replicas as i32;
        let port = b.get("Port").and_then(Value::as_i64).unwrap_or(6379) as i32;
        let engine = opt_str(&b, "Engine").unwrap_or_else(|| DEFAULT_ENGINE.to_string());
        let engine_version =
            opt_str(&b, "EngineVersion").unwrap_or_else(|| DEFAULT_ENGINE_VERSION.to_string());
        let cluster_arn = arn("cluster", &req.region, &req.account_id, &name);
        let endpoint = Endpoint {
            address: format!(
                "clustercfg.{name}.abc123.memorydb.{}.amazonaws.com",
                req.region
            ),
            port,
        };
        let shards = (0..num_shards)
            .map(|i| {
                let sname = format!("{:04}", i + 1);
                let nodes = (0..=replicas)
                    .map(|j| Node {
                        name: format!("{name}-{sname}-{:03}", j + 1),
                        status: "creating".to_string(),
                        availability_zone: format!("{}a", req.region),
                        create_time: Utc::now(),
                        endpoint: Endpoint {
                            address: format!(
                                "{name}-{sname}-{:03}.{name}.abc123.memorydb.{}.amazonaws.com",
                                j + 1,
                                req.region
                            ),
                            port,
                        },
                    })
                    .collect::<Vec<_>>();
                Shard {
                    name: sname,
                    status: "creating".to_string(),
                    slots: "0-16383".to_string(),
                    number_of_nodes: nodes.len() as i32,
                    nodes,
                }
            })
            .collect();
        let cluster = Cluster {
            name: name.clone(),
            description: opt_str(&b, "Description"),
            status: "creating".to_string(),
            number_of_shards: num_shards,
            shards,
            node_type,
            engine,
            engine_version,
            engine_patch_version: DEFAULT_PATCH_VERSION.to_string(),
            parameter_group_name: opt_str(&b, "ParameterGroupName")
                .unwrap_or_else(|| "default.memorydb-redis7".to_string()),
            parameter_group_status: "in-sync".to_string(),
            security_group_ids: str_list(&b, "SecurityGroupIds"),
            subnet_group_name: opt_str(&b, "SubnetGroupName")
                .unwrap_or_else(|| "default".to_string()),
            tls_enabled: b.get("TLSEnabled").and_then(Value::as_bool).unwrap_or(true),
            kms_key_id: opt_str(&b, "KmsKeyId"),
            arn: cluster_arn.clone(),
            sns_topic_arn: opt_str(&b, "SnsTopicArn"),
            snapshot_retention_limit: b
                .get("SnapshotRetentionLimit")
                .and_then(Value::as_i64)
                .unwrap_or(0) as i32,
            maintenance_window: opt_str(&b, "MaintenanceWindow")
                .unwrap_or_else(|| "wed:08:00-wed:09:00".to_string()),
            snapshot_window: opt_str(&b, "SnapshotWindow")
                .unwrap_or_else(|| "05:00-06:00".to_string()),
            acl_name,
            auto_minor_version_upgrade: b
                .get("AutoMinorVersionUpgrade")
                .and_then(Value::as_bool)
                .unwrap_or(true),
            data_tiering: opt_str(&b, "DataTiering")
                .map(|d| {
                    if d == "true" {
                        "true".to_string()
                    } else {
                        "false".to_string()
                    }
                })
                .unwrap_or_else(|| "false".to_string()),
            availability_mode: if replicas > 0 {
                "MultiAZ".to_string()
            } else {
                "SingleAZ".to_string()
            },
            cluster_endpoint: endpoint,
            network_type: opt_str(&b, "NetworkType").unwrap_or_else(|| "ipv4".to_string()),
            ip_discovery: opt_str(&b, "IpDiscovery").unwrap_or_else(|| "ipv4".to_string()),
            port,
            container_id: None,
            created_at: Utc::now(),
        };
        let tags = parse_tags(&b);
        if !tags.is_empty() {
            st.tags.insert(cluster_arn, tags);
        }
        // Maintain the ACL -> clusters back-reference so DescribeACLs lists the
        // clusters using each ACL (AWS keeps this bidirectional).
        if let Some(acl) = st.acls.get_mut(&cluster.acl_name) {
            if !acl.clusters.contains(&name) {
                acl.clusters.push(name.clone());
            }
        }
        st.clusters.insert(name, cluster.clone());
        ok(json!({ "Cluster": cluster_json(&cluster) }))
    }

    fn describe_clusters(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        // Lazy transition creating -> available.
        for c in st.clusters.values_mut() {
            if c.status == "creating" {
                c.status = "available".to_string();
                for sh in &mut c.shards {
                    sh.status = "available".to_string();
                    for n in &mut sh.nodes {
                        n.status = "available".to_string();
                    }
                }
            }
        }
        if let Some(name) = opt_str(&b, "ClusterName") {
            let Some(c) = st.clusters.get(&name) else {
                return Err(fault(
                    "ClusterNotFoundFault",
                    &format!("Cluster {name} not found."),
                ));
            };
            return ok(json!({ "Clusters": [cluster_json(c)] }));
        }
        let items: Vec<Value> = st.clusters.values().map(cluster_json).collect();
        let (page, next) = paginate(items, &b, 100);
        ok(json!({ "Clusters": page, "NextToken": next }))
    }

    fn update_cluster(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        let name = req_str(&b, "ClusterName")?.to_string();
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let Some(c) = st.clusters.get_mut(&name) else {
            return Err(fault(
                "ClusterNotFoundFault",
                &format!("Cluster {name} not found."),
            ));
        };
        if let Some(d) = opt_str(&b, "Description") {
            c.description = Some(d);
        }
        if let Some(v) = opt_str(&b, "EngineVersion") {
            c.engine_version = v;
        }
        if let Some(v) = opt_str(&b, "Engine") {
            c.engine = v;
        }
        if let Some(v) = opt_str(&b, "NodeType") {
            c.node_type = v;
        }
        if let Some(v) = opt_str(&b, "ParameterGroupName") {
            c.parameter_group_name = v;
        }
        // Track an ACL reassignment so the ACL -> clusters back-references can be
        // moved after the cluster borrow is released.
        let acl_change = opt_str(&b, "ACLName").map(|new_acl| {
            let old_acl = c.acl_name.clone();
            c.acl_name = new_acl.clone();
            (old_acl, new_acl)
        });
        if let Some(v) = opt_str(&b, "MaintenanceWindow") {
            c.maintenance_window = v;
        }
        if let Some(v) = opt_str(&b, "SnapshotWindow") {
            c.snapshot_window = v;
        }
        if let Some(v) = b.get("SnapshotRetentionLimit").and_then(Value::as_i64) {
            c.snapshot_retention_limit = v as i32;
        }
        if let Some(sg) = b.get("SecurityGroupIds").and_then(Value::as_array) {
            c.security_group_ids = sg
                .iter()
                .filter_map(|x| x.as_str().map(str::to_string))
                .collect();
        }
        let out = cluster_json(c);
        // Apply any ACL reassignment to the ACL -> clusters back-references.
        if let Some((old_acl, new_acl)) = acl_change {
            if old_acl != new_acl {
                if let Some(a) = st.acls.get_mut(&old_acl) {
                    a.clusters.retain(|cn| cn != &name);
                }
                if let Some(a) = st.acls.get_mut(&new_acl) {
                    if !a.clusters.contains(&name) {
                        a.clusters.push(name.clone());
                    }
                }
            }
        }
        ok(json!({ "Cluster": out }))
    }

    fn delete_cluster(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        let name = req_str(&b, "ClusterName")?.to_string();
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let Some(mut c) = st.clusters.remove(&name) else {
            return Err(fault(
                "ClusterNotFoundFault",
                &format!("Cluster {name} not found."),
            ));
        };
        c.status = "deleting".to_string();
        st.tags.remove(&c.arn);
        // Drop the ACL -> clusters back-reference.
        if let Some(a) = st.acls.get_mut(&c.acl_name) {
            a.clusters.retain(|cn| cn != &name);
        }
        ok(json!({ "Cluster": cluster_json(&c) }))
    }

    fn batch_update_cluster(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        let names = str_list(&b, "ClusterNames");
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let mut processed = Vec::new();
        for n in &names {
            if let Some(c) = st.clusters.get(n) {
                processed.push(cluster_json(c));
            }
        }
        ok(json!({ "ProcessedClusters": processed, "UnprocessedClusters": [] }))
    }

    fn failover_shard(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        let name = req_str(&b, "ClusterName")?.to_string();
        let accounts = self.state.read();
        let st = accounts.get(&req.account_id);
        let Some(c) = st.and_then(|s| s.clusters.get(&name)) else {
            return Err(fault(
                "ClusterNotFoundFault",
                &format!("Cluster {name} not found."),
            ));
        };
        ok(json!({ "Cluster": cluster_json(c) }))
    }

    fn create_acl(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        let name = req_str(&b, "ACLName")?.to_string();
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        if st.acls.contains_key(&name) {
            return Err(fault(
                "ACLAlreadyExistsFault",
                &format!("ACL {name} already exists."),
            ));
        }
        let acl_arn = arn("acl", &req.region, &req.account_id, &name);
        let acl = Acl {
            name: name.clone(),
            status: "active".to_string(),
            user_names: str_list(&b, "UserNames"),
            minimum_engine_version: "6.2".to_string(),
            clusters: vec![],
            arn: acl_arn.clone(),
        };
        let tags = parse_tags(&b);
        if !tags.is_empty() {
            st.tags.insert(acl_arn, tags);
        }
        st.acls.insert(name.clone(), acl.clone());
        // Maintain the User -> ACLNames back-reference (AWS keeps it bidirectional).
        for u in &acl.user_names {
            if let Some(user) = st.users.get_mut(u) {
                if !user.acl_names.contains(&name) {
                    user.acl_names.push(name.clone());
                }
            }
        }
        ok(json!({ "ACL": acl_json(&acl) }))
    }

    fn describe_acls(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        let accounts = self.state.read();
        let st = accounts.get(&req.account_id);
        let Some(st) = st else {
            return ok(json!({ "ACLs": [] }));
        };
        if let Some(name) = opt_str(&b, "ACLName") {
            let Some(a) = st.acls.get(&name) else {
                return Err(fault("ACLNotFoundFault", &format!("ACL {name} not found.")));
            };
            return ok(json!({ "ACLs": [acl_json(a)] }));
        }
        let items: Vec<Value> = st.acls.values().map(acl_json).collect();
        let (page, next) = paginate(items, &b, 100);
        ok(json!({ "ACLs": page, "NextToken": next }))
    }

    fn update_acl(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        let name = req_str(&b, "ACLName")?.to_string();
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let Some(a) = st.acls.get_mut(&name) else {
            return Err(fault("ACLNotFoundFault", &format!("ACL {name} not found.")));
        };
        let mut added = Vec::new();
        for u in str_list(&b, "UserNamesToAdd") {
            if !a.user_names.contains(&u) {
                a.user_names.push(u.clone());
                added.push(u);
            }
        }
        let remove = str_list(&b, "UserNamesToRemove");
        let removed: Vec<String> = a
            .user_names
            .iter()
            .filter(|u| remove.contains(u))
            .cloned()
            .collect();
        a.user_names.retain(|u| !remove.contains(u));
        let out = acl_json(a);
        // Sync the User -> ACLNames back-reference for the affected users.
        for u in &added {
            if let Some(user) = st.users.get_mut(u) {
                if !user.acl_names.contains(&name) {
                    user.acl_names.push(name.clone());
                }
            }
        }
        for u in &removed {
            if let Some(user) = st.users.get_mut(u) {
                user.acl_names.retain(|an| an != &name);
            }
        }
        ok(json!({ "ACL": out }))
    }

    fn delete_acl(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        let name = req_str(&b, "ACLName")?.to_string();
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let Some(mut a) = st.acls.remove(&name) else {
            return Err(fault("ACLNotFoundFault", &format!("ACL {name} not found.")));
        };
        a.status = "deleting".to_string();
        st.tags.remove(&a.arn);
        // Drop the ACL from each member user's ACLNames back-reference.
        for u in &a.user_names {
            if let Some(user) = st.users.get_mut(u) {
                user.acl_names.retain(|an| an != &name);
            }
        }
        ok(json!({ "ACL": acl_json(&a) }))
    }

    fn create_user(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        let name = req_str(&b, "UserName")?.to_string();
        validate_user_name(&name)?;
        let access_string = req_str(&b, "AccessString")?.to_string();
        let auth = b.get("AuthenticationMode").cloned().unwrap_or(json!({}));
        let auth_type = auth
            .get("Type")
            .and_then(Value::as_str)
            .unwrap_or("password")
            .to_string();
        let pw_count = auth
            .get("Passwords")
            .and_then(Value::as_array)
            .map(|a| a.len() as i32)
            .unwrap_or(0);
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        if st.users.contains_key(&name) {
            return Err(fault(
                "UserAlreadyExistsFault",
                &format!("User {name} already exists."),
            ));
        }
        let user_arn = arn("user", &req.region, &req.account_id, &name);
        let user = User {
            name: name.clone(),
            status: "active".to_string(),
            access_string,
            acl_names: vec![],
            minimum_engine_version: "6.2".to_string(),
            authentication: UserAuthentication {
                type_: if auth_type == "iam" {
                    "iam".to_string()
                } else {
                    auth_type
                },
                password_count: pw_count,
            },
            arn: user_arn.clone(),
        };
        let tags = parse_tags(&b);
        if !tags.is_empty() {
            st.tags.insert(user_arn, tags);
        }
        st.users.insert(name, user.clone());
        ok(json!({ "User": user_json(&user) }))
    }

    fn describe_users(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        let accounts = self.state.read();
        let st = accounts.get(&req.account_id);
        let Some(st) = st else {
            return ok(json!({ "Users": [] }));
        };
        if let Some(name) = opt_str(&b, "UserName") {
            let Some(u) = st.users.get(&name) else {
                return Err(fault(
                    "UserNotFoundFault",
                    &format!("User {name} not found."),
                ));
            };
            return ok(json!({ "Users": [user_json(u)] }));
        }
        let items: Vec<Value> = st.users.values().map(user_json).collect();
        let (page, next) = paginate(items, &b, 100);
        ok(json!({ "Users": page, "NextToken": next }))
    }

    fn update_user(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        let name = req_str(&b, "UserName")?.to_string();
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let Some(u) = st.users.get_mut(&name) else {
            return Err(fault(
                "UserNotFoundFault",
                &format!("User {name} not found."),
            ));
        };
        if let Some(a) = opt_str(&b, "AccessString") {
            u.access_string = a;
        }
        if let Some(auth) = b.get("AuthenticationMode") {
            if let Some(t) = auth.get("Type").and_then(Value::as_str) {
                u.authentication.type_ = t.to_string();
            }
            if let Some(p) = auth.get("Passwords").and_then(Value::as_array) {
                u.authentication.password_count = p.len() as i32;
            }
        }
        let out = user_json(u);
        ok(json!({ "User": out }))
    }

    fn delete_user(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        let name = req_str(&b, "UserName")?.to_string();
        validate_user_name(&name)?;
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let Some(mut u) = st.users.remove(&name) else {
            return Err(fault(
                "UserNotFoundFault",
                &format!("User {name} not found."),
            ));
        };
        u.status = "deleting".to_string();
        st.tags.remove(&u.arn);
        ok(json!({ "User": user_json(&u) }))
    }

    fn create_parameter_group(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        let name = req_str(&b, "ParameterGroupName")?.to_string();
        let family = req_str(&b, "Family")?.to_string();
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        if st.parameter_groups.contains_key(&name) {
            return Err(fault(
                "ParameterGroupAlreadyExistsFault",
                &format!("Parameter group {name} already exists."),
            ));
        }
        let pg_arn = arn("parametergroup", &req.region, &req.account_id, &name);
        let pg = ParameterGroup {
            name: name.clone(),
            family,
            description: opt_str(&b, "Description").unwrap_or_default(),
            arn: pg_arn.clone(),
            parameters: BTreeMap::new(),
        };
        let tags = parse_tags(&b);
        if !tags.is_empty() {
            st.tags.insert(pg_arn, tags);
        }
        st.parameter_groups.insert(name, pg.clone());
        ok(json!({ "ParameterGroup": pg_json(&pg) }))
    }

    fn describe_parameter_groups(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        let accounts = self.state.read();
        let st = accounts.get(&req.account_id);
        let Some(st) = st else {
            return ok(json!({ "ParameterGroups": [] }));
        };
        if let Some(name) = opt_str(&b, "ParameterGroupName") {
            let Some(p) = st.parameter_groups.get(&name) else {
                return Err(fault(
                    "ParameterGroupNotFoundFault",
                    &format!("Parameter group {name} not found."),
                ));
            };
            return ok(json!({ "ParameterGroups": [pg_json(p)] }));
        }
        let items: Vec<Value> = st.parameter_groups.values().map(pg_json).collect();
        let (page, next) = paginate(items, &b, 100);
        ok(json!({ "ParameterGroups": page, "NextToken": next }))
    }

    fn update_parameter_group(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        let name = req_str(&b, "ParameterGroupName")?.to_string();
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let Some(p) = st.parameter_groups.get_mut(&name) else {
            return Err(fault(
                "ParameterGroupNotFoundFault",
                &format!("Parameter group {name} not found."),
            ));
        };
        if let Some(arr) = b.get("ParameterNameValues").and_then(Value::as_array) {
            for pv in arr {
                if let (Some(pn), Some(pval)) = (
                    pv.get("ParameterName").and_then(Value::as_str),
                    pv.get("ParameterValue").and_then(Value::as_str),
                ) {
                    p.parameters.insert(pn.to_string(), pval.to_string());
                }
            }
        }
        let out = pg_json(p);
        ok(json!({ "ParameterGroup": out }))
    }

    fn reset_parameter_group(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        let name = req_str(&b, "ParameterGroupName")?.to_string();
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let Some(p) = st.parameter_groups.get_mut(&name) else {
            return Err(fault(
                "ParameterGroupNotFoundFault",
                &format!("Parameter group {name} not found."),
            ));
        };
        let all = b
            .get("AllParameters")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        if all {
            p.parameters.clear();
        } else {
            for pv in b
                .get("ParameterNames")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
            {
                if let Some(pn) = pv.as_str() {
                    p.parameters.remove(pn);
                }
            }
        }
        let out = pg_json(p);
        ok(json!({ "ParameterGroup": out }))
    }

    fn delete_parameter_group(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        let name = req_str(&b, "ParameterGroupName")?.to_string();
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let Some(p) = st.parameter_groups.remove(&name) else {
            return Err(fault(
                "ParameterGroupNotFoundFault",
                &format!("Parameter group {name} not found."),
            ));
        };
        st.tags.remove(&p.arn);
        ok(json!({ "ParameterGroup": pg_json(&p) }))
    }

    fn describe_parameters(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        let name = req_str(&b, "ParameterGroupName")?.to_string();
        let accounts = self.state.read();
        let st = accounts.get(&req.account_id);
        let Some(p) = st.and_then(|s| s.parameter_groups.get(&name)) else {
            return Err(fault(
                "ParameterGroupNotFoundFault",
                &format!("Parameter group {name} not found."),
            ));
        };
        let params: Vec<Value> = p
            .parameters
            .iter()
            .map(|(k, v)| {
                json!({
                    "Name": k, "Value": v, "Description": "", "DataType": "string",
                    "AllowedValues": "", "MinimumEngineVersion": "6.2",
                })
            })
            .collect();
        ok(json!({ "Parameters": params, "NextToken": Value::Null }))
    }

    fn create_subnet_group(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        let name = req_str(&b, "SubnetGroupName")?.to_string();
        let subnet_ids = str_list(&b, "SubnetIds");
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        if st.subnet_groups.contains_key(&name) {
            return Err(fault(
                "SubnetGroupAlreadyExistsFault",
                &format!("Subnet group {name} already exists."),
            ));
        }
        let sg_arn = arn("subnetgroup", &req.region, &req.account_id, &name);
        let sg = SubnetGroup {
            name: name.clone(),
            description: opt_str(&b, "Description").unwrap_or_default(),
            vpc_id: "vpc-00000000".to_string(),
            subnet_ids,
            arn: sg_arn.clone(),
            supported_network_types: vec!["ipv4".to_string()],
        };
        let tags = parse_tags(&b);
        if !tags.is_empty() {
            st.tags.insert(sg_arn, tags);
        }
        st.subnet_groups.insert(name, sg.clone());
        ok(json!({ "SubnetGroup": sg_json(&sg) }))
    }

    fn describe_subnet_groups(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        let accounts = self.state.read();
        let st = accounts.get(&req.account_id);
        let Some(st) = st else {
            return ok(json!({ "SubnetGroups": [] }));
        };
        if let Some(name) = opt_str(&b, "SubnetGroupName") {
            let Some(g) = st.subnet_groups.get(&name) else {
                return Err(fault(
                    "SubnetGroupNotFoundFault",
                    &format!("Subnet group {name} not found."),
                ));
            };
            return ok(json!({ "SubnetGroups": [sg_json(g)] }));
        }
        let items: Vec<Value> = st.subnet_groups.values().map(sg_json).collect();
        let (page, next) = paginate(items, &b, 100);
        ok(json!({ "SubnetGroups": page, "NextToken": next }))
    }

    fn update_subnet_group(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        let name = req_str(&b, "SubnetGroupName")?.to_string();
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let Some(g) = st.subnet_groups.get_mut(&name) else {
            return Err(fault(
                "SubnetGroupNotFoundFault",
                &format!("Subnet group {name} not found."),
            ));
        };
        if let Some(d) = opt_str(&b, "Description") {
            g.description = d;
        }
        let ids = str_list(&b, "SubnetIds");
        if !ids.is_empty() {
            g.subnet_ids = ids;
        }
        let out = sg_json(g);
        ok(json!({ "SubnetGroup": out }))
    }

    fn delete_subnet_group(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        let name = req_str(&b, "SubnetGroupName")?.to_string();
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let Some(g) = st.subnet_groups.remove(&name) else {
            return Err(fault(
                "SubnetGroupNotFoundFault",
                &format!("Subnet group {name} not found."),
            ));
        };
        st.tags.remove(&g.arn);
        ok(json!({ "SubnetGroup": sg_json(&g) }))
    }

    fn create_snapshot(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        let cluster_name = req_str(&b, "ClusterName")?.to_string();
        let snap_name = req_str(&b, "SnapshotName")?.to_string();
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        if st.snapshots.contains_key(&snap_name) {
            return Err(fault(
                "SnapshotAlreadyExistsFault",
                &format!("Snapshot {snap_name} already exists."),
            ));
        }
        let Some(c) = st.clusters.get(&cluster_name) else {
            return Err(fault(
                "ClusterNotFoundFault",
                &format!("Cluster {cluster_name} not found."),
            ));
        };
        let cfg = json!({
            "Name": c.name, "Description": c.description, "NodeType": c.node_type,
            "Engine": c.engine, "EngineVersion": c.engine_version, "MaintenanceWindow": c.maintenance_window,
            "TopicArn": c.sns_topic_arn, "Port": c.port, "ParameterGroupName": c.parameter_group_name,
            "SubnetGroupName": c.subnet_group_name, "VpcId": "vpc-00000000",
            "SnapshotRetentionLimit": c.snapshot_retention_limit, "SnapshotWindow": c.snapshot_window,
            "NumShards": c.number_of_shards,
        });
        let snap_arn = arn("snapshot", &req.region, &req.account_id, &snap_name);
        let snap = Snapshot {
            name: snap_name.clone(),
            status: "available".to_string(),
            source: "manual".to_string(),
            kms_key_id: opt_str(&b, "KmsKeyId"),
            arn: snap_arn.clone(),
            data_tiering: c.data_tiering.clone(),
            cluster_configuration: cfg,
        };
        let tags = parse_tags(&b);
        if !tags.is_empty() {
            st.tags.insert(snap_arn, tags);
        }
        st.snapshots.insert(snap_name, snap.clone());
        ok(json!({ "Snapshot": snapshot_json(&snap) }))
    }

    fn copy_snapshot(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        let source = req_str(&b, "SourceSnapshotName")?.to_string();
        let target = req_str(&b, "TargetSnapshotName")?.to_string();
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let Some(src) = st.snapshots.get(&source).cloned() else {
            return Err(fault(
                "SnapshotNotFoundFault",
                &format!("Snapshot {source} not found."),
            ));
        };
        if st.snapshots.contains_key(&target) {
            return Err(fault(
                "SnapshotAlreadyExistsFault",
                &format!("Snapshot {target} already exists."),
            ));
        }
        let snap_arn = arn("snapshot", &req.region, &req.account_id, &target);
        let snap = Snapshot {
            name: target.clone(),
            status: "available".to_string(),
            source: "manual".to_string(),
            kms_key_id: opt_str(&b, "KmsKeyId").or(src.kms_key_id),
            arn: snap_arn,
            data_tiering: src.data_tiering,
            cluster_configuration: src.cluster_configuration,
        };
        st.snapshots.insert(target, snap.clone());
        ok(json!({ "Snapshot": snapshot_json(&snap) }))
    }

    fn describe_snapshots(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        let accounts = self.state.read();
        let st = accounts.get(&req.account_id);
        let Some(st) = st else {
            return ok(json!({ "Snapshots": [] }));
        };
        if let Some(name) = opt_str(&b, "SnapshotName") {
            let Some(s) = st.snapshots.get(&name) else {
                return Err(fault(
                    "SnapshotNotFoundFault",
                    &format!("Snapshot {name} not found."),
                ));
            };
            return ok(json!({ "Snapshots": [snapshot_json(s)] }));
        }
        let cluster_filter = opt_str(&b, "ClusterName");
        let items: Vec<Value> = st
            .snapshots
            .values()
            .filter(|s| {
                cluster_filter.as_ref().is_none_or(|cn| {
                    s.cluster_configuration.get("Name").and_then(Value::as_str) == Some(cn)
                })
            })
            .map(snapshot_json)
            .collect();
        let (page, next) = paginate(items, &b, 50);
        ok(json!({ "Snapshots": page, "NextToken": next }))
    }

    fn delete_snapshot(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        let name = req_str(&b, "SnapshotName")?.to_string();
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let Some(mut s) = st.snapshots.remove(&name) else {
            return Err(fault(
                "SnapshotNotFoundFault",
                &format!("Snapshot {name} not found."),
            ));
        };
        s.status = "deleting".to_string();
        st.tags.remove(&s.arn);
        ok(json!({ "Snapshot": snapshot_json(&s) }))
    }

    fn create_multi_region_cluster(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        let suffix = req_str(&b, "MultiRegionClusterNameSuffix")?.to_string();
        let node_type = req_str(&b, "NodeType")?.to_string();
        let name = format!("virxk-{suffix}");
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        if st.multi_region_clusters.contains_key(&name) {
            return Err(fault(
                "MultiRegionClusterAlreadyExistsFault",
                &format!("Multi-region cluster {name} already exists."),
            ));
        }
        let mrc_arn = arn("multiregioncluster", &req.region, &req.account_id, &name);
        let mrc = MultiRegionCluster {
            name: name.clone(),
            description: opt_str(&b, "Description"),
            status: "creating".to_string(),
            node_type,
            engine: opt_str(&b, "Engine").unwrap_or_else(|| DEFAULT_ENGINE.to_string()),
            engine_version: opt_str(&b, "EngineVersion")
                .unwrap_or_else(|| DEFAULT_ENGINE_VERSION.to_string()),
            number_of_shards: b.get("NumShards").and_then(Value::as_i64).unwrap_or(1) as i32,
            multi_region_parameter_group_name: opt_str(&b, "MultiRegionParameterGroupName"),
            tls_enabled: b.get("TLSEnabled").and_then(Value::as_bool).unwrap_or(true),
            arn: mrc_arn.clone(),
            clusters: json!([]),
        };
        let tags = parse_tags(&b);
        if !tags.is_empty() {
            st.tags.insert(mrc_arn, tags);
        }
        st.multi_region_clusters.insert(name, mrc.clone());
        ok(json!({ "MultiRegionCluster": mrc_json(&mrc) }))
    }

    fn describe_multi_region_clusters(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        for m in st.multi_region_clusters.values_mut() {
            if m.status == "creating" {
                m.status = "available".to_string();
            }
        }
        if let Some(name) = opt_str(&b, "MultiRegionClusterName") {
            let Some(m) = st.multi_region_clusters.get(&name) else {
                return Err(fault(
                    "MultiRegionClusterNotFoundFault",
                    &format!("Multi-region cluster {name} not found."),
                ));
            };
            return ok(json!({ "MultiRegionClusters": [mrc_json(m)] }));
        }
        let items: Vec<Value> = st.multi_region_clusters.values().map(mrc_json).collect();
        let (page, next) = paginate(items, &b, 100);
        ok(json!({ "MultiRegionClusters": page, "NextToken": next }))
    }

    fn update_multi_region_cluster(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        let name = req_str(&b, "MultiRegionClusterName")?.to_string();
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let Some(m) = st.multi_region_clusters.get_mut(&name) else {
            return Err(fault(
                "MultiRegionClusterNotFoundFault",
                &format!("Multi-region cluster {name} not found."),
            ));
        };
        if let Some(d) = opt_str(&b, "Description") {
            m.description = Some(d);
        }
        if let Some(v) = opt_str(&b, "EngineVersion") {
            m.engine_version = v;
        }
        if let Some(v) = opt_str(&b, "NodeType") {
            m.node_type = v;
        }
        let out = mrc_json(m);
        ok(json!({ "MultiRegionCluster": out }))
    }

    fn delete_multi_region_cluster(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        let name = req_str(&b, "MultiRegionClusterName")?.to_string();
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let Some(mut m) = st.multi_region_clusters.remove(&name) else {
            return Err(fault(
                "MultiRegionClusterNotFoundFault",
                &format!("Multi-region cluster {name} not found."),
            ));
        };
        m.status = "deleting".to_string();
        st.tags.remove(&m.arn);
        ok(json!({ "MultiRegionCluster": mrc_json(&m) }))
    }

    fn list_allowed_mr_updates(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        let name = req_str(&b, "MultiRegionClusterName")?.to_string();
        let accounts = self.state.read();
        if accounts
            .get(&req.account_id)
            .and_then(|s| s.multi_region_clusters.get(&name))
            .is_none()
        {
            return Err(fault(
                "MultiRegionClusterNotFoundFault",
                &format!("Multi-region cluster {name} not found."),
            ));
        }
        ok(
            json!({ "ScaleUpNodeTypes": ["db.r7g.large", "db.r7g.xlarge"], "ScaleDownNodeTypes": [] }),
        )
    }

    fn describe_mr_parameter_groups(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let _ = parse(req)?;
        ok(json!({ "MultiRegionParameterGroups": [], "NextToken": Value::Null }))
    }

    fn describe_mr_parameters(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        req_str(&b, "MultiRegionParameterGroupName")?;
        ok(json!({ "MultiRegionParameters": [], "NextToken": Value::Null }))
    }

    fn tag_resource(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        let resource_arn = req_str(&b, "ResourceArn")?.to_string();
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        if !arn_exists(st, &resource_arn) {
            return Err(fault(
                "InvalidARNFault",
                &format!("{resource_arn} does not refer to a known resource."),
            ));
        }
        let entry = st.tags.entry(resource_arn).or_default();
        for (k, v) in parse_tags(&b) {
            entry.insert(k, v);
        }
        let all = entry.clone();
        ok(json!({ "TagList": tags_json(&all) }))
    }

    fn untag_resource(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        let resource_arn = req_str(&b, "ResourceArn")?.to_string();
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        if !arn_exists(st, &resource_arn) {
            return Err(fault(
                "InvalidARNFault",
                &format!("{resource_arn} does not refer to a known resource."),
            ));
        }
        let keys = str_list(&b, "TagKeys");
        let entry = st.tags.entry(resource_arn).or_default();
        for k in &keys {
            entry.remove(k);
        }
        let all = entry.clone();
        ok(json!({ "TagList": tags_json(&all) }))
    }

    fn list_tags(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        let resource_arn = req_str(&b, "ResourceArn")?.to_string();
        let accounts = self.state.read();
        let st = accounts.get(&req.account_id);
        let Some(st) = st else {
            return ok(json!({ "TagList": [] }));
        };
        if !arn_exists(st, &resource_arn) {
            return Err(fault(
                "InvalidARNFault",
                &format!("{resource_arn} does not refer to a known resource."),
            ));
        }
        let tags = st.tags.get(&resource_arn).cloned().unwrap_or_default();
        ok(json!({ "TagList": tags_json(&tags) }))
    }

    fn describe_engine_versions(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let _ = parse(req)?;
        let versions = json!([
            { "Engine": "redis", "EngineVersion": "7.1", "EnginePatchVersion": "7.1.1", "ParameterGroupFamily": "memorydb_redis7" },
            { "Engine": "redis", "EngineVersion": "6.2", "EnginePatchVersion": "6.2.6", "ParameterGroupFamily": "memorydb_redis6" },
            { "Engine": "valkey", "EngineVersion": "7.2", "EnginePatchVersion": "7.2.0", "ParameterGroupFamily": "memorydb_valkey7" },
        ]);
        ok(json!({ "EngineVersions": versions, "NextToken": Value::Null }))
    }

    fn describe_events(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        if let Some(st) = opt_str(&b, "SourceType") {
            const VALID: &[&str] = &[
                "node",
                "parameter_group",
                "subnet_group",
                "cluster",
                "user",
                "acl",
            ];
            if !VALID.contains(&st.as_str()) {
                return Err(fault(
                    "InvalidParameterValueException",
                    &format!("Invalid SourceType: {st}"),
                ));
            }
        }
        ok(json!({ "Events": [], "NextToken": Value::Null }))
    }

    fn describe_service_updates(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let _ = parse(req)?;
        ok(json!({ "ServiceUpdates": [], "NextToken": Value::Null }))
    }

    fn describe_reserved_nodes(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        let accounts = self.state.read();
        let st = accounts.get(&req.account_id);
        let Some(st) = st else {
            return ok(json!({ "ReservedNodes": [] }));
        };
        let items: Vec<Value> = st.reserved_nodes.values().map(reserved_node_json).collect();
        let (page, next) = paginate(items, &b, 100);
        ok(json!({ "ReservedNodes": page, "NextToken": next }))
    }

    fn describe_reserved_nodes_offerings(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let _ = parse(req)?;
        let offerings = json!([
            {
                "ReservedNodesOfferingId": "00000000-1111-2222-3333-444444444444",
                "NodeType": "db.r6g.large", "Duration": 31536000, "FixedPrice": 1200.0,
                "OfferingType": "All Upfront", "RecurringCharges": [],
            }
        ]);
        ok(json!({ "ReservedNodesOfferings": offerings, "NextToken": Value::Null }))
    }

    fn purchase_reserved_nodes_offering(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        let offering_id = req_str(&b, "ReservedNodesOfferingId")?.to_string();
        let reservation_id = opt_str(&b, "ReservationId")
            .unwrap_or_else(|| format!("ri-{}", uuid::Uuid::new_v4().simple()));
        let count = b.get("NodeCount").and_then(Value::as_i64).unwrap_or(1) as i32;
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let node_arn = arn(
            "reservednode",
            &req.region,
            &req.account_id,
            &reservation_id,
        );
        let rn = ReservedNode {
            reservation_id: reservation_id.clone(),
            reserved_nodes_offering_id: offering_id,
            node_type: "db.r6g.large".to_string(),
            start_time: Utc::now(),
            duration: 31536000,
            fixed_price: 1200.0,
            node_count: count,
            offering_type: "All Upfront".to_string(),
            state: "active".to_string(),
            arn: node_arn,
        };
        st.reserved_nodes.insert(reservation_id, rn.clone());
        ok(json!({ "ReservedNode": reserved_node_json(&rn) }))
    }

    fn list_allowed_node_type_updates(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        let name = req_str(&b, "ClusterName")?.to_string();
        let accounts = self.state.read();
        if accounts
            .get(&req.account_id)
            .and_then(|s| s.clusters.get(&name))
            .is_none()
        {
            return Err(fault(
                "ClusterNotFoundFault",
                &format!("Cluster {name} not found."),
            ));
        }
        ok(json!({
            "ScaleUpNodeTypes": ["db.r6g.xlarge", "db.r6g.2xlarge"],
            "ScaleDownNodeTypes": ["db.r6g.large"],
        }))
    }
}

/// Validate a `UserName` against the model constraints: length >= 1 and
/// pattern `^[a-zA-Z][a-zA-Z0-9-]*$`.
fn validate_user_name(name: &str) -> Result<(), AwsServiceError> {
    let valid = !name.is_empty()
        && name.starts_with(|c: char| c.is_ascii_alphabetic())
        && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '-');
    if valid {
        Ok(())
    } else {
        Err(fault(
            "InvalidParameterValueException",
            &format!("Invalid UserName: {name}"),
        ))
    }
}

/// Whether a given ARN refers to any resource in this account's state.
fn arn_exists(st: &MemoryDbState, resource_arn: &str) -> bool {
    // An empty ARN never refers to a resource (guards against matching a
    // resource that happens to carry an empty ARN string).
    if resource_arn.is_empty() {
        return false;
    }
    st.clusters.values().any(|c| c.arn == resource_arn)
        || st.acls.values().any(|a| a.arn == resource_arn)
        || st.users.values().any(|u| u.arn == resource_arn)
        || st.parameter_groups.values().any(|p| p.arn == resource_arn)
        || st.subnet_groups.values().any(|g| g.arn == resource_arn)
        || st.snapshots.values().any(|s| s.arn == resource_arn)
        || st
            .multi_region_clusters
            .values()
            .any(|m| m.arn == resource_arn)
        || st.reserved_nodes.values().any(|r| r.arn == resource_arn)
}

#[cfg(test)]
mod tests {
    use super::*;
    use fakecloud_core::multi_account::MultiAccountState;

    fn service() -> MemoryDbService {
        let state = Arc::new(parking_lot::RwLock::new(MultiAccountState::new(
            "123456789012",
            "us-east-1",
            "http://localhost:4566",
        )));
        MemoryDbService::new(state)
    }

    fn req(action: &str, body: Value) -> AwsRequest {
        AwsRequest {
            service: "memorydb".to_string(),
            action: action.to_string(),
            region: "us-east-1".to_string(),
            account_id: "123456789012".to_string(),
            request_id: "rid".to_string(),
            headers: http::HeaderMap::new(),
            query_params: std::collections::HashMap::new(),
            body: bytes::Bytes::from(serde_json::to_vec(&body).unwrap()),
            body_stream: parking_lot::Mutex::new(None),
            path_segments: vec![],
            raw_path: "/".to_string(),
            raw_query: String::new(),
            method: http::Method::POST,
            is_query_protocol: false,
            access_key_id: None,
            principal: None,
        }
    }

    fn call(s: &MemoryDbService, action: &str, body: Value) -> Result<Value, AwsServiceError> {
        let resp = dispatch(s, &req(action, body))?;
        Ok(serde_json::from_slice(resp.body.expect_bytes()).unwrap())
    }

    #[test]
    fn create_cluster_round_trips_and_describes() {
        let s = service();
        let out = call(
            &s,
            "CreateCluster",
            json!({"ClusterName": "app", "NodeType": "db.r6g.large", "ACLName": "open-access"}),
        )
        .unwrap();
        assert_eq!(out["Cluster"]["Name"], "app");

        let desc = call(&s, "DescribeClusters", json!({"ClusterName": "app"})).unwrap();
        let clusters = desc["Clusters"].as_array().unwrap();
        assert_eq!(clusters.len(), 1);
        // Lazily transitions creating -> available on describe.
        assert_eq!(clusters[0]["Status"], "available");
    }

    #[test]
    fn create_cluster_rejects_out_of_range_shard_count() {
        let s = service();
        // A huge NumShards must be rejected, not allocated (OOM guard).
        let err = call(
            &s,
            "CreateCluster",
            json!({"ClusterName": "big", "NodeType": "db.r6g.large", "ACLName": "open-access", "NumShards": 2_000_000_000i64}),
        )
        .unwrap_err();
        assert!(
            err.message().contains("NumShards"),
            "got: {}",
            err.message()
        );
    }

    #[test]
    fn create_cluster_rejects_out_of_range_replica_count() {
        let s = service();
        let err = call(
            &s,
            "CreateCluster",
            json!({"ClusterName": "r", "NodeType": "db.r6g.large", "ACLName": "open-access", "NumReplicasPerShard": 99}),
        )
        .unwrap_err();
        assert!(
            err.message().contains("NumReplicasPerShard"),
            "got: {}",
            err.message()
        );
    }

    #[test]
    fn create_user_rejects_invalid_name() {
        let s = service();
        // Must start with a letter.
        let err = call(
            &s,
            "CreateUser",
            json!({"UserName": "1bad", "AccessString": "on ~* &* +@all", "AuthenticationMode": {"Type": "no-password-required"}}),
        )
        .unwrap_err();
        assert!(err.message().contains("UserName"), "got: {}", err.message());
    }

    #[test]
    fn default_acl_and_user_are_seeded() {
        let s = service();
        let acls = call(&s, "DescribeACLs", json!({})).unwrap();
        assert!(acls["ACLs"]
            .as_array()
            .unwrap()
            .iter()
            .any(|a| a["Name"] == "open-access"));
        let users = call(&s, "DescribeUsers", json!({})).unwrap();
        assert!(users["Users"]
            .as_array()
            .unwrap()
            .iter()
            .any(|u| u["Name"] == "default"));
    }

    #[test]
    fn seeded_default_acl_and_user_have_populated_arns() {
        let s = service();
        let acls = call(&s, "DescribeACLs", json!({"ACLName": "open-access"})).unwrap();
        assert_eq!(
            acls["ACLs"][0]["ARN"],
            "arn:aws:memorydb:us-east-1:123456789012:acl/open-access"
        );
        let users = call(&s, "DescribeUsers", json!({"UserName": "default"})).unwrap();
        assert_eq!(
            users["Users"][0]["ARN"],
            "arn:aws:memorydb:us-east-1:123456789012:user/default"
        );
    }

    #[test]
    fn not_found_faults_return_http_404() {
        let s = service();
        let err = call(&s, "DescribeClusters", json!({"ClusterName": "nope"})).unwrap_err();
        assert_eq!(err.status(), http::StatusCode::NOT_FOUND);
        // A validation error stays a 400.
        let bad = call(
            &s,
            "CreateCluster",
            json!({"ClusterName": "c", "NodeType": "db.r6g.large", "ACLName": "open-access", "NumShards": 9999}),
        )
        .unwrap_err();
        assert_eq!(bad.status(), http::StatusCode::BAD_REQUEST);
    }

    #[test]
    fn create_cluster_populates_acl_clusters_backref() {
        let s = service();
        call(
            &s,
            "CreateCluster",
            json!({"ClusterName": "c1", "NodeType": "db.r6g.large", "ACLName": "open-access"}),
        )
        .unwrap();
        let acls = call(&s, "DescribeACLs", json!({"ACLName": "open-access"})).unwrap();
        let clusters = acls["ACLs"][0]["Clusters"].as_array().unwrap();
        assert!(clusters.iter().any(|c| c == "c1"), "got: {clusters:?}");

        // Deleting the cluster removes the back-reference.
        call(&s, "DeleteCluster", json!({"ClusterName": "c1"})).unwrap();
        let acls = call(&s, "DescribeACLs", json!({"ACLName": "open-access"})).unwrap();
        assert!(acls["ACLs"][0]["Clusters"].as_array().unwrap().is_empty());
    }

    #[test]
    fn acl_membership_syncs_user_acl_names_backref() {
        let s = service();
        call(
            &s,
            "CreateUser",
            json!({"UserName": "bob", "AccessString": "on ~* &* +@all", "AuthenticationMode": {"Type": "iam"}}),
        )
        .unwrap();
        call(
            &s,
            "CreateACL",
            json!({"ACLName": "team", "UserNames": ["bob"]}),
        )
        .unwrap();
        let users = call(&s, "DescribeUsers", json!({"UserName": "bob"})).unwrap();
        let acl_names = users["Users"][0]["ACLNames"].as_array().unwrap();
        assert!(acl_names.iter().any(|a| a == "team"), "got: {acl_names:?}");

        // Removing the user from the ACL drops the back-reference.
        call(
            &s,
            "UpdateACL",
            json!({"ACLName": "team", "UserNamesToRemove": ["bob"]}),
        )
        .unwrap();
        let users = call(&s, "DescribeUsers", json!({"UserName": "bob"})).unwrap();
        assert!(users["Users"][0]["ACLNames"]
            .as_array()
            .unwrap()
            .iter()
            .all(|a| a != "team"));
    }

    #[test]
    fn tag_resource_rejects_empty_arn() {
        let s = service();
        let err = call(
            &s,
            "TagResource",
            json!({"ResourceArn": "", "Tags": [{"Key": "k", "Value": "v"}]}),
        )
        .unwrap_err();
        assert_eq!(err.status(), http::StatusCode::BAD_REQUEST);
        assert!(
            err.message().contains("known resource") || err.message().contains("ARN"),
            "got: {}",
            err.message()
        );
    }
}
