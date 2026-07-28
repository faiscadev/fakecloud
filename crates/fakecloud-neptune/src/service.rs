//! Neptune Query-protocol service handler.

use std::sync::Arc;

use async_trait::async_trait;
use chrono::Utc;
use http::StatusCode;
use tokio::sync::Mutex as AsyncMutex;

use fakecloud_core::query::{optional_query_param, query_response_xml, required_query_param};
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsService, AwsServiceError};
use fakecloud_persistence::SnapshotStore;

use crate::state::{
    ClusterMember, ClusterRole, DbCluster, DbClusterEndpoint, DbClusterParameterGroup,
    DbClusterSnapshot, DbInstance, DbParameterGroup, DbSubnetGroup, EventSubscription,
    GlobalCluster, NeptuneSnapshot, NeptuneState, ParameterValue, SharedNeptuneState, Subnet, Tag,
    NEPTUNE_SNAPSHOT_SCHEMA_VERSION,
};
use crate::xml;

const NEPTUNE_NS: &str = "http://rds.amazonaws.com/doc/2014-10-31/";
const DEFAULT_ENGINE: &str = "neptune";
const DEFAULT_ENGINE_VERSION: &str = "1.3.2.1";
const DEFAULT_PORT: i32 = 8182;
const DEFAULT_FAMILY: &str = "neptune1.3";

/// Every operation the Neptune service supports (full Smithy surface).
const SUPPORTED_ACTIONS: &[&str] = &[
    "AddRoleToDBCluster",
    "AddSourceIdentifierToSubscription",
    "AddTagsToResource",
    "ApplyPendingMaintenanceAction",
    "CopyDBClusterParameterGroup",
    "CopyDBClusterSnapshot",
    "CopyDBParameterGroup",
    "CreateDBCluster",
    "CreateDBClusterEndpoint",
    "CreateDBClusterParameterGroup",
    "CreateDBClusterSnapshot",
    "CreateDBInstance",
    "CreateDBParameterGroup",
    "CreateDBSubnetGroup",
    "CreateEventSubscription",
    "CreateGlobalCluster",
    "DeleteDBCluster",
    "DeleteDBClusterEndpoint",
    "DeleteDBClusterParameterGroup",
    "DeleteDBClusterSnapshot",
    "DeleteDBInstance",
    "DeleteDBParameterGroup",
    "DeleteDBSubnetGroup",
    "DeleteEventSubscription",
    "DeleteGlobalCluster",
    "DescribeDBClusterEndpoints",
    "DescribeDBClusterParameterGroups",
    "DescribeDBClusterParameters",
    "DescribeDBClusterSnapshotAttributes",
    "DescribeDBClusterSnapshots",
    "DescribeDBClusters",
    "DescribeDBEngineVersions",
    "DescribeDBInstances",
    "DescribeDBParameterGroups",
    "DescribeDBParameters",
    "DescribeDBSubnetGroups",
    "DescribeEngineDefaultClusterParameters",
    "DescribeEngineDefaultParameters",
    "DescribeEventCategories",
    "DescribeEventSubscriptions",
    "DescribeEvents",
    "DescribeGlobalClusters",
    "DescribeOrderableDBInstanceOptions",
    "DescribePendingMaintenanceActions",
    "DescribeValidDBInstanceModifications",
    "FailoverDBCluster",
    "FailoverGlobalCluster",
    "ListTagsForResource",
    "ModifyDBCluster",
    "ModifyDBClusterEndpoint",
    "ModifyDBClusterParameterGroup",
    "ModifyDBClusterSnapshotAttribute",
    "ModifyDBInstance",
    "ModifyDBParameterGroup",
    "ModifyDBSubnetGroup",
    "ModifyEventSubscription",
    "ModifyGlobalCluster",
    "PromoteReadReplicaDBCluster",
    "RebootDBInstance",
    "RemoveFromGlobalCluster",
    "RemoveRoleFromDBCluster",
    "RemoveSourceIdentifierFromSubscription",
    "RemoveTagsFromResource",
    "ResetDBClusterParameterGroup",
    "ResetDBParameterGroup",
    "RestoreDBClusterFromSnapshot",
    "RestoreDBClusterToPointInTime",
    "StartDBCluster",
    "StopDBCluster",
    "SwitchoverGlobalCluster",
];

/// The Neptune control-plane service.
pub struct NeptuneService {
    state: SharedNeptuneState,
    snapshot_store: Option<Arc<dyn SnapshotStore>>,
    snapshot_lock: Arc<AsyncMutex<()>>,
}

impl NeptuneService {
    pub fn new(state: SharedNeptuneState) -> Self {
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

    /// Synchronous entry point for the CloudFormation provisioner. Routes the
    /// cluster create/delete/describe actions through the real handlers (the
    /// same code path `handle` runs, including prevalidation) so a
    /// CFN-provisioned `AWS::Neptune::DBCluster` has identical state to the
    /// direct API. Persistence is driven by the CFN layer's registered snapshot
    /// hook.
    pub fn provision_sync(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        crate::validation::prevalidate(req.action.as_str(), req)?;
        match req.action.as_str() {
            "CreateDBCluster" => self.create_db_cluster(req),
            "DeleteDBCluster" => self.delete_db_cluster(req),
            "DescribeDBClusters" => self.describe_db_clusters(req),
            other => Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidAction",
                format!("Neptune CFN provisioning does not support action {other}"),
            )),
        }
    }

    /// A persistence hook usable by the CloudFormation provisioner / restart
    /// path. `None` in memory mode.
    pub fn snapshot_hook(&self) -> Option<fakecloud_persistence::SnapshotHook> {
        let store = self.snapshot_store.clone()?;
        let state = self.state.clone();
        let lock = self.snapshot_lock.clone();
        Some(Arc::new(move || {
            let state = state.clone();
            let store = store.clone();
            let lock = lock.clone();
            Box::pin(async move {
                save_snapshot_static(state, Some(store), lock).await;
            })
        }))
    }

    async fn save_snapshot(&self) {
        save_snapshot_static(
            self.state.clone(),
            self.snapshot_store.clone(),
            self.snapshot_lock.clone(),
        )
        .await;
    }
}

/// Persist the current state to the configured snapshot store; no-op in
/// memory mode. Free function so callers without a `&NeptuneService` (hooks)
/// can save too.
async fn save_snapshot_static(
    state: SharedNeptuneState,
    store: Option<Arc<dyn SnapshotStore>>,
    lock: Arc<AsyncMutex<()>>,
) {
    let Some(store) = store else {
        return;
    };
    let _guard = lock.lock().await;
    let snapshot = NeptuneSnapshot {
        schema_version: NEPTUNE_SNAPSHOT_SCHEMA_VERSION,
        accounts: Some(state.read().clone()),
    };
    let join = tokio::task::spawn_blocking(move || -> std::io::Result<()> {
        let bytes = serde_json::to_vec(&snapshot)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?;
        store.save(&bytes)
    })
    .await;
    match join {
        Ok(Ok(())) => {}
        Ok(Err(err)) => tracing::error!(%err, "failed to write neptune snapshot"),
        Err(err) => tracing::error!(%err, "neptune snapshot task panicked"),
    }
}

// ---------------------------------------------------------------------------
// Fault helpers
// ---------------------------------------------------------------------------

fn fault(status: StatusCode, code: &str, message: String) -> AwsServiceError {
    AwsServiceError::aws_error(status, code, message)
}

fn db_cluster_not_found(id: &str) -> AwsServiceError {
    fault(
        StatusCode::NOT_FOUND,
        "DBClusterNotFoundFault",
        format!("DBCluster {id} not found."),
    )
}
fn db_cluster_already_exists(id: &str) -> AwsServiceError {
    fault(
        StatusCode::BAD_REQUEST,
        "DBClusterAlreadyExistsFault",
        format!("DBCluster already exists: {id}"),
    )
}
fn db_instance_not_found(id: &str) -> AwsServiceError {
    fault(
        StatusCode::NOT_FOUND,
        "DBInstanceNotFound",
        format!("DBInstance {id} not found."),
    )
}
fn db_instance_already_exists(id: &str) -> AwsServiceError {
    fault(
        StatusCode::BAD_REQUEST,
        "DBInstanceAlreadyExists",
        format!("DBInstance already exists: {id}"),
    )
}
fn cluster_endpoint_not_found(id: &str) -> AwsServiceError {
    fault(
        StatusCode::BAD_REQUEST,
        "DBClusterEndpointNotFoundFault",
        format!("DBClusterEndpoint {id} not found."),
    )
}
fn cluster_endpoint_already_exists(id: &str) -> AwsServiceError {
    fault(
        StatusCode::BAD_REQUEST,
        "DBClusterEndpointAlreadyExistsFault",
        format!("DBClusterEndpoint already exists: {id}"),
    )
}
fn snapshot_not_found(id: &str) -> AwsServiceError {
    fault(
        StatusCode::NOT_FOUND,
        "DBClusterSnapshotNotFoundFault",
        format!("DBClusterSnapshot {id} not found."),
    )
}
fn snapshot_already_exists(id: &str) -> AwsServiceError {
    fault(
        StatusCode::BAD_REQUEST,
        "DBClusterSnapshotAlreadyExistsFault",
        format!("DBClusterSnapshot already exists: {id}"),
    )
}
fn param_group_not_found(name: &str) -> AwsServiceError {
    fault(
        StatusCode::NOT_FOUND,
        "DBParameterGroupNotFound",
        format!("DBParameterGroup {name} not found."),
    )
}
fn param_group_already_exists(name: &str) -> AwsServiceError {
    fault(
        StatusCode::BAD_REQUEST,
        "DBParameterGroupAlreadyExists",
        format!("Parameter group already exists: {name}"),
    )
}
fn subnet_group_not_found(name: &str) -> AwsServiceError {
    fault(
        StatusCode::NOT_FOUND,
        "DBSubnetGroupNotFoundFault",
        format!("DBSubnetGroup {name} not found."),
    )
}
fn subnet_group_already_exists(name: &str) -> AwsServiceError {
    fault(
        StatusCode::BAD_REQUEST,
        "DBSubnetGroupAlreadyExists",
        format!("DBSubnetGroup already exists: {name}"),
    )
}
fn global_cluster_not_found(id: &str) -> AwsServiceError {
    fault(
        StatusCode::NOT_FOUND,
        "GlobalClusterNotFoundFault",
        format!("GlobalCluster {id} not found."),
    )
}
fn global_cluster_already_exists(id: &str) -> AwsServiceError {
    fault(
        StatusCode::BAD_REQUEST,
        "GlobalClusterAlreadyExistsFault",
        format!("GlobalCluster already exists: {id}"),
    )
}
fn subscription_not_found(name: &str) -> AwsServiceError {
    fault(
        StatusCode::NOT_FOUND,
        "SubscriptionNotFound",
        format!("Subscription {name} not found."),
    )
}
fn subscription_already_exists(name: &str) -> AwsServiceError {
    fault(
        StatusCode::BAD_REQUEST,
        "SubscriptionAlreadyExist",
        format!("Subscription already exists: {name}"),
    )
}
fn cluster_role_already_exists(arn: &str) -> AwsServiceError {
    fault(
        StatusCode::BAD_REQUEST,
        "DBClusterRoleAlreadyExists",
        format!("Role already associated: {arn}"),
    )
}
fn cluster_role_not_found(arn: &str) -> AwsServiceError {
    fault(
        StatusCode::NOT_FOUND,
        "DBClusterRoleNotFound",
        format!("Role not associated: {arn}"),
    )
}
fn invalid_parameter_value(message: impl Into<String>) -> AwsServiceError {
    fault(
        StatusCode::BAD_REQUEST,
        "InvalidParameterValue",
        message.into(),
    )
}

// ---------------------------------------------------------------------------
// Small helpers
// ---------------------------------------------------------------------------

/// 26-char uppercase resource-id token (`cluster-XXXX`, `db-XXXX`).
fn resource_token() -> String {
    uuid::Uuid::new_v4()
        .simple()
        .to_string()
        .to_uppercase()
        .chars()
        .take(26)
        .collect()
}

/// Lowercase suffix embedded in cluster/instance endpoints.
fn endpoint_suffix() -> String {
    uuid::Uuid::new_v4()
        .simple()
        .to_string()
        .chars()
        .take(12)
        .collect()
}

fn cluster_arn(region: &str, account: &str, id: &str) -> String {
    format!("arn:aws:rds:{region}:{account}:cluster:{id}")
}

/// Collect a Query-protocol list under `base`, trying each candidate member
/// wire-name (AWS xmlName form) plus the generic `member` form the
/// conformance probe uses.
fn collect_list(req: &AwsRequest, base: &str, members: &[&str]) -> Vec<String> {
    let mut out = Vec::new();
    for member in members {
        let mut idx = 1;
        loop {
            let key = format!("{base}.{member}.{idx}");
            match optional_query_param(req, &key) {
                Some(v) => out.push(v),
                None => break,
            }
            idx += 1;
        }
        if !out.is_empty() {
            break;
        }
    }
    out
}

/// Parse `Tags.Tag.N.{Key,Value}` (and the generic `.member.N` form).
fn parse_tags(req: &AwsRequest) -> Vec<Tag> {
    let mut out = Vec::new();
    for member in ["Tag", "member"] {
        let mut idx = 1;
        loop {
            let key = format!("Tags.{member}.{idx}.Key");
            let Some(k) = optional_query_param(req, &key) else {
                break;
            };
            let v = optional_query_param(req, &format!("Tags.{member}.{idx}.Value"))
                .unwrap_or_default();
            out.push(Tag { key: k, value: v });
            idx += 1;
        }
        if !out.is_empty() {
            break;
        }
    }
    out
}

fn ok_xml(action: &str, inner: String, request_id: &str) -> AwsResponse {
    AwsResponse::xml(
        StatusCode::OK,
        query_response_xml(action, NEPTUNE_NS, &inner, request_id),
    )
}

fn is_mutating(action: &str) -> bool {
    !(action.starts_with("Describe") || action.starts_with("List"))
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

impl NeptuneService {
    fn create_db_cluster(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let id = required_query_param(req, "DBClusterIdentifier")?;
        let engine = required_query_param(req, "Engine")?;
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        if st.clusters.contains_key(&id) {
            return Err(db_cluster_already_exists(&id));
        }
        let suffix = endpoint_suffix();
        let cluster = DbCluster {
            db_cluster_identifier: id.clone(),
            db_cluster_arn: cluster_arn(&req.region, &req.account_id, &id),
            db_cluster_resource_id: format!("cluster-{}", resource_token()),
            status: "available".to_string(),
            engine,
            engine_version: optional_query_param(req, "EngineVersion")
                .unwrap_or_else(|| DEFAULT_ENGINE_VERSION.to_string()),
            port: optional_query_param(req, "Port")
                .and_then(|p| p.parse().ok())
                .unwrap_or(DEFAULT_PORT),
            master_username: optional_query_param(req, "MasterUsername")
                .unwrap_or_else(|| "admin".to_string()),
            endpoint: format!("{id}.cluster-{suffix}.{}.neptune.amazonaws.com", req.region),
            reader_endpoint: format!(
                "{id}.cluster-ro-{suffix}.{}.neptune.amazonaws.com",
                req.region
            ),
            hosted_zone_id: "Z1NEPTUNE0000".to_string(),
            db_subnet_group: optional_query_param(req, "DBSubnetGroupName")
                .unwrap_or_else(|| "default".to_string()),
            db_cluster_parameter_group: optional_query_param(req, "DBClusterParameterGroupName")
                .unwrap_or_else(|| format!("default.{DEFAULT_FAMILY}")),
            storage_encrypted: optional_query_param(req, "StorageEncrypted")
                .map(|v| v == "true")
                .unwrap_or(false),
            kms_key_id: optional_query_param(req, "KmsKeyId"),
            deletion_protection: optional_query_param(req, "DeletionProtection")
                .map(|v| v == "true")
                .unwrap_or(false),
            iam_database_authentication_enabled: optional_query_param(
                req,
                "EnableIAMDatabaseAuthentication",
            )
            .map(|v| v == "true")
            .unwrap_or(false),
            backup_retention_period: optional_query_param(req, "BackupRetentionPeriod")
                .and_then(|v| v.parse().ok())
                .unwrap_or(1),
            preferred_backup_window: optional_query_param(req, "PreferredBackupWindow")
                .unwrap_or_else(|| "07:00-07:30".to_string()),
            preferred_maintenance_window: optional_query_param(req, "PreferredMaintenanceWindow")
                .unwrap_or_else(|| "sun:08:00-sun:08:30".to_string()),
            storage_type: optional_query_param(req, "StorageType")
                .unwrap_or_else(|| "standard".to_string()),
            availability_zones: {
                let azs = collect_list(req, "AvailabilityZones", &["AvailabilityZone", "member"]);
                if azs.is_empty() {
                    vec![
                        format!("{}a", req.region),
                        format!("{}b", req.region),
                        format!("{}c", req.region),
                    ]
                } else {
                    azs
                }
            },
            vpc_security_group_ids: collect_list(
                req,
                "VpcSecurityGroupIds",
                &["VpcSecurityGroupId", "member"],
            ),
            enabled_cloudwatch_logs_exports: collect_list(
                req,
                "EnableCloudwatchLogsExports",
                &["member", "LogType"],
            ),
            members: Vec::new(),
            associated_roles: Vec::new(),
            cluster_create_time: Utc::now(),
            tags: parse_tags(req),
        };
        st.clusters.insert(id, cluster.clone());
        Ok(ok_xml(
            "CreateDBCluster",
            format!("<DBCluster>{}</DBCluster>", xml::db_cluster(&cluster)),
            &req.request_id,
        ))
    }

    fn describe_db_clusters(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let filter = optional_query_param(req, "DBClusterIdentifier");
        let accounts = self.state.read();
        let empty = NeptuneState::new(&req.account_id, &req.region);
        let st = accounts.get(&req.account_id).unwrap_or(&empty);
        let clusters: Vec<&DbCluster> = match &filter {
            Some(id) => match st.clusters.get(id) {
                Some(c) => vec![c],
                None => return Err(db_cluster_not_found(id)),
            },
            None => st.clusters.values().collect(),
        };
        let inner: String = clusters
            .iter()
            .map(|c| format!("<DBCluster>{}</DBCluster>", xml::db_cluster(c)))
            .collect();
        Ok(ok_xml(
            "DescribeDBClusters",
            format!("<DBClusters>{inner}</DBClusters>"),
            &req.request_id,
        ))
    }

    fn modify_db_cluster(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let id = required_query_param(req, "DBClusterIdentifier")?;
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let cluster = st
            .clusters
            .get_mut(&id)
            .ok_or_else(|| db_cluster_not_found(&id))?;
        if let Some(v) = optional_query_param(req, "EngineVersion") {
            cluster.engine_version = v;
        }
        if let Some(v) =
            optional_query_param(req, "BackupRetentionPeriod").and_then(|v| v.parse().ok())
        {
            cluster.backup_retention_period = v;
        }
        if let Some(v) = optional_query_param(req, "PreferredBackupWindow") {
            cluster.preferred_backup_window = v;
        }
        if let Some(v) = optional_query_param(req, "PreferredMaintenanceWindow") {
            cluster.preferred_maintenance_window = v;
        }
        if let Some(v) = optional_query_param(req, "DeletionProtection") {
            cluster.deletion_protection = v == "true";
        }
        if let Some(v) = optional_query_param(req, "EnableIAMDatabaseAuthentication") {
            cluster.iam_database_authentication_enabled = v == "true";
        }
        if let Some(v) = optional_query_param(req, "Port").and_then(|v| v.parse().ok()) {
            cluster.port = v;
        }
        if let Some(v) = optional_query_param(req, "DBClusterParameterGroupName") {
            cluster.db_cluster_parameter_group = v;
        }
        let new_vpc_sgs = collect_list(
            req,
            "VpcSecurityGroupIds",
            &["VpcSecurityGroupId", "member"],
        );
        if !new_vpc_sgs.is_empty() {
            cluster.vpc_security_group_ids = new_vpc_sgs;
        }
        // CloudwatchLogsExportConfiguration: enable/disable individual log types.
        let enable_logs = collect_list(
            req,
            "CloudwatchLogsExportConfiguration.EnableLogTypes",
            &["member", "LogType"],
        );
        let disable_logs = collect_list(
            req,
            "CloudwatchLogsExportConfiguration.DisableLogTypes",
            &["member", "LogType"],
        );
        if !enable_logs.is_empty() || !disable_logs.is_empty() {
            cluster
                .enabled_cloudwatch_logs_exports
                .retain(|l| !disable_logs.contains(l));
            for log in enable_logs {
                if !cluster.enabled_cloudwatch_logs_exports.contains(&log) {
                    cluster.enabled_cloudwatch_logs_exports.push(log);
                }
            }
        }
        // NewDBClusterIdentifier: rename.
        let renamed = optional_query_param(req, "NewDBClusterIdentifier");
        let mut cluster = cluster.clone();
        if let Some(new_id) = renamed.filter(|n| n != &id) {
            cluster.db_cluster_identifier = new_id.clone();
            cluster.db_cluster_arn = cluster_arn(&req.region, &req.account_id, &new_id);
            st.clusters.remove(&id);
            st.clusters.insert(new_id, cluster.clone());
        } else {
            st.clusters.insert(id, cluster.clone());
        }
        Ok(ok_xml(
            "ModifyDBCluster",
            format!("<DBCluster>{}</DBCluster>", xml::db_cluster(&cluster)),
            &req.request_id,
        ))
    }

    fn delete_db_cluster(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let id = required_query_param(req, "DBClusterIdentifier")?;
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        // Optional final snapshot.
        if let Some(snap_id) = optional_query_param(req, "FinalDBSnapshotIdentifier") {
            if let Some(c) = st.clusters.get(&id) {
                let snap =
                    snapshot_from_cluster(c, &snap_id, &req.region, &req.account_id, "manual");
                st.cluster_snapshots.insert(snap_id, snap);
            }
        }
        let cluster = st
            .clusters
            .remove(&id)
            .ok_or_else(|| db_cluster_not_found(&id))?;
        // Detach member instances.
        let member_ids: Vec<String> = cluster
            .members
            .iter()
            .map(|m| m.db_instance_identifier.clone())
            .collect();
        for mid in member_ids {
            st.instances.remove(&mid);
        }
        Ok(ok_xml(
            "DeleteDBCluster",
            format!("<DBCluster>{}</DBCluster>", xml::db_cluster(&cluster)),
            &req.request_id,
        ))
    }

    fn cluster_lifecycle(
        &self,
        req: &AwsRequest,
        action: &str,
        status: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let id = required_query_param(req, "DBClusterIdentifier")?;
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let cluster = st
            .clusters
            .get_mut(&id)
            .ok_or_else(|| db_cluster_not_found(&id))?;
        cluster.status = status.to_string();
        let cluster = cluster.clone();
        Ok(ok_xml(
            action,
            format!("<DBCluster>{}</DBCluster>", xml::db_cluster(&cluster)),
            &req.request_id,
        ))
    }

    fn failover_db_cluster(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let id = optional_query_param(req, "DBClusterIdentifier")
            .ok_or_else(|| db_cluster_not_found("<unspecified>"))?;
        // Optional: promote a specific instance to writer, else the first
        // non-writer member is promoted.
        let target = optional_query_param(req, "TargetDBInstanceIdentifier");

        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let cluster = st
            .clusters
            .get_mut(&id)
            .ok_or_else(|| db_cluster_not_found(&id))?;

        // Pick the new writer: the requested target if valid, otherwise the
        // first member that isn't already the writer.
        let new_writer = target
            .as_deref()
            .filter(|t| {
                cluster
                    .members
                    .iter()
                    .any(|m| m.db_instance_identifier == *t)
            })
            .map(String::from)
            .or_else(|| {
                cluster
                    .members
                    .iter()
                    .find(|m| !m.is_writer)
                    .map(|m| m.db_instance_identifier.clone())
            });
        if let Some(writer) = new_writer {
            for m in &mut cluster.members {
                m.is_writer = m.db_instance_identifier == writer;
            }
        }
        let cluster = cluster.clone();
        Ok(ok_xml(
            "FailoverDBCluster",
            format!("<DBCluster>{}</DBCluster>", xml::db_cluster(&cluster)),
            &req.request_id,
        ))
    }

    fn promote_read_replica_db_cluster(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let id = required_query_param(req, "DBClusterIdentifier")?;
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let cluster = st
            .clusters
            .get_mut(&id)
            .ok_or_else(|| db_cluster_not_found(&id))?;
        cluster.status = "available".to_string();
        let cluster = cluster.clone();
        Ok(ok_xml(
            "PromoteReadReplicaDBCluster",
            format!("<DBCluster>{}</DBCluster>", xml::db_cluster(&cluster)),
            &req.request_id,
        ))
    }

    // --- IAM role associations ---

    fn add_role_to_db_cluster(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let id = required_query_param(req, "DBClusterIdentifier")?;
        let role_arn = required_query_param(req, "RoleArn")?;
        let feature_name = optional_query_param(req, "FeatureName");
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let cluster = st
            .clusters
            .get_mut(&id)
            .ok_or_else(|| db_cluster_not_found(&id))?;
        if cluster
            .associated_roles
            .iter()
            .any(|r| r.role_arn == role_arn)
        {
            return Err(cluster_role_already_exists(&role_arn));
        }
        cluster.associated_roles.push(ClusterRole {
            role_arn,
            status: "ACTIVE".to_string(),
            feature_name,
        });
        Ok(ok_xml("AddRoleToDBCluster", String::new(), &req.request_id))
    }

    fn remove_role_from_db_cluster(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let id = required_query_param(req, "DBClusterIdentifier")?;
        let role_arn = required_query_param(req, "RoleArn")?;
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let cluster = st
            .clusters
            .get_mut(&id)
            .ok_or_else(|| db_cluster_not_found(&id))?;
        if !cluster
            .associated_roles
            .iter()
            .any(|r| r.role_arn == role_arn)
        {
            return Err(cluster_role_not_found(&role_arn));
        }
        cluster.associated_roles.retain(|r| r.role_arn != role_arn);
        Ok(ok_xml(
            "RemoveRoleFromDBCluster",
            String::new(),
            &req.request_id,
        ))
    }

    // --- cluster endpoints (Neptune-specific) ---

    fn create_db_cluster_endpoint(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let cluster_id = required_query_param(req, "DBClusterIdentifier")?;
        let endpoint_id = required_query_param(req, "DBClusterEndpointIdentifier")?;
        let endpoint_type = required_query_param(req, "EndpointType")?;
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        if !st.clusters.contains_key(&cluster_id) {
            return Err(db_cluster_not_found(&cluster_id));
        }
        if st.cluster_endpoints.contains_key(&endpoint_id) {
            return Err(cluster_endpoint_already_exists(&endpoint_id));
        }
        let suffix = endpoint_suffix();
        let endpoint = DbClusterEndpoint {
            db_cluster_endpoint_identifier: endpoint_id.clone(),
            db_cluster_identifier: cluster_id,
            db_cluster_endpoint_resource_identifier: format!(
                "cluster-endpoint-{}",
                resource_token()
            ),
            endpoint: format!(
                "{endpoint_id}.cluster-custom-{suffix}.{}.neptune.amazonaws.com",
                req.region
            ),
            status: "available".to_string(),
            endpoint_type,
            custom_endpoint_type: optional_query_param(req, "CustomEndpointType")
                .unwrap_or_else(|| "ANY".to_string()),
            static_members: collect_list(req, "StaticMembers", &["member"]),
            excluded_members: collect_list(req, "ExcludedMembers", &["member"]),
            db_cluster_endpoint_arn: format!(
                "arn:aws:rds:{}:{}:cluster-endpoint:{endpoint_id}",
                req.region, req.account_id
            ),
            tags: parse_tags(req),
        };
        st.cluster_endpoints.insert(endpoint_id, endpoint.clone());
        Ok(ok_xml(
            "CreateDBClusterEndpoint",
            xml::db_cluster_endpoint(&endpoint),
            &req.request_id,
        ))
    }

    fn modify_db_cluster_endpoint(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let endpoint_id = required_query_param(req, "DBClusterEndpointIdentifier")?;
        let static_members = collect_list(req, "StaticMembers", &["member"]);
        let excluded_members = collect_list(req, "ExcludedMembers", &["member"]);
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let endpoint = st
            .cluster_endpoints
            .get_mut(&endpoint_id)
            .ok_or_else(|| cluster_endpoint_not_found(&endpoint_id))?;
        if let Some(v) = optional_query_param(req, "EndpointType") {
            endpoint.endpoint_type = v;
        }
        if let Some(v) = optional_query_param(req, "CustomEndpointType") {
            endpoint.custom_endpoint_type = v;
        }
        if !static_members.is_empty() {
            endpoint.static_members = static_members;
        }
        if !excluded_members.is_empty() {
            endpoint.excluded_members = excluded_members;
        }
        let endpoint = endpoint.clone();
        Ok(ok_xml(
            "ModifyDBClusterEndpoint",
            xml::db_cluster_endpoint(&endpoint),
            &req.request_id,
        ))
    }

    fn delete_db_cluster_endpoint(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let endpoint_id = required_query_param(req, "DBClusterEndpointIdentifier")?;
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let mut endpoint = st
            .cluster_endpoints
            .remove(&endpoint_id)
            .ok_or_else(|| cluster_endpoint_not_found(&endpoint_id))?;
        endpoint.status = "deleting".to_string();
        Ok(ok_xml(
            "DeleteDBClusterEndpoint",
            xml::db_cluster_endpoint(&endpoint),
            &req.request_id,
        ))
    }

    fn describe_db_cluster_endpoints(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let endpoint_filter = optional_query_param(req, "DBClusterEndpointIdentifier");
        let cluster_filter = optional_query_param(req, "DBClusterIdentifier");
        let accounts = self.state.read();
        let empty = NeptuneState::new(&req.account_id, &req.region);
        let st = accounts.get(&req.account_id).unwrap_or(&empty);
        // `DescribeDBClusterEndpoints` declares only `DBClusterNotFoundFault`;
        // an unknown `DBClusterEndpointIdentifier` filter resolves to an empty
        // list rather than an error (matching the real API).
        let endpoints: Vec<&DbClusterEndpoint> = st
            .cluster_endpoints
            .values()
            .filter(|e| {
                endpoint_filter
                    .as_ref()
                    .is_none_or(|id| &e.db_cluster_endpoint_identifier == id)
                    && cluster_filter
                        .as_ref()
                        .is_none_or(|c| &e.db_cluster_identifier == c)
            })
            .collect();
        let inner: String = endpoints
            .iter()
            .map(|e| {
                format!(
                    "<DBClusterEndpointList>{}</DBClusterEndpointList>",
                    xml::db_cluster_endpoint(e)
                )
            })
            .collect();
        Ok(ok_xml(
            "DescribeDBClusterEndpoints",
            format!("<DBClusterEndpoints>{inner}</DBClusterEndpoints>"),
            &req.request_id,
        ))
    }

    fn create_db_instance(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let id = required_query_param(req, "DBInstanceIdentifier")?;
        let class = required_query_param(req, "DBInstanceClass")?;
        let engine = required_query_param(req, "Engine")?;
        let cluster_id = required_query_param(req, "DBClusterIdentifier")?;
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        if st.instances.contains_key(&id) {
            return Err(db_instance_already_exists(&id));
        }
        let cluster = st
            .clusters
            .get_mut(&cluster_id)
            .ok_or_else(|| db_cluster_not_found(&cluster_id))?;
        let is_writer = !cluster.members.iter().any(|m| m.is_writer);
        let promotion_tier = optional_query_param(req, "PromotionTier")
            .and_then(|v| v.parse().ok())
            .unwrap_or(1);
        cluster.members.push(ClusterMember {
            db_instance_identifier: id.clone(),
            is_writer,
            promotion_tier,
        });
        let cluster_pmw = cluster.preferred_maintenance_window.clone();
        let cluster_subnet = cluster.db_subnet_group.clone();
        let cluster_engine_version = cluster.engine_version.clone();
        // A Neptune instance inherits its cluster's storage encryption; it is a
        // cluster-level property (there is no per-instance StorageEncrypted knob).
        let cluster_storage_encrypted = cluster.storage_encrypted;
        let cluster_kms_key_id = cluster.kms_key_id.clone();
        let az = cluster
            .availability_zones
            .first()
            .cloned()
            .unwrap_or_else(|| format!("{}a", req.region));
        let suffix = endpoint_suffix();
        let instance = DbInstance {
            db_instance_identifier: id.clone(),
            db_instance_arn: format!("arn:aws:rds:{}:{}:db:{id}", req.region, req.account_id),
            db_instance_class: class,
            engine,
            engine_version: optional_query_param(req, "EngineVersion")
                .unwrap_or(cluster_engine_version),
            status: "available".to_string(),
            db_cluster_identifier: cluster_id,
            endpoint_address: format!("{id}.{suffix}.{}.neptune.amazonaws.com", req.region),
            port: DEFAULT_PORT,
            availability_zone: optional_query_param(req, "AvailabilityZone").unwrap_or(az),
            publicly_accessible: optional_query_param(req, "PubliclyAccessible")
                .map(|v| v == "true")
                .unwrap_or(false),
            auto_minor_version_upgrade: optional_query_param(req, "AutoMinorVersionUpgrade")
                .map(|v| v == "true")
                .unwrap_or(true),
            promotion_tier,
            dbi_resource_id: format!("db-{}", resource_token()),
            ca_certificate_identifier: optional_query_param(req, "CACertificateIdentifier")
                .unwrap_or_else(|| "rds-ca-2019".to_string()),
            preferred_backup_window: "07:00-07:30".to_string(),
            preferred_maintenance_window: optional_query_param(req, "PreferredMaintenanceWindow")
                .unwrap_or(cluster_pmw),
            backup_retention_period: 1,
            storage_encrypted: cluster_storage_encrypted,
            kms_key_id: cluster_kms_key_id,
            db_subnet_group: cluster_subnet,
            enabled_cloudwatch_logs_exports: Vec::new(),
            instance_create_time: Utc::now(),
            tags: parse_tags(req),
        };
        st.instances.insert(id, instance.clone());
        Ok(ok_xml(
            "CreateDBInstance",
            format!("<DBInstance>{}</DBInstance>", xml::db_instance(&instance)),
            &req.request_id,
        ))
    }

    fn describe_db_instances(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let filter = optional_query_param(req, "DBInstanceIdentifier");
        let accounts = self.state.read();
        let empty = NeptuneState::new(&req.account_id, &req.region);
        let st = accounts.get(&req.account_id).unwrap_or(&empty);
        let instances: Vec<&DbInstance> = match &filter {
            Some(id) => match st.instances.get(id) {
                Some(i) => vec![i],
                None => return Err(db_instance_not_found(id)),
            },
            None => st.instances.values().collect(),
        };
        let inner: String = instances
            .iter()
            .map(|i| format!("<DBInstance>{}</DBInstance>", xml::db_instance(i)))
            .collect();
        Ok(ok_xml(
            "DescribeDBInstances",
            format!("<DBInstances>{inner}</DBInstances>"),
            &req.request_id,
        ))
    }

    fn modify_db_instance(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let id = required_query_param(req, "DBInstanceIdentifier")?;
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let inst = st
            .instances
            .get_mut(&id)
            .ok_or_else(|| db_instance_not_found(&id))?;
        if let Some(v) = optional_query_param(req, "DBInstanceClass") {
            inst.db_instance_class = v;
        }
        if let Some(v) = optional_query_param(req, "PreferredMaintenanceWindow") {
            inst.preferred_maintenance_window = v;
        }
        if let Some(v) = optional_query_param(req, "PromotionTier").and_then(|v| v.parse().ok()) {
            inst.promotion_tier = v;
        }
        if let Some(v) = optional_query_param(req, "CACertificateIdentifier") {
            inst.ca_certificate_identifier = v;
        }
        if let Some(v) = optional_query_param(req, "AutoMinorVersionUpgrade") {
            inst.auto_minor_version_upgrade = v == "true";
        }
        if let Some(v) = optional_query_param(req, "PubliclyAccessible") {
            inst.publicly_accessible = v == "true";
        }
        if let Some(v) = optional_query_param(req, "EngineVersion") {
            inst.engine_version = v;
        }
        let mut inst = inst.clone();
        if let Some(new_id) =
            optional_query_param(req, "NewDBInstanceIdentifier").filter(|n| n != &id)
        {
            inst.db_instance_identifier = new_id.clone();
            inst.db_instance_arn =
                format!("arn:aws:rds:{}:{}:db:{new_id}", req.region, req.account_id);
            st.instances.remove(&id);
            st.instances.insert(new_id, inst.clone());
        } else {
            st.instances.insert(id, inst.clone());
        }
        Ok(ok_xml(
            "ModifyDBInstance",
            format!("<DBInstance>{}</DBInstance>", xml::db_instance(&inst)),
            &req.request_id,
        ))
    }

    fn delete_db_instance(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let id = required_query_param(req, "DBInstanceIdentifier")?;
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let inst = st
            .instances
            .remove(&id)
            .ok_or_else(|| db_instance_not_found(&id))?;
        if let Some(c) = st.clusters.get_mut(&inst.db_cluster_identifier) {
            c.members.retain(|m| m.db_instance_identifier != id);
        }
        Ok(ok_xml(
            "DeleteDBInstance",
            format!("<DBInstance>{}</DBInstance>", xml::db_instance(&inst)),
            &req.request_id,
        ))
    }

    fn reboot_db_instance(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let id = required_query_param(req, "DBInstanceIdentifier")?;
        let accounts = self.state.read();
        let empty = NeptuneState::new(&req.account_id, &req.region);
        let st = accounts.get(&req.account_id).unwrap_or(&empty);
        let inst = st
            .instances
            .get(&id)
            .ok_or_else(|| db_instance_not_found(&id))?;
        Ok(ok_xml(
            "RebootDBInstance",
            format!("<DBInstance>{}</DBInstance>", xml::db_instance(inst)),
            &req.request_id,
        ))
    }

    fn describe_valid_db_instance_modifications(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let id = required_query_param(req, "DBInstanceIdentifier")?;
        let accounts = self.state.read();
        let empty = NeptuneState::new(&req.account_id, &req.region);
        let st = accounts.get(&req.account_id).unwrap_or(&empty);
        if !st.instances.contains_key(&id) {
            return Err(db_instance_not_found(&id));
        }
        let inner =
            "<ValidDBInstanceModificationsMessage><Storage/></ValidDBInstanceModificationsMessage>"
                .to_string();
        Ok(ok_xml(
            "DescribeValidDBInstanceModifications",
            inner,
            &req.request_id,
        ))
    }

    // --- snapshots ---

    fn create_db_cluster_snapshot(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let snap_id = required_query_param(req, "DBClusterSnapshotIdentifier")?;
        let cluster_id = required_query_param(req, "DBClusterIdentifier")?;
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        if st.cluster_snapshots.contains_key(&snap_id) {
            return Err(snapshot_already_exists(&snap_id));
        }
        let cluster = st
            .clusters
            .get(&cluster_id)
            .ok_or_else(|| db_cluster_not_found(&cluster_id))?;
        let mut snap =
            snapshot_from_cluster(cluster, &snap_id, &req.region, &req.account_id, "manual");
        snap.tags = parse_tags(req);
        st.cluster_snapshots.insert(snap_id, snap.clone());
        Ok(ok_xml(
            "CreateDBClusterSnapshot",
            format!(
                "<DBClusterSnapshot>{}</DBClusterSnapshot>",
                xml::db_cluster_snapshot(&snap)
            ),
            &req.request_id,
        ))
    }

    fn copy_db_cluster_snapshot(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let source = required_query_param(req, "SourceDBClusterSnapshotIdentifier")?;
        let target = required_query_param(req, "TargetDBClusterSnapshotIdentifier")?;
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        if st.cluster_snapshots.contains_key(&target) {
            return Err(snapshot_already_exists(&target));
        }
        let mut snap = st
            .cluster_snapshots
            .get(&source)
            .cloned()
            .ok_or_else(|| snapshot_not_found(&source))?;
        snap.source_db_cluster_snapshot_arn = Some(snap.db_cluster_snapshot_arn.clone());
        snap.db_cluster_snapshot_identifier = target.clone();
        snap.db_cluster_snapshot_arn = format!(
            "arn:aws:rds:{}:{}:cluster-snapshot:{target}",
            req.region, req.account_id
        );
        snap.snapshot_type = "manual".to_string();
        snap.snapshot_create_time = Utc::now();
        snap.tags = parse_tags(req);
        st.cluster_snapshots.insert(target, snap.clone());
        Ok(ok_xml(
            "CopyDBClusterSnapshot",
            format!(
                "<DBClusterSnapshot>{}</DBClusterSnapshot>",
                xml::db_cluster_snapshot(&snap)
            ),
            &req.request_id,
        ))
    }

    fn delete_db_cluster_snapshot(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let id = required_query_param(req, "DBClusterSnapshotIdentifier")?;
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let snap = st
            .cluster_snapshots
            .remove(&id)
            .ok_or_else(|| snapshot_not_found(&id))?;
        Ok(ok_xml(
            "DeleteDBClusterSnapshot",
            format!(
                "<DBClusterSnapshot>{}</DBClusterSnapshot>",
                xml::db_cluster_snapshot(&snap)
            ),
            &req.request_id,
        ))
    }

    fn describe_db_cluster_snapshots(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let snap_filter = optional_query_param(req, "DBClusterSnapshotIdentifier");
        let cluster_filter = optional_query_param(req, "DBClusterIdentifier");
        let accounts = self.state.read();
        let empty = NeptuneState::new(&req.account_id, &req.region);
        let st = accounts.get(&req.account_id).unwrap_or(&empty);
        let snaps: Vec<&DbClusterSnapshot> = if let Some(id) = &snap_filter {
            match st.cluster_snapshots.get(id) {
                Some(s) => vec![s],
                None => return Err(snapshot_not_found(id)),
            }
        } else {
            st.cluster_snapshots
                .values()
                .filter(|s| {
                    cluster_filter
                        .as_ref()
                        .is_none_or(|c| &s.db_cluster_identifier == c)
                })
                .collect()
        };
        let inner: String = snaps
            .iter()
            .map(|s| {
                format!(
                    "<DBClusterSnapshot>{}</DBClusterSnapshot>",
                    xml::db_cluster_snapshot(s)
                )
            })
            .collect();
        Ok(ok_xml(
            "DescribeDBClusterSnapshots",
            format!("<DBClusterSnapshots>{inner}</DBClusterSnapshots>"),
            &req.request_id,
        ))
    }

    fn describe_db_cluster_snapshot_attributes(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let id = required_query_param(req, "DBClusterSnapshotIdentifier")?;
        let accounts = self.state.read();
        let empty = NeptuneState::new(&req.account_id, &req.region);
        let st = accounts.get(&req.account_id).unwrap_or(&empty);
        let snap = st
            .cluster_snapshots
            .get(&id)
            .ok_or_else(|| snapshot_not_found(&id))?;
        let values: String = snap
            .restore_attribute
            .iter()
            .map(|a| {
                format!(
                    "<AttributeValue>{}</AttributeValue>",
                    fakecloud_aws::xml::xml_escape(a)
                )
            })
            .collect();
        let inner = format!(
            "<DBClusterSnapshotAttributesResult><DBClusterSnapshotIdentifier>{}</DBClusterSnapshotIdentifier><DBClusterSnapshotAttributes><DBClusterSnapshotAttribute><AttributeName>restore</AttributeName><AttributeValues>{values}</AttributeValues></DBClusterSnapshotAttribute></DBClusterSnapshotAttributes></DBClusterSnapshotAttributesResult>",
            fakecloud_aws::xml::xml_escape(&id)
        );
        Ok(ok_xml(
            "DescribeDBClusterSnapshotAttributes",
            inner,
            &req.request_id,
        ))
    }

    fn modify_db_cluster_snapshot_attribute(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let id = required_query_param(req, "DBClusterSnapshotIdentifier")?;
        let _attr = required_query_param(req, "AttributeName")?;
        let to_add = collect_list(req, "ValuesToAdd", &["AttributeValue", "member"]);
        let to_remove = collect_list(req, "ValuesToRemove", &["AttributeValue", "member"]);
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let snap = st
            .cluster_snapshots
            .get_mut(&id)
            .ok_or_else(|| snapshot_not_found(&id))?;
        for v in to_add {
            if !snap.restore_attribute.contains(&v) {
                snap.restore_attribute.push(v);
            }
        }
        snap.restore_attribute.retain(|v| !to_remove.contains(v));
        let values: String = snap
            .restore_attribute
            .iter()
            .map(|a| {
                format!(
                    "<AttributeValue>{}</AttributeValue>",
                    fakecloud_aws::xml::xml_escape(a)
                )
            })
            .collect();
        let inner = format!(
            "<DBClusterSnapshotAttributesResult><DBClusterSnapshotIdentifier>{}</DBClusterSnapshotIdentifier><DBClusterSnapshotAttributes><DBClusterSnapshotAttribute><AttributeName>restore</AttributeName><AttributeValues>{values}</AttributeValues></DBClusterSnapshotAttribute></DBClusterSnapshotAttributes></DBClusterSnapshotAttributesResult>",
            fakecloud_aws::xml::xml_escape(&id)
        );
        Ok(ok_xml(
            "ModifyDBClusterSnapshotAttribute",
            inner,
            &req.request_id,
        ))
    }

    fn restore_db_cluster_from_snapshot(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let new_id = required_query_param(req, "DBClusterIdentifier")?;
        let snap_id = required_query_param(req, "SnapshotIdentifier")?;
        let engine = required_query_param(req, "Engine")?;
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        if st.clusters.contains_key(&new_id) {
            return Err(db_cluster_already_exists(&new_id));
        }
        let snap = st
            .cluster_snapshots
            .get(&snap_id)
            .cloned()
            .ok_or_else(|| snapshot_not_found(&snap_id))?;
        let suffix = endpoint_suffix();
        let cluster = DbCluster {
            db_cluster_identifier: new_id.clone(),
            db_cluster_arn: cluster_arn(&req.region, &req.account_id, &new_id),
            db_cluster_resource_id: format!("cluster-{}", resource_token()),
            status: "available".to_string(),
            engine,
            engine_version: snap.engine_version.clone(),
            port: optional_query_param(req, "Port")
                .and_then(|p| p.parse().ok())
                .unwrap_or(snap.port),
            master_username: snap.master_username.clone(),
            endpoint: format!(
                "{new_id}.cluster-{suffix}.{}.neptune.amazonaws.com",
                req.region
            ),
            reader_endpoint: format!(
                "{new_id}.cluster-ro-{suffix}.{}.neptune.amazonaws.com",
                req.region
            ),
            hosted_zone_id: "Z1NEPTUNE0000".to_string(),
            db_subnet_group: optional_query_param(req, "DBSubnetGroupName")
                .unwrap_or_else(|| "default".to_string()),
            db_cluster_parameter_group: format!("default.{DEFAULT_FAMILY}"),
            storage_encrypted: snap.storage_encrypted,
            kms_key_id: snap.kms_key_id.clone(),
            deletion_protection: optional_query_param(req, "DeletionProtection")
                .map(|v| v == "true")
                .unwrap_or(false),
            iam_database_authentication_enabled: optional_query_param(
                req,
                "EnableIAMDatabaseAuthentication",
            )
            .map(|v| v == "true")
            .unwrap_or(false),
            backup_retention_period: 1,
            preferred_backup_window: "07:00-07:30".to_string(),
            preferred_maintenance_window: "sun:08:00-sun:08:30".to_string(),
            storage_type: "standard".to_string(),
            availability_zones: snap.availability_zones.clone(),
            vpc_security_group_ids: collect_list(
                req,
                "VpcSecurityGroupIds",
                &["VpcSecurityGroupId", "member"],
            ),
            enabled_cloudwatch_logs_exports: Vec::new(),
            members: Vec::new(),
            associated_roles: Vec::new(),
            cluster_create_time: Utc::now(),
            tags: parse_tags(req),
        };
        st.clusters.insert(new_id, cluster.clone());
        Ok(ok_xml(
            "RestoreDBClusterFromSnapshot",
            format!("<DBCluster>{}</DBCluster>", xml::db_cluster(&cluster)),
            &req.request_id,
        ))
    }

    fn restore_db_cluster_to_point_in_time(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let new_id = required_query_param(req, "DBClusterIdentifier")?;
        let source_id = required_query_param(req, "SourceDBClusterIdentifier")?;
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        if st.clusters.contains_key(&new_id) {
            return Err(db_cluster_already_exists(&new_id));
        }
        let source = st
            .clusters
            .get(&source_id)
            .cloned()
            .ok_or_else(|| db_cluster_not_found(&source_id))?;
        let suffix = endpoint_suffix();
        let mut cluster = source.clone();
        cluster.db_cluster_identifier = new_id.clone();
        cluster.db_cluster_arn = cluster_arn(&req.region, &req.account_id, &new_id);
        cluster.db_cluster_resource_id = format!("cluster-{}", resource_token());
        cluster.endpoint = format!(
            "{new_id}.cluster-{suffix}.{}.neptune.amazonaws.com",
            req.region
        );
        cluster.reader_endpoint = format!(
            "{new_id}.cluster-ro-{suffix}.{}.neptune.amazonaws.com",
            req.region
        );
        cluster.members = Vec::new();
        cluster.associated_roles = Vec::new();
        cluster.status = "available".to_string();
        cluster.cluster_create_time = Utc::now();
        cluster.tags = parse_tags(req);
        st.clusters.insert(new_id, cluster.clone());
        Ok(ok_xml(
            "RestoreDBClusterToPointInTime",
            format!("<DBCluster>{}</DBCluster>", xml::db_cluster(&cluster)),
            &req.request_id,
        ))
    }

    // --- cluster parameter groups ---

    fn create_db_cluster_parameter_group(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = required_query_param(req, "DBClusterParameterGroupName")?;
        let family = required_query_param(req, "DBParameterGroupFamily")?;
        let description = required_query_param(req, "Description")?;
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        if st.cluster_parameter_groups.contains_key(&name) {
            return Err(param_group_already_exists(&name));
        }
        let group = DbClusterParameterGroup {
            db_cluster_parameter_group_name: name.clone(),
            db_cluster_parameter_group_arn: format!(
                "arn:aws:rds:{}:{}:cluster-pg:{name}",
                req.region, req.account_id
            ),
            db_parameter_group_family: family,
            description,
            parameters: Default::default(),
            tags: parse_tags(req),
        };
        st.cluster_parameter_groups.insert(name, group.clone());
        Ok(ok_xml(
            "CreateDBClusterParameterGroup",
            format!(
                "<DBClusterParameterGroup>{}</DBClusterParameterGroup>",
                xml::db_cluster_parameter_group(&group)
            ),
            &req.request_id,
        ))
    }

    fn copy_db_cluster_parameter_group(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let source = required_query_param(req, "SourceDBClusterParameterGroupIdentifier")?;
        let target = required_query_param(req, "TargetDBClusterParameterGroupIdentifier")?;
        let target_desc = required_query_param(req, "TargetDBClusterParameterGroupDescription")?;
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        if st.cluster_parameter_groups.contains_key(&target) {
            return Err(param_group_already_exists(&target));
        }
        let mut group = st
            .cluster_parameter_groups
            .get(&source)
            .cloned()
            .ok_or_else(|| param_group_not_found(&source))?;
        group.db_cluster_parameter_group_name = target.clone();
        group.db_cluster_parameter_group_arn = format!(
            "arn:aws:rds:{}:{}:cluster-pg:{target}",
            req.region, req.account_id
        );
        group.description = target_desc;
        group.tags = parse_tags(req);
        st.cluster_parameter_groups.insert(target, group.clone());
        Ok(ok_xml(
            "CopyDBClusterParameterGroup",
            format!(
                "<DBClusterParameterGroup>{}</DBClusterParameterGroup>",
                xml::db_cluster_parameter_group(&group)
            ),
            &req.request_id,
        ))
    }

    fn delete_db_cluster_parameter_group(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = required_query_param(req, "DBClusterParameterGroupName")?;
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        st.cluster_parameter_groups
            .remove(&name)
            .ok_or_else(|| param_group_not_found(&name))?;
        Ok(ok_xml(
            "DeleteDBClusterParameterGroup",
            String::new(),
            &req.request_id,
        ))
    }

    fn describe_db_cluster_parameter_groups(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let filter = optional_query_param(req, "DBClusterParameterGroupName");
        let accounts = self.state.read();
        let empty = NeptuneState::new(&req.account_id, &req.region);
        let st = accounts.get(&req.account_id).unwrap_or(&empty);
        let groups: Vec<&DbClusterParameterGroup> = match &filter {
            Some(name) => match st.cluster_parameter_groups.get(name) {
                Some(g) => vec![g],
                None => return Err(param_group_not_found(name)),
            },
            None => st.cluster_parameter_groups.values().collect(),
        };
        let inner: String = groups
            .iter()
            .map(|g| {
                format!(
                    "<DBClusterParameterGroup>{}</DBClusterParameterGroup>",
                    xml::db_cluster_parameter_group(g)
                )
            })
            .collect();
        Ok(ok_xml(
            "DescribeDBClusterParameterGroups",
            format!("<DBClusterParameterGroups>{inner}</DBClusterParameterGroups>"),
            &req.request_id,
        ))
    }

    fn describe_db_cluster_parameters(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = required_query_param(req, "DBClusterParameterGroupName")?;
        let accounts = self.state.read();
        let empty = NeptuneState::new(&req.account_id, &req.region);
        let st = accounts.get(&req.account_id).unwrap_or(&empty);
        let group = st
            .cluster_parameter_groups
            .get(&name)
            .ok_or_else(|| param_group_not_found(&name))?;
        let user_params: String = group
            .parameters
            .iter()
            .map(|(n, p)| xml::parameter(n, p))
            .collect();
        let inner = format!(
            "<Parameters>{}{user_params}</Parameters>",
            default_parameters_xml()
        );
        Ok(ok_xml(
            "DescribeDBClusterParameters",
            inner,
            &req.request_id,
        ))
    }

    fn modify_db_cluster_parameter_group(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = required_query_param(req, "DBClusterParameterGroupName")?;
        let params = parse_parameters(req);
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let group = st
            .cluster_parameter_groups
            .get_mut(&name)
            .ok_or_else(|| param_group_not_found(&name))?;
        for (n, v, apply) in params {
            group.parameters.insert(
                n,
                ParameterValue {
                    value: v,
                    apply_method: apply,
                },
            );
        }
        Ok(ok_xml(
            "ModifyDBClusterParameterGroup",
            format!(
                "<DBClusterParameterGroupName>{}</DBClusterParameterGroupName>",
                fakecloud_aws::xml::xml_escape(&name)
            ),
            &req.request_id,
        ))
    }

    fn reset_db_cluster_parameter_group(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = required_query_param(req, "DBClusterParameterGroupName")?;
        let reset_all = optional_query_param(req, "ResetAllParameters")
            .map(|v| v == "true")
            .unwrap_or(false);
        let names: Vec<String> = parse_parameters(req)
            .into_iter()
            .map(|(n, _, _)| n)
            .collect();
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let group = st
            .cluster_parameter_groups
            .get_mut(&name)
            .ok_or_else(|| param_group_not_found(&name))?;
        if reset_all {
            group.parameters.clear();
        } else {
            for n in names {
                group.parameters.remove(&n);
            }
        }
        Ok(ok_xml(
            "ResetDBClusterParameterGroup",
            format!(
                "<DBClusterParameterGroupName>{}</DBClusterParameterGroupName>",
                fakecloud_aws::xml::xml_escape(&name)
            ),
            &req.request_id,
        ))
    }

    // --- db (instance) parameter groups ---

    fn create_db_parameter_group(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let name = required_query_param(req, "DBParameterGroupName")?;
        let family = required_query_param(req, "DBParameterGroupFamily")?;
        let description = required_query_param(req, "Description")?;
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        if st.parameter_groups.contains_key(&name) {
            return Err(param_group_already_exists(&name));
        }
        let group = DbParameterGroup {
            db_parameter_group_name: name.clone(),
            db_parameter_group_arn: format!(
                "arn:aws:rds:{}:{}:pg:{name}",
                req.region, req.account_id
            ),
            db_parameter_group_family: family,
            description,
            parameters: Default::default(),
            tags: parse_tags(req),
        };
        st.parameter_groups.insert(name, group.clone());
        Ok(ok_xml(
            "CreateDBParameterGroup",
            format!(
                "<DBParameterGroup>{}</DBParameterGroup>",
                xml::db_parameter_group(&group)
            ),
            &req.request_id,
        ))
    }

    fn copy_db_parameter_group(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let source = required_query_param(req, "SourceDBParameterGroupIdentifier")?;
        let target = required_query_param(req, "TargetDBParameterGroupIdentifier")?;
        let target_desc = required_query_param(req, "TargetDBParameterGroupDescription")?;
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        if st.parameter_groups.contains_key(&target) {
            return Err(param_group_already_exists(&target));
        }
        let mut group = st
            .parameter_groups
            .get(&source)
            .cloned()
            .ok_or_else(|| param_group_not_found(&source))?;
        group.db_parameter_group_name = target.clone();
        group.db_parameter_group_arn =
            format!("arn:aws:rds:{}:{}:pg:{target}", req.region, req.account_id);
        group.description = target_desc;
        group.tags = parse_tags(req);
        st.parameter_groups.insert(target, group.clone());
        Ok(ok_xml(
            "CopyDBParameterGroup",
            format!(
                "<DBParameterGroup>{}</DBParameterGroup>",
                xml::db_parameter_group(&group)
            ),
            &req.request_id,
        ))
    }

    fn delete_db_parameter_group(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let name = required_query_param(req, "DBParameterGroupName")?;
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        st.parameter_groups
            .remove(&name)
            .ok_or_else(|| param_group_not_found(&name))?;
        Ok(ok_xml(
            "DeleteDBParameterGroup",
            String::new(),
            &req.request_id,
        ))
    }

    fn describe_db_parameter_groups(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let filter = optional_query_param(req, "DBParameterGroupName");
        let accounts = self.state.read();
        let empty = NeptuneState::new(&req.account_id, &req.region);
        let st = accounts.get(&req.account_id).unwrap_or(&empty);
        let groups: Vec<&DbParameterGroup> = match &filter {
            Some(name) => match st.parameter_groups.get(name) {
                Some(g) => vec![g],
                None => return Err(param_group_not_found(name)),
            },
            None => st.parameter_groups.values().collect(),
        };
        let inner: String = groups
            .iter()
            .map(|g| {
                format!(
                    "<DBParameterGroup>{}</DBParameterGroup>",
                    xml::db_parameter_group(g)
                )
            })
            .collect();
        Ok(ok_xml(
            "DescribeDBParameterGroups",
            format!("<DBParameterGroups>{inner}</DBParameterGroups>"),
            &req.request_id,
        ))
    }

    fn describe_db_parameters(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let name = required_query_param(req, "DBParameterGroupName")?;
        let accounts = self.state.read();
        let empty = NeptuneState::new(&req.account_id, &req.region);
        let st = accounts.get(&req.account_id).unwrap_or(&empty);
        let group = st
            .parameter_groups
            .get(&name)
            .ok_or_else(|| param_group_not_found(&name))?;
        let user_params: String = group
            .parameters
            .iter()
            .map(|(n, p)| xml::parameter(n, p))
            .collect();
        let inner = format!(
            "<Parameters>{}{user_params}</Parameters>",
            default_parameters_xml()
        );
        Ok(ok_xml("DescribeDBParameters", inner, &req.request_id))
    }

    fn modify_db_parameter_group(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let name = required_query_param(req, "DBParameterGroupName")?;
        let params = parse_parameters(req);
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let group = st
            .parameter_groups
            .get_mut(&name)
            .ok_or_else(|| param_group_not_found(&name))?;
        for (n, v, apply) in params {
            group.parameters.insert(
                n,
                ParameterValue {
                    value: v,
                    apply_method: apply,
                },
            );
        }
        Ok(ok_xml(
            "ModifyDBParameterGroup",
            format!(
                "<DBParameterGroupName>{}</DBParameterGroupName>",
                fakecloud_aws::xml::xml_escape(&name)
            ),
            &req.request_id,
        ))
    }

    fn reset_db_parameter_group(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let name = required_query_param(req, "DBParameterGroupName")?;
        let reset_all = optional_query_param(req, "ResetAllParameters")
            .map(|v| v == "true")
            .unwrap_or(false);
        let names: Vec<String> = parse_parameters(req)
            .into_iter()
            .map(|(n, _, _)| n)
            .collect();
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let group = st
            .parameter_groups
            .get_mut(&name)
            .ok_or_else(|| param_group_not_found(&name))?;
        if reset_all {
            group.parameters.clear();
        } else {
            for n in names {
                group.parameters.remove(&n);
            }
        }
        Ok(ok_xml(
            "ResetDBParameterGroup",
            format!(
                "<DBParameterGroupName>{}</DBParameterGroupName>",
                fakecloud_aws::xml::xml_escape(&name)
            ),
            &req.request_id,
        ))
    }

    // --- subnet groups ---

    fn create_db_subnet_group(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let name = required_query_param(req, "DBSubnetGroupName")?;
        let description = required_query_param(req, "DBSubnetGroupDescription")?;
        let subnet_ids = collect_list(req, "SubnetIds", &["SubnetIdentifier", "member"]);
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        if st.subnet_groups.contains_key(&name) {
            return Err(subnet_group_already_exists(&name));
        }
        let subnets = subnet_ids
            .iter()
            .enumerate()
            .map(|(i, id)| Subnet {
                subnet_identifier: id.clone(),
                availability_zone: format!("{}{}", req.region, (b'a' + (i % 3) as u8) as char),
                status: "Active".to_string(),
            })
            .collect();
        let group = DbSubnetGroup {
            db_subnet_group_name: name.clone(),
            db_subnet_group_arn: format!(
                "arn:aws:rds:{}:{}:subgrp:{name}",
                req.region, req.account_id
            ),
            db_subnet_group_description: description,
            vpc_id: format!("vpc-{}", resource_token()[..12].to_lowercase()),
            subnet_group_status: "Complete".to_string(),
            subnets,
            tags: parse_tags(req),
        };
        st.subnet_groups.insert(name, group.clone());
        Ok(ok_xml(
            "CreateDBSubnetGroup",
            format!(
                "<DBSubnetGroup>{}</DBSubnetGroup>",
                xml::db_subnet_group(&group)
            ),
            &req.request_id,
        ))
    }

    fn modify_db_subnet_group(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let name = required_query_param(req, "DBSubnetGroupName")?;
        let subnet_ids = collect_list(req, "SubnetIds", &["SubnetIdentifier", "member"]);
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let group = st
            .subnet_groups
            .get_mut(&name)
            .ok_or_else(|| subnet_group_not_found(&name))?;
        if let Some(v) = optional_query_param(req, "DBSubnetGroupDescription") {
            group.db_subnet_group_description = v;
        }
        if !subnet_ids.is_empty() {
            group.subnets = subnet_ids
                .iter()
                .enumerate()
                .map(|(i, id)| Subnet {
                    subnet_identifier: id.clone(),
                    availability_zone: format!("{}{}", req.region, (b'a' + (i % 3) as u8) as char),
                    status: "Active".to_string(),
                })
                .collect();
        }
        let group = group.clone();
        Ok(ok_xml(
            "ModifyDBSubnetGroup",
            format!(
                "<DBSubnetGroup>{}</DBSubnetGroup>",
                xml::db_subnet_group(&group)
            ),
            &req.request_id,
        ))
    }

    fn delete_db_subnet_group(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let name = required_query_param(req, "DBSubnetGroupName")?;
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        st.subnet_groups
            .remove(&name)
            .ok_or_else(|| subnet_group_not_found(&name))?;
        Ok(ok_xml(
            "DeleteDBSubnetGroup",
            String::new(),
            &req.request_id,
        ))
    }

    fn describe_db_subnet_groups(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let filter = optional_query_param(req, "DBSubnetGroupName");
        let accounts = self.state.read();
        let empty = NeptuneState::new(&req.account_id, &req.region);
        let st = accounts.get(&req.account_id).unwrap_or(&empty);
        let groups: Vec<&DbSubnetGroup> = match &filter {
            Some(name) => match st.subnet_groups.get(name) {
                Some(g) => vec![g],
                None => return Err(subnet_group_not_found(name)),
            },
            None => st.subnet_groups.values().collect(),
        };
        let inner: String = groups
            .iter()
            .map(|g| format!("<DBSubnetGroup>{}</DBSubnetGroup>", xml::db_subnet_group(g)))
            .collect();
        Ok(ok_xml(
            "DescribeDBSubnetGroups",
            format!("<DBSubnetGroups>{inner}</DBSubnetGroups>"),
            &req.request_id,
        ))
    }

    // --- global clusters ---

    fn create_global_cluster(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let id = required_query_param(req, "GlobalClusterIdentifier")?;
        if id.len() > 255 {
            return Err(invalid_parameter_value(
                "GlobalClusterIdentifier exceeds the 255 character limit.",
            ));
        }
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        if st.global_clusters.contains_key(&id) {
            return Err(global_cluster_already_exists(&id));
        }
        // Optionally seed from an existing source cluster.
        let source = optional_query_param(req, "SourceDBClusterIdentifier");
        let (engine, engine_version) = match &source {
            Some(arn) => {
                let src_id = arn.rsplit(':').next().unwrap_or(arn);
                st.clusters
                    .get(src_id)
                    .map(|c| (c.engine.clone(), c.engine_version.clone()))
                    .unwrap_or_else(|| {
                        (
                            DEFAULT_ENGINE.to_string(),
                            DEFAULT_ENGINE_VERSION.to_string(),
                        )
                    })
            }
            None => (
                optional_query_param(req, "Engine").unwrap_or_else(|| DEFAULT_ENGINE.to_string()),
                optional_query_param(req, "EngineVersion")
                    .unwrap_or_else(|| DEFAULT_ENGINE_VERSION.to_string()),
            ),
        };
        let global = GlobalCluster {
            global_cluster_identifier: id.clone(),
            global_cluster_arn: format!("arn:aws:rds::{}:global-cluster:{id}", req.account_id),
            global_cluster_resource_id: format!("cluster-{}", resource_token()),
            status: "available".to_string(),
            engine,
            engine_version,
            database_name: optional_query_param(req, "DatabaseName"),
            storage_encrypted: optional_query_param(req, "StorageEncrypted")
                .map(|v| v == "true")
                .unwrap_or(false),
            deletion_protection: optional_query_param(req, "DeletionProtection")
                .map(|v| v == "true")
                .unwrap_or(false),
            members: Vec::new(),
            tags: parse_tags(req),
        };
        st.global_clusters.insert(id, global.clone());
        Ok(ok_xml(
            "CreateGlobalCluster",
            format!(
                "<GlobalCluster>{}</GlobalCluster>",
                xml::global_cluster(&global)
            ),
            &req.request_id,
        ))
    }

    fn modify_global_cluster(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let id = required_query_param(req, "GlobalClusterIdentifier")?;
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let global = st
            .global_clusters
            .get_mut(&id)
            .ok_or_else(|| global_cluster_not_found(&id))?;
        if let Some(v) = optional_query_param(req, "DeletionProtection") {
            global.deletion_protection = v == "true";
        }
        if let Some(v) = optional_query_param(req, "EngineVersion") {
            global.engine_version = v;
        }
        let mut global = global.clone();
        if let Some(new_id) =
            optional_query_param(req, "NewGlobalClusterIdentifier").filter(|n| n != &id)
        {
            global.global_cluster_identifier = new_id.clone();
            global.global_cluster_arn =
                format!("arn:aws:rds::{}:global-cluster:{new_id}", req.account_id);
            st.global_clusters.remove(&id);
            st.global_clusters.insert(new_id, global.clone());
        } else {
            st.global_clusters.insert(id, global.clone());
        }
        Ok(ok_xml(
            "ModifyGlobalCluster",
            format!(
                "<GlobalCluster>{}</GlobalCluster>",
                xml::global_cluster(&global)
            ),
            &req.request_id,
        ))
    }

    fn delete_global_cluster(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let id = required_query_param(req, "GlobalClusterIdentifier")?;
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let global = st
            .global_clusters
            .remove(&id)
            .ok_or_else(|| global_cluster_not_found(&id))?;
        Ok(ok_xml(
            "DeleteGlobalCluster",
            format!(
                "<GlobalCluster>{}</GlobalCluster>",
                xml::global_cluster(&global)
            ),
            &req.request_id,
        ))
    }

    fn describe_global_clusters(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        // A present-but-empty `GlobalClusterIdentifier` is an invalid filter
        // (the member's modelled length minimum is 1): AWS resolves it to no
        // cluster and returns GlobalClusterNotFoundFault, so honour the raw
        // presence rather than collapsing empty to "no filter".
        let filter = req.query_params.get("GlobalClusterIdentifier").cloned();
        let accounts = self.state.read();
        let empty = NeptuneState::new(&req.account_id, &req.region);
        let st = accounts.get(&req.account_id).unwrap_or(&empty);
        let globals: Vec<&GlobalCluster> = match &filter {
            Some(id) => match st.global_clusters.get(id) {
                Some(g) => vec![g],
                None => return Err(global_cluster_not_found(id)),
            },
            None => st.global_clusters.values().collect(),
        };
        let inner: String = globals
            .iter()
            .map(|g| format!("<GlobalCluster>{}</GlobalCluster>", xml::global_cluster(g)))
            .collect();
        Ok(ok_xml(
            "DescribeGlobalClusters",
            format!("<GlobalClusters>{inner}</GlobalClusters>"),
            &req.request_id,
        ))
    }

    fn remove_from_global_cluster(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let id = required_query_param(req, "GlobalClusterIdentifier")?;
        let db_cluster = required_query_param(req, "DbClusterIdentifier")?;
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let global = st
            .global_clusters
            .get_mut(&id)
            .ok_or_else(|| global_cluster_not_found(&id))?;
        global.members.retain(|m| m.db_cluster_arn != db_cluster);
        let global = global.clone();
        Ok(ok_xml(
            "RemoveFromGlobalCluster",
            format!(
                "<GlobalCluster>{}</GlobalCluster>",
                xml::global_cluster(&global)
            ),
            &req.request_id,
        ))
    }

    fn failover_global_cluster(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let id = required_query_param(req, "GlobalClusterIdentifier")?;
        let target = required_query_param(req, "TargetDbClusterIdentifier")?;
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let global = st
            .global_clusters
            .get_mut(&id)
            .ok_or_else(|| global_cluster_not_found(&id))?;
        for m in &mut global.members {
            m.is_writer = m.db_cluster_arn == target;
        }
        let global = global.clone();
        Ok(ok_xml(
            "FailoverGlobalCluster",
            format!(
                "<GlobalCluster>{}</GlobalCluster>",
                xml::global_cluster(&global)
            ),
            &req.request_id,
        ))
    }

    fn switchover_global_cluster(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let id = required_query_param(req, "GlobalClusterIdentifier")?;
        let target = required_query_param(req, "TargetDbClusterIdentifier")?;
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let global = st
            .global_clusters
            .get_mut(&id)
            .ok_or_else(|| global_cluster_not_found(&id))?;
        for m in &mut global.members {
            m.is_writer = m.db_cluster_arn == target;
        }
        let global = global.clone();
        Ok(ok_xml(
            "SwitchoverGlobalCluster",
            format!(
                "<GlobalCluster>{}</GlobalCluster>",
                xml::global_cluster(&global)
            ),
            &req.request_id,
        ))
    }

    // --- event subscriptions ---

    fn create_event_subscription(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let name = required_query_param(req, "SubscriptionName")?;
        let topic = required_query_param(req, "SnsTopicArn")?;
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        if st.event_subscriptions.contains_key(&name) {
            return Err(subscription_already_exists(&name));
        }
        let sub = EventSubscription {
            subscription_name: name.clone(),
            event_subscription_arn: format!(
                "arn:aws:rds:{}:{}:es:{name}",
                req.region, req.account_id
            ),
            customer_aws_id: req.account_id.clone(),
            sns_topic_arn: topic,
            status: "active".to_string(),
            subscription_creation_time: xml::ts(&Utc::now()),
            source_type: optional_query_param(req, "SourceType"),
            source_ids: collect_list(req, "SourceIds", &["SourceId", "member"]),
            event_categories: collect_list(req, "EventCategories", &["EventCategory", "member"]),
            enabled: optional_query_param(req, "Enabled")
                .map(|v| v == "true")
                .unwrap_or(true),
            tags: parse_tags(req),
        };
        st.event_subscriptions.insert(name, sub.clone());
        Ok(ok_xml(
            "CreateEventSubscription",
            format!(
                "<EventSubscription>{}</EventSubscription>",
                xml::event_subscription(&sub)
            ),
            &req.request_id,
        ))
    }

    fn modify_event_subscription(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let name = required_query_param(req, "SubscriptionName")?;
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let sub = st
            .event_subscriptions
            .get_mut(&name)
            .ok_or_else(|| subscription_not_found(&name))?;
        if let Some(v) = optional_query_param(req, "SnsTopicArn") {
            sub.sns_topic_arn = v;
        }
        if let Some(v) = optional_query_param(req, "SourceType") {
            sub.source_type = Some(v);
        }
        if let Some(v) = optional_query_param(req, "Enabled") {
            sub.enabled = v == "true";
        }
        let categories = collect_list(req, "EventCategories", &["EventCategory", "member"]);
        if !categories.is_empty() {
            sub.event_categories = categories;
        }
        let sub = sub.clone();
        Ok(ok_xml(
            "ModifyEventSubscription",
            format!(
                "<EventSubscription>{}</EventSubscription>",
                xml::event_subscription(&sub)
            ),
            &req.request_id,
        ))
    }

    fn delete_event_subscription(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let name = required_query_param(req, "SubscriptionName")?;
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let sub = st
            .event_subscriptions
            .remove(&name)
            .ok_or_else(|| subscription_not_found(&name))?;
        Ok(ok_xml(
            "DeleteEventSubscription",
            format!(
                "<EventSubscription>{}</EventSubscription>",
                xml::event_subscription(&sub)
            ),
            &req.request_id,
        ))
    }

    fn describe_event_subscriptions(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let filter = optional_query_param(req, "SubscriptionName");
        let accounts = self.state.read();
        let empty = NeptuneState::new(&req.account_id, &req.region);
        let st = accounts.get(&req.account_id).unwrap_or(&empty);
        let subs: Vec<&EventSubscription> = match &filter {
            Some(name) => match st.event_subscriptions.get(name) {
                Some(s) => vec![s],
                None => return Err(subscription_not_found(name)),
            },
            None => st.event_subscriptions.values().collect(),
        };
        let inner: String = subs
            .iter()
            .map(|s| {
                format!(
                    "<EventSubscription>{}</EventSubscription>",
                    xml::event_subscription(s)
                )
            })
            .collect();
        Ok(ok_xml(
            "DescribeEventSubscriptions",
            format!("<EventSubscriptionsList>{inner}</EventSubscriptionsList>"),
            &req.request_id,
        ))
    }

    fn add_source_identifier_to_subscription(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = required_query_param(req, "SubscriptionName")?;
        let source = required_query_param(req, "SourceIdentifier")?;
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let sub = st
            .event_subscriptions
            .get_mut(&name)
            .ok_or_else(|| subscription_not_found(&name))?;
        if !sub.source_ids.contains(&source) {
            sub.source_ids.push(source);
        }
        let sub = sub.clone();
        Ok(ok_xml(
            "AddSourceIdentifierToSubscription",
            format!(
                "<EventSubscription>{}</EventSubscription>",
                xml::event_subscription(&sub)
            ),
            &req.request_id,
        ))
    }

    fn remove_source_identifier_from_subscription(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = required_query_param(req, "SubscriptionName")?;
        let source = required_query_param(req, "SourceIdentifier")?;
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let sub = st
            .event_subscriptions
            .get_mut(&name)
            .ok_or_else(|| subscription_not_found(&name))?;
        sub.source_ids.retain(|s| s != &source);
        let sub = sub.clone();
        Ok(ok_xml(
            "RemoveSourceIdentifierFromSubscription",
            format!(
                "<EventSubscription>{}</EventSubscription>",
                xml::event_subscription(&sub)
            ),
            &req.request_id,
        ))
    }

    // --- tagging ---

    fn add_tags_to_resource(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let arn = required_query_param(req, "ResourceName")?;
        let tags = parse_tags(req);
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        if let Some(target) = resource_tags_mut(st, &arn) {
            for tag in tags {
                target.retain(|t| t.key != tag.key);
                target.push(tag);
            }
        }
        Ok(ok_xml("AddTagsToResource", String::new(), &req.request_id))
    }

    fn remove_tags_from_resource(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let arn = required_query_param(req, "ResourceName")?;
        let keys = collect_list(req, "TagKeys", &["member", "TagKey"]);
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        if let Some(target) = resource_tags_mut(st, &arn) {
            target.retain(|t| !keys.contains(&t.key));
        }
        Ok(ok_xml(
            "RemoveTagsFromResource",
            String::new(),
            &req.request_id,
        ))
    }

    fn list_tags_for_resource(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let arn = required_query_param(req, "ResourceName")?;
        let accounts = self.state.read();
        let empty = NeptuneState::new(&req.account_id, &req.region);
        let st = accounts.get(&req.account_id).unwrap_or(&empty);
        let tags = resource_tags(st, &arn).unwrap_or_default();
        Ok(ok_xml(
            "ListTagsForResource",
            xml::tag_list(&tags),
            &req.request_id,
        ))
    }

    // --- read-only catalog ops ---

    fn describe_db_engine_versions(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        // Newer majors are APPENDED (not prepended) so `DescribeDBEngineVersions`
        // with `DefaultOnly=true` (which returns the first entry) keeps resolving
        // to the established default rather than a brand-new major — see the RDS
        // engine-major lesson.
        let versions = [
            ("1.2.1.0", "neptune1.2"),
            ("1.3.0.0", "neptune1.3"),
            ("1.3.2.1", "neptune1.3"),
            ("1.4.1.0", "neptune1.4"),
            ("1.4.2.0", "neptune1.4"),
            ("1.4.5.0", "neptune1.4"),
        ];
        let inner: String = versions
            .iter()
            .map(|(v, fam)| {
                format!(
                    "<DBEngineVersion><Engine>neptune</Engine><EngineVersion>{v}</EngineVersion><DBParameterGroupFamily>{fam}</DBParameterGroupFamily><DBEngineDescription>Amazon Neptune</DBEngineDescription><DBEngineVersionDescription>Neptune {v}</DBEngineVersionDescription><ValidUpgradeTarget/><ExportableLogTypes><member>audit</member></ExportableLogTypes><SupportsLogExportsToCloudwatchLogs>true</SupportsLogExportsToCloudwatchLogs></DBEngineVersion>"
                )
            })
            .collect();
        Ok(ok_xml(
            "DescribeDBEngineVersions",
            format!("<DBEngineVersions>{inner}</DBEngineVersions>"),
            &req.request_id,
        ))
    }

    fn describe_orderable_db_instance_options(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let _engine = required_query_param(req, "Engine")?;
        let ev = optional_query_param(req, "EngineVersion")
            .unwrap_or_else(|| DEFAULT_ENGINE_VERSION.to_string());
        let classes = [
            "db.t3.medium",
            "db.t4g.medium",
            "db.r5.large",
            "db.r5.xlarge",
            "db.r6g.large",
            "db.r6g.xlarge",
        ];
        let azs = [
            format!("{}a", req.region),
            format!("{}b", req.region),
            format!("{}c", req.region),
        ];
        let az_xml: String = azs
            .iter()
            .map(|az| format!("<AvailabilityZone><Name>{az}</Name></AvailabilityZone>"))
            .collect();
        let inner: String = classes
            .iter()
            .map(|class| {
                format!(
                    "<OrderableDBInstanceOption><Engine>neptune</Engine><EngineVersion>{ev}</EngineVersion><DBInstanceClass>{class}</DBInstanceClass><LicenseModel>amazon-license</LicenseModel><Vpc>true</Vpc><AvailabilityZones>{az_xml}</AvailabilityZones></OrderableDBInstanceOption>"
                )
            })
            .collect();
        Ok(ok_xml(
            "DescribeOrderableDBInstanceOptions",
            format!("<OrderableDBInstanceOptions>{inner}</OrderableDBInstanceOptions>"),
            &req.request_id,
        ))
    }

    fn describe_event_categories(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let map = |st: &str, cats: &[&str]| {
            let c: String = cats
                .iter()
                .map(|x| format!("<EventCategory>{x}</EventCategory>"))
                .collect();
            format!(
                "<EventCategoriesMap><SourceType>{st}</SourceType><EventCategories>{c}</EventCategories></EventCategoriesMap>"
            )
        };
        let inner = format!(
            "<EventCategoriesMapList>{}{}{}</EventCategoriesMapList>",
            map(
                "db-cluster",
                &["failover", "failure", "notification", "maintenance"]
            ),
            map(
                "db-instance",
                &[
                    "availability",
                    "configuration change",
                    "creation",
                    "deletion",
                    "failover",
                    "failure",
                    "maintenance",
                    "notification",
                    "recovery"
                ]
            ),
            map("db-cluster-snapshot", &["backup", "notification"]),
        );
        Ok(ok_xml("DescribeEventCategories", inner, &req.request_id))
    }

    fn describe_events(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        // The `SourceType` filter is a modelled enum; reject invalid values.
        if let Some(st) = optional_query_param(req, "SourceType") {
            const VALID: &[&str] = &[
                "db-instance",
                "db-parameter-group",
                "db-security-group",
                "db-snapshot",
                "db-cluster",
                "db-cluster-snapshot",
            ];
            if !VALID.contains(&st.as_str()) {
                return Err(invalid_parameter_value(format!(
                    "Invalid source type: {st}"
                )));
            }
        }
        Ok(ok_xml(
            "DescribeEvents",
            "<Events/>".to_string(),
            &req.request_id,
        ))
    }

    fn describe_engine_default_cluster_parameters(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let family = required_query_param(req, "DBParameterGroupFamily")?;
        let inner = format!(
            "<EngineDefaults><DBParameterGroupFamily>{}</DBParameterGroupFamily><Parameters>{}</Parameters></EngineDefaults>",
            fakecloud_aws::xml::xml_escape(&family),
            default_parameters_xml()
        );
        Ok(ok_xml(
            "DescribeEngineDefaultClusterParameters",
            inner,
            &req.request_id,
        ))
    }

    fn describe_engine_default_parameters(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let family = required_query_param(req, "DBParameterGroupFamily")?;
        let inner = format!(
            "<EngineDefaults><DBParameterGroupFamily>{}</DBParameterGroupFamily><Parameters>{}</Parameters></EngineDefaults>",
            fakecloud_aws::xml::xml_escape(&family),
            default_parameters_xml()
        );
        Ok(ok_xml(
            "DescribeEngineDefaultParameters",
            inner,
            &req.request_id,
        ))
    }

    fn apply_pending_maintenance_action(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let resource = required_query_param(req, "ResourceIdentifier")?;
        let inner = format!(
            "<ResourcePendingMaintenanceActions><ResourceIdentifier>{}</ResourceIdentifier><PendingMaintenanceActionDetails/></ResourcePendingMaintenanceActions>",
            fakecloud_aws::xml::xml_escape(&resource)
        );
        Ok(ok_xml(
            "ApplyPendingMaintenanceAction",
            inner,
            &req.request_id,
        ))
    }

    fn describe_pending_maintenance_actions(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        Ok(ok_xml(
            "DescribePendingMaintenanceActions",
            "<PendingMaintenanceActions/>".to_string(),
            &req.request_id,
        ))
    }
}

// ---------------------------------------------------------------------------
// Free helpers used by handlers
// ---------------------------------------------------------------------------

fn snapshot_from_cluster(
    c: &DbCluster,
    snap_id: &str,
    region: &str,
    account: &str,
    snapshot_type: &str,
) -> DbClusterSnapshot {
    DbClusterSnapshot {
        db_cluster_snapshot_identifier: snap_id.to_string(),
        db_cluster_snapshot_arn: format!(
            "arn:aws:rds:{region}:{account}:cluster-snapshot:{snap_id}"
        ),
        db_cluster_identifier: c.db_cluster_identifier.clone(),
        status: "available".to_string(),
        engine: c.engine.clone(),
        engine_version: c.engine_version.clone(),
        port: c.port,
        master_username: c.master_username.clone(),
        snapshot_type: snapshot_type.to_string(),
        storage_encrypted: c.storage_encrypted,
        kms_key_id: c.kms_key_id.clone(),
        percent_progress: 100,
        vpc_id: String::new(),
        storage_type: c.storage_type.clone(),
        availability_zones: c.availability_zones.clone(),
        cluster_create_time: c.cluster_create_time,
        snapshot_create_time: Utc::now(),
        source_db_cluster_snapshot_arn: None,
        restore_attribute: Vec::new(),
        tags: Vec::new(),
    }
}

/// Parse `Parameters.Parameter.N.{ParameterName,ParameterValue,ApplyMethod}`
/// (and the generic `.member.N` form) into `(name, value, apply_method)`.
fn parse_parameters(req: &AwsRequest) -> Vec<(String, String, String)> {
    let mut out = Vec::new();
    for member in ["Parameter", "member"] {
        let mut idx = 1;
        loop {
            let name =
                optional_query_param(req, &format!("Parameters.{member}.{idx}.ParameterName"));
            let Some(name) = name else { break };
            let value =
                optional_query_param(req, &format!("Parameters.{member}.{idx}.ParameterValue"))
                    .unwrap_or_default();
            let apply =
                optional_query_param(req, &format!("Parameters.{member}.{idx}.ApplyMethod"))
                    .unwrap_or_else(|| "immediate".to_string());
            out.push((name, value, apply));
            idx += 1;
        }
        if !out.is_empty() {
            break;
        }
    }
    out
}

fn default_parameters_xml() -> String {
    "<Parameter><ParameterName>neptune_query_timeout</ParameterName><ParameterValue>120000</ParameterValue><Description>Query execution timeout, in milliseconds</Description><Source>engine-default</Source><ApplyType>dynamic</ApplyType><DataType>integer</DataType><AllowedValues>10-2147483647</AllowedValues><IsModifiable>true</IsModifiable><ApplyMethod>pending-reboot</ApplyMethod></Parameter>\
     <Parameter><ParameterName>neptune_enable_audit_log</ParameterName><ParameterValue>0</ParameterValue><Description>Enables or disables audit logs</Description><Source>engine-default</Source><ApplyType>static</ApplyType><DataType>integer</DataType><AllowedValues>0,1</AllowedValues><IsModifiable>true</IsModifiable><ApplyMethod>pending-reboot</ApplyMethod></Parameter>\
     <Parameter><ParameterName>neptune_lab_mode</ParameterName><ParameterValue></ParameterValue><Description>Enables experimental engine features</Description><Source>engine-default</Source><ApplyType>static</ApplyType><DataType>string</DataType><IsModifiable>true</IsModifiable><ApplyMethod>pending-reboot</ApplyMethod></Parameter>".to_string()
}

/// Resolve the tag vector of the resource an ARN points at (read-only).
fn resource_tags(st: &NeptuneState, arn: &str) -> Option<Vec<Tag>> {
    let (kind, id) = parse_resource_arn(arn)?;
    match kind {
        "cluster" => st.clusters.get(id).map(|c| c.tags.clone()),
        "db" => st.instances.get(id).map(|i| i.tags.clone()),
        "cluster-endpoint" => st.cluster_endpoints.get(id).map(|e| e.tags.clone()),
        "cluster-snapshot" => st.cluster_snapshots.get(id).map(|s| s.tags.clone()),
        "cluster-pg" => st.cluster_parameter_groups.get(id).map(|g| g.tags.clone()),
        "pg" => st.parameter_groups.get(id).map(|g| g.tags.clone()),
        "subgrp" => st.subnet_groups.get(id).map(|g| g.tags.clone()),
        "global-cluster" => st.global_clusters.get(id).map(|g| g.tags.clone()),
        "es" => st.event_subscriptions.get(id).map(|s| s.tags.clone()),
        _ => None,
    }
    .or(Some(Vec::new()))
}

/// Resolve a mutable reference to the tag vector of the resource an ARN
/// points at. `None` when the ARN doesn't resolve to a known resource.
fn resource_tags_mut<'a>(st: &'a mut NeptuneState, arn: &str) -> Option<&'a mut Vec<Tag>> {
    let (kind, id) = parse_resource_arn(arn)?;
    match kind {
        "cluster" => st.clusters.get_mut(id).map(|c| &mut c.tags),
        "db" => st.instances.get_mut(id).map(|i| &mut i.tags),
        "cluster-endpoint" => st.cluster_endpoints.get_mut(id).map(|e| &mut e.tags),
        "cluster-snapshot" => st.cluster_snapshots.get_mut(id).map(|s| &mut s.tags),
        "cluster-pg" => st.cluster_parameter_groups.get_mut(id).map(|g| &mut g.tags),
        "pg" => st.parameter_groups.get_mut(id).map(|g| &mut g.tags),
        "subgrp" => st.subnet_groups.get_mut(id).map(|g| &mut g.tags),
        "global-cluster" => st.global_clusters.get_mut(id).map(|g| &mut g.tags),
        "es" => st.event_subscriptions.get_mut(id).map(|s| &mut s.tags),
        _ => None,
    }
}

/// Split `arn:aws:rds:region:account:kind:id` into `(kind, id)`.
fn parse_resource_arn(arn: &str) -> Option<(&str, &str)> {
    let parts: Vec<&str> = arn.splitn(7, ':').collect();
    if parts.len() >= 7 && parts[0] == "arn" {
        Some((parts[5], parts[6]))
    } else {
        None
    }
}

#[async_trait]
impl AwsService for NeptuneService {
    fn service_name(&self) -> &str {
        "neptune"
    }

    async fn handle(&self, request: AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        crate::validation::prevalidate(request.action.as_str(), &request)?;

        let action = request.action.as_str();
        let result = match action {
            "CreateDBCluster" => self.create_db_cluster(&request),
            "DescribeDBClusters" => self.describe_db_clusters(&request),
            "ModifyDBCluster" => self.modify_db_cluster(&request),
            "DeleteDBCluster" => self.delete_db_cluster(&request),
            "StartDBCluster" => self.cluster_lifecycle(&request, "StartDBCluster", "available"),
            "StopDBCluster" => self.cluster_lifecycle(&request, "StopDBCluster", "stopped"),
            "FailoverDBCluster" => self.failover_db_cluster(&request),
            "PromoteReadReplicaDBCluster" => self.promote_read_replica_db_cluster(&request),
            "AddRoleToDBCluster" => self.add_role_to_db_cluster(&request),
            "RemoveRoleFromDBCluster" => self.remove_role_from_db_cluster(&request),
            "CreateDBClusterEndpoint" => self.create_db_cluster_endpoint(&request),
            "ModifyDBClusterEndpoint" => self.modify_db_cluster_endpoint(&request),
            "DeleteDBClusterEndpoint" => self.delete_db_cluster_endpoint(&request),
            "DescribeDBClusterEndpoints" => self.describe_db_cluster_endpoints(&request),
            "CreateDBInstance" => self.create_db_instance(&request),
            "DescribeDBInstances" => self.describe_db_instances(&request),
            "ModifyDBInstance" => self.modify_db_instance(&request),
            "DeleteDBInstance" => self.delete_db_instance(&request),
            "RebootDBInstance" => self.reboot_db_instance(&request),
            "DescribeValidDBInstanceModifications" => {
                self.describe_valid_db_instance_modifications(&request)
            }
            "CreateDBClusterSnapshot" => self.create_db_cluster_snapshot(&request),
            "CopyDBClusterSnapshot" => self.copy_db_cluster_snapshot(&request),
            "DeleteDBClusterSnapshot" => self.delete_db_cluster_snapshot(&request),
            "DescribeDBClusterSnapshots" => self.describe_db_cluster_snapshots(&request),
            "DescribeDBClusterSnapshotAttributes" => {
                self.describe_db_cluster_snapshot_attributes(&request)
            }
            "ModifyDBClusterSnapshotAttribute" => {
                self.modify_db_cluster_snapshot_attribute(&request)
            }
            "RestoreDBClusterFromSnapshot" => self.restore_db_cluster_from_snapshot(&request),
            "RestoreDBClusterToPointInTime" => self.restore_db_cluster_to_point_in_time(&request),
            "CreateDBClusterParameterGroup" => self.create_db_cluster_parameter_group(&request),
            "CopyDBClusterParameterGroup" => self.copy_db_cluster_parameter_group(&request),
            "DeleteDBClusterParameterGroup" => self.delete_db_cluster_parameter_group(&request),
            "DescribeDBClusterParameterGroups" => {
                self.describe_db_cluster_parameter_groups(&request)
            }
            "DescribeDBClusterParameters" => self.describe_db_cluster_parameters(&request),
            "ModifyDBClusterParameterGroup" => self.modify_db_cluster_parameter_group(&request),
            "ResetDBClusterParameterGroup" => self.reset_db_cluster_parameter_group(&request),
            "CreateDBParameterGroup" => self.create_db_parameter_group(&request),
            "CopyDBParameterGroup" => self.copy_db_parameter_group(&request),
            "DeleteDBParameterGroup" => self.delete_db_parameter_group(&request),
            "DescribeDBParameterGroups" => self.describe_db_parameter_groups(&request),
            "DescribeDBParameters" => self.describe_db_parameters(&request),
            "ModifyDBParameterGroup" => self.modify_db_parameter_group(&request),
            "ResetDBParameterGroup" => self.reset_db_parameter_group(&request),
            "CreateDBSubnetGroup" => self.create_db_subnet_group(&request),
            "ModifyDBSubnetGroup" => self.modify_db_subnet_group(&request),
            "DeleteDBSubnetGroup" => self.delete_db_subnet_group(&request),
            "DescribeDBSubnetGroups" => self.describe_db_subnet_groups(&request),
            "CreateGlobalCluster" => self.create_global_cluster(&request),
            "ModifyGlobalCluster" => self.modify_global_cluster(&request),
            "DeleteGlobalCluster" => self.delete_global_cluster(&request),
            "DescribeGlobalClusters" => self.describe_global_clusters(&request),
            "RemoveFromGlobalCluster" => self.remove_from_global_cluster(&request),
            "FailoverGlobalCluster" => self.failover_global_cluster(&request),
            "SwitchoverGlobalCluster" => self.switchover_global_cluster(&request),
            "CreateEventSubscription" => self.create_event_subscription(&request),
            "ModifyEventSubscription" => self.modify_event_subscription(&request),
            "DeleteEventSubscription" => self.delete_event_subscription(&request),
            "DescribeEventSubscriptions" => self.describe_event_subscriptions(&request),
            "AddSourceIdentifierToSubscription" => {
                self.add_source_identifier_to_subscription(&request)
            }
            "RemoveSourceIdentifierFromSubscription" => {
                self.remove_source_identifier_from_subscription(&request)
            }
            "AddTagsToResource" => self.add_tags_to_resource(&request),
            "RemoveTagsFromResource" => self.remove_tags_from_resource(&request),
            "ListTagsForResource" => self.list_tags_for_resource(&request),
            "DescribeDBEngineVersions" => self.describe_db_engine_versions(&request),
            "DescribeOrderableDBInstanceOptions" => {
                self.describe_orderable_db_instance_options(&request)
            }
            "DescribeEventCategories" => self.describe_event_categories(&request),
            "DescribeEvents" => self.describe_events(&request),
            "DescribeEngineDefaultClusterParameters" => {
                self.describe_engine_default_cluster_parameters(&request)
            }
            "DescribeEngineDefaultParameters" => self.describe_engine_default_parameters(&request),
            "ApplyPendingMaintenanceAction" => self.apply_pending_maintenance_action(&request),
            "DescribePendingMaintenanceActions" => {
                self.describe_pending_maintenance_actions(&request)
            }
            other => Err(AwsServiceError::action_not_implemented("neptune", other)),
        };

        if is_mutating(action) && matches!(result.as_ref(), Ok(resp) if resp.status.is_success()) {
            self.save_snapshot().await;
        }
        result
    }

    fn supported_actions(&self) -> &[&str] {
        SUPPORTED_ACTIONS
    }
}

#[cfg(test)]
#[path = "tests.rs"]
mod tests;
