use std::convert::TryFrom;
use std::sync::Arc;

use async_trait::async_trait;
use http::StatusCode;
use tokio::sync::Mutex as AsyncMutex;

use fakecloud_aws::xml::xml_escape;
use fakecloud_core::query::{optional_query_param, query_response_xml, required_query_param};
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsService, AwsServiceError};
use fakecloud_persistence::SnapshotStore;

use crate::runtime::ElastiCacheRuntime;
use crate::state::{
    default_engine_versions, default_parameters_for_family, CacheCluster, CacheEngineVersion,
    CacheParameterGroup, CacheSnapshot, CacheSubnetGroup, ElastiCacheSnapshot, ElastiCacheState,
    ElastiCacheUser, ElastiCacheUserGroup, EngineDefaultParameter, GlobalReplicationGroup,
    GlobalReplicationGroupMember, LogDeliveryConfiguration, RecurringCharge, ReplicationGroup,
    ReservedCacheNode, ReservedCacheNodesOffering, ServerlessCache, ServerlessCacheDataStorage,
    ServerlessCacheEcpuPerSecond, ServerlessCacheEndpoint, ServerlessCacheSnapshot,
    ServerlessCacheUsageLimits, SharedElastiCacheState, ELASTICACHE_SNAPSHOT_SCHEMA_VERSION,
};

const ELASTICACHE_NS: &str = "http://elasticache.amazonaws.com/doc/2015-02-02/";

/// Cache engine wire values. Stored as ``String`` on ``CacheCluster`` etc., but
/// validated against this list at the wire boundary so a typo can't slip in.
const ENGINE_REDIS: &str = "redis";
const ENGINE_VALKEY: &str = "valkey";
const ENGINE_MEMCACHED: &str = "memcached";
const SUPPORTED_ENGINES: &[&str] = &[ENGINE_REDIS, ENGINE_VALKEY, ENGINE_MEMCACHED];

fn validate_engine(engine: &str) -> Result<(), AwsServiceError> {
    if !SUPPORTED_ENGINES.contains(&engine) {
        return Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "InvalidParameterValue",
            format!(
                "Invalid value for Engine: {engine}. Supported engines: redis, valkey, memcached"
            ),
        ));
    }
    Ok(())
}

fn reject_memcached_for(engine: &str, feature: &str) -> Result<(), AwsServiceError> {
    if engine == ENGINE_MEMCACHED {
        return Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "InvalidParameterValue",
            format!("{feature} is not supported for the memcached engine."),
        ));
    }
    Ok(())
}

/// Engine + version-dependent ceiling on NumNodeGroups for cluster-mode
/// replication groups, mirroring AWS's documented limits. Redis prior to
/// 5.0.6 was capped at 90 shards; 5.0.6+ and Valkey raised the ceiling to
/// 500. Memcached has no replication groups (rejected upstream) but
/// returning the modern 500 here makes the function total. Unparseable
/// version strings fall through to the modern ceiling because fakecloud
/// only ships the redis 7.x / valkey 8.x backing images today; treating
/// unknown versions as "legacy" would surprise callers passing odd
/// strings like the engine label `Redis`.
fn max_node_groups_for(engine: &str, engine_version: &str) -> i32 {
    if engine == ENGINE_REDIS {
        // Parse "MAJOR.MINOR.PATCH" or "MAJOR.MINOR" and only flip to the
        // legacy 90-shard cap when we successfully parsed a major version.
        let mut parts = engine_version.split('.').map(|p| p.parse::<u32>().ok());
        if let Some(Some(major)) = parts.next() {
            let minor = parts.next().flatten().unwrap_or(0);
            let patch = parts.next().flatten().unwrap_or(0);
            // 5.0.6 is the threshold. Anything strictly earlier is capped at 90.
            let pre_506 = major < 5 || (major == 5 && minor == 0 && patch < 6);
            if pre_506 {
                return 90;
            }
        }
    }
    500
}
const SUPPORTED_ACTIONS: &[&str] = &[
    "AddTagsToResource",
    "CreateCacheCluster",
    "CreateGlobalReplicationGroup",
    "CreateCacheSubnetGroup",
    "CreateReplicationGroup",
    "CreateServerlessCache",
    "CreateServerlessCacheSnapshot",
    "CreateSnapshot",
    "CreateUser",
    "CreateUserGroup",
    "DecreaseReplicaCount",
    "DeleteCacheCluster",
    "DeleteGlobalReplicationGroup",
    "DeleteCacheSubnetGroup",
    "DeleteReplicationGroup",
    "DeleteServerlessCache",
    "DeleteServerlessCacheSnapshot",
    "DeleteSnapshot",
    "DeleteUser",
    "DeleteUserGroup",
    "DescribeCacheClusters",
    "DescribeCacheEngineVersions",
    "DescribeGlobalReplicationGroups",
    "DescribeCacheParameterGroups",
    "DescribeReservedCacheNodes",
    "DescribeReservedCacheNodesOfferings",
    "DescribeCacheSubnetGroups",
    "DescribeEngineDefaultParameters",
    "DescribeReplicationGroups",
    "DescribeServerlessCaches",
    "DescribeServerlessCacheSnapshots",
    "DescribeSnapshots",
    "DescribeUserGroups",
    "DescribeUsers",
    "DisassociateGlobalReplicationGroup",
    "FailoverGlobalReplicationGroup",
    "IncreaseReplicaCount",
    "ListTagsForResource",
    "ModifyCacheSubnetGroup",
    "ModifyGlobalReplicationGroup",
    "ModifyReplicationGroup",
    "ModifyServerlessCache",
    "RemoveTagsFromResource",
    "TestFailover",
    "AuthorizeCacheSecurityGroupIngress",
    "RevokeCacheSecurityGroupIngress",
    "CreateCacheSecurityGroup",
    "DeleteCacheSecurityGroup",
    "DescribeCacheSecurityGroups",
    "CreateCacheParameterGroup",
    "DeleteCacheParameterGroup",
    "ModifyCacheParameterGroup",
    "ResetCacheParameterGroup",
    "DescribeCacheParameters",
    "ModifyCacheCluster",
    "RebootCacheCluster",
    "ListAllowedNodeTypeModifications",
    "ModifyReplicationGroupShardConfiguration",
    "DecreaseNodeGroupsInGlobalReplicationGroup",
    "IncreaseNodeGroupsInGlobalReplicationGroup",
    "RebalanceSlotsInGlobalReplicationGroup",
    "ModifyUser",
    "ModifyUserGroup",
    "PurchaseReservedCacheNodesOffering",
    "DescribeEvents",
    "DescribeServiceUpdates",
    "DescribeUpdateActions",
    "BatchApplyUpdateAction",
    "BatchStopUpdateAction",
    "CopySnapshot",
    "CopyServerlessCacheSnapshot",
    "ExportServerlessCacheSnapshot",
    "StartMigration",
    "CompleteMigration",
    "TestMigration",
];

pub struct ElastiCacheService {
    state: SharedElastiCacheState,
    runtime: Option<Arc<ElastiCacheRuntime>>,
    snapshot_store: Option<Arc<dyn SnapshotStore>>,
    snapshot_lock: Arc<AsyncMutex<()>>,
}

mod clusters;
mod misc;
mod parameter_groups;
mod replication;
mod security_groups;
mod serverless;
mod snapshots;
mod subnet_groups;
mod users;

impl ElastiCacheService {
    pub fn new(state: SharedElastiCacheState) -> Self {
        Self {
            state,
            runtime: None,
            snapshot_store: None,
            snapshot_lock: Arc::new(AsyncMutex::new(())),
        }
    }

    pub fn with_runtime(mut self, runtime: Arc<ElastiCacheRuntime>) -> Self {
        self.runtime = Some(runtime);
        self
    }

    pub fn with_snapshot_store(mut self, store: Arc<dyn SnapshotStore>) -> Self {
        self.snapshot_store = Some(store);
        self
    }

    async fn save_snapshot(&self) {
        save_snapshot_static(
            self.state.clone(),
            self.snapshot_store.clone(),
            self.snapshot_lock.clone(),
        )
        .await;
    }

    /// Recreate the backing Docker/Podman containers for persisted cache
    /// clusters, replication groups, and serverless caches after a
    /// fakecloud restart. Same bug class as RDS #1338 — without this,
    /// describe ops report the cluster as `available` while the
    /// container is gone, so the endpoint is dead.
    pub async fn recover_persisted_containers(&self) {
        let Some(runtime) = self.runtime.clone() else {
            return;
        };

        struct PendingCluster {
            account_id: String,
            id: String,
            engine: String,
        }
        struct PendingReplication {
            account_id: String,
            id: String,
            engine: String,
        }
        struct PendingServerless {
            account_id: String,
            name: String,
        }

        let (clusters, replications, serverless) = {
            let mut accounts = self.state.write();
            let mut clusters = Vec::new();
            let mut replications = Vec::new();
            let mut serverless = Vec::new();
            for (_, state) in accounts.iter_mut() {
                let account_id = state.account_id.clone();
                for (id, cluster) in state.cache_clusters.iter_mut() {
                    if cluster.cache_cluster_status == "available" {
                        cluster.cache_cluster_status = "starting".to_string();
                        clusters.push(PendingCluster {
                            account_id: account_id.clone(),
                            id: id.clone(),
                            engine: cluster.engine.clone(),
                        });
                    }
                }
                for (id, rg) in state.replication_groups.iter_mut() {
                    if rg.status == "available" {
                        rg.status = "starting".to_string();
                        replications.push(PendingReplication {
                            account_id: account_id.clone(),
                            id: id.clone(),
                            engine: rg.engine.clone(),
                        });
                    }
                }
                for (name, sc) in state.serverless_caches.iter_mut() {
                    if sc.status == "available" {
                        sc.status = "creating".to_string();
                        serverless.push(PendingServerless {
                            account_id: account_id.clone(),
                            name: name.clone(),
                        });
                    }
                }
            }
            (clusters, replications, serverless)
        };

        let total = clusters.len() + replications.len() + serverless.len();
        if total == 0 {
            return;
        }
        tracing::info!(
            count = total,
            "recovering backing containers for persisted elasticache resources",
        );

        for c in clusters {
            let runtime = runtime.clone();
            let state = self.state.clone();
            let snapshot_store = self.snapshot_store.clone();
            let snapshot_lock = self.snapshot_lock.clone();
            tokio::spawn(async move {
                // Re-derive this cluster's reserved `fakecloud-k8s/*`
                // scheduling tags from persisted state so the recreated Pod
                // keeps its node placement across a restart.
                let pod_tags: std::collections::BTreeMap<String, String> = {
                    let accounts = state.read();
                    accounts
                        .get(&c.account_id)
                        .and_then(|s| {
                            s.cache_clusters
                                .get(&c.id)
                                .and_then(|cl| s.tags.get(&cl.arn))
                        })
                        .map(|t| t.iter().cloned().collect())
                        .unwrap_or_default()
                };
                let result = if c.engine == "memcached" {
                    runtime.ensure_memcached(&c.id, &pod_tags).await
                } else {
                    runtime.ensure_redis(&c.id, None, &pod_tags).await
                };
                match result {
                    Ok(running) => {
                        {
                            let mut accounts = state.write();
                            if let Some(s) = accounts.get_mut(&c.account_id) {
                                if let Some(cluster) = s.cache_clusters.get_mut(&c.id) {
                                    cluster.cache_cluster_status = "available".to_string();
                                    cluster.endpoint_address = running.endpoint_address.clone();
                                    cluster.endpoint_port = running.endpoint_port;
                                    cluster.host_port = running.host_port;
                                    cluster.container_id = running.container_id;
                                }
                            }
                        }
                        save_snapshot_static(state, snapshot_store, snapshot_lock).await;
                    }
                    Err(error) => {
                        tracing::error!(
                            %error,
                            cache_cluster_id = %c.id,
                            "failed to recover elasticache cache cluster after restart",
                        );
                        {
                            let mut accounts = state.write();
                            if let Some(s) = accounts.get_mut(&c.account_id) {
                                if let Some(cluster) = s.cache_clusters.get_mut(&c.id) {
                                    cluster.cache_cluster_status =
                                        "incompatible-network".to_string();
                                }
                            }
                        }
                        save_snapshot_static(state, snapshot_store, snapshot_lock).await;
                    }
                }
            });
        }

        for r in replications {
            let runtime = runtime.clone();
            let state = self.state.clone();
            let snapshot_store = self.snapshot_store.clone();
            let snapshot_lock = self.snapshot_lock.clone();
            tokio::spawn(async move {
                // Re-derive this group's reserved `fakecloud-k8s/*`
                // scheduling tags from persisted state for the recreated Pod.
                let pod_tags: std::collections::BTreeMap<String, String> = {
                    let accounts = state.read();
                    accounts
                        .get(&r.account_id)
                        .and_then(|s| {
                            s.replication_groups
                                .get(&r.id)
                                .and_then(|rg| s.tags.get(&rg.arn))
                        })
                        .map(|t| t.iter().cloned().collect())
                        .unwrap_or_default()
                };
                let result = if r.engine == "memcached" {
                    runtime.ensure_memcached(&r.id, &pod_tags).await
                } else {
                    runtime.ensure_redis(&r.id, None, &pod_tags).await
                };
                match result {
                    Ok(running) => {
                        {
                            let mut accounts = state.write();
                            if let Some(s) = accounts.get_mut(&r.account_id) {
                                if let Some(rg) = s.replication_groups.get_mut(&r.id) {
                                    rg.status = "available".to_string();
                                    rg.endpoint_address = running.endpoint_address.clone();
                                    rg.endpoint_port = running.endpoint_port;
                                    rg.host_port = running.host_port;
                                    rg.container_id = running.container_id;
                                }
                            }
                        }
                        save_snapshot_static(state, snapshot_store, snapshot_lock).await;
                    }
                    Err(error) => {
                        tracing::error!(
                            %error,
                            replication_group_id = %r.id,
                            "failed to recover elasticache replication group after restart",
                        );
                        {
                            let mut accounts = state.write();
                            if let Some(s) = accounts.get_mut(&r.account_id) {
                                if let Some(rg) = s.replication_groups.get_mut(&r.id) {
                                    rg.status = "incompatible-network".to_string();
                                }
                            }
                        }
                        save_snapshot_static(state, snapshot_store, snapshot_lock).await;
                    }
                }
            });
        }

        for s in serverless {
            let runtime = runtime.clone();
            let state = self.state.clone();
            let snapshot_store = self.snapshot_store.clone();
            let snapshot_lock = self.snapshot_lock.clone();
            tokio::spawn(async move {
                // Re-derive this serverless cache's reserved `fakecloud-k8s/*`
                // scheduling tags from persisted state for the recreated Pod.
                let pod_tags: std::collections::BTreeMap<String, String> = {
                    let accounts = state.read();
                    accounts
                        .get(&s.account_id)
                        .and_then(|st| {
                            st.serverless_caches
                                .get(&s.name)
                                .and_then(|c| st.tags.get(&c.arn))
                        })
                        .map(|t| t.iter().cloned().collect())
                        .unwrap_or_default()
                };
                match runtime.ensure_redis(&s.name, None, &pod_tags).await {
                    Ok(running) => {
                        {
                            let mut accounts = state.write();
                            if let Some(st) = accounts.get_mut(&s.account_id) {
                                if let Some(cache) = st.serverless_caches.get_mut(&s.name) {
                                    cache.status = "available".to_string();
                                    cache.endpoint.address = running.endpoint_address.clone();
                                    cache.endpoint.port = running.endpoint_port;
                                    cache.reader_endpoint.address =
                                        running.endpoint_address.clone();
                                    cache.reader_endpoint.port = running.endpoint_port;
                                    cache.host_port = running.host_port;
                                    cache.container_id = running.container_id;
                                }
                            }
                        }
                        save_snapshot_static(state, snapshot_store, snapshot_lock).await;
                    }
                    Err(error) => {
                        tracing::error!(
                            %error,
                            serverless_cache_name = %s.name,
                            "failed to recover elasticache serverless cache after restart",
                        );
                        {
                            let mut accounts = state.write();
                            if let Some(st) = accounts.get_mut(&s.account_id) {
                                if let Some(cache) = st.serverless_caches.get_mut(&s.name) {
                                    cache.status = "create-failed".to_string();
                                }
                            }
                        }
                        save_snapshot_static(state, snapshot_store, snapshot_lock).await;
                    }
                }
            });
        }
    }
}

async fn save_snapshot_static(
    state: SharedElastiCacheState,
    store: Option<Arc<dyn SnapshotStore>>,
    lock: Arc<AsyncMutex<()>>,
) {
    let Some(store) = store else {
        return;
    };
    let _guard = lock.lock().await;
    let snapshot = ElastiCacheSnapshot {
        schema_version: ELASTICACHE_SNAPSHOT_SCHEMA_VERSION,
        state: None,
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
        Ok(Err(err)) => tracing::error!(%err, "failed to write elasticache snapshot"),
        Err(err) => tracing::error!(%err, "elasticache snapshot task panicked"),
    }
}

fn is_mutating_action(action: &str) -> bool {
    !matches!(
        action,
        "DescribeCacheClusters"
            | "DescribeCacheEngineVersions"
            | "DescribeGlobalReplicationGroups"
            | "DescribeCacheParameterGroups"
            | "DescribeReservedCacheNodes"
            | "DescribeReservedCacheNodesOfferings"
            | "DescribeCacheSubnetGroups"
            | "DescribeEngineDefaultParameters"
            | "DescribeReplicationGroups"
            | "DescribeServerlessCaches"
            | "DescribeServerlessCacheSnapshots"
            | "DescribeSnapshots"
            | "DescribeUserGroups"
            | "DescribeUsers"
            | "ListTagsForResource"
            | "DescribeCacheSecurityGroups"
            | "DescribeCacheParameters"
            | "DescribeEvents"
            | "DescribeServiceUpdates"
            | "DescribeUpdateActions"
            | "ListAllowedNodeTypeModifications"
    )
}

#[async_trait]
impl AwsService for ElastiCacheService {
    fn service_name(&self) -> &str {
        "elasticache"
    }

    async fn handle(&self, request: AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let mutates = is_mutating_action(request.action.as_str());
        let result = match request.action.as_str() {
            "AddTagsToResource" => self.add_tags_to_resource(&request),
            "CreateCacheCluster" => self.create_cache_cluster(&request).await,
            "CreateGlobalReplicationGroup" => self.create_global_replication_group(&request),
            "CreateCacheSubnetGroup" => self.create_cache_subnet_group(&request),
            "CreateReplicationGroup" => self.create_replication_group(&request).await,
            "CreateServerlessCache" => self.create_serverless_cache(&request).await,
            "CreateServerlessCacheSnapshot" => self.create_serverless_cache_snapshot(&request),
            "CreateSnapshot" => self.create_snapshot(&request).await,
            "CreateUser" => self.create_user(&request),
            "CreateUserGroup" => self.create_user_group(&request),
            "DecreaseReplicaCount" => self.decrease_replica_count(&request),
            "DeleteCacheCluster" => self.delete_cache_cluster(&request).await,
            "DeleteGlobalReplicationGroup" => self.delete_global_replication_group(&request),
            "DeleteCacheSubnetGroup" => self.delete_cache_subnet_group(&request),
            "DeleteReplicationGroup" => self.delete_replication_group(&request).await,
            "DeleteServerlessCache" => self.delete_serverless_cache(&request).await,
            "DeleteServerlessCacheSnapshot" => self.delete_serverless_cache_snapshot(&request),
            "DeleteSnapshot" => self.delete_snapshot(&request),
            "DeleteUser" => self.delete_user(&request),
            "DeleteUserGroup" => self.delete_user_group(&request),
            "DescribeCacheClusters" => self.describe_cache_clusters(&request),
            "DescribeCacheEngineVersions" => self.describe_cache_engine_versions(&request),
            "DescribeGlobalReplicationGroups" => self.describe_global_replication_groups(&request),
            "DescribeCacheParameterGroups" => self.describe_cache_parameter_groups(&request),
            "DescribeReservedCacheNodes" => self.describe_reserved_cache_nodes(&request),
            "DescribeReservedCacheNodesOfferings" => {
                self.describe_reserved_cache_nodes_offerings(&request)
            }
            "DescribeCacheSubnetGroups" => self.describe_cache_subnet_groups(&request),
            "DescribeEngineDefaultParameters" => self.describe_engine_default_parameters(&request),
            "DescribeReplicationGroups" => self.describe_replication_groups(&request),
            "DescribeServerlessCaches" => self.describe_serverless_caches(&request),
            "DescribeServerlessCacheSnapshots" => {
                self.describe_serverless_cache_snapshots(&request)
            }
            "DescribeSnapshots" => self.describe_snapshots(&request),
            "DescribeUserGroups" => self.describe_user_groups(&request),
            "DescribeUsers" => self.describe_users(&request),
            "DisassociateGlobalReplicationGroup" => {
                self.disassociate_global_replication_group(&request)
            }
            "FailoverGlobalReplicationGroup" => self.failover_global_replication_group(&request),
            "IncreaseReplicaCount" => self.increase_replica_count(&request),
            "ListTagsForResource" => self.list_tags_for_resource(&request),
            "ModifyCacheSubnetGroup" => self.modify_cache_subnet_group(&request),
            "ModifyGlobalReplicationGroup" => self.modify_global_replication_group(&request),
            "ModifyReplicationGroup" => self.modify_replication_group(&request).await,
            "ModifyServerlessCache" => self.modify_serverless_cache(&request),
            "RemoveTagsFromResource" => self.remove_tags_from_resource(&request),
            "TestFailover" => self.test_failover(&request),
            "AuthorizeCacheSecurityGroupIngress" => {
                self.authorize_cache_security_group_ingress(&request)
            }
            "RevokeCacheSecurityGroupIngress" => self.revoke_cache_security_group_ingress(&request),
            "CreateCacheSecurityGroup" => self.create_cache_security_group(&request),
            "DeleteCacheSecurityGroup" => self.delete_cache_security_group(&request),
            "DescribeCacheSecurityGroups" => self.describe_cache_security_groups(&request),
            "CreateCacheParameterGroup" => self.create_cache_parameter_group(&request),
            "DeleteCacheParameterGroup" => self.delete_cache_parameter_group(&request),
            "ModifyCacheParameterGroup" => self.modify_cache_parameter_group(&request).await,
            "ResetCacheParameterGroup" => self.reset_cache_parameter_group(&request),
            "DescribeCacheParameters" => self.describe_cache_parameters(&request),
            "ModifyCacheCluster" => self.modify_cache_cluster(&request),
            "RebootCacheCluster" => self.reboot_cache_cluster(&request).await,
            "ListAllowedNodeTypeModifications" => {
                self.list_allowed_node_type_modifications(&request)
            }
            "ModifyReplicationGroupShardConfiguration" => {
                self.modify_replication_group_shard_configuration(&request)
            }
            "DecreaseNodeGroupsInGlobalReplicationGroup" => {
                self.decrease_node_groups_in_global_replication_group(&request)
            }
            "IncreaseNodeGroupsInGlobalReplicationGroup" => {
                self.increase_node_groups_in_global_replication_group(&request)
            }
            "RebalanceSlotsInGlobalReplicationGroup" => {
                self.rebalance_slots_in_global_replication_group(&request)
            }
            "ModifyUser" => self.modify_user(&request).await,
            "ModifyUserGroup" => self.modify_user_group(&request).await,
            "PurchaseReservedCacheNodesOffering" => {
                self.purchase_reserved_cache_nodes_offering(&request)
            }
            "DescribeEvents" => self.describe_events(&request),
            "DescribeServiceUpdates" => self.describe_service_updates(&request),
            "DescribeUpdateActions" => self.describe_update_actions(&request),
            "BatchApplyUpdateAction" => self.batch_apply_update_action(&request),
            "BatchStopUpdateAction" => self.batch_stop_update_action(&request),
            "CopySnapshot" => self.copy_snapshot(&request),
            "CopyServerlessCacheSnapshot" => self.copy_serverless_cache_snapshot(&request),
            "ExportServerlessCacheSnapshot" => self.export_serverless_cache_snapshot(&request),
            "StartMigration" => self.start_migration(&request),
            "CompleteMigration" => self.complete_migration(&request),
            "TestMigration" => self.test_migration(&request),
            _ => Err(AwsServiceError::action_not_implemented(
                self.service_name(),
                &request.action,
            )),
        };
        if mutates && matches!(result.as_ref(), Ok(resp) if resp.status.is_success()) {
            self.save_snapshot().await;
        }
        result
    }

    fn supported_actions(&self) -> &[&str] {
        SUPPORTED_ACTIONS
    }
}

impl ElastiCacheService {
    // ── Migrations ──

    fn start_migration(&self, request: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        self.migration_op(request, "StartMigration", "queued")
    }

    fn complete_migration(&self, request: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let id = required_query_param(request, "ReplicationGroupId")?;
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&request.account_id);
        // Validate all prerequisites BEFORE mutating migration.status —
        // the prior order flipped status to "complete" and only then
        // surfaced ReplicationGroupNotFoundFault on the lookup, leaving
        // a stale "complete" status on the migration.
        if !state.migrations.contains_key(&id) {
            return Err(AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "ReplicationGroupNotUnderMigrationFault",
                format!("ReplicationGroup {id} is not currently being migrated."),
            ));
        }
        if !state.replication_groups.contains_key(&id) {
            return Err(AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "ReplicationGroupNotFoundFault",
                format!("ReplicationGroup {id} not found."),
            ));
        }
        let migration = state.migrations.get_mut(&id).expect("checked above");
        migration.status = "complete".to_string();
        let group = state.replication_groups.get(&id).expect("checked above");
        let region = state.region.clone();
        let xml = replication_group_xml(group, &region);
        Ok(AwsResponse::xml(
            StatusCode::OK,
            query_response_xml(
                "CompleteMigration",
                ELASTICACHE_NS,
                &format!("<ReplicationGroup>{xml}</ReplicationGroup>"),
                &request.request_id,
            ),
        ))
    }

    fn test_migration(&self, request: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        self.migration_op(request, "TestMigration", "test-passed")
    }

    fn migration_op(
        &self,
        request: &AwsRequest,
        action: &str,
        status: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let id = required_query_param(request, "ReplicationGroupId")?;
        // AWS Query protocol nests indexed members under .{index}.{Field},
        // not .{Field}.{index}.
        let endpoint_addr =
            collect_member_field(request, "CustomerNodeEndpointList.member", "Address")
                .into_iter()
                .next()
                .unwrap_or_else(|| "127.0.0.1".to_string());
        let endpoint_port =
            collect_member_field(request, "CustomerNodeEndpointList.member", "Port")
                .into_iter()
                .next()
                .and_then(|v| v.parse::<i32>().ok())
                .unwrap_or(6379);

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&request.account_id);
        let group = state.replication_groups.get(&id).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "ReplicationGroupNotFoundFault",
                format!("ReplicationGroup {id} not found."),
            )
        })?;
        let region = state.region.clone();
        let xml = replication_group_xml(group, &region);
        state.migrations.insert(
            id.clone(),
            crate::state::Migration {
                replication_group_id: id,
                customer_node_endpoint_address: endpoint_addr,
                customer_node_endpoint_port: endpoint_port,
                status: status.to_string(),
                started_at: chrono::Utc::now().to_rfc3339(),
            },
        );
        Ok(AwsResponse::xml(
            StatusCode::OK,
            query_response_xml(
                action,
                ELASTICACHE_NS,
                &format!("<ReplicationGroup>{xml}</ReplicationGroup>"),
                &request.request_id,
            ),
        ))
    }

    /// Best-effort parameter application: run `CONFIG SET` for every
    /// user-modified parameter on every Redis/Valkey cluster and
    /// replication group that uses the given parameter group.
    async fn apply_parameters_for_group(&self, account_id: &str, param_group_name: &str) {
        let Some(runtime) = self.runtime.as_ref() else {
            return;
        };
        let (target_ids, params) = {
            let accounts = self.state.read();
            let state = accounts.get(account_id);
            let Some(state) = state else { return };
            let mut target_ids = Vec::new();
            for c in state.cache_clusters.values() {
                if c.cache_parameter_group_name.as_deref() == Some(param_group_name)
                    && (c.engine == ENGINE_REDIS || c.engine == ENGINE_VALKEY)
                {
                    target_ids.push(c.cache_cluster_id.clone());
                }
            }
            for g in state.replication_groups.values() {
                if g.cache_parameter_group_name.as_deref() == Some(param_group_name)
                    && (g.engine == ENGINE_REDIS || g.engine == ENGINE_VALKEY)
                {
                    target_ids.push(g.replication_group_id.clone());
                }
            }
            let params = state
                .parameter_group_parameters
                .get(param_group_name)
                .cloned()
                .unwrap_or_default();
            (target_ids, params)
        };
        for id in target_ids {
            for param in &params {
                if !param.is_modifiable {
                    continue;
                }
                let args = vec![
                    "CONFIG".to_string(),
                    "SET".to_string(),
                    param.parameter_name.clone(),
                    param.parameter_value.clone(),
                ];
                match runtime.exec_redis(&id, &args).await {
                    Ok(output) if !output.success => {
                        tracing::warn!(
                            resource_id = %id,
                            param = %param.parameter_name,
                            stderr = %String::from_utf8_lossy(&output.stderr),
                            "CONFIG SET failed"
                        );
                    }
                    Err(e) => {
                        tracing::warn!(
                            resource_id = %id,
                            param = %param.parameter_name,
                            %e,
                            "CONFIG SET exec failed"
                        );
                    }
                    _ => {}
                }
            }
        }
    }
}

// Helpers

#[path = "../helpers.rs"]
mod helpers;
use helpers::*;

#[cfg(test)]
#[path = "../service_tests.rs"]
mod tests;
