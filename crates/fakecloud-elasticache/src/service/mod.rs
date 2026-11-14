use std::convert::TryFrom;
use std::sync::Arc;

use async_trait::async_trait;
use http::StatusCode;
use tokio::sync::Mutex as AsyncMutex;

use fakecloud_aws::xml::xml_escape;
use fakecloud_core::query::{optional_query_param, query_response_xml, required_query_param};
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsService, AwsServiceError};
use fakecloud_persistence::SnapshotStore;
use fakecloud_s3::SharedS3State;

use crate::runtime::ElastiCacheRuntime;
use crate::state::{
    default_engine_versions, default_parameters_for_family, elasticache_arn, CacheCluster,
    CacheEngineVersion, CacheParameterGroup, CacheSnapshot, CacheSubnetGroup, ElastiCacheSnapshot,
    ElastiCacheState, ElastiCacheUser, ElastiCacheUserGroup, EngineDefaultParameter,
    GlobalReplicationGroup, GlobalReplicationGroupMember, LogDeliveryConfiguration,
    RecurringCharge, ReplicationGroup, ReservedCacheNode, ReservedCacheNodesOffering,
    ServerlessCache, ServerlessCacheDataStorage, ServerlessCacheEcpuPerSecond,
    ServerlessCacheEndpoint, ServerlessCacheSnapshot, ServerlessCacheUsageLimits,
    SharedElastiCacheState, ELASTICACHE_SNAPSHOT_SCHEMA_VERSION,
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
    "PrepareMigration",
    "StartMigration",
    "StopMigration",
    "CompleteMigration",
    "TestMigration",
];

/// On restart, recovery must re-drive every resource that isn't being torn
/// down -- not just `available` ones. A resource snapshotted mid-transition
/// (creating/modifying/rebooting/starting) would otherwise never have its
/// background task re-spawned and stay stuck forever (bug-audit 2026-06-20,
/// 4.5).
pub(crate) fn is_recoverable_status(status: &str) -> bool {
    !matches!(status, "deleting" | "deleted")
}

/// A `creating` cache snapshot whose backing replication group still exists,
/// so its RDB dump can be re-run on restart. Produced by
/// `plan_snapshot_recovery`, driven by `reconcile_inflight_snapshots`.
pub(crate) struct SnapshotRearm {
    pub(crate) account_id: String,
    pub(crate) snapshot_name: String,
    /// The snapshot row's creation time: which row of that name to complete.
    pub(crate) snapshot_created: String,
    /// The source replication group's incarnation, whose container to dump.
    pub(crate) incarnation: String,
}

pub struct ElastiCacheService {
    state: SharedElastiCacheState,
    runtime: Option<Arc<ElastiCacheRuntime>>,
    snapshot_store: Option<Arc<dyn SnapshotStore>>,
    snapshot_lock: Arc<AsyncMutex<()>>,
    /// Shared S3 state so `ExportServerlessCacheSnapshot` can write the
    /// exported artifact into the target bucket, matching AWS. `None` in
    /// unit tests / when S3 isn't wired.
    s3: Option<SharedS3State>,
    /// Durable S3 store, so an exported artifact is written through to disk
    /// and survives a restart. `None` in memory mode / unit tests.
    s3_store: Option<Arc<dyn fakecloud_persistence::S3Store>>,
    /// Durable data directory (the `<data-path>/elasticache` root) under which
    /// snapshot RDB artifacts are written so they survive a restart. `None` in
    /// memory mode / unit tests, where the RDB falls back to the platform temp
    /// dir (nothing is persisted across a restart in that mode anyway).
    data_dir: Option<std::path::PathBuf>,
    /// EC2 state: a cache subnet group's subnets resolve there (its `VpcId`
    /// and per-subnet Availability Zones). `None` in memory-only unit tests.
    ec2_state: Option<fakecloud_ec2::SharedEc2State>,
}

mod clusters;
mod migrations;
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
            s3: None,
            s3_store: None,
            data_dir: None,
            ec2_state: None,
        }
    }

    pub fn with_runtime(mut self, runtime: Arc<ElastiCacheRuntime>) -> Self {
        self.runtime = Some(runtime);
        self
    }

    /// Wire the durable data directory (`<data-path>/elasticache`) so snapshot
    /// RDB artifacts are written under it and survive a restart, rather than
    /// under the platform temp dir which reboot clears (bug: a persisted
    /// snapshot pointing into temp restores an empty cache after reboot).
    pub fn with_data_dir(mut self, data_dir: std::path::PathBuf) -> Self {
        self.data_dir = Some(data_dir);
        self
    }

    /// Wire the shared S3 state used by `ExportServerlessCacheSnapshot` to
    /// write the exported snapshot artifact into the target bucket.
    pub fn with_ec2_state(mut self, ec2_state: fakecloud_ec2::SharedEc2State) -> Self {
        self.ec2_state = Some(ec2_state);
        self
    }

    pub fn with_s3(mut self, s3: SharedS3State) -> Self {
        self.s3 = Some(s3);
        self
    }

    /// Wire the durable S3 store so exported artifacts persist across a
    /// restart, like any other object written to a bucket.
    pub fn with_s3_store(mut self, store: Arc<dyn fakecloud_persistence::S3Store>) -> Self {
        self.s3_store = Some(store);
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

    /// Build a hook that persists the current state when invoked, or `None` in
    /// memory mode. The CloudFormation provisioner mutates `state` directly and
    /// uses this to write a CFN-provisioned resource through to disk.
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

    /// Settle which data volume each persisted resource mounts before any
    /// container is recreated. Resources persisted without a binding (state
    /// written before volumes were scoped to the data dir) are bound against
    /// the daemon's volumes, keeping a legacy volume when one exists, and the
    /// bindings are persisted; every legacy binding is then registered with
    /// the runtime. If the daemon can't list volumes, the unbound resources
    /// stay unbound until a start where it can.
    async fn resolve_data_volumes(&self, runtime: &ElastiCacheRuntime) {
        let unbound = self
            .state
            .read()
            .iter()
            .any(|(_, s)| s.has_unbound_data_volumes());
        if unbound {
            if let Some(existing) = runtime.list_volumes().await {
                let tag = fakecloud_core::data_volume::current_scope().tag();
                let changed = {
                    let mut accounts = self.state.write();
                    // Pre-scoping builds keyed the volume by id alone, so an
                    // id unbound in two accounts names one shared volume.
                    let mut seen = std::collections::HashSet::new();
                    let mut contested = std::collections::HashSet::new();
                    for (_, state) in accounts.iter() {
                        // Once per account: a cluster and a group sharing an id
                        // in one account aren't a cross-account conflict.
                        let ids: std::collections::BTreeSet<String> =
                            state.unbound_volume_ids().into_iter().collect();
                        for id in ids {
                            if !seen.insert(id.clone()) {
                                contested.insert(id);
                            }
                        }
                    }
                    for id in &contested {
                        tracing::warn!(
                            resource_id = %id,
                            volume = %crate::runtime::legacy_data_volume_name(id),
                            "pre-scoping elasticache volume is shared by several accounts; \
                             none adopts it, each starts on its own volume",
                        );
                    }
                    let mut changed = false;
                    for (_, state) in accounts.iter_mut() {
                        changed |= state.resolve_data_volumes(tag, &existing, &contested);
                    }
                    changed
                };
                if changed {
                    save_snapshot_static(
                        self.state.clone(),
                        self.snapshot_store.clone(),
                        self.snapshot_lock.clone(),
                    )
                    .await;
                }
            }
        }
        let accounts = self.state.read();
        for (_, state) in accounts.iter() {
            for (id, volume) in state.legacy_data_volumes() {
                tracing::info!(
                    resource_id = %id,
                    volume = %volume,
                    "elasticache resource keeps its pre-scoping data volume",
                );
            }
        }
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
        self.resolve_data_volumes(&runtime).await;
        // On k8s there are no Docker volumes, so an unbound resource has
        // nothing to shadow and recovers as usual.
        let uses_volumes = runtime.has_data_volumes();

        struct PendingCluster {
            account_id: String,
            id: String,
            engine: String,
            incarnation: String,
            volume: String,
        }
        struct PendingReplication {
            account_id: String,
            id: String,
            engine: String,
            incarnation: String,
            volume: String,
        }
        struct PendingServerless {
            account_id: String,
            name: String,
            incarnation: String,
            volume: String,
        }

        let (clusters, replications, serverless) = {
            let mut accounts = self.state.write();
            let mut clusters = Vec::new();
            let mut replications = Vec::new();
            let mut serverless = Vec::new();
            for (_, state) in accounts.iter_mut() {
                let account_id = state.account_id.clone();
                for (id, cluster) in state.cache_clusters.iter_mut() {
                    // Re-drive anything not being torn down. The old code only
                    // recovered "available" resources, so one snapshotted
                    // mid-transition (creating/modifying/rebooting) was never
                    // re-spawned and stayed stuck forever (bug-audit 2026-06-20,
                    // 4.5).
                    // An unbound cluster's data volume couldn't be resolved
                    // (the daemon couldn't list volumes): mounting now would
                    // shadow a pre-scoping volume with an empty scoped one.
                    let bound = cluster.data_volume.is_some()
                        || cluster.engine == "memcached"
                        || !uses_volumes;
                    if is_recoverable_status(&cluster.cache_cluster_status) && !bound {
                        // No container comes back: don't keep advertising a
                        // live endpoint.
                        cluster.cache_cluster_status = "incompatible-network".to_string();
                        tracing::warn!(
                            cache_cluster_id = %id,
                            "not recovering cache cluster: its data volume could not be resolved",
                        );
                    } else if is_recoverable_status(&cluster.cache_cluster_status) {
                        cluster.cache_cluster_status = "starting".to_string();
                        clusters.push(PendingCluster {
                            account_id: account_id.clone(),
                            id: id.clone(),
                            engine: cluster.engine.clone(),
                            incarnation: cluster.incarnation(),
                            volume: cluster.data_volume_name(
                                fakecloud_core::data_volume::current_scope().tag(),
                                &account_id,
                            ),
                        });
                    }
                }
                for (id, rg) in state.replication_groups.iter_mut() {
                    let bound =
                        rg.data_volume.is_some() || rg.engine == "memcached" || !uses_volumes;
                    if is_recoverable_status(&rg.status) && !bound {
                        rg.status = "incompatible-network".to_string();
                        tracing::warn!(
                            replication_group_id = %id,
                            "not recovering replication group: its data volume could not be resolved",
                        );
                    } else if is_recoverable_status(&rg.status) {
                        rg.status = "starting".to_string();
                        replications.push(PendingReplication {
                            account_id: account_id.clone(),
                            id: id.clone(),
                            engine: rg.engine.clone(),
                            incarnation: rg.incarnation(),
                            volume: rg.data_volume_name(
                                fakecloud_core::data_volume::current_scope().tag(),
                                &account_id,
                            ),
                        });
                    }
                }
                for (name, sc) in state.serverless_caches.iter_mut() {
                    let bound = sc.data_volume.is_some() || !uses_volumes;
                    if is_recoverable_status(&sc.status) && !bound {
                        sc.status = "create-failed".to_string();
                        tracing::warn!(
                            serverless_cache_name = %name,
                            "not recovering serverless cache: its data volume could not be resolved",
                        );
                    } else if is_recoverable_status(&sc.status) {
                        sc.status = "creating".to_string();
                        serverless.push(PendingServerless {
                            account_id: account_id.clone(),
                            name: name.clone(),
                            incarnation: sc.incarnation(),
                            volume: sc.data_volume_name(
                                fakecloud_core::data_volume::current_scope().tag(),
                                &account_id,
                            ),
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
                                .filter(|cl| cl.incarnation() == c.incarnation)
                                .and_then(|cl| s.tags.get(&cl.arn))
                        })
                        .map(|t| t.iter().cloned().collect())
                        .unwrap_or_default()
                };
                let is_memcached = c.engine == "memcached";
                let result = if is_memcached {
                    runtime
                        .ensure_memcached(&c.incarnation, &c.account_id, &c.id, &pod_tags)
                        .await
                } else {
                    runtime
                        .ensure_redis(
                            &c.incarnation,
                            &c.account_id,
                            &c.id,
                            &c.volume,
                            None,
                            &pod_tags,
                        )
                        .await
                };
                match result {
                    Ok(running) => {
                        let present = {
                            let mut accounts = state.write();
                            match accounts.get_mut(&c.account_id).and_then(|s| {
                                s.cache_clusters
                                    .get_mut(&c.id)
                                    .filter(|cl| cl.incarnation() == c.incarnation)
                            }) {
                                Some(cluster) => {
                                    cluster.cache_cluster_status = "available".to_string();
                                    cluster.endpoint_address = running.endpoint_address.clone();
                                    cluster.endpoint_port = running.endpoint_port;
                                    cluster.host_port = running.host_port;
                                    cluster.container_id = running.container_id;
                                    true
                                }
                                None => false,
                            }
                        };
                        if !present {
                            // Deleted (or reset) while recovering.
                            reap_gone_start(
                                &runtime,
                                &c.incarnation,
                                (!is_memcached).then_some(&*c.volume),
                            )
                            .await;
                        }
                        save_snapshot_static(state, snapshot_store, snapshot_lock).await;
                    }
                    Err(error) => {
                        tracing::error!(
                            %error,
                            cache_cluster_id = %c.id,
                            "failed to recover elasticache cache cluster after restart",
                        );
                        let present = {
                            let mut accounts = state.write();
                            match accounts.get_mut(&c.account_id).and_then(|s| {
                                s.cache_clusters
                                    .get_mut(&c.id)
                                    .filter(|cl| cl.incarnation() == c.incarnation)
                            }) {
                                Some(cluster) => {
                                    cluster.cache_cluster_status =
                                        "incompatible-network".to_string();
                                    true
                                }
                                None => false,
                            }
                        };
                        if !present {
                            // Deleted while recovering: the failed start may
                            // still have created this incarnation's volume.
                            reap_gone_start(
                                &runtime,
                                &c.incarnation,
                                (!is_memcached).then_some(&*c.volume),
                            )
                            .await;
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
                                .filter(|rg| rg.incarnation() == r.incarnation)
                                .and_then(|rg| s.tags.get(&rg.arn))
                        })
                        .map(|t| t.iter().cloned().collect())
                        .unwrap_or_default()
                };
                let is_memcached = r.engine == "memcached";
                let result = if is_memcached {
                    runtime
                        .ensure_memcached(&r.incarnation, &r.account_id, &r.id, &pod_tags)
                        .await
                } else {
                    runtime
                        .ensure_redis(
                            &r.incarnation,
                            &r.account_id,
                            &r.id,
                            &r.volume,
                            None,
                            &pod_tags,
                        )
                        .await
                };
                match result {
                    Ok(running) => {
                        let present = {
                            let mut accounts = state.write();
                            match accounts.get_mut(&r.account_id).and_then(|s| {
                                s.replication_groups
                                    .get_mut(&r.id)
                                    .filter(|g| g.incarnation() == r.incarnation)
                            }) {
                                Some(rg) => {
                                    rg.status = "available".to_string();
                                    rg.endpoint_address = running.endpoint_address.clone();
                                    rg.endpoint_port = running.endpoint_port;
                                    rg.host_port = running.host_port;
                                    rg.container_id = running.container_id;
                                    true
                                }
                                None => false,
                            }
                        };
                        if !present {
                            reap_gone_start(
                                &runtime,
                                &r.incarnation,
                                (!is_memcached).then_some(&*r.volume),
                            )
                            .await;
                        }
                        save_snapshot_static(state, snapshot_store, snapshot_lock).await;
                    }
                    Err(error) => {
                        tracing::error!(
                            %error,
                            replication_group_id = %r.id,
                            "failed to recover elasticache replication group after restart",
                        );
                        let present = {
                            let mut accounts = state.write();
                            match accounts.get_mut(&r.account_id).and_then(|s| {
                                s.replication_groups
                                    .get_mut(&r.id)
                                    .filter(|g| g.incarnation() == r.incarnation)
                            }) {
                                Some(rg) => {
                                    rg.status = "incompatible-network".to_string();
                                    true
                                }
                                None => false,
                            }
                        };
                        if !present {
                            // Deleted while recovering: the failed start may
                            // still have created this incarnation's volume.
                            reap_gone_start(
                                &runtime,
                                &r.incarnation,
                                (!is_memcached).then_some(&*r.volume),
                            )
                            .await;
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
                                .filter(|c| c.incarnation() == s.incarnation)
                                .and_then(|c| st.tags.get(&c.arn))
                        })
                        .map(|t| t.iter().cloned().collect())
                        .unwrap_or_default()
                };
                match runtime
                    .ensure_redis(
                        &s.incarnation,
                        &s.account_id,
                        &s.name,
                        &s.volume,
                        None,
                        &pod_tags,
                    )
                    .await
                {
                    Ok(running) => {
                        let present = {
                            let mut accounts = state.write();
                            match accounts.get_mut(&s.account_id).and_then(|st| {
                                st.serverless_caches
                                    .get_mut(&s.name)
                                    .filter(|c| c.incarnation() == s.incarnation)
                            }) {
                                Some(cache) => {
                                    cache.status = "available".to_string();
                                    cache.endpoint.address = running.endpoint_address.clone();
                                    cache.endpoint.port = running.endpoint_port;
                                    cache.reader_endpoint.address =
                                        running.endpoint_address.clone();
                                    cache.reader_endpoint.port = running.endpoint_port;
                                    cache.host_port = running.host_port;
                                    cache.container_id = running.container_id;
                                    true
                                }
                                None => false,
                            }
                        };
                        if !present {
                            reap_gone_start(&runtime, &s.incarnation, Some(&s.volume)).await;
                        }
                        save_snapshot_static(state, snapshot_store, snapshot_lock).await;
                    }
                    Err(error) => {
                        tracing::error!(
                            %error,
                            serverless_cache_name = %s.name,
                            "failed to recover elasticache serverless cache after restart",
                        );
                        let present = {
                            let mut accounts = state.write();
                            match accounts.get_mut(&s.account_id).and_then(|st| {
                                st.serverless_caches
                                    .get_mut(&s.name)
                                    .filter(|c| c.incarnation() == s.incarnation)
                            }) {
                                Some(cache) => {
                                    cache.status = "create-failed".to_string();
                                    true
                                }
                                None => false,
                            }
                        };
                        if !present {
                            // Deleted while recovering: the failed start may
                            // still have created this incarnation's volume.
                            reap_gone_start(&runtime, &s.incarnation, Some(&s.volume)).await;
                        }
                        save_snapshot_static(state, snapshot_store, snapshot_lock).await;
                    }
                }
            });
        }
    }

    /// Reconcile cache snapshots persisted mid-dump (`creating`) after a
    /// restart. The RDB dump runs in a detached task that flips the row to
    /// `available`; a crash mid-dump leaves the snapshot `creating` forever
    /// (never usable, name blocked). The dispatch-level auto-save can persist
    /// that `creating` row, so on load we reconcile it: if the source
    /// replication group still exists (and a runtime is wired) re-arm the dump
    /// finalizer; otherwise mark the snapshot `failed` so the name is reusable
    /// and Describe shows a terminal state. Mirrors how RDS re-arms in-flight
    /// snapshots on load.
    pub async fn reconcile_inflight_snapshots(&self) {
        let rearm = self.plan_snapshot_recovery(self.runtime.is_some());
        // Persist any `failed` transitions the planner applied to disk.
        self.save_snapshot().await;
        if let Some(runtime) = self.runtime.clone() {
            for r in rearm {
                self.spawn_snapshot_dump(
                    runtime.clone(),
                    r.account_id,
                    r.snapshot_name,
                    r.snapshot_created,
                    r.incarnation,
                );
            }
        }
    }

    /// Pure state reconcile for `creating` snapshots: marks the un-recoverable
    /// ones `failed` and returns the set to re-arm via the dump finalizer.
    /// Split out for Docker-free unit testing. A snapshot is re-armed only when
    /// its replication group still exists AND a runtime is wired
    /// (`has_runtime`); otherwise it transitions to the terminal `failed`.
    /// `has_runtime` is passed in so the re-arm branch is exercisable in
    /// Docker-free unit tests.
    pub(crate) fn plan_snapshot_recovery(&self, has_runtime: bool) -> Vec<SnapshotRearm> {
        let mut rearm = Vec::new();
        let mut accounts = self.state.write();
        for (_, state) in accounts.iter_mut() {
            let account_id = state.account_id.clone();
            // Snapshot the present group ids so the immutable borrow ends
            // before we mutate `state.snapshots` below.
            let groups: std::collections::HashMap<String, String> = state
                .replication_groups
                .iter()
                .map(|(id, g)| (id.clone(), g.incarnation()))
                .collect();
            for (name, snap) in state.snapshots.iter_mut() {
                if snap.snapshot_status != "creating" {
                    continue;
                }
                // Re-armed only against the group incarnation it was taken
                // from (any, for snapshots persisted before that was pinned).
                let source = groups.get(&snap.replication_group_id).filter(|inc| {
                    snap.source_incarnation
                        .as_ref()
                        .is_none_or(|pinned| pinned == *inc)
                });
                if let (true, Some(incarnation)) = (has_runtime, source) {
                    rearm.push(SnapshotRearm {
                        account_id: account_id.clone(),
                        snapshot_name: name.clone(),
                        snapshot_created: snap.created_at.clone(),
                        incarnation: incarnation.clone(),
                    });
                } else {
                    snap.snapshot_status = "failed".to_string();
                }
            }
        }
        rearm
    }
}

/// A start task's container came up, but the resource incarnation it was
/// started for is gone (deleted or reset while it booted): remove the
/// container and the data volume it may have (re)created, which the delete
/// couldn't remove while it was mounted. Keyed by incarnation, so a new
/// resource reusing the id is never reached.
pub(crate) async fn reap_gone_start(
    runtime: &ElastiCacheRuntime,
    incarnation: &str,
    volume: Option<&str>,
) {
    runtime.stop(incarnation).await;
    if let Some(volume) = volume {
        runtime.remove_data_volume_named(volume).await;
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
            "PrepareMigration" => self.prepare_migration(&request),
            "StartMigration" => self.start_migration(&request).await,
            "StopMigration" => self.stop_migration(&request),
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
                    target_ids.push((c.cache_cluster_id.clone(), c.incarnation()));
                }
            }
            for g in state.replication_groups.values() {
                if g.cache_parameter_group_name.as_deref() == Some(param_group_name)
                    && (g.engine == ENGINE_REDIS || g.engine == ENGINE_VALKEY)
                {
                    target_ids.push((g.replication_group_id.clone(), g.incarnation()));
                }
            }
            let params = state
                .parameter_group_parameters
                .get(param_group_name)
                .cloned()
                .unwrap_or_default();
            (target_ids, params)
        };
        for (id, incarnation) in target_ids {
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
                match runtime.exec_redis(&incarnation, &args).await {
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
