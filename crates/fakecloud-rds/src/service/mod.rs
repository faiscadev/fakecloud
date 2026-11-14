use std::sync::Arc;

use async_trait::async_trait;
use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine;
use chrono::Utc;
use http::StatusCode;
use tokio::sync::Mutex as AsyncMutex;

use fakecloud_aws::xml::xml_escape;
use fakecloud_core::delivery::DeliveryBus;
use fakecloud_core::query::{optional_query_param, query_response_xml, required_query_param};
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsService, AwsServiceError};
use fakecloud_persistence::SnapshotStore;

use crate::runtime::{RdsRuntime, RuntimeError};
use crate::state::{
    default_engine_versions, default_orderable_options, rds_arn, DbInstance, DbParameterGroup,
    DbSnapshot, DbSubnetGroup, EngineVersionInfo, OrderableDbInstanceOption, RdsSnapshot, RdsState,
    RdsTag, SharedRdsState, RDS_SNAPSHOT_SCHEMA_VERSION,
};
use fakecloud_core::data_volume::DataVolumeBinding;

const RDS_NS: &str = "http://rds.amazonaws.com/doc/2014-10-31/";

const SUPPORTED_ACTIONS: &[&str] = &[
    "AddRoleToDBCluster",
    "AddRoleToDBInstance",
    "AddSourceIdentifierToSubscription",
    "AddTagsToResource",
    "ApplyPendingMaintenanceAction",
    "AuthorizeDBSecurityGroupIngress",
    "BacktrackDBCluster",
    "CancelDBCapacityReservation",
    "CancelExportTask",
    "CopyDBClusterParameterGroup",
    "CopyDBClusterSnapshot",
    "CopyDBParameterGroup",
    "CopyDBSnapshot",
    "CopyOptionGroup",
    "CreateBlueGreenDeployment",
    "CreateCustomDBEngineVersion",
    "CreateDBCapacityReservation",
    "CreateDBCluster",
    "CreateDBClusterEndpoint",
    "CreateDBClusterParameterGroup",
    "CreateDBClusterSnapshot",
    "CreateDBInstance",
    "CreateDBInstanceReadReplica",
    "CreateDBParameterGroup",
    "CreateDBProxy",
    "CreateDBProxyEndpoint",
    "CreateDBSecurityGroup",
    "CreateDBShardGroup",
    "CreateDBSnapshot",
    "CreateDBSubnetGroup",
    "CreateEventSubscription",
    "CreateGlobalCluster",
    "CreateIntegration",
    "CreateOptionGroup",
    "CreateTenantDatabase",
    "DeleteBlueGreenDeployment",
    "DeleteCustomDBEngineVersion",
    "DeleteDBCluster",
    "DeleteDBClusterAutomatedBackup",
    "DeleteDBClusterEndpoint",
    "DeleteDBClusterParameterGroup",
    "DeleteDBClusterSnapshot",
    "DeleteDBInstance",
    "DeleteDBInstanceAutomatedBackup",
    "DeleteDBParameterGroup",
    "DeleteDBProxy",
    "DeleteDBProxyEndpoint",
    "DeleteDBSecurityGroup",
    "DeleteDBShardGroup",
    "DeleteDBSnapshot",
    "DeleteDBSubnetGroup",
    "DeleteEventSubscription",
    "DeleteGlobalCluster",
    "DeleteIntegration",
    "DeleteOptionGroup",
    "DeleteTenantDatabase",
    "DeregisterDBProxyTargets",
    "DescribeAccountAttributes",
    "DescribeBlueGreenDeployments",
    "DescribeCertificates",
    "DescribeDBCapacityReservations",
    "DescribeDBClusterAutomatedBackups",
    "DescribeDBClusterBacktracks",
    "DescribeDBClusterEndpoints",
    "DescribeDBClusterParameterGroups",
    "DescribeDBClusterParameters",
    "DescribeDBClusterSnapshotAttributes",
    "DescribeDBClusterSnapshots",
    "DescribeDBClusters",
    "DescribeDBEngineVersions",
    "DescribeDBInstanceAutomatedBackups",
    "DescribeDBInstances",
    "DescribeDBLogFiles",
    "DescribeDBMajorEngineVersions",
    "DescribeDBParameterGroups",
    "DescribeDBParameters",
    "DescribeDBProxies",
    "DescribeDBProxyEndpoints",
    "DescribeDBProxyTargetGroups",
    "DescribeDBProxyTargets",
    "DescribeDBRecommendations",
    "DescribeDBSecurityGroups",
    "DescribeDBShardGroups",
    "DescribeDBSnapshotAttributes",
    "DescribeDBSnapshotTenantDatabases",
    "DescribeDBSnapshots",
    "DescribeDBSubnetGroups",
    "DescribeEngineDefaultClusterParameters",
    "DescribeEngineDefaultParameters",
    "DescribeEventCategories",
    "DescribeEventSubscriptions",
    "DescribeEvents",
    "DescribeExportTasks",
    "DescribeGlobalClusters",
    "DescribeIntegrations",
    "DescribeOptionGroupOptions",
    "DescribeOptionGroups",
    "DescribeOrderableDBInstanceOptions",
    "DescribePendingMaintenanceActions",
    "DescribeReservedDBInstances",
    "DescribeReservedDBInstancesOfferings",
    "DescribeServerlessV2PlatformVersions",
    "DescribeSourceRegions",
    "DescribeTenantDatabases",
    "DescribeValidDBInstanceModifications",
    "DisableHttpEndpoint",
    "DownloadDBLogFilePortion",
    "EnableHttpEndpoint",
    "FailoverDBCluster",
    "FailoverGlobalCluster",
    "ListTagsForResource",
    "ModifyActivityStream",
    "ModifyCertificates",
    "ModifyCurrentDBClusterCapacity",
    "ModifyDBCapacityReservation",
    "ModifyCustomDBEngineVersion",
    "ModifyDBCluster",
    "ModifyDBClusterEndpoint",
    "ModifyDBClusterParameterGroup",
    "ModifyDBClusterSnapshotAttribute",
    "ModifyDBInstance",
    "ModifyDBParameterGroup",
    "ModifyDBProxy",
    "ModifyDBProxyEndpoint",
    "ModifyDBProxyTargetGroup",
    "ModifyDBRecommendation",
    "ModifyDBShardGroup",
    "ModifyDBSnapshot",
    "ModifyDBSnapshotAttribute",
    "ModifyDBSubnetGroup",
    "ModifyEventSubscription",
    "ModifyGlobalCluster",
    "ModifyIntegration",
    "ModifyOptionGroup",
    "ModifyTenantDatabase",
    "PromoteReadReplica",
    "PromoteReadReplicaDBCluster",
    "PurchaseReservedDBInstancesOffering",
    "RebootDBCluster",
    "RebootDBInstance",
    "RebootDBShardGroup",
    "RegisterDBProxyTargets",
    "RemoveFromGlobalCluster",
    "RemoveRoleFromDBCluster",
    "RemoveRoleFromDBInstance",
    "RemoveSourceIdentifierFromSubscription",
    "RemoveTagsFromResource",
    "ResetDBClusterParameterGroup",
    "ResetDBParameterGroup",
    "RestoreDBClusterFromS3",
    "RestoreDBClusterFromSnapshot",
    "RestoreDBClusterToPointInTime",
    "RestoreDBInstanceFromDBSnapshot",
    "RestoreDBInstanceFromS3",
    "RestoreDBInstanceToPointInTime",
    "RevokeDBSecurityGroupIngress",
    "StartActivityStream",
    "StartDBCluster",
    "StartDBInstance",
    "StartDBInstanceAutomatedBackupsReplication",
    "StartExportTask",
    "StopActivityStream",
    "StopDBCluster",
    "StopDBInstance",
    "StopDBInstanceAutomatedBackupsReplication",
    "SwitchoverBlueGreenDeployment",
    "SwitchoverGlobalCluster",
    "SwitchoverReadReplica",
];

pub struct RdsService {
    pub(crate) state: SharedRdsState,
    runtime: Option<Arc<RdsRuntime>>,
    snapshot_store: Option<Arc<dyn SnapshotStore>>,
    snapshot_lock: Arc<AsyncMutex<()>>,
    pub(crate) delivery_bus: Option<Arc<DeliveryBus>>,
    /// KMS access, so storage encrypted without a named key reports the
    /// account's AWS-managed `aws/rds` key and a named key reports its ARN.
    kms_hook: Option<Arc<dyn fakecloud_core::delivery::KmsHook>>,
    /// EC2 state: a DB subnet group's subnets resolve there (its `VpcId` and
    /// per-subnet Availability Zones). `None` in memory-only unit tests.
    ec2_state: Option<fakecloud_ec2::SharedEc2State>,
}

/// Source type for RDS EventBridge events. Maps `aws.rds` detail-type.
#[derive(Clone, Copy)]
#[allow(dead_code, clippy::enum_variant_names)]
pub(crate) enum RdsSourceType {
    DbInstance,
    DbSnapshot,
    DbParameterGroup,
    DbCluster,
    DbClusterSnapshot,
}

impl RdsSourceType {
    /// EventBridge `SourceType` enum string. Matches the SCREAMING_SNAKE
    /// form AWS publishes in the `aws.rds` event detail.
    fn as_str(self) -> &'static str {
        match self {
            Self::DbInstance => "DB_INSTANCE",
            Self::DbSnapshot => "DB_SNAPSHOT",
            Self::DbParameterGroup => "DB_PARAMETER_GROUP",
            Self::DbCluster => "DB_CLUSTER",
            Self::DbClusterSnapshot => "DB_CLUSTER_SNAPSHOT",
        }
    }

    /// `DescribeEvents` `SourceType` filter / response value. Per AWS
    /// API spec this is the kebab-case form (`db-instance`,
    /// `db-cluster`, `db-snapshot`, `db-parameter-group`, ...) — distinct
    /// from the EventBridge `SourceType` returned by [`Self::as_str`].
    pub(crate) fn describe_events_str(self) -> &'static str {
        match self {
            Self::DbInstance => "db-instance",
            Self::DbSnapshot => "db-snapshot",
            Self::DbParameterGroup => "db-parameter-group",
            Self::DbCluster => "db-cluster",
            Self::DbClusterSnapshot => "db-cluster-snapshot",
        }
    }

    fn detail_type(self) -> &'static str {
        match self {
            Self::DbInstance => "RDS DB Instance Event",
            Self::DbSnapshot => "RDS DB Snapshot Event",
            Self::DbParameterGroup => "RDS DB Parameter Group Event",
            Self::DbCluster => "RDS DB Cluster Event",
            Self::DbClusterSnapshot => "RDS DB Cluster Snapshot Event",
        }
    }
}

mod capacity_reservations;
mod cluster_snapshots;
mod engine;
mod instances;
mod log_files;
mod parameter_groups;
mod replicas;
mod restore;
mod snapshots;
mod subnet_groups;
mod tags;

impl RdsService {
    pub(crate) fn state_handle(&self) -> &SharedRdsState {
        &self.state
    }
}

impl RdsService {
    pub fn new(state: SharedRdsState) -> Self {
        Self {
            state,
            runtime: None,
            snapshot_store: None,
            snapshot_lock: Arc::new(AsyncMutex::new(())),
            delivery_bus: None,
            kms_hook: None,
            ec2_state: None,
        }
    }

    pub fn with_ec2_state(mut self, ec2_state: fakecloud_ec2::SharedEc2State) -> Self {
        self.ec2_state = Some(ec2_state);
        self
    }

    pub fn with_kms_hook(mut self, hook: Arc<dyn fakecloud_core::delivery::KmsHook>) -> Self {
        self.kms_hook = Some(hook);
        self
    }

    /// The key storage encrypted in `region` is reported under: the ARN of
    /// `named` (resolved through KMS, ignored when empty) or, with no key
    /// named, the account's AWS-managed `aws/rds` key for the region. `None`
    /// without a KMS hook and no key named. Resolve before taking the RDS
    /// state lock (KMS may mint and persist the key).
    pub(crate) fn storage_kms_key(
        &self,
        named: Option<&str>,
        account_id: &str,
        region: &str,
    ) -> Option<String> {
        fakecloud_core::delivery::kms_key_arn_or_aws_managed(
            self.kms_hook.as_deref(),
            named,
            account_id,
            region,
            "rds",
        )
    }

    /// A create/restore request's `StorageEncrypted` and the key it reports:
    /// encrypted storage uses [`Self::storage_kms_key`] (the named key's ARN
    /// or the AWS-managed `aws/rds` key); unencrypted storage keeps any named
    /// key as given.
    pub(crate) fn requested_storage_encryption(
        &self,
        request: &AwsRequest,
    ) -> Result<(bool, Option<String>), AwsServiceError> {
        let encrypted = service_helpers::parse_optional_bool(
            optional_query_param(request, "StorageEncrypted").as_deref(),
        )?
        .unwrap_or(false);
        let named = optional_query_param(request, "KmsKeyId");
        if !encrypted {
            return Ok((false, named));
        }
        Ok((
            true,
            self.storage_kms_key(named.as_deref(), &request.account_id, &request.region),
        ))
    }

    /// For a cluster row (or cluster-shaped row) with `StorageEncrypted`
    /// set, report its storage key as AWS does: the named `KmsKeyId` as its
    /// key ARN, or the AWS-managed `aws/rds` key when none is named. An
    /// unencrypted row is left as is.
    pub(crate) fn resolve_cluster_storage_key(
        &self,
        obj: &mut serde_json::Map<String, serde_json::Value>,
        account_id: &str,
        region: &str,
    ) {
        if obj.get("StorageEncrypted").and_then(|v| v.as_bool()) != Some(true) {
            return;
        }
        let named = obj
            .get("KmsKeyId")
            .and_then(|v| v.as_str())
            .map(str::to_string);
        if let Some(key) = self.storage_kms_key(named.as_deref(), account_id, region) {
            obj.insert("KmsKeyId".to_string(), serde_json::Value::String(key));
        }
    }

    pub fn with_runtime(mut self, runtime: Arc<RdsRuntime>) -> Self {
        self.runtime = Some(runtime);
        self
    }

    /// Crate-internal accessor for the optional runtime; needed by the
    /// extras handler so cluster snapshot/restore paths can dump and
    /// replay member databases via the live container runtime.
    pub(crate) fn runtime_ref(&self) -> Option<&Arc<RdsRuntime>> {
        self.runtime.as_ref()
    }

    pub fn with_snapshot_store(mut self, store: Arc<dyn SnapshotStore>) -> Self {
        self.snapshot_store = Some(store);
        self
    }

    pub fn with_delivery_bus(mut self, bus: Arc<DeliveryBus>) -> Self {
        self.delivery_bus = Some(bus);
        self
    }

    /// Emit an `aws.rds` EventBridge event mirroring the AWS RDS event schema.
    /// Also records into the per-account events ring so DescribeEvents
    /// can serve the row. No-op for the EventBridge side when the bus
    /// isn't wired (tests, minimal configs).
    pub(crate) fn emit_event(
        &self,
        source_type: RdsSourceType,
        source_identifier: &str,
        source_arn: &str,
        event_id: &str,
        event_categories: &[&str],
        message: &str,
    ) {
        // Source the account_id off the source_arn (segment 4) — that's
        // the canonical ARN form for RDS resources.
        let account_id = source_arn.split(':').nth(4).unwrap_or("");
        emit_event_static_with_state(
            self.delivery_bus.as_ref(),
            Some(&self.state),
            if account_id.is_empty() {
                None
            } else {
                Some(account_id)
            },
            source_type,
            source_identifier,
            source_arn,
            event_id,
            event_categories,
            message,
        );
    }

    async fn save_snapshot(&self) {
        save_snapshot_static(
            self.state.clone(),
            self.snapshot_store.clone(),
            self.snapshot_lock.clone(),
        )
        .await;
    }

    /// Settle which data volume each persisted instance mounts before any
    /// container is recreated. Instances persisted without a binding (state
    /// written before volumes were scoped to the data dir) are bound against
    /// the daemon's volumes, keeping a legacy volume when one exists, and the
    /// bindings are persisted; every legacy binding is then registered with
    /// the runtime. If the daemon can't list volumes, the unbound instances
    /// stay unbound until a start where it can.
    async fn resolve_data_volumes(&self, runtime: &RdsRuntime) {
        let unbound = self
            .state
            .read()
            .iter()
            .any(|(_, s)| s.instances.values().any(|i| i.data_volume.is_none()));
        if unbound {
            if let Some(existing) = runtime.list_volumes().await {
                let tag = fakecloud_core::data_volume::current_scope().tag();
                let changed = {
                    let mut accounts = self.state.write();
                    let mut changed = false;
                    for (_, state) in accounts.iter_mut() {
                        changed |= state.resolve_data_volumes(tag, &existing);
                    }
                    changed
                };
                if changed {
                    self.save_snapshot().await;
                }
            }
        }
        let accounts = self.state.read();
        for (_, state) in accounts.iter() {
            for (id, inst) in &state.instances {
                if let Some(DataVolumeBinding::Legacy(volume)) = &inst.data_volume {
                    tracing::info!(
                        db_instance_identifier = %id,
                        volume = %volume,
                        "rds instance keeps its pre-scoping data volume",
                    );
                }
            }
        }
    }

    /// Recreate the backing Docker/Podman containers for persisted DB
    /// instances after a fakecloud restart. Without this, persistent
    /// mode loads the row back into memory with `db_instance_status =
    /// available` but the container is gone, so the endpoint is dead
    /// (issue #1338). Each candidate is flipped to `starting`
    /// synchronously, then a background task brings the container back
    /// and flips it back to `available`. Instances persisted as
    /// `stopped` are skipped — `StartDBInstance` revives those.
    pub async fn recover_persisted_containers(&self) {
        let Some(runtime) = self.runtime.clone() else {
            return;
        };
        self.resolve_data_volumes(&runtime).await;

        struct Pending {
            account_id: String,
            region: String,
            id: String,
            incarnation: String,
            volume: String,
            engine: String,
            engine_version: String,
            username: String,
            password: String,
            db_name: String,
            tags: Vec<crate::state::RdsTag>,
        }

        let pending: Vec<Pending> = {
            let mut accounts = self.state.write();
            let mut out = Vec::new();
            for (_, state) in accounts.iter_mut() {
                let account_id = state.account_id.clone();
                let region = state.region.clone();
                for (id, inst) in state.instances.iter_mut() {
                    // "creating" is included so an instance whose background
                    // create task hadn't finished (and re-saved it as
                    // "available") when the process crashed is resumed rather
                    // than silently dropped on restart — the API already
                    // returned it to the client, so DescribeDBInstances must
                    // not lose it (bug-hunt 2026-06-13, finding 4.3). Recovery
                    // re-drives it through `ensure_*` to a live container.
                    if !matches!(
                        inst.db_instance_status.as_str(),
                        "creating"
                            | "available"
                            | "starting"
                            | "modifying"
                            | "rebooting"
                            | "backing-up"
                    ) {
                        continue;
                    }
                    // Still unbound: the daemon couldn't list volumes to say
                    // whether a pre-scoping volume holds its data. Mounting
                    // now would create (and from then on prefer) an empty
                    // scoped volume, so don't; report it `stopped` (it has no
                    // container) so StartDBInstance can retry once the
                    // daemon answers.
                    if inst.data_volume.is_none() && runtime.has_data_volumes() {
                        tracing::warn!(
                            db_instance_identifier = %id,
                            "not recovering rds instance: its data volume could not be resolved",
                        );
                        inst.db_instance_status = "stopped".to_string();
                        continue;
                    }
                    inst.db_instance_status = "starting".to_string();
                    out.push(Pending {
                        account_id: account_id.clone(),
                        region: region.clone(),
                        id: id.clone(),
                        incarnation: inst.dbi_resource_id.clone(),
                        volume: inst.data_volume_name(
                            fakecloud_core::data_volume::current_scope().tag(),
                            &account_id,
                        ),
                        engine: inst.engine.clone(),
                        engine_version: inst.engine_version.clone(),
                        username: inst.master_username.clone(),
                        password: inst.master_user_password.clone(),
                        db_name: inst
                            .db_name
                            .clone()
                            .unwrap_or_else(|| default_db_name(&inst.engine).to_string()),
                        tags: inst.tags.clone(),
                    });
                }
            }
            out
        };

        if pending.is_empty() {
            return;
        }
        tracing::info!(
            count = pending.len(),
            "recovering backing containers for persisted rds instances",
        );

        for p in pending {
            let runtime = runtime.clone();
            let state = self.state.clone();
            let snapshot_store = self.snapshot_store.clone();
            let snapshot_lock = self.snapshot_lock.clone();
            let delivery_bus = self.delivery_bus.clone();
            tokio::spawn(async move {
                match runtime
                    .ensure_postgres(
                        &p.incarnation,
                        &p.id,
                        &p.engine,
                        &p.engine_version,
                        &p.username,
                        &p.password,
                        &p.db_name,
                        &p.account_id,
                        &p.region,
                        &p.tags,
                        &p.volume,
                    )
                    .await
                {
                    Ok(running) => {
                        let current = {
                            let mut accounts = state.write();
                            accounts
                                .get_mut(&p.account_id)
                                .and_then(|s| s.instance_by_incarnation_mut(&p.incarnation))
                                .map(|inst| {
                                    inst.db_instance_status = "available".to_string();
                                    inst.endpoint_address = running.endpoint_address.clone();
                                    inst.port = i32::from(running.endpoint_port);
                                    inst.host_port = running.host_port;
                                    inst.container_id = running.container_id;
                                    (
                                        inst.db_instance_identifier.clone(),
                                        inst.db_instance_arn.clone(),
                                    )
                                })
                        };
                        let Some((current_id, current_arn)) = current else {
                            // Deleted (or reset) while recovering.
                            reap_gone_start(&runtime, &p.incarnation, &p.volume).await;
                            return;
                        };
                        save_snapshot_static(
                            state.clone(),
                            snapshot_store.clone(),
                            snapshot_lock.clone(),
                        )
                        .await;
                        emit_event_static(
                            delivery_bus.as_ref(),
                            RdsSourceType::DbInstance,
                            &current_id,
                            &current_arn,
                            "RDS-EVENT-0088",
                            &["notification"],
                            "DB instance restarted after fakecloud restart",
                        );
                    }
                    Err(error) => {
                        tracing::error!(
                            %error,
                            db_instance_identifier = %p.id,
                            "failed to recover rds backing container after restart",
                        );
                        let present = {
                            let mut accounts = state.write();
                            match accounts
                                .get_mut(&p.account_id)
                                .and_then(|s| s.instance_by_incarnation_mut(&p.incarnation))
                            {
                                Some(inst) => {
                                    inst.db_instance_status = "failed".to_string();
                                    true
                                }
                                None => false,
                            }
                        };
                        if !present {
                            // Deleted while recovering: the failed start may
                            // still have created the volume after the delete
                            // removed it.
                            reap_gone_start(&runtime, &p.incarnation, &p.volume).await;
                            return;
                        }
                        save_snapshot_static(state, snapshot_store, snapshot_lock).await;
                    }
                }
            });
        }
    }

    /// Reconcile DB snapshots persisted mid-dump (`creating`) after a restart.
    ///
    /// `CreateDBSnapshot` and the final-snapshot-on-delete path insert a
    /// `creating` row synchronously, then a detached task runs the (unbounded)
    /// dump and flips it to `available`. The dispatch-level auto-save can
    /// persist the `creating` row, so a crash mid-dump leaves the snapshot
    /// `creating` forever — never usable, id blocked, data lost. On load we
    /// reconcile each such row:
    ///   * source instance still present -> re-arm the dump finalizer
    ///     (re-run the dump -> `available`);
    ///   * source gone (the final-snapshot path removed the instance row
    ///     synchronously before deferring teardown) -> mark the snapshot
    ///     `failed` so the id is reusable and Describe shows a terminal state,
    ///     and reap the orphaned source container + data volume the
    ///     interrupted finalizer would have torn down (otherwise leaked).
    ///
    /// Mirrors how ACM/CloudFront re-arm their in-flight pending states on
    /// load. The `failed`-marking is applied synchronously (persisted here);
    /// the runtime-driven dump/reap run in detached tasks like the primary
    /// recovery path.
    pub async fn reconcile_inflight_snapshots(&self) {
        let (rearm, reap) = self.plan_snapshot_recovery(self.runtime.is_some());
        if rearm.is_empty() && reap.is_empty() {
            return;
        }
        // Persist the `failed` transitions the planner applied.
        self.save_snapshot().await;
        let Some(runtime) = self.runtime.clone() else {
            return;
        };
        for r in rearm {
            tracing::info!(
                db_instance_identifier = %r.source_id,
                snapshot = %r.snapshot_id,
                "re-arming an in-flight snapshot dump after restart",
            );
            let source_key = r.incarnation.clone();
            self.spawn_finalize_snapshot(
                runtime.clone(),
                r.account_id,
                r.snapshot_id,
                r.snapshot_created,
                source_key,
                r.engine,
                r.username,
                r.password,
                r.db_name,
                // Source instance is present (a regular in-flight snapshot);
                // no deferred teardown to run.
                None,
            );
        }
        for r in reap {
            // Complete the deferred final-snapshot teardown the crash
            // interrupted: reap the orphaned source container + data volume.
            // Idempotent — a no-op if the runtime already reaped them.
            tracing::info!(
                account_id = %r.account_id,
                db_instance_identifier = %r.source_id,
                dbi_resource_id = %r.dbi_resource_id,
                "reaping the backing container of an interrupted RDS delete"
            );
            let runtime = runtime.clone();
            tokio::spawn(async move {
                runtime.stop(&r.dbi_resource_id).await;
                // The row is gone and this process never mounted the volume:
                // the final snapshot recorded which one it was (an adopted
                // legacy volume included); older snapshots predate that and
                // name the scoped one from the resource id.
                let volume = r.data_volume.unwrap_or_else(|| {
                    crate::runtime::scoped_data_volume_name(
                        fakecloud_core::data_volume::current_scope().tag(),
                        &r.account_id,
                        &r.dbi_resource_id,
                    )
                });
                runtime.remove_data_volume_named(&volume).await;
            });
        }
    }

    /// Pure state reconcile for `creating` snapshots: marks the un-recoverable
    /// ones `failed` and returns the set to re-arm plus the orphaned
    /// final-snapshot sources to reap. Split out for Docker-free unit testing.
    /// A snapshot is re-armed only when its source instance still exists AND a
    /// runtime is wired (`has_runtime`); otherwise it transitions to the
    /// terminal `failed`. `has_runtime` is passed in so the re-arm branch is
    /// exercisable in Docker-free unit tests.
    pub(crate) fn plan_snapshot_recovery(
        &self,
        has_runtime: bool,
    ) -> (Vec<SnapshotRearm>, Vec<SnapshotReap>) {
        let mut rearm = Vec::new();
        let mut reap = Vec::new();
        let mut accounts = self.state.write();
        let tag = fakecloud_core::data_volume::current_scope().tag();
        for (_, state) in accounts.iter_mut() {
            let account_id = state.account_id.clone();
            // An instance persisted as `deleting` was mid-DeleteDBInstance (its
            // final snapshot recorded, the row not yet removed) when the
            // process died. Finish the delete: drop the row and reap its
            // container and volume. Its in-flight final snapshot can't be
            // dumped any more and is failed below like any orphaned one.
            let deleting: Vec<String> = state
                .instances
                .iter()
                .filter(|(_, i)| i.db_instance_status == "deleting")
                .map(|(id, _)| id.clone())
                .collect();
            for id in deleting {
                if let Some(inst) = state.instances.remove(&id) {
                    reap.push(SnapshotReap {
                        account_id: account_id.clone(),
                        source_id: id,
                        dbi_resource_id: inst.dbi_resource_id.clone(),
                        data_volume: Some(inst.data_volume_name(tag, &account_id)),
                    });
                }
            }
            // Snapshot the present instance ids so the immutable borrow ends
            // before we mutate `state.snapshots`.
            let instances: std::collections::HashMap<String, String> = state
                .instances
                .iter()
                .map(|(id, i)| (id.clone(), i.dbi_resource_id.clone()))
                .collect();
            for (id, snap) in state.snapshots.iter_mut() {
                if snap.status != "creating" {
                    continue;
                }
                // Present only as the same incarnation the snapshot was taken
                // from: a recreate under the identifier is a different
                // instance, which must neither be dumped into this snapshot
                // nor keep the deleted one's volume from being reaped.
                let source_present = instances
                    .get(&snap.db_instance_identifier)
                    .is_some_and(|dbi| *dbi == snap.dbi_resource_id);
                if source_present && has_runtime {
                    let db_name = snap
                        .db_name
                        .clone()
                        .unwrap_or_else(|| default_db_name(&snap.engine).to_string());
                    rearm.push(SnapshotRearm {
                        account_id: account_id.clone(),
                        snapshot_id: id.clone(),
                        snapshot_created: snap.snapshot_create_time,
                        incarnation: instances[&snap.db_instance_identifier].clone(),
                        source_id: snap.db_instance_identifier.clone(),
                        engine: snap.engine.clone(),
                        username: snap.master_username.clone(),
                        password: snap.master_user_password.clone(),
                        db_name,
                    });
                } else {
                    snap.status = "failed".to_string();
                    let already_reaped = reap.iter().any(|r| {
                        r.account_id == account_id && r.dbi_resource_id == snap.dbi_resource_id
                    });
                    if !source_present && !already_reaped {
                        reap.push(SnapshotReap {
                            account_id: account_id.clone(),
                            source_id: snap.db_instance_identifier.clone(),
                            dbi_resource_id: snap.dbi_resource_id.clone(),
                            data_volume: snap.source_data_volume.clone(),
                        });
                    }
                }
            }
        }
        (rearm, reap)
    }

    /// Stop the backing container for `db_instance_identifier` and mark
    /// the row `stopped`. Synchronous wrt the runtime so callers see a
    /// real `stopped` status by the time the response goes back; mirrors
    /// the AWS contract that `StartDBInstance`/`StopDBInstance` change
    /// the visible status immediately.
    async fn stop_db_instance(&self, request: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let db_instance_identifier = required_query_param(request, "DBInstanceIdentifier")?;
        let detach_from_reservation = service_helpers::parse_optional_bool(
            optional_query_param(request, "DetachFromDBCapacityReservation").as_deref(),
        )?
        .unwrap_or(false);

        // Look up the instance first so a missing identifier returns the
        // declared `DBInstanceNotFoundFault` error rather than a 503 from a
        // missing container runtime. Conformance probes hit Start/Stop with
        // synthetic identifiers and expect the documented error shape.
        let incarnation = {
            let accounts = self.state.read();
            let empty = RdsState::new(&request.account_id, &request.region);
            let state = accounts.get(&request.account_id).unwrap_or(&empty);
            state
                .instances
                .get(&db_instance_identifier)
                .map(|i| i.dbi_resource_id.clone())
                .ok_or_else(|| db_instance_not_found(&db_instance_identifier))?
        };

        if let Some(runtime) = self.runtime.as_ref() {
            runtime.stop(&incarnation).await;
        }

        // Re-resolve by incarnation: a delete-and-recreate under the same
        // identifier during the stop must not mark the replacement stopped.
        let instance = {
            let mut accounts = self.state.write();
            let state = accounts.get_or_create(&request.account_id);
            let inst = state
                .instance_by_incarnation_mut(&incarnation)
                .ok_or_else(|| db_instance_not_found(&db_instance_identifier))?;
            inst.db_instance_status = "stopped".to_string();
            inst.container_id = String::new();
            // Every stop releases the unit; detaching also drops the
            // reservation the instance would return to on start.
            if detach_from_reservation {
                inst.capacity_reservation = None;
            }
            inst.clone()
        };

        // The row as it is now (a rename during the stop changed its
        // identifier and ARN), not the values read before the await.
        self.emit_event(
            RdsSourceType::DbInstance,
            &instance.db_instance_identifier,
            &instance.db_instance_arn,
            "RDS-EVENT-0089",
            &["notification"],
            "DB instance stopped",
        );

        Ok(AwsResponse::xml(
            StatusCode::OK,
            query_response_xml(
                "StopDBInstance",
                RDS_NS,
                &format!(
                    "<DBInstance>{}</DBInstance>",
                    db_instance_xml(
                        &instance,
                        Some("stopped"),
                        self.subnet_group_of(&request.account_id, &instance)
                            .as_ref(),
                    )
                ),
                &request.request_id,
            ),
        ))
    }

    /// Restart a stopped DB instance: spin up the backing container and
    /// flip the row back to `available`. Mirrors AWS `StartDBInstance`.
    async fn start_db_instance(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let db_instance_identifier = required_query_param(request, "DBInstanceIdentifier")?;

        // Look up the instance first so a missing identifier returns the
        // declared `DBInstanceNotFoundFault` error rather than a 503 from a
        // missing container runtime.
        let instance = {
            let accounts = self.state.read();
            let empty = RdsState::new(&request.account_id, &request.region);
            let state = accounts.get(&request.account_id).unwrap_or(&empty);
            state
                .instances
                .get(&db_instance_identifier)
                .cloned()
                .ok_or_else(|| db_instance_not_found(&db_instance_identifier))?
        };

        // Every later step re-resolves the row by this incarnation, never by
        // the reusable identifier: the awaits below can race a delete or a
        // delete-and-recreate under the same name.
        let incarnation = instance.dbi_resource_id.clone();
        let prior_status = instance.db_instance_status.clone();
        let requested_reservation =
            optional_query_param(request, "DBCapacityReservationIdentifier");

        // Flip to `starting` so concurrent DescribeDBInstances callers
        // see the in-flight state, placing the instance on the reservation
        // it names or kept (or on unreserved capacity when that one is no
        // longer active).
        {
            let mut accounts = self.state.write();
            let state = accounts.get_or_create(&request.account_id);
            let capacity = match state.instances.get(&db_instance_identifier) {
                Some(current) => capacity_reservations::capacity_on_start(
                    state,
                    current,
                    requested_reservation.as_deref(),
                )?,
                None => None,
            };
            if let Some(inst) = state.instance_by_incarnation_mut(&incarnation) {
                inst.db_instance_status = "starting".to_string();
                inst.capacity_reservation = capacity;
            }
        }

        // No container runtime configured: fail synchronously (fast) rather
        // than report success against a backend that does not exist, rolling
        // the row back from `starting` so subsequent calls don't see a stuck
        // state.
        let Some(runtime) = self.runtime.clone() else {
            {
                let mut accounts = self.state.write();
                let state = accounts.get_or_create(&request.account_id);
                if let Some(inst) = state.instance_by_incarnation_mut(&incarnation) {
                    inst.db_instance_status = "stopped".to_string();
                }
            }
            return Err(AwsServiceError::aws_error(
                StatusCode::SERVICE_UNAVAILABLE,
                "InternalFailure",
                "Container runtime is not configured; cannot start DB instance",
            ));
        };

        // A pre-scoping instance whose data volume couldn't be bound at startup
        // (the daemon couldn't list volumes) is bound now; starting it on a
        // guessed volume would shadow its legacy data with an empty one.
        if instance.data_volume.is_none() && runtime.has_data_volumes() {
            self.resolve_data_volumes(&runtime).await;
        }

        // Read the row back after the resolution: its binding may have just
        // become `legacy(name)`, and the volume to mount comes from that, not
        // from the pre-resolution copy.
        let (instance, volume) = start_target(
            &self.state.read(),
            &request.account_id,
            &incarnation,
            fakecloud_core::data_volume::current_scope().tag(),
        )
        .ok_or_else(|| db_instance_not_found(&db_instance_identifier))?;
        if instance.data_volume.is_none() && runtime.has_data_volumes() {
            let mut accounts = self.state.write();
            let state = accounts.get_or_create(&request.account_id);
            if let Some(inst) = state.instance_by_incarnation_mut(&incarnation) {
                inst.db_instance_status = prior_status;
            }
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidDBInstanceState",
                format!(
                    "DB instance {db_instance_identifier} cannot be started: its data \
                     volume could not be resolved because the container runtime did \
                     not list its volumes. Retry once it is reachable."
                ),
            ));
        }

        // Background the container start + readiness wait and return
        // immediately with `starting`. `ensure_postgres` can pull a cold image
        // and wait for engine readiness (3-6 min for Oracle/SQL Server/Db2),
        // far past the ~60s client read timeout — awaiting it inline timed the
        // CLI out (bug-hunt 2026-06-24, 3.2). CreateDBInstance already
        // backgrounds this exact call.
        {
            let state_handle = self.state.clone();
            let delivery_bus = self.delivery_bus.clone();
            let snapshot_store = self.snapshot_store.clone();
            let snapshot_lock = self.snapshot_lock.clone();
            let id = instance.db_instance_identifier.clone();
            let account_id = request.account_id.clone();
            let region = request.region.clone();
            let inst = instance.clone();
            let volume = volume.clone();
            tokio::spawn(async move {
                let logical_db = inst
                    .db_name
                    .clone()
                    .unwrap_or_else(|| default_db_name(&inst.engine).to_string());
                match runtime
                    .ensure_postgres(
                        &inst.dbi_resource_id,
                        &id,
                        &inst.engine,
                        &inst.engine_version,
                        &inst.master_username,
                        &inst.master_user_password,
                        &logical_db,
                        &account_id,
                        &region,
                        &inst.tags,
                        &volume,
                    )
                    .await
                {
                    Ok(r) => {
                        let current = {
                            let mut accounts = state_handle.write();
                            let state = accounts.get_or_create(&account_id);
                            state
                                .instance_by_incarnation_mut(&inst.dbi_resource_id)
                                .map(|row| {
                                    row.db_instance_status = "available".to_string();
                                    row.endpoint_address = r.endpoint_address.clone();
                                    row.port = i32::from(r.endpoint_port);
                                    row.host_port = r.host_port;
                                    row.container_id = r.container_id.clone();
                                    (
                                        row.db_instance_identifier.clone(),
                                        row.db_instance_arn.clone(),
                                    )
                                })
                        };
                        let Some((id, arn)) = current else {
                            // Deleted (or reset) while starting.
                            reap_gone_start(&runtime, &inst.dbi_resource_id, &volume).await;
                            return;
                        };
                        emit_event_static_with_state(
                            delivery_bus.as_ref(),
                            Some(&state_handle),
                            Some(&account_id),
                            RdsSourceType::DbInstance,
                            &id,
                            &arn,
                            "RDS-EVENT-0088",
                            &["notification"],
                            "DB instance started",
                        );
                        save_snapshot_static(state_handle.clone(), snapshot_store, snapshot_lock)
                            .await;
                    }
                    Err(_) => {
                        // Roll back to `stopped` so the next Describe doesn't
                        // report a permanently-`starting` instance. If the
                        // instance was deleted meanwhile, the failed start may
                        // have created its volume after the delete removed it.
                        let present = {
                            let mut accounts = state_handle.write();
                            let state = accounts.get_or_create(&account_id);
                            match state.instance_by_incarnation_mut(&inst.dbi_resource_id) {
                                Some(row) => {
                                    row.db_instance_status = "stopped".to_string();
                                    true
                                }
                                None => false,
                            }
                        };
                        if !present {
                            reap_gone_start(&runtime, &inst.dbi_resource_id, &volume).await;
                        }
                    }
                }
            });
        }

        Ok(AwsResponse::xml(
            StatusCode::OK,
            query_response_xml(
                "StartDBInstance",
                RDS_NS,
                &format!(
                    "<DBInstance>{}</DBInstance>",
                    db_instance_xml(
                        &instance,
                        Some("starting"),
                        self.subnet_group_of(&request.account_id, &instance)
                            .as_ref(),
                    )
                ),
                &request.request_id,
            ),
        ))
    }
}

/// The row a StartDBInstance acts on, re-read by incarnation, and the data
/// volume it mounts, named from the row's current binding. Read after the
/// lazy legacy-volume resolution so a just-adopted legacy volume is mounted
/// rather than an empty scoped one; `None` if the instance is gone.
pub(crate) fn start_target(
    accounts: &fakecloud_core::multi_account::MultiAccountState<RdsState>,
    account_id: &str,
    incarnation: &str,
    scope_tag: &str,
) -> Option<(DbInstance, String)> {
    let inst = accounts
        .get(account_id)?
        .instances
        .values()
        .find(|i| i.dbi_resource_id == incarnation)?
        .clone();
    let volume = inst.data_volume_name(scope_tag, account_id);
    Some((inst, volume))
}

/// A start task's container came up, but the instance incarnation it was
/// started for is gone (deleted or reset while it booted): remove the
/// container, and the data volume it may have (re)created, which the delete
/// couldn't remove while it was mounted. Keyed by incarnation, so a new
/// instance that reuses the identifier is never reached.
pub(crate) async fn reap_gone_start(runtime: &RdsRuntime, incarnation: &str, volume: &str) {
    runtime.stop(incarnation).await;
    runtime.remove_data_volume_named(volume).await;
}

/// Persist the current `RdsState` to the configured snapshot store. Free
/// function so background tasks (e.g. the create-DB-instance container-start
/// task) can save without holding a `&RdsService`. Returns immediately when
/// Whether the given AWS error code is in the AddTagsToResource Smithy
/// model's declared error set. Used so undeclared `*NotFound` codes
/// (OptionGroup, ParameterGroup, EventSubscription, SecurityGroup) get
/// swallowed into a no-op rather than surfaced as a non-modelled error.
fn is_declared_add_tags_not_found(code: &str) -> bool {
    matches!(
        code,
        "BlueGreenDeploymentNotFoundFault"
            | "DBCapacityReservationNotFound"
            | "DBClusterNotFoundFault"
            | "DBInstanceNotFound"
            | "DBProxyEndpointNotFoundFault"
            | "DBProxyNotFoundFault"
            | "DBProxyTargetGroupNotFoundFault"
            | "DBShardGroupNotFound"
            | "DBSnapshotNotFound"
            | "DBSnapshotTenantDatabaseNotFoundFault"
            | "IntegrationNotFoundFault"
            | "InvalidDBClusterEndpointStateFault"
            | "InvalidDBClusterStateFault"
            | "InvalidDBInstanceState"
            | "TenantDatabaseNotFound"
    )
}

/// Apply the outcome of a backgrounded snapshot dump to the stored row.
/// Split out from `spawn_finalize_snapshot` so the state transition
/// (`creating` -> `available`/`failed`) is unit-testable without a container
/// runtime. A snapshot deleted while the dump was in flight is simply left
/// untouched (the `get_mut` misses).
fn apply_snapshot_dump_result(
    state: &SharedRdsState,
    account_id: &str,
    snapshot_id: &str,
    snapshot_created: chrono::DateTime<Utc>,
    result: Result<Vec<u8>, RuntimeError>,
) {
    let mut accounts = state.write();
    let s = accounts.get_or_create(account_id);
    // Only the row this dump was started for: a snapshot deleted and
    // recreated under the same id meanwhile is another snapshot.
    let Some(snapshot) = s
        .snapshots
        .get_mut(snapshot_id)
        .filter(|snap| snap.snapshot_create_time == snapshot_created && snap.status == "creating")
    else {
        return;
    };
    match result {
        Ok(data) => {
            snapshot.dump_data = data;
            snapshot.status = "available".to_string();
            snapshot.percent_progress = Some(100);
        }
        Err(error) => {
            tracing::error!(%error, snapshot = %snapshot_id, "snapshot dump failed");
            snapshot.status = "failed".to_string();
        }
    }
}

/// A `creating` DB snapshot whose source instance still exists, so its dump
/// can be re-run on restart. Produced by `plan_snapshot_recovery`.
pub(crate) struct SnapshotRearm {
    pub(crate) account_id: String,
    pub(crate) snapshot_id: String,
    /// The snapshot row's creation time: which incarnation of the snapshot
    /// id the re-armed dump belongs to.
    pub(crate) snapshot_created: chrono::DateTime<Utc>,
    /// The source instance's incarnation (`DbiResourceId`), its container key.
    pub(crate) incarnation: String,
    pub(crate) source_id: String,
    pub(crate) engine: String,
    pub(crate) username: String,
    pub(crate) password: String,
    pub(crate) db_name: String,
}

/// A final-snapshot source whose instance row was removed synchronously by
/// `DeleteDBInstance` but whose backing container + data volume were left for
/// the (interrupted) finalizer to reap. Reaped on restart to avoid a leak.
pub(crate) struct SnapshotReap {
    pub(crate) account_id: String,
    pub(crate) source_id: String,
    /// The source instance's resource id, which its data volume is named by.
    pub(crate) dbi_resource_id: String,
    /// The source instance's data volume, as its final snapshot recorded it.
    pub(crate) data_volume: Option<String>,
}

/// no store is configured (memory-mode runs).
async fn save_snapshot_static(
    state: SharedRdsState,
    store: Option<Arc<dyn SnapshotStore>>,
    lock: Arc<AsyncMutex<()>>,
) {
    let Some(store) = store else {
        return;
    };
    let _guard = lock.lock().await;
    let snapshot = RdsSnapshot {
        schema_version: RDS_SNAPSHOT_SCHEMA_VERSION,
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
        Ok(Err(err)) => tracing::error!(%err, "failed to write rds snapshot"),
        Err(err) => tracing::error!(%err, "rds snapshot task panicked"),
    }
}

impl RdsService {
    /// Return the runtime or a ``ServiceUnavailable`` error if it was not configured.
    ///
    /// RDS operations that start, stop, or reach into a database container fail
    /// with a consistent wire error when the daemon (Docker/Podman) is missing
    /// rather than each caller restating the message.
    /// Resolve the container runtime or return a declared error shape.
    /// `InsufficientDBInstanceCapacity` is declared on every op that
    /// calls `require_runtime` (Create/Modify/Restore* DB instance and
    /// Read Replica), and is the closest Smithy-modelled analogue for
    /// "fakecloud can't satisfy this DB request right now".
    /// Look up the subnet group an instance sits in, for render paths
    /// that hold a cloned `DbInstance` and no longer have the state lock
    /// open. Callers that still hold the lock use
    /// [`instance_subnet_group`] instead.
    fn subnet_group_of(&self, account_id: &str, instance: &DbInstance) -> Option<DbSubnetGroup> {
        let name = instance.db_subnet_group_name.as_ref()?;
        let accounts = self.state.read();
        accounts.get(account_id)?.subnet_groups.get(name).cloned()
    }

    fn require_runtime(&self) -> Result<&Arc<RdsRuntime>, AwsServiceError> {
        self.runtime.as_ref().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::SERVICE_UNAVAILABLE,
                "InsufficientDBInstanceCapacity",
                format!(
                    "Docker/Podman is required for RDS DB instances but is not available. {}",
                    fakecloud_core::container_net::CONTAINER_RUNTIME_HINT
                ),
            )
        })
    }

    /// Background the container start + optional data-replay for a
    /// restore/replica op. The caller inserts a `creating` placeholder row and
    /// returns immediately; this spawns `ensure_postgres` (which can pull a cold
    /// image and wait minutes for engine readiness, far past the ~60s client
    /// read timeout) and, when `dump` is present, replays it before flipping the
    /// row to `available`. Mirrors `create_db_instance`'s backgrounding so
    /// restore/replica no longer time the client out (bug-hunt 2026-07-01,
    /// Tier-0). On failure the placeholder row is removed and any orphaned
    /// container reaped.
    #[allow(clippy::too_many_arguments)]
    fn spawn_finalize_restored_instance(
        &self,
        runtime: Arc<RdsRuntime>,
        account_id: String,
        region: String,
        id: String,
        arn: String,
        engine: String,
        engine_version: String,
        master_username: String,
        master_user_password: String,
        logical_db: String,
        tags: Vec<RdsTag>,
        dump: Option<Vec<u8>>,
        // When `Some(source_id)`, the task live-dumps that source instance
        // (slow mysqldump/pg_dump) inside the spawn instead of the caller
        // awaiting it inline — keeps the create/restore/replica response off
        // the ~60s client read timeout. Mutually exclusive with `dump`
        // (which carries an already-in-memory dump, e.g. from a snapshot or
        // S3 backup).
        dump_source_id: Option<String>,
        created_event: (&'static str, &'static str),
    ) {
        let state_handle = self.state.clone();
        let delivery_bus = self.delivery_bus.clone();
        let snapshot_store = self.snapshot_store.clone();
        let snapshot_lock = self.snapshot_lock.clone();
        let (event_id, event_message) = created_event;
        // Shared failure path: drop the placeholder row, reap any container the
        // runtime managed to start, persist, and emit the create-failure event.
        #[allow(clippy::too_many_arguments)]
        async fn fail(
            state_handle: &SharedRdsState,
            snapshot_store: Option<Arc<dyn SnapshotStore>>,
            snapshot_lock: Arc<AsyncMutex<()>>,
            delivery_bus: Option<&Arc<DeliveryBus>>,
            runtime: &Arc<RdsRuntime>,
            account_id: &str,
            id: &str,
            incarnation: &str,
            volume: &str,
            arn: &str,
            error: &str,
        ) {
            tracing::error!(%error, db_instance_identifier=%id, "restore/replica background finalize failed");
            {
                let mut accounts = state_handle.write();
                let state = accounts.get_or_create(account_id);
                // Only this incarnation's row: a replacement created under the
                // identifier meanwhile is not ours to drop.
                if state
                    .instances
                    .get(id)
                    .is_some_and(|i| i.dbi_resource_id == incarnation)
                {
                    state.instances.remove(id);
                    // A read replica registered itself against its source
                    // synchronously; drop that reverse linkage so the source
                    // doesn't keep a dangling replica id after this failure.
                    // Only when this incarnation's row went: a replacement
                    // under the id owns the linkage otherwise.
                    for inst in state.instances.values_mut() {
                        inst.read_replica_db_instance_identifiers
                            .retain(|r| r != id);
                    }
                }
            }
            reap_gone_start(runtime, incarnation, volume).await;
            save_snapshot_static(state_handle.clone(), snapshot_store, snapshot_lock).await;
            emit_event_static(
                delivery_bus,
                RdsSourceType::DbInstance,
                id,
                arn,
                "RDS-EVENT-0058",
                &["failure"],
                &format!("DB instance failed to create: {error}"),
            );
        }
        // The incarnation being created (its row is already published) and
        // the source's, both pinned now: by identifier, the task could later
        // reach a replacement or a renamed instance instead.
        let (incarnation, volume, source_incarnation) = {
            let accounts = self.state.read();
            let Some(state) = accounts.get(&account_id) else {
                return;
            };
            let Some(inst) = state.instances.get(&id) else {
                return;
            };
            (
                inst.dbi_resource_id.clone(),
                inst.data_volume_name(
                    fakecloud_core::data_volume::current_scope().tag(),
                    &account_id,
                ),
                dump_source_id
                    .as_ref()
                    .and_then(|sid| state.instances.get(sid))
                    .map(|src| src.dbi_resource_id.clone()),
            )
        };
        tokio::spawn(async move {
            // Live-dump the source instance inside the task when requested so
            // the slow mysqldump/pg_dump never blocks the request handler. On
            // failure the placeholder row is torn down like any other finalize
            // error, mirroring the inline path's `cancel_instance_creation`.
            let dump = match dump_source_id {
                Some(_) => match runtime
                    .dump(
                        source_incarnation.as_deref().unwrap_or_default(),
                        &engine,
                        &master_username,
                        &master_user_password,
                        &logical_db,
                    )
                    .await
                {
                    Ok(data) => Some(data),
                    Err(error) => {
                        fail(
                            &state_handle,
                            snapshot_store,
                            snapshot_lock,
                            delivery_bus.as_ref(),
                            &runtime,
                            &account_id,
                            &id,
                            &incarnation,
                            &volume,
                            &arn,
                            &error.to_string(),
                        )
                        .await;
                        return;
                    }
                },
                None => dump,
            };
            let running = match runtime
                .ensure_postgres(
                    &incarnation,
                    &id,
                    &engine,
                    &engine_version,
                    &master_username,
                    &master_user_password,
                    &logical_db,
                    &account_id,
                    &region,
                    &tags,
                    &volume,
                )
                .await
            {
                Ok(running) => running,
                Err(error) => {
                    fail(
                        &state_handle,
                        snapshot_store,
                        snapshot_lock,
                        delivery_bus.as_ref(),
                        &runtime,
                        &account_id,
                        &id,
                        &incarnation,
                        &volume,
                        &arn,
                        &error.to_string(),
                    )
                    .await;
                    return;
                }
            };

            if let Some(dump) = dump {
                if let Err(error) = runtime
                    .restore(
                        &incarnation,
                        &engine,
                        &master_username,
                        &master_user_password,
                        &logical_db,
                        &dump,
                    )
                    .await
                {
                    // A failed data replay must NOT be reported as a successful
                    // `available` restore with missing data — fail the instance.
                    fail(
                        &state_handle,
                        snapshot_store,
                        snapshot_lock,
                        delivery_bus.as_ref(),
                        &runtime,
                        &account_id,
                        &id,
                        &incarnation,
                        &volume,
                        &arn,
                        &error.to_string(),
                    )
                    .await;
                    return;
                }
            }

            let current = {
                let mut accounts = state_handle.write();
                let state = accounts.get_or_create(&account_id);
                state.instance_by_incarnation_mut(&incarnation).map(|inst| {
                    inst.db_instance_status = "available".to_string();
                    inst.endpoint_address = running.endpoint_address.clone();
                    inst.port = i32::from(running.endpoint_port);
                    inst.host_port = running.host_port;
                    inst.container_id = running.container_id.clone();
                    (
                        inst.db_instance_identifier.clone(),
                        inst.db_instance_arn.clone(),
                    )
                })
            };
            let Some((id, arn)) = current else {
                // Deleted (or reset) while creating: reap the orphaned backing
                // container and its volume.
                reap_gone_start(&runtime, &incarnation, &volume).await;
                save_snapshot_static(state_handle.clone(), snapshot_store, snapshot_lock).await;
                return;
            };
            emit_event_static_with_state(
                delivery_bus.as_ref(),
                Some(&state_handle),
                Some(&account_id),
                RdsSourceType::DbInstance,
                &id,
                &arn,
                event_id,
                &["creation"],
                event_message,
            );
            save_snapshot_static(state_handle.clone(), snapshot_store, snapshot_lock).await;
        });
    }

    /// Background the slow database dump for a pure-snapshot op
    /// (CreateDBSnapshot / final snapshot on DeleteDBInstance). The caller
    /// inserts the snapshot row synchronously with status `creating` and
    /// returns immediately; this task runs mysqldump/pg_dump (unbounded in
    /// dataset size, easily past the ~60s client read timeout) and only then
    /// flips the row to `available` with the captured dump. `teardown_volume`
    /// is set for the final-snapshot path so the source instance's container
    /// and data volume are reaped *after* the dump completes rather than
    /// before it (DeleteDBInstance can't stop the container until the snapshot
    /// has read from it). It names the deleted instance's volume as captured
    /// at delete time: by the time the dump finishes, a new instance may
    /// reuse the identifier, and its volume must not be the one removed.
    #[allow(clippy::too_many_arguments)]
    fn spawn_finalize_snapshot(
        &self,
        runtime: Arc<RdsRuntime>,
        account_id: String,
        snapshot_id: String,
        // The snapshot row's creation time: a snapshot deleted and recreated
        // under the same id during the dump is a different row, which this
        // dump must not complete.
        snapshot_created: chrono::DateTime<Utc>,
        // The source instance's incarnation (`DbiResourceId`): its container
        // key, which never reaches a later instance reusing the identifier.
        source_id: String,
        engine: String,
        username: String,
        password: String,
        db_name: String,
        teardown_volume: Option<String>,
    ) {
        let state_handle = self.state.clone();
        let snapshot_store = self.snapshot_store.clone();
        let snapshot_lock = self.snapshot_lock.clone();
        tokio::spawn(async move {
            let result = runtime
                .dump(&source_id, &engine, &username, &password, &db_name)
                .await;
            apply_snapshot_dump_result(
                &state_handle,
                &account_id,
                &snapshot_id,
                snapshot_created,
                result,
            );
            save_snapshot_static(state_handle.clone(), snapshot_store, snapshot_lock).await;
            if let Some(volume) = teardown_volume {
                // Final-snapshot path owns the deferred teardown: the source
                // container had to stay up for the dump above.
                runtime.stop(&source_id).await;
                runtime.remove_data_volume_named(&volume).await;
            }
        });
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
}

#[async_trait]
impl AwsService for RdsService {
    fn service_name(&self) -> &str {
        "rds"
    }

    async fn handle(&self, request: AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        // Centralized Smithy-aligned validation. Returns the appropriate
        // `MissingParameter` / `InvalidParameterValue` error before the
        // per-action handler runs. Actions without a constraint entry
        // fall through unchanged.
        crate::validation::prevalidate(request.action.as_str(), &request)?;

        let mutates = is_mutating_action(request.action.as_str());
        let result = match request.action.as_str() {
            "AddTagsToResource" => self.add_tags_to_resource(&request),
            "CreateDBInstance" => self.create_db_instance(&request).await,
            "CreateDBInstanceReadReplica" => self.create_db_instance_read_replica(&request).await,
            "CreateDBParameterGroup" => self.create_db_parameter_group(&request),
            "CreateDBSnapshot" => self.create_db_snapshot(&request).await,
            "CreateDBSubnetGroup" => self.create_db_subnet_group(&request),
            "DeleteDBInstance" => self.delete_db_instance(&request).await,
            "DeleteDBParameterGroup" => self.delete_db_parameter_group(&request),
            "DeleteDBSnapshot" => self.delete_db_snapshot(&request),
            "DeleteDBSubnetGroup" => self.delete_db_subnet_group(&request),
            "DescribeDBEngineVersions" => self.describe_db_engine_versions(&request),
            "DescribeDBInstances" => self.describe_db_instances(&request),
            "DescribeDBParameterGroups" => self.describe_db_parameter_groups(&request),
            "DescribeDBParameters" => self.describe_db_parameters_real(&request),
            "DescribeDBSnapshots" => self.describe_db_snapshots(&request),
            "DescribeDBSubnetGroups" => self.describe_db_subnet_groups(&request),
            "DescribeOrderableDBInstanceOptions" => {
                self.describe_orderable_db_instance_options(&request)
            }
            "ListTagsForResource" => self.list_tags_for_resource(&request),
            "ModifyDBInstance" => self.modify_db_instance(&request),
            "ModifyDBParameterGroup" => self.modify_db_parameter_group(&request),
            "ModifyDBSubnetGroup" => self.modify_db_subnet_group(&request),
            "RebootDBInstance" => self.reboot_db_instance(&request).await,
            "StartDBInstance" => self.start_db_instance(&request).await,
            "StopDBInstance" => self.stop_db_instance(&request).await,
            "RemoveTagsFromResource" => self.remove_tags_from_resource(&request),
            "RestoreDBInstanceFromDBSnapshot" => {
                self.restore_db_instance_from_db_snapshot(&request).await
            }
            "RestoreDBInstanceToPointInTime" => {
                self.restore_db_instance_to_point_in_time(&request).await
            }
            "RestoreDBInstanceFromS3" => self.restore_db_instance_from_s3(&request).await,
            "DescribeDBLogFiles" => self.describe_db_log_files(&request).await,
            "DownloadDBLogFilePortion" => self.download_db_log_file_portion(&request).await,
            "CreateDBClusterSnapshot" => self.create_db_cluster_snapshot(&request).await,
            "RestoreDBClusterFromSnapshot" => self.restore_db_cluster_from_snapshot(&request).await,
            "RestoreDBClusterToPointInTime" => {
                self.restore_db_cluster_to_point_in_time(&request).await
            }
            "CreateDBCapacityReservation" => self.create_db_capacity_reservation(&request),
            "DescribeDBCapacityReservations" => self.describe_db_capacity_reservations(&request),
            "ModifyDBCapacityReservation" => self.modify_db_capacity_reservation(&request),
            "CancelDBCapacityReservation" => self.cancel_db_capacity_reservation(&request),
            _ => self.handle_extra_action(&request),
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

impl RdsService {}

/// Render a single user-set parameter as the XML shape AWS emits inside
/// `DescribeDB(Cluster)Parameters` responses. We don't store metadata
/// alongside user values so we report `dynamic`/`string` defaults.
pub(crate) fn render_user_parameter_xml(name: &str, value: &str, apply_method: &str) -> String {
    format!(
        "      <Parameter>\n        <ParameterName>{}</ParameterName>\n        <ParameterValue>{}</ParameterValue>\n        <Source>user</Source>\n        <ApplyType>dynamic</ApplyType>\n        <ApplyMethod>{}</ApplyMethod>\n        <DataType>string</DataType>\n        <IsModifiable>true</IsModifiable>\n      </Parameter>\n",
        xml_escape(name),
        xml_escape(value),
        xml_escape(apply_method),
    )
}

/// Render a single engine-default parameter as the XML shape AWS emits
/// inside `DescribeDB(Cluster)Parameters` and
/// `DescribeEngineDefault(Cluster)Parameters` responses.
pub(crate) fn render_engine_default_parameter_xml(
    default: &crate::state::EngineDefaultParameter,
) -> String {
    format!(
        "      <Parameter>\n        <ParameterName>{}</ParameterName>\n        <ParameterValue>{}</ParameterValue>\n        <Source>engine-default</Source>\n        <ApplyType>{}</ApplyType>\n        <DataType>{}</DataType>\n        <AllowedValues>{}</AllowedValues>\n        <IsModifiable>{}</IsModifiable>\n      </Parameter>\n",
        xml_escape(default.name),
        xml_escape(default.value),
        xml_escape(default.apply_type),
        xml_escape(default.data_type),
        xml_escape(default.allowed_values),
        default.is_modifiable,
    )
}

/// Parse `Parameters.{Parameter|member}.N.{ParameterName,ParameterValue,ApplyMethod}`
/// from a Query-protocol request. AWS RDS uses `Parameters.Parameter.N`
/// (the `Parameter` list location name from the Smithy model); we also
/// accept the generic `Parameters.member.N` form so hand-built clients
/// using the default Query list shape keep working. Skips members
/// missing a name or value. `ApplyMethod` defaults to `immediate` (AWS's
/// default) and is preserved so a `Describe` round-trip echoes it back —
/// the Terraform provider sets `apply_method = "immediate"` by default and
/// drifts if the read-back omits it.
pub(crate) struct DbParameterInput {
    pub name: String,
    pub value: String,
    pub apply_method: String,
}

pub(crate) fn parse_db_parameter_members(request: &AwsRequest) -> Vec<DbParameterInput> {
    let mut out = Vec::new();
    for prefix in ["Parameters.Parameter", "Parameters.member"] {
        let mut index = 1;
        loop {
            let name_key = format!("{prefix}.{index}.ParameterName");
            let value_key = format!("{prefix}.{index}.ParameterValue");
            let apply_key = format!("{prefix}.{index}.ApplyMethod");
            let name = optional_query_param(request, &name_key);
            let value = optional_query_param(request, &value_key);
            if name.is_none() && value.is_none() {
                break;
            }
            if let (Some(n), Some(v)) = (name, value) {
                if !n.is_empty() {
                    let apply_method = optional_query_param(request, &apply_key)
                        .filter(|m| !m.is_empty())
                        .unwrap_or_else(|| "immediate".to_string());
                    out.push(DbParameterInput {
                        name: n,
                        value: v,
                        apply_method,
                    });
                }
            }
            index += 1;
        }
    }
    out
}

/// Resolve an AWS-shaped log file name (e.g. `error/postgres.log`) to
/// the absolute path inside the running container. Unknown names fall
/// through as-is so callers can also fetch arbitrary paths.
fn map_log_file_to_container_path(engine: &str, log_file_name: &str) -> String {
    match (engine, log_file_name) {
        (_, "error/postgres.log") => "/var/log/postgresql/postgresql.log".to_string(),
        (_, "trace/postgres-trace.log") => "/var/log/postgresql/postgresql.log".to_string(),
        ("mysql" | "mariadb", "error/mysql-error.log") => "/var/log/mysql/error.log".to_string(),
        ("mysql" | "mariadb", "slowquery/mysql-slowquery.log") => {
            "/var/log/mysql/slow.log".to_string()
        }
        _ => log_file_name.to_string(),
    }
}

pub(crate) struct PaginationResult<T> {
    pub(crate) items: Vec<T>,
    pub(crate) next_marker: Option<String>,
}

/// Attach `instance_id` to the cluster's `DBClusterMembers` array,
/// promoting it to writer when the cluster has none. Idempotent:
/// re-attaching an existing member is a no-op.
pub fn attach_cluster_member(state: &mut RdsState, cluster_id: &str, instance_id: &str) {
    use serde_json::{json, Value};
    let Some(map) = state.extras.get_mut("clusters") else {
        return;
    };
    let Some(entry) = map.get_mut(cluster_id) else {
        return;
    };
    let Some(obj) = entry.as_object_mut() else {
        return;
    };
    let mut members: Vec<Value> = obj
        .get("DBClusterMembers")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();
    if members
        .iter()
        .any(|m| m["DBInstanceIdentifier"].as_str() == Some(instance_id))
    {
        return;
    }
    let has_writer = members
        .iter()
        .any(|m| m["IsClusterWriter"].as_bool() == Some(true));
    let promotion_tier = (members.len() as i64) + 1;
    members.push(json!({
        "DBInstanceIdentifier": instance_id,
        "IsClusterWriter": !has_writer,
        "DBClusterParameterGroupStatus": "in-sync",
        "PromotionTier": promotion_tier,
    }));
    obj.insert("DBClusterMembers".to_string(), Value::Array(members));
    if !has_writer {
        obj.insert(
            "WriterDBInstanceIdentifier".to_string(),
            Value::String(instance_id.to_string()),
        );
    }
}

/// Remove `instance_id` from the cluster's `DBClusterMembers` (the instance
/// was deleted). When it was the writer, the remaining member with the lowest
/// promotion tier becomes the writer, as an Aurora failover does; a cluster
/// left with no members has no writer.
pub fn detach_cluster_member(state: &mut RdsState, cluster_id: &str, instance_id: &str) {
    use serde_json::Value;
    let Some(obj) = state
        .extras
        .get_mut("clusters")
        .and_then(|m| m.get_mut(cluster_id))
        .and_then(|e| e.as_object_mut())
    else {
        return;
    };
    let Some(Value::Array(members)) = obj.get_mut("DBClusterMembers") else {
        return;
    };
    let Some(pos) = members
        .iter()
        .position(|m| m["DBInstanceIdentifier"].as_str() == Some(instance_id))
    else {
        return;
    };
    let removed = members.remove(pos);
    if removed["IsClusterWriter"].as_bool() != Some(true) {
        return;
    }
    let next = members
        .iter_mut()
        .min_by_key(|m| m["PromotionTier"].as_i64().unwrap_or(i64::MAX));
    let new_writer = next.map(|m| {
        m["IsClusterWriter"] = Value::Bool(true);
        m["DBInstanceIdentifier"]
            .as_str()
            .unwrap_or_default()
            .to_string()
    });
    match new_writer {
        Some(id) => {
            obj.insert("WriterDBInstanceIdentifier".to_string(), Value::String(id));
        }
        None => {
            obj.remove("WriterDBInstanceIdentifier");
        }
    }
}

#[path = "../service_helpers.rs"]
pub(crate) mod service_helpers;
pub(crate) use service_helpers::*;

#[cfg(test)]
#[path = "../service_tests.rs"]
mod tests;
