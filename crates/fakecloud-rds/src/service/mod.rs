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
    default_engine_versions, default_orderable_options, DbInstance, DbParameterGroup, DbSnapshot,
    DbSubnetGroup, EngineVersionInfo, OrderableDbInstanceOption, RdsSnapshot, RdsState, RdsTag,
    SharedRdsState, RDS_SNAPSHOT_SCHEMA_VERSION,
};

const RDS_NS: &str = "http://rds.amazonaws.com/doc/2014-10-31/";

const SUPPORTED_ACTIONS: &[&str] = &[
    "AddRoleToDBCluster",
    "AddRoleToDBInstance",
    "AddSourceIdentifierToSubscription",
    "AddTagsToResource",
    "ApplyPendingMaintenanceAction",
    "AuthorizeDBSecurityGroupIngress",
    "BacktrackDBCluster",
    "CancelExportTask",
    "CopyDBClusterParameterGroup",
    "CopyDBClusterSnapshot",
    "CopyDBParameterGroup",
    "CopyDBSnapshot",
    "CopyOptionGroup",
    "CreateBlueGreenDeployment",
    "CreateCustomDBEngineVersion",
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

        struct Pending {
            account_id: String,
            region: String,
            id: String,
            arn: String,
            engine: String,
            engine_version: String,
            username: String,
            password: String,
            db_name: String,
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
                    inst.db_instance_status = "starting".to_string();
                    out.push(Pending {
                        account_id: account_id.clone(),
                        region: region.clone(),
                        id: id.clone(),
                        arn: inst.db_instance_arn.clone(),
                        engine: inst.engine.clone(),
                        engine_version: inst.engine_version.clone(),
                        username: inst.master_username.clone(),
                        password: inst.master_user_password.clone(),
                        db_name: inst
                            .db_name
                            .clone()
                            .unwrap_or_else(|| default_db_name(&inst.engine).to_string()),
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
                        &p.id,
                        &p.engine,
                        &p.engine_version,
                        &p.username,
                        &p.password,
                        &p.db_name,
                        &p.account_id,
                        &p.region,
                    )
                    .await
                {
                    Ok(running) => {
                        {
                            let mut accounts = state.write();
                            if let Some(s) = accounts.get_mut(&p.account_id) {
                                if let Some(inst) = s.instances.get_mut(&p.id) {
                                    inst.db_instance_status = "available".to_string();
                                    inst.endpoint_address = running.endpoint_address.clone();
                                    inst.port = i32::from(running.endpoint_port);
                                    inst.host_port = running.host_port;
                                    inst.container_id = running.container_id;
                                }
                            }
                        }
                        save_snapshot_static(
                            state.clone(),
                            snapshot_store.clone(),
                            snapshot_lock.clone(),
                        )
                        .await;
                        emit_event_static(
                            delivery_bus.as_ref(),
                            RdsSourceType::DbInstance,
                            &p.id,
                            &p.arn,
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
                        {
                            let mut accounts = state.write();
                            if let Some(s) = accounts.get_mut(&p.account_id) {
                                if let Some(inst) = s.instances.get_mut(&p.id) {
                                    inst.db_instance_status = "failed".to_string();
                                }
                            }
                        }
                        save_snapshot_static(state, snapshot_store, snapshot_lock).await;
                    }
                }
            });
        }
    }

    /// Stop the backing container for `db_instance_identifier` and mark
    /// the row `stopped`. Synchronous wrt the runtime so callers see a
    /// real `stopped` status by the time the response goes back; mirrors
    /// the AWS contract that `StartDBInstance`/`StopDBInstance` change
    /// the visible status immediately.
    async fn stop_db_instance(&self, request: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let db_instance_identifier = required_query_param(request, "DBInstanceIdentifier")?;

        // Look up the instance first so a missing identifier returns the
        // declared `DBInstanceNotFoundFault` error rather than a 503 from a
        // missing container runtime. Conformance probes hit Start/Stop with
        // synthetic identifiers and expect the documented error shape.
        let arn = {
            let accounts = self.state.read();
            let empty = RdsState::new(&request.account_id, &request.region);
            let state = accounts.get(&request.account_id).unwrap_or(&empty);
            state
                .instances
                .get(&db_instance_identifier)
                .map(|i| i.db_instance_arn.clone())
                .ok_or_else(|| db_instance_not_found(&db_instance_identifier))?
        };

        if let Some(runtime) = self.runtime.as_ref() {
            runtime.stop_container(&db_instance_identifier).await;
        }

        let instance = {
            let mut accounts = self.state.write();
            let state = accounts.get_or_create(&request.account_id);
            let inst = state
                .instances
                .get_mut(&db_instance_identifier)
                .ok_or_else(|| db_instance_not_found(&db_instance_identifier))?;
            inst.db_instance_status = "stopped".to_string();
            inst.container_id = String::new();
            inst.clone()
        };

        self.emit_event(
            RdsSourceType::DbInstance,
            &db_instance_identifier,
            &arn,
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
                    db_instance_xml(&instance, Some("stopped"))
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

        // Flip to `starting` so concurrent DescribeDBInstances callers
        // see the in-flight state.
        {
            let mut accounts = self.state.write();
            let state = accounts.get_or_create(&request.account_id);
            if let Some(inst) = state.instances.get_mut(&db_instance_identifier) {
                inst.db_instance_status = "starting".to_string();
            }
        }

        let running = if let Some(runtime) = self.runtime.as_ref() {
            match runtime
                .ensure_postgres(
                    &db_instance_identifier,
                    &instance.engine,
                    &instance.engine_version,
                    &instance.master_username,
                    &instance.master_user_password,
                    instance
                        .db_name
                        .as_deref()
                        .unwrap_or(default_db_name(&instance.engine)),
                    &request.account_id,
                    &request.region,
                )
                .await
            {
                Ok(r) => Some(r),
                Err(e) => {
                    // Roll the row back to `stopped` so the next
                    // Describe doesn't report a permanently-`starting`
                    // instance with no container behind it.
                    let mut accounts = self.state.write();
                    let state = accounts.get_or_create(&request.account_id);
                    if let Some(inst) = state.instances.get_mut(&db_instance_identifier) {
                        inst.db_instance_status = "stopped".to_string();
                    }
                    return Err(runtime_error_to_service_error(e));
                }
            }
        } else {
            // No container runtime configured: StartDBInstance should
            // fail rather than report success against a backend that
            // does not exist. Roll the row back so subsequent calls
            // don't see a stuck `starting` state.
            {
                let mut accounts = self.state.write();
                let state = accounts.get_or_create(&request.account_id);
                if let Some(inst) = state.instances.get_mut(&db_instance_identifier) {
                    inst.db_instance_status = "stopped".to_string();
                }
            }
            return Err(AwsServiceError::aws_error(
                StatusCode::SERVICE_UNAVAILABLE,
                "InternalFailure",
                "Container runtime is not configured; cannot start DB instance",
            ));
        };

        let instance = {
            let mut accounts = self.state.write();
            let state = accounts.get_or_create(&request.account_id);
            let inst = state
                .instances
                .get_mut(&db_instance_identifier)
                .ok_or_else(|| db_instance_not_found(&db_instance_identifier))?;
            inst.db_instance_status = "available".to_string();
            inst.endpoint_address = "127.0.0.1".to_string();
            if let Some(r) = running {
                inst.endpoint_address = r.endpoint_address.clone();
                inst.port = i32::from(r.endpoint_port);
                inst.host_port = r.host_port;
                inst.container_id = r.container_id;
            }
            inst.clone()
        };

        self.emit_event(
            RdsSourceType::DbInstance,
            &db_instance_identifier,
            &instance.db_instance_arn,
            "RDS-EVENT-0088",
            &["notification"],
            "DB instance started",
        );

        Ok(AwsResponse::xml(
            StatusCode::OK,
            query_response_xml(
                "StartDBInstance",
                RDS_NS,
                &format!(
                    "<DBInstance>{}</DBInstance>",
                    db_instance_xml(&instance, None)
                ),
                &request.request_id,
            ),
        ))
    }
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
    fn require_runtime(&self) -> Result<&Arc<RdsRuntime>, AwsServiceError> {
        self.runtime.as_ref().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::SERVICE_UNAVAILABLE,
                "InsufficientDBInstanceCapacity",
                "Docker/Podman is required for RDS DB instances but is not available",
            )
        })
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
pub(crate) fn render_user_parameter_xml(name: &str, value: &str) -> String {
    format!(
        "      <Parameter>\n        <ParameterName>{}</ParameterName>\n        <ParameterValue>{}</ParameterValue>\n        <Source>user</Source>\n        <ApplyType>dynamic</ApplyType>\n        <DataType>string</DataType>\n        <IsModifiable>true</IsModifiable>\n      </Parameter>\n",
        xml_escape(name),
        xml_escape(value),
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
/// missing a name or value; ApplyMethod is accepted but ignored
/// (single-state model).
pub(crate) fn parse_db_parameter_members(request: &AwsRequest) -> Vec<(String, String)> {
    let mut out = Vec::new();
    for prefix in ["Parameters.Parameter", "Parameters.member"] {
        let mut index = 1;
        loop {
            let name_key = format!("{prefix}.{index}.ParameterName");
            let value_key = format!("{prefix}.{index}.ParameterValue");
            let name = optional_query_param(request, &name_key);
            let value = optional_query_param(request, &value_key);
            if name.is_none() && value.is_none() {
                break;
            }
            if let (Some(n), Some(v)) = (name, value) {
                if !n.is_empty() {
                    out.push((n, v));
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
    items: Vec<T>,
    next_marker: Option<String>,
}

/// Attach `instance_id` to the cluster's `DBClusterMembers` array,
/// promoting it to writer when the cluster has none. Idempotent:
/// re-attaching an existing member is a no-op.
fn attach_cluster_member(state: &mut RdsState, cluster_id: &str, instance_id: &str) {
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

#[path = "../service_helpers.rs"]
mod service_helpers;
pub(crate) use service_helpers::*;

#[cfg(test)]
#[path = "../service_tests.rs"]
mod tests;
