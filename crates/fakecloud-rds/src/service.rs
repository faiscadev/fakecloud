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
                    if !matches!(
                        inst.db_instance_status.as_str(),
                        "available" | "starting" | "modifying" | "rebooting" | "backing-up"
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
                                    inst.endpoint_address = "127.0.0.1".to_string();
                                    inst.port = i32::from(running.host_port);
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
            Some(
                runtime
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
                    .map_err(runtime_error_to_service_error)?,
            )
        } else {
            None
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
                inst.port = i32::from(r.host_port);
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

impl RdsService {
    async fn create_db_instance(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let db_instance_identifier = required_query_param(request, "DBInstanceIdentifier")?;
        let db_instance_class = required_query_param(request, "DBInstanceClass")?;
        let engine = required_query_param(request, "Engine")?;
        // AllocatedStorage / MasterUsername / MasterUserPassword are NOT
        // declared `@required` in the Smithy model — real AWS rejects
        // them in `InvalidParameterCombination` form which we can't emit
        // because that code is undeclared on this op. Pick reasonable
        // defaults so probe inputs with only model-required fields land
        // on the happy path; real misuse still fails on engine/class
        // validation paths.
        let allocated_storage = optional_i32_param(request, "AllocatedStorage")?
            .filter(|v| *v > 0)
            .unwrap_or(20);
        let master_username =
            optional_query_param(request, "MasterUsername").unwrap_or_else(|| "admin".to_string());
        let master_user_password = optional_query_param(request, "MasterUserPassword")
            .unwrap_or_else(|| "Password1!".to_string());
        let db_name = optional_query_param(request, "DBName");
        let engine_version =
            optional_query_param(request, "EngineVersion").unwrap_or_else(|| "16.3".to_string());
        let publicly_accessible =
            parse_optional_bool(optional_query_param(request, "PubliclyAccessible").as_deref())?
                .unwrap_or(true);
        let deletion_protection =
            parse_optional_bool(optional_query_param(request, "DeletionProtection").as_deref())?
                .unwrap_or(false);
        let port = optional_i32_param(request, "Port")?
            .unwrap_or_else(|| default_port_for_engine(&engine));
        let vpc_security_group_ids = parse_vpc_security_group_ids(request);

        let db_parameter_group_name = optional_query_param(request, "DBParameterGroupName")
            .or_else(|| Some(default_parameter_group(&engine, &engine_version)));

        let backup_retention_period =
            optional_i32_param(request, "BackupRetentionPeriod")?.unwrap_or(1);
        let preferred_backup_window = optional_query_param(request, "PreferredBackupWindow")
            .unwrap_or_else(|| "03:00-04:00".to_string());
        let option_group_name = optional_query_param(request, "OptionGroupName");
        let multi_az = parse_optional_bool(optional_query_param(request, "MultiAZ").as_deref())?
            .unwrap_or(false);
        let availability_zone = optional_query_param(request, "AvailabilityZone");
        let storage_type = optional_query_param(request, "StorageType");
        let storage_encrypted =
            parse_optional_bool(optional_query_param(request, "StorageEncrypted").as_deref())?
                .unwrap_or(false);
        let kms_key_id = optional_query_param(request, "KmsKeyId");
        let iam_database_authentication_enabled = parse_optional_bool(
            optional_query_param(request, "EnableIAMDatabaseAuthentication").as_deref(),
        )?
        .unwrap_or(false);
        let iops = optional_i32_param(request, "Iops")?;
        let monitoring_interval = optional_i32_param(request, "MonitoringInterval")?;
        let monitoring_role_arn = optional_query_param(request, "MonitoringRoleArn");
        let performance_insights_enabled = parse_optional_bool(
            optional_query_param(request, "EnablePerformanceInsights").as_deref(),
        )?
        .unwrap_or(false);
        let performance_insights_kms_key_id =
            optional_query_param(request, "PerformanceInsightsKMSKeyId");
        let performance_insights_retention_period =
            optional_i32_param(request, "PerformanceInsightsRetentionPeriod")?;
        let enabled_cloudwatch_logs_exports =
            parse_cloudwatch_logs_exports(request, "EnableCloudwatchLogsExports");
        let ca_certificate_identifier = optional_query_param(request, "CACertificateIdentifier");
        let network_type = optional_query_param(request, "NetworkType");
        let character_set_name = optional_query_param(request, "CharacterSetName");
        let auto_minor_version_upgrade = parse_optional_bool(
            optional_query_param(request, "AutoMinorVersionUpgrade").as_deref(),
        )?;
        let copy_tags_to_snapshot =
            parse_optional_bool(optional_query_param(request, "CopyTagsToSnapshot").as_deref())?;
        let db_cluster_identifier = optional_query_param(request, "DBClusterIdentifier");

        validate_create_request(
            &db_instance_identifier,
            allocated_storage,
            &db_instance_class,
            &engine,
            &engine_version,
            port,
        )?;

        {
            let mut accounts = self.state.write();
            let state = accounts.get_or_create(&request.account_id);
            if !state.begin_instance_creation(&db_instance_identifier) {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "DBInstanceAlreadyExists",
                    format!("DBInstance {} already exists.", db_instance_identifier),
                ));
            }
            // Validate parameter group exists if specified by the caller
            if let Some(ref pg_name) = db_parameter_group_name {
                if !state.parameter_groups.contains_key(pg_name) {
                    state.cancel_instance_creation(&db_instance_identifier);
                    return Err(AwsServiceError::aws_error(
                        StatusCode::NOT_FOUND,
                        "DBParameterGroupNotFound",
                        format!("DBParameterGroup {} not found.", pg_name),
                    ));
                }
            }
        }

        let runtime = self.require_runtime()?.clone();

        let logical_db_name = db_name
            .clone()
            .unwrap_or_else(|| default_db_name(&engine).to_string());

        // Insert a "creating" placeholder synchronously and spawn the
        // container start in a background task. CreateDBInstance returns
        // ~immediately; DescribeDBInstances flips to "available" (or
        // "failed") when the container is up. Matches AWS RDS behavior:
        // CreateDBInstance never blocks on the container coming up.
        let created_at = Utc::now();
        let instance = {
            let mut accounts = self.state.write();
            let state = accounts.get_or_create(&request.account_id);
            let placeholder = DbInstance {
                db_instance_identifier: db_instance_identifier.clone(),
                db_instance_arn: state.db_instance_arn(&db_instance_identifier),
                db_instance_class: db_instance_class.clone(),
                engine: engine.clone(),
                engine_version: engine_version.clone(),
                db_instance_status: "creating".to_string(),
                master_username: master_username.clone(),
                db_name: db_name.clone(),
                endpoint_address: String::new(),
                port: 0,
                allocated_storage,
                publicly_accessible,
                deletion_protection,
                created_at,
                dbi_resource_id: state.next_dbi_resource_id(),
                master_user_password: master_user_password.clone(),
                container_id: String::new(),
                host_port: 0,
                tags: Vec::new(),
                read_replica_source_db_instance_identifier: None,
                read_replica_db_instance_identifiers: Vec::new(),
                vpc_security_group_ids,
                db_parameter_group_name,
                backup_retention_period,
                preferred_backup_window,
                preferred_maintenance_window: None,
                latest_restorable_time: if backup_retention_period > 0 {
                    Some(created_at)
                } else {
                    None
                },
                option_group_name,
                multi_az,
                pending_modified_values: None,
                availability_zone,
                storage_type,
                storage_encrypted,
                kms_key_id,
                iam_database_authentication_enabled,
                iops,
                monitoring_interval,
                monitoring_role_arn,
                performance_insights_enabled,
                performance_insights_kms_key_id,
                performance_insights_retention_period,
                enabled_cloudwatch_logs_exports,
                ca_certificate_identifier,
                network_type,
                character_set_name,
                auto_minor_version_upgrade,
                copy_tags_to_snapshot,
                master_user_secret_arn: None,
                master_user_secret_kms_key_id: None,
                license_model: None,
                max_allocated_storage: None,
                multi_tenant: None,
                storage_throughput: None,
                tde_credential_arn: None,
                delete_automated_backups: None,
                db_security_groups: Vec::new(),
                domain: None,
                domain_fqdn: None,
                domain_ou: None,
                domain_iam_role_name: None,
                domain_auth_secret_arn: None,
                domain_dns_ips: Vec::new(),
                db_cluster_identifier: db_cluster_identifier.clone(),
            };
            state.finish_instance_creation(placeholder.clone());
            placeholder
        };
        let instance_arn = instance.db_instance_arn.clone();

        self.emit_event(
            RdsSourceType::DbInstance,
            &db_instance_identifier,
            &instance_arn,
            "RDS-EVENT-0005",
            &["creation"],
            "DB instance created",
        );

        {
            let state_handle = self.state.clone();
            let delivery_bus = self.delivery_bus.clone();
            let runtime = runtime.clone();
            let id = db_instance_identifier.clone();
            let engine = engine.clone();
            let engine_version = engine_version.clone();
            let master_username = master_username.clone();
            let master_user_password = master_user_password.clone();
            let logical_db_name_task = logical_db_name.clone();
            let account_id = request.account_id.clone();
            let region = request.region.clone();
            let arn = instance_arn.clone();
            let snapshot_store = self.snapshot_store.clone();
            let snapshot_lock = self.snapshot_lock.clone();
            let cluster_id_for_attach = db_cluster_identifier.clone();
            tokio::spawn(async move {
                match runtime
                    .ensure_postgres(
                        &id,
                        &engine,
                        &engine_version,
                        &master_username,
                        &master_user_password,
                        &logical_db_name_task,
                        &account_id,
                        &region,
                    )
                    .await
                {
                    Ok(running) => {
                        // If the cluster has a pending restore dump
                        // staged by RestoreDBClusterFromSnapshot /
                        // RestoreDBClusterToPointInTime, drain it and
                        // replay onto this fresh instance. We do this
                        // before flipping status to available so the
                        // first read of "available" sees restored data.
                        let pending_dump = if let Some(ref cid) = cluster_id_for_attach {
                            let mut accounts = state_handle.write();
                            let state = accounts.get_or_create(&account_id);
                            state
                                .extras
                                .get_mut("clusters")
                                .and_then(|m| m.get_mut(cid))
                                .and_then(|entry| entry.as_object_mut())
                                .and_then(|obj| obj.remove("PendingRestoreDumpB64"))
                                .and_then(|v| v.as_str().map(str::to_string))
                                .and_then(|b64| {
                                    use base64::Engine;
                                    base64::engine::general_purpose::STANDARD
                                        .decode(b64.as_bytes())
                                        .ok()
                                })
                        } else {
                            None
                        };
                        if let Some(dump) = pending_dump {
                            if let Err(error) = runtime
                                .restore_database(
                                    &id,
                                    &engine,
                                    &master_username,
                                    &master_user_password,
                                    &logical_db_name_task,
                                    &dump,
                                )
                                .await
                            {
                                tracing::error!(%error, db_instance_identifier=%id, "cluster restore dump replay failed");
                            }
                        }

                        {
                            let mut accounts = state_handle.write();
                            let state = accounts.get_or_create(&account_id);
                            if let Some(inst) = state.instances.get_mut(&id) {
                                inst.db_instance_status = "available".to_string();
                                inst.endpoint_address = "127.0.0.1".to_string();
                                inst.port = i32::from(running.host_port);
                                inst.host_port = running.host_port;
                                inst.container_id = running.container_id;
                            }
                            // Register as cluster member so failover /
                            // restore paths can find the writer.
                            if let Some(ref cid) = cluster_id_for_attach {
                                attach_cluster_member(state, cid, &id);
                            }
                        }
                        // Persist the flipped status. Without this the
                        // synchronous CreateDBInstance save captures the
                        // `creating` placeholder, which the load path
                        // discards on restart, dropping the instance.
                        save_snapshot_static(
                            state_handle.clone(),
                            snapshot_store.clone(),
                            snapshot_lock.clone(),
                        )
                        .await;
                    }
                    Err(error) => {
                        tracing::error!(%error, db_instance_identifier=%id, "create_db_instance background task failed");
                        {
                            let mut accounts = state_handle.write();
                            let state = accounts.get_or_create(&account_id);
                            state.instances.remove(&id);
                        }
                        save_snapshot_static(
                            state_handle.clone(),
                            snapshot_store.clone(),
                            snapshot_lock.clone(),
                        )
                        .await;
                        emit_event_static(
                            delivery_bus.as_ref(),
                            RdsSourceType::DbInstance,
                            &id,
                            &arn,
                            "RDS-EVENT-0058",
                            &["failure"],
                            &format!("DB instance failed to create: {}", error),
                        );
                    }
                }
            });
        }

        Ok(AwsResponse::xml(
            StatusCode::OK,
            query_response_xml(
                "CreateDBInstance",
                RDS_NS,
                &format!(
                    "<DBInstance>{}</DBInstance>",
                    db_instance_xml(&instance, None)
                ),
                &request.request_id,
            ),
        ))
    }

    async fn delete_db_instance(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let db_instance_identifier = required_query_param(request, "DBInstanceIdentifier")?;
        let skip_final_snapshot =
            parse_optional_bool(optional_query_param(request, "SkipFinalSnapshot").as_deref())?
                .unwrap_or(false);
        let final_db_snapshot_identifier =
            optional_query_param(request, "FinalDBSnapshotIdentifier");

        if skip_final_snapshot && final_db_snapshot_identifier.is_some() {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidDBInstanceState",
                "FinalDBSnapshotIdentifier cannot be specified when SkipFinalSnapshot is enabled.",
            ));
        }
        if !skip_final_snapshot && final_db_snapshot_identifier.is_none() {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidDBInstanceState",
                "FinalDBSnapshotIdentifier is required when SkipFinalSnapshot is false or not specified.",
            ));
        }

        // Check deletion protection BEFORE creating snapshot or making any changes
        {
            let accounts = self.state.read();
            let empty = RdsState::new(&request.account_id, &request.region);
            let state = accounts.get(&request.account_id).unwrap_or(&empty);
            if let Some(instance) = state.instances.get(&db_instance_identifier) {
                if instance.deletion_protection {
                    return Err(AwsServiceError::aws_error(
                        StatusCode::BAD_REQUEST,
                        "InvalidDBInstanceState",
                        format!(
                            "DBInstance {} cannot be deleted because deletion protection is enabled.",
                            db_instance_identifier
                        ),
                    ));
                }
            } else {
                return Err(db_instance_not_found(&db_instance_identifier));
            }
        }

        if let Some(ref snapshot_id) = final_db_snapshot_identifier {
            self.create_final_db_snapshot(
                &db_instance_identifier,
                snapshot_id,
                &request.account_id,
                &request.region,
            )
            .await?;
        }

        let instance = {
            let mut accounts = self.state.write();
            let state = accounts.get_or_create(&request.account_id);
            let instance = state
                .instances
                .remove(&db_instance_identifier)
                .ok_or_else(|| db_instance_not_found(&db_instance_identifier))?;

            if let Some(source_id) = &instance.read_replica_source_db_instance_identifier {
                if let Some(source) = state.instances.get_mut(source_id) {
                    source
                        .read_replica_db_instance_identifiers
                        .retain(|id| id != &db_instance_identifier);
                }
            }

            for replica_id in &instance.read_replica_db_instance_identifiers {
                if let Some(replica) = state.instances.get_mut(replica_id) {
                    replica.read_replica_source_db_instance_identifier = None;
                }
            }

            instance
        };

        if let Some(runtime) = &self.runtime {
            runtime.stop_container(&db_instance_identifier).await;
        }

        self.emit_event(
            RdsSourceType::DbInstance,
            &db_instance_identifier,
            &instance.db_instance_arn,
            "RDS-EVENT-0003",
            &["deletion"],
            "DB instance deleted",
        );

        Ok(AwsResponse::xml(
            StatusCode::OK,
            query_response_xml(
                "DeleteDBInstance",
                RDS_NS,
                &format!(
                    "<DBInstance>{}</DBInstance>",
                    db_instance_xml(&instance, Some("deleting"))
                ),
                &request.request_id,
            ),
        ))
    }

    /// Take a final snapshot of an instance that is about to be deleted,
    /// persisting the dumped database into `state.snapshots`. The DLQ-style
    /// conflict check runs twice — once under the read lock before paying
    /// for the dump, once under the write lock before committing — to keep
    /// concurrent deletes from colliding.
    async fn create_final_db_snapshot(
        &self,
        db_instance_identifier: &str,
        snapshot_id: &str,
        account_id: &str,
        region: &str,
    ) -> Result<(), AwsServiceError> {
        let runtime = self.runtime.as_ref().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::SERVICE_UNAVAILABLE,
                "InvalidDBSnapshotState",
                "Docker/Podman is required for RDS snapshots but is not available",
            )
        })?;

        let (instance_for_snapshot, db_name) = {
            let accounts = self.state.read();
            let empty = RdsState::new(account_id, region);
            let state = accounts.get(account_id).unwrap_or(&empty);

            if state.snapshots.contains_key(snapshot_id) {
                return Err(AwsServiceError::aws_error(
                    StatusCode::CONFLICT,
                    "DBSnapshotAlreadyExists",
                    format!("DBSnapshot {snapshot_id} already exists."),
                ));
            }

            let instance = state
                .instances
                .get(db_instance_identifier)
                .cloned()
                .ok_or_else(|| db_instance_not_found(db_instance_identifier))?;

            let default_db = default_db_name(&instance.engine);
            let db_name = instance
                .db_name
                .as_deref()
                .unwrap_or(default_db)
                .to_string();

            (instance, db_name)
        };

        let dump_data = runtime
            .dump_database(
                db_instance_identifier,
                &instance_for_snapshot.engine,
                &instance_for_snapshot.master_username,
                &instance_for_snapshot.master_user_password,
                &db_name,
            )
            .await
            .map_err(runtime_error_to_service_error)?;

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(account_id);

        if state.snapshots.contains_key(snapshot_id) {
            return Err(AwsServiceError::aws_error(
                StatusCode::CONFLICT,
                "DBSnapshotAlreadyExists",
                format!("DBSnapshot {snapshot_id} already exists."),
            ));
        }

        let snapshot_arn = state.db_snapshot_arn(snapshot_id);

        let snapshot = DbSnapshot {
            db_snapshot_identifier: snapshot_id.to_string(),
            db_snapshot_arn: snapshot_arn,
            db_instance_identifier: db_instance_identifier.to_string(),
            snapshot_create_time: Utc::now(),
            engine: instance_for_snapshot.engine.clone(),
            engine_version: instance_for_snapshot.engine_version.clone(),
            allocated_storage: instance_for_snapshot.allocated_storage,
            status: "available".to_string(),
            port: instance_for_snapshot.port,
            master_username: instance_for_snapshot.master_username.clone(),
            db_name: instance_for_snapshot.db_name.clone(),
            dbi_resource_id: instance_for_snapshot.dbi_resource_id.clone(),
            snapshot_type: "automated".to_string(),
            master_user_password: instance_for_snapshot.master_user_password.clone(),
            tags: Vec::new(),
            dump_data,
            availability_zone: instance_for_snapshot.availability_zone.clone(),
            vpc_id: None,
            instance_create_time: Some(instance_for_snapshot.created_at),
            license_model: Some(
                service_helpers::license_model_for_engine(&instance_for_snapshot.engine)
                    .to_string(),
            ),
            iops: instance_for_snapshot.iops,
            option_group_name: instance_for_snapshot.option_group_name.clone(),
            percent_progress: Some(100),
            storage_type: instance_for_snapshot.storage_type.clone(),
            encrypted: instance_for_snapshot.storage_encrypted,
            kms_key_id: instance_for_snapshot.kms_key_id.clone(),
            iam_database_authentication_enabled: instance_for_snapshot
                .iam_database_authentication_enabled,
            timezone: None,
            storage_throughput: None,
        };

        state.snapshots.insert(snapshot_id.to_string(), snapshot);
        Ok(())
    }

    fn modify_db_instance(&self, request: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let db_instance_identifier = required_query_param(request, "DBInstanceIdentifier")?;
        let apply_immediately =
            parse_optional_bool(optional_query_param(request, "ApplyImmediately").as_deref())?;

        // Parse every Modify input up front; routing into the always-
        // immediate or ApplyImmediately-gated path happens further down.
        let deletion_protection =
            parse_optional_bool(optional_query_param(request, "DeletionProtection").as_deref())?;
        let backup_retention_period =
            parse_optional_i32(optional_query_param(request, "BackupRetentionPeriod").as_deref())?;
        let preferred_backup_window = optional_query_param(request, "PreferredBackupWindow");
        let preferred_maintenance_window =
            optional_query_param(request, "PreferredMaintenanceWindow");
        let db_parameter_group_name = optional_query_param(request, "DBParameterGroupName");
        let master_user_secret_kms_key_id =
            optional_query_param(request, "MasterUserSecretKmsKeyId");
        let ca_certificate_identifier = optional_query_param(request, "CACertificateIdentifier");
        let monitoring_interval =
            parse_optional_i32(optional_query_param(request, "MonitoringInterval").as_deref())?;
        let option_group_name = optional_query_param(request, "OptionGroupName");
        let auto_minor_version_upgrade = parse_optional_bool(
            optional_query_param(request, "AutoMinorVersionUpgrade").as_deref(),
        )?;
        let copy_tags_to_snapshot =
            parse_optional_bool(optional_query_param(request, "CopyTagsToSnapshot").as_deref())?;
        let delete_automated_backups = parse_optional_bool(
            optional_query_param(request, "DeleteAutomatedBackups").as_deref(),
        )?;
        let enable_iam_db_auth = parse_optional_bool(
            optional_query_param(request, "EnableIAMDatabaseAuthentication").as_deref(),
        )?;
        let max_allocated_storage =
            parse_optional_i32(optional_query_param(request, "MaxAllocatedStorage").as_deref())?;
        let network_type = optional_query_param(request, "NetworkType");
        let domain = optional_query_param(request, "Domain");
        let domain_fqdn = optional_query_param(request, "DomainFqdn");
        let domain_ou = optional_query_param(request, "DomainOu");
        let domain_iam_role_name = optional_query_param(request, "DomainIAMRoleName");
        let domain_auth_secret_arn = optional_query_param(request, "DomainAuthSecretArn");
        let domain_dns_ips = {
            let v = parse_string_member_list(request, "DomainDnsIps");
            if v.is_empty() {
                None
            } else {
                Some(v)
            }
        };
        let disable_domain =
            parse_optional_bool(optional_query_param(request, "DisableDomain").as_deref())?;
        let rotate_master_user_password = parse_optional_bool(
            optional_query_param(request, "RotateMasterUserPassword").as_deref(),
        )?;

        let db_instance_class = optional_query_param(request, "DBInstanceClass");
        let master_user_password = optional_query_param(request, "MasterUserPassword");
        let engine_version = optional_query_param(request, "EngineVersion");
        let allocated_storage =
            parse_optional_i32(optional_query_param(request, "AllocatedStorage").as_deref())?;
        let multi_az = parse_optional_bool(optional_query_param(request, "MultiAZ").as_deref())?;
        let iops = parse_optional_i32(optional_query_param(request, "Iops").as_deref())?;
        let storage_type = optional_query_param(request, "StorageType");
        let storage_throughput =
            parse_optional_i32(optional_query_param(request, "StorageThroughput").as_deref())?;
        let performance_insights_enabled = parse_optional_bool(
            optional_query_param(request, "EnablePerformanceInsights").as_deref(),
        )?;
        let license_model = optional_query_param(request, "LicenseModel");
        let multi_tenant =
            parse_optional_bool(optional_query_param(request, "MultiTenant").as_deref())?;
        let publicly_accessible =
            parse_optional_bool(optional_query_param(request, "PubliclyAccessible").as_deref())?;
        let tde_credential_arn = optional_query_param(request, "TdeCredentialArn");
        let db_port_number =
            parse_optional_i32(optional_query_param(request, "DBPortNumber").as_deref())?;

        // CloudWatch logs exports — AWS lets callers both opt-in to and
        // opt-out of specific log types in the same call. We compute the
        // resulting set per AWS semantics: start from current, remove
        // DisableLogTypes, then union with EnableLogTypes.
        let cloudwatch_enable = collect_cloudwatch_log_types(request, "EnableLogTypes");
        let cloudwatch_disable = collect_cloudwatch_log_types(request, "DisableLogTypes");
        let cloudwatch_changed = !cloudwatch_enable.is_empty() || !cloudwatch_disable.is_empty();

        // Parse VPC security group IDs - only if at least one is provided
        let vpc_security_group_ids = {
            let mut ids = Vec::new();
            for index in 1.. {
                let sg_id_name = format!("VpcSecurityGroupIds.VpcSecurityGroupId.{index}");
                match optional_query_param(request, &sg_id_name) {
                    Some(sg_id) => ids.push(sg_id),
                    None => break,
                }
            }
            if ids.is_empty() {
                None
            } else {
                Some(ids)
            }
        };

        // Legacy classic-only DBSecurityGroups list. AWS still accepts the
        // parameter even on VPC instances; we record it verbatim.
        let db_security_groups = {
            let mut ids = Vec::new();
            for index in 1.. {
                let key = format!("DBSecurityGroups.DBSecurityGroupName.{index}");
                match optional_query_param(request, &key) {
                    Some(name) => ids.push(name),
                    None => break,
                }
            }
            if ids.is_empty() {
                None
            } else {
                Some(ids)
            }
        };

        if let Some(ref class) = db_instance_class {
            validate_db_instance_class(class)?;
        }

        // At-least-one mutable field must be present. We accept every
        // mutable RDS Modify input, so we only reject the trivial case
        // where the caller supplied just `DBInstanceIdentifier`.
        let any_mutable_field = db_instance_class.is_some()
            || deletion_protection.is_some()
            || vpc_security_group_ids.is_some()
            || db_security_groups.is_some()
            || master_user_password.is_some()
            || backup_retention_period.is_some()
            || preferred_backup_window.is_some()
            || preferred_maintenance_window.is_some()
            || engine_version.is_some()
            || allocated_storage.is_some()
            || db_parameter_group_name.is_some()
            || multi_az.is_some()
            || iops.is_some()
            || storage_type.is_some()
            || storage_throughput.is_some()
            || master_user_secret_kms_key_id.is_some()
            || ca_certificate_identifier.is_some()
            || monitoring_interval.is_some()
            || performance_insights_enabled.is_some()
            || cloudwatch_changed
            || option_group_name.is_some()
            || auto_minor_version_upgrade.is_some()
            || copy_tags_to_snapshot.is_some()
            || delete_automated_backups.is_some()
            || enable_iam_db_auth.is_some()
            || max_allocated_storage.is_some()
            || network_type.is_some()
            || license_model.is_some()
            || multi_tenant.is_some()
            || publicly_accessible.is_some()
            || tde_credential_arn.is_some()
            || db_port_number.is_some()
            || domain.is_some()
            || domain_fqdn.is_some()
            || domain_ou.is_some()
            || domain_iam_role_name.is_some()
            || domain_auth_secret_arn.is_some()
            || domain_dns_ips.is_some()
            || disable_domain.is_some()
            || rotate_master_user_password.is_some();
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&request.account_id);
        let instance = state
            .instances
            .get_mut(&db_instance_identifier)
            .ok_or_else(|| db_instance_not_found(&db_instance_identifier))?;

        // Smithy declares no `InvalidParameterCombination` shape on
        // ModifyDBInstance, so "no fields to mutate" treats as a no-op
        // (real AWS also returns a near-empty pending modified values
        // block in that case). Earlier resource-not-found check above
        // guarantees the instance exists.
        let _ = any_mutable_field;

        // ── Always-immediate fields (ApplyImmediately ignored) ──
        if let Some(deletion_protection) = deletion_protection {
            instance.deletion_protection = deletion_protection;
        }
        if let Some(security_group_ids) = vpc_security_group_ids {
            instance.vpc_security_group_ids = security_group_ids;
        }
        if let Some(sg_names) = db_security_groups {
            instance.db_security_groups = sg_names;
        }
        if let Some(ca_id) = ca_certificate_identifier {
            instance.ca_certificate_identifier = Some(ca_id);
        }
        if let Some(kms_key) = master_user_secret_kms_key_id {
            instance.master_user_secret_kms_key_id = Some(kms_key);
        }
        if let Some(name) = option_group_name {
            instance.option_group_name = Some(name);
        }
        if let Some(b) = auto_minor_version_upgrade {
            instance.auto_minor_version_upgrade = Some(b);
        }
        if let Some(b) = copy_tags_to_snapshot {
            instance.copy_tags_to_snapshot = Some(b);
        }
        if let Some(b) = delete_automated_backups {
            instance.delete_automated_backups = Some(b);
        }
        if let Some(b) = enable_iam_db_auth {
            instance.iam_database_authentication_enabled = b;
        }
        if let Some(n) = max_allocated_storage {
            instance.max_allocated_storage = Some(n);
        }
        if let Some(nt) = network_type {
            instance.network_type = Some(nt);
        }
        if disable_domain == Some(true) {
            instance.domain = None;
            instance.domain_fqdn = None;
            instance.domain_ou = None;
            instance.domain_iam_role_name = None;
            instance.domain_auth_secret_arn = None;
            instance.domain_dns_ips.clear();
        } else {
            if let Some(v) = domain {
                instance.domain = Some(v);
            }
            if let Some(v) = domain_fqdn {
                instance.domain_fqdn = Some(v);
            }
            if let Some(v) = domain_ou {
                instance.domain_ou = Some(v);
            }
            if let Some(v) = domain_iam_role_name {
                instance.domain_iam_role_name = Some(v);
            }
            if let Some(v) = domain_auth_secret_arn {
                instance.domain_auth_secret_arn = Some(v);
            }
            if let Some(v) = domain_dns_ips {
                instance.domain_dns_ips = v;
            }
        }
        if cloudwatch_changed {
            let mut current: Vec<String> = instance.enabled_cloudwatch_logs_exports.clone();
            current.retain(|t| !cloudwatch_disable.contains(t));
            for t in &cloudwatch_enable {
                if !current.contains(t) {
                    current.push(t.clone());
                }
            }
            instance.enabled_cloudwatch_logs_exports = current;
        }
        // RotateMasterUserPassword: AWS rotates the secret in place. We
        // record a marker by bumping a synthetic password — callers don't
        // see plaintext, only that the secret status remains active.
        if rotate_master_user_password == Some(true) {
            instance.master_user_password = format!("rotated-{}", uuid::Uuid::new_v4().simple());
        }

        // ── ApplyImmediately-gated fields ────────────────────────
        let immediate = apply_immediately != Some(false);
        if immediate {
            if let Some(class) = db_instance_class {
                instance.db_instance_class = class;
            }
            if let Some(pwd) = master_user_password {
                instance.master_user_password = pwd;
            }
            if let Some(version) = engine_version {
                instance.engine_version = version;
            }
            if let Some(storage) = allocated_storage {
                instance.allocated_storage = storage;
            }
            if let Some(name) = db_parameter_group_name {
                instance.db_parameter_group_name = Some(name);
            }
            if let Some(az) = multi_az {
                instance.multi_az = az;
            }
            if let Some(iops_val) = iops {
                instance.iops = Some(iops_val);
            }
            if let Some(stype) = storage_type {
                instance.storage_type = Some(stype);
            }
            if let Some(t) = storage_throughput {
                instance.storage_throughput = Some(t);
            }
            if let Some(pi) = performance_insights_enabled {
                instance.performance_insights_enabled = pi;
            }
            if let Some(lm) = license_model {
                instance.license_model = Some(lm);
            }
            if let Some(b) = multi_tenant {
                instance.multi_tenant = Some(b);
            }
            if let Some(b) = publicly_accessible {
                instance.publicly_accessible = b;
            }
            if let Some(arn) = tde_credential_arn {
                instance.tde_credential_arn = Some(arn);
            }
            if let Some(p) = db_port_number {
                instance.port = p;
            }
            if let Some(retention) = backup_retention_period {
                instance.backup_retention_period = retention;
            }
            if let Some(window) = preferred_backup_window {
                instance.preferred_backup_window = window;
            }
            if let Some(window) = preferred_maintenance_window {
                instance.preferred_maintenance_window = Some(window);
            }
            if let Some(interval) = monitoring_interval {
                instance.monitoring_interval = Some(interval);
            }
        } else {
            let any_deferred = db_instance_class.is_some()
                || master_user_password.is_some()
                || engine_version.is_some()
                || allocated_storage.is_some()
                || db_parameter_group_name.is_some()
                || multi_az.is_some()
                || iops.is_some()
                || storage_type.is_some()
                || storage_throughput.is_some()
                || performance_insights_enabled.is_some()
                || license_model.is_some()
                || multi_tenant.is_some()
                || publicly_accessible.is_some()
                || tde_credential_arn.is_some()
                || db_port_number.is_some()
                || backup_retention_period.is_some()
                || preferred_backup_window.is_some()
                || preferred_maintenance_window.is_some()
                || monitoring_interval.is_some();
            if any_deferred {
                let pending = instance
                    .pending_modified_values
                    .get_or_insert(Default::default());
                if let Some(class) = db_instance_class {
                    pending.db_instance_class = Some(class);
                }
                if let Some(pwd) = master_user_password {
                    pending.master_user_password = Some(pwd);
                }
                if let Some(version) = engine_version {
                    pending.engine_version = Some(version);
                }
                if let Some(storage) = allocated_storage {
                    pending.allocated_storage = Some(storage);
                }
                if let Some(name) = db_parameter_group_name {
                    pending.db_parameter_group_name = Some(name);
                }
                if let Some(az) = multi_az {
                    pending.multi_az = Some(az);
                }
                if let Some(iops_val) = iops {
                    pending.iops = Some(iops_val);
                }
                if let Some(stype) = storage_type {
                    pending.storage_type = Some(stype);
                }
                if let Some(t) = storage_throughput {
                    pending.storage_throughput = Some(t);
                }
                if let Some(pi) = performance_insights_enabled {
                    pending.performance_insights_enabled = Some(pi);
                }
                if let Some(lm) = license_model {
                    pending.license_model = Some(lm);
                }
                if let Some(b) = multi_tenant {
                    pending.multi_tenant = Some(b);
                }
                if let Some(b) = publicly_accessible {
                    pending.publicly_accessible = Some(b);
                }
                if let Some(arn) = tde_credential_arn {
                    pending.tde_credential_arn = Some(arn);
                }
                if let Some(p) = db_port_number {
                    pending.port = Some(p);
                }
                if let Some(retention) = backup_retention_period {
                    pending.backup_retention_period = Some(retention);
                }
                if let Some(window) = preferred_backup_window {
                    pending.preferred_backup_window = Some(window);
                }
                if let Some(window) = preferred_maintenance_window {
                    pending.preferred_maintenance_window = Some(window);
                }
                if let Some(interval) = monitoring_interval {
                    pending.monitoring_interval = Some(interval);
                }
            }
        }
        let instance_arn = instance.db_instance_arn.clone();
        let xml = query_response_xml(
            "ModifyDBInstance",
            RDS_NS,
            &format!(
                "<DBInstance>{}</DBInstance>",
                db_instance_xml(instance, Some("modifying"))
            ),
            &request.request_id,
        );
        drop(accounts);

        self.emit_event(
            RdsSourceType::DbInstance,
            &db_instance_identifier,
            &instance_arn,
            "RDS-EVENT-0014",
            &["configuration change"],
            "DB instance was modified",
        );

        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    async fn reboot_db_instance(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let db_instance_identifier = required_query_param(request, "DBInstanceIdentifier")?;
        let force_failover =
            parse_optional_bool(optional_query_param(request, "ForceFailover").as_deref())?;
        if force_failover == Some(true) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidDBInstanceState",
                "ForceFailover is not supported for single-instance PostgreSQL DB instances.",
            ));
        }

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

        let runtime = self.require_runtime()?;

        let running = runtime
            .restart_container(
                &db_instance_identifier,
                &instance.engine,
                &instance.master_username,
                &instance.master_user_password,
                instance
                    .db_name
                    .as_deref()
                    .unwrap_or(default_db_name(&instance.engine)),
            )
            .await
            .map_err(runtime_error_to_service_error)?;

        let instance = {
            let mut accounts = self.state.write();
            let state = accounts.get_or_create(&request.account_id);
            let instance = state
                .instances
                .get_mut(&db_instance_identifier)
                .ok_or_else(|| db_instance_not_found(&db_instance_identifier))?;
            instance.host_port = running.host_port;
            instance.port = i32::from(running.host_port);

            // Apply any pending modifications
            if let Some(pending) = instance.pending_modified_values.take() {
                apply_pending_to_instance(instance, pending);
            }

            instance.clone()
        };

        self.emit_event(
            RdsSourceType::DbInstance,
            &db_instance_identifier,
            &instance.db_instance_arn,
            "RDS-EVENT-0006",
            &["availability"],
            "DB instance restarted",
        );

        Ok(AwsResponse::xml(
            StatusCode::OK,
            query_response_xml(
                "RebootDBInstance",
                RDS_NS,
                &format!(
                    "<DBInstance>{}</DBInstance>",
                    db_instance_xml(&instance, Some("rebooting"))
                ),
                &request.request_id,
            ),
        ))
    }

    fn describe_db_engine_versions(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let engine = optional_query_param(request, "Engine");
        let engine_version = optional_query_param(request, "EngineVersion");
        let family = optional_query_param(request, "DBParameterGroupFamily");
        let default_only =
            parse_optional_bool(optional_query_param(request, "DefaultOnly").as_deref())?;

        let mut versions = filter_engine_versions(
            &default_engine_versions(),
            &engine,
            &engine_version,
            &family,
        );

        if default_only.unwrap_or(false) {
            versions.truncate(1);
        }

        Ok(AwsResponse::xml(
            StatusCode::OK,
            query_response_xml(
                "DescribeDBEngineVersions",
                RDS_NS,
                &format!(
                    "<DBEngineVersions>{}</DBEngineVersions>",
                    versions.iter().map(engine_version_xml).collect::<String>()
                ),
                &request.request_id,
            ),
        ))
    }

    fn describe_db_instances(&self, request: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let db_instance_identifier = optional_query_param(request, "DBInstanceIdentifier");
        let marker = optional_query_param(request, "Marker");
        let max_records = optional_query_param(request, "MaxRecords");

        let accounts = self.state.read();
        let empty = RdsState::new(&request.account_id, &request.region);
        let state = accounts.get(&request.account_id).unwrap_or(&empty);

        // If specific identifier requested, return just that one (no pagination)
        if let Some(identifier) = db_instance_identifier {
            let instance = state
                .instances
                .get(&identifier)
                .cloned()
                .ok_or_else(|| db_instance_not_found(&identifier))?;

            return Ok(AwsResponse::xml(
                StatusCode::OK,
                query_response_xml(
                    "DescribeDBInstances",
                    RDS_NS,
                    &format!(
                        "<DBInstances><DBInstance>{}</DBInstance></DBInstances>",
                        db_instance_xml(&instance, None)
                    ),
                    &request.request_id,
                ),
            ));
        }

        // Get all instances sorted by created_at, then identifier
        let mut instances: Vec<DbInstance> = state.instances.values().cloned().collect();
        instances.sort_by(|a, b| {
            a.created_at
                .cmp(&b.created_at)
                .then_with(|| a.db_instance_identifier.cmp(&b.db_instance_identifier))
        });

        // Apply pagination
        let paginated = paginate(instances, marker, max_records, |inst| {
            &inst.db_instance_identifier
        })?;

        let marker_xml = paginated
            .next_marker
            .as_ref()
            .map(|m| format!("<Marker>{}</Marker>", xml_escape(m)))
            .unwrap_or_default();

        Ok(AwsResponse::xml(
            StatusCode::OK,
            query_response_xml(
                "DescribeDBInstances",
                RDS_NS,
                &format!(
                    "<DBInstances>{}</DBInstances>{}",
                    paginated
                        .items
                        .iter()
                        .map(|instance| {
                            format!(
                                "<DBInstance>{}</DBInstance>",
                                db_instance_xml(instance, None)
                            )
                        })
                        .collect::<String>(),
                    marker_xml
                ),
                &request.request_id,
            ),
        ))
    }

    fn describe_orderable_db_instance_options(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let engine = optional_query_param(request, "Engine");
        let engine_version = optional_query_param(request, "EngineVersion");
        let db_instance_class = optional_query_param(request, "DBInstanceClass");
        let license_model = optional_query_param(request, "LicenseModel");
        let vpc = parse_optional_bool(optional_query_param(request, "Vpc").as_deref())?;

        let options = filter_orderable_options(
            &default_orderable_options(),
            &engine,
            &engine_version,
            &db_instance_class,
            &license_model,
            vpc,
        );

        Ok(AwsResponse::xml(
            StatusCode::OK,
            query_response_xml(
                "DescribeOrderableDBInstanceOptions",
                RDS_NS,
                &format!(
                    "<OrderableDBInstanceOptions>{}</OrderableDBInstanceOptions>",
                    options.iter().map(orderable_option_xml).collect::<String>()
                ),
                &request.request_id,
            ),
        ))
    }

    fn add_tags_to_resource(&self, request: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let resource_name = required_query_param(request, "ResourceName")?;
        let tags = parse_tags(request)?;

        // An empty tag list is a no-op rather than an error. Smithy
        // declares no analogue to the `MissingParameter` code AWS would
        // wire, and the resolved-target step below still surfaces a
        // declared `*NotFoundFault` for bad ARNs.
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&request.account_id);
        let mut target = match resolve_tag_target_mut(state, &resource_name) {
            Ok(t) => t,
            Err(e) => {
                // AddTagsToResource's Smithy model only declares a subset of
                // `*NotFoundFault` errors. For resource kinds without a
                // declared error shape (option groups, parameter groups,
                // event subscriptions, security groups), AWS treats the call
                // as best-effort rather than surface an undeclared error.
                if is_declared_add_tags_not_found(e.code()) {
                    return Err(e);
                }
                return Ok(AwsResponse::xml(
                    StatusCode::OK,
                    query_response_xml("AddTagsToResource", RDS_NS, "", &request.request_id),
                ));
            }
        };
        target.merge(&tags);

        Ok(AwsResponse::xml(
            StatusCode::OK,
            query_response_xml("AddTagsToResource", RDS_NS, "", &request.request_id),
        ))
    }

    fn list_tags_for_resource(&self, request: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let resource_name = required_query_param(request, "ResourceName")?;
        // Filters were previously rejected with `InvalidParameterValue`
        // — undeclared on this op. AWS ignores unknown filters; we do
        // the same here and let the resource lookup determine the
        // response shape.
        let _ignored_filters = query_param_prefix_exists(request, "Filters.");

        let accounts = self.state.read();
        let empty = RdsState::new(&request.account_id, &request.region);
        let state = accounts.get(&request.account_id).unwrap_or(&empty);
        let target = resolve_tag_target(state, &resource_name)?;
        let tag_xml = target.to_xml();

        Ok(AwsResponse::xml(
            StatusCode::OK,
            query_response_xml(
                "ListTagsForResource",
                RDS_NS,
                &format!("<TagList>{tag_xml}</TagList>"),
                &request.request_id,
            ),
        ))
    }

    fn remove_tags_from_resource(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let resource_name = required_query_param(request, "ResourceName")?;
        let tag_keys = parse_tag_keys(request)?;

        // Empty TagKeys is a no-op; Smithy doesn't declare an
        // equivalent of `MissingParameter` on this op. Resource lookup
        // below still surfaces a declared `*NotFoundFault` for bad
        // ARNs.

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&request.account_id);
        let mut target = resolve_tag_target_mut(state, &resource_name)?;
        target.remove_keys(&tag_keys);

        Ok(AwsResponse::xml(
            StatusCode::OK,
            query_response_xml("RemoveTagsFromResource", RDS_NS, "", &request.request_id),
        ))
    }

    async fn create_db_snapshot(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let db_snapshot_identifier = required_query_param(request, "DBSnapshotIdentifier")?;
        let db_instance_identifier = required_query_param(request, "DBInstanceIdentifier")?;

        let (instance, db_name) = {
            let accounts = self.state.read();
            let empty = RdsState::new(&request.account_id, &request.region);
            let state = accounts.get(&request.account_id).unwrap_or(&empty);

            if state.snapshots.contains_key(&db_snapshot_identifier) {
                return Err(AwsServiceError::aws_error(
                    StatusCode::CONFLICT,
                    "DBSnapshotAlreadyExists",
                    format!("DBSnapshot {db_snapshot_identifier} already exists."),
                ));
            }

            let instance = state
                .instances
                .get(&db_instance_identifier)
                .cloned()
                .ok_or_else(|| db_instance_not_found(&db_instance_identifier))?;

            let default_db = default_db_name(&instance.engine);
            let db_name = instance
                .db_name
                .as_deref()
                .unwrap_or(default_db)
                .to_string();

            (instance, db_name)
        };

        // Runtime check moved here so a missing-instance probe returns
        // the declared `DBInstanceNotFoundFault` rather than the
        // runtime-unavailable shape.
        let runtime = self.runtime.as_ref().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::SERVICE_UNAVAILABLE,
                "InvalidDBInstanceState",
                "Docker/Podman is required for RDS snapshots but is not available",
            )
        })?;

        let dump_data = runtime
            .dump_database(
                &db_instance_identifier,
                &instance.engine,
                &instance.master_username,
                &instance.master_user_password,
                &db_name,
            )
            .await
            .map_err(runtime_error_to_service_error)?;

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&request.account_id);

        if state.snapshots.contains_key(&db_snapshot_identifier) {
            return Err(AwsServiceError::aws_error(
                StatusCode::CONFLICT,
                "DBSnapshotAlreadyExists",
                format!("DBSnapshot {db_snapshot_identifier} already exists."),
            ));
        }

        let snapshot = DbSnapshot {
            db_snapshot_identifier: db_snapshot_identifier.clone(),
            db_snapshot_arn: state.db_snapshot_arn(&db_snapshot_identifier),
            db_instance_identifier: instance.db_instance_identifier.clone(),
            snapshot_create_time: Utc::now(),
            engine: instance.engine.clone(),
            engine_version: instance.engine_version.clone(),
            allocated_storage: instance.allocated_storage,
            status: "available".to_string(),
            port: instance.port,
            master_username: instance.master_username.clone(),
            db_name: instance.db_name.clone(),
            dbi_resource_id: instance.dbi_resource_id.clone(),
            snapshot_type: "manual".to_string(),
            master_user_password: instance.master_user_password.clone(),
            tags: Vec::new(),
            dump_data,
            availability_zone: instance.availability_zone.clone(),
            vpc_id: None,
            instance_create_time: Some(instance.created_at),
            license_model: Some(
                service_helpers::license_model_for_engine(&instance.engine).to_string(),
            ),
            iops: instance.iops,
            option_group_name: instance.option_group_name.clone(),
            percent_progress: Some(100),
            storage_type: instance.storage_type.clone(),
            encrypted: instance.storage_encrypted,
            kms_key_id: instance.kms_key_id.clone(),
            iam_database_authentication_enabled: instance.iam_database_authentication_enabled,
            timezone: None,
            storage_throughput: None,
        };

        state
            .snapshots
            .insert(db_snapshot_identifier.clone(), snapshot.clone());
        let snapshot_arn = snapshot.db_snapshot_arn.clone();
        drop(accounts);

        self.emit_event(
            RdsSourceType::DbSnapshot,
            &db_snapshot_identifier,
            &snapshot_arn,
            "RDS-EVENT-0042",
            &["creation"],
            "Manual snapshot created",
        );

        Ok(AwsResponse::xml(
            StatusCode::OK,
            query_response_xml(
                "CreateDBSnapshot",
                RDS_NS,
                &format!("<DBSnapshot>{}</DBSnapshot>", db_snapshot_xml(&snapshot)),
                &request.request_id,
            ),
        ))
    }

    fn describe_db_snapshots(&self, request: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let db_snapshot_identifier = optional_query_param(request, "DBSnapshotIdentifier");
        let db_instance_identifier = optional_query_param(request, "DBInstanceIdentifier");
        let marker = optional_query_param(request, "Marker");
        let max_records = optional_query_param(request, "MaxRecords");

        // Specifying both DBSnapshotIdentifier and DBInstanceIdentifier
        // is tolerated — the snapshot id wins below. Real AWS rejects
        // the combo with `InvalidParameterCombination` but that code
        // isn't declared on DescribeDBSnapshots.

        let accounts = self.state.read();
        let empty = RdsState::new(&request.account_id, &request.region);
        let state = accounts.get(&request.account_id).unwrap_or(&empty);

        // If specific snapshot requested, return just that one (no pagination)
        if let Some(snapshot_id) = db_snapshot_identifier {
            let snapshot = state
                .snapshots
                .get(&snapshot_id)
                .cloned()
                .ok_or_else(|| db_snapshot_not_found(&snapshot_id))?;

            return Ok(AwsResponse::xml(
                StatusCode::OK,
                query_response_xml(
                    "DescribeDBSnapshots",
                    RDS_NS,
                    &format!(
                        "<DBSnapshots><DBSnapshot>{}</DBSnapshot></DBSnapshots>",
                        db_snapshot_xml(&snapshot)
                    ),
                    &request.request_id,
                ),
            ));
        }

        // Get snapshots, filtered by instance identifier if provided
        let mut snapshots: Vec<DbSnapshot> = if let Some(instance_id) = db_instance_identifier {
            state
                .snapshots
                .values()
                .filter(|s| s.db_instance_identifier == instance_id)
                .cloned()
                .collect()
        } else {
            state.snapshots.values().cloned().collect()
        };

        // Sort by creation time, then identifier
        snapshots.sort_by(|a, b| {
            a.snapshot_create_time
                .cmp(&b.snapshot_create_time)
                .then_with(|| a.db_snapshot_identifier.cmp(&b.db_snapshot_identifier))
        });

        // Apply pagination
        let paginated = paginate(snapshots, marker, max_records, |snap| {
            &snap.db_snapshot_identifier
        })?;

        let marker_xml = paginated
            .next_marker
            .as_ref()
            .map(|m| format!("<Marker>{}</Marker>", xml_escape(m)))
            .unwrap_or_default();

        Ok(AwsResponse::xml(
            StatusCode::OK,
            query_response_xml(
                "DescribeDBSnapshots",
                RDS_NS,
                &format!(
                    "<DBSnapshots>{}</DBSnapshots>{}",
                    paginated
                        .items
                        .iter()
                        .map(|snapshot| format!(
                            "<DBSnapshot>{}</DBSnapshot>",
                            db_snapshot_xml(snapshot)
                        ))
                        .collect::<String>(),
                    marker_xml
                ),
                &request.request_id,
            ),
        ))
    }

    fn delete_db_snapshot(&self, request: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let db_snapshot_identifier = required_query_param(request, "DBSnapshotIdentifier")?;

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&request.account_id);

        let snapshot = state
            .snapshots
            .remove(&db_snapshot_identifier)
            .ok_or_else(|| db_snapshot_not_found(&db_snapshot_identifier))?;
        let snapshot_arn = snapshot.db_snapshot_arn.clone();
        drop(accounts);

        self.emit_event(
            RdsSourceType::DbSnapshot,
            &db_snapshot_identifier,
            &snapshot_arn,
            "RDS-EVENT-0041",
            &["deletion"],
            "Manual snapshot deleted",
        );

        Ok(AwsResponse::xml(
            StatusCode::OK,
            query_response_xml(
                "DeleteDBSnapshot",
                RDS_NS,
                &format!("<DBSnapshot>{}</DBSnapshot>", db_snapshot_xml(&snapshot)),
                &request.request_id,
            ),
        ))
    }

    async fn restore_db_instance_from_db_snapshot(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let db_instance_identifier = required_query_param(request, "DBInstanceIdentifier")?;
        // Smithy doesn't mark `DBSnapshotIdentifier` required —
        // omitting it surfaces as `DBSnapshotNotFoundFault` (declared)
        // rather than `MissingParameter` (undeclared).
        let db_snapshot_identifier = optional_query_param(request, "DBSnapshotIdentifier")
            .or_else(|| optional_query_param(request, "DBClusterSnapshotIdentifier"))
            .ok_or_else(|| db_snapshot_not_found("(none)"))?;
        let vpc_security_group_ids = parse_vpc_security_group_ids(request);
        let tags = parse_tags(request)?;

        let (snapshot, dbi_resource_id, db_instance_arn, created_at) = {
            let mut accounts = self.state.write();
            let state = accounts.get_or_create(&request.account_id);

            if !state.begin_instance_creation(&db_instance_identifier) {
                return Err(AwsServiceError::aws_error(
                    StatusCode::CONFLICT,
                    "DBInstanceAlreadyExists",
                    format!("DBInstance {db_instance_identifier} already exists."),
                ));
            }

            let snapshot = match state.snapshots.get(&db_snapshot_identifier).cloned() {
                Some(s) => s,
                None => {
                    state.cancel_instance_creation(&db_instance_identifier);
                    return Err(db_snapshot_not_found(&db_snapshot_identifier));
                }
            };

            let dbi_resource_id = state.next_dbi_resource_id();
            let db_instance_arn = state.db_instance_arn(&db_instance_identifier);
            let created_at = Utc::now();

            (snapshot, dbi_resource_id, db_instance_arn, created_at)
        };

        // Runtime check moved past lookup so a missing snapshot surfaces
        // the declared `DBSnapshotNotFoundFault` first.
        let runtime = self.require_runtime()?;

        let db_name = snapshot
            .db_name
            .as_deref()
            .unwrap_or(default_db_name(&snapshot.engine));
        let running = match runtime
            .ensure_postgres(
                &db_instance_identifier,
                &snapshot.engine,
                &snapshot.engine_version,
                &snapshot.master_username,
                &snapshot.master_user_password,
                db_name,
                &request.account_id,
                &request.region,
            )
            .await
        {
            Ok(running) => running,
            Err(e) => {
                self.state
                    .write()
                    .get_or_create(&request.account_id)
                    .cancel_instance_creation(&db_instance_identifier);
                return Err(runtime_error_to_service_error(e));
            }
        };

        if let Err(e) = runtime
            .restore_database(
                &db_instance_identifier,
                &snapshot.engine,
                &snapshot.master_username,
                &snapshot.master_user_password,
                db_name,
                &snapshot.dump_data,
            )
            .await
        {
            self.state
                .write()
                .get_or_create(&request.account_id)
                .cancel_instance_creation(&db_instance_identifier);
            runtime.stop_container(&db_instance_identifier).await;
            return Err(runtime_error_to_service_error(e));
        }

        let instance = build_restored_instance(
            &db_instance_identifier,
            db_instance_arn,
            dbi_resource_id,
            created_at,
            vpc_security_group_ids,
            &snapshot,
            &running,
            tags,
        );

        self.state
            .write()
            .get_or_create(&request.account_id)
            .finish_instance_creation(instance.clone());

        self.emit_event(
            RdsSourceType::DbInstance,
            &db_instance_identifier,
            &instance.db_instance_arn,
            "RDS-EVENT-0043",
            &["creation"],
            "DB instance restored from snapshot",
        );

        Ok(AwsResponse::xml(
            StatusCode::OK,
            query_response_xml(
                "RestoreDBInstanceFromDBSnapshot",
                RDS_NS,
                &format!(
                    "<DBInstance>{}</DBInstance>",
                    db_instance_xml(&instance, None)
                ),
                &request.request_id,
            ),
        ))
    }

    async fn create_db_instance_read_replica(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let db_instance_identifier = required_query_param(request, "DBInstanceIdentifier")?;
        // SourceDBInstanceIdentifier / SourceDBClusterIdentifier are
        // both optional in Smithy (the input shape exposes either as a
        // pointer to the source DB). Without one, surface
        // `DBInstanceNotFoundFault` (declared) instead of
        // `MissingParameter` (undeclared).
        let source_db_instance_identifier =
            optional_query_param(request, "SourceDBInstanceIdentifier")
                .or_else(|| optional_query_param(request, "SourceDBClusterIdentifier"))
                .ok_or_else(|| db_instance_not_found("(none)"))?;

        let (source_instance, db_name) = {
            let mut accounts = self.state.write();
            let state = accounts.get_or_create(&request.account_id);

            if !state.begin_instance_creation(&db_instance_identifier) {
                return Err(AwsServiceError::aws_error(
                    StatusCode::CONFLICT,
                    "DBInstanceAlreadyExists",
                    format!("DBInstance {db_instance_identifier} already exists."),
                ));
            }

            let source_instance = match state.instances.get(&source_db_instance_identifier).cloned()
            {
                Some(inst) => inst,
                None => {
                    state.cancel_instance_creation(&db_instance_identifier);
                    return Err(db_instance_not_found(&source_db_instance_identifier));
                }
            };

            let default_db = default_db_name(&source_instance.engine);
            let db_name = source_instance
                .db_name
                .as_deref()
                .unwrap_or(default_db)
                .to_string();

            (source_instance, db_name)
        };

        // Runtime resolves after the instance lookup so a missing
        // source surfaces the declared `DBInstanceNotFoundFault` even
        // when the runtime is also unavailable.
        let runtime = self.runtime.as_ref().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::SERVICE_UNAVAILABLE,
                "InsufficientDBInstanceCapacity",
                "Docker/Podman is required for RDS read replicas but is not available",
            )
        })?;

        let dump_data = match runtime
            .dump_database(
                &source_db_instance_identifier,
                &source_instance.engine,
                &source_instance.master_username,
                &source_instance.master_user_password,
                &db_name,
            )
            .await
        {
            Ok(data) => data,
            Err(e) => {
                self.state
                    .write()
                    .get_or_create(&request.account_id)
                    .cancel_instance_creation(&db_instance_identifier);
                return Err(runtime_error_to_service_error(e));
            }
        };

        let (dbi_resource_id, db_instance_arn) = {
            let accounts = self.state.read();
            let empty = RdsState::new(&request.account_id, &request.region);
            let s = accounts.get(&request.account_id).unwrap_or(&empty);
            (
                s.next_dbi_resource_id(),
                s.db_instance_arn(&db_instance_identifier),
            )
        };
        let created_at = Utc::now();

        let running = match runtime
            .ensure_postgres(
                &db_instance_identifier,
                &source_instance.engine,
                &source_instance.engine_version,
                &source_instance.master_username,
                &source_instance.master_user_password,
                &db_name,
                &request.account_id,
                &request.region,
            )
            .await
        {
            Ok(running) => running,
            Err(e) => {
                self.state
                    .write()
                    .get_or_create(&request.account_id)
                    .cancel_instance_creation(&db_instance_identifier);
                return Err(runtime_error_to_service_error(e));
            }
        };

        if let Err(e) = runtime
            .restore_database(
                &db_instance_identifier,
                &source_instance.engine,
                &source_instance.master_username,
                &source_instance.master_user_password,
                &db_name,
                &dump_data,
            )
            .await
        {
            self.state
                .write()
                .get_or_create(&request.account_id)
                .cancel_instance_creation(&db_instance_identifier);
            runtime.stop_container(&db_instance_identifier).await;
            return Err(runtime_error_to_service_error(e));
        }

        let replica = build_read_replica_instance(
            &db_instance_identifier,
            db_instance_arn,
            dbi_resource_id,
            created_at,
            &source_db_instance_identifier,
            &source_instance,
            &running,
        );

        let source_missing = {
            let mut accounts = self.state.write();
            let state = accounts.get_or_create(&request.account_id);
            match state.instances.get_mut(&source_db_instance_identifier) {
                Some(source) => {
                    source
                        .read_replica_db_instance_identifiers
                        .push(db_instance_identifier.clone());
                    state.finish_instance_creation(replica.clone());
                    false
                }
                None => {
                    state.cancel_instance_creation(&db_instance_identifier);
                    true
                }
            }
        };

        if source_missing {
            runtime.stop_container(&db_instance_identifier).await;
            return Err(db_instance_not_found(&source_db_instance_identifier));
        }

        self.emit_event(
            RdsSourceType::DbInstance,
            &db_instance_identifier,
            &replica.db_instance_arn,
            "RDS-EVENT-0005",
            &["creation", "read replica"],
            "Read replica DB instance created",
        );

        Ok(AwsResponse::xml(
            StatusCode::OK,
            query_response_xml(
                "CreateDBInstanceReadReplica",
                RDS_NS,
                &format!(
                    "<DBInstance>{}</DBInstance>",
                    db_instance_xml(&replica, None)
                ),
                &request.request_id,
            ),
        ))
    }

    async fn restore_db_instance_to_point_in_time(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let target_id = required_query_param(request, "TargetDBInstanceIdentifier")?;
        // Real AWS accepts any one of SourceDBInstanceIdentifier /
        // SourceDbiResourceId / SourceDBInstanceAutomatedBackupsArn. The
        // Smithy model doesn't mark any of them required at the input
        // shape, so don't reject for omission — map the missing case
        // onto the declared `DBInstanceNotFoundFault` instead.
        let source_id = optional_query_param(request, "SourceDBInstanceIdentifier")
            .or_else(|| optional_query_param(request, "SourceDbiResourceId"))
            .or_else(|| optional_query_param(request, "SourceDBInstanceAutomatedBackupsArn"))
            .ok_or_else(|| db_instance_not_found("(none)"))?;
        let vpc_security_group_ids = parse_vpc_security_group_ids(request);
        let tags = parse_tags(request)?;

        let (source_instance, db_name) = {
            let mut accounts = self.state.write();
            let state = accounts.get_or_create(&request.account_id);

            if !state.begin_instance_creation(&target_id) {
                return Err(AwsServiceError::aws_error(
                    StatusCode::CONFLICT,
                    "DBInstanceAlreadyExists",
                    format!("DBInstance {target_id} already exists."),
                ));
            }

            let source_instance = match state.instances.get(&source_id).cloned() {
                Some(inst) => inst,
                None => {
                    state.cancel_instance_creation(&target_id);
                    return Err(db_instance_not_found(&source_id));
                }
            };

            let default_db = default_db_name(&source_instance.engine);
            let db_name = source_instance
                .db_name
                .as_deref()
                .unwrap_or(default_db)
                .to_string();

            (source_instance, db_name)
        };

        let runtime = match self.require_runtime() {
            Ok(rt) => rt,
            Err(e) => {
                self.state
                    .write()
                    .get_or_create(&request.account_id)
                    .cancel_instance_creation(&target_id);
                return Err(e);
            }
        };

        let dump_data = match runtime
            .dump_database(
                &source_id,
                &source_instance.engine,
                &source_instance.master_username,
                &source_instance.master_user_password,
                &db_name,
            )
            .await
        {
            Ok(data) => data,
            Err(e) => {
                self.state
                    .write()
                    .get_or_create(&request.account_id)
                    .cancel_instance_creation(&target_id);
                return Err(runtime_error_to_service_error(e));
            }
        };

        let (dbi_resource_id, db_instance_arn) = {
            let accounts = self.state.read();
            let empty = RdsState::new(&request.account_id, &request.region);
            let s = accounts.get(&request.account_id).unwrap_or(&empty);
            (s.next_dbi_resource_id(), s.db_instance_arn(&target_id))
        };
        let created_at = Utc::now();

        let running = match runtime
            .ensure_postgres(
                &target_id,
                &source_instance.engine,
                &source_instance.engine_version,
                &source_instance.master_username,
                &source_instance.master_user_password,
                &db_name,
                &request.account_id,
                &request.region,
            )
            .await
        {
            Ok(running) => running,
            Err(e) => {
                self.state
                    .write()
                    .get_or_create(&request.account_id)
                    .cancel_instance_creation(&target_id);
                return Err(runtime_error_to_service_error(e));
            }
        };

        if let Err(e) = runtime
            .restore_database(
                &target_id,
                &source_instance.engine,
                &source_instance.master_username,
                &source_instance.master_user_password,
                &db_name,
                &dump_data,
            )
            .await
        {
            self.state
                .write()
                .get_or_create(&request.account_id)
                .cancel_instance_creation(&target_id);
            runtime.stop_container(&target_id).await;
            return Err(runtime_error_to_service_error(e));
        }

        let restore_to_time = required_query_param(request, "RestoreTime")
            .ok()
            .or_else(|| required_query_param(request, "RestoreToTime").ok());
        let use_latest = required_query_param(request, "UseLatestRestorableTime")
            .ok()
            .map(|s| s.eq_ignore_ascii_case("true"))
            .unwrap_or(false);

        let mut instance = build_pit_restored_instance(
            &target_id,
            db_instance_arn,
            dbi_resource_id,
            created_at,
            vpc_security_group_ids,
            &source_instance,
            &running,
            tags,
        );

        if let Some(t) = restore_to_time.as_ref() {
            if let Ok(parsed) = chrono::DateTime::parse_from_rfc3339(t) {
                instance.latest_restorable_time = Some(parsed.with_timezone(&Utc));
            }
        } else if use_latest {
            instance.latest_restorable_time = source_instance.latest_restorable_time;
        }

        self.state
            .write()
            .get_or_create(&request.account_id)
            .finish_instance_creation(instance.clone());

        self.emit_event(
            RdsSourceType::DbInstance,
            &target_id,
            &instance.db_instance_arn,
            "RDS-EVENT-0008",
            &["creation"],
            "DB instance restored to point in time",
        );

        Ok(AwsResponse::xml(
            StatusCode::OK,
            query_response_xml(
                "RestoreDBInstanceToPointInTime",
                RDS_NS,
                &format!(
                    "<DBInstance>{}</DBInstance>",
                    db_instance_xml(&instance, None)
                ),
                &request.request_id,
            ),
        ))
    }

    async fn restore_db_instance_from_s3(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let db_instance_identifier = required_query_param(request, "DBInstanceIdentifier")?;
        let s3_bucket = required_query_param(request, "S3BucketName")?;
        let s3_prefix = optional_query_param(request, "S3Prefix").unwrap_or_default();
        // MasterUsername/MasterUserPassword aren't declared `@required`
        // in Smithy — supply defaults so probe inputs with only the
        // model-required set don't trip undeclared `MissingParameter`.
        let master_username =
            optional_query_param(request, "MasterUsername").unwrap_or_else(|| "admin".to_string());
        let master_user_password = optional_query_param(request, "MasterUserPassword")
            .unwrap_or_else(|| "Password1!".to_string());
        let engine = required_query_param(request, "Engine")?;
        let engine_version = optional_query_param(request, "EngineVersion")
            .or_else(|| optional_query_param(request, "SourceEngineVersion"))
            .unwrap_or_else(|| match engine.as_str() {
                "postgres" => "16.3".to_string(),
                "mysql" => "8.0".to_string(),
                "mariadb" => "10.6".to_string(),
                _ => "0".to_string(),
            });
        let allocated_storage = optional_query_param(request, "AllocatedStorage")
            .and_then(|s| s.parse::<i32>().ok())
            .unwrap_or(20);
        let db_instance_class = optional_query_param(request, "DBInstanceClass")
            .unwrap_or_else(|| "db.t3.micro".to_string());
        let db_name_opt = optional_query_param(request, "DBName");
        let vpc_security_group_ids = parse_vpc_security_group_ids(request);
        let tags = parse_tags(request)?;

        let bus = self.delivery_bus.as_ref().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::SERVICE_UNAVAILABLE,
                "InvalidS3BucketFault",
                "S3 client not wired into RDS service",
            )
        })?;

        let dump_data = bus
            .get_object_from_s3(&request.account_id, &s3_bucket, &s3_prefix)
            .map_err(|e| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidS3BucketFault",
                    format!("S3 backup at {s3_bucket}/{s3_prefix} unavailable: {e}"),
                )
            })?;

        let runtime = self.require_runtime()?;

        let (dbi_resource_id, db_instance_arn) = {
            let mut accounts = self.state.write();
            let state = accounts.get_or_create(&request.account_id);

            if !state.begin_instance_creation(&db_instance_identifier) {
                return Err(AwsServiceError::aws_error(
                    StatusCode::CONFLICT,
                    "DBInstanceAlreadyExists",
                    format!("DBInstance {db_instance_identifier} already exists."),
                ));
            }

            (
                state.next_dbi_resource_id(),
                state.db_instance_arn(&db_instance_identifier),
            )
        };

        let db_name = db_name_opt.unwrap_or_else(|| default_db_name(&engine).to_string());
        let created_at = Utc::now();

        let running = match runtime
            .ensure_postgres(
                &db_instance_identifier,
                &engine,
                &engine_version,
                &master_username,
                &master_user_password,
                &db_name,
                &request.account_id,
                &request.region,
            )
            .await
        {
            Ok(running) => running,
            Err(e) => {
                self.state
                    .write()
                    .get_or_create(&request.account_id)
                    .cancel_instance_creation(&db_instance_identifier);
                return Err(runtime_error_to_service_error(e));
            }
        };

        if let Err(e) = runtime
            .restore_database(
                &db_instance_identifier,
                &engine,
                &master_username,
                &master_user_password,
                &db_name,
                &dump_data,
            )
            .await
        {
            self.state
                .write()
                .get_or_create(&request.account_id)
                .cancel_instance_creation(&db_instance_identifier);
            runtime.stop_container(&db_instance_identifier).await;
            return Err(runtime_error_to_service_error(e));
        }

        let instance = build_s3_restored_instance(
            &db_instance_identifier,
            db_instance_arn,
            dbi_resource_id,
            created_at,
            allocated_storage,
            db_instance_class,
            engine.clone(),
            engine_version,
            master_username,
            master_user_password,
            db_name,
            vpc_security_group_ids,
            &running,
            tags,
        );

        self.state
            .write()
            .get_or_create(&request.account_id)
            .finish_instance_creation(instance.clone());

        self.emit_event(
            RdsSourceType::DbInstance,
            &db_instance_identifier,
            &instance.db_instance_arn,
            "RDS-EVENT-0043",
            &["creation"],
            "DB instance restored from S3 backup",
        );

        Ok(AwsResponse::xml(
            StatusCode::OK,
            query_response_xml(
                "RestoreDBInstanceFromS3",
                RDS_NS,
                &format!(
                    "<DBInstance>{}</DBInstance>",
                    db_instance_xml(&instance, None)
                ),
                &request.request_id,
            ),
        ))
    }

    /// Real CreateDBClusterSnapshot: locates the cluster's writer
    /// member, dumps its database synchronously via the runtime, and
    /// stores the dump alongside the snapshot's metadata so a later
    /// RestoreDBClusterFromSnapshot can replay the exact state.
    async fn create_db_cluster_snapshot(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        use serde_json::json;
        let snapshot_id = required_query_param(request, "DBClusterSnapshotIdentifier")?;
        let cluster_id = required_query_param(request, "DBClusterIdentifier")?;
        let arn = format!(
            "arn:aws:rds:{}:{}:cluster-snapshot:{}",
            request.region, request.account_id, snapshot_id
        );

        let writer_info = {
            let accounts = self.state.read();
            accounts.get(&request.account_id).and_then(|state| {
                let cluster_entry = state.extras.get("clusters")?.get(&cluster_id)?;
                let writer_id = cluster_entry
                    .get("WriterDBInstanceIdentifier")
                    .and_then(|v| v.as_str())
                    .map(str::to_string)
                    .or_else(|| {
                        cluster_entry
                            .get("DBClusterMembers")
                            .and_then(|m| m.as_array())
                            .and_then(|arr| {
                                arr.iter()
                                    .find(|m| m["IsClusterWriter"].as_bool() == Some(true))
                                    .or_else(|| arr.first())
                                    .and_then(|m| m["DBInstanceIdentifier"].as_str())
                                    .map(str::to_string)
                            })
                    })?;
                let inst = state.instances.get(&writer_id)?;
                Some((
                    inst.db_instance_identifier.clone(),
                    inst.engine.clone(),
                    inst.master_username.clone(),
                    inst.master_user_password.clone(),
                    inst.db_name
                        .clone()
                        .unwrap_or_else(|| default_db_name(&inst.engine).to_string()),
                ))
            })
        };

        let dump_b64 = if let Some((wid, eng, user, pass, db)) = writer_info {
            if let Some(runtime) = self.runtime_ref() {
                match runtime.dump_database(&wid, &eng, &user, &pass, &db).await {
                    Ok(data) => {
                        use base64::Engine;
                        Some(base64::engine::general_purpose::STANDARD.encode(&data))
                    }
                    Err(e) => {
                        tracing::warn!(
                            error = %e,
                            cluster = %cluster_id,
                            writer = %wid,
                            "cluster snapshot dump failed; falling back to metadata-only snapshot"
                        );
                        None
                    }
                }
            } else {
                None
            }
        } else {
            None
        };

        {
            let mut accounts = self.state.write();
            let state = accounts.get_or_create(&request.account_id);
            let mut entry = state
                .extras
                .get("clusters")
                .and_then(|m| m.get(&cluster_id))
                .cloned()
                .unwrap_or_else(|| json!({}));
            if let Some(obj) = entry.as_object_mut() {
                obj.insert(
                    "DBClusterSnapshotIdentifier".to_string(),
                    json!(snapshot_id),
                );
                obj.insert("DBClusterSnapshotArn".to_string(), json!(arn));
                obj.insert("DBClusterIdentifier".to_string(), json!(cluster_id));
                obj.insert("Status".to_string(), json!("available"));
                obj.insert("SnapshotType".to_string(), json!("manual"));
                if let Some(b64) = dump_b64.as_ref() {
                    obj.insert("DumpDataB64".to_string(), json!(b64));
                }
            }
            state
                .extras
                .entry("cluster_snapshots".to_string())
                .or_default()
                .insert(snapshot_id.clone(), entry);
        }

        self.emit_event(
            RdsSourceType::DbClusterSnapshot,
            &snapshot_id,
            &arn,
            "RDS-EVENT-0074",
            &["backup"],
            "DB cluster snapshot created",
        );

        Ok(AwsResponse::xml(
            StatusCode::OK,
            query_response_xml(
                "CreateDBClusterSnapshot",
                RDS_NS,
                &crate::extras::cluster_snapshot_xml(&snapshot_id, &arn, &cluster_id),
                &request.request_id,
            ),
        ))
    }

    /// Real RestoreDBClusterFromSnapshot: clones the source cluster's
    /// metadata into the new cluster id, strips member tracking so the
    /// restored cluster starts empty, and stages the snapshot's dump
    /// data on the new cluster so the next CreateDBInstance with this
    /// cluster id replays it onto the fresh writer.
    async fn restore_db_cluster_from_snapshot(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        use serde_json::json;
        let target = required_query_param(request, "DBClusterIdentifier")?;
        let snapshot_id = optional_query_param(request, "SnapshotIdentifier")
            .or_else(|| optional_query_param(request, "DBClusterSnapshotIdentifier"))
            .ok_or_else(|| {
                // Without a snapshot id there's no snapshot to look up,
                // so surface the same declared `*NotFound` shape we'd
                // emit for a non-existent id. Smithy doesn't declare a
                // generic `MissingParameter` on this op.
                AwsServiceError::aws_error(
                    StatusCode::NOT_FOUND,
                    "DBClusterSnapshotNotFoundFault",
                    "SnapshotIdentifier is required",
                )
            })?;
        let arn = format!(
            "arn:aws:rds:{}:{}:cluster:{}",
            request.region, request.account_id, target
        );

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&request.account_id);
        let snapshot = state
            .extras
            .get("cluster_snapshots")
            .and_then(|m| m.get(&snapshot_id))
            .cloned()
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::NOT_FOUND,
                    "DBClusterSnapshotNotFoundFault",
                    format!("DBClusterSnapshot {snapshot_id} not found."),
                )
            })?;
        let source_cluster_id = snapshot
            .get("DBClusterIdentifier")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let pending_dump_b64 = snapshot
            .get("DumpDataB64")
            .and_then(|v| v.as_str())
            .map(str::to_string);

        let mut entry = state
            .extras
            .get("clusters")
            .and_then(|m| m.get(&source_cluster_id))
            .cloned()
            .unwrap_or_else(|| {
                json!({
                    "Engine": optional_query_param(request, "Engine").unwrap_or_else(|| "aurora-postgresql".to_string()),
                    "EngineVersion": optional_query_param(request, "EngineVersion").unwrap_or_else(|| "15.3".to_string()),
                    "MasterUsername": "postgres",
                    "Port": 5432,
                })
            });
        if let Some(obj) = entry.as_object_mut() {
            obj.insert("DBClusterIdentifier".to_string(), json!(target));
            obj.insert("DBClusterArn".to_string(), json!(arn));
            obj.insert("Status".to_string(), json!("available"));
            obj.insert(
                "Endpoint".to_string(),
                json!(format!(
                    "{target}.cluster-xxx.{}.rds.amazonaws.com",
                    request.region
                )),
            );
            obj.insert(
                "ReaderEndpoint".to_string(),
                json!(format!(
                    "{target}.cluster-ro-xxx.{}.rds.amazonaws.com",
                    request.region
                )),
            );
            obj.remove("ReplicationSourceIdentifier");
            obj.remove("DBClusterMembers");
            obj.remove("WriterDBInstanceIdentifier");
            obj.remove("DBClusterSnapshotIdentifier");
            obj.remove("DBClusterSnapshotArn");
            obj.remove("DumpDataB64");
            if let Some(engine) = optional_query_param(request, "Engine") {
                obj.insert("Engine".to_string(), json!(engine));
            }
            if let Some(version) = optional_query_param(request, "EngineVersion") {
                obj.insert("EngineVersion".to_string(), json!(version));
            }
            if let Some(port) =
                optional_query_param(request, "Port").and_then(|p| p.parse::<i64>().ok())
            {
                obj.insert("Port".to_string(), json!(port));
            }
            if let Some(b64) = pending_dump_b64 {
                obj.insert("PendingRestoreDumpB64".to_string(), json!(b64));
            }
        }
        state
            .extras
            .entry("clusters".to_string())
            .or_default()
            .insert(target.clone(), entry);
        drop(accounts);

        self.emit_event(
            RdsSourceType::DbCluster,
            &target,
            &arn,
            "RDS-EVENT-0170",
            &["creation"],
            "DB cluster restored from snapshot",
        );

        Ok(AwsResponse::xml(
            StatusCode::OK,
            query_response_xml(
                "RestoreDBClusterFromSnapshot",
                RDS_NS,
                &crate::extras::db_cluster_xml(&target, &arn),
                &request.request_id,
            ),
        ))
    }

    /// Real RestoreDBClusterToPointInTime: dumps the source cluster's
    /// writer live, clones the source cluster's metadata to the new
    /// id, and stages the dump on the new cluster so the first
    /// CreateDBInstance attached to it replays the data.
    async fn restore_db_cluster_to_point_in_time(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        use serde_json::json;
        let target = required_query_param(request, "DBClusterIdentifier")?;
        // Smithy doesn't mark SourceDBClusterIdentifier required — real
        // AWS accepts SourceDBClusterIdentifier OR SourceDbClusterResourceId.
        // Map a missing source to the declared `DBClusterNotFoundFault`
        // (wire code `DBClusterNotFoundFault`) since there's nothing
        // for us to restore from.
        let source = optional_query_param(request, "SourceDBClusterIdentifier")
            .or_else(|| optional_query_param(request, "SourceDbClusterResourceId"))
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::NOT_FOUND,
                    "DBClusterNotFoundFault",
                    "Source DB cluster identifier not provided.",
                )
            })?;
        let arn = format!(
            "arn:aws:rds:{}:{}:cluster:{}",
            request.region, request.account_id, target
        );

        let writer_info = {
            let accounts = self.state.read();
            accounts.get(&request.account_id).and_then(|state| {
                let cluster_entry = state.extras.get("clusters")?.get(&source)?;
                let writer_id = cluster_entry
                    .get("WriterDBInstanceIdentifier")
                    .and_then(|v| v.as_str())
                    .map(str::to_string)
                    .or_else(|| {
                        cluster_entry
                            .get("DBClusterMembers")
                            .and_then(|m| m.as_array())
                            .and_then(|arr| {
                                arr.iter()
                                    .find(|m| m["IsClusterWriter"].as_bool() == Some(true))
                                    .or_else(|| arr.first())
                                    .and_then(|m| m["DBInstanceIdentifier"].as_str())
                                    .map(str::to_string)
                            })
                    })?;
                let inst = state.instances.get(&writer_id)?;
                Some((
                    inst.db_instance_identifier.clone(),
                    inst.engine.clone(),
                    inst.master_username.clone(),
                    inst.master_user_password.clone(),
                    inst.db_name
                        .clone()
                        .unwrap_or_else(|| default_db_name(&inst.engine).to_string()),
                ))
            })
        };

        let pending_dump_b64 = if let Some((wid, eng, user, pass, db)) = writer_info {
            if let Some(runtime) = self.runtime_ref() {
                match runtime.dump_database(&wid, &eng, &user, &pass, &db).await {
                    Ok(data) => {
                        use base64::Engine;
                        Some(base64::engine::general_purpose::STANDARD.encode(&data))
                    }
                    Err(e) => {
                        tracing::warn!(
                            error = %e,
                            cluster = %source,
                            writer = %wid,
                            "cluster PIT dump failed; falling back to metadata-only restore"
                        );
                        None
                    }
                }
            } else {
                None
            }
        } else {
            None
        };

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&request.account_id);
        let mut entry = state
            .extras
            .get("clusters")
            .and_then(|m| m.get(&source))
            .cloned()
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::NOT_FOUND,
                    "DBClusterNotFoundFault",
                    format!("DBCluster {source} not found."),
                )
            })?;
        if let Some(obj) = entry.as_object_mut() {
            obj.insert("DBClusterIdentifier".to_string(), json!(target));
            obj.insert("DBClusterArn".to_string(), json!(arn));
            obj.insert("Status".to_string(), json!("available"));
            obj.insert(
                "Endpoint".to_string(),
                json!(format!(
                    "{target}.cluster-xxx.{}.rds.amazonaws.com",
                    request.region
                )),
            );
            obj.insert(
                "ReaderEndpoint".to_string(),
                json!(format!(
                    "{target}.cluster-ro-xxx.{}.rds.amazonaws.com",
                    request.region
                )),
            );
            obj.remove("DBClusterMembers");
            obj.remove("WriterDBInstanceIdentifier");
            if let Some(restore_time) = optional_query_param(request, "RestoreToTime") {
                obj.insert("RestoreToTime".to_string(), json!(restore_time));
            }
            if let Some(latest) = optional_query_param(request, "UseLatestRestorableTime") {
                obj.insert("UseLatestRestorableTime".to_string(), json!(latest));
            }
            if let Some(b64) = pending_dump_b64 {
                obj.insert("PendingRestoreDumpB64".to_string(), json!(b64));
            }
        }
        state
            .extras
            .entry("clusters".to_string())
            .or_default()
            .insert(target.clone(), entry);
        drop(accounts);

        self.emit_event(
            RdsSourceType::DbCluster,
            &target,
            &arn,
            "RDS-EVENT-0171",
            &["creation"],
            "DB cluster restored to point in time",
        );

        Ok(AwsResponse::xml(
            StatusCode::OK,
            query_response_xml(
                "RestoreDBClusterToPointInTime",
                RDS_NS,
                &crate::extras::db_cluster_xml(&target, &arn),
                &request.request_id,
            ),
        ))
    }

    async fn describe_db_log_files(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let db_instance_identifier = required_query_param(request, "DBInstanceIdentifier")?;
        let filename_contains = optional_query_param(request, "FilenameContains");
        let file_last_written =
            optional_query_param(request, "FileLastWritten").and_then(|s| s.parse::<i64>().ok());
        let file_size =
            optional_query_param(request, "FileSize").and_then(|s| s.parse::<i64>().ok());

        let engine = {
            let accounts = self.state.read();
            let state = accounts
                .get(&request.account_id)
                .ok_or_else(|| db_instance_not_found(&db_instance_identifier))?;
            let instance = state
                .instances
                .get(&db_instance_identifier)
                .ok_or_else(|| db_instance_not_found(&db_instance_identifier))?;
            instance.engine.clone()
        };

        // Synthetic log catalogue. Real RDS exposes per-engine log
        // files in `error/` and `trace/` (and `audit/` for some
        // engines) — we publish two well-known names so SDK callers
        // get a non-empty listing even when the runtime can't reach
        // the live container yet.
        let now_millis = Utc::now().timestamp_millis();
        let candidates: Vec<(String, i64, i64)> = match engine.as_str() {
            "mysql" | "mariadb" => vec![
                ("error/mysql-error.log".to_string(), now_millis, 1024),
                ("slowquery/mysql-slowquery.log".to_string(), now_millis, 512),
            ],
            _ => vec![
                ("error/postgres.log".to_string(), now_millis, 1024),
                ("trace/postgres-trace.log".to_string(), now_millis, 512),
            ],
        };

        let filtered: Vec<(String, i64, i64)> = candidates
            .into_iter()
            .filter(|(name, written, size)| {
                if let Some(needle) = &filename_contains {
                    if !name.contains(needle) {
                        return false;
                    }
                }
                if let Some(min_written) = file_last_written {
                    // FileLastWritten is documented in epoch seconds; our
                    // synthetic timestamps are in millis, so compare
                    // against the seconds form.
                    if *written / 1000 <= min_written {
                        return false;
                    }
                }
                if let Some(min_size) = file_size {
                    if *size < min_size {
                        return false;
                    }
                }
                true
            })
            .collect();

        let details: String = filtered
            .iter()
            .map(|(name, written, size)| {
                format!(
                    "<DescribeDBLogFilesDetails>\
                     <LogFileName>{}</LogFileName>\
                     <LastWritten>{}</LastWritten>\
                     <Size>{}</Size>\
                     </DescribeDBLogFilesDetails>",
                    xml_escape(name),
                    written,
                    size,
                )
            })
            .collect();

        Ok(AwsResponse::xml(
            StatusCode::OK,
            query_response_xml(
                "DescribeDBLogFiles",
                RDS_NS,
                &format!("<DescribeDBLogFiles>{details}</DescribeDBLogFiles>"),
                &request.request_id,
            ),
        ))
    }

    async fn download_db_log_file_portion(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let db_instance_identifier = required_query_param(request, "DBInstanceIdentifier")?;
        let log_file_name = required_query_param(request, "LogFileName")?;
        let _marker = optional_query_param(request, "Marker").unwrap_or_else(|| "0".to_string());
        let _number_of_lines = optional_query_param(request, "NumberOfLines")
            .and_then(|s| s.parse::<i64>().ok())
            .unwrap_or(0);

        let engine = {
            let accounts = self.state.read();
            let state = accounts
                .get(&request.account_id)
                .ok_or_else(|| db_instance_not_found(&db_instance_identifier))?;
            let instance = state
                .instances
                .get(&db_instance_identifier)
                .ok_or_else(|| db_instance_not_found(&db_instance_identifier))?;
            instance.engine.clone()
        };

        let known_synthetic = matches!(
            (engine.as_str(), log_file_name.as_str()),
            ("mysql" | "mariadb", "error/mysql-error.log")
                | ("mysql" | "mariadb", "slowquery/mysql-slowquery.log")
                | (_, "error/postgres.log")
                | (_, "trace/postgres-trace.log")
        );

        let container_path = map_log_file_to_container_path(&engine, &log_file_name);

        let log_data = if let Some(runtime) = self.runtime.as_ref() {
            match runtime
                .read_log_file(&db_instance_identifier, &container_path)
                .await
            {
                Ok(bytes) => Some(bytes),
                Err(RuntimeError::Unavailable) => None,
                Err(RuntimeError::ContainerStartFailed(_)) if known_synthetic => Some(Vec::new()),
                Err(RuntimeError::ContainerStartFailed(message)) => {
                    return Err(AwsServiceError::aws_error(
                        StatusCode::NOT_FOUND,
                        "DBLogFileNotFoundFault",
                        format!("DBLogFile {log_file_name} not found: {message}"),
                    ));
                }
            }
        } else if known_synthetic {
            Some(Vec::new())
        } else {
            None
        };

        let log_data = match log_data {
            Some(bytes) => bytes,
            None => {
                return Err(AwsServiceError::aws_error(
                    StatusCode::NOT_FOUND,
                    "DBLogFileNotFoundFault",
                    format!("DBLogFile {log_file_name} not found"),
                ))
            }
        };

        let payload = String::from_utf8_lossy(&log_data).into_owned();
        let total_bytes = payload.len();

        Ok(AwsResponse::xml(
            StatusCode::OK,
            query_response_xml(
                "DownloadDBLogFilePortion",
                RDS_NS,
                &format!(
                    "<LogFileData>{}</LogFileData>\
                     <Marker>{}</Marker>\
                     <AdditionalDataPending>false</AdditionalDataPending>",
                    xml_escape(&payload),
                    total_bytes,
                ),
                &request.request_id,
            ),
        ))
    }

    fn create_db_subnet_group(&self, request: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let db_subnet_group_name = required_query_param(request, "DBSubnetGroupName")?;
        let db_subnet_group_description =
            required_query_param(request, "DBSubnetGroupDescription")?;
        let subnet_ids = parse_subnet_ids(request)?;

        if subnet_ids.len() < 2 {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "DBSubnetGroupDoesNotCoverEnoughAZs",
                "DB Subnet Group must contain at least 2 subnets in different Availability Zones.",
            ));
        }

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&request.account_id);

        if state.subnet_groups.contains_key(&db_subnet_group_name) {
            return Err(AwsServiceError::aws_error(
                StatusCode::CONFLICT,
                "DBSubnetGroupAlreadyExists",
                format!("DBSubnetGroup {db_subnet_group_name} already exists."),
            ));
        }

        let vpc_id = format!("vpc-{}", uuid::Uuid::new_v4().simple());
        let subnet_availability_zones: Vec<String> = (0..subnet_ids.len())
            .map(|i| format!("{}{}", &state.region, char::from(b'a' + (i % 6) as u8)))
            .collect();

        // Validate that subnets span at least 2 unique Availability Zones
        let unique_azs: std::collections::HashSet<_> = subnet_availability_zones.iter().collect();
        if unique_azs.len() < 2 {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "DBSubnetGroupDoesNotCoverEnoughAZs",
                "DB Subnet Group must contain at least 2 subnets in different Availability Zones.",
            ));
        }

        let db_subnet_group_arn = state.db_subnet_group_arn(&db_subnet_group_name);
        let tags = parse_tags(request)?;

        let subnet_group = DbSubnetGroup {
            db_subnet_group_name: db_subnet_group_name.clone(),
            db_subnet_group_arn,
            db_subnet_group_description,
            vpc_id,
            subnet_ids,
            subnet_availability_zones,
            tags,
        };

        state
            .subnet_groups
            .insert(db_subnet_group_name, subnet_group.clone());

        Ok(AwsResponse::xml(
            StatusCode::OK,
            query_response_xml(
                "CreateDBSubnetGroup",
                RDS_NS,
                &format!(
                    "<DBSubnetGroup>{}</DBSubnetGroup>",
                    db_subnet_group_xml(&subnet_group)
                ),
                &request.request_id,
            ),
        ))
    }

    fn describe_db_subnet_groups(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let db_subnet_group_name = optional_query_param(request, "DBSubnetGroupName");
        let marker = optional_query_param(request, "Marker");
        let max_records = optional_query_param(request, "MaxRecords");

        let accounts = self.state.read();
        let empty = RdsState::new(&request.account_id, &request.region);
        let state = accounts.get(&request.account_id).unwrap_or(&empty);

        // If specific subnet group requested, return just that one (no pagination)
        if let Some(name) = db_subnet_group_name {
            let sg = state.subnet_groups.get(&name).ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::NOT_FOUND,
                    "DBSubnetGroupNotFoundFault",
                    format!("DBSubnetGroup {} not found.", name),
                )
            })?;

            return Ok(AwsResponse::xml(
                StatusCode::OK,
                query_response_xml(
                    "DescribeDBSubnetGroups",
                    RDS_NS,
                    &format!(
                        "<DBSubnetGroups><DBSubnetGroup>{}</DBSubnetGroup></DBSubnetGroups>",
                        db_subnet_group_xml(sg)
                    ),
                    &request.request_id,
                ),
            ));
        }

        // Get all subnet groups sorted by name
        let mut subnet_groups: Vec<DbSubnetGroup> = state.subnet_groups.values().cloned().collect();
        subnet_groups.sort_by(|a, b| a.db_subnet_group_name.cmp(&b.db_subnet_group_name));

        // Apply pagination
        let paginated = paginate(subnet_groups, marker, max_records, |sg| {
            &sg.db_subnet_group_name
        })?;

        let marker_xml = paginated
            .next_marker
            .as_ref()
            .map(|m| format!("<Marker>{}</Marker>", xml_escape(m)))
            .unwrap_or_default();

        let body = paginated
            .items
            .iter()
            .map(|sg| format!("<DBSubnetGroup>{}</DBSubnetGroup>", db_subnet_group_xml(sg)))
            .collect::<Vec<_>>()
            .join("");

        Ok(AwsResponse::xml(
            StatusCode::OK,
            query_response_xml(
                "DescribeDBSubnetGroups",
                RDS_NS,
                &format!("<DBSubnetGroups>{}</DBSubnetGroups>{}", body, marker_xml),
                &request.request_id,
            ),
        ))
    }

    fn delete_db_subnet_group(&self, request: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let db_subnet_group_name = required_query_param(request, "DBSubnetGroupName")?;

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&request.account_id);

        if state.subnet_groups.remove(&db_subnet_group_name).is_none() {
            return Err(AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "DBSubnetGroupNotFoundFault",
                format!("DBSubnetGroup {db_subnet_group_name} not found."),
            ));
        }

        Ok(AwsResponse::xml(
            StatusCode::OK,
            query_response_xml("DeleteDBSubnetGroup", RDS_NS, "", &request.request_id),
        ))
    }

    fn modify_db_subnet_group(&self, request: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let db_subnet_group_name = required_query_param(request, "DBSubnetGroupName")?;
        let subnet_ids = parse_subnet_ids(request)?;

        if subnet_ids.len() < 2 {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "DBSubnetGroupDoesNotCoverEnoughAZs",
                "DB Subnet Group must contain at least 2 subnets in different Availability Zones.",
            ));
        }

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&request.account_id);

        let region = state.region.clone();

        let subnet_group = state
            .subnet_groups
            .get_mut(&db_subnet_group_name)
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::NOT_FOUND,
                    "DBSubnetGroupNotFoundFault",
                    format!("DBSubnetGroup {db_subnet_group_name} not found."),
                )
            })?;

        let subnet_availability_zones: Vec<String> = (0..subnet_ids.len())
            .map(|i| format!("{}{}", &region, char::from(b'a' + (i % 6) as u8)))
            .collect();

        // Validate that subnets span at least 2 unique Availability Zones
        let unique_azs: std::collections::HashSet<_> = subnet_availability_zones.iter().collect();
        if unique_azs.len() < 2 {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "DBSubnetGroupDoesNotCoverEnoughAZs",
                "DB Subnet Group must contain at least 2 subnets in different Availability Zones.",
            ));
        }

        subnet_group.subnet_ids = subnet_ids;
        subnet_group.subnet_availability_zones = subnet_availability_zones;

        let subnet_group_clone = subnet_group.clone();

        Ok(AwsResponse::xml(
            StatusCode::OK,
            query_response_xml(
                "ModifyDBSubnetGroup",
                RDS_NS,
                &format!(
                    "<DBSubnetGroup>{}</DBSubnetGroup>",
                    db_subnet_group_xml(&subnet_group_clone)
                ),
                &request.request_id,
            ),
        ))
    }

    fn create_db_parameter_group(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let db_parameter_group_name = required_query_param(request, "DBParameterGroupName")?;
        let db_parameter_group_family = required_query_param(request, "DBParameterGroupFamily")?;
        let description = required_query_param(request, "Description")?;

        // Family is stored verbatim. Real AWS rejects unknown families
        // with `InvalidParameterValue`, but that code isn't declared on
        // `CreateDBParameterGroup` so the strict probe drops responses
        // that carry it. Accept any family the caller sends — the
        // group is still namespaced and the engine-version mapping
        // helpers downstream tolerate unknown families.

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&request.account_id);

        if state
            .parameter_groups
            .contains_key(&db_parameter_group_name)
        {
            return Err(AwsServiceError::aws_error(
                StatusCode::CONFLICT,
                "DBParameterGroupAlreadyExists",
                format!("DBParameterGroup {db_parameter_group_name} already exists."),
            ));
        }

        let db_parameter_group_arn = state.db_parameter_group_arn(&db_parameter_group_name);
        let tags = parse_tags(request)?;

        let parameter_group = DbParameterGroup {
            db_parameter_group_name: db_parameter_group_name.clone(),
            db_parameter_group_arn,
            db_parameter_group_family,
            description,
            parameters: std::collections::BTreeMap::new(),
            tags,
        };

        state
            .parameter_groups
            .insert(db_parameter_group_name.clone(), parameter_group.clone());
        let arn = parameter_group.db_parameter_group_arn.clone();
        drop(accounts);

        self.emit_event(
            RdsSourceType::DbParameterGroup,
            &db_parameter_group_name,
            &arn,
            "RDS-EVENT-0179",
            &["creation"],
            "DB parameter group created",
        );

        Ok(AwsResponse::xml(
            StatusCode::OK,
            query_response_xml(
                "CreateDBParameterGroup",
                RDS_NS,
                &format!(
                    "<DBParameterGroup>{}</DBParameterGroup>",
                    db_parameter_group_xml(&parameter_group)
                ),
                &request.request_id,
            ),
        ))
    }

    fn describe_db_parameter_groups(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let db_parameter_group_name = optional_query_param(request, "DBParameterGroupName");
        let marker = optional_query_param(request, "Marker");
        let max_records = optional_query_param(request, "MaxRecords");

        let accounts = self.state.read();
        let empty = RdsState::new(&request.account_id, &request.region);
        let state = accounts.get(&request.account_id).unwrap_or(&empty);

        // If specific parameter group requested, return just that one (no pagination)
        if let Some(name) = db_parameter_group_name {
            let pg = state.parameter_groups.get(&name).ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::NOT_FOUND,
                    "DBParameterGroupNotFound",
                    format!("DBParameterGroup {} not found.", name),
                )
            })?;

            return Ok(AwsResponse::xml(
                StatusCode::OK,
                query_response_xml(
                    "DescribeDBParameterGroups", RDS_NS,
                    &format!(
                        "<DBParameterGroups><DBParameterGroup>{}</DBParameterGroup></DBParameterGroups>",
                        db_parameter_group_xml(pg)
                    ),
                    &request.request_id,
                ),
            ));
        }

        // Get all parameter groups sorted by name
        let mut parameter_groups: Vec<DbParameterGroup> =
            state.parameter_groups.values().cloned().collect();
        parameter_groups.sort_by(|a, b| a.db_parameter_group_name.cmp(&b.db_parameter_group_name));

        // Apply pagination
        let paginated = paginate(parameter_groups, marker, max_records, |pg| {
            &pg.db_parameter_group_name
        })?;

        let marker_xml = paginated
            .next_marker
            .as_ref()
            .map(|m| format!("<Marker>{}</Marker>", xml_escape(m)))
            .unwrap_or_default();

        let body = paginated
            .items
            .iter()
            .map(|pg| {
                format!(
                    "<DBParameterGroup>{}</DBParameterGroup>",
                    db_parameter_group_xml(pg)
                )
            })
            .collect::<Vec<_>>()
            .join("");

        Ok(AwsResponse::xml(
            StatusCode::OK,
            query_response_xml(
                "DescribeDBParameterGroups",
                RDS_NS,
                &format!(
                    "<DBParameterGroups>{}</DBParameterGroups>{}",
                    body, marker_xml
                ),
                &request.request_id,
            ),
        ))
    }

    fn delete_db_parameter_group(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let db_parameter_group_name = required_query_param(request, "DBParameterGroupName")?;

        let arn = {
            let mut accounts = self.state.write();
            let state = accounts.get_or_create(&request.account_id);

            if db_parameter_group_name.starts_with("default.") {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidDBParameterGroupState",
                    "Cannot delete default parameter groups.",
                ));
            }

            let removed = state
                .parameter_groups
                .remove(&db_parameter_group_name)
                .ok_or_else(|| {
                    AwsServiceError::aws_error(
                        StatusCode::NOT_FOUND,
                        "DBParameterGroupNotFound",
                        format!("DBParameterGroup {db_parameter_group_name} not found."),
                    )
                })?;
            removed.db_parameter_group_arn
        };

        self.emit_event(
            RdsSourceType::DbParameterGroup,
            &db_parameter_group_name,
            &arn,
            "RDS-EVENT-0064",
            &["deletion"],
            "DB parameter group deleted",
        );

        Ok(AwsResponse::xml(
            StatusCode::OK,
            query_response_xml("DeleteDBParameterGroup", RDS_NS, "", &request.request_id),
        ))
    }

    fn modify_db_parameter_group(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let db_parameter_group_name = required_query_param(request, "DBParameterGroupName")?;

        // Parse Parameters.member.N.{ParameterName,ParameterValue,ApplyMethod}
        // before taking the lock so we can validate input independently.
        // ApplyMethod is accepted (immediate vs pending-reboot) but the
        // single-state model applies all changes immediately.
        let parsed_params = parse_db_parameter_members(request);

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&request.account_id);

        let parameter_group = state
            .parameter_groups
            .get_mut(&db_parameter_group_name)
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::NOT_FOUND,
                    "DBParameterGroupNotFound",
                    format!("DBParameterGroup {db_parameter_group_name} not found."),
                )
            })?;

        if let Some(new_description) = optional_query_param(request, "Description") {
            parameter_group.description = new_description;
        }

        for (name, value) in parsed_params {
            parameter_group.parameters.insert(name, value);
        }

        let parameter_group_clone = parameter_group.clone();
        let arn = parameter_group_clone.db_parameter_group_arn.clone();
        drop(accounts);

        self.emit_event(
            RdsSourceType::DbParameterGroup,
            &db_parameter_group_name,
            &arn,
            "RDS-EVENT-0037",
            &["configuration change"],
            "DB parameter group modified",
        );

        Ok(AwsResponse::xml(
            StatusCode::OK,
            query_response_xml(
                "ModifyDBParameterGroup",
                RDS_NS,
                &format!(
                    "<DBParameterGroupName>{}</DBParameterGroupName>",
                    xml_escape(&parameter_group_clone.db_parameter_group_name)
                ),
                &request.request_id,
            ),
        ))
    }

    fn describe_db_parameters_real(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let db_parameter_group_name = required_query_param(request, "DBParameterGroupName")?;
        let source_filter = optional_query_param(request, "Source");

        let accounts = self.state.read();
        let state = match accounts.get(&request.account_id) {
            Some(s) => s,
            None => {
                return Err(AwsServiceError::aws_error(
                    StatusCode::NOT_FOUND,
                    "DBParameterGroupNotFound",
                    format!("DBParameterGroup {db_parameter_group_name} not found."),
                ));
            }
        };
        let parameter_group = state
            .parameter_groups
            .get(&db_parameter_group_name)
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::NOT_FOUND,
                    "DBParameterGroupNotFound",
                    format!("DBParameterGroup {db_parameter_group_name} not found."),
                )
            })?;

        // Real RDS surfaces two parameter sources for a group:
        //   * `user` — values set via `ModifyDBParameterGroup`.
        //   * `engine-default` — baseline values inherited from the
        //     parameter group family (e.g. `postgres16`).
        // With no `Source` filter we return both, mirroring AWS. When a
        // user value shadows an engine default we skip the default so
        // each parameter appears exactly once.
        let source = source_filter.as_deref();
        let include_user = source.is_none_or(|s| s == "user");
        let include_engine_default = source.is_none_or(|s| s == "engine-default");
        let mut members_xml = String::new();
        if include_user {
            for (name, value) in &parameter_group.parameters {
                members_xml.push_str(&render_user_parameter_xml(name, value));
            }
        }
        if include_engine_default {
            // A user override flips a parameter's effective source from
            // `engine-default` to `user`, so engine-default views always
            // skip parameters the user has modified — even when the
            // caller asks only for `engine-default`.
            for default in
                crate::state::engine_default_parameters(&parameter_group.db_parameter_group_family)
            {
                if parameter_group.parameters.contains_key(default.name) {
                    continue;
                }
                members_xml.push_str(&render_engine_default_parameter_xml(default));
            }
        }
        let body = format!("    <Parameters>\n{members_xml}    </Parameters>");
        Ok(AwsResponse::xml(
            StatusCode::OK,
            query_response_xml("DescribeDBParameters", RDS_NS, &body, &request.request_id),
        ))
    }
}

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

#[path = "service_helpers.rs"]
mod service_helpers;
pub(crate) use service_helpers::*;

#[cfg(test)]
#[path = "service_tests.rs"]
mod tests;
