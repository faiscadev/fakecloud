use std::sync::Arc;

use async_trait::async_trait;
use chrono::Utc;
use http::StatusCode;

use fakecloud_aws::xml::xml_escape;
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsService, AwsServiceError};

use crate::runtime::{RdsRuntime, RuntimeError};
use crate::state::{
    default_engine_versions, default_orderable_options, DbInstance, DbSnapshot, EngineVersionInfo,
    OrderableDbInstanceOption, RdsTag, SharedRdsState,
};

const RDS_NS: &str = "http://rds.amazonaws.com/doc/2014-10-31/";
const SUPPORTED_ACTIONS: &[&str] = &[
    "AddTagsToResource",
    "CreateDBInstance",
    "CreateDBInstanceReadReplica",
    "CreateDBSnapshot",
    "DeleteDBInstance",
    "DeleteDBSnapshot",
    "DescribeDBEngineVersions",
    "DescribeDBInstances",
    "DescribeDBSnapshots",
    "DescribeOrderableDBInstanceOptions",
    "ListTagsForResource",
    "ModifyDBInstance",
    "RebootDBInstance",
    "RemoveTagsFromResource",
    "RestoreDBInstanceFromDBSnapshot",
];

pub struct RdsService {
    state: SharedRdsState,
    runtime: Option<Arc<RdsRuntime>>,
}

impl RdsService {
    pub fn new(state: SharedRdsState) -> Self {
        Self {
            state,
            runtime: None,
        }
    }

    pub fn with_runtime(mut self, runtime: Arc<RdsRuntime>) -> Self {
        self.runtime = Some(runtime);
        self
    }
}

#[async_trait]
impl AwsService for RdsService {
    fn service_name(&self) -> &str {
        "rds"
    }

    async fn handle(&self, request: AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        match request.action.as_str() {
            "AddTagsToResource" => self.add_tags_to_resource(&request),
            "CreateDBInstance" => self.create_db_instance(&request).await,
            "CreateDBInstanceReadReplica" => self.create_db_instance_read_replica(&request).await,
            "CreateDBSnapshot" => self.create_db_snapshot(&request).await,
            "DeleteDBInstance" => self.delete_db_instance(&request).await,
            "DeleteDBSnapshot" => self.delete_db_snapshot(&request),
            "DescribeDBEngineVersions" => self.describe_db_engine_versions(&request),
            "DescribeDBInstances" => self.describe_db_instances(&request),
            "DescribeDBSnapshots" => self.describe_db_snapshots(&request),
            "DescribeOrderableDBInstanceOptions" => {
                self.describe_orderable_db_instance_options(&request)
            }
            "ListTagsForResource" => self.list_tags_for_resource(&request),
            "ModifyDBInstance" => self.modify_db_instance(&request),
            "RebootDBInstance" => self.reboot_db_instance(&request).await,
            "RemoveTagsFromResource" => self.remove_tags_from_resource(&request),
            "RestoreDBInstanceFromDBSnapshot" => {
                self.restore_db_instance_from_db_snapshot(&request).await
            }
            _ => Err(AwsServiceError::action_not_implemented(
                self.service_name(),
                &request.action,
            )),
        }
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
        let db_instance_identifier = required_param(request, "DBInstanceIdentifier")?;
        let allocated_storage = required_i32_param(request, "AllocatedStorage")?;
        let db_instance_class = required_param(request, "DBInstanceClass")?;
        let engine = required_param(request, "Engine")?;
        let master_username = required_param(request, "MasterUsername")?;
        let master_user_password = required_param(request, "MasterUserPassword")?;
        let db_name = optional_param(request, "DBName");
        let engine_version =
            optional_param(request, "EngineVersion").unwrap_or_else(|| "16.3".to_string());
        let publicly_accessible =
            parse_optional_bool(optional_param(request, "PubliclyAccessible").as_deref())?
                .unwrap_or(true);
        let deletion_protection =
            parse_optional_bool(optional_param(request, "DeletionProtection").as_deref())?
                .unwrap_or(false);
        let port = optional_i32_param(request, "Port")?.unwrap_or(5432);

        validate_create_request(
            &db_instance_identifier,
            allocated_storage,
            &db_instance_class,
            &engine,
            &engine_version,
            port,
        )?;

        {
            let mut state = self.state.write();
            if !state.begin_instance_creation(&db_instance_identifier) {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "DBInstanceAlreadyExists",
                    format!("DBInstance {} already exists.", db_instance_identifier),
                ));
            }
        }

        let runtime = self.runtime.as_ref().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::SERVICE_UNAVAILABLE,
                "InvalidParameterValue",
                "Docker/Podman is required for RDS DB instances but is not available",
            )
        })?;

        let logical_db_name = db_name.clone().unwrap_or_else(|| "postgres".to_string());
        let running = runtime
            .ensure_postgres(
                &db_instance_identifier,
                &master_username,
                &master_user_password,
                &logical_db_name,
            )
            .await
            .map_err(|error| {
                self.state
                    .write()
                    .cancel_instance_creation(&db_instance_identifier);
                runtime_error_to_service_error(error)
            })?;

        let mut state = self.state.write();
        let instance = DbInstance {
            db_instance_identifier: db_instance_identifier.clone(),
            db_instance_arn: state.db_instance_arn(&db_instance_identifier),
            db_instance_class: db_instance_class.clone(),
            engine: engine.clone(),
            engine_version: engine_version.clone(),
            db_instance_status: "available".to_string(),
            master_username: master_username.clone(),
            db_name: db_name.clone(),
            endpoint_address: "127.0.0.1".to_string(),
            port: i32::from(running.host_port),
            allocated_storage,
            publicly_accessible,
            deletion_protection,
            created_at: Utc::now(),
            dbi_resource_id: state.next_dbi_resource_id(),
            master_user_password,
            container_id: running.container_id,
            host_port: running.host_port,
            tags: Vec::new(),
            read_replica_source_db_instance_identifier: None,
            read_replica_db_instance_identifiers: Vec::new(),
        };
        state.finish_instance_creation(instance.clone());

        Ok(AwsResponse::xml(
            StatusCode::OK,
            xml_wrap(
                "CreateDBInstance",
                &format!(
                    "<DBInstance>{}</DBInstance>",
                    db_instance_xml(&instance, Some("creating"))
                ),
                &request.request_id,
            ),
        ))
    }

    async fn delete_db_instance(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let db_instance_identifier = required_param(request, "DBInstanceIdentifier")?;
        let skip_final_snapshot =
            parse_optional_bool(optional_param(request, "SkipFinalSnapshot").as_deref())?
                .unwrap_or(false);
        let final_db_snapshot_identifier = optional_param(request, "FinalDBSnapshotIdentifier");

        if skip_final_snapshot && final_db_snapshot_identifier.is_some() {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterCombination",
                "FinalDBSnapshotIdentifier cannot be specified when SkipFinalSnapshot is enabled.",
            ));
        }
        if !skip_final_snapshot {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterCombination",
                "SkipFinalSnapshot must be enabled until final snapshot support is implemented.",
            ));
        }

        let instance = {
            let mut state = self.state.write();
            let instance = state
                .instances
                .remove(&db_instance_identifier)
                .ok_or_else(|| db_instance_not_found(&db_instance_identifier))?;

            if instance.deletion_protection {
                state
                    .instances
                    .insert(db_instance_identifier.clone(), instance.clone());
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidDBInstanceState",
                    format!(
                        "DBInstance {} cannot be deleted because deletion protection is enabled.",
                        db_instance_identifier
                    ),
                ));
            }

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

        Ok(AwsResponse::xml(
            StatusCode::OK,
            xml_wrap(
                "DeleteDBInstance",
                &format!(
                    "<DBInstance>{}</DBInstance>",
                    db_instance_xml(&instance, Some("deleting"))
                ),
                &request.request_id,
            ),
        ))
    }

    fn modify_db_instance(&self, request: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let db_instance_identifier = required_param(request, "DBInstanceIdentifier")?;
        let db_instance_class = optional_param(request, "DBInstanceClass");
        let deletion_protection =
            parse_optional_bool(optional_param(request, "DeletionProtection").as_deref())?;
        let apply_immediately =
            parse_optional_bool(optional_param(request, "ApplyImmediately").as_deref())?;

        if apply_immediately == Some(false) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterValue",
                "ApplyImmediately=false is not yet supported for ModifyDBInstance.",
            ));
        }
        if db_instance_class.is_none() && deletion_protection.is_none() {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterCombination",
                "At least one supported mutable field must be provided.",
            ));
        }
        if let Some(ref class) = db_instance_class {
            validate_db_instance_class(class)?;
        }

        let mut state = self.state.write();
        let instance = state
            .instances
            .get_mut(&db_instance_identifier)
            .ok_or_else(|| db_instance_not_found(&db_instance_identifier))?;

        if let Some(class) = db_instance_class {
            instance.db_instance_class = class;
        }
        if let Some(deletion_protection) = deletion_protection {
            instance.deletion_protection = deletion_protection;
        }

        Ok(AwsResponse::xml(
            StatusCode::OK,
            xml_wrap(
                "ModifyDBInstance",
                &format!(
                    "<DBInstance>{}</DBInstance>",
                    db_instance_xml(instance, Some("modifying"))
                ),
                &request.request_id,
            ),
        ))
    }

    async fn reboot_db_instance(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let db_instance_identifier = required_param(request, "DBInstanceIdentifier")?;
        let force_failover =
            parse_optional_bool(optional_param(request, "ForceFailover").as_deref())?;
        if force_failover == Some(true) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterCombination",
                "ForceFailover is not supported for single-instance PostgreSQL DB instances.",
            ));
        }

        let instance = {
            let state = self.state.read();
            state
                .instances
                .get(&db_instance_identifier)
                .cloned()
                .ok_or_else(|| db_instance_not_found(&db_instance_identifier))?
        };

        let runtime = self.runtime.as_ref().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::SERVICE_UNAVAILABLE,
                "InvalidParameterValue",
                "Docker/Podman is required for RDS DB instances but is not available",
            )
        })?;

        let running = runtime
            .restart_container(
                &db_instance_identifier,
                &instance.master_username,
                &instance.master_user_password,
                instance.db_name.as_deref().unwrap_or("postgres"),
            )
            .await
            .map_err(runtime_error_to_service_error)?;

        let instance = {
            let mut state = self.state.write();
            let instance = state
                .instances
                .get_mut(&db_instance_identifier)
                .ok_or_else(|| db_instance_not_found(&db_instance_identifier))?;
            instance.host_port = running.host_port;
            instance.port = i32::from(running.host_port);
            instance.clone()
        };

        Ok(AwsResponse::xml(
            StatusCode::OK,
            xml_wrap(
                "RebootDBInstance",
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
        let engine = optional_param(request, "Engine");
        let engine_version = optional_param(request, "EngineVersion");
        let family = optional_param(request, "DBParameterGroupFamily");
        let default_only = parse_optional_bool(optional_param(request, "DefaultOnly").as_deref())?;

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
            xml_wrap(
                "DescribeDBEngineVersions",
                &format!(
                    "<DBEngineVersions>{}</DBEngineVersions>",
                    versions.iter().map(engine_version_xml).collect::<String>()
                ),
                &request.request_id,
            ),
        ))
    }

    fn describe_db_instances(&self, request: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let db_instance_identifier = optional_param(request, "DBInstanceIdentifier");
        let marker = optional_param(request, "Marker");
        let max_records = optional_param(request, "MaxRecords");

        if marker.is_some() || max_records.is_some() {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterValue",
                "Marker and MaxRecords are not yet supported for DescribeDBInstances.",
            ));
        }

        let state = self.state.read();
        let instances: Vec<DbInstance> = match db_instance_identifier {
            Some(identifier) => vec![state
                .instances
                .get(&identifier)
                .cloned()
                .ok_or_else(|| db_instance_not_found(&identifier))?],
            None => {
                let mut values: Vec<DbInstance> = state.instances.values().cloned().collect();
                values.sort_by(|a, b| a.db_instance_identifier.cmp(&b.db_instance_identifier));
                values
            }
        };

        Ok(AwsResponse::xml(
            StatusCode::OK,
            xml_wrap(
                "DescribeDBInstances",
                &format!(
                    "<DBInstances>{}</DBInstances>",
                    instances
                        .iter()
                        .map(|instance| {
                            format!(
                                "<DBInstance>{}</DBInstance>",
                                db_instance_xml(instance, None)
                            )
                        })
                        .collect::<String>()
                ),
                &request.request_id,
            ),
        ))
    }

    fn describe_orderable_db_instance_options(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let engine = optional_param(request, "Engine");
        let engine_version = optional_param(request, "EngineVersion");
        let db_instance_class = optional_param(request, "DBInstanceClass");
        let license_model = optional_param(request, "LicenseModel");
        let vpc = parse_optional_bool(optional_param(request, "Vpc").as_deref())?;

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
            xml_wrap(
                "DescribeOrderableDBInstanceOptions",
                &format!(
                    "<OrderableDBInstanceOptions>{}</OrderableDBInstanceOptions>",
                    options.iter().map(orderable_option_xml).collect::<String>()
                ),
                &request.request_id,
            ),
        ))
    }

    fn add_tags_to_resource(&self, request: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let resource_name = required_param(request, "ResourceName")?;
        let tags = parse_tags(request)?;

        if tags.is_empty() {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "MissingParameter",
                "The request must contain the parameter Tags.",
            ));
        }

        let mut state = self.state.write();
        let instance = find_instance_by_arn_mut(&mut state, &resource_name)?;
        merge_tags(&mut instance.tags, &tags);

        Ok(AwsResponse::xml(
            StatusCode::OK,
            xml_wrap("AddTagsToResource", "", &request.request_id),
        ))
    }

    fn list_tags_for_resource(&self, request: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let resource_name = required_param(request, "ResourceName")?;
        if query_param_prefix_exists(request, "Filters.") {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterValue",
                "Filters are not yet supported for ListTagsForResource.",
            ));
        }

        let state = self.state.read();
        let instance = find_instance_by_arn(&state, &resource_name)?;
        let tag_xml = instance.tags.iter().map(tag_xml).collect::<String>();

        Ok(AwsResponse::xml(
            StatusCode::OK,
            xml_wrap(
                "ListTagsForResource",
                &format!("<TagList>{tag_xml}</TagList>"),
                &request.request_id,
            ),
        ))
    }

    fn remove_tags_from_resource(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let resource_name = required_param(request, "ResourceName")?;
        let tag_keys = parse_tag_keys(request)?;

        if tag_keys.is_empty() {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "MissingParameter",
                "The request must contain the parameter TagKeys.",
            ));
        }

        let mut state = self.state.write();
        let instance = find_instance_by_arn_mut(&mut state, &resource_name)?;
        instance
            .tags
            .retain(|tag| !tag_keys.iter().any(|key| key == &tag.key));

        Ok(AwsResponse::xml(
            StatusCode::OK,
            xml_wrap("RemoveTagsFromResource", "", &request.request_id),
        ))
    }

    async fn create_db_snapshot(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let db_snapshot_identifier = required_param(request, "DBSnapshotIdentifier")?;
        let db_instance_identifier = required_param(request, "DBInstanceIdentifier")?;

        let runtime = self.runtime.as_ref().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::SERVICE_UNAVAILABLE,
                "InvalidParameterValue",
                "Docker/Podman is required for RDS snapshots but is not available",
            )
        })?;

        let (instance, db_name) = {
            let state = self.state.write();

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

            let db_name = instance
                .db_name
                .as_deref()
                .unwrap_or("postgres")
                .to_string();

            (instance, db_name)
        };

        let dump_data = runtime
            .dump_database(&db_instance_identifier, &instance.master_username, &db_name)
            .await
            .map_err(runtime_error_to_service_error)?;

        let mut state = self.state.write();

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
        };

        state
            .snapshots
            .insert(db_snapshot_identifier, snapshot.clone());

        Ok(AwsResponse::xml(
            StatusCode::OK,
            xml_wrap(
                "CreateDBSnapshot",
                &format!("<DBSnapshot>{}</DBSnapshot>", db_snapshot_xml(&snapshot)),
                &request.request_id,
            ),
        ))
    }

    fn describe_db_snapshots(&self, request: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let db_snapshot_identifier = optional_param(request, "DBSnapshotIdentifier");
        let db_instance_identifier = optional_param(request, "DBInstanceIdentifier");

        let state = self.state.read();
        let snapshots: Vec<DbSnapshot> = match (db_snapshot_identifier, db_instance_identifier) {
            (Some(snapshot_id), None) => vec![state
                .snapshots
                .get(&snapshot_id)
                .cloned()
                .ok_or_else(|| db_snapshot_not_found(&snapshot_id))?],
            (None, Some(instance_id)) => state
                .snapshots
                .values()
                .filter(|s| s.db_instance_identifier == instance_id)
                .cloned()
                .collect(),
            (None, None) => {
                let mut values: Vec<DbSnapshot> = state.snapshots.values().cloned().collect();
                values.sort_by(|a, b| a.db_snapshot_identifier.cmp(&b.db_snapshot_identifier));
                values
            }
            (Some(_), Some(_)) => {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterCombination",
                    "Cannot specify both DBSnapshotIdentifier and DBInstanceIdentifier.",
                ));
            }
        };

        Ok(AwsResponse::xml(
            StatusCode::OK,
            xml_wrap(
                "DescribeDBSnapshots",
                &format!(
                    "<DBSnapshots>{}</DBSnapshots>",
                    snapshots
                        .iter()
                        .map(|snapshot| format!(
                            "<DBSnapshot>{}</DBSnapshot>",
                            db_snapshot_xml(snapshot)
                        ))
                        .collect::<String>()
                ),
                &request.request_id,
            ),
        ))
    }

    fn delete_db_snapshot(&self, request: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let db_snapshot_identifier = required_param(request, "DBSnapshotIdentifier")?;

        let mut state = self.state.write();

        let snapshot = state
            .snapshots
            .remove(&db_snapshot_identifier)
            .ok_or_else(|| db_snapshot_not_found(&db_snapshot_identifier))?;

        Ok(AwsResponse::xml(
            StatusCode::OK,
            xml_wrap(
                "DeleteDBSnapshot",
                &format!("<DBSnapshot>{}</DBSnapshot>", db_snapshot_xml(&snapshot)),
                &request.request_id,
            ),
        ))
    }

    async fn restore_db_instance_from_db_snapshot(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let db_instance_identifier = required_param(request, "DBInstanceIdentifier")?;
        let db_snapshot_identifier = required_param(request, "DBSnapshotIdentifier")?;

        let runtime = self.runtime.as_ref().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::SERVICE_UNAVAILABLE,
                "InvalidParameterValue",
                "Docker/Podman is required for RDS DB instances but is not available",
            )
        })?;

        let (snapshot, dbi_resource_id, db_instance_arn, created_at) = {
            let mut state = self.state.write();

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

        let db_name = snapshot.db_name.as_deref().unwrap_or("postgres");
        let running = match runtime
            .ensure_postgres(
                &db_instance_identifier,
                &snapshot.master_username,
                &snapshot.master_user_password,
                db_name,
            )
            .await
        {
            Ok(running) => running,
            Err(e) => {
                self.state
                    .write()
                    .cancel_instance_creation(&db_instance_identifier);
                return Err(runtime_error_to_service_error(e));
            }
        };

        if let Err(e) = runtime
            .restore_database(
                &db_instance_identifier,
                &snapshot.master_username,
                db_name,
                &snapshot.dump_data,
            )
            .await
        {
            self.state
                .write()
                .cancel_instance_creation(&db_instance_identifier);
            return Err(runtime_error_to_service_error(e));
        }

        let mut state = self.state.write();

        let instance = DbInstance {
            db_instance_identifier: db_instance_identifier.clone(),
            db_instance_arn,
            db_instance_class: "db.t3.micro".to_string(),
            engine: snapshot.engine.clone(),
            engine_version: snapshot.engine_version.clone(),
            db_instance_status: "available".to_string(),
            master_username: snapshot.master_username.clone(),
            db_name: snapshot.db_name.clone(),
            endpoint_address: "127.0.0.1".to_string(),
            port: i32::from(running.host_port),
            allocated_storage: snapshot.allocated_storage,
            publicly_accessible: true,
            deletion_protection: false,
            created_at,
            dbi_resource_id,
            master_user_password: snapshot.master_user_password.clone(),
            container_id: running.container_id,
            host_port: running.host_port,
            tags: Vec::new(),
            read_replica_source_db_instance_identifier: None,
            read_replica_db_instance_identifiers: Vec::new(),
        };

        state.finish_instance_creation(instance.clone());

        Ok(AwsResponse::xml(
            StatusCode::OK,
            xml_wrap(
                "RestoreDBInstanceFromDBSnapshot",
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
        let db_instance_identifier = required_param(request, "DBInstanceIdentifier")?;
        let source_db_instance_identifier = required_param(request, "SourceDBInstanceIdentifier")?;

        let runtime = self.runtime.as_ref().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::SERVICE_UNAVAILABLE,
                "InvalidParameterValue",
                "Docker/Podman is required for RDS read replicas but is not available",
            )
        })?;

        let (source_instance, db_name) = {
            let mut state = self.state.write();

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

            let db_name = source_instance
                .db_name
                .as_deref()
                .unwrap_or("postgres")
                .to_string();

            (source_instance, db_name)
        };

        let dump_data = match runtime
            .dump_database(
                &source_db_instance_identifier,
                &source_instance.master_username,
                &db_name,
            )
            .await
        {
            Ok(data) => data,
            Err(e) => {
                self.state
                    .write()
                    .cancel_instance_creation(&db_instance_identifier);
                return Err(runtime_error_to_service_error(e));
            }
        };

        let dbi_resource_id = self.state.read().next_dbi_resource_id();
        let db_instance_arn = self.state.read().db_instance_arn(&db_instance_identifier);
        let created_at = Utc::now();

        let running = match runtime
            .ensure_postgres(
                &db_instance_identifier,
                &source_instance.master_username,
                &source_instance.master_user_password,
                &db_name,
            )
            .await
        {
            Ok(running) => running,
            Err(e) => {
                self.state
                    .write()
                    .cancel_instance_creation(&db_instance_identifier);
                return Err(runtime_error_to_service_error(e));
            }
        };

        if let Err(e) = runtime
            .restore_database(
                &db_instance_identifier,
                &source_instance.master_username,
                &db_name,
                &dump_data,
            )
            .await
        {
            self.state
                .write()
                .cancel_instance_creation(&db_instance_identifier);
            return Err(runtime_error_to_service_error(e));
        }

        let replica = DbInstance {
            db_instance_identifier: db_instance_identifier.clone(),
            db_instance_arn,
            db_instance_class: source_instance.db_instance_class.clone(),
            engine: source_instance.engine.clone(),
            engine_version: source_instance.engine_version.clone(),
            db_instance_status: "available".to_string(),
            master_username: source_instance.master_username.clone(),
            db_name: source_instance.db_name.clone(),
            endpoint_address: "127.0.0.1".to_string(),
            port: i32::from(running.host_port),
            allocated_storage: source_instance.allocated_storage,
            publicly_accessible: source_instance.publicly_accessible,
            deletion_protection: false,
            created_at,
            dbi_resource_id,
            master_user_password: source_instance.master_user_password.clone(),
            container_id: running.container_id,
            host_port: running.host_port,
            tags: Vec::new(),
            read_replica_source_db_instance_identifier: Some(source_db_instance_identifier.clone()),
            read_replica_db_instance_identifiers: Vec::new(),
        };

        let source_missing = {
            let mut state = self.state.write();
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
            runtime
                .stop_container(&db_instance_identifier)
                .await;
            return Err(db_instance_not_found(&source_db_instance_identifier));
        }

        Ok(AwsResponse::xml(
            StatusCode::OK,
            xml_wrap(
                "CreateDBInstanceReadReplica",
                &format!(
                    "<DBInstance>{}</DBInstance>",
                    db_instance_xml(&replica, None)
                ),
                &request.request_id,
            ),
        ))
    }
}

fn optional_param(req: &AwsRequest, name: &str) -> Option<String> {
    req.query_params
        .get(name)
        .cloned()
        .filter(|value| !value.is_empty())
}

fn required_param(req: &AwsRequest, name: &str) -> Result<String, AwsServiceError> {
    optional_param(req, name).ok_or_else(|| {
        AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "MissingParameter",
            format!("The request must contain the parameter {name}."),
        )
    })
}

fn required_i32_param(req: &AwsRequest, name: &str) -> Result<i32, AwsServiceError> {
    let value = required_param(req, name)?;
    value.parse::<i32>().map_err(|_| {
        AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "InvalidParameterValue",
            format!("Parameter {name} must be a valid integer."),
        )
    })
}

fn optional_i32_param(req: &AwsRequest, name: &str) -> Result<Option<i32>, AwsServiceError> {
    optional_param(req, name)
        .map(|value| {
            value.parse::<i32>().map_err(|_| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterValue",
                    format!("Parameter {name} must be a valid integer."),
                )
            })
        })
        .transpose()
}

fn parse_tags(req: &AwsRequest) -> Result<Vec<RdsTag>, AwsServiceError> {
    let mut tags = Vec::new();
    for index in 1.. {
        let key_name = format!("Tags.Tag.{index}.Key");
        let value_name = format!("Tags.Tag.{index}.Value");
        let key = optional_param(req, &key_name);
        let value = optional_param(req, &value_name);

        match (key, value) {
            (Some(key), Some(value)) => tags.push(RdsTag { key, value }),
            (None, None) => break,
            _ => {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterValue",
                    "Each tag must include both Key and Value.",
                ));
            }
        }
    }

    Ok(tags)
}

fn parse_tag_keys(req: &AwsRequest) -> Result<Vec<String>, AwsServiceError> {
    let mut keys = Vec::new();
    for index in 1.. {
        let key_name = format!("TagKeys.member.{index}");
        match optional_param(req, &key_name) {
            Some(key) => keys.push(key),
            None => break,
        }
    }

    Ok(keys)
}

fn query_param_prefix_exists(req: &AwsRequest, prefix: &str) -> bool {
    req.query_params.keys().any(|key| key.starts_with(prefix))
}

fn parse_optional_bool(value: Option<&str>) -> Result<Option<bool>, AwsServiceError> {
    value
        .map(|raw| match raw {
            "true" | "True" | "TRUE" => Ok(true),
            "false" | "False" | "FALSE" => Ok(false),
            _ => Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterValue",
                format!("Boolean parameter value '{raw}' is invalid."),
            )),
        })
        .transpose()
}

fn validate_create_request(
    db_instance_identifier: &str,
    allocated_storage: i32,
    db_instance_class: &str,
    engine: &str,
    engine_version: &str,
    port: i32,
) -> Result<(), AwsServiceError> {
    if allocated_storage <= 0 {
        return Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "InvalidParameterValue",
            "AllocatedStorage must be greater than zero.",
        ));
    }
    if port <= 0 {
        return Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "InvalidParameterValue",
            "Port must be greater than zero.",
        ));
    }
    if !db_instance_identifier
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || ch == '-')
    {
        return Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "InvalidParameterValue",
            "DBInstanceIdentifier must contain only alphanumeric characters or hyphens.",
        ));
    }
    if engine != "postgres" {
        return Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "InvalidParameterValue",
            format!("Engine '{engine}' is not supported yet."),
        ));
    }
    if engine_version != "16.3" {
        return Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "InvalidParameterValue",
            format!("EngineVersion '{engine_version}' is not supported yet."),
        ));
    }
    validate_db_instance_class(db_instance_class)?;
    Ok(())
}

fn validate_db_instance_class(db_instance_class: &str) -> Result<(), AwsServiceError> {
    if db_instance_class != "db.t3.micro" {
        return Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "InvalidParameterValue",
            format!("DBInstanceClass '{db_instance_class}' is not supported yet."),
        ));
    }
    Ok(())
}

fn filter_engine_versions(
    versions: &[EngineVersionInfo],
    engine: &Option<String>,
    engine_version: &Option<String>,
    family: &Option<String>,
) -> Vec<EngineVersionInfo> {
    versions
        .iter()
        .filter(|candidate| {
            engine
                .as_ref()
                .is_none_or(|expected| candidate.engine == *expected)
        })
        .filter(|candidate| {
            engine_version
                .as_ref()
                .is_none_or(|expected| candidate.engine_version == *expected)
        })
        .filter(|candidate| {
            family
                .as_ref()
                .is_none_or(|expected| candidate.db_parameter_group_family == *expected)
        })
        .cloned()
        .collect()
}

fn filter_orderable_options(
    options: &[OrderableDbInstanceOption],
    engine: &Option<String>,
    engine_version: &Option<String>,
    db_instance_class: &Option<String>,
    license_model: &Option<String>,
    vpc: Option<bool>,
) -> Vec<OrderableDbInstanceOption> {
    options
        .iter()
        .filter(|candidate| {
            engine
                .as_ref()
                .is_none_or(|expected| candidate.engine == *expected)
        })
        .filter(|candidate| {
            engine_version
                .as_ref()
                .is_none_or(|expected| candidate.engine_version == *expected)
        })
        .filter(|candidate| {
            db_instance_class
                .as_ref()
                .is_none_or(|expected| candidate.db_instance_class == *expected)
        })
        .filter(|candidate| {
            license_model
                .as_ref()
                .is_none_or(|expected| candidate.license_model == *expected)
        })
        .filter(|_| vpc.unwrap_or(true))
        .cloned()
        .collect()
}

fn xml_wrap(action: &str, inner: &str, request_id: &str) -> String {
    format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
         <{action}Response xmlns=\"{RDS_NS}\">\
         <{action}Result>{inner}</{action}Result>\
         <ResponseMetadata><RequestId>{request_id}</RequestId></ResponseMetadata>\
         </{action}Response>"
    )
}

fn engine_version_xml(version: &EngineVersionInfo) -> String {
    format!(
        "<DBEngineVersion>\
         <Engine>{}</Engine>\
         <EngineVersion>{}</EngineVersion>\
         <DBParameterGroupFamily>{}</DBParameterGroupFamily>\
         <DBEngineDescription>{}</DBEngineDescription>\
         <DBEngineVersionDescription>{}</DBEngineVersionDescription>\
         <Status>{}</Status>\
         </DBEngineVersion>",
        xml_escape(&version.engine),
        xml_escape(&version.engine_version),
        xml_escape(&version.db_parameter_group_family),
        xml_escape(&version.db_engine_description),
        xml_escape(&version.db_engine_version_description),
        xml_escape(&version.status),
    )
}

fn orderable_option_xml(option: &OrderableDbInstanceOption) -> String {
    format!(
        "<OrderableDBInstanceOption>\
         <Engine>{}</Engine>\
         <EngineVersion>{}</EngineVersion>\
         <DBInstanceClass>{}</DBInstanceClass>\
         <LicenseModel>{}</LicenseModel>\
         <AvailabilityZones><AvailabilityZone><Name>us-east-1a</Name></AvailabilityZone></AvailabilityZones>\
         <MultiAZCapable>true</MultiAZCapable>\
         <ReadReplicaCapable>true</ReadReplicaCapable>\
         <Vpc>true</Vpc>\
         <SupportsStorageEncryption>true</SupportsStorageEncryption>\
         <StorageType>{}</StorageType>\
         <SupportsIops>false</SupportsIops>\
         <MinStorageSize>{}</MinStorageSize>\
         <MaxStorageSize>{}</MaxStorageSize>\
         <SupportsIAMDatabaseAuthentication>true</SupportsIAMDatabaseAuthentication>\
         </OrderableDBInstanceOption>",
        xml_escape(&option.engine),
        xml_escape(&option.engine_version),
        xml_escape(&option.db_instance_class),
        xml_escape(&option.license_model),
        xml_escape(&option.storage_type),
        option.min_storage_size,
        option.max_storage_size,
    )
}

fn tag_xml(tag: &RdsTag) -> String {
    format!(
        "<Tag><Key>{}</Key><Value>{}</Value></Tag>",
        xml_escape(&tag.key),
        xml_escape(&tag.value),
    )
}

fn db_instance_xml(instance: &DbInstance, status_override: Option<&str>) -> String {
    let status = status_override.unwrap_or(&instance.db_instance_status);
    let db_name_xml = instance
        .db_name
        .as_ref()
        .map(|db_name| format!("<DBName>{}</DBName>", xml_escape(db_name)))
        .unwrap_or_default();

    let read_replica_source_xml = instance
        .read_replica_source_db_instance_identifier
        .as_ref()
        .map(|source| {
            format!(
                "<ReadReplicaSourceDBInstanceIdentifier>{}</ReadReplicaSourceDBInstanceIdentifier>",
                xml_escape(source)
            )
        })
        .unwrap_or_default();

    let read_replica_identifiers_xml = if instance.read_replica_db_instance_identifiers.is_empty() {
        "<ReadReplicaDBInstanceIdentifiers/>".to_string()
    } else {
        format!(
            "<ReadReplicaDBInstanceIdentifiers>{}</ReadReplicaDBInstanceIdentifiers>",
            instance
                .read_replica_db_instance_identifiers
                .iter()
                .map(|id| format!(
                    "<ReadReplicaDBInstanceIdentifier>{}</ReadReplicaDBInstanceIdentifier>",
                    xml_escape(id)
                ))
                .collect::<String>()
        )
    };

    format!(
        "<DBInstanceIdentifier>{}</DBInstanceIdentifier>\
         <DBInstanceClass>{}</DBInstanceClass>\
         <Engine>{}</Engine>\
         <DBInstanceStatus>{}</DBInstanceStatus>\
         <MasterUsername>{}</MasterUsername>\
         {}\
         <Endpoint><Address>{}</Address><Port>{}</Port></Endpoint>\
         <AllocatedStorage>{}</AllocatedStorage>\
         <InstanceCreateTime>{}</InstanceCreateTime>\
         <PreferredBackupWindow>00:00-00:30</PreferredBackupWindow>\
         <BackupRetentionPeriod>1</BackupRetentionPeriod>\
         <DBSecurityGroups/>\
         <VpcSecurityGroups/>\
         <DBParameterGroups/>\
         <AvailabilityZone>us-east-1a</AvailabilityZone>\
         <PreferredMaintenanceWindow>sun:00:00-sun:00:30</PreferredMaintenanceWindow>\
         <MultiAZ>false</MultiAZ>\
         <EngineVersion>{}</EngineVersion>\
         <AutoMinorVersionUpgrade>true</AutoMinorVersionUpgrade>\
         {}\
         {}\
         <LicenseModel>postgresql-license</LicenseModel>\
         <OptionGroupMemberships/>\
         <PubliclyAccessible>{}</PubliclyAccessible>\
         <StorageType>gp2</StorageType>\
         <DbInstancePort>{}</DbInstancePort>\
         <StorageEncrypted>false</StorageEncrypted>\
         <DbiResourceId>{}</DbiResourceId>\
         <DeletionProtection>{}</DeletionProtection>\
         <DBInstanceArn>{}</DBInstanceArn>",
        xml_escape(&instance.db_instance_identifier),
        xml_escape(&instance.db_instance_class),
        xml_escape(&instance.engine),
        xml_escape(status),
        xml_escape(&instance.master_username),
        db_name_xml,
        xml_escape(&instance.endpoint_address),
        instance.port,
        instance.allocated_storage,
        instance.created_at.to_rfc3339(),
        xml_escape(&instance.engine_version),
        read_replica_identifiers_xml,
        read_replica_source_xml,
        if instance.publicly_accessible {
            "true"
        } else {
            "false"
        },
        instance.port,
        xml_escape(&instance.dbi_resource_id),
        if instance.deletion_protection {
            "true"
        } else {
            "false"
        },
        xml_escape(&instance.db_instance_arn),
    )
}

fn db_snapshot_xml(snapshot: &DbSnapshot) -> String {
    format!(
        "<DBSnapshotIdentifier>{}</DBSnapshotIdentifier>\
         <DBInstanceIdentifier>{}</DBInstanceIdentifier>\
         <SnapshotCreateTime>{}</SnapshotCreateTime>\
         <Engine>{}</Engine>\
         <EngineVersion>{}</EngineVersion>\
         <AllocatedStorage>{}</AllocatedStorage>\
         <Status>{}</Status>\
         <Port>{}</Port>\
         <MasterUsername>{}</MasterUsername>\
         {}\
         <DbiResourceId>{}</DbiResourceId>\
         <SnapshotType>{}</SnapshotType>\
         <DBSnapshotArn>{}</DBSnapshotArn>",
        xml_escape(&snapshot.db_snapshot_identifier),
        xml_escape(&snapshot.db_instance_identifier),
        snapshot.snapshot_create_time.to_rfc3339(),
        xml_escape(&snapshot.engine),
        xml_escape(&snapshot.engine_version),
        snapshot.allocated_storage,
        xml_escape(&snapshot.status),
        snapshot.port,
        xml_escape(&snapshot.master_username),
        snapshot
            .db_name
            .as_ref()
            .map(|name| format!("<DBName>{}</DBName>", xml_escape(name)))
            .unwrap_or_default(),
        xml_escape(&snapshot.dbi_resource_id),
        xml_escape(&snapshot.snapshot_type),
        xml_escape(&snapshot.db_snapshot_arn),
    )
}

fn db_instance_not_found(identifier: &str) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::NOT_FOUND,
        "DBInstanceNotFound",
        format!("DBInstance {} not found.", identifier),
    )
}

fn db_snapshot_not_found(identifier: &str) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::NOT_FOUND,
        "DBSnapshotNotFound",
        format!("DBSnapshot {} not found.", identifier),
    )
}

fn db_instance_not_found_by_arn(resource_name: &str) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::NOT_FOUND,
        "DBInstanceNotFound",
        format!("DBInstance {resource_name} not found."),
    )
}

fn find_instance_by_arn<'a>(
    state: &'a crate::state::RdsState,
    resource_name: &str,
) -> Result<&'a DbInstance, AwsServiceError> {
    state
        .instances
        .values()
        .find(|instance| instance.db_instance_arn == resource_name)
        .ok_or_else(|| db_instance_not_found_by_arn(resource_name))
}

fn find_instance_by_arn_mut<'a>(
    state: &'a mut crate::state::RdsState,
    resource_name: &str,
) -> Result<&'a mut DbInstance, AwsServiceError> {
    state
        .instances
        .values_mut()
        .find(|instance| instance.db_instance_arn == resource_name)
        .ok_or_else(|| db_instance_not_found_by_arn(resource_name))
}

fn merge_tags(existing: &mut Vec<RdsTag>, incoming: &[RdsTag]) {
    for tag in incoming {
        if let Some(existing_tag) = existing
            .iter_mut()
            .find(|candidate| candidate.key == tag.key)
        {
            existing_tag.value = tag.value.clone();
        } else {
            existing.push(tag.clone());
        }
    }
}

fn runtime_error_to_service_error(error: RuntimeError) -> AwsServiceError {
    match error {
        RuntimeError::Unavailable => AwsServiceError::aws_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "InvalidParameterValue",
            "Docker/Podman is required for RDS DB instances but is not available",
        ),
        RuntimeError::ContainerStartFailed(message) => AwsServiceError::aws_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "InternalFailure",
            message,
        ),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use bytes::Bytes;
    use chrono::Utc;
    use http::{HeaderMap, Method};
    use parking_lot::RwLock;
    use uuid::Uuid;

    use super::{
        db_instance_xml, filter_engine_versions, filter_orderable_options, merge_tags,
        optional_i32_param, parse_tag_keys, parse_tags, validate_create_request, RdsService,
    };
    use crate::state::{
        default_engine_versions, default_orderable_options, DbInstance, RdsState, RdsTag,
    };
    use fakecloud_core::service::{AwsRequest, AwsService};

    #[test]
    fn filter_engine_versions_matches_requested_engine() {
        let versions = default_engine_versions();

        let filtered =
            filter_engine_versions(&versions, &Some("postgres".to_string()), &None, &None);

        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].engine, "postgres");
    }

    #[test]
    fn filter_orderable_options_respects_instance_class() {
        let options = default_orderable_options();

        let filtered = filter_orderable_options(
            &options,
            &Some("postgres".to_string()),
            &Some("16.3".to_string()),
            &Some("db.t3.micro".to_string()),
            &None,
            Some(true),
        );

        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].db_instance_class, "db.t3.micro");
    }

    #[test]
    fn validate_create_request_rejects_unsupported_engine() {
        let error = validate_create_request("test-db", 20, "db.t3.micro", "mysql", "16.3", 5432)
            .expect_err("unsupported engine");

        assert_eq!(error.code(), "InvalidParameterValue");
    }

    #[test]
    fn optional_i32_param_rejects_invalid_integer() {
        let request = request("CreateDBInstance", &[("Port", "not-a-number")]);

        let error = optional_i32_param(&request, "Port").expect_err("invalid port");

        assert_eq!(error.code(), "InvalidParameterValue");
    }

    #[test]
    fn db_instance_xml_renders_endpoint_and_status() {
        let instance = DbInstance {
            db_instance_identifier: "test-db".to_string(),
            db_instance_arn: "arn:aws:rds:us-east-1:123456789012:db:test-db".to_string(),
            db_instance_class: "db.t3.micro".to_string(),
            engine: "postgres".to_string(),
            engine_version: "16.3".to_string(),
            db_instance_status: "available".to_string(),
            master_username: "admin".to_string(),
            db_name: Some("appdb".to_string()),
            endpoint_address: "127.0.0.1".to_string(),
            port: 15432,
            allocated_storage: 20,
            publicly_accessible: true,
            deletion_protection: false,
            created_at: Utc::now(),
            dbi_resource_id: format!("db-{}", Uuid::new_v4().simple()),
            master_user_password: "secret123".to_string(),
            container_id: "container".to_string(),
            host_port: 15432,
            tags: Vec::new(),
            read_replica_source_db_instance_identifier: None,
            read_replica_db_instance_identifiers: Vec::new(),
        };

        let xml = db_instance_xml(&instance, Some("creating"));

        assert!(xml.contains("<DBInstanceIdentifier>test-db</DBInstanceIdentifier>"));
        assert!(xml.contains("<DBInstanceStatus>creating</DBInstanceStatus>"));
        assert!(xml.contains("<Address>127.0.0.1</Address><Port>15432</Port>"));
    }

    #[test]
    fn parse_tags_reads_rds_query_shape() {
        let request = request(
            "AddTagsToResource",
            &[
                ("Tags.Tag.1.Key", "env"),
                ("Tags.Tag.1.Value", "dev"),
                ("Tags.Tag.2.Key", "team"),
                ("Tags.Tag.2.Value", "core"),
            ],
        );

        let tags = parse_tags(&request).expect("tags");

        assert_eq!(
            tags,
            vec![
                RdsTag {
                    key: "env".to_string(),
                    value: "dev".to_string(),
                },
                RdsTag {
                    key: "team".to_string(),
                    value: "core".to_string(),
                }
            ]
        );
    }

    #[test]
    fn parse_tag_keys_reads_member_shape() {
        let request = request(
            "RemoveTagsFromResource",
            &[("TagKeys.member.1", "env"), ("TagKeys.member.2", "team")],
        );

        let tag_keys = parse_tag_keys(&request).expect("tag keys");

        assert_eq!(tag_keys, vec!["env".to_string(), "team".to_string()]);
    }

    #[test]
    fn merge_tags_updates_existing_values() {
        let mut tags = vec![RdsTag {
            key: "env".to_string(),
            value: "dev".to_string(),
        }];

        merge_tags(
            &mut tags,
            &[
                RdsTag {
                    key: "env".to_string(),
                    value: "prod".to_string(),
                },
                RdsTag {
                    key: "team".to_string(),
                    value: "core".to_string(),
                },
            ],
        );

        assert_eq!(tags.len(), 2);
        assert_eq!(tags[0].value, "prod");
        assert_eq!(tags[1].key, "team");
    }

    #[tokio::test]
    async fn describe_engine_versions_returns_xml_body() {
        let service = RdsService::new(Arc::new(RwLock::new(RdsState::new(
            "123456789012",
            "us-east-1",
        ))));
        let request = request("DescribeDBEngineVersions", &[("Engine", "postgres")]);

        let response = service.handle(request).await.expect("response");
        let body = String::from_utf8(response.body.to_vec()).expect("utf8");

        assert!(body.contains("<DescribeDBEngineVersionsResponse"));
        assert!(body.contains("<Engine>postgres</Engine>"));
        assert!(body.contains("<DBParameterGroupFamily>postgres16</DBParameterGroupFamily>"));
    }

    fn request(action: &str, params: &[(&str, &str)]) -> AwsRequest {
        let mut query_params = HashMap::from([("Action".to_string(), action.to_string())]);
        for (key, value) in params {
            query_params.insert((*key).to_string(), (*value).to_string());
        }

        AwsRequest {
            service: "rds".to_string(),
            action: action.to_string(),
            region: "us-east-1".to_string(),
            account_id: "123456789012".to_string(),
            request_id: "test-request-id".to_string(),
            headers: HeaderMap::new(),
            query_params,
            body: Bytes::new(),
            path_segments: vec![],
            raw_path: "/".to_string(),
            raw_query: String::new(),
            method: Method::POST,
            is_query_protocol: true,
            access_key_id: None,
        }
    }
}
