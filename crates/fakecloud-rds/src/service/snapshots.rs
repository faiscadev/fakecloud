//! RDS `snapshots` family extracted from service.rs by audit-2026-05-19.

use super::*;

impl RdsService {
    /// Take a final snapshot of an instance that is about to be deleted,
    /// persisting the dumped database into `state.snapshots`. The DLQ-style
    /// conflict check runs twice — once under the read lock before paying
    /// for the dump, once under the write lock before committing — to keep
    /// concurrent deletes from colliding.
    pub(super) async fn create_final_db_snapshot(
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

    pub(super) async fn create_db_snapshot(
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

    pub(super) fn describe_db_snapshots(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
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

    pub(super) fn delete_db_snapshot(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
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

    pub(super) async fn restore_db_instance_from_db_snapshot(
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
        // the declared `DBSnapshotNotFoundFault` first. If the runtime
        // isn't configured we have to roll back the pending instance
        // creation marker first — otherwise the slot stays reserved
        // and the next attempt hits DBInstanceAlreadyExists.
        let runtime = match self.require_runtime() {
            Ok(r) => r,
            Err(e) => {
                self.state
                    .write()
                    .get_or_create(&request.account_id)
                    .cancel_instance_creation(&db_instance_identifier);
                return Err(e);
            }
        };

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
}
