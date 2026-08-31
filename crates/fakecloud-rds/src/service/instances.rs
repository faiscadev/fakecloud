//! RDS `instances` family extracted from service.rs by audit-2026-05-19.

use super::*;

use crate::filters::{
    addresses_own_account, identifier_matches_type, normalized_identifier, parse_filters,
    sibling_rds_arn, RdsFilter,
};

/// True when `instance` satisfies every filter. Filters are AND-ed with
/// each other; the values within one filter are OR-ed. The names come
/// from the `DescribeDBInstances` docs: `db-cluster-id`,
/// `db-instance-id`, `dbi-resource-id`, `domain` and `engine`.
fn instance_matches_filters(instance: &DbInstance, filters: &[RdsFilter]) -> bool {
    filters.iter().all(|filter| match filter.name.as_str() {
        "db-instance-id" => filter.matches_any([
            Some(instance.db_instance_identifier.as_str()),
            Some(instance.db_instance_arn.as_str()),
        ]),
        "dbi-resource-id" => filter.matches(Some(instance.dbi_resource_id.as_str())),
        "engine" => filter.matches(Some(instance.engine.as_str())),
        "domain" => filter.matches(instance.domain.as_deref()),
        "db-cluster-id" => {
            let cluster = instance.db_cluster_identifier.as_deref();
            let cluster_arn =
                cluster.and_then(|id| sibling_rds_arn(&instance.db_instance_arn, "cluster", id));
            filter.matches_any([cluster, cluster_arn.as_deref()])
        }
        // A filter name AWS doesn't document for this operation
        // matches nothing — see the module docs on `crate::filters`.
        other => {
            tracing::debug!(filter = %other, "unrecognized RDS filter name; matching no resource");
            false
        }
    })
}

impl RdsService {
    pub(super) async fn create_db_instance(
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
        let engine_version = optional_query_param(request, "EngineVersion")
            .unwrap_or_else(|| default_engine_version(&engine).to_string());
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
        let db_subnet_group_name = optional_query_param(request, "DBSubnetGroupName");
        // Tags supplied at create time. Real RDS applies these to the new
        // instance immediately (they show up in DescribeDBInstances /
        // ListTagsForResource without a follow-up AddTagsToResource call),
        // so persist them on the instance rather than dropping them.
        let request_tags = parse_tags(request)?;

        validate_create_request(
            &db_instance_identifier,
            allocated_storage,
            &db_instance_class,
            &engine,
            &engine_version,
            port,
        )?;

        // Resolve the container runtime BEFORE reserving the identifier.
        // If it is unavailable this returns early, and reserving first
        // would leak the reservation into `in_progress_instance_ids`
        // (nothing ever cancels it), corrupting the service: every later
        // CreateDBInstance then fails with DBInstanceAlreadyExists while
        // Describe/Delete see an empty instance map. See issue #2107.
        let runtime = self.require_runtime()?.clone();

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
            // Same for the subnet group: AWS rejects an unknown name with
            // DBSubnetGroupNotFoundFault rather than silently ignoring it.
            validate_subnet_group_or_cancel(
                state,
                &db_instance_identifier,
                db_subnet_group_name.as_deref(),
            )?;
        }

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
                db_instance_arn: state
                    .db_instance_arn(request.region.as_str(), &db_instance_identifier),
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
                tags: request_tags,
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
                db_subnet_group_name: db_subnet_group_name.clone(),
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
                activity_stream: None,
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
            let instance_tags = instance.tags.clone();
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
                        &instance_tags,
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

                        let instance_present = {
                            let mut accounts = state_handle.write();
                            let state = accounts.get_or_create(&account_id);
                            if let Some(inst) = state.instances.get_mut(&id) {
                                inst.db_instance_status = "available".to_string();
                                inst.endpoint_address = running.endpoint_address.clone();
                                inst.port = i32::from(running.endpoint_port);
                                inst.host_port = running.host_port;
                                inst.container_id = running.container_id;
                                // Register as cluster member so failover /
                                // restore paths can find the writer.
                                if let Some(ref cid) = cluster_id_for_attach {
                                    attach_cluster_member(state, cid, &id);
                                }
                                true
                            } else {
                                false
                            }
                        };
                        // DeleteDBInstance raced this create: it removed the
                        // instance from state and called stop_container(id)
                        // while ensure_postgres was still mid-flight (before
                        // it registered the container), so that stop was a
                        // no-op. The container the runtime just registered is
                        // now an orphan holding a host port. Stop and remove
                        // it here, mirroring ElastiCache's deleted-while-
                        // creating None branch.
                        if !instance_present {
                            tracing::info!(
                                db_instance_identifier = %id,
                                "instance deleted during create; reaping orphaned backing container",
                            );
                            runtime.stop_container(&id).await;
                            save_snapshot_static(
                                state_handle.clone(),
                                snapshot_store.clone(),
                                snapshot_lock.clone(),
                            )
                            .await;
                            return;
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
                    db_instance_xml(
                        &instance,
                        None,
                        self.subnet_group_of(&request.account_id, &instance)
                            .as_ref(),
                    )
                ),
                &request.request_id,
            ),
        ))
    }

    pub(super) async fn delete_db_instance(
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

        let (instance, subnet_group) = {
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

            let subnet_group = instance_subnet_group(state, &instance).cloned();
            (instance, subnet_group)
        };

        // When a final snapshot was requested, its backgrounded finalizer owns
        // the teardown: the source container must stay up until the async dump
        // has read from it, so stopping it here would race the dump to empty.
        if final_db_snapshot_identifier.is_none() {
            if let Some(runtime) = &self.runtime {
                runtime.stop_container(&db_instance_identifier).await;
                // Drop the persisted data volume so a future instance reusing this
                // identifier starts clean instead of inheriting deleted data
                // (bug-audit 2026-06-20, 4.2).
                runtime
                    .remove_data_volume(&request.account_id, &db_instance_identifier)
                    .await;
            }
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
                    db_instance_xml(&instance, Some("deleting"), subnet_group.as_ref(),)
                ),
                &request.request_id,
            ),
        ))
    }

    pub(super) fn modify_db_instance(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
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
        let monitoring_role_arn = optional_query_param(request, "MonitoringRoleArn");
        let performance_insights_kms_key_id =
            optional_query_param(request, "PerformanceInsightsKMSKeyId");
        let performance_insights_retention_period = parse_optional_i32(
            optional_query_param(request, "PerformanceInsightsRetentionPeriod").as_deref(),
        )?;
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

        let new_db_instance_identifier = optional_query_param(request, "NewDBInstanceIdentifier");
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
            || rotate_master_user_password.is_some()
            || new_db_instance_identifier
                .as_deref()
                .is_some_and(|new_id| new_id != db_instance_identifier);
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&request.account_id);
        let instance = state
            .instances
            .get_mut(&db_instance_identifier)
            .ok_or_else(|| db_instance_not_found(&db_instance_identifier))?;

        // Reject storage shrink / engine-version downgrade before mutating
        // anything. Real AWS surfaces these as InvalidParameterCombination;
        // silently accepting them corrupts the reported size/version.
        if let Some(new_storage) = allocated_storage {
            if new_storage < instance.allocated_storage {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterCombination",
                    format!(
                        "Invalid storage size for engine name {}: new allocated storage of {} is smaller than the current value of {}.",
                        instance.engine, new_storage, instance.allocated_storage
                    ),
                ));
            }
        }
        if let Some(ref new_version) = engine_version {
            if is_version_downgrade(&instance.engine_version, new_version) {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterCombination",
                    format!(
                        "Cannot downgrade the DB engine version of instance {} from {} to {}.",
                        db_instance_identifier, instance.engine_version, new_version
                    ),
                ));
            }
        }

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
        // Enhanced Monitoring role and Performance Insights settings apply
        // immediately in AWS (no reboot), mirroring their already-applied
        // companions MonitoringInterval / EnablePerformanceInsights.
        if let Some(arn) = monitoring_role_arn {
            instance.monitoring_role_arn = Some(arn);
        }
        if let Some(kms) = performance_insights_kms_key_id {
            instance.performance_insights_kms_key_id = Some(kms);
        }
        if let Some(days) = performance_insights_retention_period {
            instance.performance_insights_retention_period = Some(days);
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
                // The reachable endpoint port is the container's published
                // `host_port`; we can't re-publish a live container on a new
                // port, so advertising an arbitrary DBPortNumber would point
                // the endpoint at a port nothing listens on. Keep the
                // advertised port equal to the reachable one whenever a
                // container backs the instance; honor the requested port only
                // when there's no live container (offline / no-backend mode),
                // where the endpoint is suppressed anyway.
                instance.port = if instance.host_port != 0 {
                    i32::from(instance.host_port)
                } else {
                    p
                };
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

        // NewDBInstanceIdentifier: rename the instance key + ARN + endpoint
        // host. AWS applies the rename immediately when ApplyImmediately is
        // set (otherwise at the next maintenance window); we apply it in
        // place, mirroring the cluster rename path. Reject when the target
        // already names another instance (or one mid-creation) so the rename
        // can't silently clobber real data. The `&mut instance` borrow above
        // has ended, so we can move the row inside `state.instances`.
        let final_identifier = match new_db_instance_identifier {
            Some(ref new_id) if *new_id != db_instance_identifier => {
                if state.instances.contains_key(new_id)
                    || state.in_progress_instance_ids.contains(new_id)
                {
                    return Err(AwsServiceError::aws_error(
                        StatusCode::BAD_REQUEST,
                        "DBInstanceAlreadyExists",
                        format!("DBInstance {new_id} already exists."),
                    ));
                }
                let new_arn = state.db_instance_arn(request.region.as_str(), new_id);
                let mut moved = state
                    .instances
                    .remove(&db_instance_identifier)
                    .expect("instance present after modify");
                // Keep the advertised endpoint host consistent if it embedded
                // the old identifier (real endpoints are `<id>.<...>`); the
                // local 127.0.0.1 endpoint is unaffected.
                if moved.endpoint_address.contains(&db_instance_identifier) {
                    moved.endpoint_address = moved
                        .endpoint_address
                        .replace(&db_instance_identifier, new_id);
                }
                moved.db_instance_identifier = new_id.clone();
                moved.db_instance_arn = new_arn;
                state.instances.insert(new_id.clone(), moved);
                new_id.clone()
            }
            _ => db_instance_identifier.clone(),
        };

        let instance = state
            .instances
            .get(&final_identifier)
            .expect("instance present after modify");
        let instance_arn = instance.db_instance_arn.clone();
        let xml = query_response_xml(
            "ModifyDBInstance",
            RDS_NS,
            &format!(
                "<DBInstance>{}</DBInstance>",
                db_instance_xml(
                    instance,
                    Some("modifying"),
                    instance_subnet_group(state, instance),
                )
            ),
            &request.request_id,
        );
        drop(accounts);

        self.emit_event(
            RdsSourceType::DbInstance,
            &final_identifier,
            &instance_arn,
            "RDS-EVENT-0014",
            &["configuration change"],
            "DB instance was modified",
        );

        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    pub(super) async fn reboot_db_instance(
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

        {
            // Validate existence before touching the runtime so a missing
            // identifier returns DBInstanceNotFoundFault, not a runtime error.
            let accounts = self.state.read();
            let empty = RdsState::new(&request.account_id, &request.region);
            let state = accounts.get(&request.account_id).unwrap_or(&empty);
            if !state.instances.contains_key(&db_instance_identifier) {
                return Err(db_instance_not_found(&db_instance_identifier));
            }
        }

        let runtime = self.require_runtime()?;

        // Flip the stored status to `rebooting` and return immediately. The
        // container restart + readiness wait runs in the background — for
        // engines like Oracle / SQL Server / Db2 it can take 3-6 minutes
        // (runtime log-marker deadlines), well past the ~60s client read
        // timeout. Awaiting it inline timed out the CLI on every non-PG/MySQL
        // reboot (bug-hunt 2026-06-24, 3.1); mirror the EC2 RebootInstances fix.
        let instance = {
            let mut accounts = self.state.write();
            let state = accounts.get_or_create(&request.account_id);
            let instance = state
                .instances
                .get_mut(&db_instance_identifier)
                .ok_or_else(|| db_instance_not_found(&db_instance_identifier))?;
            instance.db_instance_status = "rebooting".to_string();
            // Apply pending modifications SYNCHRONOUSLY: AWS applies these to the
            // instance metadata as part of the reboot, and the response must
            // reflect the new values with pending cleared. Only the container
            // restart (the slow part) is backgrounded below.
            if let Some(pending) = instance.pending_modified_values.take() {
                apply_pending_to_instance(instance, pending);
            }
            instance.clone()
        };

        {
            let state_handle = self.state.clone();
            let runtime = runtime.clone();
            let delivery_bus = self.delivery_bus.clone();
            let snapshot_store = self.snapshot_store.clone();
            let snapshot_lock = self.snapshot_lock.clone();
            let id = db_instance_identifier.clone();
            let account_id = request.account_id.clone();
            let inst = instance.clone();
            tokio::spawn(async move {
                let logical_db = inst
                    .db_name
                    .clone()
                    .unwrap_or_else(|| default_db_name(&inst.engine).to_string());
                let running = match runtime
                    .restart_container(
                        &id,
                        &inst.engine,
                        &inst.master_username,
                        &inst.master_user_password,
                        &logical_db,
                    )
                    .await
                {
                    Ok(running) => running,
                    Err(error) => {
                        // The container restart failed. Without resetting the
                        // status here the instance is stuck reporting
                        // "rebooting" forever (DescribeDBInstances never clears
                        // it), so flip it back to "available" — the previous
                        // container is still running, the reboot just didn't
                        // land — then persist and emit a failure event so the
                        // stuck state is both cleared and observable. Mirrors the
                        // CreateDBInstance background task's Err handling.
                        tracing::error!(%error, db_instance_identifier=%id, "reboot_db_instance restart failed");
                        let arn = {
                            let mut accounts = state_handle.write();
                            let state = accounts.get_or_create(&account_id);
                            let Some(instance) = state.instances.get_mut(&id) else {
                                return;
                            };
                            instance.db_instance_status = "available".to_string();
                            instance.db_instance_arn.clone()
                        };
                        save_snapshot_static(state_handle.clone(), snapshot_store, snapshot_lock)
                            .await;
                        emit_event_static_with_state(
                            delivery_bus.as_ref(),
                            Some(&state_handle),
                            Some(&account_id),
                            RdsSourceType::DbInstance,
                            &id,
                            &arn,
                            "RDS-EVENT-0006",
                            &["availability"],
                            "DB instance reboot failed; instance returned to available",
                        );
                        return;
                    }
                };
                let arn = {
                    let mut accounts = state_handle.write();
                    let state = accounts.get_or_create(&account_id);
                    let Some(instance) = state.instances.get_mut(&id) else {
                        return;
                    };
                    instance.host_port = running.host_port;
                    instance.port = i32::from(running.endpoint_port);
                    instance.endpoint_address = running.endpoint_address.clone();
                    // Pending modifications were already applied synchronously
                    // in the handler; the background task only reconciles the
                    // restarted container's endpoint + flips to available.
                    instance.db_instance_status = "available".to_string();
                    instance.db_instance_arn.clone()
                };
                emit_event_static_with_state(
                    delivery_bus.as_ref(),
                    Some(&state_handle),
                    Some(&account_id),
                    RdsSourceType::DbInstance,
                    &id,
                    &arn,
                    "RDS-EVENT-0006",
                    &["availability"],
                    "DB instance restarted",
                );
                save_snapshot_static(state_handle.clone(), snapshot_store, snapshot_lock).await;
            });
        }

        Ok(AwsResponse::xml(
            StatusCode::OK,
            query_response_xml(
                "RebootDBInstance",
                RDS_NS,
                &format!(
                    "<DBInstance>{}</DBInstance>",
                    db_instance_xml(
                        &instance,
                        Some("rebooting"),
                        self.subnet_group_of(&request.account_id, &instance)
                            .as_ref(),
                    )
                ),
                &request.request_id,
            ),
        ))
    }

    pub(super) fn describe_db_instances(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        // "The user-supplied instance identifier or the Amazon Resource
        // Name (ARN) of the DB instance" -- resolve either form. A DB
        // instance is never shared across accounts, so an ARN naming a
        // different account is simply not found here; resolving it by
        // bare id would report this account's same-named instance and
        // make a cross-account misconfiguration look like success.
        let raw_instance_identifier = optional_query_param(request, "DBInstanceIdentifier");
        if let Some(raw) = raw_instance_identifier.as_deref() {
            // An ARN of another account, or of another resource TYPE,
            // names something that is not a DB instance here. Both are
            // not-found: a `None` identifier would otherwise read as "no
            // filter" and list every instance in the account.
            if !addresses_own_account(raw, &request.account_id)
                || !identifier_matches_type(raw, "db")
            {
                return Err(db_instance_not_found(raw));
            }
        }
        let db_instance_identifier = normalized_identifier(raw_instance_identifier, "db");
        let marker = optional_query_param(request, "Marker");
        let max_records = optional_query_param(request, "MaxRecords");
        let filters = parse_filters(request);

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

            // AWS AND-s the identifier with any filters: the instance
            // exists, so this is an empty result rather than
            // `DBInstanceNotFound`.
            let body = if instance_matches_filters(&instance, &filters) {
                format!(
                    "<DBInstances><DBInstance>{}</DBInstance></DBInstances>",
                    db_instance_xml(&instance, None, instance_subnet_group(state, &instance))
                )
            } else {
                "<DBInstances></DBInstances>".to_string()
            };

            return Ok(AwsResponse::xml(
                StatusCode::OK,
                query_response_xml("DescribeDBInstances", RDS_NS, &body, &request.request_id),
            ));
        }

        // Get all instances matching the filters, sorted by created_at,
        // then identifier
        let mut instances: Vec<DbInstance> = state
            .instances
            .values()
            .filter(|instance| instance_matches_filters(instance, &filters))
            .cloned()
            .collect();
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
                                db_instance_xml(
                                    instance,
                                    None,
                                    instance_subnet_group(state, instance),
                                )
                            )
                        })
                        .collect::<String>(),
                    marker_xml
                ),
                &request.request_id,
            ),
        ))
    }

    pub(super) fn describe_orderable_db_instance_options(
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
}
