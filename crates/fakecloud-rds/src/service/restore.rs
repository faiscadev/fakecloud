//! RDS `restore` family extracted from service.rs by audit-2026-05-19.

use super::*;

impl RdsService {
    pub(super) async fn restore_db_instance_to_point_in_time(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let target_id = required_query_param(request, "TargetDBInstanceIdentifier")?;
        // Real AWS accepts any one of SourceDBInstanceIdentifier /
        // SourceDbiResourceId / SourceDBInstanceAutomatedBackupsArn. The
        // Smithy model doesn't mark any of them required at the input
        // shape, so don't reject for omission — map the missing case
        // onto the declared `DBInstanceNotFoundFault` instead.
        let source_identifier = optional_query_param(request, "SourceDBInstanceIdentifier");
        let source_dbi_resource_id = optional_query_param(request, "SourceDbiResourceId");
        let source_backup_arn =
            optional_query_param(request, "SourceDBInstanceAutomatedBackupsArn");
        if source_identifier.is_none()
            && source_dbi_resource_id.is_none()
            && source_backup_arn.is_none()
        {
            return Err(db_instance_not_found("(none)"));
        }
        let vpc_security_group_ids = parse_vpc_security_group_ids(request);
        let tags = parse_tags(request)?;

        let (source_id, source_instance, db_name) = {
            let mut accounts = self.state.write();
            let state = accounts.get_or_create(&request.account_id);

            if !state.begin_instance_creation(&target_id) {
                return Err(AwsServiceError::aws_error(
                    StatusCode::CONFLICT,
                    "DBInstanceAlreadyExists",
                    format!("DBInstance {target_id} already exists."),
                ));
            }

            // Resolve any one of the three accepted source forms to the
            // stored DBInstance. SourceDbiResourceId matches the
            // per-instance immutable resource id; the backups ARN
            // ends in `:db:<identifier>` per AWS shape.
            let resolved: Option<(String, crate::state::DbInstance)> = source_identifier
                .as_deref()
                .and_then(|id| {
                    state
                        .instances
                        .get(id)
                        .cloned()
                        .map(|i| (id.to_string(), i))
                })
                .or_else(|| {
                    source_dbi_resource_id.as_deref().and_then(|rid| {
                        state
                            .instances
                            .iter()
                            .find(|(_, inst)| inst.dbi_resource_id == rid)
                            .map(|(k, v)| (k.clone(), v.clone()))
                    })
                })
                .or_else(|| {
                    // Real shape: `arn:aws:rds:<region>:<acct>:auto-backup:ab-<id>`.
                    // Only resolve when the ARN names a recognized
                    // resource type and the trailing id matches a stored
                    // dbi_resource_id exactly — substring/suffix matches
                    // would let partial ARNs restore from unrelated
                    // instances.
                    source_backup_arn.as_deref().and_then(|arn| {
                        let parts: Vec<&str> = arn.split(':').collect();
                        if parts.len() < 7
                            || parts[0] != "arn"
                            || parts[2] != "rds"
                            || parts[5] != "auto-backup"
                        {
                            return None;
                        }
                        let last = parts[6];
                        let bare = last.strip_prefix("ab-").unwrap_or(last);
                        if bare.is_empty() {
                            return None;
                        }
                        state
                            .instances
                            .iter()
                            .find(|(_, inst)| inst.dbi_resource_id == bare)
                            .map(|(k, v)| (k.clone(), v.clone()))
                    })
                });

            let (source_id, source_instance) = match resolved {
                Some(pair) => pair,
                None => {
                    state.cancel_instance_creation(&target_id);
                    let probe = source_identifier
                        .or(source_dbi_resource_id)
                        .or(source_backup_arn)
                        .unwrap_or_default();
                    return Err(db_instance_not_found(&probe));
                }
            };

            let default_db = default_db_name(&source_instance.engine);
            let db_name = source_instance
                .db_name
                .as_deref()
                .unwrap_or(default_db)
                .to_string();

            (source_id, source_instance, db_name)
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
            (
                s.next_dbi_resource_id(),
                s.db_instance_arn(request.region.as_str(), &target_id),
            )
        };
        let created_at = Utc::now();

        let restore_to_time = required_query_param(request, "RestoreTime")
            .ok()
            .or_else(|| required_query_param(request, "RestoreToTime").ok());
        let use_latest = required_query_param(request, "UseLatestRestorableTime")
            .ok()
            .map(|s| s.eq_ignore_ascii_case("true"))
            .unwrap_or(false);

        // Build a `creating` placeholder with no endpoint yet; the backgrounded
        // container start (below) fills in the endpoint and flips to
        // `available`, mirroring CreateDBInstance so the client isn't held for
        // the cold-image pull.
        let mut instance = build_pit_restored_instance(
            &target_id,
            db_instance_arn.clone(),
            dbi_resource_id,
            created_at,
            vpc_security_group_ids,
            &source_instance,
            &creating_placeholder_container(),
            tags.clone(),
        );
        instance.db_instance_status = "creating".to_string();
        instance.endpoint_address = String::new();
        instance.port = 0;

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

        self.spawn_finalize_restored_instance(
            runtime.clone(),
            request.account_id.clone(),
            request.region.clone(),
            target_id.clone(),
            db_instance_arn,
            source_instance.engine.clone(),
            source_instance.engine_version.clone(),
            source_instance.master_username.clone(),
            source_instance.master_user_password.clone(),
            db_name,
            tags,
            Some(dump_data),
            ("RDS-EVENT-0008", "DB instance restored to point in time"),
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

    pub(super) async fn restore_db_instance_from_s3(
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
                state.db_instance_arn(request.region.as_str(), &db_instance_identifier),
            )
        };

        let db_name = db_name_opt.unwrap_or_else(|| default_db_name(&engine).to_string());
        let created_at = Utc::now();

        // Build a `creating` placeholder; the backgrounded container start
        // (below) fills in the endpoint and flips to `available`.
        let mut instance = build_s3_restored_instance(
            &db_instance_identifier,
            db_instance_arn.clone(),
            dbi_resource_id,
            created_at,
            allocated_storage,
            db_instance_class,
            engine.clone(),
            engine_version.clone(),
            master_username.clone(),
            master_user_password.clone(),
            db_name.clone(),
            vpc_security_group_ids,
            &creating_placeholder_container(),
            tags.clone(),
        );
        instance.db_instance_status = "creating".to_string();
        instance.endpoint_address = String::new();
        instance.port = 0;

        self.state
            .write()
            .get_or_create(&request.account_id)
            .finish_instance_creation(instance.clone());

        self.spawn_finalize_restored_instance(
            runtime.clone(),
            request.account_id.clone(),
            request.region.clone(),
            db_instance_identifier.clone(),
            db_instance_arn,
            engine,
            engine_version,
            master_username,
            master_user_password,
            db_name,
            tags,
            Some(dump_data),
            ("RDS-EVENT-0043", "DB instance restored from S3 backup"),
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
}
