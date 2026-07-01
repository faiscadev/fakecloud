//! RDS `replicas` family extracted from service.rs by audit-2026-05-19.

use super::*;

impl RdsService {
    pub(super) async fn create_db_instance_read_replica(
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
        let runtime = runtime.clone();

        // Build a `creating` placeholder replica; the backgrounded container
        // start (below) fills in the endpoint and flips to `available`,
        // mirroring CreateDBInstance so the client isn't held for the cold
        // image pull.
        let mut replica = build_read_replica_instance(
            &db_instance_identifier,
            db_instance_arn.clone(),
            dbi_resource_id,
            created_at,
            &source_db_instance_identifier,
            &source_instance,
            &creating_placeholder_container(),
        );
        replica.db_instance_status = "creating".to_string();
        replica.endpoint_address = String::new();
        replica.port = 0;

        // Register the replica against its source synchronously so a concurrent
        // DescribeDBInstances of the source sees the linkage immediately.
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
            return Err(db_instance_not_found(&source_db_instance_identifier));
        }

        self.spawn_finalize_restored_instance(
            runtime,
            request.account_id.clone(),
            request.region.clone(),
            db_instance_identifier.clone(),
            db_instance_arn,
            source_instance.engine.clone(),
            source_instance.engine_version.clone(),
            source_instance.master_username.clone(),
            source_instance.master_user_password.clone(),
            db_name,
            // A read replica inherits the source instance's Pod scheduling tags.
            source_instance.tags.clone(),
            Some(dump_data),
            ("RDS-EVENT-0005", "Read replica DB instance created"),
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
}
