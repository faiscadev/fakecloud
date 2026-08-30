//! RDS `snapshots` family extracted from service.rs by audit-2026-05-19.

use super::*;

use crate::filters::{normalized_identifier, parse_filters, sibling_rds_arn, RdsFilter};

/// The account ids a snapshot's `restore` attribute is shared with.
/// `ModifyDBSnapshotAttribute` writes it; the literal `all` marks the
/// snapshot public.
fn snapshot_restore_targets(snapshot: &DbSnapshot) -> &[String] {
    snapshot
        .snapshot_attributes
        .get("restore")
        .map(Vec::as_slice)
        .unwrap_or_default()
}

/// True when `snapshot` is shared with `account_id`.
fn snapshot_shared_with(snapshot: &DbSnapshot, account_id: &str) -> bool {
    snapshot_restore_targets(snapshot)
        .iter()
        .any(|target| target == account_id)
}

/// True when `snapshot` is public (`restore` shared with `all`).
fn snapshot_is_public(snapshot: &DbSnapshot) -> bool {
    snapshot_restore_targets(snapshot)
        .iter()
        .any(|target| target == "all")
}

/// True when a snapshot the caller owns satisfies the `SnapshotType`
/// request parameter. AWS returns every owned type when the parameter is
/// absent; `shared` and `public` select other accounts' snapshots instead
/// and are handled separately.
fn snapshot_matches_type(snapshot: &DbSnapshot, snapshot_type: Option<&str>) -> bool {
    match snapshot_type {
        Some("shared") | Some("public") => false,
        Some(wanted) => snapshot.snapshot_type == wanted,
        None => true,
    }
}

/// True when `snapshot` satisfies every filter. Filters are AND-ed with
/// each other; the values within one filter are OR-ed. The names come
/// from the `DescribeDBSnapshots` docs: `db-instance-id`,
/// `db-snapshot-id`, `dbi-resource-id`, `engine` and `snapshot-type`.
fn snapshot_matches_filters(snapshot: &DbSnapshot, filters: &[RdsFilter]) -> bool {
    filters.iter().all(|filter| match filter.name.as_str() {
        // Accepts DB instance identifiers and DB instance ARNs; the
        // snapshot ARN supplies the partition/region/account needed to
        // rebuild the source instance ARN.
        "db-instance-id" => {
            let instance_arn = sibling_rds_arn(
                &snapshot.db_snapshot_arn,
                "db",
                &snapshot.db_instance_identifier,
            );
            filter.matches_any([
                Some(snapshot.db_instance_identifier.as_str()),
                instance_arn.as_deref(),
            ])
        }
        "db-snapshot-id" => filter.matches_any([
            Some(snapshot.db_snapshot_identifier.as_str()),
            Some(snapshot.db_snapshot_arn.as_str()),
        ]),
        "dbi-resource-id" => filter.matches(Some(snapshot.dbi_resource_id.as_str())),
        "engine" => filter.matches(Some(snapshot.engine.as_str())),
        "snapshot-type" => filter.matches(Some(snapshot.snapshot_type.as_str())),
        // A filter name AWS doesn't document for this operation
        // matches nothing — see the module docs on `crate::filters`.
        other => {
            tracing::debug!(filter = %other, "unrecognized RDS filter name; matching no resource");
            false
        }
    })
}

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
                format!(
                    "Docker/Podman is required for RDS snapshots but is not available. {}",
                    fakecloud_core::container_net::CONTAINER_RUNTIME_HINT
                ),
            )
        })?;

        let (instance_for_snapshot, db_name) = {
            let mut accounts = self.state.write();
            let state = accounts.get_or_create(account_id);

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

            // Record the snapshot synchronously as `creating` so
            // DescribeDBSnapshots reflects it immediately (AWS-faithful for
            // the in-progress call); the backgrounded dump flips it to
            // `available` on completion.
            let snapshot_arn = state.db_snapshot_arn(region, snapshot_id);
            let snapshot = DbSnapshot {
                db_snapshot_identifier: snapshot_id.to_string(),
                db_snapshot_arn: snapshot_arn,
                db_instance_identifier: db_instance_identifier.to_string(),
                snapshot_create_time: Utc::now(),
                engine: instance.engine.clone(),
                engine_version: instance.engine_version.clone(),
                allocated_storage: instance.allocated_storage,
                status: "creating".to_string(),
                port: instance.port,
                master_username: instance.master_username.clone(),
                db_name: instance.db_name.clone(),
                dbi_resource_id: instance.dbi_resource_id.clone(),
                // AWS reports a `FinalDBSnapshotIdentifier` snapshot as
                // `manual`: it outlives the instance, unlike automated
                // backups, which are deleted with it. This matters now
                // that SnapshotType and the `snapshot-type` filter
                // actually narrow the result.
                snapshot_type: "manual".to_string(),
                master_user_password: instance.master_user_password.clone(),
                tags: Vec::new(),
                dump_data: Vec::new(),
                availability_zone: instance.availability_zone.clone(),
                vpc_id: None,
                instance_create_time: Some(instance.created_at),
                license_model: Some(
                    service_helpers::license_model_for_engine(&instance.engine).to_string(),
                ),
                iops: instance.iops,
                option_group_name: instance.option_group_name.clone(),
                percent_progress: Some(0),
                storage_type: instance.storage_type.clone(),
                encrypted: instance.storage_encrypted,
                kms_key_id: instance.kms_key_id.clone(),
                iam_database_authentication_enabled: instance.iam_database_authentication_enabled,
                timezone: None,
                storage_throughput: None,
                snapshot_attributes: std::collections::BTreeMap::new(),
            };
            state.snapshots.insert(snapshot_id.to_string(), snapshot);

            (instance, db_name)
        };

        // Background the dump AND the source container teardown: DeleteDBInstance
        // must not stop the container until the final snapshot has read from it.
        self.spawn_finalize_snapshot(
            runtime.clone(),
            account_id.to_string(),
            snapshot_id.to_string(),
            db_instance_identifier.to_string(),
            instance_for_snapshot.engine.clone(),
            instance_for_snapshot.master_username.clone(),
            instance_for_snapshot.master_user_password.clone(),
            db_name,
            true,
        );
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
                format!(
                    "Docker/Podman is required for RDS snapshots but is not available. {}",
                    fakecloud_core::container_net::CONTAINER_RUNTIME_HINT
                ),
            )
        })?;

        // Record the snapshot synchronously as `creating` and return right
        // away; the slow mysqldump/pg_dump runs in a detached task that flips
        // the row to `available` on completion. AWS returns `creating` for the
        // in-progress CreateDBSnapshot call, so DescribeDBSnapshots right after
        // create sees `creating`, then `available` once the dump lands.
        let snapshot = {
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
                db_snapshot_arn: state
                    .db_snapshot_arn(request.region.as_str(), &db_snapshot_identifier),
                db_instance_identifier: instance.db_instance_identifier.clone(),
                snapshot_create_time: Utc::now(),
                engine: instance.engine.clone(),
                engine_version: instance.engine_version.clone(),
                allocated_storage: instance.allocated_storage,
                status: "creating".to_string(),
                port: instance.port,
                master_username: instance.master_username.clone(),
                db_name: instance.db_name.clone(),
                dbi_resource_id: instance.dbi_resource_id.clone(),
                snapshot_type: "manual".to_string(),
                master_user_password: instance.master_user_password.clone(),
                tags: Vec::new(),
                dump_data: Vec::new(),
                availability_zone: instance.availability_zone.clone(),
                vpc_id: None,
                instance_create_time: Some(instance.created_at),
                license_model: Some(
                    service_helpers::license_model_for_engine(&instance.engine).to_string(),
                ),
                iops: instance.iops,
                option_group_name: instance.option_group_name.clone(),
                percent_progress: Some(0),
                storage_type: instance.storage_type.clone(),
                encrypted: instance.storage_encrypted,
                kms_key_id: instance.kms_key_id.clone(),
                iam_database_authentication_enabled: instance.iam_database_authentication_enabled,
                timezone: None,
                storage_throughput: None,
                snapshot_attributes: std::collections::BTreeMap::new(),
            };

            state
                .snapshots
                .insert(db_snapshot_identifier.clone(), snapshot.clone());
            snapshot
        };
        let snapshot_arn = snapshot.db_snapshot_arn.clone();

        self.spawn_finalize_snapshot(
            runtime.clone(),
            request.account_id.clone(),
            db_snapshot_identifier.clone(),
            db_instance_identifier.clone(),
            instance.engine.clone(),
            instance.master_username.clone(),
            instance.master_user_password.clone(),
            db_name,
            false,
        );

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
        // Snapshot / instance identifiers are accepted in ARN form here
        // the same way DescribeDBClusterSnapshots accepts them.
        let db_snapshot_identifier =
            normalized_identifier(optional_query_param(request, "DBSnapshotIdentifier"));
        let db_instance_identifier =
            normalized_identifier(optional_query_param(request, "DBInstanceIdentifier"));
        let snapshot_type = optional_query_param(request, "SnapshotType");
        let include_shared =
            parse_optional_bool(optional_query_param(request, "IncludeShared").as_deref())?
                .unwrap_or(false);
        let include_public =
            parse_optional_bool(optional_query_param(request, "IncludePublic").as_deref())?
                .unwrap_or(false);
        let marker = optional_query_param(request, "Marker");
        let max_records = optional_query_param(request, "MaxRecords");
        let filters = parse_filters(request);

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
                // A snapshot another account shared with this caller is
                // listed by SnapshotType=shared / IncludeShared, so
                // re-reading it by id or ARN has to resolve too --
                // otherwise the emulator 404s a row it just reported.
                .or_else(|| {
                    accounts
                        .iter()
                        .filter(|(owner, _)| *owner != request.account_id)
                        .find_map(|(_, other)| {
                            other.snapshots.get(&snapshot_id).filter(|snapshot| {
                                snapshot_shared_with(snapshot, &request.account_id)
                                    || snapshot_is_public(snapshot)
                            })
                        })
                        .cloned()
                })
                .ok_or_else(|| db_snapshot_not_found(&snapshot_id))?;

            // AWS AND-s the identifier with SnapshotType and any
            // filters: the snapshot exists, so a non-match is an empty
            // result rather than `DBSnapshotNotFound`.
            // A foreign snapshot answers to `shared` / `public` rather
            // than to its own stored type.
            let owned = state
                .snapshots
                .contains_key(&snapshot.db_snapshot_identifier);
            let type_ok = match snapshot_type.as_deref() {
                Some("shared") => !owned && snapshot_shared_with(&snapshot, &request.account_id),
                Some("public") => !owned && snapshot_is_public(&snapshot),
                other => owned && snapshot_matches_type(&snapshot, other),
            };
            let body = if type_ok && snapshot_matches_filters(&snapshot, &filters) {
                format!(
                    "<DBSnapshots><DBSnapshot>{}</DBSnapshot></DBSnapshots>",
                    db_snapshot_xml(&snapshot)
                )
            } else {
                "<DBSnapshots></DBSnapshots>".to_string()
            };

            return Ok(AwsResponse::xml(
                StatusCode::OK,
                query_response_xml("DescribeDBSnapshots", RDS_NS, &body, &request.request_id),
            ));
        }

        // Get snapshots, narrowed by instance identifier, SnapshotType
        // and Filters — all AND-ed together, as on real AWS.
        let mut snapshots: Vec<DbSnapshot> = state
            .snapshots
            .values()
            .filter(|snapshot| {
                db_instance_identifier
                    .as_deref()
                    .is_none_or(|instance_id| snapshot.db_instance_identifier == instance_id)
                    && snapshot_matches_type(snapshot, snapshot_type.as_deref())
                    && snapshot_matches_filters(snapshot, &filters)
            })
            .cloned()
            .collect();

        // `shared` / `public` select snapshots OTHER accounts have shared
        // with this caller (or with everyone) via
        // ModifyDBSnapshotAttribute's `restore` attribute -- AWS reports
        // them here, and IncludeShared / IncludePublic add them to an
        // otherwise-unqualified listing.
        // AWS: IncludeShared / IncludePublic do not apply when
        // SnapshotType selects an owned type (`manual`, `automated`,
        // `awsbackup`) -- only an unqualified listing is widened by them.
        let owned_type_selected = matches!(
            snapshot_type.as_deref(),
            Some(other) if other != "shared" && other != "public"
        );
        let want_shared =
            snapshot_type.as_deref() == Some("shared") || (include_shared && !owned_type_selected);
        let want_public =
            snapshot_type.as_deref() == Some("public") || (include_public && !owned_type_selected);
        if want_shared || want_public {
            for (owner, other) in accounts.iter() {
                if owner == request.account_id {
                    continue;
                }
                snapshots.extend(
                    other
                        .snapshots
                        .values()
                        .filter(|snapshot| {
                            (want_shared && snapshot_shared_with(snapshot, &request.account_id))
                                || (want_public && snapshot_is_public(snapshot))
                        })
                        .filter(|snapshot| {
                            db_instance_identifier.as_deref().is_none_or(|instance_id| {
                                snapshot.db_instance_identifier == instance_id
                            }) && snapshot_matches_filters(snapshot, &filters)
                        })
                        .cloned(),
                );
            }
        }

        // Sort by creation time, then identifier
        snapshots.sort_by(|a, b| {
            a.snapshot_create_time
                .cmp(&b.snapshot_create_time)
                // Tie-break on the ARN, which is what pagination keys on.
                .then_with(|| a.db_snapshot_arn.cmp(&b.db_snapshot_arn))
        });

        // Paginate on the ARN, not the identifier: a listing widened with
        // shared / public rows spans accounts, where an identifier is no
        // longer unique and a marker could resolve back to the wrong row.
        let paginated = paginate(snapshots, marker, max_records, |snap| &snap.db_snapshot_arn)?;

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
        // Resolve the ARN form here too: describe, restore and copy all
        // accept it, and a delete that doesn't would report success (or
        // NotFound) while leaving the snapshot in place.
        let db_snapshot_identifier =
            normalized_identifier(Some(required_query_param(request, "DBSnapshotIdentifier")?))
                .ok_or_else(|| db_snapshot_not_found("(none)"))?;

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
        // `aws_db_instance.snapshot_identifier` in the Terraform provider
        // holds a full snapshot ARN, so resolve that form here too.
        let db_snapshot_identifier = normalized_identifier(
            optional_query_param(request, "DBSnapshotIdentifier")
                .or_else(|| optional_query_param(request, "DBClusterSnapshotIdentifier")),
        )
        .ok_or_else(|| db_snapshot_not_found("(none)"))?;
        let vpc_security_group_ids = parse_vpc_security_group_ids(request);
        let tags = parse_tags(request)?;
        let db_subnet_group_name = optional_query_param(request, "DBSubnetGroupName");

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

            // Reject an explicit-but-unknown subnet group before provisioning,
            // rolling back the reservation (mirrors CreateDBInstance).
            validate_subnet_group_or_cancel(
                state,
                &db_instance_identifier,
                db_subnet_group_name.as_deref(),
            )?;

            let dbi_resource_id = state.next_dbi_resource_id();
            let db_instance_arn =
                state.db_instance_arn(request.region.as_str(), &db_instance_identifier);
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

        let runtime = runtime.clone();
        let db_name = snapshot
            .db_name
            .clone()
            .unwrap_or_else(|| default_db_name(&snapshot.engine).to_string());

        // Build a `creating` placeholder; the backgrounded container start
        // (below) fills in the endpoint and flips to `available`.
        let mut instance = build_restored_instance(
            &db_instance_identifier,
            db_instance_arn.clone(),
            dbi_resource_id,
            created_at,
            vpc_security_group_ids,
            &snapshot,
            &creating_placeholder_container(),
            tags.clone(),
        );
        instance.db_instance_status = "creating".to_string();
        instance.endpoint_address = String::new();
        instance.port = 0;
        // An explicit DBSubnetGroupName places the restored instance in that
        // group (validated above); the builder hardcodes None otherwise.
        if let Some(ref name) = db_subnet_group_name {
            instance.db_subnet_group_name = Some(name.clone());
        }

        self.state
            .write()
            .get_or_create(&request.account_id)
            .finish_instance_creation(instance.clone());

        self.spawn_finalize_restored_instance(
            runtime,
            request.account_id.clone(),
            request.region.clone(),
            db_instance_identifier.clone(),
            db_instance_arn,
            snapshot.engine.clone(),
            snapshot.engine_version.clone(),
            snapshot.master_username.clone(),
            snapshot.master_user_password.clone(),
            db_name,
            tags,
            Some(snapshot.dump_data.clone()),
            // In-memory dump from the snapshot; no live source to dump.
            None,
            ("RDS-EVENT-0043", "DB instance restored from snapshot"),
        );

        Ok(AwsResponse::xml(
            StatusCode::OK,
            query_response_xml(
                "RestoreDBInstanceFromDBSnapshot",
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
}
