//! RDS `snapshots` family extracted from service.rs by audit-2026-05-19.

use super::*;

use crate::filters::{
    addresses_own_account, identifier_account, identifier_matches_type, normalized_identifier,
    optional_flag, parse_filters, sibling_rds_arn, RdsFilter,
};

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
        // "Return all DB snapshots that have been marked as public" --
        // not scoped to other accounts, so an owned public snapshot
        // counts. `shared` is "shared TO my account", which an owned
        // snapshot never is.
        Some("public") => snapshot_is_public(snapshot),
        Some("shared") => false,
        Some(wanted) => snapshot.snapshot_type == wanted,
        None => true,
    }
}

/// The `snapshot-type` values a snapshot answers to for `caller`: its
/// stored type, plus `public` / `shared` when it is shared that way.
/// Keeps the `snapshot-type` FILTER speaking the same vocabulary as the
/// `SnapshotType` PARAMETER -- otherwise a snapshot reported by
/// `--snapshot-type public` would be missed by
/// `--filters Name=snapshot-type,Values=public`.
fn snapshot_type_labels(snapshot: &DbSnapshot, caller: &str, owned: bool) -> Vec<String> {
    let mut labels = vec![snapshot.snapshot_type.clone()];
    if snapshot_is_public(snapshot) {
        labels.push("public".to_string());
    }
    if !owned && snapshot_shared_with(snapshot, caller) {
        labels.push("shared".to_string());
    }
    labels
}

/// True when `snapshot` satisfies every filter. Filters are AND-ed with
/// each other; the values within one filter are OR-ed. The names come
/// from the `DescribeDBSnapshots` docs: `db-instance-id`,
/// `db-snapshot-id`, `dbi-resource-id`, `engine` and `snapshot-type`.
fn snapshot_matches_filters(
    snapshot: &DbSnapshot,
    filters: &[RdsFilter],
    caller: &str,
    owned: bool,
) -> bool {
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
        "snapshot-type" => snapshot_type_labels(snapshot, caller, owned)
            .iter()
            .any(|label| filter.matches(Some(label.as_str()))),
        // A filter name AWS doesn't document for this operation
        // matches nothing — see the module docs on `crate::filters`.
        other => {
            tracing::debug!(filter = %other, "unrecognized RDS filter name; matching no resource");
            false
        }
    })
}

/// The container engine an Aurora family maps onto. Aurora clusters run
/// their data on a normal engine (that is why fakecloud attaches an
/// `engine=postgres` writer to an `aurora-postgresql` cluster), and the
/// runtime only knows the concrete engines.
fn container_engine_for(engine: &str) -> &str {
    match engine {
        "aurora-mysql" | "aurora" => "mysql",
        "aurora-postgresql" => "postgres",
        other => other,
    }
}

/// Build the source-snapshot record a restore needs from a stored DB
/// cluster snapshot. AWS lets RestoreDBInstanceFromDBSnapshot take a
/// Multi-AZ DB cluster snapshot, whose metadata lives in the cluster
/// snapshot store rather than in `snapshots`.
pub(super) fn cluster_snapshot_as_source(
    entry: &serde_json::Value,
    snapshot_id: &str,
    account_id: &str,
    region: &str,
) -> DbSnapshot {
    let field = |key: &str| entry.get(key).and_then(|v| v.as_str()).map(str::to_string);
    // The snapshot's own ARN names its OWNER, which is not the caller for
    // a snapshot shared with them.
    let snapshot_arn = field("DBClusterSnapshotArn").unwrap_or_else(|| {
        format!("arn:aws:rds:{region}:{account_id}:cluster-snapshot:{snapshot_id}")
    });
    // The captured database, base64 in the entry, exactly as
    // RestoreDBClusterFromSnapshot replays it -- without this the restore
    // reports `available` with an empty database.
    let dump_data = entry
        .get("DumpDataB64")
        // A cluster restored from a snapshot stages its data under
        // `PendingRestoreDumpB64` until an instance attaches, and a
        // snapshot taken in that window clones the key verbatim.
        // RestoreDBClusterFromSnapshot carries it forward, so an instance
        // restore has to read it too or the two paths disagree about the
        // same snapshot.
        .or_else(|| entry.get("PendingRestoreDumpB64"))
        .and_then(|v| v.as_str())
        .and_then(|b64| {
            use base64::Engine;
            base64::engine::general_purpose::STANDARD.decode(b64).ok()
        })
        .unwrap_or_default();
    // Prefer the writer's engine, recorded when the dump was taken: the
    // cluster's own `aurora-*` family is not a container engine, and
    // handing it to the runtime fails the instance with "Unsupported
    // engine". Fall back to mapping the family onto the engine its
    // writer would have run.
    let engine = field("SourceEngine")
        .or_else(|| field("Engine").map(|engine| container_engine_for(&engine).to_string()))
        .unwrap_or_else(|| {
            // The container really is started with this, so a silent
            // substitution leaves the caller with an instance that is
            // `available` but not what they snapshotted.
            tracing::warn!(
                snapshot = %snapshot_id,
                "cluster snapshot records no engine; restoring as postgres"
            );
            "postgres".to_string()
        });
    // Paired with the engine above: the cluster's own Aurora version
    // against a remapped container engine is a combination AWS never
    // reports. Keyed on whether the engine WAS remapped -- not on
    // whether SourceEngine happened to be recorded -- so a snapshot
    // taken before any writer attached doesn't pair `mysql` with
    // `8.0.mysql_aurora.3.04.0`.
    let engine_was_remapped = field("SourceEngine").is_none()
        && field("Engine").is_some_and(|family| container_engine_for(&family) != family);
    let engine_version = field("SourceEngineVersion")
        .or_else(|| field("EngineVersion").filter(|_| !engine_was_remapped))
        .unwrap_or_else(|| service_helpers::default_engine_version(&engine).to_string());
    // An out-of-range value is ignored rather than truncated: `as i32`
    // would wrap a bogus stored port to something like 0.
    let port = entry
        .get("Port")
        .and_then(|v| v.as_i64())
        .and_then(|p| i32::try_from(p).ok())
        .unwrap_or_else(|| service_helpers::default_port_for_engine(&engine));
    DbSnapshot {
        db_snapshot_identifier: snapshot_id.to_string(),
        db_snapshot_arn: snapshot_arn,
        db_instance_identifier: field("DBClusterIdentifier").unwrap_or_default(),
        snapshot_create_time: Utc::now(),
        engine,
        engine_version,
        allocated_storage: entry
            .get("AllocatedStorage")
            .and_then(|v| v.as_i64())
            .and_then(|v| i32::try_from(v).ok())
            .unwrap_or(20),
        status: "available".to_string(),
        port,
        // Credentials and database come from the writer too: the dump was
        // taken with them, so replaying it under the cluster row's values
        // would restore into the wrong database or refuse to connect.
        master_username: field("SourceMasterUsername")
            .or_else(|| field("MasterUsername"))
            .unwrap_or_else(|| "admin".to_string()),
        db_name: field("SourceDBName").or_else(|| field("DatabaseName")),
        dbi_resource_id: field("DbClusterResourceId").unwrap_or_default(),
        snapshot_type: field("SnapshotType").unwrap_or_else(|| "manual".to_string()),
        // The engines refuse to start with an empty password; a cluster
        // created before the password was persisted falls back to the
        // same default CreateDBCluster records now.
        // Each candidate is emptiness-checked BEFORE the fallback, so a
        // stored empty SourceMasterUserPassword falls through to
        // MasterUserPassword instead of skipping straight to the default.
        master_user_password: field("SourceMasterUserPassword")
            .filter(|password| !password.is_empty())
            .or_else(|| field("MasterUserPassword").filter(|p| !p.is_empty()))
            .unwrap_or_else(|| {
                // The engines refuse to start with an empty password, so
                // a cluster recorded before the password was persisted
                // gets the default -- and the caller's real credentials
                // will not connect, which is worth saying out loud.
                tracing::warn!(
                    snapshot = %snapshot_id,
                    "cluster snapshot records no master password; \
                     restoring with the default -- the original credentials will not connect"
                );
                crate::extras::DEFAULT_CLUSTER_MASTER_PASSWORD.to_string()
            }),
        tags: Vec::new(),
        dump_data,
        availability_zone: None,
        vpc_id: None,
        instance_create_time: None,
        license_model: None,
        iops: None,
        option_group_name: None,
        percent_progress: Some(100),
        storage_type: None,
        // The cluster row carries these, and every other field here is
        // read off the entry: reporting the restored instance as
        // unencrypted contradicts the snapshot's own StorageEncrypted and
        // gives Terraform a permanent storage_encrypted / kms_key_id diff.
        encrypted: entry
            .get("StorageEncrypted")
            .and_then(|v| v.as_bool())
            .unwrap_or(false),
        kms_key_id: field("KmsKeyId"),
        iam_database_authentication_enabled: false,
        timezone: None,
        storage_throughput: None,
        snapshot_attributes: std::collections::BTreeMap::new(),
    }
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
                    StatusCode::BAD_REQUEST,
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
                    StatusCode::BAD_REQUEST,
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
                    StatusCode::BAD_REQUEST,
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
        let raw_snapshot_identifier = optional_query_param(request, "DBSnapshotIdentifier");
        // An ARN of another resource type is not a DB snapshot: report
        // not-found rather than letting a `None` identifier read as "no
        // filter" and list everything.
        if let Some(raw) = raw_snapshot_identifier.as_deref() {
            if !identifier_matches_type(raw, "snapshot") {
                return Err(db_snapshot_not_found(raw));
            }
        }
        let db_snapshot_identifier =
            normalized_identifier(raw_snapshot_identifier.clone(), "snapshot");
        // A DB instance is never shared across accounts, so an ARN
        // naming a different account matches none of THIS account's
        // snapshots -- but the owner may well have shared snapshots of
        // that instance, so it must not blank out the shared/public
        // widening below. A wrong-type ARN names nothing at all.
        let raw_instance_identifier = optional_query_param(request, "DBInstanceIdentifier");
        let instance_owner = raw_instance_identifier
            .as_deref()
            .and_then(identifier_account);
        let instance_wrong_type = raw_instance_identifier
            .as_deref()
            .is_some_and(|raw| !identifier_matches_type(raw, "db"));
        let foreign_instance_owner = instance_owner
            .as_deref()
            .is_some_and(|account| account != request.account_id);
        let db_instance_identifier = normalized_identifier(raw_instance_identifier.clone(), "db");
        // Modeled narrowing parameter, and the same failure this branch
        // exists to fix: ignoring it returns every snapshot to a client
        // that asked for one instance's.
        let dbi_resource_id = optional_query_param(request, "DbiResourceId");
        let snapshot_type = optional_query_param(request, "SnapshotType");
        // A junk boolean is treated as absent rather than rejected:
        // `InvalidParameterValue` isn't declared on this operation (see
        // the module docs on `crate::filters`).
        let include_shared =
            optional_flag(optional_query_param(request, "IncludeShared").as_deref())
                .unwrap_or(false);
        let include_public =
            optional_flag(optional_query_param(request, "IncludePublic").as_deref())
                .unwrap_or(false);
        let marker = optional_query_param(request, "Marker");
        let max_records = optional_query_param(request, "MaxRecords");
        let filters = parse_filters(request);

        // Specifying both DBSnapshotIdentifier and DBInstanceIdentifier
        // is tolerated: they are AND-ed, like SnapshotType and Filters.
        // Real AWS rejects the combo with `InvalidParameterCombination`,
        // but that code isn't declared on DescribeDBSnapshots (it isn't
        // a shape in the RDS model at all), so narrowing is the closest
        // in-shape behaviour.

        let accounts = self.state.read();
        let empty = RdsState::new(&request.account_id, &request.region);
        let state = accounts.get(&request.account_id).unwrap_or(&empty);

        // If specific snapshot requested, return just that one (no pagination)
        if let Some(snapshot_id) = db_snapshot_identifier {
            let named_owner = raw_snapshot_identifier
                .as_deref()
                .and_then(identifier_account);
            let owned_snapshot = named_owner
                .as_deref()
                .is_none_or(|account| account == request.account_id)
                .then(|| state.snapshots.get(&snapshot_id).cloned())
                .flatten();
            // Ownership comes from where the row actually resolved.
            // Re-probing `state.snapshots` by bare id would call a
            // foreign row "owned" whenever the caller happens to hold a
            // snapshot with the same identifier.
            let owned = owned_snapshot.is_some();
            let snapshot = owned_snapshot
                // A snapshot another account shared with this caller is
                // listed by SnapshotType=shared / IncludeShared, so
                // re-reading it by id or ARN has to resolve too --
                // otherwise the emulator 404s a row it just reported.
                .or_else(|| {
                    // AWS requires the ARN to address a snapshot another
                    // account shared with you -- and without one, a bare
                    // id could match several accounts' snapshots, so the
                    // scan would return an arbitrary (HashMap-ordered)
                    // row.
                    let named_account = named_owner.clone()?;
                    accounts
                        .iter()
                        .filter(|(owner, _)| *owner != request.account_id)
                        .filter(|(owner, _)| *owner == named_account)
                        .find_map(|(_, other)| {
                            other.snapshots.get(&snapshot_id).filter(|snapshot| {
                                snapshot_shared_with(snapshot, &request.account_id)
                                    || snapshot_is_public(snapshot)
                            })
                        })
                        .cloned()
                })
                // Echo what the caller passed (the ARN, when they used
                // one), not the id it reduced to.
                .ok_or_else(|| {
                    db_snapshot_not_found(
                        raw_snapshot_identifier.as_deref().unwrap_or(&snapshot_id),
                    )
                })?;

            // AWS AND-s the identifier with SnapshotType and any
            // filters: the snapshot exists, so a non-match is an empty
            // result rather than `DBSnapshotNotFound`.
            // A foreign snapshot answers to `shared` / `public` (or to
            // IncludeShared / IncludePublic on an unqualified read) rather
            // than to its own stored type -- the same rule the list path
            // applies, so a row the list reported can be re-read by id.
            let shared_with_caller = snapshot_shared_with(&snapshot, &request.account_id);
            let public = snapshot_is_public(&snapshot);
            let type_ok = if owned {
                snapshot_matches_type(&snapshot, snapshot_type.as_deref())
            } else {
                match snapshot_type.as_deref() {
                    Some("shared") => shared_with_caller,
                    Some("public") => public,
                    // The caller named this snapshot explicitly, so AWS
                    // resolves it without needing IncludeShared as well.
                    None => shared_with_caller || public,
                    Some(_) => false,
                }
            };
            // AWS AND-s every narrowing parameter with the identifier,
            // so a non-matching DBInstanceIdentifier excludes the named
            // snapshot rather than being dropped on this path.
            // Same rule the cross-account scan applies: the instance
            // ARN's account has to match the account the ROW came from,
            // not simply differ from the caller's -- otherwise the named
            // form of a query the list form answers returns nothing.
            let row_owner = if owned {
                request.account_id.as_str()
            } else {
                snapshot
                    .db_snapshot_arn
                    .split(':')
                    .nth(4)
                    .unwrap_or(request.account_id.as_str())
            };
            let resource_ok = dbi_resource_id
                .as_deref()
                .is_none_or(|wanted| snapshot.dbi_resource_id == wanted);
            let instance_ok = !instance_wrong_type
                && instance_owner
                    .as_deref()
                    .is_none_or(|account| account == row_owner)
                && db_instance_identifier
                    .as_deref()
                    .is_none_or(|instance_id| snapshot.db_instance_identifier == instance_id);
            let body = if type_ok
                && resource_ok
                && instance_ok
                && snapshot_matches_filters(&snapshot, &filters, &request.account_id, owned)
            {
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

        if instance_wrong_type {
            return Ok(AwsResponse::xml(
                StatusCode::OK,
                query_response_xml(
                    "DescribeDBSnapshots",
                    RDS_NS,
                    "<DBSnapshots></DBSnapshots>",
                    &request.request_id,
                ),
            ));
        }

        // Get snapshots, narrowed by instance identifier, SnapshotType
        // and Filters — all AND-ed together, as on real AWS.
        let mut snapshots: Vec<DbSnapshot> = state
            .snapshots
            .values()
            .filter(|snapshot| {
                // An instance ARN naming another account matches none of
                // this account's snapshots.
                !foreign_instance_owner
                    && dbi_resource_id
                        .as_deref()
                        .is_none_or(|wanted| snapshot.dbi_resource_id == wanted)
                    && db_instance_identifier
                        .as_deref()
                        .is_none_or(|instance_id| snapshot.db_instance_identifier == instance_id)
                    && snapshot_matches_type(snapshot, snapshot_type.as_deref())
                    && snapshot_matches_filters(snapshot, &filters, &request.account_id, true)
            })
            .cloned()
            .collect();

        // `shared` / `public` select snapshots OTHER accounts have shared
        // with this caller (or with everyone) via
        // ModifyDBSnapshotAttribute's `restore` attribute -- AWS reports
        // them here, and IncludeShared / IncludePublic add them to an
        // otherwise-unqualified listing.
        // AWS: IncludeShared / IncludePublic apply only to an
        // unqualified listing -- neither applies when SnapshotType picks
        // an owned type (`manual` / `automated` / `awsbackup`), and
        // IncludePublic doesn't apply to `shared` (nor IncludeShared to
        // `public`).
        let want_shared = snapshot_type.as_deref() == Some("shared")
            || (include_shared && snapshot_type.is_none());
        let want_public = snapshot_type.as_deref() == Some("public")
            || (include_public && snapshot_type.is_none());
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
                            // The instance ARN's owner is the account
                            // that shared these, so honour it here.
                            dbi_resource_id
                                .as_deref()
                                .is_none_or(|wanted| snapshot.dbi_resource_id == wanted)
                                && instance_owner
                                    .as_deref()
                                    .is_none_or(|account| account == owner)
                                && db_instance_identifier.as_deref().is_none_or(|instance_id| {
                                    snapshot.db_instance_identifier == instance_id
                                })
                                && snapshot_matches_filters(
                                    snapshot,
                                    &filters,
                                    &request.account_id,
                                    false,
                                )
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
        // NotFound) while leaving the snapshot in place. An ARN naming
        // ANOTHER account is not deletable here -- resolving it by bare
        // id would delete this account's same-named snapshot instead.
        let raw_identifier = required_query_param(request, "DBSnapshotIdentifier")?;
        if !addresses_own_account(&raw_identifier, &request.account_id) {
            return Err(db_snapshot_not_found(&raw_identifier));
        }
        let db_snapshot_identifier =
            normalized_identifier(Some(raw_identifier.clone()), "snapshot")
                .ok_or_else(|| db_snapshot_not_found(&raw_identifier))?;

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&request.account_id);

        let snapshot = state
            .snapshots
            .remove(&db_snapshot_identifier)
            // Echo what the caller passed (the ARN, when they used one),
            // as the describe and restore paths do.
            .ok_or_else(|| db_snapshot_not_found(&raw_identifier))?;
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
        // The cluster-snapshot parameter is accepted as an alias, so an
        // ARN of either type resolves; the caller's own identifier is
        // echoed in the error rather than a bare "(none)".
        let (raw_snapshot_identifier, snapshot_arn_type, from_cluster_snapshot) =
            match optional_query_param(request, "DBSnapshotIdentifier") {
                Some(raw) => (Some(raw), "snapshot", false),
                None => (
                    optional_query_param(request, "DBClusterSnapshotIdentifier"),
                    "cluster-snapshot",
                    true,
                ),
            };
        let snapshot_owner = raw_snapshot_identifier
            .as_deref()
            .and_then(identifier_account);
        let reported_identifier = raw_snapshot_identifier
            .clone()
            .unwrap_or_else(|| "(none)".to_string());
        let db_snapshot_identifier =
            normalized_identifier(raw_snapshot_identifier, snapshot_arn_type)
                .ok_or_else(|| db_snapshot_not_found(&reported_identifier))?;
        let vpc_security_group_ids = parse_vpc_security_group_ids(request);
        let tags = parse_tags(request)?;
        let db_subnet_group_name = optional_query_param(request, "DBSubnetGroupName");

        let (snapshot, dbi_resource_id, db_instance_arn, created_at) = {
            let mut accounts = self.state.write();
            let state = accounts.get_or_create(&request.account_id);

            if !state.begin_instance_creation(&db_instance_identifier) {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "DBInstanceAlreadyExists",
                    format!("DBInstance {db_instance_identifier} already exists."),
                ));
            }

            // The ARN names its owner: a foreign one must not hydrate the
            // new instance from this account's same-named snapshot.
            // A Multi-AZ DB cluster snapshot lives in the cluster
            // snapshot store, not `snapshots`. The model declares the
            // parameter and its own not-found fault, so resolve it there
            // and synthesize the source fields the restore needs.
            if from_cluster_snapshot {
                let entry = crate::extras::find_cluster_snapshot(
                    &accounts,
                    &request.account_id,
                    snapshot_owner.as_deref(),
                    &db_snapshot_identifier,
                );
                let Some(entry) = entry else {
                    let state = accounts.get_or_create(&request.account_id);
                    state.cancel_instance_creation(&db_instance_identifier);
                    return Err(AwsServiceError::aws_error(
                        StatusCode::NOT_FOUND,
                        "DBClusterSnapshotNotFoundFault",
                        format!("DBClusterSnapshot {reported_identifier} not found."),
                    ));
                };
                let state = accounts.get_or_create(&request.account_id);
                // Same subnet-group validation every other create/restore
                // path applies; the name is stamped onto the instance
                // below either way.
                validate_subnet_group_or_cancel(
                    state,
                    &db_instance_identifier,
                    db_subnet_group_name.as_deref(),
                )?;
                let dbi_resource_id = state.next_dbi_resource_id();
                let db_instance_arn =
                    state.db_instance_arn(&request.region, &db_instance_identifier);
                let snapshot = cluster_snapshot_as_source(
                    &entry,
                    &db_snapshot_identifier,
                    &request.account_id,
                    &request.region,
                );
                (snapshot, dbi_resource_id, db_instance_arn, Utc::now())
            } else {
                let owned = snapshot_owner
                    .as_deref()
                    .is_none_or(|account| account == request.account_id)
                    .then(|| state.snapshots.get(&db_snapshot_identifier).cloned())
                    .flatten();
                let snapshot = match owned {
                    Some(s) => s,
                    None => {
                        // DescribeDBSnapshots reports snapshots other accounts
                        // shared with this caller, so restoring from one has
                        // to work as well -- otherwise the sharing surface is
                        // listable but unusable.
                        // AWS requires the ARN of a shared snapshot to
                        // restore from it; a bare id would also make the scan
                        // pick an arbitrary account when several shared one
                        // under the same name.
                        let shared = accounts.iter().find_map(|(owner, other)| {
                            if owner == request.account_id {
                                return None;
                            }
                            if snapshot_owner.as_deref() != Some(owner) {
                                return None;
                            }
                            other
                                .snapshots
                                .get(&db_snapshot_identifier)
                                .filter(|snapshot| {
                                    snapshot_shared_with(snapshot, &request.account_id)
                                        || snapshot_is_public(snapshot)
                                })
                                .cloned()
                        });
                        match shared {
                            Some(s) => s,
                            None => {
                                let state = accounts.get_or_create(&request.account_id);
                                state.cancel_instance_creation(&db_instance_identifier);
                                // The caller's own identifier, not the id it
                                // reduced to.
                                return Err(db_snapshot_not_found(&reported_identifier));
                            }
                        }
                    }
                };
                let state = accounts.get_or_create(&request.account_id);

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
            }
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
        // Active Directory membership is settable on the restore
        // request, as it is on create: dropping it leaves the restored
        // instance reporting no DomainMembership and invisible to the
        // `domain` filter. (The PITR and read-replica paths already
        // carry the source's.)
        instance.domain = optional_query_param(request, "Domain");
        instance.domain_fqdn = optional_query_param(request, "DomainFqdn");
        instance.domain_ou = optional_query_param(request, "DomainOu");
        instance.domain_iam_role_name = optional_query_param(request, "DomainIAMRoleName");
        instance.domain_auth_secret_arn = optional_query_param(request, "DomainAuthSecretArn");
        instance.domain_dns_ips = parse_string_member_list(request, "DomainDnsIps");
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
