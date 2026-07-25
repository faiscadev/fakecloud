//! RDS `cluster_snapshots` family extracted from service.rs by audit-2026-05-19.

use super::*;

impl RdsService {
    /// Real CreateDBClusterSnapshot: locates the cluster's writer member,
    /// records the snapshot synchronously as `creating`, and backgrounds the
    /// writer's database dump so the (unbounded mysqldump/pg_dump) never blocks
    /// the request handler past the ~60s client read timeout (bug-hunt
    /// 2026-07-25 Tier-3, #2398 sibling). The detached finalizer flips the
    /// stored `cluster_snapshots` JSON entry to `available` and inserts the
    /// base64 dump so a later RestoreDBClusterFromSnapshot can replay the exact
    /// state.
    ///
    /// This redoes a previously-reverted backgrounding (commit `f5cc5ddc2`)
    /// with a *correct* finalizer: cluster snapshots live as JSON in
    /// `state.extras["cluster_snapshots"]`, so the finalizer updates that entry
    /// directly rather than the typed `state.snapshots` store the
    /// single-instance path uses.
    pub(super) async fn create_db_cluster_snapshot(
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

        // Will we background a live dump? Only when we found a writer AND a
        // container runtime is wired. Otherwise there's nothing to dump, so the
        // snapshot is metadata-only and lands as `available` synchronously.
        let will_dump = writer_info.is_some() && self.runtime_ref().is_some();
        let initial_status = if will_dump { "creating" } else { "available" };

        {
            let mut accounts = self.state.write();
            let state = accounts.get_or_create(&request.account_id);
            let mut entry = state
                .extras
                .get("clusters")
                .and_then(|m| m.get(&cluster_id))
                .cloned()
                .ok_or_else(|| {
                    AwsServiceError::aws_error(
                        StatusCode::NOT_FOUND,
                        "DBClusterNotFoundFault",
                        format!("DBCluster {cluster_id} not found."),
                    )
                })?;
            if let Some(obj) = entry.as_object_mut() {
                obj.insert(
                    "DBClusterSnapshotIdentifier".to_string(),
                    json!(snapshot_id),
                );
                obj.insert("DBClusterSnapshotArn".to_string(), json!(arn));
                obj.insert("DBClusterIdentifier".to_string(), json!(cluster_id));
                obj.insert("Status".to_string(), json!(initial_status));
                obj.insert("SnapshotType".to_string(), json!("manual"));
                // No dump inserted yet — the background finalizer stages
                // `DumpDataB64` on completion (or leaves it absent on a
                // metadata-only snapshot).
            }
            state
                .extras
                .entry("cluster_snapshots".to_string())
                .or_default()
                .insert(snapshot_id.clone(), entry);
        }

        // Background the slow writer dump so the request returns promptly. The
        // finalizer flips the stored `cluster_snapshots` entry to `available`
        // and stages the base64 dump; on dump failure it leaves the snapshot
        // metadata-only `available` (matching the previous inline fallback).
        if let (Some((wid, eng, user, pass, db)), Some(runtime)) =
            (writer_info, self.runtime_ref().cloned())
        {
            self.spawn_finalize_cluster_snapshot(
                runtime,
                request.account_id.clone(),
                snapshot_id.clone(),
                wid,
                eng,
                user,
                pass,
                db,
            );
        }

        self.emit_event(
            RdsSourceType::DbClusterSnapshot,
            &snapshot_id,
            &arn,
            "RDS-EVENT-0074",
            &["backup"],
            "DB cluster snapshot created",
        );

        // AWS returns the snapshot in `creating` from CreateDBClusterSnapshot;
        // the backgrounded dump flips it to `available` (visible via
        // DescribeDBClusterSnapshots) once complete.
        Ok(AwsResponse::xml(
            StatusCode::OK,
            query_response_xml(
                "CreateDBClusterSnapshot",
                RDS_NS,
                &crate::extras::cluster_snapshot_status_xml(
                    &snapshot_id,
                    &arn,
                    &cluster_id,
                    "creating",
                ),
                &request.request_id,
            ),
        ))
    }

    /// Background the slow writer dump for a cluster snapshot. The caller
    /// records the `cluster_snapshots` JSON entry synchronously with status
    /// `creating` and returns immediately; this task runs mysqldump/pg_dump
    /// (unbounded in dataset size, easily past the ~60s client read timeout)
    /// and only then updates that same extras-JSON entry — setting `Status` to
    /// `available` and inserting the base64 `DumpDataB64` on success, or
    /// leaving a metadata-only `available` snapshot on dump failure. Mirrors
    /// `spawn_finalize_snapshot` but writes the extras-JSON entry rather than
    /// the typed `state.snapshots` store (cluster snapshots don't live there).
    #[allow(clippy::too_many_arguments)]
    fn spawn_finalize_cluster_snapshot(
        &self,
        runtime: Arc<RdsRuntime>,
        account_id: String,
        snapshot_id: String,
        writer_id: String,
        engine: String,
        username: String,
        password: String,
        db_name: String,
    ) {
        let state_handle = self.state.clone();
        let snapshot_store = self.snapshot_store.clone();
        let snapshot_lock = self.snapshot_lock.clone();
        tokio::spawn(async move {
            let result = runtime
                .dump_database(&writer_id, &engine, &username, &password, &db_name)
                .await;
            apply_cluster_snapshot_dump_result(&state_handle, &account_id, &snapshot_id, result);
            save_snapshot_static(state_handle.clone(), snapshot_store, snapshot_lock).await;
        });
    }

    /// Real RestoreDBClusterFromSnapshot: clones the source cluster's
    /// metadata into the new cluster id, strips member tracking so the
    /// restored cluster starts empty, and stages the snapshot's dump
    /// data on the new cluster so the next CreateDBInstance with this
    /// cluster id replays it onto the fresh writer.
    pub(super) async fn restore_db_cluster_from_snapshot(
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
        let pending_dump_b64 = snapshot
            .get("DumpDataB64")
            .and_then(|v| v.as_str())
            .map(str::to_string);

        // Hydrate the restored cluster entry from the snapshot directly,
        // not from the current `clusters` map — the snapshot is the
        // point-in-time the caller wants to roll back to. CreateDBClusterSnapshot
        // copies the source cluster JSON into the snapshot, so this carries
        // engine/version/network/parameter-group/etc.
        let mut entry = snapshot.clone();
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
    pub(super) async fn restore_db_cluster_to_point_in_time(
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

        // The source writer's dump is unbounded (mysqldump/pg_dump) and easily
        // past the ~60s client read timeout, so it must NOT run inline in the
        // request handler (bug: PITR blocked the client). Record the restored
        // cluster placeholder synchronously below and return promptly; a
        // detached task (spawned after the response is built) live-dumps the
        // source writer and stages it as `PendingRestoreDumpB64` on the target
        // cluster, which the first attached CreateDBInstance replays. Mirrors
        // the instance-restore path's `spawn_finalize_restored_instance`.
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
        }
        state
            .extras
            .entry("clusters".to_string())
            .or_default()
            .insert(target.clone(), entry);
        drop(accounts);

        // Background the source-writer dump so the request returns promptly.
        // On success the dump is staged as `PendingRestoreDumpB64` on the
        // target cluster entry; on failure we fall back to a metadata-only
        // restore (matching the previous inline behaviour).
        if let (Some((wid, eng, user, pass, db)), Some(runtime)) =
            (writer_info, self.runtime_ref().cloned())
        {
            let state_handle = self.state.clone();
            let snapshot_store = self.snapshot_store.clone();
            let snapshot_lock = self.snapshot_lock.clone();
            let account_id = request.account_id.clone();
            let target = target.clone();
            let source = source.clone();
            tokio::spawn(async move {
                match runtime.dump_database(&wid, &eng, &user, &pass, &db).await {
                    Ok(data) => {
                        use base64::Engine;
                        let b64 = base64::engine::general_purpose::STANDARD.encode(&data);
                        {
                            let mut accounts = state_handle.write();
                            let state = accounts.get_or_create(&account_id);
                            if let Some(entry) = state
                                .extras
                                .get_mut("clusters")
                                .and_then(|m| m.get_mut(&target))
                                .and_then(|e| e.as_object_mut())
                            {
                                entry.insert("PendingRestoreDumpB64".to_string(), json!(b64));
                            }
                        }
                        save_snapshot_static(state_handle.clone(), snapshot_store, snapshot_lock)
                            .await;
                    }
                    Err(e) => {
                        tracing::warn!(
                            error = %e,
                            cluster = %source,
                            writer = %wid,
                            "cluster PIT dump failed; falling back to metadata-only restore"
                        );
                    }
                }
            });
        }

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
}

/// Apply the outcome of a backgrounded cluster-snapshot dump to the stored
/// `extras["cluster_snapshots"]` JSON entry. Split out from
/// `spawn_finalize_cluster_snapshot` so the state transition (`creating` ->
/// `available`, with the base64 dump staged) is unit-testable without a
/// container runtime.
///
/// On success the entry's `Status` flips to `available` and `DumpDataB64`
/// carries the base64 STANDARD dump. On dump failure the entry is left as a
/// metadata-only `available` snapshot (no dump) — matching the previous inline
/// fallback. A snapshot deleted while the dump was in flight is left untouched
/// (the lookup misses).
pub(super) fn apply_cluster_snapshot_dump_result(
    state: &SharedRdsState,
    account_id: &str,
    snapshot_id: &str,
    result: Result<Vec<u8>, RuntimeError>,
) {
    use serde_json::json;
    let mut accounts = state.write();
    let s = accounts.get_or_create(account_id);
    let Some(obj) = s
        .extras
        .get_mut("cluster_snapshots")
        .and_then(|m| m.get_mut(snapshot_id))
        .and_then(|e| e.as_object_mut())
    else {
        return;
    };
    match result {
        Ok(data) => {
            use base64::Engine;
            let b64 = base64::engine::general_purpose::STANDARD.encode(&data);
            obj.insert("Status".to_string(), json!("available"));
            obj.insert("DumpDataB64".to_string(), json!(b64));
        }
        Err(error) => {
            tracing::warn!(
                %error,
                snapshot = %snapshot_id,
                "cluster snapshot dump failed; leaving metadata-only available snapshot"
            );
            obj.insert("Status".to_string(), json!("available"));
        }
    }
}
