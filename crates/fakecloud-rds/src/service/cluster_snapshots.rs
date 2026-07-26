//! RDS `cluster_snapshots` family extracted from service.rs by audit-2026-05-19.

use super::*;

impl RdsService {
    /// Real CreateDBClusterSnapshot: locates the cluster's writer member,
    /// records the snapshot as `available` immediately, then dumps the
    /// writer's database synchronously (bounded by a hard timeout so an
    /// unbounded mysqldump/pg_dump can never hang the request forever) and
    /// stages the base64 dump on that same entry so a later
    /// RestoreDBClusterFromSnapshot can replay the exact state.
    ///
    /// The dump is kept ON the request path deliberately: the e2e drops the
    /// source writer the instant the snapshot reports `available`, so the dump
    /// MUST be staged before this returns — any backgrounding races that
    /// teardown and loses the data. Two backgrounding attempts (commits
    /// `f5cc5ddc2` and this branch's earlier finalizer) also wedged the
    /// snapshot in `creating` because the detached status flip never became
    /// visible to DescribeDBClusterSnapshots. Recording `available` up front and
    /// dumping inline eliminates both the stuck-`creating` state and the restore
    /// race; the 120s cap keeps the handler from ever blocking indefinitely.
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

        // Record the snapshot entry `available` up front — no `creating`
        // branch. DescribeDBClusterSnapshots sees `available` immediately and
        // the inline dump below only *adds* `DumpDataB64`, never touching
        // `Status`, so the snapshot can never wedge mid-flight.
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
                obj.insert("Status".to_string(), json!("available"));
                obj.insert("SnapshotType".to_string(), json!("manual"));
                // No dump inserted yet — the inline dump below stages
                // `DumpDataB64` on success (or leaves it absent on a
                // metadata-only snapshot).
            }
            state
                .extras
                .entry("cluster_snapshots".to_string())
                .or_default()
                .insert(snapshot_id.clone(), entry);
        }

        // Dump the writer synchronously and stage it on the (already
        // `available`) snapshot. Bound the (unbounded) mysqldump/pg_dump with a
        // hard 120s cap so a stalled dump under CI runner congestion can't hang
        // the handler forever: timeout and dump-error both collapse to the
        // metadata-only path (no `DumpDataB64`), while success stages the base64
        // dump. Either way the snapshot stays `available`.
        if let (Some((wid, eng, user, pass, db)), Some(runtime)) =
            (writer_info, self.runtime_ref().cloned())
        {
            let dump = tokio::time::timeout(
                std::time::Duration::from_secs(120),
                runtime.dump_database(&wid, &eng, &user, &pass, &db),
            )
            .await;
            let result = cluster_snapshot_dump_result_from_timeout(&snapshot_id, dump);
            apply_cluster_snapshot_dump_result(
                &self.state,
                &request.account_id,
                &snapshot_id,
                result,
            );
            save_snapshot_static(
                self.state.clone(),
                self.snapshot_store.clone(),
                self.snapshot_lock.clone(),
            )
            .await;
        }

        self.emit_event(
            RdsSourceType::DbClusterSnapshot,
            &snapshot_id,
            &arn,
            "RDS-EVENT-0074",
            &["backup"],
            "DB cluster snapshot created",
        );

        // The snapshot is genuinely `available` on return (dump staged inline),
        // so report `available` — matching the pre-backgrounding synchronous
        // behaviour and what DescribeDBClusterSnapshots now shows.
        Ok(AwsResponse::xml(
            StatusCode::OK,
            query_response_xml(
                "CreateDBClusterSnapshot",
                RDS_NS,
                &crate::extras::cluster_snapshot_status_xml(
                    &snapshot_id,
                    &arn,
                    &cluster_id,
                    "available",
                ),
                &request.request_id,
            ),
        ))
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

/// Stage the outcome of a cluster-snapshot dump onto the stored
/// `extras["cluster_snapshots"]` JSON entry. Split out from
/// `create_db_cluster_snapshot` so the dump-staging is unit-testable without a
/// container runtime.
///
/// The snapshot entry is recorded `available` up front and this NEVER touches
/// `Status`: on success it only inserts the base64 STANDARD `DumpDataB64`; on
/// dump failure/timeout it does nothing, leaving a metadata-only `available`
/// snapshot. A snapshot deleted while the dump was in flight is left untouched
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
            obj.insert("DumpDataB64".to_string(), json!(b64));
        }
        Err(error) => {
            tracing::warn!(
                %error,
                snapshot = %snapshot_id,
                "cluster snapshot dump failed; leaving metadata-only available snapshot"
            );
        }
    }
}

/// Collapse a bounded-dump outcome into the `Result` the finalizer applies.
///
/// The dump future is wrapped in `tokio::time::timeout`, yielding
/// `Result<Result<Vec<u8>, RuntimeError>, Elapsed>`:
///   - `Ok(inner)` passes the dump's own success/error straight through, so the
///     caller stages the dump on success and settles metadata-only on error.
///   - `Err(_elapsed)` (the dump stalled past the cap) maps to the metadata-only
///     path — same terminal state as a dump error — after logging a distinct
///     timeout warning. This is what keeps the handler from ever blocking
///     indefinitely: every arm feeds `apply_cluster_snapshot_dump_result` a
///     `Result`, and the snapshot (already `available`) simply ends up with or
///     without `DumpDataB64`.
pub(super) fn cluster_snapshot_dump_result_from_timeout(
    snapshot_id: &str,
    dump: Result<Result<Vec<u8>, RuntimeError>, tokio::time::error::Elapsed>,
) -> Result<Vec<u8>, RuntimeError> {
    match dump {
        Ok(inner) => inner,
        Err(_elapsed) => {
            tracing::warn!(
                snapshot = %snapshot_id,
                "cluster snapshot dump timed out; available metadata-only"
            );
            Err(RuntimeError::Unavailable)
        }
    }
}
