//! RDS `cluster_snapshots` family extracted from service.rs by audit-2026-05-19.

use super::*;

use crate::filters::{identifier_account, normalized_identifier};

impl RdsService {
    /// Real CreateDBClusterSnapshot: locates the cluster's writer
    /// member, dumps its database synchronously via the runtime, and
    /// stores the dump alongside the snapshot's metadata so a later
    /// RestoreDBClusterFromSnapshot can replay the exact state.
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
    pub(super) async fn restore_db_cluster_from_snapshot(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        use serde_json::json;
        let target = required_query_param(request, "DBClusterIdentifier")?;
        // The Terraform provider stores a full snapshot ARN in
        // `snapshot_identifier`, and DescribeDBClusterSnapshots /
        // CopyDBClusterSnapshot both resolve that form, so this lookup
        // has to as well.
        let raw_snapshot_id = optional_query_param(request, "SnapshotIdentifier")
            .or_else(|| optional_query_param(request, "DBClusterSnapshotIdentifier"));
        let snapshot_owner = raw_snapshot_id.as_deref().and_then(identifier_account);
        let snapshot_id =
            normalized_identifier(raw_snapshot_id, "cluster-snapshot").ok_or_else(|| {
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
        // Resolved before the mutable borrow the cluster insert needs.
        // A snapshot another account shared with this caller is listable,
        // so it has to be restorable too -- and an ARN names its owner,
        // so it can't resolve against a different account's identical id.
        let snapshot = crate::extras::find_cluster_snapshot(
            &accounts,
            &request.account_id,
            snapshot_owner.as_deref(),
            &snapshot_id,
        )
        .ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "DBClusterSnapshotNotFoundFault",
                format!("DBClusterSnapshot {snapshot_id} not found."),
            )
        })?;
        let state = accounts.get_or_create(&request.account_id);
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
            // Snapshot-only bookkeeping has no meaning on a cluster row,
            // and CreateDBClusterSnapshot copies the whole row into the
            // next snapshot, so leaving it would propagate forward.
            obj.remove("SnapshotType");
            obj.remove("SnapshotCreateTime");
            obj.remove("PercentProgress");
            // Sharing must not propagate forward either: the restored
            // cluster is snapshotted whole by CreateDBClusterSnapshot.
            obj.remove("SnapshotAttributes");
            // The snapshot carries the source cluster's identity fields
            // (CreateDBClusterSnapshot copies the whole cluster JSON).
            // A restore is an independent full copy: it gets its own
            // resource id and belongs to no clone group. Inheriting
            // either makes `db-cluster-resource-id` / `clone-group-id`
            // return two clusters for what AWS scopes to one.
            obj.insert(
                "DbClusterResourceId".to_string(),
                json!(crate::extras::new_cluster_resource_id()),
            );
            obj.remove("CloneGroupId");
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
        let restore_type = optional_query_param(request, "RestoreType");
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

        // A `copy-on-write` restore clones the source: both clusters join
        // one clone group, so stamp the source with a group id if it
        // isn't already in one.
        let clone_group_id = if restore_type.as_deref() == Some("copy-on-write") {
            let existing = entry
                .get("CloneGroupId")
                .and_then(|v| v.as_str())
                .map(str::to_string);
            // AWS reports CloneGroupId as a hyphenated UUID (the bare-hex
            // `.simple()` form this crate uses elsewhere is always
            // prefixed, e.g. `db-`/`cluster-`).
            let group_id = existing.unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
            if let Some(source_entry) = state
                .extras
                .get_mut("clusters")
                .and_then(|m| m.get_mut(&source))
                .and_then(|v| v.as_object_mut())
            {
                source_entry.insert("CloneGroupId".to_string(), json!(group_id));
            }
            group_id
        } else {
            String::new()
        };
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
            // The restored cluster is a new resource: the immutable
            // resource id must not be inherited from the source, or
            // `db-cluster-resource-id` (a unique match on AWS) selects
            // both clusters.
            obj.insert(
                "DbClusterResourceId".to_string(),
                json!(crate::extras::new_cluster_resource_id()),
            );
            // Only a `copy-on-write` restore is a clone: AWS puts the
            // clone and its source in the same clone group, which is
            // what `DescribeDBClusters --filters Name=clone-group-id`
            // selects on. A full-copy restore is an independent cluster,
            // so it must not inherit the source's group.
            match restore_type.as_deref() {
                Some("copy-on-write") => {
                    obj.insert("CloneGroupId".to_string(), json!(clone_group_id));
                }
                _ => {
                    obj.remove("CloneGroupId");
                }
            }
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
