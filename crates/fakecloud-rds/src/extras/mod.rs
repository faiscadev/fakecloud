//! RDS handlers added to close the conformance gap. Clusters, cluster
//! snapshots / parameter groups / endpoints, security groups, option
//! groups, event subscriptions, global clusters, integrations, blue/green
//! deployments, shard groups, custom engine versions, tenant databases,
//! proxies, export tasks, recommendations, certificates, accounts /
//! events / pending maintenance, and start/stop/reboot/failover ops.
//!
//! Persists into per-account state via the generic
//! `extras: HashMap<category, HashMap<id, Value>>` store on
//! `RdsState`. Returns valid Query-protocol XML responses with
//! stable IDs so SDK callers can chain operations.

use http::StatusCode;
use serde_json::{json, Value};
use std::collections::BTreeMap;

use fakecloud_aws::arn::Arn;
use fakecloud_aws::xml::xml_escape;
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use crate::service::{RdsService, RdsSourceType};

use crate::filters::{
    addresses_own_account, identifier_account, identifier_matches_type, normalized_identifier,
    optional_flag, parse_filters, sibling_rds_arn, RdsFilter,
};

const NS: &str = "http://rds.amazonaws.com/doc/2014-10-31/";

/// Password recorded for a cluster created without one. The engines
/// refuse to start with an empty password, so a restore from such a
/// cluster's snapshot needs a non-empty value to hand the container.
pub(crate) const DEFAULT_CLUSTER_MASTER_PASSWORD: &str = "fakecloud";

/// Read a string field off an extras entry, treating an absent or
/// non-string value as "the resource doesn't carry this attribute".
fn entry_str<'a>(entry: &'a Value, key: &str) -> Option<&'a str> {
    entry.get(key).and_then(|v| v.as_str())
}

/// True when a stored cluster satisfies every filter. Filters are AND-ed
/// with each other; the values within one filter are OR-ed. The names
/// come from the `DescribeDBClusters` docs: `clone-group-id`,
/// `db-cluster-id`, `db-cluster-resource-id`, `domain` and `engine`.
fn cluster_matches_filters(entry: &Value, filters: &[RdsFilter]) -> bool {
    filters.iter().all(|filter| match filter.name.as_str() {
        "db-cluster-id" => filter.matches_any([
            entry_str(entry, "DBClusterIdentifier"),
            entry_str(entry, "DBClusterArn"),
        ]),
        "db-cluster-resource-id" => filter.matches(entry_str(entry, "DbClusterResourceId")),
        "clone-group-id" => filter.matches(entry_str(entry, "CloneGroupId")),
        "domain" => filter.matches(entry_str(entry, "Domain")),
        "engine" => filter.matches(entry_str(entry, "Engine")),
        // A filter name AWS doesn't document for this operation
        // matches nothing — see the module docs on `crate::filters`.
        other => {
            tracing::debug!(filter = %other, "unrecognized RDS filter name; matching no resource");
            false
        }
    })
}

/// The `snapshot-type` values a cluster snapshot answers to for
/// `caller`: its stored type, plus `public` / `shared` when it is shared
/// that way. Keeps the filter speaking the same vocabulary as the
/// `SnapshotType` parameter.
fn cluster_snapshot_type_labels(entry: &Value, caller: &str, owned: bool) -> Vec<String> {
    // Defaulted to match the renderer, which reports a stored entry
    // carrying no SnapshotType as `manual`.
    let mut labels = vec![entry_str(entry, "SnapshotType")
        .unwrap_or("manual")
        .to_string()];
    let attrs = cluster_snapshot_attributes(entry);
    let targets = attrs.get("restore");
    if targets.is_some_and(|targets| targets.iter().any(|t| t == "all")) {
        labels.push("public".to_string());
    }
    if !owned && targets.is_some_and(|targets| targets.iter().any(|t| t == caller)) {
        labels.push("shared".to_string());
    }
    labels
}

/// True when a stored cluster snapshot satisfies every filter. The names
/// come from the `DescribeDBClusterSnapshots` docs: `db-cluster-id`,
/// `db-cluster-snapshot-id`, `engine` and `snapshot-type`.
fn cluster_snapshot_matches_filters(
    entry: &Value,
    filters: &[RdsFilter],
    caller: &str,
    owned: bool,
) -> bool {
    filters.iter().all(|filter| match filter.name.as_str() {
        // Accepts DB cluster identifiers and DB cluster ARNs; the
        // snapshot ARN supplies the partition/region/account needed to
        // rebuild the source cluster ARN.
        "db-cluster-id" => {
            let cluster = entry_str(entry, "DBClusterIdentifier");
            let cluster_arn = match (entry_str(entry, "DBClusterSnapshotArn"), cluster) {
                (Some(arn), Some(id)) => sibling_rds_arn(arn, "cluster", id),
                _ => None,
            };
            filter.matches_any([cluster, cluster_arn.as_deref()])
        }
        "db-cluster-snapshot-id" => filter.matches_any([
            entry_str(entry, "DBClusterSnapshotIdentifier"),
            entry_str(entry, "DBClusterSnapshotArn"),
        ]),
        "snapshot-type" => cluster_snapshot_type_labels(entry, caller, owned)
            .iter()
            .any(|label| filter.matches(Some(label.as_str()))),
        "engine" => filter.matches(entry_str(entry, "Engine")),
        // A filter name AWS doesn't document for this operation
        // matches nothing — see the module docs on `crate::filters`.
        other => {
            tracing::debug!(filter = %other, "unrecognized RDS filter name; matching no resource");
            false
        }
    })
}

fn rand_id() -> String {
    format!(
        "{:x}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
    )
}

pub(crate) fn xml_response(action: &str, inner: String, request_id: &str) -> AwsResponse {
    let body = format!(
        r#"<{action}Response xmlns="{NS}">
  <{action}Result>
{inner}
  </{action}Result>
  <ResponseMetadata>
    <RequestId>{rid}</RequestId>
  </ResponseMetadata>
</{action}Response>"#,
        action = action,
        NS = NS,
        inner = inner,
        rid = xml_escape(request_id),
    );
    AwsResponse::xml(StatusCode::OK, body)
}

fn xml_response_no_result(action: &str, request_id: &str) -> AwsResponse {
    let body = format!(
        r#"<{action}Response xmlns="{NS}">
  <ResponseMetadata>
    <RequestId>{rid}</RequestId>
  </ResponseMetadata>
</{action}Response>"#,
        action = action,
        NS = NS,
        rid = xml_escape(request_id),
    );
    AwsResponse::xml(StatusCode::OK, body)
}

fn store<'a>(
    extras: &'a mut BTreeMap<String, BTreeMap<String, Value>>,
    category: &str,
) -> &'a mut BTreeMap<String, Value> {
    extras.entry(category.to_string()).or_default()
}

fn get_param(req: &AwsRequest, key: &str) -> Option<String> {
    if let Some(v) = req.query_params.get(key) {
        return Some(v.clone());
    }
    let body_params = fakecloud_core::protocol::parse_query_body(&req.body);
    body_params.get(key).cloned()
}

fn missing(name: &str) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        "InvalidParameterValue",
        format!("{name} is required"),
    )
}

/// `ResourceNotFoundFault` — the generic "no such RDS resource" error
/// declared on `EnableHttpEndpoint` / `DisableHttpEndpoint` /
/// `ModifyActivityStream` (which take a `ResourceArn` rather than a typed
/// cluster/instance id, so the typed `DBClusterNotFoundFault` isn't in their
/// Smithy error set).
fn resource_not_found(arn: &str) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::NOT_FOUND,
        "ResourceNotFoundFault",
        format!("The specified resource ARN {arn} could not be found."),
    )
}

impl RdsService {
    pub(crate) fn handle_extra_action(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let action = req.action.clone();
        let aid = req.account_id.clone();
        let rid = req.request_id.clone();
        // ARNs and endpoint hosts carry the request's credential-scope region
        // (req.region), not a hardcoded default. Storage is keyed by
        // account/identifier, so this only affects returned strings.
        let region = req.region.as_str();

        macro_rules! write_state {
            () => {{
                let mut accounts = self.state_handle().write();
                accounts.get_or_create(&aid);
                accounts
            }};
        }

        match action.as_str() {
            // ── DB Clusters ──
            "CreateDBCluster" => {
                let id = get_param(req, "DBClusterIdentifier").ok_or_else(|| missing("DBClusterIdentifier"))?;
                let arn = Arn::new("rds", region, &aid, &format!("cluster:{id}")).to_string();
                let engine = get_param(req, "Engine").unwrap_or_else(|| "aurora-postgresql".to_string());
                let port = get_param(req, "Port")
                    .and_then(|p| p.parse::<i64>().ok())
                    .unwrap_or(if engine.contains("mysql") { 3306 } else { 5432 });
                let mut entry = json!({
                    "DBClusterIdentifier": id, "DBClusterArn": arn,
                    "DbClusterResourceId": new_cluster_resource_id(),
                    "Status": "available", "Engine": engine,
                    "EngineVersion": get_param(req, "EngineVersion").unwrap_or_else(|| "15.3".to_string()),
                    "Endpoint": format!("{id}.cluster-xxx.{region}.rds.amazonaws.com"),
                    "ReaderEndpoint": format!("{id}.cluster-ro-xxx.{region}.rds.amazonaws.com"),
                    "Port": port, "MasterUsername": get_param(req, "MasterUsername").unwrap_or_else(|| "postgres".to_string()),
                    // Persisted so a snapshot of this cluster carries
                    // usable credentials: RestoreDBInstanceFromDBSnapshot
                    // takes no password and has to start a container with
                    // the ones the snapshot captured.
                    "MasterUserPassword": get_param(req, "MasterUserPassword")
                        .unwrap_or_else(|| DEFAULT_CLUSTER_MASTER_PASSWORD.to_string()),
                });
                // Persist the remaining create-time input fields (safety flags
                // like DeletionProtection / StorageEncrypted, KmsKeyId,
                // BackupRetentionPeriod, DatabaseName, ...) that were otherwise
                // dropped until a follow-up ModifyDBCluster.
                if let Some(obj) = entry.as_object_mut() {
                    apply_create_cluster_params(obj, req);
                }
                {
                    let mut accounts = write_state!();
                    let state = accounts.get_or_create(&aid);
                    store(&mut state.extras, "clusters").insert(id.clone(), entry.clone());
                }
                self.emit_event(
                    RdsSourceType::DbCluster,
                    &id,
                    &arn,
                    "RDS-EVENT-0170",
                    &["creation"],
                    "DB cluster created",
                );
                Ok(xml_response(
                    "CreateDBCluster",
                    format!(
                        "    <DBCluster>\n{}\n    </DBCluster>",
                        db_cluster_member_xml(&entry)
                    ),
                    &rid,
                ))
            }
            "DeleteDBCluster" => {
                let id = get_param(req, "DBClusterIdentifier").ok_or_else(|| missing("DBClusterIdentifier"))?;
                let arn = Arn::new("rds", region, &aid, &format!("cluster:{id}")).to_string();
                {
                    let mut accounts = write_state!();
                    let state = accounts.get_or_create(&aid);
                    if let Some(m) = state.extras.get_mut("clusters") { m.remove(&id); }
                }
                self.emit_event(
                    RdsSourceType::DbCluster,
                    &id,
                    &arn,
                    "RDS-EVENT-0171",
                    &["deletion"],
                    "DB cluster deleted",
                );
                Ok(xml_response("DeleteDBCluster", db_cluster_xml(&id, &arn), &rid))
            }
            "ModifyDBCluster" => modify_db_cluster_action(self, &aid, region, req, &rid),
            "StartDBCluster" => start_db_cluster_action(self, &aid, region, req, &rid),
            "StopDBCluster" => stop_db_cluster_action(self, &aid, region, req, &rid),
            "RebootDBCluster" => reboot_db_cluster_action(self, &aid, region, req, &rid),
            "FailoverDBCluster" => failover_db_cluster_action(self, &aid, region, req, &rid),
            "BacktrackDBCluster" => backtrack_db_cluster_action(self, &aid, region, req, &rid),
            "PromoteReadReplicaDBCluster" => {
                let id = get_param(req, "DBClusterIdentifier")
                    .ok_or_else(|| missing("DBClusterIdentifier"))?;
                let arn = Arn::new("rds", region, &aid, &format!("cluster:{id}")).to_string();
                let mut accounts = write_state!();
                let state = accounts.get_or_create(&aid);
                if let Some(map) = state.extras.get_mut("clusters") {
                    if let Some(entry) = map.get_mut(&id) {
                        if let Some(obj) = entry.as_object_mut() {
                            obj.remove("ReplicationSourceIdentifier");
                        }
                    }
                }
                Ok(xml_response(
                    "PromoteReadReplicaDBCluster",
                    db_cluster_xml(&id, &arn),
                    &rid,
                ))
            }
            "DescribeDBClusters" => {
                // Same normalization as DescribeDBClusterSnapshots: an
                // empty parameter means "absent", and AWS documents this
                // one as accepting a cluster ARN too. A cluster is never
                // shared across accounts, so an ARN naming a different
                // account is not found rather than aliased onto this
                // account's same-named cluster.
                let raw_cluster_identifier = get_param(req, "DBClusterIdentifier");
                if let Some(raw) = raw_cluster_identifier.as_deref() {
                    if !addresses_own_account(raw, &aid) || !identifier_matches_type(raw, "cluster")
                    {
                        return Err(AwsServiceError::aws_error(
                            StatusCode::NOT_FOUND,
                            "DBClusterNotFoundFault",
                            format!("DBCluster {raw} not found."),
                        ));
                    }
                }
                let id_filter = normalized_identifier(raw_cluster_identifier, "cluster");
                let filters = parse_filters(req);
                let accounts = self.state_handle().read();
                // A named cluster that doesn't exist is the declared
                // `DBClusterNotFoundFault`; an empty list would tell a
                // client that distinguishes "gone" from "no match" the
                // wrong thing.
                if let Some(wanted) = id_filter.as_deref() {
                    let known = accounts
                        .get(&aid)
                        .and_then(|s| s.extras.get("clusters"))
                        .is_some_and(|m| {
                            m.values()
                                .any(|v| entry_str(v, "DBClusterIdentifier") == Some(wanted))
                        });
                    if !known {
                        return Err(AwsServiceError::aws_error(
                            StatusCode::NOT_FOUND,
                            "DBClusterNotFoundFault",
                            format!("DBCluster {wanted} not found."),
                        ));
                    }
                }
                let items: Vec<Value> = accounts.get(&aid)
                    .and_then(|s| s.extras.get("clusters"))
                    .map(|m| {
                        m.values()
                            .filter(|v| {
                                id_filter
                                    .as_deref()
                                    .map(|filter| v["DBClusterIdentifier"].as_str() == Some(filter))
                                    .unwrap_or(true)
                                    && cluster_matches_filters(v, &filters)
                            })
                            .cloned()
                            .collect()
                    })
                    .unwrap_or_default();
                let body = items
                    .iter()
                    .map(|v| {
                        format!(
                            "      <DBCluster>\n{}\n      </DBCluster>",
                            db_cluster_member_xml(v)
                        )
                    })
                    .collect::<Vec<_>>()
                    .join("\n");
                let inner = format!("    <DBClusters>\n{body}\n    </DBClusters>");
                Ok(xml_response("DescribeDBClusters", inner, &rid))
            }

            "CopyDBClusterSnapshot" => {
                let id = get_param(req, "TargetDBClusterSnapshotIdentifier")
                    .ok_or_else(|| missing("TargetDBClusterSnapshotIdentifier"))?;
                let source_id = get_param(req, "SourceDBClusterSnapshotIdentifier")
                    .ok_or_else(|| missing("SourceDBClusterSnapshotIdentifier"))?;
                let arn = Arn::new("rds", region, &aid, &format!("cluster-snapshot:{id}")).to_string();
                let mut accounts = write_state!();
                // Guarded ARN reduction: AWS automated-snapshot ids carry
                // a colon (`rds:mydb-...`), so only an `arn:` value is
                // trimmed.
                let source_key = normalized_identifier(Some(source_id.clone()), "cluster-snapshot")
                    .unwrap_or_else(|| source_id.clone());
                // AWS supports copying a snapshot another account shared
                // with you, so resolve cross-account -- but only against
                // the account the ARN names, never by aliasing a foreign
                // ARN onto this account's same-named snapshot. Resolved
                // before the mutable borrow the insert below needs.
                let source_owner = identifier_account(&source_id);
                // An existing target is the declared AlreadyExists fault:
                // overwriting would silently replace the target's dump
                // and revoke its sharing on a retried copy.
                if accounts
                    .get(&aid)
                    .and_then(|s| s.extras.get("cluster_snapshots"))
                    .is_some_and(|m| m.contains_key(&id))
                {
                    return Err(AwsServiceError::aws_error(
                        StatusCode::BAD_REQUEST,
                        "DBClusterSnapshotAlreadyExistsFault",
                        format!("DBClusterSnapshot {id} already exists."),
                    ));
                }
                let mut entry =
                    find_cluster_snapshot(&accounts, &aid, source_owner.as_deref(), &source_key)
                        .ok_or_else(|| {
                            AwsServiceError::aws_error(
                                StatusCode::NOT_FOUND,
                                "DBClusterSnapshotNotFoundFault",
                                format!("DBClusterSnapshot {source_id} not found."),
                            )
                        })?;
                let state = accounts.get_or_create(&aid);
                let cluster = entry
                    .get("DBClusterIdentifier")
                    .and_then(|v| v.as_str())
                    .unwrap_or("default")
                    .to_string();
                // The member is typed as an ARN, and the source may have
                // been named by bare id: take the resolved entry's own
                // ARN (which names its owner) rather than echoing the
                // caller's input, now that DescribeDBClusterSnapshots
                // reports this field. Read before the mutable borrow.
                let source_arn = if source_id.starts_with("arn:") {
                    source_id.clone()
                } else {
                    entry_str(&entry, "DBClusterSnapshotArn")
                        .map(str::to_string)
                        .unwrap_or_else(|| source_id.clone())
                };
                if let Some(obj) = entry.as_object_mut() {
                    obj.insert("DBClusterSnapshotIdentifier".to_string(), json!(id));
                    obj.insert("DBClusterSnapshotArn".to_string(), json!(arn));
                    obj.insert("Status".to_string(), json!("available"));
                    obj.insert("SnapshotType".to_string(), json!("manual"));
                    obj.insert("SourceDBClusterSnapshotArn".to_string(), json!(source_arn));
                    // A copy is a fresh sharing surface -- inheriting the
                    // source's `restore` list would publish a snapshot
                    // nobody shared.
                    obj.remove("SnapshotAttributes");
                }
                store(&mut state.extras, "cluster_snapshots").insert(id.clone(), entry.clone());
                Ok(xml_response(
                    action.as_str(),
                    cluster_snapshot_status_detail_xml(
                        &id,
                        &arn,
                        &cluster,
                        "available",
                        cluster_snapshot_detail_xml(Some(&entry)),
                    ),
                    &rid,
                ))
            }
            "DeleteDBClusterSnapshot" => {
                // Resolve the ARN form, as Describe and Restore do -- an
                // unnormalized delete would report success while leaving
                // the entry in place, so a Terraform destroy never
                // converges.
                let raw = get_param(req, "DBClusterSnapshotIdentifier")
                    .ok_or_else(|| missing("DBClusterSnapshotIdentifier"))?;
                if !addresses_own_account(&raw, &aid) {
                    return Err(AwsServiceError::aws_error(
                        StatusCode::NOT_FOUND,
                        "DBClusterSnapshotNotFoundFault",
                        format!("DBClusterSnapshot {raw} not found."),
                    ));
                }
                // A wrong-type ARN names no cluster snapshot. That is the
                // declared DBClusterSnapshotNotFoundFault, not the
                // undeclared InvalidParameterValue `missing()` raises --
                // an unmodeled error hard-fails a Terraform destroy that
                // would otherwise treat the snapshot as gone.
                let id = normalized_identifier(Some(raw.clone()), "cluster-snapshot").ok_or_else(
                    || {
                        AwsServiceError::aws_error(
                            StatusCode::NOT_FOUND,
                            "DBClusterSnapshotNotFoundFault",
                            format!("DBClusterSnapshot {raw} not found."),
                        )
                    },
                )?;
                let arn = Arn::new("rds", region, &aid, &format!("cluster-snapshot:{id}")).to_string();
                // Recover the source cluster id from stored state before
                // remove — emitting a hardcoded "default" would corrupt
                // downstream consumers that key off DBClusterIdentifier.
                let deleted = {
                    let mut accounts = write_state!();
                    let state = accounts.get_or_create(&aid);
                    // Deleting a snapshot that doesn't exist is the
                    // declared fault, not a 200 with an empty cluster id
                    // and a spurious "snapshot deleted" event.
                    let entry = state
                        .extras
                        .get("cluster_snapshots")
                        .and_then(|m| m.get(&id))
                        .cloned()
                        .ok_or_else(|| {
                            AwsServiceError::aws_error(
                                StatusCode::NOT_FOUND,
                                "DBClusterSnapshotNotFoundFault",
                                format!("DBClusterSnapshot {id} not found."),
                            )
                        })?;
                    if let Some(m) = state.extras.get_mut("cluster_snapshots") {
                        m.remove(&id);
                    }
                    entry
                };
                let cluster = entry_str(&deleted, "DBClusterIdentifier")
                    .unwrap_or_default()
                    .to_string();
                self.emit_event(
                    RdsSourceType::DbClusterSnapshot,
                    &id,
                    &arn,
                    "RDS-EVENT-0075",
                    &["deletion"],
                    "DB cluster snapshot deleted",
                );
                Ok(xml_response(
                    "DeleteDBClusterSnapshot",
                    // Same detail the create / copy responses report, as
                    // `cluster_snapshot_detail_xml` documents: the entry
                    // is in hand a few lines above.
                    cluster_snapshot_status_detail_xml(
                        &id,
                        &arn,
                        &cluster,
                        "deleted",
                        cluster_snapshot_detail_xml(Some(&deleted)),
                    ),
                    &rid,
                ))
            }
            "DescribeDBClusterSnapshots" => {
                // Narrow by the request's own identifier parameters,
                // SnapshotType and Filters — all AND-ed, as on real AWS.
                // Returning every snapshot regardless makes clients that
                // expect a unique match (Terraform) fail to resolve one.
                // `normalized_identifier` reduces an ARN to its resource
                // segment (clients pass either form, and
                // CopyDBClusterSnapshot already normalizes the same way)
                // and treats an explicitly-empty parameter as absent --
                // `get_param` keeps `DBClusterSnapshotIdentifier=`, which
                // would otherwise match nothing and 404 below.
                // The ARN's account is kept, not dropped: a foreign ARN
                // must not resolve against this account's same-named
                // snapshot (nor against a third account that happens to
                // have shared one under the same id).
                let raw_snapshot_id = get_param(req, "DBClusterSnapshotIdentifier");
                // A wrong-type ARN is not a cluster snapshot; without
                // this it would normalize to `None` and list everything.
                if let Some(raw) = raw_snapshot_id.as_deref() {
                    if !identifier_matches_type(raw, "cluster-snapshot") {
                        return Err(AwsServiceError::aws_error(
                            StatusCode::NOT_FOUND,
                            "DBClusterSnapshotNotFoundFault",
                            format!("DBClusterSnapshot {raw} not found."),
                        ));
                    }
                }
                let snapshot_owner = raw_snapshot_id.as_deref().and_then(identifier_account);
                let snapshot_id = normalized_identifier(raw_snapshot_id, "cluster-snapshot");
                let raw_cluster_id = get_param(req, "DBClusterIdentifier");
                // A wrong-type ARN names no cluster, so nothing matches.
                // Returning early beats dropping the parameter, which
                // would read as "no filter" and list everything.
                if raw_cluster_id
                    .as_deref()
                    .is_some_and(|raw| !identifier_matches_type(raw, "cluster"))
                {
                    return Ok(xml_response(
                        "DescribeDBClusterSnapshots",
                        "    <DBClusterSnapshots>\n\n    </DBClusterSnapshots>".to_string(),
                        &rid,
                    ));
                }
                let cluster_owner = raw_cluster_id.as_deref().and_then(identifier_account);
                let cluster_id = normalized_identifier(raw_cluster_id, "cluster");
                // An identifier naming another account can never match a
                // snapshot this account owns.
                let owner_is_caller = snapshot_owner
                    .as_deref()
                    .is_none_or(|account| account == aid)
                    && cluster_owner
                        .as_deref()
                        .is_none_or(|account| account == aid);
                let snapshot_type =
                    get_param(req, "SnapshotType").filter(|value| !value.is_empty());
                // Parsed exactly as DescribeDBSnapshots does, so the two
                // ops can't drift on what counts as true. A junk boolean
                // is treated as absent: InvalidParameterValue isn't
                // declared on this operation.
                let include_shared =
                    optional_flag(get_param(req, "IncludeShared").as_deref()).unwrap_or(false);
                let include_public =
                    optional_flag(get_param(req, "IncludePublic").as_deref()).unwrap_or(false);
                let filters = parse_filters(req);
                let accounts = self.state_handle().read();
                // A named snapshot that doesn't exist is the declared
                // `DBClusterSnapshotNotFoundFault`, same as the instance
                // and DB-snapshot describes. (Unknown *filter* names
                // can't error the same way — `InvalidParameterValue`
                // isn't declared here; see `crate::filters`.)
                if let Some(wanted) = snapshot_id.as_deref() {
                    let known = accounts.iter().any(|(owner, s)| {
                        snapshot_owner
                            .as_deref()
                            .is_none_or(|account| account == owner)
                            && s.extras.get("cluster_snapshots").is_some_and(|m| {
                            m.values().any(|v| {
                                entry_str(v, "DBClusterSnapshotIdentifier") == Some(wanted)
                                    && (owner == aid || {
                                        // Another account's snapshot only
                                        // exists for this caller when it
                                        // was shared with them AND they
                                        // addressed it by its ARN -- the
                                        // same rule the listing applies,
                                        // so the two can't disagree and
                                        // produce a 200 with no rows.
                                        let attrs = cluster_snapshot_attributes(v);
                                        snapshot_owner.as_deref() == Some(owner)
                                            && attrs.get("restore").is_some_and(|targets| {
                                                targets.contains(&aid)
                                                    || targets.iter().any(|t| t == "all")
                                            })
                                    })
                            })
                        })
                    });
                    if !known {
                        return Err(AwsServiceError::aws_error(
                            StatusCode::NOT_FOUND,
                            "DBClusterSnapshotNotFoundFault",
                            format!("DBClusterSnapshot {wanted} not found."),
                        ));
                    }
                }
                let items: Vec<Value> = accounts
                    .get(&aid)
                    .and_then(|s| s.extras.get("cluster_snapshots"))
                    .filter(|_| owner_is_caller)
                    .map(|m| {
                        m.values()
                            .filter(|v| {
                                snapshot_id.as_deref().is_none_or(|wanted| {
                                    entry_str(v, "DBClusterSnapshotIdentifier") == Some(wanted)
                                }) && cluster_id.as_deref().is_none_or(|wanted| {
                                    entry_str(v, "DBClusterIdentifier") == Some(wanted)
                                }) && owned_snapshot_type_matches(v, snapshot_type.as_deref())
                                    && cluster_snapshot_matches_filters(v, &filters, &aid, true)
                            })
                            .cloned()
                            .collect()
                    })
                    .unwrap_or_default();
                // `shared` / `public` select cluster snapshots another
                // account shared through ModifyDBClusterSnapshotAttribute,
                // exactly as on DescribeDBSnapshots.
                // IncludeShared / IncludePublic widen an unqualified
                // listing, exactly as on DescribeDBSnapshots.
                // A caller that named a snapshot explicitly resolves it
                // without IncludeShared as well -- AWS resolves a shared
                // snapshot addressed by ARN.
                // A named lookup the caller OWNS must not also pull in
                // another account's identical id: AWS resolves a named
                // snapshot uniquely, and two rows are the "couldn't
                // resolve a single result" failure this branch exists to
                // avoid. Keyed on whether an owned row with that id
                // exists at all, not on whether it survived the filters --
                // otherwise a filtered-out owned row would be replaced by
                // a foreign one.
                let owns_named = snapshot_id.as_deref().is_some_and(|wanted| {
                    owner_is_caller
                        && accounts
                            .get(&aid)
                            .and_then(|s| s.extras.get("cluster_snapshots"))
                            .is_some_and(|m| {
                                m.values().any(|v| {
                                    entry_str(v, "DBClusterSnapshotIdentifier") == Some(wanted)
                                })
                            })
                });
                // Only an ARN reaches another account's shared snapshot
                // (AWS requires it, and a bare id could match several
                // accounts at once and return duplicate rows).
                let named = snapshot_id.is_some() && !owns_named && snapshot_owner.is_some();
                // Owning the named row suppresses the cross-account scan
                // outright, not just the implicit widening: with
                // IncludeShared set (which `data.aws_db_cluster_snapshot`
                // does), another account's snapshot of the same bare id
                // would otherwise be appended and the lookup would return
                // two rows.
                let want_shared = !owns_named
                    && (snapshot_type.as_deref() == Some("shared")
                        || ((include_shared || named) && snapshot_type.is_none()));
                let want_public = !owns_named
                    && (snapshot_type.as_deref() == Some("public")
                        || ((include_public || named) && snapshot_type.is_none()));
                let mut items = items;
                if want_shared || want_public {
                    for (owner, other) in accounts.iter() {
                        if owner == aid {
                            continue;
                        }
                        // An ARN names its owner: never resolve it
                        // against a different account's identical id.
                        // The cluster ARN's account is honoured for
                        // foreign rows too, not just owned ones.
                        if snapshot_owner
                            .as_deref()
                            .is_some_and(|account| account != owner)
                            || cluster_owner
                                .as_deref()
                                .is_some_and(|account| account != owner)
                        {
                            continue;
                        }
                        let Some(bucket) = other.extras.get("cluster_snapshots") else {
                            continue;
                        };
                        items.extend(
                            bucket
                                .values()
                                .filter(|v| {
                                    let attrs = cluster_snapshot_attributes(v);
                                    let restore = attrs.get("restore");
                                    (want_shared
                                        && restore
                                            .is_some_and(|targets| targets.contains(&aid)))
                                        || (want_public
                                            && restore.is_some_and(|targets| {
                                                targets.iter().any(|t| t == "all")
                                            }))
                                })
                                .filter(|v| {
                                    snapshot_id.as_deref().is_none_or(|wanted| {
                                        entry_str(v, "DBClusterSnapshotIdentifier") == Some(wanted)
                                    }) && cluster_id.as_deref().is_none_or(|wanted| {
                                        entry_str(v, "DBClusterIdentifier") == Some(wanted)
                                    }) && cluster_snapshot_matches_filters(
                                        v, &filters, &aid, false,
                                    )
                                })
                                .cloned(),
                        );
                    }
                }
                // Named member tags, not the generic `<member>`: the
                // Smithy list carries xmlName `DBClusterSnapshot`, and the
                // AWS SDK unmarshals an empty list from `<member>` (see
                // `list_extras_named_xml`) -- which would make the filtering
                // above invisible to every real client.
                let body = items
                    .iter()
                    .map(|v| {
                        format!(
                            "        <DBClusterSnapshot>\n{}\n        </DBClusterSnapshot>",
                            cluster_snapshot_member_xml(v)
                        )
                    })
                    .collect::<Vec<_>>()
                    .join("\n");
                let inner = format!("    <DBClusterSnapshots>\n{body}\n    </DBClusterSnapshots>");
                Ok(xml_response("DescribeDBClusterSnapshots", inner, &rid))
            }
            "DescribeDBClusterSnapshotAttributes" => {
                let raw = get_param(req, "DBClusterSnapshotIdentifier")
                    .ok_or_else(|| missing("DBClusterSnapshotIdentifier"))?;
                if !addresses_own_account(&raw, &aid) {
                    return Err(AwsServiceError::aws_error(
                        StatusCode::NOT_FOUND,
                        "DBClusterSnapshotNotFoundFault",
                        format!("DBClusterSnapshot {raw} not found."),
                    ));
                }
                // A wrong-type ARN names no cluster snapshot. That is the
                // declared DBClusterSnapshotNotFoundFault, not the
                // undeclared InvalidParameterValue `missing()` raises --
                // an unmodeled error hard-fails a Terraform destroy that
                // would otherwise treat the snapshot as gone.
                let id = normalized_identifier(Some(raw.clone()), "cluster-snapshot").ok_or_else(
                    || {
                        AwsServiceError::aws_error(
                            StatusCode::NOT_FOUND,
                            "DBClusterSnapshotNotFoundFault",
                            format!("DBClusterSnapshot {raw} not found."),
                        )
                    },
                )?;
                let accounts = self.state_handle().read();
                let entry = accounts
                    .get(&aid)
                    .and_then(|s| s.extras.get("cluster_snapshots"))
                    .and_then(|m| m.get(&id))
                    .ok_or_else(|| {
                        AwsServiceError::aws_error(
                            StatusCode::NOT_FOUND,
                            "DBClusterSnapshotNotFoundFault",
                            format!("DBClusterSnapshot {id} not found."),
                        )
                    })?;
                Ok(xml_response(
                    action.as_str(),
                    cluster_snapshot_attributes_result_xml(&id, &cluster_snapshot_attributes(entry)),
                    &rid,
                ))
            }
            "ModifyDBClusterSnapshotAttribute" => {
                // Mirrors ModifyDBSnapshotAttribute: the `restore`
                // attribute records the accounts (or `all`) a snapshot is
                // shared with, which is what SnapshotType=shared/public
                // selects on.
                let raw = get_param(req, "DBClusterSnapshotIdentifier")
                    .ok_or_else(|| missing("DBClusterSnapshotIdentifier"))?;
                if !addresses_own_account(&raw, &aid) {
                    return Err(AwsServiceError::aws_error(
                        StatusCode::NOT_FOUND,
                        "DBClusterSnapshotNotFoundFault",
                        format!("DBClusterSnapshot {raw} not found."),
                    ));
                }
                // A wrong-type ARN names no cluster snapshot. That is the
                // declared DBClusterSnapshotNotFoundFault, not the
                // undeclared InvalidParameterValue `missing()` raises --
                // an unmodeled error hard-fails a Terraform destroy that
                // would otherwise treat the snapshot as gone.
                let id = normalized_identifier(Some(raw.clone()), "cluster-snapshot").ok_or_else(
                    || {
                        AwsServiceError::aws_error(
                            StatusCode::NOT_FOUND,
                            "DBClusterSnapshotNotFoundFault",
                            format!("DBClusterSnapshot {raw} not found."),
                        )
                    },
                )?;
                let attribute_name =
                    get_param(req, "AttributeName").ok_or_else(|| missing("AttributeName"))?;
                let to_add = parse_attribute_values(req, "ValuesToAdd");
                let to_remove = parse_attribute_values(req, "ValuesToRemove");
                // AWS rejects a value present in both lists, but
                // `InvalidParameterCombination` is not even a shape in the
                // RDS model, so emitting it here would be an undeclared
                // error (see the module docs on `crate::filters`). Resolve
                // it deterministically instead: removals first, then
                // additions, so the value ends up added.
                let attrs = {
                    let mut accounts = write_state!();
                    let state = accounts.get_or_create(&aid);
                    let entry = state
                        .extras
                        .get_mut("cluster_snapshots")
                        .and_then(|m| m.get_mut(&id))
                        .ok_or_else(|| {
                            AwsServiceError::aws_error(
                                StatusCode::NOT_FOUND,
                                "DBClusterSnapshotNotFoundFault",
                                format!("DBClusterSnapshot {id} not found."),
                            )
                        })?;
                    let mut attrs = cluster_snapshot_attributes(entry);
                    let values = attrs.entry(attribute_name.clone()).or_default();
                    values.retain(|v| !to_remove.contains(v));
                    for v in to_add {
                        if !values.contains(&v) {
                            values.push(v);
                        }
                    }
                    // An attribute with no values reads back as unshared,
                    // matching AWS, so drop it rather than storing [].
                    if values.is_empty() {
                        attrs.remove(&attribute_name);
                    }
                    if let Some(obj) = entry.as_object_mut() {
                        obj.insert("SnapshotAttributes".to_string(), json!(attrs));
                    }
                    attrs
                };
                Ok(xml_response(
                    "ModifyDBClusterSnapshotAttribute",
                    cluster_snapshot_attributes_result_xml(&id, &attrs),
                    &rid,
                ))
            }
            "DescribeDBClusterAutomatedBackups" => Ok(xml_response("DescribeDBClusterAutomatedBackups", "    <DBClusterAutomatedBackups/>".to_string(), &rid)),
            "DeleteDBClusterAutomatedBackup" => Ok(xml_response("DeleteDBClusterAutomatedBackup", "    <DBClusterAutomatedBackup/>".to_string(), &rid)),
            "DescribeDBClusterBacktracks" => Ok(xml_response("DescribeDBClusterBacktracks", "    <DBClusterBacktracks/>".to_string(), &rid)),

            // ── DB Cluster parameter groups ──
            "CreateDBClusterParameterGroup" | "CopyDBClusterParameterGroup" => {
                let name = get_param(req, "DBClusterParameterGroupName").or_else(|| get_param(req, "TargetDBClusterParameterGroupIdentifier"))
                    .ok_or_else(|| missing("DBClusterParameterGroupName"))?;
                let arn = Arn::new("rds", region, &aid, &format!("cluster-pg:{name}")).to_string();
                let family = get_param(req, "DBParameterGroupFamily").unwrap_or_else(|| "aurora-postgresql15".to_string());
                let description = get_param(req, "Description").unwrap_or_default();
                let entry = json!({"DBClusterParameterGroupName": name, "DBClusterParameterGroupArn": arn, "DBParameterGroupFamily": family, "Description": description});
                let mut accounts = write_state!();
                let state = accounts.get_or_create(&aid);
                store(&mut state.extras, "cluster_param_groups").insert(name.clone(), entry);
                Ok(xml_response(action.as_str(), cluster_pg_xml(&name, &arn, &family, &description), &rid))
            }
            "ModifyDBClusterParameterGroup" => {
                let name = get_param(req, "DBClusterParameterGroupName").ok_or_else(|| missing("DBClusterParameterGroupName"))?;
                let parsed = crate::service::parse_db_parameter_members(req);
                let mut accounts = write_state!();
                let state = accounts.get_or_create(&aid);
                if let Some(map) = state.extras.get_mut("cluster_param_groups") {
                    if let Some(entry) = map.get_mut(&name) {
                        if let Some(obj) = entry.as_object_mut() {
                            if !obj.contains_key("Parameters") {
                                obj.insert("Parameters".to_string(), json!({}));
                            }
                            if !obj.contains_key("ParameterApplyMethods") {
                                obj.insert("ParameterApplyMethods".to_string(), json!({}));
                            }
                            // Capture values and apply methods separately so the
                            // existing string-valued `Parameters` map shape stays
                            // backward compatible with older persisted snapshots.
                            let apply_methods: Vec<(String, String)> = parsed
                                .iter()
                                .map(|p| (p.name.clone(), p.apply_method.clone()))
                                .collect();
                            if let Some(p) = obj.get_mut("Parameters").and_then(|p| p.as_object_mut()) {
                                for param in &parsed {
                                    p.insert(param.name.clone(), json!(param.value));
                                }
                            }
                            if let Some(m) = obj
                                .get_mut("ParameterApplyMethods")
                                .and_then(|m| m.as_object_mut())
                            {
                                for (n, am) in apply_methods {
                                    m.insert(n, json!(am));
                                }
                            }
                        }
                    }
                }
                Ok(xml_response("ModifyDBClusterParameterGroup", format!("    <DBClusterParameterGroupName>{}</DBClusterParameterGroupName>", xml_escape(&name)), &rid))
            }
            "ResetDBClusterParameterGroup" => {
                let name = get_param(req, "DBClusterParameterGroupName").ok_or_else(|| missing("DBClusterParameterGroupName"))?;
                let reset_all = get_param(req, "ResetAllParameters")
                    .map(|v| v.eq_ignore_ascii_case("true"))
                    .unwrap_or(false);
                let named: Vec<String> = crate::service::parse_db_parameter_members(req)
                    .into_iter()
                    .map(|p| p.name)
                    .collect();
                {
                    let mut accounts = write_state!();
                    let state = accounts.get_or_create(&aid);
                    if let Some(entry) = state
                        .extras
                        .get_mut("cluster_param_groups")
                        .and_then(|m| m.get_mut(&name))
                        .and_then(|e| e.as_object_mut())
                    {
                        for key in ["Parameters", "ParameterApplyMethods"] {
                            if let Some(obj) = entry.get_mut(key).and_then(|p| p.as_object_mut()) {
                                if reset_all || named.is_empty() {
                                    obj.clear();
                                } else {
                                    for n in &named {
                                        obj.remove(n);
                                    }
                                }
                            }
                        }
                    }
                }
                Ok(xml_response("ResetDBClusterParameterGroup", format!("    <DBClusterParameterGroupName>{}</DBClusterParameterGroupName>", xml_escape(&name)), &rid))
            }
            "DeleteDBClusterParameterGroup" => {
                let name = get_param(req, "DBClusterParameterGroupName").ok_or_else(|| missing("DBClusterParameterGroupName"))?;
                let mut accounts = write_state!();
                let state = accounts.get_or_create(&aid);
                if let Some(m) = state.extras.get_mut("cluster_param_groups") { m.remove(&name); }
                xml_empty_action(&action, &rid)
            }
            "DescribeDBClusterParameterGroups" => {
                // RDS query lists wrap each element in its named member tag
                // (`<DBClusterParameterGroup>`), not the generic `<member>`;
                // the AWS SDK unmarshaler returns an empty list otherwise.
                // AWS also filters by name and raises NotFound for an unknown
                // group rather than returning everything.
                let wanted = get_param(req, "DBClusterParameterGroupName");
                let accounts = self.state_handle().read();
                let groups: Vec<Value> = accounts
                    .get(&aid)
                    .and_then(|s| s.extras.get("cluster_param_groups"))
                    .map(|m| m.values().cloned().collect())
                    .unwrap_or_default();
                if let Some(name) = &wanted {
                    let found = groups.iter().any(|g| {
                        g["DBClusterParameterGroupName"].as_str() == Some(name.as_str())
                    });
                    if !found {
                        return Err(AwsServiceError::aws_error(
                            StatusCode::NOT_FOUND,
                            "DBParameterGroupNotFound",
                            format!("DBClusterParameterGroup not found: {name}"),
                        ));
                    }
                }
                let members = groups
                    .iter()
                    .filter(|g| {
                        wanted.as_deref().is_none_or(|n| {
                            g["DBClusterParameterGroupName"].as_str() == Some(n)
                        })
                    })
                    .map(|g| {
                        format!(
                            "        <DBClusterParameterGroup>\n{}\n        </DBClusterParameterGroup>",
                            cluster_pg_member_xml(g)
                        )
                    })
                    .collect::<Vec<_>>()
                    .join("\n");
                Ok(xml_response(
                    "DescribeDBClusterParameterGroups",
                    format!("    <DBClusterParameterGroups>\n{members}\n    </DBClusterParameterGroups>"),
                    &rid,
                ))
            }
            "DescribeDBClusterParameters" => {
                let name = get_param(req, "DBClusterParameterGroupName").ok_or_else(|| missing("DBClusterParameterGroupName"))?;
                let source_filter = get_param(req, "Source");
                let source = source_filter.as_deref();
                let include_user = source.is_none_or(|s| s == "user");
                let include_engine_default = source.is_none_or(|s| s == "engine-default");
                let accounts = self.state_handle().read();
                let state = accounts.get(&aid);
                let entry = state
                    .and_then(|s| s.extras.get("cluster_param_groups"))
                    .and_then(|m| m.get(&name));
                let family = entry
                    .and_then(|e| e.get("DBParameterGroupFamily"))
                    .and_then(|f| f.as_str())
                    .unwrap_or("aurora-postgresql15")
                    .to_string();
                let user_params: BTreeMap<String, String> = entry
                    .and_then(|e| e.get("Parameters"))
                    .and_then(|p| p.as_object())
                    .map(|obj| obj.iter().map(|(k, v)| (k.clone(), v.as_str().unwrap_or("").to_string())).collect())
                    .unwrap_or_default();
                let apply_methods: BTreeMap<String, String> = entry
                    .and_then(|e| e.get("ParameterApplyMethods"))
                    .and_then(|p| p.as_object())
                    .map(|obj| obj.iter().map(|(k, v)| (k.clone(), v.as_str().unwrap_or("").to_string())).collect())
                    .unwrap_or_default();
                let mut members = String::new();
                if include_user {
                    for (n, v) in &user_params {
                        let apply_method = apply_methods.get(n).map(String::as_str).unwrap_or("immediate");
                        members.push_str(&crate::service::render_user_parameter_xml(n, v, apply_method));
                    }
                }
                if include_engine_default {
                    // A user override flips a parameter's effective
                    // source from `engine-default` to `user`, so we
                    // always hide modified parameters from engine-default
                    // views — even if the caller filtered to that source.
                    for default in crate::state::engine_default_parameters(&family) {
                        if user_params.contains_key(default.name) {
                            continue;
                        }
                        members.push_str(&crate::service::render_engine_default_parameter_xml(default));
                    }
                }
                Ok(xml_response("DescribeDBClusterParameters", format!("    <Parameters>\n{members}    </Parameters>"), &rid))
            }
            "DescribeEngineDefaultClusterParameters" => {
                let family = get_param(req, "DBParameterGroupFamily").unwrap_or_else(|| "aurora-postgresql15".to_string());
                let mut members = String::new();
                for default in crate::state::engine_default_parameters(&family) {
                    members.push_str(&crate::service::render_engine_default_parameter_xml(default));
                }
                let body = format!(
                    "    <EngineDefaults>\n      <DBParameterGroupFamily>{}</DBParameterGroupFamily>\n      <Parameters>\n{}      </Parameters>\n    </EngineDefaults>",
                    xml_escape(&family),
                    members,
                );
                Ok(xml_response("DescribeEngineDefaultClusterParameters", body, &rid))
            }

            // ── DB Cluster endpoints ──
            "CreateDBClusterEndpoint" => {
                let id = get_param(req, "DBClusterEndpointIdentifier").ok_or_else(|| missing("DBClusterEndpointIdentifier"))?;
                let cluster = get_param(req, "DBClusterIdentifier").unwrap_or_default();
                let kind = get_param(req, "EndpointType").unwrap_or_else(|| "READER".to_string());
                let entry = json!({"DBClusterEndpointIdentifier": id, "DBClusterIdentifier": cluster, "Endpoint": format!("{id}.cluster-custom.{region}.rds.amazonaws.com"), "EndpointType": kind, "Status": "available"});
                let mut accounts = write_state!();
                let state = accounts.get_or_create(&aid);
                store(&mut state.extras, "cluster_endpoints").insert(id.clone(), entry.clone());
                Ok(xml_response("CreateDBClusterEndpoint", cluster_endpoint_xml(&entry), &rid))
            }
            "ModifyDBClusterEndpoint" => {
                let id = get_param(req, "DBClusterEndpointIdentifier").ok_or_else(|| missing("DBClusterEndpointIdentifier"))?;
                let static_members = parse_member_list(req, "StaticMembers");
                let excluded_members = parse_member_list(req, "ExcludedMembers");
                let mut accounts = write_state!();
                let state = accounts.get_or_create(&aid);
                let entry = state
                    .extras
                    .get_mut("cluster_endpoints")
                    .and_then(|m| m.get_mut(&id))
                    .ok_or_else(|| {
                        AwsServiceError::aws_error(
                            StatusCode::NOT_FOUND,
                            "DBClusterEndpointNotFoundFault",
                            format!("DBClusterEndpoint {id} not found."),
                        )
                    })?;
                if let Some(obj) = entry.as_object_mut() {
                    if let Some(kind) = get_param(req, "EndpointType") {
                        obj.insert("EndpointType".to_string(), json!(kind));
                    }
                    if !static_members.is_empty() {
                        obj.insert("StaticMembers".to_string(), json!(static_members));
                    }
                    if !excluded_members.is_empty() {
                        obj.insert("ExcludedMembers".to_string(), json!(excluded_members));
                    }
                }
                let updated = entry.clone();
                Ok(xml_response("ModifyDBClusterEndpoint", cluster_endpoint_xml(&updated), &rid))
            }
            "DeleteDBClusterEndpoint" => {
                let id = get_param(req, "DBClusterEndpointIdentifier").ok_or_else(|| missing("DBClusterEndpointIdentifier"))?;
                let mut accounts = write_state!();
                let state = accounts.get_or_create(&aid);
                if let Some(m) = state.extras.get_mut("cluster_endpoints") { m.remove(&id); }
                Ok(xml_response("DeleteDBClusterEndpoint", format!("    <DBClusterEndpointIdentifier>{}</DBClusterEndpointIdentifier>", xml_escape(&id)), &rid))
            }
            "DescribeDBClusterEndpoints" => list_extras_xml(self, &aid, "cluster_endpoints", "DBClusterEndpoints", "DBClusterEndpointList", "DescribeDBClusterEndpoints", cluster_endpoint_xml, &rid),

            // ── DB Proxies ──
            "CreateDBProxy" => {
                let name = get_param(req, "DBProxyName").ok_or_else(|| missing("DBProxyName"))?;
                let arn = Arn::new("rds", region, &aid, &format!("db-proxy:{name}")).to_string();
                let entry = json!({"DBProxyName": name, "DBProxyArn": arn, "Status": "available", "EngineFamily": get_param(req, "EngineFamily").unwrap_or_else(|| "POSTGRESQL".to_string())});
                let mut accounts = write_state!();
                let state = accounts.get_or_create(&aid);
                store(&mut state.extras, "proxies").insert(name.clone(), entry.clone());
                Ok(xml_response("CreateDBProxy", proxy_xml(&entry), &rid))
            }
            "ModifyDBProxy" => {
                let name = get_param(req, "DBProxyName").ok_or_else(|| missing("DBProxyName"))?;
                let auth = parse_proxy_auth(req);
                let new_name = get_param(req, "NewDBProxyName");
                let mut accounts = write_state!();
                let state = accounts.get_or_create(&aid);
                let entry = state
                    .extras
                    .get_mut("proxies")
                    .and_then(|m| m.get_mut(&name))
                    .ok_or_else(|| {
                        AwsServiceError::aws_error(
                            StatusCode::NOT_FOUND,
                            "DBProxyNotFoundFault",
                            format!("DBProxy {name} not found."),
                        )
                    })?;
                if let Some(obj) = entry.as_object_mut() {
                    if !auth.is_empty() {
                        obj.insert("Auth".to_string(), json!(auth));
                    }
                    if let Some(v) = get_param(req, "RequireTLS") {
                        obj.insert("RequireTLS".to_string(), json!(v.eq_ignore_ascii_case("true")));
                    }
                    if let Some(v) = get_param(req, "IdleClientTimeout").and_then(|s| s.parse::<i64>().ok()) {
                        obj.insert("IdleClientTimeout".to_string(), json!(v));
                    }
                    if let Some(v) = get_param(req, "DebugLogging") {
                        obj.insert("DebugLogging".to_string(), json!(v.eq_ignore_ascii_case("true")));
                    }
                    if let Some(v) = new_name.as_ref() {
                        obj.insert("DBProxyName".to_string(), json!(v));
                    }
                }
                let updated = entry.clone();
                // Rekey the map so subsequent Describe/Delete/Modify
                // against NewDBProxyName actually find the entry —
                // otherwise the rename only mutates the payload field
                // and Describe still keys by the old name.
                if let Some(new) = new_name {
                    if new != name {
                        if let Some(m) = state.extras.get_mut("proxies") {
                            if let Some(val) = m.remove(&name) {
                                m.insert(new.clone(), val);
                            }
                        }
                        // proxy_target_groups is keyed `<proxy>/<group>` —
                        // rekey every entry that belongs to this proxy so
                        // filtered describes by the new name keep matching.
                        if let Some(m) = state.extras.get_mut("proxy_target_groups") {
                            let old_prefix = format!("{name}/");
                            let migrations: Vec<(String, String)> = m
                                .keys()
                                .filter(|k| k.starts_with(&old_prefix))
                                .map(|k| {
                                    let suffix = &k[old_prefix.len()..];
                                    (k.clone(), format!("{new}/{suffix}"))
                                })
                                .collect();
                            for (old_k, new_k) in migrations {
                                if let Some(mut val) = m.remove(&old_k) {
                                    if let Some(obj) = val.as_object_mut() {
                                        obj.insert("DBProxyName".to_string(), json!(new));
                                    }
                                    m.insert(new_k, val);
                                }
                            }
                        }
                    }
                }
                Ok(xml_response("ModifyDBProxy", format!("    <DBProxy>\n{}\n    </DBProxy>", proxy_xml(&updated)), &rid))
            }
            "DeleteDBProxy" => {
                let name = get_param(req, "DBProxyName").ok_or_else(|| missing("DBProxyName"))?;
                let mut accounts = write_state!();
                let state = accounts.get_or_create(&aid);
                if let Some(m) = state.extras.get_mut("proxies") { m.remove(&name); }
                Ok(xml_response("DeleteDBProxy", "    <DBProxy/>".to_string(), &rid))
            }
            "DescribeDBProxies" => list_extras_xml(self, &aid, "proxies", "DBProxies", "member", "DescribeDBProxies", proxy_xml, &rid),
            "CreateDBProxyEndpoint" => {
                let name = get_param(req, "DBProxyEndpointName").ok_or_else(|| missing("DBProxyEndpointName"))?;
                let entry = json!({"DBProxyEndpointName": name, "Status": "available"});
                let mut accounts = write_state!();
                let state = accounts.get_or_create(&aid);
                store(&mut state.extras, "proxy_endpoints").insert(name.clone(), entry);
                Ok(xml_response("CreateDBProxyEndpoint", format!("    <DBProxyEndpoint>\n      <DBProxyEndpointName>{}</DBProxyEndpointName>\n    </DBProxyEndpoint>", xml_escape(&name)), &rid))
            }
            "ModifyDBProxyEndpoint" => {
                let name = get_param(req, "DBProxyEndpointName").ok_or_else(|| missing("DBProxyEndpointName"))?;
                let vpc_sgs = parse_member_list(req, "VpcSecurityGroupIds");
                let new_name = get_param(req, "NewDBProxyEndpointName");
                let mut accounts = write_state!();
                let state = accounts.get_or_create(&aid);
                let entry = state
                    .extras
                    .get_mut("proxy_endpoints")
                    .and_then(|m| m.get_mut(&name))
                    .ok_or_else(|| {
                        AwsServiceError::aws_error(
                            StatusCode::NOT_FOUND,
                            "DBProxyEndpointNotFoundFault",
                            format!("DBProxyEndpoint {name} not found."),
                        )
                    })?;
                if let Some(obj) = entry.as_object_mut() {
                    if !vpc_sgs.is_empty() {
                        obj.insert("VpcSecurityGroupIds".to_string(), json!(vpc_sgs));
                    }
                    if let Some(v) = new_name.as_ref() {
                        obj.insert("DBProxyEndpointName".to_string(), json!(v));
                    }
                }
                let final_name = new_name.clone().unwrap_or_else(|| name.clone());
                // Rekey so the rename is visible to subsequent lookups,
                // not just to the payload field.
                if let Some(new) = new_name {
                    if new != name {
                        if let Some(m) = state.extras.get_mut("proxy_endpoints") {
                            if let Some(val) = m.remove(&name) {
                                m.insert(new, val);
                            }
                        }
                    }
                }
                Ok(xml_response("ModifyDBProxyEndpoint", format!("    <DBProxyEndpoint>\n      <DBProxyEndpointName>{}</DBProxyEndpointName>\n    </DBProxyEndpoint>", xml_escape(&final_name)), &rid))
            }
            "DeleteDBProxyEndpoint" => {
                let name = get_param(req, "DBProxyEndpointName").ok_or_else(|| missing("DBProxyEndpointName"))?;
                let mut accounts = write_state!();
                let state = accounts.get_or_create(&aid);
                if let Some(m) = state.extras.get_mut("proxy_endpoints") { m.remove(&name); }
                Ok(xml_response("DeleteDBProxyEndpoint", "    <DBProxyEndpoint/>".to_string(), &rid))
            }
            "DescribeDBProxyEndpoints" => {
                let accounts = self.state.read();
                let state_opt = accounts.get(&aid);
                let mut members = String::new();
                if let Some(state) = state_opt {
                    if let Some(m) = state.extras.get("proxy_endpoints") {
                        for v in m.values() {
                            // Render with the same field shape as
                            // CreateDBProxyEndpoint above so consumers
                            // see the persisted endpoint name and any
                            // VpcSecurityGroupIds we recorded.
                            let n = v
                                .get("DBProxyEndpointName")
                                .and_then(|x| x.as_str())
                                .unwrap_or_default();
                            members.push_str(&format!(
                                "      <member>\n        <DBProxyEndpointName>{}</DBProxyEndpointName>\n      </member>\n",
                                xml_escape(n)
                            ));
                        }
                    }
                }
                Ok(xml_response("DescribeDBProxyEndpoints", format!("    <DBProxyEndpoints>\n{members}    </DBProxyEndpoints>"), &rid))
            }
            "DescribeDBProxyTargetGroups" => {
                let accounts = self.state.read();
                let state_opt = accounts.get(&aid);
                let filter_proxy = get_param(req, "DBProxyName");
                let mut members = String::new();
                if let Some(state) = state_opt {
                    if let Some(m) = state.extras.get("proxy_target_groups") {
                        for v in m.values() {
                            let proxy = v
                                .get("DBProxyName")
                                .and_then(|x| x.as_str())
                                .unwrap_or_default();
                            if let Some(want) = filter_proxy.as_deref() {
                                if proxy != want {
                                    continue;
                                }
                            }
                            let tgn = v
                                .get("TargetGroupName")
                                .and_then(|x| x.as_str())
                                .unwrap_or_default();
                            members.push_str(&format!(
                                "      <member>\n        <DBProxyName>{}</DBProxyName>\n        <TargetGroupName>{}</TargetGroupName>\n      </member>\n",
                                xml_escape(proxy), xml_escape(tgn)
                            ));
                        }
                    }
                }
                Ok(xml_response("DescribeDBProxyTargetGroups", format!("    <TargetGroups>\n{members}    </TargetGroups>"), &rid))
            }
            "DescribeDBProxyTargets" => {
                let proxy = get_param(req, "DBProxyName").ok_or_else(|| missing("DBProxyName"))?;
                let group = get_param(req, "TargetGroupName").unwrap_or_else(|| "default".to_string());
                let key = format!("{proxy}/{group}");
                let accounts = self.state_handle().read();
                let targets: Vec<Value> = accounts
                    .get(&aid)
                    .and_then(|s| s.extras.get("proxy_targets"))
                    .and_then(|m| m.get(&key))
                    .and_then(|v| v.as_array())
                    .cloned()
                    .unwrap_or_default();
                let members: String = targets.iter().map(db_proxy_target_xml).collect();
                Ok(xml_response("DescribeDBProxyTargets", format!("    <Targets>{members}</Targets>"), &rid))
            }
            "ModifyDBProxyTargetGroup" => {
                let proxy = get_param(req, "DBProxyName").ok_or_else(|| missing("DBProxyName"))?;
                let group = get_param(req, "TargetGroupName").unwrap_or_else(|| "default".to_string());
                let key = format!("{proxy}/{group}");
                let mut pool = serde_json::Map::new();
                if let Some(v) = get_param(req, "ConnectionPoolConfig.MaxConnectionsPercent").and_then(|s| s.parse::<i64>().ok()) {
                    pool.insert("MaxConnectionsPercent".to_string(), json!(v));
                }
                if let Some(v) = get_param(req, "ConnectionPoolConfig.MaxIdleConnectionsPercent").and_then(|s| s.parse::<i64>().ok()) {
                    pool.insert("MaxIdleConnectionsPercent".to_string(), json!(v));
                }
                if let Some(v) = get_param(req, "ConnectionPoolConfig.ConnectionBorrowTimeout").and_then(|s| s.parse::<i64>().ok()) {
                    pool.insert("ConnectionBorrowTimeout".to_string(), json!(v));
                }
                if let Some(v) = get_param(req, "ConnectionPoolConfig.SessionPinningFilters") {
                    pool.insert("SessionPinningFilters".to_string(), json!(v));
                }
                if let Some(v) = get_param(req, "ConnectionPoolConfig.InitQuery") {
                    pool.insert("InitQuery".to_string(), json!(v));
                }
                let entry = json!({
                    "DBProxyName": proxy,
                    "TargetGroupName": group,
                    "ConnectionPoolConfig": Value::Object(pool),
                });
                let mut accounts = write_state!();
                let state = accounts.get_or_create(&aid);
                store(&mut state.extras, "proxy_target_groups").insert(key, entry.clone());
                Ok(xml_response("ModifyDBProxyTargetGroup", format!("    <DBProxyTargetGroup>\n      <DBProxyName>{}</DBProxyName>\n      <TargetGroupName>{}</TargetGroupName>\n    </DBProxyTargetGroup>", xml_escape(&proxy), xml_escape(&group)), &rid))
            }
            "RegisterDBProxyTargets" => {
                let proxy = get_param(req, "DBProxyName").ok_or_else(|| missing("DBProxyName"))?;
                let group = get_param(req, "TargetGroupName").unwrap_or_else(|| "default".to_string());
                let key = format!("{proxy}/{group}");
                let instances = parse_member_list(req, "DBInstanceIdentifiers");
                let clusters = parse_member_list(req, "DBClusterIdentifiers");
                let new_targets: Vec<Value> = instances
                    .iter()
                    .map(|id| json!({"RdsResourceId": id, "Type": "RDS_INSTANCE", "Port": 3306, "Endpoint": format!("{id}.{region}.rds.amazonaws.com")}))
                    .chain(clusters.iter().map(|id| {
                        json!({"RdsResourceId": id, "Type": "TRACKED_CLUSTER", "Port": 3306, "Endpoint": format!("{id}.cluster-{region}.rds.amazonaws.com")})
                    }))
                    .collect();
                {
                    let mut accounts = write_state!();
                    let state = accounts.get_or_create(&aid);
                    let map = store(&mut state.extras, "proxy_targets");
                    let existing = map.entry(key).or_insert_with(|| json!([]));
                    if let Some(arr) = existing.as_array_mut() {
                        for t in &new_targets {
                            let rid_val = t["RdsResourceId"].as_str();
                            arr.retain(|e| e["RdsResourceId"].as_str() != rid_val);
                            arr.push(t.clone());
                        }
                    }
                }
                let members: String = new_targets.iter().map(db_proxy_target_xml).collect();
                Ok(xml_response("RegisterDBProxyTargets", format!("    <DBProxyTargets>{members}</DBProxyTargets>"), &rid))
            }
            "DeregisterDBProxyTargets" => {
                let proxy = get_param(req, "DBProxyName").ok_or_else(|| missing("DBProxyName"))?;
                let group = get_param(req, "TargetGroupName").unwrap_or_else(|| "default".to_string());
                let key = format!("{proxy}/{group}");
                let remove: Vec<String> = parse_member_list(req, "DBInstanceIdentifiers")
                    .into_iter()
                    .chain(parse_member_list(req, "DBClusterIdentifiers"))
                    .collect();
                {
                    let mut accounts = write_state!();
                    let state = accounts.get_or_create(&aid);
                    if let Some(arr) = state
                        .extras
                        .get_mut("proxy_targets")
                        .and_then(|m| m.get_mut(&key))
                        .and_then(|v| v.as_array_mut())
                    {
                        arr.retain(|e| {
                            e["RdsResourceId"]
                                .as_str()
                                .map(|r| !remove.iter().any(|x| x == r))
                                .unwrap_or(true)
                        });
                    }
                }
                xml_empty_action(&action, &rid)
            }

            // ── Security groups (legacy) ──
            "CreateDBSecurityGroup" | "AuthorizeDBSecurityGroupIngress" | "RevokeDBSecurityGroupIngress" => {
                let name = get_param(req, "DBSecurityGroupName").ok_or_else(|| missing("DBSecurityGroupName"))?;
                let entry = json!({"DBSecurityGroupName": name, "DBSecurityGroupDescription": get_param(req, "DBSecurityGroupDescription").unwrap_or_default(), "OwnerId": aid.clone()});
                let mut accounts = write_state!();
                let state = accounts.get_or_create(&aid);
                store(&mut state.extras, "security_groups").insert(name.clone(), entry.clone());
                Ok(xml_response(action.as_str(), security_group_xml(&entry), &rid))
            }
            "DeleteDBSecurityGroup" => {
                let name = get_param(req, "DBSecurityGroupName").ok_or_else(|| missing("DBSecurityGroupName"))?;
                let mut accounts = write_state!();
                let state = accounts.get_or_create(&aid);
                if let Some(m) = state.extras.get_mut("security_groups") { m.remove(&name); }
                xml_empty_action(&action, &rid)
            }
            "DescribeDBSecurityGroups" => list_extras_xml(self, &aid, "security_groups", "DBSecurityGroups", "DBSecurityGroup", "DescribeDBSecurityGroups", security_group_xml, &rid),

            // ── Option groups ──
            "CreateOptionGroup" | "CopyOptionGroup" => {
                let name = get_param(req, "OptionGroupName").or_else(|| get_param(req, "TargetOptionGroupIdentifier"))
                    .ok_or_else(|| missing("OptionGroupName"))?;
                let arn = Arn::new("rds", region, &aid, &format!("og:{name}")).to_string();
                let entry = json!({"OptionGroupName": name, "OptionGroupArn": arn, "EngineName": get_param(req, "EngineName").unwrap_or_else(|| "mysql".to_string()), "MajorEngineVersion": get_param(req, "MajorEngineVersion").unwrap_or_else(|| "8.0".to_string()), "OptionGroupDescription": get_param(req, "OptionGroupDescription").unwrap_or_default()});
                let mut accounts = write_state!();
                let state = accounts.get_or_create(&aid);
                store(&mut state.extras, "option_groups").insert(name.clone(), entry.clone());
                Ok(xml_response(action.as_str(), option_group_xml(&entry), &rid))
            }
            "ModifyOptionGroup" => {
                let name = get_param(req, "OptionGroupName").ok_or_else(|| missing("OptionGroupName"))?;
                let to_include = parse_options_to_include(req);
                let to_remove = parse_member_list(req, "OptionsToRemove");
                let mut accounts = write_state!();
                let state = accounts.get_or_create(&aid);
                let entry = state
                    .extras
                    .get_mut("option_groups")
                    .and_then(|m| m.get_mut(&name))
                    .ok_or_else(|| {
                        AwsServiceError::aws_error(
                            StatusCode::NOT_FOUND,
                            "OptionGroupNotFoundFault",
                            format!("OptionGroup {name} not found."),
                        )
                    })?;
                if let Some(obj) = entry.as_object_mut() {
                    // Maintain the effective Options list so DescribeOptionGroups
                    // reflects what was added/removed: upsert each included option
                    // by OptionName, then drop any names in OptionsToRemove.
                    let mut options = obj
                        .get("Options")
                        .and_then(|o| o.as_array())
                        .cloned()
                        .unwrap_or_default();
                    for inc in &to_include {
                        let name = inc["OptionName"].as_str().unwrap_or_default().to_string();
                        options.retain(|o| o["OptionName"].as_str() != Some(name.as_str()));
                        options.push(inc.clone());
                    }
                    if !to_remove.is_empty() {
                        options.retain(|o| {
                            o["OptionName"]
                                .as_str()
                                .map(|n| !to_remove.iter().any(|r| r == n))
                                .unwrap_or(true)
                        });
                    }
                    obj.insert("Options".to_string(), json!(options));
                }
                let updated = entry.clone();
                Ok(xml_response("ModifyOptionGroup", format!("    <OptionGroup>\n{}\n    </OptionGroup>", option_group_xml(&updated)), &rid))
            }
            "DeleteOptionGroup" => {
                let name = get_param(req, "OptionGroupName").ok_or_else(|| missing("OptionGroupName"))?;
                let mut accounts = write_state!();
                let state = accounts.get_or_create(&aid);
                if let Some(m) = state.extras.get_mut("option_groups") { m.remove(&name); }
                xml_empty_action(&action, &rid)
            }
            "DescribeOptionGroups" => {
                // RDS wraps each list element in its named member tag
                // (`<OptionGroup>`), not the generic `<member>`; the SDK
                // unmarshals an empty list otherwise. AWS also filters by name
                // and raises OptionGroupNotFoundFault for an unknown group.
                let wanted = get_param(req, "OptionGroupName");
                let accounts = self.state_handle().read();
                let groups: Vec<Value> = accounts
                    .get(&aid)
                    .and_then(|s| s.extras.get("option_groups"))
                    .map(|m| m.values().cloned().collect())
                    .unwrap_or_default();
                if let Some(name) = &wanted {
                    let found = groups
                        .iter()
                        .any(|g| g["OptionGroupName"].as_str() == Some(name.as_str()));
                    if !found {
                        return Err(AwsServiceError::aws_error(
                            StatusCode::NOT_FOUND,
                            "OptionGroupNotFoundFault",
                            format!("Specified OptionGroup: {name} not found."),
                        ));
                    }
                }
                let members = groups
                    .iter()
                    .filter(|g| {
                        wanted
                            .as_deref()
                            .is_none_or(|n| g["OptionGroupName"].as_str() == Some(n))
                    })
                    .map(|g| {
                        format!(
                            "        <OptionGroup>\n{}\n        </OptionGroup>",
                            option_group_xml(g)
                        )
                    })
                    .collect::<Vec<_>>()
                    .join("\n");
                Ok(xml_response(
                    "DescribeOptionGroups",
                    format!("    <OptionGroupsList>\n{members}\n    </OptionGroupsList>"),
                    &rid,
                ))
            }
            "DescribeOptionGroupOptions" => Ok(xml_response("DescribeOptionGroupOptions", "    <OptionGroupOptions/>".to_string(), &rid)),

            // ── Event subscriptions ──
            "CreateEventSubscription" => {
                let name = get_param(req, "SubscriptionName").ok_or_else(|| missing("SubscriptionName"))?;
                let arn = Arn::new("rds", region, &aid, &format!("es:{name}")).to_string();
                let source_ids = parse_member_list(req, "SourceIds");
                let event_categories = parse_member_list(req, "EventCategories");
                let entry = json!({"CustSubscriptionId": name, "CustomerAwsId": aid, "EventSubscriptionArn": arn, "SnsTopicArn": get_param(req, "SnsTopicArn").unwrap_or_default(), "SourceType": get_param(req, "SourceType").unwrap_or_default(), "Status": "active", "Enabled": true, "SourceIdsList": source_ids, "EventCategoriesList": event_categories});
                let mut accounts = write_state!();
                let state = accounts.get_or_create(&aid);
                store(&mut state.extras, "event_subscriptions").insert(name.clone(), entry.clone());
                Ok(xml_response("CreateEventSubscription", format!("    <EventSubscription>\n{}\n    </EventSubscription>", event_sub_xml(&entry)), &rid))
            }
            "ModifyEventSubscription" => {
                let name = get_param(req, "SubscriptionName").ok_or_else(|| missing("SubscriptionName"))?;
                let mut accounts = write_state!();
                let state = accounts.get_or_create(&aid);
                let entry = state
                    .extras
                    .get_mut("event_subscriptions")
                    .and_then(|m| m.get_mut(&name))
                    .ok_or_else(|| {
                        AwsServiceError::aws_error(
                            StatusCode::NOT_FOUND,
                            "SubscriptionNotFound",
                            format!("EventSubscription {name} not found."),
                        )
                    })?;
                if let Some(obj) = entry.as_object_mut() {
                    if let Some(v) = get_param(req, "SnsTopicArn") {
                        obj.insert("SnsTopicArn".to_string(), json!(v));
                    }
                    if let Some(v) = get_param(req, "SourceType") {
                        obj.insert("SourceType".to_string(), json!(v));
                    }
                    if let Some(v) = get_param(req, "Enabled") {
                        obj.insert("Enabled".to_string(), json!(v.eq_ignore_ascii_case("true")));
                    }
                }
                let updated = entry.clone();
                Ok(xml_response("ModifyEventSubscription", format!("    <EventSubscription>\n{}\n    </EventSubscription>", event_sub_xml(&updated)), &rid))
            }
            "DeleteEventSubscription" => {
                let name = get_param(req, "SubscriptionName").ok_or_else(|| missing("SubscriptionName"))?;
                let mut accounts = write_state!();
                let state = accounts.get_or_create(&aid);
                if let Some(m) = state.extras.get_mut("event_subscriptions") { m.remove(&name); }
                Ok(xml_response("DeleteEventSubscription", "    <EventSubscription/>".to_string(), &rid))
            }
            "DescribeEventSubscriptions" => {
                let wanted = get_param(req, "SubscriptionName");
                list_extras_named_xml(self, &aid, "event_subscriptions", "EventSubscriptionsList", "EventSubscription", "DescribeEventSubscriptions", event_sub_xml, wanted.as_deref(), "CustSubscriptionId", "SubscriptionNotFound", &rid)
            }
            "AddSourceIdentifierToSubscription" | "RemoveSourceIdentifierFromSubscription" => {
                let name = get_param(req, "SubscriptionName").ok_or_else(|| missing("SubscriptionName"))?;
                let source_id = get_param(req, "SourceIdentifier");
                let adding = action.as_str() == "AddSourceIdentifierToSubscription";
                let mut accounts = write_state!();
                let state = accounts.get_or_create(&aid);
                let entry = state
                    .extras
                    .get_mut("event_subscriptions")
                    .and_then(|m| m.get_mut(&name))
                    .ok_or_else(|| {
                        AwsServiceError::aws_error(
                            StatusCode::NOT_FOUND,
                            "SubscriptionNotFound",
                            format!("Subscription {name} not found."),
                        )
                    })?;
                if let (Some(obj), Some(sid)) = (entry.as_object_mut(), source_id) {
                    let list = obj
                        .entry("SourceIdsList".to_string())
                        .or_insert_with(|| json!([]));
                    if let Some(arr) = list.as_array_mut() {
                        arr.retain(|v| v.as_str() != Some(sid.as_str()));
                        if adding {
                            arr.push(json!(sid));
                        }
                    }
                }
                let updated = entry.clone();
                Ok(xml_response(action.as_str(), format!("    <EventSubscription>\n{}\n    </EventSubscription>", event_sub_xml(&updated)), &rid))
            }

            // ── Global clusters ──
            "CreateGlobalCluster" => {
                let id = get_param(req, "GlobalClusterIdentifier").ok_or_else(|| missing("GlobalClusterIdentifier"))?;
                let arn = Arn::global("rds", &aid, &format!("global-cluster:{id}")).to_string();
                let entry = json!({
                    "GlobalClusterIdentifier": id,
                    "GlobalClusterArn": arn,
                    "GlobalClusterResourceId": new_cluster_resource_id(),
                    "Endpoint": format!("{id}.global.{region}.rds.amazonaws.com"),
                    "Status": "available",
                    "Engine": get_param(req, "Engine").unwrap_or_else(|| "aurora-postgresql".to_string()),
                    "EngineVersion": get_param(req, "EngineVersion").unwrap_or_else(|| "16.4".to_string()),
                    "EngineLifecycleSupport": get_param(req, "EngineLifecycleSupport").unwrap_or_else(|| "open-source-rds-extended-support".to_string()),
                    "DatabaseName": get_param(req, "DatabaseName").unwrap_or_default(),
                    "DeletionProtection": get_param(req, "DeletionProtection").map(|v| v.eq_ignore_ascii_case("true")).unwrap_or(false),
                    "StorageEncrypted": get_param(req, "StorageEncrypted").map(|v| v.eq_ignore_ascii_case("true")).unwrap_or(false),
                });
                let mut accounts = write_state!();
                let state = accounts.get_or_create(&aid);
                store(&mut state.extras, "global_clusters").insert(id.clone(), entry.clone());
                Ok(xml_response("CreateGlobalCluster", format!("    <GlobalCluster>\n{}\n    </GlobalCluster>", global_cluster_xml(&entry)), &rid))
            }
            "ModifyGlobalCluster" | "FailoverGlobalCluster" | "SwitchoverGlobalCluster" | "RemoveFromGlobalCluster" => {
                let id = get_param(req, "GlobalClusterIdentifier").ok_or_else(|| missing("GlobalClusterIdentifier"))?;
                let new_id = get_param(req, "NewGlobalClusterIdentifier");
                let deletion_protection = get_param(req, "DeletionProtection")
                    .map(|v| v.eq_ignore_ascii_case("true"));
                let engine_version = get_param(req, "EngineVersion");
                let updated = {
                    let mut accounts = write_state!();
                    let state = accounts.get_or_create(&aid);
                    let map = state
                        .extras
                        .get_mut("global_clusters")
                        .ok_or_else(|| {
                            AwsServiceError::aws_error(
                                StatusCode::NOT_FOUND,
                                "GlobalClusterNotFoundFault",
                                format!("{id} not found."),
                            )
                        })?;
                    let mut entry = map.get(&id).cloned().ok_or_else(|| {
                        AwsServiceError::aws_error(
                            StatusCode::NOT_FOUND,
                            "GlobalClusterNotFoundFault",
                            format!("{id} not found."),
                        )
                    })?;
                    if let Some(obj) = entry.as_object_mut() {
                        if action.as_str() == "ModifyGlobalCluster" {
                            if let Some(dp) = deletion_protection {
                                obj.insert("DeletionProtection".to_string(), json!(dp));
                            }
                            if let Some(ev) = &engine_version {
                                obj.insert("EngineVersion".to_string(), json!(ev));
                            }
                            if let Some(nid) = &new_id {
                                obj.insert("GlobalClusterIdentifier".to_string(), json!(nid));
                            }
                        }
                    }
                    // A rename re-keys the stored map entry.
                    if action.as_str() == "ModifyGlobalCluster" {
                        if let Some(nid) = &new_id {
                            map.remove(&id);
                            map.insert(nid.clone(), entry.clone());
                        } else {
                            map.insert(id.clone(), entry.clone());
                        }
                    }
                    entry
                };
                Ok(xml_response(action.as_str(), format!("    <GlobalCluster>\n{}\n    </GlobalCluster>", global_cluster_xml(&updated)), &rid))
            }
            "DeleteGlobalCluster" => {
                let id = get_param(req, "GlobalClusterIdentifier").ok_or_else(|| missing("GlobalClusterIdentifier"))?;
                let mut accounts = write_state!();
                let state = accounts.get_or_create(&aid);
                if let Some(m) = state.extras.get_mut("global_clusters") { m.remove(&id); }
                Ok(xml_response("DeleteGlobalCluster", "    <GlobalCluster/>".to_string(), &rid))
            }
            "DescribeGlobalClusters" => {
                let wanted = get_param(req, "GlobalClusterIdentifier");
                // RDS names this list's member `<GlobalClusterMember>`, not the
                // usual singular-of-wrapper (`<GlobalCluster>`); the SDK
                // unmarshals an empty list with any other tag.
                list_extras_named_xml(self, &aid, "global_clusters", "GlobalClusters", "GlobalClusterMember", "DescribeGlobalClusters", global_cluster_xml, wanted.as_deref(), "GlobalClusterIdentifier", "GlobalClusterNotFoundFault", &rid)
            }

            // ── Integrations ──
            "CreateIntegration" => {
                let name = get_param(req, "IntegrationName").ok_or_else(|| missing("IntegrationName"))?;
                let arn = Arn::new("rds", region, &aid, &format!("integration:{name}")).to_string();
                let entry = json!({"IntegrationName": name, "IntegrationArn": arn, "Status": "active"});
                let mut accounts = write_state!();
                let state = accounts.get_or_create(&aid);
                store(&mut state.extras, "integrations").insert(name.clone(), entry.clone());
                Ok(xml_response("CreateIntegration", integration_xml(&entry), &rid))
            }
            "ModifyIntegration" => {
                let ident = get_param(req, "IntegrationIdentifier")
                    .or_else(|| get_param(req, "IntegrationName"))
                    .ok_or_else(|| missing("IntegrationIdentifier"))?;
                let data_filter = get_param(req, "DataFilter");
                let description = get_param(req, "Description");
                let new_name = get_param(req, "IntegrationName");
                let updated = {
                    let mut accounts = write_state!();
                    let state = accounts.get_or_create(&aid);
                    let map = state.extras.get_mut("integrations").ok_or_else(|| {
                        AwsServiceError::aws_error(
                            StatusCode::NOT_FOUND,
                            "IntegrationNotFoundFault",
                            format!("Integration {ident} not found."),
                        )
                    })?;
                    // The identifier may be the integration name or its ARN.
                    let key = map
                        .iter()
                        .find(|(k, v)| {
                            k.as_str() == ident || v["IntegrationArn"].as_str() == Some(ident.as_str())
                        })
                        .map(|(k, _)| k.clone())
                        .ok_or_else(|| {
                            AwsServiceError::aws_error(
                                StatusCode::NOT_FOUND,
                                "IntegrationNotFoundFault",
                                format!("Integration {ident} not found."),
                            )
                        })?;
                    let mut entry = map.get(&key).cloned().unwrap_or(json!({}));
                    if let Some(obj) = entry.as_object_mut() {
                        if let Some(v) = &data_filter {
                            obj.insert("DataFilter".to_string(), json!(v));
                        }
                        if let Some(v) = &description {
                            obj.insert("Description".to_string(), json!(v));
                        }
                        if let Some(v) = &new_name {
                            obj.insert("IntegrationName".to_string(), json!(v));
                        }
                    }
                    map.insert(key, entry.clone());
                    entry
                };
                Ok(xml_response("ModifyIntegration", format!("    <Integration>\n{}\n    </Integration>", integration_xml(&updated)), &rid))
            }
            "DeleteIntegration" => {
                let name = get_param(req, "IntegrationIdentifier").or_else(|| get_param(req, "IntegrationName")).ok_or_else(|| missing("IntegrationIdentifier"))?;
                let mut accounts = write_state!();
                let state = accounts.get_or_create(&aid);
                if let Some(m) = state.extras.get_mut("integrations") { m.remove(&name); }
                Ok(xml_response("DeleteIntegration", "    <Integration/>".to_string(), &rid))
            }
            "DescribeIntegrations" => list_extras_xml(self, &aid, "integrations", "Integrations", "Integration", "DescribeIntegrations", integration_xml, &rid),

            // ── Blue/Green deployments ──
            "CreateBlueGreenDeployment" => {
                let id = format!("bgd-{}", rand_id());
                let arn = Arn::new("rds", region, &aid, &format!("blue-green-deployment:{id}"))
                    .to_string();
                let source_arn = get_param(req, "Source")
                    .ok_or_else(|| missing("Source"))?;
                let source_id = source_arn
                    .rsplit(':')
                    .next()
                    .map(|s| s.to_string())
                    .unwrap_or_default();
                let target_id = get_param(req, "TargetDBInstanceName")
                    .unwrap_or_else(|| format!("{source_id}-green-{}", rand_id()));
                let mut accounts = write_state!();
                let state = accounts.get_or_create(&aid);
                let source_arn_full = if source_arn.starts_with("arn:") {
                    source_arn.clone()
                } else {
                    state.db_instance_arn(region, &source_id)
                };
                let target_arn = state.db_instance_arn(region, &target_id);
                // AWS accepts either a DBInstance ARN or an Aurora
                // DBCluster ARN as the BG source. Look up under both
                // the real instance store and the cluster map under
                // `state.extras["clusters"]`; absent both, surface
                // DBInstanceNotFound (matching what AWS emits for the
                // more common DBInstance source).
                let instance_exists = state.instances.contains_key(&source_id);
                let cluster_exists = state
                    .extras
                    .get("clusters")
                    .map(|m| m.contains_key(&source_id))
                    .unwrap_or(false);
                if !instance_exists && !cluster_exists {
                    return Err(AwsServiceError::aws_error(
                        StatusCode::NOT_FOUND,
                        "DBInstanceNotFound",
                        format!("DBInstance {source_id} not found."),
                    ));
                }
                // Cluster sources require their own provisioning path:
                // clone the source cluster entry under the green id and
                // record the cluster ARNs in the BG record so a later
                // SwitchoverBlueGreenDeployment can operate on something
                // real.
                let target_arn_for_record = if cluster_exists {
                    let source_cluster = state
                        .extras
                        .get("clusters")
                        .and_then(|m| m.get(&source_id))
                        .cloned();
                    if let Some(mut green_cluster) = source_cluster {
                        let green_arn =
                            Arn::new("rds", region, &aid, &format!("cluster:{target_id}"))
                                .to_string();
                        if let Some(obj) = green_cluster.as_object_mut() {
                            obj.insert(
                                "DBClusterIdentifier".to_string(),
                                json!(target_id.clone()),
                            );
                            obj.insert("DBClusterArn".to_string(), json!(green_arn.clone()));
                            obj.insert("Status".to_string(), json!("available"));
                        }
                        store(&mut state.extras, "clusters")
                            .insert(target_id.clone(), green_cluster);
                        green_arn
                    } else {
                        target_arn.clone()
                    }
                } else if let Some(source) = state.instances.get(&source_id).cloned() {
                    let mut green = source.clone();
                    green.db_instance_identifier = target_id.clone();
                    green.db_instance_arn = target_arn.clone();
                    green.read_replica_db_instance_identifiers = Vec::new();
                    green.read_replica_source_db_instance_identifier = Some(source_id.clone());
                    green.dbi_resource_id = format!("db-{}", uuid::Uuid::new_v4().simple());
                    state.instances.insert(target_id.clone(), green);
                    target_arn.clone()
                } else {
                    target_arn.clone()
                };
                let entry = json!({
                    "BlueGreenDeploymentIdentifier": id,
                    "BlueGreenDeploymentName": get_param(req, "BlueGreenDeploymentName").unwrap_or_else(|| "blue-green".to_string()),
                    "Status": "AVAILABLE",
                    "Source": source_arn_full,
                    "Target": target_arn_for_record,
                    "SourceDBInstanceIdentifier": source_id,
                    "TargetDBInstanceIdentifier": target_id,
                    "SourceIsCluster": cluster_exists && !instance_exists,
                    "BlueGreenDeploymentArn": arn,
                });
                store(&mut state.extras, "blue_green").insert(id.clone(), entry.clone());
                Ok(xml_response("CreateBlueGreenDeployment", blue_green_xml(&entry), &rid))
            }
            "SwitchoverBlueGreenDeployment" => {
                let id = get_param(req, "BlueGreenDeploymentIdentifier")
                    .ok_or_else(|| missing("BlueGreenDeploymentIdentifier"))?;
                let mut accounts = write_state!();
                let state = accounts.get_or_create(&aid);
                let entry = state
                    .extras
                    .get("blue_green")
                    .and_then(|m| m.get(&id))
                    .cloned()
                    .ok_or_else(|| {
                        AwsServiceError::aws_error(
                            StatusCode::NOT_FOUND,
                            "BlueGreenDeploymentNotFoundFault",
                            format!("BlueGreenDeployment {id} not found."),
                        )
                    })?;
                let source_id = entry["SourceDBInstanceIdentifier"]
                    .as_str()
                    .unwrap_or("")
                    .to_string();
                let target_id = entry["TargetDBInstanceIdentifier"]
                    .as_str()
                    .unwrap_or("")
                    .to_string();
                if !source_id.is_empty() && !target_id.is_empty() {
                    let blue = state.instances.get(&source_id).cloned();
                    let green = state.instances.get(&target_id).cloned();
                    if let (Some(mut b), Some(mut g)) = (blue, green) {
                        // Swap endpoints (and host_port) so callers
                        // pointing at the blue address now reach the
                        // green container, mirroring AWS BG cutover.
                        std::mem::swap(&mut b.endpoint_address, &mut g.endpoint_address);
                        std::mem::swap(&mut b.port, &mut g.port);
                        std::mem::swap(&mut b.host_port, &mut g.host_port);
                        std::mem::swap(&mut b.container_id, &mut g.container_id);
                        // Green is now the writer; clear its replica
                        // pointer back at the old blue.
                        g.read_replica_source_db_instance_identifier = None;
                        state.instances.insert(source_id.clone(), b);
                        state.instances.insert(target_id.clone(), g);
                    }
                }
                if let Some(map) = state.extras.get_mut("blue_green") {
                    if let Some(e) = map.get_mut(&id) {
                        if let Some(obj) = e.as_object_mut() {
                            obj.insert("Status".to_string(), json!("SWITCHOVER_COMPLETED"));
                        }
                    }
                }
                let updated = state
                    .extras
                    .get("blue_green")
                    .and_then(|m| m.get(&id))
                    .cloned()
                    .unwrap_or(entry);
                Ok(xml_response(
                    "SwitchoverBlueGreenDeployment",
                    blue_green_xml(&updated),
                    &rid,
                ))
            }
            "DeleteBlueGreenDeployment" => {
                let id = get_param(req, "BlueGreenDeploymentIdentifier")
                    .ok_or_else(|| missing("BlueGreenDeploymentIdentifier"))?;
                let delete_target = get_param(req, "DeleteTarget")
                    .map(|v| v.eq_ignore_ascii_case("true"))
                    .unwrap_or(false);
                let mut accounts = write_state!();
                let state = accounts.get_or_create(&aid);
                let entry = state
                    .extras
                    .get_mut("blue_green")
                    .and_then(|m| m.remove(&id))
                    .ok_or_else(|| {
                        AwsServiceError::aws_error(
                            StatusCode::NOT_FOUND,
                            "BlueGreenDeploymentNotFoundFault",
                            format!("BlueGreenDeployment {id} not found."),
                        )
                    })?;
                if delete_target {
                    if let Some(target_id) = entry["TargetDBInstanceIdentifier"].as_str() {
                        state.instances.remove(target_id);
                    }
                }
                Ok(xml_response(
                    "DeleteBlueGreenDeployment",
                    blue_green_xml(&entry),
                    &rid,
                ))
            }
            "DescribeBlueGreenDeployments" => list_extras_xml(self, &aid, "blue_green", "BlueGreenDeployments", "member", "DescribeBlueGreenDeployments", blue_green_xml, &rid),

            // ── Shard groups ──
            "CreateDBShardGroup" => {
                let id = get_param(req, "DBShardGroupIdentifier").ok_or_else(|| missing("DBShardGroupIdentifier"))?;
                let mut entry = json!({"DBShardGroupIdentifier": id, "Status": "available"});
                if let Some(obj) = entry.as_object_mut() {
                    if let Some(cluster) = get_param(req, "DBClusterIdentifier") {
                        obj.insert("DBClusterIdentifier".to_string(), json!(cluster));
                    }
                    apply_shard_group_capacity(obj, req);
                }
                let mut accounts = write_state!();
                let state = accounts.get_or_create(&aid);
                store(&mut state.extras, "shard_groups").insert(id.clone(), entry.clone());
                Ok(xml_response("CreateDBShardGroup", shard_group_xml(&entry), &rid))
            }
            "ModifyDBShardGroup" => {
                let id = get_param(req, "DBShardGroupIdentifier").ok_or_else(|| missing("DBShardGroupIdentifier"))?;
                let entry = {
                    let mut accounts = write_state!();
                    let state = accounts.get_or_create(&aid);
                    let entry = state
                        .extras
                        .get_mut("shard_groups")
                        .and_then(|m| m.get_mut(&id))
                        .ok_or_else(|| {
                            AwsServiceError::aws_error(
                                StatusCode::NOT_FOUND,
                                "DBShardGroupNotFound",
                                format!("DBShardGroup {id} not found."),
                            )
                        })?;
                    if let Some(obj) = entry.as_object_mut() {
                        apply_shard_group_capacity(obj, req);
                    }
                    entry.clone()
                };
                Ok(xml_response("ModifyDBShardGroup", shard_group_xml(&entry), &rid))
            }
            "RebootDBShardGroup" => {
                let id = get_param(req, "DBShardGroupIdentifier").ok_or_else(|| missing("DBShardGroupIdentifier"))?;
                let entry = self
                    .state_handle()
                    .read()
                    .get(&aid)
                    .and_then(|s| s.extras.get("shard_groups"))
                    .and_then(|m| m.get(&id))
                    .cloned()
                    .unwrap_or_else(|| json!({"DBShardGroupIdentifier": id, "Status": "available"}));
                Ok(xml_response("RebootDBShardGroup", shard_group_xml(&entry), &rid))
            }
            "DeleteDBShardGroup" => {
                let id = get_param(req, "DBShardGroupIdentifier").ok_or_else(|| missing("DBShardGroupIdentifier"))?;
                let mut accounts = write_state!();
                let state = accounts.get_or_create(&aid);
                if let Some(m) = state.extras.get_mut("shard_groups") { m.remove(&id); }
                Ok(xml_response("DeleteDBShardGroup", "    <DBShardGroup/>".to_string(), &rid))
            }
            "DescribeDBShardGroups" => list_extras_xml(self, &aid, "shard_groups", "DBShardGroups", "DBShardGroup", "DescribeDBShardGroups", shard_group_xml, &rid),

            // ── Custom engine versions ──
            "CreateCustomDBEngineVersion" | "ModifyCustomDBEngineVersion" => {
                let v = get_param(req, "EngineVersion").unwrap_or_else(|| "1.0".to_string());
                let engine = get_param(req, "Engine").unwrap_or_else(|| "custom-oracle-ee".to_string());
                let entry = json!({"Engine": engine, "EngineVersion": v, "Status": "available"});
                let mut accounts = write_state!();
                let state = accounts.get_or_create(&aid);
                store(&mut state.extras, "custom_engine_versions").insert(v.clone(), entry.clone());
                Ok(xml_response(action.as_str(), engine_version_xml(&entry), &rid))
            }
            "DeleteCustomDBEngineVersion" => Ok(xml_response("DeleteCustomDBEngineVersion", "    <DBEngineVersion/>".to_string(), &rid)),

            // ── Tenant databases ──
            "CreateTenantDatabase" => {
                let name = get_param(req, "TenantDBName").ok_or_else(|| missing("TenantDBName"))?;
                let entry = json!({"TenantDBName": name, "Status": "available"});
                let mut accounts = write_state!();
                let state = accounts.get_or_create(&aid);
                store(&mut state.extras, "tenant_dbs").insert(name.clone(), entry.clone());
                Ok(xml_response("CreateTenantDatabase", tenant_db_xml(&entry), &rid))
            }
            "ModifyTenantDatabase" => {
                let _instance = get_param(req, "DBInstanceIdentifier").ok_or_else(|| missing("DBInstanceIdentifier"))?;
                let name = get_param(req, "TenantDBName").ok_or_else(|| missing("TenantDBName"))?;
                let new_name = get_param(req, "NewTenantDBName");
                let new_password = get_param(req, "MasterUserPassword");
                let mut accounts = write_state!();
                let state = accounts.get_or_create(&aid);
                let entry = state
                    .extras
                    .get_mut("tenant_dbs")
                    .and_then(|m| m.remove(&name))
                    .ok_or_else(|| {
                        AwsServiceError::aws_error(
                            StatusCode::NOT_FOUND,
                            "TenantDatabaseNotFound",
                            format!("TenantDatabase {name} not found."),
                        )
                    })?;
                let mut updated = entry;
                let final_name = new_name.clone().unwrap_or_else(|| name.clone());
                if let Some(obj) = updated.as_object_mut() {
                    obj.insert("TenantDBName".to_string(), json!(final_name));
                    if let Some(p) = new_password {
                        obj.insert("MasterUserPassword".to_string(), json!(p));
                    }
                }
                store(&mut state.extras, "tenant_dbs").insert(final_name, updated.clone());
                Ok(xml_response("ModifyTenantDatabase", format!("    <TenantDatabase>\n{}\n    </TenantDatabase>", tenant_db_xml(&updated)), &rid))
            }
            "DeleteTenantDatabase" => {
                let name = get_param(req, "TenantDBName").ok_or_else(|| missing("TenantDBName"))?;
                let mut accounts = write_state!();
                let state = accounts.get_or_create(&aid);
                if let Some(m) = state.extras.get_mut("tenant_dbs") { m.remove(&name); }
                Ok(xml_response("DeleteTenantDatabase", "    <TenantDatabase/>".to_string(), &rid))
            }
            "DescribeTenantDatabases" => list_extras_xml(self, &aid, "tenant_dbs", "TenantDatabases", "TenantDatabase", "DescribeTenantDatabases", tenant_db_xml, &rid),
            "DescribeDBSnapshotTenantDatabases" => Ok(xml_response("DescribeDBSnapshotTenantDatabases", "    <DBSnapshotTenantDatabases/>".to_string(), &rid)),

            // ── Export tasks ──
            "StartExportTask" => {
                let id = get_param(req, "ExportTaskIdentifier").ok_or_else(|| missing("ExportTaskIdentifier"))?;
                let entry = json!({"ExportTaskIdentifier": id, "Status": "STARTING"});
                let mut accounts = write_state!();
                let state = accounts.get_or_create(&aid);
                store(&mut state.extras, "export_tasks").insert(id.clone(), entry.clone());
                Ok(xml_response("StartExportTask", export_task_xml(&entry), &rid))
            }
            "CancelExportTask" => Ok(xml_response("CancelExportTask", "    <ExportTask/>".to_string(), &rid)),
            "DescribeExportTasks" => list_extras_xml(self, &aid, "export_tasks", "ExportTasks", "ExportTask", "DescribeExportTasks", export_task_xml, &rid),

            // ── Activity stream ──
            // ResourceArn names either an Aurora DB cluster or an RDS DB
            // instance; the stream state is persisted on whichever exists so
            // DescribeDBClusters / DescribeDBInstances round-trips it.
            "StartActivityStream" => {
                let resource_arn =
                    get_param(req, "ResourceArn").ok_or_else(|| missing("ResourceArn"))?;
                let kms_input = get_param(req, "KmsKeyId").unwrap_or_default();
                let kms_arn = format_kms_arn(&kms_input, region, &aid);
                let mode = get_param(req, "Mode").unwrap_or_else(|| "async".to_string());
                let (_, id) = parse_rds_resource_arn(&resource_arn);
                let stream = format!("aws-rds-das-{id}");
                {
                    let mut accounts = write_state!();
                    let state = accounts.get_or_create(&aid);
                    let cfg = crate::state::ActivityStreamConfig {
                        status: "started".to_string(),
                        mode: Some(mode.clone()),
                        kms_key_id: if kms_arn.is_empty() {
                            None
                        } else {
                            Some(kms_arn.clone())
                        },
                        kinesis_stream_name: Some(stream.clone()),
                    };
                    if !apply_activity_stream(state, &id, Some(cfg)) {
                        return Err(
                            crate::service::service_helpers::db_instance_not_found(&id),
                        );
                    }
                }
                Ok(xml_response("StartActivityStream", format!("    <Status>started</Status>\n    <KmsKeyId>{}</KmsKeyId>\n    <KinesisStreamName>{}</KinesisStreamName>\n    <Mode>{}</Mode>\n    <ApplyImmediately>true</ApplyImmediately>", xml_escape(&kms_arn), xml_escape(&stream), xml_escape(&mode)), &rid))
            }
            "StopActivityStream" => {
                let resource_arn =
                    get_param(req, "ResourceArn").ok_or_else(|| missing("ResourceArn"))?;
                let (_, id) = parse_rds_resource_arn(&resource_arn);
                let (kms, kinesis) = {
                    let mut accounts = write_state!();
                    let state = accounts.get_or_create(&aid);
                    // Echo the pre-stop kms/kinesis as AWS does, then clear.
                    let prev = read_activity_stream(state, &id);
                    if !apply_activity_stream(state, &id, None) {
                        return Err(
                            crate::service::service_helpers::db_instance_not_found(&id),
                        );
                    }
                    (
                        prev.as_ref()
                            .and_then(|c| c.kms_key_id.clone())
                            .unwrap_or_default(),
                        prev.and_then(|c| c.kinesis_stream_name).unwrap_or_default(),
                    )
                };
                Ok(xml_response("StopActivityStream", format!("    <Status>stopped</Status>\n    <KmsKeyId>{}</KmsKeyId>\n    <KinesisStreamName>{}</KinesisStreamName>", xml_escape(&kms), xml_escape(&kinesis)), &rid))
            }
            "ModifyActivityStream" => {
                // ResourceArn is optional in the Smithy model, so an absent or
                // unknown value must surface the declared ResourceNotFoundFault
                // rather than an undeclared InvalidParameterValue.
                let resource_arn = get_param(req, "ResourceArn").unwrap_or_default();
                let (_, id) = parse_rds_resource_arn(&resource_arn);
                let (status, kms, kinesis, mode) = {
                    let mut accounts = write_state!();
                    let state = accounts.get_or_create(&aid);
                    let mut cfg = read_activity_stream(state, &id).unwrap_or_else(|| {
                        crate::state::ActivityStreamConfig {
                            status: "started".to_string(),
                            ..Default::default()
                        }
                    });
                    if cfg.status.is_empty() {
                        cfg.status = "started".to_string();
                    }
                    if let Some(m) = get_param(req, "Mode") {
                        cfg.mode = Some(m);
                    }
                    let echo = (
                        cfg.status.clone(),
                        cfg.kms_key_id.clone().unwrap_or_default(),
                        cfg.kinesis_stream_name.clone().unwrap_or_default(),
                        cfg.mode.clone().unwrap_or_default(),
                    );
                    if !apply_activity_stream(state, &id, Some(cfg)) {
                        return Err(resource_not_found(&resource_arn));
                    }
                    echo
                };
                Ok(xml_response("ModifyActivityStream", format!("    <Status>{}</Status>\n    <KmsKeyId>{}</KmsKeyId>\n    <KinesisStreamName>{}</KinesisStreamName>\n    <Mode>{}</Mode>", xml_escape(&status), xml_escape(&kms), xml_escape(&kinesis), xml_escape(&mode)), &rid))
            }

            // ── Database read replicas ──
            "PromoteReadReplica" => promote_read_replica_action(self, &aid, req, &rid),
            "SwitchoverReadReplica" => switchover_read_replica_action(self, &aid, req, &rid),
            "StartDBInstanceAutomatedBackupsReplication" | "StopDBInstanceAutomatedBackupsReplication" => Ok(xml_response(action.as_str(), "    <DBInstanceAutomatedBackup/>".to_string(), &rid)),
            "DeleteDBInstanceAutomatedBackup" => Ok(xml_response("DeleteDBInstanceAutomatedBackup", "    <DBInstanceAutomatedBackup/>".to_string(), &rid)),
            "DescribeDBInstanceAutomatedBackups" => Ok(xml_response("DescribeDBInstanceAutomatedBackups", "    <DBInstanceAutomatedBackups/>".to_string(), &rid)),

            // ── Roles ──
            "AddRoleToDBCluster" | "RemoveRoleFromDBCluster" | "AddRoleToDBInstance" | "RemoveRoleFromDBInstance" => xml_empty_action(&action, &rid),

            // ── Pending maintenance ──
            "ApplyPendingMaintenanceAction" => {
                let resource = get_param(req, "ResourceIdentifier").ok_or_else(|| missing("ResourceIdentifier"))?;
                let _action_kind = get_param(req, "ApplyAction").ok_or_else(|| missing("ApplyAction"))?;
                let _opt_in = get_param(req, "OptInType").ok_or_else(|| missing("OptInType"))?;
                let (kind, id) = parse_rds_resource_arn(&resource);
                let mut accounts = write_state!();
                let state = accounts.get_or_create(&aid);
                match kind {
                    Some("db") => {
                        if let Some(inst) = state.instances.get_mut(&id) {
                            if let Some(pending) = inst.pending_modified_values.take() {
                                crate::service::apply_pending_to_instance(inst, pending);
                            }
                        }
                    }
                    Some("cluster") => {
                        if let Some(map) = state.extras.get_mut("clusters") {
                            if let Some(entry) = map.get_mut(&id) {
                                if let Some(obj) = entry.as_object_mut() {
                                    obj.remove("PendingModifiedValues");
                                }
                            }
                        }
                    }
                    _ => {}
                }
                Ok(xml_response("ApplyPendingMaintenanceAction", format!("    <ResourcePendingMaintenanceActions>\n      <ResourceIdentifier>{}</ResourceIdentifier>\n      <PendingMaintenanceActionDetails/>\n    </ResourcePendingMaintenanceActions>", xml_escape(&resource)), &rid))
            }
            "DescribePendingMaintenanceActions" => Ok(xml_response("DescribePendingMaintenanceActions", "    <PendingMaintenanceActions/>".to_string(), &rid)),

            // ── Reserved instances ──
            "PurchaseReservedDBInstancesOffering" => Ok(xml_response("PurchaseReservedDBInstancesOffering", "    <ReservedDBInstance/>".to_string(), &rid)),
            "DescribeReservedDBInstances" => Ok(xml_response("DescribeReservedDBInstances", "    <ReservedDBInstances/>".to_string(), &rid)),
            "DescribeReservedDBInstancesOfferings" => Ok(xml_response("DescribeReservedDBInstancesOfferings", "    <ReservedDBInstancesOfferings/>".to_string(), &rid)),

            // ── Snapshots / restores / copy ──
            "CopyDBSnapshot" => {
                let target_id = get_param(req, "TargetDBSnapshotIdentifier")
                    .ok_or_else(|| missing("TargetDBSnapshotIdentifier"))?;
                let source_id = get_param(req, "SourceDBSnapshotIdentifier")
                    .ok_or_else(|| missing("SourceDBSnapshotIdentifier"))?;
                // Source may be passed as a bare id or a full ARN; key state
                // by the trailing identifier segment either way, and keep
                // the ARN's account so a foreign ARN can't alias onto this
                // account's same-named snapshot.
                let source_key = normalized_identifier(Some(source_id.clone()), "snapshot")
                    .unwrap_or_else(|| source_id.clone());
                let source_owner = identifier_account(&source_id);
                let option_group_name = get_param(req, "OptionGroupName");
                let kms_key_id = get_param(req, "KmsKeyId");
                let (snapshot, arn) = {
                    let mut accounts = write_state!();
                    let state = accounts.get_or_create(&aid);
                    let aid = aid.clone();
                    if state.snapshots.contains_key(&target_id) {
                        return Err(AwsServiceError::aws_error(
                            StatusCode::BAD_REQUEST,
                            "DBSnapshotAlreadyExists",
                            format!("DBSnapshot {target_id} already exists."),
                        ));
                    }
                    // AWS supports copying a snapshot another account
                    // shared with you, so fall back to a shared source --
                    // only from the account the ARN names, and only when
                    // it really was shared with this caller.
                    let owned = source_owner
                        .as_deref()
                        .is_none_or(|account| account == aid)
                        .then(|| state.snapshots.get(&source_key).cloned())
                        .flatten();
                    let mut snapshot = match owned {
                        Some(snapshot) => snapshot,
                        // The ARN form is required to reach another
                        // account's shared snapshot, here as elsewhere.
                        None => accounts
                            .iter()
                            .filter(|(owner, _)| *owner != aid)
                            .filter(|(owner, _)| {
                                source_owner.as_deref() == Some(*owner)
                            })
                            .find_map(|(_, other)| {
                                other
                                    .snapshots
                                    .get(&source_key)
                                    .filter(|snapshot| {
                                        snapshot.snapshot_attributes.get("restore").is_some_and(
                                            |targets| {
                                                targets.iter().any(|t| *t == aid || t == "all")
                                            },
                                        )
                                    })
                                    .cloned()
                            })
                            .ok_or_else(|| {
                                crate::service::service_helpers::db_snapshot_not_found(&source_id)
                            })?,
                    };
                    let state = accounts.get_or_create(&aid);
                    let arn = state.db_snapshot_arn(region, &target_id);
                    snapshot.db_snapshot_identifier = target_id.clone();
                    snapshot.db_snapshot_arn = arn.clone();
                    snapshot.snapshot_create_time = chrono::Utc::now();
                    snapshot.snapshot_type = "manual".to_string();
                    snapshot.status = "available".to_string();
                    snapshot.percent_progress = Some(100);
                    if let Some(og) = option_group_name {
                        snapshot.option_group_name = Some(og);
                    }
                    if let Some(kms) = kms_key_id {
                        snapshot.encrypted = true;
                        snapshot.kms_key_id = Some(format_kms_arn(&kms, region, &aid));
                    }
                    // A copy is a fresh sharing surface; it does not inherit
                    // the source snapshot's restore attributes.
                    snapshot.snapshot_attributes = BTreeMap::new();
                    state.snapshots.insert(target_id.clone(), snapshot.clone());
                    (snapshot, arn)
                };
                self.emit_event(
                    RdsSourceType::DbSnapshot,
                    &target_id,
                    &arn,
                    "RDS-EVENT-0042",
                    &["creation"],
                    "Manual snapshot created",
                );
                Ok(xml_response(
                    "CopyDBSnapshot",
                    format!(
                        "    <DBSnapshot>{}</DBSnapshot>",
                        crate::service::service_helpers::db_snapshot_xml(&snapshot)
                    ),
                    &rid,
                ))
            }
            "CopyDBParameterGroup" => {
                let target = get_param(req, "TargetDBParameterGroupIdentifier")
                    .ok_or_else(|| missing("TargetDBParameterGroupIdentifier"))?;
                let source = get_param(req, "SourceDBParameterGroupIdentifier")
                    .ok_or_else(|| missing("SourceDBParameterGroupIdentifier"))?;
                let source_key = source.rsplit(':').next().unwrap_or(&source).to_string();
                let description = get_param(req, "TargetDBParameterGroupDescription");
                let group = {
                    let mut accounts = write_state!();
                    let state = accounts.get_or_create(&aid);
                    if state.parameter_groups.contains_key(&target) {
                        return Err(AwsServiceError::aws_error(
                            StatusCode::BAD_REQUEST,
                            "DBParameterGroupAlreadyExists",
                            format!("DBParameterGroup {target} already exists."),
                        ));
                    }
                    let mut group = state
                        .parameter_groups
                        .get(&source_key)
                        .cloned()
                        .ok_or_else(|| {
                            AwsServiceError::aws_error(
                                StatusCode::NOT_FOUND,
                                "DBParameterGroupNotFound",
                                format!("DBParameterGroup {source} not found."),
                            )
                        })?;
                    group.db_parameter_group_name = target.clone();
                    group.db_parameter_group_arn = state.db_parameter_group_arn(region, &target);
                    if let Some(desc) = description {
                        group.description = desc;
                    }
                    group.tags = Vec::new();
                    state.parameter_groups.insert(target.clone(), group.clone());
                    group
                };
                Ok(xml_response(
                    "CopyDBParameterGroup",
                    format!(
                        "    <DBParameterGroup>{}</DBParameterGroup>",
                        crate::service::service_helpers::db_parameter_group_xml(&group)
                    ),
                    &rid,
                ))
            }
            "DescribeDBParameters" => Ok(xml_response("DescribeDBParameters", "    <Parameters/>".to_string(), &rid)),
            "ResetDBParameterGroup" => {
                let name = get_param(req, "DBParameterGroupName").ok_or_else(|| missing("DBParameterGroupName"))?;
                // Resetting a parameter flips its source back from `user` to
                // `engine-default`, so we drop it from the user-set map. With
                // `ResetAllParameters=true` (or no explicit list) every user
                // value is cleared; otherwise only the named parameters are.
                let reset_all = get_param(req, "ResetAllParameters")
                    .map(|v| v.eq_ignore_ascii_case("true"))
                    .unwrap_or(false);
                let named: Vec<String> = crate::service::parse_db_parameter_members(req)
                    .into_iter()
                    .map(|p| p.name)
                    .collect();
                {
                    let mut accounts = write_state!();
                    let state = accounts.get_or_create(&aid);
                    if let Some(group) = state.parameter_groups.get_mut(&name) {
                        if reset_all || named.is_empty() {
                            group.parameters.clear();
                            group.parameter_apply_methods.clear();
                        } else {
                            for n in &named {
                                group.parameters.remove(n);
                                group.parameter_apply_methods.remove(n);
                            }
                        }
                    }
                }
                Ok(xml_response("ResetDBParameterGroup", format!("    <DBParameterGroupName>{}</DBParameterGroupName>", xml_escape(&name)), &rid))
            }
            "DescribeEngineDefaultParameters" => {
                let family = get_param(req, "DBParameterGroupFamily").unwrap_or_else(|| "postgres16".to_string());
                let mut members = String::new();
                for default in crate::state::engine_default_parameters(&family) {
                    members.push_str(&crate::service::render_engine_default_parameter_xml(default));
                }
                let body = format!(
                    "    <EngineDefaults>\n      <DBParameterGroupFamily>{}</DBParameterGroupFamily>\n      <Parameters>\n{}      </Parameters>\n    </EngineDefaults>",
                    xml_escape(&family),
                    members,
                );
                Ok(xml_response("DescribeEngineDefaultParameters", body, &rid))
            }
            "DescribeDBSnapshotAttributes" => {
                let raw = get_param(req, "DBSnapshotIdentifier")
                    .ok_or_else(|| missing("DBSnapshotIdentifier"))?;
                // An ARN naming another account addresses THEIR snapshot,
                // which this op cannot act on; resolving it by bare id
                // would silently hit this account's same-named one.
                if !addresses_own_account(&raw, &aid) {
                    return Err(crate::service::service_helpers::db_snapshot_not_found(&raw));
                }
                // A wrong-type ARN names no DB snapshot: the declared
                // DBSnapshotNotFoundFault, not an undeclared
                // InvalidParameterValue.
                let id = normalized_identifier(Some(raw.clone()), "snapshot").ok_or_else(|| {
                    crate::service::service_helpers::db_snapshot_not_found(&raw)
                })?;
                let attrs = {
                    let accounts = self.state_handle().read();
                    let snapshot = accounts
                        .get(&aid)
                        .and_then(|s| s.snapshots.get(&id))
                        .ok_or_else(|| crate::service::service_helpers::db_snapshot_not_found(&id))?;
                    snapshot.snapshot_attributes.clone()
                };
                Ok(xml_response(
                    "DescribeDBSnapshotAttributes",
                    snapshot_attributes_result_xml(&id, &attrs),
                    &rid,
                ))
            }
            "ModifyDBSnapshotAttribute" => {
                let raw = get_param(req, "DBSnapshotIdentifier")
                    .ok_or_else(|| missing("DBSnapshotIdentifier"))?;
                // An ARN naming another account addresses THEIR snapshot,
                // which this op cannot act on; resolving it by bare id
                // would silently hit this account's same-named one.
                if !addresses_own_account(&raw, &aid) {
                    return Err(crate::service::service_helpers::db_snapshot_not_found(&raw));
                }
                // A wrong-type ARN names no DB snapshot: the declared
                // DBSnapshotNotFoundFault, not an undeclared
                // InvalidParameterValue.
                let id = normalized_identifier(Some(raw.clone()), "snapshot").ok_or_else(|| {
                    crate::service::service_helpers::db_snapshot_not_found(&raw)
                })?;
                let attribute_name = get_param(req, "AttributeName")
                    .ok_or_else(|| missing("AttributeName"))?;
                let to_add = parse_attribute_values(req, "ValuesToAdd");
                let to_remove = parse_attribute_values(req, "ValuesToRemove");
                // AWS rejects a value present in both lists, but
                // `InvalidParameterCombination` is not even a shape in the
                // RDS model, so emitting it here would be an undeclared
                // error (see the module docs on `crate::filters`). Resolve
                // it deterministically instead: removals first, then
                // additions, so the value ends up added.
                let attrs = {
                    let mut accounts = write_state!();
                    let state = accounts.get_or_create(&aid);
                    let snapshot = state
                        .snapshots
                        .get_mut(&id)
                        .ok_or_else(|| crate::service::service_helpers::db_snapshot_not_found(&id))?;
                    let values = snapshot
                        .snapshot_attributes
                        .entry(attribute_name.clone())
                        .or_default();
                    values.retain(|v| !to_remove.contains(v));
                    for v in to_add {
                        if !values.contains(&v) {
                            values.push(v);
                        }
                    }
                    // Drop the attribute entirely once it has no values so
                    // Describe reports an empty (unshared) snapshot rather
                    // than an empty `restore` list, matching AWS.
                    if values.is_empty() {
                        snapshot.snapshot_attributes.remove(&attribute_name);
                    }
                    snapshot.snapshot_attributes.clone()
                };
                Ok(xml_response(
                    "ModifyDBSnapshotAttribute",
                    snapshot_attributes_result_xml(&id, &attrs),
                    &rid,
                ))
            }
            "ModifyDBSnapshot" => {
                let raw = get_param(req, "DBSnapshotIdentifier")
                    .ok_or_else(|| missing("DBSnapshotIdentifier"))?;
                // An ARN naming another account addresses THEIR snapshot,
                // which this op cannot act on; resolving it by bare id
                // would silently hit this account's same-named one.
                if !addresses_own_account(&raw, &aid) {
                    return Err(crate::service::service_helpers::db_snapshot_not_found(&raw));
                }
                // A wrong-type ARN names no DB snapshot: the declared
                // DBSnapshotNotFoundFault, not an undeclared
                // InvalidParameterValue.
                let id = normalized_identifier(Some(raw.clone()), "snapshot").ok_or_else(|| {
                    crate::service::service_helpers::db_snapshot_not_found(&raw)
                })?;
                let engine_version = get_param(req, "EngineVersion");
                let option_group_name = get_param(req, "OptionGroupName");
                let snapshot = {
                    let mut accounts = write_state!();
                    let state = accounts.get_or_create(&aid);
                    let snapshot = state
                        .snapshots
                        .get_mut(&id)
                        .ok_or_else(|| crate::service::service_helpers::db_snapshot_not_found(&id))?;
                    if let Some(v) = engine_version {
                        snapshot.engine_version = v;
                    }
                    if let Some(og) = option_group_name {
                        snapshot.option_group_name = Some(og);
                    }
                    snapshot.clone()
                };
                Ok(xml_response(
                    "ModifyDBSnapshot",
                    format!(
                        "    <DBSnapshot>{}</DBSnapshot>",
                        crate::service::service_helpers::db_snapshot_xml(&snapshot)
                    ),
                    &rid,
                ))
            }
            "RestoreDBClusterFromS3" => {
                let id = get_param(req, "DBClusterIdentifier")
                    .ok_or_else(|| missing("DBClusterIdentifier"))?;
                let arn = Arn::new("rds", region, &aid, &format!("cluster:{id}")).to_string();
                let engine =
                    get_param(req, "Engine").unwrap_or_else(|| "aurora-mysql".to_string());
                let port = get_param(req, "Port")
                    .and_then(|p| p.parse::<i64>().ok())
                    .unwrap_or(if engine.contains("postgresql") { 5432 } else { 3306 });
                let entry = json!({
                    "DBClusterIdentifier": id, "DBClusterArn": arn,
                    "DbClusterResourceId": new_cluster_resource_id(),
                    "Status": "available", "Engine": engine,
                    "EngineVersion": get_param(req, "EngineVersion").unwrap_or_else(|| "8.0.mysql_aurora.3.04.0".to_string()),
                    "Endpoint": format!("{id}.cluster-xxx.{region}.rds.amazonaws.com"),
                    "ReaderEndpoint": format!("{id}.cluster-ro-xxx.{region}.rds.amazonaws.com"),
                    "Port": port,
                    "MasterUsername": get_param(req, "MasterUsername").unwrap_or_else(|| "admin".to_string()),
                });
                {
                    let mut accounts = write_state!();
                    let state = accounts.get_or_create(&aid);
                    if state.extras.get("clusters").is_some_and(|m| m.contains_key(&id)) {
                        return Err(cluster_already_exists(&id));
                    }
                    store(&mut state.extras, "clusters").insert(id.clone(), entry.clone());
                }
                self.emit_event(
                    RdsSourceType::DbCluster,
                    &id,
                    &arn,
                    "RDS-EVENT-0170",
                    &["creation"],
                    "DB cluster created",
                );
                Ok(xml_response(
                    "RestoreDBClusterFromS3",
                    format!(
                        "    <DBCluster>\n{}\n    </DBCluster>",
                        db_cluster_member_xml(&entry)
                    ),
                    &rid,
                ))
            }

            // ── Recommendations ──
            "DescribeDBRecommendations" => Ok(xml_response("DescribeDBRecommendations", "    <DBRecommendations/>".to_string(), &rid)),
            "ModifyDBRecommendation" => Ok(xml_response("ModifyDBRecommendation", "    <DBRecommendation/>".to_string(), &rid)),

            // ── Certificates ──
            "DescribeCertificates" => Ok(xml_response("DescribeCertificates", "    <Certificates/>".to_string(), &rid)),
            "ModifyCertificates" => {
                let cert_id = get_param(req, "CertificateIdentifier");
                let remove_override = get_param(req, "RemoveCustomerOverride")
                    .map(|v| v.eq_ignore_ascii_case("true"))
                    .unwrap_or(false);
                let mut accounts = write_state!();
                let state = accounts.get_or_create(&aid);
                if remove_override {
                    state.default_certificate_identifier = None;
                } else if let Some(id) = cert_id.clone() {
                    state.default_certificate_identifier = Some(id);
                }
                let echoed = state
                    .default_certificate_identifier
                    .clone()
                    .unwrap_or_default();
                Ok(xml_response("ModifyCertificates", format!("    <Certificate>\n      <CertificateIdentifier>{}</CertificateIdentifier>\n      <CustomerOverride>{}</CustomerOverride>\n    </Certificate>", xml_escape(&echoed), !remove_override && cert_id.is_some()), &rid))
            }

            // ── Account / events / regions / log files / capacity ──
            "DescribeAccountAttributes" => Ok(xml_response("DescribeAccountAttributes", "    <AccountQuotas/>".to_string(), &rid)),
            "DescribeEventCategories" => Ok(xml_response("DescribeEventCategories", "    <EventCategoriesMapList/>".to_string(), &rid)),
            "DescribeEvents" => self.describe_events(req, &rid),
            "DescribeSourceRegions" => Ok(xml_response("DescribeSourceRegions", "    <SourceRegions/>".to_string(), &rid)),
            "DescribeDBMajorEngineVersions" => Ok(xml_response("DescribeDBMajorEngineVersions", "    <DBMajorEngineVersions/>".to_string(), &rid)),
            "DescribeServerlessV2PlatformVersions" => {
                let engine = get_param(req, "Engine").unwrap_or_else(|| "aurora-mysql".to_string());
                let version_filter = get_param(req, "ServerlessV2PlatformVersion");
                let all = [
                    ("4", true, "Version 4 offering scaling up to 256 ACUs", 256.0_f64),
                    ("3", false, "Version 3 offering scaling up to 256 ACUs", 256.0),
                    ("2", false, "Version 2 offering scaling up to 256 ACUs", 256.0),
                    ("1", false, "Version 1 offering scaling up to 128 ACUs", 128.0),
                ];
                let body = all
                    .iter()
                    .filter(|(v, ..)| version_filter.as_deref().is_none_or(|f| f == *v))
                    .map(|(v, is_default, desc, max)| {
                        format!(
                            "      <member>\n        <Engine>{e}</Engine>\n        <IsDefault>{d}</IsDefault>\n        <ServerlessV2PlatformVersion>{v}</ServerlessV2PlatformVersion>\n        <ServerlessV2PlatformVersionDescription>{desc}</ServerlessV2PlatformVersionDescription>\n        <Status>enabled</Status>\n        <ServerlessV2FeaturesSupport>\n          <MinCapacity>0.0</MinCapacity>\n          <MaxCapacity>{max:.1}</MaxCapacity>\n        </ServerlessV2FeaturesSupport>\n      </member>",
                            e = xml_escape(&engine),
                            d = is_default,
                            v = v,
                            desc = xml_escape(desc),
                            max = max,
                        )
                    })
                    .collect::<Vec<_>>()
                    .join("\n");
                Ok(xml_response(
                    "DescribeServerlessV2PlatformVersions",
                    format!("    <ServerlessV2PlatformVersions>\n{body}\n    </ServerlessV2PlatformVersions>"),
                    &rid,
                ))
            }
            "DescribeValidDBInstanceModifications" => Ok(xml_response("DescribeValidDBInstanceModifications", "    <ValidDBInstanceModificationsMessage>\n      <ValidProcessorFeatures/>\n      <Storage/>\n    </ValidDBInstanceModificationsMessage>".to_string(), &rid)),
            "ModifyCurrentDBClusterCapacity" => {
                let id = get_param(req, "DBClusterIdentifier")
                    .ok_or_else(|| missing("DBClusterIdentifier"))?;
                let capacity = get_param(req, "Capacity")
                    .and_then(|c| c.parse::<i64>().ok())
                    .unwrap_or(0);
                let seconds_before_timeout = get_param(req, "SecondsBeforeTimeout")
                    .and_then(|c| c.parse::<i64>().ok())
                    .unwrap_or(300);
                let timeout_action = get_param(req, "TimeoutAction")
                    .unwrap_or_else(|| "ForceApplyCapacityChange".to_string());
                {
                    let mut accounts = write_state!();
                    let state = accounts.get_or_create(&aid);
                    let entry = state
                        .extras
                        .get_mut("clusters")
                        .and_then(|m| m.get_mut(&id))
                        .ok_or_else(|| cluster_not_found(&id))?;
                    if let Some(obj) = entry.as_object_mut() {
                        obj.insert("Capacity".to_string(), json!(capacity));
                    }
                }
                Ok(xml_response(
                    "ModifyCurrentDBClusterCapacity",
                    format!(
                        "    <DBClusterIdentifier>{}</DBClusterIdentifier>\n    <PendingCapacity>{}</PendingCapacity>\n    <CurrentCapacity>{}</CurrentCapacity>\n    <SecondsBeforeTimeout>{}</SecondsBeforeTimeout>\n    <TimeoutAction>{}</TimeoutAction>",
                        xml_escape(&id),
                        capacity,
                        capacity,
                        seconds_before_timeout,
                        xml_escape(&timeout_action),
                    ),
                    &rid,
                ))
            }
            "EnableHttpEndpoint" | "DisableHttpEndpoint" => {
                let resource_arn =
                    get_param(req, "ResourceArn").ok_or_else(|| missing("ResourceArn"))?;
                let enabled = action == "EnableHttpEndpoint";
                // ResourceArn is the cluster ARN; recover the cluster id from
                // its trailing segment (bare ids are tolerated too).
                let (_, cluster_id) = parse_rds_resource_arn(&resource_arn);
                {
                    let mut accounts = write_state!();
                    let state = accounts.get_or_create(&aid);
                    let entry = state
                        .extras
                        .get_mut("clusters")
                        .and_then(|m| m.get_mut(&cluster_id))
                        // These ops declare ResourceNotFoundFault (not the typed
                        // DBClusterNotFoundFault) for an unknown ResourceArn.
                        .ok_or_else(|| resource_not_found(&resource_arn))?;
                    if let Some(obj) = entry.as_object_mut() {
                        obj.insert("HttpEndpointEnabled".to_string(), json!(enabled));
                    }
                }
                Ok(xml_response(
                    action.as_str(),
                    format!(
                        "    <ResourceArn>{}</ResourceArn>\n    <HttpEndpointEnabled>{}</HttpEndpointEnabled>",
                        xml_escape(&resource_arn),
                        enabled,
                    ),
                    &rid,
                ))
            }

            _ => Err(AwsServiceError::action_not_implemented("rds", &action)),
        }
    }
}

// ── XML helpers per resource ──

/// Parse a repeated attribute-value list (`ValuesToAdd` / `ValuesToRemove`)
/// from the query body. AWS serializes these as
/// `ValuesToAdd.AttributeValue.N`; some SDKs and the conformance probe emit
/// the generic `.member.N` form. Accept both.
fn parse_attribute_values(req: &AwsRequest, prefix: &str) -> Vec<String> {
    for member in ["AttributeValue", "member"] {
        let mut out = Vec::new();
        for index in 1.. {
            match get_param(req, &format!("{prefix}.{member}.{index}")) {
                Some(v) => out.push(v),
                None => break,
            }
        }
        if !out.is_empty() {
            return out;
        }
    }
    Vec::new()
}

/// Persist a Database Activity Stream state onto whichever resource the
/// `ResourceArn` names — an Aurora DB cluster (stored as JSON keys on the
/// cluster entry) or an RDS DB instance (stored in `DbInstance::activity_stream`).
/// `stream` of `None` clears the stream to `stopped`. Returns `false` when
/// neither a matching instance nor cluster exists.
fn apply_activity_stream(
    state: &mut crate::state::RdsState,
    id: &str,
    stream: Option<crate::state::ActivityStreamConfig>,
) -> bool {
    if let Some(inst) = state.instances.get_mut(id) {
        inst.activity_stream = stream;
        return true;
    }
    if let Some(entry) = state.extras.get_mut("clusters").and_then(|m| m.get_mut(id)) {
        if let Some(obj) = entry.as_object_mut() {
            match stream {
                Some(cfg) => {
                    let status = if cfg.status.is_empty() {
                        "started".to_string()
                    } else {
                        cfg.status
                    };
                    obj.insert("ActivityStreamStatus".to_string(), json!(status));
                    set_or_remove(obj, "ActivityStreamKmsKeyId", cfg.kms_key_id);
                    set_or_remove(
                        obj,
                        "ActivityStreamKinesisStreamName",
                        cfg.kinesis_stream_name,
                    );
                    set_or_remove(obj, "ActivityStreamMode", cfg.mode);
                }
                None => {
                    obj.insert("ActivityStreamStatus".to_string(), json!("stopped"));
                    obj.remove("ActivityStreamKmsKeyId");
                    obj.remove("ActivityStreamKinesisStreamName");
                    obj.remove("ActivityStreamMode");
                }
            }
        }
        return true;
    }
    false
}

/// Read the current activity-stream config from an instance or cluster entry.
fn read_activity_stream(
    state: &crate::state::RdsState,
    id: &str,
) -> Option<crate::state::ActivityStreamConfig> {
    if let Some(inst) = state.instances.get(id) {
        return inst.activity_stream.clone();
    }
    let entry = state.extras.get("clusters").and_then(|m| m.get(id))?;
    let status = entry["ActivityStreamStatus"].as_str()?;
    if status == "stopped" {
        return None;
    }
    Some(crate::state::ActivityStreamConfig {
        status: status.to_string(),
        mode: entry["ActivityStreamMode"].as_str().map(str::to_string),
        kms_key_id: entry["ActivityStreamKmsKeyId"].as_str().map(str::to_string),
        kinesis_stream_name: entry["ActivityStreamKinesisStreamName"]
            .as_str()
            .map(str::to_string),
    })
}

fn set_or_remove(obj: &mut serde_json::Map<String, Value>, key: &str, value: Option<String>) {
    match value {
        Some(v) => {
            obj.insert(key.to_string(), json!(v));
        }
        None => {
            obj.remove(key);
        }
    }
}

/// Apply the serverless capacity knobs (`MaxACU`/`MinACU` as doubles,
/// `ComputeRedundancy` as an integer) from a Create/Modify DBShardGroup
/// request onto the stored shard-group JSON object. Absent params are left
/// untouched so a Modify only overwrites what the caller sent.
fn apply_shard_group_capacity(obj: &mut serde_json::Map<String, Value>, req: &AwsRequest) {
    for key in ["MaxACU", "MinACU"] {
        if let Some(n) = get_param(req, key).and_then(|v| v.parse::<f64>().ok()) {
            obj.insert(key.to_string(), json!(n));
        }
    }
    if let Some(n) = get_param(req, "ComputeRedundancy").and_then(|v| v.parse::<i64>().ok()) {
        obj.insert("ComputeRedundancy".to_string(), json!(n));
    }
}

/// Render the `DBSnapshotAttributesResult` block shared by
/// `DescribeDBSnapshotAttributes` and `ModifyDBSnapshotAttribute`.
/// Look a cluster snapshot up across accounts: the caller's own first,
/// then any account that shared it with the caller (or with everyone).
/// `named_account` is the owner an ARN named, when the caller used one.
pub(crate) fn find_cluster_snapshot(
    accounts: &fakecloud_core::multi_account::MultiAccountState<crate::state::RdsState>,
    caller: &str,
    named_account: Option<&str>,
    id: &str,
) -> Option<Value> {
    if named_account.is_none_or(|account| account == caller) {
        if let Some(entry) = accounts
            .get(caller)
            .and_then(|s| s.extras.get("cluster_snapshots"))
            .and_then(|m| m.get(id))
        {
            return Some(entry.clone());
        }
    }
    // AWS requires the ARN to reach another account's shared snapshot,
    // and a bare id could match several accounts at once -- the scan
    // would then return an arbitrary (HashMap-ordered) row.
    let named_account = named_account?;
    accounts
        .iter()
        .filter(|(owner, _)| *owner != caller)
        .filter(|(owner, _)| *owner == named_account)
        .find_map(|(_, other)| {
            other
                .extras
                .get("cluster_snapshots")
                .and_then(|m| m.get(id))
                .filter(|entry| {
                    cluster_snapshot_attributes(entry)
                        .get("restore")
                        .is_some_and(|targets| targets.iter().any(|t| t == caller || t == "all"))
                })
                .cloned()
        })
}

/// The stored share attributes of a cluster snapshot entry
/// (`ModifyDBClusterSnapshotAttribute` writes them under
/// `SnapshotAttributes`), keyed by attribute name.
fn cluster_snapshot_attributes(entry: &Value) -> BTreeMap<String, Vec<String>> {
    entry
        .get("SnapshotAttributes")
        .and_then(|v| v.as_object())
        .map(|obj| {
            obj.iter()
                .map(|(name, values)| {
                    let values = values
                        .as_array()
                        .map(|arr| {
                            arr.iter()
                                .filter_map(|v| v.as_str().map(str::to_string))
                                .collect()
                        })
                        .unwrap_or_default();
                    (name.clone(), values)
                })
                .collect()
        })
        .unwrap_or_default()
}

/// True when a cluster snapshot the caller owns satisfies `SnapshotType`.
/// `shared` / `public` select other accounts' snapshots instead and are
/// handled separately.
fn owned_snapshot_type_matches(entry: &Value, snapshot_type: Option<&str>) -> bool {
    match snapshot_type {
        // An owned snapshot marked public is still public; `shared`
        // means "shared TO me", which an owned snapshot never is.
        Some("public") => cluster_snapshot_attributes(entry)
            .get("restore")
            .is_some_and(|targets| targets.iter().any(|t| t == "all")),
        Some("shared") => false,
        // Same default the renderer emits, so a stored entry without the
        // field can't read back as `manual` yet be excluded by
        // `--snapshot-type manual`.
        Some(wanted) => entry_str(entry, "SnapshotType").unwrap_or("manual") == wanted,
        None => true,
    }
}

/// `DBClusterSnapshotAttributesResult`, the cluster-snapshot twin of
/// [`snapshot_attributes_result_xml`].
fn cluster_snapshot_attributes_result_xml(
    id: &str,
    attrs: &BTreeMap<String, Vec<String>>,
) -> String {
    let attributes = if attrs.is_empty() {
        "      <DBClusterSnapshotAttributes/>".to_string()
    } else {
        let members = attrs
            .iter()
            .map(|(name, values)| {
                let value_members = if values.is_empty() {
                    "            <AttributeValues/>".to_string()
                } else {
                    let inner = values
                        .iter()
                        .map(|v| {
                            format!(
                                "              <AttributeValue>{}</AttributeValue>",
                                xml_escape(v)
                            )
                        })
                        .collect::<Vec<_>>()
                        .join("\n");
                    format!("            <AttributeValues>\n{inner}\n            </AttributeValues>")
                };
                format!(
                    "        <DBClusterSnapshotAttribute>\n          <AttributeName>{}</AttributeName>\n{}\n        </DBClusterSnapshotAttribute>",
                    xml_escape(name),
                    value_members,
                )
            })
            .collect::<Vec<_>>()
            .join("\n");
        format!(
            "      <DBClusterSnapshotAttributes>\n{members}\n      </DBClusterSnapshotAttributes>"
        )
    };
    format!(
        "    <DBClusterSnapshotAttributesResult>\n      <DBClusterSnapshotIdentifier>{}</DBClusterSnapshotIdentifier>\n{}\n    </DBClusterSnapshotAttributesResult>",
        xml_escape(id),
        attributes,
    )
}

fn snapshot_attributes_result_xml(id: &str, attrs: &BTreeMap<String, Vec<String>>) -> String {
    let attributes = if attrs.is_empty() {
        "      <DBSnapshotAttributes/>".to_string()
    } else {
        let members = attrs
            .iter()
            .map(|(name, values)| {
                let value_members = if values.is_empty() {
                    "            <AttributeValues/>".to_string()
                } else {
                    let inner = values
                        .iter()
                        .map(|v| {
                            format!(
                                "              <AttributeValue>{}</AttributeValue>",
                                xml_escape(v)
                            )
                        })
                        .collect::<Vec<_>>()
                        .join("\n");
                    format!("            <AttributeValues>\n{inner}\n            </AttributeValues>")
                };
                format!(
                    "        <DBSnapshotAttribute>\n          <AttributeName>{}</AttributeName>\n{}\n        </DBSnapshotAttribute>",
                    xml_escape(name),
                    value_members,
                )
            })
            .collect::<Vec<_>>()
            .join("\n");
        format!("      <DBSnapshotAttributes>\n{members}\n      </DBSnapshotAttributes>")
    };
    format!(
        "    <DBSnapshotAttributesResult>\n      <DBSnapshotIdentifier>{}</DBSnapshotIdentifier>\n{}\n    </DBSnapshotAttributesResult>",
        xml_escape(id),
        attributes,
    )
}

/// Generate a `DbClusterResourceId` in AWS's `cluster-XXXX` form. The suffix
/// is immutable and survives rename, so IAM auth / CloudWatch dimensions key
/// on it rather than the cluster identifier.
pub(crate) fn new_cluster_resource_id() -> String {
    format!("cluster-{}", uuid::Uuid::new_v4().simple())
}

pub(crate) fn db_cluster_xml(id: &str, arn: &str) -> String {
    format!(
        "    <DBCluster>\n      <DBClusterIdentifier>{}</DBClusterIdentifier>\n      <DBClusterArn>{}</DBClusterArn>\n      <Status>available</Status>\n    </DBCluster>",
        xml_escape(id), xml_escape(arn)
    )
}

/// Snapshot-type / engine fields for a single-object response, read off
/// the stored entry. The Describe path reports these, so the create /
/// copy / delete responses have to as well -- otherwise a client reads a
/// blank `SnapshotType` off the copy it just made.
pub(crate) fn cluster_snapshot_detail_xml(entry: Option<&Value>) -> String {
    let mut out = String::new();
    let entry = match entry {
        Some(entry) => entry,
        None => return out,
    };
    out.push_str(&format!(
        "\n      <SnapshotType>{}</SnapshotType>",
        xml_escape(entry_str(entry, "SnapshotType").unwrap_or("manual"))
    ));
    if let Some(engine) = entry_str(entry, "Engine") {
        out.push_str(&format!("\n      <Engine>{}</Engine>", xml_escape(engine)));
    }
    if let Some(version) = entry_str(entry, "EngineVersion") {
        out.push_str(&format!(
            "\n      <EngineVersion>{}</EngineVersion>",
            xml_escape(version)
        ));
    }
    out
}

/// A single `<DBClusterSnapshot>` element with an explicit status (so
/// CreateDBClusterSnapshot can report `creating` while the writer dump
/// runs) plus the extra fields the caller read off the stored entry (see
/// [`cluster_snapshot_detail_xml`]).
pub(crate) fn cluster_snapshot_status_detail_xml(
    id: &str,
    arn: &str,
    cluster: &str,
    status: &str,
    detail: String,
) -> String {
    format!(
        "    <DBClusterSnapshot>\n      <DBClusterSnapshotIdentifier>{}</DBClusterSnapshotIdentifier>\n      <DBClusterSnapshotArn>{}</DBClusterSnapshotArn>\n      <DBClusterIdentifier>{}</DBClusterIdentifier>\n      <Status>{}</Status>{}\n    </DBClusterSnapshot>",
        xml_escape(id),
        xml_escape(arn),
        xml_escape(cluster),
        xml_escape(status),
        detail,
    )
}

/// AWS-spec `SourceType` enum values for the `DescribeEvents` filter.
/// Anything else triggers `InvalidParameterValue`.
const VALID_DESCRIBE_EVENTS_SOURCE_TYPES: &[&str] = &[
    "db-instance",
    "db-cluster",
    "db-parameter-group",
    "db-security-group",
    "db-snapshot",
    "db-cluster-snapshot",
    "db-proxy",
    "blue-green-deployment",
    "custom-engine-version",
];

impl RdsService {
    /// Real DescribeEvents implementation: read the per-account events
    /// ring written to by `emit_event`. Honour SourceType /
    /// SourceIdentifier / Duration / StartTime / EndTime / EventCategories
    /// filters plus MaxRecords / Marker pagination, and emit them as the
    /// DescribeEventsResult shape.
    pub(crate) fn describe_events(
        &self,
        req: &AwsRequest,
        rid: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let source_type = get_param(req, "SourceType");
        if let Some(ref t) = source_type {
            if !VALID_DESCRIBE_EVENTS_SOURCE_TYPES.contains(&t.as_str()) {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterValue",
                    format!("SourceType '{t}' is not a valid value."),
                ));
            }
        }
        let source_identifier = get_param(req, "SourceIdentifier");
        let event_categories: Vec<String> = (1..=20)
            .filter_map(|i| get_param(req, &format!("EventCategories.member.{i}")))
            .collect();
        let duration_minutes: i64 = get_param(req, "Duration")
            .and_then(|s| s.parse().ok())
            .unwrap_or(60);
        let now = chrono::Utc::now();
        let start_time = get_param(req, "StartTime")
            .and_then(|s| chrono::DateTime::parse_from_rfc3339(&s).ok())
            .map(|dt| dt.with_timezone(&chrono::Utc))
            .unwrap_or_else(|| now - chrono::Duration::minutes(duration_minutes));
        let end_time = get_param(req, "EndTime")
            .and_then(|s| chrono::DateTime::parse_from_rfc3339(&s).ok())
            .map(|dt| dt.with_timezone(&chrono::Utc))
            .unwrap_or(now);

        let state = self.state_handle().read();
        let mut events = state
            .get(&req.account_id)
            .map(|s| s.events.clone())
            .unwrap_or_default();
        drop(state);

        // AWS returns events ordered by `Date` ascending (oldest first).
        events.sort_by_key(|e| e.date);

        let filtered: Vec<crate::state::RdsEventRecord> = events
            .into_iter()
            .filter(|e| {
                source_type.as_deref().is_none_or(|t| e.source_type == t)
                    && source_identifier
                        .as_deref()
                        .is_none_or(|i| e.source_identifier == i)
                    && (event_categories.is_empty()
                        || event_categories
                            .iter()
                            .any(|c| e.event_categories.iter().any(|ec| ec == c)))
                    && e.date >= start_time
                    && e.date <= end_time
            })
            .collect();

        // MaxRecords (1..=100, default 100) and Marker pagination. We key
        // the marker by the event's RFC3339 timestamp + identifier so
        // duplicate dates still paginate deterministically.
        let max_records: usize = match get_param(req, "MaxRecords") {
            Some(raw) => {
                let parsed: i32 = raw.parse().map_err(|_| {
                    AwsServiceError::aws_error(
                        StatusCode::BAD_REQUEST,
                        "InvalidParameterValue",
                        "MaxRecords must be a valid integer.",
                    )
                })?;
                if !(1..=100).contains(&parsed) {
                    return Err(AwsServiceError::aws_error(
                        StatusCode::BAD_REQUEST,
                        "InvalidParameterValue",
                        "MaxRecords must be between 1 and 100.",
                    ));
                }
                parsed as usize
            }
            None => 100,
        };

        let start_index = match get_param(req, "Marker") {
            Some(marker) => marker.parse::<usize>().map_err(|_| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterValue",
                    "Marker is invalid.",
                )
            })?,
            None => 0,
        };
        let end_index = std::cmp::min(start_index.saturating_add(max_records), filtered.len());
        let next_marker = if end_index < filtered.len() {
            Some(end_index.to_string())
        } else {
            None
        };
        let page = filtered.get(start_index..end_index).unwrap_or(&[]);

        let mut body = String::new();
        if let Some(m) = next_marker {
            body.push_str(&format!("    <Marker>{}</Marker>\n", xml_escape(&m)));
        }
        body.push_str("    <Events>\n");
        for e in page {
            body.push_str("      <Event>\n");
            body.push_str(&format!(
                "        <SourceIdentifier>{}</SourceIdentifier>\n",
                xml_escape(&e.source_identifier),
            ));
            body.push_str(&format!(
                "        <SourceType>{}</SourceType>\n",
                xml_escape(&e.source_type),
            ));
            body.push_str(&format!(
                "        <Message>{}</Message>\n",
                xml_escape(&e.message),
            ));
            body.push_str(&format!(
                "        <SourceArn>{}</SourceArn>\n",
                xml_escape(&e.source_arn),
            ));
            body.push_str("        <EventCategories>\n");
            for cat in &e.event_categories {
                body.push_str(&format!(
                    "          <EventCategory>{}</EventCategory>\n",
                    xml_escape(cat),
                ));
            }
            body.push_str("        </EventCategories>\n");
            body.push_str(&format!("        <Date>{}</Date>\n", e.date.to_rfc3339(),));
            body.push_str("      </Event>\n");
        }
        body.push_str("    </Events>");
        Ok(xml_response("DescribeEvents", body, rid))
    }
}

mod cluster_actions;
mod parse;
#[cfg(test)]
mod tests;
mod xml_renderers;
use cluster_actions::*;
use parse::*;
use xml_renderers::*;
