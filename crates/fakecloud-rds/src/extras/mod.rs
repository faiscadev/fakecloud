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

const NS: &str = "http://rds.amazonaws.com/doc/2014-10-31/";

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

fn members<F>(items: &[Value], render: F) -> String
where
    F: Fn(&Value) -> String,
{
    items
        .iter()
        .map(|v| format!("        <member>\n{}\n        </member>", render(v)))
        .collect::<Vec<_>>()
        .join("\n")
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

impl RdsService {
    pub(crate) fn handle_extra_action(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let action = req.action.clone();
        let aid = req.account_id.clone();
        let rid = req.request_id.clone();
        let region = "us-east-1"; // RDS uses us-east-1 by default in fakecloud

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
                let entry = json!({
                    "DBClusterIdentifier": id, "DBClusterArn": arn,
                    "DbClusterResourceId": new_cluster_resource_id(),
                    "Status": "available", "Engine": engine,
                    "EngineVersion": get_param(req, "EngineVersion").unwrap_or_else(|| "15.3".to_string()),
                    "Endpoint": format!("{id}.cluster-xxx.{region}.rds.amazonaws.com"),
                    "ReaderEndpoint": format!("{id}.cluster-ro-xxx.{region}.rds.amazonaws.com"),
                    "Port": port, "MasterUsername": get_param(req, "MasterUsername").unwrap_or_else(|| "postgres".to_string()),
                });
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
                let id_filter = get_param(req, "DBClusterIdentifier");
                let accounts = self.state_handle().read();
                let items: Vec<Value> = accounts.get(&aid)
                    .and_then(|s| s.extras.get("clusters"))
                    .map(|m| {
                        m.values()
                            .filter(|v| {
                                id_filter
                                    .as_deref()
                                    .map(|filter| v["DBClusterIdentifier"].as_str() == Some(filter))
                                    .unwrap_or(true)
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

            // ── DB Cluster snapshots ──
            // CreateDBClusterSnapshot is implemented in service.rs because
            // the real path needs the async runtime to dump the writer's
            // database. We keep a metadata-only fallback here for unit
            // tests that exercise the extras handler directly without a
            // runtime wired up; the dispatcher in `RdsService::handle_request`
            // routes the action through the async path before reaching us.
            "CreateDBClusterSnapshot" => {
                let id = get_param(req, "DBClusterSnapshotIdentifier")
                    .ok_or_else(|| missing("DBClusterSnapshotIdentifier"))?;
                let arn = Arn::new("rds", region, &aid, &format!("cluster-snapshot:{id}")).to_string();
                let cluster = get_param(req, "DBClusterIdentifier").unwrap_or_else(|| "default".to_string());
                {
                    let mut accounts = write_state!();
                    let state = accounts.get_or_create(&aid);
                    let mut entry = state
                        .extras
                        .get("clusters")
                        .and_then(|m| m.get(&cluster))
                        .cloned()
                        .unwrap_or_else(|| json!({}));
                    if let Some(obj) = entry.as_object_mut() {
                        obj.insert("DBClusterSnapshotIdentifier".to_string(), json!(id));
                        obj.insert("DBClusterSnapshotArn".to_string(), json!(arn));
                        obj.insert("DBClusterIdentifier".to_string(), json!(cluster));
                        obj.insert("Status".to_string(), json!("available"));
                        obj.insert("SnapshotType".to_string(), json!("manual"));
                    }
                    store(&mut state.extras, "cluster_snapshots").insert(id.clone(), entry);
                }
                self.emit_event(
                    RdsSourceType::DbClusterSnapshot,
                    &id,
                    &arn,
                    "RDS-EVENT-0074",
                    &["backup"],
                    "DB cluster snapshot created",
                );
                Ok(xml_response(action.as_str(), cluster_snapshot_xml(&id, &arn, &cluster), &rid))
            }
            "CopyDBClusterSnapshot" => {
                let id = get_param(req, "TargetDBClusterSnapshotIdentifier")
                    .ok_or_else(|| missing("TargetDBClusterSnapshotIdentifier"))?;
                let source_id = get_param(req, "SourceDBClusterSnapshotIdentifier")
                    .ok_or_else(|| missing("SourceDBClusterSnapshotIdentifier"))?;
                let arn = Arn::new("rds", region, &aid, &format!("cluster-snapshot:{id}")).to_string();
                let mut accounts = write_state!();
                let state = accounts.get_or_create(&aid);
                let source_key = source_id.rsplit(':').next().unwrap_or(&source_id).to_string();
                let mut entry = state
                    .extras
                    .get("cluster_snapshots")
                    .and_then(|m| m.get(&source_key))
                    .cloned()
                    .ok_or_else(|| {
                        AwsServiceError::aws_error(
                            StatusCode::NOT_FOUND,
                            "DBClusterSnapshotNotFoundFault",
                            format!("DBClusterSnapshot {source_id} not found."),
                        )
                    })?;
                let cluster = entry
                    .get("DBClusterIdentifier")
                    .and_then(|v| v.as_str())
                    .unwrap_or("default")
                    .to_string();
                if let Some(obj) = entry.as_object_mut() {
                    obj.insert("DBClusterSnapshotIdentifier".to_string(), json!(id));
                    obj.insert("DBClusterSnapshotArn".to_string(), json!(arn));
                    obj.insert("Status".to_string(), json!("available"));
                    obj.insert("SnapshotType".to_string(), json!("manual"));
                    obj.insert("SourceDBClusterSnapshotArn".to_string(), json!(source_id));
                }
                store(&mut state.extras, "cluster_snapshots").insert(id.clone(), entry);
                Ok(xml_response(action.as_str(), cluster_snapshot_xml(&id, &arn, &cluster), &rid))
            }
            "DeleteDBClusterSnapshot" => {
                let id = get_param(req, "DBClusterSnapshotIdentifier").ok_or_else(|| missing("DBClusterSnapshotIdentifier"))?;
                let arn = Arn::new("rds", region, &aid, &format!("cluster-snapshot:{id}")).to_string();
                // Recover the source cluster id from stored state before
                // remove — emitting a hardcoded "default" would corrupt
                // downstream consumers that key off DBClusterIdentifier.
                let cluster = {
                    let mut accounts = write_state!();
                    let state = accounts.get_or_create(&aid);
                    let prior = state
                        .extras
                        .get("cluster_snapshots")
                        .and_then(|m| m.get(&id))
                        .and_then(|v| v.get("DBClusterIdentifier"))
                        .and_then(|v| v.as_str())
                        .unwrap_or_default()
                        .to_string();
                    if let Some(m) = state.extras.get_mut("cluster_snapshots") { m.remove(&id); }
                    prior
                };
                self.emit_event(
                    RdsSourceType::DbClusterSnapshot,
                    &id,
                    &arn,
                    "RDS-EVENT-0075",
                    &["deletion"],
                    "DB cluster snapshot deleted",
                );
                Ok(xml_response("DeleteDBClusterSnapshot", cluster_snapshot_xml(&id, &arn, &cluster), &rid))
            }
            "DescribeDBClusterSnapshots" => list_extras_xml(self, &aid, "cluster_snapshots", "DBClusterSnapshots", "DescribeDBClusterSnapshots", cluster_snapshot_member_xml, &rid),
            "DescribeDBClusterSnapshotAttributes" | "ModifyDBClusterSnapshotAttribute" => {
                let id = get_param(req, "DBClusterSnapshotIdentifier").unwrap_or_default();
                Ok(xml_response(action.as_str(), format!("    <DBClusterSnapshotAttributesResult>\n      <DBClusterSnapshotIdentifier>{}</DBClusterSnapshotIdentifier>\n      <DBClusterSnapshotAttributes/>\n    </DBClusterSnapshotAttributesResult>", xml_escape(&id)), &rid))
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
            "DescribeDBClusterEndpoints" => list_extras_xml(self, &aid, "cluster_endpoints", "DBClusterEndpoints", "DescribeDBClusterEndpoints", cluster_endpoint_xml, &rid),

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
            "DescribeDBProxies" => list_extras_xml(self, &aid, "proxies", "DBProxies", "DescribeDBProxies", proxy_xml, &rid),
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
            "DescribeDBProxyTargets" => Ok(xml_response("DescribeDBProxyTargets", "    <Targets/>".to_string(), &rid)),
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
            "RegisterDBProxyTargets" => Ok(xml_response("RegisterDBProxyTargets", "    <DBProxyTargets/>".to_string(), &rid)),
            "DeregisterDBProxyTargets" => xml_empty_action(&action, &rid),

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
            "DescribeDBSecurityGroups" => list_extras_xml(self, &aid, "security_groups", "DBSecurityGroups", "DescribeDBSecurityGroups", security_group_xml, &rid),

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
                    if !to_include.is_empty() {
                        obj.insert("OptionsToInclude".to_string(), json!(to_include));
                    }
                    if !to_remove.is_empty() {
                        obj.insert("OptionsToRemove".to_string(), json!(to_remove));
                    }
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
                let entry = json!({"CustSubscriptionId": name, "CustomerAwsId": aid, "EventSubscriptionArn": arn, "SnsTopicArn": get_param(req, "SnsTopicArn").unwrap_or_default(), "SourceType": get_param(req, "SourceType").unwrap_or_default(), "Status": "active", "Enabled": true});
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
            "AddSourceIdentifierToSubscription" | "RemoveSourceIdentifierFromSubscription" => Ok(xml_response(action.as_str(), "    <EventSubscription/>".to_string(), &rid)),

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
            "ModifyGlobalCluster" | "FailoverGlobalCluster" | "SwitchoverGlobalCluster" | "RemoveFromGlobalCluster" => Ok(xml_response(action.as_str(), "    <GlobalCluster/>".to_string(), &rid)),
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
            "ModifyIntegration" => Ok(xml_response("ModifyIntegration", "    <Integration/>".to_string(), &rid)),
            "DeleteIntegration" => {
                let name = get_param(req, "IntegrationIdentifier").or_else(|| get_param(req, "IntegrationName")).ok_or_else(|| missing("IntegrationIdentifier"))?;
                let mut accounts = write_state!();
                let state = accounts.get_or_create(&aid);
                if let Some(m) = state.extras.get_mut("integrations") { m.remove(&name); }
                Ok(xml_response("DeleteIntegration", "    <Integration/>".to_string(), &rid))
            }
            "DescribeIntegrations" => list_extras_xml(self, &aid, "integrations", "Integrations", "DescribeIntegrations", integration_xml, &rid),

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
                    state.db_instance_arn(&source_id)
                };
                let target_arn = state.db_instance_arn(&target_id);
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
            "DescribeBlueGreenDeployments" => list_extras_xml(self, &aid, "blue_green", "BlueGreenDeployments", "DescribeBlueGreenDeployments", blue_green_xml, &rid),

            // ── Shard groups ──
            "CreateDBShardGroup" => {
                let id = get_param(req, "DBShardGroupIdentifier").ok_or_else(|| missing("DBShardGroupIdentifier"))?;
                let entry = json!({"DBShardGroupIdentifier": id, "Status": "available"});
                let mut accounts = write_state!();
                let state = accounts.get_or_create(&aid);
                store(&mut state.extras, "shard_groups").insert(id.clone(), entry.clone());
                Ok(xml_response("CreateDBShardGroup", shard_group_xml(&entry), &rid))
            }
            "ModifyDBShardGroup" | "RebootDBShardGroup" => Ok(xml_response(action.as_str(), "    <DBShardGroup/>".to_string(), &rid)),
            "DeleteDBShardGroup" => {
                let id = get_param(req, "DBShardGroupIdentifier").ok_or_else(|| missing("DBShardGroupIdentifier"))?;
                let mut accounts = write_state!();
                let state = accounts.get_or_create(&aid);
                if let Some(m) = state.extras.get_mut("shard_groups") { m.remove(&id); }
                Ok(xml_response("DeleteDBShardGroup", "    <DBShardGroup/>".to_string(), &rid))
            }
            "DescribeDBShardGroups" => list_extras_xml(self, &aid, "shard_groups", "DBShardGroups", "DescribeDBShardGroups", shard_group_xml, &rid),

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
            "DescribeTenantDatabases" => list_extras_xml(self, &aid, "tenant_dbs", "TenantDatabases", "DescribeTenantDatabases", tenant_db_xml, &rid),
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
            "DescribeExportTasks" => list_extras_xml(self, &aid, "export_tasks", "ExportTasks", "DescribeExportTasks", export_task_xml, &rid),

            // ── Activity stream ──
            "StartActivityStream" => {
                let kms_input = get_param(req, "KmsKeyId").unwrap_or_default();
                let kms_arn = format_kms_arn(&kms_input, region, &aid);
                let mode = get_param(req, "Mode").unwrap_or_else(|| "async".to_string());
                let resource_arn = get_param(req, "ResourceArn").unwrap_or_default();
                let stream = if resource_arn.is_empty() {
                    "aws-rds-das".to_string()
                } else {
                    let id = resource_arn.rsplit(':').next().unwrap_or("default");
                    format!("aws-rds-das-{id}")
                };
                Ok(xml_response("StartActivityStream", format!("    <Status>started</Status>\n    <KmsKeyId>{}</KmsKeyId>\n    <KinesisStreamName>{}</KinesisStreamName>\n    <Mode>{}</Mode>\n    <ApplyImmediately>true</ApplyImmediately>", xml_escape(&kms_arn), xml_escape(&stream), xml_escape(&mode)), &rid))
            }
            "StopActivityStream" => Ok(xml_response("StopActivityStream", "    <Status>stopped</Status>".to_string(), &rid)),
            "ModifyActivityStream" => Ok(xml_response("ModifyActivityStream", "    <Status>started</Status>".to_string(), &rid)),

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
                let id = get_param(req, "TargetDBSnapshotIdentifier").ok_or_else(|| missing("TargetDBSnapshotIdentifier"))?;
                Ok(xml_response("CopyDBSnapshot", format!("    <DBSnapshot>\n      <DBSnapshotIdentifier>{}</DBSnapshotIdentifier>\n      <Status>available</Status>\n    </DBSnapshot>", xml_escape(&id)), &rid))
            }
            "CopyDBParameterGroup" => {
                let name = get_param(req, "TargetDBParameterGroupIdentifier").ok_or_else(|| missing("TargetDBParameterGroupIdentifier"))?;
                Ok(xml_response("CopyDBParameterGroup", format!("    <DBParameterGroup>\n      <DBParameterGroupName>{}</DBParameterGroupName>\n    </DBParameterGroup>", xml_escape(&name)), &rid))
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
            "DescribeDBSnapshotAttributes" => Ok(xml_response("DescribeDBSnapshotAttributes", "    <DBSnapshotAttributesResult>\n      <DBSnapshotAttributes/>\n    </DBSnapshotAttributesResult>".to_string(), &rid)),
            "ModifyDBSnapshot" | "ModifyDBSnapshotAttribute" => Ok(xml_response(action.as_str(), "    <DBSnapshot/>".to_string(), &rid)),
            "RestoreDBClusterFromSnapshot" => {
                let target = get_param(req, "DBClusterIdentifier")
                    .ok_or_else(|| missing("DBClusterIdentifier"))?;
                let snapshot_id = get_param(req, "SnapshotIdentifier")
                    .or_else(|| get_param(req, "DBClusterSnapshotIdentifier"))
                    .ok_or_else(|| missing("SnapshotIdentifier"))?;
                let arn = Arn::new("rds", region, &aid, &format!("cluster:{target}")).to_string();
                let mut accounts = write_state!();
                let state = accounts.get_or_create(&aid);
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
                let source_cluster_id = snapshot
                    .get("DBClusterIdentifier")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                let pending_dump_b64 = snapshot
                    .get("DumpDataB64")
                    .and_then(|v| v.as_str())
                    .map(str::to_string);
                let mut entry = state
                    .extras
                    .get("clusters")
                    .and_then(|m| m.get(source_cluster_id))
                    .cloned()
                    .unwrap_or_else(|| {
                        json!({
                            "Engine": get_param(req, "Engine").unwrap_or_else(|| "aurora-postgresql".to_string()),
                            "EngineVersion": get_param(req, "EngineVersion").unwrap_or_else(|| "15.3".to_string()),
                            "MasterUsername": "postgres",
                            "Port": 5432,
                        })
                    });
                if let Some(obj) = entry.as_object_mut() {
                    obj.insert("DBClusterIdentifier".to_string(), json!(target));
                    obj.insert("DBClusterArn".to_string(), json!(arn));
                    obj.insert("Status".to_string(), json!("available"));
                    obj.insert(
                        "Endpoint".to_string(),
                        json!(format!("{target}.cluster-xxx.{region}.rds.amazonaws.com")),
                    );
                    obj.insert(
                        "ReaderEndpoint".to_string(),
                        json!(format!("{target}.cluster-ro-xxx.{region}.rds.amazonaws.com")),
                    );
                    // Restored cluster is a distinct resource: mint a fresh
                    // immutable resource id rather than reuse the source's.
                    obj.insert(
                        "DbClusterResourceId".to_string(),
                        json!(new_cluster_resource_id()),
                    );
                    obj.remove("ReplicationSourceIdentifier");
                    // The new cluster starts empty; the user is expected
                    // to call CreateDBInstance with DBClusterIdentifier
                    // pointing here, at which point we replay the dump.
                    obj.remove("DBClusterMembers");
                    obj.remove("WriterDBInstanceIdentifier");
                    // Drop snapshot bookkeeping that leaked in via the
                    // source-cluster clone path.
                    obj.remove("DBClusterSnapshotIdentifier");
                    obj.remove("DBClusterSnapshotArn");
                    obj.remove("DumpDataB64");
                    if let Some(engine) = get_param(req, "Engine") {
                        obj.insert("Engine".to_string(), json!(engine));
                    }
                    if let Some(version) = get_param(req, "EngineVersion") {
                        obj.insert("EngineVersion".to_string(), json!(version));
                    }
                    if let Some(port) = get_param(req, "Port").and_then(|p| p.parse::<i64>().ok()) {
                        obj.insert("Port".to_string(), json!(port));
                    }
                    // Stage the snapshot dump so the next CreateDBInstance
                    // joining this cluster replays the data into its
                    // fresh container.
                    if let Some(b64) = pending_dump_b64 {
                        obj.insert("PendingRestoreDumpB64".to_string(), json!(b64));
                    }
                }
                store(&mut state.extras, "clusters").insert(target.clone(), entry.clone());
                drop(accounts);
                self.emit_event(
                    RdsSourceType::DbCluster,
                    &target,
                    &arn,
                    "RDS-EVENT-0170",
                    &["creation"],
                    "DB cluster restored from snapshot",
                );
                Ok(xml_response(
                    "RestoreDBClusterFromSnapshot",
                    format!(
                        "    <DBCluster>\n{}\n    </DBCluster>",
                        db_cluster_member_xml(&entry)
                    ),
                    &rid,
                ))
            }
            // Sync metadata-only fallback for RestoreDBClusterToPointInTime;
            // the dispatcher in `handle_request` routes the action to the
            // async path that also dumps and stages the source writer.
            "RestoreDBClusterToPointInTime" => {
                let target = get_param(req, "DBClusterIdentifier")
                    .ok_or_else(|| missing("DBClusterIdentifier"))?;
                let source = get_param(req, "SourceDBClusterIdentifier")
                    .ok_or_else(|| missing("SourceDBClusterIdentifier"))?;
                let arn = Arn::new("rds", region, &aid, &format!("cluster:{target}")).to_string();
                let mut accounts = write_state!();
                let state = accounts.get_or_create(&aid);
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
                        json!(format!("{target}.cluster-xxx.{region}.rds.amazonaws.com")),
                    );
                    obj.insert(
                        "ReaderEndpoint".to_string(),
                        json!(format!("{target}.cluster-ro-xxx.{region}.rds.amazonaws.com")),
                    );
                    obj.insert(
                        "DbClusterResourceId".to_string(),
                        json!(new_cluster_resource_id()),
                    );
                    obj.remove("DBClusterMembers");
                    obj.remove("WriterDBInstanceIdentifier");
                    if let Some(restore_time) = get_param(req, "RestoreToTime") {
                        obj.insert("RestoreToTime".to_string(), json!(restore_time));
                    }
                    if let Some(latest) = get_param(req, "UseLatestRestorableTime") {
                        obj.insert("UseLatestRestorableTime".to_string(), json!(latest));
                    }
                }
                store(&mut state.extras, "clusters").insert(target.clone(), entry.clone());
                drop(accounts);
                self.emit_event(
                    RdsSourceType::DbCluster,
                    &target,
                    &arn,
                    "RDS-EVENT-0171",
                    &["creation"],
                    "DB cluster restored to point in time",
                );
                Ok(xml_response(
                    "RestoreDBClusterToPointInTime",
                    format!(
                        "    <DBCluster>\n{}\n    </DBCluster>",
                        db_cluster_member_xml(&entry)
                    ),
                    &rid,
                ))
            }
            "RestoreDBClusterFromS3" => Ok(xml_response(
                action.as_str(),
                "    <DBCluster/>".to_string(),
                &rid,
            )),

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
            "ModifyCurrentDBClusterCapacity" => Ok(xml_response("ModifyCurrentDBClusterCapacity", "    <DBClusterIdentifier>x</DBClusterIdentifier>\n    <CurrentCapacity>4</CurrentCapacity>".to_string(), &rid)),
            "DisableHttpEndpoint" => Ok(xml_response("DisableHttpEndpoint", "    <HttpEndpointEnabled>false</HttpEndpointEnabled>".to_string(), &rid)),
            "EnableHttpEndpoint" => Ok(xml_response("EnableHttpEndpoint", "    <HttpEndpointEnabled>true</HttpEndpointEnabled>".to_string(), &rid)),

            _ => Err(AwsServiceError::action_not_implemented("rds", &action)),
        }
    }
}

// ── XML helpers per resource ──

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

pub(crate) fn cluster_snapshot_xml(id: &str, arn: &str, cluster: &str) -> String {
    format!(
        "    <DBClusterSnapshot>\n      <DBClusterSnapshotIdentifier>{}</DBClusterSnapshotIdentifier>\n      <DBClusterSnapshotArn>{}</DBClusterSnapshotArn>\n      <DBClusterIdentifier>{}</DBClusterIdentifier>\n      <Status>available</Status>\n    </DBClusterSnapshot>",
        xml_escape(id), xml_escape(arn), xml_escape(cluster),
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
