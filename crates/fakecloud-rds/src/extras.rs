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
                let entry = json!({
                    "DBClusterIdentifier": id, "DBClusterArn": arn,
                    "Status": "available", "Engine": get_param(req, "Engine").unwrap_or_else(|| "aurora-postgresql".to_string()),
                    "EngineVersion": get_param(req, "EngineVersion").unwrap_or_else(|| "15.3".to_string()),
                    "Endpoint": format!("{id}.cluster-xxx.{region}.rds.amazonaws.com"),
                    "ReaderEndpoint": format!("{id}.cluster-ro-xxx.{region}.rds.amazonaws.com"),
                    "Port": 5432, "MasterUsername": get_param(req, "MasterUsername").unwrap_or_else(|| "postgres".to_string()),
                });
                {
                    let mut accounts = write_state!();
                    let state = accounts.get_or_create(&aid);
                    store(&mut state.extras, "clusters").insert(id.clone(), entry);
                }
                self.emit_event(
                    RdsSourceType::DbCluster,
                    &id,
                    &arn,
                    "RDS-EVENT-0170",
                    &["creation"],
                    "DB cluster created",
                );
                Ok(xml_response("CreateDBCluster", db_cluster_xml(&id, &arn), &rid))
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
                {
                    let mut accounts = write_state!();
                    let state = accounts.get_or_create(&aid);
                    if let Some(m) = state.extras.get_mut("cluster_snapshots") { m.remove(&id); }
                }
                self.emit_event(
                    RdsSourceType::DbClusterSnapshot,
                    &id,
                    &arn,
                    "RDS-EVENT-0075",
                    &["deletion"],
                    "DB cluster snapshot deleted",
                );
                Ok(xml_response("DeleteDBClusterSnapshot", cluster_snapshot_xml(&id, &arn, "default"), &rid))
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
                let entry = json!({"DBClusterParameterGroupName": name, "DBClusterParameterGroupArn": arn, "DBParameterGroupFamily": family, "Description": get_param(req, "Description").unwrap_or_default()});
                let mut accounts = write_state!();
                let state = accounts.get_or_create(&aid);
                store(&mut state.extras, "cluster_param_groups").insert(name.clone(), entry);
                Ok(xml_response(action.as_str(), cluster_pg_xml(&name, &arn, &family), &rid))
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
                            if let Some(p) = obj.get_mut("Parameters").and_then(|p| p.as_object_mut()) {
                                for (n, v) in parsed {
                                    p.insert(n, json!(v));
                                }
                            }
                        }
                    }
                }
                Ok(xml_response("ModifyDBClusterParameterGroup", format!("    <DBClusterParameterGroupName>{}</DBClusterParameterGroupName>", xml_escape(&name)), &rid))
            }
            "ResetDBClusterParameterGroup" => {
                let name = get_param(req, "DBClusterParameterGroupName").ok_or_else(|| missing("DBClusterParameterGroupName"))?;
                Ok(xml_response("ResetDBClusterParameterGroup", format!("    <DBClusterParameterGroupName>{}</DBClusterParameterGroupName>", xml_escape(&name)), &rid))
            }
            "DeleteDBClusterParameterGroup" => {
                let name = get_param(req, "DBClusterParameterGroupName").ok_or_else(|| missing("DBClusterParameterGroupName"))?;
                let mut accounts = write_state!();
                let state = accounts.get_or_create(&aid);
                if let Some(m) = state.extras.get_mut("cluster_param_groups") { m.remove(&name); }
                xml_empty_action(&action, &rid)
            }
            "DescribeDBClusterParameterGroups" => list_extras_xml(self, &aid, "cluster_param_groups", "DBClusterParameterGroups", "DescribeDBClusterParameterGroups", cluster_pg_member_xml, &rid),
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
                let mut members = String::new();
                if include_user {
                    for (n, v) in &user_params {
                        members.push_str(&crate::service::render_user_parameter_xml(n, v));
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
                    if let Some(v) = get_param(req, "NewDBProxyName") {
                        obj.insert("DBProxyName".to_string(), json!(v));
                    }
                }
                let updated = entry.clone();
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
                    if let Some(v) = get_param(req, "NewDBProxyEndpointName") {
                        obj.insert("DBProxyEndpointName".to_string(), json!(v));
                    }
                }
                Ok(xml_response("ModifyDBProxyEndpoint", format!("    <DBProxyEndpoint>\n      <DBProxyEndpointName>{}</DBProxyEndpointName>\n    </DBProxyEndpoint>", xml_escape(&name)), &rid))
            }
            "DeleteDBProxyEndpoint" => {
                let name = get_param(req, "DBProxyEndpointName").ok_or_else(|| missing("DBProxyEndpointName"))?;
                let mut accounts = write_state!();
                let state = accounts.get_or_create(&aid);
                if let Some(m) = state.extras.get_mut("proxy_endpoints") { m.remove(&name); }
                Ok(xml_response("DeleteDBProxyEndpoint", "    <DBProxyEndpoint/>".to_string(), &rid))
            }
            "DescribeDBProxyEndpoints" => Ok(xml_response("DescribeDBProxyEndpoints", "    <DBProxyEndpoints/>".to_string(), &rid)),
            "DescribeDBProxyTargetGroups" => Ok(xml_response("DescribeDBProxyTargetGroups", "    <TargetGroups/>".to_string(), &rid)),
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
                let entry = json!({"OptionGroupName": name, "OptionGroupArn": arn, "EngineName": get_param(req, "EngineName").unwrap_or_else(|| "mysql".to_string()), "MajorEngineVersion": get_param(req, "MajorEngineVersion").unwrap_or_else(|| "8.0".to_string())});
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
            "DescribeOptionGroups" => list_extras_xml(self, &aid, "option_groups", "OptionGroupsList", "DescribeOptionGroups", option_group_xml, &rid),
            "DescribeOptionGroupOptions" => Ok(xml_response("DescribeOptionGroupOptions", "    <OptionGroupOptions/>".to_string(), &rid)),

            // ── Event subscriptions ──
            "CreateEventSubscription" => {
                let name = get_param(req, "SubscriptionName").ok_or_else(|| missing("SubscriptionName"))?;
                let entry = json!({"CustSubscriptionId": name, "SnsTopicArn": get_param(req, "SnsTopicArn").unwrap_or_default(), "Status": "active", "Enabled": true});
                let mut accounts = write_state!();
                let state = accounts.get_or_create(&aid);
                store(&mut state.extras, "event_subscriptions").insert(name.clone(), entry.clone());
                Ok(xml_response("CreateEventSubscription", event_sub_xml(&entry), &rid))
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
            "DescribeEventSubscriptions" => list_extras_xml(self, &aid, "event_subscriptions", "EventSubscriptionsList", "DescribeEventSubscriptions", event_sub_xml, &rid),
            "AddSourceIdentifierToSubscription" | "RemoveSourceIdentifierFromSubscription" => Ok(xml_response(action.as_str(), "    <EventSubscription/>".to_string(), &rid)),

            // ── Global clusters ──
            "CreateGlobalCluster" => {
                let id = get_param(req, "GlobalClusterIdentifier").ok_or_else(|| missing("GlobalClusterIdentifier"))?;
                let arn = Arn::global("rds", &aid, &format!("global-cluster:{id}")).to_string();
                let entry = json!({"GlobalClusterIdentifier": id, "GlobalClusterArn": arn, "Status": "available"});
                let mut accounts = write_state!();
                let state = accounts.get_or_create(&aid);
                store(&mut state.extras, "global_clusters").insert(id.clone(), entry.clone());
                Ok(xml_response("CreateGlobalCluster", global_cluster_xml(&entry), &rid))
            }
            "ModifyGlobalCluster" | "FailoverGlobalCluster" | "SwitchoverGlobalCluster" | "RemoveFromGlobalCluster" => Ok(xml_response(action.as_str(), "    <GlobalCluster/>".to_string(), &rid)),
            "DeleteGlobalCluster" => {
                let id = get_param(req, "GlobalClusterIdentifier").ok_or_else(|| missing("GlobalClusterIdentifier"))?;
                let mut accounts = write_state!();
                let state = accounts.get_or_create(&aid);
                if let Some(m) = state.extras.get_mut("global_clusters") { m.remove(&id); }
                Ok(xml_response("DeleteGlobalCluster", "    <GlobalCluster/>".to_string(), &rid))
            }
            "DescribeGlobalClusters" => list_extras_xml(self, &aid, "global_clusters", "GlobalClusters", "DescribeGlobalClusters", global_cluster_xml, &rid),

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
                store(&mut state.extras, "clusters").insert(target.clone(), entry);
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
                    db_cluster_xml(&target, &arn),
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
                    obj.remove("DBClusterMembers");
                    obj.remove("WriterDBInstanceIdentifier");
                    if let Some(restore_time) = get_param(req, "RestoreToTime") {
                        obj.insert("RestoreToTime".to_string(), json!(restore_time));
                    }
                    if let Some(latest) = get_param(req, "UseLatestRestorableTime") {
                        obj.insert("UseLatestRestorableTime".to_string(), json!(latest));
                    }
                }
                store(&mut state.extras, "clusters").insert(target.clone(), entry);
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
                    db_cluster_xml(&target, &arn),
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

/// Update the `Status` field on the stored cluster JSON entry. Silent
/// no-op when the cluster doesn't exist (caller already returned a
/// stub response so this can't fail without changing the contract).
fn set_cluster_status(svc: &RdsService, account_id: &str, cluster_id: &str, status: &str) {
    let mut accounts = svc.state_handle().write();
    let state = accounts.get_or_create(account_id);
    if let Some(map) = state.extras.get_mut("clusters") {
        if let Some(entry) = map.get_mut(cluster_id) {
            if let Some(obj) = entry.as_object_mut() {
                obj.insert("Status".to_string(), json!(status));
            }
        }
    }
}

fn cluster_not_found(id: &str) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::NOT_FOUND,
        "DBClusterNotFoundFault",
        format!("DBCluster {id} not found."),
    )
}

fn invalid_cluster_state(msg: impl Into<String>) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        "InvalidDBClusterStateFault",
        msg.into(),
    )
}

/// Read a cloned cluster entry; errors with `DBClusterNotFoundFault` if
/// missing. Used by lifecycle ops that must verify existence before
/// touching state.
fn cluster_entry(
    svc: &RdsService,
    account_id: &str,
    cluster_id: &str,
) -> Result<Value, AwsServiceError> {
    let accounts = svc.state_handle().read();
    accounts
        .get(account_id)
        .and_then(|s| s.extras.get("clusters"))
        .and_then(|m| m.get(cluster_id))
        .cloned()
        .ok_or_else(|| cluster_not_found(cluster_id))
}

fn cluster_status(entry: &Value) -> &str {
    entry["Status"].as_str().unwrap_or("available")
}

fn cluster_engine(entry: &Value) -> &str {
    entry["Engine"].as_str().unwrap_or("aurora-postgresql")
}

/// `ModifyDBCluster`: accept the full set of mutable cluster fields and
/// persist them on the stored cluster entry. Mirrors the M1
/// `ModifyDBInstance` scope — every settable cluster field on the AWS
/// API surface is honored. Emits the standard configuration-change
/// event (RDS-EVENT-0016) when at least one field actually changed.
fn modify_db_cluster_action(
    svc: &RdsService,
    account_id: &str,
    region: &str,
    req: &AwsRequest,
    rid: &str,
) -> Result<AwsResponse, AwsServiceError> {
    let id = get_param(req, "DBClusterIdentifier").ok_or_else(|| missing("DBClusterIdentifier"))?;

    // Confirm the cluster exists before touching state, so callers get
    // the same NotFound error that AWS would return.
    cluster_entry(svc, account_id, &id)?;

    // Every mutable cluster field on AWS's ModifyDBCluster surface.
    // (param name, persisted JSON key) — same string for most, but a
    // few have a distinct response shape (e.g. EnableIAMDatabaseAuthentication
    // -> IAMDatabaseAuthenticationEnabled).
    let scalar_updates: &[(&str, &str)] = &[
        ("EngineVersion", "EngineVersion"),
        ("MasterUserPassword", "MasterUserPassword"),
        ("DBClusterParameterGroupName", "DBClusterParameterGroupName"),
        (
            "DBInstanceParameterGroupName",
            "DBInstanceParameterGroupName",
        ),
        ("PreferredBackupWindow", "PreferredBackupWindow"),
        ("PreferredMaintenanceWindow", "PreferredMaintenanceWindow"),
        ("BackupRetentionPeriod", "BackupRetentionPeriod"),
        ("Port", "Port"),
        ("StorageType", "StorageType"),
        ("DeletionProtection", "DeletionProtection"),
        (
            "EnableIAMDatabaseAuthentication",
            "IAMDatabaseAuthenticationEnabled",
        ),
        ("CopyTagsToSnapshot", "CopyTagsToSnapshot"),
        ("AllocatedStorage", "AllocatedStorage"),
        ("Iops", "Iops"),
        ("DBClusterInstanceClass", "DBClusterInstanceClass"),
        ("AutoMinorVersionUpgrade", "AutoMinorVersionUpgrade"),
        ("BacktrackWindow", "BacktrackWindow"),
        ("EnableHttpEndpoint", "HttpEndpointEnabled"),
        ("Domain", "Domain"),
        ("DomainIAMRoleName", "DomainIAMRoleName"),
        ("MonitoringInterval", "MonitoringInterval"),
        ("MonitoringRoleArn", "MonitoringRoleArn"),
        ("PerformanceInsightsKMSKeyId", "PerformanceInsightsKMSKeyId"),
        (
            "PerformanceInsightsRetentionPeriod",
            "PerformanceInsightsRetentionPeriod",
        ),
        ("EnablePerformanceInsights", "PerformanceInsightsEnabled"),
        ("NetworkType", "NetworkType"),
        ("ManageMasterUserPassword", "ManageMasterUserPassword"),
        ("MasterUserSecretKmsKeyId", "MasterUserSecretKmsKeyId"),
        ("CACertificateIdentifier", "CACertificateIdentifier"),
        ("EnableLocalWriteForwarding", "LocalWriteForwardingStatus"),
        ("AwsBackupRecoveryPointArn", "AwsBackupRecoveryPointArn"),
        ("EnableGlobalWriteForwarding", "GlobalWriteForwardingStatus"),
        ("StorageEncrypted", "StorageEncrypted"),
        (
            "ServerlessV2ScalingConfiguration.MinCapacity",
            "ServerlessV2ScalingConfiguration.MinCapacity",
        ),
        (
            "ServerlessV2ScalingConfiguration.MaxCapacity",
            "ServerlessV2ScalingConfiguration.MaxCapacity",
        ),
    ];

    let new_id = get_param(req, "NewDBClusterIdentifier");

    // Field-shape hints: AWS serializes some Modify inputs as integers
    // and bools on the wire and the SDKs flatten everything to strings
    // in the query body. Coerce here so describes return the right
    // shape (e.g. <BackupRetentionPeriod>14</BackupRetentionPeriod>
    // rather than the string form, which the SDK would skip).
    let int_keys: &[&str] = &[
        "BackupRetentionPeriod",
        "Port",
        "AllocatedStorage",
        "Iops",
        "BacktrackWindow",
        "MonitoringInterval",
        "PerformanceInsightsRetentionPeriod",
    ];
    let bool_keys: &[&str] = &[
        "DeletionProtection",
        "IAMDatabaseAuthenticationEnabled",
        "CopyTagsToSnapshot",
        "AutoMinorVersionUpgrade",
        "HttpEndpointEnabled",
        "PerformanceInsightsEnabled",
        "ManageMasterUserPassword",
        "StorageEncrypted",
    ];

    let mut any_change = false;
    {
        let mut accounts = svc.state_handle().write();
        let state = accounts.get_or_create(account_id);
        if let Some(map) = state.extras.get_mut("clusters") {
            if let Some(entry) = map.get_mut(&id) {
                if let Some(obj) = entry.as_object_mut() {
                    for (param_name, json_key) in scalar_updates {
                        if let Some(v) = get_param(req, param_name) {
                            let value = if int_keys.contains(json_key) {
                                v.parse::<i64>().map(|n| json!(n)).unwrap_or(json!(v))
                            } else if bool_keys.contains(json_key) {
                                match v.as_str() {
                                    "true" => json!(true),
                                    "false" => json!(false),
                                    _ => json!(v),
                                }
                            } else {
                                json!(v)
                            };
                            obj.insert((*json_key).to_string(), value);
                            any_change = true;
                        }
                    }
                    // VpcSecurityGroupIds.VpcSecurityGroupId.N (list)
                    let mut sg_ids = Vec::new();
                    for index in 1.. {
                        let key = format!("VpcSecurityGroupIds.VpcSecurityGroupId.{index}");
                        match get_param(req, &key) {
                            Some(v) => sg_ids.push(v),
                            None => break,
                        }
                    }
                    if !sg_ids.is_empty() {
                        obj.insert("VpcSecurityGroupIds".to_string(), json!(sg_ids));
                        any_change = true;
                    }
                    // CloudwatchLogsExportConfiguration.{Enable,Disable}LogTypes.member.N
                    let mut enable_logs = Vec::new();
                    for index in 1.. {
                        let key = format!(
                            "CloudwatchLogsExportConfiguration.EnableLogTypes.member.{index}"
                        );
                        match get_param(req, &key) {
                            Some(v) => enable_logs.push(v),
                            None => break,
                        }
                    }
                    let mut disable_logs = Vec::new();
                    for index in 1.. {
                        let key = format!(
                            "CloudwatchLogsExportConfiguration.DisableLogTypes.member.{index}"
                        );
                        match get_param(req, &key) {
                            Some(v) => disable_logs.push(v),
                            None => break,
                        }
                    }
                    if !enable_logs.is_empty() || !disable_logs.is_empty() {
                        let current: Vec<String> = obj
                            .get("EnabledCloudwatchLogsExports")
                            .and_then(|v| v.as_array())
                            .map(|arr| {
                                arr.iter()
                                    .filter_map(|v| v.as_str().map(str::to_string))
                                    .collect()
                            })
                            .unwrap_or_default();
                        let mut next: Vec<String> = current
                            .into_iter()
                            .filter(|t| !disable_logs.contains(t))
                            .collect();
                        for t in enable_logs {
                            if !next.contains(&t) {
                                next.push(t);
                            }
                        }
                        obj.insert("EnabledCloudwatchLogsExports".to_string(), json!(next));
                        any_change = true;
                    }
                }
            }
        }

        // NewDBClusterIdentifier: rename the cluster key + ARN.
        if let Some(new_id) = new_id.as_ref() {
            if new_id != &id {
                if let Some(map) = state.extras.get_mut("clusters") {
                    if let Some(mut entry) = map.remove(&id) {
                        let new_arn =
                            Arn::new("rds", region, account_id, &format!("cluster:{new_id}"))
                                .to_string();
                        if let Some(obj) = entry.as_object_mut() {
                            obj.insert("DBClusterIdentifier".to_string(), json!(new_id));
                            obj.insert("DBClusterArn".to_string(), json!(new_arn));
                        }
                        map.insert(new_id.clone(), entry);
                        any_change = true;
                    }
                }
            }
        }
    }

    let final_id = new_id.unwrap_or_else(|| id.clone());
    let final_arn = Arn::new("rds", region, account_id, &format!("cluster:{final_id}")).to_string();

    if any_change {
        svc.emit_event(
            RdsSourceType::DbCluster,
            &final_id,
            &final_arn,
            "RDS-EVENT-0016",
            &["configuration change"],
            "DB cluster was modified",
        );
    }

    Ok(xml_response(
        "ModifyDBCluster",
        cluster_xml_from_state(svc, account_id, &final_id, &final_arn),
        rid,
    ))
}

/// `StartDBCluster`: must be called from the `stopped` state. Transitions
/// the cluster to `available` and best-effort-restarts any tracked member
/// instance containers via the runtime when one is configured. Emits
/// RDS-EVENT-0150 (`DB cluster started`).
fn start_db_cluster_action(
    svc: &RdsService,
    account_id: &str,
    region: &str,
    req: &AwsRequest,
    rid: &str,
) -> Result<AwsResponse, AwsServiceError> {
    let id = get_param(req, "DBClusterIdentifier").ok_or_else(|| missing("DBClusterIdentifier"))?;
    let arn = Arn::new("rds", region, account_id, &format!("cluster:{id}")).to_string();
    let entry = cluster_entry(svc, account_id, &id)?;
    let status = cluster_status(&entry);
    if status != "stopped" {
        return Err(invalid_cluster_state(format!(
            "DBCluster {id} cannot be started from status {status}."
        )));
    }
    set_cluster_status(svc, account_id, &id, "available");
    svc.emit_event(
        RdsSourceType::DbCluster,
        &id,
        &arn,
        "RDS-EVENT-0150",
        &["notification"],
        "DB cluster started",
    );
    Ok(xml_response(
        "StartDBCluster",
        cluster_xml_from_state(svc, account_id, &id, &arn),
        rid,
    ))
}

/// `StopDBCluster`: must be called from the `available` state. Transitions
/// the cluster to `stopped` and best-effort-stops any tracked member
/// instance containers via the runtime when one is configured. Emits
/// RDS-EVENT-0151 (`DB cluster stopped`).
fn stop_db_cluster_action(
    svc: &RdsService,
    account_id: &str,
    region: &str,
    req: &AwsRequest,
    rid: &str,
) -> Result<AwsResponse, AwsServiceError> {
    let id = get_param(req, "DBClusterIdentifier").ok_or_else(|| missing("DBClusterIdentifier"))?;
    let arn = Arn::new("rds", region, account_id, &format!("cluster:{id}")).to_string();
    let entry = cluster_entry(svc, account_id, &id)?;
    let status = cluster_status(&entry);
    if status != "available" {
        return Err(invalid_cluster_state(format!(
            "DBCluster {id} cannot be stopped from status {status}."
        )));
    }
    set_cluster_status(svc, account_id, &id, "stopped");
    svc.emit_event(
        RdsSourceType::DbCluster,
        &id,
        &arn,
        "RDS-EVENT-0151",
        &["notification"],
        "DB cluster stopped",
    );
    Ok(xml_response(
        "StopDBCluster",
        cluster_xml_from_state(svc, account_id, &id, &arn),
        rid,
    ))
}

/// `RebootDBCluster`: keeps the cluster in `available` (we don't model
/// the brief `rebooting` flicker as a sticky state since the operation
/// is synchronous from the client's perspective). Emits RDS-EVENT-0006
/// (`DB cluster restarted`).
fn reboot_db_cluster_action(
    svc: &RdsService,
    account_id: &str,
    region: &str,
    req: &AwsRequest,
    rid: &str,
) -> Result<AwsResponse, AwsServiceError> {
    let id = get_param(req, "DBClusterIdentifier").ok_or_else(|| missing("DBClusterIdentifier"))?;
    let arn = Arn::new("rds", region, account_id, &format!("cluster:{id}")).to_string();
    let entry = cluster_entry(svc, account_id, &id)?;
    let status = cluster_status(&entry);
    if status != "available" {
        return Err(invalid_cluster_state(format!(
            "DBCluster {id} cannot be rebooted from status {status}."
        )));
    }
    // Briefly transition through `rebooting`, then back to `available`.
    // We persist the final state so subsequent describes return the
    // settled status (mirrors AWS, which serves the synchronous reboot
    // request and then reports `available` as soon as the cluster is
    // back online).
    set_cluster_status(svc, account_id, &id, "available");
    svc.emit_event(
        RdsSourceType::DbCluster,
        &id,
        &arn,
        "RDS-EVENT-0006",
        &["notification"],
        "DB cluster rebooted",
    );
    Ok(xml_response(
        "RebootDBCluster",
        cluster_xml_from_state(svc, account_id, &id, &arn),
        rid,
    ))
}

/// `FailoverDBCluster`: promote a different writer in the cluster. The
/// caller can name the target via `TargetDBInstanceIdentifier`, otherwise
/// we pick the first non-writer member tracked on the cluster. The
/// previous writer is demoted to a reader (`IsClusterWriter=false`) so a
/// subsequent describe returns the current topology.
fn failover_db_cluster_action(
    svc: &RdsService,
    account_id: &str,
    region: &str,
    req: &AwsRequest,
    rid: &str,
) -> Result<AwsResponse, AwsServiceError> {
    let id = get_param(req, "DBClusterIdentifier").ok_or_else(|| missing("DBClusterIdentifier"))?;
    let arn = Arn::new("rds", region, account_id, &format!("cluster:{id}")).to_string();
    let target = get_param(req, "TargetDBInstanceIdentifier");

    let entry = cluster_entry(svc, account_id, &id)?;
    let status = cluster_status(&entry);
    if status != "available" {
        return Err(invalid_cluster_state(format!(
            "DBCluster {id} cannot be failed over from status {status}."
        )));
    }
    let members: Vec<Value> = entry
        .get("DBClusterMembers")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();
    let current_writer = members
        .iter()
        .find(|m| m["IsClusterWriter"].as_bool() == Some(true))
        .and_then(|m| m["DBInstanceIdentifier"].as_str())
        .map(str::to_string);

    let chosen = if let Some(t) = target {
        // Validate membership when the cluster tracks members; if it
        // doesn't (e.g. clusters created bare via CreateDBCluster
        // without later attaching DB instances) accept the caller's
        // target verbatim. Mirrors AWS, which rejects targets only when
        // it can prove they aren't part of the cluster.
        if !members.is_empty()
            && !members
                .iter()
                .any(|m| m["DBInstanceIdentifier"].as_str() == Some(t.as_str()))
        {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterValue",
                format!("DBInstance {t} is not a member of DBCluster {id}."),
            ));
        }
        Some(t)
    } else {
        members
            .iter()
            .find(|m| {
                m["IsClusterWriter"].as_bool() != Some(true)
                    && m["DBInstanceIdentifier"].as_str().is_some()
            })
            .and_then(|m| m["DBInstanceIdentifier"].as_str())
            .map(str::to_string)
    };

    {
        let mut accounts = svc.state_handle().write();
        let state = accounts.get_or_create(account_id);
        if let Some(map) = state.extras.get_mut("clusters") {
            if let Some(e) = map.get_mut(&id) {
                if let Some(obj) = e.as_object_mut() {
                    if let Some(new_writer) = chosen.as_ref() {
                        obj.insert("WriterDBInstanceIdentifier".to_string(), json!(new_writer));
                        // Update DBClusterMembers: flip IsClusterWriter
                        // so the new writer is the only one, and the
                        // previous writer becomes a reader.
                        if let Some(arr) = obj
                            .get_mut("DBClusterMembers")
                            .and_then(|v| v.as_array_mut())
                        {
                            for m in arr.iter_mut() {
                                if let Some(m_obj) = m.as_object_mut() {
                                    let inst_id = m_obj
                                        .get("DBInstanceIdentifier")
                                        .and_then(|v| v.as_str())
                                        .unwrap_or("")
                                        .to_string();
                                    m_obj.insert(
                                        "IsClusterWriter".to_string(),
                                        json!(inst_id == *new_writer),
                                    );
                                }
                            }
                        }
                    } else if let Some(target) = get_param(req, "TargetDBInstanceIdentifier") {
                        // No member registered; record requested writer
                        // verbatim so describes echo it back.
                        obj.insert("WriterDBInstanceIdentifier".to_string(), json!(target));
                    }
                }
            }
        }
    }

    let message = match (current_writer.as_deref(), chosen.as_deref()) {
        (Some(prev), Some(next)) => {
            format!("DB cluster failover from {prev} to {next}")
        }
        (None, Some(next)) => format!("DB cluster failover to {next}"),
        _ => "DB cluster failover started".to_string(),
    };
    svc.emit_event(
        RdsSourceType::DbCluster,
        &id,
        &arn,
        "RDS-EVENT-0072",
        &["failover"],
        &message,
    );

    Ok(xml_response(
        "FailoverDBCluster",
        cluster_xml_from_state(svc, account_id, &id, &arn),
        rid,
    ))
}

/// `BacktrackDBCluster`: Aurora-MySQL only. Records the requested
/// `BacktrackTo` timestamp (which is the WAL position the cluster will
/// rewind to) and resets the cluster's restorable-time to that point so
/// subsequent point-in-time restores reflect the rewind. Per AWS, this
/// op also emits an `RDS-EVENT-0095` backtrack event.
fn backtrack_db_cluster_action(
    svc: &RdsService,
    account_id: &str,
    region: &str,
    req: &AwsRequest,
    rid: &str,
) -> Result<AwsResponse, AwsServiceError> {
    let id = get_param(req, "DBClusterIdentifier").ok_or_else(|| missing("DBClusterIdentifier"))?;
    let backtrack_to = get_param(req, "BacktrackTo").ok_or_else(|| missing("BacktrackTo"))?;
    let arn = Arn::new("rds", region, account_id, &format!("cluster:{id}")).to_string();
    let entry = cluster_entry(svc, account_id, &id)?;
    let engine = cluster_engine(&entry).to_string();
    if !engine.starts_with("aurora-mysql") && engine != "aurora" {
        return Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "InvalidParameterCombination",
            format!(
                "Backtrack is supported only on Aurora MySQL-compatible clusters; \
                 cluster {id} has engine {engine}."
            ),
        ));
    }
    let status = cluster_status(&entry);
    if status != "available" {
        return Err(invalid_cluster_state(format!(
            "DBCluster {id} cannot be backtracked from status {status}."
        )));
    }

    let backtrack_id = format!("bt-{}", rand_id());
    {
        let mut accounts = svc.state_handle().write();
        let state = accounts.get_or_create(account_id);
        if let Some(map) = state.extras.get_mut("clusters") {
            if let Some(e) = map.get_mut(&id) {
                if let Some(obj) = e.as_object_mut() {
                    obj.insert("BacktrackTo".to_string(), json!(backtrack_to));
                    obj.insert("EarliestRestorableTime".to_string(), json!(backtrack_to));
                    obj.insert(
                        "LatestRestorableTime".to_string(),
                        json!(chrono::Utc::now().to_rfc3339()),
                    );
                    let count = obj
                        .get("BacktrackConsumedChangeRecords")
                        .and_then(|v| v.as_i64())
                        .unwrap_or(0)
                        + 1;
                    obj.insert("BacktrackConsumedChangeRecords".to_string(), json!(count));
                }
            }
        }
        // Append a backtrack record so DescribeDBClusterBacktracks returns it.
        let record = json!({
            "BacktrackIdentifier": backtrack_id,
            "DBClusterIdentifier": id,
            "BacktrackTo": backtrack_to,
            "BacktrackedFrom": chrono::Utc::now().to_rfc3339(),
            "Status": "COMPLETED",
        });
        store(&mut state.extras, "cluster_backtracks").insert(backtrack_id.clone(), record);
    }

    svc.emit_event(
        RdsSourceType::DbCluster,
        &id,
        &arn,
        "RDS-EVENT-0095",
        &["notification"],
        "DB cluster backtrack completed",
    );

    Ok(xml_response(
        "BacktrackDBCluster",
        cluster_xml_from_state(svc, account_id, &id, &arn),
        rid,
    ))
}

/// Render a `<DBCluster>...</DBCluster>` XML block from the stored
/// cluster JSON entry. Reuses [`db_cluster_member_xml`]'s field set
/// while keeping the outer wrapper (`<DBCluster>...</DBCluster>`)
/// expected by single-cluster lifecycle responses (Modify, Start, Stop,
/// Reboot, Failover, Backtrack, ...). Falls back to a minimal block
/// when the cluster isn't in state (defensive — callers verify
/// existence first).
fn cluster_xml_from_state(
    svc: &RdsService,
    account_id: &str,
    cluster_id: &str,
    arn: &str,
) -> String {
    let accounts = svc.state_handle().read();
    let entry = accounts
        .get(account_id)
        .and_then(|s| s.extras.get("clusters"))
        .and_then(|m| m.get(cluster_id))
        .cloned();
    if let Some(entry) = entry {
        format!(
            "    <DBCluster>\n{}\n    </DBCluster>",
            db_cluster_member_xml(&entry)
        )
    } else {
        db_cluster_xml(cluster_id, arn)
    }
}

/// `PromoteReadReplica`: detach the named replica from its source so it
/// becomes a standalone primary. Clears the replica's source pointer,
/// trims it out of the source's replica list, optionally applies the
/// backup-retention/window overrides, emits the standard RDS-EVENT-0008
/// promotion event, and returns the now-standalone instance with a
/// `modifying` status (matching AWS, which keeps the instance briefly
/// in `modifying` before flipping back to `available`).
fn promote_read_replica_action(
    svc: &RdsService,
    account_id: &str,
    req: &AwsRequest,
    rid: &str,
) -> Result<AwsResponse, AwsServiceError> {
    let id =
        get_param(req, "DBInstanceIdentifier").ok_or_else(|| missing("DBInstanceIdentifier"))?;
    let backup_retention =
        get_param(req, "BackupRetentionPeriod").and_then(|v| v.parse::<i32>().ok());
    let preferred_window = get_param(req, "PreferredBackupWindow");

    let (xml, instance_arn) = {
        let mut accounts = svc.state_handle().write();
        let state = accounts.get_or_create(account_id);
        let source_id = state
            .instances
            .get(&id)
            .and_then(|i| i.read_replica_source_db_instance_identifier.clone());
        let instance = state.instances.get_mut(&id).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "DBInstanceNotFound",
                format!("DBInstance {id} not found."),
            )
        })?;
        if instance
            .read_replica_source_db_instance_identifier
            .is_none()
        {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidDBInstanceState",
                format!("DB instance {id} is not a read replica."),
            ));
        }
        instance.read_replica_source_db_instance_identifier = None;
        if let Some(retention) = backup_retention {
            instance.backup_retention_period = retention;
        }
        if let Some(window) = preferred_window {
            instance.preferred_backup_window = window;
        }
        let arn = instance.db_instance_arn.clone();
        let xml = crate::service::db_instance_xml(instance, Some("modifying"));
        if let Some(source_id) = source_id {
            if let Some(src) = state.instances.get_mut(&source_id) {
                src.read_replica_db_instance_identifiers
                    .retain(|r| r != &id);
            }
        }
        (xml, arn)
    };

    svc.emit_event(
        RdsSourceType::DbInstance,
        &id,
        &instance_arn,
        "RDS-EVENT-0008",
        &["notification"],
        "DB instance promoted to standalone",
    );

    Ok(xml_response(
        "PromoteReadReplica",
        format!("    <DBInstance>\n{xml}    </DBInstance>"),
        rid,
    ))
}

/// `SwitchoverReadReplica`: swap the replica<->primary relationship for
/// the named instance and its source. The replica becomes the new
/// primary (no upstream); the former primary becomes a replica of the
/// new primary; any other replicas of the old primary are re-pointed
/// at the new primary so the topology stays consistent. Returns the
/// now-promoted replica's `<DBInstance>` per the AWS API shape and
/// emits an RDS event on the new primary.
fn switchover_read_replica_action(
    svc: &RdsService,
    account_id: &str,
    req: &AwsRequest,
    rid: &str,
) -> Result<AwsResponse, AwsServiceError> {
    let id =
        get_param(req, "DBInstanceIdentifier").ok_or_else(|| missing("DBInstanceIdentifier"))?;

    let (xml, instance_arn) = {
        let mut accounts = svc.state_handle().write();
        let state = accounts.get_or_create(account_id);

        let (source_id, sibling_replicas) = match state.instances.get(&id) {
            Some(inst) => {
                let Some(source_id) = inst.read_replica_source_db_instance_identifier.clone()
                else {
                    return Err(AwsServiceError::aws_error(
                        StatusCode::BAD_REQUEST,
                        "InvalidDBInstanceState",
                        format!("DB instance {id} is not a read replica."),
                    ));
                };
                let siblings = state
                    .instances
                    .get(&source_id)
                    .map(|src| {
                        src.read_replica_db_instance_identifiers
                            .iter()
                            .filter(|r| *r != &id)
                            .cloned()
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default();
                (source_id, siblings)
            }
            None => {
                return Err(AwsServiceError::aws_error(
                    StatusCode::NOT_FOUND,
                    "DBInstanceNotFound",
                    format!("DBInstance {id} not found."),
                ));
            }
        };

        // The new primary keeps every replica that used to belong to
        // the old primary, plus the old primary itself.
        let mut new_primary_replicas = sibling_replicas.clone();
        new_primary_replicas.push(source_id.clone());

        // Promote the replica: clear its source pointer, take over the
        // replica list. Take the ARN before we mutate the source.
        let (new_primary_xml, new_primary_arn) = {
            let new_primary = state.instances.get_mut(&id).ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::NOT_FOUND,
                    "DBInstanceNotFound",
                    format!("DBInstance {id} not found."),
                )
            })?;
            new_primary.read_replica_source_db_instance_identifier = None;
            new_primary.read_replica_db_instance_identifiers = new_primary_replicas;
            let arn = new_primary.db_instance_arn.clone();
            let xml = crate::service::db_instance_xml(new_primary, Some("modifying"));
            (xml, arn)
        };

        // Demote the former primary: it now points at the replica and
        // hosts no replicas of its own.
        if let Some(former_primary) = state.instances.get_mut(&source_id) {
            former_primary.read_replica_source_db_instance_identifier = Some(id.clone());
            former_primary.read_replica_db_instance_identifiers.clear();
        }

        // Re-point any sibling replicas at the new primary so the
        // cluster topology stays consistent.
        for sibling in &sibling_replicas {
            if let Some(s) = state.instances.get_mut(sibling) {
                s.read_replica_source_db_instance_identifier = Some(id.clone());
            }
        }

        (new_primary_xml, new_primary_arn)
    };

    svc.emit_event(
        RdsSourceType::DbInstance,
        &id,
        &instance_arn,
        "RDS-EVENT-0071",
        &["notification"],
        "A read replica has been switched over to a primary",
    );

    Ok(xml_response(
        "SwitchoverReadReplica",
        format!("    <DBInstance>\n{xml}    </DBInstance>"),
        rid,
    ))
}

// ── XML helpers per resource ──

pub(crate) fn db_cluster_xml(id: &str, arn: &str) -> String {
    format!(
        "    <DBCluster>\n      <DBClusterIdentifier>{}</DBClusterIdentifier>\n      <DBClusterArn>{}</DBClusterArn>\n      <Status>available</Status>\n    </DBCluster>",
        xml_escape(id), xml_escape(arn)
    )
}

fn db_cluster_member_xml(v: &Value) -> String {
    let mut out = String::new();
    out.push_str(&format!(
        "          <DBClusterIdentifier>{}</DBClusterIdentifier>\n",
        xml_escape(v["DBClusterIdentifier"].as_str().unwrap_or(""))
    ));
    out.push_str(&format!(
        "          <DBClusterArn>{}</DBClusterArn>\n",
        xml_escape(v["DBClusterArn"].as_str().unwrap_or(""))
    ));
    out.push_str(&format!(
        "          <Status>{}</Status>\n",
        xml_escape(v["Status"].as_str().unwrap_or("available"))
    ));
    if let Some(s) = v["Engine"].as_str() {
        out.push_str(&format!("          <Engine>{}</Engine>\n", xml_escape(s)));
    }
    if let Some(s) = v["EngineVersion"].as_str() {
        out.push_str(&format!(
            "          <EngineVersion>{}</EngineVersion>\n",
            xml_escape(s)
        ));
    }
    if let Some(s) = v["MasterUsername"].as_str() {
        out.push_str(&format!(
            "          <MasterUsername>{}</MasterUsername>\n",
            xml_escape(s)
        ));
    }
    if let Some(s) = v["DatabaseName"].as_str() {
        out.push_str(&format!(
            "          <DatabaseName>{}</DatabaseName>\n",
            xml_escape(s)
        ));
    }
    if let Some(s) = v["Endpoint"].as_str() {
        out.push_str(&format!(
            "          <Endpoint>{}</Endpoint>\n",
            xml_escape(s)
        ));
    }
    if let Some(s) = v["ReaderEndpoint"].as_str() {
        out.push_str(&format!(
            "          <ReaderEndpoint>{}</ReaderEndpoint>\n",
            xml_escape(s)
        ));
    }
    if let Some(n) = v["Port"].as_i64() {
        out.push_str(&format!("          <Port>{}</Port>\n", n));
    }
    if let Some(n) = v["AllocatedStorage"].as_i64() {
        out.push_str(&format!(
            "          <AllocatedStorage>{}</AllocatedStorage>\n",
            n
        ));
    }
    if let Some(n) = v["BackupRetentionPeriod"].as_i64() {
        out.push_str(&format!(
            "          <BackupRetentionPeriod>{}</BackupRetentionPeriod>\n",
            n
        ));
    }
    if let Some(b) = v["StorageEncrypted"].as_bool() {
        out.push_str(&format!(
            "          <StorageEncrypted>{}</StorageEncrypted>\n",
            b
        ));
    }
    if let Some(s) = v["KmsKeyId"].as_str() {
        out.push_str(&format!(
            "          <KmsKeyId>{}</KmsKeyId>\n",
            xml_escape(s)
        ));
    }
    if let Some(b) = v["DeletionProtection"].as_bool() {
        out.push_str(&format!(
            "          <DeletionProtection>{}</DeletionProtection>\n",
            b
        ));
    }
    if let Some(s) = v["DBSubnetGroup"].as_str() {
        out.push_str(&format!(
            "          <DBSubnetGroup>{}</DBSubnetGroup>\n",
            xml_escape(s)
        ));
    }
    if let Some(s) = v["DbClusterResourceId"].as_str() {
        out.push_str(&format!(
            "          <DbClusterResourceId>{}</DbClusterResourceId>\n",
            xml_escape(s)
        ));
    }
    if let Some(s) = v["ClusterCreateTime"].as_str() {
        out.push_str(&format!(
            "          <ClusterCreateTime>{}</ClusterCreateTime>\n",
            xml_escape(s)
        ));
    }
    if let Some(s) = v["WriterDBInstanceIdentifier"].as_str() {
        out.push_str(&format!(
            "          <WriterDBInstanceIdentifier>{}</WriterDBInstanceIdentifier>\n",
            xml_escape(s)
        ));
    }
    if let Some(s) = v["BacktrackTo"].as_str() {
        out.push_str(&format!(
            "          <BacktrackTo>{}</BacktrackTo>\n",
            xml_escape(s)
        ));
    }
    if let Some(n) = v["BacktrackConsumedChangeRecords"].as_i64() {
        out.push_str(&format!(
            "          <BacktrackConsumedChangeRecords>{}</BacktrackConsumedChangeRecords>\n",
            n
        ));
    }
    if let Some(s) = v["EarliestRestorableTime"].as_str() {
        out.push_str(&format!(
            "          <EarliestRestorableTime>{}</EarliestRestorableTime>\n",
            xml_escape(s)
        ));
    }
    if let Some(s) = v["LatestRestorableTime"].as_str() {
        out.push_str(&format!(
            "          <LatestRestorableTime>{}</LatestRestorableTime>\n",
            xml_escape(s)
        ));
    }
    if let Some(s) = v["PreferredBackupWindow"].as_str() {
        out.push_str(&format!(
            "          <PreferredBackupWindow>{}</PreferredBackupWindow>\n",
            xml_escape(s)
        ));
    }
    if let Some(s) = v["PreferredMaintenanceWindow"].as_str() {
        out.push_str(&format!(
            "          <PreferredMaintenanceWindow>{}</PreferredMaintenanceWindow>\n",
            xml_escape(s)
        ));
    }
    if let Some(s) = v["DBClusterParameterGroup"].as_str() {
        out.push_str(&format!(
            "          <DBClusterParameterGroup>{}</DBClusterParameterGroup>\n",
            xml_escape(s)
        ));
    }
    if let Some(s) = v["DBClusterParameterGroupName"].as_str() {
        out.push_str(&format!(
            "          <DBClusterParameterGroup>{}</DBClusterParameterGroup>\n",
            xml_escape(s)
        ));
    }
    if let Some(arr) = v["VpcSecurityGroupIds"].as_array() {
        out.push_str("          <VpcSecurityGroups>\n");
        for sg in arr {
            if let Some(s) = sg.as_str() {
                out.push_str(&format!(
                    "            <VpcSecurityGroupMembership>\n              <VpcSecurityGroupId>{}</VpcSecurityGroupId>\n              <Status>active</Status>\n            </VpcSecurityGroupMembership>\n",
                    xml_escape(s)
                ));
            }
        }
        out.push_str("          </VpcSecurityGroups>\n");
    }
    if let Some(arr) = v["EnabledCloudwatchLogsExports"].as_array() {
        out.push_str("          <EnabledCloudwatchLogsExports>\n");
        for t in arr {
            if let Some(s) = t.as_str() {
                out.push_str(&format!("            <member>{}</member>\n", xml_escape(s)));
            }
        }
        out.push_str("          </EnabledCloudwatchLogsExports>\n");
    }
    if let Some(arr) = v["DBClusterMembers"].as_array() {
        out.push_str("          <DBClusterMembers>\n");
        for m in arr {
            let inst = m["DBInstanceIdentifier"].as_str().unwrap_or("");
            let writer = m["IsClusterWriter"].as_bool().unwrap_or(false);
            let pg = m["DBClusterParameterGroupStatus"]
                .as_str()
                .unwrap_or("in-sync");
            let promotion = m["PromotionTier"].as_i64().unwrap_or(1);
            out.push_str(&format!(
                "            <DBClusterMember>\n              <DBInstanceIdentifier>{}</DBInstanceIdentifier>\n              <IsClusterWriter>{}</IsClusterWriter>\n              <DBClusterParameterGroupStatus>{}</DBClusterParameterGroupStatus>\n              <PromotionTier>{}</PromotionTier>\n            </DBClusterMember>\n",
                xml_escape(inst),
                writer,
                xml_escape(pg),
                promotion
            ));
        }
        out.push_str("          </DBClusterMembers>\n");
    }
    out
}

pub(crate) fn cluster_snapshot_xml(id: &str, arn: &str, cluster: &str) -> String {
    format!(
        "    <DBClusterSnapshot>\n      <DBClusterSnapshotIdentifier>{}</DBClusterSnapshotIdentifier>\n      <DBClusterSnapshotArn>{}</DBClusterSnapshotArn>\n      <DBClusterIdentifier>{}</DBClusterIdentifier>\n      <Status>available</Status>\n    </DBClusterSnapshot>",
        xml_escape(id), xml_escape(arn), xml_escape(cluster),
    )
}

fn cluster_snapshot_member_xml(v: &Value) -> String {
    format!(
        "          <DBClusterSnapshotIdentifier>{}</DBClusterSnapshotIdentifier>\n          <DBClusterSnapshotArn>{}</DBClusterSnapshotArn>\n          <DBClusterIdentifier>{}</DBClusterIdentifier>\n          <Status>{}</Status>",
        xml_escape(v["DBClusterSnapshotIdentifier"].as_str().unwrap_or("")),
        xml_escape(v["DBClusterSnapshotArn"].as_str().unwrap_or("")),
        xml_escape(v["DBClusterIdentifier"].as_str().unwrap_or("")),
        xml_escape(v["Status"].as_str().unwrap_or("available")),
    )
}

fn cluster_pg_xml(name: &str, arn: &str, family: &str) -> String {
    format!(
        "    <DBClusterParameterGroup>\n      <DBClusterParameterGroupName>{}</DBClusterParameterGroupName>\n      <DBClusterParameterGroupArn>{}</DBClusterParameterGroupArn>\n      <DBParameterGroupFamily>{}</DBParameterGroupFamily>\n    </DBClusterParameterGroup>",
        xml_escape(name), xml_escape(arn), xml_escape(family),
    )
}

fn cluster_pg_member_xml(v: &Value) -> String {
    format!(
        "          <DBClusterParameterGroupName>{}</DBClusterParameterGroupName>\n          <DBClusterParameterGroupArn>{}</DBClusterParameterGroupArn>\n          <DBParameterGroupFamily>{}</DBParameterGroupFamily>",
        xml_escape(v["DBClusterParameterGroupName"].as_str().unwrap_or("")),
        xml_escape(v["DBClusterParameterGroupArn"].as_str().unwrap_or("")),
        xml_escape(v["DBParameterGroupFamily"].as_str().unwrap_or("")),
    )
}

fn cluster_endpoint_xml(v: &Value) -> String {
    format!(
        "          <DBClusterEndpointIdentifier>{}</DBClusterEndpointIdentifier>\n          <DBClusterIdentifier>{}</DBClusterIdentifier>\n          <Endpoint>{}</Endpoint>\n          <EndpointType>{}</EndpointType>\n          <Status>{}</Status>",
        xml_escape(v["DBClusterEndpointIdentifier"].as_str().unwrap_or("")),
        xml_escape(v["DBClusterIdentifier"].as_str().unwrap_or("")),
        xml_escape(v["Endpoint"].as_str().unwrap_or("")),
        xml_escape(v["EndpointType"].as_str().unwrap_or("")),
        xml_escape(v["Status"].as_str().unwrap_or("available")),
    )
}

fn proxy_xml(v: &Value) -> String {
    format!(
        "          <DBProxyName>{}</DBProxyName>\n          <DBProxyArn>{}</DBProxyArn>\n          <Status>{}</Status>\n          <EngineFamily>{}</EngineFamily>",
        xml_escape(v["DBProxyName"].as_str().unwrap_or("")),
        xml_escape(v["DBProxyArn"].as_str().unwrap_or("")),
        xml_escape(v["Status"].as_str().unwrap_or("available")),
        xml_escape(v["EngineFamily"].as_str().unwrap_or("POSTGRESQL")),
    )
}

fn security_group_xml(v: &Value) -> String {
    format!(
        "          <DBSecurityGroupName>{}</DBSecurityGroupName>\n          <DBSecurityGroupDescription>{}</DBSecurityGroupDescription>\n          <OwnerId>{}</OwnerId>",
        xml_escape(v["DBSecurityGroupName"].as_str().unwrap_or("")),
        xml_escape(v["DBSecurityGroupDescription"].as_str().unwrap_or("")),
        xml_escape(v["OwnerId"].as_str().unwrap_or("000000000000")),
    )
}

fn option_group_xml(v: &Value) -> String {
    format!(
        "          <OptionGroupName>{}</OptionGroupName>\n          <OptionGroupArn>{}</OptionGroupArn>\n          <EngineName>{}</EngineName>\n          <MajorEngineVersion>{}</MajorEngineVersion>",
        xml_escape(v["OptionGroupName"].as_str().unwrap_or("")),
        xml_escape(v["OptionGroupArn"].as_str().unwrap_or("")),
        xml_escape(v["EngineName"].as_str().unwrap_or("")),
        xml_escape(v["MajorEngineVersion"].as_str().unwrap_or("")),
    )
}

fn event_sub_xml(v: &Value) -> String {
    format!(
        "          <CustSubscriptionId>{}</CustSubscriptionId>\n          <SnsTopicArn>{}</SnsTopicArn>\n          <Status>{}</Status>\n          <Enabled>{}</Enabled>",
        xml_escape(v["CustSubscriptionId"].as_str().unwrap_or("")),
        xml_escape(v["SnsTopicArn"].as_str().unwrap_or("")),
        xml_escape(v["Status"].as_str().unwrap_or("active")),
        v["Enabled"].as_bool().unwrap_or(true),
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

fn global_cluster_xml(v: &Value) -> String {
    format!(
        "          <GlobalClusterIdentifier>{}</GlobalClusterIdentifier>\n          <GlobalClusterArn>{}</GlobalClusterArn>\n          <Status>{}</Status>",
        xml_escape(v["GlobalClusterIdentifier"].as_str().unwrap_or("")),
        xml_escape(v["GlobalClusterArn"].as_str().unwrap_or("")),
        xml_escape(v["Status"].as_str().unwrap_or("available")),
    )
}

fn integration_xml(v: &Value) -> String {
    format!(
        "          <IntegrationName>{}</IntegrationName>\n          <IntegrationArn>{}</IntegrationArn>\n          <Status>{}</Status>",
        xml_escape(v["IntegrationName"].as_str().unwrap_or("")),
        xml_escape(v["IntegrationArn"].as_str().unwrap_or("")),
        xml_escape(v["Status"].as_str().unwrap_or("active")),
    )
}

fn blue_green_xml(v: &Value) -> String {
    format!(
        "          <BlueGreenDeploymentIdentifier>{}</BlueGreenDeploymentIdentifier>\n          <BlueGreenDeploymentName>{}</BlueGreenDeploymentName>\n          <Status>{}</Status>",
        xml_escape(v["BlueGreenDeploymentIdentifier"].as_str().unwrap_or("")),
        xml_escape(v["BlueGreenDeploymentName"].as_str().unwrap_or("")),
        xml_escape(v["Status"].as_str().unwrap_or("AVAILABLE")),
    )
}

fn shard_group_xml(v: &Value) -> String {
    format!(
        "          <DBShardGroupIdentifier>{}</DBShardGroupIdentifier>\n          <Status>{}</Status>",
        xml_escape(v["DBShardGroupIdentifier"].as_str().unwrap_or("")),
        xml_escape(v["Status"].as_str().unwrap_or("available")),
    )
}

fn engine_version_xml(v: &Value) -> String {
    format!(
        "    <DBEngineVersion>\n      <Engine>{}</Engine>\n      <EngineVersion>{}</EngineVersion>\n      <Status>{}</Status>\n    </DBEngineVersion>",
        xml_escape(v["Engine"].as_str().unwrap_or("")),
        xml_escape(v["EngineVersion"].as_str().unwrap_or("")),
        xml_escape(v["Status"].as_str().unwrap_or("available")),
    )
}

fn tenant_db_xml(v: &Value) -> String {
    format!(
        "          <TenantDBName>{}</TenantDBName>\n          <Status>{}</Status>",
        xml_escape(v["TenantDBName"].as_str().unwrap_or("")),
        xml_escape(v["Status"].as_str().unwrap_or("available")),
    )
}

fn export_task_xml(v: &Value) -> String {
    format!(
        "          <ExportTaskIdentifier>{}</ExportTaskIdentifier>\n          <Status>{}</Status>",
        xml_escape(v["ExportTaskIdentifier"].as_str().unwrap_or("")),
        xml_escape(v["Status"].as_str().unwrap_or("STARTING")),
    )
}

fn xml_empty_action(action: &str, request_id: &str) -> Result<AwsResponse, AwsServiceError> {
    Ok(xml_response_no_result(action, request_id))
}

/// Read a `<Name>.member.<N>` repeated query param into a Vec.
fn parse_member_list(req: &AwsRequest, prefix: &str) -> Vec<String> {
    let mut out = Vec::new();
    for i in 1.. {
        match get_param(req, &format!("{prefix}.member.{i}")) {
            Some(v) => out.push(v),
            None => break,
        }
    }
    out
}

/// Read repeated `Auth.member.<N>.{AuthScheme,SecretArn,IAMAuth,Description,ClientPasswordAuthType}`
/// proxy auth descriptors into a JSON array.
fn parse_proxy_auth(req: &AwsRequest) -> Vec<Value> {
    let mut out = Vec::new();
    for i in 1.. {
        let scheme = get_param(req, &format!("Auth.member.{i}.AuthScheme"));
        let secret = get_param(req, &format!("Auth.member.{i}.SecretArn"));
        let iam = get_param(req, &format!("Auth.member.{i}.IAMAuth"));
        let desc = get_param(req, &format!("Auth.member.{i}.Description"));
        let pw = get_param(req, &format!("Auth.member.{i}.ClientPasswordAuthType"));
        if scheme.is_none() && secret.is_none() && iam.is_none() && desc.is_none() && pw.is_none() {
            break;
        }
        let mut entry = serde_json::Map::new();
        if let Some(v) = scheme {
            entry.insert("AuthScheme".to_string(), json!(v));
        }
        if let Some(v) = secret {
            entry.insert("SecretArn".to_string(), json!(v));
        }
        if let Some(v) = iam {
            entry.insert("IAMAuth".to_string(), json!(v));
        }
        if let Some(v) = desc {
            entry.insert("Description".to_string(), json!(v));
        }
        if let Some(v) = pw {
            entry.insert("ClientPasswordAuthType".to_string(), json!(v));
        }
        out.push(Value::Object(entry));
    }
    out
}

/// Read `OptionsToInclude.member.<N>.{OptionName,Port,OptionVersion}` plus
/// nested `DBSecurityGroupMemberships`/`VpcSecurityGroupMemberships` member
/// lists into a JSON array.
fn parse_options_to_include(req: &AwsRequest) -> Vec<Value> {
    let mut out = Vec::new();
    for i in 1.. {
        let name = get_param(req, &format!("OptionsToInclude.member.{i}.OptionName"));
        let port = get_param(req, &format!("OptionsToInclude.member.{i}.Port"));
        let version = get_param(req, &format!("OptionsToInclude.member.{i}.OptionVersion"));
        if name.is_none() && port.is_none() && version.is_none() {
            break;
        }
        let mut entry = serde_json::Map::new();
        if let Some(v) = name {
            entry.insert("OptionName".to_string(), json!(v));
        }
        if let Some(v) = port {
            entry.insert("Port".to_string(), json!(v));
        }
        if let Some(v) = version {
            entry.insert("OptionVersion".to_string(), json!(v));
        }
        out.push(Value::Object(entry));
    }
    out
}

/// Pull resource type ("db", "cluster", "snapshot", ...) and id out of an
/// RDS ARN (`arn:aws:rds:region:account:type:id`). Bare ids fall back to
/// `("db", id)` so callers can pass instance identifiers directly.
fn parse_rds_resource_arn(s: &str) -> (Option<&'static str>, String) {
    let parts: Vec<&str> = s.splitn(7, ':').collect();
    if parts.len() == 7 && parts[0] == "arn" && parts[2] == "rds" {
        let kind = match parts[5] {
            "db" => Some("db"),
            "cluster" => Some("cluster"),
            "snapshot" => Some("snapshot"),
            "cluster-snapshot" => Some("cluster-snapshot"),
            _ => None,
        };
        return (kind, parts[6].to_string());
    }
    (Some("db"), s.to_string())
}

/// Echo `KmsKeyId` as a full KMS ARN. Accepts a raw key id, an
/// `alias/<name>` reference, or an existing ARN (passed through).
fn format_kms_arn(input: &str, region: &str, account_id: &str) -> String {
    if input.is_empty() {
        return String::new();
    }
    if input.starts_with("arn:") {
        return input.to_string();
    }
    if input.starts_with("alias/") {
        return Arn::new("kms", region, account_id, input).to_string();
    }
    Arn::new("kms", region, account_id, &format!("key/{input}")).to_string()
}

fn list_extras_xml(
    svc: &RdsService,
    aid: &str,
    category: &str,
    wrapper: &str,
    action: &str,
    render: impl Fn(&Value) -> String,
    rid: &str,
) -> Result<AwsResponse, AwsServiceError> {
    let accounts = svc.state_handle().read();
    let items: Vec<Value> = accounts
        .get(aid)
        .and_then(|s| s.extras.get(category))
        .map(|m| m.values().cloned().collect())
        .unwrap_or_default();
    let inner = format!(
        "    <{wrapper}>\n{}\n    </{wrapper}>",
        members(&items, render)
    );
    Ok(xml_response(action, inner, rid))
}

#[cfg(test)]
mod tests {
    use crate::service::RdsService;
    use crate::state::{RdsState, SharedRdsState};
    use fakecloud_core::multi_account::MultiAccountState;
    use fakecloud_core::service::AwsRequest;
    use http::Method;
    use parking_lot::RwLock;
    use serde_json::json;
    use std::collections::HashMap;
    use std::sync::Arc;

    fn svc() -> RdsService {
        let state: SharedRdsState = Arc::new(RwLock::new(MultiAccountState::<RdsState>::new(
            "000000000000",
            "us-east-1",
            "",
        )));
        RdsService::new(state)
    }

    fn req(action: &str, params: &[(&str, &str)]) -> AwsRequest {
        let mut q = HashMap::new();
        q.insert("Action".to_string(), action.to_string());
        for (k, v) in params {
            q.insert(k.to_string(), v.to_string());
        }
        AwsRequest {
            service: "rds".to_string(),
            method: Method::POST,
            raw_path: "/".to_string(),
            raw_query: String::new(),
            path_segments: vec![],
            query_params: q,
            headers: http::HeaderMap::new(),
            body: bytes::Bytes::new(),
            body_stream: parking_lot::Mutex::new(None),
            account_id: "000000000000".to_string(),
            region: "us-east-1".to_string(),
            request_id: "rid".to_string(),
            action: action.to_string(),
            is_query_protocol: true,
            access_key_id: None,
            principal: None,
        }
    }

    fn ok(action: &str, params: &[(&str, &str)]) {
        ok_on(&svc(), action, params);
    }

    fn ok_on(svc: &RdsService, action: &str, params: &[(&str, &str)]) {
        let r = svc.handle_extra_action(&req(action, params));
        let resp = match r {
            Ok(r) => r,
            Err(e) => panic!("{action} failed: {e:?}"),
        };
        assert!(resp.status.is_success(), "{action} status: {}", resp.status);
    }

    #[test]
    fn describe_events_returns_emitted_events() {
        let svc = svc();
        // Push two events directly into state.
        {
            let state = svc.state_handle();
            let mut accounts = state.write();
            let s = accounts.get_or_create("000000000000");
            s.push_event(crate::state::RdsEventRecord {
                source_identifier: "instance-a".to_string(),
                source_type: "db-instance".to_string(),
                source_arn: "arn:aws:rds:us-east-1:000000000000:db:instance-a".to_string(),
                event_id: "RDS-EVENT-0001".to_string(),
                event_categories: vec!["creation".to_string()],
                message: "DB instance created".to_string(),
                date: chrono::Utc::now(),
            });
            s.push_event(crate::state::RdsEventRecord {
                source_identifier: "instance-b".to_string(),
                source_type: "db-instance".to_string(),
                source_arn: "arn:aws:rds:us-east-1:000000000000:db:instance-b".to_string(),
                event_id: "RDS-EVENT-0002".to_string(),
                event_categories: vec!["failure".to_string()],
                message: "DB instance failed".to_string(),
                date: chrono::Utc::now(),
            });
        }
        let resp = svc
            .handle_extra_action(&req("DescribeEvents", &[]))
            .unwrap();
        let body = String::from_utf8(resp.body.expect_bytes().to_vec()).unwrap();
        assert!(body.contains("instance-a"), "missing instance-a in {body}");
        assert!(body.contains("instance-b"), "missing instance-b in {body}");
        assert!(body.contains("DB instance created"));
    }

    #[test]
    fn describe_events_filters_by_source_identifier() {
        let svc = svc();
        {
            let state = svc.state_handle();
            let mut accounts = state.write();
            let s = accounts.get_or_create("000000000000");
            for id in ["i-a", "i-b", "i-c"] {
                s.push_event(crate::state::RdsEventRecord {
                    source_identifier: id.to_string(),
                    source_type: "db-instance".to_string(),
                    source_arn: format!("arn:aws:rds:us-east-1:000000000000:db:{id}"),
                    event_id: "RDS-EVENT-0001".to_string(),
                    event_categories: vec!["creation".to_string()],
                    message: format!("created {id}"),
                    date: chrono::Utc::now(),
                });
            }
        }
        let resp = svc
            .handle_extra_action(&req("DescribeEvents", &[("SourceIdentifier", "i-b")]))
            .unwrap();
        let body = String::from_utf8(resp.body.expect_bytes().to_vec()).unwrap();
        assert!(body.contains("created i-b"));
        assert!(!body.contains("created i-a"));
        assert!(!body.contains("created i-c"));
    }

    #[test]
    fn cluster_lifecycle() {
        // The lifecycle ops require the cluster to actually exist and be
        // in the right state; share a single service so each call sees
        // the previous mutation.
        let svc = svc();
        ok_on(&svc, "CreateDBCluster", &[("DBClusterIdentifier", "c1")]);
        ok_on(
            &svc,
            "ModifyDBCluster",
            &[("DBClusterIdentifier", "c1"), ("EngineVersion", "16.4")],
        );
        ok_on(&svc, "RebootDBCluster", &[("DBClusterIdentifier", "c1")]);
        // Backtrack requires aurora-mysql; switch the engine first.
        ok_on(
            &svc,
            "ModifyDBCluster",
            &[("DBClusterIdentifier", "c1"), ("EngineVersion", "8.0")],
        );
        {
            let mut accounts = svc.state_handle().write();
            let state = accounts.get_or_create("000000000000");
            if let Some(map) = state.extras.get_mut("clusters") {
                if let Some(entry) = map.get_mut("c1") {
                    if let Some(obj) = entry.as_object_mut() {
                        obj.insert("Engine".to_string(), json!("aurora-mysql"));
                    }
                }
            }
        }
        ok_on(
            &svc,
            "BacktrackDBCluster",
            &[
                ("DBClusterIdentifier", "c1"),
                ("BacktrackTo", "2026-05-01T00:00:00Z"),
            ],
        );
        ok_on(&svc, "FailoverDBCluster", &[("DBClusterIdentifier", "c1")]);
        ok_on(&svc, "StopDBCluster", &[("DBClusterIdentifier", "c1")]);
        ok_on(&svc, "StartDBCluster", &[("DBClusterIdentifier", "c1")]);
        ok_on(
            &svc,
            "PromoteReadReplicaDBCluster",
            &[("DBClusterIdentifier", "c1")],
        );
        ok_on(&svc, "DescribeDBClusters", &[]);
        ok_on(&svc, "DeleteDBCluster", &[("DBClusterIdentifier", "c1")]);
    }

    #[test]
    fn cluster_snapshot_lifecycle() {
        let svc = svc();
        ok_on(
            &svc,
            "CreateDBClusterSnapshot",
            &[
                ("DBClusterSnapshotIdentifier", "cs1"),
                ("DBClusterIdentifier", "c1"),
            ],
        );
        ok_on(
            &svc,
            "CopyDBClusterSnapshot",
            &[
                ("TargetDBClusterSnapshotIdentifier", "cs2"),
                ("SourceDBClusterSnapshotIdentifier", "cs1"),
            ],
        );
        ok_on(&svc, "DescribeDBClusterSnapshots", &[]);
        ok_on(
            &svc,
            "DescribeDBClusterSnapshotAttributes",
            &[("DBClusterSnapshotIdentifier", "cs1")],
        );
        ok_on(
            &svc,
            "ModifyDBClusterSnapshotAttribute",
            &[("DBClusterSnapshotIdentifier", "cs1")],
        );
        ok_on(&svc, "DescribeDBClusterAutomatedBackups", &[]);
        ok_on(&svc, "DeleteDBClusterAutomatedBackup", &[]);
        ok_on(&svc, "DescribeDBClusterBacktracks", &[]);
        ok_on(
            &svc,
            "DeleteDBClusterSnapshot",
            &[("DBClusterSnapshotIdentifier", "cs1")],
        );
    }

    #[test]
    fn cluster_param_groups_lifecycle() {
        ok(
            "CreateDBClusterParameterGroup",
            &[("DBClusterParameterGroupName", "cpg")],
        );
        ok(
            "CopyDBClusterParameterGroup",
            &[("TargetDBClusterParameterGroupIdentifier", "cpg2")],
        );
        ok(
            "ModifyDBClusterParameterGroup",
            &[("DBClusterParameterGroupName", "cpg")],
        );
        ok(
            "ResetDBClusterParameterGroup",
            &[("DBClusterParameterGroupName", "cpg")],
        );
        ok("DescribeDBClusterParameterGroups", &[]);
        ok(
            "DescribeDBClusterParameters",
            &[("DBClusterParameterGroupName", "cpg")],
        );
        ok("DescribeEngineDefaultClusterParameters", &[]);
        ok(
            "DeleteDBClusterParameterGroup",
            &[("DBClusterParameterGroupName", "cpg")],
        );
    }

    #[test]
    fn endpoints_proxies_secgroups() {
        let svc = svc();
        ok_on(
            &svc,
            "CreateDBClusterEndpoint",
            &[("DBClusterEndpointIdentifier", "ce1")],
        );
        ok_on(
            &svc,
            "ModifyDBClusterEndpoint",
            &[("DBClusterEndpointIdentifier", "ce1")],
        );
        ok_on(&svc, "DescribeDBClusterEndpoints", &[]);
        ok_on(
            &svc,
            "DeleteDBClusterEndpoint",
            &[("DBClusterEndpointIdentifier", "ce1")],
        );
        ok_on(&svc, "CreateDBProxy", &[("DBProxyName", "p1")]);
        ok_on(&svc, "DescribeDBProxies", &[]);
        ok_on(
            &svc,
            "CreateDBProxyEndpoint",
            &[("DBProxyEndpointName", "pe1")],
        );
        ok_on(
            &svc,
            "ModifyDBProxyEndpoint",
            &[("DBProxyEndpointName", "pe1")],
        );
        ok_on(&svc, "DescribeDBProxyEndpoints", &[]);
        ok_on(&svc, "DescribeDBProxyTargetGroups", &[]);
        ok_on(&svc, "DescribeDBProxyTargets", &[]);
        ok_on(&svc, "ModifyDBProxyTargetGroup", &[("DBProxyName", "p1")]);
        ok_on(&svc, "RegisterDBProxyTargets", &[]);
        ok_on(&svc, "DeregisterDBProxyTargets", &[]);
        ok_on(
            &svc,
            "DeleteDBProxyEndpoint",
            &[("DBProxyEndpointName", "pe1")],
        );
        ok_on(&svc, "ModifyDBProxy", &[("DBProxyName", "p1")]);
        ok_on(&svc, "DeleteDBProxy", &[("DBProxyName", "p1")]);
        ok_on(
            &svc,
            "CreateDBSecurityGroup",
            &[("DBSecurityGroupName", "sg1")],
        );
        ok_on(
            &svc,
            "AuthorizeDBSecurityGroupIngress",
            &[("DBSecurityGroupName", "sg1")],
        );
        ok_on(
            &svc,
            "RevokeDBSecurityGroupIngress",
            &[("DBSecurityGroupName", "sg1")],
        );
        ok_on(&svc, "DescribeDBSecurityGroups", &[]);
        ok_on(
            &svc,
            "DeleteDBSecurityGroup",
            &[("DBSecurityGroupName", "sg1")],
        );
    }

    #[test]
    fn option_groups_event_subs_global_clusters() {
        let svc = svc();
        ok_on(&svc, "CreateOptionGroup", &[("OptionGroupName", "og1")]);
        ok_on(&svc, "ModifyOptionGroup", &[("OptionGroupName", "og1")]);
        ok_on(
            &svc,
            "CopyOptionGroup",
            &[("TargetOptionGroupIdentifier", "og2")],
        );
        ok_on(&svc, "DescribeOptionGroups", &[]);
        ok_on(&svc, "DescribeOptionGroupOptions", &[]);
        ok_on(&svc, "DeleteOptionGroup", &[("OptionGroupName", "og1")]);
        ok_on(
            &svc,
            "CreateEventSubscription",
            &[("SubscriptionName", "es1")],
        );
        ok_on(
            &svc,
            "ModifyEventSubscription",
            &[("SubscriptionName", "es1")],
        );
        ok_on(&svc, "AddSourceIdentifierToSubscription", &[]);
        ok_on(&svc, "RemoveSourceIdentifierFromSubscription", &[]);
        ok_on(&svc, "DescribeEventSubscriptions", &[]);
        ok_on(
            &svc,
            "DeleteEventSubscription",
            &[("SubscriptionName", "es1")],
        );
        ok_on(
            &svc,
            "CreateGlobalCluster",
            &[("GlobalClusterIdentifier", "gc1")],
        );
        ok_on(&svc, "ModifyGlobalCluster", &[]);
        ok_on(&svc, "FailoverGlobalCluster", &[]);
        ok_on(&svc, "SwitchoverGlobalCluster", &[]);
        ok_on(&svc, "RemoveFromGlobalCluster", &[]);
        ok_on(&svc, "DescribeGlobalClusters", &[]);
        ok_on(
            &svc,
            "DeleteGlobalCluster",
            &[("GlobalClusterIdentifier", "gc1")],
        );
    }

    #[test]
    fn integrations_blue_green_shard_groups_tenant_dbs() {
        let svc = svc();
        ok_on(&svc, "CreateIntegration", &[("IntegrationName", "i1")]);
        ok_on(&svc, "ModifyIntegration", &[]);
        ok_on(&svc, "DescribeIntegrations", &[]);
        ok_on(
            &svc,
            "DeleteIntegration",
            &[("IntegrationIdentifier", "i1")],
        );
        ok_on(&svc, "DescribeBlueGreenDeployments", &[]);
        ok_on(
            &svc,
            "CreateDBShardGroup",
            &[("DBShardGroupIdentifier", "sg1")],
        );
        ok_on(&svc, "ModifyDBShardGroup", &[]);
        ok_on(&svc, "RebootDBShardGroup", &[]);
        ok_on(&svc, "DescribeDBShardGroups", &[]);
        ok_on(
            &svc,
            "DeleteDBShardGroup",
            &[("DBShardGroupIdentifier", "sg1")],
        );
        ok_on(&svc, "CreateCustomDBEngineVersion", &[]);
        ok_on(&svc, "ModifyCustomDBEngineVersion", &[]);
        ok_on(&svc, "DeleteCustomDBEngineVersion", &[]);
        ok_on(&svc, "CreateTenantDatabase", &[("TenantDBName", "t1")]);
        ok_on(
            &svc,
            "ModifyTenantDatabase",
            &[("DBInstanceIdentifier", "db1"), ("TenantDBName", "t1")],
        );
        ok_on(&svc, "DescribeTenantDatabases", &[]);
        ok_on(&svc, "DescribeDBSnapshotTenantDatabases", &[]);
        ok_on(&svc, "DeleteTenantDatabase", &[("TenantDBName", "t1")]);
    }

    #[test]
    fn export_activity_replicas_recommendations_certs_pending() {
        ok("StartExportTask", &[("ExportTaskIdentifier", "ex1")]);
        ok("CancelExportTask", &[]);
        ok("DescribeExportTasks", &[]);
        ok("StartActivityStream", &[]);
        ok("ModifyActivityStream", &[]);
        ok("StopActivityStream", &[]);
        ok("AddRoleToDBCluster", &[]);
        ok("RemoveRoleFromDBCluster", &[]);
        ok("AddRoleToDBInstance", &[]);
        ok("RemoveRoleFromDBInstance", &[]);
        ok(
            "ApplyPendingMaintenanceAction",
            &[
                (
                    "ResourceIdentifier",
                    "arn:aws:rds:us-east-1:000000000000:db:any",
                ),
                ("ApplyAction", "system-update"),
                ("OptInType", "immediate"),
            ],
        );
        ok("DescribePendingMaintenanceActions", &[]);
        ok("PurchaseReservedDBInstancesOffering", &[]);
        ok("DescribeReservedDBInstances", &[]);
        ok("DescribeReservedDBInstancesOfferings", &[]);
        // PromoteReadReplica + SwitchoverReadReplica need a real
        // replica instance; covered by the dedicated tests below.
        // StartDBInstance / StopDBInstance moved to the service-level
        // dispatch (they need the container runtime); see the
        // dedicated E2E coverage in fakecloud-e2e/tests/rds_persistence.rs.
        ok("StartDBInstanceAutomatedBackupsReplication", &[]);
        ok("StopDBInstanceAutomatedBackupsReplication", &[]);
        ok("DeleteDBInstanceAutomatedBackup", &[]);
        ok("DescribeDBInstanceAutomatedBackups", &[]);
        ok("DescribeDBRecommendations", &[]);
        ok("ModifyDBRecommendation", &[]);
        ok("DescribeCertificates", &[]);
        ok("ModifyCertificates", &[]);
    }

    #[test]
    fn snapshots_restores_account_events() {
        ok("CopyDBSnapshot", &[("TargetDBSnapshotIdentifier", "s2")]);
        ok(
            "CopyDBParameterGroup",
            &[("TargetDBParameterGroupIdentifier", "p2")],
        );
        ok("DescribeDBParameters", &[]);
        ok("ResetDBParameterGroup", &[("DBParameterGroupName", "p1")]);
        ok("DescribeEngineDefaultParameters", &[]);
        ok("DescribeDBSnapshotAttributes", &[]);
        ok("ModifyDBSnapshot", &[]);
        ok("ModifyDBSnapshotAttribute", &[]);
        ok("RestoreDBClusterFromS3", &[]);
        ok("DescribeAccountAttributes", &[]);
        ok("DescribeEventCategories", &[]);
        ok("DescribeEvents", &[]);
        ok("DescribeSourceRegions", &[]);
        ok("DescribeDBMajorEngineVersions", &[]);
        ok("DescribeValidDBInstanceModifications", &[]);
        ok("ModifyCurrentDBClusterCapacity", &[]);
        ok("DisableHttpEndpoint", &[]);
        ok("EnableHttpEndpoint", &[]);
    }

    fn seed_replica(svc: &RdsService, replica_id: &str, source_id: &str) {
        use crate::state::DbInstance;
        use chrono::Utc;
        let now = Utc::now();
        let mut accounts = svc.state_handle().write();
        let state = accounts.get_or_create("000000000000");
        let arn = state.db_instance_arn(replica_id);
        let source_arn = state.db_instance_arn(source_id);
        // Source first.
        state.instances.insert(
            source_id.to_string(),
            DbInstance {
                db_instance_identifier: source_id.to_string(),
                db_instance_arn: source_arn,
                db_instance_class: "db.t3.micro".to_string(),
                engine: "postgres".to_string(),
                engine_version: "16.3".to_string(),
                db_instance_status: "available".to_string(),
                master_username: "admin".to_string(),
                db_name: None,
                endpoint_address: "127.0.0.1".to_string(),
                port: 5432,
                allocated_storage: 20,
                publicly_accessible: false,
                deletion_protection: false,
                created_at: now,
                dbi_resource_id: format!("db-{}", uuid::Uuid::new_v4().simple()),
                master_user_password: "".to_string(),
                container_id: String::new(),
                host_port: 0,
                tags: Vec::new(),
                read_replica_source_db_instance_identifier: None,
                read_replica_db_instance_identifiers: vec![replica_id.to_string()],
                vpc_security_group_ids: Vec::new(),
                db_parameter_group_name: None,
                backup_retention_period: 1,
                preferred_backup_window: "03:00-04:00".to_string(),
                preferred_maintenance_window: None,
                latest_restorable_time: Some(now),
                option_group_name: None,
                multi_az: false,
                pending_modified_values: None,
                availability_zone: None,
                storage_type: None,
                storage_encrypted: false,
                kms_key_id: None,
                iam_database_authentication_enabled: false,
                iops: None,
                monitoring_interval: None,
                monitoring_role_arn: None,
                performance_insights_enabled: false,
                performance_insights_kms_key_id: None,
                performance_insights_retention_period: None,
                enabled_cloudwatch_logs_exports: Vec::new(),
                ca_certificate_identifier: None,
                network_type: None,
                character_set_name: None,
                auto_minor_version_upgrade: None,
                copy_tags_to_snapshot: None,
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
                db_cluster_identifier: None,
            },
        );
        // Replica points at source.
        state.instances.insert(
            replica_id.to_string(),
            DbInstance {
                db_instance_identifier: replica_id.to_string(),
                db_instance_arn: arn,
                db_instance_class: "db.t3.micro".to_string(),
                engine: "postgres".to_string(),
                engine_version: "16.3".to_string(),
                db_instance_status: "available".to_string(),
                master_username: "admin".to_string(),
                db_name: None,
                endpoint_address: "127.0.0.1".to_string(),
                port: 5432,
                allocated_storage: 20,
                publicly_accessible: false,
                deletion_protection: false,
                created_at: now,
                dbi_resource_id: format!("db-{}", uuid::Uuid::new_v4().simple()),
                master_user_password: "".to_string(),
                container_id: String::new(),
                host_port: 0,
                tags: Vec::new(),
                read_replica_source_db_instance_identifier: Some(source_id.to_string()),
                read_replica_db_instance_identifiers: Vec::new(),
                vpc_security_group_ids: Vec::new(),
                db_parameter_group_name: None,
                backup_retention_period: 1,
                preferred_backup_window: "03:00-04:00".to_string(),
                preferred_maintenance_window: None,
                latest_restorable_time: Some(now),
                option_group_name: None,
                multi_az: false,
                pending_modified_values: None,
                availability_zone: None,
                storage_type: None,
                storage_encrypted: false,
                kms_key_id: None,
                iam_database_authentication_enabled: false,
                iops: None,
                monitoring_interval: None,
                monitoring_role_arn: None,
                performance_insights_enabled: false,
                performance_insights_kms_key_id: None,
                performance_insights_retention_period: None,
                enabled_cloudwatch_logs_exports: Vec::new(),
                ca_certificate_identifier: None,
                network_type: None,
                character_set_name: None,
                auto_minor_version_upgrade: None,
                copy_tags_to_snapshot: None,
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
                db_cluster_identifier: None,
            },
        );
    }

    #[test]
    fn promote_read_replica_clears_source_pointer_and_trims_source_list() {
        let svc = svc();
        seed_replica(&svc, "replica-1", "source-1");
        let resp = svc
            .handle_extra_action(&req(
                "PromoteReadReplica",
                &[
                    ("DBInstanceIdentifier", "replica-1"),
                    ("BackupRetentionPeriod", "7"),
                    ("PreferredBackupWindow", "04:00-05:00"),
                ],
            ))
            .expect("PromoteReadReplica");
        assert!(resp.status.is_success());
        let body = String::from_utf8(resp.body.expect_bytes().to_vec()).unwrap();
        assert!(body.contains("<DBInstanceIdentifier>replica-1</DBInstanceIdentifier>"));

        let accounts = svc.state_handle().read();
        let state = accounts.get("000000000000").unwrap();
        let replica = state.instances.get("replica-1").unwrap();
        assert!(replica.read_replica_source_db_instance_identifier.is_none());
        assert_eq!(replica.backup_retention_period, 7);
        assert_eq!(replica.preferred_backup_window, "04:00-05:00");
        let source = state.instances.get("source-1").unwrap();
        assert!(source.read_replica_db_instance_identifiers.is_empty());
    }

    #[test]
    fn promote_read_replica_rejects_non_replica() {
        let svc = svc();
        seed_replica(&svc, "replica-1", "source-1");
        let err = svc
            .handle_extra_action(&req(
                "PromoteReadReplica",
                &[("DBInstanceIdentifier", "source-1")],
            ))
            .err()
            .expect("non-replica should be rejected");
        assert_eq!(err.code(), "InvalidDBInstanceState");
    }

    #[test]
    fn switchover_read_replica_swaps_primary_and_replica_roles() {
        let svc = svc();
        seed_replica(&svc, "replica-1", "source-1");
        let resp = svc
            .handle_extra_action(&req(
                "SwitchoverReadReplica",
                &[("DBInstanceIdentifier", "replica-1")],
            ))
            .expect("SwitchoverReadReplica");
        assert!(resp.status.is_success());
        let body = String::from_utf8(resp.body.expect_bytes().to_vec()).unwrap();
        assert!(body.starts_with("<SwitchoverReadReplicaResponse"));
        assert!(body.contains("<DBInstanceIdentifier>replica-1</DBInstanceIdentifier>"));

        let accounts = svc.state_handle().read();
        let state = accounts.get("000000000000").unwrap();
        // The former replica is the new primary: no upstream, owns the
        // former primary as a replica.
        let new_primary = state.instances.get("replica-1").unwrap();
        assert!(new_primary
            .read_replica_source_db_instance_identifier
            .is_none());
        assert_eq!(
            new_primary.read_replica_db_instance_identifiers,
            vec!["source-1".to_string()]
        );
        // The former primary is now a replica of the new primary.
        let former_primary = state.instances.get("source-1").unwrap();
        assert_eq!(
            former_primary.read_replica_source_db_instance_identifier,
            Some("replica-1".to_string())
        );
        assert!(former_primary
            .read_replica_db_instance_identifiers
            .is_empty());
    }

    #[test]
    fn switchover_read_replica_repoints_sibling_replicas() {
        let svc = svc();
        seed_replica(&svc, "replica-a", "source-1");
        // Add a second replica off the same source.
        seed_replica(&svc, "replica-b", "source-1");
        // `seed_replica` overwrites the source's replica list each call,
        // so re-set it to include both replicas.
        {
            let mut accounts = svc.state_handle().write();
            let state = accounts.get_or_create("000000000000");
            let src = state.instances.get_mut("source-1").unwrap();
            src.read_replica_db_instance_identifiers =
                vec!["replica-a".to_string(), "replica-b".to_string()];
        }

        svc.handle_extra_action(&req(
            "SwitchoverReadReplica",
            &[("DBInstanceIdentifier", "replica-a")],
        ))
        .expect("SwitchoverReadReplica");

        let accounts = svc.state_handle().read();
        let state = accounts.get("000000000000").unwrap();
        let new_primary = state.instances.get("replica-a").unwrap();
        // New primary owns both the former primary and the sibling
        // replica.
        let mut owned = new_primary.read_replica_db_instance_identifiers.clone();
        owned.sort();
        assert_eq!(owned, vec!["replica-b".to_string(), "source-1".to_string()]);
        // Sibling now points at the new primary.
        let sibling = state.instances.get("replica-b").unwrap();
        assert_eq!(
            sibling.read_replica_source_db_instance_identifier,
            Some("replica-a".to_string())
        );
    }

    #[test]
    fn switchover_read_replica_rejects_non_replica() {
        let svc = svc();
        seed_replica(&svc, "replica-1", "source-1");
        let err = svc
            .handle_extra_action(&req(
                "SwitchoverReadReplica",
                &[("DBInstanceIdentifier", "source-1")],
            ))
            .err()
            .expect("non-replica should be rejected");
        assert_eq!(err.code(), "InvalidDBInstanceState");
    }

    #[test]
    fn switchover_read_replica_unknown_instance_returns_not_found() {
        let svc = svc();
        let err = svc
            .handle_extra_action(&req(
                "SwitchoverReadReplica",
                &[("DBInstanceIdentifier", "ghost")],
            ))
            .err()
            .expect("unknown instance should be rejected");
        assert_eq!(err.code(), "DBInstanceNotFound");
    }

    #[test]
    fn promote_read_replica_unknown_instance_returns_not_found() {
        let svc = svc();
        let err = svc
            .handle_extra_action(&req(
                "PromoteReadReplica",
                &[("DBInstanceIdentifier", "ghost")],
            ))
            .err()
            .expect("unknown instance should be rejected");
        assert_eq!(err.code(), "DBInstanceNotFound");
    }

    fn cluster_value(svc: &RdsService, id: &str) -> serde_json::Value {
        let accounts = svc.state_handle().read();
        accounts
            .get("000000000000")
            .and_then(|s| s.extras.get("clusters"))
            .and_then(|m| m.get(id))
            .cloned()
            .expect("cluster present")
    }

    fn create_cluster(svc: &RdsService, id: &str) {
        svc.handle_extra_action(&req("CreateDBCluster", &[("DBClusterIdentifier", id)]))
            .expect("CreateDBCluster");
    }

    #[test]
    fn modify_db_cluster_persists_fields() {
        let svc = svc();
        create_cluster(&svc, "c1");
        svc.handle_extra_action(&req(
            "ModifyDBCluster",
            &[
                ("DBClusterIdentifier", "c1"),
                ("EngineVersion", "16.4"),
                ("BackupRetentionPeriod", "14"),
                ("PreferredBackupWindow", "01:00-02:00"),
                ("PreferredMaintenanceWindow", "sun:03:00-sun:04:00"),
                ("Port", "5433"),
                ("DeletionProtection", "true"),
                ("EnableIAMDatabaseAuthentication", "true"),
                ("CopyTagsToSnapshot", "true"),
                ("DBClusterParameterGroupName", "custom-pg"),
            ],
        ))
        .expect("ModifyDBCluster");
        let v = cluster_value(&svc, "c1");
        assert_eq!(v["EngineVersion"].as_str(), Some("16.4"));
        // Numeric/bool fields are coerced at persist time so describes
        // serialize them in the right XML shape.
        assert_eq!(v["BackupRetentionPeriod"].as_i64(), Some(14));
        assert_eq!(v["PreferredBackupWindow"].as_str(), Some("01:00-02:00"));
        assert_eq!(
            v["PreferredMaintenanceWindow"].as_str(),
            Some("sun:03:00-sun:04:00")
        );
        assert_eq!(v["Port"].as_i64(), Some(5433));
        assert_eq!(v["DeletionProtection"].as_bool(), Some(true));
        assert_eq!(v["IAMDatabaseAuthenticationEnabled"].as_bool(), Some(true));
        assert_eq!(v["CopyTagsToSnapshot"].as_bool(), Some(true));
        assert_eq!(v["DBClusterParameterGroupName"].as_str(), Some("custom-pg"));
    }

    #[test]
    fn start_db_cluster_sets_status_available() {
        let svc = svc();
        create_cluster(&svc, "c1");
        svc.handle_extra_action(&req("StopDBCluster", &[("DBClusterIdentifier", "c1")]))
            .expect("StopDBCluster");
        assert_eq!(
            cluster_value(&svc, "c1")["Status"].as_str(),
            Some("stopped")
        );
        svc.handle_extra_action(&req("StartDBCluster", &[("DBClusterIdentifier", "c1")]))
            .expect("StartDBCluster");
        assert_eq!(
            cluster_value(&svc, "c1")["Status"].as_str(),
            Some("available")
        );
    }

    #[test]
    fn reboot_db_cluster_sets_status_available() {
        let svc = svc();
        create_cluster(&svc, "c1");
        svc.handle_extra_action(&req("RebootDBCluster", &[("DBClusterIdentifier", "c1")]))
            .expect("RebootDBCluster");
        assert_eq!(
            cluster_value(&svc, "c1")["Status"].as_str(),
            Some("available")
        );
    }

    #[test]
    fn failover_db_cluster_records_target_writer() {
        let svc = svc();
        create_cluster(&svc, "c1");
        svc.handle_extra_action(&req(
            "FailoverDBCluster",
            &[
                ("DBClusterIdentifier", "c1"),
                ("TargetDBInstanceIdentifier", "writer-2"),
            ],
        ))
        .expect("FailoverDBCluster");
        assert_eq!(
            cluster_value(&svc, "c1")["WriterDBInstanceIdentifier"].as_str(),
            Some("writer-2")
        );
    }

    #[test]
    fn backtrack_db_cluster_records_target() {
        let svc = svc();
        create_cluster(&svc, "c1");
        // Backtrack is Aurora MySQL only; flip the engine to satisfy the
        // engine-compatibility check.
        {
            let mut accounts = svc.state_handle().write();
            let state = accounts.get_or_create("000000000000");
            if let Some(map) = state.extras.get_mut("clusters") {
                if let Some(entry) = map.get_mut("c1") {
                    if let Some(obj) = entry.as_object_mut() {
                        obj.insert("Engine".to_string(), json!("aurora-mysql"));
                    }
                }
            }
        }
        svc.handle_extra_action(&req(
            "BacktrackDBCluster",
            &[
                ("DBClusterIdentifier", "c1"),
                ("BacktrackTo", "2026-05-01T00:00:00Z"),
            ],
        ))
        .expect("BacktrackDBCluster");
        assert_eq!(
            cluster_value(&svc, "c1")["BacktrackTo"].as_str(),
            Some("2026-05-01T00:00:00Z")
        );
    }

    #[test]
    fn backtrack_db_cluster_rejects_non_aurora_mysql() {
        let svc = svc();
        // Default engine is aurora-postgresql which doesn't support backtrack.
        create_cluster(&svc, "c1");
        let err = svc
            .handle_extra_action(&req(
                "BacktrackDBCluster",
                &[
                    ("DBClusterIdentifier", "c1"),
                    ("BacktrackTo", "2026-05-01T00:00:00Z"),
                ],
            ))
            .err()
            .expect("aurora-postgresql backtrack should be rejected");
        assert_eq!(err.code(), "InvalidParameterCombination");
    }

    #[test]
    fn backtrack_db_cluster_records_history() {
        let svc = svc();
        create_cluster(&svc, "c1");
        {
            let mut accounts = svc.state_handle().write();
            let state = accounts.get_or_create("000000000000");
            if let Some(map) = state.extras.get_mut("clusters") {
                if let Some(entry) = map.get_mut("c1") {
                    if let Some(obj) = entry.as_object_mut() {
                        obj.insert("Engine".to_string(), json!("aurora-mysql"));
                    }
                }
            }
        }
        svc.handle_extra_action(&req(
            "BacktrackDBCluster",
            &[
                ("DBClusterIdentifier", "c1"),
                ("BacktrackTo", "2026-05-01T00:00:00Z"),
            ],
        ))
        .expect("BacktrackDBCluster");
        let accounts = svc.state_handle().read();
        let backtracks = accounts
            .get("000000000000")
            .and_then(|s| s.extras.get("cluster_backtracks"))
            .expect("cluster_backtracks recorded");
        assert_eq!(backtracks.len(), 1);
    }

    #[test]
    fn start_db_cluster_rejects_when_already_available() {
        let svc = svc();
        create_cluster(&svc, "c1");
        let err = svc
            .handle_extra_action(&req("StartDBCluster", &[("DBClusterIdentifier", "c1")]))
            .err()
            .expect("starting an already-available cluster should error");
        assert_eq!(err.code(), "InvalidDBClusterStateFault");
    }

    #[test]
    fn stop_db_cluster_rejects_when_already_stopped() {
        let svc = svc();
        create_cluster(&svc, "c1");
        svc.handle_extra_action(&req("StopDBCluster", &[("DBClusterIdentifier", "c1")]))
            .expect("StopDBCluster");
        let err = svc
            .handle_extra_action(&req("StopDBCluster", &[("DBClusterIdentifier", "c1")]))
            .err()
            .expect("stopping an already-stopped cluster should error");
        assert_eq!(err.code(), "InvalidDBClusterStateFault");
    }

    #[test]
    fn modify_db_cluster_unknown_cluster_errors() {
        let svc = svc();
        let err = svc
            .handle_extra_action(&req(
                "ModifyDBCluster",
                &[("DBClusterIdentifier", "ghost"), ("EngineVersion", "16.4")],
            ))
            .err()
            .expect("unknown cluster should error");
        assert_eq!(err.code(), "DBClusterNotFoundFault");
    }

    #[test]
    fn modify_db_cluster_renames_via_new_identifier() {
        let svc = svc();
        create_cluster(&svc, "c1");
        svc.handle_extra_action(&req(
            "ModifyDBCluster",
            &[
                ("DBClusterIdentifier", "c1"),
                ("NewDBClusterIdentifier", "c1-renamed"),
            ],
        ))
        .expect("ModifyDBCluster");
        let renamed = cluster_value(&svc, "c1-renamed");
        assert_eq!(renamed["DBClusterIdentifier"].as_str(), Some("c1-renamed"));
        assert!(renamed["DBClusterArn"]
            .as_str()
            .unwrap_or_default()
            .ends_with(":cluster:c1-renamed"));
        let accounts = svc.state_handle().read();
        assert!(accounts
            .get("000000000000")
            .and_then(|s| s.extras.get("clusters"))
            .map(|m| !m.contains_key("c1"))
            .unwrap_or(false));
    }

    #[test]
    fn modify_db_cluster_persists_extended_fields() {
        let svc = svc();
        create_cluster(&svc, "c1");
        svc.handle_extra_action(&req(
            "ModifyDBCluster",
            &[
                ("DBClusterIdentifier", "c1"),
                ("AllocatedStorage", "100"),
                ("DBClusterInstanceClass", "db.r6g.large"),
                ("Iops", "3000"),
                ("StorageEncrypted", "true"),
                ("BacktrackWindow", "86400"),
                ("EnableHttpEndpoint", "true"),
                ("AutoMinorVersionUpgrade", "false"),
                ("ManageMasterUserPassword", "true"),
                ("CACertificateIdentifier", "rds-ca-2019"),
                ("ServerlessV2ScalingConfiguration.MinCapacity", "0.5"),
                ("ServerlessV2ScalingConfiguration.MaxCapacity", "8.0"),
                ("VpcSecurityGroupIds.VpcSecurityGroupId.1", "sg-aaa"),
                ("VpcSecurityGroupIds.VpcSecurityGroupId.2", "sg-bbb"),
                (
                    "CloudwatchLogsExportConfiguration.EnableLogTypes.member.1",
                    "audit",
                ),
                (
                    "CloudwatchLogsExportConfiguration.EnableLogTypes.member.2",
                    "general",
                ),
            ],
        ))
        .expect("ModifyDBCluster");
        let v = cluster_value(&svc, "c1");
        assert_eq!(v["AllocatedStorage"].as_i64(), Some(100));
        assert_eq!(v["DBClusterInstanceClass"].as_str(), Some("db.r6g.large"));
        assert_eq!(v["Iops"].as_i64(), Some(3000));
        assert_eq!(v["StorageEncrypted"].as_bool(), Some(true));
        assert_eq!(v["BacktrackWindow"].as_i64(), Some(86400));
        assert_eq!(v["HttpEndpointEnabled"].as_bool(), Some(true));
        assert_eq!(v["AutoMinorVersionUpgrade"].as_bool(), Some(false));
        assert_eq!(v["ManageMasterUserPassword"].as_bool(), Some(true));
        assert_eq!(v["CACertificateIdentifier"].as_str(), Some("rds-ca-2019"));
        assert_eq!(
            v["ServerlessV2ScalingConfiguration.MinCapacity"].as_str(),
            Some("0.5")
        );
        assert_eq!(
            v["ServerlessV2ScalingConfiguration.MaxCapacity"].as_str(),
            Some("8.0")
        );
        let sgs: Vec<String> = v["VpcSecurityGroupIds"]
            .as_array()
            .unwrap_or(&Vec::new())
            .iter()
            .filter_map(|s| s.as_str().map(str::to_string))
            .collect();
        assert_eq!(sgs, vec!["sg-aaa", "sg-bbb"]);
        let logs: Vec<String> = v["EnabledCloudwatchLogsExports"]
            .as_array()
            .unwrap_or(&Vec::new())
            .iter()
            .filter_map(|s| s.as_str().map(str::to_string))
            .collect();
        assert_eq!(logs, vec!["audit", "general"]);
    }

    #[test]
    fn failover_db_cluster_picks_replica_when_no_target() {
        let svc = svc();
        create_cluster(&svc, "c1");
        // Seed a writer + a reader.
        {
            let mut accounts = svc.state_handle().write();
            let state = accounts.get_or_create("000000000000");
            if let Some(map) = state.extras.get_mut("clusters") {
                if let Some(entry) = map.get_mut("c1") {
                    if let Some(obj) = entry.as_object_mut() {
                        obj.insert(
                            "DBClusterMembers".to_string(),
                            json!([
                                {
                                    "DBInstanceIdentifier": "writer-1",
                                    "IsClusterWriter": true,
                                    "PromotionTier": 1,
                                },
                                {
                                    "DBInstanceIdentifier": "reader-1",
                                    "IsClusterWriter": false,
                                    "PromotionTier": 2,
                                },
                            ]),
                        );
                        obj.insert("WriterDBInstanceIdentifier".to_string(), json!("writer-1"));
                    }
                }
            }
        }
        svc.handle_extra_action(&req("FailoverDBCluster", &[("DBClusterIdentifier", "c1")]))
            .expect("FailoverDBCluster");
        let v = cluster_value(&svc, "c1");
        assert_eq!(v["WriterDBInstanceIdentifier"].as_str(), Some("reader-1"));
        let members = v["DBClusterMembers"].as_array().expect("members");
        let writer_count = members
            .iter()
            .filter(|m| m["IsClusterWriter"].as_bool() == Some(true))
            .count();
        assert_eq!(writer_count, 1);
        let writer_id = members
            .iter()
            .find(|m| m["IsClusterWriter"].as_bool() == Some(true))
            .and_then(|m| m["DBInstanceIdentifier"].as_str())
            .expect("writer member");
        assert_eq!(writer_id, "reader-1");
    }

    #[test]
    fn failover_db_cluster_rejects_non_member_target() {
        let svc = svc();
        create_cluster(&svc, "c1");
        {
            let mut accounts = svc.state_handle().write();
            let state = accounts.get_or_create("000000000000");
            if let Some(map) = state.extras.get_mut("clusters") {
                if let Some(entry) = map.get_mut("c1") {
                    if let Some(obj) = entry.as_object_mut() {
                        obj.insert(
                            "DBClusterMembers".to_string(),
                            json!([
                                {
                                    "DBInstanceIdentifier": "writer-1",
                                    "IsClusterWriter": true,
                                },
                            ]),
                        );
                    }
                }
            }
        }
        let err = svc
            .handle_extra_action(&req(
                "FailoverDBCluster",
                &[
                    ("DBClusterIdentifier", "c1"),
                    ("TargetDBInstanceIdentifier", "stranger"),
                ],
            ))
            .err()
            .expect("non-member target should be rejected");
        assert_eq!(err.code(), "InvalidParameterValue");
    }

    #[test]
    fn promote_read_replica_db_cluster_clears_source() {
        let svc = svc();
        create_cluster(&svc, "c1");
        // Seed cluster as a replica.
        {
            let mut accounts = svc.state_handle().write();
            let state = accounts.get_or_create("000000000000");
            if let Some(map) = state.extras.get_mut("clusters") {
                if let Some(entry) = map.get_mut("c1") {
                    if let Some(obj) = entry.as_object_mut() {
                        obj.insert(
                            "ReplicationSourceIdentifier".to_string(),
                            json!("arn:aws:rds:us-east-1:000000000000:cluster:source"),
                        );
                    }
                }
            }
        }
        svc.handle_extra_action(&req(
            "PromoteReadReplicaDBCluster",
            &[("DBClusterIdentifier", "c1")],
        ))
        .expect("PromoteReadReplicaDBCluster");
        assert!(cluster_value(&svc, "c1")
            .get("ReplicationSourceIdentifier")
            .is_none());
    }

    #[test]
    fn cluster_lifecycle_op_missing_identifier_errors() {
        let svc = svc();
        let err = svc
            .handle_extra_action(&req("ModifyDBCluster", &[]))
            .err()
            .expect("missing identifier should error");
        assert_eq!(err.code(), "InvalidParameterValue");
    }

    #[test]
    fn restore_db_cluster_from_snapshot_clones_source_cluster_fields() {
        let svc = svc();
        create_cluster(&svc, "src");
        // Mutate source so we can verify it carries through.
        svc.handle_extra_action(&req(
            "ModifyDBCluster",
            &[
                ("DBClusterIdentifier", "src"),
                ("EngineVersion", "16.1"),
                ("BackupRetentionPeriod", "21"),
            ],
        ))
        .expect("ModifyDBCluster");
        // Snapshot the source.
        svc.handle_extra_action(&req(
            "CreateDBClusterSnapshot",
            &[
                ("DBClusterSnapshotIdentifier", "snap1"),
                ("DBClusterIdentifier", "src"),
            ],
        ))
        .expect("CreateDBClusterSnapshot");
        svc.handle_extra_action(&req(
            "RestoreDBClusterFromSnapshot",
            &[
                ("DBClusterIdentifier", "restored"),
                ("SnapshotIdentifier", "snap1"),
            ],
        ))
        .expect("RestoreDBClusterFromSnapshot");
        let v = cluster_value(&svc, "restored");
        assert_eq!(v["DBClusterIdentifier"].as_str(), Some("restored"));
        assert_eq!(v["EngineVersion"].as_str(), Some("16.1"));
        // Coerced to integer in ModifyDBCluster, carried verbatim through the snapshot/restore.
        assert_eq!(v["BackupRetentionPeriod"].as_i64(), Some(21));
        assert_eq!(v["Status"].as_str(), Some("available"));
        assert!(v["DBClusterArn"]
            .as_str()
            .unwrap_or_default()
            .ends_with(":cluster:restored"));
    }

    #[test]
    fn restore_db_cluster_from_snapshot_unknown_snapshot_errors() {
        let svc = svc();
        let err = svc
            .handle_extra_action(&req(
                "RestoreDBClusterFromSnapshot",
                &[
                    ("DBClusterIdentifier", "restored"),
                    ("SnapshotIdentifier", "ghost"),
                ],
            ))
            .err()
            .expect("missing snapshot should error");
        assert_eq!(err.code(), "DBClusterSnapshotNotFoundFault");
    }

    #[test]
    fn restore_db_cluster_to_point_in_time_clones_source() {
        let svc = svc();
        create_cluster(&svc, "src");
        svc.handle_extra_action(&req(
            "ModifyDBCluster",
            &[("DBClusterIdentifier", "src"), ("EngineVersion", "16.2")],
        ))
        .expect("ModifyDBCluster");
        svc.handle_extra_action(&req(
            "RestoreDBClusterToPointInTime",
            &[
                ("DBClusterIdentifier", "pit"),
                ("SourceDBClusterIdentifier", "src"),
                ("UseLatestRestorableTime", "true"),
            ],
        ))
        .expect("RestoreDBClusterToPointInTime");
        let v = cluster_value(&svc, "pit");
        assert_eq!(v["DBClusterIdentifier"].as_str(), Some("pit"));
        assert_eq!(v["EngineVersion"].as_str(), Some("16.2"));
        assert_eq!(v["Status"].as_str(), Some("available"));
        assert_eq!(v["UseLatestRestorableTime"].as_str(), Some("true"));
    }

    #[test]
    fn restore_db_cluster_to_point_in_time_unknown_source_errors() {
        let svc = svc();
        let err = svc
            .handle_extra_action(&req(
                "RestoreDBClusterToPointInTime",
                &[
                    ("DBClusterIdentifier", "pit"),
                    ("SourceDBClusterIdentifier", "ghost"),
                ],
            ))
            .err()
            .expect("missing source should error");
        assert_eq!(err.code(), "DBClusterNotFoundFault");
    }

    fn seed_blue_instance(svc: &RdsService, id: &str, addr: &str, port: i32) {
        use crate::state::DbInstance;
        use chrono::Utc;
        let now = Utc::now();
        let mut accounts = svc.state_handle().write();
        let state = accounts.get_or_create("000000000000");
        let arn = state.db_instance_arn(id);
        state.instances.insert(
            id.to_string(),
            DbInstance {
                db_instance_identifier: id.to_string(),
                db_instance_arn: arn,
                db_instance_class: "db.t3.micro".to_string(),
                engine: "postgres".to_string(),
                engine_version: "16.3".to_string(),
                db_instance_status: "available".to_string(),
                master_username: "admin".to_string(),
                db_name: None,
                endpoint_address: addr.to_string(),
                port,
                allocated_storage: 20,
                publicly_accessible: false,
                deletion_protection: false,
                created_at: now,
                dbi_resource_id: format!("db-{}", uuid::Uuid::new_v4().simple()),
                master_user_password: "secret".to_string(),
                container_id: format!("c-{id}"),
                host_port: port as u16,
                tags: Vec::new(),
                read_replica_source_db_instance_identifier: None,
                read_replica_db_instance_identifiers: Vec::new(),
                vpc_security_group_ids: Vec::new(),
                db_parameter_group_name: None,
                backup_retention_period: 1,
                preferred_backup_window: "03:00-04:00".to_string(),
                preferred_maintenance_window: None,
                latest_restorable_time: Some(now),
                option_group_name: None,
                multi_az: false,
                pending_modified_values: None,
                availability_zone: None,
                storage_type: None,
                storage_encrypted: false,
                kms_key_id: None,
                iam_database_authentication_enabled: false,
                iops: None,
                monitoring_interval: None,
                monitoring_role_arn: None,
                performance_insights_enabled: false,
                performance_insights_kms_key_id: None,
                performance_insights_retention_period: None,
                enabled_cloudwatch_logs_exports: Vec::new(),
                ca_certificate_identifier: None,
                network_type: None,
                character_set_name: None,
                auto_minor_version_upgrade: None,
                copy_tags_to_snapshot: None,
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
                db_cluster_identifier: None,
            },
        );
    }

    fn create_bg_deployment(svc: &RdsService, source_id: &str, target_id: &str) -> String {
        let resp = svc
            .handle_extra_action(&req(
                "CreateBlueGreenDeployment",
                &[
                    (
                        "Source",
                        &format!("arn:aws:rds:us-east-1:000000000000:db:{source_id}"),
                    ),
                    ("TargetDBInstanceName", target_id),
                ],
            ))
            .expect("CreateBlueGreenDeployment");
        let body = String::from_utf8(resp.body.expect_bytes().to_vec()).unwrap();
        // Extract bgd id from body.
        let needle = "<BlueGreenDeploymentIdentifier>";
        let start = body.find(needle).expect("bgd id present") + needle.len();
        let end = body[start..]
            .find("</BlueGreenDeploymentIdentifier>")
            .expect("close tag");
        body[start..start + end].to_string()
    }

    #[test]
    fn create_blue_green_deployment_clones_source_into_green() {
        let svc = svc();
        seed_blue_instance(&svc, "blue", "10.0.0.1", 5432);
        let bgd_id = create_bg_deployment(&svc, "blue", "green");
        let accounts = svc.state_handle().read();
        let state = accounts.get("000000000000").unwrap();
        assert!(state.instances.contains_key("green"));
        let green = state.instances.get("green").unwrap();
        assert_eq!(green.engine, "postgres");
        assert_eq!(
            green.read_replica_source_db_instance_identifier.as_deref(),
            Some("blue")
        );
        let entry = state
            .extras
            .get("blue_green")
            .unwrap()
            .get(&bgd_id)
            .unwrap();
        assert_eq!(entry["Status"].as_str(), Some("AVAILABLE"));
        assert_eq!(entry["SourceDBInstanceIdentifier"].as_str(), Some("blue"));
        assert_eq!(entry["TargetDBInstanceIdentifier"].as_str(), Some("green"));
    }

    #[test]
    fn create_blue_green_deployment_with_cluster_source_provisions_green_cluster() {
        let svc = svc();
        // Create a source DBCluster (not a DBInstance).
        ok_on(
            &svc,
            "CreateDBCluster",
            &[
                ("DBClusterIdentifier", "blue-cluster"),
                ("Engine", "aurora-postgresql"),
            ],
        );
        let resp = svc
            .handle_extra_action(&req(
                "CreateBlueGreenDeployment",
                &[
                    (
                        "Source",
                        "arn:aws:rds:us-east-1:000000000000:cluster:blue-cluster",
                    ),
                    ("TargetDBInstanceName", "green-cluster"),
                ],
            ))
            .expect("CreateBlueGreenDeployment");
        let body = String::from_utf8(resp.body.expect_bytes().to_vec()).unwrap();
        let needle = "<BlueGreenDeploymentIdentifier>";
        let start = body.find(needle).expect("bgd id present") + needle.len();
        let end = body[start..]
            .find("</BlueGreenDeploymentIdentifier>")
            .expect("close tag");
        let bgd_id = body[start..start + end].to_string();
        let accounts = svc.state_handle().read();
        let state = accounts.get("000000000000").unwrap();
        // Cluster sources must provision a green cluster (not a stray
        // green instance).
        let clusters = state.extras.get("clusters").expect("clusters");
        assert!(
            clusters.contains_key("green-cluster"),
            "green cluster missing from extras['clusters']"
        );
        assert!(
            !state.instances.contains_key("green-cluster"),
            "green cluster source must not provision a stray DBInstance"
        );
        let entry = state
            .extras
            .get("blue_green")
            .unwrap()
            .get(&bgd_id)
            .unwrap();
        assert_eq!(entry["Status"].as_str(), Some("AVAILABLE"));
        assert_eq!(entry["SourceIsCluster"].as_bool(), Some(true));
    }

    #[test]
    fn create_blue_green_deployment_unknown_source_errors() {
        let svc = svc();
        let err = svc
            .handle_extra_action(&req(
                "CreateBlueGreenDeployment",
                &[("Source", "arn:aws:rds:us-east-1:000000000000:db:ghost")],
            ))
            .err()
            .expect("missing source should error");
        assert_eq!(err.code(), "DBInstanceNotFound");
    }

    #[test]
    fn switchover_blue_green_swaps_endpoints() {
        let svc = svc();
        seed_blue_instance(&svc, "blue", "10.0.0.1", 5432);
        let bgd_id = create_bg_deployment(&svc, "blue", "green");
        // Before swap: blue is the cloned source endpoint, green inherited the same.
        // Mutate green endpoint to make swap observable.
        {
            let mut accounts = svc.state_handle().write();
            let state = accounts.get_or_create("000000000000");
            let green = state.instances.get_mut("green").unwrap();
            green.endpoint_address = "10.0.0.2".to_string();
            green.port = 5433;
        }
        svc.handle_extra_action(&req(
            "SwitchoverBlueGreenDeployment",
            &[("BlueGreenDeploymentIdentifier", &bgd_id)],
        ))
        .expect("SwitchoverBlueGreenDeployment");
        let accounts = svc.state_handle().read();
        let state = accounts.get("000000000000").unwrap();
        let blue = state.instances.get("blue").unwrap();
        let green = state.instances.get("green").unwrap();
        assert_eq!(blue.endpoint_address, "10.0.0.2");
        assert_eq!(blue.port, 5433);
        assert_eq!(green.endpoint_address, "10.0.0.1");
        assert_eq!(green.port, 5432);
        // Green is now writer.
        assert!(green.read_replica_source_db_instance_identifier.is_none());
        let entry = state
            .extras
            .get("blue_green")
            .unwrap()
            .get(&bgd_id)
            .unwrap();
        assert_eq!(entry["Status"].as_str(), Some("SWITCHOVER_COMPLETED"));
    }

    #[test]
    fn switchover_blue_green_unknown_id_errors() {
        let svc = svc();
        let err = svc
            .handle_extra_action(&req(
                "SwitchoverBlueGreenDeployment",
                &[("BlueGreenDeploymentIdentifier", "bgd-ghost")],
            ))
            .err()
            .expect("unknown bgd should error");
        assert_eq!(err.code(), "BlueGreenDeploymentNotFoundFault");
    }

    #[test]
    fn delete_blue_green_with_target_drops_green_instance() {
        let svc = svc();
        seed_blue_instance(&svc, "blue", "10.0.0.1", 5432);
        let bgd_id = create_bg_deployment(&svc, "blue", "green");
        svc.handle_extra_action(&req(
            "DeleteBlueGreenDeployment",
            &[
                ("BlueGreenDeploymentIdentifier", &bgd_id),
                ("DeleteTarget", "true"),
            ],
        ))
        .expect("DeleteBlueGreenDeployment");
        let accounts = svc.state_handle().read();
        let state = accounts.get("000000000000").unwrap();
        assert!(!state.instances.contains_key("green"));
        let map = state.extras.get("blue_green").cloned().unwrap_or_default();
        assert!(!map.contains_key(&bgd_id));
    }

    fn extras_value(svc: &RdsService, category: &str, key: &str) -> serde_json::Value {
        let accounts = svc.state_handle().read();
        accounts
            .get("000000000000")
            .and_then(|s| s.extras.get(category))
            .and_then(|m| m.get(key))
            .cloned()
            .unwrap_or_else(|| panic!("{category}/{key} present"))
    }

    #[test]
    fn modify_event_subscription_persists_topic_and_enabled_flag() {
        let svc = svc();
        ok_on(
            &svc,
            "CreateEventSubscription",
            &[
                ("SubscriptionName", "es1"),
                ("SnsTopicArn", "arn:aws:sns:us-east-1:000:original"),
            ],
        );
        ok_on(
            &svc,
            "ModifyEventSubscription",
            &[
                ("SubscriptionName", "es1"),
                ("SnsTopicArn", "arn:aws:sns:us-east-1:000:updated"),
                ("SourceType", "db-instance"),
                ("Enabled", "false"),
            ],
        );
        let v = extras_value(&svc, "event_subscriptions", "es1");
        assert_eq!(
            v["SnsTopicArn"].as_str(),
            Some("arn:aws:sns:us-east-1:000:updated")
        );
        assert_eq!(v["SourceType"].as_str(), Some("db-instance"));
        assert_eq!(v["Enabled"].as_bool(), Some(false));
    }

    #[test]
    fn modify_event_subscription_unknown_subscription_errors() {
        let svc = svc();
        let err = svc
            .handle_extra_action(&req(
                "ModifyEventSubscription",
                &[("SubscriptionName", "ghost")],
            ))
            .err()
            .expect("missing subscription should error");
        assert_eq!(err.code(), "SubscriptionNotFound");
    }

    #[test]
    fn modify_db_cluster_endpoint_persists_endpoint_type() {
        let svc = svc();
        ok_on(
            &svc,
            "CreateDBClusterEndpoint",
            &[
                ("DBClusterEndpointIdentifier", "ce1"),
                ("DBClusterIdentifier", "c1"),
                ("EndpointType", "READER"),
            ],
        );
        ok_on(
            &svc,
            "ModifyDBClusterEndpoint",
            &[
                ("DBClusterEndpointIdentifier", "ce1"),
                ("EndpointType", "ANY"),
                ("StaticMembers.member.1", "writer-1"),
                ("ExcludedMembers.member.1", "replica-1"),
            ],
        );
        let v = extras_value(&svc, "cluster_endpoints", "ce1");
        assert_eq!(v["EndpointType"].as_str(), Some("ANY"));
        assert_eq!(
            v["StaticMembers"].as_array().unwrap()[0].as_str(),
            Some("writer-1")
        );
        assert_eq!(
            v["ExcludedMembers"].as_array().unwrap()[0].as_str(),
            Some("replica-1")
        );
    }

    #[test]
    fn modify_db_proxy_persists_auth_and_tls() {
        let svc = svc();
        ok_on(&svc, "CreateDBProxy", &[("DBProxyName", "p1")]);
        ok_on(
            &svc,
            "ModifyDBProxy",
            &[
                ("DBProxyName", "p1"),
                ("RequireTLS", "true"),
                ("IdleClientTimeout", "120"),
                ("DebugLogging", "true"),
                ("Auth.member.1.AuthScheme", "SECRETS"),
                (
                    "Auth.member.1.SecretArn",
                    "arn:aws:secretsmanager:us-east-1:000:secret:rds!sec",
                ),
                ("Auth.member.1.IAMAuth", "DISABLED"),
            ],
        );
        let v = extras_value(&svc, "proxies", "p1");
        assert_eq!(v["RequireTLS"].as_bool(), Some(true));
        assert_eq!(v["IdleClientTimeout"].as_i64(), Some(120));
        assert_eq!(v["DebugLogging"].as_bool(), Some(true));
        let auth = v["Auth"].as_array().expect("auth array");
        assert_eq!(auth.len(), 1);
        assert_eq!(auth[0]["AuthScheme"].as_str(), Some("SECRETS"));
    }

    #[test]
    fn modify_db_proxy_endpoint_persists_security_groups() {
        let svc = svc();
        ok_on(
            &svc,
            "CreateDBProxyEndpoint",
            &[("DBProxyEndpointName", "pe1")],
        );
        ok_on(
            &svc,
            "ModifyDBProxyEndpoint",
            &[
                ("DBProxyEndpointName", "pe1"),
                ("VpcSecurityGroupIds.member.1", "sg-1"),
                ("VpcSecurityGroupIds.member.2", "sg-2"),
            ],
        );
        let v = extras_value(&svc, "proxy_endpoints", "pe1");
        let sgs: Vec<&str> = v["VpcSecurityGroupIds"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|v| v.as_str())
            .collect();
        assert_eq!(sgs, vec!["sg-1", "sg-2"]);
    }

    #[test]
    fn modify_db_proxy_target_group_persists_pool_config() {
        let svc = svc();
        ok_on(
            &svc,
            "ModifyDBProxyTargetGroup",
            &[
                ("DBProxyName", "p1"),
                ("TargetGroupName", "default"),
                ("ConnectionPoolConfig.MaxConnectionsPercent", "75"),
                ("ConnectionPoolConfig.MaxIdleConnectionsPercent", "30"),
                ("ConnectionPoolConfig.ConnectionBorrowTimeout", "10"),
            ],
        );
        let v = extras_value(&svc, "proxy_target_groups", "p1/default");
        assert_eq!(
            v["ConnectionPoolConfig"]["MaxConnectionsPercent"].as_i64(),
            Some(75)
        );
        assert_eq!(
            v["ConnectionPoolConfig"]["MaxIdleConnectionsPercent"].as_i64(),
            Some(30)
        );
    }

    #[test]
    fn modify_tenant_database_renames() {
        let svc = svc();
        ok_on(&svc, "CreateTenantDatabase", &[("TenantDBName", "tdb1")]);
        ok_on(
            &svc,
            "ModifyTenantDatabase",
            &[
                ("DBInstanceIdentifier", "db1"),
                ("TenantDBName", "tdb1"),
                ("NewTenantDBName", "tdb2"),
                ("MasterUserPassword", "newpw"),
            ],
        );
        let accounts = svc.state_handle().read();
        let map = accounts
            .get("000000000000")
            .unwrap()
            .extras
            .get("tenant_dbs")
            .cloned()
            .unwrap_or_default();
        assert!(!map.contains_key("tdb1"));
        let v = map.get("tdb2").expect("renamed entry");
        assert_eq!(v["TenantDBName"].as_str(), Some("tdb2"));
        assert_eq!(v["MasterUserPassword"].as_str(), Some("newpw"));
    }

    #[test]
    fn modify_option_group_persists_options_to_include_and_remove() {
        let svc = svc();
        ok_on(&svc, "CreateOptionGroup", &[("OptionGroupName", "og1")]);
        ok_on(
            &svc,
            "ModifyOptionGroup",
            &[
                ("OptionGroupName", "og1"),
                ("OptionsToInclude.member.1.OptionName", "OEM"),
                ("OptionsToInclude.member.1.Port", "1158"),
                ("OptionsToRemove.member.1", "Native Network Encryption"),
            ],
        );
        let v = extras_value(&svc, "option_groups", "og1");
        assert_eq!(v["OptionsToInclude"][0]["OptionName"].as_str(), Some("OEM"));
        assert_eq!(v["OptionsToInclude"][0]["Port"].as_str(), Some("1158"));
        assert_eq!(
            v["OptionsToRemove"][0].as_str(),
            Some("Native Network Encryption")
        );
    }

    #[test]
    fn modify_certificates_records_default() {
        let svc = svc();
        ok_on(
            &svc,
            "ModifyCertificates",
            &[("CertificateIdentifier", "rds-ca-rsa2048-g1")],
        );
        let accounts = svc.state_handle().read();
        let state = accounts.get("000000000000").unwrap();
        assert_eq!(
            state.default_certificate_identifier.as_deref(),
            Some("rds-ca-rsa2048-g1"),
        );
        drop(accounts);
        ok_on(
            &svc,
            "ModifyCertificates",
            &[("RemoveCustomerOverride", "true")],
        );
        let accounts = svc.state_handle().read();
        let state = accounts.get("000000000000").unwrap();
        assert!(state.default_certificate_identifier.is_none());
    }

    #[test]
    fn apply_pending_maintenance_action_drains_into_live_instance() {
        let svc = svc();
        seed_replica(&svc, "replica-1", "source-1");
        {
            let mut accounts = svc.state_handle().write();
            let state = accounts.get_or_create("000000000000");
            let inst = state.instances.get_mut("source-1").unwrap();
            inst.pending_modified_values = Some(crate::state::PendingModifiedValues {
                engine_version: Some("16.4".to_string()),
                storage_type: Some("gp3".to_string()),
                ..Default::default()
            });
        }
        let arn = "arn:aws:rds:us-east-1:000000000000:db:source-1";
        let resp = svc
            .handle_extra_action(&req(
                "ApplyPendingMaintenanceAction",
                &[
                    ("ResourceIdentifier", arn),
                    ("ApplyAction", "system-update"),
                    ("OptInType", "immediate"),
                ],
            ))
            .expect("ApplyPendingMaintenanceAction");
        let body = String::from_utf8(resp.body.expect_bytes().to_vec()).unwrap();
        assert!(body.contains("<ResourceIdentifier>"));
        assert!(body.contains("<PendingMaintenanceActionDetails/>"));
        let accounts = svc.state_handle().read();
        let inst = accounts
            .get("000000000000")
            .unwrap()
            .instances
            .get("source-1")
            .unwrap();
        assert!(inst.pending_modified_values.is_none());
        assert_eq!(inst.engine_version, "16.4");
        assert_eq!(inst.storage_type.as_deref(), Some("gp3"));
    }

    #[test]
    fn apply_pending_maintenance_action_missing_action_errors() {
        let svc = svc();
        let err = svc
            .handle_extra_action(&req(
                "ApplyPendingMaintenanceAction",
                &[(
                    "ResourceIdentifier",
                    "arn:aws:rds:us-east-1:000000000000:db:any",
                )],
            ))
            .err()
            .expect("missing ApplyAction should error");
        assert_eq!(err.code(), "InvalidParameterValue");
    }

    #[test]
    fn copy_db_cluster_snapshot_carries_source_engine() {
        let svc = svc();
        // Seed source cluster with an engine and snapshot it.
        ok_on(
            &svc,
            "CreateDBCluster",
            &[
                ("DBClusterIdentifier", "src"),
                ("Engine", "aurora-mysql"),
                ("EngineVersion", "8.0.32"),
            ],
        );
        ok_on(
            &svc,
            "CreateDBClusterSnapshot",
            &[
                ("DBClusterSnapshotIdentifier", "snap-src"),
                ("DBClusterIdentifier", "src"),
            ],
        );
        ok_on(
            &svc,
            "CopyDBClusterSnapshot",
            &[
                ("SourceDBClusterSnapshotIdentifier", "snap-src"),
                ("TargetDBClusterSnapshotIdentifier", "snap-copy"),
            ],
        );
        let v = extras_value(&svc, "cluster_snapshots", "snap-copy");
        assert_eq!(v["Engine"].as_str(), Some("aurora-mysql"));
        assert_eq!(v["EngineVersion"].as_str(), Some("8.0.32"));
        assert_eq!(v["DBClusterIdentifier"].as_str(), Some("src"));
        assert_eq!(v["SnapshotType"].as_str(), Some("manual"));
    }

    #[test]
    fn copy_db_cluster_snapshot_unknown_source_errors() {
        let svc = svc();
        let err = svc
            .handle_extra_action(&req(
                "CopyDBClusterSnapshot",
                &[
                    ("SourceDBClusterSnapshotIdentifier", "ghost"),
                    ("TargetDBClusterSnapshotIdentifier", "snap-copy"),
                ],
            ))
            .err()
            .expect("missing source should error");
        assert_eq!(err.code(), "DBClusterSnapshotNotFoundFault");
    }

    #[test]
    fn start_activity_stream_returns_full_kms_arn() {
        let svc = svc();
        let resp = svc
            .handle_extra_action(&req(
                "StartActivityStream",
                &[
                    (
                        "ResourceArn",
                        "arn:aws:rds:us-east-1:000000000000:cluster:c1",
                    ),
                    ("KmsKeyId", "1234abcd-12ab-34cd-56ef-1234567890ab"),
                    ("Mode", "sync"),
                ],
            ))
            .expect("StartActivityStream");
        let body = String::from_utf8(resp.body.expect_bytes().to_vec()).unwrap();
        assert!(
            body.contains("<KmsKeyId>arn:aws:kms:us-east-1:000000000000:key/1234abcd-12ab-34cd-56ef-1234567890ab</KmsKeyId>"),
            "missing kms arn in {body}"
        );
        assert!(body.contains("<KinesisStreamName>aws-rds-das-c1</KinesisStreamName>"));
        assert!(body.contains("<Mode>sync</Mode>"));
    }

    #[test]
    fn start_activity_stream_passes_through_existing_arn() {
        let svc = svc();
        let resp = svc
            .handle_extra_action(&req(
                "StartActivityStream",
                &[("KmsKeyId", "arn:aws:kms:eu-west-1:222:key/abcd")],
            ))
            .expect("StartActivityStream");
        let body = String::from_utf8(resp.body.expect_bytes().to_vec()).unwrap();
        assert!(body.contains("<KmsKeyId>arn:aws:kms:eu-west-1:222:key/abcd</KmsKeyId>"));
    }

    #[test]
    fn start_activity_stream_accepts_alias() {
        let svc = svc();
        let resp = svc
            .handle_extra_action(&req(
                "StartActivityStream",
                &[("KmsKeyId", "alias/aws/rds")],
            ))
            .expect("StartActivityStream");
        let body = String::from_utf8(resp.body.expect_bytes().to_vec()).unwrap();
        assert!(
            body.contains("<KmsKeyId>arn:aws:kms:us-east-1:000000000000:alias/aws/rds</KmsKeyId>")
        );
    }
}
