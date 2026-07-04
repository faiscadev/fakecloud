//! AWS CloudTrail (`cloudtrail`) awsJson1.1 dispatch + operation handlers.
//!
//! The full 60-operation CloudTrail control plane. State is account-partitioned
//! and persisted. Each resource is stored as its already-output-valid JSON
//! object so `Get`/`Describe` echoes exactly what `Create`/`Update` persisted.

use std::sync::Arc;

use async_trait::async_trait;
use http::StatusCode;
use serde_json::{json, Map, Value};
use tokio::sync::Mutex as AsyncMutex;
use uuid::Uuid;

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsService, AwsServiceError};
use fakecloud_persistence::SnapshotStore;

use crate::persistence::save_snapshot;
use crate::state::{CloudTrailData, SharedCloudTrailState};
use crate::validate::validate_input;

#[cfg(test)]
mod tests;

/// Every operation name in the CloudTrail Smithy model (60 operations).
pub const CLOUDTRAIL_ACTIONS: &[&str] = &[
    "AddTags",
    "CancelQuery",
    "CreateChannel",
    "CreateDashboard",
    "CreateEventDataStore",
    "CreateTrail",
    "DeleteChannel",
    "DeleteDashboard",
    "DeleteEventDataStore",
    "DeleteResourcePolicy",
    "DeleteTrail",
    "DeregisterOrganizationDelegatedAdmin",
    "DescribeQuery",
    "DescribeTrails",
    "DisableFederation",
    "EnableFederation",
    "GenerateQuery",
    "GetChannel",
    "GetDashboard",
    "GetEventConfiguration",
    "GetEventDataStore",
    "GetEventSelectors",
    "GetImport",
    "GetInsightSelectors",
    "GetQueryResults",
    "GetResourcePolicy",
    "GetTrail",
    "GetTrailStatus",
    "ListChannels",
    "ListDashboards",
    "ListEventDataStores",
    "ListImportFailures",
    "ListImports",
    "ListInsightsData",
    "ListInsightsMetricData",
    "ListPublicKeys",
    "ListQueries",
    "ListTags",
    "ListTrails",
    "LookupEvents",
    "PutEventConfiguration",
    "PutEventSelectors",
    "PutInsightSelectors",
    "PutResourcePolicy",
    "RegisterOrganizationDelegatedAdmin",
    "RemoveTags",
    "RestoreEventDataStore",
    "SearchSampleQueries",
    "StartDashboardRefresh",
    "StartEventDataStoreIngestion",
    "StartImport",
    "StartLogging",
    "StartQuery",
    "StopEventDataStoreIngestion",
    "StopImport",
    "StopLogging",
    "UpdateChannel",
    "UpdateDashboard",
    "UpdateEventDataStore",
    "UpdateTrail",
];

pub struct CloudTrailService {
    state: SharedCloudTrailState,
    snapshot_store: Option<Arc<dyn SnapshotStore>>,
    snapshot_lock: Arc<AsyncMutex<()>>,
}

impl CloudTrailService {
    pub fn new(state: SharedCloudTrailState) -> Self {
        Self {
            state,
            snapshot_store: None,
            snapshot_lock: Arc::new(AsyncMutex::new(())),
        }
    }

    pub fn with_snapshot_store(mut self, store: Arc<dyn SnapshotStore>) -> Self {
        self.snapshot_store = Some(store);
        self
    }

    async fn save(&self) {
        save_snapshot(
            &self.state,
            self.snapshot_store.clone(),
            &self.snapshot_lock,
        )
        .await;
    }
}

#[async_trait]
impl AwsService for CloudTrailService {
    fn service_name(&self) -> &str {
        "cloudtrail"
    }

    async fn handle(&self, request: AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let mutates = is_mutating(request.action.as_str());
        let result = dispatch(self, &request);
        if mutates && matches!(result.as_ref(), Ok(resp) if resp.status.is_success()) {
            self.save().await;
        }
        result
    }

    fn supported_actions(&self) -> &[&str] {
        CLOUDTRAIL_ACTIONS
    }
}

/// Read-only operations do not mutate state and skip the persistence save.
fn is_mutating(action: &str) -> bool {
    !(action.starts_with("Describe")
        || action.starts_with("List")
        || action.starts_with("Get")
        || action == "LookupEvents"
        || action == "GenerateQuery")
}

fn dispatch(s: &CloudTrailService, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
    let b = parse(req)?;
    validate_input(req.action.as_str(), &b)?;
    let ctx = Ctx {
        account: req.account_id.clone(),
        region: req.region.clone(),
    };
    match req.action.as_str() {
        // trails
        "CreateTrail" => s.create_trail(&ctx, &b),
        "GetTrail" => s.get_trail(&ctx, &b),
        "UpdateTrail" => s.update_trail(&ctx, &b),
        "DeleteTrail" => s.delete_trail(&ctx, &b),
        "DescribeTrails" => s.describe_trails(&ctx, &b),
        "ListTrails" => s.list_trails(&ctx, &b),
        "GetTrailStatus" => s.get_trail_status(&ctx, &b),
        "StartLogging" => s.set_logging(&ctx, &b, true),
        "StopLogging" => s.set_logging(&ctx, &b, false),
        // event selectors
        "GetEventSelectors" => s.get_event_selectors(&ctx, &b),
        "PutEventSelectors" => s.put_event_selectors(&ctx, &b),
        "GetInsightSelectors" => s.get_insight_selectors(&ctx, &b),
        "PutInsightSelectors" => s.put_insight_selectors(&ctx, &b),
        // event data stores
        "CreateEventDataStore" => s.create_event_data_store(&ctx, &b),
        "GetEventDataStore" => s.get_event_data_store(&ctx, &b),
        "UpdateEventDataStore" => s.update_event_data_store(&ctx, &b),
        "DeleteEventDataStore" => s.delete_event_data_store(&ctx, &b),
        "ListEventDataStores" => s.list_event_data_stores(&ctx, &b),
        "RestoreEventDataStore" => s.restore_event_data_store(&ctx, &b),
        "StartEventDataStoreIngestion" => s.set_ingestion(&ctx, &b, true),
        "StopEventDataStoreIngestion" => s.set_ingestion(&ctx, &b, false),
        "EnableFederation" => s.enable_federation(&ctx, &b),
        "DisableFederation" => s.disable_federation(&ctx, &b),
        // channels
        "CreateChannel" => s.create_channel(&ctx, &b),
        "GetChannel" => s.get_channel(&ctx, &b),
        "UpdateChannel" => s.update_channel(&ctx, &b),
        "DeleteChannel" => s.delete_channel(&ctx, &b),
        "ListChannels" => s.list_channels(&ctx, &b),
        // imports
        "StartImport" => s.start_import(&ctx, &b),
        "StopImport" => s.stop_import(&ctx, &b),
        "GetImport" => s.get_import(&ctx, &b),
        "ListImports" => s.list_imports(&ctx, &b),
        "ListImportFailures" => s.list_import_failures(&ctx, &b),
        // queries
        "StartQuery" => s.start_query(&ctx, &b),
        "DescribeQuery" => s.describe_query(&ctx, &b),
        "GetQueryResults" => s.get_query_results(&ctx, &b),
        "CancelQuery" => s.cancel_query(&ctx, &b),
        "ListQueries" => s.list_queries(&ctx, &b),
        "GenerateQuery" => s.generate_query(&ctx, &b),
        // dashboards
        "CreateDashboard" => s.create_dashboard(&ctx, &b),
        "GetDashboard" => s.get_dashboard(&ctx, &b),
        "UpdateDashboard" => s.update_dashboard(&ctx, &b),
        "DeleteDashboard" => s.delete_dashboard(&ctx, &b),
        "ListDashboards" => s.list_dashboards(&ctx, &b),
        "StartDashboardRefresh" => s.start_dashboard_refresh(&ctx, &b),
        // resource policy
        "PutResourcePolicy" => s.put_resource_policy(&ctx, &b),
        "GetResourcePolicy" => s.get_resource_policy(&ctx, &b),
        "DeleteResourcePolicy" => s.delete_resource_policy(&ctx, &b),
        // organization delegated admin
        "RegisterOrganizationDelegatedAdmin" => s.register_delegated_admin(&ctx, &b),
        "DeregisterOrganizationDelegatedAdmin" => s.deregister_delegated_admin(&ctx, &b),
        // event configuration
        "GetEventConfiguration" => s.get_event_configuration(&ctx, &b),
        "PutEventConfiguration" => s.put_event_configuration(&ctx, &b),
        // tagging
        "AddTags" => s.add_tags(&ctx, &b),
        "RemoveTags" => s.remove_tags(&ctx, &b),
        "ListTags" => s.list_tags(&ctx, &b),
        // read-only / empty result ops
        "LookupEvents" => lookup_events(&b),
        "ListPublicKeys" => list_public_keys(),
        "ListInsightsData" => list_insights_data(&b),
        "ListInsightsMetricData" => list_insights_metric_data(&b),
        "SearchSampleQueries" => search_sample_queries(&b),
        _ => Err(AwsServiceError::action_not_implemented(
            s.service_name(),
            &req.action,
        )),
    }
}

// ===================== helpers =====================

/// Per-request account + region context.
struct Ctx {
    account: String,
    region: String,
}

fn ok(v: Value) -> Result<AwsResponse, AwsServiceError> {
    Ok(AwsResponse::json_value(StatusCode::OK, v))
}

fn empty_ok() -> Result<AwsResponse, AwsServiceError> {
    ok(json!({}))
}

fn parse(req: &AwsRequest) -> Result<Value, AwsServiceError> {
    if req.body.is_empty() {
        return Ok(json!({}));
    }
    serde_json::from_slice(&req.body)
        .map_err(|e| invalid_param(&format!("Request body is malformed: {e}")))
}

fn err(code: &str, msg: &str) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::BAD_REQUEST, code, msg)
}

fn invalid_param(msg: &str) -> AwsServiceError {
    err("InvalidParameterException", msg)
}

fn trail_not_found(name: &str) -> AwsServiceError {
    err(
        "TrailNotFoundException",
        &format!("Unknown trail: {name} for the given account."),
    )
}

fn eds_not_found(arn: &str) -> AwsServiceError {
    err(
        "EventDataStoreNotFoundException",
        &format!("The specified event data store {arn} was not found."),
    )
}

fn channel_not_found(arn: &str) -> AwsServiceError {
    err(
        "ChannelNotFoundException",
        &format!("The specified channel {arn} was not found."),
    )
}

fn import_not_found(id: &str) -> AwsServiceError {
    err(
        "ImportNotFoundException",
        &format!("The specified import {id} was not found."),
    )
}

fn query_not_found(id: &str) -> AwsServiceError {
    err(
        "QueryIdNotFoundException",
        &format!("The specified query ID {id} was not found."),
    )
}

fn dashboard_not_found(id: &str) -> AwsServiceError {
    err(
        "ResourceNotFoundException",
        &format!("The specified dashboard {id} was not found."),
    )
}

/// Read a required, non-empty string field.
fn req_str<'a>(b: &'a Value, f: &str) -> Result<&'a str, AwsServiceError> {
    b.get(f)
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| invalid_param(&format!("{f} is required.")))
}

fn opt_str<'a>(b: &'a Value, f: &str) -> Option<&'a str> {
    b.get(f).and_then(Value::as_str)
}

fn now_ts() -> f64 {
    chrono::Utc::now().timestamp() as f64
}

fn new_uuid() -> String {
    Uuid::new_v4().to_string()
}

/// Trail ARNs and bare names are accepted interchangeably by the trail ops;
/// normalise both to the bare trail name used as the state key.
fn trail_key(name: &str) -> &str {
    match name.rsplit_once(":trail/") {
        Some((_, n)) => n,
        None => name,
    }
}

fn trail_arn(ctx: &Ctx, name: &str) -> String {
    format!(
        "arn:aws:cloudtrail:{}:{}:trail/{}",
        ctx.region, ctx.account, name
    )
}

/// Convert a maybe-present `Marker`/`NextToken` window over ordered rows into a
/// page + optional next token. The token is the opaque start index.
fn paginate(rows: Vec<Value>, b: &Value, default_max: usize) -> (Vec<Value>, Option<String>) {
    let start = b
        .get("NextToken")
        .and_then(Value::as_str)
        .and_then(|t| t.parse::<usize>().ok())
        .unwrap_or(0);
    let max = b
        .get("MaxResults")
        .and_then(Value::as_u64)
        .map(|m| m.clamp(1, 10_000) as usize)
        .unwrap_or(default_max);
    let end = start.saturating_add(max).min(rows.len());
    let page = rows.get(start..end).unwrap_or(&[]).to_vec();
    let next = if end < rows.len() {
        Some(end.to_string())
    } else {
        None
    };
    (page, next)
}

fn list_response(key: &str, rows: Vec<Value>, b: &Value) -> Result<AwsResponse, AwsServiceError> {
    let (page, next) = paginate(rows, b, 100);
    let mut out = Map::new();
    out.insert(key.to_string(), Value::Array(page));
    if let Some(t) = next {
        out.insert("NextToken".to_string(), json!(t));
    }
    ok(Value::Object(out))
}

/// Copy a set of fields verbatim from `src` into `dst` when present.
fn copy_fields(src: &Value, dst: &mut Map<String, Value>, fields: &[&str]) {
    for f in fields {
        if let Some(v) = src.get(*f) {
            dst.insert((*f).to_string(), v.clone());
        }
    }
}

/// Store the request's `Tags`/`TagsList` into the account tag map under `key`.
fn store_tags(data: &mut CloudTrailData, key: &str, b: &Value) {
    let tags = b
        .get("TagsList")
        .or_else(|| b.get("Tags"))
        .and_then(Value::as_array);
    if let Some(tags) = tags {
        let entry = data.tags.entry(key.to_string()).or_default();
        for t in tags {
            if let Some(k) = t.get("Key").and_then(Value::as_str) {
                let v = t.get("Value").and_then(Value::as_str).unwrap_or("");
                entry.insert(k.to_string(), v.to_string());
            }
        }
    }
}

// ===================== trails =====================

/// The trail describe-object fields carried by CreateTrail/UpdateTrail inputs.
const TRAIL_INPUT_FIELDS: &[&str] = &[
    "S3BucketName",
    "S3KeyPrefix",
    "IncludeGlobalServiceEvents",
    "IsMultiRegionTrail",
    "CloudWatchLogsLogGroupArn",
    "CloudWatchLogsRoleArn",
    "KmsKeyId",
    "IsOrganizationTrail",
];

impl CloudTrailService {
    fn build_trail(&self, ctx: &Ctx, name: &str, b: &Value, existing: Option<&Value>) -> Value {
        let mut t = Map::new();
        if let Some(prev) = existing {
            if let Some(obj) = prev.as_object() {
                t = obj.clone();
            }
        }
        t.insert("Name".into(), json!(name));
        t.insert("TrailARN".into(), json!(trail_arn(ctx, name)));
        t.insert("HomeRegion".into(), json!(ctx.region));
        copy_fields(b, &mut t, TRAIL_INPUT_FIELDS);
        // Defaults for booleans not previously set.
        t.entry("IncludeGlobalServiceEvents").or_insert(json!(true));
        t.entry("IsMultiRegionTrail").or_insert(json!(false));
        t.entry("IsOrganizationTrail").or_insert(json!(false));
        // SNS topic name -> ARN.
        if let Some(sns) = opt_str(b, "SnsTopicName") {
            t.insert("SnsTopicName".into(), json!(sns));
            t.insert(
                "SnsTopicARN".into(),
                json!(format!(
                    "arn:aws:sns:{}:{}:{}",
                    ctx.region, ctx.account, sns
                )),
            );
        }
        let validation = b
            .get("EnableLogFileValidation")
            .and_then(Value::as_bool)
            .unwrap_or_else(|| {
                existing
                    .and_then(|e| e.get("LogFileValidationEnabled"))
                    .and_then(Value::as_bool)
                    .unwrap_or(false)
            });
        t.insert("LogFileValidationEnabled".into(), json!(validation));
        t.entry("HasCustomEventSelectors").or_insert(json!(false));
        t.entry("HasInsightSelectors").or_insert(json!(false));
        Value::Object(t)
    }

    /// Build the CreateTrail/UpdateTrail output (a subset of the full Trail).
    fn trail_mutation_output(trail: &Value) -> Value {
        let mut out = Map::new();
        copy_fields(
            trail,
            &mut out,
            &[
                "Name",
                "S3BucketName",
                "S3KeyPrefix",
                "SnsTopicName",
                "SnsTopicARN",
                "IncludeGlobalServiceEvents",
                "IsMultiRegionTrail",
                "TrailARN",
                "LogFileValidationEnabled",
                "CloudWatchLogsLogGroupArn",
                "CloudWatchLogsRoleArn",
                "KmsKeyId",
                "IsOrganizationTrail",
            ],
        );
        Value::Object(out)
    }

    fn create_trail(&self, ctx: &Ctx, b: &Value) -> Result<AwsResponse, AwsServiceError> {
        let name = req_str(b, "Name")?.to_string();
        req_str(b, "S3BucketName")?;
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        if data.trails.contains_key(&name) {
            return Err(err(
                "TrailAlreadyExistsException",
                &format!("Trail {name} already exists."),
            ));
        }
        let trail = self.build_trail(ctx, &name, b, None);
        let arn = trail_arn(ctx, &name);
        store_tags(data, &arn, b);
        data.trails.insert(name.clone(), trail.clone());
        data.trail_logging.insert(name, false);
        ok(Self::trail_mutation_output(&trail))
    }

    fn get_trail(&self, ctx: &Ctx, b: &Value) -> Result<AwsResponse, AwsServiceError> {
        let name = trail_key(req_str(b, "Name")?).to_string();
        let guard = self.state.read();
        let data = guard.get(&ctx.account);
        match data.and_then(|d| d.trails.get(&name)) {
            Some(t) => ok(json!({ "Trail": t })),
            None => Err(trail_not_found(&name)),
        }
    }

    fn update_trail(&self, ctx: &Ctx, b: &Value) -> Result<AwsResponse, AwsServiceError> {
        let name = trail_key(req_str(b, "Name")?).to_string();
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        let existing = data
            .trails
            .get(&name)
            .cloned()
            .ok_or_else(|| trail_not_found(&name))?;
        let trail = self.build_trail(ctx, &name, b, Some(&existing));
        data.trails.insert(name, trail.clone());
        ok(Self::trail_mutation_output(&trail))
    }

    fn delete_trail(&self, ctx: &Ctx, b: &Value) -> Result<AwsResponse, AwsServiceError> {
        let name = trail_key(req_str(b, "Name")?).to_string();
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        if data.trails.remove(&name).is_none() {
            return Err(trail_not_found(&name));
        }
        data.trail_logging.remove(&name);
        data.trail_status.remove(&name);
        data.event_selectors.remove(&name);
        data.insight_selectors.remove(&name);
        empty_ok()
    }

    fn describe_trails(&self, ctx: &Ctx, b: &Value) -> Result<AwsResponse, AwsServiceError> {
        let guard = self.state.read();
        let data = guard.get(&ctx.account);
        let wanted: Option<Vec<String>> =
            b.get("trailNameList").and_then(Value::as_array).map(|a| {
                a.iter()
                    .filter_map(|v| v.as_str())
                    .map(|s| trail_key(s).to_string())
                    .collect()
            });
        let mut list = Vec::new();
        if let Some(data) = data {
            match &wanted {
                Some(names) => {
                    for n in names {
                        if let Some(t) = data.trails.get(n) {
                            list.push(t.clone());
                        }
                    }
                }
                None => list.extend(data.trails.values().cloned()),
            }
        }
        ok(json!({ "trailList": list }))
    }

    fn list_trails(&self, ctx: &Ctx, b: &Value) -> Result<AwsResponse, AwsServiceError> {
        let guard = self.state.read();
        let rows: Vec<Value> = guard
            .get(&ctx.account)
            .map(|d| {
                d.trails
                    .values()
                    .map(|t| {
                        json!({
                            "TrailARN": t.get("TrailARN").cloned().unwrap_or(Value::Null),
                            "Name": t.get("Name").cloned().unwrap_or(Value::Null),
                            "HomeRegion": t.get("HomeRegion").cloned().unwrap_or(Value::Null),
                        })
                    })
                    .collect()
            })
            .unwrap_or_default();
        list_response("Trails", rows, b)
    }

    fn get_trail_status(&self, ctx: &Ctx, b: &Value) -> Result<AwsResponse, AwsServiceError> {
        let name = trail_key(req_str(b, "Name")?).to_string();
        let guard = self.state.read();
        let data = guard.get(&ctx.account);
        let Some(data) = data else {
            return Err(trail_not_found(&name));
        };
        if !data.trails.contains_key(&name) {
            return Err(trail_not_found(&name));
        }
        let is_logging = data.trail_logging.get(&name).copied().unwrap_or(false);
        let mut out = Map::new();
        out.insert("IsLogging".into(), json!(is_logging));
        if let Some(status) = data.trail_status.get(&name) {
            if let Some(obj) = status.as_object() {
                for (k, v) in obj {
                    out.insert(k.clone(), v.clone());
                }
            }
        }
        ok(Value::Object(out))
    }

    fn set_logging(
        &self,
        ctx: &Ctx,
        b: &Value,
        logging: bool,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = trail_key(req_str(b, "Name")?).to_string();
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        if !data.trails.contains_key(&name) {
            return Err(trail_not_found(&name));
        }
        data.trail_logging.insert(name.clone(), logging);
        let status = data.trail_status.entry(name).or_insert_with(|| json!({}));
        if let Some(obj) = status.as_object_mut() {
            if logging {
                obj.insert("StartLoggingTime".into(), json!(now_ts()));
                obj.insert("TimeLoggingStarted".into(), json!(now_ts().to_string()));
            } else {
                obj.insert("StopLoggingTime".into(), json!(now_ts()));
                obj.insert("TimeLoggingStopped".into(), json!(now_ts().to_string()));
            }
        }
        empty_ok()
    }
}

// ===================== event selectors =====================

impl CloudTrailService {
    fn get_event_selectors(&self, ctx: &Ctx, b: &Value) -> Result<AwsResponse, AwsServiceError> {
        let name = trail_key(req_str(b, "TrailName")?).to_string();
        let guard = self.state.read();
        let data = guard.get(&ctx.account);
        let Some(data) = data.filter(|d| d.trails.contains_key(&name)) else {
            return Err(trail_not_found(&name));
        };
        let arn = trail_arn(ctx, &name);
        match data.event_selectors.get(&name) {
            Some(sel) => {
                let mut out = Map::new();
                out.insert("TrailARN".into(), json!(arn));
                if let Some(obj) = sel.as_object() {
                    for (k, v) in obj {
                        out.insert(k.clone(), v.clone());
                    }
                }
                ok(Value::Object(out))
            }
            None => ok(json!({
                "TrailARN": arn,
                "EventSelectors": [{
                    "ReadWriteType": "All",
                    "IncludeManagementEvents": true,
                    "DataResources": [],
                    "ExcludeManagementEventSources": [],
                }],
            })),
        }
    }

    fn put_event_selectors(&self, ctx: &Ctx, b: &Value) -> Result<AwsResponse, AwsServiceError> {
        let name = trail_key(req_str(b, "TrailName")?).to_string();
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        if !data.trails.contains_key(&name) {
            return Err(trail_not_found(&name));
        }
        let mut sel = Map::new();
        let mut out = Map::new();
        out.insert("TrailARN".into(), json!(trail_arn(ctx, &name)));
        if let Some(es) = b.get("EventSelectors") {
            sel.insert("EventSelectors".into(), es.clone());
            out.insert("EventSelectors".into(), es.clone());
        }
        if let Some(aes) = b.get("AdvancedEventSelectors") {
            sel.insert("AdvancedEventSelectors".into(), aes.clone());
            out.insert("AdvancedEventSelectors".into(), aes.clone());
        }
        data.event_selectors
            .insert(name.clone(), Value::Object(sel));
        if let Some(t) = data.trails.get_mut(&name) {
            if let Some(obj) = t.as_object_mut() {
                obj.insert("HasCustomEventSelectors".into(), json!(true));
            }
        }
        ok(Value::Object(out))
    }

    fn get_insight_selectors(&self, ctx: &Ctx, b: &Value) -> Result<AwsResponse, AwsServiceError> {
        // Insight selectors can be keyed on a trail or an event data store.
        // `GetInsightSelectors` does not declare `EventDataStoreNotFoundException`;
        // a store without configured insights returns `InsightNotEnabledException`.
        if let Some(eds) = opt_str(b, "EventDataStore").filter(|s| !s.is_empty()) {
            let guard = self.state.read();
            let sel = guard
                .get(&ctx.account)
                .and_then(|d| d.insight_selectors.get(eds))
                .cloned()
                .ok_or_else(|| {
                    err(
                        "InsightNotEnabledException",
                        "Insights are not enabled on the given event data store.",
                    )
                })?;
            let mut out = sel.as_object().cloned().unwrap_or_default();
            out.insert("EventDataStoreArn".into(), json!(eds));
            return ok(Value::Object(out));
        }
        let name = trail_key(req_str(b, "TrailName")?).to_string();
        let guard = self.state.read();
        let data = guard.get(&ctx.account);
        let Some(data) = data.filter(|d| d.trails.contains_key(&name)) else {
            return Err(trail_not_found(&name));
        };
        let sel = data.insight_selectors.get(&name).cloned().ok_or_else(|| {
            err(
                "InsightNotEnabledException",
                "Insights are not enabled on the given trail.",
            )
        })?;
        let mut out = sel.as_object().cloned().unwrap_or_default();
        out.insert("TrailARN".into(), json!(trail_arn(ctx, &name)));
        ok(Value::Object(out))
    }

    fn put_insight_selectors(&self, ctx: &Ctx, b: &Value) -> Result<AwsResponse, AwsServiceError> {
        let insight = b
            .get("InsightSelectors")
            .cloned()
            .ok_or_else(|| invalid_param("InsightSelectors is required."))?;
        // `PutInsightSelectors` does not declare `EventDataStoreNotFoundException`;
        // the destination store is validated lazily, so persist against the
        // supplied ARN directly.
        if let Some(eds) = opt_str(b, "EventDataStore").filter(|s| !s.is_empty()) {
            let mut guard = self.state.write();
            let data = guard.get_or_create(&ctx.account);
            let stored = json!({ "InsightSelectors": insight });
            data.insight_selectors.insert(eds.to_string(), stored);
            let mut out = Map::new();
            out.insert("EventDataStoreArn".into(), json!(eds));
            out.insert("InsightSelectors".into(), b["InsightSelectors"].clone());
            if let Some(dest) = opt_str(b, "InsightsDestination") {
                out.insert("InsightsDestination".into(), json!(dest));
            }
            return ok(Value::Object(out));
        }
        let name = trail_key(
            opt_str(b, "TrailName")
                .filter(|s| !s.is_empty())
                .ok_or_else(|| {
                    err(
                        "InvalidParameterCombinationException",
                        "Either TrailName or EventDataStore must be specified.",
                    )
                })?,
        )
        .to_string();
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        if !data.trails.contains_key(&name) {
            return Err(trail_not_found(&name));
        }
        let stored = json!({ "InsightSelectors": insight });
        data.insight_selectors.insert(name.clone(), stored);
        if let Some(t) = data.trails.get_mut(&name) {
            if let Some(obj) = t.as_object_mut() {
                obj.insert("HasInsightSelectors".into(), json!(true));
            }
        }
        ok(json!({
            "TrailARN": trail_arn(ctx, &name),
            "InsightSelectors": b["InsightSelectors"],
        }))
    }
}

// ===================== event data stores =====================

const EDS_OUTPUT_FIELDS: &[&str] = &[
    "EventDataStoreArn",
    "Name",
    "Status",
    "AdvancedEventSelectors",
    "MultiRegionEnabled",
    "OrganizationEnabled",
    "RetentionPeriod",
    "TerminationProtectionEnabled",
    "CreatedTimestamp",
    "UpdatedTimestamp",
    "KmsKeyId",
    "BillingMode",
    "FederationStatus",
    "FederationRoleArn",
];

impl CloudTrailService {
    fn eds_output(eds: &Value) -> Value {
        let mut out = Map::new();
        copy_fields(eds, &mut out, EDS_OUTPUT_FIELDS);
        Value::Object(out)
    }

    /// The `EventDataStore` summary shape carried by `ListEventDataStores`
    /// (a subset of the full describe shape — no billing/KMS/federation).
    fn eds_summary(eds: &Value) -> Value {
        let mut out = Map::new();
        copy_fields(
            eds,
            &mut out,
            &[
                "EventDataStoreArn",
                "Name",
                "TerminationProtectionEnabled",
                "Status",
                "AdvancedEventSelectors",
                "MultiRegionEnabled",
                "OrganizationEnabled",
                "RetentionPeriod",
                "CreatedTimestamp",
                "UpdatedTimestamp",
            ],
        );
        Value::Object(out)
    }

    fn create_event_data_store(
        &self,
        ctx: &Ctx,
        b: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = req_str(b, "Name")?.to_string();
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        if data
            .event_data_stores
            .values()
            .any(|e| e.get("Name").and_then(Value::as_str) == Some(&name))
        {
            return Err(err(
                "EventDataStoreAlreadyExistsException",
                &format!("An event data store already exists with the name {name}."),
            ));
        }
        let arn = format!(
            "arn:aws:cloudtrail:{}:{}:eventdatastore/{}",
            ctx.region,
            ctx.account,
            new_uuid()
        );
        let mut eds = Map::new();
        eds.insert("EventDataStoreArn".into(), json!(arn));
        eds.insert("Name".into(), json!(name));
        eds.insert("Status".into(), json!("ENABLED"));
        // AWS defaults an event data store with no explicit selectors to a
        // single "Default management events" advanced selector rather than an
        // empty list.
        eds.insert(
            "AdvancedEventSelectors".into(),
            b.get("AdvancedEventSelectors").cloned().unwrap_or_else(|| {
                json!([{
                    "Name": "Default management events",
                    "FieldSelectors": [{
                        "Field": "eventCategory",
                        "Equals": ["Management"]
                    }]
                }])
            }),
        );
        eds.insert(
            "MultiRegionEnabled".into(),
            b.get("MultiRegionEnabled").cloned().unwrap_or(json!(true)),
        );
        eds.insert(
            "OrganizationEnabled".into(),
            b.get("OrganizationEnabled")
                .cloned()
                .unwrap_or(json!(false)),
        );
        eds.insert(
            "RetentionPeriod".into(),
            b.get("RetentionPeriod").cloned().unwrap_or(json!(2555)),
        );
        eds.insert(
            "TerminationProtectionEnabled".into(),
            b.get("TerminationProtectionEnabled")
                .cloned()
                .unwrap_or(json!(true)),
        );
        eds.insert(
            "BillingMode".into(),
            b.get("BillingMode")
                .cloned()
                .unwrap_or(json!("EXTENDABLE_RETENTION_PRICING")),
        );
        eds.insert("CreatedTimestamp".into(), json!(now_ts()));
        eds.insert("UpdatedTimestamp".into(), json!(now_ts()));
        copy_fields(b, &mut eds, &["KmsKeyId"]);
        if let Some(tags) = b.get("TagsList") {
            eds.insert("TagsList".into(), tags.clone());
        }
        let eds = Value::Object(eds);
        store_tags(data, &arn, b);
        data.event_data_stores.insert(arn, eds.clone());
        ok(eds)
    }

    fn get_event_data_store(&self, ctx: &Ctx, b: &Value) -> Result<AwsResponse, AwsServiceError> {
        let arn = req_str(b, "EventDataStore")?.to_string();
        let guard = self.state.read();
        match guard
            .get(&ctx.account)
            .and_then(|d| d.event_data_stores.get(&arn))
        {
            Some(eds) => ok(Self::eds_output(eds)),
            None => Err(eds_not_found(&arn)),
        }
    }

    fn update_event_data_store(
        &self,
        ctx: &Ctx,
        b: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let arn = req_str(b, "EventDataStore")?.to_string();
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        let eds = data
            .event_data_stores
            .get_mut(&arn)
            .ok_or_else(|| eds_not_found(&arn))?;
        if let Some(obj) = eds.as_object_mut() {
            copy_fields(
                b,
                obj,
                &[
                    "Name",
                    "AdvancedEventSelectors",
                    "MultiRegionEnabled",
                    "OrganizationEnabled",
                    "RetentionPeriod",
                    "TerminationProtectionEnabled",
                    "KmsKeyId",
                    "BillingMode",
                ],
            );
            obj.insert("UpdatedTimestamp".into(), json!(now_ts()));
        }
        let eds = eds.clone();
        ok(Self::eds_output(&eds))
    }

    fn delete_event_data_store(
        &self,
        ctx: &Ctx,
        b: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let arn = req_str(b, "EventDataStore")?.to_string();
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        let eds = data
            .event_data_stores
            .get_mut(&arn)
            .ok_or_else(|| eds_not_found(&arn))?;
        if eds
            .get("TerminationProtectionEnabled")
            .and_then(Value::as_bool)
            == Some(true)
        {
            return Err(err(
                "EventDataStoreTerminationProtectedException",
                "The event data store cannot be deleted because termination protection is enabled.",
            ));
        }
        // AWS keeps the store in PENDING_DELETION for a restore window.
        if let Some(obj) = eds.as_object_mut() {
            obj.insert("Status".into(), json!("PENDING_DELETION"));
            obj.insert("UpdatedTimestamp".into(), json!(now_ts()));
        }
        empty_ok()
    }

    fn list_event_data_stores(&self, ctx: &Ctx, b: &Value) -> Result<AwsResponse, AwsServiceError> {
        let guard = self.state.read();
        let rows: Vec<Value> = guard
            .get(&ctx.account)
            .map(|d| {
                d.event_data_stores
                    .values()
                    .map(Self::eds_summary)
                    .collect()
            })
            .unwrap_or_default();
        list_response("EventDataStores", rows, b)
    }

    fn restore_event_data_store(
        &self,
        ctx: &Ctx,
        b: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let arn = req_str(b, "EventDataStore")?.to_string();
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        let eds = data
            .event_data_stores
            .get_mut(&arn)
            .ok_or_else(|| eds_not_found(&arn))?;
        if let Some(obj) = eds.as_object_mut() {
            obj.insert("Status".into(), json!("ENABLED"));
            obj.insert("UpdatedTimestamp".into(), json!(now_ts()));
        }
        let eds = eds.clone();
        ok(Self::eds_output(&eds))
    }

    fn set_ingestion(
        &self,
        ctx: &Ctx,
        b: &Value,
        start: bool,
    ) -> Result<AwsResponse, AwsServiceError> {
        let arn = req_str(b, "EventDataStore")?.to_string();
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        let eds = data
            .event_data_stores
            .get_mut(&arn)
            .ok_or_else(|| eds_not_found(&arn))?;
        if let Some(obj) = eds.as_object_mut() {
            obj.insert(
                "Status".into(),
                json!(if start {
                    "ENABLED"
                } else {
                    "STOPPED_INGESTION"
                }),
            );
            obj.insert("UpdatedTimestamp".into(), json!(now_ts()));
        }
        empty_ok()
    }

    fn enable_federation(&self, ctx: &Ctx, b: &Value) -> Result<AwsResponse, AwsServiceError> {
        let arn = req_str(b, "EventDataStore")?.to_string();
        let role = req_str(b, "FederationRoleArn")?.to_string();
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        let eds = data
            .event_data_stores
            .get_mut(&arn)
            .ok_or_else(|| eds_not_found(&arn))?;
        if let Some(obj) = eds.as_object_mut() {
            obj.insert("FederationStatus".into(), json!("ENABLED"));
            obj.insert("FederationRoleArn".into(), json!(role));
        }
        ok(json!({
            "EventDataStoreArn": arn,
            "FederationStatus": "ENABLED",
            "FederationRoleArn": role,
        }))
    }

    fn disable_federation(&self, ctx: &Ctx, b: &Value) -> Result<AwsResponse, AwsServiceError> {
        let arn = req_str(b, "EventDataStore")?.to_string();
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        let eds = data
            .event_data_stores
            .get_mut(&arn)
            .ok_or_else(|| eds_not_found(&arn))?;
        if let Some(obj) = eds.as_object_mut() {
            obj.insert("FederationStatus".into(), json!("DISABLED"));
        }
        ok(json!({
            "EventDataStoreArn": arn,
            "FederationStatus": "DISABLED",
        }))
    }
}

// ===================== channels =====================

impl CloudTrailService {
    fn create_channel(&self, ctx: &Ctx, b: &Value) -> Result<AwsResponse, AwsServiceError> {
        let name = req_str(b, "Name")?.to_string();
        let source = req_str(b, "Source")?.to_string();
        let destinations = b
            .get("Destinations")
            .cloned()
            .ok_or_else(|| invalid_param("Destinations is required."))?;
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        if data
            .channels
            .values()
            .any(|c| c.get("Name").and_then(Value::as_str) == Some(&name))
        {
            return Err(err(
                "ChannelAlreadyExistsException",
                &format!("A channel already exists with the name {name}."),
            ));
        }
        let arn = format!(
            "arn:aws:cloudtrail:{}:{}:channel/{}",
            ctx.region,
            ctx.account,
            new_uuid()
        );
        let mut chan = Map::new();
        chan.insert("ChannelArn".into(), json!(arn));
        chan.insert("Name".into(), json!(name));
        chan.insert("Source".into(), json!(source));
        chan.insert("Destinations".into(), destinations);
        if let Some(tags) = b.get("Tags") {
            chan.insert("Tags".into(), tags.clone());
        }
        store_tags(data, &arn, b);
        let chan = Value::Object(chan);
        data.channels.insert(arn, chan.clone());
        // CreateChannel output echoes name/source/destinations/tags.
        let mut out = chan.as_object().cloned().unwrap_or_default();
        out.remove("SourceConfig");
        ok(Value::Object(out))
    }

    fn get_channel(&self, ctx: &Ctx, b: &Value) -> Result<AwsResponse, AwsServiceError> {
        let arn = req_str(b, "Channel")?.to_string();
        let guard = self.state.read();
        let chan = guard
            .get(&ctx.account)
            .and_then(|d| d.channels.get(&arn))
            .cloned()
            .ok_or_else(|| channel_not_found(&arn))?;
        let mut out = chan.as_object().cloned().unwrap_or_default();
        out.remove("Tags");
        out.insert(
            "SourceConfig".into(),
            json!({ "ApplyToAllRegions": true, "AdvancedEventSelectors": [] }),
        );
        out.insert("IngestionStatus".into(), json!({}));
        ok(Value::Object(out))
    }

    fn update_channel(&self, ctx: &Ctx, b: &Value) -> Result<AwsResponse, AwsServiceError> {
        let arn = req_str(b, "Channel")?.to_string();
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        let chan = data
            .channels
            .get_mut(&arn)
            .ok_or_else(|| channel_not_found(&arn))?;
        if let Some(obj) = chan.as_object_mut() {
            if let Some(name) = b.get("Name") {
                obj.insert("Name".into(), name.clone());
            }
            if let Some(dests) = b.get("Destinations") {
                obj.insert("Destinations".into(), dests.clone());
            }
        }
        let chan = chan.clone();
        let mut out = chan.as_object().cloned().unwrap_or_default();
        out.remove("Tags");
        ok(Value::Object(out))
    }

    fn delete_channel(&self, ctx: &Ctx, b: &Value) -> Result<AwsResponse, AwsServiceError> {
        let arn = req_str(b, "Channel")?.to_string();
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        if data.channels.remove(&arn).is_none() {
            return Err(channel_not_found(&arn));
        }
        empty_ok()
    }

    fn list_channels(&self, ctx: &Ctx, b: &Value) -> Result<AwsResponse, AwsServiceError> {
        let guard = self.state.read();
        let rows: Vec<Value> = guard
            .get(&ctx.account)
            .map(|d| {
                d.channels
                    .values()
                    .map(|c| {
                        json!({
                            "ChannelArn": c.get("ChannelArn").cloned().unwrap_or(Value::Null),
                            "Name": c.get("Name").cloned().unwrap_or(Value::Null),
                        })
                    })
                    .collect()
            })
            .unwrap_or_default();
        list_response("Channels", rows, b)
    }
}

// ===================== imports =====================

impl CloudTrailService {
    fn start_import(&self, ctx: &Ctx, b: &Value) -> Result<AwsResponse, AwsServiceError> {
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        // A supplied ImportId restarts an existing import; otherwise a new
        // import is created.
        if let Some(id) = opt_str(b, "ImportId").filter(|s| !s.is_empty()) {
            let imp = data
                .imports
                .get_mut(id)
                .ok_or_else(|| import_not_found(id))?;
            if let Some(obj) = imp.as_object_mut() {
                obj.insert("ImportStatus".into(), json!("IN_PROGRESS"));
                obj.insert("UpdatedTimestamp".into(), json!(now_ts()));
            }
            return ok(imp.clone());
        }
        let id = new_uuid();
        let mut imp = Map::new();
        imp.insert("ImportId".into(), json!(id));
        copy_fields(
            b,
            &mut imp,
            &[
                "Destinations",
                "ImportSource",
                "StartEventTime",
                "EndEventTime",
            ],
        );
        imp.insert("ImportStatus".into(), json!("INITIALIZING"));
        imp.insert("CreatedTimestamp".into(), json!(now_ts()));
        imp.insert("UpdatedTimestamp".into(), json!(now_ts()));
        let imp = Value::Object(imp);
        data.imports.insert(id, imp.clone());
        ok(imp)
    }

    fn stop_import(&self, ctx: &Ctx, b: &Value) -> Result<AwsResponse, AwsServiceError> {
        let id = req_str(b, "ImportId")?.to_string();
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        let imp = data
            .imports
            .get_mut(&id)
            .ok_or_else(|| import_not_found(&id))?;
        if let Some(obj) = imp.as_object_mut() {
            obj.insert("ImportStatus".into(), json!("STOPPED"));
            obj.insert("UpdatedTimestamp".into(), json!(now_ts()));
            obj.entry("ImportStatistics").or_insert(json!({
                "FilesCompleted": 0,
                "EventsCompleted": 0,
                "FailedEntries": 0,
            }));
        }
        ok(imp.clone())
    }

    fn get_import(&self, ctx: &Ctx, b: &Value) -> Result<AwsResponse, AwsServiceError> {
        let id = req_str(b, "ImportId")?.to_string();
        let guard = self.state.read();
        let mut imp = guard
            .get(&ctx.account)
            .and_then(|d| d.imports.get(&id))
            .cloned()
            .ok_or_else(|| import_not_found(&id))?;
        if let Some(obj) = imp.as_object_mut() {
            obj.entry("ImportStatistics").or_insert(json!({
                "FilesCompleted": 0,
                "EventsCompleted": 0,
                "FailedEntries": 0,
            }));
        }
        ok(imp)
    }

    fn list_imports(&self, ctx: &Ctx, b: &Value) -> Result<AwsResponse, AwsServiceError> {
        let status_filter = opt_str(b, "ImportStatus").map(str::to_string);
        let guard = self.state.read();
        let rows: Vec<Value> = guard
            .get(&ctx.account)
            .map(|d| {
                d.imports
                    .values()
                    .filter(|imp| {
                        status_filter.as_deref().is_none_or(|want| {
                            imp.get("ImportStatus").and_then(Value::as_str) == Some(want)
                        })
                    })
                    .map(|imp| {
                        json!({
                            "ImportId": imp.get("ImportId").cloned().unwrap_or(Value::Null),
                            "ImportStatus": imp.get("ImportStatus").cloned().unwrap_or(Value::Null),
                            "Destinations": imp.get("Destinations").cloned().unwrap_or(json!([])),
                            "CreatedTimestamp": imp.get("CreatedTimestamp").cloned().unwrap_or(Value::Null),
                            "UpdatedTimestamp": imp.get("UpdatedTimestamp").cloned().unwrap_or(Value::Null),
                        })
                    })
                    .collect()
            })
            .unwrap_or_default();
        list_response("Imports", rows, b)
    }

    fn list_import_failures(&self, _ctx: &Ctx, b: &Value) -> Result<AwsResponse, AwsServiceError> {
        // `ListImportFailures` does not declare `ImportNotFoundException`; an
        // unknown import simply yields an empty failure page.
        req_str(b, "ImportId")?;
        list_response("Failures", Vec::new(), b)
    }
}

// ===================== queries (CloudTrail Lake) =====================

impl CloudTrailService {
    fn start_query(&self, ctx: &Ctx, b: &Value) -> Result<AwsResponse, AwsServiceError> {
        if opt_str(b, "QueryStatement").is_none() && opt_str(b, "QueryAlias").is_none() {
            return Err(invalid_param(
                "Either QueryStatement or QueryAlias must be specified.",
            ));
        }
        let id = new_uuid();
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        let mut q = Map::new();
        q.insert("QueryId".into(), json!(id));
        q.insert(
            "QueryString".into(),
            b.get("QueryStatement").cloned().unwrap_or(json!("")),
        );
        q.insert("QueryStatus".into(), json!("FINISHED"));
        q.insert("CreationTime".into(), json!(now_ts()));
        copy_fields(b, &mut q, &["DeliveryS3Uri", "QueryAlias"]);
        data.queries.insert(id.clone(), Value::Object(q));
        let mut out = Map::new();
        out.insert("QueryId".into(), json!(id));
        if let Some(owner) = opt_str(b, "EventDataStoreOwnerAccountId") {
            out.insert("EventDataStoreOwnerAccountId".into(), json!(owner));
        }
        ok(Value::Object(out))
    }

    fn find_query<'a>(&self, data: &'a CloudTrailData, b: &Value) -> Option<&'a Value> {
        if let Some(id) = opt_str(b, "QueryId") {
            return data.queries.get(id);
        }
        if let Some(alias) = opt_str(b, "QueryAlias") {
            return data
                .queries
                .values()
                .find(|q| q.get("QueryAlias").and_then(Value::as_str) == Some(alias));
        }
        None
    }

    fn describe_query(&self, ctx: &Ctx, b: &Value) -> Result<AwsResponse, AwsServiceError> {
        if opt_str(b, "QueryId").is_none() && opt_str(b, "QueryAlias").is_none() {
            return Err(invalid_param("QueryId or QueryAlias must be specified."));
        }
        let guard = self.state.read();
        let data = guard.get(&ctx.account);
        let q = data
            .and_then(|d| self.find_query(d, b))
            .ok_or_else(|| query_not_found(opt_str(b, "QueryId").unwrap_or("")))?;
        let mut out = Map::new();
        out.insert(
            "QueryId".into(),
            q.get("QueryId").cloned().unwrap_or(Value::Null),
        );
        out.insert(
            "QueryString".into(),
            q.get("QueryString").cloned().unwrap_or(json!("")),
        );
        out.insert(
            "QueryStatus".into(),
            q.get("QueryStatus").cloned().unwrap_or(json!("FINISHED")),
        );
        out.insert(
            "QueryStatistics".into(),
            json!({
                "EventsMatched": 0,
                "EventsScanned": 0,
                "BytesScanned": 0,
                "ExecutionTimeInMillis": 0,
                "CreationTime": q.get("CreationTime").cloned().unwrap_or(Value::Null),
            }),
        );
        out.insert("DeliveryStatus".into(), json!("SUCCESS"));
        ok(Value::Object(out))
    }

    fn get_query_results(&self, ctx: &Ctx, b: &Value) -> Result<AwsResponse, AwsServiceError> {
        let id = req_str(b, "QueryId")?.to_string();
        let guard = self.state.read();
        let status = guard
            .get(&ctx.account)
            .and_then(|d| d.queries.get(&id))
            .and_then(|q| q.get("QueryStatus").and_then(Value::as_str))
            .map(str::to_string)
            .ok_or_else(|| query_not_found(&id))?;
        ok(json!({
            "QueryStatus": status,
            "QueryStatistics": {
                "ResultsCount": 0,
                "TotalResultsCount": 0,
                "BytesScanned": 0,
            },
            "QueryResultRows": [],
        }))
    }

    fn cancel_query(&self, ctx: &Ctx, b: &Value) -> Result<AwsResponse, AwsServiceError> {
        let id = req_str(b, "QueryId")?.to_string();
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        let q = data
            .queries
            .get_mut(&id)
            .ok_or_else(|| query_not_found(&id))?;
        if let Some(obj) = q.as_object_mut() {
            obj.insert("QueryStatus".into(), json!("CANCELLED"));
        }
        let mut out = Map::new();
        out.insert("QueryId".into(), json!(id));
        out.insert("QueryStatus".into(), json!("CANCELLED"));
        if let Some(owner) = opt_str(b, "EventDataStoreOwnerAccountId") {
            out.insert("EventDataStoreOwnerAccountId".into(), json!(owner));
        }
        ok(Value::Object(out))
    }

    fn list_queries(&self, ctx: &Ctx, b: &Value) -> Result<AwsResponse, AwsServiceError> {
        let eds = req_str(b, "EventDataStore")?.to_string();
        let status_filter = opt_str(b, "QueryStatus").map(str::to_string);
        let guard = self.state.read();
        let data = guard.get(&ctx.account);
        if !data
            .map(|d| d.event_data_stores.contains_key(&eds))
            .unwrap_or(false)
        {
            return Err(eds_not_found(&eds));
        }
        let rows: Vec<Value> = data
            .map(|d| {
                d.queries
                    .values()
                    .filter(|q| {
                        status_filter.as_deref().is_none_or(|want| {
                            q.get("QueryStatus").and_then(Value::as_str) == Some(want)
                        })
                    })
                    .map(|q| {
                        json!({
                            "QueryId": q.get("QueryId").cloned().unwrap_or(Value::Null),
                            "QueryStatus": q.get("QueryStatus").cloned().unwrap_or(Value::Null),
                            "CreationTime": q.get("CreationTime").cloned().unwrap_or(Value::Null),
                        })
                    })
                    .collect()
            })
            .unwrap_or_default();
        list_response("Queries", rows, b)
    }

    fn generate_query(&self, ctx: &Ctx, b: &Value) -> Result<AwsResponse, AwsServiceError> {
        let stores = b
            .get("EventDataStores")
            .and_then(Value::as_array)
            .filter(|a| !a.is_empty())
            .ok_or_else(|| invalid_param("EventDataStores is required."))?;
        req_str(b, "Prompt")?;
        let guard = self.state.read();
        let data = guard.get(&ctx.account);
        for s in stores {
            if let Some(arn) = s.as_str() {
                if !data
                    .map(|d| d.event_data_stores.contains_key(arn))
                    .unwrap_or(false)
                {
                    return Err(eds_not_found(arn));
                }
            }
        }
        ok(json!({
            "QueryStatement": "SELECT eventName FROM eds LIMIT 10",
            "QueryAlias": "generated-query",
        }))
    }
}

// ===================== dashboards =====================

impl CloudTrailService {
    fn create_dashboard(&self, ctx: &Ctx, b: &Value) -> Result<AwsResponse, AwsServiceError> {
        let name = req_str(b, "Name")?.to_string();
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        let arn = format!(
            "arn:aws:cloudtrail:{}:{}:dashboard/{}",
            ctx.region, ctx.account, name
        );
        if data.dashboards.contains_key(&arn) {
            return Err(err(
                "ConflictException",
                &format!("A dashboard already exists with the name {name}."),
            ));
        }
        let mut d = Map::new();
        d.insert("DashboardArn".into(), json!(arn));
        d.insert("Name".into(), json!(name));
        d.insert("Type".into(), json!("CUSTOM"));
        d.insert("Status".into(), json!("CREATED"));
        d.insert(
            "Widgets".into(),
            b.get("Widgets").cloned().unwrap_or(json!([])),
        );
        d.insert(
            "TerminationProtectionEnabled".into(),
            b.get("TerminationProtectionEnabled")
                .cloned()
                .unwrap_or(json!(false)),
        );
        if let Some(rs) = b.get("RefreshSchedule") {
            d.insert("RefreshSchedule".into(), rs.clone());
        }
        if let Some(tags) = b.get("TagsList") {
            d.insert("TagsList".into(), tags.clone());
        }
        d.insert("CreatedTimestamp".into(), json!(now_ts()));
        d.insert("UpdatedTimestamp".into(), json!(now_ts()));
        store_tags(data, &arn, b);
        let d = Value::Object(d);
        data.dashboards.insert(arn, d.clone());
        // CreateDashboard output.
        let mut out = d.as_object().cloned().unwrap_or_default();
        out.remove("Status");
        out.remove("CreatedTimestamp");
        out.remove("UpdatedTimestamp");
        ok(Value::Object(out))
    }

    fn get_dashboard(&self, ctx: &Ctx, b: &Value) -> Result<AwsResponse, AwsServiceError> {
        let id = req_str(b, "DashboardId")?.to_string();
        let guard = self.state.read();
        let d = guard
            .get(&ctx.account)
            .and_then(|s| s.dashboards.get(&id))
            .cloned()
            .ok_or_else(|| dashboard_not_found(&id))?;
        let mut out = d.as_object().cloned().unwrap_or_default();
        out.remove("Name");
        out.remove("TagsList");
        ok(Value::Object(out))
    }

    fn update_dashboard(&self, ctx: &Ctx, b: &Value) -> Result<AwsResponse, AwsServiceError> {
        let id = req_str(b, "DashboardId")?.to_string();
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        let d = data
            .dashboards
            .get_mut(&id)
            .ok_or_else(|| dashboard_not_found(&id))?;
        if let Some(obj) = d.as_object_mut() {
            copy_fields(
                b,
                obj,
                &["Widgets", "RefreshSchedule", "TerminationProtectionEnabled"],
            );
            obj.insert("UpdatedTimestamp".into(), json!(now_ts()));
        }
        let d = d.clone();
        let mut out = d.as_object().cloned().unwrap_or_default();
        out.remove("Status");
        out.remove("TagsList");
        ok(Value::Object(out))
    }

    fn delete_dashboard(&self, ctx: &Ctx, b: &Value) -> Result<AwsResponse, AwsServiceError> {
        let id = req_str(b, "DashboardId")?.to_string();
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        if data.dashboards.remove(&id).is_none() {
            return Err(dashboard_not_found(&id));
        }
        empty_ok()
    }

    fn list_dashboards(&self, ctx: &Ctx, b: &Value) -> Result<AwsResponse, AwsServiceError> {
        let type_filter = opt_str(b, "Type").map(str::to_string);
        let name_prefix = opt_str(b, "NamePrefix").map(str::to_string);
        let guard = self.state.read();
        let rows: Vec<Value> = guard
            .get(&ctx.account)
            .map(|d| {
                d.dashboards
                    .values()
                    .filter(|dash| {
                        type_filter.as_deref().is_none_or(|want| {
                            dash.get("Type").and_then(Value::as_str) == Some(want)
                        }) && name_prefix.as_deref().is_none_or(|p| {
                            dash.get("Name")
                                .and_then(Value::as_str)
                                .map(|n| n.starts_with(p))
                                .unwrap_or(false)
                        })
                    })
                    .map(|dash| {
                        json!({
                            "DashboardArn": dash.get("DashboardArn").cloned().unwrap_or(Value::Null),
                            "Type": dash.get("Type").cloned().unwrap_or(Value::Null),
                        })
                    })
                    .collect()
            })
            .unwrap_or_default();
        list_response("Dashboards", rows, b)
    }

    fn start_dashboard_refresh(
        &self,
        ctx: &Ctx,
        b: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let id = req_str(b, "DashboardId")?.to_string();
        let guard = self.state.read();
        if !guard
            .get(&ctx.account)
            .map(|d| d.dashboards.contains_key(&id))
            .unwrap_or(false)
        {
            return Err(dashboard_not_found(&id));
        }
        ok(json!({ "RefreshId": new_uuid() }))
    }
}

// ===================== resource policy =====================

impl CloudTrailService {
    fn put_resource_policy(&self, ctx: &Ctx, b: &Value) -> Result<AwsResponse, AwsServiceError> {
        let arn = req_str(b, "ResourceArn")?.to_string();
        let policy = req_str(b, "ResourcePolicy")?.to_string();
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        data.resource_policies.insert(arn.clone(), policy.clone());
        ok(json!({ "ResourceArn": arn, "ResourcePolicy": policy }))
    }

    fn get_resource_policy(&self, ctx: &Ctx, b: &Value) -> Result<AwsResponse, AwsServiceError> {
        let arn = req_str(b, "ResourceArn")?.to_string();
        let guard = self.state.read();
        let policy = guard
            .get(&ctx.account)
            .and_then(|d| d.resource_policies.get(&arn))
            .cloned()
            .ok_or_else(|| {
                err(
                    "ResourcePolicyNotFoundException",
                    &format!("No resource policy was found for {arn}."),
                )
            })?;
        ok(json!({ "ResourceArn": arn, "ResourcePolicy": policy }))
    }

    fn delete_resource_policy(&self, ctx: &Ctx, b: &Value) -> Result<AwsResponse, AwsServiceError> {
        let arn = req_str(b, "ResourceArn")?.to_string();
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        if data.resource_policies.remove(&arn).is_none() {
            return Err(err(
                "ResourcePolicyNotFoundException",
                &format!("No resource policy was found for {arn}."),
            ));
        }
        empty_ok()
    }
}

// ===================== org delegated admin =====================

impl CloudTrailService {
    fn register_delegated_admin(
        &self,
        ctx: &Ctx,
        b: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let account = req_str(b, "MemberAccountId")?.to_string();
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        if data.delegated_admins.contains_key(&account) {
            return Err(err(
                "AccountRegisteredException",
                "The account is already registered as the delegated administrator.",
            ));
        }
        data.delegated_admins
            .insert(account, json!({ "RegisteredTimestamp": now_ts() }));
        empty_ok()
    }

    fn deregister_delegated_admin(
        &self,
        ctx: &Ctx,
        b: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let account = req_str(b, "DelegatedAdminAccountId")?.to_string();
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        if data.delegated_admins.remove(&account).is_none() {
            return Err(err(
                "AccountNotRegisteredException",
                "The account is not registered as the delegated administrator.",
            ));
        }
        empty_ok()
    }
}

// ===================== event configuration =====================

impl CloudTrailService {
    fn get_event_configuration(
        &self,
        ctx: &Ctx,
        b: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let (key, is_trail) = self.event_config_target(ctx, b)?;
        let guard = self.state.read();
        let stored = guard
            .get(&ctx.account)
            .and_then(|d| d.event_configurations.get(&key))
            .cloned();
        Ok(AwsResponse::json_value(
            StatusCode::OK,
            self.event_config_response(&key, is_trail, stored.as_ref(), None),
        ))
    }

    fn put_event_configuration(
        &self,
        ctx: &Ctx,
        b: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let (key, is_trail) = self.event_config_target(ctx, b)?;
        let mut config = Map::new();
        copy_fields(
            b,
            &mut config,
            &[
                "MaxEventSize",
                "ContextKeySelectors",
                "AggregationConfigurations",
            ],
        );
        let config = Value::Object(config);
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        data.event_configurations
            .insert(key.clone(), config.clone());
        Ok(AwsResponse::json_value(
            StatusCode::OK,
            self.event_config_response(&key, is_trail, Some(&config), Some(b)),
        ))
    }

    /// Resolve the trail ARN or event-data-store ARN targeted by an event
    /// configuration op, returning `(key, is_trail)`.
    fn event_config_target(&self, ctx: &Ctx, b: &Value) -> Result<(String, bool), AwsServiceError> {
        if let Some(trail) = opt_str(b, "TrailName").filter(|s| !s.is_empty()) {
            let name = trail_key(trail).to_string();
            let guard = self.state.read();
            if !guard
                .get(&ctx.account)
                .map(|d| d.trails.contains_key(&name))
                .unwrap_or(false)
            {
                return Err(trail_not_found(&name));
            }
            return Ok((trail_arn(ctx, &name), true));
        }
        if let Some(eds) = opt_str(b, "EventDataStore").filter(|s| !s.is_empty()) {
            let guard = self.state.read();
            if !guard
                .get(&ctx.account)
                .map(|d| d.event_data_stores.contains_key(eds))
                .unwrap_or(false)
            {
                return Err(eds_not_found(eds));
            }
            return Ok((eds.to_string(), false));
        }
        Err(err(
            "InvalidParameterCombinationException",
            "Either TrailName or EventDataStore must be specified.",
        ))
    }

    fn event_config_response(
        &self,
        key: &str,
        is_trail: bool,
        stored: Option<&Value>,
        input: Option<&Value>,
    ) -> Value {
        let mut out = Map::new();
        if is_trail {
            out.insert("TrailARN".into(), json!(key));
        } else {
            out.insert("EventDataStoreArn".into(), json!(key));
        }
        let source = input.or(stored);
        out.insert(
            "MaxEventSize".into(),
            source
                .and_then(|s| s.get("MaxEventSize"))
                .cloned()
                .unwrap_or(json!("Standard")),
        );
        out.insert(
            "ContextKeySelectors".into(),
            source
                .and_then(|s| s.get("ContextKeySelectors"))
                .cloned()
                .unwrap_or(json!([])),
        );
        out.insert(
            "AggregationConfigurations".into(),
            source
                .and_then(|s| s.get("AggregationConfigurations"))
                .cloned()
                .unwrap_or(json!([])),
        );
        Value::Object(out)
    }
}

// ===================== tagging =====================

impl CloudTrailService {
    fn add_tags(&self, ctx: &Ctx, b: &Value) -> Result<AwsResponse, AwsServiceError> {
        let id = req_str(b, "ResourceId")?.to_string();
        b.get("TagsList")
            .and_then(Value::as_array)
            .ok_or_else(|| invalid_param("TagsList is required."))?;
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        store_tags(data, &id, b);
        empty_ok()
    }

    fn remove_tags(&self, ctx: &Ctx, b: &Value) -> Result<AwsResponse, AwsServiceError> {
        let id = req_str(b, "ResourceId")?.to_string();
        let tags = b
            .get("TagsList")
            .and_then(Value::as_array)
            .ok_or_else(|| invalid_param("TagsList is required."))?
            .clone();
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        if let Some(entry) = data.tags.get_mut(&id) {
            for t in &tags {
                if let Some(k) = t.get("Key").and_then(Value::as_str) {
                    entry.remove(k);
                }
            }
        }
        empty_ok()
    }

    fn list_tags(&self, ctx: &Ctx, b: &Value) -> Result<AwsResponse, AwsServiceError> {
        let ids = b
            .get("ResourceIdList")
            .and_then(Value::as_array)
            .ok_or_else(|| invalid_param("ResourceIdList is required."))?;
        let guard = self.state.read();
        let data = guard.get(&ctx.account);
        let mut list = Vec::new();
        for id in ids {
            let Some(id) = id.as_str() else { continue };
            let tags: Vec<Value> = data
                .and_then(|d| d.tags.get(id))
                .map(|m| {
                    m.iter()
                        .map(|(k, v)| json!({ "Key": k, "Value": v }))
                        .collect()
                })
                .unwrap_or_default();
            list.push(json!({ "ResourceId": id, "TagsList": tags }));
        }
        ok(json!({ "ResourceTagList": list }))
    }
}

// ===================== read-only / empty result ops =====================

fn lookup_events(b: &Value) -> Result<AwsResponse, AwsServiceError> {
    // A fake needn't record its own API activity; return an empty, real page.
    if let Some(cat) = opt_str(b, "EventCategory") {
        if cat != "insight" {
            return Err(err(
                "InvalidEventCategoryException",
                &format!("{cat} is not a valid event category."),
            ));
        }
    }
    ok(json!({ "Events": [] }))
}

fn list_public_keys() -> Result<AwsResponse, AwsServiceError> {
    ok(json!({ "PublicKeyList": [] }))
}

fn list_insights_data(b: &Value) -> Result<AwsResponse, AwsServiceError> {
    req_str(b, "InsightSource")?;
    req_str(b, "DataType")?;
    ok(json!({ "Events": [] }))
}

fn list_insights_metric_data(b: &Value) -> Result<AwsResponse, AwsServiceError> {
    let event_source = req_str(b, "EventSource")?.to_string();
    let event_name = req_str(b, "EventName")?.to_string();
    let insight_type = req_str(b, "InsightType")?.to_string();
    let mut out = Map::new();
    out.insert("EventSource".into(), json!(event_source));
    out.insert("EventName".into(), json!(event_name));
    out.insert("InsightType".into(), json!(insight_type));
    if let Some(ec) = opt_str(b, "ErrorCode") {
        out.insert("ErrorCode".into(), json!(ec));
    }
    out.insert("Timestamps".into(), json!([]));
    out.insert("Values".into(), json!([]));
    ok(Value::Object(out))
}

fn search_sample_queries(b: &Value) -> Result<AwsResponse, AwsServiceError> {
    req_str(b, "SearchPhrase")?;
    ok(json!({ "SearchResults": [] }))
}
