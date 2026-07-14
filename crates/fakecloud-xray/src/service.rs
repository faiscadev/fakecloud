//! AWS X-Ray (`xray`) restJson1 dispatch + operation handlers.
//!
//! Every X-Ray operation is a fixed `POST /<UriPath>`, so requests route on the
//! `@http` URI path. State is account-partitioned and persisted. The data-plane
//! reads (`BatchGetTraces`, `GetTraceSummaries`, `GetServiceGraph`,
//! `GetTraceGraph`, `GetTimeSeriesServiceStatistics`) compute deterministically
//! over the segments ingested by `PutTraceSegments`; the control-plane ops
//! (groups, sampling rules, encryption config, resource policies, indexing rule,
//! trace-segment destination, tags) are persisted CRUD.

use std::collections::BTreeSet;
use std::sync::Arc;

use async_trait::async_trait;
use http::{Method, StatusCode};
use serde_json::{json, Map, Value};
use tokio::sync::Mutex as AsyncMutex;
use uuid::Uuid;

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsService, AwsServiceError};
use fakecloud_persistence::SnapshotStore;

use crate::graph::build_service_graph;
use crate::persistence::save_snapshot;
use crate::segment::{parse_segment, StoredSegment};
use crate::state::{now_epoch, SharedXrayState, XrayData, DEFAULT_SAMPLING_RULE};

/// Every operation name in the AWS X-Ray Smithy model (38 operations).
pub const XRAY_ACTIONS: &[&str] = &[
    "BatchGetTraces",
    "CancelTraceRetrieval",
    "CreateGroup",
    "CreateSamplingRule",
    "DeleteGroup",
    "DeleteResourcePolicy",
    "DeleteSamplingRule",
    "GetEncryptionConfig",
    "GetGroup",
    "GetGroups",
    "GetIndexingRules",
    "GetInsight",
    "GetInsightEvents",
    "GetInsightImpactGraph",
    "GetInsightSummaries",
    "GetRetrievedTracesGraph",
    "GetSamplingRules",
    "GetSamplingStatisticSummaries",
    "GetSamplingTargets",
    "GetServiceGraph",
    "GetTimeSeriesServiceStatistics",
    "GetTraceGraph",
    "GetTraceSegmentDestination",
    "GetTraceSummaries",
    "ListResourcePolicies",
    "ListRetrievedTraces",
    "ListTagsForResource",
    "PutEncryptionConfig",
    "PutResourcePolicy",
    "PutTelemetryRecords",
    "PutTraceSegments",
    "StartTraceRetrieval",
    "TagResource",
    "UntagResource",
    "UpdateGroup",
    "UpdateIndexingRule",
    "UpdateSamplingRule",
    "UpdateTraceSegmentDestination",
];

/// Operations that mutate persisted state on success (so a snapshot is taken).
const MUTATING: &[&str] = &[
    "PutTraceSegments",
    "CreateGroup",
    "UpdateGroup",
    "DeleteGroup",
    "CreateSamplingRule",
    "UpdateSamplingRule",
    "DeleteSamplingRule",
    "PutEncryptionConfig",
    "PutResourcePolicy",
    "DeleteResourcePolicy",
    "TagResource",
    "UntagResource",
    "UpdateTraceSegmentDestination",
    "UpdateIndexingRule",
    "StartTraceRetrieval",
    "CancelTraceRetrieval",
];

pub struct XrayService {
    state: SharedXrayState,
    snapshot_store: Option<Arc<dyn SnapshotStore>>,
    snapshot_lock: Arc<AsyncMutex<()>>,
}

impl XrayService {
    pub fn new(state: SharedXrayState) -> Self {
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

    /// Persist hook for the CloudFormation provisioner; `None` in memory mode.
    pub fn snapshot_hook(&self) -> Option<fakecloud_persistence::SnapshotHook> {
        let store = self.snapshot_store.clone()?;
        let state = self.state.clone();
        let lock = self.snapshot_lock.clone();
        Some(Arc::new(move || {
            let state = state.clone();
            let store = store.clone();
            let lock = lock.clone();
            Box::pin(async move {
                crate::persistence::save_snapshot(&state, Some(store), &lock).await;
            })
        }))
    }

    /// Route a request to an operation name by its `@http` URI path.
    fn resolve_action(req: &AwsRequest) -> Option<&'static str> {
        if req.method != Method::POST {
            return None;
        }
        let raw = req.raw_path.split('?').next().unwrap_or(&req.raw_path);
        let path = raw.strip_suffix('/').unwrap_or(raw);
        let action = match path {
            "/Traces" => "BatchGetTraces",
            "/CancelTraceRetrieval" => "CancelTraceRetrieval",
            "/CreateGroup" => "CreateGroup",
            "/CreateSamplingRule" => "CreateSamplingRule",
            "/DeleteGroup" => "DeleteGroup",
            "/DeleteResourcePolicy" => "DeleteResourcePolicy",
            "/DeleteSamplingRule" => "DeleteSamplingRule",
            "/EncryptionConfig" => "GetEncryptionConfig",
            "/GetGroup" => "GetGroup",
            "/Groups" => "GetGroups",
            "/GetIndexingRules" => "GetIndexingRules",
            "/Insight" => "GetInsight",
            "/InsightEvents" => "GetInsightEvents",
            "/InsightImpactGraph" => "GetInsightImpactGraph",
            "/InsightSummaries" => "GetInsightSummaries",
            "/GetRetrievedTracesGraph" => "GetRetrievedTracesGraph",
            "/GetSamplingRules" => "GetSamplingRules",
            "/SamplingStatisticSummaries" => "GetSamplingStatisticSummaries",
            "/SamplingTargets" => "GetSamplingTargets",
            "/ServiceGraph" => "GetServiceGraph",
            "/TimeSeriesServiceStatistics" => "GetTimeSeriesServiceStatistics",
            "/TraceGraph" => "GetTraceGraph",
            "/GetTraceSegmentDestination" => "GetTraceSegmentDestination",
            "/TraceSummaries" => "GetTraceSummaries",
            "/ListResourcePolicies" => "ListResourcePolicies",
            "/ListRetrievedTraces" => "ListRetrievedTraces",
            "/ListTagsForResource" => "ListTagsForResource",
            "/PutEncryptionConfig" => "PutEncryptionConfig",
            "/PutResourcePolicy" => "PutResourcePolicy",
            "/TelemetryRecords" => "PutTelemetryRecords",
            "/TraceSegments" => "PutTraceSegments",
            "/StartTraceRetrieval" => "StartTraceRetrieval",
            "/TagResource" => "TagResource",
            "/UntagResource" => "UntagResource",
            "/UpdateGroup" => "UpdateGroup",
            "/UpdateIndexingRule" => "UpdateIndexingRule",
            "/UpdateSamplingRule" => "UpdateSamplingRule",
            "/UpdateTraceSegmentDestination" => "UpdateTraceSegmentDestination",
            _ => return None,
        };
        Some(action)
    }
}

#[async_trait]
impl AwsService for XrayService {
    fn service_name(&self) -> &str {
        "xray"
    }

    async fn handle(&self, req: AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let Some(action) = Self::resolve_action(&req) else {
            return Err(AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "UnknownOperationException",
                format!("Unknown operation: {} {}", req.method, req.raw_path),
            ));
        };
        let result = self.dispatch(action, &req);
        let success = matches!(result.as_ref(), Ok(resp) if resp.status.is_success());
        if MUTATING.contains(&action) && success {
            self.save().await;
        }
        result
    }

    fn supported_actions(&self) -> &[&str] {
        XRAY_ACTIONS
    }
}

/// Per-request account + region context.
struct Ctx {
    account: String,
    region: String,
}

impl XrayService {
    fn dispatch(&self, action: &str, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req)?;
        crate::validate::validate_input(action, &body)?;
        let ctx = Ctx {
            account: req.account_id.clone(),
            region: req.region.clone(),
        };
        match action {
            "PutTraceSegments" => self.put_trace_segments(&ctx, &body),
            "BatchGetTraces" => self.batch_get_traces(&ctx, &body),
            "GetTraceSummaries" => self.get_trace_summaries(&ctx, &body),
            "GetServiceGraph" => self.get_service_graph(&ctx, &body),
            "GetTraceGraph" => self.get_trace_graph(&ctx, &body),
            "GetTimeSeriesServiceStatistics" => self.get_time_series(&ctx, &body),
            "CreateGroup" => self.create_group(&ctx, &body),
            "GetGroup" => self.get_group(&ctx, &body),
            "GetGroups" => self.get_groups(&ctx),
            "UpdateGroup" => self.update_group(&ctx, &body),
            "DeleteGroup" => self.delete_group(&ctx, &body),
            "CreateSamplingRule" => self.create_sampling_rule(&ctx, &body),
            "GetSamplingRules" => self.get_sampling_rules(&ctx),
            "UpdateSamplingRule" => self.update_sampling_rule(&ctx, &body),
            "DeleteSamplingRule" => self.delete_sampling_rule(&ctx, &body),
            "GetSamplingTargets" => self.get_sampling_targets(&ctx, &body),
            "GetSamplingStatisticSummaries" => Self::get_sampling_statistic_summaries(),
            "GetEncryptionConfig" => self.get_encryption_config(&ctx),
            "PutEncryptionConfig" => self.put_encryption_config(&ctx, &body),
            "PutResourcePolicy" => self.put_resource_policy(&ctx, &body),
            "DeleteResourcePolicy" => self.delete_resource_policy(&ctx, &body),
            "ListResourcePolicies" => self.list_resource_policies(&ctx),
            "TagResource" => self.tag_resource(&ctx, &body),
            "UntagResource" => self.untag_resource(&ctx, &body),
            "ListTagsForResource" => self.list_tags_for_resource(&ctx, &body),
            "GetIndexingRules" => Self::get_indexing_rules(),
            "UpdateIndexingRule" => Self::update_indexing_rule(&body),
            "GetTraceSegmentDestination" => self.get_trace_segment_destination(&ctx),
            "UpdateTraceSegmentDestination" => self.update_trace_segment_destination(&ctx, &body),
            "PutTelemetryRecords" => Ok(ok(json!({}))),
            "StartTraceRetrieval" => self.start_trace_retrieval(&ctx, &body),
            "ListRetrievedTraces" => self.list_retrieved_traces(&ctx, &body),
            "GetRetrievedTracesGraph" => self.get_retrieved_traces_graph(&ctx, &body),
            "CancelTraceRetrieval" => self.cancel_trace_retrieval(&ctx, &body),
            "GetInsight" => Self::get_insight(&body),
            "GetInsightEvents" => Self::get_insight_events(&body),
            "GetInsightImpactGraph" => Self::get_insight_impact_graph(&body),
            "GetInsightSummaries" => Self::get_insight_summaries(),
            _ => Err(AwsServiceError::action_not_implemented("xray", action)),
        }
    }

    // ===================== data plane =====================

    fn put_trace_segments(&self, ctx: &Ctx, body: &Value) -> Result<AwsResponse, AwsServiceError> {
        let docs = body
            .get("TraceSegmentDocuments")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        let mut unprocessed: Vec<Value> = Vec::new();
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        for doc in &docs {
            let Some(s) = doc.as_str() else {
                unprocessed.push(json!({
                    "ErrorCode": "MalformedJson",
                    "Message": "Trace segment document must be a JSON string."
                }));
                continue;
            };
            match parse_segment(s) {
                Ok(seg) => {
                    let entry = data.traces.entry(seg.trace_id.clone()).or_default();
                    // A segment can be re-sent in pieces; replace any prior copy
                    // with the same id.
                    entry.retain(|e| e.id != seg.id);
                    entry.push(seg);
                }
                Err(e) => {
                    let mut m = Map::new();
                    if let Some(id) = e.id {
                        m.insert("Id".into(), json!(id));
                    }
                    m.insert("ErrorCode".into(), json!(e.code));
                    m.insert("Message".into(), json!(e.message));
                    unprocessed.push(Value::Object(m));
                }
            }
        }
        Ok(ok(json!({ "UnprocessedTraceSegments": unprocessed })))
    }

    fn batch_get_traces(&self, ctx: &Ctx, body: &Value) -> Result<AwsResponse, AwsServiceError> {
        let ids = string_list(body, "TraceIds");
        let guard = self.state.read();
        let data = guard.get(&ctx.account);
        let mut traces: Vec<Value> = Vec::new();
        let mut unprocessed: Vec<Value> = Vec::new();
        for id in &ids {
            match data.and_then(|d| d.traces.get(id)) {
                Some(segs) if !segs.is_empty() => traces.push(assemble_trace(id, segs)),
                _ => unprocessed.push(json!(id)),
            }
        }
        Ok(ok(json!({
            "Traces": traces,
            "UnprocessedTraceIds": unprocessed,
        })))
    }

    fn get_trace_summaries(&self, ctx: &Ctx, body: &Value) -> Result<AwsResponse, AwsServiceError> {
        let (start, end) = time_range(body);
        let filter = body
            .get("FilterExpression")
            .and_then(Value::as_str)
            .unwrap_or("");
        let guard = self.state.read();
        let data = guard.get(&ctx.account);
        let mut summaries: Vec<Value> = Vec::new();
        let mut processed: i64 = 0;
        if let Some(data) = data {
            for (id, segs) in &data.traces {
                let Some(root_start) = segs.iter().map(|s| s.start_time).reduce(f64::min) else {
                    continue;
                };
                if root_start < start || root_start > end {
                    continue;
                }
                processed += 1;
                if trace_matches_filter(filter, segs) {
                    summaries.push(build_trace_summary(id, segs));
                }
            }
        }
        Ok(ok(json!({
            "TraceSummaries": summaries,
            "ApproximateTime": now_epoch(),
            "TracesProcessedCount": processed,
        })))
    }

    fn get_service_graph(&self, ctx: &Ctx, body: &Value) -> Result<AwsResponse, AwsServiceError> {
        let (start, end) = time_range(body);
        let guard = self.state.read();
        let services = match guard.get(&ctx.account) {
            Some(data) => {
                let segs = segments_in_range(data, start, end);
                build_service_graph(&segs)
            }
            None => Vec::new(),
        };
        Ok(ok(json!({
            "StartTime": start,
            "EndTime": end,
            "Services": services,
            "ContainsOldGroupVersions": false,
        })))
    }

    fn get_trace_graph(&self, ctx: &Ctx, body: &Value) -> Result<AwsResponse, AwsServiceError> {
        let ids: BTreeSet<String> = string_list(body, "TraceIds").into_iter().collect();
        let guard = self.state.read();
        let services = match guard.get(&ctx.account) {
            Some(data) => {
                let segs: Vec<&StoredSegment> = data
                    .traces
                    .iter()
                    .filter(|(id, _)| ids.contains(*id))
                    .flat_map(|(_, s)| s.iter())
                    .collect();
                build_service_graph(&segs)
            }
            None => Vec::new(),
        };
        Ok(ok(json!({ "Services": services })))
    }

    fn get_time_series(&self, ctx: &Ctx, body: &Value) -> Result<AwsResponse, AwsServiceError> {
        let (start, end) = time_range(body);
        // Bucket the window by `Period` seconds. Bound the number of buckets so
        // an extreme or unbounded window (e.g. a negative StartTime, or a
        // default `now`-wide range) can never spin into a multi-million-iteration
        // loop; AWS itself caps the returned data points.
        const MAX_BUCKETS: usize = 1440;
        let mut period = body
            .get("Period")
            .and_then(Value::as_i64)
            .filter(|p| *p > 0)
            .unwrap_or(60) as f64;
        let guard = self.state.read();
        let mut out: Vec<Value> = Vec::new();
        if let Some(data) = guard.get(&ctx.account) {
            let span = end - start;
            if span > 0.0 && period > 0.0 {
                if span / period > MAX_BUCKETS as f64 {
                    period = span / MAX_BUCKETS as f64;
                }
                let mut bucket = start;
                let mut count = 0usize;
                while bucket < end && count < MAX_BUCKETS {
                    let bucket_end = (bucket + period).min(end);
                    let segs: Vec<&StoredSegment> = data
                        .traces
                        .values()
                        .flat_map(|s| s.iter())
                        .filter(|s| s.start_time >= bucket && s.start_time < bucket_end)
                        .collect();
                    if !segs.is_empty() {
                        out.push(json!({
                            "Timestamp": bucket,
                            "ServiceSummaryStatistics": aggregate_statistics(&segs),
                            "ResponseTimeHistogram": [],
                        }));
                    }
                    bucket = bucket_end;
                    count += 1;
                }
            }
        }
        Ok(ok(json!({
            "TimeSeriesServiceStatistics": out,
            "ContainsOldGroupVersions": false,
        })))
    }

    // ===================== groups =====================

    fn create_group(&self, ctx: &Ctx, body: &Value) -> Result<AwsResponse, AwsServiceError> {
        let name = body
            .get("GroupName")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        if data.groups.contains_key(&name) {
            return Err(invalid(&format!("Group {name} already exists.")));
        }
        let arn = group_arn(&ctx.region, &ctx.account, &name);
        let mut group = Map::new();
        group.insert("GroupName".into(), json!(name));
        group.insert("GroupARN".into(), json!(arn));
        group.insert(
            "FilterExpression".into(),
            body.get("FilterExpression").cloned().unwrap_or(json!("")),
        );
        group.insert(
            "InsightsConfiguration".into(),
            insights_config(body.get("InsightsConfiguration")),
        );
        data.groups.insert(name, Value::Object(group.clone()));
        store_tags(data, &arn, body.get("Tags"));
        Ok(ok(json!({ "Group": Value::Object(group) })))
    }

    fn get_group(&self, ctx: &Ctx, body: &Value) -> Result<AwsResponse, AwsServiceError> {
        let guard = self.state.read();
        let data = guard.get(&ctx.account);
        let group = data
            .and_then(|d| find_group(d, body))
            .ok_or_else(|| invalid("Group not found."))?;
        Ok(ok(json!({ "Group": group })))
    }

    fn get_groups(&self, ctx: &Ctx) -> Result<AwsResponse, AwsServiceError> {
        let guard = self.state.read();
        let groups: Vec<Value> = guard
            .get(&ctx.account)
            .map(|d| d.groups.values().cloned().collect())
            .unwrap_or_default();
        Ok(ok(json!({ "Groups": groups })))
    }

    fn update_group(&self, ctx: &Ctx, body: &Value) -> Result<AwsResponse, AwsServiceError> {
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        let Some(name) = group_key(data, body) else {
            return Err(invalid("Group not found."));
        };
        let group = data
            .groups
            .get_mut(&name)
            .and_then(Value::as_object_mut)
            .expect("group key resolved above");
        if let Some(fe) = body.get("FilterExpression") {
            group.insert("FilterExpression".into(), fe.clone());
        }
        if body.get("InsightsConfiguration").is_some() {
            group.insert(
                "InsightsConfiguration".into(),
                insights_config(body.get("InsightsConfiguration")),
            );
        }
        let out = group.clone();
        Ok(ok(json!({ "Group": Value::Object(out) })))
    }

    fn delete_group(&self, ctx: &Ctx, body: &Value) -> Result<AwsResponse, AwsServiceError> {
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        let Some(name) = group_key(data, body) else {
            return Err(invalid("Group not found."));
        };
        if let Some(g) = data.groups.remove(&name) {
            if let Some(arn) = g.get("GroupARN").and_then(Value::as_str) {
                data.tags.remove(arn);
            }
        }
        Ok(ok(json!({})))
    }

    // ===================== sampling rules =====================

    fn create_sampling_rule(
        &self,
        ctx: &Ctx,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let rule_in = body.get("SamplingRule").cloned().unwrap_or(json!({}));
        let name = rule_in
            .get("RuleName")
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
            .map(std::string::ToString::to_string)
            .unwrap_or_else(|| format!("rule-{}", short_id()));
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        if data.sampling_rules.contains_key(&name) {
            return Err(invalid(&format!("Sampling rule {name} already exists.")));
        }
        let arn = sampling_rule_arn(&ctx.region, &ctx.account, &name);
        let mut rule = rule_in.as_object().cloned().unwrap_or_default();
        rule.insert("RuleName".into(), json!(name));
        rule.insert("RuleARN".into(), json!(arn));
        rule.entry("Version").or_insert(json!(1));
        rule.entry("Attributes")
            .or_insert(Value::Object(Map::new()));
        let now = now_epoch();
        let record = json!({
            "SamplingRule": Value::Object(rule),
            "CreatedAt": now,
            "ModifiedAt": now,
        });
        data.sampling_rules.insert(name, record.clone());
        store_tags(data, &arn, body.get("Tags"));
        Ok(ok(json!({ "SamplingRuleRecord": record })))
    }

    fn get_sampling_rules(&self, ctx: &Ctx) -> Result<AwsResponse, AwsServiceError> {
        let guard = self.state.read();
        let records: Vec<Value> = guard
            .get(&ctx.account)
            .map(|d| d.sampling_rules.values().cloned().collect())
            .unwrap_or_default();
        Ok(ok(json!({ "SamplingRuleRecords": records })))
    }

    fn update_sampling_rule(
        &self,
        ctx: &Ctx,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let update = body.get("SamplingRuleUpdate").cloned().unwrap_or(json!({}));
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        let Some(name) = sampling_rule_key(data, &update) else {
            return Err(invalid("Sampling rule not found."));
        };
        let record = data
            .sampling_rules
            .get_mut(&name)
            .and_then(Value::as_object_mut)
            .expect("rule key resolved above");
        if let Some(rule) = record
            .get_mut("SamplingRule")
            .and_then(Value::as_object_mut)
        {
            for key in [
                "ResourceARN",
                "Priority",
                "FixedRate",
                "ReservoirSize",
                "ServiceName",
                "ServiceType",
                "Host",
                "HTTPMethod",
                "URLPath",
                "Attributes",
            ] {
                if let Some(v) = update.get(key) {
                    rule.insert(key.to_string(), v.clone());
                }
            }
        }
        record.insert("ModifiedAt".into(), json!(now_epoch()));
        let out = record.clone();
        Ok(ok(json!({ "SamplingRuleRecord": Value::Object(out) })))
    }

    fn delete_sampling_rule(
        &self,
        ctx: &Ctx,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        let Some(name) = sampling_rule_key(data, body) else {
            return Err(invalid("Sampling rule not found."));
        };
        if name == DEFAULT_SAMPLING_RULE {
            return Err(invalid("The Default sampling rule cannot be deleted."));
        }
        let record = data
            .sampling_rules
            .remove(&name)
            .expect("rule key resolved above");
        if let Some(arn) = record
            .get("SamplingRule")
            .and_then(|r| r.get("RuleARN"))
            .and_then(Value::as_str)
        {
            data.tags.remove(arn);
        }
        Ok(ok(json!({ "SamplingRuleRecord": record })))
    }

    fn get_sampling_targets(
        &self,
        ctx: &Ctx,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let docs = body
            .get("SamplingStatisticsDocuments")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        let guard = self.state.read();
        let data = guard.get(&ctx.account);
        let mut targets: Vec<Value> = Vec::new();
        for doc in &docs {
            let rule_name = doc
                .get("RuleName")
                .and_then(Value::as_str)
                .unwrap_or(DEFAULT_SAMPLING_RULE);
            let fixed_rate = data
                .and_then(|d| d.sampling_rules.get(rule_name))
                .and_then(|r| r.get("SamplingRule"))
                .and_then(|r| r.get("FixedRate"))
                .and_then(Value::as_f64)
                .unwrap_or(0.05);
            targets.push(json!({
                "RuleName": rule_name,
                "FixedRate": fixed_rate,
                "ReservoirQuota": 1,
                "ReservoirQuotaTTL": now_epoch() + 10.0,
                "Interval": 10,
            }));
        }
        Ok(ok(json!({
            "SamplingTargetDocuments": targets,
            "LastRuleModification": now_epoch(),
            "UnprocessedStatistics": [],
        })))
    }

    fn get_sampling_statistic_summaries() -> Result<AwsResponse, AwsServiceError> {
        Ok(ok(json!({ "SamplingStatisticSummaries": [] })))
    }

    // ===================== encryption config =====================

    fn get_encryption_config(&self, ctx: &Ctx) -> Result<AwsResponse, AwsServiceError> {
        let guard = self.state.read();
        let cfg = guard
            .get(&ctx.account)
            .and_then(|d| d.encryption_config.clone())
            .unwrap_or_else(default_encryption_config);
        Ok(ok(json!({ "EncryptionConfig": cfg })))
    }

    fn put_encryption_config(
        &self,
        ctx: &Ctx,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let etype = body.get("Type").and_then(Value::as_str).unwrap_or("NONE");
        let mut cfg = Map::new();
        cfg.insert("Type".into(), json!(etype));
        cfg.insert("Status".into(), json!("ACTIVE"));
        if etype == "KMS" {
            let key = body
                .get("KeyId")
                .and_then(Value::as_str)
                .unwrap_or_default();
            cfg.insert("KeyId".into(), json!(key));
        }
        let cfg = Value::Object(cfg);
        let mut guard = self.state.write();
        guard.get_or_create(&ctx.account).encryption_config = Some(cfg.clone());
        Ok(ok(json!({ "EncryptionConfig": cfg })))
    }

    // ===================== resource policies =====================

    fn put_resource_policy(&self, ctx: &Ctx, body: &Value) -> Result<AwsResponse, AwsServiceError> {
        let name = body
            .get("PolicyName")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        let doc = body
            .get("PolicyDocument")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        let policy = json!({
            "PolicyName": name,
            "PolicyDocument": doc,
            "PolicyRevisionId": short_id(),
            "LastUpdatedTime": now_epoch(),
        });
        data.resource_policies.insert(name, policy.clone());
        Ok(ok(json!({ "ResourcePolicy": policy })))
    }

    fn delete_resource_policy(
        &self,
        ctx: &Ctx,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = body
            .get("PolicyName")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let mut guard = self.state.write();
        guard
            .get_or_create(&ctx.account)
            .resource_policies
            .remove(name);
        Ok(ok(json!({})))
    }

    fn list_resource_policies(&self, ctx: &Ctx) -> Result<AwsResponse, AwsServiceError> {
        let guard = self.state.read();
        let policies: Vec<Value> = guard
            .get(&ctx.account)
            .map(|d| d.resource_policies.values().cloned().collect())
            .unwrap_or_default();
        Ok(ok(json!({ "ResourcePolicies": policies })))
    }

    // ===================== tagging =====================

    fn tag_resource(&self, ctx: &Ctx, body: &Value) -> Result<AwsResponse, AwsServiceError> {
        let arn = body
            .get("ResourceARN")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        if !resource_exists(data, &arn) {
            return Err(not_found(&format!("Resource {arn} not found.")));
        }
        let entry = data.tags.entry(arn).or_default();
        if let Some(list) = body.get("Tags").and_then(Value::as_array) {
            for tag in list {
                if let (Some(k), Some(v)) = (
                    tag.get("Key").and_then(Value::as_str),
                    tag.get("Value").and_then(Value::as_str),
                ) {
                    entry.insert(k.to_string(), v.to_string());
                }
            }
        }
        if entry.len() > 50 {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "TooManyTagsException",
                "The maximum number of tags (50) has been exceeded.",
            ));
        }
        Ok(ok(json!({})))
    }

    fn untag_resource(&self, ctx: &Ctx, body: &Value) -> Result<AwsResponse, AwsServiceError> {
        let arn = body
            .get("ResourceARN")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        if !resource_exists(data, &arn) {
            return Err(not_found(&format!("Resource {arn} not found.")));
        }
        if let Some(entry) = data.tags.get_mut(&arn) {
            for key in string_list(body, "TagKeys") {
                entry.remove(&key);
            }
        }
        Ok(ok(json!({})))
    }

    fn list_tags_for_resource(
        &self,
        ctx: &Ctx,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let arn = body
            .get("ResourceARN")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        let guard = self.state.read();
        let data = guard
            .get(&ctx.account)
            .ok_or_else(|| not_found(&format!("Resource {arn} not found.")))?;
        if !resource_exists(data, &arn) {
            return Err(not_found(&format!("Resource {arn} not found.")));
        }
        let tags: Vec<Value> = data
            .tags
            .get(&arn)
            .map(|m| {
                m.iter()
                    .map(|(k, v)| json!({ "Key": k, "Value": v }))
                    .collect()
            })
            .unwrap_or_default();
        Ok(ok(json!({ "Tags": tags })))
    }

    // ===================== indexing rules =====================

    fn get_indexing_rules() -> Result<AwsResponse, AwsServiceError> {
        Ok(ok(json!({ "IndexingRules": [default_indexing_rule()] })))
    }

    fn update_indexing_rule(body: &Value) -> Result<AwsResponse, AwsServiceError> {
        let name = body
            .get("Name")
            .and_then(Value::as_str)
            .unwrap_or("Default");
        Ok(ok(json!({
            "IndexingRule": {
                "Name": name,
                "ModifiedAt": now_epoch(),
                "Rule": body.get("Rule").cloned().unwrap_or_else(|| default_indexing_rule()["Rule"].clone()),
            }
        })))
    }

    // ===================== trace-segment destination =====================

    fn get_trace_segment_destination(&self, ctx: &Ctx) -> Result<AwsResponse, AwsServiceError> {
        let guard = self.state.read();
        let dest = guard
            .get(&ctx.account)
            .map(|d| d.trace_segment_destination.clone())
            .unwrap_or_else(|| "XRay".to_string());
        Ok(ok(json!({ "Destination": dest, "Status": "ACTIVE" })))
    }

    fn update_trace_segment_destination(
        &self,
        ctx: &Ctx,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let dest = body
            .get("Destination")
            .and_then(Value::as_str)
            .unwrap_or("XRay")
            .to_string();
        let mut guard = self.state.write();
        guard.get_or_create(&ctx.account).trace_segment_destination = dest.clone();
        Ok(ok(json!({ "Destination": dest, "Status": "ACTIVE" })))
    }

    // ===================== trace retrieval (Transaction Search) =====================

    fn start_trace_retrieval(
        &self,
        ctx: &Ctx,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let token = format!("{}{}", Uuid::new_v4().simple(), Uuid::new_v4().simple());
        let record = json!({
            "TraceIds": body.get("TraceIds").cloned().unwrap_or(json!([])),
            "StartTime": body.get("StartTime").cloned().unwrap_or(json!(0)),
            "EndTime": body.get("EndTime").cloned().unwrap_or(json!(0)),
        });
        let mut guard = self.state.write();
        guard
            .get_or_create(&ctx.account)
            .retrievals
            .insert(token.clone(), record);
        Ok(ok(json!({ "RetrievalToken": token })))
    }

    fn list_retrieved_traces(
        &self,
        ctx: &Ctx,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let token = body
            .get("RetrievalToken")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let format = body
            .get("TraceFormat")
            .and_then(Value::as_str)
            .unwrap_or("XRAY");
        let guard = self.state.read();
        let exists = guard
            .get(&ctx.account)
            .map(|d| d.retrievals.contains_key(token))
            .unwrap_or(false);
        if !exists {
            return Err(not_found("Trace retrieval not found for the given token."));
        }
        Ok(ok(json!({
            "RetrievalStatus": "COMPLETE",
            "TraceFormat": format,
            "Traces": [],
        })))
    }

    fn get_retrieved_traces_graph(
        &self,
        ctx: &Ctx,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let token = body
            .get("RetrievalToken")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let guard = self.state.read();
        let data = guard
            .get(&ctx.account)
            .ok_or_else(|| not_found("Trace retrieval not found for the given token."))?;
        let record = data
            .retrievals
            .get(token)
            .ok_or_else(|| not_found("Trace retrieval not found for the given token."))?;
        let ids: BTreeSet<String> = record
            .get("TraceIds")
            .and_then(Value::as_array)
            .map(|a| {
                a.iter()
                    .filter_map(|v| v.as_str().map(std::string::ToString::to_string))
                    .collect()
            })
            .unwrap_or_default();
        let segs: Vec<&StoredSegment> = data
            .traces
            .iter()
            .filter(|(id, _)| ids.contains(*id))
            .flat_map(|(_, s)| s.iter())
            .collect();
        let services = build_service_graph(&segs);
        Ok(ok(json!({
            "RetrievalStatus": "COMPLETE",
            "Services": services,
        })))
    }

    fn cancel_trace_retrieval(
        &self,
        ctx: &Ctx,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let token = body
            .get("RetrievalToken")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        if data.retrievals.remove(&token).is_none() {
            return Err(not_found("Trace retrieval not found for the given token."));
        }
        Ok(ok(json!({})))
    }

    // ===================== insights =====================
    //
    // X-Ray insights are generated by the service from detected anomalies in
    // ingested traces; fakecloud does not run anomaly detection, so no insight
    // ids exist. A lookup by a specific insight id therefore returns the
    // declared `InvalidRequestException` (a nonexistent insight), while the
    // list/summary op returns an empty page.

    fn get_insight(body: &Value) -> Result<AwsResponse, AwsServiceError> {
        let id = body.get("InsightId").and_then(Value::as_str).unwrap_or("");
        Err(invalid(&format!("Insight {id} not found.")))
    }

    fn get_insight_events(body: &Value) -> Result<AwsResponse, AwsServiceError> {
        let id = body.get("InsightId").and_then(Value::as_str).unwrap_or("");
        Err(invalid(&format!("Insight {id} not found.")))
    }

    fn get_insight_impact_graph(body: &Value) -> Result<AwsResponse, AwsServiceError> {
        let id = body.get("InsightId").and_then(Value::as_str).unwrap_or("");
        Err(invalid(&format!("Insight {id} not found.")))
    }

    fn get_insight_summaries() -> Result<AwsResponse, AwsServiceError> {
        Ok(ok(json!({ "InsightSummaries": [] })))
    }
}

// ===================== helpers =====================

fn ok(v: Value) -> AwsResponse {
    AwsResponse::json_value(StatusCode::OK, v)
}

fn parse_body(req: &AwsRequest) -> Result<Value, AwsServiceError> {
    if req.body.is_empty() {
        return Ok(json!({}));
    }
    serde_json::from_slice(&req.body)
        .map_err(|e| invalid(&format!("Request body is malformed: {e}")))
}

fn invalid(msg: &str) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::BAD_REQUEST, "InvalidRequestException", msg)
}

/// Only the tagging and trace-retrieval operations declare
/// `ResourceNotFoundException`; all other not-present cases use
/// `InvalidRequestException` (which every X-Ray op declares).
fn not_found(msg: &str) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::NOT_FOUND, "ResourceNotFoundException", msg)
}

fn short_id() -> String {
    fakecloud_core::ids::short_id(12)
}

fn group_arn(region: &str, account: &str, name: &str) -> String {
    format!(
        "arn:aws:xray:{region}:{account}:group/{name}/{}",
        short_id()
    )
}

fn sampling_rule_arn(region: &str, account: &str, name: &str) -> String {
    format!("arn:aws:xray:{region}:{account}:sampling-rule/{name}")
}

fn default_encryption_config() -> Value {
    json!({ "Type": "NONE", "Status": "ACTIVE" })
}

fn default_indexing_rule() -> Value {
    json!({
        "Name": "Default",
        "ModifiedAt": now_epoch(),
        "Rule": { "Probabilistic": { "DesiredSamplingPercentage": 100.0, "ActualSamplingPercentage": 100.0 } },
    })
}

fn insights_config(input: Option<&Value>) -> Value {
    let enabled = input
        .and_then(|c| c.get("InsightsEnabled"))
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let notifications = input
        .and_then(|c| c.get("NotificationsEnabled"))
        .and_then(Value::as_bool)
        .unwrap_or(false);
    json!({ "InsightsEnabled": enabled, "NotificationsEnabled": notifications })
}

/// Whether an ARN names a group or sampling rule that exists in this account.
fn resource_exists(data: &XrayData, arn: &str) -> bool {
    data.groups
        .values()
        .any(|g| g.get("GroupARN").and_then(Value::as_str) == Some(arn))
        || data.sampling_rules.values().any(|r| {
            r.get("SamplingRule")
                .and_then(|s| s.get("RuleARN"))
                .and_then(Value::as_str)
                == Some(arn)
        })
}

fn store_tags(data: &mut XrayData, arn: &str, tags: Option<&Value>) {
    let Some(list) = tags.and_then(Value::as_array) else {
        return;
    };
    if list.is_empty() {
        return;
    }
    let entry = data.tags.entry(arn.to_string()).or_default();
    for tag in list {
        if let (Some(k), Some(v)) = (
            tag.get("Key").and_then(Value::as_str),
            tag.get("Value").and_then(Value::as_str),
        ) {
            entry.insert(k.to_string(), v.to_string());
        }
    }
}

/// Resolve a group by `GroupName` or `GroupARN` from a request body, returning
/// its stored wire object.
fn find_group(data: &XrayData, body: &Value) -> Option<Value> {
    group_key(data, body).and_then(|k| data.groups.get(&k).cloned())
}

/// Resolve a group's map key (`GroupName`) from a `GroupName` / `GroupARN` body.
fn group_key(data: &XrayData, body: &Value) -> Option<String> {
    if let Some(name) = body.get("GroupName").and_then(Value::as_str) {
        if data.groups.contains_key(name) {
            return Some(name.to_string());
        }
    }
    if let Some(arn) = body.get("GroupARN").and_then(Value::as_str) {
        return data
            .groups
            .iter()
            .find(|(_, g)| g.get("GroupARN").and_then(Value::as_str) == Some(arn))
            .map(|(k, _)| k.clone());
    }
    None
}

/// Resolve a sampling rule's map key (`RuleName`) from a `RuleName` / `RuleARN`
/// body.
fn sampling_rule_key(data: &XrayData, body: &Value) -> Option<String> {
    if let Some(name) = body.get("RuleName").and_then(Value::as_str) {
        if data.sampling_rules.contains_key(name) {
            return Some(name.to_string());
        }
    }
    if let Some(arn) = body.get("RuleARN").and_then(Value::as_str) {
        return data
            .sampling_rules
            .iter()
            .find(|(_, r)| {
                r.get("SamplingRule")
                    .and_then(|s| s.get("RuleARN"))
                    .and_then(Value::as_str)
                    == Some(arn)
            })
            .map(|(k, _)| k.clone());
    }
    None
}

fn string_list(body: &Value, key: &str) -> Vec<String> {
    body.get(key)
        .and_then(Value::as_array)
        .map(|a| {
            a.iter()
                .filter_map(|v| v.as_str().map(std::string::ToString::to_string))
                .collect()
        })
        .unwrap_or_default()
}

/// The `[StartTime, EndTime]` window from a request body (epoch seconds).
fn time_range(body: &Value) -> (f64, f64) {
    let start = body.get("StartTime").and_then(Value::as_f64).unwrap_or(0.0);
    let end = body
        .get("EndTime")
        .and_then(Value::as_f64)
        .unwrap_or_else(now_epoch);
    (start, end)
}

fn segments_in_range(data: &XrayData, start: f64, end: f64) -> Vec<&StoredSegment> {
    data.traces
        .values()
        .flat_map(|s| s.iter())
        .filter(|s| s.start_time >= start && s.start_time <= end)
        .collect()
}

/// Assemble a stored trace into the `Trace` wire object with its segments echoed
/// verbatim as `{ Id, Document }`.
fn assemble_trace(id: &str, segs: &[StoredSegment]) -> Value {
    let start = segs.iter().map(|s| s.start_time).reduce(f64::min);
    let end = segs
        .iter()
        .filter_map(|s| s.end_time)
        .reduce(f64::max)
        .or(start);
    let duration = match (start, end) {
        (Some(s), Some(e)) => (e - s).max(0.0),
        _ => 0.0,
    };
    let segments: Vec<Value> = segs
        .iter()
        .map(|s| json!({ "Id": s.id, "Document": s.document }))
        .collect();
    json!({
        "Id": id,
        "Duration": duration,
        "LimitExceeded": false,
        "Segments": segments,
    })
}

/// Build a `TraceSummary` for a stored trace.
fn build_trace_summary(id: &str, segs: &[StoredSegment]) -> Value {
    let start = segs
        .iter()
        .map(|s| s.start_time)
        .reduce(f64::min)
        .unwrap_or(0.0);
    let end = segs
        .iter()
        .filter_map(|s| s.end_time)
        .reduce(f64::max)
        .unwrap_or(start);
    let duration = (end - start).max(0.0);
    // The entry segment is the one with no parent.
    let root = segs.iter().find(|s| s.parent_id.is_none()).or(segs.first());
    let response_time = root.and_then(StoredSegment::duration).unwrap_or(duration);

    let mut http = Map::new();
    if let Some(r) = root {
        if let Some(u) = &r.http_url {
            http.insert("HttpURL".into(), json!(u));
        }
        if let Some(s) = r.http_status {
            http.insert("HttpStatus".into(), json!(s));
        }
        if let Some(m) = &r.http_method {
            http.insert("HttpMethod".into(), json!(m));
        }
    }

    let mut service_names: BTreeSet<&str> = BTreeSet::new();
    for s in segs {
        service_names.insert(s.name.as_str());
    }
    let service_ids: Vec<Value> = service_names
        .iter()
        .map(|n| json!({ "Name": n, "Names": [n], "Type": "AWS::EC2::Instance" }))
        .collect();

    json!({
        "Id": id,
        "StartTime": start,
        "Duration": duration,
        "ResponseTime": response_time,
        "HasFault": segs.iter().any(|s| s.fault),
        "HasError": segs.iter().any(|s| s.error),
        "HasThrottle": segs.iter().any(|s| s.throttle),
        "IsPartial": false,
        "Http": Value::Object(http),
        "Annotations": {},
        "ServiceIds": service_ids,
    })
}

/// Aggregate `ServiceStatistics` over a set of segments (used by the time-series
/// op). Mirrors the graph module's per-node accumulation across the whole set.
fn aggregate_statistics(segs: &[&StoredSegment]) -> Value {
    let mut ok_count = 0i64;
    let mut throttle = 0i64;
    let mut error_other = 0i64;
    let mut fault = 0i64;
    let mut total_rt = 0.0f64;
    for s in segs {
        if let Some(d) = s.duration() {
            total_rt += d;
        }
        if s.fault {
            fault += 1;
        } else if s.throttle {
            throttle += 1;
        } else if s.error {
            error_other += 1;
        } else {
            ok_count += 1;
        }
    }
    json!({
        "OkCount": ok_count,
        "ErrorStatistics": {
            "ThrottleCount": throttle,
            "OtherCount": error_other,
            "TotalCount": throttle + error_other,
        },
        "FaultStatistics": { "OtherCount": fault, "TotalCount": fault },
        "TotalCount": segs.len() as i64,
        "TotalResponseTime": (total_rt * 1_000_000.0).round() / 1_000_000.0,
    })
}

/// Whether a trace matches a `GetTraceSummaries` filter expression.
///
/// fakecloud implements a documented subset of the X-Ray filter-expression
/// language: an empty/whitespace expression matches every trace; the boolean
/// keywords `fault`, `error`, and `throttle` match traces carrying that flag;
/// and `service("NAME")` matches traces containing a service node named `NAME`.
/// Any other (unsupported) expression is treated leniently as a match, so a
/// richer filter never silently drops data.
fn trace_matches_filter(filter: &str, segs: &[StoredSegment]) -> bool {
    let f = filter.trim();
    if f.is_empty() {
        return true;
    }
    if let Some(rest) = f.strip_prefix("service(") {
        if let Some(inner) = rest.strip_suffix(')') {
            let want = inner.trim().trim_matches(|c| c == '"' || c == '\'');
            return segs.iter().any(|s| s.name == want);
        }
    }
    match f {
        "fault" => segs.iter().any(|s| s.fault),
        "error" => segs.iter().any(|s| s.error),
        "throttle" => segs.iter().any(|s| s.throttle),
        _ => true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fakecloud_core::multi_account::MultiAccountState;
    use parking_lot::RwLock;

    fn svc() -> XrayService {
        XrayService::new(Arc::new(RwLock::new(MultiAccountState::new(
            "000000000000",
            "us-east-1",
            "",
        ))))
    }

    fn ctx() -> Ctx {
        Ctx {
            account: "000000000000".into(),
            region: "us-east-1".into(),
        }
    }

    fn body_json(resp: &AwsResponse) -> Value {
        serde_json::from_slice(resp.body.expect_bytes()).expect("json response body")
    }

    fn err_of(r: Result<AwsResponse, AwsServiceError>) -> AwsServiceError {
        match r {
            Ok(_) => panic!("expected an error"),
            Err(e) => e,
        }
    }

    fn ingest(s: &XrayService, docs: &[&str]) {
        let arr: Vec<Value> = docs.iter().map(|d| json!(*d)).collect();
        let out = s
            .put_trace_segments(&ctx(), &json!({ "TraceSegmentDocuments": arr }))
            .unwrap();
        let body = body_json(&out);
        assert!(body["UnprocessedTraceSegments"]
            .as_array()
            .unwrap()
            .is_empty());
    }

    #[test]
    fn put_then_batch_get_roundtrips() {
        let s = svc();
        ingest(
            &s,
            &[r#"{"trace_id":"1-aaaa","id":"1111","name":"web","start_time":1.0,"end_time":2.0}"#],
        );
        let out = s
            .batch_get_traces(&ctx(), &json!({ "TraceIds": ["1-aaaa", "1-missing"] }))
            .unwrap();
        let body = body_json(&out);
        assert_eq!(body["Traces"].as_array().unwrap().len(), 1);
        assert_eq!(body["Traces"][0]["Id"], json!("1-aaaa"));
        assert_eq!(body["UnprocessedTraceIds"], json!(["1-missing"]));
    }

    #[test]
    fn trace_summaries_filter_by_time_and_expression() {
        let s = svc();
        ingest(
            &s,
            &[
                r#"{"trace_id":"1-ok","id":"1","name":"web","start_time":100.0,"end_time":101.0}"#,
                r#"{"trace_id":"1-bad","id":"2","name":"web","start_time":150.0,"end_time":151.0,"fault":true}"#,
            ],
        );
        let all = body_json(
            &s.get_trace_summaries(&ctx(), &json!({ "StartTime": 0, "EndTime": 200 }))
                .unwrap(),
        );
        assert_eq!(all["TraceSummaries"].as_array().unwrap().len(), 2);
        assert_eq!(all["TracesProcessedCount"], json!(2));

        let faults = body_json(
            &s.get_trace_summaries(
                &ctx(),
                &json!({ "StartTime": 0, "EndTime": 200, "FilterExpression": "fault" }),
            )
            .unwrap(),
        );
        assert_eq!(faults["TraceSummaries"].as_array().unwrap().len(), 1);
        assert_eq!(faults["TraceSummaries"][0]["Id"], json!("1-bad"));
    }

    #[test]
    fn service_graph_derives_edge() {
        let s = svc();
        ingest(
            &s,
            &[
                r#"{"trace_id":"1-a","id":"1","name":"web","start_time":1.0,"end_time":3.0,"subsegments":[{"id":"2","name":"db","namespace":"remote","start_time":1.5,"end_time":2.5}]}"#,
            ],
        );
        let body = body_json(
            &s.get_service_graph(&ctx(), &json!({ "StartTime": 0, "EndTime": 10 }))
                .unwrap(),
        );
        let services = body["Services"].as_array().unwrap();
        assert_eq!(services.len(), 2);
        let web = services.iter().find(|x| x["Name"] == json!("web")).unwrap();
        assert_eq!(web["Edges"].as_array().unwrap().len(), 1);
    }

    #[test]
    fn create_get_group_roundtrips() {
        let s = svc();
        let created = body_json(
            &s.create_group(
                &ctx(),
                &json!({ "GroupName": "g1", "FilterExpression": "fault" }),
            )
            .unwrap(),
        );
        assert_eq!(created["Group"]["GroupName"], json!("g1"));
        let got = body_json(&s.get_group(&ctx(), &json!({ "GroupName": "g1" })).unwrap());
        assert_eq!(got["Group"]["FilterExpression"], json!("fault"));
        let groups = body_json(&s.get_groups(&ctx()).unwrap());
        assert_eq!(groups["Groups"].as_array().unwrap().len(), 1);
    }

    #[test]
    fn default_sampling_rule_present_and_undeletable() {
        let s = svc();
        let rules = body_json(&s.get_sampling_rules(&ctx()).unwrap());
        let recs = rules["SamplingRuleRecords"].as_array().unwrap();
        assert!(recs
            .iter()
            .any(|r| r["SamplingRule"]["RuleName"] == json!("Default")));
        let err = err_of(s.delete_sampling_rule(&ctx(), &json!({ "RuleName": "Default" })));
        assert_eq!(err.status(), StatusCode::BAD_REQUEST);
    }

    #[test]
    fn create_and_delete_sampling_rule() {
        let s = svc();
        let rule = json!({ "SamplingRule": {
            "RuleName": "r1", "ResourceARN": "*", "Priority": 5, "FixedRate": 0.1,
            "ReservoirSize": 2, "ServiceName": "*", "ServiceType": "*", "Host": "*",
            "HTTPMethod": "*", "URLPath": "*", "Version": 1
        }});
        let created = body_json(&s.create_sampling_rule(&ctx(), &rule).unwrap());
        assert_eq!(
            created["SamplingRuleRecord"]["SamplingRule"]["RuleName"],
            json!("r1")
        );
        let deleted = s.delete_sampling_rule(&ctx(), &json!({ "RuleName": "r1" }));
        assert!(deleted.is_ok());
    }

    #[test]
    fn encryption_config_roundtrips() {
        let s = svc();
        let default = body_json(&s.get_encryption_config(&ctx()).unwrap());
        assert_eq!(default["EncryptionConfig"]["Type"], json!("NONE"));
        let put = body_json(
            &s.put_encryption_config(&ctx(), &json!({ "Type": "KMS", "KeyId": "alias/xray" }))
                .unwrap(),
        );
        assert_eq!(put["EncryptionConfig"]["Type"], json!("KMS"));
        let got = body_json(&s.get_encryption_config(&ctx()).unwrap());
        assert_eq!(got["EncryptionConfig"]["KeyId"], json!("alias/xray"));
    }

    #[test]
    fn tagging_requires_existing_resource() {
        let s = svc();
        // Unknown ARN -> ResourceNotFoundException.
        let err = err_of(s.tag_resource(
            &ctx(),
            &json!({ "ResourceARN": "arn:aws:xray:us-east-1:000000000000:group/ghost/x", "Tags": [] }),
        ));
        assert_eq!(err.status(), StatusCode::NOT_FOUND);

        let created = body_json(
            &s.create_group(&ctx(), &json!({ "GroupName": "tg" }))
                .unwrap(),
        );
        let arn = created["Group"]["GroupARN"].as_str().unwrap().to_string();
        s.tag_resource(
            &ctx(),
            &json!({ "ResourceARN": arn, "Tags": [{ "Key": "team", "Value": "obs" }] }),
        )
        .unwrap();
        let listed = body_json(
            &s.list_tags_for_resource(&ctx(), &json!({ "ResourceARN": arn }))
                .unwrap(),
        );
        assert_eq!(listed["Tags"][0]["Key"], json!("team"));
    }

    #[test]
    fn trace_retrieval_lifecycle() {
        let s = svc();
        let started = body_json(
            &s.start_trace_retrieval(
                &ctx(),
                &json!({ "TraceIds": ["1-a"], "StartTime": 0, "EndTime": 1 }),
            )
            .unwrap(),
        );
        let token = started["RetrievalToken"].as_str().unwrap().to_string();
        let listed = body_json(
            &s.list_retrieved_traces(&ctx(), &json!({ "RetrievalToken": token }))
                .unwrap(),
        );
        assert_eq!(listed["RetrievalStatus"], json!("COMPLETE"));
        // Unknown token -> ResourceNotFoundException.
        let err = err_of(s.list_retrieved_traces(&ctx(), &json!({ "RetrievalToken": "nope" })));
        assert_eq!(err.status(), StatusCode::NOT_FOUND);
    }

    fn req(path: &str) -> AwsRequest {
        AwsRequest {
            service: "xray".to_string(),
            action: String::new(),
            region: "us-east-1".to_string(),
            account_id: "000000000000".to_string(),
            request_id: "rid".to_string(),
            headers: http::HeaderMap::new(),
            query_params: std::collections::HashMap::new(),
            body: bytes::Bytes::new(),
            body_stream: parking_lot::Mutex::new(None),
            path_segments: vec![],
            raw_path: path.to_string(),
            raw_query: String::new(),
            method: Method::POST,
            is_query_protocol: false,
            access_key_id: None,
            principal: None,
        }
    }

    #[test]
    fn resolve_action_routes_and_rejects() {
        assert_eq!(
            XrayService::resolve_action(&req("/TraceSegments")),
            Some("PutTraceSegments")
        );
        assert_eq!(
            XrayService::resolve_action(&req("/ServiceGraph")),
            Some("GetServiceGraph")
        );
        assert!(XrayService::resolve_action(&req("/NotARealOp")).is_none());
    }
}
