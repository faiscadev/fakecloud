//! AWS Support awsJson1_1 dispatch + operation handlers.
//!
//! Requests carry `X-Amz-Target: AWSSupport_20130415.<Operation>`; dispatch
//! keys off `req.action`. Every operation runs model-driven input validation
//! first (required / length / range / enum / pattern), then real,
//! account-partitioned, persisted CRUD. Dereferencing a case that does not
//! exist returns Support's canonical `CaseIdNotFound`; an unknown attachment id
//! returns `AttachmentIdNotFound`, and an unknown attachment set id returns
//! `AttachmentSetIdNotFound`.
//!
//! Honest gap: fakecloud runs no Trusted Advisor analysis engine and attaches
//! no live support agent. `DescribeTrustedAdvisorCheckResult` /
//! `DescribeTrustedAdvisorCheckSummaries` return well-formed all-clear result
//! shapes (zero flagged resources) rather than fabricating findings, and no
//! automated agent reply is generated. Cases, communications, attachment sets,
//! severity levels, the check catalogue, and the refresh state machine are all
//! real.

use std::sync::Arc;

use async_trait::async_trait;
use http::StatusCode;
use serde_json::{json, Value};
use tokio::sync::Mutex as AsyncMutex;

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsService, AwsServiceError};
use fakecloud_persistence::SnapshotStore;

use crate::catalog;
use crate::shared::{
    current_year, display_id, iso_now, new_attachment_id, new_attachment_set_id, new_case_id,
    str_member,
};
use crate::state::{SharedSupportState, SupportData};

/// Every operation name in the AWS Support Smithy model (16 operations).
pub const SUPPORT_ACTIONS: &[&str] = &[
    "AddAttachmentsToSet",
    "AddCommunicationToCase",
    "CreateCase",
    "DescribeAttachment",
    "DescribeCases",
    "DescribeCommunications",
    "DescribeCreateCaseOptions",
    "DescribeServices",
    "DescribeSeverityLevels",
    "DescribeSupportedLanguages",
    "DescribeTrustedAdvisorCheckRefreshStatuses",
    "DescribeTrustedAdvisorCheckResult",
    "DescribeTrustedAdvisorCheckSummaries",
    "DescribeTrustedAdvisorChecks",
    "RefreshTrustedAdvisorCheck",
    "ResolveCase",
];

/// Read-only verbs; any other action mutates persisted state and triggers a
/// snapshot after success. The inverse formulation guarantees no mutation is
/// ever missed if a new op is added. `DescribeTrustedAdvisorCheckRefreshStatuses`
/// advances the refresh state machine in memory but is intentionally treated as
/// read-only (the transition is re-derived on the next read, so persisting it is
/// not required).
fn is_mutating_action(action: &str) -> bool {
    !action.starts_with("Describe")
}

pub struct SupportService {
    state: SharedSupportState,
    snapshot_store: Option<Arc<dyn SnapshotStore>>,
    snapshot_lock: Arc<AsyncMutex<()>>,
}

impl SupportService {
    pub fn new(state: SharedSupportState) -> Self {
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
        crate::persistence::save_snapshot(
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

    /// Run `f` against this account's mutable state.
    fn with_account_mut<R>(&self, req: &AwsRequest, f: impl FnOnce(&mut SupportData) -> R) -> R {
        let mut guard = self.state.write();
        let acct = guard.get_or_create(&req.account_id);
        f(acct)
    }
}

#[async_trait]
impl AwsService for SupportService {
    fn service_name(&self) -> &str {
        "support"
    }

    async fn handle(&self, req: AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let action = req.action.clone();
        if let Err(msg) = crate::validate::validate_input(&action, &req.json_body()) {
            return Err(validation_error(msg));
        }
        let result = self.dispatch(&action, &req);
        if is_mutating_action(&action)
            && matches!(result.as_ref(), Ok(resp) if resp.status.is_success())
        {
            self.save().await;
        }
        result
    }

    fn supported_actions(&self) -> &[&str] {
        SUPPORT_ACTIONS
    }
}

impl SupportService {
    fn dispatch(&self, action: &str, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        match action {
            // Support Cases
            "CreateCase" => self.create_case(req, &body),
            "DescribeCases" => self.describe_cases(req, &body),
            "DescribeCommunications" => self.describe_communications(req, &body),
            "AddCommunicationToCase" => self.add_communication_to_case(req, &body),
            "ResolveCase" => self.resolve_case(req, &body),
            "AddAttachmentsToSet" => self.add_attachments_to_set(req, &body),
            "DescribeAttachment" => self.describe_attachment(req, &body),
            // Trusted Advisor
            "DescribeTrustedAdvisorChecks" => self.describe_ta_checks(&body),
            "DescribeTrustedAdvisorCheckResult" => self.describe_ta_check_result(&body),
            "DescribeTrustedAdvisorCheckSummaries" => self.describe_ta_check_summaries(&body),
            "DescribeTrustedAdvisorCheckRefreshStatuses" => {
                self.describe_ta_refresh_statuses(req, &body)
            }
            "RefreshTrustedAdvisorCheck" => self.refresh_ta_check(req, &body),
            // Severity levels + case-creation reference data
            "DescribeSeverityLevels" => self.describe_severity_levels(&body),
            "DescribeServices" => self.describe_services(&body),
            "DescribeCreateCaseOptions" => self.describe_create_case_options(&body),
            "DescribeSupportedLanguages" => self.describe_supported_languages(&body),
            _ => Err(AwsServiceError::action_not_implemented(
                self.service_name(),
                action,
            )),
        }
    }

    // ---- Support Cases ----------------------------------------------------

    fn create_case(&self, req: &AwsRequest, body: &Value) -> Result<AwsResponse, AwsServiceError> {
        let account = req.account_id.clone();
        let subject = str_member(body, "subject").unwrap_or_default().to_string();
        let comm_body = str_member(body, "communicationBody")
            .unwrap_or_default()
            .to_string();
        let service_code = str_member(body, "serviceCode")
            .unwrap_or("general-info-and-getting-started")
            .to_string();
        let severity_code = str_member(body, "severityCode")
            .unwrap_or("normal")
            .to_string();
        let category_code = str_member(body, "categoryCode")
            .unwrap_or("other")
            .to_string();
        let language = str_member(body, "language").unwrap_or("en").to_string();
        let cc = body
            .get("ccEmailAddresses")
            .cloned()
            .unwrap_or_else(|| json!([]));
        let attachment_set_id = str_member(body, "attachmentSetId").map(str::to_string);
        let submitted_by = format!("arn:aws:iam::{account}:root");
        let now = iso_now();

        self.with_account_mut(req, |d| {
            // Resolve the attachment set for the initial communication, if any.
            let attachment_set = match &attachment_set_id {
                Some(id) => {
                    let set = d
                        .attachment_sets
                        .get(id)
                        .ok_or_else(|| attachment_set_id_not_found(id))?;
                    attachment_set_summary(d, set)
                }
                None => json!([]),
            };

            let case_id = new_case_id(&account, current_year());
            let disp = display_id(&case_id);
            let case = json!({
                "caseId": case_id,
                "displayId": disp,
                "subject": subject,
                "status": "opened",
                "serviceCode": service_code,
                "categoryCode": category_code,
                "severityCode": severity_code,
                "submittedBy": submitted_by,
                "timeCreated": now,
                "ccEmailAddresses": cc,
                "language": language,
            });
            let comm = json!({
                "caseId": case_id,
                "body": comm_body,
                "submittedBy": submitted_by,
                "timeCreated": now,
                "attachmentSet": attachment_set,
            });
            d.cases.insert(case_id.clone(), case);
            d.communications.insert(case_id.clone(), vec![comm]);
            Ok(ok(json!({ "caseId": case_id })))
        })
    }

    fn describe_cases(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let case_id_list: Option<Vec<String>> =
            body.get("caseIdList").and_then(Value::as_array).map(|a| {
                a.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            });
        let display_id_filter = str_member(body, "displayId").map(str::to_string);
        let after = str_member(body, "afterTime").map(str::to_string);
        let before = str_member(body, "beforeTime").map(str::to_string);
        let include_resolved = body
            .get("includeResolvedCases")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        let include_comms = body
            .get("includeCommunications")
            .and_then(Value::as_bool)
            .unwrap_or(true);
        let max = body
            .get("maxResults")
            .and_then(Value::as_u64)
            .map(|n| n as usize);
        let start = decode_token(str_member(body, "nextToken"));

        self.with_account_mut(req, |d| {
            // Collect the candidate case ids in a deterministic order.
            let mut selected: Vec<Value> = Vec::new();
            if let Some(ids) = &case_id_list {
                for id in ids {
                    let case = d.cases.get(id).ok_or_else(|| case_id_not_found(id))?;
                    selected.push(case.clone());
                }
            } else {
                for case in d.cases.values() {
                    selected.push(case.clone());
                }
            }

            // Apply the non-id filters.
            selected.retain(|c| {
                let status = c.get("status").and_then(Value::as_str).unwrap_or("");
                if !include_resolved && status == "resolved" {
                    return false;
                }
                if let Some(disp) = &display_id_filter {
                    if c.get("displayId").and_then(Value::as_str) != Some(disp.as_str()) {
                        return false;
                    }
                }
                let created = c.get("timeCreated").and_then(Value::as_str).unwrap_or("");
                if let Some(a) = &after {
                    if created < a.as_str() {
                        return false;
                    }
                }
                if let Some(b) = &before {
                    if created > b.as_str() {
                        return false;
                    }
                }
                true
            });

            // Paginate.
            let (page, next) = paginate(&selected, start, max);
            let cases: Vec<Value> = page
                .iter()
                .map(|c| {
                    let mut c = c.clone();
                    let cid = c
                        .get("caseId")
                        .and_then(Value::as_str)
                        .unwrap_or("")
                        .to_string();
                    if include_comms {
                        let comms = d.communications.get(&cid).cloned().unwrap_or_default();
                        c.as_object_mut().unwrap().insert(
                            "recentCommunications".to_string(),
                            json!({ "communications": comms }),
                        );
                    }
                    c
                })
                .collect();

            let mut out = json!({ "cases": cases });
            if let Some(tok) = next {
                out.as_object_mut()
                    .unwrap()
                    .insert("nextToken".to_string(), json!(tok));
            }
            Ok(ok(out))
        })
    }

    fn describe_communications(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let case_id = str_member(body, "caseId").unwrap_or_default().to_string();
        let after = str_member(body, "afterTime").map(str::to_string);
        let before = str_member(body, "beforeTime").map(str::to_string);
        let max = body
            .get("maxResults")
            .and_then(Value::as_u64)
            .map(|n| n as usize);
        let start = decode_token(str_member(body, "nextToken"));

        self.with_account_mut(req, |d| {
            if !d.cases.contains_key(&case_id) {
                return Err(case_id_not_found(&case_id));
            }
            let all = d.communications.get(&case_id).cloned().unwrap_or_default();
            let filtered: Vec<Value> = all
                .into_iter()
                .filter(|c| {
                    let created = c.get("timeCreated").and_then(Value::as_str).unwrap_or("");
                    after.as_deref().map(|a| created >= a).unwrap_or(true)
                        && before.as_deref().map(|b| created <= b).unwrap_or(true)
                })
                .collect();
            let (page, next) = paginate(&filtered, start, max);
            let mut out = json!({ "communications": page });
            if let Some(tok) = next {
                out.as_object_mut()
                    .unwrap()
                    .insert("nextToken".to_string(), json!(tok));
            }
            Ok(ok(out))
        })
    }

    fn add_communication_to_case(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let account = req.account_id.clone();
        let case_id = str_member(body, "caseId").unwrap_or_default().to_string();
        let comm_body = str_member(body, "communicationBody")
            .unwrap_or_default()
            .to_string();
        let attachment_set_id = str_member(body, "attachmentSetId").map(str::to_string);
        let submitted_by = format!("arn:aws:iam::{account}:root");
        let now = iso_now();

        self.with_account_mut(req, |d| {
            if !d.cases.contains_key(&case_id) {
                return Err(case_id_not_found(&case_id));
            }
            let attachment_set = match &attachment_set_id {
                Some(id) => {
                    let set = d
                        .attachment_sets
                        .get(id)
                        .ok_or_else(|| attachment_set_id_not_found(id))?;
                    attachment_set_summary(d, set)
                }
                None => json!([]),
            };
            let comm = json!({
                "caseId": case_id,
                "body": comm_body,
                "submittedBy": submitted_by,
                "timeCreated": now,
                "attachmentSet": attachment_set,
            });
            d.communications
                .entry(case_id.clone())
                .or_default()
                .push(comm);
            Ok(ok(json!({ "result": true })))
        })
    }

    fn resolve_case(&self, req: &AwsRequest, body: &Value) -> Result<AwsResponse, AwsServiceError> {
        let case_id = str_member(body, "caseId").unwrap_or_default().to_string();
        self.with_account_mut(req, |d| {
            let case = d
                .cases
                .get_mut(&case_id)
                .ok_or_else(|| case_id_not_found(&case_id))?;
            let initial = case
                .get("status")
                .and_then(Value::as_str)
                .unwrap_or("opened")
                .to_string();
            case.as_object_mut()
                .unwrap()
                .insert("status".to_string(), json!("resolved"));
            Ok(ok(json!({
                "initialCaseStatus": initial,
                "finalCaseStatus": "resolved",
            })))
        })
    }

    fn add_attachments_to_set(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let requested_set = str_member(body, "attachmentSetId").map(str::to_string);
        let attachments = body
            .get("attachments")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        // Attachment sets expire one hour after their last modification.
        let expiry = (chrono::Utc::now() + chrono::Duration::hours(1))
            .format("%Y-%m-%dT%H:%M:%S%.3fZ")
            .to_string();

        self.with_account_mut(req, |d| {
            let set_id = match &requested_set {
                Some(id) => {
                    if !d.attachment_sets.contains_key(id) {
                        return Err(attachment_set_id_not_found(id));
                    }
                    id.clone()
                }
                None => new_attachment_set_id(),
            };

            let mut ids: Vec<String> = d
                .attachment_sets
                .get(&set_id)
                .and_then(|s| s.get("attachmentIds"))
                .and_then(Value::as_array)
                .map(|a| {
                    a.iter()
                        .filter_map(|v| v.as_str().map(String::from))
                        .collect()
                })
                .unwrap_or_default();

            for att in &attachments {
                let att_id = new_attachment_id();
                let file_name = att.get("fileName").cloned().unwrap_or(json!(""));
                let data = att.get("data").cloned().unwrap_or(json!(""));
                d.attachments.insert(
                    att_id.clone(),
                    json!({ "fileName": file_name, "data": data }),
                );
                ids.push(att_id);
            }

            d.attachment_sets.insert(
                set_id.clone(),
                json!({ "attachmentIds": ids, "expiryTime": expiry }),
            );
            Ok(ok(json!({
                "attachmentSetId": set_id,
                "expiryTime": expiry,
            })))
        })
    }

    fn describe_attachment(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let attachment_id = str_member(body, "attachmentId")
            .unwrap_or_default()
            .to_string();
        self.with_account_mut(req, |d| {
            let att = d
                .attachments
                .get(&attachment_id)
                .ok_or_else(|| attachment_id_not_found(&attachment_id))?;
            Ok(ok(json!({ "attachment": att })))
        })
    }

    // ---- Trusted Advisor --------------------------------------------------

    fn describe_ta_checks(&self, _body: &Value) -> Result<AwsResponse, AwsServiceError> {
        Ok(ok(json!({ "checks": catalog::ta_checks() })))
    }

    fn describe_ta_check_result(&self, body: &Value) -> Result<AwsResponse, AwsServiceError> {
        let check_id = str_member(body, "checkId").unwrap_or_default().to_string();
        Ok(ok(json!({
            "result": ta_check_result(&check_id),
        })))
    }

    fn describe_ta_check_summaries(&self, body: &Value) -> Result<AwsResponse, AwsServiceError> {
        let ids = string_list(body, "checkIds");
        let summaries: Vec<Value> = ids.iter().map(|id| ta_check_summary(id)).collect();
        Ok(ok(json!({ "summaries": summaries })))
    }

    fn describe_ta_refresh_statuses(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let ids = string_list(body, "checkIds");
        let statuses = self.with_account_mut(req, |d| {
            ids.iter()
                .map(|id| {
                    let status = d.advance_refresh(id);
                    json!({
                        "checkId": id,
                        "status": status,
                        "millisUntilNextRefreshable": refresh_interval_ms(&status),
                    })
                })
                .collect::<Vec<Value>>()
        });
        Ok(ok(json!({ "statuses": statuses })))
    }

    fn refresh_ta_check(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let check_id = str_member(body, "checkId").unwrap_or_default().to_string();
        self.with_account_mut(req, |d| {
            d.ta_refresh
                .insert(check_id.clone(), "enqueued".to_string());
        });
        Ok(ok(json!({
            "status": {
                "checkId": check_id,
                "status": "enqueued",
                "millisUntilNextRefreshable": refresh_interval_ms("enqueued"),
            }
        })))
    }

    // ---- Severity levels + reference data ---------------------------------

    fn describe_severity_levels(&self, _body: &Value) -> Result<AwsResponse, AwsServiceError> {
        Ok(ok(json!({ "severityLevels": catalog::severity_levels() })))
    }

    fn describe_services(&self, body: &Value) -> Result<AwsResponse, AwsServiceError> {
        let filter = body
            .get("serviceCodeList")
            .and_then(Value::as_array)
            .map(|a| {
                a.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect::<Vec<_>>()
            });
        Ok(ok(json!({
            "services": catalog::services(filter.as_deref()),
        })))
    }

    fn describe_create_case_options(&self, _body: &Value) -> Result<AwsResponse, AwsServiceError> {
        Ok(ok(json!({
            "languageAvailability": "AVAILABLE",
            "communicationTypes": [
                { "type": "web", "supportedHours": [], "datesWithoutSupport": [] }
            ],
        })))
    }

    fn describe_supported_languages(&self, _body: &Value) -> Result<AwsResponse, AwsServiceError> {
        Ok(ok(json!({
            "supportedLanguages": [
                { "code": "en", "language": "English", "display": "English" },
                { "code": "ja", "language": "Japanese", "display": "日本語" }
            ],
        })))
    }
}

// ---- Trusted Advisor result builders -------------------------------------

fn ta_check_result(check_id: &str) -> Value {
    json!({
        "checkId": check_id,
        "timestamp": iso_now(),
        "status": "ok",
        "resourcesSummary": zero_resources_summary(),
        "categorySpecificSummary": category_specific_summary(check_id),
        "flaggedResources": [],
    })
}

fn ta_check_summary(check_id: &str) -> Value {
    json!({
        "checkId": check_id,
        "timestamp": iso_now(),
        "status": "ok",
        "hasFlaggedResources": false,
        "resourcesSummary": zero_resources_summary(),
        "categorySpecificSummary": category_specific_summary(check_id),
    })
}

fn zero_resources_summary() -> Value {
    json!({
        "resourcesProcessed": 0,
        "resourcesFlagged": 0,
        "resourcesIgnored": 0,
        "resourcesSuppressed": 0,
    })
}

fn category_specific_summary(check_id: &str) -> Value {
    // The cost-optimising summary is only meaningful for cost checks, but the
    // member is always present in the live response with zeroed savings.
    let _ = catalog::check_category(check_id);
    json!({
        "costOptimizing": {
            "estimatedMonthlySavings": 0.0,
            "estimatedPercentMonthlySavings": 0.0,
        }
    })
}

/// Milliseconds until the check may be refreshed again. Zero while a refresh is
/// in flight; a full interval once it has settled.
fn refresh_interval_ms(status: &str) -> i64 {
    match status {
        "success" | "none" => 3_600_000,
        _ => 0,
    }
}

// ---- pagination helpers ---------------------------------------------------

/// Decode a `nextToken` (a plain decimal offset) into a start index.
fn decode_token(token: Option<&str>) -> usize {
    token.and_then(|t| t.parse::<usize>().ok()).unwrap_or(0)
}

/// Return `(page, next_token)` for `items[start..]` limited to `max`.
fn paginate(items: &[Value], start: usize, max: Option<usize>) -> (Vec<Value>, Option<String>) {
    let start = start.min(items.len());
    let remaining = &items[start..];
    match max {
        Some(m) if m < remaining.len() => {
            let page = remaining[..m].to_vec();
            (page, Some((start + m).to_string()))
        }
        _ => (remaining.to_vec(), None),
    }
}

/// Summarise an attachment set (stored as `{attachmentIds, expiryTime}`) into
/// the `AttachmentSet` wire list of `{attachmentId, fileName}`.
fn attachment_set_summary(d: &SupportData, set: &Value) -> Value {
    let ids = set
        .get("attachmentIds")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let details: Vec<Value> = ids
        .iter()
        .filter_map(|v| v.as_str())
        .map(|id| {
            let file_name = d
                .attachments
                .get(id)
                .and_then(|a| a.get("fileName"))
                .cloned()
                .unwrap_or(json!(""));
            json!({ "attachmentId": id, "fileName": file_name })
        })
        .collect();
    json!(details)
}

fn string_list(body: &Value, name: &str) -> Vec<String> {
    body.get(name)
        .and_then(Value::as_array)
        .map(|a| {
            a.iter()
                .filter_map(|v| v.as_str().map(String::from))
                .collect()
        })
        .unwrap_or_default()
}

// ---- error / response helpers --------------------------------------------

fn validation_error(msg: impl Into<String>) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::BAD_REQUEST, "ValidationException", msg.into())
}

fn case_id_not_found(id: &str) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        "CaseIdNotFound",
        format!("Case {id} was not found."),
    )
}

fn attachment_id_not_found(id: &str) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        "AttachmentIdNotFound",
        format!("Attachment {id} was not found."),
    )
}

fn attachment_set_id_not_found(id: &str) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        "AttachmentSetIdNotFound",
        format!("Attachment set {id} was not found."),
    )
}

fn ok(value: Value) -> AwsResponse {
    AwsResponse::ok_json(value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use fakecloud_core::multi_account::MultiAccountState;
    use parking_lot::RwLock;

    fn service() -> SupportService {
        SupportService::new(Arc::new(RwLock::new(MultiAccountState::new(
            "000000000000",
            "us-east-1",
            "",
        ))))
    }

    fn req(action: &str, body: Value) -> AwsRequest {
        AwsRequest {
            service: "support".to_string(),
            action: action.to_string(),
            region: "us-east-1".to_string(),
            account_id: "000000000000".to_string(),
            request_id: "test".to_string(),
            headers: http::HeaderMap::new(),
            query_params: std::collections::HashMap::new(),
            body: bytes::Bytes::from(serde_json::to_vec(&body).unwrap()),
            body_stream: parking_lot::Mutex::new(None),
            path_segments: Vec::new(),
            raw_path: String::new(),
            raw_query: String::new(),
            method: http::Method::POST,
            is_query_protocol: false,
            access_key_id: None,
            principal: None,
        }
    }

    fn body_of(resp: &AwsResponse) -> Value {
        serde_json::from_slice(resp.body.expect_bytes()).unwrap()
    }

    /// `Result::unwrap_err` requires the `Ok` type to be `Debug`; `AwsResponse`
    /// is not, so extract the error by matching instead.
    fn expect_err(r: Result<AwsResponse, AwsServiceError>) -> AwsServiceError {
        match r {
            Ok(_) => panic!("expected an error response"),
            Err(e) => e,
        }
    }

    #[test]
    fn case_lifecycle() {
        let svc = service();
        // Create.
        let created = svc
            .create_case(
                &req("CreateCase", json!({})),
                &json!({ "subject": "Cannot connect", "communicationBody": "help please" }),
            )
            .unwrap();
        let case_id = body_of(&created)["caseId"].as_str().unwrap().to_string();
        assert!(case_id.starts_with("case-000000000000-"));

        // Describe returns the case with its seeded communication.
        let described = svc
            .describe_cases(&req("DescribeCases", json!({})), &json!({}))
            .unwrap();
        let out = body_of(&described);
        assert_eq!(out["cases"].as_array().unwrap().len(), 1);
        let comms = &out["cases"][0]["recentCommunications"]["communications"];
        assert_eq!(comms.as_array().unwrap().len(), 1);
        assert_eq!(comms[0]["body"], "help please");

        // Add a communication.
        let added = svc
            .add_communication_to_case(
                &req("AddCommunicationToCase", json!({})),
                &json!({ "caseId": case_id, "communicationBody": "any update?" }),
            )
            .unwrap();
        assert_eq!(body_of(&added)["result"], true);
        let comms = svc
            .describe_communications(
                &req("DescribeCommunications", json!({})),
                &json!({ "caseId": case_id }),
            )
            .unwrap();
        assert_eq!(
            body_of(&comms)["communications"].as_array().unwrap().len(),
            2
        );

        // Resolve.
        let resolved = svc
            .resolve_case(
                &req("ResolveCase", json!({})),
                &json!({ "caseId": case_id }),
            )
            .unwrap();
        let out = body_of(&resolved);
        assert_eq!(out["initialCaseStatus"], "opened");
        assert_eq!(out["finalCaseStatus"], "resolved");

        // Resolved cases are excluded unless includeResolvedCases.
        let default_list = svc
            .describe_cases(&req("DescribeCases", json!({})), &json!({}))
            .unwrap();
        assert_eq!(body_of(&default_list)["cases"].as_array().unwrap().len(), 0);
        let with_resolved = svc
            .describe_cases(
                &req("DescribeCases", json!({})),
                &json!({ "includeResolvedCases": true }),
            )
            .unwrap();
        assert_eq!(
            body_of(&with_resolved)["cases"].as_array().unwrap().len(),
            1
        );
    }

    #[test]
    fn unknown_case_is_case_id_not_found() {
        let svc = service();
        let err = expect_err(svc.describe_communications(
            &req("DescribeCommunications", json!({})),
            &json!({ "caseId": "case-nope" }),
        ));
        assert!(format!("{err:?}").contains("CaseIdNotFound"));
    }

    #[test]
    fn attachment_set_lifecycle() {
        let svc = service();
        let created = svc
            .add_attachments_to_set(
                &req("AddAttachmentsToSet", json!({})),
                &json!({ "attachments": [{ "fileName": "log.txt", "data": "aGVsbG8=" }] }),
            )
            .unwrap();
        let out = body_of(&created);
        let set_id = out["attachmentSetId"].as_str().unwrap().to_string();
        assert!(set_id.starts_with("as-"));
        assert!(out["expiryTime"].is_string());

        // Extend the same set.
        let extended = svc
            .add_attachments_to_set(
                &req("AddAttachmentsToSet", json!({})),
                &json!({ "attachmentSetId": set_id, "attachments": [{ "fileName": "b.txt", "data": "eA==" }] }),
            )
            .unwrap();
        assert_eq!(body_of(&extended)["attachmentSetId"], set_id);

        // Unknown set id is rejected.
        let err = expect_err(svc.add_attachments_to_set(
            &req("AddAttachmentsToSet", json!({})),
            &json!({ "attachmentSetId": "as-missing", "attachments": [] }),
        ));
        assert!(format!("{err:?}").contains("AttachmentSetIdNotFound"));
    }

    #[test]
    fn describe_attachment_unknown_id() {
        let svc = service();
        let err = expect_err(svc.describe_attachment(
            &req("DescribeAttachment", json!({})),
            &json!({ "attachmentId": "attachment-x" }),
        ));
        assert!(format!("{err:?}").contains("AttachmentIdNotFound"));
    }

    #[test]
    fn severity_levels_and_ta_catalogue() {
        let svc = service();
        let sev = body_of(&svc.describe_severity_levels(&json!({})).unwrap());
        assert_eq!(sev["severityLevels"].as_array().unwrap().len(), 5);
        let checks = body_of(&svc.describe_ta_checks(&json!({})).unwrap());
        assert!(!checks["checks"].as_array().unwrap().is_empty());
    }

    #[test]
    fn ta_refresh_state_machine() {
        let svc = service();
        // Enqueue.
        let enq = body_of(
            &svc.refresh_ta_check(
                &req("RefreshTrustedAdvisorCheck", json!({})),
                &json!({ "checkId": "Qch7DwouX1" }),
            )
            .unwrap(),
        );
        assert_eq!(enq["status"]["status"], "enqueued");
        // Each describe advances: enqueued -> processing -> success.
        let step1 = body_of(
            &svc.describe_ta_refresh_statuses(
                &req("DescribeTrustedAdvisorCheckRefreshStatuses", json!({})),
                &json!({ "checkIds": ["Qch7DwouX1"] }),
            )
            .unwrap(),
        );
        assert_eq!(step1["statuses"][0]["status"], "processing");
        let step2 = body_of(
            &svc.describe_ta_refresh_statuses(
                &req("DescribeTrustedAdvisorCheckRefreshStatuses", json!({})),
                &json!({ "checkIds": ["Qch7DwouX1"] }),
            )
            .unwrap(),
        );
        assert_eq!(step2["statuses"][0]["status"], "success");
    }

    #[test]
    fn describe_cases_paginates() {
        let svc = service();
        for _ in 0..3 {
            svc.create_case(
                &req("CreateCase", json!({})),
                &json!({ "subject": "s", "communicationBody": "b" }),
            )
            .unwrap();
        }
        let page1 = body_of(
            &svc.describe_cases(
                &req("DescribeCases", json!({})),
                &json!({ "maxResults": 2 }),
            )
            .unwrap(),
        );
        assert_eq!(page1["cases"].as_array().unwrap().len(), 2);
        let token = page1["nextToken"].as_str().unwrap();
        let page2 = body_of(
            &svc.describe_cases(
                &req("DescribeCases", json!({})),
                &json!({ "maxResults": 2, "nextToken": token }),
            )
            .unwrap(),
        );
        assert_eq!(page2["cases"].as_array().unwrap().len(), 1);
        assert!(page2.get("nextToken").is_none());
    }
}
