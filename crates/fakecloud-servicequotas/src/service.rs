//! Service Quotas (`servicequotas`) awsJson1.1 dispatch + operation handlers.
//!
//! All 26 operations of the Smithy model: AWS default and applied quota
//! lookups, quota increase requests and their history, the Organizations
//! quota request template and its association, tags on applied quotas,
//! automatic quota management and quota utilization reports.
//!
//! By default an increase request is decided as soon as it is made: it is
//! approved (and the applied value raised) unless it asks for more than AWS
//! allows for the quota, in which case it is `NOT_APPROVED` and the applied
//! value stays. Under [`RequestApproval::Manual`] it stays `PENDING` until the
//! introspection API approves or denies it. Services that enforce a quota read
//! the applied value through [`crate::ServiceQuotasProvider`], so an approved
//! increase changes what they accept.

use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use http::StatusCode;
use serde_json::{json, Map, Value};
use tokio::sync::Mutex as AsyncMutex;

use fakecloud_core::pagination::paginate_checked;
use fakecloud_core::quota::QuotaUsageSource;
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsService, AwsServiceError};
use fakecloud_organizations::SharedOrganizationsState;
use fakecloud_persistence::SnapshotStore;

use crate::catalog::{self, QuotaDef};
use crate::persistence::save_snapshot;
use crate::provider::{
    applied_value, apply_template_if_new, decide_submission, new_request_id, quota_arn,
    quota_region, requester,
};
use crate::settings::SharedQuotaSettings;
use crate::state::{
    template_key, AutoManagement, QuotaRequest, SharedServiceQuotasState, TemplateEntry,
    UtilizationEntry, UtilizationReport,
};
use crate::validate::{
    check_str, illegal_argument, opt_bool, opt_enum, opt_int, opt_plain_str, opt_str, req_double,
    req_enum, req_str, AMAZON_RESOURCE_NAME, AWS_REGION, EXCLUDED_SERVICE, NEXT_TOKEN, QUOTA_CODE,
    REQUEST_ID, SERVICE_CODE, TAG_KEY, TAG_VALUE,
};

/// Every operation name in the Service Quotas Smithy model.
pub const SERVICEQUOTAS_ACTIONS: &[&str] = &[
    "AssociateServiceQuotaTemplate",
    "CreateSupportCase",
    "DeleteServiceQuotaIncreaseRequestFromTemplate",
    "DisassociateServiceQuotaTemplate",
    "GetAWSDefaultServiceQuota",
    "GetAssociationForServiceQuotaTemplate",
    "GetAutoManagementConfiguration",
    "GetQuotaUtilizationReport",
    "GetRequestedServiceQuotaChange",
    "GetServiceQuota",
    "GetServiceQuotaIncreaseRequestFromTemplate",
    "ListAWSDefaultServiceQuotas",
    "ListRequestedServiceQuotaChangeHistory",
    "ListRequestedServiceQuotaChangeHistoryByQuota",
    "ListServiceQuotaIncreaseRequestsInTemplate",
    "ListServiceQuotas",
    "ListServices",
    "ListTagsForResource",
    "PutServiceQuotaIncreaseRequestIntoTemplate",
    "RequestServiceQuotaIncrease",
    "StartAutoManagement",
    "StartQuotaUtilizationReport",
    "StopAutoManagement",
    "TagResource",
    "UntagResource",
    "UpdateAutoManagement",
];

const REQUEST_STATUSES: &[&str] = &[
    "PENDING",
    "CASE_OPENED",
    "APPROVED",
    "DENIED",
    "CASE_CLOSED",
    "NOT_APPROVED",
    "INVALID_REQUEST",
];
const APPLIED_LEVELS: &[&str] = &["ACCOUNT", "RESOURCE", "ALL"];
const OPT_IN_LEVELS: &[&str] = &["ACCOUNT"];
const OPT_IN_TYPES: &[&str] = &["NotifyOnly", "NotifyAndAdjust"];

/// Quota request templates are only available in this region.
const TEMPLATE_REGION: &str = "us-east-1";
/// A quota request template holds at most this many requests.
const TEMPLATE_MAX_ENTRIES: usize = 10;
/// Tags one applied quota may carry.
const MAX_TAGS: usize = 50;
const SERVICE_PRINCIPAL: &str = "servicequotas.amazonaws.com";

pub struct ServiceQuotasService {
    state: SharedServiceQuotasState,
    orgs: SharedOrganizationsState,
    /// Server-wide enforcement and approval settings, shared with the
    /// [`crate::ServiceQuotasProvider`]. They come from the startup flags and
    /// last until a restart or reset; they are not persisted.
    settings: SharedQuotaSettings,
    usage_sources: Vec<Arc<dyn QuotaUsageSource>>,
    /// Persists Organizations state, which associating the template changes
    /// (it enables trusted access for Service Quotas).
    orgs_snapshot_hook: Option<fakecloud_persistence::SnapshotHook>,
    snapshot_store: Option<Arc<dyn SnapshotStore>>,
    snapshot_lock: Arc<AsyncMutex<()>>,
}

impl ServiceQuotasService {
    /// `settings` must be the ones the [`crate::ServiceQuotasProvider`] reads,
    /// so introspection changes reach the enforcing services.
    pub fn new(
        state: SharedServiceQuotasState,
        orgs: SharedOrganizationsState,
        settings: SharedQuotaSettings,
    ) -> Self {
        Self {
            state,
            orgs,
            settings,
            usage_sources: Vec::new(),
            orgs_snapshot_hook: None,
            snapshot_store: None,
            snapshot_lock: Arc::new(AsyncMutex::new(())),
        }
    }

    pub fn with_snapshot_store(mut self, store: Arc<dyn SnapshotStore>) -> Self {
        self.snapshot_store = Some(store);
        self
    }

    /// Add a source of quota usage for utilization reports.
    pub fn with_usage_source(mut self, source: Arc<dyn QuotaUsageSource>) -> Self {
        self.usage_sources.push(source);
        self
    }

    /// Persist Organizations state through `hook` after the template is
    /// associated.
    pub fn with_organizations_snapshot_hook(
        mut self,
        hook: Option<fakecloud_persistence::SnapshotHook>,
    ) -> Self {
        self.orgs_snapshot_hook = hook;
        self
    }

    /// Apply the organization's quota request template to every member
    /// account it is due for. AWS applies it when an account is created, so
    /// this runs on every Organizations membership change; the per-request
    /// check in [`AwsService::handle`] covers accounts restored from a
    /// snapshot.
    pub async fn apply_templates_to_org_members(&self) {
        let members: Vec<String> = self
            .orgs
            .read()
            .iter()
            .flat_map(|org| {
                org.accounts
                    .keys()
                    .filter(|id| **id != org.management_account_id)
                    .cloned()
                    .collect::<Vec<_>>()
            })
            .collect();
        let now = Utc::now();
        let approval = self.settings.read().request_approval;
        let mut changed = false;
        for account in &members {
            changed |= apply_template_if_new(&self.state, &self.orgs, approval, account, now);
        }
        if changed {
            self.save().await;
        }
    }

    pub fn snapshot_hook(&self) -> Option<fakecloud_persistence::SnapshotHook> {
        let store = self.snapshot_store.clone()?;
        let state = self.state.clone();
        let lock = self.snapshot_lock.clone();
        Some(Arc::new(move || {
            let state = state.clone();
            let store = store.clone();
            let lock = lock.clone();
            Box::pin(async move {
                save_snapshot(&state, Some(store), &lock).await;
            })
        }))
    }

    /// Persist the Service Quotas snapshot (a no-op in memory mode).
    pub async fn save(&self) {
        save_snapshot(
            &self.state,
            self.snapshot_store.clone(),
            &self.snapshot_lock,
        )
        .await;
    }
}

#[async_trait]
impl AwsService for ServiceQuotasService {
    fn service_name(&self) -> &str {
        "servicequotas"
    }

    async fn handle(&self, request: AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        // Applying the organization's template to a new account changes its
        // state, so it counts as a mutation even on a read.
        let approval = self.settings.read().request_approval;
        let template_applied = apply_template_if_new(
            &self.state,
            &self.orgs,
            approval,
            &request.account_id,
            Utc::now(),
        );

        let mutates = is_mutating(request.action.as_str());
        let result = dispatch(self, &request);
        let succeeded = matches!(result.as_ref(), Ok(resp) if resp.status.is_success());
        if succeeded && request.action == "AssociateServiceQuotaTemplate" {
            if let Some(hook) = &self.orgs_snapshot_hook {
                hook().await;
            }
        }
        if template_applied || (mutates && succeeded) {
            self.save().await;
        }
        result
    }

    fn supported_actions(&self) -> &[&str] {
        SERVICEQUOTAS_ACTIONS
    }
}

fn is_mutating(action: &str) -> bool {
    matches!(
        action,
        "AssociateServiceQuotaTemplate"
            | "CreateSupportCase"
            | "DeleteServiceQuotaIncreaseRequestFromTemplate"
            | "DisassociateServiceQuotaTemplate"
            | "PutServiceQuotaIncreaseRequestIntoTemplate"
            | "RequestServiceQuotaIncrease"
            | "StartAutoManagement"
            | "StartQuotaUtilizationReport"
            | "StopAutoManagement"
            | "TagResource"
            | "UntagResource"
            | "UpdateAutoManagement"
    )
}

fn dispatch(s: &ServiceQuotasService, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
    let b = parse(req)?;
    match req.action.as_str() {
        "ListServices" => s.list_services(&b),
        "GetAWSDefaultServiceQuota" => s.get_aws_default_service_quota(req, &b),
        "ListAWSDefaultServiceQuotas" => s.list_aws_default_service_quotas(req, &b),
        "GetServiceQuota" => s.get_service_quota(req, &b),
        "ListServiceQuotas" => s.list_service_quotas(req, &b),
        "RequestServiceQuotaIncrease" => s.request_service_quota_increase(req, &b),
        "GetRequestedServiceQuotaChange" => s.get_requested_service_quota_change(req, &b),
        "ListRequestedServiceQuotaChangeHistory" => s.list_history(req, &b, false),
        "ListRequestedServiceQuotaChangeHistoryByQuota" => s.list_history(req, &b, true),
        "CreateSupportCase" => s.create_support_case(req, &b),
        "AssociateServiceQuotaTemplate" => s.associate_template(req),
        "DisassociateServiceQuotaTemplate" => s.disassociate_template(req),
        "GetAssociationForServiceQuotaTemplate" => s.get_template_association(req),
        "PutServiceQuotaIncreaseRequestIntoTemplate" => s.put_template_entry(req, &b),
        "GetServiceQuotaIncreaseRequestFromTemplate" => s.get_template_entry(req, &b),
        "DeleteServiceQuotaIncreaseRequestFromTemplate" => s.delete_template_entry(req, &b),
        "ListServiceQuotaIncreaseRequestsInTemplate" => s.list_template_entries(req, &b),
        "TagResource" => s.tag_resource(req, &b),
        "UntagResource" => s.untag_resource(req, &b),
        "ListTagsForResource" => s.list_tags_for_resource(req, &b),
        "StartAutoManagement" => s.start_auto_management(req, &b),
        "GetAutoManagementConfiguration" => s.get_auto_management(req),
        "UpdateAutoManagement" => s.update_auto_management(req, &b),
        "StopAutoManagement" => s.stop_auto_management(req),
        "StartQuotaUtilizationReport" => s.start_utilization_report(req),
        "GetQuotaUtilizationReport" => s.get_utilization_report(req, &b),
        _ => Err(AwsServiceError::action_not_implemented(
            s.service_name(),
            &req.action,
        )),
    }
}

// ===== helpers =====

fn ok(v: Value) -> Result<AwsResponse, AwsServiceError> {
    Ok(AwsResponse::json_value(StatusCode::OK, v))
}

fn parse(req: &AwsRequest) -> Result<Value, AwsServiceError> {
    if req.body.is_empty() {
        return Ok(json!({}));
    }
    serde_json::from_slice(&req.body)
        .map_err(|e| illegal_argument(format!("Request body is malformed: {e}")))
}

fn err(status: StatusCode, code: &str, msg: impl Into<String>) -> AwsServiceError {
    AwsServiceError::aws_error(status, code, msg.into())
}

fn no_such_resource(msg: impl Into<String>) -> AwsServiceError {
    err(StatusCode::NOT_FOUND, "NoSuchResourceException", msg)
}

fn access_denied(msg: impl Into<String>) -> AwsServiceError {
    err(StatusCode::FORBIDDEN, "AccessDeniedException", msg)
}

fn epoch(t: &DateTime<Utc>) -> f64 {
    t.timestamp_millis() as f64 / 1000.0
}

/// `ServiceCode` that must name a service Service Quotas knows.
fn service_code(b: &Value) -> Result<&str, AwsServiceError> {
    let code = req_str(b, "ServiceCode", &SERVICE_CODE)?;
    if catalog::service(code).is_none() {
        return Err(no_such_resource(format!(
            "The request failed because the specified service {code} does not exist."
        )));
    }
    Ok(code)
}

fn find_quota(service_code: &str, quota_code: &str) -> Result<&'static QuotaDef, AwsServiceError> {
    catalog::quota(service_code, quota_code).ok_or_else(|| {
        no_such_resource(format!(
            "The request failed because the specified quota {quota_code} does not exist for \
             service {service_code}."
        ))
    })
}

/// `ServiceCode` + `QuotaCode` naming a known quota.
fn quota_from(b: &Value) -> Result<&'static QuotaDef, AwsServiceError> {
    let svc = req_str(b, "ServiceCode", &SERVICE_CODE)?;
    let code = req_str(b, "QuotaCode", &QUOTA_CODE)?;
    if catalog::service(svc).is_none() {
        return Err(no_such_resource(format!(
            "The request failed because the specified service {svc} does not exist."
        )));
    }
    find_quota(svc, code)
}

fn page<T: Clone>(
    items: &[T],
    b: &Value,
    max_limit: i64,
    default: usize,
) -> Result<(Vec<T>, Option<String>), AwsServiceError> {
    let token = opt_str(b, "NextToken", &NEXT_TOKEN)?;
    let max = opt_int(b, "MaxResults", 1, max_limit)?
        .map(|n| n as usize)
        .unwrap_or(default);
    paginate_checked(items, token, max).map_err(|_| {
        err(
            StatusCode::BAD_REQUEST,
            "InvalidPaginationTokenException",
            "Invalid pagination token.",
        )
    })
}

fn with_next_token(mut out: Map<String, Value>, token: Option<String>) -> Value {
    if let Some(t) = token {
        out.insert("NextToken".into(), Value::String(t));
    }
    Value::Object(out)
}

fn usage_metric_json(def: &QuotaDef) -> Option<Value> {
    let m = def.usage_metric?;
    let dims: Map<String, Value> = m
        .dimensions
        .iter()
        .map(|(k, v)| ((*k).to_string(), Value::String((*v).to_string())))
        .collect();
    Some(json!({
        "MetricNamespace": m.namespace,
        "MetricName": m.name,
        "MetricDimensions": dims,
        "MetricStatisticRecommendation": m.statistic,
    }))
}

/// A `ServiceQuota` structure. `account_id` is empty for an AWS default quota.
fn quota_json(region: &str, account_id: &str, def: &QuotaDef, value: f64) -> Value {
    let mut out = json!({
        "ServiceCode": def.service_code,
        "ServiceName": catalog::service_name(def.service_code),
        "QuotaArn": quota_arn(region, account_id, def),
        "QuotaCode": def.quota_code,
        "QuotaName": def.name,
        "Value": value,
        "Unit": def.unit,
        "Adjustable": def.adjustable,
        "GlobalQuota": def.global,
        "QuotaAppliedAtLevel": "ACCOUNT",
    });
    if let Some(m) = usage_metric_json(def) {
        out["UsageMetric"] = m;
    }
    if let Some(p) = def.period {
        out["Period"] = json!({ "PeriodValue": p.value, "PeriodUnit": p.unit });
    }
    if let Some(c) = def.context {
        let mut ctx = json!({ "ContextScope": c.scope });
        if let Some(t) = c.scope_type {
            ctx["ContextScopeType"] = Value::String(t.to_string());
        }
        out["QuotaContext"] = ctx;
    }
    if !def.description.is_empty() {
        out["Description"] = Value::String(def.description.to_string());
    }
    out
}

fn request_json(r: &QuotaRequest) -> Value {
    let def = catalog::quota(&r.service_code, &r.quota_code);
    let mut out = json!({
        "Id": r.id,
        "ServiceCode": r.service_code,
        "ServiceName": catalog::service_name(&r.service_code),
        "QuotaCode": r.quota_code,
        "QuotaName": def.map(|d| d.name).unwrap_or(""),
        "DesiredValue": r.desired_value,
        "Status": r.status,
        "Created": epoch(&r.created),
        "LastUpdated": epoch(&r.last_updated),
        "Requester": r.requester,
        "QuotaArn": r.quota_arn,
        "GlobalQuota": def.is_some_and(|d| d.global),
        "Unit": def.map(|d| d.unit).unwrap_or("None"),
        "QuotaRequestedAtLevel": "ACCOUNT",
    });
    if let Some(case) = &r.case_id {
        out["CaseId"] = Value::String(case.clone());
    }
    out
}

fn template_entry_json(e: &TemplateEntry) -> Value {
    let def = catalog::quota(&e.service_code, &e.quota_code);
    json!({
        "ServiceCode": e.service_code,
        "ServiceName": catalog::service_name(&e.service_code),
        "QuotaCode": e.quota_code,
        "QuotaName": def.map(|d| d.name).unwrap_or(""),
        "DesiredValue": e.desired_value,
        "AwsRegion": e.aws_region,
        "Unit": def.map(|d| d.unit).unwrap_or("None"),
        "GlobalQuota": def.is_some_and(|d| d.global),
    })
}

/// A `ContextId` names one resource. Account-only quotas never take one;
/// quotas AWS scopes to a resource could, but fakecloud keeps only their
/// account-level value, so it says so rather than misdescribing the quota.
fn context_id_refused(def: &QuotaDef) -> AwsServiceError {
    if def.context.is_some_and(|c| c.scope == "RESOURCE") {
        illegal_argument(format!(
            "The quota {} of service {} can be applied per resource, but fakecloud does not \
             support resource-level applied values; omit ContextId to use the account-level \
             value.",
            def.quota_code, def.service_code
        ))
    } else {
        illegal_argument(format!(
            "The quota {} is applied at the account level and does not take a ContextId.",
            def.quota_code
        ))
    }
}

/// `QuotaAppliedAtLevel` / `QuotaRequestedAtLevel` filter: every quota here
/// applies at the account level, so `RESOURCE` matches nothing.
fn level_matches(level: Option<&str>) -> bool {
    !matches!(level, Some("RESOURCE"))
}

/// Requests visible from `region`: the region's own plus global quotas'.
fn visible_in(r: &QuotaRequest, region: &str) -> bool {
    r.region.is_empty() || r.region == region
}

impl ServiceQuotasService {
    // ===== services and quotas =====

    fn list_services(&self, b: &Value) -> Result<AwsResponse, AwsServiceError> {
        let (items, token) = page(catalog::services(), b, 100, 100)?;
        let services: Vec<Value> = items
            .iter()
            .map(|s| json!({ "ServiceCode": s.code, "ServiceName": s.name }))
            .collect();
        let mut out = Map::new();
        out.insert("Services".into(), Value::Array(services));
        ok(with_next_token(out, token))
    }

    fn get_aws_default_service_quota(
        &self,
        req: &AwsRequest,
        b: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let def = quota_from(b)?;
        ok(
            json!({ "Quota": quota_json(&req.region, "", def, catalog::default_value(&req.region, def)) }),
        )
    }

    fn list_aws_default_service_quotas(
        &self,
        req: &AwsRequest,
        b: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let svc = service_code(b)?;
        let (items, token) = page(catalog::quotas_of(svc), b, 100, 100)?;
        let quotas: Vec<Value> = items
            .iter()
            .map(|d| quota_json(&req.region, "", d, catalog::default_value(&req.region, d)))
            .collect();
        let mut out = Map::new();
        out.insert("Quotas".into(), Value::Array(quotas));
        ok(with_next_token(out, token))
    }

    fn get_service_quota(
        &self,
        req: &AwsRequest,
        b: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let def = quota_from(b)?;
        if opt_plain_str(b, "ContextId")?.is_some() {
            return Err(context_id_refused(def));
        }
        let guard = self.state.read();
        let value = applied_value(guard.get(&req.account_id), &req.region, def);
        ok(json!({ "Quota": quota_json(&req.region, &req.account_id, def, value) }))
    }

    fn list_service_quotas(
        &self,
        req: &AwsRequest,
        b: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let svc = service_code(b)?;
        let quota_code = opt_str(b, "QuotaCode", &QUOTA_CODE)?;
        let level = opt_enum(b, "QuotaAppliedAtLevel", APPLIED_LEVELS)?;
        // A QuotaCode names at most one quota; RESOURCE-level applied values
        // do not exist here, so that filter matches nothing.
        let all: &[QuotaDef] = if !level_matches(level) {
            &[]
        } else if let Some(code) = quota_code {
            catalog::quota(svc, code)
                .map(std::slice::from_ref)
                .unwrap_or(&[])
        } else {
            catalog::quotas_of(svc)
        };
        let (items, token) = page(all, b, 100, 100)?;
        let guard = self.state.read();
        let data = guard.get(&req.account_id);
        let quotas: Vec<Value> = items
            .iter()
            .map(|d| {
                quota_json(
                    &req.region,
                    &req.account_id,
                    d,
                    applied_value(data, &req.region, d),
                )
            })
            .collect();
        let mut out = Map::new();
        out.insert("Quotas".into(), Value::Array(quotas));
        ok(with_next_token(out, token))
    }

    // ===== increase requests =====

    fn request_service_quota_increase(
        &self,
        req: &AwsRequest,
        b: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let def = quota_from(b)?;
        let desired = req_double(b, "DesiredValue", 0.0, 10_000_000_000.0)?;
        let context_id = opt_plain_str(b, "ContextId")?;
        opt_bool(b, "SupportCaseAllowed")?;
        if context_id.is_some() {
            return Err(context_id_refused(def));
        }
        if !def.adjustable {
            return Err(illegal_argument(format!(
                "The quota {} for service {} is not adjustable.",
                def.quota_code, def.service_code
            )));
        }

        let now = Utc::now();
        let caller = req
            .principal
            .as_ref()
            .map(|p| p.arn.clone())
            .unwrap_or_else(|| {
                fakecloud_aws::arn::Arn::global_in(&req.region, "iam", &req.account_id, "root")
                    .to_string()
            });
        let approval = self.settings.read().request_approval;
        let mut guard = self.state.write();
        let data = guard.get_or_create(&req.account_id);
        let current = applied_value(Some(data), &req.region, def);
        if desired <= current {
            return Err(illegal_argument(format!(
                "The requested value {desired} must be greater than the current value {current} \
                 of quota {}.",
                def.quota_code
            )));
        }
        let region = quota_region(&req.region, def);
        if data.requests.values().any(|r| {
            r.service_code == def.service_code
                && r.quota_code == def.quota_code
                && r.region == region
                && matches!(r.status.as_str(), "PENDING" | "CASE_OPENED")
        }) {
            return Err(err(
                StatusCode::BAD_REQUEST,
                "ResourceAlreadyExistsException",
                format!(
                    "A quota increase request for quota {} is already open.",
                    def.quota_code
                ),
            ));
        }

        let id = new_request_id();
        let mut request = QuotaRequest {
            id: id.clone(),
            region,
            service_code: def.service_code.to_string(),
            quota_code: def.quota_code.to_string(),
            desired_value: desired,
            status: "PENDING".to_string(),
            case_id: None,
            created: now,
            last_updated: now,
            requester: requester(&req.account_id, &caller),
            quota_arn: quota_arn(&req.region, &req.account_id, def),
        };
        // The request is returned as AWS returns it on submission (PENDING);
        // every later read sees the decision (see `decide_submission`).
        let response = request_json(&request);
        request.status = decide_submission(data, &req.region, def, desired, approval).to_string();
        data.requests.insert(id, request);
        ok(json!({ "RequestedQuota": response }))
    }

    fn get_requested_service_quota_change(
        &self,
        req: &AwsRequest,
        b: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let id = req_str(b, "RequestId", &REQUEST_ID)?;
        let guard = self.state.read();
        let r = guard
            .get(&req.account_id)
            .and_then(|d| d.requests.get(id))
            .ok_or_else(|| request_not_found(id))?;
        ok(json!({ "RequestedQuota": request_json(r) }))
    }

    fn list_history(
        &self,
        req: &AwsRequest,
        b: &Value,
        by_quota: bool,
    ) -> Result<AwsResponse, AwsServiceError> {
        let (svc, quota_code) = if by_quota {
            let def = quota_from(b)?;
            (Some(def.service_code), Some(def.quota_code))
        } else {
            match opt_str(b, "ServiceCode", &SERVICE_CODE)? {
                Some(_) => (Some(service_code(b)?), None),
                None => (None, None),
            }
        };
        let status = opt_enum(b, "Status", REQUEST_STATUSES)?;
        let level = opt_enum(b, "QuotaRequestedAtLevel", APPLIED_LEVELS)?;
        let guard = self.state.read();
        let mut all: Vec<&QuotaRequest> = guard
            .get(&req.account_id)
            .map(|d| d.requests.values().collect())
            .unwrap_or_default();
        all.retain(|r| {
            visible_in(r, &req.region)
                && svc.is_none_or(|s| r.service_code == s)
                && quota_code.is_none_or(|q| r.quota_code == q)
                && status.is_none_or(|s| r.status == s)
                && level_matches(level)
        });
        // Newest first; the id breaks ties between requests made in the same
        // instant so pages are stable.
        all.sort_by(|a, b| b.created.cmp(&a.created).then_with(|| a.id.cmp(&b.id)));
        let (items, token) = page(&all, b, 100, 100)?;
        let mut out = Map::new();
        out.insert(
            "RequestedQuotas".into(),
            Value::Array(items.iter().map(|r| request_json(r)).collect()),
        );
        ok(with_next_token(out, token))
    }

    fn create_support_case(
        &self,
        req: &AwsRequest,
        b: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let id = req_str(b, "RequestId", &REQUEST_ID)?;
        let mut guard = self.state.write();
        let r = guard
            .get_mut(&req.account_id)
            .and_then(|d| d.requests.get_mut(id))
            .ok_or_else(|| request_not_found(id))?;
        match r.status.as_str() {
            "PENDING" => {
                r.status = "CASE_OPENED".to_string();
                r.case_id = Some(support_case_id());
                r.last_updated = Utc::now();
                ok(json!({}))
            }
            "CASE_OPENED" => Err(err(
                StatusCode::BAD_REQUEST,
                "ResourceAlreadyExistsException",
                format!("A support case is already open for request {id}."),
            )),
            other => Err(err(
                StatusCode::METHOD_NOT_ALLOWED,
                "InvalidResourceStateException",
                format!(
                    "A support case can only be opened for a PENDING request; request {id} is \
                     {other}."
                ),
            )),
        }
    }

    // ===== quota request template =====

    /// The caller's organization, checked for template use: templates live in
    /// the management account and are only available in `us-east-1`.
    fn template_org(
        &self,
        req: &AwsRequest,
    ) -> Result<fakecloud_organizations::OrganizationState, AwsServiceError> {
        if req.region != TEMPLATE_REGION {
            return Err(err(
                StatusCode::NOT_FOUND,
                "TemplatesNotAvailableInRegionException",
                format!(
                    "The Service Quotas template is not available in this AWS Region. Use \
                     {TEMPLATE_REGION}."
                ),
            ));
        }
        let org = self
            .orgs
            .read()
            .org_of_account(&req.account_id)
            .cloned()
            .ok_or_else(|| {
                err(
                    StatusCode::FORBIDDEN,
                    "NoAvailableOrganizationException",
                    "The Amazon Web Services account making this call is not a member of an \
                     organization.",
                )
            })?;
        if org.management_account_id != req.account_id {
            return Err(access_denied(
                "Only the management account of an organization can manage the Service Quotas \
                 template.",
            ));
        }
        Ok(org)
    }

    fn associate_template(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let org = self.template_org(req)?;
        if org.feature_set != "ALL" {
            return Err(err(
                StatusCode::BAD_REQUEST,
                "OrganizationNotInAllFeaturesModeException",
                "The organization that your Amazon Web Services account belongs to is not in \
                 All Features mode.",
            ));
        }
        let now = Utc::now();
        // Associating the template turns on trusted access for Service Quotas
        // in the organization.
        if let Some(org) = self.orgs.write().org_by_id_mut(&org.org_id) {
            org.trusted_services
                .entry(SERVICE_PRINCIPAL.to_string())
                .or_insert(now);
        }
        let mut guard = self.state.write();
        let data = guard.get_or_create(&req.account_id);
        if data.template_associated_at.is_none() {
            data.template_associated_at = Some(now);
        }
        data.template_ever_associated = true;
        ok(json!({}))
    }

    fn disassociate_template(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        self.template_org(req)?;
        let mut guard = self.state.write();
        let data = guard.get_or_create(&req.account_id);
        if data.template_associated_at.take().is_none() {
            return Err(template_not_in_use());
        }
        ok(json!({}))
    }

    fn get_template_association(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        self.template_org(req)?;
        let guard = self.state.read();
        let data = guard.get(&req.account_id);
        if !data.is_some_and(|d| d.template_ever_associated) {
            return Err(template_not_in_use());
        }
        let status = if data.is_some_and(|d| d.template_associated_at.is_some()) {
            "ASSOCIATED"
        } else {
            "DISASSOCIATED"
        };
        ok(json!({ "ServiceQuotaTemplateAssociationStatus": status }))
    }

    fn put_template_entry(
        &self,
        req: &AwsRequest,
        b: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let def = quota_from(b)?;
        let aws_region = req_str(b, "AwsRegion", &AWS_REGION)?;
        let desired = req_double(b, "DesiredValue", 0.0, 10_000_000_000.0)?;
        self.template_org(req)?;
        if !def.adjustable {
            return Err(illegal_argument(format!(
                "The quota {} for service {} is not adjustable.",
                def.quota_code, def.service_code
            )));
        }
        let mut guard = self.state.write();
        let data = guard.get_or_create(&req.account_id);
        let key = template_key(def.service_code, def.quota_code, aws_region);
        if !data.template.contains_key(&key) && data.template.len() >= TEMPLATE_MAX_ENTRIES {
            return Err(err(
                StatusCode::CONFLICT,
                "QuotaExceededException",
                format!(
                    "The Service Quotas template can hold at most {TEMPLATE_MAX_ENTRIES} quota \
                     increase requests."
                ),
            ));
        }
        let entry = TemplateEntry {
            service_code: def.service_code.to_string(),
            quota_code: def.quota_code.to_string(),
            aws_region: aws_region.to_string(),
            desired_value: desired,
        };
        let out = template_entry_json(&entry);
        data.template.insert(key, entry);
        ok(json!({ "ServiceQuotaIncreaseRequestInTemplate": out }))
    }

    fn template_entry_key(b: &Value) -> Result<(&'static QuotaDef, String), AwsServiceError> {
        let def = quota_from(b)?;
        let aws_region = req_str(b, "AwsRegion", &AWS_REGION)?;
        Ok((
            def,
            template_key(def.service_code, def.quota_code, aws_region),
        ))
    }

    fn get_template_entry(
        &self,
        req: &AwsRequest,
        b: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let (def, key) = Self::template_entry_key(b)?;
        self.template_org(req)?;
        let guard = self.state.read();
        let entry = guard
            .get(&req.account_id)
            .and_then(|d| d.template.get(&key))
            .ok_or_else(|| template_entry_not_found(def))?;
        ok(json!({ "ServiceQuotaIncreaseRequestInTemplate": template_entry_json(entry) }))
    }

    fn delete_template_entry(
        &self,
        req: &AwsRequest,
        b: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let (def, key) = Self::template_entry_key(b)?;
        self.template_org(req)?;
        let mut guard = self.state.write();
        guard
            .get_mut(&req.account_id)
            .and_then(|d| d.template.remove(&key))
            .ok_or_else(|| template_entry_not_found(def))?;
        ok(json!({}))
    }

    fn list_template_entries(
        &self,
        req: &AwsRequest,
        b: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let svc = opt_str(b, "ServiceCode", &SERVICE_CODE)?;
        let aws_region = opt_str(b, "AwsRegion", &AWS_REGION)?;
        self.template_org(req)?;
        let guard = self.state.read();
        let all: Vec<&TemplateEntry> = guard
            .get(&req.account_id)
            .map(|d| {
                d.template
                    .values()
                    .filter(|e| svc.is_none_or(|s| e.service_code == s))
                    .filter(|e| aws_region.is_none_or(|r| e.aws_region == r))
                    .collect()
            })
            .unwrap_or_default();
        let (items, token) = page(&all, b, 100, 100)?;
        let mut out = Map::new();
        out.insert(
            "ServiceQuotaIncreaseRequestInTemplateList".into(),
            Value::Array(items.iter().map(|e| template_entry_json(e)).collect()),
        );
        ok(with_next_token(out, token))
    }

    // ===== tags =====

    /// Resolve a quota ARN the caller may tag: an applied quota of the
    /// caller's own account.
    fn tag_target(&self, req: &AwsRequest, b: &Value) -> Result<String, AwsServiceError> {
        let arn = req_str(b, "ResourceARN", &AMAZON_RESOURCE_NAME)?;
        let not_found =
            || no_such_resource(format!("The specified resource {arn} does not exist."));
        let rest = fakecloud_aws::arn::arn_resource(arn, "servicequotas").ok_or_else(not_found)?;
        // `<region>:<account>:<service>/<quota>`
        let mut parts = rest.splitn(3, ':');
        let region = parts.next().unwrap_or_default();
        let account = parts.next().unwrap_or_default();
        let resource = parts.next().unwrap_or_default();
        let (svc, code) = resource.split_once('/').ok_or_else(not_found)?;
        let def = catalog::quota(svc, code).ok_or_else(not_found)?;
        if account != req.account_id || (def.global != region.is_empty()) {
            return Err(not_found());
        }
        Ok(arn.to_string())
    }

    fn tag_resource(&self, req: &AwsRequest, b: &Value) -> Result<AwsResponse, AwsServiceError> {
        let arn = self.tag_target(req, b)?;
        let tags = match b.get("Tags") {
            Some(Value::Array(a)) => a,
            None | Some(Value::Null) => {
                return Err(illegal_argument(
                    "1 validation error detected: Value null at 'tags' failed to satisfy \
                     constraint: Member must not be null",
                ))
            }
            Some(_) => return Err(illegal_argument("Tags must be a list.")),
        };
        if tags.is_empty() {
            return Err(illegal_argument(
                "1 validation error detected: Value '[]' at 'tags' failed to satisfy \
                 constraint: Member must have length greater than or equal to 1",
            ));
        }
        let mut parsed = Vec::with_capacity(tags.len());
        for t in tags {
            let key = req_str(t, "Key", &TAG_KEY)?;
            let value = req_str(t, "Value", &TAG_VALUE)?;
            if key.starts_with("aws:") {
                return Err(illegal_argument(
                    "Tag keys starting with 'aws:' are reserved for AWS use.",
                ));
            }
            parsed.push((key.to_string(), value.to_string()));
        }
        let mut guard = self.state.write();
        let data = guard.get_or_create(&req.account_id);
        let mut merged = data.tags.get(&arn).cloned().unwrap_or_default();
        merged.extend(parsed);
        if merged.len() > MAX_TAGS {
            return Err(err(
                StatusCode::BAD_REQUEST,
                "TooManyTagsException",
                format!("A quota can have at most {MAX_TAGS} tags."),
            ));
        }
        data.tags.insert(arn, merged);
        ok(json!({}))
    }

    fn untag_resource(&self, req: &AwsRequest, b: &Value) -> Result<AwsResponse, AwsServiceError> {
        let arn = self.tag_target(req, b)?;
        let keys = match b.get("TagKeys") {
            Some(Value::Array(a)) => a,
            None | Some(Value::Null) => {
                return Err(illegal_argument(
                    "1 validation error detected: Value null at 'tagKeys' failed to satisfy \
                     constraint: Member must not be null",
                ))
            }
            Some(_) => return Err(illegal_argument("TagKeys must be a list.")),
        };
        let mut parsed = Vec::with_capacity(keys.len());
        for k in keys {
            let key = k
                .as_str()
                .ok_or_else(|| illegal_argument("TagKeys must be a list of strings."))?;
            check_str("TagKeys", key, &TAG_KEY)?;
            parsed.push(key);
        }
        let mut guard = self.state.write();
        let data = guard.get_or_create(&req.account_id);
        if let Some(tags) = data.tags.get_mut(&arn) {
            for k in parsed {
                tags.remove(k);
            }
            if tags.is_empty() {
                data.tags.remove(&arn);
            }
        }
        ok(json!({}))
    }

    fn list_tags_for_resource(
        &self,
        req: &AwsRequest,
        b: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let arn = self.tag_target(req, b)?;
        let guard = self.state.read();
        let tags: Vec<Value> = guard
            .get(&req.account_id)
            .and_then(|d| d.tags.get(&arn))
            .map(|t| {
                t.iter()
                    .map(|(k, v)| json!({ "Key": k, "Value": v }))
                    .collect()
            })
            .unwrap_or_default();
        ok(json!({ "Tags": tags }))
    }

    // ===== automatic management =====

    fn exclusion_list(b: &Value) -> Result<Option<BTreeMap<String, Vec<String>>>, AwsServiceError> {
        let map = match b.get("ExclusionList") {
            None | Some(Value::Null) => return Ok(None),
            Some(Value::Object(m)) => m,
            Some(_) => return Err(illegal_argument("ExclusionList must be a map.")),
        };
        let mut out = BTreeMap::new();
        for (svc, codes) in map {
            check_str("ExclusionList", svc, &EXCLUDED_SERVICE)?;
            if catalog::service(svc).is_none() {
                return Err(no_such_resource(format!(
                    "The request failed because the specified service {svc} does not exist."
                )));
            }
            let codes = codes
                .as_array()
                .ok_or_else(|| illegal_argument("ExclusionList values must be lists."))?;
            let mut list = Vec::with_capacity(codes.len());
            for c in codes {
                let code = c
                    .as_str()
                    .ok_or_else(|| illegal_argument("Excluded quota codes must be strings."))?;
                check_str("ExclusionList", code, &QUOTA_CODE)?;
                find_quota(svc, code)?;
                list.push(code.to_string());
            }
            out.insert(svc.clone(), list);
        }
        Ok(Some(out))
    }

    fn start_auto_management(
        &self,
        req: &AwsRequest,
        b: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let level = req_enum(b, "OptInLevel", OPT_IN_LEVELS)?;
        let opt_in_type = req_enum(b, "OptInType", OPT_IN_TYPES)?;
        let notification_arn = opt_str(b, "NotificationArn", &AMAZON_RESOURCE_NAME)?;
        let exclusions = Self::exclusion_list(b)?.unwrap_or_default();
        let mut guard = self.state.write();
        guard.get_or_create(&req.account_id).auto_management.insert(
            req.region.clone(),
            AutoManagement {
                opt_in_level: level.to_string(),
                opt_in_type: opt_in_type.to_string(),
                notification_arn: notification_arn.map(str::to_string),
                exclusion_list: exclusions,
                status: "ENABLED".to_string(),
            },
        );
        ok(json!({}))
    }

    fn get_auto_management(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let guard = self.state.read();
        let Some(cfg) = guard
            .get(&req.account_id)
            .and_then(|d| d.auto_management.get(&req.region))
        else {
            return ok(json!({ "OptInStatus": "DISABLED" }));
        };
        let exclusions: Map<String, Value> = cfg
            .exclusion_list
            .iter()
            .map(|(svc, codes)| {
                let infos: Vec<Value> = codes
                    .iter()
                    .map(|c| {
                        json!({
                            "QuotaCode": c,
                            "QuotaName": catalog::quota(svc, c).map(|d| d.name).unwrap_or(""),
                        })
                    })
                    .collect();
                (svc.clone(), Value::Array(infos))
            })
            .collect();
        let mut out = json!({
            "OptInLevel": cfg.opt_in_level,
            "OptInType": cfg.opt_in_type,
            "OptInStatus": cfg.status,
            "ExclusionList": exclusions,
        });
        if let Some(arn) = &cfg.notification_arn {
            out["NotificationArn"] = Value::String(arn.clone());
        }
        ok(out)
    }

    fn update_auto_management(
        &self,
        req: &AwsRequest,
        b: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let opt_in_type = opt_enum(b, "OptInType", OPT_IN_TYPES)?;
        let notification_arn = opt_str(b, "NotificationArn", &AMAZON_RESOURCE_NAME)?;
        let exclusions = Self::exclusion_list(b)?;
        let mut guard = self.state.write();
        let cfg = guard
            .get_mut(&req.account_id)
            .and_then(|d| d.auto_management.get_mut(&req.region))
            .filter(|c| c.status == "ENABLED")
            .ok_or_else(|| {
                no_such_resource("Automatic management is not enabled for this account.")
            })?;
        if let Some(t) = opt_in_type {
            cfg.opt_in_type = t.to_string();
        }
        if let Some(arn) = notification_arn {
            cfg.notification_arn = Some(arn.to_string());
        }
        if let Some(list) = exclusions {
            cfg.exclusion_list = list;
        }
        ok(json!({}))
    }

    fn stop_auto_management(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let mut guard = self.state.write();
        if let Some(cfg) = guard
            .get_mut(&req.account_id)
            .and_then(|d| d.auto_management.get_mut(&req.region))
        {
            cfg.status = "DISABLED".to_string();
        }
        ok(json!({}))
    }

    // ===== utilization reports =====

    fn start_utilization_report(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        // Usage is measured before the Service Quotas lock is taken: the
        // sources read other services' state, and those services resolve
        // applied values through this crate.
        let mut measured = Vec::new();
        for source in &self.usage_sources {
            for svc in source.service_codes() {
                for def in catalog::quotas_of(svc) {
                    if let Some(usage) =
                        source.usage(&req.account_id, &req.region, svc, def.quota_code)
                    {
                        measured.push((def, usage));
                    }
                }
            }
        }
        let report_id = uuid::Uuid::new_v4().to_string();
        let mut guard = self.state.write();
        let data = guard.get_or_create(&req.account_id);
        let quotas = measured
            .into_iter()
            .map(|(def, usage)| UtilizationEntry {
                service_code: def.service_code.to_string(),
                quota_code: def.quota_code.to_string(),
                usage,
                applied_value: applied_value(Some(data), &req.region, def),
            })
            .collect();
        data.reports.insert(
            report_id.clone(),
            UtilizationReport {
                report_id: report_id.clone(),
                region: req.region.clone(),
                generated_at: Utc::now(),
                quotas,
            },
        );
        ok(json!({ "ReportId": report_id, "Status": "PENDING" }))
    }

    fn get_utilization_report(
        &self,
        req: &AwsRequest,
        b: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let id = req_str(b, "ReportId", &REQUEST_ID)?;
        let guard = self.state.read();
        let report = guard
            .get(&req.account_id)
            .and_then(|d| d.reports.get(id))
            .filter(|r| r.region == req.region)
            .ok_or_else(|| no_such_resource(format!("The report {id} does not exist.")))?;
        let (items, token) = page(&report.quotas, b, 1000, 1000)?;
        let quotas: Vec<Value> = items
            .iter()
            .map(|e| {
                let def = catalog::quota(&e.service_code, &e.quota_code);
                let utilization = if e.applied_value > 0.0 {
                    e.usage / e.applied_value * 100.0
                } else {
                    0.0
                };
                json!({
                    "QuotaCode": e.quota_code,
                    "ServiceCode": e.service_code,
                    "QuotaName": def.map(|d| d.name).unwrap_or(""),
                    "Namespace": def
                        .and_then(|d| d.usage_metric)
                        .map(|m| m.namespace)
                        .unwrap_or("AWS/Usage"),
                    "Utilization": utilization,
                    "DefaultValue": def.map(|d| catalog::default_value(&req.region, d)).unwrap_or(0.0),
                    "AppliedValue": e.applied_value,
                    "ServiceName": catalog::service_name(&e.service_code),
                    "Adjustable": def.is_some_and(|d| d.adjustable),
                })
            })
            .collect();
        let mut out = Map::new();
        out.insert("ReportId".into(), json!(report.report_id));
        out.insert("Status".into(), json!("COMPLETED"));
        out.insert("GeneratedAt".into(), json!(epoch(&report.generated_at)));
        out.insert("TotalCount".into(), json!(report.quotas.len()));
        out.insert("Quotas".into(), Value::Array(quotas));
        ok(with_next_token(out, token))
    }
}

/// A support case id, the numeric display id AWS Support assigns.
fn support_case_id() -> String {
    use rand::Rng;
    rand::thread_rng()
        .gen_range(10_000_000_000u64..100_000_000_000u64)
        .to_string()
}

fn request_not_found(id: &str) -> AwsServiceError {
    no_such_resource(format!("The request {id} does not exist."))
}

fn template_not_in_use() -> AwsServiceError {
    err(
        StatusCode::BAD_REQUEST,
        "ServiceQuotaTemplateNotInUseException",
        "The quota request template is not associated with your organization.",
    )
}

fn template_entry_not_found(def: &QuotaDef) -> AwsServiceError {
    no_such_resource(format!(
        "The template has no quota increase request for quota {} of service {}.",
        def.quota_code, def.service_code
    ))
}

mod introspection;
pub use introspection::{
    Decision, IntrospectionError, OverrideChange, PutEnforcementRequest, PutQuotaRequest,
};

#[cfg(test)]
mod tests;
