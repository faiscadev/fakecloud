//! The `/_fakecloud/service-quotas/*` introspection API: read every quota's
//! applied value, usage and enforcement state; set applied values directly
//! (including below the AWS default, which AWS itself never allows, so a test
//! can hit a limit without creating the default number of resources); switch
//! enforcement on or off globally, per quota or per account; and decide
//! increase requests held `PENDING` under manual approval.
//!
//! The methods are synchronous and leave persistence to the caller, which
//! saves the snapshot after a successful change.

use std::collections::HashSet;

use chrono::Utc;
use http::StatusCode;
use serde::{Deserialize, Deserializer};
use serde_json::{json, Value};

use super::ServiceQuotasService;
use crate::catalog::{self, QuotaDef};
use crate::provider::{applied_value, approvable};
use crate::settings::{enforcement, quota_ref, QuotaSettings, RequestApproval};
use crate::state::{applied_key, QuotaRequest, ServiceQuotasData};

/// A rejected introspection call: the HTTP status and a message for the
/// `{"error": ...}` body.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IntrospectionError {
    pub status: StatusCode,
    pub message: String,
}

type Result<T> = std::result::Result<T, IntrospectionError>;

fn bad_request(message: impl Into<String>) -> IntrospectionError {
    IntrospectionError {
        status: StatusCode::BAD_REQUEST,
        message: message.into(),
    }
}

fn not_found(message: impl Into<String>) -> IntrospectionError {
    IntrospectionError {
        status: StatusCode::NOT_FOUND,
        message: message.into(),
    }
}

/// Distinguishes an absent field (`None`) from an explicit `null`
/// (`Some(None)`).
fn explicit<'de, D: Deserializer<'de>>(
    d: D,
) -> std::result::Result<Option<Option<bool>>, D::Error> {
    Option::<bool>::deserialize(d).map(Some)
}

/// Body of `PUT /_fakecloud/service-quotas/quotas/{service}/{quota}`.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct PutQuotaRequest {
    /// The account whose applied value is set (default: the server's
    /// account). When given, `enforce` is an override for this account only;
    /// otherwise it applies to every account.
    #[serde(default)]
    pub account_id: Option<String>,
    /// The region of a regional quota (default: the server's region).
    #[serde(default)]
    pub region: Option<String>,
    /// The applied value to set. May be below the AWS default.
    #[serde(default)]
    pub value: Option<f64>,
    /// `true` enforces the quota, `false` ignores it, `null` clears the
    /// override; absent leaves enforcement as it is.
    #[serde(default, deserialize_with = "explicit")]
    pub enforce: Option<Option<bool>>,
}

/// One entry of `PUT /_fakecloud/service-quotas/enforcement`'s `overrides`.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct OverrideChange {
    pub service_code: String,
    pub quota_code: String,
    /// Scope the override to one account; absent applies to every account.
    #[serde(default)]
    pub account_id: Option<String>,
    /// `true` enforces, `false` ignores, `null` or absent clears.
    #[serde(default)]
    pub enforce: Option<bool>,
}

/// Body of `PUT /_fakecloud/service-quotas/enforcement`.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct PutEnforcementRequest {
    #[serde(default)]
    pub enforce_all: Option<bool>,
    #[serde(default)]
    pub overrides: Vec<OverrideChange>,
}

/// How an introspection call decides a pending request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Decision {
    Approve,
    /// Close the request without raising the quota, with this status.
    Deny(&'static str),
}

/// The terminal statuses a denial may leave a request in.
const DENY_STATUSES: &[&str] = &["DENIED", "NOT_APPROVED", "CASE_CLOSED", "INVALID_REQUEST"];

impl Decision {
    /// A denial with `status` (default `DENIED`).
    pub fn deny(status: Option<&str>) -> Result<Self> {
        let status = status.unwrap_or("DENIED");
        DENY_STATUSES
            .iter()
            .find(|s| **s == status)
            .map(|s| Self::Deny(s))
            .ok_or_else(|| {
                bad_request(format!(
                    "status must be one of {}, got {status:?}",
                    DENY_STATUSES.join(", ")
                ))
            })
    }
}

fn check_account(account_id: &str) -> Result<()> {
    if account_id.len() == 12 && account_id.bytes().all(|b| b.is_ascii_digit()) {
        Ok(())
    } else {
        Err(bad_request(format!(
            "accountId must be a 12-digit AWS account id, got {account_id:?}"
        )))
    }
}

fn check_region(region: &str) -> Result<()> {
    let ok = !region.is_empty()
        && region
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-');
    if ok {
        Ok(())
    } else {
        Err(bad_request(format!("invalid region {region:?}")))
    }
}

fn find_def(service_code: &str, quota_code: &str) -> Result<&'static QuotaDef> {
    catalog::quota(service_code, quota_code).ok_or_else(|| {
        not_found(format!(
            "unknown quota {service_code}/{quota_code}; GET /_fakecloud/service-quotas/quotas lists them"
        ))
    })
}

/// Only quotas a fakecloud service checks take an override: switching another
/// on would silently do nothing, and ignoring it is already the case.
fn check_enforce(def: &QuotaDef, enforce: Option<bool>) -> Result<()> {
    if enforce.is_some() && !def.enforceable {
        return Err(bad_request(format!(
            "quota {} ({}) is not enforceable: no fakecloud service checks requests against it",
            quota_ref(def),
            def.name
        )));
    }
    Ok(())
}

fn set_override(
    overrides: &mut std::collections::BTreeMap<String, bool>,
    def: &QuotaDef,
    enforce: Option<bool>,
) {
    match enforce {
        Some(on) => overrides.insert(quota_ref(def), on),
        None => overrides.remove(&quota_ref(def)),
    };
}

fn request_view(account_id: &str, r: &QuotaRequest) -> Value {
    json!({
        "accountId": account_id,
        "requestId": r.id,
        "serviceCode": r.service_code,
        "quotaCode": r.quota_code,
        "quotaName": catalog::quota(&r.service_code, &r.quota_code).map(|d| d.name).unwrap_or(""),
        "region": r.region,
        "desiredValue": r.desired_value,
        "status": r.status,
        "caseId": r.case_id,
        "created": r.created.to_rfc3339(),
        "lastUpdated": r.last_updated.to_rfc3339(),
    })
}

impl ServiceQuotasService {
    /// Usage of `def` for the account, from the service that counts it.
    fn measured_usage(&self, account_id: &str, region: &str, def: &QuotaDef) -> Option<f64> {
        self.usage_sources
            .iter()
            .filter(|s| s.service_codes().contains(&def.service_code))
            .find_map(|s| s.usage(account_id, region, def.service_code, def.quota_code))
    }

    fn quota_view(
        settings: &QuotaSettings,
        data: Option<&ServiceQuotasData>,
        region: &str,
        def: &QuotaDef,
        usage: Option<f64>,
    ) -> Value {
        let (enforced, source) = enforcement(settings, data, region, def);
        json!({
            "serviceCode": def.service_code,
            "quotaCode": def.quota_code,
            "quotaName": def.name,
            "global": def.global,
            "adjustable": def.adjustable,
            "unit": def.unit,
            "defaultValue": catalog::default_value(region, def),
            "appliedValue": applied_value(data, region, def),
            "usage": usage,
            "enforceable": def.enforceable,
            "enforced": enforced,
            "enforcementSource": source.as_str(),
        })
    }

    fn one_quota_view(&self, account_id: &str, region: &str, def: &QuotaDef) -> Value {
        // Usage is measured before the Service Quotas locks are taken: the
        // sources read other services' state, and those services resolve
        // limits through this crate.
        let usage = self.measured_usage(account_id, region, def);
        let settings = self.settings.read();
        let guard = self.state.read();
        Self::quota_view(&settings, guard.get(account_id), region, def, usage)
    }

    fn account_or_default(&self, account_id: Option<&str>) -> Result<String> {
        match account_id {
            Some(a) => check_account(a).map(|_| a.to_string()),
            None => Ok(self.state.read().default_account_id().to_string()),
        }
    }

    fn region_or_default(&self, region: Option<&str>) -> Result<String> {
        match region {
            Some(r) => check_region(r).map(|_| r.to_string()),
            None => Ok(self.state.read().region().to_string()),
        }
    }

    /// `GET /_fakecloud/service-quotas/quotas`: quotas with their applied
    /// value, usage and enforcement state for an account and region. With a
    /// service code, every quota of that service; without one, only the
    /// quotas worth looking at across the whole catalog: those a fakecloud
    /// service can enforce, those a usage source measures, and those the
    /// account (or the server) changed: an applied value in any region, an
    /// enforcement override, or an open increase request.
    pub fn introspect_quotas(
        &self,
        account_id: Option<&str>,
        region: Option<&str>,
        service_code: Option<&str>,
    ) -> Result<Value> {
        let account = self.account_or_default(account_id)?;
        let region = self.region_or_default(region)?;
        if let Some(code) = service_code {
            if catalog::service(code).is_none() {
                return Err(not_found(format!("unknown service code {code:?}")));
            }
        }
        let defs: &[QuotaDef] = match service_code {
            Some(code) => catalog::quotas_of(code),
            None => catalog::quotas(),
        };
        let usage: Vec<Option<f64>> = defs
            .iter()
            .map(|d| self.measured_usage(&account, &region, d))
            .collect();
        let settings = self.settings.read();
        let guard = self.state.read();
        let data = guard.get(&account);
        // Quotas the account or server changed, collected once so the
        // catalog-wide filter allocates nothing per quota.
        let mut changed: HashSet<(&str, &str)> = HashSet::new();
        if service_code.is_none() {
            changed.extend(settings.overrides.keys().filter_map(|k| k.split_once('/')));
            if let Some(data) = data {
                changed.extend(data.enforcement.keys().filter_map(|k| k.split_once('/')));
                changed.extend(data.applied.keys().filter_map(|k| {
                    let mut parts = k.splitn(3, '|');
                    let (_region, svc, code) = (parts.next()?, parts.next()?, parts.next()?);
                    Some((svc, code))
                }));
                changed.extend(
                    data.requests
                        .values()
                        .filter(|r| matches!(r.status.as_str(), "PENDING" | "CASE_OPENED"))
                        .map(|r| (r.service_code.as_str(), r.quota_code.as_str())),
                );
            }
        }
        let interesting = |d: &QuotaDef, usage: Option<f64>| {
            d.enforceable || usage.is_some() || changed.contains(&(d.service_code, d.quota_code))
        };
        let quotas: Vec<Value> = defs
            .iter()
            .zip(usage)
            .filter(|(d, u)| service_code.is_some() || interesting(d, *u))
            .map(|(d, u)| Self::quota_view(&settings, data, &region, d, u))
            .collect();
        Ok(json!({ "accountId": account, "region": region, "quotas": quotas }))
    }

    /// `PUT /_fakecloud/service-quotas/quotas/{service}/{quota}`: set the
    /// applied value and/or the enforcement override of one quota.
    pub fn introspect_put_quota(
        &self,
        service_code: &str,
        quota_code: &str,
        body: &PutQuotaRequest,
    ) -> Result<Value> {
        let def = find_def(service_code, quota_code)?;
        if body.value.is_none() && body.enforce.is_none() {
            return Err(bad_request("nothing to change: give value and/or enforce"));
        }
        if let Some(v) = body.value {
            if !v.is_finite() || v < 0.0 {
                return Err(bad_request(format!(
                    "value must be a non-negative number, got {v}"
                )));
            }
        }
        check_enforce(def, body.enforce.flatten())?;
        let account = self.account_or_default(body.account_id.as_deref())?;
        let region = self.region_or_default(body.region.as_deref())?;
        {
            let mut settings = self.settings.write();
            let mut guard = self.state.write();
            let data = guard.get_or_create(&account);
            if let Some(v) = body.value {
                data.applied.insert(
                    applied_key(&region, def.global, def.service_code, def.quota_code),
                    v,
                );
            }
            if let Some(enforce) = body.enforce {
                if body.account_id.is_some() {
                    set_override(&mut data.enforcement, def, enforce);
                } else {
                    set_override(&mut settings.overrides, def, enforce);
                }
            }
        }
        Ok(self.one_quota_view(&account, &region, def))
    }

    /// `DELETE /_fakecloud/service-quotas/quotas/{service}/{quota}`: put the
    /// quota back to its AWS default and drop its enforcement override (the
    /// account's when `account_id` is given, else the server-wide one).
    pub fn introspect_delete_quota(
        &self,
        service_code: &str,
        quota_code: &str,
        account_id: Option<&str>,
        region: Option<&str>,
    ) -> Result<Value> {
        let def = find_def(service_code, quota_code)?;
        let account = self.account_or_default(account_id)?;
        let region = self.region_or_default(region)?;
        {
            let mut settings = self.settings.write();
            let mut guard = self.state.write();
            if let Some(data) = guard.get_mut(&account) {
                data.applied.remove(&applied_key(
                    &region,
                    def.global,
                    def.service_code,
                    def.quota_code,
                ));
                if account_id.is_some() {
                    set_override(&mut data.enforcement, def, None);
                }
            }
            if account_id.is_none() {
                set_override(&mut settings.overrides, def, None);
            }
        }
        Ok(self.one_quota_view(&account, &region, def))
    }

    /// `GET /_fakecloud/service-quotas/enforcement`.
    pub fn introspect_enforcement(&self) -> Value {
        let settings = self.settings.read();
        let split = |key: &str| {
            let (s, q) = key.split_once('/').unwrap_or((key, ""));
            (s.to_string(), q.to_string())
        };
        let overrides: Vec<Value> = settings
            .overrides
            .iter()
            .map(|(k, on)| {
                let (s, q) = split(k);
                json!({ "serviceCode": s, "quotaCode": q, "enforce": on })
            })
            .collect();
        let guard = self.state.read();
        let account_overrides: Vec<Value> = guard
            .iter()
            .flat_map(|(account, data)| {
                data.enforcement.iter().map(move |(k, on)| {
                    let (s, q) = split(k);
                    json!({
                        "accountId": account,
                        "serviceCode": s,
                        "quotaCode": q,
                        "enforce": on,
                    })
                })
            })
            .collect();
        json!({
            "enforceAll": settings.enforce_all,
            "overrides": overrides,
            "accountOverrides": account_overrides,
        })
    }

    /// `PUT /_fakecloud/service-quotas/enforcement`: change the global switch
    /// and/or a batch of overrides. Every change is validated before any is
    /// applied.
    pub fn introspect_put_enforcement(&self, body: &PutEnforcementRequest) -> Result<Value> {
        let mut changes = Vec::with_capacity(body.overrides.len());
        for o in &body.overrides {
            let def = find_def(&o.service_code, &o.quota_code)?;
            check_enforce(def, o.enforce)?;
            if let Some(a) = &o.account_id {
                check_account(a)?;
            }
            changes.push((def, o.account_id.as_deref(), o.enforce));
        }
        {
            let mut settings = self.settings.write();
            let mut guard = self.state.write();
            if let Some(on) = body.enforce_all {
                settings.enforce_all = on;
            }
            for (def, account, enforce) in changes {
                match account {
                    Some(a) => set_override(&mut guard.get_or_create(a).enforcement, def, enforce),
                    None => set_override(&mut settings.overrides, def, enforce),
                }
            }
        }
        Ok(self.introspect_enforcement())
    }

    /// `GET /_fakecloud/service-quotas/request-approval`.
    pub fn introspect_request_approval(&self) -> Value {
        json!({ "mode": self.settings.read().request_approval.as_str() })
    }

    /// `PUT /_fakecloud/service-quotas/request-approval`.
    pub fn introspect_set_request_approval(&self, mode: &str) -> Result<Value> {
        let mode = RequestApproval::parse(mode)
            .ok_or_else(|| bad_request(format!("mode must be auto or manual, got {mode:?}")))?;
        self.settings.write().request_approval = mode;
        Ok(self.introspect_request_approval())
    }

    /// `GET /_fakecloud/service-quotas/requests`: increase requests across
    /// accounts (or one), newest first, optionally filtered by status.
    pub fn introspect_requests(
        &self,
        account_id: Option<&str>,
        status: Option<&str>,
    ) -> Result<Value> {
        if let Some(a) = account_id {
            check_account(a)?;
        }
        let guard = self.state.read();
        let mut out: Vec<(&str, &QuotaRequest)> = guard
            .iter()
            .filter(|(a, _)| account_id.is_none_or(|want| *a == want))
            .flat_map(|(a, d)| d.requests.values().map(move |r| (a, r)))
            .filter(|(_, r)| status.is_none_or(|s| r.status == s))
            .collect();
        out.sort_by(|a, b| {
            b.1.created
                .cmp(&a.1.created)
                .then_with(|| a.1.id.cmp(&b.1.id))
        });
        let requests: Vec<Value> = out.iter().map(|(a, r)| request_view(a, r)).collect();
        Ok(json!({ "requests": requests }))
    }

    /// `POST /_fakecloud/service-quotas/requests/{id}/approve|deny`: decide a
    /// `PENDING` or `CASE_OPENED` request. Approving raises the account's
    /// applied value to the requested one.
    pub fn introspect_decide_request(&self, request_id: &str, decision: Decision) -> Result<Value> {
        let mut guard = self.state.write();
        let account = guard
            .iter()
            .find(|(_, d)| d.requests.contains_key(request_id))
            .map(|(a, _)| a.to_string())
            .ok_or_else(|| not_found(format!("no quota request {request_id}")))?;
        let data = guard.get_or_create(&account);
        let request = data
            .requests
            .get(request_id)
            .cloned()
            .expect("request was found in this account");
        if !matches!(request.status.as_str(), "PENDING" | "CASE_OPENED") {
            return Err(IntrospectionError {
                status: StatusCode::CONFLICT,
                message: format!(
                    "quota request {request_id} is already decided ({})",
                    request.status
                ),
            });
        }
        let status = match decision {
            Decision::Approve => {
                // An approved increase raises the quota to the requested value;
                // it never lowers one set higher in the meantime. AWS's own
                // limits (a documented maximum, the security-group product)
                // still hold: values may have changed since submission.
                if let Some(def) = catalog::quota(&request.service_code, &request.quota_code) {
                    let current = applied_value(Some(data), &request.region, def);
                    if request.desired_value > current
                        && !approvable(data, &request.region, def, request.desired_value)
                    {
                        return Err(IntrospectionError {
                            status: StatusCode::CONFLICT,
                            message: format!(
                                "quota request {request_id} asks for {} which AWS would not \
                                 approve for {} (past its maximum or the security-group \
                                 product limit); deny it instead",
                                request.desired_value,
                                quota_ref(def)
                            ),
                        });
                    }
                    data.applied.insert(
                        applied_key(
                            &request.region,
                            def.global,
                            def.service_code,
                            def.quota_code,
                        ),
                        current.max(request.desired_value),
                    );
                }
                "APPROVED"
            }
            Decision::Deny(status) => status,
        };
        let stored = data
            .requests
            .get_mut(request_id)
            .expect("request was found in this account");
        stored.status = status.to_string();
        stored.last_updated = Utc::now();
        Ok(request_view(&account, stored))
    }
}
