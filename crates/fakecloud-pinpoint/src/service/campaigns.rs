//! Campaign handlers: versioned create/get/update/delete/list plus the
//! activities and date-range-KPI reads.

use serde_json::{json, Map, Value};

use fakecloud_core::service::{AwsResponse, AwsServiceError};

use super::{copy_many, created, not_found, ok, paginate, Ctx, PinpointService};
use crate::shared;
use crate::state::Versioned;

/// Scalar `CampaignResponse` members that are safe to echo from the write
/// request (no nested structures, so response shape validation can't trip).
const CAMPAIGN_SCALARS: &[&str] = &[
    "Name",
    "Description",
    "HoldoutPercent",
    "IsPaused",
    "TreatmentName",
    "TreatmentDescription",
    "Priority",
    "tags",
];

impl PinpointService {
    pub(super) fn create_campaign(
        &self,
        ctx: &Ctx,
        app_id: &str,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let id = shared::hex_id();
        let mut guard = self.state.write();
        let app = guard
            .get_mut(&ctx.account)
            .and_then(|d| d.apps.get_mut(app_id))
            .ok_or_else(|| super::not_found_app(app_id))?;
        let record = build_campaign(ctx, app_id, &id, 1, body);
        app.campaigns.insert(
            id,
            Versioned {
                current: record.clone(),
                versions: vec![record.clone()],
            },
        );
        created(record)
    }

    pub(super) fn get_campaign(
        &self,
        ctx: &Ctx,
        app_id: &str,
        cid: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        self.with_app(&ctx.account, app_id, |app| {
            app.campaigns
                .get(cid)
                .map(|v| v.current.clone())
                .ok_or_else(|| not_found_campaign(cid))
        })
        .and_then(ok)
    }

    pub(super) fn update_campaign(
        &self,
        ctx: &Ctx,
        app_id: &str,
        cid: &str,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut guard = self.state.write();
        let app = guard
            .get_mut(&ctx.account)
            .and_then(|d| d.apps.get_mut(app_id))
            .ok_or_else(|| super::not_found_app(app_id))?;
        let camp = app
            .campaigns
            .get_mut(cid)
            .ok_or_else(|| not_found_campaign(cid))?;
        let version = camp.versions.len() as i64 + 1;
        let record = build_campaign(ctx, app_id, cid, version, body);
        camp.versions.push(record.clone());
        camp.current = record.clone();
        ok(record)
    }

    pub(super) fn delete_campaign(
        &self,
        ctx: &Ctx,
        app_id: &str,
        cid: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut guard = self.state.write();
        let app = guard
            .get_mut(&ctx.account)
            .and_then(|d| d.apps.get_mut(app_id))
            .ok_or_else(|| super::not_found_app(app_id))?;
        match app.campaigns.remove(cid) {
            Some(v) => ok(v.current),
            None => Err(not_found_campaign(cid)),
        }
    }

    pub(super) fn get_campaigns(
        &self,
        ctx: &Ctx,
        app_id: &str,
        q: &[(String, String)],
    ) -> Result<AwsResponse, AwsServiceError> {
        let items = self.with_app(&ctx.account, app_id, |app| {
            Ok(app
                .campaigns
                .values()
                .map(|v| v.current.clone())
                .collect::<Vec<_>>())
        })?;
        campaigns_page(items, q)
    }

    pub(super) fn get_campaign_versions(
        &self,
        ctx: &Ctx,
        app_id: &str,
        cid: &str,
        q: &[(String, String)],
    ) -> Result<AwsResponse, AwsServiceError> {
        let items = self.with_app(&ctx.account, app_id, |app| {
            app.campaigns
                .get(cid)
                .map(|v| v.versions.iter().rev().cloned().collect::<Vec<_>>())
                .ok_or_else(|| not_found_campaign(cid))
        })?;
        campaigns_page(items, q)
    }

    pub(super) fn get_campaign_version(
        &self,
        ctx: &Ctx,
        app_id: &str,
        cid: &str,
        ver: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        self.with_app(&ctx.account, app_id, |app| {
            let camp = app
                .campaigns
                .get(cid)
                .ok_or_else(|| not_found_campaign(cid))?;
            let n: usize = ver
                .parse()
                .map_err(|_| not_found(&format!("Campaign version '{ver}' does not exist.")))?;
            camp.versions
                .get(n.wrapping_sub(1))
                .cloned()
                .ok_or_else(|| not_found(&format!("Campaign version '{ver}' does not exist.")))
        })
        .and_then(ok)
    }

    pub(super) fn get_campaign_activities(
        &self,
        ctx: &Ctx,
        app_id: &str,
        cid: &str,
        _q: &[(String, String)],
    ) -> Result<AwsResponse, AwsServiceError> {
        self.with_app(&ctx.account, app_id, |app| {
            if app.campaigns.contains_key(cid) {
                Ok(json!({ "Item": [] }))
            } else {
                Err(not_found_campaign(cid))
            }
        })
        .and_then(ok)
    }

    pub(super) fn get_campaign_kpi(
        &self,
        ctx: &Ctx,
        app_id: &str,
        cid: &str,
        kpi_name: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        self.with_app(&ctx.account, app_id, |_| {
            Ok(json!({
                "ApplicationId": app_id,
                "CampaignId": cid,
                "KpiName": kpi_name,
                "StartTime": shared::now_iso(),
                "EndTime": shared::now_iso(),
                "KpiResult": { "Rows": [] },
            }))
        })
        .and_then(ok)
    }
}

fn not_found_campaign(cid: &str) -> AwsServiceError {
    not_found(&format!("Campaign with id '{cid}' does not exist."))
}

fn campaigns_page(
    items: Vec<Value>,
    q: &[(String, String)],
) -> Result<AwsResponse, AwsServiceError> {
    let (page, next) = paginate(items, q)?;
    let mut out = Map::new();
    out.insert("Item".into(), json!(page));
    if let Some(n) = next {
        out.insert("NextToken".into(), json!(n));
    }
    ok(Value::Object(out))
}

/// Build a `CampaignResponse` projection carrying every required member plus the
/// scalar optionals echoed from the write request.
fn build_campaign(ctx: &Ctx, app_id: &str, id: &str, version: i64, body: &Value) -> Value {
    let now = shared::now_iso();
    let mut out = Map::new();
    out.insert("Id".into(), json!(id));
    out.insert("ApplicationId".into(), json!(app_id));
    out.insert(
        "Arn".into(),
        json!(shared::nested_arn(
            &ctx.region,
            &ctx.account,
            app_id,
            "campaigns",
            id
        )),
    );
    out.insert("CreationDate".into(), json!(now));
    out.insert("LastModifiedDate".into(), json!(now));
    out.insert(
        "SegmentId".into(),
        json!(body.get("SegmentId").and_then(Value::as_str).unwrap_or("")),
    );
    out.insert(
        "SegmentVersion".into(),
        json!(body
            .get("SegmentVersion")
            .and_then(Value::as_i64)
            .unwrap_or(0)),
    );
    out.insert("Version".into(), json!(version));
    copy_many(&mut out, body, CAMPAIGN_SCALARS);
    Value::Object(out)
}
