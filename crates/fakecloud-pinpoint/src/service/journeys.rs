//! Journey handlers: create/get/update/delete/list, the `DRAFT` -> `ACTIVE`
//! state machine, and the (empty) run + execution-metric reads.

use serde_json::{json, Map, Value};

use fakecloud_core::service::{AwsResponse, AwsServiceError};

use super::{
    created, merge_body, not_found, not_found_app, ok, paginate, str_field, Ctx, PinpointService,
};
use crate::shared;

impl PinpointService {
    pub(super) fn create_journey(
        &self,
        ctx: &Ctx,
        app_id: &str,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let id = shared::hex_id();
        let record = build_journey(app_id, &id, "DRAFT", body);
        let mut guard = self.state.write();
        let app = guard
            .get_mut(&ctx.account)
            .and_then(|d| d.apps.get_mut(app_id))
            .ok_or_else(|| super::not_found_app(app_id))?;
        app.journeys.insert(id, record.clone());
        created(record)
    }

    pub(super) fn get_journey(
        &self,
        ctx: &Ctx,
        app_id: &str,
        jid: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        self.with_app(&ctx.account, app_id, |app| {
            app.journeys
                .get(jid)
                .cloned()
                .ok_or_else(|| not_found_journey(jid))
        })
        .and_then(ok)
    }

    pub(super) fn update_journey(
        &self,
        ctx: &Ctx,
        app_id: &str,
        jid: &str,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut guard = self.state.write();
        let app = guard
            .get_mut(&ctx.account)
            .and_then(|d| d.apps.get_mut(app_id))
            .ok_or_else(|| not_found_app(app_id))?;
        let existing = app
            .journeys
            .get(jid)
            .ok_or_else(|| not_found_journey(jid))?;
        let state = existing
            .get("State")
            .and_then(Value::as_str)
            .unwrap_or("DRAFT")
            .to_string();
        let record = build_journey(app_id, jid, &state, body);
        app.journeys.insert(jid.to_string(), record.clone());
        ok(record)
    }

    pub(super) fn update_journey_state(
        &self,
        ctx: &Ctx,
        app_id: &str,
        jid: &str,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let new_state = body
            .get("State")
            .and_then(Value::as_str)
            .unwrap_or("ACTIVE")
            .to_string();
        let mut guard = self.state.write();
        let app = guard
            .get_mut(&ctx.account)
            .and_then(|d| d.apps.get_mut(app_id))
            .ok_or_else(|| not_found_app(app_id))?;
        let journey = app
            .journeys
            .get_mut(jid)
            .ok_or_else(|| not_found_journey(jid))?;
        if let Some(obj) = journey.as_object_mut() {
            obj.insert("State".into(), json!(new_state));
            obj.insert("LastModifiedDate".into(), json!(shared::now_iso()));
        }
        ok(journey.clone())
    }

    pub(super) fn delete_journey(
        &self,
        ctx: &Ctx,
        app_id: &str,
        jid: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut guard = self.state.write();
        let app = guard
            .get_mut(&ctx.account)
            .and_then(|d| d.apps.get_mut(app_id))
            .ok_or_else(|| not_found_app(app_id))?;
        match app.journeys.remove(jid) {
            Some(rec) => ok(rec),
            None => Err(not_found_journey(jid)),
        }
    }

    pub(super) fn list_journeys(
        &self,
        ctx: &Ctx,
        app_id: &str,
        q: &[(String, String)],
    ) -> Result<AwsResponse, AwsServiceError> {
        let items = self.with_app(&ctx.account, app_id, |app| {
            Ok(app.journeys.values().cloned().collect::<Vec<_>>())
        })?;
        let (page, next) = paginate(items, q)?;
        let mut out = Map::new();
        out.insert("Item".into(), json!(page));
        if let Some(n) = next {
            out.insert("NextToken".into(), json!(n));
        }
        ok(Value::Object(out))
    }

    pub(super) fn get_journey_runs(
        &self,
        _ctx: &Ctx,
        _app_id: &str,
        _jid: &str,
        _q: &[(String, String)],
    ) -> Result<AwsResponse, AwsServiceError> {
        ok(json!({ "Item": [] }))
    }

    /// Error `NotFoundException` unless both the app and the journey exist — the
    /// KPI / execution-metric reads are journey-scoped, so AWS 404s on a missing
    /// app or journey rather than returning an empty metric envelope.
    fn require_journey(&self, ctx: &Ctx, app_id: &str, jid: &str) -> Result<(), AwsServiceError> {
        self.with_app(&ctx.account, app_id, |app| {
            app.journeys
                .get(jid)
                .map(|_| ())
                .ok_or_else(|| not_found_journey(jid))
        })
    }

    pub(super) fn get_journey_kpi(
        &self,
        ctx: &Ctx,
        app_id: &str,
        jid: &str,
        kpi_name: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        self.require_journey(ctx, app_id, jid)?;
        ok(json!({
            "ApplicationId": app_id,
            "JourneyId": jid,
            "KpiName": kpi_name,
            "StartTime": shared::now_iso(),
            "EndTime": shared::now_iso(),
            "KpiResult": { "Rows": [] },
        }))
    }

    pub(super) fn get_journey_execution_metrics(
        &self,
        ctx: &Ctx,
        app_id: &str,
        jid: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        self.require_journey(ctx, app_id, jid)?;
        ok(json!({
            "ApplicationId": app_id,
            "JourneyId": jid,
            "LastEvaluatedTime": shared::now_iso(),
            "Metrics": {},
        }))
    }

    pub(super) fn get_journey_execution_activity_metrics(
        &self,
        ctx: &Ctx,
        app_id: &str,
        jid: &str,
        activity_id: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        self.require_journey(ctx, app_id, jid)?;
        ok(json!({
            "ApplicationId": app_id,
            "JourneyId": jid,
            "JourneyActivityId": activity_id,
            "ActivityType": "MULTI_CONDITION",
            "LastEvaluatedTime": shared::now_iso(),
            "Metrics": {},
        }))
    }

    pub(super) fn get_journey_run_execution_metrics(
        &self,
        ctx: &Ctx,
        app_id: &str,
        jid: &str,
        run_id: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        self.require_journey(ctx, app_id, jid)?;
        ok(json!({
            "ApplicationId": app_id,
            "JourneyId": jid,
            "RunId": run_id,
            "LastEvaluatedTime": shared::now_iso(),
            "Metrics": {},
        }))
    }

    pub(super) fn get_journey_run_execution_activity_metrics(
        &self,
        ctx: &Ctx,
        app_id: &str,
        jid: &str,
        activity_id: &str,
        run_id: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        self.require_journey(ctx, app_id, jid)?;
        ok(json!({
            "ApplicationId": app_id,
            "JourneyId": jid,
            "JourneyActivityId": activity_id,
            "RunId": run_id,
            "ActivityType": "MULTI_CONDITION",
            "LastEvaluatedTime": shared::now_iso(),
            "Metrics": {},
        }))
    }
}

fn not_found_journey(jid: &str) -> AwsServiceError {
    not_found(&format!("Journey with id '{jid}' does not exist."))
}

fn build_journey(app_id: &str, id: &str, state: &str, body: &Value) -> Value {
    let now = shared::now_iso();
    let mut out = Map::new();
    // Persist the full WriteJourneyRequest definition (Activities /
    // StartCondition / StartActivity / Schedule / Limits / ...) so GetJourney
    // round-trips it, then overlay server-authoritative members.
    merge_body(&mut out, body);
    out.insert("Id".into(), json!(id));
    out.insert("ApplicationId".into(), json!(app_id));
    out.insert("Name".into(), json!(str_field(body, "Name")));
    out.insert("State".into(), json!(state));
    out.insert("CreationDate".into(), json!(now));
    out.insert("LastModifiedDate".into(), json!(now));
    Value::Object(out)
}
