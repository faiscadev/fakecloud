//! Application (project) handlers: create/get/delete/list, application
//! settings, and the application-level KPI read.

use serde_json::{json, Map, Value};

use fakecloud_core::service::{AwsResponse, AwsServiceError};

use super::{copy_many, created, not_found_app, ok, paginate, str_field, Ctx, PinpointService};
use crate::shared;
use crate::state::App;

impl PinpointService {
    pub(super) fn create_app(
        &self,
        ctx: &Ctx,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = str_field(body, "Name");
        let id = shared::hex_id();
        let arn = shared::app_arn(&ctx.region, &ctx.account, &id);
        let mut record = Map::new();
        record.insert("Id".into(), json!(id));
        record.insert("Arn".into(), json!(arn));
        record.insert("Name".into(), json!(name));
        record.insert("CreationDate".into(), json!(shared::now_iso()));
        if let Some(tags) = body.get("tags") {
            if tags.is_object() {
                record.insert("tags".into(), tags.clone());
            }
        }
        let record = Value::Object(record);
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        let app = App {
            record: record.clone(),
            ..App::default()
        };
        data.apps.insert(id, app);
        created(record)
    }

    pub(super) fn get_app(&self, ctx: &Ctx, app_id: &str) -> Result<AwsResponse, AwsServiceError> {
        self.with_app(&ctx.account, app_id, |app| Ok(app.record.clone()))
            .and_then(ok)
    }

    pub(super) fn delete_app(
        &self,
        ctx: &Ctx,
        app_id: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        match data.apps.remove(app_id) {
            Some(app) => ok(app.record),
            None => Err(not_found_app(app_id)),
        }
    }

    pub(super) fn get_apps(
        &self,
        ctx: &Ctx,
        q: &[(String, String)],
    ) -> Result<AwsResponse, AwsServiceError> {
        let guard = self.state.read();
        let items: Vec<Value> = guard
            .get(&ctx.account)
            .map(|d| d.apps.values().map(|a| a.record.clone()).collect())
            .unwrap_or_default();
        let (page, next) = paginate(items, q)?;
        let mut out = Map::new();
        out.insert("Item".into(), json!(page));
        if let Some(n) = next {
            out.insert("NextToken".into(), json!(n));
        }
        ok(Value::Object(out))
    }

    pub(super) fn get_application_settings(
        &self,
        ctx: &Ctx,
        app_id: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        self.with_app(&ctx.account, app_id, |app| {
            Ok(settings_projection(app, app_id))
        })
        .and_then(ok)
    }

    pub(super) fn update_application_settings(
        &self,
        ctx: &Ctx,
        app_id: &str,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        let app = data
            .apps
            .get_mut(app_id)
            .ok_or_else(|| not_found_app(app_id))?;
        let mut settings = Map::new();
        settings.insert("ApplicationId".into(), json!(app_id));
        settings.insert("LastModifiedDate".into(), json!(shared::now_iso()));
        copy_many(
            &mut settings,
            body,
            &["CampaignHook", "Limits", "QuietTime", "JourneyLimits"],
        );
        app.settings = Value::Object(settings.clone());
        ok(Value::Object(settings))
    }

    pub(super) fn get_application_kpi(
        &self,
        ctx: &Ctx,
        app_id: &str,
        kpi_name: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        self.with_app(&ctx.account, app_id, |_| {
            Ok(json!({
                "ApplicationId": app_id,
                "KpiName": kpi_name,
                "StartTime": shared::now_iso(),
                "EndTime": shared::now_iso(),
                "KpiResult": { "Rows": [] },
            }))
        })
        .and_then(ok)
    }
}

/// Project an application's stored settings onto `ApplicationSettingsResource`,
/// always carrying the required `ApplicationId`.
fn settings_projection(app: &App, app_id: &str) -> Value {
    let mut out = Map::new();
    out.insert("ApplicationId".into(), json!(app_id));
    if let Some(obj) = app.settings.as_object() {
        for (k, v) in obj {
            if k != "ApplicationId" {
                out.insert(k.clone(), v.clone());
            }
        }
    }
    Value::Object(out)
}
