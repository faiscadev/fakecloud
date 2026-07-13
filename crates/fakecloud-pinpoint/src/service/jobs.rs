//! Import / export job handlers.
//!
//! Jobs mint an id and are stored settled to `COMPLETED` (fakecloud does not
//! read or write S3, so there is no real processing to observe), and the read
//! path projects the exact `ImportJobResponse` / `ExportJobResponse` shape.

use serde_json::{json, Map, Value};

use fakecloud_core::service::{AwsResponse, AwsServiceError};

use super::{copy_many, created, not_found, ok, paginate, str_field, Ctx, PinpointService};
use crate::shared;

impl PinpointService {
    pub(super) fn create_import_job(
        &self,
        ctx: &Ctx,
        app_id: &str,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let id = shared::hex_id();
        let record = build_import_job(app_id, &id, body);
        let mut guard = self.state.write();
        let app = guard
            .get_mut(&ctx.account)
            .and_then(|d| d.apps.get_mut(app_id))
            .ok_or_else(|| super::not_found_app(app_id))?;
        app.import_jobs.insert(id, record.clone());
        created(record)
    }

    pub(super) fn get_import_job(
        &self,
        ctx: &Ctx,
        app_id: &str,
        jid: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        self.with_app(&ctx.account, app_id, |app| {
            app.import_jobs
                .get(jid)
                .cloned()
                .ok_or_else(|| not_found_job(jid))
        })
        .and_then(ok)
    }

    pub(super) fn get_import_jobs(
        &self,
        ctx: &Ctx,
        app_id: &str,
        q: &[(String, String)],
    ) -> Result<AwsResponse, AwsServiceError> {
        let items = self.with_app(&ctx.account, app_id, |app| {
            Ok(app.import_jobs.values().cloned().collect::<Vec<_>>())
        })?;
        jobs_page(items, q)
    }

    pub(super) fn create_export_job(
        &self,
        ctx: &Ctx,
        app_id: &str,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let id = shared::hex_id();
        let record = build_export_job(app_id, &id, body);
        let mut guard = self.state.write();
        let app = guard
            .get_mut(&ctx.account)
            .and_then(|d| d.apps.get_mut(app_id))
            .ok_or_else(|| super::not_found_app(app_id))?;
        app.export_jobs.insert(id, record.clone());
        created(record)
    }

    pub(super) fn get_export_job(
        &self,
        ctx: &Ctx,
        app_id: &str,
        jid: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        self.with_app(&ctx.account, app_id, |app| {
            app.export_jobs
                .get(jid)
                .cloned()
                .ok_or_else(|| not_found_job(jid))
        })
        .and_then(ok)
    }

    pub(super) fn get_export_jobs(
        &self,
        ctx: &Ctx,
        app_id: &str,
        q: &[(String, String)],
    ) -> Result<AwsResponse, AwsServiceError> {
        let items = self.with_app(&ctx.account, app_id, |app| {
            Ok(app.export_jobs.values().cloned().collect::<Vec<_>>())
        })?;
        jobs_page(items, q)
    }
}

fn not_found_job(jid: &str) -> AwsServiceError {
    not_found(&format!("Job with id '{jid}' does not exist."))
}

fn jobs_page(items: Vec<Value>, q: &[(String, String)]) -> Result<AwsResponse, AwsServiceError> {
    let (page, next) = paginate(items, q)?;
    let mut out = Map::new();
    out.insert("Item".into(), json!(page));
    if let Some(n) = next {
        out.insert("NextToken".into(), json!(n));
    }
    ok(Value::Object(out))
}

fn build_import_job(app_id: &str, id: &str, body: &Value) -> Value {
    let mut definition = Map::new();
    definition.insert("Format".into(), json!(str_field(body, "Format")));
    definition.insert("RoleArn".into(), json!(str_field(body, "RoleArn")));
    definition.insert("S3Url".into(), json!(str_field(body, "S3Url")));
    copy_many(
        &mut definition,
        body,
        &[
            "DefineSegment",
            "ExternalId",
            "RegisterEndpoints",
            "SegmentId",
            "SegmentName",
        ],
    );
    json!({
        "ApplicationId": app_id,
        "Id": id,
        "CreationDate": shared::now_iso(),
        "Type": "IMPORT",
        "JobStatus": "COMPLETED",
        "Definition": Value::Object(definition),
    })
}

fn build_export_job(app_id: &str, id: &str, body: &Value) -> Value {
    let mut definition = Map::new();
    definition.insert("RoleArn".into(), json!(str_field(body, "RoleArn")));
    definition.insert("S3UrlPrefix".into(), json!(str_field(body, "S3UrlPrefix")));
    copy_many(&mut definition, body, &["SegmentId", "SegmentVersion"]);
    json!({
        "ApplicationId": app_id,
        "Id": id,
        "CreationDate": shared::now_iso(),
        "Type": "EXPORT",
        "JobStatus": "COMPLETED",
        "Definition": Value::Object(definition),
    })
}
