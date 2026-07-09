//! Message-template handlers (email / push / sms / voice / inapp).
//!
//! Templates are a global, versioned resource family keyed by template name.
//! Each `Create`/`Update` appends a version; `UpdateTemplateActiveVersion` pins
//! the active one. Responses project the members common to every
//! `*TemplateResponse` shape so the projection is valid for any template type.

use serde_json::{json, Map, Value};

use fakecloud_core::service::{AwsResponse, AwsServiceError};

use super::{
    accepted, bad_request, copy_many, created, not_found, ok, paginate, query_one, Ctx,
    PinpointService,
};
use crate::shared;
use crate::state::Template;

/// Members common to every `*TemplateResponse` shape.
const TEMPLATE_SCALARS: &[&str] = &["TemplateDescription", "tags"];

/// The valid `TemplateType` enum values.
const TEMPLATE_TYPES: &[&str] = &["EMAIL", "SMS", "VOICE", "PUSH", "INAPP"];

impl PinpointService {
    pub(super) fn create_template(
        &self,
        ctx: &Ctx,
        name: &str,
        ttype: &str,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let arn = template_arn(&ctx.region, &ctx.account, name, ttype);
        let record = build_template(name, ttype, "1", &arn, body);
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        let entry = data.templates.entry(name.to_string()).or_default();
        entry.template_type = ttype.to_string();
        entry.versions = vec![record];
        entry.active_version = "1".to_string();
        created(json!({
            "Arn": arn,
            "Message": "Template created.",
            "RequestID": shared::hex_id(),
        }))
    }

    pub(super) fn get_template(
        &self,
        ctx: &Ctx,
        name: &str,
        ttype: &str,
        q: &[(String, String)],
    ) -> Result<AwsResponse, AwsServiceError> {
        let version = query_one(q, "Version");
        let guard = self.state.read();
        let tmpl = guard
            .get(&ctx.account)
            .and_then(|d| d.templates.get(name))
            .filter(|t| t.template_type == ttype)
            .ok_or_else(|| not_found_template(name))?;
        let record = select_version(tmpl, version)?;
        ok(record)
    }

    pub(super) fn update_template(
        &self,
        ctx: &Ctx,
        name: &str,
        ttype: &str,
        body: &Value,
        q: &[(String, String)],
    ) -> Result<AwsResponse, AwsServiceError> {
        let create_new = query_one(q, "CreateNewVersion")
            .map(|v| v.eq_ignore_ascii_case("true"))
            .unwrap_or(false);
        let arn = template_arn(&ctx.region, &ctx.account, name, ttype);
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        let tmpl = data
            .templates
            .get_mut(name)
            .filter(|t| t.template_type == ttype)
            .ok_or_else(|| not_found_template(name))?;
        if create_new {
            let next = tmpl.versions.len() + 1;
            let record = build_template(name, ttype, &next.to_string(), &arn, body);
            tmpl.versions.push(record);
            tmpl.active_version = next.to_string();
        } else if !tmpl.versions.is_empty() {
            let vnum = tmpl.versions.len().to_string();
            let record = build_template(name, ttype, &vnum, &arn, body);
            *tmpl.versions.last_mut().unwrap() = record;
        }
        accepted(json!({ "Message": "Template updated.", "RequestID": shared::hex_id() }))
    }

    pub(super) fn delete_template(
        &self,
        ctx: &Ctx,
        name: &str,
        ttype: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        let matches = data
            .templates
            .get(name)
            .map(|t| t.template_type == ttype)
            .unwrap_or(false);
        if !matches {
            return Err(not_found_template(name));
        }
        data.templates.remove(name);
        accepted(json!({ "Message": "Template deleted.", "RequestID": shared::hex_id() }))
    }

    pub(super) fn list_templates(
        &self,
        ctx: &Ctx,
        q: &[(String, String)],
    ) -> Result<AwsResponse, AwsServiceError> {
        if let Some(t) = query_one(q, "TemplateType") {
            if !TEMPLATE_TYPES.contains(&t) {
                return Err(bad_request("TemplateType is not a valid template type."));
            }
        }
        let filter_type = query_one(q, "TemplateType");
        let prefix = query_one(q, "Prefix");
        let guard = self.state.read();
        let items: Vec<Value> = guard
            .get(&ctx.account)
            .map(|d| {
                d.templates
                    .iter()
                    .filter(|(name, t)| {
                        filter_type.map(|ft| ft == t.template_type).unwrap_or(true)
                            && prefix.map(|p| name.starts_with(p)).unwrap_or(true)
                    })
                    .filter_map(|(_, t)| t.versions.last().cloned())
                    .collect()
            })
            .unwrap_or_default();
        let (page, next) = paginate(items, q)?;
        let mut out = Map::new();
        out.insert("Item".into(), json!(page));
        if let Some(n) = next {
            out.insert("NextToken".into(), json!(n));
        }
        ok(Value::Object(out))
    }

    pub(super) fn list_template_versions(
        &self,
        ctx: &Ctx,
        name: &str,
        _ttype: &str,
        q: &[(String, String)],
    ) -> Result<AwsResponse, AwsServiceError> {
        let guard = self.state.read();
        let tmpl = guard
            .get(&ctx.account)
            .and_then(|d| d.templates.get(name))
            .ok_or_else(|| not_found_template(name))?;
        let items: Vec<Value> = tmpl.versions.iter().rev().map(version_projection).collect();
        let (page, next) = paginate(items, q)?;
        let mut out = Map::new();
        out.insert("Item".into(), json!(page));
        if let Some(n) = next {
            out.insert("NextToken".into(), json!(n));
        }
        ok(Value::Object(out))
    }

    pub(super) fn update_template_active_version(
        &self,
        ctx: &Ctx,
        name: &str,
        ttype: &str,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let version = body.get("Version").and_then(Value::as_str).unwrap_or("1");
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        let tmpl = data
            .templates
            .get_mut(name)
            .filter(|t| t.template_type == ttype)
            .ok_or_else(|| not_found_template(name))?;
        tmpl.active_version = version.to_string();
        accepted(json!({ "Message": "Active version updated.", "RequestID": shared::hex_id() }))
    }
}

fn not_found_template(name: &str) -> AwsServiceError {
    not_found(&format!("Template '{name}' does not exist."))
}

fn template_arn(region: &str, account: &str, name: &str, ttype: &str) -> String {
    format!(
        "arn:aws:mobiletargeting:{region}:{account}:templates/{name}/{}",
        ttype
    )
}

/// Resolve a template version record: an explicit `Version` query, else the
/// latest.
fn select_version(tmpl: &Template, version: Option<&str>) -> Result<Value, AwsServiceError> {
    match version {
        Some(v) => tmpl
            .versions
            .iter()
            .find(|rec| rec.get("Version").and_then(Value::as_str) == Some(v))
            .cloned()
            .ok_or_else(|| not_found(&format!("Template version '{v}' does not exist."))),
        None => tmpl
            .versions
            .last()
            .cloned()
            .ok_or_else(|| not_found("Template has no versions.")),
    }
}

/// Build a template response record from the common `*TemplateResponse` members.
fn build_template(name: &str, ttype: &str, version: &str, arn: &str, body: &Value) -> Value {
    let now = shared::now_iso();
    let mut out = Map::new();
    out.insert("TemplateName".into(), json!(name));
    out.insert("TemplateType".into(), json!(ttype));
    out.insert("Arn".into(), json!(arn));
    out.insert("Version".into(), json!(version));
    out.insert("CreationDate".into(), json!(now));
    out.insert("LastModifiedDate".into(), json!(now));
    copy_many(&mut out, body, TEMPLATE_SCALARS);
    Value::Object(out)
}

/// Project a stored template record onto `TemplateVersionResponse` (no `Arn` /
/// `tags` members).
fn version_projection(rec: &Value) -> Value {
    const MEMBERS: &[&str] = &[
        "CreationDate",
        "DefaultSubstitutions",
        "LastModifiedDate",
        "TemplateDescription",
        "TemplateName",
        "TemplateType",
        "Version",
    ];
    let mut out = Map::new();
    copy_many(&mut out, rec, MEMBERS);
    Value::Object(out)
}
