//! Handlers for CloudFront Field-Level Encryption (configs + profiles)
//! and Realtime Log Configs.

use chrono::Utc;
use http::{HeaderMap, StatusCode};

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use crate::fle::{
    CreateRealtimeLogConfigRequest, FieldLevelEncryptionConfig, FieldLevelEncryptionProfileConfig,
    GetOrDeleteRealtimeLogConfigRequest, StoredFieldLevelEncryption,
    StoredFieldLevelEncryptionProfile, StoredRealtimeLogConfig, UpdateRealtimeLogConfigRequest,
};
use crate::policies::{
    not_found, precondition_failed, require_if_match, rfc3339, route_id, xml_with_etag,
};
use crate::router::Route;
use crate::service::{
    aws_error, esc, generate_id_with_prefix, invalid_argument, xml_response, CloudFrontService,
};
use crate::xml_io;

const NS: &str = crate::NAMESPACE;
const XML_DECL: &str = r#"<?xml version="1.0" encoding="UTF-8"?>"#;

// ─── Field-Level Encryption Config ────────────────────────────────────

impl CloudFrontService {
    pub(crate) fn create_field_level_encryption_config(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let cfg: FieldLevelEncryptionConfig = xml_io::from_xml_root(&req.body).map_err(|e| {
            invalid_argument(format!("invalid FieldLevelEncryptionConfig XML: {e}"))
        })?;
        if cfg.caller_reference.is_empty() {
            return Err(invalid_argument("CallerReference is required"));
        }
        let mut state = self.state.write();
        let account = state.accounts.entry(req.account_id.clone()).or_default();
        if let Some(existing) = account
            .field_level_encryptions
            .values()
            .find(|f| f.config.caller_reference == cfg.caller_reference)
        {
            return Err(aws_error(
                StatusCode::CONFLICT,
                "FieldLevelEncryptionConfigAlreadyExists",
                format!(
                    "FieldLevelEncryption with same CallerReference exists: {}",
                    existing.id
                ),
            ));
        }
        let id = generate_id_with_prefix("F");
        let etag = generate_id_with_prefix("E");
        let stored = StoredFieldLevelEncryption {
            id: id.clone(),
            etag: etag.clone(),
            last_modified_time: Utc::now(),
            config: cfg,
        };
        account
            .field_level_encryptions
            .insert(id.clone(), stored.clone());
        drop(state);
        let body = render_fle(&stored);
        Ok(xml_with_etag(StatusCode::CREATED, body, &etag, Some(&id)))
    }

    pub(crate) fn get_field_level_encryption(
        &self,
        route: &Route,
    ) -> Result<AwsResponse, AwsServiceError> {
        let id = route_id(route, "FieldLevelEncryption")?;
        let state = self.state.read();
        let f = state
            .accounts
            .get(&route.account)
            .and_then(|a| a.field_level_encryptions.get(&id).cloned())
            .ok_or_else(|| not_found("FieldLevelEncryption", &id))?;
        drop(state);
        let body = render_fle(&f);
        Ok(xml_with_etag(StatusCode::OK, body, &f.etag, None))
    }

    pub(crate) fn get_field_level_encryption_config(
        &self,
        route: &Route,
    ) -> Result<AwsResponse, AwsServiceError> {
        let id = route_id(route, "FieldLevelEncryption")?;
        let state = self.state.read();
        let f = state
            .accounts
            .get(&route.account)
            .and_then(|a| a.field_level_encryptions.get(&id).cloned())
            .ok_or_else(|| not_found("FieldLevelEncryption", &id))?;
        drop(state);
        let body = render_fle_config(&f.config);
        Ok(xml_with_etag(StatusCode::OK, body, &f.etag, None))
    }

    pub(crate) fn update_field_level_encryption_config(
        &self,
        req: &AwsRequest,
        route: &Route,
    ) -> Result<AwsResponse, AwsServiceError> {
        let id = route_id(route, "FieldLevelEncryption")?;
        let if_match = require_if_match(req)?;
        let cfg: FieldLevelEncryptionConfig = xml_io::from_xml_root(&req.body).map_err(|e| {
            invalid_argument(format!("invalid FieldLevelEncryptionConfig XML: {e}"))
        })?;
        let mut state = self.state.write();
        let account = state
            .accounts
            .get_mut(&req.account_id)
            .ok_or_else(|| not_found("FieldLevelEncryption", &id))?;
        let f = account
            .field_level_encryptions
            .get_mut(&id)
            .ok_or_else(|| not_found("FieldLevelEncryption", &id))?;
        if f.etag != if_match {
            return Err(precondition_failed());
        }
        if f.config.caller_reference != cfg.caller_reference {
            return Err(invalid_argument(
                "CallerReference cannot change on UpdateFieldLevelEncryptionConfig",
            ));
        }
        f.config = cfg;
        f.etag = generate_id_with_prefix("E");
        f.last_modified_time = Utc::now();
        let snap = f.clone();
        drop(state);
        let body = render_fle(&snap);
        Ok(xml_with_etag(StatusCode::OK, body, &snap.etag, None))
    }

    pub(crate) fn delete_field_level_encryption_config(
        &self,
        req: &AwsRequest,
        route: &Route,
    ) -> Result<AwsResponse, AwsServiceError> {
        let id = route_id(route, "FieldLevelEncryption")?;
        let if_match = require_if_match(req)?;
        let mut state = self.state.write();
        let account = state
            .accounts
            .get_mut(&req.account_id)
            .ok_or_else(|| not_found("FieldLevelEncryption", &id))?;
        let f = account
            .field_level_encryptions
            .get(&id)
            .ok_or_else(|| not_found("FieldLevelEncryption", &id))?;
        if f.etag != if_match {
            return Err(precondition_failed());
        }
        // Model-declared; checked after the If-Match (inferred).
        crate::validate::refuse_in_use(
            account,
            crate::validate::Reference::FieldLevelEncryption(&id),
        )?;
        account.field_level_encryptions.remove(&id);
        drop(state);
        Ok(crate::policies::empty(StatusCode::NO_CONTENT))
    }

    pub(crate) fn list_field_level_encryption_configs(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let state = self.state.read();
        let mut items: Vec<StoredFieldLevelEncryption> = state
            .accounts
            .get(&req.account_id)
            .map(|a| a.field_level_encryptions.values().cloned().collect())
            .unwrap_or_default();
        drop(state);
        items.sort_by(|a, b| a.id.cmp(&b.id));

        let mut body = String::with_capacity(512);
        body.push_str(XML_DECL);
        body.push_str(&format!("<FieldLevelEncryptionList xmlns=\"{NS}\">"));
        body.push_str("<NextMarker></NextMarker>");
        body.push_str("<MaxItems>100</MaxItems>");
        body.push_str(&format!("<Quantity>{}</Quantity>", items.len()));
        body.push_str("<Items>");
        for f in &items {
            body.push_str("<FieldLevelEncryptionSummary>");
            body.push_str(&format!("<Id>{}</Id>", esc(&f.id)));
            body.push_str(&format!(
                "<LastModifiedTime>{}</LastModifiedTime>",
                rfc3339(&f.last_modified_time)
            ));
            if let Some(c) = &f.config.comment {
                body.push_str(&format!("<Comment>{}</Comment>", esc(c)));
            }
            if let Some(q) = &f.config.query_arg_profile_config {
                body.push_str(&render_query_arg_profile_config(q));
            }
            if let Some(c) = &f.config.content_type_profile_config {
                body.push_str(&render_content_type_profile_config(c));
            }
            body.push_str("</FieldLevelEncryptionSummary>");
        }
        body.push_str("</Items>");
        body.push_str("</FieldLevelEncryptionList>");
        Ok(xml_response(StatusCode::OK, body, HeaderMap::new()))
    }
}

// ─── Field-Level Encryption Profile ───────────────────────────────────

impl CloudFrontService {
    pub(crate) fn create_field_level_encryption_profile(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let cfg: FieldLevelEncryptionProfileConfig =
            xml_io::from_xml_root(&req.body).map_err(|e| {
                invalid_argument(format!(
                    "invalid FieldLevelEncryptionProfileConfig XML: {e}"
                ))
            })?;
        if cfg.name.is_empty() {
            return Err(invalid_argument(
                "FieldLevelEncryptionProfileConfig.Name is required",
            ));
        }
        if cfg.caller_reference.is_empty() {
            return Err(invalid_argument("CallerReference is required"));
        }
        let mut state = self.state.write();
        let account = state.accounts.entry(req.account_id.clone()).or_default();
        if let Some(existing) = account
            .field_level_encryption_profiles
            .values()
            .find(|p| p.config.caller_reference == cfg.caller_reference)
        {
            return Err(aws_error(
                StatusCode::CONFLICT,
                "FieldLevelEncryptionProfileAlreadyExists",
                format!(
                    "FieldLevelEncryptionProfile with same CallerReference exists: {}",
                    existing.id
                ),
            ));
        }
        let id = generate_id_with_prefix("FP");
        let etag = generate_id_with_prefix("E");
        let stored = StoredFieldLevelEncryptionProfile {
            id: id.clone(),
            etag: etag.clone(),
            last_modified_time: Utc::now(),
            config: cfg,
        };
        account
            .field_level_encryption_profiles
            .insert(id.clone(), stored.clone());
        drop(state);
        let body = render_fle_profile(&stored);
        Ok(xml_with_etag(StatusCode::CREATED, body, &etag, Some(&id)))
    }

    pub(crate) fn get_field_level_encryption_profile(
        &self,
        route: &Route,
    ) -> Result<AwsResponse, AwsServiceError> {
        let id = route_id(route, "FieldLevelEncryptionProfile")?;
        let state = self.state.read();
        let p = state
            .accounts
            .get(&route.account)
            .and_then(|a| a.field_level_encryption_profiles.get(&id).cloned())
            .ok_or_else(|| not_found("FieldLevelEncryptionProfile", &id))?;
        drop(state);
        let body = render_fle_profile(&p);
        Ok(xml_with_etag(StatusCode::OK, body, &p.etag, None))
    }

    pub(crate) fn get_field_level_encryption_profile_config(
        &self,
        route: &Route,
    ) -> Result<AwsResponse, AwsServiceError> {
        let id = route_id(route, "FieldLevelEncryptionProfile")?;
        let state = self.state.read();
        let p = state
            .accounts
            .get(&route.account)
            .and_then(|a| a.field_level_encryption_profiles.get(&id).cloned())
            .ok_or_else(|| not_found("FieldLevelEncryptionProfile", &id))?;
        drop(state);
        let body = render_fle_profile_config(&p.config);
        Ok(xml_with_etag(StatusCode::OK, body, &p.etag, None))
    }

    pub(crate) fn update_field_level_encryption_profile(
        &self,
        req: &AwsRequest,
        route: &Route,
    ) -> Result<AwsResponse, AwsServiceError> {
        let id = route_id(route, "FieldLevelEncryptionProfile")?;
        let if_match = require_if_match(req)?;
        let cfg: FieldLevelEncryptionProfileConfig =
            xml_io::from_xml_root(&req.body).map_err(|e| {
                invalid_argument(format!(
                    "invalid FieldLevelEncryptionProfileConfig XML: {e}"
                ))
            })?;
        if cfg.name.is_empty() {
            return Err(invalid_argument(
                "FieldLevelEncryptionProfileConfig.Name is required",
            ));
        }
        let mut state = self.state.write();
        let account = state
            .accounts
            .get_mut(&req.account_id)
            .ok_or_else(|| not_found("FieldLevelEncryptionProfile", &id))?;
        let p = account
            .field_level_encryption_profiles
            .get_mut(&id)
            .ok_or_else(|| not_found("FieldLevelEncryptionProfile", &id))?;
        if p.etag != if_match {
            return Err(precondition_failed());
        }
        if p.config.caller_reference != cfg.caller_reference {
            return Err(invalid_argument(
                "CallerReference cannot change on UpdateFieldLevelEncryptionProfile",
            ));
        }
        p.config = cfg;
        p.etag = generate_id_with_prefix("E");
        p.last_modified_time = Utc::now();
        let snap = p.clone();
        drop(state);
        let body = render_fle_profile(&snap);
        Ok(xml_with_etag(StatusCode::OK, body, &snap.etag, None))
    }

    pub(crate) fn delete_field_level_encryption_profile(
        &self,
        req: &AwsRequest,
        route: &Route,
    ) -> Result<AwsResponse, AwsServiceError> {
        let id = route_id(route, "FieldLevelEncryptionProfile")?;
        let if_match = require_if_match(req)?;
        let mut state = self.state.write();
        let account = state
            .accounts
            .get_mut(&req.account_id)
            .ok_or_else(|| not_found("FieldLevelEncryptionProfile", &id))?;
        let p = account
            .field_level_encryption_profiles
            .get(&id)
            .ok_or_else(|| not_found("FieldLevelEncryptionProfile", &id))?;
        if p.etag != if_match {
            return Err(precondition_failed());
        }
        // Model-declared; checked after the If-Match (inferred).
        crate::validate::refuse_in_use(
            account,
            crate::validate::Reference::FieldLevelEncryptionProfile(&id),
        )?;
        account.field_level_encryption_profiles.remove(&id);
        drop(state);
        Ok(crate::policies::empty(StatusCode::NO_CONTENT))
    }

    pub(crate) fn list_field_level_encryption_profiles(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let state = self.state.read();
        let mut items: Vec<StoredFieldLevelEncryptionProfile> = state
            .accounts
            .get(&req.account_id)
            .map(|a| {
                a.field_level_encryption_profiles
                    .values()
                    .cloned()
                    .collect()
            })
            .unwrap_or_default();
        drop(state);
        items.sort_by(|a, b| a.config.name.cmp(&b.config.name));

        let mut body = String::with_capacity(512);
        body.push_str(XML_DECL);
        body.push_str(&format!("<FieldLevelEncryptionProfileList xmlns=\"{NS}\">"));
        body.push_str("<NextMarker></NextMarker>");
        body.push_str("<MaxItems>100</MaxItems>");
        body.push_str(&format!("<Quantity>{}</Quantity>", items.len()));
        body.push_str("<Items>");
        for p in &items {
            body.push_str("<FieldLevelEncryptionProfileSummary>");
            body.push_str(&format!("<Id>{}</Id>", esc(&p.id)));
            body.push_str(&format!(
                "<LastModifiedTime>{}</LastModifiedTime>",
                rfc3339(&p.last_modified_time)
            ));
            body.push_str(&format!("<Name>{}</Name>", esc(&p.config.name)));
            body.push_str(&render_encryption_entities(&p.config.encryption_entities));
            if let Some(c) = &p.config.comment {
                body.push_str(&format!("<Comment>{}</Comment>", esc(c)));
            }
            body.push_str("</FieldLevelEncryptionProfileSummary>");
        }
        body.push_str("</Items>");
        body.push_str("</FieldLevelEncryptionProfileList>");
        Ok(xml_response(StatusCode::OK, body, HeaderMap::new()))
    }
}

// ─── Realtime Log Config ──────────────────────────────────────────────

impl CloudFrontService {
    pub(crate) fn create_realtime_log_config(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let parsed: CreateRealtimeLogConfigRequest =
            xml_io::from_xml_root(&req.body).map_err(|e| {
                invalid_argument(format!("invalid CreateRealtimeLogConfigRequest XML: {e}"))
            })?;
        if parsed.name.is_empty() {
            return Err(invalid_argument("Name is required"));
        }
        let mut state = self.state.write();
        let account = state.accounts.entry(req.account_id.clone()).or_default();
        let arn = crate::service::cloudfront_arn(
            &req.region,
            &req.account_id,
            &format!("realtime-log-config/{}", parsed.name),
        );
        if account
            .realtime_log_configs
            .values()
            .any(|r| r.name == parsed.name)
        {
            return Err(aws_error(
                StatusCode::CONFLICT,
                "RealtimeLogConfigAlreadyExists",
                format!("RealtimeLogConfig {} already exists", parsed.name),
            ));
        }
        let stored = StoredRealtimeLogConfig {
            arn: arn.clone(),
            name: parsed.name,
            sampling_rate: parsed.sampling_rate,
            end_points: parsed.end_points,
            fields: parsed.fields,
        };
        account
            .realtime_log_configs
            .insert(arn.clone(), stored.clone());
        drop(state);
        let body = render_realtime_log(&stored, "CreateRealtimeLogConfigResult");
        Ok(xml_response(StatusCode::CREATED, body, HeaderMap::new()))
    }

    pub(crate) fn get_realtime_log_config(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let parsed: GetOrDeleteRealtimeLogConfigRequest = xml_io::from_xml_root(&req.body)
            .map_err(|e| {
                invalid_argument(format!("invalid GetRealtimeLogConfigRequest XML: {e}"))
            })?;
        let state = self.state.read();
        let configs = state
            .accounts
            .get(&req.account_id)
            .map(|a| &a.realtime_log_configs);
        let key = resolve_rtl_key(configs, &parsed)?;
        let r = configs
            .and_then(|c| c.get(&key).cloned())
            .ok_or_else(|| not_found("RealtimeLogConfig", &key))?;
        drop(state);
        let body = render_realtime_log(&r, "GetRealtimeLogConfigResult");
        Ok(xml_response(StatusCode::OK, body, HeaderMap::new()))
    }

    pub(crate) fn update_realtime_log_config(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let parsed: UpdateRealtimeLogConfigRequest =
            xml_io::from_xml_root(&req.body).map_err(|e| {
                invalid_argument(format!("invalid UpdateRealtimeLogConfigRequest XML: {e}"))
            })?;
        let mut state = self.state.write();
        let key = resolve_rtl_key(
            state
                .accounts
                .get(&req.account_id)
                .map(|a| &a.realtime_log_configs),
            &GetOrDeleteRealtimeLogConfigRequest {
                name: parsed.name.clone(),
                arn: parsed.arn.clone(),
            },
        )?;
        let account = state
            .accounts
            .get_mut(&req.account_id)
            .ok_or_else(|| not_found("RealtimeLogConfig", &key))?;
        let r = account
            .realtime_log_configs
            .get_mut(&key)
            .ok_or_else(|| not_found("RealtimeLogConfig", &key))?;
        if let Some(sampling_rate) = parsed.sampling_rate {
            r.sampling_rate = sampling_rate;
        }
        if let Some(end_points) = parsed.end_points {
            r.end_points = end_points;
        }
        if let Some(fields) = parsed.fields {
            r.fields = fields;
        }
        let snap = r.clone();
        drop(state);
        let body = render_realtime_log(&snap, "UpdateRealtimeLogConfigResult");
        Ok(xml_response(StatusCode::OK, body, HeaderMap::new()))
    }

    pub(crate) fn delete_realtime_log_config(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let parsed: GetOrDeleteRealtimeLogConfigRequest = xml_io::from_xml_root(&req.body)
            .map_err(|e| {
                invalid_argument(format!("invalid DeleteRealtimeLogConfigRequest XML: {e}"))
            })?;
        let mut state = self.state.write();
        let key = resolve_rtl_key(
            state
                .accounts
                .get(&req.account_id)
                .map(|a| &a.realtime_log_configs),
            &parsed,
        )?;
        let account = state
            .accounts
            .get_mut(&req.account_id)
            .ok_or_else(|| not_found("RealtimeLogConfig", &key))?;
        // Model-declared; checked after the If-Match (inferred).
        crate::validate::refuse_in_use(
            account,
            crate::validate::Reference::RealtimeLogConfig(&key),
        )?;
        if account.realtime_log_configs.remove(&key).is_none() {
            return Err(not_found("RealtimeLogConfig", &key));
        }
        drop(state);
        Ok(crate::policies::empty(StatusCode::NO_CONTENT))
    }

    pub(crate) fn list_realtime_log_configs(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let state = self.state.read();
        let mut items: Vec<StoredRealtimeLogConfig> = state
            .accounts
            .get(&req.account_id)
            .map(|a| a.realtime_log_configs.values().cloned().collect())
            .unwrap_or_default();
        drop(state);
        items.sort_by(|a, b| a.name.cmp(&b.name));

        let mut body = String::with_capacity(512);
        body.push_str(XML_DECL);
        body.push_str(&format!("<RealtimeLogConfigs xmlns=\"{NS}\">"));
        body.push_str("<MaxItems>100</MaxItems>");
        body.push_str(&format!("<IsTruncated>{}</IsTruncated>", false));
        body.push_str("<Marker></Marker>");
        body.push_str("<Items>");
        for r in &items {
            body.push_str("<member>");
            push_realtime_log_inner(&mut body, r);
            body.push_str("</member>");
        }
        body.push_str("</Items>");
        body.push_str("</RealtimeLogConfigs>");
        Ok(xml_response(StatusCode::OK, body, HeaderMap::new()))
    }
}

/// The storage key (ARN) of the realtime log config a Get/Delete request
/// names. A name is resolved against the stored configs rather than rebuilt
/// into an ARN, since the stored ARN's partition is the creating request's.
fn resolve_rtl_key(
    configs: Option<&std::collections::BTreeMap<String, StoredRealtimeLogConfig>>,
    parsed: &GetOrDeleteRealtimeLogConfigRequest,
) -> Result<String, AwsServiceError> {
    if let Some(arn) = parsed.arn.as_deref().filter(|a| !a.is_empty()) {
        return Ok(arn.to_string());
    }
    if let Some(name) = parsed.name.as_deref().filter(|n| !n.is_empty()) {
        return configs
            .and_then(|c| c.values().find(|r| r.name == name))
            .map(|r| r.arn.clone())
            .ok_or_else(|| not_found("RealtimeLogConfig", name));
    }
    Err(invalid_argument("Either Name or ARN must be specified"))
}

// ─── XML render helpers ───────────────────────────────────────────────

fn render_fle(f: &StoredFieldLevelEncryption) -> String {
    let mut out = String::with_capacity(512);
    out.push_str(XML_DECL);
    out.push_str(&format!("<FieldLevelEncryption xmlns=\"{NS}\">"));
    out.push_str(&format!("<Id>{}</Id>", esc(&f.id)));
    out.push_str(&format!(
        "<LastModifiedTime>{}</LastModifiedTime>",
        rfc3339(&f.last_modified_time)
    ));
    out.push_str(&render_fle_config_inner(&f.config));
    out.push_str("</FieldLevelEncryption>");
    out
}

fn render_fle_config(cfg: &FieldLevelEncryptionConfig) -> String {
    let mut out = String::with_capacity(512);
    out.push_str(XML_DECL);
    out.push_str(&format!("<FieldLevelEncryptionConfig xmlns=\"{NS}\">"));
    out.push_str(&render_fle_config_body(cfg));
    out.push_str("</FieldLevelEncryptionConfig>");
    out
}

fn render_fle_config_inner(cfg: &FieldLevelEncryptionConfig) -> String {
    let mut out = String::with_capacity(512);
    out.push_str("<FieldLevelEncryptionConfig>");
    out.push_str(&render_fle_config_body(cfg));
    out.push_str("</FieldLevelEncryptionConfig>");
    out
}

fn render_fle_config_body(cfg: &FieldLevelEncryptionConfig) -> String {
    let mut out = String::with_capacity(512);
    out.push_str(&format!(
        "<CallerReference>{}</CallerReference>",
        esc(&cfg.caller_reference)
    ));
    if let Some(c) = &cfg.comment {
        out.push_str(&format!("<Comment>{}</Comment>", esc(c)));
    }
    if let Some(q) = &cfg.query_arg_profile_config {
        out.push_str(&render_query_arg_profile_config(q));
    }
    if let Some(c) = &cfg.content_type_profile_config {
        out.push_str(&render_content_type_profile_config(c));
    }
    out
}

fn render_query_arg_profile_config(cfg: &crate::fle::QueryArgProfileConfig) -> String {
    let mut out = String::with_capacity(128);
    out.push_str("<QueryArgProfileConfig>");
    out.push_str(&format!(
        "<ForwardWhenQueryArgProfileIsUnknown>{}</ForwardWhenQueryArgProfileIsUnknown>",
        cfg.forward_when_query_arg_profile_is_unknown
    ));
    if let Some(qp) = &cfg.query_arg_profiles {
        out.push_str("<QueryArgProfiles>");
        out.push_str(&format!("<Quantity>{}</Quantity>", qp.quantity));
        if let Some(items) = &qp.items {
            out.push_str("<Items>");
            for q in &items.query_arg_profile {
                out.push_str("<QueryArgProfile>");
                out.push_str(&format!("<QueryArg>{}</QueryArg>", esc(&q.query_arg)));
                out.push_str(&format!("<ProfileId>{}</ProfileId>", esc(&q.profile_id)));
                out.push_str("</QueryArgProfile>");
            }
            out.push_str("</Items>");
        }
        out.push_str("</QueryArgProfiles>");
    }
    out.push_str("</QueryArgProfileConfig>");
    out
}

fn render_content_type_profile_config(cfg: &crate::fle::ContentTypeProfileConfig) -> String {
    let mut out = String::with_capacity(128);
    out.push_str("<ContentTypeProfileConfig>");
    out.push_str(&format!(
        "<ForwardWhenContentTypeIsUnknown>{}</ForwardWhenContentTypeIsUnknown>",
        cfg.forward_when_content_type_is_unknown
    ));
    if let Some(ct) = &cfg.content_type_profiles {
        out.push_str("<ContentTypeProfiles>");
        out.push_str(&format!("<Quantity>{}</Quantity>", ct.quantity));
        if let Some(items) = &ct.items {
            out.push_str("<Items>");
            for c in &items.content_type_profile {
                out.push_str("<ContentTypeProfile>");
                out.push_str(&format!("<Format>{}</Format>", esc(&c.format)));
                if let Some(p) = &c.profile_id {
                    out.push_str(&format!("<ProfileId>{}</ProfileId>", esc(p)));
                }
                out.push_str(&format!(
                    "<ContentType>{}</ContentType>",
                    esc(&c.content_type)
                ));
                out.push_str("</ContentTypeProfile>");
            }
            out.push_str("</Items>");
        }
        out.push_str("</ContentTypeProfiles>");
    }
    out.push_str("</ContentTypeProfileConfig>");
    out
}

fn render_fle_profile(p: &StoredFieldLevelEncryptionProfile) -> String {
    let mut out = String::with_capacity(512);
    out.push_str(XML_DECL);
    out.push_str(&format!("<FieldLevelEncryptionProfile xmlns=\"{NS}\">"));
    out.push_str(&format!("<Id>{}</Id>", esc(&p.id)));
    out.push_str(&format!(
        "<LastModifiedTime>{}</LastModifiedTime>",
        rfc3339(&p.last_modified_time)
    ));
    out.push_str(&render_fle_profile_config_inner(&p.config));
    out.push_str("</FieldLevelEncryptionProfile>");
    out
}

fn render_fle_profile_config(cfg: &FieldLevelEncryptionProfileConfig) -> String {
    let mut out = String::with_capacity(512);
    out.push_str(XML_DECL);
    out.push_str(&format!(
        "<FieldLevelEncryptionProfileConfig xmlns=\"{NS}\">"
    ));
    out.push_str(&render_fle_profile_config_body(cfg));
    out.push_str("</FieldLevelEncryptionProfileConfig>");
    out
}

fn render_fle_profile_config_inner(cfg: &FieldLevelEncryptionProfileConfig) -> String {
    let mut out = String::with_capacity(512);
    out.push_str("<FieldLevelEncryptionProfileConfig>");
    out.push_str(&render_fle_profile_config_body(cfg));
    out.push_str("</FieldLevelEncryptionProfileConfig>");
    out
}

fn render_fle_profile_config_body(cfg: &FieldLevelEncryptionProfileConfig) -> String {
    let mut out = String::with_capacity(512);
    out.push_str(&format!("<Name>{}</Name>", esc(&cfg.name)));
    out.push_str(&format!(
        "<CallerReference>{}</CallerReference>",
        esc(&cfg.caller_reference)
    ));
    if let Some(c) = &cfg.comment {
        out.push_str(&format!("<Comment>{}</Comment>", esc(c)));
    }
    out.push_str(&render_encryption_entities(&cfg.encryption_entities));
    out
}

fn render_encryption_entities(ee: &crate::fle::EncryptionEntities) -> String {
    let mut out = String::with_capacity(128);
    out.push_str("<EncryptionEntities>");
    out.push_str(&format!("<Quantity>{}</Quantity>", ee.quantity));
    if let Some(items) = &ee.items {
        out.push_str("<Items>");
        for e in &items.encryption_entity {
            out.push_str("<EncryptionEntity>");
            out.push_str(&format!(
                "<PublicKeyId>{}</PublicKeyId>",
                esc(&e.public_key_id)
            ));
            out.push_str(&format!("<ProviderId>{}</ProviderId>", esc(&e.provider_id)));
            out.push_str("<FieldPatterns>");
            out.push_str(&format!(
                "<Quantity>{}</Quantity>",
                e.field_patterns.quantity
            ));
            if let Some(it) = &e.field_patterns.items {
                out.push_str("<Items>");
                for fp in &it.field_pattern {
                    out.push_str(&format!("<FieldPattern>{}</FieldPattern>", esc(fp)));
                }
                out.push_str("</Items>");
            }
            out.push_str("</FieldPatterns>");
            out.push_str("</EncryptionEntity>");
        }
        out.push_str("</Items>");
    }
    out.push_str("</EncryptionEntities>");
    out
}

fn render_realtime_log(r: &StoredRealtimeLogConfig, root: &str) -> String {
    let mut out = String::with_capacity(512);
    out.push_str(XML_DECL);
    out.push_str(&format!("<{root} xmlns=\"{NS}\">"));
    out.push_str("<RealtimeLogConfig>");
    push_realtime_log_inner(&mut out, r);
    out.push_str("</RealtimeLogConfig>");
    out.push_str(&format!("</{root}>"));
    out
}

fn push_realtime_log_inner(out: &mut String, r: &StoredRealtimeLogConfig) {
    out.push_str(&format!("<ARN>{}</ARN>", esc(&r.arn)));
    out.push_str(&format!("<Name>{}</Name>", esc(&r.name)));
    out.push_str(&format!("<SamplingRate>{}</SamplingRate>", r.sampling_rate));
    out.push_str("<EndPoints>");
    for ep in &r.end_points.member {
        out.push_str("<member>");
        out.push_str(&format!(
            "<StreamType>{}</StreamType>",
            esc(&ep.stream_type)
        ));
        if let Some(k) = &ep.kinesis_stream_config {
            out.push_str("<KinesisStreamConfig>");
            out.push_str(&format!("<RoleARN>{}</RoleARN>", esc(&k.role_arn)));
            out.push_str(&format!("<StreamARN>{}</StreamARN>", esc(&k.stream_arn)));
            out.push_str("</KinesisStreamConfig>");
        }
        out.push_str("</member>");
    }
    out.push_str("</EndPoints>");
    out.push_str("<Fields>");
    for f in &r.fields.field {
        out.push_str(&format!("<Field>{}</Field>", esc(f)));
    }
    out.push_str("</Fields>");
}
