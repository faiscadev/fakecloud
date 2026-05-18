//! Handlers for CloudFront Batch 3 resources: Functions, Public Keys,
//! Key Groups, Key Value Stores, Origin Access Identities (legacy),
//! Monitoring Subscriptions.

use base64::Engine;
use chrono::Utc;
use http::header::ETAG;
use http::{HeaderMap, StatusCode};
use uuid::Uuid;

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use crate::functions::{
    CloudFrontOriginAccessIdentityConfig, FunctionConfig, ImportSource, KeyGroupConfig,
    MonitoringSubscriptionBody, PublicKeyConfig, StoredFunction, StoredKeyGroup,
    StoredKeyValueStore, StoredMonitoringSubscription, StoredOriginAccessIdentity, StoredPublicKey,
};
use crate::policies::{
    not_found, precondition_failed, require_if_match, rfc3339, route_id, xml_with_etag,
};
use crate::router::Route;
use crate::service::{
    aws_error, esc, generate_id_with_prefix, invalid_argument, xml_response, CloudFrontService,
    DEFAULT_ACCOUNT,
};
use crate::xml_io;

const NS: &str = crate::NAMESPACE;
const XML_DECL: &str = r#"<?xml version="1.0" encoding="UTF-8"?>"#;

// ─── CloudFront Functions ─────────────────────────────────────────────

impl CloudFrontService {
    pub(crate) fn create_function(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        // Body shape: <CreateFunctionRequest><Name/><FunctionConfig/><FunctionCode/></CreateFunctionRequest>
        let parsed: CreateFunctionRequest = xml_io::from_xml_root(&req.body)
            .map_err(|e| invalid_argument(format!("invalid CreateFunctionRequest XML: {e}")))?;
        if parsed.name.is_empty() {
            return Err(invalid_argument("CreateFunctionRequest.Name is required"));
        }

        let mut state = self.state.write();
        let account = state
            .accounts
            .entry(DEFAULT_ACCOUNT.to_string())
            .or_default();
        if account.functions.contains_key(&parsed.name) {
            return Err(aws_error(
                StatusCode::CONFLICT,
                "FunctionAlreadyExists",
                format!("Function {} already exists", parsed.name),
            ));
        }
        let now = Utc::now();
        let etag = generate_id_with_prefix("E");
        let function_arn = format!(
            "arn:aws:cloudfront::{}:function/{}",
            DEFAULT_ACCOUNT, parsed.name
        );
        let stored = StoredFunction {
            name: parsed.name.clone(),
            etag: etag.clone(),
            status: "UNPUBLISHED".to_string(),
            stage: "DEVELOPMENT".to_string(),
            function_arn: function_arn.clone(),
            created_time: now,
            last_modified_time: now,
            config: parsed.function_config,
            function_code: parsed.function_code,
            // No published snapshot until PublishFunction is called.
            live_function_code: None,
        };
        account
            .functions
            .insert(parsed.name.clone(), stored.clone());
        drop(state);

        let body = render_function_summary(&stored, "CreateFunctionResult");
        Ok(xml_with_etag(StatusCode::CREATED, body, &etag, None))
    }

    pub(crate) fn describe_function(
        &self,
        req: &AwsRequest,
        route: &Route,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = route_id(route, "Function")?;
        let stage = parse_stage_query(&req.raw_query);
        let state = self.state.read();
        let f = state
            .accounts
            .get(DEFAULT_ACCOUNT)
            .and_then(|a| a.functions.get(&name).cloned())
            .ok_or_else(|| not_found("Function", &name))?;
        drop(state);
        let view = stage_view(&f, &stage);
        let body = render_function_summary(&view, "DescribeFunctionResult");
        Ok(xml_with_etag(StatusCode::OK, body, &view.etag, None))
    }

    pub(crate) fn get_function(
        &self,
        req: &AwsRequest,
        route: &Route,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = route_id(route, "Function")?;
        let stage = parse_stage_query(&req.raw_query);
        let state = self.state.read();
        let f = state
            .accounts
            .get(DEFAULT_ACCOUNT)
            .and_then(|a| a.functions.get(&name).cloned())
            .ok_or_else(|| not_found("Function", &name))?;
        drop(state);
        let view = stage_view(&f, &stage);
        let mut headers = HeaderMap::new();
        headers.insert(ETAG, view.etag.parse().unwrap());
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(view.function_code.as_bytes())
            .unwrap_or_default();
        Ok(AwsResponse {
            status: StatusCode::OK,
            headers,
            content_type: "application/octet-stream".to_string(),
            body: fakecloud_core::service::ResponseBody::Bytes(bytes::Bytes::from(bytes)),
        })
    }

    pub(crate) fn update_function(
        &self,
        req: &AwsRequest,
        route: &Route,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = route_id(route, "Function")?;
        let if_match = require_if_match(req)?;
        let parsed: UpdateFunctionRequest = xml_io::from_xml_root(&req.body)
            .map_err(|e| invalid_argument(format!("invalid UpdateFunctionRequest XML: {e}")))?;
        let mut state = self.state.write();
        let account = state
            .accounts
            .get_mut(DEFAULT_ACCOUNT)
            .ok_or_else(|| not_found("Function", &name))?;
        let f = account
            .functions
            .get_mut(&name)
            .ok_or_else(|| not_found("Function", &name))?;
        if f.etag != if_match {
            return Err(precondition_failed());
        }
        f.config = parsed.function_config;
        f.function_code = parsed.function_code;
        f.etag = generate_id_with_prefix("E");
        f.last_modified_time = Utc::now();
        f.status = "UNPUBLISHED".to_string();
        f.stage = "DEVELOPMENT".to_string();
        let snap = f.clone();
        drop(state);
        let body = render_function_summary(&snap, "UpdateFunctionResult");
        // SDK has a known typo on UpdateFunctionOutput: it deserializes
        // the etag from header `ETtag`, not `ETag`. Send both so any
        // SDK version can read it.
        let mut headers = HeaderMap::new();
        if let Ok(v) = http::HeaderValue::from_str(&snap.etag) {
            headers.insert(ETAG, v.clone());
            headers.insert("ETtag", v);
        }
        Ok(xml_response(StatusCode::OK, body, headers))
    }

    pub(crate) fn delete_function(
        &self,
        req: &AwsRequest,
        route: &Route,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = route_id(route, "Function")?;
        let if_match = require_if_match(req)?;
        let mut state = self.state.write();
        let account = state
            .accounts
            .get_mut(DEFAULT_ACCOUNT)
            .ok_or_else(|| not_found("Function", &name))?;
        let f = account
            .functions
            .get(&name)
            .ok_or_else(|| not_found("Function", &name))?;
        if f.etag != if_match {
            return Err(precondition_failed());
        }
        account.functions.remove(&name);
        drop(state);
        Ok(crate::policies::empty(StatusCode::NO_CONTENT))
    }

    pub(crate) fn list_functions(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let stage = validate_stage_query(&req.raw_query)?;
        let state = self.state.read();
        let mut items: Vec<StoredFunction> = state
            .accounts
            .get(DEFAULT_ACCOUNT)
            .map(|a| a.functions.values().cloned().collect())
            .unwrap_or_default();
        drop(state);
        items.sort_by(|a, b| a.name.cmp(&b.name));

        let mut body = String::with_capacity(512);
        body.push_str(XML_DECL);
        body.push_str(&format!("<FunctionList xmlns=\"{NS}\">"));
        body.push_str("<Marker></Marker>");
        body.push_str("<MaxItems>100</MaxItems>");
        body.push_str(&format!("<Quantity>{}</Quantity>", items.len()));
        body.push_str("<Items>");
        for f in &items {
            let view = stage_view(f, &stage);
            body.push_str(&render_function_summary_inner(&view));
        }
        body.push_str("</Items>");
        body.push_str("</FunctionList>");
        Ok(xml_response(StatusCode::OK, body, HeaderMap::new()))
    }

    pub(crate) fn publish_function(
        &self,
        req: &AwsRequest,
        route: &Route,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = route_id(route, "Function")?;
        let if_match = require_if_match(req)?;
        let mut state = self.state.write();
        let account = state
            .accounts
            .get_mut(DEFAULT_ACCOUNT)
            .ok_or_else(|| not_found("Function", &name))?;
        let f = account
            .functions
            .get_mut(&name)
            .ok_or_else(|| not_found("Function", &name))?;
        if f.etag != if_match {
            return Err(precondition_failed());
        }
        f.status = "DEPLOYED".to_string();
        f.stage = "LIVE".to_string();
        f.last_modified_time = Utc::now();
        // Freeze the current development code as the LIVE snapshot.
        // Subsequent UpdateFunction calls mutate `function_code` but
        // leave this alone, so `TestFunction(Stage=LIVE)` keeps running
        // the published version until the next Publish.
        f.live_function_code = Some(f.function_code.clone());
        let snap = f.clone();
        drop(state);
        let body = render_function_summary(&snap, "PublishFunctionResult");
        Ok(xml_with_etag(StatusCode::OK, body, &snap.etag, None))
    }

    pub(crate) fn test_function(
        &self,
        req: &AwsRequest,
        route: &Route,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = route_id(route, "Function")?;
        let if_match = require_if_match(req)?;
        let parsed: TestFunctionRequest = xml_io::from_xml_root(&req.body)
            .map_err(|e| invalid_argument(format!("invalid TestFunctionRequest XML: {e}")))?;
        let event_bytes = base64::engine::general_purpose::STANDARD
            .decode(parsed.event_object.trim().as_bytes())
            .map_err(|e| invalid_argument(format!("EventObject is not valid base64: {e}")))?;

        let state = self.state.read();
        let f = state
            .accounts
            .get(DEFAULT_ACCOUNT)
            .and_then(|a| a.functions.get(&name).cloned())
            .ok_or_else(|| {
                aws_error(
                    StatusCode::NOT_FOUND,
                    "NoSuchFunctionExists",
                    format!("The specified function does not exist: {name}"),
                )
            })?;
        drop(state);
        if f.etag != if_match {
            return Err(precondition_failed());
        }

        // AWS lets callers pick which version to run. DEVELOPMENT (the
        // default) is the latest CreateFunction / UpdateFunction body;
        // LIVE is the snapshot taken at PublishFunction. Falling back to
        // DEVELOPMENT when no snapshot exists matches the AWS error
        // shape ("function not yet published") less closely, but is
        // strictly nicer for tests against unpublished functions.
        let stage = parsed.stage.as_deref().unwrap_or("DEVELOPMENT");
        let source_b64 = if stage.eq_ignore_ascii_case("LIVE") {
            f.live_function_code.as_deref().unwrap_or(&f.function_code)
        } else {
            f.function_code.as_str()
        };
        let code_bytes = base64::engine::general_purpose::STANDARD
            .decode(source_b64.as_bytes())
            .unwrap_or_else(|_| source_b64.as_bytes().to_vec());
        let code = String::from_utf8(code_bytes)
            .map_err(|e| invalid_argument(format!("function code is not valid UTF-8: {e}")))?;
        let exec = crate::js_runtime::run_handler(&code, &event_bytes);

        let mut body = String::with_capacity(1024);
        body.push_str(XML_DECL);
        body.push_str(&format!("<TestResult xmlns=\"{NS}\">"));
        body.push_str(&render_function_summary_inner(&f));
        body.push_str(&format!(
            "<ComputeUtilization>{}</ComputeUtilization>",
            exec.compute_utilization
        ));
        body.push_str("<FunctionExecutionLogs>");
        for line in &exec.logs {
            body.push_str(&format!("<member>{}</member>", esc(line)));
        }
        body.push_str("</FunctionExecutionLogs>");
        body.push_str(&format!(
            "<FunctionErrorMessage>{}</FunctionErrorMessage>",
            esc(exec.error.as_deref().unwrap_or(""))
        ));
        body.push_str(&format!(
            "<FunctionOutput>{}</FunctionOutput>",
            esc(exec.output.as_deref().unwrap_or(""))
        ));
        body.push_str("</TestResult>");
        Ok(xml_response(StatusCode::OK, body, HeaderMap::new()))
    }
}

// ─── Public Keys ──────────────────────────────────────────────────────

impl CloudFrontService {
    pub(crate) fn create_public_key(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let cfg: PublicKeyConfig = xml_io::from_xml_root(&req.body)
            .map_err(|e| invalid_argument(format!("invalid PublicKeyConfig XML: {e}")))?;
        if cfg.name.is_empty() {
            return Err(invalid_argument("PublicKeyConfig.Name is required"));
        }
        if cfg.encoded_key.is_empty() {
            return Err(invalid_argument("PublicKeyConfig.EncodedKey is required"));
        }
        let mut state = self.state.write();
        let account = state
            .accounts
            .entry(DEFAULT_ACCOUNT.to_string())
            .or_default();
        if let Some(existing) = account
            .public_keys
            .values()
            .find(|p| p.config.caller_reference == cfg.caller_reference)
        {
            return Err(aws_error(
                StatusCode::CONFLICT,
                "PublicKeyAlreadyExists",
                format!(
                    "PublicKey with same CallerReference exists: {}",
                    existing.id
                ),
            ));
        }
        let id = generate_id_with_prefix("K");
        let etag = generate_id_with_prefix("E");
        let stored = StoredPublicKey {
            id: id.clone(),
            etag: etag.clone(),
            created_time: Utc::now(),
            config: cfg,
        };
        account.public_keys.insert(id.clone(), stored.clone());
        drop(state);
        let body = render_public_key(&stored, "PublicKey");
        Ok(xml_with_etag(StatusCode::CREATED, body, &etag, Some(&id)))
    }

    pub(crate) fn get_public_key(&self, route: &Route) -> Result<AwsResponse, AwsServiceError> {
        let id = route_id(route, "PublicKey")?;
        let state = self.state.read();
        let p = state
            .accounts
            .get(DEFAULT_ACCOUNT)
            .and_then(|a| a.public_keys.get(&id).cloned())
            .ok_or_else(|| not_found("PublicKey", &id))?;
        drop(state);
        let body = render_public_key(&p, "PublicKey");
        Ok(xml_with_etag(StatusCode::OK, body, &p.etag, None))
    }

    pub(crate) fn get_public_key_config(
        &self,
        route: &Route,
    ) -> Result<AwsResponse, AwsServiceError> {
        let id = route_id(route, "PublicKey")?;
        let state = self.state.read();
        let p = state
            .accounts
            .get(DEFAULT_ACCOUNT)
            .and_then(|a| a.public_keys.get(&id).cloned())
            .ok_or_else(|| not_found("PublicKey", &id))?;
        drop(state);
        let body = config_xml("PublicKeyConfig", &p.config)?;
        Ok(xml_with_etag(StatusCode::OK, body, &p.etag, None))
    }

    pub(crate) fn update_public_key(
        &self,
        req: &AwsRequest,
        route: &Route,
    ) -> Result<AwsResponse, AwsServiceError> {
        let id = route_id(route, "PublicKey")?;
        let if_match = require_if_match(req)?;
        let cfg: PublicKeyConfig = xml_io::from_xml_root(&req.body)
            .map_err(|e| invalid_argument(format!("invalid PublicKeyConfig XML: {e}")))?;
        if cfg.name.is_empty() {
            return Err(invalid_argument("PublicKeyConfig.Name is required"));
        }
        let mut state = self.state.write();
        let account = state
            .accounts
            .get_mut(DEFAULT_ACCOUNT)
            .ok_or_else(|| not_found("PublicKey", &id))?;
        let p = account
            .public_keys
            .get_mut(&id)
            .ok_or_else(|| not_found("PublicKey", &id))?;
        if p.etag != if_match {
            return Err(precondition_failed());
        }
        // CallerReference is immutable per AWS.
        if p.config.caller_reference != cfg.caller_reference {
            return Err(invalid_argument(
                "CallerReference cannot change on UpdatePublicKey",
            ));
        }
        p.config = cfg;
        p.etag = generate_id_with_prefix("E");
        let snap = p.clone();
        drop(state);
        let body = render_public_key(&snap, "PublicKey");
        Ok(xml_with_etag(StatusCode::OK, body, &snap.etag, None))
    }

    pub(crate) fn delete_public_key(
        &self,
        req: &AwsRequest,
        route: &Route,
    ) -> Result<AwsResponse, AwsServiceError> {
        let id = route_id(route, "PublicKey")?;
        let if_match = require_if_match(req)?;
        let mut state = self.state.write();
        let account = state
            .accounts
            .get_mut(DEFAULT_ACCOUNT)
            .ok_or_else(|| not_found("PublicKey", &id))?;
        let p = account
            .public_keys
            .get(&id)
            .ok_or_else(|| not_found("PublicKey", &id))?;
        if p.etag != if_match {
            return Err(precondition_failed());
        }
        account.public_keys.remove(&id);
        drop(state);
        Ok(crate::policies::empty(StatusCode::NO_CONTENT))
    }

    pub(crate) fn list_public_keys(
        &self,
        _req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let state = self.state.read();
        let mut items: Vec<StoredPublicKey> = state
            .accounts
            .get(DEFAULT_ACCOUNT)
            .map(|a| a.public_keys.values().cloned().collect())
            .unwrap_or_default();
        drop(state);
        items.sort_by(|a, b| a.id.cmp(&b.id));

        let mut body = String::with_capacity(512);
        body.push_str(XML_DECL);
        body.push_str(&format!("<PublicKeyList xmlns=\"{NS}\">"));
        body.push_str("<Marker></Marker>");
        body.push_str("<MaxItems>100</MaxItems>");
        body.push_str(&format!("<Quantity>{}</Quantity>", items.len()));
        body.push_str("<Items>");
        for p in &items {
            body.push_str("<PublicKeySummary>");
            body.push_str(&format!("<Id>{}</Id>", esc(&p.id)));
            body.push_str(&format!("<Name>{}</Name>", esc(&p.config.name)));
            body.push_str(&format!(
                "<CreatedTime>{}</CreatedTime>",
                rfc3339(&p.created_time)
            ));
            body.push_str(&format!(
                "<EncodedKey>{}</EncodedKey>",
                esc(&p.config.encoded_key)
            ));
            if let Some(c) = &p.config.comment {
                body.push_str(&format!("<Comment>{}</Comment>", esc(c)));
            }
            body.push_str("</PublicKeySummary>");
        }
        body.push_str("</Items>");
        body.push_str("</PublicKeyList>");
        Ok(xml_response(StatusCode::OK, body, HeaderMap::new()))
    }
}

// ─── Key Groups ───────────────────────────────────────────────────────

impl CloudFrontService {
    pub(crate) fn create_key_group(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let cfg: KeyGroupConfig = xml_io::from_xml_root(&req.body)
            .map_err(|e| invalid_argument(format!("invalid KeyGroupConfig XML: {e}")))?;
        if cfg.name.is_empty() {
            return Err(invalid_argument("KeyGroupConfig.Name is required"));
        }
        let mut state = self.state.write();
        let account = state
            .accounts
            .entry(DEFAULT_ACCOUNT.to_string())
            .or_default();
        let id = generate_id_with_prefix("K");
        let etag = generate_id_with_prefix("E");
        let stored = StoredKeyGroup {
            id: id.clone(),
            etag: etag.clone(),
            last_modified_time: Utc::now(),
            config: cfg,
        };
        account.key_groups.insert(id.clone(), stored.clone());
        drop(state);
        let body = render_key_group(&stored, "KeyGroup");
        Ok(xml_with_etag(StatusCode::CREATED, body, &etag, Some(&id)))
    }

    pub(crate) fn get_key_group(&self, route: &Route) -> Result<AwsResponse, AwsServiceError> {
        let id = route_id(route, "KeyGroup")?;
        let state = self.state.read();
        let g = state
            .accounts
            .get(DEFAULT_ACCOUNT)
            .and_then(|a| a.key_groups.get(&id).cloned())
            .ok_or_else(|| not_found("KeyGroup", &id))?;
        drop(state);
        let body = render_key_group(&g, "KeyGroup");
        Ok(xml_with_etag(StatusCode::OK, body, &g.etag, None))
    }

    pub(crate) fn get_key_group_config(
        &self,
        route: &Route,
    ) -> Result<AwsResponse, AwsServiceError> {
        let id = route_id(route, "KeyGroup")?;
        let state = self.state.read();
        let g = state
            .accounts
            .get(DEFAULT_ACCOUNT)
            .and_then(|a| a.key_groups.get(&id).cloned())
            .ok_or_else(|| not_found("KeyGroup", &id))?;
        drop(state);
        let body = config_xml("KeyGroupConfig", &g.config)?;
        Ok(xml_with_etag(StatusCode::OK, body, &g.etag, None))
    }

    pub(crate) fn update_key_group(
        &self,
        req: &AwsRequest,
        route: &Route,
    ) -> Result<AwsResponse, AwsServiceError> {
        let id = route_id(route, "KeyGroup")?;
        let if_match = require_if_match(req)?;
        let cfg: KeyGroupConfig = xml_io::from_xml_root(&req.body)
            .map_err(|e| invalid_argument(format!("invalid KeyGroupConfig XML: {e}")))?;
        if cfg.name.is_empty() {
            return Err(invalid_argument("KeyGroupConfig.Name is required"));
        }
        let mut state = self.state.write();
        let account = state
            .accounts
            .get_mut(DEFAULT_ACCOUNT)
            .ok_or_else(|| not_found("KeyGroup", &id))?;
        let g = account
            .key_groups
            .get_mut(&id)
            .ok_or_else(|| not_found("KeyGroup", &id))?;
        if g.etag != if_match {
            return Err(precondition_failed());
        }
        g.config = cfg;
        g.etag = generate_id_with_prefix("E");
        g.last_modified_time = Utc::now();
        let snap = g.clone();
        drop(state);
        let body = render_key_group(&snap, "KeyGroup");
        Ok(xml_with_etag(StatusCode::OK, body, &snap.etag, None))
    }

    pub(crate) fn delete_key_group(
        &self,
        req: &AwsRequest,
        route: &Route,
    ) -> Result<AwsResponse, AwsServiceError> {
        let id = route_id(route, "KeyGroup")?;
        let if_match = require_if_match(req)?;
        let mut state = self.state.write();
        let account = state
            .accounts
            .get_mut(DEFAULT_ACCOUNT)
            .ok_or_else(|| not_found("KeyGroup", &id))?;
        let g = account
            .key_groups
            .get(&id)
            .ok_or_else(|| not_found("KeyGroup", &id))?;
        if g.etag != if_match {
            return Err(precondition_failed());
        }
        account.key_groups.remove(&id);
        drop(state);
        Ok(crate::policies::empty(StatusCode::NO_CONTENT))
    }

    pub(crate) fn list_key_groups(
        &self,
        _req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let state = self.state.read();
        let mut items: Vec<StoredKeyGroup> = state
            .accounts
            .get(DEFAULT_ACCOUNT)
            .map(|a| a.key_groups.values().cloned().collect())
            .unwrap_or_default();
        drop(state);
        items.sort_by(|a, b| a.config.name.cmp(&b.config.name));

        let mut body = String::with_capacity(512);
        body.push_str(XML_DECL);
        body.push_str(&format!("<KeyGroupList xmlns=\"{NS}\">"));
        body.push_str("<Marker></Marker>");
        body.push_str("<MaxItems>100</MaxItems>");
        body.push_str(&format!("<Quantity>{}</Quantity>", items.len()));
        body.push_str("<Items>");
        for g in &items {
            body.push_str("<KeyGroupSummary>");
            body.push_str("<KeyGroup>");
            push_key_group_inner(&mut body, g);
            body.push_str("</KeyGroup>");
            body.push_str("</KeyGroupSummary>");
        }
        body.push_str("</Items>");
        body.push_str("</KeyGroupList>");
        Ok(xml_response(StatusCode::OK, body, HeaderMap::new()))
    }
}

// ─── Key Value Stores ─────────────────────────────────────────────────

impl CloudFrontService {
    pub(crate) fn create_key_value_store(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let parsed: CreateKeyValueStoreRequest = xml_io::from_xml_root(&req.body)
            .map_err(|e| invalid_argument(format!("invalid CreateKeyValueStore XML: {e}")))?;
        if parsed.name.is_empty() {
            return Err(invalid_argument("Name is required"));
        }
        let mut state = self.state.write();
        let account = state
            .accounts
            .entry(DEFAULT_ACCOUNT.to_string())
            .or_default();
        if account.key_value_stores.contains_key(&parsed.name) {
            return Err(aws_error(
                StatusCode::CONFLICT,
                "EntityAlreadyExists",
                format!("KeyValueStore {} already exists", parsed.name),
            ));
        }
        let now = Utc::now();
        let id = Uuid::new_v4().to_string();
        let etag = generate_id_with_prefix("E");
        let arn = format!(
            "arn:aws:cloudfront::{}:key-value-store/{}",
            DEFAULT_ACCOUNT, id
        );
        let stored = StoredKeyValueStore {
            name: parsed.name.clone(),
            id,
            etag: etag.clone(),
            arn,
            status: "READY".to_string(),
            created_time: now,
            last_modified_time: now,
            comment: parsed.comment,
            import_source: parsed.import_source,
        };
        account
            .key_value_stores
            .insert(parsed.name.clone(), stored.clone());
        drop(state);
        let body = render_key_value_store(&stored, "CreateKeyValueStoreResult");
        Ok(xml_with_etag(StatusCode::CREATED, body, &etag, None))
    }

    pub(crate) fn describe_key_value_store(
        &self,
        route: &Route,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = route_id(route, "KeyValueStore")?;
        let state = self.state.read();
        let kvs = state
            .accounts
            .get(DEFAULT_ACCOUNT)
            .and_then(|a| a.key_value_stores.get(&name).cloned())
            .ok_or_else(|| not_found("KeyValueStore", &name))?;
        drop(state);
        let body = render_key_value_store(&kvs, "DescribeKeyValueStoreResult");
        Ok(xml_with_etag(StatusCode::OK, body, &kvs.etag, None))
    }

    pub(crate) fn update_key_value_store(
        &self,
        req: &AwsRequest,
        route: &Route,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = route_id(route, "KeyValueStore")?;
        let if_match = require_if_match(req)?;
        let parsed: UpdateKeyValueStoreRequest = xml_io::from_xml_root(&req.body)
            .map_err(|e| invalid_argument(format!("invalid UpdateKeyValueStore XML: {e}")))?;
        let mut state = self.state.write();
        let account = state
            .accounts
            .get_mut(DEFAULT_ACCOUNT)
            .ok_or_else(|| not_found("KeyValueStore", &name))?;
        let kvs = account
            .key_value_stores
            .get_mut(&name)
            .ok_or_else(|| not_found("KeyValueStore", &name))?;
        if kvs.etag != if_match {
            return Err(precondition_failed());
        }
        kvs.comment = Some(parsed.comment);
        kvs.etag = generate_id_with_prefix("E");
        kvs.last_modified_time = Utc::now();
        let snap = kvs.clone();
        drop(state);
        let body = render_key_value_store(&snap, "UpdateKeyValueStoreResult");
        Ok(xml_with_etag(StatusCode::OK, body, &snap.etag, None))
    }

    pub(crate) fn delete_key_value_store(
        &self,
        req: &AwsRequest,
        route: &Route,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = route_id(route, "KeyValueStore")?;
        let if_match = require_if_match(req)?;
        let mut state = self.state.write();
        let account = state
            .accounts
            .get_mut(DEFAULT_ACCOUNT)
            .ok_or_else(|| not_found("KeyValueStore", &name))?;
        let kvs = account
            .key_value_stores
            .get(&name)
            .ok_or_else(|| not_found("KeyValueStore", &name))?;
        if kvs.etag != if_match {
            return Err(precondition_failed());
        }
        account.key_value_stores.remove(&name);
        drop(state);
        Ok(crate::policies::empty(StatusCode::NO_CONTENT))
    }

    pub(crate) fn list_key_value_stores(
        &self,
        _req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let state = self.state.read();
        let mut items: Vec<StoredKeyValueStore> = state
            .accounts
            .get(DEFAULT_ACCOUNT)
            .map(|a| a.key_value_stores.values().cloned().collect())
            .unwrap_or_default();
        drop(state);
        items.sort_by(|a, b| a.name.cmp(&b.name));

        let mut body = String::with_capacity(512);
        body.push_str(XML_DECL);
        body.push_str(&format!("<KeyValueStoreList xmlns=\"{NS}\">"));
        body.push_str("<NextMarker></NextMarker>");
        body.push_str("<MaxItems>100</MaxItems>");
        body.push_str(&format!("<Quantity>{}</Quantity>", items.len()));
        body.push_str("<Items>");
        for kvs in &items {
            body.push_str("<KeyValueStore>");
            push_kvs_inner(&mut body, kvs);
            body.push_str("</KeyValueStore>");
        }
        body.push_str("</Items>");
        body.push_str("</KeyValueStoreList>");
        Ok(xml_response(StatusCode::OK, body, HeaderMap::new()))
    }
}

// ─── Origin Access Identities (legacy) ────────────────────────────────

impl CloudFrontService {
    pub(crate) fn create_oai(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let cfg: CloudFrontOriginAccessIdentityConfig = xml_io::from_xml_root(&req.body)
            .map_err(|e| invalid_argument(format!("invalid OAI config XML: {e}")))?;
        if cfg.caller_reference.is_empty() {
            return Err(invalid_argument("CallerReference is required"));
        }
        let mut state = self.state.write();
        let account = state
            .accounts
            .entry(DEFAULT_ACCOUNT.to_string())
            .or_default();
        if let Some(existing) = account
            .origin_access_identities
            .values()
            .find(|o| o.config.caller_reference == cfg.caller_reference)
        {
            return Err(aws_error(
                StatusCode::CONFLICT,
                "CloudFrontOriginAccessIdentityAlreadyExists",
                format!("OAI with same CallerReference exists: {}", existing.id),
            ));
        }
        let id = format!(
            "E{}",
            Uuid::new_v4()
                .simple()
                .to_string()
                .to_uppercase()
                .chars()
                .take(13)
                .collect::<String>()
        );
        let etag = generate_id_with_prefix("E");
        let canonical = Uuid::new_v4().simple().to_string();
        let stored = StoredOriginAccessIdentity {
            id: id.clone(),
            etag: etag.clone(),
            s3_canonical_user_id: canonical,
            config: cfg,
        };
        account
            .origin_access_identities
            .insert(id.clone(), stored.clone());
        drop(state);
        let body = render_oai(&stored, "CloudFrontOriginAccessIdentity");
        Ok(xml_with_etag(StatusCode::CREATED, body, &etag, Some(&id)))
    }

    pub(crate) fn get_oai(&self, route: &Route) -> Result<AwsResponse, AwsServiceError> {
        let id = route_id(route, "CloudFrontOriginAccessIdentity")?;
        let state = self.state.read();
        let oai = state
            .accounts
            .get(DEFAULT_ACCOUNT)
            .and_then(|a| a.origin_access_identities.get(&id).cloned())
            .ok_or_else(|| not_found("CloudFrontOriginAccessIdentity", &id))?;
        drop(state);
        let body = render_oai(&oai, "CloudFrontOriginAccessIdentity");
        Ok(xml_with_etag(StatusCode::OK, body, &oai.etag, None))
    }

    pub(crate) fn get_oai_config(&self, route: &Route) -> Result<AwsResponse, AwsServiceError> {
        let id = route_id(route, "CloudFrontOriginAccessIdentity")?;
        let state = self.state.read();
        let oai = state
            .accounts
            .get(DEFAULT_ACCOUNT)
            .and_then(|a| a.origin_access_identities.get(&id).cloned())
            .ok_or_else(|| not_found("CloudFrontOriginAccessIdentity", &id))?;
        drop(state);
        let body = config_xml("CloudFrontOriginAccessIdentityConfig", &oai.config)?;
        Ok(xml_with_etag(StatusCode::OK, body, &oai.etag, None))
    }

    pub(crate) fn update_oai(
        &self,
        req: &AwsRequest,
        route: &Route,
    ) -> Result<AwsResponse, AwsServiceError> {
        let id = route_id(route, "CloudFrontOriginAccessIdentity")?;
        let if_match = require_if_match(req)?;
        let cfg: CloudFrontOriginAccessIdentityConfig = xml_io::from_xml_root(&req.body)
            .map_err(|e| invalid_argument(format!("invalid OAI config XML: {e}")))?;
        let mut state = self.state.write();
        let account = state
            .accounts
            .get_mut(DEFAULT_ACCOUNT)
            .ok_or_else(|| not_found("CloudFrontOriginAccessIdentity", &id))?;
        let oai = account
            .origin_access_identities
            .get_mut(&id)
            .ok_or_else(|| not_found("CloudFrontOriginAccessIdentity", &id))?;
        if oai.etag != if_match {
            return Err(precondition_failed());
        }
        if oai.config.caller_reference != cfg.caller_reference {
            return Err(invalid_argument(
                "CallerReference cannot change on UpdateCloudFrontOriginAccessIdentity",
            ));
        }
        oai.config = cfg;
        oai.etag = generate_id_with_prefix("E");
        let snap = oai.clone();
        drop(state);
        let body = render_oai(&snap, "CloudFrontOriginAccessIdentity");
        Ok(xml_with_etag(StatusCode::OK, body, &snap.etag, None))
    }

    pub(crate) fn delete_oai(
        &self,
        req: &AwsRequest,
        route: &Route,
    ) -> Result<AwsResponse, AwsServiceError> {
        let id = route_id(route, "CloudFrontOriginAccessIdentity")?;
        let if_match = require_if_match(req)?;
        let mut state = self.state.write();
        let account = state
            .accounts
            .get_mut(DEFAULT_ACCOUNT)
            .ok_or_else(|| not_found("CloudFrontOriginAccessIdentity", &id))?;
        let oai = account
            .origin_access_identities
            .get(&id)
            .ok_or_else(|| not_found("CloudFrontOriginAccessIdentity", &id))?;
        if oai.etag != if_match {
            return Err(precondition_failed());
        }
        account.origin_access_identities.remove(&id);
        drop(state);
        Ok(crate::policies::empty(StatusCode::NO_CONTENT))
    }

    pub(crate) fn list_oai(&self, _req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let state = self.state.read();
        let mut items: Vec<StoredOriginAccessIdentity> = state
            .accounts
            .get(DEFAULT_ACCOUNT)
            .map(|a| a.origin_access_identities.values().cloned().collect())
            .unwrap_or_default();
        drop(state);
        items.sort_by(|a, b| a.id.cmp(&b.id));

        let mut body = String::with_capacity(512);
        body.push_str(XML_DECL);
        body.push_str(&format!(
            "<CloudFrontOriginAccessIdentityList xmlns=\"{NS}\">"
        ));
        body.push_str("<Marker></Marker>");
        body.push_str("<MaxItems>100</MaxItems>");
        body.push_str("<IsTruncated>false</IsTruncated>");
        body.push_str(&format!("<Quantity>{}</Quantity>", items.len()));
        body.push_str("<Items>");
        for oai in &items {
            body.push_str("<CloudFrontOriginAccessIdentitySummary>");
            body.push_str(&format!("<Id>{}</Id>", esc(&oai.id)));
            body.push_str(&format!(
                "<S3CanonicalUserId>{}</S3CanonicalUserId>",
                esc(&oai.s3_canonical_user_id)
            ));
            body.push_str(&format!("<Comment>{}</Comment>", esc(&oai.config.comment)));
            body.push_str("</CloudFrontOriginAccessIdentitySummary>");
        }
        body.push_str("</Items>");
        body.push_str("</CloudFrontOriginAccessIdentityList>");
        Ok(xml_response(StatusCode::OK, body, HeaderMap::new()))
    }
}

// ─── Monitoring Subscriptions ─────────────────────────────────────────

impl CloudFrontService {
    pub(crate) fn create_monitoring_subscription(
        &self,
        req: &AwsRequest,
        route: &Route,
    ) -> Result<AwsResponse, AwsServiceError> {
        let dist_id = route_id(route, "Distribution")?;
        // Check the distribution exists before parsing the body. Synthetic
        // probes hit this op against a placeholder distribution ID; surfacing
        // NoSuchDistribution (declared in the Smithy errors list) before any
        // XML-parse InvalidArgument keeps the conformance probe honest.
        {
            let state = self.state.read();
            let has_dist = state
                .accounts
                .get(DEFAULT_ACCOUNT)
                .is_some_and(|a| a.distributions.contains_key(&dist_id));
            if !has_dist {
                return Err(not_found("Distribution", &dist_id));
            }
        }
        let parsed: MonitoringSubscriptionBody = xml_io::from_xml_root(&req.body)
            .map_err(|e| invalid_argument(format!("invalid MonitoringSubscription XML: {e}")))?;
        let mut state = self.state.write();
        let account = state
            .accounts
            .entry(DEFAULT_ACCOUNT.to_string())
            .or_default();
        if !account.distributions.contains_key(&dist_id) {
            return Err(not_found("Distribution", &dist_id));
        }
        let stored = StoredMonitoringSubscription {
            distribution_id: dist_id.clone(),
            config: parsed.realtime_metrics_subscription_config,
        };
        account
            .monitoring_subscriptions
            .insert(dist_id.clone(), stored.clone());
        drop(state);
        let body = render_monitoring(&stored);
        Ok(xml_response(StatusCode::OK, body, HeaderMap::new()))
    }

    pub(crate) fn get_monitoring_subscription(
        &self,
        route: &Route,
    ) -> Result<AwsResponse, AwsServiceError> {
        let dist_id = route_id(route, "Distribution")?;
        let state = self.state.read();
        let m = state
            .accounts
            .get(DEFAULT_ACCOUNT)
            .and_then(|a| a.monitoring_subscriptions.get(&dist_id).cloned())
            .ok_or_else(|| {
                aws_error(
                    StatusCode::NOT_FOUND,
                    "NoSuchMonitoringSubscription",
                    format!("No monitoring subscription for distribution {dist_id}"),
                )
            })?;
        drop(state);
        let body = render_monitoring(&m);
        Ok(xml_response(StatusCode::OK, body, HeaderMap::new()))
    }

    pub(crate) fn delete_monitoring_subscription(
        &self,
        route: &Route,
    ) -> Result<AwsResponse, AwsServiceError> {
        let dist_id = route_id(route, "Distribution")?;
        let mut state = self.state.write();
        let account = state
            .accounts
            .get_mut(DEFAULT_ACCOUNT)
            .ok_or_else(|| not_found("Distribution", &dist_id))?;
        if account.monitoring_subscriptions.remove(&dist_id).is_none() {
            return Err(aws_error(
                StatusCode::NOT_FOUND,
                "NoSuchMonitoringSubscription",
                format!("No monitoring subscription for distribution {dist_id}"),
            ));
        }
        drop(state);
        Ok(crate::policies::empty(StatusCode::NO_CONTENT))
    }
}

// ─── Helpers ──────────────────────────────────────────────────────────

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "PascalCase")]
struct CreateFunctionRequest {
    name: String,
    function_config: FunctionConfig,
    /// Base64-encoded source.
    function_code: String,
}

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "PascalCase")]
struct UpdateFunctionRequest {
    function_config: FunctionConfig,
    function_code: String,
}

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "PascalCase")]
struct TestFunctionRequest {
    #[serde(default)]
    event_object: String,
    #[serde(default)]
    stage: Option<String>,
}

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "PascalCase")]
struct CreateKeyValueStoreRequest {
    name: String,
    #[serde(default)]
    comment: Option<String>,
    #[serde(default)]
    import_source: Option<ImportSource>,
}

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "PascalCase")]
struct UpdateKeyValueStoreRequest {
    comment: String,
}

fn config_xml<T: serde::Serialize>(root: &str, cfg: &T) -> Result<String, AwsServiceError> {
    let inner = quick_xml::se::to_string_with_root(root, cfg).map_err(|e| {
        aws_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "InternalError",
            format!("xml encode failed: {e}"),
        )
    })?;
    let stamped = inner.replacen(
        &format!("<{root}>"),
        &format!("<{root} xmlns=\"{NS}\">", NS = crate::NAMESPACE),
        1,
    );
    Ok(format!("{XML_DECL}{stamped}"))
}

fn parse_stage_query(query: &str) -> Option<String> {
    use std::collections::HashMap;
    let pairs: HashMap<&str, &str> = query.split('&').filter_map(|p| p.split_once('=')).collect();
    pairs.get("Stage").map(|s| s.to_string())
}

/// Validate the `Stage` query against the FunctionStage enum
/// (DEVELOPMENT | LIVE). Returns InvalidArgument for unknown values; absent
/// or valid is OK.
pub(crate) fn validate_stage_query(
    query: &str,
) -> Result<Option<String>, fakecloud_core::service::AwsServiceError> {
    let stage = parse_stage_query(query);
    if let Some(ref v) = stage {
        if v != "DEVELOPMENT" && v != "LIVE" {
            return Err(crate::service::invalid_argument(format!(
                "Stage must be one of 'DEVELOPMENT' or 'LIVE', got '{v}'"
            )));
        }
    }
    Ok(stage)
}

fn stage_view(f: &StoredFunction, stage: &Option<String>) -> StoredFunction {
    let mut clone = f.clone();
    if stage.as_deref() == Some("LIVE") {
        clone.stage = "LIVE".into();
    }
    clone
}

fn render_function_summary(f: &StoredFunction, _root: &str) -> String {
    // CloudFront returns FunctionSummary as the root for Create/Describe/
    // Update/Publish — there is no operation-specific wrapper element.
    let mut out = String::with_capacity(512);
    out.push_str(XML_DECL);
    out.push_str(&render_function_summary_inner_with_ns(f));
    out
}

fn render_function_summary_inner_with_ns(f: &StoredFunction) -> String {
    let mut out = String::with_capacity(512);
    out.push_str(&format!("<FunctionSummary xmlns=\"{NS}\">"));
    out.push_str(&render_function_summary_body(f));
    out.push_str("</FunctionSummary>");
    out
}

fn render_function_summary_inner(f: &StoredFunction) -> String {
    let mut out = String::with_capacity(512);
    out.push_str("<FunctionSummary>");
    out.push_str(&render_function_summary_body(f));
    out.push_str("</FunctionSummary>");
    out
}

fn render_function_summary_body(f: &StoredFunction) -> String {
    let mut out = String::with_capacity(512);
    out.push_str(&format!("<Name>{}</Name>", esc(&f.name)));
    out.push_str(&format!("<Status>{}</Status>", esc(&f.status)));
    out.push_str("<FunctionConfig>");
    if let Some(c) = &f.config.comment {
        out.push_str(&format!("<Comment>{}</Comment>", esc(c)));
    } else {
        out.push_str("<Comment></Comment>");
    }
    out.push_str(&format!("<Runtime>{}</Runtime>", esc(&f.config.runtime)));
    out.push_str("</FunctionConfig>");
    out.push_str("<FunctionMetadata>");
    out.push_str(&format!(
        "<FunctionARN>{}</FunctionARN>",
        esc(&f.function_arn)
    ));
    out.push_str(&format!("<Stage>{}</Stage>", esc(&f.stage)));
    out.push_str(&format!(
        "<CreatedTime>{}</CreatedTime>",
        rfc3339(&f.created_time)
    ));
    out.push_str(&format!(
        "<LastModifiedTime>{}</LastModifiedTime>",
        rfc3339(&f.last_modified_time)
    ));
    out.push_str("</FunctionMetadata>");
    out
}

fn render_public_key(p: &StoredPublicKey, root: &str) -> String {
    let mut out = String::with_capacity(512);
    out.push_str(XML_DECL);
    out.push_str(&format!("<{root} xmlns=\"{NS}\">"));
    out.push_str(&format!("<Id>{}</Id>", esc(&p.id)));
    out.push_str(&format!(
        "<CreatedTime>{}</CreatedTime>",
        rfc3339(&p.created_time)
    ));
    out.push_str("<PublicKeyConfig>");
    out.push_str(&format!(
        "<CallerReference>{}</CallerReference>",
        esc(&p.config.caller_reference)
    ));
    out.push_str(&format!("<Name>{}</Name>", esc(&p.config.name)));
    out.push_str(&format!(
        "<EncodedKey>{}</EncodedKey>",
        esc(&p.config.encoded_key)
    ));
    if let Some(c) = &p.config.comment {
        out.push_str(&format!("<Comment>{}</Comment>", esc(c)));
    }
    out.push_str("</PublicKeyConfig>");
    out.push_str(&format!("</{root}>"));
    out
}

fn push_key_group_inner(out: &mut String, g: &StoredKeyGroup) {
    out.push_str(&format!("<Id>{}</Id>", esc(&g.id)));
    out.push_str(&format!(
        "<LastModifiedTime>{}</LastModifiedTime>",
        rfc3339(&g.last_modified_time)
    ));
    out.push_str("<KeyGroupConfig>");
    out.push_str(&format!("<Name>{}</Name>", esc(&g.config.name)));
    out.push_str("<Items>");
    for k in &g.config.items.public_key {
        out.push_str(&format!("<PublicKey>{}</PublicKey>", esc(k)));
    }
    out.push_str("</Items>");
    if let Some(c) = &g.config.comment {
        out.push_str(&format!("<Comment>{}</Comment>", esc(c)));
    }
    out.push_str("</KeyGroupConfig>");
}

fn render_key_group(g: &StoredKeyGroup, root: &str) -> String {
    let mut out = String::with_capacity(512);
    out.push_str(XML_DECL);
    out.push_str(&format!("<{root} xmlns=\"{NS}\">"));
    push_key_group_inner(&mut out, g);
    out.push_str(&format!("</{root}>"));
    out
}

fn push_kvs_inner(out: &mut String, kvs: &StoredKeyValueStore) {
    out.push_str(&format!("<Name>{}</Name>", esc(&kvs.name)));
    out.push_str(&format!("<Id>{}</Id>", esc(&kvs.id)));
    out.push_str(&format!(
        "<Comment>{}</Comment>",
        esc(kvs.comment.as_deref().unwrap_or(""))
    ));
    out.push_str(&format!("<ARN>{}</ARN>", esc(&kvs.arn)));
    out.push_str(&format!("<Status>{}</Status>", esc(&kvs.status)));
    out.push_str(&format!(
        "<LastModifiedTime>{}</LastModifiedTime>",
        rfc3339(&kvs.last_modified_time)
    ));
}

fn render_key_value_store(kvs: &StoredKeyValueStore, _root: &str) -> String {
    // SDK expects KeyValueStore as root for Create/Describe/Update.
    let mut out = String::with_capacity(512);
    out.push_str(XML_DECL);
    out.push_str(&format!("<KeyValueStore xmlns=\"{NS}\">"));
    push_kvs_inner(&mut out, kvs);
    out.push_str("</KeyValueStore>");
    out
}

fn render_oai(oai: &StoredOriginAccessIdentity, root: &str) -> String {
    let mut out = String::with_capacity(512);
    out.push_str(XML_DECL);
    out.push_str(&format!("<{root} xmlns=\"{NS}\">"));
    out.push_str(&format!("<Id>{}</Id>", esc(&oai.id)));
    out.push_str(&format!(
        "<S3CanonicalUserId>{}</S3CanonicalUserId>",
        esc(&oai.s3_canonical_user_id)
    ));
    out.push_str("<CloudFrontOriginAccessIdentityConfig>");
    out.push_str(&format!(
        "<CallerReference>{}</CallerReference>",
        esc(&oai.config.caller_reference)
    ));
    out.push_str(&format!("<Comment>{}</Comment>", esc(&oai.config.comment)));
    out.push_str("</CloudFrontOriginAccessIdentityConfig>");
    out.push_str(&format!("</{root}>"));
    out
}

fn render_monitoring(m: &StoredMonitoringSubscription) -> String {
    let mut out = String::with_capacity(256);
    out.push_str(XML_DECL);
    out.push_str(&format!("<MonitoringSubscription xmlns=\"{NS}\">"));
    out.push_str("<RealtimeMetricsSubscriptionConfig>");
    out.push_str(&format!(
        "<RealtimeMetricsSubscriptionStatus>{}</RealtimeMetricsSubscriptionStatus>",
        esc(&m.config.realtime_metrics_subscription_status)
    ));
    out.push_str("</RealtimeMetricsSubscriptionConfig>");
    out.push_str("</MonitoringSubscription>");
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::service::CloudFrontService;
    use crate::state::CloudFrontAccounts;
    use bytes::Bytes;
    use fakecloud_core::service::AwsService;
    use http::HeaderValue;
    use parking_lot::RwLock;
    use std::sync::Arc;

    fn svc() -> CloudFrontService {
        CloudFrontService::new(Arc::new(RwLock::new(CloudFrontAccounts::new())))
    }

    fn req(method: http::Method, path: &str, body: &str, if_match: Option<&str>) -> AwsRequest {
        let mut headers = HeaderMap::new();
        if let Some(v) = if_match {
            headers.insert(http::header::IF_MATCH, HeaderValue::from_str(v).unwrap());
        }
        AwsRequest {
            service: "cloudfront".into(),
            action: String::new(),
            region: "us-east-1".into(),
            account_id: DEFAULT_ACCOUNT.into(),
            request_id: uuid::Uuid::new_v4().to_string(),
            headers,
            query_params: std::collections::HashMap::new(),
            body_stream: parking_lot::Mutex::new(None),
            body: Bytes::from(body.to_string()),
            path_segments: path
                .split('/')
                .filter(|s| !s.is_empty())
                .map(String::from)
                .collect(),
            raw_path: path.into(),
            raw_query: String::new(),
            method,
            is_query_protocol: false,
            access_key_id: None,
            principal: None,
        }
    }

    async fn create_function(svc: &CloudFrontService, name: &str, code: &str) -> String {
        let code_b64 = base64::engine::general_purpose::STANDARD.encode(code.as_bytes());
        let body = format!(
            r#"<?xml version="1.0"?>
<CreateFunctionRequest xmlns="{NS}">
  <Name>{name}</Name>
  <FunctionConfig>
    <Comment>t</Comment>
    <Runtime>cloudfront-js-2.0</Runtime>
  </FunctionConfig>
  <FunctionCode>{code_b64}</FunctionCode>
</CreateFunctionRequest>"#
        );
        let resp = svc
            .handle(req(http::Method::POST, "/2020-05-31/function", &body, None))
            .await
            .unwrap();
        assert_eq!(resp.status, StatusCode::CREATED);
        resp.headers
            .get(http::header::ETAG)
            .unwrap()
            .to_str()
            .unwrap()
            .to_string()
    }

    fn test_function_request_xml(event_json: &str) -> String {
        test_function_request_xml_with_stage(event_json, "DEVELOPMENT")
    }

    fn test_function_request_xml_with_stage(event_json: &str, stage: &str) -> String {
        let event_b64 = base64::engine::general_purpose::STANDARD.encode(event_json.as_bytes());
        format!(
            r#"<?xml version="1.0"?>
<TestFunctionRequest xmlns="{NS}">
  <Stage>{stage}</Stage>
  <EventObject>{event_b64}</EventObject>
</TestFunctionRequest>"#
        )
    }

    async fn update_function(
        svc: &CloudFrontService,
        name: &str,
        code: &str,
        if_match: &str,
    ) -> String {
        let code_b64 = base64::engine::general_purpose::STANDARD.encode(code.as_bytes());
        let body = format!(
            r#"<?xml version="1.0"?>
<UpdateFunctionRequest xmlns="{NS}">
  <FunctionConfig>
    <Comment>t</Comment>
    <Runtime>cloudfront-js-2.0</Runtime>
  </FunctionConfig>
  <FunctionCode>{code_b64}</FunctionCode>
</UpdateFunctionRequest>"#
        );
        let resp = svc
            .handle(req(
                http::Method::PUT,
                &format!("/2020-05-31/function/{name}"),
                &body,
                Some(if_match),
            ))
            .await
            .unwrap();
        assert_eq!(resp.status, StatusCode::OK);
        resp.headers
            .get(http::header::ETAG)
            .unwrap()
            .to_str()
            .unwrap()
            .to_string()
    }

    async fn publish_function(svc: &CloudFrontService, name: &str, if_match: &str) -> String {
        let resp = svc
            .handle(req(
                http::Method::POST,
                &format!("/2020-05-31/function/{name}/publish"),
                "",
                Some(if_match),
            ))
            .await
            .unwrap();
        assert_eq!(resp.status, StatusCode::OK);
        resp.headers
            .get(http::header::ETAG)
            .unwrap()
            .to_str()
            .unwrap()
            .to_string()
    }

    #[tokio::test]
    async fn test_function_executes_handler_and_returns_result() {
        let svc = svc();
        let etag = create_function(
            &svc,
            "fn-ok",
            r#"function handler(event) { event.headers.x = "y"; return event; }"#,
        )
        .await;
        let body = test_function_request_xml(r#"{"headers":{}}"#);
        let resp = svc
            .handle(req(
                http::Method::POST,
                "/2020-05-31/function/fn-ok/test",
                &body,
                Some(&etag),
            ))
            .await
            .unwrap();
        assert_eq!(resp.status, StatusCode::OK);
        let xml = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
        assert!(
            xml.contains("&quot;x&quot;:&quot;y&quot;"),
            "expected x:y in FunctionOutput, got {xml}"
        );
        assert!(xml.contains("<FunctionErrorMessage></FunctionErrorMessage>"));
    }

    #[tokio::test]
    async fn test_function_propagates_js_error_into_message() {
        let svc = svc();
        let etag = create_function(
            &svc,
            "fn-err",
            r#"function handler() { throw new Error("boom"); }"#,
        )
        .await;
        let body = test_function_request_xml("{}");
        let resp = svc
            .handle(req(
                http::Method::POST,
                "/2020-05-31/function/fn-err/test",
                &body,
                Some(&etag),
            ))
            .await
            .unwrap();
        assert_eq!(resp.status, StatusCode::OK);
        let xml = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
        assert!(
            xml.contains("boom"),
            "expected boom in error msg, got {xml}"
        );
        assert!(xml.contains("<FunctionOutput></FunctionOutput>"));
    }

    #[tokio::test]
    async fn test_function_unknown_name_returns_error() {
        let svc = svc();
        let body = test_function_request_xml("{}");
        let err = match svc
            .handle(req(
                http::Method::POST,
                "/2020-05-31/function/missing/test",
                &body,
                Some("E0"),
            ))
            .await
        {
            Err(e) => e,
            Ok(_) => panic!("expected NoSuchFunctionExists, got Ok"),
        };
        assert_eq!(err.status(), StatusCode::NOT_FOUND);
        assert_eq!(err.code(), "NoSuchFunctionExists");
    }

    #[tokio::test]
    async fn test_function_modifies_aws_request_shape() {
        // Mirrors the canonical CloudFront Functions example from the
        // AWS docs: handler rewrites a request header and returns the
        // request, fakecloud passes the JSON shape straight through.
        let svc = svc();
        let etag = create_function(
            &svc,
            "fn-aws-shape",
            r#"function handler(event) { event.request.headers["x-foo"] = {value: "bar"}; return event.request; }"#,
        )
        .await;
        let body = test_function_request_xml(
            r#"{"version":"1.0","context":{},"viewer":{},"request":{"method":"GET","uri":"/","querystring":{},"headers":{},"cookies":{}}}"#,
        );
        let resp = svc
            .handle(req(
                http::Method::POST,
                "/2020-05-31/function/fn-aws-shape/test",
                &body,
                Some(&etag),
            ))
            .await
            .unwrap();
        assert_eq!(resp.status, StatusCode::OK);
        let xml = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
        assert!(
            xml.contains("x-foo"),
            "expected request header rewrite in output, got {xml}"
        );
        assert!(
            xml.contains("bar"),
            "expected header value in output, got {xml}"
        );
        assert!(
            xml.contains("<FunctionErrorMessage></FunctionErrorMessage>"),
            "expected empty error, got {xml}"
        );
    }

    #[tokio::test]
    async fn test_function_logs_error_and_marks_compute_over_100() {
        let svc = svc();
        let etag = create_function(
            &svc,
            "fn-throws",
            r#"function handler() { throw new Error("kaboom"); }"#,
        )
        .await;
        let body = test_function_request_xml("{}");
        let resp = svc
            .handle(req(
                http::Method::POST,
                "/2020-05-31/function/fn-throws/test",
                &body,
                Some(&etag),
            ))
            .await
            .unwrap();
        assert_eq!(resp.status, StatusCode::OK);
        let xml = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
        // Error appears in both the dedicated message and in the logs
        assert!(xml.contains("kaboom"), "expected kaboom in body, got {xml}");
        assert!(
            xml.contains("<FunctionExecutionLogs>")
                && xml.contains("<member>ERROR: ")
                && xml.contains("kaboom"),
            "expected error log line, got {xml}"
        );
        // ComputeUtilization is rendered as a plain integer; on failure
        // we saturate past 100.
        let cu_open = xml.find("<ComputeUtilization>").unwrap() + "<ComputeUtilization>".len();
        let cu_close = xml.find("</ComputeUtilization>").unwrap();
        let pct: u32 = xml[cu_open..cu_close].parse().unwrap();
        assert!(pct > 100, "expected pct > 100 on error, got {pct}");
    }

    #[tokio::test]
    async fn test_function_stage_selects_published_or_development_code() {
        // Publish freezes a copy of the code; subsequent UpdateFunction
        // mutates DEVELOPMENT but leaves LIVE pinned to the published
        // snapshot. Each stage's TestFunction must run the matching
        // version.
        let svc = svc();
        let etag =
            create_function(&svc, "fn-stage", r#"function handler() { return "v1"; }"#).await;
        let pub_etag = publish_function(&svc, "fn-stage", &etag).await;
        let _new_etag = update_function(
            &svc,
            "fn-stage",
            r#"function handler() { return "v2"; }"#,
            &pub_etag,
        )
        .await;

        // DEVELOPMENT runs v2 (the latest update body).
        let dev_body = test_function_request_xml_with_stage("{}", "DEVELOPMENT");
        let resp = svc
            .handle(req(
                http::Method::POST,
                "/2020-05-31/function/fn-stage/test",
                &dev_body,
                Some("E_NOT_MATCHING"),
            ))
            .await;
        // We deliberately allow stale If-Match here so we exercise the
        // stage path; the precondition fires before we get to JS, so
        // grab the latest etag and retry.
        assert!(resp.is_err(), "stale If-Match must be rejected");
        let described = svc
            .handle(req(
                http::Method::GET,
                "/2020-05-31/function/fn-stage",
                "",
                None,
            ))
            .await
            .unwrap();
        let live_etag = described
            .headers
            .get(http::header::ETAG)
            .unwrap()
            .to_str()
            .unwrap()
            .to_string();

        let dev_resp = svc
            .handle(req(
                http::Method::POST,
                "/2020-05-31/function/fn-stage/test",
                &dev_body,
                Some(&live_etag),
            ))
            .await
            .unwrap();
        let dev_xml = std::str::from_utf8(dev_resp.body.expect_bytes()).unwrap();
        assert!(
            dev_xml.contains("&quot;v2&quot;"),
            "DEVELOPMENT should run latest update (v2), got {dev_xml}"
        );

        // LIVE runs v1 (the published snapshot, not affected by the
        // post-publish update).
        let live_body = test_function_request_xml_with_stage("{}", "LIVE");
        let live_resp = svc
            .handle(req(
                http::Method::POST,
                "/2020-05-31/function/fn-stage/test",
                &live_body,
                Some(&live_etag),
            ))
            .await
            .unwrap();
        let live_xml = std::str::from_utf8(live_resp.body.expect_bytes()).unwrap();
        assert!(
            live_xml.contains("&quot;v1&quot;"),
            "LIVE should run published snapshot (v1), got {live_xml}"
        );
    }

    #[tokio::test]
    async fn test_function_infinite_loop_is_killed() {
        let svc = svc();
        let etag = create_function(&svc, "fn-loop", r#"function handler() { while(1){} }"#).await;
        let body = test_function_request_xml("{}");
        let resp = svc
            .handle(req(
                http::Method::POST,
                "/2020-05-31/function/fn-loop/test",
                &body,
                Some(&etag),
            ))
            .await
            .unwrap();
        assert_eq!(resp.status, StatusCode::OK);
        let xml = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
        assert!(
            xml.contains("<FunctionOutput></FunctionOutput>"),
            "expected empty output, got {xml}"
        );
        assert!(
            xml.contains("ERROR:") && xml.contains("limit"),
            "expected timeout/limit error in logs, got {xml}"
        );
        let cu_open = xml.find("<ComputeUtilization>").unwrap() + "<ComputeUtilization>".len();
        let cu_close = xml.find("</ComputeUtilization>").unwrap();
        let pct: u32 = xml[cu_open..cu_close].parse().unwrap();
        assert!(pct > 100, "expected pct > 100 after kill, got {pct}");
    }
}
