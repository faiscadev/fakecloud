// Handlers for CloudFront ConnectionFunction ops (8 ops). Mirrors the
// regular CloudFront Functions lifecycle: create -> describe/get ->
// update -> publish -> attach. Code blob is base64-encoded on the
// wire, returned raw for GetConnectionFunction.

use base64::Engine;
use chrono::Utc;
use http::header::ETAG;
use http::{HeaderMap, StatusCode};

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError, ResponseBody};

use crate::cfunctions::StoredConnectionFunction;
use crate::policies::{
    not_found, precondition_failed, require_if_match, rfc3339, route_id, xml_with_etag,
};
use crate::router::Route;
use crate::service::{
    aws_error, esc, extract_body_field, generate_id_with_prefix, invalid_argument, xml_response,
    CloudFrontService, DEFAULT_ACCOUNT,
};
use crate::xml_io;

const NS: &str = crate::NAMESPACE;
const XML_DECL: &str = r#"<?xml version="1.0" encoding="UTF-8"?>"#;

#[derive(Debug, Default, serde::Deserialize)]
#[serde(rename_all = "PascalCase")]
struct CreateConnectionFunctionRequest {
    pub name: String,
    pub connection_function_config: ConnectionFunctionConfigInput,
    pub connection_function_code: String,
}

#[derive(Debug, Default, serde::Deserialize)]
#[serde(rename_all = "PascalCase")]
struct UpdateConnectionFunctionRequest {
    pub connection_function_config: ConnectionFunctionConfigInput,
    pub connection_function_code: String,
}

#[derive(Debug, Default, serde::Deserialize)]
#[serde(rename_all = "PascalCase")]
struct ConnectionFunctionConfigInput {
    pub comment: String,
    pub runtime: String,
}

impl CloudFrontService {
    pub(crate) fn create_connection_function(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let parsed: CreateConnectionFunctionRequest =
            xml_io::from_xml_root(&req.body).map_err(|e| {
                invalid_argument(format!("invalid CreateConnectionFunctionRequest XML: {e}"))
            })?;
        if parsed.name.is_empty() {
            return Err(invalid_argument("Name is required"));
        }
        let mut state = self.state.write();
        let account = state
            .accounts
            .entry(DEFAULT_ACCOUNT.to_string())
            .or_default();
        if account.connection_functions.contains_key(&parsed.name) {
            return Err(aws_error(
                StatusCode::CONFLICT,
                "EntityAlreadyExists",
                format!("ConnectionFunction {} already exists", parsed.name),
            ));
        }
        let now = Utc::now();
        let etag = generate_id_with_prefix("E");
        let id = generate_id_with_prefix("CF");
        let arn = format!(
            "arn:aws:cloudfront::{}:connection-function/{}",
            DEFAULT_ACCOUNT, parsed.name
        );
        let code = base64::engine::general_purpose::STANDARD
            .decode(parsed.connection_function_code.trim())
            .unwrap_or_else(|_| parsed.connection_function_code.into_bytes());
        let stored = StoredConnectionFunction {
            id,
            name: parsed.name.clone(),
            arn,
            stage: "DEVELOPMENT".to_string(),
            status: "UNPUBLISHED".to_string(),
            runtime: parsed.connection_function_config.runtime,
            comment: parsed.connection_function_config.comment,
            code,
            live_code: None,
            etag: etag.clone(),
            created_time: now,
            last_modified_time: now,
        };
        account
            .connection_functions
            .insert(parsed.name.clone(), stored.clone());
        drop(state);
        let body = render_connection_function_summary(&stored, true);
        Ok(xml_with_etag(StatusCode::CREATED, body, &etag, None))
    }

    pub(crate) fn describe_connection_function(
        &self,
        route: &Route,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = route_id(route, "ConnectionFunction")?;
        let state = self.state.read();
        let f = state
            .accounts
            .get(DEFAULT_ACCOUNT)
            .and_then(|a| a.connection_functions.get(&name).cloned())
            .ok_or_else(|| not_found("ConnectionFunction", &name))?;
        drop(state);
        let body = render_connection_function_summary(&f, true);
        Ok(xml_with_etag(StatusCode::OK, body, &f.etag, None))
    }

    pub(crate) fn get_connection_function(
        &self,
        route: &Route,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = route_id(route, "ConnectionFunction")?;
        let state = self.state.read();
        let f = state
            .accounts
            .get(DEFAULT_ACCOUNT)
            .and_then(|a| a.connection_functions.get(&name).cloned())
            .ok_or_else(|| not_found("ConnectionFunction", &name))?;
        drop(state);
        let mut headers = HeaderMap::new();
        if let Ok(v) = http::HeaderValue::from_str(&f.etag) {
            headers.insert(ETAG, v);
        }
        Ok(AwsResponse {
            status: StatusCode::OK,
            headers,
            content_type: "application/octet-stream".to_string(),
            body: ResponseBody::Bytes(bytes::Bytes::from(f.code.clone())),
        })
    }

    pub(crate) fn update_connection_function(
        &self,
        req: &AwsRequest,
        route: &Route,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = route_id(route, "ConnectionFunction")?;
        let if_match = require_if_match(req)?;
        let parsed: UpdateConnectionFunctionRequest =
            xml_io::from_xml_root(&req.body).map_err(|e| {
                invalid_argument(format!("invalid UpdateConnectionFunctionRequest XML: {e}"))
            })?;
        let mut state = self.state.write();
        let account = state
            .accounts
            .get_mut(DEFAULT_ACCOUNT)
            .ok_or_else(|| not_found("ConnectionFunction", &name))?;
        let f = account
            .connection_functions
            .get_mut(&name)
            .ok_or_else(|| not_found("ConnectionFunction", &name))?;
        if f.etag != if_match {
            return Err(precondition_failed());
        }
        f.runtime = parsed.connection_function_config.runtime;
        f.comment = parsed.connection_function_config.comment;
        f.code = base64::engine::general_purpose::STANDARD
            .decode(parsed.connection_function_code.trim())
            .unwrap_or_else(|_| parsed.connection_function_code.into_bytes());
        f.etag = generate_id_with_prefix("E");
        f.last_modified_time = Utc::now();
        f.status = "UNPUBLISHED".to_string();
        f.stage = "DEVELOPMENT".to_string();
        let snap = f.clone();
        drop(state);
        let body = render_connection_function_summary(&snap, true);
        Ok(xml_with_etag(StatusCode::OK, body, &snap.etag, None))
    }

    pub(crate) fn delete_connection_function(
        &self,
        req: &AwsRequest,
        route: &Route,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = route_id(route, "ConnectionFunction")?;
        let if_match = require_if_match(req)?;
        let mut state = self.state.write();
        let account = state
            .accounts
            .get_mut(DEFAULT_ACCOUNT)
            .ok_or_else(|| not_found("ConnectionFunction", &name))?;
        let f = account
            .connection_functions
            .get(&name)
            .ok_or_else(|| not_found("ConnectionFunction", &name))?;
        if f.etag != if_match {
            return Err(precondition_failed());
        }
        account.connection_functions.remove(&name);
        drop(state);
        Ok(crate::policies::empty(StatusCode::NO_CONTENT))
    }

    pub(crate) fn list_connection_functions(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        // ListConnectionFunctions accepts an optional Stage filter constrained
        // to the FunctionStage enum (DEVELOPMENT | LIVE). Reject unknown
        // values up-front so conformance negative variants surface an
        // InvalidArgument (declared in the op's Smithy error list) instead of
        // silently returning the full unfiltered list.
        if let Some(stage) = extract_body_field(&req.body, "Stage") {
            if stage != "DEVELOPMENT" && stage != "LIVE" {
                return Err(crate::service::invalid_argument(format!(
                    "Stage must be one of 'DEVELOPMENT' or 'LIVE', got '{stage}'"
                )));
            }
        }
        let state = self.state.read();
        let mut items: Vec<StoredConnectionFunction> = state
            .accounts
            .get(DEFAULT_ACCOUNT)
            .map(|a| a.connection_functions.values().cloned().collect())
            .unwrap_or_default();
        drop(state);
        items.sort_by(|a, b| a.name.cmp(&b.name));
        let mut body = String::with_capacity(512);
        body.push_str(XML_DECL);
        body.push_str(&format!("<ListConnectionFunctionsResult xmlns=\"{NS}\">"));
        body.push_str("<NextMarker></NextMarker>");
        body.push_str("<ConnectionFunctions>");
        for f in &items {
            body.push_str(&render_connection_function_summary_inner(f));
        }
        body.push_str("</ConnectionFunctions>");
        body.push_str("</ListConnectionFunctionsResult>");
        Ok(xml_response(StatusCode::OK, body, HeaderMap::new()))
    }

    pub(crate) fn publish_connection_function(
        &self,
        req: &AwsRequest,
        route: &Route,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = route_id(route, "ConnectionFunction")?;
        let if_match = require_if_match(req)?;
        let mut state = self.state.write();
        let account = state
            .accounts
            .get_mut(DEFAULT_ACCOUNT)
            .ok_or_else(|| not_found("ConnectionFunction", &name))?;
        let f = account
            .connection_functions
            .get_mut(&name)
            .ok_or_else(|| not_found("ConnectionFunction", &name))?;
        if f.etag != if_match {
            return Err(precondition_failed());
        }
        f.status = "DEPLOYED".to_string();
        f.stage = "LIVE".to_string();
        f.last_modified_time = Utc::now();
        // Freeze the current development bytes as the LIVE snapshot so
        // TestConnectionFunction(Stage=LIVE) keeps the published
        // behaviour stable across subsequent UpdateConnectionFunction
        // calls.
        f.live_code = Some(f.code.clone());
        let snap = f.clone();
        drop(state);
        let body = render_connection_function_summary(&snap, true);
        Ok(xml_with_etag(StatusCode::OK, body, &snap.etag, None))
    }

    pub(crate) fn test_connection_function(
        &self,
        req: &AwsRequest,
        route: &Route,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = route_id(route, "ConnectionFunction")?;
        let if_match = require_if_match(req)?;
        let parsed: TestConnectionFunctionRequest =
            xml_io::from_xml_root(&req.body).map_err(|e| {
                invalid_argument(format!("invalid TestConnectionFunctionRequest XML: {e}"))
            })?;
        let event_bytes = base64::engine::general_purpose::STANDARD
            .decode(parsed.connection_object.trim().as_bytes())
            .map_err(|e| invalid_argument(format!("ConnectionObject is not valid base64: {e}")))?;
        let state = self.state.read();
        let f = state
            .accounts
            .get(DEFAULT_ACCOUNT)
            .and_then(|a| a.connection_functions.get(&name).cloned())
            .ok_or_else(|| {
                aws_error(
                    StatusCode::NOT_FOUND,
                    "EntityNotFound",
                    format!("ConnectionFunction {name} does not exist"),
                )
            })?;
        drop(state);
        if f.etag != if_match {
            return Err(precondition_failed());
        }
        // Pick the stored bytes that match the requested stage.
        // DEVELOPMENT (the default) is the latest body; LIVE reads the
        // snapshot taken at PublishConnectionFunction. We fall back to
        // `f.code` when no LIVE snapshot exists so unpublished
        // functions are still testable against Stage=LIVE.
        let stage = parsed.stage.as_deref().unwrap_or("DEVELOPMENT");
        let source: &[u8] = if stage.eq_ignore_ascii_case("LIVE") {
            f.live_code.as_deref().unwrap_or(&f.code)
        } else {
            &f.code
        };
        let code = std::str::from_utf8(source)
            .map_err(|e| invalid_argument(format!("function code is not valid UTF-8: {e}")))?;
        let exec = crate::js_runtime::run_handler(code, &event_bytes);

        let mut body = String::with_capacity(1024);
        body.push_str(XML_DECL);
        body.push_str(&format!("<ConnectionFunctionTestResult xmlns=\"{NS}\">"));
        body.push_str(&render_connection_function_summary_inner(&f));
        body.push_str(&format!(
            "<ComputeUtilization>{}</ComputeUtilization>",
            exec.compute_utilization
        ));
        body.push_str("<ConnectionFunctionExecutionLogs>");
        for line in &exec.logs {
            body.push_str(&format!("<member>{}</member>", esc(line)));
        }
        body.push_str("</ConnectionFunctionExecutionLogs>");
        body.push_str(&format!(
            "<ConnectionFunctionErrorMessage>{}</ConnectionFunctionErrorMessage>",
            esc(exec.error.as_deref().unwrap_or(""))
        ));
        body.push_str(&format!(
            "<ConnectionFunctionOutput>{}</ConnectionFunctionOutput>",
            esc(exec.output.as_deref().unwrap_or(""))
        ));
        body.push_str("</ConnectionFunctionTestResult>");
        Ok(xml_response(StatusCode::OK, body, HeaderMap::new()))
    }
}

#[derive(Debug, Default, serde::Deserialize)]
#[serde(rename_all = "PascalCase")]
struct TestConnectionFunctionRequest {
    #[serde(default)]
    connection_object: String,
    #[serde(default)]
    stage: Option<String>,
}

fn render_connection_function_summary(f: &StoredConnectionFunction, with_decl: bool) -> String {
    let mut out = String::with_capacity(512);
    if with_decl {
        out.push_str(XML_DECL);
    }
    out.push_str(&format!("<ConnectionFunctionSummary xmlns=\"{NS}\">"));
    push_summary_body(&mut out, f);
    out.push_str("</ConnectionFunctionSummary>");
    out
}

fn render_connection_function_summary_inner(f: &StoredConnectionFunction) -> String {
    let mut out = String::with_capacity(512);
    out.push_str("<ConnectionFunctionSummary>");
    push_summary_body(&mut out, f);
    out.push_str("</ConnectionFunctionSummary>");
    out
}

fn push_summary_body(out: &mut String, f: &StoredConnectionFunction) {
    out.push_str(&format!("<Name>{}</Name>", esc(&f.name)));
    out.push_str(&format!("<Id>{}</Id>", esc(&f.id)));
    out.push_str(&format!("<Status>{}</Status>", esc(&f.status)));
    out.push_str(&format!(
        "<ConnectionFunctionArn>{}</ConnectionFunctionArn>",
        esc(&f.arn)
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
    out.push_str("<ConnectionFunctionConfig>");
    out.push_str(&format!("<Comment>{}</Comment>", esc(&f.comment)));
    out.push_str(&format!("<Runtime>{}</Runtime>", esc(&f.runtime)));
    out.push_str("</ConnectionFunctionConfig>");
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

    async fn create_cfn(svc: &CloudFrontService, name: &str, code: &str) -> String {
        let code_b64 = base64::engine::general_purpose::STANDARD.encode(code.as_bytes());
        let body = format!(
            r#"<?xml version="1.0"?>
<CreateConnectionFunctionRequest xmlns="{NS}">
  <Name>{name}</Name>
  <ConnectionFunctionConfig>
    <Comment>t</Comment>
    <Runtime>cloudfront-js-2.0</Runtime>
  </ConnectionFunctionConfig>
  <ConnectionFunctionCode>{code_b64}</ConnectionFunctionCode>
</CreateConnectionFunctionRequest>"#
        );
        let resp = svc
            .handle(req(
                http::Method::POST,
                "/2020-05-31/connection-function",
                &body,
                None,
            ))
            .await
            .unwrap();
        assert_eq!(resp.status, StatusCode::CREATED);
        resp.headers
            .get(ETAG)
            .unwrap()
            .to_str()
            .unwrap()
            .to_string()
    }

    fn test_cfn_request_xml(event_json: &str) -> String {
        test_cfn_request_xml_with_stage(event_json, "DEVELOPMENT")
    }

    fn test_cfn_request_xml_with_stage(event_json: &str, stage: &str) -> String {
        let event_b64 = base64::engine::general_purpose::STANDARD.encode(event_json.as_bytes());
        format!(
            r#"<?xml version="1.0"?>
<TestConnectionFunctionRequest xmlns="{NS}">
  <Stage>{stage}</Stage>
  <ConnectionObject>{event_b64}</ConnectionObject>
</TestConnectionFunctionRequest>"#
        )
    }

    async fn update_cfn(svc: &CloudFrontService, name: &str, code: &str, if_match: &str) -> String {
        let code_b64 = base64::engine::general_purpose::STANDARD.encode(code.as_bytes());
        let body = format!(
            r#"<?xml version="1.0"?>
<UpdateConnectionFunctionRequest xmlns="{NS}">
  <ConnectionFunctionConfig>
    <Comment>t</Comment>
    <Runtime>cloudfront-js-2.0</Runtime>
  </ConnectionFunctionConfig>
  <ConnectionFunctionCode>{code_b64}</ConnectionFunctionCode>
</UpdateConnectionFunctionRequest>"#
        );
        let resp = svc
            .handle(req(
                http::Method::PUT,
                &format!("/2020-05-31/connection-function/{name}"),
                &body,
                Some(if_match),
            ))
            .await
            .unwrap();
        assert_eq!(resp.status, StatusCode::OK);
        resp.headers
            .get(ETAG)
            .unwrap()
            .to_str()
            .unwrap()
            .to_string()
    }

    async fn publish_cfn(svc: &CloudFrontService, name: &str, if_match: &str) -> String {
        let resp = svc
            .handle(req(
                http::Method::POST,
                &format!("/2020-05-31/connection-function/{name}/publish"),
                "",
                Some(if_match),
            ))
            .await
            .unwrap();
        assert_eq!(resp.status, StatusCode::OK);
        resp.headers
            .get(ETAG)
            .unwrap()
            .to_str()
            .unwrap()
            .to_string()
    }

    #[tokio::test]
    async fn test_connection_function_executes_handler_and_returns_result() {
        let svc = svc();
        let etag = create_cfn(
            &svc,
            "cfn-ok",
            r#"function handler(event) { event.headers.x = "y"; return event; }"#,
        )
        .await;
        let body = test_cfn_request_xml(r#"{"headers":{}}"#);
        let resp = svc
            .handle(req(
                http::Method::POST,
                "/2020-05-31/connection-function/cfn-ok/test",
                &body,
                Some(&etag),
            ))
            .await
            .unwrap();
        assert_eq!(resp.status, StatusCode::OK);
        let xml = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
        assert!(
            xml.contains("&quot;x&quot;:&quot;y&quot;"),
            "expected x:y in output, got {xml}"
        );
        assert!(
            xml.contains("<ConnectionFunctionErrorMessage></ConnectionFunctionErrorMessage>"),
            "expected empty error, got {xml}"
        );
    }

    #[tokio::test]
    async fn test_connection_function_propagates_js_error_into_message() {
        let svc = svc();
        let etag = create_cfn(
            &svc,
            "cfn-err",
            r#"function handler() { throw new Error("boom"); }"#,
        )
        .await;
        let body = test_cfn_request_xml("{}");
        let resp = svc
            .handle(req(
                http::Method::POST,
                "/2020-05-31/connection-function/cfn-err/test",
                &body,
                Some(&etag),
            ))
            .await
            .unwrap();
        assert_eq!(resp.status, StatusCode::OK);
        let xml = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
        assert!(xml.contains("boom"), "expected boom, got {xml}");
        assert!(xml.contains("<ConnectionFunctionOutput></ConnectionFunctionOutput>"));
    }

    #[tokio::test]
    async fn test_connection_function_unknown_name_returns_error() {
        let svc = svc();
        let body = test_cfn_request_xml("{}");
        let err = match svc
            .handle(req(
                http::Method::POST,
                "/2020-05-31/connection-function/missing/test",
                &body,
                Some("E0"),
            ))
            .await
        {
            Err(e) => e,
            Ok(_) => panic!("expected EntityNotFound, got Ok"),
        };
        assert_eq!(err.status(), StatusCode::NOT_FOUND);
        assert_eq!(err.code(), "EntityNotFound");
    }

    #[tokio::test]
    async fn test_connection_function_logs_error_and_marks_compute_over_100() {
        let svc = svc();
        let etag = create_cfn(
            &svc,
            "cfn-throws",
            r#"function handler() { throw new Error("kaboom"); }"#,
        )
        .await;
        let body = test_cfn_request_xml("{}");
        let resp = svc
            .handle(req(
                http::Method::POST,
                "/2020-05-31/connection-function/cfn-throws/test",
                &body,
                Some(&etag),
            ))
            .await
            .unwrap();
        assert_eq!(resp.status, StatusCode::OK);
        let xml = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
        assert!(xml.contains("kaboom"), "expected kaboom, got {xml}");
        assert!(
            xml.contains("<ConnectionFunctionExecutionLogs>") && xml.contains("ERROR: "),
            "expected error log, got {xml}"
        );
        let cu_open = xml.find("<ComputeUtilization>").unwrap() + "<ComputeUtilization>".len();
        let cu_close = xml.find("</ComputeUtilization>").unwrap();
        let pct: u32 = xml[cu_open..cu_close].parse().unwrap();
        assert!(pct > 100, "expected pct > 100 on error, got {pct}");
    }

    #[tokio::test]
    async fn test_connection_function_stage_selects_published_or_development_code() {
        // ConnectionFunctions mirror Functions: PublishConnectionFunction
        // freezes a LIVE snapshot, UpdateConnectionFunction mutates only
        // DEVELOPMENT, and TestConnectionFunction picks the matching
        // version per Stage.
        let svc = svc();
        let etag = create_cfn(&svc, "cfn-stage", r#"function handler() { return "v1"; }"#).await;
        let pub_etag = publish_cfn(&svc, "cfn-stage", &etag).await;
        let new_etag = update_cfn(
            &svc,
            "cfn-stage",
            r#"function handler() { return "v2"; }"#,
            &pub_etag,
        )
        .await;

        let dev_body = test_cfn_request_xml_with_stage("{}", "DEVELOPMENT");
        let dev_resp = svc
            .handle(req(
                http::Method::POST,
                "/2020-05-31/connection-function/cfn-stage/test",
                &dev_body,
                Some(&new_etag),
            ))
            .await
            .unwrap();
        let dev_xml = std::str::from_utf8(dev_resp.body.expect_bytes()).unwrap();
        assert!(
            dev_xml.contains("&quot;v2&quot;"),
            "DEVELOPMENT should run latest update (v2), got {dev_xml}"
        );

        let live_body = test_cfn_request_xml_with_stage("{}", "LIVE");
        let live_resp = svc
            .handle(req(
                http::Method::POST,
                "/2020-05-31/connection-function/cfn-stage/test",
                &live_body,
                Some(&new_etag),
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
    async fn test_connection_function_infinite_loop_is_killed() {
        let svc = svc();
        let etag = create_cfn(&svc, "cfn-loop", r#"function handler() { while(1){} }"#).await;
        let body = test_cfn_request_xml("{}");
        let resp = svc
            .handle(req(
                http::Method::POST,
                "/2020-05-31/connection-function/cfn-loop/test",
                &body,
                Some(&etag),
            ))
            .await
            .unwrap();
        assert_eq!(resp.status, StatusCode::OK);
        let xml = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
        assert!(
            xml.contains("<ConnectionFunctionOutput></ConnectionFunctionOutput>"),
            "expected empty output after kill, got {xml}"
        );
        assert!(
            xml.contains("ERROR:") && xml.contains("limit"),
            "expected timeout/limit log, got {xml}"
        );
        let cu_open = xml.find("<ComputeUtilization>").unwrap() + "<ComputeUtilization>".len();
        let cu_close = xml.find("</ComputeUtilization>").unwrap();
        let pct: u32 = xml[cu_open..cu_close].parse().unwrap();
        assert!(pct > 100, "expected pct > 100, got {pct}");
    }
}
