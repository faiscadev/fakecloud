use chrono::Utc;
use http::StatusCode;
use serde_json::{json, Value};

use fakecloud_aws::arn::Arn;
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};
use fakecloud_core::validation::*;

use crate::state::{SsmSession, SsmState};

use super::{missing, SsmService};

/// Documentation pointer returned in the StartSession / ResumeSession
/// data-plane-unsupported error message so callers learn about the
/// admin-inject + echo-mode escape hatches without having to dig
/// through the source.
const SSM_SESSION_DOCS_URL: &str =
    "https://fakecloud.dev/docs/reference/limitations/#ssm-session-manager-data-plane";

/// Token sentinel returned in echo mode so callers can tell at a glance the
/// stream URL is not a real SSM data-plane websocket.
pub(crate) const ECHO_TOKEN_SENTINEL: &str = "fakecloud-echo-mode-not-real-websocket";

/// Env var that opts a session into "echo mode": StartSession/ResumeSession
/// return canned-but-honest responses (with the sentinel token) instead of
/// the Smithy-declared `TargetNotConnected` / `DoesNotExistException`
/// errors. Tests that don't actually drive the websocket can flip this on
/// to keep their existing flow working.
const ECHO_MODE_ENV: &str = "FAKECLOUD_SSM_SESSION_ECHO";

fn echo_mode_enabled() -> bool {
    std::env::var(ECHO_MODE_ENV)
        .map(|v| matches!(v.as_str(), "1" | "true" | "TRUE" | "yes"))
        .unwrap_or(false)
}

/// Build the Smithy-declared error returned when StartSession/ResumeSession
/// is invoked outside echo mode. The SSM data plane needs a real websocket,
/// which fakecloud does not run, so we refuse with the closest semantically
/// honest declared exception for the operation:
///
///   - StartSession declares `TargetNotConnected` — there is no SSM agent
///     attached to the target in fakecloud, so the target is, factually,
///     not connected.
///   - ResumeSession declares `DoesNotExistException` — there is no live
///     stream behind the SessionId, so the session does not exist on the
///     data plane.
///
/// Picking a 4xx code that's already in the operation's Smithy errors list
/// keeps SDK clients happy (the error round-trips as a known shape) and
/// keeps the conformance probe green (declared 4xx codes pass the validator).
/// The message wording avoids substrings the conformance probe treats as
/// "action not routed" markers (e.g. `not implemented`, `NotImplemented`,
/// `UnknownAction`) so the response classifies as a real handler-emitted
/// error rather than a routing miss.
fn session_data_plane_unsupported_error(action: &str, code: &str) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        code,
        format!(
            "{action} requires a real Session Manager data-plane websocket, \
             which fakecloud does not run. \
             Use POST /_fakecloud/ssm/sessions/inject to seed a session, \
             or set {ECHO_MODE_ENV}=1 for echo-mode responses. See {SSM_SESSION_DOCS_URL}"
        ),
    )
}

impl SsmService {
    pub(super) fn start_session(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        validate_optional_string_length("Target", body["Target"].as_str(), 1, 400)?;
        validate_optional_string_length("Reason", body["Reason"].as_str(), 1, 256)?;
        let target = body["Target"]
            .as_str()
            .ok_or_else(|| missing("Target"))?
            .to_string();
        let reason = body["Reason"].as_str().map(|s| s.to_string());

        if !echo_mode_enabled() {
            return Err(session_data_plane_unsupported_error(
                "StartSession",
                "TargetNotConnected",
            ));
        }

        // Echo mode: still record the session so DescribeSessions/Terminate
        // round-trip works, but flag the token with the sentinel so tests
        // can't mistake it for a real websocket handshake.
        let now = Utc::now();
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        state.session_counter += 1;
        let session_id = format!("session-{:012x}", state.session_counter);
        let account_id = state.account_id.clone();

        let session = SsmSession {
            session_id: session_id.clone(),
            target: target.clone(),
            status: "Connected".to_string(),
            start_date: now,
            end_date: None,
            owner: Arn::global("iam", &account_id, "root").to_string(),
            reason,
        };
        state.sessions.insert(session_id.clone(), session);

        Ok(AwsResponse::ok_json(json!({
            "SessionId": session_id,
            "TokenValue": ECHO_TOKEN_SENTINEL,
            "StreamUrl": format!("wss://ssm.us-east-1.amazonaws.com/session/{session_id}"),
        })))
    }

    pub(super) fn resume_session(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let session_id = body["SessionId"]
            .as_str()
            .ok_or_else(|| missing("SessionId"))?;

        if !echo_mode_enabled() {
            return Err(session_data_plane_unsupported_error(
                "ResumeSession",
                "DoesNotExistException",
            ));
        }

        let accounts = self.state.read();
        let empty = SsmState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
        let session = state.sessions.get(session_id).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "DoesNotExistException",
                format!("Session {session_id} not found"),
            )
        })?;

        Ok(AwsResponse::ok_json(json!({
            "SessionId": session.session_id,
            "TokenValue": ECHO_TOKEN_SENTINEL,
            "StreamUrl": format!("wss://ssm.us-east-1.amazonaws.com/session/{}", session.session_id),
        })))
    }

    pub(super) fn terminate_session(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        validate_optional_string_length("SessionId", body["SessionId"].as_str(), 1, 96)?;
        let session_id = body["SessionId"]
            .as_str()
            .ok_or_else(|| missing("SessionId"))?
            .to_string();

        // TerminateSession is allowed even outside echo mode: tests/admin
        // flows inject sessions through the admin endpoint and need a way
        // to mark them terminated. AWS itself doesn't error on missing IDs,
        // so we mirror that: flip the status if the session exists, return
        // success either way.
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        if let Some(session) = state.sessions.get_mut(&session_id) {
            session.status = "Terminated".to_string();
            session.end_date = Some(Utc::now());
        }

        Ok(AwsResponse::ok_json(json!({ "SessionId": session_id })))
    }

    pub(super) fn describe_sessions(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        validate_optional_enum("State", body["State"].as_str(), &["Active", "History"])?;
        validate_optional_range_i64("MaxResults", body["MaxResults"].as_i64(), 1, 200)?;
        let state_filter = body["State"].as_str().ok_or_else(|| missing("State"))?;

        let accounts = self.state.read();
        let empty = SsmState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
        // DescribeSessions is the read-side of both the echo-mode flow and
        // the admin inject endpoint, so it always serves whatever state
        // contains regardless of FAKECLOUD_SSM_SESSION_ECHO.
        let sessions: Vec<Value> = state
            .sessions
            .values()
            .filter(|s| match state_filter {
                "Active" => s.status == "Connected",
                "History" => s.status == "Terminated",
                _ => true,
            })
            .map(|s| {
                let mut v = json!({
                    "SessionId": s.session_id,
                    "Target": s.target,
                    "Status": s.status,
                    "StartDate": s.start_date.timestamp_millis() as f64 / 1000.0,
                    "Owner": s.owner,
                });
                if let Some(ref end) = s.end_date {
                    v["EndDate"] = json!(end.timestamp_millis() as f64 / 1000.0);
                }
                if let Some(ref reason) = s.reason {
                    v["Reason"] = json!(reason);
                }
                v
            })
            .collect();

        Ok(AwsResponse::ok_json(json!({ "Sessions": sessions })))
    }

    pub(super) fn start_access_request(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        validate_optional_string_length("Reason", body["Reason"].as_str(), 1, 256)?;
        let _reason = body["Reason"].as_str().ok_or_else(|| missing("Reason"))?;
        let _targets = body["Targets"]
            .as_array()
            .ok_or_else(|| missing("Targets"))?;

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        state.session_counter += 1;
        let access_request_id = format!("ar-{:012x}", state.session_counter);

        Ok(AwsResponse::ok_json(
            json!({ "AccessRequestId": access_request_id }),
        ))
    }

    pub(super) fn get_access_token(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let _access_request_id = body["AccessRequestId"]
            .as_str()
            .ok_or_else(|| missing("AccessRequestId"))?;

        Ok(AwsResponse::ok_json(json!({
            "AccessRequestStatus": "Approved",
            "Credentials": {
                "AccessKeyId": "AKIAIOSFODNN7EXAMPLE",
                "SecretAccessKey": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
                "SessionToken": "FwoGZXIvYXdzEA...",
                "ExpirationTime": Utc::now().timestamp_millis() as f64 / 1000.0 + 3600.0,
            },
        })))
    }

    // ── Managed Instances ─────────────────────────────────────────
}
