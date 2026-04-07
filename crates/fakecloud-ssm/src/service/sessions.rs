use chrono::Utc;
use http::StatusCode;
use serde_json::{json, Value};

use fakecloud_aws::arn::Arn;
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};
use fakecloud_core::validation::*;

use crate::state::SsmSession;

use super::{json_resp, missing, parse_body, SsmService};

impl SsmService {
    pub(super) fn start_session(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        validate_optional_string_length("Target", body["Target"].as_str(), 1, 400)?;
        validate_optional_string_length("Reason", body["Reason"].as_str(), 1, 256)?;
        let target = body["Target"]
            .as_str()
            .ok_or_else(|| missing("Target"))?
            .to_string();
        let reason = body["Reason"].as_str().map(|s| s.to_string());

        let now = Utc::now();
        let mut state = self.state.write();
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

        Ok(json_resp(json!({
            "SessionId": session_id,
            "TokenValue": format!("token-{session_id}"),
            "StreamUrl": format!("wss://ssm.us-east-1.amazonaws.com/session/{session_id}"),
        })))
    }

    pub(super) fn resume_session(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let session_id = body["SessionId"]
            .as_str()
            .ok_or_else(|| missing("SessionId"))?;

        let state = self.state.read();
        let session = state.sessions.get(session_id).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "DoesNotExistException",
                format!("Session {session_id} not found"),
            )
        })?;

        Ok(json_resp(json!({
            "SessionId": session.session_id,
            "TokenValue": format!("token-{}", session.session_id),
            "StreamUrl": format!("wss://ssm.us-east-1.amazonaws.com/session/{}", session.session_id),
        })))
    }

    pub(super) fn terminate_session(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        validate_optional_string_length("SessionId", body["SessionId"].as_str(), 1, 96)?;
        let session_id = body["SessionId"]
            .as_str()
            .ok_or_else(|| missing("SessionId"))?;

        let mut state = self.state.write();
        if let Some(session) = state.sessions.get_mut(session_id) {
            session.status = "Terminated".to_string();
            session.end_date = Some(Utc::now());
        }
        // AWS TerminateSession doesn't error on non-existent sessions

        Ok(json_resp(json!({ "SessionId": session_id })))
    }

    pub(super) fn describe_sessions(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        validate_optional_enum("State", body["State"].as_str(), &["Active", "History"])?;
        validate_optional_range_i64("MaxResults", body["MaxResults"].as_i64(), 1, 200)?;
        let state_filter = body["State"].as_str().ok_or_else(|| missing("State"))?;

        let state = self.state.read();
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

        Ok(json_resp(json!({ "Sessions": sessions })))
    }

    pub(super) fn start_access_request(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        validate_optional_string_length("Reason", body["Reason"].as_str(), 1, 256)?;
        let _reason = body["Reason"].as_str().ok_or_else(|| missing("Reason"))?;
        let _targets = body["Targets"]
            .as_array()
            .ok_or_else(|| missing("Targets"))?;

        let mut state = self.state.write();
        state.session_counter += 1;
        let access_request_id = format!("ar-{:012x}", state.session_counter);

        Ok(json_resp(json!({ "AccessRequestId": access_request_id })))
    }

    pub(super) fn get_access_token(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let _access_request_id = body["AccessRequestId"]
            .as_str()
            .ok_or_else(|| missing("AccessRequestId"))?;

        Ok(json_resp(json!({
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
