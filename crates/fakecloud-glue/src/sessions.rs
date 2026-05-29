//! Interactive sessions and their statements.
//!
//! Sessions transition PROVISIONING -> READY on create; StopSession marks them
//! STOPPED. Statements get a monotonically-increasing integer id per session
//! and complete immediately with empty output.

use serde_json::{json, Value};

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use crate::common::{entity_not_found, missing, now_ts, req_present, req_str};
use crate::generic;
use crate::service::GlueService;

const SESSION_FIELDS: &[&str] = &[
    "Id",
    "Description",
    "Role",
    "Command",
    "DefaultArguments",
    "Connections",
    "MaxCapacity",
    "NumberOfWorkers",
    "WorkerType",
    "SecurityConfiguration",
    "GlueVersion",
    "IdleTimeout",
];

impl GlueService {
    pub(crate) fn create_session(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let id = req_str(&body, "Id")?.to_string();
        req_str(&body, "Role")?;
        req_present(&body, "Command")?;
        let now = now_ts();
        let session = crate::common::entity(
            &body,
            SESSION_FIELDS,
            vec![("Status", json!("READY")), ("CreatedOn", json!(now))],
        );
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id, &req.region);
        generic::create_unique(&mut st.sessions, &id, session.clone(), "Session")?;
        Ok(AwsResponse::ok_json(json!({ "Session": session })))
    }

    pub(crate) fn get_session(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let id = req_str(&body, "Id")?;
        let accounts = self.state.read();
        let s = accounts
            .get(&req.account_id)
            .and_then(|st| st.sessions.get(id))
            .ok_or_else(|| entity_not_found(format!("Session {id} not found")))?;
        Ok(AwsResponse::ok_json(json!({ "Session": s })))
    }

    pub(crate) fn list_sessions(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let accounts = self.state.read();
        let (ids, sessions): (Vec<String>, Vec<Value>) = accounts
            .get(&req.account_id)
            .map(|st| {
                (
                    st.sessions.keys().cloned().collect(),
                    st.sessions.values().cloned().collect(),
                )
            })
            .unwrap_or_default();
        Ok(AwsResponse::ok_json(json!({
            "Ids": ids,
            "Sessions": sessions,
        })))
    }

    pub(crate) fn delete_session(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let id = req_str(&body, "Id")?.to_string();
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id, &req.region);
        // DeleteSession does not declare EntityNotFoundException; idempotent.
        st.sessions.remove(&id);
        Ok(AwsResponse::ok_json(json!({ "Id": id })))
    }

    pub(crate) fn stop_session(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let id = req_str(&body, "Id")?.to_string();
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id, &req.region);
        // StopSession does not declare EntityNotFoundException; mark stopped if
        // present, otherwise no-op (the id echo is the only required output).
        if let Some(s) = st.sessions.get_mut(&id) {
            if let Some(obj) = s.as_object_mut() {
                obj.insert("Status".into(), json!("STOPPED"));
            }
        }
        Ok(AwsResponse::ok_json(json!({ "Id": id })))
    }

    // --- statements ---

    pub(crate) fn run_statement(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let session_id = req_str(&body, "SessionId")?.to_string();
        let code = req_str(&body, "Code")?.to_string();
        let now = now_ts();
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id, &req.region);
        if !st.sessions.contains_key(&session_id) {
            return Err(entity_not_found(format!("Session {session_id} not found")));
        }
        let next_id = st
            .statements
            .keys()
            .filter_map(|k| k.strip_prefix(&format!("{session_id}\u{1f}")))
            .filter_map(|s| s.parse::<i64>().ok())
            .max()
            .unwrap_or(0)
            + 1;
        st.statements.insert(
            format!("{session_id}\u{1f}{next_id}"),
            json!({
                "Id": next_id, "Code": code, "State": "AVAILABLE",
                "StartedOn": now, "CompletedOn": now,
                "Output": {"Status": "ok", "ExecutionCount": next_id},
            }),
        );
        Ok(AwsResponse::ok_json(json!({ "Id": next_id })))
    }

    pub(crate) fn get_statement(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let session_id = req_str(&body, "SessionId")?;
        let id = body["Id"].as_i64().ok_or_else(|| missing("Id"))?;
        let accounts = self.state.read();
        let s = accounts
            .get(&req.account_id)
            .and_then(|st| st.statements.get(&format!("{session_id}\u{1f}{id}")))
            .ok_or_else(|| entity_not_found(format!("Statement {id} not found")))?;
        Ok(AwsResponse::ok_json(json!({ "Statement": s })))
    }

    pub(crate) fn list_statements(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let session_id = req_str(&body, "SessionId")?;
        let prefix = format!("{session_id}\u{1f}");
        let accounts = self.state.read();
        let list: Vec<Value> = accounts
            .get(&req.account_id)
            .map(|st| {
                st.statements
                    .iter()
                    .filter(|(k, _)| k.starts_with(&prefix))
                    .map(|(_, v)| v.clone())
                    .collect()
            })
            .unwrap_or_default();
        Ok(AwsResponse::ok_json(json!({ "Statements": list })))
    }

    pub(crate) fn cancel_statement(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let session_id = req_str(&body, "SessionId")?.to_string();
        let id = body["Id"].as_i64().ok_or_else(|| missing("Id"))?;
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id, &req.region);
        let s = st
            .statements
            .get_mut(&format!("{session_id}\u{1f}{id}"))
            .ok_or_else(|| entity_not_found(format!("Statement {id} not found")))?;
        if let Some(obj) = s.as_object_mut() {
            obj.insert("State".into(), json!("CANCELLED"));
        }
        Ok(AwsResponse::ok_json(json!({})))
    }
}
