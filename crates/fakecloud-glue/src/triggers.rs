//! Triggers, workflows, and workflow runs.

use serde_json::{json, Value};

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use crate::common::{entity, entity_not_found, new_id, now_ts, req_present, req_str};
use crate::generic;
use crate::service::GlueService;

const TRIGGER_FIELDS: &[&str] = &[
    "Name",
    "WorkflowName",
    "Type",
    "Schedule",
    "Predicate",
    "Actions",
    "Description",
    "EventBatchingCondition",
];

const WORKFLOW_FIELDS: &[&str] = &[
    "Name",
    "Description",
    "DefaultRunProperties",
    "MaxConcurrentRuns",
];

impl GlueService {
    // --- triggers ---

    pub(crate) fn create_trigger(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let name = req_str(&body, "Name")?.to_string();
        req_str(&body, "Type")?;
        req_present(&body, "Actions")?;
        let start = body["StartOnCreation"].as_bool().unwrap_or(false);
        let state_str = if start { "ACTIVATED" } else { "CREATED" };
        let stored = entity(
            &body,
            TRIGGER_FIELDS,
            vec![("Id", json!(new_id())), ("State", json!(state_str))],
        );
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id, &req.region);
        generic::create_unique(&mut st.triggers, &name, stored, "Trigger")?;
        Ok(AwsResponse::ok_json(json!({ "Name": name })))
    }

    pub(crate) fn get_trigger(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let name = req_str(&body, "Name")?;
        let accounts = self.state.read();
        let t = accounts
            .get(&req.account_id)
            .and_then(|s| s.triggers.get(name))
            .ok_or_else(|| entity_not_found(format!("Trigger {name} not found")))?;
        Ok(AwsResponse::ok_json(json!({ "Trigger": t })))
    }

    pub(crate) fn get_triggers(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let accounts = self.state.read();
        let list: Vec<Value> = accounts
            .get(&req.account_id)
            .map(|s| s.triggers.values().cloned().collect())
            .unwrap_or_default();
        Ok(AwsResponse::ok_json(json!({ "Triggers": list })))
    }

    pub(crate) fn list_triggers(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let accounts = self.state.read();
        let names: Vec<String> = accounts
            .get(&req.account_id)
            .map(|s| s.triggers.keys().cloned().collect())
            .unwrap_or_default();
        Ok(AwsResponse::ok_json(json!({ "TriggerNames": names })))
    }

    pub(crate) fn batch_get_triggers(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let names = body["TriggerNames"].as_array().cloned().unwrap_or_default();
        let accounts = self.state.read();
        let store = accounts.get(&req.account_id).map(|s| &s.triggers);
        let mut found = Vec::new();
        let mut not_found = Vec::new();
        for n in &names {
            let Some(name) = n.as_str() else { continue };
            match store.and_then(|m| m.get(name)) {
                Some(t) => found.push(t.clone()),
                None => not_found.push(json!(name)),
            }
        }
        Ok(AwsResponse::ok_json(json!({
            "Triggers": found,
            "TriggersNotFound": not_found,
        })))
    }

    pub(crate) fn update_trigger(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let name = req_str(&body, "Name")?.to_string();
        let update = req_present(&body, "TriggerUpdate")?;
        let mut updates: Vec<(&str, Value)> = Vec::new();
        for f in [
            "Description",
            "Schedule",
            "Actions",
            "Predicate",
            "EventBatchingCondition",
        ] {
            if let Some(v) = update.get(f) {
                if !v.is_null() {
                    updates.push((f, v.clone()));
                }
            }
        }
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id, &req.region);
        generic::update_merge(&mut st.triggers, &name, "Trigger", updates)?;
        let t = st.triggers.get(&name).cloned().unwrap();
        Ok(AwsResponse::ok_json(json!({ "Trigger": t })))
    }

    pub(crate) fn delete_trigger(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let name = req_str(&body, "Name")?.to_string();
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id, &req.region);
        // DeleteTrigger does not declare EntityNotFoundException; idempotent.
        st.triggers.remove(&name);
        Ok(AwsResponse::ok_json(json!({ "Name": name })))
    }

    pub(crate) fn start_trigger(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        // ON_DEMAND triggers fire a single run on StartTrigger but stay in the
        // CREATED state; only scheduled/conditional triggers transition to
        // ACTIVATED. Matching AWS here keeps the provider's computed `enabled`
        // attribute correct (ON_DEMAND + CREATED => enabled = true).
        let body = req.json_body();
        let name = req_str(&body, "Name")?.to_string();
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id, &req.region);
        let t = st
            .triggers
            .get_mut(&name)
            .ok_or_else(|| entity_not_found(format!("Trigger {name} not found")))?;
        if let Some(obj) = t.as_object_mut() {
            let is_on_demand = obj.get("Type").and_then(|v| v.as_str()) == Some("ON_DEMAND");
            if !is_on_demand {
                obj.insert("State".into(), json!("ACTIVATED"));
            }
        }
        Ok(AwsResponse::ok_json(json!({ "Name": name })))
    }

    pub(crate) fn stop_trigger(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        self.set_trigger_state(req, "DEACTIVATED")
    }

    fn set_trigger_state(
        &self,
        req: &AwsRequest,
        new_state: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let name = req_str(&body, "Name")?.to_string();
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id, &req.region);
        let t = st
            .triggers
            .get_mut(&name)
            .ok_or_else(|| entity_not_found(format!("Trigger {name} not found")))?;
        if let Some(obj) = t.as_object_mut() {
            obj.insert("State".into(), json!(new_state));
        }
        Ok(AwsResponse::ok_json(json!({ "Name": name })))
    }

    // --- workflows ---

    pub(crate) fn create_workflow(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let name = req_str(&body, "Name")?.to_string();
        let now = now_ts();
        let stored = entity(
            &body,
            WORKFLOW_FIELDS,
            vec![("CreatedOn", json!(now)), ("LastModifiedOn", json!(now))],
        );
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id, &req.region);
        generic::create_unique(&mut st.workflows, &name, stored, "Workflow")?;
        Ok(AwsResponse::ok_json(json!({ "Name": name })))
    }

    pub(crate) fn get_workflow(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let name = req_str(&body, "Name")?;
        let accounts = self.state.read();
        let w = accounts
            .get(&req.account_id)
            .and_then(|s| s.workflows.get(name))
            .ok_or_else(|| entity_not_found(format!("Workflow {name} not found")))?;
        Ok(AwsResponse::ok_json(json!({ "Workflow": w })))
    }

    pub(crate) fn list_workflows(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let accounts = self.state.read();
        let names: Vec<String> = accounts
            .get(&req.account_id)
            .map(|s| s.workflows.keys().cloned().collect())
            .unwrap_or_default();
        Ok(AwsResponse::ok_json(json!({ "Workflows": names })))
    }

    pub(crate) fn batch_get_workflows(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let names = body["Names"].as_array().cloned().unwrap_or_default();
        let accounts = self.state.read();
        let store = accounts.get(&req.account_id).map(|s| &s.workflows);
        let mut found = Vec::new();
        let mut missing_w = Vec::new();
        for n in &names {
            let Some(name) = n.as_str() else { continue };
            match store.and_then(|m| m.get(name)) {
                Some(w) => found.push(w.clone()),
                None => missing_w.push(json!(name)),
            }
        }
        Ok(AwsResponse::ok_json(json!({
            "Workflows": found,
            "MissingWorkflows": missing_w,
        })))
    }

    pub(crate) fn update_workflow(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let name = req_str(&body, "Name")?.to_string();
        let mut updates: Vec<(&str, Value)> = Vec::new();
        for f in ["Description", "DefaultRunProperties", "MaxConcurrentRuns"] {
            if let Some(v) = body.get(f) {
                if !v.is_null() {
                    updates.push((f, v.clone()));
                }
            }
        }
        updates.push(("LastModifiedOn", json!(now_ts())));
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id, &req.region);
        generic::update_merge(&mut st.workflows, &name, "Workflow", updates)?;
        Ok(AwsResponse::ok_json(json!({ "Name": name })))
    }

    pub(crate) fn delete_workflow(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let name = req_str(&body, "Name")?.to_string();
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id, &req.region);
        // DeleteWorkflow does not declare EntityNotFoundException; idempotent.
        st.workflows.remove(&name);
        Ok(AwsResponse::ok_json(json!({ "Name": name })))
    }

    // --- workflow runs ---

    pub(crate) fn start_workflow_run(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let name = req_str(&body, "Name")?.to_string();
        let run_id = format!("wr_{}", new_id());
        let now = now_ts();
        let props = body.get("RunProperties").cloned().unwrap_or(json!({}));
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id, &req.region);
        if !st.workflows.contains_key(&name) {
            return Err(entity_not_found(format!("Workflow {name} not found")));
        }
        st.workflow_runs.insert(
            run_id.clone(),
            json!({
                "Name": name,
                "WorkflowRunId": run_id,
                "WorkflowRunProperties": props,
                "Status": "RUNNING",
                "StartedOn": now,
            }),
        );
        Ok(AwsResponse::ok_json(json!({ "RunId": run_id })))
    }

    pub(crate) fn get_workflow_run(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        req_str(&body, "Name")?;
        let run_id = req_str(&body, "RunId")?;
        let accounts = self.state.read();
        let run = accounts
            .get(&req.account_id)
            .and_then(|s| s.workflow_runs.get(run_id))
            .ok_or_else(|| entity_not_found(format!("WorkflowRun {run_id} not found")))?;
        Ok(AwsResponse::ok_json(json!({ "Run": run })))
    }

    pub(crate) fn get_workflow_runs(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let name = req_str(&body, "Name")?;
        let accounts = self.state.read();
        let runs: Vec<Value> = accounts
            .get(&req.account_id)
            .map(|s| {
                s.workflow_runs
                    .values()
                    .filter(|r| r.get("Name").and_then(|n| n.as_str()) == Some(name))
                    .cloned()
                    .collect()
            })
            .unwrap_or_default();
        Ok(AwsResponse::ok_json(json!({ "Runs": runs })))
    }

    pub(crate) fn get_workflow_run_properties(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        req_str(&body, "Name")?;
        let run_id = req_str(&body, "RunId")?;
        let accounts = self.state.read();
        let run = accounts
            .get(&req.account_id)
            .and_then(|s| s.workflow_runs.get(run_id))
            .ok_or_else(|| entity_not_found(format!("WorkflowRun {run_id} not found")))?;
        let props = run
            .get("WorkflowRunProperties")
            .cloned()
            .unwrap_or(json!({}));
        Ok(AwsResponse::ok_json(json!({ "RunProperties": props })))
    }

    pub(crate) fn put_workflow_run_properties(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        req_str(&body, "Name")?;
        let run_id = req_str(&body, "RunId")?.to_string();
        let props = req_present(&body, "RunProperties")?.clone();
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id, &req.region);
        let run = st
            .workflow_runs
            .get_mut(&run_id)
            .ok_or_else(|| entity_not_found(format!("WorkflowRun {run_id} not found")))?;
        if let Some(obj) = run.as_object_mut() {
            obj.insert("WorkflowRunProperties".into(), props);
        }
        Ok(AwsResponse::ok_json(json!({})))
    }

    pub(crate) fn stop_workflow_run(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        req_str(&body, "Name")?;
        let run_id = req_str(&body, "RunId")?.to_string();
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id, &req.region);
        let run = st
            .workflow_runs
            .get_mut(&run_id)
            .ok_or_else(|| entity_not_found(format!("WorkflowRun {run_id} not found")))?;
        if let Some(obj) = run.as_object_mut() {
            obj.insert("Status".into(), json!("STOPPED"));
        }
        Ok(AwsResponse::ok_json(json!({})))
    }

    pub(crate) fn resume_workflow_run(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        req_str(&body, "Name")?;
        let run_id = req_str(&body, "RunId")?.to_string();
        let node_ids = req_present(&body, "NodeIds")?.clone();
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id, &req.region);
        st.workflow_runs
            .get_mut(&run_id)
            .ok_or_else(|| entity_not_found(format!("WorkflowRun {run_id} not found")))?;
        let new_run = format!("wr_{}", new_id());
        Ok(AwsResponse::ok_json(json!({
            "RunId": new_run,
            "NodeIds": node_ids,
        })))
    }
}
