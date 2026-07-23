//! ML transforms, ML task runs, and data quality (rulesets, runs, results).

use serde_json::{json, Value};

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use crate::common::{entity_not_found, new_id, now_ts, req_present, req_str, settle_run_status};
use crate::generic;
use crate::service::GlueService;

const TRANSFORM_FIELDS: &[&str] = &[
    "Name",
    "Description",
    "InputRecordTables",
    "Parameters",
    "Role",
    "GlueVersion",
    "MaxCapacity",
    "WorkerType",
    "NumberOfWorkers",
    "Timeout",
    "MaxRetries",
    "TransformEncryption",
];

impl GlueService {
    // --- ML transforms ---

    pub(crate) fn create_ml_transform(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        req_str(&body, "Name")?;
        req_present(&body, "InputRecordTables")?;
        req_present(&body, "Parameters")?;
        req_str(&body, "Role")?;
        // Glue ML transform ids carry a `tfm-` prefix; the resource ARN is
        // `mlTransform/<id>`, which the provider asserts matches `tfm-.+`.
        let id = format!("tfm-{}", new_id());
        let now = now_ts();
        let stored = crate::common::entity(
            &body,
            TRANSFORM_FIELDS,
            vec![
                ("TransformId", json!(id)),
                ("Status", json!("READY")),
                ("CreatedOn", json!(now)),
                ("LastModifiedOn", json!(now)),
                ("LabelCount", json!(0)),
            ],
        );
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id, &req.region);
        st.ml_transforms.insert(id.clone(), stored);
        Ok(AwsResponse::ok_json(json!({ "TransformId": id })))
    }

    pub(crate) fn get_ml_transform(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let id = req_str(&body, "TransformId")?;
        let accounts = self.state.read();
        let state = accounts
            .get(&req.account_id)
            .ok_or_else(|| entity_not_found(format!("MLTransform {id} not found")))?;
        let t = state
            .ml_transforms
            .get(id)
            .ok_or_else(|| entity_not_found(format!("MLTransform {id} not found")))?;
        let mut out = t.clone();
        // AWS computes the transform's `Schema` from the columns of its input
        // record table; the Terraform resource reads it back as `schema`.
        if let Some(schema) = self.input_table_schema(state, &req.region, t) {
            out["Schema"] = schema;
        }
        Ok(AwsResponse::ok_json(out))
    }

    /// Build the `Schema` list (`[{Name, DataType}]`) for an ML transform from
    /// the columns of its first input record table, mirroring how AWS derives a
    /// transform's schema from the catalog table it reads.
    fn input_table_schema(
        &self,
        state: &crate::state::GlueState,
        region: &str,
        transform: &Value,
    ) -> Option<Value> {
        let table_ref = transform
            .get("InputRecordTables")
            .and_then(|v| v.as_array())
            .and_then(|arr| arr.first())?;
        let db_name = table_ref.get("DatabaseName").and_then(|v| v.as_str())?;
        let table_name = table_ref.get("TableName").and_then(|v| v.as_str())?;
        let table = state.dbs_in(region)?.get(db_name)?.tables.get(table_name)?;
        let columns = &table.storage_descriptor.as_ref()?.columns;
        let schema: Vec<Value> = columns
            .iter()
            .map(|c| json!({ "Name": c.name, "DataType": c.column_type }))
            .collect();
        Some(json!(schema))
    }

    pub(crate) fn get_ml_transforms(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let accounts = self.state.read();
        let list: Vec<Value> = accounts
            .get(&req.account_id)
            .map(|s| s.ml_transforms.values().cloned().collect())
            .unwrap_or_default();
        Ok(AwsResponse::ok_json(json!({ "Transforms": list })))
    }

    pub(crate) fn list_ml_transforms(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let accounts = self.state.read();
        let ids: Vec<String> = accounts
            .get(&req.account_id)
            .map(|s| s.ml_transforms.keys().cloned().collect())
            .unwrap_or_default();
        Ok(AwsResponse::ok_json(json!({ "TransformIds": ids })))
    }

    pub(crate) fn update_ml_transform(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let id = req_str(&body, "TransformId")?.to_string();
        let mut updates: Vec<(&str, Value)> = Vec::new();
        for f in TRANSFORM_FIELDS {
            if let Some(v) = body.get(*f) {
                if !v.is_null() {
                    updates.push((f, v.clone()));
                }
            }
        }
        updates.push(("LastModifiedOn", json!(now_ts())));
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id, &req.region);
        generic::update_merge(&mut st.ml_transforms, &id, "MLTransform", updates)?;
        Ok(AwsResponse::ok_json(json!({ "TransformId": id })))
    }

    pub(crate) fn delete_ml_transform(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let id = req_str(&body, "TransformId")?.to_string();
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id, &req.region);
        generic::delete(&mut st.ml_transforms, &id, "MLTransform")?;
        Ok(AwsResponse::ok_json(json!({ "TransformId": id })))
    }

    // --- ML task runs ---

    fn start_task_run(&self, req: &AwsRequest) -> Result<String, AwsServiceError> {
        let body = req.json_body();
        let transform_id = req_str(&body, "TransformId")?.to_string();
        let task_id = new_id();
        let now = now_ts();
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id, &req.region);
        if !st.ml_transforms.contains_key(&transform_id) {
            return Err(entity_not_found(format!(
                "MLTransform {transform_id} not found"
            )));
        }
        st.ml_task_runs.insert(
            task_id.clone(),
            json!({
                "TransformId": transform_id, "TaskRunId": task_id,
                "Status": "RUNNING", "StartedOn": now, "LastModifiedOn": now,
            }),
        );
        Ok(task_id)
    }

    pub(crate) fn start_ml_evaluation_task_run(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let id = self.start_task_run(req)?;
        Ok(AwsResponse::ok_json(json!({ "TaskRunId": id })))
    }

    pub(crate) fn start_ml_labeling_set_generation_task_run(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        req_str(&body, "OutputS3Path")?;
        let id = self.start_task_run(req)?;
        Ok(AwsResponse::ok_json(json!({ "TaskRunId": id })))
    }

    pub(crate) fn start_export_labels_task_run(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        req_str(&body, "OutputS3Path")?;
        let id = self.start_task_run(req)?;
        Ok(AwsResponse::ok_json(json!({ "TaskRunId": id })))
    }

    pub(crate) fn start_import_labels_task_run(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        req_str(&body, "InputS3Path")?;
        let id = self.start_task_run(req)?;
        Ok(AwsResponse::ok_json(json!({ "TaskRunId": id })))
    }

    pub(crate) fn get_ml_task_run(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        req_str(&body, "TransformId")?;
        let task_id = req_str(&body, "TaskRunId")?.to_string();
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id, &req.region);
        let r = st
            .ml_task_runs
            .get_mut(&task_id)
            .ok_or_else(|| entity_not_found(format!("MLTaskRun {task_id} not found")))?;
        settle_run_status(r, "Status", "SUCCEEDED", Some("CompletedOn"));
        Ok(AwsResponse::ok_json(r.clone()))
    }

    pub(crate) fn get_ml_task_runs(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let transform_id = req_str(&body, "TransformId")?.to_string();
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id, &req.region);
        let runs: Vec<Value> = st
            .ml_task_runs
            .values()
            .filter(|r| {
                r.get("TransformId").and_then(|v| v.as_str()) == Some(transform_id.as_str())
            })
            .cloned()
            .collect();
        Ok(AwsResponse::ok_json(json!({ "TaskRuns": runs })))
    }

    pub(crate) fn cancel_ml_task_run(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let transform_id = req_str(&body, "TransformId")?.to_string();
        let task_id = req_str(&body, "TaskRunId")?.to_string();
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id, &req.region);
        let r = st
            .ml_task_runs
            .get_mut(&task_id)
            .ok_or_else(|| entity_not_found(format!("MLTaskRun {task_id} not found")))?;
        if let Some(obj) = r.as_object_mut() {
            obj.insert("Status".into(), json!("STOPPING"));
        }
        Ok(AwsResponse::ok_json(json!({
            "TransformId": transform_id, "TaskRunId": task_id, "Status": "STOPPING",
        })))
    }

    // --- data quality rulesets ---

    pub(crate) fn create_data_quality_ruleset(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let name = req_str(&body, "Name")?.to_string();
        req_str(&body, "Ruleset")?;
        let now = now_ts();
        let stored = crate::common::entity(
            &body,
            &[
                "Name",
                "Description",
                "Ruleset",
                "TargetTable",
                "DataQualitySecurityConfiguration",
            ],
            vec![("CreatedOn", json!(now)), ("LastModifiedOn", json!(now))],
        );
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id, &req.region);
        generic::create_unique(&mut st.dq_rulesets, &name, stored, "DataQualityRuleset")?;
        Ok(AwsResponse::ok_json(json!({ "Name": name })))
    }

    pub(crate) fn get_data_quality_ruleset(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let name = req_str(&body, "Name")?;
        let accounts = self.state.read();
        let r = accounts
            .get(&req.account_id)
            .and_then(|s| s.dq_rulesets.get(name))
            .ok_or_else(|| entity_not_found(format!("DataQualityRuleset {name} not found")))?;
        Ok(AwsResponse::ok_json(r.clone()))
    }

    pub(crate) fn update_data_quality_ruleset(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let name = req_str(&body, "Name")?.to_string();
        let mut updates: Vec<(&str, Value)> = vec![("LastModifiedOn", json!(now_ts()))];
        for f in ["Description", "Ruleset"] {
            if let Some(v) = body.get(f) {
                if !v.is_null() {
                    updates.push((f, v.clone()));
                }
            }
        }
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id, &req.region);
        generic::update_merge(&mut st.dq_rulesets, &name, "DataQualityRuleset", updates)?;
        let r = st.dq_rulesets.get(&name).cloned().unwrap();
        Ok(AwsResponse::ok_json(json!({
            "Name": name,
            "Description": r.get("Description").cloned().unwrap_or(Value::Null),
            "Ruleset": r.get("Ruleset").cloned().unwrap_or(Value::Null),
        })))
    }

    pub(crate) fn delete_data_quality_ruleset(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let name = req_str(&body, "Name")?.to_string();
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id, &req.region);
        generic::delete(&mut st.dq_rulesets, &name, "DataQualityRuleset")?;
        Ok(AwsResponse::ok_json(json!({})))
    }

    pub(crate) fn list_data_quality_rulesets(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let accounts = self.state.read();
        let list: Vec<Value> = accounts
            .get(&req.account_id)
            .map(|s| {
                s.dq_rulesets
                    .values()
                    .map(|r| {
                        json!({
                            "Name": r.get("Name").cloned().unwrap_or(Value::Null),
                            "Description": r.get("Description").cloned().unwrap_or(Value::Null),
                            "CreatedOn": r.get("CreatedOn").cloned().unwrap_or(Value::Null),
                            "LastModifiedOn": r.get("LastModifiedOn").cloned().unwrap_or(Value::Null),
                            "TargetTable": r.get("TargetTable").cloned().unwrap_or(Value::Null),
                        })
                    })
                    .collect()
            })
            .unwrap_or_default();
        Ok(AwsResponse::ok_json(json!({ "Rulesets": list })))
    }

    // --- data quality runs & results ---

    pub(crate) fn start_data_quality_ruleset_evaluation_run(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        req_present(&body, "DataSource")?;
        req_str(&body, "Role")?;
        req_present(&body, "RulesetNames")?;
        let run_id = new_id();
        let now = now_ts();
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id, &req.region);
        st.dq_ruleset_runs.insert(
            run_id.clone(),
            json!({
                "RunId": run_id, "Status": "RUNNING", "StartedOn": now,
                "DataSource": body.get("DataSource").cloned().unwrap_or(Value::Null),
                "Role": body.get("Role").cloned().unwrap_or(Value::Null),
                "RulesetNames": body.get("RulesetNames").cloned().unwrap_or(Value::Null),
            }),
        );
        Ok(AwsResponse::ok_json(json!({ "RunId": run_id })))
    }

    pub(crate) fn get_data_quality_ruleset_evaluation_run(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let run_id = req_str(&body, "RunId")?.to_string();
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id, &req.region);
        let r = st
            .dq_ruleset_runs
            .get_mut(&run_id)
            .ok_or_else(|| entity_not_found(format!("Run {run_id} not found")))?;
        // Settle to a terminal state on read and, on the transition, publish a
        // DataQuality result so ResultIds/GetDataQualityResult resolve.
        let mut new_result: Option<Value> = None;
        if settle_run_status(r, "Status", "SUCCEEDED", Some("CompletedOn")) {
            let result_id = format!("dqresult-{}", new_id());
            let ruleset0 = r
                .get("RulesetNames")
                .and_then(|v| v.as_array())
                .and_then(|a| a.first())
                .cloned()
                .unwrap_or(Value::Null);
            let data_source = r.get("DataSource").cloned().unwrap_or(Value::Null);
            let started_on = r.get("StartedOn").cloned().unwrap_or(Value::Null);
            if let Some(obj) = r.as_object_mut() {
                obj.insert("ResultIds".into(), json!([result_id]));
            }
            new_result = Some(json!({
                "ResultId": result_id,
                "RulesetName": ruleset0,
                "Score": 1.0,
                "DataSource": data_source,
                "StartedOn": started_on,
                "CompletedOn": now_ts(),
                "RuleResults": [],
            }));
        }
        let run_json = r.clone();
        if let Some(result) = new_result {
            if let Some(rid) = result.get("ResultId").and_then(|v| v.as_str()) {
                st.dq_results.insert(rid.to_string(), result);
            }
        }
        Ok(AwsResponse::ok_json(run_json))
    }

    pub(crate) fn cancel_data_quality_ruleset_evaluation_run(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let run_id = req_str(&body, "RunId")?.to_string();
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id, &req.region);
        let r = st
            .dq_ruleset_runs
            .get_mut(&run_id)
            .ok_or_else(|| entity_not_found(format!("Run {run_id} not found")))?;
        if let Some(obj) = r.as_object_mut() {
            obj.insert("Status".into(), json!("STOPPING"));
        }
        Ok(AwsResponse::ok_json(json!({})))
    }

    pub(crate) fn list_data_quality_ruleset_evaluation_runs(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id, &req.region);
        let runs: Vec<Value> = st.dq_ruleset_runs.values().cloned().collect();
        Ok(AwsResponse::ok_json(json!({ "Runs": runs })))
    }

    pub(crate) fn start_data_quality_rule_recommendation_run(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        req_present(&body, "DataSource")?;
        req_str(&body, "Role")?;
        let run_id = new_id();
        let now = now_ts();
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id, &req.region);
        st.dq_recommendation_runs.insert(
            run_id.clone(),
            json!({
                "RunId": run_id, "Status": "RUNNING", "StartedOn": now,
                "DataSource": body.get("DataSource").cloned().unwrap_or(Value::Null),
                "Role": body.get("Role").cloned().unwrap_or(Value::Null),
            }),
        );
        Ok(AwsResponse::ok_json(json!({ "RunId": run_id })))
    }

    pub(crate) fn get_data_quality_rule_recommendation_run(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let run_id = req_str(&body, "RunId")?.to_string();
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id, &req.region);
        let r = st
            .dq_recommendation_runs
            .get_mut(&run_id)
            .ok_or_else(|| entity_not_found(format!("Run {run_id} not found")))?;
        // Settle on read and attach a recommended ruleset so the completed run
        // carries the output a poller expects.
        if settle_run_status(r, "Status", "SUCCEEDED", Some("CompletedOn")) {
            if let Some(obj) = r.as_object_mut() {
                obj.entry("RecommendedRuleset".to_string())
                    .or_insert(json!("Rules = [ ]"));
            }
        }
        Ok(AwsResponse::ok_json(r.clone()))
    }

    pub(crate) fn cancel_data_quality_rule_recommendation_run(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let run_id = req_str(&body, "RunId")?.to_string();
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id, &req.region);
        let r = st
            .dq_recommendation_runs
            .get_mut(&run_id)
            .ok_or_else(|| entity_not_found(format!("Run {run_id} not found")))?;
        if let Some(obj) = r.as_object_mut() {
            obj.insert("Status".into(), json!("STOPPING"));
        }
        Ok(AwsResponse::ok_json(json!({})))
    }

    pub(crate) fn list_data_quality_rule_recommendation_runs(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id, &req.region);
        let runs: Vec<Value> = st.dq_recommendation_runs.values().cloned().collect();
        Ok(AwsResponse::ok_json(json!({ "Runs": runs })))
    }

    pub(crate) fn get_data_quality_result(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let id = req_str(&body, "ResultId")?;
        let accounts = self.state.read();
        let r = accounts
            .get(&req.account_id)
            .and_then(|s| s.dq_results.get(id));
        match r {
            Some(v) => Ok(AwsResponse::ok_json(v.clone())),
            None => Ok(AwsResponse::ok_json(json!({ "ResultId": id }))),
        }
    }

    pub(crate) fn batch_get_data_quality_result(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let ids = body["ResultIds"].as_array().cloned().unwrap_or_default();
        let accounts = self.state.read();
        let store = accounts.get(&req.account_id).map(|s| &s.dq_results);
        let mut results = Vec::new();
        let mut not_found = Vec::new();
        for i in &ids {
            let Some(id) = i.as_str() else { continue };
            match store.and_then(|m| m.get(id)) {
                Some(v) => results.push(v.clone()),
                None => not_found.push(json!(id)),
            }
        }
        Ok(AwsResponse::ok_json(json!({
            "Results": results, "ResultsNotFound": not_found,
        })))
    }

    pub(crate) fn list_data_quality_results(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let accounts = self.state.read();
        let list: Vec<Value> = accounts
            .get(&req.account_id)
            .map(|s| s.dq_results.values().cloned().collect())
            .unwrap_or_default();
        Ok(AwsResponse::ok_json(json!({ "Results": list })))
    }
}
