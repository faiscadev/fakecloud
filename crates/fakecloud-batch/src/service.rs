//! AWS Batch restJson1 service dispatch + core control plane.

use std::sync::Arc;

use async_trait::async_trait;
use http::{Method, StatusCode};
use serde_json::{json, Map, Value};
use tokio::sync::Mutex as AsyncMutex;
use uuid::Uuid;

use fakecloud_aws::arn::Arn;
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsService, AwsServiceError};
use fakecloud_persistence::{SnapshotHook, SnapshotStore};

use crate::state::{BatchSnapshot, SharedBatchState, BATCH_SNAPSHOT_SCHEMA_VERSION};

const SUPPORTED_ACTIONS: &[&str] = &[
    "CancelJob",
    "CreateComputeEnvironment",
    "CreateConsumableResource",
    "CreateJobQueue",
    "CreateQuotaShare",
    "CreateSchedulingPolicy",
    "CreateServiceEnvironment",
    "DeleteComputeEnvironment",
    "DeleteConsumableResource",
    "DeleteJobQueue",
    "DeleteQuotaShare",
    "DeleteSchedulingPolicy",
    "DeleteServiceEnvironment",
    "DeregisterJobDefinition",
    "DescribeComputeEnvironments",
    "DescribeConsumableResource",
    "DescribeJobDefinitions",
    "DescribeJobQueues",
    "DescribeJobs",
    "DescribeQuotaShare",
    "DescribeSchedulingPolicies",
    "DescribeServiceEnvironments",
    "DescribeServiceJob",
    "GetJobQueueSnapshot",
    "ListConsumableResources",
    "ListJobs",
    "ListJobsByConsumableResource",
    "ListQuotaShares",
    "ListSchedulingPolicies",
    "ListServiceJobs",
    "ListTagsForResource",
    "RegisterJobDefinition",
    "SubmitJob",
    "SubmitServiceJob",
    "TagResource",
    "TerminateJob",
    "TerminateServiceJob",
    "UntagResource",
    "UpdateComputeEnvironment",
    "UpdateConsumableResource",
    "UpdateJobQueue",
    "UpdateQuotaShare",
    "UpdateSchedulingPolicy",
    "UpdateServiceEnvironment",
    "UpdateServiceJob",
];

/// Mutating actions trigger a snapshot write after success.
const MUTATING_ACTIONS: &[&str] = &[
    "CreateComputeEnvironment",
    "UpdateComputeEnvironment",
    "DeleteComputeEnvironment",
    "CreateJobQueue",
    "UpdateJobQueue",
    "DeleteJobQueue",
    "RegisterJobDefinition",
    "DeregisterJobDefinition",
    "SubmitJob",
    "CancelJob",
    "TerminateJob",
    "TagResource",
    "UntagResource",
];

pub struct BatchService {
    state: SharedBatchState,
    snapshot_store: Option<Arc<dyn SnapshotStore>>,
    snapshot_lock: Arc<AsyncMutex<()>>,
}

impl BatchService {
    pub fn new(state: SharedBatchState) -> Self {
        Self {
            state,
            snapshot_store: None,
            snapshot_lock: Arc::new(AsyncMutex::new(())),
        }
    }

    pub fn with_snapshot_store(mut self, store: Arc<dyn SnapshotStore>) -> Self {
        self.snapshot_store = Some(store);
        self
    }

    async fn save_snapshot(&self) {
        let Some(store) = self.snapshot_store.clone() else {
            return;
        };
        let _guard = self.snapshot_lock.lock().await;
        let bytes = {
            let snap = BatchSnapshot {
                schema_version: BATCH_SNAPSHOT_SCHEMA_VERSION,
                accounts: Some(self.state.read().clone()),
            };
            serde_json::to_vec(&snap).unwrap_or_default()
        };
        let _ = tokio::task::spawn_blocking(move || store.save(&bytes)).await;
    }

    /// CloudFormation write-through hook (used once the CFN provisioner lands).
    pub fn snapshot_hook(&self) -> Option<SnapshotHook> {
        let store = self.snapshot_store.clone()?;
        let state = self.state.clone();
        let lock = self.snapshot_lock.clone();
        Some(Arc::new(move || {
            let store = store.clone();
            let state = state.clone();
            let lock = lock.clone();
            Box::pin(async move {
                let _guard = lock.lock().await;
                let bytes = {
                    let snap = BatchSnapshot {
                        schema_version: BATCH_SNAPSHOT_SCHEMA_VERSION,
                        accounts: Some(state.read().clone()),
                    };
                    serde_json::to_vec(&snap).unwrap_or_default()
                };
                let _ = tokio::task::spawn_blocking(move || store.save(&bytes)).await;
            })
        }))
    }

    /// Map the restJson1 request (POST /v1/<op>, plus the /v1/tags/{arn}
    /// family) to its operation name.
    fn resolve_action(req: &AwsRequest) -> Option<&'static str> {
        let segs = &req.path_segments;
        if segs.first().map(|s| s.as_str()) != Some("v1") {
            return None;
        }
        // Tag family: /v1/tags/{resourceArn}
        if segs.get(1).map(|s| s.as_str()) == Some("tags") {
            return match req.method {
                Method::GET => Some("ListTagsForResource"),
                Method::POST => Some("TagResource"),
                Method::DELETE => Some("UntagResource"),
                _ => None,
            };
        }
        let op = segs.get(1)?.as_str();
        Some(match op {
            "canceljob" => "CancelJob",
            "createcomputeenvironment" => "CreateComputeEnvironment",
            "createconsumableresource" => "CreateConsumableResource",
            "createjobqueue" => "CreateJobQueue",
            "createquotashare" => "CreateQuotaShare",
            "createschedulingpolicy" => "CreateSchedulingPolicy",
            "createserviceenvironment" => "CreateServiceEnvironment",
            "deletecomputeenvironment" => "DeleteComputeEnvironment",
            "deleteconsumableresource" => "DeleteConsumableResource",
            "deletejobqueue" => "DeleteJobQueue",
            "deletequotashare" => "DeleteQuotaShare",
            "deleteschedulingpolicy" => "DeleteSchedulingPolicy",
            "deleteserviceenvironment" => "DeleteServiceEnvironment",
            "deregisterjobdefinition" => "DeregisterJobDefinition",
            "describecomputeenvironments" => "DescribeComputeEnvironments",
            "describeconsumableresource" => "DescribeConsumableResource",
            "describejobdefinitions" => "DescribeJobDefinitions",
            "describejobqueues" => "DescribeJobQueues",
            "describejobs" => "DescribeJobs",
            "describequotashare" => "DescribeQuotaShare",
            "describeschedulingpolicies" => "DescribeSchedulingPolicies",
            "describeserviceenvironments" => "DescribeServiceEnvironments",
            "describeservicejob" => "DescribeServiceJob",
            "getjobqueuesnapshot" => "GetJobQueueSnapshot",
            "listconsumableresources" => "ListConsumableResources",
            "listjobs" => "ListJobs",
            "listjobsbyconsumableresource" => "ListJobsByConsumableResource",
            "listquotashares" => "ListQuotaShares",
            "listschedulingpolicies" => "ListSchedulingPolicies",
            "listservicejobs" => "ListServiceJobs",
            "registerjobdefinition" => "RegisterJobDefinition",
            "submitjob" => "SubmitJob",
            "submitservicejob" => "SubmitServiceJob",
            "terminatejob" => "TerminateJob",
            "terminateservicejob" => "TerminateServiceJob",
            "updatecomputeenvironment" => "UpdateComputeEnvironment",
            "updateconsumableresource" => "UpdateConsumableResource",
            "updatejobqueue" => "UpdateJobQueue",
            "updatequotashare" => "UpdateQuotaShare",
            "updateschedulingpolicy" => "UpdateSchedulingPolicy",
            "updateserviceenvironment" => "UpdateServiceEnvironment",
            "updateservicejob" => "UpdateServiceJob",
            _ => return None,
        })
    }

    fn dispatch(&self, action: &str, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        match action {
            "CreateComputeEnvironment" => self.create_compute_environment(req),
            "DescribeComputeEnvironments" => self.describe_compute_environments(req),
            "DeleteComputeEnvironment" => self.delete_compute_environment(req),
            "CreateJobQueue" => self.create_job_queue(req),
            "DescribeJobQueues" => self.describe_job_queues(req),
            "DeleteJobQueue" => self.delete_job_queue(req),
            "RegisterJobDefinition" => self.register_job_definition(req),
            "DescribeJobDefinitions" => self.describe_job_definitions(req),
            "DeregisterJobDefinition" => self.deregister_job_definition(req),
            "TagResource" => self.tag_resource(req),
            "UntagResource" => self.untag_resource(req),
            "ListTagsForResource" => self.list_tags_for_resource(req),
            other => Err(AwsServiceError::action_not_implemented("batch", other)),
        }
    }
}

fn obj(v: &Value) -> Map<String, Value> {
    v.as_object().cloned().unwrap_or_default()
}

fn client_error(code: &str, msg: impl Into<String>) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::BAD_REQUEST, code, msg.into())
}

impl BatchService {
    fn arn(&self, account: &str, region: &str, resource: &str) -> String {
        Arn::new("batch", region, account, resource).to_string()
    }

    // ---- Compute environments ----

    fn create_compute_environment(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let name = body
            .get("computeEnvironmentName")
            .and_then(Value::as_str)
            .ok_or_else(|| client_error("ClientException", "computeEnvironmentName is required"))?
            .to_string();
        let arn = self.arn(
            &req.account_id,
            &req.region,
            &format!("compute-environment/{name}"),
        );
        let mut stored = obj(&body);
        stored.insert("computeEnvironmentArn".into(), json!(arn));
        stored.insert("status".into(), json!("VALID"));
        stored.insert("statusReason".into(), json!("ComputeEnvironment Healthy"));
        stored
            .entry("state".to_string())
            .or_insert_with(|| json!("ENABLED"));
        stored.insert("uuid".into(), json!(Uuid::new_v4().to_string()));

        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        if st.compute_environments.contains_key(&name) {
            return Err(client_error(
                "ClientException",
                format!("Object already exists: {name}"),
            ));
        }
        st.compute_environments
            .insert(name.clone(), Value::Object(stored));
        Ok(AwsResponse::ok_json(json!({
            "computeEnvironmentName": name,
            "computeEnvironmentArn": arn,
        })))
    }

    fn describe_compute_environments(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let wanted = string_set(&body, "computeEnvironments");
        let accounts = self.state.read();
        let items: Vec<Value> = accounts
            .get(&req.account_id)
            .map(|st| {
                st.compute_environments
                    .values()
                    .filter(|ce| {
                        match_named(
                            ce,
                            &wanted,
                            "computeEnvironmentName",
                            "computeEnvironmentArn",
                        )
                    })
                    .cloned()
                    .collect()
            })
            .unwrap_or_default();
        Ok(AwsResponse::ok_json(
            json!({ "computeEnvironments": items }),
        ))
    }

    fn delete_compute_environment(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let name = arn_or_name(&body, "computeEnvironment")?;
        let mut accounts = self.state.write();
        accounts
            .get_or_create(&req.account_id)
            .compute_environments
            .remove(&name);
        Ok(AwsResponse::ok_json(json!({})))
    }

    // ---- Job queues ----

    fn create_job_queue(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let name = body
            .get("jobQueueName")
            .and_then(Value::as_str)
            .ok_or_else(|| client_error("ClientException", "jobQueueName is required"))?
            .to_string();
        let arn = self.arn(&req.account_id, &req.region, &format!("job-queue/{name}"));
        let mut stored = obj(&body);
        stored.insert("jobQueueArn".into(), json!(arn));
        stored.insert("status".into(), json!("VALID"));
        stored.insert("statusReason".into(), json!("JobQueue Healthy"));
        stored
            .entry("state".to_string())
            .or_insert_with(|| json!("ENABLED"));

        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        if st.job_queues.contains_key(&name) {
            return Err(client_error(
                "ClientException",
                format!("Object already exists: {name}"),
            ));
        }
        st.job_queues.insert(name.clone(), Value::Object(stored));
        Ok(AwsResponse::ok_json(json!({
            "jobQueueName": name,
            "jobQueueArn": arn,
        })))
    }

    fn describe_job_queues(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let wanted = string_set(&body, "jobQueues");
        let accounts = self.state.read();
        let items: Vec<Value> = accounts
            .get(&req.account_id)
            .map(|st| {
                st.job_queues
                    .values()
                    .filter(|q| match_named(q, &wanted, "jobQueueName", "jobQueueArn"))
                    .cloned()
                    .collect()
            })
            .unwrap_or_default();
        Ok(AwsResponse::ok_json(json!({ "jobQueues": items })))
    }

    fn delete_job_queue(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let name = arn_or_name(&body, "jobQueue")?;
        let mut accounts = self.state.write();
        accounts
            .get_or_create(&req.account_id)
            .job_queues
            .remove(&name);
        Ok(AwsResponse::ok_json(json!({})))
    }

    // ---- Job definitions (revisioned) ----

    fn register_job_definition(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let name = body
            .get("jobDefinitionName")
            .and_then(Value::as_str)
            .ok_or_else(|| client_error("ClientException", "jobDefinitionName is required"))?
            .to_string();
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let revision = st.job_def_revisions.entry(name.clone()).or_insert(0);
        *revision += 1;
        let revision = *revision;
        let arn = self.arn(
            &req.account_id,
            &req.region,
            &format!("job-definition/{name}:{revision}"),
        );
        let mut stored = obj(&body);
        stored.insert("jobDefinitionArn".into(), json!(arn));
        stored.insert("revision".into(), json!(revision));
        stored.insert("status".into(), json!("ACTIVE"));
        st.job_definitions
            .insert(format!("{name}:{revision}"), Value::Object(stored));
        Ok(AwsResponse::ok_json(json!({
            "jobDefinitionName": name,
            "jobDefinitionArn": arn,
            "revision": revision,
        })))
    }

    fn describe_job_definitions(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let wanted = string_set(&body, "jobDefinitions");
        let name_filter = body
            .get("jobDefinitionName")
            .and_then(Value::as_str)
            .map(|s| s.to_string());
        let status_filter = body
            .get("status")
            .and_then(Value::as_str)
            .map(|s| s.to_string());
        let accounts = self.state.read();
        let items: Vec<Value> = accounts
            .get(&req.account_id)
            .map(|st| {
                st.job_definitions
                    .values()
                    .filter(|jd| {
                        // Match by arn (in `jobDefinitions`), by name, or all.
                        let arn_ok = wanted.is_empty()
                            || jd
                                .get("jobDefinitionArn")
                                .and_then(Value::as_str)
                                .map(|a| wanted.contains(a))
                                .unwrap_or(false)
                            || jd
                                .get("jobDefinitionName")
                                .and_then(Value::as_str)
                                .map(|n| wanted.contains(n))
                                .unwrap_or(false);
                        let name_ok = name_filter.as_deref().is_none_or(|n| {
                            jd.get("jobDefinitionName").and_then(Value::as_str) == Some(n)
                        });
                        let status_ok = status_filter
                            .as_deref()
                            .is_none_or(|s| jd.get("status").and_then(Value::as_str) == Some(s));
                        arn_ok && name_ok && status_ok
                    })
                    .cloned()
                    .collect()
            })
            .unwrap_or_default();
        Ok(AwsResponse::ok_json(json!({ "jobDefinitions": items })))
    }

    fn deregister_job_definition(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let id = body
            .get("jobDefinition")
            .and_then(Value::as_str)
            .ok_or_else(|| client_error("ClientException", "jobDefinition is required"))?;
        // Accept "name:revision" or an ARN ending in name:revision.
        let key = id.rsplit('/').next().unwrap_or(id).to_string();
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        if let Some(jd) = st.job_definitions.get_mut(&key) {
            if let Some(o) = jd.as_object_mut() {
                o.insert("status".into(), json!("INACTIVE"));
            }
        }
        Ok(AwsResponse::ok_json(json!({})))
    }

    // ---- Tags ----

    fn tag_resource(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let arn = req
            .path_segments
            .get(2)
            .map(|s| percent_decode(s))
            .ok_or_else(|| client_error("ClientException", "resourceArn is required"))?;
        let body = req.json_body();
        let tags = body
            .get("tags")
            .and_then(Value::as_object)
            .cloned()
            .unwrap_or_default();
        let mut accounts = self.state.write();
        let entry = accounts
            .get_or_create(&req.account_id)
            .tags
            .entry(arn)
            .or_default();
        for (k, v) in tags {
            if let Some(s) = v.as_str() {
                entry.insert(k, s.to_string());
            }
        }
        Ok(AwsResponse::ok_json(json!({})))
    }

    fn untag_resource(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let arn = req
            .path_segments
            .get(2)
            .map(|s| percent_decode(s))
            .ok_or_else(|| client_error("ClientException", "resourceArn is required"))?;
        let keys: Vec<String> = req
            .query_params
            .iter()
            .filter(|(k, _)| k.as_str() == "tagKeys" || k.starts_with("tagKeys"))
            .map(|(_, v)| v.clone())
            .collect();
        let mut accounts = self.state.write();
        if let Some(entry) = accounts.get_or_create(&req.account_id).tags.get_mut(&arn) {
            for k in keys {
                entry.remove(&k);
            }
        }
        Ok(AwsResponse::ok_json(json!({})))
    }

    fn list_tags_for_resource(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let arn = req
            .path_segments
            .get(2)
            .map(|s| percent_decode(s))
            .ok_or_else(|| client_error("ClientException", "resourceArn is required"))?;
        let accounts = self.state.read();
        let tags = accounts
            .get(&req.account_id)
            .and_then(|st| st.tags.get(&arn))
            .map(|m| {
                m.iter()
                    .map(|(k, v)| (k.clone(), json!(v)))
                    .collect::<Map<String, Value>>()
            })
            .unwrap_or_default();
        Ok(AwsResponse::ok_json(json!({ "tags": tags })))
    }
}

/// Read a JSON string array into a set of owned strings.
fn string_set(body: &Value, key: &str) -> std::collections::HashSet<String> {
    body.get(key)
        .and_then(Value::as_array)
        .map(|a| {
            a.iter()
                .filter_map(|v| v.as_str().map(String::from))
                .collect()
        })
        .unwrap_or_default()
}

/// True if `wanted` is empty (match-all) or the resource's name/arn is wanted.
fn match_named(
    res: &Value,
    wanted: &std::collections::HashSet<String>,
    name_key: &str,
    arn_key: &str,
) -> bool {
    if wanted.is_empty() {
        return true;
    }
    let name = res.get(name_key).and_then(Value::as_str);
    let arn = res.get(arn_key).and_then(Value::as_str);
    name.map(|n| wanted.contains(n)).unwrap_or(false)
        || arn.map(|a| wanted.contains(a)).unwrap_or(false)
}

/// Resolve a delete/describe identifier that may be a name or an ARN to the
/// store key (the resource name = last ARN path segment).
fn arn_or_name(body: &Value, key: &str) -> Result<String, AwsServiceError> {
    let raw = body
        .get(key)
        .and_then(Value::as_str)
        .ok_or_else(|| client_error("ClientException", format!("{key} is required")))?;
    Ok(raw.rsplit('/').next().unwrap_or(raw).to_string())
}

fn percent_decode(s: &str) -> String {
    // ARNs in the path are percent-encoded by SDKs; decode the common escapes.
    let mut out = String::with_capacity(s.len());
    let bytes = s.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            if let Ok(b) = u8::from_str_radix(&s[i + 1..i + 3], 16) {
                out.push(b as char);
                i += 3;
                continue;
            }
        }
        out.push(bytes[i] as char);
        i += 1;
    }
    out
}

#[async_trait]
impl AwsService for BatchService {
    fn service_name(&self) -> &str {
        "batch"
    }

    fn supported_actions(&self) -> &[&str] {
        SUPPORTED_ACTIONS
    }

    async fn handle(&self, req: AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let Some(action) = Self::resolve_action(&req) else {
            return Err(AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "ResourceNotFoundException",
                format!("Unknown operation: {} {}", req.method, req.raw_path),
            ));
        };
        let result = self.dispatch(action, &req);
        if MUTATING_ACTIONS.contains(&action)
            && matches!(result.as_ref(), Ok(resp) if resp.status.is_success())
        {
            self.save_snapshot().await;
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::BatchAccounts;
    use parking_lot::RwLock;
    use std::collections::HashMap;

    fn svc() -> BatchService {
        BatchService::new(Arc::new(RwLock::new(BatchAccounts::new())))
    }

    fn req(path: &str, body: Value) -> AwsRequest {
        let p = path.split('?').next().unwrap_or(path);
        let path_segments: Vec<String> = p
            .split('/')
            .filter(|s| !s.is_empty())
            .map(String::from)
            .collect();
        AwsRequest {
            service: "batch".into(),
            action: String::new(),
            region: "us-east-1".into(),
            account_id: "123456789012".into(),
            request_id: "t".into(),
            headers: http::HeaderMap::new(),
            query_params: HashMap::new(),
            body: bytes::Bytes::from(serde_json::to_vec(&body).unwrap()),
            body_stream: parking_lot::Mutex::new(None),
            path_segments,
            raw_path: path.to_string(),
            raw_query: String::new(),
            method: Method::POST,
            is_query_protocol: false,
            access_key_id: None,
            principal: None,
        }
    }

    fn body_of(r: AwsResponse) -> Value {
        serde_json::from_slice(r.body.expect_bytes()).unwrap()
    }

    #[tokio::test]
    async fn compute_environment_lifecycle() {
        let s = svc();
        let r = s
            .handle(req(
                "/v1/createcomputeenvironment",
                json!({"computeEnvironmentName": "ce1", "type": "MANAGED"}),
            ))
            .await
            .unwrap();
        let v = body_of(r);
        assert_eq!(v["computeEnvironmentName"], "ce1");
        assert!(v["computeEnvironmentArn"]
            .as_str()
            .unwrap()
            .contains("compute-environment/ce1"));

        let d = body_of(
            s.handle(req("/v1/describecomputeenvironments", json!({})))
                .await
                .unwrap(),
        );
        let ces = d["computeEnvironments"].as_array().unwrap();
        assert_eq!(ces.len(), 1);
        assert_eq!(ces[0]["status"], "VALID");
        assert_eq!(ces[0]["state"], "ENABLED");

        s.handle(req(
            "/v1/deletecomputeenvironment",
            json!({"computeEnvironment": "ce1"}),
        ))
        .await
        .unwrap();
        let d2 = body_of(
            s.handle(req("/v1/describecomputeenvironments", json!({})))
                .await
                .unwrap(),
        );
        assert_eq!(d2["computeEnvironments"].as_array().unwrap().len(), 0);
    }

    #[tokio::test]
    async fn job_definition_revisions_increment() {
        let s = svc();
        for expected in 1..=3 {
            let v = body_of(
                s.handle(req(
                    "/v1/registerjobdefinition",
                    json!({"jobDefinitionName": "jd", "type": "container"}),
                ))
                .await
                .unwrap(),
            );
            assert_eq!(v["revision"], expected);
        }
        let d = body_of(
            s.handle(req(
                "/v1/describejobdefinitions",
                json!({"jobDefinitionName": "jd"}),
            ))
            .await
            .unwrap(),
        );
        assert_eq!(d["jobDefinitions"].as_array().unwrap().len(), 3);
    }

    #[tokio::test]
    async fn unimplemented_op_errors_not_fakes() {
        let s = svc();
        let err = match s
            .handle(req("/v1/submitjob", json!({"jobName": "j"})))
            .await
        {
            Err(e) => e,
            Ok(_) => panic!("SubmitJob must not fake-succeed in the foundation batch"),
        };
        assert_eq!(err.status(), StatusCode::NOT_IMPLEMENTED);
    }

    #[test]
    fn routes_tag_family_by_method() {
        let mut r = req("/v1/tags/arn%3Aaws", json!({}));
        r.method = Method::GET;
        assert_eq!(
            BatchService::resolve_action(&r),
            Some("ListTagsForResource")
        );
        r.method = Method::DELETE;
        assert_eq!(BatchService::resolve_action(&r), Some("UntagResource"));
    }
}
