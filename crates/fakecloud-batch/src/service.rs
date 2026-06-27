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
    "CreateSchedulingPolicy",
    "UpdateSchedulingPolicy",
    "DeleteSchedulingPolicy",
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
    /// ECS backend so a submitted job runs as a REAL container (the wedge:
    /// every rival fakes Batch compute). `None` parks jobs at SUBMITTED
    /// honestly (unit tests / no container runtime), never auto-succeeds.
    ecs_state: Option<fakecloud_ecs::SharedEcsState>,
    ecs_runtime: Option<Arc<fakecloud_ecs::runtime::EcsRuntime>>,
}

impl BatchService {
    pub fn new(state: SharedBatchState) -> Self {
        Self {
            state,
            snapshot_store: None,
            snapshot_lock: Arc::new(AsyncMutex::new(())),
            ecs_state: None,
            ecs_runtime: None,
        }
    }

    pub fn with_snapshot_store(mut self, store: Arc<dyn SnapshotStore>) -> Self {
        self.snapshot_store = Some(store);
        self
    }

    /// Attach the ECS backend so SubmitJob launches a real container-backed
    /// task and drives the job status off its real exit code.
    pub fn with_ecs(
        mut self,
        state: fakecloud_ecs::SharedEcsState,
        runtime: Option<Arc<fakecloud_ecs::runtime::EcsRuntime>>,
    ) -> Self {
        self.ecs_state = Some(state);
        self.ecs_runtime = runtime;
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
            "UpdateComputeEnvironment" => self.update_compute_environment(req),
            "UpdateJobQueue" => self.update_job_queue(req),
            "RegisterJobDefinition" => self.register_job_definition(req),
            "DescribeJobDefinitions" => self.describe_job_definitions(req),
            "DeregisterJobDefinition" => self.deregister_job_definition(req),
            "CreateSchedulingPolicy" => self.create_scheduling_policy(req),
            "DescribeSchedulingPolicies" => self.describe_scheduling_policies(req),
            "ListSchedulingPolicies" => self.list_scheduling_policies(req),
            "UpdateSchedulingPolicy" => self.update_scheduling_policy(req),
            "DeleteSchedulingPolicy" => self.delete_scheduling_policy(req),
            "DescribeJobs" => self.describe_jobs(req),
            "ListJobs" => self.list_jobs(req),
            "CancelJob" => self.cancel_job(req),
            "TerminateJob" => self.terminate_job(req),
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

    fn update_compute_environment(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let name = arn_or_name(&body, "computeEnvironment")?;
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let ce = st
            .compute_environments
            .get_mut(&name)
            .ok_or_else(|| client_error("ClientException", format!("Object not found: {name}")))?;
        let arn = merge_updates(
            ce,
            &body,
            &[
                "state",
                "desiredvCpus",
                "computeResources",
                "serviceRole",
                "updatePolicy",
            ],
            "computeEnvironmentArn",
        );
        Ok(AwsResponse::ok_json(json!({
            "computeEnvironmentName": name,
            "computeEnvironmentArn": arn,
        })))
    }

    fn update_job_queue(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let name = arn_or_name(&body, "jobQueue")?;
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let q = st
            .job_queues
            .get_mut(&name)
            .ok_or_else(|| client_error("ClientException", format!("Object not found: {name}")))?;
        let arn = merge_updates(
            q,
            &body,
            &[
                "state",
                "priority",
                "computeEnvironmentOrder",
                "schedulingPolicyArn",
                "jobStateTimeLimitActions",
            ],
            "jobQueueArn",
        );
        Ok(AwsResponse::ok_json(json!({
            "jobQueueName": name,
            "jobQueueArn": arn,
        })))
    }

    // ---- Scheduling policies ----

    fn create_scheduling_policy(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let name = body
            .get("name")
            .and_then(Value::as_str)
            .ok_or_else(|| client_error("ClientException", "name is required"))?
            .to_string();
        let arn = self.arn(
            &req.account_id,
            &req.region,
            &format!("scheduling-policy/{name}"),
        );
        let mut stored = obj(&body);
        stored.insert("arn".into(), json!(arn));
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        if st.scheduling_policies.contains_key(&name) {
            return Err(client_error(
                "ClientException",
                format!("Object already exists: {name}"),
            ));
        }
        st.scheduling_policies
            .insert(name.clone(), Value::Object(stored));
        Ok(AwsResponse::ok_json(json!({ "name": name, "arn": arn })))
    }

    fn describe_scheduling_policies(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let wanted = string_set(&body, "arns");
        let accounts = self.state.read();
        let items: Vec<Value> = accounts
            .get(&req.account_id)
            .map(|st| {
                st.scheduling_policies
                    .values()
                    .filter(|p| {
                        wanted.is_empty()
                            || p.get("arn")
                                .and_then(Value::as_str)
                                .map(|a| wanted.contains(a))
                                .unwrap_or(false)
                    })
                    .cloned()
                    .collect()
            })
            .unwrap_or_default();
        Ok(AwsResponse::ok_json(json!({ "schedulingPolicies": items })))
    }

    fn list_scheduling_policies(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let accounts = self.state.read();
        let items: Vec<Value> = accounts
            .get(&req.account_id)
            .map(|st| {
                st.scheduling_policies
                    .values()
                    .filter_map(|p| p.get("arn").map(|a| json!({ "arn": a })))
                    .collect()
            })
            .unwrap_or_default();
        Ok(AwsResponse::ok_json(json!({ "schedulingPolicies": items })))
    }

    fn update_scheduling_policy(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let name = arn_or_name(&body, "arn")?;
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let p = st
            .scheduling_policies
            .get_mut(&name)
            .ok_or_else(|| client_error("ClientException", format!("Object not found: {name}")))?;
        merge_updates(p, &body, &["fairsharePolicy"], "arn");
        Ok(AwsResponse::ok_json(json!({})))
    }

    fn delete_scheduling_policy(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let name = arn_or_name(&body, "arn")?;
        let mut accounts = self.state.write();
        accounts
            .get_or_create(&req.account_id)
            .scheduling_policies
            .remove(&name);
        Ok(AwsResponse::ok_json(json!({})))
    }

    // ---- Jobs (control plane; real container execution lands in batch 2) ----

    async fn submit_job(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let job_name = body
            .get("jobName")
            .and_then(Value::as_str)
            .ok_or_else(|| client_error("ClientException", "jobName is required"))?
            .to_string();
        let job_queue = body
            .get("jobQueue")
            .and_then(Value::as_str)
            .ok_or_else(|| client_error("ClientException", "jobQueue is required"))?
            .to_string();
        let job_definition = body
            .get("jobDefinition")
            .and_then(Value::as_str)
            .ok_or_else(|| client_error("ClientException", "jobDefinition is required"))?
            .to_string();
        let job_id = Uuid::new_v4().to_string();
        let arn = self.arn(&req.account_id, &req.region, &format!("job/{job_id}"));
        let now = chrono::Utc::now().timestamp_millis();
        let array_size = body
            .pointer("/arrayProperties/size")
            .and_then(Value::as_i64)
            .filter(|n| *n > 1);

        let mut job = obj(&body);
        job.insert("jobId".into(), json!(job_id));
        job.insert("jobArn".into(), json!(arn));
        job.insert("jobName".into(), json!(job_name));
        job.insert("jobQueue".into(), json!(job_queue));
        job.insert("jobDefinition".into(), json!(job_definition));
        job.insert("createdAt".into(), json!(now));

        // Resolve the job definition's container properties (+ this submit's
        // containerOverrides) so the job runs the right image/command.
        let container = self.resolve_container(&req.account_id, &job_definition, &body);

        if let Some(size) = array_size {
            // Array job: the parent is a tracking record; spawn `size` child
            // jobs `<parent>:<index>`, each with AWS_BATCH_JOB_ARRAY_INDEX set,
            // and run them. The parent's status + statusSummary are computed
            // from the children at DescribeJobs time.
            job.insert("status".into(), json!("PENDING"));
            job.insert("arrayProperties".into(), json!({ "size": size }));
            {
                let mut accounts = self.state.write();
                accounts
                    .get_or_create(&req.account_id)
                    .jobs
                    .insert(job_id.clone(), Value::Object(job));
            }
            for index in 0..size {
                let child_id = format!("{job_id}:{index}");
                let child_arn = self.arn(&req.account_id, &req.region, &format!("job/{child_id}"));
                let mut child = serde_json::Map::new();
                child.insert("jobId".into(), json!(child_id));
                child.insert("jobArn".into(), json!(child_arn));
                child.insert("jobName".into(), json!(job_name));
                child.insert("jobQueue".into(), json!(job_queue));
                child.insert("jobDefinition".into(), json!(job_definition));
                child.insert("status".into(), json!("SUBMITTED"));
                child.insert("createdAt".into(), json!(now));
                child.insert(
                    "arrayProperties".into(),
                    json!({ "index": index, "statusSummary": {} }),
                );
                {
                    let mut accounts = self.state.write();
                    accounts
                        .get_or_create(&req.account_id)
                        .jobs
                        .insert(child_id.clone(), Value::Object(child));
                }
                let child_container = container.clone().map(|c| with_array_index_env(c, index));
                self.launch_job(req, &child_id, &job_name, child_container, now)
                    .await;
            }
        } else {
            job.insert("status".into(), json!("SUBMITTED"));
            {
                let mut accounts = self.state.write();
                accounts
                    .get_or_create(&req.account_id)
                    .jobs
                    .insert(job_id.clone(), Value::Object(job));
            }
            self.launch_job(req, &job_id, &job_name, container, now)
                .await;
        }

        Ok(AwsResponse::ok_json(json!({
            "jobArn": arn,
            "jobName": job_name,
            "jobId": job_id,
        })))
    }

    /// Launch a REAL container-backed task on the ECS engine when wired and the
    /// definition carries an image. With no ECS backend (unit tests / no
    /// container runtime) the job stays SUBMITTED honestly — never an
    /// auto-success, which is exactly the rival anti-pattern Batch beats.
    async fn launch_job(
        &self,
        req: &AwsRequest,
        job_id: &str,
        job_name: &str,
        container: Option<Value>,
        now: i64,
    ) {
        let (Some(ecs_state), Some(container)) = (self.ecs_state.clone(), container) else {
            return;
        };
        if container.get("image").and_then(Value::as_str).is_none() {
            return;
        }
        match self
            .launch_ecs_task(req, job_id, job_name, &container)
            .await
        {
            Ok((cluster, task_arn)) => {
                {
                    let mut accounts = self.state.write();
                    if let Some(j) = accounts
                        .get_or_create(&req.account_id)
                        .jobs
                        .get_mut(job_id)
                        .and_then(|j| j.as_object_mut())
                    {
                        j.insert("status".into(), json!("STARTING"));
                        j.insert("ecsCluster".into(), json!(cluster));
                        j.insert("ecsTaskArn".into(), json!(task_arn));
                        j.insert("startedAt".into(), json!(now));
                    }
                }
                self.spawn_status_sync(
                    ecs_state,
                    req.account_id.clone(),
                    req.region.clone(),
                    req.request_id.clone(),
                    job_id.to_string(),
                    cluster,
                    task_arn,
                );
            }
            Err(err) => {
                let mut accounts = self.state.write();
                if let Some(j) = accounts
                    .get_or_create(&req.account_id)
                    .jobs
                    .get_mut(job_id)
                    .and_then(|j| j.as_object_mut())
                {
                    j.insert("status".into(), json!("FAILED"));
                    j.insert("statusReason".into(), json!(err.message().to_string()));
                }
            }
        }
    }

    /// Resolve `containerProperties` from the stored job definition (by
    /// `name:revision`, bare name -> latest, or ARN), then overlay this
    /// submit's `containerOverrides` (command / environment / resourceRequirements).
    fn resolve_container(
        &self,
        account_id: &str,
        job_definition: &str,
        submit_body: &Value,
    ) -> Option<Value> {
        let key = job_definition.rsplit('/').next().unwrap_or(job_definition);
        let accounts = self.state.read();
        let st = accounts.get(account_id)?;
        // Exact "name:revision", else the highest-revision entry for the name.
        let jd = if key.contains(':') {
            st.job_definitions.get(key).cloned()
        } else {
            st.job_definitions
                .iter()
                .filter(|(k, _)| k.rsplit_once(':').map(|(n, _)| n) == Some(key))
                .max_by_key(|(k, _)| {
                    k.rsplit_once(':')
                        .and_then(|(_, r)| r.parse::<i64>().ok())
                        .unwrap_or(0)
                })
                .map(|(_, v)| v.clone())
        }?;
        let mut container = jd.get("containerProperties")?.as_object()?.clone();

        if let Some(ov) = submit_body
            .get("containerOverrides")
            .and_then(Value::as_object)
        {
            for f in ["command", "environment", "resourceRequirements"] {
                if let Some(v) = ov.get(f) {
                    container.insert(f.to_string(), v.clone());
                }
            }
        }
        Some(Value::Object(container))
    }

    /// Build a fresh `EcsService` over the shared ECS state (+ runtime for the
    /// launch path). Cross-service calls go through its public `handle()`,
    /// mirroring how autoscaling drives EC2 — so all of ECS's real container /
    /// portability / k8s handling is reused, not re-derived.
    fn ecs(&self, with_runtime: bool) -> Option<fakecloud_ecs::EcsService> {
        let state = self.ecs_state.clone()?;
        let mut svc = fakecloud_ecs::EcsService::new(state);
        if with_runtime {
            if let Some(rt) = self.ecs_runtime.clone() {
                svc = svc.with_runtime(rt);
            }
        }
        Some(svc)
    }

    /// Ensure the batch ECS cluster exists, register a task definition from the
    /// job's container properties, and RunTask it. Returns (cluster, taskArn).
    async fn launch_ecs_task(
        &self,
        req: &AwsRequest,
        job_id: &str,
        job_name: &str,
        container: &Value,
    ) -> Result<(String, String), AwsServiceError> {
        let ecs = self
            .ecs(true)
            .ok_or_else(|| client_error("ServerException", "ECS backend not configured"))?;
        let cluster = "fakecloud-batch".to_string();

        // Idempotent cluster create (ignore "already exists").
        let _ = ecs
            .handle(ecs_request(
                "CreateCluster",
                json!({ "clusterName": cluster }),
                req,
            ))
            .await;

        // Map Batch containerProperties -> an ECS container definition.
        let image = container.get("image").and_then(Value::as_str).unwrap_or("");
        let (vcpus, memory) = container_resources(container);
        let mut cdef = serde_json::Map::new();
        cdef.insert("name".into(), json!("default"));
        cdef.insert("image".into(), json!(image));
        cdef.insert("essential".into(), json!(true));
        cdef.insert("cpu".into(), json!((vcpus * 1024.0).round() as i64));
        cdef.insert("memory".into(), json!(memory));
        if let Some(cmd) = container.get("command").filter(|v| v.is_array()) {
            cdef.insert("command".into(), cmd.clone());
        }
        if let Some(env) = container.get("environment").filter(|v| v.is_array()) {
            cdef.insert("environment".into(), env.clone());
        }
        let family = format!("batch-{job_name}");
        let reg = ecs
            .handle(ecs_request(
                "RegisterTaskDefinition",
                json!({
                    "family": family,
                    "containerDefinitions": [Value::Object(cdef)],
                    "networkMode": "bridge",
                    "requiresCompatibilities": ["EC2"],
                }),
                req,
            ))
            .await?;
        let reg_body: Value = parse_body(&reg);
        let task_def_arn = reg_body
            .pointer("/taskDefinition/taskDefinitionArn")
            .and_then(Value::as_str)
            .map(String::from)
            .unwrap_or(family);

        let run = ecs
            .handle(ecs_request(
                "RunTask",
                json!({
                    "cluster": cluster,
                    "taskDefinition": task_def_arn,
                    "count": 1,
                    "launchType": "EC2",
                    "startedBy": format!("batch:{job_id}"),
                }),
                req,
            ))
            .await?;
        let run_body: Value = parse_body(&run);
        let task_arn = run_body
            .pointer("/tasks/0/taskArn")
            .and_then(Value::as_str)
            .map(String::from)
            .ok_or_else(|| client_error("ServerException", "RunTask returned no task"))?;
        Ok((cluster, task_arn))
    }

    /// Poll the ECS task in the background and map its real lifecycle +
    /// container exit code onto the Batch job status. NO auto-success: the job
    /// only reaches SUCCEEDED when the real container exits 0.
    #[allow(clippy::too_many_arguments)]
    fn spawn_status_sync(
        &self,
        ecs_state: fakecloud_ecs::SharedEcsState,
        account_id: String,
        region: String,
        request_id: String,
        job_id: String,
        cluster: String,
        task_arn: String,
    ) {
        let batch_state = self.state.clone();
        let snapshot_store = self.snapshot_store.clone();
        let snapshot_lock = self.snapshot_lock.clone();
        tokio::spawn(async move {
            let ecs = fakecloud_ecs::EcsService::new(ecs_state);
            let src = bare_request(&account_id, &region, &request_id);
            // Bound the watch so a stuck container can't poll forever.
            for _ in 0..900u32 {
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                let resp = match ecs
                    .handle(ecs_request(
                        "DescribeTasks",
                        json!({ "cluster": cluster, "tasks": [task_arn] }),
                        &src,
                    ))
                    .await
                {
                    Ok(r) => r,
                    Err(_) => continue,
                };
                let body = parse_body(&resp);
                let Some(task) = body.pointer("/tasks/0") else {
                    continue;
                };
                let last = task.get("lastStatus").and_then(Value::as_str).unwrap_or("");
                let (new_status, exit_code, reason) = match last {
                    "RUNNING" => ("RUNNING", None, None),
                    "STOPPED" => {
                        let code = task
                            .pointer("/containers/0/exitCode")
                            .and_then(Value::as_i64);
                        let reason = task
                            .get("stoppedReason")
                            .and_then(Value::as_str)
                            .map(String::from);
                        if code == Some(0) {
                            ("SUCCEEDED", code, reason)
                        } else {
                            (
                                "FAILED",
                                code,
                                reason.or(Some("Essential container exited".into())),
                            )
                        }
                    }
                    _ => continue,
                };
                let terminal = matches!(new_status, "SUCCEEDED" | "FAILED");
                {
                    let mut accounts = batch_state.write();
                    if let Some(j) = accounts
                        .get_or_create(&account_id)
                        .jobs
                        .get_mut(&job_id)
                        .and_then(|j| j.as_object_mut())
                    {
                        j.insert("status".into(), json!(new_status));
                        if let Some(c) = exit_code {
                            // AWS Batch surfaces the exit code under
                            // `container.exitCode`; mirror that shape.
                            let container = j
                                .entry("container".to_string())
                                .or_insert_with(|| json!({}));
                            if let Some(o) = container.as_object_mut() {
                                o.insert("exitCode".into(), json!(c));
                            }
                        }
                        if let Some(r) = reason {
                            j.insert("statusReason".into(), json!(r.clone()));
                            if let Some(o) = j.get_mut("container").and_then(|v| v.as_object_mut())
                            {
                                o.insert("reason".into(), json!(r));
                            }
                        }
                        if terminal {
                            j.insert(
                                "stoppedAt".into(),
                                json!(chrono::Utc::now().timestamp_millis()),
                            );
                        }
                    }
                }
                save_snapshot_now(&batch_state, &snapshot_store, &snapshot_lock).await;
                if terminal {
                    break;
                }
            }
        });
    }

    fn describe_jobs(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let wanted = string_set(&body, "jobs");
        let accounts = self.state.read();
        let items: Vec<Value> = accounts
            .get(&req.account_id)
            .map(|st| {
                st.jobs
                    .values()
                    .filter(|j| {
                        wanted.is_empty()
                            || j.get("jobId")
                                .and_then(Value::as_str)
                                .map(|id| wanted.contains(id))
                                .unwrap_or(false)
                    })
                    .map(|j| {
                        // An array parent (arrayProperties.size set, no own ECS
                        // task) reflects its children's aggregate status +
                        // statusSummary computed live.
                        let id = j.get("jobId").and_then(Value::as_str).unwrap_or("");
                        let is_parent = j.pointer("/arrayProperties/size").is_some();
                        if !is_parent {
                            return j.clone();
                        }
                        let prefix = format!("{id}:");
                        let children: Vec<&Value> = st
                            .jobs
                            .iter()
                            .filter(|(k, _)| k.starts_with(&prefix))
                            .map(|(_, v)| v)
                            .collect();
                        let (summary, status) = array_status_summary(&children);
                        let mut out = j.clone();
                        if let Some(o) = out.as_object_mut() {
                            o.insert("status".into(), json!(status));
                            if let Some(ap) =
                                o.get_mut("arrayProperties").and_then(|v| v.as_object_mut())
                            {
                                ap.insert("statusSummary".into(), summary);
                            }
                        }
                        out
                    })
                    .collect()
            })
            .unwrap_or_default();
        Ok(AwsResponse::ok_json(json!({ "jobs": items })))
    }

    fn list_jobs(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let queue = body
            .get("jobQueue")
            .and_then(Value::as_str)
            .map(String::from);
        let status = body
            .get("jobStatus")
            .and_then(Value::as_str)
            .map(String::from);
        let accounts = self.state.read();
        let items: Vec<Value> = accounts
            .get(&req.account_id)
            .map(|st| {
                st.jobs
                    .values()
                    .filter(|j| {
                        queue
                            .as_deref()
                            .is_none_or(|q| j.get("jobQueue").and_then(Value::as_str) == Some(q))
                            && status
                                .as_deref()
                                .is_none_or(|s| j.get("status").and_then(Value::as_str) == Some(s))
                    })
                    .map(|j| {
                        json!({
                            "jobId": j.get("jobId").cloned().unwrap_or(Value::Null),
                            "jobName": j.get("jobName").cloned().unwrap_or(Value::Null),
                            "status": j.get("status").cloned().unwrap_or(Value::Null),
                            "createdAt": j.get("createdAt").cloned().unwrap_or(Value::Null),
                        })
                    })
                    .collect()
            })
            .unwrap_or_default();
        Ok(AwsResponse::ok_json(json!({ "jobSummaryList": items })))
    }

    fn cancel_job(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        self.stop_job(req, &["SUBMITTED", "PENDING", "RUNNABLE"], "CancelJob")
    }

    fn terminate_job(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        self.stop_job(
            req,
            &["SUBMITTED", "PENDING", "RUNNABLE", "STARTING", "RUNNING"],
            "TerminateJob",
        )
    }

    fn stop_job(
        &self,
        req: &AwsRequest,
        cancelable: &[&str],
        op: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let job_id = body
            .get("jobId")
            .and_then(Value::as_str)
            .ok_or_else(|| client_error("ClientException", "jobId is required"))?
            .to_string();
        let reason = body
            .get("reason")
            .and_then(Value::as_str)
            .unwrap_or(op)
            .to_string();
        let mut accounts = self.state.write();
        if let Some(job) = accounts
            .get_or_create(&req.account_id)
            .jobs
            .get_mut(&job_id)
        {
            if let Some(o) = job.as_object_mut() {
                let cur = o.get("status").and_then(Value::as_str).unwrap_or("");
                if cancelable.contains(&cur) {
                    o.insert("status".into(), json!("FAILED"));
                    o.insert("statusReason".into(), json!(reason));
                }
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

/// Apply the named mutable fields from `body` onto the stored resource and
/// return its ARN (read from `arn_key`).
fn merge_updates(stored: &mut Value, body: &Value, fields: &[&str], arn_key: &str) -> String {
    if let Some(o) = stored.as_object_mut() {
        for f in fields {
            if let Some(v) = body.get(*f) {
                o.insert((*f).to_string(), v.clone());
            }
        }
        o.get(arn_key)
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string()
    } else {
        String::new()
    }
}

/// Build an ECS `AwsRequest` (awsJson1.1) carrying the originating
/// account/region so the launched task lands in the caller's account.
fn ecs_request(action: &str, body: Value, src: &AwsRequest) -> AwsRequest {
    AwsRequest {
        service: "ecs".to_string(),
        action: action.to_string(),
        region: src.region.clone(),
        account_id: src.account_id.clone(),
        request_id: src.request_id.clone(),
        headers: http::HeaderMap::new(),
        query_params: std::collections::HashMap::new(),
        body: bytes::Bytes::from(serde_json::to_vec(&body).unwrap_or_default()),
        body_stream: parking_lot::Mutex::new(None),
        path_segments: Vec::new(),
        raw_path: "/".to_string(),
        raw_query: String::new(),
        method: Method::POST,
        is_query_protocol: false,
        access_key_id: None,
        principal: None,
    }
}

/// A minimal `AwsRequest` for the background poller (carries only the
/// originating account/region/request-id).
fn bare_request(account_id: &str, region: &str, request_id: &str) -> AwsRequest {
    AwsRequest {
        service: "batch".to_string(),
        action: String::new(),
        region: region.to_string(),
        account_id: account_id.to_string(),
        request_id: request_id.to_string(),
        headers: http::HeaderMap::new(),
        query_params: std::collections::HashMap::new(),
        body: bytes::Bytes::new(),
        body_stream: parking_lot::Mutex::new(None),
        path_segments: Vec::new(),
        raw_path: "/".to_string(),
        raw_query: String::new(),
        method: Method::POST,
        is_query_protocol: false,
        access_key_id: None,
        principal: None,
    }
}

/// Parse a JSON `AwsResponse` body, or `Null` on failure.
fn parse_body(resp: &AwsResponse) -> Value {
    serde_json::from_slice(resp.body.expect_bytes()).unwrap_or(Value::Null)
}

/// Resolve (vcpus, memoryMiB) from Batch container properties — either the
/// legacy `vcpus`/`memory` fields or the modern `resourceRequirements` list.
fn container_resources(container: &Value) -> (f64, i64) {
    let mut vcpus = container
        .get("vcpus")
        .and_then(Value::as_f64)
        .unwrap_or(1.0);
    let mut memory = container
        .get("memory")
        .and_then(Value::as_i64)
        .unwrap_or(512);
    if let Some(rr) = container
        .get("resourceRequirements")
        .and_then(Value::as_array)
    {
        for r in rr {
            let ty = r.get("type").and_then(Value::as_str).unwrap_or("");
            let val = r
                .get("value")
                .and_then(Value::as_str)
                .and_then(|s| s.parse::<f64>().ok());
            match (ty, val) {
                ("VCPU", Some(v)) => vcpus = v,
                ("MEMORY", Some(v)) => memory = v as i64,
                _ => {}
            }
        }
    }
    (vcpus.max(0.25), memory.max(4))
}

/// Persist the Batch state now (used by the background status poller, which
/// can't call the `&self` snapshot method).
async fn save_snapshot_now(
    state: &SharedBatchState,
    store: &Option<Arc<dyn SnapshotStore>>,
    lock: &Arc<AsyncMutex<()>>,
) {
    let Some(store) = store.clone() else {
        return;
    };
    let _guard = lock.lock().await;
    let bytes = {
        let snap = BatchSnapshot {
            schema_version: BATCH_SNAPSHOT_SCHEMA_VERSION,
            accounts: Some(state.read().clone()),
        };
        serde_json::to_vec(&snap).unwrap_or_default()
    };
    let _ = tokio::task::spawn_blocking(move || store.save(&bytes)).await;
}

/// Inject `AWS_BATCH_JOB_ARRAY_INDEX=<index>` into a container's environment so
/// each array child can select its slice of work (the AWS Batch contract).
fn with_array_index_env(mut container: Value, index: i64) -> Value {
    if let Some(obj) = container.as_object_mut() {
        let env = obj
            .entry("environment".to_string())
            .or_insert_with(|| json!([]));
        if let Some(arr) = env.as_array_mut() {
            arr.retain(|e| {
                e.get("name").and_then(Value::as_str) != Some("AWS_BATCH_JOB_ARRAY_INDEX")
            });
            arr.push(json!({ "name": "AWS_BATCH_JOB_ARRAY_INDEX", "value": index.to_string() }));
        }
    }
    container
}

/// Aggregate an array parent's child statuses into AWS Batch's
/// `arrayProperties.statusSummary` counts + an overall parent status.
fn array_status_summary(children: &[&Value]) -> (Value, &'static str) {
    let mut summary = serde_json::Map::new();
    for s in [
        "SUBMITTED",
        "PENDING",
        "RUNNABLE",
        "STARTING",
        "RUNNING",
        "SUCCEEDED",
        "FAILED",
    ] {
        let n = children
            .iter()
            .filter(|c| c.get("status").and_then(Value::as_str) == Some(s))
            .count();
        summary.insert(s.to_string(), json!(n));
    }
    let total = children.len();
    let succeeded = summary["SUCCEEDED"].as_u64().unwrap_or(0) as usize;
    let failed = summary["FAILED"].as_u64().unwrap_or(0) as usize;
    let status = if total > 0 && succeeded + failed == total {
        if failed > 0 {
            "FAILED"
        } else {
            "SUCCEEDED"
        }
    } else if summary["RUNNING"].as_u64().unwrap_or(0) > 0
        || summary["STARTING"].as_u64().unwrap_or(0) > 0
    {
        "RUNNING"
    } else {
        "PENDING"
    };
    (Value::Object(summary), status)
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
        // SubmitJob is async (it launches a real ECS task); everything else is
        // a synchronous metadata op.
        let result = if action == "SubmitJob" {
            self.submit_job(&req).await
        } else {
            self.dispatch(action, &req)
        };
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
        // CreateConsumableResource is not yet implemented (a later batch); it
        // must return a faithful 501, never a fake success.
        let err = match s
            .handle(req(
                "/v1/createconsumableresource",
                json!({"consumableResourceName": "r"}),
            ))
            .await
        {
            Err(e) => e,
            Ok(_) => panic!("unimplemented op must not fake-succeed"),
        };
        assert_eq!(err.status(), StatusCode::NOT_IMPLEMENTED);
    }

    #[tokio::test]
    async fn job_submit_describe_cancel_lifecycle() {
        let s = svc();
        let sub = body_of(
            s.handle(req(
                "/v1/submitjob",
                json!({"jobName": "j1", "jobQueue": "q1", "jobDefinition": "jd:1"}),
            ))
            .await
            .unwrap(),
        );
        let job_id = sub["jobId"].as_str().unwrap().to_string();
        assert_eq!(sub["jobName"], "j1");

        let d = body_of(
            s.handle(req("/v1/describejobs", json!({"jobs": [job_id]})))
                .await
                .unwrap(),
        );
        assert_eq!(d["jobs"][0]["status"], "SUBMITTED");

        let l = body_of(
            s.handle(req("/v1/listjobs", json!({"jobQueue": "q1"})))
                .await
                .unwrap(),
        );
        assert_eq!(l["jobSummaryList"].as_array().unwrap().len(), 1);

        s.handle(req(
            "/v1/canceljob",
            json!({"jobId": job_id, "reason": "stop it"}),
        ))
        .await
        .unwrap();
        let d2 = body_of(
            s.handle(req("/v1/describejobs", json!({"jobs": [job_id]})))
                .await
                .unwrap(),
        );
        assert_eq!(d2["jobs"][0]["status"], "FAILED");
        assert_eq!(d2["jobs"][0]["statusReason"], "stop it");
    }

    #[tokio::test]
    async fn scheduling_policy_crud() {
        let s = svc();
        let c = body_of(
            s.handle(req("/v1/createschedulingpolicy", json!({"name": "sp1"})))
                .await
                .unwrap(),
        );
        let arn = c["arn"].as_str().unwrap().to_string();
        assert!(arn.contains("scheduling-policy/sp1"));
        let d = body_of(
            s.handle(req(
                "/v1/describeschedulingpolicies",
                json!({"arns": [arn]}),
            ))
            .await
            .unwrap(),
        );
        assert_eq!(d["schedulingPolicies"].as_array().unwrap().len(), 1);
        s.handle(req("/v1/deleteschedulingpolicy", json!({"arn": "sp1"})))
            .await
            .unwrap();
        let l = body_of(
            s.handle(req("/v1/listschedulingpolicies", json!({})))
                .await
                .unwrap(),
        );
        assert_eq!(l["schedulingPolicies"].as_array().unwrap().len(), 0);
    }

    #[tokio::test]
    async fn update_compute_environment_persists() {
        let s = svc();
        s.handle(req(
            "/v1/createcomputeenvironment",
            json!({"computeEnvironmentName": "ce1", "state": "ENABLED"}),
        ))
        .await
        .unwrap();
        s.handle(req(
            "/v1/updatecomputeenvironment",
            json!({"computeEnvironment": "ce1", "state": "DISABLED"}),
        ))
        .await
        .unwrap();
        let d = body_of(
            s.handle(req("/v1/describecomputeenvironments", json!({})))
                .await
                .unwrap(),
        );
        assert_eq!(d["computeEnvironments"][0]["state"], "DISABLED");
    }

    #[test]
    fn container_resources_from_both_shapes() {
        // Legacy vcpus/memory.
        let (v, m) = container_resources(&json!({"vcpus": 2, "memory": 2048}));
        assert_eq!(v, 2.0);
        assert_eq!(m, 2048);
        // Modern resourceRequirements override.
        let (v, m) = container_resources(&json!({
            "resourceRequirements": [
                {"type": "VCPU", "value": "4"},
                {"type": "MEMORY", "value": "8192"}
            ]
        }));
        assert_eq!(v, 4.0);
        assert_eq!(m, 8192);
        // Defaults + floors.
        let (v, m) = container_resources(&json!({}));
        assert_eq!(v, 1.0);
        assert_eq!(m, 512);
    }

    #[tokio::test]
    async fn resolve_container_picks_latest_revision_and_overrides() {
        let s = svc();
        for cmd in [json!(["echo", "v1"]), json!(["echo", "v2"])] {
            s.handle(req(
                "/v1/registerjobdefinition",
                json!({
                    "jobDefinitionName": "jd",
                    "type": "container",
                    "containerProperties": {"image": "alpine", "command": cmd}
                }),
            ))
            .await
            .unwrap();
        }
        // Bare name -> latest revision (v2).
        let c = s
            .resolve_container("123456789012", "jd", &json!({}))
            .unwrap();
        assert_eq!(c["command"], json!(["echo", "v2"]));
        // containerOverrides win.
        let c = s
            .resolve_container(
                "123456789012",
                "jd:1",
                &json!({"containerOverrides": {"command": ["overridden"]}}),
            )
            .unwrap();
        assert_eq!(c["command"], json!(["overridden"]));
        assert_eq!(c["image"], "alpine");
    }

    #[tokio::test]
    async fn submit_without_ecs_parks_at_submitted_never_auto_succeeds() {
        // No ECS backend wired: the job must stay SUBMITTED (honest), not
        // fake a SUCCEEDED like the rivals do.
        let s = svc();
        s.handle(req(
            "/v1/registerjobdefinition",
            json!({"jobDefinitionName": "jd", "type": "container",
                   "containerProperties": {"image": "alpine"}}),
        ))
        .await
        .unwrap();
        let sub = body_of(
            s.handle(req(
                "/v1/submitjob",
                json!({"jobName": "j", "jobQueue": "q", "jobDefinition": "jd"}),
            ))
            .await
            .unwrap(),
        );
        let id = sub["jobId"].as_str().unwrap().to_string();
        let d = body_of(
            s.handle(req("/v1/describejobs", json!({"jobs": [id]})))
                .await
                .unwrap(),
        );
        assert_eq!(d["jobs"][0]["status"], "SUBMITTED");
    }

    #[test]
    fn array_index_env_injected() {
        let c = with_array_index_env(json!({"image": "alpine"}), 5);
        let env = c["environment"].as_array().unwrap();
        assert!(env
            .iter()
            .any(|e| e["name"] == "AWS_BATCH_JOB_ARRAY_INDEX" && e["value"] == "5"));
    }

    #[tokio::test]
    async fn array_job_spawns_children_and_parent_aggregates() {
        let s = svc();
        s.handle(req(
            "/v1/registerjobdefinition",
            json!({"jobDefinitionName": "jd", "type": "container",
                   "containerProperties": {"image": "alpine"}}),
        ))
        .await
        .unwrap();
        let sub = body_of(
            s.handle(req(
                "/v1/submitjob",
                json!({"jobName": "arr", "jobQueue": "q", "jobDefinition": "jd",
                       "arrayProperties": {"size": 3}}),
            ))
            .await
            .unwrap(),
        );
        let parent = sub["jobId"].as_str().unwrap().to_string();

        // Three children exist (no ECS wired -> they stay SUBMITTED honestly).
        let listed = body_of(s.handle(req("/v1/listjobs", json!({}))).await.unwrap());
        let n_children = listed["jobSummaryList"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|j| {
                j["jobId"]
                    .as_str()
                    .unwrap_or("")
                    .starts_with(&format!("{parent}:"))
            })
            .count();
        assert_eq!(n_children, 3);

        // The parent aggregates: PENDING with statusSummary SUBMITTED=3.
        let d = body_of(
            s.handle(req("/v1/describejobs", json!({"jobs": [parent]})))
                .await
                .unwrap(),
        );
        let p = &d["jobs"][0];
        assert_eq!(p["status"], "PENDING");
        assert_eq!(p["arrayProperties"]["statusSummary"]["SUBMITTED"], 3);
        assert_eq!(p["arrayProperties"]["size"], 3);
    }

    #[test]
    fn array_summary_terminal_states() {
        let succ = json!({"status": "SUCCEEDED"});
        let fail = json!({"status": "FAILED"});
        let run = json!({"status": "RUNNING"});
        let (_, st) = array_status_summary(&[&succ, &succ]);
        assert_eq!(st, "SUCCEEDED");
        let (_, st) = array_status_summary(&[&succ, &fail]);
        assert_eq!(st, "FAILED");
        let (_, st) = array_status_summary(&[&succ, &run]);
        assert_eq!(st, "RUNNING");
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
