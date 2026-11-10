//! `AWS::Batch::*` CloudFormation provisioning. Creates compute environments,
//! job queues, and job definitions as real records in the `batch` service
//! state (the control plane; real job execution is a runtime concern, not
//! CFN-time). Writes through the batch service's snapshot hook so the
//! resources survive a restart (the #1766 lesson).

use serde_json::{json, Map, Value};
use uuid::Uuid;

use super::{ProvisionResult, ResourceDefinition, ResourceProvisioner, StackResource};

fn prop_str<'a>(p: &'a Value, k: &str) -> Option<&'a str> {
    p.get(k).and_then(|v| v.as_str())
}

/// Copy a CloudFormation PascalCase property into the stored record under its
/// camelCase API key (so a CFN-created resource reads back identically to an
/// API-created one).
fn copy_prop(stored: &mut Map<String, Value>, props: &Value, cfn_key: &str, api_key: &str) {
    if let Some(v) = props.get(cfn_key) {
        stored.insert(api_key.to_string(), v.clone());
    }
}

impl ResourceProvisioner {
    fn batch_arn(&self, kind: &str, name: &str) -> String {
        fakecloud_batch::batch_arn(&self.region, &self.account_id, &format!("{kind}/{name}"))
    }

    /// Seed the batch tag store from a resource's CFN `Tags` map (a JSON object),
    /// so a CFN-created resource's tags survive on `ListTagsForResource` /
    /// `Describe*` exactly like an API-created one's.
    fn seed_batch_tags(&self, arn: &str, props: &Value) {
        let Some(tags) = props.get("Tags").and_then(|v| v.as_object()) else {
            return;
        };
        let mut state = self.batch_state.write();
        let entry = state
            .get_or_create(&self.account_id)
            .tags
            .entry(arn.to_string())
            .or_default();
        for (k, v) in tags {
            if let Some(s) = v.as_str() {
                entry.insert(k.clone(), s.to_string());
            }
        }
    }

    pub(super) fn create_batch_compute_environment(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let name = prop_str(props, "ComputeEnvironmentName")
            .map(String::from)
            .unwrap_or_else(|| self.physical_name(resource));
        let arn = self.batch_arn("compute-environment", &name);
        let uuid = Uuid::new_v4().to_string();
        let mut stored = Map::new();
        stored.insert("computeEnvironmentName".into(), json!(name));
        stored.insert("computeEnvironmentArn".into(), json!(arn));
        stored.insert(
            "type".into(),
            json!(prop_str(props, "Type").unwrap_or("MANAGED")),
        );
        stored.insert(
            "state".into(),
            json!(prop_str(props, "State").unwrap_or("ENABLED")),
        );
        stored.insert("status".into(), json!("VALID"));
        stored.insert("statusReason".into(), json!("ComputeEnvironment Healthy"));
        // Every managed/unmanaged CE is backed by an ECS cluster whose ARN the
        // live CreateComputeEnvironment synthesizes; mirror it so CFN-created
        // and API-created environments read back identically.
        stored.insert(
            "ecsClusterArn".into(),
            json!(fakecloud_ecs::ecs_arn(
                &self.region,
                &self.account_id,
                &format!("cluster/AWSBatch-{name}-{uuid}")
            )),
        );
        stored.insert("uuid".into(), json!(uuid));
        for (cfn, api) in [
            ("ComputeResources", "computeResources"),
            ("ServiceRole", "serviceRole"),
            ("UnmanagedvCpus", "unmanagedvCpus"),
            ("EksConfiguration", "eksConfiguration"),
            ("Context", "context"),
            ("ReplaceComputeEnvironment", "replaceComputeEnvironment"),
            ("Tags", "tags"),
        ] {
            copy_prop(&mut stored, props, cfn, api);
        }
        self.batch_state
            .write()
            .get_or_create(&self.account_id)
            .compute_environments
            .insert(name.clone(), Value::Object(stored));
        self.seed_batch_tags(&arn, props);
        Ok(ProvisionResult::new(arn.clone()).with("ComputeEnvironmentArn", arn))
    }

    pub(super) fn create_batch_job_queue(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let name = prop_str(props, "JobQueueName")
            .map(String::from)
            .unwrap_or_else(|| self.physical_name(resource));
        let arn = self.batch_arn("job-queue", &name);
        let mut stored = Map::new();
        stored.insert("jobQueueName".into(), json!(name));
        stored.insert("jobQueueArn".into(), json!(arn));
        stored.insert(
            "state".into(),
            json!(prop_str(props, "State").unwrap_or("ENABLED")),
        );
        stored.insert("status".into(), json!("VALID"));
        stored.insert("statusReason".into(), json!("JobQueue Healthy"));
        stored.insert(
            "priority".into(),
            props.get("Priority").cloned().unwrap_or(json!(1)),
        );
        for (cfn, api) in [
            ("ComputeEnvironmentOrder", "computeEnvironmentOrder"),
            ("SchedulingPolicyArn", "schedulingPolicyArn"),
            ("JobStateTimeLimitActions", "jobStateTimeLimitActions"),
            ("Tags", "tags"),
        ] {
            copy_prop(&mut stored, props, cfn, api);
        }
        fakecloud_batch::normalize_compute_environment_order(&mut stored);
        self.batch_state
            .write()
            .get_or_create(&self.account_id)
            .job_queues
            .insert(name.clone(), Value::Object(stored));
        self.seed_batch_tags(&arn, props);
        Ok(ProvisionResult::new(arn.clone()).with("JobQueueArn", arn))
    }

    pub(super) fn create_batch_job_definition(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let name = prop_str(props, "JobDefinitionName")
            .map(String::from)
            .unwrap_or_else(|| self.physical_name(resource));
        let arn;
        {
            let mut state = self.batch_state.write();
            let acct = state.get_or_create(&self.account_id);
            let revision = acct.job_def_revisions.entry(name.clone()).or_insert(0);
            *revision += 1;
            let revision = *revision;
            arn = fakecloud_batch::batch_arn(
                &self.region,
                &self.account_id,
                &format!("job-definition/{name}:{revision}"),
            );
            let mut stored = Map::new();
            stored.insert("jobDefinitionName".into(), json!(name));
            stored.insert("jobDefinitionArn".into(), json!(arn));
            stored.insert(
                "type".into(),
                json!(prop_str(props, "Type").unwrap_or("container")),
            );
            stored.insert("revision".into(), json!(revision));
            stored.insert("status".into(), json!("ACTIVE"));
            for (cfn, api) in [
                ("ContainerProperties", "containerProperties"),
                ("Parameters", "parameters"),
                ("Timeout", "timeout"),
                ("RetryStrategy", "retryStrategy"),
                ("PlatformCapabilities", "platformCapabilities"),
                ("PropagateTags", "propagateTags"),
                ("SchedulingPriority", "schedulingPriority"),
                ("NodeProperties", "nodeProperties"),
                ("EksProperties", "eksProperties"),
                ("Tags", "tags"),
            ] {
                copy_prop(&mut stored, props, cfn, api);
            }
            // AWS defaults the optional containerProperties list members to empty
            // arrays and echoes them on describe; the live RegisterJobDefinition
            // does the same, so a CFN-created definition must too.
            if let Some(cp) = stored
                .get_mut("containerProperties")
                .and_then(Value::as_object_mut)
            {
                for key in [
                    "environment",
                    "mountPoints",
                    "resourceRequirements",
                    "secrets",
                    "ulimits",
                    "volumes",
                ] {
                    cp.entry(key.to_string()).or_insert_with(|| json!([]));
                }
            }
            acct.job_definitions
                .insert(format!("{name}:{revision}"), Value::Object(stored));
        }
        self.seed_batch_tags(&arn, props);
        Ok(ProvisionResult::new(arn))
    }

    pub(super) fn create_batch_scheduling_policy(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let name = prop_str(props, "Name")
            .map(String::from)
            .unwrap_or_else(|| self.physical_name(resource));
        let arn = fakecloud_batch::batch_arn(
            &self.region,
            &self.account_id,
            &format!("scheduling-policy/{name}"),
        );
        let mut stored = Map::new();
        stored.insert("name".into(), json!(name));
        stored.insert("arn".into(), json!(arn));
        copy_prop(&mut stored, props, "FairsharePolicy", "fairsharePolicy");
        copy_prop(&mut stored, props, "Tags", "tags");
        self.batch_state
            .write()
            .get_or_create(&self.account_id)
            .scheduling_policies
            .insert(name.clone(), Value::Object(stored));
        self.seed_batch_tags(&arn, props);
        Ok(ProvisionResult::new(arn.clone()).with("Arn", arn))
    }

    /// Delete a Batch resource by physical id (the ARN returned at create);
    /// matches the stored record by its `*Arn` field so hyphenated resource
    /// names round-trip cleanly.
    pub(super) fn delete_batch(&self, resource_type: &str, physical_id: &str) {
        let mut state = self.batch_state.write();
        let acct = state.get_or_create(&self.account_id);
        let arn_matches =
            |v: &Value, key: &str| v.get(key).and_then(|a| a.as_str()) == Some(physical_id);
        match resource_type {
            "AWS::Batch::ComputeEnvironment" => {
                acct.compute_environments
                    .retain(|_, v| !arn_matches(v, "computeEnvironmentArn"));
            }
            "AWS::Batch::JobQueue" => {
                acct.job_queues
                    .retain(|_, v| !arn_matches(v, "jobQueueArn"));
            }
            "AWS::Batch::JobDefinition" => {
                acct.job_definitions
                    .retain(|_, v| !arn_matches(v, "jobDefinitionArn"));
            }
            "AWS::Batch::SchedulingPolicy" => {
                acct.scheduling_policies
                    .retain(|_, v| !arn_matches(v, "arn"));
            }
            _ => {}
        }
    }
}

/// The kinds of Batch resource the CloudFormation provisioner drives through
/// the Batch API handlers: (create, update, delete action, the id request
/// key update/delete take, the ARN response key, and the name property).
struct BatchApiKind {
    create: &'static str,
    update: &'static str,
    delete: &'static str,
    id_key: &'static str,
    arn_key: &'static str,
    name_prop: &'static str,
}

fn batch_api_kind(resource_type: &str) -> Option<BatchApiKind> {
    Some(match resource_type {
        "AWS::Batch::ConsumableResource" => BatchApiKind {
            create: "CreateConsumableResource",
            update: "UpdateConsumableResource",
            delete: "DeleteConsumableResource",
            id_key: "consumableResource",
            arn_key: "consumableResourceArn",
            name_prop: "ConsumableResourceName",
        },
        "AWS::Batch::ServiceEnvironment" => BatchApiKind {
            create: "CreateServiceEnvironment",
            update: "UpdateServiceEnvironment",
            delete: "DeleteServiceEnvironment",
            id_key: "serviceEnvironment",
            arn_key: "serviceEnvironmentArn",
            name_prop: "ServiceEnvironmentName",
        },
        "AWS::Batch::QuotaShare" => BatchApiKind {
            create: "CreateQuotaShare",
            update: "UpdateQuotaShare",
            delete: "DeleteQuotaShare",
            id_key: "quotaShareArn",
            arn_key: "quotaShareArn",
            name_prop: "QuotaShareName",
        },
        _ => return None,
    })
}

/// A CFN property map as the Batch API's camelCase JSON body. Tag keys are
/// user data and keep their case.
fn batch_api_body(props: &Value) -> Map<String, Value> {
    let mut out = Map::new();
    if let Some(obj) = props.as_object() {
        for (k, v) in obj {
            let mut key = String::with_capacity(k.len());
            let mut chars = k.chars();
            if let Some(first) = chars.next() {
                key.extend(first.to_lowercase());
                key.push_str(chars.as_str());
            }
            let value = if k == "Tags" {
                v.clone()
            } else {
                super::lowercase_first_keys(v.clone())
            };
            out.insert(key, value);
        }
    }
    out
}

impl ResourceProvisioner {
    fn batch_dispatch(&self, action: &str, body: Map<String, Value>) -> Result<Value, String> {
        let req = fakecloud_core::service::AwsRequest {
            service: "batch".to_string(),
            action: action.to_string(),
            region: self.region.clone(),
            account_id: self.account_id.clone(),
            request_id: "cfn".to_string(),
            headers: http::HeaderMap::new(),
            query_params: Default::default(),
            body: bytes::Bytes::from(Value::Object(body).to_string()),
            body_stream: parking_lot::Mutex::new(None),
            path_segments: Vec::new(),
            raw_path: "/".to_string(),
            raw_query: String::new(),
            method: http::Method::POST,
            is_query_protocol: false,
            access_key_id: None,
            principal: None,
        };
        let resp = fakecloud_batch::BatchService::new(self.batch_state.clone())
            .provision_sync(action, &req)
            .map_err(|e| format!("{}: {}", e.code(), e.message()))?;
        Ok(serde_json::from_slice(resp.body.expect_bytes()).unwrap_or(Value::Null))
    }

    /// `AWS::Batch::ConsumableResource` / `ServiceEnvironment` /
    /// `QuotaShare`, created through the Batch API handler. `Ref` returns the
    /// ARN.
    pub(super) fn create_batch_api_resource(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let kind = batch_api_kind(&resource.resource_type)
            .ok_or_else(|| format!("unsupported type {}", resource.resource_type))?;
        let mut body = batch_api_body(&resource.properties);
        let name_key = batch_api_body(&serde_json::json!({ kind.name_prop: "" }))
            .keys()
            .next()
            .cloned()
            .unwrap_or_default();
        if !body.contains_key(&name_key) {
            body.insert(name_key, json!(self.physical_name(resource)));
        }
        let out = self.batch_dispatch(kind.create, body)?;
        let arn = out[kind.arn_key].as_str().unwrap_or_default().to_string();
        let attr = format!("{}{}", kind.arn_key[..1].to_uppercase(), &kind.arn_key[1..]);
        Ok(ProvisionResult::new(arn.clone()).with(&attr, arn))
    }

    /// In-place update through the Batch Update* handler. A consumable
    /// resource's `TotalQuantity` is SET to the new value.
    pub(super) fn update_batch_api_resource(
        &self,
        existing: &StackResource,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let kind = batch_api_kind(&resource.resource_type)
            .ok_or_else(|| format!("unsupported type {}", resource.resource_type))?;
        let props = &resource.properties;
        let mut body = Map::new();
        body.insert(kind.id_key.to_string(), json!(existing.physical_id));
        if resource.resource_type == "AWS::Batch::ConsumableResource" {
            if let Some(q) = props.get("TotalQuantity") {
                body.insert("operation".into(), json!("SET"));
                body.insert("quantity".into(), q.clone());
            }
        } else {
            for (k, v) in batch_api_body(props) {
                if matches!(
                    k.as_str(),
                    "state"
                        | "capacityLimits"
                        | "resourceSharingConfiguration"
                        | "preemptionConfiguration"
                ) {
                    body.insert(k, v);
                }
            }
        }
        self.batch_dispatch(kind.update, body)?;
        let mut result = ProvisionResult::new(existing.physical_id.clone());
        for (k, v) in &existing.attributes {
            result = result.with(k, v.clone());
        }
        Ok(result)
    }

    /// Delete through the Batch handler. A service environment or quota
    /// share has to be DISABLED first, so it is disabled and then deleted,
    /// as CloudFormation's handler does.
    pub(super) fn delete_batch_api_resource(&self, resource: &StackResource) -> Result<(), String> {
        let Some(kind) = batch_api_kind(&resource.resource_type) else {
            return Ok(());
        };
        let id = json!(resource.physical_id);
        if resource.resource_type != "AWS::Batch::ConsumableResource" {
            let mut disable = Map::new();
            disable.insert(kind.id_key.to_string(), id.clone());
            disable.insert("state".into(), json!("DISABLED"));
            // Already gone is not a stack failure.
            if self.batch_dispatch(kind.update, disable).is_err() {
                return Ok(());
            }
        }
        let mut body = Map::new();
        body.insert(kind.id_key.to_string(), id);
        // Already gone is handled centrally by `delete_resource`.
        self.batch_dispatch(kind.delete, body).map(|_| ())
    }
}
