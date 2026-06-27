//! `AWS::Batch::*` CloudFormation provisioning. Creates compute environments,
//! job queues, and job definitions as real records in the `batch` service
//! state (the control plane; real job execution is a runtime concern, not
//! CFN-time). Writes through the batch service's snapshot hook so the
//! resources survive a restart (the #1766 lesson).

use serde_json::{json, Map, Value};
use uuid::Uuid;

use super::{ProvisionResult, ResourceDefinition, ResourceProvisioner};

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
        format!(
            "arn:aws:batch:{}:{}:{kind}/{name}-{}",
            self.region,
            self.account_id,
            Uuid::new_v4().simple()
        )
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
            .unwrap_or_else(|| resource.logical_id.clone());
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
            json!(format!(
                "arn:aws:ecs:{}:{}:cluster/AWSBatch-{name}-{uuid}",
                self.region, self.account_id
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
            .unwrap_or_else(|| resource.logical_id.clone());
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
            .unwrap_or_else(|| resource.logical_id.clone());
        let arn;
        {
            let mut state = self.batch_state.write();
            let acct = state.get_or_create(&self.account_id);
            let revision = acct.job_def_revisions.entry(name.clone()).or_insert(0);
            *revision += 1;
            let revision = *revision;
            arn = format!(
                "arn:aws:batch:{}:{}:job-definition/{name}:{revision}",
                self.region, self.account_id
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
            .unwrap_or_else(|| resource.logical_id.clone());
        let arn = format!(
            "arn:aws:batch:{}:{}:scheduling-policy/{name}",
            self.region, self.account_id
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
