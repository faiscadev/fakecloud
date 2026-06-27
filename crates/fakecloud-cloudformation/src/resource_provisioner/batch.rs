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

impl ResourceProvisioner {
    fn batch_arn(&self, kind: &str, name: &str) -> String {
        format!(
            "arn:aws:batch:{}:{}:{kind}/{name}-{}",
            self.region,
            self.account_id,
            Uuid::new_v4().simple()
        )
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
        if let Some(cr) = props.get("ComputeResources") {
            stored.insert("computeResources".into(), cr.clone());
        }
        if let Some(role) = props.get("ServiceRole") {
            stored.insert("serviceRole".into(), role.clone());
        }
        self.batch_state
            .write()
            .get_or_create(&self.account_id)
            .compute_environments
            .insert(name.clone(), Value::Object(stored));
        Ok(ProvisionResult::new(arn))
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
        stored.insert(
            "priority".into(),
            props.get("Priority").cloned().unwrap_or(json!(1)),
        );
        if let Some(order) = props.get("ComputeEnvironmentOrder") {
            stored.insert("computeEnvironmentOrder".into(), order.clone());
        }
        self.batch_state
            .write()
            .get_or_create(&self.account_id)
            .job_queues
            .insert(name.clone(), Value::Object(stored));
        Ok(ProvisionResult::new(arn))
    }

    pub(super) fn create_batch_job_definition(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let name = prop_str(props, "JobDefinitionName")
            .map(String::from)
            .unwrap_or_else(|| resource.logical_id.clone());
        let mut state = self.batch_state.write();
        let acct = state.get_or_create(&self.account_id);
        let revision = acct.job_def_revisions.entry(name.clone()).or_insert(0);
        *revision += 1;
        let revision = *revision;
        let arn = format!(
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
        if let Some(cp) = props.get("ContainerProperties") {
            stored.insert("containerProperties".into(), cp.clone());
        }
        acct.job_definitions
            .insert(format!("{name}:{revision}"), Value::Object(stored));
        Ok(ProvisionResult::new(arn))
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
            _ => {}
        }
    }
}
