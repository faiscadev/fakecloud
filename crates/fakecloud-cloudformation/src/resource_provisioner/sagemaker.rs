//! SageMaker CloudFormation provisioning: `AWS::SageMaker::Model`,
//! `EndpointConfig`, `Endpoint` and `NotebookInstance`. Each is written through
//! to the `sagemaker` service state via the public `SageMakerData::put_resource`
//! accessor -- the same `family -> id -> record` store the direct create ops
//! feed -- so a CFN-created resource reads back on `Describe*` and persists
//! through the `sagemaker` snapshot hook (survives a restart -- #1766 class).
//!
//! The stored record is the CFN properties (PascalCase, already the SageMaker
//! wire shape) plus the minted `{Family}Arn` and `CreationTime`/
//! `LastModifiedTime` epoch timestamps -- exactly what the engine's `build_record`
//! mints. The physical id / storage key is the caller-supplied resource name, so
//! both `Ref` and the ARN-based `Fn::GetAtt` resolve.

use serde_json::{json, Value};

use super::{ProvisionResult, ResourceDefinition, ResourceProvisioner};

impl ResourceProvisioner {
    pub(super) fn create_sagemaker_model(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        self.create_sagemaker_resource(resource, "Model", "ModelName", "model")
    }

    pub(super) fn create_sagemaker_endpoint_config(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        self.create_sagemaker_resource(
            resource,
            "EndpointConfig",
            "EndpointConfigName",
            "endpoint-config",
        )
    }

    pub(super) fn create_sagemaker_endpoint(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        self.create_sagemaker_resource(resource, "Endpoint", "EndpointName", "endpoint")
    }

    pub(super) fn create_sagemaker_notebook_instance(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        self.create_sagemaker_resource(
            resource,
            "NotebookInstance",
            "NotebookInstanceName",
            "notebook-instance",
        )
    }

    fn create_sagemaker_resource(
        &self,
        resource: &ResourceDefinition,
        family: &str,
        key_prop: &str,
        arn_path: &str,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let name = props
            .get(key_prop)
            .and_then(Value::as_str)
            .map(str::to_string)
            .unwrap_or_else(|| resource.logical_id.clone());
        let region = &self.region;
        let account = &self.account_id;
        let arn = format!("arn:aws:sagemaker:{region}:{account}:{arn_path}/{name}");
        let arn_member = format!("{family}Arn");
        let now = json!(sagemaker_now());

        let mut record = match props {
            Value::Object(m) => m.clone(),
            _ => serde_json::Map::new(),
        };
        record
            .entry(key_prop.to_string())
            .or_insert_with(|| json!(name));
        record.insert(arn_member.clone(), json!(arn.clone()));
        record.insert("CreationTime".to_string(), now.clone());
        record.insert("LastModifiedTime".to_string(), now);

        let mut guard = self.sagemaker_state.write();
        let data = guard.get_or_create(account);
        if data.get_resource(family, &name).is_some() {
            return Err(format!("{family} {name} already exists"));
        }
        data.put_resource(family, &name, Value::Object(record));

        Ok(ProvisionResult::new(name.clone())
            .with(&arn_member, arn)
            .with("Id", name))
    }

    pub(super) fn delete_sagemaker_resource(
        &self,
        family: &str,
        physical_id: &str,
    ) -> Result<(), String> {
        let mut guard = self.sagemaker_state.write();
        let data = guard.get_or_create(&self.account_id);
        data.remove_resource(family, physical_id);
        Ok(())
    }

    pub(super) fn get_att_sagemaker_resource(
        &self,
        family: &str,
        physical_id: &str,
        attribute: &str,
    ) -> Option<String> {
        let guard = self.sagemaker_state.read();
        let data = guard.get(&self.account_id)?;
        let rec = data.get_resource(family, physical_id)?;
        if attribute == "Id" {
            return Some(physical_id.to_string());
        }
        rec.get(attribute)
            .and_then(Value::as_str)
            .map(str::to_string)
    }
}

/// Fractional-epoch-seconds timestamp, matching the SageMaker engine's
/// `now_epoch` (timestamps stored as JSON numbers, not ISO strings).
fn sagemaker_now() -> f64 {
    chrono::Utc::now().timestamp_millis() as f64 / 1000.0
}
