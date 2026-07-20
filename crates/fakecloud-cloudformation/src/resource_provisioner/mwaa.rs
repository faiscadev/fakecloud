//! `AWS::MWAA::Environment` CloudFormation provisioning. The environment is
//! written through to the `mwaa` service state as the same `Environment`-shaped
//! JSON (keyed by name) the direct `CreateEnvironment` handler stores, so a
//! CFN-created environment reads back on `GetEnvironment` and persists through
//! the `mwaa` snapshot hook (survives a restart -- #1766 class).
//!
//! `Ref` resolves to the environment name (the physical id); `Fn::GetAtt Arn` to
//! the environment ARN.

use serde_json::{json, Value};

use super::{ProvisionResult, ResourceDefinition, ResourceProvisioner};
use fakecloud_mwaa::shared::{
    celery_executor_queue, environment_arn, now_epoch, service_role_arn, webserver_url,
};

impl ResourceProvisioner {
    pub(super) fn create_mwaa_environment(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let name = props
            .get("Name")
            .and_then(Value::as_str)
            .map(str::to_string)
            .unwrap_or_else(|| resource.logical_id.clone());
        let region = &self.region;
        let account = &self.account_id;
        let arn = environment_arn(region, account, &name);
        let webserver = webserver_url(account, region, &name);

        // The CFN properties are the PascalCase `Environment` wire shape; carry
        // them through verbatim and stamp the server-minted members.
        let mut env = match props {
            Value::Object(m) => m.clone(),
            _ => serde_json::Map::new(),
        };
        env.insert("Name".to_string(), json!(name));
        env.insert("Arn".to_string(), json!(arn.clone()));
        env.insert("Status".to_string(), json!("CREATING"));
        env.insert("CreatedAt".to_string(), json!(now_epoch()));
        env.insert("WebserverUrl".to_string(), json!(webserver.clone()));
        env.insert(
            "ServiceRoleArn".to_string(),
            json!(service_role_arn(account)),
        );
        env.insert(
            "CeleryExecutorQueue".to_string(),
            json!(celery_executor_queue(account, region, &name)),
        );
        env.entry("EnvironmentClass").or_insert(json!("mw1.small"));
        env.entry("AirflowVersion").or_insert(json!("2.10.3"));
        env.entry("WebserverAccessMode")
            .or_insert(json!("PRIVATE_ONLY"));
        env.entry("EndpointManagement").or_insert(json!("SERVICE"));

        let mut guard = self.mwaa_state.write();
        let data = guard.get_or_create(account);
        if data.environments.contains_key(&name) {
            return Err(format!("Environment {name} already exists"));
        }
        data.environments.insert(name.clone(), Value::Object(env));

        Ok(ProvisionResult::new(name)
            .with("Arn", arn)
            .with("WebserverUrl", webserver))
    }

    pub(super) fn delete_mwaa_environment(&self, physical_id: &str) -> Result<(), String> {
        let mut guard = self.mwaa_state.write();
        let data = guard.get_or_create(&self.account_id);
        data.environments.remove(physical_id);
        Ok(())
    }

    pub(super) fn get_att_mwaa_environment(
        &self,
        physical_id: &str,
        attribute: &str,
    ) -> Option<String> {
        let guard = self.mwaa_state.read();
        let data = guard.get(&self.account_id)?;
        let env = data.environments.get(physical_id)?;
        match attribute {
            "Arn" => env.get("Arn").and_then(Value::as_str).map(str::to_string),
            "WebserverUrl" => env
                .get("WebserverUrl")
                .and_then(Value::as_str)
                .map(str::to_string),
            "CeleryExecutorQueue" => env
                .get("CeleryExecutorQueue")
                .and_then(Value::as_str)
                .map(str::to_string),
            _ => None,
        }
    }
}
