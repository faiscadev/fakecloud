//! `AWS::CodeDeploy::Application` and `AWS::CodeDeploy::DeploymentGroup`
//! CloudFormation provisioning. Each is written through to the `codedeploy`
//! service state as the same JSON the direct `CreateApplication` /
//! `CreateDeploymentGroup` handlers store, so a CFN-created resource reads back
//! identically on `GetApplication` / `GetDeploymentGroup` and persists through
//! the `codedeploy` snapshot hook (survives a restart -- #1766 class).
//!
//! `Ref` resolves to the application name (Application) / deployment-group name
//! (DeploymentGroup). A deployment group is stored nested under its application,
//! so its physical id encodes `<application>/<group>` to keep the delete /
//! `Fn::GetAtt` paths self-contained.

use serde_json::{json, Value};

use super::{cfn_props_to_camel, ProvisionResult, ResourceDefinition, ResourceProvisioner};

impl ResourceProvisioner {
    // ----------------------------------------------------------- Application

    pub(super) fn create_codedeploy_application(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let name = props
            .get("ApplicationName")
            .and_then(Value::as_str)
            .map(str::to_string)
            .unwrap_or_else(|| resource.logical_id.clone());
        let compute = props
            .get("ComputePlatform")
            .and_then(Value::as_str)
            .unwrap_or("Server")
            .to_string();
        let region = &self.region;
        let account = &self.account_id;
        let app_id = uuid::Uuid::new_v4().to_string();
        let now = ts_now();

        let mut guard = self.codedeploy_state.write();
        let st = guard.get_or_create(account);
        if st.applications.contains_key(&name) {
            return Err(format!(
                "An application already exists with the name {name}."
            ));
        }
        st.applications.insert(
            name.clone(),
            json!({
                "applicationId": app_id,
                "applicationName": name.clone(),
                "createTime": now,
                "linkedToGitHub": false,
                "computePlatform": compute,
            }),
        );
        st.application_order.push(name.clone());
        let tags = cfn_tag_list_pascal(props);
        if !tags.is_empty() {
            let arn = format!("arn:aws:codedeploy:{region}:{account}:application:{name}");
            st.tags.insert(arn, tags);
        }

        Ok(ProvisionResult::new(name))
    }

    pub(super) fn delete_codedeploy_application(&self, physical_id: &str) -> Result<(), String> {
        let region = &self.region;
        let account = &self.account_id;
        let mut guard = self.codedeploy_state.write();
        let st = guard.get_or_create(account);
        st.applications.remove(physical_id);
        st.application_order.retain(|n| n != physical_id);
        st.deployment_groups.remove(physical_id);
        st.tags.remove(&format!(
            "arn:aws:codedeploy:{region}:{account}:application:{physical_id}"
        ));
        Ok(())
    }

    // ------------------------------------------------------- DeploymentGroup

    pub(super) fn create_codedeploy_deployment_group(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let app = props
            .get("ApplicationName")
            .and_then(Value::as_str)
            .ok_or("AWS::CodeDeploy::DeploymentGroup requires ApplicationName")?
            .to_string();
        let dg_name = props
            .get("DeploymentGroupName")
            .and_then(Value::as_str)
            .map(str::to_string)
            .unwrap_or_else(|| resource.logical_id.clone());
        let service_role = props
            .get("ServiceRoleArn")
            .and_then(Value::as_str)
            .ok_or("AWS::CodeDeploy::DeploymentGroup requires ServiceRoleArn")?
            .to_string();
        let config = props
            .get("DeploymentConfigName")
            .and_then(Value::as_str)
            .unwrap_or("CodeDeployDefault.OneAtATime")
            .to_string();
        let dg_id = uuid::Uuid::new_v4().to_string();
        let region = &self.region;
        let account = &self.account_id;

        let mut guard = self.codedeploy_state.write();
        let st = guard.get_or_create(account);
        let compute = st
            .applications
            .get(&app)
            .and_then(|a| a.get("computePlatform"))
            .and_then(Value::as_str)
            .ok_or_else(|| format!("Application {app} not yet provisioned"))?
            .to_string();

        // Carry through the optional structural members (ec2TagFilters, alarm /
        // rollback / deployment-style configs, ...) in the camelCase API shape.
        let mut group = match cfn_props_to_camel(props, &[]) {
            Value::Object(m) => m,
            _ => serde_json::Map::new(),
        };
        for k in [
            "applicationName",
            "deploymentGroupName",
            "serviceRoleArn",
            "deploymentConfigName",
            "tags",
        ] {
            group.remove(k);
        }
        // Fix the one acronym-casing quirk blind camel-casing gets wrong.
        if let Some(v) = group.remove("eCSServices") {
            group.insert("ecsServices".to_string(), v);
        }
        group.insert("applicationName".to_string(), json!(app));
        group.insert("deploymentGroupId".to_string(), json!(dg_id.clone()));
        group.insert("deploymentGroupName".to_string(), json!(dg_name.clone()));
        group.insert("deploymentConfigName".to_string(), json!(config));
        group.insert("computePlatform".to_string(), json!(compute));
        group.insert("serviceRoleArn".to_string(), json!(service_role));

        let groups = st.deployment_groups.entry(app.clone()).or_default();
        if groups.contains_key(&dg_name) {
            return Err(format!("Deployment group {dg_name} already exists."));
        }
        groups.insert(dg_name.clone(), Value::Object(group));
        let tags = cfn_tag_list_pascal(props);
        if !tags.is_empty() {
            let arn =
                format!("arn:aws:codedeploy:{region}:{account}:deploymentgroup:{app}/{dg_name}");
            st.tags.insert(arn, tags);
        }

        Ok(ProvisionResult::new(format!("{app}/{dg_name}"))
            .with("Id", dg_id)
            .with("Name", dg_name))
    }

    pub(super) fn delete_codedeploy_deployment_group(
        &self,
        physical_id: &str,
    ) -> Result<(), String> {
        let Some((app, dg_name)) = physical_id.split_once('/') else {
            return Ok(());
        };
        let region = &self.region;
        let account = &self.account_id;
        let mut guard = self.codedeploy_state.write();
        let st = guard.get_or_create(account);
        if let Some(groups) = st.deployment_groups.get_mut(app) {
            groups.remove(dg_name);
        }
        st.tags.remove(&format!(
            "arn:aws:codedeploy:{region}:{account}:deploymentgroup:{app}/{dg_name}"
        ));
        Ok(())
    }

    pub(super) fn get_att_codedeploy_deployment_group(
        &self,
        physical_id: &str,
        attribute: &str,
    ) -> Option<String> {
        let (app, dg_name) = physical_id.split_once('/')?;
        let guard = self.codedeploy_state.read();
        let st = guard.get(&self.account_id)?;
        let group = st.deployment_groups.get(app)?.get(dg_name)?;
        match attribute {
            "Id" => group
                .get("deploymentGroupId")
                .and_then(Value::as_str)
                .map(str::to_string),
            "Name" => Some(dg_name.to_string()),
            _ => None,
        }
    }
}

fn ts_now() -> Value {
    json!(chrono::Utc::now().timestamp_millis() as f64 / 1000.0)
}

/// Convert CFN `Tags` (`[{Key,Value}]`) into the codedeploy wire tag shape,
/// which keeps PascalCase `{Key,Value}`.
fn cfn_tag_list_pascal(props: &Value) -> Vec<Value> {
    let mut out = Vec::new();
    if let Some(arr) = props.get("Tags").and_then(Value::as_array) {
        for t in arr {
            let key = t.get("Key").and_then(Value::as_str).unwrap_or("");
            let value = t.get("Value").and_then(Value::as_str).unwrap_or("");
            if !key.is_empty() {
                out.push(json!({ "Key": key, "Value": value }));
            }
        }
    }
    out
}
