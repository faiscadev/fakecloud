//! AppConfig CloudFormation provisioning: `AWS::AppConfig::Application`,
//! `Environment` and `ConfigurationProfile`. Each is written through to the
//! `appconfig` service state as the same typed record the direct
//! `CreateApplication` / `CreateEnvironment` / `CreateConfigurationProfile`
//! handlers store, so a CFN-created resource reads back on the `Get*` ops and
//! persists through the `appconfig` snapshot hook (survives a restart -- #1766
//! class).
//!
//! Environments and profiles are nested under their application, so their
//! physical ids encode `<applicationId>|<childId>` (matching the CFN spec's
//! composite `Ref`) to keep delete / `Fn::GetAtt` self-contained.

use serde_json::Value;

use super::{ProvisionResult, ResourceDefinition, ResourceProvisioner};
use fakecloud_appconfig::state::{
    application_arn, environment_arn, profile_arn, ApplicationRecord, EnvironmentRecord,
    ProfileRecord,
};

impl ResourceProvisioner {
    // ----------------------------------------------------------- Application

    pub(super) fn create_appconfig_application(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let name = props
            .get("Name")
            .and_then(Value::as_str)
            .map(str::to_string)
            .unwrap_or_else(|| resource.logical_id.clone());
        let id = fakecloud_core::ids::short_id(7);
        let arn = application_arn(&self.region, &self.account_id, &id);

        let record = ApplicationRecord {
            id: id.clone(),
            name,
            description: opt_str(props, "Description"),
            environments: Default::default(),
            profiles: Default::default(),
            experiments: Default::default(),
        };

        let mut guard = self.appconfig_state.write();
        let st = guard.get_or_create(&self.account_id);
        st.applications.insert(id.clone(), record);
        let tags = tag_map(props.get("Tags"));
        if !tags.is_empty() {
            st.tags.insert(arn, tags);
        }

        Ok(ProvisionResult::new(id.clone()).with("ApplicationId", id))
    }

    pub(super) fn delete_appconfig_application(&self, physical_id: &str) -> Result<(), String> {
        let arn = application_arn(&self.region, &self.account_id, physical_id);
        let mut guard = self.appconfig_state.write();
        let st = guard.get_or_create(&self.account_id);
        st.applications.remove(physical_id);
        st.tags.remove(&arn);
        Ok(())
    }

    // ----------------------------------------------------------- Environment

    pub(super) fn create_appconfig_environment(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let app_id = props
            .get("ApplicationId")
            .and_then(Value::as_str)
            .ok_or("AWS::AppConfig::Environment requires ApplicationId")?
            .to_string();
        let name = props
            .get("Name")
            .and_then(Value::as_str)
            .map(str::to_string)
            .unwrap_or_else(|| resource.logical_id.clone());
        let id = fakecloud_core::ids::short_id(7);
        let arn = environment_arn(&self.region, &self.account_id, &app_id, &id);

        let record = EnvironmentRecord {
            id: id.clone(),
            application_id: app_id.clone(),
            name,
            description: opt_str(props, "Description"),
            state: "READY_FOR_DEPLOYMENT".to_string(),
            monitors: props.get("Monitors").cloned().unwrap_or(Value::Null),
            deployments: Default::default(),
            next_deployment_number: 1,
        };

        let mut guard = self.appconfig_state.write();
        let st = guard.get_or_create(&self.account_id);
        let app = st
            .applications
            .get_mut(&app_id)
            .ok_or_else(|| format!("Application {app_id} not yet provisioned"))?;
        app.environments.insert(id.clone(), record);
        let tags = tag_map(props.get("Tags"));
        if !tags.is_empty() {
            st.tags.insert(arn, tags);
        }

        Ok(ProvisionResult::new(format!("{app_id}|{id}")).with("EnvironmentId", id))
    }

    pub(super) fn delete_appconfig_environment(&self, physical_id: &str) -> Result<(), String> {
        let Some((app_id, env_id)) = physical_id.split_once('|') else {
            return Ok(());
        };
        let arn = environment_arn(&self.region, &self.account_id, app_id, env_id);
        let mut guard = self.appconfig_state.write();
        let st = guard.get_or_create(&self.account_id);
        if let Some(app) = st.applications.get_mut(app_id) {
            app.environments.remove(env_id);
        }
        st.tags.remove(&arn);
        Ok(())
    }

    pub(super) fn get_att_appconfig_environment(
        &self,
        physical_id: &str,
        attribute: &str,
    ) -> Option<String> {
        let (app_id, env_id) = physical_id.split_once('|')?;
        if attribute == "EnvironmentId" {
            let guard = self.appconfig_state.read();
            let st = guard.get(&self.account_id)?;
            let app = st.applications.get(app_id)?;
            return app.environments.get(env_id).map(|e| e.id.clone());
        }
        None
    }

    // -------------------------------------------------- ConfigurationProfile

    pub(super) fn create_appconfig_configuration_profile(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let app_id = props
            .get("ApplicationId")
            .and_then(Value::as_str)
            .ok_or("AWS::AppConfig::ConfigurationProfile requires ApplicationId")?
            .to_string();
        let name = props
            .get("Name")
            .and_then(Value::as_str)
            .map(str::to_string)
            .unwrap_or_else(|| resource.logical_id.clone());
        let location_uri = props
            .get("LocationUri")
            .and_then(Value::as_str)
            .ok_or("AWS::AppConfig::ConfigurationProfile requires LocationUri")?
            .to_string();
        let id = fakecloud_core::ids::short_id(7);
        let arn = profile_arn(&self.region, &self.account_id, &app_id, &id);

        let record = ProfileRecord {
            id: id.clone(),
            application_id: app_id.clone(),
            name,
            description: opt_str(props, "Description"),
            location_uri,
            retrieval_role_arn: opt_str(props, "RetrievalRoleArn"),
            validators: props.get("Validators").cloned().unwrap_or(Value::Null),
            profile_type: props
                .get("Type")
                .and_then(Value::as_str)
                .unwrap_or("AWS.Freeform")
                .to_string(),
            kms_key_arn: None,
            kms_key_identifier: opt_str(props, "KmsKeyIdentifier"),
            hosted_versions: Default::default(),
            next_version_number: 1,
        };

        let mut guard = self.appconfig_state.write();
        let st = guard.get_or_create(&self.account_id);
        let app = st
            .applications
            .get_mut(&app_id)
            .ok_or_else(|| format!("Application {app_id} not yet provisioned"))?;
        app.profiles.insert(id.clone(), record);
        let tags = tag_map(props.get("Tags"));
        if !tags.is_empty() {
            st.tags.insert(arn, tags);
        }

        Ok(ProvisionResult::new(format!("{app_id}|{id}")).with("ConfigurationProfileId", id))
    }

    pub(super) fn delete_appconfig_configuration_profile(
        &self,
        physical_id: &str,
    ) -> Result<(), String> {
        let Some((app_id, profile_id)) = physical_id.split_once('|') else {
            return Ok(());
        };
        let arn = profile_arn(&self.region, &self.account_id, app_id, profile_id);
        let mut guard = self.appconfig_state.write();
        let st = guard.get_or_create(&self.account_id);
        if let Some(app) = st.applications.get_mut(app_id) {
            app.profiles.remove(profile_id);
        }
        st.tags.remove(&arn);
        Ok(())
    }

    pub(super) fn get_att_appconfig_configuration_profile(
        &self,
        physical_id: &str,
        attribute: &str,
    ) -> Option<String> {
        let (app_id, profile_id) = physical_id.split_once('|')?;
        if attribute == "ConfigurationProfileId" {
            let guard = self.appconfig_state.read();
            let st = guard.get(&self.account_id)?;
            let app = st.applications.get(app_id)?;
            return app.profiles.get(profile_id).map(|p| p.id.clone());
        }
        None
    }
}

fn opt_str(props: &Value, key: &str) -> Option<String> {
    props.get(key).and_then(Value::as_str).map(str::to_string)
}

/// Convert CFN `Tags` (`[{Key,Value}]`) into the AppConfig `TagMap`.
fn tag_map(value: Option<&Value>) -> std::collections::BTreeMap<String, String> {
    let mut out = std::collections::BTreeMap::new();
    if let Some(arr) = value.and_then(Value::as_array) {
        for t in arr {
            if let (Some(k), Some(v)) = (
                t.get("Key").and_then(Value::as_str),
                t.get("Value").and_then(Value::as_str),
            ) {
                out.insert(k.to_string(), v.to_string());
            }
        }
    }
    out
}
