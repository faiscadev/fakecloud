//! `AWS::Amplify::App` CloudFormation provisioning. The app is written through to
//! the `amplify` service state as the same camelCase `App` wire object (wrapped
//! in an `AppRecord`, keyed by appId) the direct `CreateApp` handler stores, so a
//! CFN-created app reads back on `GetApp` and persists through the `amplify`
//! snapshot hook (survives a restart -- #1766 class).
//!
//! This arm only READS the amplify crate's public state + helpers; it does not
//! modify the amplify service handler.
//!
//! `Ref` resolves to the AppId (the physical id); GetAtt exposes `AppId`,
//! `AppName`, `Arn`, `DefaultDomain`.

use serde_json::{json, Value};

use super::{ProvisionResult, ResourceDefinition, ResourceProvisioner, StackResource};
use fakecloud_amplify::shared::{app_arn, default_domain, new_app_id, now_epoch};
use fakecloud_amplify::state::AppRecord;

impl ResourceProvisioner {
    pub(super) fn create_amplify_app(
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
        let app_id = new_app_id();
        let arn = app_arn(region, account, &app_id);
        let domain = default_domain(&app_id);
        let now = now_epoch();

        let clone_method =
            if props.get("AccessToken").is_some() || props.get("OauthToken").is_some() {
                "TOKEN"
            } else {
                "SSH"
            };

        let mut app = serde_json::Map::new();
        app.insert("appId".to_string(), json!(app_id));
        app.insert("appArn".to_string(), json!(arn.clone()));
        app.insert("name".to_string(), json!(name));
        app.insert(
            "description".to_string(),
            json!(props
                .get("Description")
                .and_then(Value::as_str)
                .unwrap_or("")),
        );
        app.insert(
            "repository".to_string(),
            json!(props
                .get("Repository")
                .and_then(Value::as_str)
                .unwrap_or("")),
        );
        app.insert(
            "platform".to_string(),
            json!(props
                .get("Platform")
                .and_then(Value::as_str)
                .unwrap_or("WEB")),
        );
        app.insert("createTime".to_string(), json!(now));
        app.insert("updateTime".to_string(), json!(now));
        app.insert("defaultDomain".to_string(), json!(domain.clone()));
        app.insert(
            "enableBranchAutoBuild".to_string(),
            json!(props
                .get("EnableBranchAutoBuild")
                .and_then(Value::as_bool)
                .unwrap_or(true)),
        );
        app.insert(
            "enableBasicAuth".to_string(),
            json!(props
                .get("EnableBasicAuth")
                .and_then(Value::as_bool)
                .unwrap_or(false)),
        );
        app.insert("repositoryCloneMethod".to_string(), json!(clone_method));
        app.insert(
            "environmentVariables".to_string(),
            env_var_map(props.get("EnvironmentVariables")),
        );
        if let Some(bs) = props.get("BuildSpec").and_then(Value::as_str) {
            app.insert("buildSpec".to_string(), json!(bs));
        }
        if let Some(role) = props.get("IAMServiceRole").and_then(Value::as_str) {
            app.insert("iamServiceRoleArn".to_string(), json!(role));
        }
        let tags = key_value_map(props.get("Tags"));
        if !tags.as_object().map(|m| m.is_empty()).unwrap_or(true) {
            app.insert("tags".to_string(), tags);
        }
        let app = Value::Object(app);

        let mut guard = self.amplify_state.write();
        let data = guard.get_or_create(account);
        data.apps.insert(
            app_id.clone(),
            AppRecord {
                app,
                ..Default::default()
            },
        );

        Ok(ProvisionResult::new(app_id.clone())
            .with("AppId", app_id)
            .with("AppName", name)
            .with("Arn", arn)
            .with("DefaultDomain", domain))
    }

    /// In-place `UpdateStack` for an `AWS::Amplify::App`. Mutates the stored
    /// `App` wire object in place instead of the reprovision fallback's
    /// delete+recreate (which would mint a new `appId` and drop the app's
    /// branches/domains). Applies the mutable app properties and preserves the
    /// identity (`appId`, `appArn`, `defaultDomain`, `createTime`) and any
    /// nested state on the `AppRecord`.
    pub(super) fn update_amplify_app(
        &self,
        existing: &StackResource,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let app_id = existing.physical_id.clone();

        let mut guard = self.amplify_state.write();
        let data = guard.get_or_create(&self.account_id);
        let record = data
            .apps
            .get_mut(&app_id)
            .ok_or_else(|| format!("Amplify app {app_id} not yet provisioned"))?;
        let app = record
            .app
            .as_object_mut()
            .ok_or_else(|| format!("Amplify app {app_id} record is malformed"))?;

        if let Some(v) = props.get("Name").and_then(Value::as_str) {
            app.insert("name".to_string(), json!(v));
        }
        if let Some(v) = props.get("Description").and_then(Value::as_str) {
            app.insert("description".to_string(), json!(v));
        }
        if let Some(v) = props.get("Repository").and_then(Value::as_str) {
            app.insert("repository".to_string(), json!(v));
        }
        if let Some(v) = props.get("Platform").and_then(Value::as_str) {
            app.insert("platform".to_string(), json!(v));
        }
        if let Some(v) = props.get("EnableBranchAutoBuild").and_then(Value::as_bool) {
            app.insert("enableBranchAutoBuild".to_string(), json!(v));
        }
        if let Some(v) = props.get("EnableBasicAuth").and_then(Value::as_bool) {
            app.insert("enableBasicAuth".to_string(), json!(v));
        }
        if props.get("EnvironmentVariables").is_some() {
            app.insert(
                "environmentVariables".to_string(),
                env_var_map(props.get("EnvironmentVariables")),
            );
        }
        if let Some(bs) = props.get("BuildSpec").and_then(Value::as_str) {
            app.insert("buildSpec".to_string(), json!(bs));
        }
        if let Some(role) = props.get("IAMServiceRole").and_then(Value::as_str) {
            app.insert("iamServiceRoleArn".to_string(), json!(role));
        }
        if props.get("Tags").is_some() {
            app.insert("tags".to_string(), key_value_map(props.get("Tags")));
        }
        app.insert("updateTime".to_string(), json!(now_epoch()));

        let name = app
            .get("name")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        let arn = app
            .get("appArn")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        let domain = app
            .get("defaultDomain")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        Ok(ProvisionResult::new(app_id.clone())
            .with("AppId", app_id)
            .with("AppName", name)
            .with("Arn", arn)
            .with("DefaultDomain", domain))
    }

    pub(super) fn delete_amplify_app(&self, physical_id: &str) -> Result<(), String> {
        let mut guard = self.amplify_state.write();
        let data = guard.get_or_create(&self.account_id);
        data.apps.remove(physical_id);
        Ok(())
    }

    pub(super) fn get_att_amplify_app(&self, physical_id: &str, attribute: &str) -> Option<String> {
        let guard = self.amplify_state.read();
        let data = guard.get(&self.account_id)?;
        let app = &data.apps.get(physical_id)?.app;
        match attribute {
            "AppId" => Some(physical_id.to_string()),
            "AppName" => app.get("name").and_then(Value::as_str).map(str::to_string),
            "Arn" => app
                .get("appArn")
                .and_then(Value::as_str)
                .map(str::to_string),
            "DefaultDomain" => app
                .get("defaultDomain")
                .and_then(Value::as_str)
                .map(str::to_string),
            _ => None,
        }
    }
}

/// Convert CFN `EnvironmentVariables` (`[{Name,Value}]`) into the Amplify API
/// `{name: value}` map.
fn env_var_map(value: Option<&Value>) -> Value {
    let mut out = serde_json::Map::new();
    if let Some(arr) = value.and_then(Value::as_array) {
        for e in arr {
            if let (Some(k), Some(v)) = (
                e.get("Name").and_then(Value::as_str),
                e.get("Value").and_then(Value::as_str),
            ) {
                out.insert(k.to_string(), json!(v));
            }
        }
    }
    Value::Object(out)
}

/// Convert CFN `Tags` (`[{Key,Value}]`) into the Amplify `{key: value}` map.
fn key_value_map(value: Option<&Value>) -> Value {
    let mut out = serde_json::Map::new();
    if let Some(arr) = value.and_then(Value::as_array) {
        for t in arr {
            if let (Some(k), Some(v)) = (
                t.get("Key").and_then(Value::as_str),
                t.get("Value").and_then(Value::as_str),
            ) {
                out.insert(k.to_string(), json!(v));
            }
        }
    }
    Value::Object(out)
}
