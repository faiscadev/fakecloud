//! `AWS::CodeBuild::Project` CloudFormation provisioning. The project is written
//! through to the `codebuild` service state as the same camelCase `Project`
//! object the direct `CreateProject` handler stores, so a CFN-created project
//! reads back identically on `BatchGetProjects`/`ListProjects` and persists
//! through the `codebuild` snapshot hook (survives a restart -- #1766 class).
//!
//! `Ref` resolves to the project name (the physical id); `Fn::GetAtt Arn` to the
//! project ARN.

use serde_json::{json, Value};

use super::{cfn_props_to_camel, ProvisionResult, ResourceDefinition, ResourceProvisioner};

impl ResourceProvisioner {
    pub(super) fn create_codebuild_project(
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
        let arn = format!("arn:aws:codebuild:{region}:{account}:project/{name}");
        let now = ts_now();

        // Camel-case the CFN properties into the API `Project` shape.
        let mut project = match cfn_props_to_camel(props, &[]) {
            Value::Object(m) => m,
            _ => serde_json::Map::new(),
        };
        project.remove("name");
        // `BadgeEnabled` becomes the `badge` object; default disabled.
        let badge_enabled = project
            .remove("badgeEnabled")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        // Fix the one well-known casing quirk: CodeBuild's source uses the
        // all-lowercase `buildspec` member, not `buildSpec`.
        fix_buildspec(project.get_mut("source"));
        if let Some(sources) = project
            .get_mut("secondarySources")
            .and_then(Value::as_array_mut)
        {
            for s in sources.iter_mut() {
                fix_buildspec(Some(s));
            }
        }
        project.insert("name".to_string(), json!(name));
        project.insert("arn".to_string(), json!(arn.clone()));
        project.insert("created".to_string(), now.clone());
        project.insert("lastModified".to_string(), now);
        project.insert(
            "badge".to_string(),
            json!({ "badgeEnabled": badge_enabled }),
        );
        let project = Value::Object(project);

        let mut guard = self.codebuild_state.write();
        let st = guard.get_or_create(account);
        if st.projects.contains_key(&name) {
            return Err(format!("Project already exists: {name}"));
        }
        st.projects.insert(name.clone(), project);

        Ok(ProvisionResult::new(name).with("Arn", arn))
    }

    pub(super) fn delete_codebuild_project(&self, physical_id: &str) -> Result<(), String> {
        let mut guard = self.codebuild_state.write();
        let st = guard.get_or_create(&self.account_id);
        st.projects.remove(physical_id);
        Ok(())
    }

    pub(super) fn get_att_codebuild_project(
        &self,
        physical_id: &str,
        attribute: &str,
    ) -> Option<String> {
        let guard = self.codebuild_state.read();
        let st = guard.get(&self.account_id)?;
        let project = st.projects.get(physical_id)?;
        match attribute {
            "Arn" => project
                .get("arn")
                .and_then(Value::as_str)
                .map(str::to_string),
            _ => None,
        }
    }
}

fn ts_now() -> Value {
    json!(chrono::Utc::now().timestamp_millis() as f64 / 1000.0)
}

/// Rename a source object's `buildSpec` member to the API's `buildspec`.
fn fix_buildspec(source: Option<&mut Value>) {
    if let Some(obj) = source.and_then(Value::as_object_mut) {
        if let Some(v) = obj.remove("buildSpec") {
            obj.insert("buildspec".to_string(), v);
        }
    }
}
