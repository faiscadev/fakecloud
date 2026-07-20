//! `AWS::CodePipeline::Pipeline` CloudFormation provisioning. The pipeline is
//! written through to the `codepipeline` service state as the same camelCase
//! `PipelineDeclaration` + metadata the direct `CreatePipeline` handler stores,
//! so a CFN-created pipeline reads back identically on `GetPipeline` and
//! persists through the `codepipeline` snapshot hook (survives a restart --
//! the #1766 phantom-resource class).
//!
//! `Ref` resolves to the pipeline name; `Fn::GetAtt Version` to the pipeline
//! version. The physical id IS the pipeline name.

use serde_json::{json, Value};

use super::{cfn_props_to_camel, ProvisionResult, ResourceDefinition, ResourceProvisioner};

impl ResourceProvisioner {
    pub(super) fn create_codepipeline_pipeline(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        // CloudFormation nests the declaration one of two ways: either the
        // pipeline body's members are top-level properties (Name/RoleArn/Stages/
        // ArtifactStore/...), or, matching the API, under a `Pipeline` object.
        // Support both; top-level is the documented CFN shape.
        let decl_src = props.get("Pipeline").filter(|v| v.is_object()).cloned();
        let name = decl_src
            .as_ref()
            .and_then(|d| d.get("Name").or_else(|| d.get("name")))
            .or_else(|| props.get("Name"))
            .and_then(Value::as_str)
            .map(str::to_string)
            .unwrap_or_else(|| resource.logical_id.clone());

        // Build the camelCase PipelineDeclaration the service stores. Action
        // `Configuration` maps are provider-defined free-form keys AWS preserves
        // verbatim, so they are carried through untouched.
        let source = decl_src.unwrap_or_else(|| props.clone());
        let mut decl = match cfn_props_to_camel(&source, &["Configuration", "configuration"]) {
            Value::Object(m) => m,
            _ => serde_json::Map::new(),
        };
        // `tags` is a top-level CreatePipeline input, not part of the
        // declaration; strip it before storing the decl.
        decl.remove("tags");
        decl.insert("name".to_string(), json!(name));
        decl.insert("version".to_string(), json!(1));
        // Mirror `apply_pipeline_defaults`: default each action's runOrder to 1.
        if let Some(stages) = decl.get_mut("stages").and_then(Value::as_array_mut) {
            for stage in stages.iter_mut() {
                if let Some(actions) = stage.get_mut("actions").and_then(Value::as_array_mut) {
                    for action in actions.iter_mut() {
                        if let Some(obj) = action.as_object_mut() {
                            obj.entry("runOrder").or_insert(json!(1));
                        }
                    }
                }
            }
        }
        let decl = Value::Object(decl);

        let region = &self.region;
        let account = &self.account_id;
        let arn = format!("arn:aws:codepipeline:{region}:{account}:{name}");
        let now = ts_now();

        let mut guard = self.codepipeline_state.write();
        let st = guard.get_or_create(account);
        if st.pipelines.contains_key(&name) {
            return Err(format!("A pipeline with the name {name} already exists."));
        }
        st.pipelines.insert(name.clone(), decl.clone());
        st.pipeline_order.push(name.clone());
        st.pipeline_versions.insert(name.clone(), vec![decl]);
        st.pipeline_meta.insert(
            name.clone(),
            json!({ "pipelineArn": arn.clone(), "created": now, "updated": now }),
        );
        let tags = cfn_tag_list_lower(props);
        if !tags.is_empty() {
            st.tags.insert(arn, tags);
        }

        Ok(ProvisionResult::new(name).with("Version", "1"))
    }

    pub(super) fn delete_codepipeline_pipeline(&self, physical_id: &str) -> Result<(), String> {
        let mut guard = self.codepipeline_state.write();
        let st = guard.get_or_create(&self.account_id);
        st.pipelines.remove(physical_id);
        st.pipeline_order.retain(|n| n != physical_id);
        st.pipeline_versions.remove(physical_id);
        if let Some(meta) = st.pipeline_meta.remove(physical_id) {
            if let Some(arn) = meta.get("pipelineArn").and_then(Value::as_str) {
                st.tags.remove(arn);
            }
        }
        Ok(())
    }

    pub(super) fn get_att_codepipeline_pipeline(
        &self,
        physical_id: &str,
        attribute: &str,
    ) -> Option<String> {
        let guard = self.codepipeline_state.read();
        let st = guard.get(&self.account_id)?;
        let decl = st.pipelines.get(physical_id)?;
        match attribute {
            "Version" => decl.get("version").map(|v| {
                v.as_str()
                    .map(str::to_string)
                    .unwrap_or_else(|| v.to_string())
            }),
            "Id" => Some(physical_id.to_string()),
            _ => None,
        }
    }
}

/// Fractional-epoch-seconds timestamp, matching the codepipeline handler's `ts`.
fn ts_now() -> Value {
    json!(chrono::Utc::now().timestamp_millis() as f64 / 1000.0)
}

/// Convert CFN `Tags` (`[{Key,Value}]`) into the codepipeline wire tag shape
/// (`[{key,value}]`, lowercase) the service state uses.
fn cfn_tag_list_lower(props: &Value) -> Vec<Value> {
    let mut out = Vec::new();
    if let Some(arr) = props.get("Tags").and_then(Value::as_array) {
        for t in arr {
            let key = t.get("Key").and_then(Value::as_str).unwrap_or("");
            let value = t.get("Value").and_then(Value::as_str).unwrap_or("");
            if !key.is_empty() {
                out.push(json!({ "key": key, "value": value }));
            }
        }
    }
    out
}
