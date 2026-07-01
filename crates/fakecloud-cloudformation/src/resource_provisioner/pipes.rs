//! `AWS::Pipes::Pipe` CloudFormation provisioning. Creates a pipe as a real
//! record in the `pipes` service state — the same JSON shape the direct
//! `CreatePipe` handler stores — so the background Pipes runner picks it up and
//! a CFN-created pipe reads back identically to an API-created one on
//! `DescribePipe`. The pipe is created already settled (`RUNNING`, or `STOPPED`
//! when `DesiredState=STOPPED`) since CFN provisioning is synchronous. Persists
//! through the `pipes` snapshot hook (keyed off the `Pipes` resource segment),
//! so the resource survives a restart (the #1766 lesson).

use serde_json::{json, Map, Value};

use super::{ProvisionResult, ResourceDefinition, ResourceProvisioner};

/// The optional `CreatePipe` input fields, copied verbatim from the CFN
/// properties (Pipes uses PascalCase in both the API and CloudFormation, so the
/// keys map 1:1).
const PIPE_INPUT_FIELDS: &[&str] = &[
    "Description",
    "Source",
    "SourceParameters",
    "Enrichment",
    "EnrichmentParameters",
    "Target",
    "TargetParameters",
    "RoleArn",
    "LogConfiguration",
    "KmsKeyIdentifier",
];

/// The updatable `UpdatePipe` fields. `Source` is intentionally absent: it is
/// immutable on a pipe (a change forces a CloudFormation replacement), so an
/// in-place update never rewrites it. Mirrors the direct `UpdatePipe` handler.
const PIPE_UPDATE_FIELDS: &[&str] = &[
    "Description",
    "SourceParameters",
    "Enrichment",
    "EnrichmentParameters",
    "Target",
    "TargetParameters",
    "RoleArn",
    "LogConfiguration",
    "KmsKeyIdentifier",
];

impl ResourceProvisioner {
    pub(super) fn create_pipes_pipe(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let name = props
            .get("Name")
            .and_then(Value::as_str)
            .map(String::from)
            .unwrap_or_else(|| resource.logical_id.clone());
        // Apply the same validation the direct CreatePipe handler enforces, so a
        // CFN-created pipe can't slip past name/ARN constraints the API rejects.
        fakecloud_pipes::validate_pipe_name(&name).map_err(|e| e.message())?;

        // Source/Target/RoleArn are required and must be non-empty (a bare
        // `as_str` would accept `""`), and are bounded to the 1-1600 ARN length.
        let source = require_arn_field(props, "Source")?;
        let target = require_arn_field(props, "Target")?;
        let role_arn = require_arn_field(props, "RoleArn")?;

        let arn = format!(
            "arn:aws:pipes:{}:{}:pipe/{name}",
            self.region, self.account_id
        );
        // CFN provisioning is synchronous, so the pipe is born settled: RUNNING
        // unless the template explicitly asks for STOPPED.
        let desired = match props.get("DesiredState").and_then(Value::as_str) {
            Some("STOPPED") => "STOPPED",
            _ => "RUNNING",
        };
        let now = epoch_secs();

        // Reject a same-name collision instead of silently overwriting an
        // existing pipe (the direct CreatePipe handler returns ConflictException
        // here). CloudFormation provisions a stack single-threaded, so a plain
        // read-then-write is race-free.
        if self
            .pipes_state
            .read()
            .get(&self.account_id)
            .is_some_and(|st| st.pipes.contains_key(&name))
        {
            return Err(format!("Pipe with Name {name} already exists."));
        }

        let mut pipe = Map::new();
        pipe.insert("Name".into(), json!(name));
        pipe.insert("Arn".into(), json!(arn));
        pipe.insert("Source".into(), json!(source));
        pipe.insert("Target".into(), json!(target));
        pipe.insert("RoleArn".into(), json!(role_arn));
        for field in PIPE_INPUT_FIELDS {
            if let Some(v) = props.get(*field) {
                pipe.insert((*field).into(), v.clone());
            }
        }
        pipe.insert("DesiredState".into(), json!(desired));
        pipe.insert("CurrentState".into(), json!(desired));
        pipe.insert("StateReason".into(), json!("Pipe is healthy"));
        pipe.insert("CreationTime".into(), json!(now));
        pipe.insert("LastModifiedTime".into(), json!(now));
        // Echo the source-typed default parameter block (e.g. SqsQueueParameters)
        // just like the direct CreatePipe handler, so a CFN-created pipe reads
        // back identically on DescribePipe.
        fakecloud_pipes::ensure_source_param_defaults(&mut pipe, &source);
        fakecloud_pipes::normalize_empty_input_templates(&mut pipe);

        // Tags: AWS::Pipes::Pipe carries a JSON map; mirror the direct handler,
        // which both embeds the Tags map on the pipe and indexes it in the tag
        // store for ListTagsForResource.
        if let Some(tags) = props.get("Tags").and_then(Value::as_object) {
            let owned: Map<String, Value> = tags
                .iter()
                .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), json!(s))))
                .collect();
            if !owned.is_empty() {
                pipe.insert("Tags".into(), Value::Object(owned.clone()));
                let mut state = self.pipes_state.write();
                let entry = state
                    .get_or_create(&self.account_id)
                    .tags
                    .entry(arn.clone())
                    .or_default();
                for (k, v) in owned {
                    if let Some(s) = v.as_str() {
                        entry.insert(k, s.to_string());
                    }
                }
            }
        }

        self.pipes_state
            .write()
            .get_or_create(&self.account_id)
            .pipes
            .insert(name.clone(), Value::Object(pipe));

        // Ref returns the pipe name; GetAtt exposes Arn + the lifecycle fields.
        Ok(ProvisionResult::new(name)
            .with("Arn", arn)
            .with("CurrentState", desired)
            .with("StateReason", "Pipe is healthy")
            .with("CreationTime", now.to_string())
            .with("LastModifiedTime", now.to_string()))
    }

    /// Apply a stack UpdateStack change to an existing pipe: re-write the
    /// updatable fields (full replace — an omitted field is cleared, matching
    /// the direct `UpdatePipe` handler) and re-settle it. Without this arm the
    /// generic `update_resource` dispatcher returns `Ok(None)` for
    /// `AWS::Pipes::Pipe`, so CloudFormation reports `UPDATE_COMPLETE` while
    /// silently discarding the property change.
    pub(super) fn update_pipes_pipe(
        &self,
        existing: &super::StackResource,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        // The physical id of a pipe is its Name.
        let name = existing.physical_id.clone();
        let now = epoch_secs();

        // Target/RoleArn stay required and non-empty on update; Source is
        // immutable so it is not re-validated here. Called for validation only —
        // the applied value flows through PIPE_UPDATE_FIELDS below.
        require_arn_field(props, "Target")?;
        require_arn_field(props, "RoleArn")?;

        let desired = match props.get("DesiredState").and_then(Value::as_str) {
            Some("STOPPED") => "STOPPED",
            _ => "RUNNING",
        };

        let mut state = self.pipes_state.write();
        let acct = state.get_or_create(&self.account_id);
        let pipe = acct
            .pipes
            .get_mut(&name)
            .ok_or_else(|| format!("AWS::Pipes::Pipe {name} not yet provisioned"))?;
        let obj = pipe
            .as_object_mut()
            .ok_or_else(|| format!("corrupt pipe state for {name}"))?;

        for field in PIPE_UPDATE_FIELDS {
            match props.get(*field) {
                Some(v) => {
                    obj.insert((*field).to_string(), v.clone());
                }
                None => {
                    obj.remove(*field);
                }
            }
        }
        obj.insert("DesiredState".into(), json!(desired));
        // CFN provisioning is synchronous, so the pipe stays settled.
        obj.insert("CurrentState".into(), json!(desired));
        obj.insert("StateReason".into(), json!("Pipe is healthy"));
        obj.insert("LastModifiedTime".into(), json!(now));
        let source = obj
            .get("Source")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        fakecloud_pipes::ensure_source_param_defaults(obj, &source);
        fakecloud_pipes::normalize_empty_input_templates(obj);
        let arn = obj
            .get("Arn")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();

        Ok(ProvisionResult::new(name)
            .with("Arn", arn)
            .with("CurrentState", desired)
            .with("StateReason", "Pipe is healthy")
            .with("LastModifiedTime", now.to_string()))
    }

    /// Live-state `Fn::GetAtt` for a pipe: resolves `Arn` from the pipes service
    /// state (the create path also pre-captures it, so this is the consistency
    /// overlay used when reading back a persisted stack).
    pub(super) fn get_att_pipes_pipe(&self, physical_id: &str, attribute: &str) -> Option<String> {
        let state = self.pipes_state.read();
        let pipe = state.get(&self.account_id)?.pipes.get(physical_id)?;
        match attribute {
            "Arn" => pipe.get("Arn").and_then(Value::as_str).map(String::from),
            _ => None,
        }
    }

    /// Delete a pipe by physical id (its name). Also drops the tag-store entry
    /// keyed by the pipe's ARN.
    pub(super) fn delete_pipes_pipe(&self, physical_id: &str) {
        let mut state = self.pipes_state.write();
        let acct = state.get_or_create(&self.account_id);
        if let Some(removed) = acct.pipes.remove(physical_id) {
            if let Some(arn) = removed.get("Arn").and_then(Value::as_str) {
                acct.tags.remove(arn);
            }
        }
    }
}

/// Extract a required ARN-shaped property (`Source`/`Target`/`RoleArn`),
/// rejecting a missing, empty, or oversize value with the same non-empty +
/// 1-1600 length bound the direct CreatePipe/UpdatePipe handler enforces.
fn require_arn_field(props: &Value, field: &str) -> Result<String, String> {
    let value = props
        .get(field)
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| format!("AWS::Pipes::Pipe requires a non-empty {field}"))?;
    fakecloud_pipes::validate_resource_arn_len(field, value).map_err(|e| e.message())?;
    Ok(value.to_string())
}

/// Seconds since the Unix epoch. CloudFormation provisioning is not on the
/// hot path, so a direct `SystemTime` read is fine here.
fn epoch_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}
