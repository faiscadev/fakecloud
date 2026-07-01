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

        let source = props
            .get("Source")
            .and_then(Value::as_str)
            .ok_or("AWS::Pipes::Pipe requires Source")?
            .to_string();
        let target = props
            .get("Target")
            .and_then(Value::as_str)
            .ok_or("AWS::Pipes::Pipe requires Target")?
            .to_string();
        let role_arn = props
            .get("RoleArn")
            .and_then(Value::as_str)
            .ok_or("AWS::Pipes::Pipe requires RoleArn")?
            .to_string();

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

/// Seconds since the Unix epoch. CloudFormation provisioning is not on the
/// hot path, so a direct `SystemTime` read is fine here.
fn epoch_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}
