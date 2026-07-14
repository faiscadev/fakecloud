//! `AWS::CodeCommit::Repository` CloudFormation provisioning. The repository is
//! written through to the `codecommit` service state as the same
//! `RepositoryMetadata`-shaped JSON the direct `CreateRepository` handler
//! stores, so a CFN-created repository reads back identically on
//! `GetRepository` and persists through the `codecommit` snapshot hook
//! (survives a restart -- the #1766 phantom-resource lesson).
//!
//! Physical id + `Ref`: unlike CodeArtifact (where `Ref` is the ARN), the AWS
//! `AWS::CodeCommit::Repository` resource resolves `Ref` to the repository
//! NAME, so the physical id IS the repository name. Delete / live
//! `Fn::GetAtt` look the repository up by that name.
//!
//! `Fn::GetAtt` (verified against the AWS resource spec):
//!   Repository -> Arn, CloneUrlHttp, CloneUrlSsh, Name

use std::collections::BTreeMap;

use chrono::Utc;
use serde_json::{json, Map, Value};

use super::{ProvisionResult, ResourceDefinition, ResourceProvisioner, StackResource};

impl ResourceProvisioner {
    pub(super) fn create_codecommit_repository(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let name = cc_str(props, "RepositoryName").unwrap_or_else(|| resource.logical_id.clone());
        let account = self.account_id.clone();
        let region = self.region.clone();
        let arn = format!("arn:aws:codecommit:{region}:{account}:{name}");
        let now = Utc::now();

        let mut guard = self.codecommit_state.write();
        // CloudFormation provisions a stack single-threaded, so a plain
        // read-then-write is race-free; reject a same-name collision the way the
        // direct handler returns RepositoryNameExistsException.
        let st = guard.get_or_create(&account);
        if st.repositories.contains_key(&name) {
            return Err(format!("Repository named {name} already exists"));
        }

        // Mirror the direct CreateRepository handler: an omitted KmsKeyId mints a
        // synthetic KMS key ARN so the stored metadata round-trips a key.
        let kms = cc_str(props, "KmsKeyId").unwrap_or_else(|| {
            format!(
                "arn:aws:kms:{region}:{account}:key/{}",
                uuid::Uuid::new_v4()
            )
        });
        let clone_http = format!("https://git-codecommit.{region}.amazonaws.com/v1/repos/{name}");
        let clone_ssh = format!("ssh://git-codecommit.{region}.amazonaws.com/v1/repos/{name}");

        let mut metadata = Map::new();
        metadata.insert("accountId".into(), json!(account));
        metadata.insert(
            "repositoryId".into(),
            json!(uuid::Uuid::new_v4().to_string()),
        );
        metadata.insert("repositoryName".into(), json!(name));
        if let Some(d) = cc_str(props, "RepositoryDescription") {
            metadata.insert("repositoryDescription".into(), json!(d));
        }
        metadata.insert("lastModifiedDate".into(), cc_ts(now));
        metadata.insert("creationDate".into(), cc_ts(now));
        metadata.insert("cloneUrlHttp".into(), json!(clone_http));
        metadata.insert("cloneUrlSsh".into(), json!(clone_ssh));
        metadata.insert("Arn".into(), json!(arn));
        metadata.insert("kmsKeyId".into(), json!(kms));
        // The optional `Code` property seeds an initial commit from an S3 object.
        // There is no live git transport here, so the repository is created empty
        // (matching the metadata, which is what Ref/GetAtt and GetRepository read)
        // -- the same control-plane-only shape the direct handlers use.
        let metadata = Value::Object(metadata);

        let repo = fakecloud_codecommit::Repo {
            metadata: metadata.clone(),
            triggers: cc_triggers(props),
            ..Default::default()
        };
        st.repositories.insert(name.clone(), repo);
        st.repository_order.push(name.clone());

        let tags = cc_tags(props);
        if !tags.is_empty() {
            st.tags.insert(arn.clone(), tags);
        }

        Ok(ProvisionResult::new(name.clone())
            .with("Arn", arn)
            .with("CloneUrlHttp", clone_http)
            .with("CloneUrlSsh", clone_ssh)
            .with("Name", name))
    }

    pub(super) fn update_codecommit_repository(
        &self,
        existing: &StackResource,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let old_name = existing.physical_id.clone();
        let new_name =
            cc_str(props, "RepositoryName").unwrap_or_else(|| existing.logical_id.clone());

        // RepositoryName is replacement-required on the CFN resource -- a rename
        // re-provisions the repository from scratch.
        if new_name != old_name {
            self.delete_codecommit_repository(&existing.physical_id);
            return self.create_codecommit_repository(resource);
        }

        let mut guard = self.codecommit_state.write();
        let st = guard.get_or_create(&self.account_id);
        let repo = st
            .repositories
            .get_mut(&old_name)
            .ok_or_else(|| format!("Repository {old_name} not yet provisioned"))?;
        // In-place: RepositoryDescription and KmsKeyId are no-interruption updates
        // (UpdateRepositoryDescription / UpdateRepositoryEncryptionKey).
        if let Some(obj) = repo.metadata.as_object_mut() {
            match cc_str(props, "RepositoryDescription") {
                Some(d) => {
                    obj.insert("repositoryDescription".into(), json!(d));
                }
                None => {
                    obj.remove("repositoryDescription");
                }
            }
            if let Some(k) = cc_str(props, "KmsKeyId") {
                obj.insert("kmsKeyId".into(), json!(k));
            }
            obj.insert("lastModifiedDate".into(), cc_ts(Utc::now()));
        }
        if props.get("Triggers").is_some() {
            repo.triggers = cc_triggers(props);
        }
        let arn = repo
            .metadata
            .get("Arn")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        let clone_http = repo
            .metadata
            .get("cloneUrlHttp")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        let clone_ssh = repo
            .metadata
            .get("cloneUrlSsh")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();

        let tags = cc_tags(props);
        if tags.is_empty() {
            st.tags.remove(&arn);
        } else {
            st.tags.insert(arn.clone(), tags);
        }

        Ok(ProvisionResult::new(old_name.clone())
            .with("Arn", arn)
            .with("CloneUrlHttp", clone_http)
            .with("CloneUrlSsh", clone_ssh)
            .with("Name", old_name))
    }

    pub(super) fn get_att_codecommit_repository(
        &self,
        physical_id: &str,
        attribute: &str,
    ) -> Option<String> {
        let guard = self.codecommit_state.read();
        let repo = guard.get(&self.account_id)?.repositories.get(physical_id)?;
        let key = match attribute {
            "Arn" => "Arn",
            "CloneUrlHttp" => "cloneUrlHttp",
            "CloneUrlSsh" => "cloneUrlSsh",
            "Name" => "repositoryName",
            _ => return None,
        };
        repo.metadata
            .get(key)
            .and_then(Value::as_str)
            .map(str::to_string)
    }

    pub(super) fn delete_codecommit_repository(&self, physical_id: &str) {
        let mut guard = self.codecommit_state.write();
        let st = guard.get_or_create(&self.account_id);
        if let Some(repo) = st.repositories.remove(physical_id) {
            st.repository_order.retain(|n| n != physical_id);
            if let Some(arn) = repo.metadata.get("Arn").and_then(Value::as_str) {
                st.tags.remove(arn);
            }
        }
    }
}

/// Read a non-empty string property.
fn cc_str(props: &Value, key: &str) -> Option<String> {
    props
        .get(key)
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
}

/// The CodeCommit `creationDate` encoding used by the direct handlers: epoch
/// seconds as a float carrying millisecond precision.
fn cc_ts(dt: chrono::DateTime<Utc>) -> Value {
    json!(dt.timestamp_millis() as f64 / 1000.0)
}

/// Convert CFN `Tags` (`[{Key,Value}]`, PascalCase) into the `key -> value`
/// map the CodeCommit service state and `ListTagsForResource` use.
fn cc_tags(props: &Value) -> BTreeMap<String, String> {
    let mut out = BTreeMap::new();
    if let Some(arr) = props.get("Tags").and_then(Value::as_array) {
        for t in arr {
            let key = t.get("Key").and_then(Value::as_str).unwrap_or("");
            let value = t.get("Value").and_then(Value::as_str).unwrap_or("");
            if !key.is_empty() {
                out.insert(key.to_string(), value.to_string());
            }
        }
    }
    out
}

/// Convert the CFN `Triggers` property (a list of `RepositoryTrigger` objects in
/// PascalCase) into the camelCase `RepositoryTrigger` wire shape the service
/// state and `GetRepositoryTriggers` use.
fn cc_triggers(props: &Value) -> Vec<Value> {
    let mut out = Vec::new();
    if let Some(arr) = props.get("Triggers").and_then(Value::as_array) {
        for t in arr {
            let mut trig = Map::new();
            if let Some(v) = t.get("Name").and_then(Value::as_str) {
                trig.insert("name".into(), json!(v));
            }
            if let Some(v) = t.get("DestinationArn").and_then(Value::as_str) {
                trig.insert("destinationArn".into(), json!(v));
            }
            if let Some(v) = t.get("CustomData").and_then(Value::as_str) {
                trig.insert("customData".into(), json!(v));
            }
            if let Some(v) = t.get("Branches").and_then(Value::as_array) {
                trig.insert("branches".into(), json!(v));
            }
            if let Some(v) = t.get("Events").and_then(Value::as_array) {
                trig.insert("events".into(), json!(v));
            }
            if !trig.is_empty() {
                out.push(Value::Object(trig));
            }
        }
    }
    out
}
