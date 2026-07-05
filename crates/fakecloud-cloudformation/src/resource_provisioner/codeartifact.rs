//! `AWS::CodeArtifact::Domain` and `AWS::CodeArtifact::Repository`
//! CloudFormation provisioning. Each resource is written through to the
//! `codeartifact` service state as the same response-shaped JSON the direct
//! `CreateDomain` / `CreateRepository` handlers store, so a CFN-created domain
//! or repository reads back identically on `DescribeDomain` /
//! `DescribeRepository` and persists through the `codeartifact` snapshot hook
//! (survives a restart -- the #1766 phantom-resource lesson).
//!
//! Physical id + `Ref`: the AWS resource spec resolves `Ref` to the resource
//! ARN for both the Domain and the Repository, so the physical id IS the ARN.
//! Delete / live `Fn::GetAtt` parse the natural state key back out of that ARN.
//!
//! `Fn::GetAtt` (verified against the AWS resource spec):
//!   Domain     -> Arn, EncryptionKey, Name, Owner
//!   Repository -> Arn, DomainName, DomainOwner, Name

use chrono::{DateTime, Utc};
use serde_json::{json, Value};

use super::{ProvisionResult, ResourceDefinition, ResourceProvisioner, StackResource};

impl ResourceProvisioner {
    // -------------------------------------------------------------- Domain

    pub(super) fn create_codeartifact_domain(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let name = ca_str(props, "DomainName").unwrap_or_else(|| resource.logical_id.clone());
        let owner = self.account_id.clone();
        let region = self.region.clone();
        let arn = format!("arn:aws:codeartifact:{region}:{owner}:domain/{name}");
        // Mirror the direct CreateDomain handler: an omitted EncryptionKey mints a
        // synthetic KMS key ARN so the stored description round-trips a key.
        let encryption_key = ca_str(props, "EncryptionKey").unwrap_or_else(|| {
            format!("arn:aws:kms:{region}:{owner}:key/{}", uuid::Uuid::new_v4())
        });

        let mut guard = self.codeartifact_state.write();
        let acct = guard.get_or_create(&self.account_id);
        // CloudFormation provisions a stack single-threaded, so a plain
        // read-then-write is race-free; reject a same-name collision the way the
        // direct handler returns ConflictException.
        if acct.domains.contains_key(&name) {
            return Err(format!("Domain {name} already exists"));
        }
        let desc = json!({
            "name": name.clone(),
            "owner": owner.clone(),
            "arn": arn.clone(),
            "status": "Active",
            "createdTime": ca_ts(Utc::now()),
            "encryptionKey": encryption_key.clone(),
            "repositoryCount": 0,
            "assetSizeBytes": 0,
            "s3BucketArn": format!("arn:aws:s3:::assets-{owner}-{region}"),
        });
        acct.domains.insert(name.clone(), desc);
        acct.domain_order.push(name.clone());

        let tags = ca_tags(props);
        if !tags.is_empty() {
            acct.tags.insert(arn.clone(), tags);
        }
        if let Some(document) = permissions_policy_document(props) {
            acct.domain_policies.insert(
                name.clone(),
                json!({
                    "resourceArn": arn.clone(),
                    "revision": ca_revision(),
                    "document": document,
                }),
            );
        }

        Ok(ProvisionResult::new(arn.clone())
            .with("Arn", arn)
            .with("Name", name)
            .with("Owner", owner)
            .with("EncryptionKey", encryption_key))
    }

    pub(super) fn update_codeartifact_domain(
        &self,
        existing: &StackResource,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let old_name = ca_suffix(&existing.physical_id, "domain/")
            .ok_or_else(|| "corrupt CodeArtifact domain physical id".to_string())?
            .to_string();
        let new_name = ca_str(props, "DomainName").unwrap_or_else(|| existing.logical_id.clone());
        let new_enc = ca_str(props, "EncryptionKey");

        // DomainName and EncryptionKey are both replacement-required on the CFN
        // resource. A change to either can't be applied in place -- recreate.
        let old_enc = {
            let guard = self.codeartifact_state.read();
            guard
                .get(&self.account_id)
                .and_then(|a| a.domains.get(&old_name))
                .and_then(|d| d.get("encryptionKey").and_then(Value::as_str))
                .map(str::to_string)
        };
        let enc_changed = new_enc.is_some() && new_enc != old_enc;
        if new_name != old_name || enc_changed {
            self.delete_codeartifact_domain(&existing.physical_id);
            return self.create_codeartifact_domain(resource);
        }

        // In-place: PermissionsPolicyDocument and Tags are the only no-interruption
        // properties, so re-apply them (an omitted value is cleared).
        let arn = existing.physical_id.clone();
        let mut guard = self.codeartifact_state.write();
        let acct = guard.get_or_create(&self.account_id);
        let tags = ca_tags(props);
        if tags.is_empty() {
            acct.tags.remove(&arn);
        } else {
            acct.tags.insert(arn.clone(), tags);
        }
        match permissions_policy_document(props) {
            Some(document) => {
                acct.domain_policies.insert(
                    old_name.clone(),
                    json!({
                        "resourceArn": arn.clone(),
                        "revision": ca_revision(),
                        "document": document,
                    }),
                );
            }
            None => {
                acct.domain_policies.remove(&old_name);
            }
        }
        let d = acct
            .domains
            .get(&old_name)
            .ok_or_else(|| format!("CodeArtifact domain {old_name} not yet provisioned"))?;
        Ok(ProvisionResult::new(arn.clone())
            .with("Arn", arn)
            .with("Name", ca_field(d, "name"))
            .with("Owner", ca_field(d, "owner"))
            .with("EncryptionKey", ca_field(d, "encryptionKey")))
    }

    pub(super) fn get_att_codeartifact_domain(
        &self,
        physical_id: &str,
        attribute: &str,
    ) -> Option<String> {
        let name = ca_suffix(physical_id, "domain/")?;
        let guard = self.codeartifact_state.read();
        let d = guard.get(&self.account_id)?.domains.get(name)?;
        let key = match attribute {
            "Arn" => "arn",
            "EncryptionKey" => "encryptionKey",
            "Name" => "name",
            "Owner" => "owner",
            _ => return None,
        };
        d.get(key).and_then(Value::as_str).map(str::to_string)
    }

    pub(super) fn delete_codeartifact_domain(&self, physical_id: &str) {
        let Some(name) = ca_suffix(physical_id, "domain/").map(str::to_string) else {
            return;
        };
        let mut guard = self.codeartifact_state.write();
        let acct = guard.get_or_create(&self.account_id);
        acct.domains.remove(&name);
        acct.domain_order.retain(|n| n != &name);
        acct.domain_policies.remove(&name);
        acct.tags.remove(physical_id);
    }

    // ---------------------------------------------------------- Repository

    pub(super) fn create_codeartifact_repository(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let repo = ca_str(props, "RepositoryName").unwrap_or_else(|| resource.logical_id.clone());
        let domain = ca_str(props, "DomainName")
            .ok_or_else(|| "AWS::CodeArtifact::Repository requires DomainName".to_string())?;
        let owner = ca_str(props, "DomainOwner").unwrap_or_else(|| self.account_id.clone());
        let region = self.region.clone();
        let arn = format!("arn:aws:codeartifact:{region}:{owner}:repository/{domain}/{repo}");
        let key = format!("{domain}/{repo}");
        let description = ca_str(props, "Description").unwrap_or_default();
        let upstreams = cfn_upstreams(props);
        let external = cfn_external_connections(props);

        let mut guard = self.codeartifact_state.write();
        let acct = guard.get_or_create(&self.account_id);
        if !acct.domains.contains_key(&domain) {
            return Err(format!("Domain {domain} does not exist"));
        }
        if acct.repositories.contains_key(&key) {
            return Err(format!("Repository {repo} already exists"));
        }
        let desc = json!({
            "name": repo.clone(),
            "administratorAccount": owner.clone(),
            "domainName": domain.clone(),
            "domainOwner": owner.clone(),
            "arn": arn.clone(),
            "description": description,
            "upstreams": upstreams,
            "externalConnections": external,
            "createdTime": ca_ts(Utc::now()),
        });
        acct.repositories.insert(key.clone(), desc);
        acct.repository_order.push(key.clone());
        bump_repo_count(acct, &domain, 1);

        let tags = ca_tags(props);
        if !tags.is_empty() {
            acct.tags.insert(arn.clone(), tags);
        }
        if let Some(document) = permissions_policy_document(props) {
            acct.repository_policies.insert(
                key.clone(),
                json!({
                    "resourceArn": arn.clone(),
                    "revision": ca_revision(),
                    "document": document,
                }),
            );
        }

        Ok(ProvisionResult::new(arn.clone())
            .with("Arn", arn)
            .with("DomainName", domain)
            .with("DomainOwner", owner)
            .with("Name", repo))
    }

    pub(super) fn update_codeartifact_repository(
        &self,
        existing: &StackResource,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let old_key = ca_suffix(&existing.physical_id, "repository/")
            .ok_or_else(|| "corrupt CodeArtifact repository physical id".to_string())?
            .to_string();
        let (old_domain, old_repo) = old_key
            .split_once('/')
            .ok_or_else(|| "corrupt CodeArtifact repository key".to_string())?;
        let old_owner = ca_arn_account(&existing.physical_id).unwrap_or(&self.account_id);

        let new_repo =
            ca_str(props, "RepositoryName").unwrap_or_else(|| existing.logical_id.clone());
        let new_domain = ca_str(props, "DomainName")
            .ok_or_else(|| "AWS::CodeArtifact::Repository requires DomainName".to_string())?;
        let new_owner = ca_str(props, "DomainOwner").unwrap_or_else(|| self.account_id.clone());

        // RepositoryName, DomainName and DomainOwner are replacement-required --
        // any change re-provisions the repository from scratch.
        if new_repo != old_repo || new_domain != old_domain || new_owner != old_owner {
            self.delete_codeartifact_repository(&existing.physical_id);
            return self.create_codeartifact_repository(resource);
        }

        let arn = existing.physical_id.clone();
        let upstreams_present = props.get("Upstreams").is_some();
        let ext_present = props.get("ExternalConnections").is_some();
        let new_upstreams = cfn_upstreams(props);
        let new_external = cfn_external_connections(props);
        let description = ca_str(props, "Description");

        let mut guard = self.codeartifact_state.write();
        let acct = guard.get_or_create(&self.account_id);
        {
            let desc = acct
                .repositories
                .get_mut(&old_key)
                .ok_or_else(|| format!("Repository {old_repo} not yet provisioned"))?;
            if let Some(d) = description {
                desc["description"] = json!(d);
            }
            if upstreams_present {
                desc["upstreams"] = new_upstreams;
            }
            if ext_present {
                desc["externalConnections"] = new_external;
            }
        }
        let tags = ca_tags(props);
        if tags.is_empty() {
            acct.tags.remove(&arn);
        } else {
            acct.tags.insert(arn.clone(), tags);
        }
        match permissions_policy_document(props) {
            Some(document) => {
                acct.repository_policies.insert(
                    old_key.clone(),
                    json!({
                        "resourceArn": arn.clone(),
                        "revision": ca_revision(),
                        "document": document,
                    }),
                );
            }
            None => {
                acct.repository_policies.remove(&old_key);
            }
        }
        let desc = acct
            .repositories
            .get(&old_key)
            .ok_or_else(|| format!("Repository {old_repo} not yet provisioned"))?;
        Ok(ProvisionResult::new(arn.clone())
            .with("Arn", arn)
            .with("DomainName", ca_field(desc, "domainName"))
            .with("DomainOwner", ca_field(desc, "domainOwner"))
            .with("Name", ca_field(desc, "name")))
    }

    pub(super) fn get_att_codeartifact_repository(
        &self,
        physical_id: &str,
        attribute: &str,
    ) -> Option<String> {
        let key = ca_suffix(physical_id, "repository/")?;
        let guard = self.codeartifact_state.read();
        let r = guard.get(&self.account_id)?.repositories.get(key)?;
        let field = match attribute {
            "Arn" => "arn",
            "DomainName" => "domainName",
            "DomainOwner" => "domainOwner",
            "Name" => "name",
            _ => return None,
        };
        r.get(field).and_then(Value::as_str).map(str::to_string)
    }

    pub(super) fn delete_codeartifact_repository(&self, physical_id: &str) {
        let Some(key) = ca_suffix(physical_id, "repository/").map(str::to_string) else {
            return;
        };
        let mut guard = self.codeartifact_state.write();
        let acct = guard.get_or_create(&self.account_id);
        if acct.repositories.remove(&key).is_some() {
            acct.repository_order.retain(|k| k != &key);
            acct.repository_policies.remove(&key);
            acct.tags.remove(physical_id);
            if let Some(domain) = key.split('/').next() {
                bump_repo_count(acct, domain, -1);
            }
        }
    }
}

/// Read a non-empty string property.
fn ca_str(props: &Value, key: &str) -> Option<String> {
    props
        .get(key)
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
}

/// Read a stored string field, defaulting to empty.
fn ca_field(v: &Value, key: &str) -> String {
    v.get(key)
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string()
}

/// The CodeArtifact `createdTime` encoding used by the direct handlers: epoch
/// seconds as a float carrying millisecond precision.
fn ca_ts(dt: DateTime<Utc>) -> Value {
    let secs = dt.timestamp() as f64 + f64::from(dt.timestamp_subsec_millis()) / 1000.0;
    json!(secs)
}

/// An opaque resource-policy revision, matching AWS's opaque-token style.
fn ca_revision() -> String {
    uuid::Uuid::new_v4().simple().to_string()
}

/// The resource segment of a CodeArtifact ARN after `kind`. For a domain ARN
/// (`...:domain/name`) with `kind = "domain/"` this is the domain name; for a
/// repository ARN (`...:repository/domain/repo`) with `kind = "repository/"`
/// this is the `domain/repo` state key.
fn ca_suffix<'a>(arn: &'a str, kind: &str) -> Option<&'a str> {
    arn.rsplit_once(':')
        .and_then(|(_, res)| res.strip_prefix(kind))
}

/// The 12-digit account segment of a CodeArtifact ARN.
fn ca_arn_account(arn: &str) -> Option<&str> {
    arn.split(':').nth(4)
}

/// Convert CFN `Tags` (`[{Key,Value}]`, PascalCase) into the CodeArtifact wire
/// form (`[{key,value}]`, camelCase) the service state and `ListTagsForResource`
/// use.
fn ca_tags(props: &Value) -> Vec<Value> {
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

/// The `PermissionsPolicyDocument` property as a JSON string, accepting either an
/// inline object or an already-serialized string (matching the `document` field
/// the direct `PutDomainPermissionsPolicy` handler stores).
fn permissions_policy_document(props: &Value) -> Option<String> {
    match props.get("PermissionsPolicyDocument") {
        Some(Value::String(s)) if !s.is_empty() => Some(s.clone()),
        Some(Value::Null) | None => None,
        Some(other) => Some(other.to_string()),
    }
}

/// Convert the CFN `Upstreams` property (an array of repository-name strings)
/// into the API `upstreams` shape (`[{repositoryName}]`).
fn cfn_upstreams(props: &Value) -> Value {
    let mut out = Vec::new();
    if let Some(arr) = props.get("Upstreams").and_then(Value::as_array) {
        for u in arr {
            if let Some(name) = u.as_str().filter(|s| !s.is_empty()) {
                out.push(json!({ "repositoryName": name }));
            }
        }
    }
    Value::Array(out)
}

/// Convert the CFN `ExternalConnections` property (an array of connection-name
/// strings, e.g. `public:npmjs`) into the API `externalConnections` shape.
fn cfn_external_connections(props: &Value) -> Value {
    let mut out = Vec::new();
    if let Some(arr) = props.get("ExternalConnections").and_then(Value::as_array) {
        for c in arr {
            if let Some(name) = c.as_str().filter(|s| !s.is_empty()) {
                out.push(json!({
                    "externalConnectionName": name,
                    "packageFormat": connection_format(name),
                    "status": "Available",
                }));
            }
        }
    }
    Value::Array(out)
}

/// Map an external-connection name (`public:npmjs`) to its package format,
/// mirroring the direct `AssociateExternalConnection` handler.
fn connection_format(name: &str) -> &'static str {
    let n = name.to_ascii_lowercase();
    if n.contains("npm") {
        "npm"
    } else if n.contains("pypi") {
        "pypi"
    } else if n.contains("maven") {
        "maven"
    } else if n.contains("nuget") {
        "nuget"
    } else if n.contains("ruby") || n.contains("gems") {
        "ruby"
    } else if n.contains("crates") || n.contains("cargo") {
        "cargo"
    } else if n.contains("swift") {
        "swift"
    } else {
        "generic"
    }
}

/// Adjust a domain's `repositoryCount` by `delta`, clamped at zero. Mirrors the
/// direct handler's `bump_repo_count`.
fn bump_repo_count(acct: &mut fakecloud_codeartifact::CodeArtifactState, domain: &str, delta: i64) {
    if let Some(d) = acct.domains.get_mut(domain) {
        let cur = d
            .get("repositoryCount")
            .and_then(Value::as_i64)
            .unwrap_or(0);
        d["repositoryCount"] = json!((cur + delta).max(0));
    }
}
