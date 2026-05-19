//! Auto-extracted from resource_provisioner/mod.rs by the
//! audit-2026-05-19 file-split. All methods here continue
//! the `impl ResourceProvisioner` block; the family slug is
//! `ecr`.

use super::*;

impl ResourceProvisioner {
    // --- ECR ---

    pub(super) fn create_ecr_repository(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let repository_name = props
            .get("RepositoryName")
            .and_then(|v| v.as_str())
            .unwrap_or(&resource.logical_id)
            .to_string();
        let image_tag_mutability = props
            .get("ImageTagMutability")
            .and_then(|v| v.as_str())
            .unwrap_or("MUTABLE")
            .to_string();
        let scan_on_push = props
            .get("ImageScanningConfiguration")
            .and_then(|v| v.get("ScanOnPush"))
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let encryption_type = props
            .get("EncryptionConfiguration")
            .and_then(|v| v.get("EncryptionType"))
            .and_then(|v| v.as_str())
            .unwrap_or("AES256")
            .to_string();
        let kms_key = props
            .get("EncryptionConfiguration")
            .and_then(|v| v.get("KmsKey"))
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let policy_text = props
            .get("RepositoryPolicyText")
            .map(|v| {
                if v.is_string() {
                    v.as_str().unwrap_or("").to_string()
                } else {
                    serde_json::to_string(v).unwrap_or_default()
                }
            })
            .filter(|s| !s.is_empty());
        let lifecycle_policy = props
            .get("LifecyclePolicy")
            .and_then(|v| v.get("LifecyclePolicyText"))
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let mut tags: BTreeMap<String, String> = BTreeMap::new();
        if let Some(arr) = props.get("Tags").and_then(|v| v.as_array()) {
            for t in arr {
                if let (Some(k), Some(v)) = (
                    t.get("Key").and_then(|x| x.as_str()),
                    t.get("Value").and_then(|x| x.as_str()),
                ) {
                    tags.insert(k.to_string(), v.to_string());
                }
            }
        }

        let empty_on_delete = props
            .get("EmptyOnDelete")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);

        let mut accounts = self.ecr_state.write();
        let state = accounts.get_or_create(&self.account_id);
        if state.repositories.contains_key(&repository_name) {
            return Err(format!("Repository {repository_name} already exists"));
        }
        let arn = state.repository_arn(&repository_name);
        let registry_id = state.account_id.clone();
        let endpoint = format!(
            "{}.dkr.ecr.{}.amazonaws.com",
            state.account_id, state.region
        );
        let mut repo = Repository::new(&repository_name, arn.clone(), &registry_id, &endpoint);
        repo.image_tag_mutability = image_tag_mutability;
        repo.image_scanning_configuration.scan_on_push = scan_on_push;
        repo.encryption_configuration.encryption_type = encryption_type;
        repo.encryption_configuration.kms_key = kms_key;
        repo.policy = policy_text;
        // Apply lifecycle policy synchronously so the store reflects the
        // initial prune, matching the PutLifecyclePolicy data path.
        if let Some(policy) = lifecycle_policy.as_ref() {
            let prune = fakecloud_ecr::evaluate_lifecycle_policy(&repo, policy);
            for digest in &prune {
                repo.images.remove(digest);
                repo.image_tags.retain(|_, d| d != digest);
            }
            repo.lifecycle_policy_last_evaluated_at = Some(Utc::now());
        }
        repo.lifecycle_policy = lifecycle_policy;
        repo.tags = tags;
        let uri = repo.repository_uri.clone();
        state.repositories.insert(repository_name.clone(), repo);

        Ok(ProvisionResult::new(repository_name)
            .with("Arn", arn)
            .with("RepositoryUri", uri)
            .with("RegistryId", registry_id)
            .with("EmptyOnDelete", empty_on_delete.to_string()))
    }

    pub(super) fn delete_ecr_repository(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.ecr_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.repositories.remove(physical_id);
        Ok(())
    }

    /// Provision a standalone `AWS::ECR::RepositoryPolicy` against an
    /// existing repository. Physical id encodes `account/repo` so the
    /// delete path can find the right repository.
    pub(super) fn create_ecr_repository_policy(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let repository_name = props
            .get("RepositoryName")
            .and_then(|v| v.as_str())
            .ok_or_else(|| "RepositoryName is required".to_string())?
            .to_string();
        let policy_text = props
            .get("PolicyText")
            .map(|v| {
                if v.is_string() {
                    v.as_str().unwrap_or("").to_string()
                } else {
                    serde_json::to_string(v).unwrap_or_default()
                }
            })
            .ok_or_else(|| "PolicyText is required".to_string())?;
        let mut accounts = self.ecr_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let repo = state
            .repositories
            .get_mut(&repository_name)
            .ok_or_else(|| format!("Repository {repository_name} does not exist"))?;
        repo.policy = Some(policy_text);
        Ok(ProvisionResult::new(format!(
            "{}/{}",
            self.account_id, repository_name
        )))
    }

    pub(super) fn delete_ecr_repository_policy(&self, physical_id: &str) -> Result<(), String> {
        let repository_name = physical_id
            .split_once('/')
            .map(|(_, n)| n.to_string())
            .unwrap_or_else(|| physical_id.to_string());
        let mut accounts = self.ecr_state.write();
        let state = accounts.get_or_create(&self.account_id);
        if let Some(repo) = state.repositories.get_mut(&repository_name) {
            repo.policy = None;
        }
        Ok(())
    }

    /// Set the registry-wide policy for the active account. Physical id
    /// is just the account id since the registry is a singleton.
    pub(super) fn create_ecr_registry_policy(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let policy_text = props
            .get("PolicyText")
            .map(|v| {
                if v.is_string() {
                    v.as_str().unwrap_or("").to_string()
                } else {
                    serde_json::to_string(v).unwrap_or_default()
                }
            })
            .ok_or_else(|| "PolicyText is required".to_string())?;
        let mut accounts = self.ecr_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.registry_policy = Some(policy_text);
        Ok(ProvisionResult::new(self.account_id.clone())
            .with("RegistryId", self.account_id.clone()))
    }

    pub(super) fn delete_ecr_registry_policy(&self) -> Result<(), String> {
        let mut accounts = self.ecr_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.registry_policy = None;
        Ok(())
    }

    /// Provision the singleton `AWS::ECR::ReplicationConfiguration`.
    pub(super) fn create_ecr_replication_configuration(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        use fakecloud_ecr::{
            ReplicationConfiguration, ReplicationDestination, ReplicationRule, RepositoryFilter,
        };
        let cfg = resource
            .properties
            .get("ReplicationConfiguration")
            .ok_or_else(|| "ReplicationConfiguration is required".to_string())?;
        let rules: Vec<ReplicationRule> = cfg
            .get("Rules")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .map(|r| {
                        let destinations: Vec<ReplicationDestination> = r
                            .get("Destinations")
                            .and_then(|v| v.as_array())
                            .map(|d| {
                                d.iter()
                                    .map(|dest| ReplicationDestination {
                                        region: dest
                                            .get("Region")
                                            .and_then(|v| v.as_str())
                                            .unwrap_or_default()
                                            .to_string(),
                                        registry_id: dest
                                            .get("RegistryId")
                                            .and_then(|v| v.as_str())
                                            .unwrap_or_default()
                                            .to_string(),
                                    })
                                    .collect()
                            })
                            .unwrap_or_default();
                        let repository_filters: Vec<RepositoryFilter> = r
                            .get("RepositoryFilters")
                            .and_then(|v| v.as_array())
                            .map(|f| {
                                f.iter()
                                    .map(|f| RepositoryFilter {
                                        filter: f
                                            .get("Filter")
                                            .and_then(|v| v.as_str())
                                            .unwrap_or_default()
                                            .to_string(),
                                        filter_type: f
                                            .get("FilterType")
                                            .and_then(|v| v.as_str())
                                            .unwrap_or_default()
                                            .to_string(),
                                    })
                                    .collect()
                            })
                            .unwrap_or_default();
                        ReplicationRule {
                            destinations,
                            repository_filters,
                        }
                    })
                    .collect()
            })
            .unwrap_or_default();
        let mut accounts = self.ecr_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.replication_configuration = Some(ReplicationConfiguration { rules });
        Ok(ProvisionResult::new(self.account_id.clone()))
    }

    pub(super) fn delete_ecr_replication_configuration(&self) -> Result<(), String> {
        let mut accounts = self.ecr_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.replication_configuration = None;
        Ok(())
    }

    pub(super) fn create_ecr_pull_through_cache_rule(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        use fakecloud_ecr::PullThroughCacheRule;
        let props = &resource.properties;
        let prefix = props
            .get("EcrRepositoryPrefix")
            .and_then(|v| v.as_str())
            .ok_or_else(|| "EcrRepositoryPrefix is required".to_string())?
            .to_string();
        let upstream_url = props
            .get("UpstreamRegistryUrl")
            .and_then(|v| v.as_str())
            .ok_or_else(|| "UpstreamRegistryUrl is required".to_string())?
            .to_string();
        let upstream_registry = props
            .get("UpstreamRegistry")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let credential_arn = props
            .get("CredentialArn")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let custom_role_arn = props
            .get("CustomRoleArn")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let now = Utc::now();
        let rule = PullThroughCacheRule {
            ecr_repository_prefix: prefix.clone(),
            upstream_registry_url: upstream_url,
            upstream_registry,
            credential_arn,
            created_at: now,
            updated_at: now,
            custom_role_arn,
        };
        let mut accounts = self.ecr_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.pull_through_cache_rules.insert(prefix.clone(), rule);
        Ok(ProvisionResult::new(prefix))
    }

    pub(super) fn delete_ecr_pull_through_cache_rule(
        &self,
        physical_id: &str,
    ) -> Result<(), String> {
        let mut accounts = self.ecr_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.pull_through_cache_rules.remove(physical_id);
        Ok(())
    }

    /// Provision a standalone `AWS::ECR::LifecyclePolicy` against an
    /// existing repository. Mirrors the `PutLifecyclePolicy` data path:
    /// the policy text is stored on the repo and applied synchronously
    /// so the store reflects any prune set immediately. Physical id
    /// encodes `account/repo` so delete can find the right repository.
    pub(super) fn create_ecr_lifecycle_policy(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let repository_name = props
            .get("RepositoryName")
            .and_then(|v| v.as_str())
            .ok_or_else(|| "RepositoryName is required".to_string())?
            .to_string();
        let policy_text = props
            .get("LifecyclePolicyText")
            .map(|v| {
                if v.is_string() {
                    v.as_str().unwrap_or("").to_string()
                } else {
                    serde_json::to_string(v).unwrap_or_default()
                }
            })
            .ok_or_else(|| "LifecyclePolicyText is required".to_string())?;
        // RegistryId is informational on AWS — accepted but the registry
        // lookup is always the active account in fakecloud.
        let _registry_id = props
            .get("RegistryId")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let mut accounts = self.ecr_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let repo = state
            .repositories
            .get_mut(&repository_name)
            .ok_or_else(|| format!("Repository {repository_name} does not exist"))?;
        // Run the policy once now so callers see the same prune-on-write
        // behaviour they get from the JSON `PutLifecyclePolicy` endpoint.
        let prune = fakecloud_ecr::evaluate_lifecycle_policy(repo, &policy_text);
        for digest in &prune {
            repo.images.remove(digest);
            repo.image_tags.retain(|_, d| d != digest);
        }
        repo.lifecycle_policy = Some(policy_text);
        repo.lifecycle_policy_last_evaluated_at = Some(Utc::now());
        let registry_id = repo.registry_id.clone();
        Ok(
            ProvisionResult::new(format!("{}/{}", self.account_id, repository_name))
                .with("RepositoryName", repository_name)
                .with("RegistryId", registry_id),
        )
    }

    pub(super) fn delete_ecr_lifecycle_policy(&self, physical_id: &str) -> Result<(), String> {
        let repository_name = physical_id
            .split_once('/')
            .map(|(_, n)| n.to_string())
            .unwrap_or_else(|| physical_id.to_string());
        let mut accounts = self.ecr_state.write();
        let state = accounts.get_or_create(&self.account_id);
        if let Some(repo) = state.repositories.get_mut(&repository_name) {
            repo.lifecycle_policy = None;
            repo.lifecycle_policy_last_evaluated_at = None;
        }
        Ok(())
    }

    /// Provision the singleton `AWS::ECR::RegistryScanningConfiguration`.
    /// One per account; later applies overwrite the prior config —
    /// matching `PutRegistryScanningConfiguration` semantics.
    pub(super) fn create_ecr_registry_scanning_configuration(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        use fakecloud_ecr::{
            RegistryScanningConfiguration, RegistryScanningRule, RepositoryFilter,
        };
        let props = &resource.properties;
        let scan_type = props
            .get("ScanType")
            .and_then(|v| v.as_str())
            .unwrap_or("BASIC")
            .to_string();
        let rules: Vec<RegistryScanningRule> = props
            .get("Rules")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .map(|r| {
                        let scan_frequency = r
                            .get("ScanFrequency")
                            .and_then(|v| v.as_str())
                            .unwrap_or("MANUAL")
                            .to_string();
                        let repository_filters: Vec<RepositoryFilter> = r
                            .get("RepositoryFilters")
                            .and_then(|v| v.as_array())
                            .map(|f| {
                                f.iter()
                                    .map(|f| RepositoryFilter {
                                        filter: f
                                            .get("Filter")
                                            .and_then(|v| v.as_str())
                                            .unwrap_or_default()
                                            .to_string(),
                                        filter_type: f
                                            .get("FilterType")
                                            .and_then(|v| v.as_str())
                                            .unwrap_or_default()
                                            .to_string(),
                                    })
                                    .collect()
                            })
                            .unwrap_or_default();
                        RegistryScanningRule {
                            scan_frequency,
                            repository_filters,
                        }
                    })
                    .collect()
            })
            .unwrap_or_default();
        let mut accounts = self.ecr_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.registry_scanning_configuration = RegistryScanningConfiguration { scan_type, rules };
        Ok(ProvisionResult::new(self.account_id.clone()))
    }

    pub(super) fn delete_ecr_registry_scanning_configuration(&self) -> Result<(), String> {
        use fakecloud_ecr::RegistryScanningConfiguration;
        let mut accounts = self.ecr_state.write();
        let state = accounts.get_or_create(&self.account_id);
        // CFN delete reverts to the AWS default (BASIC, no rules).
        state.registry_scanning_configuration = RegistryScanningConfiguration::default();
        Ok(())
    }

    // ECR update_resource handlers. RepositoryName / EcrRepositoryPrefix
    // are immutable on AWS; CFN replaces the resource if they change.
    // These handlers refresh the mutable fields and keep the physical id
    // stable.

    pub(super) fn update_ecr_repository(
        &self,
        existing: &StackResource,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let repository_name = existing.physical_id.clone();
        let mut accounts = self.ecr_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let repo = state
            .repositories
            .get_mut(&repository_name)
            .ok_or_else(|| format!("Repository {repository_name} no longer exists"))?;
        if let Some(s) = props.get("ImageTagMutability").and_then(|v| v.as_str()) {
            repo.image_tag_mutability = s.to_string();
        }
        if let Some(b) = props
            .get("ImageScanningConfiguration")
            .and_then(|v| v.get("ScanOnPush"))
            .and_then(|v| v.as_bool())
        {
            repo.image_scanning_configuration.scan_on_push = b;
        }
        if let Some(cfg) = props.get("EncryptionConfiguration") {
            if let Some(s) = cfg.get("EncryptionType").and_then(|v| v.as_str()) {
                repo.encryption_configuration.encryption_type = s.to_string();
            }
            if let Some(s) = cfg.get("KmsKey").and_then(|v| v.as_str()) {
                repo.encryption_configuration.kms_key = Some(s.to_string());
            }
        }
        if let Some(v) = props.get("RepositoryPolicyText") {
            let text = if v.is_string() {
                v.as_str().unwrap_or("").to_string()
            } else {
                serde_json::to_string(v).unwrap_or_default()
            };
            repo.policy = if text.is_empty() { None } else { Some(text) };
        }
        if let Some(text) = props
            .get("LifecyclePolicy")
            .and_then(|v| v.get("LifecyclePolicyText"))
            .and_then(|v| v.as_str())
        {
            let prune = fakecloud_ecr::evaluate_lifecycle_policy(repo, text);
            for digest in &prune {
                repo.images.remove(digest);
                repo.image_tags.retain(|_, d| d != digest);
            }
            repo.lifecycle_policy = Some(text.to_string());
            repo.lifecycle_policy_last_evaluated_at = Some(Utc::now());
        }
        if let Some(arr) = props.get("Tags").and_then(|v| v.as_array()) {
            let mut tags: BTreeMap<String, String> = BTreeMap::new();
            for t in arr {
                if let (Some(k), Some(v)) = (
                    t.get("Key").and_then(|x| x.as_str()),
                    t.get("Value").and_then(|x| x.as_str()),
                ) {
                    tags.insert(k.to_string(), v.to_string());
                }
            }
            repo.tags = tags;
        }
        let arn = repo.repository_arn.clone();
        let uri = repo.repository_uri.clone();
        let registry_id = repo.registry_id.clone();
        Ok(ProvisionResult::new(repository_name)
            .with("Arn", arn)
            .with("RepositoryUri", uri)
            .with("RegistryId", registry_id))
    }

    pub(super) fn update_ecr_repository_policy(
        &self,
        existing: &StackResource,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let physical_id = existing.physical_id.clone();
        let repository_name = physical_id
            .split_once('/')
            .map(|(_, n)| n.to_string())
            .unwrap_or_else(|| physical_id.clone());
        let policy_text = props
            .get("PolicyText")
            .map(|v| {
                if v.is_string() {
                    v.as_str().unwrap_or("").to_string()
                } else {
                    serde_json::to_string(v).unwrap_or_default()
                }
            })
            .ok_or_else(|| "PolicyText is required".to_string())?;
        let mut accounts = self.ecr_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let repo = state
            .repositories
            .get_mut(&repository_name)
            .ok_or_else(|| format!("Repository {repository_name} does not exist"))?;
        repo.policy = Some(policy_text);
        Ok(ProvisionResult::new(physical_id))
    }

    pub(super) fn update_ecr_lifecycle_policy(
        &self,
        existing: &StackResource,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let physical_id = existing.physical_id.clone();
        let repository_name = physical_id
            .split_once('/')
            .map(|(_, n)| n.to_string())
            .unwrap_or_else(|| physical_id.clone());
        let policy_text = props
            .get("LifecyclePolicyText")
            .map(|v| {
                if v.is_string() {
                    v.as_str().unwrap_or("").to_string()
                } else {
                    serde_json::to_string(v).unwrap_or_default()
                }
            })
            .ok_or_else(|| "LifecyclePolicyText is required".to_string())?;
        let mut accounts = self.ecr_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let repo = state
            .repositories
            .get_mut(&repository_name)
            .ok_or_else(|| format!("Repository {repository_name} does not exist"))?;
        let prune = fakecloud_ecr::evaluate_lifecycle_policy(repo, &policy_text);
        for digest in &prune {
            repo.images.remove(digest);
            repo.image_tags.retain(|_, d| d != digest);
        }
        repo.lifecycle_policy = Some(policy_text);
        repo.lifecycle_policy_last_evaluated_at = Some(Utc::now());
        let registry_id = repo.registry_id.clone();
        Ok(ProvisionResult::new(physical_id)
            .with("RepositoryName", repository_name)
            .with("RegistryId", registry_id))
    }

    pub(super) fn update_ecr_registry_policy(
        &self,
        existing: &StackResource,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let policy_text = props
            .get("PolicyText")
            .map(|v| {
                if v.is_string() {
                    v.as_str().unwrap_or("").to_string()
                } else {
                    serde_json::to_string(v).unwrap_or_default()
                }
            })
            .ok_or_else(|| "PolicyText is required".to_string())?;
        let mut accounts = self.ecr_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.registry_policy = Some(policy_text);
        Ok(ProvisionResult::new(existing.physical_id.clone())
            .with("RegistryId", self.account_id.clone()))
    }

    pub(super) fn update_ecr_replication_configuration(
        &self,
        existing: &StackResource,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        // Updates fully replace the prior config — same as create.
        let result = self.create_ecr_replication_configuration(resource)?;
        Ok(ProvisionResult::new(existing.physical_id.clone()).merge_attributes(result.attributes))
    }

    pub(super) fn update_ecr_registry_scanning_configuration(
        &self,
        existing: &StackResource,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let result = self.create_ecr_registry_scanning_configuration(resource)?;
        Ok(ProvisionResult::new(existing.physical_id.clone()).merge_attributes(result.attributes))
    }

    pub(super) fn update_ecr_pull_through_cache_rule(
        &self,
        existing: &StackResource,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let prefix = existing.physical_id.clone();
        let mut accounts = self.ecr_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let rule = state
            .pull_through_cache_rules
            .get_mut(&prefix)
            .ok_or_else(|| format!("PullThroughCacheRule {prefix} no longer exists"))?;
        if let Some(s) = props.get("UpstreamRegistryUrl").and_then(|v| v.as_str()) {
            rule.upstream_registry_url = s.to_string();
        }
        if let Some(s) = props.get("UpstreamRegistry").and_then(|v| v.as_str()) {
            rule.upstream_registry = Some(s.to_string());
        }
        if let Some(s) = props.get("CredentialArn").and_then(|v| v.as_str()) {
            rule.credential_arn = Some(s.to_string());
        }
        if let Some(s) = props.get("CustomRoleArn").and_then(|v| v.as_str()) {
            rule.custom_role_arn = Some(s.to_string());
        }
        rule.updated_at = Utc::now();
        Ok(ProvisionResult::new(prefix))
    }

    pub(super) fn get_att_ecr_repository(
        &self,
        physical_id: &str,
        attribute: &str,
    ) -> Option<String> {
        let mut accounts = self.ecr_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let repo = state.repositories.get(physical_id)?;
        match attribute {
            "Arn" => Some(repo.repository_arn.clone()),
            "RepositoryUri" => Some(repo.repository_uri.clone()),
            "RegistryId" => Some(repo.registry_id.clone()),
            _ => None,
        }
    }
}
