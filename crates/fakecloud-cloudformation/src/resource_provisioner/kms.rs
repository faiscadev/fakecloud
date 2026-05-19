//! Auto-extracted from resource_provisioner/mod.rs by the
//! audit-2026-05-19 file-split. All methods here continue
//! the `impl ResourceProvisioner` block; the family slug is
//! `kms`.

use super::*;

impl ResourceProvisioner {
    pub(super) fn get_att_kms_key(&self, physical_id: &str, attribute: &str) -> Option<String> {
        let mut accounts = self.kms_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let key = state.keys.get(physical_id)?;
        match attribute {
            "Arn" => Some(key.arn.clone()),
            "KeyId" => Some(key.key_id.clone()),
            _ => None,
        }
    }

    // --- KMS ---

    pub(super) fn create_kms_key(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let input = parse_kms_key_input(&resource.properties);
        let (key_id, arn) =
            kms_provisioner::provision_key(&self.kms_state, &self.account_id, &input)?;
        Ok(ProvisionResult::new(key_id.clone())
            .with("Arn", arn)
            .with("KeyId", key_id))
    }

    /// Update an `AWS::KMS::Key` in place. Properties that AWS treats
    /// as immutable (`KeySpec`, `KeyUsage`, `Origin`, `MultiRegion`)
    /// trigger replacement: when the diff carries one of those, this
    /// returns an error and the upstream stack engine recreates the
    /// resource.
    pub(super) fn update_kms_key(
        &self,
        existing: &StackResource,
        new_def: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let new_input = parse_kms_key_input(&new_def.properties);
        let arn = {
            let mut accounts = self.kms_state.write();
            let state = accounts.get_or_create(&self.account_id);
            let key = state
                .keys
                .get(&existing.physical_id)
                .ok_or_else(|| format!("Key '{}' does not exist", existing.physical_id))?;
            if key.key_spec != new_input.key_spec
                || key.key_usage != new_input.key_usage
                || key.origin != new_input.origin
                || key.multi_region != new_input.multi_region
            {
                return Err(
                    "AWS::KMS::Key updates that change KeySpec, KeyUsage, Origin, or MultiRegion require replacement"
                        .to_string(),
                );
            }
            key.arn.clone()
        };
        kms_provisioner::update_key_properties(
            &self.kms_state,
            &self.account_id,
            &existing.physical_id,
            kms_provisioner::KeyUpdate {
                description: Some(new_input.description.clone()),
                enabled: Some(new_input.enabled),
                key_rotation_enabled: Some(new_input.key_rotation_enabled),
                policy: new_input.policy.clone(),
                tags: Some(new_input.tags.clone()),
            },
        )?;
        Ok(ProvisionResult::new(existing.physical_id.clone())
            .with("Arn", arn)
            .with("KeyId", existing.physical_id.clone()))
    }

    pub(super) fn delete_kms_key(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.kms_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.keys.remove(physical_id);
        state.aliases.retain(|_, a| a.target_key_id != physical_id);
        Ok(())
    }

    /// Provision an `AWS::KMS::ReplicaKey`. Looks up the primary key by
    /// arn and inserts a region-keyed replica into the same account
    /// state, mirroring the ReplicateKey API contract.
    pub(super) fn create_kms_replica_key(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let primary_arn = props
            .get("PrimaryKeyArn")
            .and_then(|v| v.as_str())
            .ok_or_else(|| "PrimaryKeyArn is required".to_string())?
            .to_string();
        let description = props
            .get("Description")
            .and_then(|v| v.as_str())
            .map(str::to_string);
        let enabled = props
            .get("Enabled")
            .and_then(|v| v.as_bool())
            .unwrap_or(true);
        let policy = parse_key_policy(props);
        let tags = parse_tag_list(props);

        let (replica_key_id, replica_arn) = kms_provisioner::provision_replica_key(
            &self.kms_state,
            &self.account_id,
            &primary_arn,
            description,
            enabled,
            policy,
            tags,
        )?;
        Ok(ProvisionResult::new(replica_key_id.clone())
            .with("KeyId", replica_key_id)
            .with("Arn", replica_arn))
    }

    /// Update an `AWS::KMS::ReplicaKey` in place. `PrimaryKeyArn`
    /// changes are replacement-only, like the AWS contract — we'd
    /// have to rebuild the replica from a different source. Other
    /// fields (description, enabled, policy, tags) flow through
    /// [`update_key_properties`].
    pub(super) fn update_kms_replica_key(
        &self,
        existing: &StackResource,
        new_def: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &new_def.properties;
        let new_primary = props
            .get("PrimaryKeyArn")
            .and_then(|v| v.as_str())
            .ok_or_else(|| "PrimaryKeyArn is required".to_string())?;
        // Compare primary region from existing replica's primary_region
        // field. Replacement triggers when the primary key changes.
        {
            let mut accounts = self.kms_state.write();
            let state = accounts.get_or_create(&self.account_id);
            let key = state
                .keys
                .get(&existing.physical_id)
                .ok_or_else(|| format!("ReplicaKey '{}' does not exist", existing.physical_id))?;
            if let Some(existing_region) = key.primary_region.as_deref() {
                let parts: Vec<&str> = new_primary.split(':').collect();
                if parts.len() < 4 || parts[3] != existing_region {
                    return Err(
                        "AWS::KMS::ReplicaKey updates that change PrimaryKeyArn require replacement"
                            .to_string(),
                    );
                }
            }
        }
        let description = props
            .get("Description")
            .and_then(|v| v.as_str())
            .map(str::to_string);
        let enabled = props
            .get("Enabled")
            .and_then(|v| v.as_bool())
            .unwrap_or(true);
        let policy = parse_key_policy(props);
        let tags = parse_tag_list(props);
        kms_provisioner::update_key_properties(
            &self.kms_state,
            &self.account_id,
            &existing.physical_id,
            kms_provisioner::KeyUpdate {
                description,
                enabled: Some(enabled),
                key_rotation_enabled: None,
                policy,
                tags: Some(tags),
            },
        )?;
        let arn = {
            let mut accounts = self.kms_state.write();
            let state = accounts.get_or_create(&self.account_id);
            state
                .keys
                .get(&existing.physical_id)
                .map(|k| k.arn.clone())
                .unwrap_or_default()
        };
        Ok(ProvisionResult::new(existing.physical_id.clone())
            .with("KeyId", existing.physical_id.clone())
            .with("Arn", arn))
    }

    pub(super) fn delete_kms_replica_key(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.kms_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.keys.remove(physical_id);
        Ok(())
    }

    pub(super) fn create_kms_alias(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let alias_name = props
            .get("AliasName")
            .and_then(|v| v.as_str())
            .ok_or_else(|| "AliasName is required".to_string())?
            .to_string();
        let target_input = props
            .get("TargetKeyId")
            .and_then(|v| v.as_str())
            .ok_or_else(|| "TargetKeyId is required".to_string())?
            .to_string();
        let alias = kms_provisioner::provision_alias(
            &self.kms_state,
            &self.account_id,
            &alias_name,
            &target_input,
        )?;
        Ok(ProvisionResult::new(alias))
    }

    /// Update an `AWS::KMS::Alias` in place. Changing `AliasName`
    /// triggers replacement (the alias is the resource's identity);
    /// only `TargetKeyId` is updatable here.
    pub(super) fn update_kms_alias(
        &self,
        existing: &StackResource,
        new_def: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &new_def.properties;
        let new_alias_name = props
            .get("AliasName")
            .and_then(|v| v.as_str())
            .ok_or_else(|| "AliasName is required".to_string())?;
        if new_alias_name != existing.physical_id {
            return Err(
                "AWS::KMS::Alias updates that change AliasName require replacement".to_string(),
            );
        }
        let target_input = props
            .get("TargetKeyId")
            .and_then(|v| v.as_str())
            .ok_or_else(|| "TargetKeyId is required".to_string())?;
        kms_provisioner::update_alias_target(
            &self.kms_state,
            &self.account_id,
            &existing.physical_id,
            target_input,
        )?;
        Ok(ProvisionResult::new(existing.physical_id.clone()))
    }

    pub(super) fn delete_kms_alias(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.kms_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.aliases.remove(physical_id);
        Ok(())
    }
}
