//! Auto-extracted from resource_provisioner/mod.rs by the
//! audit-2026-05-19 file-split. All methods here continue
//! the `impl ResourceProvisioner` block; the family slug is
//! `ssm`.

use super::*;

impl ResourceProvisioner {
    // --- SSM ---

    pub(super) fn create_ssm_parameter(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let name = props
            .get("Name")
            .and_then(|v| v.as_str())
            .ok_or("SSM Parameter requires Name")?;
        let value = props
            .get("Value")
            .and_then(|v| v.as_str())
            .ok_or("SSM Parameter requires Value")?;
        let param_type = props
            .get("Type")
            .and_then(|v| v.as_str())
            .unwrap_or("String");

        let mut accounts = self.ssm_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let arn = format!(
            "arn:aws:ssm:{}:{}:parameter{}",
            self.region,
            self.account_id,
            if name.starts_with('/') {
                name.to_string()
            } else {
                format!("/{name}")
            }
        );

        let parameter = SsmParameter {
            name: name.to_string(),
            value: value.to_string(),
            param_type: param_type.to_string(),
            version: 1,
            arn: arn.clone(),
            last_modified: Utc::now(),
            history: Vec::new(),
            tags: BTreeMap::new(),
            labels: BTreeMap::new(),
            description: props
                .get("Description")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            allowed_pattern: None,
            key_id: None,
            data_type: "text".to_string(),
            tier: "Standard".to_string(),
            policies: None,
            expiration_notified: false,
            no_change_notified: false,
        };

        state.parameters.insert(name.to_string(), parameter);
        Ok(ProvisionResult::new(name)
            .with("Type", param_type)
            .with("Value", value))
    }

    /// Apply a CFN property update to an existing SSM parameter in place.
    /// Mirrors the native `PutParameter` overwrite path (rotate the old value
    /// into history, bump the version, refresh Value/Type/Description) so a
    /// stack update actually reaches the parameter store and a subsequent
    /// `GetParameter` returns the new value instead of the stale one.
    pub(super) fn update_ssm_parameter(
        &self,
        existing: &StackResource,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let name = &existing.physical_id;
        let value = props
            .get("Value")
            .and_then(|v| v.as_str())
            .ok_or("SSM Parameter requires Value")?;

        let mut accounts = self.ssm_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let param = state
            .parameters
            .get_mut(name)
            .ok_or_else(|| format!("SSM parameter {name} not yet provisioned"))?;

        // Rotate the current value into history before overwriting, matching
        // the version-bump semantics of a direct PutParameter overwrite.
        let now = Utc::now();
        let current_labels = param
            .labels
            .get(&param.version)
            .cloned()
            .unwrap_or_default();
        param.history.push(fakecloud_ssm::SsmParameterVersion {
            value: param.value.clone(),
            version: param.version,
            last_modified: param.last_modified,
            param_type: param.param_type.clone(),
            description: param.description.clone(),
            key_id: param.key_id.clone(),
            labels: current_labels,
        });
        param.version += 1;
        param.value = value.to_string();
        param.last_modified = now;
        if let Some(pt) = props.get("Type").and_then(|v| v.as_str()) {
            param.param_type = pt.to_string();
        }
        if let Some(desc) = props.get("Description").and_then(|v| v.as_str()) {
            param.description = Some(desc.to_string());
        }
        param.expiration_notified = false;
        param.no_change_notified = false;

        let param_type = param.param_type.clone();
        Ok(ProvisionResult::new(name.clone())
            .with("Type", param_type)
            .with("Value", value))
    }

    pub(super) fn delete_ssm_parameter(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.ssm_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.parameters.remove(physical_id);
        Ok(())
    }
}
