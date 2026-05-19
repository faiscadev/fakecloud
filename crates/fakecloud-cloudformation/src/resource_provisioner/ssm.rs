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

    pub(super) fn delete_ssm_parameter(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.ssm_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.parameters.remove(physical_id);
        Ok(())
    }
}
