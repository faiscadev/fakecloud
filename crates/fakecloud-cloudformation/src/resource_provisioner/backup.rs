//! `AWS::Backup::BackupVault` and `AWS::Backup::BackupPlan` CloudFormation
//! provisioning. Each is written through to the `backup` service state as the
//! same typed `VaultRecord` / `PlanRecord` the direct `CreateBackupVault` /
//! `CreateBackupPlan` handlers store, so a CFN-created resource reads back on
//! `DescribeBackupVault` / `GetBackupPlan` and persists through the `backup`
//! snapshot hook (survives a restart -- #1766 class).
//!
//! `Ref` resolves to the vault name (Vault) / plan id (Plan). GetAtt exposes the
//! ARNs (+ `BackupVaultName` / `BackupPlanId` / `VersionId`).

use std::collections::BTreeMap;

use serde_json::Value;

use super::{ProvisionResult, ResourceDefinition, ResourceProvisioner, StackResource};
use fakecloud_backup::state::{plan_arn, vault_arn, PlanRecord, PlanVersion, VaultRecord};

impl ResourceProvisioner {
    // ------------------------------------------------------------ BackupVault

    pub(super) fn create_backup_vault(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let name = props
            .get("BackupVaultName")
            .and_then(Value::as_str)
            .map(str::to_string)
            .unwrap_or_else(|| resource.logical_id.clone());
        let arn = vault_arn(&self.region, &self.account_id, &name);
        let encryption = props
            .get("EncryptionKeyArn")
            .and_then(Value::as_str)
            .map(str::to_string);
        let notifications = props.get("Notifications").filter(|v| !v.is_null()).cloned();
        let access_policy = props.get("AccessPolicy").map(policy_to_string);

        let record = VaultRecord {
            name: name.clone(),
            arn: arn.clone(),
            vault_type: "BACKUP_VAULT".to_string(),
            vault_state: "AVAILABLE".to_string(),
            encryption_key_arn: encryption,
            creation_date: chrono::Utc::now(),
            creator_request_id: None,
            min_retention_days: None,
            max_retention_days: None,
            locked: false,
            lock_date: None,
            changeable_for_days: None,
            source_backup_vault_arn: None,
            access_policy,
            notifications,
            recovery_points: Default::default(),
        };

        let mut guard = self.backup_state.write();
        let st = guard.get_or_create(&self.account_id);
        if st.vaults.contains_key(&name) {
            return Err(format!("Backup vault {name} already exists"));
        }
        st.vaults.insert(name.clone(), record);
        let tags = tag_map(props.get("BackupVaultTags"));
        if !tags.is_empty() {
            st.tags.insert(arn.clone(), tags);
        }

        Ok(ProvisionResult::new(name.clone())
            .with("BackupVaultArn", arn)
            .with("BackupVaultName", name))
    }

    /// In-place `UpdateStack` for an `AWS::Backup::BackupVault`. Mutates the
    /// stored `VaultRecord` in place instead of the reprovision fallback's
    /// delete+recreate. `delete_backup_vault` removes the vault (and with it its
    /// `recovery_points`), so a benign `AccessPolicy` / `Notifications` / tag
    /// change would silently WIPE every stored recovery point. This applies the
    /// mutable properties and preserves the vault's identity (`arn`,
    /// `creation_date`) and, critically, its `recovery_points` -- which live on
    /// the untouched record.
    pub(super) fn update_backup_vault(
        &self,
        existing: &StackResource,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let name = existing.physical_id.clone();

        let mut guard = self.backup_state.write();
        let st = guard.get_or_create(&self.account_id);
        let arn = {
            let record = st
                .vaults
                .get_mut(&name)
                .ok_or_else(|| format!("Backup vault {name} not yet provisioned"))?;

            // Mutable properties. `recovery_points`, `arn`, `creation_date`,
            // `vault_type`, `vault_state` and any lock config are preserved.
            if props.get("AccessPolicy").is_some() {
                record.access_policy = props.get("AccessPolicy").map(policy_to_string);
            }
            if let Some(v) = props.get("Notifications").filter(|v| !v.is_null()) {
                record.notifications = Some(v.clone());
            }
            record.arn.clone()
        };
        let tags = tag_map(props.get("BackupVaultTags"));
        if tags.is_empty() {
            st.tags.remove(&arn);
        } else {
            st.tags.insert(arn.clone(), tags);
        }

        Ok(ProvisionResult::new(name.clone())
            .with("BackupVaultArn", arn)
            .with("BackupVaultName", name))
    }

    pub(super) fn delete_backup_vault(&self, physical_id: &str) -> Result<(), String> {
        let mut guard = self.backup_state.write();
        let st = guard.get_or_create(&self.account_id);
        if let Some(v) = st.vaults.remove(physical_id) {
            st.tags.remove(&v.arn);
        }
        Ok(())
    }

    pub(super) fn get_att_backup_vault(
        &self,
        physical_id: &str,
        attribute: &str,
    ) -> Option<String> {
        let guard = self.backup_state.read();
        let st = guard.get(&self.account_id)?;
        let v = st.vaults.get(physical_id)?;
        match attribute {
            "BackupVaultArn" => Some(v.arn.clone()),
            "BackupVaultName" => Some(v.name.clone()),
            _ => None,
        }
    }

    // ------------------------------------------------------------- BackupPlan

    pub(super) fn create_backup_plan(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let plan_src = props
            .get("BackupPlan")
            .filter(|v| v.is_object())
            .ok_or("AWS::Backup::BackupPlan requires BackupPlan")?;
        let plan_name = plan_src
            .get("BackupPlanName")
            .and_then(Value::as_str)
            .unwrap_or(&resource.logical_id)
            .to_string();

        // The CFN `BackupPlan` carries its rules under `BackupPlanRule`; the API
        // `BackupPlan` object uses `Rules`. Normalize so `GetBackupPlan` echoes
        // the API shape.
        let plan = normalize_backup_plan(plan_src);
        let advanced = plan_src
            .get("AdvancedBackupSettings")
            .cloned()
            .unwrap_or(Value::Array(Vec::new()));

        let id = uuid::Uuid::new_v4().to_string();
        let version = uuid::Uuid::new_v4().simple().to_string();
        let arn = plan_arn(&self.region, &self.account_id, &id);
        let now = chrono::Utc::now();

        let record = PlanRecord {
            id: id.clone(),
            arn: arn.clone(),
            version_id: version.clone(),
            creation_date: now,
            deletion_date: None,
            last_execution_date: None,
            creator_request_id: None,
            plan,
            advanced_backup_settings: advanced,
            selections: Default::default(),
            versions: vec![PlanVersion {
                version_id: version.clone(),
                creation_date: now,
                deletion_date: None,
                plan_name,
            }],
        };

        let mut guard = self.backup_state.write();
        let st = guard.get_or_create(&self.account_id);
        st.plans.insert(id.clone(), record);
        let tags = tag_map(props.get("BackupPlanTags"));
        if !tags.is_empty() {
            st.tags.insert(arn.clone(), tags);
        }

        Ok(ProvisionResult::new(id.clone())
            .with("BackupPlanArn", arn)
            .with("BackupPlanId", id)
            .with("VersionId", version))
    }

    pub(super) fn delete_backup_plan(&self, physical_id: &str) -> Result<(), String> {
        let mut guard = self.backup_state.write();
        let st = guard.get_or_create(&self.account_id);
        if let Some(p) = st.plans.remove(physical_id) {
            st.tags.remove(&p.arn);
        }
        Ok(())
    }

    pub(super) fn get_att_backup_plan(&self, physical_id: &str, attribute: &str) -> Option<String> {
        let guard = self.backup_state.read();
        let st = guard.get(&self.account_id)?;
        let p = st.plans.get(physical_id)?;
        match attribute {
            "BackupPlanArn" => Some(p.arn.clone()),
            "BackupPlanId" => Some(p.id.clone()),
            "VersionId" => Some(p.version_id.clone()),
            _ => None,
        }
    }
}

/// Convert a CFN tag-map property (`{ Key: Value }`) into the `TagMap` shape.
fn tag_map(value: Option<&Value>) -> BTreeMap<String, String> {
    let mut out = BTreeMap::new();
    if let Some(obj) = value.and_then(Value::as_object) {
        for (k, v) in obj {
            if let Some(s) = v.as_str() {
                out.insert(k.clone(), s.to_string());
            }
        }
    }
    out
}

/// A policy property rendered as a JSON string (inline object or string).
fn policy_to_string(value: &Value) -> String {
    match value {
        Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

/// Normalize the CFN `BackupPlan` object into the API shape: rename the
/// `BackupPlanRule` list to `Rules`, keeping everything else verbatim.
fn normalize_backup_plan(src: &Value) -> Value {
    let mut map = match src {
        Value::Object(m) => m.clone(),
        _ => serde_json::Map::new(),
    };
    if let Some(rules) = map.remove("BackupPlanRule") {
        map.insert("Rules".to_string(), rules);
    }
    map.remove("AdvancedBackupSettings");
    Value::Object(map)
}
