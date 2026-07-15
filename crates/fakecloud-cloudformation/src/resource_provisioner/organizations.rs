//! `AWS::ORGANIZATIONS::*` CloudFormation provisioning (extracted from the provisioner's core module).

#![allow(clippy::too_many_lines)]

use super::*;

impl ResourceProvisioner {
    pub(crate) fn create_organization(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let feature_set = props
            .get("FeatureSet")
            .and_then(|v| v.as_str())
            .unwrap_or("ALL")
            .to_string();

        let mut org = self.organizations_state.write();
        if org.is_some() {
            return Err("Organization already exists; only one per fakecloud process".to_string());
        }
        let mut state = OrganizationState::bootstrap(&self.account_id);
        state.feature_set = feature_set;
        let org_id = state.org_id.clone();
        let org_arn = state.org_arn.clone();
        let mgmt_arn = state.management_account_arn.clone();
        let root_id = state.root_id.clone();
        *org = Some(state);

        Ok(ProvisionResult::new(org_id.clone())
            .with("Id", org_id)
            .with("Arn", org_arn)
            .with("ManagementAccountArn", mgmt_arn)
            .with("RootId", root_id))
    }

    pub(crate) fn delete_organization(&self, _physical_id: &str) -> Result<(), String> {
        let mut org = self.organizations_state.write();
        *org = None;
        Ok(())
    }

    pub(crate) fn create_organization_unit(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let name = props
            .get("Name")
            .and_then(|v| v.as_str())
            .unwrap_or(&resource.logical_id)
            .to_string();
        let parent_id = props
            .get("ParentId")
            .and_then(|v| v.as_str())
            .ok_or_else(|| "ParentId is required".to_string())?
            .to_string();

        let mut org_lock = self.organizations_state.write();
        let org = org_lock
            .as_mut()
            .ok_or_else(|| "Organization not yet created".to_string())?;
        // Accept root id, OU id, or `Ref`-resolved logical id (we map to root).
        let resolved_parent_id = if parent_id == org.root_id || org.ous.contains_key(&parent_id) {
            parent_id
        } else {
            return Err(format!("Parent {parent_id} does not exist"));
        };
        let id_suffix: String = Uuid::new_v4()
            .simple()
            .to_string()
            .chars()
            .take(8)
            .collect();
        let id = format!("ou-{}-{}", &org.root_id[2..], id_suffix);
        let arn = format!(
            "arn:aws:organizations::{}:ou/{}/{}",
            org.management_account_id, org.org_id, id
        );
        org.ous.insert(
            id.clone(),
            OrganizationalUnit {
                id: id.clone(),
                arn: arn.clone(),
                name: name.clone(),
                parent_id: resolved_parent_id,
            },
        );
        Ok(ProvisionResult::new(id.clone())
            .with("Id", id)
            .with("Arn", arn)
            .with("Name", name))
    }

    pub(crate) fn delete_organization_unit(&self, physical_id: &str) -> Result<(), String> {
        let mut org_lock = self.organizations_state.write();
        if let Some(org) = org_lock.as_mut() {
            org.ous.remove(physical_id);
            org.attachments.remove(physical_id);
        }
        Ok(())
    }

    /// Provision an `AWS::Organizations::Account`. Mints a new member
    /// account synchronously (via Organizations state), optionally moves
    /// it under the first ParentId when supplied, and persists tags.
    /// Provision an `AWS::Organizations::Account`. Mints a new member
    /// account synchronously (via Organizations state), optionally moves
    /// it under the first ParentId when supplied, and persists tags.
    pub(crate) fn create_organization_account(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let email = props
            .get("Email")
            .and_then(|v| v.as_str())
            .ok_or_else(|| "Email is required".to_string())?
            .to_string();
        let name = props
            .get("AccountName")
            .and_then(|v| v.as_str())
            .ok_or_else(|| "AccountName is required".to_string())?
            .to_string();
        let parent_ids: Vec<String> = props
            .get("ParentIds")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                    .collect()
            })
            .unwrap_or_default();
        let tags: Vec<(String, String)> = props
            .get("Tags")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|t| {
                        let k = t.get("Key").and_then(|v| v.as_str())?;
                        let val = t.get("Value").and_then(|v| v.as_str()).unwrap_or("");
                        Some((k.to_string(), val.to_string()))
                    })
                    .collect()
            })
            .unwrap_or_default();

        let mut org_lock = self.organizations_state.write();
        let org = org_lock
            .as_mut()
            .ok_or_else(|| "Organization not yet created".to_string())?;
        // CFN provisioning is its own asynchronous flow; we don't need
        // a second layer of poll-for-completion on top. Begin the
        // request and immediately drive it to SUCCEEDED so the rest of
        // this provisioner sees a fully enrolled account.
        let pending = org.begin_create_account(&email, &name, None);
        let status = org.complete_create_account(&pending.id).unwrap_or(pending);
        let account_id = status
            .account_id
            .clone()
            .ok_or_else(|| "create_account did not return an account id".to_string())?;
        let account_arn = org
            .accounts
            .get(&account_id)
            .map(|a| a.arn.clone())
            .unwrap_or_default();
        let joined_method = org
            .accounts
            .get(&account_id)
            .map(|a| a.joined_method.clone())
            .unwrap_or_else(|| "CREATED".to_string());
        let joined_timestamp = org
            .accounts
            .get(&account_id)
            .map(|a| a.joined_timestamp.to_rfc3339())
            .unwrap_or_default();
        let acct_status = org
            .accounts
            .get(&account_id)
            .map(|a| a.status.clone())
            .unwrap_or_else(|| "ACTIVE".to_string());

        if let Some(parent) = parent_ids.first() {
            let source = org
                .accounts
                .get(&account_id)
                .map(|a| a.parent_id.clone())
                .unwrap_or_else(|| org.root_id.clone());
            if parent != &source {
                org.move_account(&account_id, &source, parent)
                    .map_err(|e| format!("Failed to move account to parent {parent}: {e:?}"))?;
            }
        }

        if !tags.is_empty() {
            org.set_resource_tags(&account_id, &tags);
        }

        Ok(ProvisionResult::new(account_id.clone())
            .with("AccountId", account_id)
            .with("AccountName", name)
            .with("Email", email)
            .with("Arn", account_arn)
            .with("JoinedMethod", joined_method)
            .with("JoinedTimestamp", joined_timestamp)
            .with("Status", acct_status))
    }

    /// Close the member account on stack delete. Real AWS leaves the
    /// account in `SUSPENDED` for 90 days; we just flip it via
    /// `close_account` so subsequent reads see it as suspended.
    /// Close the member account on stack delete. Real AWS leaves the
    /// account in `SUSPENDED` for 90 days; we just flip it via
    /// `close_account` so subsequent reads see it as suspended.
    pub(crate) fn delete_organization_account(&self, physical_id: &str) -> Result<(), String> {
        let mut org_lock = self.organizations_state.write();
        if let Some(org) = org_lock.as_mut() {
            let _ = org.close_account(physical_id);
        }
        Ok(())
    }

    pub(crate) fn create_organization_policy(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let name = props
            .get("Name")
            .and_then(|v| v.as_str())
            .unwrap_or(&resource.logical_id)
            .to_string();
        let description = props
            .get("Description")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let policy_type = props
            .get("Type")
            .and_then(|v| v.as_str())
            .unwrap_or(POLICY_TYPE_SCP)
            .to_string();
        let content = props
            .get("Content")
            .map(|v| {
                if v.is_string() {
                    v.as_str().unwrap_or("").to_string()
                } else {
                    serde_json::to_string(v).unwrap_or_default()
                }
            })
            .unwrap_or_default();
        let target_ids: Vec<String> = props
            .get("TargetIds")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|t| t.as_str().map(|s| s.to_string()))
                    .collect()
            })
            .unwrap_or_default();

        let mut org_lock = self.organizations_state.write();
        let org = org_lock
            .as_mut()
            .ok_or_else(|| "Organization not yet created".to_string())?;
        let id_suffix: String = Uuid::new_v4()
            .simple()
            .to_string()
            .chars()
            .take(8)
            .collect();
        let id = format!("p-{}", id_suffix);
        let arn = format!(
            "arn:aws:organizations::{}:policy/{}/{}/{}",
            org.management_account_id,
            org.org_id,
            policy_type.to_lowercase(),
            id
        );
        org.policies.insert(
            id.clone(),
            OrgPolicy {
                id: id.clone(),
                arn: arn.clone(),
                name: name.clone(),
                description,
                policy_type,
                aws_managed: false,
                content,
            },
        );
        for target in target_ids {
            org.attachments
                .entry(target)
                .or_default()
                .insert(id.clone());
        }
        Ok(ProvisionResult::new(id.clone())
            .with("Id", id)
            .with("Arn", arn)
            .with("Name", name))
    }

    pub(crate) fn delete_organization_policy(&self, physical_id: &str) -> Result<(), String> {
        let mut org_lock = self.organizations_state.write();
        if let Some(org) = org_lock.as_mut() {
            org.policies.remove(physical_id);
            for attachments in org.attachments.values_mut() {
                attachments.remove(physical_id);
            }
        }
        Ok(())
    }

    pub(crate) fn create_organization_resource_policy(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let content = props
            .get("Content")
            .map(|v| {
                if v.is_string() {
                    v.as_str().unwrap_or("").to_string()
                } else {
                    serde_json::to_string(v).unwrap_or_default()
                }
            })
            .ok_or_else(|| "Content is required".to_string())?;

        let mut org_lock = self.organizations_state.write();
        let org = org_lock
            .as_mut()
            .ok_or_else(|| "Organization not yet created".to_string())?;
        org.resource_policy = Some(content);
        let arn = format!(
            "arn:aws:organizations::{}:resourcepolicy/{}/rp",
            org.management_account_id, org.org_id
        );
        Ok(ProvisionResult::new(arn.clone()).with("Arn", arn))
    }

    pub(crate) fn delete_organization_resource_policy(
        &self,
        _physical_id: &str,
    ) -> Result<(), String> {
        let mut org_lock = self.organizations_state.write();
        if let Some(org) = org_lock.as_mut() {
            org.resource_policy = None;
        }
        Ok(())
    }
}
