//! Auto-extracted from resource_provisioner/mod.rs by the
//! audit-2026-05-19 file-split. All methods here continue
//! the `impl ResourceProvisioner` block; the family slug is
//! `secrets`.

use super::*;

impl ResourceProvisioner {
    pub(super) fn get_att_secrets_manager_secret(
        &self,
        physical_id: &str,
        attribute: &str,
    ) -> Option<String> {
        let mut accounts = self.secretsmanager_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let secret = state.secrets.get(physical_id)?;
        match attribute {
            // Secrets Manager's CFN doc treats Id and Arn interchangeably —
            // both resolve to the secret ARN.
            "Arn" | "Id" => Some(secret.arn.clone()),
            _ => None,
        }
    }

    // --- SecretsManager ---

    pub(super) fn create_secrets_manager_secret(
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
            .map(|s| s.to_string());
        let kms_key_id = props
            .get("KmsKeyId")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());

        let mut accounts = self.secretsmanager_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let arn = format!(
            "arn:aws:secretsmanager:{}:{}:secret:{}",
            state.region, state.account_id, name
        );

        if state.secrets.contains_key(&arn) {
            return Err(format!("Secret {name} already exists"));
        }

        let now = Utc::now();
        let mut versions = BTreeMap::new();
        let mut current_version_id: Option<String> = None;
        let initial_string: Option<String> =
            if let Some(secret_string) = props.get("SecretString").and_then(|v| v.as_str()) {
                Some(secret_string.to_string())
            } else if let Some(gen) = props.get("GenerateSecretString") {
                Some(generate_secret_string_payload(gen)?)
            } else {
                None
            };
        if let Some(secret_string) = initial_string {
            let version_id = Uuid::new_v4().to_string();
            versions.insert(
                version_id.clone(),
                SecretVersion {
                    version_id: version_id.clone(),
                    secret_string: Some(secret_string),
                    secret_binary: None,
                    stages: vec!["AWSCURRENT".to_string()],
                    created_at: now,
                },
            );
            current_version_id = Some(version_id);
        }

        let mut tags: Vec<(String, String)> = Vec::new();
        if let Some(arr) = props.get("Tags").and_then(|v| v.as_array()) {
            for t in arr {
                if let (Some(k), Some(v)) = (
                    t.get("Key").and_then(|x| x.as_str()),
                    t.get("Value").and_then(|x| x.as_str()),
                ) {
                    tags.push((k.to_string(), v.to_string()));
                }
            }
        }
        let tags_set = !tags.is_empty();

        let secret = Secret {
            name: name.clone(),
            arn: arn.clone(),
            description,
            kms_key_id,
            versions,
            current_version_id,
            tags,
            tags_ever_set: tags_set,
            deleted: false,
            deletion_date: None,
            created_at: now,
            last_changed_at: now,
            last_accessed_at: None,
            rotation_enabled: None,
            rotation_lambda_arn: None,
            rotation_rules: None,
            last_rotated_at: None,
            resource_policy: None,
            replica_regions: Vec::new(),
        };
        state.secrets.insert(arn.clone(), secret);

        Ok(ProvisionResult::new(arn.clone())
            .with("Id", arn.clone())
            .with("Name", name))
    }

    pub(super) fn delete_secrets_manager_secret(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.secretsmanager_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.secrets.remove(physical_id);
        Ok(())
    }

    // --- SecretsManager extras ---

    pub(super) fn create_secrets_manager_rotation_schedule(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let secret_id = props
            .get("SecretId")
            .and_then(|v| v.as_str())
            .ok_or("SecretId is required")?
            .to_string();
        let rotation_lambda_arn = props
            .get("RotationLambdaARN")
            .and_then(|v| v.as_str())
            .map(String::from);
        let automatically_after_days = props
            .get("RotationRules")
            .and_then(|v| v.get("AutomaticallyAfterDays"))
            .and_then(|v| v.as_i64());
        let rotation_duration = props
            .get("RotationRules")
            .and_then(|v| v.get("Duration"))
            .and_then(|v| v.as_str())
            .map(String::from);
        let rotation_schedule_expression = props
            .get("RotationRules")
            .and_then(|v| v.get("ScheduleExpression"))
            .and_then(|v| v.as_str())
            .map(String::from);
        let mut accounts = self.secretsmanager_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let secret_arn = if state.secrets.contains_key(&secret_id) {
            secret_id.clone()
        } else {
            let candidate = format!(
                "arn:aws:secretsmanager:{}:{}:secret:{}",
                state.region, state.account_id, secret_id
            );
            if state.secrets.contains_key(&candidate) {
                candidate
            } else {
                return Err(format!("Secret {secret_id} not yet provisioned"));
            }
        };
        let secret = state
            .secrets
            .get_mut(&secret_arn)
            .ok_or_else(|| format!("Secret {secret_arn} not found"))?;
        secret.rotation_enabled = Some(true);
        secret.rotation_lambda_arn = rotation_lambda_arn;
        secret.rotation_rules = Some(RotationRules {
            automatically_after_days,
            duration: rotation_duration,
            schedule_expression: rotation_schedule_expression,
        });
        secret.last_changed_at = Utc::now();
        Ok(ProvisionResult::new(secret_arn.clone()).with("SecretArn", secret_arn))
    }

    pub(super) fn delete_secrets_manager_rotation_schedule(
        &self,
        physical_id: &str,
    ) -> Result<(), String> {
        let mut accounts = self.secretsmanager_state.write();
        let state = accounts.get_or_create(&self.account_id);
        if let Some(secret) = state.secrets.get_mut(physical_id) {
            secret.rotation_enabled = Some(false);
            secret.rotation_lambda_arn = None;
            secret.rotation_rules = None;
            secret.last_changed_at = Utc::now();
        }
        Ok(())
    }

    pub(super) fn create_secrets_manager_resource_policy(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let secret_id = props
            .get("SecretId")
            .and_then(|v| v.as_str())
            .ok_or("SecretId is required")?
            .to_string();
        let policy_doc = props
            .get("ResourcePolicy")
            .ok_or("ResourcePolicy is required")?;
        let policy_str = match policy_doc {
            serde_json::Value::String(s) => s.clone(),
            other => other.to_string(),
        };
        let mut accounts = self.secretsmanager_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let secret_arn = if state.secrets.contains_key(&secret_id) {
            secret_id.clone()
        } else {
            let candidate = format!(
                "arn:aws:secretsmanager:{}:{}:secret:{}",
                state.region, state.account_id, secret_id
            );
            if state.secrets.contains_key(&candidate) {
                candidate
            } else {
                return Err(format!("Secret {secret_id} not yet provisioned"));
            }
        };
        let secret = state
            .secrets
            .get_mut(&secret_arn)
            .ok_or_else(|| format!("Secret {secret_arn} not found"))?;
        secret.resource_policy = Some(policy_str);
        secret.last_changed_at = Utc::now();
        Ok(ProvisionResult::new(secret_arn.clone()).with("SecretArn", secret_arn))
    }

    pub(super) fn delete_secrets_manager_resource_policy(
        &self,
        physical_id: &str,
    ) -> Result<(), String> {
        let mut accounts = self.secretsmanager_state.write();
        let state = accounts.get_or_create(&self.account_id);
        if let Some(secret) = state.secrets.get_mut(physical_id) {
            secret.resource_policy = None;
            secret.last_changed_at = Utc::now();
        }
        Ok(())
    }

    pub(super) fn create_secrets_manager_target_attachment(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let secret_id = props
            .get("SecretId")
            .and_then(|v| v.as_str())
            .ok_or("SecretId is required")?
            .to_string();
        let target_type = props
            .get("TargetType")
            .and_then(|v| v.as_str())
            .ok_or("TargetType is required")?;
        let target_id = props
            .get("TargetId")
            .and_then(|v| v.as_str())
            .ok_or("TargetId is required")?;
        let mut accounts = self.secretsmanager_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let secret_arn = if state.secrets.contains_key(&secret_id) {
            secret_id.clone()
        } else {
            let candidate = format!(
                "arn:aws:secretsmanager:{}:{}:secret:{}",
                state.region, state.account_id, secret_id
            );
            if state.secrets.contains_key(&candidate) {
                candidate
            } else {
                return Err(format!("Secret {secret_id} not yet provisioned"));
            }
        };
        let secret = state
            .secrets
            .get_mut(&secret_arn)
            .ok_or_else(|| format!("Secret {secret_arn} not found"))?;
        // Patch the AWSCURRENT version with engine/host/dbInstanceIdentifier
        // so it shows as "attached" via the RDS-style schema CFN expects.
        // If the secret has no version yet (created without SecretString or
        // GenerateSecretString), seed one — this matches CFN's behaviour of
        // making the attachment usable on its own.
        let now = Utc::now();
        if secret.current_version_id.is_none() {
            let version_id = Uuid::new_v4().to_string();
            secret.versions.insert(
                version_id.clone(),
                SecretVersion {
                    version_id: version_id.clone(),
                    secret_string: Some("{}".to_string()),
                    secret_binary: None,
                    stages: vec!["AWSCURRENT".to_string()],
                    created_at: now,
                },
            );
            secret.current_version_id = Some(version_id);
        }
        if let Some(version_id) = secret.current_version_id.clone() {
            if let Some(version) = secret.versions.get_mut(&version_id) {
                let mut existing: serde_json::Value = version
                    .secret_string
                    .as_deref()
                    .and_then(|s| serde_json::from_str(s).ok())
                    .unwrap_or_else(|| serde_json::json!({}));
                if let Some(obj) = existing.as_object_mut() {
                    let engine = match target_type {
                        "AWS::RDS::DBInstance" | "AWS::RDS::DBCluster" => "postgres",
                        _ => "unknown",
                    };
                    obj.entry("engine".to_string())
                        .or_insert(serde_json::json!(engine));
                    obj.insert("host".to_string(), serde_json::json!(target_id));
                    obj.entry("dbInstanceIdentifier".to_string())
                        .or_insert(serde_json::json!(target_id));
                }
                version.secret_string = Some(existing.to_string());
            }
        }
        secret.last_changed_at = now;
        Ok(ProvisionResult::new(secret_arn.clone()).with("SecretArn", secret_arn))
    }
}
