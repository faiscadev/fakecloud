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
        let state = accounts.regional_mut(&self.account_id, &self.region);
        let key = state.secret_key(physical_id)?;
        let secret = state.secrets.get(&key)?;
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
        let generated_name = self.physical_name(resource);
        let name = props
            .get("Name")
            .and_then(|v| v.as_str())
            .unwrap_or(&generated_name)
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
        let state = accounts.regional_mut(&self.account_id, &self.region);
        // Same ARN form Secrets Manager's CreateSecret mints: the name plus
        // the random six-character suffix. The secret is keyed by name, like
        // every secret the service itself creates.
        // Same name rules as CreateSecret: a secret whose recovery window has
        // passed is purged, one still scheduled for deletion blocks the create.
        state
            .clear_name_for_create(&name, Utc::now())
            .map_err(|e| format!("{}: {}", e.code(), e.message()))?;
        if state.secrets.contains_key(&name) {
            return Err(format!(
                "ResourceExistsException: The operation failed because the secret {name} already exists."
            ));
        }
        let arn = fakecloud_secretsmanager::secret_arn(&self.region, &self.account_id, &name);

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
            // Stored encrypted, as CreateSecret stores it.
            let sealed = fakecloud_secretsmanager::value::seal_secret_string(
                self.kms_hook.as_deref(),
                &self.account_id,
                &self.region,
                &arn,
                &version_id,
                kms_key_id.as_deref(),
                secret_string,
            )
            .map_err(|e| format!("{}: {}", e.code(), e.message()))?;
            versions.insert(
                version_id.clone(),
                SecretVersion {
                    version_id: version_id.clone(),
                    secret_string: Some(sealed),
                    secret_binary: None,
                    stages: vec!["AWSCURRENT".to_string()],
                    created_at: now,
                    kms_key_ids: fakecloud_secretsmanager::value::sealed_kms_key_ids(
                        self.kms_hook.as_deref(),
                        kms_key_id.as_deref(),
                    ),
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
            rotation_failed_at: None,
            resource_policy: None,
            replica_regions: Vec::new(),
            replica_settings: Default::default(),
            primary_region: None,
        };
        state.secrets.insert(name.clone(), secret);

        Ok(ProvisionResult::new(arn.clone())
            .with("Id", arn.clone())
            .with("Name", name))
    }

    /// Apply a CFN property update to an existing Secrets Manager secret in
    /// place. `Description` and `KmsKeyId` update without replacement; a changed
    /// `SecretString` creates a new `AWSCURRENT` version (demoting the previous
    /// one to `AWSPREVIOUS`, keeping the single-`AWSPREVIOUS` invariant) exactly
    /// like `PutSecretValue`, so a stack update reaches the secret and
    /// `GetSecretValue` returns the new value instead of the stale one. `Name`
    /// is replacement-only and left untouched.
    pub(super) fn update_secrets_manager_secret(
        &self,
        existing: &StackResource,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let arn = &existing.physical_id;

        let mut accounts = self.secretsmanager_state.write();
        let state = accounts.regional_mut(&self.account_id, &self.region);
        let stored_secret = state
            .secret_key(arn)
            .and_then(|key| state.secrets.get_mut(&key))
            .ok_or_else(|| format!("Secret {arn} not yet provisioned"))?;
        let kms_err =
            |e: fakecloud_core::service::AwsServiceError| format!("{}: {}", e.code(), e.message());
        // The update is applied to a copy and stored only once every KMS
        // call has succeeded: a key KMS refuses fails the update with the
        // secret as it was, as UpdateSecret does.
        let mut secret = stored_secret.clone();

        if let Some(desc) = props.get("Description").and_then(|v| v.as_str()) {
            secret.description = Some(desc.to_string());
        }
        // A changed key re-encrypts the staged versions under it, exactly
        // like UpdateSecret with a new KmsKeyId.
        if let Some(kms) = props.get("KmsKeyId").and_then(|v| v.as_str()) {
            if secret.kms_key_id.as_deref() != Some(kms) {
                fakecloud_secretsmanager::change_secret_kms_key(
                    self.kms_hook.as_deref(),
                    &self.account_id,
                    &self.region,
                    &mut secret,
                    kms,
                )
                .map_err(kms_err)?;
            }
        }

        // A changed SecretString stages a new AWSCURRENT version. GenerateSecretString
        // is generate-once (only honoured at create), matching CloudFormation.
        if let Some(secret_string) = props.get("SecretString").and_then(|v| v.as_str()) {
            let current_plaintext = match secret
                .current_version_id
                .as_ref()
                .and_then(|vid| secret.versions.get(vid))
            {
                Some(v) => match v.secret_string.clone() {
                    Some(stored) => Some(
                        fakecloud_secretsmanager::value::plaintext_secret_string(
                            self.kms_hook.as_deref(),
                            &self.account_id,
                            &secret.arn,
                            &v.version_id,
                            stored,
                        )
                        .map_err(kms_err)?,
                    ),
                    None => None,
                },
                None => None,
            };
            if current_plaintext.as_deref() != Some(secret_string) {
                let now = Utc::now();
                let version_id = Uuid::new_v4().to_string();
                let sealed = fakecloud_secretsmanager::value::seal_secret_string(
                    self.kms_hook.as_deref(),
                    &self.account_id,
                    &self.region,
                    &secret.arn,
                    &version_id,
                    secret.kms_key_id.as_deref(),
                    secret_string.to_string(),
                )
                .map_err(kms_err)?;
                // Demote the outgoing AWSCURRENT to AWSPREVIOUS, enforcing the
                // single-AWSPREVIOUS invariant AWS keeps per secret.
                if let Some(old_vid) = secret.current_version_id.clone() {
                    for (id, v) in secret.versions.iter_mut() {
                        if id != &old_vid {
                            v.stages.retain(|s| s != "AWSPREVIOUS");
                        }
                    }
                    if let Some(old_v) = secret.versions.get_mut(&old_vid) {
                        old_v.stages.retain(|s| s != "AWSCURRENT");
                        if !old_v.stages.iter().any(|s| s == "AWSPREVIOUS") {
                            old_v.stages.push("AWSPREVIOUS".to_string());
                        }
                    }
                }
                let kms_key_ids = fakecloud_secretsmanager::value::sealed_kms_key_ids(
                    self.kms_hook.as_deref(),
                    secret.kms_key_id.as_deref(),
                );
                secret.versions.insert(
                    version_id.clone(),
                    SecretVersion {
                        version_id: version_id.clone(),
                        secret_string: Some(sealed),
                        secret_binary: None,
                        stages: vec!["AWSCURRENT".to_string()],
                        created_at: now,
                        kms_key_ids,
                    },
                );
                secret.current_version_id = Some(version_id);
                secret.last_changed_at = now;
            }
        }

        *stored_secret = secret;
        let secret = stored_secret;
        let name = secret.name.clone();
        Ok(ProvisionResult::new(arn.clone())
            .with("Id", arn.clone())
            .with("Name", name))
    }

    pub(super) fn delete_secrets_manager_secret(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.secretsmanager_state.write();
        let state = accounts.regional_mut(&self.account_id, &self.region);
        if let Some(key) = state.secret_key(physical_id) {
            state.secrets.remove(&key);
        }
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
        // Same bound RotateSecret enforces.
        if let Some(days) = automatically_after_days {
            if !(1..=1000).contains(&days) {
                return Err(format!(
                    "RotationRules.AutomaticallyAfterDays must be within 1-1000, got {days}."
                ));
            }
        }
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
        let state = accounts.regional_mut(&self.account_id, &self.region);
        let secret = state
            .secret_key(&secret_id)
            .and_then(|key| state.secrets.get_mut(&key))
            .ok_or_else(|| format!("Secret {secret_id} not yet provisioned"))?;
        let secret_arn = secret.arn.clone();
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
        let state = accounts.regional_mut(&self.account_id, &self.region);
        if let Some(secret) = state
            .secret_key(physical_id)
            .and_then(|key| state.secrets.get_mut(&key))
        {
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
        let state = accounts.regional_mut(&self.account_id, &self.region);
        let secret = state
            .secret_key(&secret_id)
            .and_then(|key| state.secrets.get_mut(&key))
            .ok_or_else(|| format!("Secret {secret_id} not yet provisioned"))?;
        let secret_arn = secret.arn.clone();
        secret.resource_policy = Some(policy_str);
        secret.last_changed_at = Utc::now();
        Ok(ProvisionResult::new(secret_arn.clone()).with("SecretArn", secret_arn))
    }

    pub(super) fn delete_secrets_manager_resource_policy(
        &self,
        physical_id: &str,
    ) -> Result<(), String> {
        let mut accounts = self.secretsmanager_state.write();
        let state = accounts.regional_mut(&self.account_id, &self.region);
        if let Some(secret) = state
            .secret_key(physical_id)
            .and_then(|key| state.secrets.get_mut(&key))
        {
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
        let state = accounts.regional_mut(&self.account_id, &self.region);
        let stored_secret = state
            .secret_key(&secret_id)
            .and_then(|key| state.secrets.get_mut(&key))
            .ok_or_else(|| format!("Secret {secret_id} not yet provisioned"))?;
        // Patched on a copy stored only once the value is sealed: a key KMS
        // refuses fails the attachment with the secret as it was, never
        // leaving a plaintext version at rest.
        let mut secret = stored_secret.clone();
        let secret_arn = secret.arn.clone();
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
                    kms_key_ids: Vec::new(),
                },
            );
            secret.current_version_id = Some(version_id);
        }
        if let Some(version_id) = secret.current_version_id.clone() {
            let kms_key_id = secret.kms_key_id.clone();
            if let Some(version) = secret.versions.get_mut(&version_id) {
                // The stored value is encrypted: read its plaintext, patch it,
                // and store it encrypted again.
                let plaintext = version
                    .secret_string
                    .clone()
                    .map(|stored| {
                        fakecloud_secretsmanager::value::plaintext_secret_string(
                            self.kms_hook.as_deref(),
                            &self.account_id,
                            &secret_arn,
                            &version_id,
                            stored,
                        )
                    })
                    .transpose()
                    .map_err(|e| format!("{}: {}", e.code(), e.message()))?;
                let mut existing: serde_json::Value = plaintext
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
                version.secret_string = Some(
                    fakecloud_secretsmanager::value::seal_secret_string(
                        self.kms_hook.as_deref(),
                        &self.account_id,
                        &self.region,
                        &secret_arn,
                        &version_id,
                        kms_key_id.as_deref(),
                        existing.to_string(),
                    )
                    .map_err(|e| format!("{}: {}", e.code(), e.message()))?,
                );
                if version.kms_key_ids.is_empty() {
                    version.kms_key_ids = fakecloud_secretsmanager::value::sealed_kms_key_ids(
                        self.kms_hook.as_deref(),
                        kms_key_id.as_deref(),
                    );
                }
            }
        }
        secret.last_changed_at = now;
        *stored_secret = secret;
        Ok(ProvisionResult::new(secret_arn.clone()).with("SecretArn", secret_arn))
    }
}
