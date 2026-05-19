//! Auto-extracted from resource_provisioner/mod.rs by the
//! audit-2026-05-19 file-split. All methods here continue
//! the `impl ResourceProvisioner` block; the family slug is
//! `stepfunctions`.

use super::*;

impl ResourceProvisioner {
    // --- Step Functions ---

    pub(super) fn create_sfn_state_machine(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let name = props
            .get("StateMachineName")
            .and_then(|v| v.as_str())
            .map(String::from)
            .unwrap_or_else(|| {
                let suffix = Uuid::new_v4().simple().to_string();
                format!("{}-{}", resource.logical_id, &suffix[..8])
            });
        let role_arn = props
            .get("RoleArn")
            .and_then(|v| v.as_str())
            .ok_or("RoleArn is required")?
            .to_string();
        let machine_type_str = props
            .get("StateMachineType")
            .and_then(|v| v.as_str())
            .unwrap_or("STANDARD");
        let machine_type = StateMachineType::parse(machine_type_str)
            .ok_or_else(|| format!("Invalid StateMachineType: {machine_type_str}"))?;
        let definition = props
            .get("DefinitionString")
            .and_then(|v| v.as_str())
            .map(String::from)
            .or_else(|| {
                props
                    .get("Definition")
                    .map(|v| serde_json::to_string(v).unwrap_or_default())
            })
            .ok_or("Definition or DefinitionString is required")?;
        let logging_configuration = props.get("LoggingConfiguration").cloned();
        let tracing_configuration = props.get("TracingConfiguration").cloned();

        let arn = format!(
            "arn:aws:states:{}:{}:stateMachine:{}",
            self.region, self.account_id, name
        );
        let now = Utc::now();
        let revision_id = Uuid::new_v4().to_string();

        let sm = StateMachine {
            name: name.clone(),
            arn: arn.clone(),
            definition,
            role_arn,
            machine_type,
            status: StateMachineStatus::Active,
            creation_date: now,
            update_date: now,
            tags: BTreeMap::new(),
            revision_id,
            logging_configuration,
            tracing_configuration,
            description: String::new(),
        };

        let mut accounts = self.stepfunctions_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.state_machines.insert(arn.clone(), sm);

        Ok(ProvisionResult::new(arn.clone())
            .with("Arn", arn.clone())
            .with("Name", name)
            .with("StateMachineRevisionId", "INITIAL"))
    }

    pub(super) fn delete_sfn_state_machine(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.stepfunctions_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.state_machines.remove(physical_id);
        Ok(())
    }

    pub(super) fn create_sfn_activity(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let name = props
            .get("Name")
            .and_then(|v| v.as_str())
            .ok_or("Name is required")?
            .to_string();
        let arn = format!(
            "arn:aws:states:{}:{}:activity:{}",
            self.region, self.account_id, name
        );
        let activity = SfnActivity {
            name: name.clone(),
            arn: arn.clone(),
            creation_date: Utc::now(),
            tags: BTreeMap::new(),
        };

        let mut accounts = self.stepfunctions_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.activities.insert(arn.clone(), activity);

        Ok(ProvisionResult::new(arn.clone())
            .with("Arn", arn)
            .with("Name", name))
    }

    pub(super) fn delete_sfn_activity(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.stepfunctions_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.activities.remove(physical_id);
        Ok(())
    }

    pub(super) fn create_sfn_version(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let sm_arn = props
            .get("StateMachineArn")
            .and_then(|v| v.as_str())
            .ok_or("StateMachineArn is required")?
            .to_string();
        let description = props
            .get("Description")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let revision_id = props
            .get("StateMachineRevisionId")
            .and_then(|v| v.as_str())
            .unwrap_or("INITIAL")
            .to_string();

        let mut accounts = self.stepfunctions_state.write();
        let state = accounts.get_or_create(&self.account_id);

        // Derive next version number for this state machine.
        let next_version = state
            .state_machine_versions
            .values()
            .filter(|v| v.state_machine_arn == sm_arn)
            .map(|v| v.version)
            .max()
            .unwrap_or(0)
            + 1;
        let version_arn = format!("{sm_arn}:{next_version}");

        let version = StateMachineVersion {
            state_machine_arn: sm_arn,
            version: next_version,
            revision_id,
            description,
            creation_date: Utc::now(),
        };
        state
            .state_machine_versions
            .insert(version_arn.clone(), version);

        Ok(ProvisionResult::new(version_arn.clone()).with("Arn", version_arn))
    }

    pub(super) fn delete_sfn_version(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.stepfunctions_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.state_machine_versions.remove(physical_id);
        Ok(())
    }

    pub(super) fn create_sfn_alias(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let name = props
            .get("Name")
            .and_then(|v| v.as_str())
            .ok_or("Name is required")?
            .to_string();
        let description = props
            .get("Description")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let routes_value = props
            .get("RoutingConfiguration")
            .and_then(|v| v.as_array())
            .ok_or("RoutingConfiguration is required")?;
        let routing_configuration: Vec<AliasRoute> = routes_value
            .iter()
            .map(|r| AliasRoute {
                state_machine_version_arn: r
                    .get("StateMachineVersionArn")
                    .and_then(|x| x.as_str())
                    .unwrap_or("")
                    .to_string(),
                weight: r
                    .get("Weight")
                    .and_then(|x| {
                        x.as_i64()
                            .or_else(|| x.as_str().and_then(|s| s.parse::<i64>().ok()))
                    })
                    .map(|w| w as i32)
                    .unwrap_or(0),
            })
            .collect();

        let first_version_arn = routing_configuration
            .first()
            .map(|r| r.state_machine_version_arn.clone())
            .unwrap_or_default();
        // Alias ARN derives from the parent state machine ARN (everything
        // before `:<version>`) + the alias name.
        let sm_arn_root = first_version_arn
            .rsplit_once(':')
            .map(|(root, _)| root.to_string())
            .unwrap_or_else(|| {
                format!(
                    "arn:aws:states:{}:{}:stateMachine:unknown",
                    self.region, self.account_id
                )
            });
        let arn = format!("{sm_arn_root}:{name}");
        let now = Utc::now();
        let alias = StateMachineAlias {
            name: name.clone(),
            arn: arn.clone(),
            description,
            routing_configuration,
            creation_date: now,
            update_date: now,
        };

        let mut accounts = self.stepfunctions_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.state_machine_aliases.insert(arn.clone(), alias);

        Ok(ProvisionResult::new(arn.clone())
            .with("Arn", arn)
            .with("Name", name))
    }

    pub(super) fn delete_sfn_alias(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.stepfunctions_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.state_machine_aliases.remove(physical_id);
        Ok(())
    }
}
