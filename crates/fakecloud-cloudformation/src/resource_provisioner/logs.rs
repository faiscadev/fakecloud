//! Auto-extracted from resource_provisioner/mod.rs by the
//! audit-2026-05-19 file-split. All methods here continue
//! the `impl ResourceProvisioner` block; the family slug is
//! `logs`.

use super::*;

impl ResourceProvisioner {
    // --- Logs: Destination / ResourcePolicy / QueryDefinition / Delivery* ---

    pub(super) fn create_logs_destination(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let destination_name = props
            .get("DestinationName")
            .and_then(|v| v.as_str())
            .ok_or("DestinationName is required")?
            .to_string();
        let target_arn = props
            .get("TargetArn")
            .and_then(|v| v.as_str())
            .ok_or("TargetArn is required")?
            .to_string();
        let role_arn = props
            .get("RoleArn")
            .and_then(|v| v.as_str())
            .ok_or("RoleArn is required")?
            .to_string();
        let access_policy = props
            .get("DestinationPolicy")
            .and_then(|v| v.as_str())
            .map(String::from);

        let arn = format!(
            "arn:aws:logs:{}:{}:destination:{destination_name}",
            self.region, self.account_id
        );
        let dest = Destination {
            destination_name: destination_name.clone(),
            target_arn,
            role_arn,
            arn: arn.clone(),
            access_policy,
            creation_time: Utc::now().timestamp_millis(),
            tags: BTreeMap::new(),
        };

        let mut logs_accounts = self.logs_state.write();
        let state = logs_accounts.get_or_create(&self.account_id);
        state.destinations.insert(destination_name.clone(), dest);

        Ok(ProvisionResult::new(destination_name).with("Arn", arn))
    }

    pub(super) fn delete_logs_destination(&self, physical_id: &str) -> Result<(), String> {
        let mut logs_accounts = self.logs_state.write();
        let state = logs_accounts.get_or_create(&self.account_id);
        state.destinations.remove(physical_id);
        Ok(())
    }

    pub(super) fn create_logs_resource_policy(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let policy_name = props
            .get("PolicyName")
            .and_then(|v| v.as_str())
            .ok_or("PolicyName is required")?
            .to_string();
        let policy_document = props
            .get("PolicyDocument")
            .map(|v| {
                if let Some(s) = v.as_str() {
                    s.to_string()
                } else {
                    serde_json::to_string(v).unwrap_or_default()
                }
            })
            .ok_or("PolicyDocument is required")?;

        let policy = ResourcePolicy {
            policy_name: policy_name.clone(),
            policy_document,
            last_updated_time: Utc::now().timestamp_millis(),
        };

        let mut logs_accounts = self.logs_state.write();
        let state = logs_accounts.get_or_create(&self.account_id);
        state.resource_policies.insert(policy_name.clone(), policy);

        Ok(ProvisionResult::new(policy_name))
    }

    pub(super) fn delete_logs_resource_policy(&self, physical_id: &str) -> Result<(), String> {
        let mut logs_accounts = self.logs_state.write();
        let state = logs_accounts.get_or_create(&self.account_id);
        state.resource_policies.remove(physical_id);
        Ok(())
    }

    pub(super) fn create_logs_query_definition(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let name = props
            .get("Name")
            .and_then(|v| v.as_str())
            .ok_or("Name is required")?
            .to_string();
        let query_string = props
            .get("QueryString")
            .and_then(|v| v.as_str())
            .ok_or("QueryString is required")?
            .to_string();
        let log_group_names: Vec<String> = props
            .get("LogGroupNames")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();

        let id = Uuid::new_v4().to_string();
        let qd = QueryDefinition {
            query_definition_id: id.clone(),
            name,
            query_string,
            log_group_names,
            last_modified: Utc::now().timestamp_millis(),
        };

        let mut logs_accounts = self.logs_state.write();
        let state = logs_accounts.get_or_create(&self.account_id);
        state.query_definitions.insert(id.clone(), qd);

        Ok(ProvisionResult::new(id.clone()).with("QueryDefinitionId", id))
    }

    pub(super) fn delete_logs_query_definition(&self, physical_id: &str) -> Result<(), String> {
        let mut logs_accounts = self.logs_state.write();
        let state = logs_accounts.get_or_create(&self.account_id);
        state.query_definitions.remove(physical_id);
        Ok(())
    }

    pub(super) fn create_logs_delivery_destination(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let name = props
            .get("Name")
            .and_then(|v| v.as_str())
            .ok_or("Name is required")?
            .to_string();
        let output_format = props
            .get("OutputFormat")
            .and_then(|v| v.as_str())
            .map(String::from);
        let mut configuration: BTreeMap<String, String> = BTreeMap::new();
        if let Some(arn) = props.get("DestinationResourceArn").and_then(|v| v.as_str()) {
            configuration.insert("destinationResourceArn".to_string(), arn.to_string());
        }
        if let Some(cfg) = props
            .get("DeliveryDestinationConfiguration")
            .and_then(|v| v.as_object())
        {
            for (k, v) in cfg {
                if let Some(s) = v.as_str() {
                    configuration.insert(k.clone(), s.to_string());
                }
            }
        }
        let policy = props.get("DeliveryDestinationPolicy").map(|v| {
            if let Some(s) = v.as_str() {
                s.to_string()
            } else {
                serde_json::to_string(v).unwrap_or_default()
            }
        });

        let arn = format!(
            "arn:aws:logs:{}:{}:delivery-destination:{name}",
            self.region, self.account_id
        );
        let dd = DeliveryDestination {
            name: name.clone(),
            arn: arn.clone(),
            output_format,
            delivery_destination_configuration: configuration,
            tags: BTreeMap::new(),
            delivery_destination_policy: policy,
        };

        let mut logs_accounts = self.logs_state.write();
        let state = logs_accounts.get_or_create(&self.account_id);
        state.delivery_destinations.insert(name.clone(), dd);

        Ok(ProvisionResult::new(name).with("Arn", arn))
    }

    pub(super) fn delete_logs_delivery_destination(&self, physical_id: &str) -> Result<(), String> {
        let mut logs_accounts = self.logs_state.write();
        let state = logs_accounts.get_or_create(&self.account_id);
        state.delivery_destinations.remove(physical_id);
        Ok(())
    }

    pub(super) fn create_logs_delivery_source(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let name = props
            .get("Name")
            .and_then(|v| v.as_str())
            .ok_or("Name is required")?
            .to_string();
        let resource_arns: Vec<String> = props
            .get("ResourceArn")
            .and_then(|v| v.as_str())
            .map(|s| vec![s.to_string()])
            .or_else(|| {
                props
                    .get("ResourceArns")
                    .and_then(|v| v.as_array())
                    .map(|arr| {
                        arr.iter()
                            .filter_map(|v| v.as_str().map(String::from))
                            .collect()
                    })
            })
            .unwrap_or_default();
        let log_type = props
            .get("LogType")
            .and_then(|v| v.as_str())
            .ok_or("LogType is required")?
            .to_string();
        let service = props
            .get("Service")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();

        let arn = format!(
            "arn:aws:logs:{}:{}:delivery-source:{name}",
            self.region, self.account_id
        );
        let ds = DeliverySource {
            name: name.clone(),
            arn: arn.clone(),
            resource_arns,
            service,
            log_type,
            tags: BTreeMap::new(),
            created_at: chrono::Utc::now().timestamp_millis(),
        };

        let mut logs_accounts = self.logs_state.write();
        let state = logs_accounts.get_or_create(&self.account_id);
        state.delivery_sources.insert(name.clone(), ds);

        Ok(ProvisionResult::new(name).with("Arn", arn))
    }

    pub(super) fn delete_logs_delivery_source(&self, physical_id: &str) -> Result<(), String> {
        let mut logs_accounts = self.logs_state.write();
        let state = logs_accounts.get_or_create(&self.account_id);
        state.delivery_sources.remove(physical_id);
        Ok(())
    }

    pub(super) fn create_logs_delivery(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let delivery_source_name = props
            .get("DeliverySourceName")
            .and_then(|v| v.as_str())
            .ok_or("DeliverySourceName is required")?
            .to_string();
        let delivery_destination_arn = props
            .get("DeliveryDestinationArn")
            .and_then(|v| v.as_str())
            .ok_or("DeliveryDestinationArn is required")?
            .to_string();
        // Infer destination type from the destination ARN service segment.
        let delivery_destination_type = if delivery_destination_arn.contains(":s3:") {
            "S3".to_string()
        } else if delivery_destination_arn.contains(":firehose:") {
            "FH".to_string()
        } else {
            "CWL".to_string()
        };

        let id = Uuid::new_v4().simple().to_string();
        let arn = format!(
            "arn:aws:logs:{}:{}:delivery:{id}",
            self.region, self.account_id
        );
        let delivery = Delivery {
            id: id.clone(),
            delivery_source_name,
            delivery_destination_arn,
            delivery_destination_type,
            arn: arn.clone(),
            tags: BTreeMap::new(),
            field_delimiter: None,
            record_fields: Vec::new(),
            s3_delivery_configuration: None,
            created_at: chrono::Utc::now().timestamp_millis(),
        };

        let mut logs_accounts = self.logs_state.write();
        let state = logs_accounts.get_or_create(&self.account_id);
        state.deliveries.insert(id.clone(), delivery);

        Ok(ProvisionResult::new(id.clone())
            .with("DeliveryId", id)
            .with("Arn", arn))
    }

    pub(super) fn delete_logs_delivery(&self, physical_id: &str) -> Result<(), String> {
        let mut logs_accounts = self.logs_state.write();
        let state = logs_accounts.get_or_create(&self.account_id);
        state.deliveries.remove(physical_id);
        Ok(())
    }
}
