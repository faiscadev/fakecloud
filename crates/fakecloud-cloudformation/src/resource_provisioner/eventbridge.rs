//! Auto-extracted from resource_provisioner/mod.rs by the
//! audit-2026-05-19 file-split. All methods here continue
//! the `impl ResourceProvisioner` block; the family slug is
//! `eventbridge`.

use super::*;

impl ResourceProvisioner {
    // --- EventBridge ---

    pub(super) fn create_eventbridge_rule(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let rule_name = props
            .get("Name")
            .and_then(|v| v.as_str())
            .unwrap_or(&resource.logical_id);
        let event_bus_name = props
            .get("EventBusName")
            .and_then(|v| v.as_str())
            .unwrap_or("default");

        let mut eb_accounts = self.eventbridge_state.write();
        let state = eb_accounts.get_or_create(&self.account_id);

        // Validate that the event bus exists
        if !state.buses.contains_key(event_bus_name) {
            return Err(format!("Event bus does not exist: {event_bus_name}"));
        }

        let arn = if event_bus_name == "default" {
            format!(
                "arn:aws:events:{}:{}:rule/{}",
                state.region, state.account_id, rule_name
            )
        } else {
            format!(
                "arn:aws:events:{}:{}:rule/{}/{}",
                state.region, state.account_id, event_bus_name, rule_name
            )
        };

        let rule = EventRule {
            name: rule_name.to_string(),
            arn: arn.clone(),
            event_bus_name: event_bus_name.to_string(),
            event_pattern: props.get("EventPattern").map(|v| {
                if v.is_string() {
                    v.as_str().unwrap().to_string()
                } else {
                    serde_json::to_string(v).unwrap_or_default()
                }
            }),
            schedule_expression: props
                .get("ScheduleExpression")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            state: props
                .get("State")
                .and_then(|v| v.as_str())
                .unwrap_or("ENABLED")
                .to_string(),
            description: props
                .get("Description")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            role_arn: props
                .get("RoleArn")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            managed_by: None,
            created_by: None,
            targets: Vec::new(),
            tags: std::collections::BTreeMap::new(),
            last_fired: None,
        };

        state
            .rules
            .insert((event_bus_name.to_string(), rule_name.to_string()), rule);
        Ok(ProvisionResult::new(arn.clone()).with("Arn", arn))
    }

    pub(super) fn delete_eventbridge_rule(&self, physical_id: &str) -> Result<(), String> {
        let mut eb_accounts = self.eventbridge_state.write();
        let state = eb_accounts.default_mut();
        // physical_id is the ARN; find the rule key
        let key = state
            .rules
            .iter()
            .find(|(_, r)| r.arn == physical_id)
            .map(|(k, _)| k.clone());
        if let Some(k) = key {
            state.rules.remove(&k);
        }
        Ok(())
    }

    pub(super) fn create_eventbridge_connection(
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
        let authorization_type = props
            .get("AuthorizationType")
            .and_then(|v| v.as_str())
            .unwrap_or("API_KEY")
            .to_string();
        let auth_parameters = props
            .get("AuthParameters")
            .cloned()
            .unwrap_or(serde_json::Value::Object(serde_json::Map::new()));

        let mut eb_accounts = self.eventbridge_state.write();
        let state = eb_accounts.get_or_create(&self.account_id);
        if state.connections.contains_key(&name) {
            return Err(format!("Connection {name} already exists"));
        }
        let now = Utc::now();
        let arn = format!(
            "arn:aws:events:{}:{}:connection/{}/{}",
            state.region,
            state.account_id,
            name,
            Uuid::new_v4().as_simple()
        );
        let secret_arn = format!(
            "arn:aws:secretsmanager:{}:{}:secret:events!connection/{}-{}",
            state.region,
            state.account_id,
            name,
            Uuid::new_v4().as_simple()
        );
        let connection = Connection {
            name: name.clone(),
            arn: arn.clone(),
            description,
            authorization_type,
            auth_parameters,
            connection_state: "AUTHORIZED".to_string(),
            secret_arn: secret_arn.clone(),
            creation_time: now,
            last_modified_time: now,
            last_authorized_time: now,
        };
        state.connections.insert(name.clone(), connection);

        Ok(ProvisionResult::new(name)
            .with("Arn", arn)
            .with("SecretArn", secret_arn))
    }

    pub(super) fn delete_eventbridge_connection(&self, physical_id: &str) -> Result<(), String> {
        let mut eb_accounts = self.eventbridge_state.write();
        let state = eb_accounts.get_or_create(&self.account_id);
        state.connections.remove(physical_id);
        Ok(())
    }

    pub(super) fn create_eventbridge_api_destination(
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
        let connection_arn = props
            .get("ConnectionArn")
            .and_then(|v| v.as_str())
            .ok_or_else(|| "ConnectionArn is required".to_string())?
            .to_string();
        let invocation_endpoint = props
            .get("InvocationEndpoint")
            .and_then(|v| v.as_str())
            .ok_or_else(|| "InvocationEndpoint is required".to_string())?
            .to_string();
        let http_method = props
            .get("HttpMethod")
            .and_then(|v| v.as_str())
            .unwrap_or("POST")
            .to_string();
        let invocation_rate_limit_per_second = props
            .get("InvocationRateLimitPerSecond")
            .and_then(|v| v.as_i64());

        let mut eb_accounts = self.eventbridge_state.write();
        let state = eb_accounts.get_or_create(&self.account_id);
        if state.api_destinations.contains_key(&name) {
            return Err(format!("ApiDestination {name} already exists"));
        }
        let now = Utc::now();
        let arn = format!(
            "arn:aws:events:{}:{}:api-destination/{}/{}",
            state.region,
            state.account_id,
            name,
            Uuid::new_v4().as_simple()
        );
        state.api_destinations.insert(
            name.clone(),
            ApiDestination {
                name: name.clone(),
                arn: arn.clone(),
                description,
                connection_arn,
                invocation_endpoint,
                http_method,
                invocation_rate_limit_per_second,
                state: "ACTIVE".to_string(),
                creation_time: now,
                last_modified_time: now,
            },
        );

        Ok(ProvisionResult::new(name).with("Arn", arn))
    }

    pub(super) fn delete_eventbridge_api_destination(
        &self,
        physical_id: &str,
    ) -> Result<(), String> {
        let mut eb_accounts = self.eventbridge_state.write();
        let state = eb_accounts.get_or_create(&self.account_id);
        state.api_destinations.remove(physical_id);
        Ok(())
    }

    pub(super) fn create_eventbridge_archive(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let name = props
            .get("ArchiveName")
            .and_then(|v| v.as_str())
            .unwrap_or(&resource.logical_id)
            .to_string();
        let event_source_arn = props
            .get("SourceArn")
            .and_then(|v| v.as_str())
            .ok_or_else(|| "SourceArn is required".to_string())?
            .to_string();
        let description = props
            .get("Description")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let event_pattern = props.get("EventPattern").map(|v| {
            if v.is_string() {
                v.as_str().unwrap_or("").to_string()
            } else {
                serde_json::to_string(v).unwrap_or_default()
            }
        });
        let retention_days = props
            .get("RetentionDays")
            .and_then(|v| v.as_i64())
            .unwrap_or(0);

        let mut eb_accounts = self.eventbridge_state.write();
        let state = eb_accounts.get_or_create(&self.account_id);
        if state.archives.contains_key(&name) {
            return Err(format!("Archive {name} already exists"));
        }
        let arn = format!(
            "arn:aws:events:{}:{}:archive/{}",
            state.region, state.account_id, name
        );
        state.archives.insert(
            name.clone(),
            Archive {
                name: name.clone(),
                arn: arn.clone(),
                event_source_arn,
                description,
                event_pattern,
                retention_days,
                state: "ENABLED".to_string(),
                creation_time: Utc::now(),
                event_count: 0,
                size_bytes: 0,
                events: Vec::new(),
            },
        );

        Ok(ProvisionResult::new(name).with("Arn", arn))
    }

    pub(super) fn delete_eventbridge_archive(&self, physical_id: &str) -> Result<(), String> {
        let mut eb_accounts = self.eventbridge_state.write();
        let state = eb_accounts.get_or_create(&self.account_id);
        state.archives.remove(physical_id);
        Ok(())
    }

    pub(super) fn create_eventbridge_event_bus(
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
            .map(String::from);
        let kms_key_identifier = props
            .get("KmsKeyIdentifier")
            .and_then(|v| v.as_str())
            .map(String::from);
        let dead_letter_config = props.get("DeadLetterConfig").cloned();
        let policy = props.get("Policy").cloned();
        let arn = format!(
            "arn:aws:events:{}:{}:event-bus/{name}",
            self.region, self.account_id
        );
        let now = Utc::now();
        let bus = EventBus {
            name: name.clone(),
            arn: arn.clone(),
            tags: BTreeMap::new(),
            policy,
            description,
            kms_key_identifier,
            dead_letter_config,
            creation_time: now,
            last_modified_time: now,
        };

        let mut eb_accounts = self.eventbridge_state.write();
        let state = eb_accounts.get_or_create(&self.account_id);
        state.buses.insert(name.clone(), bus);

        Ok(ProvisionResult::new(name).with("Arn", arn))
    }

    pub(super) fn delete_eventbridge_event_bus(&self, physical_id: &str) -> Result<(), String> {
        let mut eb_accounts = self.eventbridge_state.write();
        let state = eb_accounts.get_or_create(&self.account_id);
        // The default bus is reserved; refuse to delete it.
        if physical_id == "default" {
            return Ok(());
        }
        state.buses.remove(physical_id);
        Ok(())
    }

    pub(super) fn create_eventbridge_event_bus_policy(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let bus_name = props
            .get("EventBusName")
            .and_then(|v| v.as_str())
            .unwrap_or("default")
            .to_string();
        // Statement is the v2 shape; the older v1 shape has Action+Principal+
        // StatementId; both ultimately end up as a single statement on the bus
        // policy. We accept either form.
        let statement = if let Some(s) = props.get("Statement") {
            s.clone()
        } else {
            let sid = props
                .get("Sid")
                .or_else(|| props.get("StatementId"))
                .and_then(|v| v.as_str())
                .map(String::from);
            let action = props
                .get("Action")
                .and_then(|v| v.as_str())
                .map(String::from);
            let principal = props.get("Principal").cloned();
            let condition = props.get("Condition").cloned();
            let mut obj = serde_json::json!({
                "Effect": "Allow",
                "Resource": format!(
                    "arn:aws:events:{}:{}:event-bus/{bus_name}",
                    self.region, self.account_id
                ),
            });
            if let (Some(sid), Some(obj)) = (sid, obj.as_object_mut()) {
                obj.insert("Sid".to_string(), serde_json::Value::String(sid));
            }
            if let (Some(action), Some(obj)) = (action, obj.as_object_mut()) {
                obj.insert("Action".to_string(), serde_json::Value::String(action));
            }
            if let (Some(principal), Some(obj)) = (principal, obj.as_object_mut()) {
                obj.insert("Principal".to_string(), principal);
            }
            if let (Some(condition), Some(obj)) = (condition, obj.as_object_mut()) {
                obj.insert("Condition".to_string(), condition);
            }
            obj
        };

        let mut eb_accounts = self.eventbridge_state.write();
        let state = eb_accounts.get_or_create(&self.account_id);
        let bus = state
            .buses
            .get_mut(&bus_name)
            .ok_or_else(|| format!("EventBus {bus_name} not yet provisioned"))?;
        // Append to the existing policy's Statement array, or create one.
        match bus.policy.as_mut() {
            Some(serde_json::Value::Object(obj)) => {
                if let Some(serde_json::Value::Array(arr)) = obj.get_mut("Statement") {
                    arr.push(statement);
                } else {
                    obj.insert(
                        "Statement".to_string(),
                        serde_json::Value::Array(vec![statement]),
                    );
                }
            }
            _ => {
                bus.policy = Some(serde_json::json!({
                    "Version": "2012-10-17",
                    "Statement": [statement],
                }));
            }
        }

        // Physical id encodes the bus name + a synthetic id so delete locates the
        // entry even when the user never sets Sid/StatementId.
        let pid = format!("{bus_name}|{}", Uuid::new_v4().simple());
        Ok(ProvisionResult::new(pid))
    }

    pub(super) fn delete_eventbridge_event_bus_policy(
        &self,
        physical_id: &str,
    ) -> Result<(), String> {
        let bus_name = physical_id
            .split_once('|')
            .map(|(b, _)| b.to_string())
            .unwrap_or_else(|| "default".to_string());
        let mut eb_accounts = self.eventbridge_state.write();
        let state = eb_accounts.get_or_create(&self.account_id);
        if let Some(bus) = state.buses.get_mut(&bus_name) {
            bus.policy = None;
        }
        Ok(())
    }

    pub(super) fn create_eventbridge_endpoint(
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
            .map(String::from);
        let routing_config = props
            .get("RoutingConfig")
            .cloned()
            .ok_or("RoutingConfig is required")?;
        let replication_config = props.get("ReplicationConfig").cloned();
        let event_buses = props
            .get("EventBuses")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        let role_arn = props
            .get("RoleArn")
            .and_then(|v| v.as_str())
            .map(String::from);

        let endpoint_id = Uuid::new_v4().simple().to_string()[..16].to_string();
        let arn = format!(
            "arn:aws:events:{}:{}:endpoint/{name}",
            self.region, self.account_id
        );
        let endpoint_url = format!("https://{endpoint_id}.endpoint.events.amazonaws.com");
        let now = Utc::now();
        let endpoint = Endpoint {
            name: name.clone(),
            arn: arn.clone(),
            endpoint_id: endpoint_id.clone(),
            endpoint_url: Some(endpoint_url.clone()),
            description,
            routing_config,
            replication_config,
            event_buses,
            role_arn,
            state: "ACTIVE".to_string(),
            creation_time: now,
            last_modified_time: now,
        };

        let mut eb_accounts = self.eventbridge_state.write();
        let state = eb_accounts.get_or_create(&self.account_id);
        state.endpoints.insert(name.clone(), endpoint);

        Ok(ProvisionResult::new(name)
            .with("Arn", arn)
            .with("EndpointId", endpoint_id)
            .with("EndpointUrl", endpoint_url)
            .with("State", "ACTIVE"))
    }

    pub(super) fn delete_eventbridge_endpoint(&self, physical_id: &str) -> Result<(), String> {
        let mut eb_accounts = self.eventbridge_state.write();
        let state = eb_accounts.get_or_create(&self.account_id);
        state.endpoints.remove(physical_id);
        Ok(())
    }
}
