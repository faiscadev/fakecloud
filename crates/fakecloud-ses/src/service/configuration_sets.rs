use http::StatusCode;
use serde_json::{json, Value};

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use crate::state::ConfigurationSet;

use super::{
    event_destination_to_json, extract_string_array, parse_event_destination_definition,
    SesV2Service,
};

impl SesV2Service {
    pub(super) fn create_configuration_set(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = Self::parse_body(req)?;
        let name = match body["ConfigurationSetName"].as_str() {
            Some(n) => n.to_string(),
            None => {
                return Ok(Self::json_error(
                    StatusCode::BAD_REQUEST,
                    "BadRequestException",
                    "ConfigurationSetName is required",
                ));
            }
        };

        let mut state = self.state.write();

        if state.configuration_sets.contains_key(&name) {
            return Ok(Self::json_error(
                StatusCode::CONFLICT,
                "AlreadyExistsException",
                &format!("Configuration set {} already exists", name),
            ));
        }

        state.configuration_sets.insert(
            name.clone(),
            ConfigurationSet {
                name,
                sending_enabled: true,
                tls_policy: "OPTIONAL".to_string(),
                sending_pool_name: None,
                custom_redirect_domain: None,
                https_policy: None,
                suppressed_reasons: Vec::new(),
                reputation_metrics_enabled: false,
                vdm_options: None,
                archive_arn: None,
            },
        );

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    pub(super) fn list_configuration_sets(&self) -> Result<AwsResponse, AwsServiceError> {
        let state = self.state.read();
        let sets: Vec<Value> = state
            .configuration_sets
            .keys()
            .map(|name| json!(name))
            .collect();

        let response = json!({
            "ConfigurationSets": sets,
        });

        Ok(AwsResponse::json(StatusCode::OK, response.to_string()))
    }

    pub(super) fn get_configuration_set(&self, name: &str) -> Result<AwsResponse, AwsServiceError> {
        let state = self.state.read();

        let cs = match state.configuration_sets.get(name) {
            Some(cs) => cs,
            None => {
                return Ok(Self::json_error(
                    StatusCode::NOT_FOUND,
                    "NotFoundException",
                    &format!("Configuration set {} does not exist", name),
                ));
            }
        };

        let mut delivery_options = json!({
            "TlsPolicy": cs.tls_policy,
        });
        if let Some(ref pool) = cs.sending_pool_name {
            delivery_options["SendingPoolName"] = json!(pool);
        }

        let mut tracking_options = json!({});
        if let Some(ref domain) = cs.custom_redirect_domain {
            tracking_options["CustomRedirectDomain"] = json!(domain);
        }
        if let Some(ref policy) = cs.https_policy {
            tracking_options["HttpsPolicy"] = json!(policy);
        }

        let mut response = json!({
            "ConfigurationSetName": name,
            "DeliveryOptions": delivery_options,
            "ReputationOptions": {
                "ReputationMetricsEnabled": cs.reputation_metrics_enabled,
            },
            "SendingOptions": {
                "SendingEnabled": cs.sending_enabled,
            },
            "Tags": [],
            "TrackingOptions": tracking_options,
        });

        if !cs.suppressed_reasons.is_empty() {
            response["SuppressionOptions"] = json!({
                "SuppressedReasons": cs.suppressed_reasons,
            });
        }

        if let Some(ref vdm) = cs.vdm_options {
            response["VdmOptions"] = vdm.clone();
        }

        if let Some(ref arn) = cs.archive_arn {
            response["ArchivingOptions"] = json!({
                "ArchiveArn": arn,
            });
        }

        Ok(AwsResponse::json(StatusCode::OK, response.to_string()))
    }

    pub(super) fn delete_configuration_set(
        &self,
        name: &str,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut state = self.state.write();

        if state.configuration_sets.remove(name).is_none() {
            return Ok(Self::json_error(
                StatusCode::NOT_FOUND,
                "NotFoundException",
                &format!("Configuration set {} does not exist", name),
            ));
        }

        // Remove tags for this configuration set
        let arn = format!(
            "arn:aws:ses:{}:{}:configuration-set/{}",
            req.region, req.account_id, name
        );
        state.tags.remove(&arn);

        // Remove event destinations for this configuration set
        state.event_destinations.remove(name);

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    // --- Configuration Set Options ---

    pub(super) fn put_configuration_set_sending_options(
        &self,
        name: &str,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = Self::parse_body(req)?;
        let mut state = self.state.write();

        let cs = match state.configuration_sets.get_mut(name) {
            Some(cs) => cs,
            None => {
                return Ok(Self::json_error(
                    StatusCode::NOT_FOUND,
                    "NotFoundException",
                    &format!("Configuration set {} does not exist", name),
                ));
            }
        };

        if let Some(enabled) = body["SendingEnabled"].as_bool() {
            cs.sending_enabled = enabled;
        }

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    pub(super) fn put_configuration_set_delivery_options(
        &self,
        name: &str,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = Self::parse_body(req)?;
        let mut state = self.state.write();

        let cs = match state.configuration_sets.get_mut(name) {
            Some(cs) => cs,
            None => {
                return Ok(Self::json_error(
                    StatusCode::NOT_FOUND,
                    "NotFoundException",
                    &format!("Configuration set {} does not exist", name),
                ));
            }
        };

        if let Some(policy) = body["TlsPolicy"].as_str() {
            cs.tls_policy = policy.to_string();
        }
        if let Some(pool) = body["SendingPoolName"].as_str() {
            cs.sending_pool_name = Some(pool.to_string());
        }

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    pub(super) fn put_configuration_set_tracking_options(
        &self,
        name: &str,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = Self::parse_body(req)?;
        let mut state = self.state.write();

        let cs = match state.configuration_sets.get_mut(name) {
            Some(cs) => cs,
            None => {
                return Ok(Self::json_error(
                    StatusCode::NOT_FOUND,
                    "NotFoundException",
                    &format!("Configuration set {} does not exist", name),
                ));
            }
        };

        if let Some(domain) = body["CustomRedirectDomain"].as_str() {
            cs.custom_redirect_domain = Some(domain.to_string());
        }
        if let Some(policy) = body["HttpsPolicy"].as_str() {
            cs.https_policy = Some(policy.to_string());
        }

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    pub(super) fn put_configuration_set_suppression_options(
        &self,
        name: &str,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = Self::parse_body(req)?;
        let mut state = self.state.write();

        let cs = match state.configuration_sets.get_mut(name) {
            Some(cs) => cs,
            None => {
                return Ok(Self::json_error(
                    StatusCode::NOT_FOUND,
                    "NotFoundException",
                    &format!("Configuration set {} does not exist", name),
                ));
            }
        };

        cs.suppressed_reasons = extract_string_array(&body["SuppressedReasons"]);

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    pub(super) fn put_configuration_set_reputation_options(
        &self,
        name: &str,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = Self::parse_body(req)?;
        let mut state = self.state.write();

        let cs = match state.configuration_sets.get_mut(name) {
            Some(cs) => cs,
            None => {
                return Ok(Self::json_error(
                    StatusCode::NOT_FOUND,
                    "NotFoundException",
                    &format!("Configuration set {} does not exist", name),
                ));
            }
        };

        if let Some(enabled) = body["ReputationMetricsEnabled"].as_bool() {
            cs.reputation_metrics_enabled = enabled;
        }

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    pub(super) fn put_configuration_set_vdm_options(
        &self,
        name: &str,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = Self::parse_body(req)?;
        let mut state = self.state.write();

        let cs = match state.configuration_sets.get_mut(name) {
            Some(cs) => cs,
            None => {
                return Ok(Self::json_error(
                    StatusCode::NOT_FOUND,
                    "NotFoundException",
                    &format!("Configuration set {} does not exist", name),
                ));
            }
        };

        cs.vdm_options = Some(body);

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    pub(super) fn put_configuration_set_archiving_options(
        &self,
        name: &str,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = Self::parse_body(req)?;
        let mut state = self.state.write();

        let cs = match state.configuration_sets.get_mut(name) {
            Some(cs) => cs,
            None => {
                return Ok(Self::json_error(
                    StatusCode::NOT_FOUND,
                    "NotFoundException",
                    &format!("Configuration set {} does not exist", name),
                ));
            }
        };

        cs.archive_arn = body["ArchiveArn"].as_str().map(|s| s.to_string());

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    // --- Event Destination operations ---

    pub(super) fn create_configuration_set_event_destination(
        &self,
        config_set_name: &str,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = Self::parse_body(req)?;

        let state_read = self.state.read();
        if !state_read.configuration_sets.contains_key(config_set_name) {
            return Ok(Self::json_error(
                StatusCode::NOT_FOUND,
                "NotFoundException",
                &format!("Configuration set {} does not exist", config_set_name),
            ));
        }
        drop(state_read);

        let dest_name = match body["EventDestinationName"].as_str() {
            Some(n) => n.to_string(),
            None => {
                return Ok(Self::json_error(
                    StatusCode::BAD_REQUEST,
                    "BadRequestException",
                    "EventDestinationName is required",
                ));
            }
        };

        let event_dest = parse_event_destination_definition(&dest_name, &body["EventDestination"]);

        let mut state = self.state.write();
        let dests = state
            .event_destinations
            .entry(config_set_name.to_string())
            .or_default();

        if dests.iter().any(|d| d.name == dest_name) {
            return Ok(Self::json_error(
                StatusCode::CONFLICT,
                "AlreadyExistsException",
                &format!("Event destination {} already exists", dest_name),
            ));
        }

        dests.push(event_dest);

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    pub(super) fn get_configuration_set_event_destinations(
        &self,
        config_set_name: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let state = self.state.read();

        if !state.configuration_sets.contains_key(config_set_name) {
            return Ok(Self::json_error(
                StatusCode::NOT_FOUND,
                "NotFoundException",
                &format!("Configuration set {} does not exist", config_set_name),
            ));
        }

        let dests = state
            .event_destinations
            .get(config_set_name)
            .cloned()
            .unwrap_or_default();

        let dests_json: Vec<Value> = dests.iter().map(event_destination_to_json).collect();

        let response = json!({
            "EventDestinations": dests_json,
        });

        Ok(AwsResponse::json(StatusCode::OK, response.to_string()))
    }

    pub(super) fn update_configuration_set_event_destination(
        &self,
        config_set_name: &str,
        dest_name: &str,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = Self::parse_body(req)?;

        let mut state = self.state.write();

        if !state.configuration_sets.contains_key(config_set_name) {
            return Ok(Self::json_error(
                StatusCode::NOT_FOUND,
                "NotFoundException",
                &format!("Configuration set {} does not exist", config_set_name),
            ));
        }

        let dests = state
            .event_destinations
            .entry(config_set_name.to_string())
            .or_default();

        let existing = match dests.iter_mut().find(|d| d.name == dest_name) {
            Some(d) => d,
            None => {
                return Ok(Self::json_error(
                    StatusCode::NOT_FOUND,
                    "NotFoundException",
                    &format!("Event destination {} does not exist", dest_name),
                ));
            }
        };

        let updated = parse_event_destination_definition(dest_name, &body["EventDestination"]);
        *existing = updated;

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    pub(super) fn delete_configuration_set_event_destination(
        &self,
        config_set_name: &str,
        dest_name: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut state = self.state.write();

        if !state.configuration_sets.contains_key(config_set_name) {
            return Ok(Self::json_error(
                StatusCode::NOT_FOUND,
                "NotFoundException",
                &format!("Configuration set {} does not exist", config_set_name),
            ));
        }

        let dests = state
            .event_destinations
            .entry(config_set_name.to_string())
            .or_default();

        let len_before = dests.len();
        dests.retain(|d| d.name != dest_name);

        if dests.len() == len_before {
            return Ok(Self::json_error(
                StatusCode::NOT_FOUND,
                "NotFoundException",
                &format!("Event destination {} does not exist", dest_name),
            ));
        }

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }
}
