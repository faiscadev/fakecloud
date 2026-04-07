use http::StatusCode;
use serde_json::{json, Value};

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};
use fakecloud_core::validation::*;

use super::dd_config_json;
use super::{body_json, require_str, LogsService};
use crate::state::{Delivery, DeliveryDestination, DeliverySource};

impl LogsService {
    // ---- Delivery Destinations ----

    pub(crate) fn put_delivery_destination(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        let name = body["name"]
            .as_str()
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterException",
                    "name is required",
                )
            })?
            .to_string();

        validate_string_length("name", &name, 1, 60)?;

        validate_optional_enum_value(
            "deliveryDestinationType",
            &body["deliveryDestinationType"],
            &["S3", "CWL", "FH", "XRAY"],
        )?;

        let output_format = body["outputFormat"].as_str().map(|s| s.to_string());

        // Validate output format
        if let Some(ref fmt) = output_format {
            let valid = ["json", "plain", "w3c", "raw", "parquet"];
            if !valid.contains(&fmt.as_str()) {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ValidationException",
                    format!("1 validation error detected: Value '{fmt}' at 'outputFormat' failed to satisfy constraint: Member must satisfy enum value set: [json, plain, w3c, raw, parquet]"),
                ));
            }
        }

        let config: std::collections::HashMap<String, String> = body
            ["deliveryDestinationConfiguration"]
            .as_object()
            .map(|m| {
                m.iter()
                    .map(|(k, v)| (k.clone(), v.as_str().unwrap_or("").to_string()))
                    .collect()
            })
            .unwrap_or_default();

        let tags: std::collections::HashMap<String, String> = body["tags"]
            .as_object()
            .map(|m| {
                m.iter()
                    .map(|(k, v)| (k.clone(), v.as_str().unwrap_or("").to_string()))
                    .collect()
            })
            .unwrap_or_default();

        let mut state = self.state.write();

        // Check if updating - cannot change output format
        if let Some(existing) = state.delivery_destinations.get(&name) {
            if let Some(ref new_fmt) = output_format {
                if let Some(ref existing_fmt) = existing.output_format {
                    if new_fmt != existing_fmt {
                        return Err(AwsServiceError::aws_error(
                            StatusCode::BAD_REQUEST,
                            "ValidationException",
                            "Cannot update outputFormat for an existing delivery destination.",
                        ));
                    }
                }
            }
        }

        let arn = format!(
            "arn:aws:logs:{}:{}:delivery-destination:{}",
            state.region, state.account_id, name
        );

        let existing_policy = state
            .delivery_destinations
            .get(&name)
            .and_then(|d| d.delivery_destination_policy.clone());

        let dd = DeliveryDestination {
            name: name.clone(),
            arn: arn.clone(),
            output_format: output_format.clone(),
            delivery_destination_configuration: config.clone(),
            tags: tags.clone(),
            delivery_destination_policy: existing_policy,
        };

        state.delivery_destinations.insert(name.clone(), dd);

        // Build the configuration object for the response, preserving existing fields
        // and always including destinationResourceArn (Smithy shape requires string, not null)
        let config_resp = {
            let mut c: serde_json::Map<String, Value> =
                config.iter().map(|(k, v)| (k.clone(), json!(v))).collect();
            c.entry("destinationResourceArn".to_string())
                .or_insert_with(|| json!(""));
            Value::Object(c)
        };

        let mut resp = json!({
            "deliveryDestination": {
                "name": name,
                "arn": arn,
                "deliveryDestinationConfiguration": config_resp,
            }
        });
        if let Some(ref fmt) = output_format {
            resp["deliveryDestination"]["outputFormat"] = json!(fmt);
        }
        if !tags.is_empty() {
            resp["deliveryDestination"]["tags"] = json!(tags);
        }

        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&resp).unwrap(),
        ))
    }

    pub(crate) fn get_delivery_destination(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        let name = body["name"].as_str().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                "name is required",
            )
        })?;

        validate_string_length("name", name, 1, 60)?;

        let state = self.state.read();
        let dd = state.delivery_destinations.get(name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("Delivery destination '{name}' does not exist."),
            )
        })?;

        let mut obj = json!({
            "name": dd.name,
            "arn": dd.arn,
            "deliveryDestinationConfiguration": dd_config_json(&dd.delivery_destination_configuration),
        });
        if let Some(ref fmt) = dd.output_format {
            obj["outputFormat"] = json!(fmt);
        }

        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&json!({ "deliveryDestination": obj })).unwrap(),
        ))
    }

    pub(crate) fn describe_delivery_destinations(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        validate_optional_range_i64("limit", body["limit"].as_i64(), 1, 50)?;
        validate_optional_string_length("nextToken", body["nextToken"].as_str(), 1, 2048)?;

        let state = self.state.read();
        let dds: Vec<Value> = state
            .delivery_destinations
            .values()
            .map(|dd| {
                let mut obj = json!({
                    "name": dd.name,
                    "arn": dd.arn,
                    "deliveryDestinationConfiguration": dd_config_json(&dd.delivery_destination_configuration),
                });
                if let Some(ref fmt) = dd.output_format {
                    obj["outputFormat"] = json!(fmt);
                }
                obj
            })
            .collect();

        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&json!({ "deliveryDestinations": dds })).unwrap(),
        ))
    }

    pub(crate) fn delete_delivery_destination(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        let name = body["name"].as_str().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                "name is required",
            )
        })?;

        validate_string_length("name", name, 1, 60)?;

        let mut state = self.state.write();
        if state.delivery_destinations.remove(name).is_none() {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("Delivery destination '{name}' does not exist."),
            ));
        }

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    pub(crate) fn put_delivery_destination_policy(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        let name = body["deliveryDestinationName"].as_str().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                "deliveryDestinationName is required",
            )
        })?;
        let policy = body["deliveryDestinationPolicy"]
            .as_str()
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterException",
                    "deliveryDestinationPolicy is required",
                )
            })?
            .to_string();

        validate_string_length("deliveryDestinationName", name, 1, 60)?;
        validate_string_length("deliveryDestinationPolicy", &policy, 1, 51200)?;

        let mut state = self.state.write();
        let dd = state.delivery_destinations.get_mut(name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("Delivery destination '{name}' does not exist."),
            )
        })?;

        dd.delivery_destination_policy = Some(policy.clone());

        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&json!({
                "policy": {
                    "deliveryDestinationPolicy": policy,
                }
            }))
            .unwrap(),
        ))
    }

    pub(crate) fn get_delivery_destination_policy(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        let name = body["deliveryDestinationName"].as_str().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                "deliveryDestinationName is required",
            )
        })?;

        validate_string_length("deliveryDestinationName", name, 1, 60)?;

        let state = self.state.read();
        let dd = state.delivery_destinations.get(name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("Delivery destination '{name}' does not exist."),
            )
        })?;

        let policy_json = if let Some(ref policy) = dd.delivery_destination_policy {
            json!({
                "deliveryDestinationPolicy": policy,
            })
        } else {
            json!({})
        };

        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&json!({
                "policy": policy_json,
            }))
            .unwrap(),
        ))
    }

    pub(crate) fn delete_delivery_destination_policy(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        let name = body["deliveryDestinationName"].as_str().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                "deliveryDestinationName is required",
            )
        })?;

        validate_string_length("deliveryDestinationName", name, 1, 60)?;

        let mut state = self.state.write();
        let dd = state.delivery_destinations.get_mut(name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("Delivery destination '{name}' does not exist."),
            )
        })?;

        dd.delivery_destination_policy = None;

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    // ---- Delivery Sources ----

    pub(crate) fn put_delivery_source(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        let name = body["name"]
            .as_str()
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterException",
                    "name is required",
                )
            })?
            .to_string();
        let resource_arn = body["resourceArn"]
            .as_str()
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterException",
                    "resourceArn is required",
                )
            })?
            .to_string();
        let log_type = body["logType"]
            .as_str()
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterException",
                    "logType is required",
                )
            })?
            .to_string();

        validate_string_length("name", &name, 1, 60)?;
        validate_string_length("logType", &log_type, 1, 255)?;

        let tags: std::collections::HashMap<String, String> = body["tags"]
            .as_object()
            .map(|m| {
                m.iter()
                    .map(|(k, v)| (k.clone(), v.as_str().unwrap_or("").to_string()))
                    .collect()
            })
            .unwrap_or_default();

        // Extract service from ARN
        let service = resource_arn
            .split(':')
            .nth(2)
            .unwrap_or("unknown")
            .to_string();

        // Validate resource ARN format - must start with arn:aws:
        if !resource_arn.starts_with("arn:aws:") {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                format!("Invalid resource ARN: {resource_arn}"),
            ));
        }

        // S3 cannot be a delivery source
        if service == "s3" {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("The resource ARN '{resource_arn}' is not a valid delivery source."),
            ));
        }

        // Validate log type based on service
        let valid_log_types: &[&str] = match service.as_str() {
            "cloudfront" => &["ACCESS_LOGS"],
            _ => &["ACCESS_LOGS", "APPLICATION_LOGS", "FW_LOGS"],
        };
        if !valid_log_types.contains(&log_type.as_str()) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                format!("Log type '{log_type}' is not valid for this resource."),
            ));
        }

        let mut state = self.state.write();

        // Cannot update with different resourceArn
        if let Some(existing) = state.delivery_sources.get(&name) {
            if !existing.resource_arns.is_empty() && existing.resource_arns[0] != resource_arn {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ConflictException",
                    "Cannot update delivery source with a different resourceArn.",
                ));
            }
        }

        let arn = format!(
            "arn:aws:logs:{}:{}:delivery-source:{}",
            state.region, state.account_id, name
        );

        let ds = DeliverySource {
            name: name.clone(),
            arn: arn.clone(),
            resource_arns: vec![resource_arn],
            service: service.clone(),
            log_type: log_type.clone(),
            tags: tags.clone(),
        };

        state.delivery_sources.insert(name.clone(), ds);

        let state_ref = state.delivery_sources.get(&name).unwrap();

        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&json!({
                "deliverySource": {
                    "name": state_ref.name,
                    "arn": state_ref.arn,
                    "resourceArns": state_ref.resource_arns,
                    "service": state_ref.service,
                    "logType": state_ref.log_type,
                    "tags": state_ref.tags,
                }
            }))
            .unwrap(),
        ))
    }

    pub(crate) fn get_delivery_source(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        let name = body["name"].as_str().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                "name is required",
            )
        })?;

        validate_string_length("name", name, 1, 60)?;

        let state = self.state.read();
        let ds = state.delivery_sources.get(name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("Delivery source '{name}' does not exist."),
            )
        })?;

        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&json!({
                "deliverySource": {
                    "name": ds.name,
                    "arn": ds.arn,
                    "resourceArns": ds.resource_arns,
                    "service": ds.service,
                    "logType": ds.log_type,
                    "tags": ds.tags,
                }
            }))
            .unwrap(),
        ))
    }

    pub(crate) fn describe_delivery_sources(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        validate_optional_range_i64("limit", body["limit"].as_i64(), 1, 50)?;
        validate_optional_string_length("nextToken", body["nextToken"].as_str(), 1, 2048)?;

        let state = self.state.read();
        let sources: Vec<Value> = state
            .delivery_sources
            .values()
            .map(|ds| {
                json!({
                    "name": ds.name,
                    "arn": ds.arn,
                    "resourceArns": ds.resource_arns,
                    "service": ds.service,
                    "logType": ds.log_type,
                    "tags": ds.tags,
                })
            })
            .collect();

        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&json!({ "deliverySources": sources })).unwrap(),
        ))
    }

    pub(crate) fn delete_delivery_source(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        let name = body["name"].as_str().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                "name is required",
            )
        })?;

        validate_string_length("name", name, 1, 60)?;

        let mut state = self.state.write();
        if state.delivery_sources.remove(name).is_none() {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("Delivery source '{name}' does not exist."),
            ));
        }

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    // ---- Deliveries ----

    pub(crate) fn create_delivery(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        let delivery_source_name = body["deliverySourceName"]
            .as_str()
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterException",
                    "deliverySourceName is required",
                )
            })?
            .to_string();
        let delivery_destination_arn = body["deliveryDestinationArn"]
            .as_str()
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterException",
                    "deliveryDestinationArn is required",
                )
            })?
            .to_string();

        validate_string_length("deliverySourceName", &delivery_source_name, 1, 60)?;
        validate_optional_string_length("fieldDelimiter", body["fieldDelimiter"].as_str(), 0, 5)?;

        let tags: std::collections::HashMap<String, String> = body["tags"]
            .as_object()
            .map(|m| {
                m.iter()
                    .map(|(k, v)| (k.clone(), v.as_str().unwrap_or("").to_string()))
                    .collect()
            })
            .unwrap_or_default();

        let record_fields: Vec<String> = body["recordFields"]
            .as_array()
            .map(|a| {
                a.iter()
                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                    .collect()
            })
            .unwrap_or_default();
        let field_delimiter = body["fieldDelimiter"].as_str().map(|s| s.to_string());
        let s3_delivery_config = body["s3DeliveryConfiguration"].clone();

        let mut state = self.state.write();

        // Verify source exists
        if !state.delivery_sources.contains_key(&delivery_source_name) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("Delivery source '{}' does not exist.", delivery_source_name),
            ));
        }

        // Verify destination exists
        let dest_exists = state
            .delivery_destinations
            .values()
            .any(|dd| dd.arn == delivery_destination_arn);
        if !dest_exists {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!(
                    "Delivery destination '{}' does not exist.",
                    delivery_destination_arn
                ),
            ));
        }

        // Check for duplicate delivery (same source + destination)
        let already_exists = state.deliveries.values().any(|d| {
            d.delivery_source_name == delivery_source_name
                && d.delivery_destination_arn == delivery_destination_arn
        });
        if already_exists {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ConflictException",
                "A delivery already exists for this source and destination.",
            ));
        }

        // Determine destination type from ARN
        let dest_type = if delivery_destination_arn.contains(":s3:") {
            "S3"
        } else if delivery_destination_arn.contains(":firehose:") {
            "FH"
        } else {
            "CWL"
        };

        let delivery_id = uuid::Uuid::new_v4().to_string();
        let arn = format!(
            "arn:aws:logs:{}:{}:delivery:{}",
            state.region, state.account_id, delivery_id
        );

        let delivery = Delivery {
            id: delivery_id.clone(),
            delivery_source_name: delivery_source_name.clone(),
            delivery_destination_arn: delivery_destination_arn.clone(),
            delivery_destination_type: dest_type.to_string(),
            arn: arn.clone(),
            tags: tags.clone(),
        };

        state.deliveries.insert(delivery_id.clone(), delivery);

        let mut delivery_json = json!({
            "id": delivery_id,
            "deliverySourceName": delivery_source_name,
            "deliveryDestinationArn": delivery_destination_arn,
            "deliveryDestinationType": dest_type,
            "arn": arn,
            "tags": tags,
        });
        if !record_fields.is_empty() {
            delivery_json["recordFields"] = json!(record_fields);
        }
        if let Some(ref delim) = field_delimiter {
            delivery_json["fieldDelimiter"] = json!(delim);
        }
        if !s3_delivery_config.is_null() {
            delivery_json["s3DeliveryConfiguration"] = s3_delivery_config;
        }

        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&json!({
                "delivery": delivery_json,
            }))
            .unwrap(),
        ))
    }

    pub(crate) fn get_delivery(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        let delivery_id = body["id"].as_str().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                "id is required",
            )
        })?;

        validate_string_length("id", delivery_id, 1, 64)?;

        let state = self.state.read();
        let d = state.deliveries.get(delivery_id).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("Delivery '{delivery_id}' does not exist."),
            )
        })?;

        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&json!({
                "delivery": {
                    "id": d.id,
                    "deliverySourceName": d.delivery_source_name,
                    "deliveryDestinationArn": d.delivery_destination_arn,
                    "deliveryDestinationType": d.delivery_destination_type,
                    "arn": d.arn,
                    "tags": d.tags,
                }
            }))
            .unwrap(),
        ))
    }

    pub(crate) fn describe_deliveries(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        validate_optional_range_i64("limit", body["limit"].as_i64(), 1, 50)?;
        validate_optional_string_length("nextToken", body["nextToken"].as_str(), 1, 2048)?;

        let state = self.state.read();
        let deliveries: Vec<Value> = state
            .deliveries
            .values()
            .map(|d| {
                json!({
                    "id": d.id,
                    "deliverySourceName": d.delivery_source_name,
                    "deliveryDestinationArn": d.delivery_destination_arn,
                    "deliveryDestinationType": d.delivery_destination_type,
                    "arn": d.arn,
                    "tags": d.tags,
                })
            })
            .collect();

        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&json!({ "deliveries": deliveries })).unwrap(),
        ))
    }

    pub(crate) fn delete_delivery(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        let delivery_id = body["id"].as_str().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                "id is required",
            )
        })?;

        validate_string_length("id", delivery_id, 1, 64)?;

        let mut state = self.state.write();
        if state.deliveries.remove(delivery_id).is_none() {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("Delivery '{delivery_id}' does not exist."),
            ));
        }

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    pub(crate) fn update_delivery_configuration(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        let id = require_str(&body, "id")?;

        let state = self.state.read();
        if !state.deliveries.contains_key(id) {
            return Err(AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "ResourceNotFoundException",
                format!("Delivery not found: {id}"),
            ));
        }
        drop(state);

        // No-op: delivery configuration update is accepted but not stored
        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    pub(crate) fn describe_configuration_templates(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        validate_optional_string_length("service", body["service"].as_str(), 1, 255)?;
        validate_optional_string_length("nextToken", body["nextToken"].as_str(), 1, 4096)?;
        validate_optional_range_i64("limit", body["limit"].as_i64(), 1, 50)?;
        // Stub: return empty configuration templates
        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&json!({ "configurationTemplates": [] })).unwrap(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use crate::service::extract_log_group_from_arn;
    use crate::service::test_helpers::*;
    use serde_json::{json, Value};

    // ---- extract_log_group_from_arn ----

    #[test]
    fn put_delivery_destination_includes_empty_destination_resource_arn() {
        let svc = make_service();
        let req = make_request(
            "PutDeliveryDestination",
            json!({
                "name": "my-dest",
                "deliveryDestinationConfiguration": {}
            }),
        );
        let resp = svc.put_delivery_destination(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        let config = &body["deliveryDestination"]["deliveryDestinationConfiguration"];
        // destinationResourceArn should always be present as a string (Smithy requirement)
        assert_eq!(
            config["destinationResourceArn"].as_str().unwrap(),
            "",
            "destinationResourceArn should be an empty string when not set"
        );
    }

    #[test]
    fn put_delivery_destination_includes_destination_resource_arn_when_set() {
        let svc = make_service();
        let req = make_request(
            "PutDeliveryDestination",
            json!({
                "name": "my-dest",
                "deliveryDestinationConfiguration": {
                    "destinationResourceArn": "arn:aws:s3:::my-bucket"
                }
            }),
        );
        let resp = svc.put_delivery_destination(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        let config = &body["deliveryDestination"]["deliveryDestinationConfiguration"];
        assert_eq!(
            config["destinationResourceArn"].as_str().unwrap(),
            "arn:aws:s3:::my-bucket"
        );
    }

    #[test]
    fn extract_log_group_from_arn_strips_wildcard_suffix() {
        let arn = "arn:aws:logs:us-east-1:123456789012:log-group:my-group:*";
        assert_eq!(
            extract_log_group_from_arn(arn),
            Some("my-group".to_string())
        );
    }

    #[test]
    fn extract_log_group_from_arn_without_wildcard() {
        let arn = "arn:aws:logs:us-east-1:123456789012:log-group:my-group";
        assert_eq!(
            extract_log_group_from_arn(arn),
            Some("my-group".to_string())
        );
    }

    #[test]
    fn extract_log_group_from_arn_invalid() {
        assert_eq!(extract_log_group_from_arn("not-an-arn"), None);
    }

    // ---- Delivery pipeline tests ----

    #[test]
    fn logs_delivery_pipeline_writes_to_storage() {
        let svc = make_service();
        create_group(&svc, "/delivery/test");
        create_stream(&svc, "/delivery/test", "stream-1");

        // Get the log group ARN
        let req = make_request(
            "DescribeLogGroups",
            json!({ "logGroupNamePrefix": "/delivery/test" }),
        );
        let resp = svc.describe_log_groups(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        let group_arn = body["logGroups"][0]["arn"].as_str().unwrap().to_string();

        // Create delivery source referencing this log group
        let req = make_request(
            "PutDeliverySource",
            json!({
                "name": "test-source",
                "resourceArn": group_arn,
                "logType": "APPLICATION_LOGS",
            }),
        );
        svc.put_delivery_source(&req).unwrap();

        // Create delivery destination targeting an S3 bucket
        let req = make_request(
            "PutDeliveryDestination",
            json!({
                "name": "test-dest",
                "deliveryDestinationConfiguration": {
                    "destinationResourceArn": "arn:aws:s3:::delivery-test-bucket"
                }
            }),
        );
        svc.put_delivery_destination(&req).unwrap();

        // Get the destination ARN
        let req = make_request("GetDeliveryDestination", json!({ "name": "test-dest" }));
        let resp = svc.get_delivery_destination(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        let dest_arn = body["deliveryDestination"]["arn"]
            .as_str()
            .unwrap()
            .to_string();

        // Create delivery linking source to destination
        let req = make_request(
            "CreateDelivery",
            json!({
                "deliverySourceName": "test-source",
                "deliveryDestinationArn": dest_arn,
            }),
        );
        svc.create_delivery(&req).unwrap();

        // Now put log events — they should be forwarded to export storage
        let now = chrono::Utc::now().timestamp_millis();
        let req = make_request(
            "PutLogEvents",
            json!({
                "logGroupName": "/delivery/test",
                "logStreamName": "stream-1",
                "logEvents": [
                    { "timestamp": now, "message": "delivered event 1" },
                    { "timestamp": now + 1, "message": "delivered event 2" },
                ],
            }),
        );
        svc.put_log_events(&req).unwrap();

        // Verify data was written to export storage under the S3 bucket path
        let req = make_request(
            "GetExportedData",
            json!({ "keyPrefix": "delivery-test-bucket/delivery" }),
        );
        let resp = svc.get_exported_data(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        let entries = body["entries"].as_array().unwrap();
        assert!(!entries.is_empty(), "Should have delivery data");
        let data = entries[0]["data"].as_str().unwrap();
        assert!(data.contains("delivered event 1"));
        assert!(data.contains("delivered event 2"));
    }

    // ---- Delivery sources CRUD ----

    #[test]
    fn delivery_source_lifecycle() {
        let svc = make_service();
        create_group(&svc, "ds-grp");

        let req = make_request(
            "DescribeLogGroups",
            json!({ "logGroupNamePrefix": "ds-grp" }),
        );
        let resp = svc.describe_log_groups(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        let group_arn = body["logGroups"][0]["arn"].as_str().unwrap().to_string();

        // Put
        let req = make_request(
            "PutDeliverySource",
            json!({
                "name": "src1",
                "resourceArn": group_arn,
                "logType": "APPLICATION_LOGS",
            }),
        );
        svc.put_delivery_source(&req).unwrap();

        // Get
        let req = make_request("GetDeliverySource", json!({ "name": "src1" }));
        let resp = svc.get_delivery_source(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["deliverySource"]["name"], "src1");
        assert_eq!(body["deliverySource"]["logType"], "APPLICATION_LOGS");

        // Describe
        let req = make_request("DescribeDeliverySources", json!({}));
        let resp = svc.describe_delivery_sources(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["deliverySources"].as_array().unwrap().len(), 1);

        // Delete
        let req = make_request("DeleteDeliverySource", json!({ "name": "src1" }));
        svc.delete_delivery_source(&req).unwrap();

        let req = make_request("DescribeDeliverySources", json!({}));
        let resp = svc.describe_delivery_sources(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert!(body["deliverySources"].as_array().unwrap().is_empty());
    }

    // ---- Delivery destinations CRUD ----

    #[test]
    fn delivery_destination_lifecycle() {
        let svc = make_service();

        let req = make_request(
            "PutDeliveryDestination",
            json!({
                "name": "dd1",
                "deliveryDestinationConfiguration": {
                    "destinationResourceArn": "arn:aws:s3:::my-bucket"
                }
            }),
        );
        svc.put_delivery_destination(&req).unwrap();

        // Get
        let req = make_request("GetDeliveryDestination", json!({ "name": "dd1" }));
        let resp = svc.get_delivery_destination(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["deliveryDestination"]["name"], "dd1");
        let arn = body["deliveryDestination"]["arn"]
            .as_str()
            .unwrap()
            .to_string();
        assert!(!arn.is_empty());

        // Describe
        let req = make_request("DescribeDeliveryDestinations", json!({}));
        let resp = svc.describe_delivery_destinations(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["deliveryDestinations"].as_array().unwrap().len(), 1);

        // Delete
        let req = make_request("DeleteDeliveryDestination", json!({ "name": "dd1" }));
        svc.delete_delivery_destination(&req).unwrap();

        let req = make_request("DescribeDeliveryDestinations", json!({}));
        let resp = svc.describe_delivery_destinations(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert!(body["deliveryDestinations"].as_array().unwrap().is_empty());
    }

    // ---- Delivery (full pipeline CRUD) ----

    #[test]
    fn delivery_crud_lifecycle() {
        let svc = make_service();
        create_group(&svc, "del-grp");

        let req = make_request(
            "DescribeLogGroups",
            json!({ "logGroupNamePrefix": "del-grp" }),
        );
        let resp = svc.describe_log_groups(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        let group_arn = body["logGroups"][0]["arn"].as_str().unwrap().to_string();

        // Source
        let req = make_request(
            "PutDeliverySource",
            json!({
                "name": "del-src",
                "resourceArn": group_arn,
                "logType": "APPLICATION_LOGS",
            }),
        );
        svc.put_delivery_source(&req).unwrap();

        // Destination
        let req = make_request(
            "PutDeliveryDestination",
            json!({
                "name": "del-dest",
                "deliveryDestinationConfiguration": {
                    "destinationResourceArn": "arn:aws:s3:::del-bucket"
                }
            }),
        );
        svc.put_delivery_destination(&req).unwrap();

        let req = make_request("GetDeliveryDestination", json!({ "name": "del-dest" }));
        let resp = svc.get_delivery_destination(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        let dest_arn = body["deliveryDestination"]["arn"]
            .as_str()
            .unwrap()
            .to_string();

        // Create delivery
        let req = make_request(
            "CreateDelivery",
            json!({
                "deliverySourceName": "del-src",
                "deliveryDestinationArn": dest_arn,
            }),
        );
        let resp = svc.create_delivery(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        let delivery_id = body["delivery"]["id"].as_str().unwrap().to_string();

        // Get delivery
        let req = make_request("GetDelivery", json!({ "id": delivery_id }));
        let resp = svc.get_delivery(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["delivery"]["deliverySourceName"], "del-src");

        // Describe deliveries
        let req = make_request("DescribeDeliveries", json!({}));
        let resp = svc.describe_deliveries(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["deliveries"].as_array().unwrap().len(), 1);

        // Delete delivery
        let req = make_request("DeleteDelivery", json!({ "id": delivery_id }));
        svc.delete_delivery(&req).unwrap();

        let req = make_request("DescribeDeliveries", json!({}));
        let resp = svc.describe_deliveries(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert!(body["deliveries"].as_array().unwrap().is_empty());
    }
}
