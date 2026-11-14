use http::StatusCode;
use serde_json::{json, Value};

use crate::validation::*;
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use super::{dd_config_json, infer_delivery_destination_type};
use super::{require_str, LogsService};
use crate::state::{Delivery, DeliveryDestination, DeliverySource};

/// Page a key-ordered listing for the `Describe*` delivery operations:
/// `nextToken` is the last key returned, `limit` defaults to 50.
fn page_by_key<'a, T: 'a>(
    items: impl Iterator<Item = (&'a String, &'a T)>,
    body: &Value,
) -> (Vec<&'a T>, Option<String>) {
    let limit = body["limit"].as_i64().unwrap_or(50).max(1) as usize;
    let token = body["nextToken"].as_str();
    let mut rest = items.filter(|(k, _)| token.is_none_or(|t| k.as_str() > t));
    let mut page = Vec::new();
    let mut last_key = None;
    for (k, v) in rest.by_ref().take(limit) {
        page.push(v);
        last_key = Some(k.clone());
    }
    let next = if rest.next().is_some() {
        last_key
    } else {
        None
    };
    (page, next)
}

/// Wrap a described page as `{ <field>: [...], nextToken? }`.
fn describe_page(field: &str, items: Vec<Value>, next: Option<String>) -> AwsResponse {
    let mut out = json!({ field: items });
    if let Some(token) = next {
        out["nextToken"] = json!(token);
    }
    AwsResponse::json(StatusCode::OK, serde_json::to_string(&out).unwrap())
}

/// Render a stored `Delivery` to the wire `delivery` shape, echoing every
/// persisted optional member (`recordFields` / `fieldDelimiter` /
/// `s3DeliveryConfiguration`) so `GetDelivery` and `DescribeDeliveries` agree.
fn delivery_to_json(d: &Delivery) -> Value {
    let mut delivery_json = json!({
        "id": d.id,
        "deliverySourceName": d.delivery_source_name,
        "deliveryDestinationArn": d.delivery_destination_arn,
        "deliveryDestinationType": d.delivery_destination_type,
        "arn": d.arn,
        "tags": d.tags,
    });
    if !d.record_fields.is_empty() {
        delivery_json["recordFields"] = json!(d.record_fields);
    }
    if let Some(ref delim) = d.field_delimiter {
        delivery_json["fieldDelimiter"] = json!(delim);
    }
    if let Some(ref s3) = d.s3_delivery_configuration {
        delivery_json["s3DeliveryConfiguration"] = s3.clone();
    }
    delivery_json
}

/// Render a stored `DeliveryDestination` to the wire shape shared by
/// `PutDeliveryDestination`, `GetDeliveryDestination` and
/// `DescribeDeliveryDestinations`.
fn delivery_destination_to_json(dd: &DeliveryDestination) -> Value {
    let mut obj = json!({
        "name": dd.name,
        "arn": dd.arn,
        "deliveryDestinationType": dd.delivery_destination_type,
        "deliveryDestinationConfiguration": dd_config_json(&dd.delivery_destination_configuration),
    });
    if let Some(ref fmt) = dd.output_format {
        obj["outputFormat"] = json!(fmt);
    }
    if !dd.tags.is_empty() {
        obj["tags"] = json!(dd.tags);
    }
    if let Some(ref role_arn) = dd.role_arn {
        obj["roleArn"] = json!(role_arn);
    }
    obj
}

impl LogsService {
    // ---- Delivery Destinations ----

    pub(crate) fn put_delivery_destination(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
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

        let config: std::collections::BTreeMap<String, String> = body
            ["deliveryDestinationConfiguration"]
            .as_object()
            .map(|m| {
                m.iter()
                    .map(|(k, v)| (k.clone(), v.as_str().unwrap_or("").to_string()))
                    .collect()
            })
            .unwrap_or_default();

        let role_arn = body["roleArn"].as_str().map(str::to_string);

        let tags: std::collections::BTreeMap<String, String> = body["tags"]
            .as_object()
            .map(|m| {
                m.iter()
                    .map(|(k, v)| (k.clone(), v.as_str().unwrap_or("").to_string()))
                    .collect()
            })
            .unwrap_or_default();

        let mut accounts = self.state.write();
        let state = accounts.regional_mut(&req.account_id, &req.region);

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
            "arn:{}:logs:{}:{}:delivery-destination:{}",
            fakecloud_aws::arn::partition_for(&req.region),
            req.region,
            state.account_id,
            name
        );

        // The destination type is either supplied explicitly or inferred from
        // the destination resource ARN's service: CloudWatch Logs -> CWL,
        // S3 -> S3, Firehose -> FH, X-Ray -> XRAY.
        let delivery_destination_type = body["deliveryDestinationType"]
            .as_str()
            .map(|s| s.to_string())
            .unwrap_or_else(|| {
                infer_delivery_destination_type(config.get("destinationResourceArn"))
            });

        let existing_policy = state
            .delivery_destinations
            .get(&name)
            .and_then(|d| d.delivery_destination_policy.clone());

        let dd = DeliveryDestination {
            name: name.clone(),
            arn,
            output_format,
            delivery_destination_configuration: config,
            delivery_destination_type,
            tags,
            delivery_destination_policy: existing_policy,
            role_arn,
        };
        let resp = json!({ "deliveryDestination": delivery_destination_to_json(&dd) });
        state.delivery_destinations.insert(name, dd);

        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&resp).unwrap(),
        ))
    }

    pub(crate) fn get_delivery_destination(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let name = body["name"].as_str().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                "name is required",
            )
        })?;

        validate_string_length("name", name, 1, 60)?;

        let accounts = self.state.read();
        let empty = crate::state::LogsState::new(&req.account_id, &req.region);
        let state = accounts
            .regional(&req.account_id, &req.region)
            .unwrap_or(&empty);
        let dd = state.delivery_destinations.get(name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("Delivery destination '{name}' does not exist."),
            )
        })?;

        let obj = delivery_destination_to_json(dd);

        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&json!({ "deliveryDestination": obj })).unwrap(),
        ))
    }

    pub(crate) fn describe_delivery_destinations(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        validate_optional_range_i64("limit", body["limit"].as_i64(), 1, 50)?;
        validate_optional_string_length("nextToken", body["nextToken"].as_str(), 1, 2048)?;
        let names = optional_string_list(&body, "names", 1, 20)?;

        let accounts = self.state.read();
        let empty = crate::state::LogsState::new(&req.account_id, &req.region);
        let state = accounts
            .regional(&req.account_id, &req.region)
            .unwrap_or(&empty);
        let (page, next) = page_by_key(
            state
                .delivery_destinations
                .iter()
                .filter(|(name, _)| names.as_ref().is_none_or(|n| n.contains(name))),
            &body,
        );
        let dds = page.into_iter().map(delivery_destination_to_json).collect();
        Ok(describe_page("deliveryDestinations", dds, next))
    }

    pub(crate) fn delete_delivery_destination(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let name = body["name"].as_str().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                "name is required",
            )
        })?;

        validate_string_length("name", name, 1, 60)?;

        let mut accounts = self.state.write();
        let state = accounts.regional_mut(&req.account_id, &req.region);
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
        let body = req.json_body();
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

        let mut accounts = self.state.write();
        let state = accounts.regional_mut(&req.account_id, &req.region);
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
        let body = req.json_body();
        let name = body["deliveryDestinationName"].as_str().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                "deliveryDestinationName is required",
            )
        })?;

        validate_string_length("deliveryDestinationName", name, 1, 60)?;

        let accounts = self.state.read();
        let empty = crate::state::LogsState::new(&req.account_id, &req.region);
        let state = accounts
            .regional(&req.account_id, &req.region)
            .unwrap_or(&empty);
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
        let body = req.json_body();
        let name = body["deliveryDestinationName"].as_str().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                "deliveryDestinationName is required",
            )
        })?;

        validate_string_length("deliveryDestinationName", name, 1, 60)?;

        let mut accounts = self.state.write();
        let state = accounts.regional_mut(&req.account_id, &req.region);
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
        let body = req.json_body();
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

        let tags: std::collections::BTreeMap<String, String> = body["tags"]
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

        // Validate resource ARN format - must be an ARN of `service`
        if fakecloud_aws::arn::arn_resource(&resource_arn, &service).is_none() {
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

        let mut accounts = self.state.write();
        let state = accounts.regional_mut(&req.account_id, &req.region);

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
            "arn:{}:logs:{}:{}:delivery-source:{}",
            fakecloud_aws::arn::partition_for(&req.region),
            req.region,
            state.account_id,
            name
        );

        let created_at = state
            .delivery_sources
            .get(&name)
            .map(|d| d.created_at)
            .filter(|t| *t > 0)
            .unwrap_or_else(|| chrono::Utc::now().timestamp_millis());

        let ds = DeliverySource {
            name: name.clone(),
            arn: arn.clone(),
            resource_arns: vec![resource_arn],
            service: service.clone(),
            log_type: log_type.clone(),
            tags: tags.clone(),
            created_at,
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
        let body = req.json_body();
        let name = body["name"].as_str().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                "name is required",
            )
        })?;

        validate_string_length("name", name, 1, 60)?;

        let accounts = self.state.read();
        let empty = crate::state::LogsState::new(&req.account_id, &req.region);
        let state = accounts
            .regional(&req.account_id, &req.region)
            .unwrap_or(&empty);
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
        let body = req.json_body();
        validate_optional_range_i64("limit", body["limit"].as_i64(), 1, 50)?;
        validate_optional_string_length("nextToken", body["nextToken"].as_str(), 1, 2048)?;
        let resource_arns = optional_string_list(&body, "resourceArns", 1, 20)?;

        let accounts = self.state.read();
        let empty = crate::state::LogsState::new(&req.account_id, &req.region);
        let state = accounts
            .regional(&req.account_id, &req.region)
            .unwrap_or(&empty);
        let (page, next) = page_by_key(
            state.delivery_sources.iter().filter(|(_, ds)| {
                resource_arns
                    .as_ref()
                    .is_none_or(|arns| ds.resource_arns.iter().any(|a| arns.contains(a)))
            }),
            &body,
        );
        let sources: Vec<Value> = page
            .into_iter()
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
        Ok(describe_page("deliverySources", sources, next))
    }

    pub(crate) fn delete_delivery_source(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let name = body["name"].as_str().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                "name is required",
            )
        })?;

        validate_string_length("name", name, 1, 60)?;

        let mut accounts = self.state.write();
        let state = accounts.regional_mut(&req.account_id, &req.region);
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
        let body = req.json_body();
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

        let tags: std::collections::BTreeMap<String, String> = body["tags"]
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

        let mut accounts = self.state.write();
        let state = accounts.regional_mut(&req.account_id, &req.region);

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

        // Determine destination type from the actual resource ARN stored in the destination
        let dest_type = state
            .delivery_destinations
            .values()
            .find(|dd| dd.arn == delivery_destination_arn)
            .and_then(|dd| {
                dd.delivery_destination_configuration
                    .get("destinationResourceArn")
            })
            .map(|dest_arn: &String| {
                if dest_arn.contains(":s3:") {
                    "S3"
                } else if dest_arn.contains(":firehose:") {
                    "FH"
                } else {
                    "CWL"
                }
            })
            .unwrap_or("CWL");

        let delivery_id = uuid::Uuid::new_v4().to_string();
        let arn = format!(
            "arn:{}:logs:{}:{}:delivery:{}",
            fakecloud_aws::arn::partition_for(&req.region),
            req.region,
            state.account_id,
            delivery_id
        );

        let delivery = Delivery {
            id: delivery_id.clone(),
            delivery_source_name: delivery_source_name.clone(),
            delivery_destination_arn: delivery_destination_arn.clone(),
            delivery_destination_type: dest_type.to_string(),
            arn: arn.clone(),
            tags,
            field_delimiter,
            record_fields,
            s3_delivery_configuration: if s3_delivery_config.is_null() {
                None
            } else {
                Some(s3_delivery_config)
            },
            created_at: chrono::Utc::now().timestamp_millis(),
        };

        let delivery_json = delivery_to_json(&delivery);
        state.deliveries.insert(delivery_id.clone(), delivery);

        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&json!({
                "delivery": delivery_json,
            }))
            .unwrap(),
        ))
    }

    pub(crate) fn get_delivery(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let delivery_id = body["id"].as_str().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                "id is required",
            )
        })?;

        validate_string_length("id", delivery_id, 1, 64)?;

        let accounts = self.state.read();
        let empty = crate::state::LogsState::new(&req.account_id, &req.region);
        let state = accounts
            .regional(&req.account_id, &req.region)
            .unwrap_or(&empty);
        let d = state.deliveries.get(delivery_id).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("Delivery '{delivery_id}' does not exist."),
            )
        })?;

        let delivery_json = delivery_to_json(d);
        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&json!({ "delivery": delivery_json })).unwrap(),
        ))
    }

    pub(crate) fn describe_deliveries(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        validate_optional_range_i64("limit", body["limit"].as_i64(), 1, 50)?;
        validate_optional_string_length("nextToken", body["nextToken"].as_str(), 1, 2048)?;
        let source_names = optional_string_list(&body, "deliverySourceNames", 1, 20)?;

        let accounts = self.state.read();
        let empty = crate::state::LogsState::new(&req.account_id, &req.region);
        let state = accounts
            .regional(&req.account_id, &req.region)
            .unwrap_or(&empty);
        let (page, next) = page_by_key(
            state.deliveries.iter().filter(|(_, d)| {
                source_names
                    .as_ref()
                    .is_none_or(|n| n.contains(&d.delivery_source_name))
            }),
            &body,
        );
        let deliveries = page.into_iter().map(delivery_to_json).collect();
        Ok(describe_page("deliveries", deliveries, next))
    }

    pub(crate) fn delete_delivery(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let delivery_id = body["id"].as_str().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                "id is required",
            )
        })?;

        validate_string_length("id", delivery_id, 1, 64)?;

        let mut accounts = self.state.write();
        let state = accounts.regional_mut(&req.account_id, &req.region);
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
        let body = req.json_body();
        let id = require_str(&body, "id")?.to_string();

        let mut accounts = self.state.write();
        let state = accounts.regional_mut(&req.account_id, &req.region);
        let delivery = state.deliveries.get_mut(&id).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "ResourceNotFoundException",
                format!("Delivery not found: {id}"),
            )
        })?;

        if let Some(delim) = body["fieldDelimiter"].as_str() {
            delivery.field_delimiter = Some(delim.to_string());
        }
        if let Some(arr) = body["recordFields"].as_array() {
            delivery.record_fields = arr
                .iter()
                .filter_map(|v| v.as_str().map(String::from))
                .collect();
        }
        if !body["s3DeliveryConfiguration"].is_null() {
            delivery.s3_delivery_configuration = Some(body["s3DeliveryConfiguration"].clone());
        }

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    pub(crate) fn describe_configuration_templates(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        validate_optional_string_length("service", body["service"].as_str(), 1, 255)?;
        validate_optional_string_length("nextToken", body["nextToken"].as_str(), 1, 4096)?;
        validate_optional_range_i64("limit", body["limit"].as_i64(), 1, 50)?;
        let service_filter = body["service"].as_str();
        let log_type_filter = body["logTypes"].as_array();
        let dest_type_filter = body["resourceType"].as_str();

        let templates = standard_delivery_templates();
        let filtered: Vec<Value> = templates
            .into_iter()
            .filter(|t| {
                service_filter.is_none_or(|s| t["service"].as_str() == Some(s))
                    && dest_type_filter
                        .is_none_or(|d| t["deliveryDestinationType"].as_str() == Some(d))
                    && log_type_filter.is_none_or(|types| {
                        types.iter().any(|v| v.as_str() == t["logType"].as_str())
                    })
            })
            .collect();

        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&json!({ "configurationTemplates": filtered })).unwrap(),
        ))
    }
}

fn standard_delivery_templates() -> Vec<Value> {
    let services = [
        "AWS/APIGateway",
        "AWS/Bedrock",
        "AWS/Lambda",
        "AWS/CloudFront",
    ];
    let dest_types = ["S3", "CWL", "FH"];
    let mut out = Vec::new();
    for svc in services {
        for dt in dest_types {
            out.push(json!({
                "service": svc,
                "logType": "ACCESS_LOGS",
                "resourceType": "AWS::Logs::DeliverySource",
                "deliveryDestinationType": dt,
                "defaultDeliveryConfigValues": {
                    "fieldDelimiter": " ",
                    "recordFields": ["timestamp", "message"],
                },
                "allowedFields": [
                    {"name": "timestamp", "mandatory": true},
                    {"name": "message", "mandatory": false},
                ],
                "allowedOutputFormats": ["json", "plain", "w3c", "raw", "parquet"],
                "allowedActionForAllowVendedLogsDeliveryForResource": "logs:CreateDelivery",
            }));
        }
    }
    out
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
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
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
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        let config = &body["deliveryDestination"]["deliveryDestinationConfiguration"];
        assert_eq!(
            config["destinationResourceArn"].as_str().unwrap(),
            "arn:aws:s3:::my-bucket"
        );
    }

    #[test]
    fn delivery_destination_role_arn_and_tags_round_trip() {
        let svc = make_service();
        let role = "arn:aws:iam::123456789012:role/xray-delivery";
        let resp = svc
            .put_delivery_destination(&make_request(
                "PutDeliveryDestination",
                json!({
                    "name": "xray-dest",
                    "deliveryDestinationType": "XRAY",
                    "roleArn": role,
                    "tags": {"team": "obs"},
                }),
            ))
            .unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(body["deliveryDestination"]["roleArn"], role);
        assert_eq!(body["deliveryDestination"]["tags"]["team"], "obs");

        let resp = svc
            .get_delivery_destination(&make_request(
                "GetDeliveryDestination",
                json!({"name": "xray-dest"}),
            ))
            .unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(body["deliveryDestination"]["roleArn"], role);
        assert_eq!(body["deliveryDestination"]["tags"]["team"], "obs");

        let resp = svc
            .describe_delivery_destinations(&make_request(
                "DescribeDeliveryDestinations",
                json!({}),
            ))
            .unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(body["deliveryDestinations"][0]["roleArn"], role);

        // A destination created without a role does not report one.
        let resp = svc
            .put_delivery_destination(&make_request(
                "PutDeliveryDestination",
                json!({"name": "plain", "deliveryDestinationConfiguration": {
                    "destinationResourceArn": "arn:aws:s3:::b"
                }}),
            ))
            .unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert!(body["deliveryDestination"].get("roleArn").is_none());
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
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
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
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
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

        // Verify data was written to export storage under the S3 bucket path.
        // GetExportedData transparently decompresses gzip payloads back to JSONL.
        let req = make_request(
            "GetExportedData",
            json!({ "keyPrefix": "delivery-test-bucket/aws-logs-write" }),
        );
        let resp = svc.get_exported_data(&req).unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
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
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
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
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(body["deliverySource"]["name"], "src1");
        assert_eq!(body["deliverySource"]["logType"], "APPLICATION_LOGS");

        // Describe
        let req = make_request("DescribeDeliverySources", json!({}));
        let resp = svc.describe_delivery_sources(&req).unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(body["deliverySources"].as_array().unwrap().len(), 1);

        // Delete
        let req = make_request("DeleteDeliverySource", json!({ "name": "src1" }));
        svc.delete_delivery_source(&req).unwrap();

        let req = make_request("DescribeDeliverySources", json!({}));
        let resp = svc.describe_delivery_sources(&req).unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
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
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(body["deliveryDestination"]["name"], "dd1");
        let arn = body["deliveryDestination"]["arn"]
            .as_str()
            .unwrap()
            .to_string();
        assert!(!arn.is_empty());

        // Describe
        let req = make_request("DescribeDeliveryDestinations", json!({}));
        let resp = svc.describe_delivery_destinations(&req).unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(body["deliveryDestinations"].as_array().unwrap().len(), 1);

        // Delete
        let req = make_request("DeleteDeliveryDestination", json!({ "name": "dd1" }));
        svc.delete_delivery_destination(&req).unwrap();

        let req = make_request("DescribeDeliveryDestinations", json!({}));
        let resp = svc.describe_delivery_destinations(&req).unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
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
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
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
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
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
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        let delivery_id = body["delivery"]["id"].as_str().unwrap().to_string();

        // Get delivery
        let req = make_request("GetDelivery", json!({ "id": delivery_id }));
        let resp = svc.get_delivery(&req).unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(body["delivery"]["deliverySourceName"], "del-src");

        // Describe deliveries
        let req = make_request("DescribeDeliveries", json!({}));
        let resp = svc.describe_deliveries(&req).unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(body["deliveries"].as_array().unwrap().len(), 1);

        // Delete delivery
        let req = make_request("DeleteDelivery", json!({ "id": delivery_id }));
        svc.delete_delivery(&req).unwrap();

        let req = make_request("DescribeDeliveries", json!({}));
        let resp = svc.describe_deliveries(&req).unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert!(body["deliveries"].as_array().unwrap().is_empty());
    }

    // DescribeDeliveries must echo the same recordFields / fieldDelimiter /
    // s3DeliveryConfiguration that GetDelivery returns (bug-hunt 2026-07-19).
    #[test]
    fn describe_deliveries_echoes_record_fields_and_delimiter() {
        let svc = make_service();
        create_group(&svc, "dd-grp");
        let req = make_request(
            "DescribeLogGroups",
            json!({ "logGroupNamePrefix": "dd-grp" }),
        );
        let resp = svc.describe_log_groups(&req).unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        let group_arn = body["logGroups"][0]["arn"].as_str().unwrap().to_string();

        svc.put_delivery_source(&make_request(
            "PutDeliverySource",
            json!({ "name": "dd-src", "resourceArn": group_arn, "logType": "APPLICATION_LOGS" }),
        ))
        .unwrap();
        svc.put_delivery_destination(&make_request(
            "PutDeliveryDestination",
            json!({
                "name": "dd-dest",
                "deliveryDestinationConfiguration": { "destinationResourceArn": "arn:aws:s3:::dd-bucket" }
            }),
        ))
        .unwrap();
        let resp = svc
            .get_delivery_destination(&make_request(
                "GetDeliveryDestination",
                json!({ "name": "dd-dest" }),
            ))
            .unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        let dest_arn = body["deliveryDestination"]["arn"]
            .as_str()
            .unwrap()
            .to_string();

        svc.create_delivery(&make_request(
            "CreateDelivery",
            json!({
                "deliverySourceName": "dd-src",
                "deliveryDestinationArn": dest_arn,
                "recordFields": ["timestamp", "message"],
                "fieldDelimiter": "|",
                "s3DeliveryConfiguration": { "suffixPath": "logs/" }
            }),
        ))
        .unwrap();

        let resp = svc
            .describe_deliveries(&make_request("DescribeDeliveries", json!({})))
            .unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        let d = &body["deliveries"][0];
        assert_eq!(d["recordFields"][0], "timestamp");
        assert_eq!(d["recordFields"][1], "message");
        assert_eq!(d["fieldDelimiter"], "|");
        assert_eq!(d["s3DeliveryConfiguration"]["suffixPath"], "logs/");
    }

    #[test]
    fn update_delivery_configuration_persists_fields() {
        let svc = make_service();

        let req = make_request(
            "PutDeliverySource",
            json!({
                "name": "my-source",
                "resourceArn": "arn:aws:logs:us-east-1:123456789012:log-group:test",
                "logType": "ACCESS_LOGS"
            }),
        );
        svc.put_delivery_source(&req).unwrap();
        let req = make_request(
            "PutDeliveryDestination",
            json!({"name": "my-dest", "deliveryDestinationConfiguration": {"destinationResourceArn": "arn:aws:s3:::my-bucket"}}),
        );
        let resp = svc.put_delivery_destination(&req).unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        let dest_arn = body["deliveryDestination"]["arn"]
            .as_str()
            .unwrap()
            .to_string();

        let req = make_request(
            "CreateDelivery",
            json!({"deliverySourceName": "my-source", "deliveryDestinationArn": dest_arn}),
        );
        let resp = svc.create_delivery(&req).unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        let id = body["delivery"]["id"].as_str().unwrap().to_string();

        let req = make_request(
            "UpdateDeliveryConfiguration",
            json!({
                "id": id,
                "fieldDelimiter": "|",
                "recordFields": ["@timestamp", "@message"],
            }),
        );
        svc.update_delivery_configuration(&req).unwrap();

        let req = make_request("GetDelivery", json!({ "id": id }));
        let resp = svc.get_delivery(&req).unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(body["delivery"]["fieldDelimiter"], "|");
        assert_eq!(body["delivery"]["recordFields"][0], "@timestamp");
    }

    #[test]
    fn describe_configuration_templates_returns_standard_set() {
        let svc = make_service();
        let req = make_request("DescribeConfigurationTemplates", json!({}));
        let resp = svc.describe_configuration_templates(&req).unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        let arr = body["configurationTemplates"].as_array().unwrap();
        assert!(!arr.is_empty(), "expected non-empty templates");

        let req = make_request(
            "DescribeConfigurationTemplates",
            json!({"service": "AWS/Lambda"}),
        );
        let resp = svc.describe_configuration_templates(&req).unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        let filtered = body["configurationTemplates"].as_array().unwrap();
        assert!(filtered.iter().all(|t| t["service"] == "AWS/Lambda"));
    }

    #[test]
    fn delivery_and_source_record_created_at_for_introspection() {
        let svc = make_service();
        create_group(&svc, "intro-grp");

        let req = make_request(
            "DescribeLogGroups",
            json!({ "logGroupNamePrefix": "intro-grp" }),
        );
        let resp = svc.describe_log_groups(&req).unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        let group_arn = body["logGroups"][0]["arn"].as_str().unwrap().to_string();

        // Source
        let before = chrono::Utc::now().timestamp_millis();
        let req = make_request(
            "PutDeliverySource",
            json!({
                "name": "intro-src",
                "resourceArn": group_arn,
                "logType": "APPLICATION_LOGS",
            }),
        );
        svc.put_delivery_source(&req).unwrap();

        // Destination
        let req = make_request(
            "PutDeliveryDestination",
            json!({
                "name": "intro-dest",
                "deliveryDestinationConfiguration": {
                    "destinationResourceArn": "arn:aws:s3:::intro-bucket"
                }
            }),
        );
        svc.put_delivery_destination(&req).unwrap();
        let req = make_request("GetDeliveryDestination", json!({ "name": "intro-dest" }));
        let resp = svc.get_delivery_destination(&req).unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        let dest_arn = body["deliveryDestination"]["arn"]
            .as_str()
            .unwrap()
            .to_string();

        let req = make_request(
            "CreateDelivery",
            json!({
                "deliverySourceName": "intro-src",
                "deliveryDestinationArn": dest_arn,
                "recordFields": ["timestamp", "message"],
                "fieldDelimiter": "|",
            }),
        );
        svc.create_delivery(&req).unwrap();

        let accounts = svc.state.read();
        let state = accounts
            .regional("123456789012", "us-east-1")
            .expect("default account state");
        let src = state.delivery_sources.get("intro-src").unwrap();
        assert!(
            src.created_at >= before,
            "delivery source created_at not populated (got {})",
            src.created_at
        );
        let delivery = state.deliveries.values().next().unwrap();
        assert!(delivery.created_at >= before);
        assert_eq!(delivery.delivery_source_name, "intro-src");
        assert_eq!(delivery.record_fields, vec!["timestamp", "message"]);
        assert_eq!(delivery.field_delimiter.as_deref(), Some("|"));
    }

    fn names_of(body: &Value, list: &str, field: &str) -> Vec<String> {
        body[list]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v[field].as_str().unwrap().to_string())
            .collect()
    }

    #[test]
    fn describe_filters_by_names_resource_arns_and_source_names() {
        let svc = make_service();
        let mut group_arns = Vec::new();
        for (i, grp) in ["flt-a", "flt-b"].iter().enumerate() {
            create_group(&svc, grp);
            let resp = svc
                .describe_log_groups(&make_request(
                    "DescribeLogGroups",
                    json!({ "logGroupNamePrefix": grp }),
                ))
                .unwrap();
            let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
            let arn = body["logGroups"][0]["arn"].as_str().unwrap().to_string();
            svc.put_delivery_source(&make_request(
                "PutDeliverySource",
                json!({ "name": format!("src-{i}"), "resourceArn": arn, "logType": "APPLICATION_LOGS" }),
            ))
            .unwrap();
            group_arns.push(arn);
            let resp = svc
                .put_delivery_destination(&make_request(
                    "PutDeliveryDestination",
                    json!({
                        "name": format!("dst-{i}"),
                        "deliveryDestinationConfiguration": {
                            "destinationResourceArn": format!("arn:aws:s3:::flt-bucket-{i}")
                        }
                    }),
                ))
                .unwrap();
            let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
            svc.create_delivery(&make_request(
                "CreateDelivery",
                json!({
                    "deliverySourceName": format!("src-{i}"),
                    "deliveryDestinationArn": body["deliveryDestination"]["arn"],
                }),
            ))
            .unwrap();
        }

        let resp = svc
            .describe_delivery_destinations(&make_request(
                "DescribeDeliveryDestinations",
                json!({ "names": ["dst-1", "missing"] }),
            ))
            .unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(names_of(&body, "deliveryDestinations", "name"), ["dst-1"]);

        let resp = svc
            .describe_delivery_sources(&make_request(
                "DescribeDeliverySources",
                json!({ "resourceArns": [group_arns[0]] }),
            ))
            .unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(names_of(&body, "deliverySources", "name"), ["src-0"]);

        let resp = svc
            .describe_deliveries(&make_request(
                "DescribeDeliveries",
                json!({ "deliverySourceNames": ["src-1"] }),
            ))
            .unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(
            names_of(&body, "deliveries", "deliverySourceName"),
            ["src-1"]
        );

        // Unfiltered + paginated: limit 1 walks both deliveries.
        let resp = svc
            .describe_deliveries(&make_request("DescribeDeliveries", json!({ "limit": 1 })))
            .unwrap();
        let first: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(first["deliveries"].as_array().unwrap().len(), 1);
        let token = first["nextToken"].as_str().unwrap().to_string();
        let resp = svc
            .describe_deliveries(&make_request(
                "DescribeDeliveries",
                json!({ "limit": 1, "nextToken": token }),
            ))
            .unwrap();
        let second: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(second["deliveries"].as_array().unwrap().len(), 1);
        assert!(second.get("nextToken").is_none());
        assert_ne!(first["deliveries"][0]["id"], second["deliveries"][0]["id"]);

        // An empty filter list violates the model's length >= 1.
        let err = svc
            .describe_deliveries(&make_request(
                "DescribeDeliveries",
                json!({ "deliverySourceNames": [] }),
            ))
            .err()
            .unwrap();
        assert_eq!(err.code(), "InvalidParameterException");
    }
}
