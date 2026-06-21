// Auto-extracted from service.rs as part of carryover service.rs split.

#![allow(clippy::too_many_arguments)]

use chrono::Utc;
use http::StatusCode;
use serde_json::{json, Value};

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use super::*;

impl LambdaService {
    pub(super) fn create_event_source_mapping(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = serde_json::from_slice(&req.body).unwrap_or_default();
        let event_source_arn = body["EventSourceArn"]
            .as_str()
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterValueException",
                    "EventSourceArn is required",
                )
            })?
            .to_string();

        let function_name = body["FunctionName"]
            .as_str()
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterValueException",
                    "FunctionName is required",
                )
            })?
            .to_string();

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);

        // Resolve function name to ARN
        let function_arn = if function_name.starts_with("arn:") {
            function_name.clone()
        } else {
            let func = state.functions.get(&function_name).ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::NOT_FOUND,
                    "ResourceNotFoundException",
                    format!(
                        "Function not found: arn:aws:lambda:{}:{}:function:{}",
                        state.region, state.account_id, function_name
                    ),
                )
            })?;
            func.function_arn.clone()
        };

        let batch_size = body["BatchSize"].as_i64().unwrap_or(10);
        let enabled = body["Enabled"].as_bool().unwrap_or(true);
        let mapping_uuid = uuid::Uuid::new_v4().to_string();
        let now = Utc::now();

        // Extract Filters[].Pattern strictly: any entry where
        // `Pattern` is missing or not a string is a hard error,
        // matching AWS. Doing this before `validate` keeps malformed
        // values from being silently dropped by the lossy serializer.
        // FilterCriteria itself must be an object (or absent) — non-
        // object values would otherwise be silently dropped by
        // `Value::get`, masking client bugs.
        let filter_patterns: Vec<String> = match body.get("FilterCriteria") {
            None | Some(Value::Null) => Vec::new(),
            Some(Value::Object(_)) => {
                match body.get("FilterCriteria").and_then(|v| v.get("Filters")) {
                    None => Vec::new(),
                    Some(Value::Array(arr)) => {
                        let mut out = Vec::with_capacity(arr.len());
                        for f in arr {
                            match f.get("Pattern") {
                                Some(Value::String(s)) => out.push(s.clone()),
                                _ => {
                                    return Err(AwsServiceError::aws_error(
                                        StatusCode::BAD_REQUEST,
                                        "InvalidParameterValueException",
                                        "FilterCriteria.Filters[].Pattern must be a string",
                                    ));
                                }
                            }
                        }
                        out
                    }
                    Some(_) => {
                        return Err(AwsServiceError::aws_error(
                            StatusCode::BAD_REQUEST,
                            "InvalidParameterValueException",
                            "FilterCriteria.Filters must be an array",
                        ));
                    }
                }
            }
            Some(_) => {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterValueException",
                    "FilterCriteria must be an object",
                ));
            }
        };
        // AWS rejects malformed FilterCriteria at create time.
        if let Err(err) = crate::filter::FilterSet::validate(filter_patterns.iter()) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterValueException",
                err,
            ));
        }
        let function_response_types: Vec<String> = body
            .get("FunctionResponseTypes")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();
        let starting_position = body
            .get("StartingPosition")
            .and_then(|v| v.as_str())
            .map(String::from);
        let starting_position_timestamp = body
            .get("StartingPositionTimestamp")
            .and_then(|v| v.as_f64());
        let parallelization_factor = body.get("ParallelizationFactor").and_then(|v| v.as_i64());
        let maximum_batching_window_in_seconds = body
            .get("MaximumBatchingWindowInSeconds")
            .and_then(|v| v.as_i64());
        let kms_key_arn = body
            .get("KMSKeyArn")
            .and_then(|v| v.as_str())
            .map(String::from);
        let metrics_config = body.get("MetricsConfig").cloned();
        let destination_config = body.get("DestinationConfig").cloned();
        let maximum_retry_attempts = body.get("MaximumRetryAttempts").and_then(|v| v.as_i64());
        let maximum_record_age_in_seconds = body
            .get("MaximumRecordAgeInSeconds")
            .and_then(|v| v.as_i64());
        let bisect_batch_on_function_error = body
            .get("BisectBatchOnFunctionError")
            .and_then(|v| v.as_bool());
        let tumbling_window_in_seconds =
            body.get("TumblingWindowInSeconds").and_then(|v| v.as_i64());
        let topics: Vec<String> = body
            .get("Topics")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();
        let queues: Vec<String> = body
            .get("Queues")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();
        // SourceAccessConfigurations (VPC/auth config for Kafka/MQ/MSK sources):
        // persist what the caller sent so Get/List/Update echo it back rather
        // than always returning [] (bug-audit 2026-05-28, 1.17).
        let source_access_configurations: Vec<Value> = body
            .get("SourceAccessConfigurations")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();

        let mapping = EventSourceMapping {
            uuid: mapping_uuid.clone(),
            function_arn: function_arn.clone(),
            event_source_arn: event_source_arn.clone(),
            batch_size,
            enabled,
            state: if enabled {
                "Enabled".to_string()
            } else {
                "Disabled".to_string()
            },
            last_modified: now,
            filter_patterns,
            maximum_batching_window_in_seconds,
            starting_position,
            starting_position_timestamp,
            parallelization_factor,
            function_response_types,
            kms_key_arn,
            metrics_config,
            destination_config,
            maximum_retry_attempts,
            maximum_record_age_in_seconds,
            bisect_batch_on_function_error,
            tumbling_window_in_seconds,
            topics,
            queues,
            source_access_configurations,
        };

        let response = self.event_source_mapping_json(&mapping);
        state.event_source_mappings.insert(mapping_uuid, mapping);

        Ok(AwsResponse::json(
            StatusCode::ACCEPTED,
            response.to_string(),
        ))
    }

    pub(super) fn list_event_source_mappings(
        &self,
        account_id: &str,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let marker = req.query_params.get("Marker").map(String::as_str);
        let max_items = crate::service::marker_page_size(req);
        // Optional FunctionName / EventSourceArn filters. FunctionName may
        // arrive as a bare name or an ARN; match on the ARN suffix so either
        // form selects the right mappings.
        let function_filter = req
            .query_params
            .get("FunctionName")
            .map(|f| crate::service::normalize_function_name(f));
        let event_source_filter = req.query_params.get("EventSourceArn").cloned();

        let accounts = self.state.read();
        let empty = LambdaState::new(account_id, "");
        let state = accounts.get(account_id).unwrap_or(&empty);
        let mappings: Vec<crate::state::EventSourceMapping> = state
            .event_source_mappings
            .values()
            .filter(|m| {
                function_filter
                    .as_deref()
                    .is_none_or(|f| crate::service::normalize_function_name(&m.function_arn) == f)
            })
            .filter(|m| {
                event_source_filter
                    .as_deref()
                    .is_none_or(|e| m.event_source_arn == e)
            })
            .cloned()
            .collect();

        let (page, next_marker) =
            crate::service::paginate_marker(mappings, marker, max_items, |m| m.uuid.clone());
        let mappings_json: Vec<Value> = page
            .iter()
            .map(|m| self.event_source_mapping_json(m))
            .collect();

        let response = json!({
            "EventSourceMappings": mappings_json,
            "NextMarker": next_marker,
        });

        Ok(AwsResponse::json(StatusCode::OK, response.to_string()))
    }

    pub(super) fn get_event_source_mapping(
        &self,
        uuid: &str,
        account_id: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let accounts = self.state.read();
        let empty = LambdaState::new(account_id, "");
        let state = accounts.get(account_id).unwrap_or(&empty);
        let mapping = state.event_source_mappings.get(uuid).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "ResourceNotFoundException",
                format!("The resource you requested does not exist. (Service: Lambda, Status Code: 404, Request ID: {uuid})"),
            )
        })?;

        let response = self.event_source_mapping_json(mapping);
        Ok(AwsResponse::json(StatusCode::OK, response.to_string()))
    }

    pub(super) fn delete_event_source_mapping(
        &self,
        uuid: &str,
        account_id: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(account_id);
        let mapping = state.event_source_mappings.remove(uuid).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "ResourceNotFoundException",
                format!("The resource you requested does not exist. (Service: Lambda, Status Code: 404, Request ID: {uuid})"),
            )
        })?;

        let mut response = self.event_source_mapping_json(&mapping);
        response["State"] = json!("Deleting");
        Ok(AwsResponse::json(
            StatusCode::ACCEPTED,
            response.to_string(),
        ))
    }

    pub(crate) fn event_source_mapping_json(&self, mapping: &EventSourceMapping) -> Value {
        // EventSourceMappingArn is the EventSourceArn variant prefixed with
        // the mapping uuid; AWS started returning it in 2024 so newer SDKs
        // expect to round-trip it.
        let esm_arn = format!(
            "{}:event-source-mapping/{}",
            mapping.function_arn, mapping.uuid
        );
        let mut out = json!({
            "UUID": mapping.uuid,
            "FunctionArn": mapping.function_arn,
            "EventSourceArn": mapping.event_source_arn,
            "BatchSize": mapping.batch_size,
            "State": mapping.state,
            "LastModified": mapping.last_modified.timestamp_millis() as f64 / 1000.0,
            "EventSourceMappingArn": esm_arn,
            // AWS always returns these — emit stored values, falling back to
            // the documented defaults (-1 = infinite, false, 0).
            "MaximumRetryAttempts": mapping.maximum_retry_attempts.unwrap_or(-1),
            "MaximumRecordAgeInSeconds": mapping.maximum_record_age_in_seconds.unwrap_or(-1),
            "BisectBatchOnFunctionError": mapping.bisect_batch_on_function_error.unwrap_or(false),
            "TumblingWindowInSeconds": mapping.tumbling_window_in_seconds.unwrap_or(0),
            "Topics": mapping.topics,
            "Queues": mapping.queues,
            "SourceAccessConfigurations": mapping.source_access_configurations,
            "LastProcessingResult": "No records processed",
            "StateTransitionReason": "User action",
        });
        let obj = out.as_object_mut().expect("json! built object");
        if !mapping.filter_patterns.is_empty() {
            obj.insert(
                "FilterCriteria".into(),
                json!({
                    "Filters": mapping.filter_patterns.iter().map(|p| json!({"Pattern": p})).collect::<Vec<_>>(),
                }),
            );
        }
        if !mapping.function_response_types.is_empty() {
            obj.insert(
                "FunctionResponseTypes".into(),
                json!(mapping.function_response_types),
            );
        }
        if let Some(sp) = &mapping.starting_position {
            obj.insert("StartingPosition".into(), json!(sp));
        }
        if let Some(ts) = mapping.starting_position_timestamp {
            obj.insert("StartingPositionTimestamp".into(), json!(ts));
        }
        if let Some(pf) = mapping.parallelization_factor {
            obj.insert("ParallelizationFactor".into(), json!(pf));
        }
        if let Some(w) = mapping.maximum_batching_window_in_seconds {
            obj.insert("MaximumBatchingWindowInSeconds".into(), json!(w));
        }
        if let Some(ref kms) = mapping.kms_key_arn {
            obj.insert("KMSKeyArn".into(), json!(kms));
        }
        if let Some(ref mc) = mapping.metrics_config {
            obj.insert("MetricsConfig".into(), mc.clone());
        }
        if let Some(ref dc) = mapping.destination_config {
            obj.insert("DestinationConfig".into(), dc.clone());
        }
        out
    }
}
