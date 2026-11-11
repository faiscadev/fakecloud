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
        // The request checks CloudFormation's AWS::Lambda::EventSourceMapping
        // runs too: settings, function reference, event source (shape,
        // region, service, existence) and filter patterns.
        let filter_patterns = crate::event_source::validate_create(
            &body,
            &req.region,
            self.event_source_lookup.as_deref(),
        )?;
        let event_source_arn = body["EventSourceArn"]
            .as_str()
            .unwrap_or_default()
            .to_string();
        let function_name = body["FunctionName"]
            .as_str()
            .unwrap_or_default()
            .to_string();

        let mut accounts = self.state.write();
        let state = accounts.regional_mut(&req.account_id, &req.region);

        let function_arn = mapping_function_arn(state, &req.region, &function_name)?;

        let batch_size = body["BatchSize"].as_i64().unwrap_or(10);
        let enabled = body["Enabled"].as_bool().unwrap_or(true);
        let mapping_uuid = uuid::Uuid::new_v4().to_string();
        let now = Utc::now();

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
            self_managed_event_source: body
                .get("SelfManagedEventSource")
                .filter(|v| v.is_object())
                .cloned(),
            self_managed_kafka_event_source_config: body
                .get("SelfManagedKafkaEventSourceConfig")
                .filter(|v| v.is_object())
                .cloned(),
            document_db_event_source_config: body
                .get("DocumentDBEventSourceConfig")
                .filter(|v| v.is_object())
                .cloned(),
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
        // `Arn.length 0..10000`: an over-long filter is rejected rather than
        // quietly matching nothing.
        if let Some(arn) = &event_source_filter {
            if arn.chars().count() > 10_000 {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterValueException",
                    "EventSourceArn exceeds the 10000-character maximum",
                ));
            }
        }

        let accounts = self.state.read();
        let empty = LambdaState::new(account_id, "");
        let state = accounts.regional(account_id, &req.region).unwrap_or(&empty);
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
        region: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let accounts = self.state.read();
        let mapping = accounts
            .regional(account_id, region)
            .and_then(|state| state.event_source_mappings.get(uuid)).ok_or_else(|| {
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
        region: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut accounts = self.state.write();
        let mapping = accounts
            .regional_get_mut(account_id, region)
            .and_then(|state| state.event_source_mappings.remove(uuid))
            .ok_or_else(|| {
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
        // A self-managed Kafka source has no ARN, and AWS sends none.
        if !mapping.event_source_arn.is_empty() {
            obj.insert("EventSourceArn".into(), json!(mapping.event_source_arn));
        }
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
        if let Some(ref src) = mapping.self_managed_event_source {
            obj.insert("SelfManagedEventSource".into(), src.clone());
        }
        if let Some(ref cfg) = mapping.self_managed_kafka_event_source_config {
            obj.insert("SelfManagedKafkaEventSourceConfig".into(), cfg.clone());
        }
        if let Some(ref cfg) = mapping.document_db_event_source_config {
            obj.insert("DocumentDBEventSourceConfig".into(), cfg.clone());
        }
        out
    }
}

/// The function ARN an event source mapping invokes. `FunctionName` may be a
/// name, a partial ARN or an ARN, optionally qualified by a version or an
/// alias; the function (and that qualifier) must exist in the mapping's
/// account, else `ResourceNotFoundException`, and an ARN must name the
/// request's region, as on AWS. Shared by
/// `CreateEventSourceMapping` and `UpdateEventSourceMapping`.
pub(crate) fn mapping_function_arn(
    state: &crate::state::LambdaState,
    region: &str,
    function_name: &str,
) -> Result<String, AwsServiceError> {
    crate::event_source::check_function_region(function_name, region)?;
    let name = crate::service::normalize_function_name(function_name);
    let qualifier = crate::service::qualifier_from_function_ref(function_name);
    let account = crate::service::function_ref_scope(function_name)
        .0
        .unwrap_or(&state.account_id);
    let not_found = || {
        let mut arn = function_arn(region, account, &name);
        if let Some(q) = &qualifier {
            arn = format!("{arn}:{q}");
        }
        AwsServiceError::aws_error(
            StatusCode::NOT_FOUND,
            "ResourceNotFoundException",
            format!("Function not found: {arn}"),
        )
    };
    if account != state.account_id {
        return Err(not_found());
    }
    let func = state.functions.get(&name).ok_or_else(not_found)?;
    match qualifier.as_deref() {
        None => Ok(func.function_arn.clone()),
        Some(q) => {
            let exists = q == "$LATEST"
                || state
                    .function_versions
                    .get(&name)
                    .is_some_and(|vs| vs.iter().any(|v| v == q))
                || state.aliases.contains_key(&format!("{name}:{q}"));
            if !exists {
                return Err(not_found());
            }
            Ok(format!("{}:{q}", func.function_arn))
        }
    }
}
