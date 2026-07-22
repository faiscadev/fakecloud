// Auto-extracted from service.rs as part of carryover service.rs split.

#![allow(clippy::too_many_arguments)]

use chrono::Utc;
use http::StatusCode;
use serde_json::{json, Value};

use fakecloud_core::pagination::paginate;
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use super::*;

impl EventBridgeService {
    pub(super) fn create_partner_event_source(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        validate_required("Name", &body["Name"])?;
        let name = body["Name"]
            .as_str()
            .ok_or_else(|| missing("Name"))?
            .to_string();
        validate_string_length("name", &name, 1, 256)?;
        validate_required("Account", &body["Account"])?;
        let account = body["Account"]
            .as_str()
            .ok_or_else(|| missing("Account"))?
            .to_string();
        validate_string_length("account", &account, 12, 12)?;

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        if state.partner_event_sources.contains_key(&name) {
            return Err(AwsServiceError::aws_error(
                StatusCode::CONFLICT,
                "ResourceAlreadyExistsException",
                format!("Partner event source {name} already exists."),
            ));
        }
        let arn = format!(
            "arn:aws:events:{}::event-source/aws.partner/{}",
            req.region, name
        );
        let now = Utc::now();
        let ps = PartnerEventSource {
            name: name.clone(),
            arn: arn.clone(),
            account,
            creation_time: now,
            expiration_time: None,
            state: "ACTIVE".to_string(),
        };
        state.partner_event_sources.insert(name.clone(), ps);

        Ok(AwsResponse::ok_json(json!({ "EventSourceArn": arn })))
    }

    pub(super) fn delete_partner_event_source(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        // Smithy declares Name (length 1..=256) and Account (length 12..=12)
        // as @required. Constraint violations surface as ValidationException
        // — the same wire shape AWS uses for malformed input even on
        // operations whose `errors:` set doesn't list it explicitly.
        validate_required("Name", &body["Name"])?;
        validate_required("Account", &body["Account"])?;
        let name = body["Name"]
            .as_str()
            .ok_or_else(|| missing("Name"))?
            .to_string();
        validate_string_length("Name", &name, 1, 256)?;
        let account = body["Account"]
            .as_str()
            .ok_or_else(|| missing("Account"))?
            .to_string();
        validate_string_length("Account", &account, 12, 12)?;

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        if let Some(ps) = state.partner_event_sources.get(&name) {
            if ps.account == account {
                state.partner_event_sources.remove(&name);
            }
        }

        Ok(AwsResponse::ok_json(json!({})))
    }

    pub(super) fn describe_partner_event_source(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        validate_required("Name", &body["Name"])?;
        let name = body["Name"]
            .as_str()
            .ok_or_else(|| missing("Name"))?
            .to_string();
        validate_string_length("name", &name, 1, 256)?;

        let accounts = self.state.read();
        let empty = EventBridgeState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
        let ps = state.partner_event_sources.get(&name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "ResourceNotFoundException",
                format!("Partner event source {name} does not exist."),
            )
        })?;

        Ok(AwsResponse::ok_json(json!({
            "Arn": ps.arn,
            "Name": ps.name,
        })))
    }

    pub(super) fn list_partner_event_sources(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        validate_required("namePrefix", &body["NamePrefix"])?;
        let name_prefix = body["NamePrefix"]
            .as_str()
            .ok_or_else(|| missing("NamePrefix"))?;
        validate_string_length("namePrefix", name_prefix, 1, 256)?;
        validate_optional_range_i64("limit", body["Limit"].as_i64(), 1, 100)?;
        validate_optional_string_length("nextToken", body["NextToken"].as_str(), 1, 2048)?;
        let limit = body["Limit"].as_i64().unwrap_or(100) as usize;

        let accounts = self.state.read();
        let empty = EventBridgeState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
        let all: Vec<Value> = state
            .partner_event_sources
            .values()
            .filter(|ps| ps.name.starts_with(name_prefix))
            .map(|ps| {
                json!({
                    "Arn": ps.arn,
                    "Name": ps.name,
                })
            })
            .collect();

        let (sources, next_token) = paginate(&all, body["NextToken"].as_str(), limit);
        let mut resp = json!({ "PartnerEventSources": sources });
        if let Some(token) = next_token {
            resp["NextToken"] = json!(token);
        }

        Ok(AwsResponse::ok_json(resp))
    }

    pub(super) fn list_partner_event_source_accounts(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        validate_required("EventSourceName", &body["EventSourceName"])?;
        let event_source_name = body["EventSourceName"]
            .as_str()
            .ok_or_else(|| missing("EventSourceName"))?;
        validate_string_length("eventSourceName", event_source_name, 1, 256)?;
        validate_optional_range_i64("limit", body["Limit"].as_i64(), 1, 100)?;
        validate_optional_string_length("nextToken", body["NextToken"].as_str(), 1, 2048)?;

        let accounts = self.state.read();
        let empty = EventBridgeState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
        let accounts: Vec<Value> = state
            .partner_event_sources
            .values()
            .filter(|ps| ps.name == event_source_name)
            .map(|ps| json!({ "Account": ps.account }))
            .collect();

        Ok(AwsResponse::ok_json(json!({
            "PartnerEventSourceAccounts": accounts
        })))
    }

    pub(super) fn activate_event_source(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        validate_required("Name", &body["Name"])?;
        let name = body["Name"]
            .as_str()
            .ok_or_else(|| missing("Name"))?
            .to_string();

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let ps = state.partner_event_sources.get_mut(&name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "ResourceNotFoundException",
                format!("Event source {name} does not exist."),
            )
        })?;
        ps.state = "ACTIVE".to_string();

        Ok(AwsResponse::ok_json(json!({})))
    }

    pub(super) fn deactivate_event_source(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        validate_required("Name", &body["Name"])?;
        let name = body["Name"]
            .as_str()
            .ok_or_else(|| missing("Name"))?
            .to_string();

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let ps = state.partner_event_sources.get_mut(&name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "ResourceNotFoundException",
                format!("Event source {name} does not exist."),
            )
        })?;
        ps.state = "INACTIVE".to_string();

        Ok(AwsResponse::ok_json(json!({})))
    }

    pub(super) fn describe_event_source(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        validate_required("Name", &body["Name"])?;
        let name = body["Name"]
            .as_str()
            .ok_or_else(|| missing("Name"))?
            .to_string();

        let accounts = self.state.read();
        let empty = EventBridgeState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
        let ps = state.partner_event_sources.get(&name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "ResourceNotFoundException",
                format!("Event source {name} does not exist."),
            )
        })?;

        Ok(AwsResponse::ok_json(json!({
            "Arn": ps.arn,
            "Name": ps.name,
            "CreatedBy": ps.account,
            "CreationTime": ps.creation_time.timestamp() as f64,
            "State": ps.state,
        })))
    }

    pub(super) fn list_event_sources(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        validate_optional_string_length("namePrefix", body["NamePrefix"].as_str(), 1, 256)?;
        validate_optional_range_i64("limit", body["Limit"].as_i64(), 1, 100)?;
        validate_optional_string_length("nextToken", body["NextToken"].as_str(), 1, 2048)?;
        let name_prefix = body["NamePrefix"].as_str();
        let limit = body["Limit"].as_i64().unwrap_or(100) as usize;

        let accounts = self.state.read();
        let empty = EventBridgeState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
        let all: Vec<Value> = state
            .partner_event_sources
            .values()
            .filter(|ps| match name_prefix {
                Some(prefix) => ps.name.starts_with(prefix),
                None => true,
            })
            .map(|ps| {
                json!({
                    "Arn": ps.arn,
                    "Name": ps.name,
                    "CreatedBy": ps.account,
                    "CreationTime": ps.creation_time.timestamp() as f64,
                    "State": ps.state,
                })
            })
            .collect();

        let (sources, next_token) = paginate(&all, body["NextToken"].as_str(), limit);
        let mut resp = json!({ "EventSources": sources });
        if let Some(token) = next_token {
            resp["NextToken"] = json!(token);
        }

        Ok(AwsResponse::ok_json(resp))
    }

    pub(super) fn put_partner_events(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        validate_required("Entries", &body["Entries"])?;
        let entries_array = body["Entries"]
            .as_array()
            .ok_or_else(|| missing("Entries"))?;
        if entries_array.is_empty() || entries_array.len() > 10 {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                "Value at 'Entries' failed to satisfy constraint: \
                 Member must have length between 1 and 10",
            ));
        }
        let entries = entries_array.clone();

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let mut result_entries = Vec::new();
        let mut events_to_deliver = Vec::new();
        let mut failed_count = 0;

        for entry in &entries {
            // For partner events the bus is the partner source name: AWS
            // routes the event to the partner event bus that mirrors the
            // source. Validate Source/DetailType/Detail like PutEvents.
            let source = entry["Source"].as_str().unwrap_or("").to_string();
            let detail_type = entry["DetailType"].as_str().unwrap_or("").to_string();
            let detail = entry["Detail"].as_str().unwrap_or("").to_string();

            if let Err(error) = validate_put_events_entry(&source, &detail_type, &detail) {
                failed_count += 1;
                result_entries.push(error);
                continue;
            }

            let event_id = uuid::Uuid::new_v4().to_string();
            // The partner event bus is named after the source. If the
            // receiving account has not created that bus yet, the event is
            // accepted (returns an EventId) but not routed -- matching AWS.
            let event_bus_name = source.clone();
            let time = parse_put_events_time(&entry["Time"]);
            let resources: Vec<String> = entry["Resources"]
                .as_array()
                .map(|arr| {
                    arr.iter()
                        .filter_map(|v| v.as_str().map(|s| s.to_string()))
                        .collect()
                })
                .unwrap_or_default();

            let event = PutEvent {
                event_id: event_id.clone(),
                source: source.clone(),
                detail_type: detail_type.clone(),
                detail: detail.clone(),
                event_bus_name: event_bus_name.clone(),
                time,
                resources: resources.clone(),
            };

            archive_matching_event(
                state,
                &event,
                &event_bus_name,
                &source,
                &detail_type,
                &detail,
                &req.account_id,
                &req.region,
                &resources,
            );

            state.events.push(event);

            // Route to rules attached to the partner event bus.
            let matching_targets: Vec<(String, EventTarget)> = state
                .rules
                .values()
                .filter(|r| {
                    r.event_bus_name == event_bus_name
                        && r.state == "ENABLED"
                        && matches_pattern(
                            r.event_pattern.as_deref(),
                            &source,
                            &detail_type,
                            &detail,
                            &req.account_id,
                            &req.region,
                            &resources,
                            &event_id,
                            &time.to_rfc3339(),
                        )
                })
                .flat_map(|r| r.targets.iter().map(|t| (r.arn.clone(), t.clone())))
                .collect();

            if !matching_targets.is_empty() {
                events_to_deliver.push((
                    event_id.clone(),
                    source,
                    detail_type,
                    detail,
                    time,
                    resources,
                    matching_targets,
                ));
            }

            result_entries.push(json!({ "EventId": event_id }));
        }

        drop(accounts);

        for (event_id, source, detail_type, detail, time, resources, targets) in events_to_deliver {
            let detail_value: Value = serde_json::from_str(&detail).unwrap_or(json!({}));
            let event_json = json!({
                "version": "0",
                "id": event_id,
                "source": source,
                "account": req.account_id,
                "detail-type": detail_type,
                "detail": detail_value,
                "time": time.format("%Y-%m-%dT%H:%M:%SZ").to_string(),
                "region": req.region,
                "resources": resources,
            });

            let ctx = EventDispatchContext {
                state: &self.state,
                delivery: &self.delivery,
                lambda_state: self.lambda_state.as_ref(),
                logs_state: self.logs_state.as_ref(),
                logs_persist: self.logs_persist.as_ref(),
                container_runtime: &self.container_runtime,
                account_id: &req.account_id,
                region: &req.region,
            };
            for (rule_arn, target) in targets {
                dispatch_event_target(
                    &ctx,
                    &target,
                    &event_json,
                    &event_id,
                    &detail_type,
                    Some(&rule_arn),
                );
            }
        }

        Ok(AwsResponse::ok_json(json!({
            "FailedEntryCount": failed_count,
            "Entries": result_entries,
        })))
    }

    // ─── TestEventPattern ────────────────────────────────────────────────
}
