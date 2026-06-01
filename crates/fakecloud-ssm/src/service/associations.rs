use std::collections::BTreeMap;

use chrono::Utc;
use http::StatusCode;
use serde_json::{json, Value};

use fakecloud_core::pagination::paginate_checked;
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};
use fakecloud_core::validation::*;

use crate::state::{SsmAssociation, SsmAssociationVersion, SsmState};

use super::{missing, missing_with_code, SsmService};

/// CreateAssociation's Smithy errors list includes `InvalidParameters`
/// but not the generic `ValidationException` that the shared `validate_*`
/// helpers emit. Wrap their results to flip the wire code while keeping
/// the same message; everything else passes through unchanged.
fn remap_validation_to_invalid_parameters(err: AwsServiceError) -> AwsServiceError {
    if err.code() == "ValidationException" {
        AwsServiceError::aws_error(StatusCode::BAD_REQUEST, "InvalidParameters", err.message())
    } else {
        err
    }
}

impl SsmService {
    pub(super) fn create_association_inner(
        &self,
        body: &Value,
        account_id: &str,
    ) -> Result<Value, AwsServiceError> {
        let input = CreateAssociationInput::from_body(body)?;

        let now = Utc::now();
        let association_id = uuid::Uuid::new_v4().to_string();

        let version = SsmAssociationVersion {
            version: 1,
            name: input.name.clone(),
            targets: input.targets.clone(),
            schedule_expression: input.schedule_expression.clone(),
            parameters: input.parameters.clone(),
            document_version: input.document_version.clone(),
            created_date: now,
            association_name: input.association_name.clone(),
            max_errors: input.max_errors.clone(),
            max_concurrency: input.max_concurrency.clone(),
            compliance_severity: input.compliance_severity.clone(),
        };

        let assoc = SsmAssociation {
            association_id: association_id.clone(),
            name: input.name,
            targets: input.targets,
            schedule_expression: input.schedule_expression,
            parameters: input.parameters,
            association_name: input.association_name,
            document_version: input.document_version,
            output_location: input.output_location,
            automation_target_parameter_name: input.automation_target_parameter_name,
            max_errors: input.max_errors,
            max_concurrency: input.max_concurrency,
            compliance_severity: input.compliance_severity,
            sync_compliance: input.sync_compliance,
            apply_only_at_cron_interval: input.apply_only_at_cron_interval,
            calendar_names: input.calendar_names,
            target_locations: input.target_locations,
            schedule_offset: input.schedule_offset,
            target_maps: input.target_maps,
            tags: input.tags,
            status: "Pending".to_string(),
            status_date: now,
            overview: json!({"Status": "Pending", "DetailedStatus": "Creating", "AssociationStatusAggregatedCount": {}}),
            created_date: now,
            last_update_association_date: now,
            last_execution_date: None,
            instance_id: input.instance_id,
            versions: vec![version],
            executions: Vec::new(),
        };

        let resp = association_to_json(&assoc);

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(account_id);
        state.associations.insert(association_id, assoc);

        Ok(resp)
    }

    pub(super) fn create_association(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        // CreateAssociation declares InvalidParameters (among others)
        // but not the generic ValidationException emitted by the shared
        // validate_* helpers. Remap so strict-Smithy clients see one
        // of the op's declared errors.
        let resp = self
            .create_association_inner(&body, &req.account_id)
            .map_err(remap_validation_to_invalid_parameters)?;
        Ok(AwsResponse::ok_json(
            json!({ "AssociationDescription": resp }),
        ))
    }

    pub(super) fn describe_association(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let association_id = body["AssociationId"].as_str();
        let name = body["Name"].as_str();
        let instance_id = body["InstanceId"].as_str();

        let accounts = self.state.read();
        let empty = SsmState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);

        let assoc = if let Some(id) = association_id {
            state.associations.get(id)
        } else if let Some(n) = name {
            state.associations.values().find(|a| {
                a.name == n && (instance_id.is_none() || a.instance_id.as_deref() == instance_id)
            })
        } else {
            // DescribeAssociation declares AssociationDoesNotExist; route
            // missing-input through it instead of ValidationException
            // (which isn't declared on this op).
            return Err(missing_with_code(
                "AssociationId",
                "AssociationDoesNotExist",
            ));
        };

        match assoc {
            Some(a) => Ok(AwsResponse::ok_json(
                json!({ "AssociationDescription": association_to_json(a) }),
            )),
            None => Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "AssociationDoesNotExist",
                "The specified association does not exist.".to_string(),
            )),
        }
    }

    pub(super) fn delete_association(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let association_id = body["AssociationId"].as_str();
        let name = body["Name"].as_str();
        let instance_id = body["InstanceId"].as_str();

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);

        let key = if let Some(id) = association_id {
            if state.associations.contains_key(id) {
                Some(id.to_string())
            } else {
                None
            }
        } else if let Some(n) = name {
            state
                .associations
                .iter()
                .find(|(_, a)| {
                    a.name == n
                        && (instance_id.is_none() || a.instance_id.as_deref() == instance_id)
                })
                .map(|(k, _)| k.clone())
        } else {
            return Err(missing_with_code(
                "AssociationId",
                "AssociationDoesNotExist",
            ));
        };

        match key {
            Some(k) => {
                state.associations.remove(&k);
                Ok(AwsResponse::ok_json(json!({})))
            }
            None => Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "AssociationDoesNotExist",
                "The specified association does not exist.".to_string(),
            )),
        }
    }

    pub(super) fn list_associations(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        validate_optional_range_i64("MaxResults", body["MaxResults"].as_i64(), 1, 50)?;
        let max_results = body["MaxResults"].as_i64().unwrap_or(50) as usize;

        let accounts = self.state.read();
        let empty = SsmState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
        let all: Vec<Value> = state
            .associations
            .values()
            .map(|a| {
                let mut v = json!({
                    "AssociationId": a.association_id,
                    "Name": a.name,
                });
                if let Some(d) = a.last_execution_date {
                    v["LastExecutionDate"] = json!(d.timestamp_millis() as f64 / 1000.0);
                }
                if let Some(ref an) = a.association_name {
                    v["AssociationName"] = json!(an);
                }
                if let Some(ref s) = a.schedule_expression {
                    v["ScheduleExpression"] = json!(s);
                }
                if !a.targets.is_empty() {
                    v["Targets"] = json!(a.targets);
                }
                if let Some(ref iid) = a.instance_id {
                    v["InstanceId"] = json!(iid);
                }
                v["Overview"] = a.overview.clone();
                v
            })
            .collect();

        let (items, next_token) = paginate_checked(&all, body["NextToken"].as_str(), max_results)
            .map_err(|_| super::invalid_next_token())?;
        let mut resp = json!({ "Associations": items });
        if let Some(token) = next_token {
            resp["NextToken"] = json!(token);
        }
        Ok(AwsResponse::ok_json(resp))
    }

    pub(super) fn update_association(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let association_id = body["AssociationId"]
            .as_str()
            .ok_or_else(|| missing("AssociationId"))?;

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let assoc = state.associations.get_mut(association_id).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "AssociationDoesNotExist",
                "The specified association does not exist.".to_string(),
            )
        })?;

        let now = Utc::now();

        if let Some(n) = body["Name"].as_str() {
            assoc.name = n.to_string();
        }
        if let Some(targets) = body["Targets"].as_array() {
            assoc.targets = targets.clone();
        }
        if let Some(s) = body["ScheduleExpression"].as_str() {
            assoc.schedule_expression = Some(s.to_string());
        }
        if let Some(obj) = body["Parameters"].as_object() {
            assoc.parameters = obj
                .iter()
                .map(|(k, v)| {
                    let vals = v
                        .as_array()
                        .map(|a| {
                            a.iter()
                                .filter_map(|x| x.as_str().map(|s| s.to_string()))
                                .collect()
                        })
                        .unwrap_or_default();
                    (k.clone(), vals)
                })
                .collect();
        }
        if let Some(an) = body["AssociationName"].as_str() {
            assoc.association_name = Some(an.to_string());
        }
        if let Some(dv) = body["DocumentVersion"].as_str() {
            assoc.document_version = Some(dv.to_string());
        }
        if let Some(me) = body["MaxErrors"].as_str() {
            assoc.max_errors = Some(me.to_string());
        }
        if let Some(mc) = body["MaxConcurrency"].as_str() {
            assoc.max_concurrency = Some(mc.to_string());
        }
        if let Some(cs) = body["ComplianceSeverity"].as_str() {
            assoc.compliance_severity = Some(cs.to_string());
        }

        assoc.last_update_association_date = now;

        let next_version = assoc.versions.len() as i64 + 1;
        assoc.versions.push(SsmAssociationVersion {
            version: next_version,
            name: assoc.name.clone(),
            targets: assoc.targets.clone(),
            schedule_expression: assoc.schedule_expression.clone(),
            parameters: assoc.parameters.clone(),
            document_version: assoc.document_version.clone(),
            created_date: now,
            association_name: assoc.association_name.clone(),
            max_errors: assoc.max_errors.clone(),
            max_concurrency: assoc.max_concurrency.clone(),
            compliance_severity: assoc.compliance_severity.clone(),
        });

        let resp = association_to_json(assoc);
        Ok(AwsResponse::ok_json(
            json!({ "AssociationDescription": resp }),
        ))
    }

    pub(super) fn list_association_versions(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let association_id = body["AssociationId"]
            .as_str()
            .ok_or_else(|| missing("AssociationId"))?;
        let max_results = body["MaxResults"].as_i64().unwrap_or(50) as usize;

        let accounts = self.state.read();
        let empty = SsmState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
        let assoc = state.associations.get(association_id).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "AssociationDoesNotExist",
                "The specified association does not exist.".to_string(),
            )
        })?;

        let all: Vec<Value> = assoc
            .versions
            .iter()
            .map(|v| {
                let mut j = json!({
                    "AssociationId": association_id,
                    "AssociationVersion": v.version.to_string(),
                    "Name": v.name,
                    "CreatedDate": v.created_date.timestamp_millis() as f64 / 1000.0,
                });
                if !v.targets.is_empty() {
                    j["Targets"] = json!(v.targets);
                }
                if let Some(ref s) = v.schedule_expression {
                    j["ScheduleExpression"] = json!(s);
                }
                if let Some(ref an) = v.association_name {
                    j["AssociationName"] = json!(an);
                }
                if let Some(ref dv) = v.document_version {
                    j["DocumentVersion"] = json!(dv);
                }
                if let Some(ref me) = v.max_errors {
                    j["MaxErrors"] = json!(me);
                }
                if let Some(ref mc) = v.max_concurrency {
                    j["MaxConcurrency"] = json!(mc);
                }
                if let Some(ref cs) = v.compliance_severity {
                    j["ComplianceSeverity"] = json!(cs);
                }
                j
            })
            .collect();

        let (items, next_token) = paginate_checked(&all, body["NextToken"].as_str(), max_results)
            .map_err(|_| super::invalid_next_token())?;
        let mut resp = json!({ "AssociationVersions": items });
        if let Some(token) = next_token {
            resp["NextToken"] = json!(token);
        }
        Ok(AwsResponse::ok_json(resp))
    }

    pub(super) fn update_association_status(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let name = body["Name"].as_str().ok_or_else(|| missing("Name"))?;
        let instance_id = body["InstanceId"]
            .as_str()
            .ok_or_else(|| missing("InstanceId"))?;
        validate_required("AssociationStatus", &body["AssociationStatus"])?;
        let association_status = &body["AssociationStatus"];
        let new_status = association_status["Name"]
            .as_str()
            .unwrap_or("Pending")
            .to_string();

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let assoc = state
            .associations
            .values_mut()
            .find(|a| a.name == name && a.instance_id.as_deref() == Some(instance_id))
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "AssociationDoesNotExist",
                    "The specified association does not exist.".to_string(),
                )
            })?;

        assoc.status = new_status;
        assoc.status_date = Utc::now();

        let resp = association_to_json(assoc);
        Ok(AwsResponse::ok_json(
            json!({ "AssociationDescription": resp }),
        ))
    }

    pub(super) fn start_associations_once(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let association_ids = body["AssociationIds"]
            .as_array()
            .ok_or_else(|| missing("AssociationIds"))?;

        let ids: Vec<String> = association_ids
            .iter()
            .filter_map(|v| v.as_str().map(|s| s.to_string()))
            .collect();

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);

        // Validate every association exists before mutating any of them so we
        // never leave half the batch flipped to Pending on a failed call.
        for id in &ids {
            if !state.associations.contains_key(id) {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "AssociationDoesNotExist",
                    format!("The specified association {id} does not exist."),
                ));
            }
        }

        let now = Utc::now();
        for id in &ids {
            if let Some(assoc) = state.associations.get_mut(id) {
                assoc.status = "Pending".to_string();
                assoc.status_date = now;
                assoc.last_update_association_date = now;
                assoc.last_execution_date = Some(now);
            }
        }
        Ok(AwsResponse::ok_json(json!({})))
    }

    pub(super) fn create_association_batch(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        validate_optional_string_length(
            "AssociationDispatchAssumeRole",
            body["AssociationDispatchAssumeRole"].as_str(),
            1,
            512,
        )?;
        let entries = body["Entries"]
            .as_array()
            .ok_or_else(|| missing("Entries"))?;

        let mut successful = Vec::new();
        let mut failed = Vec::new();

        for entry in entries {
            match self.create_association_inner(entry, &req.account_id) {
                Ok(desc) => successful.push(desc),
                Err(e) => {
                    let entry_name = entry["Name"].as_str().unwrap_or("");
                    failed.push(json!({
                        "Entry": entry,
                        "Message": e.to_string(),
                        "Fault": "Client",
                    }));
                    let _ = entry_name; // suppress unused
                }
            }
        }

        Ok(AwsResponse::ok_json(json!({
            "Successful": successful,
            "Failed": failed,
        })))
    }

    pub(super) fn describe_association_executions(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        validate_optional_range_i64("MaxResults", body["MaxResults"].as_i64(), 1, 50)?;
        let _association_id = body["AssociationId"]
            .as_str()
            .ok_or_else(|| missing("AssociationId"))?;
        // Return empty list — associations don't actually run
        Ok(AwsResponse::ok_json(json!({ "AssociationExecutions": [] })))
    }

    pub(super) fn describe_association_execution_targets(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        validate_optional_range_i64("MaxResults", body["MaxResults"].as_i64(), 1, 50)?;
        let _association_id = body["AssociationId"]
            .as_str()
            .ok_or_else(|| missing("AssociationId"))?;
        let _execution_id = body["ExecutionId"]
            .as_str()
            .ok_or_else(|| missing("ExecutionId"))?;
        Ok(AwsResponse::ok_json(
            json!({ "AssociationExecutionTargets": [] }),
        ))
    }

    pub(super) fn describe_effective_instance_associations(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        validate_optional_range_i64("MaxResults", body["MaxResults"].as_i64(), 1, 5)?;
        let instance_id = body["InstanceId"]
            .as_str()
            .ok_or_else(|| missing("InstanceId"))?;

        let accounts = self.state.read();
        let empty = SsmState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
        let associations: Vec<Value> = state
            .associations
            .values()
            .filter(|a| {
                // Match by direct instance_id or by targets containing the instance
                a.instance_id.as_deref() == Some(instance_id)
                    || a.targets.iter().any(|t| {
                        t["Key"].as_str() == Some("InstanceIds")
                            && t["Values"].as_array().is_some_and(|vals| {
                                vals.iter().any(|v| v.as_str() == Some(instance_id))
                            })
                    })
            })
            .map(|a| {
                json!({
                    "AssociationId": a.association_id,
                    "InstanceId": instance_id,
                    "Content": a.name,
                    "AssociationVersion": a.versions.len().to_string(),
                })
            })
            .collect();

        Ok(AwsResponse::ok_json(
            json!({ "Associations": associations }),
        ))
    }

    pub(super) fn describe_instance_associations_status(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        validate_optional_range_i64("MaxResults", body["MaxResults"].as_i64(), 1, 50)?;
        let instance_id = body["InstanceId"]
            .as_str()
            .ok_or_else(|| missing("InstanceId"))?;

        let accounts = self.state.read();
        let empty = SsmState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
        let statuses: Vec<Value> = state
            .associations
            .values()
            .filter(|a| {
                a.instance_id.as_deref() == Some(instance_id)
                    || a.targets.iter().any(|t| {
                        t["Key"].as_str() == Some("InstanceIds")
                            && t["Values"].as_array().is_some_and(|vals| {
                                vals.iter().any(|v| v.as_str() == Some(instance_id))
                            })
                    })
            })
            .map(|a| {
                json!({
                    "AssociationId": a.association_id,
                    "Name": a.name,
                    "InstanceId": instance_id,
                    "AssociationVersion": a.versions.len().to_string(),
                    "ExecutionDate": a.status_date.timestamp_millis() as f64 / 1000.0,
                    "Status": a.status,
                    "DetailedStatus": a.status,
                    "ExecutionSummary": format!("1 out of 1 plugin processed, 1 success"),
                })
            })
            .collect();

        Ok(AwsResponse::ok_json(
            json!({ "InstanceAssociationStatusInfos": statuses }),
        ))
    }
}

/// Parsed + validated inputs for `CreateAssociation`.
struct CreateAssociationInput {
    name: String,
    targets: Vec<Value>,
    instance_id: Option<String>,
    schedule_expression: Option<String>,
    parameters: BTreeMap<String, Vec<String>>,
    association_name: Option<String>,
    document_version: Option<String>,
    output_location: Option<Value>,
    automation_target_parameter_name: Option<String>,
    max_errors: Option<String>,
    max_concurrency: Option<String>,
    compliance_severity: Option<String>,
    sync_compliance: Option<String>,
    apply_only_at_cron_interval: bool,
    calendar_names: Vec<String>,
    target_locations: Vec<Value>,
    schedule_offset: Option<i64>,
    target_maps: Vec<Value>,
    tags: BTreeMap<String, String>,
}

impl CreateAssociationInput {
    fn from_body(body: &Value) -> Result<Self, AwsServiceError> {
        validate_optional_string_length(
            "AssociationDispatchAssumeRole",
            body["AssociationDispatchAssumeRole"].as_str(),
            1,
            512,
        )?;
        validate_optional_string_length(
            "AutomationTargetParameterName",
            body["AutomationTargetParameterName"].as_str(),
            1,
            50,
        )?;
        validate_optional_string_length(
            "ScheduleExpression",
            body["ScheduleExpression"].as_str(),
            1,
            256,
        )?;
        validate_optional_string_length("MaxConcurrency", body["MaxConcurrency"].as_str(), 1, 7)?;
        validate_optional_string_length("MaxErrors", body["MaxErrors"].as_str(), 1, 7)?;
        validate_optional_enum(
            "ComplianceSeverity",
            body["ComplianceSeverity"].as_str(),
            &["Critical", "High", "Medium", "Low", "Unspecified"],
        )?;
        validate_optional_enum(
            "SyncCompliance",
            body["SyncCompliance"].as_str(),
            &["Auto", "Manual"],
        )?;
        validate_optional_range_i64("Duration", body["Duration"].as_i64(), 1, 24)?;
        validate_optional_range_i64("ScheduleOffset", body["ScheduleOffset"].as_i64(), 1, 6)?;

        let name = body["Name"]
            .as_str()
            .ok_or_else(|| missing("Name"))?
            .to_string();

        let parameters: BTreeMap<String, Vec<String>> = body["Parameters"]
            .as_object()
            .map(|obj| {
                obj.iter()
                    .map(|(k, v)| {
                        let vals = v
                            .as_array()
                            .map(|a| {
                                a.iter()
                                    .filter_map(|x| x.as_str().map(|s| s.to_string()))
                                    .collect()
                            })
                            .unwrap_or_default();
                        (k.clone(), vals)
                    })
                    .collect()
            })
            .unwrap_or_default();

        let calendar_names: Vec<String> = body["CalendarNames"]
            .as_array()
            .map(|a| {
                a.iter()
                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                    .collect()
            })
            .unwrap_or_default();

        let tags: BTreeMap<String, String> = body["Tags"]
            .as_array()
            .map(|arr| {
                arr.iter()
                    .filter_map(|t| {
                        let k = t["Key"].as_str()?;
                        let v = t["Value"].as_str()?;
                        Some((k.to_string(), v.to_string()))
                    })
                    .collect()
            })
            .unwrap_or_default();

        Ok(Self {
            name,
            targets: body["Targets"].as_array().cloned().unwrap_or_default(),
            instance_id: body["InstanceId"].as_str().map(|s| s.to_string()),
            schedule_expression: body["ScheduleExpression"].as_str().map(|s| s.to_string()),
            parameters,
            association_name: body["AssociationName"].as_str().map(|s| s.to_string()),
            document_version: body["DocumentVersion"].as_str().map(|s| s.to_string()),
            output_location: body.get("OutputLocation").filter(|v| !v.is_null()).cloned(),
            automation_target_parameter_name: body["AutomationTargetParameterName"]
                .as_str()
                .map(|s| s.to_string()),
            max_errors: body["MaxErrors"].as_str().map(|s| s.to_string()),
            max_concurrency: body["MaxConcurrency"].as_str().map(|s| s.to_string()),
            compliance_severity: body["ComplianceSeverity"].as_str().map(|s| s.to_string()),
            sync_compliance: body["SyncCompliance"].as_str().map(|s| s.to_string()),
            apply_only_at_cron_interval: body["ApplyOnlyAtCronInterval"].as_bool().unwrap_or(false),
            calendar_names,
            target_locations: body["TargetLocations"]
                .as_array()
                .cloned()
                .unwrap_or_default(),
            schedule_offset: body["ScheduleOffset"].as_i64(),
            target_maps: body["TargetMaps"].as_array().cloned().unwrap_or_default(),
            tags,
        })
    }
}

pub(super) fn association_to_json(a: &SsmAssociation) -> Value {
    let mut v = json!({
        "AssociationId": a.association_id,
        "Name": a.name,
        "AssociationVersion": a.versions.len().to_string(),
        "Date": a.created_date.timestamp_millis() as f64 / 1000.0,
        "LastUpdateAssociationDate": a.last_update_association_date.timestamp_millis() as f64 / 1000.0,
        "Status": {
            "Date": a.status_date.timestamp_millis() as f64 / 1000.0,
            "Name": a.status,
            "Message": "",
            "AdditionalInfo": "",
        },
        "Overview": a.overview,
        "ApplyOnlyAtCronInterval": a.apply_only_at_cron_interval,
    });
    if !a.targets.is_empty() {
        v["Targets"] = json!(a.targets);
    }
    if let Some(ref s) = a.schedule_expression {
        v["ScheduleExpression"] = json!(s);
    }
    if !a.parameters.is_empty() {
        v["Parameters"] = json!(a.parameters);
    }
    if let Some(ref an) = a.association_name {
        v["AssociationName"] = json!(an);
    }
    if let Some(ref dv) = a.document_version {
        v["DocumentVersion"] = json!(dv);
    }
    if let Some(ref ol) = a.output_location {
        v["OutputLocation"] = ol.clone();
    }
    if let Some(ref me) = a.max_errors {
        v["MaxErrors"] = json!(me);
    }
    if let Some(ref mc) = a.max_concurrency {
        v["MaxConcurrency"] = json!(mc);
    }
    if let Some(ref cs) = a.compliance_severity {
        v["ComplianceSeverity"] = json!(cs);
    }
    if let Some(ref sc) = a.sync_compliance {
        v["SyncCompliance"] = json!(sc);
    }
    if let Some(ref iid) = a.instance_id {
        v["InstanceId"] = json!(iid);
    }
    if let Some(so) = a.schedule_offset {
        v["ScheduleOffset"] = json!(so);
    }
    if let Some(ref led) = a.last_execution_date {
        v["LastExecutionDate"] = json!(led.timestamp_millis() as f64 / 1000.0);
    }
    v
}
