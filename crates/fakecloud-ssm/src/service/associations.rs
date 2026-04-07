use std::collections::HashMap;

use chrono::Utc;
use http::StatusCode;
use serde_json::{json, Value};

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};
use fakecloud_core::validation::*;

use crate::state::{SsmAssociation, SsmAssociationVersion};

use super::{json_resp, missing, parse_body, SsmService};

impl SsmService {
    pub(super) fn create_association_inner(&self, body: &Value) -> Result<Value, AwsServiceError> {
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

        let targets: Vec<serde_json::Value> =
            body["Targets"].as_array().cloned().unwrap_or_default();
        let instance_id = body["InstanceId"].as_str().map(|s| s.to_string());

        // Must have either Targets or InstanceId
        if targets.is_empty() && instance_id.is_none() {
            // Accept it anyway like AWS does for document-only associations
        }

        let schedule_expression = body["ScheduleExpression"].as_str().map(|s| s.to_string());
        let parameters: HashMap<String, Vec<String>> = body["Parameters"]
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
        let association_name = body["AssociationName"].as_str().map(|s| s.to_string());
        let document_version = body["DocumentVersion"].as_str().map(|s| s.to_string());
        let output_location = body.get("OutputLocation").filter(|v| !v.is_null()).cloned();
        let automation_target_parameter_name = body["AutomationTargetParameterName"]
            .as_str()
            .map(|s| s.to_string());
        let max_errors = body["MaxErrors"].as_str().map(|s| s.to_string());
        let max_concurrency = body["MaxConcurrency"].as_str().map(|s| s.to_string());
        let compliance_severity = body["ComplianceSeverity"].as_str().map(|s| s.to_string());
        let sync_compliance = body["SyncCompliance"].as_str().map(|s| s.to_string());
        let apply_only_at_cron_interval =
            body["ApplyOnlyAtCronInterval"].as_bool().unwrap_or(false);
        let calendar_names: Vec<String> = body["CalendarNames"]
            .as_array()
            .map(|a| {
                a.iter()
                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                    .collect()
            })
            .unwrap_or_default();
        let target_locations: Vec<serde_json::Value> = body["TargetLocations"]
            .as_array()
            .cloned()
            .unwrap_or_default();
        let schedule_offset = body["ScheduleOffset"].as_i64();
        let target_maps: Vec<serde_json::Value> =
            body["TargetMaps"].as_array().cloned().unwrap_or_default();
        let tags: HashMap<String, String> = body["Tags"]
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

        let now = Utc::now();
        let association_id = uuid::Uuid::new_v4().to_string();

        let version = SsmAssociationVersion {
            version: 1,
            name: name.clone(),
            targets: targets.clone(),
            schedule_expression: schedule_expression.clone(),
            parameters: parameters.clone(),
            document_version: document_version.clone(),
            created_date: now,
            association_name: association_name.clone(),
            max_errors: max_errors.clone(),
            max_concurrency: max_concurrency.clone(),
            compliance_severity: compliance_severity.clone(),
        };

        let assoc = SsmAssociation {
            association_id: association_id.clone(),
            name: name.clone(),
            targets: targets.clone(),
            schedule_expression,
            parameters,
            association_name: association_name.clone(),
            document_version,
            output_location,
            automation_target_parameter_name,
            max_errors,
            max_concurrency,
            compliance_severity,
            sync_compliance,
            apply_only_at_cron_interval,
            calendar_names,
            target_locations,
            schedule_offset,
            target_maps,
            tags,
            status: "Pending".to_string(),
            status_date: now,
            overview: json!({"Status": "Pending", "DetailedStatus": "Creating", "AssociationStatusAggregatedCount": {}}),
            created_date: now,
            last_update_association_date: now,
            last_execution_date: None,
            instance_id,
            versions: vec![version],
        };

        let resp = association_to_json(&assoc);

        let mut state = self.state.write();
        state.associations.insert(association_id, assoc);

        Ok(resp)
    }

    pub(super) fn create_association(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let resp = self.create_association_inner(&body)?;
        Ok(json_resp(json!({ "AssociationDescription": resp })))
    }

    pub(super) fn describe_association(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let association_id = body["AssociationId"].as_str();
        let name = body["Name"].as_str();
        let instance_id = body["InstanceId"].as_str();

        let state = self.state.read();

        let assoc = if let Some(id) = association_id {
            state.associations.get(id)
        } else if let Some(n) = name {
            state.associations.values().find(|a| {
                a.name == n && (instance_id.is_none() || a.instance_id.as_deref() == instance_id)
            })
        } else {
            return Err(missing("AssociationId"));
        };

        match assoc {
            Some(a) => Ok(json_resp(
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
        let body = parse_body(req);
        let association_id = body["AssociationId"].as_str();
        let name = body["Name"].as_str();
        let instance_id = body["InstanceId"].as_str();

        let mut state = self.state.write();

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
            return Err(missing("AssociationId"));
        };

        match key {
            Some(k) => {
                state.associations.remove(&k);
                Ok(json_resp(json!({})))
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
        let body = parse_body(req);
        validate_optional_range_i64("MaxResults", body["MaxResults"].as_i64(), 1, 50)?;
        let max_results = body["MaxResults"].as_i64().unwrap_or(50) as usize;
        let next_token_offset: usize = body["NextToken"]
            .as_str()
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);

        let state = self.state.read();
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

        let page = if next_token_offset < all.len() {
            &all[next_token_offset..]
        } else {
            &[]
        };
        let has_more = page.len() > max_results;
        let items: Vec<Value> = page.iter().take(max_results).cloned().collect();

        let mut resp = json!({ "Associations": items });
        if has_more {
            resp["NextToken"] = json!((next_token_offset + max_results).to_string());
        }
        Ok(json_resp(resp))
    }

    pub(super) fn update_association(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let association_id = body["AssociationId"]
            .as_str()
            .ok_or_else(|| missing("AssociationId"))?;

        let mut state = self.state.write();
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
        Ok(json_resp(json!({ "AssociationDescription": resp })))
    }

    pub(super) fn list_association_versions(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let association_id = body["AssociationId"]
            .as_str()
            .ok_or_else(|| missing("AssociationId"))?;
        let max_results = body["MaxResults"].as_i64().unwrap_or(50) as usize;
        let next_token_offset: usize = body["NextToken"]
            .as_str()
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);

        let state = self.state.read();
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

        let page = if next_token_offset < all.len() {
            &all[next_token_offset..]
        } else {
            &[]
        };
        let has_more = page.len() > max_results;
        let items: Vec<Value> = page.iter().take(max_results).cloned().collect();

        let mut resp = json!({ "AssociationVersions": items });
        if has_more {
            resp["NextToken"] = json!((next_token_offset + max_results).to_string());
        }
        Ok(json_resp(resp))
    }

    pub(super) fn update_association_status(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
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

        let mut state = self.state.write();
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
        Ok(json_resp(json!({ "AssociationDescription": resp })))
    }

    pub(super) fn start_associations_once(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let _association_ids = body["AssociationIds"]
            .as_array()
            .ok_or_else(|| missing("AssociationIds"))?;
        // No-op: return success
        Ok(json_resp(json!({})))
    }

    pub(super) fn create_association_batch(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
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
            match self.create_association_inner(entry) {
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

        Ok(json_resp(json!({
            "Successful": successful,
            "Failed": failed,
        })))
    }

    pub(super) fn describe_association_executions(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        validate_optional_range_i64("MaxResults", body["MaxResults"].as_i64(), 1, 50)?;
        let _association_id = body["AssociationId"]
            .as_str()
            .ok_or_else(|| missing("AssociationId"))?;
        // Return empty list — associations don't actually run
        Ok(json_resp(json!({ "AssociationExecutions": [] })))
    }

    pub(super) fn describe_association_execution_targets(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        validate_optional_range_i64("MaxResults", body["MaxResults"].as_i64(), 1, 50)?;
        let _association_id = body["AssociationId"]
            .as_str()
            .ok_or_else(|| missing("AssociationId"))?;
        let _execution_id = body["ExecutionId"]
            .as_str()
            .ok_or_else(|| missing("ExecutionId"))?;
        Ok(json_resp(json!({ "AssociationExecutionTargets": [] })))
    }

    // -----------------------------------------------------------------------
    // OpsItems
    // -----------------------------------------------------------------------

    pub(super) fn describe_effective_instance_associations(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        validate_optional_range_i64("MaxResults", body["MaxResults"].as_i64(), 1, 5)?;
        let instance_id = body["InstanceId"]
            .as_str()
            .ok_or_else(|| missing("InstanceId"))?;

        let state = self.state.read();
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

        Ok(json_resp(json!({ "Associations": associations })))
    }

    pub(super) fn describe_instance_associations_status(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        validate_optional_range_i64("MaxResults", body["MaxResults"].as_i64(), 1, 50)?;
        let instance_id = body["InstanceId"]
            .as_str()
            .ok_or_else(|| missing("InstanceId"))?;

        let state = self.state.read();
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

        Ok(json_resp(
            json!({ "InstanceAssociationStatusInfos": statuses }),
        ))
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
