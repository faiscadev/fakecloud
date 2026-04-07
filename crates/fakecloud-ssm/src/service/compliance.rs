use std::collections::HashMap;

use serde_json::{json, Value};

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};
use fakecloud_core::validation::*;

use crate::state::ComplianceItem;

use super::{json_resp, missing, parse_body, SsmService};

impl SsmService {
    pub(super) fn put_compliance_items(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        validate_optional_string_length("ResourceId", body["ResourceId"].as_str(), 1, 100)?;
        validate_optional_string_length("ResourceType", body["ResourceType"].as_str(), 1, 50)?;
        validate_optional_string_length("ComplianceType", body["ComplianceType"].as_str(), 1, 100)?;
        validate_optional_string_length(
            "ItemContentHash",
            body["ItemContentHash"].as_str(),
            0,
            256,
        )?;
        validate_optional_enum(
            "UploadType",
            body["UploadType"].as_str(),
            &["COMPLETE", "PARTIAL"],
        )?;
        let resource_id = body["ResourceId"]
            .as_str()
            .ok_or_else(|| missing("ResourceId"))?
            .to_string();
        let resource_type = body["ResourceType"]
            .as_str()
            .ok_or_else(|| missing("ResourceType"))?
            .to_string();
        let compliance_type = body["ComplianceType"]
            .as_str()
            .ok_or_else(|| missing("ComplianceType"))?
            .to_string();
        let execution_summary = body
            .get("ExecutionSummary")
            .cloned()
            .ok_or_else(|| missing("ExecutionSummary"))?;
        let items = body["Items"].as_array().ok_or_else(|| missing("Items"))?;

        let mut state = self.state.write();

        // Remove existing compliance items for this resource/type
        state
            .compliance_items
            .retain(|c| !(c.resource_id == resource_id && c.compliance_type == compliance_type));

        for item in items {
            let severity = item["Severity"]
                .as_str()
                .unwrap_or("UNSPECIFIED")
                .to_string();
            let status = item["Status"].as_str().unwrap_or("COMPLIANT").to_string();
            let title = item["Title"].as_str().map(|s| s.to_string());
            let id = item["Id"].as_str().map(|s| s.to_string());
            let details: HashMap<String, String> = item["Details"]
                .as_object()
                .map(|obj| {
                    obj.iter()
                        .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                        .collect()
                })
                .unwrap_or_default();

            state.compliance_items.push(ComplianceItem {
                resource_id: resource_id.clone(),
                resource_type: resource_type.clone(),
                compliance_type: compliance_type.clone(),
                severity,
                status,
                title,
                id,
                details,
                execution_summary: execution_summary.clone(),
            });
        }

        Ok(json_resp(json!({})))
    }

    pub(super) fn list_compliance_items(
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

        let resource_ids: Vec<&str> = body["ResourceIds"]
            .as_array()
            .map(|a| a.iter().filter_map(|v| v.as_str()).collect())
            .unwrap_or_default();
        let resource_types: Vec<&str> = body["ResourceTypes"]
            .as_array()
            .map(|a| a.iter().filter_map(|v| v.as_str()).collect())
            .unwrap_or_default();

        let state = self.state.read();
        let all_items: Vec<Value> = state
            .compliance_items
            .iter()
            .filter(|c| {
                if !resource_ids.is_empty() && !resource_ids.contains(&c.resource_id.as_str()) {
                    return false;
                }
                if !resource_types.is_empty() && !resource_types.contains(&c.resource_type.as_str())
                {
                    return false;
                }
                true
            })
            .map(|c| {
                let mut v = json!({
                    "ResourceId": c.resource_id,
                    "ResourceType": c.resource_type,
                    "ComplianceType": c.compliance_type,
                    "Severity": c.severity,
                    "Status": c.status,
                    "ExecutionSummary": c.execution_summary,
                });
                if let Some(ref title) = c.title {
                    v["Title"] = json!(title);
                }
                if let Some(ref id) = c.id {
                    v["Id"] = json!(id);
                }
                if !c.details.is_empty() {
                    v["Details"] = json!(c.details);
                }
                v
            })
            .collect();

        let page = if next_token_offset < all_items.len() {
            &all_items[next_token_offset..]
        } else {
            &[]
        };
        let has_more = page.len() > max_results;
        let items: Vec<Value> = page.iter().take(max_results).cloned().collect();
        let mut resp = json!({ "ComplianceItems": items });
        if has_more {
            resp["NextToken"] = json!((next_token_offset + max_results).to_string());
        }
        Ok(json_resp(resp))
    }

    pub(super) fn list_compliance_summaries(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        validate_optional_range_i64("MaxResults", body["MaxResults"].as_i64(), 1, 50)?;
        let state = self.state.read();

        // Group by compliance_type
        let mut type_counts: HashMap<String, (i64, i64)> = HashMap::new(); // (compliant, non_compliant)
        for item in &state.compliance_items {
            let entry = type_counts
                .entry(item.compliance_type.clone())
                .or_insert((0, 0));
            if item.status == "COMPLIANT" {
                entry.0 += 1;
            } else {
                entry.1 += 1;
            }
        }

        let summaries: Vec<Value> = type_counts
            .iter()
            .map(|(ct, (compliant, non_compliant))| {
                json!({
                    "ComplianceType": ct,
                    "CompliantSummary": {
                        "CompliantCount": compliant,
                        "SeveritySummary": {"CriticalCount": 0, "HighCount": 0, "MediumCount": 0, "LowCount": 0, "InformationalCount": 0, "UnspecifiedCount": 0},
                    },
                    "NonCompliantSummary": {
                        "NonCompliantCount": non_compliant,
                        "SeveritySummary": {"CriticalCount": 0, "HighCount": 0, "MediumCount": 0, "LowCount": 0, "InformationalCount": 0, "UnspecifiedCount": 0},
                    },
                })
            })
            .collect();

        Ok(json_resp(json!({ "ComplianceSummaryItems": summaries })))
    }

    pub(super) fn list_resource_compliance_summaries(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        validate_optional_range_i64("MaxResults", body["MaxResults"].as_i64(), 1, 50)?;
        let state = self.state.read();

        // Group by resource_id
        let mut resource_status: HashMap<String, (String, String, i64, i64)> = HashMap::new();
        for item in &state.compliance_items {
            let entry = resource_status
                .entry(item.resource_id.clone())
                .or_insert_with(|| (item.resource_type.clone(), "COMPLIANT".to_string(), 0, 0));
            if item.status == "COMPLIANT" {
                entry.2 += 1;
            } else {
                entry.1 = "NON_COMPLIANT".to_string();
                entry.3 += 1;
            }
        }

        let summaries: Vec<Value> = resource_status
            .iter()
            .map(
                |(resource_id, (resource_type, status, compliant, non_compliant))| {
                    json!({
                        "ResourceId": resource_id,
                        "ResourceType": resource_type,
                        "Status": status,
                        "OverallSeverity": "UNSPECIFIED",
                        "CompliantSummary": {
                            "CompliantCount": compliant,
                            "SeveritySummary": {"CriticalCount": 0, "HighCount": 0, "MediumCount": 0, "LowCount": 0, "InformationalCount": 0, "UnspecifiedCount": 0},
                        },
                        "NonCompliantSummary": {
                            "NonCompliantCount": non_compliant,
                            "SeveritySummary": {"CriticalCount": 0, "HighCount": 0, "MediumCount": 0, "LowCount": 0, "InformationalCount": 0, "UnspecifiedCount": 0},
                        },
                    })
                },
            )
            .collect();

        Ok(json_resp(
            json!({ "ResourceComplianceSummaryItems": summaries }),
        ))
    }

    // ── Maintenance Window Details ────────────────────────────────
}
