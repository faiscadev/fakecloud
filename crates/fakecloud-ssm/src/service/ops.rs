use std::collections::HashMap;

use chrono::Utc;
use http::StatusCode;
use serde_json::{json, Value};

use fakecloud_aws::arn::Arn;
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};
use fakecloud_core::validation::*;

use crate::state::{OpsItemRelatedItem, OpsMetadataEntry, SsmOpsItem};

use super::{json_resp, missing, parse_body, SsmService};

impl SsmService {
    pub(super) fn create_ops_item(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        validate_required("Description", &body["Description"])?;
        validate_optional_string_length("Title", body["Title"].as_str(), 1, 1024)?;
        validate_optional_string_length("Source", body["Source"].as_str(), 1, 128)?;
        validate_optional_string_length("Description", body["Description"].as_str(), 1, 2048)?;
        validate_optional_string_length("Category", body["Category"].as_str(), 1, 64)?;
        validate_optional_string_length("Severity", body["Severity"].as_str(), 1, 64)?;
        validate_optional_range_i64("Priority", body["Priority"].as_i64(), 1, 5)?;
        let title = body["Title"]
            .as_str()
            .ok_or_else(|| missing("Title"))?
            .to_string();
        let source = body["Source"]
            .as_str()
            .ok_or_else(|| missing("Source"))?
            .to_string();
        let description = body["Description"].as_str().map(|s| s.to_string());
        let priority = body["Priority"].as_i64();
        let severity = body["Severity"].as_str().map(|s| s.to_string());
        let category = body["Category"].as_str().map(|s| s.to_string());
        let ops_item_type = body["OpsItemType"].as_str().map(|s| s.to_string());
        let operational_data: HashMap<String, serde_json::Value> = body["OperationalData"]
            .as_object()
            .map(|obj| obj.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
            .unwrap_or_default();
        let notifications: Vec<serde_json::Value> = body["Notifications"]
            .as_array()
            .cloned()
            .unwrap_or_default();
        let related_ops_items: Vec<serde_json::Value> = body["RelatedOpsItems"]
            .as_array()
            .cloned()
            .unwrap_or_default();
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
        let mut state = self.state.write();
        state.ops_item_counter += 1;
        let ops_item_id = format!("oi-{:012x}", state.ops_item_counter);

        let item = SsmOpsItem {
            ops_item_id: ops_item_id.clone(),
            title,
            description,
            source,
            status: "Open".to_string(),
            priority,
            severity,
            category,
            operational_data,
            notifications,
            related_ops_items,
            tags,
            created_time: now,
            last_modified_time: now,
            created_by: Arn::global("iam", &state.account_id, "root").to_string(),
            last_modified_by: Arn::global("iam", &state.account_id, "root").to_string(),
            ops_item_type,
            planned_start_time: None,
            planned_end_time: None,
            actual_start_time: None,
            actual_end_time: None,
        };

        state.ops_items.insert(ops_item_id.clone(), item);

        Ok(json_resp(json!({ "OpsItemId": ops_item_id })))
    }

    pub(super) fn get_ops_item(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let ops_item_id = body["OpsItemId"]
            .as_str()
            .ok_or_else(|| missing("OpsItemId"))?;

        let state = self.state.read();
        let item = state.ops_items.get(ops_item_id).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "OpsItemNotFoundException",
                format!("OpsItem ID {ops_item_id} not found"),
            )
        })?;

        Ok(json_resp(json!({ "OpsItem": ops_item_to_json(item) })))
    }

    pub(super) fn update_ops_item(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let ops_item_id = body["OpsItemId"]
            .as_str()
            .ok_or_else(|| missing("OpsItemId"))?;

        let mut state = self.state.write();
        let account_id = state.account_id.clone();
        let item = state.ops_items.get_mut(ops_item_id).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "OpsItemNotFoundException",
                format!("OpsItem ID {ops_item_id} not found"),
            )
        })?;

        if let Some(t) = body["Title"].as_str() {
            item.title = t.to_string();
        }
        if let Some(d) = body["Description"].as_str() {
            item.description = Some(d.to_string());
        }
        if let Some(s) = body["Status"].as_str() {
            item.status = s.to_string();
        }
        if let Some(p) = body["Priority"].as_i64() {
            item.priority = Some(p);
        }
        if let Some(s) = body["Severity"].as_str() {
            item.severity = Some(s.to_string());
        }
        if let Some(c) = body["Category"].as_str() {
            item.category = Some(c.to_string());
        }
        if let Some(obj) = body["OperationalData"].as_object() {
            for (k, v) in obj {
                item.operational_data.insert(k.clone(), v.clone());
            }
        }
        if let Some(arr) = body["Notifications"].as_array() {
            item.notifications = arr.clone();
        }
        if let Some(arr) = body["RelatedOpsItems"].as_array() {
            item.related_ops_items = arr.clone();
        }

        item.last_modified_time = Utc::now();
        item.last_modified_by = Arn::global("iam", &account_id, "root").to_string();

        Ok(json_resp(json!({})))
    }

    pub(super) fn delete_ops_item(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let ops_item_id = body["OpsItemId"]
            .as_str()
            .ok_or_else(|| missing("OpsItemId"))?;

        let mut state = self.state.write();
        state.ops_items.remove(ops_item_id);
        Ok(json_resp(json!({})))
    }

    pub(super) fn describe_ops_items(
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
            .ops_items
            .values()
            .map(|item| {
                let mut v = json!({
                    "OpsItemId": item.ops_item_id,
                    "Title": item.title,
                    "Status": item.status,
                    "Source": item.source,
                    "CreatedTime": item.created_time.timestamp_millis() as f64 / 1000.0,
                    "LastModifiedTime": item.last_modified_time.timestamp_millis() as f64 / 1000.0,
                    "CreatedBy": item.created_by,
                    "LastModifiedBy": item.last_modified_by,
                });
                if let Some(p) = item.priority {
                    v["Priority"] = json!(p);
                }
                if let Some(ref s) = item.severity {
                    v["Severity"] = json!(s);
                }
                if let Some(ref c) = item.category {
                    v["Category"] = json!(c);
                }
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

        let mut resp = json!({ "OpsItemSummaries": items });
        if has_more {
            resp["NextToken"] = json!((next_token_offset + max_results).to_string());
        }
        Ok(json_resp(resp))
    }

    // -----------------------------------------------------------------------
    // Document extras
    // -----------------------------------------------------------------------

    pub(super) fn get_ops_summary(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        validate_optional_string_length("SyncName", body["SyncName"].as_str(), 1, 64)?;
        validate_optional_range_i64("MaxResults", body["MaxResults"].as_i64(), 1, 50)?;
        Ok(json_resp(json!({ "Entities": [] })))
    }

    // ── OpsItem Related Items ─────────────────────────────────────

    pub(super) fn associate_ops_item_related_item(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let ops_item_id = body["OpsItemId"]
            .as_str()
            .ok_or_else(|| missing("OpsItemId"))?
            .to_string();
        let association_type = body["AssociationType"]
            .as_str()
            .ok_or_else(|| missing("AssociationType"))?
            .to_string();
        let resource_type = body["ResourceType"]
            .as_str()
            .ok_or_else(|| missing("ResourceType"))?
            .to_string();
        let resource_uri = body["ResourceUri"]
            .as_str()
            .ok_or_else(|| missing("ResourceUri"))?
            .to_string();

        let now = Utc::now();
        let mut state = self.state.write();

        // Verify ops item exists
        if !state.ops_items.contains_key(&ops_item_id) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "OpsItemNotFoundException",
                format!("OpsItem ID {ops_item_id} not found"),
            ));
        }

        state.ops_item_related_item_counter += 1;
        let association_id = format!("oiri-{:012x}", state.ops_item_related_item_counter);
        let account_id = state.account_id.clone();

        state.ops_item_related_items.push(OpsItemRelatedItem {
            association_id: association_id.clone(),
            ops_item_id,
            association_type,
            resource_type,
            resource_uri,
            created_time: now,
            created_by: Arn::global("iam", &account_id, "root").to_string(),
            last_modified_time: now,
            last_modified_by: Arn::global("iam", &account_id, "root").to_string(),
        });

        Ok(json_resp(json!({ "AssociationId": association_id })))
    }

    pub(super) fn disassociate_ops_item_related_item(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let ops_item_id = body["OpsItemId"]
            .as_str()
            .ok_or_else(|| missing("OpsItemId"))?;
        let association_id = body["AssociationId"]
            .as_str()
            .ok_or_else(|| missing("AssociationId"))?;

        let mut state = self.state.write();
        let before = state.ops_item_related_items.len();
        state
            .ops_item_related_items
            .retain(|ri| !(ri.ops_item_id == ops_item_id && ri.association_id == association_id));
        if state.ops_item_related_items.len() == before {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "OpsItemRelatedItemAssociationNotFoundException",
                format!("Association {association_id} not found for OpsItem {ops_item_id}"),
            ));
        }

        Ok(json_resp(json!({})))
    }

    pub(super) fn list_ops_item_related_items(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        validate_optional_range_i64("MaxResults", body["MaxResults"].as_i64(), 1, 50)?;
        let state = self.state.read();
        let ops_item_id = body["OpsItemId"].as_str();

        let items: Vec<Value> = state
            .ops_item_related_items
            .iter()
            .filter(|ri| ops_item_id.is_none_or(|id| ri.ops_item_id == id))
            .map(|ri| {
                json!({
                    "OpsItemId": ri.ops_item_id,
                    "AssociationId": ri.association_id,
                    "AssociationType": ri.association_type,
                    "ResourceType": ri.resource_type,
                    "ResourceUri": ri.resource_uri,
                    "CreatedTime": ri.created_time.timestamp() as f64,
                    "CreatedBy": { "Arn": ri.created_by },
                    "LastModifiedTime": ri.last_modified_time.timestamp() as f64,
                    "LastModifiedBy": { "Arn": ri.last_modified_by },
                })
            })
            .collect();

        Ok(json_resp(json!({ "Summaries": items })))
    }

    pub(super) fn list_ops_item_events(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        validate_optional_range_i64("MaxResults", body["MaxResults"].as_i64(), 1, 50)?;
        let state = self.state.read();

        // Filter by OpsItemId if provided in Filters
        let filter_id = body["Filters"].as_array().and_then(|filters| {
            filters.iter().find_map(|f| {
                if f["Key"].as_str() == Some("OpsItemId") {
                    f["Values"]
                        .as_array()
                        .and_then(|v| v.first())
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string())
                } else {
                    None
                }
            })
        });

        let events: Vec<Value> = state
            .ops_item_events
            .iter()
            .filter(|e| filter_id.as_ref().is_none_or(|id| e.ops_item_id == *id))
            .map(|e| {
                json!({
                    "OpsItemId": e.ops_item_id,
                    "EventId": e.event_id,
                    "Source": e.source,
                    "DetailType": e.detail_type,
                    "CreatedTime": e.created_time.timestamp_millis() as f64 / 1000.0,
                    "CreatedBy": e.created_by,
                })
            })
            .collect();

        Ok(json_resp(json!({ "Summaries": events })))
    }

    // ── OpsMetadata ───────────────────────────────────────────────

    pub(super) fn create_ops_metadata(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        validate_optional_string_length("ResourceId", body["ResourceId"].as_str(), 1, 1024)?;
        let resource_id = body["ResourceId"]
            .as_str()
            .ok_or_else(|| missing("ResourceId"))?
            .to_string();
        let metadata: HashMap<String, serde_json::Value> = body["Metadata"]
            .as_object()
            .map(|m| m.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
            .unwrap_or_default();

        let mut state = self.state.write();
        let arn = format!(
            "arn:aws:ssm:{}:{}:opsmetadata/{}",
            state.region, state.account_id, resource_id
        );

        if state.ops_metadata.contains_key(&arn) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "OpsMetadataAlreadyExistsException",
                format!("OpsMetadata for {resource_id} already exists"),
            ));
        }

        let entry = OpsMetadataEntry {
            ops_metadata_arn: arn.clone(),
            resource_id,
            metadata,
            creation_date: Utc::now(),
        };
        state.ops_metadata.insert(arn.clone(), entry);

        Ok(json_resp(json!({ "OpsMetadataArn": arn })))
    }

    pub(super) fn get_ops_metadata(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let arn = body["OpsMetadataArn"]
            .as_str()
            .ok_or_else(|| missing("OpsMetadataArn"))?;

        let state = self.state.read();
        let entry = state.ops_metadata.get(arn).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "OpsMetadataNotFoundException",
                format!("OpsMetadata {arn} not found"),
            )
        })?;

        Ok(json_resp(json!({
            "ResourceId": entry.resource_id,
            "Metadata": entry.metadata,
        })))
    }

    pub(super) fn update_ops_metadata(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let arn = body["OpsMetadataArn"]
            .as_str()
            .ok_or_else(|| missing("OpsMetadataArn"))?;

        let mut state = self.state.write();
        let entry = state.ops_metadata.get_mut(arn).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "OpsMetadataNotFoundException",
                format!("OpsMetadata {arn} not found"),
            )
        })?;

        if let Some(to_add) = body["MetadataToUpdate"].as_object() {
            for (k, v) in to_add {
                entry.metadata.insert(k.clone(), v.clone());
            }
        }
        if let Some(to_del) = body["KeysToDelete"].as_array() {
            for k in to_del {
                if let Some(key) = k.as_str() {
                    entry.metadata.remove(key);
                }
            }
        }

        Ok(json_resp(json!({ "OpsMetadataArn": arn })))
    }

    pub(super) fn delete_ops_metadata(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let arn = body["OpsMetadataArn"]
            .as_str()
            .ok_or_else(|| missing("OpsMetadataArn"))?;

        let mut state = self.state.write();
        if state.ops_metadata.remove(arn).is_none() {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "OpsMetadataNotFoundException",
                format!("OpsMetadata {arn} not found"),
            ));
        }

        Ok(json_resp(json!({})))
    }

    pub(super) fn list_ops_metadata(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        validate_optional_range_i64("MaxResults", body["MaxResults"].as_i64(), 1, 50)?;
        let state = self.state.read();
        let items: Vec<Value> = state
            .ops_metadata
            .values()
            .map(|e| {
                json!({
                    "OpsMetadataArn": e.ops_metadata_arn,
                    "ResourceId": e.resource_id,
                    "CreationDate": e.creation_date.timestamp_millis() as f64 / 1000.0,
                })
            })
            .collect();

        Ok(json_resp(json!({ "OpsMetadataList": items })))
    }

    // ── Automation ────────────────────────────────────────────────
}

pub(super) fn ops_item_to_json(item: &SsmOpsItem) -> Value {
    json!({
        "OpsItemId": item.ops_item_id,
        "Title": item.title,
        "Description": item.description,
        "Source": item.source,
        "Status": item.status,
        "Priority": item.priority,
        "Severity": item.severity,
        "Category": item.category,
        "OperationalData": item.operational_data,
        "Notifications": item.notifications,
        "RelatedOpsItems": item.related_ops_items,
        "CreatedTime": item.created_time.timestamp_millis() as f64 / 1000.0,
        "LastModifiedTime": item.last_modified_time.timestamp_millis() as f64 / 1000.0,
        "CreatedBy": item.created_by,
        "LastModifiedBy": item.last_modified_by,
        "OpsItemType": item.ops_item_type,
    })
}
