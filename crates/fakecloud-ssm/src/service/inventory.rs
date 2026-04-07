use std::collections::HashMap;

use chrono::Utc;
use serde_json::{json, Value};

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};
use fakecloud_core::validation::*;

use crate::state::{InventoryDeletion, InventoryEntry, InventoryItem};

use super::{json_resp, missing, parse_body, SsmService};

impl SsmService {
    pub(super) fn put_inventory(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let instance_id = body["InstanceId"]
            .as_str()
            .ok_or_else(|| missing("InstanceId"))?
            .to_string();
        let items = body["Items"].as_array().ok_or_else(|| missing("Items"))?;

        let mut inv_items = Vec::new();
        for item in items {
            let type_name = item["TypeName"]
                .as_str()
                .ok_or_else(|| missing("TypeName"))?
                .to_string();
            let schema_version = item["SchemaVersion"]
                .as_str()
                .ok_or_else(|| missing("SchemaVersion"))?
                .to_string();
            let capture_time = item["CaptureTime"]
                .as_str()
                .ok_or_else(|| missing("CaptureTime"))?
                .to_string();
            let content: Vec<HashMap<String, String>> = item["Content"]
                .as_array()
                .map(|arr| {
                    arr.iter()
                        .filter_map(|v| {
                            v.as_object().map(|obj| {
                                obj.iter()
                                    .filter_map(|(k, v)| {
                                        v.as_str().map(|s| (k.clone(), s.to_string()))
                                    })
                                    .collect()
                            })
                        })
                        .collect()
                })
                .unwrap_or_default();
            let content_hash = item["ContentHash"].as_str().map(|s| s.to_string());
            let context: Option<HashMap<String, String>> = item["Context"].as_object().map(|obj| {
                obj.iter()
                    .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                    .collect()
            });

            inv_items.push(InventoryItem {
                type_name,
                schema_version,
                capture_time,
                content,
                content_hash,
                context,
            });
        }

        let mut state = self.state.write();
        let entry = state
            .inventory_entries
            .entry(instance_id.clone())
            .or_insert_with(|| InventoryEntry {
                instance_id: instance_id.clone(),
                items: Vec::new(),
            });

        // Merge: replace items by TypeName, add new ones
        for new_item in inv_items {
            if let Some(existing) = entry
                .items
                .iter_mut()
                .find(|i| i.type_name == new_item.type_name)
            {
                *existing = new_item;
            } else {
                entry.items.push(new_item);
            }
        }

        Ok(json_resp(
            json!({ "Message": "Inventory was saved successfully" }),
        ))
    }

    pub(super) fn get_inventory(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        validate_optional_range_i64("MaxResults", body["MaxResults"].as_i64(), 1, 50)?;
        let state = self.state.read();
        let entities: Vec<Value> = state
            .inventory_entries
            .values()
            .map(|entry| {
                let data: HashMap<String, Value> = entry
                    .items
                    .iter()
                    .map(|item| {
                        (
                            item.type_name.clone(),
                            json!({
                                "TypeName": item.type_name,
                                "SchemaVersion": item.schema_version,
                                "CaptureTime": item.capture_time,
                                "Content": item.content,
                            }),
                        )
                    })
                    .collect();
                json!({
                    "Id": entry.instance_id,
                    "Data": data,
                })
            })
            .collect();
        Ok(json_resp(json!({ "Entities": entities })))
    }

    pub(super) fn get_inventory_schema(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        validate_optional_string_length("TypeName", body["TypeName"].as_str(), 0, 100)?;
        validate_optional_range_i64("MaxResults", body["MaxResults"].as_i64(), 50, 200)?;
        // Return standard inventory type schemas
        let schemas = vec![
            json!({
                "TypeName": "AWS:Application",
                "Version": "1.1",
                "Attributes": [
                    {"Name": "Name", "DataType": "STRING"},
                    {"Name": "ApplicationType", "DataType": "STRING"},
                    {"Name": "Publisher", "DataType": "STRING"},
                    {"Name": "Version", "DataType": "STRING"},
                    {"Name": "InstalledTime", "DataType": "STRING"},
                    {"Name": "Architecture", "DataType": "STRING"},
                    {"Name": "URL", "DataType": "STRING"},
                ]
            }),
            json!({
                "TypeName": "AWS:InstanceInformation",
                "Version": "1.0",
                "Attributes": [
                    {"Name": "AgentType", "DataType": "STRING"},
                    {"Name": "AgentVersion", "DataType": "STRING"},
                    {"Name": "ComputerName", "DataType": "STRING"},
                    {"Name": "InstanceId", "DataType": "STRING"},
                    {"Name": "IpAddress", "DataType": "STRING"},
                    {"Name": "PlatformName", "DataType": "STRING"},
                    {"Name": "PlatformType", "DataType": "STRING"},
                    {"Name": "PlatformVersion", "DataType": "STRING"},
                    {"Name": "ResourceType", "DataType": "STRING"},
                ]
            }),
            json!({
                "TypeName": "AWS:Network",
                "Version": "1.0",
                "Attributes": [
                    {"Name": "Name", "DataType": "STRING"},
                    {"Name": "SubnetMask", "DataType": "STRING"},
                    {"Name": "Gateway", "DataType": "STRING"},
                    {"Name": "DHCPServer", "DataType": "STRING"},
                    {"Name": "DNSServer", "DataType": "STRING"},
                    {"Name": "MacAddress", "DataType": "STRING"},
                    {"Name": "IPV4", "DataType": "STRING"},
                    {"Name": "IPV6", "DataType": "STRING"},
                ]
            }),
        ];
        Ok(json_resp(json!({ "Schemas": schemas })))
    }

    pub(super) fn list_inventory_entries(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        validate_optional_string_length("TypeName", body["TypeName"].as_str(), 1, 100)?;
        validate_optional_range_i64("MaxResults", body["MaxResults"].as_i64(), 1, 50)?;
        let instance_id = body["InstanceId"]
            .as_str()
            .ok_or_else(|| missing("InstanceId"))?;
        let type_name = body["TypeName"]
            .as_str()
            .ok_or_else(|| missing("TypeName"))?;

        let state = self.state.read();
        let entries: Vec<&HashMap<String, String>> = state
            .inventory_entries
            .get(instance_id)
            .map(|entry| {
                entry
                    .items
                    .iter()
                    .filter(|item| item.type_name == type_name)
                    .flat_map(|item| item.content.iter())
                    .collect()
            })
            .unwrap_or_default();

        let capture_time = state
            .inventory_entries
            .get(instance_id)
            .and_then(|e| e.items.iter().find(|i| i.type_name == type_name))
            .map(|i| i.capture_time.as_str())
            .unwrap_or("");
        let schema_version = state
            .inventory_entries
            .get(instance_id)
            .and_then(|e| e.items.iter().find(|i| i.type_name == type_name))
            .map(|i| i.schema_version.as_str())
            .unwrap_or("1.0");

        Ok(json_resp(json!({
            "TypeName": type_name,
            "InstanceId": instance_id,
            "SchemaVersion": schema_version,
            "CaptureTime": capture_time,
            "Entries": entries,
        })))
    }

    pub(super) fn delete_inventory(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        validate_optional_string_length("TypeName", body["TypeName"].as_str(), 1, 100)?;
        validate_optional_enum(
            "SchemaDeleteOption",
            body["SchemaDeleteOption"].as_str(),
            &["DISABLE_SCHEMA", "DELETE_SCHEMA"],
        )?;
        let type_name = body["TypeName"]
            .as_str()
            .ok_or_else(|| missing("TypeName"))?
            .to_string();

        let mut state = self.state.write();

        // Remove matching inventory items
        for entry in state.inventory_entries.values_mut() {
            entry.items.retain(|i| i.type_name != type_name);
        }

        state.inventory_deletion_counter += 1;
        let deletion_id = format!("{}", uuid::Uuid::new_v4());
        let now = Utc::now();

        state.inventory_deletions.push(InventoryDeletion {
            deletion_id: deletion_id.clone(),
            type_name: type_name.clone(),
            deletion_start_time: now,
            last_status: "Complete".to_string(),
            last_status_message: "Deletion completed successfully.".to_string(),
            deletion_summary: json!({
                "TotalCount": 0,
                "RemainingCount": 0,
                "SummaryItems": [],
            }),
            last_status_update_time: now,
        });

        Ok(json_resp(json!({
            "DeletionId": deletion_id,
            "TypeName": type_name,
            "DeletionSummary": {
                "TotalCount": 0,
                "RemainingCount": 0,
                "SummaryItems": [],
            },
        })))
    }

    pub(super) fn describe_inventory_deletions(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        validate_optional_range_i64("MaxResults", body["MaxResults"].as_i64(), 1, 50)?;
        let state = self.state.read();
        let deletions: Vec<Value> = state
            .inventory_deletions
            .iter()
            .map(|d| {
                json!({
                    "DeletionId": d.deletion_id,
                    "TypeName": d.type_name,
                    "DeletionStartTime": d.deletion_start_time.timestamp_millis() as f64 / 1000.0,
                    "LastStatus": d.last_status,
                    "LastStatusMessage": d.last_status_message,
                    "DeletionSummary": d.deletion_summary,
                    "LastStatusUpdateTime": d.last_status_update_time.timestamp_millis() as f64 / 1000.0,
                })
            })
            .collect();
        Ok(json_resp(json!({ "InventoryDeletions": deletions })))
    }

    // ── Compliance ────────────────────────────────────────────────
}
