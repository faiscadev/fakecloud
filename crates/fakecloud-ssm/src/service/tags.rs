use http::StatusCode;
use serde_json::{json, Value};

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};
use fakecloud_core::validation::*;

use super::parameters::{lookup_param, lookup_param_mut};
use super::{json_resp, missing, parse_body, SsmService};

impl SsmService {
    pub(super) fn add_tags_to_resource(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let resource_type = body["ResourceType"].as_str().unwrap_or("Parameter");
        let resource_id = body["ResourceId"]
            .as_str()
            .ok_or_else(|| missing("ResourceId"))?;
        let tags = body["Tags"].as_array().ok_or_else(|| missing("Tags"))?;

        let mut state = self.state.write();

        match resource_type {
            "Parameter" => {
                let param = lookup_param_mut(&mut state.parameters, resource_id)
                    .ok_or_else(|| invalid_resource_id(resource_id))?;

                for tag in tags {
                    if let (Some(key), Some(val)) = (tag["Key"].as_str(), tag["Value"].as_str()) {
                        param.tags.insert(key.to_string(), val.to_string());
                    }
                }
            }
            "Document" => {
                let doc = state
                    .documents
                    .get_mut(resource_id)
                    .ok_or_else(|| invalid_resource_id(resource_id))?;

                for tag in tags {
                    if let (Some(key), Some(val)) = (tag["Key"].as_str(), tag["Value"].as_str()) {
                        doc.tags.insert(key.to_string(), val.to_string());
                    }
                }
            }
            "MaintenanceWindow" => {
                let mw = state
                    .maintenance_windows
                    .get_mut(resource_id)
                    .ok_or_else(|| invalid_resource_id(resource_id))?;

                for tag in tags {
                    if let (Some(key), Some(val)) = (tag["Key"].as_str(), tag["Value"].as_str()) {
                        mw.tags.insert(key.to_string(), val.to_string());
                    }
                }
            }
            "PatchBaseline" => {
                let pb = state
                    .patch_baselines
                    .get_mut(resource_id)
                    .ok_or_else(|| invalid_resource_id(resource_id))?;

                for tag in tags {
                    if let (Some(key), Some(val)) = (tag["Key"].as_str(), tag["Value"].as_str()) {
                        pb.tags.insert(key.to_string(), val.to_string());
                    }
                }
            }
            _ => {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidResourceType",
                    format!(
                        "{resource_type} is not a valid resource type. \
                         Valid resource types are: ManagedInstance, MaintenanceWindow, \
                         Parameter, PatchBaseline, OpsItem, Document."
                    ),
                ));
            }
        }

        Ok(json_resp(json!({})))
    }

    pub(super) fn remove_tags_from_resource(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        validate_required("ResourceType", &body["ResourceType"])?;
        let resource_type = body["ResourceType"].as_str().unwrap_or("Parameter");
        let resource_id = body["ResourceId"]
            .as_str()
            .ok_or_else(|| missing("ResourceId"))?;
        let tag_keys = body["TagKeys"]
            .as_array()
            .ok_or_else(|| missing("TagKeys"))?;

        let mut state = self.state.write();

        match resource_type {
            "Parameter" => {
                let param = lookup_param_mut(&mut state.parameters, resource_id)
                    .ok_or_else(|| invalid_resource_id(resource_id))?;

                for key in tag_keys {
                    if let Some(k) = key.as_str() {
                        param.tags.remove(k);
                    }
                }
            }
            "Document" => {
                let doc = state
                    .documents
                    .get_mut(resource_id)
                    .ok_or_else(|| invalid_resource_id(resource_id))?;

                for key in tag_keys {
                    if let Some(k) = key.as_str() {
                        doc.tags.remove(k);
                    }
                }
            }
            "MaintenanceWindow" => {
                let mw = state
                    .maintenance_windows
                    .get_mut(resource_id)
                    .ok_or_else(|| invalid_resource_id(resource_id))?;

                for key in tag_keys {
                    if let Some(k) = key.as_str() {
                        mw.tags.remove(k);
                    }
                }
            }
            "PatchBaseline" => {
                let pb = state
                    .patch_baselines
                    .get_mut(resource_id)
                    .ok_or_else(|| invalid_resource_id(resource_id))?;

                for key in tag_keys {
                    if let Some(k) = key.as_str() {
                        pb.tags.remove(k);
                    }
                }
            }
            _ => {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidResourceType",
                    format!(
                        "{resource_type} is not a valid resource type. \
                         Valid resource types are: ManagedInstance, MaintenanceWindow, \
                         Parameter, PatchBaseline, OpsItem, Document."
                    ),
                ));
            }
        }

        Ok(json_resp(json!({})))
    }

    pub(super) fn list_tags_for_resource(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let resource_type = body["ResourceType"].as_str().unwrap_or("Parameter");
        let resource_id = body["ResourceId"]
            .as_str()
            .ok_or_else(|| missing("ResourceId"))?;

        let state = self.state.read();

        let tags: Vec<Value> = match resource_type {
            "Parameter" => {
                let param = lookup_param(&state.parameters, resource_id)
                    .ok_or_else(|| invalid_resource_id(resource_id))?;
                param
                    .tags
                    .iter()
                    .map(|(k, v)| json!({"Key": k, "Value": v}))
                    .collect()
            }
            "Document" => {
                let doc = state
                    .documents
                    .get(resource_id)
                    .ok_or_else(|| invalid_resource_id(resource_id))?;
                doc.tags
                    .iter()
                    .map(|(k, v)| json!({"Key": k, "Value": v}))
                    .collect()
            }
            "MaintenanceWindow" => {
                let mw = state
                    .maintenance_windows
                    .get(resource_id)
                    .ok_or_else(|| invalid_resource_id(resource_id))?;
                mw.tags
                    .iter()
                    .map(|(k, v)| json!({"Key": k, "Value": v}))
                    .collect()
            }
            "PatchBaseline" => {
                let pb = state
                    .patch_baselines
                    .get(resource_id)
                    .ok_or_else(|| invalid_resource_id(resource_id))?;
                pb.tags
                    .iter()
                    .map(|(k, v)| json!({"Key": k, "Value": v}))
                    .collect()
            }
            _ => {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidResourceType",
                    format!(
                        "{resource_type} is not a valid resource type. \
                         Valid resource types are: ManagedInstance, MaintenanceWindow, \
                         Parameter, PatchBaseline, OpsItem, Document."
                    ),
                ));
            }
        };

        let mut tags = tags;
        tags.sort_by(|a, b| {
            let ka = a["Key"].as_str().unwrap_or("");
            let kb = b["Key"].as_str().unwrap_or("");
            ka.cmp(kb)
        });

        Ok(json_resp(json!({ "TagList": tags })))
    }
}

pub(super) fn invalid_resource_id(id: &str) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        "InvalidResourceId",
        format!("The resource ID \"{id}\" is not valid. Verify the ID and try again."),
    )
}
