use std::collections::BTreeMap;

use http::StatusCode;
use serde_json::json;

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};
use fakecloud_core::tags;
use fakecloud_core::validation::*;

use super::parameters::{lookup_param, lookup_param_mut};
use super::{missing, SsmService};
use crate::state::SsmState;

const INVALID_RESOURCE_TYPE_MSG: &str = " is not a valid resource type. \
    Valid resource types are: ManagedInstance, MaintenanceWindow, \
    Parameter, PatchBaseline, OpsItem, Document.";

impl SsmService {
    /// Resolve a mutable reference to the tag map for the given SSM resource.
    fn resolve_tags_mut<'a>(
        state: &'a mut crate::state::SsmState,
        resource_type: &str,
        resource_id: &str,
    ) -> Result<&'a mut BTreeMap<String, String>, AwsServiceError> {
        match resource_type {
            "Parameter" => {
                let param = lookup_param_mut(&mut state.parameters, resource_id)
                    .ok_or_else(|| invalid_resource_id(resource_id))?;
                Ok(&mut param.tags)
            }
            "Document" => {
                let doc = state
                    .documents
                    .get_mut(resource_id)
                    .ok_or_else(|| invalid_resource_id(resource_id))?;
                Ok(&mut doc.tags)
            }
            "MaintenanceWindow" => {
                let mw = state
                    .maintenance_windows
                    .get_mut(resource_id)
                    .ok_or_else(|| invalid_resource_id(resource_id))?;
                Ok(&mut mw.tags)
            }
            "PatchBaseline" => {
                let pb = state
                    .patch_baselines
                    .get_mut(resource_id)
                    .ok_or_else(|| invalid_resource_id(resource_id))?;
                Ok(&mut pb.tags)
            }
            _ => Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidResourceType",
                format!("{resource_type}{INVALID_RESOURCE_TYPE_MSG}"),
            )),
        }
    }

    /// Resolve an immutable reference to the tag map for the given SSM resource.
    fn resolve_tags<'a>(
        state: &'a crate::state::SsmState,
        resource_type: &str,
        resource_id: &str,
    ) -> Result<&'a BTreeMap<String, String>, AwsServiceError> {
        match resource_type {
            "Parameter" => {
                let param = lookup_param(&state.parameters, resource_id)
                    .ok_or_else(|| invalid_resource_id(resource_id))?;
                Ok(&param.tags)
            }
            "Document" => {
                let doc = state
                    .documents
                    .get(resource_id)
                    .ok_or_else(|| invalid_resource_id(resource_id))?;
                Ok(&doc.tags)
            }
            "MaintenanceWindow" => {
                let mw = state
                    .maintenance_windows
                    .get(resource_id)
                    .ok_or_else(|| invalid_resource_id(resource_id))?;
                Ok(&mw.tags)
            }
            "PatchBaseline" => {
                let pb = state
                    .patch_baselines
                    .get(resource_id)
                    .ok_or_else(|| invalid_resource_id(resource_id))?;
                Ok(&pb.tags)
            }
            _ => Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidResourceType",
                format!("{resource_type}{INVALID_RESOURCE_TYPE_MSG}"),
            )),
        }
    }

    pub(super) fn add_tags_to_resource(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        // ResourceType is `@required` in the SSM model — match
        // RemoveTagsFromResource and reject the missing-field case
        // instead of silently defaulting to "Parameter" (which made the
        // negative `omit_ResourceType` conformance variant a silent pass
        // whenever a default Parameter happened to exist in state).
        validate_required("ResourceType", &body["ResourceType"])?;
        let resource_type = body["ResourceType"].as_str().unwrap_or("Parameter");
        let resource_id = body["ResourceId"]
            .as_str()
            .ok_or_else(|| missing("ResourceId"))?;
        validate_required("Tags", &body["Tags"])?;

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let tag_map = Self::resolve_tags_mut(state, resource_type, resource_id)?;
        tags::apply_tags(tag_map, &body, "Tags", "Key", "Value").map_err(|f| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                format!("{f} must be a list"),
            )
        })?;

        Ok(AwsResponse::ok_json(json!({})))
    }

    pub(super) fn remove_tags_from_resource(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        validate_required("ResourceType", &body["ResourceType"])?;
        let resource_type = body["ResourceType"].as_str().unwrap_or("Parameter");
        let resource_id = body["ResourceId"]
            .as_str()
            .ok_or_else(|| missing("ResourceId"))?;
        validate_required("TagKeys", &body["TagKeys"])?;

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let tag_map = Self::resolve_tags_mut(state, resource_type, resource_id)?;
        tags::remove_tags(tag_map, &body, "TagKeys").map_err(|f| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                format!("{f} must be a list"),
            )
        })?;

        Ok(AwsResponse::ok_json(json!({})))
    }

    pub(super) fn list_tags_for_resource(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let resource_type = body["ResourceType"].as_str().unwrap_or("Parameter");
        let resource_id = body["ResourceId"]
            .as_str()
            .ok_or_else(|| missing("ResourceId"))?;

        let accounts = self.state.read();
        let empty = SsmState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
        let tag_map = Self::resolve_tags(state, resource_type, resource_id)?;
        let result = tags::tags_to_json(tag_map, "Key", "Value");

        Ok(AwsResponse::ok_json(json!({ "TagList": result })))
    }
}

pub(super) fn invalid_resource_id(id: &str) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        "InvalidResourceId",
        format!("The resource ID \"{id}\" is not valid. Verify the ID and try again."),
    )
}
