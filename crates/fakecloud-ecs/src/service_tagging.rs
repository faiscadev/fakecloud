// Auto-extracted from service.rs as part of carryover service.rs split.

#![allow(clippy::too_many_arguments)]

use serde_json::json;

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use super::*;

/// Resolve the tag list of any taggable ECS resource from its decoded ARN
/// (`resource_type` + `tail`). One resolver backs TagResource, UntagResource
/// and ListTagsForResource so every taggable type is covered by all three.
fn resource_tags_mut<'a>(
    state: &'a mut EcsState,
    arn: &str,
    resource_type: &str,
    tail: &str,
) -> Result<&'a mut Vec<TagEntry>, AwsServiceError> {
    let not_found = || resource_not_found(arn);
    let revision_of = |kind: &str| {
        let (family, rev) = parse_family_revision(tail);
        rev.map(|rev| (family, rev))
            .ok_or_else(|| invalid_parameter(format!("{kind} ARN must include revision")))
    };
    match resource_type {
        "cluster" => state
            .clusters
            .get_mut(tail)
            .map(|c| &mut c.tags)
            .ok_or_else(not_found),
        "task-definition" => {
            let (family, rev) = revision_of("task-definition")?;
            state
                .task_definitions
                .get_mut(&family)
                .and_then(|m| m.get_mut(&rev))
                .map(|td| &mut td.tags)
                .ok_or_else(not_found)
        }
        "service" => {
            let key = resolve_service_key(state, tail).ok_or_else(not_found)?;
            state
                .services
                .get_mut(&key)
                .map(|s| &mut s.tags)
                .ok_or_else(not_found)
        }
        "task" => {
            let task_id = tail.rsplit('/').next().unwrap_or(tail);
            state
                .tasks
                .get_mut(task_id)
                .map(|t| &mut t.tags)
                .ok_or_else(not_found)
        }
        "task-set" => state
            .task_sets
            .get_mut(tail)
            .map(|t| &mut t.tags)
            .ok_or_else(not_found),
        "container-instance" => {
            let key = resolve_container_instance_key(state, tail).ok_or_else(not_found)?;
            state
                .container_instances
                .get_mut(&key)
                .map(|c| &mut c.tags)
                .ok_or_else(not_found)
        }
        "capacity-provider" => state
            .capacity_providers
            .get_mut(tail)
            .map(|c| &mut c.tags)
            .ok_or_else(not_found),
        // `daemon/<cluster>/<name>`, keyed `cluster/name`.
        "daemon" => state
            .daemons
            .get_mut(tail)
            .map(|d| &mut d.tags)
            .ok_or_else(not_found),
        "daemon-task-definition" => {
            let (family, rev) = revision_of("daemon-task-definition")?;
            state
                .daemon_task_definitions
                .get_mut(&family)
                .and_then(|m| m.get_mut(&rev))
                .map(|td| &mut td.tags)
                .ok_or_else(not_found)
        }
        // `express-gateway-service/<cluster>/<name>`, keyed `cluster/name`.
        "express-gateway-service" => state
            .express_gateway_services
            .get_mut(tail)
            .map(|s| &mut s.tags)
            .ok_or_else(not_found),
        // `gateway/<name>/<id>`, keyed by ARN; a deleted gateway is gone.
        "gateway" => state
            .gateways
            .get_mut(arn)
            .filter(|g| g.status != "DELETED")
            .map(|g| &mut g.tags)
            .ok_or_else(not_found),
        other => Err(invalid_parameter(format!(
            "Unknown ECS resource type: {other}"
        ))),
    }
}

impl EcsService {
    pub(super) fn tag_resource(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = request.json_body();
        let arn = req_str(&body, "resourceArn")?.to_string();
        let tags = parse_tags(&body);
        let (account, resource_type, tail) = decode_ecs_arn(&arn)?;
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&account);
        let current = resource_tags_mut(state, &arn, &resource_type, &tail)?;
        merge_tags(current, tags);
        Ok(AwsResponse::ok_json(json!({})))
    }

    pub(super) fn untag_resource(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = request.json_body();
        let arn = req_str(&body, "resourceArn")?.to_string();
        let keys: Vec<String> = body
            .get("tagKeys")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();
        let (account, resource_type, tail) = decode_ecs_arn(&arn)?;
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&account);
        let current = resource_tags_mut(state, &arn, &resource_type, &tail)?;
        current.retain(|t| !keys.contains(&t.key));
        Ok(AwsResponse::ok_json(json!({})))
    }

    pub(super) fn list_tags_for_resource(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = request.json_body();
        let arn = req_str(&body, "resourceArn")?.to_string();
        let (account, resource_type, tail) = decode_ecs_arn(&arn)?;
        let mut accounts = self.state.write();
        let state = accounts
            .get_mut(&account)
            .ok_or_else(|| resource_not_found(&arn))?;
        let tags = resource_tags_mut(state, &arn, &resource_type, &tail)?;
        Ok(AwsResponse::ok_json(json!({"tags": tags_json(tags)})))
    }
}
